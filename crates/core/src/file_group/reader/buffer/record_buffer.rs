/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Mirrors `org.apache.hudi.common.table.read.buffer.FileGroupRecordBuffer`.
//!
//! Common state for all record buffer variants. In Java this is an abstract class
//! with fields for the records map (ExternalSpillableMap), merger, delete context,
//! update processor, base file iterator, and total log records.
//!
//! In Rust, this is a struct used via composition by concrete implementations
//! (e.g. `KeyBasedFileGroupRecordBuffer`).

use crate::Result;
use crate::file_group::reader::buffered_record::BufferedRecord;
use crate::file_group::reader::delete_context::DeleteContext;
use crate::file_group::reader::record_merger::BufferedRecordMerger;
use crate::file_group::reader::update_processor::UpdateProcessor;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::collections::HashMap;
use std::vec::IntoIter;

/// Common state for all file group record buffer implementations.
///
/// Mirrors Java's `FileGroupRecordBuffer<T>` abstract class.
///
/// ## Key fields (matching Java):
/// - `records` — Java's `ExternalSpillableMap<Serializable, BufferedRecord<T>>`
///   (in Rust: `HashMap<String, BufferedRecord>` — no disk spilling for Phase 1)
/// - `buffered_record_merger` — for delta merge (log-vs-log) and final merge (base-vs-log)
/// - `update_processor` — strategy for processing updates during merge iteration
/// - `base_file_batches` / iteration state — Java's `baseFileIterator`
/// - `next_record` — Java's `nextRecord` (the lookahead for has_next/next pattern)
#[derive(Debug)]
pub struct FileGroupRecordBuffer {
    /// The per-key records map. Mirrors Java's `ExternalSpillableMap`.
    pub records: HashMap<String, BufferedRecord>,

    /// The reader schema.
    pub reader_schema: Option<SchemaRef>,

    /// The merge mode string (e.g. "COMMIT_TIME_ORDERING").
    pub record_merge_mode: String,

    /// The record merger for resolving conflicts.
    pub buffered_record_merger: Box<dyn BufferedRecordMerger>,

    /// Context for detecting delete records.
    pub delete_context: Option<DeleteContext>,

    /// Processor for update records during merge iteration.
    pub update_processor: Box<dyn UpdateProcessor>,

    /// Base file record batches (set after log scanning, before merge iteration).
    pub base_file_batches: Vec<RecordBatch>,

    /// Total number of log records processed.
    pub total_log_records: u64,

    /// Lookahead record for the has_next/next iteration pattern.
    /// Mirrors Java's `nextRecord` field.
    pub next_record: Option<BufferedRecord>,

    // ── Base file iteration state ──────────────────────────────────────
    /// Current batch index in base_file_batches.
    pub base_batch_idx: usize,
    /// Current row index within the current base file batch.
    pub base_row_idx: usize,

    // ── Log record iteration state ─────────────────────────────────────
    /// Drain iterator for remaining log records (created lazily on first call to has_next_log_record).
    pub log_record_iter: Option<IntoIter<BufferedRecord>>,
}

impl FileGroupRecordBuffer {
    pub fn new(
        record_merge_mode: String,
        buffered_record_merger: Box<dyn BufferedRecordMerger>,
        update_processor: Box<dyn UpdateProcessor>,
    ) -> Self {
        Self {
            records: HashMap::new(),
            reader_schema: None,
            record_merge_mode,
            buffered_record_merger,
            delete_context: None,
            update_processor,
            base_file_batches: Vec::new(),
            total_log_records: 0,
            next_record: None,
            base_batch_idx: 0,
            base_row_idx: 0,
            log_record_iter: None,
        }
    }

    /// Template method: check if next merged record exists.
    ///
    /// Mirrors Java's `hasNext()`: `nextRecord != null || doHasNext()`
    pub fn has_next<F>(&mut self, do_has_next: F) -> Result<bool>
    where
        F: FnOnce(&mut Self) -> Result<bool>,
    {
        if self.next_record.is_some() {
            return Ok(true);
        }
        do_has_next(self)
    }

    /// Return the next merged buffered record.
    ///
    /// Mirrors Java's `next()`: take and return nextRecord.
    pub fn next(&mut self) -> Option<BufferedRecord> {
        self.next_record.take()
    }

    /// Merge a base record with its corresponding log record (if any).
    ///
    /// Mirrors Java's `hasNextBaseRecord(T baseRecord, BufferedRecord<T> logRecordInfo)`.
    ///
    /// If `log_record_info` is Some, does a final merge (base vs log).
    /// If None, the base record is an insert (no log record for this key).
    pub fn has_next_base_record(
        &mut self,
        base_record: &BufferedRecord,
        log_record_info: Option<&BufferedRecord>,
    ) -> Result<bool> {
        if let Some(log_record) = log_record_info {
            // Merge base + log
            let merge_result = self
                .buffered_record_merger
                .final_merge(base_record, log_record)?;
            let processed = self.update_processor.process_update(
                &log_record.record_key,
                Some(base_record),
                &merge_result,
                merge_result.is_delete(),
            )?;
            self.next_record = processed;
            return Ok(self.next_record.is_some());
        }

        // Insert: base record only, no log record
        self.next_record = Some(base_record.clone());
        Ok(true)
    }

    /// Iterate remaining log records not consumed by base file iteration.
    ///
    /// Mirrors Java's `hasNextLogRecord()`.
    pub fn has_next_log_record(&mut self) -> bool {
        if self.log_record_iter.is_none() {
            // Drain remaining records from the map
            let remaining: Vec<BufferedRecord> = self.records.drain().map(|(_, v)| v).collect();
            self.log_record_iter = Some(remaining.into_iter());
        }

        let iter = self.log_record_iter.as_mut().unwrap();
        while let Some(record) = iter.next() {
            let processed = self
                .update_processor
                .process_update(&record.record_key, None, &record, record.is_delete());
            match processed {
                Ok(Some(r)) => {
                    self.next_record = Some(r);
                    return true;
                }
                Ok(None) => continue,
                Err(_) => return false,
            }
        }
        false
    }

    /// Check if there are more rows to iterate in the base file batches.
    pub fn has_more_base_rows(&self) -> bool {
        if self.base_batch_idx >= self.base_file_batches.len() {
            return false;
        }
        if self.base_batch_idx == self.base_file_batches.len() - 1 {
            return self.base_row_idx < self.base_file_batches[self.base_batch_idx].num_rows();
        }
        true
    }

    /// Get the next base file row as a single-row RecordBatch and advance the position.
    pub fn next_base_row(&mut self) -> Option<RecordBatch> {
        while self.base_batch_idx < self.base_file_batches.len() {
            let batch = &self.base_file_batches[self.base_batch_idx];
            if self.base_row_idx < batch.num_rows() {
                let row = batch.slice(self.base_row_idx, 1);
                self.base_row_idx += 1;
                return Some(row);
            }
            self.base_batch_idx += 1;
            self.base_row_idx = 0;
        }
        None
    }
}
