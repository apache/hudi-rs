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
use crate::file_group::reader_v2::buffer::spillable_map::{SpillDrainIter, SpillableRecordMap};
use crate::file_group::reader_v2::buffered_record::BufferedRecord;
use crate::file_group::reader_v2::delete_context::DeleteContext;
use crate::file_group::reader_v2::record_merger::BufferedRecordMerger;
use crate::file_group::reader_v2::update_processor::UpdateProcessor;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;
use std::sync::Arc;
use std::vec::IntoIter;

/// Common state for all file group record buffer implementations.
///
/// Mirrors Java's `FileGroupRecordBuffer<T>` abstract class.
///
/// ## Key fields (matching Java):
/// - `records` — Java's `ExternalSpillableMap<Serializable, BufferedRecord<T>>`
///   (in Rust: [`SpillableRecordMap`] — size-tracked, RocksDB-spillable)
/// - `buffered_record_merger` — for delta merge (log-vs-log) and final merge (base-vs-log)
/// - `update_processor` — strategy for processing updates during merge iteration
/// - `base_file_source` / `current_base_batch` / `base_row_idx` — lazy
///   counterpart of Java's `baseFileIterator`, and used only by the row-wise
///   legacy path (`has_next`/`next`/`merge_and_collect_with_stats`) and the
///   tests that drive it. The batch-at-a-time merge the reader uses is handed
///   each base batch by its caller and holds no source. The current batch is
///   interned into an `Arc` so base records become zero-copy `BatchRef`s.
/// - `next_record` — Java's `nextRecord` (the lookahead for has_next/next pattern)
pub struct FileGroupRecordBuffer {
    /// The per-key records map. Mirrors Java's `ExternalSpillableMap`: keeps
    /// entries in memory until the merge budget is hit, then spills to RocksDB.
    pub records: SpillableRecordMap,

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

    // ── Base file iteration state (lazy source + batch-ref) ──
    /// Lazy base file source set by [`HoodieFileGroupRecordBuffer::set_base_file_source`].
    /// `None` until log scan + base open has run, or for log-only file groups.
    ///
    /// A3 — the base file is pulled one parquet row-group at a
    /// time from this `RecordBatchReader` instead of being eagerly collected
    /// and concatenated into a `Vec<RecordBatch>`.
    ///
    /// Only the row-wise legacy path reads it — `has_next`/`next` and
    /// `merge_and_collect_with_stats`, plus the tests that drive them. The
    /// reader's own path merges a batch it is handed, so it sets nothing here.
    pub base_file_source: Option<Box<dyn RecordBatchReader + Send>>,

    /// The current base file batch being iterated, interned into a single
    /// `Arc` when pulled from `base_file_source`. Held as `Arc<RecordBatch>`
    /// so base records can become zero-copy
    /// [`RecordPayload::BatchRef`](crate::file_group::reader_v2::buffered_record::RecordPayload::BatchRef)
    /// views into the *streamed* batch rather than per-row clones during
    /// the base-vs-log merge. The batch-pinning valve still applies: a streamed
    /// batch stays alive only while a surviving `BatchRef` points into it.
    pub current_base_batch: Option<Arc<RecordBatch>>,

    /// Current row index within `current_base_batch`.
    pub base_row_idx: usize,

    /// Total number of log records processed.
    pub total_log_records: u64,

    /// Lookahead record for the has_next/next iteration pattern.
    /// Mirrors Java's `nextRecord` field.
    pub next_record: Option<BufferedRecord>,

    // ── Log record iteration state ─────────────────────────────────────
    /// Drain iterator for remaining log records (created lazily on first call to has_next_log_record).
    pub log_record_iter: Option<IntoIter<BufferedRecord>>,

    /// Streaming drain iterator for the chunked log-only-insert flush
    /// ([`drain_log_only_inserts`](super::HoodieFileGroupRecordBuffer::drain_log_only_inserts)).
    /// Created lazily on the first drain call by moving `records` into a
    /// [`SpillDrainIter`]; each drain call pulls one bounded (`DEFAULT_BATCH_SIZE`)
    /// chunk from it. Distinct from `log_record_iter` (the eager `read()`-path
    /// drain). Bounds drain-time memory to ~one output batch instead of
    /// re-materializing the whole (possibly-spilled) map at once.
    pub log_drain_iter: Option<SpillDrainIter>,

    // ── Stage timings (perf harness) ──────────────────────
    /// Cumulative wall ms inflating/decoding log blocks (subset of merge insert).
    pub stage_decode_ms: u64,
    /// Peak `records` map size observed during the scan.
    pub merge_map_peak_entries: u64,
}

// Hand-rolled Debug because `Box<dyn RecordBatchReader>` is not Debug.
impl std::fmt::Debug for FileGroupRecordBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileGroupRecordBuffer")
            .field("records_len", &self.records.len())
            .field("record_merge_mode", &self.record_merge_mode)
            .field("total_log_records", &self.total_log_records)
            .field("base_file_source_set", &self.base_file_source.is_some())
            .field(
                "current_base_batch_rows",
                &self.current_base_batch.as_ref().map(|b| b.num_rows()),
            )
            .field("base_row_idx", &self.base_row_idx)
            .field("has_next_record", &self.next_record.is_some())
            .finish()
    }
}

impl FileGroupRecordBuffer {
    pub fn new(
        record_merge_mode: String,
        buffered_record_merger: Box<dyn BufferedRecordMerger>,
        update_processor: Box<dyn UpdateProcessor>,
        reader_schema: Option<SchemaRef>,
        records: SpillableRecordMap,
    ) -> Self {
        Self {
            records,
            reader_schema,
            record_merge_mode,
            buffered_record_merger,
            delete_context: None,
            update_processor,
            base_file_source: None,
            current_base_batch: None,
            base_row_idx: 0,
            total_log_records: 0,
            next_record: None,
            log_record_iter: None,
            log_drain_iter: None,
            stage_decode_ms: 0,
            merge_map_peak_entries: 0,
        }
    }

    /// Return the next merged buffered record.
    ///
    /// Mirrors Java's `next()`: take and return nextRecord. Named `take_next`
    /// (not `next`) to avoid colliding with the `Iterator::next` contract, since
    /// this type is not an iterator (it pairs with `has_next`).
    pub fn take_next(&mut self) -> Option<BufferedRecord> {
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
    /// Mirrors Java's `hasNextLogRecord()`. A `process_update` failure is
    /// propagated as `Err`: swallowing it into `Ok(false)` would
    /// silently truncate the merged output, mirroring Java's checked-exception
    /// behavior on the `doHasNext` → `hasNext` chain.
    pub fn has_next_log_record(&mut self) -> Result<bool> {
        if self.log_record_iter.is_none() {
            // Drain remaining records from the map (in-memory tier first, then
            // any spilled disk tier — SpillableRecordMap::drain_iter order).
            // `drain_iter` consumes the map, so swap in a fresh empty one.
            let map = std::mem::take(&mut self.records);
            let remaining: Vec<BufferedRecord> = map.drain_iter().collect::<Result<Vec<_>>>()?;
            self.log_record_iter = Some(remaining.into_iter());
        }

        let iter = self.log_record_iter.as_mut().unwrap();
        for record in iter.by_ref() {
            let processed = self.update_processor.process_update(
                &record.record_key,
                None,
                &record,
                record.is_delete(),
            )?;
            if let Some(r) = processed {
                self.next_record = Some(r);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Get the next base file row as a `(shared batch, row index)` pair and
    /// advance the position. Pulls a fresh row-group from `base_file_source`
    /// whenever `current_base_batch` is exhausted (or absent on first call).
    ///
    /// Lazy: pulls one parquet row-group at a time from the streaming source
    /// rather than walking an eagerly collected `Vec<RecordBatch>`. Each
    /// pulled batch is interned into a single
    /// `Arc` so the returned `(Arc<RecordBatch>, row_idx)` lets the caller
    /// build a zero-copy
    /// [`RecordPayload::BatchRef`](crate::file_group::reader_v2::buffered_record::RecordPayload::BatchRef)
    /// into the *streamed* batch instead of slicing/cloning a single-row
    /// batch per base row. The interned `Arc` is the single mint point per
    /// streamed batch, so `Arc::as_ptr` stays a valid compaction grouping key.
    ///
    /// # Errors
    /// A source error mid-stream is **propagated** (not swallowed as
    /// end-of-stream): returning `Ok(None)` on error would silently truncate the
    /// base file and drop rows. The `Result` lets the surrounding `do_has_next`
    /// surface it loudly.
    pub fn next_base_row(&mut self) -> Result<Option<(Arc<RecordBatch>, usize)>> {
        loop {
            // Slice + advance from the current batch if rows remain.
            if let Some(batch) = self.current_base_batch.as_ref()
                && self.base_row_idx < batch.num_rows()
            {
                let row_idx = self.base_row_idx;
                self.base_row_idx += 1;
                return Ok(Some((batch.clone(), row_idx)));
            }

            // Current batch exhausted (or unset) — pull from source.
            let Some(source) = self.base_file_source.as_mut() else {
                return Ok(None);
            };
            match source.next() {
                None => {
                    // Source exhausted; release it so a later call cannot pull
                    // from a reader that has already ended.
                    self.current_base_batch = None;
                    self.base_file_source = None;
                    return Ok(None);
                }
                Some(Ok(batch)) => {
                    // Intern the streamed row-group into one `Arc` (the
                    // single mint point per base batch). BatchRefs built from
                    // it share this `Arc`; it stays alive only while a
                    // surviving ref pins it — the compaction valve applies.
                    self.current_base_batch = Some(Arc::new(batch));
                    self.base_row_idx = 0;
                    // Loop again to slice the first row of the new batch.
                }
                Some(Err(e)) => {
                    // A mid-stream base source error must NOT be treated as
                    // end-of-stream — silently ending iteration here would drop
                    // the remaining base rows and truncate the read (silent data
                    // loss). Release the source and propagate the error loudly so
                    // the caller (`do_has_next`, `Result`-returning) surfaces it.
                    // Matches the vectorized `next_merged_base_batch`, which
                    // errors the same way.
                    self.current_base_batch = None;
                    self.base_file_source = None;
                    return Err(crate::error::CoreError::ReadFileSliceError(format!(
                        "base file source error during base iteration: {e}"
                    )));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CoreError;
    use crate::file_group::reader_v2::record_merger::BufferedRecordMergerFactory;
    use crate::file_group::reader_v2::update_processor::UpdateStats;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    /// An UpdateProcessor that always errors. Used to prove `has_next_log_record`
    /// propagates `process_update` failures instead of swallowing them.
    #[derive(Debug, Default)]
    struct ErroringUpdateProcessor;

    impl UpdateProcessor for ErroringUpdateProcessor {
        fn process_update(
            &self,
            _record_key: &str,
            _previous_record: Option<&BufferedRecord>,
            _merged_record: &BufferedRecord,
            _is_delete: bool,
        ) -> Result<Option<BufferedRecord>> {
            Err(CoreError::ReadFileSliceError(
                "process_update boom (A1 propagation test)".to_string(),
            ))
        }

        fn read_stats_counts(&self) -> UpdateStats {
            UpdateStats::default()
        }
    }

    fn make_record(key: &str) -> BufferedRecord {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
        BufferedRecord::new_data(key.to_string(), batch, None)
    }

    fn buffer_with_erroring_processor() -> FileGroupRecordBuffer {
        let merger = BufferedRecordMergerFactory::create_with(
            "COMMIT_TIME_ORDERING",
            &std::collections::HashMap::new(),
        )
        .unwrap();
        FileGroupRecordBuffer::new(
            "COMMIT_TIME_ORDERING".to_string(),
            merger,
            Box::new(ErroringUpdateProcessor),
            None,
            SpillableRecordMap::new(),
        )
    }

    /// `has_next_log_record` must surface `process_update` errors rather than
    /// returning `Ok(false)` (which silently truncates the output).
    #[test]
    fn test_has_next_log_record_propagates_update_processor_error() {
        let mut buffer = buffer_with_erroring_processor();
        buffer
            .records
            .insert("k1".to_string(), make_record("k1"))
            .unwrap();

        let result = buffer.has_next_log_record();
        assert!(
            result.is_err(),
            "process_update error must propagate, not be swallowed into Ok(false)"
        );
    }

    /// With no erroring processor and an empty map, `has_next_log_record`
    /// returns `Ok(false)` cleanly (no false-positive error path).
    #[test]
    fn test_has_next_log_record_empty_map_is_ok_false() {
        let merger = BufferedRecordMergerFactory::create_with(
            "COMMIT_TIME_ORDERING",
            &std::collections::HashMap::new(),
        )
        .unwrap();
        let mut buffer = FileGroupRecordBuffer::new(
            "COMMIT_TIME_ORDERING".to_string(),
            merger,
            crate::file_group::reader_v2::update_processor::create_update_processor(false),
            None,
            SpillableRecordMap::new(),
        );
        assert!(matches!(buffer.has_next_log_record(), Ok(false)));
    }

    // ── Streamed base source + batch-ref interaction ──────

    fn id_batch(vals: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
    }

    fn buffer_with_source(source: Box<dyn RecordBatchReader + Send>) -> FileGroupRecordBuffer {
        let merger = BufferedRecordMergerFactory::create_with(
            "COMMIT_TIME_ORDERING",
            &std::collections::HashMap::new(),
        )
        .unwrap();
        let mut buffer = FileGroupRecordBuffer::new(
            "COMMIT_TIME_ORDERING".to_string(),
            merger,
            crate::file_group::reader_v2::update_processor::create_update_processor(false),
            None,
            SpillableRecordMap::new(),
        );
        buffer.base_file_source = Some(source);
        buffer
    }

    /// `next_base_row` lazily pulls a multi-row-group base source one
    /// row-group at a time, returning a `(Arc<RecordBatch>, row_idx)` into the
    /// *streamed* batch (zero-copy). Every row of a given row-group shares
    /// ONE `Arc` (the per-batch intern point → stable `Arc::as_ptr` for
    /// compaction grouping), and rows from a DIFFERENT row-group carry a
    /// DIFFERENT `Arc` — proving the source is decoded incrementally, not
    /// collected + concatenated into one batch.
    #[test]
    fn next_base_row_streams_row_groups_with_per_batch_arc_identity() {
        // Three "row groups" of 2 rows each.
        let schema = id_batch(&[0]).schema();
        let reader = arrow_array::RecordBatchIterator::new(
            vec![
                Ok(id_batch(&[1, 2])),
                Ok(id_batch(&[3, 4])),
                Ok(id_batch(&[5, 6])),
            ]
            .into_iter(),
            schema,
        );
        let mut buffer = buffer_with_source(Box::new(reader));

        let mut pulled: Vec<(usize, i64)> = Vec::new();
        // Retain each returned Arc clone for the whole run so freed-then-reused
        // allocator addresses can't alias distinct row-groups (the identity
        // check below relies on every live Arc keeping a stable address).
        let mut held: Vec<Arc<RecordBatch>> = Vec::new();
        while let Some((batch, row_idx)) = buffer.next_base_row().unwrap() {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            pulled.push((row_idx, col.value(row_idx)));
            held.push(batch);
        }

        // Full-data equivalence: all 6 rows in order, with the right row index
        // resetting per row-group.
        assert_eq!(pulled, vec![(0, 1), (1, 2), (0, 3), (1, 4), (0, 5), (1, 6)]);

        let arc_ptrs: Vec<*const RecordBatch> = held.iter().map(Arc::as_ptr).collect();
        // Per-batch Arc identity: the two rows of each row-group share one Arc;
        // the three row-groups produce three distinct Arcs.
        assert_eq!(
            arc_ptrs[0], arc_ptrs[1],
            "rows of row-group 0 share one Arc"
        );
        assert_eq!(
            arc_ptrs[2], arc_ptrs[3],
            "rows of row-group 1 share one Arc"
        );
        assert_eq!(
            arc_ptrs[4], arc_ptrs[5],
            "rows of row-group 2 share one Arc"
        );
        assert_ne!(
            arc_ptrs[0], arc_ptrs[2],
            "distinct row-groups → distinct Arcs"
        );
        assert_ne!(
            arc_ptrs[2], arc_ptrs[4],
            "distinct row-groups → distinct Arcs"
        );
        let distinct: std::collections::HashSet<_> = arc_ptrs.iter().collect();
        assert_eq!(
            distinct.len(),
            3,
            "exactly one interned Arc per streamed row-group (lazy decode, not collect+concat)"
        );
    }

    /// A source error mid-stream is PROPAGATED, not swallowed. The rows
    /// pulled before the error are yielded, then the call that hits the error
    /// returns `Err` (rather than `Ok(None)`, which would silently truncate the
    /// base file and drop rows — data loss). The source is released so a later
    /// call doesn't re-pull a closed stream. Parity with the vectorized
    /// `next_merged_base_batch`, which already errors.
    #[test]
    fn next_base_row_source_error_propagates_after_good_rows() {
        let schema = id_batch(&[0]).schema();
        let reader = arrow_array::RecordBatchIterator::new(
            vec![
                Ok(id_batch(&[10, 11])),
                Err(arrow_schema::ArrowError::ExternalError(
                    "simulated base row-group read failure".into(),
                )),
                Ok(id_batch(&[99])), // must never be reached
            ]
            .into_iter(),
            schema,
        );
        let mut buffer = buffer_with_source(Box::new(reader));

        // Pull rows until the error surfaces (must NOT be a clean end-of-stream).
        let mut seen: Vec<i64> = Vec::new();
        let err = loop {
            match buffer.next_base_row() {
                Ok(Some((batch, row_idx))) => {
                    let col = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    seen.push(col.value(row_idx));
                }
                Ok(None) => panic!("expected a source error, got clean end-of-stream"),
                Err(e) => break e,
            }
        };
        assert_eq!(
            seen,
            vec![10, 11],
            "the pre-error row-group is yielded before the error surfaces"
        );
        assert!(
            matches!(err, CoreError::ReadFileSliceError(_)),
            "base source error propagates loudly (not silently dropped): {err:?}"
        );
        assert!(
            buffer.base_file_source.is_none(),
            "source released on error so a later call doesn't re-pull a closed stream"
        );
        // Source released → subsequent calls are a clean end-of-stream.
        assert!(buffer.next_base_row().unwrap().is_none());
    }
}
