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

//! Ported from the merge-on-read reader. Nothing consumes it yet, so its
//! items are unreachable from the crate's call graph until the reader wires in.
#![allow(dead_code)]

//! Mirrors `org.apache.hudi.common.table.read.UpdateProcessor`.
//!
//! Strategy interface for processing record updates during the
//! base-file-vs-log merge phase. The default implementation passes through the
//! merged record and increments the read-stats counters (inserts / updates /
//! deletes), mirroring gold's `StandardUpdateProcessor`.

use super::buffered_record::BufferedRecord;
use crate::Result;
use std::sync::atomic::{AtomicU64, Ordering};

/// Per-merge counts of insert / update / delete operations.
///
/// Mirrors the `numInserts` / `numUpdates` / `numDeletes` that gold's
/// `StandardUpdateProcessor` increments on `HoodieReadStats`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UpdateStats {
    pub num_inserts: u64,
    pub num_updates: u64,
    pub num_deletes: u64,
}

/// Strategy for processing record updates during merge iteration.
///
/// Mirrors Java's `UpdateProcessor<T>` interface.
///
/// Created by factory method based on merge mode and configuration.
/// Wrapped optionally in a `CallbackProcessor` for update callbacks.
pub trait UpdateProcessor: Send + Sync + std::fmt::Debug {
    /// Process an update (base record merged with log record).
    ///
    /// `previous_record` is the pre-merge base record when this record came
    /// from a base+log merge (the update path), and `None` when it came from a
    /// log-only record (the insert path) — matching gold's
    /// `processUpdate(recordKey, previousRecord, mergedRecord, isDelete)`.
    ///
    /// Returns the record to emit, or `None` to skip.
    fn process_update(
        &self,
        record_key: &str,
        previous_record: Option<&BufferedRecord>,
        merged_record: &BufferedRecord,
        is_delete: bool,
    ) -> Result<Option<BufferedRecord>>;

    /// Snapshot the accumulated insert / update / delete counts.
    ///
    /// Mirrors the counters gold's `StandardUpdateProcessor` writes onto
    /// `HoodieReadStats` (`incrementNumInserts/Updates/Deletes`). The reader
    /// drains these into its `HoodieReadStats` after the merge completes.
    fn read_stats_counts(&self) -> UpdateStats {
        UpdateStats::default()
    }
}

/// Default update processor: passes through the merged record and counts
/// inserts / updates / deletes.
///
/// Corresponds to Java's `StandardUpdateProcessor<T>`
/// (`UpdateProcessor.java:75-120`). Counting uses interior mutability
/// (`AtomicU64`) because the processor is shared behind a `&self` trait
/// method, matching the way the buffer iterator calls `processUpdate` while
/// holding the processor by reference.
///
/// NOTE: `emit_delete` row-emission is intentionally NOT implemented here.
/// Gold's `emitDeletes` path emits a synthesized delete row
/// (`recordContext.getDeleteRow`) and tags `HoodieOperation`; that is a larger
/// change (delete-row synthesis + operation tagging) gated loudly at the reader
/// construction boundary (`HoodieFileGroupReader::new`). See the gaps registry.
#[derive(Debug, Default)]
pub struct StandardUpdateProcessor {
    num_inserts: AtomicU64,
    num_updates: AtomicU64,
    num_deletes: AtomicU64,
}

impl StandardUpdateProcessor {
    pub fn new() -> Self {
        Self::default()
    }
}

impl UpdateProcessor for StandardUpdateProcessor {
    fn process_update(
        &self,
        _record_key: &str,
        previous_record: Option<&BufferedRecord>,
        merged_record: &BufferedRecord,
        is_delete: bool,
    ) -> Result<Option<BufferedRecord>> {
        // Mirrors gold StandardUpdateProcessor.processUpdate (UpdateProcessor.java:88-119).
        if is_delete {
            // readStats.incrementNumDeletes(); emitDeletes is gated off upstream,
            // so a delete is always dropped from the output here.
            self.num_deletes.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        // handleNonDeletes: prev present → update; prev absent → insert.
        if previous_record.is_some() {
            self.num_updates.fetch_add(1, Ordering::Relaxed);
        } else {
            self.num_inserts.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Some(merged_record.clone()))
    }

    fn read_stats_counts(&self) -> UpdateStats {
        UpdateStats {
            num_inserts: self.num_inserts.load(Ordering::Relaxed),
            num_updates: self.num_updates.load(Ordering::Relaxed),
            num_deletes: self.num_deletes.load(Ordering::Relaxed),
        }
    }
}

/// Factory for creating `UpdateProcessor` instances.
///
/// `_emit_deletes` is always false in current execution — `emit_delete=true` is
/// rejected at buffer/loader.rs before this is reached; the parameter exists for
/// gold signature parity. The standard processor always drops deletes from the output.
pub fn create_update_processor(_emit_deletes: bool) -> Box<dyn UpdateProcessor> {
    Box::new(StandardUpdateProcessor::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::reader_v2::buffered_record::BufferedRecord;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_record(key: &str) -> BufferedRecord {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
        BufferedRecord::new_data(key.to_string(), batch, None)
    }

    /// gold StandardUpdateProcessor increments numInserts (prev=None),
    /// numUpdates (prev=Some), numDeletes (is_delete=true).
    #[test]
    fn test_standard_update_processor_counts_inserts_updates_deletes() {
        let proc = StandardUpdateProcessor::new();
        let rec = make_record("k1");

        // Insert: no previous record.
        let out = proc.process_update("k1", None, &rec, false).unwrap();
        assert!(out.is_some(), "insert should be emitted");

        // Two updates: previous record present.
        proc.process_update("k2", Some(&rec), &rec, false).unwrap();
        proc.process_update("k3", Some(&rec), &rec, false).unwrap();

        // Delete: dropped from output, counted.
        let del_out = proc.process_update("k4", Some(&rec), &rec, true).unwrap();
        assert!(
            del_out.is_none(),
            "delete must be dropped (emit_delete off)"
        );

        let counts = proc.read_stats_counts();
        assert_eq!(counts.num_inserts, 1, "one insert");
        assert_eq!(counts.num_updates, 2, "two updates");
        assert_eq!(counts.num_deletes, 1, "one delete");
    }
}
