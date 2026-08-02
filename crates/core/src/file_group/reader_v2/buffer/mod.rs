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
// Re-exports here serve buffer implementations that land in later changes.
#![allow(dead_code, unused_imports)]

//! Record buffer hierarchy for the file group reader.
//!
//! Mirrors the Java package `org.apache.hudi.common.table.read.buffer`.
//!
//! ## Interface Hierarchy (matches Java 1:1)
//!
//! ```text
//!   «trait» HoodieFileGroupRecordBuffer          (Java interface)
//!       │
//!       │ implements
//!       ▼
//!   FileGroupRecordBuffer  (common state struct)  (Java abstract class)
//!       │
//!       ├── KeyBasedFileGroupRecordBuffer          (KEY_BASED_MERGE, default)
//!       ├── [PositionBasedFileGroupRecordBuffer]    (future)
//!       ├── [SortedKeyBasedFileGroupRecordBuffer]   (future)
//!       └── [UnmergedFileGroupRecordBuffer]          (future)
//! ```

pub mod key_based;
pub mod loader;
pub mod position_based;
pub mod record_buffer;
pub mod record_positions;
pub mod row_extraction;
pub mod spillable_map;

pub use record_buffer::FileGroupRecordBuffer;
pub use spillable_map::{MergeMap, SpillableRecordMap};

use crate::Result;
use crate::file_group::log_file::log_block::LogBlock;
use crate::file_group::reader_v2::buffered_record::{BufferedRecord, DeleteRecord};
use crate::file_group::reader_v2::update_processor::UpdateStats;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

/// The type of merge buffer in use.
///
/// Mirrors Java's `HoodieFileGroupRecordBuffer.BufferType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferType {
    /// Key-based merge: deduplicates by record key, keeps latest by ordering value.
    KeyBasedMerge,
    /// Position-based merge (not yet implemented).
    PositionBasedMerge,
    /// Unmerged: skips merge (not yet implemented).
    Unmerged,
}

/// Trait for file group record buffers.
///
/// Mirrors Java's `HoodieFileGroupRecordBuffer<T>` interface.
///
/// Method names match Java 1:1:
/// - `processDataBlock` → `process_data_block`
/// - `processNextDataRecord` → `process_next_data_record`
/// - `processDeleteBlock` → `process_delete_block`
/// - `processNextDeletedRecord` → `process_next_deleted_record`
/// - `containsLogRecord` → `contains_log_record`
/// - `setBaseFileIterator` → `set_base_file_iterator` (legacy, default-impl)
///   and `set_base_file_source` (ENG-42992 lazy entry)
/// - `hasNext` / `next` → `has_next` / `next`
///
/// ENG-42992 — the trait used to require `Sync`. Dropped because the
/// buffer now holds a lazy `Box<dyn RecordBatchReader + Send>` for the
/// base file source, and arrow-rs's `RecordBatchReader` is `Send` but
/// not `Sync`. Nothing in the codebase ever shared a buffer through
/// `&buffer` across threads — every use is either owned (`Box<dyn ...>`)
/// or accessed through `&mut self` — so removing `Sync` is benign.
pub trait HoodieFileGroupRecordBuffer: Send + std::fmt::Debug {
    /// Returns the buffer type.
    fn get_buffer_type(&self) -> BufferType;

    /// Process a data block from log scanning.
    ///
    /// Mirrors Java's `processDataBlock(HoodieDataBlock, Option<KeySpec>)`.
    /// The buffer is responsible for inflating the block and extracting records,
    /// matching Java where inflate/deserialize/deflate happens inside the block
    /// triggered by the buffer's `getRecordsIterator`.
    fn process_data_block(&mut self, block: &mut LogBlock) -> Result<()>;

    /// Process a single data record within a data block.
    ///
    /// Mirrors Java's `processNextDataRecord(BufferedRecord<T>, Serializable)`.
    fn process_next_data_record(&mut self, record: BufferedRecord, key: &str) -> Result<()>;

    /// Process a delete block from log scanning.
    ///
    /// Mirrors Java's `processDeleteBlock(HoodieDeleteBlock)`.
    /// The buffer inflates the block and extracts delete records internally.
    fn process_delete_block(&mut self, block: &mut LogBlock) -> Result<()>;

    /// Process a single deleted record within a delete block.
    ///
    /// Mirrors Java's `processNextDeletedRecord(DeleteRecord, Serializable)`.
    /// Returns `Result` so spill-backend (RocksDB) I/O on the records map can be
    /// propagated rather than swallowed (A1).
    fn process_next_deleted_record(&mut self, delete_record: DeleteRecord, key: &str)
    -> Result<()>;

    /// Check if a record exists in the buffered records.
    ///
    /// Mirrors Java's `containsLogRecord(String)`.
    fn contains_log_record(&self, record_key: &str) -> bool;

    /// Returns the number of records in the buffer.
    fn size(&self) -> usize;

    /// Returns the total number of log records processed.
    fn get_total_log_records(&self) -> u64;

    /// Stage timing (perf harness): cumulative wall ms spent inflating /
    /// decoding log blocks inside `process_data_block` / `process_delete_block`.
    /// This is a SUBSET of the Pass-3 merge-insert window. Default 0 for buffers
    /// that don't instrument decode.
    fn stage_decode_ms(&self) -> u64 {
        0
    }

    /// Stage stat (perf harness): peak number of entries the merge map
    /// held during the scan. Default 0 for buffers that don't track it.
    fn merge_map_peak_entries(&self) -> u64 {
        0
    }

    /// Spill stat (A1, ENG-42993): true if the size-tracked merge map spilled
    /// any entry to disk during the scan. Default false for buffers without a
    /// spillable map. This is the M3 acceptance signal.
    fn merge_map_spilled(&self) -> bool {
        false
    }

    /// Spill stat (A1): peak in-memory byte estimate the merge map held during
    /// the scan (bounded by the merge budget). Default 0.
    fn merge_map_peak_in_memory_bytes(&self) -> u64 {
        0
    }

    /// The merge map's CURRENT tracked in-memory footprint (bytes) — the live
    /// resident heap right now, not the peak. This is the value a host memory
    /// manager (e.g. velox's `MemoryPool`) reserves against the hudi-rs reader
    /// (ENG-44436); the FFI reader-memory accessor forwards it. Default 0 for
    /// buffers without a spillable map. See
    /// [`SpillableRecordMap::current_in_memory_bytes`](super::spillable_map::SpillableRecordMap::current_in_memory_bytes)
    /// for exactly what is and isn't included.
    fn current_in_memory_bytes(&self) -> u64 {
        0
    }

    /// Snapshot the insert / update / delete counts the buffer's
    /// `UpdateProcessor` has accumulated so far (ENG-42991).
    ///
    /// With streaming output, the [`FileGroupMergeIterator`] drives
    /// `has_next/next` and the update processor increments these counters as a
    /// side effect; after the stream is exhausted the iterator reads them back
    /// through this accessor instead of through `merge_and_collect_with_stats`
    /// (which consumes the buffer). Defaults to zero for buffers that don't
    /// track update stats.
    ///
    /// [`FileGroupMergeIterator`]: crate::file_group::reader_v2::merge_iterator::FileGroupMergeIterator
    fn update_stats_snapshot(&self) -> UpdateStats {
        UpdateStats::default()
    }

    /// Returns the underlying records map.
    fn get_log_records(&self) -> &MergeMap;

    /// Set the reader schema for merge output.
    ///
    /// This schema is used as a fallback when no base file is present
    /// and the first record in the buffer is a delete (which has no schema).
    /// Mirrors Java's readerSchema from FileGroupReaderSchemaHandler.
    fn set_reader_schema(&mut self, schema: SchemaRef);

    /// Set the lazy base file source for merge iteration (ENG-42992).
    ///
    /// The buffer will pull one `RecordBatch` at a time from `source` as
    /// the merge loop calls `next_base_row` — never holding the entire
    /// base file in memory. For tests and the back-compat path use
    /// [`Self::set_base_file_iterator`], which wraps a `Vec<RecordBatch>`.
    fn set_base_file_source(&mut self, source: Box<dyn arrow_array::RecordBatchReader + Send>);

    /// Back-compat convenience: wrap a `Vec<RecordBatch>` in a
    /// `RecordBatchIterator` and hand it to [`Self::set_base_file_source`].
    ///
    /// Mirrors Java's `setBaseFileIterator(ClosableIterator<T>)`. Kept as
    /// a default-impl so the 14 existing test sites continue to compile
    /// unchanged.
    fn set_base_file_iterator(&mut self, batches: Vec<RecordBatch>) {
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| std::sync::Arc::new(arrow_schema::Schema::empty()));
        let iter = arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        self.set_base_file_source(Box::new(iter));
    }

    /// Compact sparsely-populated pinned source batches in the merge map (A2
    /// safety valve).
    ///
    /// A `BatchRef` entry keeps its whole source `Arc<RecordBatch>` alive, so a
    /// source batch with few surviving keys pins memory for its dead rows. This
    /// pass groups the live entries by source batch and, for any batch whose
    /// live-row ratio is below the buffer's compaction threshold, re-batches its
    /// survivors into one compact owned batch (via the Arrow `interleave` kernel)
    /// and repoints those entries — releasing the original batch.
    ///
    /// Called once after log scanning completes and before the drain. Default
    /// no-op for buffers that do not hold batch-ref payloads. Idempotent: running
    /// it twice compacts nothing the second time (already-compacted survivors are
    /// near-fully-live owned batches).
    fn compact_pinned_batches(&mut self) -> Result<()> {
        Ok(())
    }

    /// Check if next merged record exists.
    ///
    /// Mirrors Java's `hasNext()`.
    fn has_next(&mut self) -> Result<bool>;

    /// Return the next merged buffered record.
    ///
    /// Mirrors Java's `next()`.
    fn next(&mut self) -> Option<BufferedRecord>;

    /// Consume the buffer and produce the merged output as a `RecordBatch`
    /// together with the insert / update / delete counts accumulated by the
    /// update processor during the merge.
    ///
    /// This is Rust-specific — Java drives the `hasNext()`/`next()` iterator and
    /// the `UpdateProcessor` increments `HoodieReadStats` as a side effect; here
    /// we surface the counts so the reader can write them into its stats.
    fn merge_and_collect_with_stats(self: Box<Self>) -> Result<(RecordBatch, UpdateStats)>;

    /// Consume the buffer and produce just the merged output `RecordBatch`,
    /// discarding the merge stats. Convenience wrapper that discards stats; not
    /// called by the production read path (which uses merge_and_collect_with_stats).
    fn merge_and_collect(self: Box<Self>) -> Result<RecordBatch> {
        self.merge_and_collect_with_stats().map(|(batch, _)| batch)
    }

    /// Pull the next base file batch and merge it against the in-buffer log map.
    /// Returns:
    /// - `Ok(Some(batch))` — merged batch (kept base rows + log-side replacements
    ///   for keys whose log entry won the conflict). May have 0 rows if all base
    ///   rows in the source batch were deleted by log entries.
    /// - `Ok(None)` — the base source is exhausted; the caller should next call
    ///   [`Self::drain_log_only_inserts`] to flush remaining log-only inserts.
    ///
    /// Vectorized: extracts all keys+ordering values from `base` in one pass,
    /// builds a `BooleanArray` keep-mask, then uses
    /// `arrow::compute::filter_record_batch` to apply the mask in a single
    /// Arrow kernel call — vs. the legacy `next_base_row` path which does
    /// `batch.slice(idx, 1)` + per-row `BufferedRecord` allocation and a
    /// 4096-batch `concat_batches` per emitted chunk.
    ///
    /// `target_schema` is the schema the returned batch must conform to (used
    /// for reconcile when the base source's schema disagrees on nested-type
    /// child field names — e.g. Avro "element" vs Parquet "array").
    ///
    /// Empty input batches are skipped internally (loop pulls next non-empty
    /// batch from the source).
    fn next_merged_base_batch(&mut self, target_schema: &SchemaRef) -> Result<Option<RecordBatch>>;

    /// Drain remaining log-only records (keys never matched by any base row)
    /// as merged inserts. Returns `Ok(None)` if no log-only inserts remain
    /// (delete-only or fully consumed by base merge).
    ///
    /// Mutates self by `mem::take`-ing the records map. Subsequent calls
    /// return `Ok(None)` (idempotent).
    fn drain_log_only_inserts(&mut self, target_schema: &SchemaRef) -> Result<Option<RecordBatch>>;
}
