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

//! [ENG-42991] Streaming output for the file-group reader.
//!
//! The Java reader's `getClosableIterator()` returns a lazy iterator that
//! emits merged rows in chunks. `FileGroupMergeIterator` does the same — it
//! emits one merged chunk per `next()`, so the consumer (Velox
//! `HudiSplitReader::next()`) can free each chunk before the next is built,
//! instead of holding the entire merged `RecordBatch` resident at once.
//!
//! [`FileGroupMergeIterator`] is the streaming bridge:
//! a synchronous [`RecordBatchReader`] that emits one merged chunk per
//! `next()`. The async I/O (base file decode, log scan) happens once in
//! `HoodieFileGroupReader::open()`; from then on the iterator is pure
//! in-memory work (HashMap lookups + Arrow slicing). This matches the
//! FFI's `FFI_ArrowArrayStream` shape directly — no per-chunk `block_on`.
//!
//! ## Two source modes
//!
//! - [`MergeSource::Eager`] — used for the no-merge fast path (CoW or empty
//!   FG): the base file batches are already materialised, no log scan ran,
//!   no buffer was built. The iterator just yields each pre-computed batch
//!   through the [`OutputConverter`].
//! - [`MergeSource::Buffered`] — the MOR merge path: drives
//!   `buffer.has_next() / buffer.next()` until `batch_size` rows have
//!   accumulated, calls [`records_to_batch`] to build the chunk, applies
//!   the converter, and returns it.
//!
//! ## Schema lifecycle
//!
//! `RecordBatchReader::schema()` must report the schema callers will see in
//! every batch — so the iterator stores the **post-projection** schema. When
//! an [`OutputConverter`] is present, that comes from
//! `OutputConverter::target_schema()`. Otherwise it equals the merge schema
//! (= base file schema, or required_schema if set), determined in
//! `HoodieFileGroupReader::open()` using the same logic
//! `merge_and_collect` used to use.
//!
//! ## Default batch size
//!
//! [`DEFAULT_BATCH_SIZE`] = 4096 rows, matching Velox's typical operator
//! batch size. The Velox `HudiSplitReader::next(uint64_t size, ...)` API
//! exposes a requested size parameter; a follow-up will plumb that through
//! the FFI. For now the iterator uses its constructor-time `batch_size`.

use crate::Result;
use crate::error::CoreError;
use crate::file_group::reader_v2::buffer::HoodieFileGroupRecordBuffer;
use crate::file_group::reader_v2::output_converter::OutputConverter;
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Default chunk size (rows) emitted by [`FileGroupMergeIterator`].
///
/// Matches Velox's typical operator batch size; matches Spark Hudi's
/// `hoodie.parquet.batchsize.default`. Used as the fallback when
/// `hoodie.read.stream.batch_size` is unset/unparseable.
pub const DEFAULT_BATCH_SIZE: usize = 4096;

/// Stage stats the streaming iterator accumulates as it emits chunks
/// (ENG-42991).
///
/// The iterator owns the buffer once `open()` returns, so it accumulates these
/// per chunk through a shared [`StreamStatsHandle`]. The
/// reader drains them into its [`HoodieReadStats`] once the stream is
/// exhausted — see `HoodieFileGroupReader::drain_stream_stats`.
///
/// [`HoodieFileGroupReader::read`]: crate::file_group::reader_v2::HoodieFileGroupReader::read
/// [`HoodieReadStats`]: crate::file_group::reader_v2::read_stats::HoodieReadStats
#[derive(Debug, Default, Clone, Copy)]
pub struct StreamReadStats {
    /// Cumulative wall ms spent driving `buffer.has_next/next` (the base+log
    /// merge) across all chunks. Zero for the Eager (no-merge) source.
    pub final_merge_ms: u64,
    /// Cumulative wall ms spent turning merged records into the output batch:
    /// `records_to_batch` + the per-chunk `OutputConverter` projection.
    pub output_build_ms: u64,
    /// Peak number of entries the merge map held during the log scan. Recorded
    /// off the buffer by `HoodieFileGroupReader::open` before iteration starts
    /// (it is finalized at scan time, not during output streaming).
    pub merge_map_peak_entries: u64,
    /// Insert / update / delete counts the update processor accumulated while
    /// the iterator drove the merge. Snapshotted off the buffer when the
    /// stream is exhausted (Buffered source only).
    pub num_inserts: u64,
    pub num_updates: u64,
    pub num_deletes: u64,
}

/// Shared handle to the [`StreamReadStats`] sink.
///
/// `Arc<Mutex<…>>` (not `Rc<RefCell<…>>`) because the FFI path boxes the
/// iterator into an `FFI_ArrowArrayStream`, which requires `Send`. The lock is
/// taken once per emitted chunk — negligible against the per-chunk merge work.
pub type StreamStatsHandle = Arc<Mutex<StreamReadStats>>;

/// Create a fresh, zeroed [`StreamStatsHandle`].
pub fn new_stream_stats_handle() -> StreamStatsHandle {
    Arc::new(Mutex::new(StreamReadStats::default()))
}

/// Source of merged records for [`FileGroupMergeIterator`].
///
/// Variants reflect the two read shapes the FG reader supports:
/// no-merge (CoW or empty FG) vs full MOR merge.
enum MergeSource {
    /// No-merge path: a `RecordBatchReader` that yields the base file
    /// row-groups directly (ENG-42992 — lazy parquet stream wrapped as
    /// `ParquetSyncReader`, or a `RecordBatchIterator` over a pre-built
    /// `Vec<RecordBatch>` for tests / eager paths). One source batch =
    /// one emitted chunk.
    Eager {
        source: Box<dyn RecordBatchReader + Send>,
    },

    /// MOR merge path. Drives the buffer's vectorized
    /// `next_merged_base_batch` + `drain_log_only_inserts` methods:
    /// one emitted chunk per non-empty base source batch, plus a single
    /// final chunk for log-only inserts (keys never matched by any base row).
    ///
    /// `batch_size` is retained but unused on the merge path — it is kept
    /// in the public `new_buffered` signature for API compatibility with
    /// callers that still pass `DEFAULT_BATCH_SIZE`. Chunk size is now
    /// determined by the base source's own row-group cadence.
    Buffered {
        buffer: Box<dyn HoodieFileGroupRecordBuffer>,
        /// Schema the merged chunks conform to (used as `target_schema`
        /// when reconciling base batches with nested-type child field
        /// name drift, and as the schema for `records_to_batch` when the
        /// final drain emits log-only inserts).
        merge_schema: SchemaRef,
        /// Three-state machine: BaseScanning → DrainingLogInserts → Done.
        state: BufferedState,
    },
}

/// State for `MergeSource::Buffered::next()`.
enum BufferedState {
    /// Pulling base batches from the source and merging against the log map.
    BaseScanning,
    /// Base source exhausted; flush log-only inserts on the next call.
    /// (Single transition state — one call produces the final batch, then
    /// the iterator moves to `Done`.)
    DrainingLogInserts,
}

/// Streaming output for the file-group reader (ENG-42991).
///
/// Implements [`RecordBatchReader`] so it can be handed directly to
/// `FFI_ArrowArrayStream::new` for the C++ side, or used as a regular
/// `Iterator<Item = Result<RecordBatch, ArrowError>>` in Rust callers.
pub struct FileGroupMergeIterator {
    source: MergeSource,
    /// Schema reported via [`RecordBatchReader::schema`]. Equals the
    /// `target_schema` of the optional [`OutputConverter`], or the merge
    /// schema if no converter is configured.
    output_schema: SchemaRef,
    output_converter: Option<Box<dyn OutputConverter>>,
    /// Sticky termination — once any `next()` errors or `Eager.cursor`
    /// runs off the end, subsequent calls return `None`.
    done: bool,
    /// Shared stage-stats sink (ENG-42991). The iterator accumulates
    /// final_merge_ms + output_build_ms per chunk here, and on exhaustion
    /// snapshots the buffer's update-processor counts (Buffered source). The
    /// owning reader drains it into `HoodieReadStats` after `read()`.
    stream_stats: StreamStatsHandle,
}

impl std::fmt::Debug for FileGroupMergeIterator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match &self.source {
            MergeSource::Eager { .. } => "Eager(lazy)".to_string(),
            MergeSource::Buffered { buffer, state, .. } => {
                let state_label = match state {
                    BufferedState::BaseScanning => "BaseScanning",
                    BufferedState::DrainingLogInserts => "DrainingLogInserts",
                };
                format!(
                    "Buffered(buffer_size={}, state={state_label})",
                    buffer.size()
                )
            }
        };
        f.debug_struct("FileGroupMergeIterator")
            .field("source", &kind)
            .field("output_schema_cols", &self.output_schema.fields().len())
            .field("has_output_converter", &self.output_converter.is_some())
            .field("done", &self.done)
            .finish()
    }
}

impl FileGroupMergeIterator {
    /// Build an iterator over a `RecordBatchReader` base source (CoW / empty
    /// path). One source batch becomes one emitted chunk (after the
    /// optional `OutputConverter`).
    ///
    /// `output_schema` is the schema that will appear on every emitted
    /// `RecordBatch` — must equal the converter's `target_schema` when one
    /// is supplied, or the source's schema otherwise.
    pub fn new_eager(
        source: Box<dyn RecordBatchReader + Send>,
        output_schema: SchemaRef,
        output_converter: Option<Box<dyn OutputConverter>>,
        stream_stats: StreamStatsHandle,
    ) -> Self {
        Self {
            source: MergeSource::Eager { source },
            output_schema,
            output_converter,
            done: false,
            stream_stats,
        }
    }

    /// Convenience wrapper that wraps a `Vec<RecordBatch>` in a
    /// `RecordBatchIterator` and calls [`Self::new_eager`]. Mainly for
    /// tests + the back-compat path.
    pub fn new_eager_from_vec(
        batches: Vec<RecordBatch>,
        output_schema: SchemaRef,
        output_converter: Option<Box<dyn OutputConverter>>,
        stream_stats: StreamStatsHandle,
    ) -> Self {
        let source_schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| output_schema.clone());
        let iter = RecordBatchIterator::new(batches.into_iter().map(Ok), source_schema);
        Self::new_eager(
            Box::new(iter),
            output_schema,
            output_converter,
            stream_stats,
        )
    }

    /// Build an iterator that drives MOR merge from a populated buffer.
    ///
    /// `merge_schema` is the schema [`records_to_batch`] uses while
    /// concatenating BufferedRecords for a chunk (pre-projection).
    /// `output_schema` is the post-projection schema reported via
    /// [`RecordBatchReader::schema`].
    pub fn new_buffered(
        buffer: Box<dyn HoodieFileGroupRecordBuffer>,
        merge_schema: SchemaRef,
        output_schema: SchemaRef,
        output_converter: Option<Box<dyn OutputConverter>>,
        _batch_size: usize,
        stream_stats: StreamStatsHandle,
    ) -> Self {
        Self {
            source: MergeSource::Buffered {
                buffer,
                merge_schema,
                state: BufferedState::BaseScanning,
            },
            output_schema,
            output_converter,
            done: false,
            stream_stats,
        }
    }

    /// Add `final_merge_ms` / `output_build_ms` deltas to the shared stats sink.
    /// Called once per emitted chunk; the lock is held only for the add.
    fn record_chunk_timing(&self, merge_ms: u64, build_ms: u64) {
        if let Ok(mut s) = self.stream_stats.lock() {
            s.final_merge_ms = s.final_merge_ms.saturating_add(merge_ms);
            s.output_build_ms = s.output_build_ms.saturating_add(build_ms);
        }
    }

    /// The merge buffer's CURRENT tracked in-memory footprint, in bytes
    /// (ENG-44436). Delegates to the buffer's
    /// [`current_in_memory_bytes`](HoodieFileGroupRecordBuffer::current_in_memory_bytes)
    /// on the merge path; returns 0 for the Eager (no-merge) source, which holds
    /// no merge map. Used by the FFI reader-memory accessor so a host memory
    /// manager (velox's `MemoryPool`) can reserve against hudi-rs's live native
    /// footprint. Cheap (an `AtomicU64`/counter read on the buffer), safe to call
    /// between chunk pulls.
    #[must_use]
    pub fn current_in_memory_bytes(&self) -> u64 {
        match &self.source {
            MergeSource::Buffered { buffer, .. } => buffer.current_in_memory_bytes(),
            MergeSource::Eager { .. } => 0,
        }
    }

    /// Snapshot the buffer's update-processor insert/update/delete counts into
    /// the shared stats sink. Called once, when the stream is exhausted. No-op
    /// for the Eager source (no merge, no update processor).
    fn snapshot_update_stats(&self) {
        if let MergeSource::Buffered { buffer, .. } = &self.source {
            let counts = buffer.update_stats_snapshot();
            if let Ok(mut s) = self.stream_stats.lock() {
                s.num_inserts = counts.num_inserts;
                s.num_updates = counts.num_updates;
                s.num_deletes = counts.num_deletes;
            }
        }
    }

    /// Convenience for callers that want the legacy "one batch" behaviour
    /// (drives the iterator to completion and concatenates).
    ///
    /// Mirrors the historical `HoodieFileGroupReader::read()` return type.
    pub fn collect_into_one_batch(mut self) -> Result<RecordBatch> {
        let output_schema = self.output_schema.clone();
        let mut chunks: Vec<RecordBatch> = Vec::new();
        for next in &mut self {
            let batch = next.map_err(|e| {
                CoreError::ReadFileSliceError(format!("FileGroupMergeIterator yielded error: {e}"))
            })?;
            chunks.push(batch);
        }
        if chunks.is_empty() {
            return Ok(RecordBatch::new_empty(output_schema));
        }
        if chunks.len() == 1 {
            return Ok(chunks.into_iter().next().unwrap());
        }
        arrow::compute::concat_batches(&output_schema, &chunks).map_err(|e| {
            CoreError::ReadFileSliceError(format!(
                "Failed to concat streaming chunks back into one batch: {e}"
            ))
        })
    }

    /// Apply the configured [`OutputConverter`] (if any) and convert the
    /// internal `CoreError` into `ArrowError` for the
    /// `RecordBatchReader::next` contract.
    fn finish_chunk(&self, batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
        match &self.output_converter {
            Some(converter) => converter.apply(batch).map_err(core_to_arrow_err),
            None => Ok(batch),
        }
    }
}

impl Iterator for FileGroupMergeIterator {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        // Step 1: produce the next pre-projection chunk from the source, with
        // its merge + build timing. Done inside the `&mut self.source` borrow,
        // which must end before we touch `&self`-borrowing helpers
        // (`finish_chunk`, `record_chunk_timing`, `snapshot_update_stats`).
        // Returns:
        //   Ok(Some((batch, merge_ms, records_build_ms))) — emit this chunk
        //   Ok(None)                                       — stream exhausted
        //   Err((e, merge_ms, build_ms))                   — terminal error
        type Chunk = (RecordBatch, u64, u64);
        let produced: Result<Option<Chunk>, (CoreError, u64, u64)> = match &mut self.source {
            MergeSource::Eager { source } => {
                // A3 (ENG-42992): one source batch = one emitted chunk. The
                // source is a `RecordBatchReader` — lazy (`ParquetSyncReader`
                // doing block_on per row-group) or eager (`RecordBatchIterator`
                // over a Vec for tests / instant-range-filter fallback). The
                // CoW / empty-FG no-merge path never materializes the whole
                // base file; it pulls one row-group at a time.
                match source.next() {
                    None => Ok(None),
                    // Eager has no merge work; build timing is the converter only.
                    Some(Ok(batch)) => Ok(Some((batch, 0, 0))),
                    Some(Err(e)) => Err((CoreError::ArrowError(e), 0, 0)),
                }
            }

            MergeSource::Buffered {
                buffer,
                merge_schema,
                state,
            } => {
                // Vectorized merge path (ENG-43009): pull the next merged base
                // batch, or drain log-only inserts, then hand it to the shared
                // timing / projection / exhaustion tail below (Step 2) so
                // HEAD's stream-stats timing is preserved. Loop to skip base
                // chunks fully eliminated by log deletes.
                let merge_start = Instant::now();
                let mut chunk_err: Option<CoreError> = None;
                let mut out_batch: Option<RecordBatch> = None;
                loop {
                    match state {
                        BufferedState::BaseScanning => {
                            match buffer.next_merged_base_batch(merge_schema) {
                                Ok(Some(b)) => {
                                    if b.num_rows() == 0 {
                                        // All base rows in this source batch lost
                                        // to a log delete — pull the next.
                                        continue;
                                    }
                                    out_batch = Some(b);
                                    break;
                                }
                                Ok(None) => {
                                    // Base source exhausted — drain log-only
                                    // inserts on the next loop turn.
                                    *state = BufferedState::DrainingLogInserts;
                                    continue;
                                }
                                Err(e) => {
                                    chunk_err = Some(e);
                                    break;
                                }
                            }
                        }
                        BufferedState::DrainingLogInserts => {
                            // drain_log_only_inserts is idempotent: it returns the
                            // remaining log-only inserts once, then Ok(None).
                            match buffer.drain_log_only_inserts(merge_schema) {
                                Ok(Some(b)) if b.num_rows() > 0 => {
                                    out_batch = Some(b);
                                    break;
                                }
                                Ok(_) => break,
                                Err(e) => {
                                    chunk_err = Some(e);
                                    break;
                                }
                            }
                        }
                    }
                }
                let merge_ms = merge_start.elapsed().as_millis() as u64;
                if let Some(e) = chunk_err {
                    Err((e, merge_ms, 0))
                } else if let Some(b) = out_batch {
                    Ok(Some((b, merge_ms, 0)))
                } else {
                    // Exhausted. Record the merge probe time before the
                    // exhaustion handler (Step 2) runs.
                    if let Ok(mut s) = self.stream_stats.lock() {
                        s.final_merge_ms = s.final_merge_ms.saturating_add(merge_ms);
                    }
                    Ok(None)
                }
            }
        };

        // Step 2: the source borrow has ended — handle timing / projection /
        // exhaustion via `&self` helpers.
        match produced {
            Ok(Some((batch, merge_ms, records_build_ms))) => {
                // output_build_ms part 2: the per-chunk OutputConverter.
                let convert_start = Instant::now();
                let out = self.finish_chunk(batch);
                let build_ms = records_build_ms + convert_start.elapsed().as_millis() as u64;
                self.record_chunk_timing(merge_ms, build_ms);
                if out.is_err() {
                    self.done = true;
                }
                Some(out)
            }
            Ok(None) => {
                self.done = true;
                // Stream exhausted → snapshot the update-processor counts off
                // the buffer (Buffered source only; no-op for Eager).
                self.snapshot_update_stats();
                None
            }
            Err((e, merge_ms, build_ms)) => {
                self.done = true;
                self.record_chunk_timing(merge_ms, build_ms);
                Some(Err(core_to_arrow_err(e)))
            }
        }
    }
}

impl RecordBatchReader for FileGroupMergeIterator {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

fn core_to_arrow_err(e: CoreError) -> ArrowError {
    ArrowError::ExternalError(Box::new(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::reader_v2::buffer::{BufferType, HoodieFileGroupRecordBuffer, MergeMap};
    use crate::file_group::reader_v2::buffered_record::{BufferedRecord, DeleteRecord};
    use crate::file_group::reader_v2::update_processor::UpdateStats;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::collections::VecDeque;
    use std::sync::Arc;

    fn small_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]))
    }

    /// A single-row `(key, v)` `BufferedRecord` whose payload is a zero-copy
    /// `BatchRef` into a shared source batch — exactly how
    /// `KeyBasedFileGroupRecordBuffer` stores records post-A2
    /// (`process_next_data_record` keeps the `BatchRef`, no IPC serialization).
    /// Exercising the in-memory representation here guards the streaming
    /// iterator's drain against the real payload shape.
    fn batch_ref_record(schema: &SchemaRef, key: &str, v: i32) -> BufferedRecord {
        let b = std::sync::Arc::new(batch(schema.clone(), &[key], &[v]));
        let r = BufferedRecord::new_batch_ref(key.to_string(), b, 0, None);
        // Sanity: the payload is a zero-copy batch ref (A2), not an IPC blob.
        assert!(matches!(
            r.payload,
            crate::file_group::reader_v2::buffered_record::RecordPayload::BatchRef { .. }
        ));
        r
    }

    fn delete_record(key: &str) -> BufferedRecord {
        BufferedRecord::new_delete(key.to_string(), None)
    }

    /// Minimal `HoodieFileGroupRecordBuffer` that replays a fixed queue of
    /// already-merged `BufferedRecord`s, mirroring what a real buffer hands the
    /// streaming iterator post-merge. Only the methods the iterator actually
    /// calls (`has_next` / `next` / `size` / `update_stats_snapshot`) carry
    /// behaviour; the rest are unreachable in this test path.
    #[derive(Debug)]
    struct MockBuffer {
        queue: VecDeque<BufferedRecord>,
        stats: UpdateStats,
        empty_map: MergeMap,
    }

    impl MockBuffer {
        fn boxed(
            records: Vec<BufferedRecord>,
            stats: UpdateStats,
        ) -> Box<dyn HoodieFileGroupRecordBuffer> {
            Box::new(Self {
                queue: records.into(),
                stats,
                empty_map: HashMap::default(),
            })
        }
    }

    impl HoodieFileGroupRecordBuffer for MockBuffer {
        fn get_buffer_type(&self) -> BufferType {
            BufferType::KeyBasedMerge
        }
        fn process_data_block(
            &mut self,
            _block: &mut crate::file_group::log_file::log_block::LogBlock,
        ) -> Result<()> {
            unreachable!("mock buffer is pre-populated")
        }
        fn process_next_data_record(&mut self, _record: BufferedRecord, _key: &str) -> Result<()> {
            unreachable!("mock buffer is pre-populated")
        }
        fn process_delete_block(
            &mut self,
            _block: &mut crate::file_group::log_file::log_block::LogBlock,
        ) -> Result<()> {
            unreachable!("mock buffer is pre-populated")
        }
        fn process_next_deleted_record(
            &mut self,
            _delete_record: DeleteRecord,
            _key: &str,
        ) -> Result<()> {
            unreachable!("mock buffer is pre-populated")
        }
        fn contains_log_record(&self, _record_key: &str) -> bool {
            false
        }
        fn size(&self) -> usize {
            self.queue.len()
        }
        fn get_total_log_records(&self) -> u64 {
            0
        }
        fn update_stats_snapshot(&self) -> UpdateStats {
            self.stats
        }
        fn get_log_records(&self) -> &MergeMap {
            &self.empty_map
        }
        fn set_reader_schema(&mut self, _schema: SchemaRef) {}
        fn set_base_file_source(
            &mut self,
            _source: Box<dyn arrow_array::RecordBatchReader + Send>,
        ) {
        }
        fn has_next(&mut self) -> Result<bool> {
            Ok(!self.queue.is_empty())
        }
        fn next(&mut self) -> Option<BufferedRecord> {
            self.queue.pop_front()
        }
        fn merge_and_collect_with_stats(self: Box<Self>) -> Result<(RecordBatch, UpdateStats)> {
            unreachable!("streaming path does not call merge_and_collect_with_stats")
        }
        fn next_merged_base_batch(
            &mut self,
            _target_schema: &SchemaRef,
        ) -> Result<Option<RecordBatch>> {
            // The mock has no base file source; every pre-populated record is
            // surfaced via `drain_log_only_inserts` (log-only-insert path).
            Ok(None)
        }
        fn drain_log_only_inserts(
            &mut self,
            target_schema: &SchemaRef,
        ) -> Result<Option<RecordBatch>> {
            if self.queue.is_empty() {
                return Ok(None);
            }
            let records: Vec<BufferedRecord> = std::mem::take(&mut self.queue)
                .into_iter()
                .filter(|r| !r.is_delete())
                .collect();
            if records.is_empty() {
                return Ok(None);
            }
            let batch = crate::file_group::reader_v2::buffer::row_extraction::records_to_batch(
                records,
                target_schema.clone(),
            )?;
            if batch.num_rows() == 0 {
                Ok(None)
            } else {
                Ok(Some(batch))
            }
        }
    }

    /// Drive a Buffered iterator to exhaustion, returning the per-chunk row
    /// counts and the flattened `(key, v)` rows across all chunks (in order).
    fn drain_buffered(
        records: Vec<BufferedRecord>,
        schema: SchemaRef,
        batch_size: usize,
        stats: UpdateStats,
    ) -> (Vec<usize>, Vec<(String, i32)>, StreamStatsHandle) {
        let handle = new_stream_stats_handle();
        let it = FileGroupMergeIterator::new_buffered(
            MockBuffer::boxed(records, stats),
            schema.clone(),
            schema.clone(),
            None,
            batch_size,
            handle.clone(),
        );
        let mut chunk_rows = Vec::new();
        let mut all_rows = Vec::new();
        for chunk in it {
            let b = chunk.unwrap();
            assert_eq!(b.schema(), schema, "every chunk reports the output schema");
            chunk_rows.push(b.num_rows());
            all_rows.extend(rows(&b));
        }
        (chunk_rows, all_rows, handle)
    }

    fn batch(schema: SchemaRef, keys: &[&str], vs: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())) as _,
                Arc::new(Int32Array::from(vs.to_vec())) as _,
            ],
        )
        .unwrap()
    }

    /// Extract `(key, v)` rows from a batch (small_schema layout) for full-data
    /// assertions per codeQuality/guide.md §5.
    fn rows(b: &RecordBatch) -> Vec<(String, i32)> {
        let keys = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let vs = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        (0..b.num_rows())
            .map(|i| (keys.value(i).to_string(), vs.value(i)))
            .collect()
    }

    /// Eager mode yields each input batch unchanged when no converter is set;
    /// assert the full row contents, not just counts.
    #[test]
    fn eager_yields_each_batch() {
        let schema = small_schema();
        let b1 = batch(schema.clone(), &["a", "b"], &[1, 2]);
        let b2 = batch(schema.clone(), &["c"], &[3]);
        let it = FileGroupMergeIterator::new_eager_from_vec(
            vec![b1, b2],
            schema.clone(),
            None,
            new_stream_stats_handle(),
        );
        assert_eq!(it.schema(), schema);
        let out: Vec<RecordBatch> = it.map(|r| r.unwrap()).collect();
        assert_eq!(out.len(), 2);
        assert_eq!(rows(&out[0]), vec![("a".into(), 1), ("b".into(), 2)]);
        assert_eq!(rows(&out[1]), vec![("c".into(), 3)]);
    }

    /// Eager + collect_into_one_batch concatenates correctly (full data).
    #[test]
    fn eager_collect_into_one_batch_concats() {
        let schema = small_schema();
        let b1 = batch(schema.clone(), &["a", "b"], &[1, 2]);
        let b2 = batch(schema.clone(), &["c"], &[3]);
        let it = FileGroupMergeIterator::new_eager_from_vec(
            vec![b1, b2],
            schema,
            None,
            new_stream_stats_handle(),
        );
        let result = it.collect_into_one_batch().unwrap();
        assert_eq!(
            rows(&result),
            vec![("a".into(), 1), ("b".into(), 2), ("c".into(), 3)]
        );
    }

    /// Eager with no input → empty batch w/ the requested schema.
    #[test]
    fn eager_empty_yields_zero_rows_via_collect() {
        let schema = small_schema();
        let it = FileGroupMergeIterator::new_eager_from_vec(
            vec![],
            schema.clone(),
            None,
            new_stream_stats_handle(),
        );
        let result = it.collect_into_one_batch().unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.schema(), schema);
    }

    // ---- Buffered (MOR merge) variant: chunking + full-data equivalence ----

    /// Build N binary-payload records `(k0,0)..(k{N-1},N-1)` for chunking tests.
    fn batch_ref_seq(schema: &SchemaRef, n: usize) -> Vec<BufferedRecord> {
        (0..n)
            .map(|i| batch_ref_record(schema, &format!("k{i:04}"), i as i32))
            .collect()
    }

    fn expected_seq(n: usize) -> Vec<(String, i32)> {
        (0..n).map(|i| (format!("k{i:04}"), i as i32)).collect()
    }

    /// 10 records → all rows preserved in order. (ENG-43009 vectorized: chunk
    /// cadence now follows the base source's batches, not `batch_size`, so we
    /// assert total-row + ordered-content parity rather than a per-chunk shape.)
    #[test]
    fn stream_buffered_chunking_row_counts_and_partial_tail() {
        let schema = small_schema();
        let (chunk_rows, all_rows, _) = drain_buffered(
            batch_ref_seq(&schema, 10),
            schema.clone(),
            4,
            UpdateStats::default(),
        );
        assert_eq!(
            chunk_rows.iter().sum::<usize>(),
            10,
            "all rows emitted across chunks"
        );
        assert_eq!(
            all_rows,
            expected_seq(10),
            "full-data equivalence (ordered)"
        );
    }

    /// Small `batch_size` hint → full data preserved (ENG-43009 vectorized: the
    /// hint no longer forces singleton chunks; chunking follows the base source).
    #[test]
    fn stream_buffered_batch_size_one_yields_singleton_chunks() {
        let schema = small_schema();
        let (chunk_rows, all_rows, _) = drain_buffered(
            batch_ref_seq(&schema, 5),
            schema.clone(),
            1,
            UpdateStats::default(),
        );
        assert_eq!(chunk_rows.iter().sum::<usize>(), 5, "all rows emitted");
        assert_eq!(all_rows, expected_seq(5));
    }

    /// batch_size larger than the record count → a single chunk holding all
    /// rows (shape-equivalent to the legacy single-batch `read()` output).
    #[test]
    fn stream_buffered_single_chunk_when_batch_size_exceeds_count() {
        let schema = small_schema();
        let (chunk_rows, all_rows, _) = drain_buffered(
            batch_ref_seq(&schema, 7),
            schema.clone(),
            1_000_000,
            UpdateStats::default(),
        );
        assert_eq!(chunk_rows, vec![7]);
        assert_eq!(all_rows, expected_seq(7));
    }

    /// Empty file group (no records) → zero chunks, and the update-stats
    /// snapshot still fires (all-zero) on exhaustion.
    #[test]
    fn stream_buffered_empty_yields_no_chunks() {
        let schema = small_schema();
        let (chunk_rows, all_rows, handle) =
            drain_buffered(vec![], schema, 4, UpdateStats::default());
        assert!(chunk_rows.is_empty(), "no records → no chunks");
        assert!(all_rows.is_empty());
        let s = *handle.lock().unwrap();
        assert_eq!((s.num_inserts, s.num_updates, s.num_deletes), (0, 0, 0));
    }

    /// Deletes-only buffer → every record is skipped, no output rows, no chunks.
    /// Mirrors the Java reader and the legacy `merge_and_collect` path (deletes
    /// contribute no output row).
    #[test]
    fn stream_buffered_deletes_only_yields_no_rows() {
        let schema = small_schema();
        let records = vec![
            delete_record("k0"),
            delete_record("k1"),
            delete_record("k2"),
        ];
        let (chunk_rows, all_rows, _) = drain_buffered(records, schema, 4, UpdateStats::default());
        assert!(chunk_rows.is_empty(), "all-deletes → no emitted chunk");
        assert!(all_rows.is_empty());
    }

    /// Deletes interleaved with data → deletes drop out, data rows survive in
    /// order, and chunk boundaries are counted on EMITTED (data) rows only.
    #[test]
    fn stream_buffered_interleaved_deletes_drop_out() {
        let schema = small_schema();
        let records = vec![
            batch_ref_record(&schema, "k0", 0),
            delete_record("d0"),
            batch_ref_record(&schema, "k1", 1),
            batch_ref_record(&schema, "k2", 2),
            delete_record("d1"),
            batch_ref_record(&schema, "k3", 3),
        ];
        // 4 surviving data rows (2 deletes dropped); chunk cadence follows the
        // base source (ENG-43009 vectorized), so assert the surviving-row total.
        let (chunk_rows, all_rows, _) =
            drain_buffered(records, schema.clone(), 2, UpdateStats::default());
        assert_eq!(
            chunk_rows.iter().sum::<usize>(),
            4,
            "4 data rows survive, deletes dropped"
        );
        assert_eq!(
            all_rows,
            vec![
                ("k0".into(), 0),
                ("k1".into(), 1),
                ("k2".into(), 2),
                ("k3".into(), 3),
            ]
        );
    }

    /// On exhaustion the iterator snapshots the buffer's update-processor
    /// insert/update/delete counts into the shared stats sink (ENG-42991 stats
    /// lifecycle — the streaming path can no longer drain them via
    /// `merge_and_collect_with_stats`).
    #[test]
    fn stream_buffered_snapshots_update_stats_on_exhaustion() {
        let schema = small_schema();
        let stats = UpdateStats {
            num_inserts: 3,
            num_updates: 5,
            num_deletes: 2,
        };
        let (_, _, handle) = drain_buffered(batch_ref_seq(&schema, 4), schema, 4, stats);
        let s = *handle.lock().unwrap();
        assert_eq!(s.num_inserts, 3);
        assert_eq!(s.num_updates, 5);
        assert_eq!(s.num_deletes, 2);
    }

    /// Full-data equivalence across chunk sizes: the iterator-collected output
    /// is identical (ordered, row-for-row) regardless of `batch_size`, matching
    /// the legacy single-batch semantics. This is the streaming-vs-collect
    /// equivalence guarantee (codeQuality/guide.md §5: assert the full dataset).
    #[test]
    fn stream_buffered_full_data_equivalent_across_chunk_sizes() {
        let schema = small_schema();
        let n = 23;
        let reference = expected_seq(n);
        for bs in [1usize, 2, 3, 5, 8, n - 1, n, n + 1, 1000] {
            let (_, all_rows, _) = drain_buffered(
                batch_ref_seq(&schema, n),
                schema.clone(),
                bs,
                UpdateStats::default(),
            );
            assert_eq!(
                all_rows, reference,
                "iterator-collected output must match the single-batch reference at batch_size={bs}"
            );
        }
    }

    // ---- Error/termination spine ----

    /// A buffer whose `has_next` always errors — pins the iterator's
    /// error/termination behavior (surface once, then fuse).
    #[derive(Debug)]
    struct ErrOnHasNext {
        empty_map: MergeMap,
    }

    impl HoodieFileGroupRecordBuffer for ErrOnHasNext {
        fn get_buffer_type(&self) -> BufferType {
            BufferType::KeyBasedMerge
        }
        fn process_data_block(
            &mut self,
            _block: &mut crate::file_group::log_file::log_block::LogBlock,
        ) -> Result<()> {
            unreachable!("has_next errors before any block is processed")
        }
        fn process_next_data_record(&mut self, _record: BufferedRecord, _key: &str) -> Result<()> {
            unreachable!("has_next errors before any record is processed")
        }
        fn process_delete_block(
            &mut self,
            _block: &mut crate::file_group::log_file::log_block::LogBlock,
        ) -> Result<()> {
            unreachable!("has_next errors before any block is processed")
        }
        fn process_next_deleted_record(
            &mut self,
            _delete_record: DeleteRecord,
            _key: &str,
        ) -> Result<()> {
            unreachable!("has_next errors before any record is processed")
        }
        fn contains_log_record(&self, _record_key: &str) -> bool {
            false
        }
        fn size(&self) -> usize {
            0
        }
        fn get_total_log_records(&self) -> u64 {
            0
        }
        fn get_log_records(&self) -> &MergeMap {
            &self.empty_map
        }
        fn set_reader_schema(&mut self, _schema: SchemaRef) {}
        fn set_base_file_source(
            &mut self,
            _source: Box<dyn arrow_array::RecordBatchReader + Send>,
        ) {
        }
        fn has_next(&mut self) -> Result<bool> {
            Err(crate::error::CoreError::ReadFileSliceError("boom".into()))
        }
        fn next(&mut self) -> Option<BufferedRecord> {
            None
        }
        fn next_merged_base_batch(
            &mut self,
            _target_schema: &SchemaRef,
        ) -> Result<Option<RecordBatch>> {
            // merge-perf's state-machine iterator drives the buffer via this
            // method first (BaseScanning); error here to exercise the
            // surface-once-then-fuse contract on the current API.
            Err(crate::error::CoreError::ReadFileSliceError("boom".into()))
        }
        fn drain_log_only_inserts(
            &mut self,
            _target_schema: &SchemaRef,
        ) -> Result<Option<RecordBatch>> {
            Ok(None)
        }
        fn merge_and_collect_with_stats(self: Box<Self>) -> Result<(RecordBatch, UpdateStats)> {
            unreachable!("streaming path does not call merge_and_collect_with_stats")
        }
    }

    /// A `has_next` error surfaces exactly once as `Some(Err(_))`, then the
    /// iterator fuses (sticky `done`) — the contract every A1–A6 PR leans on.
    #[test]
    fn buffered_surfaces_error_then_fuses() {
        let schema = small_schema();
        let buffer = Box::new(ErrOnHasNext {
            empty_map: HashMap::default(),
        });
        let mut it = FileGroupMergeIterator::new_buffered(
            buffer,
            schema.clone(),
            schema,
            None,
            4,
            new_stream_stats_handle(),
        );
        // The buffer's `ReadFileSliceError` surfaces once, preserved through the
        // `ArrowError::ExternalError` wrapper (not swapped for another variant).
        match it.next() {
            Some(Err(ArrowError::ExternalError(e))) => match e.downcast_ref::<CoreError>() {
                Some(CoreError::ReadFileSliceError(msg)) => assert_eq!(msg.as_str(), "boom"),
                other => panic!("expected CoreError::ReadFileSliceError, got {other:?}"),
            },
            other => panic!("expected Some(Err(ArrowError::ExternalError)), got {other:?}"),
        }
        assert!(
            it.next().is_none(),
            "iterator fuses after an error (sticky done)"
        );
    }

    // ---- OutputConverter (projection) path ----

    /// The `OutputConverter` path: `schema()` reports the converter's target
    /// schema BEFORE any chunk is produced (Velox calls `get_schema` before the
    /// first `get_next`), and the projection is applied per chunk across >1
    /// chunk — the project-per-chunk contract that replaced the old
    /// project-once-over-the-whole-batch path.
    #[test]
    fn buffered_output_converter_projects_per_chunk() {
        let merge_schema = small_schema(); // (key: Utf8, v: Int32)
        // Target drops `key`, keeping only `v`.
        let target_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let converter = crate::file_group::reader_v2::output_converter::ProjectionConverter::new(
            &target_schema,
        );
        // 5 records, batch_size 2 → 3 chunks, so per-chunk projection is exercised.
        let it = FileGroupMergeIterator::new_buffered(
            MockBuffer::boxed(batch_ref_seq(&merge_schema, 5), UpdateStats::default()),
            merge_schema,
            target_schema.clone(),
            Some(Box::new(converter)),
            2,
            new_stream_stats_handle(),
        );
        // schema() reports the post-projection target BEFORE iterating.
        assert_eq!(
            it.schema(),
            target_schema,
            "schema() reports the converter target before the first chunk"
        );

        let chunks: Vec<RecordBatch> = it.map(|r| r.unwrap()).collect();
        // merge-perf's MockBuffer drains its log-only inserts in one batch, so
        // the projection may land in a single chunk here; assert projection is
        // applied (schema + values below) rather than a specific chunk count.
        assert!(!chunks.is_empty(), "projection yields at least one chunk");
        for c in &chunks {
            assert_eq!(
                c.schema(),
                target_schema,
                "each chunk is in the target schema"
            );
        }
        // Concatenated projected output == the `v` column of the input, in order.
        let got: Vec<i32> = chunks
            .iter()
            .flat_map(|c| {
                let vs = c.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                (0..c.num_rows()).map(|i| vs.value(i)).collect::<Vec<_>>()
            })
            .collect();
        let want: Vec<i32> = expected_seq(5).into_iter().map(|(_, v)| v).collect();
        assert_eq!(got, want, "per-chunk projection preserves values in order");
    }

    /// A3 (ENG-42992) — the Eager (no-merge) source works with an arbitrary
    /// `RecordBatchReader`, not just a Vec. Models the lazy base-file path
    /// where the FG reader hands a `ParquetSyncReader` (or similar) to
    /// `new_eager` directly, pulling one row-group per chunk.
    #[test]
    fn eager_accepts_arbitrary_record_batch_reader() {
        let schema = small_schema();
        let b1 = batch(schema.clone(), &["a", "b"], &[1, 2]);
        let b2 = batch(schema.clone(), &["c"], &[3]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(b1), Ok(b2)].into_iter(), schema.clone());
        let it = FileGroupMergeIterator::new_eager(
            Box::new(reader),
            schema.clone(),
            None,
            new_stream_stats_handle(),
        );
        let out: Vec<RecordBatch> = it.map(|r| r.unwrap()).collect();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].num_rows() + out[1].num_rows(), 3);
    }
}
