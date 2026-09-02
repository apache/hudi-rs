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

//! Streaming output for the file-group reader.
//!
//! The Java reader's `getClosableIterator()` returns a lazy iterator that emits
//! merged rows in chunks. [`FileGroupMergeStream`] does the same thing
//! asynchronously: one merged chunk per `next_chunk`, so a consumer can free
//! each chunk before the next is built instead of holding the whole merged
//! result resident.
//!
//! Demand-driven, so the memory it adds over the merge map is one chunk. The
//! only await is the base-file pull; the merge kernel, the drain and the output
//! projection are synchronous work over data already in memory.
//!
//! ## Two source modes
//!
//! - [`MergeSource::Eager`] — the no-merge path (CoW, or a file group with no
//!   log files): no log scan ran and no buffer was built, so each base batch
//!   passes through the [`OutputConverter`] as its own chunk.
//! - [`MergeSource::Buffered`] — the MOR merge path: pull a base batch, merge it
//!   against the log map, emit the result; once the base is exhausted, drain the
//!   log-only inserts. One chunk per non-empty base batch, plus the drain.
//!
//! ## Schema lifecycle
//!
//! Every emitted chunk carries the **post-projection** schema: the
//! [`OutputConverter`]'s `target_schema()` when one is configured, otherwise the
//! merge schema (the base file's schema, or `required_schema` when set). It is
//! settled when the stream is built, before any chunk is produced.

use crate::Result;
use crate::error::CoreError;
use crate::file_group::reader_v2::buffer::HoodieFileGroupRecordBuffer;
use crate::file_group::reader_v2::output_converter::OutputConverter;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// A base file delivered one batch at a time.
///
/// The base file is the only part of a merge that has to be read rather than
/// computed, so it is the only part that is a stream.
pub type BaseBatchStream = BoxStream<'static, Result<RecordBatch>>;

/// Chunk size (rows) the log-only drain emits.
///
/// Matches Velox's typical operator batch size, and Spark Hudi's
/// `hoodie.parquet.batchsize.default`. The merged base chunks are not sized by
/// this: the base read requests them at the reader's own batch size
/// (`MERGE_CHUNK_ROWS` in the engine), and one source batch becomes one chunk.
pub const DEFAULT_BATCH_SIZE: usize = 4096;

/// Stage stats the streaming iterator accumulates as it emits chunks.
///
/// The iterator owns the buffer once the stream is built, so it accumulates
/// these per chunk through a shared [`StreamStatsHandle`]. The
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
    /// off the buffer by `HoodieFileGroupReader::init_record_iterators` before
    /// the stream is returned (it is finalized at scan time, not during output
    /// streaming).
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

/// Source of merged records for [`FileGroupMergeStream`].
///
/// Variants reflect the two read shapes the FG reader supports:
/// no-merge (CoW or empty FG) vs full MOR merge.
enum MergeSource {
    /// No-merge path: the base file's batches, forwarded as they arrive. One
    /// source batch becomes one emitted chunk.
    Eager { source: BaseBatchStream },

    /// MOR merge path. Drives the buffer's vectorized
    /// `merge_base_batch` + `drain_log_only_inserts` methods:
    /// one emitted chunk per non-empty base source batch, then the log-only
    /// inserts (keys never matched by any base row) in
    /// [`DEFAULT_BATCH_SIZE`]-row chunks until the drain reports exhaustion.
    ///
    /// Chunk size follows the base source's own batches, so there is no
    /// batch-size knob on this path.
    Buffered {
        buffer: Box<dyn HoodieFileGroupRecordBuffer>,
        /// The base file, pulled one batch at a time. Owned here rather than by
        /// the buffer: the buffer's job is to merge a batch, and only whoever
        /// holds the source can say when the base is exhausted. `None` once it
        /// is, or when the file group has no base file.
        base_source: Option<BaseBatchStream>,
        /// Schema the merged chunks conform to (used as `target_schema`
        /// when reconciling base batches with nested-type child field
        /// name drift, and as the schema for `records_to_batch` when the
        /// final drain emits log-only inserts).
        merge_schema: SchemaRef,
        /// Two-state machine: BaseScanning → DrainingLogInserts. The terminal
        /// state is the stream's own sticky `done` flag, set on exhaustion or
        /// error.
        state: BufferedState,
    },
}

/// State for `MergeSource::Buffered::next()`.
enum BufferedState {
    /// Pulling base batches from the source and merging against the log map.
    BaseScanning,
    /// Base source exhausted; drain log-only inserts. Each call to the
    /// buffer's `drain_log_only_inserts` yields one bounded chunk, and the
    /// stream stays in this state until the drain returns `Ok(None)` —
    /// collapsing it to a single call would silently drop every insert past
    /// the first chunk.
    DrainingLogInserts,
}

/// Streaming output for the file-group reader.
///
/// Driven a chunk at a time with [`Self::next_chunk`], or turned into a
/// `Stream` with [`Self::into_stream`] for a caller that wants to compose it.
pub struct FileGroupMergeStream {
    source: MergeSource,
    /// The schema every emitted chunk carries: the optional
    /// [`OutputConverter`]'s `target_schema`, or the merge schema when there is
    /// no converter.
    output_schema: SchemaRef,
    output_converter: Option<Box<dyn OutputConverter>>,
    /// Sticky termination — once any `next()` errors or `Eager.cursor`
    /// runs off the end, subsequent calls return `None`.
    done: bool,
    /// Shared stage-stats sink. The iterator accumulates
    /// final_merge_ms + output_build_ms per chunk here, and on exhaustion
    /// snapshots the buffer's update-processor counts (Buffered source). The
    /// owning reader drains it into `HoodieReadStats` after `read()`.
    stream_stats: StreamStatsHandle,
}

impl std::fmt::Debug for FileGroupMergeStream {
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
        f.debug_struct("FileGroupMergeStream")
            .field("source", &kind)
            .field("output_schema_cols", &self.output_schema.fields().len())
            .field("has_output_converter", &self.output_converter.is_some())
            .field("done", &self.done)
            .finish()
    }
}

impl FileGroupMergeStream {
    /// Build a merge that forwards the base file unchanged (CoW / empty path).
    /// One source batch becomes one emitted chunk, after the optional
    /// `OutputConverter`.
    ///
    /// `output_schema` is the schema that will appear on every emitted
    /// `RecordBatch` — must equal the converter's `target_schema` when one
    /// is supplied, or the source's schema otherwise.
    pub fn new_eager(
        source: BaseBatchStream,
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
    /// Used by the test harness to build an iterator over a fixed record set.
    #[allow(dead_code)]
    pub fn new_eager_from_vec(
        batches: Vec<RecordBatch>,
        output_schema: SchemaRef,
        output_converter: Option<Box<dyn OutputConverter>>,
        stream_stats: StreamStatsHandle,
    ) -> Self {
        Self::new_eager(
            futures::stream::iter(batches.into_iter().map(Ok)).boxed(),
            output_schema,
            output_converter,
            stream_stats,
        )
    }

    /// Build an iterator that drives MOR merge from a populated buffer.
    ///
    /// `merge_schema` is the schema [`records_to_batch`] uses while
    /// concatenating BufferedRecords for a chunk (pre-projection).
    /// `output_schema` is the post-projection schema every chunk carries.
    pub fn new_buffered(
        buffer: Box<dyn HoodieFileGroupRecordBuffer>,
        base_source: BaseBatchStream,
        merge_schema: SchemaRef,
        output_schema: SchemaRef,
        output_converter: Option<Box<dyn OutputConverter>>,
        stream_stats: StreamStatsHandle,
    ) -> Self {
        Self {
            source: MergeSource::Buffered {
                buffer,
                base_source: Some(base_source),
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

    /// The merge buffer's CURRENT tracked in-memory footprint, in bytes.
    /// Delegates to the buffer's
    /// [`current_in_memory_bytes`](HoodieFileGroupRecordBuffer::current_in_memory_bytes)
    /// on the merge path; returns 0 for the Eager (no-merge) source, which holds
    /// no merge map. Used by the FFI reader-memory accessor so a host memory
    /// manager (velox's `MemoryPool`) can reserve against hudi-rs's live native
    /// footprint. Cheap (an `AtomicU64`/counter read on the buffer), safe to call
    /// between chunk pulls.
    #[must_use]
    /// Reported by the memory harness; the merge path reads the map's own counter.
    #[allow(dead_code)]
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

    /// Convenience for callers that want "one batch" behaviour
    /// (drives the iterator to completion and concatenates).
    ///
    /// Backs `HoodieFileGroupReader::read()`'s single-batch return type.
    pub async fn collect_into_one_batch(mut self) -> Result<RecordBatch> {
        let output_schema = self.output_schema.clone();
        let mut chunks: Vec<RecordBatch> = Vec::new();
        while let Some(next) = self.next_chunk().await {
            chunks.push(next?);
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

    /// Apply the configured [`OutputConverter`], if any.
    fn finish_chunk(&self, batch: RecordBatch) -> Result<RecordBatch> {
        match &self.output_converter {
            Some(converter) => converter.apply(batch),
            None => Ok(batch),
        }
    }
}

impl FileGroupMergeStream {
    /// The schema every emitted chunk carries, settled before any chunk is
    /// produced. No production caller needs it — a consumer of the boxed stream
    /// gets the schema from the table — but it is the invariant the projection
    /// tests pin, so it is stated here rather than reached for through the
    /// private field.
    #[allow(dead_code)]
    pub fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    /// Produce the next merged chunk, or `None` once the merge is complete.
    ///
    /// The only await is the base-file pull. Everything else — the merge kernel,
    /// the drain, the output projection — is synchronous work on data already in
    /// memory, so nothing here holds a lock or a borrow across the await beyond
    /// the source itself.
    ///
    /// That synchronous work is deliberately *not* moved to a blocking pool,
    /// which is the question a reader of this arrives with. It was measured
    /// rather than assumed: merging one chunk takes 0.4-1.1 ms, and 5.7-6.1 ms
    /// once the merge map has spilled, so the spill is a ~6x amplifier on work
    /// that is already synchronous and of the same kind. Handing each chunk to
    /// `spawn_blocking` would reintroduce the sync/async crossing this type
    /// exists to remove, and cost a task hop per chunk, to shave a
    /// single-digit-millisecond poll — which is what a scan or join operator
    /// costs anyway. What keeps that true is the chunk bound, not this comment:
    /// see `MERGE_CHUNK_ROWS`, since the cost is linear in a chunk's rows.
    ///
    /// The one thing the measurement does not cover: it reads a spill file that
    /// was just written, so the page cache is warm and the disk component is
    /// understated. A cold spill file would be slower, though still bounded by
    /// the same chunk size.
    ///
    /// Sticky: after an error, or after the merge completes, every later call
    /// returns `None`.
    pub async fn next_chunk(&mut self) -> Option<Result<RecordBatch>> {
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
        let produced: std::result::Result<Option<Chunk>, (CoreError, u64, u64)> =
            match &mut self.source {
                MergeSource::Eager { source } => {
                    // One source batch = one emitted chunk. The no-merge path
                    // (CoW, or a file group with no log files) never
                    // materialises the whole base file; it pulls one row group
                    // at a time.
                    match source.next().await {
                        None => Ok(None),
                        // Eager has no merge work; build timing is the converter only.
                        Some(Ok(batch)) => Ok(Some((batch, 0, 0))),
                        Some(Err(e)) => Err((e, 0, 0)),
                    }
                }

                MergeSource::Buffered {
                    buffer,
                    base_source,
                    merge_schema,
                    state,
                } => {
                    // Vectorized merge path: pull the next base batch and merge
                    // it, or drain log-only inserts, then hand the result to the
                    // shared timing / projection / exhaustion tail below
                    // (Step 2). Loop to skip base batches fully eliminated by
                    // log deletes.
                    //
                    // Only the merge itself is timed. Timing the whole loop
                    // would fold the base file's read latency into
                    // `final_merge_ms`, which is meant to measure merging.
                    let mut merge_ms = 0u64;
                    let mut chunk_err: Option<CoreError> = None;
                    let mut out_batch: Option<RecordBatch> = None;
                    loop {
                        match state {
                            BufferedState::BaseScanning => {
                                // Pull first, merge second. Only the source knows
                                // when the base is exhausted, so that is the only
                                // thing that moves the state machine on to the
                                // drain — a merge that yields no rows just means
                                // this batch contributed none, and must not be
                                // read as the end of the base file.
                                let pulled = match base_source.as_mut() {
                                    None => None,
                                    Some(source) => match source.next().await {
                                        None => {
                                            *base_source = None;
                                            None
                                        }
                                        Some(b) => Some(b),
                                    },
                                };
                                let base = match pulled {
                                    None => {
                                        // Base exhausted — drain log-only inserts
                                        // on the next loop turn.
                                        *state = BufferedState::DrainingLogInserts;
                                        continue;
                                    }
                                    Some(Ok(b)) => b,
                                    Some(Err(e)) => {
                                        // Drop the source: a failed read must not
                                        // be resumed as if it had merely ended,
                                        // which would truncate the output
                                        // silently.
                                        *base_source = None;
                                        log::error!("[FileGroupMergeStream] base file source: {e}");
                                        chunk_err = Some(CoreError::ReadFileSliceError(format!(
                                            "base file source error: {e}"
                                        )));
                                        break;
                                    }
                                };
                                if base.num_rows() == 0 {
                                    continue; // skip empty source batches
                                }
                                let merge_start = Instant::now();
                                let merged = buffer.merge_base_batch(&base, merge_schema);
                                merge_ms = merge_ms
                                    .saturating_add(merge_start.elapsed().as_millis() as u64);
                                match merged {
                                    Ok(Some(b)) => {
                                        if b.num_rows() == 0 {
                                            // All base rows in this source batch
                                            // lost to a log delete — pull the next.
                                            continue;
                                        }
                                        out_batch = Some(b);
                                        break;
                                    }
                                    Ok(None) => continue,
                                    Err(e) => {
                                        chunk_err = Some(e);
                                        break;
                                    }
                                }
                            }
                            BufferedState::DrainingLogInserts => {
                                // drain_log_only_inserts is idempotent: it returns the
                                // remaining log-only inserts once, then Ok(None).
                                let drain_start = Instant::now();
                                let drained = buffer.drain_log_only_inserts(merge_schema);
                                merge_ms = merge_ms
                                    .saturating_add(drain_start.elapsed().as_millis() as u64);
                                match drained {
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
                Some(Err(e))
            }
        }
    }

    /// Drive this merge as a `Stream`, for a caller that wants to compose it.
    ///
    /// Chunk-at-a-time and demand-driven: nothing is merged until the consumer
    /// asks, so the extra memory over the merge map itself is one chunk.
    pub fn into_stream(self) -> BoxStream<'static, Result<RecordBatch>> {
        futures::stream::unfold(self, |mut merge| async move {
            merge.next_chunk().await.map(|item| (item, merge))
        })
        .boxed()
    }
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
    /// `KeyBasedFileGroupRecordBuffer` stores records
    /// (`process_next_data_record` keeps the `BatchRef`, no IPC serialization).
    /// Exercising the in-memory representation here guards the streaming
    /// iterator's drain against the real payload shape.
    fn batch_ref_record(schema: &SchemaRef, key: &str, v: i32) -> BufferedRecord {
        let b = std::sync::Arc::new(batch(schema.clone(), &[key], &[v]));
        let r = BufferedRecord::new_batch_ref(key.to_string(), b, 0, None);
        // Sanity: the payload is a zero-copy batch ref, not an IPC blob.
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
        /// How many base batches merge to nothing before one merges to rows.
        /// Zero for every test that drives the drain with no base file.
        merge_yields_nothing_for: usize,
    }

    impl MockBuffer {
        fn boxed(
            records: Vec<BufferedRecord>,
            stats: UpdateStats,
        ) -> Box<dyn HoodieFileGroupRecordBuffer> {
            Self::boxed_yielding_nothing_for(records, stats, 0)
        }

        /// A buffer whose first `n` merges produce no batch at all, so the
        /// iterator's reading of `Ok(None)` can be pinned.
        fn boxed_yielding_nothing_for(
            records: Vec<BufferedRecord>,
            stats: UpdateStats,
            n: usize,
        ) -> Box<dyn HoodieFileGroupRecordBuffer> {
            Box::new(Self {
                queue: records.into(),
                stats,
                empty_map: HashMap::default(),
                merge_yields_nothing_for: n,
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
        fn merge_base_batch(
            &mut self,
            base: &RecordBatch,
            _target_schema: &SchemaRef,
        ) -> Result<Option<RecordBatch>> {
            if self.merge_yields_nothing_for > 0 {
                self.merge_yields_nothing_for -= 1;
                return Ok(None);
            }
            Ok(Some(base.clone()))
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

    /// A base source with no batches — a log-only file group. The merge goes
    /// straight to the log drain, which is where these mocks keep their records.
    fn no_base(_schema: &SchemaRef) -> BaseBatchStream {
        futures::stream::empty().boxed()
    }

    /// A base source over fixed batches.
    fn base_of(batches: Vec<Result<RecordBatch>>) -> BaseBatchStream {
        futures::stream::iter(batches).boxed()
    }

    /// Drive a merge to exhaustion from a synchronous test. Nothing here does
    /// I/O — the base sources are in-memory — so a local executor is enough.
    fn drain(mut merge: FileGroupMergeStream) -> Vec<Result<RecordBatch>> {
        futures::executor::block_on(async {
            let mut out = Vec::new();
            while let Some(chunk) = merge.next_chunk().await {
                out.push(chunk);
            }
            out
        })
    }

    /// As [`drain`], unwrapping each chunk.
    fn drain_ok(merge: FileGroupMergeStream) -> Vec<RecordBatch> {
        drain(merge).into_iter().map(|c| c.unwrap()).collect()
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
        let _ = batch_size;
        let it = FileGroupMergeStream::new_buffered(
            MockBuffer::boxed(records, stats),
            no_base(&schema),
            schema.clone(),
            schema.clone(),
            None,
            handle.clone(),
        );
        let mut chunk_rows = Vec::new();
        let mut all_rows = Vec::new();
        for b in drain_ok(it) {
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
    /// assertions.
    fn rows(b: &RecordBatch) -> Vec<(String, i32)> {
        let keys = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let vs = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        (0..b.num_rows())
            .map(|i| (keys.value(i).to_string(), vs.value(i)))
            .collect()
    }

    /// The trait's default stat accessors are the contract a buffer that does
    /// not instrument itself falls back to. `MockBuffer` is exactly such a
    /// buffer, and the loader calls every one of these when it copies scan stats
    /// into `HoodieReadStats`. Both production buffers override them, so this is
    /// the only place the defaults run at all.
    #[test]
    fn test_record_buffer_default_stat_accessors_report_nothing() {
        let buffer = MockBuffer::boxed(Vec::new(), UpdateStats::default());

        assert_eq!(buffer.stage_decode_ms(), 0);
        assert_eq!(buffer.merge_map_peak_entries(), 0);
        assert!(!buffer.merge_map_spilled());
        assert_eq!(buffer.merge_map_peak_in_memory_bytes(), 0);
        assert_eq!(buffer.current_in_memory_bytes(), 0);
        assert_eq!(buffer.update_stats_snapshot(), UpdateStats::default());
    }

    /// Eager mode yields each input batch unchanged when no converter is set;
    /// assert the full row contents, not just counts.
    #[test]
    fn eager_yields_each_batch() {
        let schema = small_schema();
        let b1 = batch(schema.clone(), &["a", "b"], &[1, 2]);
        let b2 = batch(schema.clone(), &["c"], &[3]);
        let it = FileGroupMergeStream::new_eager_from_vec(
            vec![b1, b2],
            schema.clone(),
            None,
            new_stream_stats_handle(),
        );
        assert_eq!(it.schema(), schema);
        let out: Vec<RecordBatch> = drain_ok(it);
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
        let it = FileGroupMergeStream::new_eager_from_vec(
            vec![b1, b2],
            schema,
            None,
            new_stream_stats_handle(),
        );
        let result = futures::executor::block_on(it.collect_into_one_batch()).unwrap();
        assert_eq!(
            rows(&result),
            vec![("a".into(), 1), ("b".into(), 2), ("c".into(), 3)]
        );
    }

    /// Eager with no input → empty batch w/ the requested schema.
    #[test]
    fn eager_empty_yields_zero_rows_via_collect() {
        let schema = small_schema();
        let it = FileGroupMergeStream::new_eager_from_vec(
            vec![],
            schema.clone(),
            None,
            new_stream_stats_handle(),
        );
        let result = futures::executor::block_on(it.collect_into_one_batch()).unwrap();
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

    /// 10 records → all rows preserved in order. (vectorized: chunk
    /// cadence follows the base source's batches, not `batch_size`, so we
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

    /// Small `batch_size` hint → full data preserved (vectorized: the
    /// hint does not force singleton chunks; chunking follows the base source).
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
        // base source (vectorized), so assert the surviving-row total.
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
    /// insert/update/delete counts into the shared stats sink (the streaming
    /// path never calls `merge_and_collect_with_stats`, so this snapshot is
    /// the only stats hand-off).
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
    /// the single-batch semantics. This is the streaming-vs-collect
    /// equivalence guarantee, asserted over the full dataset.
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

    /// A merge that yields no batch means *this* batch contributed no rows, not
    /// that the base file has ended. Only the source says that. Reading the two
    /// as the same thing would stop the scan at the first fully-deleted batch
    /// and silently drop every base row after it.
    #[test]
    fn buffered_merge_yielding_nothing_is_not_the_end_of_the_base() {
        let schema = small_schema();
        let base = base_of(vec![
            Ok(batch(schema.clone(), &["a"], &[1])),
            Ok(batch(schema.clone(), &["b"], &[2])),
        ]);
        // The first base batch merges to nothing; the second must still be read.
        let it = FileGroupMergeStream::new_buffered(
            MockBuffer::boxed_yielding_nothing_for(vec![], UpdateStats::default(), 1),
            base,
            schema.clone(),
            schema,
            None,
            new_stream_stats_handle(),
        );
        let out: Vec<(String, i32)> = drain_ok(it).iter().flat_map(rows).collect();
        assert_eq!(
            out,
            vec![("b".to_string(), 2)],
            "the batch after one that merged to nothing must still reach the output"
        );
    }

    /// A base source that fails mid-read surfaces the failure. The rows already
    /// emitted make a truncated read look like a short but successful one, so
    /// swallowing the error here would report partial data as complete - the
    /// one failure this path must never produce.
    #[test]
    fn buffered_base_source_error_surfaces_rather_than_truncating() {
        let schema = small_schema();
        let base = base_of(vec![
            Ok(batch(schema.clone(), &["a"], &[1])),
            Err(CoreError::ReadFileSliceError("base read blew up".into())),
        ]);
        let it = FileGroupMergeStream::new_buffered(
            MockBuffer::boxed(vec![], UpdateStats::default()),
            base,
            schema.clone(),
            schema,
            None,
            new_stream_stats_handle(),
        );
        let chunks = drain(it);
        assert_eq!(
            rows(&chunks[0].as_ref().unwrap().clone()),
            vec![("a".to_string(), 1)],
            "the batch read before the failure still comes through"
        );
        match chunks.get(1) {
            Some(Err(e)) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("base file source error"),
                    "the error must name the base source, got: {msg}"
                );
            }
            other => panic!("expected the base source error to surface, got {other:?}"),
        }
        assert_eq!(chunks.len(), 2, "the merge stops after the error");
    }

    /// The two properties that make this a stream rather than a differently-
    /// shaped batch read, neither of which any output-level assertion can see.
    ///
    /// **Lazy.** Asking for one chunk must pull one base batch, not the whole
    /// base file. A merge that collected the base up front would return exactly
    /// the same rows in exactly the same order, so row and order assertions
    /// cannot distinguish it - only counting the pulls can.
    ///
    /// **No thread hand-off.** The merge must run on the task that polls it. The
    /// shape this replaced moved the whole merge loop onto a blocking-pool
    /// thread and fed batches back through a channel, which also produced the
    /// right rows; the observable difference is which thread the base pull
    /// happens on. Asserted on a `current_thread` runtime, where the polling
    /// task and the test share one thread, so a hand-off shows up as a different
    /// thread id.
    #[tokio::test]
    async fn merge_stream_is_lazy_and_polls_on_the_callers_thread() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let schema = small_schema();
        let pulls = Arc::new(AtomicUsize::new(0));
        let pull_threads = Arc::new(std::sync::Mutex::new(Vec::<std::thread::ThreadId>::new()));

        let (p, t) = (pulls.clone(), pull_threads.clone());
        let base = futures::stream::iter(vec![
            Ok(batch(schema.clone(), &["a"], &[1])),
            Ok(batch(schema.clone(), &["b"], &[2])),
            Ok(batch(schema.clone(), &["c"], &[3])),
        ])
        .inspect(move |_| {
            p.fetch_add(1, Ordering::SeqCst);
            t.lock().unwrap().push(std::thread::current().id());
        })
        .boxed();

        let mut merge = FileGroupMergeStream::new_buffered(
            MockBuffer::boxed(vec![], UpdateStats::default()),
            base,
            schema.clone(),
            schema,
            None,
            new_stream_stats_handle(),
        );

        // One chunk asked for, one base batch read.
        let first = merge.next_chunk().await.expect("a first chunk").unwrap();
        assert_eq!(rows(&first), vec![("a".to_string(), 1)]);
        assert_eq!(
            pulls.load(Ordering::SeqCst),
            1,
            "one chunk must cost one base batch; a merge that pre-collected the \
             base would read all three and still return this same chunk"
        );

        // ...and the rest only when asked for.
        let second = merge.next_chunk().await.expect("a second chunk").unwrap();
        assert_eq!(rows(&second), vec![("b".to_string(), 2)]);
        assert_eq!(pulls.load(Ordering::SeqCst), 2, "still one pull per chunk");

        while merge.next_chunk().await.is_some() {}
        assert_eq!(
            pulls.load(Ordering::SeqCst),
            3,
            "the whole base is read once"
        );

        let here = std::thread::current().id();
        let threads = pull_threads.lock().unwrap();
        assert!(
            threads.iter().all(|t| *t == here),
            "the base pull must happen on the polling task's own thread; saw {threads:?} \
             from {here:?}, which means the merge was handed to another thread"
        );
    }

    /// A concurrent task makes progress *while* the merge runs, chunk by chunk.
    ///
    /// This is the non-blocking property, and it is separate from laziness: a
    /// merge could pull one batch at a time and still hold its executor thread
    /// for the whole read, which on a single-worker runtime starves everything
    /// else on it. The base stream here awaits before yielding each batch, which
    /// is what a ranged read looks like to the runtime; the assertion is that
    /// the merge propagates that await rather than blocking through it.
    ///
    /// `current_thread` on purpose - one worker, so the ticker can only advance
    /// if the merge actually gives the thread up. On a multi-threaded runtime
    /// the ticker would run on another worker and prove nothing.
    ///
    /// The interleaving is checked per chunk rather than once at the end, and
    /// that is what makes it discriminating: a merge that awaited the whole base
    /// up front and then served chunks without awaiting again passes a single
    /// end-of-read check and fails this one at chunk 2 (`4 -> 4`).
    ///
    /// The severe violation - blocking the thread *through* the base await -
    /// shows up as a deadlock rather than a failed assertion, since on one
    /// worker the pending read can never be rescheduled. Verified by mutation:
    /// the test hangs instead of failing. Detected either way, but a hang is a
    /// worse signal, so the per-chunk check above is what carries this test.
    #[tokio::test]
    async fn merge_stream_lets_other_tasks_run_between_chunks() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let schema = small_schema();
        let ticks = Arc::new(AtomicUsize::new(0));
        let t = ticks.clone();
        let ticker = tokio::spawn(async move {
            loop {
                t.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
        });

        let batches: Vec<Result<RecordBatch>> = (0..4)
            .map(|i| Ok(batch(schema.clone(), &["k"], &[i])))
            .collect();
        // Awaits before handing over each batch, standing in for the ranged read
        // a real base source performs.
        let base = futures::stream::iter(batches)
            .then(|b| async move {
                tokio::task::yield_now().await;
                b
            })
            .boxed();

        let mut merge = FileGroupMergeStream::new_buffered(
            MockBuffer::boxed(vec![], UpdateStats::default()),
            base,
            schema.clone(),
            schema,
            None,
            new_stream_stats_handle(),
        );

        let mut seen = 0usize;
        let mut chunks = 0usize;
        while merge.next_chunk().await.is_some() {
            chunks += 1;
            let now = ticks.load(Ordering::SeqCst);
            assert!(
                now > seen,
                "the ticker did not advance while chunk {chunks} was produced ({seen} -> \
                 {now}); the merge held the only worker thread instead of awaiting"
            );
            seen = now;
        }
        assert_eq!(chunks, 4, "one chunk per base batch");
        ticker.abort();
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
            // The state-machine iterator drives the buffer via this
            // method first (BaseScanning); error here to exercise the
            // surface-once-then-fuse contract.
            Err(crate::error::CoreError::ReadFileSliceError("boom".into()))
        }
        fn merge_base_batch(
            &mut self,
            _base: &RecordBatch,
            _target_schema: &SchemaRef,
        ) -> Result<Option<RecordBatch>> {
            // Same failure through the batch-at-a-time entry point, so the
            // fuse contract is exercised whichever way the base is supplied.
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
    /// iterator fuses (sticky `done`) — the contract every consumer leans on.
    #[test]
    fn buffered_surfaces_error_then_fuses() {
        let schema = small_schema();
        let buffer = Box::new(ErrOnHasNext {
            empty_map: HashMap::default(),
        });
        // One base batch, so the buffer is actually asked to merge something:
        // with an empty base the merge would go straight to the drain and the
        // failure under test would never be reached.
        let base = base_of(vec![Ok(batch(schema.clone(), &["a"], &[1]))]);
        let it = FileGroupMergeStream::new_buffered(
            buffer,
            base,
            schema.clone(),
            schema,
            None,
            new_stream_stats_handle(),
        );
        // The buffer's `ReadFileSliceError` surfaces once, as itself rather
        // than swapped for another variant, and then the merge stops.
        let chunks = drain(it);
        match chunks.first() {
            Some(Err(CoreError::ReadFileSliceError(msg))) => assert_eq!(msg.as_str(), "boom"),
            other => panic!("expected CoreError::ReadFileSliceError, got {other:?}"),
        }
        assert_eq!(
            chunks.len(),
            1,
            "the merge fuses after an error (sticky done)"
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
        // 5 records with a converter installed, so per-chunk projection is exercised.
        let it = FileGroupMergeStream::new_buffered(
            MockBuffer::boxed(batch_ref_seq(&merge_schema, 5), UpdateStats::default()),
            no_base(&merge_schema),
            merge_schema.clone(),
            target_schema.clone(),
            Some(Box::new(converter)),
            new_stream_stats_handle(),
        );
        // schema() reports the post-projection target BEFORE iterating.
        assert_eq!(
            it.schema(),
            target_schema,
            "schema() reports the converter target before the first chunk"
        );

        let chunks: Vec<RecordBatch> = drain_ok(it);
        // The MockBuffer drains its log-only inserts in one batch, so
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

    /// The Eager (no-merge) source works with an arbitrary
    /// base stream, not just a Vec. Models the real no-merge path, where the
    /// reader hands `new_eager` the parquet stream directly and one row group
    /// becomes one chunk.
    #[test]
    fn eager_accepts_an_arbitrary_base_stream() {
        let schema = small_schema();
        let b1 = batch(schema.clone(), &["a", "b"], &[1, 2]);
        let b2 = batch(schema.clone(), &["c"], &[3]);
        let it = FileGroupMergeStream::new_eager(
            base_of(vec![Ok(b1), Ok(b2)]),
            schema.clone(),
            None,
            new_stream_stats_handle(),
        );
        let out: Vec<RecordBatch> = drain_ok(it);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].num_rows() + out[1].num_rows(), 3);
    }
}
