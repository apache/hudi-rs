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

//! Mirrors:
//! - `FileGroupRecordBufferLoader` (Java interface)
//! - `LogScanningRecordBufferLoader` (Java abstract class)
//! - `DefaultFileGroupRecordBufferLoader` (Java impl)
//!
//! ## Call stack (matching Java 1:1):
//! ```text
//! DefaultFileGroupRecordBufferLoader.getRecordBuffer()
//!   ├─ new KeyBasedFileGroupRecordBuffer(...)
//!   └─ scanLogFiles(readerContext, storage, inputSplit, ..., recordBuffer)
//!        └─ HoodieMergedLogRecordReader.newBuilder()...build()
//!             └─ performScan()
//!                  └─ BaseHoodieLogRecordReader.scanInternal()
//! ```

use crate::Result;
use crate::file_group::reader_v2::buffer::HoodieFileGroupRecordBuffer;
use crate::file_group::reader_v2::buffer::key_based::KeyBasedFileGroupRecordBuffer;
use crate::file_group::reader_v2::buffer::position_based::PositionBasedFileGroupRecordBuffer;
use crate::file_group::reader_v2::input_split::InputSplit;
use crate::file_group::reader_v2::merged_log_record_reader::HoodieMergedLogRecordReader;
use crate::file_group::reader_v2::read_stats::HoodieReadStats;
use crate::file_group::reader_v2::reader_context::ReaderContext;
use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
use crate::storage::Storage;
use std::sync::Arc;

/// Result of loading a record buffer.
///
/// Mirrors Java's `Pair<HoodieFileGroupRecordBuffer<T>, List<String>>`.
pub struct RecordBufferLoadResult {
    pub record_buffer: Box<dyn HoodieFileGroupRecordBuffer>,
    pub valid_block_instants: Vec<String>,
}

/// Trait for loading file group record buffers.
///
/// Mirrors Java's `FileGroupRecordBufferLoader<T>` interface.
pub trait FileGroupRecordBufferLoader: Send + Sync + std::fmt::Debug {
    /// Create and populate a record buffer for the given input split.
    ///
    /// Mirrors Java's `getRecordBuffer(...)`.
    fn get_record_buffer(
        &self,
        reader_context: Arc<ReaderContext>,
        storage: Arc<Storage>,
        input_split: &InputSplit,
        reader_parameters: &ReaderParameters,
        read_stats: &mut HoodieReadStats,
    ) -> impl std::future::Future<Output = Result<RecordBufferLoadResult>> + Send;
}

/// Default file group record buffer loader.
///
/// Mirrors Java's `DefaultFileGroupRecordBufferLoader<T>` (singleton).
/// Extends `LogScanningRecordBufferLoader` (the `scan_log_files` method).
/// Implements `FileGroupRecordBufferLoader<T>`.
///
/// ## Buffer strategy selection:
/// ```text
/// is_skip_merge?  → UnmergedFileGroupRecordBuffer (not implemented)
/// sort_outputs?   → SortedKeyBasedFileGroupRecordBuffer (not implemented)
/// use_record_position && base_file?  → PositionBasedFileGroupRecordBuffer (not impl)
/// DEFAULT         → KeyBasedFileGroupRecordBuffer ★
/// ```
#[derive(Debug)]
pub struct DefaultFileGroupRecordBufferLoader;

impl DefaultFileGroupRecordBufferLoader {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultFileGroupRecordBufferLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl FileGroupRecordBufferLoader for DefaultFileGroupRecordBufferLoader {
    /// Mirrors Java's `DefaultFileGroupRecordBufferLoader.getRecordBuffer(...)`.
    ///
    /// Steps:
    /// 1. Create UpdateProcessor
    /// 2. Instantiate buffer (strategy selection — only KeyBased for now)
    /// 3. Call `scanLogFiles()` to populate the buffer
    async fn get_record_buffer(
        &self,
        reader_context: Arc<ReaderContext>,
        storage: Arc<Storage>,
        input_split: &InputSplit,
        reader_parameters: &ReaderParameters,
        read_stats: &mut HoodieReadStats,
    ) -> Result<RecordBufferLoadResult> {
        // Use merge mode from reader context directly (mirrors Java:
        // readerContext.getMergeMode()). Normalize to upper-case so the gate
        // below, the record-merger factory (reached via the buffer), and the
        // schema handler (which already compares case-insensitively) agree on
        // one spelling — gold's `getMergeMode` is likewise case-insensitive.
        let merge_mode = if reader_context.merge_mode.is_empty() {
            "COMMIT_TIME_ORDERING".to_string()
        } else {
            reader_context.merge_mode.to_uppercase()
        };

        // Guard: COMMIT_TIME_ORDERING and EVENT_TIME_ORDERING are supported on the
        // MOR scan path. This is the single chokepoint where the configured merge
        // mode flows into record-buffer construction, so the gate lives here.
        // EVENT_TIME_ORDERING (ENG-38318): the KeyBasedFileGroupRecordBuffer builds
        // the EventTimeRecordMerger via BufferedRecordMergerFactory and merges
        // base-vs-log by ordering value (the base record now carries its ordering
        // value — see KeyBasedFileGroupRecordBuffer::has_next_base_record_keyed).
        // CUSTOM still requires a partial-update / custom merger that is not implemented.
        match merge_mode.as_str() {
            "COMMIT_TIME_ORDERING" | "EVENT_TIME_ORDERING" => {}
            unsupported => {
                return Err(crate::error::CoreError::ReadFileSliceError(format!(
                    "Unsupported merge mode: '{unsupported}'. Only COMMIT_TIME_ORDERING \
                     and EVENT_TIME_ORDERING are supported (MOR scan path)."
                )));
            }
        }

        log::debug!(
            "[DefaultFileGroupRecordBufferLoader] getRecordBuffer: merge_mode={merge_mode} \
             record_key_field={} ordering_fields={:?} \
             log_files={} latest_commit_time={}",
            reader_context.record_key_field(),
            reader_context.ordering_field_names(),
            input_split.log_file_paths.len(),
            reader_context.latest_commit_time,
        );

        // STEP: Instantiate buffer (strategy selection)
        // Mirrors Java's DefaultFileGroupRecordBufferLoader.getRecordBuffer() lines 67-80.
        let is_skip_merge = reader_context
            .hoodie_reader_config
            .get("hoodie.datasource.merge.type")
            .map(|v| v.eq_ignore_ascii_case("skip_merge"))
            .unwrap_or(false);

        let record_buffer: Box<dyn HoodieFileGroupRecordBuffer> = if is_skip_merge {
            return Err(crate::error::CoreError::Unsupported(
                "UnmergedFileGroupRecordBuffer (skip_merge mode) is not yet implemented"
                    .to_string(),
            ));
        } else if reader_parameters.sort_output {
            return Err(crate::error::CoreError::Unsupported(
                "SortedKeyBasedFileGroupRecordBuffer (sort_output mode) is not yet implemented"
                    .to_string(),
            ));
        } else if reader_parameters.use_record_position
            && input_split.base_file_path.is_some()
            && input_split.base_file_commit_time.is_some()
            && base_file_is_parquet(&reader_context.base_file_format)
        {
            // Position-based merge: match base rows to log records by base-file
            // row position (Java PositionBasedFileGroupRecordBuffer). This gate
            // MUST match HoodieFileGroupReader::use_record_position() (which
            // decides whether the base read attaches the row-index column):
            // MOR-with-logs (implied here — skip-merge is handled above and this
            // arm is only reached when there are log records to merge) + a
            // parquet base file. A mismatch would build a position buffer for a
            // base source that lacks the row-index column. The base file's
            // commit time (validated present by the gate) is used to check
            // log-block position headers. When it is absent (e.g. the FFI could
            // not parse it from the base file name), this arm is skipped and the
            // read falls through to key-based merge — which is always correct
            // and needs no commit time — rather than erroring. Position merge
            // also falls back to key-based at runtime when a block's positions
            // are unavailable.
            // Gated on `is_some()` above, so this never errors; kept as a
            // non-panicking guard rather than an `expect()` (library code).
            let base_file_instant_time =
                input_split.base_file_commit_time.clone().ok_or_else(|| {
                    crate::error::CoreError::ReadFileSliceError(
                        "internal: position-merge branch entered without a base-file commit time"
                            .to_string(),
                    )
                })?;
            Box::new(PositionBasedFileGroupRecordBuffer::new(
                reader_context.clone(),
                merge_mode,
                reader_parameters.emit_delete,
                base_file_instant_time,
            )?)
        } else if reader_parameters.emit_delete {
            // Gold's emitDeletes path synthesizes a delete row
            // (RecordContext.getDeleteRow) and tags HoodieOperation.DELETE so
            // deletes flow into the output (UpdateProcessor.java:91-101). That
            // delete-row synthesis + operation tagging is not implemented; gate
            // it loudly here rather than silently dropping deletes (which is the
            // only supported posture). Mirrors the use_record_position gate above.
            return Err(crate::error::CoreError::Unsupported(
                "emit_delete=true (emitting delete records into the output) is not yet \
                 implemented; the supported read path drops deletes from the merged output"
                    .to_string(),
            ));
        } else {
            Box::new(KeyBasedFileGroupRecordBuffer::new(
                reader_context.clone(),
                merge_mode,
                reader_parameters.emit_delete,
            )?)
        };

        // STEP: scanLogFiles — build and run HoodieMergedLogRecordReader
        let (mut populated_buffer, valid_block_instants, stats) = scan_log_files(
            reader_context,
            storage,
            input_split,
            record_buffer,
            reader_parameters,
        )
        .await?;

        // STEP: compact sparsely-pinned source batches before the drain (A2
        // safety valve). After log scanning the merge map holds zero-copy
        // BatchRefs that each pin their whole source batch; this releases the
        // dead-row memory of any source batch whose survivors fell below the
        // compaction threshold. A no-op for buffers that don't hold batch refs,
        // and for the common case where survivors keep most batches well-populated.
        //
        // Absolute-pinned-bytes cap hook (A2 risk #1): A1 will additionally
        // trigger this — or a budget-driven variant — when distinct-pinned Arrow
        // bytes exceed a fraction of the merge budget, so the per-row size
        // estimate and the real pinned cost converge. Wired here as the
        // unconditional end-of-scan pass for now.
        populated_buffer.compact_pinned_batches()?;

        // Populate read stats from scan stats
        read_stats.total_log_read_time_ms = stats.total_time_taken_to_read_and_merge_blocks_ms;
        read_stats.total_log_records = stats.total_log_records;
        read_stats.total_log_blocks = stats.total_log_blocks;
        read_stats.total_log_files_compacted = stats.total_log_files;
        read_stats.total_corrupt_log_blocks = stats.total_corrupt_blocks;
        read_stats.total_rollback_blocks = stats.total_rollbacks;
        // Stage timings (perf harness).
        read_stats.log_block_read_ms = stats.log_block_read_ms;
        read_stats.log_block_decode_ms = stats.log_block_decode_ms;
        read_stats.merge_insert_ms = stats.merge_insert_ms;
        read_stats.merge_map_peak_entries = stats.merge_map_peak_entries;
        // Spillable merge map (A1).
        read_stats.merge_map_spilled = stats.merge_map_spilled;
        read_stats.merge_map_peak_in_memory_bytes = stats.merge_map_peak_in_memory_bytes;

        Ok(RecordBufferLoadResult {
            record_buffer: populated_buffer,
            valid_block_instants,
        })
    }
}

/// Scan log files and populate the record buffer.
///
/// Mirrors Java's `LogScanningRecordBufferLoader.scanLogFiles(...)`.
///
/// Builds a `HoodieMergedLogRecordReader` via builder, which calls
/// `performScan()` → `scanInternal()` during construction.
async fn scan_log_files(
    reader_context: Arc<ReaderContext>,
    storage: Arc<Storage>,
    input_split: &InputSplit,
    record_buffer: Box<dyn HoodieFileGroupRecordBuffer>,
    reader_parameters: &ReaderParameters,
) -> Result<(
    Box<dyn HoodieFileGroupRecordBuffer>,
    Vec<String>,
    crate::file_group::reader_v2::merged_log_record_reader::ScanStats,
)> {
    if !input_split.has_log_files() {
        let stats = crate::file_group::reader_v2::merged_log_record_reader::ScanStats::default();
        return Ok((record_buffer, Vec::new(), stats));
    }

    // Mirrors Java:
    // HoodieMergedLogRecordReader.newBuilder()
    //     .withHoodieReaderContext(readerContext)
    //     .withStorage(storage)
    //     .withLogFiles(inputSplit.getLogFiles())
    //     .withInstantRange(readerContext.getInstantRange())
    //     .withRecordBuffer(recordBuffer)
    //     .withAllowInflightInstants(readerParameters.allowInflightInstants())
    //     .build()
    // An empty watermark must mean "no upper bound" (read the full snapshot),
    // NOT "every block is in the future". Gate 2 filters log blocks with a
    // lexicographic `instant_time > latest_instant_time`; passing "" through
    // makes that true for EVERY block, silently dropping ALL log records and
    // returning base-file-only data with no error. Default empty to the
    // far-future sentinel so a missing watermark reads everything (gold's
    // behavior), matching the no-watermark default downstream.
    let latest_instant_time = if reader_context.latest_commit_time.is_empty() {
        crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string()
    } else {
        reader_context.latest_commit_time.clone()
    };
    // C-INFLIGHT-DELTA: forward the Gate-3 completed/inflight inputs the FFI
    // bridge stashed on the reader context (mirroring how instant_range rides
    // on it). `None` for v8+/incremental/non-gated reads leaves Gate 3 a no-op.
    let completion_gate_inputs = reader_context.completion_gate_inputs.clone();
    let reader = HoodieMergedLogRecordReader::new_builder()
        .with_reader_context(reader_context)
        .with_storage(storage)
        .with_log_files(input_split.log_file_paths.clone())
        .with_latest_instant_time(latest_instant_time)
        .with_record_buffer(record_buffer)
        .with_allow_inflight_instants(reader_parameters.allow_inflight_instants)
        .with_completion_gate_inputs(completion_gate_inputs)
        .with_force_full_scan(true)
        .build()
        .await?;

    // Decompose: get populated buffer + stats
    Ok(reader.into_parts())
}

/// Whether a base file of the given format is a parquet file. Position-based
/// merge is only valid for parquet base files (the row-index column comes from
/// a parquet virtual row-number column). An empty format string is treated as
/// parquet — hudi-rs's default and only base-file format. Shared by the loader's
/// buffer-selection gate and `HoodieFileGroupReader::use_record_position()` so
/// the two decisions cannot diverge.
pub(crate) fn base_file_is_parquet(base_file_format: &str) -> bool {
    base_file_format.is_empty() || base_file_format.eq_ignore_ascii_case("parquet")
}
