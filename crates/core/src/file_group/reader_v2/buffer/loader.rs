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

//! Record buffer loading: selects a buffer strategy, then populates the buffer
//! by scanning the file slice's log files.
//!
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
use crate::file_group::reader_v2::metadata_merger::resolve_custom_merger;
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
/// use_record_position && base_file?  → PositionBasedFileGroupRecordBuffer ★
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
        // one spelling — Java's `getMergeMode` is likewise case-insensitive.
        let merge_mode = if reader_context.merge_mode.is_empty() {
            "COMMIT_TIME_ORDERING".to_string()
        } else {
            reader_context.merge_mode.to_uppercase()
        };

        // Guard: COMMIT_TIME_ORDERING and EVENT_TIME_ORDERING are supported on the
        // MOR scan path. This is the single chokepoint where the configured merge
        // mode flows into record-buffer construction, so the gate lives here.
        // EVENT_TIME_ORDERING: the KeyBasedFileGroupRecordBuffer builds
        // the EventTimeRecordMerger via BufferedRecordMergerFactory and merges
        // base-vs-log by ordering value (the base record carries its ordering
        // value — see KeyBasedFileGroupRecordBuffer::has_next_base_record_at).
        // CUSTOM is admitted only when the table's payload class names a merger
        // this crate implements; the same resolution decides the schema handler's
        // gate and the record-merger factory's, so all three agree or all three
        // refuse.
        match merge_mode.as_str() {
            "COMMIT_TIME_ORDERING" | "EVENT_TIME_ORDERING" => {}
            "CUSTOM" if resolve_custom_merger(&reader_context.table_config).is_some() => {}
            unsupported => {
                return Err(crate::error::CoreError::ReadFileSliceError(format!(
                    "Unsupported merge mode: '{unsupported}'. Only COMMIT_TIME_ORDERING, \
                     EVENT_TIME_ORDERING, and CUSTOM with a supported payload class are \
                     supported (MOR scan path)."
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
            .get(crate::file_group::reader_v2::reader_context::CONFIG_MERGE_TYPE)
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
            // Java's emitDeletes path synthesizes a delete row
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

        // STEP: compact sparsely-pinned source batches before the drain (the
        // batch-pinning safety valve). After log scanning the merge map holds
        // zero-copy BatchRefs that each pin their whole source batch; this
        // releases the dead-row memory of any source batch whose survivors fell
        // below the compaction threshold. A no-op for buffers that don't hold
        // batch refs, and for the common case where survivors keep most batches
        // well-populated.
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
        // Spillable merge map.
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
    // far-future sentinel so a missing watermark reads everything (Java's
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HudiConfigs;
    use crate::error::CoreError;
    use crate::file_group::reader_v2::read_stats::HoodieReadStats;
    use crate::file_group::reader_v2::reader_context::CONFIG_MERGE_TYPE;
    use crate::storage::util::parse_uri;

    /// A context and split that reach the strategy gate without needing storage:
    /// every branch asserted below returns before the log scan.
    fn context_with(merge_mode: &str) -> ReaderContext {
        let mut ctx = ReaderContext::empty();
        ctx.merge_mode = merge_mode.to_string();
        ctx
    }

    fn split() -> InputSplit {
        InputSplit::new(
            Some("f1-0_0-1-1_20240101120000000.parquet".to_string()),
            Some("20240101120000000".to_string()),
            vec![".f1-0_20240101130000000.log.1_0-1-1".to_string()],
            String::new(),
        )
    }

    fn storage() -> Arc<Storage> {
        Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap()
    }

    /// The loader's error, or a panic naming the caller's expectation. The
    /// success value holds trait objects and has no `Debug`, so it cannot go
    /// through `expect_err`.
    async fn load_err(ctx: ReaderContext, params: ReaderParameters, expected: &str) -> CoreError {
        let mut stats = HoodieReadStats::default();
        match DefaultFileGroupRecordBufferLoader::new()
            .get_record_buffer(Arc::new(ctx), storage(), &split(), &params, &mut stats)
            .await
        {
            Ok(_) => panic!("{expected}"),
            Err(e) => e,
        }
    }

    /// A merge mode this reader cannot honor is refused by name, at the one
    /// chokepoint where the configured mode reaches buffer construction.
    /// Silently merging a `CUSTOM` table with the built-in merger would drop its
    /// deletes, so the refusal is the point.
    #[tokio::test]
    async fn test_get_record_buffer_unsupported_merge_mode_is_refused_by_name() {
        let err = load_err(
            context_with("CUSTOM"),
            ReaderParameters::default(),
            "CUSTOM must be refused",
        )
        .await;
        assert!(
            matches!(&err, CoreError::ReadFileSliceError(m) if m.contains("CUSTOM")),
            "the error must name the mode it refused, got: {err}"
        );
    }

    /// The mode is matched case-insensitively, so a table spelling it in lower
    /// case is not mistaken for an unsupported one.
    #[tokio::test]
    async fn test_get_record_buffer_merge_mode_is_case_insensitive() {
        // Reaches the log scan (and fails there on a missing file) rather than
        // being refused at the gate — which is what distinguishes "accepted" from
        // "rejected" without needing a real log file.
        let err = load_err(
            context_with("event_time_ordering"),
            ReaderParameters::default(),
            "the fixture log file does not exist",
        )
        .await;
        assert!(
            !err.to_string().contains("Unsupported merge mode"),
            "a lower-case mode must pass the gate, got: {err}"
        );
    }

    /// An unset merge mode is commit-time ordering, matching what a table that
    /// declares nothing gets elsewhere; it must not be read as unsupported.
    #[tokio::test]
    async fn test_get_record_buffer_empty_merge_mode_defaults_to_commit_time() {
        let err = load_err(
            context_with(""),
            ReaderParameters::default(),
            "the fixture log file does not exist",
        )
        .await;
        assert!(
            !err.to_string().contains("Unsupported merge mode"),
            "an unset mode must default rather than be refused, got: {err}"
        );
    }

    /// `skip_merge` asks for unmerged output, which has no buffer here. Refused
    /// loudly: served by the merging buffer instead, a caller would silently get
    /// merged rows they explicitly asked not to have.
    #[tokio::test]
    async fn test_get_record_buffer_skip_merge_is_refused() {
        let mut ctx = context_with("COMMIT_TIME_ORDERING");
        ctx.hoodie_reader_config
            .insert(CONFIG_MERGE_TYPE.to_string(), "skip_merge".to_string());
        let err = load_err(
            ctx,
            ReaderParameters::default(),
            "skip_merge must be refused",
        )
        .await;
        assert!(
            matches!(&err, CoreError::Unsupported(m) if m.contains("skip_merge")),
            "got: {err}"
        );
    }

    /// Sorted output is not implemented, and the unsorted buffer would answer a
    /// different question.
    #[tokio::test]
    async fn test_get_record_buffer_sort_output_is_refused() {
        let params = ReaderParameters {
            sort_output: true,
            ..Default::default()
        };
        let err = load_err(
            context_with("COMMIT_TIME_ORDERING"),
            params,
            "sort_output must be refused",
        )
        .await;
        assert!(
            matches!(&err, CoreError::Unsupported(m) if m.contains("sort_output")),
            "got: {err}"
        );
    }

    /// Emitting deletes needs delete-row synthesis that is not implemented.
    /// Refused rather than served by the buffer that drops deletes, which would
    /// answer with silence instead of an error.
    #[tokio::test]
    async fn test_get_record_buffer_emit_delete_is_refused() {
        let params = ReaderParameters {
            emit_delete: true,
            ..Default::default()
        };
        let err = load_err(
            context_with("COMMIT_TIME_ORDERING"),
            params,
            "emit_delete must be refused",
        )
        .await;
        assert!(
            matches!(&err, CoreError::Unsupported(m) if m.contains("emit_delete")),
            "got: {err}"
        );
    }

    /// The position-merge gate shares this predicate with
    /// `HoodieFileGroupReader::use_record_position`, so the buffer choice and the
    /// base read's row-index column cannot disagree about what a parquet base
    /// file is. An unset format is parquet — hudi-rs's default.
    #[test]
    fn test_base_file_is_parquet_accepts_unset_and_any_casing() {
        assert!(base_file_is_parquet(""));
        assert!(base_file_is_parquet("parquet"));
        assert!(base_file_is_parquet("PARQUET"));
        assert!(base_file_is_parquet("Parquet"));
        assert!(!base_file_is_parquet("lance"));
        assert!(!base_file_is_parquet("hfile"));
        assert!(!base_file_is_parquet("orc"));
    }
}
