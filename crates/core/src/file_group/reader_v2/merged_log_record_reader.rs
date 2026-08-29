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

//! Mirrors `org.apache.hudi.common.table.log.HoodieMergedLogRecordReader`.
//!
//! Thin wrapper around [`BaseHoodieLogRecordReader`] that adds:
//! - Builder pattern (matching Java's `HoodieMergedLogRecordReader.Builder<T>`)
//! - Timing of the scan operation
//! - `performScan()` invoked from constructor when `forceFullScan=true`
//!
//! ## Java hierarchy:
//! ```text
//! BaseHoodieLogRecordReader<T>  (abstract scan engine)
//!   └─ HoodieMergedLogRecordReader<T>  (this class — adds timing + builder)
//! ```

use crate::Result;
use crate::error::CoreError;
use crate::file_group::reader_v2::buffer::HoodieFileGroupRecordBuffer;
use crate::file_group::reader_v2::log_record_reader::BaseHoodieLogRecordReader;
use crate::file_group::reader_v2::reader_context::{CompletionGateInputs, ReaderContext};
use crate::storage::Storage;
use crate::timeline::selector::InstantRange;
use std::sync::Arc;

/// Statistics from the log scanning operation.
#[derive(Debug, Clone, Default)]
pub struct ScanStats {
    pub total_time_taken_to_read_and_merge_blocks_us: u64,
    /// Parity with Java's scan result; the reader reports its own counters.
    #[allow(dead_code)]
    pub num_merged_records_in_log: u64,
    pub total_log_files: u64,
    pub total_log_blocks: u64,
    pub total_log_records: u64,
    pub total_corrupt_blocks: u64,
    pub total_rollbacks: u64,

    // ── Stage timings (perf harness) ───────────────────────
    /// Wall us walking log-block headers off storage (Pass 1).
    pub log_block_read_us: u64,
    /// Wall us fetching admitted blocks' content (Pass 3's prefetch).
    pub log_block_fetch_us: u64,
    /// Wall us decoding fetched bytes into arrow batches.
    pub log_block_decode_us: u64,
    /// Wall us upserting decoded records into the merge map.
    pub merge_upsert_us: u64,
    /// Wall us dispatching blocks in Pass 3, spanning decode and upsert.
    pub merge_insert_us: u64,
    /// Peak merge-map entry count observed during the scan.
    pub merge_map_peak_entries: u64,
    /// True if the merge map spilled to disk during the scan.
    pub merge_map_spilled: bool,
    /// Peak in-memory byte estimate of the merge map during the scan.
    pub merge_map_peak_in_memory_bytes: u64,
}

/// Merged log record reader with timing and builder pattern.
///
/// Mirrors Java's `HoodieMergedLogRecordReader<T>`.
///
/// ## Usage (matching Java):
/// ```ignore
/// let reader = HoodieMergedLogRecordReader::new_builder()
///     .with_reader_context(reader_context)
///     .with_storage(storage)
///     .with_log_files(log_file_paths)
///     .with_latest_instant_time(latest_time)
///     .with_instant_range(Some(range))
///     .with_record_buffer(buffer)
///     .with_allow_inflight_instants(false)
///     .build()
///     .await?;
///
/// let (buffer, valid_instants, stats) = reader.into_parts();
/// ```
pub struct HoodieMergedLogRecordReader {
    pub(crate) base: BaseHoodieLogRecordReader,
    num_merged_records_in_log: u64,
    total_time_taken_to_read_and_merge_blocks_us: u64,
}

impl std::fmt::Debug for HoodieMergedLogRecordReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HoodieMergedLogRecordReader")
            .field("num_merged_records", &self.num_merged_records_in_log)
            .field(
                "total_time_us",
                &self.total_time_taken_to_read_and_merge_blocks_us,
            )
            .finish()
    }
}

impl HoodieMergedLogRecordReader {
    /// Create a new builder.
    ///
    /// Mirrors Java's `HoodieMergedLogRecordReader.newBuilder()`.
    pub fn new_builder() -> Builder {
        Builder::default()
    }

    /// Scan delta-log files processing blocks.
    ///
    /// Mirrors Java's `scan()`:
    /// ```java
    /// public final void scan() { scan(false); }
    /// ```
    ///
    /// Java-parity entry point. `scan_log_files` drives the reader through
    /// `build()`, which scans eagerly, so nothing calls this directly.
    #[allow(dead_code)]
    pub async fn scan(&mut self) -> Result<()> {
        self.scan_with_skip(false).await
    }

    /// Scan with control over block processing.
    ///
    /// Mirrors Java's `scan(boolean skipProcessingBlocks)`:
    /// ```java
    /// public final void scan(boolean skipProcessingBlocks) {
    ///     if (forceFullScan) { return; } // already scanned in constructor
    ///     scanInternal(Option.empty(), skipProcessingBlocks);
    /// }
    /// ```
    pub async fn scan_with_skip(&mut self, skip_processing_blocks: bool) -> Result<()> {
        if self.base.force_full_scan {
            // When full-scan is enforced, scanning is invoked upfront (during initialization)
            return Ok(());
        }
        self.base.scan_internal(skip_processing_blocks).await
    }

    /// Mirrors Java's `performScan()`:
    /// ```java
    /// private void performScan() {
    ///     timer.startTimer();
    ///     Option<KeySpec> keySpecOpt = createKeySpec(readerContext.getKeyFilterOpt());
    ///     scanInternal(keySpecOpt, false);
    ///     this.totalTimeTakenToReadAndMergeBlocks = timer.endTimer();
    ///     this.numMergedRecordsInLog = recordBuffer.size();
    /// }
    /// ```
    async fn perform_scan(&mut self) -> Result<()> {
        let start = std::time::Instant::now();

        // KeySpec filtering not yet implemented in Rust; pass skip=false
        self.base.scan_internal(false).await?;

        self.total_time_taken_to_read_and_merge_blocks_us = start.elapsed().as_micros() as u64;
        self.num_merged_records_in_log = self.base.record_buffer.size() as u64;

        log::debug!(
            "Number of log files scanned => {}",
            self.base.log_file_paths.len()
        );
        log::debug!(
            "Number of entries in Map => {}",
            self.base.record_buffer.size()
        );

        Ok(())
    }

    /// Decompose the reader after scanning, returning the populated buffer,
    /// valid block instants, and scan statistics.
    pub fn into_parts(self) -> (Box<dyn HoodieFileGroupRecordBuffer>, Vec<String>, ScanStats) {
        let stats = ScanStats {
            total_time_taken_to_read_and_merge_blocks_us: self
                .total_time_taken_to_read_and_merge_blocks_us,
            num_merged_records_in_log: self.num_merged_records_in_log,
            total_log_files: self.base.total_log_files,
            total_log_blocks: self.base.total_log_blocks,
            total_log_records: self.base.total_log_records,
            total_corrupt_blocks: self.base.total_corrupt_blocks,
            total_rollbacks: self.base.total_rollbacks,
            log_block_read_us: self.base.log_block_read_us,
            merge_insert_us: self.base.merge_insert_us,
            log_block_fetch_us: self.base.log_block_fetch_us,
            log_block_decode_us: self.base.log_block_decode_us,
            merge_upsert_us: self.base.merge_upsert_us,
            merge_map_peak_entries: self.base.record_buffer.merge_map_peak_entries(),
            merge_map_spilled: self.base.record_buffer.merge_map_spilled(),
            merge_map_peak_in_memory_bytes: self
                .base
                .record_buffer
                .merge_map_peak_in_memory_bytes(),
        };

        let valid_instants = self.base.valid_block_instants;
        let buffer = self.base.record_buffer;

        (buffer, valid_instants, stats)
    }
}

/// Java-parity accessors over the scan result.
///
/// `into_parts` hands the caller the whole stats struct, so nothing in this
/// crate reaches for them one at a time; they exist so a port of Java's call
/// sites reads the same.
#[allow(dead_code)]
impl HoodieMergedLogRecordReader {
    pub fn get_num_merged_records_in_log(&self) -> u64 {
        self.num_merged_records_in_log
    }

    pub fn get_total_time_taken_to_read_and_merge_blocks(&self) -> u64 {
        self.total_time_taken_to_read_and_merge_blocks_us
    }

    pub fn get_total_log_files(&self) -> u64 {
        self.base.get_total_log_files()
    }

    pub fn get_total_log_records(&self) -> u64 {
        self.base.get_total_log_records()
    }

    pub fn get_total_log_blocks(&self) -> u64 {
        self.base.get_total_log_blocks()
    }

    pub fn get_total_corrupt_blocks(&self) -> u64 {
        self.base.get_total_corrupt_blocks()
    }

    pub fn get_total_rollbacks(&self) -> u64 {
        self.base.get_total_rollbacks()
    }

    pub fn get_valid_block_instants(&self) -> &[String] {
        self.base.get_valid_block_instants()
    }

    pub fn get_progress(&self) -> f32 {
        self.base.get_progress()
    }
}

/// Builder for `HoodieMergedLogRecordReader`.
///
/// Mirrors Java's `HoodieMergedLogRecordReader.Builder<T> extends BaseHoodieLogRecordReader.Builder<T>`.
///
/// ## Required fields:
/// - `reader_context`
/// - `storage`
/// - `record_buffer`
///
/// ## Builder flow:
/// `build()` constructs the reader and calls `perform_scan()` when
/// `force_full_scan=true` (the default, matching Java).
pub struct Builder {
    reader_context: Option<Arc<ReaderContext>>,
    storage: Option<Arc<Storage>>,
    log_file_paths: Vec<String>,
    latest_instant_time: Option<String>,
    instant_range: Option<InstantRange>,
    /// Whether instant_range was explicitly set via `with_instant_range()`.
    instant_range_explicitly_set: bool,
    record_buffer: Option<Box<dyn HoodieFileGroupRecordBuffer>>,
    /// By default true, matching Java: `private boolean forceFullScan = true;`
    force_full_scan: bool,
    allow_inflight_instants: bool,
    /// Inputs for the Gate-3 completed/inflight check; `Some` only for
    /// table version < 8. `None` (default) leaves Gate 3 a no-op.
    completion_gate_inputs: Option<Arc<CompletionGateInputs>>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            reader_context: None,
            storage: None,
            log_file_paths: Vec::new(),
            latest_instant_time: None,
            instant_range: None,
            instant_range_explicitly_set: false,
            record_buffer: None,
            force_full_scan: true,
            allow_inflight_instants: false,
            completion_gate_inputs: None,
        }
    }
}

impl Builder {
    /// Mirrors Java's `withHoodieReaderContext(readerContext)`.
    pub fn with_reader_context(mut self, ctx: Arc<ReaderContext>) -> Self {
        self.reader_context = Some(ctx);
        self
    }

    /// Mirrors Java's `withStorage(HoodieStorage)`.
    pub fn with_storage(mut self, storage: Arc<Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Mirrors Java's `withLogFiles(List<HoodieLogFile>)`.
    pub fn with_log_files(mut self, paths: Vec<String>) -> Self {
        self.log_file_paths = paths;
        self
    }

    pub fn with_latest_instant_time(mut self, time: String) -> Self {
        self.latest_instant_time = Some(time);
        self
    }

    /// Mirrors Java's `withInstantRange(Option<InstantRange>)`.
    /// Builder parity with Java; the range travels through `ReaderContext` here.
    #[allow(dead_code)]
    pub fn with_instant_range(mut self, range: Option<InstantRange>) -> Self {
        self.instant_range = range;
        self.instant_range_explicitly_set = true;
        self
    }

    /// Mirrors Java's `withRecordBuffer(HoodieFileGroupRecordBuffer<T>)`.
    pub fn with_record_buffer(mut self, buffer: Box<dyn HoodieFileGroupRecordBuffer>) -> Self {
        self.record_buffer = Some(buffer);
        self
    }

    /// Mirrors Java's `withForceFullScan(boolean)`.
    pub fn with_force_full_scan(mut self, force: bool) -> Self {
        self.force_full_scan = force;
        self
    }

    /// Mirrors Java's `withAllowInflightInstants(boolean)`.
    pub fn with_allow_inflight_instants(mut self, allow: bool) -> Self {
        self.allow_inflight_instants = allow;
        self
    }

    /// Supply the Gate-3 completed/inflight sets. Callers pass `Some` only for
    /// table version < 8; `None` (the default) leaves Gate 3 disabled.
    pub fn with_completion_gate_inputs(
        mut self,
        inputs: Option<Arc<CompletionGateInputs>>,
    ) -> Self {
        self.completion_gate_inputs = inputs;
        self
    }

    /// Build and optionally perform scan.
    ///
    /// Mirrors Java's `build()` which calls the constructor, and the
    /// constructor calls `performScan()` when `forceFullScan=true`.
    ///
    /// If `instant_range` was not explicitly set, it is derived from
    /// `reader_context.instant_range` (matching Java's pattern where
    /// the caller passes `readerContext.getInstantRange()` to the builder).
    pub async fn build(self) -> Result<HoodieMergedLogRecordReader> {
        let reader_context = self
            .reader_context
            .ok_or_else(|| CoreError::ReadFileSliceError("reader_context required".into()))?;
        let storage = self
            .storage
            .ok_or_else(|| CoreError::ReadFileSliceError("storage required".into()))?;
        let record_buffer = self
            .record_buffer
            .ok_or_else(|| CoreError::ReadFileSliceError("record_buffer required".into()))?;

        // Derive instant_range from reader_context if not explicitly set
        let instant_range = if self.instant_range_explicitly_set {
            self.instant_range
        } else {
            reader_context.instant_range.clone()
        };

        let base = BaseHoodieLogRecordReader {
            reader_context,
            storage,
            log_file_paths: self.log_file_paths,
            latest_instant_time: self
                .latest_instant_time
                .unwrap_or_else(|| super::MAX_INSTANT_TIME.to_string()),
            instant_range,
            force_full_scan: self.force_full_scan,
            record_buffer,
            allow_inflight_instants: self.allow_inflight_instants,
            completion_gate_inputs: self.completion_gate_inputs,
            valid_block_instants: Vec::new(),
            total_log_files: 0,
            total_log_blocks: 0,
            total_log_records: 0,
            total_corrupt_blocks: 0,
            total_rollbacks: 0,
            progress: 0.0,
            log_block_read_us: 0,
            merge_insert_us: 0,
            log_block_fetch_us: 0,
            log_block_decode_us: 0,
            merge_upsert_us: 0,
        };

        let mut reader = HoodieMergedLogRecordReader {
            base,
            num_merged_records_in_log: 0,
            total_time_taken_to_read_and_merge_blocks_us: 0,
        };

        // Mirrors Java constructor: if (forceFullScan) { performScan(); }
        if self.force_full_scan {
            reader.perform_scan().await?;
        }

        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::log_file::log_block::BlockType;
    use crate::file_group::reader_v2::MAX_INSTANT_TIME;
    use crate::file_group::reader_v2::buffer::key_based::KeyBasedFileGroupRecordBuffer;
    use crate::storage::util::parse_uri;

    fn make_test_reader_context() -> Arc<ReaderContext> {
        let mut ctx = ReaderContext::empty();
        // Prepare schema handler with DeleteContext for buffer construction.
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "_hoodie_record_key",
                arrow_schema::DataType::Utf8,
                false,
            )]));
        let mut handler =
            crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler::new()
                .with_table_schema(schema.clone())
                .with_data_schema(schema);
        handler
            .prepare_required_schema(
                true,
                &["_hoodie_record_key".to_string()],
                &[],
                &ctx.table_config,
                false,
                "COMMIT_TIME_ORDERING",
            )
            .unwrap();
        ctx.schema_handler = handler;
        Arc::new(ctx)
    }

    fn make_test_buffer() -> Box<dyn HoodieFileGroupRecordBuffer> {
        let ctx = make_test_reader_context();
        Box::new(
            KeyBasedFileGroupRecordBuffer::new(ctx, "COMMIT_TIME_ORDERING".to_string(), false)
                .unwrap(),
        )
    }

    /// Copy a log-file fixture into a temp dir, so a test can rewrite its bytes.
    fn fixture_copy() -> (tempfile::TempDir, String) {
        let file_name =
            ".ff32ab89-5ad0-4968-83b4-89a34c95d32f-0_20250316025816068.log.1_0-54-122".to_string();
        let src = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/log_files/valid_log_avro_data")
            .join(&file_name);
        let dir = tempfile::tempdir().unwrap();
        std::fs::copy(&src, dir.path().join(&file_name)).unwrap();
        (dir, file_name)
    }

    fn storage_over(dir: &tempfile::TempDir) -> Arc<Storage> {
        Storage::new_with_base_url(parse_uri(dir.path().to_str().unwrap()).unwrap()).unwrap()
    }

    /// Overwrite the payload of every data block, leaving the magic, lengths,
    /// header and footer intact — well-formed on the outside, undecodable on the
    /// inside, which is what a block from a rolled-back instant can look like.
    /// Returns the instant time the blocks carry.
    async fn make_payloads_undecodable(dir: &tempfile::TempDir, file_name: &str) -> String {
        let configs = Arc::new(crate::config::HudiConfigs::new(
            std::collections::HashMap::<String, String>::new(),
        ));
        let mut reader = crate::file_group::log_file::reader::LogFileReader::new_streaming(
            configs,
            storage_over(dir),
            file_name,
        )
        .await
        .unwrap();
        let blocks = reader
            .read_all_blocks_metadata_only_unbounded()
            .await
            .unwrap();

        let path = dir.path().join(file_name);
        let mut bytes = std::fs::read(&path).unwrap();
        let mut instant = String::new();
        for block in &blocks {
            if !matches!(
                block.block_type,
                BlockType::AvroData | BlockType::ParquetData
            ) {
                continue;
            }
            instant = block.instant_time().unwrap().to_string();
            let location = &block.deferred_content.as_ref().unwrap().location;
            // The content range opens with the 8-byte content-length field, which
            // inflation reads back, so only the bytes after it are overwritten.
            let payload_start = location.content_position
                + if block.format_version.has_content_length() {
                    8
                } else {
                    0
                };
            let end = (location.content_position + location.content_length) as usize;
            bytes[payload_start as usize..end].fill(0xFF);
        }
        assert!(!instant.is_empty(), "fixture must carry a data block");
        std::fs::write(&path, &bytes).unwrap();
        instant
    }

    async fn scan_with(
        dir: &tempfile::TempDir,
        file_name: &str,
        latest_instant_time: &str,
    ) -> Result<HoodieMergedLogRecordReader> {
        HoodieMergedLogRecordReader::new_builder()
            .with_reader_context(make_test_reader_context())
            .with_storage(storage_over(dir))
            .with_log_files(vec![file_name.to_string()])
            .with_latest_instant_time(latest_instant_time.to_string())
            .with_record_buffer(make_test_buffer())
            .with_force_full_scan(true)
            .build()
            .await
    }

    /// The undecodable payload is genuinely fatal when a gate admits its instant.
    /// Pairs with the test below: without this one, that test could pass because
    /// the bytes still decode rather than because the gate ran first.
    #[tokio::test]
    async fn test_undecodable_block_fails_the_read_when_admitted() {
        let (dir, file_name) = fixture_copy();
        scan_with(&dir, &file_name, MAX_INSTANT_TIME)
            .await
            .expect("the intact fixture reads");

        make_payloads_undecodable(&dir, &file_name).await;
        assert!(
            scan_with(&dir, &file_name, MAX_INSTANT_TIME).await.is_err(),
            "an admitted block with an undecodable payload must fail the read"
        );
    }

    /// A block that cannot be decoded, inside an instant the gates discard, does
    /// not fail the read: Pass 1 walks headers, so a block that is never admitted
    /// is never fetched or decoded.
    #[tokio::test]
    async fn test_undecodable_block_in_a_discarded_instant_does_not_fail_the_read() {
        let (dir, file_name) = fixture_copy();
        let instant = make_payloads_undecodable(&dir, &file_name).await;

        // Gate 2 discards any non-command block above `latest_instant_time`.
        let below_every_instant = "0".repeat(instant.len());
        let reader = scan_with(&dir, &file_name, &below_every_instant)
            .await
            .expect("a discarded block must not fail the read");
        assert_eq!(reader.get_total_log_files(), 1);
        assert_eq!(
            reader.get_num_merged_records_in_log(),
            0,
            "the discarded block contributes no records"
        );
    }

    /// Java: TestHoodieMergedLogRecordReader — builder validation
    ///
    /// Given: Builder without record_buffer
    /// When:  build()
    /// Then:  Error returned
    #[tokio::test]
    async fn test_builder_requires_record_buffer() {
        let ctx = make_test_reader_context();
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();

        let result = HoodieMergedLogRecordReader::new_builder()
            .with_reader_context(ctx)
            .with_storage(storage)
            .with_force_full_scan(false)
            .build()
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("record_buffer"),
            "Error should mention record_buffer: {err}"
        );
    }

    /// Given: Builder without reader_context
    /// When:  build()
    /// Then:  Error returned
    #[tokio::test]
    async fn test_builder_requires_reader_context() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let buffer = make_test_buffer();

        let result = HoodieMergedLogRecordReader::new_builder()
            .with_storage(storage.clone())
            .with_record_buffer(buffer)
            .with_force_full_scan(false)
            .build()
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("reader_context"),
            "Error should mention reader_context: {err}"
        );
    }

    /// Given: Builder with empty log files (no scan needed)
    /// When:  build() with force_full_scan=true
    /// Then:  Reader created successfully, num_merged_records=0
    #[tokio::test]
    async fn test_perform_scan_empty_log_files() {
        let ctx = make_test_reader_context();
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let buffer = make_test_buffer();

        let reader = HoodieMergedLogRecordReader::new_builder()
            .with_reader_context(ctx)
            .with_storage(storage.clone())
            .with_log_files(vec![]) // empty
            .with_record_buffer(buffer)
            .with_force_full_scan(true)
            .build()
            .await
            .unwrap();

        assert_eq!(reader.get_num_merged_records_in_log(), 0);
        assert_eq!(reader.get_total_log_files(), 0);
        assert_eq!(reader.get_total_log_blocks(), 0);
    }

    /// Given: Builder with force_full_scan=false
    /// When:  build()
    /// Then:  Reader created without scanning (lazy mode)
    #[tokio::test]
    async fn test_builder_lazy_mode() {
        let ctx = make_test_reader_context();
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let buffer = make_test_buffer();

        let reader = HoodieMergedLogRecordReader::new_builder()
            .with_reader_context(ctx)
            .with_storage(storage.clone())
            .with_record_buffer(buffer)
            .with_force_full_scan(false)
            .build()
            .await
            .unwrap();

        // In lazy mode, no scan performed
        assert_eq!(reader.get_num_merged_records_in_log(), 0);
        assert_eq!(reader.get_total_time_taken_to_read_and_merge_blocks(), 0);
    }

    /// into_parts() returns buffer, valid instants, and stats.
    #[tokio::test]
    async fn test_into_parts_returns_components() {
        let ctx = make_test_reader_context();
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let buffer = make_test_buffer();

        let reader = HoodieMergedLogRecordReader::new_builder()
            .with_reader_context(ctx)
            .with_storage(storage.clone())
            .with_log_files(vec![])
            .with_record_buffer(buffer)
            .with_force_full_scan(true)
            .build()
            .await
            .unwrap();

        let (buffer, valid_instants, stats) = reader.into_parts();
        assert_eq!(buffer.size(), 0);
        assert!(valid_instants.is_empty());
        assert_eq!(stats.total_log_files, 0);
    }
}
