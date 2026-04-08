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
use crate::file_group::reader::buffer::HoodieFileGroupRecordBuffer;
use crate::file_group::reader::log_record_reader::BaseHoodieLogRecordReader;
use crate::file_group::reader::reader_context::ReaderContext;
use crate::storage::Storage;
use std::sync::Arc;

/// Statistics from the log scanning operation.
#[derive(Debug, Clone, Default)]
pub struct ScanStats {
    pub total_time_taken_to_read_and_merge_blocks_ms: u64,
    pub num_merged_records_in_log: u64,
    pub total_log_files: u64,
    pub total_log_blocks: u64,
    pub total_log_records: u64,
    pub total_corrupt_blocks: u64,
    pub total_rollbacks: u64,
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
    total_time_taken_to_read_and_merge_blocks_ms: u64,
}

impl std::fmt::Debug for HoodieMergedLogRecordReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HoodieMergedLogRecordReader")
            .field("num_merged_records", &self.num_merged_records_in_log)
            .field("total_time_ms", &self.total_time_taken_to_read_and_merge_blocks_ms)
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
    /// Mirrors Java's `performScan()`:
    /// ```java
    /// timer.startTimer();
    /// scanInternal(keySpecOpt, false);
    /// this.totalTimeTakenToReadAndMergeBlocks = timer.endTimer();
    /// this.numMergedRecordsInLog = recordBuffer.size();
    /// ```
    async fn perform_scan(&mut self) -> Result<()> {
        let start = std::time::Instant::now();

        self.base.scan_internal().await?;

        self.total_time_taken_to_read_and_merge_blocks_ms =
            start.elapsed().as_millis() as u64;
        self.num_merged_records_in_log = self.base.record_buffer.size() as u64;

        log::info!(
            "Number of log files scanned => {}",
            self.base.log_file_paths.len()
        );
        log::info!(
            "Number of entries in Map => {}",
            self.base.record_buffer.size()
        );

        Ok(())
    }

    /// Decompose the reader after scanning, returning the populated buffer,
    /// valid block instants, and scan statistics.
    pub fn into_parts(
        self,
    ) -> (
        Box<dyn HoodieFileGroupRecordBuffer>,
        Vec<String>,
        ScanStats,
    ) {
        let stats = ScanStats {
            total_time_taken_to_read_and_merge_blocks_ms: self
                .total_time_taken_to_read_and_merge_blocks_ms,
            num_merged_records_in_log: self.num_merged_records_in_log,
            total_log_files: self.base.total_log_files,
            total_log_blocks: self.base.total_log_blocks,
            total_log_records: self.base.total_log_records,
            total_corrupt_blocks: self.base.total_corrupt_blocks,
            total_rollbacks: self.base.total_rollbacks,
        };

        let valid_instants = self.base.valid_block_instants;
        let buffer = self.base.record_buffer;

        (buffer, valid_instants, stats)
    }

    pub fn get_num_merged_records_in_log(&self) -> u64 {
        self.num_merged_records_in_log
    }

    pub fn get_total_time_taken_to_read_and_merge_blocks(&self) -> u64 {
        self.total_time_taken_to_read_and_merge_blocks_ms
    }

    pub fn get_total_log_files(&self) -> u64 {
        self.base.total_log_files
    }

    pub fn get_total_log_records(&self) -> u64 {
        self.base.total_log_records
    }

    pub fn get_total_log_blocks(&self) -> u64 {
        self.base.total_log_blocks
    }

    pub fn get_total_corrupt_blocks(&self) -> u64 {
        self.base.total_corrupt_blocks
    }

    pub fn get_total_rollbacks(&self) -> u64 {
        self.base.total_rollbacks
    }

    pub fn get_valid_block_instants(&self) -> &[String] {
        &self.base.valid_block_instants
    }
}

/// Builder for `HoodieMergedLogRecordReader`.
///
/// Mirrors Java's `HoodieMergedLogRecordReader.Builder<T>`.
///
/// ## Required fields:
/// - `hudi_configs`
/// - `storage`
/// - `record_buffer`
///
/// ## Builder flow:
/// `build()` constructs the reader and calls `perform_scan()` when
/// `force_full_scan=true` (the default, matching Java).
#[derive(Default)]
pub struct Builder {
    reader_context: Option<Arc<ReaderContext>>,
    storage: Option<Arc<Storage>>,
    log_file_paths: Vec<String>,
    latest_instant_time: Option<String>,
    record_buffer: Option<Box<dyn HoodieFileGroupRecordBuffer>>,
    force_full_scan: bool,
    allow_inflight_instants: bool,
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

    /// Build and optionally perform scan.
    ///
    /// Mirrors Java's `build()` which calls the constructor, and the
    /// constructor calls `performScan()` when `forceFullScan=true`.
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

        let base = BaseHoodieLogRecordReader {
            reader_context,
            storage,
            log_file_paths: self.log_file_paths,
            latest_instant_time: self
                .latest_instant_time
                .unwrap_or_else(|| "99991231235959999".to_string()),
            record_buffer,
            allow_inflight_instants: self.allow_inflight_instants,
            valid_block_instants: Vec::new(),
            total_log_files: 0,
            total_log_blocks: 0,
            total_log_records: 0,
            total_corrupt_blocks: 0,
            total_rollbacks: 0,
        };

        let mut reader = HoodieMergedLogRecordReader {
            base,
            num_merged_records_in_log: 0,
            total_time_taken_to_read_and_merge_blocks_ms: 0,
        };

        if self.force_full_scan {
            reader.perform_scan().await?;
        }

        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::reader::buffer::key_based::KeyBasedFileGroupRecordBuffer;
    use crate::file_group::reader::read_stats::HoodieReadStats;
    use crate::storage::util::parse_uri;
    use std::collections::HashMap;

    fn make_test_reader_context() -> Arc<ReaderContext> {
        Arc::new(ReaderContext::empty())
    }

    fn make_test_buffer() -> Box<dyn HoodieFileGroupRecordBuffer> {
        let ctx = make_test_reader_context();
        let stats = HoodieReadStats::default();
        Box::new(KeyBasedFileGroupRecordBuffer::new(
            ctx,
            vec![],
            "COMMIT_TIME_ORDERING".to_string(),
            &stats,
            "_hoodie_record_key".to_string(),
        ))
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
        assert_eq!(
            reader.get_total_time_taken_to_read_and_merge_blocks(),
            0
        );
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
