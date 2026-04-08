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
use crate::config::HudiConfigs;
use crate::file_group::reader::buffer::HoodieFileGroupRecordBuffer;
use crate::file_group::reader::buffer::key_based::KeyBasedFileGroupRecordBuffer;
use crate::file_group::reader::input_split::InputSplit;
use crate::file_group::reader::merged_log_record_reader::HoodieMergedLogRecordReader;
use crate::file_group::reader::read_stats::HoodieReadStats;
use crate::file_group::reader::reader_parameters::ReaderParameters;
use crate::storage::Storage;
use crate::timeline::selector::InstantRange;
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
        hudi_configs: Arc<HudiConfigs>,
        storage: Arc<Storage>,
        input_split: &InputSplit,
        ordering_field_names: Vec<String>,
        reader_parameters: &ReaderParameters,
        read_stats: &mut HoodieReadStats,
        instant_range: &InstantRange,
        latest_instant_time: &str,
        record_key_field: &str,
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

impl FileGroupRecordBufferLoader for DefaultFileGroupRecordBufferLoader {
    /// Mirrors Java's `DefaultFileGroupRecordBufferLoader.getRecordBuffer(...)`.
    ///
    /// Steps:
    /// 1. Create UpdateProcessor
    /// 2. Instantiate buffer (strategy selection — only KeyBased for now)
    /// 3. Call `scanLogFiles()` to populate the buffer
    async fn get_record_buffer(
        &self,
        hudi_configs: Arc<HudiConfigs>,
        storage: Arc<Storage>,
        input_split: &InputSplit,
        ordering_field_names: Vec<String>,
        _reader_parameters: &ReaderParameters,
        read_stats: &mut HoodieReadStats,
        instant_range: &InstantRange,
        latest_instant_time: &str,
        record_key_field: &str,
    ) -> Result<RecordBufferLoadResult> {
        let merge_mode = "EVENT_TIME_ORDERING".to_string();

        // STEP: Instantiate buffer (strategy selection)
        let record_buffer = Box::new(KeyBasedFileGroupRecordBuffer::new(
            hudi_configs.clone(),
            ordering_field_names,
            merge_mode,
            read_stats,
            record_key_field.to_string(),
        ));

        // STEP: scanLogFiles — build and run HoodieMergedLogRecordReader
        let (populated_buffer, valid_block_instants, stats) = scan_log_files(
            hudi_configs,
            storage,
            input_split,
            record_buffer,
            instant_range,
            latest_instant_time,
        )
        .await?;

        // Populate read stats from scan stats
        read_stats.total_log_read_time_ms = stats.total_time_taken_to_read_and_merge_blocks_ms;
        read_stats.total_log_records = stats.total_log_records;
        read_stats.total_log_blocks = stats.total_log_blocks;
        read_stats.total_log_files_compacted = stats.total_log_files;
        read_stats.total_corrupt_log_blocks = stats.total_corrupt_blocks;
        read_stats.total_rollback_blocks = stats.total_rollbacks;

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
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
    input_split: &InputSplit,
    record_buffer: Box<dyn HoodieFileGroupRecordBuffer>,
    instant_range: &InstantRange,
    latest_instant_time: &str,
) -> Result<(
    Box<dyn HoodieFileGroupRecordBuffer>,
    Vec<String>,
    crate::file_group::reader::merged_log_record_reader::ScanStats,
)> {
    if !input_split.has_log_files() {
        let stats = crate::file_group::reader::merged_log_record_reader::ScanStats::default();
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
    let reader = HoodieMergedLogRecordReader::new_builder()
        .with_hudi_configs(hudi_configs)
        .with_storage(storage)
        .with_log_files(input_split.log_file_paths.clone())
        .with_latest_instant_time(latest_instant_time.to_string())
        .with_instant_range(Some(instant_range.clone()))
        .with_record_buffer(record_buffer)
        .with_force_full_scan(true)
        .build()
        .await?;

    // Decompose: get populated buffer + stats
    Ok(reader.into_parts())
}
