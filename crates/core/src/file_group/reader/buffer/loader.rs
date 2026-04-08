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
//! - `org.apache.hudi.common.table.read.buffer.FileGroupRecordBufferLoader` (interface)
//! - `org.apache.hudi.common.table.read.buffer.LogScanningRecordBufferLoader` (abstract)
//! - `org.apache.hudi.common.table.read.buffer.DefaultFileGroupRecordBufferLoader` (impl)
//!
//! The loader is responsible for:
//! 1. Creating the appropriate buffer implementation (strategy selection)
//! 2. Triggering log file scanning to populate the buffer
//! 3. Returning the populated buffer + valid block instants

use crate::Result;
use crate::config::HudiConfigs;
use crate::file_group::log_file::scanner::{LogFileScanner, ScanResult};
use crate::file_group::reader::buffer::HoodieFileGroupRecordBuffer;
use crate::file_group::reader::buffer::key_based::KeyBasedFileGroupRecordBuffer;
use crate::file_group::reader::input_split::InputSplit;
use crate::file_group::reader::read_stats::HoodieReadStats;
use crate::file_group::reader::reader_parameters::ReaderParameters;
use crate::storage::Storage;
use crate::timeline::selector::InstantRange;
use std::sync::Arc;

/// Result of loading a record buffer: the populated buffer and valid block instants.
pub struct RecordBufferLoadResult {
    pub record_buffer: Box<dyn HoodieFileGroupRecordBuffer>,
    pub valid_block_instants: Vec<String>,
}

/// Trait for loading file group record buffers.
///
/// Mirrors Java's `FileGroupRecordBufferLoader<T>` interface.
///
/// The loader selects the appropriate buffer implementation based on
/// configuration, then populates it by scanning log files.
pub trait FileGroupRecordBufferLoader: Send + Sync + std::fmt::Debug {
    /// Create and populate a record buffer for the given input split.
    ///
    /// # Steps (matching Java's `DefaultFileGroupRecordBufferLoader.getRecordBuffer`):
    /// 1. Read config to determine merge strategy
    /// 2. Create UpdateProcessor
    /// 3. Instantiate the buffer (strategy selection)
    /// 4. Scan log files to populate the buffer
    ///
    /// # Arguments
    /// * `hudi_configs` - Table/reader configuration
    /// * `storage` - Storage access
    /// * `input_split` - Input split describing base file + log files
    /// * `ordering_field_names` - Ordering (precombine) field names
    /// * `reader_parameters` - Reader flags
    /// * `read_stats` - Mutable stats accumulator
    /// * `instant_range` - Time range for log block filtering
    fn get_record_buffer(
        &self,
        hudi_configs: Arc<HudiConfigs>,
        storage: Arc<Storage>,
        input_split: &InputSplit,
        ordering_field_names: Vec<String>,
        reader_parameters: &ReaderParameters,
        read_stats: &mut HoodieReadStats,
        instant_range: &InstantRange,
    ) -> impl std::future::Future<Output = Result<RecordBufferLoadResult>> + Send;
}

/// Default file group record buffer loader.
///
/// Mirrors Java's `DefaultFileGroupRecordBufferLoader<T>` which extends
/// `LogScanningRecordBufferLoader` and implements `FileGroupRecordBufferLoader<T>`.
///
/// ## Buffer strategy selection (decision tree):
/// ```text
///   is_skip_merge?  → UnmergedFileGroupRecordBuffer (not implemented)
///   sort_outputs?   → SortedKeyBasedFileGroupRecordBuffer (not implemented)
///   use_record_position && base_file_present?  → PositionBasedFileGroupRecordBuffer (not impl)
///   DEFAULT         → KeyBasedFileGroupRecordBuffer ★
/// ```
#[derive(Debug)]
pub struct DefaultFileGroupRecordBufferLoader;

impl DefaultFileGroupRecordBufferLoader {
    pub fn new() -> Self {
        Self
    }
}

impl FileGroupRecordBufferLoader for DefaultFileGroupRecordBufferLoader {
    async fn get_record_buffer(
        &self,
        hudi_configs: Arc<HudiConfigs>,
        storage: Arc<Storage>,
        input_split: &InputSplit,
        ordering_field_names: Vec<String>,
        _reader_parameters: &ReaderParameters,
        read_stats: &mut HoodieReadStats,
        instant_range: &InstantRange,
    ) -> Result<RecordBufferLoadResult> {
        // STEP 1-2: Determine merge strategy and create UpdateProcessor
        // (handled inside the buffer constructor)
        let merge_mode = "EVENT_TIME_ORDERING".to_string();

        // STEP 3: Instantiate buffer (strategy selection)
        // Currently only KeyBasedFileGroupRecordBuffer is implemented.
        let mut record_buffer = KeyBasedFileGroupRecordBuffer::new(
            hudi_configs.clone(),
            ordering_field_names,
            merge_mode,
            read_stats,
        );

        // STEP 4: Scan log files to populate the buffer
        // This mirrors LogScanningRecordBufferLoader.scanLogFiles()
        let valid_block_instants = scan_log_files(
            hudi_configs,
            storage,
            input_split,
            &mut record_buffer,
            read_stats,
            instant_range,
        )
        .await?;

        Ok(RecordBufferLoadResult {
            record_buffer: Box::new(record_buffer),
            valid_block_instants,
        })
    }
}

/// Scan log files and populate the record buffer.
///
/// Mirrors `LogScanningRecordBufferLoader.scanLogFiles()`.
///
/// Builds a log record reader (via `LogFileScanner`), which reads all log blocks
/// and calls `record_buffer.process_data_batch()` / `process_delete_batch()`
/// for each block, populating the buffer.
///
/// Returns the list of valid block instants.
async fn scan_log_files(
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
    input_split: &InputSplit,
    record_buffer: &mut KeyBasedFileGroupRecordBuffer,
    read_stats: &mut HoodieReadStats,
    instant_range: &InstantRange,
) -> Result<Vec<String>> {
    if !input_split.has_log_files() {
        return Ok(Vec::new());
    }

    let scanner = LogFileScanner::new(hudi_configs, storage);
    let scan_result = scanner
        .scan(input_split.log_file_paths.clone(), instant_range)
        .await?;

    match scan_result {
        ScanResult::RecordBatches(batches) => {
            // Process data batches
            let num_data = batches.num_data_batches();
            let num_delete = batches.num_delete_batches();
            read_stats.total_log_blocks = (num_data + num_delete) as u64;
            read_stats.total_log_records = (batches.num_data_rows() + batches.num_delete_rows()) as u64;
            read_stats.total_log_files_compacted = input_split.log_file_paths.len() as u64;

            // Feed data batches into the record buffer
            for batch in batches.data_batches {
                record_buffer.process_data_batch(batch, "")?;
            }
            // Feed delete batches into the record buffer
            for (batch, instant_time) in batches.delete_batches {
                record_buffer.process_delete_batch(batch, &instant_time)?;
            }
        }
        ScanResult::HFileRecords(_) => {
            return Err(crate::error::CoreError::LogBlockError(
                "Unexpected HFile records in regular table log file".to_string(),
            ));
        }
        ScanResult::Empty => {}
    }

    // TODO: Return actual valid block instants from the scanner.
    // For now, return empty since the existing LogFileScanner doesn't track this.
    Ok(Vec::new())
}
