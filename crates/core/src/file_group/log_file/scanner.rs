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
use crate::config::HudiConfigs;
use crate::file_group::log_file::log_block::{BlockType, LogBlock, LogBlockContent};
use crate::file_group::log_file::reader::LogFileReader;
use crate::file_group::record_batches::RecordBatches;
use crate::hfile::HFileRecord;
use crate::storage::Storage;
use crate::timeline::selector::InstantRange;
use crate::Result;
use std::collections::HashSet;
use std::sync::Arc;

/// Result of scanning log files.
///
/// The scanner auto-detects the block type and returns the appropriate variant:
/// - `RecordBatches`: Arrow record batches from Avro/Parquet data blocks (regular table reads)
/// - `HFileRecords`: HFile key-value records from HFile data blocks (metadata table reads)
/// - `Empty`: No data blocks found (only command blocks or empty files)
#[derive(Debug)]
pub enum ScanResult {
    /// Arrow RecordBatches from Avro/Parquet data blocks.
    /// Used for regular table reads.
    RecordBatches(RecordBatches),
    /// HFile key-value records from HFile data blocks.
    /// Used for metadata table reads.
    /// Note: Records are NOT merged by key - caller is responsible for merging.
    HFileRecords(Vec<HFileRecord>),
    /// No data blocks found.
    Empty,
}

impl ScanResult {
    /// Returns true if the result contains Arrow record batches.
    #[must_use]
    pub fn is_record_batches(&self) -> bool {
        matches!(self, ScanResult::RecordBatches(_))
    }

    /// Returns true if the result contains HFile records.
    #[must_use]
    pub fn is_hfile_records(&self) -> bool {
        matches!(self, ScanResult::HFileRecords(_))
    }

    /// Returns true if the result is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, ScanResult::Empty)
    }

    /// Unwrap as RecordBatches, panicking if not that variant.
    #[must_use]
    pub fn unwrap_record_batches(self) -> RecordBatches {
        match self {
            ScanResult::RecordBatches(batches) => batches,
            _ => panic!("called unwrap_record_batches on non-RecordBatches variant"),
        }
    }

    /// Unwrap as HFileRecords, panicking if not that variant.
    #[must_use]
    pub fn unwrap_hfile_records(self) -> Vec<HFileRecord> {
        match self {
            ScanResult::HFileRecords(records) => records,
            _ => panic!("called unwrap_hfile_records on non-HFileRecords variant"),
        }
    }
}

/// Internal content type for detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContentType {
    /// Avro/Parquet/Delete blocks -> RecordBatches
    Records,
    /// HFile blocks -> HFileRecords
    HFile,
    /// No data blocks
    Empty,
}

/// Result of collecting blocks from multiple log files.
struct CollectedBlocks {
    /// All blocks organized by log file.
    all_blocks: Vec<Vec<LogBlock>>,
    /// Instant times that have been rolled back.
    rollback_targets: HashSet<String>,
}

impl CollectedBlocks {
    /// Iterate over all blocks, filtering out rollback blocks and rolled-back data.
    fn iter_valid_blocks(self) -> impl Iterator<Item = LogBlock> {
        let rollback_targets = self.rollback_targets;
        self.all_blocks.into_iter().flatten().filter(move |block| {
            // Skip rollback command blocks (they have no content)
            if block.is_rollback_block() {
                return false;
            }
            // Skip blocks whose instant time was rolled back
            match block.instant_time() {
                Ok(instant) => !rollback_targets.contains(instant),
                // If we can't get the instant time, include the block
                // and let downstream handle the error
                Err(_) => true,
            }
        })
    }
}

#[derive(Debug)]
pub struct LogFileScanner {
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
}

impl LogFileScanner {
    pub fn new(hudi_configs: Arc<HudiConfigs>, storage: Arc<Storage>) -> Self {
        Self {
            hudi_configs,
            storage,
        }
    }

    /// Read all blocks from multiple log files and collect rollback targets.
    async fn collect_blocks(
        &self,
        relative_paths: Vec<String>,
        instant_range: &InstantRange,
    ) -> Result<CollectedBlocks> {
        let mut all_blocks: Vec<Vec<LogBlock>> = Vec::with_capacity(relative_paths.len());
        let mut rollback_targets: HashSet<String> = HashSet::new();

        for path in relative_paths {
            let mut reader =
                LogFileReader::new(self.hudi_configs.clone(), self.storage.clone(), &path).await?;
            let blocks = reader.read_all_blocks(instant_range)?;

            // Collect rollback targets from command blocks
            for block in &blocks {
                if block.is_rollback_block() {
                    rollback_targets.insert(block.target_instant_time()?.to_string());
                }
            }

            all_blocks.push(blocks);
        }

        Ok(CollectedBlocks {
            all_blocks,
            rollback_targets,
        })
    }

    /// Detect the content type from collected blocks.
    ///
    /// Returns an error if the log files contain mixed block types (both Arrow-based
    /// and HFile blocks), which is invalid.
    fn detect_content_type(&self, collected: &CollectedBlocks) -> Result<ContentType> {
        let mut has_record_blocks = false;
        let mut has_hfile_blocks = false;

        for blocks in &collected.all_blocks {
            for block in blocks {
                match block.block_type {
                    BlockType::AvroData | BlockType::ParquetData | BlockType::Delete | BlockType::CdcData => {
                        has_record_blocks = true;
                    }
                    BlockType::HfileData => {
                        has_hfile_blocks = true;
                    }
                    BlockType::Command | BlockType::Corrupted => {
                        // Command and corrupted blocks don't affect content type
                    }
                }

                // Check for mixed types as early as possible
                if has_record_blocks && has_hfile_blocks {
                    return Err(crate::error::CoreError::LogBlockError(
                        "Log files contain mixed block types (both Arrow-based and HFile blocks), which is invalid".into(),
                    ));
                }
            }
        }

        if has_hfile_blocks {
            Ok(ContentType::HFile)
        } else if has_record_blocks {
            Ok(ContentType::Records)
        } else {
            Ok(ContentType::Empty)
        }
    }

    /// Collect Arrow record batches from blocks.
    fn collect_record_batches(&self, collected: CollectedBlocks) -> Result<ScanResult> {
        // Pre-count batches for capacity
        let mut num_data_batches = 0;
        let mut num_delete_batches = 0;
        for blocks in &collected.all_blocks {
            for block in blocks {
                match block.block_type {
                    BlockType::AvroData | BlockType::ParquetData | BlockType::CdcData => {
                        if let Some(records) = block.content.as_records() {
                            num_data_batches += records.num_data_batches();
                        }
                    }
                    BlockType::Delete => {
                        if let Some(records) = block.content.as_records() {
                            num_delete_batches += records.num_delete_batches();
                        }
                    }
                    _ => {}
                }
            }
        }

        // Collect valid record batches
        let mut batches = RecordBatches::new_with_capacity(num_data_batches, num_delete_batches);
        for block in collected.iter_valid_blocks() {
            if let LogBlockContent::Records(records) = block.content {
                batches.extend(records);
            }
        }

        Ok(ScanResult::RecordBatches(batches))
    }

    /// Collect HFile records from blocks.
    fn collect_hfile_records(&self, collected: CollectedBlocks) -> Result<ScanResult> {
        // Pre-count records for capacity
        let mut total_records = 0;
        for blocks in &collected.all_blocks {
            for block in blocks {
                if block.block_type == BlockType::HfileData {
                    if let Some(records) = block.content.as_hfile_records() {
                        total_records += records.len();
                    }
                }
            }
        }

        // Collect valid HFile records
        let mut records = Vec::with_capacity(total_records);
        for block in collected.iter_valid_blocks() {
            if let LogBlockContent::HFileRecords(hfile_records) = block.content {
                records.extend(hfile_records);
            }
        }

        Ok(ScanResult::HFileRecords(records))
    }

    /// Scan log files and return the appropriate content type.
    ///
    /// The scanner auto-detects the block type and returns:
    /// - `ScanResult::RecordBatches` for Avro/Parquet/Delete data blocks (regular table reads)
    /// - `ScanResult::HFileRecords` for HFile data blocks (metadata table reads)
    /// - `ScanResult::Empty` if no data blocks found
    ///
    /// # Errors
    ///
    /// Returns an error if the log files contain mixed block types (both Arrow-based
    /// and HFile blocks), which indicates data corruption or misconfiguration.
    pub async fn scan(
        &self,
        relative_paths: Vec<String>,
        instant_range: &InstantRange,
    ) -> Result<ScanResult> {
        let collected = self.collect_blocks(relative_paths, instant_range).await?;

        // Detect content type from blocks
        let content_type = self.detect_content_type(&collected)?;

        match content_type {
            ContentType::Records => self.collect_record_batches(collected),
            ContentType::HFile => self.collect_hfile_records(collected),
            ContentType::Empty => Ok(ScanResult::Empty),
        }
    }
}
