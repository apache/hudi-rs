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

    /// Scan log files for Arrow record batches (Avro/Parquet data blocks).
    ///
    /// This method is used for regular table reads where the log files contain
    /// Avro or Parquet encoded data that can be converted to Arrow RecordBatch.
    ///
    /// For metadata table log files with HFile blocks, use `scan_hfile_records` instead.
    pub async fn scan(
        &self,
        relative_paths: Vec<String>,
        instant_range: &InstantRange,
    ) -> Result<RecordBatches> {
        let collected = self.collect_blocks(relative_paths, instant_range).await?;

        // Pre-count batches for capacity and validate block types
        let mut num_data_batches = 0;
        let mut num_delete_batches = 0;
        for blocks in &collected.all_blocks {
            for block in blocks {
                match block.block_type {
                    BlockType::AvroData | BlockType::ParquetData => {
                        if let Some(records) = block.content.as_records() {
                            num_data_batches += records.num_data_batches();
                        }
                    }
                    BlockType::Delete => {
                        if let Some(records) = block.content.as_records() {
                            num_delete_batches += records.num_delete_batches();
                        }
                    }
                    BlockType::Command => {}
                    BlockType::HfileData => {
                        return Err(crate::error::CoreError::LogBlockError(
                            "HFile data blocks should be scanned with scan_hfile_records".into(),
                        ));
                    }
                    _ => {
                        return Err(crate::error::CoreError::LogBlockError(format!(
                            "Unexpected block type: {:?}",
                            block.block_type
                        )));
                    }
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

        Ok(batches)
    }

    /// Scan log files for HFile records (metadata table log files).
    ///
    /// This method is used for metadata table reads where the log files contain
    /// HFile encoded key-value pairs that should NOT be converted to Arrow.
    ///
    /// The returned records are NOT merged by key - the caller (metadata table reader)
    /// is responsible for merging records with the same key (keeping the latest value).
    pub async fn scan_hfile_records(
        &self,
        relative_paths: Vec<String>,
        instant_range: &InstantRange,
    ) -> Result<Vec<HFileRecord>> {
        let collected = self.collect_blocks(relative_paths, instant_range).await?;

        // Pre-count records for capacity and validate block types
        let mut total_records = 0;
        for blocks in &collected.all_blocks {
            for block in blocks {
                match block.block_type {
                    BlockType::HfileData => {
                        if let Some(records) = block.content.as_hfile_records() {
                            total_records += records.len();
                        }
                    }
                    BlockType::Command => {}
                    BlockType::AvroData | BlockType::ParquetData | BlockType::Delete => {
                        return Err(crate::error::CoreError::LogBlockError(
                            "Non-HFile data blocks should be scanned with scan method".into(),
                        ));
                    }
                    _ => {
                        return Err(crate::error::CoreError::LogBlockError(format!(
                            "Unexpected block type: {:?}",
                            block.block_type
                        )));
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

        Ok(records)
    }
}
