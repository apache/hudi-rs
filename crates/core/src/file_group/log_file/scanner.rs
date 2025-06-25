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
use crate::file_group::log_file::log_block::{BlockType, LogBlock};
use crate::file_group::log_file::reader::LogFileReader;
use crate::file_group::record_batches::RecordBatches;
use crate::storage::Storage;
use crate::timeline::selector::InstantRange;
use crate::Result;
use std::collections::HashSet;
use std::sync::Arc;

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

    pub async fn scan(
        &self,
        relative_paths: Vec<String>,
        instant_range: &InstantRange,
    ) -> Result<RecordBatches> {
        let mut all_blocks: Vec<Vec<LogBlock>> = Vec::with_capacity(relative_paths.len());
        let mut rollback_targets: HashSet<String> = HashSet::new();
        let mut num_data_batches = 0;
        let mut num_delete_batches = 0;

        // collect all blocks and rollback targets
        for path in relative_paths {
            let mut reader =
                LogFileReader::new(self.hudi_configs.clone(), self.storage.clone(), &path).await?;
            let blocks = reader.read_all_blocks(instant_range)?;

            for block in &blocks {
                match block.block_type {
                    BlockType::AvroData | BlockType::ParquetData => {
                        num_data_batches += block.record_batches.num_data_batches();
                    }
                    BlockType::Delete => {
                        num_delete_batches += block.record_batches.num_delete_batches()
                    }
                    BlockType::Command => {
                        if block.is_rollback_block() {
                            rollback_targets.insert(block.target_instant_time()?.to_string());
                        }
                    }
                    _ => {
                        return Err(crate::error::CoreError::LogBlockError(format!(
                            "Unexpected block type: {:?}",
                            block.block_type
                        )));
                    }
                }
            }

            // push the whole vector to avoid cloning
            // since we pre-allocate the capacity
            // based on the number of relative paths
            all_blocks.push(blocks);
        }

        // collect valid record batches
        let mut batches = RecordBatches::new_with_capacity(num_data_batches, num_delete_batches);
        for blocks in all_blocks {
            for block in blocks {
                if block.is_rollback_block() {
                    // Skip rollback blocks which contain no record batches
                    continue;
                }

                if !rollback_targets.contains(block.instant_time()?) {
                    batches.extend(block.record_batches);
                } else {
                    // If the block is a rollback target, we skip it
                    // as it indicates that the data is no longer valid.
                }
            }
        }

        Ok(batches)
    }
}
