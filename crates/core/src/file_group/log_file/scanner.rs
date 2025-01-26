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
use crate::file_group::log_file::log_block::LogBlock;
use crate::file_group::log_file::reader::LogFileReader;
use crate::storage::Storage;
use crate::timeline::selector::InstantRange;
use crate::Result;
use arrow_array::RecordBatch;
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
    ) -> Result<Vec<Vec<RecordBatch>>> {
        let mut all_blocks: Vec<Vec<LogBlock>> = Vec::with_capacity(relative_paths.len());
        let mut rollback_targets: HashSet<String> = HashSet::new();

        // collect all blocks and rollback targets
        for path in relative_paths {
            let mut reader =
                LogFileReader::new(self.hudi_configs.clone(), self.storage.clone(), &path).await?;
            let blocks = reader.read_all_blocks(instant_range)?;

            for block in &blocks {
                if block.is_rollback_block() {
                    rollback_targets.insert(block.target_instant_time()?.to_string());
                }
            }

            // only rollback and parquet data blocks are supported
            // TODO: support more block types
            // push the whole vector to avoid cloning
            all_blocks.push(blocks);
        }

        // collect valid record batches
        let mut record_batches: Vec<Vec<RecordBatch>> = Vec::new();
        for blocks in all_blocks {
            for block in blocks {
                if !rollback_targets.contains(block.instant_time()?) {
                    record_batches.push(block.record_batches);
                }
            }
        }

        Ok(record_batches)
    }
}
