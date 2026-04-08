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

//! Mirrors `org.apache.hudi.common.table.read.buffer.KeyBasedFileGroupRecordBuffer`.
//!
//! The default record buffer for file group reading. Deduplicates records by
//! record key and resolves conflicts using ordering value comparison.
//!
//! ## How it's selected
//!
//! In `DefaultFileGroupRecordBufferLoader`:
//! ```text
//! !is_skip_merge && !sort_outputs && !(use_record_position && base_file_present)
//!   → KeyBasedFileGroupRecordBuffer (DEFAULT)
//! ```

use crate::Result;
use crate::config::HudiConfigs;
use crate::file_group::reader::buffer::{BufferType, HoodieFileGroupRecordBuffer};
use crate::file_group::reader::buffer::record_buffer::FileGroupRecordBuffer;
use crate::file_group::reader::record_merger::BufferedRecordMergerFactory;
use crate::file_group::reader::update_processor::create_update_processor;
use crate::file_group::reader::read_stats::HoodieReadStats;
use crate::file_group::record_batches::RecordBatches;
use crate::merge::record_merger::RecordMerger;
use arrow_array::RecordBatch;
use std::sync::Arc;

/// Key-based file group record buffer.
///
/// The default merge buffer. Accumulates log records indexed by record key,
/// deduplicating by ordering value. During the final merge iteration,
/// base file records are merged with their corresponding log records.
///
/// ## Interface hierarchy
/// ```text
/// «trait» HoodieFileGroupRecordBuffer
///     └─ KeyBasedFileGroupRecordBuffer
///          (contains FileGroupRecordBuffer for common state)
/// ```
#[derive(Debug)]
pub struct KeyBasedFileGroupRecordBuffer {
    /// Common buffer state (records map, merger, delete context, etc.).
    pub base: FileGroupRecordBuffer,

    /// Hudi configuration for driving the merge.
    pub hudi_configs: Arc<HudiConfigs>,
}

impl KeyBasedFileGroupRecordBuffer {
    pub fn new(
        hudi_configs: Arc<HudiConfigs>,
        ordering_field_names: Vec<String>,
        merge_mode: String,
        read_stats: &HoodieReadStats,
    ) -> Self {
        let merger = BufferedRecordMergerFactory::create(&merge_mode);
        let update_processor = create_update_processor(read_stats, false);

        Self {
            base: FileGroupRecordBuffer::new(
                ordering_field_names,
                merge_mode,
                merger,
                update_processor,
            ),
            hudi_configs,
        }
    }
}

impl HoodieFileGroupRecordBuffer for KeyBasedFileGroupRecordBuffer {
    fn buffer_type(&self) -> BufferType {
        BufferType::KeyBasedMerge
    }

    fn process_data_batch(
        &mut self,
        batch: RecordBatch,
        _instant_time: &str,
    ) -> Result<()> {
        self.base.total_log_records += batch.num_rows() as u64;
        self.base.log_record_batches.push_data_batch(batch);
        Ok(())
    }

    fn process_delete_batch(
        &mut self,
        batch: RecordBatch,
        instant_time: &str,
    ) -> Result<()> {
        self.base.total_log_records += batch.num_rows() as u64;
        self.base
            .log_record_batches
            .push_delete_batch(batch, instant_time.to_string());
        Ok(())
    }

    fn set_base_file_batches(&mut self, batches: Vec<RecordBatch>) {
        self.base.base_file_batches = batches;
    }

    fn contains_log_record(&self, _record_key: &str) -> bool {
        // Per-key lookup not supported in batch mode.
        // This would require row-level indexing which we avoid for performance.
        false
    }

    fn size(&self) -> usize {
        self.base.log_record_batches.num_data_rows()
    }

    fn total_log_records(&self) -> u64 {
        self.base.total_log_records
    }

    fn merge_and_collect(self: Box<Self>) -> Result<RecordBatch> {
        let base_batches = self.base.base_file_batches;
        let log_batches = self.base.log_record_batches;

        // If no log records, just concatenate base batches
        if log_batches.num_data_rows() == 0 && log_batches.num_delete_rows() == 0 {
            if base_batches.is_empty() {
                return Err(crate::error::CoreError::ReadFileSliceError(
                    "No base file or log records to merge".to_string(),
                ));
            }
            return arrow::compute::concat_batches(
                &base_batches[0].schema(),
                &base_batches,
            ).map_err(|e| {
                crate::error::CoreError::ReadFileSliceError(format!(
                    "Failed to concatenate base batches: {e}"
                ))
            });
        }

        // Combine base and log batches, then use RecordMerger
        let schema = if !base_batches.is_empty() {
            base_batches[0].schema()
        } else if log_batches.num_data_batches() > 0 {
            // Try to get schema from first data batch
            log_batches
                .data_batches
                .first()
                .map(|b| b.schema())
                .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()))
        } else {
            return Err(crate::error::CoreError::ReadFileSliceError(
                "No schema available for merge".to_string(),
            ));
        };

        let num_data = base_batches.len() + log_batches.num_data_batches();
        let num_delete = log_batches.num_delete_batches();
        let mut all_batches = RecordBatches::new_with_capacity(num_data, num_delete);

        for batch in base_batches {
            all_batches.push_data_batch(batch);
        }
        all_batches.extend(log_batches);

        let merger = RecordMerger::new(schema, self.hudi_configs);
        merger.merge_record_batches(all_batches)
    }
}
