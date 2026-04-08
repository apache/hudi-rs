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

//! Mirrors `org.apache.hudi.common.table.read.buffer.FileGroupRecordBuffer`.
//!
//! Abstract base containing the common state for all record buffer variants:
//! the records map, the merger, the delete context, and the update processor.
//!
//! In Java, this is an abstract class. In Rust, it's a struct that holds
//! the common fields, used via composition by concrete implementations.

use crate::file_group::reader::delete_context::DeleteContext;
use crate::file_group::reader::record_merger::BufferedRecordMerger;
use crate::file_group::reader::update_processor::UpdateProcessor;
use crate::file_group::record_batches::RecordBatches;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
/// Common state for all file group record buffer implementations.
///
/// Mirrors Java's `FileGroupRecordBuffer<T>` abstract class.
///
/// In Java, the records are stored in an `ExternalSpillableMap` for disk
/// spilling. In Rust, we use `RecordBatches` to accumulate Arrow batches
/// and perform batch-level merging (more efficient for columnar data).
#[derive(Debug)]
pub struct FileGroupRecordBuffer {
    /// The reader schema.
    pub reader_schema: Option<SchemaRef>,

    /// Names of the ordering (precombine) fields.
    pub ordering_field_names: Vec<String>,

    /// The merge mode string (e.g. "COMMIT_TIME_ORDERING").
    pub record_merge_mode: String,

    /// The record merger for resolving conflicts.
    pub buffered_record_merger: Box<dyn BufferedRecordMerger>,

    /// Context for detecting delete records.
    pub delete_context: Option<DeleteContext>,

    /// Processor for update records during merge iteration.
    pub update_processor: Box<dyn UpdateProcessor>,

    /// Accumulated log data and delete batches.
    pub log_record_batches: RecordBatches,

    /// Base file record batches (set after log scanning, before merge iteration).
    pub base_file_batches: Vec<RecordBatch>,

    /// Total number of log records processed.
    pub total_log_records: u64,
}

impl FileGroupRecordBuffer {
    pub fn new(
        ordering_field_names: Vec<String>,
        record_merge_mode: String,
        buffered_record_merger: Box<dyn BufferedRecordMerger>,
        update_processor: Box<dyn UpdateProcessor>,
    ) -> Self {
        Self {
            reader_schema: None,
            ordering_field_names,
            record_merge_mode,
            buffered_record_merger,
            delete_context: None,
            update_processor,
            log_record_batches: RecordBatches::new(),
            base_file_batches: Vec::new(),
            total_log_records: 0,
        }
    }
}
