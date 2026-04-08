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

//! Record buffer hierarchy for the file group reader.
//!
//! Mirrors the Java package `org.apache.hudi.common.table.read.buffer`.
//!
//! ## Interface Hierarchy
//!
//! ```text
//!   «trait» HoodieFileGroupRecordBuffer
//!       │
//!       │ implements
//!       ▼
//!   FileGroupRecordBuffer  (base struct with common state)
//!       │
//!       ├── KeyBasedFileGroupRecordBuffer   (KEY_BASED_MERGE, default)
//!       ├── [PositionBasedFileGroupRecordBuffer]   (future)
//!       ├── [SortedKeyBasedFileGroupRecordBuffer]  (future)
//!       └── [UnmergedFileGroupRecordBuffer]         (future)
//! ```

pub mod key_based;
pub mod loader;
pub mod record_buffer;

pub use key_based::KeyBasedFileGroupRecordBuffer;
pub use loader::{DefaultFileGroupRecordBufferLoader, FileGroupRecordBufferLoader};
pub use record_buffer::FileGroupRecordBuffer;

use crate::Result;
use arrow_array::RecordBatch;

/// The type of merge buffer in use.
///
/// Mirrors Java's `HoodieFileGroupRecordBuffer.BufferType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferType {
    /// Key-based merge: deduplicates by record key, keeps latest by ordering value.
    KeyBasedMerge,

    /// Position-based merge: uses record positions for merging (not yet implemented).
    PositionBasedMerge,

    /// Unmerged: skips merge, emits all records (not yet implemented).
    Unmerged,
}

/// Trait for file group record buffers.
///
/// Mirrors Java's `HoodieFileGroupRecordBuffer<T>` interface.
///
/// During log scanning, `process_data_block` and `process_delete_block` are
/// called to populate the buffer. After scanning, `set_base_file_iterator` is
/// called to provide base file records. Then `has_next`/`next` iterate the
/// merged output.
pub trait HoodieFileGroupRecordBuffer: Send + Sync + std::fmt::Debug {
    /// Returns the buffer type.
    fn buffer_type(&self) -> BufferType;

    /// Process a data block from log scanning.
    ///
    /// In Java, this takes a `HoodieDataBlock`. In Rust, we receive the
    /// already-deserialized `RecordBatch` from log file reading.
    fn process_data_batch(
        &mut self,
        batch: RecordBatch,
        instant_time: &str,
    ) -> Result<()>;

    /// Process a delete block from log scanning.
    ///
    /// In Java, this takes a `HoodieDeleteBlock`. In Rust, we receive the
    /// delete batch and instant time from log file reading.
    fn process_delete_batch(
        &mut self,
        batch: RecordBatch,
        instant_time: &str,
    ) -> Result<()>;

    /// Set the base file record batches for merge iteration.
    fn set_base_file_batches(&mut self, batches: Vec<RecordBatch>);

    /// Returns true if there is a log record for the given key.
    fn contains_log_record(&self, record_key: &str) -> bool;

    /// Returns the number of records in the buffer.
    fn size(&self) -> usize;

    /// Returns the total number of log records processed.
    fn total_log_records(&self) -> u64;

    /// Consume the buffer and produce the merged output as a single `RecordBatch`.
    ///
    /// This is the Rust-idiomatic way of "iterating" - we do a batch-level merge
    /// rather than per-record iteration, since Arrow is columnar.
    fn merge_and_collect(self: Box<Self>) -> Result<RecordBatch>;
}
