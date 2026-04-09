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
//! ## Interface Hierarchy (matches Java 1:1)
//!
//! ```text
//!   «trait» HoodieFileGroupRecordBuffer          (Java interface)
//!       │
//!       │ implements
//!       ▼
//!   FileGroupRecordBuffer  (common state struct)  (Java abstract class)
//!       │
//!       ├── KeyBasedFileGroupRecordBuffer          (KEY_BASED_MERGE, default)
//!       ├── [PositionBasedFileGroupRecordBuffer]    (future)
//!       ├── [SortedKeyBasedFileGroupRecordBuffer]   (future)
//!       └── [UnmergedFileGroupRecordBuffer]          (future)
//! ```

pub mod key_based;
pub mod loader;
pub mod record_buffer;
pub mod row_extraction;

pub use key_based::KeyBasedFileGroupRecordBuffer;
pub use loader::{DefaultFileGroupRecordBufferLoader, FileGroupRecordBufferLoader};
pub use record_buffer::FileGroupRecordBuffer;

use crate::Result;
use crate::file_group::log_file::log_block::LogBlock;
use crate::file_group::reader::buffered_record::{BufferedRecord, DeleteRecord};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::collections::HashMap;

/// The type of merge buffer in use.
///
/// Mirrors Java's `HoodieFileGroupRecordBuffer.BufferType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferType {
    /// Key-based merge: deduplicates by record key, keeps latest by ordering value.
    KeyBasedMerge,
    /// Position-based merge (not yet implemented).
    PositionBasedMerge,
    /// Unmerged: skips merge (not yet implemented).
    Unmerged,
}

/// Trait for file group record buffers.
///
/// Mirrors Java's `HoodieFileGroupRecordBuffer<T>` interface.
///
/// Method names match Java 1:1:
/// - `processDataBlock` → `process_data_block`
/// - `processNextDataRecord` → `process_next_data_record`
/// - `processDeleteBlock` → `process_delete_block`
/// - `processNextDeletedRecord` → `process_next_deleted_record`
/// - `containsLogRecord` → `contains_log_record`
/// - `setBaseFileIterator` → `set_base_file_iterator`
/// - `hasNext` / `next` → `has_next` / `next`
pub trait HoodieFileGroupRecordBuffer: Send + Sync + std::fmt::Debug {
    /// Returns the buffer type.
    fn get_buffer_type(&self) -> BufferType;

    /// Process a data block from log scanning.
    ///
    /// Mirrors Java's `processDataBlock(HoodieDataBlock, Option<KeySpec>)`.
    /// The buffer is responsible for inflating the block and extracting records,
    /// matching Java where inflate/deserialize/deflate happens inside the block
    /// triggered by the buffer's `getRecordsIterator`.
    ///
    /// `reader_schema` mirrors Java's `dataBlock.readerSchema` field — the required
    /// schema set on the block by `BaseHoodieLogRecordReader` (Java line 143).
    /// In Java, `GenericDatumReader(writerSchema, readerSchema)` projects during
    /// Avro deserialization. In Rust, we project post-inflate to the same effect.
    fn process_data_block(
        &mut self,
        block: &mut LogBlock,
        reader_schema: Option<&SchemaRef>,
    ) -> Result<()>;

    /// Process a single data record within a data block.
    ///
    /// Mirrors Java's `processNextDataRecord(BufferedRecord<T>, Serializable)`.
    fn process_next_data_record(
        &mut self,
        record: BufferedRecord,
        key: &str,
    ) -> Result<()>;

    /// Process a delete block from log scanning.
    ///
    /// Mirrors Java's `processDeleteBlock(HoodieDeleteBlock)`.
    /// The buffer inflates the block and extracts delete records internally.
    fn process_delete_block(
        &mut self,
        block: &mut LogBlock,
    ) -> Result<()>;

    /// Process a single deleted record within a delete block.
    ///
    /// Mirrors Java's `processNextDeletedRecord(DeleteRecord, Serializable)`.
    fn process_next_deleted_record(
        &mut self,
        delete_record: DeleteRecord,
        key: &str,
    );

    /// Check if a record exists in the buffered records.
    ///
    /// Mirrors Java's `containsLogRecord(String)`.
    fn contains_log_record(&self, record_key: &str) -> bool;

    /// Returns the number of records in the buffer.
    fn size(&self) -> usize;

    /// Returns the total number of log records processed.
    fn get_total_log_records(&self) -> u64;

    /// Returns the underlying records map.
    fn get_log_records(&self) -> &HashMap<String, BufferedRecord>;

    /// Set the base file batches for merge iteration.
    ///
    /// Mirrors Java's `setBaseFileIterator(ClosableIterator<T>)`.
    fn set_base_file_iterator(&mut self, batches: Vec<RecordBatch>);

    /// Check if next merged record exists.
    ///
    /// Mirrors Java's `hasNext()`.
    fn has_next(&mut self) -> Result<bool>;

    /// Return the next merged buffered record.
    ///
    /// Mirrors Java's `next()`.
    fn next(&mut self) -> Option<BufferedRecord>;

    /// Consume the buffer and produce the merged output as a `RecordBatch`.
    ///
    /// This is Rust-specific — Java uses the `hasNext()`/`next()` iterator instead.
    fn merge_and_collect(self: Box<Self>) -> Result<RecordBatch>;
}
