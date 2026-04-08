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

//! Redesigned file group reader matching the Apache Hudi open-source class hierarchy.
//!
//! ## Class hierarchy mapping (Java → Rust)
//!
//! ```text
//! Java                                          Rust
//! ────                                          ────
//! HoodieFileGroupReader<T>                   →  HoodieFileGroupReader
//! HoodieReaderContext<T>                     →  (fields on HoodieFileGroupReader)
//! InputSplit                                 →  InputSplit
//! ReaderParameters                           →  ReaderParameters
//! HoodieReadStats                            →  HoodieReadStats
//! IteratorMode                               →  IteratorMode
//! FileGroupReaderSchemaHandler<T>            →  FileGroupReaderSchemaHandler
//! BufferedRecord<T>                          →  BufferedRecord
//! DeleteRecord                               →  DeleteRecord
//! BufferedRecords (factory)                  →  BufferedRecords
//! BufferedRecordConverter<T>                 →  BufferedRecordConverter
//! DeleteContext                               →  DeleteContext
//! BufferedRecordMerger<T>                    →  BufferedRecordMerger (trait)
//! UpdateProcessor<T>                         →  UpdateProcessor (trait)
//!
//! HoodieFileGroupRecordBuffer<T> (interface) →  HoodieFileGroupRecordBuffer (trait)
//! FileGroupRecordBuffer<T> (abstract)        →  FileGroupRecordBuffer (struct, composition)
//! KeyBasedFileGroupRecordBuffer<T>           →  KeyBasedFileGroupRecordBuffer
//!
//! FileGroupRecordBufferLoader<T> (interface) →  FileGroupRecordBufferLoader (trait)
//! LogScanningRecordBufferLoader (abstract)   →  scan_log_files() function
//! DefaultFileGroupRecordBufferLoader<T>      →  DefaultFileGroupRecordBufferLoader
//! ```
//!
//! ## Call stack tree (the implementation window)
//!
//! ```text
//! HoodieFileGroupReader.get_closable_iterator()
//!   └─ get_buffered_record_iterator(IteratorMode)
//!        └─ init_record_iterators()
//!             ├─── make_base_file_iterator()
//!             │      └─ storage.get_parquet_file_data(...)
//!             └─── record_buffer_loader.get_record_buffer(...)
//!                   └─ DefaultFileGroupRecordBufferLoader.get_record_buffer()
//!                         ├─ new KeyBasedFileGroupRecordBuffer(...)
//!                         └─ scan_log_files(...)
//!                              └─ LogFileScanner.scan()
//! ```

pub mod buffer;
pub mod buffered_record;
pub mod delete_context;
pub mod input_split;
pub mod iterator_mode;
pub mod log_record_reader;
pub mod merged_log_record_reader;
pub mod read_stats;
pub mod reader_parameters;
pub mod record_merger;
pub mod schema_handler;
pub mod update_processor;

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::read::HudiReadConfig;
use crate::config::table::HudiTableConfig;
use crate::error::CoreError;
use crate::file_group::reader::buffer::loader::{
    DefaultFileGroupRecordBufferLoader, FileGroupRecordBufferLoader,
};
use crate::file_group::reader::input_split::InputSplit;
use crate::file_group::reader::iterator_mode::IteratorMode;
use crate::file_group::reader::read_stats::HoodieReadStats;
use crate::file_group::reader::reader_parameters::ReaderParameters;
use crate::file_group::reader::schema_handler::FileGroupReaderSchemaHandler;
use crate::storage::Storage;
use crate::timeline::selector::InstantRange;
use arrow_array::RecordBatch;
use std::sync::Arc;

/// The top-level file group reader orchestrator.
///
/// Mirrors Java's `org.apache.hudi.common.table.read.HoodieFileGroupReader<T>`.
///
/// This is the main entry point for reading a file group. It:
/// 1. Accepts an `InputSplit` describing what to read (base file + log files)
/// 2. Creates base file iterators via storage
/// 3. Delegates log scanning + buffer creation to `FileGroupRecordBufferLoader`
/// 4. Merges base file records with log records via the buffer
/// 5. Returns the merged output
///
/// ## Construction
///
/// Use [`HoodieFileGroupReader::builder()`] for the builder pattern, or construct
/// directly with [`HoodieFileGroupReader::new()`].
#[derive(Debug)]
pub struct HoodieFileGroupReader {
    // ── Context (mirrors HoodieReaderContext<T> fields) ────────────────
    /// Hudi table and reader configuration.
    hudi_configs: Arc<HudiConfigs>,

    /// Storage for reading base files and log files.
    storage: Arc<Storage>,

    // ── Input ──────────────────────────────────────────────────────────
    /// Describes what to read: base file, log files, partition path.
    input_split: InputSplit,

    /// Ordering field names for merge conflict resolution (precombine fields).
    ordering_field_names: Vec<String>,

    // ── Configuration ──────────────────────────────────────────────────
    /// Reader flags: use_record_position, emit_delete, sort_output, etc.
    reader_parameters: ReaderParameters,

    /// The latest commit time (high watermark for log block filtering).
    latest_instant_time: String,

    /// Record key field name (e.g. `_hoodie_record_key`).
    record_key_field: String,

    /// Schema management for the read pipeline.
    schema_handler: FileGroupReaderSchemaHandler,

    /// The current iterator mode.
    #[allow(dead_code)]
    iterator_mode: IteratorMode,

    // ── Strategy ───────────────────────────────────────────────────────
    /// Buffer loader: selects buffer impl + triggers log scan.
    record_buffer_loader: DefaultFileGroupRecordBufferLoader,

    // ── Mutable state (populated during read) ──────────────────────────
    /// Read statistics accumulator.
    read_stats: HoodieReadStats,

    /// Valid block instants from log scanning.
    valid_block_instants: Vec<String>,
}

impl HoodieFileGroupReader {
    /// Create a new file group reader.
    pub fn new(
        hudi_configs: Arc<HudiConfigs>,
        storage: Arc<Storage>,
        input_split: InputSplit,
        ordering_field_names: Vec<String>,
        reader_parameters: ReaderParameters,
        latest_instant_time: String,
        record_key_field: String,
    ) -> Self {
        Self {
            hudi_configs,
            storage,
            input_split,
            ordering_field_names,
            reader_parameters,
            latest_instant_time,
            record_key_field,
            schema_handler: FileGroupReaderSchemaHandler::new(),
            iterator_mode: IteratorMode::EngineRecord,
            record_buffer_loader: DefaultFileGroupRecordBufferLoader::new(),
            read_stats: HoodieReadStats::default(),
            valid_block_instants: Vec::new(),
        }
    }

    /// Create a builder for configuring the reader.
    pub fn builder() -> HoodieFileGroupReaderBuilder {
        HoodieFileGroupReaderBuilder::default()
    }

    // =========================================================================
    // Main read API (mirrors Java's getClosableIterator / getBufferedRecordIterator)
    // =========================================================================

    /// Read the file group and return the merged output as a `RecordBatch`.
    ///
    /// This is the main entry point, equivalent to Java's
    /// `getClosableIterator()` → `getBufferedRecordIterator()` → `initRecordIterators()`.
    ///
    /// In Java, this returns a lazy iterator. In Rust/Arrow, we return
    /// the fully merged `RecordBatch` since Arrow's columnar format makes
    /// batch-level merging more efficient than per-record iteration.
    pub async fn read(&mut self) -> Result<RecordBatch> {
        self.init_record_iterators().await
    }

    /// Initialize record iterators: read base file + scan/merge log files.
    ///
    /// Mirrors Java's `HoodieFileGroupReader.initRecordIterators()`.
    ///
    /// ```text
    /// initRecordIterators()
    ///   ├─ makeBaseFileIterator()
    ///   └─ recordBufferLoader.getRecordBuffer(...)
    ///        → recordBuffer.setBaseFileIterator(baseFileIterator)
    /// ```
    async fn init_record_iterators(&mut self) -> Result<RecordBatch> {
        // Step 1: Make base file iterator (read base file data)
        let base_file_batches = self.make_base_file_batches().await?;

        // Step 2: If no records to merge (no log files), return base file data directly
        if self.input_split.has_no_records_to_merge() {
            if base_file_batches.is_empty() {
                return Err(CoreError::ReadFileSliceError(
                    "No base file data to read".to_string(),
                ));
            }
            return arrow::compute::concat_batches(
                &base_file_batches[0].schema(),
                &base_file_batches,
            )
            .map_err(|e| {
                CoreError::ReadFileSliceError(format!(
                    "Failed to concatenate base file batches: {e}"
                ))
            });
        }

        // Step 3: Load record buffer (scan log files + create buffer)
        let instant_range = self.create_instant_range();
        let load_result = self
            .record_buffer_loader
            .get_record_buffer(
                self.hudi_configs.clone(),
                self.storage.clone(),
                &self.input_split,
                self.ordering_field_names.clone(),
                &self.reader_parameters,
                &mut self.read_stats,
                &instant_range,
                &self.latest_instant_time,
                &self.record_key_field,
            )
            .await?;

        let mut record_buffer = load_result.record_buffer;
        self.valid_block_instants = load_result.valid_block_instants;

        // Step 4: Set base file iterator on the buffer
        record_buffer.set_base_file_iterator(base_file_batches);

        // Step 5: Merge and collect
        record_buffer.merge_and_collect()
    }

    /// Read base file data.
    ///
    /// Mirrors Java's `HoodieFileGroupReader.makeBaseFileIterator()` which calls
    /// `readerContext.getFileRecordIterator(...)`.
    async fn make_base_file_batches(&self) -> Result<Vec<RecordBatch>> {
        match &self.input_split.base_file_path {
            Some(path) => {
                let batch = self
                    .storage
                    .get_parquet_file_data(path)
                    .await
                    .map_err(|e| {
                        CoreError::ReadFileSliceError(format!(
                            "Failed to read base file '{path}': {e:?}"
                        ))
                    })?;
                Ok(vec![batch])
            }
            None => Ok(Vec::new()),
        }
    }

    /// Create an InstantRange for log file scanning.
    fn create_instant_range(&self) -> InstantRange {
        let timezone: String = self
            .hudi_configs
            .get_or_default(HudiTableConfig::TimelineTimezone)
            .into();
        let start_timestamp = self
            .hudi_configs
            .try_get(HudiReadConfig::FileGroupStartTimestamp)
            .map(|v| -> String { v.into() });
        let end_timestamp = self
            .hudi_configs
            .try_get(HudiReadConfig::FileGroupEndTimestamp)
            .map(|v| -> String { v.into() });
        InstantRange::new(timezone, start_timestamp, end_timestamp, false, true)
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    /// Returns the read statistics collected during the read.
    pub fn read_stats(&self) -> &HoodieReadStats {
        &self.read_stats
    }

    /// Returns the valid block instants from log scanning.
    pub fn valid_block_instants(&self) -> &[String] {
        &self.valid_block_instants
    }
}

// =========================================================================
// Builder
// =========================================================================

/// Builder for `HoodieFileGroupReader`.
///
/// Mirrors Java's `HoodieFileGroupReader.Builder<T>`.
#[derive(Debug, Default)]
pub struct HoodieFileGroupReaderBuilder {
    hudi_configs: Option<Arc<HudiConfigs>>,
    storage: Option<Arc<Storage>>,
    input_split: Option<InputSplit>,
    ordering_field_names: Vec<String>,
    reader_parameters: ReaderParameters,
    latest_instant_time: Option<String>,
    record_key_field: Option<String>,
    schema_handler: Option<FileGroupReaderSchemaHandler>,
}

impl HoodieFileGroupReaderBuilder {
    pub fn with_configs(mut self, configs: Arc<HudiConfigs>) -> Self {
        self.hudi_configs = Some(configs);
        self
    }

    pub fn with_storage(mut self, storage: Arc<Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    pub fn with_input_split(mut self, input_split: InputSplit) -> Self {
        self.input_split = Some(input_split);
        self
    }

    pub fn with_ordering_field_names(mut self, names: Vec<String>) -> Self {
        self.ordering_field_names = names;
        self
    }

    pub fn with_reader_parameters(mut self, params: ReaderParameters) -> Self {
        self.reader_parameters = params;
        self
    }

    pub fn with_latest_instant_time(mut self, time: String) -> Self {
        self.latest_instant_time = Some(time);
        self
    }

    pub fn with_record_key_field(mut self, field: String) -> Self {
        self.record_key_field = Some(field);
        self
    }

    pub fn with_schema_handler(mut self, handler: FileGroupReaderSchemaHandler) -> Self {
        self.schema_handler = Some(handler);
        self
    }

    pub fn build(self) -> Result<HoodieFileGroupReader> {
        let hudi_configs = self
            .hudi_configs
            .ok_or_else(|| CoreError::ReadFileSliceError("hudi_configs is required".into()))?;
        let storage = self
            .storage
            .ok_or_else(|| CoreError::ReadFileSliceError("storage is required".into()))?;
        let input_split = self
            .input_split
            .ok_or_else(|| CoreError::ReadFileSliceError("input_split is required".into()))?;
        let latest_instant_time = self
            .latest_instant_time
            .unwrap_or_else(|| "99991231235959999".to_string());
        let record_key_field = self
            .record_key_field
            .unwrap_or_else(|| crate::metadata::meta_field::MetaField::RecordKey.as_ref().to_string());

        let mut reader = HoodieFileGroupReader::new(
            hudi_configs,
            storage,
            input_split,
            self.ordering_field_names,
            self.reader_parameters,
            latest_instant_time,
            record_key_field,
        );

        if let Some(handler) = self.schema_handler {
            reader.schema_handler = handler;
        }

        Ok(reader)
    }
}
