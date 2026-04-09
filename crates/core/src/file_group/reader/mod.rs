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
//! HoodieReaderContext<T>                     →  ReaderContext (fields on HoodieFileGroupReader)
//! InputSplit                                 →  InputSplit
//! ReaderParameters                           →  ReaderParameters
//! HoodieReadStats                            →  HoodieReadStats
//! FileGroupReaderSchemaHandler<T>            →  FileGroupReaderSchemaHandler
//! OutputProjector (= Java outputConverter)   →  OutputProjector
//! BufferedRecord<T>                          →  BufferedRecord
//!
//! HoodieFileGroupRecordBuffer<T> (interface) →  HoodieFileGroupRecordBuffer (trait)
//! KeyBasedFileGroupRecordBuffer<T>           →  KeyBasedFileGroupRecordBuffer
//!
//! FileGroupRecordBufferLoader<T> (interface) →  FileGroupRecordBufferLoader (trait)
//! DefaultFileGroupRecordBufferLoader<T>      →  DefaultFileGroupRecordBufferLoader
//! ```
//!
//! ## Call stack tree (matching Java 1:1)
//!
//! ```text
//! HoodieFileGroupReader constructor
//!   ├─ new FileGroupReaderSchemaHandler(tableSchema, requestedSchema, ...)
//!   │    └─ prepareRequiredSchema()
//!   │         └─ generateRequiredSchema()
//!   │              └─ getMandatoryFieldsForMerging()
//!   ├─ this.outputConverter = schemaHandler.getOutputConverter()
//!   └─ this.orderingFieldNames = ...
//!
//! HoodieFileGroupReader.read()
//!   └─ initRecordIterators()
//!        ├─ makeBaseFileIterator()
//!        │    └─ storage.get_parquet_file_data(tableSchema, requiredSchema)
//!        ├─ if hasNoRecordsToMerge: return (+ outputConverter)
//!        └─ recordBufferLoader.getRecordBuffer(...)
//!             ├─ new KeyBasedFileGroupRecordBuffer(...)
//!             └─ scanLogFiles(...)    ← readerSchema = requiredSchema
//!
//! HoodieFileGroupReader.next()
//!   └─ if outputConverter.isPresent(): nextVal.project(outputConverter)
//! ```

pub mod buffer;
pub mod buffered_record;
pub mod delete_context;
pub mod input_split;
pub mod iterator_mode;
pub mod log_record_reader;
pub mod merged_log_record_reader;
pub mod read_stats;
pub mod reader_context;
pub mod reader_parameters;
pub mod record_context;
pub mod record_merger;
pub mod schema_handler;
pub mod update_processor;

use crate::Result;
use crate::error::CoreError;
use crate::file_group::reader::buffer::loader::{
    DefaultFileGroupRecordBufferLoader, FileGroupRecordBufferLoader,
};
use crate::file_group::reader::input_split::InputSplit;
use crate::file_group::reader::read_stats::HoodieReadStats;
use crate::file_group::reader::reader_context::ReaderContext;
use crate::file_group::reader::reader_parameters::ReaderParameters;
use crate::file_group::reader::schema_handler::{FileGroupReaderSchemaHandler, OutputProjector};
use crate::storage::{ParquetReadOptions, Storage};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::sync::Arc;

// =========================================================================
// HoodieFileGroupReader
// =========================================================================

/// The top-level file group reader orchestrator.
///
/// Mirrors Java's `HoodieFileGroupReader<T>` (line 70).
///
/// ## Construction (mirrors Java constructor, lines 91–123)
///
/// The constructor creates the `FileGroupReaderSchemaHandler` eagerly and
/// stores the `outputConverter`, matching Java:
/// ```java
/// readerContext.setSchemaHandler(new FileGroupReaderSchemaHandler<>(...));
/// this.outputConverter = readerContext.getSchemaHandler().getOutputConverter();
/// ```
#[derive(Debug)]
pub struct HoodieFileGroupReader {
    // ── Context (mirrors Java's readerContext) ────────────────────────
    reader_context: Arc<ReaderContext>,
    storage: Arc<Storage>,

    // ── Input ────────────────────────────────────────────────────────
    input_split: InputSplit,

    /// Ordering field names for merge conflict resolution.
    /// Java: `this.orderingFieldNames = HoodieRecordUtils.getOrderingFieldNames(...)` (line 121)
    ordering_field_names: Vec<String>,

    // ── Configuration ────────────────────────────────────────────────
    reader_parameters: ReaderParameters,

    /// Schema management. Created eagerly in constructor, matching Java line 117–119.
    schema_handler: FileGroupReaderSchemaHandler,

    /// Output projection: requiredSchema → requestedSchema.
    /// Java: `private final Option<UnaryOperator<T>> outputConverter` (line 83).
    /// Created in constructor via `schemaHandler.getOutputConverter()` (line 120).
    output_converter: Option<OutputProjector>,

    // ── Strategy ─────────────────────────────────────────────────────
    record_buffer_loader: DefaultFileGroupRecordBufferLoader,

    // ── Mutable state ────────────────────────────────────────────────
    read_stats: HoodieReadStats,
    valid_block_instants: Vec<String>,
}

impl HoodieFileGroupReader {
    /// Create a new file group reader — mirrors Java constructor (lines 91–123).
    ///
    /// ## Parameters (replacing Java's `metaClient`)
    ///
    /// Java receives `dataSchema` and `requestedSchema` from the caller (e.g., Spark).
    /// In Rust, `table_schema` replaces `dataSchema`, and `requested_schema` is optional
    /// (None = read all columns = no projection).
    pub fn new(
        reader_context: Arc<ReaderContext>,
        storage: Arc<Storage>,
        table_schema: SchemaRef,
        requested_schema: Option<SchemaRef>,
        input_split: InputSplit,
        ordering_field_names: Vec<String>,
        reader_parameters: ReaderParameters,
    ) -> Result<Self> {
        log::debug!(
            "HoodieFileGroupReader::new partition={} base_file={} log_files={} \
             ordering_fields={:?} latest_commit_time={} record_key_field={}",
            input_split.partition_path,
            input_split.base_file_path.as_deref().unwrap_or("<none>"),
            input_split.log_file_paths.len(),
            ordering_field_names,
            reader_context.latest_commit_time,
            reader_context.record_key_field,
        );
        for (i, lf) in input_split.log_file_paths.iter().enumerate() {
            log::debug!("  log_file[{i}]: {lf}");
        }

        // Java line 117–119: create schema handler (eagerly computes requiredSchema)
        let schema_handler = FileGroupReaderSchemaHandler::new(
            table_schema,
            requested_schema,
            &reader_context.record_key_field,
            &reader_context.merge_mode,
            input_split.has_log_files(),
            reader_context.instant_range.is_some(),
        )?;

        // Java line 120: this.outputConverter = schemaHandler.getOutputConverter()
        let output_converter = schema_handler.get_output_converter();

        Ok(Self {
            reader_context,
            storage,
            input_split,
            ordering_field_names,
            reader_parameters,
            schema_handler,
            output_converter,
            record_buffer_loader: DefaultFileGroupRecordBufferLoader::new(),
            read_stats: HoodieReadStats::default(),
            valid_block_instants: Vec::new(),
        })
    }

    /// Create a builder for configuring the reader.
    pub fn builder() -> HoodieFileGroupReaderBuilder {
        HoodieFileGroupReaderBuilder::default()
    }

    // =========================================================================
    // Main read API
    // =========================================================================

    /// Read the file group and return the merged output.
    ///
    /// Mirrors Java's `getClosableIterator()` → `initRecordIterators()`.
    pub async fn read(&mut self) -> Result<RecordBatch> {
        self.init_record_iterators().await
    }

    /// Initialize record iterators: read base file + scan/merge log files.
    ///
    /// Mirrors Java's `initRecordIterators()` (lines 128–139).
    ///
    /// NO schema computation here — that happened in the constructor.
    async fn init_record_iterators(&mut self) -> Result<RecordBatch> {
        log::debug!(
            "[HoodieFileGroupReader] initRecordIterators: partition={} base_file={} log_files={}",
            self.input_split.partition_path,
            self.input_split.base_file_path.as_deref().unwrap_or("<none>"),
            self.input_split.log_file_paths.len(),
        );

        // ── Java line 129: makeBaseFileIterator() ─────────────────────
        let base_file_batches = self.make_base_file_batches().await?;
        let base_rows: usize = base_file_batches.iter().map(|b| b.num_rows()).sum();
        log::debug!(
            "[HoodieFileGroupReader] makeBaseFileIterator: {} batches, {} total rows",
            base_file_batches.len(),
            base_rows,
        );

        // ── Java line 130–131: if hasNoRecordsToMerge → return ────────
        if self.input_split.has_no_records_to_merge() {
            log::debug!(
                "[HoodieFileGroupReader] no log files → returning base file data ({base_rows} rows)"
            );
            if base_file_batches.is_empty() {
                return Err(CoreError::ReadFileSliceError(
                    "No base file data to read".to_string(),
                ));
            }
            let result = arrow::compute::concat_batches(
                &base_file_batches[0].schema(),
                &base_file_batches,
            )
            .map_err(|e| {
                CoreError::ReadFileSliceError(format!(
                    "Failed to concatenate base file batches: {e}"
                ))
            })?;
            // Java: next() applies outputConverter. We apply it here (batch equivalent).
            return self.apply_output_converter(result);
        }

        // ── Java line 134–137: recordBufferLoader.getRecordBuffer() ───
        log::debug!(
            "[HoodieFileGroupReader] scanning {} log file(s) with latest_commit_time={}",
            self.input_split.log_file_paths.len(),
            self.reader_context.latest_commit_time,
        );

        // Java line 143: BaseHoodieLogRecordReader.readerSchema = schemaHandler.getRequiredSchema()
        // Pass requiredSchema to the log reader chain for block-level projection.
        let reader_schema = if self.schema_handler.get_required_schema()
            != self.schema_handler.get_table_schema()
        {
            Some(self.schema_handler.get_required_schema().clone())
        } else {
            None // no projection: requiredSchema == tableSchema
        };

        let load_result = self
            .record_buffer_loader
            .get_record_buffer(
                self.reader_context.clone(),
                self.storage.clone(),
                &self.input_split,
                self.ordering_field_names.clone(),
                &self.reader_parameters,
                &mut self.read_stats,
                reader_schema,
            )
            .await?;

        let mut record_buffer = load_result.record_buffer;
        self.valid_block_instants = load_result.valid_block_instants;

        log::debug!(
            "[HoodieFileGroupReader] log scan complete: buffer_size={} \
             stats: log_blocks={} log_records={}",
            record_buffer.size(),
            self.read_stats.total_log_blocks,
            self.read_stats.total_log_records,
        );

        // ── Java line 138: recordBuffer.setBaseFileIterator(baseFileIterator)
        record_buffer.set_base_file_iterator(base_file_batches);

        // ── Java: hasNext()/next() loop + outputConverter ─────────────
        let result = record_buffer.merge_and_collect()?;
        log::debug!(
            "[HoodieFileGroupReader] merge_and_collect: {} rows, {} columns",
            result.num_rows(),
            result.num_columns(),
        );

        // Java line 263–265: if (outputConverter.isPresent()) nextVal.project(outputConverter)
        self.apply_output_converter(result)
    }

    /// Read base file data — mirrors Java's `makeBaseFileIterator()` (lines 142–170).
    ///
    /// Java passes `(tableSchema, requiredSchema)` to the parquet reader:
    /// ```java
    /// readerContext.getFileRecordIterator(
    ///     schemaHandler.getTableSchema(),
    ///     schemaHandler.getRequiredSchema(),
    ///     storage)
    /// ```
    async fn make_base_file_batches(&self) -> Result<Vec<RecordBatch>> {
        match &self.input_split.base_file_path {
            Some(path) => {
                let required_cols = self.schema_handler.required_column_names();
                let is_projection_active = self.schema_handler.get_required_schema()
                    != self.schema_handler.get_table_schema();

                let batch = if is_projection_active {
                    log::debug!(
                        "[HoodieFileGroupReader] reading base file with projection: [{}]",
                        required_cols.join(", "),
                    );
                    let options =
                        ParquetReadOptions::new().with_projection(required_cols.iter().cloned());
                    let batch = self
                        .storage
                        .get_parquet_file_data_projected(path, options)
                        .await
                        .map_err(|e| {
                            CoreError::ReadFileSliceError(format!(
                                "Failed to read base file '{path}' with projection: {e:?}"
                            ))
                        })?;
                    // Reorder columns to match required_columns order.
                    // (ProjectionMask preserves parquet file order, not our requested order)
                    let schema = batch.schema();
                    let indices: Vec<usize> = required_cols
                        .iter()
                        .filter_map(|name| schema.index_of(name).ok())
                        .collect();
                    batch.project(&indices).map_err(|e| {
                        CoreError::ReadFileSliceError(format!(
                            "Failed to reorder base file columns: {e}"
                        ))
                    })?
                } else {
                    self.storage
                        .get_parquet_file_data(path)
                        .await
                        .map_err(|e| {
                            CoreError::ReadFileSliceError(format!(
                                "Failed to read base file '{path}': {e:?}"
                            ))
                        })?
                };
                Ok(vec![batch])
            }
            None => Ok(Vec::new()),
        }
    }

    /// Apply output converter — mirrors Java's `next()` (lines 261–267):
    /// ```java
    /// if (outputConverter.isPresent()) {
    ///     return nextVal.project(outputConverter.get());
    /// }
    /// return nextVal;
    /// ```
    fn apply_output_converter(&self, batch: RecordBatch) -> Result<RecordBatch> {
        match &self.output_converter {
            Some(converter) => converter.project(batch),
            None => Ok(batch),
        }
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    pub fn read_stats(&self) -> &HoodieReadStats {
        &self.read_stats
    }

    pub fn valid_block_instants(&self) -> &[String] {
        &self.valid_block_instants
    }

    pub fn schema_handler(&self) -> &FileGroupReaderSchemaHandler {
        &self.schema_handler
    }
}

// =========================================================================
// Builder
// =========================================================================

/// Builder for `HoodieFileGroupReader`.
///
/// Callers are responsible for providing `table_schema` when projection is needed.
/// In Java, the caller (e.g., Spark plan) always provides both schemas.
/// In Rust, callers read the schema from parquet metadata before building.
#[derive(Debug, Default)]
pub struct HoodieFileGroupReaderBuilder {
    reader_context: Option<Arc<ReaderContext>>,
    storage: Option<Arc<Storage>>,
    table_schema: Option<SchemaRef>,
    requested_schema: Option<SchemaRef>,
    input_split: Option<InputSplit>,
    ordering_field_names: Vec<String>,
    reader_parameters: ReaderParameters,
}

impl HoodieFileGroupReaderBuilder {
    pub fn with_reader_context(mut self, ctx: Arc<ReaderContext>) -> Self {
        self.reader_context = Some(ctx);
        self
    }

    pub fn with_storage(mut self, storage: Arc<Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Set the full table schema (equivalent to Java's `dataSchema` parameter).
    pub fn with_table_schema(mut self, schema: SchemaRef) -> Self {
        self.table_schema = Some(schema);
        self
    }

    /// Set the requested schema for column projection.
    pub fn with_requested_schema(mut self, schema: SchemaRef) -> Self {
        self.requested_schema = Some(schema);
        self
    }

    /// Convenience: set requested columns by name.
    /// Requires `table_schema` to be set first (to resolve field types).
    pub fn with_requested_columns(mut self, columns: Vec<String>) -> Self {
        if let Some(ref table) = self.table_schema {
            let fields: Vec<_> = columns
                .iter()
                .filter_map(|name| table.field_with_name(name).ok().cloned())
                .map(Arc::new)
                .collect();
            self.requested_schema =
                Some(Arc::new(arrow_schema::Schema::new(fields)));
        }
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

    pub fn build(self) -> Result<HoodieFileGroupReader> {
        let reader_context = self
            .reader_context
            .ok_or_else(|| CoreError::ReadFileSliceError("reader_context is required".into()))?;
        let storage = self
            .storage
            .ok_or_else(|| CoreError::ReadFileSliceError("storage is required".into()))?;
        let input_split = self
            .input_split
            .ok_or_else(|| CoreError::ReadFileSliceError("input_split is required".into()))?;
        let table_schema = self
            .table_schema
            .ok_or_else(|| CoreError::ReadFileSliceError("table_schema is required".into()))?;

        HoodieFileGroupReader::new(
            reader_context,
            storage,
            table_schema,
            self.requested_schema,
            input_split,
            self.ordering_field_names,
            self.reader_parameters,
        )
    }
}
