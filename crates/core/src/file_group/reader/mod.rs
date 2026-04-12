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
pub mod buffered_record_converter;
pub mod delete_context;
pub mod input_split;
pub mod iterator_mode;
pub mod log_record_reader;
pub mod merged_log_record_reader;
pub mod output_converter;
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
use crate::file_group::reader::buffer::HoodieFileGroupRecordBuffer;
use crate::file_group::reader::buffered_record_converter::BufferedRecordConverter;
use crate::file_group::reader::input_split::InputSplit;
use crate::file_group::reader::iterator_mode::IteratorMode;
use crate::file_group::reader::output_converter::OutputConverter;
use crate::file_group::reader::read_stats::HoodieReadStats;
use crate::file_group::reader::reader_context::ReaderContext;
use crate::file_group::reader::reader_parameters::ReaderParameters;
use crate::file_group::reader::schema_handler::FileGroupReaderSchemaHandler;
use crate::storage::Storage;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use std::sync::Arc;

/// The top-level file group reader orchestrator.
///
/// Mirrors Java's `org.apache.hudi.common.table.read.HoodieFileGroupReader<T>`.
///
/// This is the main entry point for reading a file group. It:
/// 1. Accepts an `InputSplit` describing what to read (base file + log files)
/// 2. Creates the [`FileGroupReaderSchemaHandler`] from `data_schema` + `requested_schema`
/// 3. Creates base file iterators via storage
/// 4. Delegates log scanning + buffer creation to `FileGroupRecordBufferLoader`
/// 5. Merges base file records with log records via the buffer
/// 6. Projects output back to `requested_schema` via `OutputConverter`
///
/// ## Construction
///
/// Use [`HoodieFileGroupReader::builder()`] for the builder pattern, or construct
/// directly with [`HoodieFileGroupReader::new()`].
#[derive(Debug)]
pub struct HoodieFileGroupReader {
    // ── Context (mirrors Java's HoodieReaderContext<T>) ────────────────
    /// Reader context carrying merge mode, instant range, and config maps.
    reader_context: Arc<ReaderContext>,

    /// Storage for reading base files and log files.
    storage: Arc<Storage>,

    // ── Input ──────────────────────────────────────────────────────────
    /// Describes what to read: base file, log files, partition path.
    input_split: InputSplit,

    // ── Configuration ──────────────────────────────────────────────────
    /// Reader flags: use_record_position, emit_delete, sort_output, etc.
    reader_parameters: ReaderParameters,

    /// The current iterator mode.
    #[allow(dead_code)]
    iterator_mode: IteratorMode,

    // ── Schema (mirrors Java's readerContext.getSchemaHandler()) ───────
    /// Schema handler created in the constructor from `data_schema` +
    /// `requested_schema`, exactly like Java lines 119-121.
    /// Owns the `required_schema` used for base file projection and the
    /// `output_converter` used for final projection.
    schema_handler: FileGroupReaderSchemaHandler,

    // ── Strategy ───────────────────────────────────────────────────────
    /// Buffer loader: selects buffer impl + triggers log scan.
    record_buffer_loader: DefaultFileGroupRecordBufferLoader,

    // ── Mutable state (populated during read) ──────────────────────────
    /// Core structure to store and process records.
    /// Mirrors Java's `HoodieFileGroupRecordBuffer<T> recordBuffer`.
    record_buffer: Option<Box<dyn HoodieFileGroupRecordBuffer>>,

    /// Base file record iterator (set during `initRecordIterators`).
    /// Mirrors Java's `ClosableIterator<T> baseFileIterator`.
    base_file_iterator: Option<Vec<RecordBatch>>,

    /// Optional converter for projecting/transforming output records.
    /// Mirrors Java's `Option<UnaryOperator<T>> outputConverter`.
    output_converter: Option<Box<dyn OutputConverter>>,

    /// Read statistics accumulator.
    read_stats: HoodieReadStats,

    /// Valid block instants from log scanning.
    valid_block_instants: Vec<String>,

    /// Converter for engine records to [`BufferedRecord`].
    /// Mirrors Java's `BufferedRecordConverter<T> bufferedRecordConverter`.
    buffered_record_converter: Option<Box<dyn BufferedRecordConverter>>,
}

impl HoodieFileGroupReader {
    /// Create a new file group reader.
    ///
    /// Mirrors Java's `HoodieFileGroupReader(readerContext, storage, tablePath,
    /// latestCommitTime, dataSchema, requestedSchema, ...)` constructor.
    ///
    /// The constructor:
    /// 1. Creates a [`FileGroupReaderSchemaHandler`] from `data_schema` +
    ///    `requested_schema` (Java lines 119-121)
    /// 2. Calls `prepare_required_schema()` to compute the `required_schema`
    ///    (Java: automatic in `FileGroupReaderSchemaHandler` constructor, line 105)
    /// 3. Obtains the `output_converter` from the schema handler (Java line 122)
    ///
    /// # Arguments
    /// * `reader_context` — Engine context with merge mode, ordering fields, table config.
    /// * `storage` — Storage layer for reading base files and log files.
    /// * `input_split` — Describes what to read (base file path, log file paths, partition).
    /// * `reader_parameters` — Reader flags (use_record_position, emit_delete, etc.).
    /// * `data_schema` — Full table schema (what columns exist in the files).
    ///    Maps to Java's `dataSchema` / `tableSchema` parameter.
    /// * `requested_schema` — Column projection requested by the caller.
    ///    Maps to Java's `requestedSchema` parameter. `None` means all columns.
    pub fn new(
        reader_context: Arc<ReaderContext>,
        storage: Arc<Storage>,
        input_split: InputSplit,
        reader_parameters: ReaderParameters,
        data_schema: Option<SchemaRef>,
        requested_schema: Option<SchemaRef>,
    ) -> Self {
        log::debug!(
            "HoodieFileGroupReader::new partition={} base_file={} log_files={} \
             ordering_fields={:?} latest_commit_time={} record_key_field={}",
            input_split.partition_path,
            input_split.base_file_path.as_deref().unwrap_or("<none>"),
            input_split.log_file_paths.len(),
            reader_context.ordering_field_names(),
            reader_context.latest_commit_time,
            reader_context.record_key_field(),
        );
        for (i, lf) in input_split.log_file_paths.iter().enumerate() {
            log::debug!("  log_file[{i}]: {lf}");
        }

        // Mirrors Java lines 119-121:
        // readerContext.setSchemaHandler(
        //     new FileGroupReaderSchemaHandler(readerContext, dataSchema, requestedSchema, ...));
        //
        // When schemas are explicitly provided (direct construction / tests), create
        // a new handler. When they are not provided (FFI path via builder), use the
        // handler already on reader_context — which was populated by the FFI bridge
        // from the Avro JSON schemas passed through the Substrait proto.
        let mut schema_handler = if data_schema.is_some() || requested_schema.is_some() {
            let mut handler = FileGroupReaderSchemaHandler::new();
            if let Some(ds) = data_schema {
                handler = handler.with_table_schema(ds.clone()).with_data_schema(ds);
            }
            if let Some(rs) = requested_schema {
                handler = handler.with_requested_schema(rs);
            }
            handler
        } else {
            reader_context.schema_handler.clone()
        };

        // Mirrors Java FileGroupReaderSchemaHandler constructor line 105:
        // this.requiredSchema = prepareRequiredSchema(this.deleteContext);
        //
        // Uses record_key_fields() (all key fields) instead of record_key_field()
        // (single) to support composite record keys in virtual-key mode.
        // Mirrors Java's getMandatoryFieldsForMerging() lines 250-258.
        let has_instant_range = reader_context.instant_range.is_some();
        schema_handler.prepare_required_schema(
            input_split.has_log_files(),
            &reader_context.record_key_fields(),
            &reader_context.ordering_field_names().to_vec(),
            &reader_context.table_config,
            has_instant_range,
            &reader_context.merge_mode,
        );

        // Bootstrap merge reordering is not yet supported in hudi-rs.
        // Java's prepareRequiredSchema() (lines 280-288) partitions fields into
        // meta and data columns and reorders them for bootstrap tables. Until
        // that is implemented, reject bootstrap merge at construction time.
        if reader_context.needs_bootstrap_merge {
            panic!(
                "Bootstrap merge is not yet supported in hudi-rs. \
                 Table at '{}' has bootstrap base files that require \
                 meta/data column reordering.",
                reader_context.table_path,
            );
        }

        // Mirrors Java line 122:
        // this.outputConverter = readerContext.getSchemaHandler().getOutputConverter();
        let output_converter = schema_handler.get_output_converter();

        // Propagate the prepared schema_handler back onto a new reader_context
        // so downstream consumers (record buffer, log scanner) see the canonical
        // schema_handler with its stored DeleteContext. Mirrors Java's
        // `readerContext.setSchemaHandler(...)` — in Java the reader context is
        // mutable; in Rust we create a new Arc with the updated handler.
        let reader_context = {
            let mut updated = (*reader_context).clone();
            updated.schema_handler = schema_handler.clone();
            Arc::new(updated)
        };

        Self {
            reader_context,
            storage,
            input_split,
            reader_parameters,
            iterator_mode: IteratorMode::EngineRecord,
            schema_handler,
            record_buffer_loader: DefaultFileGroupRecordBufferLoader::new(),
            record_buffer: None,
            base_file_iterator: None,
            output_converter,
            read_stats: HoodieReadStats::default(),
            valid_block_instants: Vec::new(),
            buffered_record_converter: None,
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
        log::debug!(
            "[HoodieFileGroupReader] initRecordIterators: partition={} base_file={} log_files={}",
            self.input_split.partition_path,
            self.input_split.base_file_path.as_deref().unwrap_or("<none>"),
            self.input_split.log_file_paths.len(),
        );

        // Step 1: Make base file iterator (read base file data)
        // Mirrors Java: this.baseFileIterator = makeBaseFileIterator();
        let mut base_file_batches = self.make_base_file_batches().await?;
        let base_rows: usize = base_file_batches.iter().map(|b| b.num_rows()).sum();
        log::debug!(
            "[HoodieFileGroupReader] makeBaseFileIterator: {} batches, {} total rows",
            base_file_batches.len(),
            base_rows,
        );
        if let Some(first) = base_file_batches.first() {
            let schema = first.schema();
            let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            log::debug!("[HoodieFileGroupReader] base file schema: {:?}", field_names);
        }

        // Apply instant range filter on base file records.
        // Mirrors Java's makeBaseFileIterator() lines 169-171:
        //   return readerContext.getInstantRange().isPresent()
        //       ? readerContext.applyInstantRangeFilter(recordIterator)
        //       : recordIterator;
        if self.reader_context.instant_range.is_some() {
            base_file_batches = self.apply_instant_range_filter(base_file_batches)?;
            let filtered_rows: usize = base_file_batches.iter().map(|b| b.num_rows()).sum();
            log::debug!(
                "[HoodieFileGroupReader] applyInstantRangeFilter: {base_rows} → {filtered_rows} rows"
            );
        }

        self.base_file_iterator = Some(base_file_batches);

        // Step 2: If no records to merge (no log files), return base file data directly
        if self.input_split.has_no_records_to_merge() {
            let batches = self.base_file_iterator.as_ref().unwrap();
            log::debug!("[HoodieFileGroupReader] no log files → returning base file data directly ({base_rows} rows)");
            if batches.is_empty() {
                // No base file and no log files — return empty batch.
                return self.apply_output_converter(RecordBatch::new_empty(
                    std::sync::Arc::new(arrow_schema::Schema::empty()),
                ));
            }
            let result = arrow::compute::concat_batches(
                &batches[0].schema(),
                batches,
            )
            .map_err(|e| {
                CoreError::ReadFileSliceError(format!(
                    "Failed to concatenate base file batches: {e}"
                ))
            })?;
            return self.apply_output_converter(result);
        }

        // Step 3: Load record buffer (scan log files + create buffer)
        // Mirrors Java: this.recordBuffer = recordBufferLoader.getRecordBuffer(...).getLeft();
        log::debug!(
            "[HoodieFileGroupReader] scanning {} log file(s) with latest_commit_time={}",
            self.input_split.log_file_paths.len(),
            self.reader_context.latest_commit_time,
        );
        let load_result = self
            .record_buffer_loader
            .get_record_buffer(
                self.reader_context.clone(),
                self.storage.clone(),
                &self.input_split,
                &self.reader_parameters,
                &mut self.read_stats,
            )
            .await?;

        self.record_buffer = Some(load_result.record_buffer);
        self.valid_block_instants = load_result.valid_block_instants;

        let record_buffer = self.record_buffer.as_mut().unwrap();

        log::debug!(
            "[HoodieFileGroupReader] log scan complete: buffer_size={} valid_instants={:?} \
             stats: log_blocks={} log_records={} corrupt={} rollbacks={}",
            record_buffer.size(),
            self.valid_block_instants,
            self.read_stats.total_log_blocks,
            self.read_stats.total_log_records,
            self.read_stats.total_corrupt_log_blocks,
            self.read_stats.total_rollback_blocks,
        );

        // Step 4a: reader_schema is now set at buffer construction time
        // (in KeyBasedFileGroupRecordBuffer::new), matching Java's
        // FileGroupRecordBuffer constructor line 102.

        // Step 4b: Set base file iterator on the buffer
        // Mirrors Java: recordBuffer.setBaseFileIterator(baseFileIterator);
        let base_batches = self.base_file_iterator.take().unwrap_or_default();
        record_buffer.set_base_file_iterator(base_batches);
        log::debug!(
            "[HoodieFileGroupReader] set base file iterator ({base_rows} rows), starting merge_and_collect"
        );

        // Step 5: Merge and collect — take ownership of the buffer since
        // merge_and_collect consumes it (mirrors Java's close() lifecycle).
        let record_buffer = self.record_buffer.take().unwrap();
        let result = record_buffer.merge_and_collect()?;
        log::debug!(
            "[HoodieFileGroupReader] merge_and_collect complete: {} rows, {} columns",
            result.num_rows(),
            result.num_columns(),
        );
        self.apply_output_converter(result)
    }

    /// Read base file data and initialize the buffered record converter.
    ///
    /// Mirrors Java's `HoodieFileGroupReader.makeBaseFileIterator()` which:
    /// 1. Returns empty iterator if no base file
    /// 2. Creates `bufferedRecordConverter` via `BufferedRecordConverter.createConverter(...)`
    /// 3. Reads the base file via `readerContext.getFileRecordIterator(...)`
    async fn make_base_file_batches(&mut self) -> Result<Vec<RecordBatch>> {
        match &self.input_split.base_file_path {
            Some(path) => {
                // Mirrors Java: this.bufferedRecordConverter = BufferedRecordConverter.createConverter(
                //     readerContext.getIteratorMode(), readerContext.getSchemaHandler().getRequiredSchema(),
                //     readerContext.getRecordContext(), orderingFieldNames);
                // In Rust, the converter is set externally or already present; log if missing.
                if self.buffered_record_converter.is_none() {
                    log::debug!(
                        "[HoodieFileGroupReader] makeBaseFileIterator: no bufferedRecordConverter set \
                         (batch-level read does not require per-record conversion)"
                    );
                }

                // Mirrors Java line 159-162:
                // readerContext.getFileRecordIterator(pathInfo, start, len,
                //     schemaHandler.getTableSchema(), schemaHandler.getRequiredSchema(), storage)
                let batch = if let Some(required_schema) =
                    &self.schema_handler.required_schema
                {
                    self.storage
                        .get_parquet_file_data_projected(path, required_schema)
                        .await
                        .map_err(|e| {
                            CoreError::ReadFileSliceError(format!(
                                "Failed to read base file '{path}' with projection: {e:?}"
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

    /// Apply the output converter to the final result, if set.
    ///
    /// Mirrors Java's `next()`:
    /// ```java
    /// if (outputConverter.isPresent()) {
    ///     return nextVal.project(outputConverter.get());
    /// }
    /// ```
    fn apply_output_converter(&self, batch: RecordBatch) -> Result<RecordBatch> {
        match &self.output_converter {
            Some(converter) => converter.apply(batch),
            None => Ok(batch),
        }
    }

    /// Filter base file records by instant range on `_hoodie_commit_time`.
    ///
    /// Mirrors Java's `HoodieReaderContext.applyInstantRangeFilter()` (lines 354-365):
    /// ```java
    /// if (HoodieTableMetadata.isMetadataTable(tablePath)) {
    ///     return fileRecordIterator; // skip for metadata table
    /// }
    /// InstantRange instantRange = getInstantRange().get();
    /// int commitTimePos = schemaHandler.getRequiredSchema().getField(COMMIT_TIME_METADATA_FIELD).pos();
    /// Predicate<T> instantFilter = row -> instantRange.isInRange(getMetaFieldValue(row, commitTimePos));
    /// return new CloseableFilterIterator<>(fileRecordIterator, instantFilter);
    /// ```
    ///
    /// In Rust/Arrow, we use columnar filtering: extract the `_hoodie_commit_time`
    /// column, build a boolean mask, and use `arrow::compute::filter_record_batch`.
    fn apply_instant_range_filter(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        let instant_range = match &self.reader_context.instant_range {
            Some(range) => range,
            None => return Ok(batches),
        };

        // Skip filtering for metadata table (mirrors Java line 356).
        if crate::util::path::is_metadata_table_path(&self.reader_context.table_path) {
            return Ok(batches);
        }

        let timezone = self.reader_context.timezone();
        let mut filtered = Vec::with_capacity(batches.len());

        for batch in batches {
            if batch.num_rows() == 0 {
                filtered.push(batch);
                continue;
            }

            // Find _hoodie_commit_time column position.
            let commit_time_idx = match batch.schema().index_of("_hoodie_commit_time") {
                Ok(idx) => idx,
                Err(_) => {
                    // Column not present — cannot filter. Pass through.
                    // This can happen if the schema didn't include commit_time
                    // (e.g., schema was not properly prepared).
                    log::warn!(
                        "[HoodieFileGroupReader] applyInstantRangeFilter: \
                         _hoodie_commit_time column not found in base file schema, \
                         skipping filter"
                    );
                    filtered.push(batch);
                    continue;
                }
            };

            let commit_time_col = batch
                .column(commit_time_idx)
                .as_any()
                .downcast_ref::<StringArray>();

            let commit_time_col = match commit_time_col {
                Some(col) => col,
                None => {
                    log::warn!(
                        "[HoodieFileGroupReader] applyInstantRangeFilter: \
                         _hoodie_commit_time is not a StringArray, skipping filter"
                    );
                    filtered.push(batch);
                    continue;
                }
            };

            // Build boolean mask: true for rows in range.
            let mut mask_builder =
                arrow_array::builder::BooleanBuilder::with_capacity(batch.num_rows());
            for i in 0..commit_time_col.len() {
                if commit_time_col.is_null(i) {
                    mask_builder.append_value(false);
                } else {
                    let ts = commit_time_col.value(i);
                    let in_range = instant_range
                        .is_in_range(ts, &timezone)
                        .map_err(|e| {
                            CoreError::ReadFileSliceError(format!(
                                "Failed to check instant range for '{ts}': {e}"
                            ))
                        })?;
                    mask_builder.append_value(in_range);
                }
            }
            let mask = mask_builder.finish();

            let filtered_batch =
                arrow::compute::filter_record_batch(&batch, &mask).map_err(|e| {
                    CoreError::ReadFileSliceError(format!(
                        "Failed to filter base file batch by instant range: {e}"
                    ))
                })?;
            filtered.push(filtered_batch);
        }

        Ok(filtered)
    }

    // =========================================================================
    // Setters (mirrors Java's mutable field assignments)
    // =========================================================================

    /// Set the output converter.
    /// Mirrors Java: `this.outputConverter = readerContext.getSchemaHandler().getOutputConverter()`.
    pub fn set_output_converter(&mut self, converter: Box<dyn OutputConverter>) {
        self.output_converter = Some(converter);
    }

    /// Set the buffered record converter.
    /// Mirrors Java: `this.bufferedRecordConverter = BufferedRecordConverter.createConverter(...)`.
    pub fn set_buffered_record_converter(&mut self, converter: Box<dyn BufferedRecordConverter>) {
        self.buffered_record_converter = Some(converter);
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
    reader_context: Option<Arc<ReaderContext>>,
    storage: Option<Arc<Storage>>,
    input_split: Option<InputSplit>,
    reader_parameters: ReaderParameters,
    data_schema: Option<SchemaRef>,
    requested_schema: Option<SchemaRef>,
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

    pub fn with_input_split(mut self, input_split: InputSplit) -> Self {
        self.input_split = Some(input_split);
        self
    }

    pub fn with_reader_parameters(mut self, params: ReaderParameters) -> Self {
        self.reader_parameters = params;
        self
    }

    /// Set the data schema (full table schema).
    /// Mirrors Java's `Builder.withDataSchema(Schema dataSchema)`.
    pub fn with_data_schema(mut self, schema: SchemaRef) -> Self {
        self.data_schema = Some(schema);
        self
    }

    /// Set the requested schema (column projection).
    /// Mirrors Java's `Builder.withRequestedSchema(Schema requestedSchema)`.
    pub fn with_requested_schema(mut self, schema: SchemaRef) -> Self {
        self.requested_schema = Some(schema);
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

        let reader = HoodieFileGroupReader::new(
            reader_context,
            storage,
            input_split,
            self.reader_parameters,
            self.data_schema,
            self.requested_schema,
        );

        Ok(reader)
    }
}
