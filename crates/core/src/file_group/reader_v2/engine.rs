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

//! The merge-on-read file group reader.
//!
//! Mirrors Java's `org.apache.hudi.common.table.read.HoodieFileGroupReader`.
//! Reached from `file_group::reader::FileGroupReader` through [`super::adapter`].

use crate::Result;
use crate::config::table::BaseFileFormatValue;
use crate::error::CoreError;
use crate::file_group::base_file::hfile::HFileBaseFileReader;
use crate::file_group::base_file::reader::{
    BaseFileReadOptions, BaseFileReader, create_base_file_reader,
};
use crate::file_group::reader_v2::buffer::BufferType;
use crate::file_group::reader_v2::buffer::loader::{
    DefaultFileGroupRecordBufferLoader, FileGroupRecordBufferLoader,
};
use crate::file_group::reader_v2::buffer::record_positions::ROW_INDEX_TEMPORARY_COLUMN_NAME;
use crate::file_group::reader_v2::buffered_record_converter::BufferedRecordConverter;
use crate::file_group::reader_v2::input_split::InputSplit;
use crate::file_group::reader_v2::iterator_mode::IteratorMode;
use crate::file_group::reader_v2::merge_iterator::{
    FileGroupMergeStream, StreamStatsHandle, new_stream_stats_handle,
};
use crate::file_group::reader_v2::output_converter::OutputConverter;
use crate::file_group::reader_v2::profiling::profile_once;
use crate::file_group::reader_v2::read_stats::HoodieReadStats;
use crate::file_group::reader_v2::reader_context::ReaderContext;
use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
use crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler;
use crate::storage::{RowFilterBuilder, Storage};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use std::str::FromStr;
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
    // NOTE: the record buffer and base-file batches are not stored on the
    // reader — they are local to `init_record_iterators` and owned by the
    // returned `FileGroupMergeStream` for the rest of the read.
    /// Optional converter for projecting/transforming output records.
    /// Mirrors Java's `Option<UnaryOperator<T>> outputConverter`.
    output_converter: Option<Box<dyn OutputConverter>>,

    /// Read statistics accumulator.
    read_stats: HoodieReadStats,

    /// Stage-timing sink shared with the [`FileGroupMergeStream`] returned by
    /// [`Self::open`]. The streaming iterator
    /// owns the buffer once `open()` returns, so the merge-phase timings
    /// (final_merge_ms, output_build_ms) and the update-processor
    /// insert/update/delete counts are accumulated through this handle during
    /// iteration and drained back into [`Self::read_stats`] by [`Self::read`]
    /// after the stream is exhausted. Wrapped in `Arc<Mutex<…>>` because the FFI
    /// path requires the iterator to be `Send` (it is boxed into an
    /// `FFI_ArrowArrayStream`); the lock is taken once per emitted chunk, so the
    /// cost is negligible against the per-chunk merge work. The FFI path never
    /// reads these stats back — only `read()`-based callers do.
    stream_stats: StreamStatsHandle,

    /// Valid block instants from log scanning.
    valid_block_instants: Vec<String>,

    /// Converter for engine records to [`BufferedRecord`].
    /// Mirrors Java's `BufferedRecordConverter<T> bufferedRecordConverter`.
    buffered_record_converter: Option<Box<dyn BufferedRecordConverter>>,
    // NOTE: the optional parquet `RowFilter` builder lives on
    // `reader_context`, not this struct, so the same builder is
    // visible to (a) the base parquet read here, and (b) the parquet log
    // block decoder in `file_group::log_file::content::Decoder`. The gate
    // (CoW || mor_pk_safe) lives at the use sites; this file's gate is at
    // `base_file_source` below.
}

/// Rows per base batch handed to the merge, and therefore per merged chunk.
///
/// Load-bearing rather than cosmetic: merging a chunk is synchronous work on the
/// task that polls the stream, and its cost is linear in the chunk's rows. On
/// this machine, one merge of a 1024-row chunk against a 50k-key log map takes
/// 0.4-1.1 ms, and 5.7-6.1 ms once the merge map has spilled to disk; at 8192
/// rows those become 2.8 ms and ~40 ms. So the chunk size is what bounds how long
/// a single poll occupies its executor, and it is set here rather than inherited.
///
/// 1024 is what `parquet` already defaults to, so this pins today's behaviour
/// instead of changing it. Pinned because the bound is silent if it moves: a
/// larger default upstream would multiply the blocking above with nothing
/// failing. Measured by `spilled_merge_blocking_duration` (ignored; run with
/// `--release --ignored --nocapture`).
const MERGE_CHUNK_ROWS: usize = 1024;

/// Base-file read options carrying an optional pushdown predicate, and the
/// row-position column when the merge is by position.
///
/// The three base reads below differ only in projection, so both are attached in
/// one place — a read that silently lost the filter would return extra rows
/// rather than fail, which is the hard kind of bug to notice, and one that lost
/// the row-position column would fail in the buffer with the column named but
/// not the read that dropped it.
fn base_read_options(
    row_filter: Option<RowFilterBuilder>,
    key_predicate: Option<crate::file_group::base_file::reader::KeyPredicate>,
    use_record_position: bool,
) -> BaseFileReadOptions {
    let mut options = BaseFileReadOptions::new();
    options = options.with_batch_size(MERGE_CHUNK_ROWS);
    if let Some(row_filter) = row_filter {
        options = options.with_row_filter(row_filter);
    }
    if let Some(key_predicate) = key_predicate {
        options = options.with_key_predicate(key_predicate);
    }
    if use_record_position {
        options = options.with_row_index_column(ROW_INDEX_TEMPORARY_COLUMN_NAME);
    }
    options
}

/// A base file as the merge consumes it: batches, plus the schema they carry.
///
/// The schema travels with the stream because a `Stream` has no `schema()` the
/// way a `RecordBatchReader` does, and the merge needs it before the first
/// batch arrives — to derive the merge schema, and to describe a base file that
/// yields no batches at all.
struct BaseSource {
    schema: SchemaRef,
    batches: crate::file_group::reader_v2::merge_iterator::BaseBatchStream,
}

impl BaseSource {
    /// A base file that contributes nothing: no base file at all, or one the
    /// instant range excludes.
    fn empty(schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: futures::stream::empty().boxed(),
        }
    }
}

/// `schema` without the internal row-position column.
///
/// The column belongs to the base read and the position buffer; it is not the
/// table's, so it must not reach a caller. Every schema derived from a base
/// source's own schema goes through here.
fn without_row_index(schema: SchemaRef) -> SchemaRef {
    if schema
        .column_with_name(ROW_INDEX_TEMPORARY_COLUMN_NAME)
        .is_none()
    {
        return schema;
    }
    Arc::new(arrow_schema::Schema::new(
        schema
            .fields()
            .iter()
            .filter(|f| f.name() != ROW_INDEX_TEMPORARY_COLUMN_NAME)
            .cloned()
            .collect::<Vec<_>>(),
    ))
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
    ///   Maps to Java's `dataSchema` / `tableSchema` parameter.
    /// * `requested_schema` — Column projection requested by the caller.
    ///   Maps to Java's `requestedSchema` parameter. `None` means all columns.
    pub fn new(
        reader_context: Arc<ReaderContext>,
        storage: Arc<Storage>,
        input_split: InputSplit,
        reader_parameters: ReaderParameters,
        data_schema: Option<SchemaRef>,
        requested_schema: Option<SchemaRef>,
    ) -> Result<Self> {
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
            reader_context.ordering_field_names(),
            &reader_context.table_config,
            has_instant_range,
            &reader_context.merge_mode,
        )?;

        // Schema-on-read (InternalSchema) evolution is not supported in hudi-rs.
        // Java loads an InternalSchema from the `.schema` folder and
        // applies column renames / type changes through InternalSchema versioning
        // when `hoodie.schema.on.read.enable=true`. hudi-rs only implements
        // schema-on-write backward-compatible evolution, so silently honoring the
        // flag would risk misreading evolved data. Reject it loudly at the same
        // table-config chokepoint as the bootstrap gate below.
        if reader_context
            .table_config
            .get("hoodie.schema.on.read.enable")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            return Err(CoreError::Unsupported(format!(
                "schema-on-read (InternalSchema) is not supported in hudi-rs. \
                 Table at '{}' has hoodie.schema.on.read.enable=true, which requires \
                 InternalSchema-based evolution (column renames / type changes) that \
                 hudi-rs does not implement; only schema-on-write backward-compatible \
                 evolution is supported.",
                reader_context.table_path,
            )));
        }

        // Bootstrap merge reordering is not yet supported in hudi-rs.
        // Java's prepareRequiredSchema() (lines 280-288) partitions fields into
        // meta and data columns and reorders them for bootstrap tables. Until
        // that is implemented, reject bootstrap merge at construction time.
        if reader_context.needs_bootstrap_merge {
            // Reachable via table state (bootstrap base files), so this is a
            // loud error rather than a panic.
            return Err(CoreError::Unsupported(format!(
                "Bootstrap merge is not yet supported in hudi-rs. \
                 Table at '{}' has bootstrap base files that require \
                 meta/data column reordering.",
                reader_context.table_path,
            )));
        }

        // Composite virtual keys ARE supported. With `hoodie.populate.meta.fields=false`
        // and a multi-field recordkey, `RecordContext::record_key_array` reconstructs the
        // full `field:val,field:val` merge key per row (mirroring Java
        // `KeyGenerator.constructRecordKey`) on BOTH the base and log sides, so records
        // sharing the first field but differing on a later one do not collide. See
        // `RecordContext::build_composite_record_key_array`.

        // Multi-field (composite) precombine/ordering keys ARE supported.
        // `RecordContext::new` splits a comma-separated `hoodie.table.precombine.field`
        // / `hoodie.table.ordering.fields` into `ordering_field_names`, and
        // `get_ordering_values` builds one `OrderingValue::Composite` per row from
        // the per-field scalars (compared lexicographically field-by-field, mirroring
        // Java `OrderingValues`). A field absent from a batch, an unsupported field
        // type, or a null component falls back to natural order — matching the scalar
        // path — so there is no silent first-field-only degradation.

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

        Ok(Self {
            reader_context,
            storage,
            input_split,
            reader_parameters,
            iterator_mode: IteratorMode::EngineRecord,
            schema_handler,
            record_buffer_loader: DefaultFileGroupRecordBufferLoader::new(),
            output_converter,
            read_stats: HoodieReadStats::default(),
            stream_stats: new_stream_stats_handle(),
            valid_block_instants: Vec::new(),
            buffered_record_converter: None,
        })
    }

    /// Java-parity surface reached only by the test harness, which drives the engine
    /// the way an FFI caller would; `FileGroupReader` goes through `adapter`.
    #[allow(dead_code)]
    /// Create a builder for configuring the reader.
    pub fn builder() -> HoodieFileGroupReaderBuilder {
        HoodieFileGroupReaderBuilder::default()
    }

    // =========================================================================
    // Main read API (mirrors Java's getClosableIterator / getBufferedRecordIterator)
    // =========================================================================

    /// The reader for this slice's base file format.
    ///
    /// Built per call rather than held: the format comes from the reader
    /// context, and constructing one is cheap next to reading a file.
    fn base_file_reader(&self) -> Result<std::sync::Arc<dyn BaseFileReader>> {
        // An unset format means the caller did not say; parquet is the default
        // base file format, and is what every non-metadata table uses here.
        let format = if self.reader_context.base_file_format.is_empty() {
            BaseFileFormatValue::Parquet
        } else {
            BaseFileFormatValue::from_str(&self.reader_context.base_file_format)?
        };
        // The shared factory refuses HFile, which is what keeps the legacy
        // reader from serving it; this reader has its own.
        if matches!(format, BaseFileFormatValue::HFile) {
            return Ok(std::sync::Arc::new(HFileBaseFileReader::new(
                self.storage.clone(),
            )));
        }
        Ok(create_base_file_reader(&self.storage, &format)?)
    }

    /// Stream the merged output.
    ///
    /// [`Self::read`] returns the whole file group as one batch, so peak memory
    /// tracks the base file. This reads the base file one bounded batch at a
    /// time instead (`MERGE_CHUNK_ROWS` rows), merging and emitting a chunk
    /// per batch.
    ///
    /// Demand-driven: nothing is merged until the consumer asks for it, so the
    /// memory this adds over the merge map is one chunk. The previous shape ran
    /// the merge on a blocking thread behind a depth-1 channel to get the same
    /// bound; a `Stream` has it by construction.
    ///
    /// Single-use: takes the output converter and, for MOR, moves the record
    /// buffer into the returned stream. Reading again needs a new reader.
    pub(crate) async fn open_stream(
        &mut self,
    ) -> Result<futures::stream::BoxStream<'static, Result<RecordBatch>>> {
        // Stage timing (perf harness): opening the base file. Only the open —
        // the per-row-group decode is paid lazily, inside the merge.
        let base = profile_once!(self.read_stats.base_read_ms, self.base_file_source().await)?;
        Ok(self.init_record_iterators(base).await?.into_stream())
    }

    /// Read the file group and return the merged output as a single
    /// `RecordBatch`.
    ///
    /// Same merge as [`Self::open_stream`], collected into one batch. Both
    /// entry points merge the base a batch at a time and therefore return
    /// the same row sequence; this one just concatenates the chunks.
    /// Single-use, like [`Self::open_stream`].
    pub async fn read(&mut self) -> Result<RecordBatch> {
        // Stage timing (perf harness): only the open, same as `open_stream` —
        // the decode happens lazily while `collect_into_one_batch` drives the
        // stream, so it lands in the merge loop rather than in `base_read_ms`.
        let base = profile_once!(self.read_stats.base_read_ms, self.base_file_source().await)?;
        let batch = self
            .init_record_iterators(base)
            .await?
            .collect_into_one_batch()
            .await?;
        // The merge accumulated the merge-phase timings + insert/update/delete
        // counts into the shared `stream_stats` while `collect_into_one_batch`
        // drove it to exhaustion. Drain them back into `self.read_stats` so
        // `read_stats()`-based callers (fg-bench, tests, reader_v1) observe them.
        self.drain_stream_stats();
        Ok(batch)
    }

    /// Copy the accumulated streaming stage-stats into [`Self::read_stats`].
    /// Called by [`Self::read`] after the iterator is exhausted.
    fn drain_stream_stats(&mut self) {
        let s = self
            .stream_stats
            .lock()
            .expect("stream_stats mutex poisoned");
        self.read_stats.final_merge_ms = s.final_merge_ms;
        self.read_stats.output_build_ms = s.output_build_ms;
        self.read_stats.merge_map_peak_entries = s.merge_map_peak_entries;
        self.read_stats.num_inserts = s.num_inserts;
        self.read_stats.num_updates = s.num_updates;
        self.read_stats.num_deletes = s.num_deletes;
    }

    /// Initialize record iterators: read base file + scan/merge log files,
    /// hand state to a [`FileGroupMergeStream`].
    ///
    /// Mirrors Java's `HoodieFileGroupReader.initRecordIterators()`. The
    /// fast path (no log files = CoW / empty) returns an `Eager` iterator
    /// over the base file source; the MOR path returns a `Buffered`
    /// iterator that drives `buffer.has_next() / buffer.next()` in chunks.
    ///
    /// ```text
    /// initRecordIterators()
    ///   └─ recordBufferLoader.getRecordBuffer(...)
    ///        → FileGroupMergeStream::new_buffered(...)
    /// ```
    async fn init_record_iterators(&mut self, base: BaseSource) -> Result<FileGroupMergeStream> {
        log::debug!(
            "[HoodieFileGroupReader] initRecordIterators: partition={} base_file={} log_files={}",
            self.input_split.partition_path,
            self.input_split
                .base_file_path
                .as_deref()
                .unwrap_or("<none>"),
            self.input_split.log_file_paths.len(),
        );

        let BaseSource {
            schema: base_source_schema,
            batches: base_source,
        } = base;
        log::debug!(
            "[HoodieFileGroupReader] base file source: schema_cols={}",
            base_source_schema.fields().len(),
        );

        // The post-projection output schema is the same regardless of
        // CoW vs MOR — it is the schema every emitted chunk carries.
        let output_converter = self.output_converter.take();
        let post_projection_schema = output_converter.as_ref().map(|c| c.target_schema());

        // Step 2: If no records to merge (no log files), build an Eager
        // iterator that yields the base file batches directly.
        if self.input_split.is_base_only() {
            log::debug!("[HoodieFileGroupReader] no log files → Eager iterator");

            // The schema travels with the source, so it is known without
            // forcing a row-group decode. A log-only file group's source is
            // empty and carries the required schema.
            let merge_schema: SchemaRef = if let Some(rs) = &self.schema_handler.required_schema {
                rs.clone()
            } else {
                base_source_schema.clone()
            };

            let output_schema = post_projection_schema.unwrap_or(merge_schema);
            // Stage timing (perf harness): the Eager iterator accumulates
            // per-chunk output_build_ms (concat is gone — each base batch flows
            // through the converter as its own chunk) into `stream_stats`, which
            // `read()` drains back into `self.read_stats`.
            return Ok(FileGroupMergeStream::new_eager(
                base_source,
                output_schema,
                output_converter,
                self.stream_stats.clone(),
            ));
        }

        // Step 3: MOR path — load record buffer (scan log files + create buffer).
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

        let record_buffer = load_result.record_buffer;
        self.valid_block_instants = load_result.valid_block_instants;

        // Anything this read expects that is quietly not done, said once, before
        // the rows come back looking unremarkable. Reported here rather than on
        // entry for two reasons: the scan has finished, so a position merge that
        // gave up partway through is visible (the buffer flips its own type when
        // it falls back, and nothing else records it); and every entry point goes
        // through here, so the streaming read is covered too — reporting from
        // `read()` alone left the streaming entry point silent.
        crate::file_group::reader_v2::gaps::report_for_read(
            &self.reader_context,
            &self.reader_parameters,
            self.use_record_position(),
            record_buffer.get_buffer_type() == BufferType::PositionBasedMerge,
        );

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

        // Step 4: Determine merge_schema. The base source's schema travels
        // with it, so this needs no row-group decode.
        let merge_schema: SchemaRef = if let Some(rs) = &self.schema_handler.required_schema {
            rs.clone()
        } else if self.input_split.base_file_path.is_some() {
            // The base source's schema is the parquet schema after projection,
            // plus the row-position column when merging by position — which is
            // the reader's own and never an output column.
            without_row_index(base_source_schema.clone())
        } else {
            // Log-only file group: peek at any non-delete log record's batch
            // (HashMap order is non-deterministic, so we must search all
            // entries — the first record could be a delete).
            // Find the first non-delete record's schema (`get_record()` returns
            // `None` for a delete tombstone).
            let mut schema = None;
            for r in record_buffer.get_log_records().values() {
                if let Some(batch) = r.get_record() {
                    schema = Some(batch.schema());
                    break;
                }
            }
            schema.ok_or_else(|| {
                CoreError::ReadFileSliceError("No schema available for merge output".to_string())
            })?
        };

        let output_schema = post_projection_schema.unwrap_or_else(|| merge_schema.clone());

        // Step 5: return the streaming iterator, which owns both the buffer and
        // the base source from here on; the reader's role ends. The source is
        // the iterator's rather than the buffer's because only its holder can
        // say when the base is exhausted, and because the base file is the one
        // part of the merge that has to be read rather than computed.
        log::debug!("[HoodieFileGroupReader] returning Buffered iterator");

        // Step 6: Hand the buffer to a Buffered streaming iterator. The
        // iterator owns the buffer and drives `has_next/next` per chunk; it
        // accumulates final_merge_ms + output_build_ms and the update-processor
        // insert/update/delete counts into the shared `stream_stats`, which
        // `read()` drains back into `self.read_stats` after the stream is
        // exhausted (mirrors Java, where StandardUpdateProcessor increments
        // HoodieReadStats during iteration). merge_map_peak_entries was already
        // recorded during the log scan; the iterator reads it off the buffer up
        // front (the buffer is moved into the iterator here).
        self.stream_stats
            .lock()
            .expect("stream_stats mutex poisoned")
            .merge_map_peak_entries = record_buffer.merge_map_peak_entries();

        Ok(FileGroupMergeStream::new_buffered(
            record_buffer,
            base_source,
            merge_schema,
            output_schema,
            output_converter,
            self.stream_stats.clone(),
        ))
    }

    /// Whether this read should merge base + log records by base-file row
    /// position (rather than by record key). Mirrors Java
    /// `HoodieFileGroupReader`'s `setShouldMergeUseRecordPosition`:
    /// `useRecordPosition && !skipMerge && hasLogFiles && parquetBaseFile`.
    ///
    /// When true, the base file is read with a synthetic row-index column (see
    /// [`ROW_INDEX_TEMPORARY_COLUMN_NAME`]) so the position buffer can match
    /// base rows to log records by position.
    fn use_record_position(&self) -> bool {
        if !self.reader_parameters.use_record_position {
            return false;
        }
        if !self.input_split.has_log_files() || self.input_split.base_file_path.is_none() {
            return false;
        }
        // Position merge needs the base file's commit time to validate log-block
        // position headers. Without it the loader falls back to key-based, so the
        // base read must not attach the row-index column either (keep the two
        // decisions in lock-step).
        if self.input_split.base_file_commit_time.is_none() {
            return false;
        }
        let is_skip_merge = self
            .reader_context
            .hoodie_reader_config
            .get(crate::file_group::reader_v2::reader_context::CONFIG_MERGE_TYPE)
            .map(|v| v.eq_ignore_ascii_case("skip_merge"))
            .unwrap_or(false);
        if is_skip_merge {
            return false;
        }
        // hudi-rs only reads parquet base files; guard defensively when the
        // format is explicitly set to something else. Shared with the loader's
        // buffer-selection gate so the row-index attachment and the buffer
        // choice cannot diverge.
        crate::file_group::reader_v2::buffer::loader::base_file_is_parquet(
            &self.reader_context.base_file_format,
        )
    }

    /// Open the base file as a stream of batches, with the schema they carry.
    ///
    /// One shape for every caller: the base file is read asynchronously, one
    /// bounded batch at a time, and the whole file is never resident. A caller
    /// that needs it as a single batch collapses it afterwards (`read()` does,
    /// via `collect_into_one_batch`) — that is a choice about chunking, not
    /// about what is safe to call from where.
    ///
    /// Returns an empty stream when the input split has no base file (log-only
    /// file group), and when the instant range excludes this base file: the
    /// range is a per-file decision, so it is settled here rather than by
    /// reading the file and discarding its rows.
    ///
    /// Mirrors Java's `HoodieFileGroupReader.makeBaseFileIterator()`.
    async fn base_file_source(&mut self) -> Result<BaseSource> {
        let Some(path) = self.input_split.base_file_path.clone() else {
            // Log-only file group — empty base. Use the required_schema
            // as the reported schema when available; otherwise an empty
            // schema (the buffer's reader_schema fallback handles schema
            // selection downstream).
            let schema = self
                .schema_handler
                .required_schema
                .clone()
                .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));
            return Ok(BaseSource::empty(schema));
        };

        if self.buffered_record_converter.is_none() {
            log::debug!(
                "[HoodieFileGroupReader] base_file_source: no bufferedRecordConverter set \
                 (batch-level read does not require per-record conversion)"
            );
        }

        // gate parquet RowFilter pushdown.
        //   CoW: always safe (no merge).
        //   MOR: safe ONLY when every column referenced by the filter is a
        //        primary key (PKs are immutable across upserts, so the predicate
        //        outcome doesn't change post-merge — `reader_context.mor_pk_safe`,
        //        mirroring Java's `filterIsSafeForPrimaryKey`).
        //   Otherwise: drop the filter; the post-merge filter (Velox/Spark above
        //        the FG reader) evaluates the predicate after base+log merge.
        let row_filter = if self.reader_context.can_push_row_filter() {
            self.reader_context.row_filter_builder.clone()
        } else {
            if self.reader_context.row_filter_builder.is_some() {
                log::debug!(
                    "MOR + non-PK predicate — skipping parquet \
                     RowFilter pushdown for base file '{path}' \
                     (post-merge filter still runs)"
                );
            }
            None
        };

        // The key predicate needs no such gate. It narrows *which blocks are read*
        // and the reader filters the records it brings back, so it cannot change the
        // merge's outcome the way a non-primary-key row filter can — and a format
        // that cannot seek ignores it and returns every row.
        let key_predicate = self.reader_context.key_predicate.clone();

        // Position-based merge: ask the base read for a synthetic row-index
        // column carrying each row's TRUE physical base-file position (a parquet
        // virtual RowNumber column — correct even under RowFilter pushdown). It
        // is kept on the base source so the position buffer can match base rows
        // to log records, then dropped by the buffer when it reconciles each
        // batch to the merge schema. The column is NOT added to
        // `required_schema`/`merge_schema` — only to the base source's physical
        // schema (`base_read_schema` = required + row-index).
        let use_position = self.use_record_position();

        // No projection schema → fall back to the unprojected helper (rare; FFI
        // always supplies a required_schema). It reads the file as one batch,
        // because its schema is only known once the file has been read, so the
        // instant-range decision below cannot be made before reading it.
        let Some(required_schema) = self.schema_handler.required_schema.clone() else {
            let batch = self
                .base_file_reader()?
                .read_data(
                    &path,
                    base_read_options(row_filter.clone(), key_predicate.clone(), use_position),
                )
                .await
                .map_err(|e| {
                    CoreError::ReadFileSliceError(format!(
                        "Failed to read base file '{path}': {e:?}"
                    ))
                })?;
            let schema = batch.schema();
            if !self.base_file_in_range()? {
                return Ok(BaseSource::empty(schema));
            }
            return Ok(BaseSource {
                schema: schema.clone(),
                batches: futures::stream::once(async move { Ok(batch) }).boxed(),
            });
        };

        // Schema-evolution intersection (Java parity:
        // HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo):
        //   1. diff footer schema vs required by name;
        //   2. ask parquet only for the INTERSECTION (in the file's own types);
        //   3. project to required per batch: null-fill added columns, cast
        //      promotions (float→double string-mediated so it is value-exact).
        // Step 3 is applied PER ROW-GROUP, so every base batch the merge
        // interleaves is already in `required_schema`.
        let file_schema = self
            .base_file_reader()?
            .read_stream(&path, BaseFileReadOptions::new())
            .await
            .map(|s| s.schema().clone())
            .map_err(|e| {
                CoreError::ReadFileSliceError(format!(
                    "Failed to read base file footer schema '{path}': {e:?}"
                ))
            })?;
        // Intersection by *case-insensitive* name (Java/Spark resolve field names
        // case-insensitively). Project under the FILE's actual name+type so the
        // parquet reader finds the column; `project_batch_to_schema` (also
        // case-insensitive) then evolves each batch to `required_schema`. A
        // required column absent from the footer is skipped here and null-filled
        // downstream; an ambiguous footer case-collision errors loudly.
        let mut present: Vec<arrow_schema::FieldRef> =
            Vec::with_capacity(required_schema.fields().len());
        for rf in required_schema.fields() {
            if let Some(idx) = crate::schema::batch_evolution::index_of_ci(&file_schema, rf.name())?
            {
                present.push(file_schema.fields()[idx].clone());
            }
        }
        let present_len = present.len();
        let intersection: arrow_schema::SchemaRef = Arc::new(arrow_schema::Schema::new(present));
        log::debug!(
            "[base-file-evolution] path={} file_cols={} required_cols={} intersect_cols={}",
            path,
            file_schema.fields().len(),
            required_schema.fields().len(),
            present_len
        );

        let base_read_schema: SchemaRef = if use_position {
            let mut fields: Vec<arrow_schema::FieldRef> =
                required_schema.fields().iter().cloned().collect();
            fields.push(Arc::new(arrow_schema::Field::new(
                ROW_INDEX_TEMPORARY_COLUMN_NAME,
                arrow_schema::DataType::Int64,
                false,
            )));
            Arc::new(arrow_schema::Schema::new(fields))
        } else {
            required_schema.clone()
        };

        // The instant range excludes whole base files, and the decision needs
        // only the file's commit instant, so it is made before opening rather
        // than by reading every row and dropping them.
        if !self.base_file_in_range()? {
            return Ok(BaseSource::empty(base_read_schema));
        }

        // Open the base file as a stream. The whole file never lives in memory;
        // one batch does. The (CoW-gated) RowFilter is threaded through the
        // intersection read so row groups can be pruned via column-index stats
        // (the builder resolves predicate columns by name and returns None when
        // any referenced column is absent — safe even for evolved/added cols).
        let base_stream = self
            .base_file_reader()?
            .read_stream(
                &path,
                base_read_options(row_filter.clone(), key_predicate.clone(), use_position)
                    .with_projection(intersection.fields().iter().map(|f| f.name())),
            )
            .await
            .map_err(|e| {
                CoreError::ReadFileSliceError(format!(
                    "Failed to open base file stream '{path}': {e:?}"
                ))
            })?;

        let evolve_to = base_read_schema.clone();
        let evolved = futures::StreamExt::map(base_stream.into_stream(), move |b| match b {
            Ok(batch) => {
                crate::schema::batch_evolution::project_batch_to_schema(&batch, &evolve_to)
            }
            Err(e) => Err(CoreError::from(e)),
        });

        Ok(BaseSource {
            schema: base_read_schema,
            batches: evolved.boxed(),
        })
    }

    /// Whether this slice's base file is inside the read's instant range.
    ///
    /// A Hudi base file belongs to exactly one commit instant — encoded in its
    /// file name (`<fileId>_<writeToken>_<commit>.<ext>`) and surfaced as
    /// [`InputSplit::base_file_commit_time`]. So every row in the file shares
    /// that one instant, and the range test is a single per-file decision: keep
    /// the whole file or drop it.
    ///
    /// This mirrors the Java reader. `HoodieFileGroupReader` only applies
    /// `applyInstantRangeFilter` when `getInstantRange().isPresent()` (empty on a
    /// plain snapshot); inflight / rolled-back *base files* are otherwise excluded
    /// at the file-slice level by `HoodieTableFileSystemView`, never by a per-row
    /// `_hoodie_commit_time` test. The range here (set by the gluten adapter for a
    /// native snapshot read: instants <= latest completed) exists to exclude base
    /// files from inflight / rolled-back commits; log-block exclusion is handled
    /// separately in the log path via `valid_block_instants`, not here.
    ///
    /// Masking rows by the per-row `_hoodie_commit_time` *column* would be a
    /// fragile proxy: **virtual-key** tables
    /// (`hoodie.populate.meta.fields=false`) persist a NULL `_hoodie_commit_time`,
    /// so every base row would be masked out and the read would silently return
    /// 0 rows even though the file's own instant is in range.
    fn base_file_in_range(&self) -> Result<bool> {
        let Some(instant_range) = &self.reader_context.instant_range else {
            return Ok(true);
        };

        // Skip filtering for metadata table (mirrors Java line 356).
        if crate::util::path::is_metadata_table_path(&self.reader_context.table_path) {
            return Ok(true);
        }

        // Production: the FFI sets `base_file_commit_time`. Fall back to parsing it
        // from the base file name when unset (robustness / tests) so the per-file
        // decision still works.
        let file_commit_time = self.input_split.base_file_commit_time.clone().or_else(|| {
            self.input_split
                .base_file_path
                .as_deref()
                .and_then(Self::base_commit_time_from_path)
        });

        let timezone = self.reader_context.timezone();
        let keep = Self::base_file_in_instant_range(
            file_commit_time.as_deref(),
            instant_range,
            &timezone,
        )?;
        if !keep {
            log::debug!(
                "[HoodieFileGroupReader] base file commit {file_commit_time:?} outside the \
                 instant range — excluding the whole base file"
            );
        }
        Ok(keep)
    }

    // NOTE: the FileGroupMergeStream owns the OutputConverter and applies
    // it per emitted chunk in its `Iterator::next()`. The reader's
    // `output_converter` field only lives up to
    // `open()`, which takes ownership and hands it to the iterator.

    /// Best-effort parse of a base file's commit instant from its path
    /// (`…/<fileId>_<writeToken>_<commit>.<ext>`). Fallback for when
    /// [`InputSplit::base_file_commit_time`] is unset (the FFI normally sets it).
    fn base_commit_time_from_path(path: &str) -> Option<String> {
        let file_name = path.rsplit('/').next().unwrap_or(path);
        file_name
            .parse::<crate::file_group::base_file::BaseFile>()
            .ok()
            .map(|bf| bf.commit_timestamp)
    }

    /// Whether a base file's rows fall within `instant_range`, decided by the
    /// file's single commit instant.
    ///
    /// `None` (log-only slice, or an unparseable base-file name) → keep, matching
    /// the Java reader's default of not row-filtering a base read when it cannot
    /// be bounded.
    fn base_file_in_instant_range(
        base_file_commit_time: Option<&str>,
        instant_range: &crate::timeline::selector::InstantRange,
        timezone: &str,
    ) -> Result<bool> {
        match base_file_commit_time {
            // An unparseable commit instant (e.g. the short '001'-style instants some Hudi
            // write-path unit tests use) can't be datetime-bounded against the range. Fall
            // back to LEXICOGRAPHIC comparison, exactly matching the JVM reader -- which
            // compares instant strings (InstantComparison) and never parses. Hudi instants
            // are fixed-format numeric strings, so lexicographic order equals chronological
            // order; keeping the file unconditionally instead would admit rows Java excludes
            // (duplicates in incremental reads). Production commit instants always parse, so
            // this fallback is inert there.
            Some(commit_time) => match instant_range.is_in_range(commit_time, timezone) {
                Ok(in_range) => Ok(in_range),
                Err(e) => {
                    let in_range = instant_range.is_in_range_lexicographic(commit_time);
                    log::debug!(
                        "[HoodieFileGroupReader] base_file_in_instant_range: commit instant \
                         '{commit_time}' is not a parseable datetime ({e}); using lexicographic \
                         comparison (JVM InstantComparison parity) -> in_range={in_range}"
                    );
                    Ok(in_range)
                }
            },
            None => Ok(true),
        }
    }

    // =========================================================================
    // Setters (mirrors Java's mutable field assignments)
    // =========================================================================

    /// Set the output converter.
    /// Mirrors Java: `this.outputConverter = readerContext.getSchemaHandler().getOutputConverter()`.
    /// Set by the FFI/harness path before `open`; the adapter path installs neither.
    #[allow(dead_code)]
    pub fn set_output_converter(&mut self, converter: Box<dyn OutputConverter>) {
        self.output_converter = Some(converter);
    }

    /// Set the buffered record converter.
    /// Mirrors Java: `this.bufferedRecordConverter = BufferedRecordConverter.createConverter(...)`.
    /// Set by the FFI/harness path — see `set_output_converter`.
    #[allow(dead_code)]
    pub fn set_buffered_record_converter(&mut self, converter: Box<dyn BufferedRecordConverter>) {
        self.buffered_record_converter = Some(converter);
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    /// Returns the read statistics collected during the read.
    /// Java-parity accessors; the adapter reads the stats it needs off the returned
    /// value.
    #[allow(dead_code)]
    /// The stats this read accumulated.
    ///
    /// Complete after [`Self::read`], which folds the merge-phase counters back
    /// in once the merge is exhausted. **After [`Self::open_stream`] the
    /// merge-phase counters read zero** - `final_merge_ms`, `output_build_ms`,
    /// `merge_map_peak_entries` and the insert/update/delete counts accumulate
    /// into the shared `stream_stats` handle as the stream is consumed, and
    /// nothing folds them back, because the caller owns the stream and the
    /// reader cannot know when it ended. The scan-phase counters (log blocks,
    /// log records, corrupt blocks, rollbacks, base read) are populated on both
    /// paths.
    ///
    /// Worth stating because the gap is silent and reads as data: a streaming
    /// read of a fixture with five deletes reports `num_deletes: 0` while
    /// returning exactly the same rows as the eager read that reports five. No
    /// production caller reads these - only the test harness and the benchmark
    /// do - but that is precisely where a zero would be believed.
    pub fn read_stats(&self) -> &HoodieReadStats {
        &self.read_stats
    }

    /// Returns the valid block instants from log scanning.
    /// See `read_stats`.
    #[allow(dead_code)]
    pub fn valid_block_instants(&self) -> &[String] {
        &self.valid_block_instants
    }
}

// =========================================================================
// Builder
// =========================================================================

/// Builder for `HoodieFileGroupReader`.
///
/// Reached only from the test harness today — `FileGroupReader` constructs the
/// engine directly through [`adapter`](super::adapter). Kept because the
/// harness is what drives the engine the way an FFI caller would, so it is the
/// only exercise of this construction path.
#[allow(dead_code)]
///
/// Mirrors Java's `HoodieFileGroupReader.Builder<T>`.
#[derive(Default)]
pub struct HoodieFileGroupReaderBuilder {
    reader_context: Option<Arc<ReaderContext>>,
    storage: Option<Arc<Storage>>,
    input_split: Option<InputSplit>,
    reader_parameters: ReaderParameters,
    data_schema: Option<SchemaRef>,
    requested_schema: Option<SchemaRef>,
    /// Set by `with_row_filter_builder`; copied onto a cloned reader_context
    /// at build time so the same builder is visible to base parquet reads
    /// (this file) and parquet log block decodes (`log_file::content`).
    row_filter_builder: Option<RowFilterBuilder>,
    /// Set by `with_mor_pk_safe`; copied onto the cloned reader_context.
    mor_pk_safe: Option<bool>,
}

/// Reached only from the test harness — see the builder's own note.
#[allow(dead_code)]
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

    /// install a parquet `RowFilter` builder.
    ///
    /// Whether the builder is actually used at scan time is gated by
    /// `reader_context.can_push_row_filter()`:
    /// - CoW table → always pushed
    /// - MOR table → pushed only if `mor_pk_safe` is true (see
    ///   [`Self::with_mor_pk_safe`])
    ///
    /// The builder is also visible to the parquet log block decoder via the
    /// same `reader_context` channel.
    pub fn with_row_filter_builder(mut self, b: RowFilterBuilder) -> Self {
        self.row_filter_builder = Some(b);
        self
    }

    /// mark the pushed predicate as safe for MOR (i.e. it
    /// references only primary-key columns). When true, the row filter
    /// pushes into both base parquet files and parquet log blocks on MOR
    /// tables. When false (default), the filter pushes only on CoW.
    ///
    /// Compute via [`crate::file_group::predicate::PushedFilter::references_only_primary_keys`]
    /// (lives in the cpp crate via FFI) and pass the result here.
    pub fn with_mor_pk_safe(mut self, mor_pk_safe: bool) -> Self {
        self.mor_pk_safe = Some(mor_pk_safe);
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

        // If the caller set a row_filter_builder or mor_pk_safe via the
        // builder API, copy them onto the reader_context. Clone-and-replace
        // mirrors the same pattern HoodieFileGroupReader::new() uses to
        // update the schema_handler on its reader_context.
        let reader_context = if self.row_filter_builder.is_some() || self.mor_pk_safe.is_some() {
            let mut updated = (*reader_context).clone();
            if let Some(b) = self.row_filter_builder {
                updated.row_filter_builder = Some(b);
            }
            if let Some(s) = self.mor_pk_safe {
                updated.mor_pk_safe = s;
            }
            Arc::new(updated)
        } else {
            reader_context
        };

        let reader = HoodieFileGroupReader::new(
            reader_context,
            storage,
            input_split,
            self.reader_parameters,
            self.data_schema,
            self.requested_schema,
        )?;

        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HudiConfigs;
    use crate::config::table::HudiTableConfig;
    use crate::storage::util::parse_uri;
    use arrow_array::Array;
    use std::collections::HashMap;

    use crate::timeline::selector::InstantRange;

    // the base-file instant-range decision is per-file (keyed on
    // `base_file_commit_time`), NOT per-row on the `_hoodie_commit_time` column —
    // so a virtual-key table (NULL commit-time column) is never wrongly dropped.
    #[test]
    fn base_file_in_instant_range_uses_file_commit_not_row_column() {
        // Range = up_to(latest) = (-INF, latest]  (the gluten snapshot cap).
        let latest = "20260710235017614";
        let range = InstantRange::up_to(latest, "UTC");

        // Valid base file at the latest completed commit → kept (inclusive end).
        // (Its rows' `_hoodie_commit_time` column is irrelevant here — could be NULL
        // for a virtual-key table; the decision is the file's own instant.)
        assert!(
            HoodieFileGroupReader::base_file_in_instant_range(Some(latest), &range, "UTC").unwrap(),
            "base file at the latest completed instant must be kept"
        );

        // A base file from a later (inflight / rolled-back) commit → excluded.
        assert!(
            !HoodieFileGroupReader::base_file_in_instant_range(
                Some("20260710235017615"),
                &range,
                "UTC"
            )
            .unwrap(),
            "base file newer than the range end must be excluded (C-PENDING-ROLLBACK)"
        );

        // No parseable base-file commit (log-only / unknown) → keep (Java default).
        assert!(
            HoodieFileGroupReader::base_file_in_instant_range(None, &range, "UTC").unwrap(),
            "unknown base-file commit must default to keep"
        );
    }

    // An unparseable base-file commit instant (e.g. the short '001'-style instants some Hudi
    // write-path unit tests use) must not fail the read — it falls back to LEXICOGRAPHIC
    // comparison, matching the JVM reader's InstantComparison (string compare, never parses).
    // The fallback must both KEEP in-range instants and EXCLUDE out-of-range ones; an
    // unconditional keep would admit rows Java excludes (dups in incremental reads).
    #[test]
    fn base_file_in_instant_range_unparseable_commit_uses_lexicographic() {
        // "001" <= end lexicographically -> kept (same outcome Java's string compare gives).
        let range = InstantRange::up_to("20260710235017614", "UTC");
        assert!(
            HoodieFileGroupReader::base_file_in_instant_range(Some("001"), &range, "UTC").unwrap(),
            "unparseable instant within the range must be kept, not error"
        );
        // "001" <= open start "100" lexicographically -> EXCLUDED, exactly as Java would.
        let range = InstantRange::within_open_closed("100", "20260710235017614", "UTC");
        assert!(
            !HoodieFileGroupReader::base_file_in_instant_range(Some("001"), &range, "UTC").unwrap(),
            "unparseable instant before the open start must be excluded (Java string-compare \
             parity), not kept unconditionally"
        );
    }

    #[test]
    fn base_file_in_instant_range_open_start_excludes_base_at_start() {
        // within_open_closed(base, log] — mirrors `instant_range_excludes_base`:
        // the base file's own instant (== open start) is excluded.
        let range =
            InstantRange::within_open_closed("20240101120000000", "20240101130000000", "UTC");
        assert!(
            !HoodieFileGroupReader::base_file_in_instant_range(
                Some("20240101120000000"),
                &range,
                "UTC"
            )
            .unwrap(),
            "open start must exclude a base file whose commit == start"
        );
        assert!(
            HoodieFileGroupReader::base_file_in_instant_range(
                Some("20240101123000000"),
                &range,
                "UTC"
            )
            .unwrap(),
            "a base file inside (start, end] must be kept"
        );
    }

    /// Write `batch` to `<dir>/<name>` as a parquet file. Minimal inline
    /// ArrowWriter helper (reader/mod.rs has no parquet-writing helper of its own).
    fn write_parquet_file(dir: &std::path::Path, name: &str, batch: &RecordBatch) {
        use parquet::arrow::ArrowWriter;
        let file = std::fs::File::create(dir.join(name)).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    /// As [`write_parquet_file`], but capping the row-group size so the file has
    /// several of them. A base file with one row group cannot tell a reader that
    /// keeps every group from one that keeps the first.
    fn write_parquet_file_in_row_groups(
        dir: &std::path::Path,
        name: &str,
        batch: &RecordBatch,
        rows_per_group: usize,
    ) {
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        let props = WriterProperties::builder()
            .set_max_row_group_size(rows_per_group)
            .build();
        let file = std::fs::File::create(dir.join(name)).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    /// Build a `HoodieFileGroupReader` rooted at `dir`, with a base file at
    /// `base_name` and `required` set as the `required_schema` driving the read.
    async fn test_file_group_reader_for_base_file(
        dir: &std::path::Path,
        base_name: &str,
        required: SchemaRef,
    ) -> HoodieFileGroupReader {
        let base_path = dir.to_str().unwrap().to_string();
        let hudi_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath.as_ref(),
            base_path,
        )]));
        let storage = Storage::new(Arc::new(HashMap::new()), hudi_configs).unwrap();

        let input_split =
            InputSplit::new(Some(base_name.to_string()), None, Vec::new(), String::new());

        let mut reader_context = ReaderContext::empty();
        reader_context.latest_commit_time =
            crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string();
        reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
        reader_context.rebuild_record_context(String::new());

        let mut reader = HoodieFileGroupReader::new(
            Arc::new(reader_context),
            storage,
            input_split,
            ReaderParameters::default(),
            None,
            None,
        )
        .unwrap();

        // Drive base_file_source with the exact required schema under test,
        // bypassing prepare_required_schema's meta/key-field augmentation.
        reader.schema_handler.required_schema = Some(required);
        reader
    }

    /// Drain a base file source into one concatenated `RecordBatch`, under the
    /// schema the source reports.
    async fn drain_base_source(source: BaseSource) -> RecordBatch {
        let BaseSource { schema, batches } = source;
        let batches: Vec<RecordBatch> = batches.map(|r| r.unwrap()).collect().await;
        if batches.is_empty() {
            RecordBatch::new_empty(schema)
        } else {
            arrow::compute::concat_batches(&schema, &batches).unwrap()
        }
    }

    /// Base file written at s1 {meta..., id:int, price:float}; required schema at
    /// s2 {id:long, price:double, tag:string?}: missing column null-filled, int
    /// widened, float→double value-exact. Mirrors Java's HoodieParquetFileFormatHelper.
    ///
    /// Runs against BOTH base-file source modes —
    /// Runs against the base source as the merge sees it and against its
    /// collapsed single-batch form — the shape `read()` merges — to prove the
    /// evolution is applied per row group and does not depend on how the base is
    /// chunked. The two outputs must be byte-identical.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_base_file_source_schema_on_write_evolution() {
        use arrow_array::{Float32Array, Int32Array};
        // -- write a parquet base file with OLD schema --
        let tmp = tempfile::tempdir().unwrap();
        let file_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, true),
            arrow_schema::Field::new("price", arrow_schema::DataType::Float32, true),
        ]));
        let batch = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["k1"])),
                Arc::new(Int32Array::from(vec![7])),
                Arc::new(Float32Array::from(vec![0.1f32])),
            ],
        )
        .unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &batch);

        // -- required schema = NEW shape --
        let required = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
            arrow_schema::Field::new("price", arrow_schema::DataType::Float64, true),
            arrow_schema::Field::new("tag", arrow_schema::DataType::Utf8, true),
        ]));

        // Assert the evolution invariants on a drained base-file source.
        let assert_evolved = |out: &RecordBatch| {
            assert_eq!(out.schema(), required);
            let id = out
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            assert_eq!(id.value(0), 7);
            let price = out
                .column(2)
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .unwrap();
            assert_eq!(
                price.value(0),
                0.1f64,
                "float→double must be value-exact (gold C6)"
            );
            assert!(out.column(3).is_null(0), "added column null-filled");
        };

        // The evolution is applied per row group, so draining the source and
        // concatenating must give the same rows as reading it whole would - the
        // property `read()` relies on now that it collects the merged chunks
        // rather than collapsing the base first.
        let dir = tmp.path().to_path_buf();
        let mut reader =
            test_file_group_reader_for_base_file(&dir, base_name, required.clone()).await;
        let streamed = drain_base_source(reader.base_file_source().await.unwrap()).await;
        assert_evolved(&streamed);
    }

    /// A base source feeding a position-based merge carries the row-position
    /// column, and one feeding a key-based merge does not.
    ///
    /// This is the join between the base read and the position buffer: the
    /// buffer looks the column up by name and errors when it is absent, so a
    /// read whose base source omits it cannot merge by position at all. Asserted
    /// on both the eager and streaming sources, which open the parquet file
    /// through different calls and could disagree.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_base_file_source_carries_row_positions_for_position_merge() {
        use arrow_array::{Int32Array, Int64Array};

        let tmp = tempfile::tempdir().unwrap();
        let file_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["k1", "k2", "k3"])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )
        .unwrap();
        let base_name = "f1-0_0-1-1_20240101120000000.parquet";
        write_parquet_file(tmp.path(), base_name, &batch);

        let required = file_schema.clone();
        let dir = tmp.path().to_path_buf();

        let build = |use_record_position: bool| {
            let dir = dir.clone();
            let required = required.clone();
            async move {
                let mut reader =
                    test_file_group_reader_for_base_file(&dir, base_name, required).await;
                // Position merge only applies to a slice that has log records to
                // merge, and only when the base file's instant is known.
                reader.input_split = InputSplit::new(
                    Some(base_name.to_string()),
                    Some("20240101120000000".to_string()),
                    vec![".f1-0_20240101130000000.log.1_0-1-1".to_string()],
                    String::new(),
                );
                reader.reader_parameters = ReaderParameters {
                    use_record_position,
                    ..Default::default()
                };
                reader
            }
        };

        let mut positional = build(true).await;
        let eager = drain_base_source(positional.base_file_source().await.unwrap()).await;
        let positions = eager
            .column_by_name(ROW_INDEX_TEMPORARY_COLUMN_NAME)
            .expect("position merge needs the row-position column on the base source")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("row positions are Int64");
        assert_eq!(positions.values(), &[0, 1, 2]);

        let mut keyed = build(false).await;
        let without = drain_base_source(keyed.base_file_source().await.unwrap()).await;
        assert_eq!(
            without.schema(),
            required,
            "a key-based merge must not pay for the row-position column"
        );
    }

    #[tokio::test]
    async fn test_make_base_file_batches_case_insensitive_column_match() {
        use arrow_array::Int32Array;
        // Base file written with `ID` (uppercase); required schema asks for
        // `id`. A case-sensitive intersection drops `ID` from the parquet
        // projection, so the column is never read and `project_batch_to_schema`
        // null-fills `id` — silently discarding the real values. The whole base
        // read path must match names case-insensitively (gold/Spark behavior).
        let tmp = tempfile::tempdir().unwrap();
        let file_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("ID", arrow_schema::DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["k1"])),
                Arc::new(Int32Array::from(vec![7])),
            ],
        )
        .unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &batch);

        let required = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, true),
        ]));

        let mut reader =
            test_file_group_reader_for_base_file(tmp.path(), base_name, required.clone()).await;
        let source = reader.base_file_source().await.unwrap();
        let out = drain_base_source(source).await;
        assert_eq!(out.schema(), required);
        let id = out.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(
            !id.is_null(0),
            "case-differing column must not be silently null-filled"
        );
        assert_eq!(
            id.value(0),
            7,
            "real value must survive case-insensitive column match"
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // builder routes `with_row_filter_builder` and
    // `with_mor_pk_safe` onto the shared `reader_context` so both the base
    // parquet read site (this file's `base_file_source`) and the
    // parquet log block decoder (`file_group::log_file::content::Decoder`)
    // see the same gating decision.
    //
    // Pure builder-state tests — they exercise the builder plumbing without
    // actually executing a read. End-to-end integration is covered by the
    // FFI-level tests + the lake-loader functional benchmark.
    // ════════════════════════════════════════════════════════════════════

    fn dummy_reader_context(table_type: &str) -> Arc<ReaderContext> {
        let mut ctx = ReaderContext::empty();
        ctx.table_config
            .insert("hoodie.table.type".to_string(), table_type.to_string());
        Arc::new(ctx)
    }

    fn dummy_input_split() -> InputSplit {
        // Bare split: no base file, no log files. Sufficient for builder
        // plumbing assertions — we never call read().
        InputSplit::new(None, None, vec![], "p1".to_string())
    }

    fn make_row_filter_builder() -> RowFilterBuilder {
        // Closure that always returns None — we only care that the builder
        // was installed, not what it produces.
        std::sync::Arc::new(|_parquet_schema, _projected_schema| None)
    }

    #[test]
    fn builder_routes_row_filter_builder_into_reader_context() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let reader = HoodieFileGroupReader::builder()
            .with_reader_context(dummy_reader_context("MERGE_ON_READ"))
            .with_storage(storage)
            .with_input_split(dummy_input_split())
            .with_row_filter_builder(make_row_filter_builder())
            .build()
            .unwrap();
        assert!(
            reader.reader_context.row_filter_builder.is_some(),
            "with_row_filter_builder should land on reader_context"
        );
    }

    #[test]
    fn builder_mor_pk_safe_true_unlocks_pushdown_on_mor() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let reader = HoodieFileGroupReader::builder()
            .with_reader_context(dummy_reader_context("MERGE_ON_READ"))
            .with_storage(storage)
            .with_input_split(dummy_input_split())
            .with_row_filter_builder(make_row_filter_builder())
            .with_mor_pk_safe(true)
            .build()
            .unwrap();
        assert!(reader.reader_context.mor_pk_safe);
        assert!(
            reader.reader_context.can_push_row_filter(),
            "MOR + mor_pk_safe=true must push"
        );
    }

    #[test]
    fn builder_mor_pk_safe_false_blocks_pushdown_on_mor() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let reader = HoodieFileGroupReader::builder()
            .with_reader_context(dummy_reader_context("MERGE_ON_READ"))
            .with_storage(storage)
            .with_input_split(dummy_input_split())
            .with_row_filter_builder(make_row_filter_builder())
            // mor_pk_safe defaults to false
            .build()
            .unwrap();
        assert!(!reader.reader_context.mor_pk_safe);
        assert!(
            !reader.reader_context.can_push_row_filter(),
            "MOR without PK-safety must NOT push (mirrors Java's morFilters gate)"
        );
    }

    // Bootstrap base files are rejected loudly at reader construction.
    // `needs_bootstrap_merge = true` (set when the table has bootstrap base files
    // requiring meta/data column reordering) must surface as CoreError::Unsupported
    // from HoodieFileGroupReader::new, not a silent wrong-data read or a panic.
    // The gate lives just after prepare_required_schema (this file, ~line 264).
    #[tokio::test]
    async fn test_bootstrap_merge_rejected_at_construction() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();

        let mut reader_context = ReaderContext::empty();
        reader_context.latest_commit_time =
            crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string();
        reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
        // Trigger condition: table has bootstrap base files (skeleton/data split).
        reader_context.needs_bootstrap_merge = true;
        reader_context.rebuild_record_context(String::new());

        // A minimal data schema lets prepare_required_schema run so the bootstrap
        // gate (which fires immediately after) is the failing point.
        let data_schema: SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
        ]));

        let input_split = InputSplit::new(
            Some("f1-0_0-1-1_001.parquet".to_string()),
            None,
            Vec::new(),
            String::new(),
        );

        let result = HoodieFileGroupReader::new(
            Arc::new(reader_context),
            storage,
            input_split,
            ReaderParameters::default(),
            Some(data_schema.clone()),
            Some(data_schema),
        );

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("bootstrap merge must be rejected at construction"),
        };
        assert!(
            matches!(err, CoreError::Unsupported(_)),
            "expected CoreError::Unsupported, got {err:?}"
        );
        assert!(
            err.to_string().contains("Bootstrap merge"),
            "error should mention Bootstrap merge, got: {err}"
        );
    }

    // Schema-on-read (InternalSchema) is rejected loudly at reader
    // construction. `hoodie.schema.on.read.enable=true` in table_config must
    // surface as CoreError::Unsupported rather than being silently ignored
    // (silent-wrong-data risk: InternalSchema evolution would be misread).
    // The gate lives just after prepare_required_schema (this file, ~line 264),
    // alongside the bootstrap gate. The FgReaderCase harness has no table_config
    // injection field, so this is asserted as a unit test at the gate's layer.
    #[tokio::test]
    async fn test_schema_on_read_rejected_at_construction() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();

        let mut reader_context = ReaderContext::empty();
        reader_context.latest_commit_time =
            crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string();
        reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
        // Trigger condition: table opts into schema-on-read / InternalSchema.
        reader_context.table_config.insert(
            "hoodie.schema.on.read.enable".to_string(),
            "true".to_string(),
        );
        reader_context.rebuild_record_context(String::new());

        let data_schema: SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
        ]));

        let input_split = InputSplit::new(
            Some("f1-0_0-1-1_001.parquet".to_string()),
            None,
            Vec::new(),
            String::new(),
        );

        let result = HoodieFileGroupReader::new(
            Arc::new(reader_context),
            storage,
            input_split,
            ReaderParameters::default(),
            Some(data_schema.clone()),
            Some(data_schema),
        );

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("schema-on-read must be rejected at construction"),
        };
        assert!(
            matches!(err, CoreError::Unsupported(_)),
            "expected CoreError::Unsupported, got {err:?}"
        );
        assert!(
            err.to_string().contains("schema-on-read"),
            "error should mention schema-on-read, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_composite_virtual_keys_accepted_at_construction() {
        // Composite virtual keys (virtual keys + a multi-field record key)
        // are supported — `RecordContext::record_key_array` reconstructs the full
        // `field:val,field:val` merge key per row on both sides, so construction
        // must succeed rather than erroring `CoreError::Unsupported`.
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();

        let mut reader_context = ReaderContext::empty();
        reader_context.latest_commit_time =
            crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string();
        reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
        // Trigger: virtual keys (no meta fields) + a multi-field record key.
        reader_context.table_config.insert(
            "hoodie.populate.meta.fields".to_string(),
            "false".to_string(),
        );
        reader_context.table_config.insert(
            "hoodie.table.recordkey.fields".to_string(),
            "pk1,pk2".to_string(),
        );
        reader_context.rebuild_record_context(String::new());
        // The full record-key field list is retained (not just the first field).
        assert_eq!(
            reader_context.get_record_context().record_key_fields,
            vec!["pk1", "pk2"],
        );

        let data_schema: SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("pk1", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("pk2", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("v", arrow_schema::DataType::Int64, true),
        ]));

        let input_split = InputSplit::new(
            Some("f1-0_0-1-1_001.parquet".to_string()),
            None,
            Vec::new(),
            String::new(),
        );

        let result = HoodieFileGroupReader::new(
            Arc::new(reader_context),
            storage,
            input_split,
            ReaderParameters::default(),
            Some(data_schema.clone()),
            Some(data_schema),
        );

        assert!(
            result.is_ok(),
            "composite virtual keys must be accepted at construction, got {:?}",
            result.err(),
        );
    }

    #[tokio::test]
    async fn test_composite_precombine_accepted_at_construction() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();

        let mut reader_context = ReaderContext::empty();
        reader_context.latest_commit_time =
            crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string();
        reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
        // Multi-field (comma-separated) precombine is supported: RecordContext splits
        // it into ordering_field_names and get_ordering_values builds a composite
        // ordering value per row — no silent first-field-only degradation, so
        // construction must accept it.
        reader_context.table_config.insert(
            "hoodie.table.precombine.field".to_string(),
            "ts,seq".to_string(),
        );
        reader_context.rebuild_record_context(String::new());
        // Both ordering fields are parsed (not just the first).
        assert_eq!(
            reader_context.record_context.ordering_field_names,
            vec!["ts".to_string(), "seq".to_string()],
            "comma-separated precombine must split into all ordering fields"
        );

        let data_schema: SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("pk", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("ts", arrow_schema::DataType::Int64, true),
            arrow_schema::Field::new("seq", arrow_schema::DataType::Int64, true),
        ]));

        let input_split = InputSplit::new(
            Some("f1-0_0-1-1_001.parquet".to_string()),
            None,
            Vec::new(),
            String::new(),
        );

        let result = HoodieFileGroupReader::new(
            Arc::new(reader_context),
            storage,
            input_split,
            ReaderParameters::default(),
            Some(data_schema.clone()),
            Some(data_schema),
        );

        assert!(
            result.is_ok(),
            "multi-field precombine must be accepted at construction, got {:?}",
            result.err()
        );
    }

    #[test]
    fn builder_cow_always_pushes_regardless_of_mor_pk_safe() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let reader = HoodieFileGroupReader::builder()
            .with_reader_context(dummy_reader_context("COPY_ON_WRITE"))
            .with_storage(storage)
            .with_input_split(dummy_input_split())
            .with_row_filter_builder(make_row_filter_builder())
            // mor_pk_safe stays default false — irrelevant for CoW.
            .build()
            .unwrap();
        assert!(
            reader.reader_context.can_push_row_filter(),
            "CoW path always pushes regardless of mor_pk_safe"
        );
    }

    /// A merged chunk is bounded, and the bound is the reader's, not the base
    /// file's layout.
    ///
    /// Merging a chunk is synchronous work on the task that polls the stream and
    /// its cost is linear in the chunk's rows, so an unbounded chunk is an
    /// unbounded poll. The fixture puts 5000 rows in a single row group: if the
    /// chunk followed the file's layout, one chunk would carry all 5000 and one
    /// poll would do five times the work `MERGE_CHUNK_ROWS` allows for.
    ///
    /// The direct assertion on the option is deliberate. The bound currently
    /// agrees with what `parquet` defaults to, so no output-level test can tell
    /// the pin from the default — but a caller that passed a larger batch size
    /// through here (making `hoodie.read.stream.batch_size` effective on the
    /// merge path, say) would multiply every poll's cost, and this is what says
    /// so out loud.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_merged_chunk_is_bounded_by_the_readers_own_batch_size() {
        // Every argument combination must carry the pin: the position-merge
        // read (row-index column attached) and the filtered read bound their
        // polls by the same argument as the plain one, so a refactor that
        // branched the builder per arm must not lose it on any branch.
        for use_position in [false, true] {
            for filter in [None, Some(make_row_filter_builder())] {
                assert_eq!(
                    base_read_options(filter, None, use_position).batch_size,
                    Some(MERGE_CHUNK_ROWS),
                    "the base read must ask for the merge's chunk bound rather than \
                     inherit one (use_position={use_position})"
                );
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int32,
            true,
        )]));
        let rows = 5_000;
        let ids: Vec<i32> = (0..rows).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int32Array::from(ids.clone()))],
        )
        .unwrap();
        let base_name = "one-big-group.parquet";
        // One row group holding every row, so the file's layout cannot be what
        // bounds the chunk.
        write_parquet_file_in_row_groups(tmp.path(), base_name, &batch, rows as usize);

        let mut reader =
            test_file_group_reader_for_base_file(tmp.path(), base_name, schema.clone()).await;
        let mut stream = reader.open_stream().await.unwrap();
        let mut sizes: Vec<usize> = Vec::new();
        let mut total = 0usize;
        while let Some(b) = stream.next().await {
            let b = b.unwrap();
            sizes.push(b.num_rows());
            total += b.num_rows();
        }
        assert_eq!(total, rows as usize, "every row must still come back");
        assert!(
            sizes.iter().all(|n| *n <= MERGE_CHUNK_ROWS),
            "every chunk must respect the bound, got {sizes:?}"
        );
        assert!(
            sizes.len() > 1,
            "5000 rows cannot arrive in one chunk under a {MERGE_CHUNK_ROWS}-row bound"
        );
    }

    /// Build a reader over a base file plus one real log file, so the read
    /// takes the Buffered (merge) path rather than the Eager one. The reader
    /// schema is the single `_hoodie_record_key` column, which is enough to
    /// decode the shipped log fixtures and extract keys on both sides.
    async fn test_file_group_reader_for_merged_slice(
        dir: &std::path::Path,
        base_name: &str,
        log_name: &str,
        required: SchemaRef,
    ) -> HoodieFileGroupReader {
        let base_path = dir.to_str().unwrap().to_string();
        let hudi_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath.as_ref(),
            base_path,
        )]));
        let storage = Storage::new(Arc::new(HashMap::new()), hudi_configs).unwrap();

        let input_split = InputSplit::new(
            Some(base_name.to_string()),
            None,
            vec![log_name.to_string()],
            String::new(),
        );

        let mut reader_context = ReaderContext::empty();
        reader_context.latest_commit_time =
            crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string();
        reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
        // Set on purpose: the knob is documented to size base-file-only slices
        // and to have NO effect on a merged slice. The chunk assertions in the
        // test below are what hold that, rather than a one-off measurement.
        reader_context.hoodie_reader_config.insert(
            crate::config::read::HudiReadConfig::StreamBatchSize
                .as_ref()
                .to_string(),
            "8192".to_string(),
        );
        reader_context.rebuild_record_context(String::new());
        // The log scan decodes blocks and builds the delete context through the
        // context's own schema handler, so it needs the prepared one.
        let mut handler =
            crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler::new()
                .with_table_schema(required.clone())
                .with_data_schema(required.clone());
        handler
            .prepare_required_schema(
                true,
                &["_hoodie_record_key".to_string()],
                &[],
                &reader_context.table_config,
                false,
                "COMMIT_TIME_ORDERING",
            )
            .unwrap();
        reader_context.schema_handler = handler;

        let mut reader = HoodieFileGroupReader::new(
            Arc::new(reader_context),
            storage,
            input_split,
            ReaderParameters::default(),
            None,
            None,
        )
        .unwrap();
        reader.schema_handler.required_schema = Some(required);
        reader
    }

    /// The chunk bound on the path it exists for: a slice WITH log files.
    ///
    /// `test_a_merged_chunk_is_bounded_by_the_readers_own_batch_size` above
    /// drives the Eager (base-only) arm, so it pins the option and the base
    /// read but never the Buffered state machine. This one merges a 5000-row
    /// single-row-group base against a real delete-block log file, so every
    /// chunk it observes came out of `merge_base_batch`: a state machine that
    /// coalesced source batches, or a base read that lost the bound only on
    /// the merge route, fails here and nowhere else.
    ///
    /// The fixture's delete keys are trips UUIDs and the base keys are
    /// synthetic, so nothing matches: every base row survives, the drain has
    /// nothing to emit, and the chunk cadence is exactly what the machine
    /// produced. `hoodie.read.stream.batch_size=8192` is set in the reader
    /// config on purpose — the knob must have no effect on a merged slice.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_the_chunk_bound_holds_on_a_slice_with_log_files() {
        let tmp = tempfile::tempdir().unwrap();
        let log_name = ".6d3d1d6e-2298-4080-a0c1-494877d6f40a-0_20250618054711154.log.1_0-26-85";
        let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/log_files/valid_log_delete")
            .join(log_name);
        std::fs::copy(&fixture, tmp.path().join(log_name)).unwrap();

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "_hoodie_record_key",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let base_rows: usize = 5_000;
        let keys: Vec<String> = (0..base_rows).map(|i| format!("base-{i:05}")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::StringArray::from(
                keys.iter().map(String::as_str).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let base_name = "one-big-group.parquet";
        // One row group holding every row, so the file's layout cannot be what
        // bounds the chunk.
        write_parquet_file_in_row_groups(tmp.path(), base_name, &batch, base_rows);

        let mut eager = test_file_group_reader_for_merged_slice(
            tmp.path(),
            base_name,
            log_name,
            schema.clone(),
        )
        .await;
        let expected_total = eager.read().await.unwrap().num_rows();
        assert_eq!(
            expected_total, base_rows,
            "no fixture delete key may collide with a synthetic base key"
        );

        let mut reader = test_file_group_reader_for_merged_slice(
            tmp.path(),
            base_name,
            log_name,
            schema.clone(),
        )
        .await;
        let mut stream = reader.open_stream().await.unwrap();
        let mut sizes: Vec<usize> = Vec::new();
        while let Some(b) = stream.next().await {
            sizes.push(b.unwrap().num_rows());
        }
        assert_eq!(
            sizes.iter().sum::<usize>(),
            expected_total,
            "the streamed merge must return the same rows as the eager read"
        );
        assert!(
            sizes.iter().all(|n| *n <= MERGE_CHUNK_ROWS),
            "every merged chunk must respect the bound, got {sizes:?}"
        );
        assert!(
            sizes.len() >= base_rows / MERGE_CHUNK_ROWS,
            "{base_rows} base rows cannot arrive in {} chunk(s) under a \
             {MERGE_CHUNK_ROWS}-row bound: {sizes:?}",
            sizes.len()
        );
    }

    /// Every row group of the base file reaches the output, on both entry
    /// points.
    ///
    /// `read()` collapses the base to one batch before merging, and a collapse
    /// that kept only the first row group would return fewer rows and raise
    /// nothing — the exact shape of silent data loss this path must not have.
    /// No other test can see it: every base file elsewhere in the suite fits in
    /// a single row group, so keeping one group and keeping all of them look
    /// identical. The stream side asserts more than one chunk, which is what
    /// proves the fixture really has several groups.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_every_base_row_group_reaches_the_output() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int32,
            true,
        )]));
        let ids: Vec<i32> = (0..40).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int32Array::from(ids.clone()))],
        )
        .unwrap();
        let base_name = "many-groups.parquet";
        write_parquet_file_in_row_groups(tmp.path(), base_name, &batch, 10);

        let read_ids = |b: &RecordBatch| -> Vec<i32> {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        };

        let mut eager =
            test_file_group_reader_for_base_file(tmp.path(), base_name, schema.clone()).await;
        let one = eager.read().await.unwrap();
        assert_eq!(
            read_ids(&one),
            ids,
            "read() must return every row of every row group"
        );

        let mut streamed =
            test_file_group_reader_for_base_file(tmp.path(), base_name, schema.clone()).await;
        let mut stream = streamed.open_stream().await.unwrap();
        let mut chunks = 0usize;
        let mut got: Vec<i32> = Vec::new();
        while let Some(b) = stream.next().await {
            chunks += 1;
            got.extend(read_ids(&b.unwrap()));
        }
        assert!(
            chunks > 1,
            "the fixture must span several row groups for this test to mean anything, got {chunks}"
        );
        assert_eq!(got, ids, "the streamed read must return every row too");
    }

    /// The streaming path must return exactly what the single-batch one does.
    /// It merges the base file a row group at a time instead of whole, which is
    /// a memory and chunking difference, not a data one — so any divergence in
    /// the rows is a bug rather than a tradeoff.
    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_and_eager_reads_agree() {
        use futures::StreamExt;

        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, true),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(arrow_array::StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .unwrap();
        let base_name = "base.parquet";
        let file = std::fs::File::create(tmp.path().join(base_name)).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let mut eager_reader =
            test_file_group_reader_for_base_file(tmp.path(), base_name, schema.clone()).await;
        let eager = eager_reader.read().await.unwrap();

        let mut stream_reader =
            test_file_group_reader_for_base_file(tmp.path(), base_name, schema.clone()).await;
        let mut stream = stream_reader.open_stream().await.unwrap();
        let mut streamed: Vec<RecordBatch> = Vec::new();
        while let Some(b) = stream.next().await {
            streamed.push(b.unwrap());
        }
        assert!(
            !streamed.is_empty(),
            "the stream yielded nothing; it should emit at least one batch"
        );

        // Row content, not just a count. Counting alone passes for a stream that
        // returns the right number of wrong rows, which is the failure a merge
        // rewrite actually produces. Sorted, because the two entry points chunk
        // the base differently and Hudi promises no row order.
        let render = |batches: &[RecordBatch]| -> Vec<String> {
            let mut out: Vec<String> = batches
                .iter()
                .flat_map(|b| {
                    (0..b.num_rows()).map(move |r| {
                        (0..b.num_columns())
                            .map(|c| {
                                format!(
                                    "{:?}",
                                    arrow::util::display::array_value_to_string(b.column(c), r)
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                })
                .collect();
            out.sort();
            out
        };
        assert_eq!(
            render(&streamed),
            render(std::slice::from_ref(&eager)),
            "the streamed read must return the same rows as the single-batch read"
        );
    }
}
