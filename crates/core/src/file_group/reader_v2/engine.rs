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
//! Ported wholesale; nothing consumes it yet, so its items are unreachable
//! from the crate's call graph until it is wired in.
#![allow(dead_code)]

use crate::Result;
use crate::config::table::BaseFileFormatValue;
use crate::error::CoreError;
use crate::file_group::base_file::reader::{
    BaseFileReadOptions, BaseFileReader, create_base_file_reader,
};
use crate::file_group::reader_v2::buffer::loader::{
    DefaultFileGroupRecordBufferLoader, FileGroupRecordBufferLoader,
};
use crate::file_group::reader_v2::buffer::record_positions::ROW_INDEX_TEMPORARY_COLUMN_NAME;
use crate::file_group::reader_v2::buffered_record_converter::BufferedRecordConverter;
use crate::file_group::reader_v2::input_split::InputSplit;
use crate::file_group::reader_v2::iterator_mode::IteratorMode;
use crate::file_group::reader_v2::merge_iterator::{
    DEFAULT_BATCH_SIZE, FileGroupMergeIterator, StreamStatsHandle, new_stream_stats_handle,
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
    // returned `FileGroupMergeIterator` for the rest of the read.
    /// Optional converter for projecting/transforming output records.
    /// Mirrors Java's `Option<UnaryOperator<T>> outputConverter`.
    output_converter: Option<Box<dyn OutputConverter>>,

    /// Read statistics accumulator.
    read_stats: HoodieReadStats,

    /// Stage-timing sink shared with the [`FileGroupMergeIterator`] returned by
    /// [`Self::open`] (ENG-42991). The streaming iterator
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
    // NOTE: ENG-42866 — the optional parquet `RowFilter` builder used to live
    // on this struct. It now lives on `reader_context` so the same builder is
    // visible to (a) the base parquet read here, and (b) the parquet log
    // block decoder in `file_group::log_file::content::Decoder`. The gate
    // (CoW || mor_pk_safe) lives at the use sites; this file's gate is at
    // `make_base_file_source` below.
}

/// Base-file read options carrying an optional pushdown predicate.
///
/// The three base reads below differ only in projection, so the filter is
/// attached in one place — a read that silently lost it would return extra rows
/// rather than fail, which is the hard kind of bug to notice.
fn base_read_options(row_filter: Option<RowFilterBuilder>) -> BaseFileReadOptions {
    match row_filter {
        Some(f) => BaseFileReadOptions::new().with_row_filter(f),
        None => BaseFileReadOptions::new(),
    }
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

        // Schema-on-read (InternalSchema) evolution is not supported in hudi-rs
        // (GAP-07). Gold loads an InternalSchema from the `.schema` folder and
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
        // sharing the first field but differing on a later one no longer collide. See
        // `RecordContext::build_composite_record_key_array`.

        // Multi-field (composite) precombine/ordering keys ARE supported.
        // `RecordContext::new` splits a comma-separated `hoodie.table.precombine.field`
        // / `hoodie.table.ordering.fields` into `ordering_field_names`, and
        // `get_ordering_values` builds one `OrderingValue::Composite` per row from
        // the per-field scalars (compared lexicographically field-by-field, mirroring
        // Java `OrderingValues`). A field absent from a batch, an unsupported field
        // type, or a null component falls back to natural order — matching the scalar
        // path — so there is no silent first-field-only degradation. (Construction
        // still rejects composite *virtual keys* above: that path reconstructs the
        // merge key from only the first record-key field, which would mis-collide.)

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

    /// Create a builder for configuring the reader.
    pub fn builder() -> HoodieFileGroupReaderBuilder {
        HoodieFileGroupReaderBuilder::default()
    }

    // =========================================================================
    // Main read API (mirrors Java's getClosableIterator / getBufferedRecordIterator)
    // =========================================================================

    /// Open the file group and return a streaming iterator over the merged
    /// output (ENG-42991). This is the modern entry point matching Java's
    /// `getClosableIterator()` semantics.
    ///
    /// Async work — base file decode + log file scan + buffer population —
    /// runs once, here. The returned [`FileGroupMergeIterator`] is then a
    /// purely synchronous [`arrow_array::RecordBatchReader`] that emits one
    /// chunk per `next()` (default chunk size [`DEFAULT_BATCH_SIZE`] rows).
    ///
    /// This is single-use: it consumes the output_converter and (for MOR)
    /// hands the buffer to the iterator. Re-opening requires constructing a
    /// new `HoodieFileGroupReader`.
    pub async fn open(&mut self) -> Result<FileGroupMergeIterator> {
        // ENG-42992: streaming mode — the base file is held as a lazy
        // `ParquetSyncReader` that does per-batch `block_on` against
        // OBJECT_STORE_RUNTIME. The caller MUST consume the returned
        // iterator from a synchronous context (e.g. the FFI driver),
        // never from inside another tokio runtime.
        self.init_record_iterators(/* streaming */ true).await
    }

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
        Ok(create_base_file_reader(&self.storage, &format)?)
    }

    /// Stream the merged output to an async caller.
    ///
    /// [`Self::read`] returns the whole file group as one batch, so peak memory
    /// tracks the base file. This reads the base file one row group at a time
    /// instead.
    ///
    /// The merge loop is synchronous and has to block on the base stream, which
    /// is only legal off the async worker threads. So it runs on a
    /// blocking-pool thread and hands batches back over a channel; the caller
    /// gets an ordinary stream and never sees the blocking.
    ///
    /// The channel holds one batch. That lets the producer decode row group
    /// N+1 while the consumer works on N, while bounding the extra memory to a
    /// single row group — a deeper channel would multiply peak memory by its
    /// depth, which is what streaming is meant to avoid.
    pub(crate) async fn open_blocking_stream(
        &mut self,
    ) -> Result<futures::stream::BoxStream<'static, Result<RecordBatch>>> {
        use futures::StreamExt;

        let iter = self.init_record_iterators(/* streaming */ true).await?;

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(1);
        tokio::task::spawn_blocking(move || {
            for batch in iter {
                let item = batch.map_err(CoreError::ArrowError);
                // A send error means the consumer dropped the stream; stop
                // rather than decoding row groups nobody will take.
                if tx.blocking_send(item).is_err() {
                    break;
                }
            }
        });

        Ok(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        })
        .boxed())
    }

    /// Read the file group and return the merged output as a single
    /// `RecordBatch`. Internally uses the same merge code path as
    /// [`Self::open`] but with **eager** base file decode — the parquet
    /// stream is drained async during this method's async body, then the
    /// resulting `Vec<RecordBatch>` is wrapped in a sync
    /// `RecordBatchIterator` for the merge loop. That makes the merge
    /// iterator safe to consume from async callers (no nested block_on).
    ///
    /// New consumers that can use a sync iterator should prefer
    /// [`Self::open`] for true streaming.
    pub async fn read(&mut self) -> Result<RecordBatch> {
        // Anything this read expects that is quietly not done, said once, before
        // the rows come back looking unremarkable.
        crate::file_group::reader_v2::gaps::report_for_read(
            &self.reader_context,
            &self.reader_parameters,
        );
        // A3 (ENG-42992): eager mode — the base parquet stream is drained
        // async during `init_record_iterators` (streaming=false), so the
        // returned iterator's `next()` is pure in-memory work and can be driven
        // from an async caller (no nested `block_on`). `open()` (streaming=true)
        // instead holds a lazy `ParquetSyncReader` for true streaming peak
        // memory, but requires a sync consumer (the FFI driver).
        let batch = self
            .init_record_iterators(/* streaming */ false)
            .await?
            .collect_into_one_batch()?;
        // ENG-42991 — the streaming iterator accumulated the merge-phase
        // timings + insert/update/delete counts into the shared `stream_stats`
        // while `collect_into_one_batch` drove it to exhaustion. Drain them back
        // into `self.read_stats` so `read_stats()`-based callers (fg-bench,
        // tests, reader_v1) observe the same stats the pre-streaming `read()`
        // surfaced. (The FFI `open()` path does not read these back — Velox
        // consumes the stream directly.)
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
    /// hand state to a [`FileGroupMergeIterator`].
    ///
    /// Mirrors Java's `HoodieFileGroupReader.initRecordIterators()`. The
    /// fast path (no log files = CoW / empty) returns an `Eager` iterator
    /// over the base file source; the MOR path returns a `Buffered`
    /// iterator that drives `buffer.has_next() / buffer.next()` in chunks.
    ///
    /// The `streaming` flag controls how the base file is read:
    /// - `true` — lazy `ParquetSyncReader` (one row group per
    ///   `next()`; does `block_on` per call → sync-context only).
    /// - `false` — async drain into `Vec<RecordBatch>`, wrap in
    ///   `RecordBatchIterator` (no `block_on`; safe from async callers).
    ///
    /// `apply_instant_range_filter` requires a materialised Vec today, so
    /// streaming mode falls back to eager when an instant range is active
    /// (rare; see ENG-42992 follow-up to make the filter per-batch).
    ///
    /// ```text
    /// initRecordIterators()
    ///   ├─ make_base_file_source(streaming)
    ///   └─ recordBufferLoader.getRecordBuffer(...)
    ///        → recordBuffer.set_base_file_source(...)
    ///        → FileGroupMergeIterator::new_buffered(...)
    /// ```
    async fn init_record_iterators(&mut self, streaming: bool) -> Result<FileGroupMergeIterator> {
        log::debug!(
            "[HoodieFileGroupReader] initRecordIterators: partition={} base_file={} log_files={} streaming={streaming}",
            self.input_split.partition_path,
            self.input_split
                .base_file_path
                .as_deref()
                .unwrap_or("<none>"),
            self.input_split.log_file_paths.len(),
        );

        // Step 1: Open the base file source (A3 / ENG-42992).
        //   - streaming=true:  a lazy `ParquetSyncReader` — one parquet
        //     row-group per `RecordBatchReader::next` call. The whole base
        //     file never lives in memory at once (this is the R3 fix).
        //   - streaming=false (or instant-range filter active, which still
        //     needs a materialised Vec): drain async into a `Vec<RecordBatch>`,
        //     optionally instant-range-filter it, wrap in a `RecordBatchIterator`.
        // Stage timing (perf harness): base parquet open + (eager) read.
        // In streaming mode this only covers the open; the per-row-group decode
        // cost is paid lazily inside the merge loop's `next_base_row` pulls.
        let base_source = profile_once!(
            self.read_stats.base_read_ms,
            self.make_base_file_source(streaming).await
        )?;
        let base_source_schema = base_source.schema();
        log::debug!(
            "[HoodieFileGroupReader] makeBaseFileSource: schema_cols={} streaming={streaming}",
            base_source_schema.fields().len(),
        );

        // The post-projection output schema is the same regardless of
        // CoW vs MOR — used as the iterator's RecordBatchReader::schema().
        let output_converter = self.output_converter.take();
        let post_projection_schema = output_converter.as_ref().map(|c| c.target_schema());

        // Step 2: If no records to merge (no log files), build an Eager
        // iterator that yields the base file batches directly.
        if self.input_split.has_no_records_to_merge() {
            log::debug!("[HoodieFileGroupReader] no log files → Eager iterator");

            // output converter runs. A3 (ENG-42992): the base source exposes
            // its (post-projection) schema via `RecordBatchReader::schema()`
            // without forcing a row-group decode, so we no longer peek at a
            // materialised first batch. For a log-only Eager FG the source is
            // an empty `RecordBatchIterator` carrying the required schema.
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
            return Ok(FileGroupMergeIterator::new_eager(
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

        let mut record_buffer = load_result.record_buffer;
        self.valid_block_instants = load_result.valid_block_instants;

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

        // Step 4: Determine merge_schema BEFORE handing the source to the
        // buffer. Schema is available via `RecordBatchReader::schema()`
        // without forcing a row-group decode.
        let merge_schema: SchemaRef = if let Some(rs) = &self.schema_handler.required_schema {
            rs.clone()
        } else if self.input_split.base_file_path.is_some() {
            // The base source's schema is the parquet schema after projection.
            base_source_schema.clone()
        } else {
            // Log-only file group: peek at any non-delete log record's batch
            // (HashMap order is non-deterministic, so we must search all
            // entries — the first record could be a delete).
            // Find the first non-delete record's schema (`get_record()` returns
            // `None` for a delete tombstone under the A2 `RecordPayload` design).
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

        // Step 5: Hand the base source to the buffer + return the streaming
        // iterator. The iterator owns the buffer from here on; the reader's
        // role ends.
        record_buffer.set_base_file_source(base_source);
        log::debug!(
            "[HoodieFileGroupReader] set base file source on buffer, \
             returning Buffered iterator (batch_size={DEFAULT_BATCH_SIZE})"
        );

        // Step 5: Hand the buffer to a Buffered streaming iterator. The
        // iterator owns the buffer and drives `has_next/next` per chunk; it
        // accumulates final_merge_ms + output_build_ms and the update-processor
        // insert/update/delete counts into the shared `stream_stats`, which
        // `read()` drains back into `self.read_stats` after the stream is
        // exhausted (mirrors gold, where StandardUpdateProcessor increments
        // HoodieReadStats during iteration). merge_map_peak_entries was already
        // recorded during the log scan; the iterator reads it off the buffer up
        // front (the buffer is moved into the iterator here).
        self.stream_stats
            .lock()
            .expect("stream_stats mutex poisoned")
            .merge_map_peak_entries = record_buffer.merge_map_peak_entries();

        // Chunk size: honor `hoodie.read.stream.batch_size` from the reader
        // config, falling back to DEFAULT_BATCH_SIZE when unset/unparseable.
        let batch_size = self.stream_batch_size();

        Ok(FileGroupMergeIterator::new_buffered(
            record_buffer,
            merge_schema,
            output_schema,
            output_converter,
            batch_size,
            self.stream_stats.clone(),
        ))
    }

    /// Resolve the streaming chunk size from `hoodie.read.stream.batch_size`
    /// on the reader config, defaulting to [`DEFAULT_BATCH_SIZE`] when the key
    /// is absent or unparseable.
    ///
    /// The key lives on `reader_context.hoodie_reader_config` (the same map the
    /// buffer loader reads `hoodie.datasource.merge.type` from). Mirrors Java's
    /// chunked `getClosableIterator` batch sizing.
    fn stream_batch_size(&self) -> usize {
        self.reader_context
            .hoodie_reader_config
            .get(crate::config::read::HudiReadConfig::StreamBatchSize.as_ref())
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_BATCH_SIZE)
    }

    /// [A3 / ENG-42992] Open the base file as a `RecordBatchReader` source.
    ///
    /// - `streaming=true`: returns a lazy [`ParquetSyncReader`] over the
    ///   parquet file. One row group per `RecordBatchReader::next` call.
    ///   Sync iteration uses `block_on(stream.next())` against
    ///   `OBJECT_STORE_RUNTIME` — caller MUST be in sync context.
    /// - `streaming=false` (or instant range present — see note): drains
    ///   the parquet stream into a `Vec<RecordBatch>` async, optionally
    ///   applies the instant range filter, and wraps the Vec in a
    ///   `RecordBatchIterator` (no `block_on` at iteration time → safe
    ///   to consume from async callers).
    ///
    /// Returns an **empty** `RecordBatchIterator` (no rows) when the
    /// input split has no base file (log-only file group).
    ///
    /// Mirrors Java's `HoodieFileGroupReader.makeBaseFileIterator()`,
    /// adapted for the Rust streaming/eager split.
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
            .get("hoodie.datasource.merge.type")
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

    async fn make_base_file_source(
        &mut self,
        streaming: bool,
    ) -> Result<Box<dyn arrow_array::RecordBatchReader + Send>> {
        let Some(path) = self.input_split.base_file_path.clone() else {
            // Log-only file group — empty base. Use the required_schema
            // as the reader's reported schema when available; otherwise
            // empty schema (the buffer's reader_schema fallback handles
            // schema selection downstream).
            let schema = self
                .schema_handler
                .required_schema
                .clone()
                .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));
            let empty = arrow_array::RecordBatchIterator::new(
                std::iter::empty::<std::result::Result<RecordBatch, arrow_schema::ArrowError>>(),
                schema,
            );
            return Ok(Box::new(empty));
        };

        if self.buffered_record_converter.is_none() {
            log::debug!(
                "[HoodieFileGroupReader] make_base_file_source: no bufferedRecordConverter set \
                 (batch-level read does not require per-record conversion)"
            );
        }

        // ENG-42276 / ENG-42866 — gate parquet RowFilter pushdown.
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
                    "[ENG-42866] MOR + non-PK predicate — skipping parquet \
                     RowFilter pushdown for base file '{path}' \
                     (post-merge filter still runs)"
                );
            }
            None
        };

        // No projection schema → fall back to the unprojected eager helper
        // (rare; FFI always supplies a required_schema). Streaming variant
        // of the unprojected helper is not exposed yet — eager Vec here.
        // The instant-range filter (if active) must still be applied on this
        // path — it gates base rows by `_hoodie_commit_time` regardless of
        // projection (the filter used to live in `init_record_iterators` and
        // ran on every base read; A3 moved it here, so all branches honor it).
        let Some(required_schema) = self.schema_handler.required_schema.clone() else {
            let batch = self
                .base_file_reader()?
                .read_data(&path, base_read_options(row_filter.clone()))
                .await
                .map_err(|e| {
                    CoreError::ReadFileSliceError(format!(
                        "Failed to read base file '{path}': {e:?}"
                    ))
                })?;
            let schema = batch.schema();
            let mut batches = vec![batch];
            if self.reader_context.instant_range.is_some() {
                let pre: usize = batches.iter().map(|b| b.num_rows()).sum();
                batches = self.apply_instant_range_filter(batches)?;
                let post: usize = batches.iter().map(|b| b.num_rows()).sum();
                log::debug!(
                    "[HoodieFileGroupReader] applyInstantRangeFilter (unprojected): {pre} → {post} rows"
                );
            }
            let iter = arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
            return Ok(Box::new(iter));
        };

        // Schema-evolution intersection (gold parity,
        // HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo; A2/A1):
        //   1. diff footer schema vs required by name;
        //   2. ask parquet only for the INTERSECTION (in the file's own types);
        //   3. project to required per batch: null-fill added columns, cast
        //      promotions (float→double string-mediated — C6).
        // Step 3 is applied PER ROW-GROUP so it works identically on the eager
        // (drain-then-project) and streaming (`ProjectingBatchReader`) paths —
        // A2 risk #3: every base batch the merge interleaves must already be in
        // `required_schema`.
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
        // Intersection by *case-insensitive* name (gold/Spark resolve field names
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
            "[base-file-evolution] path={} file_cols={} required_cols={} intersect_cols={} streaming={streaming}",
            path,
            file_schema.fields().len(),
            required_schema.fields().len(),
            present_len
        );

        // Position-based merge: append a synthetic row-index column carrying
        // each row's TRUE physical base-file position (via a parquet virtual
        // RowNumber column — correct even under RowFilter pushdown). It is kept
        // on the base source so the position buffer can match base rows to log
        // records, then stripped by the buffer before output. The column is NOT
        // added to `required_schema`/`merge_schema` — only to the base source's
        // physical schema (`base_read_schema` = required + row-index).
        let use_position = self.use_record_position();
        // Unused: position-based merging is not wired up here.
        let _row_number_col = use_position.then(|| ROW_INDEX_TEMPORARY_COLUMN_NAME.to_string());
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

        // Fallback to eager when the instant-range filter is active — the
        // filter currently needs a materialised Vec. A streaming per-batch
        // variant is a follow-up (ENG-42992 follow-up).
        let instant_range_active = self.reader_context.instant_range.is_some();
        // Streaming reads the base file one row group at a time, but the merge
        // loop is synchronous, so it has to block on the stream. That is only
        // safe off the async worker threads — `open_blocking_stream` is the one
        // caller that arranges it, and it is the only one that passes
        // `streaming = true`. The instant-range filter still forces eager: it
        // needs the batches materialized to filter them.
        let force_eager = !streaming || instant_range_active;

        if force_eager {
            // Async drain to the intersection, evolve to `required_schema`,
            // apply the instant-range filter, wrap the Vec. ENG-42276 — the
            // (CoW-gated) RowFilter is threaded through the intersection read so
            // row groups can be pruned via column-index stats (the builder
            // resolves predicate columns by name and returns None when any
            // referenced column is absent — safe even for evolved/added cols).
            // Upstream also threads a row-number column through this read, for
            // position-based merging. That is not wired up here, so it is
            // dropped rather than faked.
            let raw = self
                .base_file_reader()?
                .read_data(
                    &path,
                    base_read_options(row_filter.clone())
                        .with_projection(intersection.fields().iter().map(|f| f.name())),
                )
                .await
                .map_err(|e| {
                    CoreError::ReadFileSliceError(format!(
                        "Failed to read base file '{path}' with projection: {e:?}"
                    ))
                })?;
            let evolved =
                crate::schema::batch_evolution::project_batch_to_schema(&raw, &base_read_schema)?;
            let mut batches = vec![evolved];
            if instant_range_active {
                let pre: usize = batches.iter().map(|b| b.num_rows()).sum();
                batches = self.apply_instant_range_filter(batches)?;
                let post: usize = batches.iter().map(|b| b.num_rows()).sum();
                log::debug!("[HoodieFileGroupReader] applyInstantRangeFilter: {pre} → {post} rows");
            }
            let iter = arrow_array::RecordBatchIterator::new(
                batches.into_iter().map(Ok),
                base_read_schema,
            );
            Ok(Box::new(iter))
        } else {
            // Open the base file as a stream and adapt it back to the iterator
            // the merge loop wants. The whole file never lives in memory; one
            // row group does.
            let base_stream = self
                .base_file_reader()?
                .read_stream(
                    &path,
                    base_read_options(row_filter.clone())
                        .with_projection(intersection.fields().iter().map(|f| f.name())),
                )
                .await
                .map_err(|e| {
                    CoreError::ReadFileSliceError(format!(
                        "Failed to open base file stream '{path}': {e:?}"
                    ))
                })?;

            let handle = tokio::runtime::Handle::try_current().map_err(|_| {
                CoreError::Unsupported(
                    "A streaming base file read needs a tokio runtime to block on.".to_string(),
                )
            })?;
            let evolve_to = base_read_schema.clone();
            let evolved = futures::StreamExt::map(base_stream.into_stream(), move |b| match b {
                Ok(batch) => {
                    crate::schema::batch_evolution::project_batch_to_schema(&batch, &evolve_to)
                }
                Err(e) => Err(CoreError::from(e)),
            });

            Ok(Box::new(BlockingBatchReader {
                schema: base_read_schema,
                stream: futures::StreamExt::boxed(evolved),
                handle,
            }))
        }
    }

    // NOTE: apply_output_converter was removed in ENG-42991. The
    // FileGroupMergeIterator now owns the OutputConverter and applies it
    // per emitted chunk in its `Iterator::next()`. The reader's
    // `output_converter` field still exists for the lifetime up to
    // `open()`, which takes ownership and hands it to the iterator.

    /// Filter a base file's rows by the instant range, at the **file level**.
    ///
    /// A Hudi base file belongs to exactly one commit instant — encoded in its
    /// file name (`<fileId>_<writeToken>_<commit>.<ext>`) and surfaced as
    /// [`InputSplit::base_file_commit_time`]. So every row in the file shares that
    /// one instant, and the range test is a single per-file decision: keep the
    /// whole file or drop it.
    ///
    /// This mirrors the JVM gold. `HoodieFileGroupReader` only applies
    /// `applyInstantRangeFilter` when `getInstantRange().isPresent()` (empty on a
    /// plain snapshot); inflight / rolled-back *base files* are otherwise excluded
    /// at the file-slice level by `HoodieTableFileSystemView`, never by a per-row
    /// `_hoodie_commit_time` test. The range here (set by the gluten adapter for a
    /// native snapshot read: instants <= latest completed) exists to exclude base
    /// files from inflight / rolled-back commits; log-block exclusion is handled
    /// separately in the log path via `valid_block_instants`, not here.
    ///
    /// The previous implementation masked rows by the per-row `_hoodie_commit_time`
    /// *column*, which is a fragile proxy: **virtual-key** tables
    /// (`hoodie.populate.meta.fields=false`) persist a NULL `_hoodie_commit_time`,
    /// so every base row was masked out and the read silently returned 0 rows even
    /// though the file's own instant was in range (ENG-44975 / I-1).
    fn apply_instant_range_filter(&self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let instant_range = match &self.reader_context.instant_range {
            Some(range) => range,
            None => return Ok(batches),
        };

        // Skip filtering for metadata table (mirrors Java line 356).
        if crate::util::path::is_metadata_table_path(&self.reader_context.table_path) {
            return Ok(batches);
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
        if Self::base_file_in_instant_range(file_commit_time.as_deref(), instant_range, &timezone)?
        {
            Ok(batches)
        } else {
            log::debug!(
                "[HoodieFileGroupReader] applyInstantRangeFilter: base file commit {file_commit_time:?} \
                 outside instant range — excluding the whole base file"
            );
            // Whole base file excluded (inflight / rolled-back commit). The caller
            // wraps the result Vec in a RecordBatchIterator carrying an explicit
            // schema, so an empty Vec is a correct 0-row base source.
            Ok(Vec::new())
        }
    }

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
    /// the JVM gold's default of not row-filtering a base read when it cannot be
    /// bounded.
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

/// [A3 / ENG-42992] A `RecordBatchReader` adapter that evolves every batch
/// pulled from an inner reader to a fixed `target_schema` via
/// [`crate::schema::batch_evolution::project_batch_to_schema`] (null-fill added
/// columns, cast promotions).
///
/// This is the streaming counterpart of the eager path's per-batch
/// `project_batch_to_schema` call: the lazy base-file source reads the parquet
/// file projected to the file/required schema INTERSECTION (only the columns
/// physically present, in the file's own types), and this adapter reconciles
/// each row-group to the full `required_schema` as it streams through — so the
/// merge interleave always sees base batches in the reader schema (A2 risk #3),
/// without ever materialising the whole base file (R3).
///
/// `schema()` reports the `target_schema` (post-evolution) — the schema callers
/// will actually see on every emitted batch.
struct ProjectingBatchReader {
    inner: Box<dyn arrow_array::RecordBatchReader + Send>,
    target_schema: SchemaRef,
}

impl ProjectingBatchReader {
    fn new(
        inner: Box<dyn arrow_array::RecordBatchReader + Send>,
        target_schema: SchemaRef,
    ) -> Self {
        Self {
            inner,
            target_schema,
        }
    }
}

impl Iterator for ProjectingBatchReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next()? {
            Ok(batch) => Some(
                crate::schema::batch_evolution::project_batch_to_schema(
                    &batch,
                    &self.target_schema,
                )
                .map_err(|e| arrow_schema::ArrowError::ExternalError(Box::new(e))),
            ),
            Err(e) => Some(Err(e)),
        }
    }
}

impl arrow_array::RecordBatchReader for ProjectingBatchReader {
    fn schema(&self) -> SchemaRef {
        self.target_schema.clone()
    }
}

// =========================================================================
// Builder
// =========================================================================

/// Builder for `HoodieFileGroupReader`.
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

    /// ENG-42276 / ENG-42866 — install a parquet `RowFilter` builder.
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

    /// ENG-42866 — mark the pushed predicate as safe for MOR (i.e. it
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

/// Pulls an async base file stream from a synchronous caller, one batch per
/// `next()`.
///
/// The merge loop is synchronous, so a streaming base file has to be turned
/// back into an iterator somewhere. Doing that means blocking on the stream,
/// which is only legal off the async worker threads — so this must be driven
/// from a blocking-pool thread, which is what
/// [`FileGroupReader::open_blocking_stream`] arranges. Driving it from a worker
/// would deadlock.
struct BlockingBatchReader {
    schema: SchemaRef,
    stream: futures::stream::BoxStream<'static, Result<RecordBatch>>,
    handle: tokio::runtime::Handle,
}

impl Iterator for BlockingBatchReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        use futures::StreamExt;
        self.handle
            .block_on(self.stream.next())
            .map(|r| r.map_err(|e| arrow_schema::ArrowError::ExternalError(Box::new(e))))
    }
}

impl arrow_array::RecordBatchReader for BlockingBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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

    // ENG-44975 / I-1: the base-file instant-range decision is per-file (keyed on
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

        // No parseable base-file commit (log-only / unknown) → keep (gold default).
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

        // Drive make_base_file_source with the exact required schema under test,
        // bypassing prepare_required_schema's meta/key-field augmentation.
        reader.schema_handler.required_schema = Some(required);
        reader
    }

    /// Drain a base-file source reader into one concatenated `RecordBatch`,
    /// asserting it reports the requested schema. Shared by the eager + streaming
    /// schema-evolution tests.
    fn drain_base_source(source: Box<dyn arrow_array::RecordBatchReader + Send>) -> RecordBatch {
        use arrow_array::RecordBatchReader as _;
        let schema = source.schema();
        let batches: Vec<RecordBatch> = source.map(|r| r.unwrap()).collect();
        if batches.is_empty() {
            RecordBatch::new_empty(schema)
        } else {
            arrow::compute::concat_batches(&schema, &batches).unwrap()
        }
    }

    /// Base file written at s1 {meta..., id:int, price:float}; required schema at
    /// s2 {id:long, price:double, tag:string?}: missing column null-filled, int
    /// widened, float→double value-exact. Mirrors gold HoodieParquetFileFormatHelper.
    ///
    /// A3 (ENG-42992): runs against BOTH base-file source modes —
    /// `streaming=false` (eager drain + per-batch evolve) and `streaming=true`
    /// (lazy `ParquetSyncReader` + `ProjectingBatchReader` per row-group) — to
    /// prove the streaming path enforces the same schema evolution as the eager
    /// path. The two outputs must be byte-identical.
    ///
    /// This is a plain `#[test]` (NOT `#[tokio::test]`): the streaming source is
    /// a `ParquetSyncReader` that does `block_on(stream.next())` per row-group,
    /// which panics if driven from inside an async runtime. Async setup runs on
    /// `OBJECT_STORE_RUNTIME` then we drain from this sync context — mirroring
    /// the FFI driver's `open()` → sync `get_next` call shape.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_make_base_file_source_schema_on_write_evolution() {
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

        // Open both base-file sources via async setup on OBJECT_STORE_RUNTIME,
        // Both sources read eagerly here, so there is no sync driver to return
        // to — `streaming=true` takes the same path as `streaming=false` until
        // the merge iterator is async-native.
        let req = required.clone();
        let dir = tmp.path().to_path_buf();
        let (eager_src, stream_src) = async {
            let mut reader =
                test_file_group_reader_for_base_file(&dir, base_name, req.clone()).await;
            let eager_src = reader.make_base_file_source(false).await.unwrap();
            let mut reader2 =
                test_file_group_reader_for_base_file(&dir, base_name, req.clone()).await;
            let stream_src = reader2.make_base_file_source(true).await.unwrap();
            (eager_src, stream_src)
        }
        .await;

        // Eager path (streaming=false): async drain + per-batch evolve.
        let eager_out = drain_base_source(eager_src);
        assert_evolved(&eager_out);

        // The streaming source blocks on its base stream, so it has to be
        // drained off the worker threads — the same contract every real caller
        // honors via `open_blocking_stream`.
        let stream_out = tokio::task::spawn_blocking(move || drain_base_source(stream_src))
            .await
            .unwrap();
        assert_evolved(&stream_out);
        assert_eq!(
            eager_out, stream_out,
            "streaming base source must produce byte-identical output to the eager path"
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
        let source = reader.make_base_file_source(false).await.unwrap();
        let out = drain_base_source(source);
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
    // ENG-42866 — builder routes `with_row_filter_builder` and
    // `with_mor_pk_safe` onto the shared `reader_context` so both the base
    // parquet read site (this file's `make_base_file_source`) and the
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

    // GAP-02 — bootstrap base files are rejected loudly at reader construction.
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

    // GAP-07 — schema-on-read (InternalSchema) is rejected loudly at reader
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
        // ENG-45059: composite virtual keys (virtual keys + a multi-field record key)
        // are now supported — `RecordContext::record_key_array` reconstructs the full
        // `field:val,field:val` merge key per row on both sides, so construction must
        // succeed (it previously errored `CoreError::Unsupported`).
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
        // construction no longer rejects it.
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

    /// The streaming path must return exactly what the eager one does. It reads
    /// the base file a row group at a time instead of whole, which is a memory
    /// difference, not a data one — so any divergence here is a bug rather than
    /// a tradeoff.
    ///
    /// Multi-threaded flavor on purpose: the merge loop blocks on the base
    /// stream from a blocking-pool thread, which needs worker threads still
    /// available to drive it.
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
        let mut stream = stream_reader.open_blocking_stream().await.unwrap();
        let mut streamed_rows = 0usize;
        let mut streamed_batches = 0usize;
        while let Some(b) = stream.next().await {
            streamed_rows += b.unwrap().num_rows();
            streamed_batches += 1;
        }

        assert_eq!(streamed_rows, eager.num_rows());
        assert!(
            streamed_batches > 0,
            "the stream yielded nothing; it should emit at least one batch"
        );
    }
}
