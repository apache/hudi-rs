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
pub mod context;
mod util;

/// Re-export core types for integration tests and downstream consumers.
pub use hudi_dep as hudi_core;

use crate::context::FileGroupReaderContext;
use crate::util::{create_raw_pointer_for_record_batches, free_arrow_stream};
use hudi_dep::avro_to_arrow::to_arrow_schema;
use hudi_dep::config::HudiConfigs;
use hudi_dep::config::table::HudiTableConfig;
use hudi_dep::config::util::split_hudi_options_from_others;
use hudi_dep::file_group::reader::HoodieFileGroupReader as CoreFileGroupReader;
use hudi_dep::file_group::reader::input_split::InputSplit;
use hudi_dep::file_group::reader::reader_context::ReaderContext;
use hudi_dep::file_group::reader::reader_parameters::ReaderParameters;
use hudi_dep::file_group::reader::record_context::RecordContext;
use hudi_dep::file_group::reader::schema_handler::FileGroupReaderSchemaHandler;
use hudi_dep::storage::Storage;
use hudi_dep::timeline::selector::InstantRange;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

mod predicate;
use predicate::ColumnPredicate;

static LOGGER: OnceLock<()> = OnceLock::new();

/// Initialize env_logger exactly once for the lifetime of the loaded shared library.
fn init_logger() {
    LOGGER.get_or_init(|| {
        match std::env::var("RUST_LOG") {
            Ok(_) => {}
            Err(_) => unsafe { std::env::set_var("RUST_LOG", "debug") },
        }
        let _ = env_logger::try_init();
    });
}


#[cxx::bridge]
mod ffi {
    // ════════════════════════════════════════════════════════════════════════
    // ENG-40156 — predicate pushdown IR
    // ════════════════════════════════════════════════════════════════════════
    //
    // Velox HudiSplitReader walks scanSpec_ (and optionally remainingFilter_)
    // and produces one FfiColumnFilter per filtered column.  hudi-rs decodes
    // these into arrow::compute predicates and applies them either at parquet
    // read time (with_row_filter, for RO / no-log-files paths — Phase 4) or
    // post-merge against the final RecordBatch (Phase 3 — semantically safe
    // for all MOR cases because the merge has already resolved log updates).
    //
    // The IR is intentionally narrow: tagged union of the Velox common::Filter
    // shapes we actually see today (range, value-list, null, bool). Compound
    // expressions (OR across columns, function calls in remainingFilter) are
    // NOT modelled here — those continue to be handled by Velox's existing
    // post-scan filter evaluator. See the design doc for the rationale.
    //
    // cxx-rs doesn't support sum types directly so we encode as a "kind" enum
    // string plus parallel value vectors; each FfiColumnFilter sets the fields
    // its kind needs and leaves the rest at default.

    /// Kind of column-level predicate. String-encoded because cxx-rs doesn't
    /// pass Rust enums by value across the bridge.
    ///
    /// Each kind is interpreted as follows (all optional null-allowed unless
    /// stated). Numeric values are encoded in i64_lo/i64_hi/f64_lo/f64_hi as
    /// inclusive lower/upper bounds; string values use bytes_values.
    ///
    /// | kind              | fields                                          |
    /// |-------------------|-------------------------------------------------|
    /// | "is_null"         | (none)                                          |
    /// | "is_not_null"     | (none)                                          |
    /// | "bool"            | bool_value                                      |
    /// | "bigint_range"    | i64_lo, i64_hi, lo_unbounded, hi_unbounded      |
    /// | "double_range"    | f64_lo, f64_hi, lo_unbounded, hi_unbounded      |
    /// | "bigint_values"   | i64_values (sorted, deduped)                    |
    /// | "bytes_range"     | bytes_lo, bytes_hi, lo_unbounded, hi_unbounded  |
    /// | "bytes_values"    | bytes_values                                    |
    ///
    /// `null_allowed` applies to every kind and indicates whether NULL values
    /// in the column should pass the predicate. Mirrors Velox's
    /// Filter::testNull(). Default false (NULL fails the predicate).

    #[derive(Default, Clone)]
    struct FfiColumnFilter {
        /// Column name in the file group reader's output schema (matches the
        /// `requested_schema` field order, NOT necessarily the parquet leaf
        /// order — Phase 4's parquet pushdown maps by name).
        column: String,

        /// One of the kinds in the table above; see comment for field
        /// semantics per kind.
        kind: String,

        /// True iff NULL in the column should satisfy this predicate.
        null_allowed: bool,

        // ── range/values payload (only the fields for the chosen kind are
        //    populated; the rest stay at default zero/empty)
        i64_lo: i64,
        i64_hi: i64,
        f64_lo: f64,
        f64_hi: f64,
        lo_unbounded: bool,
        hi_unbounded: bool,
        bool_value: bool,
        i64_values: Vec<i64>,

        // cxx-rs forbids nested generics (Vec<Vec<u8>>), so bytes-typed values
        // are passed as Vec<String>. Arrow string columns are UTF-8 so this is
        // a natural fit. Truly binary columns are not supported by this IR —
        // they fall through to Velox's existing post-scan filter evaluator.
        bytes_lo: String,
        bytes_hi: String,
        bytes_values: Vec<String>,
    }

    /// Mirrors `HudiReadOptions.LogFile` proto (CXX-safe, used in `Vec`).
    #[derive(Default)]
    struct FfiLogFile {
        path_str: String,
        file_id: String,
        delta_commit_time: String,
        log_version: i32,
        log_write_token: String,
        file_extension: String,
        suffix: String,
        file_size: i64,
    }

    /// FFI-safe flat representation of `HudiReadOptions.HoodieFileGroupReaderContext`.
    ///
    /// Contains everything the file group reader needs:
    ///   - Table path (used as `hoodie.base.path` for Storage)
    ///   - Partition, commit time, reader flags
    ///   - Config maps (table_config, props, hoodie_reader_config)
    ///   - Base file / log file details
    ///   - Schemas, merge mode, instant range
    ///   - Split-level transport fields (base_file_name, log_file_names)
    #[derive(Default)]
    struct FfiReaderContext {
        // ── outer primitives (HoodieFileGroupReaderContext fields 1–9) ──
        table_path: String,
        partition_path: String,
        latest_commit_time: String,
        start: i64,
        length: i64,
        should_use_record_position: bool,
        allow_inflight_instants: bool,
        emit_delete: bool,
        sort_output: bool,

        // ── props (proto field 10) ───────────────────────────────────────
        props_keys: Vec<String>,
        props_values: Vec<String>,

        // ── BaseFile (proto field 11, flattened) ─────────────────────────
        has_base_file: bool,
        base_file_path: String,
        base_file_file_name: String,
        base_file_file_size: i64,
        base_file_file_id: String,
        base_file_commit_time: String,
        base_file_has_bootstrap: bool,
        base_file_bootstrap_path: String,
        base_file_bootstrap_file_name: String,
        base_file_bootstrap_file_size: i64,
        base_file_bootstrap_file_id: String,
        base_file_bootstrap_commit_time: String,

        // ── LogFile[] (proto field 12) ────────────────────────────────────
        log_file_details: Vec<FfiLogFile>,

        // ── HoodieSchema (proto fields 13–14, inlined) ───────────────────
        data_schema_json: String,
        requested_schema_json: String,

        // ── ReaderContext (proto field 15, flattened) ─────────────────────
        base_file_format: String,
        has_log_files: bool,
        has_bootstrap_base_file: bool,
        needs_bootstrap_merge: bool,
        should_merge_use_record_position: bool,
        enable_logical_timestamp_field_repair: bool,
        iterator_mode: String,
        merge_mode: String,
        merge_strategy_id: String,
        has_instant_range: bool,
        instant_range_start: String,
        instant_range_end: String,
        instant_range_type: String,
        table_config_keys: Vec<String>,
        table_config_values: Vec<String>,
        hoodie_reader_config_keys: Vec<String>,
        hoodie_reader_config_values: Vec<String>,

        // ── file-slice split fields (not in proto — transport only) ──────
        base_file_name: String,
        log_file_names: Vec<String>,

        // ── predicate pushdown (ENG-40156, Phases 1-4) ────────────────────
        // Column-level filters extracted from Velox's scanSpec_. Empty when
        // the caller didn't push any predicates. See FfiColumnFilter comment
        // above for the encoding.
        column_filters: Vec<FfiColumnFilter>,
    }

    unsafe extern "C++" {
        include!("arrow/c/abi.h");

        type ArrowArrayStream;
    }

    extern "Rust" {
        type HoodieFileGroupReader;

        /// Create a file group reader from the full context.
        /// `table_path` inside `ctx` is used as the storage base URI.
        fn new_file_group_reader_with_context(
            ctx: FfiReaderContext,
        ) -> Result<Box<HoodieFileGroupReader>>;

        /// Read the file group and return merged results as an ArrowArrayStream.
        fn get_closable_iterator(
            self: &HoodieFileGroupReader,
        ) -> Result<*mut ArrowArrayStream>;

        /// Free an ArrowArrayStream that was returned by `get_closable_iterator`.
        ///
        /// This drops the stream (invoking the Arrow release callback to free
        /// internal buffers) and deallocates the struct itself through Rust's
        /// allocator.  The caller must not use `ptr` after this call.
        unsafe fn hudi_free_arrow_stream(ptr: *mut ArrowArrayStream);
    }
}

pub struct HoodieFileGroupReader {
    // ── 1:1 with Java HoodieFileGroupReader fields ─────────────────
    reader_context: Arc<ReaderContext>,
    storage: Arc<Storage>,
    props: HashMap<String, String>,
    reader_parameters: ReaderParameters,
    input_split: InputSplit,
    partition_path_fields: Option<Vec<String>>,

    // ── ENG-40156 predicate pushdown (Phases 3-4) ────────────────────
    // Column-level filters from Velox's scanSpec_. Always non-empty when
    // FfiReaderContext carried any; the constructor decodes them once.
    //
    // Two evaluation sites depending on file-group shape:
    //  - Phase 4 (Storage::get_parquet_file_data_projected): if no log
    //    files exist for this file group, push to parquet's with_row_filter
    //    so we can skip pages and decode work.
    //  - Phase 3 (read_record_batch, post-merge): always apply on the final
    //    merged RecordBatch — safe regardless of log presence because the
    //    merge has already resolved updates.
    column_predicates: Vec<ColumnPredicate>,

    // ── Rust-only ──────────────────────────────────────────────────
    rt: tokio::runtime::Runtime,
}

/// Creates a `HoodieFileGroupReader` from a full `FfiReaderContext`.
pub fn new_file_group_reader_with_context(
    ctx: ffi::FfiReaderContext,
) -> std::result::Result<Box<HoodieFileGroupReader>, String> {
    init_logger();

    // Capture transport-only file names before ctx is consumed by .into().
    let base_file_name = ctx.base_file_name.clone();
    let log_file_names: Vec<String> = ctx.log_file_names.iter().cloned().collect();
    // ENG-40156 — snapshot column_filters before `ctx.into()` consumes ctx.
    let ctx_column_filters: Vec<ffi::FfiColumnFilter> = ctx.column_filters.clone();

    // Convert flat FFI struct → nested Rust types (intermediate, not stored).
    let fgrc: FileGroupReaderContext = ctx.into();

    log::debug!(
        "new_file_group_reader_with_context: \
         table_path={table_path} partition_path={partition_path} \
         latest_commit_time={latest_commit_time} start={start} length={length} \
         should_use_record_position={surp} allow_inflight_instants={aii} \
         emit_delete={ed} sort_output={so} \
         props_count={props_count} base_file={base_file} log_files_count={log_files_count} \
         has_data_schema={has_data_schema} has_requested_schema={has_req_schema}",
        table_path = fgrc.table_path,
        partition_path = fgrc.partition_path,
        latest_commit_time = fgrc.latest_commit_time,
        start = fgrc.start,
        length = fgrc.length,
        surp = fgrc.should_use_record_position,
        aii = fgrc.allow_inflight_instants,
        ed = fgrc.emit_delete,
        so = fgrc.sort_output,
        props_count = fgrc.props.len(),
        base_file = fgrc.base_file.as_ref().map(|bf| bf.file_name.as_str()).unwrap_or("<none>"),
        log_files_count = fgrc.log_files.len(),
        has_data_schema = fgrc.data_schema.is_some(),
        has_req_schema = fgrc.requested_schema.is_some(),
    );
    {
        let rc = &fgrc.reader_context;
        log::debug!(
            "new_file_group_reader_with_context: reader_context \
             table_path={table_path} latest_commit_time={latest_commit_time} \
             base_file_format={base_file_format} has_log_files={has_log_files} \
             needs_bootstrap_merge={nbm} should_merge_use_record_position={smurp} \
             iterator_mode={iterator_mode} merge_mode={merge_mode} \
             merge_strategy_id={merge_strategy_id} has_instant_range={has_instant_range} \
             table_config_count={table_config_count} hoodie_reader_config_count={hrc_count}",
            table_path = rc.table_path,
            latest_commit_time = rc.latest_commit_time,
            base_file_format = rc.base_file_format,
            has_log_files = rc.has_log_files,
            nbm = rc.needs_bootstrap_merge,
            smurp = rc.should_merge_use_record_position,
            iterator_mode = rc.iterator_mode,
            merge_mode = rc.merge_mode,
            merge_strategy_id = rc.merge_strategy_id,
            has_instant_range = rc.instant_range.is_some(),
            table_config_count = rc.table_config.len(),
            hrc_count = rc.hoodie_reader_config.len(),
        );
    }
    log::debug!(
        "new_file_group_reader_with_context: split base_file_name={base_file_name} \
         log_file_names={log_file_names:?}",
    );

    // ── 1. Build merged props ───────────────────────────────────────
    // Order: hoodie.base.path + table_config < props < hoodie_reader_config
    let mut options: Vec<(String, String)> = Vec::new();
    options.push((
        HudiTableConfig::BasePath.as_ref().to_string(),
        fgrc.table_path.clone(),
    ));
    for (k, v) in &fgrc.reader_context.table_config {
        options.push((k.clone(), v.clone()));
    }
    for (k, v) in &fgrc.props {
        options.push((k.clone(), v.clone()));
    }
    for (k, v) in &fgrc.reader_context.hoodie_reader_config {
        options.push((k.clone(), v.clone()));
    }

    // ── 2. Create Storage (needs temporary HudiConfigs) ─────────────
    let (hudi_opts, storage_opts) = split_hudi_options_from_others(options);
    let props: HashMap<String, String> = hudi_opts;
    let hudi_configs = Arc::new(HudiConfigs::new(props.clone()));
    let storage = Storage::new(Arc::new(storage_opts), hudi_configs)
        .map_err(|e| format!("Failed to create Storage: {e}"))?;

    // ── 3. Build ReaderParameters ───────────────────────────────────
    let reader_parameters = ReaderParameters {
        use_record_position: fgrc.should_use_record_position,
        emit_delete: fgrc.emit_delete,
        sort_output: fgrc.sort_output,
        allow_inflight_instants: fgrc.allow_inflight_instants,
    };

    // ── 4. Build InputSplit ─────────────────────────────────────────
    let base_file_path = if base_file_name.is_empty() {
        None
    } else if fgrc.partition_path.is_empty() {
        Some(base_file_name)
    } else {
        Some(format!("{}/{}", fgrc.partition_path, base_file_name))
    };
    let log_file_paths: Vec<String> = log_file_names
        .into_iter()
        .map(|name| {
            if fgrc.partition_path.is_empty() {
                name
            } else {
                format!("{}/{}", fgrc.partition_path, name)
            }
        })
        .collect();
    let input_split = InputSplit::new(
        base_file_path,
        None,
        log_file_paths,
        fgrc.partition_path,
    );

    // ── 5. Convert FFI ReaderContext → core ReaderContext ────────────
    let ffi_rc = fgrc.reader_context;
    let instant_range = ffi_rc.instant_range.map(|ir| {
        let timezone = ffi_rc.table_config
            .get(HudiTableConfig::TimelineTimezone.as_ref())
            .cloned()
            .unwrap_or_else(|| "utc".to_string());
        let (start_inclusive, end_inclusive) = match ir.range_type.as_str() {
            "CLOSED_CLOSED" => (true, true),
            "OPEN_CLOSED" => (false, true),
            "CLOSED_OPEN" => (true, false),
            _ => (false, true), // default
        };
        let start = if ir.start_instant.is_empty() { None } else { Some(ir.start_instant) };
        let end = if ir.end_instant.is_empty() { None } else { Some(ir.end_instant) };
        InstantRange::new(timezone, start, end, start_inclusive, end_inclusive)
    });
    // RecordContext is constructed from table_config, matching Java's
    // RecordContext(tableConfig, typeConverter) pattern. The table_config
    // carries hoodie.populate.meta.fields, hoodie.table.precombine.field,
    // and hoodie.table.recordkey.fields — RecordContext derives everything
    // from these.
    let partition_path = input_split.partition_path.clone();
    let record_context = RecordContext::new(&ffi_rc.table_config, partition_path);

    // ── Extract partition path fields from table config ─────────────
    // Mirrors Java's: tableConfig.getPartitionFields() which reads
    // "hoodie.table.partition.fields", splits on ",", and strips
    // custom key-generator partition type suffixes (split on ":").
    // e.g. "date:TIMESTAMP,region:SIMPLE" → ["date", "region"]
    let partition_path_fields: Option<Vec<String>> = ffi_rc
        .table_config
        .get("hoodie.table.partition.fields")
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().split(':').next().unwrap_or("").to_string())
                .filter(|s| !s.is_empty())
                .collect()
        });

    // ── 6. Build schema handler from Avro schemas passed via FFI ──
    // Set on ReaderContext to match Java's HoodieReaderContext.schemaHandler.
    //
    // The `data_schema` from the Scala planning layer is the pruned table
    // data schema (full table columns minus "op" and partition columns).
    // This serves as both `table_schema` (field lookup source for mandatory
    // fields) and `data_schema` (base file reading schema).
    let schema_handler = {
        let mut handler = FileGroupReaderSchemaHandler::new();
        if let Some(hs) = fgrc.data_schema.as_ref() {
            if let Ok(arrow_schema) = avro_json_to_arrow_schema(&hs.avro_schema_json) {
                let schema_ref = Arc::new(arrow_schema);
                handler = handler
                    .with_table_schema(schema_ref.clone())
                    .with_data_schema(schema_ref);
            }
        }
        if let Some(hs) = fgrc.requested_schema.as_ref() {
            if let Ok(arrow_schema) = avro_json_to_arrow_schema(&hs.avro_schema_json) {
                handler = handler.with_requested_schema(Arc::new(arrow_schema));
            }
        }
        handler
    };

    let core_reader_context = Arc::new(ReaderContext {
        table_path: ffi_rc.table_path,
        latest_commit_time: ffi_rc.latest_commit_time,
        base_file_format: ffi_rc.base_file_format,
        has_log_files: ffi_rc.has_log_files,
        has_bootstrap_base_file: ffi_rc.has_bootstrap_base_file,
        needs_bootstrap_merge: ffi_rc.needs_bootstrap_merge,
        should_merge_use_record_position: ffi_rc.should_merge_use_record_position,
        enable_logical_timestamp_field_repair: ffi_rc.enable_logical_timestamp_field_repair,
        iterator_mode: ffi_rc.iterator_mode,
        merge_mode: ffi_rc.merge_mode,
        merge_strategy_id: ffi_rc.merge_strategy_id,
        instant_range,
        record_context,
        schema_handler,
        table_config: ffi_rc.table_config,
        hoodie_reader_config: ffi_rc.hoodie_reader_config,
    });

    // ── 7. Build tokio runtime ──────────────────────────────────────
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create tokio runtime: {e}"))?;

    // ── 8. Decode ENG-40156 column predicates from FFI IR ────────────
    // Predicates whose kind isn't recognised are logged + dropped here;
    // Velox's post-scan filter still evaluates the original expression
    // so there is no correctness regression.
    let column_predicates: Vec<ColumnPredicate> = ctx_column_filters
        .iter()
        .filter_map(ColumnPredicate::from_ffi)
        .collect();
    if !column_predicates.is_empty() {
        log::info!(
            "[ENG-40156] decoded {} column predicate(s): {:?}",
            column_predicates.len(),
            column_predicates.iter().map(|p| p.column()).collect::<Vec<_>>()
        );
    }

    Ok(Box::new(HoodieFileGroupReader {
        reader_context: core_reader_context.clone(),
        storage,
        props,
        reader_parameters,
        input_split,
        partition_path_fields,
        column_predicates,
        rt,
    }))
}

impl HoodieFileGroupReader {
    /// Construct a `HoodieFileGroupReader` directly (for testing without FFI).
    pub fn new(
        reader_context: Arc<ReaderContext>,
        storage: Arc<Storage>,
        props: HashMap<String, String>,
        reader_parameters: ReaderParameters,
        input_split: InputSplit,
        partition_path_fields: Option<Vec<String>>,
    ) -> std::result::Result<Self, String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to create tokio runtime: {e}"))?;
        Ok(Self {
            reader_context,
            storage,
            props,
            reader_parameters,
            input_split,
            partition_path_fields,
            column_predicates: Vec::new(),
            rt,
        })
    }

    /// Runs the full 3-phase merge and returns the resulting `RecordBatch` and its schema.
    pub fn read_record_batch(
        &self,
    ) -> std::result::Result<(arrow_array::RecordBatch, arrow_schema::SchemaRef), String> {
        log::debug!(
            "read_record_batch: partition={} base_file={:?} log_files={} \
             latest_instant_time={} ordering_fields={:?} merge_mode={}",
            self.input_split.partition_path,
            self.input_split.base_file_path,
            self.input_split.log_file_paths.len(),
            self.reader_context.latest_commit_time,
            self.reader_context.ordering_field_names(),
            self.reader_context.merge_mode.as_str(),
        );

        let mut reader = CoreFileGroupReader::builder()
            .with_reader_context(self.reader_context.clone())
            .with_storage(self.storage.clone())
            .with_input_split(self.input_split.clone())
            .with_reader_parameters(self.reader_parameters.clone())
            .build()
            .map_err(|e| format!("Failed to build file group reader: {e}"))?;

        let record_batch = self
            .rt
            .block_on(reader.read())
            .map_err(|e| format!("Failed to read file group: {e}"))?;

        log::debug!(
            "read_record_batch: merge complete, {} rows, {} cols",
            record_batch.num_rows(),
            record_batch.num_columns(),
        );

        // ── ENG-40156 Phase 3 — post-merge predicate evaluation ──────
        // Safe for all MOR shapes: the merge has already resolved log
        // updates so predicate values reflect the snapshot the caller
        // would see. Phase 4 separately pushes the same predicates into
        // the parquet reader for the RO / no-log-files paths; this filter
        // is still applied here, but only against the already-narrowed
        // input, so it's cheap.
        let pre_rows = record_batch.num_rows();
        let record_batch = if self.column_predicates.is_empty() {
            record_batch
        } else {
            let filtered = predicate::filter_batch(&record_batch, &self.column_predicates)
                .map_err(|e| {
                    format!(
                        "[ENG-40156] post-merge filter failed: {e}; \
                         predicates={:?}",
                        self.column_predicates
                    )
                })?;
            log::info!(
                "[ENG-40156] post-merge filter: {} -> {} rows ({} predicates)",
                pre_rows,
                filtered.num_rows(),
                self.column_predicates.len()
            );
            filtered
        };
        let schema = record_batch.schema();

        Ok((record_batch, schema))
    }

    /// Uses the core `HoodieFileGroupReader` for the full 3-phase merge.
    pub fn get_closable_iterator(
        &self,
    ) -> std::result::Result<*mut ffi::ArrowArrayStream, String> {
        let (record_batch, schema) = self.read_record_batch()?;
        Ok(create_raw_pointer_for_record_batches(
            vec![record_batch],
            schema,
        ))
    }
}

/// Free an `ArrowArrayStream` returned by [`HoodieFileGroupReader::get_closable_iterator`].
///
/// # Safety
/// `ptr` must have been returned by `get_closable_iterator` and must not be
/// used after this call.
unsafe fn hudi_free_arrow_stream(ptr: *mut ffi::ArrowArrayStream) {
    unsafe { free_arrow_stream(ptr) };
}

/// Convert an Avro schema JSON string to an Arrow Schema.
fn avro_json_to_arrow_schema(
    avro_json: &str,
) -> std::result::Result<arrow_schema::Schema, String> {
    let sanitized = avro_json.trim().replace("\\:", ":");
    let avro_schema = apache_avro::Schema::parse_str(&sanitized)
        .map_err(|e| format!("Failed to parse Avro schema: {e}"))?;
    to_arrow_schema(&avro_schema).map_err(|e| format!("Failed to convert Avro→Arrow: {e}"))
}
