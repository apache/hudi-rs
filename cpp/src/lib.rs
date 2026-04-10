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

use crate::context::FileGroupReaderContext;
use crate::util::{create_raw_pointer_for_record_batches, free_arrow_stream};
use hudi::avro_to_arrow::to_arrow_schema;
use hudi::config::HudiConfigs;
use hudi::config::table::HudiTableConfig;
use hudi::config::util::split_hudi_options_from_others;
use hudi::file_group::reader::HoodieFileGroupReader as CoreFileGroupReader;
use hudi::file_group::reader::input_split::InputSplit;
use hudi::file_group::reader::reader_context::ReaderContext;
use hudi::file_group::reader::reader_parameters::ReaderParameters;
use hudi::file_group::reader::record_context::RecordContext;
use hudi::file_group::reader::schema_handler::FileGroupReaderSchemaHandler;
use hudi::storage::Storage;
use hudi::timeline::selector::InstantRange;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

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
    let schema_handler = {
        let mut handler = FileGroupReaderSchemaHandler::new();
        if let Some(hs) = fgrc.data_schema.as_ref() {
            if let Ok(arrow_schema) = avro_json_to_arrow_schema(&hs.avro_schema_json) {
                handler = handler.with_data_schema(Arc::new(arrow_schema));
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

    Ok(Box::new(HoodieFileGroupReader {
        reader_context: core_reader_context.clone(),
        storage,
        props,
        reader_parameters,
        input_split,
        partition_path_fields,
        rt,
    }))
}

impl HoodieFileGroupReader {
    /// Uses the core `HoodieFileGroupReader` for the full 3-phase merge.
    pub fn get_closable_iterator(
        &self,
    ) -> std::result::Result<*mut ffi::ArrowArrayStream, String> {
        log::debug!(
            "get_closable_iterator: partition={} base_file={:?} log_files={} \
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
        let schema = record_batch.schema();

        log::debug!(
            "get_closable_iterator: merge complete, {} rows, {} cols",
            record_batch.num_rows(),
            record_batch.num_columns(),
        );

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
