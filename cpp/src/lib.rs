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
use crate::util::create_raw_pointer_for_record_batches;
use cxx::{CxxString, CxxVector};
use hudi::config::HudiConfigs;
use hudi::config::util::split_hudi_options_from_others;
use hudi::file_group::FileGroup;
use hudi::file_group::file_slice::FileSlice;
use hudi::file_group::reader::HoodieFileGroupReader;
use hudi::file_group::reader::input_split::InputSplit;
use hudi::file_group::reader::reader_parameters::ReaderParameters;
use hudi::storage::Storage;
use hudi::table::builder::OptionResolver;
use std::sync::Arc;
use std::sync::OnceLock;

static LOGGER: OnceLock<()> = OnceLock::new();

/// Initialize env_logger exactly once for the lifetime of the loaded shared library.
///
/// Defaults to debug-level logging for the entire repo. If RUST_LOG is already
/// set, it is respected as-is.
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
    // ── CXX shared structs ─────────────────────────────────────────────
    //
    // These are flat/flattened for CXX safety.  The proper Rust hierarchy
    // lives in `context.rs` — use `FileGroupReaderContext::from(ffi)` to convert.

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

    /// FFI-safe flat representation of `HudiReadOptions.HoodieFileGroupReaderContext`
    /// proto, carried across the CXX bridge in a single call.
    ///
    /// All nested messages are flattened for CXX compatibility:
    ///   - **Outer primitives** (proto fields 1–9): `table_path`, `partition_path`,
    ///     `latest_commit_time`, `start`, `length`, `should_use_record_position`, …
    ///   - **props** (proto field 10): parallel `props_keys`/`props_values` vecs
    ///   - **BaseFile** (proto field 11): flattened with `base_file_*` prefix
    ///   - **LogFile[]** (proto field 12): `Vec<FfiLogFile>`
    ///   - **HoodieSchema** (proto fields 13–14): inlined as `*_json` strings
    ///   - **ReaderContext** (proto field 15): flattened; maps use parallel key/value vecs
    ///   - **Extra**: file-slice split fields (not in proto — transport only)
    ///
    /// Use `FileGroupReaderContext::from(ctx)` to reconstruct the proto hierarchy.
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
        // bootstrap sub-file (proto BaseFile.bootstrap_base_file, flattened)
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
        // InstantRange (proto field 15.12, flattened)
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
        type HudiFileGroupReader;

        type HudiFileSlice;
        fn new_file_slice_from_file_names(
            partition_path: &CxxString,
            base_file_name: &CxxString,
            log_file_names: &CxxVector<CxxString>,
        ) -> Result<Box<HudiFileSlice>>;

        // ── context-based APIs (mimicking Hudi OSS builder pattern) ─────
        //
        // Usage from C++:
        //   auto reader = new_file_group_reader_with_context(baseUri, ctx);
        //   auto slice  = new_file_slice_from_context(*reader);
        //   auto stream = reader->get_closable_iterator(*slice);
        //
        fn new_file_group_reader_with_context(
            base_uri: &CxxString,
            ctx: FfiReaderContext,
        ) -> Result<Box<HudiFileGroupReader>>;

        fn new_file_slice_from_context(
            reader: &HudiFileGroupReader,
        ) -> Result<Box<HudiFileSlice>>;

        fn get_closable_iterator(
            self: &HudiFileGroupReader,
            file_slice: &HudiFileSlice,
        ) -> Result<*mut ArrowArrayStream>;
    }
}

pub struct HudiFileGroupReader {
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
    rt: tokio::runtime::Runtime,
    /// Full file-group reader context converted from FfiReaderContext.
    fgrc: FileGroupReaderContext,
    /// File-slice split fields (not in proto) needed to build the FileSlice.
    split_ctx: SplitContext,
}

/// File-slice fields that come from the split (not from the proto).
struct SplitContext {
    base_file_name: String,
    log_file_names: Vec<String>,
}

// ── context-based APIs ──────────────────────────────────────────────────────

/// Creates a `HudiFileGroupReader` from a base URI and a full `FfiReaderContext`.
///
/// Builds `HudiConfigs` and `Storage` directly from the context's config maps,
/// then uses the Java-style `HoodieFileGroupReader` for all reads.
pub fn new_file_group_reader_with_context(
    base_uri: &CxxString,
    ctx: ffi::FfiReaderContext,
) -> std::result::Result<Box<HudiFileGroupReader>, String> {
    init_logger();

    let base_uri = base_uri
        .to_str()
        .map_err(|e| format!("Failed to convert CxxString to str: {e}"))?;

    // Stash the split-level fields before converting to FileGroupReaderContext
    // (these are transport-only and not part of the proto hierarchy).
    let split_ctx = SplitContext {
        base_file_name: ctx.base_file_name.clone(),
        log_file_names: ctx.log_file_names.iter().cloned().collect(),
    };

    // Convert flat FfiReaderContext → proper proto-matching hierarchy.
    let fgrc: FileGroupReaderContext = ctx.into();

    // ── log the full context at debug level ───────────────────────────────
    log::debug!(
        "new_file_group_reader_with_context: base_uri={base_uri} \
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
    if let Some(rc) = fgrc.reader_context.as_ref() {
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
        base_file_name = split_ctx.base_file_name,
        log_file_names = split_ctx.log_file_names,
    );

    // Build options from the inner ReaderContext config maps plus outer props.
    // Order: table_config < props < hoodie_reader_config (higher = takes precedence).
    let mut options: Vec<(String, String)> = Vec::new();
    if let Some(rc) = fgrc.reader_context.as_ref() {
        for (k, v) in &rc.table_config {
            options.push((k.clone(), v.clone()));
        }
    }
    for (k, v) in &fgrc.props {
        options.push((k.clone(), v.clone()));
    }
    if let Some(rc) = fgrc.reader_context.as_ref() {
        for (k, v) in &rc.hoodie_reader_config {
            options.push((k.clone(), v.clone()));
        }
    }

    // Build HudiConfigs and Storage directly (same as reader_v1 did internally).
    let (hudi_opts, storage_opts) = split_hudi_options_from_others(options);
    let hudi_configs = Arc::new(HudiConfigs::new(hudi_opts));
    let storage = Storage::new(Arc::new(storage_opts), hudi_configs.clone())
        .map_err(|e| format!("Failed to create Storage: {e}"))?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create tokio runtime: {e}"))?;

    Ok(Box::new(HudiFileGroupReader {
        hudi_configs,
        storage,
        rt,
        fgrc,
        split_ctx,
    }))
}

impl HudiFileGroupReader {
    /// Uses the Java-style `HoodieFileGroupReader` which implements the full
    /// 3-phase merge: scan log files into key-based buffer → merge with base → emit.
    pub fn get_closable_iterator(
        &self,
        _file_slice: &HudiFileSlice,
    ) -> std::result::Result<*mut ffi::ArrowArrayStream, String> {
        let fgrc = &self.fgrc;
        let split_ctx = &self.split_ctx;

        // Build InputSplit from context.
        let base_file_path = if fgrc.partition_path.is_empty() {
            split_ctx.base_file_name.clone()
        } else {
            format!("{}/{}", fgrc.partition_path, split_ctx.base_file_name)
        };
        let log_file_paths: Vec<String> = split_ctx
            .log_file_names
            .iter()
            .map(|name| {
                if fgrc.partition_path.is_empty() {
                    name.clone()
                } else {
                    format!("{}/{}", fgrc.partition_path, name)
                }
            })
            .collect();

        let input_split = InputSplit::new(
            Some(base_file_path),
            None,
            log_file_paths,
            fgrc.partition_path.clone(),
        );

        // Build ReaderParameters from context.
        let reader_parameters = ReaderParameters {
            use_record_position: fgrc.should_use_record_position,
            emit_delete: fgrc.emit_delete,
            sort_output: fgrc.sort_output,
            allow_inflight_instants: fgrc.allow_inflight_instants,
        };

        // Extract ordering field names (precombine field) from config, if present.
        let options = self.hudi_configs.as_options();
        let ordering_field_names: Vec<String> = options
            .get("hoodie.table.precombine.field")
            .or_else(|| options.get("hoodie.table.ordering.fields"))
            .map(|f| vec![f.clone()])
            .unwrap_or_default();

        let record_key_field = "_hoodie_record_key".to_string();

        log::debug!(
            "get_closable_iterator: partition={} base_file={} log_files={} \
             latest_instant_time={} ordering_fields={:?} merge_mode={}",
            fgrc.partition_path,
            split_ctx.base_file_name,
            split_ctx.log_file_names.len(),
            fgrc.latest_commit_time,
            ordering_field_names,
            fgrc.reader_context.as_ref().map(|rc| rc.merge_mode.as_str()).unwrap_or("?"),
        );

        let mut reader = HoodieFileGroupReader::new(
            self.hudi_configs.clone(),
            self.storage.clone(),
            input_split,
            ordering_field_names,
            reader_parameters,
            fgrc.latest_commit_time.clone(),
            record_key_field,
        );

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

/// Creates a `HudiFileSlice` from the split context stashed inside the reader.
pub fn new_file_slice_from_context(
    reader: &HudiFileGroupReader,
) -> std::result::Result<Box<HudiFileSlice>, String> {
    let split_ctx = &reader.split_ctx;
    let partition_path = reader.fgrc.partition_path.as_str();

    let mut file_group =
        FileGroup::new_with_base_file_name(&split_ctx.base_file_name, partition_path)
            .map_err(|e| format!("Failed to create FileGroup: {e}"))?;

    let log_refs: Vec<&str> = split_ctx.log_file_names.iter().map(|s| s.as_str()).collect();
    file_group
        .add_log_files_from_names(&log_refs)
        .map_err(|e| format!("Failed to add log files to FileGroup: {e}"))?;

    let (_, file_slice) = file_group
        .file_slices
        .iter()
        .next()
        .ok_or_else(|| format!("Failed to get file slice from FileGroup: {file_group:?}"))?;

    Ok(Box::new(HudiFileSlice {
        inner: file_slice.clone(),
    }))
}

pub struct HudiFileSlice {
    inner: FileSlice,
}

pub fn new_file_slice_from_file_names(
    partition_path: &CxxString,
    base_file_name: &CxxString,
    log_file_names: &CxxVector<CxxString>,
) -> std::result::Result<Box<HudiFileSlice>, String> {
    let partition_path = partition_path
        .to_str()
        .map_err(|e| format!("Failed to convert CxxString to str: {e}"))?;
    let base_file_name = base_file_name
        .to_str()
        .map_err(|e| format!("Failed to convert CxxString to str: {e}"))?;

    let log_file_names = log_file_names
        .iter()
        .map(|name| {
            name.to_str()
                .map_err(|e| format!("Failed to convert CxxString to str: {e}"))
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let mut file_group = FileGroup::new_with_base_file_name(base_file_name, partition_path)
        .map_err(|e| format!("Failed to create FileGroup: {e}"))?;
    file_group
        .add_log_files_from_names(&log_file_names)
        .map_err(|e| format!("Failed to add files to FileGroup: {e}"))?;

    let (_, file_slice) = file_group
        .file_slices
        .iter()
        .next()
        .ok_or_else(|| format!("Failed to get file slice from FileGroup: {file_group:?}"))?;

    Ok(Box::new(HudiFileSlice {
        inner: file_slice.clone(),
    }))
}
