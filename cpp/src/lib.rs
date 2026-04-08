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
use hudi::file_group::FileGroup;
use hudi::file_group::file_slice::FileSlice;
use hudi::file_group::reader_v1::FileGroupReader;
use std::sync::OnceLock;

static LOGGER: OnceLock<()> = OnceLock::new();

/// Initialize env_logger exactly once for the lifetime of the loaded shared library.
///
/// Always enables `debug` level for all `hudi*` modules regardless of `RUST_LOG`.
/// `RUST_LOG` directives are still parsed on top and can raise the floor for other crates.
fn init_logger() {
    LOGGER.get_or_init(|| {
        let _ = env_logger::Builder::new()
            .filter_module("hudi", log::LevelFilter::Debug)
            .parse_default_env()
            .try_init();
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

        // ── legacy APIs (kept for backward compatibility) ───────────────
        fn new_file_group_reader_with_options(
            base_uri: &CxxString,
            options: &CxxVector<CxxString>,
        ) -> Result<Box<HudiFileGroupReader>>;

        type HudiFileSlice;
        fn new_file_slice_from_file_names(
            partition_path: &CxxString,
            base_file_name: &CxxString,
            log_file_names: &CxxVector<CxxString>,
        ) -> Result<Box<HudiFileSlice>>;

        fn read_file_slice_by_base_file_path(
            self: &HudiFileGroupReader,
            relative_path: &CxxString,
        ) -> Result<*mut ArrowArrayStream>;

        fn read_file_slice(
            self: &HudiFileGroupReader,
            file_slice: &HudiFileSlice,
        ) -> Result<*mut ArrowArrayStream>;

        // ── new context-based APIs (mimicking Hudi OSS builder pattern) ─
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
    inner: FileGroupReader,
    rt: tokio::runtime::Runtime,
    /// Full file-group reader context converted from FfiReaderContext.
    /// Mirrors `HoodieFileGroupReaderContext` proto hierarchy.
    fgrc: Option<FileGroupReaderContext>,
    /// File-slice split fields (not in proto) needed to build the FileSlice.
    split_ctx: Option<SplitContext>,
}

/// File-slice fields that come from the split (not from the proto).
struct SplitContext {
    base_file_name: String,
    log_file_names: Vec<String>,
}

// ── legacy APIs ──────────────────────────────────────────────────────────────

pub fn new_file_group_reader_with_options(
    base_uri: &CxxString,
    options: &CxxVector<CxxString>,
) -> std::result::Result<Box<HudiFileGroupReader>, String> {
    init_logger();

    let base_uri = base_uri
        .to_str()
        .map_err(|e| format!("Failed to convert CxxString to str: {e}"))?;

    let mut opt_vec = Vec::new();
    for opt in options.iter() {
        let opt_str = opt
            .to_str()
            .map_err(|e| format!("Failed to convert CxxString to str: {e}"))?;
        if let Some((key, value)) = opt_str.split_once('=') {
            opt_vec.push((key, value))
        }
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create tokio runtime: {e}"))?;
    let reader = rt
        .block_on(FileGroupReader::new_with_options(base_uri, opt_vec))
        .map_err(|e| format!("Failed to create FileGroupReader: {e}"))?;
    Ok(Box::new(HudiFileGroupReader {
        inner: reader,
        rt,
        fgrc: None,
        split_ctx: None,
    }))
}

impl HudiFileGroupReader {
    pub fn read_file_slice_by_base_file_path(
        &self,
        relative_path: &CxxString,
    ) -> std::result::Result<*mut ffi::ArrowArrayStream, String> {
        let relative_path = relative_path
            .to_str()
            .map_err(|e| format!("Failed to convert CxxString to str: {e}"))?;

        let record_batch = self
            .rt
            .block_on(self.inner.read_file_slice_by_base_file_path(relative_path))
            .map_err(|e| format!("Failed to read file batch: {e}"))?;
        let schema = record_batch.schema();

        Ok(create_raw_pointer_for_record_batches(
            vec![record_batch],
            schema,
        ))
    }

    pub fn read_file_slice(
        &self,
        file_slice: &HudiFileSlice,
    ) -> std::result::Result<*mut ffi::ArrowArrayStream, String> {
        let record_batch = self
            .rt
            .block_on(self.inner.read_file_slice(&file_slice.inner))
            .map_err(|e| format!("Failed to read file slice: {e}"))?;
        let schema = record_batch.schema();

        Ok(create_raw_pointer_for_record_batches(
            vec![record_batch],
            schema,
        ))
    }

    /// New API: equivalent to `read_file_slice` but named to match Hudi OSS
    /// `HoodieFileGroupReader.getClosableIterator()`.
    /// Returns an `ArrowArrayStream*` that the C++ caller owns.
    pub fn get_closable_iterator(
        &self,
        file_slice: &HudiFileSlice,
    ) -> std::result::Result<*mut ffi::ArrowArrayStream, String> {
        // TODO: once hudi-rs supports streaming reads with context-aware
        // merge (record position, schemas, etc.), wire them through here.
        // For now, delegates to the existing read_file_slice path.
        let record_batch = self
            .rt
            .block_on(self.inner.read_file_slice(&file_slice.inner))
            .map_err(|e| format!("Failed to read file slice: {e}"))?;
        let schema = record_batch.schema();

        Ok(create_raw_pointer_for_record_batches(
            vec![record_batch],
            schema,
        ))
    }
}

// ── new context-based APIs ───────────────────────────────────────────────────

/// Creates a `FileGroupReader` from a base URI and a full `FfiReaderContext`.
///
/// Mimics Hudi OSS:
/// ```java
/// HoodieFileGroupReader.newBuilder()
///     .withReaderContext(readerContext)
///     .withHoodieTableMetaClient(metaClient)  // → table_config
///     .withLatestCommitTime(queryTimestamp)
///     .withProps(props)                        // → hoodie_reader_config
///     .withShouldUseRecordPosition(...)
///     ...
///     .build()
/// ```
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

    // TODO: map remaining context fields (merge_mode, iterator_mode,
    // instant_range, schemas, etc.) to hudi-rs config options once
    // hudi-rs supports them natively.

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create tokio runtime: {e}"))?;
    let reader = rt
        .block_on(FileGroupReader::new_with_options(base_uri, options))
        .map_err(|e| format!("Failed to create FileGroupReader with context: {e}"))?;

    Ok(Box::new(HudiFileGroupReader {
        inner: reader,
        rt,
        fgrc: Some(fgrc),
        split_ctx: Some(split_ctx),
    }))
}

/// Creates a `HudiFileSlice` from the split context stashed inside the reader.
/// This avoids a second round-trip of file-slice fields from C++.
pub fn new_file_slice_from_context(
    reader: &HudiFileGroupReader,
) -> std::result::Result<Box<HudiFileSlice>, String> {
    let split_ctx = reader
        .split_ctx
        .as_ref()
        .ok_or_else(|| {
            "new_file_slice_from_context called on a reader created with \
             new_file_group_reader_with_options (no stashed context)"
                .to_string()
        })?;
    let partition_path = reader
        .fgrc
        .as_ref()
        .map(|fgrc| fgrc.partition_path.as_str())
        .unwrap_or("");

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
