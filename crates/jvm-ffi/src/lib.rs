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
//! A plain C ABI over hudi-rs, so a JVM or a C++ engine can plan a read and
//! read file slices.
//!
//! The `cpp` crate already exposes the same reads, but `cxx` generates
//! C++-mangled symbols and `CxxString`/`CxxVector` arguments, neither of which a
//! JVM can bind to. This crate restates those calls in C: null-terminated
//! strings in, opaque handles and an `ArrowArrayStream` out. It holds no reading
//! logic of its own.
//!
//! A query engine goes through three handles: [`hudi_ffi_table_open`] for the
//! table and its schema ([`hudi_ffi_table_schema`]), [`hudi_ffi_plan_create`]
//! for the file slices of one read after partition and file pruning with the
//! engine's filters, and [`hudi_ffi_plan_read_slice`] for each slice's rows as
//! a stream, projected to the engine's columns and masked by the same filters.
//! The engine keeps every filter it pushes, so a filter the kernel cannot use
//! costs pruning only.
//!
//! # Runtime
//!
//! Every handle's async calls run on one process-wide multi-thread runtime. A
//! stream's `get_next` is called from whatever thread the engine reads on and
//! blocks there until the next batch; the tasks the kernel spawns for that
//! batch (object store requests among them) keep running on the runtime's own
//! workers meanwhile, which a current-thread runtime driven from a callback
//! cannot do. Blocking is not allowed inside a Tokio runtime, so every call,
//! `get_next` included, fails with a message when made from a thread that is
//! already running one; a Rust host calls from `spawn_blocking` instead.
//!
//! # Ownership
//!
//! Every non-null pointer returned here is owned by the caller and must be
//! released exactly once, with the matching `hudi_ffi_free_*`. Releasing twice
//! is a double free; not releasing leaks the reader or the Arrow buffers behind
//! the stream.
//!
//! # Errors and panics
//!
//! A failing call returns null and leaves a message retrievable with
//! [`hudi_ffi_last_error`] until the next call on the same thread. Panics are
//! caught at every boundary and turned into that same null-plus-message, because
//! a panic unwinding into the JVM aborts the process.

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, OnceLock};

use arrow::array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use futures::stream::{BoxStream, StreamExt};
use hudi::config::table::HudiTableConfig;
use hudi::expr::ExprOperator;
use hudi::expr::filter::Filter;
use hudi::file_group::file_slice::FileSlice;
use hudi::file_group::reader::FileGroupReader;
use hudi::table::{QueryType, ReadOptions, Table};

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_error(message: impl Into<String>) {
    let text = message.into();
    let encoded = CString::new(text).unwrap_or_else(|_| {
        CString::new("hudi-rs error message contained an interior nul byte")
            .expect("this literal has no nul")
    });
    LAST_ERROR.with(|slot| *slot.borrow_mut() = Some(encoded));
}

/// The runtime every handle's async calls are driven on; see the module docs.
///
/// Fails on a thread that is already inside a Tokio runtime, where `block_on`
/// would panic.
fn runtime() -> Result<&'static tokio::runtime::Runtime, String> {
    static RUNTIME: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(
            "called from inside a Tokio runtime, where it cannot block; call from a plain thread or spawn_blocking"
                .to_string(),
        );
    }
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("failed to build a tokio runtime: {e}"))
        })
        .as_ref()
        .map_err(Clone::clone)
}

/// The text of a panic payload, for the error message that replaces the panic.
fn panic_detail(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string())
}

/// Run `body`, turning any error or panic into a null return plus a message.
///
/// Every exported function goes through this. A panic that unwinds across the C
/// ABI aborts the process, so catching it here is what keeps a Rust bug from
/// killing the JVM that called us.
fn guard<T>(what: &str, body: impl FnOnce() -> Result<*mut T, String>) -> *mut T {
    // The previous call's message dies here, so `hudi_ffi_last_error` means
    // "the last call failed", not "some call once failed" — a binding that
    // checks the message rather than the return value must not see a stale one.
    LAST_ERROR.with(|slot| *slot.borrow_mut() = None);
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(ptr)) => ptr,
        Ok(Err(message)) => {
            set_error(format!("{what}: {message}"));
            std::ptr::null_mut()
        }
        Err(payload) => {
            set_error(format!("{what} panicked: {}", panic_detail(&*payload)));
            std::ptr::null_mut()
        }
    }
}

/// Borrow a C string, naming the argument in any error.
unsafe fn as_str<'a>(ptr: *const c_char, name: &str) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err(format!("{name} is null"));
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|e| format!("{name} is not valid UTF-8: {e}"))
}

/// Borrow `len` C strings from an array.
unsafe fn as_strs<'a>(
    ptr: *const *const c_char,
    len: usize,
    name: &str,
) -> Result<Vec<&'a str>, String> {
    if len == 0 {
        return Ok(Vec::new());
    }
    if ptr.is_null() {
        return Err(format!("{name} is null but len is {len}"));
    }
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    slice
        .iter()
        .enumerate()
        .map(|(i, p)| unsafe { as_str(*p, &format!("{name}[{i}]")) })
        .collect()
}

/// Borrow a C string that may be null, naming the argument in any error.
unsafe fn as_optional_str<'a>(ptr: *const c_char, name: &str) -> Result<Option<&'a str>, String> {
    if ptr.is_null() {
        Ok(None)
    } else {
        unsafe { as_str(ptr, name) }.map(Some)
    }
}

/// A file group reader, handed a slice by its paths.
pub struct HudiFfiReader {
    inner: FileGroupReader,
}

/// A table handle, for the schema, planning and the metadata read.
///
/// Separate from [`HudiFfiReader`] because the two wrap different things: a file
/// group reader is handed a slice, while a table resolves its own.
pub struct HudiFfiTable {
    inner: Table,
}

/// Open the table at `base_uri`.
///
/// # Safety
/// Pointers must be null-terminated C strings valid for the call, and the two
/// option arrays must each hold `option_count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_table_open(
    base_uri: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_count: usize,
) -> *mut HudiFfiTable {
    guard("hudi_ffi_table_open", || {
        let base_uri = unsafe { as_str(base_uri, "base_uri") }?;
        let keys = unsafe { as_strs(option_keys, option_count, "option_keys") }?;
        let values = unsafe { as_strs(option_values, option_count, "option_values") }?;
        let options: Vec<(&str, &str)> = keys.into_iter().zip(values).collect();

        let inner = runtime()?
            .block_on(Table::new_with_options(base_uri, options))
            .map_err(|e| format!("failed to open {base_uri}: {e}"))?;
        Ok(Box::into_raw(Box::new(HudiFfiTable { inner })))
    })
}

/// Read the metadata table's `files` partition as an Arrow stream.
///
/// `keys` may be null when `key_count` is zero, which reads every record.
/// Keys are matched as stored, so a non-partitioned table's record is asked for
/// as `"."`.
///
/// # Safety
/// `table` must come from [`hudi_ffi_table_open`] and not yet be freed. The
/// returned stream must be released with [`hudi_ffi_free_stream`] exactly once,
/// including on paths where the caller stops reading partway.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_read_metadata_files_partition(
    table: *mut HudiFfiTable,
    keys: *const *const c_char,
    key_count: usize,
) -> *mut FFI_ArrowArrayStream {
    guard("hudi_ffi_read_metadata_files_partition", || {
        if table.is_null() {
            return Err("table is null".to_string());
        }
        let table = unsafe { &*table };
        let keys = unsafe { as_strs(keys, key_count, "keys") }?;

        let batch = runtime()?
            .block_on(table.inner.read_metadata_table_files_partition_arrow(&keys))
            .map_err(|e| format!("failed to read the metadata table: {e}"))?;

        let schema = batch.schema();
        let iterator = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        Ok(Box::into_raw(Box::new(FFI_ArrowArrayStream::new(
            Box::new(iterator),
        ))))
    })
}

/// Release a table handle. Null is accepted and ignored.
///
/// # Safety
/// `table` must come from [`hudi_ffi_table_open`] and be released once only.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_free_table(table: *mut HudiFfiTable) {
    if !table.is_null() {
        drop(unsafe { Box::from_raw(table) });
    }
}

/// Open a reader for the table at `base_uri`.
///
/// `option_keys` and `option_values` are parallel arrays of `option_count`
/// entries. Returns null on failure; see [`hudi_ffi_last_error`].
///
/// # Safety
/// All pointers must be null-terminated C strings valid for the call, and the
/// two option arrays must each hold `option_count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_reader_open(
    base_uri: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_count: usize,
) -> *mut HudiFfiReader {
    guard("hudi_ffi_reader_open", || {
        let base_uri = unsafe { as_str(base_uri, "base_uri") }?;
        let keys = unsafe { as_strs(option_keys, option_count, "option_keys") }?;
        let values = unsafe { as_strs(option_values, option_count, "option_values") }?;
        let options: Vec<(&str, &str)> = keys.into_iter().zip(values).collect();

        let inner = runtime()?
            .block_on(FileGroupReader::new_with_options(base_uri, options))
            .map_err(|e| format!("failed to open {base_uri}: {e}"))?;
        Ok(Box::into_raw(Box::new(HudiFfiReader { inner })))
    })
}

/// Read one file slice, named by its base file and log files, into a stream.
///
/// `log_file_paths` may be null when `log_file_count` is zero, which reads a
/// base file on its own. Returns null on failure.
///
/// # Safety
/// `reader` must come from [`hudi_ffi_reader_open`] and not yet be freed. The
/// returned stream must be released with [`hudi_ffi_free_stream`] exactly once,
/// including on paths where the caller aborts partway through reading it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_read_slice(
    reader: *mut HudiFfiReader,
    base_file_path: *const c_char,
    log_file_paths: *const *const c_char,
    log_file_count: usize,
) -> *mut FFI_ArrowArrayStream {
    guard("hudi_ffi_read_slice", || {
        if reader.is_null() {
            return Err("reader is null".to_string());
        }
        let reader = unsafe { &*reader };
        let base_file_path = unsafe { as_str(base_file_path, "base_file_path") }?;
        let logs = unsafe { as_strs(log_file_paths, log_file_count, "log_file_paths") }?;

        let batch = runtime()?
            .block_on(reader.inner.read_file_slice_from_paths(
                base_file_path,
                logs,
                &ReadOptions::new(),
            ))
            .map_err(|e| format!("failed to read {base_file_path}: {e}"))?;

        // Eager, matching what the cxx bridge does today: one batch wrapped as a
        // stream. `read_file_slice_from_paths_stream` exists and would avoid
        // materialising the slice, but driving an async stream from inside the
        // stream's synchronous `get_next` callback needs care that belongs in its
        // own change.
        let schema = batch.schema();
        let iterator = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        Ok(Box::into_raw(Box::new(FFI_ArrowArrayStream::new(
            Box::new(iterator),
        ))))
    })
}

/// The schema a read of `table` returns: the data columns, preceded by the
/// Hudi meta fields when the table populates them. Every top-level field is
/// nullable: a column the table declares required can still come back null
/// from a file written before it existed, and a consumer that trusts a
/// non-nullable field skips its validity check.
fn read_schema(table: &Table) -> Result<SchemaRef, String> {
    let populates_meta_fields: bool = table
        .hudi_configs
        .get_or_default(HudiTableConfig::PopulatesMetaFields)
        .into();
    let schema = runtime()?
        .block_on(async {
            if populates_meta_fields {
                table.get_schema_with_meta_fields().await
            } else {
                table.get_schema().await
            }
        })
        .map_err(|e| format!("failed to resolve the table schema: {e}"))?;
    let fields: Vec<arrow::datatypes::Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(true))
        .collect();
    Ok(Arc::new(arrow::datatypes::Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

/// The schema a read of the table returns, as an `ArrowSchema`.
///
/// # Safety
/// `table` must come from [`hudi_ffi_table_open`] and not yet be freed. The
/// returned schema must be released with [`hudi_ffi_free_schema`] exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_table_schema(table: *mut HudiFfiTable) -> *mut FFI_ArrowSchema {
    guard("hudi_ffi_table_schema", || {
        if table.is_null() {
            return Err("table is null".to_string());
        }
        let table = unsafe { &*table };
        let schema = read_schema(&table.inner)?;
        let exported = FFI_ArrowSchema::try_from(schema.as_ref())
            .map_err(|e| format!("failed to export the schema: {e}"))?;
        Ok(Box::into_raw(Box::new(exported)))
    })
}

/// Release a schema. Null is accepted and ignored.
///
/// # Safety
/// `schema` must come from [`hudi_ffi_table_schema`] and be released once only.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_free_schema(schema: *mut FFI_ArrowSchema) {
    if !schema.is_null() {
        drop(unsafe { Box::from_raw(schema) });
    }
}

/// One predicate an engine pushes into a read: `column op values`.
///
/// `op` is one of `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `NOT IN`; the
/// comparisons take exactly one value, the list operators at least one. Every
/// value is text, which the kernel casts to the column's type, so an engine
/// pushes only literals whose text form is exact for that type.
#[repr(C)]
pub struct HudiFfiFilter {
    pub column: *const c_char,
    pub op: *const c_char,
    pub values: *const *const c_char,
    pub value_count: usize,
}

/// Borrow `len` filters from an array and turn them into the kernel's.
unsafe fn as_filters(ptr: *const HudiFfiFilter, len: usize) -> Result<Vec<Filter>, String> {
    if len == 0 {
        return Ok(Vec::new());
    }
    if ptr.is_null() {
        return Err(format!("filters is null but filter_count is {len}"));
    }
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    slice
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let column = unsafe { as_str(f.column, &format!("filters[{i}].column")) }?;
            let op = unsafe { as_str(f.op, &format!("filters[{i}].op")) }?;
            let values =
                unsafe { as_strs(f.values, f.value_count, &format!("filters[{i}].values")) }?;
            let operator = op
                .parse::<ExprOperator>()
                .map_err(|e| format!("filters[{i}].op {op:?} is not an operator: {e}"))?;
            Filter::new(
                column.to_string(),
                operator,
                values.into_iter().map(str::to_string).collect(),
            )
            .map_err(|e| format!("filters[{i}] on {column:?} is invalid: {e}"))
        })
        .collect()
}

/// The read options for a snapshot (optionally as of an instant) or an
/// incremental window; `as_of` cannot be combined with `start` or `end`.
fn read_options(
    as_of: Option<&str>,
    start: Option<&str>,
    end: Option<&str>,
    filters: Vec<Filter>,
) -> Result<ReadOptions, String> {
    let mut options = match (as_of, start, end) {
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
            return Err("as_of cannot be combined with start or end".to_string());
        }
        (Some(as_of), None, None) => ReadOptions::new()
            .with_query_type(QueryType::Snapshot)
            .with_as_of_timestamp(as_of),
        (None, None, None) => ReadOptions::new(),
        (None, start, end) => {
            let mut options = ReadOptions::new().with_query_type(QueryType::Incremental);
            if let Some(start) = start {
                options = options.with_start_timestamp(start);
            }
            if let Some(end) = end {
                options = options.with_end_timestamp(end);
            }
            options
        }
    };
    options.filters = filters;
    Ok(options)
}

/// A planned read: the file slices left after pruning, a reader that carries
/// the read's time bounds, and the filters every slice's rows are masked with.
pub struct HudiFfiPlan {
    reader: FileGroupReader,
    slices: Vec<FileSlice>,
    filters: Vec<Filter>,
    schema: SchemaRef,
}

/// Plan a read of `table`.
///
/// `as_of` selects a snapshot as of that instant; `start` and `end` an
/// incremental window `(start, end]`, either bound optional; all three null
/// means the latest snapshot. `filters` prune partitions and, where the base
/// files' statistics allow, files; the same filters mask the rows of every
/// slice read through the plan.
///
/// # Safety
/// `table` must come from [`hudi_ffi_table_open`] and not yet be freed; the
/// strings must be null-terminated and valid for the call, or null where
/// documented; `filters` must hold `filter_count` entries. The returned plan
/// must be released with [`hudi_ffi_free_plan`] exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_plan_create(
    table: *mut HudiFfiTable,
    as_of: *const c_char,
    start: *const c_char,
    end: *const c_char,
    filters: *const HudiFfiFilter,
    filter_count: usize,
) -> *mut HudiFfiPlan {
    guard("hudi_ffi_plan_create", || {
        if table.is_null() {
            return Err("table is null".to_string());
        }
        let table = unsafe { &*table };
        let as_of = unsafe { as_optional_str(as_of, "as_of") }?;
        let start = unsafe { as_optional_str(start, "start") }?;
        let end = unsafe { as_optional_str(end, "end") }?;
        let filters = unsafe { as_filters(filters, filter_count) }?;
        // Read settings given when the table was opened (read-optimized mode,
        // among others) decide which slices planning keeps.
        let options = read_options(as_of, start, end, filters)?
            .with_defaults_from(&table.inner.hudi_configs)
            .map_err(|e| format!("failed to apply the table's read settings: {e}"))?;

        let runtime = runtime()?;
        let slices = runtime
            .block_on(table.inner.get_file_slices(&options))
            .map_err(|e| format!("failed to list the file slices: {e}"))?;
        let reader = runtime
            .block_on(table.inner.create_file_group_reader_with_options(
                Some(&options),
                std::iter::empty::<(&str, &str)>(),
            ))
            .map_err(|e| format!("failed to create a file group reader: {e}"))?;
        let schema = read_schema(&table.inner)?;
        Ok(Box::into_raw(Box::new(HudiFfiPlan {
            reader,
            slices,
            filters: options.filters,
            schema,
        })))
    })
}

/// The number of file slices the plan reads; 0 for a null plan.
///
/// # Safety
/// `plan` must be null or come from [`hudi_ffi_plan_create`] and not yet be freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_plan_slice_count(plan: *const HudiFfiPlan) -> usize {
    match unsafe { plan.as_ref() } {
        Some(plan) => plan.slices.len(),
        None => 0,
    }
}

/// The number of distinct partitions among the plan's slices; 0 for a null plan.
///
/// # Safety
/// `plan` must be null or come from [`hudi_ffi_plan_create`] and not yet be freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_plan_partition_count(plan: *const HudiFfiPlan) -> usize {
    match unsafe { plan.as_ref() } {
        Some(plan) => {
            let mut paths: Vec<&str> = plan
                .slices
                .iter()
                .map(|s| s.partition_path.as_str())
                .collect();
            paths.sort_unstable();
            paths.dedup();
            paths.len()
        }
        None => 0,
    }
}

/// The kernel's record count of the plan's base files, or -1 when a slice does
/// not report one or the plan is null. An upper bound once filters mask rows,
/// and only an estimate where the kernel derived the count from file sizes.
///
/// # Safety
/// `plan` must be null or come from [`hudi_ffi_plan_create`] and not yet be freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_plan_record_count(plan: *const HudiFfiPlan) -> i64 {
    let Some(plan) = (unsafe { plan.as_ref() }) else {
        return -1;
    };
    let mut total = 0i64;
    for slice in &plan.slices {
        let Some(count) = slice
            .base_file
            .as_ref()
            .and_then(|f| f.file_metadata.as_ref())
            .map(|m| m.num_records)
        else {
            return -1;
        };
        total = total.saturating_add(count.max(0));
    }
    total
}

/// One slice's rows as a synchronous batch reader over the kernel's stream.
///
/// The kernel shapes every batch to the projected columns and the table's
/// types; a batch that does not match the declared schema is reported as an
/// error rather than handed to a consumer that trusts the schema. After a
/// panic the kernel's stream may be half advanced, so the stream only reports
/// errors from then on.
struct SliceStream {
    inner: BoxStream<'static, hudi::error::Result<RecordBatch>>,
    schema: SchemaRef,
    poisoned: bool,
}

impl SliceStream {
    fn pull(&mut self) -> Option<Result<RecordBatch, ArrowError>> {
        let batch = match runtime() {
            Ok(runtime) => runtime.block_on(self.inner.next())?,
            Err(e) => return Some(Err(ArrowError::ExternalError(e.into()))),
        };
        let batch = match batch {
            Ok(batch) => batch,
            Err(e) => {
                return Some(Err(ArrowError::ExternalError(
                    format!("failed to read the file slice: {e}").into(),
                )));
            }
        };
        let declared = self.schema.fields();
        let actual = batch.schema();
        for (i, field) in declared.iter().enumerate() {
            match actual.fields().get(i) {
                Some(got) if got.name() == field.name() && got.data_type() == field.data_type() => {
                }
                got => {
                    return Some(Err(ArrowError::ExternalError(
                        format!(
                            "the kernel returned column {i} as {got:?} where the schema declares {field:?}"
                        )
                        .into(),
                    )));
                }
            }
        }
        if actual.fields().len() != declared.len() {
            return Some(Err(ArrowError::ExternalError(
                format!(
                    "the kernel returned {} columns where the schema declares {}",
                    actual.fields().len(),
                    declared.len()
                )
                .into(),
            )));
        }
        // Same columns and types; relabel the batch with the declared schema so
        // its nullability is the one the stream reports.
        let options =
            arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        Some(RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            batch.columns().to_vec(),
            &options,
        ))
    }
}

impl Iterator for SliceStream {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.poisoned {
            return Some(Err(ArrowError::ExternalError(
                "reading the file slice panicked earlier; the stream cannot continue".into(),
            )));
        }
        // The stream's `get_next` carries no panic guard of its own, and a panic
        // unwinding through it would abort the process that reads the stream.
        match catch_unwind(AssertUnwindSafe(|| self.pull())) {
            Ok(item) => item,
            Err(payload) => {
                self.poisoned = true;
                Some(Err(ArrowError::ExternalError(
                    format!(
                        "reading the file slice panicked: {}",
                        panic_detail(&*payload)
                    )
                    .into(),
                )))
            }
        }
    }
}

impl RecordBatchReader for SliceStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Stream the rows of slice `slice_index` of `plan`.
///
/// `columns` names the columns to return, in that order; null with
/// `column_count` zero returns every column of the table schema. The plan's
/// filters mask the rows. `batch_size` is the number of rows per batch the
/// kernel is asked for, or 0 for its default. Returns null on failure.
///
/// # Safety
/// `plan` must come from [`hudi_ffi_plan_create`] and not yet be freed;
/// `columns` must hold `column_count` null-terminated strings. The stream
/// holds what it reads from, so it may outlive the plan. The returned pointer
/// must be released with [`hudi_ffi_free_stream`] exactly once, including on
/// paths where the caller stops reading partway or has moved the stream's
/// contents out (an empty stream frees nothing but its allocation).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_plan_read_slice(
    plan: *mut HudiFfiPlan,
    slice_index: usize,
    columns: *const *const c_char,
    column_count: usize,
    batch_size: usize,
) -> *mut FFI_ArrowArrayStream {
    guard("hudi_ffi_plan_read_slice", || {
        if plan.is_null() {
            return Err("plan is null".to_string());
        }
        let plan = unsafe { &*plan };
        let slice = plan.slices.get(slice_index).ok_or_else(|| {
            format!(
                "slice_index {slice_index} is out of range for a plan of {} slices",
                plan.slices.len()
            )
        })?;
        let columns = unsafe { as_strs(columns, column_count, "columns") }?;

        let mut options = ReadOptions::new();
        if batch_size > 0 {
            options = options
                .with_batch_size(batch_size)
                .map_err(|e| format!("batch_size is invalid: {e}"))?;
        }
        let schema = if columns.is_empty() {
            Arc::clone(&plan.schema)
        } else {
            let indices = columns
                .iter()
                .map(|name| {
                    plan.schema
                        .index_of(name)
                        .map_err(|_| format!("column {name:?} is not in the table schema"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            options = options.with_projection(columns.iter().copied());
            Arc::new(
                plan.schema
                    .project(&indices)
                    .map_err(|e| format!("failed to project the schema: {e}"))?,
            )
        };
        options.filters = plan.filters.clone();

        let inner = runtime()?
            .block_on(plan.reader.read_file_slice_stream(slice, &options))
            .map_err(|e| format!("failed to open the file slice: {e}"))?;
        Ok(Box::into_raw(Box::new(FFI_ArrowArrayStream::new(
            Box::new(SliceStream {
                inner,
                schema,
                poisoned: false,
            }),
        ))))
    })
}

/// Release a plan. Null is accepted and ignored.
///
/// # Safety
/// `plan` must come from [`hudi_ffi_plan_create`] and be released once only.
/// Streams read through it stay valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_free_plan(plan: *mut HudiFfiPlan) {
    if !plan.is_null() {
        drop(unsafe { Box::from_raw(plan) });
    }
}

/// Release a reader. Null is accepted and ignored.
///
/// # Safety
/// `reader` must come from [`hudi_ffi_reader_open`] and be released once only.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_free_reader(reader: *mut HudiFfiReader) {
    if !reader.is_null() {
        drop(unsafe { Box::from_raw(reader) });
    }
}

/// Release a stream. Null is accepted and ignored.
///
/// # Safety
/// `stream` must come from [`hudi_ffi_read_slice`], [`hudi_ffi_plan_read_slice`]
/// or [`hudi_ffi_read_metadata_files_partition`] and be released once only.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_free_stream(stream: *mut FFI_ArrowArrayStream) {
    if !stream.is_null() {
        drop(unsafe { Box::from_raw(stream) });
    }
}

/// The last error on this thread, or null if the last call succeeded.
///
/// The returned string is owned by this library and stays valid until the next
/// call on the same thread.
#[unsafe(no_mangle)]
pub extern "C" fn hudi_ffi_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| match slot.borrow().as_ref() {
        Some(message) => message.as_ptr(),
        None => std::ptr::null(),
    })
}

/// Panic on purpose, so a caller can prove a panic does not cross the boundary.
///
/// Behind a feature that is off by default, so it is absent from a shipped
/// library. A test that needs it builds with `--features ffi-test-hooks`; the
/// point is to exercise [`guard`]'s `catch_unwind` from the far side of the C
/// ABI, which a Rust unit test cannot do because it never crosses it.
#[cfg(feature = "ffi-test-hooks")]
#[unsafe(no_mangle)]
pub extern "C" fn hudi_ffi_panic_for_test() -> *mut FFI_ArrowArrayStream {
    guard("hudi_ffi_panic_for_test", || {
        panic!("deliberate panic, to prove it is caught at the boundary")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A panic inside a guarded body becomes null plus a message, not an abort.
    ///
    /// This is the whole of the panic-safety contract on the Rust side: a panic
    /// unwinding across `extern "C"` aborts the process, so every export routes
    /// through `guard`. Removing the `catch_unwind` fails this test by aborting
    /// the test binary, which is the loudest possible failure.
    #[test]
    fn a_panic_becomes_a_null_and_a_message() {
        let ptr = guard::<u8>("boom", || panic!("deliberate"));
        assert!(ptr.is_null(), "a panicking body must return null");
        let message = unsafe { CStr::from_ptr(hudi_ffi_last_error()) }
            .to_str()
            .unwrap();
        assert!(
            message.contains("boom panicked") && message.contains("deliberate"),
            "the message must name the call and the payload, got {message:?}"
        );
    }

    /// An error becomes null plus a message that names the call.
    #[test]
    fn an_error_becomes_a_null_and_a_message() {
        let ptr = guard::<u8>("open", || Err("no such table".to_string()));
        assert!(ptr.is_null());
        let message = unsafe { CStr::from_ptr(hudi_ffi_last_error()) }
            .to_str()
            .unwrap();
        assert_eq!(message, "open: no such table");
    }

    /// A successful call clears the previous failure's message.
    ///
    /// `hudi_ffi_last_error` documents "null if the last call succeeded", and a
    /// JVM binding that checks the message rather than the return value relies
    /// on it: without the clear, one failure makes every later success look
    /// like that same failure.
    #[test]
    fn a_success_clears_the_previous_failure() {
        let _ = guard::<u8>("first", || Err("first failed".to_string()));
        assert!(
            !hudi_ffi_last_error().is_null(),
            "the failure must set a message, or the clearing below is unproven"
        );

        let ptr = guard::<u8>("second", || Ok(Box::into_raw(Box::new(7u8))));
        assert!(!ptr.is_null());
        drop(unsafe { Box::from_raw(ptr) });
        assert!(
            hudi_ffi_last_error().is_null(),
            "a successful call must leave no error to read"
        );
    }

    /// Freeing null is a no-op, on every free.
    ///
    /// A JVM caller's `finally` runs whether or not the call succeeded, so it
    /// will free a null handle on the failure path. That has to be safe or the
    /// error path becomes a crash.
    #[test]
    fn freeing_null_is_a_no_op() {
        unsafe {
            hudi_ffi_free_reader(std::ptr::null_mut());
            hudi_ffi_free_stream(std::ptr::null_mut());
            hudi_ffi_free_table(std::ptr::null_mut());
            hudi_ffi_free_schema(std::ptr::null_mut());
            hudi_ffi_free_plan(std::ptr::null_mut());
        }
        assert_eq!(unsafe { hudi_ffi_plan_slice_count(std::ptr::null()) }, 0);
        assert_eq!(
            unsafe { hudi_ffi_plan_partition_count(std::ptr::null()) },
            0
        );
        assert_eq!(unsafe { hudi_ffi_plan_record_count(std::ptr::null()) }, -1);
    }

    /// A null argument is refused rather than dereferenced.
    #[test]
    fn null_arguments_are_refused() {
        let reader = unsafe {
            hudi_ffi_reader_open(std::ptr::null(), std::ptr::null(), std::ptr::null(), 0)
        };
        assert!(reader.is_null(), "a null base_uri must not open a reader");

        let stream = unsafe {
            hudi_ffi_read_metadata_files_partition(std::ptr::null_mut(), std::ptr::null(), 0)
        };
        assert!(stream.is_null(), "a null table must not produce a stream");
    }

    /// A non-UTF-8 argument is refused with a message rather than panicking.
    #[test]
    fn invalid_utf8_is_refused() {
        let bad = [0xffu8, 0xfe, 0x00];
        let ptr = unsafe {
            hudi_ffi_reader_open(
                bad.as_ptr() as *const c_char,
                std::ptr::null(),
                std::ptr::null(),
                0,
            )
        };
        assert!(ptr.is_null());
        let message = unsafe { CStr::from_ptr(hudi_ffi_last_error()) }
            .to_str()
            .unwrap();
        assert!(message.contains("not valid UTF-8"), "got {message:?}");
    }

    /// The error slot is per thread, so one thread's failure is not another's.
    #[test]
    fn the_error_slot_is_per_thread() {
        let _ = guard::<u8>("outer", || Err("outer failed".to_string()));
        std::thread::spawn(|| {
            assert!(
                hudi_ffi_last_error().is_null(),
                "a fresh thread must start with no error"
            );
        })
        .join()
        .unwrap();
        let message = unsafe { CStr::from_ptr(hudi_ffi_last_error()) }
            .to_str()
            .unwrap();
        assert_eq!(
            message, "outer: outer failed",
            "the outer thread keeps its own"
        );
    }

    use arrow::array::{Array, BooleanArray, Int32Array, StringArray};
    use arrow::ffi_stream::ArrowArrayStreamReader;
    use hudi_test::SampleTable;

    /// Owns the C strings of a call and hands out the pointer arrays.
    struct CArgs {
        strings: Vec<CString>,
    }

    impl CArgs {
        fn new<'a>(values: impl IntoIterator<Item = &'a str>) -> Self {
            Self {
                strings: values
                    .into_iter()
                    .map(|v| CString::new(v).expect("test strings have no nul"))
                    .collect(),
            }
        }

        fn pointers(&self) -> Vec<*const c_char> {
            self.strings.iter().map(|s| s.as_ptr()).collect()
        }
    }

    fn last_error() -> String {
        unsafe { CStr::from_ptr(hudi_ffi_last_error()) }
            .to_str()
            .unwrap()
            .to_string()
    }

    fn open(path: &str) -> *mut HudiFfiTable {
        open_with(path, &[])
    }

    fn open_with(path: &str, options: &[(&str, &str)]) -> *mut HudiFfiTable {
        let uri = CString::new(path).unwrap();
        let keys = CArgs::new(options.iter().map(|(k, _)| *k));
        let values = CArgs::new(options.iter().map(|(_, v)| *v));
        let (keys, values) = (keys.pointers(), values.pointers());
        let table = unsafe {
            hudi_ffi_table_open(uri.as_ptr(), keys.as_ptr(), values.as_ptr(), options.len())
        };
        assert!(!table.is_null(), "{}", last_error());
        table
    }

    /// The (id, value) pairs of a boolean column across every batch, by id.
    fn bools(batches: &[RecordBatch], column: &str) -> Vec<(i32, bool)> {
        let mut rows: Vec<(i32, bool)> = batches
            .iter()
            .flat_map(|b| {
                let ids = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let values = b
                    .column_by_name(column)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                (0..b.num_rows())
                    .map(|i| (ids.value(i), values.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect();
        rows.sort_unstable();
        rows
    }

    /// A plan with `filters` given as (column, op, values).
    fn plan(
        table: *mut HudiFfiTable,
        as_of: Option<&str>,
        window: (Option<&str>, Option<&str>),
        filters: &[(&str, &str, &[&str])],
    ) -> *mut HudiFfiPlan {
        let as_of = as_of.map(|s| CString::new(s).unwrap());
        let start = window.0.map(|s| CString::new(s).unwrap());
        let end = window.1.map(|s| CString::new(s).unwrap());
        let args: Vec<(CString, CString, CArgs)> = filters
            .iter()
            .map(|(column, op, values)| {
                (
                    CString::new(*column).unwrap(),
                    CString::new(*op).unwrap(),
                    CArgs::new(values.iter().copied()),
                )
            })
            .collect();
        let value_pointers: Vec<Vec<*const c_char>> = args.iter().map(|a| a.2.pointers()).collect();
        let ffi_filters: Vec<HudiFfiFilter> = args
            .iter()
            .zip(&value_pointers)
            .map(|((column, op, _), values)| HudiFfiFilter {
                column: column.as_ptr(),
                op: op.as_ptr(),
                values: values.as_ptr(),
                value_count: values.len(),
            })
            .collect();
        let null = std::ptr::null();
        unsafe {
            hudi_ffi_plan_create(
                table,
                as_of.as_ref().map_or(null, |s| s.as_ptr()),
                start.as_ref().map_or(null, |s| s.as_ptr()),
                end.as_ref().map_or(null, |s| s.as_ptr()),
                ffi_filters.as_ptr(),
                ffi_filters.len(),
            )
        }
    }

    /// Every batch of every slice of `plan`, projected to `columns`.
    fn read_all(plan: *mut HudiFfiPlan, columns: &[&str], batch_size: usize) -> Vec<RecordBatch> {
        let args = CArgs::new(columns.iter().copied());
        let pointers = args.pointers();
        let mut batches = Vec::new();
        for index in 0..unsafe { hudi_ffi_plan_slice_count(plan) } {
            let stream = unsafe {
                hudi_ffi_plan_read_slice(plan, index, pointers.as_ptr(), pointers.len(), batch_size)
            };
            assert!(!stream.is_null(), "{}", last_error());
            let reader = unsafe { ArrowArrayStreamReader::from_raw(stream) }.unwrap();
            let declared = reader.schema();
            for batch in reader {
                let batch = batch.unwrap();
                assert_eq!(batch.schema(), declared);
                batches.push(batch);
            }
            unsafe { hudi_ffi_free_stream(stream) };
        }
        batches
    }

    fn ids(batches: &[RecordBatch]) -> Vec<i32> {
        let mut ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// The schema leads with the meta fields and the stream reports the
    /// projected columns in the order asked for.
    #[test]
    fn test_table_schema_and_projection_order() {
        let table = open(&SampleTable::V6SimplekeygenNonhivestyle.path_to_cow());
        let schema = unsafe { hudi_ffi_table_schema(table) };
        assert!(!schema.is_null(), "{}", last_error());
        let imported = arrow::datatypes::Schema::try_from(unsafe { &*schema }).unwrap();
        assert_eq!(imported.field(0).name(), "_hoodie_commit_time");
        assert!(imported.index_of("byteField").is_ok());
        unsafe { hudi_ffi_free_schema(schema) };

        let plan = plan(table, None, (None, None), &[]);
        assert!(!plan.is_null(), "{}", last_error());
        let batches = read_all(plan, &["name", "id"], 1);
        let first = &batches[0];
        assert_eq!(first.schema().field(0).name(), "name");
        assert_eq!(first.schema().field(1).name(), "id");
        assert!(first.schema().fields().iter().all(|f| f.is_nullable()));
        // Partition 10 holds two rows in one slice, so a batch size of 1 splits it.
        assert_eq!(
            batches.len(),
            4,
            "a batch size of 1 gives one batch per row"
        );
        assert!(batches.iter().all(|b| b.num_rows() == 1));
        assert_eq!(ids(&batches), vec![1, 2, 3, 4]);
        unsafe {
            hudi_ffi_free_plan(plan);
            hudi_ffi_free_table(table);
        }
    }

    /// A partition filter prunes slices and partitions; every filter also masks
    /// rows, on a copy-on-write and on a merge-on-read table alike.
    #[test]
    fn test_plan_filters_prune_and_mask() {
        for (path, merge_on_read) in [
            (SampleTable::V6SimplekeygenNonhivestyle.path_to_cow(), false),
            (
                SampleTable::V6SimplekeygenNonhivestyle.path_to_mor_parquet(),
                true,
            ),
        ] {
            let table = open(&path);

            let all = plan(table, None, (None, None), &[]);
            let has_logs = unsafe { &*all }.slices.iter().any(|s| s.has_log_file());
            assert_eq!(
                has_logs, merge_on_read,
                "only the merge-on-read table reads a slice with a log file"
            );
            assert_eq!(unsafe { hudi_ffi_plan_slice_count(all) }, 3);
            assert_eq!(unsafe { hudi_ffi_plan_partition_count(all) }, 3);
            assert!(unsafe { hudi_ffi_plan_record_count(all) } > 0);
            assert_eq!(ids(&read_all(all, &[], 0)), vec![1, 2, 3, 4]);
            unsafe { hudi_ffi_free_plan(all) };

            let one = plan(table, None, (None, None), &[("byteField", "=", &["10"])]);
            assert!(!one.is_null(), "{}", last_error());
            assert_eq!(unsafe { hudi_ffi_plan_slice_count(one) }, 1);
            assert_eq!(unsafe { hudi_ffi_plan_partition_count(one) }, 1);
            assert_eq!(ids(&read_all(one, &["id"], 0)), vec![1, 3]);
            unsafe { hudi_ffi_free_plan(one) };

            let masked = plan(
                table,
                None,
                (None, None),
                &[
                    ("byteField", "IN", &["10", "30"]),
                    ("isActive", "=", &["true"]),
                ],
            );
            assert_eq!(unsafe { hudi_ffi_plan_slice_count(masked) }, 2);
            let batches = read_all(masked, &["id", "isActive"], 0);
            assert_eq!(bools(&batches, "isActive"), vec![(3, true), (4, true)]);
            unsafe { hudi_ffi_free_plan(masked) };

            let none = plan(table, None, (None, None), &[("byteField", "=", &["99"])]);
            assert!(!none.is_null(), "{}", last_error());
            assert_eq!(unsafe { hudi_ffi_plan_slice_count(none) }, 0);
            unsafe { hudi_ffi_free_plan(none) };

            let by_name = plan(
                table,
                None,
                (None, None),
                &[("name", "NOT IN", &["Alice", "Carol"])],
            );
            let batches = read_all(by_name, &["name", "id"], 0);
            assert_eq!(ids(&batches), vec![2, 4]);
            for batch in &batches {
                let names = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert!(names.iter().flatten().all(|n| n == "Bob" || n == "Diana"));
            }
            unsafe {
                hudi_ffi_free_plan(by_name);
                hudi_ffi_free_table(table);
            }
        }
    }

    /// Time travel and an incremental window go through the same plan.
    #[test]
    fn test_plan_time_travel_and_incremental() {
        let table = open(&SampleTable::V6SimplekeygenNonhivestyle.path_to_cow());
        let first = plan(table, Some("20240418172802262"), (None, None), &[]);
        assert!(!first.is_null(), "{}", last_error());
        assert_eq!(ids(&read_all(first, &["id"], 0)), vec![1, 2, 3]);
        unsafe { hudi_ffi_free_plan(first) };

        let window = plan(
            table,
            None,
            (Some("20240418172802262"), Some("20240418172804498")),
            &[("byteField", "IN", &["10", "30"])],
        );
        assert!(!window.is_null(), "{}", last_error());
        assert_eq!(ids(&read_all(window, &["id"], 0)), vec![1, 4]);
        unsafe { hudi_ffi_free_plan(window) };

        let both = plan(table, Some("20240418172802262"), (Some("x"), None), &[]);
        assert!(both.is_null());
        assert!(
            last_error().contains("as_of cannot be combined"),
            "{}",
            last_error()
        );
        unsafe { hudi_ffi_free_table(table) };
    }

    /// A malformed filter, a bad column or an out-of-range slice fails with a
    /// message instead of a crash.
    #[test]
    fn test_plan_refuses_bad_arguments() {
        let table = open(&SampleTable::V6SimplekeygenNonhivestyle.path_to_cow());
        assert!(plan(table, None, (None, None), &[("id", "LIKE", &["1"])]).is_null());
        assert!(last_error().contains("not an operator"), "{}", last_error());
        assert!(plan(table, None, (None, None), &[("id", "IN", &[])]).is_null());
        assert!(last_error().contains("filters[0]"), "{}", last_error());
        assert!(
            plan(
                table,
                None,
                (None, None),
                &[("no_such_column", "=", &["1"])]
            )
            .is_null()
        );

        let plan = plan(table, None, (None, None), &[]);
        let column = CArgs::new(["no_such_column"]);
        let pointers = column.pointers();
        let stream = unsafe { hudi_ffi_plan_read_slice(plan, 0, pointers.as_ptr(), 1, 0) };
        assert!(stream.is_null());
        assert!(
            last_error().contains("not in the table schema"),
            "{}",
            last_error()
        );
        let stream = unsafe { hudi_ffi_plan_read_slice(plan, 99, std::ptr::null(), 0, 0) };
        assert!(stream.is_null());
        assert!(last_error().contains("out of range"), "{}", last_error());
        unsafe {
            hudi_ffi_free_plan(plan);
            hudi_ffi_free_table(table);
        }
    }

    /// A table without meta fields reads with its data columns only.
    #[test]
    fn test_plan_without_meta_fields() {
        let table = open(&SampleTable::V6SimplekeygenHivestyleNoMetafields.path_to_cow());
        let plan = plan(table, None, (None, None), &[("isActive", "=", &["true"])]);
        assert!(!plan.is_null(), "{}", last_error());
        let batches = read_all(plan, &[], 0);
        assert!(
            batches[0]
                .schema()
                .fields()
                .iter()
                .all(|f| !f.name().starts_with("_hoodie_"))
        );
        assert_eq!(ids(&batches), vec![3, 4]);
        unsafe {
            hudi_ffi_free_plan(plan);
            hudi_ffi_free_table(table);
        }
    }

    /// A read setting given at open shapes the plan: read-optimized mode keeps
    /// the merge-on-read table's base files without their logs, so the row the
    /// log file updates reads as first written.
    #[test]
    fn test_plan_honors_read_settings_from_open() {
        let path = SampleTable::V6SimplekeygenNonhivestyle.path_to_mor_parquet();
        let table = open_with(&path, &[("hoodie.read.use.read_optimized.mode", "true")]);
        let plan = plan(table, None, (None, None), &[]);
        assert!(!plan.is_null(), "{}", last_error());
        assert!(unsafe { &*plan }.slices.iter().all(|s| !s.has_log_file()));
        let batches = read_all(plan, &["id", "isActive"], 0);
        assert_eq!(
            bools(&batches, "isActive"),
            vec![(1, true), (2, false), (3, true), (4, true)]
        );
        unsafe {
            hudi_ffi_free_plan(plan);
            hudi_ffi_free_table(table);
        }
    }

    /// A stream keeps what it reads from, so it outlives its plan and table.
    #[test]
    fn test_stream_outlives_its_plan() {
        let table = open(&SampleTable::V6SimplekeygenNonhivestyle.path_to_cow());
        let plan = plan(table, None, (None, None), &[("byteField", "=", &["10"])]);
        let column = CArgs::new(["id"]);
        let pointers = column.pointers();
        let stream = unsafe { hudi_ffi_plan_read_slice(plan, 0, pointers.as_ptr(), 1, 0) };
        assert!(!stream.is_null(), "{}", last_error());
        unsafe {
            hudi_ffi_free_plan(plan);
            hudi_ffi_free_table(table);
        }
        let reader = unsafe { ArrowArrayStreamReader::from_raw(stream) }.unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(ids(&batches), vec![1, 3]);
        unsafe { hudi_ffi_free_stream(stream) };
    }

    /// A call from inside a Tokio runtime fails with a message instead of
    /// panicking in `block_on`.
    #[test]
    fn test_call_from_inside_a_tokio_runtime_fails_cleanly() {
        let path = CString::new(SampleTable::V6SimplekeygenNonhivestyle.path_to_cow()).unwrap();
        let host = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let table = host.block_on(async {
            unsafe { hudi_ffi_table_open(path.as_ptr(), std::ptr::null(), std::ptr::null(), 0) }
        });
        assert!(table.is_null());
        assert!(
            last_error().contains("inside a Tokio runtime"),
            "{}",
            last_error()
        );
    }
}
