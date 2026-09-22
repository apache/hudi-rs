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
mod util;

use crate::util::create_raw_pointer_for_record_batches;
use arrow_array::RecordBatch;
use cxx::{CxxString, CxxVector};
use hudi::file_group::FileGroup;
use hudi::file_group::file_slice::FileSlice;
use hudi::file_group::reader::FileGroupReader;
use hudi::table::ReadOptions;
use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};

#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        include!("arrow/c/abi.h");

        type ArrowArrayStream;
    }

    /// One column predicate, `field op value`. `op` is one of `=`, `!=`, `<`,
    /// `<=`, `>`, `>=`, `IN`, `NOT IN`; the last two take a comma-separated
    /// `value`.
    struct HudiFilter {
        field: String,
        op: String,
        value: String,
    }

    /// Options for one read. An empty `projection` reads every column;
    /// `hudi_options` are `key=value` read configs such as
    /// `hoodie.read.use.read_optimized.mode=true`.
    struct HudiReadOptions {
        projection: Vec<String>,
        filters: Vec<HudiFilter>,
        hudi_options: Vec<String>,
    }

    extern "Rust" {
        type HudiFileGroupReader;
        /// Opens a reader for the table at `base_uri` with `key=value` options.
        /// An option without `=` is an error.
        ///
        /// Throws `rust::Error` on failure, including a panic inside the library.
        fn new_file_group_reader_with_options(
            base_uri: &CxxString,
            options: &CxxVector<CxxString>,
        ) -> Result<Box<HudiFileGroupReader>>;

        type HudiFileSlice;
        /// Builds a file slice from file names within `partition_path`.
        ///
        /// Throws `rust::Error` on failure, including a panic inside the library.
        fn new_file_slice_from_file_names(
            partition_path: &CxxString,
            base_file_name: &CxxString,
            log_file_names: &CxxVector<CxxString>,
        ) -> Result<Box<HudiFileSlice>>;

        /// Reads and merges a file slice given paths relative to the table base.
        ///
        /// Throws `rust::Error` on failure, including a panic inside the library.
        fn read_file_slice_from_paths(
            self: &HudiFileGroupReader,
            base_file_path: &CxxString,
            log_file_paths: &CxxVector<CxxString>,
        ) -> Result<*mut ArrowArrayStream>;

        /// Reads and merges a file slice.
        ///
        /// Throws `rust::Error` on failure, including a panic inside the library.
        fn read_file_slice(
            self: &HudiFileGroupReader,
            file_slice: &HudiFileSlice,
        ) -> Result<*mut ArrowArrayStream>;

        /// Like `read_file_slice_from_paths`, honoring `options`.
        ///
        /// Throws `rust::Error` on failure, including a panic inside the library.
        fn read_file_slice_from_paths_with_options(
            self: &HudiFileGroupReader,
            base_file_path: &CxxString,
            log_file_paths: &CxxVector<CxxString>,
            options: &HudiReadOptions,
        ) -> Result<*mut ArrowArrayStream>;

        /// Like `read_file_slice`, honoring `options`.
        ///
        /// Throws `rust::Error` on failure, including a panic inside the library.
        fn read_file_slice_with_options(
            self: &HudiFileGroupReader,
            file_slice: &HudiFileSlice,
            options: &HudiReadOptions,
        ) -> Result<*mut ArrowArrayStream>;
    }
}

/// Converts a panic in `f` into the `Err` the bridge already reports.
///
/// cxx's shims are `extern "C"`, so a panic unwinding out of one aborts the
/// process, and with it every query in a host that embeds this library.
/// `AssertUnwindSafe` holds because the reader is read-only across calls;
/// the only state a panic can leave mid-update is storage read counters,
/// which nothing depends on for correctness.
fn guard<T>(entry_point: &str, f: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|payload| {
        Err(format!(
            "{entry_point} panicked: {}",
            panic_message(payload.as_ref())
        ))
    })
}

/// The message a panic carried, for the two payload types `panic!` produces.
fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

fn cxx_str(value: &CxxString) -> Result<&str, String> {
    value
        .to_str()
        .map_err(|e| format!("Failed to convert CxxString to str: {e}"))
}

fn cxx_strs(values: &CxxVector<CxxString>) -> Result<Vec<&str>, String> {
    values.iter().map(cxx_str).collect()
}

/// Splits `key=value` options, erroring on a malformed one rather than
/// dropping it: a typo that silently loses an option is hard to find from C++.
fn key_values<'a>(
    options: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<(&'a str, &'a str)>, String> {
    options
        .into_iter()
        .map(|opt| {
            opt.split_once('=')
                .ok_or_else(|| format!("Option is not key=value: {opt}"))
        })
        .collect()
}

impl TryFrom<&ffi::HudiReadOptions> for ReadOptions {
    type Error = String;

    fn try_from(options: &ffi::HudiReadOptions) -> Result<Self, String> {
        let hudi_options = key_values(options.hudi_options.iter().map(String::as_str))?;
        let mut read_options = ReadOptions::new()
            .with_filters(options.filters.iter().map(|f| (&f.field, &f.op, &f.value)))
            .map_err(|e| format!("Invalid filter: {e}"))?
            .with_hudi_options(hudi_options);
        if !options.projection.is_empty() {
            read_options = read_options.with_projection(options.projection.clone());
        }
        Ok(read_options)
    }
}

fn to_stream(batch: RecordBatch) -> *mut ffi::ArrowArrayStream {
    let schema = batch.schema();
    create_raw_pointer_for_record_batches(vec![batch], schema)
}

#[derive(Debug)]
pub struct HudiFileGroupReader {
    inner: FileGroupReader,
    rt: tokio::runtime::Runtime,
}

pub fn new_file_group_reader_with_options(
    base_uri: &CxxString,
    options: &CxxVector<CxxString>,
) -> Result<Box<HudiFileGroupReader>, String> {
    guard("new_file_group_reader_with_options", || {
        let base_uri = cxx_str(base_uri)?;

        let opt_vec = key_values(cxx_strs(options)?)?;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to create tokio runtime: {e}"))?;
        let reader = rt
            .block_on(FileGroupReader::new_with_options(base_uri, opt_vec))
            .map_err(|e| format!("Failed to create FileGroupReader: {e}"))?;
        Ok(Box::new(HudiFileGroupReader { inner: reader, rt }))
    })
}

impl HudiFileGroupReader {
    fn read_paths(
        &self,
        base_file_path: &CxxString,
        log_file_paths: &CxxVector<CxxString>,
        options: &ReadOptions,
    ) -> Result<*mut ffi::ArrowArrayStream, String> {
        let base_file_path = cxx_str(base_file_path)?;
        let log_file_paths = cxx_strs(log_file_paths)?;
        self.rt
            .block_on(self.inner.read_file_slice_from_paths(
                base_file_path,
                log_file_paths,
                options,
            ))
            .map(to_stream)
            .map_err(|e| format!("Failed to read file batch: {e}"))
    }

    fn read_slice(
        &self,
        file_slice: &HudiFileSlice,
        options: &ReadOptions,
    ) -> Result<*mut ffi::ArrowArrayStream, String> {
        self.rt
            .block_on(self.inner.read_file_slice(&file_slice.inner, options))
            .map(to_stream)
            .map_err(|e| format!("Failed to read file slice: {e}"))
    }

    pub fn read_file_slice_from_paths(
        &self,
        base_file_path: &CxxString,
        log_file_paths: &CxxVector<CxxString>,
    ) -> Result<*mut ffi::ArrowArrayStream, String> {
        guard("read_file_slice_from_paths", || {
            self.read_paths(base_file_path, log_file_paths, &ReadOptions::new())
        })
    }

    pub fn read_file_slice_from_paths_with_options(
        &self,
        base_file_path: &CxxString,
        log_file_paths: &CxxVector<CxxString>,
        options: &ffi::HudiReadOptions,
    ) -> Result<*mut ffi::ArrowArrayStream, String> {
        guard("read_file_slice_from_paths_with_options", || {
            self.read_paths(base_file_path, log_file_paths, &options.try_into()?)
        })
    }

    pub fn read_file_slice(
        &self,
        file_slice: &HudiFileSlice,
    ) -> Result<*mut ffi::ArrowArrayStream, String> {
        guard("read_file_slice", || {
            self.read_slice(file_slice, &ReadOptions::new())
        })
    }

    pub fn read_file_slice_with_options(
        &self,
        file_slice: &HudiFileSlice,
        options: &ffi::HudiReadOptions,
    ) -> Result<*mut ffi::ArrowArrayStream, String> {
        guard("read_file_slice_with_options", || {
            self.read_slice(file_slice, &options.try_into()?)
        })
    }
}

#[derive(Debug)]
pub struct HudiFileSlice {
    inner: FileSlice,
}

pub fn new_file_slice_from_file_names(
    partition_path: &CxxString,
    base_file_name: &CxxString,
    log_file_names: &CxxVector<CxxString>,
) -> Result<Box<HudiFileSlice>, String> {
    guard("new_file_slice_from_file_names", || {
        let partition_path = cxx_str(partition_path)?;
        let base_file_name = cxx_str(base_file_name)?;
        let log_file_names = cxx_strs(log_file_names)?;

        let mut file_group = FileGroup::new_with_base_file_name(base_file_name, partition_path)
            .map_err(|e| format!("Failed to create FileGroup: {e}"))?;
        file_group
            .add_log_files_from_names(&log_file_names)
            .map_err(|e| format!("Failed to add files to FileGroup: {e}"))?;

        let (_, file_slice) =
            file_group.file_slices.iter().next().ok_or_else(|| {
                format!("Failed to get file slice from FileGroup: {file_group:?}")
            })?;

        Ok(Box::new(HudiFileSlice {
            inner: file_slice.clone(),
        }))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
    use arrow_array::{Array, Int32Array, RecordBatchReader};
    use cxx::let_cxx_string;
    use hudi_test::{QuickstartTripsTable, SampleTable};
    use std::path::Path;

    /// Takes back the stream a bridge function leaked for C++ and reads it.
    fn batch_in(raw: *mut ffi::ArrowArrayStream) -> RecordBatch {
        // SAFETY: `raw` is the one box the bridge leaks for the C++ caller,
        // reclaimed here instead.
        let stream = unsafe { Box::from_raw(raw as *mut FFI_ArrowArrayStream) };
        let reader = ArrowArrayStreamReader::try_new(*stream).unwrap();
        let schema = reader.schema();
        let batches: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
        arrow::compute::concat_batches(&schema, &batches).unwrap()
    }

    fn rows_in(raw: *mut ffi::ArrowArrayStream) -> usize {
        batch_in(raw).num_rows()
    }

    fn column_names(batch: &RecordBatch) -> Vec<String> {
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    fn file_names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    fn read_options(
        projection: &[&str],
        filters: &[(&str, &str, &str)],
        hudi_options: &[&str],
    ) -> ffi::HudiReadOptions {
        ffi::HudiReadOptions {
            projection: projection.iter().map(|s| s.to_string()).collect(),
            filters: filters
                .iter()
                .map(|(field, op, value)| ffi::HudiFilter {
                    field: field.to_string(),
                    op: op.to_string(),
                    value: value.to_string(),
                })
                .collect(),
            hudi_options: hudi_options.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn open_reader(table_path: &str) -> Box<HudiFileGroupReader> {
        let_cxx_string!(base_uri = table_path);
        let options = CxxVector::<CxxString>::new();
        new_file_group_reader_with_options(&base_uri, &options).unwrap()
    }

    /// One partition of a MOR fixture with a base file and the log file that
    /// updates it, built without going through `CxxVector`, which cannot hold
    /// strings pushed from Rust.
    fn mor_slice_with_logs() -> (String, HudiFileSlice) {
        let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();
        let partition = file_names(Path::new(&table_path))
            .into_iter()
            .find(|name| !name.starts_with('.') && Path::new(&table_path).join(name).is_dir())
            .expect("fixture has a partition");
        let names = file_names(&Path::new(&table_path).join(&partition));
        let base_file = names
            .iter()
            .find(|name| name.ends_with(".parquet") && !name.starts_with('.'))
            .expect("partition has a base file");
        let log_files: Vec<&str> = names
            .iter()
            .filter(|name| name.contains(".log."))
            .map(String::as_str)
            .collect();
        assert!(!log_files.is_empty(), "partition has log files");

        let mut file_group = FileGroup::new_with_base_file_name(base_file, &partition).unwrap();
        file_group.add_log_files_from_names(&log_files).unwrap();
        let inner = file_group.file_slices.values().next().unwrap().clone();
        (table_path, HudiFileSlice { inner })
    }

    #[test]
    fn test_guard_ok_returns_value() {
        assert_eq!(guard("op", || Ok(7)), Ok(7));
    }

    #[test]
    fn test_guard_err_passes_through_unchanged() {
        let err = guard("op", || Err::<(), _>("bad input".to_string())).unwrap_err();
        assert_eq!(err, "bad input");
    }

    #[test]
    fn test_guard_str_panic_becomes_error_naming_entry_point() {
        let err = guard("read_file_slice", || -> Result<(), String> {
            panic!("unsupported avro type")
        })
        .unwrap_err();
        assert_eq!(err, "read_file_slice panicked: unsupported avro type");
    }

    #[test]
    fn test_guard_string_payload_keeps_message() {
        let column = "map_col";
        let err = guard("op", || -> Result<(), String> {
            panic!("no support for {column}")
        })
        .unwrap_err();
        assert_eq!(err, "op panicked: no support for map_col");
    }

    #[test]
    fn test_guard_non_string_payload_is_still_an_error() {
        let err = guard("op", || -> Result<(), String> {
            std::panic::panic_any(42u8)
        })
        .unwrap_err();
        assert_eq!(err, "op panicked: non-string panic payload");
    }

    #[test]
    fn test_new_file_group_reader_with_options_unreadable_table_is_error() {
        let_cxx_string!(base_uri = "/definitely/not/a/hudi/table");
        let options = CxxVector::<CxxString>::new();
        let err = new_file_group_reader_with_options(&base_uri, &options).unwrap_err();
        assert!(
            err.starts_with("Failed to create FileGroupReader"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_new_file_slice_from_file_names_bad_base_file_name_is_error() {
        let_cxx_string!(partition_path = "");
        let_cxx_string!(base_file_name = "not-a-hudi-base-file");
        let log_file_names = CxxVector::<CxxString>::new();
        let err = new_file_slice_from_file_names(&partition_path, &base_file_name, &log_file_names)
            .unwrap_err();
        assert!(
            err.starts_with("Failed to create FileGroup"),
            "unexpected error: {err}"
        );
    }

    fn first_base_file(table_path: &str) -> String {
        file_names(Path::new(table_path))
            .into_iter()
            .find(|name| name.ends_with(".parquet") && !name.starts_with('.'))
            .expect("fixture has a base file")
    }

    /// The happy path through the same entry points the C++ caller uses, so
    /// wrapping them in the guard is shown not to change a successful read.
    #[test]
    fn test_entry_points_through_bridge_read_base_file() {
        let table_path = SampleTable::V8Nonpartitioned.path_to_cow();
        let base_file_name = first_base_file(&table_path);

        let reader = open_reader(&table_path);

        let_cxx_string!(base_file_path = base_file_name.as_str());
        let log_file_paths = CxxVector::<CxxString>::new();
        let raw = reader
            .read_file_slice_from_paths(&base_file_path, &log_file_paths)
            .unwrap();
        assert!(rows_in(raw) > 0);

        let_cxx_string!(partition_path = "");
        let file_slice =
            new_file_slice_from_file_names(&partition_path, &base_file_path, &log_file_paths)
                .unwrap();
        let raw = reader.read_file_slice(&file_slice).unwrap();
        assert!(rows_in(raw) > 0);
    }

    /// A host keeps one reader across many queries, so a panic in one read
    /// must leave the runtime and reader usable for the next.
    #[test]
    fn test_reader_usable_after_panic_inside_block_on() {
        let table_path = SampleTable::V8Nonpartitioned.path_to_cow();
        let reader = open_reader(&table_path);

        let err = guard("read", || -> Result<(), String> {
            reader.rt.block_on(async { panic!("mid-read") })
        })
        .unwrap_err();
        assert_eq!(err, "read panicked: mid-read");

        let_cxx_string!(base_file_path = first_base_file(&table_path));
        let log_file_paths = CxxVector::<CxxString>::new();
        let raw = reader
            .read_file_slice_from_paths(&base_file_path, &log_file_paths)
            .unwrap();
        assert!(rows_in(raw) > 0);
    }

    #[test]
    fn test_read_file_slice_from_paths_missing_base_file_is_error() {
        let table_path = SampleTable::V8Nonpartitioned.path_to_cow();
        let reader = open_reader(&table_path);

        let_cxx_string!(base_file_path = "missing-file.parquet");
        let log_file_paths = CxxVector::<CxxString>::new();
        let err = reader
            .read_file_slice_from_paths(&base_file_path, &log_file_paths)
            .unwrap_err();
        assert!(
            err.starts_with("Failed to read file batch"),
            "unexpected error: {err}"
        );
    }

    /// Regression for the constructor, which used to drop such an option
    /// silently. Asserted on the parser directly: `CxxVector<CxxString>`
    /// cannot be filled from Rust, so the constructor cannot be handed one.
    #[test]
    fn test_key_values_option_without_equals_is_error() {
        assert_eq!(
            key_values(["a=1", "b"]).unwrap_err(),
            "Option is not key=value: b"
        );
        assert_eq!(
            key_values(["a=1", "b=x=y"]).unwrap(),
            [("a", "1"), ("b", "x=y")]
        );
    }

    #[test]
    fn test_read_options_empty_means_defaults() {
        let options = ReadOptions::try_from(&read_options(&[], &[], &[])).unwrap();
        assert_eq!(options.projection, None);
        assert!(options.filters.is_empty());
        assert!(options.hudi_options.is_empty());
    }

    #[test]
    fn test_read_options_maps_every_field() {
        let options = ReadOptions::try_from(&read_options(
            &["id", "name"],
            &[("id", ">", "5"), ("name", "NOT IN", "a,b")],
            &["hoodie.read.stream.batch_size=10"],
        ))
        .unwrap();
        assert_eq!(
            options.projection.as_deref(),
            Some(&["id".to_string(), "name".to_string()][..])
        );
        assert_eq!(options.filters.len(), 2);
        assert_eq!(
            options.hudi_options.get("hoodie.read.stream.batch_size"),
            Some(&"10".to_string())
        );
    }

    #[test]
    fn test_read_options_bad_operator_is_error() {
        let err = ReadOptions::try_from(&read_options(&[], &[("id", "~", "5")], &[])).unwrap_err();
        assert!(err.starts_with("Invalid filter"), "unexpected error: {err}");
    }

    #[test]
    fn test_read_options_option_without_equals_is_error() {
        let err =
            ReadOptions::try_from(&read_options(&[], &[], &["hoodie.read.stream.batch_size"]))
                .unwrap_err();
        assert_eq!(
            err,
            "Option is not key=value: hoodie.read.stream.batch_size"
        );
    }

    #[test]
    fn test_read_file_slice_with_options_applies_projection_and_filter() {
        let (table_path, slice) = mor_slice_with_logs();
        let reader = open_reader(&table_path);

        let all = batch_in(reader.read_file_slice(&slice).unwrap());
        let ids = all.column_by_name("id").unwrap();
        let ids = ids.as_any().downcast_ref::<Int32Array>().unwrap();
        let max_id = ids.iter().flatten().max().unwrap();

        let options = read_options(&["id", "name"], &[("id", "=", &max_id.to_string())], &[]);
        let batch = batch_in(
            reader
                .read_file_slice_with_options(&slice, &options)
                .unwrap(),
        );
        assert_eq!(column_names(&batch), ["id", "name"]);
        assert_eq!(batch.num_rows(), 1);
    }

    /// Read-optimized mode never opens log files, so a slice whose only log
    /// file does not exist reads fine with it and fails without it.
    #[test]
    fn test_read_file_slice_with_options_read_optimized_skips_logs() {
        let table_path = SampleTable::V8Nonpartitioned.path_to_cow();
        let base_file = first_base_file(&table_path);
        let (file_id, rest) = base_file.split_once('_').unwrap();
        let commit = rest
            .rsplit('_')
            .next()
            .unwrap()
            .trim_end_matches(".parquet");
        let mut file_group = FileGroup::new_with_base_file_name(&base_file, "").unwrap();
        file_group
            .add_log_files_from_names([format!(".{file_id}_{commit}.log.1_0-0-0")])
            .unwrap();
        let slice = HudiFileSlice {
            inner: file_group.file_slices.values().next().unwrap().clone(),
        };
        let reader = open_reader(&table_path);

        let read_optimized = read_options(&[], &[], &["hoodie.read.use.read_optimized.mode=true"]);
        let raw = reader
            .read_file_slice_with_options(&slice, &read_optimized)
            .unwrap();
        assert!(rows_in(raw) > 0);

        let err = reader
            .read_file_slice_with_options(&slice, &read_options(&[], &[], &[]))
            .unwrap_err();
        assert!(
            err.starts_with("Failed to read file slice") && err.contains(".log.1_0-0-0"),
            "unexpected error: {err}"
        );
    }

    /// The paths entry point is the one a C++ caller uses with a split, so it
    /// is covered with options too, not only its slice twin.
    #[test]
    fn test_read_file_slice_from_paths_with_options_applies_projection_and_filter() {
        let table_path = SampleTable::V8Nonpartitioned.path_to_cow();
        let reader = open_reader(&table_path);
        let_cxx_string!(base_file_path = first_base_file(&table_path));
        let log_file_paths = CxxVector::<CxxString>::new();

        let all = batch_in(
            reader
                .read_file_slice_from_paths(&base_file_path, &log_file_paths)
                .unwrap(),
        );
        let options = read_options(&["name"], &[("name", "!=", "")], &[]);
        let batch = batch_in(
            reader
                .read_file_slice_from_paths_with_options(&base_file_path, &log_file_paths, &options)
                .unwrap(),
        );
        assert_eq!(column_names(&batch), ["name"]);
        assert_eq!(batch.num_rows(), all.num_rows());
    }
}
