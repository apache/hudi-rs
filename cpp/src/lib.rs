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

    extern "Rust" {
        type HudiFileGroupReader;
        /// Opens a reader for the table at `base_uri` with `key=value` options.
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

        let opt_vec: Vec<(&str, &str)> = cxx_strs(options)?
            .into_iter()
            .filter_map(|opt| opt.split_once('='))
            .collect();

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
    pub fn read_file_slice_from_paths(
        &self,
        base_file_path: &CxxString,
        log_file_paths: &CxxVector<CxxString>,
    ) -> Result<*mut ffi::ArrowArrayStream, String> {
        guard("read_file_slice_from_paths", || {
            let base_file_path = cxx_str(base_file_path)?;
            let log_file_paths = cxx_strs(log_file_paths)?;

            let record_batch = self
                .rt
                .block_on(self.inner.read_file_slice_from_paths(
                    base_file_path,
                    log_file_paths,
                    &ReadOptions::new(),
                ))
                .map_err(|e| format!("Failed to read file batch: {e}"))?;
            let schema = record_batch.schema();

            Ok(create_raw_pointer_for_record_batches(
                vec![record_batch],
                schema,
            ))
        })
    }

    pub fn read_file_slice(
        &self,
        file_slice: &HudiFileSlice,
    ) -> Result<*mut ffi::ArrowArrayStream, String> {
        guard("read_file_slice", || {
            let record_batch = self
                .rt
                .block_on(
                    self.inner
                        .read_file_slice(&file_slice.inner, &ReadOptions::new()),
                )
                .map_err(|e| format!("Failed to read file slice: {e}"))?;
            let schema = record_batch.schema();

            Ok(create_raw_pointer_for_record_batches(
                vec![record_batch],
                schema,
            ))
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
    use cxx::let_cxx_string;
    use hudi_test::SampleTable;

    /// Takes back the stream a bridge function leaked for C++ and reads it.
    fn rows_in(raw: *mut ffi::ArrowArrayStream) -> usize {
        // SAFETY: `raw` is the one box the bridge leaks for the C++ caller,
        // reclaimed here instead.
        let stream = unsafe { Box::from_raw(raw as *mut FFI_ArrowArrayStream) };
        ArrowArrayStreamReader::try_new(*stream)
            .unwrap()
            .map(|batch| batch.unwrap().num_rows())
            .sum()
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
        let mut base_files: Vec<String> = std::fs::read_dir(table_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".parquet") && !name.starts_with('.'))
            .collect();
        base_files.sort();
        base_files
            .into_iter()
            .next()
            .expect("fixture has a base file")
    }

    /// The happy path through the same entry points the C++ caller uses, so
    /// wrapping them in the guard is shown not to change a successful read.
    #[test]
    fn test_entry_points_through_bridge_read_base_file() {
        let table_path = SampleTable::V8Nonpartitioned.path_to_cow();
        let base_file_name = first_base_file(&table_path);

        let_cxx_string!(base_uri = table_path.as_str());
        let options = CxxVector::<CxxString>::new();
        let reader = new_file_group_reader_with_options(&base_uri, &options).unwrap();

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
        let_cxx_string!(base_uri = table_path.as_str());
        let options = CxxVector::<CxxString>::new();
        let reader = new_file_group_reader_with_options(&base_uri, &options).unwrap();

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
        let_cxx_string!(base_uri = table_path.as_str());
        let options = CxxVector::<CxxString>::new();
        let reader = new_file_group_reader_with_options(&base_uri, &options).unwrap();

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
}
