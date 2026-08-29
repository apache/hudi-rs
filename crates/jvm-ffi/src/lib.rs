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
//! Crate `hudi-core`.
//!
//! # The [config] module is responsible for managing configurations.
//!

//! A plain C ABI over hudi-rs, so a JVM can read a file slice.
//!
//! The `cpp` crate already exposes the same reads, but `cxx` generates
//! C++-mangled symbols and `CxxString`/`CxxVector` arguments, neither of which a
//! JVM can bind to. This crate restates those calls in C: null-terminated
//! strings in, opaque handles and an `ArrowArrayStream` out. It holds no reading
//! logic of its own.
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

use arrow::array::RecordBatchIterator;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use hudi::file_group::reader::FileGroupReader;
use hudi::table::{ReadOptions, Table};

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

/// Run `body`, turning any error or panic into a null return plus a message.
///
/// Every exported function goes through this. A panic that unwinds across the C
/// ABI aborts the process, so catching it here is what keeps a Rust bug from
/// killing the JVM that called us.
fn guard<T>(what: &str, body: impl FnOnce() -> Result<*mut T, String>) -> *mut T {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(ptr)) => ptr,
        Ok(Err(message)) => {
            set_error(format!("{what}: {message}"));
            std::ptr::null_mut()
        }
        Err(payload) => {
            let detail = payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".to_string());
            set_error(format!("{what} panicked: {detail}"));
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

/// A reader plus the runtime its async calls are driven on.
///
/// The runtime is owned here rather than created per read: a current-thread
/// runtime is cheap to build but the reader holds object-store clients whose
/// connection pools should outlive one call.
pub struct HudiFfiReader {
    inner: FileGroupReader,
    runtime: tokio::runtime::Runtime,
}

/// A table handle, for the metadata read.
///
/// Separate from [`HudiFfiReader`] because the two wrap different things: a file
/// group reader is handed a slice, while a metadata read resolves its own.
pub struct HudiFfiTable {
    inner: Table,
    runtime: tokio::runtime::Runtime,
}

/// Open the table at `base_uri` for a metadata read.
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

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("failed to build a tokio runtime: {e}"))?;
        let inner = runtime
            .block_on(Table::new_with_options(base_uri, options))
            .map_err(|e| format!("failed to open {base_uri}: {e}"))?;
        Ok(Box::into_raw(Box::new(HudiFfiTable { inner, runtime })))
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

        let batch = table
            .runtime
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

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("failed to build a tokio runtime: {e}"))?;
        let inner = runtime
            .block_on(FileGroupReader::new_with_options(base_uri, options))
            .map_err(|e| format!("failed to open {base_uri}: {e}"))?;
        Ok(Box::into_raw(Box::new(HudiFfiReader { inner, runtime })))
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

        let batch = reader
            .runtime
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
/// `stream` must come from [`hudi_ffi_read_slice`] and be released once only.
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
        }
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
}
