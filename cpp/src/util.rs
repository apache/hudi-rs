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
use crate::ffi;
use arrow::datatypes::SchemaRef;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, RecordBatchIterator};

pub fn create_raw_pointer_for_record_batches(
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
) -> *mut ffi::ArrowArrayStream {
    let batches = batches.into_iter().map(Ok);
    let batch_iterator = RecordBatchIterator::new(batches, schema);
    let ffi_array_stream = FFI_ArrowArrayStream::new(Box::new(batch_iterator));
    // Heap-allocate and leak into a raw pointer.  free_arrow_stream() below
    // is the only correct way to reclaim this allocation.
    let raw_ptr = Box::into_raw(Box::new(ffi_array_stream));
    raw_ptr as *mut ffi::ArrowArrayStream
}

/// Free an `ArrowArrayStream` that was heap-allocated by
/// [`create_raw_pointer_for_record_batches`] via `Box::into_raw`.
///
/// This reclaims the memory through Rust's own allocator, making it safe
/// regardless of which global allocator the Rust library was compiled with.
/// The caller must not use `ptr` after this call.
pub unsafe fn free_arrow_stream(ptr: *mut ffi::ArrowArrayStream) {
    if !ptr.is_null() {
        // Reconstruct the original Box<FFI_ArrowArrayStream>.  This is the
        // exact type that Box::into_raw() produced in create_raw_pointer_for_record_batches();
        // the pointer was only cast to *mut ffi::ArrowArrayStream (layout-compatible
        // CXX opaque type) for transport across the FFI boundary.
        // Dropping the Box invokes FFI_ArrowArrayStream's Drop impl (which calls
        // the Arrow release callback) and then frees the heap allocation.
        unsafe { drop(Box::from_raw(ptr as *mut FFI_ArrowArrayStream)) };
    }
}
