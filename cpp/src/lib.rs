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

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatchIterator;
use hudi::config::util::empty_filters;
use hudi::table::Table;
use hudi_test::QuickstartTripsTable;

#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        include!("arrow/c/abi.h");

        type ArrowArrayStream;
    }

    extern "Rust" {
        fn read_file_slice() -> *mut ArrowArrayStream;
    }
}

pub fn read_file_slice() -> *mut ffi::ArrowArrayStream {
    let base_url = QuickstartTripsTable::V6Trips8I1U.url_to_mor_avro();
    let hudi_table = Table::new_blocking(base_url.path()).unwrap();

    let batches = hudi_table.read_snapshot_blocking(empty_filters()).unwrap();
    let schema = batches[0].schema();

    let batch_iterator = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    // Convert to FFI_ArrowArrayStream and get raw pointer
    let ffi_array_stream = FFI_ArrowArrayStream::new(Box::new(batch_iterator));
    let raw_ptr = Box::into_raw(Box::new(ffi_array_stream));

    raw_ptr as *mut ffi::ArrowArrayStream
}
