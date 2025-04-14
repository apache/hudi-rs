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
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatchIterator;
use hudi::config::util::empty_filters;
use hudi::error::Result as HudiResult;
use hudi::file_group::reader::FileGroupReader;
use hudi::table::Table;
use hudi_test::QuickstartTripsTable;
use cxx::{CxxVector, CxxString};

#[allow(clippy::result_large_err)]
#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        include!("arrow/c/abi.h");

        type ArrowArrayStream;
    }

    extern "Rust" {
        fn read_file_slice() -> *mut ArrowArrayStream;
        type HudiFileGroupReader;

        fn new_file_group_reader_with_options(
            base_uri: &str,
            options: &CxxVector<CxxString>,
        ) -> Result<Box<HudiFileGroupReader>>;

        fn read_file_slice_by_base_file_path(
            self: &HudiFileGroupReader,
            relative_path: &str,
        ) -> Result<*mut ArrowArrayStream>;
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

pub struct HudiFileGroupReader {
    inner: FileGroupReader,
}

pub fn new_file_group_reader_with_options(
    base_uri: &str,
    options: &cxx::Vector<cxx::CxxString>,
) -> HudiResult<Box<HudiFileGroupReader>> {
    let mut opt_vec = Vec::new();
    for opt in options.iter() {
        let opt_str = opt.to_str()?;
        if let Some((key, value)) = opt_str.split_once('=') {
            opt_vec.push((key, value))
        }
    }

    let reader = FileGroupReader::new_with_options(base_uri, opt_vec)?;
    let reader_wrapper = HudiFileGroupReader { inner: reader };
    Ok(Box::new(reader_wrapper))
}

impl HudiFileGroupReader {
    pub fn read_file_slice_by_base_file_path(
        &self,
        relative_path: &str,
    ) -> HudiResult<*mut ffi::ArrowArrayStream> {
        let record_batch = self
            .inner
            .read_file_slice_by_base_file_path_blocking(relative_path)?;
        let schema = record_batch.schema();

        Ok(create_raw_pointer_for_record_batches(
            vec![record_batch],
            schema,
        ))
    }
}
