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
use hudi::file_group::file_slice::FileSlice;
use hudi::file_group::reader::FileGroupReader;
use hudi::file_group::FileGroup;

#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        include!("arrow/c/abi.h");

        type ArrowArrayStream;
    }

    extern "Rust" {
        type HudiFileGroupReader;
        fn new_file_group_reader_with_options(
            base_uri: &CxxString,
            options: &CxxVector<CxxString>,
        ) -> Box<HudiFileGroupReader>;

        type HudiFileSlice;
        fn new_file_slice_from_file_names(
            partition_path: &CxxString,
            base_file_name: &CxxString,
            log_file_names: &CxxVector<CxxString>,
        ) -> Box<HudiFileSlice>;

        fn read_file_slice_by_base_file_path(
            self: &HudiFileGroupReader,
            relative_path: &CxxString,
        ) -> *mut ArrowArrayStream;

        fn read_file_slice(
            self: &HudiFileGroupReader,
            file_slice: &HudiFileSlice,
        ) -> *mut ArrowArrayStream;
    }
}

pub struct HudiFileGroupReader {
    inner: FileGroupReader,
}

pub fn new_file_group_reader_with_options(
    base_uri: &CxxString,
    options: &CxxVector<CxxString>,
) -> Box<HudiFileGroupReader> {
    let base_uri = base_uri
        .to_str()
        .expect("Failed to convert CxxString to str: Invalid UTF-8 sequence");

    let mut opt_vec = Vec::new();
    for opt in options.iter() {
        let opt_str = opt
            .to_str()
            .expect("Failed to convert CxxString to str: Invalid UTF-8 sequence");
        if let Some((key, value)) = opt_str.split_once('=') {
            opt_vec.push((key, value))
        }
    }

    let reader = FileGroupReader::new_with_options(base_uri, opt_vec)
        .expect("Failed to create FileGroupReader with options");
    let reader_wrapper = HudiFileGroupReader { inner: reader };
    Box::new(reader_wrapper)
}

impl HudiFileGroupReader {
    pub fn read_file_slice_by_base_file_path(
        &self,
        relative_path: &CxxString,
    ) -> *mut ffi::ArrowArrayStream {
        let relative_path = relative_path
            .to_str()
            .expect("Failed to convert CxxString to str: Invalid UTF-8 sequence");

        let record_batch = self
            .inner
            .read_file_slice_by_base_file_path_blocking(relative_path)
            .expect("Failed to read file batch");
        let schema = record_batch.schema();

        create_raw_pointer_for_record_batches(vec![record_batch], schema)
    }

    pub fn read_file_slice(&self, file_slice: &HudiFileSlice) -> *mut ffi::ArrowArrayStream {
        let record_batch = self
            .inner
            .read_file_slice_blocking(&file_slice.inner)
            .expect("Failed to read file slice");
        let schema = record_batch.schema();

        create_raw_pointer_for_record_batches(vec![record_batch], schema)
    }
}

pub struct HudiFileSlice {
    inner: FileSlice,
}

pub fn new_file_slice_from_file_names(
    partition_path: &CxxString,
    base_file_name: &CxxString,
    log_file_names: &CxxVector<CxxString>,
) -> Box<HudiFileSlice> {
    let partition_path = partition_path
        .to_str()
        .expect("Failed to convert CxxString to str: Invalid UTF-8 sequence");
    let base_file_name = base_file_name
        .to_str()
        .expect("Failed to convert CxxString to str: Invalid UTF-8 sequence");

    let log_file_names = log_file_names
        .iter()
        .map(|name| {
            name.to_str()
                .expect("Failed to convert CxxString to str: Invalid UTF-8 sequence")
        })
        .collect::<Vec<_>>();

    let mut file_group = FileGroup::new_with_base_file_name(base_file_name, partition_path)
        .expect("Failed to create FileGroup");
    file_group
        .add_log_files_from_names(&log_file_names)
        .expect("Failed to add files to FileGroup");

    let (_, file_slice) = file_group
        .file_slices
        .iter()
        .next()
        .expect("Failed to get file slice from FileGroup");

    // todo: add api to create file slice from names to avoid cloning
    let file_slice_wrapper = HudiFileSlice {
        inner: file_slice.clone(),
    };

    Box::new(file_slice_wrapper)
}
