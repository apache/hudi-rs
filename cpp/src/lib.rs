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
use futures::TryStreamExt;
use hudi::file_group::reader::FileGroupReader;
use hudi::table::ReadOptions;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

fn rt() -> &'static Runtime {
    static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();
    TOKIO_RT.get_or_init(|| Runtime::new().expect("Failed to create a tokio runtime."))
}

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

        fn read_file_slice_by_paths(
            self: &HudiFileGroupReader,
            base_file_path: &CxxString,
            log_file_paths: &CxxVector<CxxString>,
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
    pub fn read_file_slice_by_paths(
        &self,
        base_file_path: &CxxString,
        log_file_paths: &CxxVector<CxxString>,
    ) -> *mut ffi::ArrowArrayStream {
        let base_file_path = base_file_path
            .to_str()
            .expect("Failed to convert CxxString to str: Invalid UTF-8 sequence");

        let log_file_paths: Vec<String> = log_file_paths
            .iter()
            .map(|path| {
                path.to_str()
                    .expect("Failed to convert CxxString to str: Invalid UTF-8 sequence")
                    .to_string()
            })
            .collect();

        let options = ReadOptions::new();
        let stream = rt()
            .block_on(
                self.inner
                    .read_file_slice_by_paths(base_file_path, log_file_paths, &options),
            )
            .expect("Failed to read file slice by paths");

        let batches: Vec<_> = rt()
            .block_on(stream.try_collect())
            .expect("Failed to collect record batches");

        if batches.is_empty() {
            panic!("No record batches read from file slice");
        }

        let schema = batches[0].schema();
        create_raw_pointer_for_record_batches(batches, schema)
    }
}
