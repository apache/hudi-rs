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
        ) -> Result<Box<HudiFileGroupReader>>;

        type HudiFileSlice;
        fn new_file_slice_from_file_names(
            partition_path: &CxxString,
            base_file_name: &CxxString,
            log_file_names: &CxxVector<CxxString>,
        ) -> Result<Box<HudiFileSlice>>;

        fn read_file_slice_from_paths(
            self: &HudiFileGroupReader,
            base_file_path: &CxxString,
            log_file_paths: &CxxVector<CxxString>,
        ) -> Result<*mut ArrowArrayStream>;

        fn read_file_slice(
            self: &HudiFileGroupReader,
            file_slice: &HudiFileSlice,
        ) -> Result<*mut ArrowArrayStream>;
    }
}

pub struct HudiFileGroupReader {
    inner: FileGroupReader,
    rt: tokio::runtime::Runtime,
}

pub fn new_file_group_reader_with_options(
    base_uri: &CxxString,
    options: &CxxVector<CxxString>,
) -> std::result::Result<Box<HudiFileGroupReader>, String> {
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
    Ok(Box::new(HudiFileGroupReader { inner: reader, rt }))
}

impl HudiFileGroupReader {
    pub fn read_file_slice_from_paths(
        &self,
        base_file_path: &CxxString,
        log_file_paths: &CxxVector<CxxString>,
    ) -> std::result::Result<*mut ffi::ArrowArrayStream, String> {
        let base_file_path = base_file_path
            .to_str()
            .map_err(|e| format!("Failed to convert CxxString to str: {e}"))?;

        let log_file_paths = log_file_paths
            .iter()
            .map(|p| {
                p.to_str()
                    .map(String::from)
                    .map_err(|e| format!("Failed to convert CxxString to str: {e}"))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

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
    }

    pub fn read_file_slice(
        &self,
        file_slice: &HudiFileSlice,
    ) -> std::result::Result<*mut ffi::ArrowArrayStream, String> {
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
    }
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
