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

use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use tempfile::tempdir;
use url::Url;

pub mod utils;

pub fn extract_test_table(zip_path: &Path) -> PathBuf {
    let target_dir = tempdir().unwrap().path().to_path_buf();
    let archive = fs::read(zip_path).unwrap();
    zip_extract::extract(Cursor::new(archive), &target_dir, true).unwrap();
    target_dir
}

pub enum TestTable {
    V6ComplexkeygenHivestyle,
    V6Empty,
    V6Nonpartitioned,
}

impl TestTable {
    pub fn zip_path(&self) -> Box<Path> {
        let dir = env!("CARGO_MANIFEST_DIR");
        let data_path = Path::new(dir).join("data/tables");
        match self {
            Self::V6ComplexkeygenHivestyle => data_path
                .join("v6_complexkeygen_hivestyle.zip")
                .into_boxed_path(),
            Self::V6Empty => data_path.join("v6_empty.zip").into_boxed_path(),
            Self::V6Nonpartitioned => data_path.join("v6_nonpartitioned.zip").into_boxed_path(),
        }
    }

    pub fn path(&self) -> String {
        let zip_path = self.zip_path();
        match self {
            Self::V6ComplexkeygenHivestyle => extract_test_table(&zip_path)
                .join("v6_complexkeygen_hivestyle")
                .to_str()
                .unwrap()
                .to_string(),
            Self::V6Empty => extract_test_table(&zip_path)
                .join("v6_empty")
                .to_str()
                .unwrap()
                .to_string(),
            Self::V6Nonpartitioned => extract_test_table(&zip_path)
                .join("v6_nonpartitioned")
                .to_str()
                .unwrap()
                .to_string(),
        }
    }

    pub fn url(&self) -> Url {
        Url::from_file_path(self.path()).unwrap()
    }
}
