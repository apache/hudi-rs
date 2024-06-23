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

use anyhow::anyhow;
use anyhow::Result;
use std::path::Path;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FileMetadata {
    pub path: String,
    pub name: String,
    pub size: usize,
    pub num_records: Option<usize>,
}

impl FileMetadata {
    pub fn new(path: String, name: String, size: usize) -> FileMetadata {
        FileMetadata {
            path,
            name,
            size,
            num_records: None,
        }
    }
}

pub fn split_filename(filename: &str) -> Result<(String, String)> {
    let path = Path::new(filename);

    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("No file stem found"))?
        .to_string();

    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default()
        .to_string();

    Ok((stem, extension))
}
