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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FileMetadata {
    /// File name
    pub name: String,

    /// Size in bytes on storage (compressed, on-disk)
    pub size: u64,

    /// Estimated uncompressed size in bytes in memory.
    /// Inferred from on-disk size and a sampled compression ratio.
    pub byte_size: i64,

    /// Estimated number of records in the file.
    /// Inferred from on-disk size and a sampled average row size.
    pub num_records: i64,
}

impl FileMetadata {
    pub fn new(name: impl Into<String>, size: u64) -> Self {
        Self {
            name: name.into(),
            size,
            byte_size: 0,
            num_records: 0,
        }
    }
}
