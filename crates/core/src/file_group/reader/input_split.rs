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

//! Mirrors `org.apache.hudi.common.table.read.InputSplit`.
//!
//! Describes the data to be read from a file group: an optional base file,
//! a list of log files, and the partition path context.

/// Describes the input data for a file group read.
///
/// Carries the base file (if any), the list of log files to scan,
/// the partition path, and the byte range to read from the base file.
#[derive(Debug, Clone)]
pub struct InputSplit {
    /// Path to the base file (relative to table root), if present.
    pub base_file_path: Option<String>,

    /// Commit time of the base file, if present.
    pub base_file_commit_time: Option<String>,

    /// Relative paths to log files to scan.
    pub log_file_paths: Vec<String>,

    /// Partition path for this file group (e.g. "year=2024/month=01").
    pub partition_path: String,

    /// Byte offset to start reading from in the base file.
    pub start: i64,

    /// Number of bytes to read from the base file.
    pub length: i64,
}

impl InputSplit {
    pub fn new(
        base_file_path: Option<String>,
        base_file_commit_time: Option<String>,
        log_file_paths: Vec<String>,
        partition_path: String,
    ) -> Self {
        Self {
            base_file_path,
            base_file_commit_time,
            log_file_paths,
            partition_path,
            start: 0,
            length: -1,
        }
    }

    /// Returns true if there are log files that need to be merged with the base file.
    pub fn has_log_files(&self) -> bool {
        !self.log_file_paths.is_empty()
    }

    /// Returns true if there is no base file and no log files.
    pub fn has_no_records_to_merge(&self) -> bool {
        !self.has_log_files()
    }
}
