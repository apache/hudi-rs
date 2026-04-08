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

use crate::file_group::log_file::LogFile;
use std::str::FromStr;

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
        // Sort log files ascending by deltaCommitTime → logVersion → writeToken,
        // matching Java's InputSplit which applies HoodieLogFile.getLogFileComparator().
        // The C++ side may send them in descending order (from FileSlice's reverse TreeSet).
        let log_file_paths = Self::sort_log_file_paths(log_file_paths);
        Self {
            base_file_path,
            base_file_commit_time,
            log_file_paths,
            partition_path,
            start: 0,
            length: -1,
        }
    }

    /// Sort log file paths ascending by deltaCommitTime → logVersion → writeToken.
    ///
    /// Mirrors Java's `InputSplit` constructor which sorts via
    /// `HoodieLogFile.getLogFileComparator()`.
    fn sort_log_file_paths(mut paths: Vec<String>) -> Vec<String> {
        if paths.len() <= 1 {
            return paths;
        }
        paths.sort_by(|a, b| {
            let name_a = a.rsplit('/').next().unwrap_or(a);
            let name_b = b.rsplit('/').next().unwrap_or(b);
            match (LogFile::from_str(name_a), LogFile::from_str(name_b)) {
                (Ok(lf_a), Ok(lf_b)) => lf_a.cmp(&lf_b),
                _ => a.cmp(b), // fallback to lexicographic if parsing fails
            }
        });
        paths
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Java's InputSplit sorts log files ascending by deltaCommitTime → logVersion → writeToken
    /// (via HoodieLogFile.getLogFileComparator()). The C++ side sends them in descending order
    /// (from FileSlice's reverse TreeSet). InputSplit must re-sort to ascending.
    #[test]
    fn test_log_file_paths_sorted_ascending() {
        // Log files in DESCENDING deltaCommitTime order (as C++ sends them)
        let log_files = vec![
            ".72d44234-0_20260408194651210.log.1_0-272-505".to_string(), // NEWER
            ".72d44234-0_20260408194649548.log.1_0-254-473".to_string(), // OLDER
        ];

        let split = InputSplit::new(
            Some("base.parquet".to_string()),
            None,
            log_files,
            String::new(),
        );

        // Must be re-sorted to ASCENDING deltaCommitTime
        assert_eq!(split.log_file_paths.len(), 2);
        assert!(
            split.log_file_paths[0].contains("20260408194649548"),
            "First log file should be the OLDER commit, got: {}",
            split.log_file_paths[0]
        );
        assert!(
            split.log_file_paths[1].contains("20260408194651210"),
            "Second log file should be the NEWER commit, got: {}",
            split.log_file_paths[1]
        );
    }

    /// Same deltaCommitTime, different logVersions — should sort by version ascending.
    #[test]
    fn test_log_file_paths_sorted_by_version() {
        let log_files = vec![
            ".fileId-0_20260408194649548.log.3_0-100-200".to_string(), // version 3
            ".fileId-0_20260408194649548.log.1_0-100-200".to_string(), // version 1
            ".fileId-0_20260408194649548.log.2_0-100-200".to_string(), // version 2
        ];

        let split = InputSplit::new(None, None, log_files, String::new());

        assert!(split.log_file_paths[0].contains(".log.1_"));
        assert!(split.log_file_paths[1].contains(".log.2_"));
        assert!(split.log_file_paths[2].contains(".log.3_"));
    }

    /// With partition path prefix — sort should work on the file name portion.
    #[test]
    fn test_log_file_paths_sorted_with_partition_prefix() {
        let log_files = vec![
            "year=2024/month=01/.fileId-0_20260408194651210.log.1_0-272-505".to_string(),
            "year=2024/month=01/.fileId-0_20260408194649548.log.1_0-254-473".to_string(),
        ];

        let split = InputSplit::new(None, None, log_files, "year=2024/month=01".to_string());

        assert!(split.log_file_paths[0].contains("20260408194649548"));
        assert!(split.log_file_paths[1].contains("20260408194651210"));
    }

    /// Empty or single log file — no sorting needed, should not crash.
    #[test]
    fn test_log_file_paths_empty_and_single() {
        let split_empty = InputSplit::new(None, None, vec![], String::new());
        assert!(split_empty.log_file_paths.is_empty());

        let split_single = InputSplit::new(
            None,
            None,
            vec![".fileId-0_20260408194649548.log.1_0-254-473".to_string()],
            String::new(),
        );
        assert_eq!(split_single.log_file_paths.len(), 1);
    }
}
