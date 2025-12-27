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
//! This module is for File Group related models and APIs.
//!
//! A set of data/base files + set of log files, that make up a unit for all operations.

pub mod base_file;
pub mod builder;
pub mod file_slice;
pub mod log_file;
pub mod reader;
pub mod record_batches;

use crate::error::CoreError;
use crate::file_group::base_file::BaseFile;
use crate::file_group::log_file::LogFile;
use crate::Result;
use file_slice::FileSlice;
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

/// A [FileGroup] contains multiple [FileSlice]s within a partition,
/// and it can be uniquely identified by `file_id` across the table.
///
/// The [FileSlice]s are keyed by `commit_timestamp` (request/base instant time).
/// This is consistent for both v6 and v8+ tables.
#[derive(Clone, Debug)]
pub struct FileGroup {
    pub file_id: String,
    pub partition_path: String,
    pub file_slices: BTreeMap<String, FileSlice>,
}

impl PartialEq for FileGroup {
    fn eq(&self, other: &Self) -> bool {
        self.file_id == other.file_id && self.partition_path == other.partition_path
    }
}

impl Eq for FileGroup {}

impl Hash for FileGroup {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_id.hash(state);
        self.partition_path.hash(state);
    }
}

impl fmt::Display for FileGroup {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(
            format!(
                "File Group: partition={}, id={}",
                &self.partition_path, &self.file_id
            )
            .as_str(),
        )
    }
}

impl FileGroup {
    /// Create a new [FileGroup] with the given `file_id` and `partition_path` with no [FileSlice]s.
    pub fn new(file_id: String, partition_path: String) -> Self {
        Self {
            file_id,
            partition_path,
            file_slices: BTreeMap::new(),
        }
    }

    /// Create a new [FileGroup] with a [BaseFile]'s file name.
    pub fn new_with_base_file_name(file_name: &str, partition_path: &str) -> Result<Self> {
        let base_file = BaseFile::from_str(file_name)?;
        let file_id = base_file.file_id.clone();
        let mut file_group = Self::new(file_id, partition_path.to_string());
        file_group.add_base_file(base_file)?;
        Ok(file_group)
    }

    /// Merge another file group into this one.
    ///
    /// The file slices are merged by their keys (commit_timestamp / base instant time).
    pub fn merge(&mut self, other: &FileGroup) -> Result<()> {
        if self != other {
            return Err(CoreError::FileGroup(format!(
                "Cannot merge different file groups: {self} and {other}",
            )));
        }

        for (key, other_file_slice) in other.file_slices.iter() {
            if let Some(existing_file_slice) = self.file_slices.get_mut(key) {
                existing_file_slice.merge(other_file_slice)?;
            } else {
                self.file_slices
                    .insert(key.clone(), other_file_slice.clone());
            }
        }

        Ok(())
    }

    /// Add a [BaseFile] based on the file name to the corresponding [FileSlice] in the [FileGroup].
    pub fn add_base_file_from_name(&mut self, file_name: &str) -> Result<&Self> {
        let base_file = BaseFile::from_str(file_name)?;
        self.add_base_file(base_file)
    }

    /// Add a [BaseFile] to the corresponding [FileSlice] in the [FileGroup].
    ///
    /// The file slice is keyed by `commit_timestamp` (request/base instant time).
    /// This is consistent for both v6 and v8+ tables.
    pub fn add_base_file(&mut self, base_file: BaseFile) -> Result<&Self> {
        let key = base_file.commit_timestamp.clone();

        if self.file_slices.contains_key(&key) {
            Err(CoreError::FileGroup(format!(
                "Timestamp {} is already present in File Group {}",
                key, self.file_id
            )))
        } else {
            self.file_slices
                .insert(key, FileSlice::new(base_file, self.partition_path.clone()));
            Ok(self)
        }
    }

    /// Add multiple [BaseFile]s to the corresponding [FileSlice]s in the [FileGroup].
    pub fn add_base_files<I>(&mut self, base_files: I) -> Result<&Self>
    where
        I: IntoIterator<Item = BaseFile>,
    {
        for base_file in base_files {
            self.add_base_file(base_file)?;
        }
        Ok(self)
    }

    /// Add a [LogFile] based on the file name to the corresponding [FileSlice] in the [FileGroup].
    pub fn add_log_file_from_name(&mut self, file_name: &str) -> Result<&Self> {
        let log_file = LogFile::from_str(file_name)?;
        self.add_log_file(log_file)
    }

    /// Add multiple [LogFile]s based on the file names to the corresponding [FileSlice]s in the
    /// [FileGroup].
    pub fn add_log_files_from_names<I, S>(&mut self, log_file_names: I) -> Result<&Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for file_name in log_file_names {
            self.add_log_file_from_name(file_name.as_ref())?;
        }
        Ok(self)
    }

    /// Add a [LogFile] to the corresponding [FileSlice] in the [FileGroup].
    ///
    /// File slices are keyed by `commit_timestamp` (request/base instant time).
    /// The log file association logic:
    ///
    /// - **With completion_timestamp (v8+ tables)**: Find the file slice with the largest
    ///   `commit_timestamp` (base instant time) that is <= log's `completion_timestamp`.
    ///
    /// - **Without completion_timestamp (v6 tables)**: Use exact matching or range lookup
    ///   based on log timestamp.
    ///
    /// TODO: support adding log files to file group without base files.
    pub fn add_log_file(&mut self, log_file: LogFile) -> Result<&Self> {
        // If log file has completion_timestamp, use completion-time-based association
        // File slices are keyed by commit_timestamp (base instant time)
        // Find the largest base instant time <= log's completion time
        if let Some(log_completion_time) = &log_file.completion_timestamp {
            // Find file slice with largest base instant time
            // (commit_timestamp) <= log's completion time
            if let Some((_, file_slice)) = self
                .file_slices
                .range_mut(..=log_completion_time.clone())
                .next_back()
            {
                file_slice.log_files.insert(log_file);
                return Ok(self);
            }

            // No file slice with base instant time <= log's completion time found.
            // This means the log file completed before any base file's request time.
            // TODO: Support log files without base files in a future priority task.
            return Err(CoreError::FileGroup(format!(
                "No suitable FileSlice found for log file with completion_timestamp {} in File Group {}. \
                Log file completed before any base file's request time.",
                log_completion_time, self.file_id
            )));
        }

        // No completion_timestamp: use base instant timestamp-based association (v6 tables)
        // Find the FileSlice with the largest base instant time <= log's timestamp
        let log_timestamp = log_file.timestamp.as_str();
        if let Some((_, file_slice)) = self
            .file_slices
            .range_mut(..=log_timestamp.to_string())
            .next_back()
        {
            file_slice.log_files.insert(log_file);
            return Ok(self);
        }

        Err(CoreError::FileGroup(format!(
            "No suitable FileSlice found for log file with timestamp {} in File Group {}",
            log_timestamp, self.file_id
        )))
    }

    /// Add multiple [LogFile]s to the corresponding [FileSlice]s in the [FileGroup].
    pub fn add_log_files<I>(&mut self, log_files: I) -> Result<&Self>
    where
        I: IntoIterator<Item = LogFile>,
    {
        for log_file in log_files {
            self.add_log_file(log_file)?;
        }
        Ok(self)
    }

    /// Retrieves a reference to the closest [FileSlice] that was created on or before the given
    /// `timestamp`.
    ///
    /// The timestamp should be a `commit_timestamp` (request/base instant time).
    pub fn get_file_slice_as_of(&self, timestamp: &str) -> Option<&FileSlice> {
        self.file_slices
            .range(..=timestamp.to_string())
            .next_back()
            .map(|(_, fs)| fs)
    }

    /// Retrieves a mutable reference to the closest [FileSlice] that was created on or before the
    /// given `timestamp`.
    ///
    /// The timestamp should be a `commit_timestamp` (request/base instant time).
    pub fn get_file_slice_mut_as_of(&mut self, timestamp: &str) -> Option<&mut FileSlice> {
        self.file_slices
            .range_mut(..=timestamp.to_string())
            .next_back()
            .map(|(_, fs)| fs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::partition::EMPTY_PARTITION_PATH;

    // ============================================================================
    // FileGroup tests (v6 tables)
    // ============================================================================

    #[test]
    fn load_a_valid_file_group() {
        let mut fg = FileGroup::new(
            "5a226868-2934-4f84-a16f-55124630c68d-0".to_owned(),
            EMPTY_PARTITION_PATH.to_string(),
        );
        let _ = fg.add_base_file_from_name(
            "5a226868-2934-4f84-a16f-55124630c68d-0_0-7-24_20240402144910683.parquet",
        );
        let _ = fg.add_base_file_from_name(
            "5a226868-2934-4f84-a16f-55124630c68d-0_2-10-0_20240402123035233.parquet",
        );
        assert_eq!(fg.file_slices.len(), 2);
        assert_eq!(fg.partition_path, EMPTY_PARTITION_PATH);
        let commit_times: Vec<&str> = fg.file_slices.keys().map(|k| k.as_str()).collect();
        assert_eq!(commit_times, vec!["20240402123035233", "20240402144910683"]);
        assert_eq!(
            fg.get_file_slice_as_of("20240402123035233")
                .unwrap()
                .base_file
                .commit_timestamp,
            "20240402123035233"
        );
        assert!(fg.get_file_slice_as_of("-1").is_none());
    }

    #[test]
    fn add_base_file_with_same_commit_time_should_fail() {
        let mut fg = FileGroup::new(
            "5a226868-2934-4f84-a16f-55124630c68d-0".to_owned(),
            EMPTY_PARTITION_PATH.to_string(),
        );
        let res1 = fg.add_base_file_from_name(
            "5a226868-2934-4f84-a16f-55124630c68d-0_0-7-24_20240402144910683.parquet",
        );
        assert!(res1.is_ok());
        let res2 = fg.add_base_file_from_name(
            "5a226868-2934-4f84-a16f-55124630c68d-0_2-10-0_20240402144910683.parquet",
        );
        assert!(res2.is_err());
        assert_eq!(res2.unwrap_err().to_string(), "File group error: Timestamp 20240402144910683 is already present in File Group 5a226868-2934-4f84-a16f-55124630c68d-0");
    }

    #[test]
    fn test_file_group_display() {
        let file_group = FileGroup {
            file_id: "group123".to_string(),
            partition_path: "part/2023-01-01".to_string(),
            file_slices: BTreeMap::new(),
        };

        let display_string = format!("{}", file_group);

        assert_eq!(
            display_string,
            "File Group: partition=part/2023-01-01, id=group123"
        );

        let file_group_no_partition = FileGroup {
            file_id: "group456".to_string(),
            partition_path: EMPTY_PARTITION_PATH.to_string(),
            file_slices: BTreeMap::new(),
        };

        let display_string_no_partition = format!("{}", file_group_no_partition);

        assert_eq!(
            display_string_no_partition,
            "File Group: partition=, id=group456"
        );
    }

    // ============================================================================
    // FileGroup tests with completion timestamps (v8+ table behavior)
    // ============================================================================

    fn create_base_file_with_completion(
        file_id: &str,
        commit_timestamp: &str,
        completion_timestamp: Option<&str>,
    ) -> BaseFile {
        BaseFile {
            file_id: file_id.to_string(),
            write_token: "0-7-24".to_string(),
            commit_timestamp: commit_timestamp.to_string(),
            completion_timestamp: completion_timestamp.map(|s| s.to_string()),
            extension: "parquet".to_string(),
            file_metadata: None,
        }
    }

    fn create_log_file_with_completion(
        file_id: &str,
        timestamp: &str,
        completion_timestamp: Option<&str>,
        version: u32,
    ) -> LogFile {
        LogFile {
            file_id: file_id.to_string(),
            timestamp: timestamp.to_string(),
            completion_timestamp: completion_timestamp.map(|s| s.to_string()),
            extension: "log".to_string(),
            version,
            write_token: "0-51-115".to_string(),
            file_metadata: None,
        }
    }

    #[test]
    fn test_file_group_add_base_file_with_completion() {
        let mut fg = FileGroup::new("file-id-0".to_string(), EMPTY_PARTITION_PATH.to_string());

        // Add a base file with completion timestamp - should succeed
        let base_file = create_base_file_with_completion(
            "file-id-0",
            "20250113230302428",       // request time
            Some("20250113230310000"), // completion time
        );

        let result = fg.add_base_file(base_file);
        assert!(result.is_ok());
        assert_eq!(fg.file_slices.len(), 1);
        // File slices are keyed by commit_timestamp (request time)
        assert!(fg.file_slices.contains_key("20250113230302428"));
        // Verify we can get the file slice using request timestamp
        let slice = fg.get_file_slice_as_of("20250113230302428").unwrap();
        assert_eq!(slice.base_file.commit_timestamp, "20250113230302428");
        assert_eq!(
            slice.base_file.completion_timestamp,
            Some("20250113230310000".to_string())
        );
    }

    #[test]
    fn test_file_group_log_file_association_by_completion_time() {
        let mut fg = FileGroup::new("file-id-0".to_string(), EMPTY_PARTITION_PATH.to_string());

        // Add two base files with different request and completion times
        // Base file 1: request=t1, completion=t3
        let base1 = create_base_file_with_completion(
            "file-id-0",
            "20250113230100000",       // request time t1
            Some("20250113230300000"), // completion time t3
        );

        // Base file 2: request=t2 (after t1), completion=t4 (after t3)
        let base2 = create_base_file_with_completion(
            "file-id-0",
            "20250113230200000",       // request time t2
            Some("20250113230400000"), // completion time t4
        );

        fg.add_base_file(base1).unwrap();
        fg.add_base_file(base2).unwrap();

        // Test 1: Log with completion time between base1's request and base2's request
        // Log completion time = t1.5 (between t1=request1 and t2=request2)
        // This should go to base1 because t1 is the largest request time <= t1.5
        let log1 = create_log_file_with_completion(
            "file-id-0",
            "20250113230050000",       // request time
            Some("20250113230150000"), // completion time t1.5 (between t1 and t2)
            1,
        );
        fg.add_log_file(log1).unwrap();

        // Test 2: Log with completion time after base2's request
        // This should go to base2 (latest base with request time <= log completion time)
        let log2 = create_log_file_with_completion(
            "file-id-0",
            "20250113230250000",
            Some("20250113230500000"), // completion after base2's request
            1,
        );
        fg.add_log_file(log2).unwrap();

        // Verify: log1 in base1's slice, log2 in base2's slice
        let slice1 = fg.file_slices.get("20250113230100000").unwrap();
        assert_eq!(slice1.log_files.len(), 1);

        let slice2 = fg.file_slices.get("20250113230200000").unwrap();
        assert_eq!(slice2.log_files.len(), 1);
    }

    #[test]
    fn test_file_group_log_file_error_cases() {
        // Test 1: Log file completed before any base file's request time
        let mut fg1 = FileGroup::new("file-id-0".to_string(), EMPTY_PARTITION_PATH.to_string());
        let base = create_base_file_with_completion(
            "file-id-0",
            "20250113230200000",       // request at t2
            Some("20250113230400000"), // completion at t4
        );
        fg1.add_base_file(base).unwrap();

        let log = create_log_file_with_completion(
            "file-id-0",
            "20250113230050000",
            Some("20250113230100000"), // completion at t1 < t2 (base request time)
            1,
        );
        let result = fg1.add_log_file(log);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("completed before any base file"));

        // Test 2: Log file with completion_timestamp when no file slices exist
        let mut fg2 = FileGroup::new("file-id-0".to_string(), EMPTY_PARTITION_PATH.to_string());
        let log = create_log_file_with_completion(
            "file-id-0",
            "20250113230000010",
            Some("20250113230000150"),
            1,
        );
        let result = fg2.add_log_file(log);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No suitable FileSlice found"));
    }

    #[test]
    fn test_file_group_multiple_log_files_different_slices() {
        let mut fg = FileGroup::new("file-id-0".to_string(), EMPTY_PARTITION_PATH.to_string());

        // Base1: request at t100
        let base1 = create_base_file_with_completion(
            "file-id-0",
            "20250113230000100",       // request t100
            Some("20250113230000150"), // completion
        );

        // Base2: request at t200
        let base2 = create_base_file_with_completion(
            "file-id-0",
            "20250113230000200",       // request t200
            Some("20250113230000250"), // completion
        );

        // Base3: request at t300
        let base3 = create_base_file_with_completion(
            "file-id-0",
            "20250113230000300",       // request t300
            Some("20250113230000350"), // completion
        );

        fg.add_base_file(base1).unwrap();
        fg.add_base_file(base2).unwrap();
        fg.add_base_file(base3).unwrap();

        // Log with completion at t150 -> should go to base1 (request t100 <= t150 < t200)
        let log1 = create_log_file_with_completion(
            "file-id-0",
            "20250113230000010",
            Some("20250113230000150"),
            1,
        );

        // Log with completion exactly at t200 (base2's request time) -> tests inclusive boundary
        // This should go to base2 because request t200 <= t200 (completion)
        let log2 = create_log_file_with_completion(
            "file-id-0",
            "20250113230000020",
            Some("20250113230000200"), // Exactly equals base2's request time - tests boundary
            1,
        );

        // Log with completion at t350 -> should go to base3 (request t300 <= t350)
        let log3 = create_log_file_with_completion(
            "file-id-0",
            "20250113230000030",
            Some("20250113230000350"),
            1,
        );

        fg.add_log_file(log1).unwrap();
        fg.add_log_file(log2).unwrap(); // Tests inclusive boundary (completion == base request)
        fg.add_log_file(log3).unwrap();

        // File slices are keyed by commit_timestamp (request time)
        // Verify each log went to the correct slice (keyed by request timestamps t100, t200, t300)
        // Note: log2 tests the inclusive boundary case (log.completion == base.request)
        assert_eq!(
            fg.file_slices
                .get("20250113230000100")
                .unwrap()
                .log_files
                .len(),
            1
        );
        assert_eq!(
            fg.file_slices
                .get("20250113230000200")
                .unwrap()
                .log_files
                .len(),
            1
        );
        assert_eq!(
            fg.file_slices
                .get("20250113230000300")
                .unwrap()
                .log_files
                .len(),
            1
        );
    }
}
