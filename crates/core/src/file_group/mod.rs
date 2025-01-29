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
/// The [FileSlice]s are ordered by the commit timestamps that indicate the creation of the
/// [FileSlice].
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
    pub fn new_with_base_file_name(
        id: String,
        partition_path: String,
        file_name: &str,
    ) -> Result<Self> {
        let mut file_group = Self::new(id, partition_path);
        file_group.add_base_file_from_name(file_name)?;
        Ok(file_group)
    }

    pub fn merge(&mut self, other: &FileGroup) -> Result<()> {
        if self != other {
            return Err(CoreError::FileGroup(format!(
                "Cannot merge FileGroup with different file groups. Existing: {}, New: {}",
                self, other
            )));
        }

        for (commit_timestamp, other_file_slice) in other.file_slices.iter() {
            if let Some(existing_file_slice) = self.file_slices.get_mut(commit_timestamp) {
                existing_file_slice.merge(other_file_slice)?;
            } else {
                self.file_slices
                    .insert(commit_timestamp.clone(), other_file_slice.clone());
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
    pub fn add_base_file(&mut self, base_file: BaseFile) -> Result<&Self> {
        let commit_timestamp = base_file.commit_timestamp.as_str();
        if self.file_slices.contains_key(commit_timestamp) {
            Err(CoreError::FileGroup(format!(
                "Instant time {commit_timestamp} is already present in File Group {}",
                self.file_id
            )))
        } else {
            self.file_slices.insert(
                commit_timestamp.to_owned(),
                FileSlice::new(base_file, self.partition_path.clone()),
            );
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

    /// Add a [LogFile] to the corresponding [FileSlice] in the [FileGroup].
    ///
    /// TODO: support adding log files to file group without base files.
    pub fn add_log_file(&mut self, log_file: LogFile) -> Result<&Self> {
        let commit_timestamp = log_file.base_commit_timestamp.as_str();
        if let Some(file_slice) = self.file_slices.get_mut(commit_timestamp) {
            file_slice.log_files.insert(log_file);
            Ok(self)
        } else {
            Err(CoreError::FileGroup(format!(
                "Instant time {commit_timestamp} not found in File Group {}",
                self.file_id
            )))
        }
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
    pub fn get_file_slice_as_of(&self, timestamp: &str) -> Option<&FileSlice> {
        let as_of = timestamp.to_string();
        if let Some((_, file_slice)) = self.file_slices.range(..=as_of).next_back() {
            Some(file_slice)
        } else {
            None
        }
    }

    /// Retrieves a mutable reference to the closest [FileSlice] that was created on or before the
    /// given `timestamp`.
    pub fn get_file_slice_mut_as_of(&mut self, timestamp: &str) -> Option<&mut FileSlice> {
        let as_of = timestamp.to_string();
        if let Some((_, file_slice)) = self.file_slices.range_mut(..=as_of).next_back() {
            Some(file_slice)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::partition::EMPTY_PARTITION_PATH;

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
        assert_eq!(res2.unwrap_err().to_string(), "File group error: Instant time 20240402144910683 is already present in File Group 5a226868-2934-4f84-a16f-55124630c68d-0");
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
}
