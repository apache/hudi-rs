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

pub mod reader;

use crate::error::CoreError;
use crate::storage::file_info::FileInfo;
use crate::storage::file_stats::FileStats;
use crate::storage::Storage;
use crate::Result;
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

/// Represents common metadata about a Hudi Base File.
#[derive(Clone, Debug)]
pub struct BaseFile {
    /// The file group id that is unique across the table.
    pub file_group_id: String,

    pub commit_time: String,

    pub info: FileInfo,

    pub stats: Option<FileStats>,
}

impl BaseFile {
    /// Parse file name and extract file_group_id and commit_time.
    fn parse_file_name(file_name: &str) -> Result<(String, String)> {
        let err_msg = format!("Failed to parse file name '{}' for base file.", file_name);
        let (name, _) = file_name
            .rsplit_once('.')
            .ok_or(CoreError::FileGroup(err_msg.clone()))?;
        let parts: Vec<&str> = name.split('_').collect();
        let file_group_id = parts
            .first()
            .ok_or(CoreError::FileGroup(err_msg.clone()))?
            .to_string();
        let commit_time = parts
            .get(2)
            .ok_or(CoreError::FileGroup(err_msg.clone()))?
            .to_string();
        Ok((file_group_id, commit_time))
    }

    /// Construct [BaseFile] with the base file name.
    pub fn from_file_name(file_name: &str) -> Result<Self> {
        let (file_group_id, commit_time) = Self::parse_file_name(file_name)?;
        Ok(Self {
            file_group_id,
            commit_time,
            info: FileInfo::default(),
            stats: None,
        })
    }

    /// Construct [BaseFile] with the [FileInfo].
    pub fn from_file_info(info: FileInfo) -> Result<Self> {
        let (file_group_id, commit_time) = Self::parse_file_name(&info.name)?;
        Ok(Self {
            file_group_id,
            commit_time,
            info,
            stats: None,
        })
    }
}

/// Within a file group, a slice is a combination of data file written at a commit time and list of log files,
/// containing changes to the data file from that commit time.
///
/// [note] The log files are not yet supported.
#[derive(Clone, Debug)]
pub struct FileSlice {
    pub base_file: BaseFile,
    pub partition_path: Option<String>,
}

impl FileSlice {
    pub fn base_file_path(&self) -> &str {
        self.base_file.info.uri.as_str()
    }

    pub fn base_file_relative_path(&self) -> String {
        let ptn = self.partition_path.as_deref().unwrap_or_default();
        let file_name = &self.base_file.info.name;
        PathBuf::from(ptn)
            .join(file_name)
            .to_str()
            .unwrap()
            .to_string()
    }

    pub fn file_group_id(&self) -> &str {
        &self.base_file.file_group_id
    }

    pub fn set_base_file(&mut self, base_file: BaseFile) {
        self.base_file = base_file
    }

    /// Load stats from storage layer for the base file if not already loaded.
    pub async fn load_stats(&mut self, storage: &Storage) -> Result<()> {
        if self.base_file.stats.is_none() {
            let parquet_meta = storage
                .get_parquet_file_metadata(&self.base_file_relative_path())
                .await?;
            let num_records = parquet_meta.file_metadata().num_rows();
            let size_bytes = parquet_meta
                .row_groups()
                .iter()
                .map(|rg| rg.total_byte_size())
                .sum::<i64>();
            let stats = FileStats {
                num_records,
                size_bytes,
            };
            self.base_file.stats = Some(stats);
        }
        Ok(())
    }
}

/// Hudi File Group.
#[derive(Clone, Debug)]
pub struct FileGroup {
    pub id: String,
    pub partition_path: Option<String>,
    pub file_slices: BTreeMap<String, FileSlice>,
}

impl PartialEq for FileGroup {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.partition_path == other.partition_path
    }
}

impl Eq for FileGroup {}

impl Hash for FileGroup {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.partition_path.hash(state);
    }
}

impl fmt::Display for FileGroup {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(
            format!(
                "File Group: partition {:?} id {}",
                &self.partition_path, &self.id
            )
            .as_str(),
        )
    }
}

impl FileGroup {
    pub fn new(id: String, partition_path: Option<String>) -> Self {
        Self {
            id,
            partition_path,
            file_slices: BTreeMap::new(),
        }
    }

    #[cfg(test)]
    fn add_base_file_from_name(&mut self, file_name: &str) -> Result<&Self> {
        let base_file = BaseFile::from_file_name(file_name)?;
        self.add_base_file(base_file)
    }

    pub fn add_base_file(&mut self, base_file: BaseFile) -> Result<&Self> {
        let commit_time = base_file.commit_time.as_str();
        if self.file_slices.contains_key(commit_time) {
            Err(CoreError::FileGroup(format!(
                "Commit time {0} is already present in File Group {1}",
                commit_time.to_owned(),
                self.id,
            )))
        } else {
            self.file_slices.insert(
                commit_time.to_owned(),
                FileSlice {
                    partition_path: self.partition_path.clone(),
                    base_file,
                },
            );
            Ok(self)
        }
    }

    pub fn get_file_slice_as_of(&self, timestamp: &str) -> Option<&FileSlice> {
        let as_of = timestamp.to_string();
        if let Some((_, file_slice)) = self.file_slices.range(..=as_of).next_back() {
            Some(file_slice)
        } else {
            None
        }
    }

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

    #[test]
    fn create_a_base_file_successfully() {
        let base_file = BaseFile::from_file_name(
            "5a226868-2934-4f84-a16f-55124630c68d-0_0-7-24_20240402144910683.parquet",
        )
        .unwrap();
        assert_eq!(
            base_file.file_group_id,
            "5a226868-2934-4f84-a16f-55124630c68d-0"
        );
        assert_eq!(base_file.commit_time, "20240402144910683");
    }

    #[test]
    fn create_a_base_file_returns_error() {
        let result = BaseFile::from_file_name("no_file_extension");
        assert!(matches!(result.unwrap_err(), CoreError::FileGroup(_)));

        let result = BaseFile::from_file_name("no-valid-delimiter.parquet");
        assert!(matches!(result.unwrap_err(), CoreError::FileGroup(_)));
    }

    #[test]
    fn load_a_valid_file_group() {
        let mut fg = FileGroup::new("5a226868-2934-4f84-a16f-55124630c68d-0".to_owned(), None);
        let _ = fg.add_base_file_from_name(
            "5a226868-2934-4f84-a16f-55124630c68d-0_0-7-24_20240402144910683.parquet",
        );
        let _ = fg.add_base_file_from_name(
            "5a226868-2934-4f84-a16f-55124630c68d-0_2-10-0_20240402123035233.parquet",
        );
        assert_eq!(fg.file_slices.len(), 2);
        assert!(fg.partition_path.is_none());
        let commit_times: Vec<&str> = fg.file_slices.keys().map(|k| k.as_str()).collect();
        assert_eq!(commit_times, vec!["20240402123035233", "20240402144910683"]);
        assert_eq!(
            fg.get_file_slice_as_of("20240402123035233")
                .unwrap()
                .base_file
                .commit_time,
            "20240402123035233"
        );
        assert!(fg.get_file_slice_as_of("-1").is_none());
    }

    #[test]
    fn add_base_file_with_same_commit_time_should_fail() {
        let mut fg = FileGroup::new("5a226868-2934-4f84-a16f-55124630c68d-0".to_owned(), None);
        let res1 = fg.add_base_file_from_name(
            "5a226868-2934-4f84-a16f-55124630c68d-0_0-7-24_20240402144910683.parquet",
        );
        assert!(res1.is_ok());
        let res2 = fg.add_base_file_from_name(
            "5a226868-2934-4f84-a16f-55124630c68d-0_2-10-0_20240402144910683.parquet",
        );
        assert!(res2.is_err());
        assert_eq!(res2.unwrap_err().to_string(), "File group error: Commit time 20240402144910683 is already present in File Group 5a226868-2934-4f84-a16f-55124630c68d-0");
    }

    #[test]
    fn test_file_group_display() {
        let file_group = FileGroup {
            id: "group123".to_string(),
            partition_path: Some("part/2023-01-01".to_string()),
            file_slices: BTreeMap::new(),
        };

        let display_string = format!("{}", file_group);

        assert_eq!(
            display_string,
            "File Group: partition Some(\"part/2023-01-01\") id group123"
        );

        let file_group_no_partition = FileGroup {
            id: "group456".to_string(),
            partition_path: None,
            file_slices: BTreeMap::new(),
        };

        let display_string_no_partition = format!("{}", file_group_no_partition);

        assert_eq!(
            display_string_no_partition,
            "File Group: partition None id group456"
        );
    }
}
