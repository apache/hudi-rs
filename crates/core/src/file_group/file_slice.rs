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
use crate::Result;
use crate::config::table::BaseFileFormatValue;
use crate::error::CoreError;
use crate::file_group::base_file::BaseFile;
use crate::file_group::log_file::LogFile;
use crate::storage::Storage;
use std::collections::BTreeSet;
use std::fmt::Display;
use std::path::PathBuf;

/// Within a [crate::file_group::FileGroup],
/// a [FileSlice] is a logical group of [BaseFile] and [LogFile]s.
#[derive(Clone, Debug)]
pub struct FileSlice {
    pub base_file: BaseFile,
    pub log_files: BTreeSet<LogFile>,
    pub partition_path: String,
}

impl Display for FileSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FileSlice {{ base_file: {}, log_files: {:?}, partition_path: {} }}",
            self.base_file, self.log_files, self.partition_path
        )
    }
}

impl PartialEq for FileSlice {
    fn eq(&self, other: &Self) -> bool {
        self.base_file == other.base_file && self.partition_path == other.partition_path
    }
}

impl Eq for FileSlice {}

impl FileSlice {
    pub fn new(base_file: BaseFile, partition_path: String) -> Self {
        Self {
            base_file,
            log_files: BTreeSet::new(),
            partition_path,
        }
    }

    #[inline]
    pub fn has_log_file(&self) -> bool {
        !self.log_files.is_empty()
    }

    pub fn merge(&mut self, other: &FileSlice) -> Result<()> {
        if self != other {
            return Err(CoreError::FileGroup(format!(
                "Cannot merge different file slices: {self} and {other}"
            )));
        }
        self.log_files.extend(other.log_files.iter().cloned());

        Ok(())
    }

    fn relative_path_for_file(&self, file_name: &str) -> Result<String> {
        let path = PathBuf::from(self.partition_path.as_str()).join(file_name);
        path.to_str().map(|s| s.to_string()).ok_or_else(|| {
            CoreError::FileGroup(format!("Failed to get relative path for file: {file_name}",))
        })
    }

    /// Returns the relative path of the [BaseFile] in the [FileSlice].
    pub fn base_file_relative_path(&self) -> Result<String> {
        let file_name = &self.base_file.file_name();
        self.relative_path_for_file(file_name)
    }

    /// Returns the relative path of the given [LogFile] in the [FileSlice].
    pub fn log_file_relative_path(&self, log_file: &LogFile) -> Result<String> {
        let file_name = &log_file.file_name();
        self.relative_path_for_file(file_name)
    }

    /// Returns the enclosing [FileGroup]'s id.
    #[inline]
    pub fn file_id(&self) -> &str {
        &self.base_file.file_id
    }

    /// Returns the instant time that marks the [FileSlice] creation.
    ///
    /// This is also an instant time stored in the [Timeline].
    #[inline]
    pub fn creation_instant_time(&self) -> &str {
        &self.base_file.commit_timestamp
    }

    /// Load [FileMetadata] from storage layer for the [BaseFile] if `file_metadata` is [None]
    /// or if `file_metadata` is not fully populated.
    ///
    /// This only loads metadata for Parquet files. For non-Parquet files (e.g., HFile),
    /// this is a no-op since Parquet-specific metadata reading would fail.
    /// TODO: see if mdt read would benefit from loading hfile metadata as well.
    pub async fn load_metadata_if_needed(&mut self, storage: &Storage) -> Result<()> {
        // Skip non-Parquet files - metadata loading uses Parquet-specific APIs
        if self.base_file.extension != BaseFileFormatValue::Parquet.as_ref() {
            return Ok(());
        }

        if let Some(metadata) = &self.base_file.file_metadata {
            if metadata.fully_populated {
                return Ok(());
            }
        }

        let relative_path = self.base_file_relative_path()?;
        let fetched_metadata = storage.get_file_metadata(&relative_path).await?;
        self.base_file.file_metadata = Some(fetched_metadata);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::EMPTY_PARTITION_PATH;
    use std::str::FromStr;

    #[test]
    fn test_file_slices_merge() -> Result<()> {
        let base = BaseFile::from_str(
            "54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_0-7-24_20250109233025121.parquet",
        )?;
        let mut log_set1 = BTreeSet::new();
        log_set1.insert(LogFile::from_str(
            ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.4_0-51-115",
        )?);
        log_set1.insert(LogFile::from_str(
            ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.2_0-51-115",
        )?);

        let mut log_set2 = BTreeSet::new();
        log_set2.insert(LogFile::from_str(
            ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.3_0-51-115",
        )?);
        log_set2.insert(LogFile::from_str(
            ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.1_0-51-115",
        )?);
        log_set1.insert(LogFile::from_str(
            ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.2_0-51-115",
        )?);

        let mut slice1 = FileSlice {
            base_file: base.clone(),
            log_files: log_set1,
            partition_path: EMPTY_PARTITION_PATH.to_string(),
        };

        let slice2 = FileSlice {
            base_file: base,
            log_files: log_set2,
            partition_path: EMPTY_PARTITION_PATH.to_string(),
        };

        slice1.merge(&slice2)?;

        // Verify merged result
        assert_eq!(slice1.log_files.len(), 4);
        let log_file_names = slice1
            .log_files
            .iter()
            .map(|log| log.file_name())
            .collect::<Vec<String>>();
        assert_eq!(
            log_file_names.as_slice(),
            &[
                ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.1_0-51-115",
                ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.2_0-51-115",
                ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.3_0-51-115",
                ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.4_0-51-115",
            ]
        );

        Ok(())
    }

    #[test]
    fn test_merge_different_base_files() -> Result<()> {
        let mut slice1 = FileSlice {
            base_file: BaseFile::from_str(
                "54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_0-7-24_20250109233025121.parquet",
            )?,
            log_files: BTreeSet::new(),
            partition_path: EMPTY_PARTITION_PATH.to_string(),
        };

        let slice2 = FileSlice {
            base_file: BaseFile::from_str(
                "54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_1-19-51_20250109233025121.parquet",
            )?,
            log_files: BTreeSet::new(),
            partition_path: EMPTY_PARTITION_PATH.to_string(),
        };

        // Should return error for different base files
        assert!(slice1.merge(&slice2).is_err());

        Ok(())
    }

    #[test]
    fn test_merge_different_partition_paths() -> Result<()> {
        let base = BaseFile::from_str(
            "54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_1-19-51_20250109233025121.parquet",
        )?;
        let mut slice1 = FileSlice {
            base_file: base.clone(),
            log_files: BTreeSet::new(),
            partition_path: "path/to/partition1".to_string(),
        };

        let slice2 = FileSlice {
            base_file: base,
            log_files: BTreeSet::new(),
            partition_path: "path/to/partition2".to_string(),
        };

        // Should return error for different partition paths
        assert!(slice1.merge(&slice2).is_err());

        Ok(())
    }
}
