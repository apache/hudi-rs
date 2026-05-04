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
use crate::error::CoreError;
use crate::file_group::base_file::BaseFile;
use crate::file_group::log_file::LogFile;
use crate::statistics::StatisticsContainer;
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
    /// Column statistics from the base file's Parquet footer.
    ///
    /// Populated when data-column filters trigger footer-based pruning
    /// on COW tables or MOR read-optimized mode. `None` otherwise.
    pub base_file_column_stats: Option<StatisticsContainer>,
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
            base_file_column_stats: None,
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

    /// Total on-disk size of the file slice (base file + all log files), in bytes.
    ///
    /// Use this for I/O cost estimation and split sizing (e.g., balancing
    /// parallel read splits by storage volume). For estimated in-memory size
    /// and record count for query planning, use [`Table::compute_table_stats`]
    /// instead — it returns values derived from the base file only.
    ///
    /// Files whose `file_metadata` is `None` contribute 0 to the sum.
    #[inline]
    pub fn total_size_bytes(&self) -> u64 {
        let base = self
            .base_file
            .file_metadata
            .as_ref()
            .map(|m| m.size)
            .unwrap_or(0);
        let logs: u64 = self
            .log_files
            .iter()
            .filter_map(|lf| lf.file_metadata.as_ref().map(|m| m.size))
            .sum();
        base + logs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::file_metadata::FileMetadata;
    use crate::table::partition::EMPTY_PARTITION_PATH;
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
            base_file_column_stats: None,
        };

        let slice2 = FileSlice {
            base_file: base,
            log_files: log_set2,
            partition_path: EMPTY_PARTITION_PATH.to_string(),
            base_file_column_stats: None,
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
            base_file_column_stats: None,
        };

        let slice2 = FileSlice {
            base_file: BaseFile::from_str(
                "54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_1-19-51_20250109233025121.parquet",
            )?,
            log_files: BTreeSet::new(),
            partition_path: EMPTY_PARTITION_PATH.to_string(),
            base_file_column_stats: None,
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
            base_file_column_stats: None,
        };

        let slice2 = FileSlice {
            base_file: base,
            log_files: BTreeSet::new(),
            partition_path: "path/to/partition2".to_string(),
            base_file_column_stats: None,
        };

        // Should return error for different partition paths
        assert!(slice1.merge(&slice2).is_err());

        Ok(())
    }

    fn make_base_file_with_metadata(size: u64) -> BaseFile {
        let mut bf = BaseFile::from_str(
            "54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_0-7-24_20250109233025121.parquet",
        )
        .unwrap();
        bf.file_metadata = Some(FileMetadata::new("base.parquet", size));
        bf
    }

    fn make_log_file_with_metadata(version: u32, size: Option<u64>) -> LogFile {
        let name = format!(
            ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.{version}_0-51-115"
        );
        let mut lf = LogFile::from_str(&name).unwrap();
        lf.file_metadata = size.map(|s| FileMetadata::new(&name, s));
        lf
    }

    #[test]
    fn test_total_size_bytes_base_only() {
        let slice = FileSlice {
            base_file: make_base_file_with_metadata(1000),
            log_files: BTreeSet::new(),
            partition_path: EMPTY_PARTITION_PATH.to_string(),
            base_file_column_stats: None,
        };
        assert_eq!(slice.total_size_bytes(), 1000);
    }

    #[test]
    fn test_total_size_bytes_with_log_files() {
        let mut logs = BTreeSet::new();
        logs.insert(make_log_file_with_metadata(1, Some(200)));
        logs.insert(make_log_file_with_metadata(2, Some(300)));
        let slice = FileSlice {
            base_file: make_base_file_with_metadata(1000),
            log_files: logs,
            partition_path: EMPTY_PARTITION_PATH.to_string(),
            base_file_column_stats: None,
        };
        assert_eq!(slice.total_size_bytes(), 1500);
    }

    #[test]
    fn test_total_size_bytes_mixed_metadata() {
        let mut logs = BTreeSet::new();
        logs.insert(make_log_file_with_metadata(1, Some(200)));
        logs.insert(make_log_file_with_metadata(2, None));
        let slice = FileSlice {
            base_file: make_base_file_with_metadata(1000),
            log_files: logs,
            partition_path: EMPTY_PARTITION_PATH.to_string(),
            base_file_column_stats: None,
        };
        assert_eq!(slice.total_size_bytes(), 1200);
    }

    #[test]
    fn test_total_size_bytes_no_metadata() {
        let mut logs = BTreeSet::new();
        logs.insert(make_log_file_with_metadata(1, None));
        let mut bf = BaseFile::from_str(
            "54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_0-7-24_20250109233025121.parquet",
        )
        .unwrap();
        bf.file_metadata = None;
        let slice = FileSlice {
            base_file: bf,
            log_files: logs,
            partition_path: EMPTY_PARTITION_PATH.to_string(),
            base_file_column_stats: None,
        };
        assert_eq!(slice.total_size_bytes(), 0);
    }
}
