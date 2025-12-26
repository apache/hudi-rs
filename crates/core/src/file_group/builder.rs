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
use crate::error::CoreError;
use crate::file_group::base_file::BaseFile;
use crate::file_group::log_file::LogFile;
use crate::file_group::FileGroup;
use crate::metadata::commit::HoodieCommitMetadata;
use crate::metadata::replace_commit::HoodieReplaceCommitMetadata;
use crate::metadata::table_record::FilesPartitionRecord;
use crate::timeline::completion_time::TimelineViewByCompletionTime;
use crate::Result;
use dashmap::DashMap;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;

pub trait FileGroupMerger {
    fn merge<I>(&mut self, file_groups: I) -> Result<()>
    where
        I: IntoIterator<Item = FileGroup>;
}

impl FileGroupMerger for HashSet<FileGroup> {
    fn merge<I>(&mut self, file_groups: I) -> Result<()>
    where
        I: IntoIterator<Item = FileGroup>,
    {
        for file_group in file_groups {
            if let Some(mut existing) = self.take(&file_group) {
                existing.merge(&file_group)?;
                self.insert(existing);
            } else {
                self.insert(file_group);
            }
        }
        Ok(())
    }
}

/// Build file groups from commit metadata.
///
/// This function is used for **incremental queries** to get file groups between two timestamps.
/// It parses file information from individual commit metadata files (e.g., `.commit`, `.deltacommit`)
/// in the `.hoodie` timeline directory.
///
/// # Arguments
///
/// * `commit_metadata` - The commit metadata JSON map
/// * `completion_time_view` - View to look up completion timestamps.
///   - For v6 tables: Pass [`V6CompletionTimeView`] (returns `None` for all lookups)
///   - For v8+ tables: Pass [`CompletionTimeView`] built from timeline instants
///
/// [`V6CompletionTimeView`]: crate::timeline::completion_time::V6CompletionTimeView
/// [`CompletionTimeView`]: crate::timeline::completion_time::CompletionTimeView
pub fn file_groups_from_commit_metadata<V: TimelineViewByCompletionTime>(
    commit_metadata: &Map<String, Value>,
    completion_time_view: &V,
) -> Result<HashSet<FileGroup>> {
    let metadata = HoodieCommitMetadata::from_json_map(commit_metadata)?;

    let mut file_groups = HashSet::new();

    for (partition, write_stat) in metadata.iter_write_stats() {
        let file_id = write_stat
            .file_id
            .as_ref()
            .ok_or_else(|| CoreError::CommitMetadata("Missing fileId in write stats".into()))?;

        let mut file_group = FileGroup::new(file_id.clone(), partition.clone());

        // Handle two cases:
        // 1. MOR table with baseFile and optionally logFiles
        // 2. COW table with path only
        if let Some(base_file_name) = &write_stat.base_file {
            let mut base_file = BaseFile::from_str(base_file_name)?;
            base_file.set_completion_time(completion_time_view);
            file_group.add_base_file(base_file)?;

            // Log files are optional - MOR tables may have:
            // - No log files yet (initial insert)
            // - Some log files (after updates)
            // - Empty log files list
            if let Some(log_file_names) = &write_stat.log_files {
                for log_file_name in log_file_names {
                    let mut log_file = LogFile::from_str(log_file_name)?;
                    log_file.set_completion_time(completion_time_view);
                    file_group.add_log_file(log_file)?;
                }
            }
            // Note: log_files being None is valid for MOR tables
        } else {
            let path = write_stat
                .path
                .as_ref()
                .ok_or_else(|| CoreError::CommitMetadata("Missing path in write stats".into()))?;

            let file_name = Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| CoreError::CommitMetadata("Invalid file name in path".into()))?;

            let mut base_file = BaseFile::from_str(file_name)?;
            base_file.set_completion_time(completion_time_view);
            file_group.add_base_file(base_file)?;
        }

        file_groups.insert(file_group);
    }

    Ok(file_groups)
}

pub fn replaced_file_groups_from_replace_commit(
    commit_metadata: &Map<String, Value>,
) -> Result<HashSet<FileGroup>> {
    // Replace commits follow HoodieReplaceCommitMetadata schema; parse with the dedicated type
    let metadata = HoodieReplaceCommitMetadata::from_json_map(commit_metadata)?;

    let mut file_groups = HashSet::new();

    for (partition, file_id) in metadata.iter_replace_file_ids() {
        let file_group = FileGroup::new(file_id.clone(), partition.clone());
        file_groups.insert(file_group);
    }

    Ok(file_groups)
}

/// Build FileGroups from metadata table FilesPartitionRecords.
///
/// This function is used for **snapshot queries** when the metadata table is enabled.
/// It parses file names from the `.hoodie/metadata` table's files partition records
/// and constructs FileGroup objects. Only includes active (non-deleted) files.
///
/// # Arguments
/// * `records` - Metadata table files partition records (partition_path -> FilesPartitionRecord)
/// * `base_file_extension` - The base file format extension (e.g., "parquet", "hfile")
/// * `completion_time_view` - View to look up completion timestamps.
///   - For v6 tables: Pass [`V6CompletionTimeView`] (returns `None` for all lookups)
///   - For v8+ tables: Pass [`CompletionTimeView`] built from timeline instants
///
/// [`V6CompletionTimeView`]: crate::timeline::completion_time::V6CompletionTimeView
/// [`CompletionTimeView`]: crate::timeline::completion_time::CompletionTimeView
///
/// # Returns
/// A map of partition paths to their FileGroups.
pub fn file_groups_from_files_partition_records<V: TimelineViewByCompletionTime>(
    records: &HashMap<String, FilesPartitionRecord>,
    base_file_extension: &str,
    completion_time_view: &V,
) -> Result<DashMap<String, Vec<FileGroup>>> {
    let file_groups_map = DashMap::new();
    let base_file_suffix = format!(".{}", base_file_extension);

    for (partition_path, record) in records {
        // Skip __all_partitions__ record - it lists partition names, not files
        if record.is_all_partitions() {
            continue;
        }

        let mut file_id_to_base_files: HashMap<String, Vec<BaseFile>> = HashMap::new();
        let mut file_id_to_log_files: HashMap<String, Vec<LogFile>> = HashMap::new();

        for file_name in record.active_file_names() {
            if file_name.starts_with('.') {
                // Log file: starts with '.'
                match LogFile::from_str(file_name) {
                    Ok(mut log_file) => {
                        log_file.set_completion_time(completion_time_view);
                        // Filter uncommitted files for v8+ tables
                        if completion_time_view.should_filter_uncommitted()
                            && log_file.completion_timestamp.is_none()
                        {
                            continue;
                        }
                        file_id_to_log_files
                            .entry(log_file.file_id.clone())
                            .or_default()
                            .push(log_file);
                    }
                    Err(e) => {
                        // Skip files that can't be parsed (e.g., .cdc files not yet supported)
                        log::warn!(
                            "Failed to parse log file from metadata table: {}: {}",
                            file_name,
                            e
                        );
                    }
                }
            } else if file_name.ends_with(&base_file_suffix) {
                // Base file: ends with base file extension
                match BaseFile::from_str(file_name) {
                    Ok(mut base_file) => {
                        base_file.set_completion_time(completion_time_view);
                        // Filter uncommitted files for v8+ tables
                        if completion_time_view.should_filter_uncommitted()
                            && base_file.completion_timestamp.is_none()
                        {
                            continue;
                        }
                        file_id_to_base_files
                            .entry(base_file.file_id.clone())
                            .or_default()
                            .push(base_file);
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to parse base file from metadata table: {}: {}",
                            file_name,
                            e
                        );
                    }
                }
            }
            // Skip files with unrecognized extensions
        }

        // Build FileGroups from parsed files
        // Note: Currently only supports file groups with base files.
        // TODO: Support file groups with only log files (P1 task)
        let mut file_groups = Vec::new();
        for (file_id, base_files) in file_id_to_base_files {
            let mut fg = FileGroup::new(file_id.clone(), partition_path.clone());
            fg.add_base_files(base_files)?;

            // Add corresponding log files if any
            if let Some(log_files) = file_id_to_log_files.remove(&file_id) {
                fg.add_log_files(log_files)?;
            }

            file_groups.push(fg);
        }

        if !file_groups.is_empty() {
            file_groups_map.insert(partition_path.clone(), file_groups);
        }
    }

    Ok(file_groups_map)
}

#[cfg(test)]
mod tests {

    mod test_file_group_merger {
        use super::super::*;
        use crate::file_group::FileGroup;

        #[test]
        fn test_merge_file_groups() {
            let mut existing = HashSet::new();
            let fg1 = FileGroup::new("file1".to_string(), "p1".to_string());
            existing.insert(fg1);

            let new_groups = vec![
                FileGroup::new("file2".to_string(), "p1".to_string()),
                FileGroup::new("file3".to_string(), "p2".to_string()),
            ];

            existing.merge(new_groups).unwrap();
            assert_eq!(existing.len(), 3);
        }

        #[test]
        fn test_merge_empty() {
            let mut existing = HashSet::new();
            let fg1 = FileGroup::new("file1".to_string(), "p1".to_string());
            existing.insert(fg1);

            let new_groups: Vec<FileGroup> = vec![];
            existing.merge(new_groups).unwrap();
            assert_eq!(existing.len(), 1);
        }
    }

    mod test_file_groups_from_commit_metadata {
        use super::super::*;
        use crate::timeline::completion_time::{CompletionTimeView, V6CompletionTimeView};
        use serde_json::{json, Map, Value};

        #[test]
        fn test_missing_partition_to_write_stats() {
            let metadata: Map<String, Value> = json!({
                "compacted": false,
                "operationType": "UPSERT"
            })
            .as_object()
            .unwrap()
            .clone();

            // Use V6CompletionTimeView for v6 table behavior (no completion time tracking)
            let result = file_groups_from_commit_metadata(&metadata, &V6CompletionTimeView::new());
            // With the new implementation, this returns Ok with an empty HashSet
            // because iter_write_stats() returns an empty iterator when partition_to_write_stats is None
            assert!(result.is_ok());
            assert_eq!(result.unwrap().len(), 0);
        }

        #[test]
        fn test_invalid_write_stats_array() {
            let metadata: Map<String, Value> = json!({
                "partitionToWriteStats": {
                    "byteField=20/shortField=100": "not_an_array"
                }
            })
            .as_object()
            .unwrap()
            .clone();

            let result = file_groups_from_commit_metadata(&metadata, &V6CompletionTimeView::new());
            assert!(matches!(
                result,
                Err(CoreError::CommitMetadata(msg)) if msg.contains("Failed to parse commit metadata")
            ));
        }

        #[test]
        fn test_missing_file_id() {
            let metadata: Map<String, Value> = json!({
                "partitionToWriteStats": {
                    "byteField=20/shortField=100": [{
                        "path": "byteField=20/shortField=100/some-file.parquet"
                    }]
                }
            })
            .as_object()
            .unwrap()
            .clone();

            let result = file_groups_from_commit_metadata(&metadata, &V6CompletionTimeView::new());
            assert!(matches!(
                result,
                Err(CoreError::CommitMetadata(msg)) if msg == "Missing fileId in write stats"
            ));
        }

        #[test]
        fn test_missing_path() {
            let metadata: Map<String, Value> = json!({
                "partitionToWriteStats": {
                    "byteField=20/shortField=100": [{
                        "fileId": "bb7c3a45-387f-490d-aab2-981c3f1a8ada-0"
                    }]
                }
            })
            .as_object()
            .unwrap()
            .clone();

            let result = file_groups_from_commit_metadata(&metadata, &V6CompletionTimeView::new());
            assert!(matches!(
                result,
                Err(CoreError::CommitMetadata(msg)) if msg == "Missing path in write stats"
            ));
        }

        #[test]
        fn test_invalid_path_format() {
            let metadata: Map<String, Value> = json!({
                "partitionToWriteStats": {
                    "byteField=20/shortField=100": [{
                        "fileId": "bb7c3a45-387f-490d-aab2-981c3f1a8ada-0",
                        "path": "" // empty path
                    }]
                }
            })
            .as_object()
            .unwrap()
            .clone();

            let result = file_groups_from_commit_metadata(&metadata, &V6CompletionTimeView::new());
            assert!(matches!(
                result,
                Err(CoreError::CommitMetadata(msg)) if msg == "Invalid file name in path"
            ));
        }

        #[test]
        fn test_non_string_file_id() {
            let metadata: Map<String, Value> = json!({
                "partitionToWriteStats": {
                    "byteField=20/shortField=100": [{
                        "fileId": 123, // number instead of string
                        "path": "byteField=20/shortField=100/some-file.parquet"
                    }]
                }
            })
            .as_object()
            .unwrap()
            .clone();

            let result = file_groups_from_commit_metadata(&metadata, &V6CompletionTimeView::new());
            // Serde will fail to parse this and return a deserialization error
            assert!(matches!(
                result,
                Err(CoreError::CommitMetadata(msg)) if msg.contains("Failed to parse commit metadata")
            ));
        }

        #[test]
        fn test_non_string_path() {
            let metadata: Map<String, Value> = json!({
                "partitionToWriteStats": {
                    "byteField=20/shortField=100": [{
                        "fileId": "bb7c3a45-387f-490d-aab2-981c3f1a8ada-0",
                        "path": 123 // number instead of string
                    }]
                }
            })
            .as_object()
            .unwrap()
            .clone();

            let result = file_groups_from_commit_metadata(&metadata, &V6CompletionTimeView::new());
            // Serde will fail to parse this and return a deserialization error
            assert!(matches!(
                result,
                Err(CoreError::CommitMetadata(msg)) if msg.contains("Failed to parse commit metadata")
            ));
        }

        #[test]
        fn test_valid_sample_data() {
            let sample_json = r#"{
            "partitionToWriteStats": {
                "byteField=20/shortField=100": [{
                    "fileId": "bb7c3a45-387f-490d-aab2-981c3f1a8ada-0",
                    "path": "byteField=20/shortField=100/bb7c3a45-387f-490d-aab2-981c3f1a8ada-0_0-140-198_20240418173213674.parquet"
                }],
                "byteField=10/shortField=300": [{
                    "fileId": "a22e8257-e249-45e9-ba46-115bc85adcba-0",
                    "path": "byteField=10/shortField=300/a22e8257-e249-45e9-ba46-115bc85adcba-0_1-140-199_20240418173213674.parquet"
                }]
            }
        }"#;

            let metadata: Map<String, Value> = serde_json::from_str(sample_json).unwrap();
            let result = file_groups_from_commit_metadata(&metadata, &V6CompletionTimeView::new());
            assert!(result.is_ok());
            let file_groups = result.unwrap();
            assert_eq!(file_groups.len(), 2);

            let expected_partitions = HashSet::from_iter(vec![
                "byteField=20/shortField=100",
                "byteField=10/shortField=300",
            ]);
            let actual_partitions =
                HashSet::<&str>::from_iter(file_groups.iter().map(|fg| fg.partition_path.as_str()));
            assert_eq!(actual_partitions, expected_partitions);
        }

        #[test]
        fn test_file_groups_from_commit_metadata_with_completion_time_view() {
            let sample_json = r#"{
            "partitionToWriteStats": {
                "partition1": [{
                    "fileId": "file-id-0",
                    "path": "partition1/file-id-0_0-7-24_20240418173200000.parquet"
                }]
            }
        }"#;

            let metadata: Map<String, Value> = serde_json::from_str(sample_json).unwrap();

            // Create a completion time view with the mapping for this file's request timestamp
            use crate::timeline::instant::{Action, Instant, State};
            let instants = vec![Instant {
                timestamp: "20240418173200000".to_string(),
                completion_timestamp: Some("20240418173210000".to_string()),
                action: Action::Commit,
                state: State::Completed,
                epoch_millis: 0,
            }];
            let view = CompletionTimeView::from_instants(&instants);

            let result = file_groups_from_commit_metadata(&metadata, &view);
            assert!(result.is_ok());
            let file_groups = result.unwrap();
            assert_eq!(file_groups.len(), 1);

            let file_group = file_groups.iter().next().unwrap();
            let file_slice = file_group.file_slices.values().next().unwrap();
            assert_eq!(
                file_slice.base_file.completion_timestamp,
                Some("20240418173210000".to_string())
            );
        }
    }

    mod test_replaced_file_groups_from_replace_commit {
        use super::super::*;
        use crate::table::partition::EMPTY_PARTITION_PATH;
        use serde_json::{json, Map, Value};

        #[test]
        fn test_missing_partition_to_replace() {
            let metadata: Map<String, Value> = json!({
                "compacted": false,
                "operationType": "UPSERT"
            })
            .as_object()
            .unwrap()
            .clone();

            let result = replaced_file_groups_from_replace_commit(&metadata);
            // With the new implementation, this returns Ok with an empty HashSet
            // because iter_replace_file_ids() returns an empty iterator when partition_to_replace_file_ids is None
            assert!(result.is_ok());
            assert_eq!(result.unwrap().len(), 0);
        }

        #[test]
        fn test_invalid_file_ids_array() {
            let metadata: Map<String, Value> = json!({
                "partitionToReplaceFileIds": {
                    "20": "not_an_array"
                }
            })
            .as_object()
            .unwrap()
            .clone();

            let result = replaced_file_groups_from_replace_commit(&metadata);
            assert!(matches!(
                result,
                Err(CoreError::CommitMetadata(msg)) if msg.contains("Failed to parse commit metadata")
            ));
        }

        #[test]
        fn test_invalid_file_id_type() {
            let metadata: Map<String, Value> = json!({
                "partitionToReplaceFileIds": {
                    "20": [123] // number instead of string
                }
            })
            .as_object()
            .unwrap()
            .clone();

            let result = replaced_file_groups_from_replace_commit(&metadata);
            // Serde will fail to parse this
            assert!(matches!(
                result,
                Err(CoreError::CommitMetadata(msg)) if msg.contains("Failed to parse commit metadata")
            ));
        }

        #[test]
        fn test_null_value_in_array() {
            let metadata: Map<String, Value> = json!({
                "partitionToReplaceFileIds": {
                    "20": [null]
                }
            })
            .as_object()
            .unwrap()
            .clone();

            let result = replaced_file_groups_from_replace_commit(&metadata);
            // Serde will fail to parse this
            assert!(matches!(
                result,
                Err(CoreError::CommitMetadata(msg)) if msg.contains("Failed to parse commit metadata")
            ));
        }

        #[test]
        fn test_empty_partition() {
            let metadata: Map<String, Value> = json!({
                "partitionToReplaceFileIds": {
                    "": ["d398fae1-c0e6-4098-8124-f55f7098bdba-0"]
                }
            })
            .as_object()
            .unwrap()
            .clone();

            let result = replaced_file_groups_from_replace_commit(&metadata);
            assert!(result.is_ok());
            let file_groups = result.unwrap();
            assert_eq!(file_groups.len(), 1);
            let file_group = file_groups.iter().next().unwrap();
            assert_eq!(file_group.partition_path, EMPTY_PARTITION_PATH);
        }

        #[test]
        fn test_multiple_file_groups_same_partition() {
            let metadata: Map<String, Value> = json!({
                "partitionToReplaceFileIds": {
                    "20": [
                        "88163884-fef0-4aab-865d-c72327a8a1d5-0",
                        "88163884-fef0-4aab-865d-c72327a8a1d5-1"
                    ]
                }
            })
            .as_object()
            .unwrap()
            .clone();

            let result = replaced_file_groups_from_replace_commit(&metadata);
            assert!(result.is_ok());
            let file_groups = result.unwrap();
            let actual_partition_paths = file_groups
                .iter()
                .map(|fg| fg.partition_path.as_str())
                .collect::<Vec<_>>();
            assert_eq!(actual_partition_paths, &["20", "20"]);
        }

        #[test]
        fn test_empty_array() {
            let metadata: Map<String, Value> = json!({
                "partitionToReplaceFileIds": {
                    "20": []
                }
            })
            .as_object()
            .unwrap()
            .clone();

            let result = replaced_file_groups_from_replace_commit(&metadata);
            assert!(result.is_ok());
            let file_groups = result.unwrap();
            assert!(file_groups.is_empty());
        }

        #[test]
        fn test_valid_sample_data() {
            let metadata: Map<String, Value> = json!({
                "partitionToReplaceFileIds": {
                    "30": ["d398fae1-c0e6-4098-8124-f55f7098bdba-0"],
                    "20": ["88163884-fef0-4aab-865d-c72327a8a1d5-0"],
                    "10": ["4f2685a3-614f-49ca-9b2b-e1cb9fb61f27-0"]
                }
            })
            .as_object()
            .unwrap()
            .clone();

            let result = replaced_file_groups_from_replace_commit(&metadata);
            assert!(result.is_ok());
            let file_groups = result.unwrap();
            assert_eq!(file_groups.len(), 3);

            let expected_partitions = HashSet::from_iter(vec!["10", "20", "30"]);
            let actual_partitions =
                HashSet::<&str>::from_iter(file_groups.iter().map(|fg| fg.partition_path.as_str()));
            assert_eq!(actual_partitions, expected_partitions);
        }
    }
}
