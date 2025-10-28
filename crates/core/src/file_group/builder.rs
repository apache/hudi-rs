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
use crate::file_group::FileGroup;
use crate::metadata::commit::HoodieCommitMetadata;
use crate::metadata::replace_commit::HoodieReplaceCommitMetadata;
use crate::Result;
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::path::Path;

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

pub fn build_file_groups(commit_metadata: &Map<String, Value>) -> Result<HashSet<FileGroup>> {
    let metadata = HoodieCommitMetadata::from_json_map(commit_metadata)?;

    let mut file_groups = HashSet::new();

    for (partition, write_stat) in metadata.iter_write_stats() {
        let file_id = write_stat
            .file_id
            .as_ref()
            .ok_or_else(|| CoreError::CommitMetadata("Missing fileId in write stats".into()))?;

        let mut file_group = FileGroup::new(file_id.clone(), partition.clone());

        // Handle two cases:
        // 1. MOR table with baseFile and logFiles
        // 2. COW table with path only
        if let Some(base_file_name) = &write_stat.base_file {
            file_group.add_base_file_from_name(base_file_name)?;

            if let Some(log_file_names) = &write_stat.log_files {
                for log_file_name in log_file_names {
                    file_group.add_log_file_from_name(log_file_name)?;
                }
            } else {
                return Err(CoreError::CommitMetadata(
                    "Missing log files in write stats".into(),
                ));
            }
        } else {
            let path = write_stat
                .path
                .as_ref()
                .ok_or_else(|| CoreError::CommitMetadata("Missing path in write stats".into()))?;

            let file_name = Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| CoreError::CommitMetadata("Invalid file name in path".into()))?;

            file_group.add_base_file_from_name(file_name)?;
        }

        file_groups.insert(file_group);
    }

    Ok(file_groups)
}

pub fn build_replaced_file_groups(
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

#[cfg(test)]
mod tests {

    mod test_build_file_groups {
        use super::super::*;
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

            let result = build_file_groups(&metadata);
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

            let result = build_file_groups(&metadata);
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

            let result = build_file_groups(&metadata);
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

            let result = build_file_groups(&metadata);
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

            let result = build_file_groups(&metadata);
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

            let result = build_file_groups(&metadata);
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

            let result = build_file_groups(&metadata);
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
            let result = build_file_groups(&metadata);
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
    }

    mod test_build_replaced_file_groups {
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

            let result = build_replaced_file_groups(&metadata);
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

            let result = build_replaced_file_groups(&metadata);
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

            let result = build_replaced_file_groups(&metadata);
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

            let result = build_replaced_file_groups(&metadata);
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

            let result = build_replaced_file_groups(&metadata);
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

            let result = build_replaced_file_groups(&metadata);
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

            let result = build_replaced_file_groups(&metadata);
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

            let result = build_replaced_file_groups(&metadata);
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
