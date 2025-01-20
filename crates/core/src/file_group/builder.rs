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
use crate::Result;
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::path::Path;

pub fn build_file_groups(commit_metadata: &Map<String, Value>) -> Result<HashSet<FileGroup>> {
    let partition_stats = commit_metadata
        .get("partitionToWriteStats")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            CoreError::CommitMetadata("Invalid or missing partitionToWriteStats object".into())
        })?;

    let mut file_groups = HashSet::new();

    for (partition, write_stats_array) in partition_stats {
        let write_stats = write_stats_array
            .as_array()
            .ok_or_else(|| CoreError::CommitMetadata("Invalid write stats array".into()))?;

        for stat in write_stats {
            let file_id = stat
                .get("fileId")
                .and_then(|v| v.as_str())
                .ok_or_else(|| CoreError::CommitMetadata("Invalid fileId in write stats".into()))?;

            let path = stat
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| CoreError::CommitMetadata("Invalid path in write stats".into()))?;

            let file_name = Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| CoreError::CommitMetadata("Invalid file name in path".into()))?;

            let file_group = FileGroup::new_with_base_file_name(
                file_id.to_string(),
                partition.clone(),
                file_name,
            )?;
            file_groups.insert(file_group);
        }
    }

    Ok(file_groups)
}

pub fn build_replaced_file_groups(
    commit_metadata: &Map<String, Value>,
) -> Result<HashSet<FileGroup>> {
    let partition_to_replaced = commit_metadata
        .get("partitionToReplaceFileIds")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            CoreError::CommitMetadata("Invalid or missing partitionToReplaceFileIds object".into())
        })?;

    let mut file_groups = HashSet::new();

    for (partition, file_ids_value) in partition_to_replaced {
        let file_ids = file_ids_value
            .as_array()
            .ok_or_else(|| CoreError::CommitMetadata("Invalid file group ids array".into()))?;

        for file_id in file_ids {
            let id = file_id
                .as_str()
                .ok_or_else(|| CoreError::CommitMetadata("Invalid file group id string".into()))?;

            let file_group = FileGroup::new(id.to_string(), partition.clone());
            file_groups.insert(file_group);
        }
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
            assert!(matches!(result,
                Err(CoreError::CommitMetadata(msg))
                if msg == "Invalid or missing partitionToWriteStats object"));
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
                Err(CoreError::CommitMetadata(msg)) if msg == "Invalid write stats array"
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
                Err(CoreError::CommitMetadata(msg)) if msg == "Invalid fileId in write stats"
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
                Err(CoreError::CommitMetadata(msg)) if msg == "Invalid path in write stats"
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
            assert!(matches!(
                result,
                Err(CoreError::CommitMetadata(msg)) if msg == "Invalid fileId in write stats"
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
            assert!(matches!(
                result,
                Err(CoreError::CommitMetadata(msg)) if msg == "Invalid path in write stats"
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
            assert!(matches!(
                result,
                Err(CoreError::CommitMetadata(msg)) if msg == "Invalid or missing partitionToReplaceFileIds object"
            ));
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
                Err(CoreError::CommitMetadata(msg)) if msg == "Invalid file group ids array"
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
            assert!(matches!(
                result,
                Err(CoreError::CommitMetadata(msg)) if msg == "Invalid file group id string"
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
            assert!(matches!(
                result,
                Err(CoreError::CommitMetadata(msg)) if msg == "Invalid file group id string"
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
