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
use crate::Result;
use apache_avro::from_value;
use apache_avro::Reader as AvroReader;
use apache_avro_derive::AvroSchema as DeriveAvroSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::io::Cursor;

/// Represents statistics for a single file write operation in a commit
///
/// This struct is automatically derived to/from Avro schema using apache-avro-derive.
/// The Avro schema can be accessed via `HoodieWriteStat::get_schema()`.
///
/// Note: For v8+ tables with Avro format, additional fields may be present in the data
/// that are not captured here. Use `#[serde(default)]` to handle missing fields gracefully.
#[derive(Debug, Clone, Default, Serialize, Deserialize, DeriveAvroSchema)]
#[serde(rename_all = "camelCase", default)]
#[avro(namespace = "org.apache.hudi.avro.model")]
pub struct HoodieWriteStat {
    #[avro(rename = "fileId")]
    pub file_id: Option<String>,
    pub path: Option<String>,
    #[avro(rename = "baseFile")]
    pub base_file: Option<String>,
    #[avro(rename = "logFiles")]
    pub log_files: Option<Vec<String>>,
    #[avro(rename = "prevCommit")]
    pub prev_commit: Option<String>,
    #[avro(rename = "numWrites")]
    pub num_writes: Option<i64>,
    #[avro(rename = "numDeletes")]
    pub num_deletes: Option<i64>,
    #[avro(rename = "numUpdateWrites")]
    pub num_update_writes: Option<i64>,
    #[avro(rename = "numInserts")]
    pub num_inserts: Option<i64>,
    #[avro(rename = "totalWriteBytes")]
    pub total_write_bytes: Option<i64>,
    #[avro(rename = "totalWriteErrors")]
    pub total_write_errors: Option<i64>,
    // Additional fields from v8+ Avro schema
    #[avro(rename = "partitionPath")]
    pub partition_path: Option<String>,
    #[avro(rename = "totalLogRecords")]
    pub total_log_records: Option<i64>,
    #[avro(rename = "totalLogFiles")]
    pub total_log_files: Option<i64>,
    #[avro(rename = "totalUpdatedRecordsCompacted")]
    pub total_updated_records_compacted: Option<i64>,
    #[avro(rename = "totalLogBlocks")]
    pub total_log_blocks: Option<i64>,
    #[avro(rename = "totalCorruptLogBlock")]
    pub total_corrupt_log_block: Option<i64>,
    #[avro(rename = "totalRollbackBlocks")]
    pub total_rollback_blocks: Option<i64>,
    #[avro(rename = "fileSizeInBytes")]
    pub file_size_in_bytes: Option<i64>,
    #[avro(rename = "logVersion")]
    pub log_version: Option<i32>,
    #[avro(rename = "logOffset")]
    pub log_offset: Option<i64>,
    #[avro(rename = "prevBaseFile")]
    pub prev_base_file: Option<String>,
    #[avro(rename = "minEventTime")]
    pub min_event_time: Option<i64>,
    #[avro(rename = "maxEventTime")]
    pub max_event_time: Option<i64>,
    #[avro(rename = "totalLogFilesCompacted")]
    pub total_log_files_compacted: Option<i64>,
    #[avro(rename = "totalLogReadTimeMs")]
    pub total_log_read_time_ms: Option<i64>,
    #[avro(rename = "totalLogSizeCompacted")]
    pub total_log_size_compacted: Option<i64>,
    #[avro(rename = "tempPath")]
    pub temp_path: Option<String>,
    #[avro(rename = "numUpdates")]
    pub num_updates: Option<i64>,
}

/// Represents the metadata for a Hudi commit
///
/// This struct is automatically derived to/from Avro schema using apache-avro-derive.
/// The Avro schema can be accessed via `HoodieCommitMetadata::get_schema()`.
///
/// # Example
/// ```
/// use hudi_core::metadata::commit::HoodieCommitMetadata;
/// use apache_avro::schema::AvroSchema;
///
/// // Get the Avro schema
/// let schema = HoodieCommitMetadata::get_schema();
/// println!("Schema: {}", schema.canonical_form());
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize, DeriveAvroSchema)]
#[serde(rename_all = "camelCase", default)]
#[avro(namespace = "org.apache.hudi.avro.model")]
pub struct HoodieCommitMetadata {
    pub version: Option<i32>,
    #[avro(rename = "operationType")]
    pub operation_type: Option<String>,
    #[avro(rename = "partitionToWriteStats")]
    pub partition_to_write_stats: Option<HashMap<String, Vec<HoodieWriteStat>>>,
    #[avro(rename = "partitionToReplaceFileIds")]
    pub partition_to_replace_file_ids: Option<HashMap<String, Vec<String>>>,
    pub compacted: Option<bool>,
    #[avro(rename = "extraMetadata")]
    pub extra_metadata: Option<HashMap<String, String>>,
}

impl HoodieCommitMetadata {
    /// Parse commit metadata from a serde_json Map
    pub fn from_json_map(map: &Map<String, Value>) -> Result<Self> {
        serde_json::from_value(Value::Object(map.clone())).map_err(|e| {
            CoreError::CommitMetadata(format!("Failed to parse commit metadata: {}", e))
        })
    }

    /// Parse commit metadata from JSON bytes
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| {
            CoreError::CommitMetadata(format!("Failed to parse commit metadata: {}", e))
        })
    }

    /// Parse commit metadata from Avro bytes (v8+ format)
    ///
    /// The Avro data should be in Avro Object Container format with an embedded schema.
    /// This format is used by table version 8 and later for commit/deltacommit instants.
    pub fn from_avro_bytes(bytes: &[u8]) -> Result<Self> {
        let cursor = Cursor::new(bytes);
        let reader = AvroReader::new(cursor).map_err(|e| {
            CoreError::CommitMetadata(format!("Failed to create Avro reader: {}", e))
        })?;

        // The commit metadata file should contain exactly one record
        let mut records = reader;
        let value = records
            .next()
            .ok_or_else(|| CoreError::CommitMetadata("Avro file contains no records".to_string()))?
            .map_err(|e| CoreError::CommitMetadata(format!("Failed to read Avro record: {}", e)))?;

        from_value::<Self>(&value).map_err(|e| {
            CoreError::CommitMetadata(format!("Failed to deserialize Avro value: {}", e))
        })
    }

    /// Convert commit metadata to a JSON Map for compatibility with existing code
    ///
    /// This is useful when the metadata is read from Avro format but needs to be
    /// processed by code that expects a serde_json Map.
    pub fn to_json_map(&self) -> Result<Map<String, Value>> {
        let value = serde_json::to_value(self).map_err(|e| {
            CoreError::CommitMetadata(format!("Failed to convert to JSON value: {}", e))
        })?;
        match value {
            Value::Object(map) => Ok(map),
            _ => Err(CoreError::CommitMetadata(
                "Expected JSON object".to_string(),
            )),
        }
    }

    /// Get the write stats for a specific partition
    pub fn get_partition_write_stats(&self, partition: &str) -> Option<&Vec<HoodieWriteStat>> {
        self.partition_to_write_stats
            .as_ref()
            .and_then(|stats| stats.get(partition))
    }

    /// Get all partitions with write stats
    pub fn get_partitions_with_writes(&self) -> Vec<String> {
        self.partition_to_write_stats
            .as_ref()
            .map(|stats| stats.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Get the file IDs to be replaced for a specific partition
    pub fn get_partition_replace_file_ids(&self, partition: &str) -> Option<&Vec<String>> {
        self.partition_to_replace_file_ids
            .as_ref()
            .and_then(|ids| ids.get(partition))
    }

    /// Get all partitions with file replacements
    pub fn get_partitions_with_replacements(&self) -> Vec<String> {
        self.partition_to_replace_file_ids
            .as_ref()
            .map(|ids| ids.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Iterate over all write stats across all partitions
    pub fn iter_write_stats(&self) -> impl Iterator<Item = (&String, &HoodieWriteStat)> {
        self.partition_to_write_stats
            .as_ref()
            .into_iter()
            .flat_map(|stats| {
                stats.iter().flat_map(|(partition, write_stats)| {
                    write_stats.iter().map(move |stat| (partition, stat))
                })
            })
    }

    /// Iterate over all replace file IDs across all partitions
    pub fn iter_replace_file_ids(&self) -> impl Iterator<Item = (&String, &String)> {
        self.partition_to_replace_file_ids
            .as_ref()
            .into_iter()
            .flat_map(|replace_ids| {
                replace_ids.iter().flat_map(|(partition, file_ids)| {
                    file_ids.iter().map(move |file_id| (partition, file_id))
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{AvroSchema, Schema};
    use serde_json::json;

    #[test]
    fn test_parse_commit_metadata() {
        let json = json!({
            "version": 1,
            "operationType": "UPSERT",
            "partitionToWriteStats": {
                "byteField=20/shortField=100": [{
                    "fileId": "bb7c3a45-387f-490d-aab2-981c3f1a8ada-0",
                    "path": "byteField=20/shortField=100/bb7c3a45-387f-490d-aab2-981c3f1a8ada-0_0-140-198_20240418173213674.parquet",
                    "numWrites": 100,
                    "totalWriteBytes": 1024
                }]
            },
            "compacted": false
        });

        let metadata: HoodieCommitMetadata = serde_json::from_value(json).unwrap();
        assert_eq!(metadata.version, Some(1));
        assert_eq!(metadata.operation_type, Some("UPSERT".to_string()));
        assert!(metadata.partition_to_write_stats.is_some());

        let stats = metadata
            .get_partition_write_stats("byteField=20/shortField=100")
            .unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(
            stats[0].file_id,
            Some("bb7c3a45-387f-490d-aab2-981c3f1a8ada-0".to_string())
        );
    }

    #[test]
    fn test_parse_replace_file_ids() {
        let json = json!({
            "partitionToReplaceFileIds": {
                "30": ["d398fae1-c0e6-4098-8124-f55f7098bdba-0"],
                "20": ["88163884-fef0-4aab-865d-c72327a8a1d5-0"]
            }
        });

        let metadata: HoodieCommitMetadata = serde_json::from_value(json).unwrap();

        let file_ids = metadata.get_partition_replace_file_ids("30").unwrap();
        assert_eq!(file_ids.len(), 1);
        assert_eq!(file_ids[0], "d398fae1-c0e6-4098-8124-f55f7098bdba-0");
    }

    #[test]
    fn test_iter_write_stats() {
        let json = json!({
            "partitionToWriteStats": {
                "p1": [{
                    "fileId": "file1",
                    "path": "p1/file1.parquet"
                }],
                "p2": [{
                    "fileId": "file2",
                    "path": "p2/file2.parquet"
                }]
            }
        });

        let metadata: HoodieCommitMetadata = serde_json::from_value(json).unwrap();
        let count = metadata.iter_write_stats().count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_avro_schema_generation() {
        // Test that the derived Avro schema can be generated
        let schema = HoodieCommitMetadata::get_schema();

        // Verify it's a record type
        if let Schema::Record(ref record) = schema {
            assert_eq!(record.name.name, "HoodieCommitMetadata");
            assert_eq!(
                record.name.namespace,
                Some("org.apache.hudi.avro.model".to_string())
            );

            // Verify key fields exist
            let field_names: Vec<_> = record.fields.iter().map(|f| f.name.as_str()).collect();
            assert!(field_names.contains(&"version"));
            assert!(field_names.contains(&"operationType"));
            assert!(field_names.contains(&"partitionToWriteStats"));
            assert!(field_names.contains(&"partitionToReplaceFileIds"));
            assert!(field_names.contains(&"compacted"));
            assert!(field_names.contains(&"extraMetadata"));
        } else {
            panic!("Expected Record schema");
        }

        // Print schema for verification (useful for debugging)
        println!("Generated Avro Schema:\n{}", schema.canonical_form());
    }

    #[test]
    fn test_write_stat_avro_schema() {
        // Test that HoodieWriteStat also has proper Avro schema
        let schema = HoodieWriteStat::get_schema();

        if let Schema::Record(ref record) = schema {
            assert_eq!(record.name.name, "HoodieWriteStat");
            assert_eq!(
                record.name.namespace,
                Some("org.apache.hudi.avro.model".to_string())
            );

            let field_names: Vec<_> = record.fields.iter().map(|f| f.name.as_str()).collect();
            assert!(field_names.contains(&"fileId"));
            assert!(field_names.contains(&"path"));
            assert!(field_names.contains(&"baseFile"));
            assert!(field_names.contains(&"logFiles"));
        } else {
            panic!("Expected Record schema");
        }
    }

    #[test]
    fn test_from_json_bytes() {
        let json_str = r#"{
            "version": 1,
            "operationType": "UPSERT",
            "partitionToWriteStats": {
                "p1": [{
                    "fileId": "file1",
                    "path": "p1/file1.parquet"
                }]
            },
            "compacted": false
        }"#;

        let metadata = HoodieCommitMetadata::from_json_bytes(json_str.as_bytes()).unwrap();
        assert_eq!(metadata.version, Some(1));
        assert_eq!(metadata.operation_type, Some("UPSERT".to_string()));
    }

    #[test]
    fn test_from_json_bytes_invalid() {
        let invalid_json = b"invalid json";
        let result = HoodieCommitMetadata::from_json_bytes(invalid_json);
        assert!(result.is_err());
        assert!(matches!(result, Err(CoreError::CommitMetadata(_))));
    }

    #[test]
    fn test_get_partition_write_stats() {
        let json = json!({
            "partitionToWriteStats": {
                "p1": [{
                    "fileId": "file1",
                    "path": "p1/file1.parquet"
                }],
                "p2": [{
                    "fileId": "file2",
                    "path": "p2/file2.parquet"
                }]
            }
        });

        let metadata: HoodieCommitMetadata = serde_json::from_value(json).unwrap();

        // Test getting existing partition
        let p1_stats = metadata.get_partition_write_stats("p1").unwrap();
        assert_eq!(p1_stats.len(), 1);
        assert_eq!(p1_stats[0].file_id, Some("file1".to_string()));

        // Test getting non-existent partition
        assert!(metadata.get_partition_write_stats("p3").is_none());
    }

    #[test]
    fn test_get_partitions_with_writes() {
        let json = json!({
            "partitionToWriteStats": {
                "p1": [{
                    "fileId": "file1",
                    "path": "p1/file1.parquet"
                }],
                "p2": [{
                    "fileId": "file2",
                    "path": "p2/file2.parquet"
                }]
            }
        });

        let metadata: HoodieCommitMetadata = serde_json::from_value(json).unwrap();
        let mut partitions = metadata.get_partitions_with_writes();
        partitions.sort();
        assert_eq!(partitions, vec!["p1".to_string(), "p2".to_string()]);
    }

    #[test]
    fn test_get_partitions_with_writes_empty() {
        let json = json!({});
        let metadata: HoodieCommitMetadata = serde_json::from_value(json).unwrap();
        let partitions = metadata.get_partitions_with_writes();
        assert_eq!(partitions.len(), 0);
    }

    #[test]
    fn test_get_partition_replace_file_ids() {
        let json = json!({
            "partitionToReplaceFileIds": {
                "30": ["d398fae1-c0e6-4098-8124-f55f7098bdba-0"],
                "20": ["88163884-fef0-4aab-865d-c72327a8a1d5-0"]
            }
        });

        let metadata: HoodieCommitMetadata = serde_json::from_value(json).unwrap();

        // Test getting existing partition
        let file_ids_30 = metadata.get_partition_replace_file_ids("30").unwrap();
        assert_eq!(file_ids_30.len(), 1);
        assert_eq!(file_ids_30[0], "d398fae1-c0e6-4098-8124-f55f7098bdba-0");

        // Test getting non-existent partition
        assert!(metadata.get_partition_replace_file_ids("40").is_none());
    }

    #[test]
    fn test_get_partitions_with_replacements() {
        let json = json!({
            "partitionToReplaceFileIds": {
                "30": ["file1"],
                "20": ["file2"]
            }
        });

        let metadata: HoodieCommitMetadata = serde_json::from_value(json).unwrap();
        let mut partitions = metadata.get_partitions_with_replacements();
        partitions.sort();
        assert_eq!(partitions, vec!["20".to_string(), "30".to_string()]);
    }

    #[test]
    fn test_get_partitions_with_replacements_empty() {
        let json = json!({});
        let metadata: HoodieCommitMetadata = serde_json::from_value(json).unwrap();
        let partitions = metadata.get_partitions_with_replacements();
        assert_eq!(partitions.len(), 0);
    }

    #[test]
    fn test_iter_replace_file_ids() {
        let json = json!({
            "partitionToReplaceFileIds": {
                "p1": ["file1", "file2"],
                "p2": ["file3"]
            }
        });

        let metadata: HoodieCommitMetadata = serde_json::from_value(json).unwrap();
        let count = metadata.iter_replace_file_ids().count();
        assert_eq!(count, 3);

        let file_ids: Vec<_> = metadata
            .iter_replace_file_ids()
            .map(|(_, file_id)| file_id.as_str())
            .collect();
        assert!(file_ids.contains(&"file1"));
        assert!(file_ids.contains(&"file2"));
        assert!(file_ids.contains(&"file3"));
    }

    #[test]
    fn test_iter_replace_file_ids_empty() {
        let json = json!({});
        let metadata: HoodieCommitMetadata = serde_json::from_value(json).unwrap();
        let count = metadata.iter_replace_file_ids().count();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_parse_v6_commit_json() {
        // Test parsing v6 COW table commit metadata (JSON format)
        let file_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/data/commit_metadata/v6_commit.json"
        );
        let bytes = std::fs::read(file_path).expect("Failed to read test fixture");

        let metadata = HoodieCommitMetadata::from_json_bytes(&bytes)
            .expect("Failed to parse v6 commit metadata");

        // Validate important fields
        assert_eq!(metadata.operation_type, Some("UPSERT".to_string()));
        assert_eq!(metadata.compacted, Some(false));

        // Validate partition write stats
        let write_stats = metadata
            .get_partition_write_stats("")
            .expect("Should have write stats for empty partition");
        assert_eq!(write_stats.len(), 1);

        let stat = &write_stats[0];
        assert_eq!(
            stat.file_id,
            Some("a079bdb3-731c-4894-b855-abfcd6921007-0".to_string())
        );
        assert_eq!(stat.num_writes, Some(4));
        assert_eq!(stat.num_inserts, Some(1));
        assert_eq!(stat.num_deletes, Some(0));
        assert_eq!(stat.num_update_writes, Some(1));
        assert_eq!(stat.total_write_bytes, Some(441520));
        assert_eq!(stat.prev_commit, Some("20240418173550988".to_string()));
    }

    #[test]
    fn test_parse_v8_deltacommit_avro() {
        // Test parsing v8 MOR table deltacommit metadata (Avro format)
        let file_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/data/commit_metadata/v8_deltacommit.avro"
        );
        let bytes = std::fs::read(file_path).expect("Failed to read test fixture");

        let metadata = HoodieCommitMetadata::from_avro_bytes(&bytes)
            .expect("Failed to parse v8 deltacommit metadata");

        // Validate important fields
        assert_eq!(metadata.operation_type, Some("UPSERT".to_string()));

        // Validate partition write stats - v8 MOR table should have stats
        assert!(metadata.partition_to_write_stats.is_some());
        let partition_map = metadata.partition_to_write_stats.as_ref().unwrap();

        // Should have write stats for multiple partitions
        assert!(
            !partition_map.is_empty(),
            "Should have write stats for at least one partition"
        );

        // Validate at least one partition has valid stats
        for (partition, stats) in partition_map {
            if !stats.is_empty() {
                let stat = &stats[0];
                // file_id should be present
                assert!(
                    stat.file_id.is_some(),
                    "file_id should be present in partition {}",
                    partition
                );
                // For UPSERT operation, num_inserts or num_update_writes should be present
                if let Some(num_inserts) = stat.num_inserts {
                    assert!(num_inserts >= 0, "num_inserts should be >= 0");
                }
                if let Some(num_updates) = stat.num_update_writes {
                    assert!(num_updates >= 0, "num_update_writes should be >= 0");
                }
                // Verify partition_path field exists (v8+ specific field)
                assert!(
                    stat.partition_path.is_some(),
                    "partition_path should be present in v8+ format"
                );
                break;
            }
        }
    }

    #[test]
    fn test_to_json_map() {
        let metadata = HoodieCommitMetadata {
            version: Some(1),
            operation_type: Some("INSERT".to_string()),
            ..Default::default()
        };

        let json_map = metadata.to_json_map().unwrap();
        assert!(json_map.contains_key("version"));
        assert!(json_map.contains_key("operationType"));
    }

    #[test]
    fn test_from_json_map() {
        let mut map = Map::new();
        map.insert("version".to_string(), json!(2));
        map.insert("operationType".to_string(), json!("UPSERT"));

        let metadata = HoodieCommitMetadata::from_json_map(&map).unwrap();
        assert_eq!(metadata.version, Some(2));
        assert_eq!(metadata.operation_type, Some("UPSERT".to_string()));
    }

    #[test]
    fn test_get_partitions_with_replacements_sorting() {
        let json = json!({
            "partitionToReplaceFileIds": {
                "p1": ["file1"],
                "p2": ["file2", "file3"]
            }
        });

        let metadata: HoodieCommitMetadata = serde_json::from_value(json).unwrap();
        let mut partitions = metadata.get_partitions_with_replacements();
        partitions.sort();
        assert_eq!(partitions, vec!["p1".to_string(), "p2".to_string()]);
    }

    #[test]
    fn test_iter_replace_file_ids_multiple_partitions() {
        let json = json!({
            "partitionToReplaceFileIds": {
                "p1": ["file1"],
                "p2": ["file2", "file3"]
            }
        });

        let metadata: HoodieCommitMetadata = serde_json::from_value(json).unwrap();
        let count = metadata.iter_replace_file_ids().count();
        assert_eq!(count, 3); // 1 from p1, 2 from p2
    }
}
