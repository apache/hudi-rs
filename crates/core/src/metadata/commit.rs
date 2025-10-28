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
use apache_avro_derive::AvroSchema as DeriveAvroSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Represents statistics for a single file write operation in a commit
///
/// This struct is automatically derived to/from Avro schema using apache-avro-derive.
/// The Avro schema can be accessed via `HoodieWriteStat::get_schema()`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveAvroSchema)]
#[serde(rename_all = "camelCase")]
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
}

/// Represents the metadata for a Hudi commit
///
/// This struct is automatically derived to/from Avro schema using apache-avro-derive.
/// The Avro schema can be accessed via `HoodieCommitMetadata::get_schema()`.
///
/// # Example
/// ```
/// use hudi_core::metadata::commit::HoodieCommitMetadata;
/// use apache_avro::Schema;
///
/// // Get the Avro schema
/// let schema = HoodieCommitMetadata::get_schema();
/// println!("Schema: {}", schema.canonical_form());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, DeriveAvroSchema)]
#[serde(rename_all = "camelCase")]
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
        self.partition_to_write_stats.iter().flat_map(|stats| {
            stats.iter().flat_map(|(partition, write_stats)| {
                write_stats.iter().map(move |stat| (partition, stat))
            })
        })
    }

    /// Iterate over all replace file IDs across all partitions
    pub fn iter_replace_file_ids(&self) -> impl Iterator<Item = (&String, &String)> {
        self.partition_to_replace_file_ids
            .iter()
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
}
