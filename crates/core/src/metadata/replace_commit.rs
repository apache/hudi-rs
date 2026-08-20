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
use crate::metadata::commit::HoodieWriteStat;
use apache_avro::Reader as AvroReader;
use apache_avro::from_value;
use apache_avro_derive::AvroSchema as DeriveAvroSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::io::Cursor;

/// Represents the metadata for a Hudi Replace Commit
///
/// This is modeled from HoodieReplaceCommitMetadata.avsc.
#[derive(Debug, Clone, Default, Serialize, Deserialize, DeriveAvroSchema)]
#[serde(rename_all = "camelCase", default)]
#[avro(namespace = "org.apache.hudi.avro.model")]
pub struct HoodieReplaceCommitMetadata {
    // Field order matches Java HoodieReplaceCommitMetadata.avsc.
    #[avro(rename = "partitionToWriteStats")]
    pub partition_to_write_stats: Option<HashMap<String, Vec<HoodieWriteStat>>>,
    pub compacted: Option<bool>,
    #[avro(rename = "extraMetadata")]
    pub extra_metadata: Option<HashMap<String, String>>,
    pub version: Option<i32>,
    #[avro(rename = "operationType")]
    pub operation_type: Option<String>,
    #[avro(rename = "partitionToReplaceFileIds")]
    pub partition_to_replace_file_ids: Option<HashMap<String, Vec<String>>>,
}

/// Java `HoodieRequestedReplaceMetadata`: the "plan" persisted into a
/// `.replacecommit.requested` instant. Our writers only produce
/// insert-overwrite operations, so `clusteringPlan` is always null.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HoodieRequestedReplaceMetadata {
    pub operation_type: Option<String>,
    pub clustering_plan: Option<Value>,
    pub extra_metadata: Option<HashMap<String, String>>,
    pub version: Option<i32>,
}

impl HoodieRequestedReplaceMetadata {
    /// Requested-replace plan for an insert-overwrite operation.
    pub fn for_operation(operation: &str) -> Self {
        Self {
            operation_type: Some(operation.to_string()),
            clustering_plan: None,
            extra_metadata: Some(HashMap::new()),
            version: Some(1),
        }
    }

    /// Serialize to Avro OCF using the vendored Java schema.
    pub fn to_avro_bytes(&self) -> Result<Vec<u8>> {
        use crate::schema::avsc::{encode_with_schema, hoodie_requested_replace_metadata_schema};
        encode_with_schema(self, hoodie_requested_replace_metadata_schema()?)
    }

    /// Parse a requested-replace plan from Avro OCF bytes.
    pub fn from_avro_bytes(bytes: &[u8]) -> Result<Self> {
        let reader = AvroReader::new(Cursor::new(bytes))
            .map_err(|e| CoreError::CommitMetadata(format!("Failed to create Avro reader: {e}")))?;
        let mut records = reader;
        let value = records
            .next()
            .ok_or_else(|| CoreError::CommitMetadata("Avro file contains no records".to_string()))?
            .map_err(|e| CoreError::CommitMetadata(format!("Failed to read Avro record: {e}")))?;
        from_value::<Self>(&value).map_err(|e| {
            CoreError::CommitMetadata(format!("Failed to deserialize Avro value: {e}"))
        })
    }
}

impl HoodieReplaceCommitMetadata {
    /// Parse replace commit metadata from a serde_json Map
    pub fn from_json_map(map: &Map<String, Value>) -> Result<Self> {
        serde_json::from_value(Value::Object(map.clone()))
            .map_err(|e| CoreError::CommitMetadata(format!("Failed to parse commit metadata: {e}")))
    }

    /// Parse replace commit metadata from JSON bytes
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map_err(|e| CoreError::CommitMetadata(format!("Failed to parse commit metadata: {e}")))
    }

    /// Parse replace-commit metadata from Avro OCF bytes (layout v2).
    pub fn from_avro_bytes(bytes: &[u8]) -> Result<Self> {
        let cursor = Cursor::new(bytes);
        let reader = AvroReader::new(cursor)
            .map_err(|e| CoreError::CommitMetadata(format!("Failed to create Avro reader: {e}")))?;
        let mut records = reader;
        let value = records
            .next()
            .ok_or_else(|| CoreError::CommitMetadata("Avro file contains no records".to_string()))?
            .map_err(|e| CoreError::CommitMetadata(format!("Failed to read Avro record: {e}")))?;
        from_value::<Self>(&value).map_err(|e| {
            CoreError::CommitMetadata(format!("Failed to deserialize Avro value: {e}"))
        })
    }

    /// Convert to a JSON Map for timeline loaders.
    pub fn to_json_map(&self) -> Result<Map<String, Value>> {
        let value = serde_json::to_value(self).map_err(|e| {
            CoreError::CommitMetadata(format!("Failed to convert to JSON value: {e}"))
        })?;
        match value {
            Value::Object(map) => Ok(map),
            _ => Err(CoreError::CommitMetadata(
                "Expected JSON object".to_string(),
            )),
        }
    }

    /// Serialize replace-commit metadata to Avro OCF using the vendored Java schema.
    pub fn to_avro_bytes(&self) -> Result<Vec<u8>> {
        use crate::schema::avsc::{encode_with_schema, hoodie_replace_commit_metadata_schema};
        encode_with_schema(self, hoodie_replace_commit_metadata_schema()?)
    }

    /// Serialize to JSON bytes (timeline layout v1).
    pub fn to_json_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| {
            CoreError::CommitMetadata(format!("Failed to serialize replace metadata: {e}"))
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
    use serde_json::json;

    #[test]
    fn test_parse_replace_commit() {
        let json = json!({
            "partitionToReplaceFileIds": {
                "30": ["a-0"],
                "20": ["b-0", "b-1"],
                "": ["c-0"]
            },
            "extraMetadata": {"k":"v"},
            "version": 1,
            "operationType": "REPLACE_COMMIT"
        });

        let metadata: HoodieReplaceCommitMetadata = serde_json::from_value(json).unwrap();
        let ids: Vec<(&String, &String)> = metadata.iter_replace_file_ids().collect();
        assert_eq!(ids.len(), 4);
    }

    #[test]
    fn test_from_json_bytes() {
        let json_str = r#"{
            "partitionToReplaceFileIds": {
                "30": ["a-0"],
                "20": ["b-0"]
            },
            "version": 1,
            "operationType": "REPLACE_COMMIT"
        }"#;

        let metadata = HoodieReplaceCommitMetadata::from_json_bytes(json_str.as_bytes()).unwrap();
        assert_eq!(metadata.version, Some(1));
        assert_eq!(metadata.operation_type, Some("REPLACE_COMMIT".to_string()));
    }

    #[test]
    fn test_from_json_bytes_invalid() {
        let invalid_json = b"invalid json";
        let result = HoodieReplaceCommitMetadata::from_json_bytes(invalid_json);
        assert!(result.is_err());
        assert!(matches!(result, Err(CoreError::CommitMetadata(_))));
    }

    #[test]
    fn test_iter_replace_file_ids_empty() {
        let json = json!({});
        let metadata: HoodieReplaceCommitMetadata = serde_json::from_value(json).unwrap();
        let count = metadata.iter_replace_file_ids().count();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_avro_roundtrip_java_schema() {
        let metadata = HoodieReplaceCommitMetadata {
            version: Some(1),
            operation_type: Some("INSERT_OVERWRITE".to_string()),
            partition_to_write_stats: Some(HashMap::new()),
            compacted: Some(false),
            extra_metadata: Some(HashMap::new()),
            partition_to_replace_file_ids: Some(HashMap::from([(
                "p0".to_string(),
                vec!["fid-0".to_string()],
            )])),
        };
        let bytes = metadata.to_avro_bytes().unwrap();
        let parsed = HoodieReplaceCommitMetadata::from_avro_bytes(&bytes).unwrap();
        assert_eq!(parsed.operation_type, metadata.operation_type);
        assert_eq!(
            parsed.partition_to_replace_file_ids,
            metadata.partition_to_replace_file_ids
        );
    }
}
