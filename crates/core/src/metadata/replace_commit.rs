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
use crate::metadata::commit::HoodieWriteStat;
use crate::Result;
use apache_avro_derive::AvroSchema as DeriveAvroSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Represents the metadata for a Hudi Replace Commit
///
/// This is modeled from HoodieReplaceCommitMetadata.avsc.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveAvroSchema)]
#[serde(rename_all = "camelCase")]
#[avro(namespace = "org.apache.hudi.avro.model")]
pub struct HoodieReplaceCommitMetadata {
    // version: ["int","null"] with default 1 in Avro; we model as Option<i32>
    pub version: Option<i32>,
    #[avro(rename = "operationType")]
    pub operation_type: Option<String>,
    #[avro(rename = "partitionToWriteStats")]
    pub partition_to_write_stats: Option<HashMap<String, Vec<HoodieWriteStat>>>,
    pub compacted: Option<bool>,
    #[avro(rename = "extraMetadata")]
    pub extra_metadata: Option<HashMap<String, String>>,
    #[avro(rename = "partitionToReplaceFileIds")]
    pub partition_to_replace_file_ids: Option<HashMap<String, Vec<String>>>,
}

impl HoodieReplaceCommitMetadata {
    /// Parse replace commit metadata from a serde_json Map
    pub fn from_json_map(map: &Map<String, Value>) -> Result<Self> {
        serde_json::from_value(Value::Object(map.clone()))
            .map_err(|e| CoreError::CommitMetadata(format!("Failed to parse commit metadata: {}", e)))
    }

    /// Parse replace commit metadata from JSON bytes
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map_err(|e| CoreError::CommitMetadata(format!("Failed to parse commit metadata: {}", e)))
    }

    /// Iterate over all replace file IDs across all partitions
    pub fn iter_replace_file_ids(&self) -> impl Iterator<Item = (&String, &String)> {
        self.partition_to_replace_file_ids
            .iter()
            .flat_map(|replace_ids| {
                replace_ids
                    .iter()
                    .flat_map(|(partition, file_ids)| file_ids.iter().map(move |file_id| (partition, file_id)))
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
}
