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
//! Rollback timeline metadata (Java `HoodieRollbackPlan` / `HoodieRollbackMetadata`).

use std::collections::HashMap;
use std::io::Cursor;

use apache_avro::Reader as AvroReader;
use apache_avro::from_value;
use serde::{Deserialize, Serialize};

use crate::Result;
use crate::error::CoreError;

/// Java `HoodieInstantInfo`: the instant being rolled back.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HoodieInstantInfo {
    pub commit_time: String,
    pub action: String,
}

/// Java `HoodieRollbackRequest`: one partition's files to remove.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HoodieRollbackRequest {
    pub partition_path: String,
    pub file_id: Option<String>,
    pub latest_base_instant: Option<String>,
    pub files_to_be_deleted: Vec<String>,
    /// tv8+: log files created by the failed commit, mapped to their sizes.
    pub log_blocks_to_be_deleted: Option<HashMap<String, i64>>,
}

/// Java `HoodieRollbackPlan`: persisted into `{ts}.rollback.requested`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct HoodieRollbackPlan {
    #[serde(rename = "instantToRollback")]
    pub instant_to_rollback: Option<HoodieInstantInfo>,
    // Java field name is not camelCase.
    #[serde(rename = "RollbackRequests")]
    pub rollback_requests: Option<Vec<HoodieRollbackRequest>>,
    pub version: Option<i32>,
}

impl HoodieRollbackPlan {
    pub fn to_avro_bytes(&self) -> Result<Vec<u8>> {
        use crate::schema::avsc::{encode_with_schema, hoodie_rollback_plan_schema};
        encode_with_schema(self, hoodie_rollback_plan_schema()?)
    }

    pub fn from_avro_bytes(bytes: &[u8]) -> Result<Self> {
        read_single_record(bytes)
    }
}

/// Java `HoodieRollbackPartitionMetadata`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HoodieRollbackPartitionMetadata {
    pub partition_path: String,
    pub success_delete_files: Vec<String>,
    pub failed_delete_files: Vec<String>,
    /// Log files appended during rollback — always empty at tv8 (deletion-only).
    pub rollback_log_files: Option<HashMap<String, i64>>,
    pub log_files_from_failed_commit: Option<HashMap<String, i64>>,
}

/// Java `HoodieRollbackMetadata`: persisted into `{ts}_{completion}.rollback`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HoodieRollbackMetadata {
    pub start_rollback_time: String,
    pub time_taken_in_millis: i64,
    pub total_files_deleted: i32,
    pub commits_rollback: Vec<String>,
    pub partition_metadata: HashMap<String, HoodieRollbackPartitionMetadata>,
    pub version: Option<i32>,
    pub instants_rollback: Vec<HoodieInstantInfo>,
}

impl HoodieRollbackMetadata {
    pub fn to_avro_bytes(&self) -> Result<Vec<u8>> {
        use crate::schema::avsc::{encode_with_schema, hoodie_rollback_metadata_schema};
        encode_with_schema(self, hoodie_rollback_metadata_schema()?)
    }

    pub fn from_avro_bytes(bytes: &[u8]) -> Result<Self> {
        read_single_record(bytes)
    }
}

fn read_single_record<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    let reader = AvroReader::new(Cursor::new(bytes))
        .map_err(|e| CoreError::CommitMetadata(format!("Failed to create Avro reader: {e}")))?;
    let mut records = reader;
    let value = records
        .next()
        .ok_or_else(|| CoreError::CommitMetadata("Avro file contains no records".to_string()))?
        .map_err(|e| CoreError::CommitMetadata(format!("Failed to read Avro record: {e}")))?;
    from_value::<T>(&value)
        .map_err(|e| CoreError::CommitMetadata(format!("Failed to deserialize Avro value: {e}")))
}
