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
//! The compaction plan a `.compaction.requested` instant carries (Java
//! `HoodieCompactionPlan.avsc`).

use std::collections::HashMap;
use std::io::Cursor;

use apache_avro::Reader as AvroReader;
use apache_avro::from_value;
use serde::{Deserialize, Serialize};

use crate::Result;
use crate::error::CoreError;
use crate::schema::avsc::{encode_with_schema, hoodie_compaction_plan_schema};

/// One file group to compact: its latest base file (absent for a log-only
/// group) and the log files to fold into the new base file, as file names.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HoodieCompactionOperation {
    pub base_instant_time: Option<String>,
    pub delta_file_paths: Option<Vec<String>>,
    pub data_file_path: Option<String>,
    pub file_id: Option<String>,
    pub partition_path: Option<String>,
    pub metrics: Option<HashMap<String, f64>>,
    pub bootstrap_file_path: Option<String>,
}

/// Java `HoodieCompactionStrategy`: which compactor runs the plan.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HoodieCompactionStrategy {
    pub compactor_class_name: Option<String>,
    pub strategy_params: Option<HashMap<String, String>>,
    pub version: Option<i32>,
}

/// Java `HoodieCompactionPlan`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HoodieCompactionPlan {
    pub operations: Option<Vec<HoodieCompactionOperation>>,
    pub extra_metadata: Option<HashMap<String, String>>,
    pub version: Option<i32>,
    pub strategy: Option<HoodieCompactionStrategy>,
    pub preserve_hoodie_metadata: Option<bool>,
    pub missing_schedule_partitions: Option<Vec<String>>,
}

impl HoodieCompactionPlan {
    /// The plan version Java's `CompactionUtils.LATEST_COMPACTION_METADATA_VERSION` writes.
    pub const LATEST_VERSION: i32 = 2;
    /// `extraMetadata` key naming the writer that scheduled the plan.
    pub const COMPACTOR_KEY: &'static str = "compactor";
    /// [`Self::COMPACTOR_KEY`] value of the plans this crate schedules, the only
    /// ones it rolls back.
    pub const HUDI_RS_COMPACTOR: &'static str = "hudi-rs";

    /// Whether this crate scheduled the plan.
    pub fn is_scheduled_by_hudi_rs(&self) -> bool {
        self.extra_metadata
            .as_ref()
            .and_then(|metadata| metadata.get(Self::COMPACTOR_KEY))
            .is_some_and(|compactor| compactor == Self::HUDI_RS_COMPACTOR)
    }

    /// Serialize to Avro OCF with the vendored Java schema.
    pub fn to_avro_bytes(&self) -> Result<Vec<u8>> {
        encode_with_schema(self, hoodie_compaction_plan_schema()?)
    }

    /// Parse a plan from Avro OCF bytes.
    pub fn from_avro_bytes(bytes: &[u8]) -> Result<Self> {
        let mut records = AvroReader::new(Cursor::new(bytes))
            .map_err(|e| CoreError::CommitMetadata(format!("Failed to create Avro reader: {e}")))?;
        let value = records
            .next()
            .ok_or_else(|| CoreError::CommitMetadata("Avro file contains no records".to_string()))?
            .map_err(|e| CoreError::CommitMetadata(format!("Failed to read Avro record: {e}")))?;
        from_value::<Self>(&value).map_err(|e| {
            CoreError::CommitMetadata(format!("Failed to deserialize Avro value: {e}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A plan round-trips through the Java schema with every union branch a
    /// writer uses populated, including the double-valued metrics map.
    #[test]
    fn test_compaction_plan_avro_round_trip() {
        let plan = HoodieCompactionPlan {
            operations: Some(vec![
                HoodieCompactionOperation {
                    base_instant_time: Some("20260101000000000".to_string()),
                    delta_file_paths: Some(vec![
                        ".f1-0_20260101000001000.log.1_0-0-0".to_string(),
                        ".f1-0_20260101000002000.log.1_0-0-0".to_string(),
                    ]),
                    data_file_path: Some("f1-0_0-0-0_20260101000000000.parquet".to_string()),
                    file_id: Some("f1-0".to_string()),
                    partition_path: Some("city=sf".to_string()),
                    metrics: Some(HashMap::from([("TOTAL_LOG_FILES".to_string(), 2.0)])),
                    bootstrap_file_path: None,
                },
                HoodieCompactionOperation {
                    base_instant_time: Some("20260101000003000".to_string()),
                    delta_file_paths: Some(vec![".f2-0_20260101000003000.log.1_0-0-0".to_string()]),
                    data_file_path: None,
                    file_id: Some("f2-0".to_string()),
                    partition_path: Some(String::new()),
                    metrics: None,
                    bootstrap_file_path: None,
                },
            ]),
            extra_metadata: Some(HashMap::new()),
            version: Some(HoodieCompactionPlan::LATEST_VERSION),
            strategy: Some(HoodieCompactionStrategy {
                compactor_class_name: None,
                strategy_params: Some(HashMap::new()),
                version: Some(1),
            }),
            preserve_hoodie_metadata: Some(true),
            missing_schedule_partitions: None,
        };
        let bytes = plan.to_avro_bytes().unwrap();
        assert_eq!(&bytes[..4], b"Obj\x01");
        assert_eq!(HoodieCompactionPlan::from_avro_bytes(&bytes).unwrap(), plan);
    }

    #[test]
    fn test_compaction_plan_rejects_non_avro_bytes() {
        assert!(HoodieCompactionPlan::from_avro_bytes(b"not avro").is_err());
    }
}
