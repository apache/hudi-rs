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
//! Avro encoding for metadata table records written by hudi-rs.

use std::collections::HashMap;

use apache_avro::Schema;
use apache_avro::types::Value;

use crate::Result;
use crate::error::CoreError;
use crate::metadata::table::records::{FilesPartitionRecord, MetadataRecordType};

/// Minimal compatible `HoodieMetadataRecord` schema for the files partition.
pub const FILES_METADATA_AVRO_SCHEMA: &str = r#"{
  "type":"record",
  "name":"HoodieMetadataRecord",
  "namespace":"org.apache.hudi.metadata",
  "fields":[
    {"name":"key","type":"string"},
    {"name":"type","type":"int"},
    {"name":"filesystemMetadata","type":["null",{"type":"map","values":{
      "type":"record","name":"HoodieMetadataFileInfo",
      "fields":[
        {"name":"size","type":"long"},
        {"name":"isDeleted","type":"boolean"}
      ]
    }}],"default":null}
  ]
}"#;

/// A file entry stored in a files-partition metadata record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesMetadataEntry {
    /// File name or partition name.
    pub name: String,
    /// File size in bytes.
    pub size: i64,
    /// Whether this entry is a deletion marker.
    pub is_deleted: bool,
}

/// Placeholder payload for future column statistics encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnStatsMetadata;

/// Placeholder payload for future partition statistics encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionStatsMetadata;

/// Encode the `__all_partitions__` files-partition record.
pub fn encode_all_partitions(partitions: impl IntoIterator<Item = String>) -> Result<Vec<u8>> {
    let entries: Vec<FilesMetadataEntry> = partitions
        .into_iter()
        .map(|name| FilesMetadataEntry {
            name,
            size: 0,
            is_deleted: false,
        })
        .collect();
    encode_files_record(
        FilesPartitionRecord::ALL_PARTITIONS_KEY,
        MetadataRecordType::AllPartitions,
        entries,
    )
}

/// Encode a files-partition metadata record.
pub fn encode_files_record(
    key: &str,
    record_type: MetadataRecordType,
    entries: impl IntoIterator<Item = FilesMetadataEntry>,
) -> Result<Vec<u8>> {
    let schema = Schema::parse_str(FILES_METADATA_AVRO_SCHEMA)?;
    let mut filesystem_metadata = HashMap::new();
    for entry in entries {
        filesystem_metadata.insert(
            entry.name,
            Value::Record(vec![
                ("size".to_string(), Value::Long(entry.size)),
                ("isDeleted".to_string(), Value::Boolean(entry.is_deleted)),
            ]),
        );
    }
    let value = Value::Record(vec![
        ("key".to_string(), Value::String(key.to_string())),
        ("type".to_string(), Value::Int(record_type as i32)),
        (
            "filesystemMetadata".to_string(),
            Value::Union(1, Box::new(Value::Map(filesystem_metadata))),
        ),
    ]);
    apache_avro::to_avro_datum(&schema, value).map_err(CoreError::from)
}

/// Column-statistics writes are intentionally deferred beyond Phase B.
pub fn encode_column_stats(_metadata: ColumnStatsMetadata) -> Result<Vec<u8>> {
    Err(CoreError::Unsupported(
        "column_stats metadata writes are not implemented".to_string(),
    ))
}

/// Partition-statistics writes are intentionally deferred beyond Phase B.
pub fn encode_partition_stats(_metadata: PartitionStatsMetadata) -> Result<Vec<u8>> {
    Err(CoreError::Unsupported(
        "partition_stats metadata writes are not implemented".to_string(),
    ))
}
