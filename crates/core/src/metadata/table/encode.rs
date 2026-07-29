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
//!
//! Uses the vendored Java `HoodieMetadata.avsc` (`org.apache.hudi.avro.model`).

use std::collections::HashMap;
use std::sync::OnceLock;

use apache_avro::Schema;
use apache_avro::types::Value;

use crate::Result;
use crate::error::CoreError;
use crate::metadata::table::records::{FilesPartitionRecord, MetadataRecordType};
use crate::schema::avsc::{hoodie_metadata_schema, strip_avro_line_comments};

/// JSON text of the metadata schema embedded into HFile file-info under key `schema`.
///
/// Matches Java `HoodieMetadataRecord.SCHEMA$`: string fields and map key types carry
/// `"avro.java.string":"String"` so Spark's `GenericDatumReader` materializes
/// `java.lang.String` instead of `Utf8` (required by `constructFilesMetadataPayload`
/// and `fetchBaseFileRecordsByKeys`).
pub fn hoodie_metadata_schema_json() -> &'static str {
    static JSON: OnceLock<String> = OnceLock::new();
    JSON.get_or_init(|| {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/HoodieMetadata.avsc"
        ));
        let cleaned = strip_avro_line_comments(raw);
        let json = cleaned
            .find('{')
            .map(|i| cleaned[i..].trim().to_string())
            .unwrap_or(cleaned);
        inject_avro_java_string_props(&json)
    })
    .as_str()
}

/// Annotate Avro JSON so Java readers decode strings as `java.lang.String`.
fn inject_avro_java_string_props(schema_json: &str) -> String {
    let mut value: serde_json::Value =
        serde_json::from_str(schema_json).expect("HoodieMetadata.avsc must be valid JSON");
    inject_avro_java_string_props_value(&mut value);
    serde_json::to_string(&value).expect("serialize annotated metadata schema")
}

fn inject_avro_java_string_props_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            match map.get("type").cloned() {
                Some(serde_json::Value::String(t)) if t == "string" => {
                    // Avoid re-expanding `{"type":"string","avro.java.string":"String"}`.
                    if !map.contains_key("avro.java.string") {
                        map.insert(
                            "type".to_string(),
                            serde_json::json!({"type": "string", "avro.java.string": "String"}),
                        );
                    }
                }
                Some(serde_json::Value::String(t)) if t == "map" => {
                    map.entry("avro.java.string".to_string()).or_insert_with(|| {
                        serde_json::Value::String("String".to_string())
                    });
                }
                _ => {}
            }
            for child in map.values_mut() {
                inject_avro_java_string_props_value(child);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                if item.as_str() == Some("string") {
                    *item = serde_json::json!({"type": "string", "avro.java.string": "String"});
                } else {
                    inject_avro_java_string_props_value(item);
                }
            }
        }
        _ => {}
    }
}

/// Alias kept for call sites that embed the files-partition schema into HFiles.
pub fn files_metadata_avro_schema_json() -> &'static str {
    hoodie_metadata_schema_json()
}

/// Alias kept for call sites that embed the record_index schema into HFiles.
pub fn record_index_metadata_avro_schema_json() -> &'static str {
    hoodie_metadata_schema_json()
}

fn metadata_schema() -> Result<&'static Schema> {
    hoodie_metadata_schema()
}

fn null_union() -> Value {
    Value::Union(0, Box::new(Value::Null))
}

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
///
/// `key` is the HFile row key and is intentionally **not** written into the Avro
/// payload. Java's `HoodieAvroHFileWriter` clears the Avro `key` field to `""` and
/// stores the real key only in the HFile key; on read,
/// `HoodieAvroHFileReaderImplBase.deserialize` reinjects a Java `String` from the
/// HFile key. Embedding a non-empty Avro key makes GenericDatumReader produce
/// `Utf8`, and Spark then `ClassCastException`s in `fetchBaseFileRecordsByKeys`.
pub fn encode_files_record(
    key: &str,
    record_type: MetadataRecordType,
    entries: impl IntoIterator<Item = FilesMetadataEntry>,
) -> Result<Vec<u8>> {
    let _hfile_row_key = key;
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
        ("key".to_string(), Value::String(String::new())),
        ("type".to_string(), Value::Int(record_type as i32)),
        (
            "filesystemMetadata".to_string(),
            Value::Union(1, Box::new(Value::Map(filesystem_metadata))),
        ),
        ("BloomFilterMetadata".to_string(), null_union()),
        ("ColumnStatsMetadata".to_string(), null_union()),
        ("recordIndexMetadata".to_string(), null_union()),
        ("SecondaryIndexMetadata".to_string(), null_union()),
    ]);
    apache_avro::to_avro_datum(metadata_schema()?, value).map_err(CoreError::from)
}

/// A location stored in the record_index metadata partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordIndexEntry {
    pub record_key: String,
    pub partition_path: String,
    pub file_id: String,
    /// Instant time as epoch millis (Hudi RLI wire format).
    pub instant_time_millis: i64,
    /// When true, this entry deletes the key from RLI.
    pub is_deleted: bool,
}

/// Encode a record-index metadata record (type = 5). Empty payload marks a delete.
pub fn encode_record_index_entry(entry: &RecordIndexEntry) -> Result<Vec<u8>> {
    let rli_payload = if entry.is_deleted {
        null_union()
    } else {
        let (high, low, file_index, file_id_str, encoding) =
            encode_file_id_for_record_index(&entry.file_id);
        Value::Union(
            1,
            Box::new(Value::Record(vec![
                (
                    "partitionName".to_string(),
                    Value::Union(1, Box::new(Value::String(entry.partition_path.clone()))),
                ),
                (
                    "fileIdHighBits".to_string(),
                    Value::Union(1, Box::new(Value::Long(high))),
                ),
                (
                    "fileIdLowBits".to_string(),
                    Value::Union(1, Box::new(Value::Long(low))),
                ),
                (
                    "fileIndex".to_string(),
                    Value::Union(1, Box::new(Value::Int(file_index))),
                ),
                (
                    "fileId".to_string(),
                    Value::Union(1, Box::new(Value::String(file_id_str))),
                ),
                (
                    "instantTime".to_string(),
                    Value::Union(1, Box::new(Value::Long(entry.instant_time_millis))),
                ),
                // 0 = UUID bits; 1 = raw fileId string (Java HoodieRecordIndexInfo).
                ("fileIdEncoding".to_string(), Value::Int(encoding)),
                ("position".to_string(), null_union()),
            ])),
        )
    };
    // Empty Avro key — real key lives only on the HFile key (see encode_files_record).
    let value = Value::Record(vec![
        ("key".to_string(), Value::String(String::new())),
        (
            "type".to_string(),
            Value::Int(MetadataRecordType::RecordIndex as i32),
        ),
        ("filesystemMetadata".to_string(), null_union()),
        ("BloomFilterMetadata".to_string(), null_union()),
        ("ColumnStatsMetadata".to_string(), null_union()),
        ("recordIndexMetadata".to_string(), rli_payload),
        ("SecondaryIndexMetadata".to_string(), null_union()),
    ]);
    apache_avro::to_avro_datum(metadata_schema()?, value).map_err(CoreError::from)
}

/// Match Java `HoodieMetadataPayload.createRecordIndexUpdate` fileId packing.
fn encode_file_id_for_record_index(file_id: &str) -> (i64, i64, i32, String, i32) {
    if let Some((uuid, file_index)) = parse_uuid_file_id(file_id) {
        let (high, low) = uuid.as_u64_pair();
        (high as i64, low as i64, file_index, String::new(), 0)
    } else {
        (-1, -1, -1, file_id.to_string(), 1)
    }
}

/// Parse `{uuid}` or `{uuid}-{fileIndex}` as used by Java FSUtils file ids.
fn parse_uuid_file_id(file_id: &str) -> Option<(uuid::Uuid, i32)> {
    if file_id.len() == 36 {
        return uuid::Uuid::parse_str(file_id).ok().map(|u| (u, -1));
    }
    let idx = file_id.rfind('-')?;
    let uuid = uuid::Uuid::parse_str(&file_id[..idx]).ok()?;
    let file_index: i32 = file_id[idx + 1..].parse().ok()?;
    Some((uuid, file_index))
}

/// Decode a record-index Avro payload. Returns `None` when the entry is a delete.
pub fn decode_record_index_entry(key: &str, bytes: &[u8]) -> Result<Option<RecordIndexEntry>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let value = apache_avro::from_avro_datum(metadata_schema()?, &mut &bytes[..], None)?;
    let Value::Record(fields) = value else {
        return Err(CoreError::MetadataTable(
            "record_index payload must be an Avro record".to_string(),
        ));
    };
    let mut partition_path = String::new();
    let mut file_id = String::new();
    let mut file_id_high = None;
    let mut file_id_low = None;
    let mut file_index = None;
    let mut file_id_encoding = 0_i32;
    let mut instant_time_millis = 0_i64;
    let mut has_info = false;
    for (name, field_value) in fields {
        if name != "recordIndexMetadata" {
            continue;
        }
        match field_value {
            Value::Union(0, _) | Value::Null => return Ok(None),
            Value::Union(1, inner) => {
                let Value::Record(info_fields) = *inner else {
                    return Err(CoreError::MetadataTable(
                        "recordIndexMetadata must be a record".to_string(),
                    ));
                };
                has_info = true;
                for (fname, fval) in info_fields {
                    match (fname.as_str(), fval) {
                        ("partitionName", Value::Union(1, v)) => {
                            if let Value::String(s) = *v {
                                partition_path = s;
                            }
                        }
                        ("fileId", Value::Union(1, v)) => {
                            if let Value::String(s) = *v {
                                file_id = s;
                            }
                        }
                        ("fileIdHighBits", Value::Union(1, v)) => {
                            if let Value::Long(v) = *v {
                                file_id_high = Some(v);
                            }
                        }
                        ("fileIdLowBits", Value::Union(1, v)) => {
                            if let Value::Long(v) = *v {
                                file_id_low = Some(v);
                            }
                        }
                        ("fileIndex", Value::Union(1, v)) => {
                            if let Value::Int(v) = *v {
                                file_index = Some(v);
                            }
                        }
                        ("fileIdEncoding", Value::Int(v)) => file_id_encoding = v,
                        ("instantTime", Value::Union(1, v)) => {
                            if let Value::Long(ts) = *v {
                                instant_time_millis = ts;
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    if !has_info {
        return Ok(None);
    }
    if file_id.is_empty() && file_id_encoding == 0 {
        if let (Some(high), Some(low), Some(idx)) = (file_id_high, file_id_low, file_index) {
            let uuid = uuid_from_u64_pair(high as u64, low as u64);
            file_id = if idx >= 0 {
                format!("{uuid}-{idx}")
            } else {
                uuid
            };
        }
    }
    Ok(Some(RecordIndexEntry {
        record_key: key.to_string(),
        partition_path,
        file_id,
        instant_time_millis,
        is_deleted: false,
    }))
}

fn uuid_from_u64_pair(high: u64, low: u64) -> String {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&high.to_be_bytes());
    bytes[8..].copy_from_slice(&low.to_be_bytes());
    let (a, b, c, d, e) = (
        u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
        u16::from_be_bytes(bytes[4..6].try_into().unwrap()),
        u16::from_be_bytes(bytes[6..8].try_into().unwrap()),
        u16::from_be_bytes(bytes[8..10].try_into().unwrap()),
        &bytes[10..16],
    );
    format!(
        "{a:08x}-{b:04x}-{c:04x}-{d:04x}-{}",
        e.iter().map(|x| format!("{x:02x}")).collect::<String>()
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    fn avro_key_field(bytes: &[u8]) -> String {
        let value = apache_avro::from_avro_datum(metadata_schema().unwrap(), &mut &bytes[..], None)
            .unwrap();
        let Value::Record(fields) = value else {
            panic!("expected record");
        };
        for (name, field) in fields {
            if name == "key" {
                return match field {
                    Value::String(s) => s,
                    other => panic!("unexpected key value: {other:?}"),
                };
            }
        }
        panic!("missing key field");
    }

    #[test]
    fn embedded_metadata_schema_has_java_string_props() {
        let json = hoodie_metadata_schema_json();
        assert!(
            json.contains("avro.java.string"),
            "HFile-embedded schema must request Java String decoding"
        );
        assert!(
            json.contains("\"type\":\"map\"") && json.contains("avro.java.string"),
            "map types need java.string for map-key typing: {json}"
        );
    }

    #[test]
    fn encode_files_record_clears_avro_key_for_hfile_interop() {
        let bytes = encode_files_record(
            "city=sf",
            MetadataRecordType::Files,
            [FilesMetadataEntry {
                name: "f.parquet".to_string(),
                size: 10,
                is_deleted: false,
            }],
        )
        .unwrap();
        assert_eq!(avro_key_field(&bytes), "");
    }

    #[test]
    fn encode_record_index_clears_avro_key_for_hfile_interop() {
        let bytes = encode_record_index_entry(&RecordIndexEntry {
            record_key: "id-1".to_string(),
            partition_path: String::new(),
            file_id: "fg-0".to_string(),
            instant_time_millis: 1,
            is_deleted: false,
        })
        .unwrap();
        assert_eq!(avro_key_field(&bytes), "");
    }

    #[test]
    fn encode_record_index_uses_uuid_bits_when_file_id_is_uuid() {
        let file_id = "184e0720-0b37-4e8b-b23a-5646f6c2fe94-0";
        let bytes = encode_record_index_entry(&RecordIndexEntry {
            record_key: "id-1".to_string(),
            partition_path: "city=sf".to_string(),
            file_id: file_id.to_string(),
            instant_time_millis: 1,
            is_deleted: false,
        })
        .unwrap();
        let decoded = decode_record_index_entry("id-1", &bytes).unwrap().unwrap();
        assert_eq!(decoded.file_id, file_id);
    }
}
