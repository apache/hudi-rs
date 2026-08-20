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
pub fn hoodie_metadata_schema_json() -> Result<&'static str> {
    static JSON: OnceLock<std::result::Result<String, String>> = OnceLock::new();
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
    .as_deref()
    .map_err(|e| {
        crate::error::CoreError::MetadataTable(format!(
            "vendored HoodieMetadata.avsc failed to annotate: {e}"
        ))
    })
}

/// Annotate Avro JSON so Java readers decode strings as `java.lang.String`.
fn inject_avro_java_string_props(schema_json: &str) -> std::result::Result<String, String> {
    let mut value: serde_json::Value =
        serde_json::from_str(schema_json).map_err(|e| e.to_string())?;
    inject_avro_java_string_props_value(&mut value);
    serde_json::to_string(&value).map_err(|e| e.to_string())
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
                    map.entry("avro.java.string".to_string())
                        .or_insert_with(|| serde_json::Value::String("String".to_string()));
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
pub fn files_metadata_avro_schema_json() -> Result<&'static str> {
    hoodie_metadata_schema_json()
}

/// Alias kept for call sites that embed the record_index schema into HFiles.
pub fn record_index_metadata_avro_schema_json() -> Result<&'static str> {
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

/// A comparable column statistic value wrapped for MDT Avro payloads.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum ColumnStatValue {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    String(String),
    Date(i32),
    TimeMicros(i64),
    TimestampMicros(i64),
    /// Timezone-less timestamp micros (Java `LOCAL_TIMESTAMP_MICROS`).
    LocalTimestampMicros(i64),
}

/// MDT stats index version: V1 (tv8, logical wrappers, no `valueType`) vs V2
/// (tv9+, primitive wrappers + `HoodieValueTypeInfo`). Java
/// `HoodieIndexVersion.getCurrentVersion`: COLUMN_STATS/PARTITION_STATS are V2
/// from table version 9.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsIndexVersion {
    V1,
    V2,
}

impl ColumnStatValue {
    /// Java `ValueType` enum ordinal (stats/ValueType.java declaration order):
    /// V1=0 NULL=1 BOOLEAN=2 INT=3 LONG=4 FLOAT=5 DOUBLE=6 STRING=7 BYTES=8
    /// FIXED=9 DECIMAL=10 UUID=11 DATE=12 TIME_MILLIS=13 TIME_MICROS=14
    /// TIMESTAMP_MILLIS=15 TIMESTAMP_MICROS=16 TIMESTAMP_NANOS=17
    /// LOCAL_TIMESTAMP_{MILLIS,MICROS,NANOS}=18/19/20.
    fn value_type_ordinal(&self) -> i32 {
        match self {
            ColumnStatValue::Boolean(_) => 2,
            ColumnStatValue::Int(_) => 3,
            ColumnStatValue::Long(_) => 4,
            ColumnStatValue::Float(_) => 5,
            ColumnStatValue::Double(_) => 6,
            ColumnStatValue::String(_) => 7,
            ColumnStatValue::Bytes(_) => 8,
            ColumnStatValue::Date(_) => 12,
            ColumnStatValue::TimeMicros(_) => 14,
            ColumnStatValue::TimestampMicros(_) => 16,
            ColumnStatValue::LocalTimestampMicros(_) => 19,
        }
    }
}

/// Column / partition statistics payload (Avro `HoodieMetadataColumnStats`).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStatsMetadata {
    pub file_name: String,
    pub column_name: String,
    pub min_value: Option<ColumnStatValue>,
    pub max_value: Option<ColumnStatValue>,
    pub value_count: i64,
    pub null_count: i64,
    pub total_size: i64,
    pub total_uncompressed_size: i64,
    pub is_deleted: bool,
    pub is_tight_bound: bool,
    /// Java `ValueType` ordinal decoded from a V2 record's `valueType`;
    /// `None` for V1 records. Ignored on encode (derived from the values).
    pub decoded_value_type_ordinal: Option<i32>,
}

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

/// Encode a record-index metadata record (type = 5).
///
/// Deletes must not use this path — emit a delete log block instead. Null
/// `recordIndexMetadata` NPEs Java's RLI reader.
pub fn encode_record_index_entry(entry: &RecordIndexEntry) -> Result<Vec<u8>> {
    if entry.is_deleted {
        return Err(CoreError::MetadataTable(
            "RLI deletes must be written as delete log blocks, not null payloads".to_string(),
        ));
    }
    let (high, low, file_index, file_id_str, encoding) =
        encode_file_id_for_record_index(&entry.file_id);
    let rli_payload = Value::Union(
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
    );
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
///
/// `writer_schema` is the schema of the container the bytes were read from
/// (mixed-writer tables have differing HoodieMetadataRecord schemas per
/// container); falls back to our vendored schema when unknown.
pub fn decode_record_index_entry(
    key: &str,
    bytes: &[u8],
    writer_schema: Option<&Schema>,
) -> Result<Option<RecordIndexEntry>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let schema = match writer_schema {
        Some(s) => s,
        None => metadata_schema()?,
    };
    let value = apache_avro::from_avro_datum(schema, &mut &bytes[..], None)?;
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
    if file_id.is_empty()
        && file_id_encoding == 0
        && let (Some(high), Some(low), Some(idx)) = (file_id_high, file_id_low, file_index)
    {
        let uuid = uuid_from_u64_pair(high as u64, low as u64);
        file_id = if idx >= 0 {
            format!("{uuid}-{idx}")
        } else {
            uuid
        };
    }
    Ok(Some(RecordIndexEntry {
        record_key: key.to_string(),
        partition_path,
        file_id,
        instant_time_millis,
        is_deleted: false,
    }))
}

/// Decode a column_stats / partition_stats Avro payload.
///
/// Returns `None` when the record carries no `ColumnStatsMetadata` (e.g. a
/// files or record_index record read from a mixed scan).
pub fn decode_column_stats_entry(
    bytes: &[u8],
    writer_schema: Option<&Schema>,
) -> Result<Option<ColumnStatsMetadata>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let schema = match writer_schema {
        Some(s) => s,
        None => metadata_schema()?,
    };
    let value = apache_avro::from_avro_datum(schema, &mut &bytes[..], None)?;
    let Value::Record(fields) = value else {
        return Err(CoreError::MetadataTable(
            "column_stats payload must be an Avro record".to_string(),
        ));
    };
    for (name, field_value) in fields {
        if name != "ColumnStatsMetadata" {
            continue;
        }
        match field_value {
            Value::Union(0, _) | Value::Null => return Ok(None),
            Value::Union(1, inner) => {
                let Value::Record(stat_fields) = *inner else {
                    return Err(CoreError::MetadataTable(
                        "ColumnStatsMetadata must be a record".to_string(),
                    ));
                };
                let mut out = ColumnStatsMetadata {
                    file_name: String::new(),
                    column_name: String::new(),
                    min_value: None,
                    max_value: None,
                    value_count: 0,
                    null_count: 0,
                    total_size: 0,
                    total_uncompressed_size: 0,
                    is_deleted: false,
                    is_tight_bound: false,
                    decoded_value_type_ordinal: None,
                };
                let mut value_type_ordinal: Option<i32> = None;
                for (fname, fval) in stat_fields {
                    match (fname.as_str(), fval) {
                        ("fileName", Value::Union(1, v)) => {
                            if let Value::String(s) = *v {
                                out.file_name = s;
                            }
                        }
                        ("valueType", Value::Union(1, v)) => {
                            if let Value::Record(info) = *v {
                                for (iname, ival) in info {
                                    if iname == "typeOrdinal"
                                        && let Value::Int(ordinal) = ival
                                    {
                                        value_type_ordinal = Some(ordinal);
                                    }
                                }
                            }
                        }
                        ("columnName", Value::Union(1, v)) => {
                            if let Value::String(s) = *v {
                                out.column_name = s;
                            }
                        }
                        ("minValue", v) => out.min_value = unwrap_stat_value(v),
                        ("maxValue", v) => out.max_value = unwrap_stat_value(v),
                        ("valueCount", Value::Union(1, v)) => {
                            if let Value::Long(n) = *v {
                                out.value_count = n;
                            }
                        }
                        ("nullCount", Value::Union(1, v)) => {
                            if let Value::Long(n) = *v {
                                out.null_count = n;
                            }
                        }
                        ("totalSize", Value::Union(1, v)) => {
                            if let Value::Long(n) = *v {
                                out.total_size = n;
                            }
                        }
                        ("totalUncompressedSize", Value::Union(1, v)) => {
                            if let Value::Long(n) = *v {
                                out.total_uncompressed_size = n;
                            }
                        }
                        ("isDeleted", Value::Boolean(b)) => out.is_deleted = b,
                        ("isTightBound", Value::Boolean(b)) => out.is_tight_bound = b,
                        _ => {}
                    }
                }
                // V2 records carry logical types in valueType over primitive
                // wrappers — lift them back to typed stat values.
                if let Some(ordinal) = value_type_ordinal {
                    out.min_value = out.min_value.map(|v| lift_stat_value(v, ordinal));
                    out.max_value = out.max_value.map(|v| lift_stat_value(v, ordinal));
                    out.decoded_value_type_ordinal = Some(ordinal);
                }
                return Ok(Some(out));
            }
            _ => {}
        }
    }
    Ok(None)
}

/// Lift a V2 primitive-wrapped value to its logical type per the Java
/// `ValueType` ordinal carried in `valueType`.
fn lift_stat_value(value: ColumnStatValue, ordinal: i32) -> ColumnStatValue {
    match (value, ordinal) {
        (ColumnStatValue::Int(v), 12) => ColumnStatValue::Date(v),
        (ColumnStatValue::Long(v), 14) => ColumnStatValue::TimeMicros(v),
        (ColumnStatValue::Long(v), 16) => ColumnStatValue::TimestampMicros(v),
        (ColumnStatValue::Long(v), 19) => ColumnStatValue::LocalTimestampMicros(v),
        (value, _) => value,
    }
}

/// Inverse of [`wrap_stat_value`]: union branch → typed stat value.
fn unwrap_stat_value(value: Value) -> Option<ColumnStatValue> {
    let Value::Union(branch, inner) = value else {
        return None;
    };
    let Value::Record(fields) = *inner else {
        return None;
    };
    let (_, inner_value) = fields.into_iter().find(|(name, _)| name == "value")?;
    match (branch, inner_value) {
        (1, Value::Boolean(v)) => Some(ColumnStatValue::Boolean(v)),
        (2, Value::Int(v)) => Some(ColumnStatValue::Int(v)),
        (3, Value::Long(v)) => Some(ColumnStatValue::Long(v)),
        (4, Value::Float(v)) => Some(ColumnStatValue::Float(v)),
        (5, Value::Double(v)) => Some(ColumnStatValue::Double(v)),
        (6, Value::Bytes(v)) => Some(ColumnStatValue::Bytes(v)),
        (7, Value::String(v)) => Some(ColumnStatValue::String(v)),
        // DateWrapper and LocalDateWrapper both carry epoch days.
        (8 | 12, Value::Int(v) | Value::Date(v)) => Some(ColumnStatValue::Date(v)),
        (10, Value::Long(v) | Value::TimeMicros(v)) => Some(ColumnStatValue::TimeMicros(v)),
        (11, Value::Long(v) | Value::TimestampMicros(v)) => {
            Some(ColumnStatValue::TimestampMicros(v))
        }
        _ => None,
    }
}

fn uuid_from_u64_pair(high: u64, low: u64) -> String {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&high.to_be_bytes());
    bytes[8..].copy_from_slice(&low.to_be_bytes());
    let (a, b, c, d, e) = (
        u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        u16::from_be_bytes([bytes[4], bytes[5]]),
        u16::from_be_bytes([bytes[6], bytes[7]]),
        u16::from_be_bytes([bytes[8], bytes[9]]),
        &bytes[10..16],
    );
    format!(
        "{a:08x}-{b:04x}-{c:04x}-{d:04x}-{}",
        e.iter().map(|x| format!("{x:02x}")).collect::<String>()
    )
}

/// Encode a column_stats metadata record (type = 3).
pub fn encode_column_stats(
    metadata: &ColumnStatsMetadata,
    version: StatsIndexVersion,
) -> Result<Vec<u8>> {
    encode_column_stats_record(metadata, MetadataRecordType::ColumnStats, version)
}

/// Encode a partition_stats metadata record (type = 6).
///
/// Uses the same `ColumnStatsMetadata` Avro shape as column_stats.
pub fn encode_partition_stats(
    metadata: &ColumnStatsMetadata,
    version: StatsIndexVersion,
) -> Result<Vec<u8>> {
    encode_column_stats_record(metadata, MetadataRecordType::PartitionStats, version)
}

fn encode_column_stats_record(
    metadata: &ColumnStatsMetadata,
    record_type: MetadataRecordType,
    version: StatsIndexVersion,
) -> Result<Vec<u8>> {
    let payload = Value::Union(
        1,
        Box::new(Value::Record(vec![
            (
                "fileName".to_string(),
                Value::Union(1, Box::new(Value::String(metadata.file_name.clone()))),
            ),
            (
                "columnName".to_string(),
                Value::Union(1, Box::new(Value::String(metadata.column_name.clone()))),
            ),
            (
                "minValue".to_string(),
                wrap_stat_value(metadata.min_value.as_ref(), version),
            ),
            (
                "maxValue".to_string(),
                wrap_stat_value(metadata.max_value.as_ref(), version),
            ),
            (
                "valueCount".to_string(),
                Value::Union(1, Box::new(Value::Long(metadata.value_count))),
            ),
            (
                "nullCount".to_string(),
                Value::Union(1, Box::new(Value::Long(metadata.null_count))),
            ),
            (
                "totalSize".to_string(),
                Value::Union(1, Box::new(Value::Long(metadata.total_size))),
            ),
            (
                "totalUncompressedSize".to_string(),
                Value::Union(1, Box::new(Value::Long(metadata.total_uncompressed_size))),
            ),
            ("isDeleted".to_string(), Value::Boolean(metadata.is_deleted)),
            (
                "isTightBound".to_string(),
                Value::Boolean(metadata.is_tight_bound),
            ),
            // V1 leaves valueType null (Java ValueMetadata.V1EmptyMetadata);
            // V2 (tv9+) carries HoodieValueTypeInfo. Ordinal derives from the
            // stat values; all-null columns fall back to NULL (ordinal 1).
            (
                "valueType".to_string(),
                match version {
                    StatsIndexVersion::V1 => null_union(),
                    StatsIndexVersion::V2 => {
                        let ordinal = metadata
                            .min_value
                            .as_ref()
                            .or(metadata.max_value.as_ref())
                            .map(ColumnStatValue::value_type_ordinal)
                            .unwrap_or(1);
                        Value::Union(
                            1,
                            Box::new(Value::Record(vec![
                                ("typeOrdinal".to_string(), Value::Int(ordinal)),
                                ("additionalInfo".to_string(), null_union()),
                            ])),
                        )
                    }
                },
            ),
        ])),
    );
    let value = Value::Record(vec![
        ("key".to_string(), Value::String(String::new())),
        ("type".to_string(), Value::Int(record_type as i32)),
        ("filesystemMetadata".to_string(), null_union()),
        ("BloomFilterMetadata".to_string(), null_union()),
        ("ColumnStatsMetadata".to_string(), payload),
        ("recordIndexMetadata".to_string(), null_union()),
        ("SecondaryIndexMetadata".to_string(), null_union()),
    ]);
    apache_avro::to_avro_datum(metadata_schema()?, value).map_err(CoreError::from)
}

/// Avro union branch indices for minValue/maxValue wrappers (null = 0).
///
/// V2 (tv9+) writes logical types via PRIMITIVE wrappers — the logical type
/// lives in `valueType` instead (Java `ValueType.primitiveWrapperType`):
/// Date → IntWrapper; TimeMicros / TimestampMicros / LocalTimestampMicros →
/// LongWrapper.
fn wrap_stat_value(value: Option<&ColumnStatValue>, version: StatsIndexVersion) -> Value {
    let Some(value) = value else {
        return null_union();
    };
    if version == StatsIndexVersion::V2 {
        let (branch, inner) = match value {
            ColumnStatValue::Boolean(v) => (1, Value::Boolean(*v)),
            ColumnStatValue::Int(v) | ColumnStatValue::Date(v) => (2, Value::Int(*v)),
            ColumnStatValue::Long(v)
            | ColumnStatValue::TimeMicros(v)
            | ColumnStatValue::TimestampMicros(v)
            | ColumnStatValue::LocalTimestampMicros(v) => (3, Value::Long(*v)),
            ColumnStatValue::Float(v) => (4, Value::Float(*v)),
            ColumnStatValue::Double(v) => (5, Value::Double(*v)),
            ColumnStatValue::Bytes(v) => (6, Value::Bytes(v.clone())),
            ColumnStatValue::String(v) => (7, Value::String(v.clone())),
        };
        return Value::Union(
            branch,
            Box::new(Value::Record(vec![("value".to_string(), inner)])),
        );
    }
    let (branch, inner) = match value {
        ColumnStatValue::Boolean(v) => (
            1,
            Value::Record(vec![("value".to_string(), Value::Boolean(*v))]),
        ),
        ColumnStatValue::Int(v) => (
            2,
            Value::Record(vec![("value".to_string(), Value::Int(*v))]),
        ),
        ColumnStatValue::Long(v) => (
            3,
            Value::Record(vec![("value".to_string(), Value::Long(*v))]),
        ),
        ColumnStatValue::Float(v) => (
            4,
            Value::Record(vec![("value".to_string(), Value::Float(*v))]),
        ),
        ColumnStatValue::Double(v) => (
            5,
            Value::Record(vec![("value".to_string(), Value::Double(*v))]),
        ),
        ColumnStatValue::Bytes(v) => (
            6,
            Value::Record(vec![("value".to_string(), Value::Bytes(v.clone()))]),
        ),
        ColumnStatValue::String(v) => (
            7,
            Value::Record(vec![("value".to_string(), Value::String(v.clone()))]),
        ),
        ColumnStatValue::Date(v) => (
            8,
            Value::Record(vec![("value".to_string(), Value::Int(*v))]),
        ),
        // DecimalWrapper = 9: decimals are not indexed (Spark record type
        // excludes bytes/fixed columns entirely).
        ColumnStatValue::TimeMicros(v) => (
            10,
            Value::Record(vec![("value".to_string(), Value::Long(*v))]),
        ),
        // V1 has no local-timestamp wrapper; both map to TimestampMicros.
        ColumnStatValue::TimestampMicros(v) | ColumnStatValue::LocalTimestampMicros(v) => (
            11,
            Value::Record(vec![("value".to_string(), Value::Long(*v))]),
        ),
    };
    Value::Union(branch, Box::new(inner))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_column_stats_v1_test(m: &ColumnStatsMetadata) -> Result<Vec<u8>> {
        encode_column_stats(m, StatsIndexVersion::V1)
    }

    fn encode_partition_stats_v1_test(m: &ColumnStatsMetadata) -> Result<Vec<u8>> {
        encode_partition_stats(m, StatsIndexVersion::V1)
    }

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
        let json = hoodie_metadata_schema_json().unwrap();
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
    fn encode_column_stats_clears_avro_key_and_sets_type() {
        let bytes = encode_column_stats_v1_test(&ColumnStatsMetadata {
            file_name: "f.parquet".to_string(),
            column_name: "id".to_string(),
            min_value: Some(ColumnStatValue::Long(1)),
            max_value: Some(ColumnStatValue::Long(10)),
            value_count: 10,
            null_count: 0,
            total_size: 100,
            total_uncompressed_size: 200,
            is_deleted: false,
            is_tight_bound: false,
            decoded_value_type_ordinal: None,
        })
        .unwrap();
        assert_eq!(avro_key_field(&bytes), "");
        let value = apache_avro::from_avro_datum(metadata_schema().unwrap(), &mut &bytes[..], None)
            .unwrap();
        let Value::Record(fields) = value else {
            panic!("expected record");
        };
        let mut saw_type = false;
        let mut saw_payload = false;
        for (name, field) in fields {
            match (name.as_str(), field) {
                ("type", Value::Int(3)) => saw_type = true,
                ("ColumnStatsMetadata", Value::Union(1, _)) => saw_payload = true,
                _ => {}
            }
        }
        assert!(saw_type && saw_payload);
    }

    #[test]
    fn encode_partition_stats_uses_type_six() {
        let bytes = encode_partition_stats_v1_test(&ColumnStatsMetadata {
            file_name: ".".to_string(),
            column_name: "id".to_string(),
            min_value: Some(ColumnStatValue::Int(1)),
            max_value: Some(ColumnStatValue::Int(2)),
            value_count: 2,
            null_count: 0,
            total_size: 10,
            total_uncompressed_size: 20,
            is_deleted: false,
            is_tight_bound: true,
            decoded_value_type_ordinal: None,
        })
        .unwrap();
        let value = apache_avro::from_avro_datum(metadata_schema().unwrap(), &mut &bytes[..], None)
            .unwrap();
        let Value::Record(fields) = value else {
            panic!("expected record");
        };
        assert!(
            fields
                .iter()
                .any(|(n, v)| n == "type" && matches!(v, Value::Int(6)))
        );
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
        let decoded = decode_record_index_entry("id-1", &bytes, None)
            .unwrap()
            .unwrap();
        assert_eq!(decoded.file_id, file_id);
    }

    /// V1 (tv8) and V2 (tv9) encodings must both decode back to the same
    /// logical stat values for every wrapper variant.
    #[test]
    fn test_column_stats_encode_decode_roundtrip_all_variants() {
        let variants = vec![
            ColumnStatValue::Boolean(true),
            ColumnStatValue::Int(7),
            ColumnStatValue::Long(9),
            ColumnStatValue::Float(1.5),
            ColumnStatValue::Double(2.5),
            ColumnStatValue::Bytes(vec![1, 2, 3]),
            ColumnStatValue::String("s".to_string()),
            ColumnStatValue::Date(19000),
            ColumnStatValue::TimeMicros(11),
            ColumnStatValue::TimestampMicros(22),
            ColumnStatValue::LocalTimestampMicros(33),
        ];
        for version in [StatsIndexVersion::V1, StatsIndexVersion::V2] {
            for value in &variants {
                let meta = ColumnStatsMetadata {
                    file_name: "f.parquet".to_string(),
                    column_name: "c".to_string(),
                    min_value: Some(value.clone()),
                    max_value: Some(value.clone()),
                    value_count: 3,
                    null_count: 1,
                    total_size: 100,
                    total_uncompressed_size: 200,
                    is_deleted: false,
                    is_tight_bound: true,
                    decoded_value_type_ordinal: None,
                };
                let bytes = encode_column_stats(&meta, version)
                    .unwrap_or_else(|e| panic!("{version:?} encode {value:?}: {e}"));
                let schema = metadata_schema().unwrap();
                let decoded = decode_column_stats_entry(&bytes, Some(schema))
                    .unwrap_or_else(|e| panic!("{version:?} decode {value:?}: {e}"))
                    .unwrap_or_else(|| panic!("{version:?} decode {value:?}: tombstone"));
                // V1 has no `valueType`, so a tz-less timestamp is
                // indistinguishable from a UTC one after decode — the reason
                // the V2 index exists.
                let expected = match (version, value) {
                    (StatsIndexVersion::V1, ColumnStatValue::LocalTimestampMicros(v)) => {
                        ColumnStatValue::TimestampMicros(*v)
                    }
                    _ => value.clone(),
                };
                assert_eq!(decoded.min_value.as_ref(), Some(&expected), "{version:?}");
                assert_eq!(decoded.max_value.as_ref(), Some(&expected), "{version:?}");
                assert_eq!(decoded.value_count, 3);
                assert_eq!(decoded.null_count, 1);
            }
        }
    }

    /// Tombstones round-trip as deletes for both index versions.
    #[test]
    fn test_column_stats_tombstone_roundtrip() {
        for version in [StatsIndexVersion::V1, StatsIndexVersion::V2] {
            let meta = ColumnStatsMetadata {
                file_name: "f.parquet".to_string(),
                column_name: "c".to_string(),
                min_value: None,
                max_value: None,
                value_count: 0,
                null_count: 0,
                total_size: 0,
                total_uncompressed_size: 0,
                is_deleted: true,
                is_tight_bound: false,
                decoded_value_type_ordinal: None,
            };
            let bytes = encode_column_stats(&meta, version).unwrap();
            let schema = metadata_schema().unwrap();
            let decoded = decode_column_stats_entry(&bytes, Some(schema))
                .unwrap()
                .expect("tombstone still decodes to a record");
            assert!(decoded.is_deleted, "{version:?}");
        }
    }
}
