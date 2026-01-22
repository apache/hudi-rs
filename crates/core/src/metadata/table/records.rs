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
//! Metadata table record types for decoding Avro-serialized values.
//!
//! The Hudi metadata table stores records as Avro-serialized `HoodieMetadataRecord`.
//! This module provides types and functions to decode these records, particularly
//! for the "files" partition which contains file listings for each partition.
//!
//! # Files Partition Structure
//!
//! In the files partition:
//! - Key: partition path (e.g., "city=chennai") or "__all_partitions__"
//! - Value: Avro-serialized `HoodieMetadataRecord` with `filesystemMetadata` field
//!
//! The `filesystemMetadata` field is a map where:
//! - Map keys are file names (e.g., "abc.parquet")
//! - Map values are `HoodieMetadataFileInfo` with size and deletion status

use crate::Result;
use crate::error::CoreError;
use crate::hfile::{HFileReader, HFileRecord};
use crate::metadata::NON_PARTITIONED_NAME;
use apache_avro::Schema as AvroSchema;
use apache_avro::types::Value as AvroValue;
use std::collections::HashMap;

/// Metadata table partition types.
///
/// These represent the different partitions (directories) within the metadata table,
/// each storing a different type of index data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetadataPartitionType {
    /// The "files" partition containing file listings per data table partition.
    Files,
    /// The "column_stats" partition containing column statistics.
    ColumnStats,
    /// The "partition_stats" partition containing partition-level statistics.
    PartitionStats,
    /// The "record_index" partition containing record-level index entries.
    RecordIndex,
}

impl MetadataPartitionType {
    /// Get the partition directory name as used in the metadata table.
    pub fn partition_name(&self) -> &'static str {
        match self {
            Self::Files => "files",
            Self::ColumnStats => "column_stats",
            Self::PartitionStats => "partition_stats",
            Self::RecordIndex => "record_index",
        }
    }
}

impl std::fmt::Display for MetadataPartitionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.partition_name())
    }
}

/// File information from the metadata table.
#[derive(Debug, Clone, PartialEq)]
pub struct HoodieMetadataFileInfo {
    /// File name (e.g., "abc-0_0-123-456_20231214.parquet")
    pub name: String,
    /// File size in bytes
    pub size: i64,
    /// Whether the file has been deleted
    pub is_deleted: bool,
}

impl HoodieMetadataFileInfo {
    /// Create a new file info.
    pub fn new(name: String, size: i64, is_deleted: bool) -> Self {
        Self {
            name,
            size,
            is_deleted,
        }
    }
}

/// Metadata record type.
///
/// The type field in `HoodieMetadataRecord` indicates which metadata partition
/// the record belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum MetadataRecordType {
    /// All partitions record (type = 1) - Lists all partitions in the table.
    /// Key is `__all_partitions__`, value contains partition names in filesystemMetadata map.
    AllPartitions = 1,
    /// Files partition record (type = 2) - Lists files in a specific partition.
    /// Key is the partition path, value contains file info in filesystemMetadata map.
    Files = 2,
    /// Column stats partition record (type = 3) - Contains column statistics for a file.
    /// Key is a composite key (partition+file+column), value contains ColumnStatsMetadata.
    ColumnStats = 3,
    /// Bloom filter partition record (type = 4) - Contains bloom filter for a file.
    /// Key is a composite key, value contains BloomFilterMetadata.
    BloomFilters = 4,
    /// Record index partition record (type = 5) - Contains record-level index info.
    RecordIndex = 5,
    /// Partition stats partition record (type = 6) - Contains partition-level column statistics.
    /// Uses the same ColumnStatsMetadata structure as COLUMN_STATS.
    PartitionStats = 6,
    /// Secondary index partition record (type = 7) - Contains secondary index info.
    SecondaryIndex = 7,
    /// Unknown type
    Unknown = -1,
}

impl From<i32> for MetadataRecordType {
    fn from(value: i32) -> Self {
        match value {
            1 => MetadataRecordType::AllPartitions,
            2 => MetadataRecordType::Files,
            3 => MetadataRecordType::ColumnStats,
            4 => MetadataRecordType::BloomFilters,
            5 => MetadataRecordType::RecordIndex,
            6 => MetadataRecordType::PartitionStats,
            7 => MetadataRecordType::SecondaryIndex,
            _ => MetadataRecordType::Unknown,
        }
    }
}

/// Decoded metadata record from the files partition.
#[derive(Debug, Clone)]
pub struct FilesPartitionRecord {
    /// Record key (partition path or "__all_partitions__")
    pub key: String,
    /// Record type (AllPartitions=1 or Files=2)
    pub record_type: MetadataRecordType,
    /// File information map: file name -> file info
    /// For AllPartitions: keys are partition names, values have size=0
    /// For Files: keys are file names, values have actual size
    pub files: HashMap<String, HoodieMetadataFileInfo>,
}

impl FilesPartitionRecord {
    /// The partition name in the metadata table that stores file listings.
    pub const PARTITION_NAME: &'static str = "files";

    /// The key for the record that contains all partition paths.
    pub const ALL_PARTITIONS_KEY: &'static str = "__all_partitions__";

    /// The key used in metadata table for non-partitioned tables.
    /// The metadata table stores "." for non-partitioned tables, which maps to "" externally.
    /// Uses the shared constant from `crate::metadata::NON_PARTITIONED_NAME`.
    pub const NON_PARTITIONED_NAME: &'static str = NON_PARTITIONED_NAME;

    /// Check if this is an ALL_PARTITIONS record.
    pub fn is_all_partitions(&self) -> bool {
        self.record_type == MetadataRecordType::AllPartitions
    }

    /// Get list of partition names (for ALL_PARTITIONS record).
    ///
    /// Returns the partition keys from the files map. When records are decoded using
    /// [`decode_files_partition_record_with_schema`], keys are normalized at decode time,
    /// so non-partitioned tables will have "" (empty string) as the partition key.
    ///
    /// The returned list is sorted in ascending order for stable, deterministic output.
    pub fn partition_names(&self) -> Vec<&str> {
        if self.is_all_partitions() {
            let mut names: Vec<&str> = self.files.keys().map(|s| s.as_str()).collect();
            names.sort();
            names
        } else {
            vec![]
        }
    }

    /// Get list of active (non-deleted) file names.
    pub fn active_file_names(&self) -> Vec<&str> {
        self.files
            .values()
            .filter(|f| !f.is_deleted)
            .map(|f| f.name.as_str())
            .collect()
    }

    /// Get list of all file names (including deleted).
    pub fn all_file_names(&self) -> Vec<&str> {
        self.files.keys().map(|s| s.as_str()).collect()
    }

    /// Check if a file exists and is not deleted.
    pub fn has_active_file(&self, name: &str) -> bool {
        self.files.get(name).map(|f| !f.is_deleted).unwrap_or(false)
    }

    /// Get total size of active files in bytes.
    pub fn total_size(&self) -> i64 {
        self.files
            .values()
            .filter(|info| !info.is_deleted)
            .map(|info| info.size)
            .sum()
    }
}

// ============================================================================
// Column Stats Types
// ============================================================================

/// Wrapper for values in the column stats Avro union type.
///
/// The `minValue` and `maxValue` fields in `HoodieMetadataColumnStats` are union types
/// that can contain various primitive and logical types. This enum represents all
/// possible variants for runtime handling.
///
/// Maps to the wrapper types in HoodieMetadata.avsc:
/// - BooleanWrapper, IntWrapper, LongWrapper, FloatWrapper, DoubleWrapper
/// - BytesWrapper, StringWrapper, DateWrapper, DecimalWrapper
/// - TimeMicrosWrapper, TimestampMicrosWrapper, LocalDateWrapper, ArrayWrapper
#[derive(Debug, Clone, PartialEq)]
pub enum WrappedStatValue {
    /// Null value (no statistics available)
    Null,
    /// Boolean value (from BooleanWrapper)
    Boolean(bool),
    /// 32-bit integer (from IntWrapper)
    Int(i32),
    /// 64-bit integer (from LongWrapper)
    Long(i64),
    /// 32-bit float (from FloatWrapper)
    Float(f32),
    /// 64-bit double (from DoubleWrapper)
    Double(f64),
    /// UTF-8 string (from StringWrapper)
    String(String),
    /// Raw bytes (from BytesWrapper)
    Bytes(Vec<u8>),
    /// Date as days since Unix epoch (from DateWrapper)
    Date(i32),
    /// Decimal with precision and scale (from DecimalWrapper)
    /// Note: Schema defines precision=30, scale=15 but actual values may vary
    Decimal {
        value: Vec<u8>,
        precision: u8,
        scale: i8,
    },
    /// Time in microseconds since midnight (from TimeMicrosWrapper)
    TimeMicros(i64),
    /// Timestamp in microseconds since Unix epoch (from TimestampMicrosWrapper)
    TimestampMicros(i64),
    /// Local date as days since Unix epoch (from LocalDateWrapper)
    /// Note: Semantically same as Date but from a different wrapper type
    LocalDate(i32),
    /// Array of wrapped values (from ArrayWrapper)
    /// Used for array column statistics
    Array(Vec<WrappedStatValue>),
}

/// Value type information from HoodieMetadataColumnStats.
///
/// Contains type metadata that helps with proper interpretation of min/max values.
#[derive(Debug, Clone, PartialEq)]
pub struct ValueTypeInfo {
    /// Enum ordinal representing the value type
    pub type_ordinal: i32,
    /// Optional additional information about the value type (e.g., timezone for timestamps)
    pub additional_info: Option<String>,
}

/// Column statistics record from the metadata table.
///
/// Represents a single column's statistics for a specific file.
/// Maps to `HoodieMetadataColumnStats` in HoodieMetadata.avsc.
///
/// The key format in the column_stats partition is:
/// `COLUMN_HASH + PARTITION_HASH + FILE_HASH` (see `util::hash` module)
#[derive(Debug, Clone)]
pub struct ColumnStatsRecord {
    /// File name this stats entry belongs to
    pub file_name: String,
    /// Column name
    pub column_name: String,
    /// Minimum value (wrapped in union type)
    pub min_value: Option<WrappedStatValue>,
    /// Maximum value (wrapped in union type)
    pub max_value: Option<WrappedStatValue>,
    /// Number of values (non-null count + null count)
    pub value_count: i64,
    /// Number of null values
    pub null_count: i64,
    /// Total size in bytes
    pub total_size: i64,
    /// Total uncompressed size in bytes
    pub total_uncompressed_size: i64,
    /// Whether this record marks a deletion
    pub is_deleted: bool,
    /// Whether the bounds are tight (exact) vs loose (may not be precise)
    pub is_tight_bound: bool,
    /// Value type information for proper interpretation of min/max values
    pub value_type: Option<ValueTypeInfo>,
}

impl ColumnStatsRecord {
    /// The partition name in the metadata table for column stats.
    pub const PARTITION_NAME: &'static str = "column_stats";
}

/// Partition-level statistics record from the metadata table.
///
/// Represents aggregated column statistics for a specific partition.
/// Uses the same structure as `ColumnStatsRecord` but at partition granularity.
///
/// The key format in the partition_stats partition is:
/// `COLUMN_HASH + PARTITION_HASH` (see `util::hash` module)
#[derive(Debug, Clone)]
pub struct PartitionStatsRecord {
    /// Partition path (e.g., "city=chennai" or "" for non-partitioned)
    pub partition_path: String,
    /// Column name
    pub column_name: String,
    /// Minimum value across all files in the partition
    pub min_value: Option<WrappedStatValue>,
    /// Maximum value across all files in the partition
    pub max_value: Option<WrappedStatValue>,
    /// Total value count across all files
    pub value_count: i64,
    /// Total null count across all files
    pub null_count: i64,
    /// Total size across all files
    pub total_size: i64,
    /// Whether this record marks a deletion
    pub is_deleted: bool,
    /// Whether the bounds are tight
    pub is_tight_bound: bool,
}

impl PartitionStatsRecord {
    /// The partition name in the metadata table for partition stats.
    pub const PARTITION_NAME: &'static str = "partition_stats";
}

// ============================================================================
// Files Partition Decoding
// ============================================================================

/// Decode an HFile record value from the files partition using Avro.
///
/// # Arguments
/// * `reader` - The HFile reader (provides the Avro schema)
/// * `record` - The HFile record containing the Avro-serialized value
///
/// # Returns
/// A `FilesPartitionRecord` with the decoded file information including record type.
///
/// # Note
/// This function fetches the Avro schema from the HFile's file info and uses it
/// to properly decode the filesystemMetadata field which contains file information.
pub fn decode_files_partition_record(
    reader: &HFileReader,
    record: &HFileRecord,
) -> Result<FilesPartitionRecord> {
    // Get schema from HFile reader
    let schema = reader
        .get_avro_schema()
        .map_err(|e| CoreError::MetadataTable(format!("Failed to get schema: {e}")))?
        .ok_or_else(|| CoreError::MetadataTable("No Avro schema in HFile".to_string()))?;

    decode_files_partition_record_with_schema(record, schema)
}

/// Decode an HFile record value from the files partition using a provided Avro schema.
///
/// This is useful when you have the schema separately (e.g., from a different HFile
/// or cached) and want to decode multiple records without repeated schema lookups.
///
/// # Arguments
/// * `record` - The HFile record containing the Avro-serialized value
/// * `schema` - The Avro schema for HoodieMetadataRecord
///
/// # Returns
/// A `FilesPartitionRecord` with the decoded file information including record type.
pub fn decode_files_partition_record_with_schema(
    record: &HFileRecord,
    schema: &AvroSchema,
) -> Result<FilesPartitionRecord> {
    let raw_key = record
        .key_as_str()
        .ok_or_else(|| CoreError::MetadataTable("Invalid UTF-8 key".to_string()))?;
    // Normalize "." -> "" for non-partitioned tables
    let key = if raw_key == FilesPartitionRecord::NON_PARTITIONED_NAME {
        String::new()
    } else {
        raw_key.to_string()
    };

    let value = record.value();
    if value.is_empty() {
        return Ok(FilesPartitionRecord {
            key,
            record_type: MetadataRecordType::Files,
            files: HashMap::new(),
        });
    }

    let avro_value = decode_avro_value(value, schema)?;
    let record_type = get_record_type(&avro_value);
    let mut files = extract_filesystem_metadata(&avro_value);

    // Normalize "." -> "" in AllPartitions files map
    if record_type == MetadataRecordType::AllPartitions {
        if let Some(mut file_info) = files.remove(FilesPartitionRecord::NON_PARTITIONED_NAME) {
            file_info.name = String::new();
            files.insert(String::new(), file_info);
        }
    }

    Ok(FilesPartitionRecord {
        key,
        record_type,
        files,
    })
}

/// Extract filesystemMetadata from an Avro-decoded HoodieMetadataRecord.
///
/// The filesystemMetadata field is a map where:
/// - Keys are file names (or partition names for ALL_PARTITIONS)
/// - Values are HoodieMetadataFileInfo records with {size, isDeleted}
pub fn extract_filesystem_metadata(
    avro_value: &AvroValue,
) -> HashMap<String, HoodieMetadataFileInfo> {
    let mut files = HashMap::new();

    // Get the filesystemMetadata field (may be a union with null)
    let fs_metadata = match avro_value {
        AvroValue::Record(fields) => fields.iter().find_map(|(name, val)| {
            if name == "filesystemMetadata" {
                match val {
                    AvroValue::Map(map) => Some(map),
                    AvroValue::Union(_, inner) => {
                        if let AvroValue::Map(map) = inner.as_ref() {
                            Some(map)
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            } else {
                None
            }
        }),
        _ => None,
    };

    if let Some(map) = fs_metadata {
        for (name, value) in map {
            if let Some(file_info) = extract_file_info(name, value) {
                files.insert(name.clone(), file_info);
            }
        }
    }

    files
}

/// Extract HoodieMetadataFileInfo from an Avro value.
fn extract_file_info(name: &str, value: &AvroValue) -> Option<HoodieMetadataFileInfo> {
    // The value is HoodieMetadataFileInfo record: {size: long, isDeleted: boolean}
    let record = match value {
        AvroValue::Record(fields) => fields,
        AvroValue::Union(_, inner) => {
            if let AvroValue::Record(fields) = inner.as_ref() {
                fields
            } else {
                return None;
            }
        }
        _ => return None,
    };

    let mut size: i64 = 0;
    let mut is_deleted = false;

    for (field_name, field_value) in record {
        match field_name.as_str() {
            "size" => {
                if let Some(n) = extract_long(field_value) {
                    size = n;
                }
            }
            "isDeleted" => {
                if let Some(b) = extract_bool(field_value) {
                    is_deleted = b;
                }
            }
            _ => {}
        }
    }

    Some(HoodieMetadataFileInfo::new(
        name.to_string(),
        size,
        is_deleted,
    ))
}

/// Extract i64 from an Avro value (handles union types).
fn extract_long(value: &AvroValue) -> Option<i64> {
    match value {
        AvroValue::Long(n) => Some(*n),
        AvroValue::Int(n) => Some(*n as i64),
        AvroValue::Union(_, inner) => extract_long(inner),
        _ => None,
    }
}

/// Extract bool from an Avro value (handles union types).
fn extract_bool(value: &AvroValue) -> Option<bool> {
    match value {
        AvroValue::Boolean(b) => Some(*b),
        AvroValue::Union(_, inner) => extract_bool(inner),
        _ => None,
    }
}

// ============================================================================
// Avro-based decoding
// ============================================================================

/// Decode a metadata record value using the provided Avro schema.
///
/// The schema should be obtained from the HFile's file info ("schema" key).
/// Returns the decoded Avro value as a GenericRecord.
pub fn decode_avro_value(value: &[u8], schema: &AvroSchema) -> Result<AvroValue> {
    if value.is_empty() {
        return Err(CoreError::MetadataTable("Empty value".to_string()));
    }

    apache_avro::from_avro_datum(schema, &mut &value[..], None)
        .map_err(|e| CoreError::MetadataTable(format!("Avro decode error: {e}")))
}

/// Parse an Avro schema from JSON string.
pub fn parse_avro_schema(schema_json: &str) -> Result<AvroSchema> {
    AvroSchema::parse_str(schema_json)
        .map_err(|e| CoreError::MetadataTable(format!("Invalid Avro schema: {e}")))
}

/// Extract an i32 field from an Avro record.
fn get_avro_int(value: &AvroValue, field: &str) -> Option<i32> {
    if let AvroValue::Record(fields) = value {
        for (name, val) in fields {
            if name == field {
                return match val {
                    AvroValue::Int(n) => Some(*n),
                    AvroValue::Union(_, inner) => {
                        if let AvroValue::Int(n) = inner.as_ref() {
                            Some(*n)
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
            }
        }
    }
    None
}

/// Get the record type from an Avro-decoded metadata record.
pub fn get_record_type(avro_value: &AvroValue) -> MetadataRecordType {
    get_avro_int(avro_value, "type")
        .map(MetadataRecordType::from)
        .unwrap_or(MetadataRecordType::Unknown)
}

// ============================================================================
// Column Stats Decoding
// ============================================================================

/// Extract a string from an Avro value (handles union types).
fn extract_string(value: &AvroValue) -> Option<String> {
    match value {
        AvroValue::String(s) => Some(s.clone()),
        AvroValue::Union(_, inner) => extract_string(inner),
        _ => None,
    }
}

/// Extract an i32 from an Avro value (handles union types).
fn extract_int(value: &AvroValue) -> Option<i32> {
    match value {
        AvroValue::Int(n) => Some(*n),
        AvroValue::Union(_, inner) => extract_int(inner),
        _ => None,
    }
}

/// Extract a f32 from an Avro value (handles union types).
#[allow(dead_code)]
fn extract_float(value: &AvroValue) -> Option<f32> {
    match value {
        AvroValue::Float(f) => Some(*f),
        AvroValue::Union(_, inner) => extract_float(inner),
        _ => None,
    }
}

/// Extract a f64 from an Avro value (handles union types).
#[allow(dead_code)]
fn extract_double(value: &AvroValue) -> Option<f64> {
    match value {
        AvroValue::Double(d) => Some(*d),
        AvroValue::Union(_, inner) => extract_double(inner),
        _ => None,
    }
}

/// Extract bytes from an Avro value (handles union types).
fn extract_bytes(value: &AvroValue) -> Option<Vec<u8>> {
    match value {
        AvroValue::Bytes(b) => Some(b.clone()),
        AvroValue::Union(_, inner) => extract_bytes(inner),
        _ => None,
    }
}

/// Extract a WrappedStatValue from an Avro union type.
///
/// The `minValue` and `maxValue` fields in column stats are Avro union types
/// that can contain various primitive and logical types.
fn extract_wrapped_value(value: &AvroValue) -> Option<WrappedStatValue> {
    match value {
        AvroValue::Null => Some(WrappedStatValue::Null),
        AvroValue::Boolean(b) => Some(WrappedStatValue::Boolean(*b)),
        AvroValue::Int(n) => Some(WrappedStatValue::Int(*n)),
        AvroValue::Long(n) => Some(WrappedStatValue::Long(*n)),
        AvroValue::Float(f) => Some(WrappedStatValue::Float(*f)),
        AvroValue::Double(d) => Some(WrappedStatValue::Double(*d)),
        AvroValue::String(s) => Some(WrappedStatValue::String(s.clone())),
        AvroValue::Bytes(b) => Some(WrappedStatValue::Bytes(b.clone())),
        AvroValue::Date(d) => Some(WrappedStatValue::Date(*d)),
        AvroValue::TimeMicros(t) => Some(WrappedStatValue::TimeMicros(*t)),
        AvroValue::TimestampMicros(t) => Some(WrappedStatValue::TimestampMicros(*t)),
        AvroValue::Union(_, inner) => extract_wrapped_value(inner),
        AvroValue::Array(items) => {
            // ArrayWrapper contains an array of wrapped values
            let values: Vec<WrappedStatValue> =
                items.iter().filter_map(extract_wrapped_value).collect();
            Some(WrappedStatValue::Array(values))
        }
        AvroValue::Record(fields) => {
            // Handle wrapper records like IntWrapper, BooleanWrapper, etc.
            // These have a "value" field containing the actual primitive value.
            // Also handles DecimalWrapper which has value + precision + scale fields.
            // Also handles ArrayWrapper which has a "wrappedValues" field.
            extract_value_from_wrapper_record(fields)
        }
        _ => None,
    }
}

/// Extract a value from an Avro wrapper record structure.
///
/// Hudi uses wrapper records like IntWrapper, BooleanWrapper, etc. with a "value" field.
/// DecimalWrapper is a special case with value, precision, and scale fields.
/// ArrayWrapper has a "wrappedValues" field containing an array of wrapped values.
///
/// Wrapper records in HoodieMetadata.avsc:
/// - BooleanWrapper { value: boolean }
/// - IntWrapper { value: int }
/// - LongWrapper { value: long }
/// - FloatWrapper { value: float }
/// - DoubleWrapper { value: double }
/// - BytesWrapper { value: bytes }
/// - StringWrapper { value: string }
/// - DateWrapper { value: int } (date logical type)
/// - DecimalWrapper { value: bytes, precision: int, scale: int }
/// - TimeMicrosWrapper { value: long } (time-micros logical type)
/// - TimestampMicrosWrapper { value: long }
/// - LocalDateWrapper { value: int }
/// - ArrayWrapper { wrappedValues: array }
fn extract_value_from_wrapper_record(fields: &[(String, AvroValue)]) -> Option<WrappedStatValue> {
    // Check if this is a DecimalWrapper (has precision and scale fields)
    let has_precision = fields.iter().any(|(n, _)| n == "precision");
    let has_scale = fields.iter().any(|(n, _)| n == "scale");

    // Check if this is an ArrayWrapper (has wrappedValues field)
    let has_wrapped_values = fields.iter().any(|(n, _)| n == "wrappedValues");

    if has_precision && has_scale {
        // DecimalWrapper case
        let mut value_bytes = None;
        let mut precision = None;
        let mut scale = None;

        for (name, val) in fields {
            match name.as_str() {
                "value" => value_bytes = extract_bytes(val),
                "precision" => precision = extract_int(val),
                "scale" => scale = extract_int(val),
                _ => {}
            }
        }

        match (value_bytes, precision, scale) {
            (Some(v), Some(p), Some(s)) => Some(WrappedStatValue::Decimal {
                value: v,
                precision: p as u8,
                scale: s as i8,
            }),
            _ => None,
        }
    } else if has_wrapped_values {
        // ArrayWrapper case
        for (name, val) in fields {
            if name == "wrappedValues" {
                return extract_array_wrapper(val);
            }
        }
        None
    } else {
        // Simple wrapper case - extract the "value" field
        for (name, val) in fields {
            if name == "value" {
                return extract_wrapped_value(val);
            }
        }
        None
    }
}

/// Extract an array of wrapped values from an ArrayWrapper's wrappedValues field.
fn extract_array_wrapper(value: &AvroValue) -> Option<WrappedStatValue> {
    let items = match value {
        AvroValue::Array(items) => items,
        AvroValue::Union(_, inner) => {
            if let AvroValue::Array(items) = inner.as_ref() {
                items
            } else {
                return None;
            }
        }
        AvroValue::Null => return Some(WrappedStatValue::Array(vec![])),
        _ => return None,
    };

    let values: Vec<WrappedStatValue> = items.iter().filter_map(extract_wrapped_value).collect();
    Some(WrappedStatValue::Array(values))
}

/// Decode column stats records from an HFile record.
///
/// # Arguments
/// * `reader` - The HFile reader (provides the Avro schema)
/// * `record` - The HFile record containing the Avro-serialized value
///
/// # Returns
/// A vector of `ColumnStatsRecord` entries (one per file in the record).
pub fn decode_column_stats_record(
    reader: &HFileReader,
    record: &HFileRecord,
) -> Result<Vec<ColumnStatsRecord>> {
    let schema = reader
        .get_avro_schema()
        .map_err(|e| CoreError::MetadataTable(format!("Failed to get schema: {e}")))?
        .ok_or_else(|| CoreError::MetadataTable("No Avro schema in HFile".to_string()))?;

    decode_column_stats_record_with_schema(record, schema)
}

/// Decode column stats records using a provided Avro schema.
///
/// The HoodieMetadataRecord has a `columnStatMetadata` field containing
/// a map of `file_name -> HoodieMetadataColumnStats`.
pub fn decode_column_stats_record_with_schema(
    record: &HFileRecord,
    schema: &AvroSchema,
) -> Result<Vec<ColumnStatsRecord>> {
    let value = record.value();
    if value.is_empty() {
        return Ok(vec![]); // Tombstone record
    }

    let avro_value = decode_avro_value(value, schema)?;

    // Verify record type is ColumnStats (type = 3)
    let record_type = get_record_type(&avro_value);
    if record_type != MetadataRecordType::ColumnStats {
        return Err(CoreError::MetadataTable(format!(
            "Expected ColumnStats record type (3), got {record_type:?}"
        )));
    }

    extract_column_stats_metadata(&avro_value)
}

/// Extract column stats from the columnStatMetadata field.
fn extract_column_stats_metadata(avro_value: &AvroValue) -> Result<Vec<ColumnStatsRecord>> {
    let mut records = Vec::new();

    // Find the columnStatMetadata field in the record
    let stat_metadata = match avro_value {
        AvroValue::Record(fields) => fields.iter().find_map(|(name, val)| {
            if name == "columnStatMetadata" {
                extract_map_from_union(val)
            } else {
                None
            }
        }),
        _ => None,
    };

    if let Some(map) = stat_metadata {
        for (file_name, stats_value) in map {
            if let Some(stats) = extract_single_column_stats(file_name, stats_value) {
                records.push(stats);
            }
        }
    }

    Ok(records)
}

/// Extract a Map from an Avro value that may be wrapped in a Union.
fn extract_map_from_union(
    value: &AvroValue,
) -> Option<&std::collections::HashMap<String, AvroValue>> {
    match value {
        AvroValue::Map(map) => Some(map),
        AvroValue::Union(_, inner) => extract_map_from_union(inner),
        _ => None,
    }
}

/// Extract a single ColumnStatsRecord from an Avro value.
fn extract_single_column_stats(file_name: &str, value: &AvroValue) -> Option<ColumnStatsRecord> {
    let fields = match value {
        AvroValue::Record(f) => f,
        AvroValue::Union(_, inner) => {
            if let AvroValue::Record(f) = inner.as_ref() {
                f
            } else {
                return None;
            }
        }
        _ => return None,
    };

    let mut stats = ColumnStatsRecord {
        file_name: file_name.to_string(),
        column_name: String::new(),
        min_value: None,
        max_value: None,
        value_count: 0,
        null_count: 0,
        total_size: 0,
        total_uncompressed_size: 0,
        is_deleted: false,
        is_tight_bound: true,
        value_type: None,
    };

    for (field_name, field_value) in fields {
        match field_name.as_str() {
            "fileName" => {
                stats.file_name = extract_string(field_value).unwrap_or(file_name.to_string())
            }
            "columnName" => stats.column_name = extract_string(field_value).unwrap_or_default(),
            "minValue" => stats.min_value = extract_wrapped_value(field_value),
            "maxValue" => stats.max_value = extract_wrapped_value(field_value),
            "valueCount" => stats.value_count = extract_long(field_value).unwrap_or(0),
            "nullCount" => stats.null_count = extract_long(field_value).unwrap_or(0),
            "totalSize" => stats.total_size = extract_long(field_value).unwrap_or(0),
            "totalUncompressedSize" => {
                stats.total_uncompressed_size = extract_long(field_value).unwrap_or(0)
            }
            "isDeleted" => stats.is_deleted = extract_bool(field_value).unwrap_or(false),
            "isTightBound" => stats.is_tight_bound = extract_bool(field_value).unwrap_or(true),
            "valueType" => stats.value_type = extract_value_type_info(field_value),
            _ => {}
        }
    }

    Some(stats)
}

/// Extract ValueTypeInfo from an Avro value (HoodieValueTypeInfo record).
fn extract_value_type_info(value: &AvroValue) -> Option<ValueTypeInfo> {
    let fields = match value {
        AvroValue::Record(f) => f,
        AvroValue::Union(_, inner) => {
            if let AvroValue::Record(f) = inner.as_ref() {
                f
            } else {
                return None;
            }
        }
        AvroValue::Null => return None,
        _ => return None,
    };

    let mut type_ordinal = None;
    let mut additional_info = None;

    for (name, val) in fields {
        match name.as_str() {
            "typeOrdinal" => type_ordinal = extract_int(val),
            "additionalInfo" => additional_info = extract_string(val),
            _ => {}
        }
    }

    type_ordinal.map(|ordinal| ValueTypeInfo {
        type_ordinal: ordinal,
        additional_info,
    })
}

/// Decode partition stats records from an HFile record.
///
/// # Arguments
/// * `reader` - The HFile reader (provides the Avro schema)
/// * `record` - The HFile record containing the Avro-serialized value
///
/// # Returns
/// A vector of `PartitionStatsRecord` entries.
pub fn decode_partition_stats_record(
    reader: &HFileReader,
    record: &HFileRecord,
) -> Result<Vec<PartitionStatsRecord>> {
    let schema = reader
        .get_avro_schema()
        .map_err(|e| CoreError::MetadataTable(format!("Failed to get schema: {e}")))?
        .ok_or_else(|| CoreError::MetadataTable("No Avro schema in HFile".to_string()))?;

    decode_partition_stats_record_with_schema(record, schema)
}

/// Decode partition stats records using a provided Avro schema.
///
/// Partition stats use the same columnStatMetadata structure as column stats,
/// but the key format is different (no file hash).
pub fn decode_partition_stats_record_with_schema(
    record: &HFileRecord,
    schema: &AvroSchema,
) -> Result<Vec<PartitionStatsRecord>> {
    let value = record.value();
    if value.is_empty() {
        return Ok(vec![]); // Tombstone record
    }

    let avro_value = decode_avro_value(value, schema)?;

    // Verify record type is PartitionStats (type = 6)
    let record_type = get_record_type(&avro_value);
    if record_type != MetadataRecordType::PartitionStats {
        return Err(CoreError::MetadataTable(format!(
            "Expected PartitionStats record type (6), got {record_type:?}"
        )));
    }

    extract_partition_stats_metadata(&avro_value)
}

/// Extract partition stats from the columnStatMetadata field.
///
/// Partition stats use the same Avro schema field as column stats.
fn extract_partition_stats_metadata(avro_value: &AvroValue) -> Result<Vec<PartitionStatsRecord>> {
    let mut records = Vec::new();

    // Partition stats also use columnStatMetadata field
    let stat_metadata = match avro_value {
        AvroValue::Record(fields) => fields.iter().find_map(|(name, val)| {
            if name == "columnStatMetadata" {
                extract_map_from_union(val)
            } else {
                None
            }
        }),
        _ => None,
    };

    if let Some(map) = stat_metadata {
        for (partition_key, stats_value) in map {
            if let Some(stats) = extract_single_partition_stats(partition_key, stats_value) {
                records.push(stats);
            }
        }
    }

    Ok(records)
}

/// Extract a single PartitionStatsRecord from an Avro value.
fn extract_single_partition_stats(
    partition_key: &str,
    value: &AvroValue,
) -> Option<PartitionStatsRecord> {
    let fields = match value {
        AvroValue::Record(f) => f,
        AvroValue::Union(_, inner) => {
            if let AvroValue::Record(f) = inner.as_ref() {
                f
            } else {
                return None;
            }
        }
        _ => return None,
    };

    let mut stats = PartitionStatsRecord {
        partition_path: partition_key.to_string(),
        column_name: String::new(),
        min_value: None,
        max_value: None,
        value_count: 0,
        null_count: 0,
        total_size: 0,
        is_deleted: false,
        is_tight_bound: true,
    };

    for (field_name, field_value) in fields {
        match field_name.as_str() {
            "columnName" => stats.column_name = extract_string(field_value).unwrap_or_default(),
            "minValue" => stats.min_value = extract_wrapped_value(field_value),
            "maxValue" => stats.max_value = extract_wrapped_value(field_value),
            "valueCount" => stats.value_count = extract_long(field_value).unwrap_or(0),
            "nullCount" => stats.null_count = extract_long(field_value).unwrap_or(0),
            "totalSize" => stats.total_size = extract_long(field_value).unwrap_or(0),
            "isDeleted" => stats.is_deleted = extract_bool(field_value).unwrap_or(false),
            "isTightBound" => stats.is_tight_bound = extract_bool(field_value).unwrap_or(true),
            _ => {}
        }
    }

    Some(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hudi_test::QuickstartTripsTable;
    use std::path::PathBuf;

    /// Get the files partition directory for the test table.
    fn files_partition_dir() -> PathBuf {
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        PathBuf::from(table_path)
            .join(".hoodie")
            .join("metadata")
            .join("files")
    }

    /// Find the latest HFile in the files partition directory.
    fn files_partition_hfile_path() -> PathBuf {
        let dir = files_partition_dir();
        let mut hfiles: Vec<_> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("Failed to read directory {dir:?}: {e}"))
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .map(|ext| ext == "hfile")
                    .unwrap_or(false)
            })
            .collect();

        // Sort by filename to get the latest (timestamps are in filename)
        hfiles.sort_by_key(|e| e.file_name());

        hfiles
            .last()
            .map(|e| e.path())
            .unwrap_or_else(|| panic!("No HFile found in {dir:?}"))
    }

    #[test]
    fn test_metadata_partition_type_partition_name() {
        assert_eq!(MetadataPartitionType::Files.partition_name(), "files");
        assert_eq!(
            MetadataPartitionType::ColumnStats.partition_name(),
            "column_stats"
        );
        assert_eq!(
            MetadataPartitionType::PartitionStats.partition_name(),
            "partition_stats"
        );
        assert_eq!(
            MetadataPartitionType::RecordIndex.partition_name(),
            "record_index"
        );
    }

    #[test]
    fn test_metadata_partition_type_display() {
        assert_eq!(format!("{}", MetadataPartitionType::Files), "files");
        assert_eq!(
            format!("{}", MetadataPartitionType::ColumnStats),
            "column_stats"
        );
        assert_eq!(
            format!("{}", MetadataPartitionType::PartitionStats),
            "partition_stats"
        );
        assert_eq!(
            format!("{}", MetadataPartitionType::RecordIndex),
            "record_index"
        );
    }

    #[test]
    fn test_metadata_record_type_from_i32() {
        assert_eq!(
            MetadataRecordType::from(1),
            MetadataRecordType::AllPartitions
        );
        assert_eq!(MetadataRecordType::from(2), MetadataRecordType::Files);
        assert_eq!(MetadataRecordType::from(3), MetadataRecordType::ColumnStats);
        assert_eq!(
            MetadataRecordType::from(4),
            MetadataRecordType::BloomFilters
        );
        assert_eq!(MetadataRecordType::from(5), MetadataRecordType::RecordIndex);
        assert_eq!(
            MetadataRecordType::from(6),
            MetadataRecordType::PartitionStats
        );
        assert_eq!(
            MetadataRecordType::from(7),
            MetadataRecordType::SecondaryIndex
        );
        assert_eq!(MetadataRecordType::from(99), MetadataRecordType::Unknown);
    }

    #[test]
    fn test_files_partition_record_active_files() {
        let mut files = HashMap::new();
        files.insert(
            "active.parquet".to_string(),
            HoodieMetadataFileInfo::new("active.parquet".to_string(), 1000, false),
        );
        files.insert(
            "deleted.parquet".to_string(),
            HoodieMetadataFileInfo::new("deleted.parquet".to_string(), 500, true),
        );

        let record = FilesPartitionRecord {
            key: "partition".to_string(),
            record_type: MetadataRecordType::Files,
            files,
        };

        let active = record.active_file_names();
        assert_eq!(active.len(), 1);
        assert!(active.contains(&"active.parquet"));

        assert!(record.has_active_file("active.parquet"));
        assert!(!record.has_active_file("deleted.parquet"));
        assert!(!record.has_active_file("nonexistent.parquet"));

        // Test total_size
        assert_eq!(record.total_size(), 1000);

        // Test is_all_partitions
        assert!(!record.is_all_partitions());
    }

    #[test]
    fn test_files_partition_avro_decode() {
        // Use test data from quickstart_trips_table v8_trips_8i3u1d via QuickstartTripsTable
        let path = files_partition_hfile_path();
        let bytes = std::fs::read(&path).expect("Failed to read test file");
        let reader = HFileReader::new(bytes.clone()).expect("Failed to create reader");
        let mut reader_mut = HFileReader::new(bytes).expect("Failed to create reader");
        let records = reader_mut
            .collect_records()
            .expect("Failed to collect records");

        // Test ALL_PARTITIONS record
        let all_partitions_record = records
            .iter()
            .find(|r| r.key_as_str() == Some(FilesPartitionRecord::ALL_PARTITIONS_KEY))
            .expect("__all_partitions__ record not found");

        let decoded = decode_files_partition_record(&reader, all_partitions_record)
            .expect("Failed to decode ALL_PARTITIONS");

        assert_eq!(decoded.record_type, MetadataRecordType::AllPartitions);
        assert!(decoded.is_all_partitions());

        // Validate partition names
        let partition_names = decoded.partition_names();
        assert_eq!(partition_names.len(), 3, "Should have 3 partitions");
        assert!(decoded.files.contains_key("city=chennai"));
        assert!(decoded.files.contains_key("city=san_francisco"));
        assert!(decoded.files.contains_key("city=sao_paulo"));

        // Test FILES record for chennai partition
        let chennai_record = records
            .iter()
            .find(|r| r.key_as_str() == Some("city=chennai"))
            .expect("chennai record not found");

        let files_record =
            decode_files_partition_record(&reader, chennai_record).expect("Failed to decode FILES");

        assert_eq!(files_record.record_type, MetadataRecordType::Files);
        assert!(!files_record.is_all_partitions());

        // Debug: Print all files found
        println!("Chennai files ({}):", files_record.files.len());
        for (name, info) in &files_record.files {
            println!(
                "  - {} (size={}, deleted={})",
                name, info.size, info.is_deleted
            );
        }

        // The test table may have more files (base + log files)
        assert!(
            files_record.files.len() >= 2,
            "chennai should have at least 2 files"
        );

        // Validate file info (size should be > 0 for actual files)
        // Count parquet files with the expected UUID
        let parquet_files: Vec<_> = files_record
            .files
            .iter()
            .filter(|(name, _)| name.ends_with(".parquet"))
            .collect();

        assert_eq!(
            parquet_files.len(),
            2,
            "chennai should have 2 parquet files"
        );

        for (file_name, file_info) in &parquet_files {
            assert!(
                file_name.contains("6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc"),
                "File should contain chennai UUID: {file_name}"
            );
            assert!(file_info.size > 0, "File size should be > 0: {file_info:?}");
            assert!(!file_info.is_deleted, "File should not be deleted");
        }

        // Validate total size
        assert!(files_record.total_size() > 0, "Total size should be > 0");
    }

    #[test]
    fn test_hoodie_metadata_file_info_new() {
        let info = HoodieMetadataFileInfo::new("test.parquet".to_string(), 12345, false);
        assert_eq!(info.name, "test.parquet");
        assert_eq!(info.size, 12345);
        assert!(!info.is_deleted);

        let deleted_info = HoodieMetadataFileInfo::new("deleted.parquet".to_string(), 0, true);
        assert_eq!(deleted_info.name, "deleted.parquet");
        assert_eq!(deleted_info.size, 0);
        assert!(deleted_info.is_deleted);
    }

    #[test]
    fn test_files_partition_record_all_file_names() {
        let mut files = HashMap::new();
        files.insert(
            "file1.parquet".to_string(),
            HoodieMetadataFileInfo::new("file1.parquet".to_string(), 1000, false),
        );
        files.insert(
            "file2.parquet".to_string(),
            HoodieMetadataFileInfo::new("file2.parquet".to_string(), 500, true),
        );
        files.insert(
            "file3.parquet".to_string(),
            HoodieMetadataFileInfo::new("file3.parquet".to_string(), 2000, false),
        );

        let record = FilesPartitionRecord {
            key: "partition".to_string(),
            record_type: MetadataRecordType::Files,
            files,
        };

        let all_names = record.all_file_names();
        assert_eq!(all_names.len(), 3);
        assert!(all_names.contains(&"file1.parquet"));
        assert!(all_names.contains(&"file2.parquet"));
        assert!(all_names.contains(&"file3.parquet"));
    }

    #[test]
    fn test_files_partition_record_partition_names_for_non_all_partitions() {
        let mut files = HashMap::new();
        files.insert(
            "file.parquet".to_string(),
            HoodieMetadataFileInfo::new("file.parquet".to_string(), 1000, false),
        );

        let record = FilesPartitionRecord {
            key: "city=chennai".to_string(),
            record_type: MetadataRecordType::Files,
            files,
        };

        // partition_names() should return empty for non-AllPartitions record
        let partition_names = record.partition_names();
        assert!(partition_names.is_empty());
    }

    #[test]
    fn test_parse_avro_schema() {
        // Valid schema
        let schema_json =
            r#"{"type": "record", "name": "Test", "fields": [{"name": "id", "type": "int"}]}"#;
        assert!(parse_avro_schema(schema_json).is_ok());

        // Invalid schema
        let result = parse_avro_schema("not valid json");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid Avro schema")
        );
    }

    #[test]
    fn test_get_record_type() {
        // Record without type field -> Unknown
        let no_type = AvroValue::Record(vec![("other".to_string(), AvroValue::Int(42))]);
        assert_eq!(get_record_type(&no_type), MetadataRecordType::Unknown);

        // Non-record value -> Unknown
        let non_record = AvroValue::String("test".to_string());
        assert_eq!(get_record_type(&non_record), MetadataRecordType::Unknown);

        // Union containing Int -> works
        let union_int = AvroValue::Record(vec![(
            "type".to_string(),
            AvroValue::Union(0, Box::new(AvroValue::Int(2))),
        )]);
        assert_eq!(get_record_type(&union_int), MetadataRecordType::Files);
    }

    #[test]
    fn test_extract_long() {
        assert_eq!(extract_long(&AvroValue::Int(42)), Some(42));
        assert_eq!(extract_long(&AvroValue::Long(123456789)), Some(123456789));
        assert_eq!(
            extract_long(&AvroValue::Union(0, Box::new(AvroValue::Long(999)))),
            Some(999)
        );
        assert_eq!(
            extract_long(&AvroValue::String("not a number".to_string())),
            None
        );
    }

    #[test]
    fn test_extract_bool() {
        assert_eq!(extract_bool(&AvroValue::Boolean(true)), Some(true));
        assert_eq!(extract_bool(&AvroValue::Boolean(false)), Some(false));
        assert_eq!(
            extract_bool(&AvroValue::Union(0, Box::new(AvroValue::Boolean(true)))),
            Some(true)
        );
        assert_eq!(
            extract_bool(&AvroValue::String("not a bool".to_string())),
            None
        );
    }

    #[test]
    fn test_extract_filesystem_metadata() {
        // Non-record -> empty
        assert!(
            extract_filesystem_metadata(&AvroValue::String("not a record".to_string())).is_empty()
        );

        // Record without field -> empty
        let no_field = AvroValue::Record(vec![("other".to_string(), AvroValue::Int(42))]);
        assert!(extract_filesystem_metadata(&no_field).is_empty());

        // Union with Null -> empty
        let union_null = AvroValue::Record(vec![(
            "filesystemMetadata".to_string(),
            AvroValue::Union(0, Box::new(AvroValue::Null)),
        )]);
        assert!(extract_filesystem_metadata(&union_null).is_empty());

        // Invalid type (String) -> empty
        let invalid_type = AvroValue::Record(vec![(
            "filesystemMetadata".to_string(),
            AvroValue::String("not a map".to_string()),
        )]);
        assert!(extract_filesystem_metadata(&invalid_type).is_empty());

        // Union with Map -> works
        use std::collections::HashMap as StdMap;
        let mut map = StdMap::new();
        map.insert(
            "test.parquet".to_string(),
            AvroValue::Record(vec![
                ("size".to_string(), AvroValue::Long(1000)),
                ("isDeleted".to_string(), AvroValue::Boolean(false)),
            ]),
        );
        let union_map = AvroValue::Record(vec![(
            "filesystemMetadata".to_string(),
            AvroValue::Union(1, Box::new(AvroValue::Map(map))),
        )]);
        let result = extract_filesystem_metadata(&union_map);
        assert_eq!(result.len(), 1);
        let info = result.get("test.parquet").unwrap();
        assert_eq!(info.size, 1000);
        assert!(!info.is_deleted);
    }

    #[test]
    fn test_extract_file_info() {
        // Non-Record/Union -> None
        assert!(
            extract_file_info(
                "test.parquet",
                &AvroValue::String("not a record".to_string())
            )
            .is_none()
        );

        // Union with non-Record -> None
        let union_string =
            AvroValue::Union(0, Box::new(AvroValue::String("not a record".to_string())));
        assert!(extract_file_info("test.parquet", &union_string).is_none());

        // Union with Record -> works
        let union_record = AvroValue::Union(
            1,
            Box::new(AvroValue::Record(vec![
                ("size".to_string(), AvroValue::Long(5000)),
                ("isDeleted".to_string(), AvroValue::Boolean(true)),
            ])),
        );
        let info = extract_file_info("deleted.parquet", &union_record).unwrap();
        assert_eq!(info.name, "deleted.parquet");
        assert_eq!(info.size, 5000);
        assert!(info.is_deleted);
    }

    #[test]
    fn test_get_avro_int() {
        // Union with non-Int -> None
        let union_string = AvroValue::Record(vec![(
            "type".to_string(),
            AvroValue::Union(0, Box::new(AvroValue::String("not int".to_string()))),
        )]);
        assert!(get_avro_int(&union_string, "type").is_none());

        // Non-Int, non-Union -> None
        let direct_string = AvroValue::Record(vec![(
            "type".to_string(),
            AvroValue::String("not int".to_string()),
        )]);
        assert!(get_avro_int(&direct_string, "type").is_none());

        // Direct Int -> works
        let direct_int = AvroValue::Record(vec![("type".to_string(), AvroValue::Int(3))]);
        assert_eq!(get_avro_int(&direct_int, "type"), Some(3));
    }

    #[test]
    fn test_decode_files_partition_record_with_schema_tombstone() {
        // Test tombstone record (empty value)
        let record = crate::hfile::HFileRecord::new(b"deleted_partition".to_vec(), vec![]);
        let schema = parse_avro_schema(
            r#"{"type": "record", "name": "Test", "fields": [{"name": "type", "type": "int"}]}"#,
        )
        .unwrap();
        let result = decode_files_partition_record_with_schema(&record, &schema);
        assert!(result.is_ok());
        let decoded = result.unwrap();
        assert_eq!(decoded.key, "deleted_partition");
        assert_eq!(decoded.record_type, MetadataRecordType::Files);
        assert!(decoded.files.is_empty());
    }

    #[test]
    fn test_decode_files_partition_record_with_schema_invalid_key() {
        // Test record with invalid UTF-8 key
        let record = crate::hfile::HFileRecord::new(vec![0xff, 0xfe], b"value".to_vec());
        let schema = parse_avro_schema(
            r#"{"type": "record", "name": "Test", "fields": [{"name": "type", "type": "int"}]}"#,
        )
        .unwrap();
        let result = decode_files_partition_record_with_schema(&record, &schema);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid UTF-8 key")
        );
    }

    #[test]
    fn test_decode_avro_value() {
        let schema = parse_avro_schema(
            r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "value", "type": "long"}]}"#,
        )
        .unwrap();

        // Empty value
        let empty_result = decode_avro_value(&[], &schema);
        assert!(empty_result.is_err());
        assert!(
            empty_result
                .unwrap_err()
                .to_string()
                .contains("Empty value")
        );

        // Invalid bytes (truncated varint)
        let invalid_bytes: &[u8] = &[0xff, 0xff, 0xff, 0xff, 0xff];
        let invalid_result = decode_avro_value(invalid_bytes, &schema);
        assert!(invalid_result.is_err());
        assert!(
            invalid_result
                .unwrap_err()
                .to_string()
                .contains("Avro decode error")
        );
    }
}
