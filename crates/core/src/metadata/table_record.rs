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

use crate::error::CoreError;
use crate::hfile::{HFileReader, HFileRecord};
use crate::Result;
use apache_avro::types::Value as AvroValue;
use apache_avro::Schema as AvroSchema;
use std::collections::HashMap;

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
/// These values match the Java implementation in `MetadataPartitionType`.
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
    /// Check if this is an ALL_PARTITIONS record.
    pub fn is_all_partitions(&self) -> bool {
        self.record_type == MetadataRecordType::AllPartitions
    }

    /// Get list of partition names (for ALL_PARTITIONS record).
    pub fn partition_names(&self) -> Vec<&str> {
        if self.is_all_partitions() {
            self.files.keys().map(|s| s.as_str()).collect()
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
        self.files
            .get(name)
            .map(|f| !f.is_deleted)
            .unwrap_or(false)
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
    let key = record
        .key_as_str()
        .ok_or_else(|| CoreError::MetadataTable("Invalid UTF-8 key".to_string()))?
        .to_string();

    let value = record.value();
    if value.is_empty() {
        // Tombstone record - treat as deleted Files record
        return Ok(FilesPartitionRecord {
            key,
            record_type: MetadataRecordType::Files,
            files: HashMap::new(),
        });
    }

    // Get schema from HFile reader
    let schema = reader
        .get_avro_schema()
        .map_err(|e| CoreError::MetadataTable(format!("Failed to get schema: {}", e)))?
        .ok_or_else(|| CoreError::MetadataTable("No Avro schema in HFile".to_string()))?;

    // Decode using Avro
    let avro_value = decode_avro_value(value, schema)?;

    // Extract record type
    let record_type = get_record_type(&avro_value);

    // Extract filesystemMetadata map
    let files = extract_filesystem_metadata(&avro_value);

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
fn extract_filesystem_metadata(avro_value: &AvroValue) -> HashMap<String, HoodieMetadataFileInfo> {
    let mut files = HashMap::new();

    // Get the filesystemMetadata field (may be a union with null)
    let fs_metadata = match avro_value {
        AvroValue::Record(fields) => {
            fields.iter().find_map(|(name, val)| {
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
            })
        }
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

    Some(HoodieMetadataFileInfo::new(name.to_string(), size, is_deleted))
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

/// Decode an HFile record value from the files partition using pattern matching.
///
/// This is a fallback method that extracts file names by searching for known
/// patterns (UUIDs + file extensions) in the raw bytes. Use this when Avro
/// decoding is not available or fails.
///
/// # Arguments
/// * `record` - The HFile record containing the Avro-serialized value
///
/// # Returns
/// A `FilesPartitionRecord` with the decoded file information.
/// Note: record_type defaults to Files since it cannot be determined without Avro.
pub fn decode_files_partition_record_fallback(record: &HFileRecord) -> Result<FilesPartitionRecord> {
    let key = record
        .key_as_str()
        .ok_or_else(|| CoreError::MetadataTable("Invalid UTF-8 key".to_string()))?
        .to_string();

    let value = record.value();
    if value.is_empty() {
        // Tombstone record
        return Ok(FilesPartitionRecord {
            key,
            record_type: MetadataRecordType::Files,
            files: HashMap::new(),
        });
    }

    // Determine record type based on key
    let record_type = if key == "__all_partitions__" {
        MetadataRecordType::AllPartitions
    } else {
        MetadataRecordType::Files
    };

    // Extract file names by pattern matching in the raw bytes.
    let files = extract_file_names_from_bytes(value);

    Ok(FilesPartitionRecord {
        key,
        record_type,
        files,
    })
}

/// Extract file names from raw metadata record bytes.
///
/// This searches for file name patterns embedded in the Avro-encoded data.
/// File names are stored as map keys in the filesystemMetadata field.
///
/// Parquet files have the pattern: `uuid-X_X-X-X_timestamp.parquet`
/// Log files have the pattern: `.uuid-X_timestamp.log.version_token`
fn extract_file_names_from_bytes(bytes: &[u8]) -> HashMap<String, HoodieMetadataFileInfo> {
    let mut files = HashMap::new();

    // Convert bytes to string for pattern matching
    let text = String::from_utf8_lossy(bytes);

    // Find parquet files by searching for ".parquet" and extracting the full filename
    let mut search_start = 0;
    while let Some(ext_pos) = text[search_start..].find(".parquet") {
        let abs_pos = search_start + ext_pos;
        let end_pos = abs_pos + ".parquet".len();

        // Find the start of the filename by looking backwards for a non-filename character
        // File names contain: hex digits, dashes, underscores, dots
        let start_pos = find_filename_start(&text[..abs_pos]);

        if start_pos < abs_pos {
            let filename = &text[start_pos..end_pos];
            // Validate it looks like a Hudi file name (contains UUID pattern)
            if is_valid_hudi_filename(filename) {
                files.insert(
                    filename.to_string(),
                    HoodieMetadataFileInfo::new(filename.to_string(), 0, false),
                );
            }
        }

        search_start = end_pos;
    }

    // Find log files by searching for ".log." pattern
    search_start = 0;
    while let Some(ext_pos) = text[search_start..].find(".log.") {
        let abs_pos = search_start + ext_pos;

        // Find the end of log file name (after version_token)
        let after_log = abs_pos + ".log.".len();
        let end_pos = find_log_filename_end(&text[after_log..]) + after_log;

        // Find the start - log files start with '.'
        let mut start_pos = abs_pos;
        while start_pos > 0 {
            let c = text.chars().nth(start_pos - 1).unwrap_or(' ');
            if !c.is_ascii_alphanumeric() && c != '-' && c != '_' {
                break;
            }
            start_pos -= 1;
        }
        // Include the leading dot if present
        if start_pos > 0 && text.chars().nth(start_pos - 1) == Some('.') {
            start_pos -= 1;
        }

        if start_pos < end_pos {
            let filename = &text[start_pos..end_pos];
            if filename.starts_with('.') && filename.contains('-') {
                files.insert(
                    filename.to_string(),
                    HoodieMetadataFileInfo::new(filename.to_string(), 0, false),
                );
            }
        }

        search_start = end_pos;
    }

    files
}

/// Find the start position of a filename by scanning backwards.
fn find_filename_start(text: &str) -> usize {
    let chars: Vec<char> = text.chars().collect();
    let mut pos = chars.len();

    while pos > 0 {
        let c = chars[pos - 1];
        // File name characters: alphanumeric, dash, underscore, dot (for version separator)
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
            pos -= 1;
        } else {
            break;
        }
    }

    // Convert char position back to byte position
    text.chars().take(pos).map(|c| c.len_utf8()).sum()
}

/// Find the end position of a log file name (version_token part).
fn find_log_filename_end(text: &str) -> usize {
    let mut pos = 0;
    for c in text.chars() {
        // Log file version_token contains digits, dashes, underscores
        if c.is_ascii_digit() || c == '-' || c == '_' {
            pos += c.len_utf8();
        } else {
            break;
        }
    }
    pos
}

/// Check if a filename looks like a valid Hudi data file.
fn is_valid_hudi_filename(filename: &str) -> bool {
    // Must contain a UUID-like pattern (8-4-4-4-12 hex)
    // and end with .parquet or contain .log.
    if !filename.ends_with(".parquet") && !filename.contains(".log.") {
        return false;
    }

    // Count hex characters to check for UUID presence
    let parts: Vec<&str> = filename.split('-').collect();
    if parts.len() < 5 {
        return false;
    }

    // First part should be hex (8 chars)
    parts[0].len() == 8 && parts[0].chars().all(|c| c.is_ascii_hexdigit())
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
        .map_err(|e| CoreError::MetadataTable(format!("Avro decode error: {}", e)))
}

/// Parse an Avro schema from JSON string.
pub fn parse_avro_schema(schema_json: &str) -> Result<AvroSchema> {
    AvroSchema::parse_str(schema_json)
        .map_err(|e| CoreError::MetadataTable(format!("Invalid Avro schema: {}", e)))
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
            .unwrap_or_else(|e| panic!("Failed to read directory {:?}: {}", dir, e))
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
            .unwrap_or_else(|| panic!("No HFile found in {:?}", dir))
    }

    #[test]
    fn test_metadata_record_type_from_i32() {
        assert_eq!(MetadataRecordType::from(1), MetadataRecordType::AllPartitions);
        assert_eq!(MetadataRecordType::from(2), MetadataRecordType::Files);
        assert_eq!(MetadataRecordType::from(3), MetadataRecordType::ColumnStats);
        assert_eq!(MetadataRecordType::from(4), MetadataRecordType::BloomFilters);
        assert_eq!(MetadataRecordType::from(5), MetadataRecordType::RecordIndex);
        assert_eq!(MetadataRecordType::from(6), MetadataRecordType::PartitionStats);
        assert_eq!(MetadataRecordType::from(7), MetadataRecordType::SecondaryIndex);
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
        let records = reader_mut.collect_records().expect("Failed to collect records");

        // Test ALL_PARTITIONS record
        let all_partitions_record = records
            .iter()
            .find(|r| r.key_as_str() == Some("__all_partitions__"))
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

        let files_record = decode_files_partition_record(&reader, chennai_record)
            .expect("Failed to decode FILES");

        assert_eq!(files_record.record_type, MetadataRecordType::Files);
        assert!(!files_record.is_all_partitions());

        // Debug: Print all files found
        println!("Chennai files ({}):", files_record.files.len());
        for (name, info) in &files_record.files {
            println!("  - {} (size={}, deleted={})", name, info.size, info.is_deleted);
        }

        // The test table may have more files (base + log files)
        assert!(files_record.files.len() >= 2, "chennai should have at least 2 files");

        // Validate file info (size should be > 0 for actual files)
        // Count parquet files with the expected UUID
        let parquet_files: Vec<_> = files_record
            .files
            .iter()
            .filter(|(name, _)| name.ends_with(".parquet"))
            .collect();

        assert_eq!(parquet_files.len(), 2, "chennai should have 2 parquet files");

        for (file_name, file_info) in &parquet_files {
            assert!(
                file_name.contains("6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc"),
                "File should contain chennai UUID: {}",
                file_name
            );
            assert!(file_info.size > 0, "File size should be > 0: {:?}", file_info);
            assert!(!file_info.is_deleted, "File should not be deleted");
        }

        // Validate total size
        assert!(files_record.total_size() > 0, "Total size should be > 0");
    }

    #[test]
    fn test_files_partition_fallback_decode() {
        // Test the fallback pattern-based decoder using QuickstartTripsTable
        let path = files_partition_hfile_path();
        let bytes = std::fs::read(&path).expect("Failed to read test file");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");
        let records = reader.collect_records().expect("Failed to collect records");

        // Test fallback extraction for chennai partition
        let chennai_record = records
            .iter()
            .find(|r| r.key_as_str() == Some("city=chennai"))
            .expect("chennai record not found");

        let files_record =
            decode_files_partition_record_fallback(chennai_record).expect("Failed to decode");

        assert_eq!(files_record.files.len(), 2, "chennai should have 2 files");

        for file_name in files_record.files.keys() {
            assert!(
                file_name.contains("6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc"),
                "File should contain chennai UUID: {}",
                file_name
            );
            assert!(file_name.ends_with(".parquet"), "Should be parquet file");
        }
    }

}
