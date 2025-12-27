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
//! Merger for metadata table records.
//!
//! This module provides functionality to merge HFile records from base files
//! and log files for the metadata table's files partition.

use crate::hfile::HFileRecord;
use crate::metadata::table_record::{
    decode_files_partition_record_with_schema, FilesPartitionRecord, HoodieMetadataFileInfo,
};
use crate::Result;
use apache_avro::Schema as AvroSchema;
use std::collections::HashMap;

/// Merger for files partition records from the metadata table.
///
/// This struct handles merging HFile records from base files (HFiles) and
/// log files according to Hudi's metadata table merge semantics:
///
/// - Records are merged by key (partition path or "__all_partitions__")
/// - For each key, the `filesystemMetadata` maps are merged:
///   - Deletion (isDeleted=true) removes existing entries
///   - Non-deletion uses max(size) for the same file
///   - Tombstones persist only if no prior entry exists to cancel
///
/// # Example
///
/// ```ignore
/// let merger = FilesPartitionMerger::new(schema);
/// let merged = merger.merge(&base_records, &log_records)?;
///
/// // Get active files for a partition
/// let chennai_files = merged.get("city=chennai")
///     .map(|r| r.active_file_names())
///     .unwrap_or_default();
/// ```
pub struct FilesPartitionMerger {
    schema: AvroSchema,
}

impl FilesPartitionMerger {
    /// Create a new merger with the given Avro schema.
    ///
    /// The schema should be obtained from an HFile's file info ("schema" key).
    pub fn new(schema: AvroSchema) -> Self {
        Self { schema }
    }

    /// Merge base HFile records with log file records.
    ///
    /// Records are processed in order:
    /// 1. All base records are decoded and added to the merged state
    /// 2. Log records are merged on top, applying the merge semantics
    ///
    /// # Arguments
    /// * `base_records` - Records from the base HFile (may be empty)
    /// * `log_records` - Records from log files, in chronological order
    ///
    /// # Returns
    /// A HashMap mapping record keys to merged `FilesPartitionRecord`s.
    /// The result contains only the final merged state for each key.
    pub fn merge(
        &self,
        base_records: &[HFileRecord],
        log_records: &[HFileRecord],
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        let mut merged: HashMap<String, FilesPartitionRecord> = HashMap::new();

        // First, decode and add all base records
        for record in base_records {
            let decoded = self.decode_record(record)?;
            merged.insert(decoded.key.clone(), decoded);
        }

        // Then, merge log records in order
        for record in log_records {
            let decoded = self.decode_record(record)?;
            match merged.get_mut(&decoded.key) {
                Some(existing) => {
                    self.merge_files_partition_records(existing, &decoded);
                }
                None => {
                    merged.insert(decoded.key.clone(), decoded);
                }
            }
        }

        Ok(merged)
    }

    /// Merge records, optionally filtering to specific keys.
    ///
    /// When `keys` is empty, processes all records (same as `merge()`).
    /// When `keys` is non-empty, only processes records matching those keys,
    /// which is useful for efficient partition pruning.
    ///
    /// # Arguments
    /// * `base_records` - Records from the base HFile
    /// * `log_records` - Records from log files
    /// * `keys` - Only process records with these keys. If empty, process all.
    ///
    /// # Returns
    /// A HashMap containing the requested keys (or all if keys is empty).
    pub fn merge_for_keys(
        &self,
        base_records: &[HFileRecord],
        log_records: &[HFileRecord],
        keys: &[&str],
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        // When keys is empty, process all records
        if keys.is_empty() {
            return self.merge(base_records, log_records);
        }

        let key_set: std::collections::HashSet<&str> = keys.iter().copied().collect();
        let mut merged: HashMap<String, FilesPartitionRecord> = HashMap::new();

        // Process base records, filtering by key
        for record in base_records {
            if let Some(key_str) = record.key_as_str() {
                if key_set.contains(key_str) {
                    let decoded = self.decode_record(record)?;
                    merged.insert(decoded.key.clone(), decoded);
                }
            }
        }

        // Process log records, filtering by key
        for record in log_records {
            if let Some(key_str) = record.key_as_str() {
                if key_set.contains(key_str) {
                    let decoded = self.decode_record(record)?;
                    match merged.get_mut(&decoded.key) {
                        Some(existing) => {
                            self.merge_files_partition_records(existing, &decoded);
                        }
                        None => {
                            merged.insert(decoded.key.clone(), decoded);
                        }
                    }
                }
            }
        }

        Ok(merged)
    }

    /// Decode an HFile record using the schema.
    fn decode_record(&self, record: &HFileRecord) -> Result<FilesPartitionRecord> {
        decode_files_partition_record_with_schema(record, &self.schema)
    }

    /// Merge a newer record into an existing record.
    ///
    /// This implements Hudi's metadata table merge semantics:
    /// - For each entry in newer.files:
    ///   - If isDeleted=true and old entry exists (not deleted): remove from result
    ///   - If isDeleted=true and old entry is also deleted: keep tombstone
    ///   - If isDeleted=false and old entry exists: keep max(size), mark not deleted
    ///   - If entry doesn't exist in old: add it (file or tombstone)
    fn merge_files_partition_records(
        &self,
        existing: &mut FilesPartitionRecord,
        newer: &FilesPartitionRecord,
    ) {
        for (name, new_info) in &newer.files {
            match existing.files.get(name) {
                Some(old_info) => {
                    if new_info.is_deleted {
                        if old_info.is_deleted {
                            // Both are tombstones: keep the newer one
                            existing.files.insert(name.clone(), new_info.clone());
                        } else {
                            // Deletion cancels existing entry: remove from result
                            existing.files.remove(name);
                        }
                    } else {
                        // Non-deletion: keep max size, mark as not deleted
                        existing.files.insert(
                            name.clone(),
                            HoodieMetadataFileInfo::new(
                                name.clone(),
                                old_info.size.max(new_info.size),
                                false,
                            ),
                        );
                    }
                }
                None => {
                    // New entry (could be file or tombstone)
                    existing.files.insert(name.clone(), new_info.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hfile::HFileReader;
    use crate::metadata::table_record::{FilesPartitionRecord, MetadataRecordType};
    use hudi_test::QuickstartTripsTable;
    use std::path::PathBuf;

    /// Get the metadata table directory.
    fn metadata_table_dir() -> PathBuf {
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        PathBuf::from(table_path).join(".hoodie").join("metadata")
    }

    /// Get the files partition directory.
    fn files_partition_dir() -> PathBuf {
        metadata_table_dir().join("files")
    }

    /// Get the Avro schema from an HFile.
    fn get_avro_schema_from_hfile() -> AvroSchema {
        let dir = files_partition_dir();
        let mut hfiles: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "hfile")
                    .unwrap_or(false)
            })
            .collect();
        hfiles.sort_by_key(|e| e.file_name());

        let hfile_path = hfiles.last().expect("No HFile found").path();
        let bytes = std::fs::read(&hfile_path).expect("Failed to read HFile");
        let reader = HFileReader::new(bytes).expect("Failed to create HFileReader");
        reader
            .get_avro_schema()
            .expect("Failed to get schema")
            .expect("No schema in HFile")
            .clone()
    }

    #[test]
    fn test_merge_files_partition_records_basic() {
        // Create test data
        let mut old_files = HashMap::new();
        old_files.insert(
            "file1.parquet".to_string(),
            HoodieMetadataFileInfo::new("file1.parquet".to_string(), 1000, false),
        );
        old_files.insert(
            "file2.parquet".to_string(),
            HoodieMetadataFileInfo::new("file2.parquet".to_string(), 2000, false),
        );

        let mut existing = FilesPartitionRecord {
            key: "partition".to_string(),
            record_type: MetadataRecordType::Files,
            files: old_files,
        };

        // Newer record: update file1 size, delete file2, add file3
        let mut new_files = HashMap::new();
        new_files.insert(
            "file1.parquet".to_string(),
            HoodieMetadataFileInfo::new("file1.parquet".to_string(), 1500, false),
        );
        new_files.insert(
            "file2.parquet".to_string(),
            HoodieMetadataFileInfo::new("file2.parquet".to_string(), 0, true), // deleted
        );
        new_files.insert(
            "file3.parquet".to_string(),
            HoodieMetadataFileInfo::new("file3.parquet".to_string(), 3000, false),
        );

        let newer = FilesPartitionRecord {
            key: "partition".to_string(),
            record_type: MetadataRecordType::Files,
            files: new_files,
        };

        let schema = get_avro_schema_from_hfile();
        let merger = FilesPartitionMerger::new(schema);
        merger.merge_files_partition_records(&mut existing, &newer);

        // Verify results
        assert_eq!(existing.files.len(), 2); // file2 was deleted
        assert!(existing.files.contains_key("file1.parquet"));
        assert!(!existing.files.contains_key("file2.parquet")); // deleted
        assert!(existing.files.contains_key("file3.parquet"));

        // file1 should have max size
        assert_eq!(existing.files.get("file1.parquet").unwrap().size, 1500);
        assert!(!existing.files.get("file1.parquet").unwrap().is_deleted);

        // file3 should be added
        assert_eq!(existing.files.get("file3.parquet").unwrap().size, 3000);
    }

    #[test]
    fn test_merge_tombstone_persists_when_no_prior_entry() {
        let schema = get_avro_schema_from_hfile();
        let merger = FilesPartitionMerger::new(schema);

        // Empty existing record
        let mut existing = FilesPartitionRecord {
            key: "partition".to_string(),
            record_type: MetadataRecordType::Files,
            files: HashMap::new(),
        };

        // Newer record with only a tombstone
        let mut new_files = HashMap::new();
        new_files.insert(
            "deleted.parquet".to_string(),
            HoodieMetadataFileInfo::new("deleted.parquet".to_string(), 0, true),
        );

        let newer = FilesPartitionRecord {
            key: "partition".to_string(),
            record_type: MetadataRecordType::Files,
            files: new_files,
        };

        merger.merge_files_partition_records(&mut existing, &newer);

        // Tombstone should persist since there was no prior entry
        assert_eq!(existing.files.len(), 1);
        assert!(existing.files.get("deleted.parquet").unwrap().is_deleted);
    }

    #[test]
    fn test_merge_deletion_removes_existing_entry() {
        let schema = get_avro_schema_from_hfile();
        let merger = FilesPartitionMerger::new(schema);

        // Existing record with a file
        let mut old_files = HashMap::new();
        old_files.insert(
            "file.parquet".to_string(),
            HoodieMetadataFileInfo::new("file.parquet".to_string(), 1000, false),
        );

        let mut existing = FilesPartitionRecord {
            key: "partition".to_string(),
            record_type: MetadataRecordType::Files,
            files: old_files,
        };

        // Delete it
        let mut new_files = HashMap::new();
        new_files.insert(
            "file.parquet".to_string(),
            HoodieMetadataFileInfo::new("file.parquet".to_string(), 0, true),
        );

        let newer = FilesPartitionRecord {
            key: "partition".to_string(),
            record_type: MetadataRecordType::Files,
            files: new_files,
        };

        merger.merge_files_partition_records(&mut existing, &newer);

        // File should be completely removed (not just marked deleted)
        assert!(existing.files.is_empty());
    }

    #[test]
    fn test_merge_max_size() {
        let schema = get_avro_schema_from_hfile();
        let merger = FilesPartitionMerger::new(schema);

        // Existing record with larger size
        let mut old_files = HashMap::new();
        old_files.insert(
            "file.parquet".to_string(),
            HoodieMetadataFileInfo::new("file.parquet".to_string(), 2000, false),
        );

        let mut existing = FilesPartitionRecord {
            key: "partition".to_string(),
            record_type: MetadataRecordType::Files,
            files: old_files,
        };

        // Newer record with smaller size (shouldn't override)
        let mut new_files = HashMap::new();
        new_files.insert(
            "file.parquet".to_string(),
            HoodieMetadataFileInfo::new("file.parquet".to_string(), 1000, false),
        );

        let newer = FilesPartitionRecord {
            key: "partition".to_string(),
            record_type: MetadataRecordType::Files,
            files: new_files,
        };

        merger.merge_files_partition_records(&mut existing, &newer);

        // Should keep max size
        assert_eq!(existing.files.get("file.parquet").unwrap().size, 2000);
    }

    /// Log files for the V8Trips8I3U1D test table's files partition.
    const FILES_PARTITION_LOG_FILES: &[&str] = &[
        "files/.files-0000-0_20251220210108078.log.1_10-999-2838",
        "files/.files-0000-0_20251220210123755.log.1_3-1032-2950",
        "files/.files-0000-0_20251220210125441.log.1_5-1057-3024",
        "files/.files-0000-0_20251220210127080.log.1_3-1082-3100",
        "files/.files-0000-0_20251220210128625.log.1_5-1107-3174",
        "files/.files-0000-0_20251220210129235.log.1_3-1118-3220",
        "files/.files-0000-0_20251220210130911.log.1_3-1149-3338",
    ];

    // UUIDs for each partition's files
    const CHENNAI_UUID: &str = "6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc";
    const SAN_FRANCISCO_UUID: &str = "036ded81-9ed4-479f-bcea-7145dfa0079b";
    const SAO_PAULO_UUID: &str = "8aa68f7e-afd6-4c94-b86c-8a886552e08d";

    #[tokio::test]
    async fn test_merge_log_files_from_scanner() -> crate::Result<()> {
        use crate::config::HudiConfigs;
        use crate::file_group::log_file::scanner::{LogFileScanner, ScanResult};
        use crate::storage::util::parse_uri;
        use crate::storage::Storage;
        use crate::timeline::selector::InstantRange;
        use std::sync::Arc;

        let metadata_dir = metadata_table_dir();
        let schema = get_avro_schema_from_hfile();

        // Set up scanner
        let dir_url = parse_uri(metadata_dir.to_str().unwrap())?;
        let storage = Storage::new_with_base_url(dir_url)?;
        let hudi_configs = Arc::new(HudiConfigs::empty());
        let scanner = LogFileScanner::new(hudi_configs, Arc::clone(&storage));
        let instant_range = InstantRange::up_to("99991231235959999", "utc");

        // Scan all log files
        let all_paths: Vec<String> = FILES_PARTITION_LOG_FILES
            .iter()
            .map(|s| s.to_string())
            .collect();
        let result = scanner.scan(all_paths, &instant_range).await?;

        let log_records = match result {
            ScanResult::HFileRecords(r) => r,
            _ => panic!("Expected HFileRecords from scanner"),
        };

        assert_eq!(log_records.len(), 18, "Should have 18 log records");

        // Merge with empty base records (log-only merge)
        let merger = FilesPartitionMerger::new(schema);
        let merged = merger.merge(&[], &log_records)?;

        // Validate merged results
        // Should have 4 keys: __all_partitions__, chennai, san_francisco, sao_paulo
        assert_eq!(merged.len(), 4, "Should have 4 merged partition keys");

        // Validate __all_partitions__ - should list all 3 city partitions
        let all_partitions = merged
            .get(FilesPartitionRecord::ALL_PARTITIONS_KEY)
            .expect("Should have __all_partitions__");
        assert_eq!(
            all_partitions.record_type,
            MetadataRecordType::AllPartitions
        );
        assert_eq!(
            all_partitions.files.len(),
            3,
            "__all_partitions__ should have 3 partition entries"
        );
        assert!(all_partitions.files.contains_key("city=chennai"));
        assert!(all_partitions.files.contains_key("city=san_francisco"));
        assert!(all_partitions.files.contains_key("city=sao_paulo"));

        // Validate chennai partition
        // Log files contain: 2 parquet files + 2 log files = 4 active files
        let chennai = merged.get("city=chennai").expect("Should have chennai");
        assert_eq!(chennai.record_type, MetadataRecordType::Files);
        assert_eq!(chennai.files.len(), 4, "Chennai should have 4 files");

        let chennai_parquets: Vec<_> = chennai
            .files
            .keys()
            .filter(|n| n.ends_with(".parquet"))
            .collect();
        assert_eq!(
            chennai_parquets.len(),
            2,
            "Chennai should have 2 parquet files"
        );

        let chennai_logs: Vec<_> = chennai
            .files
            .keys()
            .filter(|n| n.contains(".log."))
            .collect();
        assert_eq!(chennai_logs.len(), 2, "Chennai should have 2 log files");

        for (name, info) in &chennai.files {
            assert!(
                name.contains(CHENNAI_UUID),
                "File should contain Chennai UUID: {}",
                name
            );
            assert!(!info.is_deleted, "File should not be deleted");
            assert!(info.size > 0, "File size should be > 0");
        }

        // Validate san_francisco partition
        // Log files contain: 2 parquet files + 1 log file = 3 active files
        let sf = merged
            .get("city=san_francisco")
            .expect("Should have san_francisco");
        assert_eq!(sf.record_type, MetadataRecordType::Files);
        assert_eq!(sf.files.len(), 3, "San Francisco should have 3 files");

        let sf_parquets: Vec<_> = sf
            .files
            .keys()
            .filter(|n| n.ends_with(".parquet"))
            .collect();
        assert_eq!(sf_parquets.len(), 2, "SF should have 2 parquet files");

        let sf_logs: Vec<_> = sf.files.keys().filter(|n| n.contains(".log.")).collect();
        assert_eq!(sf_logs.len(), 1, "SF should have 1 log file");

        for (name, info) in &sf.files {
            assert!(
                name.contains(SAN_FRANCISCO_UUID),
                "File should contain SF UUID: {}",
                name
            );
            assert!(!info.is_deleted);
        }

        // Validate sao_paulo partition
        // Log files contain: 2 parquet files + 2 log files = 4 active files
        let sp = merged.get("city=sao_paulo").expect("Should have sao_paulo");
        assert_eq!(sp.record_type, MetadataRecordType::Files);
        assert_eq!(sp.files.len(), 4, "Sao Paulo should have 4 files");

        let sp_parquets: Vec<_> = sp
            .files
            .keys()
            .filter(|n| n.ends_with(".parquet"))
            .collect();
        assert_eq!(sp_parquets.len(), 2, "SP should have 2 parquet files");

        let sp_logs: Vec<_> = sp.files.keys().filter(|n| n.contains(".log.")).collect();
        assert_eq!(sp_logs.len(), 2, "SP should have 2 log files");

        for (name, info) in &sp.files {
            assert!(
                name.contains(SAO_PAULO_UUID),
                "File should contain SP UUID: {}",
                name
            );
            assert!(!info.is_deleted);
        }

        // Validate total active files using helper method
        let total_active: usize = merged
            .values()
            .filter(|r| r.record_type == MetadataRecordType::Files)
            .map(|r| r.active_file_names().len())
            .sum();
        assert_eq!(total_active, 11, "Total active files should be 11 (4+3+4)");

        Ok(())
    }

    #[test]
    fn test_merge_for_keys_filters_to_requested_keys() -> crate::Result<()> {
        // Get base records from the HFile
        let dir = files_partition_dir();
        let mut hfiles: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "hfile")
                    .unwrap_or(false)
            })
            .collect();
        hfiles.sort_by_key(|e| e.file_name());

        let hfile_path = hfiles.last().expect("No HFile found").path();
        let bytes = std::fs::read(&hfile_path).expect("Failed to read HFile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create HFileReader");
        let schema = reader
            .get_avro_schema()
            .expect("Failed to get schema")
            .expect("No schema in HFile")
            .clone();
        let base_records = reader.collect_records().expect("Failed to collect records");

        let merger = FilesPartitionMerger::new(schema);

        // Request only specific keys
        let keys = vec![FilesPartitionRecord::ALL_PARTITIONS_KEY, "city=chennai"];
        let merged = merger.merge_for_keys(&base_records, &[], &keys)?;

        // Should only contain requested keys
        assert_eq!(merged.len(), 2);
        assert!(merged.contains_key(FilesPartitionRecord::ALL_PARTITIONS_KEY));
        assert!(merged.contains_key("city=chennai"));
        assert!(!merged.contains_key("city=san_francisco"));

        Ok(())
    }
}
