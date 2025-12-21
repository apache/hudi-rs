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
use crate::config::HudiConfigs;
use crate::file_group::log_file::log_block::{BlockType, LogBlock, LogBlockContent};
use crate::file_group::log_file::reader::LogFileReader;
use crate::file_group::record_batches::RecordBatches;
use crate::hfile::HFileRecord;
use crate::storage::Storage;
use crate::timeline::selector::InstantRange;
use crate::Result;
use std::collections::HashSet;
use std::sync::Arc;

/// Result of scanning log files.
///
/// The scanner auto-detects the block type and returns the appropriate variant:
/// - `RecordBatches`: Arrow record batches from Avro/Parquet data blocks (regular table reads)
/// - `HFileRecords`: HFile key-value records from HFile data blocks (metadata table reads)
/// - `Empty`: No data blocks found (only command blocks or empty files)
#[derive(Debug)]
pub enum ScanResult {
    /// Arrow RecordBatches from Avro/Parquet data blocks.
    /// Used for regular table reads.
    RecordBatches(RecordBatches),
    /// HFile key-value records from HFile data blocks.
    /// Used for metadata table reads.
    /// Note: Records are NOT merged by key - caller is responsible for merging.
    HFileRecords(Vec<HFileRecord>),
    /// No data blocks found.
    Empty,
}

impl ScanResult {
    /// Returns true if the result contains Arrow record batches.
    #[must_use]
    pub fn is_record_batches(&self) -> bool {
        matches!(self, ScanResult::RecordBatches(_))
    }

    /// Returns true if the result contains HFile records.
    #[must_use]
    pub fn is_hfile_records(&self) -> bool {
        matches!(self, ScanResult::HFileRecords(_))
    }

    /// Returns true if the result is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, ScanResult::Empty)
    }

    /// Unwrap as RecordBatches, panicking if not that variant.
    #[must_use]
    pub fn unwrap_record_batches(self) -> RecordBatches {
        match self {
            ScanResult::RecordBatches(batches) => batches,
            _ => panic!("called unwrap_record_batches on non-RecordBatches variant"),
        }
    }

    /// Unwrap as HFileRecords, panicking if not that variant.
    #[must_use]
    pub fn unwrap_hfile_records(self) -> Vec<HFileRecord> {
        match self {
            ScanResult::HFileRecords(records) => records,
            _ => panic!("called unwrap_hfile_records on non-HFileRecords variant"),
        }
    }
}

/// Internal content type for detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContentType {
    /// Avro/Parquet/Delete blocks -> RecordBatches
    Records,
    /// HFile blocks -> HFileRecords
    HFile,
    /// No data blocks
    Empty,
}

/// Result of collecting blocks from multiple log files.
struct CollectedBlocks {
    /// All blocks organized by log file.
    all_blocks: Vec<Vec<LogBlock>>,
    /// Instant times that have been rolled back.
    rollback_targets: HashSet<String>,
}

impl CollectedBlocks {
    /// Iterate over all blocks, filtering out rollback blocks and rolled-back data.
    fn iter_valid_blocks(self) -> impl Iterator<Item = LogBlock> {
        let rollback_targets = self.rollback_targets;
        self.all_blocks.into_iter().flatten().filter(move |block| {
            // Skip rollback command blocks (they have no content)
            if block.is_rollback_block() {
                return false;
            }
            // Skip blocks whose instant time was rolled back
            match block.instant_time() {
                Ok(instant) => !rollback_targets.contains(instant),
                // If we can't get the instant time, include the block
                // and let downstream handle the error
                Err(_) => true,
            }
        })
    }
}

#[derive(Debug)]
pub struct LogFileScanner {
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
}

impl LogFileScanner {
    pub fn new(hudi_configs: Arc<HudiConfigs>, storage: Arc<Storage>) -> Self {
        Self {
            hudi_configs,
            storage,
        }
    }

    /// Read all blocks from multiple log files and collect rollback targets.
    async fn collect_blocks(
        &self,
        relative_paths: Vec<String>,
        instant_range: &InstantRange,
    ) -> Result<CollectedBlocks> {
        let mut all_blocks: Vec<Vec<LogBlock>> = Vec::with_capacity(relative_paths.len());
        let mut rollback_targets: HashSet<String> = HashSet::new();

        for path in relative_paths {
            let mut reader =
                LogFileReader::new(self.hudi_configs.clone(), self.storage.clone(), &path).await?;
            let blocks = reader.read_all_blocks(instant_range)?;

            // Collect rollback targets from command blocks
            for block in &blocks {
                if block.is_rollback_block() {
                    rollback_targets.insert(block.target_instant_time()?.to_string());
                }
            }

            all_blocks.push(blocks);
        }

        Ok(CollectedBlocks {
            all_blocks,
            rollback_targets,
        })
    }

    /// Detect the content type from collected blocks.
    ///
    /// Returns an error if the log files contain mixed block types (both Arrow-based
    /// and HFile blocks), which is invalid.
    fn detect_content_type(&self, collected: &CollectedBlocks) -> Result<ContentType> {
        let mut has_record_blocks = false;
        let mut has_hfile_blocks = false;

        for blocks in &collected.all_blocks {
            for block in blocks {
                match block.block_type {
                    BlockType::AvroData
                    | BlockType::ParquetData
                    | BlockType::Delete
                    | BlockType::CdcData => {
                        has_record_blocks = true;
                    }
                    BlockType::HfileData => {
                        has_hfile_blocks = true;
                    }
                    BlockType::Command | BlockType::Corrupted => {
                        // Command and corrupted blocks don't affect content type
                    }
                }

                // Check for mixed types as early as possible
                if has_record_blocks && has_hfile_blocks {
                    return Err(crate::error::CoreError::LogBlockError(
                        "Log files contain mixed block types (both Arrow-based and HFile blocks), which is invalid".into(),
                    ));
                }
            }
        }

        if has_hfile_blocks {
            Ok(ContentType::HFile)
        } else if has_record_blocks {
            Ok(ContentType::Records)
        } else {
            Ok(ContentType::Empty)
        }
    }

    /// Collect Arrow record batches from blocks.
    fn collect_record_batches(&self, collected: CollectedBlocks) -> Result<ScanResult> {
        // Pre-count batches for capacity
        let mut num_data_batches = 0;
        let mut num_delete_batches = 0;
        for blocks in &collected.all_blocks {
            for block in blocks {
                match block.block_type {
                    BlockType::AvroData | BlockType::ParquetData | BlockType::CdcData => {
                        if let Some(records) = block.content.as_records() {
                            num_data_batches += records.num_data_batches();
                        }
                    }
                    BlockType::Delete => {
                        if let Some(records) = block.content.as_records() {
                            num_delete_batches += records.num_delete_batches();
                        }
                    }
                    _ => {}
                }
            }
        }

        // Collect valid record batches
        let mut batches = RecordBatches::new_with_capacity(num_data_batches, num_delete_batches);
        for block in collected.iter_valid_blocks() {
            if let LogBlockContent::Records(records) = block.content {
                batches.extend(records);
            }
        }

        Ok(ScanResult::RecordBatches(batches))
    }

    /// Collect HFile records from blocks.
    fn collect_hfile_records(&self, collected: CollectedBlocks) -> Result<ScanResult> {
        // Pre-count records for capacity
        let mut total_records = 0;
        for blocks in &collected.all_blocks {
            for block in blocks {
                if block.block_type == BlockType::HfileData {
                    if let Some(records) = block.content.as_hfile_records() {
                        total_records += records.len();
                    }
                }
            }
        }

        // Collect valid HFile records
        let mut records = Vec::with_capacity(total_records);
        for block in collected.iter_valid_blocks() {
            if let LogBlockContent::HFileRecords(hfile_records) = block.content {
                records.extend(hfile_records);
            }
        }

        Ok(ScanResult::HFileRecords(records))
    }

    /// Scan log files and return the appropriate content type.
    ///
    /// The scanner auto-detects the block type and returns:
    /// - `ScanResult::RecordBatches` for Avro/Parquet/Delete data blocks (regular table reads)
    /// - `ScanResult::HFileRecords` for HFile data blocks (metadata table reads)
    /// - `ScanResult::Empty` if no data blocks found
    ///
    /// # Errors
    ///
    /// Returns an error if the log files contain mixed block types (both Arrow-based
    /// and HFile blocks), which indicates data corruption or misconfiguration.
    pub async fn scan(
        &self,
        relative_paths: Vec<String>,
        instant_range: &InstantRange,
    ) -> Result<ScanResult> {
        let collected = self.collect_blocks(relative_paths, instant_range).await?;

        // Detect content type from blocks
        let content_type = self.detect_content_type(&collected)?;

        match content_type {
            ContentType::Records => self.collect_record_batches(collected),
            ContentType::HFile => self.collect_hfile_records(collected),
            ContentType::Empty => Ok(ScanResult::Empty),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HudiConfigs;
    use crate::hfile::HFileReader;
    use crate::metadata::table_record::decode_files_partition_record_with_schema;
    use crate::storage::util::parse_uri;
    use apache_avro::Schema as AvroSchema;
    use hudi_test::QuickstartTripsTable;
    use std::path::PathBuf;

    /// Get the metadata table directory (base path for storage).
    fn metadata_table_dir() -> PathBuf {
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        PathBuf::from(table_path).join(".hoodie").join("metadata")
    }

    /// Get the files partition directory.
    fn files_partition_dir() -> PathBuf {
        metadata_table_dir().join("files")
    }

    /// Find the latest HFile in the files partition to get the Avro schema.
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

    /// Log files for the V8Trips8I3U1D test table's files partition.
    /// These are relative paths from the metadata table directory.
    /// Note: The epoch log file (with timestamp 00000000000000000) is excluded
    /// as it contains no data and has an unparseable timestamp.
    const FILES_PARTITION_LOG_FILES: &[&str] = &[
        "files/.files-0000-0_20251220210108078.log.1_10-999-2838",
        "files/.files-0000-0_20251220210123755.log.1_3-1032-2950",
        "files/.files-0000-0_20251220210125441.log.1_5-1057-3024",
        "files/.files-0000-0_20251220210127080.log.1_3-1082-3100",
        "files/.files-0000-0_20251220210128625.log.1_5-1107-3174",
        "files/.files-0000-0_20251220210129235.log.1_3-1118-3220",
        "files/.files-0000-0_20251220210130911.log.1_3-1149-3338",
    ];

    // Expected record counts per log file (index matches FILES_PARTITION_LOG_FILES)
    const EXPECTED_RECORDS_PER_FILE: &[usize] = &[4, 2, 2, 2, 2, 4, 2];

    // UUIDs for each partition's files
    const CHENNAI_UUID: &str = "6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc";
    const SAN_FRANCISCO_UUID: &str = "036ded81-9ed4-479f-bcea-7145dfa0079b";
    const SAO_PAULO_UUID: &str = "8aa68f7e-afd6-4c94-b86c-8a886552e08d";

    #[tokio::test]
    async fn test_scan_metadata_table_log_files() -> crate::Result<()> {
        use crate::metadata::table_record::MetadataRecordType;
        use std::collections::HashMap;

        let metadata_dir = metadata_table_dir();
        let schema = get_avro_schema_from_hfile();

        let dir_url = parse_uri(metadata_dir.to_str().unwrap())?;
        let storage = Storage::new_with_base_url(dir_url)?;
        let hudi_configs = Arc::new(HudiConfigs::empty());
        let scanner = LogFileScanner::new(hudi_configs, Arc::clone(&storage));
        let instant_range = InstantRange::up_to("99991231235959999", "utc");

        // Scan each log file individually and validate record counts
        let mut total_records = 0;
        for (file_idx, log_file) in FILES_PARTITION_LOG_FILES.iter().enumerate() {
            let result = scanner
                .scan(vec![log_file.to_string()], &instant_range)
                .await?;

            match result {
                ScanResult::HFileRecords(records) => {
                    assert_eq!(
                        records.len(),
                        EXPECTED_RECORDS_PER_FILE[file_idx],
                        "Log file {} should have {} records",
                        log_file,
                        EXPECTED_RECORDS_PER_FILE[file_idx]
                    );
                    total_records += records.len();
                }
                ScanResult::RecordBatches(_) => {
                    panic!("Expected HFileRecords, got RecordBatches");
                }
                ScanResult::Empty => {
                    panic!("Expected HFileRecords, got Empty for {}", log_file);
                }
            }
        }

        assert_eq!(total_records, 18, "Total records should be 18");

        // Scan all log files together and decode records
        let all_paths: Vec<String> = FILES_PARTITION_LOG_FILES
            .iter()
            .map(|s| s.to_string())
            .collect();
        let result = scanner.scan(all_paths, &instant_range).await?;

        let records = match result {
            ScanResult::HFileRecords(r) => r,
            _ => panic!("Expected HFileRecords from combined scan"),
        };

        assert_eq!(records.len(), 18, "Combined scan should return 18 records");

        // Decode all records and group by key
        let mut records_by_key: HashMap<String, Vec<_>> = HashMap::new();
        for record in &records {
            let decoded = decode_files_partition_record_with_schema(record, &schema)?;
            records_by_key
                .entry(decoded.key.clone())
                .or_default()
                .push(decoded);
        }

        // Validate __all_partitions__ records
        let all_partitions = records_by_key
            .get("__all_partitions__")
            .expect("Should have __all_partitions__ records");
        assert_eq!(
            all_partitions.len(),
            7,
            "Should have 7 __all_partitions__ records (one per log file)"
        );
        for record in all_partitions {
            assert_eq!(record.record_type, MetadataRecordType::AllPartitions);
        }

        // Validate chennai partition records
        let chennai = records_by_key
            .get("city=chennai")
            .expect("Should have city=chennai records");
        assert_eq!(chennai.len(), 4, "Chennai should have 4 records");
        for record in chennai {
            assert_eq!(record.record_type, MetadataRecordType::Files);
            for (name, info) in &record.files {
                assert!(
                    name.contains(CHENNAI_UUID),
                    "Chennai file should contain UUID: {}",
                    name
                );
                assert!(!info.is_deleted, "Files should not be deleted");
                assert!(info.size > 0, "File size should be > 0");
            }
        }

        // Validate san_francisco partition records
        let sf = records_by_key
            .get("city=san_francisco")
            .expect("Should have city=san_francisco records");
        assert_eq!(sf.len(), 3, "San Francisco should have 3 records");
        for record in sf {
            assert_eq!(record.record_type, MetadataRecordType::Files);
            for (name, info) in &record.files {
                assert!(
                    name.contains(SAN_FRANCISCO_UUID),
                    "San Francisco file should contain UUID: {}",
                    name
                );
                assert!(!info.is_deleted);
            }
        }

        // Validate sao_paulo partition records
        let sp = records_by_key
            .get("city=sao_paulo")
            .expect("Should have city=sao_paulo records");
        assert_eq!(sp.len(), 4, "Sao Paulo should have 4 records");
        for record in sp {
            assert_eq!(record.record_type, MetadataRecordType::Files);
            for (name, info) in &record.files {
                assert!(
                    name.contains(SAO_PAULO_UUID),
                    "Sao Paulo file should contain UUID: {}",
                    name
                );
                assert!(!info.is_deleted);
            }
        }

        // Validate we have exactly 4 partition keys
        assert_eq!(
            records_by_key.len(),
            4,
            "Should have 4 unique keys: __all_partitions__, chennai, san_francisco, sao_paulo"
        );

        Ok(())
    }
}
