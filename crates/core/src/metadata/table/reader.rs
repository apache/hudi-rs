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

//! Reads a file slice of the metadata table.
//!
//! The metadata table is a Hudi table whose base files are HFile rather than
//! Parquet, and whose log blocks carry HFile records. That makes it a different
//! read from a regular table's, sharing only the file-slice shape — which is
//! why it lives here rather than alongside the reader for regular tables.

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::read::HudiReadConfig;
use crate::config::table::HudiTableConfig;
use crate::error::CoreError;
use crate::error::CoreError::ReadFileSliceError;
use crate::file_group::file_slice::FileSlice;
use crate::file_group::log_file::scanner::{LogFileScanner, ScanResult};
use crate::hfile::{HFileReader, HFileRecord};
use crate::metadata::merger::FilesPartitionMerger;
use crate::metadata::table_record::FilesPartitionRecord;
use crate::storage::Storage;
use crate::timeline::selector::InstantRange;
use std::collections::HashMap;
use std::sync::Arc;

/// Reads records out of the metadata table's `files` partition.
pub(crate) struct MetadataTableFileGroupReader {
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
}

impl MetadataTableFileGroupReader {
    pub(crate) fn new(hudi_configs: Arc<HudiConfigs>, storage: Arc<Storage>) -> Self {
        Self {
            hudi_configs,
            storage,
        }
    }

    /// Read records from metadata table files partition.
    ///
    /// # Arguments
    /// * `file_slice` - The file slice to read from
    /// * `keys` - Only read records with these keys. If empty, reads all records.
    ///
    /// # Returns
    /// HashMap containing the requested keys (or all keys if `keys` is empty).
    pub(crate) async fn read_files_partition(
        &self,
        file_slice: &FileSlice,
        keys: &[&str],
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        let base_file_path = file_slice.base_file_relative_path()?;
        let log_file_paths: Vec<String> = if file_slice.has_log_file() {
            file_slice
                .log_files
                .iter()
                .map(|log_file| file_slice.log_file_relative_path(log_file))
                .collect::<Result<Vec<String>>>()?
        } else {
            vec![]
        };

        // Open HFile
        let mut hfile_reader = HFileReader::open(&self.storage, &base_file_path)
            .await
            .map_err(|e| {
                ReadFileSliceError(format!(
                    "Failed to read metadata table base file {base_file_path}: {e:?}"
                ))
            })?;

        // Get Avro schema from HFile
        let schema = hfile_reader
            .get_avro_schema()
            .map_err(|e| ReadFileSliceError(format!("Failed to get Avro schema: {e:?}")))?
            .ok_or_else(|| ReadFileSliceError("No Avro schema found in HFile".to_string()))?
            .clone();

        let hfile_keys: Vec<&str> = if keys.is_empty() {
            vec![]
        } else {
            let mut sorted = keys.to_vec();
            sorted.sort();
            sorted
        };

        let base_records: Vec<HFileRecord> = if hfile_keys.is_empty() {
            hfile_reader.collect_records().map_err(|e| {
                ReadFileSliceError(format!("Failed to collect HFile records: {e:?}"))
            })?
        } else {
            hfile_reader
                .lookup_records(&hfile_keys)
                .map_err(|e| ReadFileSliceError(format!("Failed to lookup HFile records: {e:?}")))?
                .into_iter()
                .filter_map(|(_, r)| r)
                .collect()
        };

        let log_records = if log_file_paths.is_empty() {
            vec![]
        } else {
            let instant_range = self.create_instant_range_for_log_file_scan()?;
            let scan_result = LogFileScanner::new(self.hudi_configs.clone(), self.storage.clone())
                .scan(log_file_paths, &instant_range)
                .await?;

            match scan_result {
                ScanResult::HFileRecords(records) => records,
                ScanResult::Empty => vec![],
                ScanResult::RecordBatches(_) => {
                    return Err(CoreError::LogBlockError(
                        "Unexpected RecordBatches in metadata table log file".to_string(),
                    ));
                }
            }
        };

        let merger = FilesPartitionMerger::new(schema);
        merger.merge_for_keys(&base_records, &log_records, &hfile_keys)
    }

    /// The window bounding which log instants this read admits.
    ///
    /// Same bounds the regular reader applies; duplicated rather than shared so
    /// the two readers do not depend on each other.
    fn create_instant_range_for_log_file_scan(&self) -> Result<InstantRange> {
        let timezone = self
            .hudi_configs
            .get_or_default(HudiTableConfig::TimelineTimezone)
            .into();
        let start_timestamp = self
            .hudi_configs
            .try_get(HudiReadConfig::StartTimestamp)?
            .map(|v| -> String { v.into() });
        let end_timestamp = self
            .hudi_configs
            .try_get(HudiReadConfig::EndTimestamp)?
            .map(|v| -> String { v.into() });
        Ok(InstantRange::new(
            timezone,
            start_timestamp,
            end_timestamp,
            false,
            true,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig;
    use crate::file_group::file_slice::FileSlice;
    use std::fs::canonicalize;
    use std::path::PathBuf;
    use url::Url;

    /// Initial HFile base file for the files partition (all zeros timestamp).
    const METADATA_TABLE_FILES_BASE_FILE: &str =
        "files/files-0000-0_0-955-2690_00000000000000000.hfile";

    /// Log files for the V8Trips8I3U1D test table's files partition.
    const METADATA_TABLE_FILES_LOG_FILES: &[&str] = &[
        "files/.files-0000-0_20251220210108078.log.1_10-999-2838",
        "files/.files-0000-0_20251220210123755.log.1_3-1032-2950",
        "files/.files-0000-0_20251220210125441.log.1_5-1057-3024",
        "files/.files-0000-0_20251220210127080.log.1_3-1082-3100",
        "files/.files-0000-0_20251220210128625.log.1_5-1107-3174",
        "files/.files-0000-0_20251220210129235.log.1_3-1118-3220",
        "files/.files-0000-0_20251220210130911.log.1_3-1149-3338",
    ];

    /// The metadata table that lives inside a regular table fixture.
    fn get_metadata_table_base_uri() -> String {
        use hudi_test::QuickstartTripsTable;
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let metadata_table_path = PathBuf::from(table_path).join(".hoodie").join("metadata");
        let url = Url::from_file_path(canonicalize(&metadata_table_path).unwrap()).unwrap();
        url.as_ref().to_string()
    }

    /// Build the reader directly, without resolving options from storage.
    fn create_metadata_table_reader() -> Result<MetadataTableFileGroupReader> {
        let metadata_table_uri = get_metadata_table_base_uri();
        let hudi_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath,
            metadata_table_uri.as_str(),
        )]));
        let storage = Storage::new(Arc::new(HashMap::new()), hudi_configs.clone())?;
        Ok(MetadataTableFileGroupReader::new(hudi_configs, storage))
    }

    fn create_test_file_slice() -> Result<FileSlice> {
        use crate::file_group::FileGroup;

        let mut fg = FileGroup::new("files-0000-0".to_string(), "files".to_string());
        let base_file_name = METADATA_TABLE_FILES_BASE_FILE
            .strip_prefix("files/")
            .unwrap();
        fg.add_base_file_from_name(base_file_name)?;
        let log_file_names: Vec<_> = METADATA_TABLE_FILES_LOG_FILES
            .iter()
            .map(|s| s.strip_prefix("files/").unwrap())
            .collect();
        fg.add_log_files_from_names(log_file_names)?;

        Ok(fg
            .get_file_slice_as_of("99999999999999999")
            .expect("Should have file slice")
            .clone())
    }

    #[tokio::test]
    async fn test_read_metadata_table_files_partition() -> Result<()> {
        use crate::metadata::table_record::{FilesPartitionRecord, MetadataRecordType};

        let reader = create_metadata_table_reader()?;
        let file_slice = create_test_file_slice()?;

        // Test 1: Read all records (empty keys)
        let all_records = reader.read_files_partition(&file_slice, &[]).await?;

        // Should have 4 keys after merging
        assert_eq!(
            all_records.len(),
            4,
            "Should have 4 partition keys after merge"
        );

        // Validate all partition keys have correct record types
        for (key, record) in &all_records {
            if key == FilesPartitionRecord::ALL_PARTITIONS_KEY {
                assert_eq!(record.record_type, MetadataRecordType::AllPartitions);
            } else {
                assert_eq!(record.record_type, MetadataRecordType::Files);
            }
        }

        // Validate chennai partition has files
        let chennai = all_records.get("city=chennai").unwrap();
        assert!(
            chennai.active_file_names().len() >= 2,
            "Chennai should have at least 2 active files"
        );
        assert!(chennai.total_size() > 0, "Total size should be > 0");

        // Test 2: Read specific keys
        let keys = vec![FilesPartitionRecord::ALL_PARTITIONS_KEY, "city=chennai"];
        let filtered_records = reader.read_files_partition(&file_slice, &keys).await?;

        // Should only contain the requested keys
        assert_eq!(filtered_records.len(), 2);
        assert!(filtered_records.contains_key(FilesPartitionRecord::ALL_PARTITIONS_KEY));
        assert!(filtered_records.contains_key("city=chennai"));
        assert!(!filtered_records.contains_key("city=san_francisco"));
        assert!(!filtered_records.contains_key("city=sao_paulo"));

        Ok(())
    }
}
