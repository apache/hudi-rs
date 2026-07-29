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
//! Record-level index backed by the metadata table `record_index` partition.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::Result;
use crate::config::table::HudiTableConfig::RecordIndexEnabled;
use crate::error::CoreError;
use crate::file_group::log_file::scanner::{LogFileScanner, ScanResult};
use crate::hfile::{HFileReader, HFileRecord};
use crate::index::{HoodieIndex, HoodieKey, RecordLocation};
use crate::metadata::table::encode::decode_record_index_entry;
use crate::metadata::table::records::MetadataPartitionType;
use crate::storage::Storage;
use crate::table::Table;
use crate::timeline::selector::InstantRange;
use crate::write::metadata::epoch_millis_to_instant;

const METADATA_BASE: &str = ".hoodie/metadata";

/// Metadata-table record-level index (global: key = record key).
#[derive(Clone, Debug, Default)]
pub struct RecordIndex;

impl HoodieIndex for RecordIndex {
    async fn tag_location(
        &self,
        table: &Table,
        keys: &[HoodieKey],
    ) -> Result<HashMap<HoodieKey, Option<RecordLocation>>> {
        let mut locations = keys
            .iter()
            .cloned()
            .map(|key| (key, None))
            .collect::<HashMap<_, _>>();
        if keys.is_empty() {
            return Ok(locations);
        }

        let storage = table.file_system_view.storage.clone();
        let merged = load_record_index_map(table, storage).await?;
        for key in keys {
            if let Some(entry) = merged.get(&key.record_key) {
                locations.insert(
                    key.clone(),
                    Some(RecordLocation {
                        file_id: entry.file_id.clone(),
                        instant_time: epoch_millis_to_instant(entry.instant_time_millis),
                        partition_path: entry.partition_path.clone(),
                    }),
                );
            }
        }
        Ok(locations)
    }
}

struct LoadedLocation {
    file_id: String,
    partition_path: String,
    instant_time_millis: i64,
}

async fn load_record_index_map(
    table: &Table,
    storage: Arc<Storage>,
) -> Result<HashMap<String, LoadedLocation>> {
    let partition = MetadataPartitionType::RecordIndex.partition_name();
    let dir = format!("{METADATA_BASE}/{partition}");
    let listed = match storage.list_files(Some(&dir)).await {
        Ok(files) => files,
        Err(_) => return Ok(HashMap::new()),
    };

    let mut base_paths = Vec::new();
    let mut log_paths = Vec::new();
    for file in listed {
        let name = file.name;
        if name.ends_with(".hfile") && name.starts_with("record-index-") {
            let relative = PathBuf::from(&dir).join(&name);
            base_paths.push(relative.to_string_lossy().to_string());
        } else if name.starts_with('.') && name.contains(".log.") {
            let relative = PathBuf::from(&dir).join(&name);
            log_paths.push(relative.to_string_lossy().to_string());
        }
    }
    base_paths.sort();
    log_paths.sort();

    let mut merged: HashMap<String, Option<LoadedLocation>> = HashMap::new();
    for base in base_paths {
        let mut reader = HFileReader::open(storage.as_ref(), &base)
            .await
            .map_err(|e| CoreError::HFile(format!("failed to open record_index base: {e:?}")))?;
        let records = reader
            .collect_records()
            .map_err(|e| CoreError::HFile(format!("failed to read record_index base: {e:?}")))?;
        apply_hfile_records(&mut merged, &records)?;
    }
    if !log_paths.is_empty() {
        let scanner = LogFileScanner::new(table.hudi_configs.clone(), storage);
        let range = InstantRange::up_to("99991231235959999", "UTC");
        match scanner.scan(log_paths, &range).await? {
            ScanResult::HFileRecords(records) => apply_hfile_records(&mut merged, &records)?,
            ScanResult::Empty => {}
            ScanResult::RecordBatches(_) => {
                return Err(CoreError::MetadataTable(
                    "record_index logs must contain HFile data blocks".to_string(),
                ));
            }
        }
    }

    Ok(merged
        .into_iter()
        .filter_map(|(key, location)| location.map(|loc| (key, loc)))
        .collect())
}

fn apply_hfile_records(
    merged: &mut HashMap<String, Option<LoadedLocation>>,
    records: &[HFileRecord],
) -> Result<()> {
    for record in records {
        let key = record
            .key_as_str()
            .ok_or_else(|| CoreError::MetadataTable("record_index key must be utf8".to_string()))?
            .to_string();
        match decode_record_index_entry(&key, record.value())? {
            Some(entry) => {
                merged.insert(
                    key,
                    Some(LoadedLocation {
                        file_id: entry.file_id,
                        partition_path: entry.partition_path,
                        instant_time_millis: entry.instant_time_millis,
                    }),
                );
            }
            None => {
                merged.insert(key, None);
            }
        }
    }
    Ok(())
}

/// Whether the table has record index enabled.
///
/// Prefer the Java table property `hoodie.table.metadata.partitions` containing
/// `record_index`. The write-side key `hoodie.metadata.record.index.enable` is
/// also honored when present (runtime option), but is not required on disk.
pub fn is_record_index_enabled(table: &Table) -> bool {
    use crate::config::table::HudiTableConfig::MetadataTablePartitions;
    let from_partitions: Vec<String> = table
        .hudi_configs
        .get_or_default(MetadataTablePartitions)
        .into();
    if from_partitions.iter().any(|p| p == "record_index") {
        return true;
    }
    table
        .hudi_configs
        .get_or_default(RecordIndexEnabled)
        .into()
}
