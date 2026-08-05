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

//! Metadata table APIs for the Hudi Table.
//!
//! This module provides methods for interacting with Hudi's metadata table,
//! which stores file listings and other metadata for efficient table operations.

pub mod column_stats;
pub mod encode;
pub mod hash;
pub mod records;

pub(crate) mod reader;

use crate::config::HudiConfigs;
use crate::storage::Storage;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema;

use crate::Result;
use crate::config::table::HudiTableConfig::{
    MetadataTableEnabled, MetadataTablePartitions, PartitionFields, TableVersion,
};
use crate::error::CoreError;
use crate::expr::filter::from_str_tuples;
use crate::metadata::METADATA_TABLE_PARTITION_FIELD;
use crate::storage::util::join_url_segments;
use crate::table::ReadOptions;
use crate::table::Table;
use crate::table::file_pruner::FilePruner;
use crate::table::partition::PartitionPruner;

use records::FilesPartitionRecord;

/// Java `HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP`: MDT bootstrap instants are
/// `00000000000000` plus a millis offset and are always valid to read.
const SOLO_COMMIT_TIMESTAMP_PREFIX: &str = "00000000000000";

/// MDT instants readers may trust: completed MDT deltacommits whose requested
/// time is **completed on the data timeline**, plus bootstrap
/// (`00000000000000`-prefixed) instants.
///
/// Stricter than Java's `getValidInstantTimestamps` ("not pending on the data
/// timeline"): requiring data-timeline completion also excludes an MDT commit
/// whose data instant was never even fenced (no `.requested`/`.inflight`),
/// which the pending-based rule would wrongly trust. The MDT-only instants
/// Java's looser rule serves are MDT compactions — those are `.commit`
/// instants on the MDT timeline and never enter this `.deltacommit`-only scan.
///
/// Must be called with a DATA table's storage/configs (its storage roots both
/// timelines).
pub(crate) async fn valid_metadata_instants(
    storage: &crate::storage::Storage,
    hudi_configs: &crate::config::HudiConfigs,
) -> Result<std::collections::HashSet<String>> {
    // Data timeline: completed requested-times.
    let timeline_layout: isize = hudi_configs
        .get_or_default(crate::config::table::HudiTableConfig::TimelineLayoutVersion)
        .into();
    let data_timeline_dir = if timeline_layout >= 2 {
        let timeline_path: String = hudi_configs
            .get_or_default(crate::config::table::HudiTableConfig::TimelinePath)
            .into();
        format!("{}/{timeline_path}", crate::metadata::HUDI_METADATA_DIR)
    } else {
        crate::metadata::HUDI_METADATA_DIR.to_string()
    };
    let mut data_completed = std::collections::HashSet::new();
    for file in storage.list_files(Some(&data_timeline_dir)).await? {
        let name = file.name;
        let Some(first) = name.chars().next() else {
            continue;
        };
        if !first.is_ascii_digit() {
            continue;
        }
        if name.ends_with(".requested") || name.ends_with(".inflight") {
            continue;
        }
        let requested: String = name.chars().take_while(char::is_ascii_digit).collect();
        data_completed.insert(requested);
    }
    // Archived instants were completed before leaving the active timeline;
    // without them, archival would fence out perfectly valid MDT records.
    data_completed
        .extend(crate::write::archival::archived_instant_times(storage, &data_timeline_dir).await?);

    // MDT timeline: completed deltacommits' requested times.
    let mdt_timeline_dir = format!(
        "{}/metadata/.hoodie/timeline",
        crate::metadata::HUDI_METADATA_DIR
    );
    let mdt_files = match storage.list_files(Some(&mdt_timeline_dir)).await {
        Ok(files) => files,
        Err(crate::storage::error::StorageError::ObjectStoreError(
            object_store::Error::NotFound { .. },
        )) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    let mut valid = std::collections::HashSet::new();
    for file in mdt_files {
        let name = file.name;
        if !name.ends_with(".deltacommit") {
            continue;
        }
        let requested: String = name.chars().take_while(char::is_ascii_digit).collect();
        if requested.is_empty() {
            continue;
        }
        if requested.starts_with(SOLO_COMMIT_TIMESTAMP_PREFIX)
            || data_completed.contains(&requested)
        {
            valid.insert(requested);
        }
    }
    Ok(valid)
}

impl Table {
    /// Check if this table is a metadata table.
    ///
    /// Detection is based on the base path ending with `.hoodie/metadata`.
    pub fn is_metadata_table(&self) -> bool {
        let base_path: String = self
            .hudi_configs
            .get_or_default(crate::config::table::HudiTableConfig::BasePath)
            .into();
        crate::util::path::is_metadata_table_path(&base_path)
    }

    /// Get the list of available metadata table partitions for this table.
    ///
    /// Returns the partitions configured in [`MetadataTablePartitions`].
    pub fn get_metadata_table_partitions(&self) -> Vec<String> {
        self.hudi_configs
            .get_or_default(MetadataTablePartitions)
            .into()
    }

    /// Check if the metadata table is enabled.
    ///
    /// Returns `true` if:
    /// 1. Table version is >= 8 (metadata table support is only for v8+ tables), AND
    /// 2. Table is not a metadata table itself, AND
    /// 3. Either:
    ///    - `hoodie.metadata.enable` is explicitly true, OR
    ///    - `files` is in the configured [`MetadataTablePartitions`]
    ///
    /// # Note
    /// The metadata table is considered active when partitions are configured,
    /// even without explicit `hoodie.metadata.enable=true`. When metadata table
    /// is enabled, it must have at least the `files` partition enabled.
    pub fn is_metadata_table_enabled(&self) -> bool {
        // TODO: drop v6 support then no need to check table version here
        let table_version: isize = self
            .hudi_configs
            .get(TableVersion)
            .map(|v| v.into())
            .unwrap_or(0);

        if table_version < 8 {
            return false;
        }

        if self.is_metadata_table() {
            return false;
        }

        // Check if "files" partition is configured
        let has_files_partition = self
            .get_metadata_table_partitions()
            .contains(&FilesPartitionRecord::PARTITION_NAME.to_string());

        // Explicit check for hoodie.metadata.enable
        let metadata_explicitly_enabled: bool = self
            .hudi_configs
            .get_or_default(MetadataTableEnabled)
            .into();

        metadata_explicitly_enabled || has_files_partition
    }

    /// Create a metadata table instance for this data table.
    ///
    /// TODO: support more partitions. Only "files" is used currently.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata table cannot be created or if there are
    /// no metadata table partitions configured.
    ///
    /// # Note
    /// Must be called on a DATA table, not a METADATA table.
    pub async fn new_metadata_table(&self) -> Result<Table> {
        if self.is_metadata_table() {
            return Err(CoreError::MetadataTable(
                "Cannot create metadata table from another metadata table".to_string(),
            ));
        }

        let mdt_partitions = self.get_metadata_table_partitions();
        if mdt_partitions.is_empty() {
            return Err(CoreError::MetadataTable(
                "No metadata table partitions configured".to_string(),
            ));
        }

        let mdt_url = join_url_segments(&self.base_url(), &[".hoodie", "metadata"])?;
        Table::new_with_options(
            mdt_url.as_str(),
            [(PartitionFields.as_ref(), METADATA_TABLE_PARTITION_FIELD)],
        )
        .await
    }

    /// Fetch records from the `files` partition of metadata table
    /// with optional data table partition pruning.
    ///
    /// Records are returned with normalized partition keys. For non-partitioned tables,
    /// the key is "" (empty string) instead of the internal "." representation.
    /// Normalization happens at decode time in [`decode_files_partition_record_with_schema`].
    ///
    /// # Note
    /// Must be called on a DATA table, not a METADATA table.
    pub async fn read_metadata_table_files_partition(
        &self,
        partition_pruner: &PartitionPruner,
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        let valid_instants =
            valid_metadata_instants(&self.file_system_view.storage, &self.hudi_configs).await?;
        let metadata_table = self.get_or_init_metadata_table().await?;
        metadata_table
            .fetch_files_partition_records(partition_pruner, Some(&valid_instants))
            .await
    }

    /// Fetch records from the `files` partition with optional partition pruning.
    ///
    /// For non-partitioned tables, directly fetches the "." record.
    /// For partitioned tables with filters, performs partition pruning via `__all_partitions__`.
    ///
    /// # Arguments
    /// * `partition_pruner` - Data table's partition pruner to filter partitions.
    /// * `valid_instants` - Data-timeline-fenced MDT instants; `None` trusts all
    ///   log blocks (only for callers without data-table access, e.g. tests).
    ///
    /// # Note
    /// Must be called on a METADATA table instance.
    pub async fn fetch_files_partition_records(
        &self,
        partition_pruner: &PartitionPruner,
        valid_instants: Option<&std::collections::HashSet<String>>,
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        // Non-partitioned table: directly fetch "." record
        if !partition_pruner.is_table_partitioned() {
            return self
                .read_files_partition(
                    &[FilesPartitionRecord::NON_PARTITIONED_NAME],
                    valid_instants,
                )
                .await;
        }

        // Partitioned table without filters: read all records
        if partition_pruner.is_empty() {
            return self.read_files_partition(&[], valid_instants).await;
        }

        // Partitioned table with filters: prune partitions first, then read only matching records
        let all_partitions_records = self
            .read_files_partition(&[FilesPartitionRecord::ALL_PARTITIONS_KEY], valid_instants)
            .await?;

        let partition_names: Vec<&str> = all_partitions_records
            .get(FilesPartitionRecord::ALL_PARTITIONS_KEY)
            .map(|r| r.partition_names())
            .unwrap_or_default();

        let pruned: Vec<&str> = partition_names
            .into_iter()
            .filter(|p| partition_pruner.should_include(p))
            .collect();

        if pruned.is_empty() {
            return Ok(HashMap::new());
        }

        self.read_files_partition(&pruned, valid_instants).await
    }

    /// Read records from the `files` partition.
    ///
    /// If keys is empty, reads all records. Otherwise, reads only the specified keys.
    ///
    /// # Note
    /// Must be called on a METADATA table instance.
    async fn read_files_partition(
        &self,
        keys: &[&str],
        valid_instants: Option<&std::collections::HashSet<String>>,
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        let Some(timestamp) = self.timeline.get_latest_commit_timestamp_as_option() else {
            return Ok(HashMap::new());
        };

        let timeline_view = self.timeline.create_view_as_of(timestamp).await?;

        let filters = from_str_tuples([(
            METADATA_TABLE_PARTITION_FIELD,
            "=",
            FilesPartitionRecord::PARTITION_NAME,
        )])?;
        let partition_schema = self.get_partition_schema().await?;
        let partition_pruner =
            PartitionPruner::new(&filters, &partition_schema, self.hudi_configs.as_ref())?;

        // Use empty file pruner for metadata table - no column stats pruning needed
        // Use empty schema since the pruner is empty and won't use the schema
        let file_pruner = FilePruner::empty();
        let table_schema = Schema::empty();

        // MDT itself uses HFile base files; no estimator applies here.
        let file_slices = self
            .file_system_view
            .get_file_slices_by_storage_listing(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
                None,
            )
            .await?;

        if file_slices.len() != 1 {
            return Err(CoreError::MetadataTable(format!(
                "Expected 1 file slice for {} partition, got {}",
                FilesPartitionRecord::PARTITION_NAME,
                file_slices.len()
            )));
        }

        let file_slice = file_slices.into_iter().next().unwrap();
        let opts = ReadOptions::new().with_end_timestamp(timestamp);
        let fg_reader = self
            .create_file_group_reader_with_options(Some(&opts), std::iter::empty::<(&str, &str)>())
            .await?;

        fg_reader
            .read_metadata_table_files_partition(&file_slice, keys, valid_instants)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig::TableVersion;
    use crate::table::partition::PartitionPruner;
    use hudi_test::{QuickstartTripsTable, SampleTable};
    use records::{FilesPartitionRecord, MetadataRecordType};
    use std::collections::HashSet;

    async fn get_data_table() -> Table {
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        Table::new(&table_path).await.unwrap()
    }

    #[tokio::test]
    async fn hudi_table_read_metadata_table_files_partition() {
        let data_table = get_data_table().await;
        let partition_schema = data_table.get_partition_schema().await.unwrap();
        let partition_pruner =
            PartitionPruner::new(&[], &partition_schema, data_table.hudi_configs.as_ref()).unwrap();

        let records = data_table
            .read_metadata_table_files_partition(&partition_pruner)
            .await
            .unwrap();

        // Should have 4 records: __all_partitions__ + 3 city partitions
        assert_eq!(records.len(), 4);

        // Validate __all_partitions__ record
        let all_partitions = records
            .get(FilesPartitionRecord::ALL_PARTITIONS_KEY)
            .unwrap();
        assert_eq!(
            all_partitions.record_type,
            MetadataRecordType::AllPartitions
        );
        let partition_names: HashSet<&str> = all_partitions.partition_names().into_iter().collect();
        assert_eq!(
            partition_names,
            HashSet::from(["city=chennai", "city=san_francisco", "city=sao_paulo"])
        );

        // Validate city=chennai record with actual file names
        let chennai = records.get("city=chennai").unwrap();
        assert_eq!(chennai.record_type, MetadataRecordType::Files);
        let chennai_files: HashSet<_> = chennai.active_file_names().into_iter().collect();
        assert_eq!(
            chennai_files,
            HashSet::from([
                "6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc-0_2-986-2794_20251220210108078.parquet",
                "6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc-0_0-1112-3190_20251220210129235.parquet",
                ".6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc-0_20251220210127080.log.1_0-1072-3078",
                ".6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc-0_20251220210128625.log.1_0-1097-3150",
            ])
        );
        assert!(chennai.total_size() > 0);
    }

    #[tokio::test]
    async fn hudi_table_get_metadata_table_partitions() {
        let data_table = get_data_table().await;

        // Verify we can get the metadata table partitions from the data table
        let partitions = data_table.get_metadata_table_partitions();

        // The test table has 5 metadata table partitions configured
        assert_eq!(
            partitions.len(),
            5,
            "Should have 5 metadata table partitions, got: {partitions:?}"
        );

        // Verify all expected partitions are present
        let expected = [
            "column_stats",
            "files",
            "partition_stats",
            "record_index",
            "secondary_index_rider_idx",
        ];
        for partition in &expected {
            assert!(
                partitions.contains(&partition.to_string()),
                "Should contain '{partition}' partition, got: {partitions:?}"
            );
        }
    }

    #[tokio::test]
    async fn hudi_table_is_metadata_table_enabled() {
        // V8 table with files partition configured should enable metadata table
        // even without explicit hoodie.metadata.enable=true
        let data_table = get_data_table().await;

        // Verify it's a v8 table
        let table_version: isize = data_table
            .hudi_configs
            .get(TableVersion)
            .map(|v| v.into())
            .unwrap_or(0);
        assert_eq!(table_version, 8, "Test table should be v8");

        // Verify files partition is configured
        let partitions = data_table.get_metadata_table_partitions();
        assert!(
            partitions.contains(&"files".to_string()),
            "Should have 'files' partition configured"
        );

        // Verify is_metadata_table_enabled returns true (implicit enablement)
        assert!(
            data_table.is_metadata_table_enabled(),
            "is_metadata_table_enabled should return true for v8 table with files partition"
        );
    }

    #[tokio::test]
    async fn hudi_table_v6_metadata_table_not_enabled() {
        // V6 tables should NOT have metadata table enabled, even with explicit setting
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();

        // Verify it's a v6 table
        let table_version: isize = hudi_table
            .hudi_configs
            .get(TableVersion)
            .map(|v| v.into())
            .unwrap_or(0);
        assert_eq!(table_version, 6, "Test table should be v6");

        // V6 tables should not have metadata table enabled
        assert!(
            !hudi_table.is_metadata_table_enabled(),
            "is_metadata_table_enabled should return false for v6 table"
        );
    }

    #[tokio::test]
    async fn hudi_table_is_not_metadata_table() {
        // A regular data table should not be a metadata table
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        assert!(
            !hudi_table.is_metadata_table(),
            "Regular data table should not be a metadata table"
        );
    }

    #[tokio::test]
    async fn hudi_table_metadata_table_is_metadata_table() {
        // Create a metadata table and verify it's recognized as such
        let data_table = get_data_table().await;
        let metadata_table = data_table.new_metadata_table().await.unwrap();
        assert!(
            metadata_table.is_metadata_table(),
            "Metadata table should be recognized as a metadata table"
        );
    }

    #[tokio::test]
    async fn hudi_table_new_metadata_table_from_metadata_table_errors() {
        // Trying to create a metadata table from a metadata table should fail
        let data_table = get_data_table().await;
        let metadata_table = data_table.new_metadata_table().await.unwrap();

        let result = metadata_table.new_metadata_table().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot create metadata table from another metadata table"),
            "Error message should indicate cannot create from metadata table"
        );
    }
}
