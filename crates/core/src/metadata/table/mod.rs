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
//!
//! # Module Structure
//! - `records` - Record types for metadata table partitions
//! - `files` - Files partition reading
//! - `stats` - Column and partition statistics reading

mod files;
pub mod records;
mod stats;

pub use stats::{column_stats_records_to_stats_map, partition_stats_records_to_stats_map};

use crate::Result;
use crate::config::table::HudiTableConfig::{
    MetadataTableEnabled, MetadataTablePartitions, PartitionFields, TableVersion,
};
use crate::error::CoreError;
use crate::metadata::METADATA_TABLE_PARTITION_FIELD;
use crate::storage::util::join_url_segments;
use crate::table::Table;

use records::{ColumnStatsRecord, FilesPartitionRecord, PartitionStatsRecord};

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

    /// Validate that this is a data table (not a metadata table).
    ///
    /// # Errors
    /// Returns an error if this is a metadata table.
    pub fn require_data_table(&self) -> Result<()> {
        if self.is_metadata_table() {
            return Err(CoreError::MetadataTable(
                "This method must be called on a data table, not a metadata table".to_string(),
            ));
        }
        Ok(())
    }

    /// Validate that this is a metadata table.
    ///
    /// # Errors
    /// Returns an error if this is not a metadata table.
    pub fn require_metadata_table(&self) -> Result<()> {
        if !self.is_metadata_table() {
            return Err(CoreError::MetadataTable(
                "This method must be called on a metadata table instance".to_string(),
            ));
        }
        Ok(())
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

    /// Check if the column_stats partition is available in the metadata table.
    ///
    /// Returns `true` if "column_stats" is in the configured [`MetadataTablePartitions`].
    pub fn has_column_stats_partition(&self) -> bool {
        self.get_metadata_table_partitions()
            .contains(&ColumnStatsRecord::PARTITION_NAME.to_string())
    }

    /// Check if the partition_stats partition is available in the metadata table.
    ///
    /// Returns `true` if "partition_stats" is in the configured [`MetadataTablePartitions`].
    pub fn has_partition_stats_partition(&self) -> bool {
        self.get_metadata_table_partitions()
            .contains(&PartitionStatsRecord::PARTITION_NAME.to_string())
    }

    /// Create a metadata table instance for this data table.
    ///
    /// # Errors
    /// Returns an error if:
    /// - Called on a metadata table instead of a data table
    /// - No metadata table partitions are configured
    /// - The metadata table cannot be created
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig::TableVersion;
    use crate::table::PartitionPruner;
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
