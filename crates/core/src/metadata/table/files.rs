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

//! Files partition reading from the metadata table.

use std::collections::HashMap;

use arrow_schema::Schema;

use crate::Result;
use crate::config::read::HudiReadConfig;
use crate::error::CoreError;
use crate::expr::filter::from_str_tuples;
use crate::metadata::METADATA_TABLE_PARTITION_FIELD;
use crate::table::Table;
use crate::table::{FilePruner, PartitionPruner};

use super::records::FilesPartitionRecord;

impl Table {
    /// Fetch records from the `files` partition of metadata table
    /// with optional data table partition pruning.
    ///
    /// This is a convenience wrapper that creates a metadata table instance internally.
    /// For multiple metadata table operations, prefer creating the metadata table once
    /// with [`new_metadata_table`] and calling [`fetch_files_partition_records`] directly.
    ///
    /// Records are returned with normalized partition keys. For non-partitioned tables,
    /// the key is "" (empty string) instead of the internal "." representation.
    /// Normalization happens at decode time in [`decode_files_partition_record_with_schema`].
    ///
    /// # Errors
    /// Returns an error if called on a metadata table instead of a data table.
    pub async fn read_metadata_table_files_partition(
        &self,
        partition_pruner: &PartitionPruner,
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        self.require_data_table()?;
        let metadata_table = self.new_metadata_table().await?;
        metadata_table
            .fetch_files_partition_records(partition_pruner)
            .await
    }

    /// Fetch records from the `files` partition with optional partition pruning.
    ///
    /// For non-partitioned tables, directly fetches the "." record.
    /// For partitioned tables with filters, performs partition pruning via `__all_partitions__`.
    ///
    /// # Arguments
    /// * `partition_pruner` - Data table's partition pruner to filter partitions.
    ///
    /// # Errors
    /// Returns an error if called on a data table instead of a metadata table.
    pub async fn fetch_files_partition_records(
        &self,
        partition_pruner: &PartitionPruner,
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        self.require_metadata_table()?;

        // Non-partitioned table: directly fetch "." record (normalized to "" key)
        //
        // Note: partition_stats is only enabled for partitioned tables, so partition_stats
        // pruning is not available for non-partitioned tables. However, column_stats
        // pruning (file-level) is still applied via fs_view.
        if !partition_pruner.is_table_partitioned() {
            return self
                .read_files_partition(&[FilesPartitionRecord::NON_PARTITIONED_NAME])
                .await;
        }

        // Partitioned table without filters: read all records
        if partition_pruner.is_empty() {
            return self.read_files_partition(&[]).await;
        }

        // Partitioned table with filters: partition pruning
        let all_partitions_records = self
            .read_files_partition(&[FilesPartitionRecord::ALL_PARTITIONS_KEY])
            .await?;

        let partition_names: Vec<&str> = all_partitions_records
            .get(FilesPartitionRecord::ALL_PARTITIONS_KEY)
            .map(|r| r.partition_names())
            .unwrap_or_default();

        // Step 2: Apply partition pruning
        let pruned: Vec<&str> = partition_names
            .into_iter()
            .filter(|p| partition_pruner.should_include(p))
            .collect();

        if pruned.is_empty() {
            return Ok(HashMap::new());
        }

        // Step 3: Read only the pruned partition records
        self.read_files_partition(&pruned).await
    }

    /// Fetch specific keys from the files partition.
    ///
    /// # Errors
    /// Returns an error if called on a data table instead of a metadata table.
    pub async fn fetch_files_partition_records_by_keys(
        &self,
        keys: &[&str],
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        self.require_metadata_table()?;
        self.read_files_partition(keys).await
    }

    /// Read records from the `files` partition.
    ///
    /// If keys is empty, reads all records. Otherwise, reads only the specified keys.
    ///
    /// This is an internal method called after validation.
    async fn read_files_partition(
        &self,
        keys: &[&str],
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

        let file_slices = self
            .file_system_view
            .get_file_slices_by_storage_listing(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
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
        let fg_reader = self.create_file_group_reader_with_options([(
            HudiReadConfig::FileGroupEndTimestamp,
            timestamp,
        )])?;

        fg_reader
            .read_metadata_table_files_partition(&file_slice, keys)
            .await
    }
}
