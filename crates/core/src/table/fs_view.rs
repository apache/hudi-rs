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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema;

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig::BaseFileFormat;
use crate::file_group::FileGroup;
use crate::file_group::builder::file_groups_from_files_partition_records;
use crate::file_group::file_slice::FileSlice;
use crate::metadata::table::records::FilesPartitionRecord;
use crate::metadata::table::{
    column_stats_records_to_stats_map, partition_stats_records_to_stats_map,
};
use crate::storage::Storage;
use crate::table::Table;
use crate::table::listing::FileLister;
use crate::table::{FilePruner, PartitionPruner};
use crate::timeline::view::TimelineView;
use crate::util::hash::{get_column_stats_key, get_partition_stats_key};
use dashmap::DashMap;

/// A view of the Hudi table's data files (files stored outside the `.hoodie/` directory) in the file system. It provides APIs to load and
/// access the file groups and file slices.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct FileSystemView {
    pub(crate) hudi_configs: Arc<HudiConfigs>,
    pub(crate) storage: Arc<Storage>,
    partition_to_file_groups: Arc<DashMap<String, Vec<FileGroup>>>,
}

impl FileSystemView {
    pub async fn new(
        hudi_configs: Arc<HudiConfigs>,
        storage_options: Arc<HashMap<String, String>>,
    ) -> Result<Self> {
        let storage = Storage::new(storage_options.clone(), hudi_configs.clone())?;
        let partition_to_file_groups = Arc::new(DashMap::new());
        Ok(FileSystemView {
            hudi_configs,
            storage,
            partition_to_file_groups,
        })
    }

    /// Load file groups from the appropriate source (storage or metadata table records)
    /// and apply stats-based pruning.
    ///
    /// # File Listing Source
    /// - If `files_partition_records` is Some: Uses pre-fetched metadata table records
    /// - If `files_partition_records` is None: Uses storage listing via FileLister
    ///
    /// # Stats Pruning
    /// Column statistics can come from two sources, both stored as `StatisticsContainer`:
    /// 1. Metadata table's column_stats partition (preferred, batch loaded)
    /// 2. Parquet file footers (fallback, loaded per-file)
    ///
    /// # Partition Stats Enhancement
    /// If metadata table has partition_stats partition, enhances PartitionPruner with
    /// aggregated statistics to prune entire partitions before processing files.
    ///
    /// # Arguments
    /// * `partition_pruner` - Filters which partitions to include
    /// * `file_pruner` - Filters files based on column statistics
    /// * `table_schema` - Table schema for statistics extraction from Parquet footers
    /// * `timeline_view` - The timeline view providing query timestamp and completion time lookups
    /// * `files_partition_records` - Optional pre-fetched metadata table records
    /// * `metadata_table` - Optional metadata table for accessing column and partition stats
    async fn load_file_groups(
        &self,
        partition_pruner: &PartitionPruner,
        file_pruner: &FilePruner,
        table_schema: &Schema,
        timeline_view: &TimelineView,
        files_partition_records: Option<&HashMap<String, FilesPartitionRecord>>,
        metadata_table: Option<&Table>,
    ) -> Result<()> {
        // Step 1: Get file groups from appropriate source
        let file_groups_map = if let Some(records) = files_partition_records {
            // Use pre-fetched metadata table records
            let base_file_format: String = self.hudi_configs.get_or_default(BaseFileFormat).into();
            file_groups_from_files_partition_records(records, &base_file_format, timeline_view)?
        } else {
            // Use storage listing
            let lister = FileLister::new(
                self.hudi_configs.clone(),
                self.storage.clone(),
                partition_pruner.to_owned(),
            );
            lister
                .list_file_groups_for_relevant_partitions(timeline_view)
                .await?
        };

        // Step 2: Enhance PartitionPruner with partition_stats if available
        let partition_paths: Vec<String> =
            file_groups_map.iter().map(|r| r.key().clone()).collect();
        let partition_pruner = self
            .enhance_partition_pruner_with_stats(
                partition_pruner,
                file_pruner,
                &partition_paths,
                metadata_table,
            )
            .await;

        // Step 3: Check if column_stats might be available for file-level pruning
        // We'll try to load column_stats from metadata table if available
        let use_column_stats = metadata_table.is_some();

        // Step 4: Apply partition pruning (for metadata table path) and stats pruning
        // Note: Storage listing path already applies partition pruning via FileLister
        for (partition_path, file_groups) in file_groups_map {
            // Skip partitions that don't match the pruner (for metadata table path)
            if files_partition_records.is_some()
                && !partition_pruner.is_empty()
                && !partition_pruner.should_include(&partition_path)
            {
                continue;
            }

            // Load column stats from metadata table if available
            let preloaded_stats = if use_column_stats && !file_pruner.is_empty() {
                self.load_column_stats_from_metadata_table(
                    &file_groups,
                    file_pruner,
                    timeline_view.as_of_timestamp(),
                    &partition_path,
                    metadata_table
                        .expect("metadata_table must be Some when use_column_stats is true"),
                )
                .await
            } else {
                HashMap::new()
            };

            // Apply unified stats pruning (uses preloaded stats, falls back to footer)
            let retained = self
                .apply_stats_pruning(
                    file_groups,
                    file_pruner,
                    table_schema,
                    timeline_view.as_of_timestamp(),
                    &preloaded_stats,
                )
                .await;

            self.partition_to_file_groups
                .insert(partition_path, retained);
        }

        Ok(())
    }

    /// Enhance PartitionPruner with partition-level statistics from the metadata table.
    ///
    /// If a metadata table is provided and there are data column filters, this method
    /// reads partition statistics and adds them to the pruner.
    async fn enhance_partition_pruner_with_stats(
        &self,
        partition_pruner: &PartitionPruner,
        file_pruner: &FilePruner,
        partition_paths: &[String],
        metadata_table: Option<&Table>,
    ) -> PartitionPruner {
        // Check if metadata table is available and we have filters to apply
        let Some(mdt) = metadata_table else {
            return partition_pruner.clone();
        };

        if file_pruner.is_empty() {
            return partition_pruner.clone();
        }

        // Get column names for stats lookup
        let column_names = file_pruner.filter_column_names();

        if partition_paths.is_empty() || column_names.is_empty() {
            return partition_pruner.clone();
        }

        // Generate keys for partition stats lookup
        let keys: Vec<String> = column_names
            .iter()
            .flat_map(|col| {
                partition_paths
                    .iter()
                    .map(move |part| get_partition_stats_key(col, part))
            })
            .collect();
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

        // Read partition stats records from metadata table
        match mdt.fetch_partition_stats_records(&key_refs).await {
            Ok(records) => {
                if records.is_empty() {
                    return partition_pruner.clone();
                }
                match partition_stats_records_to_stats_map(records) {
                    Ok(partition_stats) => {
                        log::debug!(
                            "Loaded partition_stats for {} partitions, {} columns",
                            partition_paths.len(),
                            column_names.len()
                        );
                        partition_pruner
                            .clone()
                            .with_partition_stats(partition_stats)
                            .with_data_filters(file_pruner.filters())
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to convert partition_stats: {e}. Continuing without partition stats pruning."
                        );
                        partition_pruner.clone()
                    }
                }
            }
            Err(e) => {
                log::warn!(
                    "Failed to read partition_stats: {e}. Continuing without partition stats pruning."
                );
                partition_pruner.clone()
            }
        }
    }

    /// Load column statistics from the metadata table for files in a partition.
    ///
    /// Returns a map from file name to StatisticsContainer. On error, returns an
    /// empty map (caller will fall back to Parquet footers).
    async fn load_column_stats_from_metadata_table(
        &self,
        file_groups: &[FileGroup],
        file_pruner: &FilePruner,
        as_of_timestamp: &str,
        partition_path: &str,
        metadata_table: &Table,
    ) -> HashMap<String, crate::statistics::StatisticsContainer> {
        // Collect file names for stats lookup
        let file_names: Vec<String> = file_groups
            .iter()
            .filter_map(|fg| {
                fg.get_file_slice_as_of(as_of_timestamp)
                    .and_then(|fsl| fsl.base_file_relative_path().ok())
                    .map(|path| path.rsplit('/').next().unwrap_or(&path).to_string())
            })
            .collect();

        if file_names.is_empty() {
            return HashMap::new();
        }

        let column_names = file_pruner.filter_column_names();

        // Generate keys for column stats lookup
        let keys: Vec<String> = column_names
            .iter()
            .flat_map(|col| {
                file_names
                    .iter()
                    .map(move |file| get_column_stats_key(col, partition_path, file))
            })
            .collect();
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

        // Read column stats records from metadata table
        match metadata_table.fetch_column_stats_records(&key_refs).await {
            Ok(records) => {
                if records.is_empty() {
                    log::debug!(
                        "No column_stats found for {} files in partition '{partition_path}'",
                        file_names.len()
                    );
                    return HashMap::new();
                }
                match column_stats_records_to_stats_map(records) {
                    Ok(stats) => {
                        log::debug!(
                            "Loaded column_stats for {} files in partition '{partition_path}'",
                            file_names.len()
                        );
                        stats
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to convert column_stats for partition '{partition_path}': {e}. Will fall back to Parquet footers."
                        );
                        HashMap::new()
                    }
                }
            }
            Err(e) => {
                log::warn!(
                    "Failed to read column_stats for partition '{partition_path}': {e}. Will fall back to Parquet footers."
                );
                HashMap::new()
            }
        }
    }

    /// Apply file-level stats pruning using column statistics.
    ///
    /// This is the unified pruning method that works with `StatisticsContainer` from any source:
    /// - If pre-loaded stats are available (from metadata table), use them
    /// - Otherwise, load stats from Parquet file footers as fallback
    ///
    /// Files are included (not pruned) if:
    /// - The pruner has no filters
    /// - The file is not a Parquet file (for footer fallback)
    /// - Stats cannot be loaded from any source (conservative behavior)
    /// - The file's stats indicate it might contain matching rows
    async fn apply_stats_pruning(
        &self,
        file_groups: Vec<FileGroup>,
        file_pruner: &FilePruner,
        table_schema: &Schema,
        as_of_timestamp: &str,
        preloaded_stats: &HashMap<String, crate::statistics::StatisticsContainer>,
    ) -> Vec<FileGroup> {
        if file_pruner.is_empty() {
            return file_groups;
        }

        let mut retained = Vec::with_capacity(file_groups.len());

        for mut fg in file_groups {
            if let Some(fsl) = fg.get_file_slice_mut_as_of(as_of_timestamp) {
                let relative_path = match fsl.base_file_relative_path() {
                    Ok(path) => path,
                    Err(e) => {
                        log::warn!(
                            "Cannot get base file path for pruning: {e}. Including file group."
                        );
                        retained.push(fg);
                        continue;
                    }
                };

                // Extract file name for stats lookup
                let file_name = relative_path.rsplit('/').next().unwrap_or(&relative_path);

                // Try pre-loaded stats first (from metadata table), then fall back to Parquet footer
                let stats = if let Some(s) = preloaded_stats.get(file_name) {
                    Some(s.clone())
                } else {
                    // Fall back to loading from Parquet footer
                    self.load_stats_from_parquet_footer(&relative_path, table_schema)
                        .await
                };

                match stats {
                    Some(ref s) => {
                        if file_pruner.should_include(s) {
                            retained.push(fg);
                        } else {
                            log::debug!("Pruned file {relative_path} based on column stats");
                        }
                    }
                    None => {
                        // No stats available from any source - include conservatively
                        retained.push(fg);
                    }
                }
            } else {
                // No file slice as of timestamp, include the file group
                // (it will be filtered out later in collect_file_slices)
                retained.push(fg);
            }
        }

        retained
    }

    /// Load column statistics from a Parquet file's footer.
    ///
    /// Returns None if the file is not a Parquet file or stats cannot be loaded.
    async fn load_stats_from_parquet_footer(
        &self,
        relative_path: &str,
        table_schema: &Schema,
    ) -> Option<crate::statistics::StatisticsContainer> {
        // Only load stats for Parquet files
        if !relative_path.to_lowercase().ends_with(".parquet") {
            return None;
        }

        match self
            .storage
            .get_parquet_column_stats(relative_path, table_schema)
            .await
        {
            Ok(stats) => Some(stats),
            Err(e) => {
                log::warn!(
                    "Failed to load column stats from footer for {relative_path}: {e}. Including file."
                );
                None
            }
        }
    }

    /// Collect file slices from loaded file groups using the timeline view.
    async fn collect_file_slices(
        &self,
        partition_pruner: &PartitionPruner,
        timeline_view: &TimelineView,
    ) -> Result<Vec<FileSlice>> {
        let timestamp = timeline_view.as_of_timestamp();
        let excluding_file_groups = timeline_view.excluding_file_groups();

        let mut file_slices = Vec::new();
        for mut partition_entry in self.partition_to_file_groups.iter_mut() {
            if !partition_pruner.should_include(partition_entry.key()) {
                continue;
            }
            let file_groups = partition_entry.value_mut();
            for fg in file_groups.iter_mut() {
                if excluding_file_groups.contains(fg) {
                    continue;
                }
                if let Some(fsl) = fg.get_file_slice_mut_as_of(timestamp) {
                    fsl.load_metadata_if_needed(&self.storage).await?;
                    file_slices.push(fsl.clone());
                }
            }
        }
        Ok(file_slices)
    }

    /// Get file slices using a [`TimelineView`].
    ///
    /// This is the main API for retrieving file slices for snapshot or time-travel queries.
    /// It loads file groups from metadata table (if enabled) or storage listing,
    /// then select file slices based on the timeline view.
    ///
    /// The [`TimelineView`] encapsulates:
    /// - The "as of" timestamp for the query
    /// - File groups to exclude (from replace commits for example)
    /// - Completion time mappings (if needed)
    ///
    /// # Arguments
    /// * `partition_pruner` - Filters which partitions to include
    /// * `file_pruner` - Filters files based on column statistics
    /// * `table_schema` - Table schema for statistics extraction
    /// * `timeline_view` - The timeline view containing query context
    /// * `metadata_table` - Optional metadata table for file listing and stats access
    pub(crate) async fn get_file_slices(
        &self,
        partition_pruner: &PartitionPruner,
        file_pruner: &FilePruner,
        table_schema: &Schema,
        timeline_view: &TimelineView,
        metadata_table: Option<&Table>,
    ) -> Result<Vec<FileSlice>> {
        // Fetch records from metadata table if available
        let files_partition_records = if let Some(mdt) = metadata_table {
            match mdt.fetch_files_partition_records(partition_pruner).await {
                Ok(records) => Some(records),
                Err(e) => {
                    log::warn!(
                        "Failed to read metadata table files partition: {e}. Falling back to storage listing."
                    );
                    None
                }
            }
        } else {
            None
        };

        self.load_file_groups(
            partition_pruner,
            file_pruner,
            table_schema,
            timeline_view,
            files_partition_records.as_ref(),
            metadata_table,
        )
        .await?;

        self.collect_file_slices(partition_pruner, timeline_view)
            .await
    }

    /// Get file slices using storage listing only.
    ///
    /// This method always lists files from storage, which is needed
    /// for metadata table's own file listing flow to avoid async recursion.
    ///
    /// # Arguments
    /// * `partition_pruner` - Filters which partitions to include
    /// * `file_pruner` - Filters files based on column statistics
    /// * `table_schema` - Table schema for statistics extraction
    /// * `timeline_view` - The timeline view containing query context
    pub(crate) async fn get_file_slices_by_storage_listing(
        &self,
        partition_pruner: &PartitionPruner,
        file_pruner: &FilePruner,
        table_schema: &Schema,
        timeline_view: &TimelineView,
    ) -> Result<Vec<FileSlice>> {
        // Pass None for both files_partition_records and data_table to force storage listing
        // and footer-based stats pruning (avoids recursion for metadata table)
        self.load_file_groups(
            partition_pruner,
            file_pruner,
            table_schema,
            timeline_view,
            None,
            None,
        )
        .await?;

        self.collect_file_slices(partition_pruner, timeline_view)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::filter::Filter;
    use crate::table::Table;

    use hudi_test::SampleTable;

    #[tokio::test]
    async fn fs_view_get_latest_file_slices() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let latest_timestamp = hudi_table.timeline.get_latest_commit_timestamp().unwrap();
        let fs_view = &hudi_table.file_system_view;

        assert!(fs_view.partition_to_file_groups.is_empty());

        let timeline_view = hudi_table
            .timeline
            .create_view_as_of(&latest_timestamp)
            .await
            .unwrap();
        let partition_pruner = PartitionPruner::empty();
        let file_pruner = FilePruner::empty();
        let table_schema = hudi_table.get_schema().await.unwrap();

        let file_slices = fs_view
            .get_file_slices(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
                None,
            )
            .await
            .unwrap();

        assert_eq!(fs_view.partition_to_file_groups.len(), 1);
        assert_eq!(file_slices.len(), 1);
        let file_ids = file_slices
            .iter()
            .map(|fsl| fsl.file_id())
            .collect::<Vec<_>>();
        assert_eq!(file_ids, vec!["a079bdb3-731c-4894-b855-abfcd6921007-0"]);
        for fsl in file_slices.iter() {
            assert_eq!(fsl.base_file.file_metadata.as_ref().unwrap().num_records, 4);
        }
    }

    #[tokio::test]
    async fn fs_view_get_latest_file_slices_with_replace_commit() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyleOverwritetable.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let latest_timestamp = hudi_table.timeline.get_latest_commit_timestamp().unwrap();
        let fs_view = &hudi_table.file_system_view;

        assert_eq!(fs_view.partition_to_file_groups.len(), 0);

        let timeline_view = hudi_table
            .timeline
            .create_view_as_of(&latest_timestamp)
            .await
            .unwrap();
        let partition_pruner = PartitionPruner::empty();
        let file_pruner = FilePruner::empty();
        let table_schema = hudi_table.get_schema().await.unwrap();

        let file_slices = fs_view
            .get_file_slices(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
                None,
            )
            .await
            .unwrap();

        assert_eq!(fs_view.partition_to_file_groups.len(), 3);
        assert_eq!(file_slices.len(), 1);
        let file_ids = file_slices
            .iter()
            .map(|fsl| fsl.file_id())
            .collect::<Vec<_>>();
        assert_eq!(file_ids, vec!["ebcb261d-62d3-4895-90ec-5b3c9622dff4-0"]);
        for fsl in file_slices.iter() {
            assert_eq!(fsl.base_file.file_metadata.as_ref().unwrap().num_records, 1);
        }
    }

    #[tokio::test]
    async fn fs_view_get_latest_file_slices_with_partition_filters() {
        let base_url = SampleTable::V6ComplexkeygenHivestyle.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let latest_timestamp = hudi_table.timeline.get_latest_commit_timestamp().unwrap();
        let fs_view = &hudi_table.file_system_view;

        assert_eq!(fs_view.partition_to_file_groups.len(), 0);

        let timeline_view = hudi_table
            .timeline
            .create_view_as_of(&latest_timestamp)
            .await
            .unwrap();
        let partition_schema = hudi_table.get_partition_schema().await.unwrap();
        let table_schema = hudi_table.get_schema().await.unwrap();

        let filter_lt_20 = Filter::try_from(("byteField", "<", "20")).unwrap();
        let filter_eq_300 = Filter::try_from(("shortField", "=", "300")).unwrap();
        let partition_pruner = PartitionPruner::new(
            &[filter_lt_20, filter_eq_300],
            &partition_schema,
            hudi_table.hudi_configs.as_ref(),
        )
        .unwrap();

        let file_pruner = FilePruner::empty();

        let file_slices = fs_view
            .get_file_slices(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
                None,
            )
            .await
            .unwrap();

        assert_eq!(fs_view.partition_to_file_groups.len(), 1);
        assert_eq!(file_slices.len(), 1);

        let file_ids = file_slices
            .iter()
            .map(|fsl| fsl.file_id())
            .collect::<Vec<_>>();
        assert_eq!(file_ids, vec!["a22e8257-e249-45e9-ba46-115bc85adcba-0"]);
        for fsl in file_slices.iter() {
            assert_eq!(fsl.base_file.file_metadata.as_ref().unwrap().num_records, 2);
        }
    }
}
