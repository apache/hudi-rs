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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::config::table::HudiTableConfig::BaseFileFormat;
use crate::config::HudiConfigs;
use crate::file_group::FileGroup;
use crate::metadata::table_record::FilesPartitionRecord;
use crate::storage::Storage;
use crate::timeline::completion_time::TimelineViewByCompletionTime;

use crate::file_group::builder::file_groups_from_files_partition_records;
use crate::file_group::file_slice::FileSlice;
use crate::table::listing::FileLister;
use crate::table::partition::PartitionPruner;
use crate::Result;
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

    /// Load file groups by listing from the file system.
    ///
    /// # Arguments
    /// * `partition_pruner` - Filters which partitions to include
    /// * `completion_time_view` - View to look up completion timestamps.
    ///   - For v6 tables: Pass [`V6CompletionTimeView`] (returns `None` for all lookups)
    ///   - For v8+ tables: Pass [`CompletionTimeView`] built from timeline instants
    ///
    /// [`V6CompletionTimeView`]: crate::timeline::completion_time::V6CompletionTimeView
    /// [`CompletionTimeView`]: crate::timeline::completion_time::CompletionTimeView
    async fn load_file_groups_from_file_system<V: TimelineViewByCompletionTime + Sync>(
        &self,
        partition_pruner: &PartitionPruner,
        completion_time_view: &V,
    ) -> Result<()> {
        let lister = FileLister::new(
            self.hudi_configs.clone(),
            self.storage.clone(),
            partition_pruner.to_owned(),
        );
        let file_groups_map = lister
            .list_file_groups_for_relevant_partitions(completion_time_view)
            .await?;
        for (partition_path, file_groups) in file_groups_map {
            self.partition_to_file_groups
                .insert(partition_path, file_groups);
        }
        Ok(())
    }

    /// Load file groups from metadata table records.
    ///
    /// This is an alternative to `load_file_groups_from_file_system` that uses pre-fetched
    /// metadata table records instead of listing from storage. Only partitions that pass the
    /// partition pruner will be loaded.
    ///
    /// This method is not async because it operates on pre-fetched records - no I/O is needed.
    ///
    /// TODO: Consider making this async and fetching metadata table records within this method
    /// instead of receiving pre-fetched records. This would make the API more symmetric with
    /// `load_file_groups_from_file_system` and encapsulate the metadata table fetching logic.
    ///
    /// # Arguments
    /// * `metadata_table_records` - Metadata table files partition records
    /// * `partition_pruner` - Filters which partitions to include
    /// * `completion_time_view` - View to look up completion timestamps.
    ///   - For v6 tables: Pass [`V6CompletionTimeView`] (returns `None` for all lookups)
    ///   - For v8+ tables: Pass [`CompletionTimeView`] built from timeline instants
    ///
    /// [`V6CompletionTimeView`]: crate::timeline::completion_time::V6CompletionTimeView
    /// [`CompletionTimeView`]: crate::timeline::completion_time::CompletionTimeView
    fn load_file_groups_from_metadata_table<V: TimelineViewByCompletionTime>(
        &self,
        metadata_table_records: &HashMap<String, FilesPartitionRecord>,
        partition_pruner: &PartitionPruner,
        completion_time_view: &V,
    ) -> Result<()> {
        let base_file_format: String = self.hudi_configs.get_or_default(BaseFileFormat).into();
        let file_groups_map = file_groups_from_files_partition_records(
            metadata_table_records,
            &base_file_format,
            completion_time_view,
        )?;

        for entry in file_groups_map.iter() {
            let partition_path = entry.key();
            if partition_pruner.is_empty() || partition_pruner.should_include(partition_path) {
                self.partition_to_file_groups
                    .insert(partition_path.clone(), entry.value().clone());
            }
        }
        Ok(())
    }

    /// Collect file slices from loaded file groups as of a given timestamp.
    ///
    /// This is a private method that assumes file groups have already been loaded.
    async fn collect_file_slices_as_of(
        &self,
        timestamp: &str,
        partition_pruner: &PartitionPruner,
        excluding_file_groups: &HashSet<FileGroup>,
    ) -> Result<Vec<FileSlice>> {
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

    /// Get file slices as of a given timestamp.
    ///
    /// This is the single entrypoint for retrieving file slices. It internally decides
    /// whether to load file groups from metadata table records (if provided) or from storage listing.
    ///
    /// # Arguments
    /// * `timestamp` - The timestamp to get file slices as of
    /// * `partition_pruner` - Filters which partitions to include
    /// * `excluding_file_groups` - File groups to exclude (e.g., replaced file groups)
    /// * `metadata_table_records` - Optional metadata table files partition records for
    ///   accelerated listing. If provided, file groups are loaded from these records instead
    ///   of storage listing.
    /// * `completion_time_view` - View to look up completion timestamps.
    ///   - For v6 tables: Pass [`V6CompletionTimeView`] (returns `None` for all lookups)
    ///   - For v8+ tables: Pass [`CompletionTimeView`] built from timeline instants
    ///
    /// [`V6CompletionTimeView`]: crate::timeline::completion_time::V6CompletionTimeView
    /// [`CompletionTimeView`]: crate::timeline::completion_time::CompletionTimeView
    pub async fn get_file_slices_as_of<V: TimelineViewByCompletionTime + Sync>(
        &self,
        timestamp: &str,
        partition_pruner: &PartitionPruner,
        excluding_file_groups: &HashSet<FileGroup>,
        metadata_table_records: Option<&HashMap<String, FilesPartitionRecord>>,
        completion_time_view: &V,
    ) -> Result<Vec<FileSlice>> {
        // Load file groups from metadata table records if provided, otherwise from file system
        if let Some(records) = metadata_table_records {
            self.load_file_groups_from_metadata_table(
                records,
                partition_pruner,
                completion_time_view,
            )?;
        } else {
            self.load_file_groups_from_file_system(partition_pruner, completion_time_view)
                .await?;
        }
        self.collect_file_slices_as_of(timestamp, partition_pruner, excluding_file_groups)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::filter::Filter;
    use crate::table::Table;

    use hudi_test::SampleTable;
    use std::collections::HashSet;

    #[tokio::test]
    async fn fs_view_get_latest_file_slices() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let latest_timestamp = hudi_table
            .timeline
            .completed_commits
            .iter()
            .next_back()
            .map(|i| i.timestamp.clone())
            .unwrap();
        let fs_view = &hudi_table.file_system_view;
        let completion_time_view = hudi_table.timeline.create_completion_time_view();

        assert!(fs_view.partition_to_file_groups.is_empty());
        let partition_pruner = PartitionPruner::empty();
        let excludes = HashSet::new();
        let file_slices = fs_view
            .get_file_slices_as_of(
                &latest_timestamp,
                &partition_pruner,
                &excludes,
                None, // mdt_records
                &completion_time_view,
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
        let latest_timestamp = hudi_table
            .timeline
            .completed_commits
            .iter()
            .next_back()
            .map(|i| i.timestamp.clone())
            .unwrap();
        let fs_view = &hudi_table.file_system_view;
        let completion_time_view = hudi_table.timeline.create_completion_time_view();

        assert_eq!(fs_view.partition_to_file_groups.len(), 0);
        let partition_pruner = PartitionPruner::empty();
        let excludes = &hudi_table
            .timeline
            .get_replaced_file_groups_as_of(&latest_timestamp)
            .await
            .unwrap();
        let file_slices = fs_view
            .get_file_slices_as_of(
                &latest_timestamp,
                &partition_pruner,
                excludes,
                None, // mdt_records
                &completion_time_view,
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
        let latest_timestamp = hudi_table
            .timeline
            .completed_commits
            .iter()
            .next_back()
            .map(|i| i.timestamp.clone())
            .unwrap();
        let fs_view = &hudi_table.file_system_view;
        let completion_time_view = hudi_table.timeline.create_completion_time_view();

        assert_eq!(fs_view.partition_to_file_groups.len(), 0);

        let excludes = &hudi_table
            .timeline
            .get_replaced_file_groups_as_of(&latest_timestamp)
            .await
            .unwrap();
        let partition_schema = hudi_table.get_partition_schema().await.unwrap();

        let filter_lt_20 = Filter::try_from(("byteField", "<", "20")).unwrap();
        let filter_eq_300 = Filter::try_from(("shortField", "=", "300")).unwrap();
        let partition_pruner = PartitionPruner::new(
            &[filter_lt_20, filter_eq_300],
            &partition_schema,
            hudi_table.hudi_configs.as_ref(),
        )
        .unwrap();

        let file_slices = fs_view
            .get_file_slices_as_of(
                &latest_timestamp,
                &partition_pruner,
                excludes,
                None, // mdt_records
                &completion_time_view,
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
