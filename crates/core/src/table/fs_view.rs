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

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig::BaseFileFormat;
use crate::file_group::FileGroup;
use crate::file_group::builder::file_groups_from_files_partition_records;
use crate::file_group::file_slice::FileSlice;
use crate::storage::Storage;
use crate::table::Table;
use crate::table::listing::FileLister;
use crate::table::partition::PartitionPruner;
use crate::timeline::completion_time::CompletionTimeView;
use crate::timeline::view::TimelineView;
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

    /// Load file groups by listing from the storage.
    ///
    /// # Arguments
    /// * `partition_pruner` - Filters which partitions to include
    /// * `completion_time_view` - View to look up completion timestamps
    async fn load_file_groups_from_storage<V: CompletionTimeView + Sync>(
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
    /// This is an alternative to `load_file_groups_from_file_system` that uses
    /// file listing records fetched from the metadata table. Only partitions that
    /// pass the partition pruner will be loaded.
    ///
    /// This method is not async because it operates on pre-fetched records.
    ///
    /// # Arguments
    /// * `records` - Metadata table files partition records
    /// * `partition_pruner` - Filters which partitions to include
    /// * `completion_time_view` - View to look up completion timestamps
    fn load_file_groups_from_metadata_table_records<V: CompletionTimeView>(
        &self,
        records: &HashMap<String, crate::metadata::table_record::FilesPartitionRecord>,
        partition_pruner: &PartitionPruner,
        completion_time_view: &V,
    ) -> Result<()> {
        let base_file_format: String = self.hudi_configs.get_or_default(BaseFileFormat).into();
        let file_groups_map = file_groups_from_files_partition_records(
            records,
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
    /// * `timeline_view` - The timeline view containing query context
    /// * `metadata_table` - Optional metadata table instance
    pub(crate) async fn get_file_slices(
        &self,
        partition_pruner: &PartitionPruner,
        timeline_view: &TimelineView,
        metadata_table: Option<&Table>,
    ) -> Result<Vec<FileSlice>> {
        if let Some(mdt) = metadata_table {
            // Use metadata table for file listing
            let records = mdt.fetch_files_partition_records(partition_pruner).await?;
            self.load_file_groups_from_metadata_table_records(
                &records,
                partition_pruner,
                timeline_view,
            )?;
        } else {
            // Fall back to storage listing
            self.load_file_groups_from_storage(partition_pruner, timeline_view)
                .await?;
        }
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
    /// * `timeline_view` - The timeline view containing query context
    pub(crate) async fn get_file_slices_by_storage_listing(
        &self,
        partition_pruner: &PartitionPruner,
        timeline_view: &TimelineView,
    ) -> Result<Vec<FileSlice>> {
        self.load_file_groups_from_storage(partition_pruner, timeline_view)
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

        let file_slices = fs_view
            .get_file_slices(&partition_pruner, &timeline_view, None)
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

        let file_slices = fs_view
            .get_file_slices(&partition_pruner, &timeline_view, None)
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

        let filter_lt_20 = Filter::try_from(("byteField", "<", "20")).unwrap();
        let filter_eq_300 = Filter::try_from(("shortField", "=", "300")).unwrap();
        let partition_pruner = PartitionPruner::new(
            &[filter_lt_20, filter_eq_300],
            &partition_schema,
            hudi_table.hudi_configs.as_ref(),
        )
        .unwrap();

        let file_slices = fs_view
            .get_file_slices(&partition_pruner, &timeline_view, None)
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
