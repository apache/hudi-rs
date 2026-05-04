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
use crate::config::table::BaseFileFormatValue;
use crate::config::table::HudiTableConfig::BaseFileFormat;
use crate::file_group::FileGroup;
use crate::file_group::builder::file_groups_from_files_partition_records;
use crate::file_group::file_slice::FileSlice;
use crate::metadata::table::records::FilesPartitionRecord;
use crate::statistics::estimator::FileStatsEstimator;
use crate::storage::Storage;
use crate::table::Table;
use crate::table::file_pruner::FilePruner;
use crate::table::listing::FileLister;
use crate::table::partition::PartitionPruner;
use crate::timeline::view::TimelineView;
use dashmap::DashMap;

/// A view of the Hudi table's data files (files stored outside the `.hoodie/` directory) in the file system. It provides APIs to load and
/// access the file groups and file slices.
#[derive(Clone, Debug)]
pub struct FileSystemView {
    pub(crate) hudi_configs: Arc<HudiConfigs>,
    pub(crate) storage: Arc<Storage>,
    partition_to_file_groups: Arc<DashMap<String, Vec<FileGroup>>>,
}

impl FileSystemView {
    pub(crate) fn base_file_format(&self) -> BaseFileFormatValue {
        let s: String = self.hudi_configs.get_or_default(BaseFileFormat).into();
        s.parse::<BaseFileFormatValue>()
            .unwrap_or(BaseFileFormatValue::Parquet)
    }

    /// Case-insensitive ASCII suffix check that avoids allocating a lowercased copy of `s`.
    fn ends_with_ignore_ascii_case(s: &str, suffix: &str) -> bool {
        let s_bytes = s.as_bytes();
        let suffix_bytes = suffix.as_bytes();
        s_bytes.len() >= suffix_bytes.len()
            && s_bytes[s_bytes.len() - suffix_bytes.len()..].eq_ignore_ascii_case(suffix_bytes)
    }

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
    /// # Stats Pruning Source (for non-empty file_pruner)
    /// - Currently: Always extracts stats from Parquet file footers
    /// - TODO: Use metadata table partitions when available:
    ///   - partition_stats: Enhance PartitionPruner to prune partitions before file listing
    ///   - column_stats: Prune files without reading Parquet footers
    ///
    /// # Arguments
    /// * `partition_pruner` - Filters which partitions to include
    /// * `file_pruner` - Filters files based on column statistics
    /// * `table_schema` - Table schema for statistics extraction
    /// * `timeline_view` - The timeline view providing query timestamp and completion time lookups
    /// * `files_partition_records` - Optional pre-fetched metadata table records
    /// * `estimator` - Optional estimator used to populate `byte_size` and
    ///   `num_records` on base-file metadata for both MDT and storage-listing paths
    async fn load_file_groups(
        &self,
        partition_pruner: &PartitionPruner,
        file_pruner: &FilePruner,
        table_schema: &Schema,
        timeline_view: &TimelineView,
        files_partition_records: Option<&HashMap<String, FilesPartitionRecord>>,
        estimator: Option<&FileStatsEstimator>,
    ) -> Result<()> {
        let base_file_format = self.base_file_format();
        let base_file_extension = base_file_format.as_ref();

        let file_groups_map = if let Some(records) = files_partition_records {
            file_groups_from_files_partition_records(
                records,
                base_file_extension,
                timeline_view,
                estimator,
            )?
        } else {
            let lister = FileLister::new(
                self.hudi_configs.clone(),
                self.storage.clone(),
                partition_pruner.to_owned(),
            );
            lister
                .list_file_groups_for_relevant_partitions(timeline_view, estimator)
                .await?
        };

        // Apply partition pruning (for metadata table path) and stats pruning
        for (partition_path, file_groups) in file_groups_map {
            if files_partition_records.is_some()
                && !partition_pruner.is_empty()
                && !partition_pruner.should_include(&partition_path)
            {
                continue;
            }

            let retained = self
                .apply_stats_pruning_from_footers(
                    file_groups,
                    file_pruner,
                    table_schema,
                    timeline_view.as_of_timestamp(),
                )
                .await;
            self.partition_to_file_groups
                .insert(partition_path, retained);
        }

        Ok(())
    }

    /// Apply file-level stats pruning using Parquet file footers.
    ///
    /// Returns the filtered list of file groups that pass the pruning check.
    /// Files are included (not pruned) if:
    /// - The pruner has no filters
    /// - The table's base file format does not support footer stats
    /// - The file does not match the configured base file extension
    /// - Column stats cannot be loaded (conservative behavior)
    /// - The file's stats indicate it might contain matching rows
    async fn apply_stats_pruning_from_footers(
        &self,
        file_groups: Vec<FileGroup>,
        file_pruner: &FilePruner,
        table_schema: &Schema,
        as_of_timestamp: &str,
    ) -> Vec<FileGroup> {
        if file_pruner.is_empty() {
            return file_groups;
        }

        let base_file_format = self.base_file_format();
        // Footer-based column-stats pruning only applies to Parquet base files.
        // (Separate concern from the FileStatsEstimator parquet check, which lives
        // on Table::get_or_init_estimator.)
        if !matches!(base_file_format, BaseFileFormatValue::Parquet) {
            return file_groups;
        }
        let base_file_suffix = format!(".{}", base_file_format.as_ref());

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

                // Case-insensitive check for configured base file extension.
                if !Self::ends_with_ignore_ascii_case(&relative_path, &base_file_suffix) {
                    retained.push(fg);
                    continue;
                }

                let (file_metadata, col_stats) = match self
                    .storage
                    .get_file_metadata_and_stats(&relative_path, table_schema)
                    .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        log::warn!(
                            "Failed to load file stats for {relative_path}: {e}. Including file."
                        );
                        retained.push(fg);
                        continue;
                    }
                };

                if file_pruner.should_include(&col_stats) {
                    fsl.base_file.file_metadata = Some(file_metadata);
                    fsl.base_file_column_stats = Some(col_stats);
                    retained.push(fg);
                } else {
                    log::debug!("Pruned file {relative_path} based on column stats");
                }
            } else {
                // No file slice as of timestamp, include the file group
                // (it will be filtered out later in collect_file_slices)
                retained.push(fg);
            }
        }

        retained
    }

    /// Collect file slices from loaded file groups using the timeline view.
    ///
    /// File slices are first collected from the DashMap using read locks (released
    /// promptly), then metadata is loaded on the owned clones without holding any
    /// locks.
    async fn collect_file_slices(
        &self,
        partition_pruner: &PartitionPruner,
        timeline_view: &TimelineView,
    ) -> Result<Vec<FileSlice>> {
        let timestamp = timeline_view.as_of_timestamp();
        let excluding_file_groups = timeline_view.excluding_file_groups();

        let mut file_slices = Vec::new();
        for partition_entry in self.partition_to_file_groups.iter() {
            if !partition_pruner.should_include(partition_entry.key()) {
                continue;
            }
            for fg in partition_entry.value().iter() {
                if excluding_file_groups.contains(fg) {
                    continue;
                }
                if let Some(fsl) = fg.get_file_slice_as_of(timestamp) {
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
    /// * `metadata_table` - Optional metadata table instance for file listing
    pub(crate) async fn get_file_slices(
        &self,
        partition_pruner: &PartitionPruner,
        file_pruner: &FilePruner,
        table_schema: &Schema,
        timeline_view: &TimelineView,
        metadata_table: Option<&Table>,
        estimator: Option<&FileStatsEstimator>,
    ) -> Result<Vec<FileSlice>> {
        let files_partition_records = if let Some(mdt) = metadata_table {
            Some(mdt.fetch_files_partition_records(partition_pruner).await?)
        } else {
            None
        };

        self.load_file_groups(
            partition_pruner,
            file_pruner,
            table_schema,
            timeline_view,
            files_partition_records.as_ref(),
            estimator,
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
        estimator: Option<&FileStatsEstimator>,
    ) -> Result<Vec<FileSlice>> {
        // Pass None to force storage listing (avoids recursion for metadata table)
        self.load_file_groups(
            partition_pruner,
            file_pruner,
            table_schema,
            timeline_view,
            None,
            estimator,
        )
        .await?;

        self.collect_file_slices(partition_pruner, timeline_view)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::config::HudiConfigs;
    use crate::config::table::BaseFileFormatValue;
    use crate::config::table::HudiTableConfig::BasePath;
    use crate::expr::filter::Filter;
    use crate::file_group::FileGroup;
    use crate::metadata::table::records::{
        FilesPartitionRecord, HoodieMetadataFileInfo, MetadataRecordType,
    };
    use crate::table::Table;
    use arrow_schema::Schema;
    use tempfile::tempdir;
    use url::Url;

    use hudi_test::{QuickstartTripsTable, SampleTable};

    async fn file_slices_as_of(hudi_table: &Table, timestamp: &str) -> Vec<FileSlice> {
        let fs_view = &hudi_table.file_system_view;
        let timeline_view = hudi_table
            .timeline
            .create_view_as_of(timestamp)
            .await
            .unwrap();
        let partition_pruner = PartitionPruner::empty();
        let file_pruner = FilePruner::empty();
        let table_schema = hudi_table.get_schema().await.unwrap();
        fs_view
            .get_file_slices(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
                None,
                None,
            )
            .await
            .unwrap()
    }

    async fn partition_counts_as_of(
        hudi_table: &Table,
        timestamp: &str,
    ) -> BTreeMap<String, usize> {
        let file_slices = file_slices_as_of(hudi_table, timestamp).await;

        let mut partition_counts = BTreeMap::new();
        for file_slice in file_slices {
            *partition_counts
                .entry(file_slice.partition_path.clone())
                .or_insert(0usize) += 1;
        }
        partition_counts
    }

    async fn new_fs_view_with_base_path_and_format(
        base_url: &Url,
        base_file_format: &str,
    ) -> FileSystemView {
        let hudi_configs = Arc::new(HudiConfigs::new([
            (BasePath.as_ref(), base_url.as_str()),
            (BaseFileFormat.as_ref(), base_file_format),
        ]));
        FileSystemView::new(hudi_configs, Arc::new(HashMap::new()))
            .await
            .unwrap()
    }

    fn create_files_partition_record(
        partition_key: &str,
        file_name: &str,
        file_size: i64,
    ) -> (String, FilesPartitionRecord) {
        let mut files = HashMap::new();
        files.insert(
            file_name.to_string(),
            HoodieMetadataFileInfo::new(file_name.to_string(), file_size, false),
        );
        (
            partition_key.to_string(),
            FilesPartitionRecord {
                key: partition_key.to_string(),
                record_type: MetadataRecordType::Files,
                files,
            },
        )
    }

    async fn build_file_pruning_context(
        filter: (&str, &str, &str),
    ) -> (Table, Schema, FilePruner, String) {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let table_schema = hudi_table.get_schema().await.unwrap();
        let partition_schema = hudi_table.get_partition_schema().await.unwrap();
        let filters = vec![Filter::try_from(filter).unwrap()];
        let file_pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();
        let as_of = hudi_table.timeline.get_latest_commit_timestamp().unwrap();
        (hudi_table, table_schema, file_pruner, as_of)
    }

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
            let metadata = fsl.base_file.file_metadata.as_ref().unwrap();
            assert!(metadata.size > 0);
        }
    }

    #[tokio::test]
    async fn fs_view_get_latest_file_slices_with_estimator_populates_estimated_metadata() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let latest_timestamp = hudi_table.timeline.get_latest_commit_timestamp().unwrap();
        let fs_view = &hudi_table.file_system_view;

        let timeline_view = hudi_table
            .timeline
            .create_view_as_of(&latest_timestamp)
            .await
            .unwrap();
        let partition_pruner = PartitionPruner::empty();
        let file_pruner = FilePruner::empty();
        let table_schema = hudi_table.get_schema().await.unwrap();
        let estimator = hudi_table.get_or_init_estimator(&latest_timestamp).await;
        assert!(
            estimator.is_some(),
            "Expected estimator for parquet sample table"
        );

        let file_slices = fs_view
            .get_file_slices(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
                None,
                estimator,
            )
            .await
            .unwrap();

        assert!(!file_slices.is_empty());
        for fsl in file_slices.iter() {
            let metadata = fsl.base_file.file_metadata.as_ref().unwrap();
            assert!(metadata.size > 0);
            assert!(metadata.byte_size > 0);
            assert!(metadata.num_records > 0);
        }
    }

    #[tokio::test]
    async fn fs_view_get_latest_file_slices_with_replace_commit() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyleOverwritetable.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let latest_timestamp = hudi_table.timeline.get_latest_commit_timestamp().unwrap();
        let fs_view = &hudi_table.file_system_view;

        assert_eq!(fs_view.partition_to_file_groups.len(), 0);

        let partition_counts = partition_counts_as_of(&hudi_table, &latest_timestamp).await;
        assert_eq!(
            partition_counts.values().sum::<usize>(),
            1,
            "Latest snapshot should have one surviving file slice after replacecommit pruning"
        );
        let file_slices = file_slices_as_of(&hudi_table, &latest_timestamp).await;

        assert_eq!(fs_view.partition_to_file_groups.len(), 3);
        assert_eq!(file_slices.len(), 1);
        let file_ids = file_slices
            .iter()
            .map(|fsl| fsl.file_id())
            .collect::<Vec<_>>();
        assert_eq!(file_ids, vec!["ebcb261d-62d3-4895-90ec-5b3c9622dff4-0"]);
        for fsl in file_slices.iter() {
            let metadata = fsl.base_file.file_metadata.as_ref().unwrap();
            assert!(metadata.size > 0);
        }
    }

    #[tokio::test]
    async fn fs_view_get_file_slices_with_v9_replace_commit() {
        let base_url = SampleTable::V9TxnsSimpleOverwrite.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let latest_timestamp = hudi_table.timeline.get_latest_commit_timestamp().unwrap();
        let latest_counts = partition_counts_as_of(&hudi_table, &latest_timestamp).await;
        assert_eq!(
            latest_counts,
            BTreeMap::from([
                ("apac".to_string(), 1usize),
                ("eu".to_string(), 1usize),
                ("us".to_string(), 1usize),
            ])
        );

        let commits = hudi_table
            .timeline
            .get_completed_commits(false)
            .await
            .unwrap();
        assert_eq!(commits.len(), 2);
        let partition_counts = partition_counts_as_of(&hudi_table, &commits[1].timestamp).await;
        assert_eq!(
            partition_counts,
            BTreeMap::from([
                ("apac".to_string(), 1usize),
                ("eu".to_string(), 2usize),
                ("us".to_string(), 2usize),
            ])
        );

        let replacecommits = hudi_table
            .timeline
            .get_completed_replacecommits(false)
            .await
            .unwrap();
        assert_eq!(replacecommits.len(), 1);
        let replace_counts =
            partition_counts_as_of(&hudi_table, &replacecommits[0].timestamp).await;
        assert_eq!(replace_counts, latest_counts);
    }

    #[tokio::test]
    async fn fs_view_load_file_groups_from_metadata_records() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let latest_timestamp = hudi_table.timeline.get_latest_commit_timestamp().unwrap();
        let timeline_view = hudi_table
            .timeline
            .create_view_as_of(&latest_timestamp)
            .await
            .unwrap();
        let partition_pruner = PartitionPruner::empty();
        let file_pruner = FilePruner::empty();
        let table_schema = hudi_table.get_schema().await.unwrap();
        let file_name =
            "a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet";
        let records = HashMap::from([create_files_partition_record("", file_name, 1024)]);

        hudi_table
            .file_system_view
            .load_file_groups(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
                Some(&records),
                None,
            )
            .await
            .unwrap();

        assert!(
            !hudi_table
                .file_system_view
                .partition_to_file_groups
                .is_empty()
        );
    }

    #[tokio::test]
    async fn fs_view_apply_stats_pruning_returns_early_for_non_parquet_base_format() {
        let temp_dir = tempdir().unwrap();
        let base_url = Url::from_directory_path(temp_dir.path()).unwrap();
        let fs_view =
            new_fs_view_with_base_path_and_format(&base_url, BaseFileFormatValue::HFile.as_ref())
                .await;

        let table_base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let table = Table::new(table_base_url.path()).await.unwrap();
        let table_schema = table.get_schema().await.unwrap();
        let partition_schema = table.get_partition_schema().await.unwrap();
        let filters = vec![Filter::try_from(("intField", ">=", "0")).unwrap()];
        let file_pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();
        assert!(!file_pruner.is_empty());
        let as_of = table.timeline.get_latest_commit_timestamp().unwrap();

        let file_group =
            FileGroup::new_with_base_file_name("fileid_0-0-1_20240418173551906.parquet", "")
                .unwrap();
        let retained = fs_view
            .apply_stats_pruning_from_footers(vec![file_group], &file_pruner, &table_schema, &as_of)
            .await;

        assert_eq!(retained.len(), 1);
    }

    #[tokio::test]
    async fn fs_view_apply_stats_pruning_keeps_non_matching_extension_files() {
        let (table, table_schema, file_pruner, as_of) =
            build_file_pruning_context(("intField", ">=", "0")).await;

        let file_group =
            FileGroup::new_with_base_file_name("fileid_0-0-1_20240418173551906.hfile", "").unwrap();
        let retained = table
            .file_system_view
            .apply_stats_pruning_from_footers(vec![file_group], &file_pruner, &table_schema, &as_of)
            .await;

        assert_eq!(retained.len(), 1);
    }

    #[tokio::test]
    async fn fs_view_get_file_slices_with_non_empty_file_pruner() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let latest_timestamp = hudi_table.timeline.get_latest_commit_timestamp().unwrap();
        let timeline_view = hudi_table
            .timeline
            .create_view_as_of(&latest_timestamp)
            .await
            .unwrap();
        let partition_schema = hudi_table.get_partition_schema().await.unwrap();
        let table_schema = hudi_table.get_schema().await.unwrap();
        let filters = vec![Filter::try_from(("intField", ">=", "0")).unwrap()];
        let partition_pruner = PartitionPruner::new(
            &filters,
            &partition_schema,
            hudi_table.hudi_configs.as_ref(),
        )
        .unwrap();
        let file_pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();
        assert!(!file_pruner.is_empty());

        let file_slices = hudi_table
            .file_system_view
            .get_file_slices(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
                None,
                None,
            )
            .await
            .unwrap();

        assert!(!file_slices.is_empty());
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
            let metadata = fsl.base_file.file_metadata.as_ref().unwrap();
            assert!(metadata.size > 0);
        }
    }

    #[tokio::test]
    async fn fs_view_load_file_groups_from_metadata_records_respects_partition_pruner() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyle.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let latest_timestamp = hudi_table.timeline.get_latest_commit_timestamp().unwrap();
        let timeline_view = hudi_table
            .timeline
            .create_view_as_of(&latest_timestamp)
            .await
            .unwrap();
        let partition_schema = hudi_table.get_partition_schema().await.unwrap();
        let table_schema = hudi_table.get_schema().await.unwrap();
        let partition_filters = vec![Filter::try_from(("byteField", "=", "10")).unwrap()];
        let partition_pruner = PartitionPruner::new(
            &partition_filters,
            &partition_schema,
            hudi_table.hudi_configs.as_ref(),
        )
        .unwrap();
        let file_pruner = FilePruner::empty();

        let file_name_10 =
            "97de74b1-2a8e-4bb7-874c-0a74e1f42a77-0_0-119-166_20240418172804498.parquet";
        let file_name_20 =
            "76e0556b-390d-4249-b7ad-9059e2bc2cbd-0_0-98-141_20240418172802262.parquet";
        let records = HashMap::from([
            create_files_partition_record("10", file_name_10, 1024),
            create_files_partition_record("20", file_name_20, 1024),
        ]);

        hudi_table
            .file_system_view
            .load_file_groups(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
                Some(&records),
                None,
            )
            .await
            .unwrap();

        assert!(
            hudi_table
                .file_system_view
                .partition_to_file_groups
                .contains_key("10")
        );
        assert!(
            !hudi_table
                .file_system_view
                .partition_to_file_groups
                .contains_key("20")
        );
    }

    #[tokio::test]
    async fn fs_view_apply_stats_pruning_keeps_file_when_footer_stats_fail_to_load() {
        let (hudi_table, table_schema, file_pruner, as_of) =
            build_file_pruning_context(("intField", ">=", "0")).await;

        let missing_file_group = FileGroup::new_with_base_file_name(
            "a079bdb3-731c-4894-b855-abfcd6921007-0_0-999-999_20240418173551906.parquet",
            "",
        )
        .unwrap();
        let retained = hudi_table
            .file_system_view
            .apply_stats_pruning_from_footers(
                vec![missing_file_group],
                &file_pruner,
                &table_schema,
                &as_of,
            )
            .await;

        assert_eq!(retained.len(), 1);
    }

    #[tokio::test]
    async fn fs_view_apply_stats_pruning_drops_files_that_do_not_match_filters() {
        let (hudi_table, table_schema, file_pruner, as_of) =
            build_file_pruning_context(("intField", ">", "1000000")).await;

        let file_group = FileGroup::new_with_base_file_name(
            "a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",
            "",
        )
        .unwrap();
        let retained = hudi_table
            .file_system_view
            .apply_stats_pruning_from_footers(vec![file_group], &file_pruner, &table_schema, &as_of)
            .await;

        assert!(retained.is_empty());
    }

    #[tokio::test]
    async fn fs_view_apply_stats_pruning_keeps_file_group_without_slice_as_of_timestamp() {
        let (hudi_table, table_schema, file_pruner, _) =
            build_file_pruning_context(("intField", ">", "1000000")).await;

        let file_group = FileGroup::new_with_base_file_name(
            "a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",
            "",
        )
        .unwrap();
        let retained = hudi_table
            .file_system_view
            .apply_stats_pruning_from_footers(
                vec![file_group],
                &file_pruner,
                &table_schema,
                "19700101000000",
            )
            .await;

        assert_eq!(retained.len(), 1);
    }

    #[tokio::test]
    async fn fs_view_get_file_slices_with_metadata_table() {
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let hudi_table = Table::new(&table_path).await.unwrap();
        let latest_timestamp = hudi_table.timeline.get_latest_commit_timestamp().unwrap();
        let timeline_view = hudi_table
            .timeline
            .create_view_as_of(&latest_timestamp)
            .await
            .unwrap();
        let partition_schema = hudi_table.get_partition_schema().await.unwrap();
        let partition_pruner =
            PartitionPruner::new(&[], &partition_schema, hudi_table.hudi_configs.as_ref()).unwrap();
        let file_pruner = FilePruner::empty();
        let table_schema = hudi_table.get_schema().await.unwrap();
        let metadata_table = hudi_table.get_or_init_metadata_table().await.unwrap();

        let file_slices = hudi_table
            .file_system_view
            .get_file_slices(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
                Some(metadata_table),
                None,
            )
            .await
            .unwrap();

        assert!(!file_slices.is_empty());
    }

    #[tokio::test]
    async fn fs_view_get_file_slices_by_storage_listing() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let latest_timestamp = hudi_table.timeline.get_latest_commit_timestamp().unwrap();
        let timeline_view = hudi_table
            .timeline
            .create_view_as_of(&latest_timestamp)
            .await
            .unwrap();
        let partition_pruner = PartitionPruner::empty();
        let file_pruner = FilePruner::empty();
        let table_schema = hudi_table.get_schema().await.unwrap();

        let file_slices = hudi_table
            .file_system_view
            .get_file_slices_by_storage_listing(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
                None,
            )
            .await
            .unwrap();

        assert_eq!(file_slices.len(), 1);
    }
}
