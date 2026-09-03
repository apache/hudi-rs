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
use crate::Result;
use crate::config::HudiConfigs;
use crate::config::plan::HudiPlanConfig::ListingParallelism;
use crate::config::table::BaseFileFormatValue;
use crate::error::CoreError;
use crate::file_group::FileGroup;
use crate::file_group::base_file::BaseFile;
use crate::file_group::log_file::LogFile;
use crate::metadata::LAKE_FORMAT_METADATA_DIRS;
use crate::statistics::estimator::FileStatsEstimator;
use crate::storage::{Storage, get_leaf_dirs};
use crate::table::partition::{
    EMPTY_PARTITION_PATH, PARTITION_METAFIELD_PREFIX, PartitionPruner, is_table_partitioned,
};
use crate::timeline::completion_time::CompletionTimeView;
use dashmap::DashMap;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, stream};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct FileLister {
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
    partition_pruner: PartitionPruner,
}

impl FileLister {
    pub fn new(
        hudi_configs: Arc<HudiConfigs>,
        storage: Arc<Storage>,
        partition_pruner: PartitionPruner,
    ) -> Self {
        Self {
            hudi_configs,
            storage,
            partition_pruner,
        }
    }

    fn should_exclude_for_listing(file_name: &str) -> bool {
        file_name.starts_with(PARTITION_METAFIELD_PREFIX)
    }

    /// List file groups for a partition, setting completion timestamps from the view.
    ///
    /// # Arguments
    /// * `partition_path` - The partition path to list files from
    /// * `completion_time_view` - View to look up completion timestamps.
    ///
    /// Files whose commit timestamps are not found in the completion time view
    /// (i.e., uncommitted files) will have `completion_timestamp = None`.
    async fn list_file_groups_for_partition<V: CompletionTimeView>(
        &self,
        partition_path: &str,
        completion_time_view: &V,
        estimator: Option<&FileStatsEstimator>,
    ) -> Result<Vec<FileGroup>> {
        let configured_base_file_format = BaseFileFormatValue::from_configs(&self.hudi_configs)?;

        let listed_file_metadata = self.storage.list_files(Some(partition_path)).await?;

        let mut file_id_to_base_files: HashMap<String, Vec<BaseFile>> = HashMap::new();
        let mut file_id_to_log_files: HashMap<String, Vec<LogFile>> = HashMap::new();

        for file_metadata in listed_file_metadata {
            if FileLister::should_exclude_for_listing(&file_metadata.name) {
                continue;
            }

            let is_base_file = configured_base_file_format.as_ref().map_or_else(
                || BaseFileFormatValue::from_extension(&file_metadata.name).is_some(),
                |format| format.matches_extension(&file_metadata.name),
            );
            if is_base_file {
                // After excluding the unintended files,
                // we expect a file that has the base file extension to be a valid base file.
                let mut base_file = BaseFile::try_from(file_metadata)?;

                // Look up completion timestamp, and skip the file outright if the
                // commit that wrote it never completed.
                base_file.set_completion_time(completion_time_view);
                if !completion_time_view.is_committed(&base_file.commit_timestamp) {
                    continue;
                }
                if let Some(metadata) = base_file.file_metadata.as_mut() {
                    // Populate estimated stats for storage-listing paths so
                    // snapshot/time-travel metadata matches MDT-backed paths.
                    if metadata.size > 0 {
                        let (byte_size, num_records) = estimator
                            .map(|e| e.estimate(metadata.size))
                            .unwrap_or((0, 0));
                        metadata.byte_size = byte_size;
                        metadata.num_records = num_records;
                    }
                }

                let file_id = &base_file.file_id;
                file_id_to_base_files
                    .entry(file_id.to_owned())
                    .or_default()
                    .push(base_file);
            } else {
                match LogFile::try_from(file_metadata) {
                    Ok(mut log_file) => {
                        // Look up completion timestamp, and skip the file outright
                        // if the commit that wrote it never completed.
                        log_file.set_completion_time(completion_time_view);
                        if !completion_time_view.is_committed(&log_file.timestamp) {
                            continue;
                        }

                        let file_id = &log_file.file_id;
                        file_id_to_log_files
                            .entry(file_id.to_owned())
                            .or_default()
                            .push(log_file);
                    }
                    Err(e) => {
                        // We don't support cdc log files yet, hence skipping error when parsing
                        // fails. However, once we support all data files, we should return error
                        // here because we expect all files to be either base files or log files,
                        // after excluding the unintended files.
                        log::warn!("Failed to create a log file: {e}");
                        continue;
                    }
                }
            }
        }

        let mut file_groups: Vec<FileGroup> = Vec::new();
        for (file_id, base_files) in file_id_to_base_files.into_iter() {
            let mut file_group = FileGroup::new(file_id.to_owned(), partition_path.to_string());

            file_group.add_base_files(base_files)?;

            let log_files = file_id_to_log_files.remove(&file_id).unwrap_or_default();
            file_group.add_log_files(log_files)?;

            file_groups.push(file_group);
        }

        // Whatever log files are left belong to file groups with no base file:
        // inserts that went straight to a log file, which is what Flink
        // ingestion, a bucket index's first write to a bucket, and any
        // merge-on-read file group before its first compaction all produce.
        // Dropping them loses every record in the group.
        for (file_id, log_files) in file_id_to_log_files.into_iter() {
            let mut file_group = FileGroup::new(file_id, partition_path.to_string());
            file_group.add_log_files(log_files)?;
            file_groups.push(file_group);
        }
        Ok(file_groups)
    }

    async fn list_relevant_partition_paths(&self) -> Result<Vec<String>> {
        if !is_table_partitioned(&self.hudi_configs)? {
            return Ok(vec![EMPTY_PARTITION_PATH.to_string()]);
        }

        let top_level_dirs: Vec<String> = self
            .storage
            .list_dirs(None)
            .await?
            .into_iter()
            .filter(|dir| !LAKE_FORMAT_METADATA_DIRS.contains(&dir.as_str()))
            .collect();

        let mut partition_paths = Vec::new();
        for dir in top_level_dirs {
            partition_paths.extend(get_leaf_dirs(&self.storage, Some(&dir)).await?);
        }

        if partition_paths.is_empty() || self.partition_pruner.is_empty() {
            return Ok(partition_paths);
        }

        Ok(partition_paths
            .into_iter()
            .filter(|path_str| self.partition_pruner.should_include(path_str))
            .collect())
    }

    /// Discover relevant partitions from the object store's recursive listing
    /// without waiting to materialize the table-wide partition path vector.
    ///
    /// A set of emitted partition paths is retained because object stores do
    /// not guarantee that objects from the same partition are contiguous. The
    /// set holds one path string per partition; file metadata, file groups, and
    /// slices remain bounded by downstream concurrency.
    fn list_relevant_partition_paths_stream(&self) -> Result<BoxStream<'static, Result<String>>> {
        if !is_table_partitioned(&self.hudi_configs)? {
            return Ok(stream::once(async { Ok(EMPTY_PARTITION_PATH.to_string()) }).boxed());
        }

        let paths = self.storage.list_relative_paths_stream()?;
        let partition_pruner = self.partition_pruner.clone();
        Ok(
            stream::try_unfold((paths, HashSet::new()), move |(mut paths, mut seen)| {
                let partition_pruner = partition_pruner.clone();
                async move {
                    while let Some(relative_path) = paths.try_next().await? {
                        let first_segment = relative_path.split('/').next().unwrap_or_default();
                        if LAKE_FORMAT_METADATA_DIRS.contains(&first_segment) {
                            continue;
                        }

                        let Some((partition_path, _)) = relative_path.rsplit_once('/') else {
                            continue;
                        };
                        if (partition_pruner.is_empty()
                            || partition_pruner.should_include(partition_path))
                            && seen.insert(partition_path.to_string())
                        {
                            return Ok(Some((partition_path.to_string(), (paths, seen))));
                        }
                    }
                    Ok(None)
                }
            })
            .boxed(),
        )
    }

    /// List file groups for all relevant partitions.
    ///
    /// # Arguments
    /// * `completion_time_view` - View to look up completion timestamps.
    pub async fn list_file_groups_for_relevant_partitions<V: CompletionTimeView + Sync>(
        &self,
        completion_time_view: &V,
        estimator: Option<&FileStatsEstimator>,
    ) -> Result<DashMap<String, Vec<FileGroup>>> {
        if !is_table_partitioned(&self.hudi_configs)? {
            let file_groups = self
                .list_file_groups_for_partition(
                    EMPTY_PARTITION_PATH,
                    completion_time_view,
                    estimator,
                )
                .await?;
            let file_groups_map = DashMap::with_capacity(1);
            file_groups_map.insert(EMPTY_PARTITION_PATH.to_string(), file_groups);
            return Ok(file_groups_map);
        }

        let pruned_partition_paths = self.list_relevant_partition_paths().await?;
        let file_groups_map = Arc::new(DashMap::with_capacity(pruned_partition_paths.len()));
        let parallelism = self.hudi_configs.get_or_default(ListingParallelism).into();
        stream::iter(pruned_partition_paths)
            .map(|p| async move {
                let file_groups = self
                    .list_file_groups_for_partition(&p, completion_time_view, estimator)
                    .await?;
                Ok::<_, CoreError>((p, file_groups))
            })
            .buffer_unordered(parallelism)
            .try_for_each(|(p, file_groups)| {
                let file_groups_map = file_groups_map.clone();
                async move {
                    file_groups_map.insert(p, file_groups);
                    Ok(())
                }
            })
            .await?;

        Ok(file_groups_map.as_ref().to_owned())
    }

    /// Stream file groups one partition at a time.
    ///
    /// Partition discovery and per-partition file listing are both deferred
    /// until the returned stream is polled. The recursive discovery listing
    /// intentionally does not reuse its individual file entries: object stores
    /// do not guarantee per-partition contiguity, so doing so correctly would
    /// retain incomplete file groups for the whole table. Each newly discovered
    /// partition therefore gets one delimiter listing to obtain its complete
    /// file set. At most
    /// `hoodie.plan.listing.parallelism` partition listings are in flight, so
    /// callers can consume completed partitions without retaining every table
    /// file group or slice in memory.
    pub async fn list_file_groups_for_relevant_partitions_stream<
        V: CompletionTimeView + Send + Sync + 'static,
    >(
        &self,
        completion_time_view: Arc<V>,
        estimator: Option<FileStatsEstimator>,
    ) -> Result<BoxStream<'static, Result<(String, Vec<FileGroup>)>>> {
        let partition_paths = self.list_relevant_partition_paths_stream()?;
        let parallelism = self.hudi_configs.get_or_default(ListingParallelism).into();
        let lister = self.clone();

        Ok(partition_paths
            .map_ok(move |partition_path| {
                let lister = lister.clone();
                let completion_time_view = completion_time_view.clone();
                let estimator = estimator.clone();
                async move {
                    let file_groups = lister
                        .list_file_groups_for_partition(
                            &partition_path,
                            completion_time_view.as_ref(),
                            estimator.as_ref(),
                        )
                        .await?;
                    Ok::<_, CoreError>((partition_path, file_groups))
                }
            })
            .try_buffer_unordered(parallelism)
            .boxed())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::table::HudiTableConfig::BasePath;
    use crate::table::Table;
    use crate::timeline::view::TimelineView;
    use hudi_test::SampleTable;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use tempfile::tempdir;
    use url::Url;

    /// A view that admits every file, so these tests stay about extension
    /// handling rather than commit visibility: the far-future archival boundary
    /// puts every instant below it, i.e. archived and therefore committed.
    /// Commit visibility is covered in `timeline::view`.
    fn layout_v1_view() -> TimelineView {
        TimelineView::new_with_archival_boundary(
            "99999999999999999".to_string(),
            None,
            &[],
            HashSet::new(),
            &Arc::new(HudiConfigs::new([("hoodie.timeline.layout.version", "1")])),
            Some("99999999999999999".to_string()),
        )
    }

    #[tokio::test]
    async fn list_partition_paths_for_nonpartitioned_table() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let lister = FileLister::new(
            hudi_table.hudi_configs.clone(),
            hudi_table.file_system_view.storage.clone(),
            PartitionPruner::empty(),
        );
        let partition_paths = lister.list_relevant_partition_paths().await.unwrap();
        let partition_path_set: HashSet<&str> =
            HashSet::from_iter(partition_paths.iter().map(|p| p.as_str()));
        assert_eq!(partition_path_set, HashSet::from([""]))
    }

    #[tokio::test]
    async fn list_partition_paths_for_complexkeygen_table() {
        let base_url = SampleTable::V6ComplexkeygenHivestyle.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let fs_view = &hudi_table.file_system_view;
        let lister = FileLister::new(
            fs_view.hudi_configs.clone(),
            fs_view.storage.clone(),
            PartitionPruner::empty(),
        );
        let partition_paths = lister.list_relevant_partition_paths().await.unwrap();
        let partition_path_set: HashSet<&str> =
            HashSet::from_iter(partition_paths.iter().map(|p| p.as_str()));
        assert_eq!(
            partition_path_set,
            HashSet::from_iter(vec![
                "byteField=10/shortField=300",
                "byteField=20/shortField=100",
                "byteField=30/shortField=100"
            ])
        )
    }

    #[tokio::test]
    async fn list_file_groups_uses_extension_fallback_when_format_config_is_absent() {
        let temp_dir = tempdir().unwrap();
        std::fs::write(
            temp_dir
                .path()
                .join("file-id-0_0-7-24_20240418173200000.lance"),
            [],
        )
        .unwrap();
        std::fs::write(
            temp_dir
                .path()
                .join("file-id-1_0-8-25_20240418173210000.parquet"),
            [],
        )
        .unwrap();
        std::fs::write(temp_dir.path().join("ignored.txt"), []).unwrap();

        let base_url = Url::from_directory_path(temp_dir.path()).unwrap();
        let hudi_configs = Arc::new(HudiConfigs::new([(BasePath.as_ref(), base_url.as_str())]));
        let storage = Storage::new(Arc::new(HashMap::new()), hudi_configs.clone()).unwrap();
        let lister = FileLister::new(hudi_configs, storage, PartitionPruner::empty());
        let view = layout_v1_view();

        let file_groups = lister
            .list_file_groups_for_partition(EMPTY_PARTITION_PATH, &view, None)
            .await
            .unwrap();
        let extensions: HashSet<_> = file_groups
            .iter()
            .flat_map(|fg| fg.file_slices.values())
            .map(|slice| slice.base_file.as_ref().unwrap().extension.as_str())
            .collect();

        assert_eq!(extensions, HashSet::from(["lance", "parquet"]));
    }
}
