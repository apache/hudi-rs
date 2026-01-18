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
use crate::config::read::HudiReadConfig::ListingParallelism;
use crate::config::table::HudiTableConfig::BaseFileFormat;
use crate::error::CoreError;
use crate::file_group::FileGroup;
use crate::file_group::base_file::BaseFile;
use crate::file_group::log_file::LogFile;
use crate::metadata::LAKE_FORMAT_METADATA_DIRS;
use crate::storage::{Storage, get_leaf_dirs};
use crate::table::{
    EMPTY_PARTITION_PATH, PARTITION_METAFIELD_PREFIX, PartitionPruner, is_table_partitioned,
};
use crate::timeline::completion_time::CompletionTimeView;
use dashmap::DashMap;
use futures::{StreamExt, TryStreamExt, stream};
use std::collections::HashMap;
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
    ) -> Result<Vec<FileGroup>> {
        let base_file_format: String = self.hudi_configs.get_or_default(BaseFileFormat).into();

        let listed_file_metadata = self.storage.list_files(Some(partition_path)).await?;

        let mut file_id_to_base_files: HashMap<String, Vec<BaseFile>> = HashMap::new();
        let mut file_id_to_log_files: HashMap<String, Vec<LogFile>> = HashMap::new();

        for file_metadata in listed_file_metadata {
            if FileLister::should_exclude_for_listing(&file_metadata.name) {
                continue;
            }

            let base_file_extension = format!(".{base_file_format}");
            if file_metadata.name.ends_with(&base_file_extension) {
                // After excluding the unintended files,
                // we expect a file that has the base file extension to be a valid base file.
                let mut base_file = BaseFile::try_from(file_metadata)?;

                // Look up completion timestamp and filter uncommitted files if applicable
                base_file.set_completion_time(completion_time_view);
                if completion_time_view.should_filter_uncommitted()
                    && base_file.completion_timestamp.is_none()
                {
                    // file belongs to an uncommitted commit, skip it
                    continue;
                }

                let file_id = &base_file.file_id;
                file_id_to_base_files
                    .entry(file_id.to_owned())
                    .or_default()
                    .push(base_file);
            } else {
                match LogFile::try_from(file_metadata) {
                    Ok(mut log_file) => {
                        // Look up completion timestamp and filter uncommitted files if applicable
                        log_file.set_completion_time(completion_time_view);
                        if completion_time_view.should_filter_uncommitted()
                            && log_file.completion_timestamp.is_none()
                        {
                            // file belongs to an uncommitted commit, skip it
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
        // TODO support creating file groups without base files
        for (file_id, base_files) in file_id_to_base_files.into_iter() {
            let mut file_group = FileGroup::new(file_id.to_owned(), partition_path.to_string());

            file_group.add_base_files(base_files)?;

            let log_files = file_id_to_log_files.remove(&file_id).unwrap_or_default();
            file_group.add_log_files(log_files)?;

            file_groups.push(file_group);
        }
        Ok(file_groups)
    }

    async fn list_relevant_partition_paths(&self) -> Result<Vec<String>> {
        if !is_table_partitioned(&self.hudi_configs) {
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

    /// List file groups for all relevant partitions.
    ///
    /// # Arguments
    /// * `completion_time_view` - View to look up completion timestamps.
    pub async fn list_file_groups_for_relevant_partitions<V: CompletionTimeView + Sync>(
        &self,
        completion_time_view: &V,
    ) -> Result<DashMap<String, Vec<FileGroup>>> {
        if !is_table_partitioned(&self.hudi_configs) {
            let file_groups = self
                .list_file_groups_for_partition(EMPTY_PARTITION_PATH, completion_time_view)
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
                    .list_file_groups_for_partition(&p, completion_time_view)
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
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::table::Table;
    use hudi_test::SampleTable;
    use std::collections::HashSet;

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
}
