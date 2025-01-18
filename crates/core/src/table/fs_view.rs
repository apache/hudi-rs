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

use crate::config::HudiConfigs;
use crate::file_group::base_file::BaseFile;
use crate::file_group::FileGroup;
use crate::storage::{get_leaf_dirs, Storage};

use crate::config::read::HudiReadConfig::ListingParallelism;
use crate::config::table::HudiTableConfig::BaseFileFormat;
use crate::error::CoreError;
use crate::file_group::file_slice::FileSlice;
use crate::file_group::log_file::LogFile;
use crate::table::partition::{PartitionPruner, PARTITION_METAFIELD_PREFIX};
use crate::Result;
use dashmap::DashMap;
use futures::stream::{self, StreamExt, TryStreamExt};

/// A view of the Hudi table's data files (files stored outside the `.hoodie/` directory) in the file system. It provides APIs to load and
/// access the file groups and file slices.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct FileSystemView {
    hudi_configs: Arc<HudiConfigs>,
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

    fn should_exclude_for_listing(file_name: &str) -> bool {
        file_name.starts_with(PARTITION_METAFIELD_PREFIX) || file_name.ends_with(".crc")
    }

    async fn list_all_partition_paths(storage: &Storage) -> Result<Vec<String>> {
        Self::list_partition_paths(storage, &PartitionPruner::empty()).await
    }

    async fn list_partition_paths(
        storage: &Storage,
        partition_pruner: &PartitionPruner,
    ) -> Result<Vec<String>> {
        let top_level_dirs: Vec<String> = storage
            .list_dirs(None)
            .await?
            .into_iter()
            .filter(|dir| dir != ".hoodie")
            .collect();
        let mut partition_paths = Vec::new();
        for dir in top_level_dirs {
            partition_paths.extend(get_leaf_dirs(storage, Some(&dir)).await?);
        }
        if partition_paths.is_empty() {
            partition_paths.push("".to_string())
        }
        if partition_pruner.is_empty() {
            return Ok(partition_paths);
        }

        Ok(partition_paths
            .into_iter()
            .filter(|path_str| partition_pruner.should_include(path_str))
            .collect())
    }

    async fn list_file_groups_for_partition(
        storage: &Storage,
        partition_path: &str,
        base_file_format: &str,
    ) -> Result<Vec<FileGroup>> {
        let listed_file_metadata = storage.list_files(Some(partition_path)).await?;

        let mut file_id_to_base_files: HashMap<String, Vec<BaseFile>> = HashMap::new();
        let mut file_id_to_log_files: HashMap<String, Vec<LogFile>> = HashMap::new();

        for file_metadata in listed_file_metadata {
            if Self::should_exclude_for_listing(&file_metadata.name) {
                continue;
            }

            let base_file_extension = format!(".{}", base_file_format);
            if file_metadata.name.ends_with(&base_file_extension) {
                // After excluding the unintended files,
                // we expect a file that has the base file extension to be a valid base file.
                let base_file = BaseFile::try_from(file_metadata)?;
                let file_id = &base_file.file_id;
                file_id_to_base_files
                    .entry(file_id.to_owned())
                    .or_default()
                    .push(base_file);
            } else {
                match LogFile::try_from(file_metadata) {
                    Ok(log_file) => {
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
                        log::warn!("Failed to create a log file: {}", e);
                        continue;
                    }
                }
            }
        }

        let mut file_groups: Vec<FileGroup> = Vec::new();
        // TODO support creating file groups without base files
        for (file_id, base_files) in file_id_to_base_files.into_iter() {
            let mut file_group =
                FileGroup::new(file_id.to_owned(), Some(partition_path.to_owned()));

            file_group.add_base_files(base_files)?;

            let log_files = file_id_to_log_files.remove(&file_id).unwrap_or_default();
            file_group.add_log_files(log_files)?;

            file_groups.push(file_group);
        }
        Ok(file_groups)
    }

    async fn load_file_groups(&self, partition_pruner: &PartitionPruner) -> Result<()> {
        let all_partition_paths = Self::list_all_partition_paths(&self.storage).await?;

        let partition_paths_to_list = all_partition_paths
            .into_iter()
            .filter(|p| !self.partition_to_file_groups.contains_key(p))
            .filter(|p| partition_pruner.should_include(p))
            .collect::<HashSet<_>>();

        let base_file_format = self
            .hudi_configs
            .get_or_default(BaseFileFormat)
            .to::<String>();
        let parallelism = self
            .hudi_configs
            .get_or_default(ListingParallelism)
            .to::<usize>();
        stream::iter(partition_paths_to_list)
            .map(|path| {
                let base_file_format = base_file_format.clone();
                async move {
                    let format = base_file_format.as_str();
                    let file_groups =
                        Self::list_file_groups_for_partition(&self.storage, &path, format).await?;
                    Ok::<_, CoreError>((path, file_groups))
                }
            })
            .buffer_unordered(parallelism)
            .try_for_each(|(path, file_groups)| async move {
                self.partition_to_file_groups.insert(path, file_groups);
                Ok(())
            })
            .await
    }

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

    pub async fn get_file_slices_as_of(
        &self,
        timestamp: &str,
        partition_pruner: &PartitionPruner,
        excluding_file_groups: &HashSet<FileGroup>,
    ) -> Result<Vec<FileSlice>> {
        self.load_file_groups(partition_pruner).await?;
        self.collect_file_slices_as_of(timestamp, partition_pruner, excluding_file_groups)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table::HudiTableConfig;
    use crate::config::HudiConfigs;
    use crate::expr::filter::Filter;
    use crate::storage::Storage;
    use crate::table::fs_view::FileSystemView;
    use crate::table::partition::PartitionPruner;
    use crate::table::Table;

    use hudi_tests::SampleTable;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use url::Url;

    async fn create_test_fs_view(base_url: Url) -> FileSystemView {
        FileSystemView::new(
            Arc::new(HudiConfigs::new([(HudiTableConfig::BasePath, base_url)])),
            Arc::new(HashMap::new()),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn get_partition_paths_for_nonpartitioned_table() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let partition_pruner = PartitionPruner::empty();
        let partition_paths = FileSystemView::list_partition_paths(&storage, &partition_pruner)
            .await
            .unwrap();
        let partition_path_set: HashSet<&str> =
            HashSet::from_iter(partition_paths.iter().map(|p| p.as_str()));
        assert_eq!(partition_path_set, HashSet::from([""]))
    }

    #[tokio::test]
    async fn get_partition_paths_for_complexkeygen_table() {
        let base_url = SampleTable::V6ComplexkeygenHivestyle.url_to_cow();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let partition_pruner = PartitionPruner::empty();
        let partition_paths = FileSystemView::list_partition_paths(&storage, &partition_pruner)
            .await
            .unwrap();
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
    async fn fs_view_get_latest_file_slices() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let fs_view = create_test_fs_view(base_url).await;

        assert!(fs_view.partition_to_file_groups.is_empty());
        let partition_pruner = PartitionPruner::empty();
        let excludes = HashSet::new();
        let file_slices = fs_view
            .get_file_slices_as_of("20240418173551906", &partition_pruner, &excludes)
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
        let fs_view = create_test_fs_view(base_url).await;

        assert_eq!(fs_view.partition_to_file_groups.len(), 0);
        let partition_pruner = PartitionPruner::empty();
        let excludes = &hudi_table
            .timeline
            .get_replaced_file_groups()
            .await
            .unwrap();
        let file_slices = fs_view
            .get_file_slices_as_of("20240707001303088", &partition_pruner, excludes)
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
        let fs_view = create_test_fs_view(base_url).await;

        assert_eq!(fs_view.partition_to_file_groups.len(), 0);

        let excludes = &hudi_table
            .timeline
            .get_replaced_file_groups()
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
            .get_file_slices_as_of("20240418173235694", &partition_pruner, excludes)
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
