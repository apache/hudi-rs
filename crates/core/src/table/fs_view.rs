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

use crate::config::HudiConfigs;
use crate::file_group::base_file::BaseFile;
use crate::file_group::{FileGroup, FileSlice};
use crate::storage::{get_leaf_dirs, Storage};
use async_recursion::async_recursion;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use crate::config::read::HudiReadConfig::ListingParallelism;
use crate::error::CoreError;
use crate::storage::file_metadata::FileMetadata;
use crate::table::partition::PartitionPruner;
use crate::Result;
use dashmap::DashMap;
use futures::stream::{self, StreamExt, TryStreamExt};

pub const HOODIE_PARTITION_METAFILE_NAME: &str = ".hoodie_partition_metadata";

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

    #[allow(dead_code)]
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
            let leaf_paths = if partition_pruner.is_empty() {
                // full dirs listing
                get_leaf_dirs(storage, Some(&dir)).await?
            } else {
                // leveled pruning
                Self::list_partition_paths_with_leveled_pruning(storage, dir, partition_pruner, 0)
                    .await?
            };
            partition_paths.extend(leaf_paths);
        }
        if partition_paths.is_empty() {
            // TODO: reconsider is it reasonable to add empty partition path? For partitioned table, we should return empty vec rather than vec with empty string
            partition_paths.push("".to_string())
        }

        Ok(partition_paths.into_iter().collect())
    }

    #[async_recursion]
    async fn list_partition_paths_with_leveled_pruning(
        storage: &Storage,
        path: String,
        partition_pruner: &PartitionPruner,
        current_level: usize,
    ) -> Result<Vec<String>> {
        // TODO: consider stop iterating when visit the `partition_metadata` file
        let mut leaf_matched_dirs = Vec::new();
        // 1. Check if the current level path can be pruned
        if !partition_pruner.should_include_with_level(path.as_str(), current_level) {
            // if current level path can be pruned, return empty list
            return Ok(leaf_matched_dirs);
        }
        // 2. Iterate over all child directories and keep listing with pruning
        let child_dirs = storage.list_dirs(Some(&path)).await?;
        if child_dirs.is_empty() {
            // if no child directories, return the current path
            leaf_matched_dirs.push(path);
        } else {
            for child_dir in child_dirs {
                let mut child_full_path = PathBuf::new();
                child_full_path.push(&path);
                child_full_path.push(&child_dir);
                leaf_matched_dirs.extend(
                    Self::list_partition_paths_with_leveled_pruning(
                        storage,
                        child_full_path.to_str().unwrap().to_string(),
                        partition_pruner,
                        current_level + 1,
                    )
                    .await?,
                );
            }
        }
        Ok(leaf_matched_dirs)
    }

    async fn list_file_groups_for_partition(
        storage: &Storage,
        partition_path: &str,
    ) -> Result<(
        bool,           /*if valid partition dir*/
        Vec<FileGroup>, /*file groups*/
    )> {
        let files = storage.list_files(Some(partition_path)).await?;
        if !files
            .iter()
            .any(|f| f.name.eq(HOODIE_PARTITION_METAFILE_NAME))
        {
            // not a partition directory
            return Ok((false, Vec::new()));
        }
        let data_files_metadata: Vec<FileMetadata> = files
            .into_iter()
            .filter(|f| f.name.ends_with(".parquet"))
            .collect();

        let mut fg_id_to_base_files: HashMap<String, Vec<BaseFile>> = HashMap::new();
        for metadata in data_files_metadata {
            let base_file = BaseFile::try_from(metadata)?;
            let fg_id = &base_file.file_group_id;
            fg_id_to_base_files
                .entry(fg_id.to_owned())
                .or_default()
                .push(base_file);
        }

        let mut file_groups: Vec<FileGroup> = Vec::new();
        for (fg_id, base_files) in fg_id_to_base_files.into_iter() {
            let mut fg = FileGroup::new(fg_id.to_owned(), Some(partition_path.to_owned()));
            for bf in base_files {
                fg.add_base_file(bf)?;
            }
            file_groups.push(fg);
        }
        Ok((true, file_groups))
    }

    async fn load_file_groups(&self, partition_pruner: &PartitionPruner) -> Result<()> {
        let need_partition_paths =
            Self::list_partition_paths(&self.storage, partition_pruner).await?;

        let partition_paths_to_list = need_partition_paths
            .into_iter()
            .filter(|p| !self.partition_to_file_groups.contains_key(p))
            .collect::<HashSet<_>>();

        let parallelism = self
            .hudi_configs
            .get_or_default(ListingParallelism)
            .to::<usize>();
        stream::iter(partition_paths_to_list)
            .map(|path| async move {
                let file_groups =
                    Self::list_file_groups_for_partition(&self.storage, &path).await?;
                Ok::<_, CoreError>((path, file_groups))
            })
            .buffer_unordered(parallelism)
            .try_for_each(|(path, (valid_partition_path, file_groups))| async move {
                if valid_partition_path {
                    self.partition_to_file_groups.insert(path, file_groups);
                }
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

    use arrow_schema::Schema;
    use hudi_tests::TestTable;
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
        let base_url = TestTable::V6Nonpartitioned.url();
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
        let base_url = TestTable::V6ComplexkeygenHivestyle.url();
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
        let base_url = TestTable::V6Nonpartitioned.url();
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
        let fg_ids = file_slices
            .iter()
            .map(|fsl| fsl.file_group_id())
            .collect::<Vec<_>>();
        assert_eq!(fg_ids, vec!["a079bdb3-731c-4894-b855-abfcd6921007-0"]);
        for fsl in file_slices.iter() {
            assert_eq!(fsl.base_file.file_metadata.as_ref().unwrap().num_records, 4);
        }
    }

    #[tokio::test]
    async fn fs_view_get_latest_file_slices_with_replace_commit() {
        let base_url = TestTable::V6SimplekeygenNonhivestyleOverwritetable.url();
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
        let fg_ids = file_slices
            .iter()
            .map(|fsl| fsl.file_group_id())
            .collect::<Vec<_>>();
        assert_eq!(fg_ids, vec!["ebcb261d-62d3-4895-90ec-5b3c9622dff4-0"]);
        for fsl in file_slices.iter() {
            assert_eq!(fsl.base_file.file_metadata.as_ref().unwrap().num_records, 1);
        }
    }

    #[tokio::test]
    async fn fs_view_get_latest_file_slices_with_partition_filters() {
        let base_url = TestTable::V6ComplexkeygenHivestyle.url();
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

        let fg_ids = file_slices
            .iter()
            .map(|fsl| fsl.file_group_id())
            .collect::<Vec<_>>();
        assert_eq!(fg_ids, vec!["a22e8257-e249-45e9-ba46-115bc85adcba-0"]);
        for fsl in file_slices.iter() {
            assert_eq!(fsl.base_file.file_metadata.as_ref().unwrap().num_records, 2);
        }
    }

    #[tokio::test]
    async fn fs_view_get_latest_file_slices_with_complex_partition_filters() {
        let base_url = TestTable::V6ComplexkeygenHivestyle.url();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let partition_schema = hudi_table.get_partition_schema().await.unwrap();

        fn create_filter(
            filter_triples: Vec<(&str, &str, &str)>,
            schema: &Schema,
            config: &HudiConfigs,
        ) -> PartitionPruner {
            let filters: Vec<Filter> = filter_triples
                .iter()
                .map(|binary_expr_tuple| Filter::try_from(*binary_expr_tuple).unwrap())
                .collect();
            PartitionPruner::new(&filters, schema, config).unwrap()
        }

        verify_partition_pruning(
            base_url.clone(),
            create_filter(
                vec![("byteField", "<", "20"), ("shortField", "=", "300")],
                &partition_schema,
                hudi_table.hudi_configs.as_ref(),
            ),
            1,
            1,
            vec!["a22e8257-e249-45e9-ba46-115bc85adcba-0"],
        )
        .await;

        verify_partition_pruning(
            base_url.clone(),
            create_filter(
                vec![("byteField", "<", "20"), ("shortField", "=", "100")],
                &partition_schema,
                hudi_table.hudi_configs.as_ref(),
            ),
            0,
            0,
            vec![],
        )
        .await;

        verify_partition_pruning(
            base_url.clone(),
            create_filter(
                vec![("byteField", "<=", "20")],
                &partition_schema,
                hudi_table.hudi_configs.as_ref(),
            ),
            2,
            2,
            vec![
                "a22e8257-e249-45e9-ba46-115bc85adcba-0",
                "bb7c3a45-387f-490d-aab2-981c3f1a8ada-0",
            ],
        )
        .await;

        verify_partition_pruning(
            base_url.clone(),
            create_filter(
                vec![("shortField", ">", "100")],
                &partition_schema,
                hudi_table.hudi_configs.as_ref(),
            ),
            1,
            1,
            vec!["a22e8257-e249-45e9-ba46-115bc85adcba-0"],
        )
        .await;

        verify_partition_pruning(
            base_url.clone(),
            create_filter(
                vec![("shortField", "=", "100")],
                &partition_schema,
                hudi_table.hudi_configs.as_ref(),
            ),
            2,
            2,
            vec![
                "4668e35e-bff8-4be9-9ff2-e7fb17ecb1a7-0",
                "bb7c3a45-387f-490d-aab2-981c3f1a8ada-0",
            ],
        )
        .await;

        verify_partition_pruning(
            base_url.clone(),
            PartitionPruner::empty(),
            3,
            3,
            vec![
                "4668e35e-bff8-4be9-9ff2-e7fb17ecb1a7-0",
                "a22e8257-e249-45e9-ba46-115bc85adcba-0",
                "bb7c3a45-387f-490d-aab2-981c3f1a8ada-0",
            ],
        )
        .await;
    }

    async fn verify_partition_pruning(
        base_url: Url,
        partition_pruner: PartitionPruner,
        expected_fg_num: usize,
        expected_slices_num: usize,
        expected_fg_ids: Vec<&str>,
    ) {
        let fs_view = create_test_fs_view(base_url).await;
        assert_eq!(fs_view.partition_to_file_groups.len(), 0);
        let excludes = HashSet::new();
        let file_slices = fs_view
            .get_file_slices_as_of("20240418173235694", &partition_pruner, &excludes)
            .await
            .unwrap();
        assert_eq!(fs_view.partition_to_file_groups.len(), expected_fg_num);
        assert_eq!(file_slices.len(), expected_slices_num);
        let mut fg_ids = file_slices
            .iter()
            .map(|fsl| fsl.file_group_id())
            .collect::<Vec<_>>();
        fg_ids.sort();
        assert_eq!(fg_ids, expected_fg_ids);
    }
}
