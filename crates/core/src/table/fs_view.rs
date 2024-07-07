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

use anyhow::{anyhow, Result};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use url::Url;

use crate::config::HudiConfigs;
use crate::file_group::{BaseFile, FileGroup, FileSlice};
use crate::storage::file_info::FileInfo;
use crate::storage::{get_leaf_dirs, Storage};

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct FileSystemView {
    configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
    partition_to_file_groups: Arc<DashMap<String, Vec<FileGroup>>>,
}

impl FileSystemView {
    pub async fn new(
        base_url: Arc<Url>,
        storage_options: Arc<HashMap<String, String>>,
        configs: Arc<HudiConfigs>,
    ) -> Result<Self> {
        let storage = Storage::new(base_url, &storage_options)?;
        let partition_paths = Self::load_partition_paths(&storage).await?;
        let partition_to_file_groups =
            Self::load_file_groups_for_partitions(&storage, partition_paths).await?;
        let partition_to_file_groups = Arc::new(DashMap::from_iter(partition_to_file_groups));
        Ok(FileSystemView {
            configs,
            storage,
            partition_to_file_groups,
        })
    }

    async fn load_partition_paths(storage: &Storage) -> Result<Vec<String>> {
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
        Ok(partition_paths)
    }

    async fn load_file_groups_for_partitions(
        storage: &Storage,
        partition_paths: Vec<String>,
    ) -> Result<HashMap<String, Vec<FileGroup>>> {
        let mut partition_to_file_groups = HashMap::new();
        for p in partition_paths {
            match Self::load_file_groups_for_partition(storage, p.as_str()).await {
                Ok(file_groups) => {
                    partition_to_file_groups.insert(p, file_groups);
                }
                Err(e) => return Err(anyhow!("Failed to load partitions: {}", e)),
            }
        }
        Ok(partition_to_file_groups)
    }

    async fn load_file_groups_for_partition(
        storage: &Storage,
        partition_path: &str,
    ) -> Result<Vec<FileGroup>> {
        let file_info: Vec<FileInfo> = storage
            .list_files(Some(partition_path))
            .await?
            .into_iter()
            .filter(|f| f.name.ends_with(".parquet"))
            .collect();

        let mut fg_id_to_base_files: HashMap<String, Vec<BaseFile>> = HashMap::new();
        for f in file_info {
            let base_file = BaseFile::from_file_info(f)?;
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
        Ok(file_groups)
    }

    pub fn get_file_slices_as_of(
        &self,
        timestamp: &str,
        excluding_file_groups: &HashSet<FileGroup>,
    ) -> Result<Vec<FileSlice>> {
        let mut file_slices = Vec::new();
        for fgs in self.partition_to_file_groups.iter() {
            let fgs_ref = fgs.value();
            for fg in fgs_ref {
                if excluding_file_groups.contains(fg) {
                    continue;
                }
                if let Some(fsl) = fg.get_file_slice_as_of(timestamp) {
                    // TODO: pass ref instead of copying
                    file_slices.push(fsl.clone());
                }
            }
        }
        Ok(file_slices)
    }

    pub async fn load_file_slices_stats_as_of(
        &self,
        timestamp: &str,
        exclude_file_groups: &HashSet<FileGroup>,
    ) -> Result<()> {
        for mut fgs in self.partition_to_file_groups.iter_mut() {
            let fgs_ref = fgs.value_mut();
            for fg in fgs_ref {
                if exclude_file_groups.contains(fg) {
                    continue;
                }
                if let Some(file_slice) = fg.get_file_slice_mut_as_of(timestamp) {
                    file_slice
                        .load_stats(&self.storage)
                        .await
                        .expect("Successful loading file stats.");
                }
            }
        }
        Ok(())
    }

    pub async fn read_file_slice_by_path_unchecked(
        &self,
        relative_path: &str,
    ) -> Result<RecordBatch> {
        self.storage.get_parquet_file_data(relative_path).await
    }
    pub async fn read_file_slice_unchecked(&self, file_slice: &FileSlice) -> Result<RecordBatch> {
        self.read_file_slice_by_path_unchecked(&file_slice.base_file_relative_path())
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use hudi_tests::TestTable;

    use crate::config::HudiConfigs;
    use crate::storage::Storage;
    use crate::table::fs_view::FileSystemView;

    #[tokio::test]
    async fn get_partition_paths_for_nonpartitioned_table() {
        let base_url = TestTable::V6Nonpartitioned.url();
        let storage = Storage::new(Arc::new(base_url), &HashMap::new()).unwrap();
        let partition_paths = FileSystemView::load_partition_paths(&storage)
            .await
            .unwrap();
        let partition_path_set: HashSet<&str> =
            HashSet::from_iter(partition_paths.iter().map(|p| p.as_str()));
        assert_eq!(partition_path_set, HashSet::from([""]))
    }

    #[tokio::test]
    async fn get_partition_paths_for_complexkeygen_table() {
        let base_url = TestTable::V6ComplexkeygenHivestyle.url();
        let storage = Storage::new(Arc::new(base_url), &HashMap::new()).unwrap();
        let partition_paths = FileSystemView::load_partition_paths(&storage)
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
        let fs_view = FileSystemView::new(
            Arc::new(base_url),
            Arc::new(HashMap::new()),
            Arc::new(HudiConfigs::empty()),
        )
        .await
        .unwrap();

        let excludes = HashSet::new();
        let file_slices = fs_view
            .get_file_slices_as_of("20240418173551906", &excludes)
            .unwrap();
        assert_eq!(file_slices.len(), 1);
        let fg_ids = file_slices
            .iter()
            .map(|fsl| fsl.file_group_id())
            .collect::<Vec<_>>();
        assert_eq!(fg_ids, vec!["a079bdb3-731c-4894-b855-abfcd6921007-0"])
    }
}
