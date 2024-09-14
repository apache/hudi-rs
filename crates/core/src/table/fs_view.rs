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
use crate::file_group::{BaseFile, FileGroup, FileSlice};
use crate::storage::file_info::FileInfo;
use crate::storage::{get_leaf_dirs, Storage};
use crate::table::partitions::{HudiTablePartition, PartitionFilter};
use anyhow::{anyhow, Result};
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use dashmap::DashMap;
use log::warn;
use url::Url;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct FileSystemView {
    configs: Arc<HudiConfigs>,
    pub(crate) storage: Arc<Storage>,
    partition_to_file_groups: Arc<DashMap<String, Vec<FileGroup>>>,
}

impl FileSystemView {
    pub async fn new(
        base_url: Arc<Url>,
        storage_options: Arc<HashMap<String, String>>,
        configs: Arc<HudiConfigs>,
    ) -> Result<Self> {
        let storage = Storage::new(base_url, &storage_options)?;
        let partition_to_file_groups = Arc::new(DashMap::new());
        Ok(FileSystemView {
            configs,
            storage,
            partition_to_file_groups,
        })
    }

    async fn load_partition_paths(
        storage: &Storage,
        partition_filters: &[PartitionFilter],
        partition_schema: &Schema,
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
        if partition_filters.is_empty() {
            return Ok(partition_paths);
        }
        let field_and_data_type: HashMap<_, _> = partition_schema
            .fields()
            .iter()
            .map(|field| (field.name().to_string(), field.data_type().clone()))
            .collect();

        Ok(partition_paths
            .into_iter()
            .filter(|path_str| {
                match path_str
                    .split('/')
                    .map(|s| HudiTablePartition::try_from((s, &field_and_data_type)))
                    .collect::<Result<Vec<_>>>()
                {
                    Ok(parts) => partition_filters.iter().all(|filter| filter.match_partitions(&parts)),
                    Err(e) => {
                        warn!("Failed to parse partitions for path {}: {}. Including this partition by default.", path_str, e);
                        true // include the partition despite the error
                    },
                }
            })
            .collect())
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

    pub async fn get_file_slices_as_of(
        &self,
        timestamp: &str,
        excluding_file_groups: &HashSet<FileGroup>,
        partition_filter: &[PartitionFilter],
        partition_schema: &Schema,
    ) -> Result<Vec<FileSlice>> {
        let mut file_slices = Vec::new();
        if self.partition_to_file_groups.is_empty() {
            let partition_paths =
                Self::load_partition_paths(&self.storage, partition_filter, partition_schema)
                    .await?;
            let partition_to_file_groups =
                Self::load_file_groups_for_partitions(&self.storage, partition_paths).await?;
            partition_to_file_groups.into_iter().for_each(|pair| {
                self.partition_to_file_groups.insert(pair.0, pair.1);
            });
        }
        for mut fgs in self.partition_to_file_groups.iter_mut() {
            let fgs_ref = fgs.value_mut();
            for fg in fgs_ref {
                if excluding_file_groups.contains(fg) {
                    continue;
                }
                if let Some(fsl) = fg.get_file_slice_mut_as_of(timestamp) {
                    // TODO: pass ref instead of copying
                    fsl.load_stats(&self.storage).await?;
                    let immut_fsl: &FileSlice = fsl;
                    file_slices.push(immut_fsl.clone());
                }
            }
        }
        assert!(!self.partition_to_file_groups.is_empty());
        Ok(file_slices)
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

    pub fn reset(&mut self) {
        self.partition_to_file_groups = Arc::new(DashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use hudi_tests::TestTable;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use crate::config::HudiConfigs;
    use crate::storage::Storage;
    use crate::table::fs_view::FileSystemView;
    use crate::table::partitions::PartitionFilter;
    use crate::table::Table;

    #[tokio::test]
    async fn get_partition_paths_for_nonpartitioned_table() {
        let base_url = TestTable::V6Nonpartitioned.url();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let storage = Storage::new(Arc::new(base_url), &HashMap::new()).unwrap();
        let partition_paths = FileSystemView::load_partition_paths(
            &storage,
            &hudi_table.partition_filters,
            &hudi_table.get_partition_schema().await.unwrap(),
        )
        .await
        .unwrap();
        let partition_path_set: HashSet<&str> =
            HashSet::from_iter(partition_paths.iter().map(|p| p.as_str()));
        assert_eq!(partition_path_set, HashSet::from([""]))
    }

    #[tokio::test]
    async fn get_partition_paths_for_complexkeygen_table() {
        let base_url = TestTable::V6ComplexkeygenHivestyle.url();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let storage = Storage::new(Arc::new(base_url), &HashMap::new()).unwrap();
        let partition_paths = FileSystemView::load_partition_paths(
            &storage,
            &hudi_table.partition_filters,
            &hudi_table.get_partition_schema().await.unwrap(),
        )
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
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let fs_view = FileSystemView::new(
            Arc::new(base_url),
            Arc::new(HashMap::new()),
            Arc::new(HudiConfigs::empty()),
        )
        .await
        .unwrap();

        assert!(fs_view.partition_to_file_groups.is_empty());
        let excludes = HashSet::new();
        let file_slices = fs_view
            .get_file_slices_as_of(
                "20240418173551906",
                &excludes,
                &hudi_table.partition_filters,
                &hudi_table.get_partition_schema().await.unwrap(),
            )
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
            assert_eq!(fsl.base_file.stats.as_ref().unwrap().num_records, 4);
        }
    }

    #[tokio::test]
    async fn fs_view_get_latest_file_slices_with_replace_commit() {
        let base_url = TestTable::V6SimplekeygenNonhivestyleOverwritetable.url();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let fs_view = FileSystemView::new(
            Arc::new(base_url),
            Arc::new(HashMap::new()),
            Arc::new(HudiConfigs::empty()),
        )
        .await
        .unwrap();

        assert_eq!(fs_view.partition_to_file_groups.len(), 0);
        let excludes = &hudi_table
            .timeline
            .get_replaced_file_groups()
            .await
            .unwrap();
        let file_slices = fs_view
            .get_file_slices_as_of(
                "20240707001303088",
                excludes,
                &hudi_table.partition_filters,
                &hudi_table.get_partition_schema().await.unwrap(),
            )
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
            assert_eq!(fsl.base_file.stats.as_ref().unwrap().num_records, 1);
        }
    }

    #[tokio::test]
    async fn fs_view_get_latest_file_slices_no_hive_style_filter() {
        let base_url = TestTable::V6SimplekeygenNonhivestyleOverwritetable.url();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let fs_view = FileSystemView::new(
            Arc::new(base_url),
            Arc::new(HashMap::new()),
            Arc::new(HudiConfigs::empty()),
        )
        .await
        .unwrap();
        assert_eq!(fs_view.partition_to_file_groups.len(), 0);
        let excludes = &hudi_table
            .timeline
            .get_replaced_file_groups()
            .await
            .unwrap();
        let partition_schema = hudi_table.get_partition_schema().await.unwrap();
        let byte_field_data_type = partition_schema
            .field_with_name("byteField")
            .unwrap()
            .data_type();
        let short_eq_10 =
            PartitionFilter::try_from(("byteField", "=", "10", byte_field_data_type)).unwrap();
        let file_slices = fs_view
            .get_file_slices_as_of(
                "20240707001303088",
                excludes,
                &[short_eq_10],
                &hudi_table.get_partition_schema().await.unwrap(),
            )
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
            assert_eq!(fsl.base_file.stats.as_ref().unwrap().num_records, 1);
        }
    }
}
