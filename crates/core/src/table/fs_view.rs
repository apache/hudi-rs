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
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use arrow::record_batch::RecordBatch;
use url::Url;

use crate::file_group::{BaseFile, FileGroup, FileSlice};
use crate::storage::file_info::FileInfo;
use crate::storage::file_stats::FileStats;
use crate::storage::{get_leaf_dirs, Storage};

#[derive(Clone, Debug)]
pub struct FileSystemView {
    pub base_url: Url,
    partition_to_file_groups: HashMap<String, Vec<FileGroup>>,
}

impl FileSystemView {
    pub fn new(base_url: Url) -> Self {
        FileSystemView {
            base_url,
            partition_to_file_groups: HashMap::new(),
        }
    }

    async fn get_partition_paths(&self) -> Result<Vec<String>> {
        let storage = Storage::new(self.base_url.clone(), HashMap::new());
        let top_level_dirs: Vec<String> = storage
            .list_dirs(None)
            .await
            .into_iter()
            .filter(|dir| dir != ".hoodie")
            .collect();
        let mut partition_paths = Vec::new();
        for dir in top_level_dirs {
            partition_paths.extend(get_leaf_dirs(&storage, Some(&dir)).await);
        }
        Ok(partition_paths)
    }

    async fn get_file_groups(&self, partition_path: &str) -> Result<Vec<FileGroup>> {
        let storage = Storage::new(self.base_url.clone(), HashMap::new());
        let file_info: Vec<FileInfo> = storage
            .list_files(Some(partition_path))
            .await
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

    pub fn load_file_groups(&mut self) {
        let fs_view = self.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let wrapper = async { get_partitions_and_file_groups(&fs_view).await };
        let result = rt.block_on(wrapper).unwrap();
        for (k, v) in result {
            self.partition_to_file_groups.insert(k, v);
        }
    }

    pub fn get_latest_file_slices(&self) -> Vec<&FileSlice> {
        let mut file_slices = Vec::new();
        for fgs in self.partition_to_file_groups.values() {
            for fg in fgs {
                if let Some(file_slice) = fg.get_latest_file_slice() {
                    file_slices.push(file_slice)
                }
            }
        }
        file_slices
    }

    pub fn get_latest_file_slices_with_stats(&mut self) -> Vec<&mut FileSlice> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut file_slices = Vec::new();
        let file_groups = &mut self.partition_to_file_groups.values_mut();
        for fgs in file_groups {
            for fg in fgs {
                if let Some(file_slice) = fg.get_latest_file_slice_mut() {
                    let wrapper = async { load_file_slice_stats(&self.base_url, file_slice).await };
                    let _ = rt.block_on(wrapper);
                    file_slices.push(file_slice)
                }
            }
        }
        file_slices
    }

    pub fn read_file_slice(&self, relative_path: &str) -> Vec<RecordBatch> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let storage = Storage::new(self.base_url.clone(), HashMap::new());
        let wrapper = async { storage.get_parquet_file_data(relative_path).await };
        rt.block_on(wrapper)
    }
}

async fn load_file_slice_stats(base_url: &Url, file_slice: &mut FileSlice) -> Result<()> {
    let base_file = &mut file_slice.base_file;
    if base_file.stats.is_none() {
        let storage = Storage::new(base_url.clone(), HashMap::new());
        let ptn = file_slice.partition_path.clone();
        let mut relative_path = PathBuf::from(ptn.unwrap_or("".to_string()));
        let base_file_name = &base_file.info.name;
        relative_path.push(base_file_name);
        let parquet_meta = storage
            .get_parquet_file_metadata(relative_path.to_str().unwrap())
            .await;
        let num_records = parquet_meta.file_metadata().num_rows();
        base_file.populate_stats(FileStats { num_records });
    }
    Ok(())
}

async fn get_partitions_and_file_groups(
    fs_view: &FileSystemView,
) -> Result<HashMap<String, Vec<FileGroup>>> {
    match fs_view.get_partition_paths().await {
        Ok(mut partition_paths) => {
            if partition_paths.is_empty() {
                partition_paths.push("".to_string());
            }
            let mut partition_to_file_groups = HashMap::new();
            for p in partition_paths {
                match fs_view.get_file_groups(p.as_str()).await {
                    Ok(file_groups) => {
                        partition_to_file_groups.insert(p, file_groups);
                    }
                    Err(e) => return Err(anyhow!("Failed to load partitions: {}", e)),
                }
            }
            Ok(partition_to_file_groups)
        }
        Err(e) => Err(anyhow!("Failed to load partitions: {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use hudi_tests::TestTable;

    use crate::table::fs_view::FileSystemView;

    #[tokio::test]
    async fn get_partition_paths_for_nonpartitioned_table() {
        let base_url = TestTable::V6Nonpartitioned.url();
        let fs_view = FileSystemView::new(base_url);
        let partition_paths = fs_view.get_partition_paths().await.unwrap();
        let partition_path_set: HashSet<&str> =
            HashSet::from_iter(partition_paths.iter().map(|p| p.as_str()));
        assert_eq!(partition_path_set, HashSet::new(),)
    }

    #[tokio::test]
    async fn get_partition_paths_for_complexkeygen_table() {
        let base_url = TestTable::V6ComplexkeygenHivestyle.url();
        let fs_view = FileSystemView::new(base_url);
        let partition_paths = fs_view.get_partition_paths().await.unwrap();
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

    #[test]
    fn get_latest_file_slices() {
        let base_url = TestTable::V6Nonpartitioned.url();
        let mut fs_view = FileSystemView::new(base_url);
        fs_view.load_file_groups();
        let file_slices = fs_view.get_latest_file_slices();
        assert_eq!(file_slices.len(), 1);
        let mut fg_ids = Vec::new();
        for f in file_slices {
            let fp = f.file_group_id();
            fg_ids.push(fp);
        }
        assert_eq!(fg_ids, vec!["a079bdb3-731c-4894-b855-abfcd6921007-0"])
    }
}
