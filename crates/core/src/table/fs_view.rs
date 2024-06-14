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

use std::{fs, io};
use std::error::Error;
use std::path::{Path, PathBuf};

use hashbrown::HashMap;

use hudi_fs::file_systems::FileMetadata;

use crate::error::HudiFileSystemViewError;
use crate::error::HudiFileSystemViewError::FailToLoadPartitions;
use crate::file_group::{BaseFile, FileGroup, FileSlice};
use crate::utils::get_leaf_dirs;

#[derive(Clone, Debug)]
pub struct FileSystemView {
    pub base_path: PathBuf,
    partition_to_file_groups: HashMap<String, Vec<FileGroup>>,
}

impl FileSystemView {
    pub fn new(p: &Path) -> Result<Self, HudiFileSystemViewError> {
        let mut fs_view = FileSystemView {
            base_path: p.to_path_buf(),
            partition_to_file_groups: HashMap::new(),
        };
        fs_view.load_partitions()?;
        Ok(fs_view)
    }

    fn load_partitions(&mut self) -> Result<(), HudiFileSystemViewError> {
        match self.get_partition_paths() {
            Ok(partition_paths) => {
                for p in partition_paths {
                    match self.get_file_groups(p.as_str()) {
                        Ok(file_groups) => {
                            self.partition_to_file_groups.insert(p, file_groups);
                        }
                        Err(e) => return Err(FailToLoadPartitions(e)),
                    }
                }
            }
            Err(e) => return Err(FailToLoadPartitions(Box::new(e))),
        }
        Ok(())
    }

    pub fn get_partition_paths(&self) -> Result<Vec<String>, io::Error> {
        let mut first_level_partition_paths: Vec<PathBuf> = Vec::new();
        for entry in fs::read_dir(self.base_path.as_path())? {
            let p = entry?.path();
            if p.is_dir() && p.file_name().and_then(|e| e.to_str()) != Some(".hoodie") {
                first_level_partition_paths.push(p);
            }
        }
        let mut full_partition_paths: Vec<PathBuf> = Vec::new();
        for p in first_level_partition_paths {
            full_partition_paths.extend(get_leaf_dirs(p.as_path())?)
        }
        let common_prefix_len = self.base_path.to_str().unwrap().len() + 1;
        let mut partition_paths = Vec::new();
        for p in full_partition_paths {
            let full_partition_path = p.to_str().unwrap();
            partition_paths.push(full_partition_path[common_prefix_len..].to_owned())
        }
        Ok(partition_paths)
    }

    pub fn get_file_groups(&self, partition_path: &str) -> Result<Vec<FileGroup>, Box<dyn Error>> {
        let mut part_path = self.base_path.to_path_buf();
        part_path.push(partition_path);
        let mut fg_id_to_base_files: HashMap<String, Vec<BaseFile>> = HashMap::new();
        for entry in fs::read_dir(part_path)? {
            let p = entry?.path();
            if p.is_file() && p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                let file_metadata = FileMetadata::from_path(p.as_path())?;
                let base_file = BaseFile::from_file_metadata(file_metadata);
                let fg_id = &base_file.file_group_id;
                fg_id_to_base_files
                    .entry(fg_id.to_owned())
                    .or_default()
                    .push(base_file);
            }
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
}


#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::path::Path;

    use hudi_fs::test_utils::extract_test_table;

    use crate::table::fs_view::FileSystemView;

    #[test]
    fn get_partition_paths() {
        let fixture_path = Path::new("fixtures/table/0.x_cow_partitioned.zip");
        let target_table_path = extract_test_table(fixture_path);
        let fs_view = FileSystemView::new(&target_table_path).unwrap();
        let partition_paths = fs_view.get_partition_paths().unwrap();
        let partition_path_set: HashSet<&str> =
            HashSet::from_iter(partition_paths.iter().map(|p| p.as_str()));
        assert_eq!(
            partition_path_set,
            HashSet::from_iter(vec!["chennai", "sao_paulo", "san_francisco"])
        )
    }

    #[test]
    fn get_latest_file_slices() {
        let fixture_path = Path::new("fixtures/table/0.x_cow_partitioned.zip");
        let target_table_path = extract_test_table(fixture_path);
        let fs_view = FileSystemView::new(&target_table_path).unwrap();
        let file_slices = fs_view.get_latest_file_slices();
        assert_eq!(file_slices.len(), 5);
        let mut fg_ids = Vec::new();
        for f in file_slices {
            let fp = f.file_group_id();
            fg_ids.push(fp);
        }
        let actual: HashSet<&str> = fg_ids.into_iter().collect();
        assert_eq!(
            actual,
            HashSet::from_iter(vec![
                "780b8586-3ad0-48ef-a6a1-d2217845ce4a-0",
                "d9082ffd-2eb1-4394-aefc-deb4a61ecc57-0",
                "ee915c68-d7f8-44f6-9759-e691add290d8-0",
                "68d3c349-f621-4cd8-9e8b-c6dd8eb20d08-0",
                "5a226868-2934-4f84-a16f-55124630c68d-0"
            ])
        );
    }
}
