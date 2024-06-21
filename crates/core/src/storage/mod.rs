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

use async_recursion::async_recursion;
use object_store::path::Path as ObjPath;
use object_store::{parse_url_opts, DynObjectStore, ObjectStore};
use url::Url;

use crate::storage::file_metadata::FileMetadata;

pub(crate) mod file_metadata;

#[allow(dead_code)]
pub struct Storage {
    base_url: Url,
    object_store: Box<DynObjectStore>,
    options: HashMap<String, String>,
}

#[allow(dead_code)]
impl Storage {
    pub fn new(base_uri: &str, options: HashMap<String, String>) -> Box<Storage> {
        let base_url = Url::from_file_path(PathBuf::from(base_uri).as_path()).unwrap();
        let object_store = parse_url_opts(&base_url, &options).unwrap().0;
        Box::from(Storage {
            base_url,
            object_store,
            options,
        })
    }

    pub async fn get_file_metadata(&self, path: &str) -> FileMetadata {
        let p = ObjPath::from(path);
        let meta = self.object_store.head(&p).await.unwrap();
        FileMetadata {
            path: meta.location.to_string(),
            name: p.filename().unwrap().to_string(),
            size: meta.size,
            num_records: None,
        }
    }

    pub async fn list_dirs(&self, subdir: Option<&str>) -> Vec<String> {
        self.list_dirs_as_paths(subdir)
            .await
            .into_iter()
            .map(|p| p.filename().unwrap().to_string())
            .collect()
    }

    pub async fn list_dirs_as_paths(&self, subdir: Option<&str>) -> Vec<ObjPath> {
        let mut prefix_url = self.base_url.clone();
        if let Some(subdir) = subdir {
            prefix_url.path_segments_mut().unwrap().push(subdir);
        }
        let prefix = ObjPath::from_url_path(prefix_url.path()).unwrap();
        self.object_store
            .list_with_delimiter(Some(&prefix))
            .await
            .unwrap()
            .common_prefixes
    }

    pub async fn list_files(&self, subdir: Option<&str>) -> Vec<FileMetadata> {
        let mut prefix_url = self.base_url.clone();
        if let Some(subdir) = subdir {
            prefix_url.path_segments_mut().unwrap().push(subdir);
        }
        let prefix = ObjPath::from_url_path(prefix_url.path()).unwrap();
        self.object_store
            .list_with_delimiter(Some(&prefix))
            .await
            .unwrap()
            .objects
            .into_iter()
            .map(|obj_meta| {
                FileMetadata::new(
                    obj_meta.location.to_string(),
                    obj_meta.location.filename().unwrap().to_string(),
                    obj_meta.size,
                )
            })
            .collect()
    }
}

#[async_recursion]
pub async fn get_leaf_dirs(storage: &Storage, subdir: Option<&str>) -> Vec<String> {
    let mut leaf_dirs = Vec::new();
    let child_dirs = storage.list_dirs(subdir).await;
    if child_dirs.is_empty() {
        leaf_dirs.push(subdir.unwrap().to_owned());
    } else {
        for child_dir in child_dirs {
            let mut next_subdir = PathBuf::new();
            if let Some(curr) = subdir {
                next_subdir.push(curr);
            }
            next_subdir.push(child_dir);
            let curr_leaf_dir = get_leaf_dirs(storage, Some(next_subdir.to_str().unwrap())).await;
            leaf_dirs.extend(curr_leaf_dir);
        }
    }
    leaf_dirs
}

#[cfg(test)]
mod tests {
    use object_store::path::Path as ObjPath;
    use std::collections::{HashMap, HashSet};
    use std::fs::canonicalize;
    use std::path::Path;

    use url::Url;

    use crate::storage::{get_leaf_dirs, Storage};

    #[tokio::test]
    async fn storage_list_dirs() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("fixtures/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new(base_url.path(), HashMap::new());
        let first_level_dirs: HashSet<String> = storage.list_dirs(None).await.into_iter().collect();
        assert_eq!(
            first_level_dirs,
            vec![".hoodie", "part1", "part2", "part3"]
                .into_iter()
                .map(String::from)
                .collect()
        );
        let second_level_dirs: Vec<String> = storage.list_dirs(Some("part2")).await;
        assert_eq!(second_level_dirs, vec!["part22"]);
        let no_dirs = storage.list_dirs(Some("part1")).await;
        assert!(no_dirs.is_empty());
    }

    #[tokio::test]
    async fn storage_list_dirs_as_paths() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("fixtures/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new(base_url.path(), HashMap::new());
        let first_level_dirs: HashSet<ObjPath> =
            storage.list_dirs_as_paths(None).await.into_iter().collect();
        let expected_paths: HashSet<ObjPath> = vec![".hoodie", "part1", "part2", "part3"]
            .into_iter()
            .map(|dir| ObjPath::from_url_path(base_url.join(dir).unwrap().path()).unwrap())
            .collect();
        assert_eq!(first_level_dirs, expected_paths);
    }

    #[tokio::test]
    async fn storage_list_files() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("fixtures/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new(base_url.path(), HashMap::new());
        let file_names_1: Vec<String> = storage
            .list_files(None)
            .await
            .into_iter()
            .map(|file_metadata| file_metadata.name)
            .collect();
        assert_eq!(file_names_1, vec!["a.parquet"]);
        let file_names_2: Vec<String> = storage
            .list_files(Some("part1"))
            .await
            .into_iter()
            .map(|file_metadata| file_metadata.name)
            .collect();
        assert_eq!(file_names_2, vec!["b.parquet"]);
        let file_names_3: Vec<String> = storage
            .list_files(Some("part2/part22"))
            .await
            .into_iter()
            .map(|file_metadata| file_metadata.name)
            .collect();
        assert_eq!(file_names_3, vec!["c.parquet"]);
    }

    #[tokio::test]
    async fn use_storage_to_get_leaf_dirs() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("fixtures/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new(base_url.path(), HashMap::new());
        let leaf_dirs = get_leaf_dirs(&storage, None).await;
        assert_eq!(
            leaf_dirs,
            vec![".hoodie", "part1", "part2/part22", "part3/part32/part33"]
        );
    }
}
