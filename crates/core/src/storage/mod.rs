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
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use async_recursion::async_recursion;
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{parse_url_opts, ObjectStore};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::ParquetMetaData;
use url::Url;

use crate::storage::file_info::FileInfo;
use crate::storage::utils::join_url_segments;
use crate::{Error, Result};

pub mod file_info;
pub mod file_stats;
pub mod utils;

#[derive(Clone, Debug)]
pub struct Storage {
    base_url: Arc<Url>,
    object_store: Arc<dyn ObjectStore>,
}

impl Storage {
    pub fn new(base_url: Arc<Url>, options: &HashMap<String, String>) -> Result<Arc<Storage>> {
        match parse_url_opts(&base_url, options) {
            Ok((object_store, _)) => Ok(Arc::new(Storage {
                base_url,
                object_store: Arc::new(object_store),
            })),
            Err(e) => Err(Error::Store(e)),
        }
    }

    #[cfg(feature = "datafusion")]
    pub fn register_object_store(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) {
        runtime_env.register_object_store(self.base_url.as_ref(), self.object_store.clone());
    }

    fn get_relative_path(&self, relative_path: &str) -> Result<(Url, ObjPath)> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        Ok((obj_url, obj_path))
    }

    #[cfg(test)]
    async fn get_file_info(&self, relative_path: &str) -> Result<FileInfo> {
        let (obj_url, obj_path) = self.get_relative_path(relative_path)?;
        let meta = self.object_store.head(&obj_path).await?;
        let uri = obj_url.to_string();
        let name = obj_path
            .filename()
            .ok_or(Error::InvalidPath {
                name: obj_path.to_string(),
                detail: "failed to get file name".to_string(),
            })?
            .to_string();
        Ok(FileInfo {
            uri,
            name,
            size: meta.size,
        })
    }

    pub async fn get_parquet_file_metadata(&self, relative_path: &str) -> Result<ParquetMetaData> {
        let (_, obj_path) = self.get_relative_path(relative_path)?;
        let obj_store = self.object_store.clone();
        let meta = obj_store.head(&obj_path).await?;
        let reader = ParquetObjectReader::new(obj_store, meta);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        Ok(builder.metadata().as_ref().clone())
    }

    pub async fn get_file_data(&self, relative_path: &str) -> Result<Bytes> {
        let (_, obj_path) = self.get_relative_path(relative_path)?;
        let result = self.object_store.get(&obj_path).await?;
        let bytes = result.bytes().await?;
        Ok(bytes)
    }

    pub async fn get_file_data_from_absolute_path(&self, absolute_path: &str) -> Result<Bytes> {
        let obj_path = ObjPath::from_absolute_path(PathBuf::from(absolute_path))?;
        let result = self.object_store.get(&obj_path).await?;
        let bytes = result.bytes().await?;
        Ok(bytes)
    }

    pub async fn get_parquet_file_data(&self, relative_path: &str) -> Result<RecordBatch> {
        let (_, obj_path) = self.get_relative_path(relative_path)?;
        let obj_store = self.object_store.clone();
        let meta = obj_store.head(&obj_path).await?;

        // read parquet
        let reader = ParquetObjectReader::new(obj_store, meta);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let schema = builder.schema().clone();
        let mut stream = builder.build()?;
        let mut batches = Vec::new();

        while let Some(r) = stream.next().await {
            batches.push(r?)
        }

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }

        Ok(concat_batches(&schema, &batches)?)
    }

    pub async fn list_dirs(&self, subdir: Option<&str>) -> Result<Vec<String>> {
        let dir_paths = self.list_dirs_as_obj_paths(subdir).await?;
        let mut dirs = Vec::new();
        for dir in dir_paths {
            dirs.push(
                dir.filename()
                    .ok_or(Error::InvalidPath {
                        name: dir.to_string(),
                        detail: "failed to get file name".to_string(),
                    })?
                    .to_string(),
            )
        }
        Ok(dirs)
    }

    async fn list_dirs_as_obj_paths(&self, subdir: Option<&str>) -> Result<Vec<ObjPath>> {
        let prefix_url = join_url_segments(&self.base_url, &[subdir.unwrap_or_default()])?;
        let prefix_path = ObjPath::from_url_path(prefix_url.path())?;
        let list_res = self
            .object_store
            .list_with_delimiter(Some(&prefix_path))
            .await?;
        Ok(list_res.common_prefixes)
    }

    pub async fn list_files(&self, subdir: Option<&str>) -> Result<Vec<FileInfo>> {
        let prefix_url = join_url_segments(&self.base_url, &[subdir.unwrap_or_default()])?;
        let prefix_path = ObjPath::from_url_path(prefix_url.path())?;
        let list_res = self
            .object_store
            .list_with_delimiter(Some(&prefix_path))
            .await?;
        let mut file_info = Vec::new();
        for obj_meta in list_res.objects {
            let name = obj_meta
                .location
                .filename()
                .ok_or(Error::InvalidPath {
                    name: obj_meta.location.to_string(),
                    detail: "failed to get file name".to_string(),
                })?
                .to_string();
            let uri = join_url_segments(&prefix_url, &[&name])?.to_string();
            file_info.push(FileInfo {
                uri,
                name,
                size: obj_meta.size,
            });
        }
        Ok(file_info)
    }
}

#[async_recursion]
pub async fn get_leaf_dirs(storage: &Storage, subdir: Option<&str>) -> Result<Vec<String>> {
    let mut leaf_dirs = Vec::new();
    let child_dirs = storage.list_dirs(subdir).await?;
    if child_dirs.is_empty() {
        leaf_dirs.push(subdir.unwrap_or_default().to_owned());
    } else {
        for child_dir in child_dirs {
            let mut next_subdir = PathBuf::new();
            if let Some(curr) = subdir {
                next_subdir.push(curr);
            }
            next_subdir.push(child_dir);
            let next_subdir = next_subdir.to_str().ok_or(Error::InvalidPath {
                name: format!("{:?}", next_subdir),
                detail: "failed to convert path".to_string(),
            })?;
            let curr_leaf_dir = get_leaf_dirs(storage, Some(next_subdir)).await?;
            leaf_dirs.extend(curr_leaf_dir);
        }
    }
    Ok(leaf_dirs)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs::canonicalize;
    use std::path::Path;
    use std::sync::Arc;

    use object_store::path::Path as ObjPath;
    use url::Url;

    use crate::storage::file_info::FileInfo;
    use crate::storage::utils::join_url_segments;
    use crate::storage::{get_leaf_dirs, Storage};

    #[tokio::test]
    async fn storage_list_dirs() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new(Arc::new(base_url), &HashMap::new()).unwrap();
        let first_level_dirs: HashSet<String> =
            storage.list_dirs(None).await.unwrap().into_iter().collect();
        assert_eq!(
            first_level_dirs,
            vec![".hoodie", "part1", "part2", "part3"]
                .into_iter()
                .map(String::from)
                .collect()
        );
        let second_level_dirs: Vec<String> = storage.list_dirs(Some("part2")).await.unwrap();
        assert_eq!(second_level_dirs, vec!["part22"]);
        let no_dirs = storage.list_dirs(Some("part1")).await.unwrap();
        assert!(no_dirs.is_empty());
    }

    #[tokio::test]
    async fn storage_list_dirs_as_paths() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new(Arc::new(base_url), &HashMap::new()).unwrap();
        let first_level_dirs: HashSet<ObjPath> = storage
            .list_dirs_as_obj_paths(None)
            .await
            .unwrap()
            .into_iter()
            .collect();
        let expected_paths: HashSet<ObjPath> = vec![".hoodie", "part1", "part2", "part3"]
            .into_iter()
            .map(|dir| {
                ObjPath::from_url_path(join_url_segments(&storage.base_url, &[dir]).unwrap().path())
                    .unwrap()
            })
            .collect();
        assert_eq!(first_level_dirs, expected_paths);
    }

    #[tokio::test]
    async fn storage_list_files() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new(Arc::new(base_url), &HashMap::new()).unwrap();
        let file_info_1: Vec<FileInfo> = storage
            .list_files(None)
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(
            file_info_1,
            vec![FileInfo {
                uri: storage.base_url.join("a.parquet").unwrap().to_string(),
                name: "a.parquet".to_string(),
                size: 0,
            }]
        );
        let file_info_2: Vec<FileInfo> = storage
            .list_files(Some("part1"))
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(
            file_info_2,
            vec![FileInfo {
                uri: storage
                    .base_url
                    .join("part1/b.parquet")
                    .unwrap()
                    .to_string(),
                name: "b.parquet".to_string(),
                size: 0,
            }]
        );
        let file_info_3: Vec<FileInfo> = storage
            .list_files(Some("part2/part22"))
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(
            file_info_3,
            vec![FileInfo {
                uri: storage
                    .base_url
                    .join("part2/part22/c.parquet")
                    .unwrap()
                    .to_string(),
                name: "c.parquet".to_string(),
                size: 0,
            }]
        );
    }

    #[tokio::test]
    async fn use_storage_to_get_leaf_dirs() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new(Arc::new(base_url), &HashMap::new()).unwrap();
        let leaf_dirs = get_leaf_dirs(&storage, None).await.unwrap();
        assert_eq!(
            leaf_dirs,
            vec![".hoodie", "part1", "part2/part22", "part3/part32/part33"]
        );
    }

    #[tokio::test]
    async fn use_storage_to_get_leaf_dirs_for_leaf_dir() {
        let base_url =
            Url::from_directory_path(canonicalize(Path::new("tests/data/leaf_dir")).unwrap())
                .unwrap();
        let storage = Storage::new(Arc::new(base_url), &HashMap::new()).unwrap();
        let leaf_dirs = get_leaf_dirs(&storage, None).await.unwrap();
        assert_eq!(
            leaf_dirs,
            vec![""],
            "Listing a leaf dir should get the relative path to itself."
        );
    }

    #[tokio::test]
    async fn storage_get_file_info() {
        let base_url =
            Url::from_directory_path(canonicalize(Path::new("tests/data")).unwrap()).unwrap();
        let storage = Storage::new(Arc::new(base_url), &HashMap::new()).unwrap();
        let file_info = storage.get_file_info("a.parquet").await.unwrap();
        assert_eq!(file_info.name, "a.parquet");
        assert_eq!(
            file_info.uri,
            storage.base_url.join("a.parquet").unwrap().as_ref()
        );
        assert_eq!(file_info.size, 866);
    }

    #[tokio::test]
    async fn storage_get_parquet_file_data() {
        let base_url =
            Url::from_directory_path(canonicalize(Path::new("tests/data")).unwrap()).unwrap();
        let storage = Storage::new(Arc::new(base_url), &HashMap::new()).unwrap();
        let file_data = storage.get_parquet_file_data("a.parquet").await.unwrap();
        assert_eq!(file_data.num_rows(), 5);
    }
}
