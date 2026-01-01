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
//! This module is responsible for interacting with the storage layer.

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
use parquet::arrow::{parquet_to_arrow_schema, ParquetRecordBatchStreamBuilder};
use parquet::file::metadata::ParquetMetaData;
use url::Url;

use crate::config::table::HudiTableConfig;
use crate::config::HudiConfigs;
use crate::storage::error::StorageError::{Creation, InvalidPath};
use crate::storage::error::{Result, StorageError};
use crate::storage::file_metadata::FileMetadata;
use crate::storage::reader::StorageReader;
use crate::storage::util::join_url_segments;

pub mod error;
pub mod file_metadata;
pub mod reader;
pub mod util;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct Storage {
    pub(crate) base_url: Arc<Url>,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) options: Arc<HashMap<String, String>>,
    pub(crate) hudi_configs: Arc<HudiConfigs>,
}

impl Storage {
    pub const CLOUD_STORAGE_PREFIXES: [&'static str; 3] = ["AWS_", "AZURE_", "GOOGLE_"];

    pub fn new(
        options: Arc<HashMap<String, String>>,
        hudi_configs: Arc<HudiConfigs>,
    ) -> Result<Arc<Storage>> {
        let base_url = match hudi_configs.try_get(HudiTableConfig::BasePath) {
            Some(v) => v.to_url()?,
            None => {
                return Err(Creation(format!(
                    "{} is required.",
                    HudiTableConfig::BasePath.as_ref()
                )))
            }
        };

        match parse_url_opts(&base_url, options.as_ref()) {
            Ok((object_store, _)) => Ok(Arc::new(Storage {
                base_url: Arc::new(base_url),
                object_store: Arc::new(object_store),
                options,
                hudi_configs,
            })),
            Err(e) => Err(Creation(format!("Failed to create storage: {}", e))),
        }
    }

    #[cfg(test)]
    pub fn new_with_base_url(base_url: Url) -> Result<Arc<Storage>> {
        let mut hudi_options = HashMap::new();
        hudi_options.insert(
            HudiTableConfig::BasePath.as_ref().to_string(),
            base_url.as_str().to_string(),
        );
        Self::new(
            Arc::new(HashMap::new()),
            Arc::new(HudiConfigs::new(hudi_options)),
        )
    }

    #[cfg(feature = "datafusion")]
    pub fn register_object_store(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) {
        runtime_env.register_object_store(self.base_url.as_ref(), self.object_store.clone());
    }

    #[cfg(test)]
    /// Get basic file metadata (name, size) without loading the file content.
    async fn get_file_metadata_not_populated(&self, relative_path: &str) -> Result<FileMetadata> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let meta = self.object_store.head(&obj_path).await?;
        let name = meta.location.filename().ok_or_else(|| {
            InvalidPath(format!("Failed to get file name from: {:?}", meta.location))
        })?;
        Ok(FileMetadata::new(name.to_string(), meta.size))
    }

    /// Get full file metadata for a Parquet file, including record counts from Parquet metadata.
    pub async fn get_file_metadata(&self, relative_path: &str) -> Result<FileMetadata> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let obj_store = self.object_store.clone();
        let obj_meta = obj_store.head(&obj_path).await?;
        let location = obj_meta.location.clone();
        let file_name = location
            .filename()
            .ok_or_else(|| InvalidPath(format!("Failed to get file name from: {:?}", &obj_meta)))?;
        let size = obj_meta.size;
        let reader = ParquetObjectReader::new(obj_store, obj_path).with_file_size(size);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let parquet_meta = builder.metadata().clone();
        let num_records = parquet_meta.file_metadata().num_rows();
        let size_bytes = parquet_meta
            .row_groups()
            .iter()
            .map(|rg| rg.total_byte_size())
            .sum::<i64>();
        Ok(FileMetadata {
            name: file_name.to_string(),
            size,
            byte_size: size_bytes,
            num_records,
            fully_populated: true,
        })
    }

    pub async fn get_parquet_file_metadata(&self, relative_path: &str) -> Result<ParquetMetaData> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let obj_store = self.object_store.clone();
        let meta = obj_store.head(&obj_path).await?;
        let reader = ParquetObjectReader::new(obj_store, obj_path).with_file_size(meta.size);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        Ok(builder.metadata().as_ref().clone())
    }

    pub async fn get_parquet_file_schema(
        &self,
        relative_path: &str,
    ) -> Result<arrow::datatypes::Schema> {
        let parquet_meta = self.get_parquet_file_metadata(relative_path).await?;
        Ok(parquet_to_arrow_schema(
            parquet_meta.file_metadata().schema_descr(),
            None,
        )?)
    }

    pub async fn get_file_data(&self, relative_path: &str) -> Result<Bytes> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
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
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let obj_store = self.object_store.clone();
        let meta = obj_store.head(&obj_path).await?;

        // read parquet
        let reader = ParquetObjectReader::new(obj_store, obj_path).with_file_size(meta.size);
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

    pub async fn get_storage_reader(&self, relative_path: &str) -> Result<StorageReader> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let obj_store = self.object_store.clone();
        let obj_meta = obj_store.head(&obj_path).await?;
        StorageReader::new(obj_store, obj_meta)
            .await
            .map_err(StorageError::ReaderError)
    }

    pub async fn list_dirs(&self, subdir: Option<&str>) -> Result<Vec<String>> {
        let dir_paths = self.list_dirs_as_obj_paths(subdir).await?;
        let mut dirs = Vec::new();
        for dir in dir_paths {
            dirs.push(
                dir.filename()
                    .ok_or_else(|| InvalidPath(format!("Failed to get file name from: {:?}", dir)))?
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

    pub async fn list_files(&self, subdir: Option<&str>) -> Result<Vec<FileMetadata>> {
        let prefix_url = join_url_segments(&self.base_url, &[subdir.unwrap_or_default()])?;
        let prefix_path = ObjPath::from_url_path(prefix_url.path())?;
        let list_res = self
            .object_store
            .list_with_delimiter(Some(&prefix_path))
            .await?;
        let mut file_metadata = Vec::new();
        for obj_meta in list_res.objects {
            let location = obj_meta.location;
            let name = location
                .filename()
                .ok_or_else(|| InvalidPath(format!("Failed to get file name from {location:?}")))?;

            if name.ends_with(".crc") {
                continue;
            }

            file_metadata.push(FileMetadata::new(name.to_string(), obj_meta.size));
        }
        Ok(file_metadata)
    }
}

/// Get relative paths of leaf directories under a given directory.
///
/// **Example**
/// - /usr/hudi/table_name
/// - /usr/hudi/table_name/.hoodie
/// - /usr/hudi/table_name/dt=2024/month=01/day=01
/// - /usr/hudi/table_name/dt=2025/month=02
///
/// the result is \[".hoodie", "dt=2024/mont=01/day=01", "dt=2025/month=02"\]
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
            let next_subdir = next_subdir
                .to_str()
                .ok_or_else(|| InvalidPath(format!("Failed to convert path: {:?}", next_subdir)))?;
            let curr_leaf_dir = get_leaf_dirs(storage, Some(next_subdir)).await?;
            leaf_dirs.extend(curr_leaf_dir);
        }
    }
    Ok(leaf_dirs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::fs::canonicalize;
    use std::path::Path;

    #[test]
    fn test_storage_new_error_no_base_path() {
        let options = Arc::new(HashMap::new());
        let hudi_configs = Arc::new(HudiConfigs::empty());
        let result = Storage::new(options, hudi_configs);

        assert!(
            result.is_err(),
            "Should return error when no base path is provided."
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Failed to create storage"));
    }

    #[test]
    fn test_storage_new_error_invalid_url() {
        let options = Arc::new(HashMap::new());
        let hudi_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath,
            "http://invalid_url",
        )]));
        let result = Storage::new(options, hudi_configs);

        assert!(
            result.is_err(),
            "Should return error when no base path is invalid."
        );
        assert!(matches!(result.unwrap_err(), Creation(_)));
    }

    #[tokio::test]
    async fn storage_list_dirs() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
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
        let storage = Storage::new_with_base_url(base_url).unwrap();
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
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let file_info_1: Vec<FileMetadata> = storage
            .list_files(None)
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(file_info_1, vec![FileMetadata::new("a.parquet", 0)]);
        let file_info_2: Vec<FileMetadata> = storage
            .list_files(Some("part1"))
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(file_info_2, vec![FileMetadata::new("b.parquet", 0)],);
        let file_info_3: Vec<FileMetadata> = storage
            .list_files(Some("part2/part22"))
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(file_info_3, vec![FileMetadata::new("c.parquet", 0)],);
    }

    #[tokio::test]
    async fn storage_list_files_excludes_crc_files() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();

        let files = storage.list_files(None).await.unwrap();

        assert!(!files.iter().any(|f| f.name.ends_with(".crc")));
        assert_eq!(files, vec![FileMetadata::new("a.parquet", 0)]);
    }

    #[tokio::test]
    async fn use_storage_to_get_leaf_dirs() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
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
        let storage = Storage::new_with_base_url(base_url).unwrap();
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
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let file_metadata = storage
            .get_file_metadata_not_populated("a.parquet")
            .await
            .unwrap();
        assert_eq!(file_metadata.name, "a.parquet");
        assert_eq!(file_metadata.size, 866);
    }

    #[tokio::test]
    async fn storage_get_parquet_file_data() {
        let base_url =
            Url::from_directory_path(canonicalize(Path::new("tests/data")).unwrap()).unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let file_data = storage.get_parquet_file_data("a.parquet").await.unwrap();
        assert_eq!(file_data.num_rows(), 5);
    }
}
