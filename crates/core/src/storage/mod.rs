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

use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_recursion::async_recursion;
use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, parse_url_opts};
use url::Url;

use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig;

use crate::storage::error::Result;
use crate::storage::error::StorageError::{self, Creation, InvalidPath};
use crate::storage::file_metadata::FileMetadata;
use crate::storage::reader::StorageReader;
use crate::storage::util::join_url_segments;

pub mod error;
pub mod file_metadata;
pub mod reader;
pub mod util;

/// Builds a parquet `RowFilter` for a read, given the file's parquet schema and
/// the Arrow schema it maps to. Returning `None` means no filter is pushed.
///
/// `Arc` rather than `Box` so options holding it stay `Clone`. The captured
/// state must be `Send + Sync` because the parquet stream may evaluate the
/// filter on any worker thread.
pub type RowFilterBuilder = Arc<
    dyn Fn(
            &parquet::schema::types::SchemaDescriptor,
            &arrow_schema::Schema,
        ) -> Option<parquet::arrow::arrow_reader::RowFilter>
        + Send
        + Sync,
>;

#[allow(dead_code)]
/// Runtime that owns every ranged object-store read issued from synchronous
/// code.
///
/// A log file is read through `std::io::Read`, which is synchronous, while
/// `object_store` is async. Bridging the two needs somewhere to drive the
/// future, and the obvious choices do not work: the sync read can be reached
/// from inside another runtime, where `block_on` panics with "Cannot start a
/// runtime from within a runtime", and a runtime built per read would take
/// hyper's connection dispatcher down with it when dropped, failing every
/// subsequent request against the same cached store.
///
/// So reads are spawned here and the calling thread waits on a channel. The
/// runtime outlives any individual caller, which is what keeps the dispatcher
/// alive.
pub static OBJECT_STORE_RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(OBJECT_STORE_RUNTIME_WORKERS)
        .enable_all()
        .thread_name(OBJECT_STORE_RUNTIME_THREAD_NAME)
        .build()
        .expect("the object-store runtime must build")
});

/// Worker threads on [`OBJECT_STORE_RUNTIME`].
///
/// This runtime only drives object-store I/O, which is latency-bound rather
/// than CPU-bound, so the count is a fixed small number rather than a function
/// of the machine's cores: a host with 96 of them has no more requests in
/// flight than one with 8, and sizing to cores would spend threads on nothing.
pub(crate) const OBJECT_STORE_RUNTIME_WORKERS: usize = 8;

/// Thread-name prefix for [`OBJECT_STORE_RUNTIME`]'s workers.
///
/// Load-bearing, not cosmetic: it is how a blocking bridge recognises that it is
/// about to block one of its own workers — see
/// [`in_object_store_runtime`](crate::storage::reader::in_object_store_runtime).
pub(crate) const OBJECT_STORE_RUNTIME_THREAD_NAME: &str = "hudi-rs-objstore";

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
        let base_url = match hudi_configs
            .try_get(HudiTableConfig::BasePath)
            .map_err(|e| Creation(format!("{e}")))?
        {
            Some(v) => v.to_url()?,
            None => {
                return Err(Creation(format!(
                    "{} is required.",
                    HudiTableConfig::BasePath.as_ref()
                )));
            }
        };

        match parse_url_opts(&base_url, options.as_ref()) {
            Ok((object_store, _)) => Ok(Arc::new(Storage {
                base_url: Arc::new(base_url),
                object_store: Arc::new(object_store),
                options,
                hudi_configs,
            })),
            Err(e) => Err(Creation(format!("Failed to create storage: {e}"))),
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

    pub async fn get_storage_reader(&self, relative_path: &str) -> Result<StorageReader> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let obj_store = self.object_store.clone();
        let obj_meta = obj_store.head(&obj_path).await?;
        StorageReader::new(obj_store, obj_meta)
            .await
            .map_err(StorageError::ReaderError)
    }

    /// A reader that fetches bounded windows instead of the whole file.
    ///
    /// Only the object metadata is fetched here; no file bytes are read until
    /// the caller reads.
    pub async fn get_streaming_storage_reader(&self, relative_path: &str) -> Result<StorageReader> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let obj_store = self.object_store.clone();
        let obj_meta = obj_store.head(&obj_path).await?;
        let window_size = crate::storage::reader::stream_window_size(&self.hudi_configs)
            .map_err(StorageError::ReaderError)?;
        Ok(StorageReader::new_streaming(
            obj_store,
            obj_meta,
            window_size,
        ))
    }

    pub async fn list_dirs(&self, subdir: Option<&str>) -> Result<Vec<String>> {
        let dir_paths = self.list_dirs_as_obj_paths(subdir).await?;
        let mut dirs = Vec::new();
        for dir in dir_paths {
            dirs.push(
                dir.filename()
                    .ok_or_else(|| InvalidPath(format!("Failed to get file name from: {dir:?}")))?
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

    /// Resolve a relative path to an [`ObjPath`] under this storage's base URL.
    fn relative_obj_path(&self, relative_path: &str) -> Result<ObjPath> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        Ok(ObjPath::from_url_path(obj_url.path())?)
    }

    /// Write bytes to a path relative to the table base path.
    pub async fn put_file(&self, relative_path: &str, bytes: impl Into<Bytes>) -> Result<()> {
        let obj_path = self.relative_obj_path(relative_path)?;
        self.object_store
            .put(&obj_path, PutPayload::from(bytes.into()))
            .await?;
        Ok(())
    }

    /// Write a file only if nothing exists at the path (`PutMode::Create`).
    ///
    /// Timeline files must never silently replace one another: two writers
    /// minting the same instant otherwise clobber each other's commit with no
    /// error. An existing object surfaces as
    /// [`StorageError::AlreadyExists`](error::StorageError) so callers can
    /// report the conflict.
    pub async fn put_file_if_absent(
        &self,
        relative_path: &str,
        bytes: impl Into<Bytes>,
    ) -> Result<()> {
        let obj_path = self.relative_obj_path(relative_path)?;
        let options = PutOptions::from(PutMode::Create);
        match self
            .object_store
            .put_opts(&obj_path, PutPayload::from(bytes.into()), options)
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { path, source }) => {
                Err(error::StorageError::AlreadyExists(path, source))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Delete a file at a path relative to the table base path.
    ///
    /// Missing objects are treated as success (idempotent cleanup).
    pub async fn delete_file(&self, relative_path: &str) -> Result<()> {
        let obj_path = self.relative_obj_path(relative_path)?;
        match self.object_store.delete(&obj_path).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Return true if an object exists at the relative path.
    pub async fn exists(&self, relative_path: &str) -> Result<bool> {
        let obj_path = self.relative_obj_path(relative_path)?;
        match self.object_store.head(&obj_path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Rename/move an object from `from_relative` to `to_relative`.
    pub async fn rename(&self, from_relative: &str, to_relative: &str) -> Result<()> {
        let from_path = self.relative_obj_path(from_relative)?;
        let to_path = self.relative_obj_path(to_relative)?;
        self.object_store.rename(&from_path, &to_path).await?;
        Ok(())
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
                .ok_or_else(|| InvalidPath(format!("Failed to convert path: {next_subdir:?}")))?;
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
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to create storage")
        );
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
    async fn storage_put_exists_delete() {
        let dir = tempfile::tempdir().unwrap();
        let base_url = Url::from_directory_path(dir.path()).unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();

        assert!(!storage.exists("nested/file.txt").await.unwrap());
        storage
            .put_file("nested/file.txt", b"hello".as_slice())
            .await
            .unwrap();
        assert!(storage.exists("nested/file.txt").await.unwrap());
        let data = storage.get_file_data("nested/file.txt").await.unwrap();
        assert_eq!(data.as_ref(), b"hello");

        storage
            .rename("nested/file.txt", "nested/renamed.txt")
            .await
            .unwrap();
        assert!(!storage.exists("nested/file.txt").await.unwrap());
        assert!(storage.exists("nested/renamed.txt").await.unwrap());

        storage.delete_file("nested/renamed.txt").await.unwrap();
        assert!(!storage.exists("nested/renamed.txt").await.unwrap());
        // Idempotent delete of missing object
        storage.delete_file("nested/renamed.txt").await.unwrap();
    }

    /// Timeline files rely on create-if-absent: a second write to the same
    /// path must error rather than replace, and must leave the original
    /// contents untouched.
    #[tokio::test]
    async fn storage_put_file_if_absent_rejects_existing() {
        let dir = tempfile::tempdir().unwrap();
        let base_url = Url::from_directory_path(dir.path()).unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();

        storage
            .put_file_if_absent("timeline/0001.commit", b"first".as_slice())
            .await
            .unwrap();

        let err = storage
            .put_file_if_absent("timeline/0001.commit", b"second".as_slice())
            .await
            .unwrap_err();
        assert!(
            matches!(err, error::StorageError::AlreadyExists(_, _)),
            "expected AlreadyExists, got: {err:?}"
        );

        let data = storage.get_file_data("timeline/0001.commit").await.unwrap();
        assert_eq!(data.as_ref(), b"first", "loser must not clobber the winner");
    }
}
