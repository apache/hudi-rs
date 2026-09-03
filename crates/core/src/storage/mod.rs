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
use std::sync::atomic::{AtomicU64, Ordering};

use async_recursion::async_recursion;
use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, parse_url_opts};
use url::Url;

use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig;

use crate::storage::error::Result;
use crate::storage::error::StorageError::{self, Creation, InvalidPath};
use crate::storage::file_metadata::FileMetadata;
use crate::storage::reader::StorageReader;
use crate::storage::util::join_url_segments;

#[cfg(test)]
pub(crate) mod counting;
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

/// Chooses which row groups a read fetches, from the file's parsed footer.
/// Returning `None` means "no opinion, read them all".
///
/// This is the only mechanism on the read path that can avoid reading bytes: a
/// [`RowFilterBuilder`] decides per row after the predicate columns are decoded,
/// so it saves decode, never IO, while a row group excluded here is never
/// fetched.
///
/// The selector must be CONSERVATIVE. Keeping a row group that cannot match
/// costs only time; dropping one that can match silently loses rows, and nothing
/// downstream can restore them.
///
/// `Arc` for the same reason as [`RowFilterBuilder`]: options holding it stay
/// `Clone`, and it may run on any worker thread.
pub type RowGroupSelector =
    Arc<dyn Fn(&parquet::file::metadata::ParquetMetaData) -> Option<Vec<usize>> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct Storage {
    pub(crate) base_url: Arc<Url>,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) options: Arc<HashMap<String, String>>,
    pub(crate) hudi_configs: Arc<HudiConfigs>,
    /// Read-volume counters for the base-file reads made through this
    /// `Storage`.
    ///
    /// Here rather than on a read parameter so no read signature changes. The
    /// scope is this `Storage`'s lifetime, which is as narrow as the caller
    /// makes it: a per-read `Storage` yields per-read counters, while a
    /// `FileGroupReader` builds one `Storage` in its constructor and reuses
    /// it, so every slice read through that reader accumulates into the same
    /// counters. The `object_store` inside could not carry them: it is shared
    /// even wider, so its counts would be everyone's.
    pub(crate) read_volume: Arc<ReadVolume>,
}

/// Read-volume counters for one [`Storage`]'s lifetime.
///
/// Whether a predicate was pushed is not the same question as what a push
/// bought: a parquet `RowFilter` can be installed on every file and still read
/// every byte, because it decides per row after the predicate columns are
/// decoded. Only pruning avoids IO. These counters separate the two.
///
/// `bytes_read` and `io_calls` are counted at the `AsyncFileReader` boundary,
/// which makes them exact and independent of the OS page cache: a warm re-read
/// reports the same bytes as a cold one. Wall-clock does not have that property,
/// which is what makes these the transferable numbers when comparing read paths.
/// The boundary also bounds the scope: footer fetches happen before the
/// counting wrapper is installed and log-file IO never crosses it, so these
/// two count base-file column-chunk reads only.
///
/// All fields are `AtomicU64` under an `Arc` because the parquet stream is
/// polled on whichever worker thread drives it, while a consumer may read the
/// counters from another. `Relaxed` throughout: these are advisory counters, and
/// the happens-before that makes them visible is the consumer draining the
/// stream.
#[derive(Debug, Default)]
pub struct ReadVolume {
    /// Bytes actually fetched from the object store, summed over every range read.
    pub bytes_read: AtomicU64,
    /// Number of `get_bytes` / `get_byte_ranges` calls — round trips, not ranges.
    /// A two-pass read (predicate columns, then the selected rows) shows up here
    /// as roughly double the calls of a single-pass read over the same file.
    pub io_calls: AtomicU64,
    /// Row groups the reader was configured to scan. Equal to `file_row_groups`
    /// until something prunes; the gap between the two is what pruning bought.
    pub row_groups_read: AtomicU64,
    /// Row groups the file contains. Denominator for the line above.
    pub file_row_groups: AtomicU64,
    /// Times the row-group selector closure actually RAN.
    ///
    /// Separate from its outcome on purpose. A selector returns `None` when it
    /// cannot prune anything, so `row_groups_read == file_row_groups` reads the
    /// same whether the selector ran and found nothing or was never installed.
    /// Only this counter separates them.
    pub row_group_selector_calls: AtomicU64,
    /// Times a selector WAS installed by the caller but the merge-safety gate
    /// refused to pass it down.
    ///
    /// Without this the gate silently defeats the counter above: a suppressed
    /// selector is a third state that also reads zero calls. Read the two
    /// together:
    ///   calls > 0                   the selector ran
    ///   calls == 0, suppressed > 0  the gate refused it (the read merges, and
    ///                               the predicate is not primary-key-safe)
    ///   calls == 0, suppressed == 0 no caller ever installed one
    pub row_group_selector_suppressed: AtomicU64,
    /// Rows the file contains, from parquet metadata.
    pub file_rows: AtomicU64,
    /// Rows the stream actually yielded, after any row filter. `file_rows -
    /// rows_out` is what filtering removed; `bytes_read` says what it cost to
    /// remove it.
    pub rows_out: AtomicU64,
}

impl ReadVolume {
    /// One completed fetch: its bytes, and the round trip that carried them.
    pub(crate) fn add_bytes(&self, n: u64) {
        self.bytes_read.fetch_add(n, Ordering::Relaxed);
        self.io_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// What the file holds, read off the footer the reader has already fetched.
    pub(crate) fn record_file_shape(&self, row_groups: u64, rows: u64) {
        self.file_row_groups
            .fetch_add(row_groups, Ordering::Relaxed);
        self.file_rows.fetch_add(rows, Ordering::Relaxed);
    }

    pub(crate) fn add_row_groups_read(&self, n: u64) {
        self.row_groups_read.fetch_add(n, Ordering::Relaxed);
    }

    /// The selector ran. Counted whether or not it managed to prune.
    pub(crate) fn record_selector_call(&self) {
        self.row_group_selector_calls
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A caller installed a selector and the safety gate declined to pass it on.
    pub(crate) fn record_selector_suppressed(&self) {
        self.row_group_selector_suppressed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_rows_out(&self, n: u64) {
        self.rows_out.fetch_add(n, Ordering::Relaxed);
    }
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
                read_volume: Arc::new(ReadVolume::default()),
            })),
            Err(e) => Err(Creation(format!("Failed to create storage: {e}"))),
        }
    }

    /// Clone of this `Storage`'s read-volume counters, for a consumer that
    /// outlives the read and reports them once the stream has drained.
    pub fn read_volume(&self) -> Arc<ReadVolume> {
        self.read_volume.clone()
    }

    /// Build storage over a caller-supplied object store.
    ///
    /// Test-only, so a test can wrap the real store and observe the requests a
    /// reader makes. Note that a wrapper takes the trait's default `get_ranges`,
    /// which coalesces, where `LocalFileSystem` overrides it and does not: the
    /// counts a test sees are therefore the ones an object store would serve, not
    /// the ones the local filesystem would.
    #[cfg(test)]
    pub(crate) fn new_with_object_store(
        base_url: Url,
        object_store: Arc<dyn ObjectStore>,
        hudi_configs: Arc<HudiConfigs>,
    ) -> Arc<Storage> {
        Arc::new(Storage {
            base_url: Arc::new(base_url),
            object_store,
            options: Arc::new(HashMap::new()),
            hudi_configs,
            read_volume: Arc::new(ReadVolume::default()),
        })
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
}
