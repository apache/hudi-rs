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
//! Lance implementation of [`BaseFileReader`].
//!
//! Reads individual Lance data files (single-file datasets produced by
//! Hudi's Lance integration) using the `lance-file` crate.

use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow_schema::SchemaRef;
use futures::StreamExt;
use futures::future::BoxFuture;
use lance_core::cache::LanceCache;
use lance_core::utils::tokio::get_num_compute_intensive_cpus;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::LanceEncodingsIo;
use lance_file::reader::{CachedFileMetadata, FileReader, FileReaderOptions, ReaderProjection};
use lance_io::ReadBatchParams;
use lance_io::object_store::{ObjectStore as LanceObjectStore, ObjectStoreRegistry};
use lance_io::scheduler::{FileScheduler, ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use object_store::path::Path as ObjPath;
use tokio::sync::OnceCell;

use super::reader::{BaseFileReadOptions, BaseFileReader, BaseFileStream};
use crate::statistics::{ColumnStatistics, StatisticsContainer, StatsGranularity};
use crate::storage::Storage;
use crate::storage::error::{Result, StorageError};
use crate::storage::file_metadata::FileMetadata;
use crate::storage::util::join_url_segments;

const DEFAULT_BATCH_SIZE: u32 = 8192;

/// Lance implementation of [`BaseFileReader`].
///
/// Each Hudi base file with `.lance` extension is a standalone Lance data
/// file. The Lance object store and scan scheduler are built once per
/// reader and reused across every file open.
pub struct LanceBaseFileReader {
    storage: Arc<Storage>,
    /// Lazily-initialized Lance scheduler. The scheduler holds the
    /// Lance-wrapped object store, so caching it covers both pieces of
    /// per-table infrastructure.
    scheduler: OnceCell<Arc<ScanScheduler>>,
}

impl LanceBaseFileReader {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
            scheduler: OnceCell::new(),
        }
    }

    /// Returns the Lance scan scheduler shared across all file reads on
    /// this reader, initializing it on first use.
    async fn scheduler(&self) -> Result<Arc<ScanScheduler>> {
        let scheduler = self
            .scheduler
            .get_or_try_init(|| async {
                let storage_accessor = Arc::new(
                    lance_io::object_store::StorageOptionsAccessor::with_static_options(
                        (*self.storage.options).clone(),
                    ),
                );
                // `ObjectStoreParams::object_store` is marked deprecated in lance-io
                // 4.0.x in favor of implementing `ObjectStoreProvider`. We still set
                // it here so Lance reuses the ObjectStore we already built (with
                // hudi-rs's storage options applied) and skips re-resolving
                // credentials. `storage_options_accessor` itself is current API.
                #[allow(deprecated)]
                let params = lance_io::object_store::ObjectStoreParams {
                    object_store: Some((
                        self.storage.object_store.clone(),
                        (*self.storage.base_url).clone(),
                    )),
                    storage_options_accessor: Some(storage_accessor),
                    ..Default::default()
                };

                let (lance_store, _root_path) = LanceObjectStore::from_uri_and_params(
                    Arc::new(ObjectStoreRegistry::default()),
                    self.storage.base_url.as_str(),
                    &params,
                )
                .await
                .map_err(|e| {
                    StorageError::Creation(format!(
                        "Failed to create Lance object store for {}: {e}",
                        self.storage.base_url
                    ))
                })?;

                let scheduler_config = SchedulerConfig::max_bandwidth(&lance_store);
                Ok::<_, StorageError>(ScanScheduler::new(lance_store, scheduler_config))
            })
            .await?;
        Ok(scheduler.clone())
    }

    /// Open a Lance `FileScheduler` for `relative_path`. Pass `known_size`
    /// when the caller has already discovered the file size to skip
    /// Lance's internal size lookup.
    async fn open_file_scheduler(
        &self,
        relative_path: &str,
        known_size: Option<u64>,
    ) -> Result<FileScheduler> {
        let scheduler = self.scheduler().await?;
        let obj_url = join_url_segments(&self.storage.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let cached_size = match known_size {
            Some(size) => CachedFileSize::new(size),
            None => CachedFileSize::unknown(),
        };
        scheduler
            .open_file(&obj_path, &cached_size)
            .await
            .map_err(|e| {
                StorageError::Creation(format!("Failed to open Lance file {relative_path}: {e}"))
            })
    }

    /// Build a `ReaderProjection` from the requested column names. Empty
    /// projections return `Ok(None)`: lance-file rejects zero-column
    /// projections, so callers that want a row-count-preserving scan must
    /// handle the empty case themselves. `ReaderProjection::from_column_names`
    /// is case-sensitive: every column in `col_names` must match the Lance file
    /// schema exactly. lance-core has case-insensitive helpers, but
    /// `Schema::project` does not use them here.
    fn build_projection(
        col_names: &[String],
        metadata: &CachedFileMetadata,
        relative_path: &str,
    ) -> Result<Option<ReaderProjection>> {
        if col_names.is_empty() {
            return Ok(None);
        }
        let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
        ReaderProjection::from_column_names(
            metadata.version(),
            metadata.file_schema.as_ref(),
            &col_refs,
        )
        .map(Some)
        .map_err(|e| {
            StorageError::Creation(format!(
                "Failed to create Lance projection for {relative_path}: {e}"
            ))
        })
    }

    /// Open a Lance `FileReader` and return the file metadata used to
    /// build it. The metadata is read once and reused via
    /// [`FileReader::try_open_with_file_metadata`] so callers don't pay
    /// for the read twice.
    async fn open_file_reader_with_metadata(
        &self,
        relative_path: &str,
        projection: Option<&[String]>,
        known_size: Option<u64>,
    ) -> Result<(FileReader, Arc<CachedFileMetadata>)> {
        let file_scheduler = self.open_file_scheduler(relative_path, known_size).await?;
        let metadata = Arc::new(
            FileReader::read_all_metadata(&file_scheduler)
                .await
                .map_err(|e| {
                    StorageError::Creation(format!(
                        "Failed to read Lance metadata for {relative_path}: {e}"
                    ))
                })?,
        );

        let base_projection = match projection {
            Some(cols) => Self::build_projection(cols, &metadata, relative_path)?,
            None => None,
        };

        // `no_cache` is fine for sequential full-file scans; the file metadata is
        // already shared via `Arc<CachedFileMetadata>` (see try_open_with_file_metadata).
        // Switch to `LanceCache::with_capacity(...)` if we ever add take/range workloads.
        let cache = LanceCache::no_cache();
        let path = file_scheduler.reader().path().clone();
        let options = FileReaderOptions::default();
        let encodings_io =
            LanceEncodingsIo::new(file_scheduler).with_read_chunk_size(options.read_chunk_size);

        let reader = FileReader::try_open_with_file_metadata(
            Arc::new(encodings_io),
            path,
            base_projection,
            Arc::new(DecoderPlugins {}),
            metadata.clone(),
            &cache,
            options,
        )
        .await
        .map_err(|e| {
            StorageError::Creation(format!(
                "Failed to open Lance file reader for {relative_path}: {e}"
            ))
        })?;

        Ok((reader, metadata))
    }

    /// Returns the Arrow schema of the Lance base file.
    ///
    /// Mirrors [`super::parquet::ParquetBaseFileReader::get_schema`] so that
    /// schema-resolution callers can fall back to the base file when commit
    /// metadata does not carry an explicit schema.
    pub async fn get_schema(&self, relative_path: &str) -> Result<arrow_schema::Schema> {
        let (reader, _) = self
            .open_file_reader_with_metadata(relative_path, None, None)
            .await?;
        Ok(arrow_schema::Schema::from(reader.schema().as_ref()))
    }

    /// Build a single zero-column [`RecordBatch`] with the Lance file's
    /// row count. Callers requesting an empty projection
    /// (`Some(vec![])`) get row-count-only semantics without tripping
    /// `lance-file`'s zero-column-projection rejection.
    async fn empty_projection_stream(
        &self,
        relative_path: &str,
        known_size: Option<u64>,
    ) -> Result<BaseFileStream> {
        let file_scheduler = self.open_file_scheduler(relative_path, known_size).await?;
        let metadata = FileReader::read_all_metadata(&file_scheduler)
            .await
            .map_err(|e| {
                StorageError::Creation(format!(
                    "Failed to read Lance metadata for {relative_path}: {e}"
                ))
            })?;

        let empty_schema: SchemaRef = Arc::new(arrow_schema::Schema::empty());
        let row_count = usize::try_from(metadata.num_rows).unwrap_or(usize::MAX);
        let batch = RecordBatch::try_new_with_options(
            empty_schema.clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
        .map_err(|e| {
            StorageError::Creation(format!(
                "Failed to build empty RecordBatch for {relative_path}: {e}"
            ))
        })?;
        let stream = futures::stream::once(futures::future::ready(Ok(batch))).boxed();
        Ok(BaseFileStream::new(empty_schema, stream))
    }
}

impl BaseFileReader for LanceBaseFileReader {
    fn read_stream<'a>(
        &'a self,
        relative_path: &'a str,
        options: BaseFileReadOptions,
    ) -> BoxFuture<'a, Result<BaseFileStream>> {
        Box::pin(async move {
            // `Some(vec![])` is the row-count-only request shape on
            // `BaseFileReadOptions`. lance-file rejects zero-column
            // projections, so emit a single empty-schema batch carrying
            // the row count from file metadata instead.
            if options.projection.as_ref().is_some_and(|p| p.is_empty()) {
                return self
                    .empty_projection_stream(relative_path, options.known_file_size)
                    .await;
            }

            let (reader, _) = self
                .open_file_reader_with_metadata(
                    relative_path,
                    options.projection.as_deref(),
                    options.known_file_size,
                )
                .await?;

            let batch_size = options
                .batch_size
                .map(|size| u32::try_from(size).unwrap_or(u32::MAX))
                .unwrap_or(DEFAULT_BATCH_SIZE);
            let batch_readahead =
                u32::try_from(get_num_compute_intensive_cpus()).unwrap_or(u32::MAX);
            let lance_stream = reader
                .read_stream(
                    ReadBatchParams::RangeFull,
                    batch_size,
                    batch_readahead,
                    // lance-file 4.0.x decoders do not act on FilterExpression:
                    // per lance-encoding, "the core decoders do not currently
                    // take advantage of filtering in any way." Callers needing
                    // row-level predicates must apply them on the returned
                    // record batches.
                    FilterExpression::no_filter(),
                )
                .map_err(|e| {
                    StorageError::Creation(format!(
                        "Failed to create Lance read stream for {relative_path}: {e}"
                    ))
                })?;

            let arrow_schema = lance_stream.schema().clone();

            let mapped_stream = lance_stream
                .map(|result| result.map_err(StorageError::from))
                .boxed();

            Ok(BaseFileStream::new(arrow_schema, mapped_stream))
        })
    }

    fn get_metadata_and_stats<'a>(
        &'a self,
        relative_path: &'a str,
        _table_schema: &'a arrow_schema::Schema,
    ) -> BoxFuture<'a, Result<(FileMetadata, StatisticsContainer)>> {
        Box::pin(async move {
            let obj_url = join_url_segments(&self.storage.base_url, &[relative_path])?;
            let obj_path = ObjPath::from_url_path(obj_url.path())?;
            let head = self.storage.object_store.head(&obj_path).await?;
            let file_size = head.size;

            // Open with the size we just discovered so Lance doesn't issue
            // its own head request.
            let (reader, _) = self
                .open_file_reader_with_metadata(relative_path, None, Some(file_size))
                .await?;

            let name = std::path::Path::new(relative_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(relative_path)
                .to_string();

            let num_rows = i64::try_from(reader.num_rows()).unwrap_or(i64::MAX);

            let file_metadata = FileMetadata {
                name,
                size: file_size,
                byte_size: i64::try_from(file_size).unwrap_or(i64::MAX),
                num_records: num_rows,
            };

            // lance-file 4.0.x v2 format does not expose per-column min/max via
            // `FileReader` (only num_pages and size_bytes via `FileStatistics`).
            // Populate an entry per file column with empty bounds so column-level
            // pruning falls back to "include".
            let mut col_stats = StatisticsContainer::new(StatsGranularity::File);
            col_stats.num_rows = Some(num_rows);
            let file_schema = reader.schema();
            for field in &file_schema.fields {
                let field_name = field.name.clone();
                col_stats.columns.insert(
                    field_name.clone(),
                    ColumnStatistics::new(field_name, field.data_type()),
                );
            }

            Ok((file_metadata, col_stats))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hudi_test::SampleTable;
    use url::Url;

    fn lance_test_storage() -> Arc<Storage> {
        let table_path = SampleTable::V9LanceNonpartitioned.path_to_cow();
        let base_url = Url::from_directory_path(&table_path).unwrap();
        Storage::new_with_base_url(base_url).unwrap()
    }

    fn lance_base_file_name() -> String {
        let table_path = SampleTable::V9LanceNonpartitioned.path_to_cow();
        std::fs::read_dir(&table_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.path().extension().is_some_and(|ext| ext == "lance"))
            .unwrap()
            .file_name()
            .to_str()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn test_lance_reader_read_data_with_projection() {
        let storage = lance_test_storage();
        let reader = LanceBaseFileReader::new(storage);
        let file_name = lance_base_file_name();

        let full = reader
            .read_data(&file_name, BaseFileReadOptions::new())
            .await
            .unwrap();

        let opts = BaseFileReadOptions::new().with_projection(["id", "name"]);
        let projected = reader.read_data(&file_name, opts).await.unwrap();

        assert_eq!(projected.num_rows(), full.num_rows());
        assert_eq!(projected.num_columns(), 2);
        let schema = projected.schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(col_names.contains(&"id"));
        assert!(col_names.contains(&"name"));
    }

    #[tokio::test]
    async fn test_lance_reader_metadata_returns_correct_row_count_and_stats() {
        let storage = lance_test_storage();
        let reader = LanceBaseFileReader::new(storage.clone());
        let file_name = lance_base_file_name();

        let table_schema = reader.get_schema(&file_name).await.unwrap();
        let (metadata, stats) = reader
            .get_metadata_and_stats(&file_name, &table_schema)
            .await
            .unwrap();

        assert!(metadata.num_records > 0);
        assert!(metadata.size > 0);
        assert!(metadata.name.ends_with(".lance"));

        let batch = reader
            .read_data(&file_name, BaseFileReadOptions::new())
            .await
            .unwrap();
        assert_eq!(metadata.num_records, batch.num_rows() as i64);

        // Stats are populated with row count and one entry per schema column
        // (Lance file metadata does not surface per-column min/max).
        assert_eq!(stats.num_rows, Some(metadata.num_records));
        assert_eq!(stats.columns.len(), table_schema.fields().len());
        for field in table_schema.fields() {
            let col = stats
                .columns
                .get(field.name())
                .expect("column entry present");
            assert!(col.min_value.is_none());
            assert!(col.max_value.is_none());
        }
    }

    #[tokio::test]
    async fn test_lance_reader_get_schema_matches_data_schema_with_expected_fields() {
        let storage = lance_test_storage();
        let reader = LanceBaseFileReader::new(storage);
        let file_name = lance_base_file_name();

        let schema = reader.get_schema(&file_name).await.unwrap();
        let batch = reader
            .read_data(&file_name, BaseFileReadOptions::new())
            .await
            .unwrap();
        assert!(batch.num_rows() > 0);
        assert_eq!(schema.fields(), batch.schema().fields());
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"id"));
        assert!(field_names.contains(&"name"));
    }

    #[tokio::test]
    async fn test_lance_reader_streaming_matches_eager() {
        let storage = lance_test_storage();
        let reader = LanceBaseFileReader::new(storage);
        let file_name = lance_base_file_name();

        let eager = reader
            .read_data(&file_name, BaseFileReadOptions::new())
            .await
            .unwrap();

        let mut stream = reader
            .read_stream(&file_name, BaseFileReadOptions::new())
            .await
            .unwrap();

        let mut stream_batches = Vec::new();
        while let Some(batch) = stream.next().await {
            stream_batches.push(batch.unwrap());
        }

        let total_rows: usize = stream_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, eager.num_rows());

        assert_eq!(stream_batches[0].schema().fields(), eager.schema().fields());
    }

    #[tokio::test]
    async fn test_lance_reader_stream_with_projection_schema_matches_batches() {
        let storage = lance_test_storage();
        let reader = LanceBaseFileReader::new(storage);
        let file_name = lance_base_file_name();

        let opts = BaseFileReadOptions::new().with_projection(["id", "name"]);
        let stream = reader.read_stream(&file_name, opts).await.unwrap();

        let stream_schema = stream.schema().clone();
        assert_eq!(stream_schema.fields().len(), 2);

        let mut batches = Vec::new();
        let mut inner = stream.into_stream();
        while let Some(batch) = inner.next().await {
            batches.push(batch.unwrap());
        }

        assert!(!batches.is_empty());
        for batch in &batches {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().fields(), stream_schema.fields());
        }
    }

    #[tokio::test]
    async fn test_lance_reader_empty_projection_yields_zero_column_batch_with_row_count() {
        let storage = lance_test_storage();
        let reader = LanceBaseFileReader::new(storage);
        let file_name = lance_base_file_name();

        // Sanity-check the row count via an unprojected read.
        let full = reader
            .read_data(&file_name, BaseFileReadOptions::new())
            .await
            .unwrap();
        let expected_rows = full.num_rows();
        assert!(expected_rows > 0);

        // Empty projection: lance-file would normally reject this, but the
        // reader special-cases it for row-count-preserving scans.
        let opts = BaseFileReadOptions::new().with_projection(Vec::<String>::new());
        let stream = reader.read_stream(&file_name, opts).await.unwrap();
        assert_eq!(stream.schema().fields().len(), 0);

        let mut total_rows = 0;
        let mut inner = stream.into_stream();
        while let Some(batch) = inner.next().await {
            let batch = batch.unwrap();
            assert_eq!(batch.num_columns(), 0);
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, expected_rows);
    }

    #[tokio::test]
    async fn test_lance_reader_with_batch_size_yields_multiple_batches() {
        let storage = lance_test_storage();
        let reader = LanceBaseFileReader::new(storage);
        let file_name = lance_base_file_name();

        let eager = reader
            .read_data(&file_name, BaseFileReadOptions::new())
            .await
            .unwrap();
        assert!(
            eager.num_rows() > 1,
            "fixture must have at least 2 rows for the batch-split assertion"
        );

        // Force the smallest possible batch size to exercise the explicit
        // `batch_size` branch and assert that lance-file honors it.
        let opts = BaseFileReadOptions::new().with_batch_size(1);
        let stream = reader.read_stream(&file_name, opts).await.unwrap();

        let mut batches = Vec::new();
        let mut inner = stream.into_stream();
        while let Some(batch) = inner.next().await {
            batches.push(batch.unwrap());
        }

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, eager.num_rows());
        assert!(
            batches.len() > 1,
            "with batch_size=1 we expect multiple batches, got {}",
            batches.len()
        );
        for batch in &batches {
            assert!(batch.num_rows() <= 1);
        }
    }

    #[tokio::test]
    async fn test_lance_reader_with_known_file_size_matches_unsized_read() {
        // Exercises the `with_known_file_size` Some-branch in
        // `open_file_scheduler` (which builds CachedFileSize::new instead of
        // CachedFileSize::unknown). The result must match an ordinary read.
        let storage = lance_test_storage();
        let reader = LanceBaseFileReader::new(storage.clone());
        let file_name = lance_base_file_name();

        let table_path = SampleTable::V9LanceNonpartitioned.path_to_cow();
        let file_size = std::fs::metadata(std::path::Path::new(&table_path).join(&file_name))
            .unwrap()
            .len();

        let unsized_batch = reader
            .read_data(&file_name, BaseFileReadOptions::new())
            .await
            .unwrap();
        let sized_batch = reader
            .read_data(
                &file_name,
                BaseFileReadOptions::new().with_known_file_size(file_size),
            )
            .await
            .unwrap();
        assert_eq!(sized_batch.num_rows(), unsized_batch.num_rows());
        assert_eq!(
            sized_batch.schema().fields(),
            unsized_batch.schema().fields()
        );
    }

    #[tokio::test]
    async fn test_lance_reader_unknown_projection_column_returns_error() {
        let storage = lance_test_storage();
        let reader = LanceBaseFileReader::new(storage);
        let file_name = lance_base_file_name();

        let opts = BaseFileReadOptions::new().with_projection(["does_not_exist"]);
        let err = reader
            .read_data(&file_name, opts)
            .await
            .expect_err("projection on missing column must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("Failed to create Lance projection"),
            "expected projection-error message, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_lance_reader_missing_file_returns_error() {
        let storage = lance_test_storage();
        let reader = LanceBaseFileReader::new(storage);

        // A path that lives under the same base URL but doesn't exist on disk.
        let err = reader
            .read_data("does_not_exist.lance", BaseFileReadOptions::new())
            .await
            .expect_err("missing file must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("Failed to open Lance file") || msg.contains("does_not_exist.lance"),
            "expected missing-file error to mention the path, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_lance_reader_get_metadata_and_stats_missing_file_returns_error() {
        let storage = lance_test_storage();
        let reader = LanceBaseFileReader::new(storage);
        let table_schema = arrow_schema::Schema::empty();

        // Exercises the head() error path before any Lance call is made.
        let err = reader
            .get_metadata_and_stats("does_not_exist.lance", &table_schema)
            .await
            .expect_err("missing file must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("does_not_exist") || msg.contains("not found"),
            "expected missing-file error, got: {msg}"
        );
    }
}
