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

use futures::StreamExt;
use futures::future::BoxFuture;
use lance_core::cache::LanceCache;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::reader::{FileReader, FileReaderOptions, ReaderProjection};
use lance_io::ReadBatchParams;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use object_store::path::Path as ObjPath;

use super::reader::{BaseFileReadOptions, BaseFileReader, BaseFileStream};
use crate::statistics::{ColumnStatistics, StatisticsContainer, StatsGranularity};
use crate::storage::Storage;
use crate::storage::error::{Result, StorageError};
use crate::storage::file_metadata::FileMetadata;
use crate::storage::util::join_url_segments;

const DEFAULT_BATCH_SIZE: u32 = 8192;
const DEFAULT_BATCH_READAHEAD: u32 = 16;

/// Lance implementation of [`BaseFileReader`].
///
/// Each Hudi base file with `.lance` extension is a standalone Lance data
/// file containing a single fragment. This reader opens individual files
/// using `lance-file`'s `FileReader`.
pub struct LanceBaseFileReader {
    storage: Arc<Storage>,
}

impl LanceBaseFileReader {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    async fn open_file_reader(
        &self,
        relative_path: &str,
        projection: Option<&[String]>,
    ) -> Result<FileReader> {
        let obj_url = join_url_segments(&self.storage.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;

        let storage_accessor = Arc::new(
            lance_io::object_store::StorageOptionsAccessor::with_static_options(
                (*self.storage.options).clone(),
            ),
        );
        // `ObjectStoreParams::storage_options_accessor` is marked deprecated in
        // lance-io 4.0.x but is still the only supported way to forward
        // hudi-rs's storage option map (e.g. cloud credentials, custom
        // endpoints) into Lance's provider prefix calculation. The replacement
        // API is not yet exposed by lance-io; revisit when upgrading lance-io.
        #[allow(deprecated)]
        let params = lance_io::object_store::ObjectStoreParams {
            object_store: Some((
                self.storage.object_store.clone(),
                (*self.storage.base_url).clone(),
            )),
            storage_options_accessor: Some(storage_accessor),
            ..Default::default()
        };

        let lance_store = lance_io::object_store::ObjectStore::from_uri_and_params(
            Arc::new(lance_io::object_store::ObjectStoreRegistry::default()),
            obj_url.as_str(),
            &params,
        )
        .await
        .map_err(|e| {
            StorageError::Creation(format!(
                "Failed to create Lance object store for {relative_path}: {e}"
            ))
        })?;

        let scheduler_config = SchedulerConfig::max_bandwidth(&lance_store.0);
        let scheduler = ScanScheduler::new(lance_store.0, scheduler_config);

        let file_scheduler = scheduler
            .open_file(&obj_path, &CachedFileSize::unknown())
            .await
            .map_err(|e| {
                StorageError::Creation(format!("Failed to open Lance file {relative_path}: {e}"))
            })?;

        let cache = LanceCache::no_cache();

        let base_projection = if let Some(col_names) = projection {
            let metadata = FileReader::read_all_metadata(&file_scheduler)
                .await
                .map_err(|e| {
                    StorageError::Creation(format!(
                        "Failed to read Lance metadata for {relative_path}: {e}"
                    ))
                })?;
            let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
            let proj = ReaderProjection::from_column_names(
                metadata.version(),
                metadata.file_schema.as_ref(),
                &col_refs,
            )
            .map_err(|e| {
                StorageError::Creation(format!(
                    "Failed to create Lance projection for {relative_path}: {e}"
                ))
            })?;
            Some(proj)
        } else {
            None
        };

        FileReader::try_open(
            file_scheduler,
            base_projection,
            Arc::new(DecoderPlugins {}),
            &cache,
            FileReaderOptions::default(),
        )
        .await
        .map_err(|e| {
            StorageError::Creation(format!(
                "Failed to open Lance file reader for {relative_path}: {e}"
            ))
        })
    }

    /// Returns the Arrow schema of the Lance base file.
    ///
    /// Mirrors [`super::parquet::ParquetBaseFileReader::get_schema`] so that
    /// schema-resolution callers can fall back to the base file when commit
    /// metadata does not carry an explicit schema.
    pub async fn get_schema(&self, relative_path: &str) -> Result<arrow_schema::Schema> {
        let reader = self.open_file_reader(relative_path, None).await?;
        Ok(arrow_schema::Schema::from(reader.schema().as_ref()))
    }
}

impl BaseFileReader for LanceBaseFileReader {
    fn read_stream<'a>(
        &'a self,
        relative_path: &'a str,
        options: BaseFileReadOptions,
    ) -> BoxFuture<'a, Result<BaseFileStream>> {
        Box::pin(async move {
            let reader = self
                .open_file_reader(relative_path, options.projection.as_deref())
                .await?;

            let batch_size = options.batch_size.unwrap_or(DEFAULT_BATCH_SIZE as usize) as u32;
            let lance_stream = reader
                .read_stream(
                    ReadBatchParams::RangeFull,
                    batch_size,
                    DEFAULT_BATCH_READAHEAD,
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
        table_schema: &'a arrow_schema::Schema,
    ) -> BoxFuture<'a, Result<(FileMetadata, StatisticsContainer)>> {
        Box::pin(async move {
            let reader = self.open_file_reader(relative_path, None).await?;

            let name = std::path::Path::new(relative_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(relative_path)
                .to_string();

            let obj_url = join_url_segments(&self.storage.base_url, &[relative_path])?;
            let obj_path = ObjPath::from_url_path(obj_url.path())?;
            let meta = self.storage.object_store.head(&obj_path).await?;

            let num_rows = reader.num_rows() as i64;

            let file_metadata = FileMetadata {
                name,
                size: meta.size as u64,
                byte_size: meta.size as i64,
                num_records: num_rows,
            };

            // lance-file 4.0.x exposes file-level row count and schema but no
            // per-column min/max via `FileReader`. Populate row count and an
            // entry per schema column with empty bounds so callers receive a
            // consistent shape; column-level pruning falls back to "include".
            let mut col_stats = StatisticsContainer::new(StatsGranularity::File);
            col_stats.num_rows = Some(num_rows);
            for field in table_schema.fields() {
                col_stats.columns.insert(
                    field.name().clone(),
                    ColumnStatistics::new(field.name().clone(), field.data_type().clone()),
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
    async fn test_lance_reader_read_data_schema_correctness() {
        let storage = lance_test_storage();
        let reader = LanceBaseFileReader::new(storage);
        let file_name = lance_base_file_name();

        let batch = reader
            .read_data(&file_name, BaseFileReadOptions::new())
            .await
            .unwrap();

        assert!(batch.num_rows() > 0);
        let schema = batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"id"));
        assert!(field_names.contains(&"name"));
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
    async fn test_lance_reader_get_schema_matches_data_schema() {
        let storage = lance_test_storage();
        let reader = LanceBaseFileReader::new(storage);
        let file_name = lance_base_file_name();

        let schema = reader.get_schema(&file_name).await.unwrap();
        let batch = reader
            .read_data(&file_name, BaseFileReadOptions::new())
            .await
            .unwrap();
        assert_eq!(schema.fields(), batch.schema().fields());
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
}
