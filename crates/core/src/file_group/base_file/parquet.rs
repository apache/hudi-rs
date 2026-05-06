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
//! Parquet implementation of [`BaseFileReader`].

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use futures::future::BoxFuture;
use object_store::path::Path as ObjPath;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, parquet_to_arrow_schema};
use parquet::file::metadata::ParquetMetaData;

use super::reader::{BaseFileReadOptions, BaseFileReader, BaseFileStream};
use crate::statistics::StatisticsContainer;
use crate::storage::Storage;
use crate::storage::error::{Result, StorageError};
use crate::storage::file_metadata::FileMetadata;
use crate::storage::util::join_url_segments;

/// Parquet implementation of [`BaseFileReader`].
///
/// Reads Parquet files directly via `object_store` and the `parquet` crate.
pub struct ParquetBaseFileReader {
    storage: Arc<Storage>,
}

impl ParquetBaseFileReader {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    async fn object_path_and_size(&self, relative_path: &str) -> Result<(ObjPath, u64)> {
        let obj_url = join_url_segments(&self.storage.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let meta = self.storage.object_store.head(&obj_path).await?;
        Ok((obj_path, meta.size))
    }

    async fn open_builder_with_size(
        &self,
        obj_path: ObjPath,
        file_size: u64,
    ) -> Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>> {
        let reader = ParquetObjectReader::new(self.storage.object_store.clone(), obj_path)
            .with_file_size(file_size);
        Ok(ParquetRecordBatchStreamBuilder::new(reader).await?)
    }

    async fn open_builder(
        &self,
        relative_path: &str,
    ) -> Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>> {
        let (obj_path, file_size) = self.object_path_and_size(relative_path).await?;
        self.open_builder_with_size(obj_path, file_size).await
    }

    fn apply_options(
        mut builder: ParquetRecordBatchStreamBuilder<ParquetObjectReader>,
        options: &BaseFileReadOptions,
    ) -> Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>> {
        if let Some(batch_size) = options.batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        // Handle projection: convert column names to indices using builder's schema.
        if let Some(ref column_names) = options.projection {
            let arrow_schema = builder.schema();
            let projection: Vec<usize> = column_names
                .iter()
                .map(|name| {
                    arrow_schema.index_of(name).map_err(|_| {
                        let available = arrow_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                            .join(", ");
                        StorageError::InvalidColumn(format!(
                            "Column '{name}' not found in parquet file schema. Available columns: [{available}]"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let projection_mask = parquet::arrow::ProjectionMask::roots(
                builder.parquet_schema(),
                projection.iter().copied(),
            );
            builder = builder.with_projection(projection_mask);
        }

        Ok(builder)
    }

    /// Read the raw Parquet footer metadata.
    ///
    /// Exposed for callers that need format-specific details such as row group
    /// compressed sizes for statistics estimation.
    pub async fn get_parquet_metadata(&self, relative_path: &str) -> Result<ParquetMetaData> {
        let builder = self.open_builder(relative_path).await?;
        Ok(builder.metadata().as_ref().clone())
    }

    /// Get the Arrow schema from a Parquet file's footer.
    pub async fn get_schema(&self, relative_path: &str) -> Result<arrow_schema::Schema> {
        let parquet_meta = self.get_parquet_metadata(relative_path).await?;
        Ok(parquet_to_arrow_schema(
            parquet_meta.file_metadata().schema_descr(),
            None,
        )?)
    }
}

impl BaseFileReader for ParquetBaseFileReader {
    fn read_data<'a>(
        &'a self,
        relative_path: &'a str,
        options: BaseFileReadOptions,
    ) -> BoxFuture<'a, Result<RecordBatch>> {
        Box::pin(async move {
            let builder = self.open_builder(relative_path).await?;
            let builder = Self::apply_options(builder, &options)?;
            let schema = builder.schema().clone();
            let mut stream = builder.build()?;

            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                batches.push(batch?);
            }

            if batches.is_empty() {
                return Ok(RecordBatch::new_empty(schema));
            }

            let schema = batches[0].schema();
            Ok(concat_batches(&schema, &batches)?)
        })
    }

    fn read_stream<'a>(
        &'a self,
        relative_path: &'a str,
        options: BaseFileReadOptions,
    ) -> BoxFuture<'a, Result<BaseFileStream>> {
        Box::pin(async move {
            let builder = self.open_builder(relative_path).await?;
            let builder = Self::apply_options(builder, &options)?;
            let schema = builder.schema().clone();
            let stream = builder.build()?;
            let mapped_stream = stream
                .map(|result| result.map_err(StorageError::from))
                .boxed();

            Ok(BaseFileStream::new(schema, mapped_stream))
        })
    }

    fn get_metadata_and_stats<'a>(
        &'a self,
        relative_path: &'a str,
        table_schema: &'a arrow_schema::Schema,
    ) -> BoxFuture<'a, Result<(FileMetadata, StatisticsContainer)>> {
        Box::pin(async move {
            let (obj_path, file_size) = self.object_path_and_size(relative_path).await?;
            let builder = self.open_builder_with_size(obj_path, file_size).await?;
            let parquet_meta = builder.metadata().as_ref();

            let name = std::path::Path::new(relative_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(relative_path)
                .to_string();

            let num_records = parquet_meta.file_metadata().num_rows().max(0);
            let byte_size: i64 = parquet_meta
                .row_groups()
                .iter()
                .map(|rg| rg.total_byte_size())
                .sum::<i64>()
                .max(0);

            let file_metadata = FileMetadata {
                name,
                size: file_size,
                byte_size,
                num_records,
            };

            let col_stats = StatisticsContainer::from_parquet_metadata(parquet_meta, table_schema);

            Ok((file_metadata, col_stats))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::canonicalize;
    use std::path::Path;
    use url::Url;

    fn test_storage() -> Arc<Storage> {
        let base_url =
            Url::from_directory_path(canonicalize(Path::new("tests/data")).unwrap()).unwrap();
        Storage::new_with_base_url(base_url).unwrap()
    }

    #[tokio::test]
    async fn test_read_data_returns_all_rows() {
        let reader = ParquetBaseFileReader::new(test_storage());
        let batch = reader
            .read_data("a.parquet", BaseFileReadOptions::new())
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), 5);
        assert!(batch.num_columns() > 1);
    }

    #[tokio::test]
    async fn test_read_data_with_projection() {
        let reader = ParquetBaseFileReader::new(test_storage());

        let full = reader
            .read_data("a.parquet", BaseFileReadOptions::new())
            .await
            .unwrap();

        let first_col = full.schema().field(0).name().clone();
        let opts = BaseFileReadOptions::new().with_projection([&first_col]);
        let projected = reader.read_data("a.parquet", opts).await.unwrap();

        assert_eq!(projected.num_columns(), 1);
        assert_eq!(projected.schema().field(0).name(), &first_col);
        assert_eq!(projected.num_rows(), full.num_rows());
    }

    #[tokio::test]
    async fn test_read_stream_matches_read_data() {
        let reader = ParquetBaseFileReader::new(test_storage());

        let eager = reader
            .read_data("a.parquet", BaseFileReadOptions::new())
            .await
            .unwrap();

        let opts = BaseFileReadOptions::new().with_batch_size(2);
        let mut stream = reader.read_stream("a.parquet", opts).await.unwrap();

        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, eager.num_rows());
        assert_eq!(batches[0].schema(), eager.schema());
    }

    #[tokio::test]
    async fn test_get_metadata_and_stats() {
        let reader = ParquetBaseFileReader::new(test_storage());

        let schema = reader.get_schema("a.parquet").await.unwrap();
        let (metadata, stats) = reader
            .get_metadata_and_stats("a.parquet", &schema)
            .await
            .unwrap();

        assert_eq!(metadata.name, "a.parquet");
        assert!(metadata.size > 0);
        assert_eq!(metadata.num_records, 5);
        assert!(!stats.columns.is_empty());
    }

    #[tokio::test]
    async fn test_get_schema() {
        let reader = ParquetBaseFileReader::new(test_storage());
        let schema = reader.get_schema("a.parquet").await.unwrap();
        assert!(!schema.fields().is_empty());
    }

    #[tokio::test]
    async fn test_get_parquet_metadata() {
        let reader = ParquetBaseFileReader::new(test_storage());
        let meta = reader.get_parquet_metadata("a.parquet").await.unwrap();
        assert_eq!(meta.file_metadata().num_rows(), 5);
        assert!(!meta.row_groups().is_empty());
    }
}
