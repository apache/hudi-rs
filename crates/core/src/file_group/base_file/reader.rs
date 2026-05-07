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
//! Hudi base-file reader abstraction for format-polymorphic reads.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::config::table::BaseFileFormatValue;
use crate::statistics::StatisticsContainer;
use crate::storage::Storage;
use crate::storage::error::{Result, StorageError};
use crate::storage::file_metadata::FileMetadata;

/// Options for reading a base file.
#[derive(Clone, Debug, Default)]
pub struct BaseFileReadOptions {
    /// Target batch size (number of rows per batch) for streaming reads.
    pub batch_size: Option<usize>,
    /// Column projection by names.
    pub projection: Option<Vec<String>>,
}

impl BaseFileReadOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Sets column projection by column names.
    pub fn with_projection<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.projection = Some(columns.into_iter().map(|s| s.into()).collect());
        self
    }
}

/// A stream of record batches from a base file with its schema.
pub struct BaseFileStream {
    schema: SchemaRef,
    stream: BoxStream<'static, Result<RecordBatch>>,
}

impl BaseFileStream {
    pub fn new(schema: SchemaRef, stream: BoxStream<'static, Result<RecordBatch>>) -> Self {
        Self { schema, stream }
    }

    /// Returns the Arrow schema of the base file.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Consumes self and returns the inner stream.
    pub fn into_stream(self) -> BoxStream<'static, Result<RecordBatch>> {
        self.stream
    }
}

impl futures::Stream for BaseFileStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

/// Trait for reading base files in a format-agnostic way.
pub trait BaseFileReader: Send + Sync {
    /// Read all data from a base file, returning a single concatenated RecordBatch.
    fn read_data<'a>(
        &'a self,
        relative_path: &'a str,
        options: BaseFileReadOptions,
    ) -> BoxFuture<'a, Result<RecordBatch>>;

    /// Read data from a base file as a stream of RecordBatches.
    fn read_stream<'a>(
        &'a self,
        relative_path: &'a str,
        options: BaseFileReadOptions,
    ) -> BoxFuture<'a, Result<BaseFileStream>>;

    /// Get file metadata and column statistics from a base file.
    fn get_metadata_and_stats<'a>(
        &'a self,
        relative_path: &'a str,
        table_schema: &'a arrow_schema::Schema,
    ) -> BoxFuture<'a, Result<(FileMetadata, StatisticsContainer)>>;
}

/// Create a [`BaseFileReader`] for a regular table base-file format.
///
/// Metadata-table HFile data uses a dedicated reader path instead of the
/// generic base-file reader abstraction.
pub fn create_base_file_reader(
    storage: &Arc<Storage>,
    format: &BaseFileFormatValue,
) -> Result<Arc<dyn BaseFileReader>> {
    match format {
        BaseFileFormatValue::Parquet => Ok(Arc::new(super::parquet::ParquetBaseFileReader::new(
            storage.clone(),
        ))),
        BaseFileFormatValue::HFile => Err(StorageError::UnsupportedBaseFileFormat(
            "hfile is only supported through the metadata-table HFile reader".to_string(),
        )),
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

    #[test]
    fn test_create_base_file_reader_parquet() {
        let storage = test_storage();
        let reader = create_base_file_reader(&storage, &BaseFileFormatValue::Parquet);
        assert!(reader.is_ok());
    }

    #[test]
    fn test_create_base_file_reader_hfile_is_unsupported() {
        let storage = test_storage();
        let reader = create_base_file_reader(&storage, &BaseFileFormatValue::HFile);
        match reader {
            Err(StorageError::UnsupportedBaseFileFormat(_)) => {}
            Ok(_) => panic!("HFile should not create a generic base-file reader"),
            Err(err) => panic!("Expected unsupported HFile error, got {err}"),
        }
    }
}
