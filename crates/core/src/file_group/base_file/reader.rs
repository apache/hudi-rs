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

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::config::table::BaseFileFormatValue;
use crate::statistics::StatisticsContainer;
use crate::storage::error::{Result, StorageError};
use crate::storage::file_metadata::FileMetadata;
use crate::storage::{RowFilterBuilder, Storage};

/// Which record keys a read is interested in.
///
/// Mirrors the two shapes Hudi pushes into a metadata read: `Predicates.in` over
/// a full key set, and `startsWithAny` over prefixes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyPredicate {
    /// Exactly these record keys.
    Keys(Vec<String>),
    /// Keys beginning with any of these prefixes.
    Prefixes(Vec<String>),
}

impl KeyPredicate {
    /// A form of this predicate for testing many keys against.
    ///
    /// [`Self::admits`] scans the key list, which is fine for a handful and not
    /// for the thousands a record-index or bloom lookup names: the filter runs
    /// once per record read, so a linear scan makes it records times keys. Build
    /// this once per read and use it instead.
    pub fn matcher(&self) -> KeyMatcher<'_> {
        match self {
            Self::Keys(keys) => KeyMatcher::Keys(keys.iter().map(String::as_str).collect()),
            Self::Prefixes(prefixes) => KeyMatcher::Prefixes(prefixes),
        }
    }

    /// Whether `key` satisfies this predicate.
    ///
    /// Selection reads whole blocks and so returns keys outside the predicate;
    /// this is what the caller filters with. For a large key set, build a
    /// [`Self::matcher`] once rather than calling this per record.
    pub fn admits(&self, key: &str) -> bool {
        match self {
            Self::Keys(keys) => keys.iter().any(|k| k == key),
            Self::Prefixes(prefixes) => prefixes.iter().any(|p| key.starts_with(p.as_str())),
        }
    }
}

/// A [`KeyPredicate`] prepared for repeated tests, borrowing from it.
pub enum KeyMatcher<'a> {
    /// Exact keys, hashed, so a test is constant-time in the key count.
    Keys(std::collections::HashSet<&'a str>),
    /// Prefixes stay a list: there is no hash of "starts with", and a read
    /// carries few prefixes where it can carry very many keys.
    Prefixes(&'a [String]),
}

impl KeyMatcher<'_> {
    /// Whether `key` satisfies the predicate this was built from.
    pub fn admits(&self, key: &str) -> bool {
        match self {
            Self::Keys(keys) => keys.contains(key),
            Self::Prefixes(prefixes) => prefixes.iter().any(|p| key.starts_with(p.as_str())),
        }
    }
}

/// Options for reading a base file.
#[derive(Clone, Default)]
pub struct BaseFileReadOptions {
    /// Target batch size (number of rows per batch) for streaming reads.
    pub batch_size: Option<usize>,
    /// Column projection by names.
    pub projection: Option<Vec<String>>,
    /// Known base-file size in bytes, when the caller already has file metadata.
    pub known_file_size: Option<u64>,
    /// A predicate to evaluate while reading, so whole row groups can be
    /// skipped rather than read and discarded.
    ///
    /// Only the Parquet reader honors this; other formats ignore it. It is a
    /// builder rather than a filter because the predicate has to be resolved
    /// against the file's own schema, which the caller does not have until the
    /// footer is open.
    pub row_filter: Option<RowFilterBuilder>,
    /// Record keys or key prefixes to read, so a key-ordered format can seek to
    /// the blocks that can hold them instead of reading the file.
    ///
    /// Only the HFile reader honors this; other formats ignore it and return
    /// every row, which is the fallback Java takes when a reader reports no key
    /// predicate support. A reader that honors it may still return rows outside
    /// the predicate, because a block is the smallest thing it can read — the
    /// caller filters, and this only changes what is read.
    pub key_predicate: Option<KeyPredicate>,
    /// Name for a synthetic `Int64` column carrying each row's physical position
    /// in the file, appended after the file's own columns.
    ///
    /// The position is the row's index in the file as written, not its index in
    /// what the read returns — a [`row_filter`](Self::row_filter) that skips rows
    /// leaves gaps rather than renumbering. That is what makes it usable as the
    /// merge key for position-based merge, where a log block's positions were
    /// recorded against the unfiltered base file.
    ///
    /// Only the Parquet reader honors this; other formats ignore it. The name is
    /// excluded from [`projection`](Self::projection) matching, since the column
    /// is not one of the file's own.
    pub row_index_column: Option<String>,
}

// `row_filter` holds a closure, which has no `Debug`. Report whether one is set
// rather than dropping the derive from the whole struct.
impl std::fmt::Debug for BaseFileReadOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaseFileReadOptions")
            .field("batch_size", &self.batch_size)
            .field("projection", &self.projection)
            .field("known_file_size", &self.known_file_size)
            .field("key_predicate", &self.key_predicate)
            .field("row_filter", &self.row_filter.is_some())
            .field("row_index_column", &self.row_index_column)
            .finish()
    }
}

impl BaseFileReadOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a predicate to push into the read. See [`Self::row_filter`].
    pub fn with_row_filter(mut self, row_filter: RowFilterBuilder) -> Self {
        self.row_filter = Some(row_filter);
        self
    }

    /// Read only the records a key predicate admits, where the format can seek
    /// by key. See [`Self::key_predicate`].
    pub fn with_key_predicate(mut self, predicate: KeyPredicate) -> Self {
        self.key_predicate = Some(predicate);
        self
    }

    /// Asks for a synthetic row-position column under `name`. See
    /// [`Self::row_index_column`].
    pub fn with_row_index_column(mut self, name: impl Into<String>) -> Self {
        self.row_index_column = Some(name.into());
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Sets the known base-file size in bytes.
    pub fn with_known_file_size(mut self, size: u64) -> Self {
        self.known_file_size = Some(size);
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
    ) -> BoxFuture<'a, Result<RecordBatch>> {
        Box::pin(async move {
            let base_stream = self.read_stream(relative_path, options).await?;
            let schema = base_stream.schema().clone();
            let mut stream = base_stream.into_stream();

            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                batches.push(batch?);
            }

            if batches.is_empty() {
                return Ok(RecordBatch::new_empty(schema));
            }

            Ok(concat_batches(&schema, &batches)?)
        })
    }

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
        BaseFileFormatValue::Lance => Ok(Arc::new(super::lance::LanceBaseFileReader::new(
            storage.clone(),
        ))),
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

    #[test]
    fn test_create_base_file_reader_lance() {
        let storage = test_storage();
        let result = create_base_file_reader(&storage, &BaseFileFormatValue::Lance);
        assert!(result.is_ok());
    }
}
