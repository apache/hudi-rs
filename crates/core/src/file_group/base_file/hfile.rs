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

//! HFile implementation of [`BaseFileReader`].
//!
//! Reads an HFile base file, the base-file format of Hudi's metadata table.

use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, RecordBatch, RecordBatchOptions, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::StreamExt;
use futures::future::BoxFuture;
use object_store::path::Path as ObjPath;

use super::reader::{BaseFileReadOptions, BaseFileReader, BaseFileStream};
use crate::hfile::HFileReader;
use crate::statistics::{StatisticsContainer, StatsGranularity};
use crate::storage::Storage;
use crate::storage::error::{Result, StorageError};
use crate::storage::file_metadata::FileMetadata;
use crate::storage::util::join_url_segments;

const DEFAULT_BATCH_SIZE: usize = 8192;

/// An HFile read holds the whole file in memory, because the decoder is
/// constructed from a byte buffer. That is bounded here rather than left to
/// exhaust the heap: a key-seeking reader, which reads a block at a time, is a
/// separate piece of work, and until it exists a base file above this size is
/// refused instead of being loaded. Only the metadata table's `files` partition
/// is read by full scan today, and its base files are orders of magnitude
/// smaller than this bound.
const MAX_BUFFERED_FILE_SIZE: u64 = 256 * 1024 * 1024;

/// The key and the raw record value, as an HFile stores them. The value stays
/// serialized: decoding it needs the payload's own schema, which the base-file
/// reader does not resolve.
const KEY_COLUMN: &str = "key";
const VALUE_COLUMN: &str = "value";

/// Reads HFile base files.
#[derive(Debug)]
pub struct HFileBaseFileReader {
    storage: Arc<Storage>,
}

impl HFileBaseFileReader {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(KEY_COLUMN, DataType::Utf8, false),
            Field::new(VALUE_COLUMN, DataType::Binary, true),
        ]))
    }

    /// The projected schema, or an error naming a column the format does not
    /// have. An empty projection is the row-count-only request shape.
    fn project(projection: Option<&[String]>) -> Result<SchemaRef> {
        let full = Self::schema();
        match projection {
            None => Ok(full),
            Some(names) => {
                let mut fields = Vec::with_capacity(names.len());
                for name in names {
                    let field = full.field_with_name(name).map_err(|_| {
                        StorageError::InvalidColumn(format!(
                            "HFile base files have no column {name}"
                        ))
                    })?;
                    fields.push(field.clone());
                }
                Ok(Arc::new(Schema::new(fields)))
            }
        }
    }

    async fn file_size(&self, relative_path: &str, known: Option<u64>) -> Result<u64> {
        if let Some(size) = known {
            return Ok(size);
        }
        let obj_url = join_url_segments(&self.storage.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        Ok(self.storage.object_store.head(&obj_path).await?.size)
    }

    async fn open_within_bound(
        &self,
        relative_path: &str,
        known_size: Option<u64>,
    ) -> Result<HFileReader> {
        let size = self.file_size(relative_path, known_size).await?;
        if size > MAX_BUFFERED_FILE_SIZE {
            return Err(StorageError::Creation(format!(
                "HFile base file {relative_path} is {size} bytes, above the {MAX_BUFFERED_FILE_SIZE}-byte \
                 limit for a buffered read; reading it needs a key-seeking HFile reader"
            )));
        }
        HFileReader::open(&self.storage, relative_path)
            .await
            .map_err(|e| {
                StorageError::Creation(format!("Failed to read HFile {relative_path}: {e:?}"))
            })
    }

    fn batch(schema: &SchemaRef, keys: Vec<String>, values: Vec<Vec<u8>>) -> Result<RecordBatch> {
        let row_count = keys.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            match field.name().as_str() {
                KEY_COLUMN => columns.push(Arc::new(StringArray::from(keys.clone()))),
                VALUE_COLUMN => columns.push(Arc::new(BinaryArray::from_iter_values(
                    values.iter().map(|v| v.as_slice()),
                ))),
                other => {
                    return Err(StorageError::InvalidColumn(format!(
                        "HFile base files have no column {other}"
                    )));
                }
            }
        }
        RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
        .map_err(StorageError::ArrowError)
    }
}

impl BaseFileReader for HFileBaseFileReader {
    fn read_stream<'a>(
        &'a self,
        relative_path: &'a str,
        options: BaseFileReadOptions,
    ) -> BoxFuture<'a, Result<BaseFileStream>> {
        Box::pin(async move {
            let schema = Self::project(options.projection.as_deref())?;
            let mut reader = self
                .open_within_bound(relative_path, options.known_file_size)
                .await?;

            // `Some(vec![])` is the row-count-only request shape: the entry
            // count comes from the trailer, so no record is decoded.
            if schema.fields().is_empty() {
                let row_count = usize::try_from(reader.num_entries()).unwrap_or(usize::MAX);
                let batch = RecordBatch::try_new_with_options(
                    schema.clone(),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(row_count)),
                )
                .map_err(StorageError::ArrowError)?;
                return Ok(BaseFileStream::new(
                    schema,
                    futures::stream::once(async move { Ok(batch) }).boxed(),
                ));
            }

            let records = reader.collect_records().map_err(|e| {
                StorageError::Creation(format!("Failed to read HFile {relative_path}: {e:?}"))
            })?;

            let batch_size = options.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);
            let mut batches = Vec::with_capacity(records.len() / batch_size + 1);
            for chunk in records.chunks(batch_size) {
                let mut keys = Vec::with_capacity(chunk.len());
                let mut values = Vec::with_capacity(chunk.len());
                for record in chunk {
                    let key = record.key_as_str().ok_or_else(|| {
                        StorageError::Creation(format!(
                            "HFile {relative_path} has a record key that is not valid UTF-8"
                        ))
                    })?;
                    keys.push(key.to_string());
                    values.push(record.value.clone());
                }
                batches.push(Self::batch(&schema, keys, values)?);
            }

            Ok(BaseFileStream::new(
                schema,
                futures::stream::iter(batches.into_iter().map(Ok)).boxed(),
            ))
        })
    }

    /// The record count comes from the trailer, but reaching it reads the whole
    /// file, so this is not a cheap metadata-only call.
    fn get_metadata_and_stats<'a>(
        &'a self,
        relative_path: &'a str,
        _table_schema: &'a Schema,
    ) -> BoxFuture<'a, Result<(FileMetadata, StatisticsContainer)>> {
        Box::pin(async move {
            let size = self.file_size(relative_path, None).await?;
            let reader = self.open_within_bound(relative_path, Some(size)).await?;
            let num_records = reader.num_entries();

            let name = std::path::Path::new(relative_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(relative_path)
                .to_string();

            let mut metadata = FileMetadata::new(name, size);
            metadata.num_records = i64::try_from(num_records).unwrap_or(i64::MAX);

            let stats = StatisticsContainer {
                granularity: StatsGranularity::File,
                num_rows: Some(metadata.num_records),
                columns: std::collections::HashMap::new(),
            };
            Ok((metadata, stats))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::Result;
    use crate::config::HudiConfigs;
    use crate::config::read::HudiReadConfig;
    use crate::config::table::HudiTableConfig;
    use crate::file_group::FileGroup;
    use crate::file_group::reader_v2::MAX_INSTANT_TIME;
    use crate::file_group::reader_v2::engine::HoodieFileGroupReader;
    use crate::file_group::reader_v2::input_split::InputSplit;
    use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
    use crate::file_group::reader_v2::resolver::resolve_reader_context;
    use crate::metadata::table::reader::MetadataTableFileGroupReader;
    use crate::storage::Storage;
    use arrow_array::cast::AsArray;
    use std::collections::{HashMap, HashSet};
    use std::fs::canonicalize;
    use std::path::PathBuf;
    use std::sync::Arc;
    use url::Url;

    /// The metadata table's `files` partition, base file only. This is the
    /// initial base file (all-zeros instant); the seven log files that sit
    /// beside it belong to the log-block merge path, not this one.
    const MDT_FILES_BASE_FILE: &str = "files/files-0000-0_0-955-2690_00000000000000000.hfile";
    const MDT_FILES_FILE_GROUP: &str = "files-0000-0";
    const MDT_FILES_PARTITION: &str = "files";

    /// The metadata table living inside a regular table fixture.
    fn metadata_table_uri() -> String {
        use hudi_test::QuickstartTripsTable;
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let mdt = PathBuf::from(table_path).join(".hoodie").join("metadata");
        Url::from_file_path(canonicalize(&mdt).unwrap())
            .unwrap()
            .as_ref()
            .to_string()
    }

    /// The metadata table's own properties, as a table-level read would resolve
    /// them from its `hoodie.properties`: HFILE base files, and a CUSTOM merge
    /// mode that a base-file-only slice never consults.
    fn mdt_configs() -> Arc<HudiConfigs> {
        Arc::new(HudiConfigs::new([
            (HudiTableConfig::BasePath.as_ref(), metadata_table_uri()),
            (
                HudiTableConfig::BaseFileFormat.as_ref(),
                "hfile".to_string(),
            ),
            (
                HudiReadConfig::EndTimestamp.as_ref(),
                MAX_INSTANT_TIME.to_string(),
            ),
        ]))
    }

    /// A base-file-only slice: the HFile base file, no log files.
    fn base_file_only_split() -> InputSplit {
        InputSplit::new(
            Some(MDT_FILES_BASE_FILE.to_string()),
            Some("00000000000000000".to_string()),
            vec![],
            MDT_FILES_PARTITION.to_string(),
        )
    }

    /// The oracle: what `MetadataTableFileGroupReader` returns for the same
    /// base-file-only slice.
    async fn reference_keys() -> Result<HashSet<String>> {
        let configs = mdt_configs();
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone())?;
        let reader = MetadataTableFileGroupReader::new(configs, storage);

        let mut fg = FileGroup::new(
            MDT_FILES_FILE_GROUP.to_string(),
            MDT_FILES_PARTITION.to_string(),
        );
        fg.add_base_file_from_name(
            MDT_FILES_BASE_FILE
                .strip_prefix("files/")
                .expect("base file name is prefixed by its partition"),
        )?;
        let slice = fg
            .get_file_slice_as_of(MAX_INSTANT_TIME)
            .expect("the file group has a slice")
            .clone();

        let records = reader.read_files_partition(&slice, &[]).await?;
        Ok(records.into_keys().collect())
    }

    /// Criterion 1: an HFile base file read through the v2 file group reader
    /// returns the same keys the metadata-table reader returns for the same
    /// base-file-only slice.
    #[tokio::test]
    async fn v2_reads_an_hfile_base_file_matching_the_metadata_table_reader() -> Result<()> {
        let expected = reference_keys().await?;
        assert!(
            !expected.is_empty(),
            "the oracle must return keys, or the comparison is vacuous"
        );

        let configs = mdt_configs();
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone())?;
        let context = resolve_reader_context(&configs, /* has_log_files */ false)?;

        let mut reader = HoodieFileGroupReader::new(
            Arc::new(context),
            storage,
            base_file_only_split(),
            ReaderParameters::default(),
            None,
            None,
        )?;
        let batch = reader.read().await?;

        let key_column = batch
            .column_by_name("key")
            .expect("the metadata table's record key field is `key`");
        let keys: HashSet<String> = key_column
            .as_string::<i32>()
            .iter()
            .flatten()
            .map(str::to_string)
            .collect();

        assert_eq!(
            keys, expected,
            "v2's key set must equal the metadata-table reader's for the same slice"
        );
        Ok(())
    }
}
