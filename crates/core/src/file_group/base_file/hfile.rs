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

use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, RecordBatch, RecordBatchOptions, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::StreamExt;
use futures::future::BoxFuture;

use super::reader::{BaseFileReadOptions, BaseFileReader, BaseFileStream};
use crate::hfile::{HFileReader, HFileRecord};
use crate::statistics::{StatisticsContainer, StatsGranularity};
use crate::storage::Storage;
use crate::storage::error::{Result, StorageError};
use crate::storage::file_metadata::FileMetadata;

/// An HFile stores a key and a value that is still serialized. Decoding the
/// value needs the payload's own schema, which a base-file reader does not
/// resolve, so it is handed on as bytes.
const KEY_COLUMN: &str = "key";
const VALUE_COLUMN: &str = "value";

/// Only reached if a reader reports no budget, which a ranged reader always does.
const DEFAULT_WINDOW_BUDGET_FALLBACK: u64 = 16 * 1024 * 1024;

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
        let Some(names) = projection else {
            return Ok(full);
        };
        let mut fields = Vec::with_capacity(names.len());
        for name in names {
            let field = full.field_with_name(name).map_err(|_| {
                StorageError::InvalidColumn(format!("HFile base files have no column {name}"))
            })?;
            fields.push(field.clone());
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    fn batch(schema: &SchemaRef, records: Vec<HFileRecord>) -> Result<RecordBatch> {
        let row_count = records.len();
        let mut keys: Option<Vec<String>> = None;
        let mut values: Option<Vec<Vec<u8>>> = None;

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            match field.name().as_str() {
                KEY_COLUMN => {
                    if keys.is_none() {
                        let mut decoded = Vec::with_capacity(records.len());
                        for record in &records {
                            let key = record.key_as_str().ok_or_else(|| {
                                StorageError::Creation(
                                    "an HFile record key is not valid UTF-8".to_string(),
                                )
                            })?;
                            decoded.push(key.to_string());
                        }
                        keys = Some(decoded);
                    }
                    let keys = keys.as_ref().expect("just populated");
                    columns.push(Arc::new(StringArray::from(keys.clone())));
                }
                VALUE_COLUMN => {
                    let values = values
                        .get_or_insert_with(|| records.iter().map(|r| r.value.clone()).collect());
                    columns.push(Arc::new(BinaryArray::from_iter_values(
                        values.iter().map(|v| v.as_slice()),
                    )));
                }
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

    async fn open(&self, relative_path: &str) -> Result<HFileReader> {
        HFileReader::open_ranged(&self.storage, relative_path)
            .await
            .map_err(|e| {
                StorageError::Creation(format!("Failed to read HFile {relative_path}: {e:?}"))
            })
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
            let reader = self.open(relative_path).await?;

            // `Some(vec![])` asks for the row count only. The trailer already
            // carries it, so no data block is read.
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

            let budget = reader
                .window_budget()
                .unwrap_or(DEFAULT_WINDOW_BUDGET_FALLBACK);
            let windows = HFileReader::plan_windows(&reader.data_block_entries(), budget);

            let stream = futures::stream::unfold(
                (reader, windows.into_iter(), schema.clone(), false),
                |(reader, mut windows, schema, failed)| async move {
                    // Sticky: once a window fails the read is not whole, so no
                    // later window is handed out as though it were.
                    if failed {
                        return None;
                    }
                    let window = windows.next()?;
                    let item = match reader.read_records_batched(&window).await {
                        Ok(records) => Self::batch(&schema, records),
                        Err(e) => Err(StorageError::Creation(format!(
                            "Failed to read HFile data blocks: {e:?}"
                        ))),
                    };
                    let failed = item.is_err();
                    Some((item, (reader, windows, schema, failed)))
                },
            )
            .boxed();

            Ok(BaseFileStream::new(schema, stream))
        })
    }

    fn get_metadata_and_stats<'a>(
        &'a self,
        relative_path: &'a str,
        _table_schema: &'a Schema,
    ) -> BoxFuture<'a, Result<(FileMetadata, StatisticsContainer)>> {
        Box::pin(async move {
            // `open` already learned the length, so no second head request.
            let reader = self.open(relative_path).await?;
            let size = reader.file_len();

            let name = std::path::Path::new(relative_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(relative_path)
                .to_string();

            let mut metadata = FileMetadata::new(name, size);
            metadata.num_records = i64::try_from(reader.num_entries()).unwrap_or(i64::MAX);

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
    use super::*;
    use crate::config::HudiConfigs;
    use crate::config::read::HudiReadConfig;
    use crate::config::table::HudiTableConfig;
    use crate::file_group::FileGroup;
    use crate::file_group::reader_v2::MAX_INSTANT_TIME;
    use crate::file_group::reader_v2::engine::HoodieFileGroupReader;
    use crate::file_group::reader_v2::input_split::InputSplit;
    use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
    use crate::file_group::reader_v2::resolver::resolve_reader_context;
    use crate::hfile::{BlockIndexEntry, Key};
    use crate::metadata::table::reader::MetadataTableFileGroupReader;
    use arrow_array::cast::AsArray;
    use std::collections::{HashMap, HashSet};
    use std::fs::canonicalize;
    use std::path::PathBuf;
    use url::Url;

    /// The metadata table's `files` partition, base file only. The seven log
    /// files beside it belong to the log-block merge path, not this one.
    const MDT_FILES_BASE_FILE: &str = "files/files-0000-0_0-955-2690_00000000000000000.hfile";
    const MDT_FILES_FILE_GROUP: &str = "files-0000-0";
    const MDT_FILES_PARTITION: &str = "files";

    fn metadata_table_uri() -> String {
        use hudi_test::QuickstartTripsTable;
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let mdt = PathBuf::from(table_path).join(".hoodie").join("metadata");
        Url::from_file_path(canonicalize(&mdt).unwrap())
            .unwrap()
            .as_ref()
            .to_string()
    }

    /// The metadata table's own properties: HFILE base files, and a CUSTOM merge
    /// mode that a slice with no log files never consults.
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

    /// What `MetadataTableFileGroupReader` returns for the same base-file-only
    /// slice: the oracle this read is checked against.
    async fn reference_keys() -> crate::Result<HashSet<String>> {
        let configs = mdt_configs();
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone())?;
        let reader = MetadataTableFileGroupReader::new(configs, storage);

        let mut fg = FileGroup::new(
            MDT_FILES_FILE_GROUP.to_string(),
            MDT_FILES_PARTITION.to_string(),
        );
        fg.add_base_file_from_name(MDT_FILES_BASE_FILE.strip_prefix("files/").unwrap())?;
        let slice = fg
            .get_file_slice_as_of(MAX_INSTANT_TIME)
            .expect("the file group has a slice")
            .clone();

        let records = reader.read_files_partition(&slice, &[]).await?;
        Ok(records.into_keys().collect())
    }

    /// An HFile base file read through the v2 file group reader returns the same
    /// keys the metadata-table reader returns for the same base-file-only slice.
    #[tokio::test]
    async fn v2_reads_an_hfile_base_file_matching_the_metadata_table_reader() -> crate::Result<()> {
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
            InputSplit::new(
                Some(MDT_FILES_BASE_FILE.to_string()),
                Some("00000000000000000".to_string()),
                vec![],
                MDT_FILES_PARTITION.to_string(),
            ),
            ReaderParameters::default(),
            None,
            None,
        )?;
        let batch = reader.read().await?;

        let keys: HashSet<String> = batch
            .column_by_name(KEY_COLUMN)
            .expect("the reader emits a key column")
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

    fn entry(offset: u64, size: u32) -> BlockIndexEntry {
        BlockIndexEntry::new(Key::from_bytes(vec![0]), None, offset, size)
    }

    #[test]
    fn windows_stop_at_the_budget_and_never_split_a_block() {
        let entries = vec![entry(0, 40), entry(40, 40), entry(80, 40)];

        let windows = HFileReader::plan_windows(&entries, 100);
        assert_eq!(
            windows.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![2, 1],
            "40 + 40 fits under 100, the third block starts a new window"
        );

        let windows = HFileReader::plan_windows(&entries, 1_000);
        assert_eq!(
            windows.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![3],
            "a budget above the total is one window"
        );

        // A block bigger than the budget still goes out whole: it cannot decode
        // in pieces.
        let windows = HFileReader::plan_windows(&[entry(0, 500)], 100);
        assert_eq!(windows, vec![vec![entry(0, 500)]]);

        assert!(HFileReader::plan_windows(&[], 100).is_empty());
    }
}
