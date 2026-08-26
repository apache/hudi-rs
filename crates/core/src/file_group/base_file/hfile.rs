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

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{Schema, SchemaRef};
use futures::StreamExt;
use futures::future::BoxFuture;

use super::reader::{BaseFileReadOptions, BaseFileReader, BaseFileStream};
use crate::file_group::log_file::avro::AvroBlockDecoder;
use crate::hfile::HFileReader;
use crate::hfile::record_key::fill_empty_entry_keys;
use crate::statistics::{StatisticsContainer, StatsGranularity};
use crate::storage::Storage;
use crate::storage::error::{Result, StorageError};
use crate::storage::file_metadata::FileMetadata;
use crate::util::arrow::project_batch_by_names;

/// Records per Arrow batch while decoding an HFile's values.
const DECODE_BATCH_SIZE: usize = 1024;

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

    /// The record schema an HFile was written with, as Avro JSON and as Arrow.
    ///
    /// An HFile stores each value Avro-encoded and carries the schema it used in
    /// its own file info. Decoding against that is what makes a base file and a
    /// log block of the same table yield the same columns, which is the whole
    /// reason they can merge; handing the value on as bytes does not.
    fn decoded_schema(reader: &HFileReader, relative_path: &str) -> Result<(String, SchemaRef)> {
        let json = reader
            .avro_schema_json()
            .map_err(|e| {
                StorageError::Creation(format!(
                    "Failed to read the Avro schema of HFile {relative_path}: {e:?}"
                ))
            })?
            .ok_or_else(|| {
                StorageError::Creation(format!(
                    "HFile {relative_path} carries no Avro schema, so its values cannot be decoded"
                ))
            })?
            .to_string();
        // The schema comes from the decoder, not from converting the Avro JSON:
        // `avro_to_arrow` does not handle named-type references, and the metadata
        // table's record schema uses them.
        let decoder = AvroBlockDecoder::try_new_with_reader(&json, None, DECODE_BATCH_SIZE)
            .map_err(|e| StorageError::Creation(format!("{e}")))?;
        Ok((json, decoder.schema()))
    }

    /// The projected schema, or an error naming a column the file does not have.
    /// An empty projection is the row-count-only request shape.
    fn project(full: &SchemaRef, projection: Option<&[String]>) -> Result<SchemaRef> {
        let Some(names) = projection else {
            return Ok(full.clone());
        };
        let mut fields = Vec::with_capacity(names.len());
        for name in names {
            let field = full.field_with_name(name).map_err(|_| {
                StorageError::InvalidColumn(format!("{name} is not a column of this HFile"))
            })?;
            fields.push(field.clone());
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    /// Open the file, reading it whole when it is small enough and in ranges when it
    /// is not.
    ///
    /// The threshold and the reason for it live on [`HFileReader::open_sized`].
    /// `known_file_size` lets the caller spare the size lookup when the listing
    /// already told it.
    async fn open(
        &self,
        relative_path: &str,
        options: &BaseFileReadOptions,
    ) -> Result<HFileReader> {
        let whole_below =
            crate::storage::reader::hfile_whole_read_max_size(&self.storage.hudi_configs)
                .map_err(|e| StorageError::Creation(format!("{e}")))?;
        HFileReader::open_sized(
            &self.storage,
            relative_path,
            whole_below,
            options.known_file_size,
        )
        .await
        .map_err(|e| StorageError::Creation(format!("Failed to read HFile {relative_path}: {e:?}")))
    }
}

impl BaseFileReader for HFileBaseFileReader {
    fn read_stream<'a>(
        &'a self,
        relative_path: &'a str,
        options: BaseFileReadOptions,
    ) -> BoxFuture<'a, Result<BaseFileStream>> {
        Box::pin(async move {
            let reader = self.open(relative_path, &options).await?;

            // `Some(vec![])` asks for the row count only. The trailer carries it,
            // so no data block is read and no schema is needed.
            if options.projection.as_ref().is_some_and(|p| p.is_empty()) {
                let schema: SchemaRef = Arc::new(Schema::empty());
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

            let (writer_json, full_schema) = Self::decoded_schema(&reader, relative_path)?;
            let schema = Self::project(&full_schema, options.projection.as_deref())?;
            let projection: Option<Vec<String>> =
                options.projection.as_ref().map(|names| names.to_vec());

            let budget = reader
                .window_budget()
                .unwrap_or(DEFAULT_WINDOW_BUDGET_FALLBACK);
            let windows = HFileReader::plan_windows(&reader.data_block_entries(), budget);

            let stream = futures::stream::unfold(
                (
                    reader,
                    windows.into_iter(),
                    writer_json,
                    full_schema,
                    projection,
                    false,
                ),
                |(reader, mut windows, writer_json, full_schema, projection, failed)| async move {
                    // Sticky: once a window fails the read is not whole, so no
                    // later window is handed out as though it were.
                    if failed {
                        return None;
                    }
                    let window = windows.next()?;
                    let item = decode_window(
                        &reader,
                        &window,
                        &writer_json,
                        &full_schema,
                        projection.as_deref(),
                    )
                    .await;
                    let failed = item.is_err();
                    Some((
                        item,
                        (
                            reader,
                            windows,
                            writer_json,
                            full_schema,
                            projection,
                            failed,
                        ),
                    ))
                },
            )
            .boxed();

            Ok(BaseFileStream::new(schema, stream))
        })
    }

    /// The record count comes from the trailer, and the ranged open already
    /// learned the file length, so neither costs an extra request.
    fn get_metadata_and_stats<'a>(
        &'a self,
        relative_path: &'a str,
        _table_schema: &'a Schema,
    ) -> BoxFuture<'a, Result<(FileMetadata, StatisticsContainer)>> {
        Box::pin(async move {
            // Ranged unconditionally here, whatever the threshold: this reports the
            // file's length and record count, both of which come from the trailer, so
            // buffering the file would be paid for nothing.
            let reader = HFileReader::open_ranged(&self.storage, relative_path)
                .await
                .map_err(|e| {
                    StorageError::Creation(format!("Failed to read HFile {relative_path}: {e:?}"))
                })?;
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

/// Decode one window's records into a single batch, projected if asked.
///
/// Each value is an Avro datum on its own, so it needs no de-framing; the
/// decoder batches them and is flushed once per window.
async fn decode_window(
    reader: &HFileReader,
    window: &[crate::hfile::BlockIndexEntry],
    writer_json: &str,
    decoded_schema: &SchemaRef,
    projection: Option<&[String]>,
) -> Result<RecordBatch> {
    let records = reader
        .read_records_batched(window)
        .await
        .map_err(|e| StorageError::Creation(format!("Failed to read HFile data blocks: {e:?}")))?;

    let mut decoder = AvroBlockDecoder::try_new_with_reader(writer_json, None, DECODE_BATCH_SIZE)
        .map_err(|e| StorageError::Creation(format!("{e}")))?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for record in &records {
        if let Some(batch) = decoder
            .decode(&record.value)
            .map_err(|e| StorageError::Creation(format!("{e}")))?
        {
            batches.push(batch);
        }
    }
    if let Some(batch) = decoder
        .flush()
        .map_err(|e| StorageError::Creation(format!("{e}")))?
        && batch.num_rows() > 0
    {
        batches.push(batch);
    }

    // The stream declares the decoded schema, so an empty window must carry that
    // schema too: a batch whose schema disagrees with the stream's breaks
    // projection and the merge downstream.
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| decoded_schema.clone());
    let combined =
        arrow::compute::concat_batches(&schema, &batches).map_err(StorageError::ArrowError)?;

    // A writer may leave the record's key field empty because the HFile entry key
    // already carries it. Positional, so the decode order above is load-bearing.
    let entry_keys: Vec<&str> = records
        .iter()
        .map(|r| {
            r.key_as_str().ok_or_else(|| {
                StorageError::Creation("an HFile record key is not valid UTF-8".to_string())
            })
        })
        .collect::<Result<Vec<&str>>>()?;
    let combined = fill_empty_entry_keys(combined, &entry_keys)
        .map_err(|e| StorageError::Creation(format!("{e}")))?;

    project_batch_by_names(combined, projection).map_err(|e| StorageError::Creation(format!("{e}")))
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
    /// The size threshold picks the read strategy, and the two strategies are told
    /// apart by the requests they issue.
    ///
    /// Rows cannot separate them: whole and ranged return the same records over the
    /// same file, which is the point. So the assertion is on request counts, taken
    /// from a store that wraps the real one and counts what passes through.
    #[tokio::test]
    async fn the_size_threshold_picks_whole_or_ranged() -> crate::Result<()> {
        use crate::storage::counting::CountingObjectStore;

        fn configs_with(threshold_mb: &str) -> Arc<HudiConfigs> {
            let mut options = mdt_configs().as_options();
            options.insert(
                crate::storage::reader::CONFIG_HFILE_WHOLE_READ_MAX_SIZE_MB.to_string(),
                threshold_mb.to_string(),
            );
            Arc::new(HudiConfigs::new(options))
        }

        async fn read(
            threshold_mb: &str,
            known_file_size: Option<u64>,
        ) -> crate::Result<(usize, usize, usize)> {
            let (store, counts) =
                CountingObjectStore::new(Arc::new(object_store::local::LocalFileSystem::new()));
            let storage = Storage::new_with_object_store(
                Url::parse(&metadata_table_uri()).unwrap(),
                store,
                configs_with(threshold_mb),
            );
            let reader = HFileBaseFileReader::new(storage);
            let mut options = BaseFileReadOptions::default();
            if let Some(size) = known_file_size {
                options = options.with_known_file_size(size);
            }
            let batch = reader.read_data(MDT_FILES_BASE_FILE, options).await?;
            Ok((batch.num_rows(), counts.gets(), counts.heads()))
        }

        // The file's own size, so the below-threshold read can be asked for without
        // a size lookup.
        let file_size = std::fs::metadata(
            PathBuf::from(Url::parse(&metadata_table_uri()).unwrap().path())
                .join(MDT_FILES_BASE_FILE),
        )
        .expect("the fixture base file")
        .len();

        // Below the default threshold, with the size already in hand: one read of
        // the whole file and nothing else, not even a metadata lookup.
        let (whole_rows, whole_gets, whole_heads) = read("50", Some(file_size)).await?;
        assert_eq!(
            (whole_gets, whole_heads),
            (1, 0),
            "a whole read of a known-size file is one request"
        );

        // Same threshold, size not supplied: one lookup to learn the size, then the
        // same single read.
        let (sized_rows, sized_gets, sized_heads) = read("50", None).await?;
        assert_eq!(
            (sized_gets, sized_heads),
            (1, 1),
            "learning the size costs one lookup and no extra read"
        );

        // Zero means never whole. The ranged open reads the trailer and the
        // load-on-open section before any data block, so it cannot come in at one.
        let (ranged_rows, ranged_gets, _) = read("0", Some(file_size)).await?;
        assert!(
            ranged_gets > whole_gets,
            "a ranged read issues more requests than a whole one, got {ranged_gets} \
             against {whole_gets}"
        );

        assert!(
            whole_rows > 0,
            "the fixture must return rows, or the counts above prove nothing"
        );
        assert_eq!(whole_rows, sized_rows);
        assert_eq!(
            whole_rows, ranged_rows,
            "both strategies must return the same records; only their request \
             counts differ"
        );
        Ok(())
    }

    #[tokio::test]
    async fn v2_reads_an_hfile_base_file_matching_the_metadata_table_reader() -> crate::Result<()> {
        let expected = reference_keys().await?;
        assert!(
            !expected.is_empty(),
            "the oracle must return keys, or the comparison is vacuous"
        );

        let configs = mdt_configs();
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone())?;
        let mut context = resolve_reader_context(&configs, /* has_log_files */ false)?;
        // The production path rebuilds this immediately (`adapter.rs`), and without
        // it the merge keys on `_hoodie_record_key` whatever the table says. A test
        // that skips it is not exercising the context the reader is really given.
        context.rebuild_record_context(MDT_FILES_PARTITION.to_string());

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
            .column_by_name("key")
            .expect("the metadata table's record key column, decoded from the HFile's values")
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
