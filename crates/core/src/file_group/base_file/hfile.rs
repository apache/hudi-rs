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

use super::reader::{BaseFileReadOptions, BaseFileReader, BaseFileStream, KeyPredicate};
use crate::file_group::log_file::avro::{AvroBlockDecoder, RegisteredWriterSchema};
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
    ///
    /// The decoder and the registration come back rather than being dropped, because
    /// building a decoder is the dominant cost of reading a small HFile: `arrow_avro`
    /// re-parses the writer schema's JSON on every construction, which for the
    /// metadata table's eight-kilobyte record schema is more than reading the file.
    /// The decoder has decoded nothing yet, so the first window can decode through
    /// it; the registration is immutable and serves every later window, which then
    /// pays only to build.
    fn decoded_schema(
        reader: &HFileReader,
        relative_path: &str,
    ) -> Result<(SchemaRef, AvroBlockDecoder, RegisteredWriterSchema)> {
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
        let registered = RegisteredWriterSchema::new(&json)
            .map_err(|e| StorageError::Creation(format!("{e}")))?;
        let decoder =
            AvroBlockDecoder::try_new_with_registered(&registered, None, DECODE_BATCH_SIZE)
                .map_err(|e| StorageError::Creation(format!("{e}")))?;
        let schema = decoder.schema();
        Ok((schema, decoder, registered))
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
    /// The bound depends on what the read is going to do, because a scan and a seek
    /// want opposite things: see [`HFILE_WHOLE_READ_WITH_KEYS_MAX_SIZE`] for the
    /// measurement and for why Hudi's single threshold is not followed here.
    /// `known_file_size` lets the caller spare the size lookup when the listing
    /// already told it.
    ///
    /// [`HFILE_WHOLE_READ_WITH_KEYS_MAX_SIZE`]: crate::storage::reader::HFILE_WHOLE_READ_WITH_KEYS_MAX_SIZE
    async fn open(
        &self,
        relative_path: &str,
        options: &BaseFileReadOptions,
    ) -> Result<HFileReader> {
        let mut whole_below =
            crate::storage::reader::hfile_whole_read_max_size(&self.storage.hudi_configs)
                .map_err(|e| StorageError::Creation(format!("{e}")))?;
        if options.key_predicate.is_some() {
            whole_below =
                whole_below.min(crate::storage::reader::HFILE_WHOLE_READ_WITH_KEYS_MAX_SIZE);
        }
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
            //
            // Not when a key predicate is set: the trailer counts the whole file,
            // so answering from it would report every record as though it matched.
            // A caller asking how many of five keys a file holds must get five or
            // fewer, so that combination reads blocks like any other.
            if options.key_predicate.is_none()
                && options.projection.as_ref().is_some_and(|p| p.is_empty())
            {
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

            let (full_schema, decoder, registered) = Self::decoded_schema(&reader, relative_path)?;
            let schema = Self::project(&full_schema, options.projection.as_deref())?;
            let projection: Option<Vec<String>> =
                options.projection.as_ref().map(|names| names.to_vec());

            let budget = reader
                .window_budget()
                .unwrap_or(DEFAULT_WINDOW_BUDGET_FALLBACK);
            // Seek when the caller named keys, scan when it did not. Selection
            // over-includes, so `decode_window` filters below; this only changes
            // which blocks are fetched.
            let entries = match options.key_predicate.as_ref() {
                Some(predicate) => reader.blocks_for_predicate(predicate),
                None => reader.data_block_entries(),
            };
            // Windows still bound peak memory over whatever was selected.
            let windows = HFileReader::plan_windows(&entries, budget);
            let key_predicate = options.key_predicate.clone();

            let stream = futures::stream::unfold(
                (
                    reader,
                    windows.into_iter(),
                    full_schema,
                    projection,
                    key_predicate,
                    // The decoder the schema was resolved with, for the first window.
                    // Later windows build their own: a decoder is flushed at the end of
                    // a window and `arrow_avro` does not fully reset a union's state on
                    // flush (arrow-rs#10876), so carrying one across a window boundary
                    // would decode the next window against stale offsets.
                    Some(decoder),
                    registered,
                    false,
                ),
                |(
                    reader,
                    mut windows,
                    full_schema,
                    projection,
                    key_predicate,
                    decoder,
                    registered,
                    failed,
                )| async move {
                    // Sticky: once a window fails the read is not whole, so no
                    // later window is handed out as though it were.
                    if failed {
                        return None;
                    }
                    let window = windows.next()?;
                    let item = decode_window(
                        &reader,
                        &window,
                        &full_schema,
                        projection.as_deref(),
                        key_predicate.as_ref(),
                        decoder,
                        &registered,
                    )
                    .await;
                    let failed = item.is_err();
                    Some((
                        item,
                        (
                            reader,
                            windows,
                            full_schema,
                            projection,
                            key_predicate,
                            None,
                            registered,
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
    decoded_schema: &SchemaRef,
    projection: Option<&[String]>,
    key_predicate: Option<&KeyPredicate>,
    decoder: Option<AvroBlockDecoder>,
    registered: &RegisteredWriterSchema,
) -> Result<RecordBatch> {
    let mut records = reader
        .read_records_batched(window)
        .await
        .map_err(|e| StorageError::Creation(format!("Failed to read HFile data blocks: {e:?}")))?;

    // A block is the smallest thing that can be read, so a selected block holds
    // keys nobody asked for. Dropping them here rather than after decoding keeps
    // the entry-key fill below aligned with the rows it fills, since that fill is
    // positional over `records`.
    if let Some(predicate) = key_predicate {
        // Built once per window, not per record: a record-index lookup names
        // thousands of keys and this runs on every record the window holds.
        let matcher = predicate.matcher();
        records.retain(|record| match std::str::from_utf8(&record.key) {
            Ok(key) => matcher.admits(key),
            // A key that is not UTF-8 cannot match a predicate expressed as
            // strings. Keeping it would put a row through that the caller asked
            // not to see.
            Err(_) => false,
        });
    }

    // A later window builds its own decoder, since the previous one was flushed and
    // `arrow_avro` does not fully reset a union's state on flush (arrow-rs#10876).
    // It builds from the registration rather than the JSON, which is the schema-sized
    // half of the cost and is immutable.
    let mut decoder = match decoder {
        Some(decoder) => decoder,
        None => AvroBlockDecoder::try_new_with_registered(registered, None, DECODE_BATCH_SIZE)
            .map_err(|e| StorageError::Creation(format!("{e}")))?,
    };
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
    /// The `files` partition's base file *after* the compaction at
    /// 20251220210130942, which carries the whole listing: `__all_partitions__`
    /// and one record per partition. The pre-compaction base file above holds a
    /// single record, so it cannot show a predicate filtering anything.
    const MDT_FILES_COMPACTED_BASE_FILE: &str =
        "files/files-0000-0_23-1133-3302_20251220210130942.hfile";

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
            // The metadata table's own record-key config. Without it the key
            // resolves to `_hoodie_record_key`, which every MDT record leaves
            // empty, and a merge collapses all keys into one.
            (HudiTableConfig::RecordKeyFields.as_ref(), "key".to_string()),
            // The metadata table's real merge semantics: CUSTOM, deferred to the
            // metadata payload by the all-zeros strategy id.
            ("hoodie.record.merge.mode", "CUSTOM".to_string()),
            (
                "hoodie.record.merge.strategy.id",
                "00000000-0000-0000-0000-000000000000".to_string(),
            ),
            (
                "hoodie.compaction.payload.class",
                "org.apache.hudi.metadata.HoodieMetadataPayload".to_string(),
            ),
            (
                HudiTableConfig::PopulatesMetaFields.as_ref(),
                "false".to_string(),
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
    /// The `files` partition's pre-compaction slice: the initial base HFile plus
    /// every log file written before the compaction at 20251220210130942.
    const MDT_FILES_PRECOMPACT_BASE: &str = "files-0000-0_0-955-2690_00000000000000000.hfile";
    const MDT_FILES_PRECOMPACT_LOGS: &[&str] = &[
        ".files-0000-0_20251220210108078.log.1_10-999-2838",
        ".files-0000-0_20251220210123755.log.1_3-1032-2950",
        ".files-0000-0_20251220210125441.log.1_5-1057-3024",
        ".files-0000-0_20251220210127080.log.1_3-1082-3100",
        ".files-0000-0_20251220210128625.log.1_5-1107-3174",
        ".files-0000-0_20251220210129235.log.1_3-1118-3220",
        ".files-0000-0_20251220210130911.log.1_3-1149-3338",
    ];

    fn precompact_slice() -> crate::Result<crate::file_group::FileSlice> {
        let mut fg = FileGroup::new(
            MDT_FILES_FILE_GROUP.to_string(),
            MDT_FILES_PARTITION.to_string(),
        );
        fg.add_base_file_from_name(MDT_FILES_PRECOMPACT_BASE)?;
        fg.add_log_files_from_names(MDT_FILES_PRECOMPACT_LOGS.iter().copied())?;
        Ok(fg
            .get_file_slice_as_of(MAX_INSTANT_TIME)
            .expect("the file group has a slice")
            .clone())
    }

    /// Every `filesystemMetadata` entry the metadata-table reader's own fold
    /// produces, so the comparison is on file names and sizes rather than counts.
    fn entries_by_key(
        records: &HashMap<String, crate::metadata::table::records::FilesPartitionRecord>,
    ) -> std::collections::BTreeMap<String, Vec<(String, i64, bool)>> {
        records
            .iter()
            .map(|(key, record)| {
                let mut entries: Vec<(String, i64, bool)> = record
                    .files
                    .iter()
                    .map(|(name, info)| (name.clone(), info.size, info.is_deleted))
                    .collect();
                entries.sort();
                (key.clone(), entries)
            })
            .collect()
    }

    /// The same, read off v2's merged Arrow batch.
    fn entries_from_batch(
        batch: &arrow_array::RecordBatch,
    ) -> std::collections::BTreeMap<String, Vec<(String, i64, bool)>> {
        use arrow_array::Array;
        let keys = batch
            .column_by_name("key")
            .expect("the metadata record key column")
            .as_string::<i32>();
        let map = batch
            .column_by_name("filesystemMetadata")
            .expect("the files-partition map column")
            .as_map();
        (0..batch.num_rows())
            .map(|row| {
                let entries = map.value(row);
                let names = entries.column(0).as_string::<i32>();
                let values = entries.column(1).as_struct();
                let sizes = values
                    .column_by_name("size")
                    .expect("size")
                    .as_primitive::<arrow_array::types::Int64Type>();
                let deleted = values
                    .column_by_name("isDeleted")
                    .expect("isDeleted")
                    .as_boolean();
                let mut out: Vec<(String, i64, bool)> = (0..names.len())
                    .map(|i| (names.value(i).to_string(), sizes.value(i), deleted.value(i)))
                    .collect();
                out.sort();
                (keys.value(row).to_string(), out)
            })
            .collect()
    }

    /// A metadata-table `files` slice with log blocks, merged by v2 under the
    /// table's own CUSTOM merge mode, lists exactly what the metadata-table
    /// reader's fold lists.
    ///
    /// Under `COMMIT_TIME_ORDERING` the same read returns all four keys but one
    /// file entry each, because the newest log block's map replaces the base
    /// record's rather than folding into it: four entries where the fold has
    /// fourteen. That is the regression this pins.
    #[tokio::test]
    async fn v2_folds_a_metadata_files_slice_like_the_metadata_table_reader() -> crate::Result<()> {
        let configs = mdt_configs();
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone())?;

        let expected = entries_by_key(
            &MetadataTableFileGroupReader::new(configs.clone(), storage.clone())
                .read_files_partition(&precompact_slice()?, &[])
                .await?,
        );
        assert!(
            expected.values().map(Vec::len).sum::<usize>() > expected.len(),
            "the oracle must fold several entries per key, or the comparison is vacuous"
        );

        let base = format!("{MDT_FILES_PARTITION}/{MDT_FILES_PRECOMPACT_BASE}");
        let mut context =
            resolve_reader_context(&configs, /* has_log_files */ true, Some(&base))?;
        context.rebuild_record_context(MDT_FILES_PARTITION.to_string());
        assert_eq!(
            context.merge_mode, "CUSTOM",
            "the metadata table's own merge mode must reach the reader"
        );

        let mut reader = HoodieFileGroupReader::new(
            Arc::new(context),
            storage,
            InputSplit::new(
                Some(base.clone()),
                Some("00000000000000000".to_string()),
                MDT_FILES_PRECOMPACT_LOGS
                    .iter()
                    .map(|f| format!("{MDT_FILES_PARTITION}/{f}"))
                    .collect(),
                MDT_FILES_PARTITION.to_string(),
            ),
            ReaderParameters::default(),
            None,
            None,
        )?;
        let batch = reader.read().await?;

        assert_eq!(entries_from_batch(&batch), expected);
        Ok(())
    }

    /// The `partition_stats` file group's log-only slice: eight log files, no base
    /// file, written before the compaction at 20251220210130942.
    const MDT_PARTITION_STATS_LOGS: &[&str] = &[
        ".partition-stats-0000-0_00000000000000003.log.1_0-0-0",
        ".partition-stats-0000-0_20251220210108078.log.1_9-999-2837",
        ".partition-stats-0000-0_20251220210123755.log.1_2-1032-2949",
        ".partition-stats-0000-0_20251220210125441.log.1_4-1057-3023",
        ".partition-stats-0000-0_20251220210127080.log.1_2-1082-3099",
        ".partition-stats-0000-0_20251220210128625.log.1_4-1107-3173",
        ".partition-stats-0000-0_20251220210129235.log.1_2-1118-3219",
        ".partition-stats-0000-0_20251220210130911.log.1_2-1149-3337",
    ];
    const MDT_PARTITION_STATS_PARTITION: &str = "partition_stats";

    /// Read a `partition_stats` slice through v2, keyed by record key.
    async fn read_partition_stats(
        logs: &[String],
    ) -> crate::Result<Option<arrow_array::RecordBatch>> {
        let configs = mdt_configs();
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone())?;
        let mut context = resolve_reader_context(
            &configs, /* has_log_files */ true, /* base */ None,
        )?;
        context.rebuild_record_context(MDT_PARTITION_STATS_PARTITION.to_string());
        let mut reader = HoodieFileGroupReader::new(
            Arc::new(context),
            storage,
            InputSplit::new(
                None,
                Some("00000000000000003".to_string()),
                logs.to_vec(),
                MDT_PARTITION_STATS_PARTITION.to_string(),
            ),
            ReaderParameters::default(),
            None,
            None,
        )?;
        // The first log file carries no data blocks, so a single-file read of it
        // has no schema to build an output from. That is the input's shape, not a
        // failure of the merge.
        Ok(reader.read().await.ok())
    }

    /// One row of a partition-statistics record, rendered so a comparison reads
    /// as values rather than as Arrow internals.
    fn stats_row(batch: &arrow_array::RecordBatch, row: usize) -> (String, String, String, i64) {
        use arrow_array::Array;
        let stats = batch
            .column_by_name("ColumnStatsMetadata")
            .expect("the column-statistics column")
            .as_struct();
        let render = |name: &str| -> String {
            let union = stats
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::UnionArray>()
                .expect("a bound is a union of typed wrappers");
            let child = union.child(union.type_id(row));
            let offset = union.value_offset(row);
            match child.as_struct_opt() {
                Some(wrapper) => arrow_cast::display::array_value_to_string(
                    wrapper.column_by_name("value").unwrap(),
                    offset,
                )
                .unwrap(),
                None => "null".to_string(),
            }
        };
        (
            stats
                .column_by_name("columnName")
                .unwrap()
                .as_string::<i32>()
                .value(row)
                .to_string(),
            render("minValue"),
            render("maxValue"),
            stats
                .column_by_name("valueCount")
                .unwrap()
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(row),
        )
    }

    /// A `partition_stats` slice merges through v2's CUSTOM path and lands on the
    /// newest record for each key.
    ///
    /// Every record this table writes is tight-bound, so
    /// `mergeColumnStatsRecords`'s second short-circuit applies to all of them and
    /// the merged result must equal the newest log file's record for that key.
    /// The oracle is therefore derived from the inputs: read each log file alone,
    /// in commit order, and keep the last occurrence.
    ///
    /// This pins the short-circuit and that the fold survives real dense unions.
    /// It does **not** cover the bound widening or the counter sums; no fixture in
    /// this repo writes a non-tight-bound statistics record, so those are covered
    /// by unit tests on `fold_column_stats` instead.
    #[tokio::test]
    async fn v2_merges_a_partition_stats_slice_to_the_newest_record() -> crate::Result<()> {
        let logs: Vec<String> = MDT_PARTITION_STATS_LOGS
            .iter()
            .map(|f| format!("{MDT_PARTITION_STATS_PARTITION}/{f}"))
            .collect();

        // The oracle: last writer per key, across the log files in commit order.
        let mut expected: std::collections::BTreeMap<String, (String, String, String, i64)> =
            std::collections::BTreeMap::new();
        let mut records_seen = 0usize;
        for log in &logs {
            let Some(batch) = read_partition_stats(std::slice::from_ref(log)).await? else {
                continue;
            };
            let keys = batch.column_by_name("key").unwrap().as_string::<i32>();
            for row in 0..batch.num_rows() {
                records_seen += 1;
                expected.insert(keys.value(row).to_string(), stats_row(&batch, row));
            }
        }
        assert!(
            records_seen > expected.len(),
            "the slice must contain repeated keys, or nothing merges and this test \
             proves nothing: {records_seen} records over {} keys",
            expected.len()
        );

        let merged = read_partition_stats(&logs)
            .await?
            .expect("the full slice must read");
        let keys = merged.column_by_name("key").unwrap().as_string::<i32>();
        let actual: std::collections::BTreeMap<String, (String, String, String, i64)> = (0..merged
            .num_rows())
            .map(|row| (keys.value(row).to_string(), stats_row(&merged, row)))
            .collect();

        assert_eq!(actual, expected);
        Ok(())
    }

    /// The `secondary_index` file group's earliest slice: a base HFile written at
    /// instant 4, and one log file written later that deletes its record.
    const MDT_SECONDARY_INDEX_PARTITION: &str = "secondary_index_rider_idx";
    const MDT_SECONDARY_INDEX_BASE: &str =
        "secondary-index-rider-idx-0000-0_0-1008-2875_00000000000000004.hfile";
    const MDT_SECONDARY_INDEX_LOG: &str =
        ".secondary-index-rider-idx-0000-0_20251220210128625.log.1_0-1107-3169";

    /// A delete in a metadata log block cancels the base record, under the
    /// metadata table's own CUSTOM merge mode.
    ///
    /// This is the only end-to-end coverage of the custom merger's delete path:
    /// `delta_merge_delete` returns the tombstone whatever the partition type,
    /// mirroring `preCombine`'s short-circuit on `isDeletedRecord`, and the base
    /// record must not survive it.
    ///
    /// It covers a third partition type reaching the merger (secondary index,
    /// type 7). It does **not** cover that type's data-vs-data rule: no fixture
    /// here holds the same index key as live data in both a base file and a log,
    /// so "the newer record wins" is covered by a unit test and by nothing else.
    #[tokio::test]
    async fn a_metadata_log_delete_cancels_the_base_record() -> crate::Result<()> {
        let base = format!("{MDT_SECONDARY_INDEX_PARTITION}/{MDT_SECONDARY_INDEX_BASE}");
        let log = format!("{MDT_SECONDARY_INDEX_PARTITION}/{MDT_SECONDARY_INDEX_LOG}");

        async fn read(
            base: String,
            logs: Vec<String>,
        ) -> crate::Result<(arrow_array::RecordBatch, u64)> {
            let configs = mdt_configs();
            let storage = Storage::new(Arc::new(HashMap::new()), configs.clone())?;
            let mut context =
                resolve_reader_context(&configs, !logs.is_empty(), Some(base.as_str()))?;
            context.rebuild_record_context(MDT_SECONDARY_INDEX_PARTITION.to_string());
            let mut reader = HoodieFileGroupReader::new(
                Arc::new(context),
                storage,
                InputSplit::new(
                    Some(base),
                    Some("00000000000000004".to_string()),
                    logs,
                    MDT_SECONDARY_INDEX_PARTITION.to_string(),
                ),
                ReaderParameters::default(),
                None,
                None,
            )?;
            let batch = reader.read().await?;
            let deletes = reader.read_stats().num_deletes;
            Ok((batch, deletes))
        }

        let (base_only, _) = read(base.clone(), vec![]).await?;
        assert_eq!(
            base_only.num_rows(),
            1,
            "the base file must hold the record the log then deletes, or this test \
             would pass on an empty base"
        );

        let (merged, deletes) = read(base, vec![log]).await?;
        assert_eq!(deletes, 1, "the log block must contribute one delete");
        assert_eq!(
            merged.num_rows(),
            0,
            "a delete must cancel the base record rather than leaving it readable"
        );
        Ok(())
    }

    /// A key predicate given to the reader narrows what is read and returns only
    /// the keys asked for.
    ///
    /// Driven through `read_data`, the trait method callers use, rather than
    /// through `blocks_for_keys` directly — the selection has its own tests in
    /// `hfile::reader`; this one is about the predicate surviving the trip through
    /// `BaseFileReadOptions` and being both applied and filtered by.
    #[tokio::test]
    async fn a_key_predicate_narrows_the_read_and_filters_the_rows() -> crate::Result<()> {
        let configs = mdt_configs();
        let storage = Storage::new(Arc::new(HashMap::new()), configs)?;
        let reader = HFileBaseFileReader::new(storage);

        let all = reader
            .read_data(
                MDT_FILES_COMPACTED_BASE_FILE,
                BaseFileReadOptions::default(),
            )
            .await?;
        let keys: Vec<String> = all
            .column_by_name("key")
            .expect("the metadata record key column")
            .as_string::<i32>()
            .iter()
            .flatten()
            .map(str::to_string)
            .collect();
        assert!(
            keys.len() > 1,
            "the fixture must hold several keys, or filtering cannot be observed"
        );

        let wanted = keys.last().unwrap().clone();
        let filtered = reader
            .read_data(
                MDT_FILES_COMPACTED_BASE_FILE,
                BaseFileReadOptions {
                    key_predicate: Some(KeyPredicate::Keys(vec![wanted.clone()])),
                    ..Default::default()
                },
            )
            .await?;

        let got: Vec<String> = filtered
            .column_by_name("key")
            .unwrap()
            .as_string::<i32>()
            .iter()
            .flatten()
            .map(str::to_string)
            .collect();
        assert_eq!(
            got,
            vec![wanted],
            "a key predicate must return exactly the keys it named, since a \
             selected block holds others"
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
        let mut context = resolve_reader_context(
            &configs,
            /* has_log_files */ false,
            Some(MDT_FILES_BASE_FILE),
        )?;
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

    /// A read split over several windows must equal a read that used one.
    ///
    /// The first window decodes through the decoder the schema was resolved with,
    /// and later windows build their own, because `arrow_avro` does not fully reset
    /// a union's state on flush (arrow-rs#10876) and a decoder is flushed at the end
    /// of every window. Reusing one across that boundary decodes the next window
    /// against stale offsets, which surfaces as wrong values rather than an error,
    /// so this pins the values and not only the row count.
    ///
    /// The fixture is a fifty-record HFile written with one-kilobyte blocks, because
    /// the tables in this repo hold their `files` partition in a single data block
    /// and a one-block file has only ever one window however small the budget.
    #[tokio::test]
    async fn a_read_split_over_windows_decodes_what_one_window_does() -> crate::Result<()> {
        async fn read(windowed: bool) -> crate::Result<(Vec<(String, i32)>, usize)> {
            let dir =
                std::fs::canonicalize(std::path::Path::new("tests/data/metadata_slices")).unwrap();
            let mut options = HashMap::from([
                (
                    HudiTableConfig::BasePath.as_ref().to_string(),
                    Url::from_directory_path(&dir).unwrap().to_string(),
                ),
                (
                    HudiTableConfig::BaseFileFormat.as_ref().to_string(),
                    "hfile".to_string(),
                ),
            ]);
            if windowed {
                // Ranged, because windows exist only when the file is not read whole,
                // and a one-byte budget plans a window per block without splitting one.
                options.insert(
                    crate::storage::reader::CONFIG_HFILE_WHOLE_READ_MAX_SIZE_MB.to_string(),
                    "0".to_string(),
                );
                options.insert(
                    crate::storage::reader::CONFIG_DFS_BUFFER_MAX_SIZE.to_string(),
                    "1".to_string(),
                );
            }
            let storage = Storage::new(
                Arc::new(HashMap::new()),
                Arc::new(HudiConfigs::new(options)),
            )?;
            let mut stream = HFileBaseFileReader::new(storage)
                .read_stream("files-multiblock.hfile", BaseFileReadOptions::default())
                .await?
                .into_stream();
            let mut rows = Vec::new();
            let mut batches = 0;
            while let Some(batch) = futures::StreamExt::next(&mut stream).await {
                let batch = batch?;
                batches += 1;
                let keys = batch
                    .column_by_name("key")
                    .expect("the metadata record key column")
                    .as_string::<i32>();
                let types = batch
                    .column_by_name("type")
                    .expect("the metadata record type column")
                    .as_primitive::<arrow_array::types::Int32Type>();
                for row in 0..batch.num_rows() {
                    rows.push((keys.value(row).to_string(), types.value(row)));
                }
            }
            rows.sort();
            Ok((rows, batches))
        }

        let (split, split_batches) = read(true).await?;
        let (whole, _) = read(false).await?;
        assert!(
            split_batches > 1,
            "this test is vacuous unless the budget splits the read: got \
             {split_batches} batch(es)"
        );
        assert_eq!(
            split, whole,
            "a windowed read must decode the same keys and types as a single-window read"
        );
        assert_eq!(whole.len(), 50, "the fixture holds fifty records");
        Ok(())
    }

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
        ) -> crate::Result<(Vec<String>, usize, usize)> {
            read_with(threshold_mb, known_file_size, None).await
        }

        async fn read_with(
            threshold_mb: &str,
            known_file_size: Option<u64>,
            key_predicate: Option<KeyPredicate>,
        ) -> crate::Result<(Vec<String>, usize, usize)> {
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
            if let Some(predicate) = key_predicate {
                options = options.with_key_predicate(predicate);
            }
            let batch = reader
                .read_data(MDT_FILES_COMPACTED_BASE_FILE, options)
                .await?;
            let keys: Vec<String> = batch
                .column_by_name("key")
                .expect("the metadata record key column")
                .as_string::<i32>()
                .iter()
                .flatten()
                .map(str::to_string)
                .collect();
            Ok((keys, counts.gets(), counts.heads()))
        }

        // The file's own size, so the below-threshold read can be asked for
        // without a size lookup.
        let file_size = std::fs::metadata(
            PathBuf::from(Url::parse(&metadata_table_uri()).unwrap().path())
                .join(MDT_FILES_COMPACTED_BASE_FILE),
        )
        .expect("the fixture base file")
        .len();

        // Below the default threshold, with the size already in hand: one read of
        // the whole file and nothing else, not even a metadata lookup.
        let (whole_keys, whole_gets, whole_heads) = read("50", Some(file_size)).await?;
        assert_eq!(
            (whole_gets, whole_heads),
            (1, 0),
            "a whole read of a known-size file is one request"
        );

        // Same threshold, size not supplied: one lookup to learn the size, then the
        // same single read.
        let (sized_keys, sized_gets, sized_heads) = read("50", None).await?;
        assert_eq!(
            (sized_gets, sized_heads),
            (1, 1),
            "learning the size costs one lookup and no extra read"
        );

        // Zero means never whole. The ranged open reads the trailer and the
        // load-on-open section before any data block, so it cannot come in at one.
        let (ranged_keys, ranged_gets, _) = read("0", Some(file_size)).await?;
        assert!(
            ranged_gets > whole_gets,
            "a ranged read issues more requests than a whole one, got {ranged_gets} \
             against {whole_gets}"
        );

        // A keyed read takes the same whole side here, because the fixture is far
        // below the keyed bound too. What is asserted is that naming keys does not
        // by itself force the ranged side.
        let (keyed_keys, keyed_gets, keyed_heads) = read_with(
            "50",
            Some(file_size),
            Some(KeyPredicate::Keys(vec![
                whole_keys.last().expect("the fixture returns keys").clone(),
            ])),
        )
        .await?;
        assert_eq!(
            (keyed_gets, keyed_heads),
            (1, 0),
            "a keyed read of a file below the keyed bound is still one request"
        );
        assert_eq!(keyed_keys.len(), 1, "the predicate must still filter");

        // Raising the file's apparent size past the keyed bound flips a keyed read
        // to ranged while a scan of the same file stays whole. This is the whole
        // point of the keyed bound, so it is asserted rather than assumed.
        let over_keyed_bound = crate::storage::reader::HFILE_WHOLE_READ_WITH_KEYS_MAX_SIZE + 1;
        let (_, keyed_big_gets, _) = read_with(
            "50",
            Some(over_keyed_bound),
            Some(KeyPredicate::Keys(vec![whole_keys.last().unwrap().clone()])),
        )
        .await?;
        let (_, scan_big_gets, _) = read("50", Some(over_keyed_bound)).await?;
        assert!(
            keyed_big_gets > 1,
            "past the keyed bound a keyed read must go ranged, got {keyed_big_gets} requests"
        );
        assert_eq!(
            scan_big_gets, 1,
            "a scan of the same size must stay whole; only the keyed bound moved"
        );

        assert!(
            !whole_keys.is_empty(),
            "the fixture must return rows, or the counts above prove nothing"
        );
        assert_eq!(whole_keys, sized_keys);
        assert_eq!(
            whole_keys, ranged_keys,
            "both strategies must return the same records; only their request \
             counts differ"
        );
        Ok(())
    }

    /// A prefix predicate returns every key carrying the prefix and nothing else.
    #[tokio::test]
    async fn a_prefix_predicate_returns_the_matching_keys() -> crate::Result<()> {
        let configs = mdt_configs();
        let storage = Storage::new(Arc::new(HashMap::new()), configs)?;
        let reader = HFileBaseFileReader::new(storage);

        let all = reader
            .read_data(
                MDT_FILES_COMPACTED_BASE_FILE,
                BaseFileReadOptions::default(),
            )
            .await?;
        let keys: Vec<String> = all
            .column_by_name("key")
            .unwrap()
            .as_string::<i32>()
            .iter()
            .flatten()
            .map(str::to_string)
            .collect();

        // The metadata table's partition keys share a "city=" head, so that
        // prefix matches a strict subset: the partitions but not
        // `__all_partitions__`.
        let expected: Vec<String> = keys
            .iter()
            .filter(|k| k.starts_with("city="))
            .cloned()
            .collect();
        assert!(
            !expected.is_empty() && expected.len() < keys.len(),
            "the prefix must match a strict subset, got {} of {}",
            expected.len(),
            keys.len()
        );

        let filtered = reader
            .read_data(
                MDT_FILES_COMPACTED_BASE_FILE,
                BaseFileReadOptions {
                    key_predicate: Some(KeyPredicate::Prefixes(vec!["city=".to_string()])),
                    ..Default::default()
                },
            )
            .await?;
        let got: Vec<String> = filtered
            .column_by_name("key")
            .unwrap()
            .as_string::<i32>()
            .iter()
            .flatten()
            .map(str::to_string)
            .collect();
        assert_eq!(got, expected);
        Ok(())
    }

    /// A count-only projection with a key predicate counts the matching records,
    /// not the file's.
    ///
    /// The row count comes from the trailer, which counts everything, so the fast
    /// path had to learn to stand aside. A caller asking how many of one key a
    /// file holds must not be told how many records the file holds.
    #[tokio::test]
    async fn a_count_only_projection_respects_the_predicate() -> crate::Result<()> {
        let configs = mdt_configs();
        let storage = Storage::new(Arc::new(HashMap::new()), configs)?;
        let reader = HFileBaseFileReader::new(storage);

        let count_all = reader
            .read_data(
                MDT_FILES_COMPACTED_BASE_FILE,
                BaseFileReadOptions {
                    projection: Some(vec![]),
                    ..Default::default()
                },
            )
            .await?
            .num_rows();
        assert!(count_all > 1, "the fixture must hold several records");

        let all = reader
            .read_data(
                MDT_FILES_COMPACTED_BASE_FILE,
                BaseFileReadOptions::default(),
            )
            .await?;
        let one = all
            .column_by_name("key")
            .unwrap()
            .as_string::<i32>()
            .value(0)
            .to_string();

        let count_one = reader
            .read_data(
                MDT_FILES_COMPACTED_BASE_FILE,
                BaseFileReadOptions {
                    projection: Some(vec![]),
                    key_predicate: Some(KeyPredicate::Keys(vec![one.clone()])),
                    ..Default::default()
                },
            )
            .await?
            .num_rows();
        assert_eq!(
            count_one, 1,
            "a count with a one-key predicate must be 1, not the file's {count_all}"
        );
        Ok(())
    }

    /// A key predicate set on the reader context reaches the base file reader
    /// through the engine.
    ///
    /// The other predicate tests call `read_data` directly, which is the trait
    /// method but not the path a caller takes. That left the plumbing from
    /// `ReaderContext` to `BaseFileReadOptions` untested, and it was in fact
    /// missing: nothing outside tests could set the predicate at all, so the
    /// criterion "a predicate reaches the reader through the reader context" was
    /// unmet while every predicate test passed. This is the test that fails when
    /// that route is broken.
    #[tokio::test]
    async fn a_key_predicate_on_the_reader_context_reaches_the_reader() -> crate::Result<()> {
        let configs = mdt_configs();
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone())?;

        async fn read(
            configs: Arc<HudiConfigs>,
            storage: Arc<Storage>,
            predicate: Option<KeyPredicate>,
        ) -> crate::Result<Vec<String>> {
            {
                let mut context = resolve_reader_context(
                    &configs,
                    /* has_log_files */ false,
                    Some(MDT_FILES_COMPACTED_BASE_FILE),
                )?;
                context.rebuild_record_context(MDT_FILES_PARTITION.to_string());
                context.key_predicate = predicate;
                let mut reader = HoodieFileGroupReader::new(
                    Arc::new(context),
                    storage,
                    InputSplit::new(
                        Some(MDT_FILES_COMPACTED_BASE_FILE.to_string()),
                        Some("20251220210130942".to_string()),
                        vec![],
                        MDT_FILES_PARTITION.to_string(),
                    ),
                    ReaderParameters::default(),
                    None,
                    None,
                )?;
                let batch = reader.read().await?;
                Ok(batch
                    .column_by_name("key")
                    .expect("the metadata record key column")
                    .as_string::<i32>()
                    .iter()
                    .flatten()
                    .map(str::to_string)
                    .collect::<Vec<String>>())
            }
        }

        let all = read(configs.clone(), storage.clone(), None).await?;
        assert!(
            all.len() > 1,
            "the fixture must hold several keys, or filtering cannot be observed"
        );

        let wanted = all.last().unwrap().clone();
        let filtered = read(
            configs.clone(),
            storage.clone(),
            Some(KeyPredicate::Keys(vec![wanted.clone()])),
        )
        .await?;
        assert_eq!(
            filtered,
            vec![wanted],
            "a predicate set on the reader context must reach the base file reader; \
             getting every key back means the engine dropped it"
        );
        Ok(())
    }

    /// An HFile base file and an HFile log block on a table that is **not** the
    /// metadata table, read through the ordinary public path.
    ///
    /// The fixture overlaps base and log on two keys, so the three ways this can
    /// go wrong look different: dropping the log block returns four rows at
    /// fares 11-14, merging in the wrong direction returns 13 and 14 on the
    /// overlap, and losing the base returns four rows from `uuid0003`. Values are
    /// asserted rather than a row count for that reason.
    #[tokio::test]
    async fn an_hfile_slice_reads_on_a_table_that_is_not_the_metadata_table() -> crate::Result<()> {
        use crate::file_group::reader::FileGroupReader;
        use crate::table::ReadOptions;

        let table_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../test/data/hfile_base_file_table");
        let base_url = url::Url::from_file_path(std::fs::canonicalize(&table_path).unwrap())
            .unwrap()
            .to_string();
        let reader =
            FileGroupReader::new_with_options(&base_url, crate::config::util::empty_options())
                .await?;

        let batch = reader
            .read_file_slice_from_paths(
                "f0000000-0000-0000-0000-000000000001-0_0-1-1_20250101000000000.hfile",
                vec![".f0000000-0000-0000-0000-000000000001-0_20250101000000000.log.1_0-2-2"],
                &ReadOptions::new(),
            )
            .await?;

        let keys = batch
            .column_by_name("uuid")
            .expect("uuid column")
            .as_string::<i32>();
        let fares = batch
            .column_by_name("fare")
            .expect("fare column")
            .as_primitive::<arrow_array::types::Float64Type>();
        let mut got: Vec<(String, i64)> = (0..batch.num_rows())
            .map(|i| (keys.value(i).to_string(), fares.value(i) as i64))
            .collect();
        got.sort();

        assert_eq!(
            got,
            vec![
                ("uuid0001".to_string(), 11),
                ("uuid0002".to_string(), 12),
                ("uuid0003".to_string(), 102),
                ("uuid0004".to_string(), 103),
                ("uuid0005".to_string(), 104),
                ("uuid0006".to_string(), 105),
            ],
            "the log block must be merged over the base file: the two overlapping \
             keys take the log's fares, and all six keys survive"
        );
        Ok(())
    }
}
