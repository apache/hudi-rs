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
//! Inline compaction of the metadata table (Java
//! `HoodieBackedTableMetadataWriter.compactIfNecessary`).
//!
//! Runs inside a data write's completion critical section once the metadata
//! table holds `hoodie.metadata.compact.max.delta.commits` delta commits since
//! its latest compaction. Every file slice with log files gets a new base
//! HFile. A base record no log block touches is copied as its key and value
//! bytes, never decoded; a touched record is merged with Java's
//! `HoodieMetadataPayload` semantics, which for `record_index` is the newer
//! record's bytes as they are.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use apache_avro::Schema as AvroSchema;

use crate::Result;
use crate::error::CoreError;
use crate::file_group::log_file::scanner::{HFileRecordGroup, LogFileScanner};
use crate::hfile::{HFileBuilder, HFileReader, HFileRecord};
use crate::metadata::commit::{HoodieCommitMetadata, HoodieWriteStat};
use crate::metadata::compaction::{HoodieCompactionOperation, HoodieCompactionPlan};
use crate::metadata::payload_merger::MetadataPayloadMerger;
use crate::metadata::table::encode::{
    ColumnStatsMetadata, FilesMetadataEntry, StatsIndexVersion, decode_column_stats_entry,
    encode_column_stats, encode_files_record, encode_partition_stats, hoodie_metadata_schema_json,
    metadata_schema,
};
use crate::metadata::table::records::{
    FilesPartitionRecord, MetadataPartitionType, MetadataRecordType,
    decode_files_partition_record_with_schema,
};
use crate::storage::Storage;
use crate::table::Table;
use crate::timeline::instant::Action;
use crate::timeline::selector::InstantRange;

const METADATA_BASE: &str = ".hoodie/metadata";
const MDT_TIMELINE: &str = ".hoodie/metadata/.hoodie/timeline";
const MAX_DELTA_COMMITS: &str = "hoodie.metadata.compact.max.delta.commits";
const DEFAULT_MAX_DELTA_COMMITS: usize = 10;
/// Java's default `hoodie.hfile.block.size`.
const HFILE_BLOCK_SIZE: usize = 1024 * 1024;
/// Java `HoodieAvroHFileWriter` records a base file's key range in its file info.
const MIN_RECORD_KEY: &str = "minRecordKey";
const MAX_RECORD_KEY: &str = "maxRecordKey";

/// How a metadata partition combines successive records of one key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MergeKind {
    /// `combineFileSystemMetadata`.
    Files,
    /// `mergeColumnStatsRecords`; `partition_stats` encodes with its own type.
    Stats { partition_stats: bool },
    /// The newer record replaces the older one.
    NewerWins,
}

impl MergeKind {
    /// The partitions this writer knows the payload semantics of; others are
    /// left for the engine that created them.
    fn of(partition: &str) -> Option<Self> {
        if partition == MetadataPartitionType::Files.partition_name() {
            Some(Self::Files)
        } else if partition == MetadataPartitionType::ColumnStats.partition_name() {
            Some(Self::Stats {
                partition_stats: false,
            })
        } else if partition == MetadataPartitionType::PartitionStats.partition_name() {
            Some(Self::Stats {
                partition_stats: true,
            })
        } else if partition == MetadataPartitionType::RecordIndex.partition_name() {
            Some(Self::NewerWins)
        } else {
            None
        }
    }
}

/// One metadata file slice to compact.
struct SliceToCompact {
    partition: String,
    file_id: String,
    kind: MergeKind,
    /// Relative to the data table base, like the log paths.
    base_path: Option<String>,
    log_paths: Vec<String>,
}

impl SliceToCompact {
    fn base_instant(&self) -> Option<String> {
        let name = self.base_path.as_deref().map(file_name)?;
        name.rsplit_once('_')
            .map(|(_, rest)| rest.trim_end_matches(".hfile").to_string())
    }
}

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// The metadata timeline's instants by state.
#[derive(Default)]
struct MetadataTimeline {
    /// (instant, action) of completed instants.
    completed: Vec<(String, String)>,
    /// Instant to (action, fencing file names) of instants with no completed
    /// file.
    pending: BTreeMap<String, (String, Vec<String>)>,
}

async fn load_metadata_timeline(storage: &Storage) -> Result<MetadataTimeline> {
    let files = match storage.list_files(Some(MDT_TIMELINE)).await {
        Ok(files) => files,
        Err(crate::storage::error::StorageError::ObjectStoreError(
            object_store::Error::NotFound { .. },
        )) => return Ok(MetadataTimeline::default()),
        Err(error) => return Err(error.into()),
    };
    let mut timeline = MetadataTimeline::default();
    let mut fenced: BTreeMap<String, (String, Vec<String>)> = BTreeMap::new();
    let mut completed = HashSet::new();
    for file in files {
        let name = file.name;
        if !name.starts_with(|c: char| c.is_ascii_digit()) {
            continue;
        }
        let timestamp: String = name.chars().take_while(char::is_ascii_digit).collect();
        if name.ends_with(".requested") || name.ends_with(".inflight") {
            let action = crate::write::rollback::action_from_fencing_name(&name, &timestamp);
            let entry = fenced.entry(timestamp).or_default();
            if entry.0.is_empty() {
                entry.0 = action;
            }
            entry.1.push(name);
        } else if let Some((_, action)) = name.split_once('.') {
            completed.insert(timestamp.clone());
            timeline.completed.push((timestamp, action.to_string()));
        }
    }
    timeline.pending = fenced
        .into_iter()
        .filter(|(timestamp, _)| !completed.contains(timestamp))
        .collect();
    timeline.completed.sort();
    Ok(timeline)
}

/// Compact the metadata table when its delta commits since the latest
/// compaction reach the threshold. Called with the table lock held, after
/// the data instant completed; returns the compaction instant, if one ran.
///
/// Skips while any data instant is pending: its metadata log blocks would be
/// named below the compaction instant and so attach to the superseded slice
/// (Java holds compaction behind pending data instants the same way).
pub(crate) async fn compact_metadata_table_if_needed(table: &mut Table) -> Result<Option<String>> {
    if !table.is_metadata_table_enabled() {
        return Ok(None);
    }
    let max_delta_commits = table
        .hudi_configs
        .as_options()
        .get(MAX_DELTA_COMMITS)
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(DEFAULT_MAX_DELTA_COMMITS);
    let storage = table.file_system_view.storage.clone();
    let timeline = load_metadata_timeline(storage.as_ref()).await?;
    let mut blocked = false;
    for (instant, (action, fencing)) in &timeline.pending {
        if action == "compaction" {
            rollback_metadata_compaction(storage.as_ref(), instant, fencing).await?;
        } else {
            blocked = true;
        }
    }
    if blocked {
        return Ok(None);
    }
    let latest_compaction = timeline
        .completed
        .iter()
        .filter(|(_, action)| action == "commit")
        .map(|(instant, _)| instant.as_str())
        .max();
    let delta_commits = timeline
        .completed
        .iter()
        .filter(|(instant, action)| {
            action == "deltacommit"
                && latest_compaction.is_none_or(|latest| instant.as_str() > latest)
        })
        .count();
    if delta_commits < max_delta_commits {
        return Ok(None);
    }
    if !table.timeline.all_pending_instant_times().await?.is_empty() {
        return Ok(None);
    }

    let slices = slices_to_compact(storage.as_ref()).await?;
    if slices.is_empty() {
        return Ok(None);
    }
    table.timeline.reload_completed_commits().await?;
    let mdt = table.new_metadata_table().await?;
    let valid = table.valid_instant_timestamps(&mdt).await?;
    let range = InstantRange::exact_match(valid.into_iter(), "UTC");
    let version = crate::write::metadata::current_stats_index_version(table).await?;
    let instant = crate::write::append::generate_instant_time().await;
    let plan = compaction_plan(&slices);
    crate::write::fence_timeline_instant(
        storage.as_ref(),
        MDT_TIMELINE,
        &instant,
        Action::Compaction,
        plan.to_avro_bytes()?,
        Vec::new(),
    )
    .await?;
    match write_compacted_slices(table, &slices, &range, version, &instant).await {
        Ok(stats) => {
            complete_metadata_compaction(storage.as_ref(), &instant, stats).await?;
            Ok(Some(instant))
        }
        Err(error) => {
            let fencing = [
                format!("{instant}.compaction.inflight"),
                format!("{instant}.compaction.requested"),
            ];
            let _ = rollback_metadata_compaction(storage.as_ref(), &instant, &fencing).await;
            Err(error)
        }
    }
}

/// Delete what a compaction that never completed wrote: its base files, then
/// its fencing files. Its base files are not read before the commit
/// completes, and a slice's log files stay where they were.
async fn rollback_metadata_compaction(
    storage: &Storage,
    instant: &str,
    fencing: &[String],
) -> Result<()> {
    let suffix = format!("_{instant}.hfile");
    for partition in list_metadata_partitions(storage).await? {
        let dir = format!("{METADATA_BASE}/{partition}");
        for file in storage.list_files(Some(&dir)).await? {
            if file.name.ends_with(&suffix) {
                storage.delete_file(&format!("{dir}/{}", file.name)).await?;
            }
        }
    }
    for name in fencing
        .iter()
        .filter(|name| name.ends_with(".inflight"))
        .chain(fencing.iter().filter(|name| name.ends_with(".requested")))
    {
        match storage.delete_file(&format!("{MDT_TIMELINE}/{name}")).await {
            Ok(()) => {}
            Err(crate::storage::error::StorageError::ObjectStoreError(
                object_store::Error::NotFound { .. },
            )) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

async fn list_metadata_partitions(storage: &Storage) -> Result<Vec<String>> {
    match storage.list_dirs(Some(METADATA_BASE)).await {
        Ok(dirs) => Ok(dirs.into_iter().filter(|dir| dir != ".hoodie").collect()),
        Err(crate::storage::error::StorageError::ObjectStoreError(
            object_store::Error::NotFound { .. },
        )) => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

async fn slices_to_compact(storage: &Storage) -> Result<Vec<SliceToCompact>> {
    let mut partitions = list_metadata_partitions(storage).await?;
    partitions.sort();
    let mut slices = Vec::new();
    for partition in partitions {
        let Some(kind) = MergeKind::of(&partition) else {
            continue;
        };
        for (file_id, base_path, log_paths) in
            crate::write::metadata::mdt_partition_latest_slices(storage, &partition).await?
        {
            if log_paths.is_empty() {
                continue;
            }
            slices.push(SliceToCompact {
                partition: partition.clone(),
                file_id,
                kind,
                base_path,
                log_paths,
            });
        }
    }
    Ok(slices)
}

fn compaction_plan(slices: &[SliceToCompact]) -> HoodieCompactionPlan {
    let operations = slices
        .iter()
        .map(|slice| HoodieCompactionOperation {
            base_instant_time: slice.base_instant().or_else(|| {
                slice
                    .log_paths
                    .first()
                    .and_then(|path| file_name(path).split('_').nth(1))
                    .map(|instant| instant.split('.').next().unwrap_or(instant).to_string())
            }),
            delta_file_paths: Some(
                slice
                    .log_paths
                    .iter()
                    .map(|path| file_name(path).to_string())
                    .collect(),
            ),
            data_file_path: slice
                .base_path
                .as_deref()
                .map(|path| file_name(path).to_string()),
            file_id: Some(slice.file_id.clone()),
            partition_path: Some(slice.partition.clone()),
            metrics: None,
            bootstrap_file_path: None,
        })
        .collect();
    HoodieCompactionPlan {
        operations: Some(operations),
        extra_metadata: Some(HashMap::new()),
        version: Some(HoodieCompactionPlan::LATEST_VERSION),
        strategy: None,
        preserve_hoodie_metadata: Some(false),
        missing_schedule_partitions: None,
    }
}

/// Writes every slice's new base file on the write task pool and returns
/// their write stats. A failure deletes what was written.
async fn write_compacted_slices(
    table: &Table,
    slices: &[SliceToCompact],
    range: &InstantRange,
    version: StatsIndexVersion,
    instant: &str,
) -> Result<Vec<HoodieWriteStat>> {
    let storage = table.file_system_view.storage.clone();
    let tasks = slices
        .iter()
        .map(|slice| {
            let storage = storage.clone();
            let scanner = LogFileScanner::new(table.hudi_configs.clone(), storage.clone());
            let range = range.clone();
            let instant = instant.to_string();
            let (partition, file_id, kind) =
                (slice.partition.clone(), slice.file_id.clone(), slice.kind);
            let (base_path, log_paths) = (slice.base_path.clone(), slice.log_paths.clone());
            let prev_commit = slice.base_instant().unwrap_or_else(|| "null".to_string());
            let task: futures::future::BoxFuture<'static, Result<HoodieWriteStat>> =
                Box::pin(async move {
                    let base = match &base_path {
                        Some(path) => Some(storage.get_file_data(path).await?.to_vec()),
                        None => None,
                    };
                    let num_log_files = log_paths.len() as i64;
                    let groups = scanner.scan_hfile_records(log_paths, &range).await?;
                    let (bytes, num_records) = tokio::task::spawn_blocking(move || {
                        compact_slice(kind, base, groups, version)
                    })
                    .await
                    .map_err(|e| {
                        CoreError::Write(format!("metadata compaction task failed: {e}"))
                    })??;
                    let name = format!("{file_id}_0-0-0_{instant}.hfile");
                    let relative = format!("{partition}/{name}");
                    let size = bytes.len() as i64;
                    storage
                        .put_file(&format!("{METADATA_BASE}/{relative}"), bytes)
                        .await?;
                    Ok(HoodieWriteStat {
                        file_id: Some(file_id),
                        path: Some(relative),
                        base_file: Some(name),
                        prev_commit: Some(prev_commit),
                        num_writes: Some(num_records as i64),
                        total_write_bytes: Some(size),
                        file_size_in_bytes: Some(size),
                        partition_path: Some(partition),
                        total_log_files_compacted: Some(num_log_files),
                        ..Default::default()
                    })
                });
            task
        })
        .collect();
    let parallelism = std::thread::available_parallelism()
        .map_or(4, std::num::NonZeroUsize::get)
        .min(crate::write::write_task_parallelism(table));
    let results = crate::write::run_write_tasks(tasks, parallelism).await;
    let mut stats = Vec::with_capacity(results.len());
    let mut first_error = None;
    for result in results {
        match result {
            Ok(stat) => stats.push(stat),
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    match first_error {
        Some(error) => {
            for stat in &stats {
                if let Some(path) = &stat.path {
                    let _ = storage
                        .delete_file(&format!("{METADATA_BASE}/{path}"))
                        .await;
                }
            }
            Err(error)
        }
        None => Ok(stats),
    }
}

async fn complete_metadata_compaction(
    storage: &Storage,
    instant: &str,
    stats: Vec<HoodieWriteStat>,
) -> Result<()> {
    let metadata = HoodieCommitMetadata {
        version: Some(1),
        operation_type: Some("COMPACT".to_string()),
        partition_to_write_stats: Some(crate::write::stats_by_partition(stats)),
        compacted: Some(true),
        extra_metadata: Some(HashMap::from([(
            "schema".to_string(),
            hoodie_metadata_schema_json()?.to_string(),
        )])),
        partition_to_replace_file_ids: None,
    };
    let completion = crate::write::append::generate_instant_time().await;
    storage
        .put_file_if_absent(
            &format!("{MDT_TIMELINE}/{instant}_{completion}.commit"),
            metadata.to_avro_bytes()?,
        )
        .await?;
    Ok(())
}

/// A key's state after the records merged into it so far.
enum Merged {
    /// A delete block removed the key.
    Deleted,
    /// Record bytes as written, with the schema they were written with.
    Raw {
        value: Vec<u8>,
        schema: Option<Arc<AvroSchema>>,
    },
    Files(FilesPartitionRecord),
    Stats(ColumnStatsMetadata),
}

/// Whether bytes written with `schema` read as they are under the output
/// schema; cached by schema identity, since a slice's blocks share a few.
struct SchemaCheck {
    output_canonical: String,
    known: HashMap<*const AvroSchema, bool>,
}

impl SchemaCheck {
    fn matches(&mut self, schema: Option<&Arc<AvroSchema>>) -> bool {
        let Some(schema) = schema else {
            return true;
        };
        let output = &self.output_canonical;
        *self
            .known
            .entry(Arc::as_ptr(schema))
            .or_insert_with(|| schema.canonical_form() == *output)
    }
}

/// Bytes of `value`, written with `schema`, under the output schema: the
/// bytes themselves, which requires the schemas to agree.
///
/// Records from another schema are refused rather than resolved: the
/// metadata schema's stats unions hold several wrappers of one shape (date and
/// int, timestamp-micros and long), and generic resolution picks the first,
/// which would retype every such bound.
fn output_bytes(
    check: &mut SchemaCheck,
    value: Vec<u8>,
    schema: Option<&Arc<AvroSchema>>,
) -> Result<Vec<u8>> {
    if check.matches(schema) {
        Ok(value)
    } else {
        Err(schema_mismatch())
    }
}

fn schema_mismatch() -> CoreError {
    CoreError::Unsupported(
        "metadata table compaction requires records written with the metadata schema this \
         writer uses"
            .to_string(),
    )
}

fn decode_files(key: &str, value: &[u8], schema: &AvroSchema) -> Result<FilesPartitionRecord> {
    decode_files_partition_record_with_schema(
        &HFileRecord::from_str_key(key, value.to_vec()),
        schema,
    )
}

/// Merge `next` onto `prev` with `HoodieMetadataPayload.preCombine`: a delete
/// on either side resets the key, and otherwise the partition's combine rule
/// applies.
fn merge(
    kind: MergeKind,
    key: &str,
    prev: Merged,
    next: Merged,
    output: &AvroSchema,
) -> Result<Merged> {
    if matches!(prev, Merged::Deleted) || matches!(next, Merged::Deleted) {
        return Ok(next);
    }
    match kind {
        MergeKind::NewerWins => Ok(next),
        MergeKind::Files => {
            let mut prev = into_files(key, prev, output)?;
            let next = into_files(key, next, output)?;
            MetadataPayloadMerger.merge_file_infos(&mut prev.files, &next.files);
            Ok(Merged::Files(prev))
        }
        MergeKind::Stats { .. } => match (into_stats(&prev, output)?, into_stats(&next, output)?) {
            (Some(prev), Some(next)) => Ok(Merged::Stats(
                MetadataPayloadMerger.merge_column_stats(&prev, &next),
            )),
            _ => Ok(next),
        },
    }
}

fn into_files(key: &str, merged: Merged, output: &AvroSchema) -> Result<FilesPartitionRecord> {
    match merged {
        Merged::Files(record) => Ok(record),
        Merged::Raw { value, schema } => {
            decode_files(key, &value, schema.as_deref().unwrap_or(output))
        }
        Merged::Stats(_) | Merged::Deleted => Err(CoreError::MetadataTable(format!(
            "metadata files record '{key}' merged with a non-files record"
        ))),
    }
}

fn into_stats(merged: &Merged, output: &AvroSchema) -> Result<Option<ColumnStatsMetadata>> {
    match merged {
        Merged::Stats(stats) => Ok(Some(stats.clone())),
        Merged::Raw { value, schema } => {
            decode_column_stats_entry(value, Some(schema.as_deref().unwrap_or(output)))
        }
        Merged::Files(_) | Merged::Deleted => Ok(None),
    }
}

/// The bytes a merged key is written as, or `None` when it is deleted.
fn encode_merged(
    kind: MergeKind,
    key: &str,
    merged: Merged,
    check: &mut SchemaCheck,
    version: StatsIndexVersion,
) -> Result<Option<Vec<u8>>> {
    Ok(match merged {
        Merged::Deleted => None,
        Merged::Raw { value, schema } => Some(output_bytes(check, value, schema.as_ref())?),
        Merged::Files(record) => {
            let is_all_partitions = record.record_type == MetadataRecordType::AllPartitions;
            let entries = record.files.into_values().map(|info| FilesMetadataEntry {
                // The decoder spells a non-partitioned table's partition as
                // empty; on disk it is `.`.
                name: if is_all_partitions && info.name.is_empty() {
                    FilesPartitionRecord::NON_PARTITIONED_NAME.to_string()
                } else {
                    info.name
                },
                size: info.size,
                is_deleted: info.is_deleted,
            });
            Some(encode_files_record(key, record.record_type, entries)?)
        }
        Merged::Stats(stats) => Some(match kind {
            MergeKind::Stats {
                partition_stats: true,
            } => encode_partition_stats(&stats, version)?,
            _ => encode_column_stats(&stats, version)?,
        }),
    })
}

/// Build the compacted base HFile of one slice from its base file bytes and
/// its log blocks in log order. Returns the file and its record count.
fn compact_slice(
    kind: MergeKind,
    base: Option<Vec<u8>>,
    groups: Vec<HFileRecordGroup>,
    version: StatsIndexVersion,
) -> Result<(Vec<u8>, u64)> {
    let hfile_error = |e: crate::hfile::HFileError| CoreError::HFile(e.to_string());
    let output = metadata_schema()?;
    let mut check = SchemaCheck {
        output_canonical: output.canonical_form(),
        known: HashMap::new(),
    };
    let mut base = base
        .map(HFileReader::new)
        .transpose()
        .map_err(hfile_error)?;
    let base_schema: Option<Arc<AvroSchema>> = match &base {
        Some(reader) => reader
            .get_avro_schema()
            .map_err(hfile_error)?
            .cloned()
            .map(Arc::new),
        None => None,
    };
    if !check.matches(base_schema.as_ref()) {
        return Err(schema_mismatch());
    }

    // Keys the logs touch, merged in log order onto their base record where
    // the partition's combine rule reads it.
    let mut touched: HashMap<String, Merged> = HashMap::new();
    if kind != MergeKind::NewerWins
        && let Some(reader) = base.as_mut()
    {
        let mut keys: Vec<&str> = groups
            .iter()
            .flat_map(|group| group.records.iter().filter_map(HFileRecord::key_as_str))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        keys.sort_unstable();
        for (key, record) in reader.lookup_records(&keys).map_err(hfile_error)? {
            if let Some(record) = record {
                touched.insert(
                    key,
                    Merged::Raw {
                        value: record.value,
                        schema: base_schema.clone(),
                    },
                );
            }
        }
    }
    for group in groups {
        for record in group.records {
            let key = record
                .key_as_str()
                .ok_or_else(|| {
                    CoreError::MetadataTable("metadata record key is not UTF-8".to_string())
                })?
                .to_string();
            let next = if record.is_deleted() {
                Merged::Deleted
            } else {
                Merged::Raw {
                    value: record.value,
                    schema: group.schema.clone(),
                }
            };
            let merged = match touched.remove(&key) {
                Some(prev) => merge(kind, &key, prev, next, output)?,
                None => next,
            };
            touched.insert(key, merged);
        }
    }
    let mut touched: Vec<(String, Merged)> = touched.into_iter().collect();
    touched.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let mut touched = touched.into_iter().peekable();

    let mut out = CompactedFile::new();
    if let Some(reader) = base.as_mut() {
        for kv in reader.iter().map_err(hfile_error)? {
            let kv = kv.map_err(hfile_error)?;
            let key = kv.key().content();
            while let Some((next, merged)) = touched.next_if(|(next, _)| next.as_bytes() < key) {
                out.push_merged(kind, next, merged, &mut check, version)?;
            }
            if let Some((next, merged)) = touched.next_if(|(next, _)| next.as_bytes() == key) {
                out.push_merged(kind, next, merged, &mut check, version)?;
                continue;
            }
            out.push(key, kv.value())?;
        }
    }
    for (key, merged) in touched {
        out.push_merged(kind, key, merged, &mut check, version)?;
    }
    out.finish()
}

/// The compacted base file being built, with the key range Java records in
/// its file info.
struct CompactedFile {
    builder: HFileBuilder,
    min_key: Option<Vec<u8>>,
    max_key: Vec<u8>,
}

impl CompactedFile {
    fn new() -> Self {
        Self {
            builder: HFileBuilder::new(HFILE_BLOCK_SIZE),
            min_key: None,
            max_key: Vec::new(),
        }
    }

    fn push(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.builder
            .append(key, value)
            .map_err(|e| CoreError::HFile(e.to_string()))?;
        if self.min_key.is_none() {
            self.min_key = Some(key.to_vec());
        }
        self.max_key.clear();
        self.max_key.extend_from_slice(key);
        Ok(())
    }

    fn push_merged(
        &mut self,
        kind: MergeKind,
        key: String,
        merged: Merged,
        check: &mut SchemaCheck,
        version: StatsIndexVersion,
    ) -> Result<()> {
        match encode_merged(kind, &key, merged, check, version)? {
            Some(value) => self.push(key.as_bytes(), &value),
            None => Ok(()),
        }
    }

    /// The file's bytes and record count.
    fn finish(self) -> Result<(Vec<u8>, u64)> {
        let num_records = self.builder.entry_count();
        let mut file_info = BTreeMap::from([(
            "schema".to_string(),
            hoodie_metadata_schema_json()?.as_bytes().to_vec(),
        )]);
        if let Some(min_key) = self.min_key {
            file_info.insert(MIN_RECORD_KEY.to_string(), min_key);
            file_info.insert(MAX_RECORD_KEY.to_string(), self.max_key);
        }
        let bytes = self
            .builder
            .finish(file_info)
            .map_err(|e| CoreError::HFile(e.to_string()))?;
        Ok((bytes, num_records))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::TableTypeValue;
    use crate::metadata::table::encode::encode_all_partitions;
    use crate::metadata::table::records::{decode_avro_value, extract_filesystem_metadata};
    use crate::table::ReadOptions;
    use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::path::Path;

    fn schema_info() -> BTreeMap<String, Vec<u8>> {
        BTreeMap::from([(
            "schema".to_string(),
            hoodie_metadata_schema_json().unwrap().as_bytes().to_vec(),
        )])
    }

    fn hfile(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let entries: Vec<(String, Vec<u8>)> = entries
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_vec()))
            .collect();
        crate::hfile::HFileWriter::write(&entries, schema_info()).unwrap()
    }

    fn group(records: &[(&str, &[u8])]) -> HFileRecordGroup {
        HFileRecordGroup {
            schema: Some(Arc::new(metadata_schema().unwrap().clone())),
            records: records
                .iter()
                .map(|(key, value)| HFileRecord::from_str_key(key, value.to_vec()))
                .collect(),
        }
    }

    fn cells(bytes: Vec<u8>) -> Vec<(String, Vec<u8>)> {
        HFileReader::new(bytes)
            .unwrap()
            .collect_records()
            .unwrap()
            .into_iter()
            .map(|r| (r.key_as_str().unwrap().to_string(), r.value))
            .collect()
    }

    /// Base values no log block touches are copied without being decoded,
    /// which bytes that are not Avro at all prove; touched keys take the
    /// newest record's bytes and a delete drops the key.
    #[test]
    fn test_compact_slice_copies_untouched_base_bytes() {
        let not_avro: &[u8] = b"\xff\xfe not an avro record";
        let base = hfile(&[("a", not_avro), ("b", b"b-base"), ("c", b"c-base")]);
        let groups = vec![
            group(&[("b", b"b-1"), ("d", b"d-1")]),
            group(&[("b", b"b-2")]),
            group(&[("c", b"")]),
        ];
        let (bytes, count) = compact_slice(
            MergeKind::NewerWins,
            Some(base),
            groups,
            StatsIndexVersion::V2,
        )
        .unwrap();
        assert_eq!(count, 3);
        let reader = HFileReader::new(bytes.clone()).unwrap();
        assert_eq!(
            reader.read_min_max_record_keys(),
            Some(("a".into(), "d".into()))
        );
        assert_eq!(
            cells(bytes),
            vec![
                ("a".to_string(), not_avro.to_vec()),
                ("b".to_string(), b"b-2".to_vec()),
                ("d".to_string(), b"d-1".to_vec()),
            ]
        );
    }

    /// Records written with another schema are refused rather than resolved
    /// into the metadata schema, whether they sit in the base or in a log.
    #[test]
    fn test_compact_slice_refuses_another_schema() {
        let other = r#"{"type":"record","name":"HoodieMetadataRecord","fields":[{"name":"key","type":"string"}]}"#;
        let base = crate::hfile::HFileWriter::write(
            &[("k".to_string(), b"v".to_vec())],
            BTreeMap::from([("schema".to_string(), other.as_bytes().to_vec())]),
        )
        .unwrap();
        let error = compact_slice(
            MergeKind::NewerWins,
            Some(base),
            vec![group(&[("j", b"w")])],
            StatsIndexVersion::V2,
        )
        .unwrap_err();
        assert!(matches!(error, CoreError::Unsupported(_)), "{error}");
        let foreign = HFileRecordGroup {
            schema: Some(Arc::new(AvroSchema::parse_str(other).unwrap())),
            records: vec![HFileRecord::from_str_key("j", b"w".to_vec())],
        };
        let error = compact_slice(
            MergeKind::NewerWins,
            Some(hfile(&[("k", b"v")])),
            vec![foreign],
            StatsIndexVersion::V2,
        )
        .unwrap_err();
        assert!(matches!(error, CoreError::Unsupported(_)), "{error}");
    }

    /// A log-only slice and a slice whose every key is deleted both compact.
    #[test]
    fn test_compact_slice_without_base_and_to_empty() {
        let (bytes, count) = compact_slice(
            MergeKind::NewerWins,
            None,
            vec![group(&[("k", b"v")])],
            StatsIndexVersion::V2,
        )
        .unwrap();
        assert_eq!(
            (cells(bytes), count),
            (vec![("k".to_string(), b"v".to_vec())], 1)
        );
        let (bytes, count) = compact_slice(
            MergeKind::NewerWins,
            Some(hfile(&[("k", b"v")])),
            vec![group(&[("k", b"")])],
            StatsIndexVersion::V2,
        )
        .unwrap();
        assert_eq!((cells(bytes), count), (Vec::new(), 0));
    }

    /// Files records merge with the base record (a later tombstone cancels a
    /// file the base lists), and the non-partitioned `.` partition keeps its
    /// on-disk spelling through the decode and re-encode.
    #[test]
    fn test_compact_slice_merges_files_records() {
        let files = |entries: Vec<(&str, i64, bool)>| {
            encode_files_record(
                "",
                MetadataRecordType::Files,
                entries
                    .into_iter()
                    .map(|(name, size, is_deleted)| FilesMetadataEntry {
                        name: name.to_string(),
                        size,
                        is_deleted,
                    }),
            )
            .unwrap()
        };
        let all_partitions = encode_all_partitions([".".to_string()]).unwrap();
        let base = hfile(&[
            (".", &files(vec![("f1", 100, false), ("f2", 50, false)])),
            ("__all_partitions__", &all_partitions),
        ]);
        let groups = vec![
            group(&[(".", &files(vec![("f3", 300, false)]))]),
            group(&[
                (".", &files(vec![("f1", 0, true)])),
                ("__all_partitions__", &all_partitions),
            ]),
        ];
        let (bytes, _) =
            compact_slice(MergeKind::Files, Some(base), groups, StatsIndexVersion::V2).unwrap();
        let schema = metadata_schema().unwrap();
        let decoded: HashMap<String, Vec<(String, i64, bool)>> = cells(bytes)
            .into_iter()
            .map(|(key, value)| {
                let mut entries: Vec<_> =
                    extract_filesystem_metadata(&decode_avro_value(&value, schema).unwrap())
                        .into_values()
                        .map(|info| (info.name, info.size, info.is_deleted))
                        .collect();
                entries.sort();
                (key, entries)
            })
            .collect();
        assert_eq!(
            decoded["."],
            vec![
                ("f2".to_string(), 50, false),
                ("f3".to_string(), 300, false)
            ]
        );
        assert_eq!(
            decoded["__all_partitions__"],
            vec![(".".to_string(), 0, false)]
        );
    }

    fn batch(rows: &[(&str, &str, i64, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    /// A table handle whose writes compact the metadata table at `threshold`
    /// delta commits; the threshold is a write option, not a table property.
    async fn indexed_table(path: &Path, table_type: TableTypeValue, threshold: &str) -> Table {
        Table::create(path.to_str().unwrap())
            .with_table_name("t")
            .with_table_type(table_type)
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_ordering_fields(["ts"])
            .with_record_index_enabled(true)
            .with_column_stats_enabled(true)
            .with_partition_stats_enabled(true)
            .create()
            .await
            .unwrap();
        Table::new_with_options(path.to_str().unwrap(), [(MAX_DELTA_COMMITS, threshold)])
            .await
            .unwrap()
    }

    /// Every metadata partition's merged records, as sorted rendered rows.
    async fn metadata_views(path: &Path) -> BTreeMap<String, Vec<String>> {
        let table = Table::new(path.to_str().unwrap()).await.unwrap();
        let mut views = BTreeMap::new();
        for partition in table.get_metadata_table_partitions() {
            if partition == MetadataPartitionType::Files.partition_name() {
                // Map entries carry no order, so the records are compared decoded.
                let pruner = crate::table::partition::PartitionPruner::new(
                    &[],
                    &table.get_partition_schema().await.unwrap(),
                    table.hudi_configs.as_ref(),
                )
                .unwrap();
                let mut rows: Vec<String> = table
                    .read_metadata_table_files_partition(&pruner)
                    .await
                    .unwrap()
                    .into_values()
                    .map(|record| {
                        let mut files: Vec<String> = record
                            .files
                            .into_values()
                            .map(|f| format!("{}:{}:{}", f.name, f.size, f.is_deleted))
                            .collect();
                        files.sort();
                        format!(
                            "{}|{:?}|{}",
                            record.key,
                            record.record_type,
                            files.join(",")
                        )
                    })
                    .collect();
                rows.sort();
                views.insert(partition, rows);
                continue;
            }
            let batch = table
                .read_metadata_table_partition_arrow(&partition, &[])
                .await
                .unwrap();
            let formatters: Vec<_> = batch
                .columns()
                .iter()
                .map(|column| {
                    arrow::util::display::ArrayFormatter::try_new(
                        column.as_ref(),
                        &arrow::util::display::FormatOptions::default(),
                    )
                    .unwrap()
                })
                .collect();
            let mut rows: Vec<String> = (0..batch.num_rows())
                .map(|row| {
                    formatters
                        .iter()
                        .map(|f| f.value(row).to_string())
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect();
            rows.sort();
            views.insert(partition, rows);
        }
        views
    }

    async fn data_rows(path: &Path) -> Vec<(String, String, i64, i64)> {
        let table = Table::new(path.to_str().unwrap()).await.unwrap();
        let mut rows = Vec::new();
        for batch in table.read(&ReadOptions::new()).await.unwrap() {
            let s = |n: &str| {
                batch
                    .column_by_name(n)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .clone()
            };
            let i = |n: &str| {
                batch
                    .column_by_name(n)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone()
            };
            let (id, city, ts, v) = (s("id"), s("city"), i("ts"), i("v"));
            for row in 0..batch.num_rows() {
                rows.push((
                    id.value(row).into(),
                    city.value(row).into(),
                    ts.value(row),
                    v.value(row),
                ));
            }
        }
        rows.sort();
        rows
    }

    fn metadata_timeline_files(path: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(path.join(MDT_TIMELINE))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|n| n.starts_with(|c: char| c.is_ascii_digit()))
            .collect();
        names.sort();
        names
    }

    async fn write_history(table: &mut Table) {
        table
            .append([batch(&[
                ("a", "sf", 1, 10),
                ("b", "sf", 1, 20),
                ("c", "ny", 1, 30),
                ("d", "ny", 1, 40),
            ])])
            .await
            .unwrap();
        table
            .upsert([batch(&[("a", "sf", 2, 11), ("e", "la", 2, 50)])])
            .await
            .unwrap();
        table.delete("id = 'b'").await.unwrap();
        table
            .upsert([batch(&[("c", "ny", 3, 31), ("a", "sf", 3, 12)])])
            .await
            .unwrap();
    }

    /// Compaction leaves every metadata partition's merged view as it was,
    /// for both table types, and later writes land on the new base files.
    #[tokio::test]
    async fn test_metadata_compaction_preserves_every_partition() {
        for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path();
            let mut table = indexed_table(path, table_type.clone(), "1000").await;
            write_history(&mut table).await;
            let before = metadata_views(path).await;
            let rows = data_rows(path).await;
            assert!(before.values().all(|rows| !rows.is_empty()), "{before:?}");

            let mut handle =
                Table::new_with_options(path.to_str().unwrap(), [(MAX_DELTA_COMMITS, "1")])
                    .await
                    .unwrap();
            let instant = compact_metadata_table_if_needed(&mut handle)
                .await
                .unwrap()
                .expect("delta commits are above the threshold");
            let timeline = metadata_timeline_files(path);
            assert!(timeline.contains(&format!("{instant}.compaction.requested")));
            assert!(
                timeline
                    .iter()
                    .any(|n| n.starts_with(&format!("{instant}_")) && n.ends_with(".commit"))
            );
            let storage = handle.file_system_view.storage.clone();
            for partition in handle.get_metadata_table_partitions() {
                let slices = crate::write::metadata::mdt_partition_latest_slices(
                    storage.as_ref(),
                    &partition,
                )
                .await
                .unwrap();
                assert!(
                    slices.iter().any(|(_, base, _)| base
                        .as_deref()
                        .is_some_and(|b| b.ends_with(&format!("_{instant}.hfile")))),
                    "{partition} got no compacted base"
                );
                for (file_id, _, logs) in slices {
                    assert!(logs.is_empty(), "{partition}/{file_id} kept {logs:?}");
                }
            }
            assert_eq!(metadata_views(path).await, before, "[{table_type:?}]");
            assert_eq!(data_rows(path).await, rows);

            // Writes after the compaction log onto the new base files.
            let mut table = Table::new(path.to_str().unwrap()).await.unwrap();
            table
                .upsert([batch(&[("d", "ny", 4, 41), ("f", "sf", 4, 60)])])
                .await
                .unwrap();
            table.delete("id = 'e'").await.unwrap();
            let mut expected = rows.clone();
            expected.retain(|r| r.0 != "d" && r.0 != "e");
            expected.push(("d".into(), "ny".into(), 4, 41));
            expected.push(("f".into(), "sf".into(), 4, 60));
            expected.sort();
            assert_eq!(data_rows(path).await, expected);
            let table = Table::new(path.to_str().unwrap()).await.unwrap();
            use crate::index::HoodieIndex;
            let keys: Vec<crate::index::HoodieKey> = ["a", "b", "d", "e", "f"]
                .iter()
                .map(|k| crate::index::HoodieKey {
                    record_key: k.to_string(),
                    partition_path: String::new(),
                })
                .collect();
            let located = crate::index::for_table(&table)
                .tag_location(&table, &keys)
                .await
                .unwrap();
            let found: Vec<&str> = keys
                .iter()
                .filter(|key| located.get(*key).is_some_and(Option::is_some))
                .map(|key| key.record_key.as_str())
                .collect();
            assert_eq!(found, vec!["a", "d", "f"], "[{table_type:?}]");
        }
    }

    /// With the default threshold, writes compact the metadata table on their
    /// own once ten delta commits accumulate, and reads stay correct.
    #[tokio::test]
    async fn test_writes_compact_the_metadata_table_inline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let mut table = Table::create(path.to_str().unwrap())
            .with_table_name("t")
            .with_table_type(TableTypeValue::MergeOnRead)
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_ordering_fields(["ts"])
            .with_record_index_enabled(true)
            .create()
            .await
            .unwrap();
        let mut expected = BTreeMap::new();
        for round in 0..12i64 {
            let id = format!("k{}", round % 5);
            table
                .upsert([batch(&[(id.as_str(), "sf", round, round)])])
                .await
                .unwrap();
            expected.insert(id, round);
        }
        let commits: Vec<String> = metadata_timeline_files(path)
            .into_iter()
            .filter(|n| n.ends_with(".commit"))
            .collect();
        assert_eq!(commits.len(), 1, "{commits:?}");
        let rows = data_rows(path).await;
        let got: BTreeMap<String, i64> = rows.into_iter().map(|r| (r.0, r.3)).collect();
        assert_eq!(got, expected);
    }

    /// A pending data instant holds compaction back; a compaction a crash
    /// left pending is rolled back before the next one runs.
    #[tokio::test]
    async fn test_metadata_compaction_waits_for_pending_writes_and_rolls_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let mut table = indexed_table(path, TableTypeValue::MergeOnRead, "1000").await;
        write_history(&mut table).await;
        let mut handle =
            Table::new_with_options(path.to_str().unwrap(), [(MAX_DELTA_COMMITS, "1")])
                .await
                .unwrap();
        let storage = handle.file_system_view.storage.clone();

        let pending = crate::write::append::generate_instant_time().await;
        crate::write::fence_timeline_instant(
            storage.as_ref(),
            &crate::write::append::timeline_dir(&handle),
            &pending,
            Action::DeltaCommit,
            Vec::new(),
            Vec::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            compact_metadata_table_if_needed(&mut handle).await.unwrap(),
            None
        );
        for state in ["deltacommit.inflight", "deltacommit.requested"] {
            storage
                .delete_file(&format!(".hoodie/timeline/{pending}.{state}"))
                .await
                .unwrap();
        }

        let crashed = crate::write::append::generate_instant_time().await;
        crate::write::fence_timeline_instant(
            storage.as_ref(),
            MDT_TIMELINE,
            &crashed,
            Action::Compaction,
            HoodieCompactionPlan::default().to_avro_bytes().unwrap(),
            Vec::new(),
        )
        .await
        .unwrap();
        let orphan = path.join(format!(
            ".hoodie/metadata/files/files-0000-0_0-0-0_{crashed}.hfile"
        ));
        std::fs::write(&orphan, b"half a base file").unwrap();
        let before = metadata_views(path).await;
        let instant = compact_metadata_table_if_needed(&mut handle)
            .await
            .unwrap()
            .expect("the crashed compaction no longer blocks");
        assert!(!orphan.exists());
        assert!(
            metadata_timeline_files(path)
                .iter()
                .all(|n| !n.starts_with(&crashed))
        );
        assert!(instant > crashed);
        assert_eq!(metadata_views(path).await, before);
    }
}
