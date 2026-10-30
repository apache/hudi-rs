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
//! Metadata table bootstrap and files / record_index / stats partition updates.

use std::collections::{BTreeMap, HashMap};

use crate::Result;
use crate::file_group::log_file::scanner::LogFileScanner;
use crate::file_group::log_file::writer::LogFileWriter;
use crate::file_group::log_file::{BlockMetadataKey, BlockType};
use crate::hfile::{HFileReader, HFileWriter};
use crate::metadata::commit::{HoodieCommitMetadata, HoodieWriteStat};
use crate::metadata::table::column_stats::{
    COLUMN_STATS_COLUMN_LIST_KEY, COLUMN_STATS_MAX_COLUMNS_KEY, ColumnRangeStats,
    MAX_COLUMNS_TO_INDEX, aggregate_partition_stats, coerce_stat_value, column_stats_for_file,
    column_stats_tombstones, columns_to_index,
};
use crate::metadata::table::encode::{
    ColumnStatValue, ColumnStatsMetadata, FilesMetadataEntry, RecordIndexEntry, StatsIndexVersion,
    decode_column_stats_entry, encode_all_partitions, encode_column_stats, encode_files_record,
    encode_partition_stats, encode_record_index_entry, files_metadata_avro_schema_json,
    hoodie_metadata_schema_json, record_index_metadata_avro_schema_json,
};
use crate::metadata::table::hash::{column_stats_index_key, partition_stats_index_key};
use crate::metadata::table::records::{
    FilesPartitionRecord, MetadataPartitionType, MetadataRecordType,
};
use crate::storage::Storage;
use crate::table::{ReadOptions, Table};
use crate::timeline::instant::{Action, Instant};
use crate::timeline::selector::InstantRange;
use arrow_schema::Schema;

const METADATA_BASE: &str = ".hoodie/metadata";
const FILES_FILE_ID: &str = "files-0000-0";
/// Java `hoodie.metadata.record.index.min.filegroup.count` default.
pub(crate) const DEFAULT_RLI_NUM_FILE_GROUPS: usize = 10;
/// Java `hoodie.metadata.index.column.stats.file.group.count` default.
pub(crate) const DEFAULT_COLUMN_STATS_NUM_FILE_GROUPS: usize = 2;
/// Java `hoodie.metadata.index.partition.stats.file.group.count` default.
pub(crate) const DEFAULT_PARTITION_STATS_NUM_FILE_GROUPS: usize = 1;

fn column_stats_file_id(shard: usize) -> String {
    format!("col-stats-{shard:04}-0")
}

fn partition_stats_file_id(shard: usize) -> String {
    format!("partition-stats-{shard:04}-0")
}

/// A parquet (or deleted) data file whose column ranges should update MDT stats.
#[derive(Debug, Clone)]
pub struct StatsFileUpdate {
    pub partition_path: String,
    pub file_name: String,
    pub is_deleted: bool,
    /// Footer-derived ranges for adds; ignored when `is_deleted`.
    pub ranges: Vec<ColumnRangeStats>,
}

fn record_index_file_id(shard: usize) -> String {
    format!("record-index-{shard:04}-0")
}

/// Java `HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex` (31-hash, signed i32).
pub(crate) fn map_record_key_to_file_group_index(
    record_key: &str,
    num_file_groups: usize,
) -> usize {
    if num_file_groups <= 1 {
        return 0;
    }
    let mut h: i32 = 0;
    // Java's String.hashCode iterates UTF-16 code units (charAt), not Unicode
    // scalars; supplementary-plane keys must hash to the same shard as Java.
    for unit in record_key.encode_utf16() {
        h = h.wrapping_mul(31).wrapping_add(unit as i32);
    }
    (h.unsigned_abs() as usize) % num_file_groups
}

/// Java `HoodieBackedTableMetadataWriter.generateUniqueInstantTime` on a fresh table:
/// `SOLO_COMMIT_TIMESTAMP` plus `offset` millis, zero-padded to 17 digits.
fn bootstrap_instant(offset: usize) -> String {
    format!("{offset:017}")
}

/// Create the metadata table properties and its initial partitions.
///
/// Each MDT partition is initialized in its own deltacommit with a unique
/// instant (`00000000000000000`, `...001`, ...) in Java's enum order:
/// files, column_stats, record_index, partition_stats.
pub async fn bootstrap_metadata_table(
    data_table_storage: &Storage,
    table_name: &str,
    table_version: isize,
    record_index_enabled: bool,
    column_stats_enabled: bool,
    partition_stats_enabled: bool,
) -> Result<()> {
    let mdt_name = format!("{table_name}_metadata");
    let checksum = crc32(format!(".{mdt_name}").as_bytes());
    // The MDT's table version follows the data table's (Java initializes the
    // MDT meta client with the data write config's version).
    let properties = format!(
        "# Generated by hudi-rs\n\
         hoodie.table.name={mdt_name}\n\
         hoodie.table.type=MERGE_ON_READ\n\
         hoodie.table.base.file.format=HFILE\n\
         hoodie.table.recordkey.fields=key\n\
         hoodie.populate.meta.fields=false\n\
         hoodie.table.version={table_version}\n\
         hoodie.table.initial.version={table_version}\n\
         hoodie.timeline.layout.version=2\n\
         hoodie.timeline.path=timeline\n\
         hoodie.timeline.history.path=history\n\
         hoodie.archivelog.folder=history\n\
         hoodie.table.keygenerator.type=HOODIE_TABLE_METADATA\n\
         hoodie.compaction.payload.class=org.apache.hudi.metadata.HoodieMetadataPayload\n\
         hoodie.record.merge.mode=CUSTOM\n\
         hoodie.record.merge.strategy.id=00000000-0000-0000-0000-000000000000\n\
         hoodie.datasource.write.drop.partition.columns=false\n\
         hoodie.table.checksum={checksum}\n"
    );
    data_table_storage
        .put_file(
            &format!("{METADATA_BASE}/.hoodie/hoodie.properties"),
            properties.into_bytes(),
        )
        .await?;
    data_table_storage
        .put_file(
            &format!("{METADATA_BASE}/.hoodie/timeline/.keep"),
            b"".as_slice(),
        )
        .await?;

    // Java initializeFilesPartition on an empty table writes only the
    // __all_partitions__ record with an empty partition list — no "." entry.
    let files_instant = bootstrap_instant(0);
    let all_partitions = encode_all_partitions(Vec::new())?;
    let files_base_name = format!("{FILES_FILE_ID}_0-0-0_{files_instant}.hfile");
    let hfile = HFileWriter::write(
        &[(
            FilesPartitionRecord::ALL_PARTITIONS_KEY.to_string(),
            all_partitions,
        )],
        BTreeMap::from([(
            "schema".to_string(),
            files_metadata_avro_schema_json()?.as_bytes().to_vec(),
        )]),
    )
    .map_err(|error| crate::error::CoreError::HFile(error.to_string()))?;
    let files_base_size = hfile.len() as i64;
    crate::write::ensure_partition_metadata(
        data_table_storage,
        &format!("{METADATA_BASE}/files"),
        &files_instant,
    )
    .await?;
    data_table_storage
        .put_file(&format!("{METADATA_BASE}/files/{files_base_name}"), hfile)
        .await?;
    write_metadata_commit(
        data_table_storage,
        &files_instant,
        HashMap::from([(
            "files".to_string(),
            vec![HoodieWriteStat {
                file_id: Some(FILES_FILE_ID.to_string()),
                path: Some(format!("files/{files_base_name}")),
                base_file: Some(files_base_name),
                prev_commit: Some("null".to_string()),
                num_writes: Some(1),
                total_write_bytes: Some(files_base_size),
                file_size_in_bytes: Some(files_base_size),
                partition_path: Some("files".to_string()),
                ..Default::default()
            }],
        )]),
    )
    .await?;

    // Remaining partitions in Java MetadataPartitionType enum order, each with
    // the next unique bootstrap instant and its own deltacommit.
    let mut next_offset = 1usize;
    if column_stats_enabled {
        bootstrap_empty_hfile_partition(
            data_table_storage,
            &bootstrap_instant(next_offset),
            MetadataPartitionType::ColumnStats.partition_name(),
            DEFAULT_COLUMN_STATS_NUM_FILE_GROUPS,
            &column_stats_file_id,
            hoodie_metadata_schema_json()?,
        )
        .await?;
        next_offset += 1;
    }
    if record_index_enabled {
        bootstrap_empty_hfile_partition(
            data_table_storage,
            &bootstrap_instant(next_offset),
            MetadataPartitionType::RecordIndex.partition_name(),
            DEFAULT_RLI_NUM_FILE_GROUPS,
            &record_index_file_id,
            record_index_metadata_avro_schema_json()?,
        )
        .await?;
        next_offset += 1;
    }
    if partition_stats_enabled {
        bootstrap_empty_hfile_partition(
            data_table_storage,
            &bootstrap_instant(next_offset),
            MetadataPartitionType::PartitionStats.partition_name(),
            DEFAULT_PARTITION_STATS_NUM_FILE_GROUPS,
            &partition_stats_file_id,
            hoodie_metadata_schema_json()?,
        )
        .await?;
    }
    Ok(())
}

async fn bootstrap_empty_hfile_partition(
    storage: &Storage,
    instant: &str,
    partition_name: &str,
    num_file_groups: usize,
    file_id_fn: &dyn Fn(usize) -> String,
    schema_json: &str,
) -> Result<()> {
    crate::write::ensure_partition_metadata(
        storage,
        &format!("{METADATA_BASE}/{partition_name}"),
        instant,
    )
    .await?;
    let mut stats = Vec::with_capacity(num_file_groups);
    for shard in 0..num_file_groups {
        let file_id = file_id_fn(shard);
        let base_name = format!("{file_id}_0-0-0_{instant}.hfile");
        let empty = HFileWriter::write(
            &[],
            BTreeMap::from([("schema".to_string(), schema_json.as_bytes().to_vec())]),
        )
        .map_err(|error| crate::error::CoreError::HFile(error.to_string()))?;
        let size = empty.len() as i64;
        storage
            .put_file(
                &format!("{METADATA_BASE}/{partition_name}/{base_name}"),
                empty,
            )
            .await?;
        stats.push(HoodieWriteStat {
            file_id: Some(file_id),
            path: Some(format!("{partition_name}/{base_name}")),
            base_file: Some(base_name),
            prev_commit: Some("null".to_string()),
            num_writes: Some(0),
            total_write_bytes: Some(size),
            file_size_in_bytes: Some(size),
            partition_path: Some(partition_name.to_string()),
            ..Default::default()
        });
    }
    write_metadata_commit(
        storage,
        instant,
        HashMap::from([(partition_name.to_string(), stats)]),
    )
    .await
}

/// Apply file additions and deletions to the metadata table files partition.
///
/// Returns the `files` partition write stats for the caller's single MDT
/// deltacommit (see [`write_metadata_commit`]).
pub async fn update_files_partition_entries(
    data_storage: &Storage,
    instant: &str,
    files: &[(String, String, i64, bool)],
) -> Result<Vec<HoodieWriteStat>> {
    let mut by_partition: BTreeMap<String, Vec<FilesMetadataEntry>> = BTreeMap::new();
    for (partition, file_name, size, is_deleted) in files {
        let partition = if partition.is_empty() {
            FilesPartitionRecord::NON_PARTITIONED_NAME.to_string()
        } else {
            partition.clone()
        };
        by_partition
            .entry(partition)
            .or_default()
            .push(FilesMetadataEntry {
                name: file_name.clone(),
                size: *size,
                is_deleted: *is_deleted,
            });
    }
    let partitions = by_partition.keys().cloned().collect::<Vec<_>>();
    let all_partition_entries: Vec<FilesMetadataEntry> = partitions
        .iter()
        .map(|name| FilesMetadataEntry {
            name: name.clone(),
            size: 0,
            is_deleted: false,
        })
        .collect();
    let mut entries = vec![(
        FilesPartitionRecord::ALL_PARTITIONS_KEY.to_string(),
        encode_files_record(
            FilesPartitionRecord::ALL_PARTITIONS_KEY,
            MetadataRecordType::AllPartitions,
            all_partition_entries,
        )?,
    )];
    for (partition, files) in by_partition {
        entries.push((
            partition.clone(),
            encode_files_record(&partition, MetadataRecordType::Files, files)?,
        ));
    }
    let schema_json = files_metadata_avro_schema_json()?.to_string();
    let hfile = HFileWriter::write(
        &entries,
        BTreeMap::from([("schema".to_string(), schema_json.as_bytes().to_vec())]),
    )
    .map_err(|error| crate::error::CoreError::HFile(error.to_string()))?;
    // Java HFile data blocks embed SCHEMA (ordinal 2) alongside INSTANT_TIME.
    let block = LogFileWriter::write_log_block(
        BlockType::HfileData,
        HashMap::from([
            (BlockMetadataKey::InstantTime, instant.to_string()),
            (BlockMetadataKey::Schema, schema_json),
        ]),
        &hfile,
    );
    let log_name = format!(".{FILES_FILE_ID}_{instant}.log.1_0-0-0");
    let log_rel = format!("files/{log_name}");
    let log_size = block.len() as i64;
    data_storage
        .put_file(&format!("{METADATA_BASE}/{log_rel}"), block)
        .await?;
    Ok(vec![HoodieWriteStat {
        file_id: Some(FILES_FILE_ID.to_string()),
        path: Some(log_rel),
        log_files: Some(vec![log_name]),
        prev_commit: Some("null".to_string()),
        num_writes: Some(entries.len() as i64),
        total_write_bytes: Some(log_size),
        file_size_in_bytes: Some(log_size),
        partition_path: Some("files".to_string()),
        ..Default::default()
    }])
}

/// Append record-index put/delete entries.
///
/// Puts are HFile data blocks. Deletes are v3 delete log blocks — never type-5
/// records with null `recordIndexMetadata`, which NPEs Java's RLI reader.
///
/// Returns the `record_index` partition write stats for the caller's single
/// MDT deltacommit (see [`write_metadata_commit`]). At most `max_parallelism`
/// shards are built at once.
pub async fn update_record_index(
    data_storage: &Storage,
    instant: &str,
    entries: Vec<RecordIndexEntry>,
    max_parallelism: usize,
) -> Result<Vec<HoodieWriteStat>> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let num_groups = detect_rli_num_file_groups(data_storage).await?;
    let schema_json = record_index_metadata_avro_schema_json()?.to_string();
    // Only the routing happens here; each shard encodes its own entries on
    // the blocking pool below.
    let mut puts_by_shard: HashMap<usize, Vec<RecordIndexEntry>> = HashMap::new();
    let mut deletes_by_shard: HashMap<usize, Vec<(String, String)>> = HashMap::new();
    for entry in entries {
        let shard = map_record_key_to_file_group_index(&entry.record_key, num_groups);
        if entry.is_deleted {
            // DeleteRecord.partitionPath is the MDT partition (Java HoodieKey for RLI deletes).
            deletes_by_shard.entry(shard).or_default().push((
                entry.record_key,
                MetadataPartitionType::RecordIndex
                    .partition_name()
                    .to_string(),
            ));
        } else {
            puts_by_shard.entry(shard).or_default().push(entry);
        }
    }

    let shards: std::collections::BTreeSet<usize> = puts_by_shard
        .keys()
        .chain(deletes_by_shard.keys())
        .copied()
        .collect();
    // Building a shard's blocks (sorting its keys, encoding the HFile) is
    // CPU-only, so up to one shard per core (and no more than `max_parallelism`)
    // is built on the blocking pool while finished shards are written, in shard
    // order, and released.
    use futures::StreamExt;
    let parallelism = std::thread::available_parallelism()
        .map_or(4, std::num::NonZeroUsize::get)
        .min(max_parallelism)
        .max(1);
    let mut builds = futures::stream::iter(shards)
        .map(|shard| {
            let puts = puts_by_shard.remove(&shard).unwrap_or_default();
            let deletes = deletes_by_shard.remove(&shard).unwrap_or_default();
            let schema_json = schema_json.clone();
            let instant = instant.to_string();
            tokio::task::spawn_blocking(move || -> Result<(usize, Vec<u8>, i64)> {
                let puts = puts
                    .into_iter()
                    .map(|entry| {
                        let bytes = encode_record_index_entry(&entry)?;
                        Ok((entry.record_key, bytes))
                    })
                    .collect::<Result<Vec<(String, Vec<u8>)>>>()?;
                let mut log_bytes = Vec::new();
                let mut num_writes = 0i64;
                if !puts.is_empty() {
                    num_writes += puts.len() as i64;
                    let hfile = HFileWriter::write(
                        &puts,
                        BTreeMap::from([("schema".to_string(), schema_json.as_bytes().to_vec())]),
                    )
                    .map_err(|error| crate::error::CoreError::HFile(error.to_string()))?;
                    log_bytes.extend(LogFileWriter::write_log_block(
                        BlockType::HfileData,
                        HashMap::from([
                            (BlockMetadataKey::InstantTime, instant.clone()),
                            (BlockMetadataKey::Schema, schema_json),
                        ]),
                        &hfile,
                    ));
                }
                if !deletes.is_empty() {
                    num_writes += deletes.len() as i64;
                    log_bytes.extend(crate::write::build_delete_log_block(&instant, &deletes, 0)?);
                }
                Ok((shard, log_bytes, num_writes))
            })
        })
        .buffered(parallelism);
    let mut stats = Vec::new();
    while let Some(shard_build) = builds.next().await {
        let (shard, log_bytes, num_writes) = shard_build.map_err(|e| {
            crate::error::CoreError::Write(format!("record index shard build failed: {e}"))
        })??;
        if log_bytes.is_empty() {
            continue;
        }
        let file_id = record_index_file_id(shard);
        let log_name = format!(".{file_id}_{instant}.log.1_0-0-0");
        let log_rel = format!("record_index/{log_name}");
        let log_size = log_bytes.len() as i64;
        data_storage
            .put_file(&format!("{METADATA_BASE}/{log_rel}"), log_bytes)
            .await?;
        stats.push(HoodieWriteStat {
            file_id: Some(file_id),
            path: Some(log_rel),
            log_files: Some(vec![log_name]),
            prev_commit: Some("null".to_string()),
            num_writes: Some(num_writes),
            total_write_bytes: Some(log_size),
            file_size_in_bytes: Some(log_size),
            partition_path: Some("record_index".to_string()),
            ..Default::default()
        });
    }
    Ok(stats)
}

async fn detect_rli_num_file_groups(storage: &Storage) -> Result<usize> {
    detect_num_file_groups(
        storage,
        MetadataPartitionType::RecordIndex.partition_name(),
        "record-index-",
        DEFAULT_RLI_NUM_FILE_GROUPS,
    )
    .await
}

async fn detect_num_file_groups(
    storage: &Storage,
    partition_name: &str,
    file_id_prefix: &str,
    default: usize,
) -> Result<usize> {
    // Committed slices only: an orphan HFile from a crashed writer must not
    // change the shard modulus (it would diverge from what Java computes),
    // and listing errors must propagate rather than silently changing it.
    let slices = mdt_partition_latest_slices(storage, partition_name).await?;
    let mut shards = std::collections::BTreeSet::new();
    for (file_id, _, _) in &slices {
        if let Some(rest) = file_id.strip_prefix(file_id_prefix)
            && let Some(shard_str) = rest.get(..4)
            && let Ok(shard) = shard_str.parse::<usize>()
        {
            shards.insert(shard);
        }
    }
    if shards.is_empty() {
        Ok(default)
    } else {
        Ok(shards.len())
    }
}

/// Refuse to write a table whose metadata table has a layout this writer does
/// not maintain: other index partitions would go stale, a partition an async
/// indexer is still building would miss the write, and a partitioned record
/// index hashes keys per data partition into file groups this writer cannot
/// address.
pub(crate) async fn ensure_supported_metadata_layout(table: &Table) -> Result<()> {
    use crate::config::table::HudiTableConfig::MetadataTablePartitions;
    use crate::error::CoreError;
    if !table.is_metadata_table_enabled() {
        return Ok(());
    }
    let supported = [
        MetadataPartitionType::Files,
        MetadataPartitionType::ColumnStats,
        MetadataPartitionType::PartitionStats,
        MetadataPartitionType::RecordIndex,
    ]
    .map(|partition| partition.partition_name());
    let partitions: Vec<String> = table
        .hudi_configs
        .get_or_default(MetadataTablePartitions)
        .into();
    if let Some(partition) = partitions
        .iter()
        .map(|p| p.trim())
        .find(|p| !p.is_empty() && !supported.contains(p))
    {
        return Err(CoreError::Unsupported(format!(
            "writing a table whose metadata table has the '{partition}' partition \
             ({}) is not supported; supported partitions are {}",
            MetadataTablePartitions.as_ref(),
            supported.join(", ")
        )));
    }
    let inflight_key = "hoodie.table.metadata.partitions.inflight";
    if let Some(inflight) = table
        .hudi_configs
        .as_options()
        .get(inflight_key)
        .filter(|value| value.split(',').any(|p| !p.trim().is_empty()))
    {
        return Err(CoreError::Unsupported(format!(
            "writing a table whose metadata table partitions are still being built \
             ({inflight_key}={inflight}) is not supported"
        )));
    }
    if crate::index::is_record_index_enabled(table) {
        let storage = table.file_system_view.storage.as_ref();
        let partition = MetadataPartitionType::RecordIndex.partition_name();
        for (file_id, _, _) in mdt_partition_latest_slices(storage, partition).await? {
            if !is_global_record_index_file_id(&file_id) {
                return Err(CoreError::Unsupported(format!(
                    "writing a table with a partitioned record index \
                     (hoodie.metadata.record.level.index.enable, file group '{file_id}') \
                     is not supported"
                )));
            }
        }
    }
    Ok(())
}

/// Java names global record index file groups `record-index-NNNN-0` and
/// partitioned ones `record-index-<escapedPartition>-NNNN-0`.
fn is_global_record_index_file_id(file_id: &str) -> bool {
    file_id
        .strip_prefix("record-index-")
        .and_then(|rest| rest.split_once('-'))
        .is_some_and(|(index, suffix)| {
            [index, suffix]
                .iter()
                .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
        })
}

/// Whether `column_stats` is listed in `hoodie.table.metadata.partitions`.
pub fn is_column_stats_enabled(table: &Table) -> bool {
    mdt_partition_enabled(table, MetadataPartitionType::ColumnStats)
}

/// Whether `partition_stats` is listed in `hoodie.table.metadata.partitions`.
pub fn is_partition_stats_enabled(table: &Table) -> bool {
    mdt_partition_enabled(table, MetadataPartitionType::PartitionStats)
}

fn mdt_partition_enabled(table: &Table, partition: MetadataPartitionType) -> bool {
    use crate::config::table::HudiTableConfig::MetadataTablePartitions;
    let partitions: Vec<String> = table
        .hudi_configs
        .get_or_default(MetadataTablePartitions)
        .into();
    partitions
        .iter()
        .any(|p| p.trim() == partition.partition_name())
}

/// Java `HoodieTableMetadataUtil.existingIndexVersionOrDefault`: stats keep
/// the version the column_stats (else partition_stats) index definition
/// records, so a table upgraded from version 8 stays V1; Java requires both
/// definitions to agree (`HoodieIndexMetadata.validateIndexMetadata`). Without
/// either, stats are V2 from table version 9 (`HoodieIndexVersion.getCurrentVersion`).
fn stats_index_version(
    table: &Table,
    definitions: Option<&serde_json::Value>,
) -> Result<StatsIndexVersion> {
    for partition in [
        MetadataPartitionType::ColumnStats,
        MetadataPartitionType::PartitionStats,
    ] {
        let name = partition.partition_name();
        match definitions
            .and_then(|root| root.pointer(&format!("/indexDefinitions/{name}/version")))
        {
            None | Some(serde_json::Value::Null) => {}
            Some(version) => {
                return match version.as_str() {
                    Some("V1") => Ok(StatsIndexVersion::V1),
                    Some("V2") => Ok(StatsIndexVersion::V2),
                    _ => Err(crate::error::CoreError::MetadataTable(format!(
                        "unsupported {name} index version {version}"
                    ))),
                };
            }
        }
    }
    let table_version: isize = table
        .hudi_configs
        .get_or_default(crate::config::table::HudiTableConfig::TableVersion)
        .into();
    Ok(if table_version >= 9 {
        StatsIndexVersion::V2
    } else {
        StatsIndexVersion::V1
    })
}

/// Columns indexed by `column_stats` / `partition_stats` for this table,
/// honoring the column list and max-columns write configs.
fn table_columns_to_index(
    table: &Table,
    schema: &Schema,
    version: StatsIndexVersion,
) -> Vec<String> {
    let options = table.hudi_configs.as_options();
    let column_list: Vec<String> = options
        .get(COLUMN_STATS_COLUMN_LIST_KEY)
        .map(|list| {
            list.split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let max_columns = options
        .get(COLUMN_STATS_MAX_COLUMNS_KEY)
        .and_then(|max| max.trim().parse().ok())
        .unwrap_or(MAX_COLUMNS_TO_INDEX);
    columns_to_index(schema, version, &column_list, max_columns)
}

/// Java `HoodieTableConfig.RELATIVE_INDEX_DEFINITION_PATH`.
pub(crate) const INDEX_DEFINITION_PATH_KEY: &str = "hoodie.table.index.defs.path";

/// Java's default index definition location, relative to the base path.
pub(crate) const DEFAULT_INDEX_DEFINITION_PATH: &str = ".hoodie/.index_defs/index.json";

/// The index definitions JSON with the `column_stats` definition set to
/// `columns` (Java `HoodieSparkIndexClient.createOrUpdateColumnStatsIndexDefinition`),
/// keeping every other definition. `None` when `existing` already matches.
pub(crate) fn column_stats_index_definition_json(
    existing: Option<&[u8]>,
    columns: &[String],
    version: StatsIndexVersion,
) -> Result<Option<Vec<u8>>> {
    use serde_json::{Value, json};
    let mut root: Value = match existing {
        Some(bytes) if !bytes.is_empty() => serde_json::from_slice(bytes).map_err(|e| {
            crate::error::CoreError::MetadataTable(format!("invalid index definitions: {e}"))
        })?,
        _ => json!({}),
    };
    let name = MetadataPartitionType::ColumnStats.partition_name();
    let current = root.pointer(&format!("/indexDefinitions/{name}"));
    if current
        .and_then(|def| def.get("sourceFields"))
        .is_some_and(|fields| *fields == json!(columns))
    {
        return Ok(None);
    }
    // An existing definition keeps its version (Java existingIndexVersionOrDefault).
    let version = current
        .and_then(|def| def.get("version").cloned())
        .unwrap_or_else(|| {
            json!(match version {
                StatsIndexVersion::V1 => "V1",
                StatsIndexVersion::V2 => "V2",
            })
        });
    let definition = json!({
        "indexName": name,
        "indexType": name,
        "indexFunction": name,
        "sourceFields": columns,
        "indexOptions": {},
        "version": version,
    });
    let Some(root_object) = root.as_object_mut() else {
        return Err(crate::error::CoreError::MetadataTable(
            "index definitions must be a JSON object".to_string(),
        ));
    };
    let definitions = root_object
        .entry("indexDefinitions")
        .or_insert_with(|| json!({}));
    let Some(definitions) = definitions.as_object_mut() else {
        return Err(crate::error::CoreError::MetadataTable(
            "indexDefinitions must be a JSON object".to_string(),
        ));
    };
    definitions.insert(name.to_string(), definition);
    serde_json::to_vec_pretty(&root)
        .map(Some)
        .map_err(|e| crate::error::CoreError::MetadataTable(e.to_string()))
}

/// The table's configured index definitions path and the file's contents, if
/// it exists.
async fn read_index_definitions(table: &Table) -> Result<Option<(String, Option<bytes::Bytes>)>> {
    let Some(path) = table
        .hudi_configs
        .as_options()
        .get(INDEX_DEFINITION_PATH_KEY)
        .filter(|path| !path.trim().is_empty())
        .cloned()
    else {
        return Ok(None);
    };
    let storage = table.file_system_view.storage.clone();
    let existing = if storage.exists(&path).await? {
        Some(storage.get_file_data(&path).await?)
    } else {
        None
    };
    Ok(Some((path, existing)))
}

/// Keep the table's `column_stats` index definition in step with the indexed
/// columns, as Java does on every commit. Spark only uses column_stats and
/// partition_stats for data skipping on the columns this definition lists.
async fn write_column_stats_index_definition(
    table: &Table,
    definitions: Option<&(String, Option<bytes::Bytes>)>,
    columns: &[String],
    version: StatsIndexVersion,
) -> Result<()> {
    let Some((path, existing)) = definitions else {
        return Ok(());
    };
    if let Some(json) = column_stats_index_definition_json(existing.as_deref(), columns, version)? {
        table.file_system_view.storage.put_file(path, json).await?;
    }
    Ok(())
}

/// Update MDT `column_stats` and `partition_stats` for written files.
///
/// Partition stats follow Java `convertMetadataToPartitionStatRecords`: for each
/// touched partition, recompute a tight-bound aggregate over this commit's new
/// files plus the existing MDT `column_stats` of all files still in the latest
/// file slices. `replaced_files` lists relative paths superseded by this commit
/// (COW rewrites / replacecommits) to exclude from that scan.
///
/// Returns per-MDT-partition write stats for the caller's single MDT
/// deltacommit (see [`write_metadata_commit`]).
pub async fn update_column_stats_partitions(
    table: &Table,
    instant: &str,
    updates: &[StatsFileUpdate],
    schema: &Schema,
    replaced_files: &[String],
) -> Result<HashMap<String, Vec<HoodieWriteStat>>> {
    let mut out: HashMap<String, Vec<HoodieWriteStat>> = HashMap::new();
    if updates.is_empty() {
        return Ok(out);
    }
    let data_storage = table.file_system_view.storage.clone();
    // Written files always carry the meta fields, so their footers contribute
    // meta-column ranges. If the caller passed the raw user schema, those
    // columns would be missing from the survivor fetch and the partition
    // aggregate would collapse onto this commit's files (Java indexes the
    // three meta columns unconditionally).
    let full_schema = crate::write::append::schema_with_meta_fields(schema);
    let index_definitions = read_index_definitions(table).await?;
    let parsed_definitions: Option<serde_json::Value> = index_definitions
        .as_ref()
        .and_then(|(_, existing)| existing.as_deref())
        .filter(|bytes| !bytes.is_empty())
        .map(serde_json::from_slice)
        .transpose()
        .map_err(|e| {
            crate::error::CoreError::MetadataTable(format!("invalid index definitions: {e}"))
        })?;
    let stats_version = stats_index_version(table, parsed_definitions.as_ref())?;
    let indexed_columns = table_columns_to_index(table, &full_schema, stats_version);
    write_column_stats_index_definition(
        table,
        index_definitions.as_ref(),
        &indexed_columns,
        stats_version,
    )
    .await?;
    // Footers carry every supported column; only the indexed ones are written.
    let updates: Vec<StatsFileUpdate> = updates
        .iter()
        .map(|update| {
            let mut update = update.clone();
            update
                .ranges
                .retain(|r| indexed_columns.contains(&r.column_name));
            update
        })
        .collect();
    let updates = updates.as_slice();
    let mut column_records: Vec<(String, ColumnStatsMetadata)> = Vec::new();

    for update in updates {
        let payloads = if update.is_deleted {
            column_stats_tombstones(&update.file_name, &indexed_columns)
        } else {
            column_stats_for_file(&update.file_name, &update.ranges, false)
        };
        for payload in payloads {
            let key = column_stats_index_key(
                &update.partition_path,
                &update.file_name,
                &payload.column_name,
            );
            column_records.push((key, payload));
        }
    }

    if !column_records.is_empty() {
        let stats = write_stats_log_blocks(
            data_storage.as_ref(),
            instant,
            MetadataPartitionType::ColumnStats.partition_name(),
            "col-stats-",
            DEFAULT_COLUMN_STATS_NUM_FILE_GROUPS,
            &column_stats_file_id,
            &column_records,
            true,
            stats_version,
        )
        .await?;
        out.insert(
            MetadataPartitionType::ColumnStats
                .partition_name()
                .to_string(),
            stats,
        );
    }

    if is_partition_stats_enabled(table) {
        let partition_records = compute_partition_stats_records(
            table,
            updates,
            &full_schema,
            &indexed_columns,
            replaced_files,
        )
        .await?;
        if !partition_records.is_empty() {
            let stats = write_stats_log_blocks(
                data_storage.as_ref(),
                instant,
                MetadataPartitionType::PartitionStats.partition_name(),
                "partition-stats-",
                DEFAULT_PARTITION_STATS_NUM_FILE_GROUPS,
                &partition_stats_file_id,
                &partition_records,
                false,
                stats_version,
            )
            .await?;
            out.insert(
                MetadataPartitionType::PartitionStats
                    .partition_name()
                    .to_string(),
                stats,
            );
        }
    }
    Ok(out)
}

fn split_partition_and_name(relative_path: &str) -> (String, String) {
    match relative_path.rsplit_once('/') {
        Some((partition, name)) => (partition.to_string(), name.to_string()),
        None => (String::new(), relative_path.to_string()),
    }
}

/// Tight-bound partition_stats recompute (Java `convertMetadataToPartitionStatRecords`
/// with `isShouldScanColStatsForTightBound = true`, which holds whenever the
/// `column_stats` partition exists — a prerequisite of `partition_stats`).
async fn compute_partition_stats_records(
    table: &Table,
    updates: &[StatsFileUpdate],
    schema: &Schema,
    indexed_columns: &[String],
    replaced_files: &[String],
) -> Result<Vec<(String, ColumnStatsMetadata)>> {
    let touched: std::collections::BTreeSet<String> = updates
        .iter()
        .filter(|u| !u.is_deleted)
        .map(|u| u.partition_path.clone())
        .collect();
    if touched.is_empty() {
        return Ok(Vec::new());
    }
    let excluded: std::collections::HashSet<(String, String)> = replaced_files
        .iter()
        .map(|path| split_partition_and_name(path))
        .chain(
            updates
                .iter()
                .map(|u| (u.partition_path.clone(), u.file_name.clone())),
        )
        .collect();

    // Files still live in the latest pre-commit file slices of touched partitions.
    let mut survivors: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for slice in table.get_file_slices(&ReadOptions::new()).await? {
        let partition = slice.partition_path.clone();
        if !touched.contains(&partition) {
            continue;
        }
        // Log-only slices contribute no base file name.
        let mut names = Vec::new();
        if let Some(base_path) = slice.base_file_relative_path()? {
            let (_, base_name) = split_partition_and_name(&base_path);
            names.push(base_name);
        }
        for log_file in &slice.log_files {
            let (_, log_name) = split_partition_and_name(&slice.log_file_relative_path(log_file)?);
            names.push(log_name);
        }
        for name in names {
            if !excluded.contains(&(partition.clone(), name.clone())) {
                survivors.entry(partition.clone()).or_default().push(name);
            }
        }
    }

    // Fetch the survivors' column_stats from the MDT.
    let mut wanted: HashMap<String, (String, String)> = HashMap::new();
    for (partition, files) in &survivors {
        for file in files {
            for column in indexed_columns {
                wanted.insert(
                    column_stats_index_key(partition, file, column),
                    (partition.clone(), file.clone()),
                );
            }
        }
    }
    let valid_instants = table.valid_metadata_instants().await?;
    let existing = load_column_stats_records(
        table.file_system_view.storage.clone(),
        table.hudi_configs.clone(),
        &wanted,
        &valid_instants,
    )
    .await?;

    // Ranges per (partition, file): this commit's in-memory ranges + survivors.
    let mut ranges: BTreeMap<String, BTreeMap<String, Vec<ColumnRangeStats>>> = BTreeMap::new();
    for update in updates {
        if update.is_deleted || update.ranges.is_empty() {
            continue;
        }
        ranges
            .entry(update.partition_path.clone())
            .or_default()
            .entry(update.file_name.clone())
            .or_default()
            .extend(update.ranges.iter().cloned());
    }
    for (key, stats) in existing {
        if stats.is_deleted {
            continue;
        }
        let Some((partition, file)) = wanted.get(&key) else {
            continue;
        };
        ranges
            .entry(partition.clone())
            .or_default()
            .entry(file.clone())
            .or_default()
            .push(ColumnRangeStats {
                column_name: stats.column_name,
                range_known: true,
                min_value: stats.min_value,
                max_value: stats.max_value,
                value_count: stats.value_count,
                null_count: stats.null_count,
                total_size: stats.total_size,
                total_uncompressed_size: stats.total_uncompressed_size,
            });
    }

    // Survivors decoded from the MDT and this commit's footer ranges merge in
    // the column's own type (Java FileFormatUtils.getColumnRangeInPartition).
    let coerce =
        |value: Option<ColumnStatValue>, column: &str| -> Result<Option<ColumnStatValue>> {
            let Some(value) = value else {
                return Ok(None);
            };
            let Ok(field) = schema.field_with_name(column) else {
                return Ok(Some(value));
            };
            coerce_stat_value(value.clone(), field.data_type())
                .map(Some)
                .ok_or_else(|| {
                    crate::error::CoreError::MetadataTable(format!(
                        "column_stats value {value:?} of column '{column}' does not convert to {}",
                        field.data_type()
                    ))
                })
        };
    let mut out = Vec::new();
    for (partition, by_file) in ranges {
        let mut file_ranges: Vec<Vec<ColumnRangeStats>> = by_file.into_values().collect();
        for range in file_ranges.iter_mut().flatten() {
            range.min_value = coerce(range.min_value.take(), &range.column_name)?;
            range.max_value = coerce(range.max_value.take(), &range.column_name)?;
        }
        for payload in aggregate_partition_stats(&partition, &file_ranges, true)? {
            let key = partition_stats_index_key(&partition, &payload.column_name);
            out.push((key, payload));
        }
    }
    Ok(out)
}

/// Read `column_stats` records for the requested keys from the MDT partition
/// (HFile bases + log blocks, later blocks win), mirroring the RLI loader.
///
/// `storage` and `hudi_configs` must belong to the DATA table (paths are
/// resolved under `.hoodie/metadata` and fencing lists the data timeline).
/// Latest file slice per file group in an MDT partition, via the same
/// FileGroup/FileSlice machinery the data table uses: multi-base aware (a
/// Spark MDT compaction leaves several base versions; only the newest slice
/// is live) and log files ordered by version/completion, not lexicographically.
///
/// Returns `(file id, base hfile relative path, ordered log relative paths)`.
/// The base is `None` for a log-only file group: Spark initializes
/// column_stats and partition_stats that way, and every log of such a group
/// is live.
pub(crate) async fn mdt_partition_latest_slices(
    storage: &Storage,
    partition: &str,
) -> Result<Vec<(String, Option<String>, Vec<String>)>> {
    use std::str::FromStr;
    let dir = format!("{METADATA_BASE}/{partition}");
    let listed = match storage.list_files(Some(&dir)).await {
        Ok(files) => files,
        Err(crate::storage::error::StorageError::ObjectStoreError(
            object_store::Error::NotFound { .. },
        )) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    // Admit only bases whose instant is committed on the MDT timeline: a crash
    // between writing a compaction HFile and completing its .commit must not
    // let the orphan base shadow the committed slice (and silently drop the
    // still-live pre-compaction logs). Committed means completed on the active
    // timeline or below its earliest commit instant, which archival only moves
    // past completed ones. Log blocks are fenced separately by the callers via
    // valid_metadata_instants.
    let mdt_timeline_dir = format!("{METADATA_BASE}/.hoodie/timeline");
    let mut completed_mdt_instants: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut earliest_active: Option<String> = None;
    match storage.list_files(Some(&mdt_timeline_dir)).await {
        Ok(files) => {
            for file in files {
                let name = file.name;
                if let Ok(instant) = Instant::from_str(&name)
                    && crate::timeline::DEFAULT_LOADING_ACTIONS.contains(&instant.action)
                    && earliest_active
                        .as_ref()
                        .is_none_or(|earliest| instant.timestamp < *earliest)
                {
                    earliest_active = Some(instant.timestamp);
                }
                if !name.chars().next().is_some_and(|c| c.is_ascii_digit())
                    || name.ends_with(".requested")
                    || name.ends_with(".inflight")
                {
                    continue;
                }
                completed_mdt_instants
                    .insert(name.chars().take_while(char::is_ascii_digit).collect());
            }
        }
        Err(crate::storage::error::StorageError::ObjectStoreError(
            object_store::Error::NotFound { .. },
        )) => {}
        Err(error) => return Err(error.into()),
    }
    let is_committed = |timestamp: &String| {
        completed_mdt_instants.contains(timestamp)
            || earliest_active
                .as_ref()
                .is_some_and(|earliest| timestamp < earliest)
    };

    // Two passes: log-to-slice association requires the base files to be in
    // place first, and storage listing order is not guaranteed.
    let mut bases = Vec::new();
    let mut logs = Vec::new();
    for file in listed {
        let name = file.name;
        if name.ends_with(".hfile") && !name.starts_with('.') {
            let base = crate::file_group::base_file::BaseFile::from_str(&name)?;
            if !is_committed(&base.commit_timestamp) {
                continue;
            }
            bases.push(base);
        } else if name.starts_with('.')
            && name.contains(".log.")
            && crate::file_group::log_file::LogFile::is_log_file_name(&name)
        {
            let mut log = crate::file_group::log_file::LogFile::from_str(&name)?;
            // MDT logs are named by their deltacommit instant; using it as the
            // completion time gives v8 completion-based slice attachment (and
            // correctly excludes pre-compaction logs from a newer base slice).
            log.completion_timestamp = Some(log.timestamp.clone());
            logs.push(log);
        }
    }
    let mut groups: HashMap<String, crate::file_group::FileGroup> = HashMap::new();
    for base in bases {
        groups
            .entry(base.file_id.clone())
            .or_insert_with(|| {
                crate::file_group::FileGroup::new(base.file_id.clone(), partition.to_string())
            })
            .add_base_file(base)?;
    }
    let mut log_only: BTreeMap<String, Vec<crate::file_group::log_file::LogFile>> = BTreeMap::new();
    for log in logs {
        match groups.get_mut(&log.file_id) {
            Some(group) => {
                group.add_log_file(log)?;
            }
            None => log_only.entry(log.file_id.clone()).or_default().push(log),
        }
    }
    let mut out = Vec::new();
    for (id, group) in &groups {
        let Some(slice) = group.get_file_slice_as_of("99999999999999999") else {
            continue;
        };
        let Some(base) = slice.base_file.as_ref() else {
            continue;
        };
        let base_path = format!("{dir}/{}", base.file_name());
        let logs = slice
            .log_files
            .iter()
            .map(|log| format!("{dir}/{}", log.file_name()))
            .collect();
        out.push((id.clone(), Some(base_path), logs));
    }
    for (id, mut logs) in log_only {
        logs.sort();
        let logs = logs
            .iter()
            .map(|log| format!("{dir}/{}", log.file_name()))
            .collect();
        out.push((id, None, logs));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

pub(crate) async fn load_column_stats_records(
    storage: std::sync::Arc<Storage>,
    hudi_configs: std::sync::Arc<crate::config::HudiConfigs>,
    wanted: &HashMap<String, (String, String)>,
    valid_instants: &std::collections::HashSet<String>,
) -> Result<HashMap<String, ColumnStatsMetadata>> {
    if wanted.is_empty() {
        return Ok(HashMap::new());
    }
    let partition = MetadataPartitionType::ColumnStats.partition_name();
    let mut base_paths = Vec::new();
    let mut log_paths = Vec::new();
    for (_, base, logs) in mdt_partition_latest_slices(storage.as_ref(), partition).await? {
        base_paths.extend(base);
        log_paths.extend(logs);
    }

    let mut sorted_keys: Vec<&str> = wanted.keys().map(String::as_str).collect();
    sorted_keys.sort_unstable();

    let mut merged: HashMap<String, ColumnStatsMetadata> = HashMap::new();
    for base in base_paths {
        let mut reader = HFileReader::open(storage.as_ref(), &base)
            .await
            .map_err(|e| crate::error::CoreError::HFile(format!("failed to open {base}: {e:?}")))?;
        let records = reader
            .lookup_records(&sorted_keys)
            .map_err(|e| crate::error::CoreError::HFile(format!("failed to read {base}: {e:?}")))?;
        for (_, record) in records {
            let Some(record) = record else { continue };
            apply_column_stats_record(&mut merged, wanted, &record)?;
        }
    }
    if !log_paths.is_empty() {
        // Only log blocks from instants in the caller's valid set are trusted
        // (Java getValidInstantTimestamps); orphan MDT commits are skipped.
        let scanner = LogFileScanner::new(hudi_configs, storage);
        let range = InstantRange::exact_match(valid_instants.iter().cloned(), "UTC");
        for record in &scanner.scan_hfile_records(log_paths, &range).await? {
            apply_column_stats_record(&mut merged, wanted, record)?;
        }
    }
    Ok(merged)
}

fn apply_column_stats_record(
    merged: &mut HashMap<String, ColumnStatsMetadata>,
    wanted: &HashMap<String, (String, String)>,
    record: &crate::hfile::HFileRecord,
) -> Result<()> {
    let Some(key) = record.key_as_str() else {
        return Ok(());
    };
    if !wanted.contains_key(key) {
        return Ok(());
    }
    if let Some(stats) = decode_column_stats_entry(record.value(), record.avro_schema())? {
        // Java HoodieMetadataPayload semantics: successive stats for a key
        // merge (range union + count sums) unless the newer record is a
        // tombstone or tight-bound, which replaces. Plain last-wins would
        // shrink bounds written by non-tight-bound writers (e.g. Spark
        // partition_stats).
        let payload_merger = crate::metadata::payload_merger::MetadataPayloadMerger;
        match merged.remove(key) {
            Some(existing) => {
                merged.insert(
                    key.to_string(),
                    payload_merger.merge_column_stats(&existing, &stats),
                );
            }
            None => {
                merged.insert(key.to_string(), stats);
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn write_stats_log_blocks(
    data_storage: &Storage,
    instant: &str,
    partition_name: &str,
    file_id_prefix: &str,
    default_groups: usize,
    file_id_fn: &dyn Fn(usize) -> String,
    records: &[(String, ColumnStatsMetadata)],
    is_column_stats: bool,
    version: StatsIndexVersion,
) -> Result<Vec<HoodieWriteStat>> {
    let num_groups =
        detect_num_file_groups(data_storage, partition_name, file_id_prefix, default_groups)
            .await?;
    let schema_json = hoodie_metadata_schema_json()?.to_string();
    let mut by_shard: HashMap<usize, Vec<(String, Vec<u8>)>> = HashMap::new();
    for (key, payload) in records {
        let bytes = if is_column_stats {
            encode_column_stats(payload, version)?
        } else {
            encode_partition_stats(payload, version)?
        };
        let shard = map_record_key_to_file_group_index(key, num_groups);
        by_shard
            .entry(shard)
            .or_default()
            .push((key.clone(), bytes));
    }
    let mut stats = Vec::new();
    for (shard, entries) in by_shard {
        let file_id = file_id_fn(shard);
        let hfile = HFileWriter::write(
            &entries,
            BTreeMap::from([("schema".to_string(), schema_json.as_bytes().to_vec())]),
        )
        .map_err(|error| crate::error::CoreError::HFile(error.to_string()))?;
        let block = LogFileWriter::write_log_block(
            BlockType::HfileData,
            HashMap::from([
                (BlockMetadataKey::InstantTime, instant.to_string()),
                (BlockMetadataKey::Schema, schema_json.clone()),
            ]),
            &hfile,
        );
        let log_name = format!(".{file_id}_{instant}.log.1_0-0-0");
        let log_rel = format!("{partition_name}/{log_name}");
        let log_size = block.len() as i64;
        data_storage
            .put_file(&format!("{METADATA_BASE}/{log_rel}"), block)
            .await?;
        stats.push(HoodieWriteStat {
            file_id: Some(file_id),
            path: Some(log_rel),
            log_files: Some(vec![log_name]),
            prev_commit: Some("null".to_string()),
            num_writes: Some(entries.len() as i64),
            total_write_bytes: Some(log_size),
            file_size_in_bytes: Some(log_size),
            partition_path: Some(partition_name.to_string()),
            ..Default::default()
        });
    }
    Ok(stats)
}

/// Write the MDT deltacommit for one data instant: requested + inflight fencing
/// then a completed `{instant}_{completion}.deltacommit` (Java MOR MDT).
///
/// One deltacommit per instant — callers collect write stats from all touched
/// MDT partitions and commit once, like Java `commitInternal`. Completed bytes
/// must be valid Avro `HoodieCommitMetadata` — Spark skips instants whose
/// metadata does not parse, which drops MDT log files from the file slice.
pub(crate) async fn write_metadata_commit(
    storage: &Storage,
    instant: &str,
    partition_to_write_stats: HashMap<String, Vec<HoodieWriteStat>>,
) -> Result<()> {
    if partition_to_write_stats.values().all(|s| s.is_empty()) {
        return Ok(());
    }
    let timeline = format!("{METADATA_BASE}/.hoodie/timeline");
    crate::write::fence_timeline_instant(
        storage,
        &timeline,
        instant,
        Action::DeltaCommit,
        Vec::new(),
        crate::write::inflight_commit_metadata_bytes("UPSERT", true)?,
    )
    .await?;
    let metadata = HoodieCommitMetadata {
        version: Some(1),
        operation_type: Some("UPSERT".to_string()),
        partition_to_write_stats: Some(partition_to_write_stats),
        compacted: Some(false),
        extra_metadata: Some(HashMap::new()),
    };
    // Completion time is minted at completion, after the requested time (Java
    // generates it through the same monotonic skew-adjusting time generator).
    let completion = crate::write::append::generate_instant_time().await;
    let completed_path = format!("{timeline}/{instant}_{completion}.deltacommit");
    storage
        .put_file_if_absent(&completed_path, metadata.to_avro_bytes()?)
        .await?;
    Ok(())
}

/// Convert a timeline instant to epoch millis for the record index.
///
/// The zone is the table's `hoodie.table.timeline.timezone`, matching Java's
/// HoodieInstantTimeGenerator: parsing a LOCAL-zone table's instants as UTC
/// writes an offset instantTime into the index.
pub(crate) fn instant_to_epoch_millis_in(instant: &str, timezone: &str) -> i64 {
    Instant::parse_datetime(instant, timezone)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or_else(|_| {
            debug_assert!(
                instant.parse::<i64>().is_ok(),
                "unparseable instant '{instant}'"
            );
            instant.parse::<i64>().unwrap_or(0)
        })
}

/// The zone the table's instants are minted in: `hoodie.table.timeline.timezone`,
/// or LOCAL when the table does not set it, as Java's `HoodieTableConfig`
/// defaults it (Spark does not persist the property).
pub(crate) fn timeline_timezone(hudi_configs: &crate::config::HudiConfigs) -> String {
    use crate::config::table::{HudiTableConfig::TimelineTimezone, TimelineTimezoneValue};
    if !hudi_configs.contains(TimelineTimezone) {
        return TimelineTimezoneValue::Local.as_ref().to_string();
    }
    hudi_configs.get_or_default(TimelineTimezone).into()
}

pub(crate) fn instant_to_epoch_millis(instant: &str) -> i64 {
    instant_to_epoch_millis_in(instant, "UTC")
}

/// Format epoch millis as a Hudi timeline instant (`yyyyMMddHHmmssSSS`).
pub fn epoch_millis_to_instant(millis: i64) -> String {
    use chrono::{TimeZone, Utc};
    match Utc.timestamp_millis_opt(millis) {
        chrono::LocalResult::Single(dt) => dt.format("%Y%m%d%H%M%S%3f").to_string(),
        _ => format!("{millis:017}"),
    }
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff_u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xedb8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fabricate an MDT partition dir plus a timeline whose completed
    /// instants are `committed`; base admission depends on the latter.
    async fn slices_for_with_timeline(
        files: &[&str],
        committed: &[&str],
    ) -> Result<Vec<(String, Option<String>, Vec<String>)>> {
        let dir = tempfile::tempdir().unwrap();
        let partition_dir = dir.path().join(".hoodie/metadata/column_stats");
        std::fs::create_dir_all(&partition_dir).unwrap();
        for name in files {
            std::fs::write(partition_dir.join(name), b"").unwrap();
        }
        let timeline_dir = dir.path().join(".hoodie/metadata/.hoodie/timeline");
        std::fs::create_dir_all(&timeline_dir).unwrap();
        for instant in committed {
            std::fs::write(
                timeline_dir.join(format!("{instant}_{instant}.deltacommit")),
                b"{}",
            )
            .unwrap();
        }
        let base_url = url::Url::from_directory_path(dir.path()).unwrap();
        let storage = Storage::new_with_base_url(base_url)?;
        mdt_partition_latest_slices(storage.as_ref(), "column_stats").await
    }

    async fn slices_for(files: &[&str]) -> Result<Vec<(String, Option<String>, Vec<String>)>> {
        // All referenced instants committed.
        slices_for_with_timeline(
            files,
            &[
                "00000000000000010",
                "00000000000000020",
                "00000000000000030",
                "00000000000000040",
            ],
        )
        .await
    }

    #[tokio::test]
    async fn test_mdt_latest_slice_multi_base_excludes_pre_compaction_logs() {
        // Post-compaction layout: bootstrap base + old logs, then a compacted
        // base at t=30. Only the newer base and the post-compaction log form
        // the live slice.
        let slices = slices_for(&[
            "col-stats-0000-0_0-0-0_00000000000000010.hfile",
            ".col-stats-0000-0_00000000000000020.log.1_0-0-0",
            "col-stats-0000-0_0-1-0_00000000000000030.hfile",
            ".col-stats-0000-0_00000000000000040.log.1_0-0-0",
            ".hoodie_partition_metadata",
        ])
        .await
        .unwrap();
        assert_eq!(slices.len(), 1);
        let (file_id, base, logs) = &slices[0];
        assert_eq!(file_id, "col-stats-0000-0");
        assert!(
            base.as_ref()
                .unwrap()
                .ends_with("col-stats-0000-0_0-1-0_00000000000000030.hfile")
        );
        assert_eq!(logs.len(), 1);
        assert!(logs[0].ends_with(".col-stats-0000-0_00000000000000040.log.1_0-0-0"));
    }

    #[tokio::test]
    async fn test_mdt_latest_slice_admits_archived_base_without_reading_history() {
        // The bootstrap base's instant (t=10) is archived off the MDT timeline,
        // which now starts at t=20, and the history cannot be parsed. The base
        // sits below the archival boundary, so it is committed; the t=40 base
        // has no completed instant and stays fenced.
        let dir = tempfile::tempdir().unwrap();
        let partition_dir = dir.path().join(".hoodie/metadata/column_stats");
        std::fs::create_dir_all(&partition_dir).unwrap();
        for name in [
            "col-stats-0000-0_0-0-0_00000000000000010.hfile",
            ".col-stats-0000-0_00000000000000020.log.1_0-0-0",
            "col-stats-0000-0_0-1-0_00000000000000040.hfile",
        ] {
            std::fs::write(partition_dir.join(name), b"").unwrap();
        }
        let timeline_dir = dir.path().join(".hoodie/metadata/.hoodie/timeline");
        let history_dir = timeline_dir.join("history");
        std::fs::create_dir_all(&history_dir).unwrap();
        std::fs::write(
            timeline_dir.join("00000000000000020_00000000000000021.deltacommit"),
            b"{}",
        )
        .unwrap();
        std::fs::write(timeline_dir.join("00000000000000040.commit.requested"), b"").unwrap();
        std::fs::write(history_dir.join("_version_"), b"1").unwrap();
        std::fs::write(
            history_dir.join("manifest_1"),
            br#"{"files":[{"fileName":"00000000000000010_00000000000000010_0.parquet","fileLen":3}]}"#,
        )
        .unwrap();
        std::fs::write(
            history_dir.join("00000000000000010_00000000000000010_0.parquet"),
            b"bad",
        )
        .unwrap();
        let base_url = url::Url::from_directory_path(dir.path()).unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();

        let slices = mdt_partition_latest_slices(storage.as_ref(), "column_stats")
            .await
            .unwrap();
        assert_eq!(slices.len(), 1);
        let (_, base, logs) = &slices[0];
        assert!(
            base.as_ref()
                .unwrap()
                .ends_with("col-stats-0000-0_0-0-0_00000000000000010.hfile")
        );
        assert_eq!(logs.len(), 1);
    }

    #[test]
    fn test_global_record_index_file_id() {
        assert!(is_global_record_index_file_id("record-index-0000-0"));
        assert!(is_global_record_index_file_id("record-index-0012-0"));
        for partitioned in [
            "record-index-sf-0000-0",
            "record-index-2024-0000-0",
            "record-index-2024-01-01-0003-0",
        ] {
            assert!(
                !is_global_record_index_file_id(partitioned),
                "{partitioned}"
            );
        }
    }

    #[tokio::test]
    async fn test_mdt_latest_slice_orders_logs_numerically_past_ten() {
        // Log versions 1..=12 must come back in numeric order, not the
        // lexicographic order a raw listing sort would produce (1, 10, 11, ...).
        let names: Vec<String> = (1..=12)
            .map(|v| format!(".col-stats-0000-0_00000000000000020.log.{v}_0-0-0"))
            .collect();
        let mut files: Vec<&str> = names.iter().map(String::as_str).collect();
        files.push("col-stats-0000-0_0-0-0_00000000000000010.hfile");
        let slices = slices_for(&files).await.unwrap();
        assert_eq!(slices.len(), 1);
        let logs = &slices[0].2;
        assert_eq!(logs.len(), 12);
        for (i, log) in logs.iter().enumerate() {
            assert!(
                log.ends_with(&format!(".log.{}_0-0-0", i + 1)),
                "log {i} out of order: {log}"
            );
        }
    }

    #[tokio::test]
    async fn test_detect_num_file_groups_ignores_uncommitted_shards() {
        // An orphan HFile from a crashed writer must not change the shard
        // modulus; only committed shards count, and an empty partition falls
        // back to the caller's default.
        let dir = tempfile::tempdir().unwrap();
        let partition_dir = dir.path().join(".hoodie/metadata/record_index");
        std::fs::create_dir_all(&partition_dir).unwrap();
        for name in [
            "record-index-0000-0_0-0-0_00000000000000010.hfile",
            "record-index-0001-0_0-0-0_00000000000000010.hfile",
            "record-index-0002-0_0-0-0_00000000000000099.hfile", // orphan
        ] {
            std::fs::write(partition_dir.join(name), b"").unwrap();
        }
        let timeline_dir = dir.path().join(".hoodie/metadata/.hoodie/timeline");
        std::fs::create_dir_all(&timeline_dir).unwrap();
        std::fs::write(
            timeline_dir.join("00000000000000010_00000000000000010.deltacommit"),
            b"{}",
        )
        .unwrap();
        let base_url = url::Url::from_directory_path(dir.path()).unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let shards = detect_num_file_groups(
            storage.as_ref(),
            MetadataPartitionType::RecordIndex.partition_name(),
            "record-index-",
            10,
        )
        .await
        .unwrap();
        assert_eq!(shards, 2, "orphan shard must not count");

        let empty = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(empty.path().join(".hoodie")).unwrap();
        let base_url = url::Url::from_directory_path(empty.path()).unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let shards = detect_num_file_groups(
            storage.as_ref(),
            MetadataPartitionType::RecordIndex.partition_name(),
            "record-index-",
            10,
        )
        .await
        .unwrap();
        assert_eq!(shards, 10, "missing partition falls back to default");
    }

    #[tokio::test]
    async fn test_mdt_latest_slice_ignores_uncommitted_compaction_base() {
        // A compaction HFile whose MDT .commit never completed (crash between
        // file write and commit) must not shadow the committed slice: the
        // older committed base stays active WITH its logs.
        let slices = slices_for_with_timeline(
            &[
                "col-stats-0000-0_0-0-0_00000000000000010.hfile",
                ".col-stats-0000-0_00000000000000020.log.1_0-0-0",
                "col-stats-0000-0_0-1-0_00000000000000030.hfile",
            ],
            &["00000000000000010", "00000000000000020"],
        )
        .await
        .unwrap();
        assert_eq!(slices.len(), 1);
        let (_, base, logs) = &slices[0];
        assert!(
            base.as_ref()
                .unwrap()
                .ends_with("col-stats-0000-0_0-0-0_00000000000000010.hfile"),
            "committed base must stay active, got {base:?}"
        );
        assert_eq!(logs.len(), 1);
    }

    #[tokio::test]
    async fn test_mdt_latest_slice_log_only_group_and_missing_partition() {
        // Spark lays out column_stats as log-only file groups: every log of
        // such a group is live, in log order.
        let slices = slices_for(&[
            ".col-stats-0000-0_00000000000000020.log.1_0-0-0",
            ".col-stats-0000-0_00000000000000001.log.1_0-0-0",
            ".col-stats-0001-0_00000000000000001.log.1_0-0-0",
        ])
        .await
        .unwrap();
        assert_eq!(slices.len(), 2);
        let (file_id, base, logs) = &slices[0];
        assert_eq!(file_id, "col-stats-0000-0");
        assert!(base.is_none());
        assert_eq!(logs.len(), 2);
        assert!(logs[0].ends_with(".col-stats-0000-0_00000000000000001.log.1_0-0-0"));
        assert!(logs[1].ends_with(".col-stats-0000-0_00000000000000020.log.1_0-0-0"));
        assert_eq!(slices[1].0, "col-stats-0001-0");

        // Missing partition directory is an empty result, not an error.
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".hoodie")).unwrap();
        let base_url = url::Url::from_directory_path(dir.path()).unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let slices = mdt_partition_latest_slices(storage.as_ref(), "column_stats")
            .await
            .unwrap();
        assert!(slices.is_empty());
    }

    fn id_value_batch(rows: &[(&str, i64)]) -> arrow_array::RecordBatch {
        use arrow_array::{Int64Array, StringArray};
        let schema = std::sync::Arc::new(Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("value", arrow_schema::DataType::Int64, false),
        ]));
        arrow_array::RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(StringArray::from(
                    rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                )),
                std::sync::Arc::new(Int64Array::from(
                    rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_record_index_with_delete_only_log_after_compaction() {
        use crate::config::table::TableTypeValue;
        use crate::index::{HoodieIndex, HoodieKey, RecordIndex};

        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_table_type(TableTypeValue::MergeOnRead)
            .with_record_key_fields(["id"])
            .create()
            .await
            .unwrap();
        table
            .append([id_value_batch(&[("a", 1), ("b", 2)])])
            .await
            .unwrap();
        let key = |k: &str| HoodieKey {
            record_key: k.to_string(),
            partition_path: String::new(),
        };

        // Compact the record index the way a Spark MDT compaction does: every
        // shard gets a committed base holding its entries, so the delete below
        // is the only live log in the partition.
        let compaction_instant = crate::write::append::generate_instant_time().await;
        let mdt = dir.path().join(METADATA_BASE);
        let schema_json = record_index_metadata_avro_schema_json()
            .unwrap()
            .to_string();
        let locations = RecordIndex
            .tag_location(&table, &[key("a"), key("b")])
            .await
            .unwrap();
        for record_key in ["a", "b"] {
            let location = locations[&key(record_key)].clone().unwrap();
            let entry = RecordIndexEntry {
                record_key: record_key.to_string(),
                partition_path: location.partition_path,
                file_id: location.file_id,
                instant_time_millis: instant_to_epoch_millis(&location.instant_time),
                is_deleted: false,
            };
            let base = HFileWriter::write(
                &[(
                    record_key.to_string(),
                    encode_record_index_entry(&entry).unwrap(),
                )],
                BTreeMap::from([("schema".to_string(), schema_json.clone().into_bytes())]),
            )
            .unwrap();
            let shard = map_record_key_to_file_group_index(record_key, DEFAULT_RLI_NUM_FILE_GROUPS);
            std::fs::write(
                mdt.join(format!(
                    "record_index/{}_0-0-0_{compaction_instant}.hfile",
                    record_index_file_id(shard)
                )),
                base,
            )
            .unwrap();
        }
        let timeline = mdt.join(".hoodie/timeline");
        let latest_deltacommit = std::fs::read_dir(&timeline)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.to_string_lossy().ends_with(".deltacommit"))
            .max()
            .unwrap();
        std::fs::copy(
            latest_deltacommit,
            timeline.join(format!("{compaction_instant}_{compaction_instant}.commit")),
        )
        .unwrap();

        table.delete_keys([key("a")]).await.unwrap();
        table.upsert([id_value_batch(&[("b", 20)])]).await.unwrap();

        let tagged = RecordIndex
            .tag_location(&table, &[key("a"), key("b")])
            .await
            .unwrap();
        assert!(tagged[&key("a")].is_none(), "deleted key must stay deleted");
        assert!(tagged[&key("b")].is_some());
    }
}
