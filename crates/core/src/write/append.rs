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

use std::collections::HashMap;
use std::io::Cursor;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use arrow::array::{ArrayRef, StringArray, UInt32Array};
use arrow::record_batch::RecordBatch;
use arrow_select::take::take;
use chrono::Utc;
use parquet::arrow::ArrowWriter;

use crate::Result;
use crate::config::table::HudiTableConfig::{
    PopulatesMetaFields, RecordMergeStrategy, TableType, TableVersion, TimelineLayoutVersion,
    TimelinePath,
};
use crate::config::table::TableTypeValue;
use crate::error::CoreError;
use crate::index::is_record_index_enabled;
use crate::merge::RecordMergeStrategyValue;
use crate::metadata::HUDI_METADATA_DIR;
use crate::metadata::commit::{HoodieCommitMetadata, HoodieWriteStat};
use crate::metadata::meta_field::MetaField;
use crate::metadata::table::encode::RecordIndexEntry;
use crate::table::Table;
use crate::timeline::instant::{Action, Instant};
use crate::write::keygen::{
    hoodie_keys_for_batch, hoodie_keys_for_batch_with_offset, relative_data_path,
};
use crate::write::metadata::{
    instant_to_epoch_millis, is_column_stats_enabled, update_column_stats_partitions,
    update_files_partition, update_record_index,
};

/// Result of an append write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendResult {
    pub instant: String,
    pub commit_relative_path: String,
    pub base_file_path: String,
    pub num_rows: usize,
}

static LAST_EPOCH_MILLIS: AtomicI64 = AtomicI64::new(0);

/// Append one or more record batches as a single INSERT commit.
pub async fn append_batches(table: &mut Table, batches: &[RecordBatch]) -> Result<AppendResult> {
    append_batches_inner(table, batches, false).await
}

/// Append one or more record batches, requiring append-only merge mode.
pub async fn append_batches_only(
    table: &mut Table,
    batches: &[RecordBatch],
) -> Result<AppendResult> {
    append_batches_inner(table, batches, true).await
}

async fn append_batches_inner(
    table: &mut Table,
    batches: &[RecordBatch],
    require_append_only: bool,
) -> Result<AppendResult> {
    table.reload_timeline_for_write().await?;
    if !table.is_mor() {
        if require_append_only {
            ensure_append_only_strategy(table)?;
        } else {
            ensure_insert_allowed(table)?;
        }
    }
    ensure_supported_table_type(table)?;

    if batches.is_empty() {
        return Err(CoreError::Write(
            "append requires at least one RecordBatch".to_string(),
        ));
    }
    let schema = batches[0].schema();
    for batch in batches.iter().skip(1) {
        if batch.schema().as_ref() != schema.as_ref() {
            return Err(CoreError::Write(
                "All RecordBatches in append must share the same schema".to_string(),
            ));
        }
    }
    ensure_append_schema_matches_table(table, schema.as_ref()).await?;
    let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if num_rows == 0 {
        return Err(CoreError::Write(
            "append requires at least one row".to_string(),
        ));
    }

    crate::write::keygen::validate_keygen_inputs(&table.hudi_configs, batches)?;

    let layout_two = is_layout_two(table);
    let action = if table.is_mor() {
        Action::DeltaCommit
    } else {
        Action::Commit
    };
    let timeline_dir = timeline_dir(table);
    let storage = table.file_system_view.storage.clone();
    let lock = crate::write::lock::lock_provider_for(table);
    // Critical section 1: mint the instant and fence it, then work unlocked.
    let cs1 = lock.lock().await?;
    let request_instant = generate_instant_time().await;
    crate::write::fence_timeline_instant(
        storage.as_ref(),
        &timeline_dir,
        &request_instant,
        action.clone(),
        Vec::new(),
        crate::write::inflight_commit_metadata_bytes("INSERT", layout_two)?,
    )
    .await?;
    drop(cs1);

    // Group rows across batches by partition path and write one base file per partition.
    let mut partition_rows: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
    for (batch_idx, batch) in batches.iter().enumerate() {
        let keys = hoodie_keys_for_batch(&table.hudi_configs, batch, Some(&request_instant))?;
        for (row_idx, key) in keys.iter().enumerate() {
            partition_rows
                .entry(key.partition_path.clone())
                .or_default()
                .push((batch_idx, row_idx));
        }
    }

    let mut write_stats: HashMap<String, Vec<HoodieWriteStat>> = HashMap::new();
    let mut files_mdt = Vec::new();
    let mut rli_entries = Vec::new();
    let mut stats_updates = Vec::new();
    let mut written_paths: Vec<String> = Vec::new();
    let mut primary_base_path = String::new();
    let max_records = max_records_per_file(table);
    // Monotonic auto-key row id across partitions and size-split chunks in this commit.
    let mut auto_key_row_offset = 0usize;
    let mut auto_key_partition_id = 0u32;

    // Stable partition iteration so partition_id assignment is deterministic.
    let mut partition_rows_sorted: Vec<_> = partition_rows.into_iter().collect();
    partition_rows_sorted.sort_by(|a, b| a.0.cmp(&b.0));

    // Plan every output file name up front and write markers before any data
    // file is put — rollback of a crashed write is driven by these markers.
    let mut file_plan: HashMap<String, std::collections::VecDeque<(String, String)>> =
        HashMap::new();
    let mut planned_markers = Vec::new();
    for (partition_path, row_refs) in &partition_rows_sorted {
        let chunks = row_refs.len().div_ceil(max_records).max(1);
        let entries = file_plan.entry(partition_path.clone()).or_default();
        for _ in 0..chunks {
            let file_id = crate::write::new_file_id();
            let file_name = format!("{file_id}_0-0-0_{request_instant}.parquet");
            planned_markers.push(crate::write::markers::Marker::create(
                partition_path,
                &file_name,
            ));
            entries.push_back((file_id, file_name));
        }
    }
    crate::write::markers::write_markers(storage.as_ref(), &request_instant, &planned_markers)
        .await?;

    // Phase 1 (sequential): slice + prepare every chunk, keeping table borrows
    // out of the parallel phase.
    struct ChunkPlan {
        partition_path: String,
        file_id: String,
        file_name: String,
        base_file_path: String,
        row_count: i64,
        chunk_keys: Vec<crate::index::HoodieKey>,
        prepared: Vec<RecordBatch>,
    }
    let mut chunk_plans: Vec<ChunkPlan> = Vec::new();
    for (partition_path, row_refs) in partition_rows_sorted {
        // BTreeMap: deterministic row order in the written file.
        let batch_indices: std::collections::BTreeMap<usize, Vec<u32>> = {
            let mut map: std::collections::BTreeMap<usize, Vec<u32>> =
                std::collections::BTreeMap::new();
            for (batch_idx, row_idx) in &row_refs {
                map.entry(*batch_idx).or_default().push(*row_idx as u32);
            }
            map
        };
        let mut partition_batches = Vec::new();
        for (batch_idx, rows) in batch_indices {
            partition_batches.push(take_rows(&batches[batch_idx], &rows)?);
        }
        let partition_batch = if partition_batches.len() == 1 {
            partition_batches.pop().unwrap()
        } else {
            arrow::compute::concat_batches(&schema, &partition_batches)?
        };
        crate::write::ensure_partition_metadata(
            storage.as_ref(),
            &partition_path,
            &request_instant,
        )
        .await?;

        // Split oversized partitions into multiple base files (Java max.file.size).
        let total_rows = partition_batch.num_rows();
        let mut offset = 0usize;
        while offset < total_rows {
            let end = (offset + max_records).min(total_rows);
            let indices: Vec<u32> = (offset as u32..end as u32).collect();
            let chunk = take_rows(&partition_batch, &indices)?;
            let chunk_row_offset = auto_key_row_offset;
            let chunk_keys = hoodie_keys_for_batch_with_offset(
                &table.hudi_configs,
                &chunk,
                Some(&request_instant),
                chunk_row_offset,
                auto_key_partition_id,
            )?;
            auto_key_row_offset += chunk.num_rows();
            let (file_id, file_name) = file_plan
                .get_mut(&partition_path)
                .and_then(std::collections::VecDeque::pop_front)
                .ok_or_else(|| {
                    CoreError::Write(format!(
                        "file plan exhausted for partition '{partition_path}'"
                    ))
                })?;
            let base_file_path = relative_data_path(&partition_path, &file_name);
            if primary_base_path.is_empty() {
                primary_base_path = base_file_path.clone();
            }
            let prepared = prepare_batches_for_write_with_offset(
                &table.hudi_configs,
                &[chunk],
                &request_instant,
                &file_name,
                chunk_row_offset,
                auto_key_partition_id,
            )?;
            chunk_plans.push(ChunkPlan {
                partition_path: partition_path.clone(),
                file_id,
                file_name,
                base_file_path,
                row_count: indices.len() as i64,
                chunk_keys,
                prepared,
            });
            offset = end;
        }
        auto_key_partition_id = auto_key_partition_id.saturating_add(1);
    }

    // Phase 2 (parallel): encode + put each file on the write task pool.
    let props = parquet_writer_props(table);
    let collect_ranges = is_column_stats_enabled(table);
    let tasks = chunk_plans
        .iter()
        .map(|plan| {
            crate::write::parquet_file_task(
                storage.clone(),
                props.clone(),
                plan.prepared.clone(),
                plan.base_file_path.clone(),
                collect_ranges,
            )
        })
        .collect();
    let results =
        crate::write::run_write_tasks(tasks, crate::write::write_task_parallelism(table)).await;

    // Phase 3 (sequential): assemble outputs in plan order; clean up on error.
    let mut first_error = None;
    for (plan, result) in chunk_plans.iter().zip(results) {
        match result {
            Ok(output) => {
                written_paths.push(plan.base_file_path.clone());
                if let Some(ranges) = output.ranges {
                    stats_updates.push(crate::write::metadata::StatsFileUpdate {
                        partition_path: plan.partition_path.clone(),
                        file_name: plan.file_name.clone(),
                        is_deleted: false,
                        ranges,
                    });
                }
                write_stats
                    .entry(plan.partition_path.clone())
                    .or_default()
                    .push(HoodieWriteStat {
                        file_id: Some(plan.file_id.clone()),
                        path: Some(plan.base_file_path.clone()),
                        // Basename only — FileGroup builder joins partition_path + name.
                        base_file: Some(plan.file_name.clone()),
                        prev_commit: Some("null".to_string()),
                        num_writes: Some(plan.row_count),
                        num_inserts: Some(plan.row_count),
                        total_write_bytes: Some(output.size),
                        file_size_in_bytes: Some(output.size),
                        partition_path: Some(plan.partition_path.clone()),
                        ..Default::default()
                    });
                files_mdt.push((
                    plan.partition_path.clone(),
                    plan.file_name.clone(),
                    output.size,
                ));
                for key in &plan.chunk_keys {
                    rli_entries.push(RecordIndexEntry {
                        record_key: key.record_key.clone(),
                        partition_path: plan.partition_path.clone(),
                        file_id: plan.file_id.clone(),
                        instant_time_millis: instant_to_epoch_millis(&request_instant),
                        is_deleted: false,
                    });
                }
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    if let Some(error) = first_error {
        for path in &written_paths {
            let _ = storage.delete_file(path).await;
        }
        return Err(error);
    }

    let commit_metadata = HoodieCommitMetadata {
        version: Some(1),
        operation_type: Some("INSERT".to_string()),
        partition_to_write_stats: Some(write_stats),
        compacted: Some(false),
        // Java HoodieCommitMetadata.extraMetadata is non-null; null → NPE in Spark.
        extra_metadata: Some(HashMap::from([(
            "schema".to_string(),
            arrow_schema_to_avro_json(&schema)?,
        )])),
    };
    let commit_bytes = if layout_two {
        commit_metadata.to_avro_bytes()?
    } else {
        commit_metadata.to_json_bytes()?
    };

    // Crash-consistency order: data files → MDT files → MDT deltacommit →
    // completed data commit. Finalizing the data timeline last means readers
    // fence MDT contents by completed data instants. MDT log-file writes are
    // "work" (unlocked); only completion runs in critical section 2.
    let mut mdt_stats: HashMap<String, Vec<HoodieWriteStat>> = HashMap::new();
    if table.is_metadata_table_enabled() {
        let mdt_result = async {
            mdt_stats.insert(
                "files".to_string(),
                update_files_partition(storage.as_ref(), &request_instant, &files_mdt).await?,
            );
            if is_record_index_enabled(table) {
                let stats =
                    update_record_index(storage.as_ref(), &request_instant, &rli_entries).await?;
                if !stats.is_empty() {
                    mdt_stats.insert("record_index".to_string(), stats);
                }
            }
            if is_column_stats_enabled(table) && !stats_updates.is_empty() {
                let stats = update_column_stats_partitions(
                    table,
                    &request_instant,
                    &stats_updates,
                    schema.as_ref(),
                    &[],
                )
                .await?;
                mdt_stats.extend(stats);
            }
            Ok::<(), crate::error::CoreError>(())
        }
        .await;
        if let Err(error) = mdt_result {
            for path in &written_paths {
                let _ = storage.delete_file(path).await;
            }
            return Err(error);
        }
    }

    // Critical section 2: complete the action (MDT deltacommit + data commit
    // with a completion time minted under the lock), then bookkeeping.
    let cs2 = lock.lock().await?;
    if table.is_metadata_table_enabled()
        && let Err(error) = crate::write::metadata::write_metadata_commit(
            storage.as_ref(),
            &request_instant,
            mdt_stats,
        )
        .await
    {
        for path in &written_paths {
            let _ = storage.delete_file(path).await;
        }
        return Err(error);
    }
    let completion = if layout_two {
        Some(generate_instant_time().await)
    } else {
        None
    };
    let instant = Instant::new_completed(request_instant.clone(), action, completion)?;
    let commit_relative_path = instant.relative_path_with_base(&timeline_dir)?;
    if let Err(error) = storage
        .put_file_if_absent(&commit_relative_path, commit_bytes)
        .await
    {
        for path in &written_paths {
            let _ = storage.delete_file(path).await;
        }
        return Err(error.into());
    }
    crate::write::post_complete_bookkeeping(table, storage.as_ref(), &request_instant).await?;
    drop(cs2);

    table.timeline.reload_completed_commits().await?;
    table.file_system_view.clear_cache();

    Ok(AppendResult {
        instant: request_instant,
        commit_relative_path,
        base_file_path: primary_base_path,
        num_rows,
    })
}

fn take_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
    let indices = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(Into::into)
}

fn ensure_insert_allowed(table: &Table) -> Result<()> {
    let strategy: String = table
        .hudi_configs
        .get_or_default(RecordMergeStrategy)
        .into();
    // Insert/append is valid for both append-only and upsertable (commit/event-time) tables.
    match RecordMergeStrategyValue::from_str(&strategy)? {
        RecordMergeStrategyValue::AppendOnly | RecordMergeStrategyValue::OverwriteWithLatest => {
            Ok(())
        }
    }
}

fn ensure_append_only_strategy(table: &Table) -> Result<()> {
    let strategy: String = table
        .hudi_configs
        .get_or_default(RecordMergeStrategy)
        .into();
    if RecordMergeStrategyValue::from_str(&strategy)? != RecordMergeStrategyValue::AppendOnly {
        return Err(CoreError::Unsupported(
            "append_only requires hoodie.record.merge.mode=append_only".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn ensure_copy_on_write(table: &Table) -> Result<()> {
    let table_type: String = table.hudi_configs.get(TableType)?.into();
    if TableTypeValue::from_str(&table_type)? != TableTypeValue::CopyOnWrite {
        return Err(CoreError::Unsupported(format!(
            "append is currently only supported for COPY_ON_WRITE tables. Found '{table_type}'"
        )));
    }
    Ok(())
}

fn ensure_supported_table_type(table: &Table) -> Result<()> {
    let table_type: String = table.hudi_configs.get(TableType)?.into();
    match TableTypeValue::from_str(&table_type)? {
        TableTypeValue::CopyOnWrite | TableTypeValue::MergeOnRead => Ok(()),
    }
}

pub(crate) fn is_layout_two(table: &Table) -> bool {
    let table_version: isize = match table.hudi_configs.get(TableVersion) {
        Ok(v) => v.into(),
        Err(_) => return false,
    };
    let layout_version: isize = match table.hudi_configs.get(TimelineLayoutVersion) {
        Ok(v) => v.into(),
        Err(_) => return false,
    };
    table_version >= 8 && layout_version == 2
}

pub(crate) fn timeline_dir(table: &Table) -> String {
    if is_layout_two(table) {
        let timeline_path: String = table.hudi_configs.get_or_default(TimelinePath).into();
        format!("{HUDI_METADATA_DIR}/{timeline_path}")
    } else {
        HUDI_METADATA_DIR.to_string()
    }
}

/// Java `HoodieTimeGeneratorConfig` infers a 1 ms max expected clock skew when
/// the (default) `InProcessLockProvider` is used — the single-writer case.
const MAX_EXPECTED_CLOCK_SKEW_MS: u64 = 1;

/// Skew-adjusting monotonic instant time generator (Java
/// `SkewAdjustingTimeGenerator` + `HoodieInstantTimeGenerator.createNewInstantTime`).
///
/// Under a process-wide lock: capture the clock, wait out the max expected
/// clock skew, and return the captured time — retrying (never bumping +1) until
/// it exceeds every previously minted instant. Used for both requested and
/// completion timestamps, so completion times are strictly greater than the
/// requested times minted before them. Format `yyyyMMddHHmmssSSS` (UTC).
/// Whether instants are formatted in local time (Java's
/// `HoodieTimelineTimeZone.LOCAL`, the Hudi default) or UTC. Process-wide,
/// mirroring `HoodieInstantTimeGenerator.setCommitTimeZone`; set from the
/// table's `hoodie.table.timeline.timezone` when a table is created or
/// loaded for writes. Spark writers mint LOCAL-time instants by default, so
/// matching the table's declared zone keeps mixed-writer timelines ordered.
static COMMIT_TIME_IS_LOCAL: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

pub(crate) fn set_commit_timezone(timezone: &str) {
    COMMIT_TIME_IS_LOCAL.store(
        !timezone.eq_ignore_ascii_case("utc"),
        std::sync::atomic::Ordering::Release,
    );
}

pub(crate) async fn generate_instant_time() -> String {
    static TIME_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = TIME_LOCK.lock().await;
    loop {
        let now = Utc::now();
        tokio::time::sleep(std::time::Duration::from_millis(MAX_EXPECTED_CLOCK_SKEW_MS)).await;
        let candidate_millis = now.timestamp_millis();
        // Store is race-free: all mints serialize through TIME_LOCK.
        if candidate_millis > LAST_EPOCH_MILLIS.load(Ordering::Acquire) {
            LAST_EPOCH_MILLIS.store(candidate_millis, Ordering::Release);
            let format = "%Y%m%d%H%M%S%3f";
            return if COMMIT_TIME_IS_LOCAL.load(std::sync::atomic::Ordering::Acquire) {
                now.with_timezone(&chrono::Local).format(format).to_string()
            } else {
                now.format(format).to_string()
            };
        }
    }
}

/// Default max base-file size (Java `hoodie.parquet.max.file.size` = 120 MiB).
pub(crate) const DEFAULT_MAX_FILE_SIZE_BYTES: i64 = 120 * 1024 * 1024;
/// Default record size estimate used for insert splits.
pub(crate) const DEFAULT_RECORD_SIZE_ESTIMATE: i64 = 1024;

pub(crate) fn max_records_per_file(table: &Table) -> usize {
    let max_file_size = table
        .hudi_configs
        .as_options()
        .get("hoodie.parquet.max.file.size")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(DEFAULT_MAX_FILE_SIZE_BYTES)
        .max(1);
    let record_size = table
        .hudi_configs
        .as_options()
        .get("hoodie.copyonwrite.record.size.estimate")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(DEFAULT_RECORD_SIZE_ESTIMATE)
        .max(1);
    ((max_file_size / record_size) as usize).max(1)
}

/// Parquet `WriterProperties` resolved from `hoodie.parquet.*` configs.
pub(crate) fn parquet_writer_props(table: &Table) -> parquet::file::properties::WriterProperties {
    use parquet::basic::{Compression, GzipLevel, ZstdLevel};
    use parquet::file::properties::WriterProperties;

    let options = table.hudi_configs.as_options();
    let page_size = options
        .get("hoodie.parquet.page.size")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1024 * 1024)
        .max(1);
    let block_size = options
        .get("hoodie.parquet.block.size")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(DEFAULT_MAX_FILE_SIZE_BYTES)
        .max(1);
    let record_size = options
        .get("hoodie.copyonwrite.record.size.estimate")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(DEFAULT_RECORD_SIZE_ESTIMATE)
        .max(1);
    // parquet-rs row-group limit is row-count; approximate Java's byte block size.
    let max_row_group_rows = ((block_size / record_size) as usize).max(1);
    let dictionary_enabled = options
        .get("hoodie.parquet.dictionary.enabled")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);
    let codec = options
        .get("hoodie.parquet.compression.codec")
        .map(|s| s.as_str())
        .unwrap_or("zstd");
    let compression = match codec.to_ascii_lowercase().as_str() {
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        "snappy" => Compression::SNAPPY,
        "gzip" | "gz" => Compression::GZIP(GzipLevel::try_new(6).unwrap_or_default()),
        "lz4" => Compression::LZ4,
        "brotli" => Compression::BROTLI(Default::default()),
        // Default zstd — intentional vs Java's gzip; honor hoodie.parquet.compression.codec.
        _ => Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()),
    };

    WriterProperties::builder()
        .set_compression(compression)
        .set_dictionary_enabled(dictionary_enabled)
        .set_data_page_size_limit(page_size)
        .set_dictionary_page_size_limit(page_size)
        .set_max_row_group_size(max_row_group_rows)
        .build()
}

/// Encode batches to parquet bytes with pre-resolved properties (safe to run
/// on a blocking thread with owned inputs).
pub(crate) fn write_parquet_bytes_with_props(
    props: parquet::file::properties::WriterProperties,
    batches: &[RecordBatch],
) -> Result<Vec<u8>> {
    let cursor = Cursor::new(Vec::new());
    let mut writer = ArrowWriter::try_new(cursor, batches[0].schema(), Some(props))?;
    for batch in batches {
        writer.write(batch)?;
    }
    let cursor = writer.into_inner()?;
    Ok(cursor.into_inner())
}

/// Arrow→Avro schema JSON for `HoodieCommitMetadata.extraMetadata.schema`.
///
/// Unmapped Arrow types error out — silent `"string"` fallback corrupts Spark's
/// TableSchemaResolver (Parquet types diverge from commit schema).
pub(crate) fn arrow_schema_to_avro_json(schema: &arrow_schema::Schema) -> Result<String> {
    let fields: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| {
            let avro_type = arrow_type_to_avro_type_json(f.data_type())?;
            let ty = if f.is_nullable() {
                format!("[\"null\",{avro_type}]")
            } else {
                avro_type
            };
            Ok(format!(
                "{{\"name\":\"{}\",\"type\":{}}}",
                f.name().replace('\"', "\\\""),
                ty
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(format!(
        "{{\"type\":\"record\",\"name\":\"hoodie_record\",\"fields\":[{}]}}",
        fields.join(",")
    ))
}

fn arrow_type_to_avro_type_json(dt: &arrow_schema::DataType) -> Result<String> {
    use arrow_schema::DataType;
    match dt {
        DataType::Boolean => Ok("\"boolean\"".to_string()),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Ok("\"int\"".to_string()),
        DataType::Int64 => Ok("\"long\"".to_string()),
        DataType::Float32 => Ok("\"float\"".to_string()),
        DataType::Float64 => Ok("\"double\"".to_string()),
        DataType::Utf8 | DataType::LargeUtf8 => {
            Ok("{\"type\":\"string\",\"avro.java.string\":\"String\"}".to_string())
        }
        DataType::Binary | DataType::LargeBinary => Ok("\"bytes\"".to_string()),
        DataType::Date32 => Ok("{\"type\":\"int\",\"logicalType\":\"date\"}".to_string()),
        DataType::Timestamp(unit, _) => {
            let logical = match unit {
                arrow_schema::TimeUnit::Millisecond => "timestamp-millis",
                arrow_schema::TimeUnit::Microsecond => "timestamp-micros",
                other => {
                    return Err(CoreError::Unsupported(format!(
                        "commit schema does not support Arrow timestamp unit {other:?}"
                    )));
                }
            };
            Ok(format!(
                "{{\"type\":\"long\",\"logicalType\":\"{logical}\"}}"
            ))
        }
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            Ok(format!(
                "{{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":{precision},\"scale\":{scale}}}"
            ))
        }
        DataType::List(field) | DataType::LargeList(field) => {
            let item = arrow_type_to_avro_type_json(field.data_type())?;
            let item = if field.is_nullable() {
                format!("[\"null\",{item}]")
            } else {
                item
            };
            Ok(format!("{{\"type\":\"array\",\"items\":{item}}}"))
        }
        DataType::Struct(fields) => {
            let nested: Vec<String> = fields
                .iter()
                .map(|f| {
                    let avro_type = arrow_type_to_avro_type_json(f.data_type())?;
                    let ty = if f.is_nullable() {
                        format!("[\"null\",{avro_type}]")
                    } else {
                        avro_type
                    };
                    Ok(format!(
                        "{{\"name\":\"{}\",\"type\":{}}}",
                        f.name().replace('\"', "\\\""),
                        ty
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(format!(
                "{{\"type\":\"record\",\"name\":\"nested_record\",\"fields\":[{}]}}",
                nested.join(",")
            ))
        }
        other => Err(CoreError::Unsupported(format!(
            "cannot map Arrow type {other:?} to Avro for commit metadata schema"
        ))),
    }
}

async fn ensure_append_schema_matches_table(
    table: &Table,
    batch_schema: &arrow_schema::Schema,
) -> Result<()> {
    if table.timeline.completed_commits.is_empty() {
        return Ok(());
    }
    let table_schema = table.get_schema().await?;
    let batch_data = strip_meta_fields_from_schema(batch_schema);
    if schemas_equal_by_fields(&table_schema, &batch_data) {
        return Ok(());
    }
    Err(CoreError::Write(format!(
        "append batch schema does not match table schema (table fields={:?}, batch fields={:?})",
        table_schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect::<Vec<_>>(),
        batch_data
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect::<Vec<_>>(),
    )))
}

pub(crate) fn strip_meta_fields_from_schema(schema: &arrow_schema::Schema) -> arrow_schema::Schema {
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .filter(|f| !f.name().starts_with("_hoodie_"))
        .cloned()
        .collect();
    arrow_schema::Schema::new(fields)
}

fn schemas_equal_by_fields(a: &arrow_schema::Schema, b: &arrow_schema::Schema) -> bool {
    if a.fields().len() != b.fields().len() {
        return false;
    }
    a.fields()
        .iter()
        .zip(b.fields().iter())
        .all(|(fa, fb)| fa.name() == fb.name() && fa.data_type() == fb.data_type())
}

pub(crate) fn prepare_batches_for_write(
    hudi_configs: &crate::config::HudiConfigs,
    batches: &[RecordBatch],
    instant: &str,
    file_name: &str,
) -> Result<Vec<RecordBatch>> {
    prepare_batches_for_write_with_offset(hudi_configs, batches, instant, file_name, 0, 0)
}

pub(crate) fn prepare_batches_for_write_with_offset(
    hudi_configs: &crate::config::HudiConfigs,
    batches: &[RecordBatch],
    instant: &str,
    file_name: &str,
    row_id_offset: usize,
    partition_id: u32,
) -> Result<Vec<RecordBatch>> {
    let populates_meta_fields: bool = hudi_configs.get_or_default(PopulatesMetaFields).into();
    if !populates_meta_fields {
        return Ok(batches.to_vec());
    }

    batches
        .iter()
        .map(|batch| {
            if batch
                .column_by_name(MetaField::RecordKey.as_ref())
                .is_some()
            {
                return Ok(batch.clone());
            }
            let keys = hoodie_keys_for_batch_with_offset(
                hudi_configs,
                batch,
                Some(instant),
                row_id_offset,
                partition_id,
            )?;
            let key_strs: Vec<Option<&str>> =
                keys.iter().map(|k| Some(k.record_key.as_str())).collect();
            let partition_strs: Vec<Option<&str>> = keys
                .iter()
                .map(|k| Some(k.partition_path.as_str()))
                .collect();
            let rows = batch.num_rows();
            let mut fields = MetaField::schema().fields().to_vec();
            fields.extend(batch.schema().fields().iter().cloned());
            let mut columns: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(vec![instant; rows])),
                Arc::new(StringArray::from(
                    (0..rows)
                        .map(|row| format!("{instant}_{partition_id}-{}-0", row_id_offset + row))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(key_strs)),
                Arc::new(StringArray::from(partition_strs)),
                Arc::new(StringArray::from(vec![file_name; rows])),
            ];
            columns.extend(batch.columns().iter().cloned());
            RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)
                .map_err(CoreError::ArrowError)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    #[test]
    fn test_arrow_schema_to_avro_json_type_matrix() {
        let schema = Schema::new(vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("i8", DataType::Int8, false),
            Field::new("i16", DataType::Int16, true),
            Field::new("i32", DataType::Int32, false),
            Field::new("i64", DataType::Int64, false),
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
            Field::new("s", DataType::Utf8, true),
            Field::new("ls", DataType::LargeUtf8, false),
            Field::new("bin", DataType::Binary, false),
            Field::new("d", DataType::Date32, false),
            Field::new(
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("dec", DataType::Decimal128(10, 2), false),
            Field::new(
                "list",
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Int64,
                    true,
                ))),
                false,
            ),
        ]);
        let json = arrow_schema_to_avro_json(&schema).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let fields = value["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 15);
        let type_of = |name: &str| -> serde_json::Value {
            fields
                .iter()
                .find(|f| f["name"] == name)
                .unwrap_or_else(|| panic!("missing field {name}"))["type"]
                .clone()
        };
        assert_eq!(type_of("b"), serde_json::json!("boolean"));
        assert_eq!(type_of("i32"), serde_json::json!("int"));
        // Nullable fields become ["null", T] unions.
        assert_eq!(type_of("i16")[0], serde_json::json!("null"));
        assert_eq!(type_of("i64"), serde_json::json!("long"));
        assert_eq!(type_of("d")["logicalType"], "date");
        assert_eq!(type_of("ts_ms")["logicalType"], "timestamp-millis");
        assert_eq!(type_of("ts_us")["logicalType"], "timestamp-micros");
        assert_eq!(type_of("dec")["logicalType"], "decimal");
        assert_eq!(type_of("dec")["precision"], 10);
        assert_eq!(type_of("list")["type"], "array");
    }

    #[test]
    fn test_arrow_schema_to_avro_json_rejects_unsupported() {
        // Nanosecond timestamps have no Avro logical type in commit metadata.
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]);
        assert!(arrow_schema_to_avro_json(&schema).is_err());

        let schema = Schema::new(vec![Field::new(
            "m",
            DataType::Map(
                std::sync::Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        )]);
        // Either a mapped type or a hard error is acceptable long-term; today
        // unmapped compound types must error rather than degrade to string.
        assert!(arrow_schema_to_avro_json(&schema).is_err());
    }

    #[test]
    fn test_strip_meta_fields_from_schema() {
        let schema = Schema::new(vec![
            Field::new("_hoodie_commit_time", DataType::Utf8, true),
            Field::new("_hoodie_record_key", DataType::Utf8, true),
            Field::new("id", DataType::Utf8, false),
        ]);
        let stripped = strip_meta_fields_from_schema(&schema);
        assert_eq!(
            stripped
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            vec!["id"]
        );
    }

    fn id_batch(n: usize) -> arrow_array::RecordBatch {
        use arrow_array::{ArrayRef, Int64Array, StringArray};
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        arrow_array::RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(StringArray::from(
                    (0..n).map(|i| format!("k{i:04}")).collect::<Vec<_>>(),
                )) as ArrayRef,
                std::sync::Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
            ],
        )
        .unwrap()
    }

    /// Size-split path: a small max-file-size forces multiple base files in
    /// one append commit, with unique auto-incrementing sequence metadata.
    #[tokio::test]
    async fn test_append_size_split_multiple_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = crate::table::Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .with_option("hoodie.parquet.max.file.size", "4096")
            .with_option("hoodie.copyonwrite.record.size.estimate", "1024")
            .create()
            .await
            .unwrap();
        table.append([id_batch(64)]).await.unwrap();
        let files = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
            .count();
        assert!(files > 1, "expected a size split, got {files} file(s)");
        let rows: usize = table
            .read(&crate::table::ReadOptions::new())
            .await
            .unwrap()
            .iter()
            .map(arrow_array::RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 64);
    }

    /// Append with a batch whose schema does not match the table's: rejected.
    #[tokio::test]
    async fn test_append_schema_mismatch_rejected() {
        use arrow_array::{ArrayRef, Int64Array};
        let dir = tempfile::tempdir().unwrap();
        let mut table = crate::table::Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .create()
            .await
            .unwrap();
        table.append([id_batch(2)]).await.unwrap();
        let other = arrow_array::RecordBatch::try_new(
            std::sync::Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![std::sync::Arc::new(Int64Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        assert!(table.append([other]).await.is_err());
        // Mixed schemas within one call are also rejected.
        let other = arrow_array::RecordBatch::try_new(
            std::sync::Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![std::sync::Arc::new(Int64Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        assert!(table.append([id_batch(1), other]).await.is_err());
    }
}
