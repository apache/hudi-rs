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
    MetadataTablePartitions, PartitionFields, PopulatesMetaFields, RecordMergeStrategy, TableType,
    TableVersion, TimelineLayoutVersion, TimelinePath,
};
use crate::config::table::TableTypeValue;
use crate::error::CoreError;
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
use crate::write::metadata::is_column_stats_enabled;

static LAST_EPOCH_MILLIS: AtomicI64 = AtomicI64::new(0);

/// What an append wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendResult {
    /// Timestamp of the completed instant.
    pub instant: String,
    /// Path of the completed instant, relative to the table base.
    pub commit_relative_path: String,
    /// Path of the first base file written, relative to the table base.
    pub base_file_path: String,
    /// Rows written.
    pub num_rows: usize,
}

/// Append one or more record batches as a single INSERT commit.
pub async fn append_batches(table: &mut Table, batches: &[RecordBatch]) -> Result<AppendResult> {
    if table_version(table) < 8 {
        return append_version_6(table, batches).await;
    }
    crate::write::ensure_supported_key_generator(&table.hudi_configs)?;
    table.reload_timeline_for_write().await?;
    if !table.is_mor() {
        ensure_insert_allowed(table)?;
    }
    ensure_supported_table_type(table)?;

    let schema = ensure_batches_share_schema(batches)?;
    ensure_append_schema_matches_table(table, schema.as_ref()).await?;
    // Must be expressible in commit metadata (Avro JSON) before fencing.
    arrow_schema_to_avro_json(&strip_meta_fields_from_schema(schema.as_ref()))?;
    let num_rows = ensure_rows_present(batches)?;

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
        let partition_batch = match partition_batches.pop() {
            Some(only) if partition_batches.is_empty() => only,
            Some(last) => {
                partition_batches.push(last);
                arrow::compute::concat_batches(&schema, &partition_batches)?
            }
            None => continue,
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
                        instant_time_millis: crate::write::metadata::instant_to_epoch_millis_in(
                            &request_instant,
                            &crate::write::metadata::timeline_timezone(&table.hudi_configs),
                        ),
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

    let commit_bytes =
        crate::write::commit_metadata_bytes(table, "INSERT", write_stats, None, schema.as_ref())?;
    let commit_relative_path = crate::write::complete_write(
        table,
        &request_instant,
        action,
        commit_bytes,
        schema.as_ref(),
        crate::write::MetadataTableUpdates {
            files: files_mdt
                .into_iter()
                .map(|(partition, file_name, size)| (partition, file_name, size, false))
                .collect(),
            record_index: rli_entries,
            column_stats: stats_updates,
            replaced_paths: Vec::new(),
        },
        &written_paths,
    )
    .await?;

    Ok(AppendResult {
        instant: request_instant,
        commit_relative_path,
        base_file_path: primary_base_path,
        num_rows,
    })
}

fn take_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
    // A contiguous run is a view onto the batch; copying it would double the
    // memory of every append, which splits its input into exactly such runs.
    if let Some(first) = indices.first()
        && indices
            .windows(2)
            .all(|w| w[0].checked_add(1) == Some(w[1]))
    {
        return Ok(batch.slice(*first as usize, indices.len()));
    }
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

fn table_version(table: &Table) -> isize {
    table.hudi_configs.get(TableVersion).map_or(0, isize::from)
}

fn ensure_batches_share_schema(batches: &[RecordBatch]) -> Result<arrow_schema::SchemaRef> {
    let Some(first) = batches.first() else {
        return Err(CoreError::Write(
            "append requires at least one RecordBatch".to_string(),
        ));
    };
    let schema = first.schema();
    if batches
        .iter()
        .skip(1)
        .any(|batch| batch.schema().as_ref() != schema.as_ref())
    {
        return Err(CoreError::Write(
            "All RecordBatches in append must share the same schema".to_string(),
        ));
    }
    Ok(schema)
}

fn ensure_rows_present(batches: &[RecordBatch]) -> Result<usize> {
    let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if num_rows == 0 {
        return Err(CoreError::Write(
            "append requires at least one row".to_string(),
        ));
    }
    Ok(num_rows)
}

/// The narrow path for tables below version 8: one base file for an
/// unpartitioned append-only table with `hoodie.populate.meta.fields=false`
/// and no metadata table, in the layout 0.x readers expect.
///
/// The completed commit is the only timeline file it writes. It takes no
/// lock, fences no requested or inflight instant, rolls back no earlier
/// failed write, and writes no markers and no archival, so it does not share
/// `complete_write` with the version 8 path, and a crashed write leaves
/// nothing on the timeline for a later writer to roll back.
async fn append_version_6(table: &mut Table, batches: &[RecordBatch]) -> Result<AppendResult> {
    let schema = ensure_batches_share_schema(batches)?;
    if !table.timeline.completed_commits.is_empty() {
        ensure_batch_schema_matches(&table.get_schema().await?, schema.as_ref())?;
    }
    let num_rows = ensure_rows_present(batches)?;
    ensure_version_6_append_only(table)?;
    ensure_unpartitioned(table)?;
    ensure_no_meta_fields(table)?;
    ensure_no_metadata_table(table).await?;
    // The commit records the write schema as Avro; refuse before any file is written.
    let schema_json = arrow_schema_to_avro_json(schema.as_ref())?;

    set_commit_timezone(&crate::write::metadata::timeline_timezone(
        &table.hudi_configs,
    ));
    let instant = generate_instant_time().await;
    let file_id = format!("bulk-insert-{instant}");
    let base_file_path = format!("{file_id}_0-0-0_{instant}.parquet");
    let storage = table.file_system_view.storage.clone();
    let bytes = write_parquet_bytes_with_props(parquet_writer_props(table), batches)?;
    let file_size = bytes.len() as i64;
    storage.put_file(&base_file_path, bytes).await?;

    let write_stat = HoodieWriteStat {
        file_id: Some(file_id),
        path: Some(base_file_path.clone()),
        num_writes: Some(num_rows as i64),
        num_deletes: Some(0),
        num_update_writes: Some(0),
        num_inserts: Some(num_rows as i64),
        total_write_bytes: Some(file_size),
        total_write_errors: Some(0),
        partition_path: Some(String::new()),
        file_size_in_bytes: Some(file_size),
        num_updates: Some(0),
        ..Default::default()
    };
    let commit = HoodieCommitMetadata {
        version: Some(1),
        operation_type: Some("BULK_INSERT".to_string()),
        partition_to_write_stats: Some(HashMap::from([(String::new(), vec![write_stat])])),
        compacted: Some(false),
        // Spark reads a null extraMetadata as a null map and fails on it.
        extra_metadata: Some(HashMap::from([("schema".to_string(), schema_json)])),
    };
    let commit_relative_path = Instant::new_completed(instant.clone(), Action::Commit, None)?
        .relative_path_with_base(HUDI_METADATA_DIR)?;
    if let Err(error) = storage
        .put_file(&commit_relative_path, commit.to_json_bytes()?)
        .await
    {
        // An errored PUT may still have stored the commit, which needs the
        // base file. Without the commit the file must go: no requested
        // instant marks it pending, so it reads as committed once a later
        // commit starts the timeline after this instant.
        if let Ok(false) = storage.exists(&commit_relative_path).await {
            let _ = storage.delete_file(&base_file_path).await;
        }
        return Err(error.into());
    }
    // The commit is durable; a failed refresh must not turn it into an error.
    let _ = table.timeline.reload_completed_commits().await;
    table.clear_read_caches();
    Ok(AppendResult {
        instant,
        commit_relative_path,
        base_file_path,
        num_rows,
    })
}

fn ensure_version_6_append_only(table: &Table) -> Result<()> {
    let strategy: String = table
        .hudi_configs
        .get_or_default(RecordMergeStrategy)
        .into();
    if RecordMergeStrategyValue::from_str(&strategy)? != RecordMergeStrategyValue::AppendOnly {
        return Err(CoreError::Unsupported(
            "append below table version 8 requires hoodie.table.record.merge.strategy=append_only"
                .to_string(),
        ));
    }
    Ok(())
}

fn ensure_unpartitioned(table: &Table) -> Result<()> {
    let partition_fields: Vec<String> = table.hudi_configs.get_or_default(PartitionFields).into();
    if !partition_fields.is_empty() {
        return Err(CoreError::Unsupported(
            "append below table version 8 supports only unpartitioned append-only tables"
                .to_string(),
        ));
    }
    Ok(())
}

/// The written base file carries only the batch's columns, so a table that
/// populates the `_hoodie_*` meta fields would get a file without them.
fn ensure_no_meta_fields(table: &Table) -> Result<()> {
    let populates_meta_fields: bool = table
        .hudi_configs
        .get_or_default(PopulatesMetaFields)
        .into();
    if populates_meta_fields {
        return Err(CoreError::Unsupported(
            "append below table version 8 supports only tables with hoodie.populate.meta.fields=false"
                .to_string(),
        ));
    }
    Ok(())
}

/// The metadata table is not updated, so its file listings would miss the
/// new base file.
async fn ensure_no_metadata_table(table: &Table) -> Result<()> {
    let partitions: Vec<String> = table
        .hudi_configs
        .get_or_default(MetadataTablePartitions)
        .into();
    let has_metadata_dir = table
        .file_system_view
        .storage
        .list_dirs(Some(HUDI_METADATA_DIR))
        .await?
        .iter()
        .any(|dir| dir == "metadata");
    if partitions.iter().any(|p| !p.is_empty()) || has_metadata_dir {
        return Err(CoreError::Unsupported(
            "append below table version 8 supports only tables without a metadata table"
                .to_string(),
        ));
    }
    Ok(())
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
        .set_max_row_group_row_count(Some(max_row_group_rows))
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
        .enumerate()
        .map(|(index, f)| {
            ensure_legal_avro_name(f.name())?;
            // Field INDEX, not name: a name-derived path is not injective
            // (`a.b` and `a_b` collide), and Avro refuses a redefined record.
            let avro_type = arrow_type_to_avro_type_json_named(
                f.data_type(),
                &format!("hoodie_record_f{index}"),
            )?;
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

/// Avro field names must match [A-Za-z_][A-Za-z0-9_]*. A name that does not is
/// rejected rather than renamed: renaming would diverge from the parquet column
/// and silently mislabel the data, while an unparseable commit schema makes the
/// table unreadable by every engine (including this one).
fn ensure_legal_avro_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let legal = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if legal {
        return Ok(());
    }
    Err(CoreError::Unsupported(format!(
        "column name '{name}' is not a legal Avro name ([A-Za-z_][A-Za-z0-9_]*), \
         so it cannot be written into Hudi commit metadata"
    )))
}

/// `path` makes every nested Avro record name unique: Avro forbids redefining
/// a named type, so a fixed name breaks any schema with two structs — Java's
/// `Schema.Parser` rejects the whole commit schema.
fn arrow_type_to_avro_type_json_named(dt: &arrow_schema::DataType, path: &str) -> Result<String> {
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
        DataType::Timestamp(unit, tz) => {
            // A timezone-less Arrow timestamp is Spark's TimestampNTZType,
            // which maps to Avro local-timestamp-*; mapping it to the
            // UTC-adjusted logical type shifts values on read.
            let local = tz.is_none();
            let logical = match unit {
                arrow_schema::TimeUnit::Millisecond if local => "local-timestamp-millis",
                arrow_schema::TimeUnit::Microsecond if local => "local-timestamp-micros",
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
            let item =
                arrow_type_to_avro_type_json_named(field.data_type(), &format!("{path}_item"))?;
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
                .enumerate()
                .map(|(index, f)| {
                    ensure_legal_avro_name(f.name())?;
                    let avro_type = arrow_type_to_avro_type_json_named(
                        f.data_type(),
                        &format!("{path}f{index}"),
                    )?;
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
                "{{\"type\":\"record\",\"name\":\"{path}_record\",\"fields\":[{}]}}",
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
    ensure_batch_schema_matches(&table_schema, &strip_meta_fields_from_schema(batch_schema))
}

/// The table schema resolves from the Avro schema in commit metadata, and the
/// Arrow to Avro mapping is lossy (Int16 becomes Int32, LargeUtf8 becomes
/// Utf8), so a batch also matches when its Avro round trip equals the table.
pub(crate) fn ensure_batch_schema_matches(
    table_schema: &arrow_schema::Schema,
    batch_data: &arrow_schema::Schema,
) -> Result<()> {
    if schemas_equal_by_fields(table_schema, batch_data) {
        return Ok(());
    }
    let round_trip = arrow_schema_to_avro_json(batch_data)
        .and_then(|avro| crate::schema::resolver::avro_json_to_arrow_schema(&avro));
    if round_trip.is_ok_and(|schema| schemas_equal_by_fields(table_schema, &schema)) {
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
        .filter(|f| !crate::metadata::meta_field::MetaField::is_meta_field(f.name()))
        .cloned()
        .collect();
    arrow_schema::Schema::new(fields)
}

/// Prepend the Hudi meta fields to a data schema when absent — written files
/// always carry them, so index bookkeeping must consider them.
pub(crate) fn schema_with_meta_fields(schema: &arrow_schema::Schema) -> arrow_schema::Schema {
    // Match the meta fields by exact name: a data column may legitimately
    // begin with `_hoodie_` (Hudi's own `_hoodie_is_deleted` marker does), and
    // treating it as proof the meta fields are present would silently skip
    // them.
    let meta: std::collections::HashSet<&str> =
        crate::metadata::meta_field::MetaField::field_names()
            .into_iter()
            .collect();
    if schema
        .fields()
        .iter()
        .any(|f| meta.contains(f.name().as_str()))
    {
        return schema.clone();
    }
    let mut fields: Vec<arrow_schema::FieldRef> =
        crate::metadata::meta_field::MetaField::field_names()
            .into_iter()
            .map(|name| {
                std::sync::Arc::new(arrow_schema::Field::new(
                    name,
                    arrow_schema::DataType::Utf8,
                    true,
                ))
            })
            .collect();
    fields.extend(schema.fields().iter().cloned());
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

    // Rows are numbered across all batches of the call, so auto keys and
    // sequence numbers stay unique within the commit.
    let mut batch_offset = row_id_offset;
    batches
        .iter()
        .map(|batch| {
            let row_id_offset = batch_offset;
            batch_offset += batch.num_rows();
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
    fn test_take_rows_slices_a_contiguous_run_and_gathers_the_rest() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from_iter_values(0..10))],
        )?;
        let values = |b: &RecordBatch| -> Vec<i64> {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        };
        assert_eq!(values(&take_rows(&batch, &[3, 4, 5])?), [3, 4, 5]);
        assert_eq!(values(&take_rows(&batch, &[0])?), [0]);
        assert_eq!(values(&take_rows(&batch, &[9, 2, 2])?), [9, 2, 2]);
        assert_eq!(values(&take_rows(&batch, &[1, 3])?), [1, 3]);
        assert_eq!(take_rows(&batch, &[])?.num_rows(), 0);
        Ok(())
    }

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
        // Timezone-less == Spark TimestampNTZType == Avro local-timestamp-*.
        assert_eq!(type_of("ts_ms")["logicalType"], "local-timestamp-millis");
        assert_eq!(type_of("ts_us")["logicalType"], "timestamp-micros");
        assert_eq!(type_of("dec")["logicalType"], "decimal");
        assert_eq!(type_of("dec")["precision"], 10);
        assert_eq!(type_of("list")["type"], "array");
    }

    #[test]
    fn test_arrow_schema_to_avro_json_names_each_record_uniquely() {
        // Avro forbids redefining a named type: two struct columns sharing one
        // record name make Java's Schema.Parser reject the whole commit schema.
        let member = |name: &str| {
            std::sync::Arc::new(Field::new(name, DataType::Int64, false)) as arrow_schema::FieldRef
        };
        let schema = Schema::new(vec![
            Field::new("first", DataType::Struct(vec![member("x")].into()), false),
            Field::new("second", DataType::Struct(vec![member("y")].into()), false),
        ]);
        let json = arrow_schema_to_avro_json(&schema).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let names: Vec<&str> = value["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["type"]["name"].as_str().unwrap())
            .collect();
        assert_eq!(names.len(), 2);
        assert_ne!(names[0], names[1], "nested record names must be unique");
    }

    #[test]
    fn test_arrow_schema_to_avro_json_name_paths_cannot_collide() {
        // `a.b` and `a_b` collide under any name-derived path; record names
        // are built from field indexes precisely so they cannot.
        let long = std::sync::Arc::new(Field::new("x", DataType::Int64, false));
        let inner = DataType::Struct(vec![long.clone()].into());
        let schema = Schema::new(vec![
            Field::new(
                "a",
                DataType::Struct(vec![std::sync::Arc::new(Field::new("b", inner, false))].into()),
                false,
            ),
            Field::new("a_b", DataType::Struct(vec![long].into()), false),
        ]);
        let json = arrow_schema_to_avro_json(&schema).unwrap();
        let mut names: Vec<String> = Vec::new();
        fn collect(value: &serde_json::Value, out: &mut Vec<String>) {
            match value {
                serde_json::Value::Object(map) => {
                    if let Some(serde_json::Value::String(name)) = map.get("name")
                        && map.get("type").is_some_and(|t| t == "record")
                    {
                        out.push(name.clone());
                    }
                    for v in map.values() {
                        collect(v, out);
                    }
                }
                serde_json::Value::Array(items) => items.iter().for_each(|v| collect(v, out)),
                _ => {}
            }
        }
        collect(&serde_json::from_str(&json).unwrap(), &mut names);
        let unique: std::collections::HashSet<&String> = names.iter().collect();
        assert_eq!(
            unique.len(),
            names.len(),
            "record names collided: {names:?}"
        );
    }

    #[test]
    fn test_arrow_schema_to_avro_json_rejects_illegal_field_name() {
        // Renaming would diverge from the parquet column; an unparseable
        // commit schema would make the table unreadable. Reject instead.
        let schema = Schema::new(vec![Field::new("a-b", DataType::Int64, false)]);
        let error = arrow_schema_to_avro_json(&schema).unwrap_err();
        assert!(
            error.to_string().contains("legal Avro name"),
            "unexpected error: {error}"
        );
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
            Field::new("_hoodie_operation", DataType::Utf8, true),
            Field::new("id", DataType::Utf8, false),
            Field::new("_hoodie_is_deleted", DataType::Boolean, true),
        ]);
        let stripped = strip_meta_fields_from_schema(&schema);
        assert_eq!(
            stripped
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            vec!["id", "_hoodie_is_deleted"]
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

#[cfg(test)]
mod version_6_tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};

    use crate::storage::Storage;
    use crate::storage::fault::{FaultyPutStore, PutFault};
    use crate::table::{ReadOptions, Table};

    async fn version_6_table(base_dir: &std::path::Path) -> Table {
        let hoodie_dir = base_dir.join(".hoodie");
        std::fs::create_dir_all(&hoodie_dir).unwrap();
        let properties = [
            "hoodie.table.name=t",
            "hoodie.table.type=COPY_ON_WRITE",
            "hoodie.table.version=6",
            "hoodie.timeline.layout.version=1",
            "hoodie.populate.meta.fields=false",
            "hoodie.base.file.format=parquet",
        ]
        .join("\n");
        std::fs::write(hoodie_dir.join("hoodie.properties"), properties).unwrap();
        Table::new(base_dir.to_str().unwrap()).await.unwrap()
    }

    async fn row_count(base_uri: &str) -> usize {
        Table::new(base_uri)
            .await
            .unwrap()
            .read(&ReadOptions::new())
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }

    fn id_batch(ids: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(ids))],
        )
        .unwrap()
    }

    fn fault_commit_put(table: &mut Table, fault: PutFault) -> Arc<FaultyPutStore> {
        let storage = table.file_system_view.storage.clone();
        let store = FaultyPutStore::new_for_any_put(storage.object_store.clone(), fault, |path| {
            path.as_ref().ends_with(".commit")
        });
        table.file_system_view.storage = Storage::new_with_object_store(
            storage.base_url.as_ref().clone(),
            store.clone(),
            storage.hudi_configs.clone(),
        );
        store
    }

    /// A commit PUT that errors may still have stored the commit, so the base
    /// file it references must survive the error.
    #[tokio::test]
    async fn test_failed_commit_put_keeps_written_base_file() {
        for (fault, rows_after_error) in [(PutFault::LandedThenError, 2), (PutFault::NotLanded, 0)]
        {
            let dir = tempfile::tempdir().unwrap();
            let uri = dir.path().to_str().unwrap();
            let mut table = version_6_table(dir.path()).await;
            let store = fault_commit_put(&mut table, fault);

            let result = table.append([id_batch(vec![1, 2])]).await;
            assert!(result.is_err(), "{fault:?}: the faulted PUT must surface");
            assert!(!store.is_armed(), "{fault:?}: the commit PUT was not hit");
            assert_eq!(row_count(uri).await, rows_after_error, "{fault:?}");
        }
    }

    /// A later commit moves the timeline start past the failed instant, so a
    /// base file left without its commit would then read as committed.
    #[tokio::test]
    async fn test_failed_commit_put_then_append_reads_only_committed_rows() {
        for (fault, rows_after_retry) in [(PutFault::LandedThenError, 5), (PutFault::NotLanded, 3)]
        {
            let dir = tempfile::tempdir().unwrap();
            let uri = dir.path().to_str().unwrap();
            let mut table = version_6_table(dir.path()).await;
            fault_commit_put(&mut table, fault);
            assert!(table.append([id_batch(vec![1, 2])]).await.is_err());

            let mut retry = Table::new(uri).await.unwrap();
            retry.append([id_batch(vec![3, 4, 5])]).await.unwrap();
            assert_eq!(row_count(uri).await, rows_after_retry, "{fault:?}");
        }
    }

    /// The commit records the write schema as Avro, so a batch whose schema
    /// Avro cannot express is refused before any file is written.
    #[tokio::test]
    async fn test_schema_without_avro_form_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = version_6_table(dir.path()).await;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                false,
            )])),
            vec![Arc::new(arrow::array::TimestampNanosecondArray::from(
                vec![1],
            ))],
        )
        .unwrap();
        assert!(table.append([batch]).await.is_err());
        let written: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name())
            .filter(|name| name != ".hoodie")
            .collect();
        assert!(written.is_empty(), "unexpected files: {written:?}");
    }
}
