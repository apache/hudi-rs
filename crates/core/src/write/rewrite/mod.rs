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

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use arrow::array::{Array, ArrayRef, BooleanArray, StringArray, UInt32Array};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_select::take::take;
use arrow_select::zip::zip;

use crate::Result;
use crate::config::table::HudiTableConfig::{OrderingFields, RecordKeyFields, RecordMergeStrategy};
use crate::error::CoreError;
use crate::expr::ExprOperator;
use crate::expr::filter::{Filter, filters_to_row_mask, validate_fields_against_schemas};
use crate::file_group::record_batches::RecordBatches;
use crate::index::{HoodieIndex, HoodieKey, is_record_index_enabled};
use crate::merge::RecordMergeStrategyValue;
use crate::merge::record_merger::RecordMerger;
use crate::metadata::commit::HoodieWriteStat;
use crate::metadata::meta_field::MetaField;
use crate::metadata::table::encode::RecordIndexEntry;
use crate::table::{ReadOptions, Table};
use crate::timeline::instant::Action;
use crate::write::append::{
    ensure_copy_on_write, generate_instant_time, is_layout_two, prepare_batches_for_write,
    timeline_dir,
};
use crate::write::keygen::{hoodie_keys_for_batch, relative_data_path};
use crate::write::metadata::{instant_to_epoch_millis, is_column_stats_enabled};

mod cow;
#[cfg(test)]
mod lifecycle_tests;
mod mor;
mod rows;
mod validate;

use cow::*;
use mor::*;
use rows::*;
pub(crate) use validate::ensure_writable_table_version;
use validate::*;

/// Result of an upsert, delete, or overwrite write.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteResult {
    /// Completed instant timestamp.
    pub instant: String,
    /// Number of rows written to replacement base files.
    pub num_writes: usize,
    /// Number of input keys that replaced existing rows.
    pub num_updates: usize,
    /// Number of input keys not previously present.
    pub num_inserts: usize,
    /// Number of rows deleted.
    pub num_deletes: usize,
}

/// Options controlling an upsert.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpsertOptions {
    /// Data columns to overwrite for matched rows. `None` replaces complete rows.
    pub update_columns: Option<Vec<String>>,
}

/// How a COW rewrite commits: same file-group slice vs replacecommit overwrite.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RewriteKind {
    /// New base file slice in the same file group(s); timeline action = commit.
    Commit,
    /// Replace file groups; timeline action = replacecommit (overwrite only for now).
    Replace,
}

/// Critical section 1: request the action — mint the instant time and fence
/// it (requested + inflight) under the table lock, then release; the write's
/// heavy work runs unlocked. `request_commit` / `request_replacecommit` pick
/// the timeline action from the rewrite kind.
async fn request_rewrite_instant(
    table: &Table,
    operation: &str,
    kind: RewriteKind,
) -> Result<String> {
    let storage = table.file_system_view.storage.clone();
    let lock = crate::write::lock::lock_provider_for(table);
    let _cs1 = lock.lock().await?;
    let instant = generate_instant_time().await;
    let action = match kind {
        RewriteKind::Commit => Action::Commit,
        RewriteKind::Replace => Action::ReplaceCommit,
    };
    let requested_bytes = if kind == RewriteKind::Replace {
        crate::metadata::replace_commit::HoodieRequestedReplaceMetadata::for_operation(operation)
            .to_avro_bytes()?
    } else {
        Vec::new()
    };
    crate::write::fence_timeline_instant(
        storage.as_ref(),
        &timeline_dir(table),
        &instant,
        action,
        requested_bytes,
        crate::write::inflight_commit_metadata_bytes(operation, is_layout_two(table))?,
    )
    .await?;
    Ok(instant)
}

/// Critical section 1 for MOR deltacommits.
async fn request_deltacommit(table: &Table, operation: &str) -> Result<String> {
    let storage = table.file_system_view.storage.clone();
    let lock = crate::write::lock::lock_provider_for(table);
    let _cs1 = lock.lock().await?;
    let instant = generate_instant_time().await;
    crate::write::fence_timeline_instant(
        storage.as_ref(),
        &timeline_dir(table),
        &instant,
        Action::DeltaCommit,
        Vec::new(),
        crate::write::inflight_commit_metadata_bytes(operation, is_layout_two(table))?,
    )
    .await?;
    Ok(instant)
}

pub async fn upsert_batches(
    table: &mut Table,
    batches: &[RecordBatch],
    options: UpsertOptions,
) -> Result<WriteResult> {
    crate::write::ensure_supported_key_generator(&table.hudi_configs)?;
    table.reload_timeline_for_write().await?;
    ensure_configured_record_key(table)?;
    if table.is_mor() {
        return mor_upsert_batches(table, batches, options).await;
    }
    ensure_rewrite_supported(table)?;
    if batches.is_empty() {
        return Err(CoreError::Write(
            "upsert requires at least one RecordBatch".to_string(),
        ));
    }
    validate_write_input(table, batches, "upsert").await?;
    if let Some(columns) = &options.update_columns {
        let schema = batches[0].schema();
        for column in columns {
            if schema.field_with_name(column).is_err() {
                return Err(CoreError::Schema(format!(
                    "update column '{column}' is not in the write batch schema"
                )));
            }
            if MetaField::field_names_with_operation().contains(&column.as_str()) {
                return Err(CoreError::Write(format!(
                    "update column '{column}' is a Hudi meta field"
                )));
            }
        }
    }
    crate::write::keygen::validate_keygen_inputs(&table.hudi_configs, batches)?;
    let instant = request_rewrite_instant(table, "UPSERT", RewriteKind::Commit).await?;
    let incoming = prepare_batches_for_write(&table.hudi_configs, batches, &instant, "pending")?;
    let incoming = concat(&incoming)?;
    let key_name = record_key_name(table, &incoming)?;
    let incoming = deduplicate_last_by_key(&incoming, &key_name, &table.hudi_configs)?;
    let incoming_hoodie_keys = hoodie_keys_for_batch(&table.hudi_configs, &incoming, None)?;
    let delete_markers = delete_marker_rows(&incoming);
    let locations = crate::index::for_table_checked(table)
        .await
        .tag_location(table, &incoming_hoodie_keys)
        .await?;
    // Small-file packing (Java UpsertPartitioner.assignInserts): assign insert
    // rows to existing small file groups first; chosen groups join the read
    // set so their rows are preserved through the rewrite.
    let sizing = crate::write::sizing::SizingConfig::from_table(table);
    let avg_record_size = crate::write::sizing::average_record_size(
        table,
        table.file_system_view.storage.as_ref(),
        &sizing,
    )
    .await;
    let slices_by_partition = crate::write::sizing::latest_slices_by_partition(table).await?;
    let mut insert_rows_by_partition: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, hoodie_key) in incoming_hoodie_keys.iter().enumerate() {
        if !delete_markers[index] && locations.get(hoodie_key).and_then(Option::as_ref).is_none() {
            insert_rows_by_partition
                .entry(hoodie_key.partition_path.clone())
                .or_default()
                .push(index);
        }
    }
    let mut insert_target_by_row: HashMap<usize, Option<String>> = HashMap::new();
    for (partition, rows) in &insert_rows_by_partition {
        let slices: Vec<&crate::file_group::file_slice::FileSlice> = slices_by_partition
            .get(partition)
            .map(|v| v.iter().collect())
            .unwrap_or_default();
        let small = crate::write::sizing::small_file_groups(&slices, false, &sizing);
        let assignments = crate::write::sizing::assign_insert_buckets(
            rows.len(),
            &small,
            avg_record_size,
            &sizing,
        );
        let mut cursor = 0usize;
        for (bucket, count) in assignments {
            for row in &rows[cursor..cursor + count] {
                match &bucket {
                    crate::write::sizing::InsertBucket::Existing { file_id } => {
                        insert_target_by_row.insert(*row, Some(file_id.clone()));
                    }
                    crate::write::sizing::InsertBucket::New => {
                        insert_target_by_row.insert(*row, None);
                    }
                }
            }
            cursor += count;
        }
    }
    // MergeHandle model: route each input row to its target file group, then
    // per-group workers read+merge+write. The driver only holds the input and
    // plan metadata.
    let mut slice_by_file_id: HashMap<String, (String, crate::file_group::file_slice::FileSlice)> =
        HashMap::new();
    for (partition, slices) in &slices_by_partition {
        for slice in slices {
            slice_by_file_id.insert(
                slice.file_id().to_string(),
                (partition.clone(), slice.clone()),
            );
        }
    }

    let mut updates = 0usize;
    let mut inserts = 0usize;
    let mut rows_by_group: HashMap<String, Vec<u32>> = HashMap::new();
    let mut new_rows_by_partition: HashMap<String, Vec<u32>> = HashMap::new();
    let mut row_target: Vec<Option<String>> = Vec::with_capacity(incoming.num_rows());
    // Update-partition-path (global index): a key upserted with a different
    // partition value MOVES — evicted from the group in its old partition and
    // inserted where its values say. Anything else leaves rows whose stored
    // partition columns disagree with the path they physically live in.
    let mut evictions_by_group: HashMap<String, HashSet<String>> = HashMap::new();
    // Merge-before-move (Java parity): find the cross-partition updates whose
    // incoming row loses the event-time merge; they stay in their old group.
    let mut moved_candidates: HashMap<String, Vec<String>> = HashMap::new();
    for hoodie_key in &incoming_hoodie_keys {
        if let Some(location) = locations.get(hoodie_key).and_then(Option::as_ref)
            && location.partition_path != hoodie_key.partition_path
            && slice_by_file_id.contains_key(&location.file_id)
        {
            moved_candidates
                .entry(location.file_id.clone())
                .or_default()
                .push(hoodie_key.record_key.clone());
        }
    }
    let move_slices: HashMap<String, crate::file_group::file_slice::FileSlice> = slice_by_file_id
        .iter()
        .map(|(id, (_, slice))| (id.clone(), slice.clone()))
        .collect();
    let rejected_moves =
        moved_keys_rejected_by_event_time(table, &incoming, &moved_candidates, &move_slices)
            .await?;
    let mut moved_deletes = Vec::new();
    for (index, hoodie_key) in incoming_hoodie_keys.iter().enumerate() {
        // A location whose file group has no live slice is a stale index
        // entry (its group was replaced); the row no longer exists, so the
        // incoming record is an insert (Java re-validates tags the same way).
        let located = locations
            .get(hoodie_key)
            .and_then(Option::as_ref)
            .filter(|location| slice_by_file_id.contains_key(&location.file_id));
        // A delete marker that would land as a new row writes nothing (Java's
        // create handle skips it); on a partition move it only evicts the
        // stored row. In-place markers are resolved by the group merge.
        if delete_markers[index]
            && !located.is_some_and(|location| {
                location.partition_path == hoodie_key.partition_path
                    || rejected_moves.contains(&hoodie_key.record_key)
            })
        {
            if let Some(location) = located {
                evictions_by_group
                    .entry(location.file_id.clone())
                    .or_default()
                    .insert(hoodie_key.record_key.clone());
                moved_deletes.push((
                    hoodie_key.record_key.clone(),
                    location.partition_path.clone(),
                ));
            }
            row_target.push(None);
            continue;
        }
        let target = match located {
            Some(location) if location.partition_path == hoodie_key.partition_path => {
                updates += 1;
                Some(location.file_id.clone())
            }
            // Losing cross-partition update: route to the OLD group as a
            // plain update; the event-time merge keeps the old row.
            Some(location) if rejected_moves.contains(&hoodie_key.record_key) => {
                updates += 1;
                Some(location.file_id.clone())
            }
            Some(location) => {
                updates += 1;
                evictions_by_group
                    .entry(location.file_id.clone())
                    .or_default()
                    .insert(hoodie_key.record_key.clone());
                insert_target_by_row.get(&index).cloned().flatten()
            }
            None => {
                inserts += 1;
                insert_target_by_row.get(&index).cloned().flatten()
            }
        };
        match &target {
            Some(file_id) => rows_by_group
                .entry(file_id.clone())
                .or_default()
                .push(index as u32),
            None => new_rows_by_partition
                .entry(hoodie_key.partition_path.clone())
                .or_default()
                .push(index as u32),
        }
        row_target.push(target);
    }

    let mut plans = Vec::new();
    let mut sorted_groups: Vec<_> = rows_by_group.into_iter().collect();
    sorted_groups.sort_by(|a, b| a.0.cmp(&b.0));
    for (file_id, rows) in sorted_groups {
        let (partition, slice) = slice_by_file_id.get(&file_id).cloned().ok_or_else(|| {
            CoreError::Write(format!(
                "missing latest file slice for file group '{file_id}'"
            ))
        })?;
        let out_name = format!("{file_id}_0-0-0_{instant}.parquet");
        let evict_keys =
            std::sync::Arc::new(evictions_by_group.remove(&file_id).unwrap_or_default());
        plans.push(MergePlan {
            partition,
            file_id: file_id.clone(),
            out_name,
            prev_commit: slice.creation_instant_time().to_string(),
            slice,
            op: GroupOp::Upsert {
                incoming: take_batch(&incoming, &rows)?,
                partial_columns: options.update_columns.clone(),
                evict_keys,
            },
        });
    }
    // Groups that only lose rows (every touched key moved away).
    let mut evict_only: Vec<_> = evictions_by_group.into_iter().collect();
    evict_only.sort_by(|a, b| a.0.cmp(&b.0));
    for (file_id, evicted) in evict_only {
        let (partition, slice) = slice_by_file_id.get(&file_id).cloned().ok_or_else(|| {
            CoreError::Write(format!(
                "missing latest file slice for file group '{file_id}'"
            ))
        })?;
        let out_name = format!("{file_id}_0-0-0_{instant}.parquet");
        plans.push(MergePlan {
            partition,
            file_id: file_id.clone(),
            out_name,
            prev_commit: slice.creation_instant_time().to_string(),
            slice,
            op: GroupOp::DeleteKeys {
                exact: std::sync::Arc::new(HashSet::new()),
                keys_only: std::sync::Arc::new(evicted),
            },
        });
    }

    // Untargeted inserts open new file groups, sized by the insert bucket.
    let per_bucket = sizing.insert_records_per_bucket(avg_record_size);
    let mut new_groups = Vec::new();
    let mut new_group_by_row: HashMap<u32, String> = HashMap::new();
    let mut sorted_new: Vec<_> = new_rows_by_partition.into_iter().collect();
    sorted_new.sort_by(|a, b| a.0.cmp(&b.0));
    for (partition, rows) in sorted_new {
        for chunk in rows.chunks(per_bucket.max(1)) {
            let file_id = crate::write::new_file_id();
            let out_name = format!("{file_id}_0-0-0_{instant}.parquet");
            for row in chunk {
                new_group_by_row.insert(*row, file_id.clone());
            }
            let batch = restamp_file_name(&take_batch(&incoming, chunk)?, &out_name)?;
            new_groups.push(NewGroupWrite {
                partition: partition.clone(),
                file_id,
                out_name,
                prepared: vec![batch],
            });
        }
    }

    let instant_time_millis = crate::write::metadata::instant_to_epoch_millis_in(
        &instant,
        &crate::write::metadata::timeline_timezone(&table.hudi_configs),
    );
    // RLI adds: every input row's final location is known from the plan. A
    // REJECTED cross-partition move keeps the old row, so its index entry
    // must carry the old partition, not the losing incoming value.
    let rli_entries = incoming_hoodie_keys
        .iter()
        .enumerate()
        .filter_map(|(row, key)| {
            let file_id = row_target[row]
                .clone()
                .or_else(|| new_group_by_row.get(&(row as u32)).cloned())?;
            let partition_path = if rejected_moves.contains(&key.record_key) {
                locations
                    .get(key)
                    .and_then(Option::as_ref)
                    .map(|location| location.partition_path.clone())
                    .unwrap_or_else(|| key.partition_path.clone())
            } else {
                key.partition_path.clone()
            };
            Some(RecordIndexEntry {
                record_key: key.record_key.clone(),
                partition_path,
                file_id,
                instant_time_millis,
                is_deleted: false,
            })
        })
        .chain(
            moved_deletes
                .into_iter()
                .map(|(record_key, partition_path)| RecordIndexEntry {
                    record_key,
                    partition_path,
                    file_id: String::new(),
                    instant_time_millis: instant_to_epoch_millis(&instant),
                    is_deleted: true,
                }),
        )
        .collect();

    let schema = incoming.schema();
    commit_merge_plans(
        table,
        &instant,
        "UPSERT",
        plans,
        new_groups,
        schema,
        rli_entries,
        updates,
        inserts,
        MatchedSlot::None,
        false,
    )
    .await
}

pub async fn delete_filter(table: &mut Table, filter: Filter) -> Result<WriteResult> {
    table.reload_timeline_for_write().await?;
    // Key equality / IN on the record key (or meta record key) → RLI/SimpleIndex path.
    if let Some(keys) = keys_from_record_key_filter(table, &filter)?
        && !keys.is_empty()
    {
        return delete_keys(table, &keys).await;
    }

    if table.is_mor() {
        ensure_mor_merge_supported(table)?;
        let Some((old, _, _, _)) = data_for_filter_matches(table, &filter).await? else {
            return Ok(WriteResult::default());
        };
        let mask = filters_to_row_mask(&[filter], &old)?;
        let key_name = record_key_name(table, &old).map_err(|_| {
            CoreError::Unsupported(
                "MOR expression delete requires record keys (or _hoodie_record_key) in the data"
                    .to_string(),
            )
        })?;
        let record_keys = keys(&old, &key_name)?;
        let partitions = partition_paths_for_batch(&old);
        let delete_key_list = mask
            .iter()
            .enumerate()
            .filter(|(_, matches)| matches.unwrap_or(false))
            .map(|(index, _)| HoodieKey {
                record_key: record_keys[index].clone(),
                partition_path: partitions[index].clone(),
            })
            .collect::<Vec<_>>();
        if delete_key_list.is_empty() {
            return Ok(WriteResult::default());
        }
        return mor_delete_keys(table, &delete_key_list).await;
    }

    // COW scan path — any column; no configured record key required. Each
    // file group's worker evaluates the filter itself; groups without matches
    // write nothing, and a zero-match delete aborts the fenced instant.
    ensure_rewrite_supported(table)?;
    let schema = table.get_schema_with_meta_fields().await?;
    validate_fields_against_schemas(std::slice::from_ref(&filter), [&schema])?;
    let instant = request_rewrite_instant(table, "DELETE", RewriteKind::Commit).await?;
    let plans = merge_plans_for_slices(
        table,
        &instant,
        |_| true,
        |_| GroupOp::DeleteWhere {
            filter: filter.clone(),
        },
    )
    .await?;
    let schema = table.data_schema_for_read().await?;
    commit_merge_plans(
        table,
        &instant,
        "DELETE",
        plans,
        Vec::new(),
        std::sync::Arc::new(schema),
        Vec::new(),
        0,
        0,
        MatchedSlot::Deletes,
        true,
    )
    .await
}

/// UPDATE: set columns from a single-row `updates` batch on rows matching `filter`.
pub async fn update_filter(
    table: &mut Table,
    filter: Filter,
    updates: RecordBatch,
) -> Result<WriteResult> {
    table.reload_timeline_for_write().await?;
    if updates.num_rows() != 1 {
        return Err(CoreError::Write(
            "update requires a single-row RecordBatch of SET values".to_string(),
        ));
    }
    ensure_set_columns_updatable(table, &updates)?;
    if table.is_mor() {
        return mor_update_filter(table, filter, updates).await;
    }
    ensure_rewrite_supported(table)?;
    let schema = table.get_schema_with_meta_fields().await?;
    validate_fields_against_schemas(std::slice::from_ref(&filter), [&schema])?;
    let meta: HashSet<&str> = MetaField::field_names_with_operation()
        .into_iter()
        .collect();
    let mut has_data_column = false;
    for field in updates.schema().fields() {
        let name = field.name();
        let Some((_, table_field)) = schema.column_with_name(name) else {
            return Err(CoreError::Schema(format!(
                "update column '{name}' is not in the table schema"
            )));
        };
        if meta.contains(name.as_str()) {
            continue;
        }
        has_data_column = true;
        if field.data_type() != table_field.data_type() {
            return Err(CoreError::Schema(format!(
                "update column '{name}' has type {} but the table column is {}",
                field.data_type(),
                table_field.data_type()
            )));
        }
    }
    if !has_data_column {
        return Err(CoreError::Write(
            "update requires at least one data column to set".to_string(),
        ));
    }
    let instant = request_rewrite_instant(table, "UPSERT", RewriteKind::Commit).await?;
    let plans = merge_plans_for_slices(
        table,
        &instant,
        |_| true,
        |_| GroupOp::UpdateWhere {
            filter: filter.clone(),
            set: updates.clone(),
        },
    )
    .await?;
    let schema = table.data_schema_for_read().await?;
    commit_merge_plans(
        table,
        &instant,
        "UPSERT",
        plans,
        Vec::new(),
        std::sync::Arc::new(schema),
        Vec::new(),
        0,
        0,
        MatchedSlot::Updates,
        true,
    )
    .await
}

pub async fn delete_keys(table: &mut Table, delete_keys: &[HoodieKey]) -> Result<WriteResult> {
    if delete_keys.is_empty() {
        return Err(CoreError::Write(
            "delete requires at least one record key".to_string(),
        ));
    }
    table.reload_timeline_for_write().await?;
    ensure_configured_record_key(table)?;
    if table.is_mor() {
        return mor_delete_keys(table, delete_keys).await;
    }
    ensure_rewrite_supported(table)?;
    let locations = crate::index::for_table_checked(table)
        .await
        .tag_location(table, delete_keys)
        .await?;
    let affected_file_ids = locations
        .values()
        .filter_map(|loc| loc.as_ref().map(|l| l.file_id.clone()))
        .collect::<HashSet<_>>();
    if affected_file_ids.is_empty() {
        return Ok(WriteResult::default());
    }
    let requested_exact = std::sync::Arc::new(
        delete_keys
            .iter()
            .filter(|key| !key.partition_path.is_empty())
            .map(|key| (key.record_key.clone(), key.partition_path.clone()))
            .collect::<HashSet<_>>(),
    );
    let requested_keys_only = std::sync::Arc::new(
        delete_keys
            .iter()
            .filter(|key| key.partition_path.is_empty())
            .map(|key| key.record_key.clone())
            .collect::<HashSet<_>>(),
    );
    let instant = request_rewrite_instant(table, "DELETE", RewriteKind::Commit).await?;
    let plans = merge_plans_for_slices(
        table,
        &instant,
        |slice| affected_file_ids.contains(slice.file_id()),
        |_| GroupOp::DeleteKeys {
            exact: requested_exact.clone(),
            keys_only: requested_keys_only.clone(),
        },
    )
    .await?;
    let schema = table.data_schema_for_read().await?;
    commit_merge_plans(
        table,
        &instant,
        "DELETE",
        plans,
        Vec::new(),
        std::sync::Arc::new(schema),
        Vec::new(),
        0,
        0,
        MatchedSlot::Deletes,
        true,
    )
    .await
}

/// Enumerate the latest file slices' (file id, base path) pairs — the
/// metadata a replace commit needs — without reading any data. `partitions`
/// limits the listing; `None` covers the whole table.
#[allow(clippy::type_complexity)]
async fn replaced_groups_from_listing(
    table: &Table,
    partitions: Option<&HashSet<String>>,
) -> Result<(
    Vec<String>,
    Vec<String>,
    Vec<(String, String)>,
    Vec<crate::file_group::file_slice::FileSlice>,
    Vec<String>,
)> {
    let slices = table.get_file_slices(&ReadOptions::new()).await?;
    let mut file_ids = Vec::new();
    let mut old_paths = Vec::new();
    // Log-only MOR slices have no base path but must still be excluded by
    // the replace commit, or their records outlive the overwrite.
    let mut log_only = Vec::new();
    let mut replaced_log_paths = Vec::new();
    let mut replaced_slices = Vec::new();
    for slice in slices {
        if let Some(partitions) = partitions
            && !partitions.contains(&slice.partition_path)
        {
            continue;
        }
        match slice.base_file_relative_path()? {
            Some(path) => {
                file_ids.push(slice.file_id().to_string());
                old_paths.push(path);
            }
            None => log_only.push((slice.partition_path.clone(), slice.file_id().to_string())),
        }
        // Log files of replaced slices are replaced too, but they must stay
        // OUT of `old_paths`, which is positionally zipped with `file_ids`.
        for log in &slice.log_files {
            replaced_log_paths.push(relative_data_path(&slice.partition_path, &log.file_name()));
        }
        replaced_slices.push(slice);
    }
    Ok((
        file_ids,
        old_paths,
        log_only,
        replaced_slices,
        replaced_log_paths,
    ))
}

/// RLI tombstones for keys that a replace commit drops.
///
/// Java emits these at replacecommit time (BaseRecordIndexer's left-anti-join
/// of the replaced groups' keys against the commit's upserts): without them the
/// index keeps pointing at replaced file groups forever — there is no cleaner
/// to remove them — and a Java writer, whose tagging does no live-slice
/// validation, routes updates into dead groups.
///
/// Reads one replaced slice at a time, projected to the record-key column, so
/// no file group's data columns are ever materialized. The returned tombstone
/// list is still proportional to the number of dropped keys, which is inherent
/// to the operation: Java does the same work spread across a Spark job, while
/// this writer is single-node (see docs/gaps.md).
async fn rli_deletes_for_replaced_groups(
    table: &Table,
    replaced: &[crate::file_group::file_slice::FileSlice],
    surviving_keys: &HashSet<String>,
    instant: &str,
) -> Result<Vec<RecordIndexEntry>> {
    if !is_record_index_enabled(table) || replaced.is_empty() {
        return Ok(Vec::new());
    }
    let key_field = MetaField::RecordKey.as_ref();
    // Record keys only: never materialize a replaced group's data columns.
    let options = ReadOptions::new().with_projection([key_field]);
    let reader = table
        .create_file_group_reader_with_options(Some(&options), std::iter::empty::<(&str, String)>())
        .await?;
    let millis = instant_to_epoch_millis(instant);
    let mut entries = Vec::new();
    for slice in replaced {
        let batch = reader.read_file_slice(slice, &options).await?;
        if batch.num_rows() == 0 {
            continue;
        }
        // A table without populated meta fields has no record keys to index;
        // there is nothing to tombstone and nothing to fail over.
        if batch.column_by_name(key_field).is_none() {
            continue;
        }
        for key in keys(&batch, key_field)? {
            if surviving_keys.contains(&key) {
                continue;
            }
            entries.push(RecordIndexEntry {
                record_key: key,
                // update_record_index shards by record key and writes into the
                // MDT partition, so the data partition path is not consulted
                // for deletes; reading it here would fail tables that do not
                // carry the column.
                partition_path: slice.partition_path.clone(),
                file_id: String::new(),
                instant_time_millis: millis,
                is_deleted: true,
            });
        }
    }
    Ok(entries)
}

/// Build merge plans for the latest slices matching `include`, with `op`
/// chosen per slice. Metadata-scale: no file group data is read here.
async fn merge_plans_for_slices<F, O>(
    table: &Table,
    instant: &str,
    include: F,
    mut op: O,
) -> Result<Vec<MergePlan>>
where
    F: Fn(&crate::file_group::file_slice::FileSlice) -> bool,
    O: FnMut(&crate::file_group::file_slice::FileSlice) -> GroupOp,
{
    let slices = table.get_file_slices(&ReadOptions::new()).await?;
    let mut plans = Vec::new();
    for slice in slices {
        if !include(&slice) {
            continue;
        }
        let file_id = slice.file_id().to_string();
        let out_name = format!("{file_id}_0-0-0_{instant}.parquet");
        plans.push(MergePlan {
            partition: slice.partition_path.clone(),
            file_id,
            out_name,
            prev_commit: slice.creation_instant_time().to_string(),
            op: op(&slice),
            slice,
        });
    }
    plans.sort_by(|a, b| a.file_id.cmp(&b.file_id));
    Ok(plans)
}

pub async fn overwrite_batches(table: &mut Table, batches: &[RecordBatch]) -> Result<WriteResult> {
    crate::write::ensure_supported_key_generator(&table.hudi_configs)?;
    table.reload_timeline_for_write().await?;
    // Insert-overwrite writes new BASE file groups + a replacecommit on both
    // COW and MOR (Java uses the same executor for either table type).
    ensure_supported_merge_configs(table)?;
    if batches.is_empty() {
        return Err(CoreError::Write(
            "overwrite requires at least one RecordBatch".to_string(),
        ));
    }
    validate_write_input(table, batches, "overwrite").await?;
    // Listing enumerates the replaced groups; their record keys are read
    // (key column only) afterwards to tombstone dropped index entries.
    crate::write::keygen::validate_keygen_inputs(&table.hudi_configs, batches)?;
    let (file_ids, old_paths, log_only, replaced_slices, replaced_log_paths) =
        replaced_groups_from_listing(table, None).await?;
    let instant =
        request_rewrite_instant(table, "INSERT_OVERWRITE_TABLE", RewriteKind::Replace).await?;
    let file_id = crate::write::new_file_id();
    let file_name = format!("{file_id}_0-0-0_{instant}.parquet");
    let batches = prepare_batches_for_write(&table.hudi_configs, batches, &instant, &file_name)?;
    let replacement = concat(&batches)?;
    let surviving: HashSet<String> = keys(&replacement, MetaField::RecordKey.as_ref())?
        .into_iter()
        .collect();
    let rli_deletes =
        rli_deletes_for_replaced_groups(table, &replaced_slices, &surviving, &instant).await?;
    rewrite(
        table,
        &instant,
        &replacement,
        None,
        file_ids,
        old_paths,
        log_only,
        replaced_log_paths,
        "INSERT_OVERWRITE_TABLE",
        0,
        replacement.num_rows(),
        0,
        rli_deletes,
        RewriteKind::Replace,
    )
    .await
}

pub async fn dynamic_partition_overwrite_batches(
    table: &mut Table,
    batches: &[RecordBatch],
) -> Result<WriteResult> {
    crate::write::ensure_supported_key_generator(&table.hudi_configs)?;
    table.reload_timeline_for_write().await?;
    // Works on COW and MOR alike (see overwrite_batches).
    ensure_supported_merge_configs(table)?;
    if batches.is_empty() {
        return Err(CoreError::Write(
            "dynamic_partition_overwrite requires at least one RecordBatch".to_string(),
        ));
    }
    // Validate before fencing so misuse leaves no pending instant behind.
    let partition_fields: Vec<String> = table
        .hudi_configs
        .get_or_default(crate::config::table::HudiTableConfig::PartitionFields)
        .into();
    if partition_fields.is_empty() {
        return Err(CoreError::Unsupported(
            "dynamic_partition_overwrite requires a partitioned table; use overwrite() for unpartitioned tables"
                .to_string(),
        ));
    }
    validate_write_input(table, batches, "dynamic_partition_overwrite").await?;
    // Partition paths depend only on the input batches; derive and validate
    // them before fencing so bad input leaves no timeline trace.
    let raw = concat(batches)?;
    let partition_paths = hoodie_keys_for_batch(
        &table.hudi_configs,
        &raw,
        Some(crate::write::keygen::VALIDATION_INSTANT),
    )?
    .into_iter()
    .map(|key| key.partition_path)
    .collect::<HashSet<_>>();
    if partition_paths.is_empty() || partition_paths.iter().all(|path| path.is_empty()) {
        return Err(CoreError::Unsupported(
            "dynamic_partition_overwrite requires a partitioned table; use overwrite() for unpartitioned tables"
                .to_string(),
        ));
    }
    let instant = request_rewrite_instant(table, "INSERT_OVERWRITE", RewriteKind::Replace).await?;
    let prepared = prepare_batches_for_write(&table.hudi_configs, batches, &instant, "pending")?;
    let replacement = concat(&prepared)?;
    let (file_ids, old_paths, log_only, replaced_slices, replaced_log_paths) =
        replaced_groups_from_listing(table, Some(&partition_paths)).await?;
    let replacement = if !file_ids.is_empty() {
        let table_schema = std::sync::Arc::new(table.get_schema_with_meta_fields().await?);
        crate::write::align_batch_to_schema(&replacement, &table_schema).ok_or_else(|| {
            CoreError::Schema(
                "overwrite batch schema does not match the current table schema".to_string(),
            )
        })?
    } else {
        replacement
    };
    let surviving: HashSet<String> = keys(&replacement, MetaField::RecordKey.as_ref())?
        .into_iter()
        .collect();
    let rli_deletes =
        rli_deletes_for_replaced_groups(table, &replaced_slices, &surviving, &instant).await?;
    rewrite(
        table,
        &instant,
        &replacement,
        None,
        file_ids,
        old_paths,
        log_only,
        replaced_log_paths,
        "INSERT_OVERWRITE",
        0,
        replacement.num_rows(),
        0,
        rli_deletes,
        RewriteKind::Replace,
    )
    .await
}
