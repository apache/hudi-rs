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
use crate::index::{HoodieIndex, HoodieKey, for_table, is_record_index_enabled};
use crate::merge::RecordMergeStrategyValue;
use crate::merge::record_merger::RecordMerger;
use crate::metadata::commit::{HoodieCommitMetadata, HoodieWriteStat};
use crate::metadata::meta_field::MetaField;
use crate::metadata::table::encode::RecordIndexEntry;
use crate::table::{ReadOptions, Table};
use crate::timeline::instant::{Action, Instant};
use crate::write::append::{
    ensure_copy_on_write, generate_instant_time, is_layout_two, prepare_batches_for_write,
    timeline_dir,
};
use crate::write::keygen::{hoodie_keys_for_batch, relative_data_path};
use crate::write::metadata::{
    instant_to_epoch_millis, is_column_stats_enabled, update_column_stats_partitions,
    update_files_partition_entries, update_record_index,
};

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
    let instant = request_rewrite_instant(table, "UPSERT", RewriteKind::Commit).await?;
    let incoming = prepare_batches_for_write(&table.hudi_configs, batches, &instant, "pending")?;
    let incoming = concat(&incoming)?;
    if incoming.num_rows() == 0 {
        return Err(CoreError::Write(
            "upsert requires at least one row".to_string(),
        ));
    }
    let key_name = record_key_name(table, &incoming)?;
    let incoming = deduplicate_last_by_key(&incoming, &key_name)?;
    let incoming_hoodie_keys = hoodie_keys_for_batch(&table.hudi_configs, &incoming, None)?;
    let locations = for_table(table)
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
        if locations.get(hoodie_key).and_then(Option::as_ref).is_none() {
            insert_rows_by_partition
                .entry(hoodie_key.partition_path.clone())
                .or_default()
                .push(index);
        }
    }
    let mut insert_target_by_row: HashMap<usize, Option<String>> = HashMap::new();
    let mut packed_group_ids: HashSet<String> = HashSet::new();
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
                        packed_group_ids.insert(file_id.clone());
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
    for (index, hoodie_key) in incoming_hoodie_keys.iter().enumerate() {
        let target = if let Some(Some(location)) = locations.get(hoodie_key) {
            updates += 1;
            Some(location.file_id.clone())
        } else {
            inserts += 1;
            insert_target_by_row.get(&index).cloned().flatten()
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
        plans.push(MergePlan {
            partition,
            file_id: file_id.clone(),
            out_name,
            prev_commit: slice.creation_instant_time().to_string(),
            slice,
            op: GroupOp::Upsert {
                incoming: take_batch(&incoming, &rows)?,
                partial_columns: options.update_columns.clone(),
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

    // RLI adds: every input row's final location is known from the plan.
    let rli_entries = incoming_hoodie_keys
        .iter()
        .enumerate()
        .filter_map(|(row, key)| {
            let file_id = row_target[row]
                .clone()
                .or_else(|| new_group_by_row.get(&(row as u32)).cloned())?;
            Some(RecordIndexEntry {
                record_key: key.record_key.clone(),
                partition_path: key.partition_path.clone(),
                file_id,
                instant_time_millis: instant_to_epoch_millis(&instant),
                is_deleted: false,
            })
        })
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
    if table.is_mor() {
        return mor_update_filter(table, filter, updates).await;
    }
    ensure_rewrite_supported(table)?;
    let schema = table.get_schema_with_meta_fields().await?;
    validate_fields_against_schemas(std::slice::from_ref(&filter), [&schema])?;
    for field in updates.schema().fields() {
        let name = field.name();
        if schema.column_with_name(name).is_none() {
            return Err(CoreError::Schema(format!(
                "update column '{name}' is not in the table schema"
            )));
        }
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
    let locations = for_table(table).tag_location(table, delete_keys).await?;
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
async fn replaced_groups_from_listing(
    table: &Table,
    partitions: Option<&HashSet<String>>,
) -> Result<(Vec<String>, Vec<String>)> {
    let slices = table.get_file_slices(&ReadOptions::new()).await?;
    let mut file_ids = Vec::new();
    let mut old_paths = Vec::new();
    for slice in slices {
        if let Some(partitions) = partitions
            && !partitions.contains(&slice.partition_path)
        {
            continue;
        }
        let Some(path) = slice.base_file_relative_path()? else {
            continue;
        };
        file_ids.push(slice.file_id().to_string());
        old_paths.push(path);
    }
    Ok((file_ids, old_paths))
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
    table.reload_timeline_for_write().await?;
    // Insert-overwrite writes new BASE file groups + a replacecommit on both
    // COW and MOR (Java uses the same executor for either table type).
    ensure_supported_merge_configs(table)?;
    if batches.is_empty() {
        return Err(CoreError::Write(
            "overwrite requires at least one RecordBatch".to_string(),
        ));
    }
    // Only the listing is needed to enumerate replaced groups — never the data.
    let (file_ids, old_paths) = replaced_groups_from_listing(table, None).await?;
    let instant =
        request_rewrite_instant(table, "INSERT_OVERWRITE_TABLE", RewriteKind::Replace).await?;
    let file_id = crate::write::new_file_id();
    let file_name = format!("{file_id}_0-0-0_{instant}.parquet");
    let batches = prepare_batches_for_write(&table.hudi_configs, batches, &instant, &file_name)?;
    let replacement = concat(&batches)?;
    if replacement.num_rows() == 0 {
        return Err(CoreError::Write(
            "overwrite requires at least one row".to_string(),
        ));
    }
    rewrite(
        table,
        &instant,
        &replacement,
        None,
        file_ids,
        old_paths,
        "INSERT_OVERWRITE_TABLE",
        0,
        replacement.num_rows(),
        0,
        Vec::new(),
        RewriteKind::Replace,
    )
    .await
}

pub async fn dynamic_partition_overwrite_batches(
    table: &mut Table,
    batches: &[RecordBatch],
) -> Result<WriteResult> {
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
    let instant = request_rewrite_instant(table, "INSERT_OVERWRITE", RewriteKind::Replace).await?;
    let prepared = prepare_batches_for_write(&table.hudi_configs, batches, &instant, "pending")?;
    let replacement = concat(&prepared)?;
    if replacement.num_rows() == 0 {
        return Err(CoreError::Write(
            "dynamic_partition_overwrite requires at least one row".to_string(),
        ));
    }
    let partition_paths = hoodie_keys_for_batch(&table.hudi_configs, &replacement, Some(&instant))?
        .into_iter()
        .map(|key| key.partition_path)
        .collect::<HashSet<_>>();
    if partition_paths.is_empty() || partition_paths.iter().all(|path| path.is_empty()) {
        return Err(CoreError::Unsupported(
            "dynamic_partition_overwrite requires a partitioned table; use overwrite() for unpartitioned tables"
                .to_string(),
        ));
    }
    let (file_ids, old_paths) = replaced_groups_from_listing(table, Some(&partition_paths)).await?;
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
    rewrite(
        table,
        &instant,
        &replacement,
        None,
        file_ids,
        old_paths,
        "INSERT_OVERWRITE",
        0,
        replacement.num_rows(),
        0,
        Vec::new(),
        RewriteKind::Replace,
    )
    .await
}

fn ensure_rewrite_supported(table: &Table) -> Result<()> {
    ensure_copy_on_write(table)?;
    ensure_supported_merge_configs(table)
}

fn ensure_mor_merge_supported(table: &Table) -> Result<()> {
    ensure_supported_merge_configs(table)?;
    let populates_meta_fields: bool = table
        .hudi_configs
        .get_or_default(crate::config::table::HudiTableConfig::PopulatesMetaFields)
        .into();
    if !populates_meta_fields {
        return Err(CoreError::Unsupported(
            "MERGE_ON_READ upsert and delete require hoodie.populate.meta.fields=true".to_string(),
        ));
    }
    let strategy: String = table
        .hudi_configs
        .get_or_default(RecordMergeStrategy)
        .into();
    if RecordMergeStrategyValue::from_str(&strategy)?
        != RecordMergeStrategyValue::OverwriteWithLatest
    {
        return Err(CoreError::Unsupported(
            "MERGE_ON_READ upsert and delete require an upsertable merge mode".to_string(),
        ));
    }
    Ok(())
}

fn ensure_supported_merge_configs(table: &Table) -> Result<()> {
    // Payload classes are deprecated; writers only honor merge mode / strategy.
    // Reject CUSTOM merge and custom merger impls — support COMMIT_TIME, EVENT_TIME,
    // and append-only (derived strategy).
    let options = table.hudi_configs.as_options();
    if let Some(mode) = options.get("hoodie.record.merge.mode") {
        let normalized = mode.to_ascii_uppercase();
        match normalized.as_str() {
            "COMMIT_TIME_ORDERING" | "EVENT_TIME_ORDERING" => {}
            "CUSTOM" => {
                return Err(CoreError::Unsupported(
                    "writes do not support hoodie.record.merge.mode=CUSTOM".to_string(),
                ));
            }
            other => {
                return Err(CoreError::Unsupported(format!(
                    "writes only support COMMIT_TIME_ORDERING or EVENT_TIME_ORDERING merge modes, got '{other}'"
                )));
            }
        }
    }
    if let Some(merger) = options.get("hoodie.record.merger.impls")
        && !merger.trim().is_empty()
    {
        return Err(CoreError::Unsupported(
            "writes do not support custom hoodie.record.merger.impls".to_string(),
        ));
    }
    Ok(())
}

struct MorFileLocation {
    file_id: String,
    partition_path: String,
    /// `None` for a log-only file group (e.g. a Spark delta commit that
    /// appended logs before any base file existed).
    base_file_path: Option<String>,
    base_instant: String,
}

async fn mor_file_locations(
    table: &Table,
    keys: &[HoodieKey],
) -> Result<HashMap<String, MorFileLocation>> {
    let slices = table.get_file_slices(&ReadOptions::new()).await?;
    let mut slices_by_file_id = HashMap::with_capacity(slices.len());
    for slice in slices {
        slices_by_file_id.insert(
            (slice.partition_path.clone(), slice.file_id().to_string()),
            (
                slice.base_file_relative_path()?,
                slice.creation_instant_time().to_string(),
            ),
        );
    }

    let tagged = for_table(table).tag_location(table, keys).await?;
    let mut locations = HashMap::with_capacity(tagged.len());
    for (key, location) in tagged {
        if let Some(location) = location {
            let (base_file_path, base_instant) = slices_by_file_id
                .get(&(location.partition_path.clone(), location.file_id.clone()))
                .ok_or_else(|| {
                    CoreError::Write(format!(
                        "record index returned missing file group '{}'",
                        location.file_id
                    ))
                })?;
            locations.insert(
                key.record_key,
                MorFileLocation {
                    file_id: location.file_id,
                    partition_path: location.partition_path,
                    base_file_path: base_file_path.clone(),
                    base_instant: base_instant.clone(),
                },
            );
        }
    }
    Ok(locations)
}

async fn mor_upsert_batches(
    table: &mut Table,
    batches: &[RecordBatch],
    options: UpsertOptions,
) -> Result<WriteResult> {
    ensure_mor_merge_supported(table)?;
    if options.update_columns.is_some() {
        return Err(CoreError::Unsupported(
            "partial updates are not yet supported for MERGE_ON_READ tables".to_string(),
        ));
    }
    if batches.is_empty() {
        return Err(CoreError::Write(
            "upsert requires at least one RecordBatch".to_string(),
        ));
    }

    let incoming = concat(batches)?;
    if incoming.num_rows() == 0 {
        return Err(CoreError::Write(
            "upsert requires at least one row".to_string(),
        ));
    }
    let schema_check_batches = prepare_batches_for_write(
        &table.hudi_configs,
        std::slice::from_ref(&incoming),
        "schema-check",
        "schema-check",
    )?;
    let existing_batches = table.read(&ReadOptions::new()).await?;
    if !existing_batches.is_empty() {
        let existing = concat(&existing_batches)?;
        if crate::write::align_batch_to_schema(&schema_check_batches[0], &existing.schema())
            .is_none()
        {
            return Err(CoreError::Schema(
                "upsert batch schema does not match the current table schema".to_string(),
            ));
        }
    }
    let key_name = record_key_name(table, &incoming)?;
    let incoming = deduplicate_last_by_key(&incoming, &key_name)?;
    let instant = request_deltacommit(table, "UPSERT").await?;
    let tagged_keys = hoodie_keys_for_batch(&table.hudi_configs, &incoming, Some(&instant))?;
    let incoming_keys = keys(&incoming, &key_name)?;
    let locations = mor_file_locations(table, &tagged_keys).await?;
    let mut update_indices: HashMap<(String, String), Vec<u32>> = HashMap::new();
    let mut insert_indices_by_partition: HashMap<String, Vec<u32>> = HashMap::new();
    let mut updates = 0;
    let mut inserts = 0;
    for (index, key) in incoming_keys.iter().enumerate() {
        if let Some(location) = locations.get(key) {
            update_indices
                .entry((location.partition_path.clone(), location.file_id.clone()))
                .or_default()
                .push(index as u32);
            updates += 1;
        } else {
            let partition = tagged_keys
                .get(index)
                .map(|k| k.partition_path.clone())
                .unwrap_or_default();
            insert_indices_by_partition
                .entry(partition)
                .or_default()
                .push(index as u32);
            inserts += 1;
        }
    }

    let storage = table.file_system_view.storage.clone();
    // Small-file packing (Java SparkUpsertDeltaCommitPartitioner): inserts
    // fill existing small file slices (base + logs at parquet-equivalent
    // size) as log appends, then overflow into new base file groups of
    // ~max.file.size records each.
    let sizing = crate::write::sizing::SizingConfig::from_table(table);
    let avg_record_size =
        crate::write::sizing::average_record_size(table, storage.as_ref(), &sizing).await;
    let slices_by_partition = crate::write::sizing::latest_slices_by_partition(table).await?;
    let mut packed_inserts: HashMap<(String, String), Vec<u32>> = HashMap::new();
    let mut new_group_inserts: Vec<(String, String, String, Vec<u32>)> = Vec::new();
    for (partition, indices) in insert_indices_by_partition {
        let slices: Vec<&crate::file_group::file_slice::FileSlice> = slices_by_partition
            .get(&partition)
            .map(|v| v.iter().collect())
            .unwrap_or_default();
        let small = crate::write::sizing::small_file_groups(&slices, true, &sizing);
        let assignments = crate::write::sizing::assign_insert_buckets(
            indices.len(),
            &small,
            avg_record_size,
            &sizing,
        );
        let mut cursor = 0usize;
        for (bucket, count) in assignments {
            let chunk = indices[cursor..cursor + count].to_vec();
            cursor += count;
            match bucket {
                crate::write::sizing::InsertBucket::Existing { file_id } => {
                    packed_inserts
                        .entry((partition.clone(), file_id))
                        .or_default()
                        .extend(chunk);
                }
                crate::write::sizing::InsertBucket::New => {
                    let file_id = crate::write::new_file_id();
                    let file_name = format!("{file_id}_0-0-0_{instant}.parquet");
                    new_group_inserts.push((partition.clone(), file_id, file_name, chunk));
                }
            }
        }
    }
    // Log-append targets: tagged updates plus packed inserts, per file group.
    let mut log_tasks: HashMap<(String, String), (Vec<u32>, Vec<u32>)> = HashMap::new();
    for (key, indices) in update_indices {
        log_tasks.entry(key).or_default().0.extend(indices);
    }
    for (key, indices) in packed_inserts {
        log_tasks.entry(key).or_default().1.extend(indices);
    }
    // (partition, file_id) -> (base file name, base instant) from latest slices,
    // for groups reachable only via packed inserts.
    let mut base_by_group: HashMap<(String, String), (String, String)> = HashMap::new();
    for (partition, slices) in &slices_by_partition {
        for slice in slices {
            base_by_group.insert(
                (partition.clone(), slice.file_id().to_string()),
                (
                    // Empty for log-only groups; the delta write stat's
                    // `baseFile` is empty in that case, matching Java.
                    slice
                        .base_file
                        .as_ref()
                        .map(|base_file| base_file.file_name())
                        .unwrap_or_default(),
                    slice.creation_instant_time().to_string(),
                ),
            );
        }
    }

    // Plan markers before any data file is put: new base files and (tv8+)
    // new log files are both CREATE markers.
    let mut planned_markers = Vec::new();
    for (partition_path, _, file_name, _) in &new_group_inserts {
        planned_markers.push(crate::write::markers::Marker::create(
            partition_path,
            file_name,
        ));
    }
    for (partition_path, file_id) in log_tasks.keys() {
        let log_name = format!(".{file_id}_{instant}.log.1_0-0-0");
        planned_markers.push(crate::write::markers::Marker::create(
            partition_path,
            &log_name,
        ));
    }
    crate::write::markers::write_markers(storage.as_ref(), &instant, &planned_markers).await?;

    let mut stats = Vec::new();
    let mut written_paths = Vec::<String>::new();
    let mut files_mdt = Vec::new();
    let mut rli_entries = Vec::new();
    let mut stats_updates = Vec::new();
    let props = crate::write::append::parquet_writer_props(table);
    let collect_ranges = is_column_stats_enabled(table);
    let parallelism = crate::write::write_task_parallelism(table);

    // New base file groups: prepare sequentially, encode + put in parallel.
    let mut insert_prepared: Vec<(usize, Vec<RecordBatch>, String)> = Vec::new();
    for (index, (partition_path, _, file_name, insert_indices)) in
        new_group_inserts.iter().enumerate()
    {
        let insert_batch = take_batch(&incoming, insert_indices)?;
        crate::write::ensure_partition_metadata(storage.as_ref(), partition_path, &instant).await?;
        let prepared =
            prepare_batches_for_write(&table.hudi_configs, &[insert_batch], &instant, file_name)?;
        let base_file_path = relative_data_path(partition_path, file_name);
        insert_prepared.push((index, prepared, base_file_path));
    }
    let insert_tasks = insert_prepared
        .iter()
        .map(|(_, prepared, path)| {
            crate::write::parquet_file_task(
                storage.clone(),
                props.clone(),
                prepared.clone(),
                path.clone(),
                collect_ranges,
            )
        })
        .collect();
    let insert_results = crate::write::run_write_tasks(insert_tasks, parallelism).await;
    let mut first_error = None;
    for ((index, _, path), result) in insert_prepared.iter().zip(insert_results) {
        let (partition_path, file_id, file_name, insert_indices) = &new_group_inserts[*index];
        match result {
            Ok(output) => {
                written_paths.push(path.clone());
                if let Some(ranges) = output.ranges {
                    stats_updates.push(crate::write::metadata::StatsFileUpdate {
                        partition_path: partition_path.clone(),
                        file_name: file_name.clone(),
                        is_deleted: false,
                        ranges,
                    });
                }
                files_mdt.push((
                    partition_path.clone(),
                    file_name.clone(),
                    output.size,
                    false,
                ));
                rli_entries.extend(insert_indices.iter().map(|row| {
                    let key = &tagged_keys[*row as usize];
                    RecordIndexEntry {
                        record_key: key.record_key.clone(),
                        partition_path: partition_path.clone(),
                        file_id: file_id.clone(),
                        instant_time_millis: instant_to_epoch_millis(&instant),
                        is_deleted: false,
                    }
                }));
                stats.push(HoodieWriteStat {
                    file_id: Some(file_id.clone()),
                    path: Some(path.clone()),
                    // Basename only — FileGroup builder joins partition_path + name.
                    base_file: Some(file_name.clone()),
                    prev_commit: Some("null".to_string()),
                    num_writes: Some(insert_indices.len() as i64),
                    num_inserts: Some(insert_indices.len() as i64),
                    total_write_bytes: Some(output.size),
                    file_size_in_bytes: Some(output.size),
                    partition_path: Some(partition_path.clone()),
                    ..Default::default()
                });
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

    // Log-append tasks: prepare sequentially, encode + wrap + put in parallel.
    struct LogTaskPlan {
        partition_path: String,
        file_id: String,
        log_name: String,
        log_file: String,
        base_basename: String,
        base_instant: String,
        update_count: usize,
        packed_count: usize,
        prepared: Vec<RecordBatch>,
    }
    let mut log_plans: Vec<LogTaskPlan> = Vec::new();
    for ((partition_path, file_id), (update_rows, packed_rows)) in log_tasks {
        let (base_basename, base_instant) = base_by_group
            .get(&(partition_path.clone(), file_id.clone()))
            .cloned()
            .ok_or_else(|| {
                CoreError::Write(format!(
                    "missing latest file slice for file group '{file_id}' in '{partition_path}'"
                ))
            })?;
        let log_name = format!(".{file_id}_{instant}.log.1_0-0-0");
        let log_file = relative_data_path(&partition_path, &log_name);
        crate::write::ensure_partition_metadata(storage.as_ref(), &partition_path, &instant)
            .await?;
        // Packed inserts are written as log records into the small slice and
        // must be registered in the RLI under this file group.
        rli_entries.extend(packed_rows.iter().map(|index| {
            let key = &tagged_keys[*index as usize];
            RecordIndexEntry {
                record_key: key.record_key.clone(),
                partition_path: partition_path.clone(),
                file_id: file_id.clone(),
                instant_time_millis: instant_to_epoch_millis(&instant),
                is_deleted: false,
            }
        }));
        let mut indices = update_rows.clone();
        indices.extend(&packed_rows);
        let update_batch = take_batch(&incoming, &indices)?;
        let prepared =
            prepare_batches_for_write(&table.hudi_configs, &[update_batch], &instant, &log_name)?;
        log_plans.push(LogTaskPlan {
            partition_path,
            file_id,
            log_name,
            log_file,
            base_basename,
            base_instant,
            update_count: update_rows.len(),
            packed_count: packed_rows.len(),
            prepared,
        });
    }
    let log_write_tasks = log_plans
        .iter()
        .map(|plan| -> Result<_> {
            let schema_json = crate::write::append::arrow_schema_to_avro_json(
                plan.prepared[0].schema().as_ref(),
            )?;
            Ok(crate::write::log_file_task(
                storage.clone(),
                props.clone(),
                plan.prepared.clone(),
                plan.log_file.clone(),
                instant.clone(),
                schema_json,
                collect_ranges,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let log_results = crate::write::run_write_tasks(log_write_tasks, parallelism).await;
    let mut first_error = None;
    for (plan, result) in log_plans.iter().zip(log_results) {
        match result {
            Ok(output) => {
                written_paths.push(plan.log_file.clone());
                // Java HoodieAppendHandle collects column ranges for log
                // appends; keyed by the log file name in column_stats.
                if let Some(ranges) = output.ranges {
                    stats_updates.push(crate::write::metadata::StatsFileUpdate {
                        partition_path: plan.partition_path.clone(),
                        file_name: plan.log_name.clone(),
                        is_deleted: false,
                        ranges,
                    });
                }
                files_mdt.push((
                    plan.partition_path.clone(),
                    plan.log_name.clone(),
                    output.size,
                    false,
                ));
                let total = (plan.update_count + plan.packed_count) as i64;
                stats.push(HoodieWriteStat {
                    file_id: Some(plan.file_id.clone()),
                    path: Some(plan.log_file.clone()),
                    base_file: Some(plan.base_basename.clone()),
                    log_files: Some(vec![plan.log_name.clone()]),
                    prev_commit: Some(plan.base_instant.clone()),
                    num_writes: Some(total),
                    num_update_writes: Some(plan.update_count as i64),
                    num_inserts: Some(plan.packed_count as i64),
                    total_write_bytes: Some(output.size),
                    file_size_in_bytes: Some(output.size),
                    total_log_records: Some(total),
                    total_log_files: Some(1),
                    total_log_blocks: Some(1),
                    partition_path: Some(plan.partition_path.clone()),
                    ..Default::default()
                });
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
    let mut mdt_stats: HashMap<String, Vec<HoodieWriteStat>> = HashMap::new();
    if table.is_metadata_table_enabled() {
        let mdt_result = async {
            if !files_mdt.is_empty() {
                mdt_stats.insert(
                    "files".to_string(),
                    update_files_partition_entries(storage.as_ref(), &instant, &files_mdt).await?,
                );
            }
            if is_record_index_enabled(table) && !rli_entries.is_empty() {
                let rli_stats =
                    update_record_index(storage.as_ref(), &instant, &rli_entries).await?;
                if !rli_stats.is_empty() {
                    mdt_stats.insert("record_index".to_string(), rli_stats);
                }
            }
            if is_column_stats_enabled(table) && !stats_updates.is_empty() {
                let stats_map = update_column_stats_partitions(
                    table,
                    &instant,
                    &stats_updates,
                    incoming.schema().as_ref(),
                    &[],
                )
                .await?;
                mdt_stats.extend(stats_map);
            }
            Ok::<(), CoreError>(())
        }
        .await;
        if let Err(error) = mdt_result {
            for path in written_paths {
                let _ = storage.delete_file(&path).await;
            }
            return Err(error);
        }
    }
    // Critical section 2: complete deltacommit + MDT, then bookkeeping.
    let lock = crate::write::lock::lock_provider_for(table);
    let cs2 = lock.lock().await?;
    if table.is_metadata_table_enabled()
        && let Err(error) =
            crate::write::metadata::write_metadata_commit(storage.as_ref(), &instant, mdt_stats)
                .await
    {
        for path in written_paths {
            let _ = storage.delete_file(&path).await;
        }
        return Err(error);
    }
    if let Err(error) =
        complete_deltacommit(table, &instant, "UPSERT", stats, incoming.schema().as_ref()).await
    {
        for path in written_paths {
            let _ = storage.delete_file(&path).await;
        }
        return Err(error);
    }
    crate::write::post_complete_bookkeeping(table, storage.as_ref(), &instant).await?;
    drop(cs2);
    table.timeline.reload_completed_commits().await?;
    table.file_system_view.clear_cache();
    Ok(WriteResult {
        instant,
        num_writes: incoming.num_rows(),
        num_updates: updates,
        num_inserts: inserts,
        num_deletes: 0,
    })
}

async fn mor_update_filter(
    table: &mut Table,
    filter: Filter,
    updates: RecordBatch,
) -> Result<WriteResult> {
    ensure_mor_merge_supported(table)?;
    ensure_configured_record_key(table)?;
    let Some((old, _, _, _)) = data_for_filter_matches(table, &filter).await? else {
        return Ok(WriteResult::default());
    };
    let mask = filters_to_row_mask(&[filter], &old)?;
    let (merged, num_updates) = apply_set_updates(&old, &mask, &updates)?;
    if num_updates == 0 {
        return Ok(WriteResult::default());
    }
    let matched_indices = mask
        .iter()
        .enumerate()
        .filter_map(|(index, matches)| matches.unwrap_or(false).then_some(index as u32))
        .collect::<Vec<_>>();
    let updated_rows = take_batch(&merged, &matched_indices)?;
    // Drop meta so prepare_batches_for_write stamps the new commit instant.
    let data_only = strip_meta_columns(&updated_rows)?;
    mor_upsert_batches(table, &[data_only], UpsertOptions::default()).await
}

async fn mor_delete_keys(table: &mut Table, delete_keys: &[HoodieKey]) -> Result<WriteResult> {
    ensure_mor_merge_supported(table)?;
    if delete_keys.is_empty() {
        return Err(CoreError::Write(
            "delete requires at least one record key".to_string(),
        ));
    }
    let locations = mor_file_locations(table, delete_keys).await?;
    let mut grouped: HashMap<(String, String), Vec<HoodieKey>> = HashMap::new();
    let mut seen = std::collections::HashSet::new();
    for key in delete_keys {
        if seen.insert(&key.record_key)
            && let Some(location) = locations.get(&key.record_key)
        {
            grouped
                .entry((location.partition_path.clone(), location.file_id.clone()))
                .or_default()
                .push(key.clone());
        }
    }
    if grouped.is_empty() {
        return Ok(WriteResult::default());
    }

    let instant = request_deltacommit(table, "DELETE").await?;
    let storage = table.file_system_view.storage.clone();
    // Markers for the delete log blocks before any data file is put.
    let mut planned_markers = Vec::new();
    for (partition_path, file_id) in grouped.keys() {
        let log_name = format!(".{file_id}_{instant}.log.1_0-0-0");
        planned_markers.push(crate::write::markers::Marker::create(
            partition_path,
            &log_name,
        ));
    }
    crate::write::markers::write_markers(storage.as_ref(), &instant, &planned_markers).await?;

    let mut stats = Vec::new();
    let mut written_paths = Vec::<String>::new();
    let mut files_mdt = Vec::new();
    let mut deleted = 0;
    for ((partition_path, file_id), keys) in grouped {
        let first_key = keys
            .first()
            .ok_or_else(|| CoreError::Write("missing delete key".to_string()))?;
        let location = locations
            .get(&first_key.record_key)
            .ok_or_else(|| CoreError::Write("missing file location for delete key".to_string()))?;
        let log_name = format!(".{file_id}_{instant}.log.1_0-0-0");
        let log_file = relative_data_path(&partition_path, &log_name);
        crate::write::ensure_partition_metadata(storage.as_ref(), &partition_path, &instant)
            .await?;
        let content = {
            let pairs: Vec<(String, String)> = keys
                .iter()
                .map(|key| (key.record_key.clone(), key.partition_path.clone()))
                .collect();
            // orderingVal=0: Java commit-time / unknown-ordering fallback (not i64::MAX).
            crate::write::build_delete_log_block(&instant, &pairs, 0)?
        };
        let size = content.len() as i64;
        if let Err(error) = storage.put_file(&log_file, content).await {
            for path in written_paths {
                let _ = storage.delete_file(&path).await;
            }
            return Err(error.into());
        }
        written_paths.push(log_file.clone());
        deleted += keys.len();
        files_mdt.push((partition_path.clone(), log_name.clone(), size, false));
        // Empty when the file group is log-only: Java's delta write stat leaves
        // `baseFile` empty in that case and the file-group builder treats it as
        // an unattached log file.
        let base_basename = location
            .base_file_path
            .as_deref()
            .map(|path| {
                std::path::Path::new(path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(path)
                    .to_string()
            })
            .unwrap_or_default();
        stats.push(HoodieWriteStat {
            file_id: Some(file_id),
            path: Some(log_file.clone()),
            base_file: Some(base_basename),
            log_files: Some(vec![log_name]),
            prev_commit: Some(location.base_instant.clone()),
            num_writes: Some(keys.len() as i64),
            num_deletes: Some(keys.len() as i64),
            total_write_bytes: Some(size),
            file_size_in_bytes: Some(size),
            total_log_records: Some(keys.len() as i64),
            total_log_files: Some(1),
            total_log_blocks: Some(1),
            partition_path: Some(partition_path.clone()),
            ..Default::default()
        });
    }
    let mut mdt_stats: HashMap<String, Vec<HoodieWriteStat>> = HashMap::new();
    if table.is_metadata_table_enabled() {
        let mdt_result = async {
            if !files_mdt.is_empty() {
                mdt_stats.insert(
                    "files".to_string(),
                    update_files_partition_entries(storage.as_ref(), &instant, &files_mdt).await?,
                );
            }
            if is_record_index_enabled(table) {
                let entries = delete_keys
                    .iter()
                    .filter(|key| locations.contains_key(&key.record_key))
                    .map(|key| RecordIndexEntry {
                        record_key: key.record_key.clone(),
                        partition_path: key.partition_path.clone(),
                        file_id: String::new(),
                        instant_time_millis: instant_to_epoch_millis(&instant),
                        is_deleted: true,
                    })
                    .collect::<Vec<_>>();
                if !entries.is_empty() {
                    let rli_stats =
                        update_record_index(storage.as_ref(), &instant, &entries).await?;
                    if !rli_stats.is_empty() {
                        mdt_stats.insert("record_index".to_string(), rli_stats);
                    }
                }
            }
            Ok::<(), CoreError>(())
        }
        .await;
        if let Err(error) = mdt_result {
            for path in written_paths {
                let _ = storage.delete_file(&path).await;
            }
            return Err(error);
        }
    }
    // Critical section 2: complete deltacommit + MDT, then bookkeeping.
    let lock = crate::write::lock::lock_provider_for(table);
    let cs2 = lock.lock().await?;
    if table.is_metadata_table_enabled()
        && let Err(error) =
            crate::write::metadata::write_metadata_commit(storage.as_ref(), &instant, mdt_stats)
                .await
    {
        for path in written_paths {
            let _ = storage.delete_file(&path).await;
        }
        return Err(error);
    }
    if let Err(error) = complete_deltacommit(
        table,
        &instant,
        "DELETE",
        stats,
        table.get_schema_with_meta_fields().await?.as_ref(),
    )
    .await
    {
        for path in written_paths {
            let _ = storage.delete_file(&path).await;
        }
        return Err(error);
    }
    crate::write::post_complete_bookkeeping(table, storage.as_ref(), &instant).await?;
    drop(cs2);
    table.timeline.reload_completed_commits().await?;
    table.file_system_view.clear_cache();
    Ok(WriteResult {
        instant,
        num_writes: 0,
        num_updates: 0,
        num_inserts: 0,
        num_deletes: deleted,
    })
}

async fn complete_deltacommit(
    table: &Table,
    instant: &str,
    operation: &str,
    stats: Vec<HoodieWriteStat>,
    schema: &arrow_schema::Schema,
) -> Result<()> {
    let storage = table.file_system_view.storage.clone();
    let timeline = timeline_dir(table);
    // Fencing happened at write start (before data files); this only completes.
    let mut partition_to_write_stats = HashMap::<String, Vec<HoodieWriteStat>>::new();
    for stat in stats {
        let partition = stat.partition_path.clone().unwrap_or_default();
        partition_to_write_stats
            .entry(partition)
            .or_default()
            .push(stat);
    }
    let metadata = HoodieCommitMetadata {
        version: Some(1),
        operation_type: Some(operation.to_string()),
        partition_to_write_stats: Some(partition_to_write_stats),
        compacted: Some(false),
        extra_metadata: Some(HashMap::from([(
            "schema".to_string(),
            // Commit metadata carries the data schema (no _hoodie_* meta
            // fields), matching the Java writer; some callers pass batches
            // that were read back with meta fields.
            crate::write::append::arrow_schema_to_avro_json(
                &crate::write::append::strip_meta_fields_from_schema(schema),
            )?,
        )])),
    };
    let layout_two = is_layout_two(table);
    let completed = if layout_two {
        Some(generate_instant_time().await)
    } else {
        None
    };
    let commit = Instant::new_completed(instant.to_string(), Action::DeltaCommit, completed)?;
    let path = commit.relative_path_with_base(&timeline)?;
    let bytes = if layout_two {
        metadata.to_avro_bytes()?
    } else {
        metadata.to_json_bytes()?
    };
    storage.put_file_if_absent(&path, bytes).await?;
    Ok(())
}

fn uses_event_time_merge(hudi_configs: &crate::config::HudiConfigs) -> Result<bool> {
    let strategy: String = hudi_configs.get_or_default(RecordMergeStrategy).into();
    let strategy = RecordMergeStrategyValue::from_str(&strategy)?;
    Ok(strategy == RecordMergeStrategyValue::OverwriteWithLatest
        && hudi_configs.try_get(OrderingFields)?.is_some())
}

fn merge_with_event_time(
    hudi_configs: std::sync::Arc<crate::config::HudiConfigs>,
    old: &RecordBatch,
    incoming: &RecordBatch,
    combined: &RecordBatch,
) -> Result<Vec<u32>> {
    let merger = RecordMerger::new(old.schema(), hudi_configs);
    let merged = merger.merge_record_batches(RecordBatches::new_with_data_batches([
        old.clone(),
        incoming.clone(),
    ]))?;
    selected_indices_for_merged(combined, &merged)
}

fn selected_indices_for_merged(combined: &RecordBatch, merged: &RecordBatch) -> Result<Vec<u32>> {
    let combined_keys = keys(combined, MetaField::RecordKey.as_ref())?;
    let combined_seqnos = string_values(combined, MetaField::CommitSeqno.as_ref())?;
    let mut source_indices = HashMap::with_capacity(combined.num_rows());
    for (index, (key, seqno)) in combined_keys.iter().zip(combined_seqnos.iter()).enumerate() {
        source_indices.insert((key.clone(), seqno.clone()), index as u32);
    }

    let merged_keys = keys(merged, MetaField::RecordKey.as_ref())?;
    let merged_seqnos = string_values(merged, MetaField::CommitSeqno.as_ref())?;
    merged_keys
        .iter()
        .zip(merged_seqnos.iter())
        .map(|(key, seqno)| {
            source_indices.get(&(key.clone(), seqno.clone())).copied().ok_or_else(|| {
                CoreError::Write(format!(
                    "event-time merger returned unknown record '{key}' with commit sequence '{seqno}'"
                ))
            })
        })
        .collect()
}

/// Load only the file groups holding rows that match `filter`, with per-row
/// origin file ids parallel to the concatenated batch (P1-2: expression
/// update/delete must rewrite the affected groups, not the whole table).
///
/// Scans slice by slice, so peak memory holds the affected groups plus one
/// transient slice. `filter` is validated against the first non-empty slice's
/// schema. Base paths are `None` for log-only MOR groups; COW callers convert
/// that to an error. Returns `Ok(None)` when no row matches (no commit).
#[allow(clippy::type_complexity)]
async fn data_for_filter_matches(
    table: &Table,
    filter: &Filter,
) -> Result<Option<(RecordBatch, Vec<String>, Vec<Option<String>>, Vec<String>)>> {
    let slices = table.get_file_slices(&ReadOptions::new()).await?;
    let reader = table
        .create_file_group_reader_with_options(
            Some(&ReadOptions::new()),
            std::iter::empty::<(&str, String)>(),
        )
        .await?;
    let mut batches = Vec::new();
    let mut kept_ids = Vec::new();
    let mut kept_paths = Vec::new();
    let mut row_file_ids = Vec::new();
    let mut validated = false;
    for slice in slices {
        let batch = reader.read_file_slice(&slice, &ReadOptions::new()).await?;
        if batch.num_rows() == 0 {
            continue;
        }
        if !validated {
            validate_fields_against_schemas(
                std::slice::from_ref(filter),
                [batch.schema().as_ref()],
            )?;
            validated = true;
        }
        let mask = filters_to_row_mask(std::slice::from_ref(filter), &batch)?;
        if !mask.iter().any(|m| m.unwrap_or(false)) {
            continue;
        }
        kept_ids.push(slice.file_id().to_string());
        kept_paths.push(slice.base_file_relative_path()?);
        row_file_ids.extend(std::iter::repeat_n(
            slice.file_id().to_string(),
            batch.num_rows(),
        ));
        batches.push(batch);
    }
    if batches.is_empty() {
        return Ok(None);
    }
    Ok(Some((
        concat(&batches)?,
        kept_ids,
        kept_paths,
        row_file_ids,
    )))
}

fn concat(batches: &[RecordBatch]) -> Result<RecordBatch> {
    let first = batches
        .first()
        .ok_or_else(|| CoreError::Write("no batches to concatenate".to_string()))?;
    concat_batches(&first.schema(), batches).map_err(Into::into)
}

fn record_key_name(table: &Table, batch: &RecordBatch) -> Result<String> {
    if batch
        .column_by_name(MetaField::RecordKey.as_ref())
        .is_some()
    {
        return Ok(MetaField::RecordKey.as_ref().to_string());
    }
    let fields: Vec<String> = table
        .hudi_configs
        .try_get(RecordKeyFields)?
        .map(Into::into)
        .unwrap_or_default();
    if fields.len() != 1 || fields[0].is_empty() {
        return Err(CoreError::Unsupported(
            "upsert/delete require a record key field (or _hoodie_record_key in the batch); auto-generated keys are insert-only"
                .to_string(),
        ));
    }
    Ok(fields[0].clone())
}

fn configured_record_key_field(table: &Table) -> Result<Option<String>> {
    let fields: Vec<String> = table
        .hudi_configs
        .try_get(RecordKeyFields)?
        .map(Into::into)
        .unwrap_or_default();
    if fields.len() == 1 && !fields[0].is_empty() {
        Ok(Some(fields[0].clone()))
    } else {
        Ok(None)
    }
}

/// If `filter` is `=` / `IN` on the configured record key or `_hoodie_record_key`,
/// return keys for the RLI/SimpleIndex delete path.
fn keys_from_record_key_filter(table: &Table, filter: &Filter) -> Result<Option<Vec<HoodieKey>>> {
    let configured = configured_record_key_field(table)?;
    let is_key_field = filter.field == MetaField::RecordKey.as_ref()
        || configured.as_deref() == Some(filter.field.as_str());
    if !is_key_field {
        return Ok(None);
    }
    match filter.operator {
        ExprOperator::Eq | ExprOperator::In => Ok(Some(
            filter
                .values
                .iter()
                .map(|record_key| HoodieKey {
                    record_key: record_key.clone(),
                    partition_path: String::new(),
                })
                .collect(),
        )),
        _ => Ok(None),
    }
}

fn partition_paths_for_batch(batch: &RecordBatch) -> Vec<String> {
    match batch.column_by_name(MetaField::PartitionPath.as_ref()) {
        Some(column) => column
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|array| {
                (0..array.len())
                    .map(|i| {
                        if array.is_null(i) {
                            String::new()
                        } else {
                            array.value(i).to_string()
                        }
                    })
                    .collect()
            })
            .unwrap_or_else(|| vec![String::new(); batch.num_rows()]),
        None => vec![String::new(); batch.num_rows()],
    }
}

fn strip_meta_columns(batch: &RecordBatch) -> Result<RecordBatch> {
    let meta: HashSet<&str> = MetaField::field_names_with_operation()
        .into_iter()
        .collect();
    let fields: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .filter(|f| !meta.contains(f.name().as_str()))
        .cloned()
        .collect();
    let columns: Vec<ArrayRef> = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .filter(|(f, _)| !meta.contains(f.name().as_str()))
        .map(|(_, c)| c.clone())
        .collect();
    RecordBatch::try_new(
        std::sync::Arc::new(arrow_schema::Schema::new(fields)),
        columns,
    )
    .map_err(Into::into)
}

/// Overlay single-row SET values onto rows where `matched` is true.
fn apply_set_updates(
    old: &RecordBatch,
    matched: &BooleanArray,
    updates: &RecordBatch,
) -> Result<(RecordBatch, usize)> {
    if updates.num_rows() != 1 {
        return Err(CoreError::Write(
            "update requires a single-row RecordBatch of SET values".to_string(),
        ));
    }
    let meta: HashSet<&str> = MetaField::field_names_with_operation()
        .into_iter()
        .collect();
    let mut set_names = Vec::new();
    for field in updates.schema().fields() {
        let name = field.name();
        if meta.contains(name.as_str()) {
            continue;
        }
        if old.column_by_name(name).is_none() {
            return Err(CoreError::Schema(format!(
                "update column '{name}' is not in the table schema"
            )));
        }
        set_names.push(name.clone());
    }
    if set_names.is_empty() {
        return Err(CoreError::Write(
            "update requires at least one data column to set".to_string(),
        ));
    }
    let num_updates = matched.iter().filter(|v| v.unwrap_or(false)).count();
    if num_updates == 0 {
        return Ok((old.clone(), 0));
    }
    let zeros = UInt32Array::from(vec![0u32; old.num_rows()]);
    let mut columns = Vec::with_capacity(old.num_columns());
    for field in old.schema().fields() {
        let old_col = old
            .column_by_name(field.name())
            .ok_or_else(|| CoreError::Schema(format!("missing column '{}'", field.name())))?;
        if set_names.iter().any(|n| n == field.name()) {
            let update_col = updates.column_by_name(field.name()).ok_or_else(|| {
                CoreError::Schema(format!("missing update column '{}'", field.name()))
            })?;
            if update_col.data_type() != old_col.data_type() {
                return Err(CoreError::Schema(format!(
                    "update column '{}' type {:?} does not match table type {:?}",
                    field.name(),
                    update_col.data_type(),
                    old_col.data_type()
                )));
            }
            let broadcast = take(update_col.as_ref(), &zeros, None)?;
            columns.push(zip(matched, &broadcast, old_col)?);
        } else {
            columns.push(old_col.clone());
        }
    }
    Ok((RecordBatch::try_new(old.schema(), columns)?, num_updates))
}

/// Upsert/delete need a configured `hoodie.table.recordkey.fields` (Java auto keys are insert-only).
fn ensure_configured_record_key(table: &Table) -> Result<()> {
    let fields: Vec<String> = table
        .hudi_configs
        .try_get(RecordKeyFields)?
        .map(Into::into)
        .unwrap_or_default();
    if fields.len() != 1 || fields[0].is_empty() {
        return Err(CoreError::Unsupported(
            "upsert/delete require hoodie.table.recordkey.fields; auto-generated keys are insert-only"
                .to_string(),
        ));
    }
    Ok(())
}

fn keys(batch: &RecordBatch, field: &str) -> Result<Vec<String>> {
    string_values(batch, field)
}

/// Keep the final occurrence of each key, in original relative order.
fn deduplicate_last_by_key(batch: &RecordBatch, field: &str) -> Result<RecordBatch> {
    let mut last_indices = HashMap::with_capacity(batch.num_rows());
    for (index, key) in keys(batch, field)?.into_iter().enumerate() {
        last_indices.insert(key, index as u32);
    }
    let mut indices = last_indices.into_values().collect::<Vec<_>>();
    indices.sort_unstable();
    take_batch(batch, &indices)
}

fn string_values(batch: &RecordBatch, field: &str) -> Result<Vec<String>> {
    let array = batch
        .column_by_name(field)
        .ok_or_else(|| CoreError::Schema(format!("field '{field}' is missing")))?;
    let array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| CoreError::Unsupported(format!("field '{field}' must be Utf8")))?;
    array
        .iter()
        .map(|value| {
            value
                .map(str::to_string)
                .ok_or_else(|| CoreError::Write(format!("field '{field}' cannot be null")))
        })
        .collect()
}

fn take_batch(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
    let indices = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None).map_err(Into::into))
        .collect::<Result<Vec<ArrayRef>>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(Into::into)
}

fn partial_merge(
    combined: &RecordBatch,
    old_by_key: &HashMap<String, u32>,
    selected: &[u32],
    update_columns: &[String],
) -> Result<RecordBatch> {
    let key_column = if combined
        .column_by_name(MetaField::RecordKey.as_ref())
        .is_some()
    {
        MetaField::RecordKey.as_ref()
    } else {
        return Err(CoreError::Unsupported(
            "partial updates require populated Hudi meta fields".to_string(),
        ));
    };
    let combined_keys = keys(combined, key_column)?;
    let update_columns = update_columns
        .iter()
        .collect::<std::collections::HashSet<_>>();
    let mut columns = Vec::with_capacity(combined.num_columns());
    for field in combined.schema().fields() {
        let indices = selected
            .iter()
            .map(|index| {
                let key = &combined_keys[*index as usize];
                if update_columns.contains(field.name()) || !old_by_key.contains_key(key) {
                    *index
                } else {
                    *old_by_key.get(key).unwrap_or(index)
                }
            })
            .collect::<Vec<_>>();
        columns.push(take(
            combined
                .column_by_name(field.name())
                .ok_or_else(|| CoreError::Schema(format!("missing column '{}'", field.name())))?
                .as_ref(),
            &UInt32Array::from(indices),
            None,
        )?);
    }
    RecordBatch::try_new(combined.schema(), columns).map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
async fn rewrite(
    table: &mut Table,
    instant: &str,
    batch: &RecordBatch,
    row_targets: Option<Vec<String>>,
    replaced_file_ids: Vec<String>,
    old_paths: Vec<String>,
    operation: &str,
    updates: usize,
    inserts: usize,
    deletes: usize,
    mut rli_deletes: Vec<RecordIndexEntry>,
    kind: RewriteKind,
) -> Result<WriteResult> {
    let storage = table.file_system_view.storage.clone();
    let action = match kind {
        RewriteKind::Commit => Action::Commit,
        RewriteKind::Replace => Action::ReplaceCommit,
    };
    // The instant was already requested (fenced) under critical section 1 by
    // request_rewrite_instant in the calling verb.

    // Per-file-id prev commit / per-partition replaced ids.
    let mut prev_commit_by_file_id = HashMap::<String, String>::new();
    let mut partition_by_file_id = HashMap::<String, String>::new();
    let mut partition_to_replace_file_ids = HashMap::<String, Vec<String>>::new();
    for (file_id, old_path) in replaced_file_ids.iter().zip(&old_paths) {
        let partition = old_path
            .rsplit_once('/')
            .map(|(partition, _)| partition.to_string())
            .unwrap_or_default();
        prev_commit_by_file_id.insert(file_id.clone(), prev_commit_from_base_path(old_path));
        partition_by_file_id.insert(file_id.clone(), partition.clone());
        if kind == RewriteKind::Replace {
            partition_to_replace_file_ids
                .entry(partition)
                .or_default()
                .push(file_id.clone());
        }
    }

    // Group planning: rows with a named target rewrite that file group (same
    // fileId, MERGE marker); untargeted rows form NEW file groups, split into
    // buckets of ~max.file.size records (Java UpsertPartitioner overflow /
    // insert-overwrite sizing).
    struct GroupPlan {
        partition: String,
        file_id: String,
        out_name: String,
        existing: bool,
        rows: Vec<u32>,
    }
    let mut groups: Vec<GroupPlan> = Vec::new();
    if batch.num_rows() > 0 {
        let row_keys = hoodie_keys_for_batch(&table.hudi_configs, batch, Some(instant))?;
        let mut existing_groups: HashMap<String, usize> = HashMap::new();
        let mut new_rows_by_partition: HashMap<String, Vec<u32>> = HashMap::new();
        for (row, key) in row_keys.iter().enumerate() {
            let target = if kind == RewriteKind::Replace {
                ""
            } else {
                row_targets
                    .as_ref()
                    .and_then(|t| t.get(row).map(String::as_str))
                    .unwrap_or("")
            };
            if target.is_empty() {
                new_rows_by_partition
                    .entry(key.partition_path.clone())
                    .or_default()
                    .push(row as u32);
            } else {
                let index = *existing_groups
                    .entry(target.to_string())
                    .or_insert_with(|| {
                        let out_name = format!("{target}_0-0-0_{instant}.parquet");
                        groups.push(GroupPlan {
                            partition: key.partition_path.clone(),
                            file_id: target.to_string(),
                            out_name,
                            existing: true,
                            rows: Vec::new(),
                        });
                        groups.len() - 1
                    });
                groups[index].rows.push(row as u32);
            }
        }
        // Untargeted rows: new file groups sized by the insert bucket size.
        let sizing = crate::write::sizing::SizingConfig::from_table(table);
        let avg = crate::write::sizing::average_record_size(table, storage.as_ref(), &sizing).await;
        let per_bucket = sizing.insert_records_per_bucket(avg);
        let mut sorted_new: Vec<_> = new_rows_by_partition.into_iter().collect();
        sorted_new.sort_by(|a, b| a.0.cmp(&b.0));
        for (partition, rows) in sorted_new {
            for chunk in rows.chunks(per_bucket.max(1)) {
                let file_id = crate::write::new_file_id();
                let out_name = format!("{file_id}_0-0-0_{instant}.parquet");
                groups.push(GroupPlan {
                    partition: partition.clone(),
                    file_id,
                    out_name,
                    existing: false,
                    rows: chunk.to_vec(),
                });
            }
        }
    }
    if kind == RewriteKind::Commit {
        // Any affected file group left with zero surviving rows (a delete
        // emptied it) still needs an empty base file: without a new slice
        // the old base stays the group's latest version and deleted rows
        // resurface — especially once the deleting commit is archived.
        let planned: HashSet<String> = groups.iter().map(|g| g.file_id.clone()).collect();
        for file_id in &replaced_file_ids {
            if planned.contains(file_id) {
                continue;
            }
            let partition = partition_by_file_id
                .get(file_id)
                .cloned()
                .unwrap_or_default();
            let out_name = format!("{file_id}_0-0-0_{instant}.parquet");
            groups.push(GroupPlan {
                partition,
                file_id: file_id.clone(),
                out_name,
                existing: true,
                rows: Vec::new(),
            });
        }
    }

    let mut additions = Vec::new();
    let mut partition_to_write_stats = HashMap::<String, Vec<HoodieWriteStat>>::new();
    let mut written_paths = Vec::new();
    let mut row_group_file_ids: HashMap<u32, String> = HashMap::new();
    let mut stats_updates = Vec::new();

    // Markers before any data file is put: MERGE for same-file-group rewrites,
    // CREATE for new groups.
    let mut planned_markers = Vec::new();
    for group in &groups {
        planned_markers.push(if group.existing {
            crate::write::markers::Marker::merge(&group.partition, &group.out_name)
        } else {
            crate::write::markers::Marker::create(&group.partition, &group.out_name)
        });
    }
    crate::write::markers::write_markers(storage.as_ref(), instant, &planned_markers).await?;

    // Phase 1 (sequential): slice + prepare each group.
    let mut group_prepared: Vec<(usize, Vec<RecordBatch>, String)> = Vec::new();
    for (index, group) in groups.iter().enumerate() {
        let partition_batch = if group.rows.is_empty() {
            RecordBatch::new_empty(batch.schema())
        } else {
            take_batch(batch, &group.rows)?
        };
        for row in &group.rows {
            row_group_file_ids.insert(*row, group.file_id.clone());
        }
        crate::write::ensure_partition_metadata(storage.as_ref(), &group.partition, instant)
            .await?;
        let prepared = prepare_batches_for_write(
            &table.hudi_configs,
            std::slice::from_ref(&partition_batch),
            instant,
            &group.out_name,
        )?;
        let path = relative_data_path(&group.partition, &group.out_name);
        group_prepared.push((index, prepared, path));
    }

    // Phase 2 (parallel): encode + put on the write task pool.
    let props = crate::write::append::parquet_writer_props(table);
    let collect_ranges = is_column_stats_enabled(table);
    let tasks = group_prepared
        .iter()
        .map(|(_, prepared, path)| {
            crate::write::parquet_file_task(
                storage.clone(),
                props.clone(),
                prepared.clone(),
                path.clone(),
                collect_ranges,
            )
        })
        .collect();
    let results =
        crate::write::run_write_tasks(tasks, crate::write::write_task_parallelism(table)).await;

    // Phase 3 (sequential): assemble in plan order; clean up on error.
    let mut first_error = None;
    for ((index, prepared, path), result) in group_prepared.iter().zip(results) {
        let group = &groups[*index];
        match result {
            Ok(output) => {
                written_paths.push(path.clone());
                if let Some(ranges) = output.ranges {
                    stats_updates.push(crate::write::metadata::StatsFileUpdate {
                        partition_path: group.partition.clone(),
                        file_name: group.out_name.clone(),
                        is_deleted: false,
                        ranges,
                    });
                }
                additions.push((
                    group.partition.clone(),
                    group.out_name.clone(),
                    output.size,
                    false,
                ));
                let prev_commit = prev_commit_by_file_id
                    .get(&group.file_id)
                    .cloned()
                    .unwrap_or_else(|| "null".to_string());
                let num_rows: usize = prepared.iter().map(RecordBatch::num_rows).sum();
                partition_to_write_stats
                    .entry(group.partition.clone())
                    .or_default()
                    .push(HoodieWriteStat {
                        file_id: Some(group.file_id.clone()),
                        path: Some(path.clone()),
                        base_file: Some(group.out_name.clone()),
                        prev_commit: Some(prev_commit),
                        num_writes: Some(num_rows as i64),
                        num_deletes: Some(deletes as i64),
                        num_update_writes: Some(updates as i64),
                        num_inserts: Some(inserts as i64),
                        total_write_bytes: Some(output.size),
                        file_size_in_bytes: Some(output.size),
                        partition_path: Some(group.partition.clone()),
                        ..Default::default()
                    });
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

    let rli_entries = {
        let mut entries = if batch.num_rows() > 0 && is_record_index_enabled(table) {
            hoodie_keys_for_batch(&table.hudi_configs, batch, Some(instant))?
                .into_iter()
                .enumerate()
                .filter_map(|(row, key)| {
                    let file_id = row_group_file_ids.get(&(row as u32))?.clone();
                    Some(RecordIndexEntry {
                        record_key: key.record_key,
                        partition_path: key.partition_path,
                        file_id,
                        instant_time_millis: instant_to_epoch_millis(instant),
                        is_deleted: false,
                    })
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        entries.append(&mut rli_deletes);
        entries
    };
    finalize_rewrite_commit(
        table,
        instant,
        kind,
        action,
        operation,
        batch.schema(),
        partition_to_write_stats,
        partition_to_replace_file_ids,
        additions,
        written_paths,
        stats_updates,
        &old_paths,
        rli_entries,
        batch.num_rows(),
        updates,
        inserts,
        deletes,
    )
    .await
}

/// Shared commit tail for COW rewrites: commit-metadata encode, MDT updates
/// (files / record_index / column stats), then critical section 2 (MDT
/// deltacommit -> completed instant -> bookkeeping). `rli_entries` must be
/// fully computed by the caller (adds and tombstones).
#[allow(clippy::too_many_arguments)]
async fn finalize_rewrite_commit(
    table: &mut Table,
    instant: &str,
    kind: RewriteKind,
    action: Action,
    operation: &str,
    schema: arrow_schema::SchemaRef,
    partition_to_write_stats: HashMap<String, Vec<HoodieWriteStat>>,
    partition_to_replace_file_ids: HashMap<String, Vec<String>>,
    additions: Vec<(String, String, i64, bool)>,
    written_paths: Vec<String>,
    stats_updates: Vec<crate::write::metadata::StatsFileUpdate>,
    old_paths: &[String],
    rli_entries: Vec<RecordIndexEntry>,
    num_writes: usize,
    updates: usize,
    inserts: usize,
    deletes: usize,
) -> Result<WriteResult> {
    let storage = table.file_system_view.storage.clone();
    // Data schema only (no _hoodie_* meta fields) in commit metadata, like
    // the Java writer; delete/update paths pass batches read with meta fields.
    let schema_json = crate::write::append::arrow_schema_to_avro_json(
        &crate::write::append::strip_meta_fields_from_schema(schema.as_ref()),
    )?;
    let layout_two = is_layout_two(table);
    let bytes = match kind {
        RewriteKind::Replace => {
            let metadata = crate::metadata::replace_commit::HoodieReplaceCommitMetadata {
                version: Some(1),
                operation_type: Some(operation.to_string()),
                partition_to_write_stats: Some(partition_to_write_stats),
                compacted: Some(false),
                extra_metadata: Some(HashMap::from([("schema".to_string(), schema_json)])),
                partition_to_replace_file_ids: Some(partition_to_replace_file_ids),
            };
            if layout_two {
                metadata.to_avro_bytes()?
            } else {
                metadata.to_json_bytes()?
            }
        }
        RewriteKind::Commit => {
            let metadata = HoodieCommitMetadata {
                version: Some(1),
                operation_type: Some(operation.to_string()),
                partition_to_write_stats: Some(partition_to_write_stats),
                compacted: Some(false),
                extra_metadata: Some(HashMap::from([("schema".to_string(), schema_json)])),
            };
            if layout_two {
                metadata.to_avro_bytes()?
            } else {
                metadata.to_json_bytes()?
            }
        }
    };

    // Crash-consistency: MDT files + MDT deltacommit before completed data commit.
    let mut mdt_stats: HashMap<String, Vec<HoodieWriteStat>> = HashMap::new();
    if table.is_metadata_table_enabled() {
        let mdt_result = async {
            // Replaced bases stay listed in MDT files/column_stats until clean,
            // like Java: readers exclude replaced file groups via replacecommit
            // metadata, and deleting here would break time-travel before clean.
            if !additions.is_empty() {
                mdt_stats.insert(
                    "files".to_string(),
                    update_files_partition_entries(storage.as_ref(), instant, &additions).await?,
                );
            }
            if is_record_index_enabled(table) {
                let entries = rli_entries;
                if !entries.is_empty() {
                    let stats = update_record_index(storage.as_ref(), instant, &entries).await?;
                    if !stats.is_empty() {
                        mdt_stats.insert("record_index".to_string(), stats);
                    }
                }
            }
            if is_column_stats_enabled(table) && !stats_updates.is_empty() {
                // Replaced/rewritten bases are excluded from the tight-bound
                // partition_stats scan; survivors come from latest file slices.
                let stats = update_column_stats_partitions(
                    table,
                    instant,
                    &stats_updates,
                    schema.as_ref(),
                    old_paths,
                )
                .await?;
                mdt_stats.extend(stats);
            }
            Ok::<(), CoreError>(())
        }
        .await;
        if let Err(error) = mdt_result {
            for path in written_paths {
                let _ = storage.delete_file(&path).await;
            }
            return Err(error);
        }
    }

    // Critical section 2: complete the action, then bookkeeping under lock.
    let lock = crate::write::lock::lock_provider_for(table);
    let cs2 = lock.lock().await?;
    if table.is_metadata_table_enabled()
        && let Err(error) =
            crate::write::metadata::write_metadata_commit(storage.as_ref(), instant, mdt_stats)
                .await
    {
        for path in written_paths {
            let _ = storage.delete_file(&path).await;
        }
        return Err(error);
    }
    let completed = if layout_two {
        Some(generate_instant_time().await)
    } else {
        None
    };
    let commit = Instant::new_completed(instant.to_string(), action, completed)?;
    let path = commit.relative_path_with_base(&timeline_dir(table))?;
    if let Err(error) = storage.put_file_if_absent(&path, bytes).await {
        for path in written_paths {
            let _ = storage.delete_file(&path).await;
        }
        return Err(error.into());
    }
    crate::write::post_complete_bookkeeping(table, storage.as_ref(), instant).await?;
    drop(cs2);
    table.timeline.reload_completed_commits().await?;
    table.file_system_view.clear_cache();
    Ok(WriteResult {
        instant: instant.to_string(),
        num_writes,
        num_updates: updates,
        num_inserts: inserts,
        num_deletes: deletes,
    })
}

/// Context a merge worker needs, cloneable and `'static` (MergeHandle model:
/// the driver stays at metadata scale; each worker reads, merges, stamps,
/// encodes, and writes ONE file group).
#[derive(Clone)]
struct MergeCtx {
    hudi_configs: std::sync::Arc<crate::config::HudiConfigs>,
    storage: std::sync::Arc<crate::storage::Storage>,
    /// Present only when merge plans exist (an empty table has no schema to
    /// resolve, and pure-insert commits never read).
    reader: Option<std::sync::Arc<crate::file_group::reader::FileGroupReader>>,
    instant: String,
    props: parquet::file::properties::WriterProperties,
    collect_ranges: bool,
    event_time: bool,
    /// Collect (key, partition) of removed rows for RLI tombstones.
    collect_rli_deletes: bool,
}

/// What a merge worker does to its file group's rows.
enum GroupOp {
    /// Merge `incoming` (meta-stamped) rows into the group; `partial_columns`
    /// limits which data columns matched rows take from the input.
    Upsert {
        incoming: RecordBatch,
        partial_columns: Option<Vec<String>>,
    },
    /// Remove rows by record key ((key, partition) exact or key-only).
    DeleteKeys {
        exact: std::sync::Arc<HashSet<(String, String)>>,
        keys_only: std::sync::Arc<HashSet<String>>,
    },
    /// Remove rows matching the filter.
    DeleteWhere { filter: Filter },
    /// SET single-row `set` values on rows matching the filter.
    UpdateWhere { filter: Filter, set: RecordBatch },
}

/// One file group's merge assignment.
struct MergePlan {
    partition: String,
    file_id: String,
    out_name: String,
    slice: crate::file_group::file_slice::FileSlice,
    op: GroupOp,
    prev_commit: String,
}

/// A merge worker's result.
struct MergeOutput {
    /// False when the op matched nothing in this group (no file written).
    wrote: bool,
    size: i64,
    num_rows: usize,
    /// Rows the op affected in this group (updated or deleted).
    matched: usize,
    ranges: Option<Vec<crate::metadata::table::column_stats::ColumnRangeStats>>,
    /// (record key, partition) tombstones for removed rows.
    rli_deletes: Vec<(String, String)>,
}

/// Replace `_hoodie_file_name` with `out_name` for every row.
fn restamp_file_name(batch: &RecordBatch, out_name: &str) -> Result<RecordBatch> {
    let Some((index, _)) = batch
        .schema()
        .column_with_name(MetaField::FileName.as_ref())
    else {
        return Ok(batch.clone());
    };
    let mut columns = batch.columns().to_vec();
    columns[index] = std::sync::Arc::new(StringArray::from(vec![out_name; batch.num_rows()]));
    RecordBatch::try_new(batch.schema(), columns).map_err(CoreError::ArrowError)
}

/// Stamp `_hoodie_commit_time` / `_hoodie_commit_seqno` with `instant` on rows
/// where `mask` is true, keeping other rows' values (Java MergeHandle: changed
/// rows carry the writing commit; untouched rows keep their history).
fn restamp_matched_rows(
    batch: &RecordBatch,
    mask: &BooleanArray,
    instant: &str,
) -> Result<RecordBatch> {
    let schema = batch.schema();
    let (Some((ct_idx, _)), Some((sq_idx, _))) = (
        schema.column_with_name(MetaField::CommitTime.as_ref()),
        schema.column_with_name(MetaField::CommitSeqno.as_ref()),
    ) else {
        return Ok(batch.clone());
    };
    let new_ct: ArrayRef = std::sync::Arc::new(StringArray::from(vec![instant; batch.num_rows()]));
    let new_sq: ArrayRef = std::sync::Arc::new(StringArray::from(
        (0..batch.num_rows())
            .map(|row| format!("{instant}_0-{row}-0"))
            .collect::<Vec<_>>(),
    ));
    let mut columns = batch.columns().to_vec();
    columns[ct_idx] = zip(mask, &new_ct, &columns[ct_idx])?;
    columns[sq_idx] = zip(mask, &new_sq, &columns[sq_idx])?;
    RecordBatch::try_new(schema, columns).map_err(CoreError::ArrowError)
}

/// The MergeHandle body: read the group's current rows, apply the op, stamp
/// meta, encode, and put — entirely inside the bounded write task pool.
fn merge_file_group_task(
    ctx: MergeCtx,
    plan: MergePlan,
) -> futures::future::BoxFuture<'static, Result<MergeOutput>> {
    Box::pin(async move {
        let reader = ctx.reader.as_ref().ok_or_else(|| {
            CoreError::Write("merge worker requires a file group reader".to_string())
        })?;
        let old = reader
            .read_file_slice(&plan.slice, &ReadOptions::new())
            .await?;
        let key_field = MetaField::RecordKey.as_ref();
        let mut rli_deletes = Vec::new();
        let (merged, matched) = match plan.op {
            GroupOp::Upsert {
                incoming,
                partial_columns,
            } => {
                let incoming = if old.num_rows() > 0 {
                    crate::write::align_batch_to_schema(&incoming, &old.schema()).ok_or_else(
                        || {
                            CoreError::Schema(
                                "upsert batch schema does not match the current table schema"
                                    .to_string(),
                            )
                        },
                    )?
                } else {
                    incoming
                };
                let old_keys = keys(&old, key_field)?;
                let mut old_by_key = HashMap::with_capacity(old.num_rows());
                for (index, key) in old_keys.iter().enumerate() {
                    old_by_key.insert(key.clone(), index as u32);
                }
                let incoming_keys = keys(&incoming, key_field)?;
                let mut final_indices: HashMap<String, u32> = old_by_key.clone();
                let mut matched = 0usize;
                for (index, key) in incoming_keys.iter().enumerate() {
                    if old_by_key.contains_key(key) {
                        matched += 1;
                    }
                    final_indices.insert(key.clone(), (old.num_rows() + index) as u32);
                }
                let mut selected = final_indices.into_values().collect::<Vec<_>>();
                selected.sort_unstable();
                let combined = if old.num_rows() == 0 {
                    incoming.clone()
                } else {
                    concat_batches(&old.schema(), [&old, &incoming])?
                };
                let selected = if ctx.event_time && old.num_rows() > 0 {
                    merge_with_event_time(ctx.hudi_configs.clone(), &old, &incoming, &combined)?
                } else {
                    selected
                };
                let merged = if let Some(columns) = partial_columns {
                    partial_merge(&combined, &old_by_key, &selected, &columns)?
                } else {
                    take_batch(&combined, &selected)?
                };
                (merged, matched)
            }
            GroupOp::DeleteKeys { exact, keys_only } => {
                let old_keys = keys(&old, key_field)?;
                let old_partitions = string_values(&old, MetaField::PartitionPath.as_ref())?;
                let mut selected = Vec::with_capacity(old.num_rows());
                for (index, (key, partition)) in
                    old_keys.iter().zip(old_partitions.iter()).enumerate()
                {
                    let doomed = keys_only.contains(key)
                        || exact.contains(&(key.clone(), partition.clone()));
                    if doomed {
                        if ctx.collect_rli_deletes {
                            rli_deletes.push((key.clone(), partition.clone()));
                        }
                    } else {
                        selected.push(index as u32);
                    }
                }
                let matched = old.num_rows() - selected.len();
                (take_batch(&old, &selected)?, matched)
            }
            GroupOp::DeleteWhere { filter } => {
                let mask = filters_to_row_mask(std::slice::from_ref(&filter), &old)?;
                let mut selected = Vec::with_capacity(old.num_rows());
                let mut matched = 0usize;
                let old_keys = keys(&old, key_field)?;
                let old_partitions = string_values(&old, MetaField::PartitionPath.as_ref())?;
                for index in 0..old.num_rows() {
                    if mask.value(index) {
                        matched += 1;
                        if ctx.collect_rli_deletes {
                            rli_deletes
                                .push((old_keys[index].clone(), old_partitions[index].clone()));
                        }
                    } else {
                        selected.push(index as u32);
                    }
                }
                if matched == 0 {
                    return Ok(MergeOutput {
                        wrote: false,
                        size: 0,
                        num_rows: 0,
                        matched: 0,
                        ranges: None,
                        rli_deletes,
                    });
                }
                (take_batch(&old, &selected)?, matched)
            }
            GroupOp::UpdateWhere { filter, set } => {
                let mask = filters_to_row_mask(std::slice::from_ref(&filter), &old)?;
                let (updated, matched) = apply_set_updates(&old, &mask, &set)?;
                if matched == 0 {
                    return Ok(MergeOutput {
                        wrote: false,
                        size: 0,
                        num_rows: 0,
                        matched: 0,
                        ranges: None,
                        rli_deletes,
                    });
                }
                (
                    restamp_matched_rows(&updated, &mask, &ctx.instant)?,
                    matched,
                )
            }
        };
        let merged = restamp_file_name(&merged, &plan.out_name)?;
        let path = relative_data_path(&plan.partition, &plan.out_name);
        let num_rows = merged.num_rows();
        let props = ctx.props.clone();
        let collect_ranges = ctx.collect_ranges;
        let (bytes, ranges) = tokio::task::spawn_blocking(move || -> Result<_> {
            let bytes = crate::write::append::write_parquet_bytes_with_props(props, &[merged])?;
            let ranges = if collect_ranges {
                Some(
                    crate::metadata::table::column_stats::column_ranges_from_parquet_bytes(&bytes)?,
                )
            } else {
                None
            };
            Ok((bytes, ranges))
        })
        .await
        .map_err(|e| CoreError::Write(format!("merge task panicked: {e}")))??;
        let size = bytes.len() as i64;
        ctx.storage.put_file(&path, bytes).await?;
        Ok(MergeOutput {
            wrote: true,
            size,
            num_rows,
            matched,
            ranges,
            rli_deletes,
        })
    })
}

/// Abort a fenced-but-empty instant: delete the fencing files and marker dir
/// so a zero-match op leaves no timeline trace.
async fn abort_requested_instant(table: &Table, instant: &str, action: Action) -> Result<()> {
    let storage = table.file_system_view.storage.clone();
    crate::write::markers::delete_marker_dir(storage.as_ref(), instant).await;
    let dir = timeline_dir(table);
    let requested = format!("{dir}/{instant}.{}.requested", action.as_ref());
    let inflight = if action == Action::Commit {
        format!("{dir}/{instant}.inflight")
    } else {
        format!("{dir}/{instant}.{}.inflight", action.as_ref())
    };
    storage.delete_file(&requested).await?;
    storage.delete_file(&inflight).await?;
    Ok(())
}

/// Which result slot worker `matched` counts land in.
#[derive(Clone, Copy, PartialEq)]
enum MatchedSlot {
    None,
    Updates,
    Deletes,
}

/// A pre-stamped new file group write (inserts that open new groups).
struct NewGroupWrite {
    partition: String,
    file_id: String,
    out_name: String,
    prepared: Vec<RecordBatch>,
}

/// Driver for MergeHandle-style COW commits: markers, then per-group merge
/// workers + new-group insert tasks on the bounded pool, then stats/MDT/commit.
/// The driver holds only plans, stats, and the caller's input rows — never a
/// file group's data.
#[allow(clippy::too_many_arguments)]
async fn commit_merge_plans(
    table: &mut Table,
    instant: &str,
    operation: &str,
    plans: Vec<MergePlan>,
    new_groups: Vec<NewGroupWrite>,
    schema: arrow_schema::SchemaRef,
    mut rli_entries: Vec<RecordIndexEntry>,
    updates: usize,
    inserts: usize,
    matched_slot: MatchedSlot,
    abort_on_noop: bool,
) -> Result<WriteResult> {
    let storage = table.file_system_view.storage.clone();
    let props = crate::write::append::parquet_writer_props(table);
    let collect_ranges = is_column_stats_enabled(table);
    let rli_enabled = table.is_metadata_table_enabled() && is_record_index_enabled(table);
    let ctx = MergeCtx {
        hudi_configs: table.hudi_configs.clone(),
        storage: storage.clone(),
        reader: if plans.is_empty() {
            None
        } else {
            Some(std::sync::Arc::new(
                table
                    .create_file_group_reader_with_options(
                        Some(&ReadOptions::new()),
                        std::iter::empty::<(&str, String)>(),
                    )
                    .await?,
            ))
        },
        instant: instant.to_string(),
        props: props.clone(),
        collect_ranges,
        event_time: uses_event_time_merge(&table.hudi_configs)?,
        collect_rli_deletes: rli_enabled && matched_slot == MatchedSlot::Deletes,
    };

    // Markers before any data write: MERGE for rewrites, CREATE for new groups.
    let mut planned_markers = Vec::new();
    for plan in &plans {
        planned_markers.push(crate::write::markers::Marker::merge(
            &plan.partition,
            &plan.out_name,
        ));
    }
    for group in &new_groups {
        planned_markers.push(crate::write::markers::Marker::create(
            &group.partition,
            &group.out_name,
        ));
    }
    crate::write::markers::write_markers(storage.as_ref(), instant, &planned_markers).await?;
    for partition in plans
        .iter()
        .map(|p| p.partition.clone())
        .chain(new_groups.iter().map(|g| g.partition.clone()))
        .collect::<HashSet<_>>()
    {
        crate::write::ensure_partition_metadata(storage.as_ref(), &partition, instant).await?;
    }

    // All file work on one bounded pool: merge workers + new-group encoders.
    let merge_tasks: Vec<_> = plans.iter().map(|_| ()).collect();
    let mut tasks: Vec<futures::future::BoxFuture<'static, Result<MergeOutput>>> = Vec::new();
    let mut plan_meta = Vec::with_capacity(plans.len());
    for plan in plans {
        plan_meta.push((
            plan.partition.clone(),
            plan.file_id.clone(),
            plan.out_name.clone(),
            plan.prev_commit.clone(),
            plan.slice.base_file_relative_path()?,
        ));
        tasks.push(merge_file_group_task(ctx.clone(), plan));
    }
    drop(merge_tasks);
    for group in &new_groups {
        let path = relative_data_path(&group.partition, &group.out_name);
        let storage = storage.clone();
        let props = props.clone();
        let prepared = group.prepared.clone();
        tasks.push(Box::pin(async move {
            let output = crate::write::parquet_file_task(
                storage,
                props,
                prepared.clone(),
                path,
                collect_ranges,
            )
            .await?;
            Ok(MergeOutput {
                wrote: true,
                size: output.size,
                num_rows: prepared.iter().map(RecordBatch::num_rows).sum(),
                matched: 0,
                ranges: output.ranges,
                rli_deletes: Vec::new(),
            })
        }));
    }
    let results =
        crate::write::run_write_tasks(tasks, crate::write::write_task_parallelism(table)).await;

    // Assemble in plan order; clean up written files on the first error.
    let mut additions = Vec::new();
    let mut partition_to_write_stats = HashMap::<String, Vec<HoodieWriteStat>>::new();
    let mut written_paths = Vec::new();
    let mut stats_updates = Vec::new();
    let mut old_paths = Vec::new();
    let mut matched_total = 0usize;
    let mut num_writes = 0usize;
    let mut first_error = None;
    let group_info: Vec<(String, String, String, String, Option<String>)> = plan_meta
        .into_iter()
        .chain(new_groups.iter().map(|g| {
            (
                g.partition.clone(),
                g.file_id.clone(),
                g.out_name.clone(),
                "null".to_string(),
                None,
            )
        }))
        .collect();
    let mut outputs = Vec::with_capacity(results.len());
    for ((partition, file_id, out_name, prev_commit, replaced_path), result) in
        group_info.iter().zip(results)
    {
        match result {
            Ok(output) => {
                if output.wrote {
                    let path = relative_data_path(partition, out_name);
                    written_paths.push(path.clone());
                    if let Some(ranges) = &output.ranges {
                        stats_updates.push(crate::write::metadata::StatsFileUpdate {
                            partition_path: partition.clone(),
                            file_name: out_name.clone(),
                            is_deleted: false,
                            ranges: ranges.clone(),
                        });
                    }
                    additions.push((partition.clone(), out_name.clone(), output.size, false));
                    if let Some(replaced) = replaced_path {
                        old_paths.push(replaced.clone());
                    }
                    partition_to_write_stats
                        .entry(partition.clone())
                        .or_default()
                        .push(HoodieWriteStat {
                            file_id: Some(file_id.clone()),
                            path: Some(path),
                            base_file: Some(out_name.clone()),
                            prev_commit: Some(prev_commit.clone()),
                            num_writes: Some(output.num_rows as i64),
                            partition_path: Some(partition.clone()),
                            total_write_bytes: Some(output.size),
                            file_size_in_bytes: Some(output.size),
                            ..Default::default()
                        });
                    num_writes += output.num_rows;
                }
                matched_total += output.matched;
                outputs.push(output);
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

    let (updates, deletes) = match matched_slot {
        MatchedSlot::None => (updates, 0),
        MatchedSlot::Updates => (matched_total, 0),
        MatchedSlot::Deletes => (updates, matched_total),
    };

    // Fill the per-stat operation counters the way the previous driver did
    // (totals on each stat).
    for stats in partition_to_write_stats.values_mut() {
        for stat in stats.iter_mut() {
            stat.num_deletes = Some(deletes as i64);
            stat.num_update_writes = Some(updates as i64);
            stat.num_inserts = Some(inserts as i64);
        }
    }

    // Zero effect anywhere: abort the fenced instant, leave no commit.
    if abort_on_noop && written_paths.is_empty() {
        abort_requested_instant(table, instant, Action::Commit).await?;
        return Ok(WriteResult::default());
    }

    // RLI tombstones from the workers (deleted rows' actual key/partition).
    if rli_enabled {
        for output in &outputs {
            for (record_key, partition_path) in &output.rli_deletes {
                rli_entries.push(RecordIndexEntry {
                    record_key: record_key.clone(),
                    partition_path: partition_path.clone(),
                    file_id: String::new(),
                    instant_time_millis: instant_to_epoch_millis(instant),
                    is_deleted: true,
                });
            }
        }
    }

    finalize_rewrite_commit(
        table,
        instant,
        RewriteKind::Commit,
        Action::Commit,
        operation,
        schema,
        partition_to_write_stats,
        HashMap::new(),
        additions,
        written_paths,
        stats_updates,
        &old_paths,
        if rli_enabled { rli_entries } else { Vec::new() },
        num_writes,
        updates,
        inserts,
        deletes,
    )
    .await
}

fn prev_commit_from_base_path(path: &str) -> String {
    let name = path.rsplit('/').next().unwrap_or(path);
    // `{fileId}_{writeToken}_{instant}.parquet`
    name.rsplit_once('_')
        .map(|(_, instant_ext)| instant_ext.trim_end_matches(".parquet").to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "null".to_string())
}
