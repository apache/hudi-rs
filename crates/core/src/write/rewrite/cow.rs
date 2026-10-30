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

//! Copy-on-write rewrite engine: every affected file group is merged into a
//! new base file by its own task, and the plans commit together.

use super::*;

/// Insert-overwrite: writes `batch` into new file groups that replace
/// `replaced_file_ids` in one replacecommit.
#[allow(clippy::too_many_arguments)]
pub(super) async fn rewrite(
    table: &mut Table,
    instant: &str,
    batch: &RecordBatch,
    replaced_file_ids: Vec<String>,
    mut old_paths: Vec<String>,
    log_only_replaced: Vec<(String, String)>,
    extra_replaced_paths: Vec<String>,
    operation: &str,
    inserts: usize,
    mut rli_deletes: Vec<RecordIndexEntry>,
) -> Result<WriteResult> {
    let storage = table.file_system_view.storage.clone();
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
        partition_to_replace_file_ids
            .entry(partition)
            .or_default()
            .push(file_id.clone());
    }
    // Safe only after the positional zip above.
    old_paths.extend(extra_replaced_paths);
    for (partition, file_id) in &log_only_replaced {
        partition_to_replace_file_ids
            .entry(partition.clone())
            .or_default()
            .push(file_id.clone());
    }

    // Group planning: rows form new file groups, split into buckets of
    // ~max.file.size records (Java insert-overwrite sizing).
    struct GroupPlan {
        partition: String,
        file_id: String,
        out_name: String,
        rows: Vec<u32>,
    }
    let mut groups: Vec<GroupPlan> = Vec::new();
    if batch.num_rows() > 0 {
        let row_keys = hoodie_keys_for_batch(&table.hudi_configs, batch, Some(instant))?;
        let mut new_rows_by_partition: HashMap<String, Vec<u32>> = HashMap::new();
        for (row, key) in row_keys.iter().enumerate() {
            new_rows_by_partition
                .entry(key.partition_path.clone())
                .or_default()
                .push(row as u32);
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
                    rows: chunk.to_vec(),
                });
            }
        }
    }
    let mut additions = Vec::new();
    let mut partition_to_write_stats = HashMap::<String, Vec<HoodieWriteStat>>::new();
    let mut written_paths = Vec::new();
    let mut row_group_file_ids: HashMap<u32, String> = HashMap::new();
    let mut stats_updates = Vec::new();

    // Markers before any data file is put.
    let planned_markers: Vec<_> = groups
        .iter()
        .map(|group| crate::write::markers::Marker::create(&group.partition, &group.out_name))
        .collect();
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
        )?
        .iter()
        // The batch may carry meta fields stamped with a placeholder name
        // from an earlier prepare; the file name is only real here, at the
        // point the output file is actually being produced.
        .map(|batch| restamp_file_name(batch, &group.out_name))
        .collect::<Result<Vec<_>>>()?;
        let path = relative_data_path(&group.partition, &group.out_name);
        group_prepared.push((index, prepared, path));
    }

    // Phase 2 (parallel): encode + put on the write task pool.
    let props = crate::write::append::parquet_writer_props(table);
    let bloom = crate::write::append::base_file_bloom_filter_config(table);
    let collect_ranges = is_column_stats_enabled(table);
    let tasks = group_prepared
        .iter()
        .map(|(_, prepared, path)| {
            crate::write::parquet_file_task(
                storage.clone(),
                props.clone(),
                bloom,
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
                        num_deletes: Some(0),
                        num_update_writes: Some(0),
                        num_inserts: Some(num_rows as i64),
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

    let instant_time_millis = crate::write::metadata::instant_millis(&table.hudi_configs, instant);
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
                        instant_time_millis,
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
        RewriteKind::Replace,
        Action::ReplaceCommit,
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
        0,
        inserts,
        0,
    )
    .await
}

/// Shared commit tail for COW rewrites: commit-metadata encode, MDT updates
/// (files / record_index / column stats), then critical section 2 (MDT
/// deltacommit -> completed instant -> bookkeeping). `rli_entries` must be
/// fully computed by the caller (adds and tombstones).
#[allow(clippy::too_many_arguments)]
pub(super) async fn finalize_rewrite_commit(
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
    let commit_bytes = crate::write::commit_metadata_bytes(
        table,
        operation,
        partition_to_write_stats,
        match kind {
            RewriteKind::Replace => Some(partition_to_replace_file_ids),
            RewriteKind::Commit => None,
        },
        schema.as_ref(),
    )?;
    let completed = crate::write::complete_write(
        table,
        instant,
        action,
        commit_bytes,
        schema.as_ref(),
        crate::write::MetadataTableUpdates {
            files: additions,
            record_index: rli_entries,
            column_stats: stats_updates,
            replaced_paths: old_paths.to_vec(),
        },
        &written_paths,
    )
    .await?;
    Ok(WriteResult {
        instant: instant.to_string(),
        completion_time: completed.completion_time,
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
pub(super) struct MergeCtx {
    pub(super) hudi_configs: std::sync::Arc<crate::config::HudiConfigs>,
    pub(super) storage: std::sync::Arc<crate::storage::Storage>,
    /// Present only when merge plans exist (an empty table has no schema to
    /// resolve, and pure-insert commits never read).
    pub(super) reader: Option<std::sync::Arc<crate::file_group::reader::FileGroupReader>>,
    pub(super) instant: String,
    pub(super) props: parquet::file::properties::WriterProperties,
    pub(super) bloom: Option<crate::bloom::BloomFilterConfig>,
    pub(super) collect_ranges: bool,
    pub(super) event_time: bool,
    /// Collect (key, partition) of removed rows for RLI tombstones.
    pub(super) collect_rli_deletes: bool,
}

/// What a merge worker does to its file group's rows.
pub(super) enum GroupOp {
    /// Merge `incoming` (meta-stamped) rows into the group; `partial_columns`
    /// limits which data columns matched rows take from the input.
    /// `evict_keys` are rows leaving this group for another partition
    /// (update-partition-path semantics): removed here, inserted there.
    Upsert {
        incoming: RecordBatch,
        partial_columns: Option<Vec<String>>,
        evict_keys: std::sync::Arc<HashSet<String>>,
        /// Whether incoming rows merge with stored ones by ordering value
        /// rather than by commit time.
        by_event_time: bool,
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
    /// Write the group's merged rows unchanged: the slice read already folded
    /// its log files into the base file's rows.
    Compact,
}

/// One file group's merge assignment.
pub(super) struct MergePlan {
    pub(super) partition: String,
    pub(super) file_id: String,
    pub(super) out_name: String,
    pub(super) slice: crate::file_group::file_slice::FileSlice,
    pub(super) op: GroupOp,
    pub(super) prev_commit: String,
}

/// A merge worker's result.
pub(super) struct MergeOutput {
    /// False when the op matched nothing in this group (no file written).
    pub(super) wrote: bool,
    pub(super) size: i64,
    pub(super) num_rows: usize,
    /// Rows the op affected in this group (updated or deleted).
    pub(super) matched: usize,
    /// Input rows this group gained as inserts.
    pub(super) inserted: usize,
    /// Rows a winning delete marker removed. Java's merge handle counts these
    /// as deletes on top of the update or insert they arrived as.
    pub(super) marker_deletes: usize,
    pub(super) ranges: Option<Vec<crate::metadata::table::column_stats::ColumnRangeStats>>,
    /// (record key, partition) tombstones for removed rows.
    pub(super) rli_deletes: Vec<(String, String)>,
}

impl MergeOutput {
    /// The op matched nothing in the group, which keeps its current file.
    fn unchanged() -> Self {
        Self {
            wrote: false,
            size: 0,
            num_rows: 0,
            matched: 0,
            inserted: 0,
            marker_deletes: 0,
            ranges: None,
            rli_deletes: Vec::new(),
        }
    }
}

/// Replace `_hoodie_file_name` with `out_name` for every row.
pub(super) fn restamp_file_name(batch: &RecordBatch, out_name: &str) -> Result<RecordBatch> {
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
pub(super) fn restamp_matched_rows(
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
    let new_sq: ArrayRef = std::sync::Arc::new(crate::write::append::commit_seqnos(
        instant,
        0,
        0,
        batch.num_rows(),
    ));
    let mut columns = batch.columns().to_vec();
    columns[ct_idx] = zip(mask, &new_ct, &columns[ct_idx])?;
    columns[sq_idx] = zip(mask, &new_sq, &columns[sq_idx])?;
    RecordBatch::try_new(schema, columns).map_err(CoreError::ArrowError)
}

/// The MergeHandle body: read the group's current rows, apply the op, stamp
/// meta, encode, and put — entirely inside the bounded write task pool.
pub(super) fn merge_file_group_task(
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
        let storage = ctx.storage.clone();
        let path = relative_data_path(&plan.partition, &plan.out_name);
        // The merge and encode are long synchronous sections; on the blocking
        // pool they cannot stall the async workers other tasks run on.
        let (output, bytes) = tokio::task::spawn_blocking(move || plan.merge(ctx, old))
            .await
            .map_err(|e| CoreError::Write(format!("merge task panicked: {e}")))??;
        if let Some(bytes) = bytes {
            storage.put_file(&path, bytes).await?;
        }
        Ok(output)
    })
}

impl MergePlan {
    /// Apply the op to the group's current rows `old`, stamp meta, and encode;
    /// the bytes are `None` when the op matched nothing and no file is written.
    fn merge(self, ctx: MergeCtx, old: RecordBatch) -> Result<(MergeOutput, Option<Vec<u8>>)> {
        let key_field = MetaField::RecordKey.as_ref();
        let mut rli_deletes = Vec::new();
        let mut marker_deletes = 0usize;
        let mut inserted = 0;
        let (merged, matched) = match self.op {
            GroupOp::Upsert {
                incoming,
                partial_columns,
                evict_keys,
                by_event_time,
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
                // Rows whose key moved to another partition leave this group.
                let old = if evict_keys.is_empty() {
                    old
                } else {
                    let old_keys = keys(&old, key_field)?;
                    let survivors: Vec<u32> = old_keys
                        .iter()
                        .enumerate()
                        .filter_map(|(index, key)| {
                            (!evict_keys.contains(key)).then_some(index as u32)
                        })
                        .collect();
                    take_batch(&old, &survivors)?
                };
                let old_keys = keys(&old, key_field)?;
                let incoming_keys = keys(&incoming, key_field)?;
                let incoming_key_set: HashSet<&str> =
                    incoming_keys.iter().map(String::as_str).collect();
                // Like the Java merge handle, stored rows of keys this upsert
                // does not touch are copied as they are, duplicates included.
                let (matched_old, mut selected): (Vec<u32>, Vec<u32>) = (0..old.num_rows() as u32)
                    .partition(|&index| {
                        incoming_key_set.contains(old_keys[index as usize].as_str())
                    });
                let combined = if old.num_rows() == 0 {
                    incoming.clone()
                } else {
                    concat_batches(&old.schema(), [&old, &incoming])?
                };
                let mut old_by_key: HashMap<String, u32> = matched_old
                    .iter()
                    .map(|&index| (old_keys[index as usize].clone(), index))
                    .collect();
                // A partial update takes unselected columns from the touched
                // key's stored row, which for a repeated key is its winner.
                if ctx.event_time
                    && partial_columns.is_some()
                    && old_by_key.len() < matched_old.len()
                {
                    old_by_key = merge_with_event_time(
                        ctx.hudi_configs.clone(),
                        &combined,
                        matched_old.clone(),
                    )?
                    .into_iter()
                    .map(|index| (old_keys[index as usize].clone(), index))
                    .collect();
                }
                let matched = incoming_keys
                    .iter()
                    .filter(|key| old_by_key.contains_key(*key))
                    .count();
                let incoming_rows = old.num_rows() as u32..combined.num_rows() as u32;
                if by_event_time && !matched_old.is_empty() {
                    let candidates = matched_old.into_iter().chain(incoming_rows).collect();
                    selected.extend(merge_with_event_time(
                        ctx.hudi_configs.clone(),
                        &combined,
                        candidates,
                    )?);
                } else {
                    selected.extend(incoming_rows);
                }
                selected.sort_unstable();
                let merged = if let Some(columns) = partial_columns {
                    let merged = partial_merge(&combined, &old_by_key, &selected, &columns)?;
                    // Partially-updated rows must carry the updating commit's
                    // time/seqno, or incremental readers skip the change. Only
                    // rows the merge resolved to the INCOMING side are updates;
                    // a stale event-time row that lost keeps its old lineage.
                    let mask: BooleanArray = selected
                        .iter()
                        .map(|index| Some(*index as usize >= old.num_rows()))
                        .collect();
                    restamp_matched_rows(&merged, &mask, &ctx.instant)?
                } else {
                    take_batch(&combined, &selected)?
                };
                // A winning incoming delete marker removes the row (Java
                // merge handle: `combineRecord.isDelete`); stored rows are
                // written as they are.
                let delete_markers = delete_marker_rows(&merged);
                let merged = if delete_markers.contains(&true) {
                    let merged_keys = keys(&merged, key_field)?;
                    let merged_partitions =
                        string_values(&merged, MetaField::PartitionPath.as_ref())?;
                    let mut survivors = Vec::with_capacity(merged.num_rows());
                    for (row, source) in selected.iter().enumerate() {
                        if delete_markers[row] && *source as usize >= old.num_rows() {
                            marker_deletes += 1;
                            rli_deletes
                                .push((merged_keys[row].clone(), merged_partitions[row].clone()));
                        } else {
                            survivors.push(row as u32);
                        }
                    }
                    take_batch(&merged, &survivors)?
                } else {
                    merged
                };
                inserted = incoming.num_rows() - matched;
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
                if matched == 0 {
                    return Ok((MergeOutput::unchanged(), None));
                }
                (take_batch(&old, &selected)?, matched)
            }
            GroupOp::DeleteWhere { filter } => {
                let mask = filters_to_row_mask(std::slice::from_ref(&filter), &old)?;
                let mut selected = Vec::with_capacity(old.num_rows());
                let mut matched = 0usize;
                let old_keys = keys(&old, key_field)?;
                let old_partitions = string_values(&old, MetaField::PartitionPath.as_ref())?;
                for index in 0..old.num_rows() {
                    if mask.is_valid(index) && mask.value(index) {
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
                    return Ok((MergeOutput::unchanged(), None));
                }
                (take_batch(&old, &selected)?, matched)
            }
            GroupOp::UpdateWhere { filter, set } => {
                let mask = filters_to_row_mask(std::slice::from_ref(&filter), &old)?;
                let (updated, matched) = apply_set_updates(&old, &mask, &set)?;
                if matched == 0 {
                    return Ok((MergeOutput::unchanged(), None));
                }
                let updated = restamp_matched_rows(&updated, &mask, &ctx.instant)?;
                // Setting the delete marker deletes the row, as an upsert does.
                let delete_markers = delete_marker_rows(&updated);
                let doomed: Vec<bool> = (0..updated.num_rows())
                    .map(|row| delete_markers[row] && mask.is_valid(row) && mask.value(row))
                    .collect();
                let updated = if doomed.contains(&true) {
                    let updated_keys = keys(&updated, key_field)?;
                    let partitions = string_values(&updated, MetaField::PartitionPath.as_ref())?;
                    let mut survivors = Vec::with_capacity(updated.num_rows());
                    for (row, doomed) in doomed.into_iter().enumerate() {
                        if doomed {
                            marker_deletes += 1;
                            rli_deletes.push((updated_keys[row].clone(), partitions[row].clone()));
                        } else {
                            survivors.push(row as u32);
                        }
                    }
                    take_batch(&updated, &survivors)?
                } else {
                    updated
                };
                (updated, matched)
            }
            GroupOp::Compact => (old, 0),
        };
        let merged = restamp_file_name(&merged, &self.out_name)?;
        let num_rows = merged.num_rows();
        let bytes =
            crate::write::append::write_parquet_bytes_with_props(ctx.props, ctx.bloom, &[merged])?;
        let ranges = if ctx.collect_ranges {
            Some(crate::metadata::table::column_stats::column_ranges_from_parquet_bytes(&bytes)?)
        } else {
            None
        };
        Ok((
            MergeOutput {
                wrote: true,
                size: bytes.len() as i64,
                num_rows,
                matched,
                inserted,
                marker_deletes,
                ranges,
                rli_deletes,
            },
            Some(bytes),
        ))
    }
}

/// Abort a fenced-but-empty instant: delete the fencing files and marker dir
/// so a zero-match op leaves no timeline trace.
pub(super) async fn abort_requested_instant(
    table: &Table,
    instant: &str,
    action: Action,
) -> Result<()> {
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
pub(super) enum MatchedSlot {
    None,
    Updates,
    Deletes,
}

/// A pre-stamped new file group write (inserts that open new groups).
pub(super) struct NewGroupWrite {
    pub(super) partition: String,
    pub(super) file_id: String,
    pub(super) out_name: String,
    pub(super) prepared: Vec<RecordBatch>,
}

/// Driver for MergeHandle-style COW commits: markers, then per-group merge
/// workers + new-group insert tasks on the bounded pool, then stats/MDT/commit.
/// The driver holds only plans, stats, and the caller's input rows — never a
/// file group's data.
#[allow(clippy::too_many_arguments)]
pub(super) async fn commit_merge_plans(
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
    let bloom = crate::write::append::base_file_bloom_filter_config(table);
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
        bloom,
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
    for group in &new_groups {
        let path = relative_data_path(&group.partition, &group.out_name);
        let storage = storage.clone();
        let props = props.clone();
        let prepared = group.prepared.clone();
        tasks.push(Box::pin(async move {
            let output = crate::write::parquet_file_task(
                storage,
                props,
                bloom,
                prepared.clone(),
                path,
                collect_ranges,
            )
            .await?;
            let num_rows = prepared.iter().map(RecordBatch::num_rows).sum();
            Ok(MergeOutput {
                wrote: true,
                size: output.size,
                num_rows,
                matched: 0,
                inserted: num_rows,
                marker_deletes: 0,
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
    let mut marker_deletes_total = 0usize;
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
                            num_update_writes: Some(match matched_slot {
                                MatchedSlot::Deletes => 0,
                                _ => output.matched as i64,
                            }),
                            num_deletes: Some(
                                match matched_slot {
                                    MatchedSlot::Deletes => output.matched as i64,
                                    _ => 0,
                                } + output.marker_deletes as i64,
                            ),
                            num_inserts: Some(output.inserted as i64),
                            partition_path: Some(partition.clone()),
                            total_write_bytes: Some(output.size),
                            file_size_in_bytes: Some(output.size),
                            ..Default::default()
                        });
                    num_writes += output.num_rows;
                }
                matched_total += output.matched;
                marker_deletes_total += output.marker_deletes;
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
        let mut all_deleted = true;
        for path in &written_paths {
            if storage.delete_file(path).await.is_err() {
                all_deleted = false;
            }
        }
        // Abort only when nothing this write produced is left behind: the
        // markers and fencing files are the trail a rollback follows, so
        // removing them while a data file survives orphans that file forever.
        if all_deleted {
            let _ = abort_requested_instant(table, instant, Action::Commit).await;
        }
        return Err(error);
    }

    let (updates, deletes) = match matched_slot {
        MatchedSlot::None => (updates, marker_deletes_total),
        MatchedSlot::Updates => (matched_total, marker_deletes_total),
        MatchedSlot::Deletes => (updates, matched_total + marker_deletes_total),
    };

    // Zero effect anywhere: abort the fenced instant, leave no commit.
    if abort_on_noop && written_paths.is_empty() {
        abort_requested_instant(table, instant, Action::Commit).await?;
        return Ok(WriteResult::default());
    }

    // RLI tombstones from the workers (deleted rows' actual key/partition),
    // replacing the driver's entry for a key an upsert delete marker removed.
    if rli_enabled {
        let instant_time_millis =
            crate::write::metadata::instant_millis(&table.hudi_configs, instant);
        let removed: HashSet<&str> = outputs
            .iter()
            .flat_map(|output| output.rli_deletes.iter().map(|(key, _)| key.as_str()))
            .collect();
        rli_entries.retain(|entry| !removed.contains(entry.record_key.as_str()));
        for output in &outputs {
            for (record_key, partition_path) in &output.rli_deletes {
                rli_entries.push(RecordIndexEntry {
                    record_key: record_key.clone(),
                    partition_path: partition_path.clone(),
                    file_id: String::new(),
                    instant_time_millis,
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

pub(super) fn prev_commit_from_base_path(path: &str) -> String {
    let name = path.rsplit('/').next().unwrap_or(path);
    // `{fileId}_{writeToken}_{instant}.parquet`
    name.rsplit_once('_')
        .map(|(_, instant_ext)| instant_ext.trim_end_matches(".parquet").to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "null".to_string())
}
