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

//! Merge-on-read upsert, update and delete: log blocks appended to the file
//! groups the index locates, with inserts packed into new groups.

use super::*;

pub(super) struct MorFileLocation {
    pub(super) file_id: String,
    pub(super) partition_path: String,
    /// `None` for a log-only file group (e.g. a Spark delta commit that
    /// appended logs before any base file existed).
    pub(super) base_file_path: Option<String>,
    pub(super) base_instant: String,
}

pub(super) async fn mor_file_locations(
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

    let tagged = crate::index::for_table_checked(table)
        .await
        .tag_location(table, keys)
        .await?;
    let mut locations = HashMap::with_capacity(tagged.len());
    for (key, location) in tagged {
        if let Some(location) = location {
            // Stale index entry (group replaced, row gone): treat as absent
            // so the incoming record inserts instead of erroring.
            let Some((base_file_path, base_instant)) =
                slices_by_file_id.get(&(location.partition_path.clone(), location.file_id.clone()))
            else {
                continue;
            };
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

pub(super) async fn mor_upsert_batches(
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
    if table
        .timeline
        .get_latest_commit_timestamp_as_option()
        .is_some()
    {
        let table_schema = std::sync::Arc::new(table.get_schema_with_meta_fields().await?);
        if crate::write::align_batch_to_schema(&schema_check_batches[0], &table_schema).is_none() {
            return Err(CoreError::Schema(
                "upsert batch schema does not match the current table schema".to_string(),
            ));
        }
    }
    let key_name = record_key_name(table, &incoming)?;
    let incoming = deduplicate_last_by_key(&incoming, &key_name, &table.hudi_configs)?;
    crate::write::keygen::validate_keygen_inputs(
        &table.hudi_configs,
        std::slice::from_ref(&incoming),
    )?;
    let instant = request_deltacommit(table, "UPSERT").await?;
    let instant_time_millis = crate::write::metadata::instant_to_epoch_millis_in(
        &instant,
        &crate::write::metadata::timeline_timezone(&table.hudi_configs),
    );
    let tagged_keys = hoodie_keys_for_batch(&table.hudi_configs, &incoming, Some(&instant))?;
    let incoming_keys: Vec<String> = tagged_keys.iter().map(|k| k.record_key.clone()).collect();
    let locations = mor_file_locations(table, &tagged_keys).await?;
    let mut update_indices: HashMap<(String, String), Vec<u32>> = HashMap::new();
    let mut insert_indices_by_partition: HashMap<String, Vec<u32>> = HashMap::new();
    let mut updates = 0;
    let mut inserts = 0;
    // Update-partition-path (global index): a key whose incoming partition
    // differs from its located one moves — tombstoned in the old group via a
    // delete log block, inserted into its new partition.
    let mut moved_tombstones: HashMap<(String, String), Vec<HoodieKey>> = HashMap::new();
    // Merge-before-move (Java parity, see the COW path): a cross-partition
    // update that loses the event-time merge is appended to the OLD group as
    // a plain update; the reader-side merge keeps the old row.
    let mut moved_candidates: HashMap<String, Vec<String>> = HashMap::new();
    let mut mor_slice_by_file_id: HashMap<String, crate::file_group::file_slice::FileSlice> =
        HashMap::new();
    for slice in table.get_file_slices(&ReadOptions::new()).await? {
        mor_slice_by_file_id.insert(slice.file_id().to_string(), slice);
    }
    for (index, key) in incoming_keys.iter().enumerate() {
        let incoming_partition = tagged_keys
            .get(index)
            .map(|k| k.partition_path.clone())
            .unwrap_or_default();
        if let Some(location) = locations.get(key)
            && location.partition_path != incoming_partition
        {
            moved_candidates
                .entry(location.file_id.clone())
                .or_default()
                .push(key.clone());
        }
    }
    let rejected_moves = moved_keys_rejected_by_event_time(
        table,
        &incoming,
        &moved_candidates,
        &mor_slice_by_file_id,
    )
    .await?;
    // Delete markers follow Java's append handle: one that would land as a
    // new row writes nothing, a partition move only tombstones the stored
    // row, and an in-place marker is logged for the reader to merge. The
    // record index forgets every key whose stored row the marker removes.
    let delete_markers = delete_marker_rows(&incoming);
    let mut index_deletes: Vec<HoodieKey> = Vec::new();
    let mut in_place_deletes: HashMap<String, Vec<String>> = HashMap::new();
    for (index, key) in incoming_keys.iter().enumerate() {
        let incoming_partition = tagged_keys
            .get(index)
            .map(|k| k.partition_path.clone())
            .unwrap_or_default();
        match locations.get(key) {
            Some(location)
                if location.partition_path == incoming_partition
                    || rejected_moves.contains(key) =>
            {
                update_indices
                    .entry((location.partition_path.clone(), location.file_id.clone()))
                    .or_default()
                    .push(index as u32);
                updates += 1;
                if delete_markers[index] {
                    in_place_deletes
                        .entry(location.file_id.clone())
                        .or_default()
                        .push(key.clone());
                }
            }
            Some(location) => {
                let doomed = HoodieKey {
                    record_key: key.clone(),
                    partition_path: location.partition_path.clone(),
                };
                if delete_markers[index] {
                    index_deletes.push(doomed.clone());
                } else {
                    insert_indices_by_partition
                        .entry(incoming_partition)
                        .or_default()
                        .push(index as u32);
                }
                moved_tombstones
                    .entry((location.partition_path.clone(), location.file_id.clone()))
                    .or_default()
                    .push(doomed);
                updates += 1;
            }
            None if delete_markers[index] => {}
            None => {
                insert_indices_by_partition
                    .entry(incoming_partition)
                    .or_default()
                    .push(index as u32);
                inserts += 1;
            }
        }
    }
    // A stale in-place marker loses the reader's event-time merge and leaves
    // the stored row, and its index entry, in place.
    let stale_deletes = moved_keys_rejected_by_event_time(
        table,
        &incoming,
        &in_place_deletes,
        &mor_slice_by_file_id,
    )
    .await?;
    for (key, location) in in_place_deletes
        .into_values()
        .flatten()
        .filter(|key| !stale_deletes.contains(key))
        .filter_map(|key| locations.get(&key).map(|location| (key, location)))
    {
        index_deletes.push(HoodieKey {
            record_key: key,
            partition_path: location.partition_path.clone(),
        });
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
    // Moved-key tombstones write their own delete log (version 1); a group
    // also receiving data this commit rolls its data log to version 2.
    for (partition_path, file_id) in moved_tombstones.keys() {
        let log_name = format!(".{file_id}_{instant}.log.1_0-0-0");
        planned_markers.push(crate::write::markers::Marker::create(
            partition_path,
            &log_name,
        ));
    }
    for (partition_path, file_id) in log_tasks.keys() {
        let version = if moved_tombstones.contains_key(&(partition_path.clone(), file_id.clone())) {
            2
        } else {
            1
        };
        let log_name = format!(".{file_id}_{instant}.log.{version}_0-0-0");
        planned_markers.push(crate::write::markers::Marker::create(
            partition_path,
            &log_name,
        ));
    }
    crate::write::markers::write_markers(storage.as_ref(), &instant, &planned_markers).await?;

    let mut stats = Vec::new();
    let mut written_paths = Vec::<String>::new();
    let mut files_mdt = Vec::new();
    let mut rli_entries: Vec<RecordIndexEntry> = index_deletes
        .into_iter()
        .map(|key| RecordIndexEntry {
            record_key: key.record_key,
            partition_path: key.partition_path,
            file_id: String::new(),
            instant_time_millis: instant_to_epoch_millis(&instant),
            is_deleted: true,
        })
        .collect();
    let mut stats_updates = Vec::new();
    // Delete log blocks for keys that moved partitions.
    for ((partition_path, file_id), moved) in &moved_tombstones {
        let log_name = format!(".{file_id}_{instant}.log.1_0-0-0");
        let log_file = relative_data_path(partition_path, &log_name);
        crate::write::ensure_partition_metadata(storage.as_ref(), partition_path, &instant).await?;
        let pairs: Vec<(String, String)> = moved
            .iter()
            .map(|key| (key.record_key.clone(), key.partition_path.clone()))
            .collect();
        // Ordering value 0 is Java's DEFAULT_ORDERING_VALUE, which readers
        // treat as "natural order": the delete wins regardless of the doomed
        // row's event-time ordering (BufferedRecordMergerFactory
        // deltaMergeDeleteRecord). Required for partition moves, where the
        // evicted row may carry a higher ordering value than the mover.
        let content = crate::write::build_delete_log_block(&instant, &pairs, 0)?;
        let size = content.len() as i64;
        storage.put_file(&log_file, content).await?;
        written_paths.push(log_file.clone());
        files_mdt.push((partition_path.clone(), log_name.clone(), size, false));
        stats.push(HoodieWriteStat {
            file_id: Some(file_id.clone()),
            path: Some(log_file),
            base_file: Some(String::new()),
            log_files: Some(vec![log_name]),
            num_writes: Some(moved.len() as i64),
            num_deletes: Some(moved.len() as i64),
            total_write_bytes: Some(size),
            file_size_in_bytes: Some(size),
            total_log_records: Some(moved.len() as i64),
            total_log_files: Some(1),
            total_log_blocks: Some(1),
            partition_path: Some(partition_path.clone()),
            ..Default::default()
        });
    }
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
                        instant_time_millis,
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
        let version = if moved_tombstones.contains_key(&(partition_path.clone(), file_id.clone())) {
            2
        } else {
            1
        };
        let log_name = format!(".{file_id}_{instant}.log.{version}_0-0-0");
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
                instant_time_millis,
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
    let commit_bytes = crate::write::commit_metadata_bytes(
        table,
        "UPSERT",
        crate::write::stats_by_partition(stats),
        None,
        incoming.schema().as_ref(),
    )?;
    let completed = crate::write::complete_write(
        table,
        &instant,
        Action::DeltaCommit,
        commit_bytes,
        incoming.schema().as_ref(),
        crate::write::MetadataTableUpdates {
            files: files_mdt,
            record_index: rli_entries,
            column_stats: stats_updates,
            replaced_paths: Vec::new(),
        },
        &written_paths,
    )
    .await?;
    Ok(WriteResult {
        instant,
        completion_time: completed.completion_time,
        num_writes: incoming.num_rows(),
        num_updates: updates,
        num_inserts: inserts,
        num_deletes: 0,
    })
}

pub(super) async fn mor_update_filter(
    table: &mut Table,
    filter: Filter,
    updates: RecordBatch,
) -> Result<WriteResult> {
    crate::write::ensure_supported_key_generator(&table.hudi_configs)?;
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

pub(super) async fn mor_delete_keys(
    table: &mut Table,
    delete_keys: &[HoodieKey],
) -> Result<WriteResult> {
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
        // Dedupe by (key, requested partition): the same key may legitimately
        // be requested in two partitions.
        if seen.insert((&key.record_key, &key.partition_path))
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
    let instant_time_millis = crate::write::metadata::instant_to_epoch_millis_in(
        &instant,
        &crate::write::metadata::timeline_timezone(&table.hudi_configs),
    );
    let record_index = delete_keys
        .iter()
        .filter(|key| locations.contains_key(&key.record_key))
        .map(|key| RecordIndexEntry {
            record_key: key.record_key.clone(),
            partition_path: key.partition_path.clone(),
            file_id: String::new(),
            instant_time_millis,
            is_deleted: true,
        })
        .collect::<Vec<_>>();
    let schema = table.get_schema_with_meta_fields().await?;
    let commit_bytes = crate::write::commit_metadata_bytes(
        table,
        "DELETE",
        crate::write::stats_by_partition(stats),
        None,
        &schema,
    )?;
    let completed = crate::write::complete_write(
        table,
        &instant,
        Action::DeltaCommit,
        commit_bytes,
        &schema,
        crate::write::MetadataTableUpdates {
            files: files_mdt,
            record_index,
            column_stats: Vec::new(),
            replaced_paths: Vec::new(),
        },
        &written_paths,
    )
    .await?;
    Ok(WriteResult {
        instant,
        completion_time: completed.completion_time,
        num_writes: 0,
        num_updates: 0,
        num_inserts: 0,
        num_deletes: deleted,
    })
}

/// Java merges the incoming record with the existing one BEFORE deciding a
/// partition move (HoodieIndexUtils.mergeForPartitionUpdatesAndDeletionsIfNeeded):
/// a cross-partition update that loses the event-time merge keeps the old row
/// in its old partition. Returns the record keys whose incoming rows LOSE
/// against the located slices (strictly lower ordering; ties move).
pub(super) async fn moved_keys_rejected_by_event_time(
    table: &Table,
    incoming: &RecordBatch,
    moved: &HashMap<String, Vec<String>>, // file_id -> record keys moved from it
    slices: &HashMap<String, crate::file_group::file_slice::FileSlice>,
) -> Result<HashSet<String>> {
    let mut rejected = HashSet::new();
    if moved.is_empty() || !uses_event_time_merge(&table.hudi_configs)? {
        return Ok(rejected);
    }
    let ordering_fields: Vec<String> = table
        .hudi_configs
        .try_get(OrderingFields)?
        .map(Into::into)
        .unwrap_or_default();
    let Some(ordering_field) = ordering_fields.first() else {
        return Ok(rejected);
    };
    let incoming_keys = hoodie_keys_for_batch(&table.hudi_configs, incoming, None)?;
    let mut incoming_row_by_key = HashMap::new();
    for (row, key) in incoming_keys.into_iter().enumerate() {
        incoming_row_by_key.insert(key.record_key, row);
    }
    let reader = table
        .create_file_group_reader_with_options(
            Some(&ReadOptions::new()),
            std::iter::empty::<(&str, String)>(),
        )
        .await?;
    for (file_id, moved_keys) in moved {
        let Some(slice) = slices.get(file_id) else {
            continue;
        };
        let old = reader.read_file_slice(slice, &ReadOptions::new()).await?;
        if old.num_rows() == 0 {
            continue;
        }
        let old_keys = keys(&old, MetaField::RecordKey.as_ref())?;
        let wanted: HashSet<&String> = moved_keys.iter().collect();
        let (Some(old_ordering), Some(new_ordering)) = (
            old.column_by_name(ordering_field),
            incoming.column_by_name(ordering_field),
        ) else {
            continue;
        };
        let cmp = arrow_ord::ord::make_comparator(
            new_ordering.as_ref(),
            old_ordering.as_ref(),
            arrow_schema::SortOptions::default(),
        )?;
        for (old_row, old_key) in old_keys.iter().enumerate() {
            if !wanted.contains(old_key) {
                continue;
            }
            let Some(&new_row) = incoming_row_by_key.get(old_key) else {
                continue;
            };
            if cmp(new_row, old_row) == std::cmp::Ordering::Less {
                rejected.insert(old_key.clone());
            }
        }
    }
    Ok(rejected)
}
