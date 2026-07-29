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
use std::str::FromStr;

use apache_avro::to_avro_datum;
use apache_avro::types::Value as AvroValue;
use arrow::array::{ArrayRef, StringArray, UInt32Array};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_select::take::take;

use crate::Result;
use crate::config::table::HudiTableConfig::{OrderingFields, RecordKeyFields, RecordMergeStrategy};
use crate::error::CoreError;
use crate::expr::filter::{Filter, filters_to_row_mask, validate_fields_against_schemas};
use crate::file_group::log_file::writer::LogFileWriter;
use crate::file_group::log_file::{BlockMetadataKey, BlockType};
use crate::file_group::record_batches::RecordBatches;
use crate::index::{HoodieIndex, HoodieKey, SimpleIndex};
use crate::merge::RecordMergeStrategyValue;
use crate::merge::record_merger::RecordMerger;
use crate::metadata::commit::{HoodieCommitMetadata, HoodieWriteStat};
use crate::metadata::meta_field::MetaField;
use crate::schema::delete::avro_schema_for_delete_record_list;
use crate::table::{ReadOptions, Table};
use crate::timeline::instant::{Action, Instant};
use crate::write::append::{
    ensure_copy_on_write, ensure_unpartitioned, is_layout_two, next_instant_timestamp,
    prepare_batches_for_write, timeline_dir, write_parquet_bytes,
};
use crate::write::metadata::update_files_partition_entries;

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

pub async fn upsert_batches(
    table: &mut Table,
    batches: &[RecordBatch],
    options: UpsertOptions,
) -> Result<WriteResult> {
    if table.is_mor() {
        return mor_upsert_batches(table, batches, options).await;
    }
    ensure_rewrite_supported(table)?;
    if batches.is_empty() {
        return Err(CoreError::Write(
            "upsert requires at least one RecordBatch".to_string(),
        ));
    }
    let (old, file_ids, old_paths) = current_data(table).await?;
    let instant = next_instant_timestamp();
    let file_name = format!("rewrite-{instant}_0-0-0_{instant}.parquet");
    let incoming = prepare_batches_for_write(table, batches, &instant, &file_name)?;
    let incoming = concat(&incoming)?;
    if incoming.num_rows() == 0 {
        return Err(CoreError::Write(
            "upsert requires at least one row".to_string(),
        ));
    }
    let old = old.unwrap_or_else(|| RecordBatch::new_empty(incoming.schema()));
    if old.schema() != incoming.schema() {
        return Err(CoreError::Schema(
            "upsert batch schema does not match the current table schema".to_string(),
        ));
    }
    let key_name = record_key_name(table, &old)?;
    let old_keys = keys(&old, &key_name)?;
    let incoming = deduplicate_last_by_key(&incoming, &key_name)?;
    let incoming_keys = keys(&incoming, &key_name)?;
    let mut old_by_key = HashMap::with_capacity(old.num_rows());
    for (index, key) in old_keys.iter().enumerate() {
        old_by_key.insert(key.clone(), index as u32);
    }
    let mut final_indices: HashMap<String, u32> = old_by_key.clone();
    let mut updates = 0;
    let mut inserts = 0;
    for (index, key) in incoming_keys.iter().enumerate() {
        if old_by_key.contains_key(key) {
            updates += 1;
        } else {
            inserts += 1;
        }
        final_indices.insert(key.clone(), (old.num_rows() + index) as u32);
    }
    let mut selected = final_indices.into_values().collect::<Vec<_>>();
    selected.sort_unstable();
    let combined = concat_batches(&old.schema(), [&old, &incoming])?;
    let event_time_merge = uses_event_time_merge(table)?;
    let selected = if event_time_merge {
        merge_with_event_time(table, &old, &incoming, &combined)?
    } else {
        selected
    };
    let merged = if let Some(columns) = options.update_columns {
        partial_merge(&combined, &old_by_key, &selected, &columns)?
    } else {
        take_batch(&combined, &selected)?
    };
    rewrite(
        table, &instant, &file_name, &merged, file_ids, old_paths, "UPSERT", updates, inserts, 0,
    )
    .await
}

pub async fn delete_filter(table: &mut Table, filter: Filter) -> Result<WriteResult> {
    if table.is_mor() {
        ensure_mor_merge_supported(table)?;
        let (Some(old), _, _) = current_data(table).await? else {
            return Ok(WriteResult::default());
        };
        validate_fields_against_schemas(&[filter.clone()], [old.schema().as_ref()])?;
        let mask = filters_to_row_mask(&[filter], &old)?;
        let key_name = record_key_name(table, &old)?;
        let keys = keys(&old, &key_name)?;
        let delete_keys = mask
            .iter()
            .enumerate()
            .filter(|(_, matches)| matches.unwrap_or(false))
            .map(|(index, _)| HoodieKey {
                record_key: keys[index].clone(),
                partition_path: String::new(),
            })
            .collect::<Vec<_>>();
        return mor_delete_keys(table, &delete_keys).await;
    }
    ensure_rewrite_supported(table)?;
    let (Some(old), file_ids, old_paths) = current_data(table).await? else {
        return Ok(WriteResult::default());
    };
    validate_fields_against_schemas(&[filter.clone()], [old.schema().as_ref()])?;
    let mask = filters_to_row_mask(&[filter], &old)?;
    let selected = mask
        .iter()
        .enumerate()
        .filter_map(|(index, matches)| (!matches.unwrap_or(false)).then_some(index as u32))
        .collect::<Vec<_>>();
    let deleted = old.num_rows() - selected.len();
    if deleted == 0 {
        return Ok(WriteResult::default());
    }
    let remaining = take_batch(&old, &selected)?;
    let instant = next_instant_timestamp();
    let file_name = format!("rewrite-{instant}_0-0-0_{instant}.parquet");
    rewrite(
        table, &instant, &file_name, &remaining, file_ids, old_paths, "DELETE", 0, 0, deleted,
    )
    .await
}

pub async fn delete_keys(table: &mut Table, delete_keys: &[HoodieKey]) -> Result<WriteResult> {
    if delete_keys.is_empty() {
        return Err(CoreError::Write(
            "delete requires at least one record key".to_string(),
        ));
    }
    if table.is_mor() {
        return mor_delete_keys(table, delete_keys).await;
    }
    ensure_rewrite_supported(table)?;
    let (Some(old), file_ids, old_paths) = current_data(table).await? else {
        return Ok(WriteResult::default());
    };
    let key_name = record_key_name(table, &old)?;
    let requested = delete_keys
        .iter()
        .filter(|key| key.partition_path.is_empty())
        .map(|key| key.record_key.as_str())
        .collect::<std::collections::HashSet<_>>();
    let selected = keys(&old, &key_name)?
        .iter()
        .enumerate()
        .filter_map(|(index, key)| (!requested.contains(key.as_str())).then_some(index as u32))
        .collect::<Vec<_>>();
    let deleted = old.num_rows() - selected.len();
    if deleted == 0 {
        return Ok(WriteResult::default());
    }
    let remaining = take_batch(&old, &selected)?;
    let instant = next_instant_timestamp();
    let file_name = format!("rewrite-{instant}_0-0-0_{instant}.parquet");
    rewrite(
        table, &instant, &file_name, &remaining, file_ids, old_paths, "DELETE", 0, 0, deleted,
    )
    .await
}

pub async fn overwrite_batches(table: &mut Table, batches: &[RecordBatch]) -> Result<WriteResult> {
    ensure_rewrite_supported(table)?;
    if batches.is_empty() {
        return Err(CoreError::Write(
            "overwrite requires at least one RecordBatch".to_string(),
        ));
    }
    let (_, file_ids, old_paths) = current_data(table).await?;
    let instant = next_instant_timestamp();
    let file_name = format!("rewrite-{instant}_0-0-0_{instant}.parquet");
    let batches = prepare_batches_for_write(table, batches, &instant, &file_name)?;
    let replacement = concat(&batches)?;
    if replacement.num_rows() == 0 {
        return Err(CoreError::Write(
            "overwrite requires at least one row".to_string(),
        ));
    }
    rewrite(
        table,
        &instant,
        &file_name,
        &replacement,
        file_ids,
        old_paths,
        "INSERT_OVERWRITE",
        0,
        replacement.num_rows(),
        0,
    )
    .await
}

fn ensure_rewrite_supported(table: &Table) -> Result<()> {
    ensure_copy_on_write(table)?;
    ensure_unpartitioned(table)?;
    ensure_supported_merge_configs(table)
}

fn ensure_mor_merge_supported(table: &Table) -> Result<()> {
    ensure_unpartitioned(table)?;
    ensure_supported_merge_configs(table)?;
    let strategy: String = table
        .hudi_configs
        .get_or_default(RecordMergeStrategy)
        .into();
    if RecordMergeStrategyValue::from_str(&strategy)?
        != RecordMergeStrategyValue::OverwriteWithLatest
    {
        return Err(CoreError::Unsupported(
            "MERGE_ON_READ upsert and delete require populated meta fields and an ordering field"
                .to_string(),
        ));
    }
    Ok(())
}

fn ensure_supported_merge_configs(table: &Table) -> Result<()> {
    const UNSUPPORTED_CUSTOM_MERGE_OPTIONS: [&str; 4] = [
        "hoodie.compaction.payload.class",
        "hoodie.datasource.write.payload.class",
        "hoodie.payload.class",
        "hoodie.record.merger.impls",
    ];
    let options = table.hudi_configs.as_options();
    if let Some(option) = UNSUPPORTED_CUSTOM_MERGE_OPTIONS
        .iter()
        .find(|option| options.contains_key(**option))
    {
        return Err(CoreError::Unsupported(format!(
            "writes do not support custom payload or record merger option '{option}'"
        )));
    }
    Ok(())
}

struct MorFileLocation {
    file_id: String,
    base_file_path: String,
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
            slice.file_id().to_string(),
            (
                slice.base_file_relative_path()?,
                slice.base_file.commit_timestamp.clone(),
            ),
        );
    }

    let tagged = SimpleIndex.tag_location(table, keys).await?;
    let mut locations = HashMap::with_capacity(tagged.len());
    for (key, location) in tagged {
        if let Some(location) = location {
            let (base_file_path, base_instant) =
                slices_by_file_id.get(&location.file_id).ok_or_else(|| {
                    CoreError::Write(format!(
                        "SimpleIndex returned missing file group '{}'",
                        location.file_id
                    ))
                })?;
            locations.insert(
                key.record_key,
                MorFileLocation {
                    file_id: location.file_id,
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
    let schema_check_batches =
        prepare_batches_for_write(table, &[incoming.clone()], "schema-check", "schema-check")?;
    let existing_batches = table.read(&ReadOptions::new()).await?;
    if !existing_batches.is_empty() {
        let existing = concat(&existing_batches)?;
        if existing.schema() != schema_check_batches[0].schema() {
            return Err(CoreError::Schema(
                "upsert batch schema does not match the current table schema".to_string(),
            ));
        }
    }
    let key_name = record_key_name(table, &incoming)?;
    let incoming = deduplicate_last_by_key(&incoming, &key_name)?;
    let incoming_keys = keys(&incoming, &key_name)?;
    let tagged_keys = incoming_keys
        .iter()
        .cloned()
        .map(|record_key| HoodieKey {
            record_key,
            partition_path: String::new(),
        })
        .collect::<Vec<_>>();
    let locations = mor_file_locations(table, &tagged_keys).await?;
    let instant = next_instant_timestamp();
    let mut update_indices: HashMap<String, Vec<u32>> = HashMap::new();
    let mut insert_indices = Vec::new();
    let mut updates = 0;
    let mut inserts = 0;
    for (index, key) in incoming_keys.iter().enumerate() {
        if let Some(location) = locations.get(key) {
            update_indices
                .entry(location.file_id.clone())
                .or_default()
                .push(index as u32);
            updates += 1;
        } else {
            insert_indices.push(index as u32);
            inserts += 1;
        }
    }

    let storage = table.file_system_view.storage.clone();
    let mut stats = Vec::new();
    let mut written_paths = Vec::<String>::new();
    if !insert_indices.is_empty() {
        let file_id = format!("append-{instant}");
        let base_file_path = format!("{file_id}_0-0-0_{instant}.parquet");
        let insert_batch = take_batch(&incoming, &insert_indices)?;
        let prepared =
            prepare_batches_for_write(table, &[insert_batch], &instant, &base_file_path)?;
        let bytes = write_parquet_bytes(&prepared)?;
        let size = bytes.len() as i64;
        if let Err(error) = storage.put_file(&base_file_path, bytes).await {
            return Err(error.into());
        }
        written_paths.push(base_file_path.clone());
        stats.push(HoodieWriteStat {
            file_id: Some(file_id),
            path: Some(base_file_path.clone()),
            base_file: Some(base_file_path),
            prev_commit: Some("null".to_string()),
            num_writes: Some(inserts as i64),
            num_inserts: Some(inserts as i64),
            total_write_bytes: Some(size),
            file_size_in_bytes: Some(size),
            partition_path: Some(String::new()),
            ..Default::default()
        });
    }

    for (file_id, indices) in update_indices {
        let location = locations
            .get(
                incoming_keys
                    .get(indices[0] as usize)
                    .ok_or_else(|| CoreError::Write("missing upsert key".to_string()))?,
            )
            .ok_or_else(|| CoreError::Write("missing file location for upsert key".to_string()))?;
        let log_file = format!(".{file_id}_{instant}.log.1_0-0-0");
        let update_batch = take_batch(&incoming, &indices)?;
        let prepared = prepare_batches_for_write(table, &[update_batch], &instant, &log_file)?;
        let parquet = write_parquet_bytes(&prepared)?;
        let content = LogFileWriter::write_log_block(
            BlockType::ParquetData,
            HashMap::from([(BlockMetadataKey::InstantTime, instant.clone())]),
            &parquet,
        );
        let size = content.len() as i64;
        if let Err(error) = storage.put_file(&log_file, content).await {
            for path in written_paths {
                let _ = storage.delete_file(&path).await;
            }
            return Err(error.into());
        }
        written_paths.push(log_file.clone());
        stats.push(HoodieWriteStat {
            file_id: Some(file_id),
            path: Some(log_file.clone()),
            base_file: Some(location.base_file_path.clone()),
            log_files: Some(vec![log_file]),
            prev_commit: Some(location.base_instant.clone()),
            num_writes: Some(indices.len() as i64),
            num_update_writes: Some(indices.len() as i64),
            total_write_bytes: Some(size),
            file_size_in_bytes: Some(size),
            total_log_records: Some(indices.len() as i64),
            total_log_files: Some(1),
            total_log_blocks: Some(1),
            partition_path: Some(String::new()),
            ..Default::default()
        });
    }
    if let Err(error) = write_delta_commit(table, &instant, "UPSERT", stats).await {
        for path in written_paths {
            let _ = storage.delete_file(&path).await;
        }
        return Err(error);
    }
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

async fn mor_delete_keys(table: &mut Table, delete_keys: &[HoodieKey]) -> Result<WriteResult> {
    ensure_mor_merge_supported(table)?;
    if delete_keys.is_empty() {
        return Err(CoreError::Write(
            "delete requires at least one record key".to_string(),
        ));
    }
    let locations = mor_file_locations(table, delete_keys).await?;
    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    let mut seen = std::collections::HashSet::new();
    for key in delete_keys
        .iter()
        .filter(|key| key.partition_path.is_empty())
    {
        if seen.insert(&key.record_key)
            && let Some(location) = locations.get(&key.record_key)
        {
            grouped
                .entry(location.file_id.clone())
                .or_default()
                .push(key.record_key.clone());
        }
    }
    if grouped.is_empty() {
        return Ok(WriteResult::default());
    }

    let instant = next_instant_timestamp();
    let storage = table.file_system_view.storage.clone();
    let mut stats = Vec::new();
    let mut written_paths = Vec::<String>::new();
    let mut deleted = 0;
    for (file_id, keys) in grouped {
        let location = locations
            .get(
                keys.first()
                    .ok_or_else(|| CoreError::Write("missing delete key".to_string()))?,
            )
            .ok_or_else(|| CoreError::Write("missing file location for delete key".to_string()))?;
        let log_file = format!(".{file_id}_{instant}.log.1_0-0-0");
        let content = delete_log_block(&instant, &keys)?;
        let size = content.len() as i64;
        if let Err(error) = storage.put_file(&log_file, content).await {
            for path in written_paths {
                let _ = storage.delete_file(&path).await;
            }
            return Err(error.into());
        }
        written_paths.push(log_file.clone());
        deleted += keys.len();
        stats.push(HoodieWriteStat {
            file_id: Some(file_id),
            path: Some(log_file.clone()),
            base_file: Some(location.base_file_path.clone()),
            log_files: Some(vec![log_file]),
            prev_commit: Some(location.base_instant.clone()),
            num_writes: Some(keys.len() as i64),
            num_deletes: Some(keys.len() as i64),
            total_write_bytes: Some(size),
            file_size_in_bytes: Some(size),
            total_log_records: Some(keys.len() as i64),
            total_log_files: Some(1),
            total_log_blocks: Some(1),
            partition_path: Some(String::new()),
            ..Default::default()
        });
    }
    if let Err(error) = write_delta_commit(table, &instant, "DELETE", stats).await {
        for path in written_paths {
            let _ = storage.delete_file(&path).await;
        }
        return Err(error);
    }
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

fn delete_log_block(instant: &str, keys: &[String]) -> Result<Vec<u8>> {
    let records = keys
        .iter()
        .map(|key| {
            AvroValue::Record(vec![
                (
                    "recordKey".to_string(),
                    AvroValue::Union(1, Box::new(AvroValue::String(key.clone()))),
                ),
                (
                    "partitionPath".to_string(),
                    AvroValue::Union(1, Box::new(AvroValue::String(String::new()))),
                ),
                (
                    "orderingVal".to_string(),
                    AvroValue::Union(2, Box::new(AvroValue::Long(i64::MAX))),
                ),
            ])
        })
        .collect();
    let payload = to_avro_datum(
        avro_schema_for_delete_record_list()?,
        AvroValue::Record(vec![(
            "deleteRecordList".to_string(),
            AvroValue::Array(records),
        )]),
    )?;
    let mut content = Vec::with_capacity(8 + payload.len());
    content.extend_from_slice(&3u32.to_be_bytes());
    content.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    content.extend_from_slice(&payload);
    Ok(LogFileWriter::write_log_block(
        BlockType::Delete,
        HashMap::from([(BlockMetadataKey::InstantTime, instant.to_string())]),
        &content,
    ))
}

async fn write_delta_commit(
    table: &Table,
    instant: &str,
    operation: &str,
    stats: Vec<HoodieWriteStat>,
) -> Result<()> {
    let metadata = HoodieCommitMetadata {
        version: Some(1),
        operation_type: Some(operation.to_string()),
        partition_to_write_stats: Some(HashMap::from([(String::new(), stats)])),
        partition_to_replace_file_ids: None,
        compacted: Some(false),
        extra_metadata: None,
    };
    let layout_two = is_layout_two(table);
    let completed = layout_two.then(|| instant.to_string());
    let commit = Instant::new_completed(instant.to_string(), Action::DeltaCommit, completed)?;
    let path = commit.relative_path_with_base(&timeline_dir(table))?;
    let bytes = if layout_two {
        metadata.to_avro_bytes()?
    } else {
        metadata.to_json_bytes()?
    };
    table
        .file_system_view
        .storage
        .put_file(&path, bytes)
        .await?;
    Ok(())
}

fn uses_event_time_merge(table: &Table) -> Result<bool> {
    let strategy: String = table
        .hudi_configs
        .get_or_default(RecordMergeStrategy)
        .into();
    let strategy = RecordMergeStrategyValue::from_str(&strategy)?;
    Ok(strategy == RecordMergeStrategyValue::OverwriteWithLatest
        && table.hudi_configs.try_get(OrderingFields)?.is_some())
}

fn merge_with_event_time(
    table: &Table,
    old: &RecordBatch,
    incoming: &RecordBatch,
    combined: &RecordBatch,
) -> Result<Vec<u32>> {
    let merger = RecordMerger::new(old.schema(), table.hudi_configs.clone());
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

async fn current_data(table: &Table) -> Result<(Option<RecordBatch>, Vec<String>, Vec<String>)> {
    let slices = table.get_file_slices(&ReadOptions::new()).await?;
    let file_ids = slices
        .iter()
        .map(|slice| slice.file_id().to_string())
        .collect();
    let old_paths = slices
        .iter()
        .map(|slice| slice.base_file_relative_path())
        .collect::<Result<Vec<_>>>()?;
    let batches = table.read(&ReadOptions::new()).await?;
    Ok((
        (!batches.is_empty())
            .then(|| concat(&batches))
            .transpose()?,
        file_ids,
        old_paths,
    ))
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
    if fields.len() != 1 {
        return Err(CoreError::Unsupported(
            "upsert currently requires exactly one record key field".to_string(),
        ));
    }
    Ok(fields[0].clone())
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
    let combined_keys = keys(&combined, key_column)?;
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

async fn rewrite(
    table: &mut Table,
    instant: &str,
    file_name: &str,
    batch: &RecordBatch,
    replaced_file_ids: Vec<String>,
    old_paths: Vec<String>,
    operation: &str,
    updates: usize,
    inserts: usize,
    deletes: usize,
) -> Result<WriteResult> {
    let storage = table.file_system_view.storage.clone();
    let mut additions = Vec::new();
    let mut stats = Vec::new();
    if batch.num_rows() > 0 {
        let bytes = write_parquet_bytes(std::slice::from_ref(batch))?;
        let size = bytes.len() as i64;
        storage.put_file(file_name, bytes).await?;
        additions.push((String::new(), file_name.to_string(), size, false));
        stats.push(HoodieWriteStat {
            file_id: Some(format!("rewrite-{instant}")),
            path: Some(file_name.to_string()),
            base_file: Some(file_name.to_string()),
            prev_commit: Some("null".to_string()),
            num_writes: Some(batch.num_rows() as i64),
            num_deletes: Some(deletes as i64),
            num_update_writes: Some(updates as i64),
            num_inserts: Some(inserts as i64),
            total_write_bytes: Some(size),
            file_size_in_bytes: Some(size),
            partition_path: Some(String::new()),
            ..Default::default()
        });
    }
    let metadata = HoodieCommitMetadata {
        version: Some(1),
        operation_type: Some(operation.to_string()),
        partition_to_write_stats: Some(HashMap::from([(String::new(), stats)])),
        partition_to_replace_file_ids: Some(HashMap::from([(String::new(), replaced_file_ids)])),
        compacted: Some(false),
        extra_metadata: None,
    };
    let layout_two = is_layout_two(table);
    let completed = layout_two.then(|| instant.to_string());
    let commit = Instant::new_completed(instant.to_string(), Action::ReplaceCommit, completed)?;
    let path = commit.relative_path_with_base(&timeline_dir(table))?;
    let bytes = if layout_two {
        metadata.to_avro_bytes()?
    } else {
        metadata.to_json_bytes()?
    };
    if let Err(error) = storage.put_file(&path, bytes).await {
        if batch.num_rows() > 0 {
            let _ = storage.delete_file(file_name).await;
        }
        return Err(error.into());
    }
    if table.is_metadata_table_enabled() {
        additions.extend(old_paths.into_iter().map(|path| {
            let name = path.rsplit('/').next().unwrap_or(&path).to_string();
            (String::new(), name, 0, true)
        }));
        update_files_partition_entries(storage.as_ref(), instant, &additions).await?;
    }
    table.timeline.reload_completed_commits().await?;
    table.file_system_view.clear_cache();
    Ok(WriteResult {
        instant: instant.to_string(),
        num_writes: batch.num_rows(),
        num_updates: updates,
        num_inserts: inserts,
        num_deletes: deletes,
    })
}
