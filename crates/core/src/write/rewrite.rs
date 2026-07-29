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

use apache_avro::to_avro_datum;
use apache_avro::types::Value as AvroValue;
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
use crate::file_group::log_file::writer::LogFileWriter;
use crate::file_group::log_file::{BlockMetadataKey, BlockType};
use crate::file_group::record_batches::RecordBatches;
use crate::index::{HoodieIndex, HoodieKey, for_table, is_record_index_enabled};
use crate::metadata::table::encode::RecordIndexEntry;
use crate::merge::RecordMergeStrategyValue;
use crate::merge::record_merger::RecordMerger;
use crate::metadata::commit::{HoodieCommitMetadata, HoodieWriteStat};
use crate::metadata::meta_field::MetaField;
use crate::schema::delete::avro_schema_for_delete_record_list;
use crate::table::{ReadOptions, Table};
use crate::timeline::instant::{Action, Instant};
use crate::write::append::{
    ensure_copy_on_write, is_layout_two, next_instant_timestamp,
    prepare_batches_for_write, timeline_dir, write_parquet_bytes,
};
use crate::write::keygen::{hoodie_keys_for_batch, relative_data_path};
use crate::write::metadata::{
    instant_to_epoch_millis, update_files_partition_entries, update_record_index,
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
    let instant = next_instant_timestamp();
    let incoming = prepare_batches_for_write(table, batches, &instant, "pending")?;
    let incoming = concat(&incoming)?;
    if incoming.num_rows() == 0 {
        return Err(CoreError::Write(
            "upsert requires at least one row".to_string(),
        ));
    }
    let key_name = record_key_name(table, &incoming)?;
    let incoming = deduplicate_last_by_key(&incoming, &key_name)?;
    let incoming_keys = keys(&incoming, &key_name)?;
    let incoming_hoodie_keys = hoodie_keys_for_batch(table, &incoming, None)?;
    let locations = for_table(table)
        .tag_location(table, &incoming_hoodie_keys)
        .await?;
    let affected_file_ids = locations
        .values()
        .filter_map(|loc| loc.as_ref().map(|l| l.file_id.clone()))
        .collect::<HashSet<_>>();
    let (old, file_ids, old_paths) = data_for_file_ids(table, &affected_file_ids).await?;
    let old = old.unwrap_or_else(|| RecordBatch::new_empty(incoming.schema()));
    if old.schema() != incoming.schema() && old.num_rows() > 0 {
        return Err(CoreError::Schema(
            "upsert batch schema does not match the current table schema".to_string(),
        ));
    }
    let old_keys = if old.num_rows() > 0 {
        keys(&old, &key_name)?
    } else {
        Vec::new()
    };
    let mut old_by_key = HashMap::with_capacity(old.num_rows());
    for (index, key) in old_keys.iter().enumerate() {
        old_by_key.insert(key.clone(), index as u32);
    }
    let mut final_indices: HashMap<String, u32> = old_by_key.clone();
    let mut updates = 0;
    let mut inserts = 0;
    for (index, key) in incoming_keys.iter().enumerate() {
        if locations
            .get(&incoming_hoodie_keys[index])
            .and_then(Option::as_ref)
            .is_some()
        {
            updates += 1;
        } else {
            inserts += 1;
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
    let event_time_merge = uses_event_time_merge(table)?;
    let selected = if event_time_merge && old.num_rows() > 0 {
        merge_with_event_time(table, &old, &incoming, &combined)?
    } else {
        selected
    };
    let merged = if let Some(columns) = options.update_columns {
        partial_merge(&combined, &old_by_key, &selected, &columns)?
    } else {
        take_batch(&combined, &selected)?
    };
    // Placeholder name — rewrite assigns a unique UUID file id per partition file.
    let file_name = format!("rewrite-{instant}_0-0-0_{instant}.parquet");
    rewrite(
        table, &instant, &file_name, &merged, file_ids, old_paths, "UPSERT", updates, inserts, 0,
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
        let (Some(old), _, _) = current_data(table).await? else {
            return Ok(WriteResult::default());
        };
        validate_fields_against_schemas(&[filter.clone()], [old.schema().as_ref()])?;
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

    // COW scan path — any column; no configured record key required.
    ensure_rewrite_supported(table)?;
    let (Some(old), file_ids, old_paths) = current_data(table).await? else {
        return Ok(WriteResult::default());
    };
    validate_fields_against_schemas(&[filter.clone()], [old.schema().as_ref()])?;
    let mask = filters_to_row_mask(&[filter], &old)?;
    let deleted_indices = mask
        .iter()
        .enumerate()
        .filter_map(|(index, matches)| matches.unwrap_or(false).then_some(index as u32))
        .collect::<Vec<_>>();
    let selected = mask
        .iter()
        .enumerate()
        .filter_map(|(index, matches)| (!matches.unwrap_or(false)).then_some(index as u32))
        .collect::<Vec<_>>();
    let deleted = deleted_indices.len();
    if deleted == 0 {
        return Ok(WriteResult::default());
    }
    let remaining = take_batch(&old, &selected)?;
    let instant = next_instant_timestamp();
    let file_id = crate::write::new_file_id();
    let file_name = format!("{file_id}_0-0-0_{instant}.parquet");
    let result = rewrite(
        table, &instant, &file_name, &remaining, file_ids, old_paths, "DELETE", 0, 0, deleted,
    )
    .await?;
    // Tombstone deleted keys in RLI when keys are recoverable from the snapshot.
    if table.is_metadata_table_enabled()
        && is_record_index_enabled(table)
        && let Ok(key_name) = record_key_name(table, &old)
    {
        let deleted_batch = take_batch(&old, &deleted_indices)?;
        let deleted_record_keys = keys(&deleted_batch, &key_name)?;
        let partitions = partition_paths_for_batch(&deleted_batch);
        let entries = deleted_record_keys
            .into_iter()
            .zip(partitions)
            .map(|(record_key, partition_path)| RecordIndexEntry {
                record_key,
                partition_path,
                file_id: String::new(),
                instant_time_millis: instant_to_epoch_millis(&instant),
                is_deleted: true,
            })
            .collect::<Vec<_>>();
        if !entries.is_empty() {
            update_record_index(
                table.file_system_view.storage.as_ref(),
                &instant,
                &entries,
            )
            .await?;
        }
    }
    Ok(result)
}

/// Iceberg-style UPDATE: set columns from a single-row `updates` batch on rows matching `filter`.
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
    let (Some(old), file_ids, old_paths) = current_data(table).await? else {
        return Ok(WriteResult::default());
    };
    validate_fields_against_schemas(&[filter.clone()], [old.schema().as_ref()])?;
    let mask = filters_to_row_mask(&[filter], &old)?;
    let (merged, num_updates) = apply_set_updates(&old, &mask, &updates)?;
    if num_updates == 0 {
        return Ok(WriteResult::default());
    }
    let instant = next_instant_timestamp();
    let file_id = crate::write::new_file_id();
    let file_name = format!("{file_id}_0-0-0_{instant}.parquet");
    let data_only = strip_meta_columns(&merged)?;
    let prepared = prepare_batches_for_write(table, &[data_only], &instant, &file_name)?;
    let merged = concat(&prepared)?;
    rewrite(
        table,
        &instant,
        &file_name,
        &merged,
        file_ids,
        old_paths,
        "UPSERT",
        num_updates,
        0,
        0,
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
    let (Some(old), file_ids, old_paths) = data_for_file_ids(table, &affected_file_ids).await?
    else {
        return Ok(WriteResult::default());
    };
    let requested = delete_keys
        .iter()
        .map(|key| (key.record_key.as_str(), key.partition_path.as_str()))
        .collect::<std::collections::HashSet<_>>();
    let old_keys = hoodie_keys_for_batch(table, &old, None)?;
    let selected = old_keys
        .iter()
        .enumerate()
        .filter_map(|(index, key)| {
            (!requested.contains(&(key.record_key.as_str(), key.partition_path.as_str())))
                .then_some(index as u32)
        })
        .collect::<Vec<_>>();
    let deleted = old.num_rows() - selected.len();
    if deleted == 0 {
        return Ok(WriteResult::default());
    }
    let remaining = take_batch(&old, &selected)?;
    let instant = next_instant_timestamp();
    let file_name = format!("rewrite-{instant}_0-0-0_{instant}.parquet");
    let result = rewrite(
        table, &instant, &file_name, &remaining, file_ids, old_paths, "DELETE", 0, 0, deleted,
    )
    .await?;
    if table.is_metadata_table_enabled() && is_record_index_enabled(table) {
        let deleted_keys = old_keys
            .into_iter()
            .filter(|key| requested.contains(&(key.record_key.as_str(), key.partition_path.as_str())))
            .map(|key| RecordIndexEntry {
                record_key: key.record_key,
                partition_path: key.partition_path,
                file_id: String::new(),
                instant_time_millis: instant_to_epoch_millis(&instant),
                is_deleted: true,
            })
            .collect::<Vec<_>>();
        update_record_index(
            table.file_system_view.storage.as_ref(),
            &instant,
            &deleted_keys,
        )
        .await?;
    }
    Ok(result)
}

pub async fn overwrite_batches(table: &mut Table, batches: &[RecordBatch]) -> Result<WriteResult> {
    table.reload_timeline_for_write().await?;
    ensure_rewrite_supported(table)?;
    if batches.is_empty() {
        return Err(CoreError::Write(
            "overwrite requires at least one RecordBatch".to_string(),
        ));
    }
    let (_, file_ids, old_paths) = current_data(table).await?;
    let instant = next_instant_timestamp();
    let file_id = crate::write::new_file_id();
    let file_name = format!("{file_id}_0-0-0_{instant}.parquet");
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
    const UNSUPPORTED_CUSTOM_MERGE_OPTIONS: [&str; 4] = [
        "hoodie.compaction.payload.class",
        "hoodie.datasource.write.payload.class",
        "hoodie.payload.class",
        "hoodie.record.merger.impls",
    ];
    const DEFAULT_PAYLOAD: &str = "org.apache.hudi.common.model.DefaultHoodieRecordPayload";
    let options = table.hudi_configs.as_options();
    for option in UNSUPPORTED_CUSTOM_MERGE_OPTIONS {
        if let Some(value) = options.get(option) {
            // Java tables always persist the default payload class; only reject customs.
            if option.ends_with("payload.class") && value == DEFAULT_PAYLOAD {
                continue;
            }
            return Err(CoreError::Unsupported(format!(
                "writes do not support custom payload or record merger option '{option}'"
            )));
        }
    }
    Ok(())
}

struct MorFileLocation {
    file_id: String,
    partition_path: String,
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
            (slice.partition_path.clone(), slice.file_id().to_string()),
            (
                slice.base_file_relative_path()?,
                slice.base_file.commit_timestamp.clone(),
            ),
        );
    }

    let tagged = for_table(table).tag_location(table, keys).await?;
    let mut locations = HashMap::with_capacity(tagged.len());
    for (key, location) in tagged {
        if let Some(location) = location {
            let (base_file_path, base_instant) =
                slices_by_file_id
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
    let instant = next_instant_timestamp();
    let tagged_keys = hoodie_keys_for_batch(table, &incoming, Some(&instant))?;
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
    let mut stats = Vec::new();
    let mut written_paths = Vec::<String>::new();
    let mut files_mdt = Vec::new();
    let mut rli_entries = Vec::new();
    for (partition_path, insert_indices) in insert_indices_by_partition {
        let file_id = crate::write::new_file_id();
        let file_name = format!("{file_id}_0-0-0_{instant}.parquet");
        let insert_batch = take_batch(&incoming, &insert_indices)?;
        let base_file_path = relative_data_path(&partition_path, &file_name);
        crate::write::ensure_partition_metadata(storage.as_ref(), &partition_path, &instant)
            .await?;
        let prepared =
            prepare_batches_for_write(table, &[insert_batch], &instant, &file_name)?;
        let bytes = write_parquet_bytes(table, &prepared)?;
        let size = bytes.len() as i64;
        if let Err(error) = storage.put_file(&base_file_path, bytes).await {
            return Err(error.into());
        }
        written_paths.push(base_file_path.clone());
        files_mdt.push((partition_path.clone(), file_name.clone(), size, false));
        rli_entries.extend(insert_indices.iter().map(|index| {
            let key = &tagged_keys[*index as usize];
            RecordIndexEntry {
                record_key: key.record_key.clone(),
                partition_path: partition_path.clone(),
                file_id: file_id.clone(),
                instant_time_millis: instant_to_epoch_millis(&instant),
                is_deleted: false,
            }
        }));
        stats.push(HoodieWriteStat {
            file_id: Some(file_id),
            path: Some(base_file_path),
            // Basename only — FileGroup builder joins partition_path + name.
            base_file: Some(file_name),
            prev_commit: Some("null".to_string()),
            num_writes: Some(insert_indices.len() as i64),
            num_inserts: Some(insert_indices.len() as i64),
            total_write_bytes: Some(size),
            file_size_in_bytes: Some(size),
            partition_path: Some(partition_path),
            ..Default::default()
        });
    }

    for ((_partition_path, file_id), indices) in update_indices {
        let location = locations
            .get(
                incoming_keys
                    .get(indices[0] as usize)
                    .ok_or_else(|| CoreError::Write("missing upsert key".to_string()))?,
            )
            .ok_or_else(|| CoreError::Write("missing file location for upsert key".to_string()))?;
        let log_name = format!(".{file_id}_{instant}.log.1_0-0-0");
        let log_file = relative_data_path(&location.partition_path, &log_name);
        crate::write::ensure_partition_metadata(
            storage.as_ref(),
            &location.partition_path,
            &instant,
        )
        .await?;
        let base_basename = std::path::Path::new(&location.base_file_path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(location.base_file_path.as_str())
            .to_string();
        let update_batch = take_batch(&incoming, &indices)?;
        let prepared = prepare_batches_for_write(table, &[update_batch], &instant, &log_name)?;
        let parquet = write_parquet_bytes(table, &prepared)?;
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
        files_mdt.push((
            location.partition_path.clone(),
            log_name.clone(),
            size,
            false,
        ));
        stats.push(HoodieWriteStat {
            file_id: Some(file_id),
            path: Some(log_file.clone()),
            base_file: Some(base_basename),
            log_files: Some(vec![log_name]),
            prev_commit: Some(location.base_instant.clone()),
            num_writes: Some(indices.len() as i64),
            num_update_writes: Some(indices.len() as i64),
            total_write_bytes: Some(size),
            file_size_in_bytes: Some(size),
            total_log_records: Some(indices.len() as i64),
            total_log_files: Some(1),
            total_log_blocks: Some(1),
            partition_path: Some(location.partition_path.clone()),
            ..Default::default()
        });
    }
    if table.is_metadata_table_enabled() {
        if !files_mdt.is_empty()
            && let Err(error) =
                update_files_partition_entries(storage.as_ref(), &instant, &files_mdt).await
        {
            for path in written_paths {
                let _ = storage.delete_file(&path).await;
            }
            return Err(error);
        }
        if is_record_index_enabled(table)
            && !rli_entries.is_empty()
            && let Err(error) = update_record_index(storage.as_ref(), &instant, &rli_entries).await
        {
            for path in written_paths {
                let _ = storage.delete_file(&path).await;
            }
            return Err(error);
        }
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

async fn mor_update_filter(
    table: &mut Table,
    filter: Filter,
    updates: RecordBatch,
) -> Result<WriteResult> {
    ensure_mor_merge_supported(table)?;
    ensure_configured_record_key(table)?;
    let (Some(old), _, _) = current_data(table).await? else {
        return Ok(WriteResult::default());
    };
    validate_fields_against_schemas(&[filter.clone()], [old.schema().as_ref()])?;
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

    let instant = next_instant_timestamp();
    let storage = table.file_system_view.storage.clone();
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
        files_mdt.push((partition_path.clone(), log_name.clone(), size, false));
        let base_basename = std::path::Path::new(&location.base_file_path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(location.base_file_path.as_str())
            .to_string();
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
    if table.is_metadata_table_enabled() {
        if !files_mdt.is_empty()
            && let Err(error) =
                update_files_partition_entries(storage.as_ref(), &instant, &files_mdt).await
        {
            for path in written_paths {
                let _ = storage.delete_file(&path).await;
            }
            return Err(error);
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
            if !entries.is_empty()
                && let Err(error) = update_record_index(storage.as_ref(), &instant, &entries).await
            {
                for path in written_paths {
                    let _ = storage.delete_file(&path).await;
                }
                return Err(error);
            }
        }
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

fn delete_log_block(instant: &str, keys: &[HoodieKey]) -> Result<Vec<u8>> {
    let records = keys
        .iter()
        .map(|key| {
            AvroValue::Record(vec![
                (
                    "recordKey".to_string(),
                    AvroValue::Union(1, Box::new(AvroValue::String(key.record_key.clone()))),
                ),
                (
                    "partitionPath".to_string(),
                    AvroValue::Union(1, Box::new(AvroValue::String(key.partition_path.clone()))),
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
    let storage = table.file_system_view.storage.clone();
    let timeline = timeline_dir(table);
    crate::write::fence_timeline_instant(
        storage.as_ref(),
        &timeline,
        instant,
        Action::DeltaCommit,
    )
    .await?;
    let mut partition_to_write_stats = HashMap::<String, Vec<HoodieWriteStat>>::new();
    for stat in stats {
        let partition = stat.partition_path.clone().unwrap_or_default();
        partition_to_write_stats.entry(partition).or_default().push(stat);
    }
    let metadata = HoodieCommitMetadata {
        version: Some(1),
        operation_type: Some(operation.to_string()),
        partition_to_write_stats: Some(partition_to_write_stats),
        compacted: Some(false),
        extra_metadata: Some(HashMap::new()),
    };
    let layout_two = is_layout_two(table);
    let completed = layout_two.then(|| instant.to_string());
    let commit = Instant::new_completed(instant.to_string(), Action::DeltaCommit, completed)?;
    let path = commit.relative_path_with_base(&timeline)?;
    let bytes = if layout_two {
        metadata.to_avro_bytes()?
    } else {
        metadata.to_json_bytes()?
    };
    storage.put_file(&path, bytes).await?;
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

/// Load only the listed file groups (for location-scoped COW rewrites).
async fn data_for_file_ids(
    table: &Table,
    file_ids: &HashSet<String>,
) -> Result<(Option<RecordBatch>, Vec<String>, Vec<String>)> {
    if file_ids.is_empty() {
        return Ok((None, Vec::new(), Vec::new()));
    }
    let slices = table.get_file_slices(&ReadOptions::new()).await?;
    let reader = table.create_file_group_reader_with_options(
        Some(&ReadOptions::new()),
        std::iter::empty::<(&str, String)>(),
    )?;
    let mut batches = Vec::new();
    let mut kept_ids = Vec::new();
    let mut kept_paths = Vec::new();
    for slice in slices {
        if !file_ids.contains(slice.file_id()) {
            continue;
        }
        kept_ids.push(slice.file_id().to_string());
        kept_paths.push(slice.base_file_relative_path()?);
        batches.push(
            reader
                .read_file_slice(&slice, &ReadOptions::new())
                .await?,
        );
    }
    Ok((
        (!batches.is_empty())
            .then(|| concat(&batches))
            .transpose()?,
        kept_ids,
        kept_paths,
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
fn keys_from_record_key_filter(
    table: &Table,
    filter: &Filter,
) -> Result<Option<Vec<HoodieKey>>> {
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
    RecordBatch::try_new(std::sync::Arc::new(arrow_schema::Schema::new(fields)), columns)
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
    let num_updates = matched
        .iter()
        .filter(|v| v.unwrap_or(false))
        .count();
    if num_updates == 0 {
        return Ok((old.clone(), 0));
    }
    let zeros = UInt32Array::from(vec![0u32; old.num_rows()]);
    let mut columns = Vec::with_capacity(old.num_columns());
    for field in old.schema().fields() {
        let old_col = old.column_by_name(field.name()).ok_or_else(|| {
            CoreError::Schema(format!("missing column '{}'", field.name()))
        })?;
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
    Ok((
        RecordBatch::try_new(old.schema(), columns)?,
        num_updates,
    ))
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
    _file_name: &str,
    batch: &RecordBatch,
    replaced_file_ids: Vec<String>,
    old_paths: Vec<String>,
    operation: &str,
    updates: usize,
    inserts: usize,
    deletes: usize,
) -> Result<WriteResult> {
    let storage = table.file_system_view.storage.clone();
    crate::write::fence_timeline_instant(
        storage.as_ref(),
        &timeline_dir(table),
        instant,
        Action::ReplaceCommit,
    )
    .await?;
    let mut additions = Vec::new();
    let mut partition_to_write_stats = HashMap::<String, Vec<HoodieWriteStat>>::new();
    let mut partition_to_replace_file_ids = HashMap::<String, Vec<String>>::new();
    for (file_id, old_path) in replaced_file_ids.iter().zip(&old_paths) {
        let partition = old_path
            .rsplit_once('/')
            .map(|(partition, _)| partition.to_string())
            .unwrap_or_default();
        partition_to_replace_file_ids
            .entry(partition)
            .or_default()
            .push(file_id.clone());
    }
    let mut written_paths = Vec::new();
    let mut rli_file_id_by_partition = HashMap::<String, String>::new();
    if batch.num_rows() > 0 {
        let mut rows_by_partition = HashMap::<String, Vec<u32>>::new();
        for (row, key) in hoodie_keys_for_batch(table, batch, Some(instant))?
            .iter()
            .enumerate()
        {
            rows_by_partition
                .entry(key.partition_path.clone())
                .or_default()
                .push(row as u32);
        }
        for (partition_path, rows) in rows_by_partition {
            let partition_batch = take_batch(batch, &rows)?;
            let file_id = crate::write::new_file_id();
            let out_name = format!("{file_id}_0-0-0_{instant}.parquet");
            let path = relative_data_path(&partition_path, &out_name);
            crate::write::ensure_partition_metadata(storage.as_ref(), &partition_path, instant)
                .await?;
            let prepared =
                prepare_batches_for_write(table, &[partition_batch.clone()], instant, &out_name)?;
            let bytes = write_parquet_bytes(table, &prepared)?;
            let size = bytes.len() as i64;
            storage.put_file(&path, bytes).await?;
            written_paths.push(path.clone());
            additions.push((partition_path.clone(), out_name.clone(), size, false));
            rli_file_id_by_partition.insert(partition_path.clone(), file_id.clone());
            partition_to_write_stats
                .entry(partition_path.clone())
                .or_default()
                .push(HoodieWriteStat {
                    file_id: Some(file_id),
                    path: Some(path),
                    base_file: Some(out_name),
                    prev_commit: Some("null".to_string()),
                    num_writes: Some(partition_batch.num_rows() as i64),
                    num_deletes: Some(deletes as i64),
                    num_update_writes: Some(updates as i64),
                    num_inserts: Some(inserts as i64),
                    total_write_bytes: Some(size),
                    file_size_in_bytes: Some(size),
                    partition_path: Some(partition_path),
                    ..Default::default()
                });
        }
    }
    let metadata = crate::metadata::replace_commit::HoodieReplaceCommitMetadata {
        version: Some(1),
        operation_type: Some(operation.to_string()),
        partition_to_write_stats: Some(partition_to_write_stats),
        compacted: Some(false),
        extra_metadata: Some(HashMap::from([(
            "schema".to_string(),
            crate::write::append::arrow_schema_to_avro_json(batch.schema().as_ref()),
        )])),
        partition_to_replace_file_ids: Some(partition_to_replace_file_ids),
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

    // Crash-consistency: MDT before completed replacecommit (see append.rs).
    if table.is_metadata_table_enabled() {
        additions.extend(old_paths.into_iter().map(|path| {
            let name = path.rsplit('/').next().unwrap_or(&path).to_string();
            let partition = path
                .rsplit_once('/')
                .map(|(partition, _)| partition.to_string())
                .unwrap_or_default();
            (partition, name, 0, true)
        }));
        if let Err(error) =
            update_files_partition_entries(storage.as_ref(), instant, &additions).await
        {
            for path in written_paths {
                let _ = storage.delete_file(&path).await;
            }
            return Err(error);
        }
        if is_record_index_enabled(table) {
            let entries = hoodie_keys_for_batch(table, batch, Some(instant))?
                .into_iter()
                .filter_map(|key| {
                    let file_id = rli_file_id_by_partition.get(&key.partition_path)?.clone();
                    Some(RecordIndexEntry {
                        record_key: key.record_key,
                        partition_path: key.partition_path,
                        file_id,
                        instant_time_millis: instant_to_epoch_millis(instant),
                        is_deleted: false,
                    })
                })
                .collect::<Vec<_>>();
            if let Err(error) = update_record_index(storage.as_ref(), instant, &entries).await {
                for path in written_paths {
                    let _ = storage.delete_file(&path).await;
                }
                return Err(error);
            }
        }
    }

    if let Err(error) = storage.put_file(&path, bytes).await {
        for path in written_paths {
            let _ = storage.delete_file(&path).await;
        }
        return Err(error.into());
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
