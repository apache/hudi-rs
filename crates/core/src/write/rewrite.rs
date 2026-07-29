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

use arrow::array::{ArrayRef, StringArray, UInt32Array};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_select::take::take;

use crate::Result;
use crate::config::table::HudiTableConfig::RecordKeyFields;
use crate::error::CoreError;
use crate::expr::filter::{Filter, filters_to_row_mask, validate_fields_against_schemas};
use crate::index::HoodieKey;
use crate::metadata::commit::{HoodieCommitMetadata, HoodieWriteStat};
use crate::metadata::meta_field::MetaField;
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
    let old = old.unwrap_or_else(|| RecordBatch::new_empty(incoming.schema()));
    if old.schema() != incoming.schema() {
        return Err(CoreError::Schema(
            "upsert batch schema does not match the current table schema".to_string(),
        ));
    }
    let key_name = record_key_name(table, &old)?;
    let old_keys = keys(&old, &key_name)?;
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
    let merged = if let Some(columns) = options.update_columns {
        partial_merge(
            &old,
            &incoming,
            &old_by_key,
            &incoming_keys,
            &selected,
            &columns,
        )?
    } else {
        take_batch(&combined, &selected)?
    };
    rewrite(
        table, &instant, &file_name, &merged, file_ids, old_paths, "UPSERT", updates, inserts, 0,
    )
    .await
}

pub async fn delete_filter(table: &mut Table, filter: Filter) -> Result<WriteResult> {
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
    let remaining = take_batch(&old, &selected)?;
    let instant = next_instant_timestamp();
    let file_name = format!("rewrite-{instant}_0-0-0_{instant}.parquet");
    rewrite(
        table, &instant, &file_name, &remaining, file_ids, old_paths, "DELETE", 0, 0, deleted,
    )
    .await
}

pub async fn delete_keys(table: &mut Table, delete_keys: &[HoodieKey]) -> Result<WriteResult> {
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
    ensure_unpartitioned(table)
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
    let fields: Vec<String> = table.hudi_configs.get(RecordKeyFields)?.into();
    if fields.len() != 1 {
        return Err(CoreError::Unsupported(
            "upsert currently requires exactly one record key field".to_string(),
        ));
    }
    Ok(fields[0].clone())
}

fn keys(batch: &RecordBatch, field: &str) -> Result<Vec<String>> {
    let array = batch
        .column_by_name(field)
        .ok_or_else(|| CoreError::Schema(format!("record key field '{field}' is missing")))?;
    let array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            CoreError::Unsupported(format!("record key field '{field}' must be Utf8"))
        })?;
    array
        .iter()
        .map(|value| {
            value.map(str::to_string).ok_or_else(|| {
                CoreError::Write(format!("record key field '{field}' cannot be null"))
            })
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
    old: &RecordBatch,
    incoming: &RecordBatch,
    old_by_key: &HashMap<String, u32>,
    incoming_keys: &[String],
    selected: &[u32],
    update_columns: &[String],
) -> Result<RecordBatch> {
    let combined = concat_batches(&old.schema(), [old, incoming])?;
    let mut final_source = HashMap::new();
    for (index, key) in incoming_keys.iter().enumerate() {
        final_source.insert(key, (old.num_rows() + index) as u32);
    }
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
                    *final_source.get(key).unwrap_or(index)
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
    storage.put_file(&path, bytes).await?;
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
