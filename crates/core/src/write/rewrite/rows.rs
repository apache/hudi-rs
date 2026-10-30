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

//! Row and batch helpers shared by the copy-on-write and merge-on-read paths.

use super::*;

/// The `candidates` of `combined` that survive an event-time merge, sorted.
///
/// Only the candidates go through the merger, since it collapses every repeated
/// key; rows left out are the caller's to keep or drop.
pub(super) fn merge_with_event_time(
    hudi_configs: std::sync::Arc<crate::config::HudiConfigs>,
    combined: &RecordBatch,
    candidates: Vec<u32>,
) -> Result<Vec<u32>> {
    let candidate_rows = take_batch(combined, &candidates)?;
    let merger = RecordMerger::new(combined.schema(), hudi_configs);
    let merged = merger.merge_record_batches(RecordBatches::new_with_data_batches([
        candidate_rows.clone(),
    ]))?;
    let mut selected: Vec<u32> = selected_indices_for_merged(&candidate_rows, &merged)?
        .into_iter()
        .map(|index| candidates[index as usize])
        .collect();
    selected.sort_unstable();
    Ok(selected)
}

pub(super) fn selected_indices_for_merged(
    combined: &RecordBatch,
    merged: &RecordBatch,
) -> Result<Vec<u32>> {
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
pub(super) async fn data_for_filter_matches(
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

pub(super) fn concat(batches: &[RecordBatch]) -> Result<RecordBatch> {
    let first = batches
        .first()
        .ok_or_else(|| CoreError::Write("no batches to concatenate".to_string()))?;
    concat_batches(&first.schema(), batches).map_err(Into::into)
}

/// Record keys of `batch`: `_hoodie_record_key` when the batch carries it,
/// otherwise derived from the configured record key fields.
pub(super) fn batch_record_keys(table: &Table, batch: &RecordBatch) -> Result<Vec<String>> {
    if batch
        .column_by_name(MetaField::RecordKey.as_ref())
        .is_some()
    {
        return keys(batch, MetaField::RecordKey.as_ref());
    }
    if configured_record_key_fields(&table.hudi_configs)?.is_empty() {
        return Err(CoreError::Unsupported(
            "upsert/delete require a record key field (or _hoodie_record_key in the batch); auto-generated keys are insert-only"
                .to_string(),
        ));
    }
    record_keys_for_batch(&table.hudi_configs, batch)
}

/// If `filter` is `=` / `IN` on `_hoodie_record_key`, or on the record key
/// field of a single-field key, return keys for the RLI/SimpleIndex delete
/// path. A filter on one field of a composite key does not name whole keys.
pub(super) async fn keys_from_record_key_filter(
    table: &Table,
    filter: &Filter,
) -> Result<Option<Vec<HoodieKey>>> {
    if !matches!(filter.operator, ExprOperator::Eq | ExprOperator::In) {
        return Ok(None);
    }
    let to_keys = |record_keys: Vec<String>| {
        record_keys
            .into_iter()
            .map(|record_key| HoodieKey {
                record_key,
                partition_path: String::new(),
            })
            .collect()
    };
    if filter.field == MetaField::RecordKey.as_ref() {
        return Ok(Some(to_keys(filter.values.clone())));
    }
    if configured_record_key_fields(&table.hudi_configs)? != [filter.field.as_str()] {
        return Ok(None);
    }
    let key_type = match table.get_schema().await {
        Ok(schema) => schema
            .field_with_name(&filter.field)
            .map(|field| field.data_type().clone())
            .unwrap_or(DataType::Utf8),
        Err(_) => DataType::Utf8,
    };
    // The index matches stored key text, so a value names a key only when it
    // renders back to itself: `id = 07` matches row 7, whose key is `7`.
    let Ok(values) = arrow_cast::cast(&StringArray::from(filter.values.clone()), &key_type) else {
        return Ok(None);
    };
    let renders_as_given = render_key_field(values.as_ref(), &filter.field).is_ok_and(|rendered| {
        rendered
            .iter()
            .zip(&filter.values)
            .all(|(rendered, given)| rendered == Some(given.as_str()))
    });
    // An empty value names no key, as the writer refuses to store one.
    if !renders_as_given || filter.values.iter().any(String::is_empty) {
        return Ok(None);
    }
    // A filter on the key column holds column values, which the complex key
    // generator stores as `field:value`. A generator the writer cannot
    // reproduce falls back to the scan path.
    let Ok(encode_field_name) = encodes_single_key_field_name(&table.hudi_configs) else {
        return Ok(None);
    };
    Ok(Some(to_keys(
        filter
            .values
            .iter()
            .map(|value| {
                if encode_field_name {
                    format!("{}:{value}", filter.field)
                } else {
                    value.clone()
                }
            })
            .collect(),
    )))
}

pub(super) fn partition_paths_for_batch(batch: &RecordBatch) -> Vec<String> {
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

/// Rows carrying a true `_hoodie_is_deleted` delete marker, read as the file
/// group reader reads it (a null or non-boolean marker is not a delete).
pub(super) fn delete_marker_rows(batch: &RecordBatch) -> Vec<bool> {
    batch
        .column_by_name("_hoodie_is_deleted")
        .and_then(|column| column.as_any().downcast_ref::<BooleanArray>())
        .map(|markers| {
            (0..batch.num_rows())
                .map(|row| markers.is_valid(row) && markers.value(row))
                .collect()
        })
        .unwrap_or_else(|| vec![false; batch.num_rows()])
}

/// Overlay single-row SET values onto rows where `matched` is true.
pub(super) fn apply_set_updates(
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
pub(super) fn ensure_configured_record_key(table: &Table) -> Result<()> {
    if configured_record_key_fields(&table.hudi_configs)?.is_empty() {
        return Err(CoreError::Unsupported(
            "upsert/delete require hoodie.table.recordkey.fields; auto-generated keys are insert-only"
                .to_string(),
        ));
    }
    Ok(())
}

pub(super) fn keys(batch: &RecordBatch, field: &str) -> Result<Vec<String>> {
    string_values(batch, field)
}

/// Keep the final occurrence of each key, in original relative order.
/// Deduplicate a write batch by record key.
///
/// With an ordering (precombine) field configured, the row with the highest
/// ordering value wins (ties: later occurrence), matching Java precombine.
/// Without one, the LAST occurrence wins — the pick among duplicates is
/// arbitrary by design when no ordering field is configured.
pub(super) fn deduplicate_last_by_key(
    batch: &RecordBatch,
    keys: &[String],
    hudi_configs: &crate::config::HudiConfigs,
) -> Result<RecordBatch> {
    // Java routes in-batch dedupe through the merge-mode-derived merger
    // (BaseWriteHelper.reduceRecords), so a COMMIT_TIME_ORDERING table never
    // consults the ordering value here either — last occurrence wins.
    let ordering_fields: Vec<String> = if uses_event_time_merge(hudi_configs)? {
        hudi_configs
            .try_get(OrderingFields)?
            .map(Into::into)
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    // One comparator per ordering field, hoisted out of the duplicate loop;
    // fields compare lexicographically (first field breaks ties via the next).
    let comparators = ordering_fields
        .iter()
        .filter_map(|name| batch.column_by_name(name))
        .map(|column| {
            arrow_ord::ord::make_comparator(
                column.as_ref(),
                column.as_ref(),
                arrow_schema::SortOptions::default(),
            )
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut winners: HashMap<String, u32> = HashMap::with_capacity(batch.num_rows());
    for (index, key) in keys.iter().enumerate() {
        let index = index as u32;
        match winners.get(key) {
            None => {
                winners.insert(key.clone(), index);
            }
            Some(&existing) => {
                let keep_new = comparators
                    .iter()
                    .map(|cmp| cmp(index as usize, existing as usize))
                    .find(|ordering| *ordering != std::cmp::Ordering::Equal)
                    .unwrap_or(std::cmp::Ordering::Greater)
                    != std::cmp::Ordering::Less;
                if keep_new {
                    winners.insert(key.clone(), index);
                }
            }
        }
    }
    let mut indices = winners.into_values().collect::<Vec<_>>();
    indices.sort_unstable();
    take_batch(batch, &indices)
}

pub(super) fn string_values(batch: &RecordBatch, field: &str) -> Result<Vec<String>> {
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

pub(super) fn take_batch(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
    let indices = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None).map_err(Into::into))
        .collect::<Result<Vec<ArrayRef>>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(Into::into)
}

pub(super) fn partial_merge(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HudiConfigs;
    use arrow::array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Rows are `(commit time, key, ts)`; the seqno is unique per row.
    fn combined(rows: &[(&str, &str, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(MetaField::CommitTime.as_ref(), DataType::Utf8, false),
            Field::new(MetaField::CommitSeqno.as_ref(), DataType::Utf8, false),
            Field::new(MetaField::RecordKey.as_ref(), DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let seqnos: Vec<String> = rows
            .iter()
            .enumerate()
            .map(|(index, (commit, _, _))| format!("{commit}_0-{index}-0"))
            .collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(StringArray::from(seqnos)),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
            ],
        )
        .unwrap()
    }

    /// The selection over every row of the group, sorted into `combined` order.
    fn full_merge(configs: Arc<HudiConfigs>, combined: &RecordBatch) -> Vec<u32> {
        let merged = RecordMerger::new(combined.schema(), configs)
            .merge_record_batches(RecordBatches::new_with_data_batches([combined.clone()]))
            .unwrap();
        let mut selected = selected_indices_for_merged(combined, &merged).unwrap();
        selected.sort_unstable();
        selected
    }

    #[test]
    fn test_merge_with_event_time_merges_only_the_candidates() {
        let configs = Arc::new(HudiConfigs::new([
            (RecordMergeStrategy, "OVERWRITE_WITH_LATEST".to_string()),
            (OrderingFields, "ts".to_string()),
        ]));
        // Incoming: lower (a), equal (b) and higher (c) ordering than stored,
        // two versions of d, and a new key f twice; e is untouched.
        let incoming = [
            ("002", "a", 50),
            ("002", "b", 100),
            ("002", "c", 150),
            ("002", "d", 120),
            ("002", "d", 130),
            ("002", "f", 10),
            ("002", "f", 20),
        ];
        let unique = vec![
            ("001", "e", 100),
            ("001", "a", 100),
            ("001", "b", 100),
            ("001", "c", 100),
            ("001", "d", 100),
        ];
        let mut repeated = unique.clone();
        repeated.extend([("001", "e", 90), ("001", "a", 110)]);
        for old in [unique, repeated] {
            let rows: Vec<_> = old.iter().chain(incoming.iter()).copied().collect();
            let combined = combined(&rows);
            let num_old = old.len() as u32;
            // Every stored row except e's is touched.
            let (untouched, touched): (Vec<u32>, Vec<u32>) =
                (0..num_old).partition(|&index| old[index as usize].1 == "e");
            let candidates = touched
                .into_iter()
                .chain(num_old..combined.num_rows() as u32)
                .collect();
            let mut selected =
                merge_with_event_time(configs.clone(), &combined, candidates).unwrap();
            selected.extend(untouched);
            selected.sort_unstable();
            if num_old == 5 {
                // e and a stored; b, c, the later d and the later f incoming.
                assert_eq!(selected, vec![0, 1, 6, 7, 9, 11]);
                assert_eq!(selected, full_merge(configs.clone(), &combined));
            } else {
                // Both stored e rows stay; the later stored a wins over both.
                assert_eq!(selected, vec![0, 5, 6, 8, 9, 11, 13]);
            }
        }
    }
}
