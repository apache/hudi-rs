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
use crate::write::keygen::{hoodie_keys_for_batch, relative_data_path};
use crate::write::metadata::{
    epoch_millis_to_instant, instant_to_epoch_millis, update_files_partition, update_record_index,
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
    table.reload_timeline_for_write().await?;
    if !table.is_mor() {
        ensure_append_only(table)?;
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
    let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if num_rows == 0 {
        return Err(CoreError::Write(
            "append requires at least one row".to_string(),
        ));
    }

    let request_instant = next_instant_timestamp();
    let layout_two = is_layout_two(table);
    let completion = if layout_two {
        Some(request_instant.clone())
    } else {
        None
    };
    let action = if table.is_mor() {
        Action::DeltaCommit
    } else {
        Action::Commit
    };
    let instant = Instant::new_completed(request_instant.clone(), action, completion)?;
    let timeline_dir = timeline_dir(table);
    let storage = table.file_system_view.storage.clone();

    // Group rows across batches by partition path and write one base file per partition.
    let mut partition_rows: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
    for (batch_idx, batch) in batches.iter().enumerate() {
        let keys = hoodie_keys_for_batch(table, batch, Some(&request_instant))?;
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
    let mut written_paths: Vec<String> = Vec::new();
    let mut primary_base_path = String::new();
    let mut partition_ordinal = 0usize;
    let max_records = max_records_per_file(table);

    for (partition_path, row_refs) in partition_rows {
        let batch_indices: HashMap<usize, Vec<u32>> = {
            let mut map: HashMap<usize, Vec<u32>> = HashMap::new();
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

        // Split oversized partitions into multiple base files (Java max.file.size).
        let total_rows = partition_batch.num_rows();
        let mut offset = 0usize;
        let mut part_file_idx = 0usize;
        while offset < total_rows {
            let end = (offset + max_records).min(total_rows);
            let indices: Vec<u32> = (offset as u32..end as u32).collect();
            let chunk = take_rows(&partition_batch, &indices)?;
            let chunk_keys = hoodie_keys_for_batch(table, &chunk, Some(&request_instant))?;
            let file_id = if partition_path.is_empty() && part_file_idx == 0 && partition_ordinal == 0
            {
                format!("append-{request_instant}")
            } else {
                format!("append-{request_instant}-{partition_ordinal}")
            };
            partition_ordinal += 1;
            part_file_idx += 1;
            let file_name = format!("{file_id}_0-0-0_{request_instant}.parquet");
            let base_file_path = relative_data_path(&partition_path, &file_name);
            if primary_base_path.is_empty() {
                primary_base_path = base_file_path.clone();
            }

            let write_batches =
                prepare_batches_for_write(table, &[chunk], &request_instant, &file_name)?;
            let file_bytes = write_parquet_bytes(&write_batches)?;
            let file_size = file_bytes.len() as i64;
            if let Err(error) = storage.put_file(&base_file_path, file_bytes).await {
                for path in &written_paths {
                    let _ = storage.delete_file(path).await;
                }
                return Err(error.into());
            }
            written_paths.push(base_file_path.clone());

            let row_count = indices.len() as i64;
            write_stats
                .entry(partition_path.clone())
                .or_default()
                .push(HoodieWriteStat {
                    file_id: Some(file_id.clone()),
                    path: Some(base_file_path.clone()),
                    // Basename only — FileGroup builder joins partition_path + name.
                    base_file: Some(file_name.clone()),
                    prev_commit: Some("null".to_string()),
                    num_writes: Some(row_count),
                    num_inserts: Some(row_count),
                    total_write_bytes: Some(file_size),
                    file_size_in_bytes: Some(file_size),
                    partition_path: Some(partition_path.clone()),
                    ..Default::default()
                });
            files_mdt.push((partition_path.clone(), file_name, file_size));

            for key in chunk_keys {
                rli_entries.push(RecordIndexEntry {
                    record_key: key.record_key,
                    partition_path: partition_path.clone(),
                    file_id: file_id.clone(),
                    instant_time_millis: instant_to_epoch_millis(&request_instant),
                    is_deleted: false,
                });
            }
            offset = end;
        }
    }

    let commit_metadata = HoodieCommitMetadata {
        version: Some(1),
        operation_type: Some("INSERT".to_string()),
        partition_to_write_stats: Some(write_stats),
        compacted: Some(false),
        // Java HoodieCommitMetadata.extraMetadata is non-null; null → NPE in Spark.
        extra_metadata: Some(HashMap::from([(
            "schema".to_string(),
            arrow_schema_to_avro_json(&schema),
        )])),
    };
    let commit_relative_path = instant.relative_path_with_base(&timeline_dir)?;
    let commit_bytes = if layout_two {
        commit_metadata.to_avro_bytes()?
    } else {
        commit_metadata.to_json_bytes()?
    };

    if let Err(error) = storage.put_file(&commit_relative_path, commit_bytes).await {
        for path in &written_paths {
            let _ = storage.delete_file(path).await;
        }
        return Err(error.into());
    }
    if table.is_metadata_table_enabled() {
        update_files_partition(storage.as_ref(), &request_instant, &files_mdt).await?;
        if is_record_index_enabled(table) {
            update_record_index(storage.as_ref(), &request_instant, &rli_entries).await?;
        }
    }

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

fn ensure_append_only(table: &Table) -> Result<()> {
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

pub(crate) fn next_instant_timestamp() -> String {
    // Match Java HoodieInstantTimeGenerator: yyyyMMddHHmmssSSS (UTC).
    // Monotonic within-process so rapid commits never collide.
    loop {
        let now = Utc::now();
        let candidate = now.format("%Y%m%d%H%M%S%3f").to_string();
        let candidate_millis = now.timestamp_millis();
        let last = LAST_EPOCH_MILLIS.load(Ordering::Relaxed);
        let next_millis = if candidate_millis <= last {
            last + 1
        } else {
            candidate_millis
        };
        if LAST_EPOCH_MILLIS
            .compare_exchange(last, next_millis, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            if next_millis == candidate_millis {
                return candidate;
            }
            return epoch_millis_to_instant(next_millis);
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

pub(crate) fn write_parquet_bytes(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;

    // Match Java/Hudi default request: zstd (hoodie.parquet.compression.codec).
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()))
        .build();
    let cursor = Cursor::new(Vec::new());
    let mut writer = ArrowWriter::try_new(cursor, batches[0].schema(), Some(props))?;
    for batch in batches {
        writer.write(batch)?;
    }
    let cursor = writer.into_inner()?;
    Ok(cursor.into_inner())
}

/// Minimal Arrow→Avro schema JSON for `HoodieCommitMetadata.extraMetadata.schema`.
///
/// Spark resolves table schema from this key; a missing/null `extraMetadata` NPEs.
pub(crate) fn arrow_schema_to_avro_json(schema: &arrow_schema::Schema) -> String {
    let fields: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| {
            let avro_type = match f.data_type() {
                arrow_schema::DataType::Boolean => "\"boolean\"",
                arrow_schema::DataType::Int32 => "\"int\"",
                arrow_schema::DataType::Int64 => "\"long\"",
                arrow_schema::DataType::Float32 => "\"float\"",
                arrow_schema::DataType::Float64 => "\"double\"",
                arrow_schema::DataType::Utf8 | arrow_schema::DataType::LargeUtf8 => "\"string\"",
                arrow_schema::DataType::Binary | arrow_schema::DataType::LargeBinary => {
                    "\"bytes\""
                }
                _ => "\"string\"",
            };
            let ty = if f.is_nullable() {
                format!("[\"null\",{avro_type}]")
            } else {
                avro_type.to_string()
            };
            format!(
                "{{\"name\":\"{}\",\"type\":{}}}",
                f.name().replace('\"', "\\\""),
                ty
            )
        })
        .collect();
    format!(
        "{{\"type\":\"record\",\"name\":\"hoodie_record\",\"fields\":[{}]}}",
        fields.join(",")
    )
}

pub(crate) fn prepare_batches_for_write(
    table: &Table,
    batches: &[RecordBatch],
    instant: &str,
    file_name: &str,
) -> Result<Vec<RecordBatch>> {
    let populates_meta_fields: bool = table
        .hudi_configs
        .get_or_default(PopulatesMetaFields)
        .into();
    if !populates_meta_fields {
        return Ok(batches.to_vec());
    }

    batches
        .iter()
        .map(|batch| {
            if batch.column_by_name(MetaField::RecordKey.as_ref()).is_some() {
                return Ok(batch.clone());
            }
            let keys = hoodie_keys_for_batch(table, batch, Some(instant))?;
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
                        .map(|row| format!("{instant}_0-{row}-0"))
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
