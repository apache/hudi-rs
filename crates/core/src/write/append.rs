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

use arrow::array::{ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use parquet::arrow::ArrowWriter;

use crate::Result;
use crate::config::table::HudiTableConfig::{
    PartitionFields, PopulatesMetaFields, RecordKeyFields, RecordMergeStrategy, TableType,
    TableVersion, TimelineLayoutVersion, TimelinePath,
};
use crate::config::table::TableTypeValue;
use crate::error::CoreError;
use crate::merge::RecordMergeStrategyValue;
use crate::metadata::HUDI_METADATA_DIR;
use crate::metadata::commit::{HoodieCommitMetadata, HoodieWriteStat};
use crate::metadata::meta_field::MetaField;
use crate::table::Table;
use crate::timeline::instant::{Action, Instant};
use crate::write::metadata::update_files_partition;

/// Result of an append write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendResult {
    pub instant: String,
    pub commit_relative_path: String,
    pub base_file_path: String,
    pub num_rows: usize,
}

static LAST_EPOCH_MILLIS: AtomicI64 = AtomicI64::new(0);

/// Append one or more record batches as a single INSERT commit (append-only COW).
pub async fn append_batches(table: &mut Table, batches: &[RecordBatch]) -> Result<AppendResult> {
    ensure_append_only(table)?;
    ensure_copy_on_write(table)?;
    ensure_unpartitioned(table)?;

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
    let instant = Instant::new_completed(request_instant.clone(), Action::Commit, completion)?;
    let timeline_dir = timeline_dir(table);

    let file_id = format!("append-{request_instant}");
    let write_token = "0-0-0";
    let base_file_name = format!("{file_id}_{write_token}_{request_instant}.parquet");
    let base_file_path = base_file_name.clone();

    let write_batches =
        prepare_batches_for_write(table, batches, &request_instant, &base_file_name)?;
    let file_bytes = write_parquet_bytes(&write_batches)?;
    let file_size = file_bytes.len() as i64;
    let storage = table.file_system_view.storage.clone();
    storage.put_file(&base_file_path, file_bytes).await?;

    let commit_metadata =
        build_insert_commit_metadata(&file_id, &base_file_path, num_rows as i64, file_size);
    let commit_relative_path = instant.relative_path_with_base(&timeline_dir)?;
    let commit_bytes = if layout_two {
        commit_metadata.to_avro_bytes()?
    } else {
        commit_metadata.to_json_bytes()?
    };

    if let Err(error) = storage.put_file(&commit_relative_path, commit_bytes).await {
        let _ = storage.delete_file(&base_file_path).await;
        return Err(error.into());
    }
    if table.is_metadata_table_enabled() {
        update_files_partition(
            storage.as_ref(),
            &request_instant,
            &[(String::new(), base_file_name.clone(), file_size)],
        )
        .await?;
    }

    table.timeline.reload_completed_commits().await?;
    table.file_system_view.clear_cache();

    Ok(AppendResult {
        instant: request_instant,
        commit_relative_path,
        base_file_path,
        num_rows,
    })
}

fn ensure_append_only(table: &Table) -> Result<()> {
    let strategy: String = table
        .hudi_configs
        .get_or_default(RecordMergeStrategy)
        .into();
    if RecordMergeStrategyValue::from_str(&strategy)? != RecordMergeStrategyValue::AppendOnly {
        return Err(CoreError::Unsupported(format!(
            "append is currently only supported for append-only tables. Found merge strategy '{strategy}'"
        )));
    }
    Ok(())
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

pub(crate) fn ensure_unpartitioned(table: &Table) -> Result<()> {
    let partition_fields: Vec<String> = table.hudi_configs.get_or_default(PartitionFields).into();
    if !partition_fields.is_empty() {
        return Err(CoreError::Unsupported(
            "append currently supports only unpartitioned tables".to_string(),
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

pub(crate) fn next_instant_timestamp() -> String {
    let now = Utc::now().timestamp_millis();
    loop {
        let last = LAST_EPOCH_MILLIS.load(Ordering::Relaxed);
        let next = if now <= last { last + 1 } else { now };
        if LAST_EPOCH_MILLIS
            .compare_exchange(last, next, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            return format!("{next:017}");
        }
    }
}

pub(crate) fn write_parquet_bytes(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let cursor = Cursor::new(Vec::new());
    let mut writer = ArrowWriter::try_new(cursor, batches[0].schema(), None)?;
    for batch in batches {
        writer.write(batch)?;
    }
    let cursor = writer.into_inner()?;
    Ok(cursor.into_inner())
}

pub(crate) fn build_insert_commit_metadata(
    file_id: &str,
    base_file_path: &str,
    num_rows: i64,
    file_size: i64,
) -> HoodieCommitMetadata {
    let write_stat = HoodieWriteStat {
        file_id: Some(file_id.to_string()),
        path: Some(base_file_path.to_string()),
        base_file: Some(base_file_path.to_string()),
        log_files: None,
        prev_commit: Some("null".to_string()),
        num_writes: Some(num_rows),
        num_deletes: Some(0),
        num_update_writes: Some(0),
        num_inserts: Some(num_rows),
        total_write_bytes: Some(file_size),
        total_write_errors: Some(0),
        partition_path: Some(String::new()),
        file_size_in_bytes: Some(file_size),
        num_updates: Some(0),
        ..Default::default()
    };

    HoodieCommitMetadata {
        version: Some(1),
        operation_type: Some("INSERT".to_string()),
        partition_to_write_stats: Some(HashMap::from([(String::new(), vec![write_stat])])),
        partition_to_replace_file_ids: None,
        compacted: Some(false),
        extra_metadata: None,
    }
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

    let record_key_fields: Vec<String> = table.hudi_configs.get(RecordKeyFields)?.into();
    if record_key_fields.len() != 1 {
        return Err(CoreError::Unsupported(
            "writes with meta fields currently require exactly one record key field".to_string(),
        ));
    }
    let record_key_field = &record_key_fields[0];
    batches
        .iter()
        .map(|batch| {
            if batch.column_by_name(MetaField::RecordKey.as_ref()).is_some() {
                return Ok(batch.clone());
            }
            let key_array = batch
                .column_by_name(record_key_field)
                .ok_or_else(|| {
                    CoreError::Schema(format!(
                        "Record key field '{record_key_field}' is missing from write batch"
                    ))
                })?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    CoreError::Unsupported(format!(
                        "Record key field '{record_key_field}' must be Utf8 for writes with meta fields"
                    ))
                })?;
            let keys: Vec<Option<&str>> = key_array.iter().collect();
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
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(vec![""; rows])),
                Arc::new(StringArray::from(vec![file_name; rows])),
            ];
            columns.extend(batch.columns().iter().cloned());
            RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)
                .map_err(CoreError::ArrowError)
        })
        .collect()
}
