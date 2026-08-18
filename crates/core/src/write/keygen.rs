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
//! Derive Hoodie record keys and hive-style partition paths from write batches.

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig::{
    IsHiveStylePartitioning, PartitionFields, RecordKeyFields,
};
use crate::error::CoreError;
use crate::index::HoodieKey;

/// Build per-row [`HoodieKey`]s from a write batch.
///
/// When `hoodie.table.recordkey.fields` is unset/empty, keys are auto-generated
/// like Java `AutoRecordGenWrapper*KeyGenerator`: `{instant}_{partitionId}_{rowId}`.
///
/// `row_id_offset` continues the auto-key row counter across size-split chunks and
/// partitions within one commit. `partition_id` distinguishes writer partitions
/// (Java `HoodieRecord.generateSequenceId`).
///
/// Partition paths use hive-style (`field=value/...`) when
/// `hoodie.datasource.write.hive_style_partitioning` is true; otherwise value-only
/// segments. Null partition values are rejected. Partition columns are not stripped
/// from the batch — callers must leave them in the written Parquet.
pub fn hoodie_keys_for_batch(
    hudi_configs: &HudiConfigs,
    batch: &RecordBatch,
    instant: Option<&str>,
) -> Result<Vec<HoodieKey>> {
    hoodie_keys_for_batch_with_offset(hudi_configs, batch, instant, 0, 0)
}

/// Like [`hoodie_keys_for_batch`], with an explicit auto-key row offset and partition id.
pub fn hoodie_keys_for_batch_with_offset(
    hudi_configs: &HudiConfigs,
    batch: &RecordBatch,
    instant: Option<&str>,
    row_id_offset: usize,
    partition_id: u32,
) -> Result<Vec<HoodieKey>> {
    let record_key_fields: Option<Vec<String>> =
        hudi_configs.try_get(RecordKeyFields)?.map(|v| v.into());
    let auto_keys = record_key_fields
        .as_ref()
        .map(|f| f.is_empty() || (f.len() == 1 && f[0].is_empty()))
        .unwrap_or(true);

    let partition_fields: Vec<String> = hudi_configs.get_or_default(PartitionFields).into();
    let hive_style: bool = hudi_configs.get_or_default(IsHiveStylePartitioning).into();

    if auto_keys {
        let instant = instant.ok_or_else(|| {
            CoreError::Write(
                "auto record-key generation requires a commit instant timestamp".to_string(),
            )
        })?;
        let mut keys = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            // Match Java HoodieRecord.generateSequenceId(instant, partitionId, rowId).
            let record_key = format!("{instant}_{partition_id}_{}", row_id_offset + row);
            let partition_path = partition_path_for_row(batch, row, &partition_fields, hive_style)?;
            keys.push(HoodieKey {
                record_key,
                partition_path,
            });
        }
        return Ok(keys);
    }

    let fields = record_key_fields.unwrap();
    if fields.len() != 1 {
        return Err(CoreError::Unsupported(
            "writes currently require exactly one record key field (or none for auto keys)"
                .to_string(),
        ));
    }
    let key_name = &fields[0];
    let key_array = batch
        .column_by_name(key_name)
        .ok_or_else(|| {
            CoreError::Schema(format!(
                "record key field '{key_name}' is missing from write batch"
            ))
        })?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            CoreError::Unsupported(format!(
                "record key field '{key_name}' must be Utf8 for writes"
            ))
        })?;

    let mut keys = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if key_array.is_null(row) {
            return Err(CoreError::Write(
                "record key values must be non-null".to_string(),
            ));
        }
        let record_key = key_array.value(row).to_string();
        let partition_path = partition_path_for_row(batch, row, &partition_fields, hive_style)?;
        keys.push(HoodieKey {
            record_key,
            partition_path,
        });
    }
    Ok(keys)
}

fn partition_path_for_row(
    batch: &RecordBatch,
    row: usize,
    partition_fields: &[String],
    hive_style: bool,
) -> Result<String> {
    if partition_fields.is_empty() {
        return Ok(String::new());
    }
    let mut segments = Vec::with_capacity(partition_fields.len());
    for field in partition_fields {
        let column = batch.column_by_name(field).ok_or_else(|| {
            CoreError::Schema(format!(
                "partition field '{field}' is missing from write batch"
            ))
        })?;
        if column.is_null(row) {
            return Err(CoreError::Write(format!(
                "partition field '{field}' must be non-null"
            )));
        }
        let value = array_value_as_string(column.as_ref(), row)?;
        if hive_style {
            segments.push(format!("{field}={value}"));
        } else {
            segments.push(value);
        }
    }
    Ok(segments.join("/"))
}

fn array_value_as_string(array: &dyn Array, row: usize) -> Result<String> {
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(strings.value(row).to_string());
    }
    use arrow::array::{Int32Array, Int64Array};
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(values.value(row).to_string());
    }
    Err(CoreError::Unsupported(
        "partition field types currently support Utf8, Int32, and Int64".to_string(),
    ))
}

/// Join a partition path and file name into a table-relative path.
pub fn relative_data_path(partition_path: &str, file_name: &str) -> String {
    if partition_path.is_empty() {
        file_name.to_string()
    } else {
        format!("{partition_path}/{file_name}")
    }
}
