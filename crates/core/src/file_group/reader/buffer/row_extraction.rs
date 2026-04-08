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

//! Utility functions to bridge Arrow columnar data with per-record operations.
//!
//! These helpers are used by `KeyBasedFileGroupRecordBuffer` to convert between
//! multi-row `RecordBatch` (Arrow's native format) and individual `BufferedRecord`
//! entries (needed for per-key merging in the record buffer's HashMap).

use crate::Result;
use crate::error::CoreError;
use crate::file_group::reader::buffered_record::{BufferedRecord, OrderingValue};
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, SchemaRef};

/// Extract record key strings from a RecordBatch column.
///
/// # Arguments
/// * `batch` - The RecordBatch to extract keys from
/// * `key_field` - The column name containing record keys (e.g. `_hoodie_record_key`)
pub fn extract_record_keys(batch: &RecordBatch, key_field: &str) -> Result<Vec<String>> {
    let col_idx = batch
        .schema()
        .index_of(key_field)
        .map_err(|e| CoreError::ReadFileSliceError(format!("Key field '{key_field}' not found in schema: {e}")))?;

    let key_array = batch
        .column(col_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            CoreError::ReadFileSliceError(format!("Key field '{key_field}' is not a StringArray"))
        })?;

    Ok((0..key_array.len())
        .map(|i| key_array.value(i).to_string())
        .collect())
}

/// Slice a single row from a RecordBatch as a new single-row RecordBatch.
///
/// This is zero-copy (just adjusts array offsets in Arrow).
pub fn slice_row(batch: &RecordBatch, row_index: usize) -> RecordBatch {
    batch.slice(row_index, 1)
}

/// Convert a multi-row RecordBatch into a Vec of (key, BufferedRecord) pairs.
///
/// Each row becomes an individual `BufferedRecord` with a single-row `RecordBatch`
/// as its data payload.
pub fn batch_to_buffered_records(
    batch: &RecordBatch,
    key_field: &str,
    ordering_field_names: &[String],
) -> Result<Vec<(String, BufferedRecord)>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let keys = extract_record_keys(batch, key_field)?;
    let mut records = Vec::with_capacity(batch.num_rows());

    // Extract ordering values from the first ordering field (if any)
    let ordering_values = extract_ordering_values(batch, ordering_field_names);

    for (row_idx, key) in keys.into_iter().enumerate() {
        let row_batch = slice_row(batch, row_idx);
        let ordering_value = ordering_values
            .as_ref()
            .and_then(|vals| vals.get(row_idx).cloned())
            .flatten();
        let record = BufferedRecord::new_data(key.clone(), row_batch, ordering_value);
        records.push((key, record));
    }

    Ok(records)
}

/// Extract ordering values from the first ordering field column.
///
/// Supports Int32, Int64, and Utf8 column types.
/// Returns None if no ordering fields specified or column not found.
fn extract_ordering_values(
    batch: &RecordBatch,
    ordering_field_names: &[String],
) -> Option<Vec<Option<OrderingValue>>> {
    let field_name = ordering_field_names.first()?;
    let col_idx = batch.schema().index_of(field_name).ok()?;
    let col = batch.column(col_idx);

    let values: Vec<Option<OrderingValue>> = match col.data_type() {
        DataType::Int64 => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()?;
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(OrderingValue::Long(arr.value(i)))
                    }
                })
                .collect()
        }
        DataType::Int32 => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()?;
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(OrderingValue::Long(arr.value(i) as i64))
                    }
                })
                .collect()
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>()?;
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(OrderingValue::String(arr.value(i).to_string()))
                    }
                })
                .collect()
        }
        _ => return None,
    };
    Some(values)
}

/// Convert a delete RecordBatch into a Vec of (key, BufferedRecord) pairs.
///
/// Delete batches have schema: (recordKey, partitionPath, orderingVal).
pub fn delete_batch_to_buffered_records(
    batch: &RecordBatch,
) -> Result<Vec<(String, BufferedRecord)>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    // Delete batch schema: recordKey (col 0), partitionPath (col 1), orderingVal (col 2)
    let key_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            CoreError::ReadFileSliceError("Delete batch column 0 is not a StringArray".to_string())
        })?;

    let mut records = Vec::with_capacity(batch.num_rows());
    for i in 0..key_array.len() {
        let key = key_array.value(i).to_string();
        // TODO: Extract ordering value from column 2
        let record = BufferedRecord::new_delete(key.clone(), None);
        records.push((key, record));
    }

    Ok(records)
}

/// Reassemble a collection of BufferedRecords back into a single RecordBatch.
///
/// Concatenates all single-row batches from the records into one batch.
/// Records without data (deletes) are skipped.
pub fn records_to_batch(
    records: Vec<BufferedRecord>,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let batches: Vec<&RecordBatch> = records
        .iter()
        .filter_map(|r| r.data.as_ref())
        .collect();

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    arrow::compute::concat_batches(&schema, batches.iter().copied())
        .map_err(|e| CoreError::ReadFileSliceError(format!("Failed to concat record batches: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Float64Array;
    use std::sync::Arc;
    use arrow_schema::{DataType, Field, Schema};

    fn make_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
        ]));

        let keys: Vec<String> = (0..num_rows).map(|i| format!("key_{i}")).collect();
        let names: Vec<Option<String>> = (0..num_rows).map(|i| Some(format!("name_{i}"))).collect();
        let values: Vec<f64> = (0..num_rows).map(|i| i as f64).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_extract_record_keys() {
        let batch = make_test_batch(3);
        let keys = extract_record_keys(&batch, "_hoodie_record_key").unwrap();
        assert_eq!(keys, vec!["key_0", "key_1", "key_2"]);
    }

    #[test]
    fn test_extract_record_keys_missing_field() {
        let batch = make_test_batch(1);
        let result = extract_record_keys(&batch, "nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_slice_row() {
        let batch = make_test_batch(3);
        let row = slice_row(&batch, 1);
        assert_eq!(row.num_rows(), 1);
        let keys = extract_record_keys(&row, "_hoodie_record_key").unwrap();
        assert_eq!(keys, vec!["key_1"]);
    }

    #[test]
    fn test_batch_to_buffered_records() {
        let batch = make_test_batch(3);
        let records =
            batch_to_buffered_records(&batch, "_hoodie_record_key", &[]).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].0, "key_0");
        assert_eq!(records[1].0, "key_1");
        assert_eq!(records[2].0, "key_2");
        assert!(!records[0].1.is_delete());
        assert!(records[0].1.data.is_some());
    }

    #[test]
    fn test_batch_to_buffered_records_empty() {
        let batch = make_test_batch(0);
        let records =
            batch_to_buffered_records(&batch, "_hoodie_record_key", &[]).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_records_to_batch_roundtrip() {
        let batch = make_test_batch(3);
        let schema = batch.schema();
        let records =
            batch_to_buffered_records(&batch, "_hoodie_record_key", &[]).unwrap();
        let buffered: Vec<BufferedRecord> = records.into_iter().map(|(_, r)| r).collect();

        let result = records_to_batch(buffered, schema.clone()).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.schema(), schema);
    }

    #[test]
    fn test_records_to_batch_empty() {
        let schema = make_test_batch(0).schema();
        let result = records_to_batch(vec![], schema.clone()).unwrap();
        assert_eq!(result.num_rows(), 0);
    }
}
