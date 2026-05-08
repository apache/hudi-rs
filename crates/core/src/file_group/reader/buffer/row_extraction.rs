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
//! Record-level operations (key extraction, ordering value extraction, batch-to-record
//! conversion) have moved to [`RecordContext`](crate::file_group::reader::record_context::RecordContext).
//!
//! This module retains Arrow batch-level utilities that don't need RecordContext:
//! - `slice_row` — zero-copy single-row extraction
//! - `records_to_batch` — reassemble BufferedRecords into a RecordBatch
//! - `reconcile_batch_to_schema` — handle Avro/Parquet schema differences

use crate::Result;
use crate::error::CoreError;
use crate::file_group::reader::buffered_record::BufferedRecord;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;

/// Slice a single row from a RecordBatch as a new single-row RecordBatch.
///
/// This is zero-copy (just adjusts array offsets in Arrow).
pub fn slice_row(batch: &RecordBatch, row_index: usize) -> RecordBatch {
    batch.slice(row_index, 1)
}

/// Reassemble a collection of BufferedRecords back into a single RecordBatch.
///
/// Concatenates all single-row batches from the records into one batch.
/// Records without data (deletes) are skipped.
pub fn records_to_batch(records: Vec<BufferedRecord>, schema: SchemaRef) -> Result<RecordBatch> {
    // Mirrors Java: BufferedRecord.getRecord() unwraps from binary if needed
    let batches: Vec<RecordBatch> = records.iter().filter_map(|r| r.get_record()).collect();

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    // Reconcile schemas: if a batch's schema differs from the target (e.g., Avro
    // log records vs Parquet base records have different field names for Map/List
    // children), rebuild each batch under the target schema.
    let reconciled: Vec<RecordBatch> = batches
        .into_iter()
        .map(|b| {
            if b.schema() == schema {
                b
            } else {
                reconcile_batch_to_schema(&b, &schema)
            }
        })
        .collect();

    let batch_refs: Vec<&RecordBatch> = reconciled.iter().collect();
    arrow::compute::concat_batches(&schema, batch_refs.into_iter())
        .map_err(|e| CoreError::ReadFileSliceError(format!("Failed to concat record batches: {e}")))
}

/// Reconcile a RecordBatch to a target schema.
///
/// Handles field name differences between Avro-derived schemas (from log files) and
/// Parquet-derived schemas (from base files). For example:
/// - List child field: Avro uses "element", Parquet uses "array"
/// - Map entries field: Avro uses "key_value", Parquet uses column name
///
/// Rebuilds each column's ArrayData with the target schema's field metadata.
fn reconcile_batch_to_schema(batch: &RecordBatch, target_schema: &SchemaRef) -> RecordBatch {
    let columns: Vec<ArrayRef> = target_schema
        .fields()
        .iter()
        .map(|target_field| {
            let idx = batch
                .schema()
                .index_of(target_field.name())
                .expect("column name must exist in source batch");
            let source_col = batch.column(idx);
            // If types match exactly, use as-is
            if source_col.data_type() == target_field.data_type() {
                source_col.clone()
            } else {
                // Try arrow_cast first; if it fails, rebuild the ArrayData
                // with the target DataType (changes field names but not actual data)
                arrow_cast::cast(source_col, target_field.data_type()).unwrap_or_else(|_| {
                    let source_data = source_col.to_data();
                    let rebuilt = source_data
                        .into_builder()
                        .data_type(target_field.data_type().clone())
                        .build();
                    match rebuilt {
                        Ok(data) => arrow_array::make_array(data),
                        Err(_) => source_col.clone(),
                    }
                })
            }
        })
        .collect();
    RecordBatch::try_new(target_schema.clone(), columns).unwrap_or_else(|_| batch.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::reader::record_context::RecordContext;
    use arrow_array::{Float64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

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
    fn test_slice_row() {
        let batch = make_test_batch(3);
        let row = slice_row(&batch, 1);
        assert_eq!(row.num_rows(), 1);
        let ctx = RecordContext::default();
        let keys = ctx.get_record_keys(&row).unwrap();
        assert_eq!(keys, vec!["key_1"]);
    }

    #[test]
    fn test_records_to_batch_roundtrip() {
        let batch = make_test_batch(3);
        let schema = batch.schema();
        let ctx = RecordContext::default();
        let records = ctx.batch_to_buffered_records(&batch, None).unwrap();
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
