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

//! Mirrors `org.apache.hudi.common.engine.RecordContext<T>`.
//!
//! In Java Hudi, `RecordContext<T>` is an abstract class with engine-specific
//! implementations (Spark, Flink, Avro). It provides APIs for record operations
//! including key extraction, ordering value extraction, delete detection,
//! binary conversion (`toBinaryRow`), sealing (`seal`), and schema management.
//!
//! In hudi-rs, the engine is always Arrow, so `RecordContext` is a concrete
//! struct. Binary conversion uses Arrow IPC serialization.

use crate::Result;
use crate::config::table::HudiTableConfig;
use crate::error::CoreError;
use crate::file_group::reader::buffered_record::{BufferedRecord, OrderingValue};
use crate::metadata::meta_field::MetaField;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, SchemaRef};
use std::collections::HashMap;
use std::io::Cursor;

use super::buffer::row_extraction::slice_row;

/// Record context for Arrow engine record operations.
///
/// Mirrors Java's `org.apache.hudi.common.engine.RecordContext<T>`.
///
/// In Java, `RecordContext` is the engine-specific "glue" that lets the
/// engine-agnostic `FileGroupRecordBuffer` and `BufferedRecordMerger`
/// infrastructure manipulate engine records without knowing the type system.
///
/// ## Instance methods (record-level operations needing config):
/// - `get_record_keys` — mirrors `RecordContext.getRecordKey(T, Schema)`
/// - `get_ordering_values` — mirrors `RecordContext.getOrderingValue(T, Schema, List<String>)`
/// - `batch_to_buffered_records` — combines key + ordering extraction
/// - `delete_batch_to_buffered_records` — converts delete batches
/// - `is_delete_record` — mirrors `RecordContext.isDeleteRecord(T, DeleteContext)`
///
/// ## Associated functions (engine-level, no config needed):
/// - `to_binary_row` — mirrors `RecordContext.toBinaryRow(Schema, T)`
/// - `seal` — mirrors `RecordContext.seal(T)`
/// - `from_binary` — Arrow-specific IPC deserialization
/// - `get_schema_from_buffer_record` — mirrors `RecordContext.getSchemaFromBufferRecord`
#[derive(Debug, Clone)]
pub struct RecordContext {
    /// Which column contains the record key (e.g. `_hoodie_record_key` or a PK column).
    /// Mirrors Java's `recordKeyExtractor` strategy.
    pub record_key_field: String,

    /// Ordering (precombine) field names for conflict resolution.
    /// Mirrors Java's `orderingFieldNames` used in `getOrderingValue()`.
    pub ordering_field_names: Vec<String>,

    /// Whether meta fields are populated (affects key extraction strategy).
    /// When true, reads `_hoodie_record_key` directly.
    /// When false, computes key from configured record key columns.
    pub populate_meta_fields: bool,

    /// Partition path for constructing delete rows.
    /// Mirrors Java's `RecordContext.partitionPath`.
    pub partition_path: String,
}

impl Default for RecordContext {
    fn default() -> Self {
        Self::new(&HashMap::new(), String::new())
    }
}

impl RecordContext {
    /// Create a new RecordContext from table configuration.
    ///
    /// Mirrors Java's `RecordContext(HoodieTableConfig tableConfig, JavaTypeConverter typeConverter)`.
    ///
    /// In Java, the constructor reads `tableConfig.populateMetaFields()` to choose
    /// the key extraction strategy (metadata vs virtual keys), and later
    /// `initOrderingValueConverter(schema, orderingFieldNames)` is called via
    /// `setSchemaHandler()`.
    ///
    /// In hudi-rs, we derive all fields from the `table_config` map in one shot:
    /// - `populate_meta_fields` from `hoodie.populate.meta.fields` (default: true)
    /// - `record_key_field` from `_hoodie_record_key` (meta) or `hoodie.table.recordkey.fields` (virtual)
    /// - `ordering_field_names` from `hoodie.table.precombine.field` or `hoodie.table.ordering.fields`
    pub fn new(table_config: &HashMap<String, String>, partition_path: String) -> Self {
        let populate_meta_fields = table_config
            .get(HudiTableConfig::PopulatesMetaFields.as_ref())
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);

        let record_key_field = if populate_meta_fields {
            MetaField::RecordKey.as_ref().to_string()
        } else {
            // Virtual keys mode: use the first record key field from config.
            // Java: virtualKeyExtractor(tableConfig.getRecordKeyFields())
            table_config
                .get(HudiTableConfig::RecordKeyFields.as_ref())
                .and_then(|fields| fields.split(',').next().map(|f| f.trim().to_string()))
                .unwrap_or_else(|| MetaField::RecordKey.as_ref().to_string())
        };

        let ordering_field_names = table_config
            .get(HudiTableConfig::PrecombineField.as_ref())
            .or_else(|| table_config.get("hoodie.table.ordering.fields"))
            .map(|f| vec![f.clone()])
            .unwrap_or_default();

        Self {
            record_key_field,
            ordering_field_names,
            populate_meta_fields,
            partition_path,
        }
    }

    // =========================================================================
    // Instance methods — record-level operations (mirrors Java RecordContext<T>)
    // =========================================================================

    /// Extract record key strings from a RecordBatch column.
    ///
    /// Mirrors Java's `RecordContext.getRecordKey(T record, Schema schema)`,
    /// applied to each row in the batch.
    ///
    /// Uses `self.record_key_field` to locate the key column.
    pub fn get_record_keys(&self, batch: &RecordBatch) -> Result<Vec<String>> {
        let col_idx = batch.schema().index_of(&self.record_key_field).map_err(|e| {
            CoreError::ReadFileSliceError(format!(
                "Key field '{}' not found in schema: {e}",
                self.record_key_field
            ))
        })?;

        let key_array = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                CoreError::ReadFileSliceError(format!(
                    "Key field '{}' is not a StringArray",
                    self.record_key_field
                ))
            })?;

        Ok((0..key_array.len())
            .map(|i| key_array.value(i).to_string())
            .collect())
    }

    /// Extract ordering values from the first ordering field column.
    ///
    /// Mirrors Java's `RecordContext.getOrderingValue(T, Schema, List<String>)`,
    /// applied to each row in the batch.
    ///
    /// Uses `self.ordering_field_names` to locate the ordering column.
    /// Supports Int32, Int64, and Utf8 column types.
    /// Returns None if no ordering fields specified or column not found.
    pub fn get_ordering_values(
        &self,
        batch: &RecordBatch,
    ) -> Option<Vec<Option<OrderingValue>>> {
        let field_name = self.ordering_field_names.first()?;
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

    /// Convert a multi-row RecordBatch into a Vec of (key, BufferedRecord) pairs.
    ///
    /// Each row becomes an individual `BufferedRecord` with a single-row `RecordBatch`
    /// as its data payload. Combines `get_record_keys` + `get_ordering_values`.
    ///
    /// Mirrors the Java pattern:
    /// ```text
    /// for each record in dataBlock.getEngineRecordIterator():
    ///     key = recordContext.getRecordKey(record, schema)
    ///     ordering = recordContext.getOrderingValue(record, schema, orderingFields)
    ///     buffered = BufferedRecords.fromEngineRecord(record, key, ordering, ...)
    /// ```
    pub fn batch_to_buffered_records(
        &self,
        batch: &RecordBatch,
    ) -> Result<Vec<(String, BufferedRecord)>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        let keys = self.get_record_keys(batch)?;
        let ordering_values = self.get_ordering_values(batch);
        let mut records = Vec::with_capacity(batch.num_rows());

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

    /// Convert a delete RecordBatch into a Vec of (key, BufferedRecord) pairs.
    ///
    /// Delete batches have schema: (recordKey, partitionPath, orderingVal).
    ///
    /// Mirrors the Java pattern:
    /// ```text
    /// for each deleteRecord in deleteBlock.getRecordsToDelete():
    ///     ordering = recordContext.getOrderingValue(deleteRecord)
    ///     buffered = BufferedRecords.fromDeleteRecord(deleteRecord, ordering, ...)
    /// ```
    pub fn delete_batch_to_buffered_records(
        &self,
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
                CoreError::ReadFileSliceError(
                    "Delete batch column 0 is not a StringArray".to_string(),
                )
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

    /// Check if a record is a delete record.
    ///
    /// Mirrors Java's `RecordContext.isDeleteRecord(T record, DeleteContext deleteContext)`.
    ///
    /// Checks (in order):
    /// 1. `_hoodie_is_deleted` field is true (built-in delete marker)
    /// 2. `_hoodie_operation` field is DELETE or UPDATE_BEFORE
    ///
    /// Returns false if the batch has no delete markers.
    pub fn is_delete_record(&self, batch: &RecordBatch, row_idx: usize) -> bool {
        let schema = batch.schema();

        // Check _hoodie_is_deleted field
        if let Some((idx, _)) = schema.column_with_name("_hoodie_is_deleted") {
            if let Some(arr) = batch
                .column(idx)
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
            {
                if !arr.is_null(row_idx) && arr.value(row_idx) {
                    return true;
                }
            }
        }

        // Check _hoodie_operation field for DELETE or UPDATE_BEFORE
        if let Some((idx, _)) = schema.column_with_name("_hoodie_operation") {
            if let Some(arr) = batch
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
            {
                if !arr.is_null(row_idx) {
                    let op = arr.value(row_idx);
                    if op == "DELETE" || op == "UPDATE_BEFORE" {
                        return true;
                    }
                }
            }
        }

        false
    }

    // =========================================================================
    // Associated functions — engine-level operations (no config needed)
    // =========================================================================

    /// Get the schema from a buffered record's data.
    ///
    /// Mirrors Java's `RecordContext.getSchemaFromBufferRecord(BufferedRecord)`.
    ///
    /// In Java, this decodes the schema from the record's schema ID via a
    /// schema registry. In Arrow, we extract the schema directly from the
    /// RecordBatch.
    pub fn get_schema_from_buffer_record(record: &BufferedRecord) -> Option<SchemaRef> {
        record.data.as_ref().map(|batch| batch.schema())
    }

    /// Convert an engine record to binary format.
    ///
    /// Mirrors Java's `RecordContext.toBinaryRow(HoodieSchema, T)`.
    ///
    /// For Spark: converts `InternalRow` -> `UnsafeRow` (compact off-heap binary).
    /// For Arrow: serializes `RecordBatch` -> Arrow IPC stream bytes.
    pub fn to_binary_row(_schema: &SchemaRef, record: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut buf, &record.schema()).expect("IPC writer creation");
            writer.write(record).expect("IPC write");
            writer.finish().expect("IPC finish");
        }
        buf
    }

    /// Seal the binary record to ensure data referenced in memory does not change.
    ///
    /// Mirrors Java's `RecordContext.seal(T)`.
    ///
    /// For Spark: calls `UnsafeRow.copy()` to make the row self-contained.
    /// For Arrow: no-op -- IPC bytes are already self-contained (owned `Vec<u8>`).
    pub fn seal(bytes: Vec<u8>) -> Vec<u8> {
        bytes
    }

    /// Deserialize binary IPC bytes back to a RecordBatch.
    ///
    /// This is the Rust-specific "unwrap" counterpart to `to_binary_row`.
    /// In Java, unwrapping is implicit because `UnsafeRow` IS an `InternalRow`.
    /// In Rust/Arrow, we must explicitly deserialize IPC bytes back to `RecordBatch`.
    pub fn from_binary(bytes: &[u8]) -> Result<RecordBatch> {
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).map_err(|e| {
            CoreError::ReadFileSliceError(format!("IPC reader creation failed: {e}"))
        })?;
        reader
            .next()
            .ok_or_else(|| {
                CoreError::ReadFileSliceError("IPC stream contained no batches".to_string())
            })?
            .map_err(|e| CoreError::ReadFileSliceError(format!("IPC read failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["k1"])),
                Arc::new(Int64Array::from(vec![42])),
            ],
        )
        .unwrap()
    }

    fn make_record_context() -> RecordContext {
        let table_config = HashMap::from([
            (HudiTableConfig::PopulatesMetaFields.as_ref().to_string(), "true".to_string()),
            (HudiTableConfig::PrecombineField.as_ref().to_string(), "ts".to_string()),
        ]);
        RecordContext::new(&table_config, "partition/path".to_string())
    }

    fn make_keyed_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, false),
        ]));
        let keys: Vec<String> = (0..num_rows).map(|i| format!("key_{i}")).collect();
        let names: Vec<Option<String>> =
            (0..num_rows).map(|i| Some(format!("name_{i}"))).collect();
        let timestamps: Vec<i64> = (0..num_rows).map(|i| i as i64).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(timestamps)),
            ],
        )
        .unwrap()
    }

    // =========================================================================
    // Associated function tests (IPC roundtrip, schema extraction)
    // =========================================================================

    #[test]
    fn test_to_binary_row_and_from_binary_roundtrip() {
        let batch = make_test_batch();
        let schema = batch.schema();

        let bytes = RecordContext::to_binary_row(&schema, &batch);
        assert!(!bytes.is_empty());

        let sealed = RecordContext::seal(bytes);

        let restored = RecordContext::from_binary(&sealed).unwrap();
        assert_eq!(restored.num_rows(), 1);
        assert_eq!(restored.schema(), schema);
        assert_eq!(
            restored
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "k1"
        );
        assert_eq!(
            restored
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );
    }

    #[test]
    fn test_get_schema_from_buffer_record() {
        let batch = make_test_batch();
        let schema = batch.schema();
        let record = BufferedRecord::new_data("k1".to_string(), batch, None);
        let extracted = RecordContext::get_schema_from_buffer_record(&record);
        assert_eq!(extracted.unwrap(), schema);
    }

    #[test]
    fn test_get_schema_from_delete_record_returns_none() {
        let record = BufferedRecord::new_delete("k1".to_string(), None);
        let extracted = RecordContext::get_schema_from_buffer_record(&record);
        assert!(extracted.is_none());
    }

    // =========================================================================
    // Instance method tests (key extraction, ordering, batch conversion)
    // =========================================================================

    #[test]
    fn test_get_record_keys() {
        let ctx = make_record_context();
        let batch = make_keyed_batch(3);
        let keys = ctx.get_record_keys(&batch).unwrap();
        assert_eq!(keys, vec!["key_0", "key_1", "key_2"]);
    }

    #[test]
    fn test_get_record_keys_missing_field() {
        // Virtual keys mode with a nonexistent field
        let table_config = HashMap::from([
            (HudiTableConfig::PopulatesMetaFields.as_ref().to_string(), "false".to_string()),
            (HudiTableConfig::RecordKeyFields.as_ref().to_string(), "nonexistent".to_string()),
        ]);
        let ctx = RecordContext::new(&table_config, String::new());
        let batch = make_keyed_batch(1);
        assert!(ctx.get_record_keys(&batch).is_err());
    }

    #[test]
    fn test_get_ordering_values_int64() {
        let ctx = make_record_context();
        let batch = make_keyed_batch(3);
        let values = ctx.get_ordering_values(&batch).unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Some(OrderingValue::Long(0)));
        assert_eq!(values[1], Some(OrderingValue::Long(1)));
        assert_eq!(values[2], Some(OrderingValue::Long(2)));
    }

    #[test]
    fn test_get_ordering_values_no_fields() {
        // No precombine/ordering field → empty ordering_field_names
        let ctx = RecordContext::new(&HashMap::new(), String::new());
        let batch = make_keyed_batch(3);
        assert!(ctx.get_ordering_values(&batch).is_none());
    }

    #[test]
    fn test_batch_to_buffered_records() {
        let ctx = make_record_context();
        let batch = make_keyed_batch(3);
        let records = ctx.batch_to_buffered_records(&batch).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].0, "key_0");
        assert_eq!(records[1].0, "key_1");
        assert_eq!(records[2].0, "key_2");
        assert!(!records[0].1.is_delete());
        assert!(!records[0].1.is_empty());
        // Check ordering values were extracted
        assert_eq!(
            records[0].1.ordering_value,
            Some(OrderingValue::Long(0))
        );
    }

    #[test]
    fn test_batch_to_buffered_records_empty() {
        let ctx = make_record_context();
        let batch = make_keyed_batch(0);
        let records = ctx.batch_to_buffered_records(&batch).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_is_delete_record_hoodie_is_deleted() {
        let ctx = RecordContext::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("_hoodie_is_deleted", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["k1", "k2"])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
            ],
        )
        .unwrap();

        assert!(ctx.is_delete_record(&batch, 0));
        assert!(!ctx.is_delete_record(&batch, 1));
    }

    #[test]
    fn test_is_delete_record_hoodie_operation() {
        let ctx = RecordContext::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("_hoodie_operation", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["k1", "k2", "k3"])),
                Arc::new(StringArray::from(vec![
                    Some("DELETE"),
                    Some("UPDATE_BEFORE"),
                    Some("INSERT"),
                ])),
            ],
        )
        .unwrap();

        assert!(ctx.is_delete_record(&batch, 0));
        assert!(ctx.is_delete_record(&batch, 1));
        assert!(!ctx.is_delete_record(&batch, 2));
    }

    #[test]
    fn test_is_delete_record_no_markers() {
        let ctx = RecordContext::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["k1"])),
                Arc::new(Int64Array::from(vec![42])),
            ],
        )
        .unwrap();

        assert!(!ctx.is_delete_record(&batch, 0));
    }

    #[test]
    fn test_default_record_context() {
        let ctx = RecordContext::default();
        assert_eq!(ctx.record_key_field, "_hoodie_record_key");
        assert!(ctx.ordering_field_names.is_empty());
        assert!(ctx.populate_meta_fields);
        assert!(ctx.partition_path.is_empty());
    }
}
