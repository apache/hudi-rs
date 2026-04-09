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
//! including binary conversion (`toBinaryRow`), sealing (`seal`), and schema
//! management.
//!
//! In hudi-rs, the engine is always Arrow, so `RecordContext` is a concrete
//! struct. Binary conversion uses Arrow IPC serialization.

use crate::error::CoreError;
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use std::io::Cursor;

use super::buffered_record::BufferedRecord;

/// Record context for Arrow engine record operations.
///
/// Mirrors Java's `org.apache.hudi.common.engine.RecordContext<T>`.
///
/// ## Method mapping (Java → Rust):
/// - `getSchemaFromBufferRecord(BufferedRecord)` → `get_schema_from_buffer_record`
/// - `toBinaryRow(HoodieSchema, T)` → `to_binary_row`
/// - `seal(T)` → `seal`
#[derive(Debug, Clone)]
pub struct RecordContext;

impl RecordContext {
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
    /// For Spark: converts `InternalRow` → `UnsafeRow` (compact off-heap binary).
    /// For Arrow: serializes `RecordBatch` → Arrow IPC stream bytes.
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
    /// For Arrow: no-op — IPC bytes are already self-contained (owned `Vec<u8>`).
    pub fn seal(bytes: Vec<u8>) -> Vec<u8> {
        bytes
    }

    /// Deserialize binary IPC bytes back to a RecordBatch.
    ///
    /// This is the Rust-specific "unwrap" counterpart to `to_binary_row`.
    /// In Java, unwrapping is implicit because `UnsafeRow` IS an `InternalRow`.
    /// In Rust/Arrow, we must explicitly deserialize IPC bytes back to `RecordBatch`.
    pub fn from_binary(bytes: &[u8]) -> crate::Result<RecordBatch> {
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None)
            .map_err(|e| CoreError::ReadFileSliceError(format!("IPC reader creation failed: {e}")))?;
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
    use arrow_array::{Int64Array, StringArray};
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
}
