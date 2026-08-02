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

//! Ported from the merge-on-read reader. Nothing consumes it yet, so its
//! items are unreachable from the crate's call graph until the reader wires in.
#![allow(dead_code)]

//! Arrow IPC serialization for spilled records.
//!
//! These are the primitives the spill path uses to move a `RecordBatch` to disk
//! and back. They are pure functions over Arrow values — they read no reader
//! state — so they live here rather than on
//! [`RecordContext`](super::record_context::RecordContext), which is what
//! Java's `RecordContext.toBinaryRow` / `seal` correspond to.
//!
//! Keeping them separate also keeps the module graph acyclic: `buffered_record`
//! and the spill map need these functions, `record_context` needs
//! `buffered_record`, and routing the first two through `record_context` would
//! close a cycle.

use crate::Result;
use crate::error::CoreError;
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use std::io::Cursor;

/// Convert an engine record to binary format. **Spill-only (A1).**
///
/// Mirrors Java's `RecordContext.toBinaryRow(HoodieSchema, T)`.
///
/// For Spark: converts `InternalRow` -> `UnsafeRow` (compact off-heap binary).
/// For Arrow: serializes `RecordBatch` -> Arrow IPC stream bytes.
///
/// As of A2 this is NOT called on the in-memory merge hot path (the merge map
/// holds zero-copy [`RecordPayload::BatchRef`](super::buffered_record::RecordPayload::BatchRef)
/// entries). It is retained as the spill serialization primitive that A1's
/// `SpillableRecordMap` consumes when an entry is evicted to disk.
pub fn to_binary_row(_schema: &SchemaRef, record: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        // INVARIANT (not reachable-by-input): the writer is an in-memory
        // `Vec<u8>` whose `io::Write` impl is infallible, and `record` is an
        // already-validated Arrow batch produced by this reader's own decode
        // path. An IPC encode failure here would mean an internal bug
        // (a malformed batch we constructed), not bad external input — so a
        // panic is the correct signal rather than a `CoreError`.
        let mut writer =
            StreamWriter::try_new(&mut buf, &record.schema()).expect("IPC writer creation");
        writer.write(record).expect("IPC write");
        writer.finish().expect("IPC finish");
    }
    buf
}

/// Encode JUST the record-batch body (Arrow IPC RecordBatch message + buffers)
/// WITHOUT the schema. The schema is identical for every record in a file
/// group's spill map, so the map caches it once out-of-band and supplies it at
/// decode time ([`from_binary_body`]). This drops the per-record schema framing
/// that [`to_binary_row`] re-serializes into every blob (the dominant B5 spill
/// cost).
///
/// Returns `None` if the batch carries dictionaries (they would need their own
/// out-of-band messages) — the caller then falls back to the self-describing
/// [`to_binary_row`]. Blob layout: `[u32 LE ipc_message_len][ipc_message][arrow_data]`.
pub fn to_binary_row_body(record: &RecordBatch) -> Option<Vec<u8>> {
    let generator = arrow_ipc::writer::IpcDataGenerator {};
    let mut dict_tracker = arrow_ipc::writer::DictionaryTracker::new(false);
    let opts = arrow_ipc::writer::IpcWriteOptions::default();
    let mut compression_context = arrow_ipc::writer::CompressionContext::default();
    let (dictionaries, encoded) = generator
        .encode(record, &mut dict_tracker, &opts, &mut compression_context)
        .ok()?;
    if !dictionaries.is_empty() {
        return None; // dictionary columns: not body-only encodable here
    }
    let mut buf = Vec::with_capacity(4 + encoded.ipc_message.len() + encoded.arrow_data.len());
    buf.extend_from_slice(&(encoded.ipc_message.len() as u32).to_le_bytes());
    buf.extend_from_slice(&encoded.ipc_message);
    buf.extend_from_slice(&encoded.arrow_data);
    Some(buf)
}

/// Decode a body-only blob produced by [`to_binary_row_body`] against the spill
/// map's cached schema (fields resolved by position).
pub fn from_binary_body(bytes: &[u8], schema: SchemaRef) -> Result<RecordBatch> {
    if bytes.len() < 4 {
        return Err(CoreError::ReadFileSliceError(
            "spill body decode: truncated length header".to_string(),
        ));
    }
    let msg_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let msg_end = 4 + msg_len;
    if bytes.len() < msg_end {
        return Err(CoreError::ReadFileSliceError(
            "spill body decode: truncated IPC message".to_string(),
        ));
    }
    let message = arrow_ipc::root_as_message(&bytes[4..msg_end]).map_err(|e| {
        CoreError::ReadFileSliceError(format!("spill body decode: bad IPC message: {e}"))
    })?;
    let record_batch = message.header_as_record_batch().ok_or_else(|| {
        CoreError::ReadFileSliceError(
            "spill body decode: message header is not a RecordBatch".to_string(),
        )
    })?;
    let body = arrow_buffer::Buffer::from_vec(bytes[msg_end..].to_vec());
    arrow_ipc::reader::read_record_batch(
        &body,
        record_batch,
        schema,
        &std::collections::HashMap::new(),
        None,
        &message.version(),
    )
    .map_err(|e| {
        CoreError::ReadFileSliceError(format!("spill body decode: read_record_batch: {e}"))
    })
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

/// Deserialize binary IPC bytes back to a RecordBatch. **Spill-only (A1).**
///
/// This is the Rust-specific "unwrap" counterpart to [`to_binary_row`].
/// In Java, unwrapping is implicit because `UnsafeRow` IS an `InternalRow`.
/// In Rust/Arrow, we must explicitly deserialize IPC bytes back to `RecordBatch`.
///
/// As of A2 this is not used on the in-memory path; it is retained for A1's
/// spill reload path (reading an entry back from disk).
pub fn from_binary(bytes: &[u8]) -> Result<RecordBatch> {
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

        let bytes = to_binary_row(&schema, &batch);
        assert!(!bytes.is_empty());

        let sealed = seal(bytes);

        let restored = from_binary(&sealed).unwrap();
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
    }

    /// The body-only encoding carries no schema, so it only round-trips when the
    /// caller supplies the same schema it was written with. Nothing asserted this
    /// directly before — the pair was only exercised through the spill map.
    #[test]
    fn test_to_binary_row_body_and_from_binary_body_roundtrip() {
        let batch = make_test_batch();
        let schema = batch.schema();

        let bytes = to_binary_row_body(&batch).expect("no dictionaries, so body-only encodable");
        assert!(!bytes.is_empty());
        assert!(
            bytes.len() < to_binary_row(&schema, &batch).len(),
            "dropping the schema framing should make the blob smaller"
        );

        let restored = from_binary_body(&bytes, schema.clone()).unwrap();
        assert_eq!(restored.num_rows(), 1);
        assert_eq!(restored.schema(), schema);
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
    fn test_from_binary_body_rejects_truncated_input() {
        let schema = make_test_batch().schema();

        for truncated in [&[][..], &[1, 2, 3][..], &[255, 255, 255, 255][..]] {
            let err = from_binary_body(truncated, schema.clone()).unwrap_err();
            assert!(
                err.to_string().contains("spill body decode"),
                "error should name the decode step, got: {err}"
            );
        }
    }
}
