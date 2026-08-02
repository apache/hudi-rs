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

//! Decoding of the `RECORD_POSITIONS` log-block header for position-based merge.
//!
//! A log block that was written with record positions carries two headers:
//!
//! - `RECORD_POSITIONS` (key 5): a Base64-encoded, portable-serialized
//!   `Roaring64NavigableMap` bitmap holding the positions (row indices in the
//!   base file) of the records in the block. This mirrors Java
//!   `LogReaderUtils.encodePositions` / `decodeRecordPositionsHeader`, which use
//!   `Roaring64NavigableMap.serializePortable` (the CRoaring "portable" 64-bit
//!   format) after `runOptimize()`.
//! - `BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS` (key 8): the commit/instant
//!   time of the base file those positions were computed against. Positions are
//!   only valid when this matches the base file being merged.
//!
//! The Rust [`roaring`] crate's [`RoaringTreemap`] (de)serialization is
//! documented as compatible with the official C/C++, Java, and Go
//! implementations, so it decodes Java's `serializePortable` output directly.
//! The base64 alphabet is the standard (non-URL) alphabet, matching Java's
//! `java.util.Base64` used by Hudi's `Base64CodecUtil`.

use arrow_array::{Array, Int64Array, RecordBatch};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use roaring::RoaringTreemap;

use crate::Result;
use crate::error::CoreError;

/// Name of the internal, synthetic column carrying each base-file row's TRUE
/// physical position, used only for position-based merge. Mirrors Java
/// `PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME`. It is
/// appended to the base-file read (via a parquet virtual `RowNumber` column),
/// consumed by the position buffer to match base rows to log records, and
/// stripped before the merged output — it never appears in the reader's output.
pub const ROW_INDEX_TEMPORARY_COLUMN_NAME: &str = "_tmp_metadata_row_index";

/// Decode the Base64 + portable-Roaring64 `RECORD_POSITIONS` header value into
/// its record positions, in ascending order (the bitmap iteration order).
///
/// # Errors
///
/// Returns [`CoreError::LogBlockError`] if the value is not valid Base64 or not
/// a valid portable Roaring64 bitmap.
pub fn decode_record_positions(encoded: &str) -> Result<Vec<u64>> {
    let bytes = STANDARD.decode(encoded.trim()).map_err(|e| {
        CoreError::LogBlockError(format!("RECORD_POSITIONS header is not valid base64: {e}"))
    })?;
    let bitmap = RoaringTreemap::deserialize_from(bytes.as_slice()).map_err(|e| {
        CoreError::LogBlockError(format!(
            "RECORD_POSITIONS header is not a valid portable Roaring64 bitmap: {e}"
        ))
    })?;
    Ok(bitmap.iter().collect())
}

/// Borrow the base batch's synthetic row-index column
/// ([`ROW_INDEX_TEMPORARY_COLUMN_NAME`]) as an `Int64Array`. Each value is the
/// row's TRUE physical position in the base file. Returns an error if the column
/// is absent (position-based merge requires it on the base read) or not `Int64`.
pub(super) fn base_row_position_array(batch: &RecordBatch) -> Result<&Int64Array> {
    let col = batch
        .column_by_name(ROW_INDEX_TEMPORARY_COLUMN_NAME)
        .ok_or_else(|| {
            CoreError::ReadFileSliceError(format!(
                "position-based merge: base batch is missing the '{ROW_INDEX_TEMPORARY_COLUMN_NAME}' \
                 column (the parquet virtual row-number column was not attached)"
            ))
        })?;
    col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        CoreError::ReadFileSliceError(format!(
            "position-based merge: '{ROW_INDEX_TEMPORARY_COLUMN_NAME}' column has type {:?}, expected Int64",
            col.data_type()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;

    /// Encode positions the way Hudi's Java writer does (portable Roaring64 +
    /// standard base64), so the decode test exercises the real wire format.
    fn encode_positions(positions: &[u64]) -> String {
        let mut bitmap = RoaringTreemap::new();
        for &p in positions {
            bitmap.insert(p);
        }
        let mut buf = Vec::with_capacity(bitmap.serialized_size());
        bitmap.serialize_into(&mut buf).unwrap();
        STANDARD.encode(buf)
    }

    #[test]
    fn round_trips_sparse_positions() {
        let positions = vec![0u64, 3, 7, 42, 1000, 65_537];
        let encoded = encode_positions(&positions);
        let decoded = decode_record_positions(&encoded).unwrap();
        assert_eq!(decoded, positions);
    }

    #[test]
    fn round_trips_empty() {
        let encoded = encode_positions(&[]);
        let decoded = decode_record_positions(&encoded).unwrap();
        assert_eq!(decoded, Vec::<u64>::new());
    }

    #[test]
    fn decodes_ascending_regardless_of_insert_order() {
        let encoded = encode_positions(&[100, 5, 5, 1, 50]);
        let decoded = decode_record_positions(&encoded).unwrap();
        assert_eq!(decoded, vec![1, 5, 50, 100]);
    }

    /// Java calls `runOptimize()` before serializing, so a block covering a
    /// contiguous run of base-file rows (the common case) is written with a RUN
    /// container. Build that exact payload — a run-optimized 32-bit bitmap wrapped
    /// in the portable 64-bit envelope (one bucket, high key 0) — and assert the
    /// decoder handles RUN containers.
    #[test]
    fn decodes_run_container_from_contiguous_range() {
        let range: Vec<u64> = (0..5000).collect();

        let mut low = RoaringBitmap::new();
        low.insert_range(0..5000);
        // Java calls runOptimize() before serializing; do the same so the block
        // is serialized with a RUN container (as it would be on the wire).
        low.optimize();
        let mut low_bytes = Vec::with_capacity(low.serialized_size());
        low.serialize_into(&mut low_bytes).unwrap();

        // Portable Roaring64 envelope: u64 LE bucket count, then per bucket a
        // u32 LE high key followed by the portable 32-bit bitmap bytes.
        let mut payload = Vec::new();
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&low_bytes);

        let encoded = STANDARD.encode(payload);
        let decoded = decode_record_positions(&encoded).unwrap();
        assert_eq!(decoded, range);
    }

    #[test]
    fn rejects_invalid_base64() {
        let err = decode_record_positions("not valid base64!!!").unwrap_err();
        assert!(matches!(err, CoreError::LogBlockError(_)));
    }

    #[test]
    fn rejects_valid_base64_that_is_not_a_bitmap() {
        let encoded = STANDARD.encode([0xDE, 0xAD, 0xBE, 0xEF]);
        let err = decode_record_positions(&encoded).unwrap_err();
        assert!(matches!(err, CoreError::LogBlockError(_)));
    }
}
