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

//! Mirrors `org.apache.hudi.common.table.read.BufferedRecord` and
//! `org.apache.hudi.common.model.DeleteRecord`.
//!
//! In Java Hudi, `BufferedRecord<T>` wraps a single engine-native record
//! with its key, ordering value, and operation type. In hudi-rs, the
//! "engine record" is an Arrow `RecordBatch`.
//!
//! During log scanning, records are accumulated into the record buffer.
//! At read time, base file records are merged with log records to produce
//! the final output.
//!
//! ## In-memory representation (A2)
//!
//! Per the A2 alignment work (05-a2a1-design.md), the in-memory payload is a
//! zero-copy [`RecordPayload::BatchRef`] into the shared source batch that the
//! row was decoded from — NOT a per-row Arrow IPC blob. IPC serialization is a
//! spill-only concern (A1) and lives in
//! [`row_serde`](super::row_serde) (`to_binary_row` /
//! `from_binary`); it is no longer paid on the in-memory merge hot path. The
//! data-driven motivation: the per-row IPC blob cost ~27x the build memory and
//! ~12x the build CPU of the batch-ref representation (spike Part 1).

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_buffer::i256;

use crate::Result;
use crate::error::CoreError;
use crate::file_group::reader_v2::row_serde;

// ── Spill value encoding (A1) ──────────────────────────────────────────────
// On spill, a `BufferedRecord` is serialized to a self-describing byte blob
// stored as the RocksDB value. The record key is the RocksDB key, so it is NOT
// repeated in the value. The layout is:
//
//   [payload_tag: u8]
//     0x00 = Delete  (no further payload bytes)
//     0x01 = Data    (followed by an Arrow IPC single-row stream)
//   [ordering_tag: u8]
//     0x00 = None
//     0x01 = Long   followed by 8 bytes  (i64 little-endian)
//     0x02 = String followed by [len: u32 LE][utf8 bytes]
//   [if payload_tag == Data] Arrow IPC stream bytes (rest of the blob)
//
// All multi-byte integers are little-endian. The format is internal to the
// reader's own spill round-trip (never persisted across runs), so no on-disk
// version negotiation is needed — the spill dir is created and destroyed within
// a single read (see `SpillableRecordMap`).

/// Spill payload tag: the record is a delete tombstone (no data follows).
const SPILL_PAYLOAD_TAG_DELETE: u8 = 0x00;
/// Spill payload tag: the record carries data (an Arrow IPC stream follows the
/// ordering-value header).
const SPILL_PAYLOAD_TAG_DATA: u8 = 0x01;
/// Spill ordering-value tag: no ordering value.
const SPILL_ORDERING_TAG_NONE: u8 = 0x00;
/// Spill ordering-value tag: `OrderingValue::Long` (8-byte i64 follows).
const SPILL_ORDERING_TAG_LONG: u8 = 0x01;
/// Spill ordering-value tag: `OrderingValue::String` (u32 length + utf8 follow).
const SPILL_ORDERING_TAG_STRING: u8 = 0x02;
/// Spill ordering-value tag: `OrderingValue::Composite` (u32 element count, then
/// each element encoded with this same tag scheme).
const SPILL_ORDERING_TAG_COMPOSITE: u8 = 0x03;
/// Spill ordering-value tag: `OrderingValue::Double` (8-byte IEEE-754 f64 follows).
const SPILL_ORDERING_TAG_DOUBLE: u8 = 0x04;
/// Spill ordering-value tag: `OrderingValue::Decimal` (16-byte little-endian i128
/// unscaled value, then a 1-byte i8 scale follow).
const SPILL_ORDERING_TAG_DECIMAL: u8 = 0x05;
/// Spill ordering-value tag: `OrderingValue::Default` (no payload). The null-coerced
/// default ordering value (Java `HoodieRecord.DEFAULT_ORDERING_VALUE` == `Integer(0)`).
const SPILL_ORDERING_TAG_DEFAULT: u8 = 0x06;

/// In-memory payload of a buffered record (A2).
///
/// The default representation is a zero-copy reference into a shared source
/// batch ([`BatchRef`](RecordPayload::BatchRef)). Serialization (Arrow IPC)
/// happens only on spill (A1), never on the in-memory hot path — so this enum
/// deliberately carries no binary variant.
///
/// ## Pinning caveat
///
/// A [`BatchRef`](RecordPayload::BatchRef) keeps its WHOLE source
/// `Arc<RecordBatch>` alive (a 1-row [`RecordBatch::slice`] still references the
/// parent's full buffers). When survivors are sparse across many source batches,
/// the merge map can pin far more Arrow memory than the live rows occupy. The
/// `compact_pinned_batches` safety valve (see `key_based.rs`) re-batches the
/// survivors of sparsely-populated source batches and repoints their entries to
/// release the dead-row memory.
#[derive(Debug, Clone)]
pub enum RecordPayload {
    /// Zero-copy reference into a shared source batch (the A2 default). `row_idx`
    /// addresses a single row within `batch`. Reading the payload slices that row
    /// out via [`RecordBatch::slice`] (cheap, shares buffers).
    BatchRef {
        /// The shared source batch this row was decoded from. Interned once per
        /// decoded block batch (one `Arc` per batch) so that `Arc::as_ptr`
        /// identity is a valid grouping key for compaction.
        batch: Arc<RecordBatch>,
        /// Row index of this record within `batch`.
        row_idx: usize,
    },
    /// Owned single-row batch. Produced by `compact_pinned_batches` (a freshly
    /// re-batched survivor set) or for records that have no shared source batch.
    Owned(RecordBatch),
    /// Tombstone — the record represents a deletion and carries no payload.
    Delete,
}

impl RecordPayload {
    /// Returns the payload as a single-row `RecordBatch`, or `None` for a delete.
    ///
    /// `BatchRef` slices the addressed row out of its source batch (zero-copy
    /// buffer slice); `Owned` clones the held batch. Mirrors the read side of
    /// Java's `BufferedRecord.getRecord()`.
    pub fn get_record(&self) -> Option<RecordBatch> {
        match self {
            RecordPayload::BatchRef { batch, row_idx } => Some(batch.slice(*row_idx, 1)),
            RecordPayload::Owned(batch) => Some(batch.clone()),
            RecordPayload::Delete => None,
        }
    }

    /// Consume the payload and return its single-row `RecordBatch`, or `None`
    /// for a delete. The move-not-clone counterpart to [`get_record`](Self::get_record).
    pub fn into_record(self) -> Option<RecordBatch> {
        match self {
            RecordPayload::BatchRef { batch, row_idx } => Some(batch.slice(row_idx, 1)),
            RecordPayload::Owned(batch) => Some(batch),
            RecordPayload::Delete => None,
        }
    }

    /// Returns true if this payload is a delete tombstone.
    pub fn is_delete(&self) -> bool {
        matches!(self, RecordPayload::Delete)
    }
}

/// The universal record envelope flowing through the merge pipeline.
///
/// Mirrors Java's `BufferedRecord<T>`. In Rust/Arrow, a single "record" is a
/// single-row view ([`RecordPayload::BatchRef`]) into a shared decoded batch;
/// the merge map stores one of these per live key.
///
/// During the log scanning phase, `BufferedRecord`s are stored in the record
/// buffer's map keyed by record key.
#[derive(Debug, Clone)]
pub struct BufferedRecord {
    /// The record key extracted from the record.
    pub record_key: String,

    /// The in-memory record payload (zero-copy batch ref, owned batch, or delete).
    pub payload: RecordPayload,

    /// The ordering value used for merge conflict resolution.
    /// Higher ordering value wins during delta merge.
    pub ordering_value: Option<OrderingValue>,
}

impl BufferedRecord {
    /// Create a new data record from a zero-copy reference into a shared source
    /// batch (the A2 hot-path constructor).
    ///
    /// `batch` must be the interned `Arc` for the decoded block batch (one `Arc`
    /// per batch) so that `Arc::as_ptr` grouping during compaction is valid.
    pub fn new_batch_ref(
        record_key: String,
        batch: Arc<RecordBatch>,
        row_idx: usize,
        ordering_value: Option<OrderingValue>,
    ) -> Self {
        Self {
            record_key,
            payload: RecordPayload::BatchRef { batch, row_idx },
            ordering_value,
        }
    }

    /// Create a new data record from an owned single-row batch.
    ///
    /// Used by tests and by call sites that hold a single-row batch with no
    /// shared source (e.g. base-record construction in tests). The hot path
    /// uses [`new_batch_ref`](Self::new_batch_ref) instead.
    pub fn new_data(
        record_key: String,
        data: RecordBatch,
        ordering_value: Option<OrderingValue>,
    ) -> Self {
        Self {
            record_key,
            payload: RecordPayload::Owned(data),
            ordering_value,
        }
    }

    /// Create a new delete record (tombstone).
    pub fn new_delete(record_key: String, ordering_value: Option<OrderingValue>) -> Self {
        Self {
            record_key,
            payload: RecordPayload::Delete,
            ordering_value,
        }
    }

    /// Returns true if this record represents a deletion.
    pub fn is_delete(&self) -> bool {
        self.payload.is_delete()
    }

    /// Returns true if this record has no data payload (a delete tombstone).
    pub fn is_empty(&self) -> bool {
        self.payload.is_delete()
    }

    /// Return the record data as a single-row `RecordBatch`.
    ///
    /// Mirrors Java's `BufferedRecord.getRecord()`. Delegates to
    /// [`RecordPayload::get_record`]: `BatchRef` slices the row out (zero-copy),
    /// `Owned` clones, `Delete` returns `None`.
    pub fn get_record(&self) -> Option<RecordBatch> {
        self.payload.get_record()
    }

    /// Consume the record and return its data batch. The move-not-clone
    /// counterpart to [`get_record`](Self::get_record), used on the merge-output
    /// hot path where the record is no longer needed afterward.
    pub fn into_record(self) -> Option<RecordBatch> {
        self.payload.into_record()
    }

    /// Serialize this record to a self-describing spill blob (**spill-only, A1**).
    ///
    /// Used by [`SpillableRecordMap`](super::buffer::spillable_map::SpillableRecordMap)
    /// to store an entry in the on-disk (RocksDB) tier when the in-memory budget
    /// is exhausted. The record key is NOT included (it is the RocksDB key); the
    /// blob carries the payload (delete tombstone or a single-row Arrow IPC
    /// stream) and the ordering value. See the module-level spill-encoding notes.
    ///
    /// Start with single-row IPC for correctness; a batched-spill optimization
    /// (group survivors by source batch, spill multi-row blobs) is tracked as B5
    /// in the backlog (05-a2a1-design.md) and deliberately NOT built here.
    pub fn to_spill_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self.get_record() {
            None => buf.push(SPILL_PAYLOAD_TAG_DELETE),
            Some(batch) => {
                buf.push(SPILL_PAYLOAD_TAG_DATA);
                encode_ordering_value(&mut buf, self.ordering_value.as_ref());
                // Arrow IPC single-row stream (the spill serialization primitive,
                // row_serde::to_binary_row). For deletes we skip this entirely.
                let ipc = row_serde::to_binary_row(&batch.schema(), &batch);
                buf.extend_from_slice(&ipc);
                return buf;
            }
        }
        // Delete path: still record the ordering value (it can matter when a
        // spilled delete tombstone is later compared during the base merge).
        encode_ordering_value(&mut buf, self.ordering_value.as_ref());
        buf
    }

    /// Reconstruct a record from a spill blob produced by
    /// [`to_spill_bytes`](Self::to_spill_bytes) (**spill-only, A1**).
    ///
    /// `record_key` is supplied by the caller (it is the RocksDB key, not stored
    /// in the blob). Returns a typed [`CoreError`] on a malformed blob rather
    /// than panicking — a corrupt spill entry is a recoverable read-path error,
    /// not an internal invariant violation.
    pub fn from_spill_bytes(record_key: String, bytes: &[u8]) -> Result<Self> {
        let mut cursor = 0usize;
        let payload_tag = read_u8(bytes, &mut cursor)?;
        match payload_tag {
            SPILL_PAYLOAD_TAG_DELETE => {
                let ordering_value = decode_ordering_value(bytes, &mut cursor)?;
                Ok(BufferedRecord::new_delete(record_key, ordering_value))
            }
            SPILL_PAYLOAD_TAG_DATA => {
                let ordering_value = decode_ordering_value(bytes, &mut cursor)?;
                let batch = row_serde::from_binary(&bytes[cursor..])?;
                // Reloaded from disk: there is no shared source batch to reference,
                // so the payload is necessarily `Owned`.
                Ok(BufferedRecord::new_data(record_key, batch, ordering_value))
            }
            other => Err(CoreError::ReadFileSliceError(format!(
                "spill decode: unknown payload tag {other:#04x}"
            ))),
        }
    }
}

/// Append an ordering value to a spill blob using the tag scheme documented at
/// the module level.
fn encode_ordering_value(buf: &mut Vec<u8>, ordering_value: Option<&OrderingValue>) {
    match ordering_value {
        None => buf.push(SPILL_ORDERING_TAG_NONE),
        Some(OrderingValue::Long(v)) => {
            buf.push(SPILL_ORDERING_TAG_LONG);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Some(OrderingValue::Double(v)) => {
            buf.push(SPILL_ORDERING_TAG_DOUBLE);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Some(OrderingValue::Decimal { unscaled, scale }) => {
            buf.push(SPILL_ORDERING_TAG_DECIMAL);
            buf.extend_from_slice(&unscaled.to_le_bytes());
            buf.push(*scale as u8);
        }
        Some(OrderingValue::String(s)) => {
            buf.push(SPILL_ORDERING_TAG_STRING);
            let sb = s.as_bytes();
            buf.extend_from_slice(&(sb.len() as u32).to_le_bytes());
            buf.extend_from_slice(sb);
        }
        Some(OrderingValue::Default) => buf.push(SPILL_ORDERING_TAG_DEFAULT),
        Some(OrderingValue::Composite(elems)) => {
            buf.push(SPILL_ORDERING_TAG_COMPOSITE);
            buf.extend_from_slice(&(elems.len() as u32).to_le_bytes());
            for e in elems {
                encode_ordering_value(buf, Some(e));
            }
        }
    }
}

/// Read a single byte at `*cursor`, advancing it. Errors if out of bounds.
fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8> {
    let b = *bytes.get(*cursor).ok_or_else(|| {
        CoreError::ReadFileSliceError("spill decode: unexpected end of blob".to_string())
    })?;
    *cursor += 1;
    Ok(b)
}

/// Decode an ordering value written by [`encode_ordering_value`], advancing
/// `*cursor` past it.
fn decode_ordering_value(bytes: &[u8], cursor: &mut usize) -> Result<Option<OrderingValue>> {
    let tag = read_u8(bytes, cursor)?;
    match tag {
        SPILL_ORDERING_TAG_NONE => Ok(None),
        SPILL_ORDERING_TAG_DEFAULT => Ok(Some(OrderingValue::Default)),
        SPILL_ORDERING_TAG_LONG => {
            let end = *cursor + 8;
            let slice = bytes.get(*cursor..end).ok_or_else(|| {
                CoreError::ReadFileSliceError(
                    "spill decode: truncated Long ordering value".to_string(),
                )
            })?;
            let v = i64::from_le_bytes(slice.try_into().expect("8-byte slice"));
            *cursor = end;
            Ok(Some(OrderingValue::Long(v)))
        }
        SPILL_ORDERING_TAG_DOUBLE => {
            let end = *cursor + 8;
            let slice = bytes.get(*cursor..end).ok_or_else(|| {
                CoreError::ReadFileSliceError(
                    "spill decode: truncated Double ordering value".to_string(),
                )
            })?;
            let v = f64::from_le_bytes(slice.try_into().expect("8-byte slice"));
            *cursor = end;
            Ok(Some(OrderingValue::Double(v)))
        }
        SPILL_ORDERING_TAG_DECIMAL => {
            let end = *cursor + 16;
            let slice = bytes.get(*cursor..end).ok_or_else(|| {
                CoreError::ReadFileSliceError(
                    "spill decode: truncated Decimal ordering value".to_string(),
                )
            })?;
            let unscaled = i128::from_le_bytes(slice.try_into().expect("16-byte slice"));
            *cursor = end;
            let scale = read_u8(bytes, cursor)? as i8;
            Ok(Some(OrderingValue::Decimal { unscaled, scale }))
        }
        SPILL_ORDERING_TAG_STRING => {
            let len_end = *cursor + 4;
            let len_slice = bytes.get(*cursor..len_end).ok_or_else(|| {
                CoreError::ReadFileSliceError(
                    "spill decode: truncated String ordering length".to_string(),
                )
            })?;
            let len = u32::from_le_bytes(len_slice.try_into().expect("4-byte slice")) as usize;
            *cursor = len_end;
            let str_end = *cursor + len;
            let str_slice = bytes.get(*cursor..str_end).ok_or_else(|| {
                CoreError::ReadFileSliceError(
                    "spill decode: truncated String ordering value".to_string(),
                )
            })?;
            let s = std::str::from_utf8(str_slice)
                .map_err(|e| {
                    CoreError::ReadFileSliceError(format!(
                        "spill decode: invalid utf8 in String ordering value: {e}"
                    ))
                })?
                .to_string();
            *cursor = str_end;
            Ok(Some(OrderingValue::String(s)))
        }
        SPILL_ORDERING_TAG_COMPOSITE => {
            let len_end = *cursor + 4;
            let len_slice = bytes.get(*cursor..len_end).ok_or_else(|| {
                CoreError::ReadFileSliceError(
                    "spill decode: truncated composite ordering length".to_string(),
                )
            })?;
            let n = u32::from_le_bytes(len_slice.try_into().expect("4-byte slice")) as usize;
            *cursor = len_end;
            let mut elems = Vec::with_capacity(n);
            for _ in 0..n {
                match decode_ordering_value(bytes, cursor)? {
                    Some(v) => elems.push(v),
                    None => {
                        return Err(CoreError::ReadFileSliceError(
                            "spill decode: null element in composite ordering value".to_string(),
                        ));
                    }
                }
            }
            Ok(Some(OrderingValue::Composite(elems)))
        }
        other => Err(CoreError::ReadFileSliceError(format!(
            "spill decode: unknown ordering tag {other:#04x}"
        ))),
    }
}

/// Comparable ordering value for merge conflict resolution.
///
/// In Java Hudi, this is `Comparable<?>`. In Rust, we support
/// the common types used as precombine fields.
#[derive(Debug, Clone)]
pub enum OrderingValue {
    Long(i64),
    /// Floating-point ordering value (e.g. a `double`/`float` precombine field
    /// such as `weight`). Compared via `f64::total_cmp` for a total order
    /// (`Float32` is widened to `f64` on extraction). Mirrors Java Hudi, where a
    /// `Comparable` Double/Float precombine field is a valid EVENT_TIME ordering
    /// key.
    Double(f64),
    /// Fixed-point decimal ordering value (e.g. a `decimal(p,s)` precombine field),
    /// stored as the `i128` unscaled value plus its `scale` — mirroring Arrow
    /// `Decimal128` and Java Hudi's `BigDecimal`/`DecimalWrapper` ordering key.
    /// Compared **value-based / scale-independently** (see [`cmp_decimal`]), so
    /// `1.0` (scale 1) and `1.00` (scale 2) compare equal, matching
    /// `BigDecimal.compareTo`. Scale is assumed non-negative and `<= 38` (the
    /// Arrow `Decimal128` / Avro decimal-logical range); larger scales are not
    /// produced by any Hudi ordering path.
    Decimal {
        unscaled: i128,
        scale: i8,
    },
    String(String),
    /// The null-coerced DEFAULT ordering value, mirroring Java
    /// `OrderingValues.getDefault()` == `HoodieRecord.DEFAULT_ORDERING_VALUE` ==
    /// `Integer(0)`. Produced when an ordering field is null/absent for a record
    /// (see [`RecordContext::get_ordering_values`](crate::file_group::reader_v2::record_context::RecordContext)),
    /// so a null-ordering record does NOT auto-win a merge — it compares as `0`
    /// against a same-domain (integer) ordering value (Java coerces null → `Integer(0)`
    /// then compares `newer >= older`).
    ///
    /// Distinct from `Long(0)`: a GENUINE field value of `0` is `Long(0)`, which is
    /// NOT the default (mirrors Java `OrderingValues.isDefault` == `Integer(0).equals(x)`,
    /// which is `false` for a `Long(0)`). Only this `Default` sentinel and an absent
    /// (`None`) ordering count as the natural-order/commit-time default — see
    /// [`Self::is_default`]. hudi-rs collapses Int/Long/Date/Timestamp/Boolean → `Long`,
    /// so this variant is what preserves the "was-null vs genuine 0" distinction.
    Default,
    /// Multi-field (composite) ordering — a tuple of per-field ordering values
    /// compared lexicographically (field 1, then field 2, …), each element by
    /// its own type. Mirrors Java's `OrderingValues` for a multi-field
    /// `hoodie.table.ordering.fields` (e.g. MySql Debezium's
    /// `(_event_bin_file, _event_pos)`). Built only when every field is present
    /// for the row; a null in any field yields no ordering value (see
    /// `RecordContext::get_ordering_values`).
    Composite(Vec<OrderingValue>),
}

impl OrderingValue {
    /// Stable discriminant rank for cross-variant comparison (Long < String <
    /// Composite). Only used as a total-order fallback when two values are of
    /// different variants; the merge path gates real comparisons on
    /// `is_same_class`, so this never decides a same-class merge.
    fn variant_rank(&self) -> u8 {
        match self {
            OrderingValue::Long(_) => 0,
            OrderingValue::String(_) => 1,
            OrderingValue::Composite(_) => 2,
            OrderingValue::Double(_) => 3,
            OrderingValue::Decimal { .. } => 4,
            // `Default` shares the integer domain with `Long` (it compares as `0`
            // against `Long`; see `cmp`), so it takes `Long`'s rank. This only
            // affects the cross-variant fallback for `Default` vs a non-integer
            // type (Double/Decimal/String/Composite), which the merge paths never
            // reach for a same-class decision (they gate on `is_same_class`).
            OrderingValue::Default => 0,
        }
    }

    /// True if this is the default ordering value (`HoodieRecord.DEFAULT_ORDERING_VALUE`
    /// == int `0`, "natural order"). Mirrors Java `OrderingValues.isDefault`.
    /// hudi-rs maps both Avro Int/Long ordering wrappers to [`OrderingValue::Long`],
    /// so the default surfaces here as `Long(0)`. Used by the delete-merge tie-break:
    /// a delete carrying the default ordering always applies.
    ///
    /// Note: an *absent* ordering (`None`) is ALSO the default — the caller
    /// (`is_default_ordering` in `record_merger.rs`, and `pick_winner`'s delete
    /// arm) treats `None` and `Some(Default)` identically. A GENUINE `Long(0)`
    /// is NOT the default (ENG: GAP-2 — mirrors Java `OrderingValues.isDefault`,
    /// `Integer(0).equals(Long(0))` == `false`).
    pub fn is_default(&self) -> bool {
        matches!(self, OrderingValue::Default)
    }

    /// True if `self` and `other` are the same variant (mirrors Java
    /// `OrderingValues.isSameClass`). Cross-variant ordering comparisons are not
    /// meaningful, so the delete-merge tie-break only defers to the base when the
    /// two ordering values share a class.
    ///
    /// The null-coerced [`Default`](Self::Default) is in the same (integer)
    /// class as [`Long`](Self::Long): hudi-rs collapses Int/Long/Date/Timestamp/
    /// Boolean → `Long`, and Java coerces a null integer-domain ordering field to
    /// `Integer(0)`, comparing it numerically against the field's `Long`/`Integer`
    /// values. `Default` is NOT the same class as Double/Decimal/String/Composite
    /// (Java throws on those cross-type compares; the merge paths' `is_same_class`
    /// guard turns that into "newer wins" rather than a panic).
    pub fn is_same_class(&self, other: &OrderingValue) -> bool {
        use OrderingValue::{Default, Long};
        match (self, other) {
            (Default, Default) | (Default, Long(_)) | (Long(_), Default) => true,
            _ => std::mem::discriminant(self) == std::mem::discriminant(other),
        }
    }
}

impl PartialOrd for OrderingValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Value-based equality that agrees with [`Ord`] (`a == b` ⇔ `a.cmp(b)` is
/// `Equal`), upholding the stdlib `Ord`/`Eq`/`PartialEq` consistency contract —
/// a *derived* field-wise `PartialEq` would disagree with [`cmp`](Ord::cmp) for
/// `Decimal` (compared scale-independently, so `1.0` equals `1.00`) and `Double`
/// (compared via `total_cmp`), a latent footgun for any equality- or
/// ordered-collection-based use.
impl PartialEq for OrderingValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for OrderingValue {}

impl Ord for OrderingValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (OrderingValue::Long(a), OrderingValue::Long(b)) => a.cmp(b),
            // f64 is not `Ord` (NaN); `total_cmp` gives the IEEE-754 total order
            // so two `Double`s always compare consistently with the manual `Eq`.
            (OrderingValue::Double(a), OrderingValue::Double(b)) => a.total_cmp(b),
            (OrderingValue::String(a), OrderingValue::String(b)) => a.cmp(b),
            // Value-based, scale-independent decimal compare (mirrors Java
            // BigDecimal.compareTo): `1.0` == `1.00`. A base column and a delete
            // wrapper often carry different scales for the same field.
            (
                OrderingValue::Decimal {
                    unscaled: a,
                    scale: a_scale,
                },
                OrderingValue::Decimal {
                    unscaled: b,
                    scale: b_scale,
                },
            ) => cmp_decimal(*a, *a_scale, *b, *b_scale),
            // Element-wise lexicographic comparison (Vec<OrderingValue>: Ord).
            (OrderingValue::Composite(a), OrderingValue::Composite(b)) => a.cmp(b),
            // The null-coerced default compares as the integer `0` (Java
            // `Integer(0)`), in the same domain as `Long`: `Default` vs `Long(n)`
            // is `0.cmp(n)`, and two defaults are equal. This makes a null-ordering
            // record compare `>= / <` a positive-ordering record instead of
            // auto-winning the merge.
            (OrderingValue::Default, OrderingValue::Default) => std::cmp::Ordering::Equal,
            (OrderingValue::Default, OrderingValue::Long(b)) => 0i64.cmp(b),
            (OrderingValue::Long(a), OrderingValue::Default) => a.cmp(&0i64),
            // Cross-variant: order by a stable discriminant rank (consistent, but
            // never relied upon for a same-class merge — see `is_same_class`).
            _ => self.variant_rank().cmp(&other.variant_rank()),
        }
    }
}

/// Compare two fixed-point decimals **by value, independent of scale** — the
/// semantics of Java `BigDecimal.compareTo` (so `1.0` == `1.00`). Each decimal is
/// `unscaled * 10^-scale`; to compare, the operand with the smaller scale is
/// rescaled up to the larger scale, then the unscaled integers are compared.
///
/// The rescale multiply is done in `i256` because an Arrow `Decimal128` unscaled
/// value (up to 38 digits) times `10^(scale_diff)` can exceed `i128`. Both scales
/// are validated to the `Decimal128` domain (`0..=38`) at the decode boundary
/// (see `RecordContext::{from_column, scalar_ordering_value}`), so `scale_diff <=
/// 38` and the widened product is exact in `i256`. The scale difference is
/// nonetheless computed in `i32` (not `i8`) so a directly-constructed
/// out-of-contract value can never overflow the subtraction, and `pow10_i256`
/// uses `wrapping_mul` — the function is total (never panics) for any input.
fn cmp_decimal(a_unscaled: i128, a_scale: i8, b_unscaled: i128, b_scale: i8) -> std::cmp::Ordering {
    if a_scale == b_scale {
        // Same scale (the common case, incl. every base-vs-log read-path compare):
        // the unscaled integers are directly comparable.
        return a_unscaled.cmp(&b_unscaled);
    }
    // Rescale the smaller-scale operand up so both are expressed at the same scale.
    // `scale_diff` is computed in i32 to avoid any i8 subtraction overflow.
    let (a256, b256) = if a_scale < b_scale {
        let scale_diff = (b_scale as i32 - a_scale as i32) as u32;
        (
            i256::from_i128(a_unscaled).wrapping_mul(pow10_i256(scale_diff)),
            i256::from_i128(b_unscaled),
        )
    } else {
        let scale_diff = (a_scale as i32 - b_scale as i32) as u32;
        (
            i256::from_i128(a_unscaled),
            i256::from_i128(b_unscaled).wrapping_mul(pow10_i256(scale_diff)),
        )
    };
    a256.cmp(&b256)
}

/// `10^exp` as an `i256`, built by repeated multiplication (no `i128::pow`
/// overflow panic). For the supported decimal range `exp <= 38`, the result is
/// exact in `i256`.
fn pow10_i256(exp: u32) -> i256 {
    let ten = i256::from_i128(10);
    let mut acc = i256::ONE;
    for _ in 0..exp {
        acc = acc.wrapping_mul(ten);
    }
    acc
}

/// Immutable value object representing a record to be deleted.
///
/// Mirrors Java's `org.apache.hudi.common.model.DeleteRecord`.
/// Created from `HoodieDeleteBlock` during log scanning.
#[derive(Debug, Clone)]
pub struct DeleteRecord {
    /// The record key to delete.
    pub record_key: String,

    /// The partition path.
    pub partition_path: String,

    /// Ordering value for merge conflict resolution with existing records.
    pub ordering_value: Option<OrderingValue>,
}

/// Factory methods for creating `BufferedRecord` instances.
///
/// Mirrors Java's `org.apache.hudi.common.table.read.BufferedRecords` (static factory).
pub struct BufferedRecords;

impl BufferedRecords {
    /// Create a `BufferedRecord` from an engine record (owned Arrow RecordBatch).
    ///
    /// Extracts the record key and ordering value from the batch using the
    /// provided schema and field names.
    pub fn from_engine_record(
        record_key: String,
        data: RecordBatch,
        ordering_value: Option<OrderingValue>,
        is_delete: bool,
    ) -> BufferedRecord {
        if is_delete {
            BufferedRecord::new_delete(record_key, ordering_value)
        } else {
            BufferedRecord::new_data(record_key, data, ordering_value)
        }
    }

    /// Create a `BufferedRecord` from a `DeleteRecord`.
    ///
    /// Produces a `BufferedRecord` with a `Delete` payload.
    pub fn from_delete_record(delete_record: &DeleteRecord) -> BufferedRecord {
        BufferedRecord::new_delete(
            delete_record.record_key.clone(),
            delete_record.ordering_value.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]))
    }

    fn batch(keys: &[&str], vs: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(keys.to_vec())) as _,
                Arc::new(Int32Array::from(vs.to_vec())) as _,
            ],
        )
        .unwrap()
    }

    fn row_tuple(b: &RecordBatch, row: usize) -> (String, i32) {
        let keys = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let vs = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        (keys.value(row).to_string(), vs.value(row))
    }

    /// `OrderingValue::Composite` (e.g. MySql Debezium `(bin_file, pos)`) must
    /// survive the spill encode/decode round-trip element-wise, including a
    /// nested composite. Zero coverage before this — the composite spill codec
    /// is recursive and easy to desync on a refactor.
    #[test]
    fn spill_roundtrip_composite_ordering() {
        for ov in [
            OrderingValue::Composite(vec![
                OrderingValue::String("mysql-bin.000042".to_string()),
                OrderingValue::Long(1234),
            ]),
            // nested composite + a mix of variants
            OrderingValue::Composite(vec![
                OrderingValue::Long(-7),
                OrderingValue::Composite(vec![OrderingValue::String("x".to_string())]),
            ]),
        ] {
            let mut buf = Vec::new();
            encode_ordering_value(&mut buf, Some(&ov));
            let mut cursor = 0usize;
            let decoded = decode_ordering_value(&buf, &mut cursor).unwrap();
            assert_eq!(decoded, Some(ov), "composite ordering must round-trip");
            assert_eq!(cursor, buf.len(), "decoder consumed the whole blob");
        }
    }

    /// `OrderingValue::Double` (a floating-point precombine field such as
    /// `weight`, ENG-38318/A3c) must round-trip through the spill codec and
    /// compare by IEEE-754 total order, and must NOT be treated as the
    /// natural-order default (only the `Default` sentinel is).
    #[test]
    fn double_ordering_roundtrip_compare_and_default() {
        for ov in [
            OrderingValue::Double(1.5),
            OrderingValue::Double(-0.25),
            OrderingValue::Double(0.0),
            OrderingValue::Composite(vec![OrderingValue::Double(3.5), OrderingValue::Long(7)]),
        ] {
            let mut buf = Vec::new();
            encode_ordering_value(&mut buf, Some(&ov));
            let mut cursor = 0usize;
            let decoded = decode_ordering_value(&buf, &mut cursor).unwrap();
            assert_eq!(decoded, Some(ov), "double ordering must round-trip");
            assert_eq!(cursor, buf.len(), "decoder consumed the whole blob");
        }
        // Total order via total_cmp: higher weight is greater (drives the
        // EVENT_TIME winner pick), and same-class compares only against Double.
        assert!(OrderingValue::Double(2.0) > OrderingValue::Double(1.0));
        assert!(OrderingValue::Double(2.0).is_same_class(&OrderingValue::Double(-1.0)));
        assert!(!OrderingValue::Double(2.0).is_same_class(&OrderingValue::Long(2)));
        // A double is never the integer natural-order default sentinel.
        assert!(!OrderingValue::Double(0.0).is_default());
    }

    /// `OrderingValue::Decimal` (a `decimal(p,s)` precombine / Java `DecimalWrapper`
    /// ordering key) must round-trip through the spill codec (unscaled + scale) and
    /// compare VALUE-BASED, independent of scale — the semantics of Java
    /// `BigDecimal.compareTo`. The cross-scale case is the real one: a base column
    /// and a delete wrapper routinely carry the same value at different scales.
    #[test]
    fn decimal_ordering_roundtrip_and_value_based_compare() {
        use std::cmp::Ordering;
        // Spill round-trip (incl. negative unscaled and nested in a composite).
        for ov in [
            OrderingValue::Decimal {
                unscaled: 12345,
                scale: 2,
            },
            OrderingValue::Decimal {
                unscaled: -50,
                scale: 4,
            },
            OrderingValue::Composite(vec![
                OrderingValue::Decimal {
                    unscaled: 4_000_000_000_000_000,
                    scale: 15,
                },
                OrderingValue::Long(7),
            ]),
        ] {
            let mut buf = Vec::new();
            encode_ordering_value(&mut buf, Some(&ov));
            let mut cursor = 0usize;
            let decoded = decode_ordering_value(&buf, &mut cursor).unwrap();
            assert_eq!(decoded, Some(ov), "decimal ordering must round-trip");
            assert_eq!(cursor, buf.len(), "decoder consumed the whole blob");
        }
        // Value-based, scale-independent compare (the `harness_delete_ord_decimal`
        // fixture: base DECIMAL(20,4) `4.0000` vs delete DecimalWrapper(scale 15)
        // `4.000000000000000` must be EQUAL, not ordered by raw unscaled ints).
        let base_4 = OrderingValue::Decimal {
            unscaled: 40_000,
            scale: 4,
        };
        let del_4 = OrderingValue::Decimal {
            unscaled: 4_000_000_000_000_000,
            scale: 15,
        };
        assert_eq!(
            base_4.cmp(&del_4),
            Ordering::Equal,
            "4.0000 == 4.000000000000000 (value-based)"
        );
        // Ord/Eq consistency: the manual `PartialEq` agrees with `cmp`
        // (`a == b` ⇔ `a.cmp(b) == Equal`), so value-equal decimals at different
        // scales are `==` even though their fields differ. A derived field-wise
        // `PartialEq` would report them unequal, violating the stdlib contract.
        assert_eq!(base_4, del_4, "value-equal decimals must be == (Ord⇔Eq)");
        // Ordering across scales: 4.0000 < 5.0 < ... — a raw-unscaled compare would
        // be WRONG (40000 > 50), so this is discriminating for the scale-aware path.
        let five = OrderingValue::Decimal {
            unscaled: 50,
            scale: 1,
        };
        assert_ne!(base_4, five, "4.0000 != 5.0 (Ord⇔Eq)");
        assert!(base_4 < five, "4.0000 < 5.0");
        assert!(five > del_4, "5.0 > 4.000000000000000");
        // Negative + cross-scale: -0.5 < 0.25.
        let neg_half = OrderingValue::Decimal {
            unscaled: -5,
            scale: 1,
        };
        let quarter = OrderingValue::Decimal {
            unscaled: 25,
            scale: 2,
        };
        assert!(neg_half < quarter, "-0.5 < 0.25");
        // Class + default predicates (Decimal is its own class; never the default).
        assert!(base_4.is_same_class(&del_4));
        assert!(!base_4.is_same_class(&OrderingValue::Long(4)));
        assert!(!base_4.is_default());
        // `cmp_decimal` is TOTAL: even out-of-contract scales far enough apart to
        // overflow an i8 subtraction (the decode boundary guards these out of
        // production) must not panic — the scale diff is computed in i32. With the
        // old `(b_scale - a_scale) as u32` on i8, `120 - (-120) = 240` overflowed.
        let wide_lo = OrderingValue::Decimal {
            unscaled: 7,
            scale: -120,
        };
        let wide_hi = OrderingValue::Decimal {
            unscaled: 7,
            scale: 120,
        };
        let _ = wide_lo.cmp(&wide_hi); // must not panic
    }

    /// `is_default` recognizes ONLY the null-coerced `Default` sentinel — a
    /// GENUINE `Long(0)` is NOT the default (GAP-2; mirrors Java
    /// `OrderingValues.isDefault` == `Integer(0).equals(x)`, `false` for `Long(0)`).
    /// `is_same_class` is true when the variants match, PLUS the `Default`↔`Long`
    /// (integer-domain) pairing, and it drives the cross-class rejection the delete
    /// tie-break relies on.
    #[test]
    fn ordering_value_default_and_same_class_predicates() {
        use OrderingValue::*;
        // is_default: only the Default sentinel (NOT a genuine Long(0)).
        assert!(Default.is_default());
        assert!(!Long(0).is_default());
        assert!(!Long(1).is_default());
        assert!(!Long(-1).is_default());
        assert!(!String("0".to_string()).is_default());
        assert!(!Composite(vec![Long(0)]).is_default());

        // is_same_class: same variant ⇒ true (regardless of inner value).
        assert!(Long(0).is_same_class(&Long(999)));
        assert!(String("a".to_string()).is_same_class(&String("b".to_string())));
        assert!(Composite(vec![Long(1)]).is_same_class(&Composite(vec![String("x".to_string())])));
        // Default is in the integer domain: same-class with Long and itself.
        assert!(Default.is_same_class(&Long(5)));
        assert!(Long(5).is_same_class(&Default));
        assert!(Default.is_same_class(&Default));
        // Default vs a non-integer type ⇒ different class (Java would throw).
        assert!(!Default.is_same_class(&String("0".to_string())));
        assert!(!Default.is_same_class(&Double(0.0)));
        // Default compares as the integer 0 against Long, and is EQUAL to Long(0).
        assert_eq!(Default.cmp(&Long(0)), std::cmp::Ordering::Equal);
        assert!(Default < Long(100));
        assert!(Long(100) > Default);
        assert!(Default > Long(-1));
        // cross-class ⇒ false.
        assert!(!Long(0).is_same_class(&String("0".to_string())));
        assert!(!Long(0).is_same_class(&Composite(vec![Long(0)])));
        assert!(!String("x".to_string()).is_same_class(&Composite(vec![])));

        // Default round-trips through the spill codec (no payload).
        let mut buf = Vec::new();
        encode_ordering_value(&mut buf, Some(&Default));
        let mut cursor = 0usize;
        assert_eq!(
            decode_ordering_value(&buf, &mut cursor).unwrap(),
            Some(Default)
        );
        assert_eq!(cursor, buf.len());
    }

    /// `BatchRef::get_record` slices the addressed row — full data must match the
    /// source row exactly (not just the row count), and it must NOT return the
    /// wrong row.
    #[test]
    fn batch_ref_get_record_slices_correct_row() {
        let src = Arc::new(batch(&["a", "b", "c"], &[10, 20, 30]));
        let payload = RecordPayload::BatchRef {
            batch: src.clone(),
            row_idx: 1,
        };
        let got = payload.get_record().expect("batch ref yields a row");
        assert_eq!(got.num_rows(), 1, "slice is exactly one row");
        assert_eq!(
            row_tuple(&got, 0),
            ("b".to_string(), 20),
            "must slice row 1 (b,20), not row 0 or 2"
        );
        // get_record must not consume the payload — a second call returns the
        // same row.
        assert_eq!(
            row_tuple(&payload.get_record().unwrap(), 0),
            ("b".into(), 20)
        );
    }

    /// `BatchRef::into_record` moves out the same single row (full data assert).
    #[test]
    fn batch_ref_into_record_yields_same_row() {
        let src = Arc::new(batch(&["x", "y"], &[1, 2]));
        let payload = RecordPayload::BatchRef {
            batch: src,
            row_idx: 0,
        };
        let got = payload.into_record().expect("yields a row");
        assert_eq!(row_tuple(&got, 0), ("x".to_string(), 1));
    }

    /// `Owned::get_record` returns a clone of the held batch's data exactly.
    #[test]
    fn owned_get_record_returns_data() {
        let payload = RecordPayload::Owned(batch(&["only"], &[42]));
        let got = payload.get_record().unwrap();
        assert_eq!(row_tuple(&got, 0), ("only".to_string(), 42));
    }

    /// `Delete` payloads are deletes and carry no record.
    #[test]
    fn delete_payload_is_delete_and_has_no_record() {
        let payload = RecordPayload::Delete;
        assert!(payload.is_delete());
        assert!(payload.get_record().is_none());
        assert!(payload.into_record().is_none());
    }

    /// `BufferedRecord` delegates `is_delete` / `get_record` to its payload.
    #[test]
    fn buffered_record_delegates_to_payload() {
        let src = Arc::new(batch(&["k"], &[7]));
        let data = BufferedRecord::new_batch_ref("k".to_string(), src, 0, None);
        assert!(!data.is_delete());
        assert!(!data.is_empty());
        assert_eq!(
            row_tuple(&data.get_record().unwrap(), 0),
            ("k".to_string(), 7)
        );

        let del = BufferedRecord::new_delete("d".to_string(), None);
        assert!(del.is_delete());
        assert!(del.is_empty());
        assert!(del.get_record().is_none());
    }

    // ── Spill round-trip (A1) ─────────────────────────────────────────────

    /// A data record survives a spill round-trip byte-exact: same key, same row
    /// data, same ordering value (guide §5: assert full data, not counts).
    #[test]
    fn spill_roundtrip_data_record_with_long_ordering() {
        let src = Arc::new(batch(&["a", "b", "c"], &[10, 20, 30]));
        let rec =
            BufferedRecord::new_batch_ref("b".to_string(), src, 1, Some(OrderingValue::Long(99)));

        let blob = rec.to_spill_bytes();
        let back = BufferedRecord::from_spill_bytes("b".to_string(), &blob).unwrap();

        assert_eq!(back.record_key, "b");
        assert_eq!(back.ordering_value, Some(OrderingValue::Long(99)));
        assert!(!back.is_delete());
        let got = back.get_record().expect("data");
        assert_eq!(
            row_tuple(&got, 0),
            ("b".to_string(), 20),
            "spilled row must reload byte-exact (row 1 = b,20)"
        );
    }

    /// A data record with a String ordering value round-trips exactly.
    #[test]
    fn spill_roundtrip_data_record_with_string_ordering() {
        let src = Arc::new(batch(&["only"], &[7]));
        let rec = BufferedRecord::new_batch_ref(
            "only".to_string(),
            src,
            0,
            Some(OrderingValue::String("v-2026".to_string())),
        );
        let blob = rec.to_spill_bytes();
        let back = BufferedRecord::from_spill_bytes("only".to_string(), &blob).unwrap();
        assert_eq!(
            back.ordering_value,
            Some(OrderingValue::String("v-2026".to_string()))
        );
        assert_eq!(
            row_tuple(&back.get_record().unwrap(), 0),
            ("only".to_string(), 7)
        );
    }

    /// A delete tombstone round-trips as a delete (no data), preserving ordering.
    #[test]
    fn spill_roundtrip_delete_tombstone() {
        let rec = BufferedRecord::new_delete("gone".to_string(), Some(OrderingValue::Long(5)));
        let blob = rec.to_spill_bytes();
        let back = BufferedRecord::from_spill_bytes("gone".to_string(), &blob).unwrap();
        assert!(back.is_delete(), "tombstone must reload as a delete");
        assert!(back.get_record().is_none());
        assert_eq!(back.ordering_value, Some(OrderingValue::Long(5)));
    }

    /// A record with no ordering value round-trips with `None`.
    #[test]
    fn spill_roundtrip_no_ordering_value() {
        let src = Arc::new(batch(&["k"], &[1]));
        let rec = BufferedRecord::new_batch_ref("k".to_string(), src, 0, None);
        let back =
            BufferedRecord::from_spill_bytes("k".to_string(), &rec.to_spill_bytes()).unwrap();
        assert_eq!(back.ordering_value, None);
    }

    /// A truncated / malformed blob yields a typed error, not a panic.
    #[test]
    fn spill_decode_rejects_malformed_blob() {
        // Unknown payload tag.
        assert!(BufferedRecord::from_spill_bytes("k".to_string(), &[0xFF]).is_err());
        // Empty blob.
        assert!(BufferedRecord::from_spill_bytes("k".to_string(), &[]).is_err());
        // Data tag but truncated before the IPC stream.
        assert!(
            BufferedRecord::from_spill_bytes("k".to_string(), &[SPILL_PAYLOAD_TAG_DATA]).is_err()
        );
    }
}
