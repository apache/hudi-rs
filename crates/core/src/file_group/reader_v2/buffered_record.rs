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
//! ## In-memory representation
//!
//! The in-memory payload is a
//! zero-copy [`RecordPayload::BatchRef`] into the shared source batch that the
//! row was decoded from — NOT a per-row Arrow IPC blob. Measured on the port's
//! own benchmark, the per-row blob cost ~27x the build memory and ~12x the build
//! CPU of the batch-ref representation. Serialization is a spill-only concern
//! and happens a whole batch at a time in
//! [`spillable_map`](super::buffer::spillable_map), through
//! the `row_serde` module, which is built only with the `spill-rocksdb`
//! feature.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_buffer::i256;

/// In-memory payload of a buffered record.
///
/// The default representation is a zero-copy reference into a shared source
/// batch ([`BatchRef`](RecordPayload::BatchRef)). Serialization (Arrow IPC)
/// happens only on spill, never on the in-memory hot path — so this enum
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

    /// Return the record data as a single-row `RecordBatch`.
    ///
    /// Mirrors Java's `BufferedRecord.getRecord()`. Delegates to
    /// [`RecordPayload::get_record`]: `BatchRef` slices the row out (zero-copy),
    /// `Owned` clones, `Delete` returns `None`.
    pub fn get_record(&self) -> Option<RecordBatch> {
        self.payload.get_record()
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

    /// Ordering value for merge conflict resolution with existing records.
    pub ordering_value: Option<OrderingValue>,
}

/// Factory methods for creating `BufferedRecord` instances.
///
/// Mirrors Java's `org.apache.hudi.common.table.read.BufferedRecords` (static factory).
pub struct BufferedRecords;

impl BufferedRecords {
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

    /// `OrderingValue::Double` (a floating-point precombine field such as
    /// `weight`) compares by IEEE-754 total order, and is NOT the natural-order
    /// default — only the `Default` sentinel is.
    #[test]
    fn double_ordering_compares_by_total_order_and_is_never_default() {
        // Total order via total_cmp: higher weight is greater (drives the
        // EVENT_TIME winner pick), and same-class compares only against Double.
        assert!(OrderingValue::Double(2.0) > OrderingValue::Double(1.0));
        assert!(OrderingValue::Double(2.0).is_same_class(&OrderingValue::Double(-1.0)));
        assert!(!OrderingValue::Double(2.0).is_same_class(&OrderingValue::Long(2)));
        // A double is never the integer natural-order default sentinel.
        assert!(!OrderingValue::Double(0.0).is_default());
    }

    /// `OrderingValue::Decimal` (a `decimal(p,s)` precombine / Java `DecimalWrapper`
    /// ordering key) compares VALUE-BASED, independent of scale — the semantics of Java
    /// `BigDecimal.compareTo`. The cross-scale case is the real one: a base column
    /// and a delete wrapper routinely carry the same value at different scales.
    #[test]
    fn decimal_ordering_compares_value_based_across_scales() {
        use std::cmp::Ordering;
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
    }

    /// `BufferedRecord` delegates `is_delete` / `get_record` to its payload.
    #[test]
    fn buffered_record_delegates_to_payload() {
        let src = Arc::new(batch(&["k"], &[7]));
        let data = BufferedRecord::new_batch_ref("k".to_string(), src, 0, None);
        assert!(!data.is_delete());
        assert_eq!(
            row_tuple(&data.get_record().unwrap(), 0),
            ("k".to_string(), 7)
        );

        let del = BufferedRecord::new_delete("d".to_string(), None);
        assert!(del.is_delete());
        assert!(del.get_record().is_none());
    }

    // ── Spill round-trip ─────────────────────────────────────────────
}
