//! ENG-40156 — column-level predicate pushdown from Velox into hudi-rs.
//!
//! The IR is intentionally narrow (tagged union over a handful of
//! arrow-friendly filter shapes); compound expressions stay in Velox.
//! See ENG-40156-filter-pushdown.md for the design rationale.

use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray,
};
use arrow_schema::DataType;
use arrow_select::filter::filter_record_batch;
use std::sync::Arc;

use crate::ffi::FfiColumnFilter;

/// Decoded form of `FfiColumnFilter`. Sum type rather than tagged struct so
/// the arrow-side evaluator is total over the variants we care about.
#[derive(Debug, Clone)]
pub enum ColumnPredicate {
    IsNull { column: String, null_allowed: bool },
    IsNotNull { column: String, null_allowed: bool },
    BoolEq { column: String, value: bool, null_allowed: bool },
    BigintRange {
        column: String,
        lo: i64,
        hi: i64,
        lo_unbounded: bool,
        hi_unbounded: bool,
        null_allowed: bool,
    },
    DoubleRange {
        column: String,
        lo: f64,
        hi: f64,
        lo_unbounded: bool,
        hi_unbounded: bool,
        null_allowed: bool,
    },
    BigintValues {
        column: String,
        // Pre-sorted, deduped on the C++ side.
        values: Vec<i64>,
        null_allowed: bool,
    },
    BytesRange {
        column: String,
        lo: String,
        hi: String,
        lo_unbounded: bool,
        hi_unbounded: bool,
        null_allowed: bool,
    },
    BytesValues {
        column: String,
        values: Vec<String>,
        null_allowed: bool,
    },
}

impl ColumnPredicate {
    /// Column this predicate targets. Used by callers to figure out whether
    /// the predicate can be pushed to the parquet base read (column is in
    /// base schema) vs only applied post-merge.
    pub fn column(&self) -> &str {
        match self {
            ColumnPredicate::IsNull { column, .. }
            | ColumnPredicate::IsNotNull { column, .. }
            | ColumnPredicate::BoolEq { column, .. }
            | ColumnPredicate::BigintRange { column, .. }
            | ColumnPredicate::DoubleRange { column, .. }
            | ColumnPredicate::BigintValues { column, .. }
            | ColumnPredicate::BytesRange { column, .. }
            | ColumnPredicate::BytesValues { column, .. } => column,
        }
    }

    /// Build from the FFI wire form. Returns None when the kind string is
    /// not recognised — the caller logs and skips (Velox post-scan filter
    /// will still evaluate the original predicate, so correctness is
    /// preserved).
    pub fn from_ffi(ff: &FfiColumnFilter) -> Option<Self> {
        let col = ff.column.clone();
        let null_allowed = ff.null_allowed;
        match ff.kind.as_str() {
            "is_null" => Some(Self::IsNull { column: col, null_allowed }),
            "is_not_null" => Some(Self::IsNotNull { column: col, null_allowed }),
            "bool" => Some(Self::BoolEq {
                column: col,
                value: ff.bool_value,
                null_allowed,
            }),
            "bigint_range" => Some(Self::BigintRange {
                column: col,
                lo: ff.i64_lo,
                hi: ff.i64_hi,
                lo_unbounded: ff.lo_unbounded,
                hi_unbounded: ff.hi_unbounded,
                null_allowed,
            }),
            "double_range" => Some(Self::DoubleRange {
                column: col,
                lo: ff.f64_lo,
                hi: ff.f64_hi,
                lo_unbounded: ff.lo_unbounded,
                hi_unbounded: ff.hi_unbounded,
                null_allowed,
            }),
            "bigint_values" => Some(Self::BigintValues {
                column: col,
                values: ff.i64_values.clone(),
                null_allowed,
            }),
            "bytes_range" => Some(Self::BytesRange {
                column: col,
                lo: ff.bytes_lo.clone(),
                hi: ff.bytes_hi.clone(),
                lo_unbounded: ff.lo_unbounded,
                hi_unbounded: ff.hi_unbounded,
                null_allowed,
            }),
            "bytes_values" => Some(Self::BytesValues {
                column: col,
                values: ff.bytes_values.clone(),
                null_allowed,
            }),
            other => {
                log::warn!(
                    "[ENG-40156] unrecognised FfiColumnFilter kind '{other}' \
                     for column '{}'; predicate dropped on Rust side, Velox \
                     post-scan filter will still evaluate it",
                    ff.column
                );
                None
            }
        }
    }

    /// Evaluate this predicate against `batch`, returning a BooleanArray that
    /// is `true` for rows passing the predicate (including `null_allowed`
    /// rows for NULL inputs) and `false` otherwise.
    ///
    /// Returns Ok(None) when the target column isn't present in the batch
    /// (e.g., projected out by hudi-rs). The caller treats "predicate column
    /// not in batch" as a pass-through (effectively skip this predicate).
    /// Returns Err when the column is present but the runtime type doesn't
    /// match the predicate kind — a programmer error from the Velox side
    /// that should be loud rather than silently ignored.
    pub fn evaluate(&self, batch: &RecordBatch) -> Result<Option<BooleanArray>, String> {
        let schema = batch.schema();
        let col_idx = match schema.index_of(self.column()) {
            Ok(i) => i,
            Err(_) => return Ok(None),
        };
        let col = batch.column(col_idx);
        let nulls = col.nulls();
        let row_count = col.len();
        let null_allowed = self.null_allowed_value();

        match self {
            ColumnPredicate::IsNull { .. } => {
                let mut builder = vec![false; row_count];
                if let Some(nb) = nulls {
                    for (i, b) in builder.iter_mut().enumerate() {
                        *b = nb.is_null(i);
                    }
                }
                Ok(Some(BooleanArray::from(builder)))
            }
            ColumnPredicate::IsNotNull { .. } => {
                let mut builder = vec![true; row_count];
                if let Some(nb) = nulls {
                    for (i, b) in builder.iter_mut().enumerate() {
                        *b = !nb.is_null(i);
                    }
                }
                // null_allowed for IsNotNull doesn't make semantic sense
                // (nulls fail by definition); ignore.
                let _ = null_allowed;
                Ok(Some(BooleanArray::from(builder)))
            }
            ColumnPredicate::BoolEq { value, .. } => {
                let arr = col.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                    format!(
                        "ColumnPredicate::BoolEq on column '{}' but runtime type is {:?}",
                        self.column(),
                        col.data_type()
                    )
                })?;
                Ok(Some(eval_bool_eq(arr, *value, null_allowed)))
            }
            ColumnPredicate::BigintRange { lo, hi, lo_unbounded, hi_unbounded, .. } => {
                // Accept Int32/Int64 columns; widen Int32 to i64 for compare.
                let result = match col.data_type() {
                    DataType::Int64 => {
                        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                        eval_int_range(
                            arr.len(),
                            |i| arr.value(i),
                            arr.nulls(),
                            *lo,
                            *hi,
                            *lo_unbounded,
                            *hi_unbounded,
                            null_allowed,
                        )
                    }
                    DataType::Int32 => {
                        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                        eval_int_range(
                            arr.len(),
                            |i| arr.value(i) as i64,
                            arr.nulls(),
                            *lo,
                            *hi,
                            *lo_unbounded,
                            *hi_unbounded,
                            null_allowed,
                        )
                    }
                    other => {
                        return Err(format!(
                            "ColumnPredicate::BigintRange on column '{}' \
                             expected Int32/Int64 but got {:?}",
                            self.column(),
                            other
                        ));
                    }
                };
                Ok(Some(result))
            }
            ColumnPredicate::DoubleRange { lo, hi, lo_unbounded, hi_unbounded, .. } => {
                let result = match col.data_type() {
                    DataType::Float64 => {
                        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                        eval_double_range(
                            arr.len(),
                            |i| arr.value(i),
                            arr.nulls(),
                            *lo,
                            *hi,
                            *lo_unbounded,
                            *hi_unbounded,
                            null_allowed,
                        )
                    }
                    DataType::Float32 => {
                        let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                        eval_double_range(
                            arr.len(),
                            |i| arr.value(i) as f64,
                            arr.nulls(),
                            *lo,
                            *hi,
                            *lo_unbounded,
                            *hi_unbounded,
                            null_allowed,
                        )
                    }
                    other => {
                        return Err(format!(
                            "ColumnPredicate::DoubleRange on column '{}' \
                             expected Float32/Float64 but got {:?}",
                            self.column(),
                            other
                        ));
                    }
                };
                Ok(Some(result))
            }
            ColumnPredicate::BigintValues { values, .. } => {
                let result = match col.data_type() {
                    DataType::Int64 => {
                        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                        eval_int_values(
                            arr.len(),
                            |i| arr.value(i),
                            arr.nulls(),
                            values,
                            null_allowed,
                        )
                    }
                    DataType::Int32 => {
                        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                        eval_int_values(
                            arr.len(),
                            |i| arr.value(i) as i64,
                            arr.nulls(),
                            values,
                            null_allowed,
                        )
                    }
                    other => {
                        return Err(format!(
                            "ColumnPredicate::BigintValues on column '{}' \
                             expected Int32/Int64 but got {:?}",
                            self.column(),
                            other
                        ));
                    }
                };
                Ok(Some(result))
            }
            ColumnPredicate::BytesRange { lo, hi, lo_unbounded, hi_unbounded, .. } => {
                let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    format!(
                        "ColumnPredicate::BytesRange on column '{}' but \
                         runtime type is {:?}",
                        self.column(),
                        col.data_type()
                    )
                })?;
                Ok(Some(eval_bytes_range(
                    arr,
                    lo,
                    hi,
                    *lo_unbounded,
                    *hi_unbounded,
                    null_allowed,
                )))
            }
            ColumnPredicate::BytesValues { values, .. } => {
                let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    format!(
                        "ColumnPredicate::BytesValues on column '{}' but \
                         runtime type is {:?}",
                        self.column(),
                        col.data_type()
                    )
                })?;
                Ok(Some(eval_bytes_values(arr, values, null_allowed)))
            }
        }
    }

    fn null_allowed_value(&self) -> bool {
        match self {
            ColumnPredicate::IsNull { null_allowed, .. }
            | ColumnPredicate::IsNotNull { null_allowed, .. }
            | ColumnPredicate::BoolEq { null_allowed, .. }
            | ColumnPredicate::BigintRange { null_allowed, .. }
            | ColumnPredicate::DoubleRange { null_allowed, .. }
            | ColumnPredicate::BigintValues { null_allowed, .. }
            | ColumnPredicate::BytesRange { null_allowed, .. }
            | ColumnPredicate::BytesValues { null_allowed, .. } => *null_allowed,
        }
    }
}

/// Apply a list of predicates to `batch` and return the filtered batch. AND
/// across predicates. A predicate whose target column isn't in the batch is
/// treated as a pass-through. Errors short-circuit so callers see exactly
/// which predicate / column tripped.
pub fn filter_batch(
    batch: &RecordBatch,
    predicates: &[ColumnPredicate],
) -> Result<RecordBatch, String> {
    if predicates.is_empty() || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    // AND-combine row-level booleans.
    let mut mask: Option<BooleanArray> = None;
    for p in predicates {
        let part = p.evaluate(batch)?;
        let Some(part) = part else { continue };
        mask = Some(match mask {
            None => part,
            Some(prev) => and_boolean(&prev, &part),
        });
    }
    let Some(final_mask) = mask else {
        return Ok(batch.clone());
    };
    filter_record_batch(batch, &final_mask).map_err(|e| {
        format!("filter_record_batch failed: {e}")
    })
}

// ──────────────────────────────────────────────────────────────────────────
// Internal evaluators — kept generic over typed-array accessors so we don't
// duplicate the null-handling boilerplate per arrow type.
// ──────────────────────────────────────────────────────────────────────────

fn eval_bool_eq(arr: &BooleanArray, value: bool, null_allowed: bool) -> BooleanArray {
    let mut out = vec![false; arr.len()];
    let nulls = arr.nulls();
    for i in 0..arr.len() {
        let is_null = nulls.map_or(false, |n| n.is_null(i));
        out[i] = if is_null {
            null_allowed
        } else {
            arr.value(i) == value
        };
    }
    BooleanArray::from(out)
}

fn eval_int_range<F: Fn(usize) -> i64>(
    len: usize,
    get: F,
    nulls: Option<&arrow_buffer::NullBuffer>,
    lo: i64,
    hi: i64,
    lo_unbounded: bool,
    hi_unbounded: bool,
    null_allowed: bool,
) -> BooleanArray {
    let mut out = vec![false; len];
    for i in 0..len {
        let is_null = nulls.map_or(false, |n| n.is_null(i));
        out[i] = if is_null {
            null_allowed
        } else {
            let v = get(i);
            let lo_ok = lo_unbounded || v >= lo;
            let hi_ok = hi_unbounded || v <= hi;
            lo_ok && hi_ok
        };
    }
    BooleanArray::from(out)
}

fn eval_double_range<F: Fn(usize) -> f64>(
    len: usize,
    get: F,
    nulls: Option<&arrow_buffer::NullBuffer>,
    lo: f64,
    hi: f64,
    lo_unbounded: bool,
    hi_unbounded: bool,
    null_allowed: bool,
) -> BooleanArray {
    let mut out = vec![false; len];
    for i in 0..len {
        let is_null = nulls.map_or(false, |n| n.is_null(i));
        out[i] = if is_null {
            null_allowed
        } else {
            let v = get(i);
            // NaN: Velox FloatingPointRange treats NaN as not-in-range.
            if v.is_nan() {
                false
            } else {
                let lo_ok = lo_unbounded || v >= lo;
                let hi_ok = hi_unbounded || v <= hi;
                lo_ok && hi_ok
            }
        };
    }
    BooleanArray::from(out)
}

fn eval_int_values<F: Fn(usize) -> i64>(
    len: usize,
    get: F,
    nulls: Option<&arrow_buffer::NullBuffer>,
    values: &[i64],          // sorted, deduped by C++
    null_allowed: bool,
) -> BooleanArray {
    let mut out = vec![false; len];
    for i in 0..len {
        let is_null = nulls.map_or(false, |n| n.is_null(i));
        out[i] = if is_null {
            null_allowed
        } else {
            values.binary_search(&get(i)).is_ok()
        };
    }
    BooleanArray::from(out)
}

fn eval_bytes_range(
    arr: &StringArray,
    lo: &str,
    hi: &str,
    lo_unbounded: bool,
    hi_unbounded: bool,
    null_allowed: bool,
) -> BooleanArray {
    let mut out = vec![false; arr.len()];
    let nulls = arr.nulls();
    for i in 0..arr.len() {
        let is_null = nulls.map_or(false, |n| n.is_null(i));
        out[i] = if is_null {
            null_allowed
        } else {
            let v = arr.value(i);
            let lo_ok = lo_unbounded || v >= lo;
            let hi_ok = hi_unbounded || v <= hi;
            lo_ok && hi_ok
        };
    }
    BooleanArray::from(out)
}

fn eval_bytes_values(
    arr: &StringArray,
    values: &[String],
    null_allowed: bool,
) -> BooleanArray {
    // Small set (typical: a dozen IN-list entries); linear scan is fine.
    // If perf data calls for it, replace with a HashSet<&str> built once.
    let mut out = vec![false; arr.len()];
    let nulls = arr.nulls();
    for i in 0..arr.len() {
        let is_null = nulls.map_or(false, |n| n.is_null(i));
        out[i] = if is_null {
            null_allowed
        } else {
            let v = arr.value(i);
            values.iter().any(|x| x == v)
        };
    }
    BooleanArray::from(out)
}

fn and_boolean(a: &BooleanArray, b: &BooleanArray) -> BooleanArray {
    debug_assert_eq!(a.len(), b.len());
    let mut out = vec![false; a.len()];
    for i in 0..a.len() {
        out[i] = a.value(i) && b.value(i);
    }
    BooleanArray::from(out)
}

/// Build an arrow-rs `parquet::arrow::arrow_reader::RowFilter` for predicate
/// pushdown into the parquet reader. **Phase 4 — safe only when the file
/// group has no log files** (RO mode or pure CoW), because pushing to base
/// parquet would otherwise filter out rows that should have been resolved
/// by a log update.
///
/// Each predicate becomes one `ArrowPredicateFn` evaluated against the
/// projected columns the predicate touches. The parquet reader can skip
/// decoding pages whose statistics don't overlap the predicate.
///
/// Returns `None` when no predicate is pushable (e.g. all target columns
/// missing from the parquet schema, or the predicates set is empty) — the
/// caller then relies on Phase 3's post-merge filter alone.
pub fn build_row_filter(
    predicates: &[ColumnPredicate],
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    arrow_schema: &arrow_schema::Schema,
) -> Option<parquet::arrow::arrow_reader::RowFilter> {
    use parquet::arrow::ProjectionMask;
    use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};

    if predicates.is_empty() {
        return None;
    }

    let mut filters: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> =
        Vec::new();

    for predicate in predicates {
        let Ok(col_idx) = arrow_schema.index_of(predicate.column()) else {
            // Predicate column not in file schema — skip (Phase 3 post-merge
            // filter still runs against the merged batch).
            continue;
        };
        let mask = ProjectionMask::roots(parquet_schema, std::iter::once(col_idx));
        let pred_clone = predicate.clone();
        let arrow_pred = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
            // ArrowPredicateFn expects an arrow-rs error type; map our string
            // errors to ArrowError::ComputeError so they surface cleanly.
            pred_clone
                .evaluate(&batch)
                .map_err(|e| {
                    arrow_schema::ArrowError::ComputeError(format!(
                        "[ENG-40156] parquet row filter eval: {e}"
                    ))
                })
                .map(|opt| {
                    opt.unwrap_or_else(|| {
                        // Column dropped between schema check and eval — treat as
                        // pass-through (all true).
                        BooleanArray::from(vec![true; batch.num_rows()])
                    })
                })
        });
        filters.push(Box::new(arrow_pred));
    }

    if filters.is_empty() { None } else { Some(RowFilter::new(filters)) }
}

/// Phase 4 safety check: parquet pushdown is safe only when no log file
/// updates can override base-file values. Currently we require no log files
/// at all. A future refinement could split predicates per-column based on
/// which columns appear in any log block's schema.
///
/// `has_log_files` mirrors `FileGroupReaderContext.has_log_files` /
/// `ReaderParameters` / `use_read_optimized`. Caller wires whichever they
/// already have.
pub fn is_parquet_pushdown_safe(has_log_files: bool, use_read_optimized: bool) -> bool {
    use_read_optimized || !has_log_files
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn batch_i64_str(ints: Vec<Option<i64>>, strs: Vec<Option<&str>>) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(ints)),
                Arc::new(StringArray::from(strs)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn bigint_range_inclusive_no_nulls() {
        let batch = batch_i64_str(
            vec![Some(1), Some(5), Some(10), Some(15)],
            vec![Some("a"), Some("b"), Some("c"), Some("d")],
        );
        let pred = ColumnPredicate::BigintRange {
            column: "i".into(),
            lo: 5,
            hi: 10,
            lo_unbounded: false,
            hi_unbounded: false,
            null_allowed: false,
        };
        let out = filter_batch(&batch, &[pred]).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn bigint_values_skips_nulls_when_null_disallowed() {
        let batch = batch_i64_str(
            vec![Some(1), None, Some(2), Some(3)],
            vec![Some("a"), Some("b"), Some("c"), Some("d")],
        );
        let pred = ColumnPredicate::BigintValues {
            column: "i".into(),
            values: vec![2, 3],
            null_allowed: false,
        };
        let out = filter_batch(&batch, &[pred]).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn bytes_values_lets_nulls_through_when_allowed() {
        let batch = batch_i64_str(
            vec![Some(1), Some(2), Some(3)],
            vec![Some("a"), None, Some("c")],
        );
        let pred = ColumnPredicate::BytesValues {
            column: "s".into(),
            values: vec!["a".into()],
            null_allowed: true,
        };
        let out = filter_batch(&batch, &[pred]).unwrap();
        // "a" matches; null matches because null_allowed=true; "c" rejected.
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn missing_column_is_passthrough() {
        let batch = batch_i64_str(vec![Some(1)], vec![Some("a")]);
        let pred = ColumnPredicate::BigintRange {
            column: "nonexistent".into(),
            lo: 0,
            hi: 100,
            lo_unbounded: false,
            hi_unbounded: false,
            null_allowed: false,
        };
        let out = filter_batch(&batch, &[pred]).unwrap();
        assert_eq!(out.num_rows(), 1);
    }

    #[test]
    fn and_combines_predicates() {
        let batch = batch_i64_str(
            vec![Some(1), Some(5), Some(10), Some(15)],
            vec![Some("a"), Some("b"), Some("c"), Some("d")],
        );
        let preds = vec![
            ColumnPredicate::BigintRange {
                column: "i".into(),
                lo: 1,
                hi: 10,
                lo_unbounded: false,
                hi_unbounded: false,
                null_allowed: false,
            },
            ColumnPredicate::BytesValues {
                column: "s".into(),
                values: vec!["b".into(), "c".into()],
                null_allowed: false,
            },
        ];
        let out = filter_batch(&batch, &preds).unwrap();
        // i ∈ [1,10] AND s ∈ {b,c} → rows (5,b), (10,c)
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn type_mismatch_is_error() {
        let batch = batch_i64_str(vec![Some(1)], vec![Some("a")]);
        let pred = ColumnPredicate::BigintRange {
            column: "s".into(),  // s is Utf8 not Int64
            lo: 0,
            hi: 100,
            lo_unbounded: false,
            hi_unbounded: false,
            null_allowed: false,
        };
        assert!(filter_batch(&batch, &[pred]).is_err());
    }
}
