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

//! Gold-data comparison helpers shared across reader test suites.
//!
//! A "gold" file is a Spark `SELECT *` snapshot of the merge-correct table
//! contents, stored as a single parquet file under a fixture's `gold_data`
//! directory. [`compare_against_gold`] checks that a reader's output matches
//! that snapshot cell-for-cell on the user (non-`_hoodie_`) columns.
//!
//! Both the core fg-reader harness and the cpp consumer tests compare reader
//! output against the same gold snapshots; this module is the single source of
//! truth for *how* that comparison is done, so the semantics (missing-column
//! handling, timestamp-type tolerance) cannot drift between the two suites.

use std::fs::File;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Column the gold and actual batches are sorted on before comparison.
/// Column the gold and actual batches are sorted on when the caller does not
/// name one. Most fixtures key on `key`; those that do not pass their own.
const SORT_KEY: &str = "key";

/// Prefix of Hudi metadata columns, excluded from the comparison.
const HOODIE_META_PREFIX: &str = "_hoodie_";

/// Read a fixture's gold parquet (`SELECT *` snapshot) from `gold_dir`.
///
/// # Errors
/// Returns `Err` if the directory is unreadable, does not contain exactly one
/// parquet file, or the parquet cannot be decoded.
pub fn read_gold_parquet(gold_dir: &str) -> Result<RecordBatch, String> {
    let entries: Vec<_> = std::fs::read_dir(gold_dir)
        .map_err(|e| format!("gold_data dir '{gold_dir}' not readable: {e}"))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .collect();
    if entries.is_empty() {
        return Err(format!("no parquet files in {gold_dir}"));
    }
    if entries.len() > 1 {
        return Err(format!(
            "expected exactly one parquet file in {gold_dir}, found {}",
            entries.len()
        ));
    }
    let file = File::open(entries[0].path()).map_err(|e| format!("open gold parquet: {e}"))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("build parquet reader: {e}"))?
        .build()
        .map_err(|e| format!("build reader: {e}"))?;
    let batches: Result<Vec<_>, String> = reader
        .map(|b| b.map_err(|e| format!("read gold batch: {e}")))
        .collect();
    let batches = batches?;
    if batches.is_empty() {
        return Err(format!("gold parquet in {gold_dir} produced no batches"));
    }
    arrow::compute::concat_batches(&batches[0].schema(), &batches)
        .map_err(|e| format!("concat gold batches: {e}"))
}

/// Sort `batch` by the [`SORT_KEY`] column (ascending) so rows line up
/// positionally for comparison.
/// Order rows by the key columns so gold and actual line up positionally.
///
/// `sort_key` may name several comma-separated columns: a Hudi record key is
/// unique only within a partition, so a partitioned table needs the partition
/// column alongside it to identify a row.
fn sort_by_key(batch: &RecordBatch, sort_key: &str) -> Result<RecordBatch, String> {
    let mut columns = Vec::new();
    for name in sort_key.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        let idx = batch
            .schema()
            .index_of(name)
            .map_err(|e| format!("sort key column '{name}' not found: {e}"))?;
        columns.push(arrow_ord::sort::SortColumn {
            values: batch.column(idx).clone(),
            options: None,
        });
    }
    if columns.is_empty() {
        return Err(format!("sort key '{sort_key}' names no columns"));
    }
    let indices = arrow_ord::sort::lexsort_to_indices(&columns, None)
        .map_err(|e| format!("lexsort on '{sort_key}' failed: {e}"))?;
    let columns: Result<Vec<_>, String> = batch
        .columns()
        .iter()
        .map(|col| {
            arrow_select::take::take(col, &indices, None).map_err(|e| format!("take failed: {e}"))
        })
        .collect();
    RecordBatch::try_new(batch.schema(), columns?)
        .map_err(|e| format!("rebuild sorted batch failed: {e}"))
}

/// Two cells are "the same" iff both are null, or both are non-null and render
/// to the same string. Used to detect duplicate sort keys; rendering errors are
/// surfaced rather than swallowed.
fn cells_equal(col: &dyn Array, lhs: usize, rhs: usize) -> Result<bool, String> {
    match (col.is_null(lhs), col.is_null(rhs)) {
        (true, true) => Ok(true),
        (true, false) | (false, true) => Ok(false),
        (false, false) => Ok(render_cell(col, lhs)? == render_cell(col, rhs)?),
    }
}

/// Fail if the (already key-sorted) `batch` has duplicate [`SORT_KEY`] values.
///
/// Positional row alignment between actual and gold is only sound when the sort
/// key is unique: `sort_to_indices` is not stable, so rows that share a key can
/// be permuted differently on each side and mask (or fabricate) a mismatch.
/// Enforcing uniqueness here keeps the comparison honest — a fixture that needs
/// a non-unique key must add a tiebreaker rather than silently misalign.
fn ensure_unique_sort_key(batch: &RecordBatch, side: &str, sort_key: &str) -> Result<(), String> {
    let mut cols = Vec::new();
    for name in sort_key.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        let idx = batch
            .schema()
            .index_of(name)
            .map_err(|e| format!("sort key column '{name}' not found: {e}"))?;
        cols.push(batch.column(idx).clone());
    }
    for row in 1..batch.num_rows() {
        // A composite key repeats only when every one of its columns does.
        let mut all_equal = true;
        for col in &cols {
            if !cells_equal(col.as_ref(), row - 1, row)? {
                all_equal = false;
                break;
            }
        }
        if all_equal {
            let dup: Vec<String> = cols
                .iter()
                .map(|col| render_cell(col.as_ref(), row))
                .collect::<Result<_, _>>()?;
            return Err(format!(
                "{side} has a duplicate '{sort_key}' value ('{}'); positional \
                 comparison against gold requires a unique sort key",
                dup.join(",")
            ));
        }
    }
    Ok(())
}

/// Render a single cell to string, propagating (never swallowing) formatter
/// errors. Note: a NULL renders to the empty string here; callers that must
/// distinguish NULL from `""` check [`Array::is_null`] separately.
fn render_cell(col: &dyn Array, row: usize) -> Result<String, String> {
    arrow_cast::display::array_value_to_string(col, row)
        .map_err(|e| format!("render cell row={row} failed: {e}"))
}

/// Whether two arrow types are *value*-compatible for gold comparison.
///
/// Exact `DataType` equality is too strict for nested types: it also compares a
/// child field's **name** and **nullability flag**, which diverge harmlessly
/// between a Spark gold snapshot and the reader's output without any value
/// changing — e.g. the list child is named `element` (nullable) by Spark but
/// `array` by the reader's arrow-avro decode. The arms below tolerate those
/// representation-only differences while still failing on any width/precision
/// change (`Int32` vs `Int64`, `Float32` vs `Float64`, decimal scale, `Utf8` vs
/// `LargeUtf8`), so a value rendered identically under the wrong type can no
/// longer slip through.
///
/// Only the variants the gold fixtures actually exercise are special-cased
/// (`Timestamp`, `List`, `Struct` — see the per-arm rationale). Every other type
/// — including `Map`, `Dictionary`, `LargeList`, and `FixedSizeList` — falls
/// through to exact `DataType` equality, so a divergence there fails LOUDLY at
/// the `_` arm rather than being silently tolerated. If a future fixture needs
/// one of those relaxed, add a *tested* arm for it then; do not pre-add untested
/// arms, which only give false confidence that a path is covered.
fn types_compatible(a: &DataType, b: &DataType) -> bool {
    use DataType::*;
    match (a, b) {
        // Reader emits `Timestamp(us, "UTC")`, Spark gold `Timestamp(ns, None)`:
        // the unit and tz differ but the instant does not. Returning `true` here
        // is ONLY sound because the caller normalizes both columns UP to the
        // finer unit via `normalize_timestamp_pair` BEFORE comparing values —
        // this arm does not itself verify the instant. A caller that compares
        // without that normalization, or that coarsens instead of widens,
        // reintroduces the masking bug where a wrong instant differing only in
        // sub-unit digits rendered identically. Keep this arm and
        // `normalize_timestamp_pair` in lockstep.
        (Timestamp(_, _), Timestamp(_, _)) => true,
        // The list child field name (`array` vs `element`) and its nullability
        // flag differ harmlessly between reader and gold; recurse on the child
        // DATA TYPE only so both are ignored, while a child width/precision
        // change still fails. An element-level null *value* difference is not
        // tolerated — it is caught downstream by the cell render.
        (List(fa), List(fb)) => types_compatible(fa.data_type(), fb.data_type()),
        // Structs must line up by field count, order, and NAME — a renamed or
        // reordered field is a genuine schema change, unlike a list's single
        // anonymous child — then each subfield recurses on its data type. Like
        // the list arm this intentionally drops the per-subfield nullability flag
        // and field metadata: a flag-only difference with no actual null is
        // harmless, and a null *value* mismatch surfaces in the cell render.
        (Struct(fa), Struct(fb)) => {
            fa.len() == fb.len()
                && fa.iter().zip(fb.iter()).all(|(x, y)| {
                    x.name() == y.name() && types_compatible(x.data_type(), y.data_type())
                })
        }
        _ => a == b,
    }
}

/// Resolution rank of a timestamp [`TimeUnit`], higher = finer. Used to pick a
/// common target unit that never coarsens either side.
fn timeunit_rank(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

/// Normalize a pair of `Timestamp` columns to a common dtype so their rendered
/// values are comparable, **without ever coarsening** either side.
///
/// The target unit is the FINER of the two (max [`timeunit_rank`]) so both casts
/// only rescale upward — a coarsening cast (e.g. `ns -> us`) would truncate
/// sub-unit precision and could mask a wrong instant differing only in those low
/// digits. The target tz is gold's, since tz is display-only metadata and
/// unifying it makes `array_value_to_string` render both sides identically when
/// the instant matches; the change is value-preserving.
///
/// Returns `Some((actual, gold))` with the normalized columns when a cast was
/// needed, or `None` when the dtypes already match or either column is not a
/// `Timestamp` (caller keeps the originals). Casting failures are surfaced with
/// the column name for context.
///
/// This is the value-correctness counterpart to the `(Timestamp, Timestamp)`
/// arm of [`types_compatible`], which tolerates the dtype difference on the
/// assumption that this normalization runs first. The two MUST stay coupled: the
/// type arm only checks *type* compatibility, never the instant.
fn normalize_timestamp_pair(
    col_name: &str,
    actual: &ArrayRef,
    gold: &ArrayRef,
) -> Result<Option<(ArrayRef, ArrayRef)>, String> {
    match (actual.data_type(), gold.data_type()) {
        (DataType::Timestamp(au, _), DataType::Timestamp(gu, gtz))
            if actual.data_type() != gold.data_type() =>
        {
            let finer = if timeunit_rank(au) >= timeunit_rank(gu) {
                *au
            } else {
                *gu
            };
            let target = DataType::Timestamp(finer, gtz.clone());
            let cast = |col: &ArrayRef| {
                arrow_cast::cast(col, &target).map_err(|e| {
                    format!(
                        "col '{col_name}' cast {:?} -> {target:?} failed: {e}",
                        col.data_type()
                    )
                })
            };
            Ok(Some((cast(actual)?, cast(gold)?)))
        }
        _ => Ok(None),
    }
}

/// Whether `col` carries a null *below the top level* — a null list/map/struct
/// element, a null map value, or a null struct subfield — that the positional
/// top-level [`Array::is_null`] check in [`compare_against_gold`] cannot see.
///
/// String rendering collapses a null element and an empty value to the same text
/// (`["a", null]` and `["a", ""]` both render `[a, ]`), so a container holding
/// such a null cannot be compared safely by rendering: a genuine null-vs-empty
/// divergence would be masked. [`compare_against_gold`] therefore refuses to
/// render-compare such a column and directs the caller to `Expected::Custom`
/// typed extraction (see the `null_container_elements` harness case).
///
/// Conservative by design: it also fires when a nested null is merely the shadow
/// of a null *parent* cell (which the top-level check would already catch),
/// trading a few needless `Expected::Custom` routings for a hard guarantee that
/// no container-internal null is ever silently rendered away. Recurses so a null
/// nested arbitrarily deep (e.g. a null struct subfield inside a list element) is
/// still caught.
fn has_container_internal_null(col: &dyn Array) -> bool {
    use arrow_array::cast::AsArray;
    let child_has_null =
        |child: &ArrayRef| child.null_count() > 0 || has_container_internal_null(child.as_ref());
    match col.data_type() {
        DataType::List(_) => child_has_null(col.as_list::<i32>().values()),
        DataType::LargeList(_) => child_has_null(col.as_list::<i64>().values()),
        DataType::FixedSizeList(_, _) => child_has_null(col.as_fixed_size_list().values()),
        // Map keys are non-null by the parquet/arrow contract; checking every
        // entries column (keys and values) is still correct and future-proof.
        DataType::Map(_, _) => col.as_map().entries().columns().iter().any(child_has_null),
        DataType::Struct(_) => col.as_struct().columns().iter().any(child_has_null),
        _ => false,
    }
}

/// Compare reader output against gold data.
///
/// Both batches are sorted by the `key` column (which must be unique on each
/// side — see [`ensure_unique_sort_key`]), then every user column
/// (non-`_hoodie_`) present in `gold` is compared cell-for-cell against the
/// same-named column in `actual`:
///
/// - A user column present in gold but **absent from `actual` is a failure** —
///   a dropped column is a regression, not something to silently skip.
/// - **Column arrow types must match.** A differing dtype is a failure, so a
///   value that happens to render identically under a wrong type (e.g. `Int32`
///   vs `Int64`, `Float32` vs `Float64`) can no longer pass. The sole exception
///   is timestamp representation: two `Timestamp` columns that differ only in
///   unit (`us` vs `ns`) or tz encode the same instant, so BOTH columns are
///   normalized to the FINER of the two units (and gold's tz) before the value
///   check — see [`normalize_timestamp_pair`]. Casting to a finer-or-equal unit
///   only rescales (never truncates) and tz changes are metadata-only, so the
///   normalization is value-correct in EITHER direction: crucially it never
///   coarsens a column, which could otherwise truncate sub-unit precision and
///   mask a wrong instant. A `Timestamp` vs non-`Timestamp` mismatch is still a
///   failure.
/// - **NULL is compared positionally and is distinct from any value.** Cells
///   are checked with [`Array::is_null`] *before* rendering, so a NULL on one
///   side and a non-NULL (including the empty string `""`) on the other is a
///   failure rather than being conflated — `array_value_to_string` renders both
///   NULL and `""` as `""`.
///
/// # Errors
/// Returns `Err(message)` describing the first failure (duplicate sort key,
/// row-count mismatch, missing column, dtype mismatch, failed timestamp cast,
/// null/non-null mismatch, render error, or differing cell value).
pub fn compare_against_gold(actual: &RecordBatch, gold: &RecordBatch) -> Result<(), String> {
    compare_against_gold_keyed(actual, gold, SORT_KEY)
}

/// [`compare_against_gold`] for a fixture whose rows are identified by a column
/// other than `key`.
pub fn compare_against_gold_keyed(
    actual: &RecordBatch,
    gold: &RecordBatch,
    sort_key: &str,
) -> Result<(), String> {
    let actual_sorted = sort_by_key(actual, sort_key)?;
    let gold_sorted = sort_by_key(gold, sort_key)?;

    ensure_unique_sort_key(&actual_sorted, "actual", sort_key)?;
    ensure_unique_sort_key(&gold_sorted, "gold", sort_key)?;

    if actual_sorted.num_rows() != gold_sorted.num_rows() {
        return Err(format!(
            "row count mismatch: actual={} gold={}",
            actual_sorted.num_rows(),
            gold_sorted.num_rows()
        ));
    }

    let user_cols: Vec<String> = gold_sorted
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .filter(|n| !n.starts_with(HOODIE_META_PREFIX))
        .collect();

    for col_name in &user_cols {
        let actual_idx = match actual_sorted.schema().index_of(col_name) {
            Ok(i) => i,
            Err(_) => {
                return Err(format!(
                    "column '{col_name}' present in gold but missing from actual output"
                ));
            }
        };
        let gold_idx = gold_sorted
            .schema()
            .index_of(col_name)
            .map_err(|e| format!("gold missing column '{col_name}': {e}"))?;
        let actual_col = actual_sorted.column(actual_idx);
        let gold_col = gold_sorted.column(gold_idx);

        // Column types must be value-compatible. A width/precision change
        // (Int32 vs Int64, Float32 vs Float64, decimal scale) is a genuine
        // regression and must fail here rather than be masked by the string
        // comparison below; harmless representation differences (nested field
        // names/nullability, timestamp unit/tz) are tolerated — see
        // [`types_compatible`].
        if !types_compatible(actual_col.data_type(), gold_col.data_type()) {
            return Err(format!(
                "col '{col_name}' dtype mismatch: actual={:?} gold={:?}",
                actual_col.data_type(),
                gold_col.data_type(),
            ));
        }
        // A null inside a container (list/map/struct element) is invisible to the
        // top-level positional null check below and renders identically to an
        // empty value, so render-comparing such a column could silently mask a
        // null-vs-empty divergence. Refuse, and direct to typed extraction — see
        // [`has_container_internal_null`].
        if has_container_internal_null(actual_col.as_ref())
            || has_container_internal_null(gold_col.as_ref())
        {
            return Err(format!(
                "col '{col_name}' has a null inside a container (list/map/struct element); \
                 gold string comparison renders a null element and an empty value identically \
                 and cannot distinguish them — assert this column with Expected::Custom typed \
                 extraction instead"
            ));
        }
        // Two Timestamp columns may differ in unit/tz while encoding the same
        // instant. Normalize BOTH to the finer of the two units (and gold's tz)
        // so neither side is coarsened — a coarsening cast would truncate
        // sub-unit precision and could mask a wrong instant. Other tolerated
        // differences (names/nullability) don't change the rendering, so no cast
        // is needed.
        let normalized = normalize_timestamp_pair(col_name, actual_col, gold_col)?;
        let (actual_col, gold_col) = match &normalized {
            Some((actual_norm, gold_norm)) => (actual_norm, gold_norm),
            None => (actual_col, gold_col),
        };

        for row in 0..actual_sorted.num_rows() {
            // Check null-ness before rendering: a NULL and an empty string both
            // render to "", so they must be distinguished here, not by value.
            let actual_null = actual_col.is_null(row);
            let gold_null = gold_col.is_null(row);
            if actual_null != gold_null {
                return Err(format!(
                    "null mismatch at row={row} col='{col_name}': actual_null={actual_null} gold_null={gold_null}"
                ));
            }
            if actual_null {
                continue;
            }
            let actual_str = render_cell(actual_col.as_ref(), row)
                .map_err(|e| format!("col '{col_name}' actual: {e}"))?;
            let gold_str = render_cell(gold_col.as_ref(), row)
                .map_err(|e| format!("col '{col_name}' gold: {e}"))?;
            if actual_str != gold_str {
                return Err(format!(
                    "mismatch at row={row} col='{col_name}': actual='{actual_str}' gold='{gold_str}'"
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::types::Int32Type;
    use arrow_array::{Int32Array, ListArray, TimestampMicrosecondArray, TimestampNanosecondArray};
    use arrow_schema::{Field, Schema};

    use super::*;

    /// One-row batch with a unique `key` and a single `arr` list-of-int column,
    /// so [`compare_against_gold`]'s sort/uniqueness gates pass and only the
    /// container-null guard is under test.
    fn list_batch(key: i32, elems: Vec<Option<i32>>) -> RecordBatch {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(elems)]);
        let schema = Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("arr", list.data_type().clone(), true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![key])) as ArrayRef,
                Arc::new(list),
            ],
        )
        .unwrap()
    }

    /// Build a one-row batch with a unique `key` and a single timestamp `ts`
    /// column, so [`compare_against_gold`]'s sort/uniqueness gates are satisfied
    /// and only the timestamp normalization is under test.
    fn ts_batch(key: i32, ts: ArrayRef) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("ts", ts.data_type().clone(), true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![key])) as ArrayRef, ts],
        )
        .unwrap()
    }

    /// Regression for the "coarsening cast masks a wrong instant" bug: when gold
    /// is the coarser column (`us`) and actual carries finer (`ns`) precision,
    /// the two instants here differ only by 500 ns. Casting actual DOWN to `us`
    /// would truncate that difference and pass; normalizing both UP to `ns` must
    /// instead surface the mismatch.
    #[test]
    fn sub_microsecond_difference_not_masked_when_gold_is_coarser() {
        let actual = ts_batch(
            1,
            Arc::new(TimestampNanosecondArray::from(vec![1_000_000_500])),
        );
        let gold = ts_batch(
            1,
            Arc::new(TimestampMicrosecondArray::from(vec![1_000_000])),
        );
        let err = compare_against_gold(&actual, &gold)
            .expect_err("a 500ns instant difference must not be masked by coarsening");
        assert!(
            err.contains("col='ts'"),
            "expected a 'ts' value mismatch, got: {err}"
        );
    }

    /// The same instant expressed in different units (`us` vs `ns`) must compare
    /// equal: normalizing both to the finer unit leaves them identical.
    #[test]
    fn equal_instant_compares_equal_across_units() {
        let actual = ts_batch(
            1,
            Arc::new(TimestampMicrosecondArray::from(vec![1_000_000])),
        );
        let gold = ts_batch(
            1,
            Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000])),
        );
        compare_against_gold(&actual, &gold)
            .expect("equal instants in us vs ns must compare equal");
    }

    /// A null element inside a container must be REFUSED, not render-compared —
    /// even when both sides are byte-identical. Rendering can't tell a null
    /// element from an empty value, so the only safe outcome is to fail and
    /// direct the author to `Expected::Custom`. Using the same batch on both
    /// sides proves the guard fires on the representation, not on a diff.
    #[test]
    fn container_internal_null_is_refused_not_masked() {
        let batch = list_batch(1, vec![Some(1), None, Some(3)]);
        let err = compare_against_gold(&batch, &batch)
            .expect_err("a container-internal null must not be render-compared");
        assert!(
            err.contains("Expected::Custom") && err.contains("'arr'"),
            "guard must name the column and direct to Expected::Custom, got: {err}"
        );
    }

    /// A null-free container must NOT trip the guard — it compares normally.
    #[test]
    fn null_free_container_compares_normally() {
        let batch = list_batch(1, vec![Some(1), Some(2), Some(3)]);
        compare_against_gold(&batch, &batch)
            .expect("a null-free list column must compare without tripping the guard");
    }
}
