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

//! Batch-level schema-evolution projector.
//!
//! Equivalent of gold's record rewrite (`HoodieAvroUtils.rewriteRecordWithNewSchema`,
//! avro log path) and cast projection (`HoodieParquetFileFormatHelper.generateUnsafeProjection`,
//! parquet base path): reorder columns by name, null-fill added columns, cast
//! promoted types. Gold-parity cast rules:
//!   * Float32→Float64: STRING-MEDIATED (both gold paths do this; C6 value-exactness)
//!   * numeric→Utf8: Java `String.valueOf` formatting
//!   * struct/list/map: recursive
//!   * everything else: `arrow_cast::cast`

use crate::Result;
use crate::error::CoreError;
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, new_null_array};
use arrow_schema::{DataType, FieldRef, SchemaRef, TimeUnit};
use std::sync::Arc;

/// Microseconds per millisecond — the ÷1000 factor for the NTZ (local-timestamp)
/// micros→millis arithmetic conversion. Mirrors Java `DateTimeUtils.MICROS_PER_MILLIS`.
const MICROS_PER_MILLIS: i64 = 1000;

/// Project `batch` to `target` schema: reorder by name, null-fill missing
/// nullable columns, evolve types. Identity-cheap when schemas already match.
pub fn project_batch_to_schema(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    if batch.schema() == *target {
        return Ok(batch.clone());
    }
    let num_rows = batch.num_rows();
    let batch_schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for tf in target.fields() {
        match index_of_ci(&batch_schema, tf.name())? {
            Some(idx) => columns.push(evolve_array(batch.column(idx), tf)?),
            None => {
                if tf.is_nullable() {
                    columns.push(new_null_array(tf.data_type(), num_rows));
                } else {
                    return Err(CoreError::Schema(format!(
                        "evolution: non-nullable column '{}' absent from source batch",
                        tf.name()
                    )));
                }
            }
        }
    }
    RecordBatch::try_new(target.clone(), columns)
        .map_err(|e| CoreError::Schema(format!("evolution: rebuild under target schema: {e}")))
}

/// Locate a column by name, preferring an exact match and falling back to a
/// case-insensitive match (gold/Spark resolve field names case-insensitively).
///
/// Returns `Ok(None)` when no field matches (the caller null-fills) and an
/// error when more than one field matches case-insensitively without an exact
/// match — ambiguous, so fail loudly rather than silently picking one.
pub(crate) fn index_of_ci(schema: &arrow_schema::Schema, name: &str) -> Result<Option<usize>> {
    if let Ok(idx) = schema.index_of(name) {
        return Ok(Some(idx));
    }
    let mut found: Option<usize> = None;
    for (idx, field) in schema.fields().iter().enumerate() {
        if field.name().eq_ignore_ascii_case(name) {
            if found.is_some() {
                return Err(CoreError::Schema(format!(
                    "evolution: column '{name}' matches multiple source columns \
                     case-insensitively; ambiguous projection"
                )));
            }
            found = Some(idx);
        }
    }
    Ok(found)
}

/// True for any nested/container Arrow type the recursion arms care about.
/// Matching variants (List/Struct/Map) are handled by the recursion arms above
/// the guard; this catches everything else (LargeList, FixedSizeList, and any
/// container present on only one side) so it errors instead of silently routing
/// through `arrow_cast`.
fn is_container(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
    )
}

fn evolve_array(src: &ArrayRef, target_field: &FieldRef) -> Result<ArrayRef> {
    let st = src.data_type();
    let tt = target_field.data_type();
    if st == tt {
        return Ok(src.clone());
    }
    match (st, tt) {
        // Gold C6: float→double via string round-trip (both gold paths).
        (DataType::Float32, DataType::Float64) => {
            let s = float_to_java_string_array(src)?;
            arrow_cast::cast(&s, &DataType::Float64)
                .map_err(|e| CoreError::Schema(format!("evolution f32->f64: {e}")))
        }
        // numeric → string with Java String.valueOf formatting.
        // Widening an integer is exact, so a direct cast matches Java. Avro
        // permits int → long as a spec promotion, and a base file written before
        // the column was promoted still holds the narrow type.
        (DataType::Int32, DataType::Int64) => arrow_cast::cast(src, &DataType::Int64)
            .map_err(|e| CoreError::Schema(format!("evolution i32->i64: {e}"))),
        (DataType::Float32 | DataType::Float64, DataType::Utf8) => float_to_java_string_array(src),
        (DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64, DataType::Utf8) => {
            arrow_cast::cast(src, &DataType::Utf8)
                .map_err(|e| CoreError::Schema(format!("evolution int->utf8: {e}")))
        }
        // Nested struct: recurse field-by-field (handles add+promote inside).
        (DataType::Struct(_), DataType::Struct(tfields)) => {
            let sa = src
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .expect("struct array");
            let mut children: Vec<ArrayRef> = Vec::with_capacity(tfields.len());
            for tf in tfields {
                match sa.column_by_name(tf.name()) {
                    Some(child) => children.push(evolve_array(child, tf)?),
                    None if tf.is_nullable() => {
                        children.push(new_null_array(tf.data_type(), sa.len()))
                    }
                    None => {
                        return Err(CoreError::Schema(format!(
                            "evolution: non-nullable struct child '{}' absent",
                            tf.name()
                        )));
                    }
                }
            }
            Ok(Arc::new(
                arrow_array::StructArray::try_new(tfields.clone(), children, sa.nulls().cloned())
                    .map_err(|e| {
                    CoreError::Schema(format!(
                        "evolution: rebuild struct '{}': {e}",
                        target_field.name()
                    ))
                })?,
            ))
        }
        // Nested list: recurse on values; rebuilt list carries the TARGET element field.
        (DataType::List(_), DataType::List(telem)) => {
            let la = src
                .as_any()
                .downcast_ref::<arrow_array::ListArray>()
                .expect("list array");
            let new_values = evolve_array(la.values(), telem)?;
            Ok(Arc::new(
                arrow_array::ListArray::try_new(
                    telem.clone(),
                    la.offsets().clone(),
                    new_values,
                    la.nulls().cloned(),
                )
                .map_err(|e| {
                    CoreError::Schema(format!(
                        "evolution: rebuild list '{}': {e}",
                        target_field.name()
                    ))
                })?,
            ))
        }
        // Nested map: recurse on entries struct.
        (DataType::Map(_, _), DataType::Map(tentries, sorted)) => {
            let ma = src
                .as_any()
                .downcast_ref::<arrow_array::MapArray>()
                .expect("map array");
            let entries: ArrayRef = Arc::new(ma.entries().clone());
            let new_entries = evolve_array(&entries, tentries)?;
            let sa = new_entries
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .expect("map entries struct")
                .clone();
            Ok(Arc::new(
                arrow_array::MapArray::try_new(
                    tentries.clone(),
                    ma.offsets().clone(),
                    sa,
                    ma.nulls().cloned(),
                    *sorted,
                )
                .map_err(|e| {
                    CoreError::Schema(format!(
                        "evolution: rebuild map '{}': {e}",
                        target_field.name()
                    ))
                })?,
            ))
        }
        // TZ-AWARE (LTZ) timestamp-millis logical-type repair (apache/hudi#18132). A
        // column the table schema declares timestamp-millis but the file stored as
        // timestamp-micros is an "affected" column: Hudi's old InternalSchema had a
        // single (micros-assumed) Timestamp type, so schema processing mislabeled
        // millis columns as micros — yet the stored i64 values are actually
        // milliseconds. Match the Java reader (`AvroSchemaRepair.needsLogicalTypeRepair`
        // case 2, AvroSchemaRepair.java:133-134): REINTERPRET the i64 as millis
        // (relabel the unit, keep the value) rather than arrow_cast, which would
        // arithmetically divide by 1000 and corrupt the timestamp by ~3 orders of
        // magnitude. The target field's unit + timezone are applied to the same epoch
        // buffer.
        //
        // GATED ON tz-AWARENESS (both sides `Some`): Java's `needsLogicalTypeRepair`
        // matches ONLY the tz-AWARE logical classes — `LogicalTypes.TimestampMicros`
        // (file) and `LogicalTypes.TimestampMillis` (table). arrow-avro maps those to
        // `Timestamp(_, Some(tz))` and the NTZ `local-timestamp-*` classes to
        // `Timestamp(_, None)` (arrow-avro codec.rs), so `Some` on both sides is
        // exactly the tz-aware pair Java repairs. A tz-aware micros→millis *narrowing*
        // is not valid Hudi schema evolution, so a file-micros/table-millis pairing
        // can only arise from the #18132 mislabel — which is why the reinterpret is
        // correct. The NTZ pair is handled by the next arm; every other timestamp
        // combination goes through arrow_cast below.
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some(src_tz)),
            DataType::Timestamp(TimeUnit::Millisecond, Some(target_tz)),
        ) => {
            // A differing timezone is unexpected for the #18132 mislabel (Java gates
            // the repair on isAdjustedToUTC being equal on both sides). The value is
            // still instant-preserving — the i64 epoch is unchanged and Arrow stores
            // tz-aware timestamps as UTC epoch, so relabeling the tz does not move the
            // instant — but surface it so an operator can spot a genuinely unexpected
            // schema pairing during debugging.
            if src_tz != target_tz {
                log::warn!(
                    "evolution: reinterpret timestamp micros→millis for field '{}' \
                     across differing timezones (file={src_tz:?}, table={target_tz:?}); \
                     value is instant-preserving but the pairing is unexpected for #18132",
                    target_field.name()
                );
            }
            let rebuilt = src
                .to_data()
                .into_builder()
                .data_type(tt.clone())
                .build()
                .map_err(|e| {
                    CoreError::Schema(format!(
                        "evolution: reinterpret timestamp micros→millis for field '{}': {e}",
                        target_field.name()
                    ))
                })?;
            Ok(arrow_array::make_array(rebuilt))
        }
        // NTZ (local-timestamp) micros→millis: ARITHMETIC ÷1000, NOT a reinterpret.
        // Java's `AvroSchemaRepair` does NOT repair the NTZ pair (its case 2 matches
        // only the tz-aware classes, AvroSchemaRepair.java:133-134), so a NTZ
        // `local-timestamp-micros` writer feeding a `local-timestamp-millis` reader is
        // treated as a genuine unit conversion and flows through
        // `HoodieAvroUtils.rewriteRecordWithNewSchema` →
        // `rewritePrimaryType` → `DateTimeUtils.microsToMillis` (HoodieAvroUtils.java:1225-1227),
        // i.e. `Math.floorDiv(micros, 1000)`.
        //
        // We compute the divide HERE with `div_euclid` (== `Math.floorDiv` for a
        // positive divisor, including negative/pre-1970 instants) instead of delegating
        // to `arrow_cast`, whose timestamp downscale truncates toward zero (`o / 1000`)
        // and therefore disagrees with Java by 1ms on negative sub-millisecond values.
        // Both sides are `None` (NTZ) by construction — arrow-avro maps the local
        // logical classes to a tz-less `Timestamp`.
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ) => {
            let micros = src
                .as_any()
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    CoreError::Schema(format!(
                        "evolution: NTZ micros→millis for field '{}': source is not a \
                         TimestampMicrosecondArray",
                        target_field.name()
                    ))
                })?;
            let millis: arrow_array::TimestampMillisecondArray =
                micros.unary(|v| v.div_euclid(MICROS_PER_MILLIS));
            Ok(Arc::new(millis))
        }
        // Container-variant drift (e.g. LargeList source vs List target, or a
        // container on only one side) would silently bypass the recursion arms
        // and their gold-parity casts (string-mediated float→double) via
        // arrow_cast. Fail loudly instead. Both-sides-non-container falls through
        // to the arrow_cast arm below.
        (st, tt) if is_container(st) || is_container(tt) => Err(CoreError::Schema(format!(
            "evolution: unsupported container combination {st} -> {tt} for field '{}' \
             (recursion expects matching List/Struct/Map variants)",
            target_field.name()
        ))),
        // Everything else (int widenings, string<->bytes, decimal, ...): arrow cast.
        _ => arrow_cast::cast(src, tt)
            .map_err(|e| CoreError::Schema(format!("evolution cast {st} -> {tt}: {e}"))),
    }
}

/// Classified parts of a finite, non-zero float value, computed at the value's
/// native width (f32 vs f64) so the digits match Java's per-type shortest repr.
struct FiniteFloatParts {
    /// Whether `1e-3 <= |v| < 1e7` (Java's decimal-notation window).
    in_decimal_range: bool,
    /// `format!("{v}")` — Rust shortest decimal.
    shortest: String,
    /// `format!("{v:e}")`, e.g. `"1e10"`, `"-1.5e-8"`.
    scientific: String,
}

/// Re-shape Rust's shortest-repr float strings into Java `Float`/`Double.toString`
/// notation: decimal with >=1 fraction digit for `1e-3 <= |v| < 1e7`, else
/// `"m.mmE±x"` scientific (no `+` after `E` for positive exponents).
fn java_repr_finite(parts: FiniteFloatParts) -> String {
    let FiniteFloatParts {
        in_decimal_range,
        shortest,
        scientific,
    } = parts;
    if in_decimal_range {
        if shortest.contains('.') || shortest.contains('e') || shortest.contains('E') {
            shortest
        } else {
            format!("{shortest}.0")
        }
    } else {
        let (m, e) = scientific.split_once('e').expect("exp format");
        let m = if m.contains('.') {
            m.to_string()
        } else {
            format!("{m}.0")
        };
        format!("{m}E{e}")
    }
}

/// Java `Double.toString` semantics for an `f64`.
fn java_double_repr(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v < 0.0 { "-Infinity" } else { "Infinity" }.to_string();
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0.0" } else { "0.0" }.to_string();
    }
    let a = v.abs();
    java_repr_finite(FiniteFloatParts {
        in_decimal_range: (1e-3..1e7).contains(&a),
        shortest: format!("{v}"),
        scientific: format!("{v:e}"),
    })
}

/// Java `Float.toString` semantics for an `f32`. Formatting is done at f32 width
/// so the shortest representation matches Java (e.g. `0.1f32 -> "0.1"`, NOT the
/// widened `"0.10000000149011612"`).
fn java_float_repr(v: f32) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v < 0.0 { "-Infinity" } else { "Infinity" }.to_string();
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0.0" } else { "0.0" }.to_string();
    }
    let a = v.abs();
    java_repr_finite(FiniteFloatParts {
        in_decimal_range: (1e-3f32..1e7f32).contains(&a),
        shortest: format!("{v}"),
        scientific: format!("{v:e}"),
    })
}

fn float_to_java_string_array(src: &ArrayRef) -> Result<ArrayRef> {
    let out: StringArray = match src.data_type() {
        DataType::Float32 => {
            let a = src
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .unwrap();
            a.iter().map(|o| o.map(java_float_repr)).collect()
        }
        DataType::Float64 => {
            let a = src
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .unwrap();
            a.iter().map(|o| o.map(java_double_repr)).collect()
        }
        other => {
            return Err(CoreError::Schema(format!(
                "float_to_java_string_array on non-float {other}"
            )));
        }
    };
    Ok(Arc::new(out))
}

#[cfg(test)]
mod tests {
    use super::project_batch_to_schema;
    use arrow_array::{
        Array, ArrayRef, Float32Array, Int32Array, RecordBatch, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use std::sync::Arc;

    fn batch(fields: Vec<Field>, cols: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    #[test]
    fn test_project_null_fill_missing_column() {
        let b = batch(
            vec![Field::new("id", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        );
        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("tag", DataType::Utf8, true),
        ]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        assert_eq!(out.schema(), target);
        assert!(out.column(1).is_null(0) && out.column(1).is_null(1));
    }

    #[test]
    fn test_project_case_insensitive_preserves_values() {
        // Source column differs from target only in case (`ID` vs `id`). A
        // case-sensitive lookup would treat `id` as absent and null-fill it,
        // silently discarding the real values. Case-insensitive matching must
        // carry the real values through.
        let b = batch(
            vec![Field::new("ID", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![10, 20]))],
        );
        let target: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        assert_eq!(out.schema(), target);
        let col = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(col.values(), &[10, 20]);
        assert!(!col.is_null(0) && !col.is_null(1));
    }

    #[test]
    fn test_project_exact_match_wins_over_case_insensitive() {
        // When both an exact and a case-variant column exist, the exact match
        // is selected (no ambiguity error).
        let b = batch(
            vec![
                Field::new("ID", DataType::Int32, true),
                Field::new("id", DataType::Int32, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![2])),
            ],
        );
        let target: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        let col = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(col.value(0), 2, "exact-named `id` column must win");
    }

    #[test]
    fn test_project_ambiguous_case_insensitive_match_errors() {
        // Two source columns match the target case-insensitively and neither is
        // an exact match — ambiguous, must error rather than guess.
        let b = batch(
            vec![
                Field::new("ID", DataType::Int32, true),
                Field::new("Id", DataType::Int32, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![2])),
            ],
        );
        let target: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        assert!(project_batch_to_schema(&b, &target).is_err());
    }

    #[test]
    fn test_project_timestamp_micros_to_millis_ntz_divides_like_java() {
        // S4: the NTZ (local-timestamp) micros→millis pair is a GENUINE arithmetic
        // conversion, NOT the #18132 reinterpret. Java's AvroSchemaRepair does not
        // repair the NTZ classes (AvroSchemaRepair.java:133-134 matches only the
        // tz-aware TimestampMicros/TimestampMillis), so the value flows through
        // HoodieAvroUtils.rewriteRecordWithNewSchema → DateTimeUtils.microsToMillis =
        // Math.floorDiv(micros, 1000). Both sides are tz-less (None) — the NTZ pair.
        //
        // Discriminating: a POSITIVE value proves ÷1000 (not the old reinterpret that
        // kept the i64), and a NEGATIVE sub-millisecond value proves floorDiv, NOT
        // arrow_cast's truncate-toward-zero. floorDiv(-1500, 1000) = -2; trunc = -1.
        const POS_MICROS: i64 = 1_700_000_000_000_123; // → 1_700_000_000_000 ms
        const NEG_MICROS: i64 = -1500; // → floorDiv -2 ms (arrow_cast trunc would give -1)
        let b = batch(
            vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            )],
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                Some(POS_MICROS),
                None,
                Some(NEG_MICROS),
            ]))],
        );
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        assert_eq!(
            out.schema(),
            target,
            "output must carry the millis target type"
        );
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("NTZ micros→millis column must be TimestampMillisecondArray");
        assert_eq!(
            col.value(0),
            POS_MICROS.div_euclid(1000),
            "NTZ micros→millis must divide by 1000 (Java microsToMillis), not reinterpret"
        );
        assert_ne!(
            col.value(0),
            POS_MICROS,
            "must NOT keep the raw i64 (that is the tz-aware #18132 reinterpret path)"
        );
        assert!(col.is_null(1), "null must survive the conversion");
        assert_eq!(
            col.value(2),
            -2,
            "floorDiv(-1500, 1000) = -2; arrow_cast trunc-toward-zero would wrongly give -1"
        );
    }

    #[test]
    fn test_project_timestamp_micros_to_millis_preserves_timezone_and_nulls() {
        // The target field's timezone is applied to the same epoch buffer, and
        // null entries are preserved through the reinterpret. tz-AWARE on both sides
        // (Some) — the only pairing that reaches the #18132 reinterpret arm after S4.
        let b = batch(
            vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            )],
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![Some(1_700_000_000_000), None])
                    .with_timezone("UTC"),
            )],
        );
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        )]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        assert_eq!(out.schema(), target);
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(col.value(0), 1_700_000_000_000);
        assert!(col.is_null(1), "null must survive the reinterpret");
    }

    #[test]
    fn test_project_timestamp_micros_to_millis_tz_aware_both_sides() {
        // The canonical apache/hudi#18132 shape: the affected column is tz-aware
        // (isAdjustedToUTC=true) on BOTH sides -- Timestamp(Micros, Some("UTC")) in
        // the file, Timestamp(Millis, Some("UTC")) in the table. The i64 must be
        // reinterpreted (unit relabeled, value kept) and the timezone preserved.
        const MS_SINCE_EPOCH: i64 = 1_700_000_000_000; // 2023-11-14 as ms
        let b = batch(
            vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            )],
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![MS_SINCE_EPOCH]).with_timezone("UTC"),
            )],
        );
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        )]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        assert_eq!(out.schema(), target, "unit relabeled + timezone preserved");
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("reinterpreted column must be TimestampMillisecondArray");
        assert_eq!(
            col.value(0),
            MS_SINCE_EPOCH,
            "value reinterpreted (same i64), not divided by 1000"
        );
    }

    #[test]
    fn test_project_timestamp_millis_to_micros_uses_arrow_cast() {
        // Guard that the reinterpret special case is ONE-directional: the reverse
        // (Millis→Micros) is a legitimate widening and must still go through
        // arrow_cast (value ×1000), not the reinterpret arm. 1_700 ms → 1_700_000 µs.
        const MS: i64 = 1_700;
        let b = batch(
            vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )],
            vec![Arc::new(TimestampMillisecondArray::from(vec![MS]))],
        );
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("target is micros");
        assert_eq!(
            col.value(0),
            MS * 1000,
            "reverse direction must arrow_cast (×1000), not reinterpret"
        );
    }

    #[test]
    fn test_project_timestamp_same_unit_micros_does_not_reinterpret() {
        // Guard against future match-arm reordering: a same-unit micros→micros
        // pairing that differs only by timezone must NOT hit the micros→millis
        // reinterpret arm. It goes through arrow_cast, which for equal units is a
        // value-preserving tz relabel (the i64 is unchanged). 1_700_000_000_000 µs.
        const US: i64 = 1_700_000_000_000;
        let b = batch(
            vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            )],
            vec![Arc::new(TimestampMicrosecondArray::from(vec![US]))],
        );
        // Offset-based tz ("+00:00"): arrow_cast parses the target tz, and named
        // zones ("UTC") need the chrono-tz feature which this build omits. The
        // reinterpret arm only relabels so it accepts any string, but this pairing
        // must NOT reach it — it goes through arrow_cast, so use an offset tz.
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        )]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        assert_eq!(out.schema(), target, "target micros type + tz applied");
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("stays micros");
        assert_eq!(col.value(0), US, "same-unit value unchanged (not ÷1000)");
    }

    #[test]
    fn test_project_timestamp_micros_to_millis_uses_target_field_tz_not_source_array_tz() {
        // The rebuild must apply the TARGET field's metadata regardless of the
        // source array's own embedded timezone: here the source is tz-aware "+00:00"
        // but the target field declares "UTC". The output must be
        // Timestamp(Millis, Some("UTC")) with the i64 reinterpreted (not ÷1000).
        // Both sides tz-aware (Some) — the only pairing reaching the reinterpret arm.
        const MS_SINCE_EPOCH: i64 = 1_700_000_000_000;
        let b = batch(
            vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                true,
            )],
            // Source array with a tz that DIFFERS from the target field's tz.
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![MS_SINCE_EPOCH]).with_timezone("+00:00"),
            )],
        );
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        )]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        assert_eq!(
            out.schema(),
            target,
            "output carries the TARGET field's tz (UTC), not the source array's (+00:00)"
        );
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("reinterpreted column must be TimestampMillisecondArray");
        assert_eq!(
            col.value(0),
            MS_SINCE_EPOCH,
            "value reinterpreted, not ÷1000"
        );
    }

    #[test]
    fn test_project_missing_non_nullable_errors() {
        let b = batch(
            vec![Field::new("id", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![1]))],
        );
        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("must", DataType::Utf8, false), // non-nullable
        ]));
        assert!(project_batch_to_schema(&b, &target).is_err());
    }

    #[test]
    fn test_project_int_promotions_plain_cast() {
        let b = batch(
            vec![Field::new("v", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![7]))],
        );
        for target_type in [DataType::Int64, DataType::Float32, DataType::Float64] {
            let target: SchemaRef = Arc::new(Schema::new(vec![Field::new(
                "v",
                target_type.clone(),
                true,
            )]));
            let out = project_batch_to_schema(&b, &target).unwrap();
            assert_eq!(out.column(0).data_type(), &target_type);
        }
    }

    #[test]
    fn test_project_float_to_double_is_value_exact() {
        // Gold C6: 0.1f must become 0.1 (string-mediated), NOT 0.10000000149011612.
        let b = batch(
            vec![Field::new("v", DataType::Float32, true)],
            vec![Arc::new(Float32Array::from(vec![0.1f32]))],
        );
        let target: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        let v = out
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert_eq!(v.value(0), 0.1f64);
    }

    #[test]
    fn test_project_numeric_to_string_java_format() {
        // Java String.valueOf semantics: integral floats render "1.0"; large → "1.0E10".
        let b = batch(
            vec![
                Field::new("i", DataType::Int32, true),
                Field::new("f", DataType::Float32, true),
                Field::new("d", DataType::Float64, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![123])),
                Arc::new(Float32Array::from(vec![1.0f32])),
                Arc::new(arrow_array::Float64Array::from(vec![1.0e10f64])),
            ],
        );
        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Utf8, true),
            Field::new("f", DataType::Utf8, true),
            Field::new("d", DataType::Utf8, true),
        ]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        let col = |i: usize| {
            out.column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        assert_eq!(col(0), "123");
        assert_eq!(col(1), "1.0");
        assert_eq!(col(2), "1.0E10");
    }

    #[test]
    fn test_project_nested_struct_add_and_promote() {
        use arrow_array::StructArray;
        let inner = StructArray::from(vec![(
            Arc::new(Field::new("x", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![5])) as ArrayRef,
        )]);
        let b = batch(
            vec![Field::new("s", inner.data_type().clone(), true)],
            vec![Arc::new(inner)],
        );
        let target_inner = DataType::Struct(
            vec![
                Field::new("x", DataType::Int64, true), // promoted
                Field::new("y", DataType::Utf8, true),  // added
            ]
            .into(),
        );
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new("s", target_inner, true)]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        assert_eq!(out.schema(), target);
        let s = out
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let x = s
            .column_by_name("x")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(x.value(0), 5i64);
        let y = s.column_by_name("y").unwrap();
        assert!(y.is_null(0));
    }

    #[test]
    fn test_project_map_value_promotion() {
        use arrow_array::{Int32Array, MapArray, StringArray};
        use arrow_buffer::OffsetBuffer;
        // map<utf8, int32> with one row {"a": 1, "b": 2} → map<utf8, int64>
        let keys = StringArray::from(vec!["a", "b"]);
        let vals = Int32Array::from(vec![1, 2]);
        let entry_fields: arrow_schema::Fields = vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]
        .into();
        let entries = arrow_array::StructArray::new(
            entry_fields.clone(),
            vec![Arc::new(keys) as ArrayRef, Arc::new(vals) as ArrayRef],
            None,
        );
        let entries_field = Arc::new(Field::new("entries", DataType::Struct(entry_fields), false));
        let map = MapArray::new(
            entries_field.clone(),
            OffsetBuffer::new(vec![0, 2].into()),
            entries,
            None,
            false,
        );
        let b = batch(
            vec![Field::new("m", map.data_type().clone(), true)],
            vec![Arc::new(map)],
        );

        let target_entry_fields: arrow_schema::Fields = vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]
        .into();
        let target_entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(target_entry_fields),
            false,
        ));
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "m",
            DataType::Map(target_entries_field, false),
            true,
        )]));

        let out = project_batch_to_schema(&b, &target).unwrap();
        assert_eq!(out.schema(), target);
        let m = out.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        let ev = m
            .entries()
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(ev.value(0), 1);
        assert_eq!(ev.value(1), 2);
    }

    // --- Added cases (prompt) ---

    #[test]
    fn test_project_float32_to_string_shortest_f32_repr() {
        // Locks the format-as-f32 rule: 0.1f32 → "0.1", not "0.10000000149011612".
        let b = batch(
            vec![Field::new("f", DataType::Float32, true)],
            vec![Arc::new(Float32Array::from(vec![0.1f32]))],
        );
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new("f", DataType::Utf8, true)]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        let v = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(v.value(0), "0.1");
    }

    #[test]
    fn test_project_float64_to_string_java_boundaries() {
        let b = batch(
            vec![
                Field::new("a", DataType::Float64, true),
                Field::new("b", DataType::Float64, true),
                Field::new("c", DataType::Float64, true),
                Field::new("e", DataType::Float64, true),
            ],
            vec![
                Arc::new(arrow_array::Float64Array::from(vec![1.0e-4f64])),
                Arc::new(arrow_array::Float64Array::from(vec![0.001f64])),
                Arc::new(arrow_array::Float64Array::from(vec![9999999.0f64])),
                Arc::new(arrow_array::Float64Array::from(vec![1.0e7f64])),
            ],
        );
        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Utf8, true),
            Field::new("e", DataType::Utf8, true),
        ]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        let col = |i: usize| {
            out.column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        assert_eq!(col(0), "1.0E-4"); // < 1e-3 → scientific
        assert_eq!(col(1), "0.001"); // >= 1e-3 → decimal
        assert_eq!(col(2), "9999999.0"); // < 1e7 → decimal
        assert_eq!(col(3), "1.0E7"); // >= 1e7 → scientific
    }

    #[test]
    fn test_project_float_to_string_nan_and_infinities() {
        // Java Float/Double.toString: NaN → "NaN", +inf → "Infinity",
        // -inf → "-Infinity". Pin exact tokens for both widths (review B1).
        let b = batch(
            vec![
                Field::new("f_nan", DataType::Float32, true),
                Field::new("f_pinf", DataType::Float32, true),
                Field::new("f_ninf", DataType::Float32, true),
                Field::new("d_nan", DataType::Float64, true),
                Field::new("d_pinf", DataType::Float64, true),
                Field::new("d_ninf", DataType::Float64, true),
            ],
            vec![
                Arc::new(Float32Array::from(vec![f32::NAN])),
                Arc::new(Float32Array::from(vec![f32::INFINITY])),
                Arc::new(Float32Array::from(vec![f32::NEG_INFINITY])),
                Arc::new(arrow_array::Float64Array::from(vec![f64::NAN])),
                Arc::new(arrow_array::Float64Array::from(vec![f64::INFINITY])),
                Arc::new(arrow_array::Float64Array::from(vec![f64::NEG_INFINITY])),
            ],
        );
        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("f_nan", DataType::Utf8, true),
            Field::new("f_pinf", DataType::Utf8, true),
            Field::new("f_ninf", DataType::Utf8, true),
            Field::new("d_nan", DataType::Utf8, true),
            Field::new("d_pinf", DataType::Utf8, true),
            Field::new("d_ninf", DataType::Utf8, true),
        ]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        let col = |i: usize| {
            out.column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        assert_eq!(col(0), "NaN");
        assert_eq!(col(1), "Infinity");
        assert_eq!(col(2), "-Infinity");
        assert_eq!(col(3), "NaN");
        assert_eq!(col(4), "Infinity");
        assert_eq!(col(5), "-Infinity");
    }

    #[test]
    fn test_project_list_int_to_string_preserves_nulls() {
        use arrow_array::ListArray;
        use arrow_buffer::OffsetBuffer;
        // List with nullable Int32 elements: [1, null, 3], [4]
        let values = Int32Array::from(vec![Some(1), None, Some(3), Some(4)]);
        let offsets = OffsetBuffer::new(vec![0, 3, 4].into());
        let src_elem = Arc::new(Field::new("element", DataType::Int32, true));
        let list = ListArray::new(src_elem.clone(), offsets, Arc::new(values), None);
        let b = batch(
            vec![Field::new("l", DataType::List(src_elem.clone()), true)],
            vec![Arc::new(list)],
        );
        let target_elem = Arc::new(Field::new("item", DataType::Utf8, true));
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "l",
            DataType::List(target_elem.clone()),
            true,
        )]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        assert_eq!(out.schema(), target);
        let la = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        let vals = la.values().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(vals.value(0), "1");
        assert!(vals.is_null(1));
        assert_eq!(vals.value(2), "3");
        assert_eq!(vals.value(3), "4");
    }

    #[test]
    fn test_project_identity_returns_same_data() {
        let b = batch(
            vec![Field::new("id", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        );
        let target: SchemaRef = b.schema();
        let out = project_batch_to_schema(&b, &target).unwrap();
        assert_eq!(out.schema(), target);
        let c = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(c.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_project_string_to_bytes_and_bytes_to_string_fallback() {
        use arrow_array::BinaryArray;
        // string → bytes
        let b = batch(
            vec![Field::new("s", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec!["hello"]))],
        );
        let target: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("s", DataType::Binary, true)]));
        let out = project_batch_to_schema(&b, &target).unwrap();
        let bin = out
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(bin.value(0), b"hello");

        // bytes → string
        let b2 = batch(
            vec![Field::new("s", DataType::Binary, true)],
            vec![Arc::new(BinaryArray::from(vec![&b"world"[..]]))],
        );
        let target2: SchemaRef = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let out2 = project_batch_to_schema(&b2, &target2).unwrap();
        let s = out2
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(s.value(0), "world");
    }
}
