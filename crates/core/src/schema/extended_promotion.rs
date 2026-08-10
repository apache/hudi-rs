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

//! Port of gold `HoodieAvroUtils.recordNeedsRewriteForExtendedAvroTypePromotion`
//! and `needsRewriteToString` (hudi-internal `HoodieAvroUtils.java:1451-1509`),
//! operating on `apache_avro::Schema`.
//!
//! This decides, PER LOG BLOCK, whether the writer→reader schema evolution exceeds
//! what Avro's own schema-resolution can perform:
//!
//! * `true`  → caller must decode the block writer-only and batch-rewrite the records into the reader schema (gold `rewriteRecordWithNewSchema`).
//! * `false` → caller can lean on Avro resolution (`GenericDatumReader(writer, reader)` / arrow-avro `with_reader_schema`).
//!
//! It is NOT a compatibility check; it only answers "does the reader expect a
//! promotion Avro-resolution cannot do (e.g. number→string, float→double)?".
//!
//! ## Bug-for-bug parity with gold
//! Several arms look wrong but are preserved deliberately because the caller's
//! branch decision was tuned against gold's exact behavior. Changing them here
//! would silently diverge from the Java write/read path:
//!
//! * reader RECORD with MORE fields than writer → `true` (add-column routes to the rewrite path, NOT Avro resolution) — gold line 1461.
//! * reader ARRAY with a non-ARRAY writer → `false` — gold line 1475.
//! * reader MAP with a non-MAP writer → `false` — gold line 1480.
//! * `needsRewriteToString` returns `true` for a BYTES writer feeding a STRING reader even though the Avro spec supports bytes↔string — gold line 1508.
//!
//! ## Union handling — keyed on the reader, throws on malformed
//! Like gold, the UNION case is keyed on the READER type (gold line 1482): only when
//! the reader is a union are both sides unwrapped via [`actual_schema_from_union`]
//! (gold `getActualSchemaFromUnion(.., null)`). A writer union feeding a *plain*
//! reader is therefore NOT unwrapped — `physical_type` reports it as `UNION`, so a
//! `[null,int]` writer → plain `long` reader rewrites, matching gold's
//! `writer.getType() == UNION` check. Malformed unions (≥3 branches, or two non-null
//! branches) return `Err`, mirroring gold's `HoodieAvroSchemaException`.
//!
//! ## Recursive schemas (`Schema::Ref`)
//! apache-avro parses a self-reference as a `Schema::Ref { name }` leaf (compared by
//! name), so recursion terminates and same-named recursive types match gold. Gold
//! instead recurses the referenced RECORD structurally with cycle detection, so a
//! RENAMED or partially-INLINED recursive type could diverge — see
//! `test_recursive_schema_terminates_and_matches_gold`.
//!
//! ## apache-avro 0.21 logical-type modeling
//! Gold checks `readerSchema.getLogicalType() != null` (line 1455) BEFORE the type
//! switch. In apache-avro, logical types are distinct `Schema` variants (e.g.
//! `Schema::TimestampMicros`, `Schema::Date`, `Schema::Decimal(..)`) rather than an
//! annotation on a base type, so we mirror the early check by explicitly matching
//! those variants and returning logical-types-not-equal.

use crate::Result;
use crate::error::CoreError;
use apache_avro::schema::Schema;

/// Resolve a union to its single concrete branch, mirroring gold
/// `getActualSchemaFromUnion(schema, null)` (`HoodieAvroUtils.java:1539`).
///
/// Gold accepts only `[null, X]`, `[X, null]`, and single-element `[X]` unions when
/// the datum is `null`; ANY other union shape (≥3 branches, or two non-null
/// branches) throws `HoodieAvroSchemaException("Union is malformed")` because it
/// cannot pick a branch without a concrete datum. The detector always calls this
/// with `data == null`, so we mirror that throw with an `Err` rather than silently
/// picking the first non-null branch — a malformed union must fail loudly and
/// identically to gold. A non-union schema is returned unchanged.
fn actual_schema_from_union(s: &Schema) -> Result<&Schema> {
    let Schema::Union(u) = s else {
        return Ok(s);
    };
    let variants = u.variants();
    match variants.len() {
        2 if matches!(variants[0], Schema::Null) => Ok(&variants[1]),
        2 if matches!(variants[1], Schema::Null) => Ok(&variants[0]),
        1 => Ok(&variants[0]),
        _ => Err(CoreError::Schema(format!("Union is malformed: {s:?}"))),
    }
}

/// Gold uses `Schema.equals` (structural equality), which COMPARES logical types
/// and record FIELD NAMES.
///
/// We must NOT compare via Parsing Canonical Form here: apache-avro's
/// `canonical_form()` STRIPS `logicalType` attributes (see apache-avro `schema.rs`
/// `parsing_canonical_form`, which emits only the base `type`). So two schemas with
/// the same backing primitive but different logical types — e.g. time-millis(int)
/// vs date(int), or timestamp-millis(long) vs timestamp-micros(long) — produce
/// IDENTICAL canonical forms and would wrongly compare EQUAL, even nested inside a
/// record (the record's canonical form strips the inner field logical types too).
/// That would make this detector early-return `false` and skip the rewrite, whereas
/// gold's `Schema.equals` treats those pairs as NOT equal, letting them fall through
/// to the reader-has-logical-type check (gold line 1455) which returns `true`.
///
/// We must ALSO NOT use apache-avro's `PartialEq` (`==`) directly for records:
/// its `StructFieldEq::compare_fields` zips fields and compares each field's
/// SCHEMA only — field names are ignored. A column rename (`{a:int}` → `{b:int}`)
/// would compare EQUAL and early-return `false`, whereas gold's `Field.equals`
/// compares names, falls through to the RECORD arm, finds the renamed field
/// missing from the writer, and returns `true` (rewrite). So records, arrays,
/// maps, and unions recurse here with an explicit field-name check; leaf types
/// (primitives and logical variants, where `PartialEq` compares each logical
/// variant and Decimal precision/scale as gold does) delegate to `==`.
///
/// Record names and field defaults (which gold's `equals` also compares) are
/// intentionally NOT compared: a difference there only shifts which path returns
/// the result (early-equal here vs the RECORD arm recursing to the same per-field
/// comparisons), never the final boolean.
fn schemas_equal(a: &Schema, b: &Schema) -> bool {
    match (a, b) {
        (Schema::Record(ra), Schema::Record(rb)) => {
            ra.fields.len() == rb.fields.len()
                && ra
                    .fields
                    .iter()
                    .zip(rb.fields.iter())
                    .all(|(fa, fb)| fa.name == fb.name && schemas_equal(&fa.schema, &fb.schema))
        }
        (Schema::Array(x), Schema::Array(y)) => schemas_equal(&x.items, &y.items),
        (Schema::Map(x), Schema::Map(y)) => schemas_equal(&x.types, &y.types),
        (Schema::Union(x), Schema::Union(y)) => {
            x.variants().len() == y.variants().len()
                && x.variants()
                    .iter()
                    .zip(y.variants().iter())
                    .all(|(s, t)| schemas_equal(s, t))
        }
        _ => a == b,
    }
}

/// `true` if this variant is one of apache-avro's logical-type variants, i.e. the
/// cases where gold's `getLogicalType() != null` would have fired (line 1455).
fn is_logical_type(s: &Schema) -> bool {
    matches!(
        s,
        Schema::Decimal(_)
            | Schema::BigDecimal
            | Schema::Uuid
            | Schema::Date
            | Schema::TimeMillis
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos
            | Schema::Duration
    )
}

/// The Avro base (physical) type underlying a (possibly logical) schema, mirroring
/// gold's `Schema.getType()`. Logical types in gold report their backing primitive
/// (e.g. timestamp-micros → LONG, date → INT, decimal → BYTES or FIXED, duration →
/// FIXED), so both the `LONG`/`FLOAT`/`DOUBLE` reader arm (gold line 1490) and the
/// default arm (gold line 1492, `!writer.getType().equals(reader.getType())`)
/// compare against this base. apache-avro models logical types as distinct variants,
/// so we recover the backing physical type here to keep parity in both arms.
fn physical_type(s: &Schema) -> PhysicalType {
    match s {
        Schema::Null => PhysicalType::Null,
        Schema::Boolean => PhysicalType::Boolean,
        Schema::Int | Schema::Date | Schema::TimeMillis => PhysicalType::Int,
        Schema::Long
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => PhysicalType::Long,
        Schema::Float => PhysicalType::Float,
        Schema::Double => PhysicalType::Double,
        // Decimal reports its backing primitive (bytes or fixed), exactly as gold.
        Schema::Decimal(d) => physical_type(&d.inner),
        Schema::Bytes | Schema::BigDecimal => PhysicalType::Bytes,
        Schema::String | Schema::Uuid => PhysicalType::String,
        Schema::Fixed(_) | Schema::Duration => PhysicalType::Fixed,
        Schema::Record(_) => PhysicalType::Record,
        Schema::Enum(_) => PhysicalType::Enum,
        Schema::Array(_) => PhysicalType::Array,
        Schema::Map(_) => PhysicalType::Map,
        Schema::Union(_) => PhysicalType::Union,
        Schema::Ref { .. } => PhysicalType::Ref,
    }
}

#[derive(PartialEq)]
enum PhysicalType {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Fixed,
    Record,
    Enum,
    Array,
    Map,
    Union,
    Ref,
}

/// Gold `needsRewriteToString` (`HoodieAvroUtils.java:1501-1509`). Returns `true`
/// for any writer feeding a STRING/ENUM reader, EXCEPT enum→enum.
/// - writer with a logical type → `true` (line 1502-1504).
/// - writer ENUM → `!reader_is_enum` (line 1505-1507).
/// - otherwise → `true` (line 1508): int/long/float/double/bytes→string all rewrite.
fn needs_rewrite_to_string(writer: &Schema, reader_is_enum: bool) -> bool {
    if is_logical_type(writer) {
        return true;
    }
    if let Schema::Enum(_) = writer {
        return !reader_is_enum;
    }
    true
}

/// See module docs. Port of gold `recordNeedsRewriteForExtendedAvroTypePromotion`
/// (`HoodieAvroUtils.java:1451-1494`).
///
/// Returns `Err` for a malformed union (see [`actual_schema_from_union`]), exactly
/// where gold throws `HoodieAvroSchemaException`.
pub fn record_needs_rewrite_for_extended_promotion(
    writer: &Schema,
    reader: &Schema,
) -> Result<bool> {
    // Gold line 1452: equal schemas → resolution path. Compared on the RAW schemas
    // (incl. unbroken unions) just like gold, BEFORE any union unwrap.
    if schemas_equal(writer, reader) {
        return Ok(false);
    }

    // Gold line 1455-1458: reader has a logical type → rewrite unless the writer
    // has the identical logical type. With structural equality already handled
    // above, an unequal reader logical type lands here. We compare variants
    // (Decimal compares precision/scale via PartialEq).
    if is_logical_type(reader) {
        return Ok(!logical_types_equal(writer, reader));
    }

    match reader {
        // Gold case RECORD (lines 1460-1470).
        Schema::Record(rrec) => match writer {
            Schema::Record(wrec) => {
                // Gold line 1461: reader with MORE fields → rewrite (add-column).
                if rrec.fields.len() > wrec.fields.len() {
                    return Ok(true);
                }
                // Gold lines 1464-1469: any reader field missing from writer, or
                // any field needing rewrite recursively → rewrite.
                for rf in &rrec.fields {
                    match wrec.fields.iter().find(|wf| wf.name == rf.name) {
                        None => return Ok(true),
                        Some(wf) => {
                            if record_needs_rewrite_for_extended_promotion(&wf.schema, &rf.schema)?
                            {
                                return Ok(true);
                            }
                        }
                    }
                }
                Ok(false)
            }
            // Reader RECORD, writer not RECORD: gold calls writerSchema.getFields()
            // on the non-record, which throws AvroRuntimeException("Not a record").
            // Mirror the throw rather than guessing a boolean.
            _ => Err(CoreError::Schema(format!(
                "Not a record: {writer:?} (reader expects a record)"
            ))),
        },
        // Gold case ARRAY (lines 1471-1475).
        Schema::Array(relem) => match writer {
            Schema::Array(welem) => {
                record_needs_rewrite_for_extended_promotion(&welem.items, &relem.items)
            }
            // Gold line 1475 quirk: reader ARRAY but writer not ARRAY → false.
            _ => Ok(false),
        },
        // Gold case MAP (lines 1476-1480).
        Schema::Map(rval) => match writer {
            Schema::Map(wval) => {
                record_needs_rewrite_for_extended_promotion(&wval.types, &rval.types)
            }
            // Gold line 1480 quirk: reader MAP but writer not MAP → false.
            _ => Ok(false),
        },
        // Gold case UNION (line 1482): unwrap BOTH sides via getActualSchemaFromUnion
        // and recurse. Keyed on the READER being a union — this is why we do NOT
        // unwrap up-front: when the writer is a union but the reader is a plain type,
        // gold leaves the writer as a union and compares `writer.getType() == UNION`
        // in the arm below (so writer `[null,int]` → plain reader `long` rewrites,
        // because UNION ∉ {INT, LONG}). `physical_type(Union)` preserves that.
        Schema::Union(_) => {
            let w = actual_schema_from_union(writer)?;
            let r = actual_schema_from_union(reader)?;
            record_needs_rewrite_for_extended_promotion(w, r)
        }
        // Gold case ENUM (line 1483-1484).
        Schema::Enum(_) => Ok(needs_rewrite_to_string(writer, true)),
        // Gold case STRING (line 1485-1486).
        Schema::String => Ok(needs_rewrite_to_string(writer, false)),
        // Gold cases DOUBLE/FLOAT/LONG (lines 1487-1490): rewrite UNLESS writer's
        // base type is INT or LONG. So int→{long,float,double} and long→{float,
        // double} are resolution; float→double is rewrite. A union writer reports
        // physical type UNION here (gold `writer.getType()`), so it rewrites.
        Schema::Double | Schema::Float | Schema::Long => Ok(!matches!(
            physical_type(writer),
            PhysicalType::Int | PhysicalType::Long
        )),
        // Gold default (lines 1491-1492): rewrite iff base (physical) types differ,
        // mirroring `!writerSchema.getType().equals(readerSchema.getType())`. A
        // logical writer reports its backing primitive here, so e.g. date(int)→plain
        // int or decimal(bytes)→plain bytes is NOT a rewrite, matching gold; comparing
        // apache-avro's distinct logical variants by discriminant would wrongly
        // over-trigger. string→bytes lands here too (types differ → rewrite).
        _ => Ok(physical_type(writer) != physical_type(reader)),
    }
}

/// Mirror gold's `readerLogical.equals(writerLogical)` (line 1457). Two schemas have
/// equal logical types iff they are the same logical variant (with matching
/// precision/scale for Decimal). A non-logical writer has a `null` logical type in
/// gold, so it is never equal to a logical reader.
fn logical_types_equal(writer: &Schema, reader: &Schema) -> bool {
    if !is_logical_type(writer) {
        return false;
    }
    // Decimal must match precision and scale; apache-avro's PartialEq (used by
    // schemas_equal) compares them directly, and for the other logical variants
    // matching variant == matching logical type.
    schemas_equal(writer, reader)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::Schema as AvroSchema;

    fn rec(fields: &str) -> AvroSchema {
        AvroSchema::parse_str(&format!(
            r#"{{"type":"record","name":"r","fields":[{fields}]}}"#
        ))
        .unwrap()
    }

    #[test]
    fn test_detector_matches_gold_matrix() {
        // (writer fields, reader fields, expect_rewrite)
        let cases = vec![
            // identical → resolution path (gold line 1452)
            (
                r#"{"name":"a","type":"int"}"#,
                r#"{"name":"a","type":"int"}"#,
                false,
            ),
            // spec promotions int→long/float/double → resolution path (gold line 1490)
            (
                r#"{"name":"a","type":"int"}"#,
                r#"{"name":"a","type":"long"}"#,
                false,
            ),
            (
                r#"{"name":"a","type":"int"}"#,
                r#"{"name":"a","type":"float"}"#,
                false,
            ),
            (
                r#"{"name":"a","type":"int"}"#,
                r#"{"name":"a","type":"double"}"#,
                false,
            ),
            (
                r#"{"name":"a","type":"long"}"#,
                r#"{"name":"a","type":"double"}"#,
                false,
            ),
            // float→double → REWRITE (gold line 1490: writer not in {INT,LONG})
            (
                r#"{"name":"a","type":"float"}"#,
                r#"{"name":"a","type":"double"}"#,
                true,
            ),
            // x→string → REWRITE (gold line 1486 → needsRewriteToString true)
            (
                r#"{"name":"a","type":"int"}"#,
                r#"{"name":"a","type":"string"}"#,
                true,
            ),
            (
                r#"{"name":"a","type":"long"}"#,
                r#"{"name":"a","type":"string"}"#,
                true,
            ),
            (
                r#"{"name":"a","type":"float"}"#,
                r#"{"name":"a","type":"string"}"#,
                true,
            ),
            // bytes→string → REWRITE (gold needsRewriteToString line 1508 returns true for BYTES)
            (
                r#"{"name":"a","type":"bytes"}"#,
                r#"{"name":"a","type":"string"}"#,
                true,
            ),
            // string→bytes → REWRITE (gold default branch line 1492: type mismatch)
            (
                r#"{"name":"a","type":"string"}"#,
                r#"{"name":"a","type":"bytes"}"#,
                true,
            ),
            // add column (reader has more fields) → REWRITE (gold line 1461)
            (
                r#"{"name":"a","type":"int"}"#,
                r#"{"name":"a","type":"int"},{"name":"b","type":["null","string"],"default":null}"#,
                true,
            ),
            // projection (reader subset, same types) → resolution path (gold lines 1461-1470 fall through)
            (
                r#"{"name":"a","type":"int"},{"name":"b","type":"string"}"#,
                r#"{"name":"a","type":"int"}"#,
                false,
            ),
            // nullable union promotion int→long → resolution path (gold union unwrap line 1482)
            (
                r#"{"name":"a","type":["null","int"],"default":null}"#,
                r#"{"name":"a","type":["null","long"],"default":null}"#,
                false,
            ),
            // nested array element int→string → REWRITE (gold lines 1471-1473 recurse → string)
            (
                r#"{"name":"a","type":{"type":"array","items":"int"}}"#,
                r#"{"name":"a","type":{"type":"array","items":"string"}}"#,
                true,
            ),
            // nested map value int→long → resolution path (gold lines 1476-1478 recurse → long arm false)
            (
                r#"{"name":"a","type":{"type":"map","values":"int"}}"#,
                r#"{"name":"a","type":{"type":"map","values":"long"}}"#,
                false,
            ),
            // same-primitive, different logical type → reader-logical check → TRUE
            // (gold 1455-1458; canonical_form strips logicalType so naive equality
            //  would wrongly early-return false — regression guard)
            (
                r#"{"name":"a","type":{"type":"int","logicalType":"time-millis"}}"#,
                r#"{"name":"a","type":{"type":"int","logicalType":"date"}}"#,
                true,
            ),
            (
                r#"{"name":"a","type":{"type":"long","logicalType":"timestamp-millis"}}"#,
                r#"{"name":"a","type":{"type":"long","logicalType":"timestamp-micros"}}"#,
                true,
            ),
            // identical logical types → equal → false (gold 1452)
            (
                r#"{"name":"a","type":{"type":"long","logicalType":"timestamp-micros"}}"#,
                r#"{"name":"a","type":{"type":"long","logicalType":"timestamp-micros"}}"#,
                false,
            ),
            // --- extra cases beyond the plan's list ---
            // writer timestamp-micros long vs reader PLAIN long:
            // gold line 1455: reader plain long has getLogicalType() == null, so skip
            // the logical early-return. reader type LONG (line 1489) →
            // !(writer.getType() in {INT,LONG}). writer's backing type is LONG →
            // returns !true = FALSE. (apache-avro models timestamp-micros as a
            // distinct variant, so physical_type() recovers LONG to preserve this.)
            (
                r#"{"name":"a","type":{"type":"long","logicalType":"timestamp-micros"}}"#,
                r#"{"name":"a","type":"long"}"#,
                false,
            ),
            // reader timestamp-micros vs writer PLAIN long:
            // gold line 1455-1457: reader has a logical type, writer's logical type is
            // null → readerLogical.equals(null) is false → return true.
            (
                r#"{"name":"a","type":"long"}"#,
                r#"{"name":"a","type":{"type":"long","logicalType":"timestamp-micros"}}"#,
                true,
            ),
            // nested record inside record: writer {s:{x:int}} reader {s:{x:long}} →
            // false (gold recurses RECORD → x int→long → line 1490 false).
            (
                r#"{"name":"s","type":{"type":"record","name":"s","fields":[{"name":"x","type":"int"}]}}"#,
                r#"{"name":"s","type":{"type":"record","name":"s","fields":[{"name":"x","type":"long"}]}}"#,
                false,
            ),
            // nested record add: writer {s:{x:int}} reader {s:{x:int,y:string?}} →
            // true (gold inner RECORD reader has more fields → line 1461).
            (
                r#"{"name":"s","type":{"type":"record","name":"s","fields":[{"name":"x","type":"int"}]}}"#,
                r#"{"name":"s","type":{"type":"record","name":"s","fields":[{"name":"x","type":"int"},{"name":"y","type":["null","string"],"default":null}]}}"#,
                true,
            ),
            // --- default arm: logical writer vs plain reader of the SAME backing type ---
            // gold line 1492 compares getType() (the backing primitive), so a logical
            // writer narrowing to its own plain backing type is NOT a rewrite. Comparing
            // apache-avro's distinct logical variants by discriminant would wrongly
            // return true; physical_type() recovers the backing type to match gold.
            // date(int) → plain int → resolution path.
            (
                r#"{"name":"a","type":{"type":"int","logicalType":"date"}}"#,
                r#"{"name":"a","type":"int"}"#,
                false,
            ),
            // time-millis(int) → plain int → resolution path.
            (
                r#"{"name":"a","type":{"type":"int","logicalType":"time-millis"}}"#,
                r#"{"name":"a","type":"int"}"#,
                false,
            ),
            // decimal(bytes) → plain bytes → resolution path.
            (
                r#"{"name":"a","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}"#,
                r#"{"name":"a","type":"bytes"}"#,
                false,
            ),
            // sanity: genuinely different physical types still rewrite (int → bytes).
            (
                r#"{"name":"a","type":"int"}"#,
                r#"{"name":"a","type":"bytes"}"#,
                true,
            ),
            // --- column rename: positionally identical, names differ ---
            // gold's Schema.equals compares Field.name → not equal → RECORD arm →
            // renamed field missing from writer → REWRITE. apache-avro's PartialEq
            // ignores field names (StructFieldEq zips field SCHEMAS only), so a raw
            // `==` would wrongly early-return false; schemas_equal compares names.
            (
                r#"{"name":"a","type":"int"}"#,
                r#"{"name":"b","type":"int"}"#,
                true,
            ),
            // nested rename: writer {s:{x:int}} reader {s:{y:int}} → REWRITE.
            (
                r#"{"name":"s","type":{"type":"record","name":"s","fields":[{"name":"x","type":"int"}]}}"#,
                r#"{"name":"s","type":{"type":"record","name":"s","fields":[{"name":"y","type":"int"}]}}"#,
                true,
            ),
        ];
        for (w, r, expect) in cases {
            let got = record_needs_rewrite_for_extended_promotion(&rec(w), &rec(r)).unwrap();
            assert_eq!(got, expect, "writer=[{w}] reader=[{r}]");
        }
    }

    /// Reader RECORD with a non-record writer: gold's `writerSchema.getFields()`
    /// throws `AvroRuntimeException("Not a record")`; we mirror with `Err`.
    /// Covers the nullable-to-required tightening shape `[null,Rec]` → `Rec`
    /// (the union writer is NOT pre-unwrapped) and a plain-primitive writer.
    #[test]
    fn test_reader_record_non_record_writer_errors_like_gold() {
        let w = rec(
            r#"{"name":"s","type":["null",{"type":"record","name":"inner","fields":[{"name":"x","type":"int"}]}],"default":null}"#,
        );
        let r = rec(
            r#"{"name":"s","type":{"type":"record","name":"inner","fields":[{"name":"x","type":"int"}]}}"#,
        );
        assert!(record_needs_rewrite_for_extended_promotion(&w, &r).is_err());

        let w = rec(r#"{"name":"s","type":"string"}"#);
        let r = rec(
            r#"{"name":"s","type":{"type":"record","name":"inner","fields":[{"name":"x","type":"int"}]}}"#,
        );
        assert!(record_needs_rewrite_for_extended_promotion(&w, &r).is_err());
    }

    /// Gold keys the UNION case on the READER type. When the writer is a nullable
    /// union but the reader is a PLAIN numeric, gold leaves the writer un-unwrapped
    /// and reaches the LONG/FLOAT/DOUBLE arm, where `writerSchema.getType() == UNION`
    /// (∉ {INT, LONG}) → rewrite. We must NOT pre-unwrap the writer; `physical_type`
    /// maps a union to `PhysicalType::Union` so the arm rewrites, matching gold.
    #[test]
    fn test_writer_union_vs_plain_numeric_matches_gold() {
        // writer [null,int] → plain long: REWRITE (gold: writer.getType()==UNION).
        let w = rec(r#"{"name":"a","type":["null","int"],"default":null}"#);
        let r = rec(r#"{"name":"a","type":"long"}"#);
        assert!(record_needs_rewrite_for_extended_promotion(&w, &r).unwrap());

        // Symmetric direction (reader is the union) still routes through the UNION
        // arm and recurses long→int → REWRITE, unchanged by this fix.
        let w = rec(r#"{"name":"a","type":"long"}"#);
        let r = rec(r#"{"name":"a","type":["null","int"],"default":null}"#);
        assert!(record_needs_rewrite_for_extended_promotion(&w, &r).unwrap());
    }

    /// Gold's `getActualSchemaFromUnion(schema, null)` throws `HoodieAvroSchemaException`
    /// for any union that is not `[null,X]` / `[X,null]` / `[X]`. We mirror that with
    /// an `Err` instead of silently comparing only the first non-null branch.
    #[test]
    fn test_malformed_union_errors_like_gold() {
        // ≥3 branches on the reader side.
        let w = rec(r#"{"name":"a","type":["null","int","string"],"default":null}"#);
        let r = rec(r#"{"name":"a","type":["null","string","int"]}"#);
        assert!(record_needs_rewrite_for_extended_promotion(&w, &r).is_err());

        // Two NON-null branches (no null member).
        let w = rec(r#"{"name":"a","type":["int","string"]}"#);
        let r = rec(r#"{"name":"a","type":["string","int"]}"#);
        assert!(record_needs_rewrite_for_extended_promotion(&w, &r).is_err());

        // A nullable `[null,X]` field next to a malformed one still surfaces the Err
        // (recursion propagates it), proving we don't silently swallow it.
        let w = rec(
            r#"{"name":"ok","type":["null","int"],"default":null},{"name":"bad","type":["null","int","long"],"default":null}"#,
        );
        let r = rec(
            r#"{"name":"ok","type":["null","long"],"default":null},{"name":"bad","type":["null","long","int"]}"#,
        );
        assert!(record_needs_rewrite_for_extended_promotion(&w, &r).is_err());
    }

    /// Recursive (self-referential) schemas. apache-avro parses the self-reference as
    /// a `Schema::Ref { name }` leaf, so the detector's recursion bottoms out there
    /// instead of looping forever. For the common case — the SAME recursive type on
    /// both sides — Rust agrees with gold: here only the non-recursive `value` field
    /// changes (int→long, Avro-resolvable) and the recursive `next` field is identical,
    /// so → no rewrite, and crucially the call terminates.
    ///
    /// GOLD-DIVERGENCE NOTE: gold holds a *cyclic* `Schema` object and, on hitting the
    /// recursive field, recurses into the referenced RECORD structurally (Avro's
    /// `Schema.equals` uses a seen-set to break the cycle). apache-avro instead leaves
    /// a `Schema::Ref` leaf that is compared by NAME only (`schema_equality.rs`) and
    /// reaches the default arm as `PhysicalType::Ref`. So if a recursive type were
    /// RENAMED or partially INLINED between writer and reader (a `Ref` on one side vs
    /// an expanded record on the other, or two refs of different names), Rust and gold
    /// could disagree. Such shapes do not arise from normal Hudi schema evolution, so
    /// this is documented rather than reconciled.
    #[test]
    fn test_recursive_schema_terminates_and_matches_gold() {
        let w = AvroSchema::parse_str(
            r#"{"type":"record","name":"Node","fields":[
                {"name":"value","type":"int"},
                {"name":"next","type":["null","Node"],"default":null}]}"#,
        )
        .unwrap();
        let r = AvroSchema::parse_str(
            r#"{"type":"record","name":"Node","fields":[
                {"name":"value","type":"long"},
                {"name":"next","type":["null","Node"],"default":null}]}"#,
        )
        .unwrap();
        // Terminates (no stack overflow) and matches gold: value int→long resolves,
        // recursive `next` unchanged → no rewrite.
        assert!(!record_needs_rewrite_for_extended_promotion(&w, &r).unwrap());
    }
}
