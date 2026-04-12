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

//! Mirrors Java's `org.apache.hudi.avro.AvroSchemaUtils`.
//!
//! Provides schema comparison utilities for checking projection equivalence
//! between Arrow schemas.

use arrow_schema::SchemaRef;

/// Check if two schemas are projection-equivalent: same field names in same order.
///
/// Mirrors Java's `AvroSchemaUtils.areSchemasProjectionEquivalent()`.
///
/// In the Arrow representation this compares field names at the top level of
/// two record schemas.  The Java implementation also recurses into nested Avro
/// types (arrays, maps, enums, fixed, logical types) — that depth is not yet
/// needed on the Rust read path because Arrow schemas always represent records
/// and the comparison is used only to decide whether a projection converter is
/// required.
pub fn are_schemas_projection_equivalent(a: &SchemaRef, b: &SchemaRef) -> bool {
    if a.fields().len() != b.fields().len() {
        return false;
    }
    a.fields()
        .iter()
        .zip(b.fields().iter())
        .all(|(fa, fb)| fa.name() == fb.name())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Helper: build an Arrow schema from `(name, DataType)` pairs.
    fn make_schema(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, dt)| Field::new(*name, dt.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    /// Helper: build a simple schema where every field is Utf8.
    fn make_simple_schema(names: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            names
                .iter()
                .map(|n| Field::new(*n, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ))
    }

    // =========================================================================
    // Ported from Java TestAvroSchemaUtils.java — record-level tests
    // =========================================================================

    /// Java: testAreSchemasProjectionEquivalentRecordSchemas
    /// Two record schemas with the same field name are projection-equivalent
    /// regardless of schema/record name (Arrow schemas have no "record name").
    #[test]
    fn test_are_schemas_projection_equivalent_record_schemas() {
        let s1 = make_schema(&[("f1", DataType::Int32)]);
        let s2 = make_schema(&[("f1", DataType::Int32)]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentDifferentFieldCountInRecords
    #[test]
    fn test_are_schemas_projection_equivalent_different_field_count_in_records() {
        let s1 = make_schema(&[("a", DataType::Int32)]);
        let s2: SchemaRef = Arc::new(Schema::empty());
        assert!(!are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentNestedRecordSchemas
    /// Nested struct fields — top-level field names match so these are equivalent.
    #[test]
    fn test_are_schemas_projection_equivalent_nested_record_schemas() {
        let inner1 = DataType::Struct(
            vec![Field::new("x", DataType::Utf8, true)].into(),
        );
        let inner2 = DataType::Struct(
            vec![Field::new("x", DataType::Utf8, true)].into(),
        );
        let s1 = make_schema(&[("inner", inner1)]);
        let s2 = make_schema(&[("inner", inner2)]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentArraySchemas
    /// Schemas with identically-named array fields.
    #[test]
    fn test_are_schemas_projection_equivalent_array_schemas() {
        let s1 = make_schema(&[(
            "arr",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        )]);
        let s2 = make_schema(&[(
            "arr",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        )]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentDifferentElementTypeInArray
    /// Same field name but different list element types.
    /// NOTE: Rust only compares field names, so this returns true (Java returns false).
    #[test]
    fn test_are_schemas_projection_equivalent_different_element_type_in_array() {
        let s1 = make_schema(&[(
            "arr",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        )]);
        let s2 = make_schema(&[(
            "arr",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        )]);
        // Name-only comparison: same field name → equivalent.
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentMapSchemas
    #[test]
    fn test_are_schemas_projection_equivalent_map_schemas() {
        let s1 = make_schema(&[(
            "m",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        )]);
        let s2 = make_schema(&[(
            "m",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        )]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentDifferentMapValueTypes
    /// Same field name but different map value types.
    /// NOTE: Rust only compares field names, so this returns true (Java returns false).
    #[test]
    fn test_are_schemas_projection_equivalent_different_map_value_types() {
        let s1 = make_schema(&[(
            "m",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        )]);
        let s2 = make_schema(&[(
            "m",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        )]);
        // Name-only comparison: same field name → equivalent.
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentNullableSchemaComparison
    /// One field nullable, the other not — same field name.
    /// NOTE: Rust only compares field names, so this returns true (Java unwraps
    /// nullable unions and then compares, also returning true).
    #[test]
    fn test_are_schemas_projection_equivalent_nullable_schema_comparison() {
        let s1 = make_schema(&[("f", DataType::Int32)]);
        // In Arrow, nullable is a field property, not a union wrapper.
        let s2 = Arc::new(Schema::new(vec![Field::new("f", DataType::Int32, false)]));
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentListVsString
    /// Same field name but List type vs String type.
    /// NOTE: Rust only compares field names, so this returns true (Java returns false).
    #[test]
    fn test_are_schemas_projection_equivalent_list_vs_string() {
        let s1 = make_schema(&[(
            "f",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        )]);
        let s2 = make_schema(&[("f", DataType::Utf8)]);
        // Name-only comparison: same field name → equivalent.
        assert!(are_schemas_projection_equivalent(&s1, &s2));
        assert!(are_schemas_projection_equivalent(&s2, &s1));
    }

    /// Java: testAreSchemasProjectionEquivalentMapVsString
    /// Same field name but Map type vs String type.
    /// NOTE: Rust only compares field names, so this returns true (Java returns false).
    #[test]
    fn test_are_schemas_projection_equivalent_map_vs_string() {
        let s1 = make_schema(&[(
            "f",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        )]);
        let s2 = make_schema(&[("f", DataType::Utf8)]);
        // Name-only comparison: same field name → equivalent.
        assert!(are_schemas_projection_equivalent(&s1, &s2));
        assert!(are_schemas_projection_equivalent(&s2, &s1));
    }

    /// Java: testAreSchemasProjectionEquivalentEqualFixedSchemas
    #[test]
    fn test_are_schemas_projection_equivalent_equal_fixed_schemas() {
        let s1 = make_schema(&[("f", DataType::FixedSizeBinary(16))]);
        let s2 = make_schema(&[("f", DataType::FixedSizeBinary(16))]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentDifferentFixedSize
    /// Same field name but different FixedSizeBinary sizes.
    /// NOTE: Rust only compares field names, so this returns true (Java returns false).
    #[test]
    fn test_are_schemas_projection_equivalent_different_fixed_size() {
        let s1 = make_schema(&[("f", DataType::FixedSizeBinary(8))]);
        let s2 = make_schema(&[("f", DataType::FixedSizeBinary(4))]);
        // Name-only comparison: same field name → equivalent.
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentEnums
    /// Arrow uses Dictionary encoding as the closest analog to Avro enums.
    #[test]
    fn test_are_schemas_projection_equivalent_enums() {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let s1 = make_schema(&[("e", dict_type.clone())]);
        let s2 = make_schema(&[("e", dict_type)]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentDifferentEnumSymbols
    /// NOTE: Rust only compares field names, so this returns true (Java returns false).
    #[test]
    fn test_are_schemas_projection_equivalent_different_enum_symbols() {
        let s1 = make_schema(&[(
            "e",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        )]);
        let s2 = make_schema(&[(
            "e",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        )]);
        // Name-only comparison: same field name → equivalent.
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentEnumSymbolSubset
    /// Avro allows the first enum to be a subset of the second's symbols.
    /// In Arrow there is no direct analog; we test with the same field name.
    /// NOTE: Rust only compares field names, so this returns true.
    #[test]
    fn test_are_schemas_projection_equivalent_enum_symbol_subset() {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let s1 = make_schema(&[("e", dict_type.clone())]);
        let s2 = make_schema(&[("e", dict_type)]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
        assert!(are_schemas_projection_equivalent(&s2, &s1));
    }

    /// Java: testAreSchemasProjectionEquivalentEqualDecimalLogicalTypes
    #[test]
    fn test_are_schemas_projection_equivalent_equal_decimal_logical_types() {
        let s1 = make_schema(&[("d", DataType::Decimal128(12, 2))]);
        let s2 = make_schema(&[("d", DataType::Decimal128(12, 2))]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentDifferentPrecision
    /// Same field name but different Decimal precision.
    /// NOTE: Rust only compares field names, so this returns true (Java returns false).
    #[test]
    fn test_are_schemas_projection_equivalent_different_precision() {
        let s1 = make_schema(&[("d", DataType::Decimal128(12, 2))]);
        let s2 = make_schema(&[("d", DataType::Decimal128(13, 2))]);
        // Name-only comparison: same field name → equivalent.
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentLogicalVsNoLogicalType
    /// Decimal field vs plain Binary field — same field name.
    /// NOTE: Rust only compares field names, so this returns true (Java returns false).
    #[test]
    fn test_are_schemas_projection_equivalent_logical_vs_no_logical_type() {
        let s1 = make_schema(&[("d", DataType::Decimal128(10, 2))]);
        let s2 = make_schema(&[("d", DataType::Binary)]);
        // Name-only comparison: same field name → equivalent.
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentSameReferenceSchema
    #[test]
    fn test_are_schemas_projection_equivalent_same_reference_schema() {
        let s = make_simple_schema(&["f"]);
        assert!(are_schemas_projection_equivalent(&s, &s));
    }

    /// Java: testAreSchemasProjectionEquivalentNullSchemaComparison
    /// Rust uses references so null is not applicable.  We test empty schemas
    /// and schemas with different names instead to cover the boundary case.
    #[test]
    fn test_are_schemas_projection_equivalent_empty_schemas() {
        let s1: SchemaRef = Arc::new(Schema::empty());
        let s2: SchemaRef = Arc::new(Schema::empty());
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    // =========================================================================
    // Additional edge-case tests (no direct Java equivalent)
    // =========================================================================

    #[test]
    fn test_are_schemas_projection_equivalent_different_field_names() {
        let s1 = make_simple_schema(&["a"]);
        let s2 = make_simple_schema(&["b"]);
        assert!(!are_schemas_projection_equivalent(&s1, &s2));
    }

    #[test]
    fn test_are_schemas_projection_equivalent_field_order_matters() {
        let s1 = make_simple_schema(&["a", "b"]);
        let s2 = make_simple_schema(&["b", "a"]);
        assert!(!are_schemas_projection_equivalent(&s1, &s2));
    }
}
