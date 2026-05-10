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

//! Mirrors Java's `Option<UnaryOperator<T>> outputConverter` field
//! on `HoodieFileGroupReader`.
//!
//! The output converter applies a final transformation (typically schema
//! projection) to each record returned by the reader. In Java it is
//! obtained from `readerContext.getSchemaHandler().getOutputConverter()`.

use crate::Result;
use crate::error::CoreError;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, StructArray};
use arrow_schema::{DataType, Field, SchemaRef};
use std::sync::Arc;

/// A converter that transforms output records (e.g., schema projection).
///
/// Mirrors Java's `UnaryOperator<T>` used as `outputConverter` in
/// `HoodieFileGroupReader`. Obtained from `FileGroupReaderSchemaHandler`.
pub trait OutputConverter: Send + Sync + std::fmt::Debug {
    /// Apply the conversion to a record batch.
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch>;
}

/// Projects a `RecordBatch` from `required_schema` down to `requested_schema`.
///
/// Mirrors Java's `readerContext.getRecordContext().projectRecord(requiredSchema, requestedSchema)`.
/// Selects only the columns present in the target schema, in the order they
/// appear, **including narrowing nested struct fields** when the target's
/// struct has fewer (or reordered) subfields than the source's.
///
/// Without nested narrowing, downstream consumers that expect a pruned struct
/// (e.g. Velox's `RowVector` ctor type check at `ComplexVector.h:62`) reject
/// the record because the child's type doesn't match the declared parent
/// schema (ENG-40168).
///
/// Currently handles:
///   - top-level reorder / drop / passthrough (as before)
///   - nested struct narrowing: `fare<amount,currency>` → `fare<currency>`
///   - nested struct reorder: `fare<amount,currency>` → `fare<currency,amount>`
///
/// Does NOT yet handle narrowing of struct elements inside `Array` or `Map`.
/// If the test set ever exercises that path, expand `narrow_array_to_field`.
#[derive(Debug)]
pub struct ProjectionConverter {
    target_schema: SchemaRef,
}

impl ProjectionConverter {
    pub fn new(target_schema: &SchemaRef) -> Self {
        Self {
            target_schema: target_schema.clone(),
        }
    }
}

/// Narrow a single source array to match `target_field`'s type.
///
/// - Identical types → cheap clone of the Arc.
/// - Both `Struct` → recursively narrow each requested subfield by name.
///   Subfield order follows `target_field`'s order (so reordering also works).
/// - Anything else → `Err`.  Arrow's `RecordBatch::try_new` would catch a
///   primitive mismatch later, but raising here gives a column-name-aware
///   error message.
fn narrow_array_to_field(
    source: &ArrayRef,
    source_field: &Field,
    target_field: &Field,
) -> Result<ArrayRef> {
    if source_field.data_type() == target_field.data_type() {
        return Ok(Arc::clone(source));
    }

    match (source_field.data_type(), target_field.data_type()) {
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            let source_struct: &StructArray = source.as_struct();
            let mut narrowed_children: Vec<ArrayRef> =
                Vec::with_capacity(target_fields.len());
            for tgt_sub in target_fields.iter() {
                // Locate the matching subfield in the source struct by name.
                let (idx, src_sub) = source_fields
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.name() == tgt_sub.name())
                    .ok_or_else(|| {
                        CoreError::ReadFileSliceError(format!(
                            "Output projection: subfield '{}' not found in struct '{}'. \
                             Available: [{}]",
                            tgt_sub.name(),
                            source_field.name(),
                            source_fields
                                .iter()
                                .map(|f| f.name().as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        ))
                    })?;
                let src_child = source_struct.column(idx);
                narrowed_children.push(narrow_array_to_field(
                    src_child, src_sub, tgt_sub,
                )?);
            }
            // Preserve the source struct's parent null buffer.
            let nulls = source_struct.nulls().cloned();
            let narrowed = StructArray::try_new(
                target_fields.clone(),
                narrowed_children,
                nulls,
            )
            .map_err(|e| {
                CoreError::ReadFileSliceError(format!(
                    "Output projection: failed to build narrowed struct '{}': {e}",
                    source_field.name()
                ))
            })?;
            Ok(Arc::new(narrowed))
        }
        _ => Err(CoreError::ReadFileSliceError(format!(
            "Output projection: incompatible types for column '{}': source={:?}, target={:?}",
            source_field.name(),
            source_field.data_type(),
            target_field.data_type()
        ))),
    }
}

impl OutputConverter for ProjectionConverter {
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let source_schema = batch.schema();
        let mut narrowed_columns: Vec<ArrayRef> =
            Vec::with_capacity(self.target_schema.fields().len());

        for tgt in self.target_schema.fields().iter() {
            let (idx, src_field) = source_schema
                .fields()
                .iter()
                .enumerate()
                .find(|(_, f)| f.name() == tgt.name())
                .ok_or_else(|| {
                    CoreError::ReadFileSliceError(format!(
                        "Output projection: column '{}' not found in batch schema. \
                         Available: [{}]",
                        tgt.name(),
                        source_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                })?;
            let src_col = batch.column(idx);
            narrowed_columns
                .push(narrow_array_to_field(src_col, src_field, tgt)?);
        }

        // Preserve the source batch's row count explicitly. Without this,
        // `RecordBatch::try_new` with zero columns fails Arrow's invariant
        // ("must either specify a row count or at least one column"). That
        // is reachable when Spark prunes the requested data schema to zero
        // fields — e.g. a query that selects only partition columns. The
        // standard Hive/Hudi scan path expects partition columns to be
        // injected at a higher layer (split metadata), so the data-side
        // batch can legitimately be empty.
        let row_count = batch.num_rows();
        RecordBatch::try_new_with_options(
            self.target_schema.clone(),
            narrowed_columns,
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
        .map_err(|e| {
            CoreError::ReadFileSliceError(format!("Output projection failed: {e}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Fields, Schema};
    use std::sync::Arc;

    /// Test that ProjectionConverter correctly narrows a RecordBatch.
    #[test]
    fn test_output_converter_projects_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, true),
            Field::new("col_b", DataType::Int64, true),
            Field::new("col_c", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a1", "a2"])),
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["c1", "c2"])),
            ],
        )
        .unwrap();

        // Project to only col_a and col_c.
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, true),
            Field::new("col_c", DataType::Utf8, true),
        ]));
        let converter = ProjectionConverter::new(&target_schema);
        let projected = converter.apply(batch).unwrap();

        assert_eq!(projected.num_columns(), 2);
        assert_eq!(projected.schema().field(0).name(), "col_a");
        assert_eq!(projected.schema().field(1).name(), "col_c");
        assert_eq!(projected.num_rows(), 2);
    }

    /// Test that ProjectionConverter returns all columns when target matches source.
    #[test]
    fn test_output_converter_noop_when_same_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, true),
            Field::new("col_b", DataType::Int64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a1"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();

        let converter = ProjectionConverter::new(&schema);
        let projected = converter.apply(batch).unwrap();

        assert_eq!(projected.num_columns(), 2);
    }

    /// Test that ProjectionConverter errors on missing column.
    #[test]
    fn test_output_converter_error_on_missing_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col_a",
            DataType::Utf8,
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["a1"]))],
        )
        .unwrap();

        let target_schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, true),
            Field::new("col_missing", DataType::Utf8, true),
        ]));
        let converter = ProjectionConverter::new(&target_schema);
        let result = converter.apply(batch);

        assert!(result.is_err());
    }

    /// Helper: build the source `fare` struct array `{amount: f64, currency: str}`.
    fn make_fare_struct(rows: &[(f64, &str)]) -> (Arc<Field>, ArrayRef) {
        let amount = Arc::new(Float64Array::from(
            rows.iter().map(|(a, _)| *a).collect::<Vec<_>>(),
        ));
        let currency = Arc::new(StringArray::from(
            rows.iter().map(|(_, c)| *c).collect::<Vec<_>>(),
        ));
        let fields = Fields::from(vec![
            Field::new("amount", DataType::Float64, true),
            Field::new("currency", DataType::Utf8, true),
        ]);
        let struct_arr =
            StructArray::try_new(fields.clone(), vec![amount, currency], None).unwrap();
        let field = Arc::new(Field::new(
            "fare",
            DataType::Struct(fields),
            true,
        ));
        (field, Arc::new(struct_arr))
    }

    /// ENG-40168: top-level struct narrowing.
    ///
    /// Given: a RecordBatch whose `fare` column is `struct<amount, currency>`.
    /// When:  target_schema requests `fare<currency>` only.
    /// Then:  output's `fare` is a StructArray with a single `currency` child,
    ///        sharing the original `currency` underlying array (zero-copy
    ///        leaf), and the parent struct null buffer is preserved.
    #[test]
    fn test_output_converter_narrows_nested_struct() {
        let (fare_field, fare_arr) =
            make_fare_struct(&[(1.5, "USD"), (2.5, "EUR")]);
        let other_arr: ArrayRef = Arc::new(Int64Array::from(vec![100, 200]));

        let source_schema = Arc::new(Schema::new(vec![
            (*fare_field).clone(),
            Field::new("other", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            source_schema.clone(),
            vec![fare_arr, other_arr],
        )
        .unwrap();

        // Drop `other` and narrow `fare` to just `currency`.
        let target_fare = Field::new(
            "fare",
            DataType::Struct(Fields::from(vec![Field::new(
                "currency",
                DataType::Utf8,
                true,
            )])),
            true,
        );
        let target_schema =
            Arc::new(Schema::new(vec![target_fare.clone()]));

        let converter = ProjectionConverter::new(&target_schema);
        let result = converter.apply(batch).unwrap();

        assert_eq!(result.num_columns(), 1);
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.schema().field(0).name(), "fare");

        // The narrowed `fare` struct has only one child: `currency`.
        let narrowed_fare = result.column(0).as_struct();
        assert_eq!(narrowed_fare.num_columns(), 1);
        let narrowed_fields = match narrowed_fare.data_type() {
            DataType::Struct(f) => f,
            other => panic!("expected Struct, got {other:?}"),
        };
        assert_eq!(narrowed_fields.len(), 1);
        assert_eq!(narrowed_fields[0].name(), "currency");

        // Currency values intact.
        let cur_arr = narrowed_fare
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(cur_arr.value(0), "USD");
        assert_eq!(cur_arr.value(1), "EUR");
    }

    /// Reorder of struct subfields: `<amount, currency>` → `<currency, amount>`.
    /// Both subfields are kept, order flips.
    #[test]
    fn test_output_converter_reorders_struct_subfields() {
        let (fare_field, fare_arr) = make_fare_struct(&[(9.99, "JPY")]);

        let source_schema =
            Arc::new(Schema::new(vec![(*fare_field).clone()]));
        let batch =
            RecordBatch::try_new(source_schema, vec![fare_arr]).unwrap();

        let target_fare = Field::new(
            "fare",
            DataType::Struct(Fields::from(vec![
                Field::new("currency", DataType::Utf8, true),
                Field::new("amount", DataType::Float64, true),
            ])),
            true,
        );
        let target_schema = Arc::new(Schema::new(vec![target_fare]));

        let converter = ProjectionConverter::new(&target_schema);
        let result = converter.apply(batch).unwrap();

        let narrowed_fare = result.column(0).as_struct();
        let narrowed_fields = match narrowed_fare.data_type() {
            DataType::Struct(f) => f,
            other => panic!("expected Struct, got {other:?}"),
        };
        assert_eq!(narrowed_fields[0].name(), "currency");
        assert_eq!(narrowed_fields[1].name(), "amount");

        let cur = narrowed_fare
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(cur.value(0), "JPY");
        let amt = narrowed_fare
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(amt.value(0), 9.99);
    }

    /// Empty target schema (zero columns) preserves the source row count.
    /// Reachable when Spark prunes the requested data schema to nothing —
    /// e.g. a query that only references partition columns. Without
    /// `with_row_count`, Arrow rejects an empty-column RecordBatch.
    #[test]
    fn test_output_converter_empty_target_preserves_row_count() {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, true),
            Field::new("col_b", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 3);

        let target_schema = Arc::new(Schema::new(Vec::<Field>::new()));
        let converter = ProjectionConverter::new(&target_schema);
        let projected = converter.apply(batch).unwrap();

        assert_eq!(projected.num_columns(), 0);
        assert_eq!(projected.num_rows(), 3);
    }

    /// Asking for a subfield that doesn't exist in the source struct surfaces
    /// a column-name-aware error rather than a cryptic Arrow type mismatch.
    #[test]
    fn test_output_converter_errors_on_missing_subfield() {
        let (fare_field, fare_arr) = make_fare_struct(&[(1.0, "USD")]);
        let source_schema =
            Arc::new(Schema::new(vec![(*fare_field).clone()]));
        let batch =
            RecordBatch::try_new(source_schema, vec![fare_arr]).unwrap();

        let target_fare = Field::new(
            "fare",
            DataType::Struct(Fields::from(vec![Field::new(
                "missing_subfield",
                DataType::Utf8,
                true,
            )])),
            true,
        );
        let target_schema = Arc::new(Schema::new(vec![target_fare]));

        let converter = ProjectionConverter::new(&target_schema);
        let err = converter.apply(batch).unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("missing_subfield"),
            "error should name the missing subfield, got: {msg}"
        );
    }
}
