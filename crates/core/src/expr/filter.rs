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

use crate::Result;
use crate::error::CoreError;
use crate::expr::ExprOperator;
use arrow_arith::boolean;
use arrow_array::{ArrayRef, BooleanArray, Datum, Scalar, StringArray};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{DataType, Field, Schema};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct Filter {
    pub field: String,
    pub operator: ExprOperator,
    pub values: Vec<String>,
}

impl Filter {
    pub fn new(field: String, operator: ExprOperator, values: Vec<String>) -> Result<Self> {
        if operator.is_multi_value() {
            if values.is_empty() {
                return Err(CoreError::Schema(format!(
                    "{operator} operator requires at least one value for field '{field}'"
                )));
            }
        } else if values.len() != 1 {
            return Err(CoreError::Schema(format!(
                "Operator {operator} requires exactly one value for field '{field}', got {}",
                values.len()
            )));
        }
        Ok(Self {
            field,
            operator,
            values,
        })
    }
}

impl Filter {
    pub fn negate(&self) -> Option<Self> {
        self.operator.negate().map(|op| Self {
            operator: op,
            ..self.clone()
        })
    }
}

impl From<Filter> for (String, String, String) {
    fn from(filter: Filter) -> Self {
        let value_str = if filter.operator.is_multi_value() {
            filter
                .values
                .iter()
                .map(|v| escape_in_value(v))
                .collect::<Vec<_>>()
                .join(",")
        } else {
            filter.values.first().cloned().unwrap_or_default()
        };
        (filter.field, filter.operator.to_string(), value_str)
    }
}

impl TryFrom<(&str, &str, &str)> for Filter {
    type Error = CoreError;

    fn try_from(binary_expr_tuple: (&str, &str, &str)) -> Result<Self, Self::Error> {
        let (field, operator_str, field_value) = binary_expr_tuple;
        let field = field.to_string();
        let operator = ExprOperator::from_str(operator_str)?;
        let values = if operator.is_multi_value() {
            split_in_values(field_value)
        } else {
            vec![field_value.to_string()]
        };
        Filter::new(field, operator, values)
    }
}

/// Split a multi-value (`IN` / `NOT IN`) value string on unescaped commas, then
/// trim each segment. `\,` is a literal comma; `\\` is a literal backslash; any
/// other backslash is preserved as-is so callers that don't use escapes are not
/// surprised. Empty segments after trimming are dropped — the cardinality check
/// in [`Filter::new`] enforces at least one value.
///
/// Symmetric with [`escape_in_value`] used by `From<Filter> for (String, String, String)`,
/// so a round-trip preserves values that contain commas or backslashes.
fn split_in_values(s: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.peek() {
                Some(&',') => {
                    chars.next();
                    current.push(',');
                }
                Some(&'\\') => {
                    chars.next();
                    current.push('\\');
                }
                _ => current.push('\\'),
            }
        } else if c == ',' {
            let trimmed = current.trim().to_string();
            if !trimmed.is_empty() {
                out.push(trimmed);
            }
            current.clear();
        } else {
            current.push(c);
        }
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        out.push(trimmed);
    }
    out
}

/// Escape backslash and comma so a value can survive a round trip through the
/// `(String, String, String)` tuple form used for `IN` / `NOT IN`.
fn escape_in_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            ',' => out.push_str("\\,"),
            other => out.push(other),
        }
    }
    out
}

pub fn from_str_tuples<I, S>(tuples: I) -> Result<Vec<Filter>>
where
    I: IntoIterator<Item = (S, S, S)>,
    S: AsRef<str>,
{
    tuples
        .into_iter()
        .map(|t| Filter::try_from((t.0.as_ref(), t.1.as_ref(), t.2.as_ref())))
        .collect()
}

/// Evaluate a slice of [`Filter`]s against a [`RecordBatch`] and return a row-level mask
/// where `true` indicates the row should be retained.
///
/// This is a low-level primitive: filters whose field is not present in the batch are
/// skipped silently. Callers that want strict-on-unknown-column behavior must call
/// [`validate_fields_against_schemas`] before invoking this function (this is what
/// the file-group reader paths do).
///
/// All applicable filters are evaluated as row-level predicates and ANDed together.
pub fn filters_to_row_mask(
    filters: &[Filter],
    batch: &arrow_array::RecordBatch,
) -> Result<BooleanArray> {
    let num_rows = batch.num_rows();
    let mut mask: Option<BooleanArray> = None;
    for filter in filters {
        let Some(column) = batch.column_by_name(&filter.field) else {
            continue;
        };
        let schemable = SchemableFilter::try_from((filter.clone(), batch.schema().as_ref()))?;
        let column_mask = schemable.apply_comparison(column)?;
        mask = Some(match mask {
            Some(prev) => boolean::and(&prev, &column_mask)?,
            None => column_mask,
        });
    }
    Ok(mask.unwrap_or_else(|| BooleanArray::from(vec![true; num_rows])))
}

/// Error if any [`Filter`] targets a column not present in any of the provided schemas.
///
/// This is the strict counterpart to [`filters_to_row_mask`], which silently skips
/// missing columns. Pass one schema for the file-group-reader case (validate against
/// the read batch). Pass multiple — e.g. data schema + partition schema — at the
/// table layer, where partition columns may be valid filter targets even when not
/// physically present in the data schema. Failing fast prevents typos like
/// `("rder_id", "=", "x")` from becoming silent no-ops.
pub fn validate_fields_against_schemas<'a, I>(filters: &[Filter], schemas: I) -> Result<()>
where
    I: IntoIterator<Item = &'a Schema>,
{
    use std::collections::HashSet;
    let mut valid: HashSet<&str> = HashSet::new();
    for schema in schemas {
        valid.extend(schema.fields().iter().map(|f| f.name().as_str()));
    }
    for filter in filters {
        if !valid.contains(filter.field.as_str()) {
            return Err(CoreError::Schema(format!(
                "Filter field '{}' not found in schema",
                filter.field
            )));
        }
    }
    Ok(())
}

pub struct FilterField {
    pub name: String,
}

impl FilterField {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn eq(&self, value: impl Into<String>) -> Filter {
        Filter {
            field: self.name.clone(),
            operator: ExprOperator::Eq,
            values: vec![value.into()],
        }
    }

    pub fn ne(&self, value: impl Into<String>) -> Filter {
        Filter {
            field: self.name.clone(),
            operator: ExprOperator::Ne,
            values: vec![value.into()],
        }
    }

    pub fn lt(&self, value: impl Into<String>) -> Filter {
        Filter {
            field: self.name.clone(),
            operator: ExprOperator::Lt,
            values: vec![value.into()],
        }
    }

    pub fn lte(&self, value: impl Into<String>) -> Filter {
        Filter {
            field: self.name.clone(),
            operator: ExprOperator::Lte,
            values: vec![value.into()],
        }
    }

    pub fn gt(&self, value: impl Into<String>) -> Filter {
        Filter {
            field: self.name.clone(),
            operator: ExprOperator::Gt,
            values: vec![value.into()],
        }
    }

    pub fn gte(&self, value: impl Into<String>) -> Filter {
        Filter {
            field: self.name.clone(),
            operator: ExprOperator::Gte,
            values: vec![value.into()],
        }
    }

    pub fn in_list<I, S>(&self, values: I) -> Filter
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Filter {
            field: self.name.clone(),
            operator: ExprOperator::In,
            values: values.into_iter().map(|v| v.into()).collect(),
        }
    }

    pub fn not_in_list<I, S>(&self, values: I) -> Filter
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Filter {
            field: self.name.clone(),
            operator: ExprOperator::NotIn,
            values: values.into_iter().map(|v| v.into()).collect(),
        }
    }
}

pub fn col(name: impl Into<String>) -> FilterField {
    FilterField::new(name)
}

#[derive(Debug, Clone)]
pub struct SchemableFilter {
    pub field: Field,
    pub operator: ExprOperator,
    pub values: Vec<Scalar<ArrayRef>>,
}

impl TryFrom<(Filter, &Schema)> for SchemableFilter {
    type Error = CoreError;

    fn try_from((filter, schema): (Filter, &Schema)) -> Result<Self, Self::Error> {
        let field_name = filter.field.as_str();
        let arrow_field = schema.field_with_name(field_name).map_err(|e| {
            CoreError::Schema(format!("Field {field_name} not found in schema: {e:?}"))
        })?;

        let values: Result<Vec<_>> = filter
            .values
            .iter()
            .map(|v| Self::cast_value(&[v.as_str()], arrow_field.data_type()))
            .collect();

        Ok(SchemableFilter {
            field: arrow_field.clone(),
            operator: filter.operator,
            values: values?,
        })
    }
}

impl SchemableFilter {
    pub fn cast_value(value: &[&str; 1], data_type: &DataType) -> Result<Scalar<ArrayRef>> {
        let cast_options = CastOptions {
            safe: false,
            format_options: Default::default(),
        };

        let value = StringArray::from(Vec::from(value));

        Ok(Scalar::new(
            cast_with_options(&value, data_type, &cast_options)
                .map_err(|e| CoreError::Schema(format!("Unable to cast {data_type:?}: {e:?}")))?,
        ))
    }

    pub fn apply_comparison(&self, value: &dyn Datum) -> Result<BooleanArray> {
        match self.operator {
            ExprOperator::Eq => eq(value, &self.values[0]),
            ExprOperator::Ne => neq(value, &self.values[0]),
            ExprOperator::Lt => lt(value, &self.values[0]),
            ExprOperator::Lte => lt_eq(value, &self.values[0]),
            ExprOperator::Gt => gt(value, &self.values[0]),
            ExprOperator::Gte => gt_eq(value, &self.values[0]),
            ExprOperator::In => {
                // IN: value == values[0] OR value == values[1] OR ...
                let mut result = eq(value, &self.values[0])?;
                for filter_value in &self.values[1..] {
                    let comparison = eq(value, filter_value)?;
                    result = boolean::or(&result, &comparison)?;
                }
                Ok(result)
            }
            ExprOperator::NotIn => {
                // NOT IN: value != values[0] AND value != values[1] AND ...
                let mut result = neq(value, &self.values[0])?;
                for filter_value in &self.values[1..] {
                    let comparison = neq(value, filter_value)?;
                    result = boolean::and(&result, &comparison)?;
                }
                Ok(result)
            }
        }
        .map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("string_col", DataType::Utf8, false),
            Field::new("int_col", DataType::Int64, false),
        ])
    }

    #[test]
    fn test_filters_to_row_mask_combines_filters_with_and() -> Result<()> {
        let schema = Arc::new(create_test_schema());
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ],
        )
        .unwrap();
        let filters = vec![
            Filter::try_from(("string_col", "=", "a"))?,
            Filter::try_from(("int_col", ">", "1"))?,
        ];
        let mask = filters_to_row_mask(&filters, &batch)?;
        assert_eq!(mask, BooleanArray::from(vec![false, false, true]));
        Ok(())
    }

    #[test]
    fn test_filters_to_row_mask_skips_missing_columns() -> Result<()> {
        let schema = Arc::new(create_test_schema());
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            ],
        )
        .unwrap();
        let filters = vec![
            Filter::try_from(("not_in_batch", "=", "x"))?,
            Filter::try_from(("string_col", "=", "a"))?,
        ];
        let mask = filters_to_row_mask(&filters, &batch)?;
        assert_eq!(mask, BooleanArray::from(vec![true, false]));
        Ok(())
    }

    #[test]
    fn test_validate_fields_against_schemas() -> Result<()> {
        let schema = create_test_schema();

        // Empty filter list passes (single-schema form).
        assert!(validate_fields_against_schemas(&[], [&schema]).is_ok());

        // All-known fields pass.
        let valid = vec![
            Filter::try_from(("string_col", "=", "x"))?,
            Filter::try_from(("int_col", ">", "1"))?,
        ];
        assert!(validate_fields_against_schemas(&valid, [&schema]).is_ok());

        // Unknown field errors with a Schema error naming the bad column.
        let invalid = vec![
            Filter::try_from(("string_col", "=", "x"))?,
            Filter::try_from(("typo_col", "=", "y"))?,
        ];
        let err = validate_fields_against_schemas(&invalid, [&schema]).unwrap_err();
        assert!(matches!(err, CoreError::Schema(_)));
        assert!(err.to_string().contains("typo_col"));

        // Multi-schema: a filter on a column present in only one of the schemas
        // passes when the union is considered. This mirrors the Table-layer use case
        // where partition fields may not be in the data schema but are still valid
        // filter targets.
        let partition_schema = Schema::new(vec![Field::new("city", DataType::Utf8, false)]);
        let cross_schema_filters = vec![
            Filter::try_from(("string_col", "=", "x"))?, // in data schema only
            Filter::try_from(("city", "=", "sf"))?,      // in partition schema only
        ];
        assert!(
            validate_fields_against_schemas(&cross_schema_filters, [&schema, &partition_schema])
                .is_ok()
        );

        // A filter on a column missing from BOTH schemas still errors.
        let still_invalid = vec![Filter::try_from(("nope", "=", "x"))?];
        let err = validate_fields_against_schemas(&still_invalid, [&schema, &partition_schema])
            .unwrap_err();
        assert!(err.to_string().contains("nope"));

        Ok(())
    }

    #[test]
    fn test_filters_to_row_mask_empty_returns_all_true() -> Result<()> {
        let schema = Arc::new(create_test_schema());
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            ],
        )
        .unwrap();
        let mask = filters_to_row_mask(&[], &batch)?;
        assert_eq!(mask, BooleanArray::from(vec![true, true]));
        Ok(())
    }

    #[test]
    fn test_schemable_filter_try_from_schema_lookup() -> Result<()> {
        let schema = create_test_schema();

        // Happy path: looks up the field, copies operator, casts values.
        let filter = Filter::new(
            "string_col".to_string(),
            ExprOperator::Eq,
            vec!["test_value".to_string()],
        )?;
        let schemable = SchemableFilter::try_from((filter, &schema))?;
        assert_eq!(schemable.field.name(), "string_col");
        assert_eq!(schemable.field.data_type(), &DataType::Utf8);
        assert_eq!(schemable.operator, ExprOperator::Eq);

        // Error path: unknown column.
        let invalid = Filter::new(
            "non_existent".to_string(),
            ExprOperator::Eq,
            vec!["value".to_string()],
        )?;
        assert!(SchemableFilter::try_from((invalid, &schema)).is_err());

        Ok(())
    }

    #[test]
    fn test_filter_in_empty_values_error() -> Result<()> {
        // IN operator with empty values should error at construction
        let result = Filter::new("int_col".to_string(), ExprOperator::In, vec![]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least one value")
        );

        // NOT IN operator with empty values should also error at construction
        let result = Filter::new("int_col".to_string(), ExprOperator::NotIn, vec![]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least one value")
        );

        Ok(())
    }

    #[test]
    fn test_schemable_filter_cast_value() -> Result<()> {
        // Test casting to string
        let string_value = SchemableFilter::cast_value(&["test"], &DataType::Utf8)?;
        assert_eq!(string_value.get().0.len(), 1);

        // Test casting to integer
        let int_value = SchemableFilter::cast_value(&["42"], &DataType::Int64)?;
        assert_eq!(int_value.get().0.len(), 1);

        // Test invalid integer cast
        let result = SchemableFilter::cast_value(&["not_a_number"], &DataType::Int64);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_schemable_filter_apply_comparison_all_operators() -> Result<()> {
        // Single parameterized test covering all 8 operators across both a string
        // column ("a", "b", "a") and an int column (40, 50, 60). Verifies the
        // dispatch in `apply_comparison` and that the per-operator kernels are
        // wired correctly. Single-value ops use `Vec::from([v])`; set ops use
        // multi-value vectors.
        let schema = create_test_schema();
        let string_array = StringArray::from(vec!["a", "b", "a"]);
        let int_array = Int64Array::from(vec![40, 50, 60]);

        let cases: Vec<(&str, Vec<&str>, ExprOperator, Vec<bool>)> = vec![
            // String column
            (
                "string_col",
                vec!["a"],
                ExprOperator::Eq,
                vec![true, false, true],
            ),
            (
                "string_col",
                vec!["a"],
                ExprOperator::Ne,
                vec![false, true, false],
            ),
            (
                "string_col",
                vec!["a", "b"],
                ExprOperator::In,
                vec![true, true, true],
            ),
            (
                "string_col",
                vec!["a", "b"],
                ExprOperator::NotIn,
                vec![false, false, false],
            ),
            // Int column — covers all 6 binary operators + 2 set operators
            (
                "int_col",
                vec!["50"],
                ExprOperator::Eq,
                vec![false, true, false],
            ),
            (
                "int_col",
                vec!["50"],
                ExprOperator::Ne,
                vec![true, false, true],
            ),
            (
                "int_col",
                vec!["50"],
                ExprOperator::Lt,
                vec![true, false, false],
            ),
            (
                "int_col",
                vec!["50"],
                ExprOperator::Lte,
                vec![true, true, false],
            ),
            (
                "int_col",
                vec!["50"],
                ExprOperator::Gt,
                vec![false, false, true],
            ),
            (
                "int_col",
                vec!["50"],
                ExprOperator::Gte,
                vec![false, true, true],
            ),
            (
                "int_col",
                vec!["40", "60"],
                ExprOperator::In,
                vec![true, false, true],
            ),
            (
                "int_col",
                vec!["40", "60"],
                ExprOperator::NotIn,
                vec![false, true, false],
            ),
        ];

        for (column, values, operator, expected) in cases {
            let filter = Filter::new(
                column.to_string(),
                operator,
                values.iter().map(|s| s.to_string()).collect(),
            )?;
            let schemable = SchemableFilter::try_from((filter, &schema))?;
            let result = match column {
                "string_col" => schemable.apply_comparison(&string_array)?,
                "int_col" => schemable.apply_comparison(&int_array)?,
                other => unreachable!("unexpected column {other}"),
            };
            assert_eq!(
                result,
                BooleanArray::from(expected),
                "operator {operator:?} on {column} with {values:?}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_filter_try_from_tuple_in_operator() -> Result<()> {
        // TryFrom should parse comma-separated values for IN
        let filter = Filter::try_from(("col", "IN", "a,b,c"))?;
        assert_eq!(filter.operator, ExprOperator::In);
        assert_eq!(filter.values, vec!["a", "b", "c"]);

        // TryFrom should parse comma-separated values for NOT IN
        let filter = Filter::try_from(("col", "NOT IN", "x, y"))?;
        assert_eq!(filter.operator, ExprOperator::NotIn);
        assert_eq!(filter.values, vec!["x", "y"]);

        // Empty value string for IN should error
        let result = Filter::try_from(("col", "IN", ""));
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_filter_roundtrip_in_operator() -> Result<()> {
        // Round-trip plain values (no escape needed).
        let original = Filter::new(
            "col".to_string(),
            ExprOperator::In,
            vec!["a".to_string(), "b".to_string()],
        )
        .unwrap();

        let tuple: (String, String, String) = original.into();
        assert_eq!(
            tuple,
            ("col".to_string(), "IN".to_string(), "a,b".to_string())
        );

        let restored = Filter::try_from((tuple.0.as_str(), tuple.1.as_str(), tuple.2.as_str()))?;
        assert_eq!(restored.operator, ExprOperator::In);
        assert_eq!(restored.values, vec!["a", "b"]);

        Ok(())
    }

    #[test]
    fn test_filter_in_value_comma_escape_parses_literal_comma() -> Result<()> {
        // `\,` is a literal comma; `,` is a separator.
        let filter = Filter::try_from(("name", "IN", "Smith\\, John,Jane"))?;
        assert_eq!(filter.operator, ExprOperator::In);
        assert_eq!(filter.values, vec!["Smith, John", "Jane"]);

        // `\\` is a literal backslash; non-special escapes are preserved as `\`.
        let filter = Filter::try_from(("name", "IN", "a\\\\b,c\\d"))?;
        assert_eq!(filter.values, vec!["a\\b", "c\\d"]);

        // Trailing escape with no following char is preserved as a literal `\`.
        let filter = Filter::try_from(("name", "IN", "x\\"))?;
        assert_eq!(filter.values, vec!["x\\"]);

        // Empty segments after trim are dropped; a fully empty value still errors via cardinality.
        let filter = Filter::try_from(("name", "IN", "a,,b"))?;
        assert_eq!(filter.values, vec!["a", "b"]);

        Ok(())
    }

    #[test]
    fn test_filter_roundtrip_in_value_with_special_chars() -> Result<()> {
        // Filter values containing commas and backslashes must round-trip
        // through the (String, String, String) tuple form.
        let original = Filter::new(
            "name".to_string(),
            ExprOperator::In,
            vec!["Smith, John".to_string(), "back\\slash".to_string()],
        )?;
        let tuple: (String, String, String) = original.into();
        assert_eq!(
            tuple,
            (
                "name".to_string(),
                "IN".to_string(),
                "Smith\\, John,back\\\\slash".to_string()
            )
        );
        let restored = Filter::try_from((tuple.0.as_str(), tuple.1.as_str(), tuple.2.as_str()))?;
        assert_eq!(restored.values, vec!["Smith, John", "back\\slash"]);
        Ok(())
    }

    #[test]
    fn test_filter_roundtrip_scalar_with_special_chars() -> Result<()> {
        // Scalar operators must not escape commas or backslashes because the
        // parser does not unescape them for non-IN operators.
        for op in [
            ExprOperator::Eq,
            ExprOperator::Ne,
            ExprOperator::Lt,
            ExprOperator::Lte,
            ExprOperator::Gt,
            ExprOperator::Gte,
        ] {
            let original = Filter::new("city".to_string(), op, vec!["a,b\\c".to_string()])?;
            let tuple: (String, String, String) = original.into();
            assert_eq!(tuple.2, "a,b\\c", "operator {op:?} should not escape");
            let restored =
                Filter::try_from((tuple.0.as_str(), tuple.1.as_str(), tuple.2.as_str()))?;
            assert_eq!(
                restored.values,
                vec!["a,b\\c"],
                "operator {op:?} round-trip"
            );
        }
        Ok(())
    }
}
