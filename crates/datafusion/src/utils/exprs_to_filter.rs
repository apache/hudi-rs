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

use arrow_array::{Array, Scalar};
use arrow_schema::SchemaRef;
use datafusion::logical_expr::Operator;
use datafusion_expr::{BinaryExpr, Expr};
use hudi_core::exprs::{HudiOperator, PartitionFilter};
use std::sync::Arc;

// TODO: Handle other Datafusion `Expr`

/// Converts a slice of DataFusion expressions (`Expr`) into a vector of `PartitionFilter`.
/// Returns `Some(Vec<PartitionFilter>)` if at least one filter is successfully converted,
/// otherwise returns `None`.
pub fn convert_exprs_to_filter(
    filters: &[Expr],
    partition_schema: &SchemaRef,
) -> Vec<PartitionFilter> {
    let mut partition_filters = Vec::new();

    for expr in filters {
        match expr {
            Expr::BinaryExpr(binary_expr) => {
                if let Some(partition_filter) = convert_binary_expr(binary_expr, partition_schema) {
                    partition_filters.push(partition_filter);
                }
            }
            Expr::Not(not_expr) => {
                // Handle NOT expressions
                if let Some(partition_filter) = convert_not_expr(not_expr, partition_schema) {
                    partition_filters.push(partition_filter);
                }
            }
            _ => {
                continue;
            }
        }
    }

    partition_filters
}

/// Converts a binary expression (`Expr::BinaryExpr`) into a `PartitionFilter`.
fn convert_binary_expr(
    binary_expr: &BinaryExpr,
    partition_schema: &SchemaRef,
) -> Option<PartitionFilter> {
    // extract the column and literal from the binary expression
    let (column, literal) = match (&*binary_expr.left, &*binary_expr.right) {
        (Expr::Column(col), Expr::Literal(lit)) => (col, lit),
        (Expr::Literal(lit), Expr::Column(col)) => (col, lit),
        _ => return None,
    };

    let field = partition_schema
        .field_with_name(column.name())
        .unwrap()
        .clone();

    let operator = match binary_expr.op {
        Operator::Eq => HudiOperator::Eq,
        Operator::NotEq => HudiOperator::Ne,
        Operator::Lt => HudiOperator::Lt,
        Operator::LtEq => HudiOperator::Lte,
        Operator::Gt => HudiOperator::Gt,
        Operator::GtEq => HudiOperator::Gte,
        _ => return None,
    };

    let value = match literal.cast_to(field.data_type()) {
        Ok(value) => {
            let array_ref: Arc<dyn Array> = value.to_array().unwrap();
            Scalar::new(array_ref)
        }
        Err(_) => return None,
    };

    Some(PartitionFilter {
        field,
        operator,
        value,
    })
}

/// Converts a NOT expression (`Expr::Not`) into a `PartitionFilter`.
fn convert_not_expr(not_expr: &Expr, partition_schema: &SchemaRef) -> Option<PartitionFilter> {
    match not_expr {
        Expr::BinaryExpr(ref binary_expr) => {
            let mut partition_filter = convert_binary_expr(binary_expr, partition_schema)?;
            partition_filter.operator = negate_operator(partition_filter.operator)?;
            Some(partition_filter)
        }
        _ => None,
    }
}

/// Negates a given operator
fn negate_operator(op: HudiOperator) -> Option<HudiOperator> {
    match op {
        HudiOperator::Eq => Some(HudiOperator::Ne),
        HudiOperator::Ne => Some(HudiOperator::Eq),
        HudiOperator::Lt => Some(HudiOperator::Gte),
        HudiOperator::Lte => Some(HudiOperator::Gt),
        HudiOperator::Gt => Some(HudiOperator::Lte),
        HudiOperator::Gte => Some(HudiOperator::Lt),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit};
    use datafusion_expr::{BinaryExpr, Expr};
    use hudi_core::exprs::{HudiOperator, PartitionFilter};
    use std::f64::consts::PI;
    use std::sync::Arc;

    #[test]
    fn test_convert_simple_binary_expr() {
        let partition_schema = Arc::new(Schema::new(vec![Field::new(
            "partition_col",
            DataType::Int32,
            false,
        )]));

        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("partition_col")),
            Operator::Eq,
            Box::new(lit(42i32)),
        ));

        let filters = vec![expr];

        let result = convert_exprs_to_filter(&filters, &partition_schema);

        assert_eq!(result.len(), 1);

        let expected_filter = PartitionFilter {
            field: partition_schema.field(0).clone(),
            operator: HudiOperator::Eq,
            value: Scalar::new(Arc::new(Int32Array::from(vec![42])) as ArrayRef),
        };

        assert_eq!(result[0].field, expected_filter.field);
        assert_eq!(result[0].operator, expected_filter.operator);
        assert_eq!(
            *result[0].value.clone().into_inner(),
            expected_filter.value.into_inner()
        );
    }

    // Tests the conversion of a NOT expression
    #[test]
    fn test_convert_not_expr() {
        let partition_schema = Arc::new(Schema::new(vec![Field::new(
            "partition_col",
            DataType::Int32,
            false,
        )]));

        let inner_expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("partition_col")),
            Operator::Eq,
            Box::new(lit(42i32)),
        ));
        let expr = Expr::Not(Box::new(inner_expr));

        let filters = vec![expr];

        let result = convert_exprs_to_filter(&filters, &partition_schema);

        assert_eq!(result.len(), 1);

        let expected_filter = PartitionFilter {
            field: partition_schema.field(0).clone(),
            operator: HudiOperator::Ne,
            value: Scalar::new(Arc::new(Int32Array::from(vec![42])) as ArrayRef),
        };

        assert_eq!(result[0].field, expected_filter.field);
        assert_eq!(result[0].operator, expected_filter.operator);
        assert_eq!(
            *result[0].value.clone().into_inner(),
            expected_filter.value.into_inner()
        );
    }

    #[test]
    fn test_convert_binary_expr_extensive() {
        // partition schema with multiple fields of different data types
        let partition_schema = Arc::new(Schema::new(vec![
            Field::new("int32_col", DataType::Int32, false),
            Field::new("int64_col", DataType::Int64, false),
            Field::new("float64_col", DataType::Float64, false),
            Field::new("string_col", DataType::Utf8, false),
        ]));

        // list of test cases with different operators and data types
        let test_cases = vec![
            (
                col("int32_col").eq(lit(42i32)),
                Some(PartitionFilter {
                    field: partition_schema
                        .field_with_name("int32_col")
                        .unwrap()
                        .clone(),
                    operator: HudiOperator::Eq,
                    value: Scalar::new(Arc::new(Int32Array::from(vec![42])) as ArrayRef),
                }),
            ),
            (
                col("int64_col").gt_eq(lit(100i64)),
                Some(PartitionFilter {
                    field: partition_schema
                        .field_with_name("int64_col")
                        .unwrap()
                        .clone(),
                    operator: HudiOperator::Gte,
                    value: Scalar::new(Arc::new(Int64Array::from(vec![100])) as ArrayRef),
                }),
            ),
            (
                col("float64_col").lt(lit(PI)),
                Some(PartitionFilter {
                    field: partition_schema
                        .field_with_name("float64_col")
                        .unwrap()
                        .clone(),
                    operator: HudiOperator::Lt,
                    value: Scalar::new(Arc::new(Float64Array::from(vec![PI])) as ArrayRef),
                }),
            ),
            (
                col("string_col").not_eq(lit("test")),
                Some(PartitionFilter {
                    field: partition_schema
                        .field_with_name("string_col")
                        .unwrap()
                        .clone(),
                    operator: HudiOperator::Ne,
                    value: Scalar::new(Arc::new(StringArray::from(vec!["test"])) as ArrayRef),
                }),
            ),
        ];

        let filters: Vec<Expr> = test_cases.iter().map(|(expr, _)| expr.clone()).collect();
        let result = convert_exprs_to_filter(&filters, &partition_schema);
        let expected_filters: Vec<&PartitionFilter> = test_cases
            .iter()
            .filter_map(|(_, opt_filter)| opt_filter.as_ref())
            .collect();

        assert_eq!(result.len(), expected_filters.len());

        for (converted_filter, expected_filter) in result.iter().zip(expected_filters.iter()) {
            assert_eq!(converted_filter.field.name(), expected_filter.field.name());
            assert_eq!(converted_filter.operator, expected_filter.operator);
            assert_eq!(
                *converted_filter.value.clone().into_inner(),
                expected_filter.value.clone().into_inner()
            );
        }
    }

    // Tests conversion with different operators (e.g., <, <=, >, >=)
    #[test]
    fn test_convert_various_operators() {
        let partition_schema = Arc::new(Schema::new(vec![Field::new(
            "partition_col",
            DataType::Int32,
            false,
        )]));

        let operators = vec![
            (Operator::Lt, HudiOperator::Lt),
            (Operator::LtEq, HudiOperator::Lte),
            (Operator::Gt, HudiOperator::Gt),
            (Operator::GtEq, HudiOperator::Gte),
        ];

        for (op, expected_op) in operators {
            let expr = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("partition_col")),
                op,
                Box::new(lit(42i32)),
            ));

            let filters = vec![expr];

            let result = convert_exprs_to_filter(&filters, &partition_schema);

            assert_eq!(result.len(), 1);

            let expected_filter = PartitionFilter {
                field: partition_schema.field(0).clone(),
                operator: expected_op,
                value: Scalar::new(Arc::new(Int32Array::from(vec![42])) as ArrayRef),
            };

            assert_eq!(result[0].field, expected_filter.field);
            assert_eq!(result[0].operator, expected_filter.operator);
            assert_eq!(
                *result[0].value.clone().into_inner(),
                expected_filter.value.into_inner()
            );
        }
    }

    #[test]
    fn test_convert_expr_with_unsupported_operator() {
        let partition_schema = Arc::new(Schema::new(vec![Field::new(
            "partition_col",
            DataType::Int32,
            false,
        )]));

        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("partition_col")),
            Operator::And,
            Box::new(lit("value")),
        ));

        let filters = vec![expr];
        let result = convert_exprs_to_filter(&filters, &partition_schema);
        assert!(result.is_empty());
    }
}
