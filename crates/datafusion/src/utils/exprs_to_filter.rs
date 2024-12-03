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

use datafusion::logical_expr::Operator;
use datafusion_expr::{BinaryExpr, Expr};
use hudi_core::exprs::filter::Filter;
use hudi_core::exprs::ExprOperator;

// TODO: Handle other Datafusion `Expr`

/// Converts a slice of DataFusion expressions (`Expr`) into a vector of `Filter`.
/// Returns `Some(Vec<Filter>)` if at least one filter is successfully converted,
/// otherwise returns `None`.
pub fn convert_exprs_to_filter(exprs: &[Expr]) -> Vec<Filter> {
    let mut filters: Vec<Filter> = Vec::new();

    for expr in exprs {
        match expr {
            Expr::BinaryExpr(binary_expr) => {
                if let Some(filter) = convert_binary_expr(binary_expr) {
                    filters.push(filter);
                }
            }
            Expr::Not(not_expr) => {
                // Handle NOT expressions
                if let Some(filter) = convert_not_expr(not_expr) {
                    filters.push(filter);
                }
            }
            _ => {
                continue;
            }
        }
    }

    filters
}

/// Converts a binary expression (`Expr::BinaryExpr`) into a `Filter`.
fn convert_binary_expr(binary_expr: &BinaryExpr) -> Option<Filter> {
    // extract the column and literal from the binary expression
    let (column, literal) = match (&*binary_expr.left, &*binary_expr.right) {
        (Expr::Column(col), Expr::Literal(lit)) => (col, lit),
        (Expr::Literal(lit), Expr::Column(col)) => (col, lit),
        _ => return None,
    };

    let field_name = column.name().to_string();

    let operator = match binary_expr.op {
        Operator::Eq => ExprOperator::Eq,
        Operator::NotEq => ExprOperator::Ne,
        Operator::Lt => ExprOperator::Lt,
        Operator::LtEq => ExprOperator::Lte,
        Operator::Gt => ExprOperator::Gt,
        Operator::GtEq => ExprOperator::Gte,
        _ => return None,
    };

    let value = literal.to_string();

    Some(Filter {
        field_name,
        operator,
        value,
    })
}

/// Converts a NOT expression (`Expr::Not`) into a `PartitionFilter`.
fn convert_not_expr(not_expr: &Expr) -> Option<Filter> {
    match not_expr {
        Expr::BinaryExpr(ref binary_expr) => {
            let mut filter = convert_binary_expr(binary_expr)?;
            filter.operator = negate_operator(filter.operator)?;
            Some(filter)
        }
        _ => None,
    }
}

/// Negates a given operator
fn negate_operator(op: ExprOperator) -> Option<ExprOperator> {
    match op {
        ExprOperator::Eq => Some(ExprOperator::Ne),
        ExprOperator::Ne => Some(ExprOperator::Eq),
        ExprOperator::Lt => Some(ExprOperator::Gte),
        ExprOperator::Lte => Some(ExprOperator::Gt),
        ExprOperator::Gt => Some(ExprOperator::Lte),
        ExprOperator::Gte => Some(ExprOperator::Lt),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit};
    use datafusion_expr::{BinaryExpr, Expr};
    use hudi_core::exprs::ExprOperator;
    use std::str::FromStr;
    use std::sync::Arc;

    #[test]
    fn test_convert_simple_binary_expr() {
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));

        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("col")),
            Operator::Eq,
            Box::new(lit(42i32)),
        ));

        let filters = vec![expr];

        let result = convert_exprs_to_filter(&filters);

        assert_eq!(result.len(), 1);

        let expected_filter = Filter {
            field_name: schema.field(0).name().to_string(),
            operator: ExprOperator::Eq,
            value: "42".to_string(),
        };

        assert_eq!(result[0].field_name, expected_filter.field_name);
        assert_eq!(result[0].operator, expected_filter.operator);
        assert_eq!(*result[0].value.clone(), expected_filter.value);
    }

    // Tests the conversion of a NOT expression
    #[test]
    fn test_convert_not_expr() {
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));

        let inner_expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("col")),
            Operator::Eq,
            Box::new(lit(42i32)),
        ));
        let expr = Expr::Not(Box::new(inner_expr));

        let filters = vec![expr];

        let result = convert_exprs_to_filter(&filters);

        assert_eq!(result.len(), 1);

        let expected_filter = Filter {
            field_name: schema.field(0).name().to_string(),
            operator: ExprOperator::Ne,
            value: "42".to_string(),
        };

        assert_eq!(result[0].field_name, expected_filter.field_name);
        assert_eq!(result[0].operator, expected_filter.operator);
        assert_eq!(*result[0].value.clone(), expected_filter.value);
    }

    #[test]
    fn test_convert_binary_expr_extensive() {
        // list of test cases with different operators and data types
        let test_cases = vec![
            (
                col("int32_col").eq(lit(42i32)),
                Some(Filter {
                    field_name: String::from("int32_col"),
                    operator: ExprOperator::Eq,
                    value: String::from("42"),
                }),
            ),
            (
                col("int64_col").gt_eq(lit(100i64)),
                Some(Filter {
                    field_name: String::from("int64_col"),
                    operator: ExprOperator::Gte,
                    value: String::from("100"),
                }),
            ),
            (
                col("float64_col").lt(lit(32.666)),
                Some(Filter {
                    field_name: String::from("float64_col"),
                    operator: ExprOperator::Lt,
                    value: "32.666".to_string(),
                }),
            ),
            (
                col("string_col").not_eq(lit("test")),
                Some(Filter {
                    field_name: String::from("string_col"),
                    operator: ExprOperator::Ne,
                    value: String::from("test"),
                }),
            ),
        ];

        let filters: Vec<Expr> = test_cases.iter().map(|(expr, _)| expr.clone()).collect();
        let result = convert_exprs_to_filter(&filters);
        let expected_filters: Vec<&Filter> = test_cases
            .iter()
            .filter_map(|(_, opt_filter)| opt_filter.as_ref())
            .collect();

        assert_eq!(result.len(), expected_filters.len());

        for (result, expected_filter) in result.iter().zip(expected_filters.iter()) {
            assert_eq!(result.field_name, expected_filter.field_name);
            assert_eq!(result.operator, expected_filter.operator);
            assert_eq!(*result.value.clone(), expected_filter.value);
        }
    }

    // Tests conversion with different operators (e.g., <, <=, >, >=)
    #[test]
    fn test_convert_various_operators() {
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));

        let operators = vec![
            (Operator::Lt, ExprOperator::Lt),
            (Operator::LtEq, ExprOperator::Lte),
            (Operator::Gt, ExprOperator::Gt),
            (Operator::GtEq, ExprOperator::Gte),
        ];

        for (op, expected_op) in operators {
            let expr = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("col")),
                op,
                Box::new(lit(42i32)),
            ));

            let filters = vec![expr];

            let result = convert_exprs_to_filter(&filters);

            assert_eq!(result.len(), 1);

            let expected_filter = Filter {
                field_name: schema.field(0).name().to_string(),
                operator: expected_op,
                value: String::from("42"),
            };

            assert_eq!(result[0].field_name, expected_filter.field_name);
            assert_eq!(result[0].operator, expected_filter.operator);
            assert_eq!(*result[0].value.clone(), expected_filter.value);
        }
    }

    #[test]
    fn test_convert_expr_with_unsupported_operator() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("col")),
            Operator::And,
            Box::new(lit("value")),
        ));

        let filters = vec![expr];
        let result = convert_exprs_to_filter(&filters);
        assert!(result.is_empty());
    }

    #[test]
    fn test_negate_operator_for_all_ops() {
        for (op, _) in ExprOperator::TOKEN_OP_PAIRS {
            if let Some(negated_op) = negate_operator(ExprOperator::from_str(op).unwrap()) {
                let double_negated_op = negate_operator(negated_op)
                    .expect("Negation should be defined for all operators");

                assert_eq!(double_negated_op, ExprOperator::from_str(op).unwrap());
            }
        }
    }
}
