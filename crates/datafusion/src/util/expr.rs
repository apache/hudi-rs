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
use hudi_core::expr::filter::{col, Filter as HudiFilter};

/// Converts DataFusion expressions into Hudi filters.
///
/// Takes a slice of DataFusion [`Expr`] and attempts to convert each expression
/// into a [`HudiFilter`]. Only binary expressions and NOT expressions are currently supported.
///
/// # Arguments
/// * `exprs` - A slice of DataFusion expressions to convert
///
/// # Returns
/// Returns `Some(Vec<HudiFilter>)` if at least one filter is successfully converted,
/// otherwise returns `None`.
///
/// TODO: Handle other DataFusion [`Expr`]
pub fn exprs_to_filters(exprs: &[Expr]) -> Vec<(String, String, String)> {
    exprs
        .iter()
        .filter_map(|expr| match expr {
            Expr::BinaryExpr(binary_expr) => binary_expr_to_filter(binary_expr),
            Expr::Not(not_expr) => not_expr_to_filter(not_expr),
            _ => None,
        })
        .map(|filter| filter.into())
        .collect()
}

/// Converts a binary expression [`Expr::BinaryExpr`] into a [`HudiFilter`].
fn binary_expr_to_filter(binary_expr: &BinaryExpr) -> Option<HudiFilter> {
    // extract the column and literal from the binary expression
    let (column, literal) = match (&*binary_expr.left, &*binary_expr.right) {
        (Expr::Column(col), Expr::Literal(lit)) => (col, lit),
        (Expr::Literal(lit), Expr::Column(col)) => (col, lit),
        _ => return None,
    };

    let field = col(column.name());
    let lit_str = literal.to_string();

    let filter = match binary_expr.op {
        Operator::Eq => field.eq(lit_str),
        Operator::NotEq => field.ne(lit_str),
        Operator::Lt => field.lt(lit_str),
        Operator::LtEq => field.lte(lit_str),
        Operator::Gt => field.gt(lit_str),
        Operator::GtEq => field.gte(lit_str),
        _ => return None,
    };

    Some(filter)
}

/// Converts a NOT expression (`Expr::Not`) into a `PartitionFilter`.
fn not_expr_to_filter(not_expr: &Expr) -> Option<HudiFilter> {
    match not_expr {
        Expr::BinaryExpr(ref binary_expr) => {
            binary_expr_to_filter(binary_expr).map(|filter| filter.negate())?
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit};
    use datafusion_expr::{BinaryExpr, Expr};
    use hudi_core::expr::ExprOperator;
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

        let result = exprs_to_filters(&filters);

        assert_eq!(result.len(), 1);

        let expected_filter = HudiFilter {
            field_name: schema.field(0).name().to_string(),
            operator: ExprOperator::Eq,
            field_value: "42".to_string(),
        };
        assert_eq!(
            result[0],
            (
                expected_filter.field_name,
                expected_filter.operator.to_string(),
                expected_filter.field_value
            )
        );
    }

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

        let result = exprs_to_filters(&filters);

        assert_eq!(result.len(), 1);

        let expected_filter = HudiFilter {
            field_name: schema.field(0).name().to_string(),
            operator: ExprOperator::Ne,
            field_value: "42".to_string(),
        };
        assert_eq!(
            result[0],
            (
                expected_filter.field_name,
                expected_filter.operator.to_string(),
                expected_filter.field_value
            )
        );
    }

    #[test]
    fn test_convert_binary_expr_extensive() {
        // list of test cases with different operators and data types
        let test_cases = vec![
            (
                col("int32_col").eq(lit(42i32)),
                Some(HudiFilter {
                    field_name: String::from("int32_col"),
                    operator: ExprOperator::Eq,
                    field_value: String::from("42"),
                }),
            ),
            (
                col("int64_col").gt_eq(lit(100i64)),
                Some(HudiFilter {
                    field_name: String::from("int64_col"),
                    operator: ExprOperator::Gte,
                    field_value: String::from("100"),
                }),
            ),
            (
                col("float64_col").lt(lit(32.666)),
                Some(HudiFilter {
                    field_name: String::from("float64_col"),
                    operator: ExprOperator::Lt,
                    field_value: "32.666".to_string(),
                }),
            ),
            (
                col("string_col").not_eq(lit("test")),
                Some(HudiFilter {
                    field_name: String::from("string_col"),
                    operator: ExprOperator::Ne,
                    field_value: String::from("test"),
                }),
            ),
        ];

        let filters: Vec<Expr> = test_cases.iter().map(|(expr, _)| expr.clone()).collect();
        let result = exprs_to_filters(&filters);
        let expected_filters: Vec<&HudiFilter> = test_cases
            .iter()
            .filter_map(|(_, opt_filter)| opt_filter.as_ref())
            .collect();

        assert_eq!(result.len(), expected_filters.len());

        for (result, expected_filter) in result.iter().zip(expected_filters.iter()) {
            assert_eq!(
                result,
                &(
                    expected_filter.field_name.clone(),
                    expected_filter.operator.to_string(),
                    expected_filter.field_value.clone()
                )
            );
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

            let result = exprs_to_filters(&filters);

            assert_eq!(result.len(), 1);

            let expected_filter = HudiFilter {
                field_name: schema.field(0).name().to_string(),
                operator: expected_op,
                field_value: String::from("42"),
            };
            assert_eq!(
                result[0],
                (
                    expected_filter.field_name,
                    expected_filter.operator.to_string(),
                    expected_filter.field_value
                )
            );
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
        let result = exprs_to_filters(&filters);
        assert!(result.is_empty());
    }

    #[test]
    fn test_negate_operator_for_all_ops() {
        for (op, _) in ExprOperator::TOKEN_OP_PAIRS {
            if let Some(negated_op) = ExprOperator::from_str(op).unwrap().negate() {
                let double_negated_op = negated_op
                    .negate()
                    .expect("Negation should be defined for all operators");

                assert_eq!(double_negated_op, ExprOperator::from_str(op).unwrap());
            }
        }
    }
}
