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
use datafusion_expr::{Between, BinaryExpr, Expr};
use hudi_core::expr::filter::{Filter as HudiFilter, col};
use log::{debug, warn};

/// Extracts pushdown-safe filters from DataFusion expressions for partition pruning.
///
/// Takes a slice of DataFusion [`Expr`] and extracts filters that can be safely
/// pushed down for partition pruning. The returned filters represent a **subset**
/// of the original expression's constraints.
///
/// # Supported Expressions
/// - Binary comparisons: `=`, `!=`, `<`, `>`, `<=`, `>=`
/// - `NOT` expressions: negates inner binary expression
/// - `AND` compound expressions: recursively flattens both sides
/// - `BETWEEN` expressions: converts to `>= low AND <= high`
///
/// # OR Expression Handling
///
/// `OR` expressions cannot be represented in the current filter model and are
/// **skipped**. This means expressions containing `OR` will be **partially extracted**:
///
/// | Input Expression      | Extracted Filters | Notes                          |
/// |-----------------------|-------------------|--------------------------------|
/// | `A AND B`             | `[A, B]`          | Full extraction                |
/// | `A OR B`              | `[]`              | OR skipped entirely            |
/// | `A AND (B OR C)`      | `[A]`             | Only A extracted, OR skipped   |
/// | `(A OR B) AND C`      | `[C]`             | Only C extracted, OR skipped   |
///
/// # Safety
///
/// This function is **safe for partition pruning** because:
/// - Extracted filters are a weaker constraint (may match more rows than original)
/// - Partitions that don't match extracted filters definitely don't match original
/// - The original expression must still be applied to filter actual row data
///
/// **Callers must still apply the original expression for correctness.**
/// The extracted filters are for optimization (pruning), not semantic equivalence.
///
/// # Arguments
/// * `exprs` - A slice of DataFusion expressions to convert
///
/// # Returns
/// A vector of filter tuples `(field_name, operator, value)`. All returned filters
/// are implicitly AND-ed together.
pub fn exprs_to_filters(exprs: &[Expr]) -> Vec<(String, String, String)> {
    exprs
        .iter()
        .flat_map(expr_to_filters)
        .map(|filter| filter.into())
        .collect()
}

/// Recursively extracts pushdown-safe filters from a single expression.
///
/// OR expressions return empty (cannot be pushed down), which may result in
/// partial extraction when OR is nested within AND expressions.
fn expr_to_filters(expr: &Expr) -> Vec<HudiFilter> {
    match expr {
        Expr::BinaryExpr(binary_expr) => match binary_expr.op {
            Operator::And => {
                // Recursively flatten AND expressions
                let mut filters = expr_to_filters(&binary_expr.left);
                filters.extend(expr_to_filters(&binary_expr.right));
                filters
            }
            Operator::Or => {
                // Cannot represent OR in current filter model - skip
                vec![]
            }
            _ => binary_expr_to_filter(binary_expr).into_iter().collect(),
        },
        Expr::Not(not_expr) => not_expr_to_filter(not_expr).into_iter().collect(),
        Expr::Between(between) => between_to_filters(between),
        _ => vec![],
    }
}

/// Converts a binary expression [`Expr::BinaryExpr`] into a [`HudiFilter`].
fn binary_expr_to_filter(binary_expr: &BinaryExpr) -> Option<HudiFilter> {
    // extract the column and literal from the binary expression
    let (column, literal) = match (&*binary_expr.left, &*binary_expr.right) {
        (Expr::Column(col), Expr::Literal(lit, _)) => (col, lit),
        (Expr::Literal(lit, _), Expr::Column(col)) => (col, lit),
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
        Expr::BinaryExpr(binary_expr) => {
            binary_expr_to_filter(binary_expr).map(|filter| filter.negate())?
        }
        _ => None,
    }
}

/// Converts a BETWEEN expression into two filters: >= low AND <= high.
///
/// If `negated` is true, returns empty (NOT BETWEEN is complex to represent).
fn between_to_filters(between: &Between) -> Vec<HudiFilter> {
    if between.negated {
        debug!("NOT BETWEEN expressions cannot be pushed down");
        return vec![];
    }

    // Extract column name from the expression
    let column_name = match &*between.expr {
        Expr::Column(col) => col.name.clone(),
        _ => {
            debug!("BETWEEN with non-column expression cannot be pushed down");
            return vec![];
        }
    };

    // Extract literal values from low and high bounds
    let low_str = match &*between.low {
        Expr::Literal(lit, _) => lit.to_string(),
        _ => {
            warn!(
                "BETWEEN low bound is not a literal for column '{column_name}', skipping pushdown"
            );
            return vec![];
        }
    };

    let high_str = match &*between.high {
        Expr::Literal(lit, _) => lit.to_string(),
        _ => {
            warn!(
                "BETWEEN high bound is not a literal for column '{column_name}', skipping pushdown"
            );
            return vec![];
        }
    };

    // Create two filters: >= low AND <= high
    vec![
        col(&column_name).gte(low_str),
        col(&column_name).lte(high_str),
    ]
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
        // Modulo operator is not supported
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("col")),
            Operator::Modulo,
            Box::new(lit(2i32)),
        ));

        let filters = vec![expr];
        let result = exprs_to_filters(&filters);
        assert!(result.is_empty());
    }

    #[test]
    fn test_convert_and_compound_expr() {
        // Test: col1 = 'a' AND col2 = 'b' should produce two filters
        let left = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("col1")),
            Operator::Eq,
            Box::new(lit("a")),
        ));
        let right = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("col2")),
            Operator::Eq,
            Box::new(lit("b")),
        ));
        let and_expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::And,
            Box::new(right),
        ));

        let result = exprs_to_filters(&[and_expr]);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "col1");
        assert_eq!(result[0].1, "=");
        assert_eq!(result[0].2, "a");
        assert_eq!(result[1].0, "col2");
        assert_eq!(result[1].1, "=");
        assert_eq!(result[1].2, "b");
    }

    #[test]
    fn test_convert_or_expr_returns_empty() {
        // OR expressions cannot be pushed down
        let left = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("col1")),
            Operator::Eq,
            Box::new(lit("a")),
        ));
        let right = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("col2")),
            Operator::Eq,
            Box::new(lit("b")),
        ));
        let or_expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::Or,
            Box::new(right),
        ));

        let result = exprs_to_filters(&[or_expr]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_convert_between_expr() {
        // Test: col BETWEEN 10 AND 20 should produce >= 10 AND <= 20
        let between = Expr::Between(Between::new(
            Box::new(col("count")),
            false,
            Box::new(lit(10i32)),
            Box::new(lit(20i32)),
        ));

        let result = exprs_to_filters(&[between]);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "count");
        assert_eq!(result[0].1, ">=");
        assert_eq!(result[0].2, "10");
        assert_eq!(result[1].0, "count");
        assert_eq!(result[1].1, "<=");
        assert_eq!(result[1].2, "20");
    }

    #[test]
    fn test_convert_not_between_returns_empty() {
        // NOT BETWEEN cannot be represented in current filter model
        let not_between = Expr::Between(Between::new(
            Box::new(col("count")),
            true, // negated
            Box::new(lit(10i32)),
            Box::new(lit(20i32)),
        ));

        let result = exprs_to_filters(&[not_between]);
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

    // =========================================================================
    // Partial extraction tests for OR expressions
    // =========================================================================
    //
    // These tests verify the documented behavior: OR expressions cannot be
    // pushed down, so expressions containing OR are partially extracted.
    // This is safe for partition pruning (extracted filters are weaker
    // constraints) but callers must apply original expressions for correctness.

    #[test]
    fn test_partial_extraction_and_with_or_on_right() {
        // Test: A AND (B OR C) should extract only [A]
        // The OR subtree is skipped, leaving only the left AND operand
        let a = col("col_a").eq(lit("a"));
        let b = col("col_b").eq(lit("b"));
        let c = col("col_c").eq(lit("c"));

        // Build: (B OR C)
        let b_or_c = Expr::BinaryExpr(BinaryExpr::new(Box::new(b), Operator::Or, Box::new(c)));

        // Build: A AND (B OR C)
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(a),
            Operator::And,
            Box::new(b_or_c),
        ));

        let result = exprs_to_filters(&[expr]);

        // Only A should be extracted; (B OR C) is skipped
        assert_eq!(
            result.len(),
            1,
            "Expected only 1 filter (A), OR subtree skipped"
        );
        assert_eq!(result[0].0, "col_a");
        assert_eq!(result[0].1, "=");
        assert_eq!(result[0].2, "a");
    }

    #[test]
    fn test_partial_extraction_and_with_or_on_left() {
        // Test: (A OR B) AND C should extract only [C]
        // The OR subtree is skipped, leaving only the right AND operand
        let a = col("col_a").eq(lit("a"));
        let b = col("col_b").eq(lit("b"));
        let c = col("col_c").eq(lit("c"));

        // Build: (A OR B)
        let a_or_b = Expr::BinaryExpr(BinaryExpr::new(Box::new(a), Operator::Or, Box::new(b)));

        // Build: (A OR B) AND C
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(a_or_b),
            Operator::And,
            Box::new(c),
        ));

        let result = exprs_to_filters(&[expr]);

        // Only C should be extracted; (A OR B) is skipped
        assert_eq!(
            result.len(),
            1,
            "Expected only 1 filter (C), OR subtree skipped"
        );
        assert_eq!(result[0].0, "col_c");
        assert_eq!(result[0].1, "=");
        assert_eq!(result[0].2, "c");
    }

    #[test]
    fn test_partial_extraction_complex_and_or_mix() {
        // Test: (A AND B) AND (C OR D) should extract [A, B]
        // The left AND subtree is fully extracted, right OR subtree is skipped
        let a = col("col_a").eq(lit("a"));
        let b = col("col_b").eq(lit("b"));
        let c = col("col_c").eq(lit("c"));
        let d = col("col_d").eq(lit("d"));

        // Build: (A AND B)
        let a_and_b = Expr::BinaryExpr(BinaryExpr::new(Box::new(a), Operator::And, Box::new(b)));

        // Build: (C OR D)
        let c_or_d = Expr::BinaryExpr(BinaryExpr::new(Box::new(c), Operator::Or, Box::new(d)));

        // Build: (A AND B) AND (C OR D)
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(a_and_b),
            Operator::And,
            Box::new(c_or_d),
        ));

        let result = exprs_to_filters(&[expr]);

        // A and B should be extracted; (C OR D) is skipped
        assert_eq!(
            result.len(),
            2,
            "Expected 2 filters (A, B), OR subtree skipped"
        );
        assert_eq!(result[0].0, "col_a");
        assert_eq!(result[1].0, "col_b");
    }

    #[test]
    fn test_partial_extraction_or_both_sides_skipped() {
        // Test: (A OR B) AND (C OR D) should extract []
        // Both sides are OR, so nothing can be extracted
        let a = col("col_a").eq(lit("a"));
        let b = col("col_b").eq(lit("b"));
        let c = col("col_c").eq(lit("c"));
        let d = col("col_d").eq(lit("d"));

        let a_or_b = Expr::BinaryExpr(BinaryExpr::new(Box::new(a), Operator::Or, Box::new(b)));

        let c_or_d = Expr::BinaryExpr(BinaryExpr::new(Box::new(c), Operator::Or, Box::new(d)));

        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(a_or_b),
            Operator::And,
            Box::new(c_or_d),
        ));

        let result = exprs_to_filters(&[expr]);

        // Both sides are OR, nothing extracted
        assert!(
            result.is_empty(),
            "Expected empty result when both AND operands are OR"
        );
    }

    #[test]
    fn test_partial_extraction_deeply_nested() {
        // Test: A AND (B AND (C OR D)) should extract [A, B]
        // Nested AND is flattened, nested OR is skipped
        let a = col("col_a").eq(lit("a"));
        let b = col("col_b").eq(lit("b"));
        let c = col("col_c").eq(lit("c"));
        let d = col("col_d").eq(lit("d"));

        // Build: (C OR D)
        let c_or_d = Expr::BinaryExpr(BinaryExpr::new(Box::new(c), Operator::Or, Box::new(d)));

        // Build: B AND (C OR D)
        let b_and_c_or_d = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(b),
            Operator::And,
            Box::new(c_or_d),
        ));

        // Build: A AND (B AND (C OR D))
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(a),
            Operator::And,
            Box::new(b_and_c_or_d),
        ));

        let result = exprs_to_filters(&[expr]);

        // A and B should be extracted from nested ANDs; (C OR D) is skipped
        assert_eq!(
            result.len(),
            2,
            "Expected 2 filters (A, B) from nested ANDs"
        );
        assert_eq!(result[0].0, "col_a");
        assert_eq!(result[1].0, "col_b");
    }

    #[test]
    fn test_partial_extraction_multiple_input_exprs() {
        // Test: Multiple expressions in input slice
        // Input: [A, (B OR C)] should extract [A] (B OR C skipped)
        let a = col("col_a").eq(lit("a"));
        let b = col("col_b").eq(lit("b"));
        let c = col("col_c").eq(lit("c"));

        let b_or_c = Expr::BinaryExpr(BinaryExpr::new(Box::new(b), Operator::Or, Box::new(c)));

        let result = exprs_to_filters(&[a, b_or_c]);

        // Only A from first expr; second expr (B OR C) is skipped entirely
        assert_eq!(
            result.len(),
            1,
            "Expected 1 filter from first expr, OR expr skipped"
        );
        assert_eq!(result[0].0, "col_a");
    }
}
