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
 */

//! Mirrors Java `org.apache.hudi.expression.Predicates`.
//!
//! All 12 inner classes from Java's `Predicates` live in this single file
//! to mirror Java's structure. Factory functions at the bottom mirror the
//! static methods on Java's `Predicates`.

use crate::expression::expression::ExpressionKind;
use crate::expression::leaf_expression::LeafExpression;
use crate::expression::predicate::{Predicate, PredicateKind};
use crate::expression::struct_like::StructLike;
use crate::expression::{Expression, Operator};
use crate::internal_schema::types::BooleanType;
use crate::internal_schema::Type;
use std::any::Any;

// =========================================================================
// Singleton predicates
// =========================================================================

/// Mirrors Java `Predicates.TrueExpression`.
#[derive(Debug, Clone, Copy)]
pub struct TrueExpression;

impl Expression for TrueExpression {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::True }
    fn eval(&self, _data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        Some(Box::new(true))
    }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl LeafExpression for TrueExpression {}

impl Predicate for TrueExpression {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::True }
}

/// Mirrors Java `Predicates.FalseExpression`.
#[derive(Debug, Clone, Copy)]
pub struct FalseExpression;

impl Expression for FalseExpression {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::False }
    fn eval(&self, _data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        Some(Box::new(false))
    }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl LeafExpression for FalseExpression {}

impl Predicate for FalseExpression {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::False }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{Expression, Predicate, Operator};

    #[test]
    fn true_expression_eval_returns_true() {
        let t = TrueExpression;
        let result = t.eval(None).unwrap();
        let bool_val = result.downcast_ref::<bool>().expect("expected bool");
        assert!(bool_val);
    }

    #[test]
    fn true_expression_operator_is_true() {
        assert_eq!(TrueExpression.operator(), Operator::True);
    }

    #[test]
    fn false_expression_eval_returns_false() {
        let f = FalseExpression;
        let result = f.eval(None).unwrap();
        let bool_val = result.downcast_ref::<bool>().expect("expected bool");
        assert!(!bool_val);
    }

    #[test]
    fn false_expression_operator_is_false() {
        assert_eq!(FalseExpression.operator(), Operator::False);
    }
}
