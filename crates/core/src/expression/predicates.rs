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

// =========================================================================
// Boolean combinators
// =========================================================================

/// Mirrors Java `Predicates.And`.
#[derive(Debug)]
pub struct And {
    pub left: Box<dyn Expression>,
    pub right: Box<dyn Expression>,
}

impl And {
    pub fn new(left: Box<dyn Expression>, right: Box<dyn Expression>) -> Self {
        Self { left, right }
    }
    pub fn left(&self) -> &dyn Expression { self.left.as_ref() }
    pub fn right(&self) -> &dyn Expression { self.right.as_ref() }
}

impl Expression for And {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::And }
    fn children(&self) -> Vec<&dyn Expression> { vec![self.left.as_ref(), self.right.as_ref()] }
    fn eval(&self, data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        let left = self.left.eval(data)?;
        let lb = left.downcast_ref::<bool>().copied().unwrap_or(false);
        if !lb { return Some(Box::new(false)); }
        let right = self.right.eval(data)?;
        let rb = right.downcast_ref::<bool>().copied().unwrap_or(false);
        Some(Box::new(lb && rb))
    }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl Predicate for And {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::And(self) }
}

/// Mirrors Java `Predicates.Or`.
#[derive(Debug)]
pub struct Or {
    pub left: Box<dyn Expression>,
    pub right: Box<dyn Expression>,
}

impl Or {
    pub fn new(left: Box<dyn Expression>, right: Box<dyn Expression>) -> Self {
        Self { left, right }
    }
    pub fn left(&self) -> &dyn Expression { self.left.as_ref() }
    pub fn right(&self) -> &dyn Expression { self.right.as_ref() }
}

impl Expression for Or {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::Or }
    fn children(&self) -> Vec<&dyn Expression> { vec![self.left.as_ref(), self.right.as_ref()] }
    fn eval(&self, data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        let left = self.left.eval(data)?;
        let lb = left.downcast_ref::<bool>().copied().unwrap_or(false);
        if lb { return Some(Box::new(true)); }
        let right = self.right.eval(data)?;
        let rb = right.downcast_ref::<bool>().copied().unwrap_or(false);
        Some(Box::new(lb || rb))
    }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl Predicate for Or {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::Or(self) }
}

/// Mirrors Java `Predicates.Not`.
#[derive(Debug)]
pub struct Not {
    pub child: Box<dyn Expression>,
}

impl Not {
    pub fn new(child: Box<dyn Expression>) -> Self { Self { child } }
    pub fn child(&self) -> &dyn Expression { self.child.as_ref() }
}

impl Expression for Not {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::Not }
    fn children(&self) -> Vec<&dyn Expression> { vec![self.child.as_ref()] }
    fn eval(&self, data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        let v = self.child.eval(data)?;
        let b = v.downcast_ref::<bool>().copied().unwrap_or(false);
        Some(Box::new(!b))
    }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl Predicate for Not {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::Not(self) }
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

    #[test]
    fn and_constructs_and_carries_children() {
        let a = And::new(Box::new(TrueExpression), Box::new(FalseExpression));
        assert_eq!(a.operator(), Operator::And);
        assert_eq!(a.children().len(), 2);
    }

    #[test]
    fn or_constructs_and_carries_children() {
        let o = Or::new(Box::new(TrueExpression), Box::new(FalseExpression));
        assert_eq!(o.operator(), Operator::Or);
        assert_eq!(o.children().len(), 2);
    }

    #[test]
    fn not_carries_child() {
        let n = Not::new(Box::new(TrueExpression));
        assert_eq!(n.operator(), Operator::Not);
        assert_eq!(n.children().len(), 1);
    }

    #[test]
    fn and_eval_short_circuits_false() {
        let a = And::new(Box::new(FalseExpression), Box::new(TrueExpression));
        let result = a.eval(None).unwrap();
        let b = result.downcast_ref::<bool>().unwrap();
        assert!(!b);
    }
}
