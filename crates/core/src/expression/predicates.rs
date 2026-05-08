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
use crate::internal_schema::Type;
use crate::internal_schema::types::BooleanType;
use std::any::Any;

// =========================================================================
// Singleton predicates
// =========================================================================

/// Mirrors Java `Predicates.TrueExpression`.
#[derive(Debug, Clone, Copy)]
pub struct TrueExpression;

impl Expression for TrueExpression {
    fn data_type(&self) -> &dyn Type {
        static T: BooleanType = BooleanType;
        &T
    }
    fn operator(&self) -> Operator {
        Operator::True
    }
    fn eval(&self, _data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        Some(Box::new(true))
    }
    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::Predicate(self)
    }
}

impl LeafExpression for TrueExpression {}

impl Predicate for TrueExpression {
    fn pred_kind(&self) -> PredicateKind<'_> {
        PredicateKind::True
    }
}

/// Mirrors Java `Predicates.FalseExpression`.
#[derive(Debug, Clone, Copy)]
pub struct FalseExpression;

impl Expression for FalseExpression {
    fn data_type(&self) -> &dyn Type {
        static T: BooleanType = BooleanType;
        &T
    }
    fn operator(&self) -> Operator {
        Operator::False
    }
    fn eval(&self, _data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        Some(Box::new(false))
    }
    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::Predicate(self)
    }
}

impl LeafExpression for FalseExpression {}

impl Predicate for FalseExpression {
    fn pred_kind(&self) -> PredicateKind<'_> {
        PredicateKind::False
    }
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
    pub fn left(&self) -> &dyn Expression {
        self.left.as_ref()
    }
    pub fn right(&self) -> &dyn Expression {
        self.right.as_ref()
    }
}

impl Expression for And {
    fn data_type(&self) -> &dyn Type {
        static T: BooleanType = BooleanType;
        &T
    }
    fn operator(&self) -> Operator {
        Operator::And
    }
    fn children(&self) -> Vec<&dyn Expression> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }
    fn eval(&self, data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        let left = self.left.eval(data)?;
        let lb = left.downcast_ref::<bool>().copied().unwrap_or(false);
        if !lb {
            return Some(Box::new(false));
        }
        let right = self.right.eval(data)?;
        let rb = right.downcast_ref::<bool>().copied().unwrap_or(false);
        Some(Box::new(lb && rb))
    }
    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::Predicate(self)
    }
}

impl Predicate for And {
    fn pred_kind(&self) -> PredicateKind<'_> {
        PredicateKind::And(self)
    }
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
    pub fn left(&self) -> &dyn Expression {
        self.left.as_ref()
    }
    pub fn right(&self) -> &dyn Expression {
        self.right.as_ref()
    }
}

impl Expression for Or {
    fn data_type(&self) -> &dyn Type {
        static T: BooleanType = BooleanType;
        &T
    }
    fn operator(&self) -> Operator {
        Operator::Or
    }
    fn children(&self) -> Vec<&dyn Expression> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }
    fn eval(&self, data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        let left = self.left.eval(data)?;
        let lb = left.downcast_ref::<bool>().copied().unwrap_or(false);
        if lb {
            return Some(Box::new(true));
        }
        let right = self.right.eval(data)?;
        let rb = right.downcast_ref::<bool>().copied().unwrap_or(false);
        Some(Box::new(lb || rb))
    }
    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::Predicate(self)
    }
}

impl Predicate for Or {
    fn pred_kind(&self) -> PredicateKind<'_> {
        PredicateKind::Or(self)
    }
}

/// Mirrors Java `Predicates.Not`.
#[derive(Debug)]
pub struct Not {
    pub child: Box<dyn Expression>,
}

impl Not {
    pub fn new(child: Box<dyn Expression>) -> Self {
        Self { child }
    }
    pub fn child(&self) -> &dyn Expression {
        self.child.as_ref()
    }
}

impl Expression for Not {
    fn data_type(&self) -> &dyn Type {
        static T: BooleanType = BooleanType;
        &T
    }
    fn operator(&self) -> Operator {
        Operator::Not
    }
    fn children(&self) -> Vec<&dyn Expression> {
        vec![self.child.as_ref()]
    }
    fn eval(&self, data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        let v = self.child.eval(data)?;
        let b = v.downcast_ref::<bool>().copied().unwrap_or(false);
        Some(Box::new(!b))
    }
    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::Predicate(self)
    }
}

impl Predicate for Not {
    fn pred_kind(&self) -> PredicateKind<'_> {
        PredicateKind::Not(self)
    }
}

// =========================================================================
// Comparison + null + string predicates
// =========================================================================

/// Mirrors Java `Predicates.BinaryComparison`.
#[derive(Debug)]
pub struct BinaryComparison {
    pub left: Box<dyn Expression>,
    pub op: Operator,
    pub right: Box<dyn Expression>,
}

impl BinaryComparison {
    pub fn new(left: Box<dyn Expression>, op: Operator, right: Box<dyn Expression>) -> Self {
        Self { left, op, right }
    }
    pub fn left(&self) -> &dyn Expression {
        self.left.as_ref()
    }
    pub fn right(&self) -> &dyn Expression {
        self.right.as_ref()
    }
}

impl Expression for BinaryComparison {
    fn data_type(&self) -> &dyn Type {
        static T: BooleanType = BooleanType;
        &T
    }
    fn operator(&self) -> Operator {
        self.op
    }
    fn children(&self) -> Vec<&dyn Expression> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }
    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::Predicate(self)
    }
}

impl Predicate for BinaryComparison {
    fn pred_kind(&self) -> PredicateKind<'_> {
        PredicateKind::BinaryComparison(self)
    }
}

/// Mirrors Java `Predicates.In`.
#[derive(Debug)]
pub struct In {
    pub value: Box<dyn Expression>,
    pub valid_values: Vec<Box<dyn Expression>>,
}

impl In {
    pub fn new(value: Box<dyn Expression>, valid_values: Vec<Box<dyn Expression>>) -> Self {
        Self {
            value,
            valid_values,
        }
    }
    pub fn value(&self) -> &dyn Expression {
        self.value.as_ref()
    }
    pub fn right_children(&self) -> &[Box<dyn Expression>] {
        &self.valid_values
    }
}

impl Expression for In {
    fn data_type(&self) -> &dyn Type {
        static T: BooleanType = BooleanType;
        &T
    }
    fn operator(&self) -> Operator {
        Operator::In
    }
    fn children(&self) -> Vec<&dyn Expression> {
        let mut all = vec![self.value.as_ref()];
        all.extend(self.valid_values.iter().map(|e| e.as_ref()));
        all
    }
    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::Predicate(self)
    }
}

impl Predicate for In {
    fn pred_kind(&self) -> PredicateKind<'_> {
        PredicateKind::In(self)
    }
}

/// Mirrors Java `Predicates.IsNull`.
#[derive(Debug)]
pub struct IsNull {
    pub child: Box<dyn Expression>,
}

impl IsNull {
    pub fn new(child: Box<dyn Expression>) -> Self {
        Self { child }
    }
    pub fn child(&self) -> &dyn Expression {
        self.child.as_ref()
    }
}

impl Expression for IsNull {
    fn data_type(&self) -> &dyn Type {
        static T: BooleanType = BooleanType;
        &T
    }
    fn operator(&self) -> Operator {
        Operator::IsNull
    }
    fn children(&self) -> Vec<&dyn Expression> {
        vec![self.child.as_ref()]
    }
    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::Predicate(self)
    }
}

impl Predicate for IsNull {
    fn pred_kind(&self) -> PredicateKind<'_> {
        PredicateKind::IsNull(self)
    }
}

/// Mirrors Java `Predicates.IsNotNull`.
#[derive(Debug)]
pub struct IsNotNull {
    pub child: Box<dyn Expression>,
}

impl IsNotNull {
    pub fn new(child: Box<dyn Expression>) -> Self {
        Self { child }
    }
    pub fn child(&self) -> &dyn Expression {
        self.child.as_ref()
    }
}

impl Expression for IsNotNull {
    fn data_type(&self) -> &dyn Type {
        static T: BooleanType = BooleanType;
        &T
    }
    fn operator(&self) -> Operator {
        Operator::IsNotNull
    }
    fn children(&self) -> Vec<&dyn Expression> {
        vec![self.child.as_ref()]
    }
    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::Predicate(self)
    }
}

impl Predicate for IsNotNull {
    fn pred_kind(&self) -> PredicateKind<'_> {
        PredicateKind::IsNotNull(self)
    }
}

/// Mirrors Java `Predicates.StringStartsWith`.
#[derive(Debug)]
pub struct StringStartsWith {
    pub left: Box<dyn Expression>,
    pub right: Box<dyn Expression>,
}

impl StringStartsWith {
    pub fn new(left: Box<dyn Expression>, right: Box<dyn Expression>) -> Self {
        Self { left, right }
    }
    pub fn left(&self) -> &dyn Expression {
        self.left.as_ref()
    }
    pub fn right(&self) -> &dyn Expression {
        self.right.as_ref()
    }
}

impl Expression for StringStartsWith {
    fn data_type(&self) -> &dyn Type {
        static T: BooleanType = BooleanType;
        &T
    }
    fn operator(&self) -> Operator {
        Operator::StartsWith
    }
    fn children(&self) -> Vec<&dyn Expression> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }
    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::Predicate(self)
    }
}

impl Predicate for StringStartsWith {
    fn pred_kind(&self) -> PredicateKind<'_> {
        PredicateKind::StringStartsWith(self)
    }
}

/// Mirrors Java `Predicates.StringStartsWithAny`.
#[derive(Debug)]
pub struct StringStartsWithAny {
    pub left: Box<dyn Expression>,
    pub right: Vec<Box<dyn Expression>>,
}

impl StringStartsWithAny {
    pub fn new(left: Box<dyn Expression>, right: Vec<Box<dyn Expression>>) -> Self {
        Self { left, right }
    }
    pub fn left(&self) -> &dyn Expression {
        self.left.as_ref()
    }
    pub fn right_children(&self) -> &[Box<dyn Expression>] {
        &self.right
    }
}

impl Expression for StringStartsWithAny {
    fn data_type(&self) -> &dyn Type {
        static T: BooleanType = BooleanType;
        &T
    }
    fn operator(&self) -> Operator {
        Operator::StartsWith
    }
    fn children(&self) -> Vec<&dyn Expression> {
        let mut all = vec![self.left.as_ref()];
        all.extend(self.right.iter().map(|e| e.as_ref()));
        all
    }
    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::Predicate(self)
    }
}

impl Predicate for StringStartsWithAny {
    fn pred_kind(&self) -> PredicateKind<'_> {
        PredicateKind::StringStartsWithAny(self)
    }
}

/// Mirrors Java `Predicates.StringContains`.
#[derive(Debug)]
pub struct StringContains {
    pub left: Box<dyn Expression>,
    pub right: Box<dyn Expression>,
}

impl StringContains {
    pub fn new(left: Box<dyn Expression>, right: Box<dyn Expression>) -> Self {
        Self { left, right }
    }
    pub fn left(&self) -> &dyn Expression {
        self.left.as_ref()
    }
    pub fn right(&self) -> &dyn Expression {
        self.right.as_ref()
    }
}

impl Expression for StringContains {
    fn data_type(&self) -> &dyn Type {
        static T: BooleanType = BooleanType;
        &T
    }
    fn operator(&self) -> Operator {
        Operator::Contains
    }
    fn children(&self) -> Vec<&dyn Expression> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }
    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::Predicate(self)
    }
}

impl Predicate for StringContains {
    fn pred_kind(&self) -> PredicateKind<'_> {
        PredicateKind::StringContains(self)
    }
}

// =========================================================================
// Factory functions — mirror Java `Predicates` static methods.
//
// `in` is a Rust keyword so the function is named `in_`.
// =========================================================================

pub mod predicates_factory {
    use super::*;

    pub fn always_true() -> TrueExpression {
        TrueExpression
    }
    pub fn always_false() -> FalseExpression {
        FalseExpression
    }
    pub fn and(left: Box<dyn Expression>, right: Box<dyn Expression>) -> And {
        And::new(left, right)
    }
    pub fn or(left: Box<dyn Expression>, right: Box<dyn Expression>) -> Or {
        Or::new(left, right)
    }
    pub fn not(child: Box<dyn Expression>) -> Not {
        Not::new(child)
    }
    pub fn gt(left: Box<dyn Expression>, right: Box<dyn Expression>) -> BinaryComparison {
        BinaryComparison::new(left, Operator::Gt, right)
    }
    pub fn lt(left: Box<dyn Expression>, right: Box<dyn Expression>) -> BinaryComparison {
        BinaryComparison::new(left, Operator::Lt, right)
    }
    pub fn eq(left: Box<dyn Expression>, right: Box<dyn Expression>) -> BinaryComparison {
        BinaryComparison::new(left, Operator::Eq, right)
    }
    pub fn gteq(left: Box<dyn Expression>, right: Box<dyn Expression>) -> BinaryComparison {
        BinaryComparison::new(left, Operator::GtEq, right)
    }
    pub fn lteq(left: Box<dyn Expression>, right: Box<dyn Expression>) -> BinaryComparison {
        BinaryComparison::new(left, Operator::LtEq, right)
    }
    pub fn starts_with(left: Box<dyn Expression>, right: Box<dyn Expression>) -> StringStartsWith {
        StringStartsWith::new(left, right)
    }
    pub fn contains(left: Box<dyn Expression>, right: Box<dyn Expression>) -> StringContains {
        StringContains::new(left, right)
    }
    pub fn in_(value: Box<dyn Expression>, valid_values: Vec<Box<dyn Expression>>) -> In {
        In::new(value, valid_values)
    }
    pub fn is_null(child: Box<dyn Expression>) -> IsNull {
        IsNull::new(child)
    }
    pub fn is_not_null(child: Box<dyn Expression>) -> IsNotNull {
        IsNotNull::new(child)
    }
    pub fn starts_with_any(
        left: Box<dyn Expression>,
        right: Vec<Box<dyn Expression>>,
    ) -> StringStartsWithAny {
        StringStartsWithAny::new(left, right)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::Literal;
    use crate::expression::{Expression, Operator, Predicate};

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

    #[test]
    fn binary_comparison_eq() {
        let bc = BinaryComparison::new(
            Box::new(Literal::int(5)),
            Operator::Eq,
            Box::new(Literal::int(5)),
        );
        assert_eq!(bc.operator(), Operator::Eq);
    }

    #[test]
    fn in_predicate_carries_children() {
        let val = Box::new(Literal::string("col"));
        let validvalues = vec![
            Box::new(Literal::string("a")) as Box<dyn Expression>,
            Box::new(Literal::string("b")) as Box<dyn Expression>,
        ];
        let p = In::new(val, validvalues);
        assert_eq!(p.operator(), Operator::In);
        assert_eq!(p.right_children().len(), 2);
        assert_eq!(p.children().len(), 3);
    }

    #[test]
    fn is_null_carries_child() {
        let p = IsNull::new(Box::new(Literal::int(1)));
        assert_eq!(p.operator(), Operator::IsNull);
    }

    #[test]
    fn is_not_null_carries_child() {
        let p = IsNotNull::new(Box::new(Literal::int(1)));
        assert_eq!(p.operator(), Operator::IsNotNull);
    }

    #[test]
    fn string_starts_with_carries_left_right() {
        let p = StringStartsWith::new(
            Box::new(Literal::string("name")),
            Box::new(Literal::string("Al")),
        );
        assert_eq!(p.operator(), Operator::StartsWith);
    }

    #[test]
    fn string_starts_with_any_carries_children() {
        let p = StringStartsWithAny::new(
            Box::new(Literal::string("name")),
            vec![
                Box::new(Literal::string("Al")) as Box<dyn Expression>,
                Box::new(Literal::string("Bo")) as Box<dyn Expression>,
            ],
        );
        assert_eq!(p.operator(), Operator::StartsWith);
        assert_eq!(p.right_children().len(), 2);
    }

    #[test]
    fn string_contains_constructs() {
        let p = StringContains::new(
            Box::new(Literal::string("desc")),
            Box::new(Literal::string("foo")),
        );
        assert_eq!(p.operator(), Operator::Contains);
    }

    #[test]
    fn factory_in_constructs_in() {
        let p = predicates_factory::in_(
            Box::new(Literal::string("col")),
            vec![Box::new(Literal::string("a"))],
        );
        assert_eq!(p.operator(), Operator::In);
    }

    #[test]
    fn factory_starts_with_any_constructs_string_starts_with_any() {
        let p = predicates_factory::starts_with_any(
            Box::new(Literal::string("col")),
            vec![Box::new(Literal::string("a"))],
        );
        assert_eq!(p.operator(), Operator::StartsWith);
    }
}
