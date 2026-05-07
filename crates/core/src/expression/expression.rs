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

//! Mirrors Java `org.apache.hudi.expression.Expression`.
//!
//! Defines the `Expression` trait and the `Operator` enum. Adds an
//! `ExpressionKind<'_>` accessor for idiomatic Rust pattern matching
//! (see keyFilterOpt design spec §4.1, deviation #2).

use crate::expression::struct_like::StructLike;
use crate::internal_schema::Type;
use std::any::Any;
use std::fmt::Debug;

/// Mirrors Java `Expression.Operator`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    And,
    Or,
    Not,
    Eq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    In,
    IsNull,
    IsNotNull,
    StartsWith,
    Contains,
    True,
    False,
}

impl Operator {
    /// Mirrors Java `Operator.symbol`.
    pub fn symbol(&self) -> &'static str {
        match self {
            Operator::And => "AND",
            Operator::Or => "OR",
            Operator::Not => "NOT",
            Operator::Eq => "=",
            Operator::Gt => ">",
            Operator::Lt => "<",
            Operator::GtEq => ">=",
            Operator::LtEq => "<=",
            Operator::In => "IN",
            Operator::IsNull => "IS NULL",
            Operator::IsNotNull => "IS NOT NULL",
            Operator::StartsWith => "starts_with",
            Operator::Contains => "contains",
            Operator::True => "TRUE",
            Operator::False => "FALSE",
        }
    }
}

/// Mirrors Java `org.apache.hudi.expression.Expression`.
///
/// Trait must be `Send + Sync + Debug` so trait objects can be `Arc`'d
/// across the reader pipeline.
pub trait Expression: Debug + Send + Sync + Any {
    /// Mirrors Java `Expression.getDataType()`.
    fn data_type(&self) -> &dyn Type;

    /// Mirrors Java `Expression.getChildren()`.
    fn children(&self) -> Vec<&dyn Expression> {
        Vec::new()
    }

    /// Mirrors Java `Expression.getOperator()`.
    fn operator(&self) -> Operator;

    /// Mirrors Java `Expression.eval(StructLike data)`. Default is `None`
    /// — concrete types override.
    ///
    /// Returns `Box<dyn Any>` so callers can downcast to the expected type.
    /// Not used on the keyFilterOpt reader path; ported for unit-test parity.
    fn eval(&self, _data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        None
    }

    /// Borrowed enum view for idiomatic Rust pattern matching.
    ///
    /// Variants are added incrementally as each concrete Expression type is
    /// introduced in subsequent tasks (Literal — Task 1.8, NameReference —
    /// Task 1.9, BoundReference — Task 1.10, Predicate — Task 1.11).
    fn kind(&self) -> ExpressionKind<'_>;
}

/// Borrowed enum view of `Expression` for pattern matching. Variants are
/// added as concrete types come online — see comment on `Expression::kind`.
pub enum ExpressionKind<'a> {
    Literal(&'a crate::expression::literal::Literal),
    NameReference(&'a crate::expression::name_reference::NameReference),
    BoundReference(&'a crate::expression::bound_reference::BoundReference),
    // Future variant:
    //   Predicate(&'a dyn crate::expression::Predicate),  (Task 1.11)
    _Placeholder(std::marker::PhantomData<&'a ()>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_symbol_matches_java() {
        assert_eq!(Operator::And.symbol(), "AND");
        assert_eq!(Operator::Or.symbol(), "OR");
        assert_eq!(Operator::Not.symbol(), "NOT");
        assert_eq!(Operator::Eq.symbol(), "=");
        assert_eq!(Operator::Gt.symbol(), ">");
        assert_eq!(Operator::Lt.symbol(), "<");
        assert_eq!(Operator::GtEq.symbol(), ">=");
        assert_eq!(Operator::LtEq.symbol(), "<=");
        assert_eq!(Operator::In.symbol(), "IN");
        assert_eq!(Operator::IsNull.symbol(), "IS NULL");
        assert_eq!(Operator::IsNotNull.symbol(), "IS NOT NULL");
        assert_eq!(Operator::StartsWith.symbol(), "starts_with");
        assert_eq!(Operator::Contains.symbol(), "contains");
        assert_eq!(Operator::True.symbol(), "TRUE");
        assert_eq!(Operator::False.symbol(), "FALSE");
    }
}
