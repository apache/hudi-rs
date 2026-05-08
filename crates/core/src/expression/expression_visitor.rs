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

//! Mirrors Java `org.apache.hudi.expression.ExpressionVisitor`.

use crate::expression::bound_reference::BoundReference;
use crate::expression::predicates::{
    And, BinaryComparison, FalseExpression, In, IsNotNull, IsNull, Not, Or, StringContains,
    StringStartsWith, StringStartsWithAny, TrueExpression,
};
use crate::expression::{Expression, Literal, NameReference};

/// Mirrors Java `ExpressionVisitor<T>`. Each visit method has a default
/// returning `T::default()` (Rust constraint: `T: Default`).
pub trait ExpressionVisitor<T> {
    fn always_true(&mut self) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn always_false(&mut self) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_and(&mut self, _and: &And) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_or(&mut self, _or: &Or) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_not(&mut self, _not: &Not) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_predicate(&mut self, _expr: &dyn Expression) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_literal(&mut self, _lit: &Literal) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_name_reference(&mut self, _nr: &NameReference) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_bound_reference(&mut self, _br: &BoundReference) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_binary_comparison(&mut self, _bc: &BinaryComparison) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_in(&mut self, _in_: &In) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_is_null(&mut self, _p: &IsNull) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_is_not_null(&mut self, _p: &IsNotNull) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_string_starts_with(&mut self, _p: &StringStartsWith) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_string_starts_with_any(&mut self, _p: &StringStartsWithAny) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_string_contains(&mut self, _p: &StringContains) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_true(&mut self, _t: &TrueExpression) -> T
    where
        T: Default,
    {
        T::default()
    }
    fn visit_false(&mut self, _f: &FalseExpression) -> T
    where
        T: Default,
    {
        T::default()
    }
}
