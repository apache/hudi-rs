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

//! Mirrors Java `org.apache.hudi.expression.BinaryExpression`.

use crate::expression::{Expression, Operator};

/// Mirrors Java `BinaryExpression`. Fields: left, operator, right.
///
/// Java has `protected final` fields and getters. Rust struct mirrors
/// that with public accessors. Note: this is a helper struct used by
/// concrete predicates (And, Or, BinaryComparison) via composition,
/// not an abstract class.
#[derive(Debug)]
pub struct BinaryExpression {
    pub left: Box<dyn Expression>,
    pub operator: Operator,
    pub right: Box<dyn Expression>,
}

impl BinaryExpression {
    pub fn new(left: Box<dyn Expression>, operator: Operator, right: Box<dyn Expression>) -> Self {
        Self {
            left,
            operator,
            right,
        }
    }

    pub fn left(&self) -> &dyn Expression {
        self.left.as_ref()
    }
    pub fn right(&self) -> &dyn Expression {
        self.right.as_ref()
    }
    pub fn operator(&self) -> Operator {
        self.operator
    }
}
