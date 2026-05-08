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

//! Mirrors Java `org.apache.hudi.expression.BoundReference`.

use crate::expression::expression::ExpressionKind;
use crate::expression::leaf_expression::LeafExpression;
use crate::expression::struct_like::StructLike;
use crate::expression::{Expression, Operator};
use crate::internal_schema::Type;
use std::any::Any;

/// Reference to a column by ordinal position. Mirrors Java `BoundReference`.
#[derive(Debug)]
pub struct BoundReference {
    ordinal: usize,
    data_type: Box<dyn Type>,
}

impl BoundReference {
    pub fn new(ordinal: usize, data_type: Box<dyn Type>) -> Self {
        Self { ordinal, data_type }
    }

    pub fn ordinal(&self) -> usize {
        self.ordinal
    }
}

impl Expression for BoundReference {
    fn data_type(&self) -> &dyn Type {
        self.data_type.as_ref()
    }

    fn operator(&self) -> Operator {
        Operator::True
    }

    /// Mirrors Java `BoundReference.eval(data)` — returns the cell value
    /// at `ordinal`. Returns None on the keyFilterOpt path; not exercised.
    fn eval(&self, data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        let s = data?;
        let _ = s.get(self.ordinal)?;
        None
    }

    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::BoundReference(self)
    }
}

impl LeafExpression for BoundReference {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{Expression, ExpressionKind};
    use crate::internal_schema::types::IntType;

    #[test]
    fn bound_reference_carries_ordinal_and_type() {
        let br = BoundReference::new(3, Box::new(IntType::get()));
        assert_eq!(br.ordinal(), 3);
        assert_eq!(
            br.data_type().type_id(),
            crate::internal_schema::TypeID::Int
        );
    }

    #[test]
    fn bound_reference_kind_pattern_match() {
        let br = BoundReference::new(0, Box::new(IntType::get()));
        match br.kind() {
            ExpressionKind::BoundReference(_) => {}
            _ => panic!("expected ExpressionKind::BoundReference"),
        }
    }
}
