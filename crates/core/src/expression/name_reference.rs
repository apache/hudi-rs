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

//! Mirrors Java `org.apache.hudi.expression.NameReference`.

use crate::expression::expression::ExpressionKind;
use crate::expression::leaf_expression::LeafExpression;
use crate::expression::{Expression, Operator};
use crate::internal_schema::Type;
use crate::internal_schema::types::StringType;

/// Reference to a column by name. Mirrors Java `NameReference(String name)`.
///
/// Java's `NameReference` has `getDataType()` returning a placeholder
/// `Types.StringType` since the actual type is resolved by `BindVisitor`.
/// Rust mirrors this default.
#[derive(Debug, Clone)]
pub struct NameReference {
    name: String,
}

impl NameReference {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Expression for NameReference {
    fn data_type(&self) -> &dyn Type {
        // Java returns Types.StringType.get() as a placeholder until
        // BindVisitor resolves the real type. Rust mirrors via a
        // singleton StringType reference.
        static TYPE: StringType = StringType;
        &TYPE
    }

    fn operator(&self) -> Operator {
        Operator::True
    }

    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::NameReference(self)
    }
}

impl LeafExpression for NameReference {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{Expression, ExpressionKind};

    #[test]
    fn name_reference_carries_name() {
        let nr = NameReference::new("_hoodie_record_key");
        assert_eq!(nr.name(), "_hoodie_record_key");
    }

    #[test]
    fn name_reference_kind_pattern_match() {
        let nr = NameReference::new("foo");
        match nr.kind() {
            ExpressionKind::NameReference(_) => {}
            _ => panic!("expected ExpressionKind::NameReference"),
        }
    }
}
