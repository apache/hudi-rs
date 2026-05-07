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

//! Mirrors Java `org.apache.hudi.expression.Predicate`.

use crate::expression::Expression;

/// Mirrors Java `interface Predicate extends Expression`.
///
/// Adds a `pred_kind() -> PredicateKind<'_>` accessor for idiomatic Rust
/// pattern matching. Note this is named `pred_kind` (not `kind`) to avoid
/// collision with the `Expression::kind()` supertrait method when called
/// through a `dyn Predicate` trait object. See keyFilterOpt design spec
/// §4.1, deviation #2.
pub trait Predicate: Expression {
    /// Borrowed enum view for pattern matching.
    fn pred_kind(&self) -> PredicateKind<'_>;
}

/// Borrowed enum view of `Predicate` for pattern matching.
pub enum PredicateKind<'a> {
    True,
    False,
    And(&'a crate::expression::predicates::And),
    Or(&'a crate::expression::predicates::Or),
    Not(&'a crate::expression::predicates::Not),
    BinaryComparison(&'a crate::expression::predicates::BinaryComparison),
    In(&'a crate::expression::predicates::In),
    IsNull(&'a crate::expression::predicates::IsNull),
    IsNotNull(&'a crate::expression::predicates::IsNotNull),
    StringStartsWith(&'a crate::expression::predicates::StringStartsWith),
    StringStartsWithAny(&'a crate::expression::predicates::StringStartsWithAny),
    StringContains(&'a crate::expression::predicates::StringContains),
}
