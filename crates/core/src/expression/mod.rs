// crates/core/src/expression/mod.rs
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

//! Mirrors `org.apache.hudi.expression`.
//!
//! Full port of the Predicate / Expression hierarchy. Includes a
//! `kind() -> {Predicate,Expression}Kind<'_>` accessor on each trait
//! to give downstream Rust code idiomatic pattern-matching instead
//! of `Any::downcast_ref`. See the keyFilterOpt design spec §4.1.

pub mod array_data;
pub mod bind_visitor;
pub mod binary_expression;
pub mod bound_reference;
pub mod comparators;
pub mod expression;
pub mod expression_visitor;
pub mod leaf_expression;
pub mod literal;
pub mod name_reference;
pub mod partial_bind_visitor;
pub mod predicate;
pub mod predicates;
pub mod struct_like;

// The following re-exports are commented out until each item is introduced
// in subsequent tasks. Uncomment as each task lands:
pub use expression::{Expression, ExpressionKind, Operator};
pub use literal::{Literal, LiteralValue};
pub use name_reference::NameReference;                           // Task 1.9
// pub use predicate::{Predicate, PredicateKind};                // Task 1.11
