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

//! Mirrors Java `org.apache.hudi.expression.PartialBindVisitor`.
//!
//! Like BindVisitor but tolerates unresolvable NameReferences. Ported as
//! placeholder; not wired. Spec §6 deviation #6.

use crate::expression::expression_visitor::ExpressionVisitor;
use crate::internal_schema::types::RecordType;

pub struct PartialBindVisitor {
    pub schema: RecordType,
    pub case_sensitive: bool,
}

impl PartialBindVisitor {
    pub fn new(schema: RecordType, case_sensitive: bool) -> Self {
        Self { schema, case_sensitive }
    }
}

impl<T: Default> ExpressionVisitor<T> for PartialBindVisitor {}
