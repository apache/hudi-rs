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

//! Mirrors Java `org.apache.hudi.expression.Comparators`.

use crate::expression::LiteralValue;
use crate::internal_schema::{Type, TypeID};
use std::cmp::Ordering;

pub struct Comparators;

impl Comparators {
    /// Mirrors Java `Comparators.forType(Type.PrimitiveType)`. Returns a
    /// comparator function over `LiteralValue`. For unsupported types the
    /// comparator returns `Ordering::Equal` (equivalent to Java throwing,
    /// but we don't panic since the keyFilterOpt path doesn't exercise
    /// non-string comparisons).
    pub fn for_type(ty: &dyn Type) -> fn(&LiteralValue, &LiteralValue) -> Ordering {
        match ty.type_id() {
            TypeID::Boolean => |a, b| match (a, b) {
                (LiteralValue::Bool(x), LiteralValue::Bool(y)) => x.cmp(y),
                _ => Ordering::Equal,
            },
            TypeID::Int | TypeID::Date | TypeID::TimeMillis => |a, b| match (a, b) {
                (LiteralValue::Int(x), LiteralValue::Int(y)) => x.cmp(y),
                _ => Ordering::Equal,
            },
            TypeID::Long
            | TypeID::Time
            | TypeID::Timestamp
            | TypeID::TimestampMillis
            | TypeID::LocalTimestampMillis
            | TypeID::LocalTimestampMicros => |a, b| match (a, b) {
                (LiteralValue::Long(x), LiteralValue::Long(y)) => x.cmp(y),
                _ => Ordering::Equal,
            },
            TypeID::Float => |a, b| match (a, b) {
                (LiteralValue::Float(x), LiteralValue::Float(y)) => {
                    x.partial_cmp(y).unwrap_or(Ordering::Equal)
                }
                _ => Ordering::Equal,
            },
            TypeID::Double => |a, b| match (a, b) {
                (LiteralValue::Double(x), LiteralValue::Double(y)) => {
                    x.partial_cmp(y).unwrap_or(Ordering::Equal)
                }
                _ => Ordering::Equal,
            },
            TypeID::String => |a, b| match (a, b) {
                (LiteralValue::String(x), LiteralValue::String(y)) => x.cmp(y),
                _ => Ordering::Equal,
            },
            TypeID::UUID => |a, b| match (a, b) {
                (LiteralValue::UUID(x), LiteralValue::UUID(y)) => x.cmp(y),
                _ => Ordering::Equal,
            },
            _ => |_, _| Ordering::Equal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::LiteralValue;
    use crate::internal_schema::types::*;
    use std::cmp::Ordering;

    #[test]
    fn int_comparator_orders_correctly() {
        let cmp = Comparators::for_type(&IntType::get());
        assert_eq!(cmp(&LiteralValue::Int(1), &LiteralValue::Int(2)), Ordering::Less);
        assert_eq!(cmp(&LiteralValue::Int(5), &LiteralValue::Int(5)), Ordering::Equal);
        assert_eq!(cmp(&LiteralValue::Int(9), &LiteralValue::Int(2)), Ordering::Greater);
    }

    #[test]
    fn string_comparator_lexicographic() {
        let cmp = Comparators::for_type(&StringType::get());
        assert_eq!(
            cmp(&LiteralValue::String("a".into()), &LiteralValue::String("b".into())),
            Ordering::Less
        );
    }

    #[test]
    fn boolean_comparator() {
        let cmp = Comparators::for_type(&BooleanType::get());
        assert_eq!(cmp(&LiteralValue::Bool(false), &LiteralValue::Bool(true)), Ordering::Less);
    }
}
