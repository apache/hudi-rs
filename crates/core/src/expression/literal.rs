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

//! Mirrors Java `org.apache.hudi.expression.Literal`.
//!
//! Diverges from Java in one place: Java `Literal<T>` is generic over T;
//! Rust uses a closed `LiteralValue` enum so callers extracting values
//! via `kind()` can pattern-match without a second downcast. See the
//! keyFilterOpt design spec §6 deviation #1.

use crate::expression::expression::ExpressionKind;
use crate::expression::leaf_expression::LeafExpression;
use crate::expression::{Expression, Operator};
use crate::internal_schema::Type;
use crate::internal_schema::types::*;

/// Closed enum of literal-value variants. Mirrors the runtime values
/// Java's `Literal<T>` can carry. Variants are 1:1 with `Types.java` primitives
/// (we store the unscaled+precision+scale tuple for Decimal rather than a
/// `BigDecimal`).
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Bool(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Date(i32),            // days since epoch
    Time(i64),            // micros since midnight
    TimeMillis(i32),      // millis since midnight
    Timestamp(i64),       // micros since epoch
    TimestampMillis(i64), // millis since epoch
    LocalTimestampMillis(i64),
    LocalTimestampMicros(i64),
    Decimal {
        unscaled: i128,
        precision: u8,
        scale: u8,
    },
    String(String),
    Binary(Vec<u8>),
    Fixed(Vec<u8>),
    UUID([u8; 16]),
    Null,
}

/// Mirrors Java `Literal<T>(T value, Type type)` — non-generic in Rust.
#[derive(Debug)]
pub struct Literal {
    value: LiteralValue,
    data_type: Box<dyn Type>,
}

impl Clone for Literal {
    fn clone(&self) -> Self {
        // Reconstruct data_type from TypeID since Box<dyn Type> is not Clone.
        let data_type = clone_type_by_id(self.data_type.as_ref());
        Self {
            value: self.value.clone(),
            data_type,
        }
    }
}

/// Clones a `Box<dyn Type>` by rebuilding the concrete type from its TypeID.
/// Parameterised types (FixedType, DecimalType) are not yet reachable via the
/// convenience constructors, so they fall through to StringType as a safe
/// default (they can be added here when needed).
fn clone_type_by_id(t: &dyn Type) -> Box<dyn Type> {
    use crate::internal_schema::TypeID;
    match t.type_id() {
        TypeID::Boolean => Box::new(BooleanType::get()),
        TypeID::Int => Box::new(IntType::get()),
        TypeID::Long => Box::new(LongType::get()),
        TypeID::Float => Box::new(FloatType::get()),
        TypeID::Double => Box::new(DoubleType::get()),
        TypeID::Date => Box::new(DateType::get()),
        TypeID::Time => Box::new(TimeType::get()),
        TypeID::TimeMillis => Box::new(TimeMillisType::get()),
        TypeID::Timestamp => Box::new(TimestampType::get()),
        TypeID::TimestampMillis => Box::new(TimestampMillisType::get()),
        TypeID::LocalTimestampMillis => Box::new(LocalTimestampMillisType::get()),
        TypeID::LocalTimestampMicros => Box::new(LocalTimestampMicrosType::get()),
        TypeID::String => Box::new(StringType::get()),
        TypeID::Binary => Box::new(BinaryType::get()),
        TypeID::UUID => Box::new(UUIDType::get()),
        // FixedType and DecimalType carry parameters not recoverable from TypeID alone;
        // they are not yet used by convenience constructors. Fall back to StringType.
        _ => Box::new(StringType::get()),
    }
}

impl Literal {
    pub fn new(value: LiteralValue, data_type: Box<dyn Type>) -> Self {
        Self { value, data_type }
    }

    /// Mirrors Java `Literal.getValue()` (returns the carried value enum).
    pub fn value(&self) -> &LiteralValue {
        &self.value
    }

    // Convenience constructors — mirror Java's `Literal.from(value)` static.
    pub fn string(s: impl Into<String>) -> Self {
        Self::new(LiteralValue::String(s.into()), Box::new(StringType::get()))
    }
    pub fn int(n: i32) -> Self {
        Self::new(LiteralValue::Int(n), Box::new(IntType::get()))
    }
    pub fn long(n: i64) -> Self {
        Self::new(LiteralValue::Long(n), Box::new(LongType::get()))
    }
    pub fn boolean(b: bool) -> Self {
        Self::new(LiteralValue::Bool(b), Box::new(BooleanType::get()))
    }
    pub fn double(d: f64) -> Self {
        Self::new(LiteralValue::Double(d), Box::new(DoubleType::get()))
    }
    pub fn float(d: f32) -> Self {
        Self::new(LiteralValue::Float(d), Box::new(FloatType::get()))
    }
    pub fn null() -> Self {
        Self::new(LiteralValue::Null, Box::new(StringType::get()))
        // ^ Java's Literal.from(null) defaults to STRING type. Rust mirrors.
    }
}

impl Expression for Literal {
    fn data_type(&self) -> &dyn Type {
        self.data_type.as_ref()
    }

    /// Java `Literal.getOperator()` is inherited from LeafExpression which
    /// in turn returns `Operator.TRUE` (Java has no specific literal op).
    fn operator(&self) -> Operator {
        Operator::True
    }

    fn eval(
        &self,
        _data: Option<&dyn crate::expression::struct_like::StructLike>,
    ) -> Option<Box<dyn std::any::Any>> {
        // Mirrors Java: `Literal.eval(data)` returns the carried value
        // ignoring `data`. We box-clone the LiteralValue.
        Some(Box::new(self.value.clone()))
    }

    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::Literal(self)
    }
}

impl LeafExpression for Literal {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{Expression, ExpressionKind, Operator};
    use crate::internal_schema::TypeID;

    #[test]
    fn literal_string_value_extraction() {
        let lit = Literal::string("hello");
        assert_eq!(lit.data_type().type_id(), TypeID::String);
        match lit.value() {
            LiteralValue::String(s) => assert_eq!(s, "hello"),
            _ => panic!("expected String literal"),
        }
    }

    #[test]
    fn literal_int_value_extraction() {
        let lit = Literal::int(42);
        assert_eq!(lit.data_type().type_id(), TypeID::Int);
        match lit.value() {
            LiteralValue::Int(n) => assert_eq!(*n, 42),
            _ => panic!("expected Int literal"),
        }
    }

    #[test]
    fn literal_kind_pattern_match() {
        let lit = Literal::string("k");
        match lit.kind() {
            ExpressionKind::Literal(_) => {}
            _ => panic!("expected ExpressionKind::Literal"),
        }
    }

    #[test]
    fn literal_operator_is_true() {
        let lit = Literal::int(1);
        assert_eq!(lit.operator(), Operator::True);
    }
}
