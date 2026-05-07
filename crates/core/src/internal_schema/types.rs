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

//! Mirrors `org.apache.hudi.internal.schema.Types`.

use crate::internal_schema::{Type, TypeID};
use std::fmt;

// =========================================================================
// Singleton primitive types — all map to a single `Self` instance via `get()`.
// =========================================================================

macro_rules! singleton_primitive {
    ($name:ident, $id:ident, $disp:literal) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name;

        impl $name {
            pub fn get() -> Self { $name }
        }

        impl Type for $name {
            fn type_id(&self) -> TypeID { TypeID::$id }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str($disp)
            }
        }
    };
}

singleton_primitive!(BooleanType, Boolean, "boolean");
singleton_primitive!(IntType, Int, "int");
singleton_primitive!(LongType, Long, "long");
singleton_primitive!(FloatType, Float, "float");
singleton_primitive!(DoubleType, Double, "double");
singleton_primitive!(DateType, Date, "date");
singleton_primitive!(TimeType, Time, "time");
singleton_primitive!(TimestampType, Timestamp, "timestamp");
singleton_primitive!(TimeMillisType, TimeMillis, "time_millis");
singleton_primitive!(TimestampMillisType, TimestampMillis, "timestamp_millis");
singleton_primitive!(LocalTimestampMillisType, LocalTimestampMillis, "local_timestamp_millis");
singleton_primitive!(LocalTimestampMicrosType, LocalTimestampMicros, "local_timestamp_micros");
singleton_primitive!(StringType, String, "string");
singleton_primitive!(BinaryType, Binary, "binary");
singleton_primitive!(UUIDType, UUID, "uuid");

// =========================================================================
// Parameterized primitive types
// =========================================================================

/// Mirrors Java `Types.FixedType(int size)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FixedType {
    size: u32,
}

impl FixedType {
    pub fn new(size: u32) -> Self { Self { size } }
    pub fn size(&self) -> u32 { self.size }
}

impl Type for FixedType {
    fn type_id(&self) -> TypeID { TypeID::Fixed }
}

impl fmt::Display for FixedType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fixed({})", self.size)
    }
}

/// Mirrors Java `Types.DecimalType(int precision, int scale)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DecimalType {
    precision: u8,
    scale: u8,
}

impl DecimalType {
    pub fn new(precision: u8, scale: u8) -> Self { Self { precision, scale } }
    pub fn precision(&self) -> u8 { self.precision }
    pub fn scale(&self) -> u8 { self.scale }
}

impl Type for DecimalType {
    fn type_id(&self) -> TypeID { TypeID::Decimal }
}

impl fmt::Display for DecimalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "decimal({}, {})", self.precision, self.scale)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_schema::TypeID;

    #[test]
    fn primitive_type_ids() {
        assert_eq!(BooleanType::get().type_id(), TypeID::Boolean);
        assert_eq!(IntType::get().type_id(), TypeID::Int);
        assert_eq!(LongType::get().type_id(), TypeID::Long);
        assert_eq!(FloatType::get().type_id(), TypeID::Float);
        assert_eq!(DoubleType::get().type_id(), TypeID::Double);
        assert_eq!(DateType::get().type_id(), TypeID::Date);
        assert_eq!(TimeType::get().type_id(), TypeID::Time);
        assert_eq!(TimestampType::get().type_id(), TypeID::Timestamp);
        assert_eq!(TimeMillisType::get().type_id(), TypeID::TimeMillis);
        assert_eq!(TimestampMillisType::get().type_id(), TypeID::TimestampMillis);
        assert_eq!(LocalTimestampMillisType::get().type_id(), TypeID::LocalTimestampMillis);
        assert_eq!(LocalTimestampMicrosType::get().type_id(), TypeID::LocalTimestampMicros);
        assert_eq!(StringType::get().type_id(), TypeID::String);
        assert_eq!(BinaryType::get().type_id(), TypeID::Binary);
        assert_eq!(UUIDType::get().type_id(), TypeID::UUID);
    }

    #[test]
    fn primitive_type_to_string() {
        assert_eq!(BooleanType::get().to_string(), "boolean");
        assert_eq!(IntType::get().to_string(), "int");
        assert_eq!(StringType::get().to_string(), "string");
    }

    #[test]
    fn primitive_types_are_not_nested() {
        assert!(!BooleanType::get().is_nested_type());
        assert!(!IntType::get().is_nested_type());
        assert!(!StringType::get().is_nested_type());
    }

    #[test]
    fn primitive_types_equal_their_singletons() {
        assert_eq!(IntType::get(), IntType::get());
        // Different types are never equal even if both are PrimitiveType.
        // (PrimitiveType equality in Java compares typeId only.)
    }

    #[test]
    fn fixed_type_carries_size() {
        let f = FixedType::new(16);
        assert_eq!(f.type_id(), TypeID::Fixed);
        assert_eq!(f.size(), 16);
        assert_eq!(f.to_string(), "fixed(16)");
    }

    #[test]
    fn decimal_type_carries_precision_and_scale() {
        let d = DecimalType::new(10, 2);
        assert_eq!(d.type_id(), TypeID::Decimal);
        assert_eq!(d.precision(), 10);
        assert_eq!(d.scale(), 2);
        assert_eq!(d.to_string(), "decimal(10, 2)");
    }
}
