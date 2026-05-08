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
            pub fn get() -> Self {
                $name
            }
        }

        impl Type for $name {
            fn type_id(&self) -> TypeID {
                TypeID::$id
            }
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
singleton_primitive!(
    LocalTimestampMillisType,
    LocalTimestampMillis,
    "local_timestamp_millis"
);
singleton_primitive!(
    LocalTimestampMicrosType,
    LocalTimestampMicros,
    "local_timestamp_micros"
);
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
    pub fn new(size: u32) -> Self {
        Self { size }
    }
    pub fn size(&self) -> u32 {
        self.size
    }
}

impl Type for FixedType {
    fn type_id(&self) -> TypeID {
        TypeID::Fixed
    }
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
    pub fn new(precision: u8, scale: u8) -> Self {
        Self { precision, scale }
    }
    pub fn precision(&self) -> u8 {
        self.precision
    }
    pub fn scale(&self) -> u8 {
        self.scale
    }
}

impl Type for DecimalType {
    fn type_id(&self) -> TypeID {
        TypeID::Decimal
    }
}

impl fmt::Display for DecimalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "decimal({}, {})", self.precision, self.scale)
    }
}

// =========================================================================
// Nested types
// =========================================================================

/// Mirrors Java `Types.Field`.
///
/// Java fields: `int id`, `boolean isOptional`, `String name`, `Type type`, `String doc`.
/// Default Java constructor sets `doc = null`.
#[derive(Debug)]
pub struct Field {
    id: i32,
    is_optional: bool,
    name: String,
    field_type: Box<dyn Type>,
    doc: Option<String>,
}

impl Field {
    pub fn new(
        id: i32,
        name: impl Into<String>,
        field_type: Box<dyn Type>,
        is_optional: bool,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            field_type,
            is_optional,
            doc: None,
        }
    }

    pub fn with_doc(mut self, doc: impl Into<String>) -> Self {
        self.doc = Some(doc.into());
        self
    }

    pub fn id(&self) -> i32 {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn field_type(&self) -> &dyn Type {
        self.field_type.as_ref()
    }
    pub fn is_optional(&self) -> bool {
        self.is_optional
    }
    pub fn doc(&self) -> Option<&str> {
        self.doc.as_deref()
    }
}

impl PartialEq for Field {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.is_optional == other.is_optional
            && self.name == other.name
            && self.field_type.type_id() == other.field_type.type_id()
            && self.doc == other.doc
    }
}

impl Eq for Field {}

/// Mirrors Java `Types.RecordType`.
#[derive(Debug)]
pub struct RecordType {
    fields: Vec<Field>,
}

impl RecordType {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Mirrors Java `RecordType.fieldType(String name)`.
    pub fn field_type_by_name(&self, name: &str) -> Option<&dyn Type> {
        self.fields
            .iter()
            .find(|f| f.name == name)
            .map(|f| f.field_type())
    }

    /// Mirrors Java `RecordType.field(int id)` — find by field id.
    pub fn field_by_id(&self, id: i32) -> Option<&Field> {
        self.fields.iter().find(|f| f.id == id)
    }

    /// Find a field by name (Rust convenience accessor).
    pub fn field_by_name(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }
}

impl Type for RecordType {
    fn type_id(&self) -> TypeID {
        TypeID::Record
    }
    fn is_nested_type(&self) -> bool {
        true
    }
}

impl fmt::Display for RecordType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "record<{}>", self.fields.len())
    }
}

/// Mirrors Java `Types.ArrayType`.
#[derive(Debug)]
pub struct ArrayType {
    element_id: i32,
    element_optional: bool,
    element_type: Box<dyn Type>,
}

impl ArrayType {
    pub fn new(element_id: i32, element_optional: bool, element_type: Box<dyn Type>) -> Self {
        Self {
            element_id,
            element_optional,
            element_type,
        }
    }

    pub fn element_id(&self) -> i32 {
        self.element_id
    }
    pub fn is_element_optional(&self) -> bool {
        self.element_optional
    }
    pub fn element_type(&self) -> &dyn Type {
        self.element_type.as_ref()
    }
}

impl Type for ArrayType {
    fn type_id(&self) -> TypeID {
        TypeID::Array
    }
    fn is_nested_type(&self) -> bool {
        true
    }
}

impl fmt::Display for ArrayType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "array<{}>", self.element_type.type_id().get_name())
    }
}

/// Mirrors Java `Types.MapType`.
#[derive(Debug)]
pub struct MapType {
    key_id: i32,
    value_id: i32,
    value_optional: bool,
    key_type: Box<dyn Type>,
    value_type: Box<dyn Type>,
}

impl MapType {
    pub fn new(
        key_id: i32,
        value_id: i32,
        value_optional: bool,
        key_type: Box<dyn Type>,
        value_type: Box<dyn Type>,
    ) -> Self {
        Self {
            key_id,
            value_id,
            value_optional,
            key_type,
            value_type,
        }
    }

    pub fn key_id(&self) -> i32 {
        self.key_id
    }
    pub fn value_id(&self) -> i32 {
        self.value_id
    }
    pub fn is_value_optional(&self) -> bool {
        self.value_optional
    }
    pub fn key_type(&self) -> &dyn Type {
        self.key_type.as_ref()
    }
    pub fn value_type(&self) -> &dyn Type {
        self.value_type.as_ref()
    }
}

impl Type for MapType {
    fn type_id(&self) -> TypeID {
        TypeID::Map
    }
    fn is_nested_type(&self) -> bool {
        true
    }
}

impl fmt::Display for MapType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "map<{},{}>",
            self.key_type.type_id().get_name(),
            self.value_type.type_id().get_name()
        )
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
        assert_eq!(
            TimestampMillisType::get().type_id(),
            TypeID::TimestampMillis
        );
        assert_eq!(
            LocalTimestampMillisType::get().type_id(),
            TypeID::LocalTimestampMillis
        );
        assert_eq!(
            LocalTimestampMicrosType::get().type_id(),
            TypeID::LocalTimestampMicros
        );
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

    #[test]
    fn field_carries_id_name_type_nullable() {
        let f = Field::new(1, "id", Box::new(IntType::get()), false);
        assert_eq!(f.id(), 1);
        assert_eq!(f.name(), "id");
        assert!(!f.is_optional());
        assert_eq!(f.field_type().type_id(), TypeID::Int);
    }

    #[test]
    fn record_type_lookup_field_by_name() {
        let fields = vec![
            Field::new(1, "id", Box::new(IntType::get()), false),
            Field::new(2, "name", Box::new(StringType::get()), true),
        ];
        let rec = RecordType::new(fields);
        assert_eq!(rec.type_id(), TypeID::Record);
        assert!(rec.is_nested_type());
        assert_eq!(rec.field_by_name("id").unwrap().id(), 1);
        assert_eq!(rec.field_by_name("name").unwrap().id(), 2);
        assert!(rec.field_by_name("missing").is_none());
    }

    #[test]
    fn array_type_carries_element_type() {
        let arr = ArrayType::new(2, true, Box::new(IntType::get()));
        assert_eq!(arr.type_id(), TypeID::Array);
        assert!(arr.is_nested_type());
        assert!(arr.is_element_optional());
        assert_eq!(arr.element_type().type_id(), TypeID::Int);
    }

    #[test]
    fn map_type_carries_key_value_types() {
        let m = MapType::new(
            3,
            4,
            true,
            Box::new(StringType::get()),
            Box::new(LongType::get()),
        );
        assert_eq!(m.type_id(), TypeID::Map);
        assert!(m.is_nested_type());
        assert_eq!(m.key_type().type_id(), TypeID::String);
        assert_eq!(m.value_type().type_id(), TypeID::Long);
        assert!(m.is_value_optional());
    }
}
