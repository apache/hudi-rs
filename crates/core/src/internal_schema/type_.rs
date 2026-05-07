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

//! Mirrors `org.apache.hudi.internal.schema.Type`.

use std::fmt::Debug;

/// Type identifier — mirrors Java's `Type.TypeID` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypeID {
    Record,
    Array,
    Map,
    Fixed,
    String,
    Binary,
    Int,
    Long,
    Float,
    Double,
    Date,
    Boolean,
    Time,
    Timestamp,
    Decimal,
    UUID,
    DecimalBytes,
    DecimalFixed,
    TimeMillis,
    TimestampMillis,
    LocalTimestampMillis,
    LocalTimestampMicros,
}

impl TypeID {
    /// Mirrors Java `TypeID.getName()` — lowercase snake_case of the enum name.
    pub fn get_name(&self) -> &'static str {
        match self {
            TypeID::Record => "record",
            TypeID::Array => "array",
            TypeID::Map => "map",
            TypeID::Fixed => "fixed",
            TypeID::String => "string",
            TypeID::Binary => "binary",
            TypeID::Int => "int",
            TypeID::Long => "long",
            TypeID::Float => "float",
            TypeID::Double => "double",
            TypeID::Date => "date",
            TypeID::Boolean => "boolean",
            TypeID::Time => "time",
            TypeID::Timestamp => "timestamp",
            TypeID::Decimal => "decimal",
            TypeID::UUID => "uuid",
            TypeID::DecimalBytes => "decimal_bytes",
            TypeID::DecimalFixed => "decimal_fixed",
            TypeID::TimeMillis => "time_millis",
            TypeID::TimestampMillis => "timestamp_millis",
            TypeID::LocalTimestampMillis => "local_timestamp_millis",
            TypeID::LocalTimestampMicros => "local_timestamp_micros",
        }
    }

    /// Mirrors Java `Type.fromValue(String)` — looks up a TypeID by its
    /// uppercase Java enum name (case-insensitive). Returns Err for unknown.
    pub fn from_value(value: &str) -> Result<Self, String> {
        match value.to_ascii_uppercase().as_str() {
            "RECORD" => Ok(TypeID::Record),
            "ARRAY" => Ok(TypeID::Array),
            "MAP" => Ok(TypeID::Map),
            "FIXED" => Ok(TypeID::Fixed),
            "STRING" => Ok(TypeID::String),
            "BINARY" => Ok(TypeID::Binary),
            "INT" => Ok(TypeID::Int),
            "LONG" => Ok(TypeID::Long),
            "FLOAT" => Ok(TypeID::Float),
            "DOUBLE" => Ok(TypeID::Double),
            "DATE" => Ok(TypeID::Date),
            "BOOLEAN" => Ok(TypeID::Boolean),
            "TIME" => Ok(TypeID::Time),
            "TIMESTAMP" => Ok(TypeID::Timestamp),
            "DECIMAL" => Ok(TypeID::Decimal),
            "UUID" => Ok(TypeID::UUID),
            "DECIMAL_BYTES" => Ok(TypeID::DecimalBytes),
            "DECIMAL_FIXED" => Ok(TypeID::DecimalFixed),
            "TIME_MILLIS" => Ok(TypeID::TimeMillis),
            "TIMESTAMP_MILLIS" => Ok(TypeID::TimestampMillis),
            "LOCAL_TIMESTAMP_MILLIS" => Ok(TypeID::LocalTimestampMillis),
            "LOCAL_TIMESTAMP_MICROS" => Ok(TypeID::LocalTimestampMicros),
            _ => Err(format!("Invalid value of Type: {value}")),
        }
    }

    /// Test helper — list all variants for round-trip testing.
    #[cfg(test)]
    pub fn all() -> Vec<TypeID> {
        vec![
            TypeID::Record,
            TypeID::Array,
            TypeID::Map,
            TypeID::Fixed,
            TypeID::String,
            TypeID::Binary,
            TypeID::Int,
            TypeID::Long,
            TypeID::Float,
            TypeID::Double,
            TypeID::Date,
            TypeID::Boolean,
            TypeID::Time,
            TypeID::Timestamp,
            TypeID::Decimal,
            TypeID::UUID,
            TypeID::DecimalBytes,
            TypeID::DecimalFixed,
            TypeID::TimeMillis,
            TypeID::TimestampMillis,
            TypeID::LocalTimestampMillis,
            TypeID::LocalTimestampMicros,
        ]
    }
}

/// Mirrors Java `Type` interface. All concrete types implement this trait.
///
/// Java has `extends Serializable` — Rust trait objects can derive `Debug`
/// and the concrete impls in `types.rs` derive `Clone, PartialEq, Eq, Hash`.
pub trait Type: Debug + Send + Sync {
    /// Mirrors Java `Type.typeId()`.
    fn type_id(&self) -> TypeID;

    /// Mirrors Java default `Type.isNestedType()`. Default false; nested
    /// types override.
    fn is_nested_type(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_id_get_name_returns_lowercase() {
        assert_eq!(TypeID::Int.get_name(), "int");
        assert_eq!(TypeID::TimestampMillis.get_name(), "timestamp_millis");
        assert_eq!(TypeID::LocalTimestampMicros.get_name(), "local_timestamp_micros");
    }

    #[test]
    fn type_id_from_value_round_trips() {
        for variant in TypeID::all() {
            let s = variant.get_name();
            assert_eq!(TypeID::from_value(s).unwrap(), variant);
        }
    }

    #[test]
    fn type_id_from_value_rejects_unknown() {
        assert!(TypeID::from_value("nope").is_err());
    }
}
