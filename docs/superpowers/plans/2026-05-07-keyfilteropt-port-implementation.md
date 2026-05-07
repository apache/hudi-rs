# keyFilterOpt port — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring hudi-rs's `ReaderContext` to parity with Java's `HoodieReaderContext.keyFilterOpt` by porting `org.apache.hudi.expression` (Predicate hierarchy) plus the `org.apache.hudi.internal.schema.{Type,Types}` subset it depends on, then wiring `key_filter_opt: Option<Arc<dyn Predicate>>` through the file-group reader so an `In`-predicate on `_hoodie_record_key` actually filters output rows on a v9 MOR + COMMIT_TIME_ORDERING table.

**Architecture:** Two new top-level modules: `internal_schema` (Type/Types port) and `expression` (full Predicate/Expression hierarchy with `kind() -> {Predicate,Expression}Kind<'_>` accessors for idiomatic Rust pattern matching). Phase 2 adds a new field on `ReaderContext`, a new `key_spec.rs` file with `create_key_spec()`, and modifies `BaseHoodieLogRecordReader::scan_internal` to accept and apply `Option<KeySpec>`. Phase 3 adds one e2e integration test.

**Tech Stack:** Rust 2024 edition, Tokio async runtime, Arrow `RecordBatch`, existing hudi-rs crates (`hudi_core`, `hudi_test`).

**Spec reference:** `docs/superpowers/specs/2026-05-07-keyfilteropt-port-design.md`

**Source-of-truth Java files** (read these alongside the plan):
- `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/internal/schema/Type.java` (189 lines)
- `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/internal/schema/Types.java` (955 lines)
- All 14 files under `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/`
- `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/common/engine/HoodieReaderContext.java` (the keyFilterOpt declaration + getter)
- `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/common/table/log/HoodieMergedLogRecordReader.java` (createKeySpec)

**File structure created/modified:**

```
crates/core/src/
├── lib.rs                                  ← MODIFY: add `pub mod expression;` + `pub mod internal_schema;`
│
├── internal_schema/                        ← NEW
│   ├── mod.rs
│   ├── type_.rs                            ← Type trait + TypeID enum + PrimitiveType/NestedType markers
│   ├── types.rs                            ← all concrete types (Boolean, Int, …, Decimal, Record, Array, Map)
│   └── tests.rs                            ← integration tests for type system
│
├── expression/                             ← NEW
│   ├── mod.rs
│   ├── expression.rs                       ← Expression trait + ExpressionKind + Operator
│   ├── struct_like.rs                      ← StructLike trait
│   ├── array_data.rs                       ← ArrayData
│   ├── leaf_expression.rs                  ← LeafExpression trait
│   ├── binary_expression.rs                ← BinaryExpression
│   ├── literal.rs                          ← Literal + LiteralValue enum
│   ├── name_reference.rs                   ← NameReference
│   ├── bound_reference.rs                  ← BoundReference
│   ├── predicate.rs                        ← Predicate trait + PredicateKind enum
│   ├── predicates.rs                       ← Predicates module (12 inner classes + factories)
│   ├── comparators.rs                      ← Comparators::for_type
│   ├── expression_visitor.rs               ← ExpressionVisitor trait
│   ├── bind_visitor.rs                     ← BindVisitor
│   ├── partial_bind_visitor.rs             ← PartialBindVisitor
│   └── tests.rs                            ← integration tests for expression hierarchy
│
└── file_group/reader/
    ├── reader_context.rs                   ← MODIFY: add `key_filter_opt` field + getter
    ├── key_spec.rs                         ← NEW: KeySpec enum + create_key_spec + string_literals
    ├── log_record_reader.rs                ← MODIFY: scan_internal takes Option<KeySpec>; apply filter
    ├── merged_log_record_reader.rs         ← MODIFY: perform_scan calls create_key_spec
    └── mod.rs                              ← MODIFY: HoodieFileGroupReader::make_base_file_batches applies row filter

crates/core/tests/
└── file_group_reader_tests.rs              ← MODIFY: add fg_reader_with_key_filter_filters_rows test
```

---

## Phase 1 — Port the Predicate hierarchy + Type system

Phase 1 produces a self-contained, fully unit-tested port. **No reader integration in this phase.** The phase ends with one commit and `cargo test -p hudi-core` passing.

### Task 1.1: Module skeleton + lib.rs re-exports

**Files:**
- Create: `crates/core/src/internal_schema/mod.rs`
- Create: `crates/core/src/expression/mod.rs`
- Modify: `crates/core/src/lib.rs:49-60` (insert two new module declarations alphabetically)

- [ ] **Step 1: Create empty `internal_schema/mod.rs`**

```rust
// crates/core/src/internal_schema/mod.rs
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

//! Mirrors `org.apache.hudi.internal.schema.{Type, Types}`.
//!
//! Schema-evolution code (`InternalSchema`, `AvroInternalSchemaConverter`,
//! schema-change actions/visitors, `SchemaChangeUtils`) is intentionally
//! out of scope — see the keyFilterOpt design spec §2.

pub mod type_;
pub mod types;

pub use type_::{Type, TypeID};
```

- [ ] **Step 2: Create empty `expression/mod.rs`**

```rust
// crates/core/src/expression/mod.rs
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * ...
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

pub use expression::{Expression, ExpressionKind, Operator};
pub use literal::{Literal, LiteralValue};
pub use name_reference::NameReference;
pub use predicate::{Predicate, PredicateKind};
```

(The submodule files don't exist yet — they will be created in subsequent tasks. The `mod` declarations will fail to compile until each task adds its file. We could stub each one as empty modules first; do that in Step 3.)

- [ ] **Step 3: Stub all `expression/*.rs` and `internal_schema/*.rs` files as empty placeholders**

Create each file with just the Apache license header + one-line module doc comment. Example:

```rust
// crates/core/src/internal_schema/type_.rs
/* … license header … */

//! Stub — populated in Task 1.2.
```

Files to stub:
- `crates/core/src/internal_schema/type_.rs`
- `crates/core/src/internal_schema/types.rs`
- `crates/core/src/expression/array_data.rs`
- `crates/core/src/expression/bind_visitor.rs`
- `crates/core/src/expression/binary_expression.rs`
- `crates/core/src/expression/bound_reference.rs`
- `crates/core/src/expression/comparators.rs`
- `crates/core/src/expression/expression.rs`
- `crates/core/src/expression/expression_visitor.rs`
- `crates/core/src/expression/leaf_expression.rs`
- `crates/core/src/expression/literal.rs`
- `crates/core/src/expression/name_reference.rs`
- `crates/core/src/expression/partial_bind_visitor.rs`
- `crates/core/src/expression/predicate.rs`
- `crates/core/src/expression/predicates.rs`
- `crates/core/src/expression/struct_like.rs`

The two `mod.rs` files reference items (`Type`, `TypeID`, `Expression`, etc.) that don't exist yet, so temporarily comment out the `pub use` statements in both `mod.rs` files. They'll be uncommented as the items are introduced in later tasks.

- [ ] **Step 4: Update `crates/core/src/lib.rs`**

In `crates/core/src/lib.rs:49-60`, insert the two module declarations alphabetically:

```rust
pub mod avro_to_arrow;
pub mod config;
pub mod error;
pub mod expr;
pub mod expression;            // ← NEW
pub mod file_group;
pub mod hfile;
pub mod internal_schema;       // ← NEW
pub mod keygen;
pub mod merge;
pub mod metadata;
mod record;
pub mod schema;
pub mod statistics;
```

- [ ] **Step 5: Verify the crate compiles**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo check -p hudi-core 2>&1 | tail -20`
Expected: clean compile (no errors). Possible warnings about unused empty modules are OK.

- [ ] **Step 6: Commit the skeleton**

```bash
git add crates/core/src/lib.rs crates/core/src/internal_schema crates/core/src/expression
git commit -m "$(cat <<'EOF'
add empty module skeletons for expression + internal_schema

Stub modules to be populated in subsequent tasks of the keyFilterOpt
port. See docs/superpowers/plans/2026-05-07-keyfilteropt-port-implementation.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 1.2: Port `Type` trait + `TypeID` enum + PrimitiveType/NestedType markers

**Files:**
- Modify: `crates/core/src/internal_schema/type_.rs`
- Modify: `crates/core/src/internal_schema/mod.rs:14` (uncomment `pub use type_::{Type, TypeID};`)

**Java source:** `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/internal/schema/Type.java` (189 lines, full file)

- [ ] **Step 1: Write failing tests** for `crates/core/src/internal_schema/type_.rs` (append at end of file)

```rust
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
```

- [ ] **Step 2: Run tests; expect compile failure**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib internal_schema::type_ 2>&1 | tail -10`
Expected: FAIL — `TypeID` does not exist.

- [ ] **Step 3: Implement `Type` trait + `TypeID` enum**

Replace the file body with:

```rust
// crates/core/src/internal_schema/type_.rs
/* … license header … */

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
            TypeID::Record, TypeID::Array, TypeID::Map, TypeID::Fixed,
            TypeID::String, TypeID::Binary, TypeID::Int, TypeID::Long,
            TypeID::Float, TypeID::Double, TypeID::Date, TypeID::Boolean,
            TypeID::Time, TypeID::Timestamp, TypeID::Decimal, TypeID::UUID,
            TypeID::DecimalBytes, TypeID::DecimalFixed, TypeID::TimeMillis,
            TypeID::TimestampMillis, TypeID::LocalTimestampMillis,
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
```

- [ ] **Step 4: Uncomment the re-export in `internal_schema/mod.rs`**

Edit `crates/core/src/internal_schema/mod.rs` to uncomment:

```rust
pub use type_::{Type, TypeID};
```

- [ ] **Step 5: Run tests and verify pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib internal_schema::type_ 2>&1 | tail -10`
Expected: 3 tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/core/src/internal_schema/type_.rs crates/core/src/internal_schema/mod.rs
git commit -m "port Type trait + TypeID enum from internal.schema.Type.java

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.3: Port primitive types from `Types.java`

**Files:**
- Modify: `crates/core/src/internal_schema/types.rs`

**Java source:** `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/internal/schema/Types.java` lines 36-560 (covering BooleanType through UUIDType — i.e., all classes that extend `PrimitiveType`).

This task ports the 17 primitive types: BooleanType, IntType, LongType, FloatType, DoubleType, DateType, TimeType, TimestampType, TimeMillisType, TimestampMillisType, LocalTimestampMillisType, LocalTimestampMicrosType, StringType, BinaryType, FixedType, DecimalType, UUIDType.

- [ ] **Step 1: Write failing tests** at the bottom of `crates/core/src/internal_schema/types.rs`

```rust
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
```

- [ ] **Step 2: Run tests; expect compile failure (none of the types exist yet)**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib internal_schema::types 2>&1 | tail -20`
Expected: FAIL — `BooleanType`, `IntType`, etc. do not exist.

- [ ] **Step 3: Implement all 17 primitive types**

Replace `crates/core/src/internal_schema/types.rs` body with:

```rust
// crates/core/src/internal_schema/types.rs
/* … license header … */

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
```

- [ ] **Step 4: Run tests; verify pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib internal_schema::types 2>&1 | tail -10`
Expected: 6 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/core/src/internal_schema/types.rs
git commit -m "port primitive Types from internal.schema.Types.java

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.4: Port nested types (Field, RecordType, ArrayType, MapType)

**Files:**
- Modify: `crates/core/src/internal_schema/types.rs` (append nested types after primitive types)

**Java source:** `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/internal/schema/Types.java` lines 562-955 (Field, RecordType, ArrayType, MapType inner classes).

- [ ] **Step 1: Write failing tests** (append to existing `tests` mod in `types.rs`)

```rust
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
            3, 4, true,
            Box::new(StringType::get()),
            Box::new(LongType::get()),
        );
        assert_eq!(m.type_id(), TypeID::Map);
        assert!(m.is_nested_type());
        assert_eq!(m.key_type().type_id(), TypeID::String);
        assert_eq!(m.value_type().type_id(), TypeID::Long);
        assert!(m.is_value_optional());
    }
```

- [ ] **Step 2: Run tests; expect compile failure**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib internal_schema::types 2>&1 | tail -10`
Expected: FAIL — `Field`, `RecordType`, etc. don't exist.

- [ ] **Step 3: Implement Field + RecordType + ArrayType + MapType**

Append to `crates/core/src/internal_schema/types.rs` (before the `#[cfg(test)]` mod):

```rust
// =========================================================================
// Nested types
// =========================================================================

/// Mirrors Java `Types.Field`.
///
/// Java fields: `int id`, `boolean isOptional`, `String name`, `Type type`, `String doc`.
/// Default Java constructor sets `doc = null`.
#[derive(Debug, Clone)]
pub struct Field {
    id: i32,
    is_optional: bool,
    name: String,
    field_type: Box<dyn Type>,
    doc: Option<String>,
}

impl Field {
    pub fn new(id: i32, name: impl Into<String>, field_type: Box<dyn Type>, is_optional: bool) -> Self {
        Self { id, name: name.into(), field_type, is_optional, doc: None }
    }

    pub fn with_doc(mut self, doc: impl Into<String>) -> Self {
        self.doc = Some(doc.into());
        self
    }

    pub fn id(&self) -> i32 { self.id }
    pub fn name(&self) -> &str { &self.name }
    pub fn field_type(&self) -> &dyn Type { self.field_type.as_ref() }
    pub fn is_optional(&self) -> bool { self.is_optional }
    pub fn doc(&self) -> Option<&str> { self.doc.as_deref() }
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
#[derive(Debug, Clone)]
pub struct RecordType {
    fields: Vec<Field>,
}

impl RecordType {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn fields(&self) -> &[Field] { &self.fields }

    /// Mirrors Java `RecordType.fieldType(String name)`.
    pub fn field_type_by_name(&self, name: &str) -> Option<&dyn Type> {
        self.fields.iter().find(|f| f.name == name).map(|f| f.field_type())
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
    fn type_id(&self) -> TypeID { TypeID::Record }
    fn is_nested_type(&self) -> bool { true }
}

impl fmt::Display for RecordType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "record<{}>", self.fields.len())
    }
}

/// Mirrors Java `Types.ArrayType`.
#[derive(Debug, Clone)]
pub struct ArrayType {
    element_id: i32,
    element_optional: bool,
    element_type: Box<dyn Type>,
}

impl ArrayType {
    pub fn new(element_id: i32, element_optional: bool, element_type: Box<dyn Type>) -> Self {
        Self { element_id, element_optional, element_type }
    }

    pub fn element_id(&self) -> i32 { self.element_id }
    pub fn is_element_optional(&self) -> bool { self.element_optional }
    pub fn element_type(&self) -> &dyn Type { self.element_type.as_ref() }
}

impl Type for ArrayType {
    fn type_id(&self) -> TypeID { TypeID::Array }
    fn is_nested_type(&self) -> bool { true }
}

impl fmt::Display for ArrayType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "array<{}>", self.element_type.type_id().get_name())
    }
}

/// Mirrors Java `Types.MapType`.
#[derive(Debug, Clone)]
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
        Self { key_id, value_id, value_optional, key_type, value_type }
    }

    pub fn key_id(&self) -> i32 { self.key_id }
    pub fn value_id(&self) -> i32 { self.value_id }
    pub fn is_value_optional(&self) -> bool { self.value_optional }
    pub fn key_type(&self) -> &dyn Type { self.key_type.as_ref() }
    pub fn value_type(&self) -> &dyn Type { self.value_type.as_ref() }
}

impl Type for MapType {
    fn type_id(&self) -> TypeID { TypeID::Map }
    fn is_nested_type(&self) -> bool { true }
}

impl fmt::Display for MapType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "map<{},{}>", self.key_type.type_id().get_name(), self.value_type.type_id().get_name())
    }
}
```

- [ ] **Step 4: Run tests; verify pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib internal_schema::types 2>&1 | tail -10`
Expected: 10 tests pass (6 from Task 1.3 + 4 new).

- [ ] **Step 5: Commit**

```bash
git add crates/core/src/internal_schema/types.rs
git commit -m "port nested Types (Field, RecordType, ArrayType, MapType)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.5: Port `StructLike` trait + `ArrayData`

**Files:**
- Modify: `crates/core/src/expression/struct_like.rs`
- Modify: `crates/core/src/expression/array_data.rs`

**Java sources:**
- `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/StructLike.java` (26 lines, full)
- `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/ArrayData.java` (40 lines, full)

- [ ] **Step 1: Write failing tests** at the bottom of `struct_like.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    struct MockStruct(Vec<Option<Box<dyn std::any::Any>>>);

    impl StructLike for MockStruct {
        fn num_fields(&self) -> usize { self.0.len() }
        fn get(&self, pos: usize) -> Option<&dyn std::any::Any> {
            self.0[pos].as_deref()
        }
    }

    #[test]
    fn struct_like_basic() {
        let s = MockStruct(vec![
            Some(Box::new(42i32)),
            Some(Box::new("hello".to_string())),
            None,
        ]);
        assert_eq!(s.num_fields(), 3);
        assert!(s.get(2).is_none());
    }
}
```

- [ ] **Step 2: Run tests; expect compile failure**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::struct_like 2>&1 | tail -10`
Expected: FAIL.

- [ ] **Step 3: Implement `StructLike`**

Replace `crates/core/src/expression/struct_like.rs` body:

```rust
// crates/core/src/expression/struct_like.rs
/* … license header … */

//! Mirrors Java `org.apache.hudi.expression.StructLike`.

use std::any::Any;

/// Read-only view over a record-like value. Mirrors Java's `StructLike`.
///
/// Java has `int numFields()` + `<T> T get(int pos, Class<T> javaClass)`.
/// Rust uses `&dyn Any` so callers can downcast.
pub trait StructLike: Send + Sync {
    fn num_fields(&self) -> usize;
    fn get(&self, pos: usize) -> Option<&dyn Any>;
}
```

- [ ] **Step 4: Implement `ArrayData`**

Replace `crates/core/src/expression/array_data.rs` body:

```rust
// crates/core/src/expression/array_data.rs
/* … license header … */

//! Mirrors Java `org.apache.hudi.expression.ArrayData`.

use std::any::Any;

/// Read-only view over an array-like value. Mirrors Java's `ArrayData`.
pub trait ArrayData: Send + Sync {
    fn num_elements(&self) -> usize;
    fn get(&self, pos: usize) -> Option<&dyn Any>;
}
```

- [ ] **Step 5: Run tests; verify pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::struct_like 2>&1 | tail -10`
Expected: 1 test passes.

- [ ] **Step 6: Commit**

```bash
git add crates/core/src/expression/struct_like.rs crates/core/src/expression/array_data.rs
git commit -m "port StructLike + ArrayData traits

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.6: Port `Operator` enum + `Expression` trait + `ExpressionKind`

**Files:**
- Modify: `crates/core/src/expression/expression.rs`
- Modify: `crates/core/src/expression/mod.rs:18-20` (uncomment `pub use expression::{Expression, ExpressionKind, Operator};`)

**Java source:** `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/Expression.java` (69 lines, full)

This is the most central piece of Phase 1. It defines:
1. `Operator` enum mirroring Java `Expression.Operator`.
2. `Expression` trait with the `kind()` accessor (deviation #2 from spec §6).
3. `ExpressionKind<'a>` borrowed enum view.

The concrete variants `Literal`, `NameReference`, `BoundReference`, and `Predicate` referenced in `ExpressionKind` don't exist yet — we forward-declare with `#[allow(dead_code)]` or use type aliases that resolve when those tasks land. Plan: declare `ExpressionKind` as enum with placeholder unit-variant signatures referring to types defined in later tasks, and use `pub mod` re-exports to make them visible. Alternative: define `ExpressionKind` as a marker trait now and convert to enum later. Easier: define the enum but use `&dyn Expression` for variants we haven't created concrete types for, then refine in subsequent tasks.

For this task: define `ExpressionKind<'a>` with **only the variants whose target types exist** at this point (none — Literal/NameReference/BoundReference/Predicate are all later). So the initial `ExpressionKind<'a>` is empty-but-extensible — we'll add variants in Tasks 1.7, 1.8, 1.9, 1.11. Use a single `Other(&'a dyn Expression)` placeholder variant that is removed once all real variants exist.

Actually, simpler: define `ExpressionKind<'a>` as enum **with all 4 variants up-front** but each variant references a concrete struct that we'll build in next tasks. Since the structs don't exist, use a **forward-declared module path** that compiles only after those modules are populated. Concretely: temporarily comment out the variants, add them as each subsequent task introduces the type.

Cleanest approach: define `ExpressionKind` minimally now (no variants), grow it in subsequent tasks. Each later task adds:
- the concrete struct,
- the `ExpressionKind` variant pointing at it,
- the `kind()` impl returning that variant.

Implement that pattern.

- [ ] **Step 1: Write failing tests** at the bottom of `expression.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_symbol_matches_java() {
        assert_eq!(Operator::And.symbol(), "AND");
        assert_eq!(Operator::Or.symbol(), "OR");
        assert_eq!(Operator::Not.symbol(), "NOT");
        assert_eq!(Operator::Eq.symbol(), "=");
        assert_eq!(Operator::Gt.symbol(), ">");
        assert_eq!(Operator::Lt.symbol(), "<");
        assert_eq!(Operator::GtEq.symbol(), ">=");
        assert_eq!(Operator::LtEq.symbol(), "<=");
        assert_eq!(Operator::In.symbol(), "IN");
        assert_eq!(Operator::IsNull.symbol(), "IS NULL");
        assert_eq!(Operator::IsNotNull.symbol(), "IS NOT NULL");
        assert_eq!(Operator::StartsWith.symbol(), "starts_with");
        assert_eq!(Operator::Contains.symbol(), "contains");
        assert_eq!(Operator::True.symbol(), "TRUE");
        assert_eq!(Operator::False.symbol(), "FALSE");
    }
}
```

- [ ] **Step 2: Run tests; expect compile failure**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::expression 2>&1 | tail -10`
Expected: FAIL — `Operator` does not exist.

- [ ] **Step 3: Implement `Operator` + `Expression` + `ExpressionKind`**

Replace `crates/core/src/expression/expression.rs` body:

```rust
// crates/core/src/expression/expression.rs
/* … license header … */

//! Mirrors Java `org.apache.hudi.expression.Expression`.
//!
//! Defines the `Expression` trait and the `Operator` enum. Adds an
//! `ExpressionKind<'_>` accessor for idiomatic Rust pattern matching
//! (see keyFilterOpt design spec §4.1, deviation #2).

use crate::expression::struct_like::StructLike;
use crate::internal_schema::Type;
use std::any::Any;
use std::fmt::Debug;

/// Mirrors Java `Expression.Operator`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    And,
    Or,
    Not,
    Eq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    In,
    IsNull,
    IsNotNull,
    StartsWith,
    Contains,
    True,
    False,
}

impl Operator {
    /// Mirrors Java `Operator.symbol`.
    pub fn symbol(&self) -> &'static str {
        match self {
            Operator::And => "AND",
            Operator::Or => "OR",
            Operator::Not => "NOT",
            Operator::Eq => "=",
            Operator::Gt => ">",
            Operator::Lt => "<",
            Operator::GtEq => ">=",
            Operator::LtEq => "<=",
            Operator::In => "IN",
            Operator::IsNull => "IS NULL",
            Operator::IsNotNull => "IS NOT NULL",
            Operator::StartsWith => "starts_with",
            Operator::Contains => "contains",
            Operator::True => "TRUE",
            Operator::False => "FALSE",
        }
    }
}

/// Mirrors Java `org.apache.hudi.expression.Expression`.
///
/// Trait must be `Send + Sync + Debug` so trait objects can be `Arc`'d
/// across the reader pipeline.
pub trait Expression: Debug + Send + Sync + Any {
    /// Mirrors Java `Expression.getDataType()`.
    fn data_type(&self) -> &dyn Type;

    /// Mirrors Java `Expression.getChildren()`.
    fn children(&self) -> Vec<&dyn Expression> { Vec::new() }

    /// Mirrors Java `Expression.getOperator()`.
    fn operator(&self) -> Operator;

    /// Mirrors Java `Expression.eval(StructLike data)`. Default is `None`
    /// — concrete types override.
    ///
    /// Returns `Box<dyn Any>` so callers can downcast to the expected type.
    /// Not used on the keyFilterOpt reader path; ported for unit-test parity.
    fn eval(&self, _data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        None
    }

    /// Borrowed enum view for idiomatic Rust pattern matching.
    ///
    /// Variants are added incrementally as each concrete Expression type is
    /// introduced in subsequent tasks (Literal — Task 1.8, NameReference —
    /// Task 1.9, BoundReference — Task 1.10, Predicate — Task 1.11).
    fn kind(&self) -> ExpressionKind<'_>;
}

/// Borrowed enum view of `Expression` for pattern matching. Variants are
/// added as concrete types come online — see comment on `Expression::kind`.
pub enum ExpressionKind<'a> {
    // Variants added by subsequent tasks:
    //   Literal(&'a Literal),                   (Task 1.8)
    //   NameReference(&'a NameReference),       (Task 1.9)
    //   BoundReference(&'a BoundReference),     (Task 1.10)
    //   Predicate(&'a dyn crate::expression::Predicate),  (Task 1.11)
    /// Placeholder so the empty enum compiles. Removed in Task 1.11.
    _Placeholder(std::marker::PhantomData<&'a ()>),
}
```

- [ ] **Step 4: Uncomment the re-export in `expression/mod.rs`**

```rust
pub use expression::{Expression, ExpressionKind, Operator};
```

- [ ] **Step 5: Run tests; verify pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::expression 2>&1 | tail -10`
Expected: 1 test passes.

- [ ] **Step 6: Commit**

```bash
git add crates/core/src/expression/expression.rs crates/core/src/expression/mod.rs
git commit -m "port Expression trait + Operator enum + empty ExpressionKind

ExpressionKind variants land in subsequent tasks as concrete types are
introduced (Literal, NameReference, BoundReference, Predicate).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.7: Port `LeafExpression` and `BinaryExpression`

**Files:**
- Modify: `crates/core/src/expression/leaf_expression.rs`
- Modify: `crates/core/src/expression/binary_expression.rs`

**Java sources:**
- `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/LeafExpression.java` (32 lines, full)
- `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/BinaryExpression.java` (60 lines, full)

These are abstract base classes in Java. In Rust they become traits. Concrete subclasses (Literal, NameReference for LeafExpression; And, Or, BinaryComparison for BinaryExpression) implement them in later tasks.

- [ ] **Step 1: Implement `LeafExpression`** (no separate test — exercised by concrete subclasses in later tasks)

Replace `crates/core/src/expression/leaf_expression.rs`:

```rust
// crates/core/src/expression/leaf_expression.rs
/* … license header … */

//! Mirrors Java `org.apache.hudi.expression.LeafExpression`.

use crate::expression::Expression;

/// Marker trait for expressions with no children. Mirrors Java's
/// `LeafExpression` abstract class.
pub trait LeafExpression: Expression {}
```

- [ ] **Step 2: Implement `BinaryExpression`**

Replace `crates/core/src/expression/binary_expression.rs`:

```rust
// crates/core/src/expression/binary_expression.rs
/* … license header … */

//! Mirrors Java `org.apache.hudi.expression.BinaryExpression`.

use crate::expression::{Expression, Operator};

/// Mirrors Java `BinaryExpression`. Fields: left, operator, right.
///
/// Java has `protected final` fields and getters. Rust struct mirrors
/// that with public accessors.
#[derive(Debug)]
pub struct BinaryExpression {
    pub left: Box<dyn Expression>,
    pub operator: Operator,
    pub right: Box<dyn Expression>,
}

impl BinaryExpression {
    pub fn new(left: Box<dyn Expression>, operator: Operator, right: Box<dyn Expression>) -> Self {
        Self { left, operator, right }
    }

    pub fn left(&self) -> &dyn Expression { self.left.as_ref() }
    pub fn right(&self) -> &dyn Expression { self.right.as_ref() }
    pub fn operator(&self) -> Operator { self.operator }
}
```

(Note: `BinaryExpression` is a *helper* struct that concrete predicate impls compose, not a trait. This deviates slightly from Java where `BinaryExpression` is an abstract class — in Rust we can't naturally express "concrete struct with virtual methods" without trait objects. The composition approach keeps the data shape identical.)

- [ ] **Step 3: Run tests; verify still pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression 2>&1 | tail -10`
Expected: previous tests still pass; nothing new since these have no behavior of their own.

- [ ] **Step 4: Commit**

```bash
git add crates/core/src/expression/leaf_expression.rs crates/core/src/expression/binary_expression.rs
git commit -m "port LeafExpression trait + BinaryExpression helper struct

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.8: Port `LiteralValue` enum + `Literal` struct + add `ExpressionKind::Literal`

**Files:**
- Modify: `crates/core/src/expression/literal.rs`
- Modify: `crates/core/src/expression/expression.rs` (add `Literal` variant to `ExpressionKind`)
- Modify: `crates/core/src/expression/mod.rs` (uncomment `pub use literal::{Literal, LiteralValue};`)

**Java source:** `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/Literal.java` (122 lines, full)

This task introduces the **first concrete Expression variant**. After this task, `ExpressionKind::_Placeholder` shrinks (we add `Literal` alongside it). The placeholder gets removed in Task 1.11 once all four variants exist.

- [ ] **Step 1: Write failing tests** at the bottom of `literal.rs`

```rust
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
            ExpressionKind::Literal(_) => {} // expected
            _ => panic!("expected ExpressionKind::Literal"),
        }
    }

    #[test]
    fn literal_operator_is_true() {
        // Java's Literal has no specific operator semantics — getOperator()
        // returns Operator.TRUE by default since LeafExpression. Rust mirrors.
        let lit = Literal::int(1);
        assert_eq!(lit.operator(), Operator::True);
    }
}
```

- [ ] **Step 2: Run tests; expect compile failure**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::literal 2>&1 | tail -10`
Expected: FAIL.

- [ ] **Step 3: Implement `LiteralValue` + `Literal`**

Replace `crates/core/src/expression/literal.rs`:

```rust
// crates/core/src/expression/literal.rs
/* … license header … */

//! Mirrors Java `org.apache.hudi.expression.Literal`.
//!
//! Diverges from Java in one place: Java `Literal<T>` is generic over T;
//! Rust uses a closed `LiteralValue` enum so callers extracting values
//! via `kind()` can pattern-match without a second downcast. See the
//! keyFilterOpt design spec §6 deviation #1.

use crate::expression::expression::ExpressionKind;
use crate::expression::leaf_expression::LeafExpression;
use crate::expression::{Expression, Operator};
use crate::internal_schema::types::*;
use crate::internal_schema::Type;

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
    Date(i32),                              // days since epoch
    Time(i64),                              // micros since midnight
    TimeMillis(i32),                        // millis since midnight
    Timestamp(i64),                         // micros since epoch
    TimestampMillis(i64),                   // millis since epoch
    LocalTimestampMillis(i64),
    LocalTimestampMicros(i64),
    Decimal { unscaled: i128, precision: u8, scale: u8 },
    String(String),
    Binary(Vec<u8>),
    Fixed(Vec<u8>),
    UUID([u8; 16]),
    Null,
}

/// Mirrors Java `Literal<T>(T value, Type type)` — non-generic in Rust.
#[derive(Debug, Clone)]
pub struct Literal {
    value: LiteralValue,
    data_type: Box<dyn Type>,
}

impl Literal {
    pub fn new(value: LiteralValue, data_type: Box<dyn Type>) -> Self {
        Self { value, data_type }
    }

    /// Mirrors Java `Literal.getValue()` (returns the carried value enum).
    pub fn value(&self) -> &LiteralValue { &self.value }

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
    fn data_type(&self) -> &dyn Type { self.data_type.as_ref() }

    /// Java `Literal.getOperator()` is inherited from LeafExpression which
    /// in turn returns `Operator.TRUE` (Java has no specific literal op).
    fn operator(&self) -> Operator { Operator::True }

    fn eval(&self, _data: Option<&dyn crate::expression::struct_like::StructLike>)
        -> Option<Box<dyn std::any::Any>> {
        // Mirrors Java: `Literal.eval(data)` returns the carried value
        // ignoring `data`. We box-clone the LiteralValue.
        Some(Box::new(self.value.clone()))
    }

    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::Literal(self)
    }
}

impl LeafExpression for Literal {}
```

- [ ] **Step 4: Add `Literal` variant to `ExpressionKind`** in `expression.rs`

Edit `crates/core/src/expression/expression.rs`. Replace the `ExpressionKind` enum body with:

```rust
pub enum ExpressionKind<'a> {
    Literal(&'a crate::expression::literal::Literal),
    // Future variants (added in subsequent tasks):
    //   NameReference(&'a NameReference),       (Task 1.9)
    //   BoundReference(&'a BoundReference),     (Task 1.10)
    //   Predicate(&'a dyn crate::expression::Predicate),  (Task 1.11)
    /// Placeholder so the enum can grow until all variants exist. Removed in Task 1.11.
    _Placeholder(std::marker::PhantomData<&'a ()>),
}
```

- [ ] **Step 5: Uncomment `pub use` in `expression/mod.rs`**

```rust
pub use literal::{Literal, LiteralValue};
```

- [ ] **Step 6: Run tests; verify pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::literal 2>&1 | tail -10`
Expected: 4 tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/core/src/expression
git commit -m "port Literal + LiteralValue + add ExpressionKind::Literal variant

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.9: Port `NameReference` + add `ExpressionKind::NameReference`

**Files:**
- Modify: `crates/core/src/expression/name_reference.rs`
- Modify: `crates/core/src/expression/expression.rs` (add `NameReference` variant to `ExpressionKind`)
- Modify: `crates/core/src/expression/mod.rs` (uncomment `pub use name_reference::NameReference;`)

**Java source:** `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/NameReference.java` (49 lines, full)

- [ ] **Step 1: Write failing tests** at the bottom of `name_reference.rs`

```rust
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
```

- [ ] **Step 2: Run tests; expect compile failure**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::name_reference 2>&1 | tail -10`
Expected: FAIL.

- [ ] **Step 3: Implement `NameReference`**

Replace `crates/core/src/expression/name_reference.rs`:

```rust
// crates/core/src/expression/name_reference.rs
/* … license header … */

//! Mirrors Java `org.apache.hudi.expression.NameReference`.

use crate::expression::expression::ExpressionKind;
use crate::expression::leaf_expression::LeafExpression;
use crate::expression::{Expression, Operator};
use crate::internal_schema::types::StringType;
use crate::internal_schema::Type;

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

    pub fn name(&self) -> &str { &self.name }
}

impl Expression for NameReference {
    fn data_type(&self) -> &dyn Type {
        // Java returns Types.StringType.get() as a placeholder until
        // BindVisitor resolves the real type. Rust mirrors via a
        // `'static` reference held by the struct's static `BIND_PLACEHOLDER`.
        // For simplicity we return a fresh StringType — same observable
        // behaviour (Java's `Types.StringType.get()` is also a singleton).
        static TYPE: StringType = StringType;
        &TYPE
    }

    fn operator(&self) -> Operator { Operator::True }

    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::NameReference(self)
    }
}

impl LeafExpression for NameReference {}
```

- [ ] **Step 4: Add `NameReference` variant to `ExpressionKind`** in `expression.rs`

Replace the `ExpressionKind` enum:

```rust
pub enum ExpressionKind<'a> {
    Literal(&'a crate::expression::literal::Literal),
    NameReference(&'a crate::expression::name_reference::NameReference),
    // Future variants:
    //   BoundReference(&'a BoundReference),     (Task 1.10)
    //   Predicate(&'a dyn crate::expression::Predicate),  (Task 1.11)
    _Placeholder(std::marker::PhantomData<&'a ()>),
}
```

- [ ] **Step 5: Uncomment `pub use` in `expression/mod.rs`**

```rust
pub use name_reference::NameReference;
```

- [ ] **Step 6: Run tests; verify pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::name_reference 2>&1 | tail -10`
Expected: 2 tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/core/src/expression
git commit -m "port NameReference + add ExpressionKind::NameReference variant

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.10: Port `BoundReference` + add `ExpressionKind::BoundReference`

**Files:**
- Modify: `crates/core/src/expression/bound_reference.rs`
- Modify: `crates/core/src/expression/expression.rs` (add `BoundReference` variant to `ExpressionKind`)

**Java source:** `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/BoundReference.java` (53 lines, full)

- [ ] **Step 1: Write failing tests** at the bottom of `bound_reference.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{Expression, ExpressionKind};
    use crate::internal_schema::types::IntType;
    use crate::internal_schema::Type;

    #[test]
    fn bound_reference_carries_ordinal_and_type() {
        let br = BoundReference::new(3, Box::new(IntType::get()));
        assert_eq!(br.ordinal(), 3);
        assert_eq!(br.data_type().type_id(), crate::internal_schema::TypeID::Int);
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
```

- [ ] **Step 2: Run tests; expect compile failure**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::bound_reference 2>&1 | tail -10`
Expected: FAIL.

- [ ] **Step 3: Implement `BoundReference`**

Replace `crates/core/src/expression/bound_reference.rs`:

```rust
// crates/core/src/expression/bound_reference.rs
/* … license header … */

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

    pub fn ordinal(&self) -> usize { self.ordinal }
}

impl Expression for BoundReference {
    fn data_type(&self) -> &dyn Type { self.data_type.as_ref() }

    fn operator(&self) -> Operator { Operator::True }

    /// Mirrors Java `BoundReference.eval(data)` — returns the cell value
    /// at `ordinal`.
    fn eval(&self, data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        let s = data?;
        let _ = s.get(self.ordinal)?;
        // The borrow can't escape; we return None for now — eval is not
        // exercised on the keyFilterOpt path. To enable real eval we'd
        // need an owned getter on StructLike.
        None
    }

    fn kind(&self) -> ExpressionKind<'_> {
        ExpressionKind::BoundReference(self)
    }
}

impl LeafExpression for BoundReference {}
```

- [ ] **Step 4: Add `BoundReference` variant to `ExpressionKind`**

```rust
pub enum ExpressionKind<'a> {
    Literal(&'a crate::expression::literal::Literal),
    NameReference(&'a crate::expression::name_reference::NameReference),
    BoundReference(&'a crate::expression::bound_reference::BoundReference),
    // Future variants:
    //   Predicate(&'a dyn crate::expression::Predicate),  (Task 1.11)
    _Placeholder(std::marker::PhantomData<&'a ()>),
}
```

- [ ] **Step 5: Run tests; verify pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::bound_reference 2>&1 | tail -10`
Expected: 2 tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/core/src/expression
git commit -m "port BoundReference + add ExpressionKind::BoundReference variant

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.11: Port `Predicate` trait + `PredicateKind` enum + remove `ExpressionKind::_Placeholder`

**Files:**
- Modify: `crates/core/src/expression/predicate.rs`
- Modify: `crates/core/src/expression/expression.rs` (add `Predicate` variant; remove `_Placeholder`)
- Modify: `crates/core/src/expression/mod.rs` (uncomment `pub use predicate::{Predicate, PredicateKind};`)

**Java source:** `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/Predicate.java` (40 lines, full)

`PredicateKind<'a>` references the 12 Predicate concrete types from `predicates.rs` which are *not yet defined*. We use the same incremental approach: define `PredicateKind` here with **all 12 variants from the start**, but each variant references a struct in `predicates.rs` that will be created in Tasks 1.12-1.14. Until those tasks land, this task uses a placeholder unit-variant `Pending(std::marker::PhantomData<&'a ()>)`. We remove the placeholder and add real variants in Task 1.14.

Actually — to avoid 4 incremental edits, the cleanest approach is: **add all 12 variants now** but stub the referenced types. That requires `predicates.rs` to define empty struct stubs. Simpler: **declare `PredicateKind` with a single placeholder variant for now**; replace in Task 1.14 once `predicates.rs` is fully populated.

This task: define `Predicate` trait + minimal `PredicateKind` placeholder. Real variants added in Task 1.14.

- [ ] **Step 1: Write failing tests** at the bottom of `predicate.rs`

```rust
#[cfg(test)]
mod tests {
    // No tests until concrete predicates are implemented in Task 1.14.
    // This file just defines the trait + enum skeleton.
}
```

- [ ] **Step 2: Implement `Predicate` trait + `PredicateKind` skeleton**

Replace `crates/core/src/expression/predicate.rs`:

```rust
// crates/core/src/expression/predicate.rs
/* … license header … */

//! Mirrors Java `org.apache.hudi.expression.Predicate`.

use crate::expression::Expression;

/// Mirrors Java `interface Predicate extends Expression`.
///
/// Adds a `kind() -> PredicateKind<'_>` accessor for idiomatic Rust
/// pattern matching. See keyFilterOpt design spec §4.1, deviation #2.
pub trait Predicate: Expression {
    /// Borrowed enum view for pattern matching.
    fn kind_predicate(&self) -> PredicateKind<'_>;
}

/// Borrowed enum view of `Predicate` for pattern matching. Variants
/// added in Task 1.14 once concrete predicate types exist.
pub enum PredicateKind<'a> {
    // Variants added in Task 1.14:
    //   True,
    //   False,
    //   And(&'a predicates::And),
    //   Or(&'a predicates::Or),
    //   Not(&'a predicates::Not),
    //   BinaryComparison(&'a predicates::BinaryComparison),
    //   In(&'a predicates::In),
    //   IsNull(&'a predicates::IsNull),
    //   IsNotNull(&'a predicates::IsNotNull),
    //   StringStartsWith(&'a predicates::StringStartsWith),
    //   StringStartsWithAny(&'a predicates::StringStartsWithAny),
    //   StringContains(&'a predicates::StringContains),
    /// Placeholder so the enum compiles. Removed in Task 1.14.
    _Placeholder(std::marker::PhantomData<&'a ()>),
}
```

(Note: the trait method is `kind_predicate()` not `kind()` to avoid clashing with the `Expression::kind() -> ExpressionKind` already on the supertrait. Java has both `Predicate.getOperator()` (inherited) and the cast-based pattern; Rust separates them. Alternatively, we can rename `Expression::kind` to `Expression::expr_kind` — pick whichever feels less surprising.)

For consistency with the spec text which says "trait Predicate has a `kind()` accessor", we'll go with the spec name. Solution: name the supertrait method differently. Edit `Expression::kind` → `Expression::expr_kind` if conflict surfaces; or use disambiguated calls (`<Self as Expression>::kind()`). Decision: keep both as `kind()` — Rust allows it via UFCS but call sites use `<Predicate>::kind` or `<Expression>::kind`. Cleaner path: name them differently. For this task, name them:
- `Expression::kind() -> ExpressionKind`  
- `Predicate::pred_kind() -> PredicateKind`

Rename the spec accordingly later (or note this in the implementation deviation).

Actually simpler: Predicate is an Expression. So its `Expression::kind()` could return `ExpressionKind::Predicate(self)`, and its own pattern-match accessor for *predicate-specific* matching is named differently. Let's call the predicate-specific one `Predicate::pred_kind()` to avoid the name clash entirely.

Apply this naming to the trait:

```rust
pub trait Predicate: Expression {
    fn pred_kind(&self) -> PredicateKind<'_>;
}
```

Update the spec text-mention in commit message if needed.

- [ ] **Step 3: Add `Predicate` variant to `ExpressionKind`** in `expression.rs`

Replace `ExpressionKind`:

```rust
pub enum ExpressionKind<'a> {
    Literal(&'a crate::expression::literal::Literal),
    NameReference(&'a crate::expression::name_reference::NameReference),
    BoundReference(&'a crate::expression::bound_reference::BoundReference),
    Predicate(&'a dyn crate::expression::predicate::Predicate),
}
```

(The `_Placeholder` variant is now removed. All four variants exist.)

- [ ] **Step 4: Uncomment `pub use` in `expression/mod.rs`**

```rust
pub use predicate::{Predicate, PredicateKind};
```

- [ ] **Step 5: Run tests; verify still pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression 2>&1 | tail -10`
Expected: all previous tests pass; nothing new in this task.

- [ ] **Step 6: Commit**

```bash
git add crates/core/src/expression
git commit -m "port Predicate trait + PredicateKind skeleton; finalize ExpressionKind

PredicateKind variants are added in Task 1.14 once concrete predicate
types exist. ExpressionKind now has all four variants and the placeholder
is removed.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.12: Port `Predicates` — singleton predicates (TrueExpression, FalseExpression)

**Files:**
- Modify: `crates/core/src/expression/predicates.rs`

**Java source:** `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/Predicates.java` lines 96-152 (TrueExpression + FalseExpression).

The `predicates.rs` file holds all 12 inner classes from Java's `Predicates`. We split the port across Tasks 1.12-1.14 by class group. Each task adds tests + implementation + (eventually) a `PredicateKind` variant.

- [ ] **Step 1: Write failing tests** at the bottom of `predicates.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{Expression, Predicate, Operator};

    #[test]
    fn true_expression_eval_returns_true() {
        let t = TrueExpression;
        let result = t.eval(None).unwrap();
        let bool_val = result.downcast_ref::<bool>().expect("expected bool");
        assert!(bool_val);
    }

    #[test]
    fn true_expression_operator_is_true() {
        assert_eq!(TrueExpression.operator(), Operator::True);
    }

    #[test]
    fn false_expression_eval_returns_false() {
        let f = FalseExpression;
        let result = f.eval(None).unwrap();
        let bool_val = result.downcast_ref::<bool>().expect("expected bool");
        assert!(!bool_val);
    }

    #[test]
    fn false_expression_operator_is_false() {
        assert_eq!(FalseExpression.operator(), Operator::False);
    }
}
```

- [ ] **Step 2: Run tests; expect compile failure**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::predicates 2>&1 | tail -10`
Expected: FAIL.

- [ ] **Step 3: Implement `TrueExpression` + `FalseExpression`**

Replace `crates/core/src/expression/predicates.rs`:

```rust
// crates/core/src/expression/predicates.rs
/* … license header … */

//! Mirrors Java `org.apache.hudi.expression.Predicates`.
//!
//! All 12 inner classes from Java's `Predicates` live in this single file
//! to mirror Java's structure. Factory functions at the bottom mirror the
//! static methods on Java's `Predicates`.

use crate::expression::expression::ExpressionKind;
use crate::expression::leaf_expression::LeafExpression;
use crate::expression::predicate::{Predicate, PredicateKind};
use crate::expression::struct_like::StructLike;
use crate::expression::{Expression, Operator};
use crate::internal_schema::types::BooleanType;
use crate::internal_schema::Type;
use std::any::Any;

// =========================================================================
// Singleton predicates
// =========================================================================

/// Mirrors Java `Predicates.TrueExpression`.
#[derive(Debug, Clone, Copy)]
pub struct TrueExpression;

impl Expression for TrueExpression {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::True }
    fn eval(&self, _data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        Some(Box::new(true))
    }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl LeafExpression for TrueExpression {}

impl Predicate for TrueExpression {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::True }
}

/// Mirrors Java `Predicates.FalseExpression`.
#[derive(Debug, Clone, Copy)]
pub struct FalseExpression;

impl Expression for FalseExpression {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::False }
    fn eval(&self, _data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        Some(Box::new(false))
    }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl LeafExpression for FalseExpression {}

impl Predicate for FalseExpression {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::False }
}
```

(`PredicateKind::True` / `PredicateKind::False` don't exist yet — referenced here in anticipation of Task 1.14. Compilation will fail. Add them now to `predicate.rs::PredicateKind`:)

- [ ] **Step 4: Update `PredicateKind` to include True + False variants**

Edit `crates/core/src/expression/predicate.rs`. Replace the `PredicateKind` enum:

```rust
pub enum PredicateKind<'a> {
    True,
    False,
    // Future variants (Task 1.13, 1.14):
    //   And(&'a predicates::And), Or(&'a predicates::Or), Not(&'a predicates::Not),
    //   BinaryComparison(&'a predicates::BinaryComparison),
    //   In(&'a predicates::In), IsNull(&'a predicates::IsNull), IsNotNull(&'a predicates::IsNotNull),
    //   StringStartsWith(&'a predicates::StringStartsWith),
    //   StringStartsWithAny(&'a predicates::StringStartsWithAny),
    //   StringContains(&'a predicates::StringContains),
    _Placeholder(std::marker::PhantomData<&'a ()>),
}
```

- [ ] **Step 5: Run tests; verify pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::predicates 2>&1 | tail -10`
Expected: 4 tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/core/src/expression
git commit -m "port Predicates::TrueExpression + FalseExpression

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.13: Port `Predicates` — boolean combinators (And, Or, Not)

**Files:**
- Modify: `crates/core/src/expression/predicates.rs` (append)
- Modify: `crates/core/src/expression/predicate.rs` (add 3 variants to `PredicateKind`)

**Java source:** `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/Predicates.java` lines 154-224 (And + Or) and 360-387 (Not).

- [ ] **Step 1: Write failing tests** (append to existing `tests` mod in `predicates.rs`)

```rust
    #[test]
    fn and_constructs_and_carries_children() {
        let a = And::new(Box::new(TrueExpression), Box::new(FalseExpression));
        assert_eq!(a.operator(), Operator::And);
        assert_eq!(a.children().len(), 2);
    }

    #[test]
    fn or_constructs_and_carries_children() {
        let o = Or::new(Box::new(TrueExpression), Box::new(FalseExpression));
        assert_eq!(o.operator(), Operator::Or);
        assert_eq!(o.children().len(), 2);
    }

    #[test]
    fn not_carries_child() {
        let n = Not::new(Box::new(TrueExpression));
        assert_eq!(n.operator(), Operator::Not);
        assert_eq!(n.children().len(), 1);
    }

    #[test]
    fn and_eval_short_circuits_false() {
        let a = And::new(Box::new(FalseExpression), Box::new(TrueExpression));
        let result = a.eval(None).unwrap();
        let b = result.downcast_ref::<bool>().unwrap();
        assert!(!b);
    }
```

- [ ] **Step 2: Run tests; expect compile failure**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::predicates 2>&1 | tail -10`
Expected: FAIL.

- [ ] **Step 3: Implement `And`, `Or`, `Not`**

Append to `crates/core/src/expression/predicates.rs`:

```rust
// =========================================================================
// Boolean combinators
// =========================================================================

/// Mirrors Java `Predicates.And`.
#[derive(Debug)]
pub struct And {
    pub left: Box<dyn Expression>,
    pub right: Box<dyn Expression>,
}

impl And {
    pub fn new(left: Box<dyn Expression>, right: Box<dyn Expression>) -> Self {
        Self { left, right }
    }
    pub fn left(&self) -> &dyn Expression { self.left.as_ref() }
    pub fn right(&self) -> &dyn Expression { self.right.as_ref() }
}

impl Expression for And {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::And }
    fn children(&self) -> Vec<&dyn Expression> { vec![self.left.as_ref(), self.right.as_ref()] }
    fn eval(&self, data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        let left = self.left.eval(data)?;
        let lb = left.downcast_ref::<bool>().copied().unwrap_or(false);
        if !lb { return Some(Box::new(false)); }
        let right = self.right.eval(data)?;
        let rb = right.downcast_ref::<bool>().copied().unwrap_or(false);
        Some(Box::new(lb && rb))
    }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl Predicate for And {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::And(self) }
}

/// Mirrors Java `Predicates.Or`.
#[derive(Debug)]
pub struct Or {
    pub left: Box<dyn Expression>,
    pub right: Box<dyn Expression>,
}

impl Or {
    pub fn new(left: Box<dyn Expression>, right: Box<dyn Expression>) -> Self {
        Self { left, right }
    }
    pub fn left(&self) -> &dyn Expression { self.left.as_ref() }
    pub fn right(&self) -> &dyn Expression { self.right.as_ref() }
}

impl Expression for Or {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::Or }
    fn children(&self) -> Vec<&dyn Expression> { vec![self.left.as_ref(), self.right.as_ref()] }
    fn eval(&self, data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        let left = self.left.eval(data)?;
        let lb = left.downcast_ref::<bool>().copied().unwrap_or(false);
        if lb { return Some(Box::new(true)); }
        let right = self.right.eval(data)?;
        let rb = right.downcast_ref::<bool>().copied().unwrap_or(false);
        Some(Box::new(lb || rb))
    }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl Predicate for Or {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::Or(self) }
}

/// Mirrors Java `Predicates.Not`.
#[derive(Debug)]
pub struct Not {
    pub child: Box<dyn Expression>,
}

impl Not {
    pub fn new(child: Box<dyn Expression>) -> Self { Self { child } }
    pub fn child(&self) -> &dyn Expression { self.child.as_ref() }
}

impl Expression for Not {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::Not }
    fn children(&self) -> Vec<&dyn Expression> { vec![self.child.as_ref()] }
    fn eval(&self, data: Option<&dyn StructLike>) -> Option<Box<dyn Any>> {
        let v = self.child.eval(data)?;
        let b = v.downcast_ref::<bool>().copied().unwrap_or(false);
        Some(Box::new(!b))
    }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl Predicate for Not {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::Not(self) }
}
```

- [ ] **Step 4: Add 3 variants to `PredicateKind`** in `predicate.rs`

```rust
pub enum PredicateKind<'a> {
    True,
    False,
    And(&'a crate::expression::predicates::And),
    Or(&'a crate::expression::predicates::Or),
    Not(&'a crate::expression::predicates::Not),
    // Future variants (Task 1.14):
    //   BinaryComparison(...), In(...), IsNull(...), IsNotNull(...),
    //   StringStartsWith(...), StringStartsWithAny(...), StringContains(...)
    _Placeholder(std::marker::PhantomData<&'a ()>),
}
```

- [ ] **Step 5: Run tests; verify pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::predicates 2>&1 | tail -10`
Expected: 8 tests pass (4 from Task 1.12 + 4 new).

- [ ] **Step 6: Commit**

```bash
git add crates/core/src/expression
git commit -m "port Predicates::And/Or/Not + 3 PredicateKind variants

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.14: Port `Predicates` — comparison + null + string predicates (BinaryComparison, In, IsNull, IsNotNull, StringStartsWith, StringStartsWithAny, StringContains) + factories

**Files:**
- Modify: `crates/core/src/expression/predicates.rs` (append)
- Modify: `crates/core/src/expression/predicate.rs` (replace `PredicateKind` with full 12-variant enum, remove `_Placeholder`)

**Java source:** `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/Predicates.java` lines 226-456 (StringStartsWith, StringContains, In, IsNull, IsNotNull, BinaryComparison, StringStartsWithAny) plus the static factory methods on lines 30-94.

This is the largest single task in Phase 1 — 7 new predicate types plus factory functions plus the final PredicateKind shape.

- [ ] **Step 1: Write failing tests** (append to existing `tests` mod in `predicates.rs`)

```rust
    use crate::expression::Literal;

    #[test]
    fn binary_comparison_eq() {
        let bc = BinaryComparison::new(
            Box::new(Literal::int(5)), Operator::Eq, Box::new(Literal::int(5)));
        assert_eq!(bc.operator(), Operator::Eq);
    }

    #[test]
    fn in_predicate_carries_children() {
        let val = Box::new(Literal::string("col"));
        let validvalues = vec![
            Box::new(Literal::string("a")) as Box<dyn Expression>,
            Box::new(Literal::string("b")) as Box<dyn Expression>,
        ];
        let p = In::new(val, validvalues);
        assert_eq!(p.operator(), Operator::In);
        assert_eq!(p.right_children().len(), 2);
        assert_eq!(p.children().len(), 3); // value + 2 valid values
    }

    #[test]
    fn is_null_carries_child() {
        let p = IsNull::new(Box::new(Literal::int(1)));
        assert_eq!(p.operator(), Operator::IsNull);
    }

    #[test]
    fn is_not_null_carries_child() {
        let p = IsNotNull::new(Box::new(Literal::int(1)));
        assert_eq!(p.operator(), Operator::IsNotNull);
    }

    #[test]
    fn string_starts_with_carries_left_right() {
        let p = StringStartsWith::new(
            Box::new(Literal::string("name")), Box::new(Literal::string("Al")));
        assert_eq!(p.operator(), Operator::StartsWith);
    }

    #[test]
    fn string_starts_with_any_carries_children() {
        let p = StringStartsWithAny::new(
            Box::new(Literal::string("name")),
            vec![
                Box::new(Literal::string("Al")) as Box<dyn Expression>,
                Box::new(Literal::string("Bo")) as Box<dyn Expression>,
            ]
        );
        assert_eq!(p.operator(), Operator::StartsWith);
        assert_eq!(p.right_children().len(), 2);
    }

    #[test]
    fn string_contains_constructs() {
        let p = StringContains::new(
            Box::new(Literal::string("desc")), Box::new(Literal::string("foo")));
        assert_eq!(p.operator(), Operator::Contains);
    }

    // Factory functions
    #[test]
    fn factory_in_constructs_in() {
        let p = predicates_factory::in_(
            Box::new(Literal::string("col")),
            vec![Box::new(Literal::string("a"))],
        );
        assert_eq!(p.operator(), Operator::In);
    }

    #[test]
    fn factory_starts_with_any_constructs_string_starts_with_any() {
        let p = predicates_factory::starts_with_any(
            Box::new(Literal::string("col")),
            vec![Box::new(Literal::string("a"))],
        );
        assert_eq!(p.operator(), Operator::StartsWith);
    }
```

- [ ] **Step 2: Run tests; expect compile failure**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::predicates 2>&1 | tail -10`
Expected: FAIL.

- [ ] **Step 3: Implement `BinaryComparison`, `In`, `IsNull`, `IsNotNull`, `StringStartsWith`, `StringStartsWithAny`, `StringContains`**

Append to `crates/core/src/expression/predicates.rs`:

```rust
// =========================================================================
// Comparison + null + string predicates
// =========================================================================

/// Mirrors Java `Predicates.BinaryComparison`.
#[derive(Debug)]
pub struct BinaryComparison {
    pub left: Box<dyn Expression>,
    pub op: Operator,
    pub right: Box<dyn Expression>,
}

impl BinaryComparison {
    pub fn new(left: Box<dyn Expression>, op: Operator, right: Box<dyn Expression>) -> Self {
        Self { left, op, right }
    }
    pub fn left(&self) -> &dyn Expression { self.left.as_ref() }
    pub fn right(&self) -> &dyn Expression { self.right.as_ref() }
}

impl Expression for BinaryComparison {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { self.op }
    fn children(&self) -> Vec<&dyn Expression> { vec![self.left.as_ref(), self.right.as_ref()] }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl Predicate for BinaryComparison {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::BinaryComparison(self) }
}

/// Mirrors Java `Predicates.In`.
#[derive(Debug)]
pub struct In {
    pub value: Box<dyn Expression>,
    pub valid_values: Vec<Box<dyn Expression>>,
}

impl In {
    pub fn new(value: Box<dyn Expression>, valid_values: Vec<Box<dyn Expression>>) -> Self {
        Self { value, valid_values }
    }
    pub fn value(&self) -> &dyn Expression { self.value.as_ref() }
    pub fn right_children(&self) -> &[Box<dyn Expression>] { &self.valid_values }
}

impl Expression for In {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::In }
    fn children(&self) -> Vec<&dyn Expression> {
        let mut all = vec![self.value.as_ref()];
        all.extend(self.valid_values.iter().map(|e| e.as_ref()));
        all
    }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl Predicate for In {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::In(self) }
}

/// Mirrors Java `Predicates.IsNull`.
#[derive(Debug)]
pub struct IsNull {
    pub child: Box<dyn Expression>,
}

impl IsNull {
    pub fn new(child: Box<dyn Expression>) -> Self { Self { child } }
    pub fn child(&self) -> &dyn Expression { self.child.as_ref() }
}

impl Expression for IsNull {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::IsNull }
    fn children(&self) -> Vec<&dyn Expression> { vec![self.child.as_ref()] }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl Predicate for IsNull {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::IsNull(self) }
}

/// Mirrors Java `Predicates.IsNotNull`.
#[derive(Debug)]
pub struct IsNotNull {
    pub child: Box<dyn Expression>,
}

impl IsNotNull {
    pub fn new(child: Box<dyn Expression>) -> Self { Self { child } }
    pub fn child(&self) -> &dyn Expression { self.child.as_ref() }
}

impl Expression for IsNotNull {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::IsNotNull }
    fn children(&self) -> Vec<&dyn Expression> { vec![self.child.as_ref()] }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl Predicate for IsNotNull {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::IsNotNull(self) }
}

/// Mirrors Java `Predicates.StringStartsWith`.
#[derive(Debug)]
pub struct StringStartsWith {
    pub left: Box<dyn Expression>,
    pub right: Box<dyn Expression>,
}

impl StringStartsWith {
    pub fn new(left: Box<dyn Expression>, right: Box<dyn Expression>) -> Self {
        Self { left, right }
    }
    pub fn left(&self) -> &dyn Expression { self.left.as_ref() }
    pub fn right(&self) -> &dyn Expression { self.right.as_ref() }
}

impl Expression for StringStartsWith {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::StartsWith }
    fn children(&self) -> Vec<&dyn Expression> { vec![self.left.as_ref(), self.right.as_ref()] }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl Predicate for StringStartsWith {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::StringStartsWith(self) }
}

/// Mirrors Java `Predicates.StringStartsWithAny`.
#[derive(Debug)]
pub struct StringStartsWithAny {
    pub left: Box<dyn Expression>,
    pub right: Vec<Box<dyn Expression>>,
}

impl StringStartsWithAny {
    pub fn new(left: Box<dyn Expression>, right: Vec<Box<dyn Expression>>) -> Self {
        Self { left, right }
    }
    pub fn left(&self) -> &dyn Expression { self.left.as_ref() }
    pub fn right_children(&self) -> &[Box<dyn Expression>] { &self.right }
}

impl Expression for StringStartsWithAny {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::StartsWith }
    fn children(&self) -> Vec<&dyn Expression> {
        let mut all = vec![self.left.as_ref()];
        all.extend(self.right.iter().map(|e| e.as_ref()));
        all
    }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl Predicate for StringStartsWithAny {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::StringStartsWithAny(self) }
}

/// Mirrors Java `Predicates.StringContains`.
#[derive(Debug)]
pub struct StringContains {
    pub left: Box<dyn Expression>,
    pub right: Box<dyn Expression>,
}

impl StringContains {
    pub fn new(left: Box<dyn Expression>, right: Box<dyn Expression>) -> Self {
        Self { left, right }
    }
    pub fn left(&self) -> &dyn Expression { self.left.as_ref() }
    pub fn right(&self) -> &dyn Expression { self.right.as_ref() }
}

impl Expression for StringContains {
    fn data_type(&self) -> &dyn Type { static T: BooleanType = BooleanType; &T }
    fn operator(&self) -> Operator { Operator::Contains }
    fn children(&self) -> Vec<&dyn Expression> { vec![self.left.as_ref(), self.right.as_ref()] }
    fn kind(&self) -> ExpressionKind<'_> { ExpressionKind::Predicate(self) }
}

impl Predicate for StringContains {
    fn pred_kind(&self) -> PredicateKind<'_> { PredicateKind::StringContains(self) }
}

// =========================================================================
// Factory functions — mirror Java `Predicates` static methods.
//
// `in` is a Rust keyword so the function is named `in_`.
// =========================================================================

pub mod predicates_factory {
    use super::*;

    pub fn always_true() -> TrueExpression { TrueExpression }
    pub fn always_false() -> FalseExpression { FalseExpression }
    pub fn and(left: Box<dyn Expression>, right: Box<dyn Expression>) -> And { And::new(left, right) }
    pub fn or(left: Box<dyn Expression>, right: Box<dyn Expression>) -> Or { Or::new(left, right) }
    pub fn not(child: Box<dyn Expression>) -> Not { Not::new(child) }
    pub fn gt(left: Box<dyn Expression>, right: Box<dyn Expression>) -> BinaryComparison {
        BinaryComparison::new(left, Operator::Gt, right)
    }
    pub fn lt(left: Box<dyn Expression>, right: Box<dyn Expression>) -> BinaryComparison {
        BinaryComparison::new(left, Operator::Lt, right)
    }
    pub fn eq(left: Box<dyn Expression>, right: Box<dyn Expression>) -> BinaryComparison {
        BinaryComparison::new(left, Operator::Eq, right)
    }
    pub fn gteq(left: Box<dyn Expression>, right: Box<dyn Expression>) -> BinaryComparison {
        BinaryComparison::new(left, Operator::GtEq, right)
    }
    pub fn lteq(left: Box<dyn Expression>, right: Box<dyn Expression>) -> BinaryComparison {
        BinaryComparison::new(left, Operator::LtEq, right)
    }
    pub fn starts_with(left: Box<dyn Expression>, right: Box<dyn Expression>) -> StringStartsWith {
        StringStartsWith::new(left, right)
    }
    pub fn contains(left: Box<dyn Expression>, right: Box<dyn Expression>) -> StringContains {
        StringContains::new(left, right)
    }
    pub fn in_(value: Box<dyn Expression>, valid_values: Vec<Box<dyn Expression>>) -> In {
        In::new(value, valid_values)
    }
    pub fn is_null(child: Box<dyn Expression>) -> IsNull { IsNull::new(child) }
    pub fn is_not_null(child: Box<dyn Expression>) -> IsNotNull { IsNotNull::new(child) }
    pub fn starts_with_any(left: Box<dyn Expression>, right: Vec<Box<dyn Expression>>) -> StringStartsWithAny {
        StringStartsWithAny::new(left, right)
    }
}
```

- [ ] **Step 4: Replace `PredicateKind` with full 12-variant enum (no placeholder)** in `predicate.rs`

```rust
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
```

(The `_Placeholder` variant is now removed.)

- [ ] **Step 5: Run tests; verify pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::predicates 2>&1 | tail -10`
Expected: 18 tests pass (8 from prior tasks + 10 new).

- [ ] **Step 6: Commit**

```bash
git add crates/core/src/expression
git commit -m "port Predicates::{BinaryComparison,In,IsNull,IsNotNull,StringStartsWith*,StringContains} + factories

Finalizes PredicateKind to 12 variants. All Predicates inner classes
from Java's Predicates.java are now ported.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.15: Port `Comparators`

**Files:**
- Modify: `crates/core/src/expression/comparators.rs`

**Java source:** `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/Comparators.java` (55 lines, full)

- [ ] **Step 1: Write failing tests** at the bottom of `comparators.rs`

```rust
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
        assert_eq!(cmp(&LiteralValue::String("a".into()), &LiteralValue::String("b".into())), Ordering::Less);
    }

    #[test]
    fn boolean_comparator() {
        let cmp = Comparators::for_type(&BooleanType::get());
        assert_eq!(cmp(&LiteralValue::Bool(false), &LiteralValue::Bool(true)), Ordering::Less);
    }
}
```

- [ ] **Step 2: Run tests; expect compile failure**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::comparators 2>&1 | tail -10`
Expected: FAIL.

- [ ] **Step 3: Implement `Comparators`**

Replace `crates/core/src/expression/comparators.rs`:

```rust
// crates/core/src/expression/comparators.rs
/* … license header … */

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
                (LiteralValue::Float(x), LiteralValue::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
                _ => Ordering::Equal,
            },
            TypeID::Double => |a, b| match (a, b) {
                (LiteralValue::Double(x), LiteralValue::Double(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
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
```

- [ ] **Step 4: Run tests; verify pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression::comparators 2>&1 | tail -10`
Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/core/src/expression/comparators.rs
git commit -m "port Comparators

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.16: Port `ExpressionVisitor`, `BindVisitor`, `PartialBindVisitor`

**Files:**
- Modify: `crates/core/src/expression/expression_visitor.rs`
- Modify: `crates/core/src/expression/bind_visitor.rs`
- Modify: `crates/core/src/expression/partial_bind_visitor.rs`

**Java sources:**
- `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/ExpressionVisitor.java` (41 lines, full)
- `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/BindVisitor.java` (187 lines)
- `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/expression/PartialBindVisitor.java` (153 lines)

These are ported but **not wired into the reader path** (deviation #6 in spec §6). Since they're not used by keyFilterOpt's runtime path, our test bar is "they compile, basic construction works". A full implementation of bind logic mirrors Java behavior but is dead code in our scope.

- [ ] **Step 1: Implement `ExpressionVisitor` trait**

Replace `crates/core/src/expression/expression_visitor.rs`:

```rust
// crates/core/src/expression/expression_visitor.rs
/* … license header … */

//! Mirrors Java `org.apache.hudi.expression.ExpressionVisitor`.

use crate::expression::predicates::{
    And, BinaryComparison, FalseExpression, In, IsNotNull, IsNull, Not, Or, StringContains,
    StringStartsWith, StringStartsWithAny, TrueExpression,
};
use crate::expression::{Expression, Literal, NameReference};
use crate::expression::bound_reference::BoundReference;

/// Mirrors Java `ExpressionVisitor<T>`. Each visit method has a default
/// returning `T::default()` (Rust constraint: T: Default).
pub trait ExpressionVisitor<T> {
    fn always_true(&mut self) -> T where T: Default { T::default() }
    fn always_false(&mut self) -> T where T: Default { T::default() }
    fn visit_and(&mut self, _and: &And) -> T where T: Default { T::default() }
    fn visit_or(&mut self, _or: &Or) -> T where T: Default { T::default() }
    fn visit_not(&mut self, _not: &Not) -> T where T: Default { T::default() }
    fn visit_predicate(&mut self, _expr: &dyn Expression) -> T where T: Default { T::default() }
    fn visit_literal(&mut self, _lit: &Literal) -> T where T: Default { T::default() }
    fn visit_name_reference(&mut self, _nr: &NameReference) -> T where T: Default { T::default() }
    fn visit_bound_reference(&mut self, _br: &BoundReference) -> T where T: Default { T::default() }
    fn visit_binary_comparison(&mut self, _bc: &BinaryComparison) -> T where T: Default { T::default() }
    fn visit_in(&mut self, _in_: &In) -> T where T: Default { T::default() }
    fn visit_is_null(&mut self, _p: &IsNull) -> T where T: Default { T::default() }
    fn visit_is_not_null(&mut self, _p: &IsNotNull) -> T where T: Default { T::default() }
    fn visit_string_starts_with(&mut self, _p: &StringStartsWith) -> T where T: Default { T::default() }
    fn visit_string_starts_with_any(&mut self, _p: &StringStartsWithAny) -> T where T: Default { T::default() }
    fn visit_string_contains(&mut self, _p: &StringContains) -> T where T: Default { T::default() }
    fn visit_true(&mut self, _t: &TrueExpression) -> T where T: Default { T::default() }
    fn visit_false(&mut self, _f: &FalseExpression) -> T where T: Default { T::default() }
}
```

- [ ] **Step 2: Implement `BindVisitor` (stub)**

Replace `crates/core/src/expression/bind_visitor.rs`:

```rust
// crates/core/src/expression/bind_visitor.rs
/* … license header … */

//! Mirrors Java `org.apache.hudi.expression.BindVisitor`.
//!
//! BindVisitor walks an expression tree, replacing `NameReference`s with
//! `BoundReference`s by looking up names in a `RecordType` schema.
//!
//! Ported as a placeholder — not wired into the keyFilterOpt reader path.
//! See keyFilterOpt design spec §6 deviation #6.

use crate::expression::ExpressionVisitor;
use crate::internal_schema::types::RecordType;

pub struct BindVisitor {
    pub schema: RecordType,
    pub case_sensitive: bool,
}

impl BindVisitor {
    pub fn new(schema: RecordType, case_sensitive: bool) -> Self {
        Self { schema, case_sensitive }
    }
}

// Minimal ExpressionVisitor impl — defaults return Box::<dyn Expression>::default
// which doesn't compile, so we'll need a wrapper. For now, BindVisitor is a
// declared-but-unused type. Full implementation is deferred until the
// keyFilterOpt scope expands to need binding. Spec §6 deviation #6.
impl<T: Default> ExpressionVisitor<T> for BindVisitor {}
```

- [ ] **Step 3: Implement `PartialBindVisitor` (stub)**

Replace `crates/core/src/expression/partial_bind_visitor.rs`:

```rust
// crates/core/src/expression/partial_bind_visitor.rs
/* … license header … */

//! Mirrors Java `org.apache.hudi.expression.PartialBindVisitor`.
//!
//! Like BindVisitor but tolerates unresolvable NameReferences. Ported as
//! placeholder; not wired. Spec §6 deviation #6.

use crate::expression::ExpressionVisitor;
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
```

- [ ] **Step 4: Run tests; verify still pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib expression 2>&1 | tail -10`
Expected: all previous tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/core/src/expression
git commit -m "port ExpressionVisitor + BindVisitor + PartialBindVisitor (stubs)

Visitors are ported as declarations for hierarchy completeness. The bind
logic is not wired into the reader path — see spec §6 deviation #6.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.17: Phase 1 verification — full crate test pass + lint

**Files:** None modified — verification only.

- [ ] **Step 1: Run full crate test suite**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core 2>&1 | tail -15`
Expected: all tests pass. No regressions to existing tests.

- [ ] **Step 2: Run clippy**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo clippy -p hudi-core --all-targets 2>&1 | tail -30`
Expected: no errors. Address any warnings introduced by the new modules.

- [ ] **Step 3: Build the workspace**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo build --workspace 2>&1 | tail -10`
Expected: clean build, no warnings introduced by Phase 1.

- [ ] **Step 4: If lint or build issues found, fix and commit**

```bash
git add -A
git commit -m "phase 1: clippy fixes / lint cleanup

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

If no issues, skip the commit.

---

## Phase 2 — Wire keyFilterOpt through ReaderContext + reader

### Task 2.1: Add `KeySpec` enum + `create_key_spec` + `string_literals` helper

**Files:**
- Create: `crates/core/src/file_group/reader/key_spec.rs`
- Modify: `crates/core/src/file_group/reader/mod.rs:66-81` (add `pub mod key_spec;` to module list)

**Java source:** `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/common/table/log/HoodieMergedLogRecordReader.java` lines 109-126 (`createKeySpec`).

- [ ] **Step 1: Write failing tests**

Create `crates/core/src/file_group/reader/key_spec.rs`:

```rust
// crates/core/src/file_group/reader/key_spec.rs
/* … license header … */

//! Mirrors Java `org.apache.hudi.common.table.log.KeySpec` hierarchy
//! (`KeySpec`, `FullKeySpec`, `PrefixKeySpec`) plus
//! `HoodieMergedLogRecordReader.createKeySpec(Option<Predicate>)`.

use crate::expression::predicate::{Predicate, PredicateKind};
use crate::expression::{Expression, ExpressionKind, LiteralValue};
use std::sync::Arc;

/// Mirrors Java's `KeySpec` interface, with `FullKeySpec` and `PrefixKeySpec`
/// flattened into enum variants.
#[derive(Debug, Clone)]
pub enum KeySpec {
    FullKeys(Vec<String>),
    PrefixKeys(Vec<String>),
}

impl KeySpec {
    /// Test the given record-key string against this key-spec.
    pub fn matches(&self, key: &str) -> bool {
        match self {
            KeySpec::FullKeys(keys) => keys.iter().any(|k| k == key),
            KeySpec::PrefixKeys(prefixes) => prefixes.iter().any(|p| key.starts_with(p.as_str())),
        }
    }
}

/// Mirrors Java `HoodieMergedLogRecordReader.createKeySpec(Option<Predicate>)`,
/// implemented in idiomatic Rust via `PredicateKind` pattern matching instead
/// of Java's `instanceof`/cast.
pub fn create_key_spec(filter: Option<&dyn Predicate>) -> Option<KeySpec> {
    match filter?.pred_kind() {
        PredicateKind::In(p) => Some(KeySpec::FullKeys(string_literals(p.right_children())?)),
        PredicateKind::StringStartsWithAny(p) => {
            Some(KeySpec::PrefixKeys(string_literals(p.right_children())?))
        }
        _ => None,
    }
}

/// Extract `String` values from a list of `Literal(LiteralValue::String(_))`
/// expressions. Returns `None` if any child is not a String literal.
pub(crate) fn string_literals(exprs: &[Box<dyn Expression>]) -> Option<Vec<String>> {
    exprs
        .iter()
        .map(|e| match e.kind() {
            ExpressionKind::Literal(lit) => match lit.value() {
                LiteralValue::String(s) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// Convenience: accept an `Option<Arc<dyn Predicate>>` directly (the type
/// stored on `ReaderContext`).
pub fn create_key_spec_from_arc(filter: &Option<Arc<dyn Predicate>>) -> Option<KeySpec> {
    create_key_spec(filter.as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::predicates::predicates_factory;
    use crate::expression::{Literal, NameReference};

    #[test]
    fn create_key_spec_none_when_filter_is_none() {
        assert!(create_key_spec(None).is_none());
    }

    #[test]
    fn create_key_spec_in_predicate_yields_full_keys() {
        let p = predicates_factory::in_(
            Box::new(NameReference::new("_hoodie_record_key")),
            vec![
                Box::new(Literal::string("k1")) as Box<dyn Expression>,
                Box::new(Literal::string("k2")) as Box<dyn Expression>,
            ],
        );
        let spec = create_key_spec(Some(&p)).expect("expected Some(KeySpec)");
        match spec {
            KeySpec::FullKeys(keys) => assert_eq!(keys, vec!["k1".to_string(), "k2".to_string()]),
            _ => panic!("expected FullKeys"),
        }
    }

    #[test]
    fn create_key_spec_starts_with_any_yields_prefix_keys() {
        let p = predicates_factory::starts_with_any(
            Box::new(NameReference::new("_hoodie_record_key")),
            vec![
                Box::new(Literal::string("alpha")) as Box<dyn Expression>,
                Box::new(Literal::string("beta")) as Box<dyn Expression>,
            ],
        );
        let spec = create_key_spec(Some(&p)).expect("expected Some(KeySpec)");
        match spec {
            KeySpec::PrefixKeys(prefixes) => {
                assert_eq!(prefixes, vec!["alpha".to_string(), "beta".to_string()])
            }
            _ => panic!("expected PrefixKeys"),
        }
    }

    #[test]
    fn create_key_spec_returns_none_for_non_in_non_starts_with() {
        let p = predicates_factory::eq(
            Box::new(Literal::string("a")),
            Box::new(Literal::string("b")),
        );
        assert!(create_key_spec(Some(&p)).is_none());
    }

    #[test]
    fn full_keys_matches() {
        let spec = KeySpec::FullKeys(vec!["a".into(), "b".into()]);
        assert!(spec.matches("a"));
        assert!(spec.matches("b"));
        assert!(!spec.matches("c"));
    }

    #[test]
    fn prefix_keys_matches() {
        let spec = KeySpec::PrefixKeys(vec!["pre".into(), "alpha".into()]);
        assert!(spec.matches("prefix-1"));
        assert!(spec.matches("alphabet"));
        assert!(!spec.matches("beta"));
    }
}
```

- [ ] **Step 2: Add `pub mod key_spec;`** to `crates/core/src/file_group/reader/mod.rs`

Find the module declarations at lines 66-81 and insert alphabetically:

```rust
pub mod buffer;
pub mod buffered_record;
pub mod buffered_record_converter;
pub mod delete_context;
pub mod input_split;
pub mod iterator_mode;
pub mod key_spec;        // ← NEW
pub mod log_record_reader;
pub mod merged_log_record_reader;
pub mod output_converter;
pub mod read_stats;
pub mod reader_context;
pub mod reader_parameters;
pub mod record_context;
pub mod record_merger;
pub mod schema_handler;
pub mod update_processor;
```

- [ ] **Step 3: Run tests; expect pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib file_group::reader::key_spec 2>&1 | tail -10`
Expected: 6 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/core/src/file_group/reader
git commit -m "add KeySpec + create_key_spec for keyFilterOpt log-scan path

Mirrors Java HoodieMergedLogRecordReader.createKeySpec via Rust kind()
pattern matching. Pure addition; not yet wired into scanner. Tests
cover In, StringStartsWithAny, non-matching, and KeySpec.matches.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2.2: Add `key_filter_opt` field to `ReaderContext`

**Files:**
- Modify: `crates/core/src/file_group/reader/reader_context.rs`

- [ ] **Step 1: Write failing tests** (append to existing `tests` mod or add new mod at the bottom)

Append at bottom of `crates/core/src/file_group/reader/reader_context.rs`:

```rust
#[cfg(test)]
mod key_filter_opt_tests {
    use super::*;
    use crate::expression::predicates::predicates_factory;
    use crate::expression::{Expression, Literal, NameReference};
    use std::sync::Arc;

    #[test]
    fn empty_reader_context_has_no_key_filter() {
        let ctx = ReaderContext::empty();
        assert!(ctx.get_key_filter_opt().is_none());
    }

    #[test]
    fn key_filter_opt_can_be_set_and_retrieved() {
        let mut ctx = ReaderContext::empty();
        let pred = predicates_factory::in_(
            Box::new(NameReference::new("_hoodie_record_key")),
            vec![Box::new(Literal::string("k1")) as Box<dyn Expression>],
        );
        ctx.key_filter_opt = Some(Arc::new(pred));
        assert!(ctx.get_key_filter_opt().is_some());
    }
}
```

- [ ] **Step 2: Run tests; expect compile failure**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib file_group::reader::reader_context::key_filter_opt 2>&1 | tail -10`
Expected: FAIL — `key_filter_opt` field does not exist.

- [ ] **Step 3: Add the field + getter**

In `crates/core/src/file_group/reader/reader_context.rs`:

1. Add import at the top (after the existing `use` block):

```rust
use crate::expression::predicate::Predicate;
use std::sync::Arc;
```

2. In the `ReaderContext` struct (around lines 53-84), add a new field at the bottom of the struct:

```rust
    /// Mirrors Java `HoodieReaderContext.keyFilterOpt` — an optional
    /// predicate (typically `In` or `StringStartsWithAny`) used to
    /// narrow which records are scanned. None by default. See spec §5.1.
    pub key_filter_opt: Option<Arc<dyn Predicate>>,
```

3. Add a getter accessor inside the `impl ReaderContext` block:

```rust
    /// Mirrors Java `HoodieReaderContext.getKeyFilterOpt()`.
    pub fn get_key_filter_opt(&self) -> Option<Arc<dyn Predicate>> {
        self.key_filter_opt.clone()
    }
```

4. In `ReaderContext::empty()` (around line 176-194), add `key_filter_opt: None,` to the struct literal:

```rust
    pub fn empty() -> Self {
        Self {
            // … existing fields …
            table_config: HashMap::new(),
            hoodie_reader_config: HashMap::new(),
            key_filter_opt: None,                    // ← NEW
        }
    }
```

5. Update the doc comment on `ReaderContext` (the table at lines 41-52) to add the new field. Insert a row:

```rust
    /// | `readerContext.getKeyFilterOpt()`           | `key_filter_opt`                        |
```

- [ ] **Step 4: Audit and update other ReaderContext construction sites**

Search for places that build a `ReaderContext` literal:

```bash
cd /home/ubuntu/ws3/hudi-rs && grep -rn "ReaderContext {" crates/core/src --include='*.rs' | grep -v "/target/"
```

For each such site, add `key_filter_opt: None,` to the literal (initializer). Likely sites: `crates/core/src/cpp/context.rs` (FFI bridge), test fixtures. Spec §5.1 says FFI initializes to None.

- [ ] **Step 5: Run tests; verify pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib file_group::reader::reader_context 2>&1 | tail -10`
Expected: tests pass; entire crate still compiles.

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo build -p hudi-core 2>&1 | tail -10`
Expected: clean build.

- [ ] **Step 6: Commit**

```bash
git add crates/core/src
git commit -m "add key_filter_opt field to ReaderContext

Mirrors Java HoodieReaderContext.keyFilterOpt. Default None on all
construction sites including FFI bridge and ReaderContext::empty().

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2.3: Modify `scan_internal` signature to accept `Option<KeySpec>` + apply filter in record processing

**Files:**
- Modify: `crates/core/src/file_group/reader/log_record_reader.rs`

**Java reference:** `BaseHoodieLogRecordReader.scanInternal(Option<KeySpec>, boolean)` — the Java signature already has the KeySpec parameter; we're catching the Rust port up.

The key change is twofold:
1. Add `key_spec_opt: Option<KeySpec>` parameter to `scan_internal`.
2. Inside `process_queued_blocks_for_instant` (around line 476), filter records by `KeySpec` before dispatching to the buffer's `process_data_block` / `process_delete_block`.

The filter must apply per-record, not per-block, because a block has many records. Approach: pass the `KeySpec` reference into the buffer's process methods, and the buffer applies it. Or: filter in the scanner before calling the buffer.

**Cleaner design:** filter inside the scanner. The scanner has access to the record key (via `record_context`). After block content is loaded, iterate records and skip those that don't match the KeySpec.

But hudi-rs's `process_data_block` / `process_delete_block` are *implemented on the buffer*, taking a `LogBlock` and inflating it internally. To filter records inside the scan, we need the scanner to inflate the block, iterate records, filter, then dispatch — duplicating buffer logic.

**Pragmatic approach:** add a `key_spec_opt: Option<KeySpec>` field to `BaseHoodieLogRecordReader`, and modify the buffer trait to accept an optional KeySpec. The buffer's process methods skip records that don't match.

Concretely:
- `BaseHoodieLogRecordReader` stores `key_spec_opt: Option<KeySpec>`.
- The `HoodieFileGroupRecordBuffer` trait grows two new methods: `set_key_spec(&mut self, key_spec: Option<KeySpec>)` and `key_spec_filter(&self, key: &str) -> bool` (default: true).
- `KeyBasedFileGroupRecordBuffer::process_data_block` and `process_delete_block` call `key_spec_filter(&key)` per record and skip non-matching keys.

**Even simpler:** filter at the buffer's `process_next_data_record` / `process_next_deleted_record`, since those receive the key. Buffer holds an `Option<KeySpec>` field set by the scanner before scan. No trait changes needed beyond a setter.

That's the path of least resistance. Implementation:

- [ ] **Step 1: Write failing test** for `BaseHoodieLogRecordReader::scan_internal` accepting `Option<KeySpec>`

This is hard to test cleanly without a full scan setup. Skip a unit test at this layer — Phase 3's e2e test exercises the wired-up flow. For Task 2.3, our test bar is "scan_internal compiles with new signature; existing tests still pass after callers are updated".

- [ ] **Step 2: Add `key_spec_opt` field to `BaseHoodieLogRecordReader`**

In `crates/core/src/file_group/reader/log_record_reader.rs`, find the struct definition (search `pub struct BaseHoodieLogRecordReader`). Add a new field:

```rust
    /// Mirrors Java `BaseHoodieLogRecordReader.keySpecOpt` — set by the
    /// scanner caller to narrow which records are processed.
    pub key_spec_opt: Option<crate::file_group::reader::key_spec::KeySpec>,
```

Update the constructor (or the builder) to default this to `None`. The constructor calls in `merged_log_record_reader.rs` will need a corresponding `key_spec_opt: None` default added.

- [ ] **Step 3: Update `scan_internal` signature**

Change `crates/core/src/file_group/reader/log_record_reader.rs:371`:

```rust
    pub async fn scan_internal(
        &mut self,
        key_spec_opt: Option<crate::file_group::reader::key_spec::KeySpec>,
        skip_processing_blocks: bool,
    ) -> Result<()> {
        // Store on self so deeper dispatch points can consult it.
        self.key_spec_opt = key_spec_opt;
        // … rest of the method unchanged
    }
```

- [ ] **Step 4: Apply filter in `process_queued_blocks_for_instant`**

Find the function (around line 476). Before calling `self.record_buffer.process_data_block` / `process_delete_block`, propagate the `key_spec_opt` to the record buffer via a new buffer trait method `set_key_spec` (next step) — or, if the buffer's `process_*` methods iterate records, do the filter inside the buffer.

For consistency with the Java structure, let's add a setter on the `HoodieFileGroupRecordBuffer` trait:

In `crates/core/src/file_group/reader/buffer/mod.rs` (find the `HoodieFileGroupRecordBuffer` trait definition), add:

```rust
    /// Set the key-spec filter. Records whose key doesn't match are skipped
    /// during `process_data_block` / `process_delete_block`. Mirrors how
    /// Java's BaseHoodieLogRecordReader threads keySpecOpt through to
    /// per-record processing.
    fn set_key_spec(&mut self, _key_spec: Option<crate::file_group::reader::key_spec::KeySpec>) {
        // default: no-op for buffers that don't support filtering
    }
```

In `crates/core/src/file_group/reader/buffer/key_based.rs`, override `set_key_spec` and store on the buffer:

1. Add field to `KeyBasedFileGroupRecordBuffer`:

```rust
pub struct KeyBasedFileGroupRecordBuffer {
    pub base: FileGroupRecordBuffer,
    pub reader_context: Arc<ReaderContext>,
    pub record_context: RecordContext,
    pub key_spec_opt: Option<crate::file_group::reader::key_spec::KeySpec>,  // ← NEW
}
```

Initialize to `None` in `KeyBasedFileGroupRecordBuffer::new`:

```rust
        Ok(Self {
            base,
            reader_context,
            record_context,
            key_spec_opt: None,        // ← NEW
        })
```

2. Override `set_key_spec`:

```rust
    fn set_key_spec(&mut self, key_spec: Option<crate::file_group::reader::key_spec::KeySpec>) {
        self.key_spec_opt = key_spec;
    }
```

3. In `process_next_data_record` (around the `for (key, record) in records { … }` loop in `process_data_block`), check the key against `key_spec_opt`:

Edit `process_data_block` (around line 213-228 of `key_based.rs`):

```rust
            for batch in record_batches.data_batches {
                let records = self.record_context.batch_to_buffered_records(
                    &batch,
                    self.base.delete_context.as_ref(),
                )?;
                for (key, record) in records {
                    // Skip records that don't match the key-spec filter.
                    if let Some(spec) = &self.key_spec_opt {
                        if !spec.matches(&key) { continue; }
                    }
                    self.process_next_data_record(record, &key)?;
                }
            }
```

Same in `process_delete_block` (around line 287-298):

```rust
            for (batch, _inst) in record_batches.delete_batches {
                let delete_records = self.record_context.delete_batch_to_buffered_records(&batch)?;
                for (key, _record) in delete_records {
                    if let Some(spec) = &self.key_spec_opt {
                        if !spec.matches(&key) { continue; }
                    }
                    let delete_record = DeleteRecord {
                        record_key: key.clone(),
                        partition_path: String::new(),
                        ordering_value: None,
                    };
                    self.process_next_deleted_record(delete_record, &key);
                }
            }
```

4. Propagate `self.key_spec_opt` from the scanner to the buffer in `BaseHoodieLogRecordReader::scan_internal`:

After the line `self.key_spec_opt = key_spec_opt;` (Step 3), add:

```rust
        // Propagate to buffer for per-record filtering (mirrors Java's
        // BaseHoodieLogRecordReader → buffer key-spec threading).
        self.record_buffer.set_key_spec(self.key_spec_opt.clone());
```

- [ ] **Step 5: Update all callers of `scan_internal`**

Search for `scan_internal(`:

```bash
grep -rn "scan_internal(" crates/core/src --include='*.rs' | grep -v "fn scan_internal"
```

Expected hits:
- `crates/core/src/file_group/reader/merged_log_record_reader.rs:120` and `:137` — change to `scan_internal(None, skip_processing_blocks)` and `scan_internal(None, false)` respectively. (The actual `Some(KeySpec)` plumb-through happens in Task 2.4.)

- [ ] **Step 6: Run tests; verify still pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib 2>&1 | tail -15`
Expected: all existing tests pass. The new path (key_spec_opt = Some(…)) is unexercised yet — Phase 3 tests it end-to-end.

- [ ] **Step 7: Commit**

```bash
git add crates/core/src
git commit -m "scan_internal accepts Option<KeySpec>; buffer applies key-spec filter

Mirrors Java BaseHoodieLogRecordReader.scanInternal(Option<KeySpec>, boolean).
KeyBasedFileGroupRecordBuffer overrides set_key_spec to store the spec
and skip non-matching records in process_data_block/process_delete_block.
All existing callers pass None; behavior unchanged.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2.4: Wire `create_key_spec` into `HoodieMergedLogRecordReader::perform_scan`

**Files:**
- Modify: `crates/core/src/file_group/reader/merged_log_record_reader.rs:133-150`

**Java source:** `~/ws3/hudi-internal/hudi-common/src/main/java/org/apache/hudi/common/table/log/HoodieMergedLogRecordReader.java` lines 95-107 (`performScan`).

- [ ] **Step 1: Replace the TODO in `perform_scan`**

In `crates/core/src/file_group/reader/merged_log_record_reader.rs`, find `perform_scan` (around line 133). Replace the body:

```rust
    async fn perform_scan(&mut self) -> Result<()> {
        let start = std::time::Instant::now();

        // Mirrors Java: Option<KeySpec> keySpecOpt = createKeySpec(readerContext.getKeyFilterOpt());
        let key_spec_opt = crate::file_group::reader::key_spec::create_key_spec_from_arc(
            &self.base.reader_context.key_filter_opt,
        );

        self.base.scan_internal(key_spec_opt, false).await?;

        self.total_time_taken_to_read_and_merge_blocks_ms =
            start.elapsed().as_millis() as u64;
        self.num_merged_records_in_log = self.base.record_buffer.size() as u64;

        log::info!(
            "Number of log files scanned => {}",
            self.base.log_file_paths.len()
        );
        log::info!(
            "Number of entries in Map => {}",
            self.base.record_buffer.size()
        );
        Ok(())
    }
```

The other `scan_internal` call site at line 120 (in `scan_with_skip`) is unchanged — pass `None` for the key spec because regular `scan` (not `performScan`) doesn't apply the key filter:

```rust
    pub async fn scan_with_skip(&mut self, skip_processing_blocks: bool) -> Result<()> {
        if self.base.force_full_scan {
            return Ok(());
        }
        self.base.scan_internal(None, skip_processing_blocks).await
    }
```

- [ ] **Step 2: Run tests; verify still pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --lib 2>&1 | tail -10`
Expected: all tests pass. The behavior with `key_filter_opt = None` (the default everywhere) is identical to before.

- [ ] **Step 3: Commit**

```bash
git add crates/core/src/file_group/reader/merged_log_record_reader.rs
git commit -m "wire create_key_spec into HoodieMergedLogRecordReader::perform_scan

Removes the existing TODO. Mirrors Java performScan() lines 95-107.
key_filter_opt defaults to None on all current call paths so behavior
is unchanged for existing users.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2.5: Apply key-spec row filter to base file batches in `HoodieFileGroupReader::make_base_file_batches`

**Files:**
- Modify: `crates/core/src/file_group/reader/mod.rs:455-497` (`make_base_file_batches`)

**Java source:** `HoodieAvroReaderContext.getFileRecordIterator` lines 218-228 — the "fall through to row-level filter when reader doesn't support key predicate" semantics.

- [ ] **Step 1: Locate `_hoodie_record_key` column index utility**

Look for an existing helper to find a column index by name. If none exists, write one inline.

- [ ] **Step 2: Add filter step after parquet read**

In `crates/core/src/file_group/reader/mod.rs`, find `make_base_file_batches` (around line 455). After the `Ok(vec![batch])` line at the end of the `Some(path)` arm, replace it with a filter step:

```rust
                let batch = if let Some(required_schema) = &self.schema_handler.required_schema {
                    self.storage
                        .get_parquet_file_data_projected(path, required_schema)
                        .await
                        .map_err(|e| {
                            CoreError::ReadFileSliceError(format!(
                                "Failed to read base file '{path}' with projection: {e:?}"
                            ))
                        })?
                } else {
                    self.storage
                        .get_parquet_file_data(path)
                        .await
                        .map_err(|e| {
                            CoreError::ReadFileSliceError(format!(
                                "Failed to read base file '{path}': {e:?}"
                            ))
                        })?
                };

                // Mirrors Java HoodieAvroReaderContext.getFileRecordIterator
                // (lines 218-228): when keyFilterOpt is set and the reader does
                // not support a native key predicate (Parquet has none), apply
                // a row-level filter on _hoodie_record_key.
                let batch = apply_key_filter_to_batch(
                    batch,
                    self.reader_context.key_filter_opt.as_deref(),
                )?;
                Ok(vec![batch])
```

- [ ] **Step 3: Add the helper function `apply_key_filter_to_batch`**

Add to the same file (`crates/core/src/file_group/reader/mod.rs`), as a free function near the bottom of the impl block or the file:

```rust
/// Filter rows in `batch` so only those whose `_hoodie_record_key` matches
/// the given key-spec (derived from `key_filter_opt`) survive. If
/// `key_filter_opt` is None or doesn't yield a KeySpec, returns the batch
/// unchanged.
///
/// Mirrors Java HoodieAvroReaderContext.getFileRecordIterator (lines
/// 218-228) for the non-key-predicate-supporting reader case.
fn apply_key_filter_to_batch(
    batch: RecordBatch,
    key_filter_opt: Option<&dyn crate::expression::Predicate>,
) -> Result<RecordBatch> {
    use crate::file_group::reader::key_spec::create_key_spec;
    let key_spec = match create_key_spec(key_filter_opt) {
        Some(spec) => spec,
        None => return Ok(batch),
    };

    // Find the _hoodie_record_key column.
    let key_col_idx = match batch.schema().index_of("_hoodie_record_key") {
        Ok(i) => i,
        Err(_) => {
            // Column not present (e.g., virtual keys). Return batch as-is —
            // the filter cannot apply.
            log::debug!(
                "[HoodieFileGroupReader] key_filter_opt set but _hoodie_record_key \
                 column not present in base file; skipping base-file filter"
            );
            return Ok(batch);
        }
    };

    let key_col = batch
        .column(key_col_idx)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or_else(|| {
            CoreError::ReadFileSliceError(
                "_hoodie_record_key column is not StringArray".to_string(),
            )
        })?;

    // Build a BooleanArray mask: true for rows whose key matches the spec.
    let mask: arrow_array::BooleanArray = (0..batch.num_rows())
        .map(|i| {
            if key_col.is_null(i) {
                Some(false)
            } else {
                Some(key_spec.matches(key_col.value(i)))
            }
        })
        .collect();

    arrow::compute::filter_record_batch(&batch, &mask)
        .map_err(|e| CoreError::ReadFileSliceError(format!("filter_record_batch: {e}")))
}
```

(Update existing imports at the top of the file: ensure `arrow_array::StringArray` is in scope — there's already `use arrow_array::{Array, RecordBatch, StringArray};` per the earlier read, so it is.)

- [ ] **Step 4: Run tests; verify still pass**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core 2>&1 | tail -15`
Expected: all tests pass. With `key_filter_opt = None`, the helper is a no-op early-return.

- [ ] **Step 5: Commit**

```bash
git add crates/core/src/file_group/reader/mod.rs
git commit -m "apply keyFilterOpt as row filter on base parquet batches

Mirrors Java HoodieAvroReaderContext.getFileRecordIterator (lines
218-228) "fall through to row-level filter" path. Parquet has no native
key predicate, so we filter via Arrow filter_record_batch on the
_hoodie_record_key column. No-op when key_filter_opt is None.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2.6: Manual cross-check vs `readerContext_callstack.md`

**Files:** None modified — verification only.

**Reference:** `/home/ubuntu/operations/tasks/quantonMORScanSupport/scanSpecPushDown/Tasks/phase3_rust_fg_integration/migrateFilterOptCode/readerContext_callstack.md`

- [ ] **Step 1: Re-read the call-stack doc**

Open `~/operations/tasks/quantonMORScanSupport/scanSpecPushDown/Tasks/phase3_rust_fg_integration/migrateFilterOptCode/readerContext_callstack.md`. Focus on §3.1 (eager-during-construction stack) and §3.2 (lazy `getClosableIterator` stack).

- [ ] **Step 2: For each `readerContext.*` line in the doc, identify the hudi-rs equivalent**

Produce a checklist as the body of the Phase 2 commit message — for every line in the validation table (§5 of the doc), name the specific Rust `file:line` that does the same thing. Example:

```
Java line 105 readerContext.setHasLogFiles(...)        ← rs file_group/reader/mod.rs:??? hudi_file_group_reader_constructor
Java line 113 readerContext.initRecordMerger(props)    ← rs file_group/reader/mod.rs:??? init_record_merger
Java line 119-121 readerContext.setSchemaHandler(...)   ← rs file_group/reader/mod.rs:??? line 246-253
… (all 47 lines)
```

For lines without a Rust equivalent, document the gap explicitly. The keyFilterOpt-specific call sites for Phase 2 should be:
- Java `HoodieReaderContext.keyFilterOpt` → `crates/core/src/file_group/reader/reader_context.rs:???`
- Java `HoodieMergedLogRecordReader.createKeySpec(...)` → `crates/core/src/file_group/reader/key_spec.rs:???`
- Java `BaseHoodieLogRecordReader.scanInternal(Option<KeySpec>, boolean)` → `crates/core/src/file_group/reader/log_record_reader.rs:371`
- Java `HoodieAvroReaderContext.getFileRecordIterator` (lines 218-228) → `crates/core/src/file_group/reader/mod.rs::apply_key_filter_to_batch`

- [ ] **Step 3: If the cross-check uncovers a gap, file a follow-up note in the design spec or fix it inline**

For Phase 2, the only required gaps to address are anything related to keyFilterOpt's reader-side flow. Other readerContext interactions (setSchemaHandler, getMergeMode, etc.) are outside Phase 2's scope but should be checked off as already-equivalent.

- [ ] **Step 4: Save the checklist**

Append the checklist to `docs/superpowers/specs/2026-05-07-keyfilteropt-port-design.md` under a new appendix `## Appendix A — Phase 2 cross-check vs readerContext_callstack.md`. Commit:

```bash
git add docs/superpowers/specs/2026-05-07-keyfilteropt-port-design.md
git commit -m "phase 2 cross-check vs readerContext_callstack.md (appendix)

Maps each Java readerContext interaction in the call-stack doc to its
hudi-rs file:line equivalent. Validates Phase 2 implementation parity
for the keyFilterOpt path.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Phase 3 — E2E smoke test

### Task 3.1: Locate the v9 MOR fixture and write helper functions

**Files:**
- Modify: `crates/core/tests/file_group_reader_tests.rs` (add helpers at the bottom)

- [ ] **Step 1: Verify fixture availability**

Run:
```bash
cd /home/ubuntu/ws3/hudi-rs && grep -rn "V9Mor8I4UCommitTime\|v9_mor_8i4u_commit_time" crates/test/src --include='*.rs' | head -10
```
Expected: confirms the fixture variant exists. Note the file path for the fixture (likely under `crates/test/data/...`).

- [ ] **Step 2: Open the test fixture and find one file group**

Run:
```bash
cd /home/ubuntu/ws3/hudi-rs && find crates/test -path '*v9_mor_8i4u_commit_time*' -type f 2>/dev/null | head -20
```

Identify the partition path for `city=sf`, the base parquet file, and the log file(s).

- [ ] **Step 3: Add helper functions to the test file**

Open `crates/core/tests/file_group_reader_tests.rs` and append at the bottom (after the existing helpers and the city=sf / city=nyc tests). The new helper `read_file_group_with_key_filter` is a deliberate inline variant of the existing `read_file_group` (lines 69-121); we don't refactor `read_file_group` itself in this plan to keep the change surface small.

```rust
// =============================================================================
// Phase 3 helpers — keyFilterOpt e2e smoke test
// =============================================================================

use hudi_core::expression::predicates::predicates_factory;
use hudi_core::expression::{Expression, Literal, NameReference, Predicate};
use std::sync::Arc as StdArc;

/// Variant of `read_file_group` that accepts a `key_filter_opt`.
/// Mirrors the existing helper at lines 69-121, with the addition of
/// `reader_context.key_filter_opt = key_filter_opt;` on the ctx.
async fn read_file_group_with_key_filter(
    table_path: &str,
    partition: &str,
    base_file: &str,
    log_files: Vec<&str>,
    key_filter_opt: Option<StdArc<dyn Predicate>>,
) -> Result<arrow_array::RecordBatch> {
    let (_hudi_configs, storage) = create_configs_and_storage(table_path).await?;

    let base_path = if base_file.is_empty() {
        None
    } else if partition.is_empty() {
        Some(base_file.to_string())
    } else {
        Some(format!("{}/{}", partition, base_file))
    };
    let log_paths: Vec<String> = log_files
        .iter()
        .map(|lf| {
            if partition.is_empty() {
                lf.to_string()
            } else {
                format!("{}/{}", partition, lf)
            }
        })
        .collect();

    let input_split = InputSplit::new(
        base_path,
        None,
        log_paths,
        partition.to_string(),
    );

    let mut reader_context = ReaderContext::empty();
    reader_context.latest_commit_time = "99991231235959999".to_string();
    reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
    reader_context.table_config.insert(
        HudiTableConfig::PrecombineField.as_ref().to_string(),
        "ts".to_string(),
    );
    reader_context.rebuild_record_context(partition.to_string());
    reader_context.key_filter_opt = key_filter_opt;       // ← the only addition

    let mut reader = HoodieFileGroupReader::new(
        Arc::new(reader_context),
        storage,
        input_split,
        ReaderParameters::default(),
        None,
        None,
    );

    reader.read().await
}

/// Look up the `_hoodie_record_key` string for a given `id` in the baseline
/// batch. Used to derive the literal value for the test filter without
/// hard-coding the keygen encoding.
fn lookup_record_key(batch: &arrow_array::RecordBatch, id: i32) -> String {
    let id_col_idx = batch.schema().index_of("id").expect("id column missing");
    let key_col_idx = batch
        .schema()
        .index_of("_hoodie_record_key")
        .expect("_hoodie_record_key column missing");

    let id_col = batch
        .column(id_col_idx)
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .expect("id column should be Int32");
    let key_col = batch
        .column(key_col_idx)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("_hoodie_record_key column should be StringArray");

    for i in 0..batch.num_rows() {
        if id_col.value(i) == id {
            return key_col.value(i).to_string();
        }
    }
    panic!("id {id} not found in baseline batch");
}

/// Pull the row tuple `(id, name, age)` for the given id, or None. Reuses
/// the exact column projection of the existing `extract_id_name_age` helper.
fn extract_row_with_id_opt(
    batch: &arrow_array::RecordBatch,
    id: i32,
) -> Option<(i32, String, i32)> {
    let id_col = batch.column_by_name("id")?
        .as_any().downcast_ref::<arrow_array::Int32Array>()?;
    let name_col = batch.column_by_name("name")?
        .as_any().downcast_ref::<arrow_array::StringArray>()?;
    let age_col = batch.column_by_name("age")?
        .as_any().downcast_ref::<arrow_array::Int32Array>()?;

    for i in 0..batch.num_rows() {
        if id_col.value(i) == id {
            return Some((
                id_col.value(i),
                name_col.value(i).to_string(),
                age_col.value(i),
            ));
        }
    }
    None
}
```

Notes:
- The fixture's `_hoodie_record_key` column is exposed in the merged output because hudi-rs's reader does not strip meta columns. If a test run reveals it isn't present, the helper's `expect("_hoodie_record_key column missing")` will panic — that's a clear signal to investigate the schema rather than a silent bug.
- `extract_row_with_id_opt` returns `(id, name, age)` to match the shape used by the existing `extract_id_name_age` helper.

- [ ] **Step 4: Compile-check the helpers**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --test file_group_reader_tests --no-run 2>&1 | tail -10`
Expected: clean build.

- [ ] **Step 5: Commit**

```bash
git add crates/core/tests/file_group_reader_tests.rs
git commit -m "add e2e test helpers for keyFilterOpt phase 3 smoke test

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3.2: Write the e2e smoke test

**Files:**
- Modify: `crates/core/tests/file_group_reader_tests.rs`

- [ ] **Step 1: Add the test**

Append to `crates/core/tests/file_group_reader_tests.rs`:

```rust
/// Phase 3 e2e smoke test — keyFilterOpt actually filters rows.
///
/// Reads partition `city=sf` of v9_mor_8i4u_commit_time twice:
///   1. baseline (no filter)         → expect 2 rows (id=1 V2 update, id=2 base)
///   2. filtered (In on record-key)  → expect 1 row (id=1)
///
/// See keyFilterOpt design spec §7.
#[tokio::test]
async fn fg_reader_with_key_filter_filters_rows() -> Result<()> {
    // Fixture filenames copied from the existing test_e2e_v9_mor_commit_time_sf_merge
    // test (lines 218-223) so both tests stay in sync if the fixture is regenerated.
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();
    let partition = "city=sf";
    let base_file = "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet";
    let log_files: Vec<&str> = vec![
        ".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73",
    ];

    // ── Read 1: no filter (baseline) ────────────────────────────────
    let baseline_batch = read_file_group_with_key_filter(
        &table_path,
        partition,
        base_file,
        log_files.clone(),
        None,
    ).await?;

    assert_eq!(baseline_batch.num_rows(), 2, "city=sf should have 2 rows in baseline");

    // Derive the actual _hoodie_record_key string for id=1 from the baseline batch.
    // Avoids hard-coding the keygen encoding (id:1 vs 1 vs id=1).
    let key_for_id1 = lookup_record_key(&baseline_batch, 1);

    // ── Read 2: filter with In(_hoodie_record_key, [<key for id=1>]) ────
    //  Construction mirrors Java: Predicates.in(NameReference("_hoodie_record_key"),
    //                                            [Literal.from(key_for_id1)])
    let key_filter: StdArc<dyn Predicate> = StdArc::new(
        predicates_factory::in_(
            Box::new(NameReference::new("_hoodie_record_key")),
            vec![Box::new(Literal::string(key_for_id1.clone())) as Box<dyn Expression>],
        ),
    );

    let filtered_batch = read_file_group_with_key_filter(
        &table_path,
        partition,
        base_file,
        log_files.clone(),
        Some(key_filter),
    ).await?;

    assert_eq!(filtered_batch.num_rows(), 1,
        "filtered read should return only id=1");

    // ── Cross-validate row content ────────────────────────────────
    let expected = extract_row_with_id_opt(&baseline_batch, 1)
        .expect("id=1 should exist in baseline");
    let actual = extract_row_with_id_opt(&filtered_batch, 1)
        .expect("id=1 should exist in filtered");
    assert_eq!(expected, actual, "filtered id=1 row must equal baseline id=1 row");

    // Negative assertion: id=2 (base-only) is NOT in filtered output.
    assert!(extract_row_with_id_opt(&filtered_batch, 2).is_none(),
        "id=2 must be excluded by the key filter");

    Ok(())
}
```

- [ ] **Step 2: Run the test**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --test file_group_reader_tests fg_reader_with_key_filter_filters_rows -- --nocapture 2>&1 | tail -30`
Expected: PASS. Inspect output for `[HoodieFileGroupReader]` debug logs that confirm the filter ran on both base and log paths.

- [ ] **Step 3: Commit**

```bash
git add crates/core/tests/file_group_reader_tests.rs
git commit -m "add Phase 3 e2e smoke test for keyFilterOpt

Reads city=sf partition of v9_mor_8i4u_commit_time with and without
an In predicate on _hoodie_record_key. Asserts:
  * baseline returns 2 rows
  * filtered returns 1 row matching baseline content for id=1
  * id=2 (base-only, not in filter) is excluded

End-to-end validation of the keyFilterOpt port: ReaderContext field,
log-scan KeySpec, base-file row filter all confirmed working.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3.3: Final verification and PR-ready commit

**Files:** None modified — verification only.

- [ ] **Step 1: Run the full test suite**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test --workspace 2>&1 | tail -30`
Expected: all tests pass (existing + new).

- [ ] **Step 2: Run clippy on the workspace**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo clippy --workspace --all-targets 2>&1 | tail -30`
Expected: no errors.

- [ ] **Step 3: Verify branch state**

Run: `cd /home/ubuntu/ws3/hudi-rs && git log --oneline davis/phase3migrateFilterOptCode ^master 2>&1 | tail -30`
Expected: commit list shows all Phase 1, 2, 3 commits in order. No accidental amendments.

- [ ] **Step 4: Final commit if any cleanup needed**

If clippy or test fixes uncovered changes:

```bash
git add -A
git commit -m "phase 3: final test cleanup

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

If no cleanup needed, no commit.

---

## Phase 1 → 2 → 3 dependency notes

- Phase 2 strictly requires Phase 1 (uses `Predicate`, `Literal`, `predicates_factory`).
- Phase 3 strictly requires Phase 2 (uses `key_filter_opt` field, `read_file_group_with_key_filter` helper).
- Within Phase 1, Tasks 1.6 → 1.10 → 1.11 → 1.12 → 1.13 → 1.14 are sequential because each task adds variants to `ExpressionKind` / `PredicateKind`.
- Within Phase 2, Tasks 2.1 → 2.2 → 2.3 → 2.4 → 2.5 → 2.6 are sequential.

## Out of plan scope (already documented in spec §2)

- FFI / `FfiReaderContext` integration.
- Engines other than Spark.
- Schema evolution.
- HFile reader for `HoodieAvroReaderContext` HFile branch.
- Per-record `Predicate.eval(StructLike)` runtime evaluation against scan-time rows.
