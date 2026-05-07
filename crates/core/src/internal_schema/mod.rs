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
