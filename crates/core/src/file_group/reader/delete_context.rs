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
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Mirrors `org.apache.hudi.common.table.read.DeleteContext`.
//!
//! Knows how to detect delete operations from schema metadata and
//! table properties. Constructed in two phases:
//! 1. `new(properties, table_schema)` — in the schema handler
//! 2. `with_reader_schema(reader_schema)` — in the record buffer

use arrow_schema::SchemaRef;
use std::collections::HashMap;

/// Context for detecting delete records in the merge pipeline.
///
/// Built from table properties and schema. Tells the buffer
/// "how do I detect a delete in a record?"
///
/// ## Java counterpart
/// `org.apache.hudi.common.table.read.DeleteContext`
///
/// ## Two-phase construction (mirrors Java)
/// - Phase 1: `new(props, table_schema)` — created in `FileGroupReaderSchemaHandler`
///   Sets `custom_delete_marker` and `has_built_in_delete_field` from props + tableSchema.
/// - Phase 2: `with_reader_schema(schema)` — called in `FileGroupRecordBuffer`
///   Sets `reader_schema` and computes `hoodie_operation_pos` from the reader schema.
#[derive(Debug, Clone)]
pub struct DeleteContext {
    /// Custom delete marker key-value pair, if configured.
    /// When set, a record is considered deleted if it contains this key-value.
    /// Sourced from `DELETE_KEY` and `DELETE_MARKER` table properties.
    pub custom_delete_marker: Option<(String, String)>,

    /// Whether the schema has a built-in delete field (`_hoodie_is_deleted`).
    pub has_built_in_delete_field: bool,

    /// Position of the `_hoodie_operation` field in the reader schema, if present.
    pub hoodie_operation_pos: Option<usize>,

    /// The reader schema.
    pub reader_schema: SchemaRef,
}

/// Property key for the custom delete marker field name.
/// Mirrors Java's `DefaultHoodieRecordPayload.DELETE_KEY`.
const DELETE_KEY_PROP: &str = "hoodie.datasource.write.payload.delete.field";

/// Property key for the custom delete marker value.
/// Mirrors Java's `DefaultHoodieRecordPayload.DELETE_MARKER`.
const DELETE_MARKER_PROP: &str = "hoodie.datasource.write.payload.delete.marker";

impl DeleteContext {
    /// Phase 1: Create a `DeleteContext` from table properties and table schema.
    ///
    /// Mirrors Java's `new DeleteContext(Properties props, Schema tableSchema)`.
    /// - Extracts custom delete marker from properties (DELETE_KEY, DELETE_MARKER).
    /// - Checks if `_hoodie_is_deleted` field exists in the table schema.
    /// - `hoodie_operation_pos` is NOT set here (set in `with_reader_schema`).
    pub fn new(props: &HashMap<String, String>, table_schema: &SchemaRef) -> Self {
        let custom_delete_marker = Self::get_custom_delete_marker(props);
        let has_built_in_delete_field = table_schema
            .column_with_name("_hoodie_is_deleted")
            .is_some();

        Self {
            custom_delete_marker,
            has_built_in_delete_field,
            hoodie_operation_pos: None,
            reader_schema: table_schema.clone(),
        }
    }

    /// Create a `DeleteContext` from table properties only (no schema yet).
    ///
    /// Used at buffer construction time when the reader schema is not yet
    /// available. Uses conservative defaults for schema-dependent fields
    /// (`has_built_in_delete_field = true`) so that `is_delete_record` falls
    /// through to runtime column lookups rather than skipping checks.
    ///
    /// Call `with_reader_schema()` later to provide actual schema info.
    pub fn from_props(props: &HashMap<String, String>) -> Self {
        let custom_delete_marker = Self::get_custom_delete_marker(props);
        Self {
            custom_delete_marker,
            // Conservative: assume field might exist; is_delete_record will
            // do a runtime column lookup and handle absence gracefully.
            has_built_in_delete_field: true,
            hoodie_operation_pos: None,
            reader_schema: arrow_schema::Schema::empty().into(),
        }
    }

    /// Phase 2: Enrich with the reader schema.
    ///
    /// Mirrors Java's `DeleteContext.withReaderSchema(Schema readerSchema)`.
    /// Sets `reader_schema` and computes `hoodie_operation_pos` and
    /// `has_built_in_delete_field` from it.
    pub fn with_reader_schema(mut self, schema: SchemaRef) -> Self {
        self.has_built_in_delete_field = schema.column_with_name("_hoodie_is_deleted").is_some();
        self.hoodie_operation_pos = schema
            .column_with_name("_hoodie_operation")
            .map(|(idx, _)| idx);
        self.reader_schema = schema;
        self
    }

    /// Convenience: create from reader schema only (no properties).
    ///
    /// Used when properties are not available (e.g., in tests or simple paths).
    pub fn from_reader_schema(schema: SchemaRef) -> Self {
        let has_built_in_delete_field = schema.column_with_name("_hoodie_is_deleted").is_some();

        let hoodie_operation_pos = schema
            .column_with_name("_hoodie_operation")
            .map(|(idx, _)| idx);

        Self {
            custom_delete_marker: None,
            has_built_in_delete_field,
            hoodie_operation_pos,
            reader_schema: schema,
        }
    }

    /// Extract custom delete marker key-value from properties.
    ///
    /// Mirrors Java's `DeleteContext.getCustomDeleteMarkerKeyValue(Properties)`.
    fn get_custom_delete_marker(props: &HashMap<String, String>) -> Option<(String, String)> {
        let key = props.get(DELETE_KEY_PROP)?;
        let marker = props.get(DELETE_MARKER_PROP)?;
        if key.is_empty() || marker.is_empty() {
            return None;
        }
        Some((key.clone(), marker.clone()))
    }
}
