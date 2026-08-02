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

//! Ported from the merge-on-read reader. Nothing consumes it yet, so its
//! items are unreachable from the crate's call graph until the reader wires in.
#![allow(dead_code)]

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
const DELETE_KEY_PROP: &str = "hoodie.payload.delete.field";

/// Property key for the custom delete marker value.
/// Mirrors Java's `DefaultHoodieRecordPayload.DELETE_MARKER`.
const DELETE_MARKER_PROP: &str = "hoodie.payload.delete.marker";

/// Prefix under which the delete key/marker are persisted in the table config.
/// Mirrors Java's `HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX`;
/// `HoodieTableConfig.getTableMergeProperties()` extracts keys with this prefix
/// (stripping it) into the reader's merge properties.
const RECORD_MERGE_PROPERTY_PREFIX: &str = "hoodie.record.merge.property.";

/// Table config key for the table version (Java `HoodieTableConfig.VERSION`). Below
/// version NINE, legacy payload classes have their delete key/marker synthesized in
/// `getTableMergeProperties()` rather than stored as properties.
const TABLE_VERSION_KEY: &str = "hoodie.table.version";

/// First table version (Java `HoodieTableVersion.NINE`) that persists legacy-payload
/// merge properties directly, so no payload-class synthesis is needed at or above it.
const MERGE_PROPS_SYNTHESIS_MAX_TABLE_VERSION: i32 = 9;

/// Payload-class config keys, checked in Java `HoodieRecordPayload.getPayloadClassName`
/// order (write class, then persisted table-config classes).
const PAYLOAD_CLASS_KEYS: [&str; 3] = [
    "hoodie.compaction.payload.class",
    "hoodie.datasource.write.payload.class",
    "hoodie.table.legacy.payload.class",
];

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

    /// Extract the custom delete marker key-value from the table properties.
    ///
    /// Mirrors Java's `DeleteContext.getCustomDeleteMarkerKeyValue(Properties)` where
    /// the properties are `HoodieTableConfig.getTableMergeProperties()`. hudi-rs
    /// receives the RAW table config, so it must reproduce that method:
    /// 1. The delete key/marker are persisted under the
    ///    [`RECORD_MERGE_PROPERTY_PREFIX`] (e.g. a v9 `DefaultHoodieRecordPayload`
    ///    table sets `hoodie.record.merge.property.hoodie.payload.delete.field`). The
    ///    non-prefixed form is also accepted in case a caller already stripped it.
    /// 2. For a table below version NINE, the delete key/marker for the legacy
    ///    `AWSDmsAvroPayload` / `MySqlDebeziumAvroPayload` / `PostgresDebeziumAvroPayload`
    ///    payloads are SYNTHESIZED from the payload class (they are not stored).
    ///
    /// Both the key and marker must be present and non-empty (matching Java's
    /// `DELETE_MARKER should be configured with DELETE_KEY` invariant).
    fn get_custom_delete_marker(props: &HashMap<String, String>) -> Option<(String, String)> {
        // (1) Prefixed (as persisted) or bare Java delete key/marker.
        let lookup = |suffix: &str| {
            props
                .get(&format!("{RECORD_MERGE_PROPERTY_PREFIX}{suffix}"))
                .or_else(|| props.get(suffix))
                .filter(|v| !v.is_empty())
                .cloned()
        };
        if let (Some(key), Some(marker)) = (lookup(DELETE_KEY_PROP), lookup(DELETE_MARKER_PROP)) {
            return Some((key, marker));
        }

        // (2) Legacy-payload synthesis for tables below version NINE.
        Self::synthesize_legacy_delete_marker(props)
    }

    /// Synthesize the delete key/marker from the payload class for tables below
    /// version NINE, mirroring Java `HoodieTableConfig.getTableMergeProperties()`.
    /// Returns `None` for version >= NINE (properties are persisted, not synthesized),
    /// an unparseable version, or a payload class without a legacy delete convention.
    fn synthesize_legacy_delete_marker(
        props: &HashMap<String, String>,
    ) -> Option<(String, String)> {
        let version = props.get(TABLE_VERSION_KEY)?.trim().parse::<i32>().ok()?;
        if version >= MERGE_PROPS_SYNTHESIS_MAX_TABLE_VERSION {
            return None;
        }
        let payload_class = PAYLOAD_CLASS_KEYS
            .iter()
            .find_map(|k| props.get(*k))
            .map(String::as_str)?;
        // Debezium op column / delete op (`DebeziumConstants`) and AWS DMS `Op`/`D`
        // (`AWSDmsAvroPayload`). Matched by class-name suffix to tolerate shading.
        if payload_class.ends_with("AWSDmsAvroPayload") {
            Some(("Op".to_string(), "D".to_string()))
        } else if payload_class.ends_with("MySqlDebeziumAvroPayload")
            || payload_class.ends_with("PostgresDebeziumAvroPayload")
        {
            Some(("_change_operation_type".to_string(), "d".to_string()))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("Op", DataType::Utf8, true),
            Field::new("val", DataType::Int64, true),
        ]))
    }

    /// G-D: the custom delete marker MUST be read under the Java property keys
    /// (`hoodie.payload.delete.field` / `.marker`, persisted under the
    /// `hoodie.record.merge.property.` prefix) — NOT the non-existent
    /// `hoodie.datasource.write.payload.delete.*` keys hudi-rs used before. A record
    /// whose marker field equals the marker value is then a delete.
    ///
    /// Discriminating: with the old (wrong) keys, the same config produced NO custom
    /// delete marker, so the marked record was NOT detected as a delete.
    #[test]
    fn test_custom_delete_marker_read_under_java_keys() {
        let props = HashMap::from([
            (
                "hoodie.record.merge.property.hoodie.payload.delete.field".to_string(),
                "Op".to_string(),
            ),
            (
                "hoodie.record.merge.property.hoodie.payload.delete.marker".to_string(),
                "D".to_string(),
            ),
        ]);
        let ctx = DeleteContext::new(&props, &schema());
        assert_eq!(
            ctx.custom_delete_marker,
            Some(("Op".to_string(), "D".to_string())),
            "custom delete marker must be read under the Java keys"
        );

        // The non-prefixed Java keys are also accepted (already-stripped merge props).
        let bare = HashMap::from([
            ("hoodie.payload.delete.field".to_string(), "Op".to_string()),
            ("hoodie.payload.delete.marker".to_string(), "D".to_string()),
        ]);
        assert_eq!(
            DeleteContext::new(&bare, &schema()).custom_delete_marker,
            Some(("Op".to_string(), "D".to_string()))
        );

        // The OLD hudi-rs keys must NOT be honored (they do not exist in Java).
        let old_keys = HashMap::from([
            (
                "hoodie.datasource.write.payload.delete.field".to_string(),
                "Op".to_string(),
            ),
            (
                "hoodie.datasource.write.payload.delete.marker".to_string(),
                "D".to_string(),
            ),
        ]);
        assert_eq!(
            DeleteContext::new(&old_keys, &schema()).custom_delete_marker,
            None,
            "the non-Java (old hudi-rs) keys must NOT configure a delete marker"
        );
    }

    /// G-D: for a table below version NINE, a legacy payload's delete key/marker are
    /// SYNTHESIZED from the payload class (mirrors Java `getTableMergeProperties`).
    /// At/above version NINE they are persisted, not synthesized.
    #[test]
    fn test_legacy_payload_delete_marker_synthesis() {
        let aws_v6 = HashMap::from([
            ("hoodie.table.version".to_string(), "6".to_string()),
            (
                "hoodie.compaction.payload.class".to_string(),
                "org.apache.hudi.common.model.AWSDmsAvroPayload".to_string(),
            ),
        ]);
        assert_eq!(
            DeleteContext::new(&aws_v6, &schema()).custom_delete_marker,
            Some(("Op".to_string(), "D".to_string())),
            "AWS DMS payload (v6) synthesizes Op/D"
        );

        let pg_v6 = HashMap::from([
            ("hoodie.table.version".to_string(), "6".to_string()),
            (
                "hoodie.datasource.write.payload.class".to_string(),
                "org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload".to_string(),
            ),
        ]);
        assert_eq!(
            DeleteContext::new(&pg_v6, &schema()).custom_delete_marker,
            Some(("_change_operation_type".to_string(), "d".to_string())),
            "Postgres Debezium payload (v6) synthesizes _change_operation_type/d"
        );

        // Version >= NINE: no synthesis (properties would be persisted instead).
        let aws_v9 = HashMap::from([
            ("hoodie.table.version".to_string(), "9".to_string()),
            (
                "hoodie.compaction.payload.class".to_string(),
                "org.apache.hudi.common.model.AWSDmsAvroPayload".to_string(),
            ),
        ]);
        assert_eq!(
            DeleteContext::new(&aws_v9, &schema()).custom_delete_marker,
            None,
            "v9+ does not synthesize; the marker is persisted as a merge property"
        );
    }
}
