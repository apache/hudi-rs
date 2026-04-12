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

//! Mirrors `org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler`.
//!
//! Manages the various schemas involved in reading a file group:
//! table schema, requested schema, required schema, and the schema
//! used for updates (log file merging).

use crate::file_group::reader::delete_context::DeleteContext;
use crate::file_group::reader::output_converter::{OutputConverter, ProjectionConverter};
use arrow_schema::{Field, Schema, SchemaRef};
use std::sync::Arc;

/// The `_hoodie_commit_time` meta field name.
/// Mirrors Java's `HoodieRecord.COMMIT_TIME_METADATA_FIELD`.
const COMMIT_TIME_FIELD: &str = "_hoodie_commit_time";

/// Schema handler for file group reading.
///
/// Manages the different schema views needed during the read pipeline:
/// - **table_schema**: The full schema of the Hudi table.
/// - **requested_schema**: The schema requested by the query (column projection).
/// - **required_schema**: The minimum schema required for reading (includes
///   merge keys and ordering fields even if not in requested_schema).
/// - **data_schema**: The schema used for reading base files.
/// - **schema_for_updates**: Schema for incoming log records (initially == required_schema).
/// - **delete_context**: Canonical delete detection context (single source of truth).
///
/// In Java Hudi, this class also handles internal schema evolution.
/// That is not yet implemented in hudi-rs.
#[derive(Debug, Clone, Default)]
pub struct FileGroupReaderSchemaHandler {
    /// The full table schema.
    pub table_schema: Option<SchemaRef>,

    /// The schema requested by the query (column projection).
    pub requested_schema: Option<SchemaRef>,

    /// The data schema (used for base file reading).
    pub data_schema: Option<SchemaRef>,

    /// The required schema for merge operations.
    /// Includes record key and ordering fields even if not requested.
    pub required_schema: Option<SchemaRef>,

    /// Schema for incoming update records (log file merging).
    /// Initially equals `required_schema`. May be set to a different schema
    /// by `StreamingFileGroupRecordBufferLoader` for incoming records
    /// without metadata fields.
    ///
    /// Mirrors Java's `FileGroupReaderSchemaHandler.schemaForUpdates`.
    schema_for_updates: Option<SchemaRef>,

    /// Canonical delete context, created during `prepare_required_schema()`.
    /// Single source of truth — downstream consumers (record buffer, etc.)
    /// should use this instead of creating their own.
    ///
    /// Mirrors Java's `FileGroupReaderSchemaHandler.deleteContext`.
    delete_context: Option<DeleteContext>,
}

impl FileGroupReaderSchemaHandler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_table_schema(mut self, schema: SchemaRef) -> Self {
        self.table_schema = Some(schema);
        self
    }

    pub fn with_data_schema(mut self, schema: SchemaRef) -> Self {
        self.data_schema = Some(schema);
        self
    }

    pub fn with_requested_schema(mut self, schema: SchemaRef) -> Self {
        self.requested_schema = Some(schema);
        self
    }

    /// Get the canonical delete context.
    ///
    /// Mirrors Java's `FileGroupReaderSchemaHandler.getDeleteContext()`.
    /// Returns `None` only if `prepare_required_schema()` has not been called.
    pub fn delete_context(&self) -> Option<&DeleteContext> {
        self.delete_context.as_ref()
    }

    /// Get the schema for updates (log record merging).
    ///
    /// Mirrors Java's `FileGroupReaderSchemaHandler.getSchemaForUpdates()`.
    pub fn schema_for_updates(&self) -> Option<&SchemaRef> {
        self.schema_for_updates.as_ref()
    }

    /// Set the schema for updates.
    ///
    /// Mirrors Java's `FileGroupReaderSchemaHandler.setSchemaForUpdates(Schema)`.
    /// Used by streaming buffer loaders for incoming records without metadata.
    pub fn set_schema_for_updates(&mut self, schema: SchemaRef) {
        self.schema_for_updates = Some(schema);
    }

    /// Generate the required schema for reading files.
    ///
    /// Mirrors Java's `FileGroupReaderSchemaHandler.generateRequiredSchema()`
    /// (lines 187-229).
    ///
    /// For COW (no log files): returns `requested_schema` as-is, unless
    /// `has_instant_range` is true — then adds `_hoodie_commit_time` for
    /// base file filtering.
    ///
    /// For MOR (with log files): adds mandatory fields needed for merging
    /// (record key, ordering fields, delete markers, commit time) to
    /// `requested_schema`.
    ///
    /// # Arguments
    /// * `has_log_files` — Whether the file group has log files (MOR gate).
    /// * `record_key_fields` — All record key field names (single for meta-field
    ///   tables, potentially multiple for virtual-key composite keys).
    /// * `ordering_field_names` — Precombine/ordering field names.
    /// * `delete_context` — Delete detection context.
    /// * `has_instant_range` — Whether an instant range filter is active.
    /// * `merge_mode` — The merge mode string (e.g. "COMMIT_TIME_ORDERING").
    pub fn generate_required_schema(
        &self,
        has_log_files: bool,
        record_key_fields: &[String],
        ordering_field_names: &[String],
        delete_context: &DeleteContext,
        has_instant_range: bool,
        merge_mode: &str,
    ) -> Option<SchemaRef> {
        // The base schema to start from: requested_schema, or data_schema as fallback.
        let base_schema = self
            .requested_schema
            .as_ref()
            .or(self.data_schema.as_ref())?;

        // The field source for looking up field definitions.
        let field_source = self
            .table_schema
            .as_ref()
            .or(self.data_schema.as_ref())?;

        // COW path: no log files.
        // Mirrors Java lines 190-197:
        //   if (!readerContext.getHasLogFiles()) {
        //     if (hasInstantRange && !findNestedField(requestedSchema, COMMIT_TIME_METADATA_FIELD)) {
        //       addedFields.add(getField(tableSchema, COMMIT_TIME_METADATA_FIELD));
        //       return appendFields(requestedSchema, addedFields);
        //     }
        //     return requestedSchema;
        //   }
        if !has_log_files {
            if has_instant_range
                && base_schema.column_with_name(COMMIT_TIME_FIELD).is_none()
            {
                if let Some((_, field)) = field_source.column_with_name(COMMIT_TIME_FIELD) {
                    let mut fields: Vec<Arc<Field>> = base_schema.fields().to_vec();
                    fields.push(Arc::new(field.clone()));
                    return Some(Arc::new(Schema::new(fields)));
                }
            }
            return Some(base_schema.clone());
        }

        // CUSTOM merge mode is not supported in hudi-rs.
        // Mirrors Java lines 209-213.
        if merge_mode.eq_ignore_ascii_case("CUSTOM") {
            panic!(
                "CUSTOM merge mode is not supported in hudi-rs. \
                 Use COMMIT_TIME_ORDERING or EVENT_TIME_ORDERING."
            );
        }

        // MOR path: collect mandatory fields for merging.
        // Mirrors Java's getMandatoryFieldsForMerging() (lines 231-278).
        let mut mandatory_field_names: Vec<&str> = Vec::new();

        // Add _hoodie_commit_time if instant range is active (Java lines 246-248).
        if has_instant_range {
            mandatory_field_names.push(COMMIT_TIME_FIELD);
        }

        // Add record key fields (Java lines 251-258).
        // Java checks populateMetaFields to decide between _hoodie_record_key
        // and the configured record key fields. The caller already resolves
        // this and passes the correct fields.
        for key_field in record_key_fields {
            mandatory_field_names.push(key_field.as_str());
        }

        // Add ordering/precombine fields only for EVENT_TIME_ORDERING
        // (Java lines 260-263).
        if merge_mode.eq_ignore_ascii_case("EVENT_TIME_ORDERING") {
            for ordering_field in ordering_field_names {
                mandatory_field_names.push(ordering_field.as_str());
            }
        }

        // Add _hoodie_is_deleted if it exists in table schema (Java lines 265-267).
        if delete_context.has_built_in_delete_field {
            mandatory_field_names.push("_hoodie_is_deleted");
        }

        // Add custom delete marker field if configured (Java lines 269-271).
        if let Some((key_field, _)) = &delete_context.custom_delete_marker {
            mandatory_field_names.push(key_field.as_str());
        }

        // Add _hoodie_operation if it exists in table schema (Java lines 273-274).
        if field_source.column_with_name("_hoodie_operation").is_some() {
            mandatory_field_names.push("_hoodie_operation");
        }

        // Append only fields not already in the base schema (Java line 219).
        let mut extra_fields: Vec<Arc<Field>> = Vec::new();
        for &field_name in &mandatory_field_names {
            if base_schema.column_with_name(field_name).is_none() {
                if let Some((_, field)) = field_source.column_with_name(field_name) {
                    // Deduplicate: only add if not already in extra_fields.
                    if !extra_fields.iter().any(|f| f.name() == field_name) {
                        extra_fields.push(Arc::new(field.clone()));
                    }
                }
            }
        }

        if extra_fields.is_empty() {
            Some(base_schema.clone())
        } else {
            let mut fields: Vec<Arc<Field>> = base_schema.fields().to_vec();
            fields.extend(extra_fields);
            Some(Arc::new(Schema::new(fields)))
        }
    }

    /// Prepare the required schema: generate it and store it.
    ///
    /// Mirrors Java's `prepareRequiredSchema()` (lines 280-288) and the
    /// constructor body (lines 104-106) which creates `DeleteContext`,
    /// computes `requiredSchema`, and sets `schemaForUpdates`.
    ///
    /// Creates a `DeleteContext` (stored as canonical source of truth),
    /// calls `generate_required_schema()`, and sets `schema_for_updates`.
    ///
    /// # Arguments
    /// * `has_log_files` — Whether the file group has log files.
    /// * `record_key_fields` — All record key field names.
    /// * `ordering_field_names` — Precombine/ordering field names.
    /// * `props` — Table config properties.
    /// * `has_instant_range` — Whether an instant range filter is active.
    /// * `merge_mode` — The merge mode string.
    pub fn prepare_required_schema(
        &mut self,
        has_log_files: bool,
        record_key_fields: &[String],
        ordering_field_names: &[String],
        props: &std::collections::HashMap<String, String>,
        has_instant_range: bool,
        merge_mode: &str,
    ) {
        // Create and store the canonical DeleteContext (Java line 104).
        let delete_context = if let Some(table_schema) = &self.table_schema {
            DeleteContext::new(props, table_schema)
        } else {
            DeleteContext::from_props(props)
        };
        self.delete_context = Some(delete_context.clone());

        // Compute required schema (Java line 105).
        self.required_schema = self.generate_required_schema(
            has_log_files,
            record_key_fields,
            ordering_field_names,
            &delete_context,
            has_instant_range,
            merge_mode,
        );

        // Initialize schema_for_updates = required_schema (Java line 106).
        self.schema_for_updates = self.required_schema.clone();
    }

    /// Get the output converter for projecting from required_schema to requested_schema.
    ///
    /// Mirrors Java's `getOutputConverter()` (lines 143-148):
    /// Returns `Some(ProjectionConverter)` when required_schema has more fields
    /// than requested_schema; `None` when they are equivalent or when schemas
    /// are not set.
    pub fn get_output_converter(&self) -> Option<Box<dyn OutputConverter>> {
        let required = self.required_schema.as_ref()?;
        let requested = self.requested_schema.as_ref()?;

        // If schemas are projection-equivalent (same fields in same order),
        // no converter is needed.
        if schemas_projection_equivalent(required, requested) {
            return None;
        }

        Some(Box::new(ProjectionConverter::new(requested)))
    }
}

/// Check if two schemas are projection-equivalent: same field names in same order.
///
/// Mirrors Java's `AvroSchemaUtils.areSchemasProjectionEquivalent()`.
fn schemas_projection_equivalent(a: &SchemaRef, b: &SchemaRef) -> bool {
    if a.fields().len() != b.fields().len() {
        return false;
    }
    a.fields()
        .iter()
        .zip(b.fields().iter())
        .all(|(fa, fb)| fa.name() == fb.name())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use std::collections::HashMap;

    /// Helper to create a simple Arrow schema from field names (all Utf8).
    fn make_schema(field_names: &[&str]) -> SchemaRef {
        let fields: Vec<Field> = field_names
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect();
        Arc::new(Schema::new(fields))
    }

    /// Helper to create a schema with mixed types for more realistic testing.
    fn make_table_schema() -> SchemaRef {
        let fields = vec![
            Field::new("_hoodie_commit_time", DataType::Utf8, true),
            Field::new("_hoodie_commit_seqno", DataType::Utf8, true),
            Field::new("_hoodie_record_key", DataType::Utf8, true),
            Field::new("_hoodie_partition_path", DataType::Utf8, true),
            Field::new("_hoodie_file_name", DataType::Utf8, true),
            Field::new("begin_lat", DataType::Float64, true),
            Field::new("begin_lon", DataType::Float64, true),
            Field::new("rider", DataType::Utf8, true),
            Field::new("tip_history", DataType::Utf8, true),
            Field::new("timestamp", DataType::Int64, true),
            Field::new("_hoodie_is_deleted", DataType::Boolean, true),
            Field::new("_hoodie_operation", DataType::Utf8, true),
        ];
        Arc::new(Schema::new(fields))
    }

    // =========================================================================
    // COW tests
    // =========================================================================

    /// COW (no log files) → required_schema == requested_schema.
    #[test]
    fn test_cow_required_schema_equals_requested() {
        let table_schema = make_table_schema();
        let requested_schema = make_schema(&["begin_lat", "tip_history", "rider"]);

        let handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema.clone())
            .with_data_schema(table_schema.clone())
            .with_requested_schema(requested_schema.clone());

        let delete_context = DeleteContext::new(&HashMap::new(), &table_schema);
        let required = handler
            .generate_required_schema(
                false, // no log files (COW)
                &["_hoodie_record_key".to_string()],
                &[],
                &delete_context,
                false, // no instant range
                "COMMIT_TIME_ORDERING",
            )
            .unwrap();

        assert_eq!(required, requested_schema);
    }

    /// COW with instant range → adds _hoodie_commit_time if not in requested schema.
    /// Mirrors Java lines 191-194.
    #[test]
    fn test_cow_with_instant_range_adds_commit_time() {
        let table_schema = make_table_schema();
        let requested_schema = make_schema(&["begin_lat", "rider"]);

        let handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema.clone())
            .with_data_schema(table_schema.clone())
            .with_requested_schema(requested_schema.clone());

        let delete_context = DeleteContext::new(&HashMap::new(), &table_schema);
        let required = handler
            .generate_required_schema(
                false, // COW
                &["_hoodie_record_key".to_string()],
                &[],
                &delete_context,
                true, // has instant range
                "COMMIT_TIME_ORDERING",
            )
            .unwrap();

        let field_names: Vec<&str> = required.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            field_names.contains(&"_hoodie_commit_time"),
            "COW with instant range should add _hoodie_commit_time"
        );
        assert!(field_names.contains(&"begin_lat"));
        assert!(field_names.contains(&"rider"));
    }

    /// COW with instant range but commit_time already requested → no duplicate.
    #[test]
    fn test_cow_with_instant_range_no_duplicate_commit_time() {
        let table_schema = make_table_schema();
        let requested_schema = make_schema(&["_hoodie_commit_time", "begin_lat"]);

        let handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema.clone())
            .with_data_schema(table_schema.clone())
            .with_requested_schema(requested_schema.clone());

        let delete_context = DeleteContext::new(&HashMap::new(), &table_schema);
        let required = handler
            .generate_required_schema(
                false,
                &["_hoodie_record_key".to_string()],
                &[],
                &delete_context,
                true,
                "COMMIT_TIME_ORDERING",
            )
            .unwrap();

        // Should be same as requested since commit_time is already present.
        assert_eq!(required, requested_schema);
    }

    // =========================================================================
    // MOR tests
    // =========================================================================

    /// MOR with subset projection → record key field appended.
    #[test]
    fn test_mor_adds_record_key_field() {
        let table_schema = make_table_schema();
        let requested_schema = make_schema(&["begin_lat", "tip_history", "rider"]);

        let handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema.clone())
            .with_data_schema(table_schema.clone())
            .with_requested_schema(requested_schema.clone());

        let delete_context = DeleteContext::new(&HashMap::new(), &table_schema);
        let required = handler
            .generate_required_schema(
                true, // has log files (MOR)
                &["_hoodie_record_key".to_string()],
                &[], // no ordering fields (COMMIT_TIME_ORDERING)
                &delete_context,
                false,
                "COMMIT_TIME_ORDERING",
            )
            .unwrap();

        let field_names: Vec<&str> = required.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            field_names.contains(&"_hoodie_record_key"),
            "required_schema should include record key field"
        );
        assert!(
            field_names.contains(&"begin_lat"),
            "required_schema should include requested fields"
        );
        assert!(
            field_names.contains(&"_hoodie_is_deleted"),
            "required_schema should include _hoodie_is_deleted when present in table schema"
        );
        assert!(
            field_names.contains(&"_hoodie_operation"),
            "required_schema should include _hoodie_operation when present in table schema"
        );
    }

    /// MOR with instant range → adds _hoodie_commit_time to mandatory fields.
    /// Mirrors Java lines 246-248.
    #[test]
    fn test_mor_with_instant_range_adds_commit_time() {
        let table_schema = make_table_schema();
        let requested_schema = make_schema(&["begin_lat", "rider"]);

        let handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema.clone())
            .with_data_schema(table_schema.clone())
            .with_requested_schema(requested_schema);

        let delete_context = DeleteContext::new(&HashMap::new(), &table_schema);
        let required = handler
            .generate_required_schema(
                true,
                &["_hoodie_record_key".to_string()],
                &[],
                &delete_context,
                true, // has instant range
                "COMMIT_TIME_ORDERING",
            )
            .unwrap();

        let field_names: Vec<&str> = required.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            field_names.contains(&"_hoodie_commit_time"),
            "MOR with instant range should add _hoodie_commit_time"
        );
        assert!(field_names.contains(&"_hoodie_record_key"));
    }

    /// COMMIT_TIME_ORDERING should NOT include ordering fields.
    #[test]
    fn test_mor_commit_time_ordering_excludes_ordering_fields() {
        let table_schema = make_table_schema();
        let requested_schema = make_schema(&["begin_lat", "rider"]);

        let handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema.clone())
            .with_data_schema(table_schema.clone())
            .with_requested_schema(requested_schema);

        let delete_context = DeleteContext::new(&HashMap::new(), &table_schema);
        let required = handler
            .generate_required_schema(
                true,
                &["_hoodie_record_key".to_string()],
                &["timestamp".to_string()],
                &delete_context,
                false,
                "COMMIT_TIME_ORDERING",
            )
            .unwrap();

        let field_names: Vec<&str> = required.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            !field_names.contains(&"timestamp"),
            "COMMIT_TIME_ORDERING should NOT include ordering field"
        );
        assert!(
            field_names.contains(&"_hoodie_record_key"),
            "required_schema should still include record key field"
        );
    }

    /// EVENT_TIME_ORDERING → ordering field appended.
    #[test]
    fn test_mor_adds_ordering_field() {
        let table_schema = make_table_schema();
        let requested_schema = make_schema(&["begin_lat", "tip_history", "rider"]);

        let handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema.clone())
            .with_data_schema(table_schema.clone())
            .with_requested_schema(requested_schema.clone());

        let delete_context = DeleteContext::new(&HashMap::new(), &table_schema);
        let required = handler
            .generate_required_schema(
                true,
                &["_hoodie_record_key".to_string()],
                &["timestamp".to_string()],
                &delete_context,
                false,
                "EVENT_TIME_ORDERING",
            )
            .unwrap();

        let field_names: Vec<&str> = required.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            field_names.contains(&"timestamp"),
            "required_schema should include ordering field"
        );
        assert!(
            field_names.contains(&"_hoodie_record_key"),
            "required_schema should include record key field"
        );
    }

    /// MOR with _hoodie_is_deleted in table schema → added to required.
    #[test]
    fn test_mor_adds_delete_marker_fields() {
        let table_schema = make_table_schema(); // has _hoodie_is_deleted
        let requested_schema = make_schema(&["begin_lat", "rider"]);

        let handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema.clone())
            .with_data_schema(table_schema.clone())
            .with_requested_schema(requested_schema);

        let delete_context = DeleteContext::new(&HashMap::new(), &table_schema);
        let required = handler
            .generate_required_schema(
                true,
                &["_hoodie_record_key".to_string()],
                &[],
                &delete_context,
                false,
                "COMMIT_TIME_ORDERING",
            )
            .unwrap();

        let field_names: Vec<&str> = required.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"_hoodie_is_deleted"));
        assert!(field_names.contains(&"_hoodie_operation"));
    }

    /// MOR with custom delete marker config → custom field added.
    #[test]
    fn test_mor_adds_custom_delete_marker() {
        let table_schema = make_schema(&[
            "_hoodie_record_key",
            "col_a",
            "col_b",
            "is_deleted_custom",
        ]);
        let requested_schema = make_schema(&["col_a"]);

        let handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema.clone())
            .with_data_schema(table_schema.clone())
            .with_requested_schema(requested_schema);

        let mut props = HashMap::new();
        props.insert(
            "hoodie.datasource.write.payload.delete.field".to_string(),
            "is_deleted_custom".to_string(),
        );
        props.insert(
            "hoodie.datasource.write.payload.delete.marker".to_string(),
            "true".to_string(),
        );
        let delete_context = DeleteContext::new(&props, &table_schema);

        let required = handler
            .generate_required_schema(
                true,
                &["_hoodie_record_key".to_string()],
                &[],
                &delete_context,
                false,
                "COMMIT_TIME_ORDERING",
            )
            .unwrap();

        let field_names: Vec<&str> = required.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            field_names.contains(&"is_deleted_custom"),
            "required_schema should include custom delete marker field"
        );
    }

    /// When all mandatory fields already in requested → no extra fields.
    #[test]
    fn test_mor_all_mandatory_fields_already_present() {
        let table_schema = make_table_schema();
        let requested_schema = table_schema.clone();

        let handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema.clone())
            .with_data_schema(table_schema.clone())
            .with_requested_schema(requested_schema.clone());

        let delete_context = DeleteContext::new(&HashMap::new(), &table_schema);
        let required = handler
            .generate_required_schema(
                true,
                &["_hoodie_record_key".to_string()],
                &["timestamp".to_string()],
                &delete_context,
                false,
                "EVENT_TIME_ORDERING",
            )
            .unwrap();

        assert_eq!(required, requested_schema);
    }

    /// CUSTOM merge mode should panic.
    #[test]
    #[should_panic(expected = "CUSTOM merge mode is not supported")]
    fn test_custom_merge_mode_panics() {
        let table_schema = make_table_schema();
        let requested_schema = make_schema(&["begin_lat"]);

        let handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema.clone())
            .with_data_schema(table_schema.clone())
            .with_requested_schema(requested_schema);

        let delete_context = DeleteContext::new(&HashMap::new(), &table_schema);
        handler.generate_required_schema(
            true,
            &["_hoodie_record_key".to_string()],
            &[],
            &delete_context,
            false,
            "CUSTOM",
        );
    }

    // =========================================================================
    // Composite record key tests
    // =========================================================================

    /// MOR with composite record keys → all key fields added.
    #[test]
    fn test_mor_composite_record_keys() {
        let table_schema = make_schema(&[
            "pk1",
            "pk2",
            "col_a",
            "_hoodie_is_deleted",
        ]);
        let requested_schema = make_schema(&["col_a"]);

        let handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema.clone())
            .with_data_schema(table_schema.clone())
            .with_requested_schema(requested_schema);

        let delete_context = DeleteContext::new(&HashMap::new(), &table_schema);
        let required = handler
            .generate_required_schema(
                true,
                &["pk1".to_string(), "pk2".to_string()], // composite keys
                &[],
                &delete_context,
                false,
                "COMMIT_TIME_ORDERING",
            )
            .unwrap();

        let field_names: Vec<&str> = required.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"pk1"), "should include first key field");
        assert!(field_names.contains(&"pk2"), "should include second key field");
        assert!(field_names.contains(&"col_a"), "should include requested fields");
    }

    // =========================================================================
    // Output converter tests
    // =========================================================================

    #[test]
    fn test_get_output_converter_none_when_equal() {
        let schema = make_schema(&["col_a", "col_b"]);
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(schema.clone())
            .with_data_schema(schema.clone())
            .with_requested_schema(schema.clone());
        handler.required_schema = Some(schema);

        assert!(handler.get_output_converter().is_none());
    }

    #[test]
    fn test_get_output_converter_some_when_different() {
        let requested = make_schema(&["col_a"]);
        let required = make_schema(&["col_a", "col_b"]);
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_requested_schema(requested);
        handler.required_schema = Some(required);

        assert!(handler.get_output_converter().is_some());
    }

    // =========================================================================
    // prepare_required_schema end-to-end tests
    // =========================================================================

    #[test]
    fn test_prepare_required_schema_event_time() {
        let table_schema = make_table_schema();
        let requested_schema = make_schema(&["begin_lat", "rider"]);

        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema)
            .with_data_schema(make_table_schema())
            .with_requested_schema(requested_schema);

        handler.prepare_required_schema(
            true, // has log files
            &["_hoodie_record_key".to_string()],
            &["timestamp".to_string()],
            &HashMap::new(),
            false,
            "EVENT_TIME_ORDERING",
        );

        let required = handler.required_schema.as_ref().unwrap();
        let field_names: Vec<&str> = required.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"begin_lat"));
        assert!(field_names.contains(&"rider"));
        assert!(field_names.contains(&"_hoodie_record_key"));
        assert!(field_names.contains(&"timestamp"));
        assert!(field_names.contains(&"_hoodie_is_deleted"));

        // schema_for_updates should equal required_schema
        assert_eq!(handler.schema_for_updates(), handler.required_schema.as_ref());
        // delete_context should be stored
        assert!(handler.delete_context().is_some());
    }

    #[test]
    fn test_prepare_required_schema_commit_time() {
        let table_schema = make_table_schema();
        let requested_schema = make_schema(&["begin_lat", "rider"]);

        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema)
            .with_data_schema(make_table_schema())
            .with_requested_schema(requested_schema);

        handler.prepare_required_schema(
            true,
            &["_hoodie_record_key".to_string()],
            &["timestamp".to_string()],
            &HashMap::new(),
            false,
            "COMMIT_TIME_ORDERING",
        );

        let required = handler.required_schema.as_ref().unwrap();
        let field_names: Vec<&str> = required.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"begin_lat"));
        assert!(field_names.contains(&"rider"));
        assert!(field_names.contains(&"_hoodie_record_key"));
        assert!(
            !field_names.contains(&"timestamp"),
            "COMMIT_TIME_ORDERING should NOT include ordering field"
        );
        assert!(field_names.contains(&"_hoodie_is_deleted"));
    }

    /// prepare_required_schema with instant range stores DeleteContext and adds commit time.
    #[test]
    fn test_prepare_required_schema_with_instant_range() {
        let table_schema = make_table_schema();
        let requested_schema = make_schema(&["begin_lat", "rider"]);

        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema)
            .with_data_schema(make_table_schema())
            .with_requested_schema(requested_schema);

        handler.prepare_required_schema(
            true, // MOR
            &["_hoodie_record_key".to_string()],
            &[],
            &HashMap::new(),
            true, // has instant range
            "COMMIT_TIME_ORDERING",
        );

        let required = handler.required_schema.as_ref().unwrap();
        let field_names: Vec<&str> = required.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            field_names.contains(&"_hoodie_commit_time"),
            "should add _hoodie_commit_time when instant range is active"
        );

        // DeleteContext should be stored
        let dc = handler.delete_context().unwrap();
        assert!(dc.has_built_in_delete_field);
    }
}
