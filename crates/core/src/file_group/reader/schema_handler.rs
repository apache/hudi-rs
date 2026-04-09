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
//! table schema, requested schema, required schema, and output projection.
//!
//! ## Java field mapping
//!
//! | Java field          | Rust field          |
//! |---------------------|---------------------|
//! | `tableSchema`       | `table_schema`      |
//! | `requestedSchema`   | `requested_schema`  |
//! | `requiredSchema`    | `required_schema`   |
//! | `schemaForUpdates`  | (= `required_schema`, no separate field) |
//!
//! ## Intentionally omitted (per scope)
//!
//! - `internalSchema` / `internalSchemaOpt` — schema evolution not supported
//! - `deleteContext` — custom delete mode not supported
//! - `hoodieTableConfig` / `metaClient` — replaced by direct parameters

use crate::Result;
use crate::error::CoreError;
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use std::collections::HashSet;
use std::sync::Arc;

// =========================================================================
// OutputProjector (mirrors Java's outputConverter: Option<UnaryOperator<T>>)
// =========================================================================

/// Projects a `RecordBatch` from `requiredSchema` → `requestedSchema`.
///
/// Mirrors Java's `outputConverter` stored on `HoodieFileGroupReader`.
/// Created by [`FileGroupReaderSchemaHandler::get_output_converter()`],
/// which mirrors Java's `FileGroupReaderSchemaHandler.getOutputConverter()`.
#[derive(Debug, Clone)]
pub struct OutputProjector {
    /// The column names to keep in the output (the user's original request).
    requested_columns: Vec<String>,
}

impl OutputProjector {
    /// Project a batch from `requiredSchema` → `requestedSchema`.
    ///
    /// Mirrors Java's `nextVal.project(outputConverter.get())` in
    /// `HoodieFileGroupReader.next()` (line 264).
    pub fn project(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let indices: Vec<usize> = self
            .requested_columns
            .iter()
            .map(|name| {
                schema.index_of(name).map_err(|_| {
                    CoreError::ReadFileSliceError(format!(
                        "Column '{}' not found in output batch for projection",
                        name
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        batch.project(&indices).map_err(|e| {
            CoreError::ReadFileSliceError(format!("Failed to project output: {e}"))
        })
    }
}

// =========================================================================
// FileGroupReaderSchemaHandler
// =========================================================================

/// Schema handler for file group reading.
///
/// Mirrors Java's `FileGroupReaderSchemaHandler<T>`.
///
/// ## Construction
///
/// Matches Java constructor (line 108–125):
/// ```text
/// FileGroupReaderSchemaHandler(readerContext, tableSchema, requestedSchema, ..., metaClient)
///   this.tableSchema = tableSchema
///   this.requestedSchema = requestedSchema
///   this.requiredSchema = prepareRequiredSchema(deleteContext)
///                           └─ generateRequiredSchema(deleteContext)
///                               └─ getMandatoryFieldsForMerging(...)
/// ```
///
/// In Rust, `metaClient.getTableConfig()` is replaced by direct parameters
/// (`record_key_field`, `merge_mode`, etc.) passed to the constructor.
#[derive(Debug, Clone)]
pub struct FileGroupReaderSchemaHandler {
    // ── Mirrors Java fields (lines 72–81) ─────────────────────────────
    /// The full table schema. Java: `@Getter protected final HoodieSchema tableSchema`.
    table_schema: SchemaRef,

    /// The schema requested by the query (column projection).
    /// Java: `@Getter protected final HoodieSchema requestedSchema`.
    requested_schema: SchemaRef,

    /// The required schema for reading = requestedSchema + mandatory merge fields.
    /// Java: `@Getter protected final HoodieSchema requiredSchema`.
    required_schema: SchemaRef,
}

impl FileGroupReaderSchemaHandler {
    /// Construct a schema handler — mirrors Java constructor (lines 108–125).
    ///
    /// ## Parameters (replacing Java's `metaClient.getTableConfig()`)
    ///
    /// | Java source                              | Rust parameter        |
    /// |------------------------------------------|-----------------------|
    /// | `tableSchema` (constructor arg)           | `table_schema`        |
    /// | `requestedSchema` (constructor arg)        | `requested_schema`    |
    /// | `readerContext.getHasLogFiles()`           | `has_log_files`       |
    /// | `readerContext.getInstantRange().isPresent()` | `has_instant_range` |
    /// | `cfg.populateMetaFields()` / `cfg.getRecordKeyFields()` | `record_key_field` |
    /// | `readerContext.getMergeMode()` / `cfg.getRecordMergeMode()` | `merge_mode` |
    pub fn new(
        table_schema: SchemaRef,
        requested_schema: Option<SchemaRef>,
        record_key_field: &str,
        merge_mode: &str,
        has_log_files: bool,
        has_instant_range: bool,
    ) -> Result<Self> {
        // Java line 116–117: this.tableSchema = tableSchema; this.requestedSchema = requestedSchema
        let requested_schema = requested_schema.unwrap_or_else(|| table_schema.clone());

        // Java line 120: this.requiredSchema = prepareRequiredSchema(this.deleteContext)
        let required_schema = Self::prepare_required_schema(
            &table_schema,
            &requested_schema,
            record_key_field,
            merge_mode,
            has_log_files,
            has_instant_range,
        )?;

        log::debug!(
            "[SchemaHandler] new: table_schema={} cols, requested_schema={} cols, \
             required_schema={} cols, merge_mode={}",
            table_schema.fields().len(),
            requested_schema.fields().len(),
            required_schema.fields().len(),
            merge_mode,
        );

        Ok(Self {
            table_schema,
            requested_schema,
            required_schema,
        })
    }

    // ── Java: prepareRequiredSchema (line 259) ────────────────────────

    /// Mirrors Java's `prepareRequiredSchema(DeleteContext)` (line 259–267).
    ///
    /// Calls `generateRequiredSchema` and handles bootstrap reordering.
    /// (Bootstrap reordering is not applicable in Rust — we skip it.)
    fn prepare_required_schema(
        table_schema: &SchemaRef,
        requested_schema: &SchemaRef,
        record_key_field: &str,
        merge_mode: &str,
        has_log_files: bool,
        has_instant_range: bool,
    ) -> Result<SchemaRef> {
        // Java line 260: generateRequiredSchema(deleteContext)
        Self::generate_required_schema(
            table_schema,
            requested_schema,
            record_key_field,
            merge_mode,
            has_log_files,
            has_instant_range,
        )
    }

    // ── Java: generateRequiredSchema (line 167) ───────────────────────

    /// Mirrors Java's `generateRequiredSchema(DeleteContext)` (lines 167–208).
    ///
    /// Logic:
    /// 1. If no log files and no instant range → return requestedSchema as-is
    /// 2. If no log files but has instant range → add `_hoodie_commit_time`
    /// 3. If has log files → add mandatory merge fields via `get_mandatory_fields_for_merging()`
    fn generate_required_schema(
        table_schema: &SchemaRef,
        requested_schema: &SchemaRef,
        record_key_field: &str,
        merge_mode: &str,
        has_log_files: bool,
        has_instant_range: bool,
    ) -> Result<SchemaRef> {
        // Java line 170–176: if no log files, only add commit time if instant range
        if !has_log_files {
            if has_instant_range
                && requested_schema
                    .field_with_name("_hoodie_commit_time")
                    .is_err()
            {
                return Ok(Self::append_fields(
                    requested_schema,
                    table_schema,
                    &["_hoodie_commit_time"],
                ));
            }
            return Ok(requested_schema.clone());
        }

        // If requested == table schema (no projection), mandatory fields are already present.
        // Skip merge mode validation — this is the no-projection path.
        if requested_schema == table_schema {
            return Ok(requested_schema.clone());
        }

        // Java line 188–192: CUSTOM merge → if !projectionCompatible, return tableSchema
        // We only support COMMIT_TIME_ORDERING; reject others.
        if merge_mode != "COMMIT_TIME_ORDERING" {
            return Err(CoreError::Unsupported(format!(
                "Column projection with merge mode '{}' is not supported; \
                 only COMMIT_TIME_ORDERING is supported",
                merge_mode
            )));
        }

        // Java line 194–207: collect mandatory fields not already in requestedSchema
        let mandatory = Self::get_mandatory_fields_for_merging(
            record_key_field,
            has_instant_range,
            table_schema,
        );

        let to_add: Vec<&str> = mandatory
            .iter()
            .filter(|f| requested_schema.field_with_name(f).is_err())
            .map(|s| s.as_str())
            .collect();

        if to_add.is_empty() {
            return Ok(requested_schema.clone());
        }

        // Java line 207: appendFieldsToSchemaDedupNested(requestedSchema, addedFields)
        Ok(Self::append_fields(requested_schema, table_schema, &to_add))
    }

    // ── Java: getMandatoryFieldsForMerging (line 210) ─────────────────

    /// Mirrors Java's `getMandatoryFieldsForMerging(...)` (lines 210–257).
    ///
    /// For COMMIT_TIME_ORDERING, the mandatory fields are:
    /// - record key field (always, for key-based merge) — Java lines 230–237
    /// - `_hoodie_commit_time` (if instant range) — Java line 225–227
    /// - `_hoodie_is_deleted` (if present in table schema) — Java line 244–245
    /// - `_hoodie_operation` (if present in table schema) — Java line 252–253
    ///
    /// Ordering fields are NOT added — Java line 239 only adds them for
    /// `EVENT_TIME_ORDERING`.
    fn get_mandatory_fields_for_merging(
        record_key_field: &str,
        has_instant_range: bool,
        table_schema: &SchemaRef,
    ) -> Vec<String> {
        let table_field_names: HashSet<&str> = table_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        let mut required_fields: Vec<String> = Vec::new();

        // Java line 225–227: commit time for instant range
        if has_instant_range {
            required_fields.push("_hoodie_commit_time".to_string());
        }

        // Java line 230–237: record key field
        // (In Java: cfg.populateMetaFields() → _hoodie_record_key, else cfg.getRecordKeyFields())
        // In Rust: record_key_field is already resolved by the caller.
        if table_field_names.contains(record_key_field) {
            required_fields.push(record_key_field.to_string());
        }

        // Java line 239–242: ordering fields for EVENT_TIME_ORDERING only
        // Skipped — we only support COMMIT_TIME_ORDERING.

        // Java line 244–245: _hoodie_is_deleted if present
        if table_field_names.contains("_hoodie_is_deleted") {
            required_fields.push("_hoodie_is_deleted".to_string());
        }

        // Java line 248–249: custom delete marker — ignored per scope

        // Java line 252–253: _hoodie_operation if present
        if table_field_names.contains("_hoodie_operation") {
            required_fields.push("_hoodie_operation".to_string());
        }

        required_fields
    }

    // ── Java: getOutputConverter (line 127) ───────────────────────────

    /// Mirrors Java's `getOutputConverter()` (lines 127–132).
    ///
    /// ```java
    /// if (!areSchemasProjectionEquivalent(requiredSchema, requestedSchema)) {
    ///     return Option.of(readerContext.getRecordContext().projectRecord(requiredSchema, requestedSchema));
    /// }
    /// return Option.empty();
    /// ```
    pub fn get_output_converter(&self) -> Option<OutputProjector> {
        if self.required_schema == self.requested_schema {
            return None;
        }

        // Build the list of requested column names for the projector
        let requested_columns: Vec<String> = self
            .requested_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        Some(OutputProjector { requested_columns })
    }

    // ── Getters (mirrors Java @Getter annotations) ────────────────────

    /// Java: `@Getter protected final HoodieSchema tableSchema`
    pub fn get_table_schema(&self) -> &SchemaRef {
        &self.table_schema
    }

    /// Java: `@Getter protected final HoodieSchema requestedSchema`
    pub fn get_requested_schema(&self) -> &SchemaRef {
        &self.requested_schema
    }

    /// Java: `@Getter protected final HoodieSchema requiredSchema`
    pub fn get_required_schema(&self) -> &SchemaRef {
        &self.required_schema
    }

    /// Derive required column names from the required schema.
    ///
    /// Used to push column projection to parquet I/O and log block processing.
    pub fn required_column_names(&self) -> Vec<String> {
        self.required_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    // ── Internal helpers ──────────────────────────────────────────────

    /// Append fields from `source_schema` to `base_schema`.
    /// Mirrors Java's `appendFieldsToSchemaDedupNested`.
    fn append_fields(
        base_schema: &SchemaRef,
        source_schema: &SchemaRef,
        field_names: &[&str],
    ) -> SchemaRef {
        let mut fields: Vec<_> = base_schema.fields().iter().cloned().collect();
        for name in field_names {
            if let Ok(field) = source_schema.field_with_name(name) {
                fields.push(Arc::new(field.clone()));
            }
        }
        Arc::new(Schema::new(fields))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};

    fn test_table_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("_hoodie_commit_time", DataType::Utf8, true),
            Field::new("_hoodie_commit_seqno", DataType::Utf8, true),
            Field::new("_hoodie_record_key", DataType::Utf8, true),
            Field::new("_hoodie_partition_path", DataType::Utf8, true),
            Field::new("_hoodie_file_name", DataType::Utf8, true),
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
            Field::new("ts", DataType::Utf8, true),
            Field::new("city", DataType::Utf8, true),
        ]))
    }

    fn requested_schema(cols: &[&str]) -> SchemaRef {
        let table = test_table_schema();
        let fields: Vec<_> = cols
            .iter()
            .map(|name| Arc::new(table.field_with_name(name).unwrap().clone()))
            .collect();
        Arc::new(Schema::new(fields))
    }

    // ── generateRequiredSchema tests ──────────────────────────────────

    /// COMMIT_TIME_ORDERING with log files: adds record key but NOT ordering fields.
    #[test]
    fn test_generate_required_adds_record_key() {
        let handler = FileGroupReaderSchemaHandler::new(
            test_table_schema(),
            Some(requested_schema(&["id", "name"])),
            "_hoodie_record_key",
            "COMMIT_TIME_ORDERING",
            true,  // has_log_files
            false, // has_instant_range
        )
        .unwrap();

        let req = handler.get_required_schema();
        let names: Vec<&str> = req.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name", "_hoodie_record_key"]);
        // ts NOT added — only EVENT_TIME_ORDERING adds ordering fields
        assert!(!names.contains(&"ts"));
    }

    /// requestedSchema already has all mandatory fields → requiredSchema == requestedSchema.
    #[test]
    fn test_no_extra_fields_when_already_requested() {
        let handler = FileGroupReaderSchemaHandler::new(
            test_table_schema(),
            Some(requested_schema(&["id", "_hoodie_record_key"])),
            "_hoodie_record_key",
            "COMMIT_TIME_ORDERING",
            true,
            false,
        )
        .unwrap();

        assert_eq!(
            handler.get_required_schema(),
            handler.get_requested_schema(),
        );
    }

    /// Instant range adds `_hoodie_commit_time`.
    #[test]
    fn test_instant_range_adds_commit_time() {
        let handler = FileGroupReaderSchemaHandler::new(
            test_table_schema(),
            Some(requested_schema(&["id"])),
            "_hoodie_record_key",
            "COMMIT_TIME_ORDERING",
            false, // no log files
            true,  // has instant range
        )
        .unwrap();

        let names: Vec<&str> = handler
            .get_required_schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert!(names.contains(&"_hoodie_commit_time"));
    }

    /// No log files, no instant range → requiredSchema == requestedSchema.
    #[test]
    fn test_base_only_no_extra_fields() {
        let handler = FileGroupReaderSchemaHandler::new(
            test_table_schema(),
            Some(requested_schema(&["id", "name"])),
            "_hoodie_record_key",
            "COMMIT_TIME_ORDERING",
            false,
            false,
        )
        .unwrap();

        assert_eq!(
            handler.get_required_schema(),
            handler.get_requested_schema(),
        );
    }

    /// No requestedSchema (None) → requiredSchema == tableSchema (read all).
    #[test]
    fn test_no_requested_schema_reads_all() {
        let table_schema = test_table_schema();
        let handler = FileGroupReaderSchemaHandler::new(
            table_schema.clone(),
            None, // no projection
            "_hoodie_record_key",
            "COMMIT_TIME_ORDERING",
            true,
            false,
        )
        .unwrap();

        assert_eq!(handler.get_required_schema(), &table_schema);
        assert_eq!(handler.get_requested_schema(), &table_schema);
    }

    /// EVENT_TIME_ORDERING with log files is rejected.
    #[test]
    fn test_rejects_event_time_ordering() {
        let result = FileGroupReaderSchemaHandler::new(
            test_table_schema(),
            Some(requested_schema(&["id"])),
            "_hoodie_record_key",
            "EVENT_TIME_ORDERING",
            true,
            false,
        );
        assert!(result.is_err());
    }

    /// Any merge mode without log files is fine (base-only reads don't merge).
    #[test]
    fn test_any_mode_ok_without_log_files() {
        let handler = FileGroupReaderSchemaHandler::new(
            test_table_schema(),
            Some(requested_schema(&["id"])),
            "_hoodie_record_key",
            "EVENT_TIME_ORDERING",
            false, // no log files
            false,
        )
        .unwrap();

        let names: Vec<&str> = handler
            .get_required_schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["id"]);
    }

    // ── getOutputConverter tests ──────────────────────────────────────

    /// requiredSchema ≠ requestedSchema → output converter present.
    #[test]
    fn test_output_converter_present_when_schemas_differ() {
        let handler = FileGroupReaderSchemaHandler::new(
            test_table_schema(),
            Some(requested_schema(&["id", "name"])),
            "_hoodie_record_key",
            "COMMIT_TIME_ORDERING",
            true,
            false,
        )
        .unwrap();

        let converter = handler.get_output_converter();
        assert!(converter.is_some());
    }

    /// requiredSchema == requestedSchema → no output converter.
    #[test]
    fn test_output_converter_absent_when_schemas_equal() {
        let handler = FileGroupReaderSchemaHandler::new(
            test_table_schema(),
            Some(requested_schema(&["id", "name"])),
            "_hoodie_record_key",
            "COMMIT_TIME_ORDERING",
            false, // no log files
            false, // no instant range
        )
        .unwrap();

        // Base-only, no extra fields → required == requested
        assert!(handler.get_output_converter().is_none());
    }

    /// OutputProjector projects correctly.
    #[test]
    fn test_output_projector_strips_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("_hoodie_record_key", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::new_empty(schema);

        let projector = OutputProjector {
            requested_columns: vec!["id".to_string(), "name".to_string()],
        };
        let projected = projector.project(batch).unwrap();
        assert_eq!(projected.num_columns(), 2);
        let schema = projected.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name"]);
    }
}
