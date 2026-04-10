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
use arrow_schema::{Field, Schema, SchemaRef};
use std::sync::Arc;

/// Schema handler for file group reading.
///
/// Manages the different schema views needed during the read pipeline:
/// - **table_schema**: The full schema of the Hudi table.
/// - **requested_schema**: The schema requested by the query (column projection).
/// - **required_schema**: The minimum schema required for reading (includes
///   merge keys and ordering fields even if not in requested_schema).
/// - **data_schema**: The schema used for reading base files.
///
/// In Java Hudi, this class also handles internal schema evolution.
/// That is not yet implemented in hudi-rs.
#[derive(Debug, Clone)]
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
}

/// Fields that may need to be included in the required schema for delete detection.
///
/// Mirrors Java's `getMandatoryFieldsForMerging()` logic in
/// `FileGroupReaderSchemaHandler` (lines 231-278).
const DELETE_MARKER_FIELDS: &[&str] = &["_hoodie_is_deleted", "_hoodie_operation"];

impl Default for FileGroupReaderSchemaHandler {
    fn default() -> Self {
        Self {
            table_schema: None,
            requested_schema: None,
            data_schema: None,
            required_schema: None,
        }
    }
}

impl FileGroupReaderSchemaHandler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_data_schema(mut self, schema: SchemaRef) -> Self {
        self.data_schema = Some(schema);
        self
    }

    pub fn with_requested_schema(mut self, schema: SchemaRef) -> Self {
        self.requested_schema = Some(schema);
        self
    }

    /// Ensure the data/required schema includes fields needed for delete detection.
    ///
    /// Mirrors part of Java's `FileGroupReaderSchemaHandler.generateRequiredSchema()`
    /// + `getMandatoryFieldsForMerging()`. If the table schema contains delete marker
    /// fields (`_hoodie_is_deleted`, `_hoodie_operation`, custom delete key) that are
    /// not in the data schema, this method adds them.
    ///
    /// Also respects the `DeleteContext` for custom delete marker fields.
    ///
    /// Should be called after `table_schema` and `data_schema` are set.
    pub fn ensure_delete_fields(&mut self, delete_context: Option<&DeleteContext>) {
        let table_schema = match &self.table_schema {
            Some(s) => s.clone(),
            None => return,
        };

        let data_schema = match &self.data_schema {
            Some(s) => s.clone(),
            None => return,
        };

        let mut extra_fields: Vec<Field> = Vec::new();

        // Add built-in delete marker fields if they exist in the table schema
        // but are missing from the data schema.
        for &field_name in DELETE_MARKER_FIELDS {
            if data_schema.column_with_name(field_name).is_none() {
                if let Some((_, field)) = table_schema.column_with_name(field_name) {
                    extra_fields.push(field.clone());
                }
            }
        }

        // Add custom delete marker field if configured and missing.
        if let Some(ctx) = delete_context {
            if let Some((key_field, _)) = &ctx.custom_delete_marker {
                if data_schema.column_with_name(key_field).is_none() {
                    if let Some((_, field)) = table_schema.column_with_name(key_field) {
                        extra_fields.push(field.clone());
                    }
                }
            }
        }

        if !extra_fields.is_empty() {
            let mut fields: Vec<Arc<Field>> = data_schema.fields().to_vec();
            fields.extend(extra_fields.into_iter().map(Arc::new));
            let new_schema: SchemaRef = Arc::new(Schema::new(fields));
            self.data_schema = Some(new_schema);
        }
    }
}
