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
//! Knows how to detect delete operations from schema metadata.
//! Constructed from properties and the reader schema.

use arrow_schema::SchemaRef;

/// Context for detecting delete records in the merge pipeline.
///
/// Built from properties and the reader schema. Tells the buffer
/// "how do I detect a delete in a record?"
#[derive(Debug, Clone)]
pub struct DeleteContext {
    /// Custom delete marker key-value pair, if configured.
    /// When set, a record is considered deleted if it contains this key-value.
    pub custom_delete_marker: Option<(String, String)>,

    /// Whether the schema has a built-in delete field (`_hoodie_is_deleted`).
    pub has_built_in_delete_field: bool,

    /// Position of the `_hoodie_operation` field in the schema, if present.
    pub hoodie_operation_pos: Option<usize>,

    /// The reader schema.
    pub reader_schema: SchemaRef,
}

impl DeleteContext {
    /// Create a `DeleteContext` from the reader schema.
    ///
    /// Inspects the schema for built-in delete fields and operation columns.
    pub fn from_reader_schema(schema: SchemaRef) -> Self {
        let has_built_in_delete_field = schema
            .column_with_name("_hoodie_is_deleted")
            .is_some();

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
}
