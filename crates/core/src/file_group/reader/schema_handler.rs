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

use arrow_schema::SchemaRef;

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
}
