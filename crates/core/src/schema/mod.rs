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
use crate::error::{CoreError, Result};
use crate::metadata::meta_field::MetaField;
use arrow_schema::{Schema, SchemaRef};

pub mod delete;

pub fn prepend_meta_fields(schema: SchemaRef) -> Result<Schema> {
    let meta_field_schema = MetaField::schema();
    Schema::try_merge([meta_field_schema.as_ref().clone(), schema.as_ref().clone()])
        .map_err(CoreError::ArrowError)
}

pub fn prepend_meta_fields_with_operation(schema: SchemaRef) -> Result<Schema> {
    let meta_field_schema = MetaField::schema_with_operation();
    Schema::try_merge([meta_field_schema.as_ref().clone(), schema.as_ref().clone()])
        .map_err(CoreError::ArrowError)
}
