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
use crate::error::CoreError;
use crate::metadata::meta_field::MetaField;
use crate::util::arrow::{create_row_converter, get_column_arrays};
use crate::Result;
use arrow_array::RecordBatch;
use arrow_row::{RowConverter, Rows};
use arrow_schema::SchemaRef;

pub fn create_record_key_converter(schema: SchemaRef) -> Result<RowConverter> {
    create_row_converter(schema, [MetaField::RecordKey.as_ref()])
}

pub fn create_event_time_ordering_converter(
    schema: SchemaRef,
    ordering_field: &str,
) -> Result<RowConverter> {
    create_row_converter(schema, [ordering_field])
}

pub fn create_commit_time_ordering_converter(schema: SchemaRef) -> Result<RowConverter> {
    create_row_converter(schema, [MetaField::CommitTime.as_ref()])
}

pub fn extract_record_keys(converter: &RowConverter, batch: &RecordBatch) -> Result<Rows> {
    let columns = get_column_arrays(batch, [MetaField::RecordKey.as_ref()])?;
    converter
        .convert_columns(&columns)
        .map_err(CoreError::ArrowError)
}

pub fn extract_event_time_ordering_values(
    converter: &RowConverter,
    batch: &RecordBatch,
    ordering_field: &str,
) -> Result<Rows> {
    let ordering_columns = get_column_arrays(batch, [ordering_field])?;
    converter
        .convert_columns(&ordering_columns)
        .map_err(CoreError::ArrowError)
}

pub fn extract_commit_time_ordering_values(
    converter: &RowConverter,
    batch: &RecordBatch,
) -> Result<Rows> {
    let ordering_columns = get_column_arrays(batch, [MetaField::CommitTime.as_ref()])?;
    converter
        .convert_columns(&ordering_columns)
        .map_err(CoreError::ArrowError)
}
