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
pub mod resolver;

pub fn prepend_meta_fields(schema: SchemaRef) -> Result<Schema> {
    let meta_field_schema = MetaField::schema();
    Schema::try_merge([meta_field_schema.as_ref().clone(), schema.as_ref().clone()])
        .map_err(CoreError::ArrowError)
}

// TODO use this when applicable, like some table config says there is an operation field
pub fn prepend_meta_fields_with_operation(schema: SchemaRef) -> Result<Schema> {
    let meta_field_schema = MetaField::schema_with_operation();
    Schema::try_merge([meta_field_schema.as_ref().clone(), schema.as_ref().clone()])
        .map_err(CoreError::ArrowError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use hudi_test::assert_arrow_field_names_eq;
    use std::sync::Arc;

    #[test]
    fn test_prepend_meta_fields() {
        let schema = Schema::new(vec![Field::new("field1", DataType::Int32, false)]);
        let new_schema = prepend_meta_fields(Arc::new(schema)).unwrap();
        assert_arrow_field_names_eq!(
            new_schema,
            [MetaField::field_names(), vec!["field1"]].concat()
        )
    }

    #[test]
    fn test_prepend_meta_fields_with_operation() {
        let schema = Schema::new(vec![Field::new("field1", DataType::Int32, false)]);
        let new_schema = prepend_meta_fields_with_operation(Arc::new(schema)).unwrap();
        assert_arrow_field_names_eq!(
            new_schema,
            [MetaField::field_names_with_operation(), vec!["field1"]].concat()
        )
    }
}
