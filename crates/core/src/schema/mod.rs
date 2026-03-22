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
use crate::schema::resolver::sanitize_avro_schema_str;
use arrow_schema::{Schema, SchemaRef};
use serde_json::Value;

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

pub fn prepend_meta_fields_to_avro_schema_str(avro_schema_str: &str) -> Result<String> {
    let mut schema: Value = serde_json::from_str(&sanitize_avro_schema_str(avro_schema_str))
        .map_err(|e| CoreError::Schema(format!("Failed to parse Avro schema JSON: {e}")))?;

    let fields = schema
        .get_mut("fields")
        .and_then(|f| f.as_array_mut())
        .ok_or_else(|| CoreError::Schema("Avro schema has no 'fields' array".to_string()))?;

    let meta_field_defs: Vec<Value> = MetaField::field_names()
        .iter()
        .map(|name| {
            serde_json::json!({
                "name": name,
                "type": ["null", "string"],
                "default": null
            })
        })
        .collect();

    let existing_names: std::collections::HashSet<&str> = fields
        .iter()
        .filter_map(|f| f.get("name").and_then(|n| n.as_str()))
        .collect();

    let new_meta_fields: Vec<Value> = meta_field_defs
        .into_iter()
        .filter(|f| {
            f.get("name")
                .and_then(|n| n.as_str())
                .is_none_or(|name| !existing_names.contains(name))
        })
        .collect();

    let mut all_fields = new_meta_fields;
    all_fields.append(fields);
    *fields = all_fields;

    serde_json::to_string(&schema)
        .map_err(|e| CoreError::Schema(format!("Failed to serialize Avro schema: {e}")))
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

    #[test]
    fn test_prepend_meta_fields_to_avro_schema_str() {
        let avro_schema =
            r#"{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"int"}]}"#;
        let result = prepend_meta_fields_to_avro_schema_str(avro_schema).unwrap();
        let parsed: Value = serde_json::from_str(&result).unwrap();
        let fields = parsed["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 6, "Expected 5 meta fields + 1 data field");
        assert_eq!(fields[0]["name"], "_hoodie_commit_time");
        assert_eq!(fields[5]["name"], "id");
    }

    #[test]
    fn test_prepend_meta_fields_to_avro_schema_str_dedup() {
        let avro_schema = r#"{"type":"record","name":"TestRecord","fields":[{"name":"_hoodie_commit_time","type":["null","string"],"default":null},{"name":"id","type":"int"}]}"#;
        let result = prepend_meta_fields_to_avro_schema_str(avro_schema).unwrap();
        let parsed: Value = serde_json::from_str(&result).unwrap();
        let fields = parsed["fields"].as_array().unwrap();
        assert_eq!(
            fields.len(),
            6,
            "Expected 5 meta fields + 1 data field (deduped existing meta field)"
        );
        // The existing _hoodie_commit_time should not be duplicated
        let commit_time_count = fields
            .iter()
            .filter(|f| f["name"] == "_hoodie_commit_time")
            .count();
        assert_eq!(commit_time_count, 1);
    }
}
