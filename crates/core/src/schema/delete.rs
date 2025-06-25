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
use crate::error::Result;
use apache_avro::schema::Schema as AvroSchema;
use apache_avro::types::Value as AvroValue;
use once_cell::sync::Lazy;
use serde_json::Value as JsonValue;
use std::fs;
use std::fs::File;
use std::path::PathBuf;

static DELETE_RECORD_AVRO_SCHEMA_IN_JSON: Lazy<Result<JsonValue>> = Lazy::new(|| {
    let schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("schemas")
        .join("HoodieDeleteRecord.avsc");

    let content = fs::read_to_string(schema_path)
        .map_err(|e| CoreError::Schema(format!("Failed to read schema file: {}", e)))?;

    serde_json::from_str(&content)
        .map_err(|e| CoreError::Schema(format!("Failed to parse schema to JSON: {}", e)))
});

// TODO further improve perf by using once_cell for the whole function based on table config
// OR
// TODO better make avro-arrow conversion work for multiple union types
pub fn avro_schema_for_delete_record(delete_record_value: &AvroValue) -> Result<AvroSchema> {
    let fields = match delete_record_value {
        AvroValue::Record(fields) => fields,
        _ => {
            return Err(CoreError::Schema(
                "Expected a record for delete record schema".to_string(),
            ))
        }
    };

    // Extract the ordering value type position from the union (always at field position 2)
    let ordering_val = &fields[2].1;
    let ordering_val_type_pos = match ordering_val {
        AvroValue::Union(type_pos, _) => {
            if *type_pos == 0 {
                return Err(CoreError::Schema(
                    "Ordering value type position must not be 0 (null) in delete log block"
                        .to_string(),
                ));
            }
            *type_pos
        }
        _ => {
            return Err(CoreError::Schema(
                "Expected a union for ordering value in delete record schema".to_string(),
            ))
        }
    };

    let json = DELETE_RECORD_AVRO_SCHEMA_IN_JSON
        .as_ref()
        .map_err(|e| CoreError::Schema(e.to_string()))?;
    let mut json = json.clone();

    // Modify the orderingVal type array to keep only null and the selected type
    {
        let type_array = json
            .get_mut("fields")
            .and_then(|v| v.as_array_mut())
            .and_then(|fields| fields.get_mut(2)) // orderingVal is always at position 2
            .and_then(|field| field.get_mut("type"))
            .and_then(|v| v.as_array_mut())
            .ok_or_else(|| {
                CoreError::Schema("Could not access orderingVal type array in schema".to_string())
            })?;

        // Validate bounds
        if ordering_val_type_pos as usize >= type_array.len() {
            return Err(CoreError::Schema(format!(
                "Type position {} is out of bounds (max: {})",
                ordering_val_type_pos,
                type_array.len() - 1
            )));
        }

        // Keep only "null" (index 0) and the type at the specified position
        let null_type = type_array[0].clone();
        let selected_type = type_array[ordering_val_type_pos as usize].clone();
        *type_array = vec![null_type, selected_type];
    }

    // Parse and return the modified schema
    AvroSchema::parse(&json).map_err(CoreError::AvroError)
}

static DELETE_RECORD_LIST_AVRO_SCHEMA: Lazy<Result<AvroSchema>> = Lazy::new(|| {
    let schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("schemas")
        .join("HoodieDeleteRecordList.avsc");

    let mut file = File::open(&schema_path)
        .map_err(|e| CoreError::Schema(format!("Failed to open schema file: {}", e)))?;

    AvroSchema::parse_reader(&mut file).map_err(CoreError::AvroError)
});

pub fn avro_schema_for_delete_record_list() -> Result<&'static AvroSchema> {
    match DELETE_RECORD_LIST_AVRO_SCHEMA.as_ref() {
        Ok(schema) => Ok(schema),
        Err(e) => Err(CoreError::Schema(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::schema::{DecimalSchema, RecordField, RecordSchema};

    fn validate_delete_fields(
        fields: &[RecordField],
        minimized_ordering_field_type: Option<AvroSchema>,
    ) {
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name, "recordKey");
        assert_eq!(fields[1].name, "partitionPath");
        assert_eq!(fields[2].name, "orderingVal");

        let record_key_field = &fields[0];
        match record_key_field.clone().schema {
            AvroSchema::Union(union) => {
                assert_eq!(union.variants().len(), 2);
                assert_eq!(union.variants()[0], AvroSchema::Null);
                assert_eq!(union.variants()[1], AvroSchema::String);
            }
            _ => panic!("Expected a Union schema for recordKey"),
        }

        let partition_path_field = &fields[1];
        match partition_path_field.clone().schema {
            AvroSchema::Union(union) => {
                assert_eq!(union.variants().len(), 2);
                assert_eq!(union.variants()[0], AvroSchema::Null);
                assert_eq!(union.variants()[1], AvroSchema::String);
            }
            _ => panic!("Expected a Union schema for partitionPath"),
        }

        let ordering_field = &fields[2];
        match ordering_field.clone().schema {
            AvroSchema::Union(union) => {
                if let Some(minimized_type) = minimized_ordering_field_type {
                    assert_eq!(union.variants().len(), 2);
                    assert_eq!(union.variants()[0], AvroSchema::Null);
                    assert_eq!(union.variants()[1], minimized_type);
                } else {
                    assert_eq!(union.variants().len(), 13);
                    assert_eq!(union.variants()[0], AvroSchema::Null);
                    assert_eq!(union.variants()[1], AvroSchema::Int);
                    assert_eq!(union.variants()[2], AvroSchema::Long);
                    assert_eq!(union.variants()[3], AvroSchema::Float);
                    assert_eq!(union.variants()[4], AvroSchema::Double);
                    assert_eq!(union.variants()[5], AvroSchema::Bytes);
                    assert_eq!(union.variants()[6], AvroSchema::String);
                    assert_eq!(
                        union.variants()[7],
                        AvroSchema::Decimal(DecimalSchema {
                            precision: 30,
                            scale: 15,
                            inner: Box::new(AvroSchema::Bytes)
                        })
                    );
                    assert_eq!(union.variants()[8], AvroSchema::Date);
                    assert_eq!(union.variants()[9], AvroSchema::TimeMillis);
                    assert_eq!(union.variants()[10], AvroSchema::TimeMicros);
                    assert_eq!(union.variants()[11], AvroSchema::TimestampMillis);
                    assert_eq!(union.variants()[12], AvroSchema::TimestampMicros);
                }
            }
            _ => panic!("Expected a Union schema for orderingVal"),
        }
    }

    #[test]
    fn test_schema_for_delete_record() {
        let delete_record_value = AvroValue::Record(vec![
            (
                "recordKey".to_string(),
                AvroValue::String("key1".to_string()),
            ),
            (
                "partitionPath".to_string(),
                AvroValue::String("path1".to_string()),
            ),
            (
                "orderingVal".to_string(),
                AvroValue::Union(1, Box::new(AvroValue::Int(42))),
            ),
        ]);

        let schema = avro_schema_for_delete_record(&delete_record_value).unwrap();
        let schema_name = schema.name().unwrap().clone();
        assert_eq!(schema_name.namespace.unwrap(), "org.apache.hudi.avro.model");
        assert_eq!(schema_name.name, "HoodieDeleteRecord");

        match schema {
            AvroSchema::Record(RecordSchema { fields, .. }) => {
                validate_delete_fields(&fields, Some(AvroSchema::Int));
            }
            _ => panic!("Expected a Record schema"),
        }
    }

    #[test]
    fn test_schema_for_delete_record_list() {
        let schema = avro_schema_for_delete_record_list().unwrap();
        let schema_name = schema.name().unwrap().clone();
        assert_eq!(schema_name.namespace.unwrap(), "org.apache.hudi.avro.model");
        assert_eq!(schema_name.name, "HoodieDeleteRecordList");

        match schema {
            AvroSchema::Record(RecordSchema { fields, .. }) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name, "deleteRecordList");
                let delete_record_list_field = &fields[0];
                match delete_record_list_field.clone().schema {
                    AvroSchema::Array(array_schema) => {
                        let item_schema_name = array_schema.items.name().unwrap().clone();
                        assert_eq!(
                            item_schema_name.namespace.unwrap(),
                            "org.apache.hudi.avro.model"
                        );
                        assert_eq!(item_schema_name.name, "HoodieDeleteRecord");

                        let item_schema = array_schema.items;
                        match item_schema.as_ref() {
                            AvroSchema::Record(RecordSchema { fields, .. }) => {
                                validate_delete_fields(fields, None);
                            }
                            _ => panic!("Expected a Record schema for items in deleteRecordList"),
                        }
                    }
                    _ => panic!("Expected an Array schema for deleteRecordList"),
                }
            }
            _ => panic!("Expected a Record schema at the top level"),
        }
    }
}
