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
use crate::metadata::meta_field::MetaField;
use apache_avro::schema::Schema as AvroSchema;
use apache_avro::types::Value as AvroValue;
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use once_cell::sync::Lazy;
use serde_json::Value as JsonValue;
use std::fs;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

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
    DELETE_RECORD_LIST_AVRO_SCHEMA
        .as_ref()
        .map_err(|e| CoreError::Schema(e.to_string()))
}

/// Transforms a RecordBatch representing delete records into a new RecordBatch
pub fn transform_delete_record_batch(
    batch: &RecordBatch,
    commit_time: &str,
    ordering_field: &str,
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();

    // Create the new _hoodie_commit_time column
    let commit_time_array = Arc::new(StringArray::from(vec![commit_time.to_string(); num_rows]));

    // Get the original column data directly by position
    let record_key_array = batch.column(0).clone(); // recordKey at pos 0
    let partition_path_array = batch.column(1).clone(); // partitionPath at pos 1
    let ordering_val_array = batch.column(2).clone(); // orderingVal at pos 2

    // Create new columns vector with the new order
    let new_columns = vec![
        commit_time_array,
        record_key_array,
        partition_path_array,
        ordering_val_array,
    ];

    let new_fields = vec![
        Arc::new(Field::new(
            MetaField::CommitTime.as_ref(),
            DataType::Utf8,
            true,
        )),
        Arc::new(Field::new(
            MetaField::RecordKey.as_ref(),
            DataType::Utf8,
            true,
        )),
        Arc::new(Field::new(
            MetaField::PartitionPath.as_ref(),
            DataType::Utf8,
            true,
        )),
        Arc::new(Field::new(
            ordering_field,
            batch.schema().field(2).data_type().clone(),
            batch.schema().field(2).is_nullable(),
        )),
    ];
    let new_schema = SchemaRef::from(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns).map_err(CoreError::ArrowError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::schema::{DecimalSchema, RecordField, RecordSchema};
    use arrow_array::{Array, Int64Array};

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

    fn create_test_delete_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("recordKey", DataType::Utf8, false),
            Field::new("partitionPath", DataType::Utf8, true),
            Field::new("orderingVal", DataType::Int64, false),
        ]))
    }

    // Helper function to create a test RecordBatch
    fn create_test_delete_record_batch() -> RecordBatch {
        let schema = create_test_delete_schema();

        let record_keys = Arc::new(StringArray::from(vec!["key1", "key2", "key3"]));
        let partition_paths = Arc::new(StringArray::from(vec![
            Some("path1"),
            Some("path2"),
            Some("path3"),
        ]));
        let ordering_vals = Arc::new(Int64Array::from(vec![100, 200, 300]));

        RecordBatch::try_new(schema, vec![record_keys, partition_paths, ordering_vals]).unwrap()
    }

    // Helper function to create empty RecordBatch
    fn create_empty_delete_record_batch() -> RecordBatch {
        let schema = create_test_delete_schema();

        let record_keys = Arc::new(StringArray::from(Vec::<&str>::new()));
        let partition_paths = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
        let ordering_vals = Arc::new(Int64Array::from(Vec::<i64>::new()));

        RecordBatch::try_new(schema, vec![record_keys, partition_paths, ordering_vals]).unwrap()
    }

    #[test]
    fn test_transform_delete_record_batch_basic() {
        let batch = create_test_delete_record_batch();
        let commit_time = "20240101000000";
        let ordering_field = "sequenceNumber";

        let result = transform_delete_record_batch(&batch, commit_time, ordering_field).unwrap();

        // Check number of rows preserved
        assert_eq!(result.num_rows(), 3);

        // Check number of columns (should be 4: commit_time + original 2 + renamed ordering field)
        assert_eq!(result.num_columns(), 4);

        // Check schema field names
        let schema = result.schema();
        assert_eq!(schema.field(0).name(), MetaField::CommitTime.as_ref());
        assert_eq!(schema.field(1).name(), MetaField::RecordKey.as_ref());
        assert_eq!(schema.field(2).name(), MetaField::PartitionPath.as_ref());
        assert_eq!(schema.field(3).name(), ordering_field);

        // Check commit_time column values
        let commit_time_array = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(commit_time_array.len(), 3);
        assert_eq!(commit_time_array.value(0), commit_time);
        assert_eq!(commit_time_array.value(1), commit_time);
        assert_eq!(commit_time_array.value(2), commit_time);

        // Check record key values preserved
        let record_key_array = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(record_key_array.value(0), "key1");
        assert_eq!(record_key_array.value(1), "key2");
        assert_eq!(record_key_array.value(2), "key3");

        // Check partition path values preserved (including nulls)
        let partition_path_array = result
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(partition_path_array.value(0), "path1");
        assert_eq!(partition_path_array.value(1), "path2");
        assert_eq!(partition_path_array.value(2), "path3");

        // Check ordering value column values preserved
        let ordering_val_array = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ordering_val_array.value(0), 100);
        assert_eq!(ordering_val_array.value(1), 200);
        assert_eq!(ordering_val_array.value(2), 300);
    }

    #[test]
    fn test_transform_delete_record_batch_empty() {
        let batch = create_empty_delete_record_batch();
        let commit_time = "20240101000000";
        let ordering_field = "sequenceNumber";

        let result = transform_delete_record_batch(&batch, commit_time, ordering_field).unwrap();

        // Check empty batch handling
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 4);

        // Check schema is still correct
        let schema = result.schema();
        assert_eq!(schema.field(0).name(), MetaField::CommitTime.as_ref());
        assert_eq!(schema.field(1).name(), MetaField::RecordKey.as_ref());
        assert_eq!(schema.field(2).name(), MetaField::PartitionPath.as_ref());
        assert_eq!(schema.field(3).name(), "sequenceNumber");

        // Check all arrays are empty
        for i in 0..4 {
            assert_eq!(result.column(i).len(), 0);
        }
    }

    #[test]
    fn test_commit_time_values() {
        let batch = create_test_delete_record_batch();
        let commit_times = ["20240101000000", "20231225123045", "20240630235959"];

        for commit_time in commit_times {
            let result = transform_delete_record_batch(&batch, commit_time, "seq").unwrap();

            let commit_time_array = result
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            // All rows should have the same commit time
            for i in 0..result.num_rows() {
                assert_eq!(commit_time_array.value(i), commit_time);
            }
        }
    }
}
