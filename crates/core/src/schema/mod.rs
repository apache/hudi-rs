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
use crate::Result;
use apache_avro::schema::Schema as AvroSchema;
use apache_avro::types::Value as AvroValue;
use std::fs;
use std::fs::File;
use std::path::PathBuf;

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

    // Load the base schema file
    let schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("schemas")
        .join("HoodieDeleteRecord.avsc");

    let content = fs::read_to_string(schema_path)
        .map_err(|e| CoreError::Schema(format!("Failed to read schema file: {}", e)))?;

    let mut json: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| CoreError::Schema(format!("Failed to parse schema JSON: {}", e)))?;

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

pub fn avro_schema_for_delete_record_list() -> Result<AvroSchema> {
    let schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("schemas")
        .join("HoodieDeleteRecordList.avsc");
    let schema = AvroSchema::parse_reader(&mut File::open(schema_path)?)?;
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::schema::{Name, ResolvedSchema};

    #[test]
    fn test_schema_for_delete_record_list() {
        let schema = avro_schema_for_delete_record_list().unwrap();
        let schema = ResolvedSchema::try_from(&schema).unwrap();
        let name = Name::from("org.apache.hudi.avro.model.HoodieDeleteRecord");
        let item_schema = schema.get_names().get(&name).unwrap();
        let item_schema = ResolvedSchema::try_from(*item_schema).unwrap();
        assert_eq!(item_schema.get_schemata().len(), 1);
    }
}
