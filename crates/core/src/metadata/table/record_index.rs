// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Decoding and routing of global record-index entries.

use crate::{Result, error::CoreError};
use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray, StructArray};
use std::collections::HashSet;

/// An RLI location. A position is a hint until checked against the selected base file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordIndexLocation {
    /// Data-table record key.
    pub key: String,
    /// Data-table partition path.
    pub partition_path: String,
    /// Data-table file-group identifier.
    pub file_id: String,
    /// Indexed location instant, in epoch milliseconds.
    pub instant_time_millis: i64,
    /// Optional zero-based physical row ordinal.
    pub position: Option<u64>,
}

/// Map a key to Hudi's physical metadata shard, independently of execution partitions.
/// Returns an error for an empty or unrepresentably large shard set.
pub fn record_index_shard(key: &str, shard_count: usize) -> Result<usize> {
    if shard_count == 0 || shard_count > i32::MAX as usize {
        return Err(CoreError::MetadataTable(format!(
            "Invalid RLI shard count: {shard_count}"
        )));
    }
    Ok(super::routing::file_group_index(key, shard_count))
}

fn invalid(field: &str) -> CoreError {
    CoreError::MetadataTable(format!("Missing, null, or invalid RLI field '{field}'"))
}

pub(crate) fn decode(batch: &RecordBatch, keys: &[&str]) -> Result<Vec<RecordIndexLocation>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let wanted: HashSet<&str> = keys.iter().copied().collect();
    let key_array = arrow_cast::cast(
        batch.column_by_name("key").ok_or_else(|| invalid("key"))?,
        &arrow_schema::DataType::Utf8,
    )?;
    let key_array = key_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid("key"))?;
    let types = batch
        .column_by_name("type")
        .and_then(|a| a.as_any().downcast_ref::<Int32Array>())
        .ok_or_else(|| invalid("type"))?;
    let payload = batch
        .column_by_name("recordIndexMetadata")
        .and_then(|a| a.as_any().downcast_ref::<StructArray>())
        .ok_or_else(|| invalid("recordIndexMetadata"))?;
    let mut result = Vec::new();
    for row in 0..batch.num_rows() {
        if key_array.is_null(row) {
            return Err(invalid("key"));
        }
        let key = key_array.value(row);
        if !wanted.contains(key) {
            continue;
        }
        if types.is_null(row) || types.value(row) != 5 || payload.is_null(row) {
            return Err(invalid("recordIndexMetadata"));
        }
        let integer = |name: &str| -> Result<i64> {
            let a = payload.column_by_name(name).ok_or_else(|| invalid(name))?;
            if a.is_null(row) {
                return Err(invalid(name));
            }
            if let Some(a) = a.as_any().downcast_ref::<Int64Array>() {
                return Ok(a.value(row));
            }
            if let Some(a) = a.as_any().downcast_ref::<Int32Array>() {
                return Ok(i64::from(a.value(row)));
            }
            Err(invalid(name))
        };
        let string = |name: &str| -> Result<String> {
            let a = payload.column_by_name(name).ok_or_else(|| invalid(name))?;
            if a.is_null(row) {
                return Err(invalid(name));
            }
            let a = arrow_cast::cast(a, &arrow_schema::DataType::Utf8)?;
            Ok(a.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid(name))?
                .value(row)
                .to_string())
        };
        let file_id = match integer("fileIdEncoding")? {
            0 => {
                let bits = ((integer("fileIdHighBits")? as u64 as u128) << 64)
                    | integer("fileIdLowBits")? as u64 as u128;
                let hex = format!("{bits:032x}");
                let uuid = format!(
                    "{}-{}-{}-{}-{}",
                    &hex[..8],
                    &hex[8..12],
                    &hex[12..16],
                    &hex[16..20],
                    &hex[20..]
                );
                match integer("fileIndex")? {
                    -1 => uuid,
                    index if index >= 0 => format!("{uuid}-{index}"),
                    _ => return Err(invalid("fileIndex")),
                }
            }
            1 => string("fileId")?,
            _ => return Err(invalid("fileIdEncoding")),
        };
        let position = match payload.column_by_name("position") {
            None => None,
            Some(a) if a.is_null(row) => None,
            Some(_) => match integer("position")? {
                -1 => None,
                value if value >= 0 => Some(value as u64),
                _ => return Err(invalid("position")),
            },
        };
        result.push(RecordIndexLocation {
            key: key.to_string(),
            partition_path: string("partitionName")?,
            file_id,
            instant_time_millis: integer("instantTime")?,
            position,
        });
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ArrayRef;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn batch(encoding: i32, position: Option<i64>, file_index: i32) -> RecordBatch {
        let mut fields: Vec<(&str, ArrayRef)> = vec![
            (
                "partitionName",
                Arc::new(StringArray::from(vec!["city=sf"])),
            ),
            ("fileId", Arc::new(StringArray::from(vec!["raw-file-7"]))),
            ("fileIdEncoding", Arc::new(Int32Array::from(vec![encoding]))),
            ("fileIdHighBits", Arc::new(Int64Array::from(vec![-1]))),
            ("fileIdLowBits", Arc::new(Int64Array::from(vec![0]))),
            ("fileIndex", Arc::new(Int32Array::from(vec![file_index]))),
            ("instantTime", Arc::new(Int64Array::from(vec![1234]))),
        ];
        if let Some(value) = position {
            fields.push(("position", Arc::new(Int64Array::from(vec![value]))));
        }
        let fields = fields
            .into_iter()
            .map(|(name, a)| (Arc::new(Field::new(name, a.data_type().clone(), true)), a))
            .collect::<Vec<_>>();
        let payload = Arc::new(StructArray::from(fields));
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", arrow_schema::DataType::Utf8, false),
                Field::new("type", arrow_schema::DataType::Int32, false),
                Field::new("recordIndexMetadata", payload.data_type().clone(), true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["k"])),
                Arc::new(Int32Array::from(vec![5])),
                payload,
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_decode_record_index_encodings_and_optional_positions() {
        let raw = decode(&batch(1, None, 0), &["k"]).unwrap();
        assert_eq!(raw[0].file_id, "raw-file-7");
        assert_eq!(raw[0].position, None);
        assert_eq!(raw[0].partition_path, "city=sf");
        assert_eq!(raw[0].instant_time_millis, 1234);
        let uuid = decode(&batch(0, Some(42), 7), &["k"]).unwrap();
        assert_eq!(uuid[0].file_id, "ffffffff-ffff-ffff-0000-000000000000-7");
        assert_eq!(uuid[0].position, Some(42));
        let legacy = decode(&batch(0, Some(-1), -1), &["k"]).unwrap();
        assert_eq!(legacy[0].file_id, "ffffffff-ffff-ffff-0000-000000000000");
        assert_eq!(legacy[0].position, None);
    }

    #[test]
    fn test_decode_record_index_rejects_invalid_payload_and_filters_extra_keys() {
        assert!(decode(&batch(2, None, 0), &["k"]).is_err());
        assert!(decode(&batch(0, Some(-2), 0), &["k"]).is_err());
        assert!(decode(&batch(0, None, -2), &["k"]).is_err());
        assert!(decode(&batch(1, None, 0), &["other"]).unwrap().is_empty());
    }
}
