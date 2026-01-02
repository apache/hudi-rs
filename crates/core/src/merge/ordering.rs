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
use crate::Result;
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig;
use crate::record::{
    extract_commit_time_ordering_values, extract_event_time_ordering_values, extract_record_keys,
};
use arrow_array::{
    Array, Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow_row::{OwnedRow, Row, RowConverter};
use arrow_schema::DataType;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MaxOrderingInfo {
    event_time_ordering: OwnedRow,
    commit_time_ordering: OwnedRow,
    is_event_time_zero: bool,
}

impl MaxOrderingInfo {
    pub fn is_greater_than(&self, event_time: Row, commit_time: Row) -> bool {
        if self.is_event_time_zero {
            self.commit_time_ordering.row() > commit_time
        } else {
            self.event_time_ordering.row() > event_time
                || (self.event_time_ordering.row() == event_time
                    && self.commit_time_ordering.row() > commit_time)
        }
    }
}

pub fn process_batch_for_max_orderings(
    batch: &RecordBatch,
    max_ordering: &mut HashMap<OwnedRow, MaxOrderingInfo>,
    key_converter: &RowConverter,
    event_time_converter: &RowConverter,
    commit_time_converter: &RowConverter,
    hudi_configs: Arc<HudiConfigs>,
) -> Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }

    let ordering_field: String = hudi_configs.get(HudiTableConfig::PrecombineField)?.into();

    let keys = extract_record_keys(key_converter, batch)?;
    let event_times =
        extract_event_time_ordering_values(event_time_converter, batch, &ordering_field)?;
    let commit_times = extract_commit_time_ordering_values(commit_time_converter, batch)?;
    for i in 0..batch.num_rows() {
        let key = keys.row(i).owned();
        let event_time = event_times.row(i).owned();
        let commit_time = commit_times.row(i).owned();
        let is_event_time_zero = is_event_time_zero(event_time.row(), event_time_converter)?;

        match max_ordering.get_mut(&key) {
            Some(info) => {
                if event_time > info.event_time_ordering {
                    info.event_time_ordering = event_time;
                    info.is_event_time_zero = is_event_time_zero;
                }
                if commit_time > info.commit_time_ordering {
                    info.commit_time_ordering = commit_time;
                }
            }
            None => {
                max_ordering.insert(
                    key,
                    MaxOrderingInfo {
                        event_time_ordering: event_time,
                        commit_time_ordering: commit_time,
                        is_event_time_zero,
                    },
                );
            }
        }
    }

    Ok(())
}

pub fn is_event_time_zero(
    event_time_row: Row,
    event_time_converter: &RowConverter,
) -> Result<bool> {
    let event_times = event_time_converter.convert_rows([event_time_row])?;
    assert_eq!(
        event_times.len(),
        1,
        "Expected exactly one row for event time conversion"
    );
    let event_time = &event_times[0];

    let is_zero = match event_time.data_type() {
        DataType::Int8 => {
            event_time
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0)
                == 0
        }
        DataType::Int16 => {
            event_time
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(0)
                == 0
        }
        DataType::Int32 => {
            event_time
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
                == 0
        }
        DataType::Int64 => {
            event_time
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
                == 0
        }
        DataType::UInt8 => {
            event_time
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(0)
                == 0
        }
        DataType::UInt16 => {
            event_time
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(0)
                == 0
        }
        DataType::UInt32 => {
            event_time
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(0)
                == 0
        }
        DataType::UInt64 => {
            event_time
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0)
                == 0
        }
        _ => false, // not an integer type
    };

    Ok(is_zero)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_row::{RowConverter, SortField};
    use std::collections::HashMap;

    use arrow_array::{
        Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use std::sync::Arc;

    // Helper function to create a basic test schema
    fn create_test_delete_schema() -> SchemaRef {
        create_test_delete_schema_with_event_time_type(DataType::UInt32)
    }

    // Helper function to create test schema with different event time data type
    fn create_test_delete_schema_with_event_time_type(event_time_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("_hoodie_commit_time", DataType::Utf8, false),
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("_hoodie_partition_path", DataType::Utf8, false),
            Field::new("an_ordering_field", event_time_type, true),
        ]))
    }

    // Helper function to create a test RecordBatch
    fn create_test_delete_batch() -> RecordBatch {
        let schema = create_test_delete_schema();

        let commit_times = Arc::new(StringArray::from(vec![
            "20240101000000",
            "20240102000000",
            "20240103000000",
        ]));
        let record_keys = Arc::new(StringArray::from(vec!["key1", "key2", "key3"]));
        let partition_paths = Arc::new(StringArray::from(vec!["part1", "part2", "part3"]));
        let ordering_values = Arc::new(UInt32Array::from(vec![100, 0, 300]));

        RecordBatch::try_new(
            schema,
            vec![commit_times, record_keys, partition_paths, ordering_values],
        )
        .unwrap()
    }

    // Helper function to create test batch with specific event time type
    fn create_test_delete_batch_with_event_time_type<T>(
        event_time_type: DataType,
        values: Vec<T>,
    ) -> RecordBatch
    where
        T: Into<i64> + Copy,
    {
        let schema = create_test_delete_schema_with_event_time_type(event_time_type.clone());

        let commit_times = Arc::new(StringArray::from(vec!["20240101000000", "20240102000000"]));
        let record_keys = Arc::new(StringArray::from(vec!["key1", "key2"]));
        let partition_paths = Arc::new(StringArray::from(vec!["part1", "part2"]));

        let ordering_values: Arc<dyn Array> = match event_time_type {
            DataType::Int8 => Arc::new(Int8Array::from(
                values
                    .into_iter()
                    .map(|v| v.into() as i8)
                    .collect::<Vec<_>>(),
            )),
            DataType::Int16 => Arc::new(Int16Array::from(
                values
                    .into_iter()
                    .map(|v| v.into() as i16)
                    .collect::<Vec<_>>(),
            )),
            DataType::Int32 => Arc::new(Int32Array::from(
                values
                    .into_iter()
                    .map(|v| v.into() as i32)
                    .collect::<Vec<_>>(),
            )),
            DataType::Int64 => Arc::new(Int64Array::from(
                values.into_iter().map(|v| v.into()).collect::<Vec<_>>(),
            )),
            DataType::UInt8 => Arc::new(UInt8Array::from(
                values
                    .into_iter()
                    .map(|v| v.into() as u8)
                    .collect::<Vec<_>>(),
            )),
            DataType::UInt16 => Arc::new(UInt16Array::from(
                values
                    .into_iter()
                    .map(|v| v.into() as u16)
                    .collect::<Vec<_>>(),
            )),
            DataType::UInt32 => Arc::new(UInt32Array::from(
                values
                    .into_iter()
                    .map(|v| v.into() as u32)
                    .collect::<Vec<_>>(),
            )),
            DataType::UInt64 => Arc::new(UInt64Array::from(
                values
                    .into_iter()
                    .map(|v| v.into() as u64)
                    .collect::<Vec<_>>(),
            )),
            _ => panic!("Unsupported event time type for test"),
        };

        RecordBatch::try_new(
            schema,
            vec![commit_times, record_keys, partition_paths, ordering_values],
        )
        .unwrap()
    }

    // Helper function to create empty test batch
    fn create_empty_test_delete_batch() -> RecordBatch {
        let schema = create_test_delete_schema();

        let commit_times = Arc::new(StringArray::from(Vec::<&str>::new()));
        let record_keys = Arc::new(StringArray::from(Vec::<&str>::new()));
        let partition_paths = Arc::new(StringArray::from(Vec::<&str>::new()));
        let ordering_values = Arc::new(UInt32Array::from(Vec::<u32>::new()));

        RecordBatch::try_new(
            schema,
            vec![commit_times, record_keys, partition_paths, ordering_values],
        )
        .unwrap()
    }

    // Helper function to create row converters
    fn create_test_converters(schema: SchemaRef) -> (RowConverter, RowConverter, RowConverter) {
        let key_converter = RowConverter::new(vec![
            SortField::new(schema.field(1).data_type().clone()), // _hoodie_record_key
        ])
        .unwrap();

        let event_time_converter = RowConverter::new(vec![
            SortField::new(schema.field(3).data_type().clone()), // an_ordering_field
        ])
        .unwrap();

        let commit_time_converter = RowConverter::new(vec![
            SortField::new(schema.field(0).data_type().clone()), // _hoodie_commit_time
        ])
        .unwrap();

        (key_converter, event_time_converter, commit_time_converter)
    }

    // Helper function to create test HudiConfigs
    fn create_test_hudi_configs() -> Arc<HudiConfigs> {
        Arc::new(HudiConfigs::new([(
            HudiTableConfig::PrecombineField.as_ref(),
            "an_ordering_field",
        )]))
    }

    #[test]
    fn test_max_ordering_info_is_greater_than_event_time_not_zero() {
        let schema = create_test_delete_schema();
        let (_, event_time_converter, commit_time_converter) = create_test_converters(schema);

        // Create test data
        let event_times = event_time_converter
            .convert_columns(&[Arc::new(UInt32Array::from(vec![100, 200, 300]))])
            .unwrap();
        let commit_times = commit_time_converter
            .convert_columns(&[Arc::new(StringArray::from(vec![
                "20240101000000",
                "20240102000000",
                "20240102000000",
            ]))])
            .unwrap();

        let max_info = MaxOrderingInfo {
            event_time_ordering: event_times.row(1).owned(), // 200
            commit_time_ordering: commit_times.row(1).owned(), // "20240102000000"
            is_event_time_zero: false,
        };

        // Test: the info's event time is larger
        assert!(max_info.is_greater_than(event_times.row(0), commit_times.row(0))); // 200 > 100

        // Test: the info's event time is smaller with the same commit time
        assert!(!max_info.is_greater_than(event_times.row(2), commit_times.row(1))); // 200 < 300

        // Test: the info's event time is the same, but commit time is larger
        assert!(max_info.is_greater_than(event_times.row(1), commit_times.row(0)));
        // same event,
    }

    #[test]
    fn test_max_ordering_info_is_greater_than_event_time_zero() {
        let schema = create_test_delete_schema();
        let (_, event_time_converter, commit_time_converter) = create_test_converters(schema);

        // Create test data
        let event_times = event_time_converter
            .convert_columns(&[Arc::new(UInt32Array::from(vec![0, 0, 100]))])
            .unwrap();
        let commit_times = commit_time_converter
            .convert_columns(&[Arc::new(StringArray::from(vec![
                "20240101000000",
                "20240102000000",
                "20240103000000",
            ]))])
            .unwrap();

        let max_info = MaxOrderingInfo {
            event_time_ordering: event_times.row(0).owned(), // 0
            commit_time_ordering: commit_times.row(1).owned(), // "20240102000000"
            is_event_time_zero: true,
        };

        // When event time is zero, only commit time matters
        assert!(
            !max_info.is_greater_than(event_times.row(1), commit_times.row(2)),
            "Event time is zero, max_info is not greater than later commit"
        );
        assert!(
            max_info.is_greater_than(event_times.row(1), commit_times.row(0)),
            "Event time is zero, max_info is greater than earlier commit"
        );
    }

    #[test]
    fn test_process_batch_for_max_orderings_basic() {
        let batch = create_test_delete_batch();
        let schema = batch.schema();
        let (key_converter, event_time_converter, commit_time_converter) =
            create_test_converters(schema);
        let hudi_configs = create_test_hudi_configs();

        let mut max_ordering = HashMap::new();

        let result = process_batch_for_max_orderings(
            &batch,
            &mut max_ordering,
            &key_converter,
            &event_time_converter,
            &commit_time_converter,
            hudi_configs,
        );

        assert!(result.is_ok());
        assert_eq!(max_ordering.len(), 3); // 3 unique keys

        // Verify that all keys are present
        let keys = key_converter
            .convert_columns(&[batch.column(1).clone()])
            .unwrap(); // _hoodie_record_key
        for i in 0..3 {
            let key = keys.row(i).owned();
            assert!(max_ordering.contains_key(&key));
        }
    }

    #[test]
    fn test_process_batch_for_max_orderings_empty_batch() {
        let batch = create_empty_test_delete_batch();
        let schema = batch.schema();
        let (key_converter, event_time_converter, commit_time_converter) =
            create_test_converters(schema);
        let hudi_configs = create_test_hudi_configs();

        let mut max_ordering = HashMap::new();

        let result = process_batch_for_max_orderings(
            &batch,
            &mut max_ordering,
            &key_converter,
            &event_time_converter,
            &commit_time_converter,
            hudi_configs,
        );

        assert!(result.is_ok());
        assert_eq!(max_ordering.len(), 0);
    }

    fn validate_max_ordering_info_with_batch(
        max_ordering: &HashMap<OwnedRow, MaxOrderingInfo>,
        batch: &RecordBatch,
        key_converter: &RowConverter,
        event_time_converter: &RowConverter,
        commit_time_converter: &RowConverter,
    ) {
        let keys = key_converter
            .convert_columns(&[batch.column(1).clone()])
            .unwrap(); // _hoodie_record_key
        let event_times = event_time_converter
            .convert_columns(&[batch.column(3).clone()])
            .unwrap();
        let commit_times = commit_time_converter
            .convert_columns(&[batch.column(0).clone()])
            .unwrap();

        for i in 0..batch.num_rows() {
            let key = keys.row(i).owned();
            let info = max_ordering
                .get(&key)
                .expect("Key should exist in max_ordering");

            assert_eq!(info.event_time_ordering.row(), event_times.row(i));
            assert_eq!(info.commit_time_ordering.row(), commit_times.row(i));
        }
    }

    #[test]
    fn test_process_batch_for_max_orderings_updates_existing() {
        let batch = create_test_delete_batch();
        let schema = batch.schema();
        let (key_converter, event_time_converter, commit_time_converter) =
            create_test_converters(schema.clone());
        let hudi_configs = create_test_hudi_configs();

        let mut max_ordering = HashMap::new();

        // Process first time
        process_batch_for_max_orderings(
            &batch,
            &mut max_ordering,
            &key_converter,
            &event_time_converter,
            &commit_time_converter,
            hudi_configs.clone(),
        )
        .unwrap();

        let initial_count = max_ordering.len();
        assert_eq!(initial_count, 3);

        validate_max_ordering_info_with_batch(
            &max_ordering,
            &batch,
            &key_converter,
            &event_time_converter,
            &commit_time_converter,
        );

        // Create second batch with same keys but different values
        let commit_times = Arc::new(StringArray::from(vec!["20240201000000", "20240202000000"])); // Later commits
        let record_keys = Arc::new(StringArray::from(vec!["key1", "key2"])); // Same keys
        let partition_paths = Arc::new(StringArray::from(vec!["part1", "part2"]));
        let ordering_values = Arc::new(UInt32Array::from(vec![500, 600])); // Higher event times

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![commit_times, record_keys, partition_paths, ordering_values],
        )
        .unwrap();

        // Process second batch
        process_batch_for_max_orderings(
            &batch2,
            &mut max_ordering,
            &key_converter,
            &event_time_converter,
            &commit_time_converter,
            hudi_configs,
        )
        .unwrap();

        // Should still have same number of unique keys, but values should be updated
        assert_eq!(max_ordering.len(), initial_count);

        validate_max_ordering_info_with_batch(
            &max_ordering,
            &batch2,
            &key_converter,
            &event_time_converter,
            &commit_time_converter,
        );
    }

    #[test]
    fn test_process_batch_for_max_orderings_missing_config() {
        let batch = create_test_delete_batch();
        let schema = batch.schema();
        let (key_converter, event_time_converter, commit_time_converter) =
            create_test_converters(schema);

        // Create configs without PrecombineField
        let hudi_configs = Arc::new(HudiConfigs::empty());

        let mut max_ordering = HashMap::new();

        let result = process_batch_for_max_orderings(
            &batch,
            &mut max_ordering,
            &key_converter,
            &event_time_converter,
            &commit_time_converter,
            hudi_configs,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_is_event_time_zero_with_integer_types() {
        let int_types = vec![
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ];
        for t in int_types {
            let schema = create_test_delete_schema_with_event_time_type(t.clone());
            let (_, event_time_converter, _) = create_test_converters(schema);

            // Test zero value
            let zero_batch = create_test_delete_batch_with_event_time_type(t.clone(), vec![0, 0]);
            let event_times = event_time_converter
                .convert_columns(&[zero_batch.column(3).clone()])
                .unwrap();

            let result = is_event_time_zero(event_times.row(0), &event_time_converter);
            assert!(result.is_ok());
            assert!(result.unwrap());

            // Test non-zero value
            let non_zero_batch =
                create_test_delete_batch_with_event_time_type(t.clone(), vec![1, 123]);
            let event_times = event_time_converter
                .convert_columns(&[non_zero_batch.column(3).clone()])
                .unwrap();

            let result = is_event_time_zero(event_times.row(0), &event_time_converter);
            assert!(result.is_ok());
            assert!(!result.unwrap());
        }
    }

    #[test]
    fn test_is_event_time_zero_unsupported_type() {
        let schema = create_test_delete_schema_with_event_time_type(DataType::Utf8);
        let (_, event_time_converter, _) = create_test_converters(schema.clone());

        // Create a batch with string ordering field (unsupported for zero check)
        let commit_times = Arc::new(StringArray::from(vec!["20240101000000"]));
        let record_keys = Arc::new(StringArray::from(vec!["key1"]));
        let partition_paths = Arc::new(StringArray::from(vec!["part1"]));
        let ordering_values = Arc::new(StringArray::from(vec!["0"])); // String "0"

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![commit_times, record_keys, partition_paths, ordering_values],
        )
        .unwrap();

        let event_times = event_time_converter
            .convert_columns(&[batch.column(3).clone()])
            .unwrap();

        let result = is_event_time_zero(event_times.row(0), &event_time_converter);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Should return false for unsupported types
    }

    #[test]
    fn test_max_ordering_info_clone() {
        let schema = create_test_delete_schema();
        let (_, event_time_converter, commit_time_converter) = create_test_converters(schema);

        let event_times = event_time_converter
            .convert_columns(&[Arc::new(UInt32Array::from(vec![100]))])
            .unwrap();
        let commit_times = commit_time_converter
            .convert_columns(&[Arc::new(StringArray::from(vec!["20240101000000"]))])
            .unwrap();

        let original = MaxOrderingInfo {
            event_time_ordering: event_times.row(0).owned(),
            commit_time_ordering: commit_times.row(0).owned(),
            is_event_time_zero: false,
        };

        let cloned = original.clone();

        assert_eq!(original.is_event_time_zero, cloned.is_event_time_zero);
        assert_eq!(
            original.event_time_ordering.row(),
            cloned.event_time_ordering.row()
        );
        assert_eq!(
            original.commit_time_ordering.row(),
            cloned.commit_time_ordering.row()
        );
    }

    #[test]
    fn test_process_batch_with_duplicate_keys() {
        let schema = create_test_delete_schema();

        // Create batch with duplicate keys
        let commit_times = Arc::new(StringArray::from(vec![
            "20240101000000",
            "20240102000000",
            "20240103000000",
        ]));
        let record_keys = Arc::new(StringArray::from(vec!["key1", "key1", "key2"])); // key1 appears twice
        let partition_paths = Arc::new(StringArray::from(vec!["part1", "part1", "part2"]));
        let ordering_values = Arc::new(UInt32Array::from(vec![0, 200, 300])); // Different event times

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![commit_times, record_keys, partition_paths, ordering_values],
        )
        .unwrap();

        let (key_converter, event_time_converter, commit_time_converter) =
            create_test_converters(schema);
        let hudi_configs = create_test_hudi_configs();

        let mut max_ordering = HashMap::new();

        let result = process_batch_for_max_orderings(
            &batch,
            &mut max_ordering,
            &key_converter,
            &event_time_converter,
            &commit_time_converter,
            hudi_configs,
        );

        assert!(result.is_ok());
        assert_eq!(max_ordering.len(), 2); // Only 2 unique keys

        // Verify that key1 has the maximum values from both records
        let keys = key_converter
            .convert_columns(&[batch.column(1).clone()])
            .unwrap(); // _hoodie_record_key
        let key1 = keys.row(0).owned(); // Should be "key1"
        let event_times = event_time_converter
            .convert_columns(&[batch.column(3).clone()])
            .unwrap();
        let commit_times = commit_time_converter
            .convert_columns(&[batch.column(0).clone()])
            .unwrap();

        let info = max_ordering.get(&key1).unwrap();
        assert_eq!(
            info.event_time_ordering.row(),
            event_times.row(1),
            "Event time for key1 should be the max of 0 and 200"
        );
        assert_eq!(
            info.commit_time_ordering.row(),
            commit_times.row(1),
            "Commit time for key1 should be the max of 20240101000000 and 20240102000000"
        );
        assert!(!info.is_event_time_zero);
    }
}
