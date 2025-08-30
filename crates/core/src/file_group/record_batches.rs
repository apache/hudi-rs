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
use crate::config::table::HudiTableConfig;
use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::schema::delete::transform_delete_record_batch;
use crate::Result;
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RecordBatches {
    pub(crate) data_batches: Vec<RecordBatch>,
    pub(crate) delete_batches: Vec<(RecordBatch, String)>,
    num_data_rows: usize,
    num_delete_rows: usize,
}

impl Default for RecordBatches {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordBatches {
    pub fn new() -> Self {
        Self {
            data_batches: Vec::new(),
            delete_batches: Vec::new(),
            num_data_rows: 0,
            num_delete_rows: 0,
        }
    }

    pub fn new_with_capacity(num_data_batches: usize, num_delete_batches: usize) -> Self {
        Self {
            data_batches: Vec::with_capacity(num_data_batches),
            delete_batches: Vec::with_capacity(num_delete_batches),
            num_data_rows: 0,
            num_delete_rows: 0,
        }
    }

    pub fn new_with_data_batches<I>(data_batches: I) -> Self
    where
        I: IntoIterator<Item = RecordBatch>,
    {
        let mut record_batches = Self::new();
        for batch in data_batches {
            record_batches.push_data_batch(batch);
        }
        record_batches
    }

    pub fn num_data_batches(&self) -> usize {
        self.data_batches.len()
    }

    pub fn num_delete_batches(&self) -> usize {
        self.delete_batches.len()
    }

    pub fn num_data_rows(&self) -> usize {
        self.num_data_rows
    }

    pub fn num_delete_rows(&self) -> usize {
        self.num_delete_rows
    }

    pub fn push_data_batch(&mut self, batch: RecordBatch) {
        self.num_data_rows += batch.num_rows();
        self.data_batches.push(batch);
    }

    pub fn push_delete_batch(&mut self, batch: RecordBatch, instant_time: String) {
        self.num_delete_rows += batch.num_rows();
        self.delete_batches.push((batch, instant_time));
    }

    pub fn extend(&mut self, other: RecordBatches) {
        self.num_data_rows += other.num_data_rows;
        self.data_batches.extend(other.data_batches);
        self.num_delete_rows += other.num_delete_rows;
        self.delete_batches.extend(other.delete_batches);
    }

    pub fn concat_data_batches(&self, schema: SchemaRef) -> Result<RecordBatch> {
        if self.num_data_rows == 0 {
            return Ok(RecordBatch::new_empty(schema));
        }

        concat_batches(&schema, &self.data_batches).map_err(CoreError::ArrowError)
    }

    pub fn concat_delete_batches_transformed(
        &self,
        hudi_configs: Arc<HudiConfigs>,
    ) -> Result<RecordBatch> {
        let ordering_field: String = hudi_configs
            .get(HudiTableConfig::PrecombineField)?
            .into();

        if self.num_delete_rows == 0 {
            return Ok(RecordBatch::new_empty(SchemaRef::from(Schema::empty())));
        }

        let mut delete_batches = Vec::with_capacity(self.delete_batches.len());
        for (batch, instant_time) in &self.delete_batches {
            let batch = transform_delete_record_batch(batch, instant_time, &ordering_field)?;
            delete_batches.push(batch);
        }

        concat_batches(&delete_batches[0].schema(), &delete_batches).map_err(CoreError::ArrowError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::error::ConfigError;
    use crate::metadata::meta_field::MetaField;
    use arrow_array::{Float64Array, StringArray};
    use arrow_schema::{DataType, Field};

    // Helper function to create a test data RecordBatch
    fn create_test_data_batch(num_rows: usize) -> RecordBatch {
        create_test_data_batch_with_ordering_field(num_rows, "ord_val")
    }

    fn create_test_data_batch_with_ordering_field(
        num_rows: usize,
        ordering_field: &str,
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(ordering_field, DataType::Float64, false),
        ]));

        let ids: Vec<String> = (0..num_rows).map(|i| i.to_string()).collect();
        let names: Vec<Option<String>> = (0..num_rows)
            .map(|i| {
                if i % 2 == 0 {
                    Some(format!("name_{}", i))
                } else {
                    None
                }
            })
            .collect();
        let values: Vec<f64> = (0..num_rows).map(|i| i as f64 * 1.5).collect();

        let id_array = Arc::new(StringArray::from(ids));
        let name_array = Arc::new(StringArray::from(names));
        let value_array = Arc::new(Float64Array::from(values));

        RecordBatch::try_new(schema, vec![id_array, name_array, value_array]).unwrap()
    }

    // Helper function to create a test delete RecordBatch
    fn create_test_delete_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("recordKey", DataType::Utf8, false),
            Field::new("partitionPath", DataType::Utf8, false),
            Field::new("orderingVal", DataType::Float64, false),
        ]));

        let record_keys: Vec<String> = (0..num_rows).map(|i| i.to_string()).collect();
        let partition_paths: Vec<String> =
            (0..num_rows).map(|i| format!("partition_{}", i)).collect();
        let ordering_vals: Vec<f64> = (0..num_rows).map(|i| i as f64 * 1.6).collect();

        let record_key_array = Arc::new(StringArray::from(record_keys));
        let partition_path_array = Arc::new(StringArray::from(partition_paths));
        let ordering_val_array = Arc::new(Float64Array::from(ordering_vals));

        RecordBatch::try_new(
            schema,
            vec![record_key_array, partition_path_array, ordering_val_array],
        )
        .unwrap()
    }

    // Helper function to create empty data batch
    fn create_empty_data_batch() -> RecordBatch {
        create_test_data_batch(0)
    }

    // Helper function to create empty delete batch
    fn create_empty_delete_batch() -> RecordBatch {
        create_test_delete_batch(0)
    }

    #[test]
    fn test_new_and_default() {
        let record_batches = [RecordBatches::new(), RecordBatches::default()];
        for rb in record_batches {
            assert_eq!(rb.num_data_batches(), 0);
            assert_eq!(rb.num_delete_batches(), 0);
            assert_eq!(rb.num_data_rows(), 0);
            assert_eq!(rb.num_delete_rows(), 0);
            assert!(rb.data_batches.is_empty());
            assert!(rb.delete_batches.is_empty());
        }
    }

    #[test]
    fn test_new_with_capacity() {
        let record_batches = RecordBatches::new_with_capacity(5, 3);

        assert_eq!(record_batches.num_data_batches(), 0);
        assert_eq!(record_batches.num_delete_batches(), 0);
        assert_eq!(record_batches.num_data_rows(), 0);
        assert_eq!(record_batches.num_delete_rows(), 0);

        // Check capacity
        assert!(record_batches.data_batches.capacity() >= 5);
        assert!(record_batches.delete_batches.capacity() >= 3);
    }

    #[test]
    fn test_new_with_data_batches_empty() {
        let data_batches: Vec<RecordBatch> = vec![];
        let record_batches = RecordBatches::new_with_data_batches(data_batches);

        assert_eq!(record_batches.num_data_batches(), 0);
        assert_eq!(record_batches.num_data_rows(), 0);
    }

    #[test]
    fn test_new_with_data_batches_single() {
        let batch = create_test_data_batch(10);
        let data_batches = vec![batch];
        let record_batches = RecordBatches::new_with_data_batches(data_batches);

        assert_eq!(record_batches.num_data_batches(), 1);
        assert_eq!(record_batches.num_data_rows(), 10);
        assert_eq!(record_batches.num_delete_batches(), 0);
        assert_eq!(record_batches.num_delete_rows(), 0);
    }

    #[test]
    fn test_new_with_data_batches_multiple() {
        let batches = vec![
            create_test_data_batch(5),
            create_test_data_batch(3),
            create_test_data_batch(7),
        ];
        let record_batches = RecordBatches::new_with_data_batches(batches);

        assert_eq!(record_batches.num_data_batches(), 3);
        assert_eq!(record_batches.num_data_rows(), 15); // 5 + 3 + 7
    }

    #[test]
    fn test_push_data_batch() {
        let mut record_batches = RecordBatches::new();

        // Add first batch
        let batch1 = create_test_data_batch(5);
        record_batches.push_data_batch(batch1);

        assert_eq!(record_batches.num_data_batches(), 1);
        assert_eq!(record_batches.num_data_rows(), 5);

        // Add second batch
        let batch2 = create_test_data_batch(8);
        record_batches.push_data_batch(batch2);

        assert_eq!(record_batches.num_data_batches(), 2);
        assert_eq!(record_batches.num_data_rows(), 13); // 5 + 8
    }

    #[test]
    fn test_push_data_batch_empty() {
        let mut record_batches = RecordBatches::new();
        let empty_batch = create_empty_data_batch();

        record_batches.push_data_batch(empty_batch);

        assert_eq!(record_batches.num_data_batches(), 1);
        assert_eq!(record_batches.num_data_rows(), 0);
    }

    #[test]
    fn test_push_delete_batch() {
        let mut record_batches = RecordBatches::new();

        // Add first delete batch
        let delete_batch1 = create_test_delete_batch(3);
        record_batches.push_delete_batch(delete_batch1, "20240101000000".to_string());

        assert_eq!(record_batches.num_delete_batches(), 1);
        assert_eq!(record_batches.num_delete_rows(), 3);

        // Add second delete batch
        let delete_batch2 = create_test_delete_batch(4);
        record_batches.push_delete_batch(delete_batch2, "20240102000000".to_string());

        assert_eq!(record_batches.num_delete_batches(), 2);
        assert_eq!(record_batches.num_delete_rows(), 7); // 3 + 4

        // Check instant times are stored
        assert_eq!(record_batches.delete_batches[0].1, "20240101000000");
        assert_eq!(record_batches.delete_batches[1].1, "20240102000000");
    }

    #[test]
    fn test_push_delete_batch_empty() {
        let mut record_batches = RecordBatches::new();
        let empty_delete_batch = create_empty_delete_batch();

        record_batches.push_delete_batch(empty_delete_batch, "20240101000000".to_string());

        assert_eq!(record_batches.num_delete_batches(), 1);
        assert_eq!(record_batches.num_delete_rows(), 0);
    }

    #[test]
    fn test_extend_empty() {
        let mut record_batches1 = RecordBatches::new();
        let record_batches2 = RecordBatches::new();

        record_batches1.extend(record_batches2);

        assert_eq!(record_batches1.num_data_batches(), 0);
        assert_eq!(record_batches1.num_delete_batches(), 0);
        assert_eq!(record_batches1.num_data_rows(), 0);
        assert_eq!(record_batches1.num_delete_rows(), 0);
    }

    #[test]
    fn test_extend_with_data() {
        let mut record_batches1 = RecordBatches::new();
        record_batches1.push_data_batch(create_test_data_batch(5));
        record_batches1
            .push_delete_batch(create_test_delete_batch(2), "20240101000000".to_string());

        let mut record_batches2 = RecordBatches::new();
        record_batches2.push_data_batch(create_test_data_batch(8));
        record_batches2
            .push_delete_batch(create_test_delete_batch(3), "20240102000000".to_string());

        record_batches1.extend(record_batches2);

        assert_eq!(record_batches1.num_data_batches(), 2);
        assert_eq!(record_batches1.num_delete_batches(), 2);
        assert_eq!(record_batches1.num_data_rows(), 13); // 5 + 8
        assert_eq!(record_batches1.num_delete_rows(), 5); // 2 + 3

        // Check instant times are preserved
        assert_eq!(record_batches1.delete_batches[0].1, "20240101000000");
        assert_eq!(record_batches1.delete_batches[1].1, "20240102000000");
    }

    #[test]
    fn test_concat_data_batches_empty() {
        let record_batches = RecordBatches::new();
        let schema = create_test_data_batch(0).schema();

        let result = record_batches.concat_data_batches(schema.clone()).unwrap();

        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.schema(), schema);
    }

    #[test]
    fn test_concat_data_batches_single() {
        let mut record_batches = RecordBatches::new();
        let batch = create_test_data_batch(5);
        let schema = batch.schema();
        record_batches.push_data_batch(batch);

        let result = record_batches.concat_data_batches(schema.clone()).unwrap();

        assert_eq!(result.num_rows(), 5);
        assert_eq!(result.schema(), schema);
        assert_eq!(result.num_columns(), 3);
    }

    #[test]
    fn test_concat_data_batches_multiple() {
        let mut record_batches = RecordBatches::new();
        record_batches.push_data_batch(create_test_data_batch(3));
        record_batches.push_data_batch(create_test_data_batch(5));
        record_batches.push_data_batch(create_test_data_batch(2));

        let schema = create_test_data_batch(0).schema();
        let result = record_batches.concat_data_batches(schema.clone()).unwrap();

        assert_eq!(result.num_rows(), 10); // 3 + 5 + 2
        assert_eq!(result.schema(), schema);
        assert_eq!(result.num_columns(), 3);
    }

    #[test]
    fn test_mixed_operations() {
        let mut record_batches = RecordBatches::new();

        // Add some data batches
        record_batches.push_data_batch(create_test_data_batch(10));
        record_batches.push_data_batch(create_test_data_batch(5));

        // Add some delete batches
        record_batches.push_delete_batch(create_test_delete_batch(3), "20240101000000".to_string());
        record_batches.push_delete_batch(create_test_delete_batch(7), "20240102000000".to_string());

        // Verify counts
        assert_eq!(record_batches.num_data_batches(), 2);
        assert_eq!(record_batches.num_delete_batches(), 2);
        assert_eq!(record_batches.num_data_rows(), 15); // 10 + 5
        assert_eq!(record_batches.num_delete_rows(), 10); // 3 + 7

        // Test extending with another RecordBatches
        let mut other = RecordBatches::new();
        other.push_data_batch(create_test_data_batch(8));
        other.push_delete_batch(create_test_delete_batch(2), "20240103000000".to_string());

        record_batches.extend(other);

        assert_eq!(record_batches.num_data_batches(), 3);
        assert_eq!(record_batches.num_delete_batches(), 3);
        assert_eq!(record_batches.num_data_rows(), 23); // 15 + 8
        assert_eq!(record_batches.num_delete_rows(), 12); // 10 + 2
    }

    #[test]
    fn test_getters_consistency() {
        let mut record_batches = RecordBatches::new();

        // Test that getters are consistent with internal state
        assert_eq!(
            record_batches.num_data_batches(),
            record_batches.data_batches.len()
        );
        assert_eq!(
            record_batches.num_delete_batches(),
            record_batches.delete_batches.len()
        );

        // Add some batches and test again
        record_batches.push_data_batch(create_test_data_batch(3));
        record_batches.push_data_batch(create_test_data_batch(4));
        record_batches.push_delete_batch(create_test_delete_batch(2), "20240101000000".to_string());

        assert_eq!(
            record_batches.num_data_batches(),
            record_batches.data_batches.len()
        );
        assert_eq!(
            record_batches.num_delete_batches(),
            record_batches.delete_batches.len()
        );

        // Verify row counts match actual batch row counts
        let expected_data_rows: usize = record_batches
            .data_batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum();
        let expected_delete_rows: usize = record_batches
            .delete_batches
            .iter()
            .map(|(batch, _)| batch.num_rows())
            .sum();

        assert_eq!(record_batches.num_data_rows(), expected_data_rows);
        assert_eq!(record_batches.num_delete_rows(), expected_delete_rows);
    }

    #[test]
    fn test_with_zero_row_batches() {
        let mut record_batches = RecordBatches::new();

        // Add mix of empty and non-empty batches
        record_batches.push_data_batch(create_test_data_batch(0));
        record_batches.push_data_batch(create_test_data_batch(5));
        record_batches.push_data_batch(create_test_data_batch(0));

        record_batches.push_delete_batch(create_test_delete_batch(0), "20240101000000".to_string());
        record_batches.push_delete_batch(create_test_delete_batch(3), "20240102000000".to_string());

        assert_eq!(record_batches.num_data_batches(), 3);
        assert_eq!(record_batches.num_delete_batches(), 2);
        assert_eq!(record_batches.num_data_rows(), 5); // 0 + 5 + 0
        assert_eq!(record_batches.num_delete_rows(), 3); // 0 + 3
    }

    #[test]
    fn test_concat_delete_batches_transformed_empty() {
        let record_batches = RecordBatches::new();
        let hudi_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::PrecombineField.as_ref(),
            "any_ordering_field",
        )]));

        let result = record_batches
            .concat_delete_batches_transformed(hudi_configs)
            .unwrap();

        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 0); // Empty schema
    }

    #[test]
    fn test_concat_delete_batches_transformed_single() {
        let mut record_batches = RecordBatches::new();

        // Need at least one data batch for schema reference
        let ordering_field = "seq_num";
        record_batches.push_data_batch(create_test_data_batch_with_ordering_field(
            1,
            ordering_field,
        ));

        // Add delete batch
        record_batches.push_delete_batch(create_test_delete_batch(3), "20240101000000".to_string());

        let hudi_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::PrecombineField.as_ref(),
            ordering_field,
        )]));

        let result = record_batches
            .concat_delete_batches_transformed(hudi_configs)
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 4); // commit_time, record_key, partition_path, seq_num

        // Check schema field names
        let schema = result.schema();
        assert_eq!(schema.field(0).name(), MetaField::CommitTime.as_ref());
        assert_eq!(schema.field(1).name(), MetaField::RecordKey.as_ref());
        assert_eq!(schema.field(2).name(), MetaField::PartitionPath.as_ref());
        assert_eq!(schema.field(3).name(), ordering_field);
    }

    #[test]
    fn test_concat_delete_batches_transformed_multiple() {
        let mut record_batches = RecordBatches::new();

        let ordering_field = "seq_num";
        // Need at least one data batch for schema reference
        record_batches.push_data_batch(create_test_data_batch_with_ordering_field(
            1,
            ordering_field,
        ));

        // Add multiple delete batches
        record_batches.push_delete_batch(create_test_delete_batch(2), "20240101000000".to_string());
        record_batches.push_delete_batch(create_test_delete_batch(3), "20240102000000".to_string());
        record_batches.push_delete_batch(create_test_delete_batch(1), "20240103000000".to_string());

        let hudi_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::PrecombineField.as_ref(),
            ordering_field,
        )]));

        let result = record_batches
            .concat_delete_batches_transformed(hudi_configs)
            .unwrap();

        assert_eq!(result.num_rows(), 6); // 2 + 3 + 1
        assert_eq!(result.num_columns(), 4);

        // Verify all commit times are preserved correctly
        let commit_time_array = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // First 2 rows should have first commit time
        assert_eq!(commit_time_array.value(0), "20240101000000");
        assert_eq!(commit_time_array.value(1), "20240101000000");

        // Next 3 rows should have second commit time
        assert_eq!(commit_time_array.value(2), "20240102000000");
        assert_eq!(commit_time_array.value(3), "20240102000000");
        assert_eq!(commit_time_array.value(4), "20240102000000");

        // Last row should have third commit time
        assert_eq!(commit_time_array.value(5), "20240103000000");
    }

    #[test]
    fn test_concat_delete_batches_transformed_custom_ordering_field() {
        let mut record_batches = RecordBatches::new();

        // Create data batch with custom ordering field
        let ordering_field = "custom_ordering_field";
        record_batches.push_data_batch(create_test_data_batch_with_ordering_field(
            1,
            ordering_field,
        ));
        record_batches.push_delete_batch(create_test_delete_batch(2), "20240101000000".to_string());

        let hudi_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::PrecombineField.as_ref(),
            ordering_field,
        )]));

        let result = record_batches
            .concat_delete_batches_transformed(hudi_configs)
            .unwrap();

        // Check schema field names
        let schema = result.schema();
        assert_eq!(schema.field(0).name(), MetaField::CommitTime.as_ref());
        assert_eq!(schema.field(1).name(), MetaField::RecordKey.as_ref());
        assert_eq!(schema.field(2).name(), MetaField::PartitionPath.as_ref());
        assert_eq!(schema.field(3).name(), ordering_field);
    }

    #[test]
    fn test_concat_delete_batches_transformed_missing_config() {
        let mut record_batches = RecordBatches::new();
        record_batches.push_data_batch(create_test_data_batch(1));
        record_batches.push_delete_batch(create_test_delete_batch(1), "20240101000000".to_string());

        // Create config without PrecombineField
        let hudi_configs = Arc::new(HudiConfigs::empty());

        // This should return an error
        let result = record_batches.concat_delete_batches_transformed(hudi_configs);
        match result {
            Err(CoreError::Config(ConfigError::NotFound(s))) => {
                assert_eq!(s, HudiTableConfig::PrecombineField.as_ref());
            }
            _ => panic!(
                "Expected ConfigError::NotFound, got {:?}",
                result.unwrap_err()
            ),
        }
    }
}
