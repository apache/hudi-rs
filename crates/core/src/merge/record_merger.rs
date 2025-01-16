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
use crate::config::error::ConfigError;
use crate::config::error::Result as ConfigResult;
use crate::config::table::HudiTableConfig::{
    PopulatesMetaFields, PrecombineField, RecordMergeStrategy,
};
use crate::config::HudiConfigs;
use crate::merge::RecordMergeStrategyValue;
use crate::Result;
use arrow::compute::{sort_to_indices, take_record_batch};
use arrow::error::ArrowError;
use arrow_array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{SchemaRef, SortOptions};
use arrow_select::concat::concat_batches;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RecordMerger {
    pub hudi_configs: Arc<HudiConfigs>,
}

impl RecordMerger {
    /// Validates the given [HudiConfigs] against the [RecordMergeStrategy].
    ///
    /// # Notes
    /// This should be ideally called during table creation. However, an empty
    /// table could have no precombine field being set, and we also want to keep
    /// the default merge strategy as [OverwriteWithLatest] to fulfill the
    /// snapshot read semantics out-of-the-box. This would conflict with
    /// having no precombine field.
    ///
    /// TODO: We should derive merge strategy dynamically if not set by user.
    pub fn validate_configs(hudi_configs: &HudiConfigs) -> ConfigResult<()> {
        let merge_strategy = hudi_configs
            .get_or_default(RecordMergeStrategy)
            .to::<String>();
        let merge_strategy = RecordMergeStrategyValue::from_str(&merge_strategy)?;

        let populate_meta_fields = hudi_configs
            .get_or_default(PopulatesMetaFields)
            .to::<bool>();
        if !populate_meta_fields && merge_strategy != RecordMergeStrategyValue::AppendOnly {
            return Err(ConfigError::InvalidValue(format!(
                "When {:?} is false, {:?} must be {:?}.",
                PopulatesMetaFields,
                RecordMergeStrategy,
                RecordMergeStrategyValue::AppendOnly
            )));
        }

        let precombine_field = hudi_configs.try_get(PrecombineField);
        if precombine_field.is_none()
            && merge_strategy == RecordMergeStrategyValue::OverwriteWithLatest
        {
            return Err(ConfigError::InvalidValue(format!(
                "When {:?} is {:?}, {:?} must be set.",
                RecordMergeStrategy,
                RecordMergeStrategyValue::OverwriteWithLatest,
                PrecombineField
            )));
        }

        Ok(())
    }

    pub fn new(hudi_configs: Arc<HudiConfigs>) -> Self {
        Self { hudi_configs }
    }

    pub fn merge_record_batches(
        &self,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<RecordBatch> {
        Self::validate_configs(&self.hudi_configs)?;

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }

        let merge_strategy = self
            .hudi_configs
            .get_or_default(RecordMergeStrategy)
            .to::<String>();
        let merge_strategy = RecordMergeStrategyValue::from_str(&merge_strategy)?;
        match merge_strategy {
            RecordMergeStrategyValue::AppendOnly => {
                let concat_batch = concat_batches(schema, batches)?;
                Ok(concat_batch)
            }
            RecordMergeStrategyValue::OverwriteWithLatest => {
                let concat_batch = concat_batches(schema, batches)?;
                if concat_batch.num_rows() == 0 {
                    return Ok(concat_batch);
                }

                let precombine_field = self.hudi_configs.get(PrecombineField)?.to::<String>();
                let precombine_array =
                    concat_batch
                        .column_by_name(&precombine_field)
                        .ok_or_else(|| {
                            ArrowError::SchemaError(format!("Column {precombine_field} not found."))
                        })?;

                // Sort the precombine values in descending order, and put nulls last.
                let sort_options = SortOptions::new(true, false);
                let sorted_indices = sort_to_indices(precombine_array, Some(sort_options), None)?;

                let record_key_field = "_hoodie_record_key";
                let record_key_array = concat_batch
                    .column_by_name(record_key_field)
                    .ok_or_else(|| {
                        ArrowError::SchemaError(format!("Column {record_key_field} not found."))
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        ArrowError::CastError(format!(
                            "Column {record_key_field} cannot be cast to StringArray."
                        ))
                    })?;

                let mut keys_and_latest_indices: HashMap<&str, u32> =
                    HashMap::with_capacity(record_key_array.len());
                for i in sorted_indices.values() {
                    let record_key = record_key_array.value(*i as usize);
                    if keys_and_latest_indices.contains_key(record_key) {
                        // We sorted the precombine field in descending order, so if the record key
                        // is already in the map, the associated index will be already pointing to
                        // the latest version of that record.
                        continue;
                    } else {
                        keys_and_latest_indices.insert(record_key, *i);
                    }
                }
                let latest_indices: UInt32Array =
                    keys_and_latest_indices.values().copied().collect();
                Ok(take_record_batch(&concat_batch, &latest_indices)?)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};

    fn create_configs(strategy: &str, meta_fields: bool, precombine: Option<&str>) -> HudiConfigs {
        if let Some(precombine) = precombine {
            HudiConfigs::new([
                (RecordMergeStrategy, strategy.to_string()),
                (PopulatesMetaFields, meta_fields.to_string()),
                (PrecombineField, precombine.to_string()),
            ])
        } else {
            HudiConfigs::new([
                (RecordMergeStrategy, strategy.to_string()),
                (PopulatesMetaFields, meta_fields.to_string()),
            ])
        }
    }

    #[test]
    fn test_validate_configs() {
        // Valid config with precombine field and meta fields
        let configs = create_configs("OVERWRITE_WITH_LATEST", true, Some("ts"));
        assert!(RecordMerger::validate_configs(&configs).is_ok());

        // Valid append only config without meta fields
        let configs = create_configs("APPEND_ONLY", false, None);
        assert!(RecordMerger::validate_configs(&configs).is_ok());

        // Invalid: Overwrite without precombine field
        let configs = create_configs("OVERWRITE_WITH_LATEST", true, None);
        assert!(RecordMerger::validate_configs(&configs).is_err());

        // Invalid: No meta fields with overwrite strategy
        let configs = create_configs("OVERWRITE_WITH_LATEST", false, Some("ts"));
        assert!(RecordMerger::validate_configs(&configs).is_err());
    }

    fn create_schema(fields: Vec<(&str, DataType, bool)>) -> SchemaRef {
        let fields: Vec<Field> = fields
            .into_iter()
            .map(|(name, dtype, nullable)| Field::new(name, dtype, nullable))
            .collect();
        let schema = Schema::new(fields);
        SchemaRef::from(schema)
    }

    fn get_sorted_rows(batch: &RecordBatch) -> Vec<(String, i32, i32)> {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("First column must be strings");
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Second column must be i32");
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Third column must be i32");

        let mut result: Vec<(String, i32, i32)> = keys
            .iter()
            .zip(timestamps.iter())
            .zip(values.iter())
            .map(|((k, t), v)| (k.unwrap().to_string(), t.unwrap(), v.unwrap()))
            .collect();
        result.sort_unstable_by_key(|(k, ts, _)| (k.clone(), *ts));
        result
    }

    #[test]
    fn test_merge_records_empty() {
        let schema = create_schema(vec![
            ("_hoodie_record_key", DataType::Utf8, false),
            ("ts", DataType::Int32, false),
            ("value", DataType::Int32, false),
        ]);

        let configs = create_configs("OVERWRITE_WITH_LATEST", true, Some("ts"));
        let merger = RecordMerger::new(Arc::new(configs));

        // Test empty input
        let empty_result = merger.merge_record_batches(&schema, &[]).unwrap();
        assert_eq!(empty_result.num_rows(), 0);

        // Test single empty batch
        let empty_batch = RecordBatch::new_empty(schema.clone());
        let single_empty_result = merger
            .merge_record_batches(&schema, &[empty_batch])
            .unwrap();
        assert_eq!(single_empty_result.num_rows(), 0);
    }

    #[test]
    fn test_merge_records_append_only() {
        let schema = create_schema(vec![
            ("_hoodie_record_key", DataType::Utf8, false),
            ("ts", DataType::Int32, false),
            ("value", DataType::Int32, false),
        ]);

        // First batch
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k1", "k2"])),
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )
        .unwrap();

        // Second batch
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k1", "k3"])),
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(Int32Array::from(vec![30, 40])),
            ],
        )
        .unwrap();

        let configs = create_configs("APPEND_ONLY", false, None);
        let merger = RecordMerger::new(Arc::new(configs));
        let merged = merger
            .merge_record_batches(&schema, &[batch1, batch2])
            .unwrap();

        // Should contain all records in order without deduplication
        assert_eq!(merged.num_rows(), 4);

        let result = get_sorted_rows(&merged);
        assert_eq!(
            result,
            vec![
                ("k1".to_string(), 1, 10),
                ("k1".to_string(), 3, 30),
                ("k2".to_string(), 2, 20),
                ("k3".to_string(), 4, 40),
            ]
        );
    }

    #[test]
    fn test_merge_records_nulls() {
        let schema = create_schema(vec![
            ("_hoodie_record_key", DataType::Utf8, false),
            ("ts", DataType::Int32, true), // Nullable timestamp
            ("value", DataType::Int32, false),
        ]);

        // First batch with some null timestamps
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k1", "k2", "k3"])),
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        // Second batch with updates and nulls
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k1", "k2"])),
                Arc::new(Int32Array::from(vec![None, Some(5)])),
                Arc::new(Int32Array::from(vec![40, 50])),
            ],
        )
        .unwrap();

        let configs = create_configs("OVERWRITE_WITH_LATEST", true, Some("ts"));
        let merger = RecordMerger::new(Arc::new(configs));
        let merged = merger
            .merge_record_batches(&schema, &[batch1, batch2])
            .unwrap();

        assert_eq!(merged.num_rows(), 3);

        let result = get_sorted_rows(&merged);
        assert_eq!(
            result,
            vec![
                ("k1".to_string(), 1, 10), // Keep original since both updates have null ts
                ("k2".to_string(), 5, 50), // Take second value due to higher ts
                ("k3".to_string(), 3, 30), // Unchanged
            ]
        );
    }

    #[test]
    fn test_merge_records_overwrite_with_latest() {
        let schema = create_schema(vec![
            ("_hoodie_record_key", DataType::Utf8, false),
            ("ts", DataType::Int32, false),
            ("value", DataType::Int32, false),
        ]);

        // First batch
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k1", "k2", "k3"])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        // Second batch with updates
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k1", "k2"])),
                Arc::new(Int32Array::from(vec![4, 1])),
                Arc::new(Int32Array::from(vec![40, 50])),
            ],
        )
        .unwrap();

        let configs = create_configs("OVERWRITE_WITH_LATEST", true, Some("ts"));
        let merger = RecordMerger::new(Arc::new(configs));
        let merged = merger
            .merge_record_batches(&schema, &[batch1, batch2])
            .unwrap();

        assert_eq!(merged.num_rows(), 3);

        let result = get_sorted_rows(&merged);
        assert_eq!(
            result,
            vec![
                ("k1".to_string(), 4, 40), // Latest value due to ts=4
                ("k2".to_string(), 2, 20), // Original value since ts=1 < ts=2
                ("k3".to_string(), 3, 30), // Unchanged
            ]
        );
    }
}
