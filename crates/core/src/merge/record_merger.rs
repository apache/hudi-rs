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
use crate::file_group::record_batches::RecordBatches;
use crate::merge::ordering::{process_batch_for_max_orderings, MaxOrderingInfo};
use crate::merge::RecordMergeStrategyValue;
use crate::metadata::meta_field::MetaField;
use crate::record::{
    create_commit_time_ordering_converter, create_event_time_ordering_converter,
    create_record_key_converter, extract_commit_time_ordering_values,
    extract_event_time_ordering_values, extract_record_keys,
};
use crate::util::arrow::lexsort_to_indices;
use crate::util::arrow::ColumnAsArray;
use crate::Result;
use arrow_array::{BooleanArray, RecordBatch};
use arrow_row::{OwnedRow, Row};
use arrow_schema::SchemaRef;
use arrow_select::take::take_record_batch;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RecordMerger {
    pub schema: SchemaRef,
    pub hudi_configs: Arc<HudiConfigs>,
}

impl RecordMerger {
    /// Validates the given [HudiConfigs] against the [RecordMergeStrategy].
    pub fn validate_configs(hudi_configs: &HudiConfigs) -> ConfigResult<()> {
        let merge_strategy: String = hudi_configs
            .get_or_default(RecordMergeStrategy)
            .into();
        let merge_strategy = RecordMergeStrategyValue::from_str(&merge_strategy)?;

        let populate_meta_fields: bool = hudi_configs
            .get_or_default(PopulatesMetaFields)
            .into();
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

    pub fn new(schema: SchemaRef, hudi_configs: Arc<HudiConfigs>) -> Self {
        Self {
            schema,
            hudi_configs,
        }
    }

    pub fn merge_record_batches(&self, record_batches: RecordBatches) -> Result<RecordBatch> {
        let merge_strategy: String = self
            .hudi_configs
            .get_or_default(RecordMergeStrategy)
            .into();
        let merge_strategy = RecordMergeStrategyValue::from_str(&merge_strategy)?;
        match merge_strategy {
            RecordMergeStrategyValue::AppendOnly => {
                record_batches.concat_data_batches(self.schema.clone())
            }
            RecordMergeStrategyValue::OverwriteWithLatest => {
                let data_batch = record_batches.concat_data_batches(self.schema.clone())?;
                let num_records = data_batch.num_rows();
                if num_records == 0 {
                    return Ok(data_batch.clone());
                }

                // Use sorting fields to get sorted indices of the data batch (inserts and updates)
                let key_array = data_batch.get_array(MetaField::RecordKey.as_ref())?;
                let ordering_field: String = self.hudi_configs.get(PrecombineField)?.into();
                let ordering_array = data_batch.get_array(&ordering_field)?;
                let commit_seqno_array = data_batch.get_array(MetaField::CommitSeqno.as_ref())?;
                let desc_indices =
                    lexsort_to_indices(&[key_array, ordering_array, commit_seqno_array], true);

                // Create shared converters for record keys and ordering values
                let key_converter = create_record_key_converter(data_batch.schema())?;
                let ordering_field: String = self.hudi_configs.get(PrecombineField)?.into();
                let event_time_converter =
                    create_event_time_ordering_converter(data_batch.schema(), &ordering_field)?;
                let commit_time_converter =
                    create_commit_time_ordering_converter(data_batch.schema())?;

                // Process the delete batches to get the max ordering of each deleting key
                let delete_orderings: HashMap<OwnedRow, MaxOrderingInfo> =
                    if record_batches.num_delete_rows() == 0 {
                        HashMap::new()
                    } else {
                        let delete_batch = record_batches
                            .concat_delete_batches_transformed(self.hudi_configs.clone())?;
                        let mut delete_orderings: HashMap<OwnedRow, MaxOrderingInfo> =
                            HashMap::with_capacity(delete_batch.num_rows());
                        process_batch_for_max_orderings(
                            &delete_batch,
                            &mut delete_orderings,
                            &key_converter,
                            &event_time_converter,
                            &commit_time_converter,
                            self.hudi_configs.clone(),
                        )?;
                        delete_orderings
                    };

                // Build a mask for records that should be kept
                let mut keep_mask_builder = BooleanArray::builder(num_records);

                let record_keys = extract_record_keys(&key_converter, &data_batch)?;
                let event_times = extract_event_time_ordering_values(
                    &event_time_converter,
                    &data_batch,
                    &ordering_field,
                )?;
                let commit_times =
                    extract_commit_time_ordering_values(&commit_time_converter, &data_batch)?;

                let mut last_key: Option<Row> = None;
                for i in 0..num_records {
                    // Iterator over sorted indices to process records in desc order
                    let idx = desc_indices.value(i) as usize;
                    let curr_key = record_keys.row(idx);
                    let curr_event_time = event_times.row(idx);
                    let curr_commit_time = commit_times.row(idx);

                    let first_seen = last_key != Some(curr_key);
                    if first_seen {
                        last_key = Some(curr_key);

                        let should_keep = match delete_orderings.get(&curr_key.owned()) {
                            Some(delete_max_ordering) => {
                                // If the delete ordering is not greater than the record's ordering,
                                // we keep the record.
                                // Otherwise, we discard it as the delete is more recent.
                                !delete_max_ordering
                                    .is_greater_than(curr_event_time, curr_commit_time)
                            }
                            None => true, // There is no delete for this key, keep it.
                        };

                        keep_mask_builder.append_value(should_keep);
                    } else {
                        // If the record is not first seen,
                        // we don't keep it as its latest version has been processed.
                        keep_mask_builder.append_value(false);
                    }
                }

                // Filter the sorted indices based on the keep mask
                // then take the records
                let keep_mask = keep_mask_builder.finish();
                let keep_indices = arrow::compute::filter(&desc_indices, &keep_mask)?;
                Ok(take_record_batch(&data_batch, &keep_indices)?)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    fn create_configs(
        strategy: &str,
        populates_meta_fields: bool,
        precombine: Option<&str>,
    ) -> HudiConfigs {
        if let Some(precombine) = precombine {
            HudiConfigs::new([
                (RecordMergeStrategy, strategy.to_string()),
                (PopulatesMetaFields, populates_meta_fields.to_string()),
                (PrecombineField, precombine.to_string()),
            ])
        } else {
            HudiConfigs::new([
                (RecordMergeStrategy, strategy.to_string()),
                (PopulatesMetaFields, populates_meta_fields.to_string()),
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

    fn create_test_schema(ts_nullable: bool) -> SchemaRef {
        create_schema(vec![
            (MetaField::CommitTime.as_ref(), DataType::Utf8, false),
            (MetaField::CommitSeqno.as_ref(), DataType::Utf8, false),
            (MetaField::RecordKey.as_ref(), DataType::Utf8, false),
            ("ts", DataType::Int32, ts_nullable),
            ("value", DataType::Int32, false),
        ])
    }

    fn get_sorted_rows(batch: &RecordBatch) -> Vec<(String, String, String, i32, i32)> {
        let commit_time = batch
            .get_string_array(MetaField::CommitTime.as_ref())
            .unwrap();
        let seqno = batch
            .get_string_array(MetaField::CommitSeqno.as_ref())
            .unwrap();
        let keys = batch
            .get_string_array(MetaField::RecordKey.as_ref())
            .unwrap();
        let timestamps = batch.get_array("ts").unwrap();
        let timestamps = timestamps
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        let values = batch.get_array("value").unwrap();
        let values = values
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();

        let mut result: Vec<(String, String, String, i32, i32)> = commit_time
            .iter()
            .zip(seqno.iter())
            .zip(keys.iter())
            .zip(timestamps.iter())
            .zip(values.iter())
            .map(|((((c, s), k), t), v)| {
                (
                    c.unwrap().to_string(),
                    s.unwrap().to_string(),
                    k.unwrap().to_string(),
                    t.unwrap(),
                    v.unwrap(),
                )
            })
            .collect();
        result.sort_unstable_by_key(|(c, s, k, ts, _)| (k.clone(), *ts, c.clone(), s.clone()));
        result
    }

    #[test]
    fn test_merge_records_empty() {
        let schema = create_test_schema(false);

        let configs = create_configs("OVERWRITE_WITH_LATEST", true, Some("ts"));
        let merger = RecordMerger::new(schema.clone(), Arc::new(configs));

        // Test empty input
        let empty_result = merger.merge_record_batches(RecordBatches::new()).unwrap();
        assert_eq!(empty_result.num_rows(), 0);

        // Test single empty batch
        let empty_batch = RecordBatch::new_empty(schema.clone());
        let empty_batches = RecordBatches::new_with_data_batches([empty_batch]);
        let single_empty_result = merger.merge_record_batches(empty_batches).unwrap();
        assert_eq!(single_empty_result.num_rows(), 0);
    }

    #[test]
    fn test_merge_records_append_only() {
        let schema = create_test_schema(false);

        // First batch
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["c1", "c1"])),
                Arc::new(StringArray::from(vec!["s1", "s1"])),
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
                Arc::new(StringArray::from(vec!["c2", "c2"])),
                Arc::new(StringArray::from(vec!["s2", "s2"])),
                Arc::new(StringArray::from(vec!["k1", "k3"])),
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(Int32Array::from(vec![30, 40])),
            ],
        )
        .unwrap();

        let configs = create_configs("APPEND_ONLY", false, None);
        let merger = RecordMerger::new(schema.clone(), Arc::new(configs));
        let merged = merger
            .merge_record_batches(RecordBatches::new_with_data_batches([batch1, batch2]))
            .unwrap();

        // Should contain all records in order without deduplication
        assert_eq!(merged.num_rows(), 4);

        let result = get_sorted_rows(&merged);
        assert_eq!(
            result,
            vec![
                ("c1".to_string(), "s1".to_string(), "k1".to_string(), 1, 10),
                ("c2".to_string(), "s2".to_string(), "k1".to_string(), 3, 30),
                ("c1".to_string(), "s1".to_string(), "k2".to_string(), 2, 20),
                ("c2".to_string(), "s2".to_string(), "k3".to_string(), 4, 40),
            ]
        );
    }

    #[test]
    fn test_merge_records_nulls() {
        let schema = create_test_schema(true);

        // First batch with some null timestamps
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["c1", "c1", "c1"])),
                Arc::new(StringArray::from(vec!["s1", "s1", "s1"])),
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
                Arc::new(StringArray::from(vec!["c2", "c2"])),
                Arc::new(StringArray::from(vec!["s2", "s2"])),
                Arc::new(StringArray::from(vec!["k1", "k2"])),
                Arc::new(Int32Array::from(vec![None, Some(5)])),
                Arc::new(Int32Array::from(vec![40, 50])),
            ],
        )
        .unwrap();

        let configs = create_configs("OVERWRITE_WITH_LATEST", true, Some("ts"));
        let merger = RecordMerger::new(schema.clone(), Arc::new(configs));
        let batches = RecordBatches::new_with_data_batches([batch1, batch2]);
        let merged = merger.merge_record_batches(batches).unwrap();

        assert_eq!(merged.num_rows(), 3);

        let result = get_sorted_rows(&merged);
        assert_eq!(
            result,
            vec![
                ("c1".to_string(), "s1".to_string(), "k1".to_string(), 1, 10), // Keep original since ts is null in 2nd batch
                ("c2".to_string(), "s2".to_string(), "k2".to_string(), 5, 50), // Take second value due to higher ts
                ("c1".to_string(), "s1".to_string(), "k3".to_string(), 3, 30), // Unchanged
            ]
        );
    }

    #[test]
    fn test_merge_records_overwrite_with_latest() {
        let schema = create_test_schema(false);

        // First batch
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["c1", "c1", "c1"])),
                Arc::new(StringArray::from(vec!["s1", "s1", "s1"])),
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
                Arc::new(StringArray::from(vec!["c2", "c2", "c2"])),
                Arc::new(StringArray::from(vec!["s2", "s2", "s2"])),
                Arc::new(StringArray::from(vec!["k1", "k2", "k3"])),
                Arc::new(Int32Array::from(vec![4, 1, 3])),
                Arc::new(Int32Array::from(vec![40, 50, 60])),
            ],
        )
        .unwrap();

        let configs = create_configs("OVERWRITE_WITH_LATEST", true, Some("ts"));
        let merger = RecordMerger::new(schema.clone(), Arc::new(configs));
        let batches = RecordBatches::new_with_data_batches([batch1, batch2]);
        let merged = merger.merge_record_batches(batches).unwrap();

        assert_eq!(merged.num_rows(), 3);

        let result = get_sorted_rows(&merged);
        assert_eq!(
            result,
            vec![
                ("c2".to_string(), "s2".to_string(), "k1".to_string(), 4, 40), // Latest value due to ts=4
                ("c1".to_string(), "s1".to_string(), "k2".to_string(), 2, 20), // Original value since ts=1 < ts=2
                ("c2".to_string(), "s2".to_string(), "k3".to_string(), 3, 60), // Latest value due to equal ts and seqno=s2
            ]
        );
    }
}
