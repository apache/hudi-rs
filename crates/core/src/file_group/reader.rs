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
use crate::config::util::split_hudi_options_from_others;
use crate::config::HudiConfigs;
use crate::error::CoreError::ReadFileSliceError;
use crate::expr::filter::{Filter, SchemableFilter};
use crate::file_group::file_slice::FileSlice;
use crate::storage::Storage;
use crate::Result;
use arrow::compute::and;
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::Schema;
use futures::TryFutureExt;
use std::sync::Arc;

use crate::file_group::log_file::reader::LogFileReader;
use arrow::compute::filter_record_batch;
use arrow_select::concat::concat_batches;

/// File group reader handles all read operations against a file group.
#[derive(Clone, Debug)]
pub struct FileGroupReader {
    storage: Arc<Storage>,
    and_filters: Vec<SchemableFilter>,
}

impl FileGroupReader {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
            and_filters: Vec::new(),
        }
    }

    pub fn new_with_filters(
        storage: Arc<Storage>,
        and_filters: &[Filter],
        schema: &Schema,
    ) -> Result<Self> {
        let and_filters = and_filters
            .iter()
            .map(|filter| SchemableFilter::try_from((filter.clone(), schema)))
            .collect::<Result<Vec<SchemableFilter>>>()?;

        Ok(Self {
            storage,
            and_filters,
        })
    }

    pub fn new_with_options<I, K, V>(base_uri: &str, options: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let (mut hudi_opts, others) = split_hudi_options_from_others(options);
        hudi_opts.insert(
            HudiTableConfig::BasePath.as_ref().to_string(),
            base_uri.to_string(),
        );

        let hudi_configs = Arc::new(HudiConfigs::new(hudi_opts));

        let storage = Storage::new(Arc::new(others), hudi_configs)?;
        Ok(Self {
            storage,
            and_filters: Vec::new(),
        })
    }

    fn create_boolean_array_mask(&self, records: &RecordBatch) -> Result<BooleanArray> {
        let mut mask = BooleanArray::from(vec![true; records.num_rows()]);
        for filter in &self.and_filters {
            let col_name = filter.field.name().as_str();
            let col_values = records
                .column_by_name(col_name)
                .ok_or_else(|| ReadFileSliceError(format!("Column {col_name} not found")))?;

            let comparison = filter.apply_comparsion(col_values)?;
            mask = and(&mask, &comparison)?;
        }
        Ok(mask)
    }

    pub async fn read_file_slice_by_base_file_path(
        &self,
        relative_path: &str,
    ) -> Result<RecordBatch> {
        let records: RecordBatch = self
            .storage
            .get_parquet_file_data(relative_path)
            .map_err(|e| ReadFileSliceError(format!("Failed to read path {relative_path}: {e:?}")))
            .await?;

        if self.and_filters.is_empty() {
            return Ok(records);
        }

        let mask = self.create_boolean_array_mask(&records)?;
        filter_record_batch(&records, &mask)
            .map_err(|e| ReadFileSliceError(format!("Failed to filter records: {e:?}")))
    }

    pub async fn read_file_slice(
        &self,
        file_slice: &FileSlice,
        base_file_only: bool,
    ) -> Result<RecordBatch> {
        let relative_path = file_slice.base_file_relative_path()?;
        if base_file_only {
            // TODO caller to support read optimized queries
            self.read_file_slice_by_base_file_path(&relative_path).await
        } else {
            let mut log_file_records_unmerged: Vec<RecordBatch> = Vec::new();
            for log_file in &file_slice.log_files {
                let relative_path = file_slice.log_file_relative_path(log_file)?;
                let storage = self.storage.clone();
                let mut log_file_reader = LogFileReader::new(storage, &relative_path).await?;
                let batches = log_file_reader.read_all_records_unmerged()?;
                log_file_records_unmerged.extend_from_slice(&batches);
            }
            let base_file_records = self
                .read_file_slice_by_base_file_path(&relative_path)
                .await?;
            let schema = base_file_records.schema();
            let mut all_records = vec![base_file_records];
            all_records.extend_from_slice(&log_file_records_unmerged);
            // TODO perform merge
            Ok(concat_batches(&schema, &all_records)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CoreError;
    use crate::expr::filter::FilterField;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    use url::Url;

    #[test]
    fn test_new() {
        let base_url = Url::parse("file:///tmp/hudi_data").unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let fg_reader = FileGroupReader::new(storage.clone());
        assert!(Arc::ptr_eq(&fg_reader.storage, &storage));
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("_hoodie_commit_time", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ])
    }

    #[tokio::test]
    async fn test_new_with_filters() -> Result<()> {
        let base_url = Url::parse("file:///tmp/hudi_data").unwrap();
        let storage = Storage::new_with_base_url(base_url)?;
        let schema = create_test_schema();

        // Test case 1: Empty filters
        let reader = FileGroupReader::new_with_filters(storage.clone(), &[], &schema)?;
        assert!(reader.and_filters.is_empty());

        // Test case 2: Multiple filters
        let filters = vec![
            FilterField::new("_hoodie_commit_time").gt("0"),
            FilterField::new("age").gte("18"),
        ];
        let reader = FileGroupReader::new_with_filters(storage.clone(), &filters, &schema)?;
        assert_eq!(reader.and_filters.len(), 2);

        // Test case 3: Invalid field name should error
        let invalid_filters = vec![FilterField::new("non_existent_field").eq("value")];
        assert!(
            FileGroupReader::new_with_filters(storage.clone(), &invalid_filters, &schema).is_err()
        );

        Ok(())
    }

    #[test]
    fn test_new_with_options() -> Result<()> {
        let options = vec![("key1", "value1"), ("key2", "value2")];
        let reader = FileGroupReader::new_with_options("/tmp/hudi_data", options)?;
        assert!(!reader.storage.options.is_empty());
        assert!(reader
            .storage
            .hudi_configs
            .contains(HudiTableConfig::BasePath));
        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_returns_error() {
        let storage =
            Storage::new_with_base_url(Url::parse("file:///non-existent-path/table").unwrap())
                .unwrap();
        let reader = FileGroupReader::new(storage);
        let result = reader
            .read_file_slice_by_base_file_path("non_existent_file")
            .await;
        assert!(matches!(result.unwrap_err(), ReadFileSliceError(_)));
    }

    fn create_test_record_batch() -> Result<RecordBatch> {
        let schema = Arc::new(create_test_schema());

        let commit_times: ArrayRef = Arc::new(StringArray::from(vec!["1", "2", "3", "4", "5"]));
        let names: ArrayRef = Arc::new(StringArray::from(vec![
            "Alice", "Bob", "Charlie", "David", "Eve",
        ]));
        let ages: ArrayRef = Arc::new(Int64Array::from(vec![25, 30, 35, 40, 45]));

        RecordBatch::try_new(schema, vec![commit_times, names, ages]).map_err(CoreError::ArrowError)
    }

    #[test]
    fn test_create_boolean_array_mask() -> Result<()> {
        let storage =
            Storage::new_with_base_url(Url::parse("file:///non-existent-path/table").unwrap())?;
        let schema = create_test_schema();
        let records = create_test_record_batch()?;

        // Test case 1: No filters
        let reader = FileGroupReader::new_with_filters(storage.clone(), &[], &schema)?;
        let mask = reader.create_boolean_array_mask(&records)?;
        assert_eq!(mask, BooleanArray::from(vec![true; 5]));

        // Test case 2: Single filter on commit time
        let filters = vec![FilterField::new("_hoodie_commit_time").gt("2")];
        let reader = FileGroupReader::new_with_filters(storage.clone(), &filters, &schema)?;
        let mask = reader.create_boolean_array_mask(&records)?;
        assert_eq!(
            mask,
            BooleanArray::from(vec![false, false, true, true, true]),
            "Expected only records with commit_time > '2'"
        );

        // Test case 3: Multiple AND filters
        let filters = vec![
            FilterField::new("_hoodie_commit_time").gt("2"),
            FilterField::new("age").lt("40"),
        ];
        let reader = FileGroupReader::new_with_filters(storage.clone(), &filters, &schema)?;
        let mask = reader.create_boolean_array_mask(&records)?;
        assert_eq!(
            mask,
            BooleanArray::from(vec![false, false, true, false, false]),
            "Expected only record with commit_time > '2' AND age < 40"
        );

        // Test case 4: Filter resulting in all false
        let filters = vec![FilterField::new("age").gt("100")];
        let reader = FileGroupReader::new_with_filters(storage.clone(), &filters, &schema)?;
        let mask = reader.create_boolean_array_mask(&records)?;
        assert_eq!(mask, BooleanArray::from(vec![false; 5]));

        Ok(())
    }
}
