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
use crate::config::read::HudiReadConfig;
use crate::config::table::HudiTableConfig;
use crate::config::util::split_hudi_options_from_others;
use crate::config::HudiConfigs;
use crate::error::CoreError::ReadFileSliceError;
use crate::expr::filter::{Filter, SchemableFilter};
use crate::file_group::file_slice::FileSlice;
use crate::file_group::log_file::scanner::LogFileScanner;
use crate::file_group::record_batches::RecordBatches;
use crate::merge::record_merger::RecordMerger;
use crate::metadata::meta_field::MetaField;
use crate::storage::Storage;
use crate::table::builder::OptionResolver;
use crate::timeline::selector::InstantRange;
use crate::Result;
use arrow::compute::and;
use arrow::compute::filter_record_batch;
use arrow_array::{BooleanArray, RecordBatch};
use futures::TryFutureExt;
use std::convert::TryFrom;
use std::sync::Arc;

/// The reader that handles all read operations against a file group.
#[derive(Clone, Debug)]
pub struct FileGroupReader {
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
}

impl FileGroupReader {
    /// Creates a new reader with the given Hudi configurations and overwriting options.
    ///
    /// # Notes
    /// This API does **not** use [`OptionResolver`] that loads table properties from storage to resolve options.
    pub(crate) fn new_with_configs_and_overwriting_options<I, K, V>(
        hudi_configs: Arc<HudiConfigs>,
        overwriting_options: I,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let (hudi_opts, others) = split_hudi_options_from_others(overwriting_options);

        let mut final_opts = hudi_configs.as_options();
        final_opts.extend(hudi_opts);
        let hudi_configs = Arc::new(HudiConfigs::new(final_opts));
        let storage = Storage::new(Arc::new(others), hudi_configs.clone())?;

        Ok(Self {
            hudi_configs,
            storage,
        })
    }

    /// Creates a new reader with the given base URI and options.
    ///
    /// # Arguments
    /// * `base_uri` - The base URI of the file group's residing table.
    /// * `options` - Additional options for the reader.
    ///
    /// # Notes
    /// This API uses [`OptionResolver`] that loads table properties from storage to resolve options.
    pub fn new_with_options<I, K, V>(base_uri: &str, options: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                let mut resolver = OptionResolver::new_with_options(base_uri, options);
                resolver.resolve_options().await?;
                let hudi_configs = Arc::new(HudiConfigs::new(resolver.hudi_options));
                let storage =
                    Storage::new(Arc::new(resolver.storage_options), hudi_configs.clone())?;

                Ok(Self {
                    hudi_configs,
                    storage,
                })
            })
    }

    fn create_filtering_mask_for_base_file_records(
        &self,
        records: &RecordBatch,
    ) -> Result<Option<BooleanArray>> {
        let populates_meta_fields = self
            .hudi_configs
            .get_or_default(HudiTableConfig::PopulatesMetaFields)
            .to::<bool>();
        if !populates_meta_fields {
            // If meta fields are not populated, commit time filtering is not applicable.
            return Ok(None);
        }

        let mut and_filters: Vec<SchemableFilter> = Vec::new();
        let schema = MetaField::schema();
        if let Some(start) = self
            .hudi_configs
            .try_get(HudiReadConfig::FileGroupStartTimestamp)
            .map(|v| v.to::<String>())
        {
            let filter: Filter =
                Filter::try_from((MetaField::CommitTime.as_ref(), ">", start.as_str()))?;
            let filter = SchemableFilter::try_from((filter, schema.as_ref()))?;
            and_filters.push(filter);
        } else {
            // If start timestamp is not provided, the query is snapshot or time-travel, so
            // commit time filtering is not needed as the base file being read is already
            // filtered and selected by the timeline.
            return Ok(None);
        }

        if let Some(end) = self
            .hudi_configs
            .try_get(HudiReadConfig::FileGroupEndTimestamp)
            .map(|v| v.to::<String>())
        {
            let filter = Filter::try_from((MetaField::CommitTime.as_ref(), "<=", end.as_str()))?;
            let filter = SchemableFilter::try_from((filter, schema.as_ref()))?;
            and_filters.push(filter);
        }

        if and_filters.is_empty() {
            return Ok(None);
        }

        let mut mask = BooleanArray::from(vec![true; records.num_rows()]);
        for filter in &and_filters {
            let col_name = filter.field.name().as_str();
            let col_values = records
                .column_by_name(col_name)
                .ok_or_else(|| ReadFileSliceError(format!("Column {col_name} not found")))?;

            let comparison = filter.apply_comparsion(col_values)?;
            mask = and(&mask, &comparison)?;
        }
        Ok(Some(mask))
    }

    /// Reads the data from the base file at the given relative path.
    ///
    /// # Arguments
    /// * `relative_path` - The relative path to the base file.
    ///
    /// # Returns
    /// A record batch read from the base file.
    pub async fn read_file_slice_by_base_file_path(
        &self,
        relative_path: &str,
    ) -> Result<RecordBatch> {
        let records: RecordBatch = self
            .storage
            .get_parquet_file_data(relative_path)
            .map_err(|e| ReadFileSliceError(format!("Failed to read path {relative_path}: {e:?}")))
            .await?;

        if let Some(mask) = self.create_filtering_mask_for_base_file_records(&records)? {
            filter_record_batch(&records, &mask)
                .map_err(|e| ReadFileSliceError(format!("Failed to filter records: {e:?}")))
        } else {
            Ok(records)
        }
    }

    /// Same as [FileGroupReader::read_file_slice_by_base_file_path], but blocking.
    pub fn read_file_slice_by_base_file_path_blocking(
        &self,
        relative_path: &str,
    ) -> Result<RecordBatch> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(self.read_file_slice_by_base_file_path(relative_path))
    }

    fn create_instant_range_for_log_file_scan(&self) -> InstantRange {
        let timezone = self
            .hudi_configs
            .get_or_default(HudiTableConfig::TimelineTimezone)
            .to::<String>();
        let start_timestamp = self
            .hudi_configs
            .try_get(HudiReadConfig::FileGroupStartTimestamp)
            .map(|v| v.to::<String>());
        let end_timestamp = self
            .hudi_configs
            .try_get(HudiReadConfig::FileGroupEndTimestamp)
            .map(|v| v.to::<String>());
        InstantRange::new(timezone, start_timestamp, end_timestamp, false, true)
    }

    /// Reads the data from the given file slice.
    ///
    /// # Arguments
    /// * `file_slice` - The file slice to read.
    ///
    /// # Returns
    /// A record batch read from the file slice.
    pub async fn read_file_slice(&self, file_slice: &FileSlice) -> Result<RecordBatch> {
        let relative_path = file_slice.base_file_relative_path()?;
        let base_file_only = !file_slice.has_log_file()
            || self
                .hudi_configs
                .get_or_default(HudiReadConfig::UseReadOptimizedMode)
                .to::<bool>();
        if base_file_only {
            self.read_file_slice_by_base_file_path(&relative_path).await
        } else {
            let log_file_paths = file_slice
                .log_files
                .iter()
                .map(|log_file| file_slice.log_file_relative_path(log_file))
                .collect::<Result<Vec<String>>>()?;
            let instant_range = self.create_instant_range_for_log_file_scan();
            let log_batches = LogFileScanner::new(self.hudi_configs.clone(), self.storage.clone())
                .scan(log_file_paths, &instant_range)
                .await?;

            let base_batch = self
                .read_file_slice_by_base_file_path(&relative_path)
                .await?;
            let schema = base_batch.schema();
            let num_data_batches = log_batches.num_data_batches() + 1;
            let num_delete_batches = log_batches.num_delete_batches();
            let mut all_batches =
                RecordBatches::new_with_capacity(num_data_batches, num_delete_batches);
            all_batches.push_data_batch(base_batch);
            all_batches.extend(log_batches);

            let merger = RecordMerger::new(schema.clone(), self.hudi_configs.clone());
            merger.merge_record_batches(all_batches)
        }
    }

    /// Same as [FileGroupReader::read_file_slice], but blocking.
    pub fn read_file_slice_blocking(&self, file_slice: &FileSlice) -> Result<RecordBatch> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(self.read_file_slice(file_slice))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::util::empty_options;
    use crate::error::CoreError;
    use crate::Result;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use std::fs::canonicalize;
    use std::path::PathBuf;
    use std::sync::Arc;
    use url::Url;

    fn get_non_existent_base_uri() -> String {
        "file:///non-existent-path/table".to_string()
    }

    fn get_base_uri_with_valid_props() -> String {
        let url = Url::from_file_path(
            canonicalize(
                PathBuf::from("tests")
                    .join("data")
                    .join("table_props_valid"),
            )
            .unwrap(),
        )
        .unwrap();
        url.as_ref().to_string()
    }

    fn get_base_uri_with_valid_props_minimum() -> String {
        let url = Url::from_file_path(
            canonicalize(
                PathBuf::from("tests")
                    .join("data")
                    .join("table_props_valid_minimum"),
            )
            .unwrap(),
        )
        .unwrap();
        url.as_ref().to_string()
    }

    fn get_base_uri_with_invalid_props() -> String {
        let url = Url::from_file_path(
            canonicalize(
                PathBuf::from("tests")
                    .join("data")
                    .join("table_props_invalid"),
            )
            .unwrap(),
        )
        .unwrap();
        url.as_ref().to_string()
    }

    #[test]
    fn test_new_with_options() {
        let options = vec![("key1", "value1"), ("key2", "value2")];
        let base_uri = get_base_uri_with_valid_props();
        let reader = FileGroupReader::new_with_options(&base_uri, options).unwrap();
        assert!(!reader.storage.options.is_empty());
        assert!(reader
            .storage
            .hudi_configs
            .contains(HudiTableConfig::BasePath));
    }

    #[test]
    fn test_new_with_options_invalid_base_uri_or_invalid_props() {
        let base_uri = get_non_existent_base_uri();
        let result = FileGroupReader::new_with_options(&base_uri, empty_options());
        assert!(result.is_err());

        let base_uri = get_base_uri_with_invalid_props();
        let result = FileGroupReader::new_with_options(&base_uri, empty_options());
        assert!(result.is_err())
    }

    fn create_test_record_batch() -> Result<RecordBatch> {
        let schema = Schema::new(vec![
            Field::new("_hoodie_commit_time", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]);
        let schema = Arc::new(schema);

        let commit_times: ArrayRef = Arc::new(StringArray::from(vec!["1", "2", "3", "4", "5"]));
        let names: ArrayRef = Arc::new(StringArray::from(vec![
            "Alice", "Bob", "Charlie", "David", "Eve",
        ]));
        let ages: ArrayRef = Arc::new(Int64Array::from(vec![25, 30, 35, 40, 45]));

        RecordBatch::try_new(schema, vec![commit_times, names, ages]).map_err(CoreError::ArrowError)
    }

    #[test]
    fn test_create_filtering_mask_for_base_file_records() -> Result<()> {
        let base_uri = get_base_uri_with_valid_props_minimum();
        let records = create_test_record_batch()?;

        // Test case 1: Disable populating the meta fields
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [
                (HudiTableConfig::PopulatesMetaFields.as_ref(), "false"),
                (HudiReadConfig::FileGroupStartTimestamp.as_ref(), "2"),
            ],
        )?;
        let mask = reader.create_filtering_mask_for_base_file_records(&records)?;
        assert_eq!(mask, None, "Commit time filtering should not be needed");

        // Test case 2: No commit time filtering options
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options())?;
        let mask = reader.create_filtering_mask_for_base_file_records(&records)?;
        assert_eq!(mask, None);

        // Test case 3: Filtering commit time > '2'
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [(HudiReadConfig::FileGroupStartTimestamp, "2")],
        )?;
        let mask = reader.create_filtering_mask_for_base_file_records(&records)?;
        assert_eq!(
            mask,
            Some(BooleanArray::from(vec![false, false, true, true, true])),
            "Expected only records with commit_time > '2'"
        );

        // Test case 4: Filtering commit time <= '4'
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [(HudiReadConfig::FileGroupEndTimestamp, "4")],
        )?;
        let mask = reader.create_filtering_mask_for_base_file_records(&records)?;
        assert_eq!(mask, None, "Commit time filtering should not be needed");

        // Test case 5: Filtering commit time > '2' and <= '4'
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [
                (HudiReadConfig::FileGroupStartTimestamp, "2"),
                (HudiReadConfig::FileGroupEndTimestamp, "4"),
            ],
        )?;
        let mask = reader.create_filtering_mask_for_base_file_records(&records)?;
        assert_eq!(
            mask,
            Some(BooleanArray::from(vec![false, false, true, true, false])),
            "Expected only records with commit_time > '2' and <= '4'"
        );

        Ok(())
    }
}
