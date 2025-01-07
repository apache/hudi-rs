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
use crate::file_group::FileSlice;
use crate::storage::Storage;
use crate::Result;
use arrow::compute::and;
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::Schema;
use futures::TryFutureExt;
use std::sync::Arc;

use arrow::compute::filter_record_batch;

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

        let mut mask = BooleanArray::from(vec![true; records.num_rows()]);
        for filter in &self.and_filters {
            let col_name = filter.field.name().as_str();
            let col_values = records
                .column_by_name(col_name)
                .ok_or_else(|| ReadFileSliceError(format!("Column {col_name} not found")))?;

            let comparison = filter.apply_comparsion(col_values)?;
            mask = and(&mask, &comparison)?;
        }

        filter_record_batch(&records, &mask)
            .map_err(|e| ReadFileSliceError(format!("Failed to filter records: {e:?}")))
    }

    pub async fn read_file_slice(&self, file_slice: &FileSlice) -> Result<RecordBatch> {
        self.read_file_slice_by_base_file_path(&file_slice.base_file_relative_path())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    #[test]
    fn test_new() {
        let base_url = Url::parse("file:///tmp/hudi_data").unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let fg_reader = FileGroupReader::new(storage.clone());
        assert!(Arc::ptr_eq(&fg_reader.storage, &storage));
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
}
