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
//! This module is responsible for Hudi table APIs.
//!
//! It provides a quick entry point for reading Hudi table metadata and data,
//! facilitating adaptation and compatibility across various engines.
//!
//! **Example**
//! 1. create hudi table
//! ```rust
//! use url::Url;
//! use hudi_core::table::Table;
//!
//! pub async fn test() {
//!     let base_uri = Url::from_file_path("/tmp/hudi_data").unwrap();
//!     let hudi_table = Table::new(base_uri.path()).await.unwrap();
//! }
//! ```
//! 2. get hudi table schema(arrow_schema::Schema)
//! ```rust
//! use url::Url;
//! use hudi_core::table::Table;
//!
//! pub async fn test() {
//!     use arrow_schema::Schema;
//!     let base_uri = Url::from_file_path("/tmp/hudi_data").unwrap();
//!     let hudi_table = Table::new(base_uri.path()).await.unwrap();
//!     let schema = hudi_table.get_schema().await.unwrap();
//! }
//! ```
//! 3. read hudi table
//! ```rust
//! use url::Url;
//! use hudi_core::table::Table;
//!
//! pub async fn test() {
//!     let base_uri = Url::from_file_path("/tmp/hudi_data").unwrap();
//!     let hudi_table = Table::new(base_uri.path()).await.unwrap();
//!     let record_read = hudi_table.read_snapshot(&[]).await.unwrap();
//! }
//! ```
//! 4. get file slice
//!    Users can obtain metadata to customize reading methods, read in batches, perform parallel reads, and more.
//! ```rust
//! use url::Url;
//! use hudi_core::table::Table;
//! use hudi_core::storage::util::parse_uri;
//! use hudi_core::storage::util::join_url_segments;
//!
//! pub async fn test() {
//!     let base_uri = Url::from_file_path("/tmp/hudi_data").unwrap();
//!     let hudi_table = Table::new(base_uri.path()).await.unwrap();
//!     let file_slices = hudi_table
//!             .get_file_slices_splits(2, &[])
//!             .await.unwrap();
//!     // define every parquet task reader how many slice
//!     let mut parquet_file_groups: Vec<Vec<String>> = Vec::new();
//!         for file_slice_vec in file_slices {
//!             let file_group_vec = file_slice_vec
//!                 .iter()
//!                 .map(|f| {
//!                     let relative_path = f.base_file_relative_path().unwrap();
//!                     let url = join_url_segments(&base_uri, &[relative_path.as_str()]).unwrap();
//!                     url.path().to_string()
//!                 })
//!                 .collect();
//!             parquet_file_groups.push(file_group_vec)
//!         }
//! }
//! ```

pub mod builder;
mod fs_view;
mod listing;
pub mod partition;

use crate::config::table::HudiTableConfig::PartitionFields;
use crate::config::table::{HudiTableConfig, TableTypeValue};
use crate::config::HudiConfigs;
use crate::expr::filter::{from_str_tuples, Filter};
use crate::file_group::file_slice::FileSlice;
use crate::file_group::reader::FileGroupReader;
use crate::table::builder::TableBuilder;
use crate::table::fs_view::FileSystemView;
use crate::table::partition::PartitionPruner;
use crate::timeline::{Timeline, EARLIEST_START_TIMESTAMP};
use crate::Result;

use crate::config::read::HudiReadConfig;
use arrow::record_batch::RecordBatch;
use arrow_schema::{Field, Schema};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

/// The main struct that provides table APIs for interacting with a Hudi table.
#[derive(Clone, Debug)]
pub struct Table {
    pub hudi_configs: Arc<HudiConfigs>,
    pub storage_options: Arc<HashMap<String, String>>,
    pub timeline: Timeline,
    pub file_system_view: FileSystemView,
}

impl Table {
    /// Create hudi table by base_uri
    pub async fn new(base_uri: &str) -> Result<Self> {
        TableBuilder::from_base_uri(base_uri).build().await
    }

    /// Create hudi table with options
    pub async fn new_with_options<I, K, V>(base_uri: &str, options: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        TableBuilder::from_base_uri(base_uri)
            .with_options(options)
            .build()
            .await
    }

    pub fn hudi_options(&self) -> HashMap<String, String> {
        self.hudi_configs.as_options()
    }

    pub fn storage_options(&self) -> HashMap<String, String> {
        self.storage_options.as_ref().clone()
    }

    #[cfg(feature = "datafusion")]
    pub fn register_storage(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) {
        self.timeline
            .storage
            .register_object_store(runtime_env.clone());
        self.file_system_view
            .storage
            .register_object_store(runtime_env.clone());
    }

    pub fn base_url(&self) -> Url {
        let err_msg = format!("{:?} is missing or invalid.", HudiTableConfig::BasePath);
        self.hudi_configs
            .get(HudiTableConfig::BasePath)
            .expect(&err_msg)
            .to_url()
            .expect(&err_msg)
    }

    pub fn table_name(&self) -> String {
        let err_msg = format!("{:?} is missing or invalid.", HudiTableConfig::TableName);
        self.hudi_configs
            .get(HudiTableConfig::TableName)
            .expect(&err_msg)
            .to::<String>()
    }

    pub fn table_type(&self) -> String {
        let err_msg = format!("{:?} is missing or invalid.", HudiTableConfig::TableType);
        self.hudi_configs
            .get(HudiTableConfig::TableType)
            .expect(&err_msg)
            .to::<String>()
    }

    pub fn timezone(&self) -> String {
        self.hudi_configs
            .get_or_default(HudiTableConfig::TimelineTimezone)
            .to::<String>()
    }

    /// Get the latest Avro schema string of the table.
    pub async fn get_avro_schema(&self) -> Result<String> {
        self.timeline.get_latest_avro_schema().await
    }

    /// Get the latest [Schema] of the table.
    pub async fn get_schema(&self) -> Result<Schema> {
        self.timeline.get_latest_schema().await
    }

    /// Get the latest partition [Schema] of the table
    pub async fn get_partition_schema(&self) -> Result<Schema> {
        let partition_fields: HashSet<String> = self
            .hudi_configs
            .get_or_default(PartitionFields)
            .to::<Vec<String>>()
            .into_iter()
            .collect();

        let schema = self.get_schema().await?;
        let partition_fields: Vec<Arc<Field>> = schema
            .fields()
            .iter()
            .filter(|field| partition_fields.contains(field.name()))
            .cloned()
            .collect();

        Ok(Schema::new(partition_fields))
    }

    /// Get all the [FileSlice]s in splits from the table.
    ///
    /// # Arguments
    ///     * `n` - The number of chunks to split the file slices into.
    ///     * `filters` - Partition filters to apply.
    pub async fn get_file_slices_splits(
        &self,
        n: usize,
        filters: &[(&str, &str, &str)],
    ) -> Result<Vec<Vec<FileSlice>>> {
        if let Some(timestamp) = self.timeline.get_latest_commit_timestamp() {
            let filters = from_str_tuples(filters)?;
            self.get_file_slices_splits_internal(n, timestamp, &filters)
                .await
        } else {
            Ok(Vec::new())
        }
    }

    /// Get all the [FileSlice]s in splits from the table at a given timestamp.
    ///
    /// # Arguments
    ///     * `n` - The number of chunks to split the file slices into.
    ///     * `timestamp` - The timestamp which file slices associated with.
    ///     * `filters` - Partition filters to apply.
    pub async fn get_file_slices_splits_as_of(
        &self,
        n: usize,
        timestamp: &str,
        filters: &[(&str, &str, &str)],
    ) -> Result<Vec<Vec<FileSlice>>> {
        let filters = from_str_tuples(filters)?;
        self.get_file_slices_splits_internal(n, timestamp, &filters)
            .await
    }

    async fn get_file_slices_splits_internal(
        &self,
        n: usize,
        timestamp: &str,
        filters: &[Filter],
    ) -> Result<Vec<Vec<FileSlice>>> {
        let file_slices = self.get_file_slices_internal(timestamp, filters).await?;
        if file_slices.is_empty() {
            return Ok(Vec::new());
        }

        let n = std::cmp::max(1, n);
        let chunk_size = file_slices.len().div_ceil(n);

        Ok(file_slices
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect())
    }

    /// Get all the [FileSlice]s in the table.
    ///
    /// # Arguments
    ///     * `filters` - Partition filters to apply.
    ///
    /// # Notes
    ///     * This API is useful for implementing snapshot query.
    pub async fn get_file_slices(&self, filters: &[(&str, &str, &str)]) -> Result<Vec<FileSlice>> {
        if let Some(timestamp) = self.timeline.get_latest_commit_timestamp() {
            let filters = from_str_tuples(filters)?;
            self.get_file_slices_internal(timestamp, &filters).await
        } else {
            Ok(Vec::new())
        }
    }

    /// Get all the [FileSlice]s in the table at a given timestamp.
    ///
    /// # Arguments
    ///     * `timestamp` - The timestamp which file slices associated with.
    ///     * `filters` - Partition filters to apply.
    ///
    /// # Notes
    ///     * This API is useful for implementing time travel query.
    pub async fn get_file_slices_as_of(
        &self,
        timestamp: &str,
        filters: &[(&str, &str, &str)],
    ) -> Result<Vec<FileSlice>> {
        let filters = from_str_tuples(filters)?;
        self.get_file_slices_internal(timestamp, &filters).await
    }

    async fn get_file_slices_internal(
        &self,
        timestamp: &str,
        filters: &[Filter],
    ) -> Result<Vec<FileSlice>> {
        let excludes = self
            .timeline
            .get_replaced_file_groups_as_of(timestamp)
            .await?;
        let partition_schema = self.get_partition_schema().await?;
        let partition_pruner =
            PartitionPruner::new(filters, &partition_schema, self.hudi_configs.as_ref())?;
        self.file_system_view
            .get_file_slices_as_of(timestamp, &partition_pruner, &excludes)
            .await
    }

    /// Get all the changed [FileSlice]s in the table between the given timestamps.
    ///
    /// # Arguments
    ///     * `start_timestamp` - If provided, only file slices that were changed after this timestamp will be returned.
    ///     * `end_timestamp` - If provided, only file slices that were changed before or at this timestamp will be returned.
    ///
    /// # Notes
    ///     * This API is useful for implementing incremental query.
    pub async fn get_file_slices_between(
        &self,
        start_timestamp: Option<&str>,
        end_timestamp: Option<&str>,
    ) -> Result<Vec<FileSlice>> {
        // If the end timestamp is not provided, use the latest commit timestamp.
        let Some(end) = end_timestamp.or_else(|| self.timeline.get_latest_commit_timestamp())
        else {
            // No latest commit timestamp means the table is empty.
            return Ok(Vec::new());
        };

        let start = start_timestamp.unwrap_or(EARLIEST_START_TIMESTAMP);

        self.get_file_slices_between_internal(start, end).await
    }

    async fn get_file_slices_between_internal(
        &self,
        start_timestamp: &str,
        end_timestamp: &str,
    ) -> Result<Vec<FileSlice>> {
        let mut file_slices: Vec<FileSlice> = Vec::new();
        let file_groups = self
            .timeline
            .get_file_groups_between(Some(start_timestamp), Some(end_timestamp))
            .await?;
        for file_group in file_groups {
            if let Some(file_slice) = file_group.get_file_slice_as_of(end_timestamp) {
                file_slices.push(file_slice.clone());
            }
        }

        Ok(file_slices)
    }

    /// Create a [FileGroupReader] using the [Table]'s Hudi configs, and overwriting options.
    pub fn create_file_group_reader_with_options<I, K, V>(
        &self,
        options: I,
    ) -> Result<FileGroupReader>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let mut overwriting_options = HashMap::with_capacity(self.storage_options.len());
        for (k, v) in self.storage_options.iter() {
            overwriting_options.insert(k.clone(), v.clone());
        }
        for (k, v) in options {
            overwriting_options.insert(k.as_ref().to_string(), v.into());
        }
        FileGroupReader::new_with_configs_and_options(
            self.hudi_configs.clone(),
            overwriting_options,
        )
    }

    /// Get all the latest records in the table.
    ///
    /// # Arguments
    ///     * `filters` - Partition filters to apply.
    pub async fn read_snapshot(&self, filters: &[(&str, &str, &str)]) -> Result<Vec<RecordBatch>> {
        if let Some(timestamp) = self.timeline.get_latest_commit_timestamp() {
            let filters = from_str_tuples(filters)?;
            self.read_snapshot_internal(timestamp, &filters).await
        } else {
            Ok(Vec::new())
        }
    }

    /// Get all the records in the table at a given timestamp.
    ///
    /// # Arguments
    ///     * `timestamp` - The timestamp which records associated with.
    ///     * `filters` - Partition filters to apply.
    pub async fn read_snapshot_as_of(
        &self,
        timestamp: &str,
        filters: &[(&str, &str, &str)],
    ) -> Result<Vec<RecordBatch>> {
        let filters = from_str_tuples(filters)?;
        self.read_snapshot_internal(timestamp, &filters).await
    }

    async fn read_snapshot_internal(
        &self,
        timestamp: &str,
        filters: &[Filter],
    ) -> Result<Vec<RecordBatch>> {
        let file_slices = self.get_file_slices_internal(timestamp, filters).await?;
        let fg_reader = self.create_file_group_reader_with_options([(
            HudiReadConfig::FileGroupEndTimestamp,
            timestamp,
        )])?;
        let batches =
            futures::future::try_join_all(file_slices.iter().map(|f| fg_reader.read_file_slice(f)))
                .await?;
        Ok(batches)
    }

    /// Get records that were inserted or updated between the given timestamps.
    ///
    /// Records that were updated multiple times should have their latest states within
    /// the time span being returned.
    ///
    /// # Arguments
    ///     * `start_timestamp` - Only records that were inserted or updated after this timestamp will be returned.
    ///     * `end_timestamp` - If provided, only records that were inserted or updated before or at this timestamp will be returned.
    pub async fn read_incremental_records(
        &self,
        start_timestamp: &str,
        end_timestamp: Option<&str>,
    ) -> Result<Vec<RecordBatch>> {
        // If the end timestamp is not provided, use the latest commit timestamp.
        let Some(end_timestamp) =
            end_timestamp.or_else(|| self.timeline.get_latest_commit_timestamp())
        else {
            return Ok(Vec::new());
        };

        let file_slices = self
            .get_file_slices_between_internal(start_timestamp, end_timestamp)
            .await?;

        let fg_reader = self.create_file_group_reader_with_options([
            (HudiReadConfig::FileGroupStartTimestamp, start_timestamp),
            (HudiReadConfig::FileGroupEndTimestamp, end_timestamp),
        ])?;

        let batches =
            futures::future::try_join_all(file_slices.iter().map(|f| fg_reader.read_file_slice(f)))
                .await?;
        Ok(batches)
    }

    /// Get the change-data-capture (CDC) records between the given timestamps.
    ///
    /// The CDC records should reflect the records that were inserted, updated, and deleted
    /// between the timestamps.
    #[allow(dead_code)]
    async fn read_incremental_changes(
        &self,
        _start_timestamp: &str,
        _end_timestamp: Option<&str>,
    ) -> Result<Vec<RecordBatch>> {
        todo!("read_incremental_changes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig::{
        BaseFileFormat, Checksum, DatabaseName, DropsPartitionFields, IsHiveStylePartitioning,
        IsPartitionPathUrlencoded, KeyGeneratorClass, PartitionFields, PopulatesMetaFields,
        PrecombineField, RecordKeyFields, TableName, TableType, TableVersion,
        TimelineLayoutVersion, TimelineTimezone,
    };
    use crate::config::util::empty_options;
    use crate::config::HUDI_CONF_DIR;
    use crate::storage::util::join_url_segments;
    use crate::storage::Storage;
    use hudi_test::SampleTable;
    use std::collections::HashSet;
    use std::fs::canonicalize;
    use std::path::PathBuf;
    use std::{env, panic};

    /// Test helper to create a new `Table` instance without validating the configuration.
    ///
    /// # Arguments
    ///
    /// * `table_dir_name` - Name of the table root directory; all under `crates/core/tests/data/`.
    async fn get_test_table_without_validation(table_dir_name: &str) -> Table {
        let base_url = Url::from_file_path(
            canonicalize(PathBuf::from("tests").join("data").join(table_dir_name)).unwrap(),
        )
        .unwrap();
        Table::new_with_options(
            base_url.as_str(),
            [("hoodie.internal.skip.config.validation", "true")],
        )
        .await
        .unwrap()
    }

    /// Test helper to get relative file paths from the table with filters.
    async fn get_file_paths_with_filters(
        table: &Table,
        filters: &[(&str, &str, &str)],
    ) -> Result<Vec<String>> {
        let mut file_paths = Vec::new();
        let base_url = table.base_url();
        for f in table.get_file_slices(filters).await? {
            let relative_path = f.base_file_relative_path()?;
            let file_url = join_url_segments(&base_url, &[relative_path.as_str()])?;
            file_paths.push(file_url.to_string());
        }
        Ok(file_paths)
    }

    #[tokio::test]
    async fn test_hudi_table_get_hudi_options() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let hudi_options = hudi_table.hudi_options();
        for (k, v) in hudi_options.iter() {
            assert!(k.starts_with("hoodie."));
            assert!(!v.is_empty());
        }
    }

    #[tokio::test]
    async fn test_hudi_table_get_storage_options() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();

        let cloud_prefixes: HashSet<_> = Storage::CLOUD_STORAGE_PREFIXES
            .iter()
            .map(|prefix| prefix.to_lowercase())
            .collect();

        for (key, value) in hudi_table.storage_options.iter() {
            let key_lower = key.to_lowercase();
            assert!(
                cloud_prefixes
                    .iter()
                    .any(|prefix| key_lower.starts_with(prefix)),
                "Storage option key '{}' should start with a cloud storage prefix",
                key
            );
            assert!(
                !value.is_empty(),
                "Storage option value for key '{}' should not be empty",
                key
            );
        }
    }

    #[tokio::test]
    async fn hudi_table_get_schema() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let fields: Vec<String> = hudi_table
            .get_schema()
            .await
            .unwrap()
            .flattened_fields()
            .into_iter()
            .map(|f| f.name().to_string())
            .collect();
        assert_eq!(
            fields,
            vec![
                "_hoodie_commit_time",
                "_hoodie_commit_seqno",
                "_hoodie_record_key",
                "_hoodie_partition_path",
                "_hoodie_file_name",
                "id",
                "name",
                "isActive",
                "byteField",
                "shortField",
                "intField",
                "longField",
                "floatField",
                "doubleField",
                "decimalField",
                "dateField",
                "timestampField",
                "binaryField",
                "arrayField",
                "array",
                "arr_struct_f1",
                "arr_struct_f2",
                "mapField",
                "key_value",
                "key",
                "value",
                "map_field_value_struct_f1",
                "map_field_value_struct_f2",
                "structField",
                "field1",
                "field2",
                "child_struct",
                "child_field1",
                "child_field2"
            ]
        );
    }

    #[tokio::test]
    async fn hudi_table_get_partition_schema() {
        let base_url = SampleTable::V6TimebasedkeygenNonhivestyle.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let fields: Vec<String> = hudi_table
            .get_partition_schema()
            .await
            .unwrap()
            .flattened_fields()
            .into_iter()
            .map(|f| f.name().to_string())
            .collect();
        assert_eq!(fields, vec!["ts_str"]);
    }

    #[tokio::test]
    async fn validate_invalid_table_props() {
        let table = get_test_table_without_validation("table_props_invalid").await;
        let configs = table.hudi_configs;
        assert!(
            configs.validate(BaseFileFormat).is_err(),
            "required config is missing"
        );
        assert!(configs.validate(Checksum).is_err());
        assert!(
            configs.validate(DatabaseName).is_ok(),
            "non-required config is missing"
        );
        assert!(configs.validate(DropsPartitionFields).is_err());
        assert!(configs.validate(IsHiveStylePartitioning).is_err());
        assert!(configs.validate(IsPartitionPathUrlencoded).is_err());
        assert!(
            configs.validate(KeyGeneratorClass).is_ok(),
            "non-required config is missing"
        );
        assert!(
            configs.validate(PartitionFields).is_ok(),
            "non-required config is missing"
        );
        assert!(
            configs.validate(PrecombineField).is_ok(),
            "non-required config is missing"
        );
        assert!(
            configs.validate(PopulatesMetaFields).is_ok(),
            "non-required config is missing"
        );
        assert!(
            configs.validate(RecordKeyFields).is_ok(),
            "non-required config is missing"
        );
        assert!(
            configs.validate(TableName).is_err(),
            "required config is missing"
        );
        assert!(
            configs.validate(TableType).is_ok(),
            "Valid table type value"
        );
        assert!(configs.validate(TableVersion).is_err());
        assert!(configs.validate(TimelineLayoutVersion).is_err());
        assert!(
            configs.validate(TimelineTimezone).is_ok(),
            "non-required config is missing"
        );
    }

    #[tokio::test]
    async fn get_invalid_table_props() {
        let table = get_test_table_without_validation("table_props_invalid").await;
        let configs = table.hudi_configs;
        assert!(configs.get(BaseFileFormat).is_err());
        assert!(configs.get(Checksum).is_err());
        assert!(configs.get(DatabaseName).is_err());
        assert!(configs.get(DropsPartitionFields).is_err());
        assert!(configs.get(IsHiveStylePartitioning).is_err());
        assert!(configs.get(IsPartitionPathUrlencoded).is_err());
        assert!(configs.get(KeyGeneratorClass).is_err());
        assert!(configs.get(PartitionFields).is_err());
        assert!(configs.get(PrecombineField).is_err());
        assert!(configs.get(PopulatesMetaFields).is_err());
        assert!(configs.get(RecordKeyFields).is_err());
        assert!(configs.get(TableName).is_err());
        assert!(configs.get(TableType).is_ok(), "Valid table type value");
        assert!(configs.get(TableVersion).is_err());
        assert!(configs.get(TimelineLayoutVersion).is_err());
        assert!(configs.get(TimelineTimezone).is_err());
    }

    #[tokio::test]
    async fn get_default_for_invalid_table_props() {
        let table = get_test_table_without_validation("table_props_invalid").await;
        let configs = table.hudi_configs;
        assert_eq!(
            configs.get_or_default(BaseFileFormat).to::<String>(),
            "parquet"
        );
        assert!(panic::catch_unwind(|| configs.get_or_default(Checksum)).is_err());
        assert_eq!(
            configs.get_or_default(DatabaseName).to::<String>(),
            "default"
        );
        assert!(!configs.get_or_default(DropsPartitionFields).to::<bool>());
        assert!(panic::catch_unwind(|| configs.get_or_default(IsHiveStylePartitioning)).is_err());
        assert!(panic::catch_unwind(|| configs.get_or_default(IsPartitionPathUrlencoded)).is_err());
        assert!(panic::catch_unwind(|| configs.get_or_default(KeyGeneratorClass)).is_err());
        assert!(configs
            .get_or_default(PartitionFields)
            .to::<Vec<String>>()
            .is_empty());
        assert!(panic::catch_unwind(|| configs.get_or_default(PrecombineField)).is_err());
        assert!(configs.get_or_default(PopulatesMetaFields).to::<bool>());
        assert!(panic::catch_unwind(|| configs.get_or_default(RecordKeyFields)).is_err());
        assert!(panic::catch_unwind(|| configs.get_or_default(TableName)).is_err());
        assert_eq!(
            configs.get_or_default(TableType).to::<String>(),
            "COPY_ON_WRITE"
        );
        assert!(panic::catch_unwind(|| configs.get_or_default(TableVersion)).is_err());
        assert!(panic::catch_unwind(|| configs.get_or_default(TimelineLayoutVersion)).is_err());
        assert_eq!(
            configs.get_or_default(TimelineTimezone).to::<String>(),
            "utc"
        );
    }

    #[tokio::test]
    async fn get_valid_table_props() {
        let table = get_test_table_without_validation("table_props_valid").await;
        let configs = table.hudi_configs;
        assert_eq!(
            configs.get(BaseFileFormat).unwrap().to::<String>(),
            "parquet"
        );
        assert_eq!(configs.get(Checksum).unwrap().to::<isize>(), 3761586722);
        assert_eq!(configs.get(DatabaseName).unwrap().to::<String>(), "db");
        assert!(!configs.get(DropsPartitionFields).unwrap().to::<bool>());
        assert!(!configs.get(IsHiveStylePartitioning).unwrap().to::<bool>());
        assert!(!configs.get(IsPartitionPathUrlencoded).unwrap().to::<bool>());
        assert_eq!(
            configs.get(KeyGeneratorClass).unwrap().to::<String>(),
            "org.apache.hudi.keygen.SimpleKeyGenerator"
        );
        assert_eq!(
            configs.get(PartitionFields).unwrap().to::<Vec<String>>(),
            vec!["city"]
        );
        assert_eq!(configs.get(PrecombineField).unwrap().to::<String>(), "ts");
        assert!(configs.get(PopulatesMetaFields).unwrap().to::<bool>());
        assert_eq!(
            configs.get(RecordKeyFields).unwrap().to::<Vec<String>>(),
            vec!["uuid"]
        );
        assert_eq!(configs.get(TableName).unwrap().to::<String>(), "trips");
        assert_eq!(
            configs.get(TableType).unwrap().to::<String>(),
            "COPY_ON_WRITE"
        );
        assert_eq!(configs.get(TableVersion).unwrap().to::<isize>(), 6);
        assert_eq!(configs.get(TimelineLayoutVersion).unwrap().to::<isize>(), 1);
        assert_eq!(
            configs.get(TimelineTimezone).unwrap().to::<String>(),
            "local"
        );
    }

    #[tokio::test]
    async fn get_global_table_props() {
        // Without the environment variable HUDI_CONF_DIR
        let table = get_test_table_without_validation("table_props_partial").await;
        let configs = table.hudi_configs;
        assert!(configs.get(DatabaseName).is_err());
        assert!(configs.get(TableType).is_err());
        assert_eq!(configs.get(TableName).unwrap().to::<String>(), "trips");

        // Environment variable HUDI_CONF_DIR points to nothing
        let base_path = env::current_dir().unwrap();
        let hudi_conf_dir = base_path.join("random/wrong/dir");
        env::set_var(HUDI_CONF_DIR, hudi_conf_dir.as_os_str());
        let table = get_test_table_without_validation("table_props_partial").await;
        let configs = table.hudi_configs;
        assert!(configs.get(DatabaseName).is_err());
        assert!(configs.get(TableType).is_err());
        assert_eq!(configs.get(TableName).unwrap().to::<String>(), "trips");

        // With global config
        let base_path = env::current_dir().unwrap();
        let hudi_conf_dir = base_path.join("tests/data/hudi_conf_dir");
        env::set_var(HUDI_CONF_DIR, hudi_conf_dir.as_os_str());
        let table = get_test_table_without_validation("table_props_partial").await;
        let configs = table.hudi_configs;
        assert_eq!(configs.get(DatabaseName).unwrap().to::<String>(), "tmpdb");
        assert_eq!(
            configs.get(TableType).unwrap().to::<String>(),
            "MERGE_ON_READ"
        );
        assert_eq!(configs.get(TableName).unwrap().to::<String>(), "trips");
        env::remove_var(HUDI_CONF_DIR)
    }

    #[tokio::test]
    async fn hudi_table_read_file_slice() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let batches = hudi_table
            .create_file_group_reader_with_options(empty_options())
            .unwrap()
            .read_file_slice_by_base_file_path(
                "a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",
            )
            .await
            .unwrap();
        assert_eq!(batches.num_rows(), 4);
        assert_eq!(batches.num_columns(), 21);
    }

    #[tokio::test]
    async fn empty_hudi_table_get_file_slices_splits() {
        let base_url = SampleTable::V6Empty.url_to_cow();

        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let file_slices_splits = hudi_table.get_file_slices_splits(2, &[]).await.unwrap();
        assert!(file_slices_splits.is_empty());
    }

    #[tokio::test]
    async fn hudi_table_get_file_slices_splits() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyle.url_to_cow();

        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let file_slices_splits = hudi_table.get_file_slices_splits(2, &[]).await.unwrap();
        assert_eq!(file_slices_splits.len(), 2);
        assert_eq!(file_slices_splits[0].len(), 2);
        assert_eq!(file_slices_splits[1].len(), 1);
    }

    #[tokio::test]
    async fn hudi_table_get_file_slices_splits_as_of_timestamps() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyleOverwritetable.url_to_mor_parquet();
        let hudi_table = Table::new(base_url.path()).await.unwrap();

        // before replacecommit (insert overwrite table)
        let second_latest_timestamp = "20250121000656060";
        let file_slices_splits = hudi_table
            .get_file_slices_splits_as_of(2, second_latest_timestamp, &[])
            .await
            .unwrap();
        assert_eq!(file_slices_splits.len(), 2);
        assert_eq!(file_slices_splits[0].len(), 2);
        assert_eq!(file_slices_splits[1].len(), 1);
        let file_slices = file_slices_splits
            .iter()
            .flatten()
            .filter(|f| f.partition_path == "10")
            .collect::<Vec<_>>();
        assert_eq!(
            file_slices.len(),
            1,
            "Partition 10 should have 1 file slice"
        );
        let file_slice = file_slices[0];
        assert_eq!(
            file_slice.base_file.file_name(),
            "92e64357-e4d1-4639-a9d3-c3535829d0aa-0_1-53-79_20250121000647668.parquet"
        );
        assert_eq!(
            file_slice.log_files.len(),
            1,
            "File slice should have 1 log file"
        );
        assert_eq!(
            file_slice.log_files.iter().next().unwrap().file_name(),
            ".92e64357-e4d1-4639-a9d3-c3535829d0aa-0_20250121000647668.log.1_0-73-101"
        );

        // as of replacecommit (insert overwrite table)
        let latest_timestamp = "20250121000702475";
        let file_slices_splits = hudi_table
            .get_file_slices_splits_as_of(2, latest_timestamp, &[])
            .await
            .unwrap();
        assert_eq!(file_slices_splits.len(), 1);
        assert_eq!(file_slices_splits[0].len(), 1);
    }

    #[tokio::test]
    async fn hudi_table_get_file_slices_as_of_timestamps() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();

        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let file_slices = hudi_table.get_file_slices(&[]).await.unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path().unwrap())
                .collect::<Vec<_>>(),
            vec!["a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",]
        );

        // as of the latest timestamp
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let file_slices = hudi_table
            .get_file_slices_as_of("20240418173551906", &[])
            .await
            .unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path().unwrap())
                .collect::<Vec<_>>(),
            vec!["a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",]
        );

        // as of just smaller than the latest timestamp
        let hudi_table = Table::new_with_options(base_url.path(), empty_options())
            .await
            .unwrap();
        let file_slices = hudi_table
            .get_file_slices_as_of("20240418173551905", &[])
            .await
            .unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path().unwrap())
                .collect::<Vec<_>>(),
            vec!["a079bdb3-731c-4894-b855-abfcd6921007-0_0-182-253_20240418173550988.parquet",]
        );

        // as of non-exist old timestamp
        let hudi_table = Table::new_with_options(base_url.path(), empty_options())
            .await
            .unwrap();
        let file_slices = hudi_table
            .get_file_slices_as_of("19700101000000", &[])
            .await
            .unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path().unwrap())
                .collect::<Vec<_>>(),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn empty_hudi_table_get_file_slices_between_timestamps() {
        let base_url = SampleTable::V6Empty.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let file_slices = hudi_table
            .get_file_slices_between(Some(EARLIEST_START_TIMESTAMP), None)
            .await
            .unwrap();
        assert!(file_slices.is_empty())
    }

    #[tokio::test]
    async fn hudi_table_get_file_slices_between_timestamps() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyleOverwritetable.url_to_mor_parquet();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let mut file_slices = hudi_table
            .get_file_slices_between(None, Some("20250121000656060"))
            .await
            .unwrap();
        assert_eq!(file_slices.len(), 3);

        file_slices.sort_unstable_by_key(|f| f.partition_path.clone());

        let file_slice_0 = &file_slices[0];
        assert_eq!(file_slice_0.partition_path, "10");
        assert_eq!(
            file_slice_0.file_id(),
            "92e64357-e4d1-4639-a9d3-c3535829d0aa-0"
        );
        assert_eq!(file_slice_0.log_files.len(), 1);

        let file_slice_1 = &file_slices[1];
        assert_eq!(file_slice_1.partition_path, "20");
        assert_eq!(
            file_slice_1.file_id(),
            "d49ae379-4f20-4549-8e23-a5f9604412c0-0"
        );
        assert!(file_slice_1.log_files.is_empty());

        let file_slice_2 = &file_slices[2];
        assert_eq!(file_slice_2.partition_path, "30");
        assert_eq!(
            file_slice_2.file_id(),
            "de3550df-e12c-4591-9335-92ff992258a2-0"
        );
        assert!(file_slice_2.log_files.is_empty());
    }

    #[tokio::test]
    async fn hudi_table_get_file_paths_for_simple_keygen_non_hive_style() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyle.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        assert_eq!(hudi_table.timeline.completed_commits.len(), 2);

        let partition_filters = &[];
        let actual = get_file_paths_with_filters(&hudi_table, partition_filters)
            .await
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();
        let expected = [
            "10/97de74b1-2a8e-4bb7-874c-0a74e1f42a77-0_0-119-166_20240418172804498.parquet",
            "20/76e0556b-390d-4249-b7ad-9059e2bc2cbd-0_0-98-141_20240418172802262.parquet",
            "30/6db57019-98ee-480e-8eb1-fb3de48e1c24-0_1-119-167_20240418172804498.parquet",
        ]
        .map(|f| join_url_segments(&base_url, &[f]).unwrap().to_string())
        .into_iter()
        .collect::<HashSet<_>>();
        assert_eq!(actual, expected);

        let filters = [("byteField", ">=", "10"), ("byteField", "<", "30")];
        let actual = get_file_paths_with_filters(&hudi_table, &filters)
            .await
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();
        let expected = [
            "10/97de74b1-2a8e-4bb7-874c-0a74e1f42a77-0_0-119-166_20240418172804498.parquet",
            "20/76e0556b-390d-4249-b7ad-9059e2bc2cbd-0_0-98-141_20240418172802262.parquet",
        ]
        .map(|f| join_url_segments(&base_url, &[f]).unwrap().to_string())
        .into_iter()
        .collect::<HashSet<_>>();
        assert_eq!(actual, expected);

        let actual = get_file_paths_with_filters(&hudi_table, &[("byteField", ">", "30")])
            .await
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();
        let expected = HashSet::new();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn hudi_table_get_file_paths_for_complex_keygen_hive_style() {
        let base_url = SampleTable::V6ComplexkeygenHivestyle.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        assert_eq!(hudi_table.timeline.completed_commits.len(), 2);

        let partition_filters = &[];
        let actual = get_file_paths_with_filters(&hudi_table, partition_filters)
            .await
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();
        let expected= [
            "byteField=10/shortField=300/a22e8257-e249-45e9-ba46-115bc85adcba-0_0-161-223_20240418173235694.parquet",
            "byteField=20/shortField=100/bb7c3a45-387f-490d-aab2-981c3f1a8ada-0_0-140-198_20240418173213674.parquet",
            "byteField=30/shortField=100/4668e35e-bff8-4be9-9ff2-e7fb17ecb1a7-0_1-161-224_20240418173235694.parquet",
        ]
            .map(|f| { join_url_segments(&base_url, &[f]).unwrap().to_string() })
            .into_iter()
            .collect::<HashSet<_>>();
        assert_eq!(actual, expected);

        let filters = [
            ("byteField", ">=", "10"),
            ("byteField", "<", "20"),
            ("shortField", "!=", "100"),
        ];
        let actual = get_file_paths_with_filters(&hudi_table, &filters)
            .await
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();
        let expected = [
            "byteField=10/shortField=300/a22e8257-e249-45e9-ba46-115bc85adcba-0_0-161-223_20240418173235694.parquet",
        ]
            .map(|f| { join_url_segments(&base_url, &[f]).unwrap().to_string() })
            .into_iter()
            .collect::<HashSet<_>>();
        assert_eq!(actual, expected);

        let filters = [("byteField", ">=", "20"), ("shortField", "=", "300")];
        let actual = get_file_paths_with_filters(&hudi_table, &filters)
            .await
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();
        let expected = HashSet::new();
        assert_eq!(actual, expected);
    }

    mod test_snapshot_and_time_travel_queries {
        use super::super::*;
        use arrow::compute::concat_batches;
        use hudi_test::{QuickstartTripsTable, SampleTable};

        #[tokio::test]
        async fn test_empty() -> Result<()> {
            for base_url in SampleTable::V6Empty.urls() {
                let hudi_table = Table::new(base_url.path()).await?;
                let records = hudi_table.read_snapshot(&[]).await?;
                assert!(records.is_empty());
            }
            Ok(())
        }

        #[tokio::test]
        async fn test_quickstart_trips_table() -> Result<()> {
            let base_url = QuickstartTripsTable::V6Trips8I1U.url_to_mor_avro();
            let hudi_table = Table::new(base_url.path()).await?;

            let updated_rider = "rider-D";

            // verify updated record as of the latest commit
            let records = hudi_table.read_snapshot(&[]).await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;
            let uuid_rider_and_fare = QuickstartTripsTable::uuid_rider_and_fare(&records)
                .into_iter()
                .filter(|(_, rider, _)| rider == updated_rider)
                .collect::<Vec<_>>();
            assert_eq!(uuid_rider_and_fare.len(), 1);
            assert_eq!(
                uuid_rider_and_fare[0].0,
                "9909a8b1-2d15-4d3d-8ec9-efc48c536a00"
            );
            assert_eq!(uuid_rider_and_fare[0].2, 25.0);

            // verify updated record as of the first commit
            let commit_timestamps = hudi_table
                .timeline
                .completed_commits
                .iter()
                .map(|i| i.timestamp.as_str())
                .collect::<Vec<_>>();
            let first_commit = commit_timestamps[0];
            let records = hudi_table.read_snapshot_as_of(first_commit, &[]).await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;
            let uuid_rider_and_fare = QuickstartTripsTable::uuid_rider_and_fare(&records)
                .into_iter()
                .filter(|(_, rider, _)| rider == updated_rider)
                .collect::<Vec<_>>();
            assert_eq!(uuid_rider_and_fare.len(), 1);
            assert_eq!(
                uuid_rider_and_fare[0].0,
                "9909a8b1-2d15-4d3d-8ec9-efc48c536a00"
            );
            assert_eq!(uuid_rider_and_fare[0].2, 33.9);

            Ok(())
        }

        #[tokio::test]
        async fn test_non_partitioned() -> Result<()> {
            for base_url in SampleTable::V6Nonpartitioned.urls() {
                let hudi_table = Table::new(base_url.path()).await?;
                let records = hudi_table.read_snapshot(&[]).await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;

                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![
                        (1, "Alice", false),
                        (2, "Bob", false),
                        (3, "Carol", true),
                        (4, "Diana", true),
                    ]
                );
            }
            Ok(())
        }

        #[tokio::test]
        async fn test_non_partitioned_read_optimized() -> Result<()> {
            let base_url = SampleTable::V6Nonpartitioned.url_to_mor_parquet();
            let hudi_table = Table::new_with_options(
                base_url.path(),
                [(HudiReadConfig::UseReadOptimizedMode.as_ref(), "true")],
            )
            .await?;
            let commit_timestamps = hudi_table
                .timeline
                .completed_commits
                .iter()
                .map(|i| i.timestamp.as_str())
                .collect::<Vec<_>>();
            let latest_commit = commit_timestamps.last().unwrap();
            let records = hudi_table.read_snapshot_as_of(latest_commit, &[]).await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;

            let sample_data = SampleTable::sample_data_order_by_id(&records);
            assert_eq!(
                sample_data,
                vec![
                    (1, "Alice", true), // this was updated to false in a log file and not to be read out
                    (2, "Bob", false),
                    (3, "Carol", true),
                    (4, "Diana", true), // this was inserted in a base file and should be read out
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_non_partitioned_rollback() -> Result<()> {
            let base_url = SampleTable::V6NonpartitionedRollback.url_to_mor_parquet();
            let hudi_table = Table::new(base_url.path()).await?;
            let records = hudi_table.read_snapshot(&[]).await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;

            let sample_data = SampleTable::sample_data_order_by_id(&records);
            assert_eq!(
                sample_data,
                vec![
                    (1, "Alice", true), // this was updated to false then rolled back to true
                    (2, "Bob", true),   // this was updated to true after rollback
                    (3, "Carol", true),
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_complex_keygen_hive_style_with_filters() -> Result<()> {
            for base_url in SampleTable::V6ComplexkeygenHivestyle.urls() {
                let hudi_table = Table::new(base_url.path()).await?;

                let filters = &[
                    ("byteField", ">=", "10"),
                    ("byteField", "<", "20"),
                    ("shortField", "!=", "100"),
                ];
                let records = hudi_table.read_snapshot(filters).await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;

                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(sample_data, vec![(1, "Alice", false), (3, "Carol", true),]);
            }
            Ok(())
        }

        #[tokio::test]
        async fn test_simple_keygen_nonhivestyle_time_travel() -> Result<()> {
            for base_url in SampleTable::V6SimplekeygenNonhivestyle.urls() {
                let hudi_table = Table::new(base_url.path()).await?;
                let commit_timestamps = hudi_table
                    .timeline
                    .completed_commits
                    .iter()
                    .map(|i| i.timestamp.as_str())
                    .collect::<Vec<_>>();
                let first_commit = commit_timestamps[0];
                let records = hudi_table.read_snapshot_as_of(first_commit, &[]).await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;

                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![(1, "Alice", true), (2, "Bob", false), (3, "Carol", true),]
                );
            }
            Ok(())
        }

        #[tokio::test]
        async fn test_simple_keygen_hivestyle_no_metafields() -> Result<()> {
            for base_url in SampleTable::V6SimplekeygenHivestyleNoMetafields.urls() {
                let hudi_table = Table::new(base_url.path()).await?;
                let records = hudi_table.read_snapshot(&[]).await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;

                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![
                        (1, "Alice", false),
                        (2, "Bob", false),
                        (3, "Carol", true),
                        (4, "Diana", true),
                    ]
                )
            }
            Ok(())
        }
    }

    mod test_incremental_queries {
        use super::super::*;
        use arrow_select::concat::concat_batches;
        use hudi_test::SampleTable;

        #[tokio::test]
        async fn test_empty() -> Result<()> {
            for base_url in SampleTable::V6Empty.urls() {
                let hudi_table = Table::new(base_url.path()).await?;
                let records = hudi_table.read_incremental_records("0", None).await?;
                assert!(records.is_empty())
            }
            Ok(())
        }

        #[tokio::test]
        async fn test_simplekeygen_nonhivestyle_overwritetable() -> Result<()> {
            for base_url in SampleTable::V6SimplekeygenNonhivestyleOverwritetable.urls() {
                let hudi_table = Table::new(base_url.path()).await?;
                let commit_timestamps = hudi_table
                    .timeline
                    .completed_commits
                    .iter()
                    .map(|i| i.timestamp.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(commit_timestamps.len(), 3);
                let first_commit = commit_timestamps[0];
                let second_commit = commit_timestamps[1];
                let third_commit = commit_timestamps[2];

                // read records changed from the beginning to the 1st commit
                let records = hudi_table
                    .read_incremental_records("19700101000000", Some(first_commit))
                    .await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;
                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![(1, "Alice", true), (2, "Bob", false), (3, "Carol", true),],
                    "Should return 3 records inserted in the 1st commit"
                );

                // read records changed from the 1st to the 2nd commit
                let records = hudi_table
                    .read_incremental_records(first_commit, Some(second_commit))
                    .await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;
                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![(1, "Alice", false), (4, "Diana", true),],
                    "Should return 2 records inserted or updated in the 2nd commit"
                );

                // read records changed from the 2nd to the 3rd commit
                let records = hudi_table
                    .read_incremental_records(second_commit, Some(third_commit))
                    .await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;
                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![(4, "Diana", false),],
                    "Should return 1 record insert-overwritten in the 3rd commit"
                );

                // read records changed from the 1st commit
                let records = hudi_table
                    .read_incremental_records(first_commit, None)
                    .await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;
                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![(4, "Diana", false),],
                    "Should return 1 record insert-overwritten in the 3rd commit"
                );

                // read records changed from the 3rd commit
                let records = hudi_table
                    .read_incremental_records(third_commit, None)
                    .await?;
                assert!(
                    records.is_empty(),
                    "Should return 0 record as it's the latest commit"
                );
            }
            Ok(())
        }
    }
}
