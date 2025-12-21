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
//! use hudi_core::config::util::empty_filters;
//! use hudi_core::table::Table;
//!
//! pub async fn test() {
//!     let base_uri = Url::from_file_path("/tmp/hudi_data").unwrap();
//!     let hudi_table = Table::new(base_uri.path()).await.unwrap();
//!     let record_read = hudi_table.read_snapshot(empty_filters()).await.unwrap();
//! }
//! ```
//! 4. get file slice
//!    Users can obtain metadata to customize reading methods, read in batches, perform parallel reads, and more.
//! ```rust
//! use url::Url;
//! use hudi_core::config::util::empty_filters;
//! use hudi_core::table::Table;
//! use hudi_core::storage::util::parse_uri;
//! use hudi_core::storage::util::join_url_segments;
//!
//! pub async fn test() {
//!     let base_uri = Url::from_file_path("/tmp/hudi_data").unwrap();
//!     let hudi_table = Table::new(base_uri.path()).await.unwrap();
//!     let file_slices = hudi_table
//!             .get_file_slices_splits(2, empty_filters())
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
mod validation;

use crate::config::read::HudiReadConfig;
use crate::config::table::HudiTableConfig::PartitionFields;
use crate::config::table::{HudiTableConfig, TableTypeValue};
use crate::config::HudiConfigs;
use crate::expr::filter::{from_str_tuples, Filter};
use crate::file_group::file_slice::FileSlice;
use crate::file_group::reader::FileGroupReader;
use crate::schema::resolver::{resolve_avro_schema, resolve_schema};
use crate::table::builder::TableBuilder;
use crate::table::fs_view::FileSystemView;
use crate::table::partition::PartitionPruner;
use crate::timeline::util::format_timestamp;
use crate::timeline::{Timeline, EARLIEST_START_TIMESTAMP};
use crate::util::collection::split_into_chunks;
use crate::Result;
use arrow::record_batch::RecordBatch;
use arrow_schema::{Field, Schema};
use std::collections::{HashMap, HashSet};
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

    /// Same as [Table::new], but blocking.
    pub fn new_blocking(base_uri: &str) -> Result<Self> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async { Table::new(base_uri).await })
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

    /// Same as [Table::new_with_options], but blocking.
    pub fn new_with_options_blocking<I, K, V>(base_uri: &str, options: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async { Table::new_with_options(base_uri, options).await })
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
            .into()
    }

    pub fn table_type(&self) -> String {
        let err_msg = format!("{:?} is missing or invalid.", HudiTableConfig::TableType);
        self.hudi_configs
            .get(HudiTableConfig::TableType)
            .expect(&err_msg)
            .into()
    }

    pub fn is_mor(&self) -> bool {
        self.table_type() == TableTypeValue::MergeOnRead.as_ref()
    }

    pub fn timezone(&self) -> String {
        self.hudi_configs
            .get_or_default(HudiTableConfig::TimelineTimezone)
            .into()
    }

    /// Get the latest Avro schema string of the table.
    ///
    /// The implementation looks for the schema in the following order:
    /// 1. Timeline commit metadata.
    /// 2. `hoodie.properties` file's [HudiTableConfig::CreateSchema].
    ///
    /// ### Note
    ///
    /// The schema returned does not contain Hudi's [MetaField]s,
    /// which is different from the one returned by [Table::get_schema].
    pub async fn get_avro_schema(&self) -> Result<String> {
        resolve_avro_schema(self).await
    }

    /// Same as [Table::get_avro_schema], but blocking.
    pub fn get_avro_schema_blocking(&self) -> Result<String> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async { self.get_avro_schema().await })
    }

    /// Get the latest [arrow_schema::Schema] of the table.
    ///
    /// The implementation looks for the schema in the following order:
    /// 1. Timeline commit metadata.
    /// 2. Base file schema.
    /// 3. `hoodie.properties` file's [HudiTableConfig::CreateSchema].
    pub async fn get_schema(&self) -> Result<Schema> {
        resolve_schema(self).await
    }

    /// Same as [Table::get_schema], but blocking.
    pub fn get_schema_blocking(&self) -> Result<Schema> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async { self.get_schema().await })
    }

    /// Get the latest partition [arrow_schema::Schema] of the table.
    pub async fn get_partition_schema(&self) -> Result<Schema> {
        let partition_fields: HashSet<String> = {
            let fields: Vec<String> = self.hudi_configs.get_or_default(PartitionFields).into();
            fields.into_iter().collect()
        };

        let schema = self.get_schema().await?;
        let partition_fields: Vec<Arc<Field>> = schema
            .fields()
            .iter()
            .filter(|field| partition_fields.contains(field.name()))
            .cloned()
            .collect();

        Ok(Schema::new(partition_fields))
    }

    /// Same as [Table::get_partition_schema], but blocking.
    pub fn get_partition_schema_blocking(&self) -> Result<Schema> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async { self.get_partition_schema().await })
    }

    /// Get the [Timeline] of the table.
    pub fn get_timeline(&self) -> &Timeline {
        &self.timeline
    }

    /// Get all the [FileSlice]s in splits from the table.
    ///
    /// # Arguments
    ///     * `num_splits` - The number of chunks to split the file slices into.
    ///     * `filters` - Partition filters to apply.
    pub async fn get_file_slices_splits<I, S>(
        &self,
        num_splits: usize,
        filters: I,
    ) -> Result<Vec<Vec<FileSlice>>>
    where
        I: IntoIterator<Item = (S, S, S)>,
        S: AsRef<str>,
    {
        if let Some(timestamp) = self.timeline.get_latest_commit_timestamp_as_option() {
            let filters = from_str_tuples(filters)?;
            self.get_file_slices_splits_internal(num_splits, timestamp, &filters)
                .await
        } else {
            Ok(Vec::new())
        }
    }

    /// Same as [Table::get_file_slices_splits], but blocking.
    pub fn get_file_slices_splits_blocking<I, S>(
        &self,
        num_splits: usize,
        filters: I,
    ) -> Result<Vec<Vec<FileSlice>>>
    where
        I: IntoIterator<Item = (S, S, S)>,
        S: AsRef<str>,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async { self.get_file_slices_splits(num_splits, filters).await })
    }

    /// Get all the [FileSlice]s in splits from the table at a given timestamp.
    ///
    /// # Arguments
    ///     * `num_splits` - The number of chunks to split the file slices into.
    ///     * `timestamp` - The timestamp which file slices associated with.
    ///     * `filters` - Partition filters to apply.
    pub async fn get_file_slices_splits_as_of<I, S>(
        &self,
        num_splits: usize,
        timestamp: &str,
        filters: I,
    ) -> Result<Vec<Vec<FileSlice>>>
    where
        I: IntoIterator<Item = (S, S, S)>,
        S: AsRef<str>,
    {
        let timestamp = format_timestamp(timestamp, &self.timezone())?;
        let filters = from_str_tuples(filters)?;
        self.get_file_slices_splits_internal(num_splits, &timestamp, &filters)
            .await
    }

    /// Same as [Table::get_file_slices_splits_as_of], but blocking.
    pub fn get_file_slices_splits_as_of_blocking<I, S>(
        &self,
        num_splits: usize,
        timestamp: &str,
        filters: I,
    ) -> Result<Vec<Vec<FileSlice>>>
    where
        I: IntoIterator<Item = (S, S, S)>,
        S: AsRef<str>,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                self.get_file_slices_splits_as_of(num_splits, timestamp, filters)
                    .await
            })
    }

    async fn get_file_slices_splits_internal(
        &self,
        num_splits: usize,
        timestamp: &str,
        filters: &[Filter],
    ) -> Result<Vec<Vec<FileSlice>>> {
        let file_slices = self.get_file_slices_internal(timestamp, filters).await?;
        Ok(split_into_chunks(file_slices, num_splits))
    }

    /// Get all the [FileSlice]s in the table.
    ///
    /// # Arguments
    ///     * `filters` - Partition filters to apply.
    ///
    /// # Notes
    ///     * This API is useful for implementing snapshot query.
    pub async fn get_file_slices<I, S>(&self, filters: I) -> Result<Vec<FileSlice>>
    where
        I: IntoIterator<Item = (S, S, S)>,
        S: AsRef<str>,
    {
        if let Some(timestamp) = self.timeline.get_latest_commit_timestamp_as_option() {
            let filters = from_str_tuples(filters)?;
            self.get_file_slices_internal(timestamp, &filters).await
        } else {
            Ok(Vec::new())
        }
    }

    /// Same as [Table::get_file_slices], but blocking.
    pub fn get_file_slices_blocking<I, S>(&self, filters: I) -> Result<Vec<FileSlice>>
    where
        I: IntoIterator<Item = (S, S, S)>,
        S: AsRef<str>,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async { self.get_file_slices(filters).await })
    }

    /// Get all the [FileSlice]s in the table at a given timestamp.
    ///
    /// # Arguments
    ///     * `timestamp` - The timestamp which file slices associated with.
    ///     * `filters` - Partition filters to apply.
    ///
    /// # Notes
    ///     * This API is useful for implementing time travel query.
    pub async fn get_file_slices_as_of<I, S>(
        &self,
        timestamp: &str,
        filters: I,
    ) -> Result<Vec<FileSlice>>
    where
        I: IntoIterator<Item = (S, S, S)>,
        S: AsRef<str>,
    {
        let timestamp = format_timestamp(timestamp, &self.timezone())?;
        let filters = from_str_tuples(filters)?;
        self.get_file_slices_internal(&timestamp, &filters).await
    }

    /// Same as [Table::get_file_slices_as_of], but blocking.
    pub fn get_file_slices_as_of_blocking<I, S>(
        &self,
        timestamp: &str,
        filters: I,
    ) -> Result<Vec<FileSlice>>
    where
        I: IntoIterator<Item = (S, S, S)>,
        S: AsRef<str>,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async { self.get_file_slices_as_of(timestamp, filters).await })
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
        let Some(end) =
            end_timestamp.or_else(|| self.timeline.get_latest_commit_timestamp_as_option())
        else {
            // No latest commit timestamp means the table is empty.
            return Ok(Vec::new());
        };

        let start = start_timestamp.unwrap_or(EARLIEST_START_TIMESTAMP);

        self.get_file_slices_between_internal(start, end).await
    }

    /// Same as [Table::get_file_slices_between], but blocking.
    pub fn get_file_slices_between_blocking(
        &self,
        start_timestamp: Option<&str>,
        end_timestamp: Option<&str>,
    ) -> Result<Vec<FileSlice>> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                self.get_file_slices_between(start_timestamp, end_timestamp)
                    .await
            })
    }

    /// Get all the changed [FileSlice]s in splits from the table between the given timestamps.
    ///
    /// # Arguments
    ///     * `num_splits` - The number of chunks to split the file slices into.
    ///     * `start_timestamp` - If provided, only file slices that were changed after this timestamp will be returned.
    ///     * `end_timestamp` - If provided, only file slices that were changed before or at this timestamp will be returned.
    ///
    /// # Notes
    ///     * This API is useful for implementing incremental query with read parallelism.
    ///     * Uses the same splitting flow as the time-travel API to respect read parallelism config.
    pub async fn get_file_slices_splits_between(
        &self,
        num_splits: usize,
        start_timestamp: Option<&str>,
        end_timestamp: Option<&str>,
    ) -> Result<Vec<Vec<FileSlice>>> {
        // If the end timestamp is not provided, use the latest commit timestamp.
        let Some(end) =
            end_timestamp.or_else(|| self.timeline.get_latest_commit_timestamp_as_option())
        else {
            // No latest commit timestamp means the table is empty.
            return Ok(Vec::new());
        };

        let start = start_timestamp.unwrap_or(EARLIEST_START_TIMESTAMP);

        self.get_file_slices_splits_between_internal(num_splits, start, end)
            .await
    }

    /// Same as [Table::get_file_slices_splits_between], but blocking.
    pub fn get_file_slices_splits_between_blocking(
        &self,
        num_splits: usize,
        start_timestamp: Option<&str>,
        end_timestamp: Option<&str>,
    ) -> Result<Vec<Vec<FileSlice>>> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                self.get_file_slices_splits_between(num_splits, start_timestamp, end_timestamp)
                    .await
            })
    }

    async fn get_file_slices_splits_between_internal(
        &self,
        num_splits: usize,
        start_timestamp: &str,
        end_timestamp: &str,
    ) -> Result<Vec<Vec<FileSlice>>> {
        let file_slices = self
            .get_file_slices_between_internal(start_timestamp, end_timestamp)
            .await?;
        Ok(split_into_chunks(file_slices, num_splits))
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
        FileGroupReader::new_with_configs_and_overwriting_options(
            self.hudi_configs.clone(),
            overwriting_options,
        )
    }

    /// Get all the latest records in the table.
    ///
    /// # Arguments
    ///     * `filters` - Partition filters to apply.
    pub async fn read_snapshot<I, S>(&self, filters: I) -> Result<Vec<RecordBatch>>
    where
        I: IntoIterator<Item = (S, S, S)>,
        S: AsRef<str>,
    {
        if let Some(timestamp) = self.timeline.get_latest_commit_timestamp_as_option() {
            let filters = from_str_tuples(filters)?;
            self.read_snapshot_internal(timestamp, &filters).await
        } else {
            Ok(Vec::new())
        }
    }

    /// Same as [Table::read_snapshot], but blocking.
    pub fn read_snapshot_blocking<I, S>(&self, filters: I) -> Result<Vec<RecordBatch>>
    where
        I: IntoIterator<Item = (S, S, S)>,
        S: AsRef<str>,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async { self.read_snapshot(filters).await })
    }

    /// Get all the records in the table at a given timestamp.
    ///
    /// # Arguments
    ///     * `timestamp` - The timestamp which records associated with.
    ///     * `filters` - Partition filters to apply.
    pub async fn read_snapshot_as_of<I, S>(
        &self,
        timestamp: &str,
        filters: I,
    ) -> Result<Vec<RecordBatch>>
    where
        I: IntoIterator<Item = (S, S, S)>,
        S: AsRef<str>,
    {
        let timestamp = format_timestamp(timestamp, &self.timezone())?;
        let filters = from_str_tuples(filters)?;
        self.read_snapshot_internal(&timestamp, &filters).await
    }

    /// Same as [Table::read_snapshot_as_of], but blocking.
    pub fn read_snapshot_as_of_blocking<I, S>(
        &self,
        timestamp: &str,
        filters: I,
    ) -> Result<Vec<RecordBatch>>
    where
        I: IntoIterator<Item = (S, S, S)>,
        S: AsRef<str>,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async { self.read_snapshot_as_of(timestamp, filters).await })
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
            end_timestamp.or_else(|| self.timeline.get_latest_commit_timestamp_as_option())
        else {
            return Ok(Vec::new());
        };

        let timezone = self.timezone();
        let start_timestamp = format_timestamp(start_timestamp, &timezone)?;
        let end_timestamp = format_timestamp(end_timestamp, &timezone)?;

        let file_slices = self
            .get_file_slices_between_internal(&start_timestamp, &end_timestamp)
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

    /// Same as [Table::read_incremental_records], but blocking.
    pub fn read_incremental_records_blocking(
        &self,
        start_timestamp: &str,
        end_timestamp: Option<&str>,
    ) -> Result<Vec<RecordBatch>> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                self.read_incremental_records(start_timestamp, end_timestamp)
                    .await
            })
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
    use crate::config::util::{empty_filters, empty_options};
    use crate::config::HUDI_CONF_DIR;
    use crate::error::CoreError;
    use crate::metadata::meta_field::MetaField;
    use crate::storage::util::join_url_segments;
    use crate::storage::Storage;
    use hudi_test::{assert_arrow_field_names_eq, assert_avro_field_names_eq, SampleTable};
    use std::collections::HashSet;
    use std::fs::canonicalize;
    use std::path::PathBuf;
    use std::{env, panic};

    /// Test helper to create a new `Table` instance without validating the configuration.
    ///
    /// # Arguments
    ///
    /// * `table_dir_name` - Name of the table root directory; all under `crates/core/tests/data/`.
    fn get_test_table_without_validation(table_dir_name: &str) -> Table {
        let base_url = Url::from_file_path(
            canonicalize(PathBuf::from("tests").join("data").join(table_dir_name)).unwrap(),
        )
        .unwrap();
        Table::new_with_options_blocking(
            base_url.as_str(),
            [("hoodie.internal.skip.config.validation", "true")],
        )
        .unwrap()
    }

    /// Test helper to get relative file paths from the table with filters.
    fn get_file_paths_with_filters(
        table: &Table,
        filters: &[(&str, &str, &str)],
    ) -> Result<Vec<String>> {
        let mut file_paths = Vec::new();
        let base_url = table.base_url();
        for f in table.get_file_slices_blocking(filters.to_vec())? {
            let relative_path = f.base_file_relative_path()?;
            let file_url = join_url_segments(&base_url, &[relative_path.as_str()])?;
            file_paths.push(file_url.to_string());
        }
        Ok(file_paths)
    }

    #[test]
    fn test_hudi_table_get_hudi_options() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        let hudi_options = hudi_table.hudi_options();
        for (k, v) in hudi_options.iter() {
            assert!(k.starts_with("hoodie."));
            assert!(!v.is_empty());
        }
    }

    #[test]
    fn test_hudi_table_get_storage_options() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();

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

    #[test]
    fn hudi_table_get_schema_from_empty_table_without_create_schema() {
        let table = get_test_table_without_validation("table_props_no_create_schema");

        let schema = table.get_schema_blocking();
        assert!(schema.is_err());
        assert!(matches!(schema.unwrap_err(), CoreError::SchemaNotFound(_)));

        let schema = table.get_avro_schema_blocking();
        assert!(schema.is_err());
        assert!(matches!(schema.unwrap_err(), CoreError::SchemaNotFound(_)));
    }

    #[test]
    fn hudi_table_get_schema_from_empty_table_resolves_to_table_create_schema() {
        for base_url in SampleTable::V6Empty.urls() {
            let hudi_table = Table::new_blocking(base_url.path()).unwrap();

            // Validate the Arrow schema
            let schema = hudi_table.get_schema_blocking();
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            assert_arrow_field_names_eq!(
                schema,
                [MetaField::field_names(), vec!["id", "name", "isActive"]].concat()
            );

            // Validate the Avro schema
            let avro_schema = hudi_table.get_avro_schema_blocking();
            assert!(avro_schema.is_ok());
            let avro_schema = avro_schema.unwrap();
            assert_avro_field_names_eq!(&avro_schema, ["id", "name", "isActive"])
        }
    }

    #[test]
    fn hudi_table_get_schema() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        let original_field_names = [
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
            "mapField",
            "structField",
        ];

        // Check Arrow schema
        let arrow_schema = hudi_table.get_schema_blocking();
        assert!(arrow_schema.is_ok());
        let arrow_schema = arrow_schema.unwrap();
        assert_arrow_field_names_eq!(
            arrow_schema,
            [MetaField::field_names(), original_field_names.to_vec()].concat()
        );

        // Check Avro schema
        let avro_schema = hudi_table.get_avro_schema_blocking();
        assert!(avro_schema.is_ok());
        let avro_schema = avro_schema.unwrap();
        assert_avro_field_names_eq!(&avro_schema, original_field_names);
    }

    #[test]
    fn hudi_table_get_partition_schema() {
        let base_url = SampleTable::V6TimebasedkeygenNonhivestyle.url_to_cow();
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        let schema = hudi_table.get_partition_schema_blocking();
        assert!(schema.is_ok());
        let schema = schema.unwrap();
        assert_arrow_field_names_eq!(schema, ["ts_str"]);
    }

    #[test]
    fn validate_invalid_table_props() {
        let table = get_test_table_without_validation("table_props_invalid");
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

    #[test]
    fn get_invalid_table_props() {
        let table = get_test_table_without_validation("table_props_invalid");
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

    #[test]
    fn get_default_for_invalid_table_props() {
        let table = get_test_table_without_validation("table_props_invalid");
        let configs = table.hudi_configs;
        let actual: String = configs.get_or_default(BaseFileFormat).into();
        assert_eq!(actual, "parquet");
        assert!(panic::catch_unwind(|| configs.get_or_default(Checksum)).is_err());
        let actual: String = configs.get_or_default(DatabaseName).into();
        assert_eq!(actual, "default");
        let actual: bool = configs.get_or_default(DropsPartitionFields).into();
        assert!(!actual);
        assert!(panic::catch_unwind(|| configs.get_or_default(IsHiveStylePartitioning)).is_err());
        assert!(panic::catch_unwind(|| configs.get_or_default(IsPartitionPathUrlencoded)).is_err());
        assert!(panic::catch_unwind(|| configs.get_or_default(KeyGeneratorClass)).is_err());
        let actual: Vec<String> = configs.get_or_default(PartitionFields).into();
        assert!(actual.is_empty());
        assert!(panic::catch_unwind(|| configs.get_or_default(PrecombineField)).is_err());
        let actual: bool = configs.get_or_default(PopulatesMetaFields).into();
        assert!(actual);
        assert!(panic::catch_unwind(|| configs.get_or_default(RecordKeyFields)).is_err());
        assert!(panic::catch_unwind(|| configs.get_or_default(TableName)).is_err());
        let actual: String = configs.get_or_default(TableType).into();
        assert_eq!(actual, "COPY_ON_WRITE");
        assert!(panic::catch_unwind(|| configs.get_or_default(TableVersion)).is_err());
        assert!(panic::catch_unwind(|| configs.get_or_default(TimelineLayoutVersion)).is_err());
        let actual: String = configs.get_or_default(TimelineTimezone).into();
        assert_eq!(actual, "utc");
    }

    #[test]
    fn get_valid_table_props() {
        let table = get_test_table_without_validation("table_props_valid");
        let configs = table.hudi_configs;
        let actual: String = configs.get(BaseFileFormat).unwrap().into();
        assert_eq!(actual, "parquet");
        let actual: isize = configs.get(Checksum).unwrap().into();
        assert_eq!(actual, 3761586722);
        let actual: String = configs.get(DatabaseName).unwrap().into();
        assert_eq!(actual, "db");
        let actual: bool = configs.get(DropsPartitionFields).unwrap().into();
        assert!(!actual);
        let actual: bool = configs.get(IsHiveStylePartitioning).unwrap().into();
        assert!(!actual);
        let actual: bool = configs.get(IsPartitionPathUrlencoded).unwrap().into();
        assert!(!actual);
        let actual: String = configs.get(KeyGeneratorClass).unwrap().into();
        assert_eq!(actual, "org.apache.hudi.keygen.SimpleKeyGenerator");
        let actual: Vec<String> = configs.get(PartitionFields).unwrap().into();
        assert_eq!(actual, vec!["city"]);
        let actual: String = configs.get(PrecombineField).unwrap().into();
        assert_eq!(actual, "ts");
        let actual: bool = configs.get(PopulatesMetaFields).unwrap().into();
        assert!(actual);
        let actual: Vec<String> = configs.get(RecordKeyFields).unwrap().into();
        assert_eq!(actual, vec!["uuid"]);
        let actual: String = configs.get(TableName).unwrap().into();
        assert_eq!(actual, "trips");
        let actual: String = configs.get(TableType).unwrap().into();
        assert_eq!(actual, "COPY_ON_WRITE");
        let actual: isize = configs.get(TableVersion).unwrap().into();
        assert_eq!(actual, 6);
        let actual: isize = configs.get(TimelineLayoutVersion).unwrap().into();
        assert_eq!(actual, 1);
        let actual: String = configs.get(TimelineTimezone).unwrap().into();
        assert_eq!(actual, "local");
    }

    #[test]
    fn get_global_table_props() {
        // Without the environment variable HUDI_CONF_DIR
        let table = get_test_table_without_validation("table_props_partial");
        let configs = table.hudi_configs;
        assert!(configs.get(DatabaseName).is_err());
        assert!(configs.get(TableType).is_err());
        let actual: String = configs.get(TableName).unwrap().into();
        assert_eq!(actual, "trips");

        // Environment variable HUDI_CONF_DIR points to nothing
        let base_path = env::current_dir().unwrap();
        let hudi_conf_dir = base_path.join("random/wrong/dir");
        env::set_var(HUDI_CONF_DIR, hudi_conf_dir.as_os_str());
        let table = get_test_table_without_validation("table_props_partial");
        let configs = table.hudi_configs;
        assert!(configs.get(DatabaseName).is_err());
        assert!(configs.get(TableType).is_err());
        let actual: String = configs.get(TableName).unwrap().into();
        assert_eq!(actual, "trips");

        // With global config
        let base_path = env::current_dir().unwrap();
        let hudi_conf_dir = base_path.join("tests/data/hudi_conf_dir");
        env::set_var(HUDI_CONF_DIR, hudi_conf_dir.as_os_str());
        let table = get_test_table_without_validation("table_props_partial");
        let configs = table.hudi_configs;
        let actual: String = configs.get(DatabaseName).unwrap().into();
        assert_eq!(actual, "tmpdb");
        let actual: String = configs.get(TableType).unwrap().into();
        assert_eq!(actual, "MERGE_ON_READ");
        let actual: String = configs.get(TableName).unwrap().into();
        assert_eq!(actual, "trips");
        env::remove_var(HUDI_CONF_DIR)
    }

    #[test]
    fn hudi_table_read_file_slice() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        let batches = hudi_table
            .create_file_group_reader_with_options(empty_options())
            .unwrap()
            .read_file_slice_by_base_file_path_blocking(
                "a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",
            )
            .unwrap();
        assert_eq!(batches.num_rows(), 4);
        assert_eq!(batches.num_columns(), 21);
    }

    #[test]
    fn empty_hudi_table_get_file_slices_splits() {
        let base_url = SampleTable::V6Empty.url_to_cow();

        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        let file_slices_splits = hudi_table
            .get_file_slices_splits_blocking(2, empty_filters())
            .unwrap();
        assert!(file_slices_splits.is_empty());
    }

    #[test]
    fn hudi_table_get_file_slices_splits() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyle.url_to_cow();

        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        let file_slices_splits = hudi_table
            .get_file_slices_splits_blocking(2, empty_filters())
            .unwrap();
        assert_eq!(file_slices_splits.len(), 2);
        assert_eq!(file_slices_splits[0].len(), 2);
        assert_eq!(file_slices_splits[1].len(), 1);
    }

    #[test]
    fn hudi_table_get_file_slices_splits_as_of_timestamps() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyleOverwritetable.url_to_mor_parquet();
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();

        // before replacecommit (insert overwrite table)
        let second_latest_timestamp = "20250121000656060";
        let file_slices_splits = hudi_table
            .get_file_slices_splits_as_of_blocking(2, second_latest_timestamp, empty_filters())
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
            .get_file_slices_splits_as_of_blocking(2, latest_timestamp, empty_filters())
            .unwrap();
        assert_eq!(file_slices_splits.len(), 1);
        assert_eq!(file_slices_splits[0].len(), 1);
    }

    #[test]
    fn hudi_table_get_file_slices_as_of_timestamps() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();

        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        let file_slices = hudi_table
            .get_file_slices_blocking(empty_filters())
            .unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path().unwrap())
                .collect::<Vec<_>>(),
            vec!["a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",]
        );

        // as of the latest timestamp
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        let file_slices = hudi_table
            .get_file_slices_as_of_blocking("20240418173551906", empty_filters())
            .unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path().unwrap())
                .collect::<Vec<_>>(),
            vec!["a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",]
        );

        // as of just smaller than the latest timestamp
        let hudi_table =
            Table::new_with_options_blocking(base_url.path(), empty_options()).unwrap();
        let file_slices = hudi_table
            .get_file_slices_as_of_blocking("20240418173551905", empty_filters())
            .unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path().unwrap())
                .collect::<Vec<_>>(),
            vec!["a079bdb3-731c-4894-b855-abfcd6921007-0_0-182-253_20240418173550988.parquet",]
        );

        // as of non-exist old timestamp
        let hudi_table =
            Table::new_with_options_blocking(base_url.path(), empty_options()).unwrap();
        let file_slices = hudi_table
            .get_file_slices_as_of_blocking("19700101000000", empty_filters())
            .unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path().unwrap())
                .collect::<Vec<_>>(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn empty_hudi_table_get_file_slices_between_timestamps() {
        let base_url = SampleTable::V6Empty.url_to_cow();
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        let file_slices = hudi_table
            .get_file_slices_between_blocking(Some(EARLIEST_START_TIMESTAMP), None)
            .unwrap();
        assert!(file_slices.is_empty())
    }

    #[test]
    fn hudi_table_get_file_slices_between_timestamps() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyleOverwritetable.url_to_mor_parquet();
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        let mut file_slices = hudi_table
            .get_file_slices_between_blocking(None, Some("20250121000656060"))
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

    #[test]
    fn empty_hudi_table_get_file_slices_splits_between() {
        let base_url = SampleTable::V6Empty.url_to_cow();
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        let file_slices_splits = hudi_table
            .get_file_slices_splits_between_blocking(2, Some(EARLIEST_START_TIMESTAMP), None)
            .unwrap();
        assert!(file_slices_splits.is_empty())
    }

    #[test]
    fn hudi_table_get_file_slices_splits_between() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyleOverwritetable.url_to_mor_parquet();
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        let file_slices_splits = hudi_table
            .get_file_slices_splits_between_blocking(2, None, Some("20250121000656060"))
            .unwrap();

        assert_eq!(file_slices_splits.len(), 2);
        let total_file_slices: usize = file_slices_splits.iter().map(|split| split.len()).sum();
        assert_eq!(total_file_slices, 3);
        assert_eq!(file_slices_splits[0].len(), 2);
        assert_eq!(file_slices_splits[1].len(), 1);
    }

    #[test]
    fn hudi_table_get_file_slices_splits_between_with_single_split() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyleOverwritetable.url_to_mor_parquet();
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        let file_slices_splits = hudi_table
            .get_file_slices_splits_between_blocking(1, None, Some("20250121000656060"))
            .unwrap();

        // Should have 1 split with all 3 file slices
        assert_eq!(file_slices_splits.len(), 1);
        assert_eq!(file_slices_splits[0].len(), 3);
    }

    #[test]
    fn hudi_table_get_file_slices_splits_between_with_many_splits() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyleOverwritetable.url_to_mor_parquet();
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        let file_slices_splits = hudi_table
            .get_file_slices_splits_between_blocking(10, None, Some("20250121000656060"))
            .unwrap();

        assert_eq!(file_slices_splits.len(), 3);
        for split in &file_slices_splits {
            assert_eq!(split.len(), 1);
        }
    }

    #[test]
    fn hudi_table_get_file_paths_for_simple_keygen_non_hive_style() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyle.url_to_cow();
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        assert_eq!(hudi_table.timeline.completed_commits.len(), 2);

        let partition_filters = &[];
        let actual = get_file_paths_with_filters(&hudi_table, partition_filters)
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
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();
        let expected = HashSet::new();
        assert_eq!(actual, expected);
    }

    #[test]
    fn hudi_table_get_file_paths_for_complex_keygen_hive_style() {
        let base_url = SampleTable::V6ComplexkeygenHivestyle.url_to_cow();
        let hudi_table = Table::new_blocking(base_url.path()).unwrap();
        assert_eq!(hudi_table.timeline.completed_commits.len(), 2);

        let partition_filters = &[];
        let actual = get_file_paths_with_filters(&hudi_table, partition_filters)
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
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();
        let expected = HashSet::new();
        assert_eq!(actual, expected);
    }
}
