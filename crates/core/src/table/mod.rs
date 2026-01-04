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
mod read_options;
mod validation;

pub use read_options::ReadOptions;

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::read::HudiReadConfig;
use crate::config::table::HudiTableConfig::PartitionFields;
use crate::config::table::{HudiTableConfig, TableTypeValue};
use crate::expr::filter::Filter;
use crate::file_group::file_slice::FileSlice;
use crate::file_group::reader::FileGroupReader;
use crate::metadata::METADATA_TABLE_PARTITION_FIELD;
use crate::schema::resolver::{resolve_avro_schema, resolve_schema};
use crate::table::builder::TableBuilder;
use crate::table::fs_view::FileSystemView;
use crate::table::partition::PartitionPruner;
use crate::timeline::util::format_timestamp;
use crate::timeline::{EARLIEST_START_TIMESTAMP, Timeline};
use arrow::record_batch::RecordBatch;
use arrow_schema::{Field, Schema};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
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

    /// Get the latest [arrow_schema::Schema] of the table.
    ///
    /// The implementation looks for the schema in the following order:
    /// 1. Timeline commit metadata.
    /// 2. Base file schema.
    /// 3. `hoodie.properties` file's [HudiTableConfig::CreateSchema].
    pub async fn get_schema(&self) -> Result<Schema> {
        resolve_schema(self).await
    }

    /// Get the latest partition [arrow_schema::Schema] of the table.
    ///
    /// For metadata tables, returns a schema with a single `partition` field
    /// typed as [arrow_schema::DataType::Utf8], since metadata tables use a single partition
    /// column to identify partitions like "files", "column_stats", etc.
    ///
    /// For regular tables, returns the partition fields with their actual data types
    /// derived from the table schema.
    pub async fn get_partition_schema(&self) -> Result<Schema> {
        if self.is_metadata_table() {
            return Ok(Schema::new(vec![Field::new(
                METADATA_TABLE_PARTITION_FIELD,
                arrow_schema::DataType::Utf8,
                false,
            )]));
        }

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

    /// Get the [Timeline] of the table.
    pub fn get_timeline(&self) -> &Timeline {
        &self.timeline
    }

    /// Get all the [FileSlice]s in the table using [ReadOptions].
    ///
    /// This is the primary API for getting file slices. Use [ReadOptions] to configure:
    /// - `as_of_timestamp`: Time travel to a specific timestamp
    /// - `partition_filter`: Partition pruning filters
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hudi_core::table::{Table, ReadOptions};
    /// use hudi_core::expr::filter::col;
    ///
    /// // Get latest file slices
    /// let file_slices = table.get_file_slices(ReadOptions::new()).await?;
    ///
    /// // Get file slices with partition filter
    /// let options = ReadOptions::new()
    ///     .with_partition_filter(col("date").eq("2024-01-01"));
    /// let file_slices = table.get_file_slices(options).await?;
    ///
    /// // Time travel: get file slices as of a specific timestamp
    /// let options = ReadOptions::new()
    ///     .as_of("2024-01-01T12:00:00Z");
    /// let file_slices = table.get_file_slices(options).await?;
    /// ```
    pub async fn get_file_slices(&self, options: ReadOptions) -> Result<Vec<FileSlice>> {
        // Determine the timestamp: use as_of_timestamp from options, or latest
        let timestamp = if let Some(ref ts) = options.as_of_timestamp {
            format_timestamp(ts, &self.timezone())?
        } else if let Some(ts) = self.timeline.get_latest_commit_timestamp_as_option() {
            ts.to_string()
        } else {
            return Ok(Vec::new());
        };

        self.get_file_slices_internal(&timestamp, &options.partition_filter)
            .await
    }

    async fn get_file_slices_internal(
        &self,
        timestamp: &str,
        filters: &[Filter],
    ) -> Result<Vec<FileSlice>> {
        let timeline_view = self.timeline.create_view_as_of(timestamp).await?;

        let partition_schema = self.get_partition_schema().await?;
        let partition_pruner =
            PartitionPruner::new(filters, &partition_schema, self.hudi_configs.as_ref())?;

        // Try to create metadata table instance if enabled
        let metadata_table = if self.is_metadata_table_enabled() {
            log::debug!("Using metadata table for file listing");
            match self.new_metadata_table().await {
                Ok(mdt) => Some(mdt),
                Err(e) => {
                    log::warn!(
                        "Failed to create metadata table, falling back to storage listing: {e}"
                    );
                    None
                }
            }
        } else {
            None
        };

        self.file_system_view
            .get_file_slices(&partition_pruner, &timeline_view, metadata_table.as_ref())
            .await
    }

    /// Get all the changed [FileSlice]s for incremental reads using [ReadOptions].
    ///
    /// Use `from_timestamp` and `to_timestamp` in [ReadOptions] to specify the range.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hudi_core::table::{Table, ReadOptions};
    ///
    /// // Get file slices changed between two timestamps
    /// let options = ReadOptions::new()
    ///     .from_timestamp("20240101120000000")
    ///     .to_timestamp("20240102120000000");
    /// let file_slices = table.get_file_slices_incremental(options).await?;
    /// ```
    pub async fn get_file_slices_incremental(
        &self,
        options: ReadOptions,
    ) -> Result<Vec<FileSlice>> {
        let start = options
            .start_timestamp
            .as_deref()
            .unwrap_or(EARLIEST_START_TIMESTAMP);

        // If the end timestamp is not provided, use the latest file slice timestamp.
        let Some(end) = options
            .end_timestamp
            .as_deref()
            .or_else(|| self.timeline.get_latest_commit_timestamp_as_option())
        else {
            return Ok(Vec::new());
        };

        self.get_file_slices_incremental_internal(start, end).await
    }

    async fn get_file_slices_incremental_internal(
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

    /// Create a [FileGroupReader] using the [Table]'s Hudi configs.
    pub(crate) fn create_file_group_reader_with_options<I, K, V>(
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

    /// Create a [FileGroupReader] for this table.
    ///
    /// # Arguments
    /// * `options` - Read options for configuring the reader
    pub fn create_file_group_reader(&self, options: &ReadOptions) -> Result<FileGroupReader> {
        let mut read_configs: Vec<(HudiReadConfig, String)> = vec![];

        if let Some(ref ts) = options.as_of_timestamp {
            let formatted = format_timestamp(ts, &self.timezone())?;
            read_configs.push((HudiReadConfig::FileGroupEndTimestamp, formatted));
        } else if let Some(ts) = self.timeline.get_latest_commit_timestamp_as_option() {
            read_configs.push((HudiReadConfig::FileGroupEndTimestamp, ts.to_string()));
        }

        if let Some(ref ts) = options.start_timestamp {
            let formatted = format_timestamp(ts, &self.timezone())?;
            read_configs.push((HudiReadConfig::FileGroupStartTimestamp, formatted));
        }

        if let Some(batch_size) = options.batch_size {
            read_configs.push((HudiReadConfig::StreamBatchSize, batch_size.to_string()));
        }

        self.create_file_group_reader_with_options(read_configs)
    }

    /// Read records from the table as a stream using [ReadOptions].
    ///
    /// This is the primary read API. It returns a stream that yields record batches
    /// as they are read, enabling memory-efficient processing of large tables.
    ///
    /// For COW tables or read-optimized mode, each parquet file is streamed batch-by-batch.
    /// For MOR tables with log files, each file slice is read, merged, and yielded as a batch.
    ///
    /// # Arguments
    ///     * `options` - Read options for configuring the read operation:
    ///         - `as_of_timestamp`: If set, reads data as of this timestamp (time travel)
    ///         - `partition_filter`: Filters for partition pruning
    ///         - `projection`: Column names to project (select)
    ///         - `row_predicate`: Row-level filter predicate
    ///         - `batch_size`: Target rows per batch (effective for COW/read-optimized)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hudi_core::table::{Table, ReadOptions};
    /// use hudi_core::expr::filter::col;
    /// use futures::StreamExt;
    ///
    /// // Read latest snapshot
    /// let mut stream = table.read_snapshot(ReadOptions::new()).await?;
    ///
    /// // Read with partition filter and column projection
    /// let options = ReadOptions::new()
    ///     .with_partition_filter(col("date").eq("2024-01-01"))
    ///     .with_column(["id", "name", "value"])
    ///     .with_batch_size(4096);
    ///
    /// let mut stream = table.read_snapshot(options).await?;
    /// while let Some(result) = stream.next().await {
    ///     let batch = result?;
    ///     // Process batch...
    /// }
    ///
    /// // Time travel: read snapshot as of a specific timestamp
    /// let options = ReadOptions::new().as_of("2024-01-01T12:00:00Z");
    /// let mut stream = table.read_snapshot(options).await?;
    /// ```
    pub async fn read_snapshot(
        &self,
        options: ReadOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        // Determine the timestamp: use as_of_timestamp from options, or latest
        let timestamp = if let Some(ref ts) = options.as_of_timestamp {
            format_timestamp(ts, &self.timezone())?
        } else if let Some(ts) = self.timeline.get_latest_commit_timestamp_as_option() {
            ts.to_string()
        } else {
            return Ok(Box::pin(futures::stream::empty()));
        };

        self.read_snapshot_internal(&timestamp, options).await
    }

    async fn read_snapshot_internal(
        &self,
        timestamp: &str,
        options: ReadOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let file_slices = self
            .get_file_slices_internal(timestamp, &options.partition_filter)
            .await?;

        // Build the base read config options
        let mut read_configs: Vec<(HudiReadConfig, String)> =
            vec![(HudiReadConfig::FileGroupEndTimestamp, timestamp.to_string())];

        // Add batch size if specified
        if let Some(batch_size) = options.batch_size {
            read_configs.push((HudiReadConfig::StreamBatchSize, batch_size.to_string()));
        }

        let fg_reader = self.create_file_group_reader_with_options(read_configs)?;

        // Create a stream that yields batches from all file slices
        // Each file slice produces a 'static stream, which we flatten
        let stream = futures::stream::iter(file_slices)
            .then(move |file_slice| {
                let fg_reader = fg_reader.clone();
                let options = options.clone();
                async move {
                    fg_reader.read_file_slice(&file_slice, &options).await
                }
            })
            .try_flatten();

        Ok(Box::pin(stream))
    }

    /// Read incremental records from the table as a stream using [ReadOptions].
    ///
    /// Returns records that were inserted or updated in the given time range.
    /// Use `from_timestamp` and `to_timestamp` in [ReadOptions] to specify the range.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hudi_core::table::{Table, ReadOptions};
    /// use futures::StreamExt;
    ///
    /// let options = ReadOptions::new()
    ///     .from_timestamp("20240101120000000")
    ///     .to_timestamp("20240102120000000");
    ///
    /// let mut stream = table.read_incremental(options).await?;
    /// while let Some(result) = stream.next().await {
    ///     let batch = result?;
    ///     // Process batch...
    /// }
    /// ```
    pub async fn read_incremental(
        &self,
        options: ReadOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let timezone = self.timezone();

        let start_ts = if let Some(ref ts) = options.start_timestamp {
            format_timestamp(ts, &timezone)?
        } else {
            EARLIEST_START_TIMESTAMP.to_string()
        };

        let Some(end_ts) = options
            .end_timestamp
            .as_ref()
            .map(|ts| format_timestamp(ts, &timezone))
            .transpose()?
            .or_else(|| self.timeline.get_latest_commit_timestamp_as_option().map(String::from))
        else {
            return Ok(Box::pin(futures::stream::empty()));
        };

        // Get file slices for the incremental range
        let file_slices = self
            .get_file_slices_incremental_internal(&start_ts, &end_ts)
            .await?;

        // Build the base read config options with start and end timestamps
        let mut read_configs: Vec<(HudiReadConfig, String)> = vec![
            (HudiReadConfig::FileGroupStartTimestamp, start_ts),
            (HudiReadConfig::FileGroupEndTimestamp, end_ts),
        ];

        // Add batch size if specified
        if let Some(batch_size) = options.batch_size {
            read_configs.push((HudiReadConfig::StreamBatchSize, batch_size.to_string()));
        }

        let fg_reader = self.create_file_group_reader_with_options(read_configs)?;

        // Create a stream that yields batches from all file slices
        let stream = futures::stream::iter(file_slices)
            .then(move |file_slice| {
                let fg_reader = fg_reader.clone();
                let options = options.clone();
                async move {
                    fg_reader.read_file_slice(&file_slice, &options).await
                }
            })
            .try_flatten();

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HUDI_CONF_DIR;
    use crate::config::table::HudiTableConfig::{
        BaseFileFormat, Checksum, DatabaseName, DropsPartitionFields, IsHiveStylePartitioning,
        IsPartitionPathUrlencoded, KeyGeneratorClass, PartitionFields, PopulatesMetaFields,
        PrecombineField, RecordKeyFields, TableName, TableType, TableVersion,
        TimelineLayoutVersion, TimelineTimezone,
    };
    use crate::expr::filter::from_str_tuples;
    use crate::error::CoreError;
    use crate::metadata::meta_field::MetaField;
    use crate::storage::Storage;
    use crate::storage::util::join_url_segments;
    use hudi_test::{SampleTable, assert_arrow_field_names_eq, assert_avro_field_names_eq};
    use serial_test::serial;
    use std::collections::HashSet;
    use std::fs::canonicalize;
    use std::path::PathBuf;
    use std::{env, panic};

    /// Test helper to create a new `Table` instance without validating the configuration.
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
        let filter_vec = from_str_tuples(filters.to_vec())?;
        let options = ReadOptions::new().with_partition_filters(filter_vec);
        for f in table.get_file_slices(options).await? {
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
                "Storage option key '{key}' should start with a cloud storage prefix"
            );
            assert!(
                !value.is_empty(),
                "Storage option value for key '{key}' should not be empty"
            );
        }
    }

    #[tokio::test]
    async fn hudi_table_get_schema_from_empty_table_without_create_schema() {
        let table = get_test_table_without_validation("table_props_no_create_schema").await;

        let schema = table.get_schema().await;
        assert!(schema.is_err());
        assert!(matches!(schema.unwrap_err(), CoreError::SchemaNotFound(_)));

        let schema = table.get_avro_schema().await;
        assert!(schema.is_err());
        assert!(matches!(schema.unwrap_err(), CoreError::SchemaNotFound(_)));
    }

    #[tokio::test]
    async fn hudi_table_get_schema_from_empty_table_resolves_to_table_create_schema() {
        for base_url in SampleTable::V6Empty.urls() {
            let hudi_table = Table::new(base_url.path()).await.unwrap();

            // Validate the Arrow schema
            let schema = hudi_table.get_schema().await;
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            assert_arrow_field_names_eq!(
                schema,
                [MetaField::field_names(), vec!["id", "name", "isActive"]].concat()
            );

            // Validate the Avro schema
            let avro_schema = hudi_table.get_avro_schema().await;
            assert!(avro_schema.is_ok());
            let avro_schema = avro_schema.unwrap();
            assert_avro_field_names_eq!(&avro_schema, ["id", "name", "isActive"])
        }
    }

    #[tokio::test]
    async fn hudi_table_get_schema() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
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
        let arrow_schema = hudi_table.get_schema().await;
        assert!(arrow_schema.is_ok());
        let arrow_schema = arrow_schema.unwrap();
        assert_arrow_field_names_eq!(
            arrow_schema,
            [MetaField::field_names(), original_field_names.to_vec()].concat()
        );

        // Check Avro schema
        let avro_schema = hudi_table.get_avro_schema().await;
        assert!(avro_schema.is_ok());
        let avro_schema = avro_schema.unwrap();
        assert_avro_field_names_eq!(&avro_schema, original_field_names);
    }

    #[tokio::test]
    async fn hudi_table_get_partition_schema() {
        let base_url = SampleTable::V6TimebasedkeygenNonhivestyle.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let schema = hudi_table.get_partition_schema().await;
        assert!(schema.is_ok());
        let schema = schema.unwrap();
        assert_arrow_field_names_eq!(schema, ["ts_str"]);
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
        let actual: String = configs.get_or_default(BaseFileFormat).into();
        assert_eq!(actual, "parquet");
        assert!(panic::catch_unwind(|| configs.get_or_default(Checksum)).is_err());
        let actual: String = configs.get_or_default(DatabaseName).into();
        assert_eq!(actual, "default");
        let actual: bool = configs.get_or_default(DropsPartitionFields).into();
        assert!(!actual);
        let actual: bool = configs.get_or_default(IsHiveStylePartitioning).into();
        assert!(!actual);
        let actual: bool = configs.get_or_default(IsPartitionPathUrlencoded).into();
        assert!(!actual);
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

    #[tokio::test]
    async fn get_valid_table_props() {
        let table = get_test_table_without_validation("table_props_valid").await;
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

    #[tokio::test]
    #[serial(env_vars)]
    async fn get_global_table_props() {
        // Without the environment variable HUDI_CONF_DIR
        let table = get_test_table_without_validation("table_props_partial").await;
        let configs = table.hudi_configs;
        assert!(configs.get(DatabaseName).is_err());
        assert!(configs.get(TableType).is_err());
        let actual: String = configs.get(TableName).unwrap().into();
        assert_eq!(actual, "trips");

        // Environment variable HUDI_CONF_DIR points to nothing
        let base_path = env::current_dir().unwrap();
        let hudi_conf_dir = base_path.join("random/wrong/dir");
        unsafe {
            env::set_var(HUDI_CONF_DIR, hudi_conf_dir.as_os_str());
        }
        let table = get_test_table_without_validation("table_props_partial").await;
        let configs = table.hudi_configs;
        assert!(configs.get(DatabaseName).is_err());
        assert!(configs.get(TableType).is_err());
        let actual: String = configs.get(TableName).unwrap().into();
        assert_eq!(actual, "trips");

        // With global config
        let base_path = env::current_dir().unwrap();
        let hudi_conf_dir = base_path.join("tests/data/hudi_conf_dir");
        unsafe {
            env::set_var(HUDI_CONF_DIR, hudi_conf_dir.as_os_str());
        }
        let table = get_test_table_without_validation("table_props_partial").await;
        let configs = table.hudi_configs;
        let actual: String = configs.get(DatabaseName).unwrap().into();
        assert_eq!(actual, "tmpdb");
        let actual: String = configs.get(TableType).unwrap().into();
        assert_eq!(actual, "MERGE_ON_READ");
        let actual: String = configs.get(TableName).unwrap().into();
        assert_eq!(actual, "trips");
        unsafe {
            env::remove_var(HUDI_CONF_DIR);
        }
    }

    #[tokio::test]
    async fn hudi_table_read_file_slice() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let options = ReadOptions::new();
        let file_slices = hudi_table.get_file_slices(options).await.unwrap();
        let file_slice = file_slices.first().unwrap();
        let fg_reader = hudi_table.create_file_group_reader(&ReadOptions::new()).unwrap();
        let mut stream = fg_reader.read_file_slice(file_slice, &ReadOptions::new()).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 21);
    }

    #[tokio::test]
    async fn empty_hudi_table_get_file_slices() {
        let base_url = SampleTable::V6Empty.url_to_cow();

        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let file_slices = hudi_table
            .get_file_slices(ReadOptions::new())
            .await
            .unwrap();
        assert!(file_slices.is_empty());
    }

    #[tokio::test]
    async fn hudi_table_get_file_slices() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyle.url_to_cow();

        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let file_slices = hudi_table
            .get_file_slices(ReadOptions::new())
            .await
            .unwrap();
        assert_eq!(file_slices.len(), 3);
    }

    #[tokio::test]
    async fn hudi_table_get_file_slices_as_of_timestamps() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyleOverwritetable.url_to_mor_parquet();
        let hudi_table = Table::new(base_url.path()).await.unwrap();

        // before replacecommit (insert overwrite table)
        let second_latest_timestamp = "20250121000656060";
        let options = ReadOptions::new().as_of(second_latest_timestamp);
        let file_slices = hudi_table
            .get_file_slices(options)
            .await
            .unwrap();
        assert_eq!(file_slices.len(), 3);
        let file_slices_10 = file_slices
            .iter()
            .filter(|f| f.partition_path == "10")
            .collect::<Vec<_>>();
        assert_eq!(
            file_slices_10.len(),
            1,
            "Partition 10 should have 1 file slice"
        );
        let file_slice = file_slices_10[0];
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
        let options = ReadOptions::new().as_of(latest_timestamp);
        let file_slices = hudi_table
            .get_file_slices(options)
            .await
            .unwrap();
        assert_eq!(file_slices.len(), 1);
    }

    #[tokio::test]
    async fn hudi_table_get_file_slices_time_travel() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();

        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let file_slices = hudi_table
            .get_file_slices(ReadOptions::new())
            .await
            .unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path().unwrap())
                .collect::<Vec<_>>(),
            vec!["a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",]
        );

        // as of the latest timestamp
        let options = ReadOptions::new().as_of("20240418173551906");
        let file_slices = hudi_table
            .get_file_slices(options)
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
        let options = ReadOptions::new().as_of("20240418173551905");
        let file_slices = hudi_table
            .get_file_slices(options)
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
        let options = ReadOptions::new().as_of("19700101000000");
        let file_slices = hudi_table
            .get_file_slices(options)
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
    async fn empty_hudi_table_get_file_slices_incremental() {
        let base_url = SampleTable::V6Empty.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let options = ReadOptions::new().from_timestamp(EARLIEST_START_TIMESTAMP);
        let file_slices = hudi_table
            .get_file_slices_incremental(options)
            .await
            .unwrap();
        assert!(file_slices.is_empty())
    }

    #[tokio::test]
    async fn hudi_table_get_file_slices_incremental() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyleOverwritetable.url_to_mor_parquet();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let options = ReadOptions::new().to_timestamp("20250121000656060");
        let mut file_slices = hudi_table
            .get_file_slices_incremental(options)
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
}
