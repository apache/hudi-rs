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
//! use hudi_core::table::{ReadOptions, Table};
//!
//! pub async fn test() {
//!     let base_uri = Url::from_file_path("/tmp/hudi_data").unwrap();
//!     let hudi_table = Table::new(base_uri.path()).await.unwrap();
//!     let record_read = hudi_table.read(&ReadOptions::new()).await.unwrap();
//! }
//! ```
//! 4. get file slice
//!    Users can obtain metadata to customize reading methods, read in batches, perform parallel reads, and more.
//! ```rust
//! use url::Url;
//! use hudi_core::table::{ReadOptions, Table};
//! use hudi_core::storage::util::parse_uri;
//! use hudi_core::storage::util::join_url_segments;
//!
//! pub async fn test() {
//!     let base_uri = Url::from_file_path("/tmp/hudi_data").unwrap();
//!     let hudi_table = Table::new(base_uri.path()).await.unwrap();
//!     let flat_slices = hudi_table
//!             .get_file_slices(&ReadOptions::new())
//!             .await.unwrap();
//!     let file_slices = hudi_core::util::collection::split_into_chunks(flat_slices, 2);
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
pub mod file_pruner;
pub(crate) mod fs_view;
mod listing;
pub mod partition;
mod read_options;
mod validation;

pub use read_options::{QueryType, ReadOptions};

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::read::HudiReadConfig;
use crate::config::table::HudiTableConfig::PartitionFields;
use crate::config::table::{BaseFileFormatValue, HudiTableConfig, TableTypeValue};
use crate::error::CoreError;
use crate::expr::filter::{Filter, validate_fields_against_schemas};
use crate::file_group::file_slice::FileSlice;
use crate::file_group::reader::FileGroupReader;
use crate::keygen::is_timestamp_based_keygen;
use crate::metadata::METADATA_TABLE_PARTITION_FIELD;
use crate::metadata::commit::HoodieCommitMetadata;
use crate::metadata::meta_field::MetaField;
use crate::schema::resolver::{
    resolve_avro_schema, resolve_avro_schema_with_meta_fields, resolve_data_schema, resolve_schema,
};
use crate::statistics::estimator::FileStatsEstimator;
use crate::table::builder::TableBuilder;
use crate::table::file_pruner::FilePruner;
use crate::table::fs_view::FileSystemView;
use crate::table::partition::{PartitionPruner, project_partition_schema};
use crate::timeline::util::format_timestamp;
use crate::timeline::{EARLIEST_START_TIMESTAMP, Timeline};
use arrow::record_batch::RecordBatch;
use arrow_schema::{Field, Schema};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OnceCell;
use url::Url;

/// The main struct that provides table APIs for interacting with a Hudi table.
#[derive(Debug)]
pub struct Table {
    pub hudi_configs: Arc<HudiConfigs>,
    pub storage_options: Arc<HashMap<String, String>>,
    pub timeline: Timeline,
    pub file_system_view: FileSystemView,
    /// Cached metadata table instance, lazily initialized on first use.
    /// Only populated when metadata table is enabled (v8+ with files partition).
    /// Shared across clones via `Arc` so all scan() calls reuse the same instance.
    cached_metadata_table: Arc<OnceCell<Table>>,
    /// Cached file stats estimator. Materialized on first successful call to
    /// [`Table::get_or_init_estimator`]. Failed or inapplicable attempts do not
    /// populate the cache, allowing later calls with newer timestamps to retry.
    cached_estimator: Arc<OnceCell<FileStatsEstimator>>,
}

impl Clone for Table {
    fn clone(&self) -> Self {
        Self {
            hudi_configs: self.hudi_configs.clone(),
            storage_options: self.storage_options.clone(),
            timeline: self.timeline.clone(),
            file_system_view: self.file_system_view.clone(),
            cached_metadata_table: self.cached_metadata_table.clone(),
            cached_estimator: self.cached_estimator.clone(),
        }
    }
}

impl Table {
    /// Get or initialize the cached metadata table instance.
    ///
    /// Returns `Ok(&Table)` if metadata table is successfully created or was already cached.
    /// The instance is created once and reused across all subsequent calls.
    pub(crate) async fn get_or_init_metadata_table(&self) -> Result<&Table> {
        self.cached_metadata_table
            .get_or_try_init(|| async {
                log::debug!("Initializing cached metadata table instance");
                self.new_metadata_table().await
            })
            .await
    }

    /// Get or initialize the cached `FileStatsEstimator` for this **data table**.
    ///
    /// This is the single point where eligibility for footer-based stats
    /// estimation is validated. Returns `None` when:
    /// - The base file format is not Parquet (e.g., HFile metadata tables).
    /// - No sample base-file path can be derived from commit metadata at or
    ///   before `sample_at_timestamp`.
    /// - The parquet footer read fails for the sample path.
    ///
    /// The cache is only materialized on successful initialization. This avoids
    /// pinning a "no sample yet" state from an early timestamp.
    ///
    /// Not intended for use on metadata tables. MDTs use HFile, which already
    /// short-circuits via the format check below; this is documented for the
    /// reader, not enforced via a separate flag.
    pub(crate) async fn get_or_init_estimator(
        &self,
        sample_at_timestamp: &str,
    ) -> Option<&FileStatsEstimator> {
        if let Some(estimator) = self.cached_estimator.get() {
            return Some(estimator);
        }

        // SINGLE point of parquet validation for the estimator concern.
        if !matches!(
            self.file_system_view.base_file_format(),
            BaseFileFormatValue::Parquet
        ) {
            return None;
        }

        let path = self
            .sample_base_file_path_at_or_before(sample_at_timestamp)
            .await?;

        self.cached_estimator
            .get_or_try_init(|| async {
                FileStatsEstimator::from_parquet_footer(&self.file_system_view.storage, &path).await
            })
            .await
            .map(Some)
            .unwrap_or_else(|e| {
                log::warn!(
                    "Failed to initialize file stats estimator from sample base file '{path}' \
                     (as-of timestamp: '{sample_at_timestamp}'): {e}"
                );
                None
            })
    }

    /// Sample one base file path active at or before the given timestamp.
    ///
    /// Walks completed commits ≤ `timestamp` in reverse (latest-first) and
    /// returns the first recorded base-file path. This handles MOR tables
    /// whose latest commit may be a delta commit with only log files —
    /// in that case we fall back to earlier commits that wrote base files.
    ///
    /// Format-agnostic by design: callers that require a specific format
    /// must validate before invoking. Returns `None` if no commit in range
    /// has a recorded base-file path.
    pub(crate) async fn sample_base_file_path_at_or_before(
        &self,
        timestamp: &str,
    ) -> Option<String> {
        let commits = self
            .timeline
            .get_completed_instants_at_or_before(timestamp)
            .ok()?;
        for commit in commits.iter().rev() {
            let Ok(metadata) = self.timeline.get_instant_metadata(commit).await else {
                continue;
            };
            let Ok(parsed) = HoodieCommitMetadata::from_json_map(&metadata) else {
                continue;
            };
            // Pick a stable sample path so estimator-derived stats are reproducible.
            // `partition_to_write_stats` is a HashMap, so raw iteration order is
            // non-deterministic across process runs.
            if let Some(path) = parsed.iter_base_file_paths().min() {
                return Some(path);
            }
        }
        None
    }

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

    /// Get the latest Avro schema string of the table, without Hudi meta fields (`_hoodie_*`).
    ///
    /// The implementation looks for the schema in the following order:
    /// 1. Timeline commit metadata.
    /// 2. `hoodie.properties` file's [HudiTableConfig::CreateSchema].
    pub async fn get_schema_in_avro_str(&self) -> Result<String> {
        self.get_schema_in_avro_str_inner(false).await
    }

    /// Get the latest Avro schema string of the table, with Hudi meta fields (`_hoodie_*`)
    /// prepended.
    pub async fn get_schema_in_avro_str_with_meta_fields(&self) -> Result<String> {
        self.get_schema_in_avro_str_inner(true).await
    }

    async fn get_schema_in_avro_str_inner(&self, includes_meta_fields: bool) -> Result<String> {
        if includes_meta_fields {
            resolve_avro_schema_with_meta_fields(self).await
        } else {
            resolve_avro_schema(self).await
        }
    }

    /// Get the latest [arrow_schema::Schema] of the table, without Hudi meta fields
    /// (`_hoodie_*`).
    ///
    /// The implementation looks for the schema in the following order:
    /// 1. Timeline commit metadata.
    /// 2. Base file schema.
    /// 3. `hoodie.properties` file's [HudiTableConfig::CreateSchema].
    pub async fn get_schema(&self) -> Result<Schema> {
        self.get_schema_inner(false).await
    }

    /// Get the latest [arrow_schema::Schema] of the table, with Hudi meta fields (`_hoodie_*`)
    /// prepended.
    pub async fn get_schema_with_meta_fields(&self) -> Result<Schema> {
        self.get_schema_inner(true).await
    }

    async fn get_schema_inner(&self, includes_meta_fields: bool) -> Result<Schema> {
        if includes_meta_fields {
            resolve_schema(self).await
        } else {
            resolve_data_schema(self).await
        }
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

        // Timestamp-based keygen: the source field is transformed into partition path
        // strings, so use a single _hoodie_partition_path field.
        if is_timestamp_based_keygen(&self.hudi_configs) {
            return Ok(Schema::new(vec![Field::new(
                MetaField::PartitionPath.as_ref(),
                arrow_schema::DataType::Utf8,
                false,
            )]));
        }

        let partition_field_names: Vec<String> =
            self.hudi_configs.get_or_default(PartitionFields).into();

        let schema = self.get_schema().await?;
        project_partition_schema(&schema, &partition_field_names)
    }

    /// Get the [Timeline] of the table.
    pub fn get_timeline(&self) -> &Timeline {
        &self.timeline
    }

    /// Get the [FileSlice]s the read targets, dispatching on `options.query_type`.
    ///
    /// - [`QueryType::Snapshot`]: returns slices visible at `options.as_of_timestamp`,
    ///   defaulting to the latest commit. `options.filters` drive both partition
    ///   pruning and file-level stats pruning (when min/max stats are available).
    /// - [`QueryType::Incremental`]: returns slices changed in
    ///   (`options.start_timestamp`, `options.end_timestamp`], defaulting to earliest
    ///   and latest respectively. `options.filters` drive partition pruning only;
    ///   data-column filters do not prune files at planning time.
    ///
    /// Returns an empty vector when the table has no commits.
    ///
    /// To bucket the result for parallel reads, use
    /// [`crate::util::collection::split_into_chunks`] or your engine's preferred
    /// partitioning policy.
    pub async fn get_file_slices(&self, options: &ReadOptions) -> Result<Vec<FileSlice>> {
        match options.query_type()? {
            QueryType::Snapshot => self.get_snapshot_file_slices(options).await,
            QueryType::Incremental => self.get_incremental_file_slices(options).await,
        }
    }

    async fn get_snapshot_file_slices(&self, options: &ReadOptions) -> Result<Vec<FileSlice>> {
        let Some(timestamp) = self.resolve_snapshot_timestamp(options)? else {
            return Ok(Vec::new());
        };
        self.get_file_slices_inner(&timestamp, &options.filters)
            .await
    }

    async fn get_incremental_file_slices(&self, options: &ReadOptions) -> Result<Vec<FileSlice>> {
        let Some((start, end)) = self.resolve_incremental_range(options)? else {
            return Ok(Vec::new());
        };
        self.get_file_slices_between_inner(&start, &end, &options.filters)
            .await
    }

    async fn get_file_slices_inner(
        &self,
        timestamp: &str,
        filters: &[Filter],
    ) -> Result<Vec<FileSlice>> {
        let timeline_view = self.timeline.create_view_as_of(timestamp).await?;

        let partition_schema = self.get_partition_schema().await?;
        // Validate against the meta-inclusive schema so filters on Hudi meta fields
        // (e.g. `_hoodie_record_key`) are accepted — those columns are present in
        // returned batches and the row-level mask applies them. The pruners are
        // tolerant of meta-field filters: PartitionPruner ignores non-partition
        // columns, and FilePruner skips columns without stats.
        let table_schema = self.get_schema_with_meta_fields().await?;
        validate_fields_against_schemas(filters, [&table_schema, &partition_schema])?;

        let partition_pruner =
            PartitionPruner::new(filters, &partition_schema, self.hudi_configs.as_ref())?;

        // Create file pruner with filters on non-partition columns
        let file_pruner = FilePruner::new(filters, &table_schema, &partition_schema)?;

        // Use cached metadata table instance if enabled
        let metadata_table = if self.is_metadata_table_enabled() {
            match self.get_or_init_metadata_table().await {
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

        // Estimator-backed metadata enrichment is used by both MDT-backed loading
        // and fallback storage listing.
        let estimator = self.get_or_init_estimator(timestamp).await;

        self.file_system_view
            .get_file_slices(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
                metadata_table,
                estimator,
            )
            .await
    }

    async fn get_file_slices_between_inner(
        &self,
        start_timestamp: &str,
        end_timestamp: &str,
        filters: &[Filter],
    ) -> Result<Vec<FileSlice>> {
        // Seed the cached estimator from a sample base file at or before
        // end_timestamp so the file group builder can populate FileMetadata
        // (size, byte_size, num_records) on each base file.
        let estimator = self.get_or_init_estimator(end_timestamp).await;

        let file_groups = self
            .timeline
            .get_file_groups_between(Some(start_timestamp), Some(end_timestamp), estimator)
            .await?;

        // Skip schema fetch and pruner construction when there are no filters.
        let partition_pruner = if filters.is_empty() {
            None
        } else {
            let partition_schema = self.get_partition_schema().await?;
            // See `get_file_slices_inner` for why validation uses the
            // meta-inclusive schema.
            let table_schema = self.get_schema_with_meta_fields().await?;
            validate_fields_against_schemas(filters, [&table_schema, &partition_schema])?;
            Some(PartitionPruner::new(
                filters,
                &partition_schema,
                self.hudi_configs.as_ref(),
            )?)
        };

        let mut file_slices: Vec<FileSlice> = Vec::new();
        for file_group in file_groups {
            if let Some(ref pruner) = partition_pruner
                && !pruner.should_include(&file_group.partition_path)
            {
                continue;
            }
            if let Some(file_slice) = file_group.get_file_slice_as_of(end_timestamp) {
                file_slices.push(file_slice.clone());
            }
        }

        Ok(file_slices)
    }

    /// Create a [FileGroupReader] using the [Table]'s Hudi configs.
    ///
    /// Two override channels keep Hudi configs and storage credentials cleanly
    /// separated — a `hoodie.*` key can't be misclassified as storage, and a
    /// stray storage option can't be silently picked up as a Hudi config.
    ///
    /// **Hudi configs** (last-writer-wins):
    /// 1. Table-level Hudi configs (constant for this `Table` instance).
    /// 2. `read_options.hudi_options` when `read_options` is `Some`. The four
    ///    `Table`-owned read keys ([`HudiReadConfig::QueryType`],
    ///    [`HudiReadConfig::AsOfTimestamp`], [`HudiReadConfig::StartTimestamp`],
    ///    [`HudiReadConfig::EndTimestamp`]) are stripped here: the `Table` layer
    ///    interprets them for dispatch and snapshot resolution, and a stale value
    ///    (e.g. an incremental `StartTimestamp` left in the bag) would silently
    ///    activate commit-time filtering at the FG reader.
    /// 3. `extra_hudi_overrides` — caller-supplied resolved Hudi configs;
    ///    always win. Use these to inject the resolved timestamps for the
    ///    current read.
    ///
    /// **Storage options** (last-writer-wins):
    /// 1. Table-level storage options (cloud credentials, endpoints, etc).
    /// 2. `extra_storage_overrides` — caller-supplied per-path storage overrides.
    pub fn create_file_group_reader_with_options<H, S, K1, V1, K2, V2>(
        &self,
        read_options: Option<&ReadOptions>,
        extra_hudi_overrides: H,
        extra_storage_overrides: S,
    ) -> Result<FileGroupReader>
    where
        H: IntoIterator<Item = (K1, V1)>,
        K1: AsRef<str>,
        V1: Into<String>,
        S: IntoIterator<Item = (K2, V2)>,
        K2: AsRef<str>,
        V2: Into<String>,
    {
        let mut hudi_opts: HashMap<String, String> = HashMap::new();
        if let Some(opts) = read_options {
            for (k, v) in &opts.hudi_options {
                if Self::TABLE_OWNED_READ_KEYS.contains(&k.as_str()) {
                    continue;
                }
                hudi_opts.insert(k.clone(), v.clone());
            }
        }
        for (k, v) in extra_hudi_overrides {
            hudi_opts.insert(k.as_ref().to_string(), v.into());
        }

        let mut storage_opts: HashMap<String, String> =
            HashMap::with_capacity(self.storage_options.len());
        for (k, v) in self.storage_options.iter() {
            storage_opts.insert(k.clone(), v.clone());
        }
        for (k, v) in extra_storage_overrides {
            storage_opts.insert(k.as_ref().to_string(), v.into());
        }

        FileGroupReader::new_with_overrides(self.hudi_configs.clone(), hudi_opts, storage_opts)
    }

    /// Read-option keys the `Table` layer interprets directly. These are
    /// excluded from the per-read overrides forwarded to the FG reader so a
    /// stale value can't change physical-read behavior — see
    /// [`Self::create_file_group_reader_with_options`].
    const TABLE_OWNED_READ_KEYS: [&'static str; 4] = [
        HudiReadConfig::QueryType.key_str(),
        HudiReadConfig::AsOfTimestamp.key_str(),
        HudiReadConfig::StartTimestamp.key_str(),
        HudiReadConfig::EndTimestamp.key_str(),
    ];

    /// Read records, dispatching on `options.query_type`.
    ///
    /// - [`QueryType::Snapshot`] reads at `options.as_of_timestamp` or the latest commit.
    /// - [`QueryType::Incremental`] reads the change range
    ///   (`options.start_timestamp`, `options.end_timestamp`].
    ///
    /// `options.filters` drive partition pruning, file-level stats pruning (snapshot
    /// only), and a row-level mask on every returned batch — see [`ReadOptions::filters`]
    /// for the full breakdown. `options.hudi_options` override table-level Hudi configs
    /// for this single read.
    pub async fn read(&self, options: &ReadOptions) -> Result<Vec<RecordBatch>> {
        match options.query_type()? {
            QueryType::Snapshot => self.read_snapshot_inner(options).await,
            QueryType::Incremental => self.read_incremental_inner(options).await,
        }
    }

    async fn read_snapshot_inner(&self, options: &ReadOptions) -> Result<Vec<RecordBatch>> {
        let Some(timestamp) = self.resolve_snapshot_timestamp(options)? else {
            return Ok(Vec::new());
        };
        let file_slices = self
            .get_file_slices_inner(&timestamp, &options.filters)
            .await?;
        let fg_reader = self.create_file_group_reader_for_snapshot(options, &timestamp)?;
        let fg_options = self.options_for_file_group(options);
        let batches = futures::future::try_join_all(
            file_slices
                .iter()
                .map(|f| fg_reader.read_file_slice(f, &fg_options)),
        )
        .await?;
        Ok(batches)
    }

    /// Build a `FileGroupReader` for a snapshot read with the resolved snapshot
    /// bound injected as `EndTimestamp`. The Table-owned read keys in
    /// `options.hudi_options` (notably any stray incremental `StartTimestamp`)
    /// are dropped by [`Self::create_file_group_reader_with_options`].
    fn create_file_group_reader_for_snapshot(
        &self,
        options: &ReadOptions,
        snapshot_timestamp: &str,
    ) -> Result<FileGroupReader> {
        self.create_file_group_reader_with_options(
            Some(options),
            [(HudiReadConfig::EndTimestamp, snapshot_timestamp.to_string())],
            std::iter::empty::<(&str, &str)>(),
        )
    }

    async fn read_incremental_inner(&self, options: &ReadOptions) -> Result<Vec<RecordBatch>> {
        let Some((start, end)) = self.resolve_incremental_range(options)? else {
            return Ok(Vec::new());
        };
        let file_slices = self
            .get_file_slices_between_inner(&start, &end, &options.filters)
            .await?;

        let fg_reader = self.create_file_group_reader_with_options(
            Some(options),
            [
                (HudiReadConfig::StartTimestamp, start),
                (HudiReadConfig::EndTimestamp, end),
            ],
            std::iter::empty::<(&str, &str)>(),
        )?;
        let fg_options = self.options_for_file_group(options);

        let batches = futures::future::try_join_all(
            file_slices
                .iter()
                .map(|f| fg_reader.read_file_slice(f, &fg_options)),
        )
        .await?;
        Ok(batches)
    }

    /// Build the [`ReadOptions`] passed to `FileGroupReader` for a per-slice read,
    /// stripping filters that target a partition column dropped from data files.
    ///
    /// `FileGroupReader` validates filter columns strictly against the read batch
    /// schema; when `hoodie.datasource.write.drop.partition.columns` is enabled,
    /// partition columns aren't in parquet, so a partition filter would surface as
    /// an error there. The partition pruner has already used those filters at
    /// table level, so dropping them here is safe and avoids the false-positive.
    fn options_for_file_group(&self, options: &ReadOptions) -> ReadOptions {
        let drops: bool = self
            .hudi_configs
            .get_or_default(HudiTableConfig::DropsPartitionFields)
            .into();
        if !drops || options.filters.is_empty() {
            return options.clone();
        }
        let partition_columns: Vec<String> = self
            .hudi_configs
            .get_or_default(HudiTableConfig::PartitionFields)
            .into();
        let mut applicable = options.clone();
        applicable.filters = options
            .filters
            .iter()
            .filter(|filter| !partition_columns.iter().any(|p| p == &filter.field))
            .cloned()
            .collect();
        applicable
    }

    /// Resolve the snapshot timestamp from `options`: explicit `as_of_timestamp` if set,
    /// otherwise the table's latest commit. Returns `None` only when the table has no
    /// commits and no explicit timestamp was given.
    fn resolve_snapshot_timestamp(&self, options: &ReadOptions) -> Result<Option<String>> {
        if let Some(ts) = options.as_of_timestamp() {
            return Ok(Some(format_timestamp(ts, &self.timezone())?));
        }
        Ok(self
            .timeline
            .get_latest_commit_timestamp_as_option()
            .map(|s| s.to_string()))
    }

    /// Resolve the incremental change range `(start, end]` from `options`. `start`
    /// defaults to [`EARLIEST_START_TIMESTAMP`]; `end` defaults to the latest commit.
    ///
    /// Returns `Ok(None)` only when no `end_timestamp` is provided AND the table has
    /// no commits. Invalid timestamp strings propagate as `Err`.
    fn resolve_incremental_range(&self, options: &ReadOptions) -> Result<Option<(String, String)>> {
        let timezone = self.timezone();
        let Some(end) = options
            .end_timestamp()
            .or_else(|| self.timeline.get_latest_commit_timestamp_as_option())
        else {
            return Ok(None);
        };
        let end = format_timestamp(end, &timezone)?;
        let start = options
            .start_timestamp()
            .unwrap_or(EARLIEST_START_TIMESTAMP);
        let start = format_timestamp(start, &timezone)?;
        Ok(Some((start, end)))
    }

    // =========================================================================
    // Streaming Read APIs
    // =========================================================================

    /// Streaming read; dispatches on `options.query_type`.
    ///
    /// Snapshot streams batches as they are read from each file slice. Incremental
    /// streaming is not yet supported and returns an `Unsupported` error.
    ///
    /// For MOR tables with log files, streaming falls back to a collect-and-merge that
    /// yields the merged result as a single batch.
    ///
    /// # Example
    /// ```ignore
    /// use futures::StreamExt;
    /// use hudi::table::ReadOptions;
    ///
    /// let options = ReadOptions::new()
    ///     .with_filters([("city", "=", "san_francisco")])?
    ///     .with_batch_size(4096)?;
    /// let mut stream = table.read_stream(&options).await?;
    /// while let Some(result) = stream.next().await {
    ///     println!("Read {} rows", result?.num_rows());
    /// }
    /// ```
    pub async fn read_stream(
        &self,
        options: &ReadOptions,
    ) -> Result<futures::stream::BoxStream<'static, Result<RecordBatch>>> {
        match options.query_type()? {
            QueryType::Snapshot => self.read_snapshot_stream_inner(options).await,
            QueryType::Incremental => Err(CoreError::Unsupported(
                "Streaming for incremental queries is not yet supported".to_string(),
            )),
        }
    }

    async fn read_snapshot_stream_inner(
        &self,
        options: &ReadOptions,
    ) -> Result<futures::stream::BoxStream<'static, Result<RecordBatch>>> {
        use futures::stream::{self, StreamExt};

        let Some(timestamp) = self.resolve_snapshot_timestamp(options)? else {
            return Ok(Box::pin(stream::empty()));
        };

        let file_slices = self
            .get_file_slices_inner(&timestamp, &options.filters)
            .await?;

        if file_slices.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        let fg_reader = self.create_file_group_reader_for_snapshot(options, &timestamp)?;

        // Extract per-batch options. Keep `filters` so they apply at row-level too —
        // the upstream pruning already used them at file/partition level; applying at
        // row-level closes the gap for non-partition column filters. Strip filters
        // on dropped partition columns so they don't trigger FGR validation errors.
        let fg_options_template = self.options_for_file_group(options);
        let projection = fg_options_template.projection.clone();
        let row_filters = fg_options_template.filters.clone();
        // Carry batch_size in hudi_options if set; everything else (timestamps,
        // query_type) is irrelevant to the per-slice FG-reader read.
        let mut per_slice_hudi_options: HashMap<String, String> = HashMap::new();
        if let Some(bs) = fg_options_template.batch_size()? {
            per_slice_hudi_options.insert(
                HudiReadConfig::StreamBatchSize.as_ref().to_string(),
                bs.to_string(),
            );
        }

        let streams_iter = file_slices.into_iter().map(move |file_slice| {
            let fg_reader = fg_reader.clone();
            let projection = projection.clone();
            let row_filters = row_filters.clone();
            let options = ReadOptions {
                filters: row_filters,
                projection,
                hudi_options: per_slice_hudi_options.clone(),
            };
            async move {
                fg_reader
                    .read_file_slice_stream(&file_slice, &options)
                    .await
            }
        });

        // Chain all file slice streams together, propagating errors to the caller.
        let combined_stream = stream::iter(streams_iter)
            .then(|fut| fut)
            .flat_map(|result| match result {
                Ok(file_stream) => file_stream.left_stream(),
                Err(e) => stream::once(async move { Err(e) }).right_stream(),
            });

        Ok(Box::pin(combined_stream))
    }

    /// Compute estimated table-level statistics from the metadata table for scan planning.
    ///
    /// Returns `(estimated_num_rows, estimated_total_byte_size)` where byte size is the
    /// estimated uncompressed in-memory size.
    ///
    /// The approach:
    /// 1. Read MDT files partition to get all active base files with on-disk sizes
    /// 2. For Parquet tables, read ONE sampled footer to derive a compression ratio
    /// 3. Infer total rows and byte size for all base files
    ///
    /// Only base files are counted (log files are excluded).
    ///
    /// TODO: support including log files in the estimation for MOR tables for scan planning.
    ///
    /// Returns `None` if metadata table is not enabled or if statistics cannot be computed.
    pub async fn compute_table_stats(&self) -> Option<(u64, u64)> {
        if !self.is_metadata_table_enabled() {
            return None;
        }

        // Step 1: Get MDT files partition records (cached on the MDT instance).
        let partition_schema = self.get_partition_schema().await.ok()?;
        let hudi_configs = self.hudi_configs.as_ref();
        let partition_pruner = PartitionPruner::new(&[], &partition_schema, hudi_configs).ok()?;
        let mdt = self.get_or_init_metadata_table().await.ok()?;
        let records = mdt
            .fetch_files_partition_records(&partition_pruner)
            .await
            .ok()?;

        // Only count base files (exclude log files which start with '.').
        let base_file_format = self.file_system_view.base_file_format();
        let base_file_suffix = format!(".{}", base_file_format.as_ref());
        let total_on_disk_size = records
            .values()
            .filter(|record| !record.is_all_partitions())
            .flat_map(|record| record.active_files_with_sizes())
            .filter(|(name, _)| name.ends_with(&base_file_suffix))
            .map(|(_, size)| size)
            .sum::<u64>();

        if total_on_disk_size == 0 {
            return None;
        }

        // Step 2: Use the cached estimator (seeded from the latest commit's sample).
        // Return None if estimator is not available (non-Parquet, no sample, or read failed).
        let latest_ts = self.timeline.get_latest_commit_timestamp().ok()?;
        let estimator = self.get_or_init_estimator(&latest_ts).await?;
        let (estimated_total_byte_size, estimated_total_rows) =
            estimator.estimate(total_on_disk_size);
        // Estimator math is non-negative by construction (u64 size * f64 ratio).
        // `i64 -> u64` via `max(0) as u64` defends against any future change.
        Some((
            estimated_total_rows.max(0) as u64,
            estimated_total_byte_size.max(0) as u64,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HUDI_CONF_DIR;
    use crate::config::internal::HudiInternalConfig;
    use crate::config::table::BaseFileFormatValue;
    use crate::config::table::HudiTableConfig::{
        BaseFileFormat, Checksum, DatabaseName, DropsPartitionFields, IsHiveStylePartitioning,
        IsPartitionPathUrlencoded, KeyGeneratorClass, PartitionFields, PopulatesMetaFields,
        PrecombineField, RecordKeyFields, TableName, TableType, TableVersion,
        TimelineLayoutVersion, TimelineTimezone,
    };
    use crate::config::util::empty_options;
    use crate::error::CoreError;
    use crate::metadata::meta_field::MetaField;
    use crate::storage::Storage;
    use crate::storage::util::join_url_segments;
    use crate::timeline::EARLIEST_START_TIMESTAMP;
    use hudi_test::{SampleTable, assert_arrow_field_names_eq, assert_avro_field_names_eq};
    use serial_test::serial;
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
        let options = ReadOptions::new().with_filters(filters.iter().copied())?;
        for f in table.get_file_slices(&options).await? {
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
    async fn test_hudi_table_storage_options_accessor_and_is_mor() {
        let cow_base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let cow_table = Table::new(cow_base_url.path()).await.unwrap();
        assert_eq!(
            cow_table.storage_options(),
            cow_table.storage_options.as_ref().clone()
        );
        assert!(!cow_table.is_mor());

        let mor_base_url =
            SampleTable::V6SimplekeygenNonhivestyleOverwritetable.url_to_mor_parquet();
        let mor_table = Table::new(mor_base_url.path()).await.unwrap();
        assert!(mor_table.is_mor());
    }

    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_hudi_table_register_storage() {
        use datafusion::execution::runtime_env::RuntimeEnv;
        use std::sync::Arc;

        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let runtime_env = Arc::new(RuntimeEnv::default());
        hudi_table.register_storage(runtime_env);
    }

    #[tokio::test]
    #[serial(env_vars)]
    async fn hudi_table_get_schema_from_empty_table_without_create_schema() {
        let table = get_test_table_without_validation("table_props_no_create_schema").await;

        let schema = table.get_schema().await;
        assert!(schema.is_err());
        assert!(matches!(schema.unwrap_err(), CoreError::SchemaNotFound(_)));

        let schema = table.get_schema_in_avro_str().await;
        assert!(schema.is_err());
        assert!(matches!(schema.unwrap_err(), CoreError::SchemaNotFound(_)));
    }

    #[tokio::test]
    async fn hudi_table_get_schema_from_empty_table_resolves_to_table_create_schema() {
        for base_url in SampleTable::V6Empty.urls() {
            let hudi_table = Table::new(base_url.path()).await.unwrap();

            // Validate the Arrow schema without meta fields
            let schema = hudi_table.get_schema().await;
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            assert_arrow_field_names_eq!(schema, ["id", "name", "isActive"]);

            // Validate the Arrow schema with meta fields
            let schema = hudi_table.get_schema_with_meta_fields().await;
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            assert_arrow_field_names_eq!(
                schema,
                [MetaField::field_names(), vec!["id", "name", "isActive"]].concat()
            );

            // Validate the Avro schema without meta fields
            let avro_schema = hudi_table.get_schema_in_avro_str().await;
            assert!(avro_schema.is_ok());
            let avro_schema = avro_schema.unwrap();
            assert_avro_field_names_eq!(&avro_schema, ["id", "name", "isActive"]);

            // Validate the Avro schema with meta fields
            let avro_schema = hudi_table.get_schema_in_avro_str_with_meta_fields().await;
            assert!(avro_schema.is_ok());
            let avro_schema = avro_schema.unwrap();
            assert_avro_field_names_eq!(
                &avro_schema,
                [
                    MetaField::field_names().as_slice(),
                    &["id", "name", "isActive"]
                ]
                .concat()
            );
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

        // Check Arrow schema without meta fields
        let arrow_schema = hudi_table.get_schema().await;
        assert!(arrow_schema.is_ok());
        let arrow_schema = arrow_schema.unwrap();
        assert_arrow_field_names_eq!(arrow_schema, original_field_names);

        // Check Arrow schema with meta fields
        let arrow_schema = hudi_table.get_schema_with_meta_fields().await;
        assert!(arrow_schema.is_ok());
        let arrow_schema = arrow_schema.unwrap();
        assert_arrow_field_names_eq!(
            arrow_schema,
            [MetaField::field_names(), original_field_names.to_vec()].concat()
        );

        // Check Avro schema without meta fields
        let avro_schema = hudi_table.get_schema_in_avro_str().await;
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
        assert_arrow_field_names_eq!(schema, [MetaField::PartitionPath.as_ref()]);
    }

    #[tokio::test]
    async fn hudi_table_get_partition_schema_uses_config_order_not_table_schema_order() {
        // V6ComplexkeygenHivestyle declares PARTITIONED BY (byteField, shortField).
        // The returned partition schema must follow the partition.fields config order,
        // which also matches the on-disk partition path order: byteField=.../shortField=...
        let base_url = SampleTable::V6ComplexkeygenHivestyle.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let schema = hudi_table.get_partition_schema().await.unwrap();
        assert_arrow_field_names_eq!(schema, ["byteField", "shortField"]);
    }

    #[tokio::test]
    #[serial(env_vars)]
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
    #[serial(env_vars)]
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
    #[serial(env_vars)]
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
    #[serial(env_vars)]
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
        let batches = hudi_table
            .create_file_group_reader_with_options(None, empty_options(), empty_options())
            .unwrap()
            .read_file_slice_from_paths(
                "a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",
                Vec::<&str>::new(),
                &ReadOptions::new(),
            )
            .await
            .unwrap();
        assert_eq!(batches.num_rows(), 4);
        assert_eq!(batches.num_columns(), 21);
    }

    #[tokio::test]
    async fn hudi_table_read_snapshot_and_as_of() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let latest_timestamp = hudi_table.timeline.get_latest_commit_timestamp().unwrap();

        let snapshot_batches = hudi_table.read(&ReadOptions::new()).await.unwrap();
        assert!(!snapshot_batches.is_empty());
        let snapshot_rows = snapshot_batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>();
        assert!(snapshot_rows > 0);

        let as_of_batches = hudi_table
            .read(&ReadOptions::new().with_as_of_timestamp(&latest_timestamp))
            .await
            .unwrap();
        assert!(!as_of_batches.is_empty());
    }

    #[tokio::test]
    async fn empty_hudi_table_read_apis_return_empty() {
        use futures::StreamExt;

        let base_url = SampleTable::V6Empty.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();

        let snapshot_batches = hudi_table.read(&ReadOptions::new()).await.unwrap();
        assert!(snapshot_batches.is_empty());

        let incremental_batches = hudi_table
            .read(
                &ReadOptions::new()
                    .with_query_type(QueryType::Incremental)
                    .with_start_timestamp(EARLIEST_START_TIMESTAMP),
            )
            .await
            .unwrap();
        assert!(incremental_batches.is_empty());

        let mut snapshot_stream = hudi_table.read_stream(&ReadOptions::new()).await.unwrap();
        assert!(snapshot_stream.next().await.is_none());
    }

    #[tokio::test]
    async fn hudi_table_read_snapshot_stream_returns_batches_with_options() -> Result<()> {
        use futures::TryStreamExt;

        let base_url = SampleTable::V6SimplekeygenNonhivestyle.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let options = ReadOptions::new()
            .with_filters([("byteField", ">=", "10")])?
            .with_projection(["id"])
            .with_batch_size(2)?;

        let stream = hudi_table.read_stream(&options).await.unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert!(!batches.is_empty());
        assert!(batches[0].column_by_name("id").is_some());
        Ok(())
    }

    #[tokio::test]
    async fn hudi_table_read_snapshot_stream_returns_empty_when_no_file_slices_match_filters()
    -> Result<()> {
        use futures::StreamExt;

        let base_url = SampleTable::V6SimplekeygenNonhivestyle.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let options = ReadOptions::new().with_filters([("byteField", "=", "999")])?;

        let mut stream = hudi_table.read_stream(&options).await.unwrap();
        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn hudi_table_read_with_incremental_query_type() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let batches = hudi_table
            .read(
                &ReadOptions::new()
                    .with_query_type(QueryType::Incremental)
                    .with_start_timestamp(EARLIEST_START_TIMESTAMP),
            )
            .await
            .unwrap();
        assert!(!batches.is_empty());
    }

    #[tokio::test]
    async fn hudi_table_read_dispatches_on_query_type() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();

        // Default: Snapshot.
        let snapshot = hudi_table.read(&ReadOptions::new()).await.unwrap();
        assert!(!snapshot.is_empty());

        // Explicit Incremental returns the same shape as the dedicated shortcut.
        let incremental = hudi_table
            .read(
                &ReadOptions::new()
                    .with_query_type(QueryType::Incremental)
                    .with_start_timestamp(EARLIEST_START_TIMESTAMP),
            )
            .await
            .unwrap();
        assert!(!incremental.is_empty());

        // Shortcuts override query_type set on the input options.
        let forced_snapshot = hudi_table
            .read(&ReadOptions::new().with_query_type(QueryType::Incremental))
            .await
            .unwrap();
        assert!(!forced_snapshot.is_empty());
    }

    #[tokio::test]
    async fn hudi_table_read_stream_errors_on_incremental() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let result = hudi_table
            .read_stream(&ReadOptions::new().with_query_type(QueryType::Incremental))
            .await;
        match result {
            Ok(_) => panic!("incremental streaming must error"),
            Err(e) => {
                assert!(matches!(e, CoreError::Unsupported(_)));
                assert!(e.to_string().contains("not yet supported"));
            }
        }
    }

    #[tokio::test]
    async fn hudi_table_get_file_slices_dispatches_on_query_type() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();

        let snapshot_slices = hudi_table
            .get_file_slices(&ReadOptions::new())
            .await
            .unwrap();
        let incremental_slices = hudi_table
            .get_file_slices(
                &ReadOptions::new()
                    .with_query_type(QueryType::Incremental)
                    .with_start_timestamp(EARLIEST_START_TIMESTAMP),
            )
            .await
            .unwrap();
        assert!(!snapshot_slices.is_empty());
        assert!(!incremental_slices.is_empty());
    }

    #[tokio::test]
    async fn read_with_invalid_as_of_timestamp_errors() {
        // `as_of_timestamp` is parsed via `format_timestamp` before any IO. A
        // malformed value must surface as `TimestampParsingError` rather than
        // silently being treated as "latest" or sliced into a no-op result.
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let options = ReadOptions::new().with_as_of_timestamp("not-a-timestamp");

        let snapshot_err = hudi_table.read(&options).await.unwrap_err();
        assert!(
            matches!(snapshot_err, CoreError::TimestampParsingError(_)),
            "expected TimestampParsingError, got: {snapshot_err}"
        );

        let stream_result = hudi_table.read_stream(&options).await;
        match stream_result {
            Ok(_) => panic!("read_stream must propagate the parse error synchronously"),
            Err(e) => assert!(
                matches!(e, CoreError::TimestampParsingError(_)),
                "expected TimestampParsingError on stream path, got: {e}"
            ),
        }
    }

    #[tokio::test]
    async fn hudi_table_read_options_hudi_options_plumbed_to_reader() {
        // Per-read hudi_options should override table-level configs without
        // mutating the table. Here we set the per-read StartTimestamp
        // via hudi_options on a snapshot read; the read should still succeed.
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let options =
            ReadOptions::new().with_hudi_option(HudiReadConfig::StartTimestamp.as_ref(), "0");
        let batches = hudi_table.read(&options).await.unwrap();
        assert!(!batches.is_empty());
    }

    #[tokio::test]
    async fn hudi_table_get_file_slices_as_of_replacecommit() {
        // Insert-overwrite-table replacecommit: as_of before vs at the replacecommit
        // returns different slice sets. Splitter behavior is covered by
        // util::collection::split_into_chunks tests.
        let base_url = SampleTable::V6SimplekeygenNonhivestyleOverwritetable.url_to_mor_parquet();
        let hudi_table = Table::new(base_url.path()).await.unwrap();

        // before replacecommit
        let second_latest_timestamp = "20250121000656060";
        let file_slices = hudi_table
            .get_file_slices(&ReadOptions::new().with_as_of_timestamp(second_latest_timestamp))
            .await
            .unwrap();
        assert_eq!(file_slices.len(), 3);
        let p10: Vec<_> = file_slices
            .iter()
            .filter(|f| f.partition_path == "10")
            .collect();
        assert_eq!(p10.len(), 1, "Partition 10 should have 1 file slice");
        let file_slice = p10[0];
        assert_eq!(
            file_slice.base_file.file_name(),
            "92e64357-e4d1-4639-a9d3-c3535829d0aa-0_1-53-79_20250121000647668.parquet"
        );
        assert_eq!(file_slice.log_files.len(), 1);
        assert_eq!(
            file_slice.log_files.iter().next().unwrap().file_name(),
            ".92e64357-e4d1-4639-a9d3-c3535829d0aa-0_20250121000647668.log.1_0-73-101"
        );

        // as of replacecommit
        let latest_timestamp = "20250121000702475";
        let file_slices = hudi_table
            .get_file_slices(&ReadOptions::new().with_as_of_timestamp(latest_timestamp))
            .await
            .unwrap();
        assert_eq!(file_slices.len(), 1);
    }

    #[tokio::test]
    async fn hudi_table_get_file_slices_as_of_timestamps() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();

        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let file_slices = hudi_table
            .get_file_slices(&ReadOptions::new())
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
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let file_slices = hudi_table
            .get_file_slices(&ReadOptions::new().with_as_of_timestamp("20240418173551906"))
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
            .get_file_slices(&ReadOptions::new().with_as_of_timestamp("20240418173551905"))
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
            .get_file_slices(&ReadOptions::new().with_as_of_timestamp("19700101000000"))
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
        let file_slices = hudi_table
            .get_file_slices(
                &ReadOptions::new()
                    .with_query_type(QueryType::Incremental)
                    .with_start_timestamp(EARLIEST_START_TIMESTAMP),
            )
            .await
            .unwrap();
        assert!(file_slices.is_empty())
    }

    #[tokio::test]
    async fn hudi_table_get_file_slices_incremental() {
        let base_url = SampleTable::V6SimplekeygenNonhivestyleOverwritetable.url_to_mor_parquet();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let mut file_slices = hudi_table
            .get_file_slices(
                &ReadOptions::new()
                    .with_query_type(QueryType::Incremental)
                    .with_end_timestamp("20250121000656060"),
            )
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

        // FileMetadata is populated for incremental queries (issue #401).
        // size comes from HoodieWriteStat.fileSizeInBytes; byte_size and num_records
        // are estimated from the cached FileStatsEstimator (seeded from a sample
        // base file in commit metadata at or before end_timestamp).
        let m0 = file_slice_0.base_file.file_metadata.as_ref().unwrap();
        assert_eq!(m0.size, 440878);
        assert_eq!(m0.byte_size, 326703);
        assert_eq!(m0.num_records, 458);

        let m1 = file_slice_1.base_file.file_metadata.as_ref().unwrap();
        assert_eq!(m1.size, 440616);
        assert_eq!(m1.byte_size, 326509);
        assert_eq!(m1.num_records, 458);

        let m2 = file_slice_2.base_file.file_metadata.as_ref().unwrap();
        assert_eq!(m2.size, 440638);
        assert_eq!(m2.byte_size, 326525);
        assert_eq!(m2.num_records, 458);
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

    #[tokio::test]
    async fn test_get_or_init_estimator_returns_none_for_non_parquet_format() {
        let base_url = SampleTable::V9TxnsSimpleMeta.url_to_cow();
        let table = Table::new_with_options(
            base_url.path(),
            [
                (BaseFileFormat.as_ref(), BaseFileFormatValue::HFile.as_ref()),
                (HudiInternalConfig::SkipConfigValidation.as_ref(), "true"),
            ],
        )
        .await
        .unwrap();
        let latest_ts = table.timeline.get_latest_commit_timestamp().unwrap();
        assert!(table.get_or_init_estimator(&latest_ts).await.is_none());
    }

    #[tokio::test]
    async fn test_get_or_init_estimator_retries_after_early_timestamp_without_sample() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let table = Table::new(base_url.path()).await.unwrap();

        // No completed commits at or before this timestamp, so no sample file can be found.
        let early_ts = "19700101000000";
        assert!(table.get_or_init_estimator(early_ts).await.is_none());

        // A later request should still be able to initialize and cache the estimator,
        // and the subsequent early-timestamp call must hit the same cached instance.
        let latest_ts = table.timeline.get_latest_commit_timestamp().unwrap();
        let initialized = table.get_or_init_estimator(&latest_ts).await.unwrap();
        let cached = table.get_or_init_estimator(early_ts).await.unwrap();
        assert!(std::ptr::eq(initialized, cached));
    }

    #[tokio::test]
    async fn test_compute_table_stats_with_mdt() {
        use hudi_test::QuickstartTripsTable;
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let table = Table::new(&table_path).await.unwrap();
        assert!(table.is_metadata_table_enabled());

        let stats = table.compute_table_stats().await;
        assert!(
            stats.is_some(),
            "Stats should be Some for MDT-enabled table"
        );
        let (rows, bytes) = stats.unwrap();
        assert!(rows > 0, "Should have estimated rows > 0, got {rows}");
        assert!(bytes > 0, "Should have estimated bytes > 0, got {bytes}");
    }

    #[tokio::test]
    async fn test_compute_table_stats_with_sample_mdt_table() {
        let base_url = SampleTable::V9TxnsSimpleMeta.url_to_cow();
        let table = Table::new(base_url.path()).await.unwrap();
        assert!(table.is_metadata_table_enabled());

        let stats = table.compute_table_stats().await;
        assert!(stats.is_some(), "Stats should be Some for sample MDT table");
        let (rows, bytes) = stats.unwrap();
        assert!(rows > 0);
        assert!(bytes > 0);
    }

    #[tokio::test]
    async fn test_compute_table_stats_returns_none_when_base_file_extension_does_not_match() {
        let base_url = SampleTable::V9TxnsSimpleMeta.url_to_cow();
        let table = Table::new_with_options(
            base_url.path(),
            [
                (BaseFileFormat.as_ref(), BaseFileFormatValue::HFile.as_ref()),
                (HudiInternalConfig::SkipConfigValidation.as_ref(), "true"),
            ],
        )
        .await
        .unwrap();
        assert!(table.is_metadata_table_enabled());
        assert!(table.compute_table_stats().await.is_none());
    }

    #[tokio::test]
    async fn test_compute_table_stats_without_mdt() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let table = Table::new(base_url.path()).await.unwrap();
        assert!(!table.is_metadata_table_enabled());

        let stats = table.compute_table_stats().await;
        assert!(stats.is_none(), "Stats should be None for non-MDT table");
    }

    #[tokio::test]
    async fn test_clone_table_with_mdt() {
        let base_url = SampleTable::V9TxnsNonpartMeta.url_to_mor_avro();
        let table = Table::new(base_url.path()).await.unwrap();
        assert!(table.is_metadata_table_enabled());

        let cloned = table.clone();
        assert_eq!(cloned.table_name(), table.table_name());
        assert_eq!(cloned.table_type(), table.table_type());

        // Clone shares the cached metadata table via Arc<OnceCell>
        let file_slices = cloned.get_file_slices(&ReadOptions::new()).await.unwrap();
        assert!(!file_slices.is_empty());

        // compute_table_stats works on cloned table
        let stats = cloned.compute_table_stats().await;
        assert!(stats.is_some());
        let (rows, bytes) = stats.unwrap();
        assert!(rows > 0);
        assert!(bytes > 0);
    }

    #[tokio::test]
    async fn test_get_file_slices_falls_back_to_storage_when_metadata_table_init_fails() {
        let base_url = SampleTable::V9TxnsSimpleNometa.url_to_cow();
        let table = Table::new_with_options(base_url.path(), [("hoodie.metadata.enable", "true")])
            .await
            .unwrap();
        assert!(table.is_metadata_table_enabled());

        let file_slices = table.get_file_slices(&ReadOptions::new()).await.unwrap();
        assert!(!file_slices.is_empty());
    }

    #[tokio::test]
    async fn test_get_file_slices_with_mdt() {
        let base_url = SampleTable::V9TxnsSimpleMeta.url_to_cow();
        let table = Table::new(base_url.path()).await.unwrap();
        assert!(table.is_metadata_table_enabled());

        // This exercises the MDT code path in get_file_slices_inner:
        // metadata table init, fetch_files_partition_records, and
        // fs_view's load_file_groups with estimator
        let file_slices = table.get_file_slices(&ReadOptions::new()).await.unwrap();
        assert!(!file_slices.is_empty());

        // Verify file metadata is populated from MDT with estimated stats
        for fsl in &file_slices {
            let metadata = fsl.base_file.file_metadata.as_ref().unwrap();
            assert!(metadata.size > 0);
        }
    }

    #[tokio::test]
    async fn test_get_file_slices_with_mdt_quickstart_table() {
        use hudi_test::QuickstartTripsTable;

        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let table = Table::new(&table_path).await.unwrap();
        assert!(table.is_metadata_table_enabled());

        let metadata_table = table.get_or_init_metadata_table().await.unwrap();
        let partition_schema = table.get_partition_schema().await.unwrap();
        let partition_pruner =
            PartitionPruner::new(&[], &partition_schema, table.hudi_configs.as_ref()).unwrap();
        let records = metadata_table
            .fetch_files_partition_records(&partition_pruner)
            .await
            .unwrap();
        assert!(!records.is_empty());

        let file_slices = table.get_file_slices(&ReadOptions::new()).await.unwrap();
        assert!(!file_slices.is_empty());
    }
}
