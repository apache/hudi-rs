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
//! Users can obtain metadata to customize reading methods, read in batches, perform parallel reads, and more.
//! ```rust
//! use url::Url;
//! use hudi_core::table::Table;
//! use hudi_core::storage::utils::parse_uri;
//!
//! pub async fn test() {
//!     let base_uri = Url::from_file_path("/tmp/hudi_data").unwrap();
//!     let hudi_table = Table::new(base_uri.path()).await.unwrap();
//!     let file_slices = hudi_table
//!             .split_file_slices(2, &[])
//!             .await.unwrap();
//!     // define every parquet task reader how many slice
//!     let mut parquet_file_groups: Vec<Vec<String>> = Vec::new();
//!         for file_slice_vec in file_slices {
//!             let file_group_vec = file_slice_vec
//!                 .iter()
//!                 .map(|f| {
//!                     let url = parse_uri(&f.base_file.info.uri).unwrap();
//!                     let size = f.base_file.info.size as u64;
//!                     url.path().to_string()
//!                 })
//!                 .collect();
//!             parquet_file_groups.push(file_group_vec)
//!         }
//! }
//! ```

use std::collections::{HashMap, HashSet};
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
use arrow_schema::{Field, Schema};
use strum::IntoEnumIterator;

use HudiInternalConfig::SkipConfigValidation;
use HudiTableConfig::{DropsPartitionFields, TableType, TableVersion};
use TableTypeValue::CopyOnWrite;

use crate::config::internal::HudiInternalConfig;
use crate::config::read::HudiReadConfig;
use crate::config::read::HudiReadConfig::AsOfTimestamp;
use crate::config::table::HudiTableConfig::PartitionFields;
use crate::config::table::{HudiTableConfig, TableTypeValue};
use crate::config::utils::{empty_options, split_hudi_options_from_others};
use crate::config::HudiConfigs;
use crate::config::HUDI_CONF_DIR;
use crate::file_group::FileSlice;
use crate::storage::utils::parse_config_data;
use crate::storage::Storage;
use crate::table::fs_view::FileSystemView;
use crate::table::partition::PartitionPruner;
use crate::table::timeline::Timeline;

mod fs_view;
mod partition;
mod timeline;

/// Hudi Table in-memory
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
        Self::new_with_options(base_uri, empty_options()).await
    }

    /// Create hudi table with options
    pub async fn new_with_options<I, K, V>(base_uri: &str, options: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let (hudi_configs, storage_options) = Self::load_configs(base_uri, options)
            .await
            .context("Failed to load table properties")?;
        let hudi_configs = Arc::new(hudi_configs);
        let storage_options = Arc::new(storage_options);

        let timeline = Timeline::new(hudi_configs.clone(), storage_options.clone())
            .await
            .context("Failed to load timeline")?;

        let file_system_view = FileSystemView::new(hudi_configs.clone(), storage_options.clone())
            .await
            .context("Failed to load file system view")?;

        Ok(Table {
            hudi_configs,
            storage_options,
            timeline,
            file_system_view,
        })
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

    async fn load_configs<I, K, V>(
        base_uri: &str,
        all_options: I,
    ) -> Result<(HudiConfigs, HashMap<String, String>)>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let mut hudi_options = HashMap::new();
        let mut other_options = HashMap::new();

        Self::imbue_cloud_env_vars(&mut other_options);

        let (hudi_opts, others) = split_hudi_options_from_others(all_options);
        hudi_options.extend(hudi_opts);
        other_options.extend(others);

        hudi_options.insert(
            HudiTableConfig::BasePath.as_ref().to_string(),
            base_uri.to_string(),
        );

        // create a [Storage] instance to load properties from storage layer.
        let storage = Storage::new(
            Arc::new(other_options.clone()),
            Arc::new(HudiConfigs::new(&hudi_options)),
        )?;

        Self::imbue_table_properties(&mut hudi_options, storage.clone()).await?;

        Self::imbue_global_hudi_configs_if_not_present(&mut hudi_options, storage.clone()).await?;

        let hudi_configs = HudiConfigs::new(hudi_options);

        Self::validate_configs(&hudi_configs).map(|_| (hudi_configs, other_options))
    }

    fn imbue_cloud_env_vars(options: &mut HashMap<String, String>) {
        const PREFIXES: [&str; 3] = ["AWS_", "AZURE_", "GOOGLE_"];

        for (key, value) in env::vars() {
            if PREFIXES.iter().any(|prefix| key.starts_with(prefix))
                && !options.contains_key(&key.to_ascii_lowercase())
            {
                options.insert(key.to_ascii_lowercase(), value);
            }
        }
    }

    async fn imbue_table_properties(
        options: &mut HashMap<String, String>,
        storage: Arc<Storage>,
    ) -> Result<()> {
        let bytes = storage.get_file_data(".hoodie/hoodie.properties").await?;
        let table_properties = parse_config_data(&bytes, "=").await?;

        // TODO: handle the case where the same key is present in both table properties and options
        for (k, v) in table_properties {
            options.insert(k.to_string(), v.to_string());
        }

        Ok(())
    }

    async fn imbue_global_hudi_configs_if_not_present(
        options: &mut HashMap<String, String>,
        storage: Arc<Storage>,
    ) -> Result<()> {
        let global_config_path = env::var(HUDI_CONF_DIR)
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/etc/hudi/conf"))
            .join("hudi-defaults.conf");

        if let Ok(bytes) = storage
            .get_file_data_from_absolute_path(global_config_path.to_str().unwrap())
            .await
        {
            if let Ok(global_configs) = parse_config_data(&bytes, " \t=").await {
                for (key, value) in global_configs {
                    if key.starts_with("hoodie.") && !options.contains_key(&key) {
                        options.insert(key.to_string(), value.to_string());
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_configs(hudi_configs: &HudiConfigs) -> Result<()> {
        if hudi_configs
            .get_or_default(SkipConfigValidation)
            .to::<bool>()
        {
            return Ok(());
        }

        for conf in HudiTableConfig::iter() {
            hudi_configs.validate(conf)?
        }

        for conf in HudiReadConfig::iter() {
            hudi_configs.validate(conf)?
        }

        // additional validation
        let table_type = hudi_configs.get(TableType)?.to::<String>();
        if TableTypeValue::from_str(&table_type)? != CopyOnWrite {
            return Err(anyhow!("Only support copy-on-write table."));
        }

        let table_version = hudi_configs.get(TableVersion)?.to::<isize>();
        if !(5..=6).contains(&table_version) {
            return Err(anyhow!("Only support table version 5 and 6."));
        }

        let drops_partition_cols = hudi_configs
            .get_or_default(DropsPartitionFields)
            .to::<bool>();
        if drops_partition_cols {
            return Err(anyhow!(
                "Only support when `{}` is disabled",
                DropsPartitionFields.as_ref()
            ));
        }

        Ok(())
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

    /// Split the file into a specified number of parts
    pub async fn split_file_slices(
        &self,
        n: usize,
        filters: &[&str],
    ) -> Result<Vec<Vec<FileSlice>>> {
        let n = std::cmp::max(1, n);
        let file_slices = self.get_file_slices(filters).await?;
        let chunk_size = (file_slices.len() + n - 1) / n;

        Ok(file_slices
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect())
    }

    /// Get all the [FileSlice]s in the table.
    ///
    /// If the [AsOfTimestamp] configuration is set, the file slices at the specified timestamp will be returned.
    pub async fn get_file_slices(&self, filters: &[&str]) -> Result<Vec<FileSlice>> {
        if let Some(timestamp) = self.hudi_configs.try_get(AsOfTimestamp) {
            self.get_file_slices_as_of(timestamp.to::<String>().as_str(), filters)
                .await
        } else if let Some(timestamp) = self.timeline.get_latest_commit_timestamp() {
            self.get_file_slices_as_of(timestamp, filters).await
        } else {
            Ok(Vec::new())
        }
    }

    /// Get all the [FileSlice]s at a given timestamp, as a time travel query.
    async fn get_file_slices_as_of(
        &self,
        timestamp: &str,
        filters: &[&str],
    ) -> Result<Vec<FileSlice>> {
        let excludes = self.timeline.get_replaced_file_groups().await?;
        let partition_schema = self.get_partition_schema().await?;
        let partition_pruner =
            PartitionPruner::new(filters, &partition_schema, self.hudi_configs.as_ref())?;
        self.file_system_view
            .get_file_slices_as_of(timestamp, &partition_pruner, &excludes)
            .await
    }

    /// Get all the latest records in the table.
    ///
    /// If the [AsOfTimestamp] configuration is set, the records at the specified timestamp will be returned.
    pub async fn read_snapshot(&self, filters: &[&str]) -> Result<Vec<RecordBatch>> {
        if let Some(timestamp) = self.hudi_configs.try_get(AsOfTimestamp) {
            self.read_snapshot_as_of(timestamp.to::<String>().as_str(), filters)
                .await
        } else if let Some(timestamp) = self.timeline.get_latest_commit_timestamp() {
            self.read_snapshot_as_of(timestamp, filters).await
        } else {
            Ok(Vec::new())
        }
    }

    /// Get all the records in the table at a given timestamp, as a time travel query.
    async fn read_snapshot_as_of(
        &self,
        timestamp: &str,
        filters: &[&str],
    ) -> Result<Vec<RecordBatch>> {
        let file_slices = self
            .get_file_slices_as_of(timestamp, filters)
            .await
            .context(format!("Failed to get file slices as of {}", timestamp))?;
        let mut batches = Vec::new();
        for f in file_slices {
            match self.file_system_view.read_file_slice_unchecked(&f).await {
                Ok(batch) => batches.push(batch),
                Err(e) => return Err(anyhow!("Failed to read file slice {:?} - {}", f, e)),
            }
        }
        Ok(batches)
    }

    #[cfg(test)]
    async fn get_file_paths_with_filters(&self, filters: &[&str]) -> Result<Vec<String>> {
        let mut file_paths = Vec::new();
        for f in self.get_file_slices(filters).await? {
            file_paths.push(f.base_file_path().to_string());
        }
        Ok(file_paths)
    }

    /// Read records from a [FileSlice] by its relative path.
    ///
    /// **Example**
    ///
    /// ```rust
    /// use url::Url;
    /// use hudi_core::table::Table;
    ///
    /// pub async fn test() {
    ///     let base_uri = Url::from_file_path("/tmp/hudi_data").unwrap();
    ///     let hudi_table = Table::new(base_uri.path()).await.unwrap();
    ///     let batches = hudi_table
    ///         .read_file_slice_by_path(
    ///             "a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",
    ///         )
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn read_file_slice_by_path(&self, relative_path: &str) -> Result<RecordBatch> {
        self.file_system_view
            .read_file_slice_by_path_unchecked(relative_path)
            .await
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::StringArray;
    use std::collections::HashSet;
    use std::fs::canonicalize;
    use std::path::PathBuf;
    use std::{env, panic};
    use url::Url;

    use hudi_tests::{assert_not, TestTable};

    use crate::config::read::HudiReadConfig::AsOfTimestamp;
    use crate::config::table::HudiTableConfig::{
        BaseFileFormat, Checksum, DatabaseName, DropsPartitionFields, IsHiveStylePartitioning,
        IsPartitionPathUrlencoded, KeyGeneratorClass, PartitionFields, PopulatesMetaFields,
        PrecombineField, RecordKeyFields, TableName, TableType, TableVersion,
        TimelineLayoutVersion,
    };
    use crate::config::HUDI_CONF_DIR;
    use crate::storage::utils::join_url_segments;
    use crate::table::Table;

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

    #[tokio::test]
    async fn hudi_table_get_schema() {
        let base_url = TestTable::V6Nonpartitioned.url();
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
        let base_url = TestTable::V6TimebasedkeygenNonhivestyle.url();
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
    }

    #[tokio::test]
    async fn get_default_for_invalid_table_props() {
        let table = get_test_table_without_validation("table_props_invalid").await;
        let configs = table.hudi_configs;
        assert!(panic::catch_unwind(|| configs.get_or_default(BaseFileFormat)).is_err());
        assert!(panic::catch_unwind(|| configs.get_or_default(Checksum)).is_err());
        assert_eq!(
            configs.get_or_default(DatabaseName).to::<String>(),
            "default"
        );
        assert_not!(configs.get_or_default(DropsPartitionFields).to::<bool>());
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
        let base_url = TestTable::V6Nonpartitioned.url();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let batches = hudi_table
            .read_file_slice_by_path(
                "a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",
            )
            .await
            .unwrap();
        assert_eq!(batches.num_rows(), 4);
        assert_eq!(batches.num_columns(), 21);
    }

    #[tokio::test]
    async fn hudi_table_get_file_slices_as_of_timestamps() {
        let base_url = TestTable::V6Nonpartitioned.url();

        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let file_slices = hudi_table.get_file_slices(&[]).await.unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path())
                .collect::<Vec<_>>(),
            vec!["a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",]
        );

        // as of the latest timestamp
        let opts = [(AsOfTimestamp.as_ref(), "20240418173551906")];
        let hudi_table = Table::new_with_options(base_url.path(), opts)
            .await
            .unwrap();
        let file_slices = hudi_table.get_file_slices(&[]).await.unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path())
                .collect::<Vec<_>>(),
            vec!["a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",]
        );

        // as of just smaller than the latest timestamp
        let opts = [(AsOfTimestamp.as_ref(), "20240418173551905")];
        let hudi_table = Table::new_with_options(base_url.path(), opts)
            .await
            .unwrap();
        let file_slices = hudi_table.get_file_slices(&[]).await.unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path())
                .collect::<Vec<_>>(),
            vec!["a079bdb3-731c-4894-b855-abfcd6921007-0_0-182-253_20240418173550988.parquet",]
        );

        // as of non-exist old timestamp
        let opts = [(AsOfTimestamp.as_ref(), "0")];
        let hudi_table = Table::new_with_options(base_url.path(), opts)
            .await
            .unwrap();
        let file_slices = hudi_table.get_file_slices(&[]).await.unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path())
                .collect::<Vec<_>>(),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn hudi_table_get_file_paths_for_simple_keygen_non_hive_style() {
        let base_url = TestTable::V6SimplekeygenNonhivestyle.url();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        assert_eq!(hudi_table.timeline.instants.len(), 2);

        let partition_filters = &[];
        let actual = hudi_table
            .get_file_paths_with_filters(partition_filters)
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

        let partition_filters = &["byteField >= 10", "byteField < 30"];
        let actual = hudi_table
            .get_file_paths_with_filters(partition_filters)
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

        let partition_filters = &["byteField > 30"];
        let actual = hudi_table
            .get_file_paths_with_filters(partition_filters)
            .await
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();
        let expected = HashSet::new();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn hudi_table_get_file_paths_for_complex_keygen_hive_style() {
        let base_url = TestTable::V6ComplexkeygenHivestyle.url();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        assert_eq!(hudi_table.timeline.instants.len(), 2);

        let partition_filters = &[];
        let actual = hudi_table
            .get_file_paths_with_filters(partition_filters)
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

        let partition_filters = &["byteField >= 10", "byteField < 20", "shortField != 100"];
        let actual = hudi_table
            .get_file_paths_with_filters(partition_filters)
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

        let partition_filters = &["byteField > 20", "shortField = 300"];
        let actual = hudi_table
            .get_file_paths_with_filters(partition_filters)
            .await
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();
        let expected = HashSet::new();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn hudi_table_read_snapshot_for_complex_keygen_hive_style() {
        let base_url = TestTable::V6ComplexkeygenHivestyle.url();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let partition_filters = &["byteField >= 10", "byteField < 20", "shortField != 100"];
        let records = hudi_table.read_snapshot(partition_filters).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].num_rows(), 2);
        let actual_partition_paths: HashSet<&str> = HashSet::from_iter(
            records[0]
                .column_by_name("_hoodie_partition_path")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|s| s.unwrap())
                .collect::<Vec<_>>(),
        );
        let expected_partition_paths: HashSet<&str> =
            HashSet::from_iter(vec!["byteField=10/shortField=300"]);
        assert_eq!(actual_partition_paths, expected_partition_paths);

        let actual_file_names: HashSet<&str> = HashSet::from_iter(
            records[0]
                .column_by_name("_hoodie_file_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|s| s.unwrap())
                .collect::<Vec<_>>(),
        );
        let expected_file_names: HashSet<&str> = HashSet::from_iter(vec![
            "a22e8257-e249-45e9-ba46-115bc85adcba-0_0-161-223_20240418173235694.parquet",
        ]);
        assert_eq!(actual_file_names, expected_file_names);
    }
}
