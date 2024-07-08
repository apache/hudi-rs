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

use std::collections::HashMap;
use std::env;
use std::io::{BufRead, BufReader};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use strum::IntoEnumIterator;
use url::Url;

use HudiInternalConfig::SkipConfigValidation;
use HudiTableConfig::{DropsPartitionFields, TableType, TableVersion};
use TableTypeValue::CopyOnWrite;

use crate::config::internal::HudiInternalConfig;
use crate::config::read::HudiReadConfig;
use crate::config::read::HudiReadConfig::AsOfTimestamp;
use crate::config::table::{HudiTableConfig, TableTypeValue};
use crate::config::HudiConfigs;
use crate::file_group::FileSlice;
use crate::storage::utils::{empty_options, parse_uri};
use crate::storage::Storage;
use crate::table::fs_view::FileSystemView;
use crate::table::timeline::Timeline;

mod fs_view;
mod timeline;

#[derive(Clone, Debug)]
pub struct Table {
    pub base_url: Arc<Url>,
    pub configs: Arc<HudiConfigs>,
    pub extra_options: Arc<HashMap<String, String>>,
    pub timeline: Timeline,
    pub file_system_view: FileSystemView,
}

impl Table {
    pub async fn new(base_uri: &str) -> Result<Self> {
        Self::new_with_options(base_uri, empty_options()).await
    }

    pub async fn new_with_options<I, K, V>(base_uri: &str, all_options: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let base_url = Arc::new(parse_uri(base_uri)?);

        let (configs, extra_options) = Self::load_configs(base_url.clone(), all_options)
            .await
            .context("Failed to load table properties")?;
        let configs = Arc::new(configs);
        let extra_options = Arc::new(extra_options);

        let timeline = Timeline::new(base_url.clone(), extra_options.clone(), configs.clone())
            .await
            .context("Failed to load timeline")?;

        let file_system_view =
            FileSystemView::new(base_url.clone(), extra_options.clone(), configs.clone())
                .await
                .context("Failed to load file system view")?;

        Ok(Table {
            base_url,
            configs,
            extra_options,
            timeline,
            file_system_view,
        })
    }

    async fn load_configs<I, K, V>(
        base_url: Arc<Url>,
        all_options: I,
    ) -> Result<(HudiConfigs, HashMap<String, String>)>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        // TODO: load hudi global config
        let mut hudi_options = HashMap::new();
        let mut extra_options = HashMap::new();

        Self::imbue_cloud_env_vars(&mut extra_options);

        for (k, v) in all_options {
            if k.as_ref().starts_with("hoodie.") {
                hudi_options.insert(k.as_ref().to_string(), v.into());
            } else {
                extra_options.insert(k.as_ref().to_string(), v.into());
            }
        }
        let storage = Storage::new(base_url, &extra_options)?;
        let data = storage.get_file_data(".hoodie/hoodie.properties").await?;
        let cursor = std::io::Cursor::new(data);
        let lines = BufReader::new(cursor).lines();
        for line in lines {
            let line = line?;
            let trimmed_line = line.trim();
            if trimmed_line.is_empty() || trimmed_line.starts_with('#') {
                continue;
            }
            let mut parts = trimmed_line.splitn(2, '=');
            let key = parts.next().unwrap().to_owned();
            let value = parts.next().unwrap_or("").to_owned();
            // `hoodie.properties` takes precedence TODO handle conflicts where applicable
            hudi_options.insert(key, value);
        }
        let hudi_configs = HudiConfigs::new(hudi_options);

        Self::validate_configs(&hudi_configs).map(|_| (hudi_configs, extra_options))
    }

    fn imbue_cloud_env_vars(options: &mut HashMap<String, String>) {
        let prefixes = ["AWS_", "AZURE_", "GOOGLE_"];
        options.extend(
            env::vars()
                .filter(|(key, _)| prefixes.iter().any(|prefix| key.starts_with(prefix)))
                .map(|(k, v)| (k.to_ascii_lowercase(), v)),
        );
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

        let drops_partition_cols = hudi_configs.get(DropsPartitionFields)?.to::<bool>();
        if drops_partition_cols {
            return Err(anyhow!(
                "Only support when `{}` is disabled",
                DropsPartitionFields.as_ref()
            ));
        }

        Ok(())
    }

    pub async fn get_schema(&self) -> Result<Schema> {
        self.timeline.get_latest_schema().await
    }

    pub async fn split_file_slices(&self, n: usize) -> Result<Vec<Vec<FileSlice>>> {
        let n = std::cmp::max(1, n);
        let file_slices = self.get_file_slices().await?;
        let chunk_size = (file_slices.len() + n - 1) / n;

        Ok(file_slices
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect())
    }

    pub async fn get_file_slices(&self) -> Result<Vec<FileSlice>> {
        if let Some(timestamp) = self.configs.try_get(AsOfTimestamp) {
            self.get_file_slices_as_of(timestamp.to::<String>().as_str())
                .await
        } else if let Some(timestamp) = self.timeline.get_latest_commit_timestamp() {
            self.get_file_slices_as_of(timestamp).await
        } else {
            Ok(Vec::new())
        }
    }

    async fn get_file_slices_as_of(&self, timestamp: &str) -> Result<Vec<FileSlice>> {
        let excludes = self.timeline.get_replaced_file_groups().await?;
        self.file_system_view
            .load_file_slices_stats_as_of(timestamp, &excludes)
            .await
            .context("Fail to load file slice stats.")?;
        self.file_system_view
            .get_file_slices_as_of(timestamp, &excludes)
    }

    pub async fn read_snapshot(&self) -> Result<Vec<RecordBatch>> {
        if let Some(timestamp) = self.configs.try_get(AsOfTimestamp) {
            self.read_snapshot_as_of(timestamp.to::<String>().as_str())
                .await
        } else if let Some(timestamp) = self.timeline.get_latest_commit_timestamp() {
            self.read_snapshot_as_of(timestamp).await
        } else {
            Ok(Vec::new())
        }
    }

    async fn read_snapshot_as_of(&self, timestamp: &str) -> Result<Vec<RecordBatch>> {
        let file_slices = self
            .get_file_slices_as_of(timestamp)
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
    async fn get_file_paths(&self) -> Result<Vec<String>> {
        let mut file_paths = Vec::new();
        for f in self.get_file_slices().await? {
            file_paths.push(f.base_file_path().to_string());
        }
        Ok(file_paths)
    }

    pub async fn read_file_slice_by_path(&self, relative_path: &str) -> Result<RecordBatch> {
        self.file_system_view
            .read_file_slice_by_path_unchecked(relative_path)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fs::canonicalize;
    use std::panic;
    use std::path::Path;

    use url::Url;

    use hudi_tests::{assert_not, TestTable};

    use crate::config::read::HudiReadConfig::AsOfTimestamp;
    use crate::config::table::HudiTableConfig::{
        BaseFileFormat, Checksum, DatabaseName, DropsPartitionFields, IsHiveStylePartitioning,
        IsPartitionPathUrlencoded, KeyGeneratorClass, PartitionFields, PopulatesMetaFields,
        PrecombineField, RecordKeyFields, TableName, TableType, TableVersion,
        TimelineLayoutVersion,
    };
    use crate::storage::utils::join_url_segments;
    use crate::table::Table;

    #[tokio::test]
    async fn hudi_table_get_schema() {
        let base_url = TestTable::V6Nonpartitioned.url();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let fields: Vec<String> = hudi_table
            .get_schema()
            .await
            .unwrap()
            .all_fields()
            .into_iter()
            .map(|f| f.name().to_string())
            .collect();
        assert_eq!(
            fields,
            Vec::from([
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
            ])
        );
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
    async fn hudi_table_get_file_paths() {
        let base_url = TestTable::V6ComplexkeygenHivestyle.url();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        assert_eq!(hudi_table.timeline.instants.len(), 2);
        let actual: HashSet<String> =
            HashSet::from_iter(hudi_table.get_file_paths().await.unwrap());
        let expected: HashSet<String> = HashSet::from_iter(vec![
            "byteField=10/shortField=300/a22e8257-e249-45e9-ba46-115bc85adcba-0_0-161-223_20240418173235694.parquet",
            "byteField=20/shortField=100/bb7c3a45-387f-490d-aab2-981c3f1a8ada-0_0-140-198_20240418173213674.parquet",
            "byteField=30/shortField=100/4668e35e-bff8-4be9-9ff2-e7fb17ecb1a7-0_1-161-224_20240418173235694.parquet",
        ]
            .into_iter().map(|f| { join_url_segments(&base_url, &[f]).unwrap().to_string() })
            .collect::<Vec<_>>());
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn hudi_table_get_file_slices_as_of_timestamps() {
        let base_url = TestTable::V6Nonpartitioned.url();

        let hudi_table = Table::new(base_url.path()).await.unwrap();
        let file_slices = hudi_table.get_file_slices().await.unwrap();
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
        let file_slices = hudi_table.get_file_slices().await.unwrap();
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
        let file_slices = hudi_table.get_file_slices().await.unwrap();
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
        let file_slices = hudi_table.get_file_slices().await.unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path())
                .collect::<Vec<_>>(),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn validate_invalid_table_props() {
        let base_url =
            Url::from_file_path(canonicalize(Path::new("tests/data/table_props_invalid")).unwrap())
                .unwrap();
        let table = Table::new_with_options(
            base_url.as_str(),
            [("hoodie.internal.skip.config.validation", "true")],
        )
        .await
        .unwrap();
        let configs = table.configs;
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
        let base_url =
            Url::from_file_path(canonicalize(Path::new("tests/data/table_props_invalid")).unwrap())
                .unwrap();
        let table = Table::new_with_options(
            base_url.as_str(),
            [("hoodie.internal.skip.config.validation", "true")],
        )
        .await
        .unwrap();
        let configs = table.configs;
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
        let base_url =
            Url::from_file_path(canonicalize(Path::new("tests/data/table_props_invalid")).unwrap())
                .unwrap();
        let table = Table::new_with_options(
            base_url.as_str(),
            [("hoodie.internal.skip.config.validation", "true")],
        )
        .await
        .unwrap();
        let configs = table.configs;
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
        assert!(panic::catch_unwind(|| configs.get_or_default(PartitionFields)).is_err());
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
        let base_url =
            Url::from_file_path(canonicalize(Path::new("tests/data/table_props_valid")).unwrap())
                .unwrap();
        let table = Table::new_with_options(
            base_url.as_str(),
            [("hoodie.internal.skip.config.validation", "true")],
        )
        .await
        .unwrap();
        let configs = table.configs;
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
}
