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
use std::io::{BufRead, BufReader};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use url::Url;

use crate::file_group::FileSlice;
use crate::storage::utils::parse_uri;
use crate::storage::Storage;
use crate::table::config::BaseFileFormat;
use crate::table::config::{ConfigKey, TableType};
use crate::table::fs_view::FileSystemView;
use crate::table::metadata::ProvidesTableMetadata;
use crate::table::timeline::Timeline;

mod config;
mod fs_view;
mod metadata;
mod timeline;

#[derive(Debug, Clone)]
pub struct Table {
    pub base_url: Arc<Url>,
    pub storage_options: Arc<HashMap<String, String>>,
    pub props: Arc<HashMap<String, String>>,
    pub timeline: Timeline,
    pub file_system_view: FileSystemView,
}

impl Table {
    pub async fn new(base_uri: &str, storage_options: HashMap<String, String>) -> Result<Self> {
        let base_url = Arc::new(parse_uri(base_uri)?);
        let storage_options = Arc::new(storage_options);

        let props = Self::load_properties(base_url.clone(), storage_options.clone())
            .await
            .context("Failed to create a table")?;

        let props = Arc::new(props);
        let timeline = Timeline::new(base_url.clone(), storage_options.clone(), props.clone())
            .await
            .context("Failed to load timeline")?;

        let file_system_view =
            FileSystemView::new(base_url.clone(), storage_options.clone(), props.clone())
                .await
                .context("Failed to load file system view")?;

        Ok(Table {
            base_url,
            storage_options,
            props,
            timeline,
            file_system_view,
        })
    }

    async fn load_properties(
        base_url: Arc<Url>,
        storage_options: Arc<HashMap<String, String>>,
    ) -> Result<HashMap<String, String>> {
        let storage = Storage::new(base_url, storage_options)?;
        let data = storage.get_file_data(".hoodie/hoodie.properties").await?;
        let cursor = std::io::Cursor::new(data);
        let lines = BufReader::new(cursor).lines();
        let mut properties: HashMap<String, String> = HashMap::new();
        for line in lines {
            let line = line?;
            let trimmed_line = line.trim();
            if trimmed_line.is_empty() || trimmed_line.starts_with('#') {
                continue;
            }
            let mut parts = trimmed_line.splitn(2, '=');
            let key = parts.next().unwrap().to_owned();
            let value = parts.next().unwrap_or("").to_owned();
            properties.insert(key, value);
        }
        Ok(properties)
    }

    pub fn get_property(&self, key: &str) -> &str {
        match self.props.get(key) {
            Some(value) => value,
            None => panic!("Failed to retrieve property {}", key),
        }
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
        if let Some(timestamp) = self.timeline.get_latest_commit_timestamp() {
            self.get_file_slices_as_of(timestamp).await
        } else {
            Ok(Vec::new())
        }
    }

    pub async fn get_file_slices_as_of(&self, timestamp: &str) -> Result<Vec<FileSlice>> {
        self.file_system_view
            .load_file_slices_stats_as_of(timestamp)
            .await
            .context("Fail to load file slice stats.")?;
        self.file_system_view.get_file_slices_as_of(timestamp)
    }

    pub async fn read_snapshot(&self) -> Result<Vec<RecordBatch>> {
        if let Some(timestamp) = self.timeline.get_latest_commit_timestamp() {
            self.read_snapshot_as_of(timestamp).await
        } else {
            Ok(Vec::new())
        }
    }

    pub async fn read_snapshot_as_of(&self, timestamp: &str) -> Result<Vec<RecordBatch>> {
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

impl ProvidesTableMetadata for Table {
    fn base_file_format(&self) -> BaseFileFormat {
        BaseFileFormat::from_str(self.get_property(ConfigKey::BaseFileFormat.as_ref())).unwrap()
    }

    fn checksum(&self) -> i64 {
        i64::from_str(self.get_property(ConfigKey::Checksum.as_ref())).unwrap()
    }

    fn database_name(&self) -> String {
        match self.props.get(ConfigKey::DatabaseName.as_ref()) {
            Some(value) => value.to_string(),
            None => "default".to_string(),
        }
    }

    fn drops_partition_fields(&self) -> bool {
        bool::from_str(self.get_property(ConfigKey::DropsPartitionFields.as_ref())).unwrap()
    }

    fn is_hive_style_partitioning(&self) -> bool {
        bool::from_str(self.get_property(ConfigKey::IsHiveStylePartitioning.as_ref())).unwrap()
    }

    fn is_partition_path_urlencoded(&self) -> bool {
        bool::from_str(self.get_property(ConfigKey::IsPartitionPathUrlencoded.as_ref())).unwrap()
    }

    fn is_partitioned(&self) -> bool {
        !self
            .key_generator_class()
            .ends_with("NonpartitionedKeyGenerator")
    }

    fn key_generator_class(&self) -> String {
        self.get_property(ConfigKey::KeyGeneratorClass.as_ref())
            .to_string()
    }

    fn location(&self) -> String {
        self.base_url.path().to_string()
    }

    fn partition_fields(&self) -> Vec<String> {
        self.get_property(ConfigKey::PartitionFields.as_ref())
            .split(',')
            .map(str::to_string)
            .collect()
    }

    fn precombine_field(&self) -> String {
        self.get_property(ConfigKey::PrecombineField.as_ref())
            .to_string()
    }

    fn populates_meta_fields(&self) -> bool {
        bool::from_str(self.get_property(ConfigKey::PopulatesMetaFields.as_ref())).unwrap()
    }

    fn record_key_fields(&self) -> Vec<String> {
        self.get_property(ConfigKey::RecordKeyFields.as_ref())
            .split(',')
            .map(str::to_string)
            .collect()
    }

    fn table_name(&self) -> String {
        self.get_property(ConfigKey::TableName.as_ref()).to_string()
    }

    fn table_type(&self) -> TableType {
        TableType::from_str(self.get_property(ConfigKey::TableType.as_ref())).unwrap()
    }

    fn table_version(&self) -> u32 {
        u32::from_str(self.get_property(ConfigKey::TableVersion.as_ref())).unwrap()
    }

    fn timeline_layout_version(&self) -> u32 {
        u32::from_str(self.get_property(ConfigKey::TimelineLayoutVersion.as_ref())).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs::canonicalize;
    use std::path::Path;

    use hudi_tests::TestTable;

    use crate::storage::utils::join_url_segments;
    use crate::table::config::BaseFileFormat::Parquet;
    use crate::table::config::TableType::CopyOnWrite;
    use crate::table::metadata::ProvidesTableMetadata;
    use crate::table::Table;

    #[tokio::test]
    async fn hudi_table_get_schema() {
        let base_url = TestTable::V6Nonpartitioned.url();
        let hudi_table = Table::new(base_url.path(), HashMap::new()).await.unwrap();
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
        let hudi_table = Table::new(base_url.path(), HashMap::new()).await.unwrap();
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
        let hudi_table = Table::new(base_url.path(), HashMap::new()).await.unwrap();
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
        let hudi_table = Table::new(base_url.path(), HashMap::new()).await.unwrap();

        let file_slices = hudi_table.get_file_slices().await.unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path())
                .collect::<Vec<_>>(),
            vec!["a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",]
        );

        // as of the latest timestamp
        let file_slices = hudi_table
            .get_file_slices_as_of("20240418173551906")
            .await
            .unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path())
                .collect::<Vec<_>>(),
            vec!["a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet",]
        );

        // as of just smaller than the latest timestamp
        let file_slices = hudi_table
            .get_file_slices_as_of("20240418173551905")
            .await
            .unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path())
                .collect::<Vec<_>>(),
            vec!["a079bdb3-731c-4894-b855-abfcd6921007-0_0-182-253_20240418173550988.parquet",]
        );

        // as of non-exist old timestamp
        let file_slices = hudi_table.get_file_slices_as_of("0").await.unwrap();
        assert_eq!(
            file_slices
                .iter()
                .map(|f| f.base_file_relative_path())
                .collect::<Vec<_>>(),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn hudi_table_get_table_metadata() {
        let base_path =
            canonicalize(Path::new("tests/data/table_metadata/sample_table_properties")).unwrap();
        let table = Table::new(base_path.to_str().unwrap(), HashMap::new())
            .await
            .unwrap();
        assert_eq!(table.base_file_format(), Parquet);
        assert_eq!(table.checksum(), 3761586722);
        assert_eq!(table.database_name(), "default");
        assert!(!table.drops_partition_fields());
        assert!(!table.is_hive_style_partitioning());
        assert!(!table.is_partition_path_urlencoded());
        assert!(table.is_partitioned());
        assert_eq!(
            table.key_generator_class(),
            "org.apache.hudi.keygen.SimpleKeyGenerator"
        );
        assert_eq!(table.location(), base_path.to_str().unwrap());
        assert_eq!(table.partition_fields(), vec!["city"]);
        assert_eq!(table.precombine_field(), "ts");
        assert!(table.populates_meta_fields());
        assert_eq!(table.record_key_fields(), vec!["uuid"]);
        assert_eq!(table.table_name(), "trips");
        assert_eq!(table.table_type(), CopyOnWrite);
        assert_eq!(table.table_version(), 6);
        assert_eq!(table.timeline_layout_version(), 1);
    }
}
