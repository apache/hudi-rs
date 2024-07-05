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

use HudiTableConfig::{
    BaseFileFormat, Checksum, DatabaseName, DropsPartitionFields, IsHiveStylePartitioning,
    IsPartitionPathUrlencoded, KeyGeneratorClass, PartitionFields, PopulatesMetaFields,
    PrecombineField, RecordKeyFields, TableName, TableType, TableVersion, TimelineLayoutVersion,
};

use crate::config::table::TablePropsProvider;
use crate::config::table::{BaseFileFormatValue, HudiTableConfig, TableTypeValue};
use crate::file_group::FileSlice;
use crate::storage::utils::parse_uri;
use crate::storage::Storage;
use crate::table::fs_view::FileSystemView;
use crate::table::timeline::Timeline;

mod fs_view;
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
            .context("Failed to load table properties")?;

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

    pub fn get_property(&self, key: &str) -> Option<&String> {
        self.props.get(key)
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

impl TablePropsProvider for Table {
    fn base_file_format(&self) -> BaseFileFormatValue {
        let v = self
            .get_property(BaseFileFormat.as_ref())
            .unwrap_or_else(|| panic!("Missing {}", BaseFileFormat.as_ref()));
        BaseFileFormatValue::from_str(v)
            .unwrap_or_else(|e| panic!("Failed to parse {}: {}", BaseFileFormat.as_ref(), e))
    }

    fn checksum(&self) -> Option<i64> {
        self.get_property(Checksum.as_ref()).map(|v| {
            i64::from_str(v)
                .unwrap_or_else(|e| panic!("Failed to parse {}: {}", Checksum.as_ref(), e))
        })
    }

    fn database_name(&self) -> Option<String> {
        self.props.get(DatabaseName.as_ref()).cloned()
    }

    fn drops_partition_fields(&self) -> Option<bool> {
        self.get_property(DropsPartitionFields.as_ref()).map(|v| {
            bool::from_str(v).unwrap_or_else(|e| {
                panic!("Failed to parse {}: {}", DropsPartitionFields.as_ref(), e)
            })
        })
    }

    fn is_hive_style_partitioning(&self) -> Option<bool> {
        self.get_property(IsHiveStylePartitioning.as_ref())
            .map(|v| {
                bool::from_str(v).unwrap_or_else(|e| {
                    panic!(
                        "Failed to parse {}: {}",
                        IsHiveStylePartitioning.as_ref(),
                        e
                    )
                })
            })
    }

    fn is_partition_path_urlencoded(&self) -> Option<bool> {
        self.get_property(IsPartitionPathUrlencoded.as_ref())
            .map(|v| {
                bool::from_str(v).unwrap_or_else(|e| {
                    panic!(
                        "Failed to parse {}: {}",
                        IsPartitionPathUrlencoded.as_ref(),
                        e
                    )
                })
            })
    }

    fn is_partitioned(&self) -> Option<bool> {
        self.key_generator_class()
            .map(|v| !v.ends_with("NonpartitionedKeyGenerator"))
    }

    fn key_generator_class(&self) -> Option<String> {
        self.get_property(KeyGeneratorClass.as_ref()).cloned()
    }

    fn location(&self) -> String {
        self.base_url.as_str().to_string()
    }

    fn partition_fields(&self) -> Option<Vec<String>> {
        self.get_property(PartitionFields.as_ref())
            .map(|s| s.split(',').map(str::to_string).collect())
    }

    fn precombine_field(&self) -> Option<String> {
        self.get_property(PrecombineField.as_ref()).cloned()
    }

    fn populates_meta_fields(&self) -> Option<bool> {
        self.get_property(PopulatesMetaFields.as_ref()).map(|v| {
            bool::from_str(v).unwrap_or_else(|e| {
                panic!("Failed to parse {}: {}", PopulatesMetaFields.as_ref(), e)
            })
        })
    }

    fn record_key_fields(&self) -> Option<Vec<String>> {
        self.get_property(RecordKeyFields.as_ref())
            .map(|s| s.split(',').map(str::to_string).collect())
    }

    fn table_name(&self) -> String {
        self.get_property(TableName.as_ref())
            .unwrap_or_else(|| panic!("Missing {}", TableName.as_ref()))
            .clone()
    }

    fn table_type(&self) -> TableTypeValue {
        let v = self
            .get_property(TableType.as_ref())
            .unwrap_or_else(|| panic!("Missing {}", TableType.as_ref()));
        TableTypeValue::from_str(v)
            .unwrap_or_else(|e| panic!("Failed to parse {}: {}", TableType.as_ref(), e))
    }

    fn table_version(&self) -> i32 {
        let v = self
            .get_property(TableVersion.as_ref())
            .unwrap_or_else(|| panic!("Missing {}", TableVersion.as_ref()));
        i32::from_str(v)
            .unwrap_or_else(|e| panic!("Failed to parse {}: {}", TableVersion.as_ref(), e))
    }

    fn timeline_layout_version(&self) -> Option<i32> {
        self.get_property(TimelineLayoutVersion.as_ref()).map(|v| {
            i32::from_str(v).unwrap_or_else(|e| {
                panic!("Failed to parse {}: {}", TimelineLayoutVersion.as_ref(), e)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs::canonicalize;
    use std::path::Path;

    use url::Url;

    use hudi_tests::TestTable;

    use crate::config::table::BaseFileFormatValue::Parquet;
    use crate::config::table::TablePropsProvider;
    use crate::config::table::TableTypeValue::CopyOnWrite;
    use crate::storage::utils::join_url_segments;
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
    async fn hudi_table_get_table_props() {
        let base_url = Url::from_file_path(
            canonicalize(Path::new(
                "tests/data/table_metadata/sample_table_properties",
            ))
            .unwrap(),
        )
        .unwrap();
        let table = Table::new(base_url.as_str(), HashMap::new()).await.unwrap();
        assert_eq!(table.base_file_format(), Parquet);
        assert_eq!(table.checksum().unwrap(), 3761586722);
        assert!(table.database_name().is_none());
        assert!(!table.drops_partition_fields().unwrap());
        assert!(!table.is_hive_style_partitioning().unwrap());
        assert!(!table.is_partition_path_urlencoded().unwrap());
        assert!(table.is_partitioned().unwrap());
        assert_eq!(
            table.key_generator_class().unwrap(),
            "org.apache.hudi.keygen.SimpleKeyGenerator"
        );
        assert_eq!(table.location(), base_url.as_str());
        assert_eq!(table.partition_fields().unwrap(), vec!["city"]);
        assert_eq!(table.precombine_field().unwrap(), "ts");
        assert!(table.populates_meta_fields().unwrap());
        assert_eq!(table.record_key_fields().unwrap(), vec!["uuid"]);
        assert_eq!(table.table_name(), "trips");
        assert_eq!(table.table_type(), CopyOnWrite);
        assert_eq!(table.table_version(), 6);
        assert_eq!(table.timeline_layout_version().unwrap(), 1);
    }
}
