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
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use url::Url;

use crate::file_group::FileSlice;
use crate::storage::Storage;
use crate::table::config::BaseFileFormat;
use crate::table::config::{ConfigKey, TableType};
use crate::table::fs_view::FileSystemView;
use crate::table::metadata::ProvidesTableMetadata;
use crate::timeline::Timeline;

mod config;
mod fs_view;
mod metadata;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Table {
    pub base_url: Url,
    pub props: HashMap<String, String>,
    pub file_system_view: Option<FileSystemView>,
    pub storage_options: HashMap<String, String>,
}

impl Table {
    pub fn new(base_uri: &str, storage_options: HashMap<String, String>) -> Self {
        let base_url = Url::from_file_path(PathBuf::from(base_uri).as_path()).unwrap();
        match Self::load_properties(&base_url, ".hoodie/hoodie.properties", &storage_options) {
            Ok(props) => Self {
                base_url,
                props,
                file_system_view: None,
                storage_options,
            },
            Err(e) => {
                panic!("Failed to load table properties: {}", e)
            }
        }
    }

    fn load_properties(
        base_url: &Url,
        props_path: &str,
        storage_options: &HashMap<String, String>,
    ) -> Result<HashMap<String, String>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let storage = Storage::new(base_url.clone(), storage_options.clone());
        let get_data = async { storage.get_file_data(props_path).await };
        let data = rt.block_on(get_data);
        let cursor = std::io::Cursor::new(data);
        let reader = BufReader::new(cursor);
        let lines = reader.lines();
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

    #[cfg(test)]
    fn get_timeline(&self) -> Result<Timeline> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let init_timeline = async { Timeline::new(self.base_url.clone()).await };
        rt.block_on(init_timeline)
    }

    pub fn get_latest_schema(&self) -> SchemaRef {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let init_timeline = async { Timeline::new(self.base_url.clone()).await };
        let timeline = rt.block_on(init_timeline);
        match timeline {
            Ok(timeline) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let get_schema = async { timeline.get_latest_schema().await };
                match rt.block_on(get_schema) {
                    Ok(schema) => SchemaRef::from(schema),
                    Err(e) => panic!("Failed to resolve table schema: {}", e),
                }
            }
            Err(e) => panic!("Failed to resolve table schema: {}", e),
        }
    }

    pub fn get_latest_file_slices(&mut self) -> Result<Vec<FileSlice>> {
        let mut file_slices = Vec::new();

        if self.file_system_view.is_none() {
            let mut new_fs_view = FileSystemView::new(self.base_url.clone());
            new_fs_view.load_file_groups();
            self.file_system_view = Some(new_fs_view);
        }

        let fs_view = self.file_system_view.as_mut().unwrap();

        for f in fs_view.get_latest_file_slices_with_stats() {
            file_slices.push(f.clone());
        }
        Ok(file_slices)
    }

    pub fn read_file_slice(&self, relative_path: &str) -> Vec<RecordBatch> {
        let fs_view = self.file_system_view.as_ref().unwrap();
        fs_view.read_file_slice(relative_path)
    }

    pub fn get_latest_file_paths(&mut self) -> Result<Vec<String>> {
        let mut file_paths = Vec::new();
        for f in self.get_latest_file_slices()? {
            file_paths.push(f.base_file_path().to_string());
        }
        Ok(file_paths)
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
    use std::collections::HashMap;
    use std::fs::canonicalize;
    use std::path::Path;
    use url::Url;

    use crate::table::config::BaseFileFormat::Parquet;
    use crate::table::config::TableType::CopyOnWrite;
    use crate::table::metadata::ProvidesTableMetadata;
    use crate::table::Table;
    use crate::test_utils::extract_test_table;

    #[test]
    fn hudi_table_get_latest_file_paths() {
        let fixture_path = Path::new("fixtures/table/0.x_cow_partitioned.zip");
        let base_url = Url::from_file_path(extract_test_table(fixture_path)).unwrap();
        let mut hudi_table = Table::new(&base_url.path(), HashMap::new());
        assert_eq!(hudi_table.get_timeline().unwrap().instants.len(), 2);
        assert_eq!(hudi_table.get_latest_file_paths().unwrap().len(), 5);
    }

    #[test]
    fn hudi_table_get_table_metadata() {
        let base_path = canonicalize(Path::new("fixtures/table_metadata/sample_table_properties")).unwrap();
        let table = Table::new(base_path.to_str().unwrap(), HashMap::new());
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
