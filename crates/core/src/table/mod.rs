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

use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use arrow_schema::SchemaRef;

use crate::file_group::FileSlice;
use crate::table::config::BaseFileFormat;
use crate::table::config::{ConfigKey, TableType};
use crate::table::fs_view::FileSystemView;
use crate::table::metadata::ProvidesTableMetadata;
use crate::timeline::Timeline;

mod config;
mod fs_view;
mod metadata;

#[derive(Debug, Clone)]
pub struct Table {
    pub base_path: PathBuf,
    pub props: HashMap<String, String>,
}

impl Table {
    pub fn new(table_base_path: &str) -> Self {
        let base_path = PathBuf::from(table_base_path);
        let props_path = base_path.join(".hoodie").join("hoodie.properties");
        match Self::load_properties(props_path.as_path()) {
            Ok(props) => Self { base_path, props },
            Err(e) => {
                panic!("Failed to load table properties: {}", e)
            }
        }
    }

    fn load_properties(path: &Path) -> Result<HashMap<String, String>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
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

    pub fn get_timeline(&self) -> Result<Timeline> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let f = async { Timeline::new(self.base_path.to_str().unwrap()).await };
        rt.block_on(f)
    }

    pub fn schema(&self) -> SchemaRef {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let f = async { Timeline::new(self.base_path.to_str().unwrap()).await };
        let timeline = rt.block_on(f);
        match timeline {
            Ok(timeline) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let wrapper = async { timeline.get_latest_schema().await };
                let result = rt.block_on(wrapper);
                match result {
                    Ok(schema) => SchemaRef::from(schema),
                    Err(e) => {
                        panic!("Failed to resolve table schema: {}", e)
                    }
                }
            }
            Err(e) => {
                panic!("Failed to resolve table schema: {}", e)
            }
        }
    }

    pub fn get_latest_file_slices(&self) -> Result<Vec<FileSlice>> {
        let mut file_slices = Vec::new();
        let mut fs_view = FileSystemView::new(self.base_path.as_path());
        for f in fs_view.get_latest_file_slices() {
            file_slices.push(f.clone());
        }
        Ok(file_slices)
    }

    pub fn get_latest_file_paths(&self) -> Result<Vec<String>> {
        let mut file_paths = Vec::new();
        for f in self.get_latest_file_slices()? {
            if let Some(f) = f.base_file_path() {
                file_paths.push(f.to_string());
            }
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
        self.base_path.to_str().unwrap().to_string()
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
    use std::path::Path;

    use crate::table::config::BaseFileFormat::Parquet;
    use crate::table::config::TableType::CopyOnWrite;
    use crate::table::metadata::ProvidesTableMetadata;
    use crate::table::Table;
    use crate::test_utils::extract_test_table;

    #[test]
    fn hudi_table_get_latest_file_paths() {
        let fixture_path = Path::new("fixtures/table/0.x_cow_partitioned.zip");
        let target_table_path = extract_test_table(fixture_path);
        let hudi_table = Table::new(target_table_path.to_str().unwrap());
        assert_eq!(hudi_table.get_timeline().unwrap().instants.len(), 2);
        assert_eq!(hudi_table.get_latest_file_paths().unwrap().len(), 5);
        println!("{}", hudi_table.schema());
    }

    #[test]
    fn hudi_table_get_table_metadata() {
        let fixture_path = Path::new("fixtures/table_metadata/sample_table_properties");
        let table = Table::new(fixture_path.to_str().unwrap());
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
        assert_eq!(
            table.location(),
            "fixtures/table_metadata/sample_table_properties"
        );
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
