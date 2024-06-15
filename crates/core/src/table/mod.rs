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
use std::error::Error;
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

    fn load_properties(path: &Path) -> Result<HashMap<String, String>, std::io::Error> {
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

    pub fn get_timeline(&self) -> Result<Timeline, std::io::Error> {
        Timeline::new(self.base_path.as_path())
    }

    pub fn schema(&self) -> SchemaRef {
        match Timeline::new(self.base_path.as_path()) {
            Ok(timeline) => match timeline.get_latest_schema() {
                Ok(schema) => SchemaRef::from(schema),
                Err(e) => {
                    panic!("Failed to resolve table schema: {}", e)
                }
            },
            Err(e) => {
                panic!("Failed to resolve table schema: {}", e)
            }
        }
    }

    pub fn get_latest_file_slices(&self) -> Result<Vec<FileSlice>, Box<dyn Error>> {
        let mut file_slices = Vec::new();
        let fs_view = FileSystemView::new(self.base_path.as_path())?;
        for f in fs_view.get_latest_file_slices() {
            file_slices.push(f.clone());
        }
        Ok(file_slices)
    }

    pub fn get_latest_file_paths(&self) -> Result<Vec<String>, Box<dyn Error>> {
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
        self.get_property(ConfigKey::DatabaseName.as_ref())
            .to_string()
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
        self.key_generator_class()
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

    fn timeline_timezone(&self) -> String {
        self.get_property(ConfigKey::TimelineTimezone.as_ref())
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use hudi_fs::test_utils::extract_test_table;

    use crate::table::Table;

    #[test]
    fn load_snapshot_file_paths() {
        let fixture_path = Path::new("fixtures/table/0.x_cow_partitioned.zip");
        let target_table_path = extract_test_table(fixture_path);
        let hudi_table = Table::new(target_table_path.to_str().unwrap());
        assert_eq!(hudi_table.get_timeline().unwrap().instants.len(), 2);
        assert_eq!(hudi_table.get_latest_file_paths().unwrap().len(), 5);
        println!("{}", hudi_table.schema());
    }
}
