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

use std::error::Error;
use std::path::PathBuf;

use arrow_schema::SchemaRef;

use crate::table::file_system_view::FileSystemView;
use crate::table::meta_client::MetaClient;

mod file_system_view;
mod meta_client;

#[derive(Debug, Clone)]
pub struct Table {
    pub base_path: PathBuf,
    meta_client: MetaClient,
}

impl Table {
    pub fn new(base_path: &str) -> Self {
        let p = PathBuf::from(base_path);
        let meta_client = MetaClient::new(p.as_path());
        Self {
            base_path: p,
            meta_client,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match self.meta_client.timeline.get_latest_schema() {
            Ok(table_schema) => SchemaRef::from(table_schema),
            Err(e) => {
                panic!("Failed to resolve table schema: {}", e)
            }
        }
    }

    pub fn get_snapshot_file_paths(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let meta_client = MetaClient::new(&self.base_path);
        let fs_view = FileSystemView::init(meta_client)?;
        let mut file_paths = Vec::new();
        for f in fs_view.get_latest_file_slices() {
            if let Some(f) = f.file_path() {
                file_paths.push(f.to_string());
            }
        }
        Ok(file_paths)
    }
}

#[cfg(test)]
mod tests {
    use crate::table::Table;
    use hudi_fs::test_utils::extract_test_table;
    use std::path::Path;

    #[test]
    fn load_snapshot_file_paths() {
        let fixture_path = Path::new("fixtures/table/0.x_cow_partitioned.zip");
        let target_table_path = extract_test_table(fixture_path);
        let hudi_table = Table::new(target_table_path.as_path().to_str().unwrap());
        assert_eq!(hudi_table.get_snapshot_file_paths().unwrap().len(), 5);
        println!("{}", hudi_table.schema());
    }
}
