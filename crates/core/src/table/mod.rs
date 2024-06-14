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

use crate::table::fs_view::FileSystemView;
use crate::timeline::Timeline;

mod fs_view;

#[derive(Debug, Clone)]
pub struct Table {
    pub path: PathBuf,
}

impl Table {
    pub fn new(p: &str) -> Self {
        let path = PathBuf::from(p);
        Self { path }
    }

    pub fn get_timeline(&self) -> Result<Timeline, std::io::Error> {
        Timeline::new(self.path.as_path())
    }

    pub fn schema(&self) -> SchemaRef {
        match Timeline::new(self.path.as_path()) {
            Ok(timeline) => {
                match timeline.get_latest_schema() {
                    Ok(schema) => {
                        SchemaRef::from(schema)
                    }
                    Err(e) => { panic!("Failed to resolve table schema: {}", e) }
                }
            }
            Err(e) => { panic!("Failed to resolve table schema: {}", e) }
        }
    }

    pub fn get_snapshot_file_paths(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let mut file_paths = Vec::new();
        let fs_view = FileSystemView::new(self.path.as_path())?;
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
        let hudi_table = Table::new(target_table_path.to_str().unwrap());
        assert_eq!(hudi_table.get_timeline().unwrap().instants.len(), 2);
        assert_eq!(hudi_table.get_snapshot_file_paths().unwrap().len(), 5);
        println!("{}", hudi_table.schema());
    }
}
