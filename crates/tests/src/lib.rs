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

use arrow_array::{BooleanArray, Int32Array, RecordBatch, StringArray};
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use strum_macros::{AsRefStr, EnumIter, EnumString};
use tempfile::tempdir;
use url::Url;

pub mod utils;

pub fn extract_test_table(zip_path: &Path) -> PathBuf {
    let target_dir = tempdir().unwrap().path().to_path_buf();
    let archive = fs::read(zip_path).unwrap();
    zip_extract::extract(Cursor::new(archive), &target_dir, false).unwrap();
    target_dir
}

#[allow(dead_code)]
#[derive(Debug, EnumString, AsRefStr, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum SampleTable {
    V6ComplexkeygenHivestyle,
    V6Empty,
    V6Nonpartitioned,
    V6SimplekeygenHivestyleNoMetafields,
    V6SimplekeygenNonhivestyle,
    V6SimplekeygenNonhivestyleOverwritetable,
    V6TimebasedkeygenNonhivestyle,
}

impl SampleTable {
    /// Return rows of columns (id, name, isActive) for the given [RecordBatch] order by id.
    pub fn sample_data_order_by_id(record_batch: &RecordBatch) -> Vec<(i32, &str, bool)> {
        let ids = record_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = record_batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let is_actives = record_batch
            .column_by_name("isActive")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        let mut data: Vec<(i32, &str, bool)> = ids
            .iter()
            .zip(names.iter())
            .zip(is_actives.iter())
            .map(|((id, name), is_active)| (id.unwrap(), name.unwrap(), is_active.unwrap()))
            .collect();
        data.sort_unstable_by_key(|(id, _, _)| *id);
        data
    }

    fn zip_path(&self, table_type: &str) -> Box<Path> {
        let dir = env!("CARGO_MANIFEST_DIR");
        let data_path = Path::new(dir)
            .join("data/tables")
            .join(table_type.to_lowercase())
            .join(format!("{}.zip", self.as_ref()));
        data_path.into_boxed_path()
    }

    pub fn path_to_cow(&self) -> String {
        let zip_path = self.zip_path("cow");
        let path_buf = extract_test_table(zip_path.as_ref()).join(self.as_ref());
        path_buf.to_str().unwrap().to_string()
    }

    pub fn path_to_mor(&self) -> String {
        let zip_path = self.zip_path("mor");
        let path_buf = extract_test_table(zip_path.as_ref()).join(self.as_ref());
        path_buf.to_str().unwrap().to_string()
    }

    pub fn paths(&self) -> Vec<String> {
        vec![self.path_to_cow(), self.path_to_mor()]
    }

    pub fn url_to_cow(&self) -> Url {
        let path = self.path_to_cow();
        Url::from_file_path(path).unwrap()
    }

    pub fn url_to_mor(&self) -> Url {
        let path = self.path_to_mor();
        Url::from_file_path(path).unwrap()
    }

    pub fn urls(&self) -> Vec<Url> {
        vec![self.url_to_cow(), self.url_to_mor()]
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use crate::SampleTable;

    #[test]
    fn sample_table_zip_file_should_exist() {
        for t in SampleTable::iter() {
            let path = t.zip_path("cow");
            assert!(path.exists());
            assert!(path.is_file());
        }
    }
}
