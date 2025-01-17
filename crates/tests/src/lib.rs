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

    pub fn url_to_cow(&self) -> Url {
        let path = self.path_to_cow();
        Url::from_file_path(path).unwrap()
    }

    pub fn url_to_mor(&self) -> Url {
        let path = self.path_to_mor();
        Url::from_file_path(path).unwrap()
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
