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
use std::{fs::File, path::Path};

use parquet::file::reader::{FileReader, SerializedFileReader};

#[derive(Clone, Debug)]
pub struct FileMetadata {
    pub path: String,
    pub name: String,
    pub size: u64,
    pub num_records: i64,
}

impl FileMetadata {
    pub fn from_path(p: &Path) -> Result<Self, Box<dyn Error>> {
        let file = File::open(p)?;
        let reader = SerializedFileReader::new(file).unwrap();
        let num_records = reader.metadata().file_metadata().num_rows();
        Ok(Self {
            path: p.to_str().unwrap().to_string(),
            name: p.file_name().unwrap().to_os_string().into_string().unwrap(),
            size: p.metadata().unwrap().len(),
            num_records,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_approx_eq;
    use crate::file_systems::FileMetadata;
    use std::path::Path;

    #[test]
    fn read_file_metadata() {
        let fixture_path = Path::new("fixtures/a.parquet");
        let fm = FileMetadata::from_path(fixture_path).unwrap();
        assert_eq!(fm.path, "fixtures/a.parquet");
        assert_eq!(fm.name, "a.parquet");
        assert_approx_eq!(fm.size, 866, 20);
        assert_eq!(fm.num_records, 5);
    }
}
