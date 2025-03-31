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

use arrow_array::{BooleanArray, Float64Array, Int32Array, RecordBatch, StringArray};
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use strum_macros::{AsRefStr, EnumIter, EnumString};
use tempfile::tempdir;
use url::Url;

pub mod util;

pub fn extract_test_table(zip_path: &Path) -> PathBuf {
    let target_dir = tempdir().unwrap().path().to_path_buf();
    let archive = fs::read(zip_path).unwrap();
    zip_extract::extract(Cursor::new(archive), &target_dir, false).unwrap();
    target_dir
}

#[allow(dead_code)]
#[derive(Debug, EnumString, AsRefStr, EnumIter)]
pub enum QuickstartTripsTable {
    #[strum(serialize = "v6_trips_8i1u")]
    V6Trips8I1U,
}

impl QuickstartTripsTable {
    pub fn uuid_rider_and_fare(record_batch: &RecordBatch) -> Vec<(String, String, f64)> {
        let uuids = record_batch
            .column_by_name("uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let riders = record_batch
            .column_by_name("rider")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let fares = record_batch
            .column_by_name("fare")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        uuids
            .iter()
            .zip(riders.iter())
            .zip(fares.iter())
            .map(|((uuid, rider), fare)| {
                (
                    uuid.unwrap().to_string(),
                    rider.unwrap().to_string(),
                    fare.unwrap(),
                )
            })
            .collect()
    }
    fn zip_path(&self, table_type: &str, log_format: Option<&str>) -> Box<Path> {
        let dir = env!("CARGO_MANIFEST_DIR");
        let data_path = Path::new(dir)
            .join("data/quickstart_trips_table")
            .join(table_type.to_lowercase())
            .join(log_format.unwrap_or_default())
            .join(format!("{}.zip", self.as_ref()));
        data_path.into_boxed_path()
    }

    pub fn path_to_mor_avro(&self) -> String {
        let zip_path = self.zip_path("mor", Some("avro"));
        let path_buf = extract_test_table(zip_path.as_ref()).join(self.as_ref());
        path_buf.to_str().unwrap().to_string()
    }

    pub fn url_to_mor_avro(&self) -> Url {
        let path = self.path_to_mor_avro();
        Url::from_file_path(path).unwrap()
    }
}

#[allow(dead_code)]
#[derive(Debug, EnumString, AsRefStr, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum SampleTable {
    V6ComplexkeygenHivestyle,
    V6Empty,
    V6Nonpartitioned,
    V6NonpartitionedRollback,
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

    fn zip_path(&self, table_type: &str, log_format: Option<&str>) -> Box<Path> {
        let dir = env!("CARGO_MANIFEST_DIR");
        let data_path = Path::new(dir)
            .join("data/sample_table")
            .join(table_type.to_lowercase())
            .join(log_format.unwrap_or_default())
            .join(format!("{}.zip", self.as_ref()));
        data_path.into_boxed_path()
    }

    pub fn path_to_cow(&self) -> String {
        let zip_path = self.zip_path("cow", None);
        let path_buf = extract_test_table(zip_path.as_ref()).join(self.as_ref());
        path_buf.to_str().unwrap().to_string()
    }

    pub fn path_to_mor_parquet(&self) -> String {
        let zip_path = self.zip_path("mor", Some("parquet"));
        let path_buf = extract_test_table(zip_path.as_ref()).join(self.as_ref());
        path_buf.to_str().unwrap().to_string()
    }

    pub fn url_to_cow(&self) -> Url {
        let path = self.path_to_cow();
        Url::from_file_path(path).unwrap()
    }

    pub fn url_to_mor_parquet(&self) -> Url {
        let path = self.path_to_mor_parquet();
        Url::from_file_path(path).unwrap()
    }

    pub fn urls(&self) -> Vec<Url> {
        vec![self.url_to_cow(), self.url_to_mor_parquet()]
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use crate::{QuickstartTripsTable, SampleTable};

    #[test]
    fn quickstart_trips_table_zip_file_should_exist() {
        for t in QuickstartTripsTable::iter() {
            match t {
                QuickstartTripsTable::V6Trips8I1U => {
                    let path = t.zip_path("mor", Some("avro"));
                    assert!(path.exists());
                }
            }
        }
    }
    #[test]
    fn sample_table_zip_file_should_exist() {
        for t in SampleTable::iter() {
            match t {
                SampleTable::V6TimebasedkeygenNonhivestyle => {
                    let path = t.zip_path("cow", None);
                    assert!(path.exists());
                }
                SampleTable::V6NonpartitionedRollback => {
                    let path = t.zip_path("mor", Some("parquet"));
                    assert!(path.exists());
                }
                _ => {
                    let path = t.zip_path("cow", None);
                    assert!(path.exists());
                    let path = t.zip_path("mor", Some("parquet"));
                    assert!(path.exists());
                }
            }
        }
    }
}
