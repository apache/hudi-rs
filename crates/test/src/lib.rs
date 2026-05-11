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
use zip::ZipArchive;

pub mod util;

#[cfg(feature = "datafusion")]
pub mod v9_verification;

pub fn extract_test_table(zip_path: &Path) -> PathBuf {
    let target_dir = tempdir().unwrap().path().to_path_buf();
    let archive = fs::read(zip_path).unwrap();
    ZipArchive::new(Cursor::new(archive))
        .unwrap()
        .extract(&target_dir)
        .unwrap();
    target_dir
}

#[allow(dead_code)]
#[derive(Debug, EnumString, AsRefStr, EnumIter)]
pub enum QuickstartTripsTable {
    #[strum(serialize = "v6_trips_8i1u")]
    V6Trips8I1U,
    #[strum(serialize = "v6_trips_8i3d")]
    V6Trips8I3D,
    #[strum(serialize = "v8_trips_8i3u1d")]
    V8Trips8I3U1D,
    #[strum(serialize = "v9_trips_lance")]
    V9TripsLance,
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

    pub fn path_to_cow(&self) -> String {
        let zip_path = self.zip_path("cow", None);
        let path_buf = extract_test_table(zip_path.as_ref()).join(self.as_ref());
        path_buf.to_str().unwrap().to_string()
    }

    pub fn url_to_cow(&self) -> Url {
        let path = self.path_to_cow();
        Url::from_file_path(path).unwrap()
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
    V8ComplexkeygenHivestyle,
    V8Empty,
    V8Nonpartitioned,
    V8SimplekeygenHivestyleNoMetafields,
    V8SimplekeygenNonhivestyle,
    V9NonpartitionedRollback,
    V9TimebasedkeygenEpochmillis,
    V9TimebasedkeygenNonhivestyle,
    V9TimebasedkeygenUnixtimestamp,
    V9TxnsComplexMeta,
    V9TxnsComplexNometa,
    V9TxnsNonpartMeta,
    V9TxnsNonpartNometa,
    V9TxnsSimpleMeta,
    V9TxnsSimpleNometa,
    V9TxnsSimpleOverwrite,
    V9LanceNonpartitioned,
    V9LanceNonhivestyle,
    V9LanceTxnsNonpart,
    V9LanceTxnsSimple,
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

    pub fn path_to_mor_avro(&self) -> String {
        let zip_path = self.zip_path("mor", Some("avro"));
        let path_buf = extract_test_table(zip_path.as_ref()).join(self.as_ref());
        path_buf.to_str().unwrap().to_string()
    }

    pub fn url_to_mor_parquet(&self) -> Url {
        let path = self.path_to_mor_parquet();
        Url::from_file_path(path).unwrap()
    }

    pub fn url_to_mor_avro(&self) -> Url {
        let path = self.path_to_mor_avro();
        Url::from_file_path(path).unwrap()
    }

    pub fn urls(&self) -> Vec<Url> {
        vec![self.url_to_cow(), self.url_to_mor_parquet()]
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use crate::{QuickstartTripsTable, SampleTable, extract_test_table};

    fn collect_regular_files(dir: &std::path::Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                collect_regular_files(&path, files);
            } else {
                files.push(path);
            }
        }
    }

    #[test]
    fn quickstart_trips_table_zip_file_should_exist() {
        for t in QuickstartTripsTable::iter() {
            match t {
                QuickstartTripsTable::V6Trips8I1U => {
                    let path = t.zip_path("mor", Some("avro"));
                    assert!(path.exists());
                }
                QuickstartTripsTable::V6Trips8I3D => {
                    let path = t.zip_path("mor", Some("avro"));
                    assert!(path.exists());
                }
                QuickstartTripsTable::V8Trips8I3U1D => {
                    let path = t.zip_path("mor", Some("avro"));
                    assert!(path.exists());
                }
                QuickstartTripsTable::V9TripsLance => {
                    let path = t.zip_path("cow", None);
                    assert!(path.exists());
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
                SampleTable::V9NonpartitionedRollback => {
                    let path = t.zip_path("mor", Some("avro"));
                    assert!(path.exists());
                }
                SampleTable::V9TimebasedkeygenEpochmillis
                | SampleTable::V9TimebasedkeygenUnixtimestamp
                | SampleTable::V9LanceNonpartitioned
                | SampleTable::V9LanceTxnsNonpart
                | SampleTable::V9LanceTxnsSimple => {
                    let path = t.zip_path("cow", None);
                    assert!(path.exists());
                }
                SampleTable::V9LanceNonhivestyle => {
                    let path = t.zip_path("mor", Some("avro"));
                    assert!(path.exists());
                }
                ref table if table.as_ref().starts_with("v9") => {
                    let path = t.zip_path("cow", None);
                    assert!(path.exists());
                    let path = t.zip_path("mor", Some("avro"));
                    assert!(path.exists());
                }
                ref table if table.as_ref().starts_with("v8") => {
                    let path = t.zip_path("cow", None);
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

    fn assert_lance_fixture_valid(table_dir: &std::path::Path) {
        let mut files = Vec::new();
        collect_regular_files(table_dir, &mut files);

        assert!(
            files.iter().any(|path| path
                .extension()
                .is_some_and(|extension| extension == "lance")),
            "Lance fixture should contain .lance base files"
        );
        assert!(
            files.iter().all(|path| path
                .extension()
                .is_none_or(|extension| extension != "parquet")),
            "Lance fixture should not contain .parquet files"
        );
        assert!(
            files.iter().all(|path| path.file_name().is_none_or(|name| {
                let name = name.to_string_lossy();
                !matches!(name.as_ref(), ".DS_Store") && !name.ends_with(".crc")
            })),
            "Lance fixture should not contain local checksum or platform files"
        );

        let props = std::fs::read_to_string(table_dir.join(".hoodie/hoodie.properties")).unwrap();
        assert!(
            props
                .lines()
                .any(|line| line == "hoodie.table.base.file.format=LANCE")
        );
    }

    #[test]
    fn v9_lance_nonpartitioned_fixture_contains_only_lance_base_files() {
        let zip_path = SampleTable::V9LanceNonpartitioned.zip_path("cow", None);
        let table_dir =
            extract_test_table(zip_path.as_ref()).join(SampleTable::V9LanceNonpartitioned.as_ref());
        assert_lance_fixture_valid(&table_dir);
    }

    #[test]
    fn v9_lance_cow_fixtures_are_valid() {
        for table in [
            SampleTable::V9LanceTxnsSimple,
            SampleTable::V9LanceTxnsNonpart,
        ] {
            let zip_path = table.zip_path("cow", None);
            let table_dir = extract_test_table(zip_path.as_ref()).join(table.as_ref());
            assert_lance_fixture_valid(&table_dir);
        }
    }

    #[test]
    fn v9_lance_mor_fixtures_are_valid() {
        let table = SampleTable::V9LanceNonhivestyle;
        let zip_path = table.zip_path("mor", Some("avro"));
        let table_dir = extract_test_table(zip_path.as_ref()).join(table.as_ref());
        assert_lance_fixture_valid(&table_dir);

        let mut files = Vec::new();
        collect_regular_files(&table_dir, &mut files);
        let has_log_files = files
            .iter()
            .any(|path| path.to_string_lossy().contains(".log."));
        assert!(
            has_log_files,
            "MOR Lance fixture {table:?} should contain .log files"
        );
    }

    #[test]
    fn v9_trips_lance_cow_and_mor_fixtures_are_valid() {
        // V9TripsLance lives under QuickstartTripsTable because it has the
        // trips schema, but the fixture itself is still a Lance-format Hudi
        // table — same shape checks apply.
        let table = QuickstartTripsTable::V9TripsLance;

        let cow_dir = extract_test_table(table.zip_path("cow", None).as_ref()).join(table.as_ref());
        assert_lance_fixture_valid(&cow_dir);

        let mor_dir =
            extract_test_table(table.zip_path("mor", Some("avro")).as_ref()).join(table.as_ref());
        assert_lance_fixture_valid(&mor_dir);
        let mut files = Vec::new();
        collect_regular_files(&mor_dir, &mut files);
        assert!(
            files
                .iter()
                .any(|path| path.to_string_lossy().contains(".log.")),
            "MOR Lance trips fixture should contain .log files"
        );
    }
}
