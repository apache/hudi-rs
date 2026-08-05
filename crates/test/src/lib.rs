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
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::UNIX_EPOCH;
use strum_macros::{AsRefStr, EnumIter, EnumString};
use tempfile::{Builder as TempDirBuilder, tempdir};
use url::Url;
use zip::ZipArchive;

pub mod util;

#[cfg(feature = "datafusion")]
pub mod v9_verification;

static EXTRACT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum TableFormat {
    Cow,
    MorParquet,
    MorAvro,
}

impl TableFormat {
    fn table_type(self) -> &'static str {
        match self {
            Self::Cow => "cow",
            Self::MorParquet | Self::MorAvro => "mor",
        }
    }

    fn log_format(self) -> Option<&'static str> {
        match self {
            Self::Cow => None,
            Self::MorParquet => Some("parquet"),
            Self::MorAvro => Some("avro"),
        }
    }
}

const COW: &[TableFormat] = &[TableFormat::Cow];
const MOR_AVRO: &[TableFormat] = &[TableFormat::MorAvro];
const MOR_PARQUET: &[TableFormat] = &[TableFormat::MorParquet];
const COW_AND_MOR_AVRO: &[TableFormat] = &[TableFormat::Cow, TableFormat::MorAvro];
const COW_AND_MOR_PARQUET: &[TableFormat] = &[TableFormat::Cow, TableFormat::MorParquet];

pub fn extract_test_table(zip_path: &Path) -> PathBuf {
    let _lock = EXTRACT_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("fixture extraction lock should not be poisoned");

    let target_dir = cached_extract_dir(zip_path);
    if target_dir.exists() {
        return target_dir;
    }

    let cache_root = target_dir
        .parent()
        .expect("fixture cache path should have a parent");
    fs::create_dir_all(cache_root)
        .unwrap_or_else(|e| panic!("create fixture cache {}: {e}", cache_root.display()));
    let temp_dir = TempDirBuilder::new()
        .prefix("extract-")
        .tempdir_in(cache_root)
        .unwrap_or_else(|e| panic!("create temp fixture dir in {}: {e}", cache_root.display()));

    extract_zip_to_dir(zip_path, temp_dir.path());

    match fs::rename(temp_dir.path(), &target_dir) {
        Ok(()) => target_dir,
        Err(_) if target_dir.exists() => target_dir,
        Err(e) => panic!(
            "move extracted fixture {} to {}: {e}",
            temp_dir.path().display(),
            target_dir.display()
        ),
    }
}

/// Extracts a fixture into a unique temp directory.
///
/// Use this only for tests that mutate the extracted table in place. Normal
/// readers should use [`extract_test_table`], which reuses a bounded fixture
/// cache keyed by zip path and metadata.
pub fn extract_test_table_fresh(zip_path: &Path) -> PathBuf {
    let temp_dir = tempdir().expect("create temp fixture dir");
    let target_dir = temp_dir.path().to_path_buf();
    extract_zip_to_dir(zip_path, &target_dir);
    let kept_dir = temp_dir.keep();
    debug_assert_eq!(kept_dir, target_dir);
    target_dir
}

fn cached_extract_dir(zip_path: &Path) -> PathBuf {
    let zip_path = zip_path
        .canonicalize()
        .unwrap_or_else(|e| panic!("canonicalize fixture {}: {e}", zip_path.display()));
    let metadata = fs::metadata(&zip_path)
        .unwrap_or_else(|e| panic!("stat fixture {}: {e}", zip_path.display()));
    let modified = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();

    let mut hasher = DefaultHasher::new();
    zip_path.hash(&mut hasher);
    metadata.len().hash(&mut hasher);
    modified.hash(&mut hasher);

    std::env::temp_dir()
        .join("hudi-rs-test-fixtures")
        .join(format!("{:016x}", hasher.finish()))
}

fn extract_zip_to_dir(zip_path: &Path, target_dir: &Path) {
    let archive =
        fs::read(zip_path).unwrap_or_else(|e| panic!("read fixture {}: {e}", zip_path.display()));
    let mut zip = ZipArchive::new(Cursor::new(archive))
        .unwrap_or_else(|e| panic!("open fixture zip {}: {e}", zip_path.display()));
    zip.extract(target_dir).unwrap_or_else(|e| {
        panic!(
            "extract fixture {} to {}: {e}",
            zip_path.display(),
            target_dir.display()
        )
    });
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

    fn zip_path_for(&self, format: TableFormat) -> Box<Path> {
        self.zip_path(format.table_type(), format.log_format())
    }

    pub fn available_formats(&self) -> &'static [TableFormat] {
        match self {
            Self::V6Trips8I1U | Self::V6Trips8I3D | Self::V8Trips8I3U1D => MOR_AVRO,
            Self::V9TripsLance => COW_AND_MOR_AVRO,
        }
    }

    pub fn path(&self, format: TableFormat) -> String {
        let zip_path = self.zip_path_for(format);
        let path_buf = extract_test_table(zip_path.as_ref()).join(self.as_ref());
        path_buf.to_str().unwrap().to_string()
    }

    pub fn path_to_cow(&self) -> String {
        self.path(TableFormat::Cow)
    }

    pub fn url_to_cow(&self) -> Url {
        let path = self.path_to_cow();
        Url::from_file_path(path).unwrap()
    }

    pub fn path_to_mor_avro(&self) -> String {
        self.path(TableFormat::MorAvro)
    }

    pub fn url(&self, format: TableFormat) -> Url {
        let path = self.path(format);
        Url::from_file_path(path).unwrap()
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

    fn zip_path_for(&self, format: TableFormat) -> Box<Path> {
        self.zip_path(format.table_type(), format.log_format())
    }

    pub fn available_formats(&self) -> &'static [TableFormat] {
        match self {
            Self::V6TimebasedkeygenNonhivestyle
            | Self::V8ComplexkeygenHivestyle
            | Self::V8Empty
            | Self::V8Nonpartitioned
            | Self::V8SimplekeygenHivestyleNoMetafields
            | Self::V8SimplekeygenNonhivestyle
            | Self::V9TimebasedkeygenEpochmillis
            | Self::V9TimebasedkeygenUnixtimestamp
            | Self::V9LanceNonpartitioned
            | Self::V9LanceTxnsNonpart
            | Self::V9LanceTxnsSimple => COW,

            Self::V6NonpartitionedRollback => MOR_PARQUET,

            Self::V9NonpartitionedRollback | Self::V9LanceNonhivestyle => MOR_AVRO,

            Self::V9TimebasedkeygenNonhivestyle
            | Self::V9TxnsComplexMeta
            | Self::V9TxnsComplexNometa
            | Self::V9TxnsNonpartMeta
            | Self::V9TxnsNonpartNometa
            | Self::V9TxnsSimpleMeta
            | Self::V9TxnsSimpleNometa
            | Self::V9TxnsSimpleOverwrite => COW_AND_MOR_AVRO,

            Self::V6ComplexkeygenHivestyle
            | Self::V6Empty
            | Self::V6Nonpartitioned
            | Self::V6SimplekeygenHivestyleNoMetafields
            | Self::V6SimplekeygenNonhivestyle
            | Self::V6SimplekeygenNonhivestyleOverwritetable => COW_AND_MOR_PARQUET,
        }
    }

    pub fn path(&self, format: TableFormat) -> String {
        let zip_path = self.zip_path_for(format);
        let path_buf = extract_test_table(zip_path.as_ref()).join(self.as_ref());
        path_buf.to_str().unwrap().to_string()
    }

    pub fn path_fresh(&self, format: TableFormat) -> String {
        let zip_path = self.zip_path_for(format);
        let path_buf = extract_test_table_fresh(zip_path.as_ref()).join(self.as_ref());
        path_buf.to_str().unwrap().to_string()
    }

    pub fn path_to_cow(&self) -> String {
        self.path(TableFormat::Cow)
    }

    pub fn path_to_cow_fresh(&self) -> String {
        self.path_fresh(TableFormat::Cow)
    }

    pub fn path_to_mor_parquet(&self) -> String {
        self.path(TableFormat::MorParquet)
    }

    pub fn path_to_mor_parquet_fresh(&self) -> String {
        self.path_fresh(TableFormat::MorParquet)
    }

    pub fn url_to_cow(&self) -> Url {
        let path = self.path_to_cow();
        Url::from_file_path(path).unwrap()
    }

    pub fn path_to_mor_avro(&self) -> String {
        self.path(TableFormat::MorAvro)
    }

    pub fn url(&self, format: TableFormat) -> Url {
        let path = self.path(format);
        Url::from_file_path(path).unwrap()
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
        self.available_formats()
            .iter()
            .map(|format| self.url(*format))
            .collect()
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
            for format in t.available_formats() {
                let path = t.zip_path_for(*format);
                assert!(
                    path.exists(),
                    "missing fixture {path:?} for {t:?} {format:?}"
                );
            }
        }
    }

    #[test]
    fn sample_table_zip_file_should_exist() {
        for t in SampleTable::iter() {
            for format in t.available_formats() {
                let path = t.zip_path_for(*format);
                assert!(
                    path.exists(),
                    "missing fixture {path:?} for {t:?} {format:?}"
                );
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
