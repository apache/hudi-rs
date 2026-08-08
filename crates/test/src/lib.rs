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

pub mod gold;
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
    /// v9 MOR table, 8 inserts + 4 updates, COMMIT_TIME_ORDERING.
    /// Schema: id INT, name STRING, age INT, ts STRING, city STRING (partitioned by city)
    /// Commit 1: INSERT 8 rows → base .parquet per partition
    /// Commit 2: UPSERT 4 rows (ids 1,3,5,7) → .log files
    #[strum(serialize = "v9_mor_8i4u_commit_time")]
    V9Mor8I4UCommitTime,
    /// v9 MOR non-partitioned table, 3 commits (insert + merge-delete + merge-update).
    /// Schema: id INT, name STRING, price DOUBLE, ts LONG (non-partitioned)
    /// Commit 1: INSERT 7 rows (ids 0-6) → base .parquet
    /// Commit 2: MERGE INTO DELETE 3 rows (ids 0,1,2) → .log file 1 (delete block)
    /// Commit 3: MERGE INTO UPDATE 3 rows (ids 4,5,6) → .log file 2 (avro data block)
    #[strum(serialize = "v9_mor_nonpart_3commits")]
    V9MorNonpart3Commits,
    /// v9 MOR non-partitioned, base file written by COMPACTION so it carries
    /// records from three commits at once.
    ///
    /// Schema: ts LONG, uuid STRING, rider STRING, fare DOUBLE (non-partitioned).
    /// Timeline (see the sibling `.sql`):
    ///   `20260807223522627` deltacommit — INSERT a, b, c, d
    ///   `20260807223526409` deltacommit — UPDATE a  → log
    ///   `20260807223528666` deltacommit — UPDATE b  → log
    ///   `20260807223529164` **commit**  — inline COMPACTION; the base file it
    ///       writes holds a@…526409, b@…528666, c@…522627, d@…522627
    ///   `20260807223530452` deltacommit — UPDATE c  → log
    ///   `20260807223531562` deltacommit — UPDATE d  → log
    ///
    /// Exists so an incremental read has a base file whose records span the
    /// window boundary: admitting the file must not admit every record in it.
    #[strum(serialize = "v9_mor_compacted_incremental")]
    V9MorCompactedIncremental,
    /// v8 MOR non-partitioned, four delta commits, for incremental windows whose
    /// bounds land on a commit's requested or completion time.
    ///
    /// Schema: ts LONG, uuid STRING, rider STRING, fare DOUBLE (non-partitioned).
    /// Table version 8, timeline layout v2 — so every completed instant is named
    /// `{requested}_{completion}` and the two differ. That is what makes the
    /// boundary cases expressible; a v6 table has no completion time to disagree
    /// about.
    ///
    ///   `20260808010716256_20260808010719396` INSERT a, b, c, d
    ///   `20260808010720902_20260808010722082` UPDATE a
    ///   `20260808010723246_20260808010723734` UPDATE b   <- the pivot
    ///   `20260808010724567_20260808010724916` UPDATE c
    ///
    /// `gold_incremental/` holds what Hudi returns for four windows placed
    /// around that third commit: between commits, starting on its requested
    /// time, starting on its completion time, and spanning requested to
    /// completion.
    #[strum(serialize = "v8_mor_boundary_windows")]
    V8MorBoundaryWindows,
    /// v9 MOR non-partitioned, log-only with compacted log block (5 log files).
    #[strum(serialize = "table_log_compaction")]
    MorLayoutLogCompaction,
    /// v9 MOR non-partitioned, log-only (3 log files: insert + update + delete).
    #[strum(serialize = "table_log_only")]
    MorLayoutLogOnly,
    /// v9 MOR non-partitioned, base + 2 log files (update + delete), all column types.
    #[strum(serialize = "table_column_projection")]
    MorLayoutColumnProjection,
    /// v9 MOR non-partitioned, base + 3 log files (update + delete + update), all data types.
    #[strum(serialize = "table_all_data_types")]
    MorLayoutAllDataTypes,
    /// v9 MOR non-partitioned, base + 1 log file containing NULL container elements.
    /// Schema: id INT, arr_null_elem ARRAY<INT>, map_null_val MAP<STRING,INT>,
    /// st_null_field STRUCT<a INT, b STRING>, arr_empty ARRAY<STRING>,
    /// map_empty MAP<STRING,INT>, emptyinit_arr ARRAY<INT>, ts LONG
    /// Commit 1: INSERT 12 rows (ids 1-12) → base .parquet
    /// Commit 2: UPDATE id=1 SET arr_null_elem = array(1, NULL, 3), ts = 101 → .log file
    /// (avro data block whose array carries a NULL element)
    #[strum(serialize = "table_null_containers")]
    MorLayoutNullContainers,
    /// v9 MOR non-partitioned, base + 2 AVRO log files with a CORRUPT tail block.
    ///
    /// Provenance: hudi-internal `TestMORFileSliceLayoutsFixturesV2`
    /// (commit 309a0b287e), self-validating generator that asserts the log
    /// layout before dumping gold.
    /// Schema: key STRING, ts LONG, value STRING, num INT (non-partitioned).
    /// Layout: base .parquet + log.1 (AVRO data block) + log.2 (AVRO data block
    /// followed by appended garbage bytes forming a CORRUPT tail block).
    /// Semantics: the corrupt tail block is skipped during log scan
    /// (`total_corrupt_log_blocks >= 1`) and the valid data is read intact.
    /// gold_data = Spark `SELECT *` snapshot, 4 rows.
    #[strum(serialize = "table_corrupt_tail_block")]
    MorLayoutCorruptTailBlock,
    /// v9 MOR non-partitioned, base + 3 PARQUET-format log files.
    ///
    /// Provenance: hudi-internal `TestMORFileSliceLayoutsFixturesV2`
    /// (commit 309a0b287e); written with `hoodie.logfile.data.block.format=parquet`.
    /// Schema: key STRING, ts LONG, value STRING, num INT (non-partitioned).
    /// Layout: base .parquet + log.1/log.2 (PARQUET_DATA_BLOCK) + log.3
    /// (DELETE block).
    /// Semantics: parquet log blocks are decoded and merged, delete block
    /// applied. gold_data = Spark `SELECT *` snapshot, 3 rows.
    #[strum(serialize = "table_parquet_log_block")]
    MorLayoutParquetLogBlock,
    /// v9 MOR non-partitioned, base + 1 AVRO log file carrying a PARTIAL-update
    /// data block (`IS_PARTIAL=true` block header).
    ///
    /// Provenance: hudi-internal `TestMORFileSliceLayoutsFixturesV2`
    /// (commit 309a0b287e); MERGE INTO updating a column subset with partial
    /// updates enabled.
    /// Schema: key STRING, ts LONG, value STRING, num INT (non-partitioned).
    /// Layout: base .parquet + log.1 (AVRO data block, `IS_PARTIAL=true`).
    /// Semantics: hudi-rs applies IS_PARTIAL / KEEP_VALUES blocks by overlaying
    /// the updated column subset onto the prior record (the D7 refuse-loudly gap
    /// is now closed). gold_data = Spark `SELECT *` snapshot, 4 rows (the
    /// merge-correct truth the applied result must match).
    #[strum(serialize = "table_partial_update")]
    MorLayoutPartialUpdate,
    /// v9 MOR non-partitioned, base + 1 HFILE-format log file
    /// (`HFILE_DATA_BLOCK`).
    ///
    /// Provenance: hudi-internal `TestMORFileSliceLayoutsFixturesV2`
    /// (commit 309a0b287e); written with `hoodie.logfile.data.block.format=hfile`.
    /// Schema: key STRING, ts LONG, value STRING, num INT (non-partitioned).
    /// Layout: base .parquet + log.1 (HFILE_DATA_BLOCK). The fixture carries no
    /// gold_data: it was dumped against a reader with no HFile support, where
    /// the expectation was a loud failure. This crate does read HFile (for the
    /// metadata table), so what this fixture should assert here is an open
    /// question — hence no case wired up for it yet.
    #[strum(serialize = "table_hfile_log_block")]
    MorLayoutHfileLogBlock,

    // -------------------------------------------------------------------------
    // Delete-block orderingVal wrapper-type fixtures (Task 7).
    // Each table: v9 MOR, COMMIT_TIME_ORDERING, NON_PARTITIONED, 4 rows inserted
    // (ids 1-4), ids 3 and 4 deleted via upsert with `_hoodie_is_deleted=true`.
    // After applying the DELETE log block: ids 1 and 2 remain.
    // Schema: id INT32, val STRING, ts <type-under-test> (non-partitioned).
    // Generated by gold Hudi Spark; provenance: hudi-rs-delete-fixtures.
    // -------------------------------------------------------------------------
    /// DELETE block orderingVal type: IntWrapper (Avro int).
    /// ts column is INT32; precombine values in delete block: [4, 3].
    #[strum(serialize = "table_delete_ord_int")]
    MorDeleteOrdInt,
    /// DELETE block orderingVal type: LongWrapper (Avro long).
    /// ts column is INT64; precombine values in delete block: [4000, 3000].
    #[strum(serialize = "table_delete_ord_long")]
    MorDeleteOrdLong,
    /// DELETE block orderingVal type: DoubleWrapper (Avro double).
    /// ts column is FLOAT64; precombine values in delete block: [6.0, 4.5].
    #[strum(serialize = "table_delete_ord_double")]
    MorDeleteOrdDouble,
    /// DELETE block orderingVal type: StringWrapper (Avro string).
    /// ts column is UTF8; precombine values in delete block: ["ord_4", "ord_3"].
    #[strum(serialize = "table_delete_ord_string")]
    MorDeleteOrdString,
    /// DELETE block orderingVal type: DecimalWrapper (precision=30, scale=15).
    /// ts column is DECIMAL(20,4); precombine values in delete block: [4.0, 3.0].
    #[strum(serialize = "table_delete_ord_decimal")]
    MorDeleteOrdDecimal,
    /// DELETE block orderingVal type: TimestampMicrosWrapper (Avro long / epoch µs).
    /// ts column is TIMESTAMP[us, UTC]; precombine values stored as Long (epoch
    /// micros). ENG-42987-adjacent: unwrapAvroValueWrapper returns Long, not
    /// Instant — but the delete block IS written and can be decoded.
    #[strum(serialize = "table_delete_ord_timestamp")]
    MorDeleteOrdTimestamp,

    // -------------------------------------------------------------------------
    // Schema-on-write evolution fixtures (0610 review).
    // Provenance: hudi-internal `TestMORFileSliceLayoutsSchemaEvo` (self-
    // validating generator: asserts v9/MOR/COMMIT_TIME, 1 base + 2 avro logs,
    // and the two data blocks carrying DIFFERENT writer schemas).
    // -------------------------------------------------------------------------
    /// v9 MOR non-partitioned, base + 2 AVRO logs written under DIFFERENT
    /// writer schemas (added column).
    ///
    /// Schema v1: key STRING, ts LONG, val STRING; v2 adds extra STRING.
    /// c1 INSERT k1-k4 (base @ v1) → c2 UPDATE k1 (log1 @ v1, no `extra`)
    /// → ALTER TABLE ADD COLUMNS (extra STRING)
    /// → c3 UPDATE k2 SET val,extra (log2 @ v2).
    /// Merged truth: k1 v1_upd/NULL, k2 v2_upd/x2, k3 v3/NULL, k4 v4/NULL.
    /// gold_data = Spark `SELECT *` snapshot, 4 rows.
    #[strum(serialize = "table_evo_add_col")]
    MorEvoAddCol,
    /// v9 MOR non-partitioned, base + 2 AVRO logs written under DIFFERENT
    /// writer schemas (type promotion int→long and float→double).
    ///
    /// Schema v1: key STRING, ts LONG, num INT, fnum FLOAT; v2 promotes num
    /// to LONG and fnum to DOUBLE (schema-on-write DataFrame upsert).
    /// c1 INSERT k1-k4 (base @ v1) → c2 UPDATE k1 num=11 (log1 @ INT/FLOAT)
    /// → c3 upsert k2 num=5000000000 (beyond i32), fnum=2.25 (log2 @ LONG/DOUBLE).
    /// Merged truth: k1 11/1.25, k2 5000000000/2.25, k3 3/3.75, k4 4/5.0.
    /// gold_data = path-based snapshot (the SQL catalog keeps the
    /// pre-promotion types), 4 rows.
    #[strum(serialize = "table_evo_promotion")]
    MorEvoPromotion,
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
            Self::V9TripsLance => COW_AND_MOR_AVRO,
            // Everything else is a merge-on-read fixture with Avro log blocks.
            // The layout fixtures in particular exist to exercise log-block
            // shapes, so they have no copy-on-write counterpart.
            _ => MOR_AVRO,
        }
    }

    pub fn path(&self, format: TableFormat) -> String {
        let zip_path = self.zip_path_for(format);
        let path_buf = extract_test_table(zip_path.as_ref()).join(self.as_ref());
        path_buf.to_str().unwrap().to_string()
    }

    /// Where this fixture's Spark snapshot lives.
    ///
    /// Beside the table directory, never inside it: a full-table read lists the
    /// table's own directory, and a stray parquet under it is picked up as a
    /// base file (or a partition) and fails the read.
    pub fn gold_dir(&self, format: TableFormat) -> String {
        let zip_path = self.zip_path_for(format);
        extract_test_table(zip_path.as_ref())
            .join("gold_data")
            .to_str()
            .unwrap()
            .to_string()
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

    /// Where this fixture's Spark snapshot lives.
    ///
    /// Beside the table directory, never inside it: a full-table read lists the
    /// table's own directory, and a stray parquet under it is picked up as a
    /// base file (or a partition) and fails the read.
    pub fn gold_dir(&self, format: TableFormat) -> String {
        let zip_path = self.zip_path_for(format);
        extract_test_table(zip_path.as_ref())
            .join("gold_data")
            .to_str()
            .unwrap()
            .to_string()
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
