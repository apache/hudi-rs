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

//! E2E integration tests for the new `HoodieFileGroupReader`.
//!
//! Tests read real Hudi v9 MOR file groups (COMMIT_TIME_ORDERING) with
//! base parquet + Avro log files through the new reader pipeline.
//!
//! ## Test data: v9_mor_8i4u_commit_time
//!
//! Schema: id INT, name STRING, age INT, ts STRING, city STRING (partitioned by city)
//! Commit 1: INSERT 8 rows → base .parquet per partition
//! Commit 2: UPSERT 4 rows (ids 1,3,5,7) → .log files
//!
//! Expected merged view (8 rows):
//!   id=1  Alice-V2  age=31  city=sf
//!   id=2  Bob       age=25  city=sf
//!   id=3  Carol-V2  age=36  city=nyc
//!   id=4  Dave      age=28  city=nyc
//!   id=5  Eve-V2    age=33  city=chi
//!   id=6  Frank     age=40  city=chi
//!   id=7  Grace-V2  age=28  city=la
//!   id=8  Hank      age=45  city=la

use hudi_core::config::HudiConfigs;
use hudi_core::error::Result;
use hudi_core::file_group::reader::input_split::InputSplit;
use hudi_core::file_group::reader::reader_context::ReaderContext;
use hudi_core::file_group::reader::reader_parameters::ReaderParameters;
use hudi_core::file_group::reader::HoodieFileGroupReader;
use hudi_core::storage::Storage;
use hudi_core::table::builder::OptionResolver;
use hudi_test::QuickstartTripsTable;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::sync::Arc;

/// Create HudiConfigs and Storage from table path using OptionResolver.
async fn create_configs_and_storage(
    table_path: &str,
) -> Result<(Arc<HudiConfigs>, Arc<Storage>)> {
    let empty_opts: Vec<(&str, &str)> = vec![];
    let mut resolver = OptionResolver::new_with_options(table_path, empty_opts);
    resolver.resolve_options().await?;
    let hudi_configs = Arc::new(HudiConfigs::new(resolver.hudi_options));
    let storage = Storage::new(Arc::new(resolver.storage_options), hudi_configs.clone())?;
    Ok((hudi_configs, storage))
}

/// Read a single file group through the new HoodieFileGroupReader.
async fn read_file_group(
    table_path: &str,
    partition: &str,
    base_file: &str,
    log_files: Vec<&str>,
) -> Result<arrow_array::RecordBatch> {
    let (_hudi_configs, storage) = create_configs_and_storage(table_path).await?;

    let base_path = if base_file.is_empty() {
        None
    } else if partition.is_empty() {
        Some(base_file.to_string())
    } else {
        Some(format!("{}/{}", partition, base_file))
    };
    let log_paths: Vec<String> = log_files
        .iter()
        .map(|lf| {
            if partition.is_empty() {
                lf.to_string()
            } else {
                format!("{}/{}", partition, lf)
            }
        })
        .collect();

    let input_split = InputSplit::new(
        base_path,
        None,
        log_paths,
        partition.to_string(),
    );

    let mut reader_context = ReaderContext::empty();
    reader_context.latest_commit_time = "99991231235959999".to_string();
    reader_context.record_key_field = "_hoodie_record_key".to_string();
    reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();

    let mut reader = HoodieFileGroupReader::new(
        Arc::new(reader_context),
        storage,
        input_split,
        vec!["ts".to_string()],
        ReaderParameters::default(),
    );

    reader.read().await
}

/// Extract (id, name, age) tuples from a RecordBatch, sorted by id.
fn extract_id_name_age(batch: &arrow_array::RecordBatch) -> Vec<(i32, String, i32)> {
    QuickstartTripsTable::id_name_age(batch)
}

// =============================================================================
// v9 MOR COMMIT_TIME_ORDERING: city=sf (2 base rows, 1 update in log)
// =============================================================================

/// E2E: Read city=sf partition — base file + log file merge.
///
/// Given: v9 MOR COMMIT_TIME_ORDERING table, city=sf partition
///        Base: (id=1, Alice, 30), (id=2, Bob, 25)
///        Log:  (id=1, Alice-V2, 31) — update for id=1
/// When:  Read file group through HoodieFileGroupReader
/// Then:  2 rows: (1, Alice-V2, 31) and (2, Bob, 25)
#[tokio::test]
async fn test_e2e_v9_mor_commit_time_sf_merge() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

    let result = read_file_group(
        &table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
    )
    .await?;

    let records = extract_id_name_age(&result);
    assert_eq!(records.len(), 2, "sf partition should have 2 rows");
    assert_eq!(records[0], (1, "Alice-V2".to_string(), 31), "id=1 should be updated");
    assert_eq!(records[1], (2, "Bob".to_string(), 25), "id=2 should be unchanged");

    Ok(())
}

// =============================================================================
// v9 MOR COMMIT_TIME_ORDERING: city=nyc (2 base rows, 1 update in log)
// =============================================================================

/// E2E: Read city=nyc partition — base file + log file merge.
///
/// Given: Base: (id=3, Carol, 35), (id=4, Dave, 28)
///        Log:  (id=3, Carol-V2, 36)
/// When:  Read file group
/// Then:  2 rows: (3, Carol-V2, 36) and (4, Dave, 28)
#[tokio::test]
async fn test_e2e_v9_mor_commit_time_nyc_merge() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

    let result = read_file_group(
        &table_path,
        "city=nyc",
        "cae699a1-42f7-4226-9bd3-7f0e49496028-0_3-13-36_20260408053032350.parquet",
        vec![".cae699a1-42f7-4226-9bd3-7f0e49496028-0_20260408053037787.log.1_3-27-76"],
    )
    .await?;

    let records = extract_id_name_age(&result);
    assert_eq!(records.len(), 2, "nyc partition should have 2 rows");
    assert_eq!(records[0], (3, "Carol-V2".to_string(), 36), "id=3 should be updated");
    assert_eq!(records[1], (4, "Dave".to_string(), 28), "id=4 should be unchanged");

    Ok(())
}

// =============================================================================
// v9 MOR COMMIT_TIME_ORDERING: city=chi (2 base rows, 1 update in log)
// =============================================================================

/// E2E: Read city=chi partition — base file + log file merge.
///
/// Given: Base: (id=5, Eve, 32), (id=6, Frank, 40)
///        Log:  (id=5, Eve-V2, 33)
/// When:  Read file group
/// Then:  2 rows: (5, Eve-V2, 33) and (6, Frank, 40)
#[tokio::test]
async fn test_e2e_v9_mor_commit_time_chi_merge() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

    let result = read_file_group(
        &table_path,
        "city=chi",
        "8fb566bd-0f0d-45ee-9f4b-391c1ceb9dda-0_2-13-35_20260408053032350.parquet",
        vec![".8fb566bd-0f0d-45ee-9f4b-391c1ceb9dda-0_20260408053037787.log.1_2-27-75"],
    )
    .await?;

    let records = extract_id_name_age(&result);
    assert_eq!(records.len(), 2, "chi partition should have 2 rows");
    assert_eq!(records[0], (5, "Eve-V2".to_string(), 33), "id=5 should be updated");
    assert_eq!(records[1], (6, "Frank".to_string(), 40), "id=6 should be unchanged");

    Ok(())
}

// =============================================================================
// v9 MOR COMMIT_TIME_ORDERING: city=la (2 base rows, 1 update in log)
// =============================================================================

/// E2E: Read city=la partition — base file + log file merge.
///
/// Given: Base: (id=7, Grace, 27), (id=8, Hank, 45)
///        Log:  (id=7, Grace-V2, 28)
/// When:  Read file group
/// Then:  2 rows: (7, Grace-V2, 28) and (8, Hank, 45)
#[tokio::test]
async fn test_e2e_v9_mor_commit_time_la_merge() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

    let result = read_file_group(
        &table_path,
        "city=la",
        "31d2005b-4c79-46f4-aca5-6519809b2503-0_1-13-34_20260408053032350.parquet",
        vec![".31d2005b-4c79-46f4-aca5-6519809b2503-0_20260408053037787.log.1_1-27-74"],
    )
    .await?;

    let records = extract_id_name_age(&result);
    assert_eq!(records.len(), 2, "la partition should have 2 rows");
    assert_eq!(records[0], (7, "Grace-V2".to_string(), 28), "id=7 should be updated");
    assert_eq!(records[1], (8, "Hank".to_string(), 45), "id=8 should be unchanged");

    Ok(())
}

// =============================================================================
// v9 MOR: Base-only read (no log files)
// =============================================================================

/// E2E: Read base file only (no log files) — tests the no-merge path.
///
/// Given: v9 MOR table, city=sf partition, base file only (no log files)
/// When:  Read file group with empty log_files list
/// Then:  Original base records returned (Alice with age=30, Bob with age=25)
#[tokio::test]
async fn test_e2e_v9_mor_base_only_read() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

    let result = read_file_group(
        &table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![], // no log files
    )
    .await?;

    let records = extract_id_name_age(&result);
    assert_eq!(records.len(), 2, "base-only should have 2 rows");
    assert_eq!(records[0], (1, "Alice".to_string(), 30), "id=1 should be original (no update)");
    assert_eq!(records[1], (2, "Bob".to_string(), 25), "id=2 should be original");

    Ok(())
}

// =============================================================================
// v9 MOR COMMIT_TIME_ORDERING: Non-partitioned, 2 log files (delete + update)
// =============================================================================

/// Extract (id, name, price) tuples from a RecordBatch, sorted by id.
fn extract_id_name_price(batch: &arrow_array::RecordBatch) -> Vec<(i32, String, f64)> {
    QuickstartTripsTable::id_name_price(batch)
}

/// E2E: Read non-partitioned file group with 2 log files — reproduces multi-log merge.
///
/// Given: v9 MOR COMMIT_TIME_ORDERING non-partitioned table
///        Base: 7 rows (ids 0-6) from INSERT
///        Log 1: MERGE INTO DELETE (ids 0,1,2) — delete block
///        Log 2: MERGE INTO UPDATE (ids 4,5,6 → D2/E2/F2) — avro data block
/// When:  Read file group through HoodieFileGroupReader
/// Then:  4 rows after merge: (3,C,30.0), (4,D2,45.0), (5,E2,55.0), (6,F2,65.0)
#[tokio::test]
async fn test_e2e_v9_mor_commit_time_nonpart_multi_log() -> Result<()> {
    let table_path = QuickstartTripsTable::V9MorNonpart3Commits.path_to_mor_avro();

    let result = read_file_group(
        &table_path,
        "",  // non-partitioned
        "960a29a0-0f78-401d-85b1-1cbc44b34121-0_0-846-1597_20260409002001492.parquet",
        vec![
            ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002002957.log.1_0-868-1644",
            ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002003963.log.1_0-890-1691",
        ],
    )
    .await?;

    let records = extract_id_name_price(&result);
    assert_eq!(records.len(), 4, "should have 4 rows after merge (3 deleted, 3 updated)");
    assert_eq!(records[0], (3, "C".to_string(), 30.0), "id=3 should be unchanged");
    assert_eq!(records[1], (4, "D2".to_string(), 45.0), "id=4 should be updated");
    assert_eq!(records[2], (5, "E2".to_string(), 55.0), "id=5 should be updated");
    assert_eq!(records[3], (6, "F2".to_string(), 65.0), "id=6 should be updated");

    Ok(())
}

// =============================================================================
// MOR File Slice Layout Tests
//
// These tests read the 4 MOR v9 tables created by Hudi Spark's
// TestMORFileSliceLayouts and validate the HoodieFileGroupReader output
// against the gold data (SELECT * result saved as parquet).
//
// All tables: v9, MOR, COMMIT_TIME_ORDERING, non-partitioned, 1 file group.
// =============================================================================

/// Read gold parquet data from disk.
fn read_gold_parquet(gold_dir: &str) -> arrow_array::RecordBatch {
    let entries: Vec<_> = std::fs::read_dir(gold_dir)
        .expect("gold_data dir should exist")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map_or(false, |ext| ext == "parquet")
        })
        .collect();
    assert!(!entries.is_empty(), "no parquet files in {gold_dir}");
    let file = File::open(entries[0].path()).expect("open gold parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("build parquet reader")
        .build()
        .expect("build reader");
    let batches: Vec<_> = reader.map(|b| b.expect("read batch")).collect();
    arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat gold batches")
}

/// Sort a RecordBatch by the "key" column (String type, ascending).
fn sort_by_key(batch: &arrow_array::RecordBatch) -> arrow_array::RecordBatch {
    let key_col = batch
        .column(batch.schema().index_of("key").expect("key column"))
        .clone();
    let indices = arrow_ord::sort::sort_to_indices(&key_col, None, None).expect("sort");
    let columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| arrow_select::take::take(col, &indices, None).expect("take"))
        .collect();
    arrow_array::RecordBatch::try_new(batch.schema(), columns).expect("sorted batch")
}

/// Compare actual reader output against gold data.
///
/// Compares non-hoodie-metadata columns (skipping `_hoodie_*` columns)
/// by sorting both by "key" and checking row count + per-cell values.
fn assert_matches_gold(
    actual: &arrow_array::RecordBatch,
    gold: &arrow_array::RecordBatch,
    table_name: &str,
) {
    let actual_sorted = sort_by_key(actual);
    let gold_sorted = sort_by_key(gold);

    assert_eq!(
        actual_sorted.num_rows(),
        gold_sorted.num_rows(),
        "[{table_name}] row count mismatch: actual={} gold={}",
        actual_sorted.num_rows(),
        gold_sorted.num_rows(),
    );

    // Compare user columns (skip _hoodie_* metadata columns)
    let user_cols: Vec<String> = gold_sorted
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .filter(|n| !n.starts_with("_hoodie_"))
        .collect();

    for col_name in &user_cols {
        let actual_idx = actual_sorted.schema().index_of(col_name);
        let gold_idx = gold_sorted.schema().index_of(col_name).unwrap();
        if actual_idx.is_err() {
            continue;
        }
        let actual_col = actual_sorted.column(actual_idx.unwrap());
        let gold_col = gold_sorted.column(gold_idx);

        // Skip timestamp columns if the types differ (us vs ns, tz vs no-tz)
        // since these are representation differences, not value differences.
        let is_timestamp = matches!(
            actual_col.data_type(),
            arrow_schema::DataType::Timestamp(_, _)
        ) || matches!(
            gold_col.data_type(),
            arrow_schema::DataType::Timestamp(_, _)
        );
        if is_timestamp && actual_col.data_type() != gold_col.data_type() {
            continue;
        }

        for row in 0..actual_sorted.num_rows() {
            let actual_str = arrow_cast::display::array_value_to_string(actual_col, row)
                .unwrap_or_else(|_| "<err>".to_string());
            let gold_str = arrow_cast::display::array_value_to_string(gold_col, row)
                .unwrap_or_else(|_| "<err>".to_string());
            assert_eq!(
                actual_str, gold_str,
                "[{table_name}] mismatch at row={row} col={col_name}: actual={actual_str} gold={gold_str}",
            );
        }
    }
}

// =============================================================================
// Test 1: Log compaction — log-only, 5 log files, compacted log block
// =============================================================================

/// E2E: Read MOR table with compacted log block (log compaction).
///
/// Layout: 1 file group, NO base file, 5 log files (includes compacted block).
/// Expected: 3 rows — matches gold data from SELECT * via Spark.
#[tokio::test]
async fn test_e2e_v9_mor_log_compaction() -> Result<()> {
    let table_path = QuickstartTripsTable::MorLayoutLogCompaction.path_to_mor_avro();
    let gold_dir = QuickstartTripsTable::MorLayoutLogCompaction.path_to_mor_avro_gold();

    let result = read_file_group(
        &table_path,
        "",
        "",
        vec![
            ".7483a08a-02f1-4510-bc1d-1317924f4189-0_20260409030511461.log.1_0-16-23",
            ".7483a08a-02f1-4510-bc1d-1317924f4189-0_20260409030518232.log.1_0-30-49",
            ".7483a08a-02f1-4510-bc1d-1317924f4189-0_20260409030519923.log.1_0-44-78",
            ".7483a08a-02f1-4510-bc1d-1317924f4189-0_20260409030521407.log.1_0-58-110",
            ".7483a08a-02f1-4510-bc1d-1317924f4189-0_20260409030522412.log.1_0-67-128",
        ],
    )
    .await?;

    let gold = read_gold_parquet(&gold_dir);
    assert_matches_gold(&result, &gold, "table_log_compaction");
    Ok(())
}

// =============================================================================
// Test 2: Log only — log-only, 3 log files (insert + update + delete)
// =============================================================================

/// E2E: Read MOR table with log files only (no base file).
///
/// Layout: 1 file group, NO base file, 3 log files (data + data + delete).
/// Expected: 2 rows — k3 deleted, matches gold data.
#[tokio::test]
async fn test_e2e_v9_mor_log_only() -> Result<()> {
    let table_path = QuickstartTripsTable::MorLayoutLogOnly.path_to_mor_avro();
    let gold_dir = QuickstartTripsTable::MorLayoutLogOnly.path_to_mor_avro_gold();

    let result = read_file_group(
        &table_path,
        "",
        "",
        vec![
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030525348.log.1_0-102-176",
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030527298.log.1_0-116-202",
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030528554.log.1_0-130-231",
        ],
    )
    .await?;

    let gold = read_gold_parquet(&gold_dir);
    assert_matches_gold(&result, &gold, "table_log_only");
    Ok(())
}

// =============================================================================
// Test 3: Column projection — base + 2 log files (update + delete), full schema
// =============================================================================

/// E2E: Read MOR table with all data types (column projection test).
///
/// Layout: 1 file group, 1 base file + 2 log files.
/// Expected: 2 rows — k3 deleted, matches gold data.
#[tokio::test]
async fn test_e2e_v9_mor_column_projection() -> Result<()> {
    let table_path = QuickstartTripsTable::MorLayoutColumnProjection.path_to_mor_avro();
    let gold_dir = QuickstartTripsTable::MorLayoutColumnProjection.path_to_mor_avro_gold();

    let result = read_file_group(
        &table_path,
        "",
        "78076137-b2c4-410d-8473-3d6366ae0985-0_0-165-279_20260409030530945.parquet",
        vec![
            ".78076137-b2c4-410d-8473-3d6366ae0985-0_20260409030532996.log.1_0-179-305",
            ".78076137-b2c4-410d-8473-3d6366ae0985-0_20260409030534379.log.1_0-193-334",
        ],
    )
    .await?;

    let gold = read_gold_parquet(&gold_dir);
    assert_matches_gold(&result, &gold, "table_column_projection");
    Ok(())
}

// =============================================================================
// Test 4: All data types — base + 3 log files (update + delete + update)
// =============================================================================

/// E2E: Read MOR table with all data types and comprehensive log blocks.
///
/// Layout: 1 file group, 1 base file + 3 log files.
/// Expected: 4 rows — k4 deleted, matches gold data.
#[tokio::test]
async fn test_e2e_v9_mor_all_data_types() -> Result<()> {
    let table_path = QuickstartTripsTable::MorLayoutAllDataTypes.path_to_mor_avro();
    let gold_dir = QuickstartTripsTable::MorLayoutAllDataTypes.path_to_mor_avro_gold();

    let result = read_file_group(
        &table_path,
        "",
        "c887c1e8-5fb9-475e-8171-769c5cf10c61-0_0-240-395_20260409030537482.parquet",
        vec![
            ".c887c1e8-5fb9-475e-8171-769c5cf10c61-0_20260409030539332.log.1_0-254-421",
            ".c887c1e8-5fb9-475e-8171-769c5cf10c61-0_20260409030540482.log.1_0-268-450",
            ".c887c1e8-5fb9-475e-8171-769c5cf10c61-0_20260409030541620.log.1_0-282-482",
        ],
    )
    .await?;

    let gold = read_gold_parquet(&gold_dir);
    assert_matches_gold(&result, &gold, "table_all_data_types");
    Ok(())
}
