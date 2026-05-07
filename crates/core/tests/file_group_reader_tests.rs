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
use hudi_core::config::table::HudiTableConfig;
use hudi_core::error::Result;
use hudi_core::file_group::reader::input_split::InputSplit;
use hudi_core::file_group::reader::reader_context::ReaderContext;
use hudi_core::file_group::reader::reader_parameters::ReaderParameters;
use hudi_core::file_group::reader::HoodieFileGroupReader;
use hudi_core::storage::Storage;
use hudi_core::table::builder::OptionResolver;
use hudi_test::QuickstartTripsTable;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::sync::Arc;
use hudi_core::expression::predicates::predicates_factory;
use hudi_core::expression::{Expression, Literal, NameReference, Predicate};
use std::sync::Arc as StdArc;

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
    reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
    reader_context.table_config.insert(
        HudiTableConfig::PrecombineField.as_ref().to_string(),
        "ts".to_string(),
    );
    reader_context.rebuild_record_context(partition.to_string());

    let mut reader = HoodieFileGroupReader::new(
        Arc::new(reader_context),
        storage,
        input_split,
        ReaderParameters::default(),
        None, // data_schema — None means no projection
        None, // requested_schema
    );

    reader.read().await
}

/// Read a file group with column projection.
///
/// Passes `data_schema` + `requested_schema` to `HoodieFileGroupReader::new()`,
/// which mirrors Java's constructor: it creates the schema handler internally,
/// calls `prepare_required_schema()` to add mandatory merge fields, and sets
/// the `output_converter` for final projection.
async fn read_file_group_with_projection(
    table_path: &str,
    partition: &str,
    base_file: &str,
    log_files: Vec<&str>,
    requested_schema: SchemaRef,
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
        base_path.clone(),
        None,
        log_paths,
        partition.to_string(),
    );

    let mut reader_context = ReaderContext::empty();
    reader_context.latest_commit_time = "99991231235959999".to_string();
    reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
    reader_context.table_config.insert(
        HudiTableConfig::PrecombineField.as_ref().to_string(),
        "ts".to_string(),
    );
    reader_context.rebuild_record_context(partition.to_string());

    // Read table schema from base file parquet metadata.
    let data_schema: Option<SchemaRef> = if let Some(ref bp) = base_path {
        storage
            .get_parquet_file_schema(bp)
            .await
            .ok()
            .map(|s| Arc::new(s) as SchemaRef)
    } else {
        None
    };

    // Pass data_schema + requested_schema to the constructor.
    // The constructor creates the schema handler, calls prepare_required_schema,
    // and sets the output_converter — matching Java's pattern.
    let mut reader = HoodieFileGroupReader::new(
        Arc::new(reader_context),
        storage,
        input_split,
        ReaderParameters::default(),
        data_schema,
        Some(requested_schema),
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
// Test 3: Mixed column types — base + 2 log files (update + delete), full schema
// =============================================================================

/// E2E: Read MOR table with mixed column types (int, string, double, array, map, etc.).
///
/// Layout: 1 file group, 1 base file + 2 log files.
/// Expected: 2 rows — k3 deleted, matches gold data.
#[tokio::test]
async fn test_e2e_v9_mor_mixed_column_types() -> Result<()> {
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

// =============================================================================
// Test: Column projection — request subset of columns through schema handler
// =============================================================================

/// E2E: Column projection via `FileGroupReaderSchemaHandler` — explicit schemas.
///
/// Uses the city=sf partition (base + log merge) from V9Mor8I4UCommitTime.
/// Requests only `id` and `name` columns. The schema handler should:
///   1. Add `_hoodie_record_key` to required_schema (mandatory for merge)
///   2. Read base file with only [id, name, _hoodie_record_key]
///   3. Merge with log file (which also has _hoodie_record_key)
///   4. Project output back to [id, name] via OutputConverter
///
/// NOTE: This test passes `data_schema` and `requested_schema` directly to
/// `HoodieFileGroupReader::new()`. This exercises the direct construction path
/// but does NOT test the FFI/builder path where schemas live on
/// `ReaderContext.schema_handler`. See `test_e2e_v9_mor_column_projection_via_builder`
/// for the builder path test.
#[tokio::test]
async fn test_e2e_v9_mor_column_projection_via_schema_handler() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

    // Request only id and name — NOT _hoodie_record_key, age, ts, city, etc.
    let requested_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    let result = read_file_group_with_projection(
        &table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        requested_schema,
    )
    .await?;

    // Output should have exactly 2 columns: id and name.
    let schema = result.schema();
    let output_col_names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(
        output_col_names,
        vec!["id", "name"],
        "output should contain only the requested columns"
    );

    // Verify merge results are correct (same as the non-projected test).
    let ids = result
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .unwrap();
    let names = result
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let mut rows: Vec<(i32, String)> = ids
        .iter()
        .zip(names.iter())
        .map(|(id, name)| (id.unwrap(), name.unwrap().to_string()))
        .collect();
    rows.sort_by_key(|(id, _)| *id);

    assert_eq!(rows.len(), 2, "sf partition should have 2 rows");
    assert_eq!(rows[0], (1, "Alice-V2".to_string()), "id=1 should be updated");
    assert_eq!(rows[1], (2, "Bob".to_string()), "id=2 should be unchanged");

    Ok(())
}

// =============================================================================
// Tests: Column projection via the builder path (schemas on ReaderContext)
//
// These tests exercise the code path used by the FFI bridge, where schemas
// are placed on `ReaderContext.schema_handler` and the `CoreFileGroupReader`
// is constructed via `.builder()` WITHOUT explicit `data_schema` /
// `requested_schema`.
//
// Before FIX 1 (commit wiring ReaderContext.schema_handler), the builder path
// created an empty schema handler, so `required_schema = None`, the base file
// was read with ALL columns, and no output projection was applied.
// =============================================================================

use hudi_core::file_group::reader::schema_handler::FileGroupReaderSchemaHandler;

/// Helper: Read a file group via the builder path (schemas on ReaderContext).
///
/// Mimics the FFI bridge flow:
///   1. Populate `ReaderContext.schema_handler` with table_schema, data_schema,
///      and requested_schema (just like `new_file_group_reader_with_context` does).
///   2. Construct `HoodieFileGroupReader` via `.builder()` WITHOUT calling
///      `.with_data_schema()` or `.with_requested_schema()`.
///   3. The constructor should pick up schemas from `reader_context.schema_handler`.
async fn read_file_group_via_builder(
    table_path: &str,
    partition: &str,
    base_file: &str,
    log_files: Vec<&str>,
    requested_schema: SchemaRef,
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
        base_path.clone(),
        None,
        log_paths,
        partition.to_string(),
    );

    // Read table schema from the base file parquet metadata (like the FFI bridge does).
    let table_schema: Option<SchemaRef> = if let Some(ref bp) = base_path {
        storage
            .get_parquet_file_schema(bp)
            .await
            .ok()
            .map(|s| Arc::new(s) as SchemaRef)
    } else {
        None
    };

    // Build schema handler the same way the FFI bridge does:
    //   data_schema → with_table_schema + with_data_schema
    //   requested_schema → with_requested_schema
    let schema_handler = {
        let mut handler = FileGroupReaderSchemaHandler::new();
        if let Some(ts) = table_schema {
            handler = handler
                .with_table_schema(ts.clone())
                .with_data_schema(ts);
        }
        handler = handler.with_requested_schema(requested_schema);
        handler
    };

    let mut reader_context = ReaderContext::empty();
    reader_context.latest_commit_time = "99991231235959999".to_string();
    reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
    reader_context.table_config.insert(
        HudiTableConfig::PrecombineField.as_ref().to_string(),
        "ts".to_string(),
    );
    reader_context.rebuild_record_context(partition.to_string());
    reader_context.schema_handler = schema_handler;
    reader_context.has_log_files = !log_files.is_empty();

    // Use the builder WITHOUT setting data_schema or requested_schema.
    // This forces the constructor to use reader_context.schema_handler.
    let mut reader = HoodieFileGroupReader::builder()
        .with_reader_context(Arc::new(reader_context))
        .with_storage(storage)
        .with_input_split(input_split)
        .with_reader_parameters(ReaderParameters::default())
        .build()?;

    reader.read().await
}

/// E2E: Column projection via the builder path — MOR merge with projection.
///
/// This is the critical test that would have caught the original bug:
/// schemas are on `ReaderContext.schema_handler` (not passed as explicit args).
/// The constructor must pick them up from `reader_context.schema_handler`.
///
/// Requests only `id` and `name` on the city=sf partition (MOR with log files).
/// Validates that:
///   - Output has exactly [id, name] — merge-internal fields stripped
///   - Merge results are correct (Alice-V2, Bob)
///   - Base file was NOT read with all columns (indirectly: if required_schema
///     was None the output would have all columns, failing the schema assert)
#[tokio::test]
async fn test_e2e_v9_mor_column_projection_via_builder() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

    let requested_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    let result = read_file_group_via_builder(
        &table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        requested_schema,
    )
    .await?;

    // Output should have exactly 2 columns: id and name.
    let schema = result.schema();
    let output_col_names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(
        output_col_names,
        vec!["id", "name"],
        "builder path: output should contain only the requested columns, \
         not all parquet columns (was the bug before FIX 1)"
    );

    // Verify merge results are correct.
    let ids = result.column_by_name("id").unwrap().as_any()
        .downcast_ref::<arrow_array::Int32Array>().unwrap();
    let names = result.column_by_name("name").unwrap().as_any()
        .downcast_ref::<arrow_array::StringArray>().unwrap();
    let mut rows: Vec<(i32, String)> = ids.iter().zip(names.iter())
        .map(|(id, name)| (id.unwrap(), name.unwrap().to_string())).collect();
    rows.sort_by_key(|(id, _)| *id);

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "Alice-V2".to_string()));
    assert_eq!(rows[1], (2, "Bob".to_string()));

    Ok(())
}

/// E2E: COW (base-only) column projection via builder — no log files.
///
/// Tests that column pruning works on the COW path (no merge needed).
/// Requests only `id` and `age` columns. No log files, so
/// `generate_required_schema` returns `requested_schema` as-is (COW path).
/// Output should have exactly [id, age] with correct values.
#[tokio::test]
async fn test_e2e_cow_column_projection_via_builder() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

    let requested_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("age", DataType::Int32, true),
    ]));

    // No log files → COW path.
    let result = read_file_group_via_builder(
        &table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![],
        requested_schema,
    )
    .await?;

    let schema = result.schema();
    let output_col_names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(
        output_col_names,
        vec!["id", "age"],
        "COW builder path: output should contain only [id, age]"
    );

    assert_eq!(result.num_rows(), 2, "sf base has 2 rows");

    let ids = result.column_by_name("id").unwrap().as_any()
        .downcast_ref::<arrow_array::Int32Array>().unwrap();
    let ages = result.column_by_name("age").unwrap().as_any()
        .downcast_ref::<arrow_array::Int32Array>().unwrap();
    let mut rows: Vec<(i32, i32)> = ids.iter().zip(ages.iter())
        .map(|(id, age)| (id.unwrap(), age.unwrap())).collect();
    rows.sort_by_key(|(id, _)| *id);

    assert_eq!(rows[0], (1, 30), "id=1 original age=30 (no merge)");
    assert_eq!(rows[1], (2, 25), "id=2 original age=25");

    Ok(())
}

/// E2E: Single column projection via builder — request only `name`.
///
/// Validates minimal column projection: only 1 user column requested.
/// For MOR, the schema handler must still add `_hoodie_record_key` +
/// `_hoodie_is_deleted` + `_hoodie_operation` internally, but the output
/// must contain ONLY [name].
#[tokio::test]
async fn test_e2e_mor_single_column_projection_via_builder() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

    let requested_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
    ]));

    let result = read_file_group_via_builder(
        &table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        requested_schema,
    )
    .await?;

    let schema = result.schema();
    let output_col_names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(
        output_col_names,
        vec!["name"],
        "single column projection: output should contain only [name]"
    );

    assert_eq!(result.num_rows(), 2);

    let names = result.column_by_name("name").unwrap().as_any()
        .downcast_ref::<arrow_array::StringArray>().unwrap();
    let mut values: Vec<String> = names.iter()
        .map(|n| n.unwrap().to_string()).collect();
    values.sort();

    assert!(values.contains(&"Alice-V2".to_string()), "merged name should be Alice-V2");
    assert!(values.contains(&"Bob".to_string()), "unchanged name should be Bob");

    Ok(())
}

// =============================================================================
// Component tests: validate schema_handler.required_schema has the right columns
// =============================================================================

/// Component: Verify `required_schema` includes only the expected columns.
///
/// Constructs a `FileGroupReaderSchemaHandler` mimicking the FFI flow, calls
/// `prepare_required_schema`, and verifies the computed `required_schema`
/// contains the requested columns PLUS mandatory merge fields, but NOT the
/// full parquet schema.
#[test]
fn test_component_required_schema_is_pruned_not_full() {
    let table_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("_hoodie_commit_time", DataType::Utf8, true),
        Field::new("_hoodie_commit_seqno", DataType::Utf8, true),
        Field::new("_hoodie_record_key", DataType::Utf8, true),
        Field::new("_hoodie_partition_path", DataType::Utf8, true),
        Field::new("_hoodie_file_name", DataType::Utf8, true),
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("ts", DataType::Utf8, true),
    ]));

    let requested_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    let mut handler = FileGroupReaderSchemaHandler::new()
        .with_table_schema(table_schema.clone())
        .with_data_schema(table_schema)
        .with_requested_schema(requested_schema);

    let mut props = std::collections::HashMap::new();
    props.insert(
        "hoodie.table.precombine.field".to_string(),
        "ts".to_string(),
    );
    props.insert(
        "hoodie.table.recordkey.fields".to_string(),
        "_hoodie_record_key".to_string(),
    );

    handler.prepare_required_schema(
        true, // has_log_files (MOR)
        &["_hoodie_record_key".to_string()],
        &[], // no ordering fields for COMMIT_TIME_ORDERING
        &props,
        false,
        "COMMIT_TIME_ORDERING",
    );

    let required = handler.required_schema.as_ref()
        .expect("required_schema should be computed");
    let required_cols: Vec<&str> = required.fields().iter()
        .map(|f| f.name().as_str()).collect();

    // Must include user-requested columns.
    assert!(required_cols.contains(&"id"), "must include requested 'id'");
    assert!(required_cols.contains(&"name"), "must include requested 'name'");

    // Must include merge-mandatory field.
    assert!(
        required_cols.contains(&"_hoodie_record_key"),
        "must include _hoodie_record_key for merge"
    );

    // Must NOT include columns that are neither requested nor mandatory.
    assert!(
        !required_cols.contains(&"age"),
        "should NOT include 'age' — not requested and not mandatory"
    );
    assert!(
        !required_cols.contains(&"ts"),
        "should NOT include 'ts' — not requested, not mandatory \
         (COMMIT_TIME_ORDERING does not require ordering fields)"
    );
    assert!(
        !required_cols.contains(&"_hoodie_commit_seqno"),
        "should NOT include _hoodie_commit_seqno — not mandatory"
    );
    assert!(
        !required_cols.contains(&"_hoodie_partition_path"),
        "should NOT include _hoodie_partition_path — not mandatory"
    );
    assert!(
        !required_cols.contains(&"_hoodie_file_name"),
        "should NOT include _hoodie_file_name — not mandatory"
    );

    // output_converter should exist because required ≠ requested.
    assert!(
        handler.get_output_converter().is_some(),
        "output_converter should be Some because required_schema has more \
         columns than requested_schema"
    );
}

/// Component: COW path — required_schema equals requested_schema.
///
/// For COW (no log files), `generate_required_schema` should return the
/// requested schema as-is. No mandatory fields are added.
#[test]
fn test_component_cow_required_equals_requested() {
    let table_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("_hoodie_commit_time", DataType::Utf8, true),
        Field::new("_hoodie_record_key", DataType::Utf8, true),
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
    ]));

    let requested_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
    ]));

    let mut handler = FileGroupReaderSchemaHandler::new()
        .with_table_schema(table_schema.clone())
        .with_data_schema(table_schema)
        .with_requested_schema(requested_schema.clone());

    handler.prepare_required_schema(
        false, // no log files (COW)
        &["_hoodie_record_key".to_string()],
        &[],
        &std::collections::HashMap::new(),
        false,
        "COMMIT_TIME_ORDERING",
    );

    let required = handler.required_schema.as_ref().unwrap();
    assert_eq!(
        required, &requested_schema,
        "COW: required_schema should equal requested_schema exactly"
    );

    // No output converter needed (required == requested).
    assert!(
        handler.get_output_converter().is_none(),
        "COW: no output converter when required == requested"
    );
}

/// Component: When schemas are on ReaderContext, the builder should use them.
///
/// This is the component-level equivalent of the E2E builder tests.
/// Constructs ReaderContext with a pre-populated schema_handler and verifies
/// that `HoodieFileGroupReader::new()` with `data_schema=None` picks it up.
#[tokio::test]
async fn test_component_builder_uses_reader_context_schema_handler() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();
    let (_hudi_configs, storage) = create_configs_and_storage(&table_path).await?;

    let base_file = "city=sf/fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet";

    // Read the full parquet schema.
    let full_schema: SchemaRef = Arc::new(storage.get_parquet_file_schema(base_file).await?);
    let full_col_count = full_schema.fields().len();

    // Request only 2 columns.
    let requested_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    let schema_handler = FileGroupReaderSchemaHandler::new()
        .with_table_schema(full_schema.clone())
        .with_data_schema(full_schema)
        .with_requested_schema(requested_schema);

    let mut reader_context = ReaderContext::empty();
    reader_context.latest_commit_time = "99991231235959999".to_string();
    reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
    reader_context.table_config.insert(
        HudiTableConfig::PrecombineField.as_ref().to_string(),
        "ts".to_string(),
    );
    reader_context.rebuild_record_context("city=sf".to_string());
    reader_context.schema_handler = schema_handler;

    // COW path (no log files) — simplest case.
    let input_split = InputSplit::new(
        Some(base_file.to_string()),
        None,
        Vec::new(),
        "city=sf".to_string(),
    );

    // Build via builder — no explicit schemas.
    let mut reader = HoodieFileGroupReader::builder()
        .with_reader_context(Arc::new(reader_context))
        .with_storage(storage)
        .with_input_split(input_split)
        .with_reader_parameters(ReaderParameters::default())
        .build()?;

    let result = reader.read().await?;

    // The base parquet has many columns, but output should have only 2.
    assert!(
        full_col_count > 2,
        "parquet should have more than 2 columns (has {full_col_count})"
    );
    assert_eq!(
        result.num_columns(), 2,
        "builder path with schema_handler: output should have only 2 columns, \
         not all {full_col_count} parquet columns"
    );

    let schema = result.schema();
    let output_col_names: Vec<&str> = schema.fields().iter()
        .map(|f| f.name().as_str()).collect();
    assert_eq!(output_col_names, vec!["id", "name"]);

    Ok(())
}

// =============================================================================
// Phase 3 helpers — keyFilterOpt e2e smoke test
// =============================================================================

/// Variant of `read_file_group` (lines 69-121 of this file) that accepts a
/// `key_filter_opt`. Mirrors the existing helper, with the addition of
/// `reader_context.key_filter_opt = key_filter_opt;` on the ctx before the
/// reader is constructed.
async fn read_file_group_with_key_filter(
    table_path: &str,
    partition: &str,
    base_file: &str,
    log_files: Vec<&str>,
    key_filter_opt: Option<StdArc<dyn Predicate>>,
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
    reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
    reader_context.table_config.insert(
        HudiTableConfig::PrecombineField.as_ref().to_string(),
        "ts".to_string(),
    );
    reader_context.rebuild_record_context(partition.to_string());
    reader_context.key_filter_opt = key_filter_opt;       // ← the only addition

    let mut reader = HoodieFileGroupReader::new(
        Arc::new(reader_context),
        storage,
        input_split,
        ReaderParameters::default(),
        None,
        None,
    );

    reader.read().await
}

/// Look up the `_hoodie_record_key` string for a given `id` in the baseline
/// batch. Used to derive the literal value for the test filter without
/// hard-coding the keygen encoding.
fn lookup_record_key(batch: &arrow_array::RecordBatch, id: i32) -> String {
    let id_col_idx = batch.schema().index_of("id").expect("id column missing");
    let key_col_idx = batch
        .schema()
        .index_of("_hoodie_record_key")
        .expect("_hoodie_record_key column missing");

    let id_col = batch
        .column(id_col_idx)
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .expect("id column should be Int32");
    let key_col = batch
        .column(key_col_idx)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("_hoodie_record_key column should be StringArray");

    for i in 0..batch.num_rows() {
        if id_col.value(i) == id {
            return key_col.value(i).to_string();
        }
    }
    panic!("id {id} not found in baseline batch");
}

/// Pull the row tuple `(id, name, age)` for the given id, or None.
fn extract_row_with_id_opt(
    batch: &arrow_array::RecordBatch,
    id: i32,
) -> Option<(i32, String, i32)> {
    let id_col = batch.column_by_name("id")?
        .as_any().downcast_ref::<arrow_array::Int32Array>()?;
    let name_col = batch.column_by_name("name")?
        .as_any().downcast_ref::<arrow_array::StringArray>()?;
    let age_col = batch.column_by_name("age")?
        .as_any().downcast_ref::<arrow_array::Int32Array>()?;

    for i in 0..batch.num_rows() {
        if id_col.value(i) == id {
            return Some((
                id_col.value(i),
                name_col.value(i).to_string(),
                age_col.value(i),
            ));
        }
    }
    None
}

// =============================================================================
// Phase 3 e2e smoke test — keyFilterOpt actually filters rows
// =============================================================================

/// Test 1: filter on a key updated in log → 1 row, value from log.
///
/// Validates: filter applies during BOTH log scan and base read. id=1 is
/// updated in log → filtered output should have the log value (Alice-V2).
#[tokio::test]
async fn fg_filter_in_log_updated_key() -> Result<()> {
    let (table_path, partition, base_file, log_files) = sf_file_group();

    // First read no-filter to discover id=1's record key dynamically.
    let baseline_for_key_lookup = read_file_group_with_key_filter(
        &table_path, partition, base_file, log_files.clone(), None,
    ).await?;
    let key_for_id1 = lookup_record_key(&baseline_for_key_lookup, 1);

    let filter: StdArc<dyn Predicate> = StdArc::new(predicates_factory::in_(
        Box::new(NameReference::new("_hoodie_record_key")),
        vec![Box::new(Literal::string(key_for_id1)) as Box<dyn Expression>],
    ));

    let ab = ab_read_with_filter(&table_path, partition, base_file, log_files, filter).await?;

    ab.assert_filter_narrowed();         // 1 < 2
    ab.assert_filtered_ids_eq(&[1]);     // exactly id=1

    // Cross-validate: filtered id=1 row equals baseline id=1 row.
    let expected = extract_row_with_id_opt(&ab.baseline, 1).expect("id=1 in baseline");
    let actual = extract_row_with_id_opt(&ab.filtered, 1).expect("id=1 in filtered");
    assert_eq!(expected, actual, "filtered id=1 must equal baseline id=1 (Alice-V2)");

    Ok(())
}

// =============================================================================
// keyFilterOpt e2e coverage — shared helpers (FilterAbResult + ab_read_with_filter)
//
// Each test in this section ABs a no-filter read against a filtered read of
// the same file group, then asserts via FilterAbResult that the filter is
// real (or, for unsupported predicates, that it's a no-op).
// =============================================================================

/// Result of an AB read: same file group read once with no filter and once
/// with `Some(filter)`.
struct FilterAbResult {
    baseline: arrow_array::RecordBatch,
    filtered: arrow_array::RecordBatch,
}

impl FilterAbResult {
    /// Assert filter is real, not a no-op: filtered must have STRICTLY fewer
    /// rows than baseline.
    fn assert_filter_narrowed(&self) {
        assert!(
            self.filtered.num_rows() < self.baseline.num_rows(),
            "filter did not narrow rows: baseline={}, filtered={} \
             (this means the filter was a no-op — bug)",
            self.baseline.num_rows(),
            self.filtered.num_rows(),
        );
    }

    /// Assert filter is a no-op: filtered.num_rows == baseline.num_rows AND
    /// every row in filtered also appears in baseline (compared by sorted
    /// id-only since row order may differ between two reads).
    fn assert_filter_was_noop(&self) {
        assert_eq!(
            self.filtered.num_rows(),
            self.baseline.num_rows(),
            "expected filter to be no-op (predicate not In/StringStartsWithAny), \
             but row counts differ: baseline={}, filtered={}",
            self.baseline.num_rows(),
            self.filtered.num_rows(),
        );
        let baseline_ids: std::collections::BTreeSet<i32> = ids_in_batch(&self.baseline);
        let filtered_ids: std::collections::BTreeSet<i32> = ids_in_batch(&self.filtered);
        assert_eq!(
            baseline_ids, filtered_ids,
            "expected same id set after no-op filter"
        );
    }

    /// Assert that filtered.num_rows == 0 AND baseline has rows (filter
    /// narrowed everything away).
    fn assert_filtered_empty(&self) {
        assert!(
            self.baseline.num_rows() > 0,
            "baseline must have rows for the empty-filter assertion to be meaningful"
        );
        assert_eq!(
            self.filtered.num_rows(),
            0,
            "expected filter to drop all rows, got {}",
            self.filtered.num_rows()
        );
    }

    /// Assert the exact set of `id`s present in filtered (sorted).
    fn assert_filtered_ids_eq(&self, expected: &[i32]) {
        let actual: Vec<i32> = ids_in_batch(&self.filtered).into_iter().collect();
        let mut expected_sorted = expected.to_vec();
        expected_sorted.sort();
        assert_eq!(actual, expected_sorted, "filtered id set mismatch");
    }
}

/// Helper: collect `id` column values from a RecordBatch as a sorted set.
fn ids_in_batch(batch: &arrow_array::RecordBatch) -> std::collections::BTreeSet<i32> {
    let id_col = batch
        .column_by_name("id")
        .expect("id column missing from batch")
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .expect("id column should be Int32");
    (0..batch.num_rows()).map(|i| id_col.value(i)).collect()
}

/// Shared driver: read once with None, once with Some(filter), return both.
async fn ab_read_with_filter(
    table_path: &str,
    partition: &str,
    base_file: &str,
    log_files: Vec<&str>,
    filter: StdArc<dyn Predicate>,
) -> Result<FilterAbResult> {
    let baseline = read_file_group_with_key_filter(
        table_path,
        partition,
        base_file,
        log_files.clone(),
        None,
    )
    .await?;
    let filtered = read_file_group_with_key_filter(
        table_path,
        partition,
        base_file,
        log_files,
        Some(filter),
    )
    .await?;
    Ok(FilterAbResult { baseline, filtered })
}

// =============================================================================
// Fixture locators
// =============================================================================

/// Fixture locator: V9Mor8I4UCommitTime city=sf file group.
/// Same filenames as test_e2e_v9_mor_commit_time_sf_merge.
fn sf_file_group() -> (String, &'static str, &'static str, Vec<&'static str>) {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();
    (
        table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
    )
}

/// Fixture locator: V9MorNonpart3Commits — non-partitioned, single file group,
/// 1 base + 2 log files (delete block + update block).
/// Schema: id INT, name STRING, price DOUBLE, ts LONG.
fn nonpart_3commits_file_group() -> (String, &'static str, &'static str, Vec<&'static str>) {
    let table_path = QuickstartTripsTable::V9MorNonpart3Commits.path_to_mor_avro();
    (
        table_path,
        "",
        "960a29a0-0f78-401d-85b1-1cbc44b34121-0_0-846-1597_20260409002001492.parquet",
        vec![
            ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002002957.log.1_0-868-1644",
            ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002003963.log.1_0-890-1691",
        ],
    )
}

/// Fixture locator: MorLayoutLogOnly — no base file, 3 log files.
fn log_only_file_group() -> (String, &'static str, &'static str, Vec<&'static str>) {
    let table_path = QuickstartTripsTable::MorLayoutLogOnly.path_to_mor_avro();
    (
        table_path,
        "",
        "",
        vec![
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030525348.log.1_0-102-176",
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030527298.log.1_0-116-202",
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030528554.log.1_0-130-231",
        ],
    )
}
