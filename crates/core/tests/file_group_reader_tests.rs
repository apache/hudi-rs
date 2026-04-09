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

use arrow_schema::SchemaRef;
use hudi_core::config::HudiConfigs;
use hudi_core::error::Result;
use hudi_core::file_group::reader::input_split::InputSplit;
use hudi_core::file_group::reader::reader_context::ReaderContext;
use hudi_core::file_group::reader::reader_parameters::ReaderParameters;
use hudi_core::file_group::reader::HoodieFileGroupReader;
use hudi_core::storage::Storage;
use hudi_core::table::builder::OptionResolver;
use hudi_test::QuickstartTripsTable;
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

/// Build InputSplit + base_path from partition/file info.
fn build_paths(
    partition: &str,
    base_file: &str,
    log_files: Vec<&str>,
) -> (String, InputSplit) {
    let base_path = if partition.is_empty() {
        base_file.to_string()
    } else {
        format!("{}/{}", partition, base_file)
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
        Some(base_path.clone()),
        None,
        log_paths,
        partition.to_string(),
    );
    (base_path, input_split)
}

/// Read a single file group through the new HoodieFileGroupReader (no projection).
async fn read_file_group(
    table_path: &str,
    partition: &str,
    base_file: &str,
    log_files: Vec<&str>,
) -> Result<arrow_array::RecordBatch> {
    let (_hudi_configs, storage) = create_configs_and_storage(table_path).await?;
    let (base_path, input_split) = build_paths(partition, base_file, log_files);

    // Read table schema from parquet metadata (replaces Java's metaClient)
    let table_schema: SchemaRef = Arc::new(storage.get_parquet_file_schema(&base_path).await?);

    let mut reader_context = ReaderContext::empty();
    reader_context.latest_commit_time = "99991231235959999".to_string();
    reader_context.record_key_field = "_hoodie_record_key".to_string();
    reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();

    let mut reader = HoodieFileGroupReader::new(
        Arc::new(reader_context),
        storage,
        table_schema,
        None, // no projection
        input_split,
        vec!["ts".to_string()],
        ReaderParameters::default(),
    )?;

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
// Column Projection Tests
// =============================================================================

/// Extract (id, name) tuples from a RecordBatch, sorted by id.
fn extract_id_name(batch: &arrow_array::RecordBatch) -> Vec<(i32, String)> {
    let id_col = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .unwrap();
    let name_col = batch
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let mut rows: Vec<(i32, String)> = (0..batch.num_rows())
        .map(|i| (id_col.value(i), name_col.value(i).to_string()))
        .collect();
    rows.sort_by_key(|r| r.0);
    rows
}

/// Read a file group with column projection through the builder API.
async fn read_file_group_with_projection(
    table_path: &str,
    partition: &str,
    base_file: &str,
    log_files: Vec<&str>,
    requested_columns: Vec<&str>,
) -> Result<arrow_array::RecordBatch> {
    let (_hudi_configs, storage) = create_configs_and_storage(table_path).await?;
    let (base_path, input_split) = build_paths(partition, base_file, log_files);

    // Read table schema from parquet metadata (replaces Java's metaClient)
    let table_schema: SchemaRef = Arc::new(storage.get_parquet_file_schema(&base_path).await?);

    // Build requested schema from column names + table schema
    let requested_fields: Vec<_> = requested_columns
        .iter()
        .filter_map(|name| table_schema.field_with_name(name).ok().cloned())
        .map(Arc::new)
        .collect();
    let requested_schema: SchemaRef = Arc::new(arrow_schema::Schema::new(requested_fields));

    let mut reader_context = ReaderContext::empty();
    reader_context.latest_commit_time = "99991231235959999".to_string();
    reader_context.record_key_field = "_hoodie_record_key".to_string();
    reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();

    let mut reader = HoodieFileGroupReader::new(
        Arc::new(reader_context),
        storage,
        table_schema,
        Some(requested_schema),
        input_split,
        vec!["ts".to_string()],
        ReaderParameters::default(),
    )?;

    reader.read().await
}

/// E2E: Base-only read with projection — only id and name columns.
///
/// Given: Base file only, projection = ["id", "name"]
/// Then: Output has exactly 2 columns, values correct
#[tokio::test]
async fn test_e2e_projection_base_only() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

    let result = read_file_group_with_projection(
        &table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![], // no log files
        vec!["id", "name"],
    )
    .await?;

    // Verify only requested columns are in output
    let schema = result.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["id", "name"], "output should have exactly id and name");
    assert_eq!(result.num_columns(), 2);
    assert_eq!(result.num_rows(), 2);

    // Extract and sort by id (row order from parquet is not guaranteed)
    let rows = extract_id_name(&result);
    assert_eq!(rows[0], (1, "Alice".to_string()));
    assert_eq!(rows[1], (2, "Bob".to_string()));

    Ok(())
}

/// E2E: MOR merge with projection — merge still works with only id and name requested.
///
/// Given: Base + log files, projection = ["id", "name"]
/// Then: Output has exactly 2 columns, merge applied (Alice → Alice-V2)
#[tokio::test]
async fn test_e2e_projection_mor_merge() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

    let result = read_file_group_with_projection(
        &table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        vec!["id", "name"],
    )
    .await?;

    // Verify only requested columns
    let schema = result.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["id", "name"], "output should have exactly id and name");
    assert_eq!(result.num_columns(), 2);

    // Verify merge was applied despite not requesting _hoodie_record_key or ts
    assert_eq!(result.num_rows(), 2);
    let rows = extract_id_name(&result);
    assert_eq!(rows[0], (1, "Alice-V2".to_string()), "id=1 should be updated from log");
    assert_eq!(rows[1], (2, "Bob".to_string()), "id=2 should be unchanged");

    Ok(())
}

/// E2E: Projection that includes a mandatory field explicitly.
///
/// Given: projection = ["id", "name", "_hoodie_record_key"]
/// Then: All 3 columns appear in output (record key is not stripped because user requested it)
#[tokio::test]
async fn test_e2e_projection_includes_mandatory_field() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

    let result = read_file_group_with_projection(
        &table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        vec!["id", "name", "_hoodie_record_key"],
    )
    .await?;

    let schema = result.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        field_names,
        vec!["id", "name", "_hoodie_record_key"],
        "_hoodie_record_key should be in output when explicitly requested"
    );
    assert_eq!(result.num_columns(), 3);

    Ok(())
}

/// E2E: Projection with a single column.
///
/// Given: projection = ["id"]
/// Then: 1-column output, values correct after merge
#[tokio::test]
async fn test_e2e_projection_single_column() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

    let result = read_file_group_with_projection(
        &table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        vec!["id"],
    )
    .await?;

    let schema = result.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["id"]);
    assert_eq!(result.num_columns(), 1);
    assert_eq!(result.num_rows(), 2);

    let id_col = result.column(0).as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
    let mut ids: Vec<i32> = (0..result.num_rows()).map(|i| id_col.value(i)).collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2]);

    Ok(())
}

/// E2E: No projection — reads all columns (backward compatibility).
///
/// Given: No projection set (use existing read_file_group helper)
/// Then: All columns present, identical to current behavior
#[tokio::test]
async fn test_e2e_projection_none_reads_all() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

    // Read with projection (all data columns)
    let all_cols = read_file_group(
        &table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
    )
    .await?;

    // Should have all the columns from the table schema
    assert!(
        all_cols.num_columns() > 2,
        "no-projection read should return all columns, got {}",
        all_cols.num_columns()
    );

    // Verify merge still works
    let records = extract_id_name_age(&all_cols);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0], (1, "Alice-V2".to_string(), 31));
    assert_eq!(records[1], (2, "Bob".to_string(), 25));

    Ok(())
}

/// E2E: Projection with multi-log MOR (delete + update).
///
/// Given: Non-partitioned table with 2 log files (delete + update), projection = ["id", "name"]
/// Then: Output has 2 columns, merge/delete correctly applied
#[tokio::test]
async fn test_e2e_projection_mor_multi_log() -> Result<()> {
    let table_path = QuickstartTripsTable::V9MorNonpart3Commits.path_to_mor_avro();

    let result = read_file_group_with_projection(
        &table_path,
        "",
        "960a29a0-0f78-401d-85b1-1cbc44b34121-0_0-846-1597_20260409002001492.parquet",
        vec![
            ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002002957.log.1_0-868-1644",
            ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002003963.log.1_0-890-1691",
        ],
        vec!["id", "name"],
    )
    .await?;

    let schema = result.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["id", "name"]);
    assert_eq!(result.num_columns(), 2);
    assert_eq!(result.num_rows(), 4, "should have 4 rows after merge (3 deleted, 3 updated)");

    let id_col = result.column(0).as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
    let name_col = result.column(1).as_any().downcast_ref::<arrow_array::StringArray>().unwrap();

    // Collect and sort by id
    let mut rows: Vec<(i32, String)> = (0..result.num_rows())
        .map(|i| (id_col.value(i), name_col.value(i).to_string()))
        .collect();
    rows.sort_by_key(|r| r.0);

    assert_eq!(rows[0], (3, "C".to_string()), "id=3 should be unchanged");
    assert_eq!(rows[1], (4, "D2".to_string()), "id=4 should be updated");
    assert_eq!(rows[2], (5, "E2".to_string()), "id=5 should be updated");
    assert_eq!(rows[3], (6, "F2".to_string()), "id=6 should be updated");

    Ok(())
}
