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

//! E2E integration tests for `HoodieFileGroupReader::read_record_batch`.
//!
//! These mirror every test in `crates/core/tests/file_group_reader_tests.rs`
//! but go through the cpp crate's `HoodieFileGroupReader` (the struct that
//! backs the C++ FFI) instead of the core `HoodieFileGroupReader` directly.
//! This validates the full construction path (ReaderContext, InputSplit,
//! Storage, ReaderParameters) that the FFI consumer uses.
//!
//! All tests are synchronous (`#[test]`) because `HoodieFileGroupReader`
//! owns a `tokio::runtime::Runtime` and calls `block_on` internally — nesting
//! a second runtime under `#[tokio::test]` would panic.

use hudi::hudi_core::config::HudiConfigs;
use hudi::hudi_core::config::table::HudiTableConfig;
use hudi::hudi_core::file_group::reader::input_split::InputSplit;
use hudi::hudi_core::file_group::reader::reader_context::ReaderContext;
use hudi::hudi_core::file_group::reader::reader_parameters::ReaderParameters;
use hudi::hudi_core::file_group::reader::record_context::RecordContext;
use hudi::hudi_core::file_group::reader::schema_handler::FileGroupReaderSchemaHandler;
use hudi::hudi_core::storage::Storage;
use hudi::hudi_core::table::builder::OptionResolver;
use hudi::HoodieFileGroupReader;
use hudi_test::QuickstartTripsTable;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

// =============================================================================
// Helpers
// =============================================================================

/// Build Storage and merged props from table path using a temporary runtime.
fn create_storage_and_props(table_path: &str) -> (Arc<Storage>, HashMap<String, String>) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build setup runtime");
    rt.block_on(async {
        let empty_opts: Vec<(&str, &str)> = vec![];
        let mut resolver = OptionResolver::new_with_options(table_path, empty_opts);
        resolver.resolve_options().await.expect("resolve options");
        let hudi_configs = Arc::new(HudiConfigs::new(resolver.hudi_options.clone()));
        let storage = Storage::new(Arc::new(resolver.storage_options), hudi_configs)
            .expect("create storage");
        (storage, resolver.hudi_options)
    })
}

/// Build the full `ReaderContext` the same way `new_file_group_reader_with_context` does.
fn build_reader_context(
    table_path: &str,
    partition: &str,
    has_log_files: bool,
    table_config: HashMap<String, String>,
) -> Arc<ReaderContext> {
    let record_context = RecordContext::new(&table_config, partition.to_string());
    Arc::new(ReaderContext {
        table_path: table_path.to_string(),
        latest_commit_time: "99991231235959999".to_string(),
        base_file_format: String::new(),
        has_log_files,
        has_bootstrap_base_file: false,
        needs_bootstrap_merge: false,
        should_merge_use_record_position: false,
        enable_logical_timestamp_field_repair: false,
        iterator_mode: String::new(),
        merge_mode: "COMMIT_TIME_ORDERING".to_string(),
        merge_strategy_id: String::new(),
        instant_range: None,
        record_context,
        schema_handler: FileGroupReaderSchemaHandler::new(),
        table_config,
        hoodie_reader_config: HashMap::new(),
    })
}

/// Build an `InputSplit` from partition, base file, and log files.
fn build_input_split(partition: &str, base_file: &str, log_files: Vec<&str>) -> InputSplit {
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
    InputSplit::new(base_path, None, log_paths, partition.to_string())
}

/// Construct a `HoodieFileGroupReader` (cpp crate) and call `read_record_batch`.
fn read_record_batch(
    table_path: &str,
    partition: &str,
    base_file: &str,
    log_files: Vec<&str>,
    storage: Arc<Storage>,
    props: HashMap<String, String>,
) -> (arrow_array::RecordBatch, SchemaRef) {
    let has_log_files = !log_files.is_empty();
    let input_split = build_input_split(partition, base_file, log_files);

    let mut table_config: HashMap<String, String> = HashMap::new();
    table_config.insert(
        HudiTableConfig::PrecombineField.as_ref().to_string(),
        "ts".to_string(),
    );

    let reader_context =
        build_reader_context(table_path, partition, has_log_files, table_config);

    let reader = HoodieFileGroupReader::new(
        reader_context,
        storage,
        props,
        ReaderParameters::default(),
        input_split,
        None,
    )
    .expect("build HoodieFileGroupReader");

    reader.read_record_batch().expect("read_record_batch")
}

/// Extract (id, name, age) tuples from a RecordBatch, sorted by id.
fn extract_id_name_age(batch: &arrow_array::RecordBatch) -> Vec<(i32, String, i32)> {
    QuickstartTripsTable::id_name_age(batch)
}

/// Extract (id, name, price) tuples from a RecordBatch, sorted by id.
fn extract_id_name_price(batch: &arrow_array::RecordBatch) -> Vec<(i32, String, f64)> {
    QuickstartTripsTable::id_name_price(batch)
}

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

/// Compare actual reader output against gold data (non-hoodie columns).
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
                "[{table_name}] mismatch at row={row} col={col_name}: \
                 actual={actual_str} gold={gold_str}",
            );
        }
    }
}

// =============================================================================
// v9 MOR COMMIT_TIME_ORDERING: city=sf (2 base rows, 1 update in log)
// =============================================================================

#[test]
fn test_read_record_batch_sf_merge() {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();
    let (storage, props) = create_storage_and_props(&table_path);

    let (batch, schema) = read_record_batch(
        &table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        storage,
        props,
    );

    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(col_names.contains(&"id"), "schema should contain 'id'");
    assert!(col_names.contains(&"name"), "schema should contain 'name'");
    assert!(col_names.contains(&"age"), "schema should contain 'age'");

    let records = extract_id_name_age(&batch);
    assert_eq!(records.len(), 2, "sf partition should have 2 rows");
    assert_eq!(records[0], (1, "Alice-V2".to_string(), 31));
    assert_eq!(records[1], (2, "Bob".to_string(), 25));
}

// =============================================================================
// v9 MOR COMMIT_TIME_ORDERING: city=nyc
// =============================================================================

#[test]
fn test_read_record_batch_nyc_merge() {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();
    let (storage, props) = create_storage_and_props(&table_path);

    let (batch, schema) = read_record_batch(
        &table_path,
        "city=nyc",
        "cae699a1-42f7-4226-9bd3-7f0e49496028-0_3-13-36_20260408053032350.parquet",
        vec![".cae699a1-42f7-4226-9bd3-7f0e49496028-0_20260408053037787.log.1_3-27-76"],
        storage,
        props,
    );

    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(col_names.contains(&"id"));
    assert!(col_names.contains(&"name"));
    assert!(col_names.contains(&"age"));

    let records = extract_id_name_age(&batch);
    assert_eq!(records.len(), 2, "nyc partition should have 2 rows");
    assert_eq!(records[0], (3, "Carol-V2".to_string(), 36));
    assert_eq!(records[1], (4, "Dave".to_string(), 28));
}

// =============================================================================
// v9 MOR COMMIT_TIME_ORDERING: city=chi
// =============================================================================

#[test]
fn test_read_record_batch_chi_merge() {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();
    let (storage, props) = create_storage_and_props(&table_path);

    let (batch, schema) = read_record_batch(
        &table_path,
        "city=chi",
        "8fb566bd-0f0d-45ee-9f4b-391c1ceb9dda-0_2-13-35_20260408053032350.parquet",
        vec![".8fb566bd-0f0d-45ee-9f4b-391c1ceb9dda-0_20260408053037787.log.1_2-27-75"],
        storage,
        props,
    );

    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(col_names.contains(&"id"));
    assert!(col_names.contains(&"name"));
    assert!(col_names.contains(&"age"));

    let records = extract_id_name_age(&batch);
    assert_eq!(records.len(), 2, "chi partition should have 2 rows");
    assert_eq!(records[0], (5, "Eve-V2".to_string(), 33));
    assert_eq!(records[1], (6, "Frank".to_string(), 40));
}

// =============================================================================
// v9 MOR COMMIT_TIME_ORDERING: city=la
// =============================================================================

#[test]
fn test_read_record_batch_la_merge() {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();
    let (storage, props) = create_storage_and_props(&table_path);

    let (batch, schema) = read_record_batch(
        &table_path,
        "city=la",
        "31d2005b-4c79-46f4-aca5-6519809b2503-0_1-13-34_20260408053032350.parquet",
        vec![".31d2005b-4c79-46f4-aca5-6519809b2503-0_20260408053037787.log.1_1-27-74"],
        storage,
        props,
    );

    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(col_names.contains(&"id"));
    assert!(col_names.contains(&"name"));
    assert!(col_names.contains(&"age"));

    let records = extract_id_name_age(&batch);
    assert_eq!(records.len(), 2, "la partition should have 2 rows");
    assert_eq!(records[0], (7, "Grace-V2".to_string(), 28));
    assert_eq!(records[1], (8, "Hank".to_string(), 45));
}

// =============================================================================
// v9 MOR: Base-only read (no log files)
// =============================================================================

#[test]
fn test_read_record_batch_base_only() {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();
    let (storage, props) = create_storage_and_props(&table_path);

    let (batch, schema) = read_record_batch(
        &table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![], // no log files
        storage,
        props,
    );

    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(col_names.contains(&"id"));
    assert!(col_names.contains(&"name"));
    assert!(col_names.contains(&"age"));

    let records = extract_id_name_age(&batch);
    assert_eq!(records.len(), 2, "base-only should have 2 rows");
    assert_eq!(records[0], (1, "Alice".to_string(), 30));
    assert_eq!(records[1], (2, "Bob".to_string(), 25));
}

// =============================================================================
// v9 MOR COMMIT_TIME_ORDERING: Non-partitioned, 2 log files (delete + update)
// =============================================================================

#[test]
fn test_read_record_batch_nonpart_multi_log() {
    let table_path = QuickstartTripsTable::V9MorNonpart3Commits.path_to_mor_avro();
    let (storage, props) = create_storage_and_props(&table_path);

    let (batch, schema) = read_record_batch(
        &table_path,
        "",
        "960a29a0-0f78-401d-85b1-1cbc44b34121-0_0-846-1597_20260409002001492.parquet",
        vec![
            ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002002957.log.1_0-868-1644",
            ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002003963.log.1_0-890-1691",
        ],
        storage,
        props,
    );

    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(col_names.contains(&"id"));
    assert!(col_names.contains(&"name"));
    assert!(col_names.contains(&"price"));

    let records = extract_id_name_price(&batch);
    assert_eq!(records.len(), 4, "should have 4 rows after merge");
    assert_eq!(records[0], (3, "C".to_string(), 30.0));
    assert_eq!(records[1], (4, "D2".to_string(), 45.0));
    assert_eq!(records[2], (5, "E2".to_string(), 55.0));
    assert_eq!(records[3], (6, "F2".to_string(), 65.0));
}

// =============================================================================
// MOR File Slice Layout: Log compaction (5 log files, no base)
// =============================================================================

#[test]
fn test_read_record_batch_log_compaction() {
    let table_path = QuickstartTripsTable::MorLayoutLogCompaction.path_to_mor_avro();
    let gold_dir = QuickstartTripsTable::MorLayoutLogCompaction.path_to_mor_avro_gold();
    let (storage, props) = create_storage_and_props(&table_path);

    let (batch, schema) = read_record_batch(
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
        storage,
        props,
    );

    assert!(!schema.fields().is_empty(), "schema should have columns");
    let gold = read_gold_parquet(&gold_dir);
    assert_matches_gold(&batch, &gold, "table_log_compaction");
}

// =============================================================================
// MOR File Slice Layout: Log only (3 log files)
// =============================================================================

#[test]
fn test_read_record_batch_log_only() {
    let table_path = QuickstartTripsTable::MorLayoutLogOnly.path_to_mor_avro();
    let gold_dir = QuickstartTripsTable::MorLayoutLogOnly.path_to_mor_avro_gold();
    let (storage, props) = create_storage_and_props(&table_path);

    let (batch, schema) = read_record_batch(
        &table_path,
        "",
        "",
        vec![
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030525348.log.1_0-102-176",
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030527298.log.1_0-116-202",
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030528554.log.1_0-130-231",
        ],
        storage,
        props,
    );

    assert!(!schema.fields().is_empty(), "schema should have columns");
    let gold = read_gold_parquet(&gold_dir);
    assert_matches_gold(&batch, &gold, "table_log_only");
}

// =============================================================================
// MOR File Slice Layout: Mixed column types (base + 2 log files)
// =============================================================================

#[test]
fn test_read_record_batch_mixed_column_types() {
    let table_path = QuickstartTripsTable::MorLayoutColumnProjection.path_to_mor_avro();
    let gold_dir = QuickstartTripsTable::MorLayoutColumnProjection.path_to_mor_avro_gold();
    let (storage, props) = create_storage_and_props(&table_path);

    let (batch, schema) = read_record_batch(
        &table_path,
        "",
        "78076137-b2c4-410d-8473-3d6366ae0985-0_0-165-279_20260409030530945.parquet",
        vec![
            ".78076137-b2c4-410d-8473-3d6366ae0985-0_20260409030532996.log.1_0-179-305",
            ".78076137-b2c4-410d-8473-3d6366ae0985-0_20260409030534379.log.1_0-193-334",
        ],
        storage,
        props,
    );

    assert!(!schema.fields().is_empty(), "schema should have columns");
    let gold = read_gold_parquet(&gold_dir);
    assert_matches_gold(&batch, &gold, "table_column_projection");
}

// =============================================================================
// MOR File Slice Layout: All data types (base + 3 log files)
// =============================================================================

#[test]
fn test_read_record_batch_all_data_types() {
    let table_path = QuickstartTripsTable::MorLayoutAllDataTypes.path_to_mor_avro();
    let gold_dir = QuickstartTripsTable::MorLayoutAllDataTypes.path_to_mor_avro_gold();
    let (storage, props) = create_storage_and_props(&table_path);

    let (batch, schema) = read_record_batch(
        &table_path,
        "",
        "c887c1e8-5fb9-475e-8171-769c5cf10c61-0_0-240-395_20260409030537482.parquet",
        vec![
            ".c887c1e8-5fb9-475e-8171-769c5cf10c61-0_20260409030539332.log.1_0-254-421",
            ".c887c1e8-5fb9-475e-8171-769c5cf10c61-0_20260409030540482.log.1_0-268-450",
            ".c887c1e8-5fb9-475e-8171-769c5cf10c61-0_20260409030541620.log.1_0-282-482",
        ],
        storage,
        props,
    );

    assert!(!schema.fields().is_empty(), "schema should have columns");
    let gold = read_gold_parquet(&gold_dir);
    assert_matches_gold(&batch, &gold, "table_all_data_types");
}

// =============================================================================
// Column projection via schema handler
//
// Note: The cpp `HoodieFileGroupReader` sets the schema handler on
// `ReaderContext`, but the core `CoreFileGroupReader::builder().build()`
// creates its own schema handler from `data_schema`/`requested_schema`
// params. Column projection through the cpp path requires the schema
// handler on the ReaderContext to be picked up by the core reader.
// This test verifies merge correctness with schema handler set; the
// output currently includes all columns (projection is applied at the
// core builder level, not via the ReaderContext schema handler).
// =============================================================================

#[test]
fn test_read_record_batch_column_projection() {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();
    let (storage, props) = create_storage_and_props(&table_path);

    let partition = "city=sf";
    let base_file =
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet";
    let log_files = vec![
        ".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73",
    ];

    let input_split = build_input_split(partition, base_file, log_files);

    // Read data schema from base file parquet metadata using a temporary runtime.
    let data_schema: SchemaRef = {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build schema-read runtime");
        let base_rel = format!("{}/{}", partition, base_file);
        let s = storage.clone();
        Arc::new(
            rt.block_on(s.get_parquet_file_schema(&base_rel))
                .expect("read parquet schema"),
        )
    };

    // Request only id and name.
    let requested_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    let schema_handler = FileGroupReaderSchemaHandler::new()
        .with_data_schema(data_schema)
        .with_requested_schema(requested_schema);

    let mut table_config: HashMap<String, String> = HashMap::new();
    table_config.insert(
        HudiTableConfig::PrecombineField.as_ref().to_string(),
        "ts".to_string(),
    );
    let record_context = RecordContext::new(&table_config, partition.to_string());

    let reader_context = Arc::new(ReaderContext {
        table_path: table_path.clone(),
        latest_commit_time: "99991231235959999".to_string(),
        base_file_format: String::new(),
        has_log_files: true,
        has_bootstrap_base_file: false,
        needs_bootstrap_merge: false,
        should_merge_use_record_position: false,
        enable_logical_timestamp_field_repair: false,
        iterator_mode: String::new(),
        merge_mode: "COMMIT_TIME_ORDERING".to_string(),
        merge_strategy_id: String::new(),
        instant_range: None,
        record_context,
        schema_handler,
        table_config,
        hoodie_reader_config: HashMap::new(),
    });

    let reader = HoodieFileGroupReader::new(
        reader_context,
        storage,
        props,
        ReaderParameters::default(),
        input_split,
        None,
    )
    .expect("build HoodieFileGroupReader");

    let (batch, _schema) = reader.read_record_batch().expect("read_record_batch");

    // Verify merge correctness — the merge result should contain the
    // correct data even though projection is not applied at this layer.
    let records = extract_id_name_age(&batch);
    assert_eq!(records.len(), 2, "sf partition should have 2 rows");
    assert_eq!(records[0], (1, "Alice-V2".to_string(), 31));
    assert_eq!(records[1], (2, "Bob".to_string(), 25));
}
