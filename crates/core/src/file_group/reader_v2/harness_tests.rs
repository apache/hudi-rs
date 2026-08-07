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

//! E2E tests for `HoodieFileGroupReader`, driven by the declarative harness
//! in `fg_harness` (see `fg_harness/mod.rs`).
//!
//! Supported read matrix under test: table v9, MOR snapshot,
//! COMMIT_TIME_ORDERING, schema-on-write backward-compatible evolution,
//! parquet base + avro log blocks. Each case validates the FULL output
//! dataset; unsupported configurations assert loud errors (ErrContains).
//!
//! Adding coverage = adding an `fg_case_test!` entry. Fixture provenance:
//! `crates/test/data/` (each zip's generator test + layout).

use super::harness as fg_harness;
use crate::config::table::HudiTableConfig;
use crate::error::Result;
use crate::fg_case_test;
use crate::file_group::reader_v2::MAX_INSTANT_TIME;
use crate::file_group::reader_v2::engine::HoodieFileGroupReader;
use crate::file_group::reader_v2::input_split::InputSplit;
use crate::file_group::reader_v2::reader_context::ReaderContext;
use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
use crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler;
use crate::timeline::selector::InstantRange;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use fg_harness::{Expected, FgReaderCase, FilterPredicate, RowFilterSpec, SchemaSpec};
use hudi_test::QuickstartTripsTable;
use std::sync::Arc;

/// city=sf merge case as a harness case — the migration template.
fn case_sf_merge() -> FgReaderCase {
    FgReaderCase {
        name: "v9_mor_commit_time_sf_merge",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        log_files: &[".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "age"],
            rows: &[&["1", "Alice-V2", "31"], &["2", "Bob", "25"]],
        },
        ..Default::default()
    }
}

fg_case_test!(harness_v9_mor_commit_time_sf_merge, case_sf_merge());

/// Harness self-test: a poisoned expectation MUST be rejected. This proves
/// the harness can fail — guarding against a runner that vacuously passes.
#[tokio::test]
async fn harness_self_test_detects_wrong_expectation() {
    let mut case = case_sf_merge();
    case.name = "self_test_poisoned";
    case.expected = Expected::Rows {
        sort_key: "id",
        columns: &["id", "name", "age"],
        rows: &[&["1", "Alice", "30"], &["2", "Bob", "25"]], // wrong: claims id=1 not updated
    };
    let result = fg_harness::try_run_case(&case).await;
    let err = result.expect_err("poisoned case must fail");
    assert!(
        err.contains("mismatch"),
        "error should pinpoint the cell mismatch, got: {err}"
    );
}

fg_case_test!(
    harness_v9_mor_commit_time_nyc_merge,
    FgReaderCase {
        name: "v9_mor_commit_time_nyc_merge",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=nyc",
        base_file: "cae699a1-42f7-4226-9bd3-7f0e49496028-0_3-13-36_20260408053032350.parquet",
        log_files: &[".cae699a1-42f7-4226-9bd3-7f0e49496028-0_20260408053037787.log.1_3-27-76"],
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "age"],
            rows: &[&["3", "Carol-V2", "36"], &["4", "Dave", "28"]],
        },
        ..Default::default()
    }
);

fg_case_test!(
    harness_v9_mor_commit_time_chi_merge,
    FgReaderCase {
        name: "v9_mor_commit_time_chi_merge",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=chi",
        base_file: "8fb566bd-0f0d-45ee-9f4b-391c1ceb9dda-0_2-13-35_20260408053032350.parquet",
        log_files: &[".8fb566bd-0f0d-45ee-9f4b-391c1ceb9dda-0_20260408053037787.log.1_2-27-75"],
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "age"],
            rows: &[&["5", "Eve-V2", "33"], &["6", "Frank", "40"]],
        },
        ..Default::default()
    }
);

fg_case_test!(
    harness_v9_mor_commit_time_la_merge,
    FgReaderCase {
        name: "v9_mor_commit_time_la_merge",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=la",
        base_file: "31d2005b-4c79-46f4-aca5-6519809b2503-0_1-13-34_20260408053032350.parquet",
        log_files: &[".31d2005b-4c79-46f4-aca5-6519809b2503-0_20260408053037787.log.1_1-27-74"],
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "age"],
            rows: &[&["7", "Grace-V2", "28"], &["8", "Hank", "45"]],
        },
        ..Default::default()
    }
);

// =============================================================================
// ENG-44437: peak-memory HARD cap, end-to-end through HoodieFileGroupReader.
//
// These two cases prove the cap fires through the REAL read path — the config
// STRING `hoodie.memory.merge.max.peak.size` flows via
// `ReaderContext.hoodie_reader_config` → `SpillConfig::from_config` →
// `HoodieFileGroupRecordBuffer`'s `SpillableRecordMap` → `enforce_peak_cap` —
// NOT by constructing a `SpillConfig`/calling `enforce_peak_cap` directly
// (the unit-level path is covered in `nonfunctional_gaps_repro.rs`).
// =============================================================================

/// Config key for the hard peak-memory cap (`SpillableRecordMap`'s
/// `CONFIG_MAX_PEAK_MEMORY`). Spelled here as the literal an operator/gluten
/// forwards, so the case exercises the string→config parse just like production.
const MERGE_MAX_PEAK_SIZE_KEY: &str = "hoodie.memory.merge.max.peak.size";

// Fixture shared by the two cap cases: a base parquet + two log files (a DELETE
// block for ids 0-2, then an avro update block for ids 4-6). The DELETE block
// leaves `RecordPayload::Delete` tombstones resident in the merge map; unlike a
// `BatchRef` log record (which the cap path can always spill to disk), a delete
// tombstone has no source batch to evict, so it is exactly the IRREDUCIBLE
// footprint the hard cap must fail loud on rather than spill away.
const CAP_FIXTURE_BASE: &str =
    "960a29a0-0f78-401d-85b1-1cbc44b34121-0_0-846-1597_20260409002001492.parquet";
const CAP_FIXTURE_LOGS: &[&str] = &[
    ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002002957.log.1_0-868-1644",
    ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002003963.log.1_0-890-1691",
];

// Peak cap forwarded LOW (100 bytes — below the tracked footprint of the delete
// tombstones the DELETE block leaves resident, each `key.len() + 64` overhead
// bytes). The tombstones cannot be spilled to relieve the cap, so the read fails
// LOUD with `CoreError::MemoryLimitExceeded`. The asserted substring is the
// config key itself, which appears ONLY in that error message — proving the loud
// failure is the cap's, reached through the config string, not a direct
// `enforce_peak_cap` call.
fg_case_test!(
    harness_v9_mor_peak_cap_fires_loud,
    FgReaderCase {
        name: "v9_mor_peak_cap_fires_loud",
        fixture: QuickstartTripsTable::V9MorNonpart3Commits,
        partition: "",
        base_file: CAP_FIXTURE_BASE,
        log_files: CAP_FIXTURE_LOGS,
        reader_config: &[(MERGE_MAX_PEAK_SIZE_KEY, "100")],
        expected: Expected::ErrContains(MERGE_MAX_PEAK_SIZE_KEY),
        ..Default::default()
    }
);

// Companion no-op case: the SAME fixture with the peak cap UNSET (default) reads
// correctly and raises no error — proving the cap is opt-in and its default is a
// pure no-op that preserves the pre-existing read behavior. (Byte-identical
// expectation to `harness_v9_mor_nonpart_multi_log`, isolated here to document
// the contrast with the loud case above.)
fg_case_test!(
    harness_v9_mor_peak_cap_unset_is_noop,
    FgReaderCase {
        name: "v9_mor_peak_cap_unset_is_noop",
        fixture: QuickstartTripsTable::V9MorNonpart3Commits,
        partition: "",
        base_file: CAP_FIXTURE_BASE,
        log_files: CAP_FIXTURE_LOGS,
        reader_config: &[],
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "price"],
            rows: &[
                &["3", "C", "30.0"],
                &["4", "D2", "45.0"],
                &["5", "E2", "55.0"],
                &["6", "F2", "65.0"],
            ],
        },
        ..Default::default()
    }
);

// Base-file-only (no-merge) path: `log_files: &[]` bypasses merge entirely;
// original inserted values must come back unchanged.
fg_case_test!(
    harness_v9_mor_base_only_read,
    FgReaderCase {
        name: "v9_mor_base_only_read",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        log_files: &[],
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "age"],
            rows: &[&["1", "Alice", "30"], &["2", "Bob", "25"]],
        },
        ..Default::default()
    }
);

// Non-partitioned, 2 heterogeneous log files: log 1 is a DELETE block
// (ids 0-2), log 2 an avro data block (updates ids 4-6 to D2/E2/F2).
// After merge: 4 rows — 7 base - 3 deleted, ids 4-6 updated, id=3 untouched.
fg_case_test!(
    harness_v9_mor_nonpart_multi_log,
    FgReaderCase {
        name: "v9_mor_nonpart_multi_log",
        fixture: QuickstartTripsTable::V9MorNonpart3Commits,
        partition: "",
        base_file: "960a29a0-0f78-401d-85b1-1cbc44b34121-0_0-846-1597_20260409002001492.parquet",
        log_files: &[
            ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002002957.log.1_0-868-1644",
            ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002003963.log.1_0-890-1691",
        ],
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "price"],
            rows: &[
                &["3", "C", "30.0"],
                &["4", "D2", "45.0"],
                &["5", "E2", "55.0"],
                &["6", "F2", "65.0"],
            ],
        },
        // Stats derived from the fixture semantics (B2 wiring, mirrors gold
        // StandardUpdateProcessor): log 1 deletes ids 0-2 (3 deletes), log 2
        // updates ids 4-6 via base+log merge (3 updates), id=3 is an untouched
        // base record (emitted directly, NOT counted). No log-only new key, so
        // no inserts.
        expect_stats: Some(|s| {
            if s.num_deletes != 3 {
                return Err(format!("expected num_deletes=3, got {}", s.num_deletes));
            }
            if s.num_updates != 3 {
                return Err(format!("expected num_updates=3, got {}", s.num_updates));
            }
            if s.num_inserts != 0 {
                return Err(format!("expected num_inserts=0, got {}", s.num_inserts));
            }
            Ok(())
        }),
        ..Default::default()
    }
);

// =============================================================================
// MOR File Slice Layout Tests (harness cases)
//
// These cases read the 4 MOR v9 tables created by Hudi Spark's
// TestMORFileSliceLayouts and validate the HoodieFileGroupReader output
// against the gold data (SELECT * result saved as parquet).
//
// All tables: v9, MOR, COMMIT_TIME_ORDERING, non-partitioned, 1 file group.
// =============================================================================

// Log compaction layout: 1 file group, NO base file, 5 log files including a
// compacted log block. Validated against gold (Spark SELECT * snapshot).
fg_case_test!(
    harness_log_compaction,
    FgReaderCase {
        name: "log_compaction",
        fixture: QuickstartTripsTable::MorLayoutLogCompaction,
        partition: "",
        base_file: "",
        log_files: &[
            ".7483a08a-02f1-4510-bc1d-1317924f4189-0_20260409030511461.log.1_0-16-23",
            ".7483a08a-02f1-4510-bc1d-1317924f4189-0_20260409030518232.log.1_0-30-49",
            ".7483a08a-02f1-4510-bc1d-1317924f4189-0_20260409030519923.log.1_0-44-78",
            ".7483a08a-02f1-4510-bc1d-1317924f4189-0_20260409030521407.log.1_0-58-110",
            ".7483a08a-02f1-4510-bc1d-1317924f4189-0_20260409030522412.log.1_0-67-128",
        ],
        expected: Expected::GoldParquet,
        ..Default::default()
    }
);

// Log-only layout: NO base file, 3 log files (insert + update + delete block);
// k3 deleted. Validated against gold.
fg_case_test!(
    harness_log_only,
    FgReaderCase {
        name: "log_only",
        fixture: QuickstartTripsTable::MorLayoutLogOnly,
        partition: "",
        base_file: "",
        log_files: &[
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030525348.log.1_0-102-176",
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030527298.log.1_0-116-202",
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030528554.log.1_0-130-231",
        ],
        expected: Expected::GoldParquet,
        ..Default::default()
    }
);

// Mixed column types (int/string/double/array/map/...): base + 2 logs
// (update + delete); k3 deleted. Validated against gold.
fg_case_test!(
    harness_mixed_column_types,
    FgReaderCase {
        name: "mixed_column_types",
        fixture: QuickstartTripsTable::MorLayoutColumnProjection,
        partition: "",
        base_file: "78076137-b2c4-410d-8473-3d6366ae0985-0_0-165-279_20260409030530945.parquet",
        log_files: &[
            ".78076137-b2c4-410d-8473-3d6366ae0985-0_20260409030532996.log.1_0-179-305",
            ".78076137-b2c4-410d-8473-3d6366ae0985-0_20260409030534379.log.1_0-193-334",
        ],
        expected: Expected::GoldParquet,
        ..Default::default()
    }
);

// All data types incl. logical types: base + 3 logs (update + delete + update);
// k4 deleted. Validated against gold.
fg_case_test!(
    harness_all_data_types,
    FgReaderCase {
        name: "all_data_types",
        fixture: QuickstartTripsTable::MorLayoutAllDataTypes,
        partition: "",
        base_file: "c887c1e8-5fb9-475e-8171-769c5cf10c61-0_0-240-395_20260409030537482.parquet",
        log_files: &[
            ".c887c1e8-5fb9-475e-8171-769c5cf10c61-0_20260409030539332.log.1_0-254-421",
            ".c887c1e8-5fb9-475e-8171-769c5cf10c61-0_20260409030540482.log.1_0-268-450",
            ".c887c1e8-5fb9-475e-8171-769c5cf10c61-0_20260409030541620.log.1_0-282-482",
        ],
        expected: Expected::GoldParquet,
        ..Default::default()
    }
);

// =============================================================================
// Harness cases: column projection through explicit-args and builder paths
// =============================================================================

fn requested_id_name() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]))
}

// Projection through the explicit-args path: data schema derived from the
// base parquet footer, requested schema passed to the constructor. Schema
// handler must add mandatory merge fields internally, read base pruned, and
// project the output back to exactly [id, name].
fg_case_test!(
    harness_projection_via_schema_handler,
    FgReaderCase {
        name: "projection_via_schema_handler",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        log_files: &[".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        schema: SchemaSpec::Projection(requested_id_name()),
        expect_output_columns: Some(&["id", "name"]),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name"],
            rows: &[&["1", "Alice-V2"], &["2", "Bob"]],
        },
        ..Default::default()
    }
);

// Projection through the FFI/builder path: schemas live on
// `ReaderContext.schema_handler`, reader built via `.builder()` with NO
// explicit schemas. This is the case that would have caught the original
// bug where the builder created an empty schema handler (required_schema =
// None → base read unpruned, no output projection).
fg_case_test!(
    harness_projection_via_builder,
    FgReaderCase {
        name: "projection_via_builder",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        log_files: &[".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        schema: SchemaSpec::BuilderProjection(requested_id_name()),
        expect_output_columns: Some(&["id", "name"]),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name"],
            rows: &[&["1", "Alice-V2"], &["2", "Bob"]],
        },
        ..Default::default()
    }
);

// COW path (no log files) through the builder: `generate_required_schema`
// returns requested as-is; output must be exactly [id, age], original values.
fg_case_test!(
    harness_cow_projection_via_builder,
    FgReaderCase {
        name: "cow_projection_via_builder",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        log_files: &[],
        schema: SchemaSpec::BuilderProjection(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("age", DataType::Int32, true),
        ]))),
        expect_output_columns: Some(&["id", "age"]),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "age"],
            rows: &[&["1", "30"], &["2", "25"]],
        },
        ..Default::default()
    }
);

// Minimal projection: single user column; merge-internal fields
// (_hoodie_record_key etc.) added internally must be stripped from output.
fg_case_test!(
    harness_mor_single_column_projection_via_builder,
    FgReaderCase {
        name: "mor_single_column_projection_via_builder",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        log_files: &[".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        schema: SchemaSpec::BuilderProjection(Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Utf8,
            true
        ),]))),
        expect_output_columns: Some(&["name"]),
        expected: Expected::Rows {
            sort_key: "name",
            columns: &["name"],
            rows: &[&["Alice-V2"], &["Bob"]],
        },
        ..Default::default()
    }
);

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

    handler
        .prepare_required_schema(
            true, // has_log_files (MOR)
            &["_hoodie_record_key".to_string()],
            &[], // no ordering fields for COMMIT_TIME_ORDERING
            &props,
            false,
            "COMMIT_TIME_ORDERING",
        )
        .unwrap();

    let required = handler
        .required_schema
        .as_ref()
        .expect("required_schema should be computed");
    let required_cols: Vec<&str> = required
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    // Must include user-requested columns.
    assert!(required_cols.contains(&"id"), "must include requested 'id'");
    assert!(
        required_cols.contains(&"name"),
        "must include requested 'name'"
    );

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

    let requested_schema: SchemaRef =
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));

    let mut handler = FileGroupReaderSchemaHandler::new()
        .with_table_schema(table_schema.clone())
        .with_data_schema(table_schema)
        .with_requested_schema(requested_schema.clone());

    handler
        .prepare_required_schema(
            false, // no log files (COW)
            &["_hoodie_record_key".to_string()],
            &[],
            &std::collections::HashMap::new(),
            false,
            "COMMIT_TIME_ORDERING",
        )
        .unwrap();

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
    let (_hudi_configs, storage) = fg_harness::create_configs_and_storage(&table_path).await?;

    let base_file =
        "city=sf/fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet";

    // Read the full parquet schema.
    let full_schema: SchemaRef = Arc::new(
        crate::file_group::base_file::parquet::ParquetBaseFileReader::new(storage.clone())
            .get_schema(base_file)
            .await?,
    );
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
    reader_context.latest_commit_time = MAX_INSTANT_TIME.to_string();
    reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
    reader_context.table_config.insert(
        HudiTableConfig::OrderingFields.as_ref().to_string(),
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
        result.num_columns(),
        2,
        "builder path with schema_handler: output should have only 2 columns, \
         not all {full_col_count} parquet columns"
    );

    let schema = result.schema();
    let output_col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(output_col_names, vec!["id", "name"]);

    Ok(())
}

// =============================================================================
// NULL elements inside containers (table_null_containers fixture)
//
// Mirrors the Gluten/FFI read path of TestMORFileSliceLayoutsExtendedTypes
// "2. NULL elements in containers and empty containers": the data/requested
// schemas are derived from the table's Avro schema JSON (as passed over FFI),
// the base parquet holds arrays without NULL elements, and the log file holds
// an UPDATE whose array carries a NULL element ([1, NULL, 3]).
//
// The Avro schema declares array items as the nullable union ["null","int"],
// so the merged read must preserve the NULL element.
// =============================================================================

/// Table Avro schema of the `table_null_containers` fixture (verbatim from the
/// base parquet footer's `parquet.avro.schema`, which is what the Gluten side
/// passes over FFI as `data_schema_json` / `requested_schema_json`).
const NULL_CONTAINERS_AVRO_JSON: &str = r#"{"type":"record","name":"h2_record","namespace":"hoodie.h2","fields":[{"name":"_hoodie_commit_time","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_commit_seqno","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_record_key","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_partition_path","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_file_name","type":["null","string"],"doc":"","default":null},{"name":"id","type":["null","int"],"default":null},{"name":"arr_null_elem","type":["null",{"type":"array","items":["null","int"]}],"default":null},{"name":"map_null_val","type":["null",{"type":"map","values":["null","int"]}],"default":null},{"name":"st_null_field","type":["null",{"type":"record","name":"st_null_field","namespace":"hoodie.h2.h2_record","fields":[{"name":"a","type":["null","int"],"default":null},{"name":"b","type":["null","string"],"default":null}]}],"default":null},{"name":"arr_empty","type":["null",{"type":"array","items":["null","string"]}],"default":null},{"name":"map_empty","type":["null",{"type":"map","values":["null","int"]}],"default":null},{"name":"emptyinit_arr","type":["null",{"type":"array","items":["null","int"]}],"default":null},{"name":"ts","type":["null","long"],"default":null}]}"#;

/// Extract (id -> Vec<Option<i32>>) for an INT-array column, sorted by id.
///
/// Narrow contract: caller guarantees `id` (Int32) and `col_name` (ListArray
/// of Int32) are present in `batch`. A panic here is an acceptable test-failure
/// UX because a missing/wrong-typed column would indicate fixture or schema
/// regression, not a harness bug.
fn id_to_int_array(
    batch: &arrow_array::RecordBatch,
    col_name: &str,
) -> Vec<(i32, Vec<Option<i32>>)> {
    use arrow_array::{Array, Int32Array, ListArray};
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let arrs = batch
        .column_by_name(col_name)
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap_or_else(|| panic!("{col_name} must be a ListArray"));
    let mut rows: Vec<(i32, Vec<Option<i32>>)> = (0..batch.num_rows())
        .map(|row| {
            let values = arrs.value(row);
            let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
            (ids.value(row), values.iter().collect())
        })
        .collect();
    rows.sort_by_key(|(id, _)| *id);
    rows
}

/// `(id, list-of-nullable-strings)` rows, keyed and sorted by id.
type IdStrListRows = Vec<(i32, Vec<Option<String>>)>;
/// `(id, map entries as (key, nullable-value))` rows, keyed and sorted by id.
type IdIntMapRows = Vec<(i32, Vec<(String, Option<i32>)>)>;
/// `(id, struct {a, b} as nullable fields)` rows, keyed and sorted by id.
type IdStructAbRows = Vec<(i32, (Option<i32>, Option<String>))>;

/// Extract (id -> Vec<Option<String>>) for a STRING-array (`list<utf8>`) column,
/// sorted by id. Same narrow contract as [`id_to_int_array`].
fn id_to_str_array(batch: &arrow_array::RecordBatch, col_name: &str) -> IdStrListRows {
    use arrow_array::{Array, Int32Array, ListArray, StringArray};
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let arrs = batch
        .column_by_name(col_name)
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap_or_else(|| panic!("{col_name} must be a ListArray"));
    let mut rows: IdStrListRows = (0..batch.num_rows())
        .map(|row| {
            let values = arrs.value(row);
            let values = values.as_any().downcast_ref::<StringArray>().unwrap();
            (
                ids.value(row),
                values.iter().map(|v| v.map(str::to_string)).collect(),
            )
        })
        .collect();
    rows.sort_by_key(|(id, _)| *id);
    rows
}

/// Extract (id -> Vec<(key, Option<value>)>) for a `map<string,int>` column,
/// sorted by id. A null map value surfaces as `None`. Same narrow contract as
/// [`id_to_int_array`].
fn id_to_int_map(batch: &arrow_array::RecordBatch, col_name: &str) -> IdIntMapRows {
    use arrow_array::{Array, Int32Array, MapArray, StringArray};
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let maps = batch
        .column_by_name(col_name)
        .unwrap()
        .as_any()
        .downcast_ref::<MapArray>()
        .unwrap_or_else(|| panic!("{col_name} must be a MapArray"));
    let mut rows: IdIntMapRows = (0..batch.num_rows())
        .map(|row| {
            let entries = maps.value(row);
            let keys = entries
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let vals = entries
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let pairs = (0..entries.len())
                .map(|i| {
                    (
                        keys.value(i).to_string(),
                        (!vals.is_null(i)).then(|| vals.value(i)),
                    )
                })
                .collect();
            (ids.value(row), pairs)
        })
        .collect();
    rows.sort_by_key(|(id, _)| *id);
    rows
}

/// Extract (id -> (Option<a>, Option<b>)) for the `st_null_field` struct column
/// (fields `a: int`, `b: string`), sorted by id. A null field surfaces as
/// `None`. Same narrow contract as [`id_to_int_array`].
fn id_to_struct_ab(batch: &arrow_array::RecordBatch, col_name: &str) -> IdStructAbRows {
    use arrow_array::{Array, Int32Array, StringArray, StructArray};
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let st = batch
        .column_by_name(col_name)
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap_or_else(|| panic!("{col_name} must be a StructArray"));
    let a = st
        .column_by_name("a")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let b = st
        .column_by_name("b")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut rows: IdStructAbRows = (0..batch.num_rows())
        .map(|row| {
            let av = (!a.is_null(row)).then(|| a.value(row));
            let bv = (!b.is_null(row)).then(|| b.value(row).to_string());
            (ids.value(row), (av, bv))
        })
        .collect();
    rows.sort_by_key(|(id, _)| *id);
    rows
}

fn validate_null_containers(batch: &arrow_array::RecordBatch) -> std::result::Result<(), String> {
    // Row count is enforced by the harness (Expected::Custom { rows: 12, .. }).
    // arr_null_elem: id=1 updated via log to [1, NULL, 3]; others keep [id, id+1].
    let mut expected: Vec<(i32, Vec<Option<i32>>)> = vec![(1, vec![Some(1), None, Some(3)])];
    expected.extend((2..=12).map(|id| (id, vec![Some(id), Some(id + 1)])));
    let actual = id_to_int_array(batch, "arr_null_elem");
    if actual != expected {
        return Err(format!(
            "arr_null_elem mismatch:\n actual={actual:?}\n expected={expected:?}"
        ));
    }
    // emptyinit_arr stays an empty (non-null) array on every row.
    let expected_empty: Vec<(i32, Vec<Option<i32>>)> = (1..=12).map(|id| (id, vec![])).collect();
    let actual_empty = id_to_int_array(batch, "emptyinit_arr");
    if actual_empty != expected_empty {
        return Err(format!("emptyinit_arr mismatch: {actual_empty:?}"));
    }
    // ts: id=1 bumped to 101 by the update; all others keep ts=id.
    let ids = batch
        .column_by_name("id")
        .ok_or("no id column")?
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .ok_or("id not Int32")?;
    let tss = batch
        .column_by_name("ts")
        .ok_or("no ts column")?
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .ok_or("ts not Int64")?;
    let mut ts_rows: Vec<(i32, i64)> = ids
        .iter()
        .zip(tss.iter())
        .map(|(id, ts)| (id.unwrap(), ts.unwrap()))
        .collect();
    ts_rows.sort_by_key(|(id, _)| *id);
    let expected_ts: Vec<(i32, i64)> = std::iter::once((1, 101i64))
        .chain((2..=12).map(|id| (id, id as i64)))
        .collect();
    if ts_rows != expected_ts {
        return Err(format!("ts mismatch: {ts_rows:?}"));
    }
    // map_null_val and map_empty: each row id carries the single entry
    // {"k{id}": id} (the id=1 update keeps the same entry — see decoded log).
    let expected_map: IdIntMapRows = (1..=12)
        .map(|id| (id, vec![(format!("k{id}"), Some(id))]))
        .collect();
    let actual_map_null = id_to_int_map(batch, "map_null_val");
    if actual_map_null != expected_map {
        return Err(format!(
            "map_null_val mismatch:\n actual={actual_map_null:?}\n expected={expected_map:?}"
        ));
    }
    let actual_map_empty = id_to_int_map(batch, "map_empty");
    if actual_map_empty != expected_map {
        return Err(format!(
            "map_empty mismatch:\n actual={actual_map_empty:?}\n expected={expected_map:?}"
        ));
    }
    // arr_empty: each row id carries the single-element list ["v{id}"].
    let expected_arr_empty: IdStrListRows = (1..=12)
        .map(|id| (id, vec![Some(format!("v{id}"))]))
        .collect();
    let actual_arr_empty = id_to_str_array(batch, "arr_empty");
    if actual_arr_empty != expected_arr_empty {
        return Err(format!(
            "arr_empty mismatch:\n actual={actual_arr_empty:?}\n expected={expected_arr_empty:?}"
        ));
    }
    // st_null_field: each row id carries the struct {a: id, b: "b{id}"}.
    let expected_st: IdStructAbRows = (1..=12)
        .map(|id| (id, (Some(id), Some(format!("b{id}")))))
        .collect();
    let actual_st = id_to_struct_ab(batch, "st_null_field");
    if actual_st != expected_st {
        return Err(format!(
            "st_null_field mismatch:\n actual={actual_st:?}\n expected={expected_st:?}"
        ));
    }
    Ok(())
}

// NULL elements inside containers, read through the FFI-style explicit-schema
// path: data/requested schemas derived from the table's Avro schema JSON
// (what Gluten passes over FFI). Base has arrays without NULLs; the log
// UPDATE carries [1, NULL, 3] — the NULL element must survive the merge.
fn case_null_containers() -> FgReaderCase {
    let avro_derived: SchemaRef = Arc::new(
        crate::schema::resolver::avro_json_to_arrow_schema(NULL_CONTAINERS_AVRO_JSON)
            .expect("fixture avro schema must parse"),
    );
    FgReaderCase {
        name: "null_container_elements",
        fixture: QuickstartTripsTable::MorLayoutNullContainers,
        partition: "",
        base_file: "11e826d2-dd6e-4463-b65a-2924f2faf501-0_0-290-1002_20260602220651604.parquet",
        log_files: &[".11e826d2-dd6e-4463-b65a-2924f2faf501-0_20260602220652266.log.1_0-299-1016"],
        schema: SchemaSpec::Explicit {
            data: avro_derived.clone(),
            requested: avro_derived,
        },
        expected: Expected::Custom {
            rows: 12,
            validate: validate_null_containers,
        },
        ..Default::default()
    }
}

fg_case_test!(harness_null_container_elements, case_null_containers());

// Unhappy path: a log file that does not exist must surface a loud read
// error (storage-level), never an empty/partial result. P3 adds the
// unsupported-feature cases (IsPartial, event-time ordering, ...) here.
fg_case_test!(
    harness_missing_log_file_errors,
    FgReaderCase {
        name: "missing_log_file_errors",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        log_files: &[".does-not-exist-0000.log.1_0-0-0"],
        expected: Expected::ErrContains("does-not-exist"),
        ..Default::default()
    }
);

// =============================================================================
// Instant-range + latest-commit-time watermark coverage (gold block-filter
// gates). Read configs are set on the case via the harness extension; expected
// values follow GOLD semantics (D-P2-5). Any hudi-rs divergence is captured as
// an #[ignore] + reviewNotes/p2-findings.md entry — never an expectation weaken.
//
// -- V9MorNonpart3Commits timeline & data (verified from the extracted zip) ---
// c1 = 20260409002001492  base (insert), 7 rows ids 0-6
// c2 = 20260409002002957  log block 1: DELETE ids 0,1,2
// c3 = 20260409002003963  log block 2: UPDATE ids 4,5,6
//
// Base parquet original values (read directly from the c1 base file, pre-merge):
//   id | name | price | ts
//    0 |  X   | 20.0  | 100
//    1 |  A   | 10.0  | 100
//    2 |  B   | 20.0  | 100
//    3 |  C   | 30.0  | 100
//    4 |  D   | 40.0  | 100
//    5 |  E   | 50.0  | 100
//    6 |  F   | 60.0  | 100
// c3 rewrites ids 4,5,6 -> D2/E2/F2 at prices 45.0/55.0/65.0 (see
// harness_v9_mor_nonpart_multi_log, which reads all 3 commits).
//
// -- V9Mor8I4UCommitTime, partition city=sf -----------------------------------
// base = 20260408053032350  ids 1,2 -> Alice(30), Bob(25)
// log  = 20260408053037787  UPDATE id=1 -> Alice-V2(31); id=2 untouched
// =============================================================================

const NONPART_C1: &str = "20260409002001492";
const NONPART_C2: &str = "20260409002002957";
const I8I4U_BASE: &str = "20260408053032350";
const I8I4U_LOG: &str = "20260408053037787";

const NONPART_BASE_FILE: &str =
    "960a29a0-0f78-401d-85b1-1cbc44b34121-0_0-846-1597_20260409002001492.parquet";
const NONPART_LOG_FILES: &[&str] = &[
    ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002002957.log.1_0-868-1644",
    ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002003963.log.1_0-890-1691",
];
const I8I4U_SF_BASE: &str =
    "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet";
const I8I4U_SF_LOG: &str =
    ".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73";

// (a) Watermark = c2: the c3 UPDATE block (INSTANT_TIME=c3 > c2) is a FUTURE
// block (gold gate 2) and is excluded. The c2 DELETE block still applies.
// => ids 0,1,2 deleted; ids 4,5,6 keep ORIGINAL values; id=3 untouched.
fg_case_test!(
    harness_watermark_excludes_future_blocks,
    FgReaderCase {
        name: "watermark_excludes_future_blocks",
        fixture: QuickstartTripsTable::V9MorNonpart3Commits,
        partition: "",
        base_file: NONPART_BASE_FILE,
        log_files: NONPART_LOG_FILES,
        latest_commit_time: Some(NONPART_C2),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "price"],
            rows: &[
                &["3", "C", "30.0"],
                &["4", "D", "40.0"],
                &["5", "E", "50.0"],
                &["6", "F", "60.0"],
            ],
        },
        ..Default::default()
    }
);

// (b) Watermark = c1: BOTH log blocks (c2 delete, c3 update) are future blocks
// and excluded. => original 7 base rows ids 0-6, unchanged.
fg_case_test!(
    harness_watermark_excludes_all_logs,
    FgReaderCase {
        name: "watermark_excludes_all_logs",
        fixture: QuickstartTripsTable::V9MorNonpart3Commits,
        partition: "",
        base_file: NONPART_BASE_FILE,
        log_files: NONPART_LOG_FILES,
        latest_commit_time: Some(NONPART_C1),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "price"],
            rows: &[
                &["0", "X", "20.0"],
                &["1", "A", "10.0"],
                &["2", "B", "20.0"],
                &["3", "C", "30.0"],
                &["4", "D", "40.0"],
                &["5", "E", "50.0"],
                &["6", "F", "60.0"],
            ],
        },
        ..Default::default()
    }
);

// (c) instant_range up_to(c2) (inclusive end): the c3 UPDATE block's
// INSTANT_TIME=c3 is outside the range and gold gate 3 skips it; the c2 DELETE
// block (INSTANT_TIME=c2, in range) applies. Watermark left at default.
// => same expectation as case (a).
fn instant_range_up_to_c2() -> InstantRange {
    InstantRange::up_to(NONPART_C2, "UTC")
}

fg_case_test!(
    harness_instant_range_up_to_c2,
    FgReaderCase {
        name: "instant_range_up_to_c2",
        fixture: QuickstartTripsTable::V9MorNonpart3Commits,
        partition: "",
        base_file: NONPART_BASE_FILE,
        log_files: NONPART_LOG_FILES,
        instant_range: Some(instant_range_up_to_c2),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "price"],
            rows: &[
                &["3", "C", "30.0"],
                &["4", "D", "40.0"],
                &["5", "E", "50.0"],
                &["6", "F", "60.0"],
            ],
        },
        ..Default::default()
    }
);

// (d) instant_range within_open_closed(base, log] on V9Mor8I4UCommitTime sf:
// the base commit (INSTANT_TIME=base) is EXCLUDED (open start) so base rows are
// filtered out by _hoodie_commit_time; the log commit (INSTANT_TIME=log) is in
// range. GOLD: log records in range are emitted standalone. The log carries
// only the id=1 UPDATE (Alice-V2/31); id=2 has no log record, so with its base
// row filtered out it disappears. => only id=1 Alice-V2.
fn instant_range_excludes_base() -> InstantRange {
    InstantRange::within_open_closed(I8I4U_BASE, I8I4U_LOG, "UTC")
}

fg_case_test!(
    harness_instant_range_excludes_base,
    FgReaderCase {
        name: "instant_range_excludes_base",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: I8I4U_SF_BASE,
        log_files: &[I8I4U_SF_LOG],
        instant_range: Some(instant_range_excludes_base),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "age"],
            rows: &[&["1", "Alice-V2", "31"]],
        },
        ..Default::default()
    }
);

// (e) instant_range up_to(base) on V9Mor8I4UCommitTime sf: the log commit
// (INSTANT_TIME=log > base) is outside the range and skipped; the base commit
// (INSTANT_TIME=base, inclusive end) is in range. => original Alice(30)/Bob(25).
fn instant_range_up_to_base() -> InstantRange {
    InstantRange::up_to(I8I4U_BASE, "UTC")
}

fg_case_test!(
    harness_instant_range_up_to_base,
    FgReaderCase {
        name: "instant_range_up_to_base",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: I8I4U_SF_BASE,
        log_files: &[I8I4U_SF_LOG],
        instant_range: Some(instant_range_up_to_base),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "age"],
            rows: &[&["1", "Alice", "30"], &["2", "Bob", "25"]],
        },
        ..Default::default()
    }
);

// =============================================================================
// Supported-matrix boundary: unsupported-config loud-error cases (design.md §1)
//
// The normative supported matrix: table v9, MOR snapshot,
// COMMIT_TIME_ORDERING ONLY, schema-on-write only.
// These cases prove the boundary — unsupported configs must error loudly.
//
// Fixture: V9Mor8I4UCommitTime, partition city=sf (base + log; MOR merge path).
// =============================================================================

// EVENT_TIME_ORDERING on a MOR fixture (ENG-38318: now SUPPORTED).
// buffer/loader.rs accepts EVENT_TIME_ORDERING and KeyBasedFileGroupRecordBuffer
// merges base-vs-log by ordering value (the base record now carries its ordering
// value — has_next_base_record_keyed). For this 8I4U fixture the updates carry the
// latest ordering, so EVENT_TIME picks the same winners as COMMIT_TIME — expected
// rows match case_sf_merge().
fg_case_test!(
    harness_v9_mor_event_time_sf_merge,
    FgReaderCase {
        name: "v9_mor_event_time_sf_merge",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        log_files: &[".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        merge_mode: Some("EVENT_TIME_ORDERING"),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "age"],
            rows: &[&["1", "Alice-V2", "31"], &["2", "Bob", "25"]],
        },
        ..Default::default()
    }
);

// CUSTOM merge mode on a MOR fixture.
// Per design.md §1, only COMMIT_TIME_ORDERING is supported; CUSTOM (partial-update
// / custom merger) is not implemented. The loader.rs guard rejects it with:
// "Unsupported merge mode: 'CUSTOM'. Only COMMIT_TIME_ORDERING is supported
// (table v9 MOR scan path)." — stable substring: "Unsupported merge mode".
fg_case_test!(
    harness_unsupported_custom_merge_mode,
    FgReaderCase {
        name: "unsupported_custom_merge_mode",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        log_files: &[".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        merge_mode: Some("CUSTOM"),
        expected: Expected::ErrContains("Unsupported merge mode"),
        ..Default::default()
    }
);

// use_record_position=true on a MOR fixture with a base file.
// Position-based merging is implemented, so the loader would take the position-buffer
// arm — but that arm needs the base file's commit time. The harness supplies only file
// paths (no base_file_commit_time — the real FFI derives it from the base file name), so
// the position-merge gate is not satisfied and the read gracefully falls back to
// key-based merge (always correct, needs no commit time) instead of hard-erroring.
// The result must therefore match the plain key-based read of this exact slice
// (`case_sf_merge`): id=1 updated to Alice-V2/31, id=2 (Bob/25) carried from the base.
fg_case_test!(
    harness_record_position_falls_back_key_based,
    FgReaderCase {
        name: "record_position_falls_back_key_based",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        log_files: &[".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        reader_parameters: Some(ReaderParameters {
            use_record_position: true,
            ..Default::default()
        }),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "age"],
            rows: &[&["1", "Alice-V2", "31"], &["2", "Bob", "25"]],
        },
        ..Default::default()
    }
);

// emit_delete=true on a MOR fixture (GAP-06).
// Per design.md §1, the supported read path drops deletes from the merged output;
// emitting delete records (gold's emitDeletes / RecordContext.getDeleteRow) is not
// implemented. The loader.rs guard rejects it with:
// "emit_delete=true (emitting delete records into the output) is not yet
// implemented; ..." — stable substring: "emit_delete".
fg_case_test!(
    harness_unsupported_emit_delete,
    FgReaderCase {
        name: "unsupported_emit_delete",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        log_files: &[".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
        reader_parameters: Some(ReaderParameters {
            emit_delete: true,
            ..Default::default()
        }),
        expected: Expected::ErrContains("emit_delete"),
        ..Default::default()
    }
);

// =============================================================================
// Filter-pushdown e2e coverage via the Rust-reachable channel (D-P2-1):
//   HoodieFileGroupReaderBuilder::with_row_filter_builder + with_mor_pk_safe.
//
// Declarative `RowFilterSpec` on the case is compiled by the harness into a
// parquet `RowFilter` (one `ArrowPredicateFn`); filtered cases route through
// the builder/projected path (the only path that threads the filter onto the
// base parquet read). Poison principle: if the filter is NOT applied, extra
// rows survive and the exact-rows assert fails.
//
// Gold contract (SparkFileFormatInternalRowReaderContext.filterIsSafeForPrimaryKey):
//   - PK-safe filters (record-key columns) may be pushed under MOR merge.
//   - Data-column filters may be pushed only on the CoW path (no log files).
//   - The gate `can_push_row_filter() = is_cow() || mor_pk_safe` blocks unsafe
//     pushes; when blocked, ALL rows return and the post-merge filter (above
//     the FG reader, e.g. Velox/Spark) evaluates the predicate.
//
// Record-key format for V9Mor8I4UCommitTime: plain id value ("1", "2", ...)
// (verified by reading the sf base parquet's _hoodie_record_key column).
// =============================================================================

// (1) PK eq under MOR merge: `_hoodie_record_key` Eq "1", mor_pk_safe=true.
// The gate is open (pk-safe), so the filter is pushed onto the base parquet:
// only the id=1 base row survives, then the log UPDATE applies -> Alice-V2(31).
// If the filter were NOT applied, Bob(25) would leak and the assert fails.
fg_case_test!(
    harness_filter_pk_eq_mor_merge,
    FgReaderCase {
        name: "filter_pk_eq_mor_merge",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: I8I4U_SF_BASE,
        log_files: &[I8I4U_SF_LOG],
        expect_output_columns: Some(&["id", "name", "age"]),
        row_filter: Some(RowFilterSpec {
            column: "_hoodie_record_key",
            predicate: FilterPredicate::Eq("1"),
            mor_pk_safe: true,
        }),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "age"],
            rows: &[&["1", "Alice-V2", "31"]],
        },
        ..Default::default()
    }
);

// (2) PK range filter under MOR merge: `_hoodie_record_key` Lt "2",
// mor_pk_safe=true. Keys "1","2" compared as strings: only "1" < "2".
// The gate is open (pk-safe), so the filter pushes onto the base parquet:
// only the id=1 base row survives; the (avro) log UPDATE for id=1 then applies
// -> Alice-V2(31). Unfiltered would be {Alice-V2, Bob} (2 rows), so this
// exact-row assert poisons if the base pushdown did not engage.
//
// NB: a PK *Gt* "1" filter is NOT discriminating on this fixture. Its log is
// an AVRO data block, and the gold contract pushes the filter only to base
// parquet + PARQUET log blocks (never avro blocks). Gt "1" prunes id=1 from the
// base, but the unfiltered avro log re-introduces id=1's UPDATE during merge,
// so the result is {Alice-V2, Bob} — identical to unfiltered. That is
// gold-correct (the merge must not lose a log-sourced record), just not a
// poison test; Lt "2" is used here because base+log agree on id=1.
// See reviewNotes/p2-findings.md (T3 note) for the avro-log pushdown scope.
fg_case_test!(
    harness_filter_pk_lt_mor_merge,
    FgReaderCase {
        name: "filter_pk_lt_mor_merge",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: I8I4U_SF_BASE,
        log_files: &[I8I4U_SF_LOG],
        expect_output_columns: Some(&["id", "name", "age"]),
        row_filter: Some(RowFilterSpec {
            column: "_hoodie_record_key",
            predicate: FilterPredicate::Lt("2"),
            mor_pk_safe: true, // range filter on a PK column is pk-safe -> may push under merge
        }),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "age"],
            rows: &[&["1", "Alice-V2", "31"]],
        },
        ..Default::default()
    }
);

// (3) Data-column filter on the CoW path (base-only, no logs -> is_cow gate):
// `age` Gt "27", mor_pk_safe=false. The gate is open via is_cow(), so the
// filter is pushed: Alice(30) survives, Bob(25) is filtered out.
fg_case_test!(
    harness_filter_data_col_cow,
    FgReaderCase {
        name: "filter_data_col_cow",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: I8I4U_SF_BASE,
        log_files: &[], // base-only => COW path => can_push_row_filter via is_cow()
        expect_output_columns: Some(&["id", "name", "age"]),
        row_filter: Some(RowFilterSpec {
            column: "age",
            predicate: FilterPredicate::Gt("27"),
            mor_pk_safe: false,
        }),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "age"],
            rows: &[&["1", "Alice", "30"]],
        },
        ..Default::default()
    }
);

// (4) Logical/typed-column filter on the CoW path: MorLayoutAllDataTypes
// base-only read, `long_field` (Int64) Gt "250". Base values long_field =
// 100,200,300,400,500 for keys k1..k5 (read from the base parquet, pre-merge).
// => k3,k4,k5 (300,400,500) survive.
fg_case_test!(
    harness_filter_logical_type_cow,
    FgReaderCase {
        name: "filter_logical_type_cow",
        fixture: QuickstartTripsTable::MorLayoutAllDataTypes,
        partition: "",
        base_file: "c887c1e8-5fb9-475e-8171-769c5cf10c61-0_0-240-395_20260409030537482.parquet",
        log_files: &[], // base-only => COW path
        expect_output_columns: Some(&["key", "long_field", "severity"]),
        row_filter: Some(RowFilterSpec {
            column: "long_field",
            predicate: FilterPredicate::Gt("250"),
            mor_pk_safe: false,
        }),
        expected: Expected::Rows {
            sort_key: "key",
            columns: &["key", "long_field", "severity"],
            rows: &[
                &["k3", "300", "3"],
                &["k4", "400", "4"],
                &["k5", "500", "5"],
            ],
        },
        ..Default::default()
    }
);

// (5) NEGATIVE gate case: data-column filter under MOR merge with
// mor_pk_safe=false. `age` Gt "27" — pushing this under merge could DROP a
// base row (Bob, age 25) whose log update might later have made it match, so
// the gate (can_push_row_filter = is_cow() || mor_pk_safe = false) BLOCKS the
// push. The post-merge filter (above the FG reader) is responsible instead, so
// the FG reader returns ALL merged rows: Alice-V2(31) AND Bob(25).
// (Unsafe because the predicate evaluated on BASE values can disagree with post-merge values for
// the same key: a log update may change the column so a pruned base row would have matched after
// merge.)
fg_case_test!(
    harness_filter_unsafe_not_pushed_mor,
    FgReaderCase {
        name: "filter_unsafe_not_pushed_mor",
        fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
        partition: "city=sf",
        base_file: I8I4U_SF_BASE,
        log_files: &[I8I4U_SF_LOG], // MOR merge => is_cow() false
        expect_output_columns: Some(&["id", "name", "age"]),
        row_filter: Some(RowFilterSpec {
            column: "age",
            predicate: FilterPredicate::Gt("27"),
            mor_pk_safe: false, // gate closed: filter NOT pushed
        }),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "age"],
            rows: &[&["1", "Alice-V2", "31"], &["2", "Bob", "25"]],
        },
        ..Default::default()
    }
);

// ---------------------------------------------------------------------------
// Tier-2 fixtures (P2 Task 5): corrupt-tail / parquet-log / partial / hfile.
// Provenance: hudi-internal `TestMORFileSliceLayoutsFixturesV2` (commit
// 309a0b287e), self-validating layout generator. All v9 MOR,
// COMMIT_TIME_ORDERING, non-partitioned; schema key STRING / ts LONG /
// value STRING / num INT. See the `QuickstartTripsTable` doc comments.
// ---------------------------------------------------------------------------

// (a) Corrupt tail block: base + 2 AVRO logs, the second followed by appended
// garbage forming a CORRUPT tail block. Gold (4 rows) reflects the data with
// the corrupt tail ignored; the corrupt block should surface LOUDLY in
// read_stats (total_corrupt_log_blocks >= 1) rather than aborting the read —
// matching gold/Java HoodieLogFileReader semantics (Gate 1: corrupt → skip).
//
// RESOLVED (T5-1, P3 Task 4): hudi-rs now recovers from a corrupt tail block,
// matching gold/Java HoodieLogFileReader. The reader validates each block via a
// footer reverse-pointer probe (`LogFileReader::is_block_corrupted`) before
// parsing its body; on an anomaly (oversized/truncated block length, footer
// mismatch, or missing next MAGIC) it synthesizes a `BlockType::Corrupted`
// block spanning to the next MAGIC marker or EOF and re-seeks there to continue
// scanning (`create_corrupted_block` + `scan_for_next_block_offset`). Gate 1 in
// `log_record_reader.rs` then skips the corrupt block while incrementing
// `total_corrupt_blocks`, surfaced as `total_corrupt_log_blocks` in read_stats.
// Net: the 4 valid rows are read and the corrupt tail is counted, not fatal.
fg_case_test!(
    harness_corrupt_tail_block_skipped,
    FgReaderCase {
        name: "corrupt_tail_block_skipped",
        fixture: QuickstartTripsTable::MorLayoutCorruptTailBlock,
        partition: "",
        base_file: "5ae929e7-c845-4c91-8877-d5d0c85cdef2-0_0-16-25_20260607061218483.parquet",
        log_files: &[
            ".5ae929e7-c845-4c91-8877-d5d0c85cdef2-0_20260607061226267.log.1_0-30-51",
            ".5ae929e7-c845-4c91-8877-d5d0c85cdef2-0_20260607061228578.log.1_0-44-80",
        ],
        expect_stats: Some(|s| {
            if s.total_corrupt_log_blocks >= 1 {
                Ok(())
            } else {
                Err(format!(
                    "expected total_corrupt_log_blocks >= 1, got {}",
                    s.total_corrupt_log_blocks
                ))
            }
        }),
        expected: Expected::GoldParquet,
        ..Default::default()
    }
);

// (b) Parquet log blocks: base + 2 PARQUET_DATA_BLOCK logs + 1 DELETE block.
// Exercises the parquet-format log decode path plus delete handling; gold has
// 3 rows. Gold comparison sorts by "key" (harness default).
/// A primary-key predicate is pushed into the parquet log blocks themselves.
///
/// The fixture holds three keys; filtering to one has to return exactly that
/// row. This is the whole chain — the merge-on-read reader decides the predicate
/// is safe, hands it to the log file reader, which hands it to the block decoder
/// — so it fails if any link drops it, which is how the base-file pushdown was
/// lost before.
fn case_parquet_log_block_pk_filter() -> FgReaderCase {
    let mut case = FgReaderCase {
        name: "parquet_log_block_pk_filter",
        fixture: QuickstartTripsTable::MorLayoutParquetLogBlock,
        partition: "",
        base_file: "9c161c05-86e5-46cc-85ab-14ce5326cfb0-0_0-79-130_20260607061232259.parquet",
        log_files: &[
            ".9c161c05-86e5-46cc-85ab-14ce5326cfb0-0_20260607061234788.log.1_0-93-156",
            ".9c161c05-86e5-46cc-85ab-14ce5326cfb0-0_20260607061236403.log.1_0-107-185",
            ".9c161c05-86e5-46cc-85ab-14ce5326cfb0-0_20260607061238120.log.1_0-121-217",
        ],
        expect_output_columns: Some(&["key", "ts", "value", "num"]),
        ..Default::default()
    };
    case.row_filter = Some(RowFilterSpec {
        column: "_hoodie_record_key",
        predicate: FilterPredicate::Eq("k2"),
        mor_pk_safe: true,
    });
    case.expected = Expected::Custom {
        rows: 1,
        validate: |batch| {
            let keys = batch
                .column_by_name("key")
                .ok_or_else(|| "no `key` column".to_string())?;
            let keys = keys
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| "`key` should be a string column".to_string())?;
            if keys.value(0) != "k2" {
                return Err(format!("expected only k2, got {}", keys.value(0)));
            }
            Ok(())
        },
    };
    case
}

fg_case_test!(
    harness_parquet_log_block_pk_filter,
    case_parquet_log_block_pk_filter()
);

fg_case_test!(
    harness_parquet_log_blocks_merge,
    FgReaderCase {
        name: "parquet_log_blocks_merge",
        fixture: QuickstartTripsTable::MorLayoutParquetLogBlock,
        partition: "",
        base_file: "9c161c05-86e5-46cc-85ab-14ce5326cfb0-0_0-79-130_20260607061232259.parquet",
        log_files: &[
            ".9c161c05-86e5-46cc-85ab-14ce5326cfb0-0_20260607061234788.log.1_0-93-156",
            ".9c161c05-86e5-46cc-85ab-14ce5326cfb0-0_20260607061236403.log.1_0-107-185",
            ".9c161c05-86e5-46cc-85ab-14ce5326cfb0-0_20260607061238120.log.1_0-121-217",
        ],
        expected: Expected::GoldParquet,
        ..Default::default()
    }
);

// (c) Partial-update block: base + 1 AVRO log carrying an IS_PARTIAL=true data
// block. Originally this was a gap (D-P2-2) and hudi-rs refused such blocks
// loudly to avoid silently null-filling absent columns. That gap is now closed:
// hudi-rs applies IS_PARTIAL / KEEP_VALUES blocks by overlaying the updated
// column subset onto the prior record. So the case now asserts the read
// SUCCEEDS and matches the gold Spark `SELECT *` snapshot (the merge-correct
// truth, 4 rows) — any divergence from gold surfaces an incorrect partial merge.
fg_case_test!(
    harness_partial_update_block_applied,
    FgReaderCase {
        name: "partial_update_block_applied",
        fixture: QuickstartTripsTable::MorLayoutPartialUpdate,
        partition: "",
        base_file: "f9093583-6d9d-411f-929d-9f17fb46f622-0_0-156-267_20260607061241151.parquet",
        log_files: &[".f9093583-6d9d-411f-929d-9f17fb46f622-0_20260607061243026.log.1_0-177-312"],
        expected: Expected::GoldParquet,
        ..Default::default()
    }
);

// (d) HFile log block: base + 1 HFILE_DATA_BLOCK log. The HFile log reader was
// removed from hudi-rs, so the read fails loudly while parsing the log block
// header: the HFILE_DATA_BLOCK type ordinal (4) is no longer a recognized block
// type, so block-header parsing rejects it with
// `LogFormatError("Invalid block type: 4")` — a loud, unambiguous refusal (the
// data is never silently dropped). The assertion matches that stable text.
// NOTE: the matched string "Invalid block type: 4" comes from
// `BlockType::TryFrom` (log_block.rs) formatting the raw ordinal.
// If HFILE_DATA_BLOCK is ever added back as ordinal 4, this case will
// flip to an unexpected-success failure — that's the intended signal.
fg_case_test!(
    harness_hfile_log_block_rejected,
    FgReaderCase {
        name: "hfile_log_block_rejected",
        fixture: QuickstartTripsTable::MorLayoutHfileLogBlock,
        partition: "",
        base_file: "c62149b1-ec4b-4f3a-9302-149ffcfe3cde-0_0-212-362_20260607061245908.parquet",
        log_files: &[".c62149b1-ec4b-4f3a-9302-149ffcfe3cde-0_20260607061248155.log.1_0-226-388"],
        expected: Expected::ErrContains("Invalid block type: 4"),
        ..Default::default()
    },
    ignore = "asserts an HFile log block is rejected, which held upstream; this crate has HFile support and the read completes, so the expectation needs re-deriving rather than inverting"
);

// =============================================================================
// DELETE-block orderingVal wrapper-type e2e fixtures (Task 7)
//
// Six real MOR v9 COMMIT_TIME_ORDERING tables generated by gold Hudi Spark.
// Each table: 4 rows inserted (ids 1-4), ids 3 and 4 deleted via upsert with
// `_hoodie_is_deleted=true`. After applying the DELETE log block: ids 1 and 2
// remain. The DELETE block's orderingVal is wrapped in the Avro type named by
// the fixture; these cases exercise arrow-avro decoding of each wrapper type
// end-to-end through the HoodieFileGroupReader MOR snapshot path.
//
// Schema: id INT32, val STRING, ts <type> (non-partitioned).
// Precombine field: ts (set via table_config, matching hoodie.properties).
//
// Provenance: /home/ubuntu/ws2/hudi-rs-delete-fixtures/{ord_int,...}
// Generated: 2026-06-08, gold Hudi Spark, `hoodie.table.format=native`.
// =============================================================================

// DELETE block orderingVal: IntWrapper (Avro int, Java Integer).
// ts is INT32; delete block carries [4, 3] for ids 4 and 3.
// After merge: ids 1 and 2 survive.
fg_case_test!(
    harness_delete_ord_int,
    FgReaderCase {
        name: "delete_ord_int",
        fixture: QuickstartTripsTable::MorDeleteOrdInt,
        partition: "",
        base_file: "119a1f43-95a8-4689-b497-e817a5ce71e2-0_0-938-2416_20260608021824592.parquet",
        log_files: &[".119a1f43-95a8-4689-b497-e817a5ce71e2-0_20260608021824926.log.1_0-952-2447",],
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "val"],
            rows: &[&["1", "val_1"], &["2", "val_2"]],
        },
        ..Default::default()
    }
);

// DELETE block orderingVal: LongWrapper (Avro long, Java Long).
// ts is INT64; delete block carries [4000, 3000] for ids 4 and 3.
// After merge: ids 1 and 2 survive.
fg_case_test!(
    harness_delete_ord_long,
    FgReaderCase {
        name: "delete_ord_long",
        fixture: QuickstartTripsTable::MorDeleteOrdLong,
        partition: "",
        base_file: "db24c83c-6f2b-4b38-9901-ad3580fb38da-0_0-958-2462_20260608021825735.parquet",
        log_files: &[".db24c83c-6f2b-4b38-9901-ad3580fb38da-0_20260608021826071.log.1_0-972-2493",],
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "val"],
            rows: &[&["1", "val_1"], &["2", "val_2"]],
        },
        ..Default::default()
    }
);

// DELETE block orderingVal: DoubleWrapper (Avro double, Java Double).
// ts is FLOAT64; delete block carries [6.0, 4.5] for ids 4 and 3.
// After merge: ids 1 and 2 survive.
fg_case_test!(
    harness_delete_ord_double,
    FgReaderCase {
        name: "delete_ord_double",
        fixture: QuickstartTripsTable::MorDeleteOrdDouble,
        partition: "",
        base_file: "bec1126b-d037-473d-a921-5e5bc99acf02-0_0-978-2508_20260608021826833.parquet",
        log_files: &[".bec1126b-d037-473d-a921-5e5bc99acf02-0_20260608021827174.log.1_0-992-2539",],
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "val"],
            rows: &[&["1", "val_1"], &["2", "val_2"]],
        },
        ..Default::default()
    }
);

// DELETE block orderingVal: StringWrapper (Avro string, Java String).
// ts is UTF8; delete block carries ["ord_4", "ord_3"] for ids 4 and 3.
// After merge: ids 1 and 2 survive.
fg_case_test!(
    harness_delete_ord_string,
    FgReaderCase {
        name: "delete_ord_string",
        fixture: QuickstartTripsTable::MorDeleteOrdString,
        partition: "",
        base_file: "f77d5d0d-0604-4932-bd04-4b84ca627c9e-0_0-998-2554_20260608021827928.parquet",
        log_files: &[".f77d5d0d-0604-4932-bd04-4b84ca627c9e-0_20260608021828257.log.1_0-1012-2585",],
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "val"],
            rows: &[&["1", "val_1"], &["2", "val_2"]],
        },
        ..Default::default()
    }
);

// DELETE block orderingVal: DecimalWrapper (Avro bytes + decimal logical type,
// precision=30 scale=15, Java BigDecimal). ts is DECIMAL(20,4); delete block
// carries [4.000000000000000, 3.000000000000000] for ids 4 and 3.
// After merge: ids 1 and 2 survive.
//
// Exercises decimal ordering parity with the Java reader: the base column
// (DECIMAL(20,4)) and the delete wrapper (scale 15) carry DIFFERENT scales, so
// this pins the value-based, scale-independent decimal compare (`cmp_decimal`)
// — `4.0000` == `4.000000000000000`. `OrderingValue::Decimal` decodes on both the
// read path and the DELETE path (`scalar_ordering_value`).
fg_case_test!(
    harness_delete_ord_decimal,
    FgReaderCase {
        name: "delete_ord_decimal",
        fixture: QuickstartTripsTable::MorDeleteOrdDecimal,
        partition: "",
        base_file: "26fd26f9-efeb-4cf7-93e7-7f1ecb826550-0_0-1018-2600_20260608021829002.parquet",
        log_files: &[".26fd26f9-efeb-4cf7-93e7-7f1ecb826550-0_20260608021829336.log.1_0-1032-2631",],
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "val"],
            rows: &[&["1", "val_1"], &["2", "val_2"]],
        },
        ..Default::default()
    }
);

// DELETE block orderingVal: TimestampMicrosWrapper (Avro long / epoch micros).
// ts is TIMESTAMP[us, UTC]; delete block carries Long (epoch micros) values —
// ENG-42987-adjacent: unwrapAvroValueWrapper returns Long, not Instant, but the
// block IS written and the decoder must handle TimestampMicrosWrapper.value as a
// plain long. After merge: ids 1 and 2 survive.
fg_case_test!(
    harness_delete_ord_timestamp,
    FgReaderCase {
        name: "delete_ord_timestamp",
        fixture: QuickstartTripsTable::MorDeleteOrdTimestamp,
        partition: "",
        base_file: "2c3b1ef4-a740-4685-9825-b802c1755ea7-0_0-1056-2690_20260608021831146.parquet",
        log_files: &[".2c3b1ef4-a740-4685-9825-b802c1755ea7-0_20260608021831509.log.1_0-1070-2721",],
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "val"],
            rows: &[&["1", "val_1"], &["2", "val_2"]],
        },
        ..Default::default()
    }
);

// =============================================================================
// Filter-pushdown extensions (0610 review): IN predicates and logical/typed
// columns (GAP-HARNESS-T3B closed). Scope note: the supported matrix is
// parquet BASE + avro LOG only, and gold never pushes filters into avro log
// blocks, so pushdown coverage targets the base parquet read (CoW path and
// the PK-safe MOR cases above).
//
// MorLayoutAllDataTypes BASE parquet ground truth (pre-merge; logs excluded):
//   k1..k5, severity 1..5, boolean F/T/F/T/F,
//   date_nullable_field raw days 19723/19754/19783/19814/19844,
//   timestamp_micros_nullable_field (us, UTC)
//     1704067201000000 / 1706745602000000 / 1709251203000000 /
//     1711929604000000 / 1714521605000000,
//   decimal_field (20,2) 100.10/200.20/300.30/400.40/500.50,
//   float_field (f32) 1.2/2.3/3.4/4.5/5.6.
// =============================================================================

// PK IN on the CoW path (base-only): V9MorNonpart3Commits base has ids
// 0-6; IN ("1","4") keeps exactly those two original rows. Unfiltered would
// be 7 rows.
fg_case_test!(
    harness_filter_pk_in_cow,
    FgReaderCase {
        name: "filter_pk_in_cow",
        fixture: QuickstartTripsTable::V9MorNonpart3Commits,
        partition: "",
        base_file: NONPART_BASE_FILE,
        log_files: &[],
        expect_output_columns: Some(&["id", "name", "price"]),
        row_filter: Some(RowFilterSpec {
            column: "_hoodie_record_key",
            predicate: FilterPredicate::In(&["1", "4"]),
            mor_pk_safe: false,
        }),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "price"],
            rows: &[&["1", "A", "10.0"], &["4", "D", "40.0"]],
        },
        ..Default::default()
    }
);

// Logical/typed-column filters on the CoW path (MorLayoutAllDataTypes
// base-only). The predicate column does NOT need to be projected — the filter
// reads it through its own ProjectionMask — so the output stays on simple
// columns while the comparison exercises the typed arm.

// Boolean Eq: true rows are k2, k4.
fg_case_test!(
    harness_filter_boolean_cow,
    FgReaderCase {
        name: "filter_boolean_cow",
        fixture: QuickstartTripsTable::MorLayoutAllDataTypes,
        partition: "",
        base_file: "c887c1e8-5fb9-475e-8171-769c5cf10c61-0_0-240-395_20260409030537482.parquet",
        log_files: &[],
        expect_output_columns: Some(&["key", "severity"]),
        row_filter: Some(RowFilterSpec {
            column: "boolean_field",
            predicate: FilterPredicate::Eq("true"),
            mor_pk_safe: false,
        }),
        expected: Expected::Rows {
            sort_key: "key",
            columns: &["key", "severity"],
            rows: &[&["k2", "2"], &["k4", "4"]],
        },
        ..Default::default()
    }
);

// Date32 Gt: days > 19754 (2024-02-01) are k3, k4, k5.
fg_case_test!(
    harness_filter_date_cow,
    FgReaderCase {
        name: "filter_date_cow",
        fixture: QuickstartTripsTable::MorLayoutAllDataTypes,
        partition: "",
        base_file: "c887c1e8-5fb9-475e-8171-769c5cf10c61-0_0-240-395_20260409030537482.parquet",
        log_files: &[],
        expect_output_columns: Some(&["key", "severity"]),
        row_filter: Some(RowFilterSpec {
            column: "date_nullable_field",
            predicate: FilterPredicate::Gt("19754"),
            mor_pk_safe: false,
        }),
        expected: Expected::Rows {
            sort_key: "key",
            columns: &["key", "severity"],
            rows: &[&["k3", "3"], &["k4", "4"], &["k5", "5"]],
        },
        ..Default::default()
    }
);

// Timestamp(us, UTC) Lt: strictly before k3's 2024-03-01T00:00:03Z are k1, k2.
fg_case_test!(
    harness_filter_timestamp_cow,
    FgReaderCase {
        name: "filter_timestamp_cow",
        fixture: QuickstartTripsTable::MorLayoutAllDataTypes,
        partition: "",
        base_file: "c887c1e8-5fb9-475e-8171-769c5cf10c61-0_0-240-395_20260409030537482.parquet",
        log_files: &[],
        expect_output_columns: Some(&["key", "severity"]),
        row_filter: Some(RowFilterSpec {
            column: "timestamp_micros_nullable_field",
            predicate: FilterPredicate::Lt("1709251203000000"),
            mor_pk_safe: false,
        }),
        expected: Expected::Rows {
            sort_key: "key",
            columns: &["key", "severity"],
            rows: &[&["k1", "1"], &["k2", "2"]],
        },
        ..Default::default()
    }
);

// Decimal128(20,2) Gt: values > 300.30 are k4 (400.40), k5 (500.50).
fg_case_test!(
    harness_filter_decimal_cow,
    FgReaderCase {
        name: "filter_decimal_cow",
        fixture: QuickstartTripsTable::MorLayoutAllDataTypes,
        partition: "",
        base_file: "c887c1e8-5fb9-475e-8171-769c5cf10c61-0_0-240-395_20260409030537482.parquet",
        log_files: &[],
        expect_output_columns: Some(&["key", "severity"]),
        row_filter: Some(RowFilterSpec {
            column: "decimal_field",
            predicate: FilterPredicate::Gt("300.30"),
            mor_pk_safe: false,
        }),
        expected: Expected::Rows {
            sort_key: "key",
            columns: &["key", "severity"],
            rows: &[&["k4", "4"], &["k5", "5"]],
        },
        ..Default::default()
    }
);

// Float32 Gt: 3.4f32 equals k3's stored value exactly (same literal), so the
// strictly-greater rows are k4 (4.5), k5 (5.6).
fg_case_test!(
    harness_filter_float32_cow,
    FgReaderCase {
        name: "filter_float32_cow",
        fixture: QuickstartTripsTable::MorLayoutAllDataTypes,
        partition: "",
        base_file: "c887c1e8-5fb9-475e-8171-769c5cf10c61-0_0-240-395_20260409030537482.parquet",
        log_files: &[],
        expect_output_columns: Some(&["key", "severity"]),
        row_filter: Some(RowFilterSpec {
            column: "float_field",
            predicate: FilterPredicate::Gt("3.4"),
            mor_pk_safe: false,
        }),
        expected: Expected::Rows {
            sort_key: "key",
            columns: &["key", "severity"],
            rows: &[&["k4", "4"], &["k5", "5"]],
        },
        ..Default::default()
    }
);

// =============================================================================
// Log-only file-group read-config coverage (0610 review): projection,
// instant-range, and watermark on a file group with NO base file.
//
// MorLayoutLogOnly ground truth (decoded from the log blocks):
//   c1 = 20260409030525348  AVRO insert: k1 INFO/1, k2 WARN/2, k3 ERROR/3
//                           (all ts=100, round=1)
//   c2 = 20260409030527298  AVRO update: k1 -> UPD, round=2
//   c3 = 20260409030528554  DELETE: k3
// =============================================================================

const LOG_ONLY_C1: &str = "20260409030525348";
const LOG_ONLY_C2: &str = "20260409030527298";
const LOG_ONLY_LOG_FILES: &[&str] = &[
    ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030525348.log.1_0-102-176",
    ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030527298.log.1_0-116-202",
    ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030528554.log.1_0-130-231",
];

/// Full table arrow schema of MorLayoutLogOnly, hand-built exactly as the FFI
/// path receives it (there is no base parquet footer to derive it from).
fn log_only_table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_hoodie_commit_time", DataType::Utf8, true),
        Field::new("_hoodie_commit_seqno", DataType::Utf8, true),
        Field::new("_hoodie_record_key", DataType::Utf8, true),
        Field::new("_hoodie_partition_path", DataType::Utf8, true),
        Field::new("_hoodie_file_name", DataType::Utf8, true),
        Field::new("key", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("severity", DataType::Int32, true),
        Field::new("partition", DataType::Utf8, true),
        Field::new("round", DataType::Int32, true),
    ]))
}

// Projection on a log-only file group. No base footer exists, so the
// data schema must come in explicitly (FFI-style); the requested schema
// prunes to [key, level] and merge-internal fields must be stripped from the
// output. Validates the avro log decode honors the required schema without a
// base file.
fg_case_test!(
    harness_log_only_projection,
    FgReaderCase {
        name: "log_only_projection",
        fixture: QuickstartTripsTable::MorLayoutLogOnly,
        partition: "",
        base_file: "",
        log_files: LOG_ONLY_LOG_FILES,
        schema: SchemaSpec::Explicit {
            data: log_only_table_schema(),
            requested: Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("level", DataType::Utf8, true),
            ])),
        },
        expect_output_columns: Some(&["key", "level"]),
        expected: Expected::Rows {
            sort_key: "key",
            columns: &["key", "level"],
            rows: &[&["k1", "UPD"], &["k2", "WARN"]],
        },
        ..Default::default()
    }
);

// Instant-range up_to(c2) on a log-only file group: the c3 DELETE block
// is outside the range and skipped, so k3 survives with its insert values and
// k1 keeps the c2 update.
fn log_only_instant_range_up_to_c2() -> InstantRange {
    InstantRange::up_to(LOG_ONLY_C2, "UTC")
}

fg_case_test!(
    harness_log_only_instant_range_up_to_c2,
    FgReaderCase {
        name: "log_only_instant_range_up_to_c2",
        fixture: QuickstartTripsTable::MorLayoutLogOnly,
        partition: "",
        base_file: "",
        log_files: LOG_ONLY_LOG_FILES,
        instant_range: Some(log_only_instant_range_up_to_c2),
        expected: Expected::Rows {
            sort_key: "key",
            columns: &["key", "level", "severity", "round"],
            rows: &[
                &["k1", "UPD", "1", "2"],
                &["k2", "WARN", "2", "1"],
                &["k3", "ERROR", "3", "1"],
            ],
        },
        ..Default::default()
    }
);

// Watermark = c1 on a log-only file group: the c2 update and c3 delete
// are FUTURE blocks (INSTANT_TIME > latest_commit_time) and excluded — the
// read returns exactly the c1 inserts.
fg_case_test!(
    harness_log_only_watermark_excludes_updates,
    FgReaderCase {
        name: "log_only_watermark_excludes_updates",
        fixture: QuickstartTripsTable::MorLayoutLogOnly,
        partition: "",
        base_file: "",
        log_files: LOG_ONLY_LOG_FILES,
        latest_commit_time: Some(LOG_ONLY_C1),
        expected: Expected::Rows {
            sort_key: "key",
            columns: &["key", "level", "severity", "round"],
            rows: &[
                &["k1", "INFO", "1", "1"],
                &["k2", "WARN", "2", "1"],
                &["k3", "ERROR", "3", "1"],
            ],
        },
        ..Default::default()
    }
);

// =============================================================================
// Schema-on-write evolution e2e (0610 review).
//
// Fixtures from hudi-internal `TestMORFileSliceLayoutsSchemaEvo` (self-
// validating generator): one file group whose two AVRO log blocks were written
// under DIFFERENT writer schemas. Read through `SchemaSpec::ExplicitJson` —
// the full FFI mirror — so `required_schema_json` is armed and the log-block
// decoder takes the avro RESOLUTION branch (older writer schema resolved
// against the required schema), and the base parquet goes through
// intersection-read + projection (null-fill / promotion).
// =============================================================================

/// table_evo_add_col table avro schema (printed by the generator; what Gluten
/// would pass over FFI as data_schema_json after the ALTER).
const EVO_ADD_COL_AVRO_JSON: &str = r#"{"type":"record","name":"h1_record","namespace":"hoodie.h1","fields":[{"name":"_hoodie_commit_time","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_commit_seqno","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_record_key","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_partition_path","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_file_name","type":["null","string"],"doc":"","default":null},{"name":"key","type":["null","string"],"default":null},{"name":"ts","type":["null","long"],"default":null},{"name":"val","type":["null","string"],"default":null},{"name":"extra","type":["null","string"],"default":null}]}"#;

/// Projected requested schema for the add-col fixture: key, val, extra only.
const EVO_ADD_COL_PROJ_AVRO_JSON: &str = r#"{"type":"record","name":"h1_record","namespace":"hoodie.h1","fields":[{"name":"key","type":["null","string"],"default":null},{"name":"val","type":["null","string"],"default":null},{"name":"extra","type":["null","string"],"default":null}]}"#;

/// table_evo_promotion table avro schema (post-promotion: num long, fnum double).
const EVO_PROMOTION_AVRO_JSON: &str = r#"{"type":"record","name":"h2_record","namespace":"hoodie.h2","fields":[{"name":"_hoodie_commit_time","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_commit_seqno","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_record_key","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_partition_path","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_file_name","type":["null","string"],"doc":"","default":null},{"name":"key","type":["null","string"],"default":null},{"name":"ts","type":["null","long"],"default":null},{"name":"num","type":["null","long"],"default":null},{"name":"fnum","type":["null","double"],"default":null}]}"#;

const EVO_ADD_COL_BASE: &str =
    "163429de-9ca5-47a4-858c-6f3884aec758-0_0-16-25_20260611035339496.parquet";
const EVO_ADD_COL_LOGS: &[&str] = &[
    ".163429de-9ca5-47a4-858c-6f3884aec758-0_20260611035345811.log.1_0-30-51",
    ".163429de-9ca5-47a4-858c-6f3884aec758-0_20260611035347744.log.1_0-50-89",
];
const EVO_PROMOTION_BASE: &str =
    "25e4b267-703d-40b3-b80f-98464549c6ab-0_0-85-139_20260611035350236.parquet";
const EVO_PROMOTION_LOGS: &[&str] = &[
    ".25e4b267-703d-40b3-b80f-98464549c6ab-0_20260611035351880.log.1_0-99-165",
    ".25e4b267-703d-40b3-b80f-98464549c6ab-0_20260611035352695.log.1_0-120-209",
];

// Added column: base + log1 predate `extra`; log2 carries it. The merged read
// must null-fill `extra` for k1 (updated via the OLD-schema log block), k3, k4
// and surface x2 for k2. Validated against the Spark gold snapshot.
fg_case_test!(
    harness_evo_add_col,
    FgReaderCase {
        name: "evo_add_col",
        fixture: QuickstartTripsTable::MorEvoAddCol,
        partition: "",
        base_file: EVO_ADD_COL_BASE,
        log_files: EVO_ADD_COL_LOGS,
        schema: SchemaSpec::ExplicitJson {
            data_json: EVO_ADD_COL_AVRO_JSON,
            requested_json: EVO_ADD_COL_AVRO_JSON,
        },
        expected: Expected::GoldParquet,
        ..Default::default()
    }
);

// Added column + projection: requested = [key, val, extra]. Proves the
// required-schema JSON path prunes correctly while still resolving the
// old-writer-schema log block (k1's update has no `extra` → NULL).
fn validate_evo_add_col_projection(
    batch: &arrow_array::RecordBatch,
) -> std::result::Result<(), String> {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    // Row count is enforced by the harness (Expected::Custom { rows: 4, .. }).
    let keys = batch
        .column_by_name("key")
        .ok_or("no key col")?
        .as_string::<i32>();
    let vals = batch
        .column_by_name("val")
        .ok_or("no val col")?
        .as_string::<i32>();
    let extras = batch
        .column_by_name("extra")
        .ok_or("no extra col")?
        .as_string::<i32>();
    let mut rows: Vec<(String, String, Option<String>)> = (0..batch.num_rows())
        .map(|i| {
            (
                keys.value(i).to_string(),
                vals.value(i).to_string(),
                (!extras.is_null(i)).then(|| extras.value(i).to_string()),
            )
        })
        .collect();
    rows.sort();
    let expected = vec![
        ("k1".to_string(), "v1_upd".to_string(), None),
        (
            "k2".to_string(),
            "v2_upd".to_string(),
            Some("x2".to_string()),
        ),
        ("k3".to_string(), "v3".to_string(), None),
        ("k4".to_string(), "v4".to_string(), None),
    ];
    if rows != expected {
        return Err(format!(
            "rows mismatch:\n actual={rows:?}\n expected={expected:?}"
        ));
    }
    Ok(())
}

fg_case_test!(
    harness_evo_add_col_projection,
    FgReaderCase {
        name: "evo_add_col_projection",
        fixture: QuickstartTripsTable::MorEvoAddCol,
        partition: "",
        base_file: EVO_ADD_COL_BASE,
        log_files: EVO_ADD_COL_LOGS,
        schema: SchemaSpec::ExplicitJson {
            data_json: EVO_ADD_COL_AVRO_JSON,
            requested_json: EVO_ADD_COL_PROJ_AVRO_JSON,
        },
        expect_output_columns: Some(&["key", "val", "extra"]),
        expected: Expected::Custom {
            rows: 4,
            validate: validate_evo_add_col_projection,
        },
        ..Default::default()
    }
);

// Type promotion: base + log1 wrote num INT / fnum FLOAT; log2 wrote num LONG
// (5000000000, beyond i32) / fnum DOUBLE after the schema-on-write promotion.
// The base parquet's INT/FLOAT columns and log1's INT/FLOAT avro block must
// both surface as LONG/DOUBLE under the promoted required schema. Validated
// against the gold snapshot (which contains the >i32 value).
fg_case_test!(
    harness_evo_promotion_int_to_long,
    FgReaderCase {
        name: "evo_promotion_int_to_long",
        fixture: QuickstartTripsTable::MorEvoPromotion,
        partition: "",
        base_file: EVO_PROMOTION_BASE,
        log_files: EVO_PROMOTION_LOGS,
        schema: SchemaSpec::ExplicitJson {
            data_json: EVO_PROMOTION_AVRO_JSON,
            requested_json: EVO_PROMOTION_AVRO_JSON,
        },
        expected: Expected::GoldParquet,
        ..Default::default()
    }
);

// =============================================================================
// Read-boundary hardening regression cases (0610 review).
// =============================================================================

// Empty latest_commit_time must read the FULL snapshot, not base-file-only.
// Poison: before the loader's empty-watermark guard, "" flowed into Gate 2 as
// the lexicographic upper bound, making `instant > ""` true for every log
// block — all log records silently dropped → the 7 original base rows (ids
// 0-6) with no error. With the guard, empty defaults to the far-future
// sentinel and the full 3-commit merge applies (ids 0-2 deleted, 4-6 updated).
fg_case_test!(
    harness_empty_watermark_reads_full_snapshot,
    FgReaderCase {
        name: "empty_watermark_reads_full_snapshot",
        fixture: QuickstartTripsTable::V9MorNonpart3Commits,
        partition: "",
        base_file: NONPART_BASE_FILE,
        log_files: NONPART_LOG_FILES,
        latest_commit_time: Some(""),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "price"],
            rows: &[
                &["3", "C", "30.0"],
                &["4", "D2", "45.0"],
                &["5", "E2", "55.0"],
                &["6", "F2", "65.0"],
            ],
        },
        ..Default::default()
    }
);

// A lowercase merge mode must be accepted (gold's getMergeMode is
// case-insensitive). Poison: before the loader upper-cased merge_mode, the
// schema handler accepted "commit_time_ordering" (eq_ignore_ascii_case) but the
// loader gate's exact match rejected it with "Unsupported merge mode", so a
// semantically-supported read failed. With normalization the full merge runs.
fg_case_test!(
    harness_merge_mode_lowercase_accepted,
    FgReaderCase {
        name: "merge_mode_lowercase_accepted",
        fixture: QuickstartTripsTable::V9MorNonpart3Commits,
        partition: "",
        base_file: NONPART_BASE_FILE,
        log_files: NONPART_LOG_FILES,
        merge_mode: Some("commit_time_ordering"),
        expected: Expected::Rows {
            sort_key: "id",
            columns: &["id", "name", "price"],
            rows: &[
                &["3", "C", "30.0"],
                &["4", "D2", "45.0"],
                &["5", "E2", "55.0"],
                &["6", "F2", "65.0"],
            ],
        },
        ..Default::default()
    }
);
