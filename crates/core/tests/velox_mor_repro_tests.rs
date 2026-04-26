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

//! Unit test that reproduces the Velox/Gluten MOR read issue (ENG-39590).
//!
//! This test mirrors **exactly** how Velox constructs and calls hudi-rs components
//! when processing a Hudi MOR split.  The parameters used here come directly from
//! the real test table created by `dev/test-hudi-mor.sh` in gluten-internal.
//!
//! # Velox call chain (reproduced below)
//!
//! In `WholeStageResultIterator.cc` Velox builds a `HiveHudiMORSplit` with:
//!   - `baseUri`       = table root path
//!   - `hudiOptions`   = [] (empty — hudi-rs loads hoodie.properties from baseUri)
//!   - `baseFileName`  = base parquet file name (relative)
//!   - `logFileNames`  = [delta log file name (relative, with leading dot)]
//!   - `partitionPath` = hive-style partition path (e.g. "city=sf")
//!
//! Then `HudiSplitReader::prepareSplit()` calls three hudi-rs FFI functions in order:
//!   1. `new_file_group_reader_with_options(baseUri, hudiOptions)`
//!      → `FileGroupReader::new_with_options(base_uri, [])`
//!   2. `new_file_slice_from_file_names(partitionPath, baseFileName, logFileNames)`
//!      → `FileGroup::new_with_base_file_name` + `add_log_files_from_names` + take first slice
//!   3. `file_group_reader.read_file_slice(file_slice)`
//!      → reads base parquet, scans log file, applies EVENT_TIME_ORDERING merge
//!
//! The `eprintln!("[VELOX-REPRO] ...")` lines in this test intentionally mirror the
//! `LOG(INFO) << "[HudiMOR] ..."` lines in `HudiSplitReader.cpp` so you can compare
//! both outputs side-by-side to verify equivalence.
//!
//! # How to run
//!
//! ```bash
//! source ~/.cargo/env
//! cd /home/ubuntu/ws1/hudi-rs
//! RUST_LOG=hudi_core=debug \
//!   cargo test -p hudi-core --test velox_mor_repro_tests \
//!     -- --nocapture 2>&1 | tee /tmp/hudi_rs_repro.log
//! ```
//!
//! Compare against the Velox-side logs from:
//! ```bash
//! cd /home/ubuntu/ws1/gluten-internal
//! RUST_LOG=hudi_core=debug \
//!   ./dev/test-hudi-mor.sh --read-only --gluten-only 2>&1 \
//!   | grep -E '\[HudiMOR\]' | tee /tmp/velox_repro.log
//! ```
//!
//! The `baseUri`, `partitionPath`, `baseFileName`, and `logFile[0]` values in both
//! logs must be identical for the repro to be valid.

use arrow_array::{Array, Int32Array, StringArray};
use hudi_core::error::Result;
use hudi_core::file_group::FileGroup;
use hudi_core::file_group::reader::FileGroupReader;

/// Initialize env_logger exactly once so RUST_LOG=hudi_core=debug emits to stderr.
/// Mirrors what the C++ bridge does via `init_logger()` in cpp/src/lib.rs.
fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

/// Base URI of the test table (created by `dev/test-hudi-mor.sh`).
///
/// Velox receives this as `HiveHudiMORSplit::baseUri`.
const BASE_URI: &str = "file:///home/ubuntu/ws1/test-data/hudi_mor_partitioned";

/// One partition's worth of MOR split metadata — mirrors one `HiveHudiMORSplit`.
struct PartitionCase {
    /// Velox field: `HiveHudiMORSplit::partitionPath`
    partition_path: &'static str,
    /// Velox field: `HiveHudiMORSplit::baseFileName`
    base_file_name: &'static str,
    /// Velox field: `HiveHudiMORSplit::logFileNames`  (with leading dot, as on disk)
    log_file_names: &'static [&'static str],
    /// Expected row count after MOR merge
    expected_rows: usize,
    /// (id, expected_name, expected_age) for the record that was updated by Commit 2.
    /// Used to verify the merge chose the newer value over the base-file value.
    updated_record: (i32, &'static str, i32),
    /// (id, expected_name, expected_age) for the record that was NOT updated.
    unchanged_record: (i32, &'static str, i32),
}

/// All four partitions of the test table.
///
/// File names taken directly from:
///   /home/ubuntu/ws1/test-data/hudi_mor_partitioned/city={sf,nyc,chi,la}/
///
/// Commit timeline:
///   20260327190925114 → INSERT 8 rows → base .parquet files per partition
///   20260327190932786 → UPSERT 4 rows → .log files (updates ids 1, 3, 5, 7)
const PARTITIONS: &[PartitionCase] = &[
    PartitionCase {
        partition_path: "city=sf",
        base_file_name: "a50cb7ba-a9eb-46bd-8613-5a1d4914ac4a-0_0-21-40_20260327190925114.parquet",
        log_file_names: &[".a50cb7ba-a9eb-46bd-8613-5a1d4914ac4a-0_20260327190932786.log.1_0-42-87"],
        expected_rows: 2,
        updated_record: (1, "Alice-V2", 31),
        unchanged_record: (2, "Bob", 25),
    },
    PartitionCase {
        partition_path: "city=nyc",
        base_file_name: "5910dc3f-242b-4530-af06-d99a0db44803-0_3-21-43_20260327190925114.parquet",
        log_file_names: &[".5910dc3f-242b-4530-af06-d99a0db44803-0_20260327190932786.log.1_3-42-90"],
        expected_rows: 2,
        updated_record: (3, "Carol-V2", 36),
        unchanged_record: (4, "Dave", 28),
    },
    PartitionCase {
        partition_path: "city=chi",
        base_file_name: "da10ee68-1cd5-4662-a5ad-be0b68c2ebb0-0_2-21-42_20260327190925114.parquet",
        log_file_names: &[".da10ee68-1cd5-4662-a5ad-be0b68c2ebb0-0_20260327190932786.log.1_2-42-89"],
        expected_rows: 2,
        updated_record: (5, "Eve-V2", 33),
        unchanged_record: (6, "Frank", 40),
    },
    PartitionCase {
        partition_path: "city=la",
        base_file_name: "30701f2b-85b7-4a28-a065-25ed3ed3e03e-0_1-21-41_20260327190925114.parquet",
        log_file_names: &[".30701f2b-85b7-4a28-a065-25ed3ed3e03e-0_20260327190932786.log.1_1-42-88"],
        expected_rows: 2,
        updated_record: (7, "Grace-V2", 28),
        unchanged_record: (8, "Hank", 45),
    },
];

/// Look up a column in a RecordBatch by name.  Panics with a descriptive message
/// if the column is not found.
fn col_by_name<'a>(
    batch: &'a arrow_array::RecordBatch,
    name: &str,
) -> &'a arrow_array::ArrayRef {
    let idx = batch
        .schema()
        .index_of(name)
        .unwrap_or_else(|_| panic!("column '{name}' not found in schema: {:?}", batch.schema()));
    batch.column(idx)
}

/// Print a human-readable summary of a RecordBatch to stderr (always visible with
/// `-- --nocapture`).  Lists schema and all rows so the caller can inspect results.
fn dump_batch(label: &str, batch: &arrow_array::RecordBatch) {
    eprintln!("[VELOX-REPRO]   {label}: {rows} rows, schema={schema:?}",
        rows = batch.num_rows(),
        schema = batch.schema());
    for row in 0..batch.num_rows() {
        let mut fields = Vec::new();
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let col = batch.column(i);
            // Format a small subset of common types for readability.
            let val = if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                format!("{:?}", a.value(row))
            } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                format!("{:?}", a.value(row))
            } else {
                format!("<{}>", col.data_type())
            };
            fields.push(format!("{}={}", field.name(), val));
        }
        eprintln!("[VELOX-REPRO]     row[{row}]: {}", fields.join(", "));
    }
}

/// Reproduces the exact Velox MOR split-read sequence for every partition of the
/// test table created by `dev/test-hudi-mor.sh`.
///
/// This test validates that hudi-rs, when called with the **same inputs** as Velox,
/// produces a correctly merged result batch.  If the test fails (wrong row count,
/// wrong values, or a Rust error/panic), that failure is the Rust-side reproduction
/// of the issue observed via the Gluten/Velox pipeline.
#[tokio::test]
async fn test_velox_mor_repro_partitioned_table() -> Result<()> {
    // Initialize logger so RUST_LOG=hudi_core=debug reaches stderr.
    // Same as cpp/src/lib.rs::init_logger() which is called by new_file_group_reader_with_options.
    init_logger();

    // =========================================================================
    // Step 1: Create FileGroupReader
    //
    // Mirrors Velox's call to new_file_group_reader_with_options() in
    // HudiSplitReader::prepareSplit() (HudiSplitReader.cpp:88-90).
    //
    // Note: hudiOptions is EMPTY.  The Scala side (HudiScanTransformer.scala:379-384)
    // always passes java.util.Collections.emptyList() for hudiOptions.
    // hudi-rs resolves all table config from hoodie.properties at baseUri.
    // =========================================================================
    // Simulates what HudiSplitReader::prepareSplit() will do after the fix:
    // serialize readerOutputType_ (Spark's ReadSchema columns) into hudiOptions.
    // For this test table the query selects id, name, age — matching the 3-col ReadSchema
    // used in the acceptance-criteria Spark run.
    let output_cols = "id,name,age";
    eprintln!(
        "[VELOX-REPRO] prepareSplit: creating FileGroupReader\
         \n[VELOX-REPRO]   baseUri={BASE_URI}\
         \n[VELOX-REPRO]   hudiOptions=1  hoodie.read.output.columns={output_cols}"
    );
    let reader = FileGroupReader::new_with_options(
        BASE_URI,
        [("hoodie.read.output.columns", output_cols)],
    )
    .await?;
    eprintln!("[VELOX-REPRO] prepareSplit: FileGroupReader created OK");

    // =========================================================================
    // Steps 2 + 3: per-partition FileSlice creation and read_file_slice
    //
    // For each partition Velox calls new_file_slice_from_file_names() then
    // read_file_slice() (HudiSplitReader.cpp:100-118).
    // =========================================================================
    for case in PARTITIONS {
        eprintln!(
            "\n[VELOX-REPRO] === partition: {} ===",
            case.partition_path
        );

        // -----------------------------------------------------------------------
        // Step 2: Build FileSlice
        //
        // Mirrors new_file_slice_from_file_names() in cpp/src/lib.rs:151-186.
        //   FileGroup::new_with_base_file_name(base_file_name, partition_path)
        //   file_group.add_log_files_from_names(log_file_names)
        //   let (_, file_slice) = file_group.file_slices.iter().next()
        // -----------------------------------------------------------------------
        eprintln!(
            "[VELOX-REPRO] prepareSplit: creating FileSlice\
             \n[VELOX-REPRO]   partitionPath={}\
             \n[VELOX-REPRO]   baseFileName={}\
             \n[VELOX-REPRO]   logFileNames={}",
            case.partition_path,
            case.base_file_name,
            case.log_file_names.len()
        );
        for (i, lf) in case.log_file_names.iter().enumerate() {
            eprintln!("[VELOX-REPRO]   logFile[{i}]={lf}");
        }

        let mut file_group =
            FileGroup::new_with_base_file_name(case.base_file_name, case.partition_path)?;
        file_group.add_log_files_from_names(case.log_file_names)?;

        let (commit_ts, file_slice) = file_group
            .file_slices
            .iter()
            .next()
            .expect("FileGroup must have exactly one FileSlice after adding the base file");
        eprintln!(
            "[VELOX-REPRO] prepareSplit: FileSlice created OK\
             \n[VELOX-REPRO]   commit_ts={commit_ts}\
             \n[VELOX-REPRO]   has_log_file={}",
            file_slice.has_log_file()
        );

        // -----------------------------------------------------------------------
        // Step 3: Read FileSlice (MOR merge)
        //
        // Mirrors HudiSplitReader.cpp:106-118:
        //   arrowStream_ = (*fileGroupReader_)->read_file_slice(**fileSlice_);
        // -----------------------------------------------------------------------
        eprintln!("[VELOX-REPRO] prepareSplit: calling read_file_slice (hudi-rs MOR merge)...");
        let batch = reader.read_file_slice(file_slice).await?;
        eprintln!(
            "[VELOX-REPRO] prepareSplit: read_file_slice returned OK\
             \n[VELOX-REPRO]   arrowStream rows={}  cols={}",
            batch.num_rows(),
            batch.num_columns()
        );

        dump_batch(case.partition_path, &batch);

        // -----------------------------------------------------------------------
        // Assertions
        // -----------------------------------------------------------------------

        // Column projection: must have exactly 7 cols (output + merge-required),
        // NOT all 10 from the parquet file.
        // Expected: _hoodie_commit_time, _hoodie_commit_seqno, _hoodie_record_key, id, name, age, ts
        // (_hoodie_commit_time is required by RecordMerger for delete-marker comparison)
        let expected_cols = [
            "_hoodie_commit_time",
            "_hoodie_record_key",
            "_hoodie_commit_seqno",
            "id",
            "name",
            "age",
            "ts",
        ];
        assert_eq!(
            batch.num_columns(),
            expected_cols.len(),
            "[{}] Expected {} projected cols, got {} — schema: {:?}",
            case.partition_path,
            expected_cols.len(),
            batch.num_columns(),
            batch.schema()
        );
        let schema = batch.schema();
        let actual_col_names: Vec<&str> = schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        for expected in &expected_cols {
            assert!(
                actual_col_names.contains(expected),
                "[{}] Column '{}' missing from projected schema: {:?}",
                case.partition_path,
                expected,
                actual_col_names,
            );
        }
        eprintln!(
            "[VELOX-REPRO]   column projection OK: {} cols {:?}",
            batch.num_columns(),
            actual_col_names,
        );

        // Row count: each partition has 2 rows after MOR merge
        assert_eq!(
            batch.num_rows(),
            case.expected_rows,
            "[{}] Expected {} rows after MOR merge, got {}",
            case.partition_path,
            case.expected_rows,
            batch.num_rows()
        );

        // Find the updated and unchanged records by scanning the `id` column.
        let id_col = col_by_name(&batch, "id");
        let id_arr = id_col
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("'id' column should be Int32Array");
        let name_col = col_by_name(&batch, "name");
        let name_arr = name_col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("'name' column should be StringArray");
        let age_col = col_by_name(&batch, "age");
        let age_arr = age_col
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("'age' column should be Int32Array");

        // Check updated record (id from Commit 2 upsert — should have V2 values)
        let (upd_id, upd_name, upd_age) = case.updated_record;
        let upd_row = (0..batch.num_rows())
            .find(|&r| id_arr.value(r) == upd_id)
            .unwrap_or_else(|| {
                panic!(
                    "[{}] Updated record id={upd_id} not found in merged batch",
                    case.partition_path
                )
            });
        assert_eq!(
            name_arr.value(upd_row),
            upd_name,
            "[{}] id={upd_id}: expected name='{}' after MOR merge (EVENT_TIME_ORDERING), got '{}'",
            case.partition_path,
            upd_name,
            name_arr.value(upd_row)
        );
        assert_eq!(
            age_arr.value(upd_row),
            upd_age,
            "[{}] id={upd_id}: expected age={upd_age} after MOR merge, got {}",
            case.partition_path,
            age_arr.value(upd_row)
        );

        // Check unchanged record (id NOT in Commit 2 — should keep base-file values)
        let (unc_id, unc_name, unc_age) = case.unchanged_record;
        let unc_row = (0..batch.num_rows())
            .find(|&r| id_arr.value(r) == unc_id)
            .unwrap_or_else(|| {
                panic!(
                    "[{}] Unchanged record id={unc_id} not found in merged batch",
                    case.partition_path
                )
            });
        assert_eq!(
            name_arr.value(unc_row),
            unc_name,
            "[{}] id={unc_id}: expected unchanged name='{unc_name}', got '{}'",
            case.partition_path,
            name_arr.value(unc_row)
        );
        assert_eq!(
            age_arr.value(unc_row),
            unc_age,
            "[{}] id={unc_id}: expected unchanged age={unc_age}, got {}",
            case.partition_path,
            age_arr.value(unc_row)
        );

        eprintln!("[VELOX-REPRO] partition {} PASSED", case.partition_path);
    }

    eprintln!("\n[VELOX-REPRO] ALL PARTITIONS PASSED — MOR merge is correct");
    Ok(())
}
