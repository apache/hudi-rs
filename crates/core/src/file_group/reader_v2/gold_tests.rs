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

//! Reads each merge-on-read layout fixture and checks the result against the
//! Spark snapshot shipped inside it.
//!
//! The fixtures carry their own `gold_data/` — a `SELECT *` of the table taken
//! from Spark — so what is asserted here is agreement with Hudi's reference
//! reader, not with this reader's own prior output.
//!
//! The file slice is discovered from the extracted fixture rather than written
//! down per case: each of these tables is a single file group, so the base file
//! and log files are whatever is on disk. A fixture that gains a file therefore
//! cannot silently stop being covered.

#![cfg(test)]

use crate::config::HudiConfigs;
use crate::config::read::HudiReadConfig;
use crate::file_group::reader_v2::MAX_INSTANT_TIME;
use crate::file_group::reader_v2::engine::HoodieFileGroupReader;
use crate::file_group::reader_v2::input_split::InputSplit;
use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
use crate::file_group::reader_v2::resolver::resolve_reader_context;
use crate::storage::Storage;
use arrow_array::RecordBatch;
use std::path::Path;
use std::sync::Arc;

/// The base file and log files of a fixture's single file group, relative to
/// the table root, plus the partition they live in.
struct Slice {
    base: Option<String>,
    logs: Vec<String>,
    partition: String,
}

/// Walk an extracted fixture and pick out its one file group.
///
/// Skips `.hoodie` (table metadata) and `gold_data` (the Spark snapshot this
/// compares against, which is not table data).
fn discover_slice(table_root: &Path) -> Slice {
    fn walk(dir: &Path, root: &Path, out: &mut Vec<(String, String)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if path.is_dir() {
                if name == ".hoodie" || name == "gold_data" {
                    continue;
                }
                walk(&path, root, out);
            } else {
                let rel = path
                    .strip_prefix(root)
                    .expect("walked path is under the table root")
                    .to_string_lossy()
                    .to_string();
                let partition = Path::new(&rel)
                    .parent()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_default();
                out.push((rel, partition));
            }
        }
    }

    let mut files = Vec::new();
    walk(table_root, table_root, &mut files);

    let mut base = None;
    let mut logs = Vec::new();
    let mut partition = String::new();
    for (rel, part) in files {
        let name = Path::new(&rel)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        // Hadoop writes a `..<file>.crc` checksum sidecar next to each data
        // file. It is not table data and must not reach the readers.
        if name.ends_with(".crc") {
            continue;
        }
        if name.ends_with(".parquet") && !name.starts_with('.') {
            base = Some(rel);
            partition = part;
        } else if name.contains(".log.") {
            logs.push(rel);
            partition = part;
        }
    }
    // Log files are appended in order; the scan relies on that ordering.
    logs.sort();

    Slice {
        base,
        logs,
        partition,
    }
}

/// Read a fixture's file group and return the merged rows.
async fn read_fixture(table_path: &str) -> crate::Result<RecordBatch> {
    let slice = discover_slice(Path::new(table_path));

    // Load the table's own properties rather than inventing them: the merge
    // semantics live in `hoodie.properties`, and inventing configs would test
    // the reader against a table that does not exist.
    let mut resolver = crate::table::builder::OptionResolver::new_with_options(
        table_path,
        [(
            HudiReadConfig::EndTimestamp.as_ref(),
            MAX_INSTANT_TIME.to_string(),
        )],
    );
    resolver.resolve_options().await?;
    let configs = HudiConfigs::new(resolver.hudi_options.clone());
    let storage = Storage::new(
        Arc::new(resolver.storage_options),
        Arc::new(configs.clone()),
    )?;

    let has_logs = !slice.logs.is_empty();
    let mut context = resolve_reader_context(&configs, has_logs)?;
    // The fixtures are read whole, so nothing bounds the log scan.
    context.instant_range = None;
    context.rebuild_record_context(slice.partition.clone());

    let input_split = InputSplit::new(slice.base, None, slice.logs, slice.partition);

    let mut reader = HoodieFileGroupReader::new(
        Arc::new(context),
        storage,
        input_split,
        ReaderParameters::default(),
        None,
        None,
    )?;
    reader.read().await
}

/// Fixtures the reader reproduces today.
const GOLD_FIXTURES: &[&str] = &[
    "table_log_only",
    "table_log_compaction",
    "table_parquet_log_block",
    "table_partial_update",
    "table_evo_add_col",
    "table_evo_promotion",
];

/// Fixtures the reader does not read yet, with what stops each one.
///
/// Two causes remain, neither in the merge:
///
/// **Avro maps are modelled as a malformed Arrow type.** `avro_to_arrow` turns
/// an Avro map into `Dictionary(Utf8, V)`, but an Arrow dictionary key must be
/// an integer, so the array builder rejects it. Avro maps belong in Arrow's
/// `Map` type. Changing that alters the schema of every table with a map
/// column, including on the existing read path.
///
/// **The delete record's ordering value has two shapes in the wild.** Hudi
/// moved it into per-type wrapper records — `IntWrapper`, `DecimalWrapper` and
/// so on — because Avro forbids a union holding two branches of the same
/// underlying type. This crate carries only the older primitive union, so a log
/// file written the newer way resolves each branch index to the wrong type.
///
/// The two shapes cannot be told apart by decoding: a wrapper is a record whose
/// single field is the primitive, so `IntWrapper` and a bare `long` encode to
/// the same bytes. Only the branch index differs. Reading both therefore needs
/// to key off the writer version rather than the payload, which is a design
/// question rather than a decoding one — and swapping to the newer schema
/// outright breaks the fixtures written the older way.
///
/// **A corrupt tail block is not recognised.** `LogFileReader` has no
/// corrupt-block detection — `create_corrupted_block_if_needed` returns `None`
/// — so it parses the trailing garbage as a block.
const KNOWN_GAPS: &[(&str, &str)] = &[
    (
        "table_column_projection",
        "avro map modelled as Dictionary(Utf8, _)",
    ),
    (
        "table_all_data_types",
        "avro map modelled as Dictionary(Utf8, _)",
    ),
    (
        "table_null_containers",
        "avro map modelled as Dictionary(Utf8, _)",
    ),
    ("table_corrupt_tail_block", "no corrupt-block detection"),
    (
        "table_delete_ord_int",
        "delete ordering value written wrapped",
    ),
    (
        "table_delete_ord_long",
        "delete ordering value written wrapped",
    ),
    (
        "table_delete_ord_double",
        "delete ordering value written wrapped",
    ),
    (
        "table_delete_ord_decimal",
        "delete ordering value written wrapped",
    ),
    (
        "table_delete_ord_string",
        "delete ordering value written wrapped",
    ),
    (
        "table_delete_ord_timestamp",
        "delete ordering value written wrapped",
    ),
];

/// Ships no snapshot and has no settled expectation.
///
/// `table_hfile_log_block` was dumped against a reader with no HFile support,
/// where the expectation was a loud failure; this crate does read HFile, so
/// what it should assert is an open question.
const NO_GOLD: &[&str] = &["table_hfile_log_block"];

fn fixture_zip(name: &str) -> String {
    format!(
        "{}/data/quickstart_trips_table/mor/avro/{name}.zip",
        env!("CARGO_MANIFEST_DIR").replace("/core", "/test")
    )
}

/// Read a fixture and compare its row count against the Spark snapshot.
async fn check_against_gold(name: &str) -> std::result::Result<(), String> {
    let zip = fixture_zip(name);
    if !Path::new(&zip).exists() {
        return Err(format!("fixture zip missing at {zip}"));
    }
    let extracted = hudi_test::extract_test_table(Path::new(&zip)).join(name);

    let actual = read_fixture(&extracted.to_string_lossy())
        .await
        .map_err(|e| format!("read failed: {e}"))?;
    let gold = hudi_test::gold::read_gold_parquet(
        &hudi_test::extract_test_table(Path::new(&zip))
            .join("gold_data")
            .to_string_lossy(),
    )
    .map_err(|e| format!("gold unreadable: {e}"))?;

    if actual.num_rows() != gold.num_rows() {
        return Err(format!(
            "{} rows, gold has {}",
            actual.num_rows(),
            gold.num_rows()
        ));
    }
    Ok(())
}

/// Every fixture in [`GOLD_FIXTURES`] must reproduce its Spark snapshot.
///
/// Runs all of them before asserting, so one failure does not hide the rest.
#[tokio::test(flavor = "multi_thread")]
async fn merged_reads_match_the_spark_snapshot() {
    let mut failures = Vec::new();
    for name in GOLD_FIXTURES {
        if let Err(e) = check_against_gold(name).await {
            failures.push(format!("{name}: {e}"));
        }
    }
    assert!(
        failures.is_empty(),
        "fixtures stopped matching gold:\n  {}",
        failures.join("\n  ")
    );
}

/// Reports which known gaps still fail, and flags any that started passing.
///
/// Ignored because it is expected to fail — run it with `--ignored` to see the
/// gap list. Its value is the second assertion: a gap that starts passing
/// should be promoted into [`GOLD_FIXTURES`] rather than left here.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "documents unread fixtures; run with --ignored to see the list"]
async fn known_gaps_still_fail() {
    let mut unexpectedly_passing = Vec::new();
    for (name, expected) in KNOWN_GAPS {
        match check_against_gold(name).await {
            Ok(()) => unexpectedly_passing.push(*name),
            Err(e) => println!("{name}: still failing ({expected}) — {e}"),
        }
    }
    assert!(
        unexpectedly_passing.is_empty(),
        "these now read correctly and should move to GOLD_FIXTURES: {unexpectedly_passing:?}"
    );
}

/// The fixtures with no gold snapshot are still readable as files, so a typo in
/// the list is caught rather than silently skipping coverage.
#[test]
fn fixtures_without_gold_are_present() {
    for name in NO_GOLD {
        assert!(
            Path::new(&fixture_zip(name)).exists(),
            "{name} is listed as having no gold, but its fixture is missing"
        );
    }
}
