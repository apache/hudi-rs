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
//! Every fixture, both engines, against what Hudi returns.
//!
//! Each fixture ships a `gold_data/` snapshot produced by Spark reading the same
//! table. This walks the whole corpus and reads each one twice — once with the
//! reader in use today, once with the merge-on-read engine — and compares both
//! against that snapshot.
//!
//! The point is the second column. Individual tests assert values we wrote down
//! ourselves; this asserts agreement with the reference implementation, across
//! the version / keygen / partitioning / log-format grid rather than the handful
//! of fixtures that happened to get attention.
//!
//! A fixture with no `gold_data/` is skipped and counted. Skipping silently is
//! how a corpus ends up looking covered when it is not, so the count is
//! asserted at the end.

use std::collections::HashMap;
use std::path::Path;

use arrow::compute::concat_batches;
use arrow_array::{Array, RecordBatch};
use hudi_core::config::read::HudiReadConfig;
use hudi_core::table::{ReadOptions, Table};
use hudi_test::{QuickstartTripsTable, SampleTable, TableFormat};
use strum::IntoEnumIterator;

/// The readers under comparison. `legacy` is what ships today.
const ENGINES: [&str; 2] = ["legacy", "v2"];

/// One fixture in one on-disk format.
struct Fixture {
    name: String,
    format: TableFormat,
    table_path: String,
    gold_dir_owned: String,
    /// The snapshot Hudi returns when merging by record position, for the one
    /// fixture where that differs from merging by key. Absent means the two
    /// agree and `gold_dir_owned` is the answer for both.
    gold_positions_dir: Option<String>,
}

impl Fixture {
    fn label(&self) -> String {
        format!("{} [{:?}]", self.name, self.format)
    }

    /// The snapshot to compare against for one way of merging.
    fn gold_dir(&self, merge_by_position: bool) -> String {
        match (merge_by_position, &self.gold_positions_dir) {
            (true, Some(dir)) => dir.clone(),
            _ => self.gold_dir_owned.clone(),
        }
    }

    fn has_gold(&self) -> bool {
        Path::new(&self.gold_dir_owned).is_dir()
    }
}

/// Every fixture on disk, in every format it ships in.
fn all_fixtures() -> Vec<Fixture> {
    let mut fixtures = Vec::new();
    for table in SampleTable::iter() {
        for &format in table.available_formats() {
            fixtures.push(Fixture {
                name: table.as_ref().to_string(),
                format,
                table_path: table.path(format),
                gold_dir_owned: table.gold_dir(format),
                gold_positions_dir: None,
            });
        }
    }
    for table in QuickstartTripsTable::iter() {
        for &format in table.available_formats() {
            fixtures.push(Fixture {
                name: table.as_ref().to_string(),
                format,
                table_path: table.path(format),
                gold_dir_owned: table.gold_dir(format),
                gold_positions_dir: table.gold_positions_dir(format),
            });
        }
    }
    fixtures
}

async fn read_with(
    engine: &str,
    merge_by_position: bool,
    table_path: &str,
) -> Result<RecordBatch, String> {
    let table = Table::new(table_path)
        .await
        .map_err(|e| format!("open failed: {e}"))?;
    let batches = table
        .read(
            &ReadOptions::new()
                .with_hudi_option(HudiReadConfig::MergeEngine.as_ref(), engine)
                .with_hudi_option(
                    HudiReadConfig::MergeUseRecordPositions.as_ref(),
                    merge_by_position.to_string(),
                ),
        )
        .await
        .map_err(|e| format!("read failed: {e}"))?;
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(std::sync::Arc::new(
            arrow_schema::Schema::empty(),
        )));
    }
    // `concat_batches` panics rather than erroring when a batch has fewer
    // columns than the schema it is handed, so check first: a reader returning
    // batches of differing shape is a finding, not a crash.
    let schema = batches[0].schema();
    let shape = |s: &arrow_schema::Schema| -> Vec<(String, arrow_schema::DataType)> {
        s.fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect()
    };
    let first = shape(&schema);
    if let Some(odd) = batches.iter().find(|b| shape(&b.schema()) != first) {
        let diffs: Vec<String> = first
            .iter()
            .zip(shape(&odd.schema()))
            .filter(|(a, b)| *a != b)
            .map(|(a, b)| format!("{}: {:?} vs {:?}", a.0, a.1, b.1))
            .collect();
        return Err(format!(
            "reader returned batches of differing shape: {}",
            diffs.join("; ")
        ));
    }
    concat_batches(&schema, &batches).map_err(|e| format!("concat failed: {e}"))
}

/// Compare one fixture under one engine, returning a failure description.
///
/// Rows are compared as a multiset — every user column rendered, then both
/// sides sorted — rather than positionally against a key. Several fixtures
/// carry duplicate record keys, so there is no column that identifies a row.
async fn compare_owned(
    table_path: &str,
    gold_dir: &str,
    engine: &str,
    merge_by_position: bool,
) -> Result<(), String> {
    let actual = read_with(engine, merge_by_position, table_path).await?;
    let gold = match hudi_test::gold::read_gold_parquet(gold_dir) {
        Ok(gold) => gold,
        // Spark writes a snapshot with no row groups for an empty table, which
        // the gold reader reports as producing no batches. An empty table and
        // an empty read agree.
        Err(e) if e.contains("produced no batches") && actual.num_rows() == 0 => return Ok(()),
        Err(e) => return Err(e),
    };

    let columns: Vec<String> = gold
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .filter(|n| !n.starts_with("_hoodie_"))
        .collect();

    let gold_rows = render_rows(&gold, &columns, "gold")?;
    let actual_rows = render_rows(&actual, &columns, "actual")?;

    if gold_rows.len() != actual_rows.len() {
        return Err(format!(
            "row count mismatch: actual={} gold={}",
            actual_rows.len(),
            gold_rows.len()
        ));
    }
    for (actual_row, gold_row) in actual_rows.iter().zip(gold_rows.iter()) {
        if actual_row != gold_row {
            return Err(format!(
                "row differs:\n      actual {actual_row}\n      gold   {gold_row}"
            ));
        }
    }
    Ok(())
}

/// Every row rendered to a comparable string, sorted.
fn render_rows(batch: &RecordBatch, columns: &[String], side: &str) -> Result<Vec<String>, String> {
    let mut indices = Vec::with_capacity(columns.len());
    for name in columns {
        let idx = batch.schema().index_of(name).map_err(|_| {
            format!("column '{name}' present in gold but missing from {side} output")
        })?;
        indices.push(idx);
    }
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut cells = Vec::with_capacity(indices.len());
        for &idx in &indices {
            let col = batch.column(idx);
            let cell = if col.is_null(row) {
                "NULL".to_string()
            } else {
                let rendered = arrow_cast::display::array_value_to_string(col.as_ref(), row)
                    .map_err(|e| format!("render {side} cell: {e}"))?;
                // A timestamp carrying a zone renders with a trailing `Z` and
                // one without does not, for the same instant. The zone is
                // schema metadata, not a value difference.
                rendered.strip_suffix('Z').unwrap_or(&rendered).to_string()
            };
            cells.push(cell);
        }
        rows.push(cells.join(" | "));
    }
    rows.sort();
    Ok(rows)
}

/// Read every fixture that ships gold under each of `engines`, returning one
/// description per disagreement.
async fn sweep(engines: &[(&'static str, bool)]) -> Vec<String> {
    let fixtures = all_fixtures();
    let mut without_gold: Vec<String> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    let mut compared: HashMap<String, usize> = HashMap::new();

    for fixture in &fixtures {
        if !fixture.has_gold() {
            without_gold.push(fixture.label());
            continue;
        }
        for &(engine, merge_by_position) in engines {
            let label = if merge_by_position {
                format!("{engine}+positions")
            } else {
                engine.to_string()
            };
            // Run each comparison on its own task: a reader that panics should
            // be reported like any other disagreement, not abort the sweep and
            // hide every fixture after it.
            let path = fixture.table_path.clone();
            let gold_dir = fixture.gold_dir(merge_by_position);
            let outcome = tokio::spawn(async move {
                compare_owned(&path, &gold_dir, engine, merge_by_position).await
            })
            .await;
            match outcome {
                Ok(Ok(())) => *compared.entry(label).or_default() += 1,
                Ok(Err(e)) => failures.push(format!("{} engine '{label}': {e}", fixture.label())),
                Err(join) if join.is_panic() => failures.push(format!(
                    "{} engine '{label}': PANICKED during read",
                    fixture.label()
                )),
                Err(join) => failures.push(format!("{} engine '{label}': {join}", fixture.label())),
            }
        }
    }

    let counts: Vec<String> = engines
        .iter()
        .map(|&(engine, positions)| {
            let label = if positions {
                format!("{engine}+positions")
            } else {
                engine.to_string()
            };
            let n = compared.get(&label).copied().unwrap_or(0);
            format!("{label} {n}")
        })
        .collect();
    println!(
        "compared {} fixtures — {}; {} without gold",
        fixtures.len() - without_gold.len(),
        counts.join(", "),
        without_gold.len(),
    );
    if !without_gold.is_empty() {
        println!("without gold:\n  {}", without_gold.join("\n  "));
    }

    failures
}

#[tokio::test(flavor = "multi_thread")]
async fn every_fixture_matches_hudi_on_both_engines() {
    let engines: Vec<(&str, bool)> = ENGINES.iter().map(|&e| (e, false)).collect();
    let failures = sweep(&engines).await;

    // Known disagreements, each with a cause. The sweep is a ratchet: a new
    // disagreement fails the build, and one that starts passing has to be
    // removed from here, so this list cannot quietly go stale.
    let known: &[(&str, &str)] = &[
        // Legacy only: it now reads a log-only slice but returns it without the
        // base columns.
        ("table_log_compaction", "legacy drops the base columns"),
        ("table_log_only", "legacy drops the base columns"),
        // Legacy only: the partition column comes back as Utf8 from one file
        // slice and Int64 from another, so the batches cannot be concatenated.
        // The merge-on-read engine reads both as Int64 now that it takes the
        // table's schema rather than each base file's.
        (
            "v9_timebasedkeygen_epochmillis",
            "batches of differing shape",
        ),
        (
            "v9_timebasedkeygen_unixtimestamp",
            "batches of differing shape",
        ),
        // Legacy only, all fixed in the merge-on-read engine:
        // it concatenates a partial-update log block onto a wider base batch
        // without checking and panics; it models an avro map's entries field
        // differently between base and log; and it keeps a superseded row.
        (
            "table_partial_update",
            "legacy panics on a partial-update block",
        ),
        (
            "table_all_data_types",
            "legacy models an avro map's entries field",
        ),
        (
            "table_column_projection",
            "legacy models an avro map's entries field",
        ),
        (
            "table_null_containers",
            "legacy models an avro map's entries field",
        ),
        (
            "table_evo_promotion",
            "legacy cannot widen a promoted column",
        ),
        (
            "table_evo_add_col",
            "legacy drops a column added by a later writer",
        ),
        ("v9_mor_nonpart_3commits", "legacy returns a stale row"),
        // Legacy only: it merges whole batches by sorting and de-duplicating on
        // record key, so a file group holding a key more than once collapses to
        // one row per key. Hudi keeps every base row. The merge-on-read engine
        // matches Hudi both by key and by position.
        (
            "table_duplicate_keys",
            "legacy collapses duplicate record keys",
        ),
    ];
    let unexpected: Vec<&String> = failures
        .iter()
        .filter(|f| !known.iter().any(|(name, _)| f.starts_with(name)))
        .collect();
    assert!(
        unexpected.is_empty(),
        "{} comparison(s) disagree with Hudi for a reason not on the known list:\n  {}",
        unexpected.len(),
        unexpected
            .iter()
            .map(|f| f.as_str())
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// The two merge strategies are not the same read.
///
/// Everywhere else in the corpus they agree, which means the position sweep
/// below would still pass if position merge quietly fell back to key merge. This
/// is the fixture that can tell them apart: one file group holding `k1` twice
/// and `k2` three times, an upsert of `k1` and a delete of `k2`. Hudi's writer
/// tags an incoming record with every base row its index matches, so those
/// become two and three log records; keyed by record key they collapse to one
/// entry each and only the first base row of each key is merged.
///
/// Both expected answers are Hudi's own, taken from the same table with only
/// `hoodie.merge.use.record.positions` differing. Asserting they differ is what
/// keeps the fixture honest: if a regenerated one ever made them equal, the
/// position sweep would go back to proving nothing and this would say so.
#[tokio::test(flavor = "multi_thread")]
async fn merging_by_position_and_by_key_disagree_on_duplicate_record_keys() {
    let fixture = QuickstartTripsTable::MorLayoutDuplicateKeys;
    let table_path = fixture.path(TableFormat::MorAvro);
    let by_key = fixture.gold_dir(TableFormat::MorAvro);
    let by_position = fixture
        .gold_positions_dir(TableFormat::MorAvro)
        .expect("the duplicate-key fixture ships a position-merge snapshot");

    let rows = |gold: &str| {
        let batch = hudi_test::gold::read_gold_parquet(gold).expect("gold readable");
        let columns = vec!["id".to_string(), "val".to_string()];
        render_rows(&batch, &columns, "gold").expect("gold renderable")
    };
    assert_eq!(
        rows(&by_key),
        vec![
            "k1 | k1-row-b",
            "k1 | k1-updated",
            "k2 | k2-row-b",
            "k2 | k2-row-c",
            "k3 | k3-row-a",
        ],
        "merging by key leaves a stale k1 row and two undeleted k2 rows"
    );
    assert_eq!(
        rows(&by_position),
        vec!["k1 | k1-updated", "k1 | k1-updated", "k3 | k3-row-a"],
        "merging by position reaches every duplicate"
    );

    // And the reader lands on whichever of the two it was asked for.
    compare_owned(&table_path, &by_key, "v2", false)
        .await
        .expect("v2 merging by key must match Hudi merging by key");
    compare_owned(&table_path, &by_position, "v2", true)
        .await
        .expect("v2 merging by position must match Hudi merging by position");
}

/// The same corpus read with `hoodie.merge.use.record.positions` on.
///
/// Position-based merge matches a log record to the base row it updates by that
/// row's index in the base file rather than by record key, and several of these
/// fixtures were written by Hudi with `RECORD_POSITIONS` headers in their log
/// blocks — so this is the path actually running, not a configuration that gets
/// ignored. None of these tables has duplicate record keys inside one file
/// group, which is the only case where the two strategies can disagree, so the
/// answer must be the one Hudi gives either way.
///
/// Only the merge-on-read engine is swept: the legacy reader has no
/// position-based merge and ignores the setting.
#[tokio::test(flavor = "multi_thread")]
async fn every_fixture_matches_hudi_when_merging_by_record_position() {
    let failures = sweep(&[("v2", true)]).await;

    assert!(
        failures.is_empty(),
        "{} comparison(s) disagree with Hudi under position-based merge:\n  {}",
        failures.len(),
        failures
            .iter()
            .map(|f| f.as_str())
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}
