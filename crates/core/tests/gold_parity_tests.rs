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
//! Every fixture, both file group reader versions, against what Hudi returns.
//!
//! Each fixture ships a `gold_data/` snapshot produced by Spark reading the same
//! table. This walks the whole corpus and reads each one twice — once with the
//! table twice — once with file group reader version 1, once with version 2 — and compares both
//! against that snapshot.
//!
//! The point is the second column. Individual tests assert values we wrote down
//! ourselves; this asserts agreement with the reference implementation, across
//! the version / keygen / partitioning / log-format grid rather than the handful
//! of fixtures that happened to get attention.
//!
//! # The option matrix
//!
//! A `SELECT *` snapshot only proves the two versions agree when nothing is asked of
//! them. Projection is where they differ structurally — it reaches the base file
//! read, the log-block decode and the post-merge trim by different routes — and
//! read-optimized and incremental change which files are even considered. So a
//! fixture may additionally ship `gold_options/`: one Spark snapshot per read
//! option case, plus a manifest naming the cases.
//!
//! That manifest is written by the generator that produces the snapshots
//! (`crates/test/data/quickstart_trips_table/mor/avro/gold_options.scala`) and
//! is only *read* here. A case is therefore never spelled out twice, so this
//! test and the generator cannot come to disagree about what a case means. Which
//! cases a fixture ships depends on the fixture — a table declaring no ordering
//! field has no `drop_ordering` case — so nothing here may assume a fixed set.
//!
//! # Skipping
//!
//! A fixture with no `gold_data/` is skipped, and so is one with no
//! `gold_options/`. Skipping silently is how a corpus ends up looking covered
//! when it is not, so both sets are asserted against a written-down expectation
//! rather than merely printed.

use std::collections::HashMap;
use std::path::Path;

use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use hudi_core::config::read::HudiReadConfig;
use hudi_core::table::{QueryType, ReadOptions, Table};
use hudi_test::gold_options::{OptionCase, has_option_manifest, read_option_manifest};
use hudi_test::{QuickstartTripsTable, SampleTable, TableFormat};
use strum::IntoEnumIterator;

/// The file group reader versions under comparison. Version 1 is what served
/// every read before this port.
const READER_VERSIONS: [&str; 2] = ["1", "2"];

/// Rendered stand-in for a NULL, at any depth.
///
/// Rendering with an explicit token is what lets a null be told apart from an
/// empty value: with the default options both a NULL cell and `""` render to
/// `""`, and — the case that matters for the container fixtures — so do a null
/// list element and an empty one. `FormatOptions` is handed to nested child
/// formatters too, so this reaches inside lists, maps and structs, where a
/// positional `is_null` check cannot.
const NULL_TOKEN: &str = "<null>";

/// Fixtures shipping no `gold_data/`, and why.
///
/// Lance fixtures have no Spark snapshot; `table_hfile_log_block` was dumped
/// against a reader with no HFile support, so what it should assert is still an
/// open question. Asserted rather than printed so a fixture cannot quietly stop
/// being compared.
const EXPECTED_WITHOUT_GOLD: &[&str] = &[
    "table_hfile_log_block [MorAvro]",
    // Partition-pruning fixtures. They exist to pin the on-disk partition path shape a
    // hive-style timestamp or custom key generator produces, which is asserted directly in
    // table_read_tests, so they carry no Hudi read snapshot.
    "v6_customkeygen_hivestyle [Cow]",
    "v6_timebasedkeygen_hivestyle_day [Cow]",
    "v6_timebasedkeygen_hivestyle_hour [Cow]",
    "v9_customkeygen_hivestyle [Cow]",
    "v9_lance_nonhivestyle [MorAvro]",
    "v9_lance_nonpartitioned [Cow]",
    "v9_lance_txns_nonpart [Cow]",
    "v9_lance_txns_simple [Cow]",
    "v9_trips_lance [Cow]",
    "v9_trips_lance [MorAvro]",
];

/// Fixtures shipping a `gold_options/` manifest.
///
/// The option matrix currently covers the merge-on-read corpus; a fixture that
/// stops shipping a manifest, or one that starts, has to be reflected here.
const EXPECTED_OPTION_FIXTURES: &[&str] = &[
    "table_all_data_types [MorAvro]",
    "table_column_projection [MorAvro]",
    "table_corrupt_tail_block [MorAvro]",
    "table_delete_ord_decimal [MorAvro]",
    "table_delete_ord_double [MorAvro]",
    "table_delete_ord_int [MorAvro]",
    "table_delete_ord_long [MorAvro]",
    "table_delete_ord_string [MorAvro]",
    "table_delete_ord_timestamp [MorAvro]",
    "table_event_time_stale [MorAvro]",
    "table_evo_add_col [MorAvro]",
    "table_evo_promotion [MorAvro]",
    "table_log_compaction [MorAvro]",
    "table_log_only [MorAvro]",
    "table_null_containers [MorAvro]",
    "table_parquet_log_block [MorAvro]",
    "table_partial_update [MorAvro]",
    "table_uncommitted_log_v6 [MorAvro]",
    "table_uncommitted_log_v9 [MorAvro]",
    "v6_trips_8i1u [MorAvro]",
    "v6_trips_8i3d [MorAvro]",
    "v8_mor_boundary_windows [MorAvro]",
    "v8_trips_8i3u1d [MorAvro]",
    "v9_mor_8i4u_commit_time [MorAvro]",
    "v9_mor_compacted_incremental [MorAvro]",
    "v9_mor_nonpart_3commits [MorAvro]",
];

/// One fixture in one on-disk format.
struct Fixture {
    name: String,
    format: TableFormat,
    table_path: String,
    gold_dir_owned: String,
    options_dir_owned: String,
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

    fn has_option_cases(&self) -> bool {
        has_option_manifest(&self.options_dir_owned)
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
                options_dir_owned: table.option_cases_dir(format),
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
                options_dir_owned: table.option_cases_dir(format),
                gold_positions_dir: table.gold_positions_dir(format),
            });
        }
    }
    fixtures
}

/// Build the read for a case: the reader version under test, how it merges, plus
/// whichever options the case sets. `None` is the full `SELECT *` read.
fn options_for(
    reader_version: &str,
    merge_by_position: bool,
    case: Option<&OptionCase>,
) -> ReadOptions {
    let mut options = ReadOptions::new()
        .with_hudi_option(
            HudiReadConfig::FileGroupReaderVersion.as_ref(),
            reader_version,
        )
        .with_hudi_option(
            HudiReadConfig::MergeUseRecordPositions.as_ref(),
            merge_by_position.to_string(),
        );
    let Some(case) = case else { return options };
    if let Some(columns) = &case.projection {
        options = options.with_projection(columns.clone());
    }
    if case.read_optimized {
        options = options.with_hudi_option(HudiReadConfig::UseReadOptimizedMode.as_ref(), "true");
    }
    if let (Some(start), Some(end)) = (&case.start_timestamp, &case.end_timestamp) {
        options = options
            .with_query_type(QueryType::Incremental)
            .with_start_timestamp(start)
            .with_end_timestamp(end);
    }
    options
}

async fn read_with(
    reader_version: &str,
    merge_by_position: bool,
    table_path: &str,
    case: Option<&OptionCase>,
) -> Result<RecordBatch, String> {
    let table = Table::new(table_path)
        .await
        .map_err(|e| format!("open failed: {e}"))?;
    let options = options_for(reader_version, merge_by_position, case);
    let batches = table
        .read(&options)
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

/// Compare one fixture's full read under one reader version, returning a failure
/// description.
///
/// Rows are compared as a multiset — every user column rendered, then both
/// sides sorted — rather than positionally against a key. Several fixtures
/// carry duplicate record keys, so there is no column that identifies a row.
async fn compare_owned(
    table_path: &str,
    gold_dir: &str,
    reader_version: &str,
    merge_by_position: bool,
) -> Result<(), String> {
    let actual = read_with(reader_version, merge_by_position, table_path, None).await?;
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

    compare_rows(&actual, &gold, &columns)
}

/// Compare one read-option case under one reader version.
///
/// When the case projects, the column *set* is itself under test: the
/// projection is the exact contract, so both sides must have precisely those
/// columns in precisely that order. Comparing only the columns gold happens to
/// carry would let a reader that fails to strip an internal column — the record
/// key it merged on, say — pass unnoticed, which is the main thing the
/// projection cases exist to catch.
///
/// A case that does not project (read-optimized, incremental) has no such
/// contract, so its columns come from gold the way the full read's do.
async fn compare_option_case_owned(
    table_path: &str,
    options_dir: String,
    case: OptionCase,
    reader_version: &str,
) -> Result<(), String> {
    let actual = read_with(reader_version, false, table_path, Some(&case)).await?;
    let gold_dir = hudi_test::gold_options::case_gold_dir(&options_dir, &case.name);
    let gold = match hudi_test::gold::read_gold_parquet(&gold_dir) {
        Ok(gold) => gold,
        Err(e) if e.contains("produced no batches") && actual.num_rows() == 0 => return Ok(()),
        Err(e) => return Err(e),
    };

    let Some(projection) = &case.projection else {
        let columns: Vec<String> = gold
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| !n.starts_with("_hoodie_"))
            .collect();
        return compare_rows(&actual, &gold, &columns);
    };

    exact_columns(&gold, projection, "gold")?;
    exact_columns(&actual, projection, "actual")?;
    compare_rows(&actual, &gold, projection)
}

/// Fail unless `batch` has exactly `expected` columns, in order.
fn exact_columns(batch: &RecordBatch, expected: &[String], side: &str) -> Result<(), String> {
    let schema = batch.schema();
    let actual: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    if actual != expected {
        return Err(format!(
            "{side} columns mismatch: {side}={actual:?} projection={expected:?}"
        ));
    }
    Ok(())
}

/// Render both sides and compare them row for row.
fn compare_rows(
    actual: &RecordBatch,
    gold: &RecordBatch,
    columns: &[String],
) -> Result<(), String> {
    let gold_rows = render_rows(gold, columns, "gold")?;
    let actual_rows = render_rows(actual, columns, "actual")?;

    if gold_rows.len() != actual_rows.len() {
        // Show the rows themselves: a bare count leaves no way to tell a
        // dropped row from a duplicated one without rerunning by hand.
        let sample = |rows: &[String]| {
            rows.iter()
                .take(6)
                .map(|r| format!("\n        {r}"))
                .collect::<String>()
        };
        return Err(format!(
            "row count mismatch: actual={} gold={}\n      actual rows:{}\n      gold rows:{}",
            actual_rows.len(),
            gold_rows.len(),
            sample(&actual_rows),
            sample(&gold_rows)
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
    // One formatter per column, built with an explicit null token so a NULL is
    // never conflated with an empty value — at the top level or nested inside a
    // list, map or struct.
    let format_options = FormatOptions::new().with_null(NULL_TOKEN);
    let mut formatters = Vec::with_capacity(columns.len());
    for name in columns {
        let idx = batch.schema().index_of(name).map_err(|_| {
            format!("column '{name}' present in gold but missing from {side} output")
        })?;
        formatters.push(
            ArrayFormatter::try_new(batch.column(idx).as_ref(), &format_options)
                .map_err(|e| format!("build {side} formatter for '{name}': {e}"))?,
        );
    }
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut cells = Vec::with_capacity(formatters.len());
        for formatter in &formatters {
            let rendered = formatter.value(row).to_string();
            // A timestamp carrying a zone renders with a trailing `Z` and one
            // without does not, for the same instant. The zone is schema
            // metadata, not a value difference.
            cells.push(rendered.strip_suffix('Z').unwrap_or(&rendered).to_string());
        }
        rows.push(cells.join(" | "));
    }
    rows.sort();
    Ok(rows)
}

/// One comparison that disagreed with Hudi.
struct Failure {
    fixture: String,
    /// The option case, or `None` for the full `SELECT *` read.
    case: Option<String>,
    reader_version: &'static str,
    /// Whether the read merged by record position rather than by record key.
    merge_by_position: bool,
    message: String,
}

impl Failure {
    /// The reader version as the sweep reports it, distinguishing the two merges.
    fn version_label(&self) -> String {
        if self.merge_by_position {
            format!("{}+positions", self.reader_version)
        } else {
            self.reader_version.to_string()
        }
    }

    fn describe(&self, format: TableFormat) -> String {
        let version = self.version_label();
        match &self.case {
            Some(case) => format!(
                "{} [{format:?}] case '{case}' reader version '{version}': {}",
                self.fixture, self.message
            ),
            None => format!(
                "{} [{format:?}] reader version '{version}': {}",
                self.fixture, self.message
            ),
        }
    }
}

/// Which comparisons a [`Known`] entry speaks for.
enum CaseScope {
    /// The full `SELECT *` read only. An option case on the same fixture is
    /// still held to account.
    SelectStar,
    /// One named option case.
    Case(&'static str),
    /// Every comparison for this fixture.
    ///
    /// Deliberate blanket coverage, and only defensible when the reader fails
    /// on the fixture *whatever* is asked of it — every case failing for the one
    /// cause. Where a fixture fails only for some cases, name them with
    /// [`CaseScope::Case`] instead: a blanket there would absorb a later
    /// regression in the cases that pass today, which is the hole this ratchet
    /// exists to close.
    Any,
}

/// A disagreement we have already explained.
///
/// The reader version is part of the identity: nearly every entry below describes
/// a *version 1* failure, and without naming the version the same entry would
/// quietly absorb a version 2 regression on that fixture too.
struct Known {
    fixture: &'static str,
    scope: CaseScope,
    reader_version: &'static str,
    reason: &'static str,
}

impl Known {
    fn covers(&self, failure: &Failure) -> bool {
        if self.fixture != failure.fixture || self.reader_version != failure.reader_version {
            return false;
        }
        match self.scope {
            CaseScope::Any => true,
            CaseScope::SelectStar => failure.case.is_none(),
            CaseScope::Case(name) => failure.case.as_deref() == Some(name),
        }
    }
}

/// Known disagreements, each with a cause. The sweep is a ratchet: a new
/// disagreement fails the build, and one that starts passing has to be removed
/// from here, so this list cannot quietly go stale.
const KNOWN: &[Known] = &[
    // Version 1 only: it applies no completed/inflight check to log blocks, so
    // the blocks of a delta commit that never completed still reach the merge.
    // The fixture exists to pin that version 2 does check; this records that
    // version 1 does not, which is the divergence itself rather than a fixture
    // flaw. Only the version 6 fixture separates the two readers: on version 9
    // the log file is dropped when the slice is built, which both of them share.
    //
    // Unlike the other version 1 entries here, which record a narrower or
    // differently-shaped answer, this one records a wrong one: rows that were
    // never committed. It is accepted rather than fixed because version 2 is the
    // default and version 1 is on its way out. Two things would reopen that: a
    // version 1 read reachable without asking for it, or version 1 outliving the
    // migration. Fixing it means giving `LogFileScanner` the instant state its
    // `scan` has no notion of, which reaches the metadata table reader too.
    Known {
        fixture: "table_uncommitted_log_v6",
        scope: CaseScope::Any,
        reader_version: "1",
        reason: "version 1 admits blocks from an instant that never completed",
    },
    // Version 1 only: it now reads a log-only slice but returns it without the
    // base columns. Nothing it is asked to project changes that, so the
    // fixture is blanketed.
    Known {
        fixture: "table_log_compaction",
        scope: CaseScope::Any,
        reader_version: "1",
        reason: "version 1 drops the base columns",
    },
    Known {
        fixture: "table_log_only",
        scope: CaseScope::Any,
        reader_version: "1",
        reason: "version 1 drops the base columns",
    },
    // Version 1 only: the partition column comes back as Utf8 from one file slice
    // and Int64 from another, so the batches cannot be concatenated. The
    // version 2 reads both as Int64 now that it takes the table's
    // schema rather than each base file's.
    Known {
        fixture: "v9_timebasedkeygen_epochmillis",
        scope: CaseScope::SelectStar,
        reader_version: "1",
        reason: "batches of differing shape",
    },
    Known {
        fixture: "v9_timebasedkeygen_unixtimestamp",
        scope: CaseScope::SelectStar,
        reader_version: "1",
        reason: "batches of differing shape",
    },
    // Version 1 only, all fixed in version 2: it concatenates a
    // partial-update log block onto a wider base batch without checking and
    // panics; it models an avro map's entries field differently between base
    // and log; and it keeps a superseded row.
    Known {
        fixture: "table_partial_update",
        scope: CaseScope::Any,
        reader_version: "1",
        reason: "version 1 panics on a partial-update block",
    },
    // The same version 1 limitation as the fixture above, reached by the same
    // route: the partial block's narrower schema is concatenated onto the base
    // batch positionally, so the key column meets the ordering column. Version 2
    // folds both blocks correctly, which is what this fixture exists to pin.
    Known {
        fixture: "table_partial_update_event_time",
        scope: CaseScope::Any,
        reader_version: "1",
        reason: "version 1 concatenates a partial-update block onto a wider base batch",
    },
    Known {
        fixture: "table_all_data_types",
        scope: CaseScope::Any,
        reader_version: "1",
        reason: "version 1 models an avro map's entries field",
    },
    Known {
        fixture: "table_column_projection",
        scope: CaseScope::Any,
        reader_version: "1",
        reason: "version 1 models an avro map's entries field",
    },
    Known {
        fixture: "table_null_containers",
        scope: CaseScope::Any,
        reader_version: "1",
        reason: "version 1 models an avro map's entries field",
    },
    Known {
        fixture: "table_evo_promotion",
        scope: CaseScope::Any,
        reader_version: "1",
        reason: "version 1 cannot widen a promoted column",
    },
    // Version 1 only, and only partly: it never sees the column a later writer
    // added, so the full read comes back without it and any projection naming it
    // fails outright. The cases that don't name `extra` read correctly, so they
    // are held to account rather than blanketed.
    Known {
        fixture: "table_evo_add_col",
        scope: CaseScope::SelectStar,
        reader_version: "1",
        reason: "version 1 drops a column added by a later writer",
    },
    Known {
        fixture: "table_evo_add_col",
        scope: CaseScope::Case("drop_key"),
        reader_version: "1",
        reason: "version 1 cannot project a column added by a later writer",
    },
    Known {
        fixture: "table_evo_add_col",
        scope: CaseScope::Case("drop_ordering"),
        reader_version: "1",
        reason: "version 1 cannot project a column added by a later writer",
    },
    Known {
        fixture: "table_evo_add_col",
        scope: CaseScope::Case("reordered"),
        reader_version: "1",
        reason: "version 1 cannot project a column added by a later writer",
    },
    // Same cause again — the column is simply absent from what version 1 returns,
    // whatever query type asks for it.
    Known {
        fixture: "table_evo_add_col",
        scope: CaseScope::Case("read_optimized"),
        reader_version: "1",
        reason: "version 1 drops a column added by a later writer",
    },
    Known {
        fixture: "table_evo_add_col",
        scope: CaseScope::Case("incr_all"),
        reader_version: "1",
        reason: "version 1 drops a column added by a later writer",
    },
    Known {
        fixture: "table_evo_add_col",
        scope: CaseScope::Case("incr_through_penultimate"),
        reader_version: "1",
        reason: "version 1 drops a column added by a later writer",
    },
    Known {
        fixture: "v9_mor_nonpart_3commits",
        scope: CaseScope::Any,
        reader_version: "1",
        reason: "version 1 returns a stale row",
    },
    // Version 1 only: it merges whole batches by sorting and de-duplicating on
    // record key, so a file group holding a key more than once collapses to one
    // row per key. Hudi keeps every base row. Version 2 matches
    // Hudi both by key and by position.
    Known {
        fixture: "table_duplicate_keys",
        scope: CaseScope::Any,
        reader_version: "1",
        reason: "version 1 collapses duplicate record keys",
    },
];

/// Run one comparison on its own task, so a reader that panics is reported like
/// any other disagreement rather than aborting the sweep and hiding every
/// fixture after it.
async fn record<F>(failures: &mut Vec<Failure>, compared: &mut usize, failure: Failure, work: F)
where
    F: std::future::Future<Output = Result<(), String>> + Send + 'static,
{
    match tokio::spawn(work).await {
        Ok(Ok(())) => *compared += 1,
        Ok(Err(message)) => failures.push(Failure { message, ..failure }),
        Err(join) if join.is_panic() => failures.push(Failure {
            message: "PANICKED during read".to_string(),
            ..failure
        }),
        Err(join) => failures.push(Failure {
            message: join.to_string(),
            ..failure
        }),
    }
}

/// A null *inside* a container must not render the same as an empty value.
///
/// This is the one thing the sweep cannot prove by passing: if both sides render
/// a null element and an empty one to the same text, a genuine null-vs-empty
/// divergence compares equal and the container fixtures pass vacuously. The
/// check has to be made directly, on two batches that differ only in that way.
#[test]
fn a_null_list_element_renders_differently_from_an_empty_one() {
    use arrow_array::Array;
    use arrow_array::builder::{ListBuilder, StringBuilder};

    let list_batch = |empty_instead_of_null: bool| {
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("a");
        if empty_instead_of_null {
            builder.values().append_value("");
        } else {
            builder.values().append_null();
        }
        builder.append(true);
        let array = builder.finish();
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "arr",
            array.data_type().clone(),
            true,
        )]);
        RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![std::sync::Arc::new(array)],
        )
        .unwrap()
    };

    let columns = vec!["arr".to_string()];
    let with_null = render_rows(&list_batch(false), &columns, "test").unwrap();
    let with_empty = render_rows(&list_batch(true), &columns, "test").unwrap();

    assert!(
        with_null[0].contains(NULL_TOKEN),
        "a null element must render as the null token, got {:?}",
        with_null[0]
    );
    assert_ne!(
        with_null, with_empty,
        "a null list element and an empty one rendered identically, so a \
         null-vs-empty divergence inside a container would compare equal"
    );
}

/// What one pass over the corpus found: the disagreements no [`KNOWN`] entry
/// explains, which entries did explain something, and what was skipped.
struct SweepResult {
    reported: Vec<String>,
    entry_used: Vec<bool>,
    without_gold: Vec<String>,
    with_options: Vec<String>,
}

/// Read every fixture that ships gold under each of `reader_versions`, where an
/// entry is a reader version and whether it merges by record position.
///
/// Option cases run only for an entry that merges by key: a case's gold is a
/// single Spark snapshot taken without record positions, so there is nothing to
/// compare a position-merged option read against.
async fn sweep(reader_versions: &[(&'static str, bool)]) -> SweepResult {
    let fixtures = all_fixtures();
    let mut without_gold: Vec<String> = Vec::new();
    let mut with_options: Vec<String> = Vec::new();
    let mut all_failures: Vec<(TableFormat, Failure)> = Vec::new();
    let mut compared: HashMap<String, usize> = HashMap::new();
    let mut case_compared: HashMap<&str, usize> = HashMap::new();

    for fixture in &fixtures {
        if !fixture.has_gold() {
            without_gold.push(fixture.label());
            continue;
        }

        let mut failures: Vec<Failure> = Vec::new();

        for &(reader_version, merge_by_position) in reader_versions {
            let label = if merge_by_position {
                format!("{reader_version}+positions")
            } else {
                reader_version.to_string()
            };
            let path = fixture.table_path.clone();
            let gold_dir = fixture.gold_dir(merge_by_position);
            let counter = compared.entry(label).or_default();
            record(
                &mut failures,
                counter,
                Failure {
                    fixture: fixture.name.clone(),
                    case: None,
                    reader_version,
                    merge_by_position,
                    message: String::new(),
                },
                async move { compare_owned(&path, &gold_dir, reader_version, merge_by_position).await },
            )
            .await;
        }

        // Option cases compare against snapshots taken without record positions,
        // so they run once per key-merging version and not at all under position
        // merge.
        if fixture.has_option_cases() && reader_versions.iter().any(|&(_, positions)| !positions) {
            with_options.push(fixture.label());
            let manifest = match read_option_manifest(&fixture.options_dir_owned) {
                Ok(manifest) => manifest,
                Err(e) => {
                    // A malformed manifest is not a comparison failure that can
                    // be explained away; it means the fixture is wrong.
                    panic!("{}: {e}", fixture.label());
                }
            };
            for case in &manifest.cases {
                for &(reader_version, positions) in reader_versions {
                    if positions {
                        continue;
                    }
                    let path = fixture.table_path.clone();
                    let options_dir = fixture.options_dir_owned.clone();
                    let case = case.clone();
                    let case_name = case.name.clone();
                    let counter = case_compared.entry(reader_version).or_default();
                    record(
                        &mut failures,
                        counter,
                        Failure {
                            fixture: fixture.name.clone(),
                            case: Some(case_name),
                            reader_version,
                            merge_by_position: false,
                            message: String::new(),
                        },
                        async move {
                            compare_option_case_owned(&path, options_dir, case, reader_version)
                                .await
                        },
                    )
                    .await;
                }
            }
        }

        all_failures.extend(failures.into_iter().map(|f| (fixture.format, f)));
    }

    // Match each disagreement against the known list, tracking which entries
    // actually spoke for something. An entry that matches nothing describes a
    // disagreement that no longer happens, and has to go — otherwise the list
    // accumulates explanations for behaviour that has since been fixed, and
    // stops being a description of what is actually wrong.
    let mut entry_used = vec![false; KNOWN.len()];
    let mut reported: Vec<String> = Vec::new();
    for (format, failure) in &all_failures {
        match KNOWN.iter().position(|known| known.covers(failure)) {
            Some(index) => entry_used[index] = true,
            None => reported.push(failure.describe(*format)),
        }
    }

    let counts: Vec<String> = reader_versions
        .iter()
        .map(|&(reader_version, positions)| {
            let label = if positions {
                format!("{reader_version}+positions")
            } else {
                reader_version.to_string()
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
    println!(
        "option cases on {} fixtures — version 1 {}, version 2 {}",
        with_options.len(),
        case_compared.get("1").copied().unwrap_or(0),
        case_compared.get("2").copied().unwrap_or(0),
    );

    SweepResult {
        reported,
        entry_used,
        without_gold,
        with_options,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_fixture_matches_hudi_on_both_reader_versions() {
    let versions: Vec<(&str, bool)> = READER_VERSIONS.iter().map(|&v| (v, false)).collect();
    let SweepResult {
        reported,
        entry_used,
        mut without_gold,
        mut with_options,
    } = sweep(&versions).await;

    // Coverage first: a fixture that silently stopped being compared would
    // otherwise look like a pass.
    without_gold.sort();
    let without_gold: Vec<&str> = without_gold.iter().map(String::as_str).collect();
    assert_eq!(
        without_gold, EXPECTED_WITHOUT_GOLD,
        "the set of fixtures shipping no gold_data changed; update \
         EXPECTED_WITHOUT_GOLD with the reason"
    );
    with_options.sort();
    let with_options: Vec<&str> = with_options.iter().map(String::as_str).collect();
    assert_eq!(
        with_options, EXPECTED_OPTION_FIXTURES,
        "the set of fixtures shipping a gold_options manifest changed; update \
         EXPECTED_OPTION_FIXTURES"
    );

    assert!(
        reported.is_empty(),
        "{} comparison(s) disagree with Hudi for a reason not on the known list:\n  {}",
        reported.len(),
        reported.join("\n  ")
    );

    let stale: Vec<String> = KNOWN
        .iter()
        .zip(&entry_used)
        .filter(|(_, used)| !**used)
        .map(|(known, _)| {
            format!(
                "{} reader version '{}': {}",
                known.fixture, known.reader_version, known.reason
            )
        })
        .collect();
    assert!(
        stale.is_empty(),
        "{} known-disagreement entr(ies) matched nothing — the disagreement is \
         gone, so remove them:\n  {}",
        stale.len(),
        stale.join("\n  ")
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
    compare_owned(&table_path, &by_key, "2", false)
        .await
        .expect("version 2 merging by key must match Hudi merging by key");
    compare_owned(&table_path, &by_position, "2", true)
        .await
        .expect("version 2 merging by position must match Hudi merging by position");
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
/// Only version 2 is swept: version 1 has no position-based merge and ignores
/// the setting.
#[tokio::test(flavor = "multi_thread")]
async fn every_fixture_matches_hudi_when_merging_by_record_position() {
    // No entry on the known list describes a position-merged read, so anything
    // this reports is unexplained by construction.
    let SweepResult { reported, .. } = sweep(&[("2", true)]).await;

    assert!(
        reported.is_empty(),
        "{} comparison(s) disagree with Hudi under position-based merge:\n  {}",
        reported.len(),
        reported.join("\n  ")
    );
}
