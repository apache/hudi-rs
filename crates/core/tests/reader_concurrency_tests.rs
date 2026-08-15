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
//! Reader-vs-writer concurrency repros.
//!
//! The `#[ignore]`d ones are probes: they print what the reader does without
//! asserting anything, which would pass whatever happened, so they do not belong
//! in the suite. (An `#[ignore]` documenting an unfixed divergence would also
//! belong here — there are none at present.) Run them with
//! `cargo test -p hudi-core --test reader_concurrency_tests -- --ignored --nocapture`.
//!
//! A divergence that gets fixed should leave this file, or become an ordinary
//! test that pins the fixed behavior. An `#[ignore]` reason describing something
//! that no longer happens is worse than no note at all.

use hudi_core::table::{ReadOptions, Table};
use std::path::{Path, PathBuf};

fn rows(batches: &[arrow_array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Copy an existing base file under a NEW file id at `instant`, leaving no
/// commit for that instant — a writer that wrote its data file and then died.
fn plant_orphan_base_file(table_dir: &Path, instant: &str) -> PathBuf {
    let existing = std::fs::read_dir(table_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "parquet"))
        .expect("fixture has a base file to copy");
    let orphan = table_dir.join(format!(
        "b179bdb3-731c-4894-b855-abfcd6921008-0_0-1-1_{instant}.parquet"
    ));
    std::fs::copy(&existing, &orphan).unwrap();
    orphan
}

fn fixture_zip(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR").replace("/core", "/test")).join(relative)
}

async fn probe(zip: &str, label: &str) {
    probe_with_reader_version(zip, label, "1").await;
    probe_with_reader_version(zip, label, "2").await;
}

async fn probe_with_reader_version(zip: &str, label: &str, reader_version: &str) {
    use hudi_core::config::read::HudiReadConfig;
    let dir = hudi_test::extract_test_table_fresh(&fixture_zip(zip));
    let table_dir = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.is_dir() && p.join(".hoodie").exists())
        .expect("extracted fixture holds one table dir");

    let opts = ReadOptions::new().with_hudi_option(
        HudiReadConfig::FileGroupReaderVersion.as_ref(),
        reader_version,
    );
    let table = Table::new(table_dir.to_str().unwrap()).await.unwrap();
    let before = rows(&table.read(&opts).await.unwrap());

    // One tick below the latest commit: above the archival boundary (so it reads
    // as pending, not as archived) and at or below the snapshot bound (so the
    // as-of filter does not exclude it either). That is an in-flight commit.
    let latest_ts = table
        .get_timeline()
        .get_latest_commit_timestamp()
        .unwrap()
        .to_string();
    let orphan_instant = format!(
        "{:0width$}",
        latest_ts.parse::<u64>().unwrap() - 1,
        width = latest_ts.len()
    );
    let orphan_instant = orphan_instant.as_str();
    plant_orphan_base_file(&table_dir, orphan_instant);

    let table = Table::new(table_dir.to_str().unwrap()).await.unwrap();
    let after = rows(&table.read(&opts).await.unwrap());
    assert_eq!(
        after, before,
        "{label} (reader version {reader_version}): a base file whose commit never \
         completed must not be readable; {before} rows became {after}"
    );
}

/// Regression test: a base file whose commit never completed is not readable, on
/// either timeline layout and under either reader version.
///
/// This began as an `#[ignore]`d probe recording a divergence: readability was
/// decided by finding a completion timestamp, and layout v1 records none, so
/// nothing was filtered there and a snapshot returned rows from commits that
/// never completed — 4 rows became 7 on the v6 fixtures. The fix landed in the
/// same commit that added the probe (`CompletionTimeView` gained
/// `completed_requests`, populated for both layouts, plus the archival
/// boundary), and the note was never updated. It is an ordinary test now,
/// because the thing it documented no longer happens.
#[tokio::test]
async fn test_a_base_file_from_an_uncommitted_instant_is_not_readable() {
    // Instant sits between the fixture's two commits, so it is <= the snapshot
    // timestamp and therefore not excluded by the as-of bound.
    probe(
        "data/sample_table/cow/v6_nonpartitioned.zip",
        "v6 COW (layout v1)",
    )
    .await;
    probe(
        "data/sample_table/mor/parquet/v6_nonpartitioned.zip",
        "v6 MOR (layout v1)",
    )
    .await;
    probe(
        "data/sample_table/cow/v8_nonpartitioned.zip",
        "v8 COW (layout v2)",
    )
    .await;
}

/// The instants in the v9 fixture, as (requested, completion) pairs.
const V9_INSTANTS: &[(&str, &str)] = &[
    ("20260307162557587", "20260307162601961"),
    ("20260307162604102", "20260307162605084"),
    ("20260307162606034", "20260307162606865"),
    ("20260307162607806", "20260307162608609"),
    ("20260307162609526", "20260307162610303"),
    ("20260307162611195", "20260307162612005"),
    ("20260307162612542", "20260307162613746"),
    ("20260307162615591", "20260307162616433"),
    ("20260307162616932", "20260307162617668"),
    ("20260307162620509", "20260307162621330"),
];

/// An incremental read admits a commit by its COMPLETION time, as Hudi does.
///
/// Java's `CompletionTimeQueryViewV2.getInstantTimes` filters
/// `instantTime -> completionTime` by the window, and deliberately loads
/// instants from a day BEFORE the window start so a long transaction requested
/// earlier is still found. Ranging on requested time instead diverged whenever a
/// window straddled one commit's requested/completion pair — the divergence this
/// used to document, before it was fixed.
///
/// The window below is chosen to tell the two apart: it contains instant #2's
/// completion time and no instant's requested time. Both halves are asserted, so
/// the test cannot pass by ranging on neither: the discriminating window must
/// return the commit, and a window around the same commit's *requested* time
/// must return nothing.
///
/// The window's discriminating power is asserted too. If a regenerated fixture
/// ever made the two measures agree here, this would go back to proving nothing,
/// and it says so instead.
#[tokio::test]
async fn incremental_admits_a_commit_by_completion_time_not_requested_time() {
    use hudi_core::config::read::HudiReadConfig;
    use hudi_core::table::QueryType;

    let dir = hudi_test::extract_test_table_fresh(&fixture_zip(
        "data/sample_table/mor/avro/v9_txns_simple_meta.zip",
    ));
    let table_dir = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.is_dir() && p.join(".hoodie").exists())
        .unwrap();
    let table = Table::new(table_dir.to_str().unwrap()).await.unwrap();

    // A window that contains instant #2's COMPLETION time but not its
    // REQUESTED time, and no other instant by either measure.
    let start = "20260307162604500";
    let end = "20260307162605500";

    let by_requested: Vec<&str> = V9_INSTANTS
        .iter()
        .filter(|(req, _)| *req > start && *req <= end)
        .map(|(req, _)| *req)
        .collect();
    let by_completion: Vec<&str> = V9_INSTANTS
        .iter()
        .filter(|(_, comp)| *comp > start && *comp <= end)
        .map(|(req, _)| *req)
        .collect();
    println!("window ({start}, {end}]");
    println!("  instants by REQUESTED time  : {by_requested:?}");
    println!("  instants by COMPLETION time : {by_completion:?}  <- what Hudi 1.x returns");

    // The window only distinguishes the two measures if exactly one instant
    // falls in it by completion time and none by requested time.
    assert!(
        by_requested.is_empty(),
        "the window must contain no instant's REQUESTED time, or it cannot tell \
         the two measures apart; got {by_requested:?}"
    );
    assert_eq!(
        by_completion.len(),
        1,
        "the window must contain exactly one instant's COMPLETION time; got {by_completion:?}"
    );

    for reader_version in ["1", "2"] {
        let options = ReadOptions::new()
            .with_query_type(QueryType::Incremental)
            .with_start_timestamp(start)
            .with_end_timestamp(end)
            .with_hudi_option(
                HudiReadConfig::FileGroupReaderVersion.as_ref(),
                reader_version,
            );
        let got = rows(&table.read(&options).await.unwrap());

        // Control: a window around the same commit's REQUESTED time.
        let control_opts = ReadOptions::new()
            .with_query_type(QueryType::Incremental)
            .with_start_timestamp("20260307162604000")
            .with_end_timestamp("20260307162604500")
            .with_hudi_option(
                HudiReadConfig::FileGroupReaderVersion.as_ref(),
                reader_version,
            );
        let control = rows(&table.read(&control_opts).await.unwrap());
        println!(
            "  reader version={reader_version} completion-time window -> {got} row(s); \
             requested-time control window -> {control} row(s)"
        );

        assert_eq!(
            got, 1,
            "reader version '{reader_version}': the commit whose COMPLETION time is in the window \
             must be returned, as Hudi returns it"
        );
        assert_eq!(
            control, 0,
            "reader version '{reader_version}': a window holding only the commit's REQUESTED time \
             must return nothing — returning rows there is ranging on requested time"
        );
    }
}

/// Does filtering uncommitted files also discard files from ARCHIVED commits?
///
/// Java's guard is `containsInstant(ts) || isBeforeTimelineStarts(ts)`
/// (`BaseHoodieTimeline.java:494`). The second disjunct is what keeps a file
/// written by a commit that has since been archived visible: it is committed,
/// it is just no longer in the active timeline. A membership-only test would
/// drop it.
#[ignore = "probe: archived-commit visibility"]
#[tokio::test]
async fn probe_archived_commit_file_visibility() {
    let dir = hudi_test::extract_test_table_fresh(&fixture_zip(
        "data/sample_table/cow/v8_nonpartitioned.zip",
    ));
    let table_dir = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.is_dir() && p.join(".hoodie").exists())
        .unwrap();

    let earliest = "20250713010535797";
    let as_of = ReadOptions::new().with_as_of_timestamp(earliest);

    let table = Table::new(table_dir.to_str().unwrap()).await.unwrap();
    let before = rows(&table.read(&as_of).await.unwrap());

    // Archive it: Hudi moves the completed instant out of the active timeline.
    let timeline = table_dir.join(".hoodie").join("timeline");
    let archived_dir = timeline.join("history");
    std::fs::create_dir_all(&archived_dir).unwrap();
    let mut moved = 0;
    for entry in std::fs::read_dir(&timeline).unwrap().filter_map(|e| e.ok()) {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(earliest) {
            std::fs::rename(entry.path(), archived_dir.join(&name)).unwrap();
            moved += 1;
        }
    }
    assert!(
        moved > 0,
        "expected to archive the earliest instant's files"
    );

    let table = Table::new(table_dir.to_str().unwrap()).await.unwrap();
    let after = rows(&table.read(&as_of).await.unwrap());
    println!(
        "v8 time-travel as-of the earliest commit: rows before archival={before} after={after}  {}",
        if after == before {
            "OK (archived commit still visible)"
        } else {
            "*** COMMITTED ROWS LOST AFTER ARCHIVAL ***"
        }
    );
}

#[ignore = "probe: what commit times does the window's slice carry?"]
#[tokio::test]
async fn probe_commit_times_in_window_slice() {
    use hudi_core::config::read::HudiReadConfig;
    let dir = hudi_test::extract_test_table_fresh(&fixture_zip(
        "data/sample_table/mor/avro/v9_txns_simple_meta.zip",
    ));
    let table_dir = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.is_dir() && p.join(".hoodie").exists())
        .unwrap();
    let table = Table::new(table_dir.to_str().unwrap()).await.unwrap();

    // Snapshot as of the window end: these are the rows the incremental read
    // then masks.
    let opts = ReadOptions::new()
        .with_as_of_timestamp("20260307162605500")
        .with_hudi_option(HudiReadConfig::FileGroupReaderVersion.as_ref(), "2");
    let batches = table.read(&opts).await.unwrap();
    for b in &batches {
        let col = b.column_by_name("_hoodie_commit_time").unwrap();
        let s = col
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let mut times: Vec<&str> = (0..b.num_rows()).map(|i| s.value(i)).collect();
        times.sort_unstable();
        times.dedup();
        println!(
            "slice rows={} distinct _hoodie_commit_time={:?}",
            b.num_rows(),
            times
        );
    }
}
