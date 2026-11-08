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
//! Instant lifecycle and MDT fencing semantics (Java 1.0.2 timeline layout v2):
//! completion times minted at completion, instant state contents, one MDT
//! deltacommit per data instant, and MDT reads fenced by the data timeline.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::table::TableTypeValue;
use hudi_core::metadata::commit::HoodieCommitMetadata;
use hudi_core::metadata::replace_commit::HoodieRequestedReplaceMetadata;
use hudi_core::table::partition::PartitionPruner;
use hudi_core::table::{QueryType, ReadOptions, Table};
use tempfile::tempdir;

fn trips_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn batch(rows: Vec<(&str, &str, i64)>) -> RecordBatch {
    RecordBatch::try_new(
        trips_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, city, _)| *city).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, _, value)| *value).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn create_partitioned(dir: &Path, table_type: TableTypeValue) -> Table {
    Table::create(dir.to_str().unwrap())
        .with_table_name("trips")
        .with_table_type(table_type)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .create()
        .await
        .unwrap()
}

fn timeline_files(timeline: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(timeline)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.chars().next().is_some_and(|c| c.is_ascii_digit()))
        .collect();
    names.sort();
    names
}

/// `({requested}, {completion})` for a completed instant file of an action.
fn completed_times(timeline: &Path, action_suffix: &str) -> Vec<(String, String)> {
    timeline_files(timeline)
        .into_iter()
        .filter(|n| n.ends_with(action_suffix) && n.contains('_'))
        .map(|n| {
            let stem = n.trim_end_matches(action_suffix);
            let (requested, completion) = stem.trim_end_matches('.').split_once('_').unwrap();
            (requested.to_string(), completion.to_string())
        })
        .collect()
}

#[tokio::test]
async fn test_completion_times_minted_at_completion_and_monotonic() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path(), TableTypeValue::CopyOnWrite).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();
    table.upsert([batch(vec![("a", "sf", 2)])]).await.unwrap();

    let data_timeline = dir.path().join(".hoodie/timeline");
    let commits = completed_times(&data_timeline, ".commit");
    assert_eq!(commits.len(), 2);
    for (requested, completion) in &commits {
        assert!(
            completion > requested,
            "completion {completion} must be later than requested {requested}"
        );
    }
    // Sequential commits do not overlap: prior completion < next requested.
    assert!(commits[0].1 < commits[1].0);

    // The MDT deltacommit reuses the data requested time but mints its own,
    // earlier completion (MDT completes before the data commit).
    let mdt_timeline = dir.path().join(".hoodie/metadata/.hoodie/timeline");
    for (requested, data_completion) in &commits {
        let mdt = completed_times(&mdt_timeline, ".deltacommit")
            .into_iter()
            .find(|(r, _)| r == requested)
            .unwrap_or_else(|| panic!("missing MDT deltacommit for {requested}"));
        assert!(mdt.1 > *requested);
        assert!(mdt.1 < *data_completion, "MDT completes before data commit");
    }
}

#[tokio::test]
async fn test_instant_state_contents_match_java() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path(), TableTypeValue::CopyOnWrite).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();
    table
        .dynamic_partition_overwrite([batch(vec![("b", "sf", 2)])])
        .await
        .unwrap();

    let data_timeline = dir.path().join(".hoodie/timeline");
    let names = timeline_files(&data_timeline);

    // commit: requested empty, inflight parses as commit metadata w/ operation.
    let commit_requested = names
        .iter()
        .find(|n| n.ends_with(".commit.requested"))
        .unwrap();
    assert_eq!(
        std::fs::metadata(data_timeline.join(commit_requested))
            .unwrap()
            .len(),
        0,
        "commit.requested must be empty"
    );
    let commit_inflight = names
        .iter()
        .find(|n| n.ends_with(".inflight") && !n.contains(".commit.") && !n.contains("replace"))
        .unwrap();
    let inflight_meta = HoodieCommitMetadata::from_avro_bytes(
        &std::fs::read(data_timeline.join(commit_inflight)).unwrap(),
    )
    .unwrap();
    assert_eq!(inflight_meta.operation_type.as_deref(), Some("INSERT"));

    // replacecommit: requested carries the plan (HoodieRequestedReplaceMetadata).
    let replace_requested = names
        .iter()
        .find(|n| n.ends_with(".replacecommit.requested"))
        .unwrap();
    let plan = HoodieRequestedReplaceMetadata::from_avro_bytes(
        &std::fs::read(data_timeline.join(replace_requested)).unwrap(),
    )
    .unwrap();
    assert_eq!(plan.operation_type.as_deref(), Some("INSERT_OVERWRITE"));
    assert_eq!(plan.version, Some(1));
    let replace_inflight = names
        .iter()
        .find(|n| n.ends_with(".replacecommit.inflight"))
        .unwrap();
    let replace_inflight_meta = HoodieCommitMetadata::from_avro_bytes(
        &std::fs::read(data_timeline.join(replace_inflight)).unwrap(),
    )
    .unwrap();
    assert_eq!(
        replace_inflight_meta.operation_type.as_deref(),
        Some("INSERT_OVERWRITE")
    );

    // MDT deltacommit fencing states exist with parseable inflight.
    let mdt_timeline = dir.path().join(".hoodie/metadata/.hoodie/timeline");
    let mdt_names = timeline_files(&mdt_timeline);
    let mdt_inflight = mdt_names
        .iter()
        .find(|n| n.ends_with(".deltacommit.inflight"))
        .unwrap();
    HoodieCommitMetadata::from_avro_bytes(&std::fs::read(mdt_timeline.join(mdt_inflight)).unwrap())
        .unwrap();
}

#[tokio::test]
async fn test_single_mdt_deltacommit_per_data_instant() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path(), TableTypeValue::CopyOnWrite).await;
    table
        .upsert([batch(vec![("a", "sf", 1), ("b", "sf", 2)])])
        .await
        .unwrap();

    let data_timeline = dir.path().join(".hoodie/timeline");
    let (instant, _) = completed_times(&data_timeline, ".commit")
        .pop()
        .expect("one completed data commit");

    // One completed MDT deltacommit for the instant, holding files +
    // record_index + column_stats + partition_stats stats together.
    let mdt_timeline = dir.path().join(".hoodie/metadata/.hoodie/timeline");
    let completed: Vec<String> = timeline_files(&mdt_timeline)
        .into_iter()
        .filter(|n| n.starts_with(&format!("{instant}_")) && n.ends_with(".deltacommit"))
        .collect();
    assert_eq!(completed.len(), 1, "one MDT deltacommit per data instant");
    let metadata = HoodieCommitMetadata::from_avro_bytes(
        &std::fs::read(mdt_timeline.join(&completed[0])).unwrap(),
    )
    .unwrap();
    let stats = metadata.partition_to_write_stats.unwrap();
    for partition in ["files", "record_index", "column_stats", "partition_stats"] {
        assert!(
            stats.contains_key(partition),
            "MDT deltacommit missing {partition} stats: {:?}",
            stats.keys()
        );
    }
}

#[tokio::test]
async fn test_orphan_mdt_commit_is_fenced_from_reads() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path(), TableTypeValue::CopyOnWrite).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();
    table.append([batch(vec![("b", "sf", 2)])]).await.unwrap();

    // Simulate a crash between MDT completion and data completion: delete the
    // second data commit's completed file, keeping its fencing markers and its
    // completed MDT deltacommit + log blocks (an "orphan" MDT commit).
    let data_timeline = dir.path().join(".hoodie/timeline");
    let commits = completed_times(&data_timeline, ".commit");
    assert_eq!(commits.len(), 2);
    let (orphan_instant, orphan_completion) = commits.last().unwrap().clone();
    std::fs::remove_file(
        data_timeline.join(format!("{orphan_instant}_{orphan_completion}.commit")),
    )
    .unwrap();
    // The orphan's MDT deltacommit is still completed on the MDT timeline.
    let mdt_timeline = dir.path().join(".hoodie/metadata/.hoodie/timeline");
    assert!(
        completed_times(&mdt_timeline, ".deltacommit")
            .iter()
            .any(|(r, _)| r == &orphan_instant),
        "orphan MDT deltacommit must exist for this test to be meaningful"
    );

    // Readers must not trust the orphan's MDT records: the files partition
    // lists only the first commit's base file.
    let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    let partition_schema = reopened.get_partition_schema().await.unwrap();
    let pruner =
        PartitionPruner::new(&[], &partition_schema, reopened.hudi_configs.as_ref()).unwrap();
    let records = reopened
        .read_metadata_table_files_partition(&pruner)
        .await
        .unwrap();
    let sf = records.get("city=sf").expect("city=sf record");
    let listed: Vec<&str> = sf.active_file_names();
    assert_eq!(listed.len(), 1, "orphan commit's file must be fenced out");
    assert!(
        listed[0].contains(&commits[0].0),
        "surviving file should be from the first commit: {listed:?}"
    );

    // And the RLI must not resolve keys written by the orphan commit: a new
    // upsert of that key is an insert, not an update.
    let mut reopened = reopened;
    let result = reopened
        .upsert([batch(vec![("b", "sf", 20)])])
        .await
        .unwrap();
    assert_eq!(result.num_inserts, 1, "orphan RLI entry must be fenced out");
    assert_eq!(result.num_updates, 0);
}

/// Java parity for the valid-instant set: a metadata delta commit with NO data
/// instant at all is admitted (`HoodieTableMetadataUtil.getValidInstantTimestamps`
/// excludes only commits whose data instant is *pending*), because that is the
/// shape of an async indexing commit on a Spark-written table. Fencing it would
/// silently drop every record such a commit indexed. The writer never produces
/// this shape itself: it fences the data instant before touching the metadata
/// table, and rollback removes the metadata commit together with the fencing.
#[tokio::test]
async fn test_mdt_commit_without_any_data_instant_is_read_like_java() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path(), TableTypeValue::CopyOnWrite).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();
    table.append([batch(vec![("b", "sf", 2)])]).await.unwrap();

    // Remove EVERY trace of the second instant from the data timeline —
    // completed AND fencing files — leaving a completed MDT deltacommit with
    // no data instant at all: indistinguishable from an indexing commit.
    let data_timeline = dir.path().join(".hoodie/timeline");
    let commits = completed_times(&data_timeline, ".commit");
    assert_eq!(commits.len(), 2);
    let (orphan_instant, orphan_completion) = commits.last().unwrap().clone();
    for name in timeline_files(&data_timeline) {
        if name.starts_with(&orphan_instant) {
            std::fs::remove_file(data_timeline.join(name)).unwrap();
        }
    }
    let _ = orphan_completion;
    let mdt_timeline = dir.path().join(".hoodie/metadata/.hoodie/timeline");
    assert!(
        completed_times(&mdt_timeline, ".deltacommit")
            .iter()
            .any(|(r, _)| r == &orphan_instant),
        "MDT deltacommit for the vanished data instant must still exist"
    );

    let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    let partition_schema = reopened.get_partition_schema().await.unwrap();
    let pruner =
        PartitionPruner::new(&[], &partition_schema, reopened.hudi_configs.as_ref()).unwrap();
    let records = reopened
        .read_metadata_table_files_partition(&pruner)
        .await
        .unwrap();
    let sf = records.get("city=sf").expect("city=sf record");
    let listed: Vec<&str> = sf.active_file_names();
    assert_eq!(
        listed.len(),
        2,
        "an MDT commit with no data instant reads as an indexing commit: {listed:?}"
    );
    assert!(listed.iter().any(|name| name.contains(&commits[0].0)));
    assert!(listed.iter().any(|name| name.contains(&orphan_instant)));
}

fn sf_files(
    records: &std::collections::HashMap<
        String,
        hudi_core::metadata::table::records::FilesPartitionRecord,
    >,
) -> usize {
    records["city=sf"].active_file_names().len()
}

/// A reader handle opens the metadata table once and serves every later read
/// from that handle: once the first read has opened it, the metadata table's
/// own `hoodie.properties` is not read again.
#[tokio::test]
async fn test_reader_handle_opens_metadata_table_once() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path(), TableTypeValue::CopyOnWrite).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();
    table.append([batch(vec![("b", "sf", 2)])]).await.unwrap();

    let reader = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    let partition_schema = reader.get_partition_schema().await.unwrap();
    let pruner =
        PartitionPruner::new(&[], &partition_schema, reader.hudi_configs.as_ref()).unwrap();
    let options = hudi_core::table::ReadOptions::new();
    assert_eq!(reader.get_file_slices(&options).await.unwrap().len(), 2);

    let mdt_properties = dir
        .path()
        .join(".hoodie/metadata/.hoodie/hoodie.properties");
    std::fs::rename(&mdt_properties, dir.path().join("mdt.properties.moved")).unwrap();

    for _ in 0..2 {
        assert_eq!(reader.get_file_slices(&options).await.unwrap().len(), 2);
        let records = reader
            .read_metadata_table_files_partition(&pruner)
            .await
            .unwrap();
        assert_eq!(sf_files(&records), 2);
    }
}

/// A handle that reads, writes, and reads again sees its own write through the
/// metadata table: the write drops the metadata-table handle the first read
/// opened.
#[tokio::test]
async fn test_writing_handle_reads_its_own_writes_through_metadata_table() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path(), TableTypeValue::CopyOnWrite).await;
    let options = hudi_core::table::ReadOptions::new();

    for (i, id) in ["a", "b", "c"].into_iter().enumerate() {
        table
            .append([batch(vec![(id, "sf", i as i64)])])
            .await
            .unwrap();
        let partition_schema = table.get_partition_schema().await.unwrap();
        let pruner =
            PartitionPruner::new(&[], &partition_schema, table.hudi_configs.as_ref()).unwrap();
        let records = table
            .read_metadata_table_files_partition(&pruner)
            .await
            .unwrap();
        assert_eq!(sf_files(&records), i + 1);
        assert_eq!(table.get_file_slices(&options).await.unwrap().len(), i + 1);
    }

    let result = table.upsert([batch(vec![("c", "sf", 30)])]).await.unwrap();
    assert_eq!(result.num_updates, 1);
    assert_eq!(result.num_inserts, 0);
    let rows = table.read(&options).await.unwrap();
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
}

/// The instant one millisecond after `instant`.
fn instant_after(instant: &str) -> String {
    const FORMAT: &str = "%Y%m%d%H%M%S%3f";
    let at = chrono::NaiveDateTime::parse_from_str(instant, FORMAT).unwrap();
    (at + chrono::TimeDelta::milliseconds(1))
        .format(FORMAT)
        .to_string()
}

/// `(id, value)` of every row `window` reads, sorted by id, through a fresh
/// handle so it sees the timeline as it is on storage.
async fn read_window(base_uri: &str, window: &ReadOptions) -> Vec<(String, i64)> {
    let batches = Table::new(base_uri)
        .await
        .unwrap()
        .read(window)
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i).to_string(), values.value(i)));
        }
    }
    rows.sort();
    rows
}

/// `(partition, file id, instants of its files)` for every slice `window`
/// plans, through the entry point the DataFusion scan and the Python binding
/// call.
async fn plan_window(base_uri: &str, window: &ReadOptions) -> Vec<(String, String, Vec<String>)> {
    let table = Table::new(base_uri).await.unwrap();
    let mut planned: Vec<_> = table
        .get_file_slices(window)
        .await
        .unwrap()
        .into_iter()
        .map(|slice| {
            let instants = slice
                .base_file
                .iter()
                .map(|base| base.commit_timestamp.clone())
                .chain(slice.log_files.iter().map(|log| log.timestamp.clone()))
                .collect();
            (
                slice.partition_path.clone(),
                slice.file_id().to_string(),
                instants,
            )
        })
        .collect();
    planned.sort();
    planned
}

/// How the commit that completes after the window writes `a`.
#[derive(Debug)]
enum LateCommit {
    /// Updates `a` in place.
    Upsert,
    /// Replaces the file group the insert wrote.
    PartitionOverwrite,
    /// Compacts `a`'s file group after a delta commit inside the window
    /// updated it. Guards against leaving out too much: without the late
    /// compaction, the slice must still carry that delta commit's log.
    Compaction,
}

/// A commit requested inside an incremental window but completed after its end
/// is outside the window, so the window's answer is the same before and after
/// that commit completes. Concurrent writers and table services complete out of
/// requested order this way.
///
/// `x` is requested before `c3` and completes after it. The window ends at
/// `c3`'s completion, so it admits every commit but `x`: it plans the file
/// slices it planned while `x` was inflight, and none of `x`'s files.
#[tokio::test]
async fn test_incremental_window_ignores_a_commit_completing_after_its_end() {
    use LateCommit::{Compaction, PartitionOverwrite, Upsert};
    use TableTypeValue::{CopyOnWrite, MergeOnRead};

    for (table_type, late, x_action, a_in_window) in [
        (CopyOnWrite, Upsert, "commit", 1),
        (MergeOnRead, Upsert, "deltacommit", 1),
        (CopyOnWrite, PartitionOverwrite, "replacecommit", 1),
        (MergeOnRead, Compaction, "commit", 2),
    ] {
        let case = format!("{table_type:?} {late:?}");
        let dir = tempdir().unwrap();
        let base_uri = dir.path().to_str().unwrap();
        let mut table = create_partitioned(dir.path(), table_type).await;
        let c1 = table
            .append([batch(vec![("a", "sf", 1), ("b", "la", 1)])])
            .await
            .unwrap();
        let a2 = || batch(vec![("a", "sf", 2)]);
        let (x_requested, x_completion) = match late {
            Upsert => {
                let x = table.upsert([a2()]).await.unwrap();
                (x.requested_time, x.completion_time)
            }
            PartitionOverwrite => {
                let x = table.dynamic_partition_overwrite([a2()]).await.unwrap();
                (x.requested_time, x.completion_time)
            }
            Compaction => {
                table.upsert([a2()]).await.unwrap();
                let x = table.compact().await.unwrap();
                (x.requested_time, x.completion_time)
            }
        };
        let c3 = table.upsert([batch(vec![("b", "la", 2)])]).await.unwrap();
        let x_requested = x_requested.unwrap();

        let end = c3.completion_time.unwrap();
        let window = ReadOptions::new()
            .with_query_type(QueryType::Incremental)
            .with_start_timestamp(c1.requested_time.unwrap())
            .with_end_timestamp(&end);
        // Opens after `x` is requested, so it admits `c3` alone: a guard that
        // the start bound keeps `x`'s file group out of the plan.
        let opened_after_x = ReadOptions::new()
            .with_query_type(QueryType::Incremental)
            .with_start_timestamp(instant_after(&x_requested))
            .with_end_timestamp(&end);
        let expected = [
            (
                &window,
                vec![("a".to_string(), a_in_window), ("b".to_string(), 2)],
            ),
            (&opened_after_x, vec![("b".to_string(), 2)]),
        ];

        let timeline = dir.path().join(".hoodie/timeline");
        let x_completed = timeline.join(format!(
            "{x_requested}_{}.{x_action}",
            x_completion.unwrap()
        ));
        let x_metadata = std::fs::read(&x_completed).unwrap();

        std::fs::remove_file(&x_completed).unwrap();
        let planned_while_inflight = plan_window(base_uri, &window).await;
        for (options, rows) in &expected {
            assert_eq!(
                read_window(base_uri, options).await,
                *rows,
                "{case}, x inflight"
            );
        }

        std::fs::write(
            timeline.join(format!("{x_requested}_{}.{x_action}", instant_after(&end))),
            x_metadata,
        )
        .unwrap();
        let planned = plan_window(base_uri, &window).await;
        assert!(
            planned
                .iter()
                .all(|(_, _, instants)| !instants.contains(&x_requested)),
            "{case}, x completed after the window: planned x's files {planned:?}"
        );
        assert_eq!(
            planned, planned_while_inflight,
            "{case}, x completed after the window: planned slices"
        );
        for (options, rows) in &expected {
            assert_eq!(
                read_window(base_uri, options).await,
                *rows,
                "{case}, x completed after the window"
            );
        }
    }
}
