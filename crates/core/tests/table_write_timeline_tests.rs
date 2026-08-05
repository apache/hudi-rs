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
use hudi_core::table::Table;
use hudi_core::table::partition::PartitionPruner;
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

#[tokio::test]
async fn test_mdt_commit_without_any_data_instant_is_fenced() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path(), TableTypeValue::CopyOnWrite).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();
    table.append([batch(vec![("b", "sf", 2)])]).await.unwrap();

    // Harsher crash shape than the orphan test: remove EVERY trace of the
    // second instant from the data timeline — completed AND fencing files —
    // leaving a completed MDT deltacommit with no data instant at all. The
    // eager rollback cannot see it (nothing pending), so only the
    // valid-instants fence protects readers.
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
        1,
        "MDT records without any data instant must be fenced: {listed:?}"
    );
    assert!(listed[0].contains(&commits[0].0));
}
