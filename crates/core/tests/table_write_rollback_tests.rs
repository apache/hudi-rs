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
//! Marker files + eager marker-based rollback (Java EAGER policy, tv8
//! deletion-only): crashed writes are rolled back at the next write's start.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::metadata::rollback::HoodieRollbackMetadata;
use hudi_core::table::Table;
use tempfile::tempdir;

fn batch(rows: Vec<(&str, &str, i64)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
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

async fn create_partitioned(dir: &Path) -> Table {
    Table::create(dir.to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .create()
        .await
        .unwrap()
}

/// Fabricate a crashed write at `ts`: fencing files, timeline-server-based
/// markers, a partial data file, and (optionally) a completed-but-orphaned MDT
/// deltacommit with an instant-named MDT log file.
fn fabricate_crashed_write(dir: &Path, ts: &str, with_mdt_orphan: bool) -> String {
    let timeline = dir.join(".hoodie/timeline");
    std::fs::write(timeline.join(format!("{ts}.commit.requested")), b"").unwrap();
    std::fs::write(timeline.join(format!("{ts}.inflight")), b"").unwrap();

    let file_name = format!("11111111-2222-3333-4444-555555555555-0_0-0-0_{ts}.parquet");
    let partition_dir = dir.join("city=sf");
    std::fs::create_dir_all(&partition_dir).unwrap();
    std::fs::write(partition_dir.join(&file_name), b"partial parquet bytes").unwrap();

    let marker_dir = dir.join(".hoodie/.temp").join(ts);
    std::fs::create_dir_all(&marker_dir).unwrap();
    std::fs::write(marker_dir.join("MARKERS.type"), b"TIMELINE_SERVER_BASED").unwrap();
    std::fs::write(
        marker_dir.join("MARKERS0"),
        format!("city=sf/{file_name}.marker.CREATE\n"),
    )
    .unwrap();

    if with_mdt_orphan {
        let mdt_timeline = dir.join(".hoodie/metadata/.hoodie/timeline");
        std::fs::write(
            mdt_timeline.join(format!("{ts}.deltacommit.requested")),
            b"",
        )
        .unwrap();
        std::fs::write(mdt_timeline.join(format!("{ts}.deltacommit.inflight")), b"").unwrap();
        // Completion time content is irrelevant for rollback (name-matched).
        std::fs::write(
            mdt_timeline.join(format!("{ts}_{ts}.deltacommit")),
            b"orphan",
        )
        .unwrap();
        std::fs::write(
            dir.join(".hoodie/metadata/files")
                .join(format!(".files-0000-0_{ts}.log.1_0-0-0")),
            b"orphan log block",
        )
        .unwrap();
    }
    file_name
}

#[tokio::test]
async fn test_marker_dir_removed_after_successful_commit() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path()).await;
    let result = table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();

    let marker_dir = dir.path().join(".hoodie/.temp").join(&result.instant);
    let leftovers = std::fs::read_dir(&marker_dir)
        .map(|entries| entries.count())
        .unwrap_or(0);
    assert_eq!(leftovers, 0, "marker dir must be emptied after commit");
}

#[tokio::test]
async fn test_crashed_write_rolled_back_on_next_write() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path()).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();

    let crashed_ts = "30000101000000000";
    let partial_file = fabricate_crashed_write(dir.path(), crashed_ts, true);

    // The next write eagerly rolls back the crashed instant.
    table.append([batch(vec![("b", "sf", 2)])]).await.unwrap();

    // Partial data file deleted; fencing files and marker dir gone.
    assert!(
        !dir.path().join("city=sf").join(&partial_file).exists(),
        "marker-listed partial file must be deleted"
    );
    let timeline = dir.path().join(".hoodie/timeline");
    assert!(
        !timeline
            .join(format!("{crashed_ts}.commit.requested"))
            .exists()
    );
    assert!(!timeline.join(format!("{crashed_ts}.inflight")).exists());
    let marker_dir = dir.path().join(".hoodie/.temp").join(crashed_ts);
    let leftovers = std::fs::read_dir(&marker_dir)
        .map(|entries| entries.count())
        .unwrap_or(0);
    assert_eq!(leftovers, 0, "crashed write's marker dir must be removed");

    // Orphan MDT deltacommit + its instant-named log file removed.
    let mdt_timeline = dir.path().join(".hoodie/metadata/.hoodie/timeline");
    assert!(
        !mdt_timeline
            .join(format!("{crashed_ts}_{crashed_ts}.deltacommit"))
            .exists(),
        "orphan MDT deltacommit must be rolled back"
    );
    assert!(
        !dir.path()
            .join(".hoodie/metadata/files")
            .join(format!(".files-0000-0_{crashed_ts}.log.1_0-0-0"))
            .exists(),
        "orphan MDT log file must be deleted"
    );

    // A completed rollback instant records the operation.
    let rollback_file = std::fs::read_dir(&timeline)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .find(|n| n.ends_with(".rollback"))
        .expect("completed rollback instant");
    let metadata = HoodieRollbackMetadata::from_avro_bytes(
        &std::fs::read(timeline.join(&rollback_file)).unwrap(),
    )
    .unwrap();
    assert_eq!(metadata.commits_rollback, vec![crashed_ts.to_string()]);
    assert_eq!(metadata.total_files_deleted, 1);
    let sf = metadata
        .partition_metadata
        .get("city=sf")
        .expect("partition metadata for city=sf");
    assert!(
        sf.success_delete_files
            .iter()
            .any(|f| f.ends_with(&partial_file)),
        "rollback metadata must record the deleted file: {sf:?}"
    );
    // {requested}_{completion}.rollback with a later completion time.
    let stem = rollback_file.trim_end_matches(".rollback");
    let (requested, completion) = stem.split_once('_').unwrap();
    assert!(completion > requested);

    // Table reads still work and reflect only committed data.
    let rows = table
        .read(&hudi_core::table::ReadOptions::new())
        .await
        .unwrap();
    let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 2, "a and b remain after rollback");
}

#[tokio::test]
async fn test_rollback_without_markers_is_clean_noop() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path()).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();

    // Crash before markers: only fencing files exist.
    let crashed_ts = "30000101000000001";
    let timeline = dir.path().join(".hoodie/timeline");
    std::fs::write(timeline.join(format!("{crashed_ts}.commit.requested")), b"").unwrap();
    std::fs::write(timeline.join(format!("{crashed_ts}.inflight")), b"").unwrap();

    table.append([batch(vec![("b", "sf", 2)])]).await.unwrap();

    assert!(
        !timeline
            .join(format!("{crashed_ts}.commit.requested"))
            .exists(),
        "fencing files cleaned even when no markers were written"
    );
    let rollbacks = std::fs::read_dir(&timeline)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with(".rollback"))
        .count();
    assert_eq!(rollbacks, 1, "rollback instant still recorded");
}
