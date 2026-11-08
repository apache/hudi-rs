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
use hudi_core::config::table::TableTypeValue;
use hudi_core::error::CoreError;
use hudi_core::metadata::rollback::{
    HoodieInstantInfo, HoodieRollbackMetadata, HoodieRollbackPlan, HoodieRollbackRequest,
};
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

    let marker_dir = dir
        .path()
        .join(".hoodie/.temp")
        .join(result.requested_time.as_deref().unwrap());
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

/// Makes `dir` read-only so unlinking files inside it fails, returning false
/// when the platform still allows the delete (e.g. running as root).
#[cfg(unix)]
fn deny_deletes_in(dir: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    let probe = dir.join("delete-probe");
    std::fs::write(&probe, b"").unwrap();
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    if std::fs::remove_file(&probe).is_ok() {
        allow_deletes_in(dir);
        return false;
    }
    true
}

#[cfg(unix)]
fn allow_deletes_in(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    let _ = std::fs::remove_file(dir.join("delete-probe"));
}

fn completed_rollbacks(timeline: &Path) -> Vec<String> {
    std::fs::read_dir(timeline)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with(".rollback"))
        .collect()
}

/// A data file the rollback cannot delete must fail the rollback: completing
/// it removes the failed instant's fencing files, after which archival would
/// make the surviving file read as committed. The rollback is retried by the
/// next write once the delete succeeds.
#[cfg(unix)]
#[tokio::test]
async fn test_rollback_fails_when_data_file_delete_fails() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path()).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();

    let crashed_ts = "30000101000000002";
    let partial_file = fabricate_crashed_write(dir.path(), crashed_ts, false);
    let partition = dir.path().join("city=sf");
    if !deny_deletes_in(&partition) {
        eprintln!("skipping: directory permissions do not block deletes here");
        return;
    }

    let result = table.append([batch(vec![("b", "la", 2)])]).await;
    allow_deletes_in(&partition);
    assert!(
        result.is_err(),
        "rollback must fail when a marker-listed file cannot be deleted"
    );
    assert!(partition.join(&partial_file).exists());
    let timeline = dir.path().join(".hoodie/timeline");
    assert!(
        timeline.join(format!("{crashed_ts}.inflight")).exists()
            && timeline
                .join(format!("{crashed_ts}.commit.requested"))
                .exists(),
        "the failed instant must stay pending so the rollback is retried"
    );
    assert!(
        completed_rollbacks(&timeline).is_empty(),
        "no rollback may complete while the file survives"
    );
    let pending = pending_rollbacks(&timeline);
    assert_eq!(
        pending.len(),
        1,
        "the persisted plan stays pending: {pending:?}"
    );

    // Once the delete can succeed, the next write finishes the same rollback.
    table.append([batch(vec![("b", "la", 2)])]).await.unwrap();
    assert!(!partition.join(&partial_file).exists());
    assert!(!timeline.join(format!("{crashed_ts}.inflight")).exists());
    let completed = completed_rollbacks(&timeline);
    assert_eq!(completed.len(), 1);
    assert!(completed[0].starts_with(&format!("{}_", pending[0])));
    assert_eq!(pending_rollbacks(&timeline), Vec::<String>::new());
    let rows = table
        .read(&hudi_core::table::ReadOptions::new())
        .await
        .unwrap();
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
}

/// The same holds for the orphan MDT deltacommit's log files: a surviving
/// orphan becomes trusted once the failed data instant is no longer pending.
#[cfg(unix)]
#[tokio::test]
async fn test_rollback_fails_when_orphan_mdt_delete_fails() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path()).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();

    let crashed_ts = "30000101000000003";
    fabricate_crashed_write(dir.path(), crashed_ts, true);
    let mdt_files = dir.path().join(".hoodie/metadata/files");
    if !deny_deletes_in(&mdt_files) {
        eprintln!("skipping: directory permissions do not block deletes here");
        return;
    }

    let result = table.append([batch(vec![("b", "la", 2)])]).await;
    allow_deletes_in(&mdt_files);
    assert!(
        result.is_err(),
        "rollback must fail when the orphan MDT log file cannot be deleted"
    );
    let timeline = dir.path().join(".hoodie/timeline");
    assert!(timeline.join(format!("{crashed_ts}.inflight")).exists());
    assert!(completed_rollbacks(&timeline).is_empty());

    table.append([batch(vec![("b", "la", 2)])]).await.unwrap();
    let mdt_timeline = dir.path().join(".hoodie/metadata/.hoodie/timeline");
    assert!(
        !mdt_timeline
            .join(format!("{crashed_ts}_{crashed_ts}.deltacommit"))
            .exists()
    );
    assert!(
        !mdt_files
            .join(format!(".files-0000-0_{crashed_ts}.log.1_0-0-0"))
            .exists()
    );
    assert!(!timeline.join(format!("{crashed_ts}.inflight")).exists());
    let rows = table
        .read(&hudi_core::table::ReadOptions::new())
        .await
        .unwrap();
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
}

/// An MDT partition the rollback cannot list may hold the failed instant's
/// log blocks; completing the rollback would delete the orphan deltacommit
/// and leave those blocks to read as valid.
#[cfg(unix)]
#[tokio::test]
async fn test_rollback_fails_when_mdt_partition_listing_fails() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path()).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();

    let crashed_ts = "30000101000000018";
    fabricate_crashed_write(dir.path(), crashed_ts, true);
    let mdt_files = dir.path().join(".hoodie/metadata/files");
    std::fs::set_permissions(&mdt_files, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read_dir(&mdt_files).is_ok() {
        std::fs::set_permissions(&mdt_files, std::fs::Permissions::from_mode(0o755)).unwrap();
        eprintln!("skipping: directory permissions do not block listing here");
        return;
    }

    let result = table.append([batch(vec![("b", "la", 2)])]).await;
    std::fs::set_permissions(&mdt_files, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(result.is_err());
    let timeline = dir.path().join(".hoodie/timeline");
    assert!(completed_rollbacks(&timeline).is_empty());
    assert!(
        dir.path()
            .join(".hoodie/metadata/.hoodie/timeline")
            .join(format!("{crashed_ts}_{crashed_ts}.deltacommit"))
            .exists(),
        "the orphan MDT deltacommit must stay until its log blocks are deleted"
    );

    table.append([batch(vec![("b", "la", 2)])]).await.unwrap();
    assert!(
        !mdt_files
            .join(format!(".files-0000-0_{crashed_ts}.log.1_0-0-0"))
            .exists()
    );
    assert_eq!(completed_rollbacks(&timeline).len(), 1);
}

fn timeline_names(timeline: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(timeline)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    names.sort();
    names
}

/// Rollback instants with a plan but no completed file.
fn pending_rollbacks(timeline: &Path) -> Vec<String> {
    let names = timeline_names(timeline);
    names
        .iter()
        .filter_map(|n| n.strip_suffix(".rollback.requested"))
        .filter(|ts| {
            !names
                .iter()
                .any(|n| n.starts_with(&format!("{ts}_")) && n.ends_with(".rollback"))
        })
        .map(str::to_string)
        .collect()
}

fn absolute(table: &Table, relative: &str) -> String {
    format!(
        "{}/{relative}",
        table.base_url().as_str().trim_end_matches('/')
    )
}

/// The plan follows Java's table-version-8 marker-based shape, which Spark
/// re-executes when it finds the rollback pending: one request per marker,
/// absolute paths in `filesToBeDeleted` (log files included), an empty
/// `logBlocksToBeDeleted`, and the file id and failed instant filled in.
#[tokio::test]
async fn test_rollback_plan_matches_java_v8_marker_based_shape() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_table_type(TableTypeValue::MergeOnRead)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .create()
        .await
        .unwrap();
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();

    let crashed_ts = "30000101000000004";
    let timeline = dir.path().join(".hoodie/timeline");
    std::fs::write(
        timeline.join(format!("{crashed_ts}.deltacommit.requested")),
        b"",
    )
    .unwrap();
    std::fs::write(
        timeline.join(format!("{crashed_ts}.deltacommit.inflight")),
        b"",
    )
    .unwrap();
    let log_id = "11111111-2222-3333-4444-555555555555-0";
    let base_id = "66666666-7777-8888-9999-000000000000-0";
    let log_file = format!("city=sf/.{log_id}_{crashed_ts}.log.1_0-0-0");
    let base_file = format!("city=la/{base_id}_0-0-0_{crashed_ts}.parquet");
    for file in [&log_file, &base_file] {
        let path = dir.path().join(file);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"partial").unwrap();
    }
    let marker_dir = dir.path().join(".hoodie/.temp").join(crashed_ts);
    std::fs::create_dir_all(&marker_dir).unwrap();
    std::fs::write(marker_dir.join("MARKERS.type"), b"TIMELINE_SERVER_BASED").unwrap();
    std::fs::write(
        marker_dir.join("MARKERS0"),
        format!("{log_file}.marker.CREATE\n{base_file}.marker.CREATE\n"),
    )
    .unwrap();

    table.append([batch(vec![("b", "sf", 2)])]).await.unwrap();

    assert!(!dir.path().join(&log_file).exists());
    assert!(!dir.path().join(&base_file).exists());
    let requested = timeline_names(&timeline)
        .into_iter()
        .find(|n| n.ends_with(".rollback.requested"))
        .expect("rollback plan");
    let plan =
        HoodieRollbackPlan::from_avro_bytes(&std::fs::read(timeline.join(&requested)).unwrap())
            .unwrap();
    assert_eq!(plan.version, Some(1));
    let target = plan.instant_to_rollback.as_ref().unwrap();
    assert_eq!(target.commit_time, crashed_ts);
    assert_eq!(target.action, "deltacommit");
    let mut requests = plan.rollback_requests.clone().unwrap();
    requests.sort_by(|a, b| a.partition_path.cmp(&b.partition_path));
    assert_eq!(requests.len(), 2, "one request per marker: {requests:?}");
    for (request, (partition, file_id, file)) in requests.iter().zip([
        ("city=la", base_id, &base_file),
        ("city=sf", log_id, &log_file),
    ]) {
        assert_eq!(request.partition_path, partition);
        assert_eq!(request.file_id.as_deref(), Some(file_id));
        assert_eq!(request.latest_base_instant.as_deref(), Some(crashed_ts));
        assert_eq!(request.files_to_be_deleted, vec![absolute(&table, file)]);
        assert_eq!(
            request.log_blocks_to_be_deleted,
            Some(Default::default()),
            "v8 rollback deletes log files; logBlocksToBeDeleted must be an empty map"
        );
    }

    let completed = timeline_names(&timeline)
        .into_iter()
        .find(|n| n.ends_with(".rollback"))
        .unwrap();
    let metadata =
        HoodieRollbackMetadata::from_avro_bytes(&std::fs::read(timeline.join(completed)).unwrap())
            .unwrap();
    assert_eq!(metadata.total_files_deleted, 2);
    assert_eq!(
        metadata.partition_metadata["city=sf"].success_delete_files,
        vec![absolute(&table, &log_file)]
    );
}

/// Fabricates the state a crash leaves after the rollback plan of a failed
/// write is persisted: the failed write (unless `target_unfenced`) plus
/// `{rollback_ts}.rollback.requested|inflight` carrying a v8 plan.
fn fabricate_pending_rollback(
    table: &Table,
    dir: &Path,
    crashed_ts: &str,
    rollback_ts: &str,
    target_unfenced: bool,
) -> String {
    let partial = fabricate_crashed_write(dir, crashed_ts, false);
    let timeline = dir.join(".hoodie/timeline");
    if target_unfenced {
        std::fs::remove_file(timeline.join(format!("{crashed_ts}.commit.requested"))).unwrap();
        std::fs::remove_file(timeline.join(format!("{crashed_ts}.inflight"))).unwrap();
    }
    let relative = format!("city=sf/{partial}");
    let plan = HoodieRollbackPlan {
        instant_to_rollback: Some(HoodieInstantInfo {
            commit_time: crashed_ts.to_string(),
            action: "commit".to_string(),
        }),
        rollback_requests: Some(vec![HoodieRollbackRequest {
            partition_path: "city=sf".to_string(),
            file_id: Some(partial.split('_').next().unwrap().to_string()),
            latest_base_instant: Some(crashed_ts.to_string()),
            files_to_be_deleted: vec![absolute(table, &relative)],
            log_blocks_to_be_deleted: Some(Default::default()),
        }]),
        version: Some(1),
    };
    std::fs::write(
        timeline.join(format!("{rollback_ts}.rollback.requested")),
        plan.to_avro_bytes().unwrap(),
    )
    .unwrap();
    std::fs::write(
        timeline.join(format!("{rollback_ts}.rollback.inflight")),
        b"",
    )
    .unwrap();
    relative
}

/// Java writes plan paths unescaped (`StoragePath.toString`), so a partition
/// with spaces or non-ASCII characters appears raw in `filesToBeDeleted`.
#[tokio::test]
async fn test_pending_rollback_deletes_java_plan_paths_with_special_characters() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path()).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();

    let crashed_ts = "30000101000000013";
    let rollback_ts = "30000101000000014";
    let timeline = dir.path().join(".hoodie/timeline");
    std::fs::write(timeline.join(format!("{crashed_ts}.commit.requested")), b"").unwrap();
    std::fs::write(timeline.join(format!("{crashed_ts}.inflight")), b"").unwrap();
    let partition = "city=san jos\u{e9}";
    let file_name = format!("11111111-2222-3333-4444-555555555555-0_0-0-0_{crashed_ts}.parquet");
    std::fs::create_dir_all(dir.path().join(partition)).unwrap();
    let partial = dir.path().join(partition).join(&file_name);
    std::fs::write(&partial, b"partial parquet bytes").unwrap();
    let plan = HoodieRollbackPlan {
        instant_to_rollback: Some(HoodieInstantInfo {
            commit_time: crashed_ts.to_string(),
            action: "commit".to_string(),
        }),
        rollback_requests: Some(vec![HoodieRollbackRequest {
            partition_path: partition.to_string(),
            file_id: Some(file_name.split('_').next().unwrap().to_string()),
            latest_base_instant: Some(crashed_ts.to_string()),
            files_to_be_deleted: vec![format!(
                "file:{}/{partition}/{file_name}",
                dir.path().to_str().unwrap()
            )],
            log_blocks_to_be_deleted: Some(Default::default()),
        }]),
        version: Some(1),
    };
    std::fs::write(
        timeline.join(format!("{rollback_ts}.rollback.requested")),
        plan.to_avro_bytes().unwrap(),
    )
    .unwrap();
    std::fs::write(
        timeline.join(format!("{rollback_ts}.rollback.inflight")),
        b"",
    )
    .unwrap();

    table.append([batch(vec![("b", "sf", 2)])]).await.unwrap();

    assert_eq!(pending_rollbacks(&timeline), Vec::<String>::new());
    assert!(!partial.exists(), "the planned file must be deleted");
}

/// A rollback left pending by a crash is completed by the next write on its
/// own instant, as Java resumes it, instead of a second rollback being
/// minted while the first stays pending forever.
#[tokio::test]
async fn test_pending_rollback_resumed_by_next_write() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path()).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();

    let crashed_ts = "30000101000000005";
    let rollback_ts = "30000101000000006";
    let partial = fabricate_pending_rollback(&table, dir.path(), crashed_ts, rollback_ts, false);

    table.append([batch(vec![("b", "sf", 2)])]).await.unwrap();

    let timeline = dir.path().join(".hoodie/timeline");
    assert_eq!(
        pending_rollbacks(&timeline),
        Vec::<String>::new(),
        "no rollback may stay pending"
    );
    let completed = completed_rollbacks(&timeline);
    assert_eq!(completed.len(), 1, "{completed:?}");
    assert!(
        completed[0].starts_with(&format!("{rollback_ts}_")),
        "the pending rollback instant itself must complete: {completed:?}"
    );
    assert!(!dir.path().join(&partial).exists());
    assert!(!timeline.join(format!("{crashed_ts}.inflight")).exists());
    assert!(
        !timeline
            .join(format!("{crashed_ts}.commit.requested"))
            .exists()
    );
    let rows = table
        .read(&hudi_core::table::ReadOptions::new())
        .await
        .unwrap();
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
}

/// The failed instant may already be gone when the rollback is resumed (Java
/// deletes it before completing the rollback); the plan alone drives it.
#[tokio::test]
async fn test_pending_rollback_resumed_after_target_unfenced() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path()).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();

    let crashed_ts = "30000101000000007";
    let rollback_ts = "30000101000000008";
    let partial = fabricate_pending_rollback(&table, dir.path(), crashed_ts, rollback_ts, true);

    table.append([batch(vec![("b", "sf", 2)])]).await.unwrap();

    let timeline = dir.path().join(".hoodie/timeline");
    assert_eq!(pending_rollbacks(&timeline), Vec::<String>::new());
    let completed = completed_rollbacks(&timeline);
    assert_eq!(completed.len(), 1, "{completed:?}");
    assert!(completed[0].starts_with(&format!("{rollback_ts}_")));
    assert!(!dir.path().join(&partial).exists());
}

/// An unreadable plan on a rollback that never reached inflight is discarded
/// (Java `getPendingRollbackInfos`), and the failed write gets a new rollback.
#[tokio::test]
async fn test_unreadable_requested_rollback_plan_is_discarded() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path()).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();

    let crashed_ts = "30000101000000009";
    let partial = fabricate_crashed_write(dir.path(), crashed_ts, false);
    let timeline = dir.path().join(".hoodie/timeline");
    let rollback_ts = "30000101000000010";
    std::fs::write(
        timeline.join(format!("{rollback_ts}.rollback.requested")),
        b"not avro",
    )
    .unwrap();

    table.append([batch(vec![("b", "sf", 2)])]).await.unwrap();

    assert!(
        !timeline
            .join(format!("{rollback_ts}.rollback.requested"))
            .exists()
    );
    assert_eq!(pending_rollbacks(&timeline), Vec::<String>::new());
    assert_eq!(completed_rollbacks(&timeline).len(), 1);
    assert!(!dir.path().join("city=sf").join(&partial).exists());
}

/// An inflight rollback whose plan cannot be read is skipped, as Java does,
/// rather than failing every write; the failed write gets a new rollback.
#[tokio::test]
async fn test_inflight_rollback_without_readable_plan_is_skipped() {
    for plan in [None, Some(b"not avro".as_slice())] {
        let dir = tempdir().unwrap();
        let mut table = create_partitioned(dir.path()).await;
        table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();

        let crashed_ts = "30000101000000015";
        let partial = fabricate_crashed_write(dir.path(), crashed_ts, false);
        let timeline = dir.path().join(".hoodie/timeline");
        let rollback_ts = "30000101000000016";
        if let Some(plan) = plan {
            std::fs::write(
                timeline.join(format!("{rollback_ts}.rollback.requested")),
                plan,
            )
            .unwrap();
        }
        std::fs::write(
            timeline.join(format!("{rollback_ts}.rollback.inflight")),
            b"",
        )
        .unwrap();

        table.append([batch(vec![("b", "sf", 2)])]).await.unwrap();

        assert!(
            timeline
                .join(format!("{rollback_ts}.rollback.inflight"))
                .exists()
        );
        assert_eq!(completed_rollbacks(&timeline).len(), 1, "{plan:?}");
        assert!(!dir.path().join("city=sf").join(&partial).exists());
    }
}

/// Java reverts a completed target to inflight before deleting its files,
/// which this writer does not do; resuming such a rollback here would delete
/// committed files under a commit that stays published.
#[tokio::test]
async fn test_pending_rollback_of_completed_instant_is_refused() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path()).await;
    let committed = table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();
    let data_file = std::fs::read_dir(dir.path().join("city=sf"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .find(|name| name.ends_with(".parquet"))
        .unwrap();
    let relative = format!("city=sf/{data_file}");

    let timeline = dir.path().join(".hoodie/timeline");
    let rollback_ts = "30000101000000017";
    let plan = HoodieRollbackPlan {
        instant_to_rollback: Some(HoodieInstantInfo {
            commit_time: committed.requested_time.clone().unwrap(),
            action: "commit".to_string(),
        }),
        rollback_requests: Some(vec![HoodieRollbackRequest {
            partition_path: "city=sf".to_string(),
            file_id: Some(data_file.split('_').next().unwrap().to_string()),
            latest_base_instant: Some(committed.requested_time.clone().unwrap()),
            files_to_be_deleted: vec![absolute(&table, &relative)],
            log_blocks_to_be_deleted: Some(Default::default()),
        }]),
        version: Some(1),
    };
    std::fs::write(
        timeline.join(format!("{rollback_ts}.rollback.requested")),
        plan.to_avro_bytes().unwrap(),
    )
    .unwrap();

    let result = table.append([batch(vec![("b", "sf", 2)])]).await;
    assert!(
        dir.path().join(&relative).exists(),
        "committed data file must survive"
    );
    let err = result.unwrap_err();
    assert!(
        matches!(&err, CoreError::Unsupported(msg)
            if msg.contains(rollback_ts) && msg.contains(committed.requested_time.as_deref().unwrap())),
        "{err}"
    );
    assert_eq!(pending_rollbacks(&timeline), vec![rollback_ts.to_string()]);
}

/// A pending rollback of a compaction belongs to the table service that owns
/// the compaction; the writer leaves it alone.
#[tokio::test]
async fn test_pending_rollback_of_compaction_is_not_resumed() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path()).await;
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();

    let timeline = dir.path().join(".hoodie/timeline");
    let rollback_ts = "30000101000000011";
    let plan = HoodieRollbackPlan {
        instant_to_rollback: Some(HoodieInstantInfo {
            commit_time: "30000101000000012".to_string(),
            action: "compaction".to_string(),
        }),
        rollback_requests: Some(Vec::new()),
        version: Some(1),
    };
    std::fs::write(
        timeline.join(format!("{rollback_ts}.rollback.requested")),
        plan.to_avro_bytes().unwrap(),
    )
    .unwrap();

    table.append([batch(vec![("b", "sf", 2)])]).await.unwrap();

    assert_eq!(pending_rollbacks(&timeline), vec![rollback_ts.to_string()]);
    assert!(completed_rollbacks(&timeline).is_empty());
}
