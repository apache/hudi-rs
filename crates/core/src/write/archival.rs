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
//! Timeline archival into the LSM history timeline (Java `TimelineArchiverV2`
//! + `LSMTimelineWriter`).
//!
//! When the active timeline holds more than `hoodie.keep.max.commits`
//! completed commits, the oldest are archived down to `hoodie.keep.min.commits`:
//! written as one level-0 parquet of `HoodieLSMTimelineInstant` rows under
//! `{timeline}/history/`, registered in a new `manifest_{N}` (JSON) pointed at
//! by `_version_`, then removed from the active timeline (pending fencing
//! files first, completed files after — Java's ordering). Runs inside the
//! completion critical section (user design; Java runs it post-commit).
//!
//! LSM level compaction (merging L0 files into L1+) is not yet implemented —
//! readers are unaffected since every file stays manifest-listed.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::Result;
use crate::error::CoreError;
use crate::storage::Storage;
use crate::write::rollback::action_from_fencing_name;

/// Java `hoodie.keep.min.commits` default.
pub(crate) const DEFAULT_KEEP_MIN_COMMITS: usize = 20;
/// Java `hoodie.keep.max.commits` default.
pub(crate) const DEFAULT_KEEP_MAX_COMMITS: usize = 30;
/// Java `hoodie.timeline.manifest.retained.versions` default.
const RETAINED_MANIFEST_VERSIONS: i64 = 3;

/// Java `HoodieLSMTimelineManifest`: `{"files":[{"fileName":..,"fileLen":..}]}`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LsmManifest {
    files: Vec<LsmFileEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LsmFileEntry {
    file_name: String,
    file_len: i64,
}

#[derive(Debug, Clone)]
struct CompletedInstant {
    requested: String,
    completion: String,
    action: String,
    file_name: String,
}

/// Archive the timeline at `timeline_dir` if it exceeds `max_keep` completed
/// commits, retaining the newest `min_keep`.
pub(crate) async fn archive_timeline_if_needed(
    storage: &Storage,
    timeline_dir: &str,
    min_keep: usize,
    max_keep: usize,
) -> Result<()> {
    let listed = match storage.list_files(Some(timeline_dir)).await {
        Ok(files) => files,
        Err(crate::storage::error::StorageError::ObjectStoreError(
            object_store::Error::NotFound { .. },
        )) => return Ok(()),
        Err(error) => return Err(error.into()),
    };

    let mut completed: Vec<CompletedInstant> = Vec::new();
    let mut fencing_by_instant: HashMap<String, Vec<String>> = HashMap::new();
    let mut earliest_pending: Option<String> = None;
    for file in &listed {
        let name = &file.name;
        let Some(first) = name.chars().next() else {
            continue;
        };
        if !first.is_ascii_digit() {
            continue;
        }
        let requested: String = name.chars().take_while(char::is_ascii_digit).collect();
        if name.ends_with(".requested") || name.ends_with(".inflight") {
            fencing_by_instant
                .entry(requested.clone())
                .or_default()
                .push(name.clone());
            continue;
        }
        // Completed layout-v2 name: `{req}_{comp}.{action}`.
        let Some((stem, action)) = name.rsplit_once('.') else {
            continue;
        };
        let Some((req, comp)) = stem.split_once('_') else {
            continue;
        };
        completed.push(CompletedInstant {
            requested: req.to_string(),
            completion: comp.to_string(),
            action: action.to_string(),
            file_name: name.clone(),
        });
        let _ = requested;
    }
    // Pending = fenced without a completed file. Only write actions count
    // (Java's `getWriteTimeline`): a pending clean or rollback writes no data
    // files a reader could mistake for committed.
    let completed_set: std::collections::HashSet<&str> =
        completed.iter().map(|c| c.requested.as_str()).collect();
    for (ts, names) in &fencing_by_instant {
        let is_write_action = names.iter().any(|name| {
            matches!(
                action_from_fencing_name(name, ts).as_str(),
                "" | "commit"
                    | "deltacommit"
                    | "compaction"
                    | "logcompaction"
                    | "replacecommit"
                    | "clustering"
            )
        });
        if is_write_action
            && !completed_set.contains(ts.as_str())
            && earliest_pending.as_ref().is_none_or(|e| ts < e)
        {
            earliest_pending = Some(ts.clone());
        }
    }

    completed.sort_by(|a, b| a.requested.cmp(&b.requested));
    let is_commit_action =
        |action: &str| matches!(action, "commit" | "deltacommit" | "replacecommit");
    // Retain the last completed commit before the earliest pending instant
    // (Java `completedCommitBeforeEarliestPendingInstant`): the reader treats
    // everything below the earliest active commit as committed, and pending
    // compaction/clustering instants do not count as commits there, so
    // archiving up to the pending instant would make its partial output
    // readable.
    let retain_from_pending = earliest_pending.map(|pending| {
        completed
            .iter()
            .filter(|c| is_commit_action(&c.action) && c.requested < pending)
            .map(|c| c.requested.clone())
            .next_back()
            .unwrap_or(pending)
    });
    // A replacecommit's partitionToReplaceFileIds is what hides the file
    // groups it replaced; until a cleaner physically deletes those groups,
    // archiving it resurrects their rows. Java blocks archival at the first
    // uncleaned replace instant; with no cleaner implemented, that means
    // never archiving at or past the earliest active replacecommit.
    let earliest_replace = completed
        .iter()
        .filter(|c| c.action == "replacecommit")
        .map(|c| c.requested.clone())
        .min();
    // A savepoint pins its commit and everything after it: archiving past one
    // would discard the very files the savepoint exists to retain (Java's
    // TimelineArchiverV2 stops at the first savepoint unless
    // hoodie.archive.beyond.savepoint, default false).
    let earliest_savepoint = completed
        .iter()
        .filter(|c| c.action == "savepoint")
        .map(|c| c.requested.clone())
        .min();
    let retain_from = [retain_from_pending, earliest_replace, earliest_savepoint]
        .into_iter()
        .flatten()
        .min();
    let commit_count = completed
        .iter()
        .filter(|c| is_commit_action(&c.action))
        .count();
    // Misconfigured min > max would underflow below and archive the whole
    // timeline; treat min as the effective floor.
    let min_keep = min_keep.min(max_keep);
    if commit_count <= max_keep {
        return Ok(());
    }

    // Archive the oldest commits down to `min_keep`, but never at or past
    // `retain_from`; sweep along non-commit actions (rollbacks) up to the
    // newest archived commit.
    let mut commits_to_archive = commit_count - min_keep;
    let mut max_archived_commit: Option<String> = None;
    let mut to_archive: Vec<CompletedInstant> = Vec::new();
    for instant in &completed {
        if let Some(bound) = &retain_from
            && instant.requested.as_str() >= bound.as_str()
        {
            break;
        }
        if is_commit_action(&instant.action) {
            if commits_to_archive == 0 {
                break;
            }
            commits_to_archive -= 1;
            max_archived_commit = Some(instant.requested.clone());
            to_archive.push(instant.clone());
        } else if instant.action == "savepoint" {
            // A savepoint must never be swept along as an incidental
            // non-commit action; it also shares its commit's instant time, so
            // the fencing-file deletion below must not reach it.
            break;
        } else {
            to_archive.push(instant.clone());
        }
    }
    // Trim trailing non-commit instants newer than the last archived commit.
    if let Some(max_commit) = &max_archived_commit {
        to_archive.retain(|i| i.requested.as_str() <= max_commit.as_str());
    } else {
        return Ok(());
    }
    if to_archive.is_empty() {
        return Ok(());
    }

    // Build HoodieLSMTimelineInstant rows: metadata = completed file bytes;
    // plan = inflight (fallback requested) bytes for replacecommits.
    let mut instant_times = Vec::new();
    let mut completion_times = Vec::new();
    let mut actions = Vec::new();
    let mut metadata_bytes: Vec<Option<Vec<u8>>> = Vec::new();
    let mut plan_bytes: Vec<Option<Vec<u8>>> = Vec::new();
    for instant in &to_archive {
        let metadata = storage
            .get_file_data(&format!("{timeline_dir}/{}", instant.file_name))
            .await?;
        let plan = if instant.action == "replacecommit" {
            let mut plan = None;
            for fencing in fencing_by_instant
                .get(&instant.requested)
                .into_iter()
                .flatten()
            {
                if fencing.ends_with(".inflight")
                    && let Ok(bytes) = storage
                        .get_file_data(&format!("{timeline_dir}/{fencing}"))
                        .await
                    && !bytes.is_empty()
                {
                    plan = Some(bytes.to_vec());
                }
            }
            if plan.is_none() {
                for fencing in fencing_by_instant
                    .get(&instant.requested)
                    .into_iter()
                    .flatten()
                {
                    if fencing.ends_with(".requested")
                        && let Ok(bytes) = storage
                            .get_file_data(&format!("{timeline_dir}/{fencing}"))
                            .await
                        && !bytes.is_empty()
                    {
                        plan = Some(bytes.to_vec());
                    }
                }
            }
            plan
        } else {
            None
        };
        instant_times.push(instant.requested.clone());
        completion_times.push(instant.completion.clone());
        actions.push(instant.action.clone());
        metadata_bytes.push(Some(metadata.to_vec()));
        plan_bytes.push(plan);
    }

    let history_dir = format!("{timeline_dir}/history");
    let min_instant = &to_archive[0].requested;
    let max_instant = &to_archive[to_archive.len() - 1].requested;
    let parquet_name = format!("{min_instant}_{max_instant}_0.parquet");
    let (current_version, mut manifest) = read_manifest(storage, &history_dir).await?;
    // A retry after the active files failed to go finds its file already
    // published (Java LSMTimelineWriter skips it too); only the deletes remain.
    if !manifest.files.iter().any(|f| f.file_name == parquet_name) {
        publish_lsm_file(
            storage,
            &history_dir,
            current_version,
            &mut manifest,
            parquet_name,
            write_lsm_parquet(
                &instant_times,
                &completion_times,
                &actions,
                &metadata_bytes,
                &plan_bytes,
            )?,
        )
        .await?;
    }

    // Remove archived instants from the active timeline: fencing files first
    // (ascending), completed files after. A fencing file that survives stops
    // archival before any completed file goes, since an instant with fencing
    // files and no completed file reads as a failed write to roll back.
    for instant in &to_archive {
        for fencing in fencing_by_instant
            .get(&instant.requested)
            .into_iter()
            .flatten()
        {
            storage
                .delete_file(&format!("{timeline_dir}/{fencing}"))
                .await?;
        }
    }
    // Completed files go in ascending order and stop at the first failure
    // (Java TimelineArchiverV2), so the survivors are the newest instants and
    // a retry never archives a set that skips a published instant.
    for instant in &to_archive {
        storage
            .delete_file(&format!("{timeline_dir}/{}", instant.file_name))
            .await?;
    }
    Ok(())
}

/// Writes one level-0 file, then publishes it: manifest_{N+1}, then
/// `_version_` (Java order).
async fn publish_lsm_file(
    storage: &Storage,
    history_dir: &str,
    current_version: i64,
    manifest: &mut LsmManifest,
    parquet_name: String,
    parquet: Vec<u8>,
) -> Result<()> {
    let parquet_len = parquet.len() as i64;
    storage
        .put_file(&format!("{history_dir}/{parquet_name}"), parquet)
        .await?;
    manifest.files.push(LsmFileEntry {
        file_name: parquet_name,
        file_len: parquet_len,
    });
    let next_version = current_version + 1;
    let manifest_json = serde_json::to_vec(&manifest)
        .map_err(|e| CoreError::Timeline(format!("serialize LSM manifest: {e}")))?;
    storage
        .put_file(
            &format!("{history_dir}/manifest_{next_version}"),
            manifest_json,
        )
        .await?;
    storage
        .put_file(
            &format!("{history_dir}/_version_"),
            next_version.to_string().into_bytes(),
        )
        .await?;
    // Retain only the last N manifest versions.
    let stale_floor = next_version - RETAINED_MANIFEST_VERSIONS;
    let mut stale = stale_floor;
    while stale > 0 {
        let path = format!("{history_dir}/manifest_{stale}");
        if storage.delete_file(&path).await.is_err() {
            break;
        }
        stale -= 1;
    }
    Ok(())
}

async fn read_manifest(storage: &Storage, history_dir: &str) -> Result<(i64, LsmManifest)> {
    let version_path = format!("{history_dir}/_version_");
    match storage.get_file_data(&version_path).await {
        Ok(bytes) => {
            let version: i64 = String::from_utf8_lossy(&bytes)
                .trim()
                .parse()
                .map_err(|e| CoreError::Timeline(format!("invalid LSM _version_: {e}")))?;
            let manifest_bytes = storage
                .get_file_data(&format!("{history_dir}/manifest_{version}"))
                .await?;
            let manifest: LsmManifest = serde_json::from_slice(&manifest_bytes)
                .map_err(|e| CoreError::Timeline(format!("invalid LSM manifest: {e}")))?;
            Ok((version, manifest))
        }
        Err(crate::storage::error::StorageError::ObjectStoreError(
            object_store::Error::NotFound { .. },
        )) => Ok((0, LsmManifest::default())),
        Err(error) => Err(error.into()),
    }
}

/// Arrow shape of Java `HoodieLSMTimelineInstant.avsc`.
fn lsm_instant_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("instantTime", DataType::Utf8, true),
        Field::new("completionTime", DataType::Utf8, true),
        Field::new("action", DataType::Utf8, true),
        Field::new("metadata", DataType::Binary, true),
        Field::new("plan", DataType::Binary, true),
        Field::new("version", DataType::Int32, false),
    ]))
}

fn write_lsm_parquet(
    instant_times: &[String],
    completion_times: &[String],
    actions: &[String],
    metadata: &[Option<Vec<u8>>],
    plans: &[Option<Vec<u8>>],
) -> Result<Vec<u8>> {
    let schema = lsm_instant_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                instant_times.iter().map(String::as_str).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                completion_times
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                actions.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(BinaryArray::from(
                metadata
                    .iter()
                    .map(|m| m.as_deref())
                    .collect::<Vec<Option<&[u8]>>>(),
            )),
            Arc::new(BinaryArray::from(
                plans
                    .iter()
                    .map(|p| p.as_deref())
                    .collect::<Vec<Option<&[u8]>>>(),
            )),
            Arc::new(Int32Array::from(vec![1; instant_times.len()])),
        ],
    )?;
    let mut out = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut out, schema, None)
        .map_err(|e| CoreError::Timeline(format!("create LSM parquet writer: {e}")))?;
    writer
        .write(&batch)
        .map_err(|e| CoreError::Timeline(format!("write LSM parquet: {e}")))?;
    writer
        .close()
        .map_err(|e| CoreError::Timeline(format!("close LSM parquet: {e}")))?;
    Ok(out)
}

#[cfg(test)]
mod lifecycle_tests {
    use crate::table::Table;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(id: &str, v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![id])) as ArrayRef,
                Arc::new(Int64Array::from(vec![v])),
            ],
        )
        .unwrap()
    }

    /// A savepoint pins its commit: archival must not cross it, and must not
    /// sweep the `.savepoint` instant off the active timeline.
    #[tokio::test]
    async fn test_archival_stops_at_savepoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .with_option("hoodie.keep.min.commits", "2")
            .with_option("hoodie.keep.max.commits", "3")
            .create()
            .await
            .unwrap();
        for i in 0..3 {
            table.append([batch(&format!("k{i}"), i)]).await.unwrap();
        }
        let timeline = dir.path().join(".hoodie/timeline");
        // Savepoint the OLDEST commit, the one archival would take first.
        let mut completed: Vec<String> = std::fs::read_dir(&timeline)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".commit") || n.ends_with(".deltacommit"))
            .collect();
        completed.sort();
        let oldest = completed.first().expect("a completed commit").clone();
        let requested = oldest.split('_').next().unwrap().to_string();
        std::fs::write(
            timeline.join(format!("{requested}_{requested}.savepoint")),
            b"{}",
        )
        .unwrap();

        for i in 3..10 {
            table.append([batch(&format!("k{i}"), i)]).await.unwrap();
        }

        let names: Vec<String> = std::fs::read_dir(&timeline)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(
            names.iter().any(|n| n.ends_with(".savepoint")),
            "the savepoint instant must stay on the active timeline: {names:?}"
        );
        assert!(
            names.iter().any(|n| n.starts_with(&format!("{requested}_"))
                && (n.ends_with(".commit") || n.ends_with(".deltacommit"))),
            "the savepointed commit must not be archived: {names:?}"
        );
    }

    /// Drives repeated archival from inside the crate: manifest rotation,
    /// retention, and reads across the boundary on the writing handle.
    #[tokio::test]
    async fn test_archival_rotation_in_crate() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .with_option("hoodie.keep.min.commits", "4")
            .with_option("hoodie.keep.max.commits", "6")
            .create()
            .await
            .unwrap();
        for i in 0..12 {
            table.append([batch(&format!("k{i}"), i)]).await.unwrap();
        }
        let history = dir.path().join(".hoodie/timeline/history");
        let names: Vec<String> = std::fs::read_dir(&history)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(names.iter().filter(|n| n.ends_with(".parquet")).count() >= 2);
        assert!(names.iter().filter(|n| n.starts_with("manifest_")).count() <= 3);
        let rows: usize = table
            .read(&crate::table::ReadOptions::new())
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 12);
    }
}

#[cfg(test)]
mod delete_failure_tests {
    use super::archive_timeline_if_needed;
    use crate::config::HudiConfigs;
    use crate::storage::Storage;
    use crate::storage::fault::DeleteFails;
    use object_store::local::LocalFileSystem;
    use std::sync::Arc;

    /// A fencing file archival cannot delete must stop it before any
    /// completed file goes: an archived instant left with fencing files and
    /// no completed file reads as a failed write, which the next write's
    /// eager rollback would undo.
    #[tokio::test]
    async fn test_archival_keeps_completed_files_when_fencing_delete_fails() {
        let dir = tempfile::tempdir().unwrap();
        let timeline = dir.path().join(".hoodie/timeline");
        std::fs::create_dir_all(&timeline).unwrap();
        let instants: Vec<String> = (1..=4).map(|i| format!("2026010100000000{i}")).collect();
        for ts in &instants {
            std::fs::write(timeline.join(format!("{ts}.commit.requested")), b"").unwrap();
            std::fs::write(timeline.join(format!("{ts}.inflight")), b"").unwrap();
            std::fs::write(timeline.join(format!("{ts}_{ts}.commit")), b"{}").unwrap();
        }
        let base_url = url::Url::from_directory_path(dir.path()).unwrap();
        let store = Arc::new(DeleteFails(Arc::new(LocalFileSystem::new()), |path| {
            path.ends_with(".inflight")
        }));
        let storage =
            Storage::new_with_object_store(base_url, store, Arc::new(HudiConfigs::empty()));

        let result = archive_timeline_if_needed(storage.as_ref(), ".hoodie/timeline", 1, 2).await;

        for ts in &instants {
            assert!(
                !timeline.join(format!("{ts}.inflight")).exists()
                    || timeline.join(format!("{ts}_{ts}.commit")).exists(),
                "{ts} is left looking pending"
            );
        }
        assert!(result.is_err());
    }

    fn archived_instants(timeline: &std::path::Path) -> Vec<String> {
        use arrow_array::{Array, StringArray};
        let history = timeline.join("history");
        let version = std::fs::read_to_string(history.join("_version_")).unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(
            &std::fs::read(history.join(format!("manifest_{}", version.trim()))).unwrap(),
        )
        .unwrap();
        let mut instants = Vec::new();
        for entry in manifest["files"].as_array().unwrap() {
            let bytes = std::fs::read(history.join(entry["fileName"].as_str().unwrap())).unwrap();
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                bytes::Bytes::from(bytes),
            )
            .unwrap()
            .build()
            .unwrap();
            for batch in reader {
                let column = batch.unwrap().column(0).clone();
                let column = column.as_any().downcast_ref::<StringArray>().unwrap();
                instants.extend((0..column.len()).map(|i| column.value(i).to_string()));
            }
        }
        instants.sort();
        instants
    }

    /// Completed files are deleted in instant order and archival stops at the
    /// first failure, as in Java, and a retry does not rewrite a level-0 file
    /// the manifest already lists. Otherwise a retry that archives only the
    /// survivors can replace a published file and lose the instants deleted
    /// in between.
    #[tokio::test]
    async fn test_archival_retry_after_completed_delete_failure_keeps_history() {
        let dir = tempfile::tempdir().unwrap();
        let timeline = dir.path().join(".hoodie/timeline");
        std::fs::create_dir_all(&timeline).unwrap();
        let instants: Vec<String> = (1..=4).map(|i| format!("2026010100000000{i}")).collect();
        for ts in &instants {
            std::fs::write(timeline.join(format!("{ts}.commit.requested")), b"").unwrap();
            std::fs::write(timeline.join(format!("{ts}.inflight")), b"").unwrap();
            std::fs::write(timeline.join(format!("{ts}_{ts}.commit")), b"{}").unwrap();
        }
        let base_url = url::Url::from_directory_path(dir.path()).unwrap();
        let failing = Arc::new(DeleteFails(Arc::new(LocalFileSystem::new()), |path| {
            path.ends_with("20260101000000001_20260101000000001.commit")
                || path.ends_with("20260101000000003_20260101000000003.commit")
        }));
        let storage = Storage::new_with_object_store(
            base_url.clone(),
            failing,
            Arc::new(HudiConfigs::empty()),
        );
        let _ = archive_timeline_if_needed(storage.as_ref(), ".hoodie/timeline", 1, 2).await;

        let storage = Storage::new_with_object_store(
            base_url,
            Arc::new(LocalFileSystem::new()),
            Arc::new(HudiConfigs::empty()),
        );
        archive_timeline_if_needed(storage.as_ref(), ".hoodie/timeline", 1, 2)
            .await
            .unwrap();

        assert_eq!(archived_instants(&timeline), instants[..3].to_vec());
        for ts in &instants[..3] {
            assert!(!timeline.join(format!("{ts}_{ts}.commit")).exists(), "{ts}");
        }
        assert!(
            timeline
                .join(format!("{0}_{0}.commit", instants[3]))
                .exists()
        );
    }

    /// A `_version_` write that lands and then errors leaves the active files
    /// in place; the retry must not publish the same file a second time.
    #[tokio::test]
    async fn test_archival_retry_after_version_put_error_publishes_once() {
        use crate::storage::fault::{FaultyPutStore, PutFault};
        let dir = tempfile::tempdir().unwrap();
        let timeline = dir.path().join(".hoodie/timeline");
        std::fs::create_dir_all(&timeline).unwrap();
        let instants: Vec<String> = (1..=4).map(|i| format!("2026010100000000{i}")).collect();
        for ts in &instants {
            std::fs::write(timeline.join(format!("{ts}_{ts}.commit")), b"{}").unwrap();
        }
        let base_url = url::Url::from_directory_path(dir.path()).unwrap();
        let faulty = FaultyPutStore::new_for_any_put(
            Arc::new(LocalFileSystem::new()),
            PutFault::LandedThenError,
            |path| path.as_ref().ends_with("history/_version_"),
        );
        let storage = Storage::new_with_object_store(
            base_url.clone(),
            faulty.clone(),
            Arc::new(HudiConfigs::empty()),
        );
        let result = archive_timeline_if_needed(storage.as_ref(), ".hoodie/timeline", 1, 2).await;
        assert!(result.is_err() && !faulty.is_armed());

        let storage = Storage::new_with_object_store(
            base_url,
            Arc::new(LocalFileSystem::new()),
            Arc::new(HudiConfigs::empty()),
        );
        archive_timeline_if_needed(storage.as_ref(), ".hoodie/timeline", 1, 2)
            .await
            .unwrap();
        assert_eq!(archived_instants(&timeline), instants[..3].to_vec());
    }
}
