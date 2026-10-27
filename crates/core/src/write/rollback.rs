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
//! Eager marker-based rollback of failed writes (Java `EAGER` cleaning policy).
//!
//! At write start, any pending data instant (fenced but never completed) is
//! rolled back: a `rollback` plan listing its marker-listed base/log files is
//! persisted, those files are deleted (tv8+ rollback is deletion-only), its
//! completed-but-orphaned MDT deltacommit and MDT log files are removed, the
//! rollback completes, and the failed instant's fencing files and marker dir
//! are cleaned up. A rollback interrupted after its plan was persisted is
//! resumed from that plan.

use std::collections::HashMap;

use crate::Result;
use crate::error::CoreError;
use crate::metadata::rollback::{
    HoodieInstantInfo, HoodieRollbackMetadata, HoodieRollbackPartitionMetadata, HoodieRollbackPlan,
    HoodieRollbackRequest,
};
use crate::storage::Storage;
use crate::table::Table;
use crate::write::append::{generate_instant_time, is_layout_two, timeline_dir};
use crate::write::markers::{delete_marker_dir, parse_marker_name, read_markers};

const METADATA_BASE: &str = ".hoodie/metadata";

/// A pending (fenced, never completed) instant on the data timeline.
#[derive(Debug, Clone)]
struct PendingInstant {
    timestamp: String,
    action: String,
    /// Fencing file names present on the timeline (to delete after rollback).
    fencing_files: Vec<String>,
}

/// A rollback whose plan was persisted but which never completed.
struct PendingRollback {
    timestamp: String,
    plan: HoodieRollbackPlan,
    inflight: bool,
}

/// Roll back all pending data instants (Java `rollbackFailedWrites`, EAGER).
///
/// Single-writer: any pending instant belongs to a crashed previous write.
/// A rollback left pending by a crash is resumed on its own instant and plan
/// (Java `getPendingRollbackInfos`), whether or not its target is still
/// pending; MDT-internal instants are never targets.
pub(crate) async fn rollback_failed_writes(table: &Table) -> Result<()> {
    let storage = table.file_system_view.storage.clone();
    let timeline = timeline_dir(table);
    let (pending, pending_rollbacks) = find_pending_instants(storage.as_ref(), &timeline).await?;
    let mut resumable = HashMap::new();
    for rollback in pending_rollbacks {
        let Some(rollback) = load_pending_rollback(storage.as_ref(), &timeline, rollback).await?
        else {
            continue;
        };
        let Some(target) = rollback.plan.instant_to_rollback.clone() else {
            continue;
        };
        // Rollbacks of table-service instants belong to that service.
        if !matches!(
            target.action.as_str(),
            "commit" | "deltacommit" | "replacecommit"
        ) {
            continue;
        }
        resumable.entry(target.commit_time).or_insert(rollback);
    }
    let mut targets: Vec<(String, Option<PendingInstant>)> = pending
        .into_iter()
        .map(|instant| (instant.timestamp.clone(), Some(instant)))
        .collect();
    targets.extend(
        resumable
            .keys()
            .filter(|ts| !targets.iter().any(|(t, _)| t == *ts))
            .map(|ts| (ts.clone(), None))
            .collect::<Vec<_>>(),
    );
    targets.sort_by(|a, b| a.0.cmp(&b.0));
    for (failed_ts, instant) in targets {
        let rollback = resumable.remove(&failed_ts);
        rollback_instant(
            table,
            storage.as_ref(),
            &timeline,
            &failed_ts,
            instant.as_ref(),
            rollback,
        )
        .await?;
    }
    Ok(())
}

/// Reads a pending rollback's plan. An unreadable plan on a rollback that
/// never reached inflight is deleted (nothing ran yet); one that did is
/// skipped, as Java does.
async fn load_pending_rollback(
    storage: &Storage,
    timeline_dir: &str,
    rollback: PendingInstant,
) -> Result<Option<PendingRollback>> {
    let requested = format!("{}.rollback.requested", rollback.timestamp);
    let inflight = rollback
        .fencing_files
        .iter()
        .any(|n| n.ends_with(".inflight"));
    let bytes = storage
        .get_file_data(&format!("{timeline_dir}/{requested}"))
        .await?;
    match HoodieRollbackPlan::from_avro_bytes(&bytes) {
        Ok(plan) => Ok(Some(PendingRollback {
            timestamp: rollback.timestamp,
            plan,
            inflight,
        })),
        Err(_) => {
            if !inflight {
                storage
                    .delete_file(&format!("{timeline_dir}/{requested}"))
                    .await?;
            }
            Ok(None)
        }
    }
}

/// The action infix of a fencing file name: `{ts}.{action}.requested|inflight`
/// or the bare COW forms `{ts}.inflight` / `{ts}.requested` (empty action).
///
/// Must not depend on which of an instant's fencing files is listed first —
/// directory listing order is filesystem-specific (ext4 returns hash order,
/// so `.inflight` can precede `.commit.requested`).
pub(crate) fn action_from_fencing_name(name: &str, timestamp: &str) -> String {
    let rest = name.trim_start_matches(timestamp).trim_start_matches('.');
    match rest {
        "inflight" | "requested" => String::new(),
        other => other
            .trim_end_matches(".requested")
            .trim_end_matches(".inflight")
            .to_string(),
    }
}

/// Pending data instants to roll back, and pending rollback instants.
async fn find_pending_instants(
    storage: &Storage,
    timeline_dir: &str,
) -> Result<(Vec<PendingInstant>, Vec<PendingInstant>)> {
    let files = match storage.list_files(Some(timeline_dir)).await {
        Ok(files) => files,
        Err(crate::storage::error::StorageError::ObjectStoreError(
            object_store::Error::NotFound { .. },
        )) => return Ok((Vec::new(), Vec::new())),
        Err(error) => return Err(error.into()),
    };
    let mut completed = std::collections::HashSet::new();
    let mut fenced: HashMap<String, (String, Vec<String>)> = HashMap::new();
    for file in &files {
        let name = &file.name;
        let Some(first) = name.chars().next() else {
            continue;
        };
        if !first.is_ascii_digit() {
            continue;
        }
        let timestamp: String = name.chars().take_while(char::is_ascii_digit).collect();
        if name.ends_with(".requested") || name.ends_with(".inflight") {
            let action = action_from_fencing_name(name, &timestamp);
            let entry = fenced
                .entry(timestamp)
                .or_insert_with(|| (action.clone(), Vec::new()));
            // Prefer the action-bearing name (COW inflight has no action infix).
            if entry.0.is_empty() && !action.is_empty() {
                entry.0 = action;
            }
            entry.1.push(name.clone());
        } else {
            completed.insert(timestamp);
        }
    }
    let mut pending = Vec::new();
    let mut rollbacks = Vec::new();
    for (timestamp, (action, fencing_files)) in fenced {
        if completed.contains(&timestamp) {
            continue;
        }
        let instant = PendingInstant {
            timestamp,
            action: if action.is_empty() {
                "commit".to_string()
            } else {
                action
            },
            fencing_files,
        };
        match instant.action.as_str() {
            "commit" | "deltacommit" | "replacecommit" => pending.push(instant),
            "rollback" => rollbacks.push(instant),
            // Compaction-style instants are never rolled back here.
            _ => {}
        }
    }
    pending.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    rollbacks.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    Ok((pending, rollbacks))
}

/// Java `MarkerBasedRollbackStrategy` at table version 8+: one request per
/// marker, deleting the file whether base or log, by absolute path.
async fn rollback_requests_from_markers(
    table: &Table,
    storage: &Storage,
    failed_ts: &str,
) -> Result<Vec<HoodieRollbackRequest>> {
    // Markers tell us exactly what the failed write intended to put. A crash
    // before markers means no data or MDT files were written (markers are
    // written first), so an empty list still produces a valid rollback.
    let markers = read_markers(storage, failed_ts).await?;
    let base = table.base_url();
    let base = base.as_str().trim_end_matches('/');
    let mut requests = Vec::new();
    for marker in &markers {
        let Some((file_path, _io_type)) = parse_marker_name(marker) else {
            continue;
        };
        let (partition, file_name) = match file_path.rsplit_once('/') {
            Some((partition, name)) => (partition, name),
            None => ("", file_path.as_str()),
        };
        let file_id = file_name
            .trim_start_matches('.')
            .split('_')
            .next()
            .unwrap_or_default();
        requests.push(HoodieRollbackRequest {
            partition_path: partition.to_string(),
            file_id: Some(file_id.to_string()),
            latest_base_instant: Some(failed_ts.to_string()),
            files_to_be_deleted: vec![format!("{base}/{file_path}")],
            log_blocks_to_be_deleted: Some(HashMap::new()),
        });
    }
    Ok(requests)
}

/// The table-relative form of a path in a rollback plan, matched on the URL
/// path so the scheme a plan's writer used does not matter (Java
/// `FSUtils.getRelativePartitionPath`).
fn relative_to_base(base: &url::Url, path: &str) -> Result<String> {
    if let Some(relative) = path
        .strip_prefix(base.as_str().trim_end_matches('/'))
        .and_then(|rest| rest.strip_prefix('/'))
    {
        return Ok(relative.to_string());
    }
    let path_part = match url::Url::parse(path) {
        Ok(url) => url.path().to_string(),
        Err(_) if !path.starts_with('/') => return Ok(path.to_string()),
        Err(_) => path.to_string(),
    };
    path_part
        .strip_prefix(base.path().trim_end_matches('/'))
        .and_then(|rest| rest.strip_prefix('/'))
        .map(str::to_string)
        .ok_or_else(|| {
            CoreError::Unsupported(format!(
                "rollback plan path {path} is not under the table base path {base}"
            ))
        })
}

async fn rollback_instant(
    table: &Table,
    storage: &Storage,
    timeline_dir: &str,
    failed_ts: &str,
    pending: Option<&PendingInstant>,
    resumed: Option<PendingRollback>,
) -> Result<()> {
    let start_time = std::time::Instant::now();

    // Persist the plan before deleting anything, so a rollback interrupted
    // midway is resumed from the same plan and instant.
    let (rollback_ts, plan) = match resumed {
        Some(rollback) => {
            if !rollback.inflight {
                storage
                    .put_file_if_absent(
                        &format!("{timeline_dir}/{}.rollback.inflight", rollback.timestamp),
                        Vec::new(),
                    )
                    .await?;
            }
            (rollback.timestamp, rollback.plan)
        }
        None => {
            let action = pending.map_or("commit", |p| p.action.as_str());
            let plan = HoodieRollbackPlan {
                instant_to_rollback: Some(HoodieInstantInfo {
                    commit_time: failed_ts.to_string(),
                    action: action.to_string(),
                }),
                rollback_requests: Some(
                    rollback_requests_from_markers(table, storage, failed_ts).await?,
                ),
                version: Some(1),
            };
            let rollback_ts = generate_instant_time().await;
            storage
                .put_file_if_absent(
                    &format!("{timeline_dir}/{rollback_ts}.rollback.requested"),
                    plan.to_avro_bytes()?,
                )
                .await?;
            storage
                .put_file_if_absent(
                    &format!("{timeline_dir}/{rollback_ts}.rollback.inflight"),
                    Vec::new(),
                )
                .await?;
            (rollback_ts, plan)
        }
    };
    let target = plan
        .instant_to_rollback
        .clone()
        .unwrap_or(HoodieInstantInfo {
            commit_time: failed_ts.to_string(),
            action: "commit".to_string(),
        });

    let base = table.base_url();
    let mut partition_meta: HashMap<String, HoodieRollbackPartitionMetadata> = HashMap::new();
    let mut total_deleted = 0i32;
    for request in plan.rollback_requests.iter().flatten() {
        if request
            .log_blocks_to_be_deleted
            .as_ref()
            .is_some_and(|blocks| !blocks.is_empty())
        {
            return Err(CoreError::Unsupported(format!(
                "rollback {rollback_ts} appends rollback blocks to log files, which table \
                 version 8+ does not use"
            )));
        }
        for file in &request.files_to_be_deleted {
            let relative = relative_to_base(&base, file)?;
            // A surviving file must fail the rollback (Java `RollbackHelper.deleteFiles`):
            // completing it unfences the instant, and once archival passes the
            // instant the file reads as committed.
            if let Err(error) = storage.delete_file(&relative).await
                && storage.exists(&relative).await.unwrap_or(true)
            {
                return Err(error.into());
            }
            let meta = partition_meta
                .entry(request.partition_path.clone())
                .or_insert_with(|| HoodieRollbackPartitionMetadata {
                    partition_path: request.partition_path.clone(),
                    ..Default::default()
                });
            total_deleted += 1;
            meta.success_delete_files.push(file.clone());
        }
    }

    // Remove the orphan MDT deltacommit (and its log blocks) if the crashed
    // write got as far as committing the MDT. Required: once the failed
    // instant's fencing files are gone, the valid-instants fence would start
    // trusting the orphan.
    if table.is_metadata_table_enabled() {
        rollback_orphan_mdt_commit(storage, failed_ts).await?;
    }

    let metadata = HoodieRollbackMetadata {
        start_rollback_time: rollback_ts.clone(),
        time_taken_in_millis: start_time.elapsed().as_millis() as i64,
        total_files_deleted: total_deleted,
        commits_rollback: vec![failed_ts.to_string()],
        partition_metadata: partition_meta,
        version: Some(1),
        instants_rollback: vec![target],
    };
    let completion = if is_layout_two(table) {
        generate_instant_time().await
    } else {
        rollback_ts.clone()
    };
    storage
        .put_file_if_absent(
            &format!("{timeline_dir}/{rollback_ts}_{completion}.rollback"),
            metadata.to_avro_bytes()?,
        )
        .await?;

    // Java order: inflight first, then requested, then the marker dir.
    let fencing_files = pending
        .map(|p| p.fencing_files.as_slice())
        .unwrap_or_default();
    for fencing in fencing_files
        .iter()
        .filter(|n| n.ends_with(".inflight"))
        .chain(fencing_files.iter().filter(|n| n.ends_with(".requested")))
    {
        let _ = storage
            .delete_file(&format!("{timeline_dir}/{fencing}"))
            .await;
    }
    delete_marker_dir(storage, failed_ts).await;
    Ok(())
}

/// Delete a completed-but-orphaned MDT deltacommit for a failed data instant:
/// its instant-named log files in every MDT partition, then its timeline files.
/// (Java rolls the MDT deltacommit back via the MDT write client; deletion is
/// the single-writer equivalent — the log files are per-instant, never shared.)
async fn rollback_orphan_mdt_commit(storage: &Storage, failed_ts: &str) -> Result<()> {
    let mdt_timeline = format!("{METADATA_BASE}/.hoodie/timeline");
    let timeline_files = match storage.list_files(Some(&mdt_timeline)).await {
        Ok(files) => files,
        Err(crate::storage::error::StorageError::ObjectStoreError(
            object_store::Error::NotFound { .. },
        )) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let mut mdt_instant_files = Vec::new();
    let mut has_completed = false;
    for file in timeline_files {
        let name = file.name;
        let matches_instant = name.starts_with(&format!("{failed_ts}_"))
            || name.starts_with(&format!("{failed_ts}."));
        if matches_instant {
            if name.ends_with(".deltacommit") {
                has_completed = true;
            }
            mdt_instant_files.push(name);
        }
    }
    if mdt_instant_files.is_empty() {
        return Ok(());
    }
    if has_completed {
        // Delete the failed instant's log blocks from every MDT partition.
        let partitions = storage.list_dirs(Some(METADATA_BASE)).await?;
        for partition in partitions {
            if partition == ".hoodie" {
                continue;
            }
            let dir = format!("{METADATA_BASE}/{partition}");
            let Ok(files) = storage.list_files(Some(&dir)).await else {
                continue;
            };
            for file in files {
                // MDT log files embed the data instant: `.{fileId}_{ts}.log.N_...`.
                if file.name.contains(&format!("_{failed_ts}.log.")) {
                    storage.delete_file(&format!("{dir}/{}", file.name)).await?;
                }
            }
        }
    }
    for name in mdt_instant_files {
        storage
            .delete_file(&format!("{mdt_timeline}/{name}"))
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{action_from_fencing_name, relative_to_base};

    /// Plan paths written by either engine resolve to the same table-relative
    /// path, whatever scheme spelling the writer used.
    #[test]
    fn test_relative_to_base_accepts_any_scheme_spelling() {
        let base = url::Url::parse("file:///tmp/t/").unwrap();
        for path in [
            "file:///tmp/t/city=sf/f.parquet",
            "file:/tmp/t/city=sf/f.parquet",
            "/tmp/t/city=sf/f.parquet",
            "city=sf/f.parquet",
        ] {
            assert_eq!(relative_to_base(&base, path).unwrap(), "city=sf/f.parquet");
        }
        let s3 = url::Url::parse("s3://bucket/t").unwrap();
        assert_eq!(
            relative_to_base(&s3, "s3a://bucket/t/f.parquet").unwrap(),
            "f.parquet"
        );
        assert!(relative_to_base(&base, "file:///tmp/other/f.parquet").is_err());
        assert!(relative_to_base(&base, "file:///tmp/t2/f.parquet").is_err());
    }

    /// Regression: on ext4 the bare COW inflight can be listed before the
    /// action-bearing requested file; "inflight" must never leak out as an
    /// action name or the instant is silently skipped by rollback.
    #[test]
    fn test_action_from_fencing_name_all_forms() {
        let ts = "30000101000000000";
        assert_eq!(
            action_from_fencing_name("30000101000000000.commit.requested", ts),
            "commit"
        );
        assert_eq!(
            action_from_fencing_name("30000101000000000.inflight", ts),
            ""
        );
        assert_eq!(
            action_from_fencing_name("30000101000000000.deltacommit.inflight", ts),
            "deltacommit"
        );
        assert_eq!(
            action_from_fencing_name("30000101000000000.replacecommit.requested", ts),
            "replacecommit"
        );
        assert_eq!(
            action_from_fencing_name("30000101000000000.rollback.requested", ts),
            "rollback"
        );
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use crate::table::Table;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(id: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![id])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1i64])),
            ],
        )
        .unwrap()
    }

    /// Fabricated crashed write (fencing + markers + partial file + orphan MDT
    /// deltacommit) rolled back by the next write, from inside the crate.
    #[tokio::test]
    async fn test_eager_rollback_in_crate() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .create()
            .await
            .unwrap();
        table.append([batch("a")]).await.unwrap();

        let ts = "30000101000000000";
        let timeline = dir.path().join(".hoodie/timeline");
        std::fs::write(timeline.join(format!("{ts}.commit.requested")), b"").unwrap();
        std::fs::write(timeline.join(format!("{ts}.inflight")), b"").unwrap();
        let partial = format!("00000000-1111-2222-3333-444444444444-0_0-0-0_{ts}.parquet");
        std::fs::write(dir.path().join(&partial), b"partial").unwrap();
        let marker_dir = dir.path().join(".hoodie/.temp").join(ts);
        std::fs::create_dir_all(&marker_dir).unwrap();
        std::fs::write(marker_dir.join("MARKERS.type"), b"TIMELINE_SERVER_BASED").unwrap();
        std::fs::write(
            marker_dir.join("MARKERS0"),
            format!("{partial}.marker.CREATE\n"),
        )
        .unwrap();
        let mdt_timeline = dir.path().join(".hoodie/metadata/.hoodie/timeline");
        std::fs::write(
            mdt_timeline.join(format!("{ts}.deltacommit.requested")),
            b"",
        )
        .unwrap();
        std::fs::write(mdt_timeline.join(format!("{ts}.deltacommit.inflight")), b"").unwrap();
        std::fs::write(mdt_timeline.join(format!("{ts}_{ts}.deltacommit")), b"x").unwrap();

        table.append([batch("b")]).await.unwrap();

        assert!(!dir.path().join(&partial).exists());
        assert!(!timeline.join(format!("{ts}.commit.requested")).exists());
        assert!(!timeline.join(format!("{ts}.inflight")).exists());
        let leftovers = std::fs::read_dir(&marker_dir)
            .map(|entries| entries.count())
            .unwrap_or(0);
        assert_eq!(leftovers, 0, "marker dir must be emptied");
        assert!(!mdt_timeline.join(format!("{ts}_{ts}.deltacommit")).exists());
        let rollbacks = std::fs::read_dir(&timeline)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let n = e.file_name().to_string_lossy().to_string();
                n.contains(".rollback") && !n.ends_with(".requested") && !n.ends_with(".inflight")
            })
            .count();
        assert_eq!(rollbacks, 1, "one completed rollback instant");
    }
}
