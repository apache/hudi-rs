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
//! rolled back: its marker-listed base/log files are deleted (tv8+ rollback is
//! deletion-only), its completed-but-orphaned MDT deltacommit and MDT log
//! files are removed, a `rollback` instant records the operation, and the
//! failed instant's fencing files and marker dir are cleaned up.

use std::collections::HashMap;

use crate::Result;
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

/// Roll back all pending data instants (Java `rollbackFailedWrites`, EAGER).
///
/// Single-writer: any pending instant belongs to a crashed previous write.
/// Rollback instants themselves and MDT-internal instants are never targets.
pub(crate) async fn rollback_failed_writes(table: &Table) -> Result<()> {
    let storage = table.file_system_view.storage.clone();
    let timeline = timeline_dir(table);
    let pending = find_pending_instants(storage.as_ref(), &timeline).await?;
    for instant in pending {
        rollback_instant(table, storage.as_ref(), &timeline, &instant).await?;
    }
    Ok(())
}

async fn find_pending_instants(
    storage: &Storage,
    timeline_dir: &str,
) -> Result<Vec<PendingInstant>> {
    let files = match storage.list_files(Some(timeline_dir)).await {
        Ok(files) => files,
        Err(crate::storage::error::StorageError::ObjectStoreError(
            object_store::Error::NotFound { .. },
        )) => return Ok(Vec::new()),
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
            // `{ts}.{action}.requested|inflight` or COW `{ts}.inflight`.
            let action = name
                .trim_start_matches(&timestamp)
                .trim_start_matches('.')
                .trim_end_matches(".requested")
                .trim_end_matches(".inflight")
                .trim_end_matches('.')
                .to_string();
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
    let mut pending: Vec<PendingInstant> = fenced
        .into_iter()
        .filter(|(ts, _)| !completed.contains(ts))
        // Rollback/compaction-style instants are never rolled back here.
        .filter(|(_, (action, _))| {
            matches!(
                action.as_str(),
                "commit" | "deltacommit" | "replacecommit" | ""
            )
        })
        .map(|(timestamp, (action, fencing_files))| PendingInstant {
            timestamp,
            action: if action.is_empty() {
                "commit".to_string()
            } else {
                action
            },
            fencing_files,
        })
        .collect();
    pending.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    Ok(pending)
}

async fn rollback_instant(
    table: &Table,
    storage: &Storage,
    timeline_dir: &str,
    pending: &PendingInstant,
) -> Result<()> {
    let start_time = std::time::Instant::now();
    let failed_ts = &pending.timestamp;

    // Markers tell us exactly what the failed write intended to put. A crash
    // before markers means no data or MDT files were written (markers are
    // written first), so an empty list still produces a valid rollback.
    let markers = read_markers(storage, failed_ts).await?;
    let mut requests_by_partition: HashMap<String, HoodieRollbackRequest> = HashMap::new();
    let mut partition_meta: HashMap<String, HoodieRollbackPartitionMetadata> = HashMap::new();
    let mut total_deleted = 0i32;
    for marker in &markers {
        let Some((file_path, _io_type)) = parse_marker_name(marker) else {
            continue;
        };
        let (partition, file_name) = match file_path.rsplit_once('/') {
            Some((partition, name)) => (partition.to_string(), name.to_string()),
            None => (String::new(), file_path.clone()),
        };
        let is_log_file = file_name.starts_with('.') && file_name.contains(".log.");
        let request = requests_by_partition
            .entry(partition.clone())
            .or_insert_with(|| HoodieRollbackRequest {
                partition_path: partition.clone(),
                file_id: None,
                latest_base_instant: None,
                files_to_be_deleted: Vec::new(),
                log_blocks_to_be_deleted: None,
            });
        if is_log_file {
            request
                .log_blocks_to_be_deleted
                .get_or_insert_with(HashMap::new)
                .insert(file_path.clone(), 1);
        } else {
            request.files_to_be_deleted.push(file_path.clone());
        }
        let deleted = storage.delete_file(&file_path).await.is_ok();
        let meta = partition_meta.entry(partition.clone()).or_insert_with(|| {
            HoodieRollbackPartitionMetadata {
                partition_path: partition.clone(),
                ..Default::default()
            }
        });
        if deleted {
            total_deleted += 1;
            meta.success_delete_files.push(file_path);
        } else {
            meta.failed_delete_files.push(file_path);
        }
    }

    // Rollback plan + lifecycle on its own (new) instant.
    let rollback_ts = generate_instant_time().await;
    let plan = HoodieRollbackPlan {
        instant_to_rollback: Some(HoodieInstantInfo {
            commit_time: failed_ts.clone(),
            action: pending.action.clone(),
        }),
        rollback_requests: Some(requests_by_partition.into_values().collect()),
        version: Some(1),
    };
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
        commits_rollback: vec![failed_ts.clone()],
        partition_metadata: partition_meta,
        version: Some(1),
        instants_rollback: vec![HoodieInstantInfo {
            commit_time: failed_ts.clone(),
            action: pending.action.clone(),
        }],
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
    for fencing in pending
        .fencing_files
        .iter()
        .filter(|n| n.ends_with(".inflight"))
        .chain(
            pending
                .fencing_files
                .iter()
                .filter(|n| n.ends_with(".requested")),
        )
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
                    let _ = storage.delete_file(&format!("{dir}/{}", file.name)).await;
                }
            }
        }
    }
    for name in mdt_instant_files {
        let _ = storage.delete_file(&format!("{mdt_timeline}/{name}")).await;
    }
    Ok(())
}
