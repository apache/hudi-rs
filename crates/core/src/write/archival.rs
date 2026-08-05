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

use arrow_array::{Array, ArrayRef, BinaryArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::Result;
use crate::error::CoreError;
use crate::storage::Storage;

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
    // Pending = fenced without a completed file.
    let completed_set: std::collections::HashSet<&str> =
        completed.iter().map(|c| c.requested.as_str()).collect();
    for ts in fencing_by_instant.keys() {
        if !completed_set.contains(ts.as_str()) && earliest_pending.as_ref().is_none_or(|e| ts < e)
        {
            earliest_pending = Some(ts.clone());
        }
    }

    completed.sort_by(|a, b| a.requested.cmp(&b.requested));
    let is_commit_action =
        |action: &str| matches!(action, "commit" | "deltacommit" | "replacecommit");
    let commit_count = completed
        .iter()
        .filter(|c| is_commit_action(&c.action))
        .count();
    if commit_count <= max_keep {
        return Ok(());
    }

    // Archive the oldest commits down to `min_keep`, but never at or past the
    // earliest pending instant; sweep along non-commit actions (rollbacks) up
    // to the newest archived commit.
    let mut commits_to_archive = commit_count - min_keep;
    let mut max_archived_commit: Option<String> = None;
    let mut to_archive: Vec<CompletedInstant> = Vec::new();
    for instant in &completed {
        if let Some(pending) = &earliest_pending
            && instant.requested.as_str() >= pending.as_str()
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

    let parquet = write_lsm_parquet(
        &instant_times,
        &completion_times,
        &actions,
        &metadata_bytes,
        &plan_bytes,
    )?;
    let history_dir = format!("{timeline_dir}/history");
    let min_instant = &to_archive[0].requested;
    let max_instant = &to_archive[to_archive.len() - 1].requested;
    let parquet_name = format!("{min_instant}_{max_instant}_0.parquet");
    let parquet_len = parquet.len() as i64;
    storage
        .put_file(&format!("{history_dir}/{parquet_name}"), parquet)
        .await?;

    // Manifest protocol: parquet → manifest_{N+1} → _version_ (Java order).
    let (current_version, mut manifest) = read_manifest(storage, &history_dir).await?;
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

    // Remove archived instants from the active timeline: fencing files first
    // (ascending), completed files after.
    for instant in &to_archive {
        for fencing in fencing_by_instant
            .get(&instant.requested)
            .into_iter()
            .flatten()
        {
            let _ = storage
                .delete_file(&format!("{timeline_dir}/{fencing}"))
                .await;
        }
    }
    for instant in &to_archive {
        let _ = storage
            .delete_file(&format!("{timeline_dir}/{}", instant.file_name))
            .await;
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

/// Requested times of all archived instants (for MDT valid-instants fencing:
/// archived instants are completed instants whose active-timeline files are
/// gone). Empty when no history exists.
pub(crate) async fn archived_instant_times(
    storage: &Storage,
    timeline_dir: &str,
) -> Result<std::collections::HashSet<String>> {
    let history_dir = format!("{timeline_dir}/history");
    let (version, manifest) = read_manifest(storage, &history_dir).await?;
    let mut out = std::collections::HashSet::new();
    if version == 0 {
        return Ok(out);
    }
    for entry in &manifest.files {
        let bytes = storage
            .get_file_data(&format!("{history_dir}/{}", entry.file_name))
            .await?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(bytes.to_vec()),
        )
        .map_err(|e| CoreError::Timeline(format!("open LSM parquet: {e}")))?
        .build()
        .map_err(|e| CoreError::Timeline(format!("read LSM parquet: {e}")))?;
        for batch in reader {
            let batch = batch.map_err(|e| CoreError::Timeline(format!("read LSM batch: {e}")))?;
            let Some(column) = batch.column_by_name("instantTime") else {
                continue;
            };
            if let Some(strings) = column.as_any().downcast_ref::<StringArray>() {
                for i in 0..strings.len() {
                    if !strings.is_null(i) {
                        out.insert(strings.value(i).to_string());
                    }
                }
            }
        }
    }
    Ok(out)
}
