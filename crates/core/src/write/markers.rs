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
//! Write markers (Java `.hoodie/.temp/{instant}` marker files).
//!
//! We emit the TIMELINE_SERVER_BASED layout — `MARKERS.type` plus a single
//! `MARKERS0` — written **without** a timeline server: the server only batches
//! marker RPCs from distributed executors, which a single-node writer that
//! knows every file name up front does not need. Readers (including Java's
//! rollback) consume the files straight from storage.

use crate::Result;
use crate::storage::Storage;

const TEMP_DIR: &str = ".hoodie/.temp";
const MARKER_TYPE_FILENAME: &str = "MARKERS.type";
const MARKERS_FILENAME_PREFIX: &str = "MARKERS";
const MARKER_EXTENSION: &str = ".marker.";

/// Java `IOType` for markers: how the marked file came to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MarkerIoType {
    /// New base file or (tv8+) new log file.
    Create,
    /// Same-file-group base file rewrite.
    Merge,
}

impl MarkerIoType {
    fn as_str(&self) -> &'static str {
        match self {
            MarkerIoType::Create => "CREATE",
            MarkerIoType::Merge => "MERGE",
        }
    }
}

/// One marker: a data file this commit intends to write.
#[derive(Debug, Clone)]
pub(crate) struct Marker {
    /// Relative partition path; empty for non-partitioned tables.
    pub partition_path: String,
    /// Base or log file name (log names keep their leading dot).
    pub file_name: String,
    pub io_type: MarkerIoType,
}

impl Marker {
    pub(crate) fn create(partition_path: &str, file_name: &str) -> Self {
        Self {
            partition_path: partition_path.to_string(),
            file_name: file_name.to_string(),
            io_type: MarkerIoType::Create,
        }
    }

    pub(crate) fn merge(partition_path: &str, file_name: &str) -> Self {
        Self {
            partition_path: partition_path.to_string(),
            file_name: file_name.to_string(),
            io_type: MarkerIoType::Merge,
        }
    }

    /// Marker name relative to the marker dir:
    /// `{partition}/{file}.marker.{IOTYPE}` (no partition prefix when empty).
    fn name(&self) -> String {
        let suffix = format!(
            "{}{}{}",
            self.file_name,
            MARKER_EXTENSION,
            self.io_type.as_str()
        );
        if self.partition_path.is_empty() {
            suffix
        } else {
            format!("{}/{suffix}", self.partition_path)
        }
    }
}

fn marker_dir(instant: &str) -> String {
    format!("{TEMP_DIR}/{instant}")
}

/// Write `MARKERS.type` + `MARKERS0` for a commit, before any data file is put.
pub(crate) async fn write_markers(
    storage: &Storage,
    instant: &str,
    markers: &[Marker],
) -> Result<()> {
    if markers.is_empty() {
        return Ok(());
    }
    let dir = marker_dir(instant);
    storage
        .put_file(
            &format!("{dir}/{MARKER_TYPE_FILENAME}"),
            b"TIMELINE_SERVER_BASED".to_vec(),
        )
        .await?;
    let mut body = String::new();
    for marker in markers {
        body.push_str(&marker.name());
        body.push('\n');
    }
    storage
        .put_file(
            &format!("{dir}/{MARKERS_FILENAME_PREFIX}0"),
            body.into_bytes(),
        )
        .await?;
    Ok(())
}

/// Read all marker names for an instant (relative to the marker dir).
///
/// Supports both layouts so crashed Spark commits roll back too: MARKERS*
/// files (timeline-server-based) when present, otherwise individual
/// `*.marker.*` files (direct).
pub(crate) async fn read_markers(storage: &Storage, instant: &str) -> Result<Vec<String>> {
    let dir = marker_dir(instant);
    let listed = match storage.list_files(Some(&dir)).await {
        Ok(files) => files,
        Err(crate::storage::error::StorageError::ObjectStoreError(
            object_store::Error::NotFound { .. },
        )) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut markers = Vec::new();
    let mut has_markers_files = false;
    for file in &listed {
        if file.name.starts_with(MARKERS_FILENAME_PREFIX) && file.name != MARKER_TYPE_FILENAME {
            has_markers_files = true;
            let bytes = storage
                .get_file_data(&format!("{dir}/{}", file.name))
                .await?;
            let text = String::from_utf8_lossy(&bytes);
            markers.extend(
                text.lines()
                    .map(str::trim)
                    .filter(|l| !l.is_empty())
                    .map(String::from),
            );
        }
    }
    if has_markers_files {
        return Ok(markers);
    }
    // Direct markers: one `*.marker.*` file per data file, possibly nested in
    // partition sub-dirs (crashed Spark writers with direct markers).
    let mut markers: Vec<String> = list_files_recursive(storage, &dir)
        .await?
        .into_iter()
        .filter(|name| name.contains(MARKER_EXTENSION))
        .collect();
    markers.sort();
    markers.dedup();
    Ok(markers)
}

/// List all files under `dir` recursively, as paths relative to `dir`.
async fn list_files_recursive(storage: &Storage, dir: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut stack = vec![String::new()];
    while let Some(rel) = stack.pop() {
        let full = if rel.is_empty() {
            dir.to_string()
        } else {
            format!("{dir}/{rel}")
        };
        match storage.list_files(Some(&full)).await {
            Ok(files) => {
                for file in files {
                    out.push(if rel.is_empty() {
                        file.name
                    } else {
                        format!("{rel}/{}", file.name)
                    });
                }
            }
            Err(crate::storage::error::StorageError::ObjectStoreError(
                object_store::Error::NotFound { .. },
            )) => {}
            Err(error) => return Err(error.into()),
        }
        match storage.list_dirs(Some(&full)).await {
            Ok(dirs) => {
                for child in dirs {
                    stack.push(if rel.is_empty() {
                        child
                    } else {
                        format!("{rel}/{child}")
                    });
                }
            }
            Err(crate::storage::error::StorageError::ObjectStoreError(
                object_store::Error::NotFound { .. },
            )) => {}
            // A partition we cannot list is a partition we cannot roll back;
            // surfacing beats silently skipping it.
            Err(error) => return Err(error.into()),
        }
    }
    Ok(out)
}

/// Delete the marker dir for an instant (after successful commit or rollback).
/// Best-effort: failures are ignored — a stale empty dir is harmless.
pub(crate) async fn delete_marker_dir(storage: &Storage, instant: &str) {
    let dir = marker_dir(instant);
    if let Ok(names) = list_files_recursive(storage, &dir).await {
        for name in names {
            let _ = storage.delete_file(&format!("{dir}/{name}")).await;
        }
    }
}

/// Marker names (relative to marker dir) → data-file relative paths + IO types.
///
/// `city=sf/uuid_0-0-0_t.parquet.marker.CREATE` → (`city=sf/uuid_0-0-0_t.parquet`, CREATE).
pub(crate) fn parse_marker_name(marker: &str) -> Option<(String, String)> {
    let idx = marker.rfind(MARKER_EXTENSION)?;
    let file_path = marker[..idx].to_string();
    let io_type = marker[idx + MARKER_EXTENSION.len()..].to_string();
    Some((file_path, io_type))
}
