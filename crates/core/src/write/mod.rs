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
//! Write APIs for Hudi tables (pyIceberg-style Table verbs).

mod append;
mod create;
mod keygen;
pub(crate) mod metadata;
mod rewrite;

pub use append::{AppendResult, append_batches};
pub use create::TableCreateBuilder;
pub use rewrite::{
    UpsertOptions, WriteResult, delete_filter, delete_keys, overwrite_batches, update_filter,
    upsert_batches,
};

use std::str::FromStr;

use crate::Result;
use crate::error::CoreError;
use crate::storage::Storage;
use crate::table::partition::PARTITION_METAFIELD_PREFIX;
use crate::timeline::instant::{Action, Instant};

/// Write `.hoodie_partition_metadata` if missing (required for Spark FS partition listing).
pub(crate) async fn ensure_partition_metadata(
    storage: &Storage,
    partition_path: &str,
    instant: &str,
) -> Result<()> {
    if partition_path.is_empty() {
        return Ok(());
    }
    let meta_rel = format!("{partition_path}/{PARTITION_METAFIELD_PREFIX}");
    if storage.exists(&meta_rel).await? {
        return Ok(());
    }
    let depth = partition_path.matches('/').count() + 1;
    // Java `Properties.store` format consumed by HoodiePartitionMetadata.
    let body = format!(
        "#partition metadata\n\
         commitTime={instant}\n\
         partitionDepth={depth}\n"
    );
    storage.put_file(&meta_rel, body.into_bytes()).await?;
    Ok(())
}

/// Java-style data file id: `{uuid}-0` (FSUtils.createNewFileId).
pub(crate) fn new_file_id() -> String {
    format!("{}-0", uuid::Uuid::new_v4())
}

/// Extract `{fileId}` from `{fileId}_{writeToken}_{instant}.parquet`.
#[allow(dead_code)]
pub(crate) fn file_id_from_base_name(file_name: &str) -> String {
    file_name
        .split_once('_')
        .map(|(file_id, _)| file_id.to_string())
        .unwrap_or_else(|| file_name.to_string())
}

/// Write `{ts}.{action}.requested` then inflight markers (Java ActiveTimeline fencing).
///
/// COW commit inflight uses `{ts}.inflight` (no action infix); other actions use
/// `{ts}.{action}.inflight`.
pub(crate) async fn fence_timeline_instant(
    storage: &Storage,
    timeline_dir: &str,
    timestamp: &str,
    action: Action,
) -> Result<()> {
    let requested = Instant::from_str(&format!(
        "{}.{}.requested",
        timestamp,
        action.as_ref()
    ))
    .map_err(|e| CoreError::Write(format!("invalid fencing instant: {e}")))?;
    let inflight = if action == Action::Commit {
        Instant::from_str(&format!("{timestamp}.inflight"))
    } else {
        Instant::from_str(&format!(
            "{}.{}.inflight",
            timestamp,
            action.as_ref()
        ))
    }
    .map_err(|e| CoreError::Write(format!("invalid fencing instant: {e}")))?;

    storage
        .put_file(
            &requested.relative_path_with_base(timeline_dir)?,
            b"".as_slice(),
        )
        .await?;
    storage
        .put_file(
            &inflight.relative_path_with_base(timeline_dir)?,
            b"".as_slice(),
        )
        .await?;
    Ok(())
}
