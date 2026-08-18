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
//! Write APIs for Hudi tables (`Table` verbs).

mod append;
pub(crate) mod archival;
mod create;
mod keygen;
pub mod lock;
pub(crate) mod markers;
pub(crate) mod metadata;
mod rewrite;
pub(crate) mod rollback;
pub(crate) mod sizing;

pub(crate) use append::set_commit_timezone;
pub use append::{AppendResult, append_batches, append_batches_only};
pub use create::TableCreateBuilder;
pub use lock::{InProcessLockProvider, LockLease, LockProvider};
pub use rewrite::{
    UpsertOptions, WriteResult, delete_filter, delete_keys, dynamic_partition_overwrite_batches,
    overwrite_batches, update_filter, upsert_batches,
};

use std::collections::HashMap;
use std::str::FromStr;

use apache_avro::to_avro_datum;
use apache_avro::types::Value as AvroValue;

use crate::Result;
use crate::error::CoreError;
use crate::file_group::log_file::writer::LogFileWriter;
use crate::file_group::log_file::{BlockMetadataKey, BlockType};
use crate::schema::delete::{
    avro_schema_for_delete_record_list, delete_record_list_avro_schema_json,
};
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
/// Align `batch` to `target` when the two schemas differ only in nullability
/// or field metadata (e.g. a Spark writer evolved the table schema to
/// all-nullable fields). Returns `None` when names, types, or order differ,
/// or when tightening nullability would be invalidated by actual nulls.
pub(crate) fn align_batch_to_schema(
    batch: &arrow_array::RecordBatch,
    target: &std::sync::Arc<arrow_schema::Schema>,
) -> Option<arrow_array::RecordBatch> {
    if &batch.schema() == target {
        return Some(batch.clone());
    }
    if batch.num_columns() != target.fields().len() {
        return None;
    }
    for (batch_field, target_field) in batch.schema().fields().iter().zip(target.fields()) {
        if batch_field.name() != target_field.name()
            || batch_field.data_type() != target_field.data_type()
        {
            return None;
        }
    }
    arrow_array::RecordBatch::try_new(target.clone(), batch.columns().to_vec()).ok()
}

pub(crate) fn new_file_id() -> String {
    format!("{}-0", uuid::Uuid::new_v4())
}

/// Write `{ts}.{action}.requested` then inflight markers (Java ActiveTimeline fencing).
///
/// COW commit inflight uses `{ts}.inflight` (no action infix); other actions use
/// `{ts}.{action}.inflight`. Java v2 content parity: `requested` is empty except
/// for replacecommits (which carry `HoodieRequestedReplaceMetadata`); `inflight`
/// carries a `HoodieCommitMetadata` container with the operation type.
pub(crate) async fn fence_timeline_instant(
    storage: &Storage,
    timeline_dir: &str,
    timestamp: &str,
    action: Action,
    requested_bytes: Vec<u8>,
    inflight_bytes: Vec<u8>,
) -> Result<()> {
    let requested = Instant::from_str(&format!("{}.{}.requested", timestamp, action.as_ref()))
        .map_err(|e| CoreError::Write(format!("invalid fencing instant: {e}")))?;
    let inflight = if action == Action::Commit {
        Instant::from_str(&format!("{timestamp}.inflight"))
    } else {
        Instant::from_str(&format!("{}.{}.inflight", timestamp, action.as_ref()))
    }
    .map_err(|e| CoreError::Write(format!("invalid fencing instant: {e}")))?;

    // Create-if-absent: a second writer minting the same instant must surface
    // as a conflict here, not silently share (and later clobber) the instant.
    storage
        .put_file_if_absent(
            &requested.relative_path_with_base(timeline_dir)?,
            requested_bytes,
        )
        .await?;
    storage
        .put_file_if_absent(
            &inflight.relative_path_with_base(timeline_dir)?,
            inflight_bytes,
        )
        .await?;
    Ok(())
}

/// Concurrent file-write tasks per write (optional
/// `hoodie.write.task.parallelism`; default 2 × available cores to overlap
/// CPU-bound parquet encoding with storage I/O). `1` runs sequentially.
pub(crate) fn write_task_parallelism(table: &crate::table::Table) -> usize {
    table
        .hudi_configs
        .as_options()
        .get("hoodie.write.task.parallelism")
        .and_then(|v| v.parse().ok())
        .filter(|n: &usize| *n >= 1)
        .unwrap_or_else(|| {
            2 * std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(4)
        })
}

/// Run write tasks with bounded concurrency; results come back in input order.
pub(crate) async fn run_write_tasks<T: Send + 'static>(
    tasks: Vec<futures::future::BoxFuture<'static, Result<T>>>,
    parallelism: usize,
) -> Vec<Result<T>> {
    use futures::StreamExt;
    let count = tasks.len();
    let mut results: Vec<Option<Result<T>>> = std::iter::repeat_with(|| None).take(count).collect();
    let mut stream = futures::stream::iter(
        tasks
            .into_iter()
            .enumerate()
            .map(|(index, task)| async move { (index, task.await) }),
    )
    .buffer_unordered(parallelism.max(1));
    while let Some((index, result)) = stream.next().await {
        results[index] = Some(result);
    }
    results
        .into_iter()
        .map(|r| r.unwrap_or_else(|| Err(CoreError::Write("write task vanished".to_string()))))
        .collect()
}

/// Output of one parquet base-file or log-file write task.
pub(crate) struct FileTaskOutput {
    pub size: i64,
    /// Footer column ranges when column_stats collection was requested.
    pub ranges: Option<Vec<crate::metadata::table::column_stats::ColumnRangeStats>>,
}

/// A base-file write task: encode on a blocking thread, then put.
pub(crate) fn parquet_file_task(
    storage: std::sync::Arc<Storage>,
    props: parquet::file::properties::WriterProperties,
    prepared: Vec<arrow_array::RecordBatch>,
    relative_path: String,
    collect_ranges: bool,
) -> futures::future::BoxFuture<'static, Result<FileTaskOutput>> {
    Box::pin(async move {
        let (bytes, ranges) = tokio::task::spawn_blocking(move || -> Result<_> {
            let bytes = crate::write::append::write_parquet_bytes_with_props(props, &prepared)?;
            let ranges = if collect_ranges {
                Some(
                    crate::metadata::table::column_stats::column_ranges_from_parquet_bytes(&bytes)?,
                )
            } else {
                None
            };
            Ok((bytes, ranges))
        })
        .await
        .map_err(|e| CoreError::Write(format!("write task panicked: {e}")))??;
        let size = bytes.len() as i64;
        storage.put_file(&relative_path, bytes).await?;
        Ok(FileTaskOutput { size, ranges })
    })
}

/// A MOR log-file write task: encode the parquet payload on a blocking
/// thread, wrap it into a `ParquetData` log block, then put.
pub(crate) fn log_file_task(
    storage: std::sync::Arc<Storage>,
    props: parquet::file::properties::WriterProperties,
    prepared: Vec<arrow_array::RecordBatch>,
    relative_path: String,
    instant: String,
    schema_json: String,
    collect_ranges: bool,
) -> futures::future::BoxFuture<'static, Result<FileTaskOutput>> {
    Box::pin(async move {
        let (block, ranges) = tokio::task::spawn_blocking(move || -> Result<_> {
            let parquet = crate::write::append::write_parquet_bytes_with_props(props, &prepared)?;
            let ranges = if collect_ranges {
                Some(
                    crate::metadata::table::column_stats::column_ranges_from_parquet_bytes(
                        &parquet,
                    )?,
                )
            } else {
                None
            };
            let block = LogFileWriter::write_log_block(
                BlockType::ParquetData,
                HashMap::from([
                    (BlockMetadataKey::InstantTime, instant),
                    (BlockMetadataKey::Schema, schema_json),
                ]),
                &parquet,
            );
            Ok((block, ranges))
        })
        .await
        .map_err(|e| CoreError::Write(format!("write task panicked: {e}")))??;
        let size = block.len() as i64;
        storage.put_file(&relative_path, block).await?;
        Ok(FileTaskOutput { size, ranges })
    })
}

/// Completion-side bookkeeping, run inside the second critical section after
/// the action completes: marker cleanup, then archival of the data and MDT
/// timelines (user design; Java archives post-commit outside the lock).
pub(crate) async fn post_complete_bookkeeping(
    table: &crate::table::Table,
    storage: &Storage,
    instant: &str,
) -> Result<()> {
    markers::delete_marker_dir(storage, instant).await;
    let options = table.hudi_configs.as_options();
    let min_keep = options
        .get("hoodie.keep.min.commits")
        .and_then(|v| v.parse().ok())
        .unwrap_or(archival::DEFAULT_KEEP_MIN_COMMITS);
    let max_keep = options
        .get("hoodie.keep.max.commits")
        .and_then(|v| v.parse().ok())
        .unwrap_or(archival::DEFAULT_KEEP_MAX_COMMITS);
    archival::archive_timeline_if_needed(
        storage,
        &crate::write::append::timeline_dir(table),
        min_keep,
        max_keep,
    )
    .await?;
    // The MDT's own timeline is NOT archived: like Java, MDT archival is
    // bounded by the latest MDT compaction (file slices need every instant
    // since the last base file), and we do not compact the MDT yet.
    Ok(())
}

/// Inflight instant metadata: a `HoodieCommitMetadata` with the operation type
/// and empty stats (Java writes the workload profile here; readers only need a
/// parseable container).
pub(crate) fn inflight_commit_metadata_bytes(operation: &str, layout_two: bool) -> Result<Vec<u8>> {
    let metadata = crate::metadata::commit::HoodieCommitMetadata {
        version: Some(1),
        operation_type: Some(operation.to_string()),
        partition_to_write_stats: Some(std::collections::HashMap::new()),
        compacted: Some(false),
        extra_metadata: Some(std::collections::HashMap::new()),
    };
    if layout_two {
        metadata.to_avro_bytes()
    } else {
        metadata.to_json_bytes()
    }
}

/// Build a v3 `BlockType::Delete` log block (`HoodieDeleteRecordList` Avro payload).
///
/// `ordering_val` of `0` matches Java's commit-time / unknown-ordering fallback so a later
/// re-insert is not permanently shadowed under event-time merge.
pub(crate) fn build_delete_log_block(
    instant: &str,
    keys: &[(String, String)],
    ordering_val: i64,
) -> Result<Vec<u8>> {
    let records = keys
        .iter()
        .map(|(record_key, partition_path)| {
            AvroValue::Record(vec![
                (
                    "recordKey".to_string(),
                    AvroValue::Union(1, Box::new(AvroValue::String(record_key.clone()))),
                ),
                (
                    "partitionPath".to_string(),
                    AvroValue::Union(1, Box::new(AvroValue::String(partition_path.clone()))),
                ),
                (
                    "orderingVal".to_string(),
                    // Java wraps ordering primitives in wrapper records inside
                    // the union. `HoodieRecord.DEFAULT_ORDERING_VALUE` (0,
                    // commit-time fallback) is an Integer serialized via
                    // IntWrapper (union position 2); readers only treat the
                    // Int32 zero as the default. A real bigint ordering value
                    // goes through LongWrapper (position 3).
                    if ordering_val == 0 {
                        AvroValue::Union(
                            2,
                            Box::new(AvroValue::Record(vec![(
                                "value".to_string(),
                                AvroValue::Int(0),
                            )])),
                        )
                    } else {
                        AvroValue::Union(
                            3,
                            Box::new(AvroValue::Record(vec![(
                                "value".to_string(),
                                AvroValue::Long(ordering_val),
                            )])),
                        )
                    },
                ),
            ])
        })
        .collect();
    let payload = to_avro_datum(
        avro_schema_for_delete_record_list()?,
        AvroValue::Record(vec![(
            "deleteRecordList".to_string(),
            AvroValue::Array(records),
        )]),
    )?;
    let mut content = Vec::with_capacity(8 + payload.len());
    content.extend_from_slice(&3u32.to_be_bytes());
    content.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    content.extend_from_slice(&payload);
    Ok(LogFileWriter::write_log_block(
        BlockType::Delete,
        HashMap::from([
            (BlockMetadataKey::InstantTime, instant.to_string()),
            (
                BlockMetadataKey::Schema,
                delete_record_list_avro_schema_json().to_string(),
            ),
        ]),
        &content,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two writers fencing the same instant: the second must get a conflict,
    /// not silently share the instant.
    #[tokio::test]
    async fn test_fence_timeline_instant_conflicts_on_same_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let base_url = url::Url::from_directory_path(dir.path()).unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();

        fence_timeline_instant(
            &storage,
            ".hoodie/timeline",
            "20260101000000000",
            Action::Commit,
            Vec::new(),
            Vec::new(),
        )
        .await
        .unwrap();

        let err = fence_timeline_instant(
            &storage,
            ".hoodie/timeline",
            "20260101000000000",
            Action::Commit,
            Vec::new(),
            Vec::new(),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("already exists"),
            "expected instant conflict, got: {err}"
        );
    }

    /// Non-zero ordering values take the LongWrapper union branch; zero takes
    /// IntWrapper (Java DEFAULT_ORDERING_VALUE). Both must produce parseable
    /// delete blocks.
    #[test]
    fn test_build_delete_log_block_both_ordering_branches() {
        for ordering in [0i64, 42i64] {
            let block = build_delete_log_block(
                "20260101000000000",
                &[("k1".to_string(), "p".to_string())],
                ordering,
            )
            .unwrap();
            assert!(!block.is_empty());
            assert_eq!(&block[..6], b"#HUDI#");
        }
    }

    /// Bare-path partition metadata: empty partition is a no-op; a real
    /// partition writes the metafile once and never rewrites it.
    #[tokio::test]
    async fn test_ensure_partition_metadata_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let base_url = url::Url::from_directory_path(dir.path()).unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        ensure_partition_metadata(&storage, "", "20260101000000000")
            .await
            .unwrap();
        ensure_partition_metadata(&storage, "city=sf", "20260101000000000")
            .await
            .unwrap();
        let meta = dir.path().join("city=sf/.hoodie_partition_metadata");
        let first = std::fs::read_to_string(&meta).unwrap();
        assert!(first.contains("commitTime=20260101000000000"));
        assert!(first.contains("partitionDepth=1"));
        ensure_partition_metadata(&storage, "city=sf", "20270101000000000")
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(&meta).unwrap(), first);
    }

    /// Inflight commit metadata bytes: Avro OCF for layout v2, JSON for v1.
    #[test]
    fn test_inflight_commit_metadata_bytes_both_layouts() {
        let avro = inflight_commit_metadata_bytes("UPSERT", true).unwrap();
        assert_eq!(&avro[..4], b"Obj\x01");
        let json = inflight_commit_metadata_bytes("UPSERT", false).unwrap();
        assert!(std::str::from_utf8(&json).unwrap().contains("UPSERT"));
    }
}
