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

pub(crate) mod append;
pub(crate) mod archival;
mod create;
mod keygen;
pub mod lock;
pub(crate) mod markers;
pub(crate) mod metadata;
mod rewrite;
pub(crate) mod rollback;
pub(crate) mod sizing;

pub(crate) use append::append_batches;
pub(crate) use append::set_commit_timezone;
pub use create::TableCreateBuilder;
pub(crate) use keygen::{
    complex_keygen_validation_enabled, ensure_complex_keygen_validated,
    ensure_supported_key_generator,
};
pub use lock::{InProcessLockProvider, LockLease, LockProvider};
pub(crate) use rewrite::ensure_writable_table_version;
pub use rewrite::{UpsertOptions, WriteResult};
pub(crate) use rewrite::{
    delete_filter, delete_keys, dynamic_partition_overwrite_batches, overwrite_batches,
    update_filter, upsert_batches,
};

use std::collections::HashMap;
use std::str::FromStr;

use apache_avro::to_avro_datum;
use apache_avro::types::Value as AvroValue;

use crate::Result;
use crate::error::CoreError;
use crate::file_group::log_file::writer::LogFileWriter;
use crate::file_group::log_file::{BlockMetadataKey, BlockType};
use crate::metadata::meta_field::MetaField;
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

/// Drops the writer-managed meta columns; `_hoodie_is_deleted` and other
/// `_hoodie_`-prefixed data columns stay.
pub(crate) fn strip_meta_columns(
    batch: &arrow_array::RecordBatch,
) -> Result<arrow_array::RecordBatch> {
    let keep: Vec<usize> = (0..batch.num_columns())
        .filter(|&i| !MetaField::is_meta_field(batch.schema().field(i).name()))
        .collect();
    batch.project(&keep).map_err(CoreError::ArrowError)
}

/// Normalizes caller batches at a public write entry point.
///
/// Meta columns are dropped as in a non-prepped Java write, since the writer
/// assigns them. Each column is cast to the type the table schema reads back
/// (see [`normalize_type`]), so a later write with the same input schema
/// matches the table and every file carries the commit schema's types. The
/// first write is included so that every file of the table agrees, which a
/// reader taking each file's own schema needs in order to combine them.
pub(crate) fn normalize_for_write(
    batches: &[arrow_array::RecordBatch],
) -> Result<Vec<arrow_array::RecordBatch>> {
    batches.iter().map(normalize_write_batch).collect()
}

pub(crate) fn normalize_write_batch(
    batch: &arrow_array::RecordBatch,
) -> Result<arrow_array::RecordBatch> {
    use arrow_schema::Field;
    let batch = strip_meta_columns(batch)?;
    let mut changed = false;
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let target = normalize_type(field.data_type());
        if &target == field.data_type() {
            fields.push(field.clone());
            columns.push(column.clone());
            continue;
        }
        changed = true;
        // A lossy cast must fail rather than write nulls.
        let options = arrow::compute::CastOptions {
            safe: false,
            ..Default::default()
        };
        let casted = arrow::compute::cast_with_options(column.as_ref(), &target, &options)
            .map_err(|e| {
                CoreError::Schema(format!(
                    "cannot convert column '{}' from {} to {target} for writing: {e}",
                    field.name(),
                    field.data_type()
                ))
            })?;
        fields.push(std::sync::Arc::new(
            Field::new(field.name(), target, field.is_nullable())
                .with_metadata(field.metadata().clone()),
        ));
        columns.push(casted);
    }
    if !changed {
        return Ok(batch);
    }
    let schema = std::sync::Arc::new(arrow_schema::Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    arrow_array::RecordBatch::try_new(schema, columns)
        .map_err(|e| CoreError::Schema(format!("cannot rebuild batch for writing: {e}")))
}

/// The type `dt` reads back as after [`append::arrow_schema_to_avro_json`]
/// stores it, at any depth. Avro has one type each for the narrow ints,
/// strings, bytes, zoned timestamps, decimals up to 38 digits and lists, and
/// its `array` reads back with an `element` item while PyArrow and most Arrow
/// producers name it `item`.
fn normalize_type(dt: &arrow_schema::DataType) -> arrow_schema::DataType {
    use arrow_schema::{DataType, Field};
    let normalize_field = |f: &std::sync::Arc<Field>, name: &str| {
        std::sync::Arc::new(
            Field::new(name, normalize_type(f.data_type()), f.is_nullable())
                .with_metadata(f.metadata().clone()),
        )
    };
    match dt {
        DataType::Int8 | DataType::Int16 => DataType::Int32,
        DataType::LargeUtf8 => DataType::Utf8,
        DataType::LargeBinary => DataType::Binary,
        DataType::Timestamp(unit, Some(_)) => DataType::Timestamp(*unit, Some("UTC".into())),
        DataType::Decimal256(precision, scale) if *precision <= 38 => {
            DataType::Decimal128(*precision, *scale)
        }
        DataType::FixedSizeList(item, _) | DataType::List(item) | DataType::LargeList(item) => {
            DataType::List(normalize_field(item, "element"))
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| normalize_field(f, f.name()))
                .collect(),
        ),
        DataType::Map(entries, sorted) => {
            DataType::Map(normalize_field(entries, entries.name()), *sorted)
        }
        other => other.clone(),
    }
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
    // Collected rather than mapped lazily: a stream that projects through the
    // closure's output defeats the compiler's Send check on callers' futures.
    let indexed: Vec<_> = tasks
        .into_iter()
        .enumerate()
        .map(|(index, task)| async move { (index, task.await) })
        .collect();
    let mut stream = futures::stream::iter(indexed).buffer_unordered(parallelism.max(1));
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
    bloom: Option<crate::bloom::BloomFilterConfig>,
    prepared: Vec<arrow_array::RecordBatch>,
    relative_path: String,
    collect_ranges: bool,
) -> futures::future::BoxFuture<'static, Result<FileTaskOutput>> {
    Box::pin(async move {
        let (bytes, ranges) = tokio::task::spawn_blocking(move || -> Result<_> {
            let bytes =
                crate::write::append::write_parquet_bytes_with_props(props, bloom, &prepared)?;
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
            let parquet =
                crate::write::append::write_parquet_bytes_with_props(props, None, &prepared)?;
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
    let history_path: String = table
        .hudi_configs
        .get_or_default(crate::config::table::HudiTableConfig::TimelineHistoryPath)
        .into();
    archival::archive_timeline_if_needed(
        storage,
        &crate::write::append::timeline_dir(table),
        &history_path,
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
        partition_to_replace_file_ids: None,
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

/// What a write adds to the metadata table when it completes.
#[derive(Default)]
pub(crate) struct MetadataTableUpdates {
    /// `files` partition entries as (partition path, file name, size, is_deleted).
    pub files: Vec<(String, String, i64, bool)>,
    pub record_index: Vec<crate::metadata::table::encode::RecordIndexEntry>,
    pub column_stats: Vec<metadata::StatsFileUpdate>,
    /// Base files this write replaced or rewrote. The column stats update
    /// leaves them out of its partition_stats scan; they stay listed in the
    /// `files` partition until clean, like Java, because readers exclude
    /// replaced file groups through the replacecommit metadata and deleting
    /// them here would break time travel.
    pub replaced_paths: Vec<String>,
}

/// Group flat write stats by partition path, the shape commit metadata carries.
pub(crate) fn stats_by_partition(
    stats: Vec<crate::metadata::commit::HoodieWriteStat>,
) -> HashMap<String, Vec<crate::metadata::commit::HoodieWriteStat>> {
    let mut by_partition: HashMap<String, Vec<_>> = HashMap::new();
    for stat in stats {
        let partition = stat.partition_path.clone().unwrap_or_default();
        by_partition.entry(partition).or_default().push(stat);
    }
    by_partition
}

/// The bytes a completed data instant carries: commit metadata, or
/// replacecommit metadata when `partition_to_replace_file_ids` is given, as
/// Avro on timeline layout 2 and JSON otherwise. The recorded schema carries
/// no `_hoodie_*` meta fields, like the Java writer; callers may pass batches
/// that were read back with them.
pub(crate) fn commit_metadata_bytes(
    table: &crate::table::Table,
    operation: &str,
    partition_to_write_stats: HashMap<String, Vec<crate::metadata::commit::HoodieWriteStat>>,
    partition_to_replace_file_ids: Option<HashMap<String, Vec<String>>>,
    schema: &arrow_schema::Schema,
) -> Result<Vec<u8>> {
    let schema_json =
        append::arrow_schema_to_avro_json(&append::strip_meta_fields_from_schema(schema))?;
    let extra_metadata = Some(HashMap::from([("schema".to_string(), schema_json)]));
    let layout_two = append::is_layout_two(table);
    match partition_to_replace_file_ids {
        Some(partition_to_replace_file_ids) => {
            let metadata = crate::metadata::replace_commit::HoodieReplaceCommitMetadata {
                version: Some(1),
                operation_type: Some(operation.to_string()),
                partition_to_write_stats: Some(partition_to_write_stats),
                compacted: Some(false),
                extra_metadata,
                partition_to_replace_file_ids: Some(partition_to_replace_file_ids),
            };
            if layout_two {
                metadata.to_avro_bytes()
            } else {
                metadata.to_json_bytes()
            }
        }
        None => {
            let metadata = crate::metadata::commit::HoodieCommitMetadata {
                version: Some(1),
                operation_type: Some(operation.to_string()),
                partition_to_write_stats: Some(partition_to_write_stats),
                compacted: Some(false),
                extra_metadata,
                partition_to_replace_file_ids: None,
            };
            if layout_two {
                metadata.to_avro_bytes()
            } else {
                metadata.to_json_bytes()
            }
        }
    }
}

/// Complete a write whose data files are already on storage, in the order
/// crash consistency needs: metadata table updates first, then under the lock
/// the metadata table deltacommit, the completed data instant and post-commit
/// bookkeeping, then a reload of this handle's timeline. Finishing the data
/// timeline last lets readers fence metadata table contents by completed data
/// instants. The metadata table log writes are work and run unlocked; only
/// completion runs in the critical section.
///
/// A failure before the completed instant is stored deletes `written_paths`.
/// An errored PUT of the instant may still have stored it, so the files stay
/// and the next write's rollback removes them only if the instant is still
/// pending. From the stored instant on, bookkeeping and the reload are best
/// effort: a transient failure must not turn a committed write into an error
/// the caller would retry.
///
/// Returns the completed instant's path relative to the table base and its
/// completion time (timeline layout v2 only).
pub(crate) async fn complete_write(
    table: &mut crate::table::Table,
    instant: &str,
    action: Action,
    commit_bytes: Vec<u8>,
    schema: &arrow_schema::Schema,
    mut updates: MetadataTableUpdates,
    written_paths: &[String],
) -> Result<CompletedWrite> {
    let storage = table.file_system_view.storage.clone();
    let mut mdt_stats = HashMap::new();
    if table.is_metadata_table_enabled() {
        let mdt_result = async {
            if !updates.files.is_empty() {
                mdt_stats.insert(
                    "files".to_string(),
                    metadata::update_files_partition_entries(
                        storage.as_ref(),
                        instant,
                        &updates.files,
                    )
                    .await?,
                );
            }
            if crate::index::is_record_index_enabled(table) && !updates.record_index.is_empty() {
                let stats = metadata::update_record_index(
                    storage.as_ref(),
                    instant,
                    std::mem::take(&mut updates.record_index),
                    write_task_parallelism(table),
                )
                .await?;
                if !stats.is_empty() {
                    mdt_stats.insert("record_index".to_string(), stats);
                }
            }
            if metadata::is_column_stats_enabled(table) && !updates.column_stats.is_empty() {
                let stats = metadata::update_column_stats_partitions(
                    table,
                    instant,
                    &updates.column_stats,
                    schema,
                    &updates.replaced_paths,
                )
                .await?;
                mdt_stats.extend(stats);
            }
            Ok::<(), CoreError>(())
        }
        .await;
        if let Err(error) = mdt_result {
            for path in written_paths {
                let _ = storage.delete_file(path).await;
            }
            return Err(error);
        }
    }

    let lock = lock::lock_provider_for(table);
    let critical_section = lock.lock().await?;
    if table.is_metadata_table_enabled()
        && let Err(error) =
            metadata::write_metadata_commit(storage.as_ref(), instant, mdt_stats).await
    {
        for path in written_paths {
            let _ = storage.delete_file(path).await;
        }
        return Err(error);
    }
    let completion = if append::is_layout_two(table) {
        Some(append::generate_instant_time().await)
    } else {
        None
    };
    let completed = Instant::new_completed(instant.to_string(), action, completion.clone())?;
    let commit_relative_path = completed.relative_path_with_base(&append::timeline_dir(table))?;
    storage
        .put_file_if_absent(&commit_relative_path, commit_bytes)
        .await?;
    let _ = post_complete_bookkeeping(table, storage.as_ref(), instant).await;
    drop(critical_section);

    let _ = table.timeline.reload_completed_commits().await;
    table.clear_read_caches();
    Ok(CompletedWrite {
        commit_relative_path,
        completion_time: completion,
    })
}

/// What [`complete_write`] stored on the timeline.
pub(crate) struct CompletedWrite {
    /// Path of the completed instant, relative to the table base.
    pub commit_relative_path: String,
    /// Completion time of the instant on timeline layout v2; `None` on v1,
    /// which records none.
    pub completion_time: Option<String>,
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

    fn list_of(name: &str, item: arrow_schema::DataType) -> arrow_schema::DataType {
        arrow_schema::DataType::List(std::sync::Arc::new(arrow_schema::Field::new(
            name, item, true,
        )))
    }

    #[test]
    fn test_normalize_type_renames_items_and_widens_fixed_size_lists_at_any_depth() {
        use arrow_schema::{DataType, Field, Fields};
        let item = std::sync::Arc::new(Field::new("item", DataType::Float32, true));
        let element = list_of("element", DataType::Float32);

        assert_eq!(
            normalize_type(&DataType::FixedSizeList(item.clone(), 2)),
            element
        );
        assert_eq!(normalize_type(&list_of("item", DataType::Float32)), element);
        assert_eq!(normalize_type(&DataType::LargeList(item.clone())), element);
        assert_eq!(
            normalize_type(&list_of("item", DataType::FixedSizeList(item.clone(), 2))),
            list_of("element", element.clone())
        );

        // Struct children and map entries keep their names; only list items change.
        let nested = |vec: DataType| {
            let entries = Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", vec.clone(), true),
                ])),
                false,
            );
            DataType::Struct(Fields::from(vec![
                Field::new("vec", vec, true),
                Field::new(
                    "tags",
                    DataType::Map(std::sync::Arc::new(entries), false),
                    true,
                ),
            ]))
        };
        assert_eq!(
            normalize_type(&nested(DataType::FixedSizeList(item, 2))),
            nested(element.clone())
        );

        assert_eq!(normalize_type(&DataType::Int64), DataType::Int64);
        assert_eq!(normalize_type(&element), element);
    }

    #[test]
    fn test_normalize_write_batch_casts_list_columns_and_keeps_the_rest() {
        use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float32Array, Int64Array};
        use arrow_schema::{DataType, Field, Schema};
        let item = std::sync::Arc::new(Field::new("item", DataType::Float32, true));
        let embedding = FixedSizeListArray::new(
            item.clone(),
            2,
            std::sync::Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            Some(vec![true, false].into()),
        );
        let field_metadata = HashMap::from([("k".to_string(), "v".to_string())]);
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("embedding", DataType::FixedSizeList(item, 2), true)
                .with_metadata(field_metadata.clone()),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                std::sync::Arc::new(embedding),
            ],
        )
        .unwrap();

        let normalized = normalize_write_batch(&batch).unwrap();
        let field = normalized.schema().field(1).clone();
        assert_eq!(field.data_type(), &list_of("element", DataType::Float32));
        assert_eq!(field.metadata(), &field_metadata);
        assert_eq!(normalized.schema().field(0), batch.schema().field(0));
        let list = normalized
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        assert!(list.is_null(1));
        let first = list.value(0);
        let first = first.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(first.values(), &[1.0, 2.0]);

        // A batch without lists, or already normalized, comes back unchanged.
        assert_eq!(normalize_write_batch(&normalized).unwrap(), normalized);
        let ids = batch.project(&[0]).unwrap();
        assert_eq!(normalize_write_batch(&ids).unwrap(), ids);
    }

    /// Normalized input types must equal what the Avro commit schema reads
    /// back as, so this pins the mapping to the real round trip.
    #[test]
    fn test_normalized_types_match_avro_read_back() {
        use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
        let item = |data_type| std::sync::Arc::new(Field::new("item", data_type, false));
        let schema = Schema::new(vec![
            Field::new("i8", DataType::Int8, false),
            Field::new("i16", DataType::Int16, false),
            Field::new("s", DataType::LargeUtf8, false),
            Field::new("b", DataType::LargeBinary, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+05:00".into())),
                false,
            ),
            Field::new("d", DataType::Decimal256(10, 2), false),
            Field::new("l", DataType::List(item(DataType::Int16)), false),
            Field::new("ll", DataType::LargeList(item(DataType::LargeUtf8)), false),
            Field::new(
                "nl",
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Int16,
                    true,
                ))),
                false,
            ),
            Field::new(
                "st",
                DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int8, false)])),
                false,
            ),
        ]);
        let batch = arrow_array::RecordBatch::new_empty(std::sync::Arc::new(schema.clone()));
        let normalized = normalize_for_write(&[batch]).unwrap().remove(0).schema();
        let read_back = crate::schema::resolver::avro_json_to_arrow_schema(
            &append::arrow_schema_to_avro_json(&schema).unwrap(),
        )
        .unwrap();
        for (actual, expected) in normalized.fields().iter().zip(read_back.fields()) {
            assert_eq!(
                actual.data_type(),
                expected.data_type(),
                "{}",
                actual.name()
            );
        }
    }
}
