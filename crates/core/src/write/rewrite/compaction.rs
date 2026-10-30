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

//! Merge-on-read compaction: every latest file slice that carries log files
//! is folded into a new base file under one compaction instant.
//!
//! The plan is written into the `.compaction.requested` instant as Java's
//! `HoodieCompactionPlan`, so a crashed compaction is recognizable to either
//! engine, and the instant completes as a `commit` whose metadata is marked
//! compacted, as Java's does. Scheduling and execution happen in one call:
//! with one writer there is nothing to interleave between them.

use super::*;
use crate::file_group::base_file::BaseFile;
use crate::file_group::file_slice::FileSlice;
use crate::file_group::log_file::LogFile;
use crate::metadata::compaction::{CompactionOperation, CompactionPlan, CompactionStrategy};
use crate::write::markers::Marker;

/// Result of a compaction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompactionResult {
    /// Requested time of the compaction commit's instant, or `None` when no
    /// file group carried log files and nothing was written.
    pub requested_time: Option<String>,
    /// Completion time of the instant on timeline layout v2; `None` on
    /// layout v1 and when nothing was written.
    pub completion_time: Option<String>,
    /// File groups that received a new base file.
    pub num_file_groups: usize,
    /// Log files folded into those base files.
    pub num_log_files: usize,
    /// Rows written to the new base files.
    pub num_writes: usize,
}

/// Java `hoodie.compact.inline`: compact in the write call once the delta
/// commit threshold is reached.
const INLINE_COMPACT: &str = "hoodie.compact.inline";
/// Spark's `hoodie.datasource.compaction.async.enable`: compact on a
/// background task once the threshold is reached, while writes continue.
const ASYNC_COMPACT: &str = "hoodie.datasource.compaction.async.enable";
/// Java `hoodie.compact.inline.max.delta.commits`: the threshold both modes
/// share, as in Java's `ScheduleCompactionActionExecutor`.
const COMPACT_NUM_DELTA_COMMITS: &str = "hoodie.compact.inline.max.delta.commits";
const DEFAULT_COMPACT_NUM_DELTA_COMMITS: usize = 5;

type CompactionHandle = tokio::task::JoinHandle<Result<CompactionResult>>;

/// Background compactions by table base path, one at most per table.
fn async_compactions() -> &'static std::sync::Mutex<HashMap<String, CompactionHandle>> {
    static REGISTRY: std::sync::OnceLock<std::sync::Mutex<HashMap<String, CompactionHandle>>> =
        std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn table_base_path(table: &Table) -> String {
    table
        .hudi_configs
        .get_or_default(crate::config::table::HudiTableConfig::BasePath)
        .into()
}

/// Whether a background compaction of the table is still running.
fn async_compaction_running(base_path: &str) -> bool {
    let registry = async_compactions()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry
        .get(base_path)
        .is_some_and(|handle| !handle.is_finished())
}

/// Waits for the table's background compaction, if one was started, and
/// returns its outcome.
pub(crate) async fn await_async_compaction(table: &Table) -> Result<Option<CompactionResult>> {
    let handle = async_compactions()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&table_base_path(table));
    match handle {
        Some(handle) => handle
            .await
            .map_err(|e| CoreError::Write(format!("background compaction failed: {e}")))?
            .map(Some),
        None => Ok(None),
    }
}

pub(crate) async fn compact_table(table: &mut Table) -> Result<CompactionResult> {
    if !table.is_mor() {
        return Err(CoreError::Unsupported(format!(
            "compaction applies to MERGE_ON_READ tables; '{}' is {}",
            table.table_name(),
            table.table_type()
        )));
    }
    ensure_mor_merge_supported(table)?;
    // Two compactions of one table would each read the other's pending
    // instant as crashed and roll it back.
    let _running = CompactionInProgress::acquire(table_base_path(table))?;
    table.reload_timeline_for_compaction().await?;
    match request_compaction(table).await? {
        Some((instant, slices)) => execute_compaction(table, &instant, slices).await,
        None => Ok(CompactionResult::default()),
    }
}

/// Tables with a compaction in progress in this process; the guard releases
/// on drop.
struct CompactionInProgress(String);

impl CompactionInProgress {
    fn acquire(base_path: String) -> Result<Self> {
        let mut guard = Self::registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !guard.insert(base_path.clone()) {
            return Err(CoreError::Write(format!(
                "a compaction of table '{base_path}' is already running"
            )));
        }
        drop(guard);
        Ok(Self(base_path))
    }

    fn registry() -> &'static std::sync::Mutex<HashSet<String>> {
        static IN_PROGRESS: std::sync::OnceLock<std::sync::Mutex<HashSet<String>>> =
            std::sync::OnceLock::new();
        IN_PROGRESS.get_or_init(|| std::sync::Mutex::new(HashSet::new()))
    }
}

impl Drop for CompactionInProgress {
    fn drop(&mut self) {
        Self::registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.0);
    }
}

/// After a merge-on-read delta commit, compact when a trigger is configured
/// and the completed delta commits since the latest compaction reach the
/// threshold: in the write call under `hoodie.compact.inline`, or on a
/// background task under `hoodie.datasource.compaction.async.enable`, which
/// the next write skips while the previous one still runs. Both are off by
/// default. The delta commit is already durable, so an inline failure is
/// logged rather than returned and the next delta commit tries again; a
/// background failure surfaces from [`await_async_compaction`].
pub(crate) async fn maybe_compact_inline(table: &mut Table) {
    let options = table.hudi_configs.as_options();
    let is_on = |key: &str| {
        options
            .get(key)
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"))
    };
    let (inline, background) = (is_on(INLINE_COMPACT), is_on(ASYNC_COMPACT));
    if !inline && !background {
        return;
    }
    let max_delta_commits = options
        .get(COMPACT_NUM_DELTA_COMMITS)
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(DEFAULT_COMPACT_NUM_DELTA_COMMITS);
    if delta_commits_since_last_compaction(table) < max_delta_commits {
        return;
    }
    if inline {
        match compact_table(table).await {
            Ok(CompactionResult {
                requested_time: Some(instant),
                num_file_groups,
                num_log_files,
                ..
            }) => log::info!(
                "inline compaction {instant} folded {num_log_files} log files of {num_file_groups} file groups"
            ),
            Ok(_) => {}
            Err(error) => log::warn!("inline compaction failed: {error}"),
        }
        return;
    }
    let base_path = table_base_path(table);
    if async_compaction_running(&base_path) {
        return;
    }
    // The task gets its own handle: this one goes back to the caller, whose
    // next write must not see the compaction's half-built state.
    let options: Vec<(String, String)> = table
        .hudi_options()
        .into_iter()
        .chain(table.storage_options())
        .collect();
    let task_base_path = base_path.clone();
    let handle = tokio::spawn(async move {
        let mut table = Table::new_with_options(&task_base_path, options).await?;
        compact_table(&mut table).await
    });
    let previous = async_compactions()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(base_path, handle);
    // A finished compaction nobody awaited: its outcome is only logged.
    if let Some(Some(outcome)) = previous.map(futures::FutureExt::now_or_never) {
        match outcome {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => log::warn!("background compaction failed: {error}"),
            Err(error) => log::warn!("background compaction failed: {error}"),
        }
    }
}

/// Completed delta commits after the latest completed compaction, which on
/// a merge-on-read table is the latest `commit` (Java
/// `getDeltaCommitsSinceLatestCompaction`).
fn delta_commits_since_last_compaction(table: &Table) -> usize {
    table
        .timeline
        .completed_commits
        .iter()
        .rev()
        .take_while(|instant| instant.action != Action::Commit)
        .filter(|instant| instant.action == Action::DeltaCommit)
        .count()
}

fn compaction_plan(slices: &[FileSlice]) -> CompactionPlan {
    let operations = slices
        .iter()
        .map(|slice| {
            let mut metrics =
                HashMap::from([("TOTAL_LOG_FILES".to_string(), slice.log_files.len() as f64)]);
            if let Some(size) = slice.log_size_bytes() {
                metrics.insert("TOTAL_LOG_FILES_SIZE".to_string(), size as f64);
            }
            CompactionOperation {
                base_instant_time: Some(slice.creation_instant_time().to_string()),
                delta_file_paths: Some(slice.log_files.iter().map(LogFile::file_name).collect()),
                data_file_path: slice.base_file.as_ref().map(BaseFile::file_name),
                file_id: Some(slice.file_id().to_string()),
                partition_path: Some(slice.partition_path.clone()),
                metrics: Some(metrics),
                bootstrap_file_path: None,
            }
        })
        .collect();
    CompactionPlan {
        operations: Some(operations),
        extra_metadata: Some(HashMap::from([(
            CompactionPlan::COMPACTOR_KEY.to_string(),
            CompactionPlan::HUDI_RS_COMPACTOR.to_string(),
        )])),
        version: Some(CompactionPlan::LATEST_VERSION),
        strategy: Some(CompactionStrategy {
            compactor_class_name: None,
            strategy_params: Some(HashMap::new()),
            version: Some(1),
        }),
        // Rows keep the commit time and sequence number of the delta commit
        // that wrote them, as Java's compactor preserves them.
        preserve_hoodie_metadata: Some(true),
        missing_schedule_partitions: None,
    }
}

/// Critical section 1 for a compaction: list the file slices to compact,
/// mint the instant and write the plan into its requested file, with an empty
/// inflight file, as Java writes them. `None` when no slice has log files.
///
/// The listing happens under the lock: a delta commit completing between it
/// and the minting would complete below the compaction instant, attach its
/// log file to the superseded slice, and drop out of every later read.
async fn request_compaction(table: &mut Table) -> Result<Option<(String, Vec<FileSlice>)>> {
    let storage = table.file_system_view.storage.clone();
    let lock = crate::write::lock::lock_provider_for(table);
    let _cs1 = lock.lock().await?;
    table.timeline.reload_completed_commits().await?;
    table.clear_read_caches();
    let mut slices: Vec<FileSlice> = table
        .get_file_slices(&ReadOptions::new())
        .await?
        .into_iter()
        .filter(FileSlice::has_log_file)
        .collect();
    if slices.is_empty() {
        return Ok(None);
    }
    slices.sort_by(|a, b| {
        (a.partition_path.as_str(), a.file_id()).cmp(&(b.partition_path.as_str(), b.file_id()))
    });
    let instant = generate_instant_time().await;
    crate::write::fence_timeline_instant(
        storage.as_ref(),
        &timeline_dir(table),
        &instant,
        Action::Compaction,
        compaction_plan(&slices).to_avro_bytes()?,
        Vec::new(),
    )
    .await?;
    Ok(Some((instant, slices)))
}

/// What the driver keeps about a file group while its merge worker runs.
struct CompactedGroup {
    partition: String,
    file_id: String,
    out_name: String,
    prev_commit: String,
    /// The slice's files, relative to the table base, which the new base
    /// file supersedes.
    replaced_paths: Vec<String>,
    num_log_files: usize,
    log_size: Option<u64>,
}

async fn execute_compaction(
    table: &mut Table,
    instant: &str,
    slices: Vec<FileSlice>,
) -> Result<CompactionResult> {
    let storage = table.file_system_view.storage.clone();
    let ctx = MergeCtx {
        hudi_configs: table.hudi_configs.clone(),
        storage: storage.clone(),
        reader: Some(std::sync::Arc::new(
            table
                .create_file_group_reader_with_options(
                    Some(&ReadOptions::new()),
                    std::iter::empty::<(&str, String)>(),
                )
                .await?,
        )),
        instant: instant.to_string(),
        props: crate::write::append::parquet_writer_props(table),
        bloom: crate::write::append::base_file_bloom_filter_config(table),
        collect_ranges: is_column_stats_enabled(table),
        event_time: uses_event_time_merge(&table.hudi_configs)?,
        collect_rli_deletes: false,
    };

    let mut groups = Vec::with_capacity(slices.len());
    let mut plans = Vec::with_capacity(slices.len());
    let mut planned_markers = Vec::with_capacity(slices.len());
    for slice in slices {
        let file_id = slice.file_id().to_string();
        let out_name = format!("{file_id}_0-0-0_{instant}.parquet");
        let base_path = slice.base_file_relative_path()?;
        // Java merges into an existing base file and creates one for a
        // log-only group, and names the previous commit accordingly.
        let (marker, prev_commit) = match &base_path {
            Some(_) => (
                Marker::merge(&slice.partition_path, &out_name),
                slice.creation_instant_time().to_string(),
            ),
            None => (
                Marker::create(&slice.partition_path, &out_name),
                "null".to_string(),
            ),
        };
        planned_markers.push(marker);
        let replaced_paths = base_path
            .into_iter()
            .chain(
                slice
                    .log_files
                    .iter()
                    .map(|log| slice.log_file_relative_path(log))
                    .collect::<Result<Vec<_>>>()?,
            )
            .collect();
        groups.push(CompactedGroup {
            partition: slice.partition_path.clone(),
            file_id: file_id.clone(),
            out_name: out_name.clone(),
            prev_commit: prev_commit.clone(),
            replaced_paths,
            num_log_files: slice.log_files.len(),
            log_size: slice.log_size_bytes(),
        });
        plans.push(MergePlan {
            partition: slice.partition_path.clone(),
            file_id,
            out_name,
            prev_commit,
            slice,
            op: GroupOp::Compact,
        });
    }
    crate::write::markers::write_markers(storage.as_ref(), instant, &planned_markers).await?;

    let tasks = plans
        .into_iter()
        .map(|plan| merge_file_group_task(ctx.clone(), plan))
        .collect();
    let results =
        crate::write::run_write_tasks(tasks, crate::write::write_task_parallelism(table)).await;

    let mut additions = Vec::new();
    let mut partition_to_write_stats = HashMap::<String, Vec<HoodieWriteStat>>::new();
    let mut written_paths = Vec::new();
    let mut stats_updates = Vec::new();
    let mut replaced_paths = Vec::new();
    let mut num_writes = 0usize;
    let mut num_log_files = 0usize;
    let mut first_error = None;
    for (group, result) in groups.iter().zip(results) {
        let output = match result {
            Ok(output) => output,
            Err(error) => {
                first_error.get_or_insert(error);
                continue;
            }
        };
        let path = relative_data_path(&group.partition, &group.out_name);
        written_paths.push(path.clone());
        if let Some(ranges) = output.ranges {
            stats_updates.push(crate::write::metadata::StatsFileUpdate {
                partition_path: group.partition.clone(),
                file_name: group.out_name.clone(),
                ranges,
            });
        }
        additions.push((
            group.partition.clone(),
            group.out_name.clone(),
            output.size,
            false,
        ));
        replaced_paths.extend(group.replaced_paths.iter().cloned());
        num_writes += output.num_rows;
        num_log_files += group.num_log_files;
        partition_to_write_stats
            .entry(group.partition.clone())
            .or_default()
            .push(HoodieWriteStat {
                file_id: Some(group.file_id.clone()),
                path: Some(path),
                base_file: Some(group.out_name.clone()),
                prev_commit: Some(group.prev_commit.clone()),
                num_writes: Some(output.num_rows as i64),
                total_write_bytes: Some(output.size),
                file_size_in_bytes: Some(output.size),
                partition_path: Some(group.partition.clone()),
                total_log_files_compacted: Some(group.num_log_files as i64),
                total_log_size_compacted: group.log_size.map(|size| size as i64),
                ..Default::default()
            });
    }
    if let Some(error) = first_error {
        let mut all_deleted = true;
        for path in &written_paths {
            if storage.delete_file(path).await.is_err() {
                all_deleted = false;
            }
        }
        // As in the copy-on-write driver: the fencing files and markers are
        // the trail a rollback follows, so they stay while a data file does.
        if all_deleted {
            let _ = abort_requested_instant(table, instant, Action::Compaction).await;
        }
        return Err(error);
    }

    let schema = table.data_schema_for_read().await?;
    let commit_bytes =
        crate::write::compaction_commit_metadata_bytes(table, partition_to_write_stats, &schema)?;
    let completed = crate::write::complete_write(
        table,
        instant,
        Action::Commit,
        commit_bytes,
        &schema,
        crate::write::MetadataTableUpdates {
            files: additions,
            // Compaction keeps every record in its file group, so the record
            // index has nothing to learn.
            record_index: Vec::new(),
            column_stats: stats_updates,
            replaced_paths,
        },
        &written_paths,
    )
    .await?;
    Ok(CompactionResult {
        requested_time: Some(instant.to_string()),
        completion_time: completed.completion_time,
        num_file_groups: groups.len(),
        num_log_files,
        num_writes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::read::QueryType;
    use crate::config::table::TableTypeValue;
    use crate::metadata::commit::HoodieCommitMetadata;
    use arrow::array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::path::Path;
    use std::sync::Arc;

    fn batch(rows: &[(&str, &str, i64, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    async fn mor_table(path: &Path, options: &[(&str, &str)]) -> Table {
        let mut builder = Table::create(path.to_str().unwrap())
            .with_table_name("t")
            .with_table_type(TableTypeValue::MergeOnRead)
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_ordering_fields(["ts"]);
        for (key, value) in options {
            builder = builder.with_option(*key, *value);
        }
        builder.create().await.unwrap()
    }

    /// `(id, city, ts, v, _hoodie_commit_time, _hoodie_file_name)` by id.
    async fn snapshot(table: &Table) -> Vec<(String, String, i64, i64, String, String)> {
        let mut rows = Vec::new();
        for batch in table.read(&ReadOptions::new()).await.unwrap() {
            let strings = |name: &str| {
                batch
                    .column_by_name(name)
                    .unwrap_or_else(|| panic!("missing column {name}"))
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .clone()
            };
            let ints = |name: &str| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone()
            };
            let (id, city, ts, v) = (strings("id"), strings("city"), ints("ts"), ints("v"));
            let (commit_time, file_name) = (
                strings(MetaField::CommitTime.as_ref()),
                strings(MetaField::FileName.as_ref()),
            );
            for row in 0..batch.num_rows() {
                rows.push((
                    id.value(row).to_string(),
                    city.value(row).to_string(),
                    ts.value(row),
                    v.value(row),
                    commit_time.value(row).to_string(),
                    file_name.value(row).to_string(),
                ));
            }
        }
        rows.sort();
        rows
    }

    fn data_only<T: Clone>(
        rows: &[(String, String, i64, i64, T, T)],
    ) -> Vec<(String, String, i64, i64)> {
        rows.iter()
            .map(|r| (r.0.clone(), r.1.clone(), r.2, r.3))
            .collect()
    }

    fn timeline_files(path: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(path.join(".hoodie/timeline"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .filter(|name| name.chars().next().is_some_and(|c| c.is_ascii_digit()))
            .collect();
        names.sort();
        names
    }

    fn completed_commits(path: &Path) -> Vec<String> {
        timeline_files(path)
            .into_iter()
            .filter(|name| name.ends_with(".commit"))
            .collect()
    }

    async fn latest_slices(table: &Table) -> Vec<FileSlice> {
        let mut slices = table.get_file_slices(&ReadOptions::new()).await.unwrap();
        slices.sort_by(|a, b| a.file_id().cmp(b.file_id()));
        slices
    }

    /// Every file group with log files gets a new base file under one
    /// compaction instant whose plan and commit take Java's shape, rows keep
    /// their delta commit's lineage, and later writes land on the new base.
    #[tokio::test]
    async fn test_compact_folds_log_files_into_new_base_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = mor_table(dir.path(), &[]).await;
        table
            .append([batch(&[
                ("a", "sf", 1, 10),
                ("b", "sf", 1, 20),
                ("c", "ny", 1, 30),
            ])])
            .await
            .unwrap();
        let upsert = table
            .upsert([batch(&[("a", "sf", 2, 11), ("d", "ny", 2, 40)])])
            .await
            .unwrap();
        let delete = table.delete("id = 'c'").await.unwrap();
        let before = snapshot(&table).await;
        assert_eq!(before.len(), 3);
        assert!(
            completed_commits(dir.path()).is_empty(),
            "no compaction runs unless configured or requested"
        );
        let mut with_logs: Vec<_> = latest_slices(&table)
            .await
            .into_iter()
            .filter(FileSlice::has_log_file)
            .collect();
        with_logs.sort_by(|a, b| {
            (a.partition_path.as_str(), a.file_id()).cmp(&(b.partition_path.as_str(), b.file_id()))
        });
        assert_eq!(with_logs.len(), 2, "a's group in sf and c's group in ny");

        let result = table.compact().await.unwrap();
        let instant = result
            .requested_time
            .as_deref()
            .expect("compaction commits");
        assert!(result.completion_time.is_some());
        assert_eq!(result.num_file_groups, 2);
        assert_eq!(result.num_log_files, 2);
        assert_eq!(
            result.num_writes, 2,
            "a and b; c's group is empty after its delete"
        );

        let after = snapshot(&table).await;
        assert_eq!(data_only(&after), data_only(&before));
        let row_a = after.iter().find(|r| r.0 == "a").unwrap();
        assert_eq!(
            Some(row_a.4.clone()),
            upsert.requested_time,
            "compaction preserves commit time"
        );
        assert_eq!(
            row_a.5,
            format!(
                "{}_0-0-0_{}.parquet",
                with_logs
                    .iter()
                    .find(|s| s.partition_path == "city=sf")
                    .unwrap()
                    .file_id(),
                instant
            ),
            "compaction stamps the new base file name"
        );
        let slices = latest_slices(&table).await;
        assert!(slices.iter().all(|s| !s.has_log_file()));
        assert_eq!(
            slices
                .iter()
                .filter(|s| s.creation_instant_time() == instant)
                .count(),
            2
        );

        // Timeline: plan in the requested file, empty inflight, compacted commit.
        let timeline = dir.path().join(".hoodie/timeline");
        let plan = CompactionPlan::from_avro_bytes(
            &std::fs::read(timeline.join(format!("{}.compaction.requested", instant))).unwrap(),
        )
        .unwrap();
        let operations = plan.operations.clone().unwrap();
        assert_eq!(operations.len(), 2);
        for (operation, slice) in operations.iter().zip(&with_logs) {
            assert_eq!(operation.file_id.as_deref(), Some(slice.file_id()));
            assert_eq!(
                operation.partition_path.as_deref(),
                Some(slice.partition_path.as_str())
            );
            assert_eq!(
                operation.delta_file_paths.as_deref().unwrap(),
                slice
                    .log_files
                    .iter()
                    .map(LogFile::file_name)
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                operation.data_file_path,
                slice.base_file.as_ref().map(BaseFile::file_name)
            );
        }
        assert_eq!(plan.preserve_hoodie_metadata, Some(true));
        assert!(plan.is_scheduled_by_hudi_rs());
        assert!(
            std::fs::read(timeline.join(format!("{}.compaction.inflight", instant)))
                .unwrap()
                .is_empty()
        );
        let commit_name = format!(
            "{}_{}.commit",
            instant,
            result.completion_time.as_deref().unwrap()
        );
        assert_eq!(completed_commits(dir.path()), vec![commit_name.clone()]);
        let metadata = HoodieCommitMetadata::from_avro_bytes(
            &std::fs::read(timeline.join(commit_name)).unwrap(),
        )
        .unwrap();
        assert_eq!(metadata.compacted, Some(true));
        assert_eq!(metadata.operation_type.as_deref(), Some("COMPACT"));
        let stats: Vec<_> = metadata.iter_write_stats().map(|(_, s)| s).collect();
        assert_eq!(stats.len(), 2);
        assert!(stats.iter().all(|s| s.total_log_files_compacted == Some(1)));
        assert_eq!(stats.iter().map(|s| s.num_writes.unwrap()).sum::<i64>(), 2);

        // Incremental: the range holding only the compaction yields no rows,
        // since every row keeps its delta commit's time.
        let incremental = table
            .read(
                &ReadOptions::new()
                    .with_query_type(QueryType::Incremental)
                    .with_start_timestamp(delete.completion_time.as_deref().unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(
            incremental.iter().map(RecordBatch::num_rows).sum::<usize>(),
            0
        );

        // A later update is a log on the compacted base, found through the index.
        let later = table.upsert([batch(&[("b", "sf", 3, 21)])]).await.unwrap();
        assert_eq!(later.num_updates, 1);
        let sf = latest_slices(&table)
            .await
            .into_iter()
            .find(|s| s.partition_path == "city=sf")
            .unwrap();
        assert_eq!(
            Some(sf.creation_instant_time()),
            result.requested_time.as_deref()
        );
        assert_eq!(sf.log_files.len(), 1);
        let row_b = snapshot(&table)
            .await
            .into_iter()
            .find(|r| r.0 == "b")
            .unwrap();
        assert_eq!((row_b.3, row_b.4), (21, later.requested_time.unwrap()));
    }

    #[tokio::test]
    async fn test_compact_without_log_files_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = mor_table(dir.path(), &[]).await;
        table.append([batch(&[("a", "sf", 1, 10)])]).await.unwrap();
        let files = timeline_files(dir.path());
        assert_eq!(table.compact().await.unwrap(), CompactionResult::default());
        assert_eq!(timeline_files(dir.path()), files);
    }

    #[tokio::test]
    async fn test_compact_rejects_copy_on_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .create()
            .await
            .unwrap();
        let error = table.compact().await.unwrap_err();
        assert!(
            matches!(error, CoreError::Unsupported(ref message) if message.contains("COPY_ON_WRITE")),
            "{error}"
        );
    }

    /// A compaction a crash left pending is rolled back from its markers
    /// before the next one plans, and that one still compacts everything.
    #[tokio::test]
    async fn test_compact_rolls_back_a_crashed_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = mor_table(dir.path(), &[]).await;
        table.append([batch(&[("a", "sf", 1, 10)])]).await.unwrap();
        table.upsert([batch(&[("a", "sf", 2, 11)])]).await.unwrap();
        let slice = latest_slices(&table).await.remove(0);
        let storage = table.file_system_view.storage.clone();
        let crashed = generate_instant_time().await;
        crate::write::fence_timeline_instant(
            storage.as_ref(),
            &timeline_dir(&table),
            &crashed,
            Action::Compaction,
            compaction_plan(std::slice::from_ref(&slice))
                .to_avro_bytes()
                .unwrap(),
            Vec::new(),
        )
        .await
        .unwrap();
        let dangling = format!("{}_0-0-0_{crashed}.parquet", slice.file_id());
        crate::write::markers::write_markers(
            storage.as_ref(),
            &crashed,
            &[Marker::merge("city=sf", &dangling)],
        )
        .await
        .unwrap();
        let dangling_path = dir.path().join("city=sf").join(&dangling);
        std::fs::write(&dangling_path, b"half a base file").unwrap();

        let result = table.compact().await.unwrap();
        assert_eq!(result.num_file_groups, 1);
        assert!(
            !dangling_path.exists(),
            "the crashed compaction's file is gone"
        );
        let files = timeline_files(dir.path());
        assert!(files.iter().all(|f| !f.starts_with(&crashed)));
        assert!(files.iter().any(|f| f.ends_with(".rollback")));
        assert_eq!(completed_commits(dir.path()).len(), 1);
        assert_eq!(
            data_only(&snapshot(&table).await),
            vec![("a".into(), "sf".into(), 2, 11)]
        );
    }

    /// A pending plan another engine scheduled is neither rolled back nor
    /// compacted over.
    #[tokio::test]
    async fn test_compact_refuses_a_foreign_pending_plan() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = mor_table(dir.path(), &[]).await;
        table.append([batch(&[("a", "sf", 1, 10)])]).await.unwrap();
        table.upsert([batch(&[("a", "sf", 2, 11)])]).await.unwrap();
        let storage = table.file_system_view.storage.clone();
        let foreign = generate_instant_time().await;
        let mut plan = compaction_plan(&latest_slices(&table).await);
        plan.extra_metadata = Some(HashMap::new());
        crate::write::fence_timeline_instant(
            storage.as_ref(),
            &timeline_dir(&table),
            &foreign,
            Action::Compaction,
            plan.to_avro_bytes().unwrap(),
            Vec::new(),
        )
        .await
        .unwrap();
        let files = timeline_files(dir.path());
        let error = table.compact().await.unwrap_err();
        assert!(
            matches!(error, CoreError::Unsupported(ref message) if message.contains(&foreign)),
            "{error}"
        );
        assert_eq!(timeline_files(dir.path()), files);
    }

    /// Append `rows` to `slice` as a completed delta commit, without the
    /// table lock a write would take: stands in for a write that completes
    /// while a compaction waits for the lock.
    async fn complete_delta_commit_unlocked(table: &Table, slice: &FileSlice, rows: RecordBatch) {
        let pending = start_delta_commit_unlocked(table, slice, rows).await;
        finish_delta_commit_unlocked(table, pending).await;
    }

    /// A delta commit whose log file is written and whose instant is not yet
    /// completed.
    struct PendingDeltaCommit {
        instant: String,
        stat: HoodieWriteStat,
        schema: arrow_schema::SchemaRef,
    }

    /// Mint a delta commit and write `rows` to a log file of `slice`, leaving
    /// the instant pending.
    async fn start_delta_commit_unlocked(
        table: &Table,
        slice: &FileSlice,
        rows: RecordBatch,
    ) -> PendingDeltaCommit {
        let storage = table.file_system_view.storage.clone();
        let instant = generate_instant_time().await;
        crate::write::fence_timeline_instant(
            storage.as_ref(),
            &timeline_dir(table),
            &instant,
            Action::DeltaCommit,
            Vec::new(),
            crate::write::inflight_commit_metadata_bytes("UPSERT", true).unwrap(),
        )
        .await
        .unwrap();
        let file_id = slice.file_id().to_string();
        let log_name = format!(".{file_id}_{instant}.log.1_0-0-0");
        let path = relative_data_path(&slice.partition_path, &log_name);
        let prepared =
            prepare_batches_for_write(&table.hudi_configs, &[rows], &instant, &file_id).unwrap();
        let schema = prepared[0].schema();
        crate::write::log_file_task(
            storage.clone(),
            crate::write::append::parquet_writer_props(table),
            prepared,
            path.clone(),
            instant.clone(),
            crate::write::append::arrow_schema_to_avro_json(schema.as_ref()).unwrap(),
            false,
        )
        .await
        .unwrap();
        let stat = HoodieWriteStat {
            file_id: Some(file_id),
            path: Some(path),
            base_file: slice.base_file.as_ref().map(BaseFile::file_name),
            log_files: Some(vec![log_name]),
            prev_commit: Some(slice.creation_instant_time().to_string()),
            partition_path: Some(slice.partition_path.clone()),
            num_writes: Some(1),
            ..Default::default()
        };
        PendingDeltaCommit {
            instant,
            stat,
            schema,
        }
    }

    /// Complete a delta commit started by [`start_delta_commit_unlocked`].
    async fn finish_delta_commit_unlocked(table: &Table, pending: PendingDeltaCommit) {
        let storage = table.file_system_view.storage.clone();
        let bytes = crate::write::commit_metadata_bytes(
            table,
            "UPSERT",
            crate::write::stats_by_partition(vec![pending.stat]),
            None,
            pending.schema.as_ref(),
        )
        .unwrap();
        let completed = crate::timeline::instant::Instant::new_completed(
            pending.instant,
            Action::DeltaCommit,
            Some(generate_instant_time().await),
        )
        .unwrap();
        storage
            .put_file_if_absent(
                &completed
                    .relative_path_with_base(&timeline_dir(table))
                    .unwrap(),
                bytes,
            )
            .await
            .unwrap();
    }

    /// A delta commit completing while a compaction waits for the lock is in
    /// its plan: listed after it, it would complete below the compaction
    /// instant, stay on the superseded slice and drop out of reads.
    #[tokio::test]
    async fn test_compaction_plans_after_waiting_for_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        // File listing comes from storage, so the unlocked delta commit needs
        // no metadata table entry to be seen.
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_table_type(TableTypeValue::MergeOnRead)
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_ordering_fields(["ts"])
            .with_metadata_table_enabled(false)
            .create()
            .await
            .unwrap();
        table.append([batch(&[("a", "sf", 1, 10)])]).await.unwrap();
        table.upsert([batch(&[("a", "sf", 2, 11)])]).await.unwrap();
        let slice = latest_slices(&table).await.remove(0);

        let lock = crate::write::lock::lock_provider_for(&table);
        let held = lock.lock().await.unwrap();
        let mut compacting = Table::new(dir.path().to_str().unwrap()).await.unwrap();
        let task = tokio::spawn(async move { compacting.compact().await });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        complete_delta_commit_unlocked(&table, &slice, batch(&[("a", "sf", 3, 12)])).await;
        drop(held);

        let result = task.await.unwrap().unwrap();
        assert_eq!(result.num_log_files, 2);
        let reader = Table::new(dir.path().to_str().unwrap()).await.unwrap();
        assert_eq!(
            data_only(&snapshot(&reader).await),
            vec![("a".into(), "sf".into(), 3, 12)]
        );
    }

    /// Delta commits completing while a compaction is pending, as they do
    /// beside a background compaction, attach to its new slice by completion
    /// time: one minted before the plan (named below the compaction instant)
    /// and one minted after. Both survive the compaction's completion.
    #[tokio::test]
    async fn test_writes_while_a_compaction_is_pending_survive_it() {
        let dir = tempfile::tempdir().unwrap();
        // The unlocked delta commit writes no metadata table entry, so file
        // listing must come from storage.
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_table_type(TableTypeValue::MergeOnRead)
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_ordering_fields(["ts"])
            .with_metadata_table_enabled(false)
            .create()
            .await
            .unwrap();
        table
            .append([batch(&[("a", "sf", 1, 10), ("b", "sf", 1, 20)])])
            .await
            .unwrap();
        table.upsert([batch(&[("a", "sf", 2, 11)])]).await.unwrap();
        let slice = latest_slices(&table).await.remove(0);

        let before =
            start_delta_commit_unlocked(&table, &slice, batch(&[("a", "sf", 3, 12)])).await;
        let (instant, slices) = request_compaction(&mut table).await.unwrap().unwrap();
        assert!(before.instant < instant);
        assert_eq!(slices[0].log_files.len(), 1, "a pending log is not planned");
        finish_delta_commit_unlocked(&table, before).await;
        let during = table.upsert([batch(&[("b", "sf", 2, 21)])]).await.unwrap();
        assert!(during.requested_time.unwrap() > instant);
        execute_compaction(&mut table, &instant, slices)
            .await
            .unwrap();

        let reader = Table::new(dir.path().to_str().unwrap()).await.unwrap();
        let slice = latest_slices(&reader).await.remove(0);
        assert_eq!(slice.creation_instant_time(), instant);
        assert_eq!(slice.log_files.len(), 2);
        assert_eq!(
            data_only(&snapshot(&reader).await),
            vec![
                ("a".into(), "sf".into(), 3, 12),
                ("b".into(), "sf".into(), 2, 21)
            ]
        );
    }

    #[tokio::test]
    async fn test_inline_compaction_runs_at_the_delta_commit_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = mor_table(
            dir.path(),
            &[
                ("hoodie.compact.inline", "true"),
                ("hoodie.compact.inline.max.delta.commits", "2"),
            ],
        )
        .await;
        table.append([batch(&[("a", "sf", 1, 10)])]).await.unwrap();
        assert!(
            completed_commits(dir.path()).is_empty(),
            "one delta commit is below the threshold"
        );
        table.upsert([batch(&[("a", "sf", 2, 11)])]).await.unwrap();
        assert_eq!(completed_commits(dir.path()).len(), 1);
        assert!(
            latest_slices(&table)
                .await
                .iter()
                .all(|s| !s.has_log_file())
        );
        // The count restarts at the compaction.
        table.upsert([batch(&[("a", "sf", 3, 12)])]).await.unwrap();
        assert_eq!(completed_commits(dir.path()).len(), 1);
        assert_eq!(
            data_only(&snapshot(&table).await),
            vec![("a".into(), "sf".into(), 3, 12)]
        );
    }

    #[tokio::test]
    async fn test_async_compaction_runs_in_the_background() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = mor_table(
            dir.path(),
            &[
                ("hoodie.datasource.compaction.async.enable", "true"),
                ("hoodie.compact.inline.max.delta.commits", "2"),
            ],
        )
        .await;
        assert_eq!(table.await_compaction().await.unwrap(), None);
        table.append([batch(&[("a", "sf", 1, 10)])]).await.unwrap();
        table.upsert([batch(&[("a", "sf", 2, 11)])]).await.unwrap();
        let result = table
            .await_compaction()
            .await
            .unwrap()
            .expect("a compaction was started");
        assert_eq!((result.num_file_groups, result.num_log_files), (1, 1));
        assert_eq!(completed_commits(dir.path()).len(), 1);
        assert!(
            latest_slices(&table)
                .await
                .iter()
                .all(|s| !s.has_log_file())
        );
        assert_eq!(table.await_compaction().await.unwrap(), None);
        assert_eq!(
            data_only(&snapshot(&table).await),
            vec![("a".into(), "sf".into(), 2, 11)]
        );
    }
}
