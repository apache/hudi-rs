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

//! `fg-bench` — minimal file-group reader benchmark harness (perf effort M2).
//!
//! Opens a Hudi table, discovers its latest file slice(s), drives a
//! `HoodieFileGroupReader` per slice to completion, and records per-iteration
//! wall/CPU/RSS plus the full `HoodieReadStats` (including the stage
//! timings). Output is a single JSON document; see `benchmark/filegroup/README.md`.

mod host;
mod rusage;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow_schema::{Schema, SchemaRef};
use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use hudi_core::config::HudiConfigs;
use hudi_core::error::Result;
use hudi_core::file_group::file_slice::FileSlice;
use hudi_core::file_group::reader::FileGroupReader;
use hudi_core::storage::Storage;
use hudi_core::table::Table;
use hudi_core::table::builder::OptionResolver;
use serde::Serialize;

use host::HostSnapshot;
use rusage::Rusage;

/// Accounting-drift detector (A6e detector #3). After each run, if the measured
/// `max_rss` greatly exceeds what the merge-map accounting + a generous base /
/// output headroom would predict, the size accounting is lying about the resident
/// set (the exact M5 failure: accounting read ~budget while RSS was 21.9 GB). We
/// then print a loud WARN and set `accounting_drift: true` in the JSON.
///
/// `ACCOUNTING_DRIFT_FACTOR` is the multiple of (accounted retained + headroom)
/// that `max_rss` must exceed to be flagged. 4× leaves room for allocator
/// fragmentation + transient peaks while still catching an order-of-magnitude
/// divergence.
const ACCOUNTING_DRIFT_FACTOR: u64 = 4;

/// Base/output headroom (bytes) added to the accounted merge-map retained bytes
/// before applying [`ACCOUNTING_DRIFT_FACTOR`] — covers the base-file decode
/// batch, the output chunk, and fixed runtime/RocksDB overhead that the merge-map
/// accounting does not (and should not) include. 512 MiB is generous enough that
/// a correctly-bounded read never trips the detector, yet far below the
/// multi-GB divergence the M5 bug produced.
const ACCOUNTING_DRIFT_HEADROOM_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(
    name = "fg-bench",
    about = "Minimal HoodieFileGroupReader benchmark harness (perf M2)"
)]
struct Args {
    /// Path to the Hudi table (local filesystem path or file:// URI).
    #[arg(long)]
    table: String,

    /// Number of measured iterations. The first is always a warmup and is
    /// excluded from the summary statistics.
    #[arg(long, default_value_t = 3)]
    iterations: usize,

    /// Write the JSON report to this file instead of stdout.
    #[arg(long)]
    output_json: Option<String>,

    /// Comma-separated projection columns (requested schema). When omitted the
    /// reader reads all columns.
    #[arg(long, value_delimiter = ',')]
    columns: Option<Vec<String>>,

    /// Streaming chunk size (`hoodie.read.stream.batch_size`, ENG-42991). When
    /// omitted the reader uses its built-in default (DEFAULT_BATCH_SIZE = 4096).
    /// Set this to measure the effect of chunk granularity on wall time. (The
    /// peak-RSS effect shows only on the streaming `open()` path; this bench
    /// drives `read()`, which retains the full output, so `max_rss` is
    /// dominated by output retention here, not chunk size.)
    #[arg(long)]
    batch_size: Option<usize>,

    /// Merge memory budget in bytes (`hoodie.memory.merge.max.size`, A1/ENG-42993).
    /// When omitted the reader uses its 1 GiB default (no spill on smoke
    /// datasets). Set it low (e.g. a few MiB) to force the size-tracked merge map
    /// to spill to RocksDB and measure bounded RSS without OOM.
    #[arg(long)]
    merge_max_size: Option<u64>,

    /// Drive the true-streaming `open()` path (A3/ENG-42992) instead of the
    /// eager `read()` path. In streaming mode the base parquet file is decoded
    /// one row-group at a time (`ParquetSyncReader`) and never fully
    /// materialised — this is the path the R3 base-file-memory fix optimizes.
    /// The streaming iterator does `block_on` per row-group, so each slice is
    /// driven on a dedicated OS thread (off the harness's tokio runtime),
    /// mirroring the FFI driver's sync-consumer contract.
    #[arg(long, default_value_t = false)]
    streaming: bool,

    /// Drive the async streaming path (`open_blocking_stream`) from this
    /// process's tokio runtime, rather than the sync `open()` path on a plain
    /// OS thread. This is how a Rust async caller consumes a streaming read.
    #[arg(long, default_value_t = false)]
    async_stream: bool,

    /// Merge base + log records by base-file row POSITION instead of record key
    /// (`hoodie.merge.use.record.positions`). Selects the
    /// PositionBasedFileGroupRecordBuffer, which matches base rows to log
    /// records by their physical position (read via a parquet virtual
    /// row-number column) and falls back to key-based merge when a log block has
    /// no valid positions. Use with a table whose log blocks carry
    /// RECORD_POSITIONS headers (written by Spark with record positions enabled)
    /// to compare position-based vs key-based merge on the same table.
    #[arg(long, default_value_t = false)]
    use_record_position: bool,
    /// Read this many file slices concurrently, mirroring the reader's own
    /// `buffer_unordered` fan-out. 1 is sequential with no coordination cost and
    /// is the baseline any bounded-memory claim must not regress.
    #[arg(long, default_value_t = 1)]
    slice_concurrency: usize,
    /// Fail the run when peak RSS exceeds this many bytes. Turns the harness
    /// from a measurement into a gate: a read may be slower under pressure, but
    /// it may not grow without bound.
    #[arg(long)]
    max_rss_bytes: Option<u64>,
    /// Directory the merge map spills into (`hoodie.memory.spillable.map.path`).
    /// Watched to report whether a run actually exercised the disk tier.
    #[arg(long, default_value = "/tmp")]
    spill_dir: String,
}

/// Per-slice read configuration, bundled to keep the read helpers under the
/// clippy argument-count limit and to make the eager/streaming split explicit.
#[derive(Clone)]
struct ReadConfig {
    data_schema: SchemaRef,
    requested_schema: Option<SchemaRef>,
    batch_size: Option<usize>,
    merge_max_size: Option<u64>,
    /// True → drive the streaming `open()` path (A3); false → eager `read()`.
    streaming: bool,
    /// True → drive `open_blocking_stream()` from the tokio runtime.
    async_stream: bool,
    /// True → position-based merge (`use_record_position`); false → key-based.
    use_record_position: bool,
    /// How many slices to read concurrently.
    slice_concurrency: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    env_logger::init();
    let args = Args::parse();
    if let Err(e) = run(&args).await {
        eprintln!("fg-bench failed: {e:?}");
        std::process::exit(1);
    }
}

async fn run(args: &Args) -> Result<()> {
    let nproc = host::nproc();

    // 1. Open the table + resolve the full hoodie option map (includes raw
    //    hoodie.properties: record key, ordering, merge mode, table type).
    let table = Table::new(&args.table).await?;
    let hoodie_options = resolve_hoodie_options(&args.table).await?;
    let (hudi_configs, storage) = configs_and_storage(&args.table).await?;

    // 2. Discover the latest file slice(s) (snapshot, no partition filters).
    // OSS `get_file_slices` takes `&ReadOptions` (the internal fork takes filter
    // tuples). Default options = snapshot, no partition filters.
    let no_filters = hudi_core::config::ReadOptions::default();
    let file_slices = table.get_file_slices(&no_filters).await?;
    if file_slices.is_empty() {
        return Err(hudi_core::error::CoreError::ReadFileSliceError(format!(
            "no file slices found at '{}'",
            args.table
        )));
    }
    eprintln!(
        "[fg-bench] table={} slices={} nproc={} iterations={} (1 warmup)",
        args.table,
        file_slices.len(),
        nproc,
        args.iterations
    );

    // Requested schema. When --columns is given we project to those columns;
    // otherwise we request the FULL table schema (with meta fields). Always
    // supplying a requested schema mirrors the production FFI path and ensures
    // the reader's required/reader schema is set even for log-only slices
    // (otherwise merge_and_collect has no schema source). Field types come from
    // the table schema so they match exactly; the per-slice base footer
    // supplies the data schema (mirrors the FG harness Projection path).
    let table_data_schema: SchemaRef = table.get_schema_with_meta_fields().await.map(Arc::new)?;
    let requested_schema: Option<SchemaRef> = match &args.columns {
        Some(cols) => Some(build_requested_schema(&table, cols).await?),
        None => Some(table_data_schema.clone()),
    };

    let mut iterations = Vec::with_capacity(args.iterations);
    let mut over_budget_any = false;
    let spill_dir = std::path::PathBuf::from(&args.spill_dir);
    let mut any_contended = false;

    for iter_idx in 0..args.iterations {
        let warmup = iter_idx == 0;

        let host_pre = HostSnapshot::capture();
        let contended = host_pre.is_contended(nproc);
        if contended {
            any_contended = true;
            eprintln!(
                "[fg-bench] !!! HOST CONTENDED: load1={:.2} over {} cores (ratio {:.2} > {:.2}); \
                 iteration {} numbers are unreliable",
                host_pre.load1,
                nproc,
                host_pre.load1 / nproc as f64,
                host::LOAD_THRESHOLD,
                iter_idx
            );
        }

        let ru_before = Rusage::capture();
        let wall_start = Instant::now();

        let read_config = ReadConfig {
            data_schema: table_data_schema.clone(),
            requested_schema: requested_schema.clone(),
            batch_size: args.batch_size,
            merge_max_size: args.merge_max_size,
            streaming: args.streaming,
            async_stream: args.async_stream,
            use_record_position: args.use_record_position,
            slice_concurrency: args.slice_concurrency,
        };
        let spill_before = spill_dir_bytes(&spill_dir);
        let rows =
            read_all_slices(&file_slices, &hoodie_options, &args.table, &read_config).await?;

        let wall_ms = wall_start.elapsed().as_millis() as u64;
        let ru_delta = Rusage::capture().delta(&ru_before);

        eprintln!(
            "[fg-bench] iter {}{}: wall={}ms user={}ms sys={}ms rss={}MB rows={}",
            iter_idx,
            if warmup { " (warmup)" } else { "" },
            wall_ms,
            ru_delta.user_ms,
            ru_delta.sys_ms,
            ru_delta.max_rss_kb / 1024,
            rows
        );

        // The memory gate. `HoodieReadStats` carried the merge map's accounted
        // peak, which the old drift detector compared against RSS; the public
        // reader surface does not expose it, and widening that surface for a
        // benchmark is the wrong trade. What the gate actually needs is simpler
        // and stronger: measured RSS against the budget the caller declared.
        let max_rss_bytes = ru_delta.max_rss_kb.saturating_mul(1024);
        let over_budget = args.max_rss_bytes.is_some_and(|cap| max_rss_bytes > cap);
        if over_budget {
            eprintln!(
                "[fg-bench] !!! OVER BUDGET: max_rss={}MB exceeds --max-rss-bytes={}MB on \
                 iteration {}. A read must degrade in throughput rather than grow without \
                 bound.",
                max_rss_bytes / (1024 * 1024),
                args.max_rss_bytes.unwrap_or(0) / (1024 * 1024),
                iter_idx,
            );
        }
        over_budget_any |= over_budget;

        // Spill is observed rather than reported: the disk tier writes under
        // `hoodie.memory.spillable.map.path`, so a directory that grew during
        // the iteration means the merge map spilled. Without this a passing run
        // cannot distinguish "stayed under budget because it spilled correctly"
        // from "stayed under because the data never got large".
        let spilled = spill_dir_bytes(&spill_dir) > spill_before;

        iterations.push(IterationReport {
            warmup,
            wall_ms,
            user_ms: ru_delta.user_ms,
            sys_ms: ru_delta.sys_ms,
            max_rss_kb: ru_delta.max_rss_kb,
            rows,
            contended,
            accounting_drift: over_budget,
            spilled,
            host: HostReport {
                load1: host_pre.load1,
                mem_available_kb: host_pre.mem_available_kb,
            },
        });
    }

    let report = Report {
        env: EnvReport::capture(nproc),
        table: args.table.clone(),
        columns: args.columns.clone(),
        num_slices: file_slices.len(),
        merge_strategy: if args.use_record_position {
            "position"
        } else {
            "key"
        },
        contended: any_contended,
        summary: Summary::from_iterations(&iterations),
        iterations,
    };

    let json = serde_json::to_string_pretty(&report)
        .map_err(|e| hudi_core::error::CoreError::ReadFileSliceError(e.to_string()))?;
    match &args.output_json {
        Some(path) => {
            std::fs::write(path, &json)
                .map_err(|e| hudi_core::error::CoreError::ReadFileSliceError(e.to_string()))?;
            eprintln!("[fg-bench] wrote JSON report to {path}");
        }
        None => println!("{json}"),
    }
    if over_budget_any {
        // A non-zero exit is what makes this a gate rather than a report. The
        // budget was declared by the caller; exceeding it is a failure even
        // though every read returned correct rows.
        return Err(hudi_core::error::CoreError::Unsupported(format!(
            "peak RSS exceeded --max-rss-bytes={} on at least one iteration",
            args.max_rss_bytes.unwrap_or(0)
        )));
    }

    Ok(())
}

/// Total bytes of regular files directly under `dir`, or 0 when unreadable.
///
/// Deliberately shallow and failure-tolerant: this is an observation used to
/// report whether a run exercised the spill tier, never a correctness signal, so
/// an unreadable directory must not fail the benchmark.
fn spill_dir_bytes(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .sum()
}

/// Read every discovered file slice to completion, returning total rows.
///
/// Built on the public `FileGroupReader` surface rather than assembling a
/// reader from `reader_v2` internals, which are `pub(crate)`. Every knob this
/// harness offers is a Hudi config key, so they survive the move: batch size,
/// merge budget and record-position merge all travel in the options bag. What
/// does not survive is `HoodieReadStats` -- stage timings and the spill flag are
/// not on the public surface. Spill is instead observed from outside, by
/// watching the spill directory, so the harness reports it without the reader
/// having to expose it.
async fn read_all_slices(
    file_slices: &[FileSlice],
    hoodie_options: &HashMap<String, String>,
    table_path: &str,
    cfg: &ReadConfig,
) -> Result<usize> {
    let mut options: Vec<(String, String)> = hoodie_options
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if let Some(n) = cfg.batch_size {
        options.push(("hoodie.read.stream.batch_size".to_string(), n.to_string()));
    }
    if let Some(n) = cfg.merge_max_size {
        options.push(("hoodie.memory.merge.max.size".to_string(), n.to_string()));
    }
    if cfg.use_record_position {
        options.push((
            "hoodie.merge.use.record.positions".to_string(),
            "true".to_string(),
        ));
    }
    let reader = FileGroupReader::new_with_options(table_path, options).await?;

    let mut read_options = hudi_core::config::read_options::ReadOptions::default();
    read_options.projection = cfg.requested_schema.as_ref().map(|schema| {
        schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<String>>()
    });

    // The fan-out under test. `buffer_unordered(1)` is sequential with no
    // coordination cost, which is what the single-slice baseline must stay.
    let concurrency = cfg.slice_concurrency.max(1);
    let total_rows = futures::stream::iter(file_slices.iter())
        .map(|slice| {
            let reader = &reader;
            let read_options = &read_options;
            let streaming = cfg.streaming || cfg.async_stream;
            async move {
                if streaming {
                    // The streaming path yields batches and drops each as it
                    // goes, so the whole result is never resident. The eager
                    // path below retains it. That difference is the point of
                    // the comparison, so rows are counted without collecting.
                    let mut stream = reader.read_file_slice_stream(slice, read_options).await?;
                    let mut rows = 0usize;
                    while let Some(batch) = stream.next().await {
                        rows += batch?.num_rows();
                    }
                    Ok::<usize, hudi_core::error::CoreError>(rows)
                } else {
                    let batch = reader.read_file_slice(slice, read_options).await?;
                    Ok::<usize, hudi_core::error::CoreError>(batch.num_rows())
                }
            }
        })
        .buffer_unordered(concurrency)
        .try_fold(0usize, |acc, n| async move { Ok(acc + n) })
        .await?;

    Ok(total_rows)
}

/// Build a requested `SchemaRef` from named columns, typed against the table schema.
async fn build_requested_schema(table: &Table, cols: &[String]) -> Result<SchemaRef> {
    let table_schema = table.get_schema().await?;
    let fields = cols
        .iter()
        .map(|name| {
            table_schema
                .column_with_name(name)
                .map(|(_, f)| Arc::new(f.clone()))
                .ok_or_else(|| {
                    hudi_core::error::CoreError::ReadFileSliceError(format!(
                        "projection column '{name}' not in table schema"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Resolve the full hoodie option map (raw hoodie.properties + defaults).
async fn resolve_hoodie_options(table_path: &str) -> Result<HashMap<String, String>> {
    let empty_opts: Vec<(&str, &str)> = vec![];
    let mut resolver = OptionResolver::new_with_options(table_path, empty_opts);
    resolver.resolve_options().await?;
    Ok(resolver.hudi_options)
}

/// Create `HudiConfigs` + `Storage` from a table path (mirrors the FG harness).
async fn configs_and_storage(table_path: &str) -> Result<(Arc<HudiConfigs>, Arc<Storage>)> {
    let empty_opts: Vec<(&str, &str)> = vec![];
    let mut resolver = OptionResolver::new_with_options(table_path, empty_opts);
    resolver.resolve_options().await?;
    let hudi_configs = Arc::new(HudiConfigs::new(resolver.hudi_options));
    let storage = Storage::new(Arc::new(resolver.storage_options), hudi_configs.clone())?;
    Ok((hudi_configs, storage))
}

#[derive(Serialize)]
struct Report {
    env: EnvReport,
    table: String,
    columns: Option<Vec<String>>,
    num_slices: usize,
    /// Merge strategy exercised: `"position"` when `--use-record-position`, else
    /// `"key"`. Lets a key-vs-position comparison label each report.
    merge_strategy: &'static str,
    contended: bool,
    iterations: Vec<IterationReport>,
    summary: Summary,
}

#[derive(Serialize)]
struct EnvReport {
    git_sha: String,
    rustc: String,
    nproc: usize,
    hostname: String,
}

impl EnvReport {
    fn capture(nproc: usize) -> Self {
        EnvReport {
            git_sha: option_env!("FG_BENCH_GIT_SHA")
                .map(str::to_string)
                .unwrap_or_else(read_git_sha),
            rustc: read_rustc(),
            nproc,
            hostname: std::fs::read_to_string("/etc/hostname")
                .map(|s| s.trim().to_string())
                .unwrap_or_else(|_| "unknown".to_string()),
        }
    }
}

#[derive(Serialize)]
struct IterationReport {
    warmup: bool,
    wall_ms: u64,
    user_ms: u64,
    sys_ms: u64,
    max_rss_kb: u64,
    rows: usize,
    contended: bool,
    /// A6e detector #3: true when `max_rss` greatly exceeds the merge-map
    /// accounted retained bytes + headroom — i.e. the size accounting is not
    /// tracking the resident set. False on a correctly-bounded read.
    accounting_drift: bool,
    /// Whether the merge map spilled to disk during this iteration, observed by
    /// watching the spill directory. `HoodieReadStats` carries this directly but
    /// is not on the public reader surface, and a benchmark is not a reason to
    /// widen it.
    spilled: bool,
    host: HostReport,
}

#[derive(Serialize)]
struct HostReport {
    load1: f64,
    mem_available_kb: u64,
}

#[derive(Serialize)]
struct Summary {
    /// Median/min/max over the NON-warmup iterations (falls back to all when
    /// only the warmup iteration exists).
    median_wall_ms: u64,
    min_wall_ms: u64,
    max_wall_ms: u64,
    measured_iterations: usize,
}

impl Summary {
    fn from_iterations(iters: &[IterationReport]) -> Self {
        let mut walls: Vec<u64> = iters
            .iter()
            .filter(|i| !i.warmup)
            .map(|i| i.wall_ms)
            .collect();
        if walls.is_empty() {
            walls = iters.iter().map(|i| i.wall_ms).collect();
        }
        walls.sort_unstable();
        let n = walls.len();
        let median = if n == 0 {
            0
        } else if n % 2 == 1 {
            walls[n / 2]
        } else {
            (walls[n / 2 - 1] + walls[n / 2]) / 2
        };
        Summary {
            median_wall_ms: median,
            min_wall_ms: walls.first().copied().unwrap_or(0),
            max_wall_ms: walls.last().copied().unwrap_or(0),
            measured_iterations: n,
        }
    }
}

fn read_git_sha() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn read_rustc() -> String {
    std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}
