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

//! `fg-bench` — minimal file-group reader benchmark harness.
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
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use futures::{StreamExt, TryStreamExt};
use hudi_core::error::Result;
use hudi_core::file_group::file_slice::FileSlice;
use hudi_core::file_group::reader::FileGroupReader;
use hudi_core::table::Table;
use hudi_core::table::builder::OptionResolver;
use serde::Serialize;

use host::HostSnapshot;
use rusage::Rusage;

#[derive(Parser, Debug)]
#[command(name = "fg-bench", about = "File-group reader benchmark harness")]
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

    /// Streaming chunk size (`hoodie.read.stream.batch_size`). When
    /// omitted the reader uses its built-in default (1024 rows).
    /// Set this to measure the effect of chunk granularity on wall time. (The
    /// peak-RSS effect shows only on the streaming `open()` path; this bench
    /// drives `read()`, which retains the full output, so `max_rss` is
    /// dominated by output retention here, not chunk size.)
    #[arg(long)]
    batch_size: Option<usize>,

    /// Merge memory budget in bytes (`hoodie.memory.merge.max.size`).
    /// When omitted the reader uses its 1 GiB default (no spill on smoke
    /// datasets). Set it low (e.g. a few MiB) to force the size-tracked merge map
    /// to spill to RocksDB and measure bounded RSS without OOM.
    #[arg(long)]
    merge_max_size: Option<u64>,

    /// Consume the read as a stream of batches instead of eagerly.
    ///
    /// `read_file_slice_stream` yields batches and the harness drops each as it
    /// counts it, so the whole result is never resident; the eager
    /// `read_file_slice` retains it. On a large table that difference dominates
    /// `max_rss_kb`, which is the comparison this flag exists for.
    ///
    /// One flag, not two: the public reader offers a single streaming entry
    /// point, so a sync-consumer variant would drive the same call and measure
    /// the same path twice.
    #[arg(long, default_value_t = false)]
    streaming: bool,

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
    /// Total memory the scan may use for concurrent slice reads
    /// (`hoodie.read.scan.max.memory.size`). When set, slices-in-flight is
    /// derived from it by `hudi_core::file_group::admission::slices_in_flight`
    /// — the same function the DataFusion plan uses — and `--slice-concurrency`
    /// becomes the ceiling it may not exceed rather than the value used.
    #[arg(long)]
    scan_memory_budget: Option<u64>,

    /// Directory the merge map spills into (`hoodie.memory.spillable.map.path`).
    /// Watched to report whether a run actually exercised the disk tier.
    ///
    /// Defaults to a fresh per-run subdirectory of the system temp directory.
    /// Defaulting to `/tmp` itself does not work: the watcher would report the
    /// whole directory's contents as this run's spill — several GB on an
    /// ordinary host, before the read has done anything — and would re-walk all
    /// of `/tmp` every 100 ms to do it.
    #[arg(long)]
    spill_dir: Option<String>,

    /// Read with this many concurrent readers, each a `FileGroupReader` of its
    /// own on its own task, taking the slices round-robin and reading its share
    /// one at a time. This is the engine-thread shape: one reader per thread.
    ///
    /// Replaces the shared reader and `--slice-concurrency` fan-out. A shared
    /// reader pools every slice's requests into one set of counters, so its
    /// `peak_in_flight` cannot tell concurrent slices from pipelined requests;
    /// a reader each keeps the per-reader request report meaningful.
    #[arg(long)]
    readers: Option<usize>,
}

/// A fresh spill directory for this run, under the system temp directory.
///
/// Named by pid so two concurrent runs cannot watch each other's spill.
fn default_spill_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("fg-bench-spill-{}", std::process::id()))
}

/// Per-slice read configuration, bundled to keep the read helpers under the
/// clippy argument-count limit and to make the eager/streaming split explicit.
#[derive(Clone)]
struct ReadConfig {
    requested_schema: Option<SchemaRef>,
    batch_size: Option<usize>,
    merge_max_size: Option<u64>,
    /// True → consume `read_file_slice_stream`; false → eager `read_file_slice`.
    streaming: bool,
    /// True → position-based merge (`use_record_position`); false → key-based.
    use_record_position: bool,
    /// How many slices to read concurrently — a ceiling when a budget is set.
    slice_concurrency: usize,
    /// Scan-wide memory budget, in bytes.
    scan_memory_budget: Option<u64>,
    /// Where the merge map spills (`hoodie.memory.spillable.map.path`). Passed
    /// to the reader, not only watched: the reader's own default is `/tmp`, so
    /// without this the reader would spill somewhere the watcher is not
    /// looking and every run would report no spill.
    spill_dir: String,
    /// One reader per task instead of one shared reader; see `Args::readers`.
    readers: Option<usize>,
}

/// A global allocator that refuses to exceed a byte ceiling.
///
/// `--max-rss-bytes` asserts *after* the fact: it reads peak RSS once the
/// allocation has already happened, so it answers "how much did this read want"
/// on a machine with memory to spare. It cannot answer "does this read survive
/// on a small machine", because nothing ever told the process no.
///
/// This does. Past the ceiling, `alloc` returns null. Rust's runtime turns that
/// into an abort, which is deliberately not graceful: an abort proves the
/// ceiling is real. A read that wants to degrade instead has to stay under it by
/// spilling, which is why this pairs with a low `--merge-max-size` rather than
/// standing alone.
///
/// The ceiling is read from `FG_BENCH_ALLOC_CAP_BYTES`, so an ordinary run pays
/// only the counter and is never refused.
///
/// The counter runs unconditionally, from the first allocation the process
/// makes. Gating it on the cap being set does not work: the cap can only be
/// installed once `main` runs, which is after the tokio runtime and the
/// environment have already allocated. Those allocations would never be added,
/// but their frees would still be subtracted, underflowing the counter to near
/// `u64::MAX` — every later `live > cap` test then fails and a capped run aborts
/// immediately whatever its real usage. Counting every allocation and every free
/// keeps `ALLOC_LIVE` balanced, which is what makes the comparison meaningful.
struct CappedAllocator;

static ALLOC_CAP: AtomicU64 = AtomicU64::new(0);
static ALLOC_LIVE: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CappedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let size = layout.size() as u64;
        let live = ALLOC_LIVE.fetch_add(size, Ordering::Relaxed) + size;
        let cap = ALLOC_CAP.load(Ordering::Relaxed);
        if cap > 0 && live > cap {
            ALLOC_LIVE.fetch_sub(size, Ordering::Relaxed);
            // Null here, rather than a panic: a panic would unwind through
            // an allocation path that has just been told there is no memory.
            return std::ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        ALLOC_LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CappedAllocator = CappedAllocator;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    env_logger::init();
    // Read before anything large is allocated, so the cap covers the run rather
    // than only its tail.
    if let Ok(v) = std::env::var("FG_BENCH_ALLOC_CAP_BYTES")
        && let Ok(n) = v.parse::<u64>()
    {
        ALLOC_CAP.store(n, Ordering::Relaxed);
        eprintln!(
            "[fg-bench] allocation ceiling: {} MiB (hard)",
            n / (1024 * 1024)
        );
    }
    let args = Args::parse();
    if args.readers.is_some() && args.scan_memory_budget.is_some() {
        eprintln!("fg-bench failed: --readers and --scan-memory-budget cannot be combined");
        std::process::exit(2);
    }
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

    // 2. Discover the latest file slice(s) (snapshot, no partition filters).
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
    // Created up front so the watcher has something to stat from the first
    // sample, and so its baseline is this run's own starting size.
    let spill_dir = args
        .spill_dir
        .as_ref()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(default_spill_dir);
    if let Err(e) = std::fs::create_dir_all(&spill_dir) {
        eprintln!(
            "[fg-bench] could not create spill dir {}: {e}",
            spill_dir.display()
        );
    }
    let spill_dir_string = spill_dir.to_string_lossy().into_owned();
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
            requested_schema: requested_schema.clone(),
            batch_size: args.batch_size,
            merge_max_size: args.merge_max_size,
            streaming: args.streaming,
            use_record_position: args.use_record_position,
            slice_concurrency: args.slice_concurrency,
            scan_memory_budget: args.scan_memory_budget,
            spill_dir: spill_dir_string.clone(),
            readers: args.readers,
        };
        let spill_watch = SpillWatcher::start(spill_dir.clone());
        let (rows, readers) =
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
        for r in &readers {
            r.print();
        }

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
        let spill_peak_bytes = spill_watch.finish();
        let spilled = spill_peak_bytes > 0;

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
            spill_peak_bytes,
            readers,
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

/// Watches a spill directory for its high-water mark while a read runs.
///
/// A before/after comparison cannot work here. RocksDB creates its directory,
/// writes, compacts, and removes it when the reader closes -- which happens
/// before the read call returns. Sampling at iteration boundaries therefore sees
/// an empty directory both times and reports "never spilled" for a read that
/// spilled a gigabyte. Only sampling *during* the read observes it.
struct SpillWatcher {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicU64>,
    /// What the directory already held when sampling began, subtracted from the
    /// peak so the reported number is this read's spill and not the directory's
    /// prior contents. Zero for the default per-run directory; non-zero matters
    /// when `--spill-dir` names somewhere shared.
    baseline: u64,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl SpillWatcher {
    fn start(dir: std::path::PathBuf) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let baseline = spill_dir_bytes(&dir);
        let peak = Arc::new(AtomicU64::new(baseline));
        let (s, pk) = (stop.clone(), peak.clone());
        let handle = std::thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                let now = spill_dir_bytes(&dir);
                pk.fetch_max(now, Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        });
        Self {
            stop,
            peak,
            baseline,
            handle: Some(handle),
        }
    }

    /// Stops sampling and returns the largest *growth* over the starting size,
    /// in bytes. Saturating, because the directory can end smaller than it
    /// began once RocksDB removes what it wrote.
    fn finish(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        self.peak
            .load(Ordering::Relaxed)
            .saturating_sub(self.baseline)
    }
}

/// Total bytes of files under `dir`, walked recursively, or 0 when unreadable.
///
/// Recursive because the disk tier is RocksDB, which writes into a subdirectory
/// rather than into `dir` itself. A shallow scan reports zero while a gigabyte
/// of spill sits one level down, which reads as "never spilled" -- the opposite
/// of the truth.
///
/// Even so this only detects spill that is still on disk when it runs: RocksDB
/// compacts as it goes, so a sample after the read can miss a spill the read
/// certainly did. Treat a `true` as proof and a `false` as unproven, never as
/// proof of absence.
///
/// Failure-tolerant by intent: an unreadable directory is an observation
/// problem, not a benchmark failure.
fn spill_dir_bytes(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(|e| e.ok())
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => spill_dir_bytes(&e.path()),
            Ok(t) if t.is_file() => e.metadata().map(|m| m.len()).unwrap_or(0),
            _ => 0,
        })
        .sum()
}

/// Read every discovered file slice to completion, returning total rows and
/// each reader's request report.
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
) -> Result<(usize, Vec<ReaderReport>)> {
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
    // Point the reader at the directory the harness watches. Without this the
    // reader spills to its `/tmp` default while the watcher looks elsewhere, and
    // `spilled` reports the watched directory rather than the read.
    options.push((
        "hoodie.memory.spillable.map.path".to_string(),
        cfg.spill_dir.clone(),
    ));
    if cfg.use_record_position {
        options.push((
            "hoodie.merge.use.record.positions".to_string(),
            "true".to_string(),
        ));
    }

    let read_options = hudi_core::config::read_options::ReadOptions {
        projection: cfg.requested_schema.as_ref().map(|schema| {
            schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<String>>()
        }),
        ..Default::default()
    };

    if let Some(readers) = cfg.readers {
        return read_with_reader_each(
            file_slices,
            options,
            table_path,
            read_options,
            cfg.streaming,
            readers,
        )
        .await;
    }

    let reader = FileGroupReader::new_with_options(table_path, options).await?;

    // The fan-out under test. `buffer_unordered(1)` is sequential with no
    // coordination cost, which is what the single-slice baseline must stay.
    // Derived by the library, not by the harness: the gate has to exercise the
    // shipped decision, or it measures the benchmark instead of the product.
    // One benchmark process is one engine partition, hence `partitions = 1`.
    let slice_log_bytes: Vec<Option<u64>> = file_slices
        .iter()
        .map(hudi_core::file_group::file_slice::FileSlice::log_size_bytes)
        .collect();
    let concurrency = hudi_core::file_group::admission::slices_in_flight(
        cfg.scan_memory_budget,
        1,
        &slice_log_bytes,
        cfg.slice_concurrency,
    );
    if cfg.scan_memory_budget.is_some() {
        eprintln!(
            "[fg-bench] budget {} MiB over {} slices -> concurrency {concurrency} (ceiling {})",
            cfg.scan_memory_budget.unwrap_or(0) / (1024 * 1024),
            file_slices.len(),
            cfg.slice_concurrency,
        );
    }
    let start = Instant::now();
    let total_rows = futures::stream::iter(file_slices.iter())
        .map(|slice| read_slice(&reader, slice, &read_options, cfg.streaming))
        .buffer_unordered(concurrency)
        .try_fold(0usize, |acc, n| async move { Ok(acc + n) })
        .await?;
    let report = ReaderReport::capture(
        0,
        file_slices.len(),
        start.elapsed(),
        &reader.read_volume().requests,
    );

    Ok((total_rows, vec![report]))
}

/// `readers` tasks, each with a reader of its own, reading slices
/// `i, i + readers, ...` one at a time.
async fn read_with_reader_each(
    file_slices: &[FileSlice],
    options: Vec<(String, String)>,
    table_path: &str,
    read_options: hudi_core::config::read_options::ReadOptions,
    streaming: bool,
    readers: usize,
) -> Result<(usize, Vec<ReaderReport>)> {
    let readers = readers.clamp(1, file_slices.len().max(1));
    let tasks: Vec<_> = (0..readers)
        .map(|i| {
            let slices: Vec<FileSlice> = file_slices
                .iter()
                .skip(i)
                .step_by(readers)
                .cloned()
                .collect();
            let options = options.clone();
            let table_path = table_path.to_string();
            let read_options = read_options.clone();
            tokio::spawn(async move {
                let reader = FileGroupReader::new_with_options(&table_path, options).await?;
                let start = Instant::now();
                let mut rows = 0usize;
                for slice in &slices {
                    rows += read_slice(&reader, slice, &read_options, streaming).await?;
                }
                let report = ReaderReport::capture(
                    i,
                    slices.len(),
                    start.elapsed(),
                    &reader.read_volume().requests,
                );
                Ok::<_, hudi_core::error::CoreError>((rows, report))
            })
        })
        .collect();

    let mut total_rows = 0usize;
    let mut reports = Vec::with_capacity(readers);
    for task in tasks {
        let (rows, report) = task.await.map_err(|e| {
            hudi_core::error::CoreError::ReadFileSliceError(format!("reader task failed: {e}"))
        })??;
        total_rows += rows;
        reports.push(report);
    }
    Ok((total_rows, reports))
}

/// Read one slice to completion, returning its row count.
async fn read_slice(
    reader: &FileGroupReader,
    slice: &FileSlice,
    read_options: &hudi_core::config::read_options::ReadOptions,
    streaming: bool,
) -> Result<usize> {
    if streaming {
        // The streaming path yields batches and drops each as it goes, so the
        // whole result is never resident. The eager path below retains it.
        // That difference is the point of the comparison, so rows are counted
        // without collecting.
        let mut stream = reader.read_file_slice_stream(slice, read_options).await?;
        let mut rows = 0usize;
        while let Some(batch) = stream.next().await {
            rows += batch?.num_rows();
        }
        Ok(rows)
    } else {
        Ok(reader
            .read_file_slice(slice, read_options)
            .await?
            .num_rows())
    }
}

/// One reader's object-store requests over an iteration, from its
/// `ReadVolume::requests`. Latencies are microseconds, sizes bytes.
#[derive(Serialize)]
struct ReaderReport {
    reader: usize,
    slices: usize,
    /// From this reader's first read to its last, excluding construction.
    wall_ms: u64,
    requests: u64,
    failed_requests: u64,
    request_bytes: u64,
    mean_latency_us: u64,
    max_latency_us: u64,
    peak_in_flight: u64,
    /// Fraction of `wall_ms` with at least one request outstanding. Well
    /// under 1 with `mean_in_flight_while_busy` near 1 means fetch and decode
    /// take turns.
    busy_fraction: f64,
    mean_in_flight_while_busy: f64,
    /// Lower bound of the log2 bucket holding the median / 99th percentile.
    latency_us_p50: u64,
    latency_us_p99: u64,
    request_bytes_p50: u64,
    request_bytes_p99: u64,
    /// Non-empty log2 buckets as `[lower_bound, count]`.
    latency_us_histogram: Vec<[u64; 2]>,
    request_bytes_histogram: Vec<[u64; 2]>,
}

impl ReaderReport {
    fn capture(
        reader: usize,
        slices: usize,
        wall: std::time::Duration,
        stats: &hudi_core::storage::RequestStats,
    ) -> Self {
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let latency_counts: Vec<u64> = stats.latency_buckets.iter().map(load).collect();
        let size_counts: Vec<u64> = stats.size_buckets.iter().map(load).collect();
        let requests = load(&stats.count);
        let failed_requests = load(&stats.failed);
        let latency_nanos = load(&stats.latency_nanos);
        let busy_nanos = load(&stats.busy_nanos);
        let wall_nanos = wall.as_nanos() as f64;
        let ratio = |num: f64, den: f64| if den > 0.0 { num / den } else { 0.0 };
        ReaderReport {
            reader,
            slices,
            wall_ms: wall.as_millis() as u64,
            requests,
            failed_requests,
            request_bytes: load(&stats.bytes),
            mean_latency_us: (latency_nanos / (requests + failed_requests).max(1)) / 1000,
            max_latency_us: load(&stats.max_latency_nanos) / 1000,
            peak_in_flight: load(&stats.peak_in_flight),
            busy_fraction: ratio(busy_nanos as f64, wall_nanos),
            mean_in_flight_while_busy: ratio(latency_nanos as f64, busy_nanos as f64),
            latency_us_p50: bucket_percentile(&latency_counts, 0.50),
            latency_us_p99: bucket_percentile(&latency_counts, 0.99),
            request_bytes_p50: bucket_percentile(&size_counts, 0.50),
            request_bytes_p99: bucket_percentile(&size_counts, 0.99),
            latency_us_histogram: nonempty_buckets(&latency_counts),
            request_bytes_histogram: nonempty_buckets(&size_counts),
        }
    }

    fn print(&self) {
        eprintln!(
            "[fg-bench]   reader {}: slices={} requests={} failed={} bytes={}KB \
             mean={}us max={}us p50>={}us p99>={}us size_p50>={}B peak_in_flight={} \
             busy={:.0}% in_flight_while_busy={:.2}",
            self.reader,
            self.slices,
            self.requests,
            self.failed_requests,
            self.request_bytes / 1024,
            self.mean_latency_us,
            self.max_latency_us,
            self.latency_us_p50,
            self.latency_us_p99,
            self.request_bytes_p50,
            self.peak_in_flight,
            self.busy_fraction * 100.0,
            self.mean_in_flight_while_busy,
        );
    }
}

/// Lower bound of the log2 bucket in which the `q` quantile falls, or 0 when
/// the histogram is empty.
fn bucket_percentile(counts: &[u64], q: f64) -> u64 {
    let total: u64 = counts.iter().sum();
    if total == 0 {
        return 0;
    }
    let rank = ((total as f64) * q).ceil().max(1.0) as u64;
    let mut seen = 0u64;
    for (i, &n) in counts.iter().enumerate() {
        seen += n;
        if seen >= rank {
            return hudi_core::storage::RequestStats::bucket_lower_bound(i);
        }
    }
    hudi_core::storage::RequestStats::bucket_lower_bound(counts.len() - 1)
}

fn nonempty_buckets(counts: &[u64]) -> Vec<[u64; 2]> {
    counts
        .iter()
        .enumerate()
        .filter(|&(_, &n)| n > 0)
        .map(|(i, &n)| [hudi_core::storage::RequestStats::bucket_lower_bound(i), n])
        .collect()
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
    /// True when `max_rss` greatly exceeds the merge-map
    /// accounted retained bytes + headroom — i.e. the size accounting is not
    /// tracking the resident set. False on a correctly-bounded read.
    accounting_drift: bool,
    /// Whether the merge map spilled to disk during this iteration, observed by
    /// watching the spill directory. `HoodieReadStats` carries this directly but
    /// is not on the public reader surface, and a benchmark is not a reason to
    /// widen it.
    spilled: bool,
    /// Largest total seen in the spill directory while the read ran.
    spill_peak_bytes: u64,
    /// Object-store requests per reader: one entry for the shared reader, or
    /// one per reader with `--readers`.
    readers: Vec<ReaderReport>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_percentile() {
        // 1 in bucket 0, 8 in bucket 3 ([4, 8)), 1 in bucket 10 ([512, 1024)).
        let mut counts = vec![0u64; 12];
        counts[0] = 1;
        counts[3] = 8;
        counts[10] = 1;
        assert_eq!(bucket_percentile(&counts, 0.0), 0);
        assert_eq!(bucket_percentile(&counts, 0.5), 4);
        assert_eq!(bucket_percentile(&counts, 0.9), 4);
        assert_eq!(bucket_percentile(&counts, 0.99), 512);
        assert_eq!(bucket_percentile(&[0; 4], 0.5), 0);
        assert_eq!(nonempty_buckets(&counts), vec![[0, 1], [4, 8], [512, 1]]);
    }

    #[test]
    fn test_reader_report_from_empty_stats_is_zero_not_nan() {
        let stats = hudi_core::storage::RequestStats::default();
        let r = ReaderReport::capture(0, 0, std::time::Duration::ZERO, &stats);
        assert_eq!(r.requests, 0);
        assert_eq!(r.mean_latency_us, 0);
        assert_eq!(r.busy_fraction, 0.0);
        assert_eq!(r.mean_in_flight_while_busy, 0.0);
        assert!(r.latency_us_histogram.is_empty());
    }
}
