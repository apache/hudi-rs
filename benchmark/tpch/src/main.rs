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

mod config;
mod datagen;

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::BufRead;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::DataType;
use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use clap::{Parser, Subcommand};
use comfy_table::{Cell, Table};
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionConfig;
use hudi::HudiDataSource;

/// The 8 TPC-H tables.
const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

/// Total number of TPC-H queries.
const NUM_QUERIES: usize = 22;

/// Cloud URL scheme prefixes.
const CLOUD_SCHEMES: &[&str] = &["s3://", "s3a://", "gs://", "wasb://", "wasbs://", "az://"];

#[derive(Parser)]
#[command(name = "tpch", about = "TPC-H benchmark tool for Apache Hudi")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate TPC-H parquet data
    Generate {
        /// TPC-H scale factor
        #[arg(long, default_value_t = 1.0)]
        scale_factor: f64,

        /// Output directory (local path or cloud URL); defaults to data/sf{N}-parquet
        #[arg(long)]
        output_dir: Option<String>,
    },
    /// Render CTAS SQL from scale-factor config
    RenderCtas {
        /// TPC-H scale factor (loads config/sf{N}.yaml)
        #[arg(long)]
        scale_factor: f64,

        /// Parquet source base path (e.g., /opt/parquet or gs://bucket/path)
        #[arg(long)]
        parquet_base: String,

        /// Hudi output base path (e.g., /opt/hudi or gs://bucket/path)
        #[arg(long)]
        hudi_base: String,
    },
    /// Render benchmark SQL (table registrations + query iterations)
    RenderBenchSql {
        /// TPC-H scale factor (loads config/sf{N}.yaml)
        #[arg(long)]
        scale_factor: f64,

        /// Hudi tables base path (e.g., /opt/hudi)
        #[arg(long)]
        hudi_base: String,

        /// Comma-separated query numbers (default: all 22)
        #[arg(long)]
        queries: Option<String>,

        /// Number of iterations per query
        #[arg(long, default_value_t = 3)]
        iterations: usize,
    },
    /// Output spark-submit arguments from scale-factor config (one per line)
    SparkArgs {
        /// TPC-H scale factor (loads config/sf{N}.yaml)
        #[arg(long)]
        scale_factor: f64,

        /// Command profile to use: "create-tables" or "bench"
        #[arg(long)]
        command: String,
    },
    /// Run TPC-H benchmark queries via DataFusion (Hudi, Parquet, or both)
    Bench {
        /// Hudi tables location (local path or cloud URL)
        #[arg(long)]
        hudi_dir: Option<String>,

        /// Parquet tables location (local path or cloud URL)
        #[arg(long)]
        parquet_dir: Option<String>,

        /// TPC-H scale factor (used for query parameter substitution)
        #[arg(long, default_value_t = 1.0)]
        scale_factor: f64,

        /// Comma-separated query numbers to run (e.g., "1,3,6"); defaults to all 22
        #[arg(long)]
        queries: Option<String>,

        /// Number of measured iterations per query
        #[arg(long, default_value_t = 3)]
        iterations: usize,

        /// Number of unmeasured warmup iterations per query
        #[arg(long, default_value_t = 2)]
        warmup: usize,

        /// DataFusion memory limit (e.g., "3g", "512m"); unlimited if not set
        #[arg(long)]
        memory_limit: Option<String>,
    },
    /// Validate Hudi query results against Parquet (runs each query once, compares output)
    Validate {
        /// Hudi tables location (local path or cloud URL)
        #[arg(long)]
        hudi_dir: String,

        /// Parquet tables location (local path or cloud URL)
        #[arg(long)]
        parquet_dir: String,

        /// TPC-H scale factor (used for query parameter substitution)
        #[arg(long, default_value_t = 1.0)]
        scale_factor: f64,

        /// Comma-separated query numbers to run (e.g., "1,3,6"); defaults to all 22
        #[arg(long)]
        queries: Option<String>,

        /// DataFusion memory limit (e.g., "3g", "512m"); unlimited if not set
        #[arg(long)]
        memory_limit: Option<String>,
    },
    /// Parse Spark benchmark JSON output into a timing table
    ParseSparkOutput {
        /// Input file (reads from stdin if omitted)
        #[arg(long)]
        input: Option<String>,
    },
}

/// Check if a path string is a cloud URL.
fn is_cloud_url(path: &str) -> bool {
    CLOUD_SCHEMES.iter().any(|s| path.starts_with(s))
}

/// Resolve a local path to an absolute path string, or return cloud URL as-is.
fn resolve_path(path: &str) -> std::result::Result<String, String> {
    if is_cloud_url(path) {
        Ok(path.to_string())
    } else {
        fs::canonicalize(path)
            .map(|p| p.to_string_lossy().to_string())
            .map_err(|e| format!("Failed to resolve path {path}: {e}"))
    }
}

/// Collect cloud storage env vars as options for object_store.
fn collect_cloud_env_vars() -> Vec<(String, String)> {
    std::env::vars()
        .filter(|(k, _)| {
            k.starts_with("AWS_")
                || k.starts_with("GOOGLE_")
                || k.starts_with("AZURE_")
                || k.starts_with("OBJECT_STORE_")
        })
        .collect()
}

/// Parse a memory size string (e.g., "3g", "512m", "1024k") into bytes.
fn parse_memory_size(s: &str) -> std::result::Result<usize, String> {
    let s = s.trim().to_lowercase();
    let (num_str, multiplier) = if let Some(n) = s.strip_suffix('g') {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix('k') {
        (n, 1024)
    } else {
        (s.as_str(), 1usize)
    };
    let num: f64 = num_str
        .parse()
        .map_err(|_| format!("Invalid memory size: {s}"))?;
    Ok((num * multiplier as f64) as usize)
}

/// Create a SessionContext, optionally bounded by a memory pool.
fn create_session_context(memory_limit: Option<&str>) -> std::result::Result<SessionContext, String> {
    match memory_limit {
        Some(limit) => {
            let pool_size = parse_memory_size(limit)?;
            let pool = FairSpillPool::new(pool_size);
            let runtime = RuntimeEnvBuilder::default()
                .with_memory_pool(Arc::new(pool))
                .build_arc()
                .map_err(|e| format!("Failed to build runtime: {e}"))?;
            Ok(SessionContext::new_with_config_rt(
                SessionConfig::new(),
                runtime,
            ))
        }
        None => Ok(SessionContext::new()),
    }
}

/// Register a cloud object store on the SessionContext's RuntimeEnv.
fn register_cloud_store(ctx: &SessionContext, base_url: &str) -> Result<()> {
    let url = url::Url::parse(base_url).map_err(|e| {
        datafusion::error::DataFusionError::Plan(format!("Invalid URL {base_url}: {e}"))
    })?;
    let cloud_opts: HashMap<String, String> = collect_cloud_env_vars().into_iter().collect();
    let (store, _) = object_store::parse_url_opts(&url, &cloud_opts).map_err(|e| {
        datafusion::error::DataFusionError::Plan(format!(
            "Failed to create object store for {base_url}: {e}"
        ))
    })?;
    ctx.runtime_env()
        .register_object_store(&url, Arc::new(store));
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Generate {
            scale_factor,
            output_dir,
        } => {
            let dir = output_dir.unwrap_or_else(|| {
                let sf_label = if scale_factor == scale_factor.floor() && scale_factor >= 1.0 {
                    format!("sf{}", scale_factor as u64)
                } else {
                    format!("sf{scale_factor}")
                };
                let default = Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("data")
                    .join(format!("{sf_label}-parquet"));
                default.to_string_lossy().to_string()
            });
            if !is_cloud_url(&dir) {
                std::fs::create_dir_all(&dir).map_err(|e| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Failed to create output dir {dir}: {e}"
                    ))
                })?;
            }
            datagen::run_generate(scale_factor, &dir)
                .await
                .map_err(|e| {
                    datafusion::error::DataFusionError::Plan(format!("Generation failed: {e}"))
                })
        }
        Commands::RenderCtas {
            scale_factor,
            parquet_base,
            hudi_base,
        } => {
            let cfg = config::ScaleFactorConfig::load(scale_factor).map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!("{e}"))
            })?;
            print!("{}", cfg.render_ctas_sql(&parquet_base, &hudi_base));
            Ok(())
        }
        Commands::RenderBenchSql {
            scale_factor,
            hudi_base,
            queries,
            iterations,
        } => {
            let cfg = config::ScaleFactorConfig::load(scale_factor).map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!("{e}"))
            })?;
            let query_nums = parse_query_numbers(queries);
            let sql = cfg
                .render_bench_sql(&hudi_base, &query_nums, iterations, scale_factor)
                .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{e}")))?;
            print!("{sql}");
            Ok(())
        }
        Commands::SparkArgs {
            scale_factor,
            command,
        } => {
            let cfg = config::ScaleFactorConfig::load(scale_factor).map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!("{e}"))
            })?;
            let args = cfg
                .render_spark_args(&command)
                .map_err(datafusion::error::DataFusionError::Plan)?;
            for arg in args {
                println!("{arg}");
            }
            Ok(())
        }
        Commands::Bench {
            hudi_dir,
            parquet_dir,
            scale_factor,
            queries,
            iterations,
            warmup,
            memory_limit,
        } => run_bench(hudi_dir.as_deref(), parquet_dir.as_deref(), scale_factor, queries, warmup, iterations, memory_limit.as_deref()).await,
        Commands::Validate {
            hudi_dir,
            parquet_dir,
            scale_factor,
            queries,
            memory_limit,
        } => run_validate(&hudi_dir, &parquet_dir, scale_factor, queries, memory_limit.as_deref()).await,
        Commands::ParseSparkOutput { input } => run_parse_spark_output(input.as_deref()),
    }
}

/// Parse query numbers from the user-provided comma-separated string, or return all 22.
fn parse_query_numbers(queries: Option<String>) -> Vec<usize> {
    match queries {
        Some(s) => s
            .split(',')
            .filter_map(|q| q.trim().parse::<usize>().ok())
            .filter(|&q| q >= 1 && q <= NUM_QUERIES)
            .collect(),
        None => (1..=NUM_QUERIES).collect(),
    }
}

/// Load a SQL query file, applying scale-factor-dependent substitutions.
fn load_query(query_num: usize, scale_factor: f64) -> std::result::Result<String, String> {
    let cache_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("queries");
    let file_name = format!("q{query_num}.sql");
    let path = cache_dir.join(&file_name);
    let sql = fs::read_to_string(&path).map_err(|e| format!("Failed to read {file_name}: {e}"))?;
    let q11_fraction = format!("{:.10}", 0.0001 / scale_factor);
    Ok(sql.replace("${Q11_FRACTION}", &q11_fraction))
}

/// Register all 8 TPC-H Hudi tables. Supports local paths and cloud URLs.
async fn register_hudi_tables(ctx: &SessionContext, base_dir: &str) -> Result<()> {
    let resolved =
        resolve_path(base_dir).map_err(datafusion::error::DataFusionError::Plan)?;

    for table_name in TPCH_TABLES {
        let table_uri = if is_cloud_url(&resolved) {
            format!("{}/{table_name}", resolved.trim_end_matches('/'))
        } else {
            let table_path = Path::new(&resolved).join(table_name);
            url::Url::from_file_path(&table_path)
                .map_err(|_| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Failed to create file URL for {}",
                        table_path.display()
                    ))
                })?
                .to_string()
        };
        let hudi = HudiDataSource::new(&table_uri).await?;
        ctx.register_table(*table_name, Arc::new(hudi))?;
    }
    Ok(())
}

/// Register all 8 TPC-H parquet tables. Supports local paths and cloud URLs.
async fn register_parquet_tables(ctx: &SessionContext, base_dir: &str) -> Result<()> {
    let resolved =
        resolve_path(base_dir).map_err(datafusion::error::DataFusionError::Plan)?;

    if is_cloud_url(&resolved) {
        register_cloud_store(ctx, &resolved)?;
    }

    for table_name in TPCH_TABLES {
        let table_path = if is_cloud_url(&resolved) {
            format!("{}/{table_name}", resolved.trim_end_matches('/'))
        } else {
            Path::new(&resolved)
                .join(table_name)
                .to_string_lossy()
                .to_string()
        };
        ctx.register_parquet(*table_name, &table_path, Default::default())
            .await?;
    }
    Ok(())
}

/// Collect a DataFrame into a Vec of RecordBatches.
async fn collect_results(df: DataFrame) -> Result<Vec<RecordBatch>> {
    df.collect().await
}

/// Benchmark a single source (hudi or parquet) and return per-query timings and last batches.
async fn bench_source(
    ctx: &SessionContext,
    query_nums: &[usize],
    warmup: usize,
    iterations: usize,
    scale_factor: f64,
) -> Vec<QueryResult> {
    let total_runs = warmup + iterations;
    let mut results = Vec::new();

    for query_num in query_nums {
        let sql = match load_query(*query_num, scale_factor) {
            Ok(s) => s,
            Err(e) => {
                results.push(QueryResult {
                    query_num: *query_num,
                    timings_ms: vec![],
                    last_batches: vec![],
                    error: Some(e),
                });
                continue;
            }
        };

        let mut timings_ms: Vec<f64> = Vec::with_capacity(iterations);
        let mut last_batches: Vec<RecordBatch> = Vec::new();
        let mut error = None;

        // Split multi-statement queries (e.g., Q15: CREATE VIEW; SELECT; DROP VIEW)
        let statements: Vec<&str> = sql
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        for i in 0..total_runs {
            if i < warmup {
                print!("  Q{:02} warmup {}/{}...", query_num, i + 1, warmup);
            } else {
                print!(
                    "  Q{:02} iter {}/{}...",
                    query_num,
                    i - warmup + 1,
                    iterations
                );
            }

            let start = Instant::now();
            let mut iter_error = None;
            let mut iter_batches = Vec::new();

            for stmt in &statements {
                match ctx.sql(stmt).await {
                    Ok(df) => match collect_results(df).await {
                        Ok(batches) => {
                            if !batches.is_empty() {
                                iter_batches = batches;
                            }
                        }
                        Err(e) => {
                            iter_error = Some(format!("{e}"));
                            break;
                        }
                    },
                    Err(e) => {
                        iter_error = Some(format!("{e}"));
                        break;
                    }
                }
            }

            let elapsed = start.elapsed().as_secs_f64() * 1000.0;

            if let Some(e) = iter_error {
                println!(" ERROR");
                error = Some(e);
                break;
            }

            println!(" {elapsed:.1}ms");

            if i >= warmup {
                timings_ms.push(elapsed);
            }
            if i == total_runs - 1 {
                last_batches = iter_batches;
            }
        }

        results.push(QueryResult {
            query_num: *query_num,
            timings_ms,
            last_batches,
            error,
        });
    }

    results
}

struct QueryResult {
    query_num: usize,
    timings_ms: Vec<f64>,
    last_batches: Vec<RecordBatch>,
    error: Option<String>,
}

struct TimingStats {
    min: f64,
    median: f64,
    mean: f64,
    max: f64,
}

fn compute_stats(timings: &[f64]) -> Option<TimingStats> {
    if timings.is_empty() {
        return None;
    }
    let mut sorted = timings.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let min = sorted[0];
    let max = sorted[sorted.len() - 1];
    let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
    let median = if sorted.len() % 2 == 0 {
        let mid = sorted.len() / 2;
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[sorted.len() / 2]
    };
    Some(TimingStats {
        min,
        median,
        mean,
        max,
    })
}

/// Run the benchmark against Hudi, Parquet, or both.
async fn run_bench(
    hudi_dir: Option<&str>,
    parquet_dir: Option<&str>,
    scale_factor: f64,
    queries: Option<String>,
    warmup: usize,
    iterations: usize,
    memory_limit: Option<&str>,
) -> Result<()> {
    if hudi_dir.is_none() && parquet_dir.is_none() {
        return Err(datafusion::error::DataFusionError::Plan(
            "At least one of --hudi-dir or --parquet-dir must be provided".to_string(),
        ));
    }

    let query_nums = parse_query_numbers(queries);

    if let Some(limit) = memory_limit {
        println!("DataFusion memory limit: {limit}");
    }
    println!(
        "Warmup: {warmup} iteration(s), Measured: {iterations} iteration(s)"
    );

    if let Some(hudi_dir) = hudi_dir {
        let ctx = create_session_context(memory_limit)
            .map_err(datafusion::error::DataFusionError::Plan)?;
        println!("Registering Hudi tables from {hudi_dir}");
        register_hudi_tables(&ctx, hudi_dir).await?;
        println!("Benchmarking Hudi...");
        let results = bench_source(&ctx, &query_nums, warmup, iterations, scale_factor).await;
        print_single_table("Hudi", &results);
    }

    if let Some(parquet_dir) = parquet_dir {
        let ctx = create_session_context(memory_limit)
            .map_err(datafusion::error::DataFusionError::Plan)?;
        println!("Registering Parquet tables from {parquet_dir}");
        register_parquet_tables(&ctx, parquet_dir).await?;
        println!("Benchmarking Parquet...");
        let results = bench_source(&ctx, &query_nums, warmup, iterations, scale_factor).await;
        print_single_table("Parquet", &results);
    }

    Ok(())
}

/// Run validation: query both Hudi and Parquet once, compare results.
async fn run_validate(
    hudi_dir: &str,
    parquet_dir: &str,
    scale_factor: f64,
    queries: Option<String>,
    memory_limit: Option<&str>,
) -> Result<()> {
    let query_nums = parse_query_numbers(queries);

    if let Some(limit) = memory_limit {
        println!("DataFusion memory limit: {limit}");
    }

    println!("Registering Hudi tables from {hudi_dir}");
    let hudi_ctx = create_session_context(memory_limit)
        .map_err(datafusion::error::DataFusionError::Plan)?;
    register_hudi_tables(&hudi_ctx, hudi_dir).await?;

    println!("Registering Parquet tables from {parquet_dir}");
    let parquet_ctx = create_session_context(memory_limit)
        .map_err(datafusion::error::DataFusionError::Plan)?;
    register_parquet_tables(&parquet_ctx, parquet_dir).await?;

    println!("Running Hudi queries...");
    let hudi_results = bench_source(&hudi_ctx, &query_nums, 0, 1, scale_factor).await;

    println!("Running Parquet queries...");
    let parquet_results = bench_source(&parquet_ctx, &query_nums, 0, 1, scale_factor).await;

    print_validation_table(&query_nums, &hudi_results, &parquet_results);

    Ok(())
}

/// Parse Spark benchmark JSON output into a timing table.
fn run_parse_spark_output(input: Option<&str>) -> Result<()> {
    let reader: Box<dyn BufRead> = match input {
        Some(path) => {
            let file = fs::File::open(path).map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!("Failed to open {path}: {e}"))
            })?;
            Box::new(std::io::BufReader::new(file))
        }
        None => Box::new(std::io::BufReader::new(std::io::stdin())),
    };

    let results = parse_spark_timings(reader);
    if results.is_empty() {
        println!("No benchmark data found in input.");
    } else {
        print_single_table("Spark", &results);
    }
    Ok(())
}

/// Parse JSON lines from the PySpark bench script.
///
/// Each line is: {"query": N, "elapsed_ms": X.X}
/// Warmup iterations are already excluded by the Python script.
fn parse_spark_timings(reader: Box<dyn BufRead>) -> Vec<QueryResult> {
    let mut all_timings: BTreeMap<usize, Vec<f64>> = BTreeMap::new();

    for line in reader.lines().map_while(|l| l.ok()) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
            if let (Some(q), Some(ms)) = (v["query"].as_u64(), v["elapsed_ms"].as_f64()) {
                all_timings.entry(q as usize).or_default().push(ms);
            }
        }
    }

    all_timings
        .into_iter()
        .map(|(q, times)| QueryResult {
            query_num: q,
            timings_ms: times,
            last_batches: vec![],
            error: None,
        })
        .collect()
}

fn print_single_table(label: &str, results: &[QueryResult]) {
    let mut table = Table::new();
    table.set_header(vec![
        Cell::new("Query"),
        Cell::new(format!("{label} Min (ms)")),
        Cell::new(format!("{label} Median (ms)")),
        Cell::new(format!("{label} Mean (ms)")),
        Cell::new(format!("{label} Max (ms)")),
        Cell::new("Status"),
    ]);

    for r in results {
        if let Some(ref e) = r.error {
            table.add_row(vec![
                Cell::new(format!("Q{:02}", r.query_num)),
                Cell::new("-"),
                Cell::new("-"),
                Cell::new("-"),
                Cell::new("-"),
                Cell::new(format!("ERROR: {e}")),
            ]);
        } else if let Some(stats) = compute_stats(&r.timings_ms) {
            table.add_row(vec![
                Cell::new(format!("Q{:02}", r.query_num)),
                Cell::new(format!("{:.1}", stats.min)),
                Cell::new(format!("{:.1}", stats.median)),
                Cell::new(format!("{:.1}", stats.mean)),
                Cell::new(format!("{:.1}", stats.max)),
                Cell::new("OK"),
            ]);
        }
    }

    println!("{table}");
}

fn print_validation_table(
    query_nums: &[usize],
    hudi_results: &[QueryResult],
    parquet_results: &[QueryResult],
) {
    let mut table = Table::new();
    table.set_header(vec![
        Cell::new("Query"),
        Cell::new("Hudi (ms)"),
        Cell::new("Parquet (ms)"),
        Cell::new("Result"),
    ]);

    for (i, qn) in query_nums.iter().enumerate() {
        let hr = &hudi_results[i];
        let pr = &parquet_results[i];

        let h_err = hr.error.as_deref();
        let p_err = pr.error.as_deref();

        if h_err.is_some() || p_err.is_some() {
            let err_msg = h_err.or(p_err).unwrap_or("unknown error");
            table.add_row(vec![
                Cell::new(format!("Q{qn:02}")),
                Cell::new(if h_err.is_some() { "-" } else { "OK" }),
                Cell::new(if p_err.is_some() { "-" } else { "OK" }),
                Cell::new(format!("ERROR: {err_msg}")),
            ]);
            continue;
        }

        let h_ms = hr.timings_ms.first().map(|t| format!("{t:.1}")).unwrap_or("-".into());
        let p_ms = pr.timings_ms.first().map(|t| format!("{t:.1}")).unwrap_or("-".into());
        let validation = compare_batches(&hr.last_batches, &pr.last_batches);

        table.add_row(vec![
            Cell::new(format!("Q{qn:02}")),
            Cell::new(h_ms),
            Cell::new(p_ms),
            Cell::new(validation),
        ]);
    }

    println!("{table}");
}

/// Compare two sets of record batches for correctness validation.
fn compare_batches(actual: &[RecordBatch], expected: &[RecordBatch]) -> String {
    let actual_rows = match batches_to_csv_rows(actual) {
        Ok(r) => r,
        Err(e) => return format!("ERROR: {e}"),
    };
    let expected_rows = match batches_to_csv_rows(expected) {
        Ok(r) => r,
        Err(e) => return format!("ERROR: {e}"),
    };

    if actual_rows.len() != expected_rows.len() {
        return format!(
            "FAIL (rows: {} vs {})",
            actual_rows.len(),
            expected_rows.len()
        );
    }

    let mut actual_sorted = actual_rows;
    actual_sorted.sort();
    let mut expected_sorted = expected_rows;
    expected_sorted.sort();

    for (i, (a, e)) in actual_sorted.iter().zip(expected_sorted.iter()).enumerate() {
        if !rows_match(a, e) {
            return format!("FAIL (row {i} mismatch)");
        }
    }

    "PASS".to_string()
}

/// Compare two CSV row strings, using tolerance for floating-point values.
fn rows_match(actual: &str, expected: &str) -> bool {
    let actual_cols: Vec<&str> = actual.split(',').collect();
    let expected_cols: Vec<&str> = expected.split(',').collect();

    if actual_cols.len() != expected_cols.len() {
        return false;
    }

    for (a, e) in actual_cols.iter().zip(expected_cols.iter()) {
        if a == e {
            continue;
        }
        match (a.parse::<f64>(), e.parse::<f64>()) {
            (Ok(av), Ok(ev)) => {
                let diff = (av - ev).abs();
                let max_abs = av.abs().max(ev.abs());
                if max_abs == 0.0 {
                    if diff > 1e-10 {
                        return false;
                    }
                } else if diff / max_abs > 1e-6 {
                    return false;
                }
            }
            _ => return false,
        }
    }

    true
}

/// Convert record batches to CSV-like row strings for comparison.
fn batches_to_csv_rows(batches: &[RecordBatch]) -> std::result::Result<Vec<String>, String> {
    let mut rows = Vec::new();
    let fmt_opts = FormatOptions::default();

    for batch in batches {
        let formatters: Vec<ArrayFormatter> = batch
            .columns()
            .iter()
            .map(|col| ArrayFormatter::try_new(col.as_ref(), &fmt_opts))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to create formatter: {e}"))?;

        for row_idx in 0..batch.num_rows() {
            let cols: Vec<String> = batch
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(col_idx, field)| {
                    if batch.column(col_idx).is_null(row_idx) {
                        return "".to_string();
                    }
                    match field.data_type() {
                        DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
                            formatters[col_idx].value(row_idx).to_string()
                        }
                        _ => formatters[col_idx].value(row_idx).to_string(),
                    }
                })
                .collect();
            rows.push(cols.join(","));
        }
    }

    Ok(rows)
}
