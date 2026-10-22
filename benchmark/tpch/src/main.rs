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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::BufRead;
use std::path::Path;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::datatypes::DataType;
use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use clap::{Parser, Subcommand};
use comfy_table::{Cell, Table};
use datafusion::common::ScalarValue;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionConfig;
use hudi::HudiDataSource;
use serde::{Deserialize, Serialize};

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
    /// Verify every TPC-H Hudi table is readable at a base path
    CheckTables {
        /// Hudi tables base path (e.g., /opt/hudi or s3://bucket/path)
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

        /// Number of iterations per query (overrides config)
        #[arg(long)]
        iterations: Option<usize>,
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
    /// Print bench defaults from config (warmup and iterations)
    BenchDefaults {
        /// TPC-H scale factor (loads config/sf{N}.yaml)
        #[arg(long, default_value_t = 1.0)]
        scale_factor: f64,
    },
    /// Run TPC-H benchmark queries via DataFusion (Hudi, Parquet, or both)
    Bench {
        /// Hudi tables location (local path or cloud URL)
        #[arg(long)]
        hudi_dir: Option<String>,

        /// Parquet tables location (local path or cloud URL)
        #[arg(long)]
        parquet_dir: Option<String>,

        /// TPC-H scale factor (used for query parameter substitution and config loading)
        #[arg(long, default_value_t = 1.0)]
        scale_factor: f64,

        /// Comma-separated query numbers to run (e.g., "1,3,6"); defaults to all 22
        #[arg(long)]
        queries: Option<String>,

        /// Number of measured iterations per query (overrides config)
        #[arg(long)]
        iterations: Option<usize>,

        /// Number of unmeasured warmup iterations per query (overrides config)
        #[arg(long)]
        warmup: Option<usize>,

        /// DataFusion memory limit (e.g., "3g", "512m"); unlimited if not set
        #[arg(long)]
        memory_limit: Option<String>,

        /// Directory to persist results as JSON (enables result saving)
        #[arg(long)]
        output_dir: Option<String>,

        /// Engine label for persisted results (e.g., "datafusion")
        #[arg(long)]
        engine_label: Option<String>,

        /// Format label for persisted results (e.g., "hudi"); auto-detected if omitted
        #[arg(long)]
        format_label: Option<String>,

        /// Display name for charts (e.g., "datafusion+hudi-rs"); defaults to engine_label
        #[arg(long)]
        display_name: Option<String>,
    },
    /// Print DataFusion plans (EXPLAIN / EXPLAIN ANALYZE) for TPC-H queries
    Explain {
        /// Hudi tables location (local path or cloud URL)
        #[arg(long)]
        hudi_dir: Option<String>,

        /// Parquet tables location (local path or cloud URL)
        #[arg(long)]
        parquet_dir: Option<String>,

        /// TPC-H scale factor (used for query parameter substitution and config loading)
        #[arg(long, default_value_t = 1.0)]
        scale_factor: f64,

        /// Comma-separated query numbers to run (e.g., "1,3,6"); defaults to all 22
        #[arg(long)]
        queries: Option<String>,

        /// Execute the queries and report per-operator runtime metrics (EXPLAIN ANALYZE)
        #[arg(long)]
        analyze: bool,

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

        /// Directory to persist results as JSON
        #[arg(long)]
        output_dir: Option<String>,

        /// Engine label for persisted results (default: "spark")
        #[arg(long)]
        engine_label: Option<String>,

        /// Format label for persisted results (e.g., "hudi")
        #[arg(long)]
        format_label: Option<String>,

        /// Display name for charts (e.g., "spark+hudi"); defaults to engine_label
        #[arg(long)]
        display_name: Option<String>,

        /// TPC-H scale factor (used for result file naming)
        #[arg(long, default_value_t = 1.0)]
        scale_factor: f64,
    },
    /// Compare persisted benchmark results with terminal bar charts
    Compare {
        /// Directory containing result JSON files
        #[arg(long)]
        results_dir: String,

        /// Comma-separated result file stems (e.g., "datafusion_hudi_sf1,spark_hudi_sf1")
        #[arg(long)]
        runs: String,
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

/// Create a SessionContext from DataFusion config.
fn create_session_context(
    df_conf: &config::DataFusionConfig,
) -> std::result::Result<SessionContext, String> {
    let mut session_config = SessionConfig::new();
    for (key, value) in &df_conf.settings {
        session_config = session_config.set(key, &ScalarValue::Utf8(Some(value.clone())));
    }
    match &df_conf.memory_limit {
        Some(limit) => {
            let pool_size = parse_memory_size(limit)?;
            let pool = FairSpillPool::new(pool_size);
            let runtime = RuntimeEnvBuilder::default()
                .with_memory_pool(Arc::new(pool))
                .build_arc()
                .map_err(|e| format!("Failed to build runtime: {e}"))?;
            Ok(SessionContext::new_with_config_rt(session_config, runtime))
        }
        None => Ok(SessionContext::new_with_config(session_config)),
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
    env_logger::init();
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
            let cfg = config::ScaleFactorConfig::load(scale_factor)
                .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{e}")))?;
            print!("{}", cfg.render_ctas_sql(&parquet_base, &hudi_base));
            Ok(())
        }
        Commands::CheckTables { hudi_base } => {
            let missing = missing_hudi_tables(&hudi_base).await?;
            if missing.is_empty() {
                Ok(())
            } else {
                Err(datafusion::error::DataFusionError::Plan(format!(
                    "Hudi tables not found at {hudi_base}: {}",
                    missing.join(", ")
                )))
            }
        }
        Commands::RenderBenchSql {
            scale_factor,
            hudi_base,
            queries,
            iterations,
        } => {
            let cfg = config::ScaleFactorConfig::load(scale_factor)
                .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{e}")))?;
            let iterations = iterations.unwrap_or(cfg.bench.iterations);
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
            let cfg = config::ScaleFactorConfig::load(scale_factor)
                .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{e}")))?;
            let args = cfg
                .render_spark_args(&command)
                .map_err(datafusion::error::DataFusionError::Plan)?;
            for arg in args {
                println!("{arg}");
            }
            Ok(())
        }
        Commands::BenchDefaults { scale_factor } => {
            let cfg = config::ScaleFactorConfig::load(scale_factor)
                .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{e}")))?;
            println!("{} {}", cfg.bench.warmup, cfg.bench.iterations);
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
            output_dir,
            engine_label,
            format_label,
            display_name,
        } => {
            let cfg = config::ScaleFactorConfig::load(scale_factor)
                .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{e}")))?;
            let warmup = warmup.unwrap_or(cfg.bench.warmup);
            let iterations = iterations.unwrap_or(cfg.bench.iterations);
            let mut df_conf = cfg.bench.datafusion_conf;
            if memory_limit.is_some() {
                df_conf.memory_limit = memory_limit;
            }
            run_bench(
                hudi_dir.as_deref(),
                parquet_dir.as_deref(),
                scale_factor,
                queries,
                warmup,
                iterations,
                &df_conf,
                output_dir.as_deref(),
                engine_label.as_deref(),
                format_label.as_deref(),
                display_name.as_deref(),
            )
            .await
        }
        Commands::Explain {
            hudi_dir,
            parquet_dir,
            scale_factor,
            queries,
            analyze,
            memory_limit,
        } => {
            let cfg = config::ScaleFactorConfig::load(scale_factor)
                .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{e}")))?;
            let mut df_conf = cfg.bench.datafusion_conf;
            if memory_limit.is_some() {
                df_conf.memory_limit = memory_limit;
            }
            run_explain(
                hudi_dir.as_deref(),
                parquet_dir.as_deref(),
                scale_factor,
                queries,
                analyze,
                &df_conf,
            )
            .await
        }
        Commands::Validate {
            hudi_dir,
            parquet_dir,
            scale_factor,
            queries,
            memory_limit,
        } => {
            let cfg = config::ScaleFactorConfig::load(scale_factor)
                .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{e}")))?;
            let mut df_conf = cfg.bench.datafusion_conf;
            if memory_limit.is_some() {
                df_conf.memory_limit = memory_limit;
            }
            run_validate(&hudi_dir, &parquet_dir, scale_factor, queries, &df_conf).await
        }
        Commands::ParseSparkOutput {
            input,
            output_dir,
            engine_label,
            format_label,
            display_name,
            scale_factor,
        } => run_parse_spark_output(
            input.as_deref(),
            output_dir.as_deref(),
            engine_label.as_deref(),
            format_label.as_deref(),
            display_name.as_deref(),
            scale_factor,
        ),
        Commands::Compare { results_dir, runs } => run_compare(&results_dir, &runs),
    }
}

/// Parse query numbers from the user-provided comma-separated string, or return all 22.
fn parse_query_numbers(queries: Option<String>) -> Vec<usize> {
    match queries {
        Some(s) => s
            .split(',')
            .filter_map(|q| q.trim().parse::<usize>().ok())
            .filter(|&q| (1..=NUM_QUERIES).contains(&q))
            .collect(),
        None => (1..=NUM_QUERIES).collect(),
    }
}

/// Load a SQL query file, applying scale-factor-dependent substitutions.
fn load_query(query_num: usize, scale_factor: f64) -> std::result::Result<String, String> {
    let cache_dir = std::env::var("TPCH_QUERY_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| Path::new(env!("CARGO_MANIFEST_DIR")).join("queries"));
    let file_name = format!("q{query_num}.sql");
    let path = cache_dir.join(&file_name);
    let sql = fs::read_to_string(&path).map_err(|e| format!("Failed to read {file_name}: {e}"))?;
    let q11_fraction = format!("{:.10}", 0.0001 / scale_factor);
    Ok(sql.replace("${Q11_FRACTION}", &q11_fraction))
}

/// Names of the TPC-H tables that cannot be opened under `base_dir`.
///
/// Opens each through the same `HudiDataSource` the benchmark uses, so a table
/// that passes here is one the benchmark can actually read, not merely a
/// directory that exists.
async fn missing_hudi_tables(base_dir: &str) -> Result<Vec<String>> {
    let resolved = resolve_path(base_dir).map_err(datafusion::error::DataFusionError::Plan)?;

    let mut missing = Vec::new();
    for table_name in TPCH_TABLES {
        let table_uri = hudi_table_uri(&resolved, table_name)?;
        if HudiDataSource::new(&table_uri).await.is_err() {
            missing.push((*table_name).to_string());
        }
    }
    Ok(missing)
}

/// Build the URI for one Hudi table under a local path or cloud URL.
fn hudi_table_uri(resolved_base: &str, table_name: &str) -> Result<String> {
    if is_cloud_url(resolved_base) {
        Ok(format!(
            "{}/{table_name}",
            resolved_base.trim_end_matches('/')
        ))
    } else {
        let table_path = Path::new(resolved_base).join(table_name);
        Ok(url::Url::from_file_path(&table_path)
            .map_err(|_| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Failed to create file URL for {}",
                    table_path.display()
                ))
            })?
            .to_string())
    }
}

/// Register all 8 TPC-H Hudi tables. Supports local paths and cloud URLs.
async fn register_hudi_tables(ctx: &SessionContext, base_dir: &str) -> Result<()> {
    let resolved = resolve_path(base_dir).map_err(datafusion::error::DataFusionError::Plan)?;

    for table_name in TPCH_TABLES {
        let table_uri = hudi_table_uri(&resolved, table_name)?;
        let hudi = HudiDataSource::new(&table_uri).await?;
        ctx.register_table(*table_name, Arc::new(hudi))?;
    }
    Ok(())
}

/// Register all 8 TPC-H parquet tables. Supports local paths and cloud URLs.
async fn register_parquet_tables(ctx: &SessionContext, base_dir: &str) -> Result<()> {
    let resolved = resolve_path(base_dir).map_err(datafusion::error::DataFusionError::Plan)?;

    if is_cloud_url(&resolved) {
        register_cloud_store(ctx, &resolved)?;
    }

    for table_name in TPCH_TABLES {
        // The trailing slash is what marks the path as a directory to list. A
        // local path can be stat'ed, but an object store prefix cannot, so
        // without it each table reads as a single file and is rejected for not
        // ending in the parquet extension.
        let table_path = if is_cloud_url(&resolved) {
            format!("{}/{table_name}/", resolved.trim_end_matches('/'))
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

/// Split a query file into executable statements. Comment lines are stripped
/// first so semicolons inside comments (e.g., license headers) don't produce
/// spurious empty statements; Q15 is CREATE VIEW; SELECT; DROP VIEW.
fn split_statements(sql: &str) -> Vec<String> {
    let sql_no_comments: String = sql
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    sql_no_comments
        .split(';')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn explain_keyword(analyze: bool) -> &'static str {
    if analyze {
        "EXPLAIN ANALYZE"
    } else {
        "EXPLAIN"
    }
}

/// Wrap a statement in EXPLAIN when it is a query; returns None for DDL
/// (e.g., Q15's CREATE VIEW / DROP VIEW), which must run unwrapped so the
/// query statement can plan against it.
fn wrap_in_explain(stmt: &str, analyze: bool) -> Option<String> {
    let head: String = stmt.chars().take(6).collect::<String>().to_uppercase();
    if head.starts_with("SELECT") || head.starts_with("WITH") || head.starts_with("VALUES") {
        Some(format!("{} {stmt}", explain_keyword(analyze)))
    } else {
        None
    }
}

/// Print EXPLAIN result batches as raw text; a bordered table wraps badly for
/// wide plans.
fn print_plan_batches(batches: &[RecordBatch]) -> Result<()> {
    let options = FormatOptions::default();
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            for (i, field) in schema.fields().iter().enumerate() {
                println!("{}:", field.name());
                println!("{}", formatters[i].value(row));
            }
        }
    }
    Ok(())
}

/// Run EXPLAIN (or EXPLAIN ANALYZE) for each query against one registered source.
///
/// `plan` is the time spent in `SessionContext::sql` (parsing + logical
/// planning); `collect` is the time to produce the EXPLAIN output, which covers
/// physical planning including `TableProvider::scan` (for Hudi: timeline load
/// and file listing), and with --analyze also full query execution. The
/// hudi-vs-parquet difference in plain EXPLAIN's `collect` is therefore the
/// per-query planning overhead.
async fn explain_source(
    ctx: &SessionContext,
    label: &str,
    query_nums: &[usize],
    scale_factor: f64,
    analyze: bool,
) -> Result<()> {
    let mode = explain_keyword(analyze);
    for query_num in query_nums {
        let sql = load_query(*query_num, scale_factor)
            .map_err(datafusion::error::DataFusionError::Plan)?;
        println!();
        println!("=== Q{query_num:02} [{label}] {mode} ===");
        // One failing query shouldn't kill the remaining queries or the other
        // source's pass; report it under the query header and move on.
        if let Err(e) = explain_query(ctx, &sql, analyze).await {
            println!("ERROR: {e}");
        }
    }
    Ok(())
}

async fn explain_query(ctx: &SessionContext, sql: &str, analyze: bool) -> Result<()> {
    for stmt in &split_statements(sql) {
        match wrap_in_explain(stmt, analyze) {
            Some(explain_sql) => {
                let start = Instant::now();
                let df = ctx.sql(&explain_sql).await?;
                let plan_ms = start.elapsed().as_secs_f64() * 1000.0;
                let start = Instant::now();
                let batches = df.collect().await?;
                let collect_ms = start.elapsed().as_secs_f64() * 1000.0;
                println!("plan: {plan_ms:.1} ms, collect: {collect_ms:.1} ms");
                print_plan_batches(&batches)?;
            }
            None => {
                let head = stmt.lines().next().unwrap_or_default();
                println!("running without EXPLAIN (not a query statement): {head}");
                ctx.sql(stmt).await?.collect().await?;
            }
        }
    }
    Ok(())
}

/// Run EXPLAIN / EXPLAIN ANALYZE against Hudi and/or Parquet registrations.
async fn run_explain(
    hudi_dir: Option<&str>,
    parquet_dir: Option<&str>,
    scale_factor: f64,
    queries: Option<String>,
    analyze: bool,
    df_conf: &config::DataFusionConfig,
) -> Result<()> {
    if hudi_dir.is_none() && parquet_dir.is_none() {
        return Err(datafusion::error::DataFusionError::Plan(
            "At least one of --hudi-dir or --parquet-dir must be provided".to_string(),
        ));
    }

    let query_nums = parse_query_numbers(queries);

    if let Some(hudi_dir) = hudi_dir {
        let ctx =
            create_session_context(df_conf).map_err(datafusion::error::DataFusionError::Plan)?;
        let start = Instant::now();
        register_hudi_tables(&ctx, hudi_dir).await?;
        println!(
            "Registered Hudi tables from {hudi_dir} in {:.1} ms",
            start.elapsed().as_secs_f64() * 1000.0
        );
        explain_source(&ctx, "hudi", &query_nums, scale_factor, analyze).await?;
    }

    if let Some(parquet_dir) = parquet_dir {
        let ctx =
            create_session_context(df_conf).map_err(datafusion::error::DataFusionError::Plan)?;
        let start = Instant::now();
        register_parquet_tables(&ctx, parquet_dir).await?;
        println!(
            "Registered Parquet tables from {parquet_dir} in {:.1} ms",
            start.elapsed().as_secs_f64() * 1000.0
        );
        explain_source(&ctx, "parquet", &query_nums, scale_factor, analyze).await?;
    }

    Ok(())
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

        let statements = split_statements(&sql);

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
    let median = if sorted.len().is_multiple_of(2) {
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

#[derive(Serialize, Deserialize)]
struct PersistedQueryStats {
    avg_ms: f64,
    min_ms: f64,
    median_ms: f64,
    max_ms: f64,
}

#[derive(Serialize, Deserialize)]
struct PersistedResults {
    engine: String,
    #[serde(default)]
    display_name: Option<String>,
    format: String,
    scale_factor: f64,
    timestamp: u64,
    queries: BTreeMap<String, PersistedQueryStats>,
}

impl PersistedResults {
    fn label(&self) -> &str {
        self.display_name.as_deref().unwrap_or(&self.engine)
    }
}

fn format_sf_label(sf: f64) -> String {
    if sf == sf.floor() && sf >= 1.0 {
        format!("sf{}", sf as u64)
    } else {
        format!("sf{sf}")
    }
}

fn save_results(
    results: &[QueryResult],
    engine: &str,
    display_name: Option<&str>,
    format_name: &str,
    scale_factor: f64,
    output_dir: &str,
) -> std::result::Result<(), String> {
    let mut queries = BTreeMap::new();
    for r in results {
        if r.error.is_some() {
            continue;
        }
        if let Some(stats) = compute_stats(&r.timings_ms) {
            queries.insert(
                r.query_num.to_string(),
                PersistedQueryStats {
                    avg_ms: stats.mean,
                    min_ms: stats.min,
                    median_ms: stats.median,
                    max_ms: stats.max,
                },
            );
        }
    }

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let persisted = PersistedResults {
        engine: engine.to_string(),
        display_name: display_name.map(|s| s.to_string()),
        format: format_name.to_string(),
        scale_factor,
        timestamp,
        queries,
    };

    fs::create_dir_all(output_dir)
        .map_err(|e| format!("Failed to create output dir {output_dir}: {e}"))?;

    let sf_label = format_sf_label(scale_factor);
    let filename = format!("{engine}_{format_name}_{sf_label}.json");
    let path = Path::new(output_dir).join(&filename);

    let json = serde_json::to_string_pretty(&persisted)
        .map_err(|e| format!("Failed to serialize results: {e}"))?;
    fs::write(&path, json).map_err(|e| format!("Failed to write {}: {e}", path.display()))?;

    println!("Results saved to {}", path.display());
    Ok(())
}

fn load_results(path: &str) -> std::result::Result<PersistedResults, String> {
    let content = fs::read_to_string(path).map_err(|e| format!("Failed to read {path}: {e}"))?;
    serde_json::from_str(&content).map_err(|e| format!("Failed to parse {path}: {e}"))
}

/// Run the benchmark against Hudi, Parquet, or both.
#[allow(clippy::too_many_arguments)]
async fn run_bench(
    hudi_dir: Option<&str>,
    parquet_dir: Option<&str>,
    scale_factor: f64,
    queries: Option<String>,
    warmup: usize,
    iterations: usize,
    df_conf: &config::DataFusionConfig,
    output_dir: Option<&str>,
    engine_label: Option<&str>,
    format_label: Option<&str>,
    display_name: Option<&str>,
) -> Result<()> {
    if hudi_dir.is_none() && parquet_dir.is_none() {
        return Err(datafusion::error::DataFusionError::Plan(
            "At least one of --hudi-dir or --parquet-dir must be provided".to_string(),
        ));
    }

    let query_nums = parse_query_numbers(queries);

    if let Some(limit) = &df_conf.memory_limit {
        println!("DataFusion memory limit: {limit}");
    }
    println!("Warmup: {warmup} iteration(s), Measured: {iterations} iteration(s)");

    if let Some(hudi_dir) = hudi_dir {
        let ctx =
            create_session_context(df_conf).map_err(datafusion::error::DataFusionError::Plan)?;
        println!("Registering Hudi tables from {hudi_dir}");
        register_hudi_tables(&ctx, hudi_dir).await?;
        println!("Benchmarking Hudi...");
        let results = bench_source(&ctx, &query_nums, warmup, iterations, scale_factor).await;
        print_single_table("Hudi", &results);
        if let Some(dir) = output_dir {
            let engine = engine_label.unwrap_or("datafusion");
            let fmt = format_label.unwrap_or("hudi");
            save_results(&results, engine, display_name, fmt, scale_factor, dir)
                .map_err(datafusion::error::DataFusionError::Plan)?;
        }
    }

    if let Some(parquet_dir) = parquet_dir {
        let ctx =
            create_session_context(df_conf).map_err(datafusion::error::DataFusionError::Plan)?;
        println!("Registering Parquet tables from {parquet_dir}");
        register_parquet_tables(&ctx, parquet_dir).await?;
        println!("Benchmarking Parquet...");
        let results = bench_source(&ctx, &query_nums, warmup, iterations, scale_factor).await;
        print_single_table("Parquet", &results);
        if let Some(dir) = output_dir {
            let engine = engine_label.unwrap_or("datafusion");
            let fmt = format_label.unwrap_or("parquet");
            save_results(&results, engine, display_name, fmt, scale_factor, dir)
                .map_err(datafusion::error::DataFusionError::Plan)?;
        }
    }

    Ok(())
}

/// Run validation: query both Hudi and Parquet once, compare results.
async fn run_validate(
    hudi_dir: &str,
    parquet_dir: &str,
    scale_factor: f64,
    queries: Option<String>,
    df_conf: &config::DataFusionConfig,
) -> Result<()> {
    let query_nums = parse_query_numbers(queries);

    if let Some(limit) = &df_conf.memory_limit {
        println!("DataFusion memory limit: {limit}");
    }

    println!("Registering Hudi tables from {hudi_dir}");
    let hudi_ctx =
        create_session_context(df_conf).map_err(datafusion::error::DataFusionError::Plan)?;
    register_hudi_tables(&hudi_ctx, hudi_dir).await?;

    println!("Registering Parquet tables from {parquet_dir}");
    let parquet_ctx =
        create_session_context(df_conf).map_err(datafusion::error::DataFusionError::Plan)?;
    register_parquet_tables(&parquet_ctx, parquet_dir).await?;

    println!("Running Hudi queries...");
    let hudi_results = bench_source(&hudi_ctx, &query_nums, 0, 1, scale_factor).await;

    println!("Running Parquet queries...");
    let parquet_results = bench_source(&parquet_ctx, &query_nums, 0, 1, scale_factor).await;

    let failed = print_validation_table(&query_nums, &hudi_results, &parquet_results);

    if failed.is_empty() {
        Ok(())
    } else {
        let names: Vec<String> = failed.iter().map(|qn| format!("Q{qn:02}")).collect();
        Err(datafusion::error::DataFusionError::Plan(format!(
            "Hudi results differ from parquet for {}",
            names.join(", ")
        )))
    }
}

/// Parse Spark benchmark JSON output into a timing table.
fn run_parse_spark_output(
    input: Option<&str>,
    output_dir: Option<&str>,
    engine_label: Option<&str>,
    format_label: Option<&str>,
    display_name: Option<&str>,
    scale_factor: f64,
) -> Result<()> {
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
        if let Some(dir) = output_dir {
            let engine = engine_label.unwrap_or("spark");
            let fmt = format_label.unwrap_or("hudi");
            save_results(&results, engine, display_name, fmt, scale_factor, dir)
                .map_err(datafusion::error::DataFusionError::Plan)?;
        }
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
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line)
            && let (Some(q), Some(ms)) = (v["query"].as_u64(), v["elapsed_ms"].as_f64())
        {
            all_timings.entry(q as usize).or_default().push(ms);
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

/// Compare persisted benchmark results and render terminal bar charts.
fn run_compare(results_dir: &str, runs: &str) -> Result<()> {
    let stems: Vec<&str> = runs.split(',').map(|s| s.trim()).collect();
    if stems.is_empty() {
        return Err(datafusion::error::DataFusionError::Plan(
            "No runs specified".to_string(),
        ));
    }

    let mut loaded: Vec<PersistedResults> = Vec::new();
    for stem in &stems {
        let path = format!("{results_dir}/{stem}.json");
        let r = load_results(&path).map_err(datafusion::error::DataFusionError::Plan)?;
        loaded.push(r);
    }

    // Collect all query numbers across all runs
    let mut all_queries = BTreeSet::new();
    for r in &loaded {
        for q in r.queries.keys() {
            if let Ok(n) = q.parse::<usize>() {
                all_queries.insert(n);
            }
        }
    }

    if all_queries.is_empty() {
        println!("No query data found in the provided result files.");
        return Ok(());
    }

    // Find global max avg_ms for bar scaling
    let global_max = loaded
        .iter()
        .flat_map(|r| r.queries.values().map(|s| s.avg_ms))
        .fold(0.0_f64, f64::max);

    if global_max == 0.0 {
        println!("All query timings are zero.");
        return Ok(());
    }

    let bar_width: usize = 40;
    let engine_names: Vec<&str> = loaded.iter().map(|r| r.label()).collect();
    let max_name_len = engine_names.iter().map(|n| n.len()).max().unwrap_or(0);

    println!();
    println!("TPC-H Query Runtime Comparison");
    println!("{}", "=".repeat(max_name_len + 6 + bar_width + 16));
    println!();

    for q in &all_queries {
        let q_str = q.to_string();
        for (i, r) in loaded.iter().enumerate() {
            let label = if i == 0 {
                format!("Q{q:02}  {:<width$}", r.label(), width = max_name_len)
            } else {
                format!("     {:<width$}", r.label(), width = max_name_len)
            };

            if let Some(stats) = r.queries.get(&q_str) {
                let filled = ((stats.avg_ms / global_max) * bar_width as f64).round() as usize;
                let filled = filled.min(bar_width);
                let empty = bar_width - filled;
                println!(
                    "{label} |{}{} | {:>9.1} ms",
                    "\u{2588}".repeat(filled),
                    " ".repeat(empty),
                    stats.avg_ms,
                );
            } else {
                println!("{label} |{} |       N/A", " ".repeat(bar_width),);
            }
        }
        println!();
    }

    // Summary: Total and Geometric Mean as bar charts
    let mut totals: Vec<(String, f64)> = Vec::new();
    let mut geomeans: Vec<(String, f64)> = Vec::new();
    for r in &loaded {
        let total: f64 = r.queries.values().map(|s| s.avg_ms).sum();
        totals.push((r.label().to_string(), total));

        let values: Vec<f64> = r.queries.values().map(|s| s.avg_ms).collect();
        if !values.is_empty() && values.iter().all(|v| *v > 0.0) {
            let log_sum: f64 = values.iter().map(|v| v.ln()).sum::<f64>();
            let geomean = (log_sum / values.len() as f64).exp();
            geomeans.push((r.label().to_string(), geomean));
        }
    }

    println!("Summary");
    println!("{}", "-".repeat(max_name_len + 6 + bar_width + 16));
    println!();

    // Total runtime bars
    let total_max = totals.iter().map(|(_, v)| *v).fold(0.0_f64, f64::max);
    if total_max > 0.0 {
        for (i, (engine, total)) in totals.iter().enumerate() {
            let label = if i == 0 {
                format!("Tot  {engine:<max_name_len$}")
            } else {
                format!("     {engine:<max_name_len$}")
            };
            let filled = ((total / total_max) * bar_width as f64).round() as usize;
            let filled = filled.min(bar_width);
            let empty = bar_width - filled;
            println!(
                "{label} |{}{} | {:>9.1} ms",
                "\u{2588}".repeat(filled),
                " ".repeat(empty),
                total,
            );
        }
        println!();
    }

    // Geometric mean bars
    if !geomeans.is_empty() {
        let geomean_max = geomeans.iter().map(|(_, v)| *v).fold(0.0_f64, f64::max);
        if geomean_max > 0.0 {
            for (i, (engine, geomean)) in geomeans.iter().enumerate() {
                let label = if i == 0 {
                    format!("Geo  {engine:<max_name_len$}")
                } else {
                    format!("     {engine:<max_name_len$}")
                };
                let filled = ((geomean / geomean_max) * bar_width as f64).round() as usize;
                let filled = filled.min(bar_width);
                let empty = bar_width - filled;
                println!(
                    "{label} |{}{} | {:>9.1} ms",
                    "\u{2588}".repeat(filled),
                    " ".repeat(empty),
                    geomean,
                );
            }
            println!();
        }
    }

    Ok(())
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

/// Returns the queries that did not match, so the caller can fail the run
/// rather than leave a mismatch to be noticed in the output.
fn print_validation_table(
    query_nums: &[usize],
    hudi_results: &[QueryResult],
    parquet_results: &[QueryResult],
) -> Vec<usize> {
    let mut failed = Vec::new();
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
            failed.push(*qn);
            table.add_row(vec![
                Cell::new(format!("Q{qn:02}")),
                Cell::new(if h_err.is_some() { "-" } else { "OK" }),
                Cell::new(if p_err.is_some() { "-" } else { "OK" }),
                Cell::new(format!("ERROR: {err_msg}")),
            ]);
            continue;
        }

        let h_ms = hr
            .timings_ms
            .first()
            .map(|t| format!("{t:.1}"))
            .unwrap_or("-".into());
        let p_ms = pr
            .timings_ms
            .first()
            .map(|t| format!("{t:.1}"))
            .unwrap_or("-".into());
        let validation = compare_batches(&hr.last_batches, &pr.last_batches);
        if validation != "PASS" {
            failed.push(*qn);
        }

        table.add_row(vec![
            Cell::new(format!("Q{qn:02}")),
            Cell::new(h_ms),
            Cell::new(p_ms),
            Cell::new(validation),
        ]);
    }

    println!("{table}");
    failed
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_statements_strips_comments_and_splits_on_semicolons() {
        let sql = "-- Licensed to the ASF;\n-- under the License.\nselect 1;\n\ncreate view v as\nselect 2;\ndrop view v;\n";
        let statements = split_statements(sql);
        assert_eq!(
            statements,
            vec!["select 1", "create view v as\nselect 2", "drop view v"]
        );
    }

    #[test]
    fn test_split_statements_single_query_without_trailing_semicolon() {
        assert_eq!(split_statements("select 1"), vec!["select 1"]);
    }

    #[test]
    fn test_wrap_in_explain_wraps_queries_only() {
        assert_eq!(
            wrap_in_explain("select 1", false).as_deref(),
            Some("EXPLAIN select 1")
        );
        assert_eq!(
            wrap_in_explain("with t as (select 1) select * from t", true).as_deref(),
            Some("EXPLAIN ANALYZE with t as (select 1) select * from t")
        );
        assert_eq!(
            wrap_in_explain("values (1)", false).as_deref(),
            Some("EXPLAIN values (1)")
        );
        assert_eq!(wrap_in_explain("create view v as select 1", true), None);
        assert_eq!(wrap_in_explain("drop view v", false), None);
        assert_eq!(wrap_in_explain("(select 1) union (select 2)", false), None);
    }
}
