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

use std::collections::BTreeMap;
use std::fmt::Write;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Canonical table ordering for SQL output (matches TPC-H dependency order).
const TABLE_ORDER: &[&str] = &[
    "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
];

/// Common table definition shared across all scale factors (from tables.yaml).
#[derive(Deserialize)]
struct CommonTableConfig {
    primary_key: String,
    pre_combine_field: String,
    record_size_estimate: u32,
}

/// Common tables file (tables.yaml).
#[derive(Deserialize)]
struct CommonConfig {
    tables: BTreeMap<String, CommonTableConfig>,
}

/// Per-scale-factor overrides (sf*.yaml).
#[derive(Deserialize)]
struct ScaleFactorOverrides {
    shuffle_parallelism: BTreeMap<String, u32>,
    create_tables: SparkCommandConfig,
    bench: BenchConfig,
}

/// Merged table config used at runtime.
pub struct TableConfig {
    pub primary_key: String,
    pub pre_combine_field: String,
    pub record_size_estimate: u32,
    pub shuffle_parallelism: u32,
}

pub struct ScaleFactorConfig {
    pub tables: BTreeMap<String, TableConfig>,
    pub create_tables: SparkCommandConfig,
    pub bench: BenchConfig,
}

#[derive(Deserialize)]
pub struct SparkCommandConfig {
    #[serde(default)]
    pub spark_conf: BTreeMap<String, String>,
}

#[derive(Deserialize)]
pub struct BenchConfig {
    #[serde(default)]
    pub warmup: usize,
    #[serde(default = "default_iterations")]
    pub iterations: usize,
    #[serde(default)]
    pub spark_conf: BTreeMap<String, String>,
    #[serde(default)]
    pub datafusion_conf: DataFusionConfig,
}

#[derive(Deserialize, Default)]
pub struct DataFusionConfig {
    /// Memory pool limit (e.g., "16g", "512m"); unlimited if not set.
    /// Handled specially because it requires creating a memory pool at runtime.
    pub memory_limit: Option<String>,
    /// Additional DataFusion session config key-value pairs.
    /// Keys use DataFusion's dotted config namespace (e.g., "datafusion.execution.target_partitions").
    /// Values are passed directly to `SessionConfig::set()`.
    #[serde(default, flatten)]
    pub settings: BTreeMap<String, String>,
}

fn default_iterations() -> usize {
    1
}

impl ScaleFactorConfig {
    /// Supported scale factors that have config files.
    const SUPPORTED: &[u64] = &[1, 10, 100];

    /// Load common table definitions and per-SF overrides, then merge them.
    pub fn load(scale_factor: f64) -> Result<Self, Box<dyn std::error::Error>> {
        let effective_sf = if scale_factor < 1.0 {
            1u64
        } else {
            let sf = scale_factor as u64;
            if !Self::SUPPORTED.contains(&sf) {
                return Err(format!(
                    "Unsupported scale factor {scale_factor}. Supported: {:?}",
                    Self::SUPPORTED
                )
                .into());
            }
            sf
        };

        let config_dir = std::env::var("TPCH_CONFIG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| Path::new(env!("CARGO_MANIFEST_DIR")).join("config"));

        // Load common table definitions
        let common_path = config_dir.join("tables.yaml");
        let common_content = std::fs::read_to_string(&common_path)
            .map_err(|e| format!("Failed to read {}: {e}", common_path.display()))?;
        let common: CommonConfig = serde_yaml::from_str(&common_content)
            .map_err(|e| format!("Failed to parse tables.yaml: {e}"))?;

        // Load per-SF overrides
        let sf_filename = format!("sf{effective_sf}.yaml");
        let sf_path = config_dir.join(&sf_filename);
        let sf_content = std::fs::read_to_string(&sf_path)
            .map_err(|e| format!("Failed to read config {}: {e}", sf_path.display()))?;
        let overrides: ScaleFactorOverrides = serde_yaml::from_str(&sf_content)
            .map_err(|e| format!("Failed to parse config {sf_filename}: {e}"))?;

        // Merge: common tables + per-SF shuffle_parallelism
        let mut tables = BTreeMap::new();
        for (name, common_table) in common.tables {
            let shuffle_parallelism = overrides
                .shuffle_parallelism
                .get(&name)
                .copied()
                .unwrap_or(1);
            tables.insert(
                name,
                TableConfig {
                    primary_key: common_table.primary_key,
                    pre_combine_field: common_table.pre_combine_field,
                    record_size_estimate: common_table.record_size_estimate,
                    shuffle_parallelism,
                },
            );
        }

        Ok(Self {
            tables,
            create_tables: overrides.create_tables,
            bench: overrides.bench,
        })
    }

    /// Generate CTAS SQL for creating Hudi tables from parquet sources.
    pub fn render_ctas_sql(&self, parquet_base: &str, hudi_base: &str) -> String {
        let mut sql = String::new();
        for &name in TABLE_ORDER {
            let Some(table) = self.tables.get(name) else {
                continue;
            };
            writeln!(sql, "CREATE TABLE {name} USING hudi").unwrap();
            writeln!(sql, "LOCATION '{hudi_base}/{name}'").unwrap();
            writeln!(sql, "TBLPROPERTIES (").unwrap();
            writeln!(sql, "  type = 'cow',").unwrap();
            writeln!(sql, "  primaryKey = '{}',", table.primary_key).unwrap();
            writeln!(sql, "  preCombineField = '{}',", table.pre_combine_field).unwrap();
            writeln!(sql, "  'hoodie.table.name' = '{name}',").unwrap();
            writeln!(
                sql,
                "  'hoodie.bulkinsert.shuffle.parallelism' = '{}',",
                table.shuffle_parallelism
            )
            .unwrap();
            writeln!(
                sql,
                "  'hoodie.copyonwrite.record.size.estimate' = '{}'",
                table.record_size_estimate
            )
            .unwrap();
            writeln!(sql, ") AS SELECT * FROM parquet.`{parquet_base}/{name}/`;").unwrap();
            writeln!(sql).unwrap();
        }
        sql
    }

    /// Generate benchmark SQL: table registrations followed by query iterations.
    pub fn render_bench_sql(
        &self,
        hudi_base: &str,
        query_nums: &[usize],
        iterations: usize,
        scale_factor: f64,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let mut sql = String::new();

        // Register Hudi tables
        for &name in TABLE_ORDER {
            if self.tables.contains_key(name) {
                writeln!(
                    sql,
                    "CREATE TABLE {name} USING hudi LOCATION '{hudi_base}/{name}';"
                )
                .unwrap();
            }
        }
        writeln!(sql).unwrap();

        // Per-SF substitution values (TPC-H spec Section 2.4.11.3: FRACTION = 0.0001 / SF)
        let q11_fraction = format!("{:.10}", 0.0001 / scale_factor);

        // Add queries with bench markers
        let queries_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("queries");
        for &qn in query_nums {
            let qfile = queries_dir.join(format!("q{qn}.sql"));
            let query_sql = std::fs::read_to_string(&qfile)
                .map_err(|e| format!("Failed to read q{qn}.sql: {e}"))?;
            let query_sql = query_sql.replace("${Q11_FRACTION}", &q11_fraction);
            for i in 1..=iterations {
                writeln!(sql).unwrap();
                writeln!(sql, "SELECT 'BENCH_MARKER q{qn} iter{i}' as marker;").unwrap();
                write!(sql, "{query_sql}").unwrap();
                if !query_sql.ends_with('\n') {
                    writeln!(sql).unwrap();
                }
            }
        }

        Ok(sql)
    }

    /// Generate spark-submit arguments for a given command, one per line.
    pub fn render_spark_args(&self, command: &str) -> Result<Vec<String>, String> {
        let spark_conf = match command {
            "create-tables" => &self.create_tables.spark_conf,
            "bench" => &self.bench.spark_conf,
            _ => return Err(format!("Unknown command: {command}")),
        };

        let mut args = vec!["--master".to_string(), "local[*]".to_string()];

        for (key, value) in spark_conf {
            args.push("--conf".to_string());
            args.push(format!("{key}={value}"));
        }

        Ok(args)
    }
}
