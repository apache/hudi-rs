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

use clap::ValueEnum;
use serde::Deserialize;

/// Canonical table ordering for SQL output (matches TPC-H dependency order).
const TABLE_ORDER: &[&str] = &[
    "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
];

/// Hudi table type the benchmark tables are created as.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum TableType {
    /// Copy-on-write: one commit, base files only.
    Cow,
    /// Merge-on-read: the bulk insert plus one update commit that leaves a log
    /// file in every file group.
    Mor,
}

impl TableType {
    /// The value Spark SQL's `type` table property takes.
    pub fn as_str(&self) -> &'static str {
        match self {
            TableType::Cow => "cow",
            TableType::Mor => "mor",
        }
    }
}

/// Common table definition shared across all scale factors (from tables.yaml).
#[derive(Deserialize)]
struct CommonTableConfig {
    primary_key: String,
    /// Absent when the table has no real ordering column; see tables.yaml.
    pre_combine_field: Option<String>,
    record_size_estimate: u32,
    update_field: String,
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
    pub pre_combine_field: Option<String>,
    pub record_size_estimate: u32,
    pub update_field: String,
    pub shuffle_parallelism: u32,
}

impl TableConfig {
    /// The primary key columns, in declaration order.
    fn key_columns(&self) -> Vec<&str> {
        self.primary_key.split(',').map(str::trim).collect()
    }
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
                    update_field: common_table.update_field,
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
    pub fn render_ctas_sql(
        &self,
        parquet_base: &str,
        hudi_base: &str,
        table_type: TableType,
    ) -> String {
        let mut sql = String::new();
        for &name in TABLE_ORDER {
            let Some(table) = self.tables.get(name) else {
                continue;
            };
            // The catalog registration outlives the data it points at, so
            // without this a rerun fails on the previous run's entry.
            writeln!(sql, "DROP TABLE IF EXISTS {name};").unwrap();
            writeln!(sql, "CREATE TABLE {name} USING hudi").unwrap();
            writeln!(sql, "LOCATION '{hudi_base}/{name}'").unwrap();
            writeln!(sql, "TBLPROPERTIES (").unwrap();
            writeln!(sql, "  type = '{}',", table_type.as_str()).unwrap();
            writeln!(sql, "  primaryKey = '{}',", table.primary_key).unwrap();
            match &table.pre_combine_field {
                Some(field) => writeln!(sql, "  preCombineField = '{field}',").unwrap(),
                // Hudi infers this when no ordering field is given; stated so
                // the rendered SQL says how the update commit is merged.
                None => writeln!(
                    sql,
                    "  'hoodie.record.merge.mode' = 'COMMIT_TIME_ORDERING',"
                )
                .unwrap(),
            }
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

    /// Generate the update commit that gives every file group a log file.
    ///
    /// One `UPDATE` per table, run in the session that created the tables. Two
    /// clauses select the rows: a hash of the key picks one row in every
    /// `round(1 / update_fraction)` uniformly (exactly the fraction only when
    /// that reciprocal is whole), and the smallest key in each file group is
    /// added so a table too small for the fraction to land a row still gets
    /// its log file.
    /// The assigned value is the column's own, so the merged rows, and every
    /// query over them, are identical to the parquet the table was built
    /// from — which is what lets `validate` run unchanged against a
    /// merge-on-read table.
    pub fn render_update_sql(&self, update_fraction: f64) -> Result<String, String> {
        if !(update_fraction > 0.0 && update_fraction <= 1.0) {
            return Err(format!(
                "update fraction must be in (0, 1], got {update_fraction}"
            ));
        }
        let modulus = (1.0 / update_fraction).round().max(1.0) as u64;

        let mut sql = String::new();
        for &name in TABLE_ORDER {
            let Some(table) = self.tables.get(name) else {
                continue;
            };
            let keys = table.key_columns();
            let key_list = keys.join(", ");
            let first_key = keys
                .iter()
                .map(|k| format!("first_key.{k}"))
                .collect::<Vec<_>>()
                .join(", ");
            let field = &table.update_field;
            writeln!(sql, "UPDATE {name} SET {field} = {field}").unwrap();
            writeln!(sql, "WHERE pmod(hash({key_list}), {modulus}) = 0").unwrap();
            writeln!(sql, "   OR ({key_list}) IN (").unwrap();
            writeln!(sql, "     SELECT {first_key} FROM (").unwrap();
            writeln!(
                sql,
                "       SELECT min(struct({key_list})) AS first_key FROM {name} GROUP BY _hoodie_file_name"
            )
            .unwrap();
            writeln!(sql, "     )").unwrap();
            writeln!(sql, "   );").unwrap();
            writeln!(sql).unwrap();
        }
        Ok(sql)
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
                writeln!(sql, "DROP TABLE IF EXISTS {name};").unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sf1() -> ScaleFactorConfig {
        ScaleFactorConfig::load(1.0).expect("sf1 config loads")
    }

    fn statements_for(sql: &str, table: &str) -> Vec<String> {
        sql.split(";\n")
            .filter(|stmt| {
                stmt.contains(&format!(" {table} ")) || stmt.contains(&format!(" {table}\n"))
            })
            .map(|stmt| stmt.to_string())
            .collect()
    }

    #[test]
    fn test_render_ctas_sql_mor_sets_table_type_per_table() {
        let sql = sf1().render_ctas_sql("/pq", "/hudi", TableType::Mor);
        assert_eq!(sql.matches("type = 'mor',").count(), 8);
        assert!(!sql.contains("type = 'cow'"));
        let cow = sf1().render_ctas_sql("/pq", "/hudi", TableType::Cow);
        assert_eq!(cow.matches("type = 'cow',").count(), 8);
    }

    #[test]
    fn test_render_ctas_sql_ordering_follows_pre_combine_field() {
        let sql = sf1().render_ctas_sql("/pq", "/hudi", TableType::Mor);
        let lineitem = statements_for(&sql, "lineitem").join(";");
        assert!(lineitem.contains("preCombineField = 'l_shipdate'"));
        assert!(!lineitem.contains("COMMIT_TIME_ORDERING"));
        let customer = statements_for(&sql, "customer").join(";");
        assert!(!customer.contains("preCombineField"));
        assert!(customer.contains("'hoodie.record.merge.mode' = 'COMMIT_TIME_ORDERING'"));
    }

    #[test]
    fn test_render_update_sql_assigns_update_field_to_itself() {
        let sql = sf1().render_update_sql(0.001).expect("renders");
        assert_eq!(sql.matches("UPDATE ").count(), 8);
        assert!(sql.contains("UPDATE lineitem SET l_comment = l_comment"));
        assert!(sql.contains("UPDATE customer SET c_comment = c_comment"));
    }

    #[test]
    fn test_render_update_sql_fraction_becomes_hash_modulus() {
        let sql = sf1().render_update_sql(0.001).expect("renders");
        assert!(sql.contains("pmod(hash(o_orderkey), 1000) = 0"));
        let sql = sf1().render_update_sql(0.25).expect("renders");
        assert!(sql.contains("pmod(hash(o_orderkey), 4) = 0"));
    }

    #[test]
    fn test_render_update_sql_composite_key_selects_first_key_per_file_group() {
        let sql = sf1().render_update_sql(0.001).expect("renders");
        assert!(sql.contains("OR (l_orderkey, l_linenumber) IN ("));
        assert!(sql.contains("SELECT first_key.l_orderkey, first_key.l_linenumber FROM ("));
        assert!(sql.contains(
            "SELECT min(struct(l_orderkey, l_linenumber)) AS first_key FROM lineitem GROUP BY _hoodie_file_name"
        ));
    }

    #[test]
    fn test_render_update_sql_rejects_fraction_outside_unit_interval() {
        for bad in [0.0, -0.1, 1.5, f64::NAN] {
            let err = sf1().render_update_sql(bad).expect_err("rejects");
            assert!(err.contains("update fraction"), "{err}");
        }
    }
}
