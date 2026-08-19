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
//! Spark-in-the-loop parity harness.
//!
//! Gated behind `HUDI_SPARK_PARITY=1` (needs a local Spark + Hudi bundle; see
//! `scripts/parity/README.md`). Run with `make parity`.
//!
//! Scenarios: hudi-rs writes → Spark reads; Spark writes → rs reads;
//! Spark table services on rs-written tables; interleaved writers.
//!
//! MDT HFile interop is exercised here too: whenever Spark reads or compacts
//! an rs-written table with metadata enabled, its native HFile reader parses
//! the rs-written MDT base files and log blocks (it ignores the comparator
//! class name and block checksums, which are the two places our minimal
//! writer diverges from HBase output).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::table::TableTypeValue;
use hudi_core::table::{QueryType, ReadOptions, Table};
use tempfile::tempdir;

/// Harness row: (id, part, ts, value); `part` empty for unpartitioned tables.
type Row = (String, Option<String>, i64, i64);

fn enabled() -> bool {
    std::env::var("HUDI_SPARK_PARITY")
        .map(|v| v == "1")
        .unwrap_or(false)
}

struct SparkEnv {
    spark_submit: PathBuf,
    bundle_jar: PathBuf,
    scripts_dir: PathBuf,
}

fn spark_env() -> SparkEnv {
    let spark_home = std::env::var("SPARK_HOME").expect("SPARK_HOME must be set for parity runs");
    let bundle_jar = std::env::var("HUDI_SPARK_BUNDLE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").expect("HOME");
            PathBuf::from(home).join(
                ".m2/repository/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.1.1/hudi-spark3.5-bundle_2.12-1.1.1.jar",
            )
        });
    assert!(
        bundle_jar.is_file(),
        "Hudi Spark bundle not found at {bundle_jar:?}; set HUDI_SPARK_BUNDLE"
    );
    let scripts_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/parity");
    SparkEnv {
        spark_submit: PathBuf::from(spark_home).join("bin/spark-submit"),
        bundle_jar,
        scripts_dir,
    }
}

fn run_spark(env: &SparkEnv, script: &str, args: &[&str]) {
    let mut command = Command::new(&env.spark_submit);
    command
        .arg("--master")
        .arg("local[2]")
        .arg("--jars")
        .arg(&env.bundle_jar)
        .arg("--conf")
        .arg("spark.serializer=org.apache.spark.serializer.KryoSerializer")
        .arg("--conf")
        .arg("spark.ui.enabled=false")
        // Pin the driver to loopback so parity runs survive offline /
        // hostname-changing environments (laptop off Wi-Fi, VPN flips).
        .arg("--conf")
        .arg("spark.driver.bindAddress=127.0.0.1")
        .arg("--conf")
        .arg("spark.driver.host=127.0.0.1")
        .arg("--driver-memory")
        .arg("2g")
        .arg(env.scripts_dir.join(script))
        .args(args);
    if let Ok(java_home) = std::env::var("HUDI_PARITY_JAVA_HOME") {
        command.env("JAVA_HOME", java_home);
    }
    let output = command.output().expect("failed to launch spark-submit");
    assert!(
        output.status.success(),
        "spark-submit {script} failed (status {:?})\n--- stdout ---\n{}\n--- stderr (tail) ---\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        {
            let err = String::from_utf8_lossy(&output.stderr).to_string();
            let lines: Vec<&str> = err.lines().collect();
            lines[lines.len().saturating_sub(60)..].join("\n")
        }
    );
}

fn rows_json(rows: &[Row]) -> String {
    let items: Vec<String> = rows
        .iter()
        .map(|(id, part, ts, value)| {
            let part = part
                .as_ref()
                .map(|p| format!("\"{p}\""))
                .unwrap_or_else(|| "null".to_string());
            format!("{{\"id\":\"{id}\",\"part\":{part},\"ts\":{ts},\"value\":{value}}}")
        })
        .collect();
    format!("[{}]", items.join(","))
}

fn spark_write(
    env: &SparkEnv,
    path: &Path,
    op: &str,
    table_type: &str,
    partitioned: bool,
    rows: &[Row],
) {
    spark_write_with(env, path, op, table_type, partitioned, rows, &[]);
}

fn spark_write_with(
    env: &SparkEnv,
    path: &Path,
    op: &str,
    table_type: &str,
    partitioned: bool,
    rows: &[Row],
    extra_args: &[&str],
) {
    let mut args = vec![
        "--path".to_string(),
        path.to_string_lossy().to_string(),
        "--op".to_string(),
        op.to_string(),
        "--table-type".to_string(),
        table_type.to_string(),
        "--rows".to_string(),
        rows_json(rows),
    ];
    if partitioned {
        args.push("--partitioned".to_string());
    }
    args.extend(extra_args.iter().map(|s| s.to_string()));
    let args_ref: Vec<&str> = args.iter().map(String::as_str).collect();
    run_spark(env, "spark_write.py", &args_ref);
}

fn spark_read_mode(env: &SparkEnv, path: &Path, extra_args: &[&str]) -> Vec<Row> {
    let out = path.with_extension("spark_read.json");
    let path_str = path.to_string_lossy().to_string();
    let out_str = out.to_string_lossy().to_string();
    let mut args = vec!["--path", path_str.as_str(), "--out", out_str.as_str()];
    args.extend_from_slice(extra_args);
    run_spark(env, "spark_read.py", &args);
    let text = std::fs::read_to_string(&out).expect("spark_read output");
    let parsed: serde_json::Value = serde_json::from_str(&text).expect("spark_read json");
    let mut rows: Vec<Row> = parsed
        .as_array()
        .expect("array")
        .iter()
        .map(|r| {
            (
                r["id"].as_str().expect("id").to_string(),
                r["part"].as_str().map(str::to_string),
                r["ts"].as_i64().expect("ts"),
                r["value"].as_i64().expect("value"),
            )
        })
        .collect();
    rows.sort();
    rows
}

fn spark_read(env: &SparkEnv, path: &Path) -> Vec<Row> {
    spark_read_mode(env, path, &[])
}

fn spark_read_as_of(env: &SparkEnv, path: &Path, instant: &str) -> Vec<Row> {
    spark_read_mode(env, path, &["--mode", "timetravel", "--as-of", instant])
}

fn spark_read_incremental(env: &SparkEnv, path: &Path, begin: &str, end: Option<&str>) -> Vec<Row> {
    let mut args = vec!["--mode", "incremental", "--begin", begin];
    if let Some(end) = end {
        args.extend_from_slice(&["--end", end]);
    }
    spark_read_mode(env, path, &args)
}

/// Latest completed data-timeline instant's request timestamp.
fn latest_completed_instant(path: &Path) -> String {
    let timeline = path.join(".hoodie").join("timeline");
    let mut instants: Vec<String> = std::fs::read_dir(&timeline)
        .expect("timeline dir")
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            let (stem, action) = name.rsplit_once('.')?;
            if !matches!(action, "commit" | "deltacommit" | "replacecommit") {
                return None;
            }
            let (request, completion) = stem.split_once('_')?;
            if request.chars().all(|c| c.is_ascii_digit())
                && completion.chars().all(|c| c.is_ascii_digit())
            {
                Some(request.to_string())
            } else {
                None
            }
        })
        .collect();
    instants.sort();
    instants.pop().expect("at least one completed instant")
}

fn spark_service(env: &SparkEnv, path: &Path, service: &str) {
    run_spark(
        env,
        "spark_service.py",
        &["--path", &path.to_string_lossy(), "--service", service],
    );
}

fn harness_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("part", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn batch(rows: &[Row]) -> RecordBatch {
    RecordBatch::try_new(
        harness_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.1.as_deref())
                    .collect::<Vec<Option<&str>>>(),
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

async fn rs_create(path: &Path, table_type: TableTypeValue, partitioned: bool) -> Table {
    let mut builder = Table::create(path.to_str().unwrap())
        .with_table_name("parity")
        .with_table_type(table_type)
        .with_record_key_fields(["id"])
        .with_ordering_fields(["ts"]);
    if partitioned {
        builder = builder.with_partition_fields(["part"]);
    }
    builder.create().await.unwrap()
}

async fn rs_read(path: &Path) -> Vec<Row> {
    rs_read_with(path, ReadOptions::new()).await
}

async fn rs_read_as_of(path: &Path, instant: &str) -> Vec<Row> {
    rs_read_with(path, ReadOptions::new().with_as_of_timestamp(instant)).await
}

async fn rs_read_incremental(path: &Path, begin: &str, end: Option<&str>) -> Vec<Row> {
    let mut options = ReadOptions::new()
        .with_query_type(QueryType::Incremental)
        .with_start_timestamp(begin);
    if let Some(end) = end {
        options = options.with_end_timestamp(end);
    }
    rs_read_with(path, options).await
}

async fn rs_read_with(path: &Path, options: ReadOptions) -> Vec<Row> {
    let table = Table::new(path.to_str().unwrap()).await.unwrap();
    let batches = table.read(&options).await.unwrap();
    let mut rows: Vec<Row> = batches
        .iter()
        .flat_map(|b| {
            let ids = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let parts = b
                .column_by_name("part")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let ts = b
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let values = b
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..b.num_rows())
                .map(|i| {
                    (
                        ids.value(i).to_string(),
                        parts.and_then(|p| (!p.is_null(i)).then(|| p.value(i).to_string())),
                        ts.value(i),
                        values.value(i),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort();
    rows
}

fn row(id: &str, part: Option<&str>, ts: i64, value: i64) -> Row {
    (id.to_string(), part.map(str::to_string), ts, value)
}

// ---------------------------------------------------------------------------
// Scenario A: hudi-rs writes → Spark reads.
// ---------------------------------------------------------------------------
#[tokio::test]
#[ignore = "Spark parity: run via `make parity` (needs SPARK_HOME)"]
async fn parity_a_rs_writes_spark_reads() {
    if !enabled() {
        return;
    }
    let env = spark_env();
    for (table_type, type_name) in [
        (TableTypeValue::CopyOnWrite, "cow"),
        (TableTypeValue::MergeOnRead, "mor"),
    ] {
        for partitioned in [false, true] {
            let dir = tempdir().unwrap();
            let path = dir.path();
            let part = |p: &str| partitioned.then(|| p.to_string());
            let mut table = rs_create(path, table_type.clone(), partitioned).await;
            table
                .append([batch(&[
                    row("a", part("sf").as_deref(), 1, 10),
                    row("b", part("nyc").as_deref(), 1, 20),
                ])])
                .await
                .unwrap();
            table
                .upsert([batch(&[
                    row("a", part("sf").as_deref(), 2, 11),
                    row("c", part("sf").as_deref(), 1, 30),
                ])])
                .await
                .unwrap();
            table.delete("id = 'b'").await.unwrap();

            let expected = vec![
                row("a", part("sf").as_deref(), 2, 11),
                row("c", part("sf").as_deref(), 1, 30),
            ];
            assert_eq!(
                rs_read(path).await,
                expected,
                "[{type_name} partitioned={partitioned}] rs self-read"
            );
            assert_eq!(
                spark_read(&env, path),
                expected,
                "[{type_name} partitioned={partitioned}] spark must read the rs-written table"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Scenario B: Spark writes → hudi-rs reads.
// ---------------------------------------------------------------------------
#[tokio::test]
#[ignore = "Spark parity: run via `make parity` (needs SPARK_HOME)"]
async fn parity_b_spark_writes_rs_reads() {
    if !enabled() {
        return;
    }
    let env = spark_env();
    for type_name in ["cow", "mor"] {
        for partitioned in [false, true] {
            let dir = tempdir().unwrap();
            let path = dir.path().join("t");
            let part = |p: &str| partitioned.then(|| p.to_string());
            spark_write(
                &env,
                &path,
                "upsert",
                type_name,
                partitioned,
                &[
                    row("a", part("sf").as_deref(), 1, 10),
                    row("b", part("nyc").as_deref(), 1, 20),
                ],
            );
            spark_write(
                &env,
                &path,
                "upsert",
                type_name,
                partitioned,
                &[
                    row("a", part("sf").as_deref(), 2, 11),
                    row("c", part("sf").as_deref(), 1, 30),
                ],
            );
            spark_write(
                &env,
                &path,
                "delete",
                type_name,
                partitioned,
                &[row("b", part("nyc").as_deref(), 3, 20)],
            );

            let expected = vec![
                row("a", part("sf").as_deref(), 2, 11),
                row("c", part("sf").as_deref(), 1, 30),
            ];
            assert_eq!(
                spark_read(&env, &path),
                expected,
                "[{type_name} partitioned={partitioned}] spark self-read"
            );
            assert_eq!(
                rs_read(&path).await,
                expected,
                "[{type_name} partitioned={partitioned}] rs must read the spark-written table"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Scenario C: Spark table services on hudi-rs-written tables.
// ---------------------------------------------------------------------------
#[tokio::test]
#[ignore = "Spark parity: run via `make parity` (needs SPARK_HOME)"]
async fn parity_c_spark_services_on_rs_tables() {
    if !enabled() {
        return;
    }
    let env = spark_env();

    // MOR + compaction.
    {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let mut table = rs_create(path, TableTypeValue::MergeOnRead, true).await;
        table
            .append([batch(&[
                row("a", Some("sf"), 1, 10),
                row("b", Some("sf"), 1, 20),
            ])])
            .await
            .unwrap();
        table
            .upsert([batch(&[row("a", Some("sf"), 2, 11)])])
            .await
            .unwrap();
        let expected = vec![row("a", Some("sf"), 2, 11), row("b", Some("sf"), 1, 20)];

        spark_service(&env, path, "compact");
        assert_eq!(
            spark_read(&env, path),
            expected,
            "spark read after compacting the rs-written MOR table"
        );
        assert_eq!(
            rs_read(path).await,
            expected,
            "rs read after spark compacted the rs-written MOR table"
        );

        // rs writes on top of the compaction commit.
        let mut table = Table::new(path.to_str().unwrap()).await.unwrap();
        table
            .upsert([batch(&[
                row("a", Some("sf"), 3, 12),
                row("c", Some("nyc"), 1, 30),
            ])])
            .await
            .unwrap();
        table.delete("id = 'b'").await.unwrap();
        let expected = vec![row("a", Some("sf"), 3, 12), row("c", Some("nyc"), 1, 30)];
        assert_eq!(
            rs_read(path).await,
            expected,
            "rs read after rs writes on the compacted table"
        );
        assert_eq!(
            spark_read(&env, path),
            expected,
            "spark read after rs writes on the compacted table"
        );
    }

    // COW + clustering, then clean.
    {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let mut table = rs_create(path, TableTypeValue::CopyOnWrite, true).await;
        table
            .append([batch(&[row("a", Some("sf"), 1, 10)])])
            .await
            .unwrap();
        table
            .append([batch(&[row("b", Some("sf"), 1, 20)])])
            .await
            .unwrap();
        table
            .upsert([batch(&[row("a", Some("sf"), 2, 11)])])
            .await
            .unwrap();
        let expected = vec![row("a", Some("sf"), 2, 11), row("b", Some("sf"), 1, 20)];

        spark_service(&env, path, "cluster");
        assert_eq!(
            spark_read(&env, path),
            expected,
            "spark read after clustering"
        );
        assert_eq!(
            rs_read(path).await,
            expected,
            "rs read after spark clustering"
        );

        // rs writes on top of the clustering replacecommit: updates must land
        // in the clustered file groups (replaced groups stay dead).
        let mut table = Table::new(path.to_str().unwrap()).await.unwrap();
        table
            .upsert([batch(&[
                row("b", Some("sf"), 2, 21),
                row("c", Some("nyc"), 1, 30),
            ])])
            .await
            .unwrap();
        let expected = vec![
            row("a", Some("sf"), 2, 11),
            row("b", Some("sf"), 2, 21),
            row("c", Some("nyc"), 1, 30),
        ];
        assert_eq!(
            rs_read(path).await,
            expected,
            "rs read after rs upsert on the clustered table"
        );
        assert_eq!(
            spark_read(&env, path),
            expected,
            "spark read after rs upsert on the clustered table"
        );

        spark_service(&env, path, "clean");
        assert_eq!(rs_read(path).await, expected, "rs read after spark clean");
    }
}

// ---------------------------------------------------------------------------
// Scenario D: interleaved writers.
// ---------------------------------------------------------------------------
#[tokio::test]
#[ignore = "Spark parity: run via `make parity` (needs SPARK_HOME)"]
async fn parity_d_interleaved_writers() {
    if !enabled() {
        return;
    }
    let env = spark_env();
    for (table_type, type_name) in [
        (TableTypeValue::CopyOnWrite, "cow"),
        (TableTypeValue::MergeOnRead, "mor"),
    ] {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let mut table = rs_create(path, table_type, true).await;
        table
            .append([batch(&[
                row("a", Some("sf"), 1, 10),
                row("b", Some("sf"), 1, 20),
            ])])
            .await
            .unwrap();

        // Spark upserts on the rs-created table.
        spark_write(
            &env,
            path,
            "upsert",
            type_name,
            true,
            &[row("a", Some("sf"), 2, 11), row("c", Some("nyc"), 1, 30)],
        );

        // rs upserts on top of the spark commit.
        let mut table = Table::new(path.to_str().unwrap()).await.unwrap();
        table
            .upsert([batch(&[row("b", Some("sf"), 2, 21)])])
            .await
            .unwrap();

        let expected = vec![
            row("a", Some("sf"), 2, 11),
            row("b", Some("sf"), 2, 21),
            row("c", Some("nyc"), 1, 30),
        ];
        assert_eq!(
            rs_read(path).await,
            expected,
            "[{type_name}] rs read after interleaved writes"
        );
        assert_eq!(
            spark_read(&env, path),
            expected,
            "[{type_name}] spark read after interleaved writes"
        );
    }
}

// ---------------------------------------------------------------------------
// Scenario E: mixed-operation workload with enough commits for timeline
// archival, 5 partitions, tens of file groups — verified across snapshot,
// time-travel, and incremental queries in both engines.
// ---------------------------------------------------------------------------
#[tokio::test]
#[ignore = "Spark parity: run via `make parity` (needs SPARK_HOME)"]
async fn parity_e_mixed_workload_archival_and_query_types() {
    use std::collections::BTreeMap;

    if !enabled() {
        return;
    }
    let env = spark_env();
    let parts = ["p0", "p1", "p2", "p3", "p4"];

    for (table_type, type_name) in [
        (TableTypeValue::CopyOnWrite, "cow"),
        (TableTypeValue::MergeOnRead, "mor"),
    ] {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let mut table = rs_create(path, table_type.clone(), true).await;

        // Expected table state keyed by record key.
        let mut expected: BTreeMap<String, Row> = BTreeMap::new();
        let mut next_id: usize = 0;
        let mut clock: i64 = 0;
        // (instant, snapshot-at-instant) checkpoints taken late in the
        // workload so they stay in the active timeline after archival.
        let mut checkpoints: Vec<(String, Vec<Row>)> = Vec::new();

        let snapshot = |m: &BTreeMap<String, Row>| -> Vec<Row> {
            let mut rows: Vec<Row> = m.values().cloned().collect();
            rows.sort();
            rows
        };

        // 6 rounds x 6 ops = 36 commits; archival (keeps 20-30 active)
        // must kick in on the data timeline.
        for round in 0..6 {
            // 1. append: 10 new rows, 2 per partition (new file groups).
            let mut rows = Vec::new();
            for (i, part) in parts.iter().enumerate() {
                for _ in 0..2 {
                    clock += 1;
                    let id = format!("id-{next_id:04}");
                    next_id += 1;
                    let r = row(&id, Some(part), clock, (round * 100 + i) as i64);
                    expected.insert(id, r.clone());
                    rows.push(r);
                }
            }
            table.append([batch(&rows)]).await.unwrap();

            // 2. update: bump 5 existing rows via upsert.
            let keys: Vec<String> = expected.keys().take(5).cloned().collect();
            let mut rows = Vec::new();
            for id in keys {
                clock += 1;
                let old = expected.get(&id).unwrap().clone();
                let r = (id.clone(), old.1.clone(), clock, old.3 + 1000);
                expected.insert(id, r.clone());
                rows.push(r);
            }
            table.upsert([batch(&rows)]).await.unwrap();

            // 3. insert: 5 new rows via upsert (small-file packing path).
            let mut rows = Vec::new();
            for (i, part) in parts.iter().enumerate() {
                clock += 1;
                let id = format!("id-{next_id:04}");
                next_id += 1;
                let r = row(&id, Some(part), clock, (round * 100 + 50 + i) as i64);
                expected.insert(id, r.clone());
                rows.push(r);
            }
            table.upsert([batch(&rows)]).await.unwrap();

            // 4. delete: drop 2 existing rows.
            let doomed: Vec<String> = expected.keys().skip(7).take(2).cloned().collect();
            let filter = format!("id IN ('{}', '{}')", doomed[0], doomed[1]);
            for id in &doomed {
                expected.remove(id);
            }
            table.delete(&filter).await.unwrap();

            // 5. update again: 3 rows.
            let keys: Vec<String> = expected.keys().rev().take(3).cloned().collect();
            let mut rows = Vec::new();
            for id in keys {
                clock += 1;
                let old = expected.get(&id).unwrap().clone();
                let r = (id.clone(), old.1.clone(), clock, old.3 + 5000);
                expected.insert(id, r.clone());
                rows.push(r);
            }
            table.upsert([batch(&rows)]).await.unwrap();

            // 6. append again: 5 more rows, one per partition.
            let mut rows = Vec::new();
            for part in parts.iter() {
                clock += 1;
                let id = format!("id-{next_id:04}");
                next_id += 1;
                let r = row(&id, Some(part), clock, 7 + clock);
                expected.insert(id, r.clone());
                rows.push(r);
            }
            table.append([batch(&rows)]).await.unwrap();

            // Take checkpoints late in the workload (rounds 4 and 5) so the
            // checkpoint instants survive archival in the active timeline.
            if round >= 4 {
                checkpoints.push((latest_completed_instant(path), snapshot(&expected)));
            }
        }

        // Archival must have kicked in on the data timeline.
        let manifest_dir = path.join(".hoodie").join("timeline").join("history");
        let archived: Vec<_> = std::fs::read_dir(&manifest_dir)
            .expect("timeline history dir must exist after 36 commits")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(
            archived.iter().any(|n| n.starts_with("manifest_")),
            "[{type_name}] archival should have produced an LSM manifest, saw {archived:?}"
        );

        // Snapshot parity.
        let expected_rows = snapshot(&expected);
        assert!(expected_rows.len() > 50, "workload should retain 50+ rows");
        assert_eq!(
            rs_read(path).await,
            expected_rows,
            "[{type_name}] rs snapshot after mixed workload"
        );
        assert_eq!(
            spark_read(&env, path),
            expected_rows,
            "[{type_name}] spark snapshot after mixed workload (MDT-checked)"
        );

        // Time-travel parity at each checkpoint.
        for (i, (instant, rows_at)) in checkpoints.iter().enumerate() {
            assert_eq!(
                &rs_read_as_of(path, instant).await,
                rows_at,
                "[{type_name}] rs time-travel at checkpoint {i} ({instant})"
            );
            assert_eq!(
                &spark_read_as_of(&env, path, instant),
                rows_at,
                "[{type_name}] spark time-travel at checkpoint {i} ({instant})"
            );
        }

        // Incremental parity: cross-engine agreement from the first
        // checkpoint (open-ended) and between the two checkpoints.
        let begin = &checkpoints[0].0;
        let end = &checkpoints[1].0;
        let rs_inc = rs_read_incremental(path, begin, None).await;
        let spark_inc = spark_read_incremental(&env, path, begin, None);
        assert!(!rs_inc.is_empty(), "[{type_name}] rs incremental non-empty");
        assert_eq!(
            rs_inc, spark_inc,
            "[{type_name}] incremental from {begin} must agree across engines"
        );
        let rs_inc_bounded = rs_read_incremental(path, begin, Some(end)).await;
        let spark_inc_bounded = spark_read_incremental(&env, path, begin, Some(end));
        assert_eq!(
            rs_inc_bounded, spark_inc_bounded,
            "[{type_name}] bounded incremental ({begin}, {end}] must agree across engines"
        );
    }
}

// ---------------------------------------------------------------------------
// Scenario F: CDC-enabled Spark table — rs snapshot reads must tolerate CDC
// artifacts; Spark CDC read sanity-checked. (rs-side CDC-format incremental
// queries are not yet implemented.)
// ---------------------------------------------------------------------------
#[tokio::test]
#[ignore = "Spark parity: run via `make parity` (needs SPARK_HOME)"]
async fn parity_f_cdc_enabled_spark_table() {
    if !enabled() {
        return;
    }
    let env = spark_env();
    let dir = tempdir().unwrap();
    let path = dir.path().join("t");

    let cdc_opt = &["--write-option", "hoodie.table.cdc.enabled=true"];
    spark_write_with(
        &env,
        &path,
        "upsert",
        "cow",
        true,
        &[row("a", Some("sf"), 1, 10), row("b", Some("nyc"), 1, 20)],
        cdc_opt,
    );
    let begin = latest_completed_instant(&path);
    spark_write_with(
        &env,
        &path,
        "upsert",
        "cow",
        true,
        &[row("a", Some("sf"), 2, 11)],
        cdc_opt,
    );
    spark_write_with(
        &env,
        &path,
        "delete",
        "cow",
        true,
        &[row("b", Some("nyc"), 3, 20)],
        cdc_opt,
    );

    let expected = vec![row("a", Some("sf"), 2, 11)];
    assert_eq!(
        spark_read(&env, &path),
        expected,
        "spark snapshot on CDC table"
    );
    assert_eq!(
        rs_read(&path).await,
        expected,
        "rs snapshot must tolerate CDC artifacts"
    );

    // Spark CDC read sanity: the raw CDC dump must be non-empty (update +
    // delete after `begin`). Row shape differs from the harness Row type, so
    // only presence is asserted here.
    let out = path.with_extension("spark_cdc.json");
    run_spark(
        &env,
        "spark_read.py",
        &[
            "--path",
            &path.to_string_lossy(),
            "--out",
            &out.to_string_lossy(),
            "--mode",
            "cdc",
            "--begin",
            &begin,
        ],
    );
    let text = std::fs::read_to_string(&out).expect("cdc output");
    let parsed: serde_json::Value = serde_json::from_str(&text).expect("cdc json");
    assert!(
        !parsed.as_array().expect("array").is_empty(),
        "spark CDC read should surface changes after {begin}"
    );
}
