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
//! Model-based write testing: drive deterministic pseudo-random operation
//! sequences against a table and an in-memory oracle, asserting after every
//! operation that reads, partition filters, meta fields, and the timeline all
//! agree with the model. Targets interaction seams (event-time × partial ×
//! partition moves × replace verbs) that example-based tests cannot cover
//! combinatorially.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hudi_core::config::table::TableTypeValue;
use hudi_core::index::HoodieKey;
use hudi_core::table::{ReadOptions, Table, UpsertOptions};
use tempfile::tempdir;

/// Deterministic LCG so failures reproduce from the printed seed.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Row {
    city: String,
    ts: i64,
    v: i64,
}

/// In-memory oracle mirroring the intended single-writer semantics:
/// - full upsert: same-partition merge by event-time ordering (ties: incoming
///   wins); changed partition moves the row unconditionally (global-index
///   update-partition-path).
/// - partial upsert (COW): only `v` updates when the incoming row wins the
///   event-time merge; `ts` keeps the OLD value (non-update column).
/// - overwrite: table becomes the batch. dpo: touched partitions become the
///   batch's rows for those partitions. delete_keys: rows vanish.
type Oracle = BTreeMap<String, Row>;

const CITIES: [&str; 3] = ["sf", "nyc", "la"];

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn batch_of(rows: &[(String, String, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1.as_str()).collect::<Vec<_>>(),
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

/// In-batch dedup the way the writer documents it: highest ordering wins,
/// ties resolved to the later occurrence.
fn dedup_batch(rows: Vec<(String, String, i64, i64)>) -> Vec<(String, String, i64, i64)> {
    let mut best: BTreeMap<String, (String, String, i64, i64)> = BTreeMap::new();
    for row in rows {
        match best.get(&row.0) {
            Some(existing) if existing.2 > row.2 => {}
            _ => {
                best.insert(row.0.clone(), row);
            }
        }
    }
    best.into_values().collect()
}

async fn read_table(table: &Table) -> Vec<(String, String, i64, i64, String)> {
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let mut out = Vec::new();
    for b in &batches {
        let ids = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let cities = b
            .column_by_name("city")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tss = b
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let vs = b
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let paths = b
            .column_by_name("_hoodie_partition_path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((
                ids.value(i).to_string(),
                cities.value(i).to_string(),
                tss.value(i),
                vs.value(i),
                paths.value(i).to_string(),
            ));
        }
    }
    out.sort();
    out
}

async fn check_invariants(table: &Table, oracle: &Oracle, context: &str) {
    let actual = read_table(table).await;
    // No duplicate keys.
    let mut seen = HashSet::new();
    for (id, ..) in &actual {
        assert!(
            seen.insert(id.clone()),
            "{context}: duplicate key '{id}'; full read: {actual:?}"
        );
    }
    // Content matches oracle exactly.
    let expected: Vec<(String, String, i64, i64)> = oracle
        .iter()
        .map(|(id, r)| (id.clone(), r.city.clone(), r.ts, r.v))
        .collect();
    let actual_data: Vec<(String, String, i64, i64)> = actual
        .iter()
        .map(|(id, city, ts, v, _)| (id.clone(), city.clone(), *ts, *v))
        .collect();
    assert_eq!(
        actual_data, expected,
        "{context}: table diverged from oracle"
    );
    // Meta partition path agrees with the data column.
    for (id, city, _, _, path) in &actual {
        assert_eq!(
            path,
            &format!("city={city}"),
            "{context}: key '{id}' partition path mismatch"
        );
    }
    // Partition-filtered reads partition the table (no strays, no losses).
    let mut filtered_total = 0usize;
    for city in CITIES {
        let batches = table
            .read(
                &ReadOptions::new()
                    .with_filters([("city", "=", city)])
                    .unwrap(),
            )
            .await
            .unwrap();
        let count: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let expected_count = oracle.values().filter(|r| r.city == city).count();
        assert_eq!(
            count, expected_count,
            "{context}: filter city='{city}' row count mismatch"
        );
        filtered_total += count;
    }
    assert_eq!(
        filtered_total,
        oracle.len(),
        "{context}: partition reads do not cover the table"
    );
}

fn check_timeline_hygiene(base: &std::path::Path, context: &str) {
    let dir = base.join(".hoodie/timeline");
    let names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    for name in &names {
        if let Some(stem) = name.strip_suffix(".requested") {
            // {ts}.{action}.requested must have a completed {ts}_{completion}.{action}
            let (ts, action) = stem.split_once('.').unwrap();
            let completed = names
                .iter()
                .any(|n| n.starts_with(&format!("{ts}_")) && n.ends_with(&format!(".{action}")));
            assert!(
                completed,
                "{context}: pending instant '{name}' has no completed instant"
            );
        }
    }
}

async fn run_sequence(table_type: TableTypeValue, metadata: bool, seed: u64, ops: usize) {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("model")
        .with_table_type(table_type.clone())
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["ts"])
        .with_metadata(metadata)
        .create()
        .await
        .unwrap();

    let mut rng = Rng(seed);
    let mut oracle: Oracle = BTreeMap::new();
    let mut next_key = 0usize;
    let mut next_v = 0i64;
    let key_pool: Vec<String> = (0..24).map(|i| format!("k{i:02}")).collect();
    let is_cow = table_type == TableTypeValue::CopyOnWrite;

    for op_index in 0..ops {
        let context = format!("{table_type:?} mdt={metadata} seed={seed} op#{op_index}");
        let mut rand_rows = |rng: &mut Rng, allow_existing: bool| {
            let count = 1 + rng.below(3) as usize;
            let mut rows = Vec::new();
            for _ in 0..count {
                let key = if allow_existing {
                    key_pool[rng.below(key_pool.len() as u64) as usize].clone()
                } else {
                    // Fresh key outside the pool: appends must not collide.
                    next_key += 1;
                    format!("a{next_key:03}")
                };
                let city = CITIES[rng.below(3) as usize].to_string();
                let ts = rng.below(50) as i64;
                next_v += 1;
                rows.push((key, city, ts, next_v));
            }
            rows
        };
        match rng.below(100) {
            // Full upsert: merges, moves, stale rejections.
            0..=39 => {
                let rows = rand_rows(&mut rng, true);
                eprintln!("{context}: upsert {rows:?}");
                table.upsert([batch_of(&rows)]).await.unwrap();
                for (id, city, ts, v) in dedup_batch(rows) {
                    match oracle.get(&id) {
                        Some(old) if old.city == city && old.ts > ts => {}
                        _ => {
                            oracle.insert(id, Row { city, ts, v });
                        }
                    }
                }
            }
            // Append with fresh keys.
            40..=54 => {
                let rows = rand_rows(&mut rng, false);
                eprintln!("{context}: append {rows:?}");
                table.append([batch_of(&rows)]).await.unwrap();
                for (id, city, ts, v) in rows {
                    oracle.insert(id, Row { city, ts, v });
                }
            }
            // Partial upsert of `v` only (COW; unsupported on MOR). Keys keep
            // their current partition; missing keys insert as full rows.
            55..=64 if is_cow => {
                let mut rows = rand_rows(&mut rng, true);
                for row in &mut rows {
                    if let Some(existing) = oracle.get(&row.0) {
                        row.1 = existing.city.clone();
                    }
                }
                eprintln!("{context}: partial {rows:?}");
                table
                    .upsert_with(
                        [batch_of(&rows)],
                        UpsertOptions {
                            update_columns: Some(vec!["v".to_string()]),
                        },
                    )
                    .await
                    .unwrap();
                for (id, city, ts, v) in dedup_batch(rows) {
                    match oracle.get_mut(&id) {
                        Some(old) => {
                            if ts >= old.ts {
                                old.v = v; // ts is not an update column
                            }
                        }
                        None => {
                            oracle.insert(id, Row { city, ts, v });
                        }
                    }
                }
            }
            // Keyed delete of currently-live keys.
            65..=74 => {
                let candidates: Vec<String> = oracle.keys().cloned().collect();
                if candidates.is_empty() {
                    continue;
                }
                let mut doomed = HashSet::new();
                for _ in 0..=rng.below(2) {
                    doomed.insert(candidates[rng.below(candidates.len() as u64) as usize].clone());
                }
                let keys: Vec<HoodieKey> = doomed
                    .iter()
                    .map(|id| HoodieKey {
                        record_key: id.clone(),
                        partition_path: format!("city={}", oracle[id].city),
                    })
                    .collect();
                eprintln!("{context}: delete {keys:?}");
                table.delete_keys(keys).await.unwrap();
                for id in doomed {
                    oracle.remove(&id);
                }
            }
            // Full overwrite.
            75..=79 => {
                let rows = rand_rows(&mut rng, true);
                let rows = dedup_batch(rows);
                // overwrite clears the table, so no pinning needed.
                eprintln!("{context}: overwrite {rows:?}");
                table.overwrite([batch_of(&rows)]).await.unwrap();
                oracle.clear();
                for (id, city, ts, v) in rows {
                    oracle.insert(id, Row { city, ts, v });
                }
            }
            // Dynamic partition overwrite.
            _ => {
                let mut rows = rand_rows(&mut rng, true);
                // dpo is insert-type (no global index): a key written to a new
                // partition would duplicate its live copy elsewhere, which is
                // Java-consistent but outside this oracle. Pin existing keys
                // to their current partition.
                for row in &mut rows {
                    if let Some(existing) = oracle.get(&row.0) {
                        row.1 = existing.city.clone();
                    }
                }
                let rows = dedup_batch(rows);
                let touched: HashSet<String> = rows.iter().map(|r| r.1.clone()).collect();
                eprintln!("{context}: dpo {rows:?}");
                table
                    .dynamic_partition_overwrite([batch_of(&rows)])
                    .await
                    .unwrap();
                oracle.retain(|_, row| !touched.contains(&row.city));
                for (id, city, ts, v) in rows {
                    oracle.insert(id, Row { city, ts, v });
                }
            }
        }
        check_invariants(&table, &oracle, &context).await;
        check_timeline_hygiene(dir.path(), &context);
    }
}

#[tokio::test]
async fn test_model_cow_with_mdt() {
    run_sequence(TableTypeValue::CopyOnWrite, true, 7, 30).await;
}

/// Deep sweep for local runs: `cargo test ... test_model_extended_sweep -- --ignored`
#[tokio::test]
#[ignore = "long-running fuzz sweep; run on demand"]
async fn test_model_extended_sweep() {
    for seed in [19u64, 23, 29, 31] {
        run_sequence(TableTypeValue::CopyOnWrite, true, seed, 60).await;
        run_sequence(TableTypeValue::MergeOnRead, true, seed, 60).await;
        run_sequence(TableTypeValue::CopyOnWrite, false, seed, 60).await;
        run_sequence(TableTypeValue::MergeOnRead, false, seed, 60).await;
    }
}

#[tokio::test]
async fn test_model_cow_without_mdt() {
    run_sequence(TableTypeValue::CopyOnWrite, false, 11, 30).await;
}

#[tokio::test]
async fn test_model_mor_with_mdt() {
    run_sequence(TableTypeValue::MergeOnRead, true, 13, 30).await;
}

#[tokio::test]
async fn test_model_mor_without_mdt() {
    run_sequence(TableTypeValue::MergeOnRead, false, 17, 30).await;
}
