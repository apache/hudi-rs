// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Known-value lookup tests against a fresh SQL-generated Hudi table.

use arrow_array::{Array, Decimal128Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use hudi_core::metadata::table::record_index::record_index_shard;
use hudi_core::table::Table;
use std::collections::BTreeMap;
use std::path::PathBuf;

// Expected latest state from v9_txns_nonpart_meta.sql; amounts are integer cents.
const ROWS: &[(&str, &str, &str, i128)] = &[
    ("TXN-001", "ACC-A", "reversal", 125000),
    ("TXN-003", "ACC-A", "transfer", 500000),
    ("TXN-004", "ACC-C", "debit", 45075),
    ("TXN-006", "ACC-C", "debit", 17550),
    ("TXN-007", "ACC-E", "debit", 890000),
    ("TXN-008", "ACC-F", "debit", 32025),
    ("TXN-009", "ACC-G", "debit", 150000),
    ("TXN-010", "ACC-H", "transfer", 220000),
    ("TXN-011", "ACC-I", "debit", 99999),
    ("TXN-012", "ACC-J", "debit", 35000),
    ("TXN-013", "ACC-K", "debit", 75000),
    ("TXN-014", "ACC-L", "debit", 12550),
    ("TXN-015", "ACC-M", "debit", 450000),
    ("TXN-016", "ACC-N", "debit", 8800),
];

async fn fixture() -> (tempfile::TempDir, Table) {
    let (directory, path) = tokio::task::spawn_blocking(|| {
        let zip = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../test/data/sample_table/cow/v9_txns_nonpart_meta.zip");
        let extracted = hudi_test::extract_test_table_fresh(&zip);
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("table");
        std::fs::rename(extracted, &root).unwrap();
        let path = std::fs::read_dir(&root)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.join(".hoodie").is_dir())
            .unwrap();
        (directory, path)
    })
    .await
    .unwrap();
    (directory, Table::new(path.to_str().unwrap()).await.unwrap())
}

async fn assert_lookup(table: &Table, keys: &[&str], expected_keys: &[&str]) {
    let keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
    let projection = ["txn_id", "account_id", "txn_type", "amount"].map(String::from);
    let batches: Vec<RecordBatch> = table
        .lookup_records(&keys, Some(&projection))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut actual = BTreeMap::new();
    for batch in batches {
        assert_eq!(batch.num_columns(), 4);
        let strings = |name| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
        };
        let amounts = batch
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(amounts.scale(), 2);
        for i in 0..batch.num_rows() {
            assert!(!amounts.is_null(i));
            let previous = actual.insert(
                strings("txn_id").value(i).to_string(),
                (
                    strings("account_id").value(i).to_string(),
                    strings("txn_type").value(i).to_string(),
                    amounts.value(i),
                ),
            );
            assert!(previous.is_none(), "lookup returned a duplicate record");
        }
    }
    let expected: BTreeMap<_, _> = ROWS
        .iter()
        .filter(|r| expected_keys.contains(&r.0))
        .map(|r| (r.0.to_string(), (r.1.to_string(), r.2.to_string(), r.3)))
        .collect();
    assert_eq!(expected.len(), expected_keys.len());
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn test_lookup_single_key_returns_known_updated_values() {
    let (_directory, table) = fixture().await;
    assert_lookup(&table, &["TXN-001"], &["TXN-001"]).await;
}

#[tokio::test]
async fn test_lookup_multiple_keys_same_shard_returns_each_once() {
    let (_directory, table) = fixture().await;
    let lookup = table.prepare_record_lookup(None).await.unwrap();
    // Independent Java hashCode vectors for the fixture's ten physical shards.
    assert_eq!(lookup.shard_for_key("TXN-001").unwrap(), 8);
    assert_eq!(lookup.shard_for_key("TXN-010").unwrap(), 8);
    let locations = lookup
        .lookup_shard(8, &["TXN-001", "TXN-010"])
        .await
        .unwrap();
    let mut keys: Vec<_> = locations.iter().map(|l| l.key.as_str()).collect();
    keys.sort();
    assert_eq!(keys, ["TXN-001", "TXN-010"]);
    assert_lookup(
        &table,
        &["TXN-010", "TXN-001", "TXN-010"],
        &["TXN-001", "TXN-010"],
    )
    .await;
    assert!(lookup.lookup_shard(7, &["TXN-001"]).await.is_err());
}

#[tokio::test]
async fn test_lookup_cross_shard_returns_known_table_state() {
    let (_directory, table) = fixture().await;
    let keys: Vec<_> = ROWS.iter().map(|r| r.0).collect();
    assert_lookup(&table, &keys, &keys).await;
}

#[tokio::test]
async fn test_lookup_missing_deleted_and_empty_keys_return_no_rows() {
    let (_directory, table) = fixture().await;
    for key in ["TXN-002", "TXN-005", "missing"] {
        assert_lookup(&table, &[key], &[]).await;
    }
    assert_lookup(&table, &[], &[]).await;
    assert_lookup(&table, &["TXN-002", "TXN-001", "missing"], &["TXN-001"]).await;
}

#[test]
fn test_record_index_routing_balances_distinct_key_workload() {
    // Count work instead of elapsed time to make skew regression checks stable in CI.
    // This bounds uniform-key load; a hot key still belongs to one physical shard.
    for shards in [10, 16] {
        for prefix in ["TXN-", "customer/", "客户/😀/"] {
            let mut loads = vec![0usize; shards];
            for i in 0..100_000 {
                let key = format!("{prefix}{i:08}");
                loads[record_index_shard(&key, shards).unwrap()] += 1;
            }
            let average = 100_000 / shards;
            assert_eq!(loads.iter().sum::<usize>(), 100_000);
            assert!(
                loads
                    .iter()
                    .all(|&n| n >= average * 3 / 4 && n <= average * 5 / 4),
                "excessive shard skew for {prefix}, {shards} shards: {loads:?}"
            );
        }
    }
    assert!(record_index_shard("key", 0).is_err());
    assert!(record_index_shard("key", i32::MAX as usize + 1).is_err());
}

#[test]
fn test_record_index_routing_sequential_keys_expose_64_shard_hotspot() {
    // Hudi's hash is a storage-format contract. Changing it to balance work
    // would route existing records to the wrong files; scheduling must handle skew.
    let mut loads = [0usize; 64];
    for i in 0..100_000 {
        loads[record_index_shard(&format!("TXN-{i:08}"), 64).unwrap()] += 1;
    }
    assert_eq!(loads.iter().sum::<usize>(), 100_000);
    assert_eq!(loads.iter().min(), Some(&270));
    assert_eq!(loads.iter().max(), Some(&3016));
}
