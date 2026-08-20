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
//! Small-file packing and insert bucket sizing (Java `UpsertPartitioner`).

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::table::{ReadOptions, Table};
use tempfile::tempdir;

fn batch(rows: Vec<(&str, i64)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_cow_upsert_packs_inserts_into_small_file_group() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    table.upsert([batch(vec![("a", 1)])]).await.unwrap();
    // The existing tiny base file is a small-file candidate: the new insert
    // must be packed into the SAME file group (a new slice, same fileId).
    table.upsert([batch(vec![("b", 2)])]).await.unwrap();

    let slices = table.get_file_slices(&ReadOptions::new()).await.unwrap();
    assert_eq!(
        slices.len(),
        1,
        "insert must pack into the existing small file group"
    );
    let rows: usize = table
        .read(&ReadOptions::new())
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn test_cow_upsert_preserves_multiple_file_groups() {
    // Regression: with two file groups in one partition, an upsert touching
    // keys in both must NOT collapse them into a single group (which would
    // duplicate the untouched group's rows).
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        // Disable packing so the two appends create distinct groups and the
        // upsert exercises multi-group routing.
        .with_option("hoodie.parquet.small.file.limit", "0")
        .create()
        .await
        .unwrap();
    table.append([batch(vec![("a", 1)])]).await.unwrap();
    table.append([batch(vec![("b", 2)])]).await.unwrap();
    let before: HashSet<String> = table
        .get_file_slices(&ReadOptions::new())
        .await
        .unwrap()
        .iter()
        .map(|s| s.file_id().to_string())
        .collect();
    assert_eq!(before.len(), 2);

    table
        .upsert([batch(vec![("a", 10), ("b", 20)])])
        .await
        .unwrap();

    let after: HashSet<String> = table
        .get_file_slices(&ReadOptions::new())
        .await
        .unwrap()
        .iter()
        .map(|s| s.file_id().to_string())
        .collect();
    assert_eq!(after, before, "updates must stay in their file groups");
    let rows = table.read(&ReadOptions::new()).await.unwrap();
    let mut ids: Vec<(String, i64)> = rows
        .iter()
        .flat_map(|b| {
            let ids = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = b
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..b.num_rows())
                .map(|i| (ids.value(i).to_string(), values.value(i)))
                .collect::<Vec<_>>()
        })
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec![("a".to_string(), 10), ("b".to_string(), 20)],
        "no duplicated rows after multi-group upsert"
    );
}

#[tokio::test]
async fn test_cow_insert_bucket_split_creates_multiple_groups() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_option("hoodie.copyonwrite.insert.auto.split", "false")
        .with_option("hoodie.copyonwrite.insert.split.size", "2")
        .create()
        .await
        .unwrap();
    table
        .upsert([batch(vec![
            ("a", 1),
            ("b", 2),
            ("c", 3),
            ("d", 4),
            ("e", 5),
        ])])
        .await
        .unwrap();

    let slices = table.get_file_slices(&ReadOptions::new()).await.unwrap();
    assert_eq!(
        slices.len(),
        3,
        "5 inserts at split size 2 → 3 new file groups"
    );
    let rows: usize = table
        .read(&ReadOptions::new())
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 5);
}

#[tokio::test]
async fn test_parallel_and_sequential_writes_equivalent() {
    async fn run(parallelism: Option<&str>) -> (Vec<(String, i64)>, usize) {
        let dir = tempdir().unwrap();
        let mut builder = Table::create(dir.path().to_str().unwrap())
            .with_table_name("trips")
            .with_record_key_fields(["id"])
            // Force multiple chunks per partition so several tasks run.
            .with_option("hoodie.parquet.max.file.size", "4096")
            .with_option("hoodie.copyonwrite.record.size.estimate", "1024");
        if let Some(n) = parallelism {
            builder = builder.with_option("hoodie.write.task.parallelism", n);
        }
        let mut table = builder.create().await.unwrap();
        let rows: Vec<(String, i64)> = (0..20).map(|i| (format!("k{i:02}"), i)).collect();
        let batch = batch(rows.iter().map(|(k, v)| (k.as_str(), *v)).collect());
        table.append([batch]).await.unwrap();
        let mut read: Vec<(String, i64)> = table
            .read(&ReadOptions::new())
            .await
            .unwrap()
            .iter()
            .flat_map(|b| {
                let ids = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let values = b
                    .column_by_name("value")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..b.num_rows())
                    .map(|i| (ids.value(i).to_string(), values.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect();
        read.sort();
        let groups = table
            .get_file_slices(&ReadOptions::new())
            .await
            .unwrap()
            .len();
        (read, groups)
    }

    let (parallel_rows, parallel_groups) = run(None).await;
    let (sequential_rows, sequential_groups) = run(Some("1")).await;
    assert_eq!(parallel_rows, sequential_rows);
    assert_eq!(parallel_groups, sequential_groups);
    assert!(
        parallel_groups > 1,
        "test must exercise multiple write tasks"
    );
}
