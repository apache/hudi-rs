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

//! A tour of the native write API: create a partitioned table, then append,
//! upsert, update, delete, and overwrite it — reading back after each step.
//!
//! Run with: `cargo run -p hudi-core --example table_writes`

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::table::TableTypeValue;
use hudi_core::table::{ReadOptions, Table};

fn batch(rows: &[(&str, &str, i64, i64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("fare", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
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

async fn show(table: &Table, label: &str) {
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    println!("{label}: {rows} rows");
}

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().unwrap();

    // Create: table config, key/partition/ordering fields, metadata table +
    // record-level index on by default. `MergeOnRead` works the same way.
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_table_type(TableTypeValue::CopyOnWrite)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["ts"]) // event-time ordering; omit for commit-time
        .create()
        .await
        .unwrap();

    // Append: insert-only fast path (small files are packed on later upserts).
    table
        .append([batch(&[
            ("t1", "sf", 1, 30),
            ("t2", "sf", 1, 45),
            ("t3", "nyc", 1, 20),
        ])])
        .await
        .unwrap();
    show(&table, "after append").await;

    // Upsert: full-row upsert by record key, routed via the record-level index.
    table
        .upsert([batch(&[("t2", "sf", 2, 50), ("t4", "nyc", 2, 15)])])
        .await
        .unwrap();
    show(&table, "after upsert").await;

    // Update: SQL-ish SET on rows matching a filter. The single-row SET batch
    // carries only the columns to change.
    let set_fare = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "fare",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![99])) as ArrayRef],
    )
    .unwrap();
    table.update("city = 'nyc'", set_fare).await.unwrap();

    // Delete: by filter — key predicates route through the index; other
    // predicates scan only the file groups holding matching rows.
    table.delete("fare > 60").await.unwrap();
    show(&table, "after update + delete").await;

    // Overwrite: INSERT_OVERWRITE_TABLE (full replace); use
    // `dynamic_partition_overwrite` to replace only the partitions present
    // in the input.
    table
        .overwrite([batch(&[("t9", "sf", 9, 10)])])
        .await
        .unwrap();
    show(&table, "after overwrite").await;

    // Time travel and incremental reads work against the write timeline.
    let opts = ReadOptions::new().with_query_type(hudi_core::table::QueryType::Incremental);
    let _changes = table.read(&opts).await;
}
