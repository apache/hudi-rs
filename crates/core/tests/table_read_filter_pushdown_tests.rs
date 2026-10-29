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
//! Filters pushed into the parquet reader must not change what a read returns,
//! on copy-on-write and on merge-on-read tables whose log files update the
//! filtered columns.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::table::TableTypeValue;
use hudi_core::table::{ReadOptions, Table};
use tempfile::tempdir;

fn batch(ids: &[&str], values: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

fn filters(filters: &[(&str, &str, &str)]) -> ReadOptions {
    ReadOptions::new()
        .with_filters(filters.iter().copied())
        .unwrap()
}

async fn rows(table: &Table, options: &ReadOptions) -> Vec<(String, i64)> {
    let mut out: Vec<(String, i64)> = table
        .read(options)
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
            ids.iter()
                .zip(values.iter())
                .map(|(i, v)| (i.unwrap().to_string(), v.unwrap()))
                .collect::<Vec<_>>()
        })
        .collect();
    out.sort();
    out
}

/// Two file groups per table, with a few rows per parquet row group so the
/// row-group selection has something to drop.
async fn table(dir: &std::path::Path, table_type: TableTypeValue) -> Table {
    let mut table = Table::create(dir.to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_table_type(table_type)
        .with_option("hoodie.parquet.small.file.limit", "0")
        .with_option("hoodie.parquet.block.size", "2048")
        .with_option("hoodie.copyonwrite.record.size.estimate", "1024")
        .create()
        .await
        .unwrap();
    table
        .append([batch(&["a1", "a2", "a3", "a4"], &[1, 2, 3, 4])])
        .await
        .unwrap();
    table
        .append([batch(&["b1", "b2", "b3", "b4"], &[5, 6, 7, 8])])
        .await
        .unwrap();
    Table::new(dir.to_str().unwrap()).await.unwrap()
}

#[tokio::test]
async fn test_pushed_filters_return_the_same_rows_on_copy_on_write() {
    let dir = tempdir().unwrap();
    let table = table(dir.path(), TableTypeValue::CopyOnWrite).await;
    assert_eq!(
        rows(
            &table,
            &filters(&[("value", ">=", "3"), ("value", "<", "7")])
        )
        .await,
        vec![
            ("a3".to_string(), 3),
            ("a4".to_string(), 4),
            ("b1".to_string(), 5),
            ("b2".to_string(), 6),
        ]
    );
    assert_eq!(
        rows(&table, &filters(&[("_hoodie_record_key", "IN", "a2,b4")])).await,
        vec![("a2".to_string(), 2), ("b4".to_string(), 8)]
    );
    assert_eq!(rows(&table, &filters(&[("id", "=", "zz")])).await, vec![]);
}

/// A log file changes `value`; a filter on `value` must see the merged row,
/// so it cannot drop base rows before the merge, while a filter on the key
/// can.
#[tokio::test]
async fn test_pushed_filters_see_merged_rows_on_merge_on_read() {
    let dir = tempdir().unwrap();
    table(dir.path(), TableTypeValue::MergeOnRead).await;
    let mut writer = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    writer
        .upsert([batch(&["a2", "b3"], &[200, 300])])
        .await
        .unwrap();
    let table = Table::new(dir.path().to_str().unwrap()).await.unwrap();

    // The old values are gone and the new ones match.
    assert_eq!(rows(&table, &filters(&[("value", "=", "2")])).await, vec![]);
    assert_eq!(
        rows(&table, &filters(&[("value", ">", "100")])).await,
        vec![("a2".to_string(), 200), ("b3".to_string(), 300)]
    );
    // Key filters are pushed into the base read and still merge the update.
    assert_eq!(
        rows(&table, &filters(&[("id", "IN", "a2,a3")])).await,
        vec![("a2".to_string(), 200), ("a3".to_string(), 3)]
    );
    assert_eq!(
        rows(
            &table,
            &filters(&[("id", "=", "b3"), ("value", "=", "300")])
        )
        .await,
        vec![("b3".to_string(), 300)]
    );
}

/// With an ordering field a log record can lose to the base row. A filter on
/// the column it would have changed must then see the base value, which a
/// filter pushed below the merge would have dropped.
#[tokio::test]
async fn test_a_losing_log_record_does_not_leak_through_a_pushed_filter() {
    let dir = tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let batch = |value: i64, ts: i64| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a1"])),
                Arc::new(Int64Array::from(vec![value])),
                Arc::new(Int64Array::from(vec![ts])),
            ],
        )
        .unwrap()
    };
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_ordering_fields(["ts"])
        .with_table_type(TableTypeValue::MergeOnRead)
        .create()
        .await
        .unwrap();
    table.append([batch(5, 10)]).await.unwrap();
    table.upsert([batch(1, 5)]).await.unwrap();
    let table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    let slices = table.get_file_slices(&ReadOptions::new()).await.unwrap();
    assert!(
        slices.iter().any(|s| s.has_log_file()),
        "the upsert went to a log"
    );
    assert_eq!(rows(&table, &filters(&[("value", "=", "1")])).await, vec![]);
    assert_eq!(
        rows(&table, &filters(&[("value", "=", "5")])).await,
        vec![("a1".to_string(), 5)]
    );
}
