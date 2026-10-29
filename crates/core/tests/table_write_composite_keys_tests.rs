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

//! Writes to tables keyed on several record key fields, or on a non-string
//! field, with `_hoodie_record_key` encoded like Java's key generators.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hudi_core::config::table::TableTypeValue;
use hudi_core::index::{HoodieIndex, HoodieKey, SimpleIndex};
use hudi_core::table::{ReadOptions, Table};
use tempfile::tempdir;

const KEY_FIELDS: [&str; 2] = ["wr_order_number", "wr_item_sk"];

type OrderRow<'a> = (i64, &'a str, i64);

fn order_batch(rows: &[OrderRow]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("wr_order_number", DataType::Int64, true),
        Field::new("wr_item_sk", DataType::Utf8, true),
        Field::new("wr_quantity", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(order, _, _)| *order).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, item, _)| *item).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, _, qty)| *qty).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn strings(batch: &RecordBatch, name: &str) -> Vec<String> {
    let column = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|i| column.value(i).to_string())
        .collect()
}

fn longs(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let column = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..batch.num_rows()).map(|i| column.value(i)).collect()
}

/// `(_hoodie_record_key, wr_order_number, wr_item_sk, wr_quantity)`, sorted.
async fn order_rows(table: &Table) -> Vec<(String, i64, String, i64)> {
    let mut rows = Vec::new();
    for batch in table.read(&ReadOptions::new()).await.unwrap() {
        let keys = strings(&batch, "_hoodie_record_key");
        let orders = longs(&batch, "wr_order_number");
        let items = strings(&batch, "wr_item_sk");
        let quantities = longs(&batch, "wr_quantity");
        for i in 0..batch.num_rows() {
            rows.push((keys[i].clone(), orders[i], items[i].clone(), quantities[i]));
        }
    }
    rows.sort();
    rows
}

fn order_row(order: i64, item: &str, quantity: i64) -> (String, i64, String, i64) {
    (
        format!("wr_order_number:{order},wr_item_sk:{item}"),
        order,
        item.to_string(),
        quantity,
    )
}

fn key(record_key: &str) -> HoodieKey {
    HoodieKey {
        record_key: record_key.to_string(),
        partition_path: String::new(),
    }
}

/// Append, upsert, delete by key, delete by filter, and the SET guard on a
/// table keyed on `(wr_order_number BIGINT, wr_item_sk STRING)`.
async fn run_composite_key_lifecycle(table_type: TableTypeValue, record_index: bool) {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("web_returns")
        .with_table_type(table_type)
        .with_record_key_fields(KEY_FIELDS)
        .with_record_index(record_index)
        .create()
        .await
        .unwrap();
    let props = std::fs::read_to_string(dir.path().join(".hoodie/hoodie.properties")).unwrap();
    assert!(props.contains("hoodie.table.recordkey.fields=wr_order_number,wr_item_sk"));
    assert!(props.contains("hoodie.table.keygenerator.type=NON_PARTITION"));

    table
        .append([order_batch(&[(1, "a", 10), (1, "b", 20), (2, "a", 30)])])
        .await
        .unwrap();
    assert_eq!(
        order_rows(&table).await,
        vec![
            order_row(1, "a", 10),
            order_row(1, "b", 20),
            order_row(2, "a", 30)
        ]
    );

    let upsert = table
        .upsert([order_batch(&[(1, "b", 21), (3, "c", 40), (4, "a", 50)])])
        .await
        .unwrap();
    assert_eq!((upsert.num_updates, upsert.num_inserts), (1, 2));
    assert_eq!(
        order_rows(&table).await,
        vec![
            order_row(1, "a", 10),
            order_row(1, "b", 21),
            order_row(2, "a", 30),
            order_row(3, "c", 40),
            order_row(4, "a", 50),
        ]
    );

    let deleted = table
        .delete_keys([key("wr_order_number:2,wr_item_sk:a")])
        .await
        .unwrap();
    assert_eq!(deleted.num_deletes, 1);

    let deleted = table
        .delete("_hoodie_record_key = 'wr_order_number:3,wr_item_sk:c'")
        .await
        .unwrap();
    assert_eq!(deleted.num_deletes, 1);
    let remaining = vec![
        order_row(1, "a", 10),
        order_row(1, "b", 21),
        order_row(4, "a", 50),
    ];
    assert_eq!(order_rows(&table).await, remaining);

    // Write filters split an IN list on every ',', which each composite key holds.
    let in_list = "_hoodie_record_key IN ('wr_order_number:1,wr_item_sk:a', \
                   'wr_order_number:4,wr_item_sk:a')";
    let set = order_batch(&[(1, "a", 0)]);
    let errors = [
        table.delete(in_list).await.unwrap_err(),
        table.update(in_list, set).await.unwrap_err(),
    ];
    for err in errors {
        assert!(
            err.to_string()
                .contains("cannot hold composite record keys"),
            "{err}"
        );
    }
    assert_eq!(order_rows(&table).await, remaining);

    // One key field is not a whole key: every row with that item goes.
    let deleted = table.delete("wr_item_sk = 'a'").await.unwrap();
    assert_eq!(deleted.num_deletes, 2);
    assert_eq!(order_rows(&table).await, vec![order_row(1, "b", 21)]);

    // The guard runs before the SET type check, so one Int64 column probes both fields.
    for field in KEY_FIELDS {
        let set =
            RecordBatch::try_from_iter([(field, Arc::new(Int64Array::from(vec![9])) as ArrayRef)])
                .unwrap();
        let err = table.update("wr_quantity = 21", set).await.unwrap_err();
        assert!(
            err.to_string().contains(&format!(
                "cannot SET record-key or partition column '{field}'"
            )),
            "{err}"
        );
    }
}

#[tokio::test]
async fn test_cow_composite_key_lifecycle_with_record_index() {
    run_composite_key_lifecycle(TableTypeValue::CopyOnWrite, true).await;
}

#[tokio::test]
async fn test_cow_composite_key_lifecycle_with_simple_index() {
    run_composite_key_lifecycle(TableTypeValue::CopyOnWrite, false).await;
}

#[tokio::test]
async fn test_mor_composite_key_lifecycle_with_record_index() {
    run_composite_key_lifecycle(TableTypeValue::MergeOnRead, true).await;
}

#[tokio::test]
async fn test_mor_composite_key_lifecycle_with_simple_index() {
    run_composite_key_lifecycle(TableTypeValue::MergeOnRead, false).await;
}

/// Without meta fields, SimpleIndex derives the composite keys from the data columns.
#[tokio::test]
async fn test_simple_index_tags_virtual_composite_keys() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("web_returns")
        .with_record_key_fields(KEY_FIELDS)
        .with_populates_meta_fields(false)
        .with_record_index(false)
        .create()
        .await
        .unwrap();
    let append = table
        .append([order_batch(&[(1, "a", 10), (1, "b", 20)])])
        .await
        .unwrap();
    let stored = key("wr_order_number:1,wr_item_sk:b");
    let absent = key("wr_order_number:2,wr_item_sk:b");
    let locations = SimpleIndex
        .tag_location(&table, &[stored.clone(), absent.clone()])
        .await
        .unwrap();
    let location = locations[&stored].as_ref().expect("stored key is tagged");
    assert!(append.base_file_path.contains(&location.file_id));
    assert!(locations[&absent].is_none());
}

#[tokio::test]
async fn test_composite_key_partitioned_table_uses_complex_key_generator() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("web_returns")
        .with_record_key_fields(KEY_FIELDS)
        .with_partition_fields(["wr_item_sk"])
        .create()
        .await
        .unwrap();
    let props = std::fs::read_to_string(dir.path().join(".hoodie/hoodie.properties")).unwrap();
    assert!(props.contains("hoodie.table.keygenerator.type=COMPLEX"));

    table.append([order_batch(&[(1, "a", 10)])]).await.unwrap();
    let upsert = table
        .upsert([order_batch(&[(1, "a", 11), (2, "a", 20)])])
        .await
        .unwrap();
    assert_eq!((upsert.num_updates, upsert.num_inserts), (1, 1));
    assert_eq!(
        order_rows(&table).await,
        vec![order_row(1, "a", 11), order_row(2, "a", 20)]
    );
}

#[tokio::test]
async fn test_single_int64_record_key_lifecycle() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("web_returns")
        .with_record_key_fields(["wr_order_number"])
        .create()
        .await
        .unwrap();
    table
        .append([order_batch(&[(1, "a", 10), (2, "a", 20)])])
        .await
        .unwrap();
    let upsert = table
        .upsert([order_batch(&[(2, "a", 21), (3, "a", 30)])])
        .await
        .unwrap();
    assert_eq!((upsert.num_updates, upsert.num_inserts), (1, 1));
    let key_and_quantity = |rows: Vec<(String, i64, String, i64)>| -> Vec<(String, i64)> {
        rows.into_iter()
            .map(|(key, _, _, qty)| (key, qty))
            .collect()
    };
    assert_eq!(
        key_and_quantity(order_rows(&table).await),
        [
            ("1".to_string(), 10),
            ("2".to_string(), 21),
            ("3".to_string(), 30)
        ]
    );

    assert_eq!(table.delete_keys([key("1")]).await.unwrap().num_deletes, 1);
    // `02` is not the stored key text, but it still names row 2.
    for filter in ["wr_order_number = 3", "wr_order_number = 02"] {
        assert_eq!(
            table.delete(filter).await.unwrap().num_deletes,
            1,
            "{filter}"
        );
    }
    assert!(order_rows(&table).await.is_empty());
}
