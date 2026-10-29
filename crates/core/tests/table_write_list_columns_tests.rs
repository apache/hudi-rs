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
//! Writes carrying list columns, the shape embedding vectors arrive in.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Int64Array, ListArray, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hudi_core::config::table::TableTypeValue;
use hudi_core::table::{ReadOptions, Table};
use tempfile::tempdir;

fn embedding_values(rows: &[(&str, f32)]) -> Float32Array {
    Float32Array::from(
        rows.iter()
            .flat_map(|(_, v)| [*v, *v + 1.0])
            .collect::<Vec<_>>(),
    )
}

/// `id`, `embedding: list<item: float32>`, `v`, with the item field named the way
/// PyArrow names it.
fn list_batch(rows: &[(&str, f32)], version: i64) -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Float32, true));
    let offsets = arrow::buffer::OffsetBuffer::from_lengths(rows.iter().map(|_| 2));
    let embedding = ListArray::new(
        item.clone(),
        offsets,
        Arc::new(embedding_values(rows)),
        None,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("embedding", DataType::List(item), true),
        Field::new("v", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(embedding) as ArrayRef,
            Arc::new(Int64Array::from(vec![version; rows.len()])),
        ],
    )
    .unwrap()
}

/// The same rows with `embedding: fixed_size_list<item: float32>[2]`.
fn fixed_batch(rows: &[(&str, f32)], version: i64) -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Float32, true));
    let embedding =
        FixedSizeListArray::new(item.clone(), 2, Arc::new(embedding_values(rows)), None);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("embedding", DataType::FixedSizeList(item, 2), true),
        Field::new("v", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(embedding) as ArrayRef,
            Arc::new(Int64Array::from(vec![version; rows.len()])),
        ],
    )
    .unwrap()
}

/// `(id, first embedding value)` for every row, sorted by id.
async fn read_embeddings(table: &Table) -> Vec<(String, f32)> {
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let embeddings = batch
            .column_by_name("embedding")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("embedding reads back as a variable-size list");
        for i in 0..batch.num_rows() {
            let values = embeddings.value(i);
            let first = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0);
            rows.push((ids.value(i).to_string(), first));
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

#[tokio::test]
async fn test_upsert_accepts_list_column_whose_item_name_differs_from_table_schema() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("chunks")
        .with_record_key_fields(["id"])
        .with_ordering_fields(["v"])
        .create()
        .await
        .unwrap();
    table
        .append([list_batch(&[("a", 1.0), ("b", 2.0)], 1)])
        .await
        .unwrap();
    // The table schema now comes from Avro and names the item `element`; the
    // incoming batch still names it `item`.
    let result = table
        .upsert([list_batch(&[("b", 20.0), ("c", 3.0)], 2)])
        .await
        .unwrap();
    assert_eq!((result.num_updates, result.num_inserts), (1, 1));
    assert_eq!(
        read_embeddings(&table).await,
        vec![("a".into(), 1.0), ("b".into(), 20.0), ("c".into(), 3.0)]
    );

    let result = table.delete("id = 'a'").await.unwrap();
    assert_eq!(result.num_deletes, 1);
    assert_eq!(
        read_embeddings(&table).await,
        vec![("b".into(), 20.0), ("c".into(), 3.0)]
    );
}

#[tokio::test]
async fn test_fixed_size_list_column_is_written_as_list() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("chunks")
        .with_record_key_fields(["id"])
        .with_ordering_fields(["v"])
        .create()
        .await
        .unwrap();
    table
        .append([fixed_batch(&[("a", 1.0), ("b", 2.0)], 1)])
        .await
        .unwrap();
    let schema = table.get_schema().await.unwrap();
    assert!(matches!(
        schema.field_with_name("embedding").unwrap().data_type(),
        DataType::List(_)
    ));
    let result = table
        .upsert([fixed_batch(&[("b", 20.0)], 2)])
        .await
        .unwrap();
    assert_eq!(result.num_updates, 1);
    assert_eq!(
        read_embeddings(&table).await,
        vec![("a".into(), 1.0), ("b".into(), 20.0)]
    );
}

async fn new_table(dir: &tempfile::TempDir, table_type: TableTypeValue) -> Table {
    Table::create(dir.path().to_str().unwrap())
        .with_table_name("chunks")
        .with_table_type(table_type)
        .with_record_key_fields(["id"])
        .with_ordering_fields(["v"])
        .create()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_update_and_multi_batch_upsert_accept_list_item_name_mismatch() {
    let dir = tempdir().unwrap();
    let mut table = new_table(&dir, TableTypeValue::CopyOnWrite).await;
    table
        .append([list_batch(&[("a", 1.0), ("b", 2.0)], 1)])
        .await
        .unwrap();
    // Two batches in one write, both named `item` against a table that says `element`.
    let result = table
        .upsert([list_batch(&[("a", 10.0)], 2), fixed_batch(&[("c", 3.0)], 2)])
        .await
        .unwrap();
    assert_eq!((result.num_updates, result.num_inserts), (1, 1));
    // SET on the list column itself through a one-row batch named `item`.
    let set = list_batch(&[("ignored", 7.0)], 3).project(&[1]).unwrap();
    let result = table.update("id = 'b'", set).await.unwrap();
    assert_eq!(result.num_updates, 1);
    assert_eq!(
        read_embeddings(&table).await,
        vec![("a".into(), 10.0), ("b".into(), 7.0), ("c".into(), 3.0)]
    );
}

#[tokio::test]
async fn test_merge_on_read_upsert_accepts_list_column() {
    let dir = tempdir().unwrap();
    let mut table = new_table(&dir, TableTypeValue::MergeOnRead).await;
    table
        .append([list_batch(&[("a", 1.0), ("b", 2.0)], 1)])
        .await
        .unwrap();
    let result = table
        .upsert([fixed_batch(&[("b", 20.0), ("c", 3.0)], 2)])
        .await
        .unwrap();
    assert_eq!((result.num_updates, result.num_inserts), (1, 1));
    assert_eq!(
        read_embeddings(&table).await,
        vec![("a".into(), 1.0), ("b".into(), 20.0), ("c".into(), 3.0)]
    );
}

#[tokio::test]
async fn test_nested_fixed_size_list_is_widened_and_struct_child_names_stay_strict() {
    use arrow::array::StructArray;
    let dir = tempdir().unwrap();
    let mut table = new_table(&dir, TableTypeValue::CopyOnWrite).await;
    // `payload: struct<vec: fixed_size_list<float32>[2]>`
    let item = Arc::new(Field::new("item", DataType::Float32, true));
    let vec_field = Arc::new(Field::new(
        "vec",
        DataType::FixedSizeList(item.clone(), 2),
        true,
    ));
    let payload = StructArray::from(vec![(
        vec_field.clone(),
        Arc::new(FixedSizeListArray::new(
            item,
            2,
            Arc::new(Float32Array::from(vec![1.0, 2.0])),
            None,
        )) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("payload", DataType::Struct(vec![vec_field].into()), true),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(payload) as ArrayRef,
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    table.append([batch]).await.unwrap();
    let stored = table.get_schema().await.unwrap();
    let DataType::Struct(children) = stored.field_with_name("payload").unwrap().data_type() else {
        panic!("payload must stay a struct");
    };
    assert!(matches!(children[0].data_type(), DataType::List(_)));

    // Same shape but a differently named struct child: not the same column.
    let other = Arc::new(Field::new(
        "other",
        DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
        true,
    ));
    let payload = StructArray::from(vec![(
        other.clone(),
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            arrow::buffer::OffsetBuffer::from_lengths([2]),
            Arc::new(Float32Array::from(vec![3.0, 4.0])),
            None,
        )) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("payload", DataType::Struct(vec![other].into()), true),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(payload) as ArrayRef,
            Arc::new(Int64Array::from(vec![2])),
        ],
    )
    .unwrap();
    let err = table.upsert([batch]).await.unwrap_err().to_string();
    assert!(err.contains("does not match"), "{err}");
}

#[tokio::test]
async fn test_stale_handle_and_overwrite_accept_list_item_name_mismatch() {
    let dir = tempdir().unwrap();
    let mut stale = new_table(&dir, TableTypeValue::CopyOnWrite).await;
    // Another handle writes first, so `stale` has not seen any commit.
    let mut fresh = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    fresh
        .append([list_batch(&[("a", 1.0), ("b", 2.0)], 1)])
        .await
        .unwrap();
    let result = stale.upsert([list_batch(&[("b", 20.0)], 2)]).await.unwrap();
    assert_eq!(result.num_updates, 1);
    stale
        .overwrite([fixed_batch(&[("c", 3.0)], 3)])
        .await
        .unwrap();
    assert_eq!(read_embeddings(&stale).await, vec![("c".into(), 3.0)]);
}

#[tokio::test]
async fn test_merge_on_read_update_sets_list_column() {
    let dir = tempdir().unwrap();
    let mut table = new_table(&dir, TableTypeValue::MergeOnRead).await;
    table
        .append([list_batch(&[("a", 1.0), ("b", 2.0)], 1)])
        .await
        .unwrap();
    let set = fixed_batch(&[("ignored", 7.0)], 2).project(&[1]).unwrap();
    let result = table.update("id = 'a'", set).await.unwrap();
    assert_eq!(result.num_updates, 1);
    assert_eq!(
        read_embeddings(&table).await,
        vec![("a".into(), 7.0), ("b".into(), 2.0)]
    );
}
