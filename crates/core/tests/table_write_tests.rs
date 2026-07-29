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

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hudi_core::error::CoreError;
use hudi_core::index::HoodieKey;
use hudi_core::table::partition::PartitionPruner;
use hudi_core::table::{ReadOptions, Table, UpsertOptions};
use tempfile::tempdir;

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap()
}

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
async fn test_create_and_append_then_read() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();

    let mut table = Table::create(base_uri)
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();

    let result = table.append([sample_batch()]).await.unwrap();
    assert_eq!(result.num_rows, 3);
    assert!(
        dir.path().join(&result.base_file_path).is_file(),
        "base file should exist at {}",
        result.base_file_path
    );
    assert!(
        dir.path().join(&result.commit_relative_path).is_file(),
        "commit file should exist at {}",
        result.commit_relative_path
    );

    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_create_rejects_existing_table() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();

    Table::create(base_uri)
        .with_table_name("trips")
        .create()
        .await
        .unwrap();

    let err = Table::create(base_uri)
        .with_table_name("trips")
        .create()
        .await
        .unwrap_err();
    assert!(matches!(err, CoreError::Write(_)));
}

#[tokio::test]
async fn test_create_with_metadata_and_append_updates_files_partition() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let mut table = Table::create(base_uri)
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_metadata(true)
        .create()
        .await
        .unwrap();
    let result = table.append([sample_batch()]).await.unwrap();
    assert!(table.is_metadata_table_enabled());

    let reopened = Table::new(base_uri).await.unwrap();
    let partition_schema = reopened.get_partition_schema().await.unwrap();
    let pruner =
        PartitionPruner::new(&[], &partition_schema, reopened.hudi_configs.as_ref()).unwrap();
    let records = reopened
        .read_metadata_table_files_partition(&pruner)
        .await
        .unwrap();
    let files = records.get("").unwrap();
    assert!(files.has_active_file(&result.base_file_path));
}

#[tokio::test]
async fn test_upsert_delete_and_overwrite() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let mut table = Table::create(base_uri)
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();

    table
        .upsert([batch(vec![("a", 1), ("b", 2)])])
        .await
        .unwrap();
    let upsert = table
        .upsert([batch(vec![("a", 10), ("c", 3)])])
        .await
        .unwrap();
    assert_eq!(upsert.num_updates, 1);
    assert_eq!(upsert.num_inserts, 1);

    let deleted = table.delete("id = 'b'").await.unwrap();
    assert_eq!(deleted.num_deletes, 1);
    let deleted = table
        .delete_keys([HoodieKey {
            record_key: "a".to_string(),
            partition_path: String::new(),
        }])
        .await
        .unwrap();
    assert_eq!(deleted.num_deletes, 1);
    let overwritten = table.overwrite([batch(vec![("z", 99)])]).await.unwrap();
    assert_eq!(overwritten.num_writes, 1);
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[tokio::test]
async fn test_partial_upsert_preserves_unselected_columns() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let mut table = Table::create(base_uri)
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_populates_meta_fields(true)
        .create()
        .await
        .unwrap();

    table.upsert([batch(vec![("a", 1)])]).await.unwrap();
    table
        .upsert_with(
            [batch(vec![("a", 2)])],
            UpsertOptions {
                update_columns: Some(vec!["value".to_string()]),
            },
        )
        .await
        .unwrap();
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let values = batches[0]
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 2);
}
