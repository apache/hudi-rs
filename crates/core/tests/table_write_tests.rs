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
use hudi_core::config::table::TableTypeValue;
use hudi_core::error::CoreError;
use hudi_core::index::{HoodieIndex, HoodieKey, SimpleIndex};
use hudi_core::table::partition::PartitionPruner;
use hudi_core::table::{QueryType, ReadOptions, Table, UpsertOptions};
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

fn ordered_batch(rows: Vec<(&str, i64, i64)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("event_time", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, value, _)| *value).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, event_time)| *event_time)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn partitioned_batch(rows: Vec<(&str, &str, i64)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, city, _)| *city).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, _, value)| *value).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn rows_by_id(batches: &[RecordBatch]) -> Vec<(String, i64)> {
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|index| (ids.value(index).to_string(), values.value(index)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
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
    let timeline = dir.path().join(".hoodie/timeline");
    assert!(
        timeline
            .join(format!("{}.commit.requested", result.instant))
            .is_file(),
        "data timeline should fence with .commit.requested"
    );
    assert!(
        timeline
            .join(format!("{}.inflight", result.instant))
            .is_file(),
        "data timeline should fence with .inflight"
    );

    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_append_only_requires_append_only_merge_mode() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_populates_meta_fields(false)
        .create()
        .await
        .unwrap();
    table.append_only([sample_batch()]).await.unwrap();

    let dir = tempdir().unwrap();
    let mut upsertable = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_ordering_fields(["event_time"])
        .create()
        .await
        .unwrap();
    let err = upsertable.append_only([ordered_batch(vec![("a", 1, 1)])]).await;
    assert!(matches!(err, Err(CoreError::Unsupported(_))));
}

#[tokio::test]
async fn test_mdt_failure_before_commit_leaves_no_completed_data_instant() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let mut table = Table::create(root.to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_metadata(true)
        .create()
        .await
        .unwrap();

    // Corrupt MDT so the post-data MDT update fails before the data commit is finalized.
    let mdt = root.join(".hoodie/metadata");
    std::fs::remove_dir_all(&mdt).unwrap();
    std::fs::write(&mdt, b"not-a-directory").unwrap();

    let err = table.append([sample_batch()]).await.unwrap_err();
    assert!(
        matches!(err, hudi_core::error::CoreError::Storage(_))
            || err.to_string().to_lowercase().contains("metadata")
            || err.to_string().to_lowercase().contains("not a directory")
            || err.to_string().to_lowercase().contains("file"),
        "expected MDT write failure, got {err}"
    );

    let timeline = root.join(".hoodie/timeline");
    let completed: Vec<_> = std::fs::read_dir(&timeline)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.contains(".commit") && !n.contains(".requested") && !n.contains(".inflight"))
        .collect();
    assert!(
        completed.is_empty(),
        "failed MDT update must not leave a completed data commit: {completed:?}"
    );
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
    let deleted = table.delete("id = 'a'").await.unwrap();
    assert_eq!(deleted.num_deletes, 1);
    let overwritten = table.overwrite([batch(vec![("z", 99)])]).await.unwrap();
    assert_eq!(overwritten.num_writes, 1);
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[tokio::test]
async fn test_overwrite_replaces_all_partitions() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .create()
        .await
        .unwrap();
    table
        .append([partitioned_batch(vec![
            ("a", "sf", 1),
            ("b", "nyc", 2),
            ("c", "la", 3),
        ])])
        .await
        .unwrap();

    let overwritten = table
        .overwrite([partitioned_batch(vec![("z", "chi", 99), ("y", "bos", 98)])])
        .await
        .unwrap();
    assert_eq!(overwritten.num_writes, 2);
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![("y".to_string(), 98), ("z".to_string(), 99)]
    );
}

#[tokio::test]
async fn test_dynamic_partition_overwrite_replaces_only_touched_partitions() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .create()
        .await
        .unwrap();
    table
        .append([partitioned_batch(vec![
            ("a", "sf", 1),
            ("b", "sf", 2),
            ("c", "nyc", 3),
            ("d", "la", 4),
        ])])
        .await
        .unwrap();

    let result = table
        .dynamic_partition_overwrite([partitioned_batch(vec![
            ("x", "sf", 10),
            ("y", "sf", 11),
        ])])
        .await
        .unwrap();
    assert_eq!(result.num_writes, 2);
    let rows = rows_by_id(&table.read(&ReadOptions::new()).await.unwrap());
    assert_eq!(
        rows,
        vec![
            ("c".to_string(), 3),
            ("d".to_string(), 4),
            ("x".to_string(), 10),
            ("y".to_string(), 11),
        ]
    );
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

#[tokio::test]
async fn test_upsert_uses_event_time_ordering() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let mut table = Table::create(base_uri)
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_ordering_fields(["event_time"])
        .with_populates_meta_fields(true)
        .create()
        .await
        .unwrap();

    table
        .upsert([ordered_batch(vec![("a", 1, 100)])])
        .await
        .unwrap();
    table
        .upsert([ordered_batch(vec![("a", 2, 50)])])
        .await
        .unwrap();

    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let values = batches[0]
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 1);

    table
        .upsert([ordered_batch(vec![("a", 3, 100)])])
        .await
        .unwrap();
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let values = batches[0]
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 3);

    table
        .upsert([ordered_batch(vec![("a", 4, 200)])])
        .await
        .unwrap();
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let values = batches[0]
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 4);
}

#[tokio::test]
async fn test_merge_on_read_writes_parquet_logs_and_merges_snapshot() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let mut table = Table::create(base_uri)
        .with_table_name("trips")
        .with_table_type(TableTypeValue::MergeOnRead)
        .with_record_key_fields(["id"])
        .with_ordering_fields(["event_time"])
        .with_populates_meta_fields(true)
        .create()
        .await
        .unwrap();

    let append = table
        .append([ordered_batch(vec![("a", 1, 1), ("b", 2, 1)])])
        .await
        .unwrap();
    assert!(append.commit_relative_path.ends_with(".deltacommit"));
    assert!(dir.path().join(&append.base_file_path).is_file());
    let incremental = table
        .read(
            &ReadOptions::new()
                .with_query_type(QueryType::Incremental)
                .with_start_timestamp("00000000000000000"),
        )
        .await
        .unwrap();
    assert_eq!(
        rows_by_id(&incremental),
        vec![("a".to_string(), 1), ("b".to_string(), 2)]
    );
    table
        .append([ordered_batch(vec![("d", 4, 1)])])
        .await
        .unwrap();

    let first_upsert = table
        .upsert([ordered_batch(vec![("a", 10, 2), ("c", 3, 1), ("d", 40, 2)])])
        .await
        .unwrap();
    assert_eq!(first_upsert.num_updates, 2);
    assert_eq!(first_upsert.num_inserts, 1);
    let second_upsert = table
        .upsert([ordered_batch(vec![("a", 20, 3)])])
        .await
        .unwrap();
    assert_eq!(second_upsert.num_updates, 1);

    let snapshot = table.read(&ReadOptions::new()).await.unwrap();
    assert_eq!(
        rows_by_id(&snapshot),
        vec![
            ("a".to_string(), 20),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
            ("d".to_string(), 40),
        ]
    );

    let read_optimized = table
        .read(&ReadOptions::new().with_hudi_option("hoodie.read.use.read_optimized.mode", "true"))
        .await
        .unwrap();
    assert_eq!(
        rows_by_id(&read_optimized),
        vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
            ("d".to_string(), 4),
        ]
    );

    let deleted = table
        .delete_keys([HoodieKey {
            record_key: "a".to_string(),
            partition_path: String::new(),
        }])
        .await
        .unwrap();
    assert_eq!(deleted.num_deletes, 1);
    let snapshot = table.read(&ReadOptions::new()).await.unwrap();
    assert_eq!(
        rows_by_id(&snapshot),
        vec![
            ("b".to_string(), 2),
            ("c".to_string(), 3),
            ("d".to_string(), 40),
        ]
    );
}

#[tokio::test]
async fn test_upsert_rejects_custom_payload_or_merger() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let mut table = Table::create(base_uri)
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_option(
            "hoodie.datasource.write.payload.class",
            "example.CustomPayload",
        )
        .create()
        .await
        .unwrap();

    let error = table.upsert([batch(vec![("a", 1)])]).await.unwrap_err();
    assert!(matches!(error, CoreError::Unsupported(_)));
    assert!(
        error
            .to_string()
            .contains("hoodie.datasource.write.payload.class")
    );
}

#[tokio::test]
async fn test_write_operations_reject_empty_inputs() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();

    for error in [
        table.upsert(Vec::<RecordBatch>::new()).await.unwrap_err(),
        table
            .overwrite(Vec::<RecordBatch>::new())
            .await
            .unwrap_err(),
        table
            .delete_keys(Vec::<HoodieKey>::new())
            .await
            .unwrap_err(),
    ] {
        assert!(matches!(error, CoreError::Write(_)));
        assert!(error.to_string().contains("at least one"));
    }
}

#[tokio::test]
async fn test_upsert_deduplicates_keys_and_delete_missing_key_is_noop() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();

    table.upsert([batch(vec![("a", 1)])]).await.unwrap();
    let result = table
        .upsert([batch(vec![("a", 2), ("a", 3), ("b", 4), ("b", 5)])])
        .await
        .unwrap();
    assert_eq!(result.num_updates, 1);
    assert_eq!(result.num_inserts, 1);
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![("a".to_string(), 3), ("b".to_string(), 5)]
    );

    let result = table
        .delete_keys([HoodieKey {
            record_key: "missing".to_string(),
            partition_path: String::new(),
        }])
        .await
        .unwrap();
    assert_eq!(result.num_deletes, 0);
    assert!(result.instant.is_empty());
}

#[tokio::test]
async fn test_append_multiple_commits_then_upsert() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();

    table.append([batch(vec![("a", 1)])]).await.unwrap();
    table.append([batch(vec![("b", 2)])]).await.unwrap();
    let result = table
        .upsert([batch(vec![("b", 20), ("c", 3)])])
        .await
        .unwrap();
    assert_eq!(result.num_updates, 1);
    assert_eq!(result.num_inserts, 1);
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![
            ("a".to_string(), 1),
            ("b".to_string(), 20),
            ("c".to_string(), 3),
        ]
    );
}

#[tokio::test]
async fn test_create_requires_table_name_and_upsert_requires_record_key() {
    let dir = tempdir().unwrap();
    let error = Table::create(dir.path().to_str().unwrap())
        .create()
        .await
        .unwrap_err();
    assert!(matches!(error, CoreError::Write(_)));
    assert!(error.to_string().contains("Table name is required"));

    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .create()
        .await
        .unwrap();
    let error = table.upsert([batch(vec![("a", 1)])]).await.unwrap_err();
    assert!(
        error.to_string().contains("record key")
            || error.to_string().contains("recordkey.fields")
            || error.to_string().contains("auto-generated keys"),
        "{error}"
    );
}

#[tokio::test]
async fn test_simple_index_tags_existing_and_missing_keys() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    table.append([batch(vec![("a", 1)])]).await.unwrap();

    let keys = vec![
        HoodieKey {
            record_key: "a".to_string(),
            partition_path: String::new(),
        },
        HoodieKey {
            record_key: "missing".to_string(),
            partition_path: String::new(),
        },
    ];
    let locations = SimpleIndex.tag_location(&table, &keys).await.unwrap();
    assert!(locations[&keys[0]].is_some());
    assert!(locations[&keys[1]].is_none());
}

#[tokio::test]
async fn test_upsert_rejects_schema_mismatch() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    table.upsert([batch(vec![("a", 1)])]).await.unwrap();

    let mismatched = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["a"]))],
    )
    .unwrap();
    let error = table.upsert([mismatched]).await.unwrap_err();
    assert!(matches!(error, CoreError::Schema(_)));
}

fn set_value(value: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap()
}

fn city_batch(rows: Vec<(&str, &str, i64)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, city, _)| *city).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, _, value)| *value).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn set_fare(value: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap()
}

#[tokio::test]
async fn test_delete_by_non_key_column_scan() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    table.append([batch(vec![("a", 1), ("b", 2), ("c", 2)])]).await.unwrap();

    let deleted = table.delete("value = 2").await.unwrap();
    assert_eq!(deleted.num_deletes, 2);
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![("a".to_string(), 1)]
    );
}

#[tokio::test]
async fn test_delete_by_record_key_equality_routes_keyed_path() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    table.append([batch(vec![("a", 1), ("b", 2)])]).await.unwrap();

    let deleted = table.delete("id = 'b'").await.unwrap();
    assert_eq!(deleted.num_deletes, 1);
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![("a".to_string(), 1)]
    );

    // Meta record-key predicate also routes through keyed delete.
    let deleted = table.delete("_hoodie_record_key = 'a'").await.unwrap();
    assert_eq!(deleted.num_deletes, 1);
    assert!(table.read(&ReadOptions::new()).await.unwrap().is_empty());
}

#[tokio::test]
async fn test_update_by_non_key_filter() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .create()
        .await
        .unwrap();
    table
        .append([city_batch(vec![
            ("a", "sf", 1),
            ("b", "nyc", 2),
            ("c", "sf", 3),
        ])])
        .await
        .unwrap();

    let result = table.update("city = 'sf'", set_fare(99)).await.unwrap();
    assert_eq!(result.num_updates, 2);
    let rows = rows_by_id(&table.read(&ReadOptions::new()).await.unwrap());
    assert_eq!(
        rows,
        vec![
            ("a".to_string(), 99),
            ("b".to_string(), 2),
            ("c".to_string(), 99),
        ]
    );
}

#[tokio::test]
async fn test_update_zero_matches_is_noop() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    table.append([batch(vec![("a", 1)])]).await.unwrap();

    let result = table.update("value = 999", set_value(0)).await.unwrap();
    assert_eq!(result, hudi_core::WriteResult::default());
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![("a".to_string(), 1)]
    );
}

#[tokio::test]
async fn test_update_rejects_multi_row_set_batch() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    table.append([batch(vec![("a", 1)])]).await.unwrap();

    let multi = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let error = table.update("id = 'a'", multi).await.unwrap_err();
    assert!(
        error.to_string().contains("single-row"),
        "{error}"
    );
}

#[tokio::test]
async fn test_mor_delete_by_non_key_column() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_table_type(TableTypeValue::MergeOnRead)
        .with_record_key_fields(["id"])
        .with_ordering_fields(["event_time"])
        .with_populates_meta_fields(true)
        .create()
        .await
        .unwrap();
    table
        .append([ordered_batch(vec![("a", 1, 1), ("b", 2, 1), ("c", 3, 1)])])
        .await
        .unwrap();

    let deleted = table.delete("value = 2").await.unwrap();
    assert_eq!(deleted.num_deletes, 1);
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![("a".to_string(), 1), ("c".to_string(), 3)]
    );
}

#[tokio::test]
async fn test_mor_update_by_filter() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_table_type(TableTypeValue::MergeOnRead)
        .with_record_key_fields(["id"])
        .with_ordering_fields(["event_time"])
        .with_populates_meta_fields(true)
        .create()
        .await
        .unwrap();
    table
        .append([ordered_batch(vec![("a", 1, 1), ("b", 2, 1)])])
        .await
        .unwrap();

    let result = table
        .update("id = 'a'", set_value(10))
        .await
        .unwrap();
    assert!(result.num_updates >= 1);
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![("a".to_string(), 10), ("b".to_string(), 2)]
    );
}
