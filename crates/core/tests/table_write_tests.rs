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

use arrow::array::{Array, Int64Array, StringArray};
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
    let err = upsertable
        .append_only([ordered_batch(vec![("a", 1, 1)])])
        .await;
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
async fn test_append_writes_column_stats_and_partition_stats_mdt() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    table.append([sample_batch()]).await.unwrap();

    let col_stats_dir = dir.path().join(".hoodie/metadata/column_stats");
    let part_stats_dir = dir.path().join(".hoodie/metadata/partition_stats");
    assert!(col_stats_dir.is_dir());
    // Java disables partition_stats for non-partitioned tables.
    assert!(!part_stats_dir.exists());

    let col_bases: Vec<_> = std::fs::read_dir(&col_stats_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with(".hfile") && n.starts_with("col-stats-"))
        .collect();
    assert_eq!(
        col_bases.len(),
        2,
        "Java default column_stats FG count is 2"
    );

    let col_logs: Vec<_> = std::fs::read_dir(&col_stats_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.contains(".log."))
        .collect();
    assert!(
        !col_logs.is_empty(),
        "append should write column_stats HFile log blocks: {col_logs:?}"
    );
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
        .dynamic_partition_overwrite([partitioned_batch(vec![("x", "sf", 10), ("y", "sf", 11)])])
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
    // `c` was a packed insert (small-file routing) living only in log files,
    // so read-optimized mode cannot see it until compaction — Java parity.
    assert_eq!(
        rows_by_id(&read_optimized),
        vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
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
async fn test_upsert_rejects_custom_merge_mode() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let mut table = Table::create(base_uri)
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_option("hoodie.record.merge.mode", "CUSTOM")
        .create()
        .await
        .unwrap();

    let error = table.upsert([batch(vec![("a", 1)])]).await.unwrap_err();
    assert!(matches!(error, CoreError::Unsupported(_)));
    assert!(error.to_string().contains("CUSTOM"), "{error}");
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
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
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
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
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
    table
        .append([batch(vec![("a", 1), ("b", 2), ("c", 2)])])
        .await
        .unwrap();

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
    table
        .append([batch(vec![("a", 1), ("b", 2)])])
        .await
        .unwrap();

    let deleted = table.delete("id = 'b'").await.unwrap();
    assert_eq!(deleted.num_deletes, 1);
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![("a".to_string(), 1)]
    );

    // Meta record-key predicate also routes through keyed delete.
    let deleted = table.delete("_hoodie_record_key = 'a'").await.unwrap();
    assert_eq!(deleted.num_deletes, 1);
    // Delete-all keeps an empty base slice under the same file group (commit, not
    // replacecommit), so read may return empty batches — assert on row content.
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        Vec::<(String, i64)>::new()
    );
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
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let error = table.update("id = 'a'", multi).await.unwrap_err();
    assert!(error.to_string().contains("single-row"), "{error}");
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

    let result = table.update("id = 'a'", set_value(10)).await.unwrap();
    assert!(result.num_updates >= 1);
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![("a".to_string(), 10), ("b".to_string(), 2)]
    );
}

#[tokio::test]
async fn test_partitioned_cow_filter_delete_by_record_key() {
    // P0-5: empty partition_path from filter must still delete via key-only match.
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .create()
        .await
        .unwrap();
    table
        .append([city_batch(vec![("a", "sf", 1), ("b", "nyc", 2)])])
        .await
        .unwrap();

    let deleted = table.delete("id = 'b'").await.unwrap();
    assert_eq!(deleted.num_deletes, 1);
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![("a".to_string(), 1)]
    );
}

#[tokio::test]
async fn test_append_rejects_schema_mismatch() {
    // P0-8: divergent append schema must error, not silently rewrite commit schema.
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    table.append([batch(vec![("a", 1)])]).await.unwrap();

    let bad_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("other", DataType::Int64, false),
    ]));
    let bad = RecordBatch::try_new(
        bad_schema,
        vec![
            Arc::new(StringArray::from(vec!["b"])),
            Arc::new(Int64Array::from(vec![2])),
        ],
    )
    .unwrap();
    let err = table.append([bad]).await.unwrap_err();
    assert!(
        err.to_string().contains("does not match table schema"),
        "{err}"
    );
}

#[tokio::test]
async fn test_default_create_uses_commit_time_merge_mode() {
    // Payload classes are deprecated; merge mode is the source of truth.
    let dir = tempdir().unwrap();
    let _table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    let props = std::fs::read_to_string(dir.path().join(".hoodie/hoodie.properties")).unwrap();
    assert!(props.contains("hoodie.record.merge.mode=COMMIT_TIME_ORDERING"));
    assert!(!props.contains("hoodie.compaction.payload.class="));
    assert!(props.contains("hoodie.table.timeline.timezone=LOCAL"));
}

#[tokio::test]
async fn test_auto_keys_unique_across_size_split_chunks() {
    // P0-4: auto keys must not restart per size-split chunk within a commit.
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_option("hoodie.parquet.max.file.size", "1")
        .create()
        .await
        .unwrap();
    // No record key fields → auto keys. Force tiny files so one append splits.
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let data =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))]).unwrap();
    table.append([data]).await.unwrap();
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let mut seen = std::collections::HashSet::new();
    for rows in &batches {
        let key_col = rows.column_by_name("_hoodie_record_key").expect("meta key");
        let keys = key_col.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..rows.num_rows() {
            assert!(
                seen.insert(keys.value(i).to_string()),
                "duplicate auto key {}",
                keys.value(i)
            );
        }
    }
    assert_eq!(seen.len(), 4);
}

#[tokio::test]
async fn test_cow_delete_then_rli_lookup_misses_deleted_key() {
    // P0-1: RLI deletes must not poison subsequent index lookups.
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    table
        .append([batch(vec![("a", 1), ("b", 2)])])
        .await
        .unwrap();
    table
        .delete_keys([HoodieKey {
            record_key: "b".to_string(),
            partition_path: String::new(),
        }])
        .await
        .unwrap();

    let tagged = hudi_core::index::for_table(&table)
        .tag_location(
            &table,
            &[
                HoodieKey {
                    record_key: "a".to_string(),
                    partition_path: String::new(),
                },
                HoodieKey {
                    record_key: "b".to_string(),
                    partition_path: String::new(),
                },
            ],
        )
        .await
        .unwrap();
    assert!(
        tagged
            .get(&HoodieKey {
                record_key: "a".to_string(),
                partition_path: String::new(),
            })
            .unwrap()
            .is_some()
    );
    assert!(
        tagged
            .get(&HoodieKey {
                record_key: "b".to_string(),
                partition_path: String::new(),
            })
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_mor_delete_then_upsert_same_key_visible() {
    // P0-7: delete orderingVal must not permanently shadow a later upsert.
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
        .append([ordered_batch(vec![("a", 1, 10)])])
        .await
        .unwrap();
    table.delete("id = 'a'").await.unwrap();
    // The re-insert's ordering value must beat the deleted row's (20 > 10):
    // under event-time ordering a late-arriving upsert loses to the base
    // record even across a delete (the tombstone is consumed in the log
    // buffer and the base wins the final merge — Java
    // `BufferedRecordMergerFactory` semantics).
    table
        .upsert([ordered_batch(vec![("a", 2, 20)])])
        .await
        .unwrap();
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![("a".to_string(), 2)]
    );
}

#[tokio::test]
async fn test_mor_overwrite_replaces_file_groups() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_table_type(TableTypeValue::MergeOnRead)
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    table
        .upsert([batch(vec![("a", 1), ("b", 2)])])
        .await
        .unwrap();
    // Log files exist for the group after an update.
    table.upsert([batch(vec![("a", 10)])]).await.unwrap();

    let result = table.overwrite([batch(vec![("z", 99)])]).await.unwrap();
    assert_eq!(result.num_writes, 1);

    // Replacecommit (not deltacommit) with the plan in requested.
    let timeline = dir.path().join(".hoodie/timeline");
    let names: Vec<String> = std::fs::read_dir(&timeline)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        names
            .iter()
            .any(|n| n.ends_with(".replacecommit") && n.contains('_')),
        "MOR overwrite must complete a replacecommit: {names:?}"
    );
    assert!(
        names
            .iter()
            .any(|n| n.ends_with(".replacecommit.requested")),
        "replacecommit plan must be requested: {names:?}"
    );

    // Old file groups (base + logs) replaced: only the new row reads back.
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![("z".to_string(), 99)]
    );
}

#[tokio::test]
async fn test_mor_dynamic_partition_overwrite() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_table_type(TableTypeValue::MergeOnRead)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .create()
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let city_batch = |rows: Vec<(&str, &str, i64)>| {
        RecordBatch::try_new(
            schema.clone(),
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
    };
    table
        .upsert([city_batch(vec![("a", "sf", 1), ("c", "nyc", 7)])])
        .await
        .unwrap();
    table
        .dynamic_partition_overwrite([city_batch(vec![("d", "sf", 5)])])
        .await
        .unwrap();

    // Touched partition replaced; untouched partition intact.
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let mut ids: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let ids = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..b.num_rows())
                .map(|i| ids.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["c".to_string(), "d".to_string()]);
}

#[tokio::test]
async fn test_cow_delete_emptying_one_group_writes_empty_base() {
    // Regression: a delete that empties one file group while another affected
    // group keeps rows must still write an empty base for the emptied group.
    // Without it the old base stays the group's latest slice and the deleted
    // row resurfaces on a fresh table open.
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_partition_fields(["city"])
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    // Two file groups: sf holds only "a"; nyc holds "b1" and "b2".
    table
        .append([partitioned_batch(vec![
            ("a", "sf", 1),
            ("b1", "nyc", 2),
            ("b2", "nyc", 3),
        ])])
        .await
        .unwrap();
    // One delete touching both groups: empties sf, leaves b2 in nyc.
    let result = table.delete("id IN ('a', 'b1')").await.unwrap();
    assert_eq!(result.num_deletes, 2);

    // Fresh open (cold file-system view) must not resurrect "a".
    let table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let mut ids: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let col = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..b.num_rows())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["b2".to_string()]);

    // The emptied sf group must have a second (empty) base file version.
    let sf_files: Vec<String> = std::fs::read_dir(dir.path().join("city=sf"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with(".parquet"))
        .collect();
    assert_eq!(
        sf_files.len(),
        2,
        "sf group should have the original base plus an empty rewrite, saw {sf_files:?}"
    );
}

/// P1-2: an expression delete must rewrite ONLY the file groups holding
/// matching rows — file groups in other partitions keep their base files
/// byte-identical (no whole-table rewrite).
#[tokio::test]
async fn test_expression_delete_rewrites_only_affected_file_groups() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .create()
        .await
        .unwrap();
    table
        .append([partitioned_batch(vec![("a", "sf", 1), ("b", "sf", 2)])])
        .await
        .unwrap();
    table
        .append([partitioned_batch(vec![("c", "nyc", 3), ("d", "nyc", 4)])])
        .await
        .unwrap();

    let parquets = |part: &str| -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir.path().join(format!("city={part}")))
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".parquet"))
            .collect();
        names.sort();
        names
    };
    let nyc_before = parquets("nyc");

    // Non-key filter → scan path; matches only the sf partition's group.
    let result = table.delete("value = 2").await.unwrap();
    assert_eq!(result.num_deletes, 1);

    assert_eq!(
        parquets("nyc"),
        nyc_before,
        "file groups without matching rows must not be rewritten"
    );
    assert_eq!(
        parquets("sf").len(),
        2,
        "the affected sf group gains one rewritten base file"
    );
    let rows = table.read(&ReadOptions::new()).await.unwrap();
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
}

/// Data-column filters on an MDT-stats-enabled table go through MDT
/// column_stats pruning (no parquet footer reads) and must return exactly the
/// matching rows — including from files whose ranges straddle the predicate.
#[tokio::test]
async fn test_read_filter_prunes_via_mdt_column_stats() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .create()
        .await
        .unwrap();
    // Disjoint value ranges per file group: sf in [1,2], nyc in [100,200].
    table
        .append([partitioned_batch(vec![("a", "sf", 1), ("b", "sf", 2)])])
        .await
        .unwrap();
    table
        .append([partitioned_batch(vec![
            ("c", "nyc", 100),
            ("d", "nyc", 200),
        ])])
        .await
        .unwrap();
    assert!(
        table
            .get_metadata_table_partitions()
            .contains(&"column_stats".to_string()),
        "column_stats must be enabled for this test to exercise MDT pruning"
    );

    // Filter matched only by the nyc group; the sf group prunes on its range.
    let options = ReadOptions::new()
        .with_filters([("value", ">", "50")])
        .unwrap();
    let rows = rows_by_id(&table.read(&options).await.unwrap());
    assert_eq!(
        rows,
        vec![("c".to_string(), 100), ("d".to_string(), 200)],
        "filtered read must return exactly the matching rows"
    );

    // A filter matching nothing anywhere prunes every group.
    let options = ReadOptions::new()
        .with_filters([("value", ">", "1000")])
        .unwrap();
    let rows = rows_by_id(&table.read(&options).await.unwrap());
    assert!(rows.is_empty());
}

/// Merged files must carry their own name in `_hoodie_file_name`, and an
/// UPDATE must re-stamp `_hoodie_commit_time` ONLY on the rows it changed —
/// untouched rows keep their original commit lineage (incremental queries
/// depend on it).
#[tokio::test]
async fn test_merge_stamps_file_name_and_only_matched_commit_times() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    table
        .append([batch(vec![("a", 1), ("b", 2)])])
        .await
        .unwrap();
    let first_instant = {
        let batches = table.read(&ReadOptions::new()).await.unwrap();
        let commit_times = batches[0]
            .column_by_name("_hoodie_commit_time")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        commit_times.value(0).to_string()
    };

    // UPDATE one row: commit time changes for it alone; file name is the new
    // file for every row of the rewritten group.
    let set = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![99]))],
    )
    .unwrap();
    table.update("id = 'a'", set).await.unwrap();

    let batches = table.read(&ReadOptions::new()).await.unwrap();
    for b in &batches {
        let ids = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let commit_times = b
            .column_by_name("_hoodie_commit_time")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let file_names = b
            .column_by_name("_hoodie_file_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..b.num_rows() {
            let file_name = file_names.value(row);
            assert!(
                dir.path().join(file_name).is_file(),
                "_hoodie_file_name '{file_name}' must be a real file"
            );
            assert_ne!(file_name, "pending");
            if ids.value(row) == "a" {
                assert_ne!(
                    commit_times.value(row),
                    first_instant,
                    "updated row must carry the updating commit"
                );
            } else {
                assert_eq!(
                    commit_times.value(row),
                    first_instant,
                    "untouched row must keep its original commit time"
                );
            }
        }
    }
}
