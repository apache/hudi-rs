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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hudi_core::config::table::{HudiTableConfig, TableTypeValue};
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

/// A rejected write must leave no instant on the data timeline.
fn assert_no_instant(base: &std::path::Path) {
    let names: Vec<String> = std::fs::read_dir(base.join(".hoodie/timeline"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with('2'))
        .collect();
    assert!(names.is_empty(), "rejected write left instants: {names:?}");
}

/// hoodie.properties is written and read with the Java `Properties` escapes,
/// so names and a pretty-printed create schema survive a reopen.
#[tokio::test]
async fn test_properties_preserve_names_and_formatted_schema() {
    for (name, escaped) in [
        (r"café a=b:c\d", r"caf\u00E9 a\=b\:c\\d"),
        (" review", r"\ review"),
    ] {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let schema = r#"{
  "type": "record",
  "name": "record",
  "fields": [{"name": "id", "type": "string"}, {"name": "value", "type": "long"}]
}"#;
        Table::create(path)
            .with_table_name(name)
            .with_record_key_fields(["id"])
            .with_option(HudiTableConfig::CreateSchema, schema)
            .create()
            .await
            .unwrap();
        let reopened = Table::new(path).await.unwrap();
        let actual: String = reopened
            .hudi_configs
            .get(HudiTableConfig::TableName)
            .unwrap()
            .into();
        assert_eq!(actual, name);
        assert_eq!(
            reopened.get_schema().await.unwrap().fields().len(),
            2,
            "{name}"
        );
        let props = std::fs::read_to_string(dir.path().join(".hoodie/hoodie.properties")).unwrap();
        assert!(props.is_ascii(), "{props}");
        assert!(
            props.contains(&format!("hoodie.table.name={escaped}\n")),
            "{props}"
        );
        let metadata = std::fs::read_to_string(
            dir.path()
                .join(".hoodie/metadata/.hoodie/hoodie.properties"),
        )
        .unwrap();
        assert!(
            metadata.contains(&format!("hoodie.table.name={escaped}_metadata\n")),
            "{metadata}"
        );
    }
}

#[tokio::test]
async fn test_create_rejects_managed_option_overrides_before_persisting() {
    for (key, value) in [
        ("hoodie.table.name", "renamed"),
        ("hoodie.table.version", "6"),
        ("hoodie.metadata.enable", "false"),
    ] {
        let dir = tempdir().unwrap();
        let error = Table::create(dir.path().to_str().unwrap())
            .with_table_name("original")
            .with_record_key_fields(["id"])
            .with_option(key, value)
            .create()
            .await
            .err()
            .unwrap_or_else(|| panic!("{key} override accepted"));
        assert!(error.to_string().contains(key), "{key}: {error}");
        assert!(
            !dir.path().join(".hoodie").exists(),
            "{key} left a partial table"
        );
    }
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
async fn test_write_result_completion_time_matches_timeline() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("trips")
            .with_table_type(table_type.clone())
            .with_record_key_fields(["id"])
            .create()
            .await
            .unwrap();
        let append = table.append([sample_batch()]).await.unwrap();
        let upsert = table
            .upsert([batch(vec![("a", 10), ("d", 4)])])
            .await
            .unwrap();
        let delete = table.delete("id = 'a'").await.unwrap();
        assert_eq!(delete.num_deletes, 1, "{table_type:?}");

        let timeline = table.get_timeline();
        let instants = match table_type {
            TableTypeValue::CopyOnWrite => timeline.get_completed_commits(false).await,
            TableTypeValue::MergeOnRead => timeline.get_completed_deltacommits(false).await,
        }
        .unwrap();
        for (verb, instant, completion_time) in [
            ("append", &append.instant, &append.completion_time),
            ("upsert", &upsert.instant, &upsert.completion_time),
            ("delete", &delete.instant, &delete.completion_time),
        ] {
            let completed = instants
                .iter()
                .find(|i| &i.timestamp == instant)
                .unwrap_or_else(|| panic!("{table_type:?}: {verb} instant not on the timeline"));
            assert!(completion_time.is_some(), "{table_type:?} {verb}");
            assert_eq!(
                completion_time, &completed.completion_timestamp,
                "{table_type:?} {verb}"
            );
        }

        let noop = table.delete("id = 'missing'").await.unwrap();
        assert_eq!(noop.num_deletes, 0, "{table_type:?}");
        assert_eq!(noop.completion_time, None, "{table_type:?}");
    }
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

fn rows_with_event_time(batches: &[RecordBatch]) -> Vec<(String, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let column = |name| batch.column_by_name(name).unwrap().as_any();
        let ids = column("id").downcast_ref::<StringArray>().unwrap();
        let values = column("value").downcast_ref::<Int64Array>().unwrap();
        let times = column("event_time").downcast_ref::<Int64Array>().unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row).to_string(),
                values.value(row),
                times.value(row),
            ));
        }
    }
    rows.sort();
    rows
}

/// A copy-on-write merge copies the stored rows of keys it does not touch as
/// they are, duplicates included, and a partial update takes its unselected
/// columns from the winning stored duplicate.
#[tokio::test]
async fn test_cow_merge_keeps_untouched_duplicates_and_partial_baseline() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_ordering_fields(["event_time"])
        .create()
        .await
        .unwrap();
    table
        .append([ordered_batch(vec![
            ("a", 1, 1),
            ("a", 2, 2),
            ("b", 100, 10),
            ("b", 50, 5),
        ])])
        .await
        .unwrap();

    table
        .upsert([ordered_batch(vec![("c", 3, 3)])])
        .await
        .unwrap();
    let row = |id: &str, value, event_time| (id.to_string(), value, event_time);
    assert_eq!(
        rows_with_event_time(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![
            row("a", 1, 1),
            row("a", 2, 2),
            row("b", 50, 5),
            row("b", 100, 10),
            row("c", 3, 3),
        ]
    );

    table
        .upsert_with(
            [ordered_batch(vec![("b", 999, 20)])],
            UpsertOptions {
                update_columns: Some(vec!["value".to_string()]),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        rows_with_event_time(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![
            row("a", 1, 1),
            row("a", 2, 2),
            row("b", 999, 10),
            row("c", 3, 3)
        ]
    );
}

fn zoned_batch(rows: &[(&str, &str, &str, i64, i64)]) -> RecordBatch {
    let column =
        |index: usize| -> Vec<&str> { rows.iter().map(|r| [r.0, r.1, r.2][index]).collect() };
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("zone", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(column(0))),
            Arc::new(StringArray::from(column(1))),
            Arc::new(StringArray::from(column(2))),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.4).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// A partial upsert merges with the stored row before routing: the merged
/// partition decides whether the row moves, and a moved row keeps the
/// stored values of the columns it does not update.
#[tokio::test]
async fn test_partial_upsert_routes_by_merged_partition() {
    for (update_columns, expected) in [
        (vec!["value"], ("sf", "z1", "city=sf/zone=z1")),
        (vec!["city", "value"], ("ny", "z1", "city=ny/zone=z1")),
        (
            vec!["city", "zone", "value"],
            ("ny", "z9", "city=ny/zone=z9"),
        ),
    ] {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let mut table = Table::create(path)
            .with_table_name("trips")
            .with_record_key_fields(["id"])
            .with_partition_fields(["city", "zone"])
            .create()
            .await
            .unwrap();
        table
            .append([zoned_batch(&[("a", "sf", "z1", 1, 10)])])
            .await
            .unwrap();
        table
            .upsert_with(
                [zoned_batch(&[("a", "ny", "z9", 2, 20)])],
                UpsertOptions {
                    update_columns: Some(update_columns.iter().map(|c| c.to_string()).collect()),
                },
            )
            .await
            .unwrap();

        let batches = Table::new(path)
            .await
            .unwrap()
            .read(&ReadOptions::new())
            .await
            .unwrap();
        let rows = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let text = |name: &str| {
            let column =
                arrow::compute::cast(rows.column_by_name(name).unwrap(), &DataType::Utf8).unwrap();
            let column = column.as_any().downcast_ref::<StringArray>().unwrap();
            (0..column.len())
                .map(|i| column.value(i).to_string())
                .collect::<Vec<_>>()
        };
        let (city, zone, partition_path) = expected;
        assert_eq!(
            [
                text("id"),
                text("city"),
                text("zone"),
                text("ts"),
                text("value"),
                text("_hoodie_partition_path")
            ],
            [
                vec!["a".to_string()],
                vec![city.to_string()],
                vec![zone.to_string()],
                vec!["1".to_string()],
                vec!["20".to_string()],
                vec![partition_path.to_string()]
            ],
            "{update_columns:?}"
        );

        // The index must point at the row's final location.
        table
            .upsert([zoned_batch(&[("a", city, zone, 3, 30)])])
            .await
            .unwrap();
        let rows: usize = table
            .read(&ReadOptions::new())
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 1, "{update_columns:?}");
    }
}

#[tokio::test]
async fn test_partial_upsert_stale_event_time_keeps_lineage() {
    // A partial update whose ordering value LOSES the event-time merge must
    // not restamp the surviving old row: its commit lineage stays with the
    // original commit, so incremental readers do not re-emit it.
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
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
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let commit_time = |batches: &Vec<RecordBatch>| {
        batches[0]
            .column_by_name("_hoodie_commit_time")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    };
    let original_commit = commit_time(&batches);

    table
        .upsert_with(
            [ordered_batch(vec![("a", 2, 50)])],
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
    assert_eq!(values.value(0), 1, "stale partial update must not win");
    assert_eq!(
        commit_time(&batches),
        original_commit,
        "rejected row must keep its original commit lineage"
    );
}

#[tokio::test]
async fn test_invalid_input_leaves_no_pending_instant() {
    // Deterministic input errors must surface before an instant is fenced:
    // the timeline is byte-identical before and after each failed call.
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .create()
        .await
        .unwrap();
    let timeline_files = || {
        let mut names: Vec<String> = std::fs::read_dir(dir.path().join(".hoodie/timeline"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    };

    // An empty record key on a fresh table exercises the keygen validation
    // pass in append (no table schema yet to catch it earlier).
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, true),
    ]));
    let empty_key = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("")])),
            Arc::new(StringArray::from(vec![Some("sf")])),
        ],
    )
    .unwrap();
    let before = timeline_files();
    table.append([empty_key]).await.unwrap_err();
    assert_eq!(
        timeline_files(),
        before,
        "append must not fence on bad input"
    );

    table
        .append([partitioned_batch(vec![("a", "sf", 1)])])
        .await
        .unwrap();

    // Misspelled update column on COW upsert_with.
    let before = timeline_files();
    table
        .upsert_with(
            [partitioned_batch(vec![("a", "sf", 2)])],
            UpsertOptions {
                update_columns: Some(vec!["nope".to_string()]),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        timeline_files(),
        before,
        "upsert_with must not fence on unknown update column"
    );

    // COW update(): SET value type mismatch and all-meta SET are deterministic
    // input errors — no pending instant either.
    let before = timeline_files();
    let wrong_type = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["oops"]))],
    )
    .unwrap();
    table.update("id = 'a'", wrong_type).await.unwrap_err();
    assert_eq!(
        timeline_files(),
        before,
        "update must not fence on SET type mismatch"
    );
    let all_meta = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "_hoodie_commit_time",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["x"]))],
    )
    .unwrap();
    table.update("id = 'a'", all_meta).await.unwrap_err();
    assert_eq!(
        timeline_files(),
        before,
        "update must not fence on all-meta SET"
    );

    // A schema that cannot be expressed in commit metadata (UInt32 has no
    // Avro mapping) must fail before fencing or writing anything.
    let before = timeline_files();
    let unmappable = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("value", DataType::UInt32, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["u"])),
            Arc::new(StringArray::from(vec!["sf"])),
            Arc::new(arrow::array::UInt32Array::from(vec![1u32])),
        ],
    )
    .unwrap();
    table.append([unmappable.clone()]).await.unwrap_err();
    table.upsert([unmappable]).await.unwrap_err();
    assert_eq!(
        timeline_files(),
        before,
        "unmappable schema must not fence or write"
    );
}

#[tokio::test]
async fn test_create_rejects_unsupported_version_without_persisting() {
    // tv10 must be rejected before hoodie.properties or MDT files exist, so
    // the target stays usable for a corrected retry.
    let dir = tempdir().unwrap();
    Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_table_version(10)
        .create()
        .await
        .unwrap_err();
    assert!(
        !dir.path().join(".hoodie").exists(),
        "rejected create must leave the target empty"
    );
    // The corrected retry succeeds in the same location.
    Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_commit_time_ordering_ignores_ordering_field() {
    // A COMMIT_TIME_ORDERING table with an ordering field configured (the
    // common 0.x OverwriteWithLatestAvroPayload upgrade shape) must let the
    // newest write win, exactly as Java's CommitTimeRecordMerger does.
    // Deriving event-time semantics from the ordering field alone silently
    // dropped these updates.
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_ordering_fields(["event_time"])
        .with_option("hoodie.record.merge.mode", "COMMIT_TIME_ORDERING")
        .with_option(
            "hoodie.record.merge.strategy.id",
            // Java's COMMIT_TIME_ORDERING strategy id: leaving create()'s
            // event-time id in place would be a self-contradictory table that
            // Java itself rejects (inferMergingConfigsForPreV9Table).
            "ce9acb64-bde0-424c-9b91-f6ebba25356d",
        )
        .create()
        .await
        .unwrap();
    table
        .upsert([ordered_batch(vec![("a", 1, 100)])])
        .await
        .unwrap();
    // Lower ordering value, newer commit: commit-time ordering keeps it.
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
    assert_eq!(
        values.value(0),
        2,
        "COMMIT_TIME_ORDERING must take the newest write"
    );
}

#[tokio::test]
async fn test_commit_time_ordering_in_batch_dedup_takes_last() {
    // In-batch dedupe must follow the merge mode too (Java routes it through
    // the merge-mode-derived merger): under COMMIT_TIME_ORDERING the last
    // occurrence wins even though its ordering value is lower.
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_ordering_fields(["event_time"])
        .with_option("hoodie.record.merge.mode", "COMMIT_TIME_ORDERING")
        .with_option(
            "hoodie.record.merge.strategy.id",
            "ce9acb64-bde0-424c-9b91-f6ebba25356d",
        )
        .create()
        .await
        .unwrap();
    // One batch, same key twice: ordering 100 then 50.
    table
        .upsert([ordered_batch(vec![("a", 1, 100), ("a", 2, 50)])])
        .await
        .unwrap();
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 1, "in-batch duplicates must collapse");
    let values = batches[0]
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        values.value(0),
        2,
        "COMMIT_TIME_ORDERING dedupe keeps the last occurrence"
    );
}

#[tokio::test]
async fn test_commit_time_ordering_inferred_from_legacy_payload() {
    // The shape this fix exists for: no explicit merge mode, a precombine
    // field, and the 0.x default payload class — Java infers
    // COMMIT_TIME_ORDERING, so the newest write must win.
    let dir = tempdir().unwrap();
    Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_ordering_fields(["event_time"])
        .create()
        .await
        .unwrap();
    // Rewrite hoodie.properties into the legacy shape: drop the merge mode and
    // strategy, name the deprecated payload class.
    let props_path = dir.path().join(".hoodie/hoodie.properties");
    let props = std::fs::read_to_string(&props_path).unwrap();
    let rewritten: String = props
        .lines()
        .filter(|l| {
            !l.starts_with("hoodie.record.merge.mode")
                && !l.starts_with("hoodie.record.merge.strategy.id")
        })
        .map(|l| format!("{l}\n"))
        .collect();
    std::fs::write(
        &props_path,
        format!(
            "{rewritten}hoodie.compaction.payload.class=org.apache.hudi.common.model.OverwriteWithLatestAvroPayload\n"
        ),
    )
    .unwrap();
    let mut table = Table::new(dir.path().to_str().unwrap()).await.unwrap();

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
    assert_eq!(
        values.value(0),
        2,
        "an inferred COMMIT_TIME_ORDERING table must take the newest write"
    );
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

/// Rows are `(id, value, event_time, _hoodie_is_deleted)`.
fn ordered_flagged_batch(rows: Vec<(&str, i64, i64, bool)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("event_time", DataType::Int64, false),
        Field::new("_hoodie_is_deleted", DataType::Boolean, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn ordered_flagged_rows(batches: &[RecordBatch]) -> Vec<(String, i64, i64, bool)> {
    let mut rows = Vec::new();
    for batch in batches {
        let column = |name: &str| batch.column_by_name(name).unwrap().as_any();
        let ids = column("id").downcast_ref::<StringArray>().unwrap();
        let values = column("value").downcast_ref::<Int64Array>().unwrap();
        let times = column("event_time").downcast_ref::<Int64Array>().unwrap();
        let deleted = column("_hoodie_is_deleted")
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row).to_string(),
                values.value(row),
                times.value(row),
                deleted.value(row),
            ));
        }
    }
    rows.sort();
    rows
}

/// Copy-on-write event-time upserts into a group where most stored rows are
/// untouched: each touched key resolves by ordering value (ties go to the
/// incoming row), a winning delete marker removes the row, in-batch duplicates
/// keep the highest ordering value, and untouched rows pass through.
#[tokio::test]
async fn test_cow_event_time_upsert_merges_only_touched_keys() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_ordering_fields(["event_time"])
        .with_populates_meta_fields(true)
        .create()
        .await
        .unwrap();
    let stored: Vec<_> = ["a", "b", "c", "d", "e", "f", "g", "h"]
        .into_iter()
        .zip(1..)
        .map(|(id, value)| (id, value, 100, false))
        .collect();
    table.upsert([ordered_flagged_batch(stored)]).await.unwrap();
    table
        .upsert([ordered_flagged_batch(vec![
            ("a", 10, 50, false),
            ("b", 20, 100, false),
            ("c", 30, 150, false),
            ("c", 31, 120, false),
            ("d", 40, 150, true),
            ("e", 50, 50, true),
            ("i", 90, 10, false),
            ("j", 100, 10, false),
            ("j", 101, 20, false),
        ])])
        .await
        .unwrap();
    table
        .upsert_with(
            [ordered_flagged_batch(vec![
                ("g", 70, 50, false),
                ("h", 80, 200, false),
            ])],
            UpsertOptions {
                update_columns: Some(vec!["value".to_string()]),
            },
        )
        .await
        .unwrap();

    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let expected = [
        ("a", 1, 100),
        ("b", 20, 100),
        ("c", 30, 150),
        ("e", 5, 100),
        ("f", 6, 100),
        ("g", 7, 100),
        ("h", 80, 100),
        ("i", 90, 10),
        ("j", 101, 20),
    ]
    .into_iter()
    .map(|(id, value, event_time)| (id.to_string(), value, event_time, false))
    .collect::<Vec<_>>();
    assert_eq!(ordered_flagged_rows(&batches), expected);
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
    // Inserts land in base files, so read-optimized mode sees `c`; the
    // logged updates stay invisible until compaction.
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

/// Without the metadata table the simple index tags keys; on a table with no
/// commits every key is an insert and no table schema is needed.
#[tokio::test]
async fn test_simple_index_first_upsert_inserts_every_key() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("trips")
            .with_table_type(table_type.clone())
            .with_record_key_fields(["id"])
            .with_metadata(false)
            .create()
            .await
            .unwrap();

        let first = table
            .upsert([batch(vec![("a", 1), ("b", 2)])])
            .await
            .unwrap();
        assert_eq!(
            (first.num_inserts, first.num_updates),
            (2, 0),
            "{table_type:?}"
        );
        let second = table.upsert([batch(vec![("a", 10)])]).await.unwrap();
        assert_eq!(
            (second.num_inserts, second.num_updates),
            (0, 1),
            "{table_type:?}"
        );
        assert_eq!(
            rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
            vec![("a".to_string(), 10), ("b".to_string(), 2)],
            "{table_type:?}"
        );
    }
}

/// Arrow types that the Avro commit schema stores as one type, each with a
/// null value, and the type the table reads them back as.
fn aliased_batch(id: &str) -> RecordBatch {
    use arrow::array::{
        Decimal256Array, Int8Array, Int16Array, LargeBinaryArray, LargeStringArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{TimeUnit, i256};
    let columns: Vec<(Field, arrow::array::ArrayRef)> = vec![
        (
            Field::new("id", DataType::Utf8, false),
            Arc::new(StringArray::from(vec![id])),
        ),
        (
            Field::new("i8", DataType::Int8, true),
            Arc::new(Int8Array::from(vec![Some(-7)])),
        ),
        (
            Field::new("i16", DataType::Int16, true),
            Arc::new(Int16Array::from(vec![None::<i16>])),
        ),
        (
            Field::new("s", DataType::LargeUtf8, true),
            Arc::new(LargeStringArray::from(vec![Some("text")])),
        ),
        (
            Field::new("b", DataType::LargeBinary, true),
            Arc::new(LargeBinaryArray::from(vec![Some(&b"\x00\x01"[..])])),
        ),
        (
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+05:00".into())),
                true,
            ),
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(1_000_000)]).with_timezone("+05:00"),
            ),
        ),
        (
            Field::new(
                "l",
                DataType::LargeList(Arc::new(Field::new("item", DataType::Int16, true))),
                true,
            ),
            Arc::new(arrow::array::LargeListArray::new(
                Arc::new(Field::new("item", DataType::Int16, true)),
                arrow::buffer::OffsetBuffer::from_lengths([2]),
                Arc::new(Int16Array::from(vec![Some(1), None])),
                None,
            )),
        ),
        (
            Field::new("d", DataType::Decimal256(10, 2), true),
            Arc::new(
                Decimal256Array::from(vec![Some(i256::from_i128(12345))])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        ),
    ];
    let (fields, arrays): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

#[tokio::test]
async fn test_aliased_arrow_types_support_repeated_writes() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let mut table = Table::create(path)
            .with_table_name("types")
            .with_table_type(table_type.clone())
            .with_record_key_fields(["id"])
            .create()
            .await
            .unwrap();
        table.append([aliased_batch("a")]).await.unwrap();
        table.append([aliased_batch("b")]).await.unwrap();
        table.upsert([aliased_batch("a")]).await.unwrap();
        table.overwrite([aliased_batch("c")]).await.unwrap();
        table
            .update("id = 'c'", aliased_batch("c").project(&[1]).unwrap())
            .await
            .unwrap();

        let table = Table::new(path).await.unwrap();
        let batches = table.read(&ReadOptions::new()).await.unwrap();
        let rows = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(rows.num_rows(), 1, "{table_type:?}");
        let rendered: Vec<String> = ["i8", "i16", "s", "b", "ts", "l", "d"]
            .iter()
            .map(|name| {
                let column = rows.column_by_name(name).unwrap();
                let value = match column.data_type() {
                    DataType::Timestamp(..) => {
                        arrow::compute::cast(column, &DataType::Int64).unwrap()
                    }
                    _ => column.clone(),
                };
                format!(
                    "{:?}={}",
                    column.data_type(),
                    arrow::util::display::array_value_to_string(&value, 0).unwrap()
                )
            })
            .collect();
        assert_eq!(
            rendered,
            vec![
                "Int32=-7",
                "Int32=",
                "Utf8=text",
                "Binary=0001",
                "Timestamp(Microsecond, Some(\"UTC\"))=1000000",
                "List(Field { name: \"element\", data_type: Int32, nullable: true })=[1, ]",
                "Decimal128(10, 2)=123.45",
            ],
            "{table_type:?}"
        );
    }
}

#[tokio::test]
async fn test_decimal_beyond_avro_precision_rejected_before_commit() {
    use arrow::array::Decimal256Array;
    use arrow::datatypes::i256;
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("types")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("d", DataType::Decimal256(40, 0), false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(
                Decimal256Array::from(vec![i256::from_i128(1)])
                    .with_precision_and_scale(40, 0)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    let error = table.append([batch]).await.unwrap_err();
    assert!(error.to_string().contains("precision 40"), "{error}");
    assert_no_instant(dir.path());
}

/// Like a non-prepped Java write, caller-supplied meta columns are dropped,
/// so stale values cannot hide a write or change a row's key.
#[tokio::test]
async fn test_input_meta_columns_are_replaced() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("trips")
            .with_table_type(table_type.clone())
            .with_record_key_fields(["id"])
            .create()
            .await
            .unwrap();
        table.append([batch(vec![("a", 1)])]).await.unwrap();

        let read_back = table.read(&ReadOptions::new()).await.unwrap();
        let result = table.upsert(read_back).await.unwrap();
        let rows = table.read(&ReadOptions::new()).await.unwrap();
        let commit_times = rows[0]
            .column_by_name("_hoodie_commit_time")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(commit_times.value(0), result.instant, "{table_type:?}");

        let mut fields = vec![Field::new("_hoodie_record_key", DataType::Utf8, true)];
        fields.extend(
            batch(vec![])
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone()),
        );
        let mut columns: Vec<arrow::array::ArrayRef> =
            vec![Arc::new(StringArray::from(vec!["stale"]))];
        columns.extend(batch(vec![("b", 2)]).columns().iter().cloned());
        let stale_key = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        table.append([stale_key]).await.unwrap();
        table
            .delete_keys([HoodieKey {
                record_key: "b".to_string(),
                partition_path: String::new(),
            }])
            .await
            .unwrap();
        assert_eq!(
            rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
            vec![("a".to_string(), 1)],
            "{table_type:?}"
        );
    }
}

/// Batches are concatenated by position, so a later batch whose columns are
/// renamed or reordered must be rejected rather than merged.
#[tokio::test]
async fn test_upsert_rejects_batches_with_differing_schemas() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("trips")
            .with_table_type(table_type.clone())
            .with_record_key_fields(["id"])
            .with_ordering_fields(["ts"])
            .create()
            .await
            .unwrap();
        let first = ordered_batch(vec![("a", 1, 10)]);
        let schema = first.schema();
        let reordered = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                schema.field(0).clone(),
                schema.field(2).clone(),
                schema.field(1).clone(),
            ])),
            {
                let other = ordered_batch(vec![("b", 2, 20)]);
                vec![
                    other.column(0).clone(),
                    other.column(2).clone(),
                    other.column(1).clone(),
                ]
            },
        )
        .unwrap();
        let error = table
            .upsert([first, reordered])
            .await
            .err()
            .unwrap_or_else(|| panic!("{table_type:?} accepted the batches"));
        assert!(
            matches!(error, CoreError::Schema(_)),
            "{table_type:?}: {error}"
        );
        assert_no_instant(dir.path());
    }
}

/// Rows written into a merge-on-read log file carry the file group id in
/// `_hoodie_file_name`, as Java's append handle writes them; Spark SQL derives
/// the record location from it.
#[tokio::test]
async fn test_mor_log_rows_name_their_file_group() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_table_type(TableTypeValue::MergeOnRead)
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    table.append([batch(vec![("a", 1)])]).await.unwrap();
    table
        .upsert([batch(vec![("a", 2), ("b", 3)])])
        .await
        .unwrap();

    let file_groups: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .filter(|name| name.ends_with(".parquet"))
        .map(|name| name.split('_').next().unwrap().to_string())
        .collect();
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    for batch in &batches {
        let names = batch
            .column_by_name("_hoodie_file_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for name in names.iter().flatten() {
            let file_id = name.split('_').next().unwrap();
            assert!(file_groups.iter().any(|id| id == file_id), "{name}");
        }
    }
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

/// A row whose filter column is null does not match the filter.
#[tokio::test]
async fn test_delete_by_filter_keeps_rows_with_null_filter_column() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("trips")
            .with_table_type(table_type.clone())
            .with_record_key_fields(["id"])
            .create()
            .await
            .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]));
        let rows = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![Some(10), None, Some(20)])),
            ],
        )
        .unwrap();
        table.append([rows]).await.unwrap();

        let deleted = table.delete("value = 20").await.unwrap();
        assert_eq!(deleted.num_deletes, 1, "{table_type:?}");
        let batches = table.read(&ReadOptions::new()).await.unwrap();
        let mut ids: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                ids.iter().flatten().map(str::to_string).collect::<Vec<_>>()
            })
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "b"], "{table_type:?}");
    }
}

/// An update by filter that deletes rows carrying a delete marker leaves a
/// marked row whose filter column is null.
#[tokio::test]
async fn test_update_by_filter_keeps_marked_rows_with_null_filter_column() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, true),
        Field::new("_hoodie_is_deleted", DataType::Boolean, true),
    ]));
    let rows = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![None, Some(20)])),
            Arc::new(BooleanArray::from(vec![true, false])),
        ],
    )
    .unwrap();
    table.append([rows]).await.unwrap();

    table.update("value = 20", set_value(30)).await.unwrap();
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let mut rows: Vec<(String, Option<i64>)> = batches
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
                .map(|row| {
                    let value = values.is_valid(row).then(|| values.value(row));
                    (ids.value(row).to_string(), value)
                })
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![("a".to_string(), None), ("b".to_string(), Some(30))]
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

/// Auto-generated keys are unique across the batches of one overwrite, and
/// the record index holds exactly the keys stored in the files.
#[tokio::test]
async fn test_auto_keys_unique_across_overwrite_batches() {
    for (table_type, dynamic) in [
        (TableTypeValue::CopyOnWrite, false),
        (TableTypeValue::CopyOnWrite, true),
        (TableTypeValue::MergeOnRead, false),
        (TableTypeValue::MergeOnRead, true),
    ] {
        let ctx = format!("{table_type:?} dynamic={dynamic}");
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_table_type(table_type)
            .with_partition_fields(["city"])
            .create()
            .await
            .unwrap();
        table
            .append([partitioned_batch(vec![("x", "sf", 0)])])
            .await
            .unwrap();
        let batches = [
            partitioned_batch(vec![("a", "sf", 1), ("b", "sf", 2)]),
            partitioned_batch(vec![("c", "sf", 3), ("d", "sf", 4)]),
        ];
        if dynamic {
            table.dynamic_partition_overwrite(batches).await.unwrap();
        } else {
            table.overwrite(batches).await.unwrap();
        }

        let mut stored = std::collections::HashSet::new();
        let mut instant = String::new();
        for rows in table.read(&ReadOptions::new()).await.unwrap() {
            let keys = rows
                .column_by_name("_hoodie_record_key")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let commit_times = rows
                .column_by_name("_hoodie_commit_time")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..rows.num_rows() {
                assert!(
                    stored.insert(keys.value(row).to_string()),
                    "{ctx}: duplicate auto key {}",
                    keys.value(row)
                );
                instant = commit_times.value(row).to_string();
            }
        }
        assert_eq!(stored.len(), 4, "{ctx}");

        let candidates: Vec<HoodieKey> = (0..8)
            .map(|row| HoodieKey {
                record_key: format!("{instant}_0_{row}"),
                partition_path: "city=sf".to_string(),
            })
            .collect();
        let located = hudi_core::index::for_table(&table)
            .tag_location(&table, &candidates)
            .await
            .unwrap();
        let indexed: std::collections::HashSet<String> = candidates
            .iter()
            .filter(|key| located.get(*key).and_then(Option::as_ref).is_some())
            .map(|key| key.record_key.clone())
            .collect();
        assert_eq!(indexed, stored, "{ctx}");
    }
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

/// Create-builder validation: every unsupported combination must fail up
/// front with a clear error, before anything lands on storage.
#[tokio::test]
async fn test_create_builder_rejects_invalid_combinations() {
    // Missing table name.
    let dir = tempdir().unwrap();
    let err = Table::create(dir.path().to_str().unwrap())
        .create()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Table name is required"));

    // RLI without MDT.
    let dir = tempdir().unwrap();
    let err = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_metadata(false)
        .with_record_index(true)
        .create()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("record index requires"));

    // column_stats without MDT.
    let dir = tempdir().unwrap();
    let err = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_metadata(false)
        .with_record_index(false)
        .with_column_stats(true)
        .create()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("column_stats requires"));

    // partition_stats without column_stats.
    let dir = tempdir().unwrap();
    let err = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_column_stats(false)
        .with_partition_stats(true)
        .create()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("partition_stats requires"));

    // v9 with layout 1 / v6 with layout 2: both incoherent.
    let dir = tempdir().unwrap();
    let err = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_timeline_layout_version(1)
        .create()
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("requires timeline layout version 2")
    );
}

/// MOR-unsupported combinations must return clear errors.
#[tokio::test]
async fn test_mor_unsupported_verbs_error_clearly() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_table_type(TableTypeValue::MergeOnRead)
        .with_record_key_fields(["id"])
        .with_ordering_fields(["event_time"])
        .with_populates_meta_fields(true)
        .create()
        .await
        .unwrap();
    table
        .append([ordered_batch(vec![("a", 1, 1)])])
        .await
        .unwrap();

    // Partial-column upsert is COW-only.
    let err = table
        .upsert_with(
            [ordered_batch(vec![("a", 2, 2)])],
            UpsertOptions {
                update_columns: Some(vec!["value".to_string()]),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CoreError::Unsupported(_)), "{err}");

    // Full-table overwrite works on MOR (INSERT_OVERWRITE_TABLE semantics).
    let result = table
        .overwrite([ordered_batch(vec![("z", 9, 9)])])
        .await
        .unwrap();
    assert_eq!(result.num_writes, 1);

    // Zero-match expression delete: no commit, empty result.
    let commits_before = std::fs::read_dir(dir.path().join(".hoodie/timeline"))
        .unwrap()
        .count();
    let result = table.delete("value = 12345").await.unwrap();
    assert_eq!(result.num_deletes, 0);
    assert_eq!(result.instant, "");
    let commits_after = std::fs::read_dir(dir.path().join(".hoodie/timeline"))
        .unwrap()
        .count();
    assert_eq!(commits_before, commits_after, "zero-match must not commit");

    // Dynamic partition overwrite requires a partitioned table — and the
    // error must not leave a pending instant behind.
    let err = table
        .dynamic_partition_overwrite([ordered_batch(vec![("z2", 10, 10)])])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("partitioned"), "{err}");
    let commits_final = std::fs::read_dir(dir.path().join(".hoodie/timeline"))
        .unwrap()
        .count();
    assert_eq!(
        commits_after, commits_final,
        "failed validation must not fence an instant"
    );
}

/// Update-partition-path: upserting a key with a changed partition value must
/// MOVE the row — visible in the new partition, gone from the old, with
/// partition column and path in agreement. (Regression: the row used to stay
/// physically in the old partition with new-partition values, unreachable
/// from either partition filter.)
#[tokio::test]
async fn test_stale_partition_move_rejected_by_event_time() {
    // Java merges BEFORE deciding a partition move: a cross-partition update
    // with a lower ordering value loses and must not destroy the newer row
    // (HoodieIndexUtils.mergeForPartitionUpdatesAndDeletionsIfNeeded).
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_table_type(table_type.clone())
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_ordering_fields(["value"])
            .create()
            .await
            .unwrap();
        table
            .append([partitioned_batch(vec![("a", "sf", 5)])])
            .await
            .unwrap();
        // Stale move: ordering 3 < 5 loses; the sf row survives untouched.
        table
            .upsert([partitioned_batch(vec![("a", "nyc", 3)])])
            .await
            .unwrap();
        let rows = rows_by_id(&table.read(&ReadOptions::new()).await.unwrap());
        assert_eq!(
            rows,
            vec![("a".to_string(), 5)],
            "{table_type:?}: stale move"
        );
        let sf = rows_by_id(
            &table
                .read(
                    &ReadOptions::new()
                        .with_filters([("city", "=", "sf")])
                        .unwrap(),
                )
                .await
                .unwrap(),
        );
        assert_eq!(
            sf,
            vec![("a".to_string(), 5)],
            "{table_type:?}: stays in sf"
        );
        // Winning move: ordering 7 > 5 moves the row to la.
        table
            .upsert([partitioned_batch(vec![("a", "la", 7)])])
            .await
            .unwrap();
        let la = rows_by_id(
            &table
                .read(
                    &ReadOptions::new()
                        .with_filters([("city", "=", "la")])
                        .unwrap(),
                )
                .await
                .unwrap(),
        );
        assert_eq!(
            la,
            vec![("a".to_string(), 7)],
            "{table_type:?}: winning move"
        );
        let total = rows_by_id(&table.read(&ReadOptions::new()).await.unwrap());
        assert_eq!(
            total,
            vec![("a".to_string(), 7)],
            "{table_type:?}: one copy"
        );
    }
}

#[tokio::test]
async fn test_upsert_partition_change_moves_row() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_table_type(table_type.clone())
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .create()
            .await
            .unwrap();
        table
            .append([partitioned_batch(vec![("a", "sf", 1), ("b", "sf", 2)])])
            .await
            .unwrap();
        let result = table
            .upsert([partitioned_batch(vec![("a", "nyc", 10)])])
            .await
            .unwrap();
        assert_eq!(result.num_updates, 1, "{table_type:?}");

        // Whole-table read: exactly one copy of `a`, with the new value.
        let rows = rows_by_id(&table.read(&ReadOptions::new()).await.unwrap());
        assert_eq!(
            rows,
            vec![("a".to_string(), 10), ("b".to_string(), 2)],
            "{table_type:?}"
        );

        // Partition-filtered reads: `a` only in nyc, `b` only in sf.
        let nyc = rows_by_id(
            &table
                .read(
                    &ReadOptions::new()
                        .with_filters([("city", "=", "nyc")])
                        .unwrap(),
                )
                .await
                .unwrap(),
        );
        assert_eq!(nyc, vec![("a".to_string(), 10)], "{table_type:?}");
        let sf = rows_by_id(
            &table
                .read(
                    &ReadOptions::new()
                        .with_filters([("city", "=", "sf")])
                        .unwrap(),
                )
                .await
                .unwrap(),
        );
        assert_eq!(sf, vec![("b".to_string(), 2)], "{table_type:?}");
    }
}

fn flagged_batch(rows: Vec<(&str, &str, i64, bool)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("_hoodie_is_deleted", DataType::Boolean, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// A delete marker deletes the stored row even in a partial upsert that does
/// not select it, whether or not the input names the stored partition.
#[tokio::test]
async fn test_partial_upsert_honors_delete_marker() {
    for input_city in ["sf", "nyc"] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .create()
            .await
            .unwrap();
        table
            .append([flagged_batch(vec![
                ("a", "sf", 1, false),
                ("b", "nyc", 2, false),
            ])])
            .await
            .unwrap();
        table
            .upsert_with(
                [flagged_batch(vec![("a", input_city, 9, true)])],
                UpsertOptions {
                    update_columns: Some(vec!["value".to_string()]),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
            vec![("b".to_string(), 2)],
            "input city {input_city}"
        );
    }
}

/// `_hoodie_is_deleted` is a data column (Hudi's delete marker), not one of
/// the writer-managed meta fields: it stays in the table and commit schema.
#[tokio::test]
async fn test_hoodie_is_deleted_column_is_data_not_meta() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_table_type(table_type.clone())
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .create()
            .await
            .unwrap();
        table
            .append([flagged_batch(vec![
                ("a", "sf", 1, false),
                ("b", "nyc", 2, false),
            ])])
            .await
            .unwrap();
        table
            .upsert([flagged_batch(vec![("a", "sf", 4, false)])])
            .await
            .unwrap();
        let commit_schema = table.timeline.get_latest_avro_schema().await.unwrap();
        assert!(
            commit_schema.contains("\"_hoodie_is_deleted\""),
            "{table_type:?}: {commit_schema}"
        );
        assert!(
            table
                .get_schema()
                .await
                .unwrap()
                .field_with_name("_hoodie_is_deleted")
                .is_ok(),
            "{table_type:?}"
        );

        table
            .append([flagged_batch(vec![("c", "sf", 3, false)])])
            .await
            .unwrap();
        assert_eq!(
            rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
            vec![
                ("a".to_string(), 4),
                ("b".to_string(), 2),
                ("c".to_string(), 3)
            ],
            "{table_type:?}"
        );
    }
}

/// An upserted row with `_hoodie_is_deleted = true` is a delete when it wins
/// the merge: the row disappears and the record index forgets the key. A
/// flagged row for a new key writes nothing, a flagged partition move only
/// deletes, and a stale flagged row loses to the stored row.
#[tokio::test]
async fn test_upsert_hoodie_is_deleted_removes_row_and_index_entry() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_table_type(table_type.clone())
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_ordering_fields(["value"])
            .create()
            .await
            .unwrap();
        table
            .append([flagged_batch(vec![
                ("a", "sf", 10, false),
                ("b", "sf", 10, false),
                ("c", "sf", 10, false),
            ])])
            .await
            .unwrap();
        table
            .upsert([flagged_batch(vec![
                ("a", "sf", 20, true),
                ("b", "sf", 5, true),
                ("c", "nyc", 20, true),
                ("d", "sf", 20, true),
                ("e", "nyc", 20, false),
            ])])
            .await
            .unwrap();

        let batches = table.read(&ReadOptions::new()).await.unwrap();
        assert_eq!(
            rows_by_id(&batches),
            vec![("b".to_string(), 10), ("e".to_string(), 20)],
            "{table_type:?}"
        );

        let keys: Vec<HoodieKey> = [
            ("a", "sf"),
            ("b", "sf"),
            ("c", "nyc"),
            ("d", "sf"),
            ("e", "nyc"),
        ]
        .iter()
        .map(|(key, city)| HoodieKey {
            record_key: key.to_string(),
            partition_path: format!("city={city}"),
        })
        .collect();
        let located = hudi_core::index::for_table(&table)
            .tag_location(&table, &keys)
            .await
            .unwrap();
        let indexed: Vec<&str> = keys
            .iter()
            .filter(|key| located.get(*key).and_then(Option::as_ref).is_some())
            .map(|key| key.record_key.as_str())
            .collect();
        assert_eq!(indexed, vec!["b", "e"], "{table_type:?}");
    }
}

/// An update that sets `_hoodie_is_deleted = true` deletes the matched rows
/// on either table type, as Java's merge handle does.
#[tokio::test]
async fn test_update_setting_hoodie_is_deleted_removes_row_and_index_entry() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_table_type(table_type.clone())
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .create()
            .await
            .unwrap();
        table
            .append([flagged_batch(vec![
                ("a", "sf", 1, false),
                ("b", "sf", 2, false),
            ])])
            .await
            .unwrap();
        let set_deleted = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "_hoodie_is_deleted",
                DataType::Boolean,
                true,
            )])),
            vec![Arc::new(BooleanArray::from(vec![true]))],
        )
        .unwrap();
        table.update("id = 'a'", set_deleted).await.unwrap();

        let batches = table.read(&ReadOptions::new()).await.unwrap();
        assert_eq!(
            rows_by_id(&batches),
            vec![("b".to_string(), 2)],
            "{table_type:?}"
        );
        let keys: Vec<HoodieKey> = ["a", "b"]
            .iter()
            .map(|key| HoodieKey {
                record_key: key.to_string(),
                partition_path: "city=sf".to_string(),
            })
            .collect();
        let located = hudi_core::index::for_table(&table)
            .tag_location(&table, &keys)
            .await
            .unwrap();
        assert!(located[&keys[0]].is_none(), "{table_type:?}");
        assert!(located[&keys[1]].is_some(), "{table_type:?}");
    }
}

/// Rewrite `hoodie.properties` the way another writer would have left it.
fn rewrite_table_props(base: &std::path::Path, remove: &[&str], set: &[(&str, &str)]) {
    let path = base.join(".hoodie/hoodie.properties");
    let existing = std::fs::read_to_string(&path).unwrap();
    let mut rewritten: String = existing
        .lines()
        .filter(|line| {
            let key = line.split_once('=').map(|(k, _)| k).unwrap_or(line);
            !remove.contains(&key) && !set.iter().any(|(k, _)| *k == key)
        })
        .map(|line| format!("{line}\n"))
        .collect();
    for (key, value) in set {
        rewritten.push_str(&format!("{key}={value}\n"));
    }
    std::fs::write(&path, rewritten).unwrap();
}

/// `(_hoodie_record_key, _hoodie_partition_path)` of every row, sorted.
fn meta_keys(batches: &[RecordBatch]) -> Vec<(String, String)> {
    let mut keys = batches
        .iter()
        .flat_map(|batch| {
            let record_keys = batch
                .column_by_name("_hoodie_record_key")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let partitions = batch
                .column_by_name("_hoodie_partition_path")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|i| {
                    (
                        record_keys.value(i).to_string(),
                        partitions.value(i).to_string(),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    keys.sort();
    keys
}

fn nullable_city_batch(rows: Vec<(&str, Option<&str>, i64)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, true),
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

/// Writes that derive keys from data refuse a key generator they cannot
/// reproduce; deletes and COW updates, which take keys from the table, do not.
#[tokio::test]
async fn test_unsupported_key_generators_block_only_key_deriving_writes() {
    let cases = [
        ("hoodie.table.keygenerator.type", "TIMESTAMP"),
        ("hoodie.table.keygenerator.type", "CUSTOM"),
        (
            "hoodie.table.keygenerator.class",
            "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
        ),
        (
            "hoodie.table.keygenerator.class",
            "org.apache.hudi.keygen.CustomKeyGenerator",
        ),
        (
            "hoodie.table.keygenerator.class",
            "com.example.UserKeyGenerator",
        ),
    ];
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for (key, value) in cases {
            let dir = tempdir().unwrap();
            let mut table = Table::create(dir.path().to_str().unwrap())
                .with_table_name("t")
                .with_table_type(table_type.clone())
                .with_record_key_fields(["id"])
                .with_partition_fields(["city"])
                .create()
                .await
                .unwrap();
            table
                .append([city_batch(vec![
                    ("a", "sf", 1),
                    ("b", "sf", 2),
                    ("c", "sf", 3),
                ])])
                .await
                .unwrap();
            rewrite_table_props(
                dir.path(),
                &["hoodie.table.keygenerator.type"],
                &[(key, value)],
            );
            let mut table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            let rejected = [
                table
                    .upsert([city_batch(vec![("a", "sf", 10)])])
                    .await
                    .err(),
                table.append([city_batch(vec![("d", "sf", 4)])]).await.err(),
                table
                    .overwrite([city_batch(vec![("d", "sf", 4)])])
                    .await
                    .err(),
            ];
            for err in rejected {
                assert!(
                    matches!(&err, Some(CoreError::Unsupported(msg))
                        if msg.contains(key) && msg.contains(value)),
                    "{table_type:?} {key}={value}: {err:?}"
                );
            }

            table
                .delete_keys([HoodieKey {
                    record_key: "a".to_string(),
                    partition_path: "city=sf".to_string(),
                }])
                .await
                .unwrap();
            table.delete("value = 2").await.unwrap();
            let update = table.update("id = 'c'", set_value(30)).await;
            if table_type == TableTypeValue::CopyOnWrite {
                update.unwrap();
            } else {
                assert!(
                    matches!(&update, Err(CoreError::Unsupported(_))),
                    "{key}={value}: {update:?}"
                );
            }
            let expected = if table_type == TableTypeValue::CopyOnWrite {
                30
            } else {
                3
            };
            let batches = table.read(&ReadOptions::new()).await.unwrap();
            assert_eq!(
                rows_by_id(&batches),
                vec![("c".to_string(), expected)],
                "{table_type:?} {key}={value}"
            );
        }
    }
}

/// A custom key generator types its partition fields (`city:simple`); SET of
/// the bare column name must still be refused as a partition column.
#[tokio::test]
async fn test_update_rejects_set_of_typed_partition_field() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_table_type(table_type.clone())
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .create()
            .await
            .unwrap();
        table
            .append([city_batch(vec![("a", "sf", 1)])])
            .await
            .unwrap();
        rewrite_table_props(
            dir.path(),
            &["hoodie.table.keygenerator.type"],
            &[
                (
                    "hoodie.table.keygenerator.class",
                    "org.apache.hudi.keygen.CustomKeyGenerator",
                ),
                ("hoodie.table.partition.fields", "city:simple"),
            ],
        );
        let mut table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
        let set_city = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("city", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["la"]))],
        )
        .unwrap();
        let result = table.update("id = 'a'", set_city).await;
        assert!(
            matches!(&result, Err(CoreError::Write(msg)) if msg.contains("'city'")),
            "{table_type:?}: {result:?}"
        );
    }
}

#[tokio::test]
async fn test_complex_key_generator_encodes_single_key_field() {
    // Spark records the key generator by class; SqlKeyGenerator with no origin
    // class in the table config delegates to ComplexKeyGenerator.
    let cases = [
        ("hoodie.table.keygenerator.type", "COMPLEX"),
        (
            "hoodie.table.keygenerator.class",
            "org.apache.hudi.keygen.ComplexKeyGenerator",
        ),
        (
            "hoodie.table.keygenerator.class",
            "org.apache.spark.sql.hudi.command.SqlKeyGenerator",
        ),
    ];
    for (key, value) in cases {
        let dir = tempdir().unwrap();
        Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .create()
            .await
            .unwrap();
        rewrite_table_props(
            dir.path(),
            &["hoodie.table.keygenerator.type"],
            &[(key, value)],
        );
        let mut table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
        table
            .append([city_batch(vec![("a", "sf", 1), ("b", "sf", 2)])])
            .await
            .unwrap();
        table
            .upsert([city_batch(vec![("a", "sf", 10)])])
            .await
            .unwrap();
        let batches = table.read(&ReadOptions::new()).await.unwrap();
        assert_eq!(
            meta_keys(&batches),
            vec![
                ("id:a".to_string(), "city=sf".to_string()),
                ("id:b".to_string(), "city=sf".to_string()),
            ],
            "{key}={value}"
        );
        assert_eq!(
            rows_by_id(&batches),
            vec![("a".to_string(), 10), ("b".to_string(), 2)],
            "{key}={value}"
        );

        // A delete filter on the key column takes the column value.
        table.delete("id = 'a'").await.unwrap();
        let batches = table.read(&ReadOptions::new()).await.unwrap();
        assert_eq!(
            rows_by_id(&batches),
            vec![("b".to_string(), 2)],
            "{key}={value}"
        );
    }
}

/// Upserts locate stored rows by their encoded complex record key: a flagged
/// row deletes, a stale flagged row loses, a flagged move only deletes, and a
/// stale move keeps the stored row, with the same counts as a simple key.
#[tokio::test]
async fn test_complex_key_upsert_matches_stored_rows() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let mut results = Vec::new();
        for complex in [false, true] {
            let dir = tempdir().unwrap();
            Table::create(dir.path().to_str().unwrap())
                .with_table_name("t")
                .with_table_type(table_type.clone())
                .with_record_key_fields(["id"])
                .with_partition_fields(["city"])
                .with_ordering_fields(["value"])
                .create()
                .await
                .unwrap();
            if complex {
                rewrite_table_props(
                    dir.path(),
                    &["hoodie.table.keygenerator.type"],
                    &[("hoodie.table.keygenerator.type", "COMPLEX")],
                );
            }
            let mut table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            table
                .append([flagged_batch(vec![
                    ("a", "sf", 10, false),
                    ("b", "sf", 10, false),
                    ("c", "sf", 10, false),
                    ("f", "sf", 10, false),
                    ("g", "sf", 10, false),
                ])])
                .await
                .unwrap();
            let result = table
                .upsert([flagged_batch(vec![
                    ("a", "sf", 20, true),
                    ("b", "sf", 5, true),
                    ("c", "nyc", 20, true),
                    ("e", "nyc", 20, false),
                    ("f", "sf", 20, false),
                    ("g", "nyc", 5, false),
                ])])
                .await
                .unwrap();

            let batches = table.read(&ReadOptions::new()).await.unwrap();
            let label = format!("{table_type:?} complex={complex}");
            assert_eq!(
                rows_by_id(&batches),
                vec![
                    ("b".to_string(), 10),
                    ("e".to_string(), 20),
                    ("f".to_string(), 20),
                    ("g".to_string(), 10),
                ],
                "{label}"
            );
            let prefix = if complex { "id:" } else { "" };
            assert_eq!(
                meta_keys(&batches),
                [("b", "sf"), ("e", "nyc"), ("f", "sf"), ("g", "sf")]
                    .iter()
                    .map(|(key, city)| (format!("{prefix}{key}"), format!("city={city}")))
                    .collect::<Vec<_>>(),
                "{label}"
            );
            results.push((result.num_updates, result.num_inserts, result.num_deletes));
        }
        assert_eq!(results[0], results[1], "{table_type:?}");
    }
}

/// Below table version 9 a declared complex key generator with one record key
/// field may have stored bare keys, so writes are refused (Java
/// `validateAgainstTableProperties`) until the validation is turned off.
#[tokio::test]
async fn test_tv8_complex_key_generator_with_single_key_field_requires_opt_out() {
    let cases = [
        ("hoodie.table.keygenerator.type", "COMPLEX"),
        (
            "hoodie.table.keygenerator.class",
            "org.apache.hudi.keygen.ComplexKeyGenerator",
        ),
    ];
    for (key, value) in cases {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_table_version(8)
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .create()
            .await
            .unwrap();
        // Bare keys, as a 1.0.x writer stored them.
        table
            .append([city_batch(vec![("a", "sf", 1)])])
            .await
            .unwrap();
        rewrite_table_props(
            dir.path(),
            &["hoodie.table.keygenerator.type"],
            &[(key, value)],
        );

        let mut table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
        let err = table
            .upsert([city_batch(vec![("a", "sf", 10)])])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, CoreError::Unsupported(msg)
                if msg.contains("hoodie.write.complex.keygen.validation.enable")),
            "{key}={value}: {err}"
        );
        let batches = table.read(&ReadOptions::new()).await.unwrap();
        assert_eq!(
            meta_keys(&batches),
            vec![("a".to_string(), "city=sf".to_string())],
            "{key}={value}"
        );

        let mut table = Table::new_with_options(
            dir.path().to_str().unwrap(),
            [("hoodie.write.complex.keygen.validation.enable", "false")],
        )
        .await
        .unwrap();
        table
            .upsert([city_batch(vec![("b", "sf", 2)])])
            .await
            .unwrap();
    }
}

/// Java refuses to write a table version 8 table whose single record key and
/// several partition fields make it a complex key generator table, so such a
/// table is not created unless the validation is turned off.
#[tokio::test]
async fn test_tv8_create_with_single_key_and_partition_fields_requires_opt_out() {
    let dir = tempdir().unwrap();
    let err = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_table_version(8)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city", "value"])
        .create()
        .await
        .unwrap_err();
    assert!(
        matches!(&err, CoreError::Unsupported(msg)
            if msg.contains("hoodie.write.complex.keygen.validation.enable")),
        "{err}"
    );
    assert!(!dir.path().join(".hoodie/hoodie.properties").exists());

    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_table_version(8)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city", "value"])
        .with_option("hoodie.write.complex.keygen.validation.enable", "false")
        .create()
        .await
        .unwrap();
    table
        .append([city_batch(vec![("a", "sf", 1)])])
        .await
        .unwrap();
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    assert_eq!(meta_keys(&batches)[0].0, "id:a");
}

#[tokio::test]
async fn test_urlencoded_partition_path_matches_java_escaping() {
    let dir = tempdir().unwrap();
    Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_hive_style_partitioning(true)
        .create()
        .await
        .unwrap();
    rewrite_table_props(
        dir.path(),
        &[],
        &[("hoodie.datasource.write.partitionpath.urlencode", "true")],
    );
    let mut table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    table
        .append([city_batch(vec![("a", "2024/01/02", 1), ("b", "a b:c", 2)])])
        .await
        .unwrap();
    table
        .upsert([city_batch(vec![("a", "2024/01/02", 10)])])
        .await
        .unwrap();
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    assert_eq!(
        meta_keys(&batches),
        vec![
            ("a".to_string(), "city=2024%2F01%2F02".to_string()),
            ("b".to_string(), "city=a b%3Ac".to_string()),
        ]
    );
    assert_eq!(
        rows_by_id(&batches),
        vec![("a".to_string(), 10), ("b".to_string(), 2)]
    );
    assert!(dir.path().join("city=2024%2F01%2F02").is_dir());
    assert!(!dir.path().join("city=2024").exists());
}

#[tokio::test]
async fn test_empty_and_null_partition_values_use_default_partition() {
    for hive_style in [true, false] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_hive_style_partitioning(hive_style)
            .create()
            .await
            .unwrap();
        let default_partition = if hive_style {
            "city=__HIVE_DEFAULT_PARTITION__"
        } else {
            "__HIVE_DEFAULT_PARTITION__"
        };
        let sf = if hive_style { "city=sf" } else { "sf" };

        table
            .append([nullable_city_batch(vec![
                ("a", Some(""), 1),
                ("c", Some("sf"), 3),
            ])])
            .await
            .unwrap();
        assert_eq!(
            meta_keys(&table.read(&ReadOptions::new()).await.unwrap()),
            vec![
                ("a".to_string(), default_partition.to_string()),
                ("c".to_string(), sf.to_string()),
            ],
            "hive_style={hive_style}"
        );

        table
            .upsert([nullable_city_batch(vec![
                ("a", Some(""), 10),
                ("b", None, 2),
            ])])
            .await
            .unwrap();
        let batches = table.read(&ReadOptions::new()).await.unwrap();
        assert_eq!(
            meta_keys(&batches),
            vec![
                ("a".to_string(), default_partition.to_string()),
                ("b".to_string(), default_partition.to_string()),
                ("c".to_string(), sf.to_string()),
            ],
            "hive_style={hive_style}"
        );
        assert_eq!(
            rows_by_id(&batches),
            vec![
                ("a".to_string(), 10),
                ("b".to_string(), 2),
                ("c".to_string(), 3)
            ],
            "hive_style={hive_style}"
        );
        assert!(dir.path().join(default_partition).is_dir());
    }
}

#[tokio::test]
async fn test_empty_record_key_rejected() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    let err = table
        .append([batch(vec![("a", 1), ("", 2)])])
        .await
        .unwrap_err();
    assert!(
        matches!(&err, CoreError::Write(msg) if msg.contains("empty")),
        "{err}"
    );
    let err = table.upsert([batch(vec![("", 2)])]).await.unwrap_err();
    assert!(
        matches!(&err, CoreError::Write(msg) if msg.contains("empty")),
        "{err}"
    );
    assert!(
        table.timeline.get_latest_commit_timestamp().is_err(),
        "a rejected key must not leave a commit"
    );
}

/// Spark writes its metadata table files with the `_hoodie_*` meta fields, and
/// readers merge a metadata file slice to its base file's schema, so rs blocks
/// appended to Spark's base files must carry those fields too.
#[tokio::test]
async fn upserts_into_spark_written_table_read_through_metadata_table() {
    let data_rows = |batches: Vec<RecordBatch>| -> RecordBatch {
        let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let data_columns: Vec<usize> = (0..batch.num_columns())
            .filter(|&i| !batch.schema().field(i).name().starts_with("_hoodie_"))
            .collect();
        batch.project(&data_columns).unwrap()
    };
    for format in [hudi_test::TableFormat::Cow, hudi_test::TableFormat::MorAvro] {
        let path = hudi_test::SampleTable::V9TxnsSimpleOverwrite.path_fresh(format);
        let mut table = Table::new(&path).await.unwrap();
        let rows = data_rows(table.read(&ReadOptions::new()).await.unwrap());
        assert!(rows.num_rows() > 0);

        // The second upsert plans through the metadata table, merging Spark's
        // base files with the first upsert's log blocks.
        table.upsert([rows.clone()]).await.unwrap();
        // Spark leaves the timeline timezone unset, which Java reads as LOCAL.
        let local_now = || chrono::Local::now().format("%Y%m%d%H%M%S%3f").to_string();
        let before = local_now();
        let instant = table.upsert([rows.clone()]).await.unwrap().instant;
        let after = local_now();
        assert!(
            before <= instant && instant <= after,
            "{format:?}: instant {instant} must be local time in [{before}, {after}]"
        );

        let table = Table::new(&path).await.unwrap();
        let after = data_rows(table.read(&ReadOptions::new()).await.unwrap());
        assert_eq!(after.num_rows(), rows.num_rows(), "{format:?}");
    }
}

fn assert_send<F: std::future::Future + Send>(_: F) {}

/// Compile-time check that every public write future can be spawned on a
/// multi-threaded runtime.
#[allow(dead_code)]
fn write_futures_are_send(table: &mut Table) {
    let key = HoodieKey {
        record_key: "a".to_string(),
        partition_path: String::new(),
    };
    assert_send(Table::create("unused").create());
    assert_send(table.append([sample_batch()]));
    assert_send(table.upsert([sample_batch()]));
    assert_send(table.upsert_with([sample_batch()], UpsertOptions::default()));
    assert_send(table.overwrite([sample_batch()]));
    assert_send(table.dynamic_partition_overwrite([sample_batch()]));
    assert_send(table.delete("id = 'a'"));
    assert_send(table.delete_keys([key]));
    assert_send(table.update("id = 'a'", sample_batch()));
}

/// Footer key-value metadata and record keys of every base file under `dir`.
fn base_file_footers(dir: &std::path::Path) -> Vec<(HashMap<String, String>, Vec<String>)> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut footers = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(&path).unwrap().map(Result::unwrap) {
            let path = entry.path();
            if path.is_dir() && entry.file_name() != ".hoodie" {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                let builder =
                    ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap())
                        .unwrap();
                let footer = builder
                    .metadata()
                    .file_metadata()
                    .key_value_metadata()
                    .into_iter()
                    .flatten()
                    .map(|kv| (kv.key.clone(), kv.value.clone().unwrap_or_default()))
                    .collect();
                let keys = builder
                    .build()
                    .unwrap()
                    .flat_map(|batch| {
                        let batch = batch.unwrap();
                        let keys = batch.column_by_name("_hoodie_record_key").map(|keys| {
                            keys.as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap()
                                .iter()
                                .map(|key| key.unwrap().to_string())
                                .collect::<Vec<_>>()
                        });
                        keys.unwrap_or_default()
                    })
                    .collect();
                footers.push((footer, keys));
            }
        }
    }
    footers
}

/// Every base file carries the footer Java's bloom index reads: the bloom
/// filter at Java's default sizing holding every record key, its type code,
/// and the record key range, which an empty file leaves out.
#[tokio::test]
async fn test_base_file_footer_carries_bloom_filter_and_key_range() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("trips")
            .with_table_type(table_type.clone())
            .with_partition_fields(["city"])
            .with_record_key_fields(["id"])
            .create()
            .await
            .unwrap();
        table
            .upsert([partitioned_batch(vec![
                ("k3", "sf", 1),
                ("k1", "sf", 2),
                ("k2", "nyc", 3),
            ])])
            .await
            .unwrap();
        table
            .upsert([partitioned_batch(vec![
                ("k1", "sf", 20),
                ("k0", "sf", 5),
                ("k4", "nyc", 6),
            ])])
            .await
            .unwrap();
        table.delete("id IN ('k2', 'k4')").await.unwrap();

        let footers = base_file_footers(dir.path());
        if table_type == TableTypeValue::CopyOnWrite {
            // Insert, merge rewrite, and the delete's empty rewrite of nyc.
            assert!(footers.len() >= 4, "saw {} base files", footers.len());
            assert!(footers.iter().any(|(_, keys)| keys.is_empty()));
        } else {
            assert!(footers.len() >= 2, "saw {} base files", footers.len());
        }
        for (footer, keys) in &footers {
            assert_record_key_footer(footer, keys);
        }
    }
}

fn assert_record_key_footer(footer: &HashMap<String, String>, keys: &[String]) {
    use base64::Engine;
    assert_eq!(
        footer
            .get("hoodie_bloom_filter_type_code")
            .map(String::as_str),
        Some("DYNAMIC_V0")
    );
    let bloom = base64::engine::general_purpose::STANDARD
        .decode(&footer["org.apache.hudi.bloomfilter"])
        .unwrap();
    let int_at = |offset: usize| i32::from_be_bytes(bloom[offset..offset + 4].try_into().unwrap());
    // Version, hash count, hash type, bit count: Java's 60000 entries at fpp 1e-9.
    assert_eq!(
        (int_at(0), int_at(4), bloom[8], int_at(9)),
        (-1, 30, 1, 2_587_966)
    );
    // Entries per row, keys added, row count.
    assert_eq!(
        (int_at(13), int_at(17), int_at(21)),
        (60_000, keys.len() as i32, 1)
    );
    assert_eq!(footer.get("hoodie_min_record_key"), keys.iter().min());
    assert_eq!(footer.get("hoodie_max_record_key"), keys.iter().max());
}

/// A table without meta fields writes no bloom filter, as Java does.
#[tokio::test]
async fn test_base_file_footer_omits_bloom_filter_without_meta_fields() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_metadata(false)
        .with_record_index(false)
        .with_column_stats(false)
        .with_populates_meta_fields(false)
        .create()
        .await
        .unwrap();
    table.append([sample_batch()]).await.unwrap();
    let footers = base_file_footers(dir.path());
    assert_eq!(footers.len(), 1);
    assert!(
        footers[0].0.keys().all(|key| key == "ARROW:schema"),
        "{:?}",
        footers[0].0.keys()
    );
}
