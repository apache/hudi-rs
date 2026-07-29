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

//! Lifecycle write tests inspired by pyIceberg write coverage:
//! append → upsert → delete → overwrite, with on-disk file-layout assertions
//! (base vs log) and metadata-table consistency checks.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hudi_core::config::table::TableTypeValue;
use hudi_core::index::HoodieKey;
use hudi_core::table::partition::PartitionPruner;
use hudi_core::table::{QueryType, ReadOptions, Table};
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

#[derive(Debug, Default, PartialEq, Eq)]
struct FileLayout {
    base_parquet: usize,
    log_files: usize,
    commits: usize,
    deltacommits: usize,
    replacecommits: usize,
}

fn visit_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if path.is_dir() {
            visit_files(&path, out);
        } else if path.is_file() {
            out.push(path);
        }
    }
}

fn data_layout(table_root: &Path) -> FileLayout {
    let mut files = Vec::new();
    visit_files(table_root, &mut files);
    let mut layout = FileLayout::default();
    for path in files {
        // Skip metadata table internals for data-layout counts.
        if path.components().any(|c| c.as_os_str() == "metadata") {
            continue;
        }
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.ends_with(".parquet") && !name.starts_with('.') {
            layout.base_parquet += 1;
        } else if name.contains(".log.") && name.starts_with('.') {
            layout.log_files += 1;
        } else if name.ends_with(".replacecommit") {
            layout.replacecommits += 1;
        } else if name.ends_with(".deltacommit") {
            layout.deltacommits += 1;
        } else if name.ends_with(".commit") {
            layout.commits += 1;
        }
    }
    layout
}

fn list_data_log_files(table_root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    visit_files(table_root, &mut files);
    files
        .into_iter()
        .filter(|path| {
            if path.components().any(|c| c.as_os_str() == "metadata") {
                return false;
            }
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with('.') && n.contains(".log."))
        })
        .collect()
}

async fn assert_snapshot_async(table: &Table, expected: Vec<(&str, i64)>) {
    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let expected = expected
        .into_iter()
        .map(|(id, value)| (id.to_string(), value))
        .collect::<Vec<_>>();
    assert_eq!(rows_by_id(&batches), expected);
}

async fn mdt_active_files(table: &Table) -> Vec<String> {
    assert!(table.is_metadata_table_enabled());
    let partition_schema = table.get_partition_schema().await.unwrap();
    let pruner = PartitionPruner::new(&[], &partition_schema, table.hudi_configs.as_ref()).unwrap();
    let records = table
        .read_metadata_table_files_partition(&pruner)
        .await
        .unwrap();
    let files = records.get("").expect("non-partitioned MDT files record");
    let mut names = files.active_file_names();
    names.sort();
    names.into_iter().map(str::to_string).collect()
}

#[tokio::test]
async fn test_cow_lifecycle_append_upsert_delete_overwrite_with_mdt() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let base_uri = root.to_str().unwrap();

    // 1) Initialize with metadata enabled; append builds MDT files partition.
    let mut table = Table::create(base_uri)
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_metadata(true)
        .create()
        .await
        .unwrap();
    assert!(table.is_metadata_table_enabled());
    assert!(root.join(".hoodie/metadata/.hoodie/hoodie.properties").is_file());

    let append = table
        .append([batch(vec![("a", 1), ("b", 2), ("c", 3)])])
        .await
        .unwrap();
    assert_eq!(
        data_layout(root),
        FileLayout {
            base_parquet: 1,
            log_files: 0,
            commits: 1,
            deltacommits: 0,
            replacecommits: 0,
        }
    );
    assert!(mdt_active_files(&table).await.contains(&append.base_file_path));
    assert_snapshot_async(&table, vec![("a", 1), ("b", 2), ("c", 3)]).await;

    // 2) Upsert over time: update + insert; state stays consistent.
    let upsert = table
        .upsert([batch(vec![("a", 10), ("d", 4)])])
        .await
        .unwrap();
    assert_eq!(upsert.num_updates, 1);
    assert_eq!(upsert.num_inserts, 1);
    // COW rewrite replaces bases via replacecommit — still no log files.
    let layout = data_layout(root);
    assert_eq!(layout.log_files, 0);
    assert!(layout.replacecommits >= 1 || layout.commits >= 2);
    let mdt_after = mdt_active_files(&table).await;
    assert_eq!(
        mdt_after.len(),
        1,
        "MDT should list the replacement base only: {mdt_after:?}"
    );
    assert!(
        mdt_after[0].starts_with("rewrite-"),
        "MDT active file should be the rewrite base: {mdt_after:?}"
    );
    assert_snapshot_async(&table, vec![("a", 10), ("b", 2), ("c", 3), ("d", 4)]).await;

    // 3) Delete keeps remaining rows only.
    let deleted = table.delete("id = 'b'").await.unwrap();
    assert_eq!(deleted.num_deletes, 1);
    assert_eq!(data_layout(root).log_files, 0);
    assert_snapshot_async(&table, vec![("a", 10), ("c", 3), ("d", 4)]).await;

    let deleted = table
        .delete_keys([HoodieKey {
            record_key: "c".to_string(),
            partition_path: String::new(),
        }])
        .await
        .unwrap();
    assert_eq!(deleted.num_deletes, 1);
    assert_snapshot_async(&table, vec![("a", 10), ("d", 4)]).await;

    // 4) Overwrite replaces table contents (pyIceberg-style).
    table
        .overwrite([batch(vec![("z", 99), ("y", 98)])])
        .await
        .unwrap();
    assert_eq!(data_layout(root).log_files, 0);
    assert_snapshot_async(&table, vec![("y", 98), ("z", 99)]).await;

    // Reopen and confirm durable state.
    let reopened = Table::new(base_uri).await.unwrap();
    assert_snapshot_async(&reopened, vec![("y", 98), ("z", 99)]).await;
    assert!(reopened.is_metadata_table_enabled());
}

#[tokio::test]
async fn test_mor_lifecycle_asserts_base_and_log_file_counts() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let base_uri = root.to_str().unwrap();

    let mut table = Table::create(base_uri)
        .with_table_name("trips")
        .with_table_type(TableTypeValue::MergeOnRead)
        .with_record_key_fields(["id"])
        .with_ordering_fields(["event_time"])
        .with_populates_meta_fields(true)
        .create()
        .await
        .unwrap();

    // Append → base parquet + deltacommit, no logs yet.
    let append = table
        .append([ordered_batch(vec![("a", 1, 1), ("b", 2, 1)])])
        .await
        .unwrap();
    assert!(append.commit_relative_path.ends_with(".deltacommit"));
    assert_eq!(
        data_layout(root),
        FileLayout {
            base_parquet: 1,
            log_files: 0,
            commits: 0,
            deltacommits: 1,
            replacecommits: 0,
        }
    );
    assert_snapshot_async(&table, vec![("a", 1), ("b", 2)]).await;

    // Upsert update-only → must write a log file against existing file group.
    let update = table
        .upsert([ordered_batch(vec![("a", 10, 2)])])
        .await
        .unwrap();
    assert_eq!(update.num_updates, 1);
    assert_eq!(update.num_inserts, 0);
    let layout = data_layout(root);
    assert_eq!(layout.base_parquet, 1, "update-only upsert must not add bases");
    assert_eq!(layout.log_files, 1, "MOR update must append a parquet log");
    assert_eq!(layout.deltacommits, 2);
    assert_snapshot_async(&table, vec![("a", 10), ("b", 2)]).await;

    // List the actual log path for clarity in failures.
    let logs = list_data_log_files(root);
    assert_eq!(logs.len(), 1);
    assert!(
        logs[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains(&format!("_{}.log.", update.instant)),
        "log file should include upsert instant: {:?}",
        logs[0]
    );

    // Upsert insert-only → new base file, no additional log for inserts.
    let insert = table
        .upsert([ordered_batch(vec![("c", 3, 1)])])
        .await
        .unwrap();
    assert_eq!(insert.num_updates, 0);
    assert_eq!(insert.num_inserts, 1);
    let layout = data_layout(root);
    assert_eq!(layout.base_parquet, 2, "MOR insert should create a new base");
    assert_eq!(layout.log_files, 1, "insert-only upsert should not add logs");
    assert_eq!(layout.deltacommits, 3);
    assert_snapshot_async(&table, vec![("a", 10), ("b", 2), ("c", 3)]).await;

    // Mixed upsert: update existing + insert new → +1 log and +1 base.
    let mixed = table
        .upsert([ordered_batch(vec![("b", 20, 2), ("d", 4, 1)])])
        .await
        .unwrap();
    assert_eq!(mixed.num_updates, 1);
    assert_eq!(mixed.num_inserts, 1);
    let layout = data_layout(root);
    assert_eq!(layout.base_parquet, 3);
    assert_eq!(layout.log_files, 2);
    assert_eq!(layout.deltacommits, 4);
    assert_snapshot_async(&table, vec![("a", 10), ("b", 20), ("c", 3), ("d", 4)]).await;

    // Second update on same key → another log (log accumulation before compaction).
    table
        .upsert([ordered_batch(vec![("a", 11, 3)])])
        .await
        .unwrap();
    let layout = data_layout(root);
    assert_eq!(layout.base_parquet, 3);
    assert_eq!(layout.log_files, 3);
    assert_snapshot_async(&table, vec![("a", 11), ("b", 20), ("c", 3), ("d", 4)]).await;

    // Delete → delete log block file(s), snapshot excludes key.
    let deleted = table
        .delete_keys([HoodieKey {
            record_key: "c".to_string(),
            partition_path: String::new(),
        }])
        .await
        .unwrap();
    assert_eq!(deleted.num_deletes, 1);
    let layout = data_layout(root);
    assert_eq!(layout.base_parquet, 3);
    assert_eq!(layout.log_files, 4, "MOR delete must append a log file");
    assert_snapshot_async(&table, vec![("a", 11), ("b", 20), ("d", 4)]).await;

    // Read-optimized should ignore logs and still see pre-log base values for updated keys.
    let ro = table
        .read(&ReadOptions::new().with_hudi_option("hoodie.read.use.read_optimized.mode", "true"))
        .await
        .unwrap();
    // RO sees bases only: append(a=1,b=2) + insert(c=3) + insert(d=4); updates in logs ignored.
    assert_eq!(
        rows_by_id(&ro),
        vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
            ("d".to_string(), 4),
        ]
    );

    // Incremental from earliest includes all committed data as of latest (merged).
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
        vec![
            ("a".to_string(), 11),
            ("b".to_string(), 20),
            ("d".to_string(), 4),
        ]
    );

    // File slices from planner should report logs for updated groups.
    let slices = table.get_file_slices(&ReadOptions::new()).await.unwrap();
    let total_logs: usize = slices.iter().map(|s| s.log_files.len()).sum();
    assert!(
        total_logs >= 4,
        "planner should surface MOR log files on slices, got {total_logs} across {} slices",
        slices.len()
    );

    // Durable after reopen.
    let reopened = Table::new(base_uri).await.unwrap();
    assert_snapshot_async(&reopened, vec![("a", 11), ("b", 20), ("d", 4)]).await;
    assert_eq!(data_layout(root).log_files, 4);
}

#[tokio::test]
async fn test_cow_and_mor_mutable_ops_roundtrip_like_pyiceberg() {
    // Mirrors pyIceberg-style verb coverage: append, upsert, delete, overwrite
    // for both table types, asserting read-after-write consistency.
    for table_type in [
        TableTypeValue::CopyOnWrite,
        TableTypeValue::MergeOnRead,
    ] {
        let dir = tempdir().unwrap();
        let base_uri = dir.path().to_str().unwrap();
        let mut builder = Table::create(base_uri)
            .with_table_name("trips")
            .with_table_type(table_type.clone())
            .with_record_key_fields(["id"]);
        if table_type == TableTypeValue::MergeOnRead {
            builder = builder
                .with_ordering_fields(["event_time"])
                .with_populates_meta_fields(true);
        }
        let mut table = builder.create().await.unwrap();

        if table_type == TableTypeValue::CopyOnWrite {
            table
                .append([batch(vec![("a", 1), ("b", 2)])])
                .await
                .unwrap();
            table
                .upsert([batch(vec![("a", 10), ("c", 3)])])
                .await
                .unwrap();
            table.delete("id = 'b'").await.unwrap();
            assert_snapshot_async(&table, vec![("a", 10), ("c", 3)]).await;
            table.overwrite([batch(vec![("z", 9)])]).await.unwrap();
            assert_snapshot_async(&table, vec![("z", 9)]).await;
            assert_eq!(data_layout(dir.path()).log_files, 0);
        } else {
            table
                .append([ordered_batch(vec![("a", 1, 1), ("b", 2, 1)])])
                .await
                .unwrap();
            table
                .upsert([ordered_batch(vec![("a", 10, 2), ("c", 3, 1)])])
                .await
                .unwrap();
            table
                .delete_keys([HoodieKey {
                    record_key: "b".to_string(),
                    partition_path: String::new(),
                }])
                .await
                .unwrap();
            assert_snapshot_async(&table, vec![("a", 10), ("c", 3)]).await;
            let layout = data_layout(dir.path());
            assert!(layout.base_parquet >= 1);
            assert!(
                layout.log_files >= 2,
                "MOR mutable path should produce update + delete logs, got {layout:?}"
            );
            // overwrite not yet supported for MOR — skip
        }
    }
}
