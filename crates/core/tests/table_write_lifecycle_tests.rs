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
        mdt_after[0].ends_with(".parquet") && mdt_after[0].contains('_'),
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

fn partitioned_ordered_batch(rows: Vec<(&str, &str, i64, i64)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("event_time", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(id, _, _, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, city, _, _)| *city).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, _, value, _)| *value).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, _, event_time)| *event_time)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_create_defaults_enable_mdt_and_record_index() {
    let dir = tempdir().unwrap();
    let table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    assert!(table.is_metadata_table_enabled());
    assert!(hudi_core::index::is_record_index_enabled(&table));
    assert!(
        dir.path()
            .join(".hoodie/metadata/record_index")
            .is_dir()
    );
    let rli_bases: Vec<_> = std::fs::read_dir(dir.path().join(".hoodie/metadata/record_index"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with(".hfile") && n.starts_with("record-index-"))
        .collect();
    assert_eq!(
        rli_bases.len(),
        10,
        "Java default RLI min file groups is 10: {rli_bases:?}"
    );
}

#[tokio::test]
async fn test_record_index_requires_metadata() {
    let dir = tempdir().unwrap();
    let err = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_metadata(false)
        .with_record_index(true)
        .create()
        .await
        .unwrap_err();
    assert!(matches!(err, hudi_core::error::CoreError::Write(_)));
    assert!(err.to_string().contains("record index requires"));
}

#[tokio::test]
async fn test_record_index_off_falls_back_to_simple_index() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_record_index(false)
        .create()
        .await
        .unwrap();
    assert!(table.is_metadata_table_enabled());
    assert!(!hudi_core::index::is_record_index_enabled(&table));
    assert!(
        !dir.path()
            .join(".hoodie/metadata/record_index")
            .is_dir()
    );

    table
        .append([batch(vec![("a", 1), ("b", 2)])])
        .await
        .unwrap();
    table
        .upsert([batch(vec![("a", 10), ("c", 3)])])
        .await
        .unwrap();
    assert_snapshot_async(&table, vec![("a", 10), ("b", 2), ("c", 3)]).await;
}

#[tokio::test]
async fn test_partitioned_cow_hive_style_keeps_partition_columns() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let base_uri = root.to_str().unwrap();
    let mut table = Table::create(base_uri)
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
        ])])
        .await
        .unwrap();
    assert!(root.join("city=sf").is_dir());
    assert!(root.join("city=nyc").is_dir());
    assert!(
        root.join("city=sf/.hoodie_partition_metadata").is_file(),
        "Spark FS listing requires .hoodie_partition_metadata"
    );
    assert!(root.join("city=nyc/.hoodie_partition_metadata").is_file());
    let meta = std::fs::read_to_string(root.join("city=sf/.hoodie_partition_metadata")).unwrap();
    assert!(meta.contains("commitTime="));
    assert!(meta.contains("partitionDepth=1"));

    table
        .upsert([partitioned_batch(vec![
            ("a", "sf", 10),
            ("c", "sf", 3),
        ])])
        .await
        .unwrap();
    table
        .delete_keys([HoodieKey {
            record_key: "b".to_string(),
            partition_path: "city=nyc".to_string(),
        }])
        .await
        .unwrap();

    let batches = table.read(&ReadOptions::new()).await.unwrap();
    assert_eq!(
        rows_by_id(&batches),
        vec![("a".to_string(), 10), ("c".to_string(), 3)]
    );
    // Partition column retained in data files (not stripped).
    let city = batches[0]
        .column_by_name("city")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(city.iter().flatten().any(|v| v == "sf"));

    let index = hudi_core::index::RecordIndex;
    use hudi_core::index::HoodieIndex;
    let tagged = index
        .tag_location(
            &table,
            &[HoodieKey {
                record_key: "a".to_string(),
                partition_path: "city=sf".to_string(),
            }],
        )
        .await
        .unwrap();
    let location = tagged
        .values()
        .next()
        .and_then(|v| v.as_ref())
        .expect("RLI should locate key a");
    assert_eq!(location.partition_path, "city=sf");
}

#[tokio::test]
async fn test_partitioned_mor_writes_logs_under_partition() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let mut table = Table::create(root.to_str().unwrap())
        .with_table_name("trips")
        .with_table_type(TableTypeValue::MergeOnRead)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["event_time"])
        .with_populates_meta_fields(true)
        .create()
        .await
        .unwrap();

    table
        .append([partitioned_ordered_batch(vec![
            ("a", "sf", 1, 1),
            ("b", "nyc", 2, 1),
        ])])
        .await
        .unwrap();

    table
        .upsert([partitioned_ordered_batch(vec![("a", "sf", 10, 2)])])
        .await
        .unwrap();

    let logs = list_data_log_files(root);
    assert_eq!(logs.len(), 1);
    assert!(
        logs[0].to_string_lossy().contains("city=sf"),
        "MOR log should live under hive-style partition: {:?}",
        logs[0]
    );
    assert_snapshot_async(&table, vec![("a", 10), ("b", 2)]).await;
}


#[tokio::test]
async fn test_create_props_match_spark_shape() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let _table = Table::create(root.to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["event_time"])
        .create()
        .await
        .unwrap();

    let props = std::fs::read_to_string(root.join(".hoodie/hoodie.properties")).unwrap();
    for key in [
        "hoodie.table.checksum=",
        "hoodie.database.name=default",
        "hoodie.table.base.file.format=PARQUET",
        "hoodie.populate.meta.fields=true",
        "hoodie.timeline.path=timeline",
        "hoodie.timeline.history.path=history",
        "hoodie.table.keygenerator.type=SIMPLE",
        "hoodie.table.metadata.partitions=files,record_index",
        "hoodie.table.version=8",
        "hoodie.timeline.layout.version=2",
        "hoodie.record.merge.mode=EVENT_TIME_ORDERING",
    ] {
        assert!(props.contains(key), "missing {key} in:\n{props}");
    }
    assert!(
        !props.contains("hoodie.metadata.enable="),
        "must not invent write-side hoodie.metadata.enable in table props:\n{props}"
    );
    assert!(
        !props.contains("hoodie.metadata.record.index.enable="),
        "must not invent write-side hoodie.metadata.record.index.enable in table props:\n{props}"
    );
    // Checksum must match Java CRC32("default.trips")
    assert!(props.contains("hoodie.table.checksum=2200697520"), "{props}");

    let mdt_props =
        std::fs::read_to_string(root.join(".hoodie/metadata/.hoodie/hoodie.properties")).unwrap();
    assert!(mdt_props.contains("hoodie.table.keygenerator.type=HOODIE_TABLE_METADATA"));
    assert!(mdt_props.contains("hoodie.compaction.payload.class=org.apache.hudi.metadata.HoodieMetadataPayload"));
    assert!(mdt_props.contains("hoodie.table.checksum=1249152950"));

    let mdt_timeline = std::fs::read_dir(root.join(".hoodie/metadata/.hoodie/timeline"))
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<Vec<_>>();
    assert!(
        mdt_timeline.iter().any(|n| n.ends_with(".deltacommit.requested")),
        "{mdt_timeline:?}"
    );
    assert!(
        mdt_timeline.iter().any(|n| n.ends_with(".deltacommit")),
        "{mdt_timeline:?}"
    );

    // Optional dump for Spark side-by-side inspection.
    if let Ok(out) = std::env::var("HUDI_RS_INTEROP_OUT") {
        let dest = std::path::PathBuf::from(&out);
        let _ = std::fs::remove_dir_all(&dest);
        std::fs::create_dir_all(&dest).unwrap();
        // Fresh append-only create (ordering fields force overwrite strategy which
        // rejects append).
        let mut table = Table::create(dest.to_str().unwrap())
            .with_table_name("trips")
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .create()
            .await
            .unwrap();
        table
            .append([partitioned_ordered_batch(vec![
                ("a", "sf", 1, 1),
                ("b", "nyc", 2, 1),
                ("c", "la", 3, 1),
                ("d", "chi", 4, 1),
                ("e", "sf", 5, 1),
                ("f", "nyc", 6, 1),
                ("g", "la", 7, 1),
                ("h", "chi", 8, 1),
                ("i", "sf", 9, 1),
                ("j", "nyc", 10, 1),
            ])])
            .await
            .unwrap();
        eprintln!("wrote interop table to {}", dest.display());
    }
}

#[tokio::test]
async fn test_append_splits_files_by_max_file_size() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let mut table = Table::create(root.to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_option("hoodie.parquet.max.file.size", "1")
        .with_option("hoodie.copyonwrite.record.size.estimate", "1")
        .create()
        .await
        .unwrap();

    // 6 rows in one partition → with max_records=1, expect 6 base files.
    table
        .append([partitioned_ordered_batch(vec![
            ("k0", "sf", 0, 0),
            ("k1", "sf", 1, 1),
            ("k2", "sf", 2, 2),
            ("k3", "sf", 3, 3),
            ("k4", "sf", 4, 4),
            ("k5", "sf", 5, 5),
        ])])
        .await
        .unwrap();

    let parquet_count = walkdir_count_parquet(root);
    assert!(
        parquet_count >= 6,
        "expected file-size splitting to create >=6 files, got {parquet_count}"
    );
}

#[tokio::test]
async fn test_cow_upsert_preserves_unrelated_file_groups() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let mut table = Table::create(root.to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_metadata(false)
        .with_option("hoodie.parquet.max.file.size", "1")
        .with_option("hoodie.copyonwrite.record.size.estimate", "1")
        .create()
        .await
        .unwrap();

    table
        .append([partitioned_batch(vec![
            ("a", "sf", 1),
            ("b", "sf", 2),
            ("c", "nyc", 3),
            ("d", "nyc", 4),
        ])])
        .await
        .unwrap();
    let before = walkdir_count_parquet(root);
    assert!(
        before >= 2,
        "expected multiple base files before upsert, got {before}"
    );

    // Touch only key `a` in city=sf — nyc file groups must remain.
    table
        .upsert([partitioned_batch(vec![("a", "sf", 10)])])
        .await
        .unwrap();
    let after = walkdir_count_parquet(root);
    assert!(
        after >= before,
        "upsert must not collapse unrelated file groups (before={before}, after={after})"
    );
    assert_eq!(
        rows_by_id(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![
            ("a".to_string(), 10),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
            ("d".to_string(), 4),
        ]
    );
}

fn walkdir_count_parquet(root: &std::path::Path) -> usize {
    let mut n = 0;
    for entry in std::fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() && entry.file_name() != ".hoodie" {
            n += walkdir_count_parquet(&path);
        } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            n += 1;
        }
    }
    n
}
