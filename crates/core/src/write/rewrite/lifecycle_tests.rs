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

/// Fabricate a committed log-only MOR file slice (data log block + fenced
/// and completed deltacommit, no base file) — the layout a Java writer
/// with log-only inserts produces, which rs verbs never create themselves.
async fn fabricate_log_only_slice(table: &Table, partition: &str, rows: RecordBatch) {
    let storage = table.file_system_view.storage.clone();
    let instant = crate::write::append::generate_instant_time().await;
    crate::write::fence_timeline_instant(
        storage.as_ref(),
        &super::timeline_dir(table),
        &instant,
        crate::timeline::instant::Action::DeltaCommit,
        Vec::new(),
        crate::write::inflight_commit_metadata_bytes("UPSERT", true).unwrap(),
    )
    .await
    .unwrap();
    let file_id = crate::write::new_file_id();
    let log_name = format!(".{file_id}_{instant}.log.1_0-0-0");
    let rel = format!("{partition}/{log_name}");
    crate::write::ensure_partition_metadata(storage.as_ref(), partition, &instant)
        .await
        .unwrap();
    let prepared = super::prepare_batches_for_write(
        &table.hudi_configs,
        std::slice::from_ref(&rows),
        &instant,
        &log_name,
    )
    .unwrap();
    let schema = prepared[0].schema();
    let schema_json = crate::write::append::arrow_schema_to_avro_json(schema.as_ref()).unwrap();
    crate::write::log_file_task(
        storage.clone(),
        crate::write::append::parquet_writer_props(table),
        prepared,
        rel.clone(),
        instant.clone(),
        schema_json,
        false,
    )
    .await
    .unwrap();
    let stat = crate::metadata::commit::HoodieWriteStat {
        file_id: Some(file_id),
        path: Some(rel),
        base_file: Some(String::new()),
        log_files: Some(vec![log_name]),
        partition_path: Some(partition.to_string()),
        num_writes: Some(rows.num_rows() as i64),
        ..Default::default()
    };
    let bytes = crate::write::commit_metadata_bytes(
        table,
        "UPSERT",
        crate::write::stats_by_partition(vec![stat]),
        None,
        schema.as_ref(),
    )
    .unwrap();
    let completion = if super::is_layout_two(table) {
        Some(crate::write::append::generate_instant_time().await)
    } else {
        None
    };
    let completed = crate::timeline::instant::Instant::new_completed(
        instant.clone(),
        crate::timeline::instant::Action::DeltaCommit,
        completion,
    )
    .unwrap();
    storage
        .put_file_if_absent(
            &completed
                .relative_path_with_base(&super::timeline_dir(table))
                .unwrap(),
            bytes,
        )
        .await
        .unwrap();
}

/// MOR coverage for the replace-commit tombstones: keys living in a
/// base+log slice and in a log-only slice must be tombstoned too — those
/// read through different reader paths than a plain COW base file.
#[tokio::test]
async fn test_replace_emits_rli_deletes_for_mor_slices() {
    let dir = tempfile::tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_table_type(TableTypeValue::MergeOnRead)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["ts"])
        .create()
        .await
        .unwrap();
    // Base + log slice: "a" in the base, updated through a log block.
    table
        .append([batch(&[("a", "sf", 1, 10), ("b", "sf", 1, 20)])])
        .await
        .unwrap();
    table.upsert([batch(&[("a", "sf", 2, 11)])]).await.unwrap();
    table.reload_timeline_for_write().await.unwrap();

    table
        .overwrite([batch(&[("z", "sf", 3, 99)])])
        .await
        .unwrap();
    table.reload_timeline_for_write().await.unwrap();

    use crate::index::HoodieIndex;
    let dropped: Vec<crate::index::HoodieKey> = ["a", "b"]
        .iter()
        .map(|k| crate::index::HoodieKey {
            record_key: k.to_string(),
            partition_path: "city=sf".to_string(),
        })
        .collect();
    let located = crate::index::for_table(&table)
        .tag_location(&table, &dropped)
        .await
        .unwrap();
    for key in &dropped {
        assert!(
            located.get(key).and_then(Option::as_ref).is_none(),
            "MOR: dropped key '{}' must not stay indexed",
            key.record_key
        );
    }
}

/// A replace commit must remove index entries for keys it drops, or the
/// RLI points at replaced file groups forever (no cleaner removes them)
/// and a Java writer routes updates into dead groups.
#[tokio::test]
async fn test_replace_verbs_emit_rli_deletes_for_dropped_keys() {
    for dpo in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_ordering_fields(["ts"])
            .create()
            .await
            .unwrap();
        table
            .append([batch(&[("a", "sf", 1, 10), ("b", "sf", 1, 20)])])
            .await
            .unwrap();
        let replacement = batch(&[("a", "sf", 2, 11)]);
        if dpo {
            table
                .dynamic_partition_overwrite([replacement])
                .await
                .unwrap();
        } else {
            table.overwrite([replacement]).await.unwrap();
        }
        table.reload_timeline_for_write().await.unwrap();

        use crate::index::HoodieIndex;
        let dropped = crate::index::HoodieKey {
            record_key: "b".to_string(),
            partition_path: "city=sf".to_string(),
        };
        let kept = crate::index::HoodieKey {
            record_key: "a".to_string(),
            partition_path: "city=sf".to_string(),
        };
        let located = crate::index::for_table(&table)
            .tag_location(&table, &[dropped.clone(), kept.clone()])
            .await
            .unwrap();
        assert!(
            located.get(&dropped).and_then(Option::as_ref).is_none(),
            "dpo={dpo}: dropped key must not stay indexed"
        );
        assert!(
            located.get(&kept).and_then(Option::as_ref).is_some(),
            "dpo={dpo}: surviving key must stay indexed"
        );
    }
}

/// A REJECTED cross-partition move must leave the record index pointing
/// at the surviving old partition, not the losing incoming value.
#[tokio::test]
async fn test_rejected_move_keeps_rli_partition() {
    let dir = tempfile::tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["ts"])
        .create()
        .await
        .unwrap();
    table.append([batch(&[("a", "sf", 5, 100)])]).await.unwrap();
    // Stale move: rejected, row stays in sf.
    table
        .upsert([batch(&[("a", "nyc", 3, 333)])])
        .await
        .unwrap();
    table.reload_timeline_for_write().await.unwrap();
    let key = crate::index::HoodieKey {
        record_key: "a".to_string(),
        partition_path: "city=nyc".to_string(),
    };
    use crate::index::HoodieIndex;
    let locations = crate::index::for_table(&table)
        .tag_location(&table, std::slice::from_ref(&key))
        .await
        .unwrap();
    let location = locations
        .get(&key)
        .and_then(Option::as_ref)
        .expect("key must stay indexed");
    assert_eq!(
        location.partition_path, "city=sf",
        "index must keep the surviving old partition"
    );
}

/// Overwrite and dynamic partition overwrite must replace log-only MOR
/// file groups too: their ids must land in partitionToReplaceFileIds or
/// the old rows outlive the overwrite.
#[tokio::test]
async fn test_replace_verbs_exclude_log_only_mor_groups() {
    for dpo in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_table_type(TableTypeValue::MergeOnRead)
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_ordering_fields(["ts"])
            .with_metadata(false)
            .create()
            .await
            .unwrap();
        table.append([batch(&[("a", "sf", 1, 10)])]).await.unwrap();
        fabricate_log_only_slice(&table, "city=sf", batch(&[("b", "sf", 1, 20)])).await;
        table.reload_timeline_for_write().await.unwrap();
        assert_eq!(
            rows(&table).await,
            2,
            "fabricated log-only row must be readable"
        );

        let replacement = batch(&[("c", "sf", 2, 30)]);
        if dpo {
            table
                .dynamic_partition_overwrite([replacement])
                .await
                .unwrap();
        } else {
            table.overwrite([replacement]).await.unwrap();
        }
        let batches = table.read(&ReadOptions::new()).await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 1, "dpo={dpo}: log-only group must be replaced");
        let ids = batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.value(0), "c", "dpo={dpo}");
    }
}

use crate::config::table::TableTypeValue;
use crate::table::{ReadOptions, Table, UpsertOptions};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

fn batch(rows: &[(&str, &str, i64, i64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
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

async fn rows(table: &Table) -> usize {
    table
        .read(&ReadOptions::new())
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

/// Full COW verb lifecycle from inside the crate: append, keyed upsert
/// (update + insert + packing), partial upsert, expression update, keyed
/// and expression delete, dynamic partition overwrite, full overwrite.
#[tokio::test]
async fn test_cow_verb_lifecycle_in_crate() {
    let dir = tempfile::tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["ts"])
        .create()
        .await
        .unwrap();

    table
        .append([batch(&[
            ("a", "sf", 1, 10),
            ("b", "sf", 1, 20),
            ("c", "ny", 1, 30),
        ])])
        .await
        .unwrap();
    assert_eq!(rows(&table).await, 3);

    let result = table
        .upsert([batch(&[("a", "sf", 2, 11), ("d", "ny", 2, 40)])])
        .await
        .unwrap();
    assert_eq!(result.num_updates, 1);
    assert_eq!(result.num_inserts, 1);

    table
        .upsert_with(
            [batch(&[("b", "sf", 3, 999)])],
            UpsertOptions {
                update_columns: Some(vec!["v".to_string()]),
            },
        )
        .await
        .unwrap();

    let set = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![777])) as ArrayRef],
    )
    .unwrap();
    let result = table.update("city = 'ny'", set).await.unwrap();
    assert_eq!(result.num_updates, 2);

    let result = table.delete("id = 'b'").await.unwrap();
    assert_eq!(result.num_deletes, 1);
    let result = table.delete("v = 777").await.unwrap();
    assert_eq!(result.num_deletes, 2);
    assert_eq!(rows(&table).await, 1);

    table
        .dynamic_partition_overwrite([batch(&[("z1", "sf", 9, 1)])])
        .await
        .unwrap();
    assert_eq!(rows(&table).await, 1, "sf replaced; ny had no rows left");

    table
        .overwrite([batch(&[("w", "ny", 10, 5)])])
        .await
        .unwrap();
    assert_eq!(rows(&table).await, 1);
}

/// MOR verb lifecycle: log-block upserts, expression update, keyed and
/// expression deletes, read-back through base+log merge.
#[tokio::test]
async fn test_mor_verb_lifecycle_in_crate() {
    let dir = tempfile::tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("t")
        .with_table_type(TableTypeValue::MergeOnRead)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["ts"])
        .create()
        .await
        .unwrap();

    table
        .append([batch(&[("a", "sf", 1, 10), ("b", "ny", 1, 20)])])
        .await
        .unwrap();
    let result = table
        .upsert([batch(&[("a", "sf", 2, 11), ("c", "ny", 2, 30)])])
        .await
        .unwrap();
    assert_eq!(result.num_updates, 1);
    assert_eq!(result.num_inserts, 1);

    let set = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![555])) as ArrayRef],
    )
    .unwrap();
    table.update("id = 'b'", set).await.unwrap();

    table.delete("id = 'c'").await.unwrap();
    let result = table.delete("v = 555").await.unwrap();
    assert_eq!(result.num_deletes, 1);
    assert_eq!(rows(&table).await, 1);
}

/// Fail the next completed data-instant PUT of `table` with `fault`.
fn arm_completion_put_fault(
    table: &mut Table,
    fault: crate::storage::fault::PutFault,
) -> Arc<crate::storage::fault::FaultyPutStore> {
    let storage = table.file_system_view.storage.clone();
    let store =
        crate::storage::fault::FaultyPutStore::new(storage.object_store.clone(), fault, |path| {
            let path = path.as_ref();
            !path.contains(".hoodie/metadata/")
                && path.contains(".hoodie/timeline/")
                && [".commit", ".deltacommit", ".replacecommit"]
                    .iter()
                    .any(|action| path.ends_with(action))
        });
    table.file_system_view.storage = crate::storage::Storage::new_with_object_store(
        storage.base_url.as_ref().clone(),
        store.clone(),
        storage.hudi_configs.clone(),
    );
    store
}

/// A completion PUT that errors may still have stored the completed
/// instant, so the files it references must survive the error; the
/// instant is either durable or left pending for the next write's
/// rollback.
#[tokio::test]
async fn test_failed_completion_put_keeps_written_files() {
    use crate::storage::fault::PutFault;
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for fault in [
            PutFault::LandedThenError,
            PutFault::LandedThenAlreadyExists,
            PutFault::NotLanded,
        ] {
            for verb in ["append", "upsert", "delete"] {
                let ctx = format!("{table_type:?} {fault:?} {verb}");
                let dir = tempfile::tempdir().unwrap();
                let uri = dir.path().to_str().unwrap();
                let mut table = Table::create(uri)
                    .with_table_name("t")
                    .with_table_type(table_type.clone())
                    .with_record_key_fields(["id"])
                    .with_partition_fields(["city"])
                    .with_ordering_fields(["ts"])
                    .create()
                    .await
                    .unwrap();
                table
                    .append([batch(&[
                        ("a", "sf", 1, 10),
                        ("b", "sf", 1, 20),
                        ("c", "ny", 1, 30),
                    ])])
                    .await
                    .unwrap();

                let store = arm_completion_put_fault(&mut table, fault);
                let (result, rows_if_committed) = match verb {
                    "append" => (
                        table
                            .append([batch(&[("d", "ny", 2, 40)])])
                            .await
                            .map(|_| ()),
                        4,
                    ),
                    "upsert" => (
                        table
                            .upsert([batch(&[("a", "sf", 2, 11), ("d", "ny", 2, 40)])])
                            .await
                            .map(|_| ()),
                        4,
                    ),
                    _ => (table.delete("id = 'b'").await.map(|_| ()), 2),
                };
                assert!(result.is_err(), "{ctx}: the faulted PUT must surface");
                assert!(!store.is_armed(), "{ctx}: the completion PUT was not hit");

                let expected = if matches!(fault, PutFault::NotLanded) {
                    3
                } else {
                    rows_if_committed
                };
                let mut fresh = Table::new(uri).await.unwrap();
                assert_eq!(rows(&fresh).await, expected, "{ctx}");

                // The next write rolls back only an instant that never
                // completed, so a landed commit keeps its rows.
                fresh.append([batch(&[("e", "ny", 3, 50)])]).await.unwrap();
                let fresh = Table::new(uri).await.unwrap();
                assert_eq!(rows(&fresh).await, expected + 1, "{ctx}: after rollback");
            }
        }
    }
}

fn mdt_log_files(base: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.to_string_lossy().contains(".log.") {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(&base.join(".hoodie/metadata"), &mut out);
    out
}

/// A write that fails while updating the metadata table leaves log files
/// named for its instant, whether or not the metadata commit was fenced or
/// completed; rolling the write back must delete them.
#[tokio::test]
async fn test_rollback_deletes_orphan_mdt_logs_of_incomplete_mdt_commit() {
    use crate::storage::fault::{FaultyPutStore, PutFault};
    type Case = (&'static str, PutFault, fn(&str) -> bool);
    let cases: [Case; 2] = [
        ("log put", PutFault::LandedThenError, |path| {
            path.contains(".hoodie/metadata/record_index/") && path.contains(".log.")
        }),
        ("metadata completion", PutFault::NotLanded, |path| {
            path.contains(".hoodie/metadata/.hoodie/timeline/") && path.ends_with(".deltacommit")
        }),
    ];
    for (case, fault, matches) in cases {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut table = Table::create(uri)
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_ordering_fields(["ts"])
            .create()
            .await
            .unwrap();
        table
            .append([batch(&[("a", "sf", 1, 10), ("b", "ny", 1, 20)])])
            .await
            .unwrap();
        let before = mdt_log_files(dir.path());

        let storage = table.file_system_view.storage.clone();
        let store =
            FaultyPutStore::new_for_any_put(storage.object_store.clone(), fault, move |p| {
                matches(p.as_ref())
            });
        table.file_system_view.storage = crate::storage::Storage::new_with_object_store(
            storage.base_url.as_ref().clone(),
            store.clone(),
            storage.hudi_configs.clone(),
        );
        let result = table
            .upsert([batch(&[("a", "sf", 2, 11), ("c", "ny", 2, 30)])])
            .await;
        assert!(result.is_err(), "{case}: the faulted PUT must surface");
        assert!(!store.is_armed(), "{case}: the faulted PUT was not hit");
        let orphans: Vec<_> = mdt_log_files(dir.path())
            .into_iter()
            .filter(|path| !before.contains(path))
            .collect();
        assert!(!orphans.is_empty(), "{case}: no metadata log was written");

        let mut fresh = Table::new(uri).await.unwrap();
        fresh.append([batch(&[("d", "ny", 3, 40)])]).await.unwrap();
        let left: Vec<_> = orphans.iter().filter(|path| path.exists()).collect();
        assert!(
            left.is_empty(),
            "{case}: orphan metadata logs survived: {left:?}"
        );
        assert_eq!(rows(&Table::new(uri).await.unwrap()).await, 3, "{case}");
    }
}

/// A rollback that cannot delete the failed instant's fencing files fails the
/// write that triggered it, as Java's rollback does, leaving the instant
/// pending; the next write finishes the cleanup.
#[tokio::test]
async fn test_rollback_fence_delete_failure_fails_the_write() {
    use crate::storage::fault::{DeleteFails, FaultyPutStore, PutFault};
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut table = Table::create(uri)
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["ts"])
        .with_metadata(false)
        .create()
        .await
        .unwrap();
    table.append([batch(&[("a", "sf", 1, 10)])]).await.unwrap();
    let timeline = dir.path().join(".hoodie/timeline");
    let pending = |timeline: &std::path::Path| -> Vec<String> {
        std::fs::read_dir(timeline)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".inflight"))
            .collect()
    };
    let before = pending(&timeline);
    let storage = table.file_system_view.storage.clone();
    let failing_completion =
        FaultyPutStore::new(storage.object_store.clone(), PutFault::NotLanded, |path| {
            path.as_ref().ends_with(".commit")
        });
    table.file_system_view.storage = crate::storage::Storage::new_with_object_store(
        storage.base_url.as_ref().clone(),
        failing_completion,
        storage.hudi_configs.clone(),
    );
    assert!(table.append([batch(&[("b", "sf", 2, 20)])]).await.is_err());
    // In layout v2 a completed instant keeps its inflight file, so the failed
    // write's fence is the one that appeared during it.
    let failed: Vec<String> = pending(&timeline)
        .into_iter()
        .filter(|name| !before.contains(name))
        .collect();
    assert_eq!(failed.len(), 1);

    let mut table = Table::new(uri).await.unwrap();
    let storage = table.file_system_view.storage.clone();
    table.file_system_view.storage = crate::storage::Storage::new_with_object_store(
        storage.base_url.as_ref().clone(),
        Arc::new(DeleteFails(storage.object_store.clone(), |path| {
            path.ends_with(".inflight")
        })),
        storage.hudi_configs.clone(),
    );
    let result = table.append([batch(&[("c", "sf", 3, 30)])]).await;
    assert!(result.is_err(), "the fence delete failure must surface");
    for name in &failed {
        assert!(
            timeline.join(name).exists(),
            "{name} must stay until cleanup succeeds"
        );
    }

    let mut table = Table::new(uri).await.unwrap();
    table.append([batch(&[("c", "sf", 3, 30)])]).await.unwrap();
    for name in &failed {
        assert!(!timeline.join(name).exists(), "{name} was not cleaned up");
    }
    assert_eq!(rows(&Table::new(uri).await.unwrap()).await, 2);
}
