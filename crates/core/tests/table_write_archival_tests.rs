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
//! LSM timeline archival: active-timeline pruning, history layout
//! (`manifest_N` + `_version_` + level-0 parquet), and reads that keep
//! working after both the data and MDT timelines have archived instants.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::metadata::commit::HoodieCommitMetadata;
use hudi_core::table::Table;
use hudi_core::table::partition::PartitionPruner;
use tempfile::tempdir;

fn batch(rows: Vec<(&str, &str, i64)>) -> RecordBatch {
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

fn completed_commits(timeline: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(timeline)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with(".commit") && n.contains('_'))
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn test_archival_prunes_active_timeline_and_keeps_reads_working() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_option("hoodie.keep.min.commits", "4")
        .with_option("hoodie.keep.max.commits", "6")
        .create()
        .await
        .unwrap();

    let mut all_ids = Vec::new();
    for i in 0..8 {
        let id = format!("k{i}");
        table
            .append([batch(vec![(id.as_str(), "sf", i)])])
            .await
            .unwrap();
        all_ids.push(id);
    }

    // Active data timeline pruned: 8 commits written, archival at the 7th
    // (7 > 6) keeps 4 + 1 more commit afterwards.
    let data_timeline = dir.path().join(".hoodie/timeline");
    let active = completed_commits(&data_timeline);
    assert_eq!(
        active.len(),
        5,
        "expected archival down to min=4 plus one more commit: {active:?}"
    );

    // History layout: _version_, manifest, one L0 parquet with the 3 archived
    // commits whose metadata bytes still parse as commit metadata.
    let history = data_timeline.join("history");
    let version = std::fs::read_to_string(history.join("_version_")).unwrap();
    assert_eq!(version.trim(), "1");
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(history.join("manifest_1")).unwrap()).unwrap();
    let files = manifest["files"].as_array().unwrap();
    assert_eq!(files.len(), 1, "one L0 file after one archival run");
    let file_name = files[0]["fileName"].as_str().unwrap();
    assert!(file_name.ends_with("_0.parquet"), "{file_name}");
    assert!(files[0]["fileLen"].as_i64().unwrap() > 0);

    let parquet_bytes = std::fs::read(history.join(file_name)).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(parquet_bytes),
    )
    .unwrap()
    .build()
    .unwrap();
    let mut archived_rows = 0;
    for batch in reader {
        let batch = batch.unwrap();
        archived_rows += batch.num_rows();
        let actions = batch
            .column_by_name("action")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let metadata = batch
            .column_by_name("metadata")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert_eq!(actions.value(i), "commit");
            HoodieCommitMetadata::from_avro_bytes(metadata.value(i))
                .expect("archived metadata must be the completed instant bytes");
        }
    }
    assert_eq!(archived_rows, 3, "8 commits - 5 active = 3 archived");

    // Archived instants' fencing files are gone from the active timeline.
    let leftover_fencing = std::fs::read_dir(&data_timeline)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with(".requested") || n.ends_with(".inflight"))
        .count();
    assert_eq!(
        leftover_fencing, 10,
        "only the 5 active commits keep fencing files (requested + inflight each)"
    );

    // The MDT's own timeline must NOT archive (bounded by MDT compaction,
    // which does not exist yet) — its file slices need every instant since
    // the bootstrap base files.
    let mdt_history = dir.path().join(".hoodie/metadata/.hoodie/timeline/history");
    assert!(
        !mdt_history.join("_version_").exists(),
        "MDT timeline archival must be compaction-gated (i.e. skipped today)"
    );

    // The MDT files partition still lists every data file even though the
    // early instants are archived on both timelines.
    let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    let partition_schema = reopened.get_partition_schema().await.unwrap();
    let pruner =
        PartitionPruner::new(&[], &partition_schema, reopened.hudi_configs.as_ref()).unwrap();
    let records = reopened
        .read_metadata_table_files_partition(&pruner)
        .await
        .unwrap();
    let sf = records.get("city=sf").expect("city=sf record");
    assert_eq!(
        sf.active_file_names().len(),
        8,
        "archived instants' MDT records must remain readable"
    );

    // Reads return all rows; RLI still resolves keys from archived commits.
    let mut reopened = reopened;
    let rows = reopened
        .read(&hudi_core::table::ReadOptions::new())
        .await
        .unwrap();
    let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 8);
    let result = reopened
        .upsert([batch(vec![("k0", "sf", 100)])])
        .await
        .unwrap();
    assert_eq!(
        result.num_updates, 1,
        "RLI entry from an archived commit must still tag"
    );
    assert_eq!(result.num_inserts, 0);
}

/// The SAME handle that ran the writes must keep seeing rows from archived
/// commits.
///
/// Regression: `reload_completed_commits` (run before every write) refreshed
/// the completed set but not the archival boundary upstream's `is_committed`
/// keys on. Once archival (inside a write) raised the real boundary, the
/// handle's stale boundary left newly-archived instants looking uncommitted:
/// reads on the handle dropped their file groups, and later writes planned on
/// the same view could silently miss the rows they were meant to rewrite.
/// A reopened table was never affected, which is why the test above could not
/// catch it.
#[tokio::test]
async fn test_stale_write_handle_sees_archived_commits() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_option("hoodie.keep.min.commits", "4")
        .with_option("hoodie.keep.max.commits", "6")
        .create()
        .await
        .unwrap();

    // 8 appends on one handle: archival runs at the 7th commit (7 > max=6),
    // pushing the first commits — and the rows only they wrote — into history.
    for i in 0..8 {
        let id = format!("k{i}");
        table
            .append([batch(vec![(id.as_str(), "sf", i)])])
            .await
            .unwrap();
    }

    // Read through the SAME handle, not a reopen.
    let rows = table
        .read(&hudi_core::table::ReadOptions::new())
        .await
        .unwrap();
    let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total, 8,
        "rows written by archived commits must stay visible on the writing handle"
    );

    // A keyed mutation through the same handle must still find the row a
    // now-archived commit wrote (the write plans against the same view).
    let result = table.delete("id = 'k0'").await.unwrap();
    assert_eq!(
        result.num_deletes, 1,
        "delete must locate a row written by an archived commit"
    );
}
