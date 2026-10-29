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
//! Record-level index lookups on the read path: a key filter reads only the
//! file groups the index names.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::table::TableTypeValue;
use hudi_core::index::HoodieKey;
use hudi_core::table::{ReadOptions, Table};
use tempfile::tempdir;

/// Ten keys `k{group + 3i}` with values `base + i`: the three groups' key
/// ranges overlap, so footer min/max statistics cannot tell them apart and
/// only the index can.
fn batch(group: usize, base: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                (0..10)
                    .map(|i| format!("k{:02}", group + 3 * i))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from_iter_values((0..10).map(|i| base + i))),
        ],
    )
    .unwrap()
}

/// Three file groups: group 0 holds `k00, k03, ...`, group 1 `k01, k04, ...`,
/// group 2 `k02, k05, ...`.
async fn three_groups(
    dir: &std::path::Path,
    table_type: TableTypeValue,
    record_index: bool,
) -> Table {
    let mut table = Table::create(dir.to_str().unwrap())
        .with_table_name("t")
        .with_record_key_fields(["id"])
        .with_table_type(table_type)
        .with_record_index(record_index)
        // One file group per append.
        .with_option("hoodie.parquet.small.file.limit", "0")
        .create()
        .await
        .unwrap();
    for (group, base) in [(0, 0), (1, 100), (2, 200)] {
        table.append([batch(group, base)]).await.unwrap();
    }
    Table::new(dir.to_str().unwrap()).await.unwrap()
}

fn filters(filters: &[(&str, &str, &str)]) -> ReadOptions {
    ReadOptions::new()
        .with_filters(filters.iter().copied())
        .unwrap()
}

/// How many file slices the read plans, and the ids it returns.
async fn slices_and_ids(table: &Table, options: &ReadOptions) -> (usize, Vec<String>) {
    let slices = table.get_file_slices(options).await.unwrap().len();
    let mut ids: Vec<String> = table
        .read(options)
        .await
        .unwrap()
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    ids.sort();
    (slices, ids)
}

#[tokio::test]
async fn test_key_filter_reads_only_the_indexed_file_group() {
    let dir = tempdir().unwrap();
    let table = three_groups(dir.path(), TableTypeValue::CopyOnWrite, true).await;

    assert_eq!(
        table
            .get_file_slices(&ReadOptions::new())
            .await
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        slices_and_ids(&table, &filters(&[("id", "=", "k04")])).await,
        (1, vec!["k04".to_string()])
    );
    assert_eq!(
        slices_and_ids(&table, &filters(&[("_hoodie_record_key", "IN", "k03,k05")])).await,
        (2, vec!["k03".to_string(), "k05".to_string()])
    );
    // Two key filters intersect; a filter on another column changes nothing.
    assert_eq!(
        slices_and_ids(
            &table,
            &filters(&[
                ("id", "IN", "k03,k04"),
                ("id", "=", "k04"),
                ("value", ">", "0")
            ])
        )
        .await,
        (1, vec!["k04".to_string()])
    );
    // A key the index does not know reads nothing.
    assert_eq!(
        slices_and_ids(&table, &filters(&[("id", "=", "zz")])).await,
        (0, vec![])
    );
}

#[tokio::test]
async fn test_key_filter_does_not_use_the_index_for_an_earlier_snapshot() {
    let dir = tempdir().unwrap();
    let table = three_groups(dir.path(), TableTypeValue::CopyOnWrite, true).await;
    let mut instants: Vec<String> = table
        .get_timeline()
        .completed_commits
        .iter()
        .map(|i| i.timestamp.clone())
        .collect();
    instants.sort();
    let options = filters(&[("id", "=", "k04")]).with_as_of_timestamp(&instants[1]);
    // Both file groups of that snapshot are read; the row mask still applies.
    assert_eq!(
        slices_and_ids(&table, &options).await,
        (2, vec!["k04".to_string()])
    );
}

#[tokio::test]
async fn test_key_filter_without_the_index_reads_every_file_group() {
    let dir = tempdir().unwrap();
    let table = three_groups(dir.path(), TableTypeValue::CopyOnWrite, false).await;
    assert_eq!(
        slices_and_ids(&table, &filters(&[("id", "=", "k04")])).await,
        (3, vec!["k04".to_string()])
    );
}

#[tokio::test]
async fn test_key_filter_on_a_merge_on_read_table_reads_the_updated_row() {
    let dir = tempdir().unwrap();
    three_groups(dir.path(), TableTypeValue::MergeOnRead, true).await;
    let mut writer = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    writer.upsert([batch(1, 1000)]).await.unwrap();

    let reader = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    let options = filters(&[("id", "=", "k04")]);
    let slices = reader.get_file_slices(&options).await.unwrap();
    assert_eq!(slices.len(), 1);
    assert!(slices[0].has_log_file(), "the upsert went to a log file");
    let values: Vec<i64> = reader
        .read(&options)
        .await
        .unwrap()
        .iter()
        .flat_map(|b| {
            b.column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(values, vec![1001]);
}

/// A reader opened before a delete keeps its snapshot: the index is ahead of
/// it, so the key still reads back from a full scan.
#[tokio::test]
async fn test_a_stale_reader_does_not_use_a_newer_index() {
    let dir = tempdir().unwrap();
    let reader = three_groups(dir.path(), TableTypeValue::CopyOnWrite, true).await;
    let mut writer = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    writer
        .delete_keys([HoodieKey {
            record_key: "k04".to_string(),
            partition_path: String::new(),
        }])
        .await
        .unwrap();
    assert_eq!(
        slices_and_ids(&reader, &filters(&[("id", "=", "k04")])).await,
        (3, vec!["k04".to_string()])
    );
    let fresh = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    assert_eq!(
        slices_and_ids(&fresh, &filters(&[("id", "=", "k04")])).await,
        (0, vec![])
    );
}

/// `hoodie.metadata.enable=false` on the reader turns the lookup off.
#[tokio::test]
async fn test_metadata_disabled_on_the_reader_reads_every_file_group() {
    let dir = tempdir().unwrap();
    three_groups(dir.path(), TableTypeValue::CopyOnWrite, true).await;
    let table = Table::new_with_options(
        dir.path().to_str().unwrap(),
        [("hoodie.metadata.enable", "false")],
    )
    .await
    .unwrap();
    assert_eq!(
        slices_and_ids(&table, &filters(&[("id", "=", "k04")])).await,
        (3, vec!["k04".to_string()])
    );
}
