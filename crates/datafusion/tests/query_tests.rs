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

//! DataFusion read tests for v9 txns tables.

use std::sync::Arc;

use arrow_array::{BooleanArray, Float64Array, Int32Array, RecordBatch, StringArray};
use datafusion::prelude::SessionContext;
use hudi_core::config::read::HudiReadConfig::{InputPartitions, UseReadOptimizedMode};
use hudi_core::table::{ReadOptions, Table};
use hudi_datafusion::HudiDataSource;
use hudi_test::QuickstartTripsTable;
use hudi_test::SampleTable;
use hudi_test::v9_verification::verify_v9_txns_table;

fn rider_fare_rows(batches: &[RecordBatch]) -> Vec<(String, f64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let riders = batch
            .column_by_name("rider")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let fares = batch
            .column_by_name("fare")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for row_idx in 0..batch.num_rows() {
            rows.push((riders.value(row_idx).to_string(), fares.value(row_idx)));
        }
    }
    rows.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    rows
}

fn uuid_rider_fare_rows(batches: &[RecordBatch]) -> Vec<(String, String, f64)> {
    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(QuickstartTripsTable::uuid_rider_and_fare(batch));
    }
    rows.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    rows
}

fn id_name_active_rows(batches: &[RecordBatch]) -> Vec<(i32, String, bool)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let active = batch
            .column_by_name("isActive")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        for row_idx in 0..batch.num_rows() {
            rows.push((
                ids.value(row_idx),
                names.value(row_idx).to_string(),
                active.value(row_idx),
            ));
        }
    }
    rows.sort_unstable_by_key(|(id, _, _)| *id);
    rows
}

// ============================================================================
// COW tests
// ============================================================================

#[tokio::test]
async fn test_v9_txns_cow_simple_nometa() {
    verify_v9_txns_table(&SampleTable::V9TxnsSimpleNometa, true, true).await;
}

#[tokio::test]
async fn test_v9_txns_cow_simple_meta() {
    verify_v9_txns_table(&SampleTable::V9TxnsSimpleMeta, true, true).await;
}

#[tokio::test]
async fn test_v9_txns_cow_complex_nometa() {
    verify_v9_txns_table(&SampleTable::V9TxnsComplexNometa, true, true).await;
}

#[tokio::test]
async fn test_v9_txns_cow_complex_meta() {
    verify_v9_txns_table(&SampleTable::V9TxnsComplexMeta, true, true).await;
}

#[tokio::test]
async fn test_v9_txns_cow_nonpart_nometa() {
    verify_v9_txns_table(&SampleTable::V9TxnsNonpartNometa, true, false).await;
}

#[tokio::test]
async fn test_v9_txns_cow_nonpart_meta() {
    verify_v9_txns_table(&SampleTable::V9TxnsNonpartMeta, true, false).await;
}

// ============================================================================
// MOR tests (read-optimized mode, after compaction + clustering)
// ============================================================================

#[tokio::test]
async fn test_v9_txns_mor_simple_nometa() {
    verify_v9_txns_table(&SampleTable::V9TxnsSimpleNometa, false, true).await;
}

#[tokio::test]
async fn test_v9_txns_mor_simple_meta() {
    verify_v9_txns_table(&SampleTable::V9TxnsSimpleMeta, false, true).await;
}

#[tokio::test]
async fn test_v9_txns_mor_complex_nometa() {
    verify_v9_txns_table(&SampleTable::V9TxnsComplexNometa, false, true).await;
}

#[tokio::test]
async fn test_v9_txns_mor_complex_meta() {
    verify_v9_txns_table(&SampleTable::V9TxnsComplexMeta, false, true).await;
}

#[tokio::test]
async fn test_v9_txns_mor_nonpart_nometa() {
    verify_v9_txns_table(&SampleTable::V9TxnsNonpartNometa, false, false).await;
}

#[tokio::test]
async fn test_v9_txns_mor_nonpart_meta() {
    verify_v9_txns_table(&SampleTable::V9TxnsNonpartMeta, false, false).await;
}

#[tokio::test]
async fn test_mor_snapshot_query_matches_core_log_merged_read() {
    let base_url = QuickstartTripsTable::V8Trips8I3U1D.url_to_mor_avro();

    let table = Table::new(base_url.path()).await.unwrap();
    let expected_batches = table
        .read(&ReadOptions::new().with_projection(["rider", "fare"]))
        .await
        .unwrap();
    let expected_rows = rider_fare_rows(&expected_batches);

    let ctx = SessionContext::new();
    let hudi = HudiDataSource::new(base_url.as_str()).await.unwrap();
    ctx.register_table("trips", Arc::new(hudi)).unwrap();
    let actual_batches = ctx
        .sql("SELECT rider, fare FROM trips")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let actual_rows = rider_fare_rows(&actual_batches);

    assert_eq!(actual_rows, expected_rows);
    assert_eq!(actual_rows.len(), 6);
    assert!(actual_rows.iter().all(|(rider, _)| rider != "rider-F"));
    assert!(actual_rows.iter().all(|(rider, _)| rider != "rider-J"));
    assert_eq!(
        actual_rows
            .iter()
            .find(|(rider, _)| rider == "rider-A")
            .unwrap()
            .1,
        0.0
    );
    assert_eq!(
        actual_rows
            .iter()
            .find(|(rider, _)| rider == "rider-G")
            .unwrap()
            .1,
        0.0
    );
}

#[tokio::test]
async fn test_lance_mor_snapshot_query_matches_core_log_merged_read() {
    let base_url = QuickstartTripsTable::V9TripsLance.url_to_mor_avro();

    let table = Table::new(base_url.path()).await.unwrap();
    let expected_batches = table
        .read(&ReadOptions::new().with_projection(["uuid", "rider", "fare"]))
        .await
        .unwrap();
    let expected_rows = uuid_rider_fare_rows(&expected_batches);
    let read_optimized_batches = table
        .read(
            &ReadOptions::new()
                .with_projection(["uuid", "rider", "fare"])
                .with_hudi_option(UseReadOptimizedMode.as_ref(), "true"),
        )
        .await
        .unwrap();
    let read_optimized_rows = uuid_rider_fare_rows(&read_optimized_batches);

    let ctx = SessionContext::new();
    let hudi = HudiDataSource::new(base_url.as_str()).await.unwrap();
    ctx.register_table("trips", Arc::new(hudi)).unwrap();
    let actual_batches = ctx
        .sql("SELECT uuid, rider, fare FROM trips")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let actual_rows = uuid_rider_fare_rows(&actual_batches);

    assert_eq!(actual_rows, expected_rows);
    assert_eq!(actual_rows.len(), 12);
    assert_ne!(
        actual_rows, read_optimized_rows,
        "snapshot read should merge the active Lance MOR data log"
    );

    // The archived fixture has one active data log, for rider-A. The later
    // SQL delete operations are not present as active data-log delete blocks,
    // so the merge-specific assertion here is the rider-A fare update.
    assert_eq!(
        actual_rows
            .iter()
            .find(|(_, rider, _)| rider == "rider-A")
            .unwrap()
            .2,
        0.0
    );
    assert_eq!(
        read_optimized_rows
            .iter()
            .find(|(_, rider, _)| rider == "rider-A")
            .unwrap()
            .2,
        19.1
    );
}

#[tokio::test]
async fn test_partitioned_parquet_mor_snapshot_query_matches_core_after_partition_pruning() {
    let base_url = SampleTable::V6SimplekeygenNonhivestyle.url_to_mor_parquet();
    let projection = ["id", "name", "isActive"];

    let table = Table::new(base_url.path()).await.unwrap();
    let expected_all_batches = table
        .read(&ReadOptions::new().with_projection(projection))
        .await
        .unwrap();
    let expected_all_rows = id_name_active_rows(&expected_all_batches);
    let expected_filtered_batches = table
        .read(
            &ReadOptions::new()
                .with_filters([("byteField", "=", "10")])
                .unwrap()
                .with_projection(projection),
        )
        .await
        .unwrap();
    let expected_filtered_rows = id_name_active_rows(&expected_filtered_batches);
    let read_optimized_filtered_batches = table
        .read(
            &ReadOptions::new()
                .with_filters([("byteField", "=", "10")])
                .unwrap()
                .with_projection(projection)
                .with_hudi_option(UseReadOptimizedMode.as_ref(), "true"),
        )
        .await
        .unwrap();
    let read_optimized_filtered_rows = id_name_active_rows(&read_optimized_filtered_batches);

    let ctx = SessionContext::new();
    let hudi =
        HudiDataSource::new_with_options(base_url.as_str(), [(InputPartitions.as_ref(), "2")])
            .await
            .unwrap();
    ctx.register_table("sample", Arc::new(hudi)).unwrap();

    let actual_all_batches = ctx
        .sql(r#"SELECT id, name, "isActive" FROM sample"#)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let actual_all_rows = id_name_active_rows(&actual_all_batches);
    let actual_filtered_batches = ctx
        .sql(r#"SELECT id, name, "isActive" FROM sample WHERE "byteField" = 10"#)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let actual_filtered_rows = id_name_active_rows(&actual_filtered_batches);

    assert_eq!(actual_all_rows, expected_all_rows);
    assert_eq!(actual_all_rows.len(), 4);
    assert_eq!(actual_filtered_rows, expected_filtered_rows);
    assert_eq!(
        actual_filtered_rows,
        [
            (1, "Alice".to_string(), false),
            (3, "Carol".to_string(), true)
        ]
    );
    assert_ne!(
        actual_filtered_rows, read_optimized_filtered_rows,
        "partition-filtered snapshot should merge the active MOR log in byteField=10"
    );
    assert_eq!(
        read_optimized_filtered_rows,
        [
            (1, "Alice".to_string(), true),
            (3, "Carol".to_string(), true)
        ]
    );
}
