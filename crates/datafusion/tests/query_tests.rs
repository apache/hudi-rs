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

use arrow_array::{Float64Array, RecordBatch, StringArray};
use datafusion::prelude::SessionContext;
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
