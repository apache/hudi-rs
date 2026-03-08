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

use hudi_test::SampleTable;
use hudi_test::v9_verification::verify_v9_txns_table;

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
