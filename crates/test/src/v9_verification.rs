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

//! Expected record state for v9 txns sample tables after inserts, updates,
//! deletes, INSERT OVERWRITE, compaction, and clustering.

use std::sync::Arc;

use arrow_array::{Int64Array, StringArray};
use datafusion::prelude::SessionContext;
use hudi_datafusion::HudiDataSource;

use crate::SampleTable;

pub const EXPECTED_PARTITIONED_TXN_IDS: &[&str] = &[
    "TXN-001", "TXN-003", "TXN-007", "TXN-008", "TXN-011", "TXN-012", "TXN-013", "TXN-014",
    "TXN-015", "TXN-016", "TXN-017", "TXN-018",
];

pub const EXPECTED_NONPART_TXN_IDS: &[&str] = &[
    "TXN-001", "TXN-003", "TXN-004", "TXN-006", "TXN-007", "TXN-008", "TXN-009", "TXN-010",
    "TXN-011", "TXN-012", "TXN-013", "TXN-014", "TXN-015", "TXN-016",
];

async fn query_count(ctx: &SessionContext, sql: &str) -> i64 {
    let result = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

async fn query_str_col(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Verify records for partitioned tables (simple/complex keygen, by region).
///
/// After: 8 inserts, 2 updates, 2 deletes, INSERT OVERWRITE eu, 4 inserts,
/// compaction (MOR) + clustering, 2 inserts → 12 surviving rows.
pub async fn verify_partitioned_records(ctx: &SessionContext) {
    let txn_ids = query_str_col(ctx, "SELECT txn_id FROM txns ORDER BY txn_id").await;
    assert_eq!(txn_ids, EXPECTED_PARTITIONED_TXN_IDS);

    // TXN-001 updated: txn_type changed to 'reversal'
    let txn_type = query_str_col(ctx, "SELECT txn_type FROM txns WHERE txn_id = 'TXN-001'").await;
    assert_eq!(txn_type, ["reversal"]);

    // TXN-007 updated: fee_amount changed to 75.00
    assert_eq!(
        query_count(
            ctx,
            "SELECT count(*) FROM txns WHERE txn_id = 'TXN-007' AND fee_amount = 75.00"
        )
        .await,
        1
    );

    // INSERT OVERWRITE replaced eu partition (TXN-004/006/009/010 gone)
    let eu_ids = query_str_col(
        ctx,
        "SELECT txn_id FROM txns WHERE region = 'eu' ORDER BY txn_id",
    )
    .await;
    assert_eq!(eu_ids, ["TXN-011", "TXN-012", "TXN-018"]);

    let us_ids = query_str_col(
        ctx,
        "SELECT txn_id FROM txns WHERE region = 'us' ORDER BY txn_id",
    )
    .await;
    assert_eq!(
        us_ids,
        ["TXN-001", "TXN-003", "TXN-013", "TXN-014", "TXN-017"]
    );

    let apac_ids = query_str_col(
        ctx,
        "SELECT txn_id FROM txns WHERE region = 'apac' ORDER BY txn_id",
    )
    .await;
    assert_eq!(apac_ids, ["TXN-007", "TXN-008", "TXN-015", "TXN-016"]);
}

/// Verify records for non-partitioned tables.
///
/// After: 8 inserts, 2 updates, 2 deletes, 8 more inserts,
/// compaction (MOR) + clustering → 14 surviving rows.
/// (No INSERT OVERWRITE, so TXN-004/006/009/010 survive.)
pub async fn verify_nonpart_records(ctx: &SessionContext) {
    let txn_ids = query_str_col(ctx, "SELECT txn_id FROM txns ORDER BY txn_id").await;
    assert_eq!(txn_ids, EXPECTED_NONPART_TXN_IDS);

    // TXN-001 updated: txn_type changed to 'reversal'
    let txn_type = query_str_col(ctx, "SELECT txn_type FROM txns WHERE txn_id = 'TXN-001'").await;
    assert_eq!(txn_type, ["reversal"]);

    // TXN-007 updated: fee_amount changed to 75.00
    assert_eq!(
        query_count(
            ctx,
            "SELECT count(*) FROM txns WHERE txn_id = 'TXN-007' AND fee_amount = 75.00"
        )
        .await,
        1
    );

    let merchants = query_str_col(
        ctx,
        "SELECT merchant_name FROM txns WHERE txn_id IN ('TXN-001', 'TXN-008', 'TXN-015') ORDER BY txn_id",
    )
    .await;
    assert_eq!(merchants, ["Amazon", "Grab", "Japan Airlines"]);
}

/// Register a v9 txns table with a DataFusion session context.
async fn register_v9_table(ctx: &SessionContext, table: &SampleTable, cow: bool) {
    let url = if cow {
        table.url_to_cow()
    } else {
        table.url_to_mor_avro()
    };
    let mut opts: Vec<(&str, &str)> = vec![];
    if !cow {
        opts.push(("hoodie.read.use.read_optimized.mode", "true"));
    }
    let hudi = HudiDataSource::new_with_options(url.as_str(), opts)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Failed to create HudiDataSource for {}: {e}",
                table.as_ref()
            )
        });
    ctx.register_table("txns", Arc::new(hudi)).unwrap();
}

/// Entry point for v9 txns table verification, used by both Rust and Python tests.
pub async fn verify_v9_txns_table(table: &SampleTable, cow: bool, partitioned: bool) {
    let ctx = SessionContext::new();
    register_v9_table(&ctx, table, cow).await;
    if partitioned {
        verify_partitioned_records(&ctx).await;
    } else {
        verify_nonpart_records(&ctx).await;
    }
}
