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

//! Plan verification tests for DataFusion integration with Hudi tables.
//! Tests execution plan structure (filter pushdown, partitioning, projections).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableProviderFactory;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{DataFusionError, ScalarValue};

use hudi_core::config::read::HudiReadConfig::{InputPartitions, UseReadOptimizedMode};
use hudi_core::config::util::empty_options;
use hudi_core::metadata::meta_field::MetaField;
use hudi_datafusion::{HudiDataSource, HudiTableFactory};
use hudi_test::util::{get_bool_column, get_i32_column, get_str_column};
use hudi_test::{SampleTable, assert_arrow_field_names_eq};

// ============================================================================
// Helper Functions
// ============================================================================

async fn create_test_session() -> SessionContext {
    let config = SessionConfig::new().set(
        "datafusion.sql_parser.enable_ident_normalization",
        &ScalarValue::from(false),
    );
    let table_factory: Arc<dyn TableProviderFactory> = Arc::new(HudiTableFactory::default());

    let session_state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_table_factories(HashMap::from([("HUDI".to_string(), table_factory)]))
        .build();

    SessionContext::new_with_state(session_state)
}

async fn register_test_table_with_session<I, K, V>(
    test_table: &SampleTable,
    options: I,
    use_sql: bool,
) -> Result<SessionContext, DataFusionError>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let ctx = create_test_session().await;
    if use_sql {
        let create_table_sql = format!(
            "CREATE EXTERNAL TABLE {} STORED AS HUDI LOCATION '{}' {}",
            test_table.as_ref(),
            test_table.path_to_cow(),
            concat_as_sql_options(options)
        );
        ctx.sql(create_table_sql.as_str()).await?;
    } else {
        let base_url = test_table.url_to_cow();
        let hudi = HudiDataSource::new_with_options(base_url.as_str(), options).await?;
        ctx.register_table(test_table.as_ref(), Arc::new(hudi))?;
    }
    Ok(ctx)
}

async fn register_table_direct<I, K, V>(
    test_table: &SampleTable,
    options: I,
) -> Result<SessionContext, DataFusionError>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let ctx = create_test_session().await;
    let base_url = test_table.url_to_cow();
    let hudi = HudiDataSource::new_with_options(base_url.as_str(), options).await?;
    ctx.register_table(test_table.as_ref(), Arc::new(hudi))?;
    Ok(ctx)
}

async fn register_uri_as_table<I, K, V>(
    table_name: &str,
    base_uri: &str,
    options: I,
) -> Result<SessionContext, DataFusionError>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let ctx = create_test_session().await;
    let hudi = HudiDataSource::new_with_options(base_uri, options).await?;
    ctx.register_table(table_name, Arc::new(hudi))?;
    Ok(ctx)
}

fn concat_as_sql_options<I, K, V>(options: I) -> String
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let kv_pairs: Vec<String> = options
        .into_iter()
        .map(|(k, v)| format!("'{}' '{}'", k.as_ref(), v.into()))
        .collect();

    if kv_pairs.is_empty() {
        String::new()
    } else {
        format!("OPTIONS ({})", kv_pairs.join(", "))
    }
}

async fn verify_plan(
    ctx: &SessionContext,
    sql: &str,
    table_name: &str,
    planned_input_partitioned: &i32,
) {
    let explaining_df = ctx.sql(sql).await.unwrap().explain(false, true).unwrap();
    let explaining_rb = explaining_df.collect().await.unwrap();
    let explaining_rb = explaining_rb.first().unwrap();
    let plan = get_str_column(explaining_rb, "plan").join("");
    assert!(
        plan.contains("SortExec: TopK(fetch=10)"),
        "Plan should contain TopK sort"
    );
    assert!(
        plan.contains(&format!(
            "ProjectionExec: expr=[id@0 as id, name@1 as name, isActive@2 as isActive, \
            get_field(structField@3, field2) as {table_name}.structField[field2]]"
        )),
        "Plan should contain expected projection"
    );
    // With pushdown_filters enabled, simple predicates (id % 2 = 0, name != Alice)
    // are pushed into the Parquet source. Only non-pushable predicates like
    // struct field access remain in FilterExec.
    assert!(
        plan.contains("get_field(structField@3, field2) > 30"),
        "Plan should contain struct field filter (either in FilterExec or DataSourceExec)"
    );
    assert!(
        plan.contains(&format!("input_partitions={planned_input_partitioned}")),
        "Plan should contain expected input_partitions={planned_input_partitioned}"
    );
}

async fn explain_physical_plan(ctx: &SessionContext, sql: &str) -> String {
    let explaining_df = ctx.sql(sql).await.unwrap().explain(false, true).unwrap();
    let explaining_rb = explaining_df.collect().await.unwrap();
    let explaining_rb = explaining_rb.first().unwrap();
    get_str_column(explaining_rb, "plan").join("")
}

async fn verify_data(ctx: &SessionContext, sql: &str, table_name: &str) {
    let df = ctx.sql(sql).await.unwrap();
    let rb = df.collect().await.unwrap();
    let rb = rb.first().unwrap();
    assert_eq!(get_i32_column(rb, "id"), &[2, 4]);
    assert_eq!(get_str_column(rb, "name"), &["Bob", "Diana"]);
    assert_eq!(get_bool_column(rb, "isActive"), &[false, true]);
    assert_eq!(
        get_i32_column(rb, &format!("{table_name}.structField[field2]")),
        &[40, 50]
    );
}

async fn verify_data_with_replacecommits(ctx: &SessionContext, sql: &str, table_name: &str) {
    let df = ctx.sql(sql).await.unwrap();
    let rb = df.collect().await.unwrap();
    let rb = rb.first().unwrap();
    assert_eq!(get_i32_column(rb, "id"), &[4]);
    assert_eq!(get_str_column(rb, "name"), &["Diana"]);
    assert_eq!(get_bool_column(rb, "isActive"), &[false]);
    assert_eq!(
        get_i32_column(rb, &format!("{table_name}.structField[field2]")),
        &[50]
    );
}

// ============================================================================
// V6 Table Tests
// ============================================================================

mod v6_tests {
    use super::*;
    use hudi_test::SampleTable::{
        V6ComplexkeygenHivestyle, V6Empty, V6Nonpartitioned, V6SimplekeygenHivestyleNoMetafields,
        V6SimplekeygenNonhivestyle, V6SimplekeygenNonhivestyleOverwritetable,
        V6TimebasedkeygenNonhivestyle,
    };

    #[tokio::test]
    async fn test_get_create_schema_from_empty_table() {
        let table_provider =
            HudiDataSource::new_with_options(V6Empty.path_to_cow().as_str(), empty_options())
                .await
                .unwrap();
        let schema = table_provider.schema();
        assert_arrow_field_names_eq!(
            schema,
            [MetaField::field_names(), vec!["id", "name", "isActive"]].concat()
        );
    }

    #[tokio::test]
    async fn test_create_table_with_unknown_format() {
        let test_table = V6Nonpartitioned;
        let invalid_format = "UNKNOWN_FORMAT";
        let create_table_sql = format!(
            "CREATE EXTERNAL TABLE {} STORED AS {} LOCATION '{}'",
            test_table.as_ref(),
            invalid_format,
            test_table.path_to_cow()
        );

        let ctx = create_test_session().await;
        let result = ctx.sql(create_table_sql.as_str()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_datafusion_read_hudi_table_with_partition_filter_pushdown() {
        for (test_table, use_sql, planned_input_partitions) in &[
            (V6ComplexkeygenHivestyle, true, 2),
            (V6Nonpartitioned, true, 1),
            (V6SimplekeygenNonhivestyle, false, 2),
            (V6SimplekeygenHivestyleNoMetafields, true, 2),
            (V6TimebasedkeygenNonhivestyle, false, 2),
        ] {
            println!(">>> testing for {}", test_table.as_ref());
            let options = [(InputPartitions, "2")];
            let ctx = register_test_table_with_session(test_table, options, *use_sql)
                .await
                .unwrap();

            let sql = format!(
                r#"
            SELECT id, name, isActive, structField.field2
            FROM {} WHERE id % 2 = 0 AND name != 'Alice'
            AND structField.field2 > 30 ORDER BY name LIMIT 10"#,
                test_table.as_ref()
            );

            verify_plan(&ctx, &sql, test_table.as_ref(), planned_input_partitions).await;
            verify_data(&ctx, &sql, test_table.as_ref()).await
        }
    }

    #[tokio::test]
    async fn test_datafusion_read_hudi_table_with_partition_column_filter() {
        let test_table = V6SimplekeygenNonhivestyle;
        let ctx = register_table_direct(&test_table, [(InputPartitions, "2")])
            .await
            .unwrap();

        let sql = format!(
            "SELECT id FROM {} WHERE byteField = 20 ORDER BY id",
            test_table.as_ref()
        );
        let df = ctx.sql(&sql).await.unwrap();
        let rb = df.collect().await.unwrap();
        let rb = rb.first().unwrap();

        assert_eq!(get_i32_column(rb, "id"), &[2]);
    }

    #[tokio::test]
    async fn test_datafusion_read_hudi_table_with_replacecommits_with_partition_filter_pushdown() {
        for (test_table, use_sql, planned_input_partitions) in
            &[(V6SimplekeygenNonhivestyleOverwritetable, true, 1)]
        {
            println!(">>> testing for {}", test_table.as_ref());
            let ctx =
                register_test_table_with_session(test_table, [(InputPartitions, "2")], *use_sql)
                    .await
                    .unwrap();

            let sql = format!(
                r#"
            SELECT id, name, isActive, structField.field2
            FROM {} WHERE id % 2 = 0 AND name != 'Alice'
            AND structField.field2 > 30 ORDER BY name LIMIT 10"#,
                test_table.as_ref()
            );

            verify_plan(&ctx, &sql, test_table.as_ref(), planned_input_partitions).await;
            verify_data_with_replacecommits(&ctx, &sql, test_table.as_ref()).await
        }
    }
}

// ============================================================================
// V8 Table Tests
// ============================================================================

mod v8_tests {
    use super::*;
    use hudi_test::SampleTable::{
        V8ComplexkeygenHivestyle, V8Nonpartitioned, V8SimplekeygenNonhivestyle,
    };

    #[tokio::test]
    async fn test_v8_nonpartitioned_read() {
        let test_table = V8Nonpartitioned;
        println!(">>> testing V8 for {}", test_table.as_ref());

        let ctx = register_table_direct(&test_table, [(InputPartitions, "2")])
            .await
            .unwrap();

        // Verify schema
        let df = ctx
            .sql(&format!("SELECT * FROM {} LIMIT 1", test_table.as_ref()))
            .await
            .unwrap();
        let schema = df.schema();
        assert!(schema.field_with_name(None, "id").is_ok());
        assert!(schema.field_with_name(None, "name").is_ok());
        assert!(schema.field_with_name(None, "isActive").is_ok());

        // Verify data read with filters
        let sql = format!(
            r#"SELECT id, name, isActive FROM {} WHERE id > 0 ORDER BY id"#,
            test_table.as_ref()
        );
        let df = ctx.sql(&sql).await.unwrap();
        let rb = df.collect().await.unwrap();
        assert!(!rb.is_empty(), "Should return data from V8 table");

        // Verify plan includes DataSourceExec
        let explaining_df = ctx.sql(&sql).await.unwrap().explain(false, true).unwrap();
        let explaining_rb = explaining_df.collect().await.unwrap();
        let explaining_rb = explaining_rb.first().unwrap();
        let plan = get_str_column(explaining_rb, "plan").join("");
        assert!(
            plan.contains("DataSourceExec"),
            "Plan should contain DataSourceExec"
        );
    }

    #[tokio::test]
    async fn test_v8_partitioned_filter_pushdown() {
        let test_table = V8SimplekeygenNonhivestyle;
        println!(">>> testing V8 for {}", test_table.as_ref());

        let ctx = register_table_direct(&test_table, [(InputPartitions, "2")])
            .await
            .unwrap();

        let sql = format!(
            r#"
            SELECT id, name, isActive, structField.field2
            FROM {} WHERE id % 2 = 0 AND name != 'Alice'
            AND structField.field2 > 30 ORDER BY name LIMIT 10"#,
            test_table.as_ref()
        );

        // Verify plan
        let explaining_df = ctx.sql(&sql).await.unwrap().explain(false, true).unwrap();
        let explaining_rb = explaining_df.collect().await.unwrap();
        let explaining_rb = explaining_rb.first().unwrap();
        let plan = get_str_column(explaining_rb, "plan").join("");
        let plan_lines: Vec<&str> = plan.lines().map(str::trim).collect();

        // Verify execution plan structure
        assert!(
            plan_lines[1].starts_with("SortExec: TopK(fetch=10)"),
            "Should have TopK sort"
        );
        assert!(
            plan_lines[2].contains("ProjectionExec"),
            "Should have ProjectionExec"
        );
        assert!(
            plan.contains("FilterExec"),
            "Should have FilterExec for non-partition filters"
        );
        assert!(
            plan.contains("input_partitions=2"),
            "Should have input_partitions=2"
        );

        // Verify data
        let df = ctx.sql(&sql).await.unwrap();
        let rb = df.collect().await.unwrap();
        let rb = rb.first().unwrap();
        assert_eq!(get_i32_column(rb, "id"), &[2, 4]);
        assert_eq!(get_str_column(rb, "name"), &["Bob", "Diana"]);
        assert_eq!(get_bool_column(rb, "isActive"), &[false, true]);
    }

    #[tokio::test]
    async fn test_v8_complex_keygen() {
        let test_table = V8ComplexkeygenHivestyle;
        println!(">>> testing V8 for {}", test_table.as_ref());

        let ctx = register_table_direct(&test_table, [(InputPartitions, "2")])
            .await
            .unwrap();

        let sql = format!(
            r#"
            SELECT id, name, isActive, structField.field2
            FROM {} WHERE id % 2 = 0 AND name != 'Alice'
            AND structField.field2 > 30 ORDER BY name LIMIT 10"#,
            test_table.as_ref()
        );

        // Verify plan has correct input partitions for complex keygen
        let explaining_df = ctx.sql(&sql).await.unwrap().explain(false, true).unwrap();
        let explaining_rb = explaining_df.collect().await.unwrap();
        let explaining_rb = explaining_rb.first().unwrap();
        let plan = get_str_column(explaining_rb, "plan").join("");

        assert!(
            plan.contains("input_partitions=2"),
            "Complex keygen table should have input_partitions=2"
        );
        assert!(
            plan.contains("DataSourceExec"),
            "Plan should contain DataSourceExec"
        );

        // Verify data
        let df = ctx.sql(&sql).await.unwrap();
        let rb = df.collect().await.unwrap();
        let rb = rb.first().unwrap();
        assert_eq!(get_i32_column(rb, "id"), &[2, 4]);
        assert_eq!(get_str_column(rb, "name"), &["Bob", "Diana"]);
        assert_eq!(get_bool_column(rb, "isActive"), &[false, true]);
    }
}

mod dispatch_tests {
    use super::*;
    use hudi_test::SampleTable::V6Nonpartitioned;

    #[tokio::test]
    async fn test_parquet_cow_uses_data_source_exec() {
        let ctx = register_table_direct(&V6Nonpartitioned, empty_options())
            .await
            .unwrap();

        let sql = format!("SELECT id FROM {}", V6Nonpartitioned.as_ref());
        let plan = explain_physical_plan(&ctx, &sql).await;

        assert!(
            plan.contains("DataSourceExec"),
            "Parquet COW should use DataSourceExec. Plan: {plan}"
        );
        assert!(
            !plan.contains("HudiScanExec"),
            "Parquet COW should not use HudiScanExec. Plan: {plan}"
        );
    }

    #[tokio::test]
    async fn test_parquet_mor_read_optimized_uses_data_source_exec() {
        let base_url = V6Nonpartitioned.url_to_mor_parquet();
        let ctx = register_uri_as_table(
            "mor_ro",
            base_url.as_str(),
            [(UseReadOptimizedMode.as_ref(), "true")],
        )
        .await
        .unwrap();

        let plan = explain_physical_plan(&ctx, "SELECT id FROM mor_ro").await;

        assert!(
            plan.contains("DataSourceExec"),
            "Parquet MOR read-optimized should use DataSourceExec. Plan: {plan}"
        );
        assert!(
            !plan.contains("HudiScanExec"),
            "Parquet MOR read-optimized should not use HudiScanExec. Plan: {plan}"
        );
    }

    #[tokio::test]
    async fn test_parquet_mor_snapshot_uses_hudi_scan_exec() {
        let base_url = V6Nonpartitioned.url_to_mor_parquet();
        let ctx = register_uri_as_table("mor_snapshot", base_url.as_str(), empty_options())
            .await
            .unwrap();

        let plan = explain_physical_plan(&ctx, "SELECT id FROM mor_snapshot").await;

        assert!(
            plan.contains("HudiScanExec"),
            "Parquet MOR snapshot should use HudiScanExec. Plan: {plan}"
        );
    }
}

// ============================================================================
// Lance Table Tests (requires `lance` feature)
// ============================================================================

#[cfg(feature = "lance")]
mod lance_tests {
    use super::*;
    use arrow_array::{Float64Array, Int32Array};
    use hudi_test::SampleTable::V9LanceNonpartitioned;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_datafusion_read_lance_cow_table() {
        let test_table = V9LanceNonpartitioned;
        let ctx = register_table_direct(&test_table, empty_options())
            .await
            .unwrap();

        let sql = format!("SELECT * FROM {}", test_table.as_ref());
        let df = ctx.sql(&sql).await.unwrap();
        let batches = df.collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "Lance COW table should return rows");

        let schema = batches[0].schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"id"), "Schema should contain 'id'");
        assert!(
            field_names.contains(&"name"),
            "Schema should contain 'name'"
        );
    }

    #[tokio::test]
    async fn test_datafusion_lance_table_has_meta_fields() {
        let test_table = V9LanceNonpartitioned;
        let ctx = register_table_direct(&test_table, empty_options())
            .await
            .unwrap();

        let sql = format!("SELECT * FROM {} LIMIT 1", test_table.as_ref());
        let df = ctx.sql(&sql).await.unwrap();
        let schema = df.schema();

        assert!(
            schema.field_with_name(None, "_hoodie_commit_time").is_ok(),
            "Lance table schema should include _hoodie_commit_time meta field"
        );
        assert!(
            schema.field_with_name(None, "_hoodie_record_key").is_ok(),
            "Lance table schema should include _hoodie_record_key meta field"
        );
    }

    #[tokio::test]
    async fn test_datafusion_lance_uses_hudi_scan_exec() {
        let test_table = V9LanceNonpartitioned;
        let ctx = register_table_direct(&test_table, empty_options())
            .await
            .unwrap();

        let sql = format!("SELECT id, name FROM {}", test_table.as_ref());
        let explaining_df = ctx.sql(&sql).await.unwrap().explain(false, true).unwrap();
        let explaining_rb = explaining_df.collect().await.unwrap();
        let explaining_rb = explaining_rb.first().unwrap();
        let plan = get_str_column(explaining_rb, "plan").join("");

        assert!(
            plan.contains("HudiScanExec"),
            "Lance table should use HudiScanExec, not DataSourceExec. Plan: {plan}"
        );
    }

    #[tokio::test]
    async fn test_datafusion_lance_cow_query_applies_hudi_updates_deletes_and_inserts() {
        let test_table = V9LanceNonpartitioned;
        let ctx = register_table_direct(&test_table, empty_options())
            .await
            .unwrap();

        let sql = format!("SELECT id, score FROM {} ORDER BY id", test_table.as_ref());
        let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let mut rows = HashMap::new();
        for batch in batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let scores = batch
                .column_by_name("score")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row_idx in 0..batch.num_rows() {
                rows.insert(ids.value(row_idx), scores.value(row_idx));
            }
        }

        let mut ids = rows.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3, 5, 6, 7, 8, 9, 10]);
        assert!(!rows.contains_key(&4));
        assert!((rows[&1] - 0.96).abs() < 1e-9);
        assert!((rows[&2] - 0.93).abs() < 1e-9);
        assert!(rows.contains_key(&9));
        assert!(rows.contains_key(&10));
    }
}
