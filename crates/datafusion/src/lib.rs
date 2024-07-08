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

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use std::thread;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::ParquetExecBuilder;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use datafusion_common::{DFSchema, DataFusionError};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_expr::create_physical_expr;
use DataFusionError::Execution;

use hudi_core::config::read::HudiReadConfig::InputPartitions;
use hudi_core::storage::utils::{empty_options, get_scheme_authority, parse_uri};
use hudi_core::table::Table as HudiTable;

#[derive(Clone, Debug)]
pub struct HudiDataSource {
    table: Arc<HudiTable>,
}

impl HudiDataSource {
    pub async fn new(base_uri: &str) -> Result<Self> {
        Self::new_with_options(base_uri, empty_options()).await
    }

    pub async fn new_with_options<I, K, V>(base_uri: &str, options: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        match HudiTable::new_with_options(base_uri, options).await {
            Ok(t) => Ok(Self { table: Arc::new(t) }),
            Err(e) => Err(Execution(format!("Failed to create Hudi table: {}", e))),
        }
    }

    fn get_input_partitions(&self) -> usize {
        self.table
            .configs
            .get_or_default(InputPartitions)
            .to::<usize>()
    }
}

#[async_trait]
impl TableProvider for HudiDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let table = self.table.clone();
        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async { table.get_schema().await })
        });
        SchemaRef::from(handle.join().unwrap().unwrap())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let file_slices = self
            .table
            .split_file_slices(self.get_input_partitions())
            .await
            .map_err(|e| Execution(format!("Failed to get file slices from Hudi table: {}", e)))?;
        let mut parquet_file_groups: Vec<Vec<PartitionedFile>> = Vec::new();
        for file_slice_vec in file_slices {
            let parquet_file_group_vec = file_slice_vec
                .iter()
                .map(|f| {
                    let url = parse_uri(&f.base_file.info.uri).unwrap();
                    let size = f.base_file.info.size as u64;
                    PartitionedFile::new(url.path(), size)
                })
                .collect();
            parquet_file_groups.push(parquet_file_group_vec)
        }

        let url = ObjectStoreUrl::parse(get_scheme_authority(&self.table.base_url))?;
        let fsc = FileScanConfig::new(url, self.schema())
            .with_file_groups(parquet_file_groups)
            .with_projection(projection.cloned())
            .with_limit(limit);

        let parquet_opts = state.table_options().parquet.clone();
        let mut exec_builder = ParquetExecBuilder::new_with_options(fsc, parquet_opts);

        let filter = filters.iter().cloned().reduce(|acc, new| acc.and(new));
        if let Some(expr) = filter {
            let df_schema = DFSchema::try_from(self.schema())?;
            let predicate = create_physical_expr(&expr, &df_schema, state.execution_props())?;
            exec_builder = exec_builder.with_predicate(predicate)
        }

        return Ok(exec_builder.build_arc());
    }
}

#[cfg(test)]
mod tests {
    use std::fs::canonicalize;
    use std::path::Path;
    use std::sync::Arc;

    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_common::ScalarValue;
    use url::Url;

    use hudi_core::config::read::HudiReadConfig::InputPartitions;
    use hudi_tests::TestTable::{
        V6ComplexkeygenHivestyle, V6Nonpartitioned, V6SimplekeygenHivestyleNoMetafields,
        V6SimplekeygenNonhivestyle, V6SimplekeygenNonhivestyleOverwritetable,
        V6TimebasedkeygenNonhivestyle,
    };
    use hudi_tests::{utils, TestTable};
    use utils::{get_bool_column, get_i32_column, get_str_column};

    use crate::HudiDataSource;

    #[tokio::test]
    async fn get_default_input_partitions() {
        let base_url =
            Url::from_file_path(canonicalize(Path::new("tests/data/table_props_valid")).unwrap())
                .unwrap();
        let hudi = HudiDataSource::new(base_url.as_str()).await.unwrap();
        assert_eq!(hudi.get_input_partitions(), 0)
    }

    async fn prepare_session_context(
        test_table: &TestTable,
        options: Vec<(&str, &str)>,
    ) -> SessionContext {
        let config = SessionConfig::new().set(
            "datafusion.sql_parser.enable_ident_normalization",
            ScalarValue::from(false),
        );
        let ctx = SessionContext::new_with_config(config);
        let base_url = test_table.url();
        let hudi = HudiDataSource::new_with_options(base_url.as_str(), options)
            .await
            .unwrap();
        ctx.register_table(test_table.as_ref(), Arc::new(hudi))
            .unwrap();
        ctx
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
        let plan_lines: Vec<&str> = plan.lines().map(str::trim).collect();
        assert!(plan_lines[2].starts_with("SortExec: TopK(fetch=10)"));
        assert!(plan_lines[3].starts_with(&format!(
            "ProjectionExec: expr=[id@0 as id, name@1 as name, isActive@2 as isActive, \
            get_field(structField@3, field2) as {}.structField[field2]]",
            table_name
        )));
        assert!(plan_lines[5].starts_with(
            "FilterExec: CAST(id@0 AS Int64) % 2 = 0 AND get_field(structField@3, field2) > 30"
        ));
        assert!(plan_lines[6].contains(&format!("input_partitions={}", planned_input_partitioned)));
    }

    async fn verify_data(ctx: &SessionContext, sql: &str, table_name: &str) {
        let df = ctx.sql(sql).await.unwrap();
        let rb = df.collect().await.unwrap();
        let rb = rb.first().unwrap();
        assert_eq!(get_i32_column(rb, "id"), &[2, 4]);
        assert_eq!(get_str_column(rb, "name"), &["Bob", "Diana"]);
        assert_eq!(get_bool_column(rb, "isActive"), &[false, true]);
        assert_eq!(
            get_i32_column(rb, &format!("{}.structField[field2]", table_name)),
            &[40, 50]
        );
    }

    #[tokio::test]
    async fn datafusion_read_hudi_table() {
        for (test_table, planned_input_partitions) in &[
            (V6ComplexkeygenHivestyle, 2),
            (V6Nonpartitioned, 1),
            (V6SimplekeygenNonhivestyle, 2),
            (V6SimplekeygenHivestyleNoMetafields, 2),
            (V6TimebasedkeygenNonhivestyle, 2),
        ] {
            println!(">>> testing for {}", test_table.as_ref());
            let options = vec![(InputPartitions.as_ref(), "2")];
            let ctx = prepare_session_context(test_table, options).await;

            let sql = format!(
                r#"
            SELECT id, name, isActive, structField.field2
            FROM {} WHERE id % 2 = 0
            AND structField.field2 > 30 ORDER BY name LIMIT 10"#,
                test_table.as_ref()
            );

            verify_plan(&ctx, &sql, test_table.as_ref(), planned_input_partitions).await;
            verify_data(&ctx, &sql, test_table.as_ref()).await
        }
    }

    async fn verify_data_with_replacecommits(ctx: &SessionContext, sql: &str, table_name: &str) {
        let df = ctx.sql(sql).await.unwrap();
        let rb = df.collect().await.unwrap();
        let rb = rb.first().unwrap();
        assert_eq!(get_i32_column(rb, "id"), &[4]);
        assert_eq!(get_str_column(rb, "name"), &["Diana"]);
        assert_eq!(get_bool_column(rb, "isActive"), &[false]);
        assert_eq!(
            get_i32_column(rb, &format!("{}.structField[field2]", table_name)),
            &[50]
        );
    }

    #[tokio::test]
    async fn datafusion_read_hudi_table_with_replacecommits() {
        for (test_table, planned_input_partitions) in
            &[(V6SimplekeygenNonhivestyleOverwritetable, 1)]
        {
            println!(">>> testing for {}", test_table.as_ref());
            let ctx =
                prepare_session_context(test_table, vec![(InputPartitions.as_ref(), "2")]).await;

            let sql = format!(
                r#"
            SELECT id, name, isActive, structField.field2
            FROM {} WHERE id % 2 = 0
            AND structField.field2 > 30 ORDER BY name LIMIT 10"#,
                test_table.as_ref()
            );

            verify_plan(&ctx, &sql, test_table.as_ref(), planned_input_partitions).await;
            verify_data_with_replacecommits(&ctx, &sql, test_table.as_ref()).await
        }
    }
}
