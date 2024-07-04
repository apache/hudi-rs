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
use std::collections::HashMap;
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

use hudi_core::config::HudiConfig::ReadInputPartitions;
use hudi_core::config::OptionsParser;
use hudi_core::storage::utils::{get_scheme_authority, parse_uri};
use hudi_core::HudiTable;

#[derive(Clone, Debug)]
pub struct HudiDataSource {
    table: Arc<HudiTable>,
    input_partitions: usize,
}

impl HudiDataSource {
    pub async fn new(base_uri: &str, options: HashMap<String, String>) -> Result<Self> {
        let input_partitions = ReadInputPartitions
            .parse_value_or_default(&options)
            .cast::<usize>();
        match HudiTable::new(base_uri, options).await {
            Ok(t) => Ok(Self {
                table: Arc::new(t),
                input_partitions,
            }),
            Err(e) => Err(Execution(format!("Failed to create Hudi table: {}", e))),
        }
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
            .split_file_slices(self.input_partitions)
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_common::{DataFusionError, Result, ScalarValue};

    use hudi_core::config::HudiConfig;
    use hudi_tests::TestTable;

    use crate::HudiDataSource;

    #[tokio::test]
    async fn datafusion_read_hudi_table() -> Result<(), DataFusionError> {
        let config = SessionConfig::new().set(
            "datafusion.sql_parser.enable_ident_normalization",
            ScalarValue::from(false),
        );
        let ctx = SessionContext::new_with_config(config);
        let base_url = TestTable::V6ComplexkeygenHivestyle.url();
        let hudi = HudiDataSource::new(
            base_url.as_str(),
            HashMap::from([(
                HudiConfig::ReadInputPartitions.as_ref().to_string(),
                "2".to_string(),
            )]),
        )
        .await?;
        ctx.register_table("hudi_table_complexkeygen", Arc::new(hudi))?;
        let sql = r#"
        SELECT _hoodie_file_name, id, name, structField.field2
        FROM hudi_table_complexkeygen WHERE id % 2 = 0
        AND structField.field2 > 30 ORDER BY name LIMIT 10"#;

        // verify plan
        let explaining_df = ctx.sql(sql).await?.explain(false, true).unwrap();
        let explaining_rb = explaining_df.collect().await?;
        let explaining_rb = explaining_rb.first().unwrap();
        let plan = get_str_column(explaining_rb, "plan").join("");
        let plan_lines: Vec<&str> = plan.lines().map(str::trim).collect();
        assert!(plan_lines[2].starts_with("SortExec: TopK(fetch=10)"));
        assert!(plan_lines[3].starts_with("ProjectionExec: expr=[_hoodie_file_name@0 as _hoodie_file_name, id@1 as id, name@2 as name, get_field(structField@3, field2) as hudi_table_complexkeygen.structField[field2]]"));
        assert!(plan_lines[5].starts_with(
            "FilterExec: CAST(id@1 AS Int64) % 2 = 0 AND get_field(structField@3, field2) > 30"
        ));
        assert!(plan_lines[6].contains("input_partitions=2"));

        // verify data
        let df = ctx.sql(sql).await?;
        let rb = df.collect().await?;
        let rb = rb.first().unwrap();
        assert_eq!(
            get_str_column(rb, "_hoodie_file_name"),
            &[
                "bb7c3a45-387f-490d-aab2-981c3f1a8ada-0_0-140-198_20240418173213674.parquet",
                "4668e35e-bff8-4be9-9ff2-e7fb17ecb1a7-0_1-161-224_20240418173235694.parquet"
            ]
        );
        assert_eq!(get_i32_column(rb, "id"), &[2, 4]);
        assert_eq!(get_str_column(rb, "name"), &["Bob", "Diana"]);
        assert_eq!(
            get_i32_column(rb, "hudi_table_complexkeygen.structField[field2]"),
            &[40, 50]
        );

        Ok(())
    }

    fn get_str_column<'a>(record_batch: &'a RecordBatch, name: &str) -> Vec<&'a str> {
        record_batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap())
            .collect::<Vec<_>>()
    }

    fn get_i32_column(record_batch: &RecordBatch, name: &str) -> Vec<i32> {
        record_batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap())
            .collect::<Vec<_>>()
    }
}
