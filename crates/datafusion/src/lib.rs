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
use datafusion_expr::{and, Expr, TableType};
use datafusion_physical_expr::create_physical_expr;

use hudi_core::storage::utils::{get_scheme_authority, parse_uri};
use hudi_core::HudiTable;

#[derive(Clone, Debug)]
pub struct HudiDataSource {
    table: Arc<HudiTable>,
}

impl HudiDataSource {
    pub async fn new(base_uri: &str, storage_options: HashMap<String, String>) -> Result<Self> {
        match HudiTable::new(base_uri, storage_options).await {
            Ok(t) => Ok(Self { table: Arc::new(t) }),
            Err(e) => Err(DataFusionError::Execution(format!(
                "Failed to create Hudi table: {}",
                e
            ))),
        }
    }

    fn and(exprs: &[Expr]) -> Option<Expr> {
        match exprs.len() {
            0 => None,
            1 => Some(exprs[0].clone()),
            _ => {
                let combined = exprs
                    .iter()
                    .skip(1)
                    .fold(exprs[0].clone(), |acc, expr| and(acc.clone(), expr.clone()));
                Some(combined)
            }
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
        let file_slices = self.table.get_file_slices().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get file slices from Hudi table: {}", e))
        })?;
        let parquet_file_group = file_slices
            .iter()
            .map(|f| {
                let url = parse_uri(&f.base_file.info.uri).unwrap();
                let size = f.base_file.info.size as u64;
                PartitionedFile::new(url.path(), size)
            })
            .collect();

        let url = ObjectStoreUrl::parse(get_scheme_authority(&self.table.base_url))?;
        let fsc = FileScanConfig::new(url, self.schema())
            .with_file_group(parquet_file_group)
            .with_projection(projection.cloned())
            .with_limit(limit);

        let parquet_opts = state.table_options().parquet.clone();
        let mut exec_builder = ParquetExecBuilder::new_with_options(fsc, parquet_opts);
        if let Some(expr) = HudiDataSource::and(filters) {
            let df_schema = DFSchema::try_from(self.schema())?;
            let predicate = create_physical_expr(&expr, &df_schema, state.execution_props())?;
            exec_builder = exec_builder.with_predicate(predicate)
        }

        return Ok(Arc::new(exec_builder.build()));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, StringArray};
    use datafusion::dataframe::DataFrame;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_common::ScalarValue;

    use hudi_tests::TestTable;

    use crate::HudiDataSource;

    #[tokio::test]
    async fn datafusion_read_hudi_table() {
        let config = SessionConfig::new().set(
            "datafusion.sql_parser.enable_ident_normalization",
            ScalarValue::from(false),
        );
        let ctx = SessionContext::new_with_config(config);
        let base_url = TestTable::V6ComplexkeygenHivestyle.url();
        let hudi = HudiDataSource::new(base_url.as_str(), HashMap::new())
            .await
            .unwrap();
        ctx.register_table("hudi_table_complexkeygen", Arc::new(hudi))
            .unwrap();
        let df: DataFrame = ctx
            .sql("SELECT * from hudi_table_complexkeygen where structField.field2 > 30 order by name")
            .await.unwrap();
        let records = df
            .collect()
            .await
            .unwrap()
            .to_vec()
            .first()
            .unwrap()
            .to_owned();
        let files: Vec<String> = records
            .column_by_name("_hoodie_file_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap_or_default().to_string())
            .collect();
        assert_eq!(
            files,
            vec![
                "bb7c3a45-387f-490d-aab2-981c3f1a8ada-0_0-140-198_20240418173213674.parquet",
                "4668e35e-bff8-4be9-9ff2-e7fb17ecb1a7-0_1-161-224_20240418173235694.parquet"
            ]
        );
        let ids: Vec<i32> = records
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|i| i.unwrap_or_default())
            .collect();
        assert_eq!(ids, vec![2, 4])
    }
}
