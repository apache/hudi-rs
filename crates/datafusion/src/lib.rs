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

use hudi_core::storage::utils::{get_scheme_authority, parse_uri};
use hudi_core::HudiTable;

#[derive(Clone, Debug)]
pub struct HudiTableProvider {
    table: Arc<HudiTable>,
    read_parallelism: isize,
}

impl HudiTableProvider {
    pub async fn new(base_uri: &str, options: HashMap<String, String>) -> Result<Self> {
        let read_parallelism = Self::get_read_parallelism(&options);
        match HudiTable::new(base_uri, options).await {
            Ok(t) => Ok(Self {
                table: Arc::new(t),
                read_parallelism,
            }),
            Err(e) => Err(Execution(format!("Failed to create Hudi table: {}", e))),
        }
    }

    fn get_read_parallelism(options: &HashMap<String, String>) -> isize {
        let mut read_parallelism = options
            .get("hoodie.read.parallelism")
            .map_or(0, |s| s.parse::<isize>().unwrap_or(0));
        if read_parallelism < 0 {
            read_parallelism = 0
        }
        read_parallelism
    }
}

#[async_trait]
impl TableProvider for HudiTableProvider {
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
            .split_file_slices(self.read_parallelism as usize)
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

    use datafusion::dataframe::DataFrame;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_common::ScalarValue;

    use hudi_tests::TestTable;

    use crate::HudiTableProvider;

    #[tokio::test]
    async fn datafusion_read_hudi_table() {
        let config = SessionConfig::new().set(
            "datafusion.sql_parser.enable_ident_normalization",
            ScalarValue::from(false),
        );
        let ctx = SessionContext::new_with_config(config);
        let base_url = TestTable::V6ComplexkeygenHivestyle.url();
        let hudi = HudiTableProvider::new(base_url.as_str(), HashMap::new())
            .await
            .unwrap();
        ctx.register_table("hudi_table_complexkeygen", Arc::new(hudi))
            .unwrap();
        let sql =
            "SELECT _hoodie_file_name, id, name, structField.field2 from hudi_table_complexkeygen \
        where id % 2 = 0 AND structField.field2 > 30 order by name limit 10";
        let explaining_df: DataFrame = ctx.sql(sql).await.unwrap();
        explaining_df
            .explain(false, true)
            .unwrap()
            .show()
            .await
            .unwrap();

        let df = ctx.sql(sql).await.unwrap();
        df.show().await.unwrap()
    }
}
