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

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_common::{project_schema, DataFusionError};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_expr::PhysicalSortExpr;

use hudi_core::HudiTable;

#[derive(Debug, Clone)]
pub struct HudiDataSource {
    table: Arc<HudiTable>,
}

impl HudiDataSource {
    pub async fn new(
        base_uri: &str,
        storage_options: HashMap<String, String>,
    ) -> datafusion_common::Result<Self> {
        match HudiTable::new(base_uri, storage_options).await {
            Ok(t) => Ok(Self { table: Arc::new(t) }),
            Err(e) => Err(DataFusionError::Execution(format!(
                "Failed to create Hudi table: {}",
                e
            ))),
        }
    }

    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(HudiExec::new(projections, schema, self.clone())))
    }

    async fn get_record_batches(&mut self) -> datafusion_common::Result<Vec<RecordBatch>> {
        let record_batches =
            self.table.read_snapshot().await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to read snapshot: {}", e))
            })?;

        Ok(record_batches)
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
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, self.schema()).await;
    }
}

#[derive(Debug, Clone)]
pub struct HudiExec {
    data_source: HudiDataSource,
    projected_schema: SchemaRef,
}

impl HudiExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
        data_source: HudiDataSource,
    ) -> Self {
        let projected_schema = project_schema(&schema, projections).unwrap();
        Self {
            data_source,
            projected_schema,
        }
    }
}

impl DisplayAs for HudiExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "HudiExec")
    }
}

impl ExecutionPlan for HudiExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let mut data_source = self.data_source.clone();
        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(data_source.get_record_batches()).unwrap()
        });
        let data = handle.join().unwrap();
        Ok(Box::pin(MemoryStream::try_new(data, self.schema(), None)?))
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
        let hudi = HudiDataSource::new(base_url.path(), HashMap::new())
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
