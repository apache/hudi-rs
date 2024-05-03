use arrow_array::RecordBatch;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_common;
use datafusion_common::{project_schema, DataFusionError};
use datafusion_expr;
use datafusion_expr::{Expr, TableType};
use datafusion_physical_expr;
use datafusion_physical_expr::PhysicalSortExpr;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use hudi_core::HudiTable;
use hudi_fs::test_utils::extract_test_table;

#[derive(Debug, Clone)]
pub struct HudiDataSource {
    table: HudiTable,
}

impl HudiDataSource {
    pub fn new(base_path: &str) -> Self {
        let table = HudiTable::new(base_path);
        Self { table }
    }
    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(HudiExec::new(projections, schema, self.clone())))
    }

    fn get_record_batches(&self) -> datafusion_common::Result<Vec<RecordBatch>> {
        match self.table.get_snapshot_file_paths() {
            Ok(file_paths) => {
                let mut record_batches = Vec::new();
                for f in file_paths {
                    let file = File::open(f)?;
                    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
                    let mut reader = builder.build()?;
                    if let Ok(Some(result)) = reader.next().transpose() {
                        record_batches.push(result)
                    }
                }
                Ok(record_batches)
            }
            Err(e) => Err(DataFusionError::Execution(
                "Failed to read records from table.".to_owned(),
            )),
        }
    }
}

#[async_trait]
impl TableProvider for HudiDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.schema()
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let data = self.data_source.get_record_batches()?;
        Ok(Box::pin(MemoryStream::try_new(data, self.schema(), None)?))
    }
}
