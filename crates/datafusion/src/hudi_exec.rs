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
//! Custom DataFusion execution plan for reading Hudi tables through
//! [`FileGroupReader`], supporting all base file formats and MOR log merging.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::DataFusionError::Execution;
use datafusion_common::Result;
use futures::stream::TryStreamExt;
use futures::{StreamExt, stream};

use hudi_core::file_group::file_slice::FileSlice;
use hudi_core::file_group::reader::FileGroupReader;
use hudi_core::table::ReadOptions;

/// DataFusion execution plan that reads Hudi file slices through
/// [`FileGroupReader`].
///
/// Used for non-Parquet base file formats (Lance) and MOR snapshot
/// queries where base + log file merging is required. Parquet-only
/// COW and MOR read-optimized queries continue to use DataFusion's
/// native `ParquetSource` path for row-group/page-level pruning.
#[derive(Debug)]
pub struct HudiScanExec {
    file_slice_partitions: Vec<Vec<FileSlice>>,
    file_group_reader: Arc<FileGroupReader>,
    read_options: ReadOptions,
    file_slice_read_concurrency: usize,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl HudiScanExec {
    pub fn new(
        file_slice_partitions: Vec<Vec<FileSlice>>,
        file_group_reader: Arc<FileGroupReader>,
        read_options: ReadOptions,
        file_slice_read_concurrency: usize,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let projected_schema = if let Some(ref proj) = projection {
            let fields: Vec<_> = proj.iter().map(|&i| schema.field(i).clone()).collect();
            Arc::new(arrow_schema::Schema::new(fields))
        } else {
            schema.clone()
        };

        let partitions = if file_slice_partitions.is_empty() {
            vec![vec![]]
        } else {
            file_slice_partitions
        };
        let n_partitions = partitions.len();

        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(n_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            file_slice_partitions: partitions,
            file_group_reader,
            read_options,
            file_slice_read_concurrency: file_slice_read_concurrency.max(1),
            projected_schema,
            projection,
            properties,
        }
    }
}

impl DisplayAs for HudiScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total_slices: usize = self.file_slice_partitions.iter().map(|p| p.len()).sum();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "HudiScanExec: partitions={}, file_slices={}, file_slice_read_concurrency={}, projection={:?}",
                    self.file_slice_partitions.len(),
                    total_slices,
                    self.file_slice_read_concurrency,
                    self.projection,
                )
            }
            _ => {
                write!(f, "HudiScanExec")
            }
        }
    }
}

impl ExecutionPlan for HudiScanExec {
    fn name(&self) -> &str {
        "HudiScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(Execution(
                "HudiScanExec is a leaf node and does not accept children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let file_slices = self
            .file_slice_partitions
            .get(partition)
            .ok_or_else(|| {
                Execution(format!(
                    "HudiScanExec partition {partition} out of range (have {})",
                    self.file_slice_partitions.len()
                ))
            })?
            .clone();

        let projected_schema = self.projected_schema.clone();

        if file_slices.is_empty() {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                projected_schema,
                futures::stream::empty(),
            )));
        }

        let reader = self.file_group_reader.clone();
        let options = self.read_options.clone();
        let concurrency = self
            .file_slice_read_concurrency
            .min(file_slices.len())
            .max(1);

        let stream = stream::iter(file_slices)
            .map(move |file_slice| {
                let reader = reader.clone();
                let options = options.clone();
                async move {
                    let inner_stream = reader
                        .read_file_slice_stream(&file_slice, &options)
                        .await
                        .map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Failed to read file slice: {e}"
                        ))
                    })?;
                    Ok::<_, datafusion_common::DataFusionError>(inner_stream.map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Failed to read batch: {e}"
                        ))
                    }))
                }
            })
            .buffered(concurrency)
            // Scan output is unordered; SQL ordering is provided by explicit SortExec nodes.
            .try_flatten_unordered(concurrency);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }
}
