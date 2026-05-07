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
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, Result, Statistics};
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

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.aggregate_file_slice_statistics())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        let column_statistics =
            vec![ColumnStatistics::new_unknown(); self.projected_schema.fields().len()];

        let partitions: Box<dyn Iterator<Item = &Vec<FileSlice>>> = match partition {
            None => Box::new(self.file_slice_partitions.iter()),
            Some(idx) => match self.file_slice_partitions.get(idx) {
                Some(slices) => Box::new(std::iter::once(slices)),
                None => return Ok(Statistics::new_unknown(&self.projected_schema)),
            },
        };

        Ok(Self::aggregate_partitions(partitions, column_statistics))
    }
}

impl HudiScanExec {
    fn aggregate_file_slice_statistics(&self) -> Statistics {
        let column_statistics =
            vec![ColumnStatistics::new_unknown(); self.projected_schema.fields().len()];
        Self::aggregate_partitions(self.file_slice_partitions.iter(), column_statistics)
    }

    fn aggregate_partitions<'a, I>(
        partitions: I,
        column_statistics: Vec<ColumnStatistics>,
    ) -> Statistics
    where
        I: IntoIterator<Item = &'a Vec<FileSlice>>,
    {
        let mut total_rows: u64 = 0;
        let mut total_byte_size: u64 = 0;
        let mut have_row_estimate = false;
        let mut have_byte_estimate = false;

        for slices in partitions {
            for file_slice in slices {
                if let Some(meta) = &file_slice.base_file.file_metadata {
                    if meta.num_records > 0 {
                        total_rows = total_rows.saturating_add(meta.num_records as u64);
                        have_row_estimate = true;
                    }
                    if meta.size > 0 {
                        total_byte_size = total_byte_size.saturating_add(meta.size);
                        have_byte_estimate = true;
                    }
                }
                for log_file in &file_slice.log_files {
                    if let Some(meta) = &log_file.file_metadata
                        && meta.size > 0
                    {
                        total_byte_size = total_byte_size.saturating_add(meta.size);
                        have_byte_estimate = true;
                    }
                }
            }
        }

        let num_rows = if have_row_estimate {
            Precision::Inexact(usize::try_from(total_rows).unwrap_or(usize::MAX))
        } else {
            Precision::Absent
        };
        let total_byte_size = if have_byte_estimate {
            Precision::Inexact(usize::try_from(total_byte_size).unwrap_or(usize::MAX))
        } else {
            Precision::Absent
        };

        Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hudi_core::file_group::base_file::BaseFile;
    use hudi_core::storage::file_metadata::FileMetadata;
    use std::collections::BTreeSet;
    use std::str::FromStr;

    fn file_slice_with_meta(
        file_name: &str,
        size: u64,
        num_records: i64,
        byte_size: i64,
    ) -> FileSlice {
        let mut bf = BaseFile::from_str(file_name).unwrap();
        bf.file_metadata = Some(FileMetadata {
            name: file_name.to_string(),
            size,
            byte_size,
            num_records,
        });
        FileSlice {
            base_file: bf,
            log_files: BTreeSet::new(),
            partition_path: String::new(),
            base_file_column_stats: None,
        }
    }

    #[test]
    fn test_aggregate_partitions_sums_rows_and_bytes() {
        let partitions = [
            vec![
                file_slice_with_meta("fileA-0_0-1-1_20250101000000000.parquet", 100, 10, 200),
                file_slice_with_meta("fileB-0_0-1-1_20250101000000000.parquet", 300, 20, 600),
            ],
            vec![file_slice_with_meta(
                "fileC-0_0-1-1_20250101000000000.parquet",
                500,
                30,
                1000,
            )],
        ];

        let stats = HudiScanExec::aggregate_partitions(
            partitions.iter(),
            vec![ColumnStatistics::new_unknown(); 2],
        );

        assert_eq!(stats.num_rows, Precision::Inexact(60));
        assert_eq!(stats.total_byte_size, Precision::Inexact(900));
        assert_eq!(stats.column_statistics.len(), 2);
    }

    #[test]
    fn test_aggregate_partitions_returns_absent_when_metadata_missing() {
        let mut bf = BaseFile::from_str("fileA-0_0-1-1_20250101000000000.parquet").unwrap();
        bf.file_metadata = None;
        let slices = [vec![FileSlice {
            base_file: bf,
            log_files: BTreeSet::new(),
            partition_path: String::new(),
            base_file_column_stats: None,
        }]];

        let stats = HudiScanExec::aggregate_partitions(
            slices.iter(),
            vec![ColumnStatistics::new_unknown()],
        );

        assert!(matches!(stats.num_rows, Precision::Absent));
        assert!(matches!(stats.total_byte_size, Precision::Absent));
    }

    #[test]
    fn test_aggregate_partitions_byte_size_only_when_records_unknown() {
        // Mirrors the Lance case: file listing reports `size` but
        // FileStatsEstimator has not populated `num_records`.
        let slices = [vec![file_slice_with_meta(
            "fileA-0_0-1-1_20250101000000000.lance",
            500,
            0,
            0,
        )]];

        let stats = HudiScanExec::aggregate_partitions(
            slices.iter(),
            vec![ColumnStatistics::new_unknown()],
        );

        assert!(matches!(stats.num_rows, Precision::Absent));
        assert_eq!(stats.total_byte_size, Precision::Inexact(500));
    }
}
