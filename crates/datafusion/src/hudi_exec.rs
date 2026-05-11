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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::DataFusionError::Execution;
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, DataFusionError, Result, Statistics};
use futures::stream::{self, BoxStream, TryStreamExt};
use futures::{Stream, StreamExt};

use crate::{external_error, inexact_usize_from_u64};
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
    file_slice_partitions: Vec<Arc<Vec<FileSlice>>>,
    file_group_reader: Arc<FileGroupReader>,
    read_options: ReadOptions,
    input_partitions: usize,
    file_slice_read_concurrency: usize,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl HudiScanExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        file_slice_partitions: Vec<Vec<FileSlice>>,
        file_group_reader: Arc<FileGroupReader>,
        read_options: ReadOptions,
        input_partitions: usize,
        file_slice_read_concurrency: usize,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        let projected_schema = if let Some(ref proj) = projection {
            let fields: Vec<_> = proj.iter().map(|&i| schema.field(i).clone()).collect();
            Arc::new(arrow_schema::Schema::new(fields))
        } else {
            schema.clone()
        };

        // Empty input is normalized to one empty partition so that
        // `Partitioning::UnknownPartitioning(0)` doesn't propagate downstream
        // (DataFusion's planners typically expect at least one partition; some
        // operators panic on zero). `execute(0)` then returns an empty stream
        // via the `if file_slices.is_empty()` short-circuit below.
        let partitions: Vec<Arc<Vec<FileSlice>>> = if file_slice_partitions.is_empty() {
            vec![Arc::new(vec![])]
        } else {
            file_slice_partitions.into_iter().map(Arc::new).collect()
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
            input_partitions,
            file_slice_read_concurrency: file_slice_read_concurrency.max(1),
            projected_schema,
            projection,
            limit,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

struct LimitBatchStream {
    inner: BoxStream<'static, Result<RecordBatch>>,
    remaining: usize,
}

impl LimitBatchStream {
    fn new<S>(stream: S, limit: usize) -> Self
    where
        S: Stream<Item = Result<RecordBatch>> + Send + 'static,
    {
        Self {
            inner: stream.boxed(),
            remaining: limit,
        }
    }
}

impl Stream for LimitBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.remaining == 0 {
            return Poll::Ready(None);
        }

        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let row_count = batch.num_rows();
                if row_count > this.remaining {
                    let limited = batch.slice(0, this.remaining);
                    this.remaining = 0;
                    Poll::Ready(Some(Ok(limited)))
                } else {
                    this.remaining -= row_count;
                    Poll::Ready(Some(Ok(batch)))
                }
            }
            other => other,
        }
    }
}

struct BaselineMetricStream {
    inner: BoxStream<'static, Result<RecordBatch>>,
    baseline_metrics: BaselineMetrics,
}

impl BaselineMetricStream {
    fn new<S>(stream: S, baseline_metrics: BaselineMetrics) -> Self
    where
        S: Stream<Item = Result<RecordBatch>> + Send + 'static,
    {
        Self {
            inner: stream.boxed(),
            baseline_metrics,
        }
    }
}

impl Stream for BaselineMetricStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let _timer = this.baseline_metrics.elapsed_compute().timer();
        let poll = this.inner.as_mut().poll_next(cx);
        this.baseline_metrics.record_poll(poll)
    }
}

impl DisplayAs for HudiScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total_slices: usize = self.file_slice_partitions.iter().map(|p| p.len()).sum();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "HudiScanExec: input_partitions={}, partitions={}, file_slices={}, file_slice_read_concurrency={}, projection={:?}, limit={:?}",
                    self.input_partitions,
                    self.file_slice_partitions.len(),
                    total_slices,
                    self.file_slice_read_concurrency,
                    self.projection,
                    self.limit,
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

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    // Default `cardinality_effect()` (`Unknown`) is intentional. The
    // `CardinalityEffect::Equal` variant describes pass-through nodes that
    // preserve their input cardinality; for a leaf scan whose row count
    // comes from `partition_statistics()`, `Unknown` is the conservative
    // and correct value.

    // We deliberately do NOT override `repartitioned`. Partition count is
    // fixed at `scan()` time from `state.target_partitions` (or the
    // `hoodie.read.input.partitions` config) and we re-chunk file slices
    // accordingly. Re-chunking after the fact would require redistributing
    // already-bound `FileSlice` lists, which is not currently worth the
    // complexity. Default `Ok(None)` declines re-partitioning.
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
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        if file_slices.is_empty() || self.limit == Some(0) {
            let stream = BaselineMetricStream::new(futures::stream::empty(), baseline_metrics);
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                projected_schema,
                stream,
            )));
        }

        let reader = self.file_group_reader.clone();
        let options = self.read_options.clone();
        let concurrency = self
            .file_slice_read_concurrency
            .min(file_slices.len())
            .max(1);

        let stream = stream::iter(0..file_slices.len())
            .map(move |idx| {
                let file_slice = file_slices[idx].clone();
                let reader = reader.clone();
                let options = options.clone();
                async move {
                    let inner_stream =
                        reader
                            .read_file_slice_stream(&file_slice, &options)
                            .await
                            .map_err(|e| external_error("Failed to read file slice", e))?;
                    Ok::<_, DataFusionError>(
                        inner_stream.map_err(|e| external_error("Failed to read batch", e)),
                    )
                }
            })
            // Scan output is unordered; SQL ordering is provided by explicit
            // SortExec nodes. `buffer_unordered` avoids head-of-line blocking
            // when one slice is slower than its neighbours.
            .buffer_unordered(concurrency)
            .try_flatten_unordered(concurrency)
            .boxed();
        let stream = if let Some(limit) = self.limit {
            LimitBatchStream::new(stream, limit).boxed()
        } else {
            stream
        };
        let stream = BaselineMetricStream::new(stream, baseline_metrics);

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

        let partitions: Box<dyn Iterator<Item = &[FileSlice]> + '_> = match partition {
            None => Box::new(
                self.file_slice_partitions
                    .iter()
                    .map(|slices| slices.as_slice()),
            ),
            Some(idx) => match self.file_slice_partitions.get(idx) {
                Some(slices) => Box::new(std::iter::once(slices.as_slice())),
                None => return Ok(Statistics::new_unknown(&self.projected_schema)),
            },
        };

        Ok(Self::aggregate_partitions(partitions, column_statistics))
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(Self {
            file_slice_partitions: self.file_slice_partitions.clone(),
            file_group_reader: self.file_group_reader.clone(),
            read_options: self.read_options.clone(),
            input_partitions: self.input_partitions,
            file_slice_read_concurrency: self.file_slice_read_concurrency,
            projected_schema: self.projected_schema.clone(),
            projection: self.projection.clone(),
            limit,
            properties: self.properties.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn fetch(&self) -> Option<usize> {
        self.limit
    }
}

impl HudiScanExec {
    fn aggregate_file_slice_statistics(&self) -> Statistics {
        let column_statistics =
            vec![ColumnStatistics::new_unknown(); self.projected_schema.fields().len()];
        Self::aggregate_partitions(
            self.file_slice_partitions
                .iter()
                .map(|slices| slices.as_slice()),
            column_statistics,
        )
    }

    fn aggregate_partitions<'a, I>(
        partitions: I,
        column_statistics: Vec<ColumnStatistics>,
    ) -> Statistics
    where
        I: IntoIterator<Item = &'a [FileSlice]>,
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
            inexact_usize_from_u64(total_rows)
        } else {
            Precision::Absent
        };
        let total_byte_size = if have_byte_estimate {
            inexact_usize_from_u64(total_byte_size)
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
    use arrow_array::Int32Array;
    use arrow_schema::Schema;
    use arrow_schema::{DataType, Field};
    use hudi_core::config::util::empty_options;
    use hudi_core::file_group::base_file::BaseFile;
    use hudi_core::storage::file_metadata::FileMetadata;
    use std::collections::BTreeSet;
    use std::fs::canonicalize;
    use std::path::Path;
    use std::str::FromStr;
    use url::Url;

    fn int_batch(values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap()
    }

    fn int_values(batch: &RecordBatch) -> Vec<i32> {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..values.len()).map(|idx| values.value(idx)).collect()
    }

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
            partitions.iter().map(Vec::as_slice),
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
            slices.iter().map(Vec::as_slice),
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
            slices.iter().map(Vec::as_slice),
            vec![ColumnStatistics::new_unknown()],
        );

        assert!(matches!(stats.num_rows, Precision::Absent));
        assert_eq!(stats.total_byte_size, Precision::Inexact(500));
    }

    #[tokio::test]
    async fn test_limit_batch_stream_truncates_and_stops() {
        let batches = vec![
            Ok(int_batch(vec![1, 2, 3])),
            Ok(int_batch(vec![4, 5, 6])),
            Ok(int_batch(vec![7, 8, 9])),
        ];

        let limited = LimitBatchStream::new(stream::iter(batches), 4)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(limited.len(), 2);
        assert_eq!(int_values(&limited[0]), vec![1, 2, 3]);
        assert_eq!(int_values(&limited[1]), vec![4]);
    }

    #[tokio::test]
    async fn test_metrics_override_is_wired() {
        let base_url =
            Url::from_file_path(canonicalize(Path::new("tests/data/table_props_valid")).unwrap())
                .unwrap();
        let reader = Arc::new(
            FileGroupReader::new_with_options(base_url.as_str(), empty_options())
                .await
                .unwrap(),
        );
        let exec = HudiScanExec::new(
            vec![],
            reader,
            ReadOptions::new(),
            1,
            1,
            Arc::new(Schema::empty()),
            None,
            None,
        );

        assert!(exec.metrics().is_some());
    }
}
