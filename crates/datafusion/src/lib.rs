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

pub(crate) mod hudi_exec;
pub(crate) mod util;

use std::any::Any;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::datasource::physical_plan::FileScanConfigBuilder;
use datafusion::datasource::physical_plan::parquet::source::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result;
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DFSchema;
use datafusion_common::DataFusionError::Execution;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::stats::Precision;
use datafusion_common::{DataFusionError, Statistics};
use datafusion_expr::utils::split_conjunction;
use datafusion_expr::{CreateExternalTable, Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::create_physical_expr;
use log::warn;

use crate::hudi_exec::HudiScanExec;
use crate::util::expr::exprs_to_filters;
use hudi_core::config::read::HudiReadConfig::{
    FileSliceReadConcurrency, InputPartitions, UseReadOptimizedMode,
};
use hudi_core::config::table::{BaseFileFormatValue, HudiTableConfig};
use hudi_core::config::util::empty_options;
use hudi_core::config::{ConfigParser, HudiConfigs};
use hudi_core::file_group::file_slice::FileSlice;
use hudi_core::storage::util::{get_scheme_authority, join_url_segments};
use hudi_core::table::{ReadOptions, Table as HudiTable};

fn default_file_slice_read_concurrency() -> usize {
    match FileSliceReadConcurrency.default_value() {
        Some(value) => value.into(),
        None => unreachable!("FileSliceReadConcurrency has a default value defined in hudi-core"),
    }
}

pub(crate) fn inexact_usize_from_u64(value: u64) -> Precision<usize> {
    match usize::try_from(value) {
        Ok(value) => Precision::Inexact(value),
        Err(_) => Precision::Absent,
    }
}

pub(crate) fn external_error<E>(context: impl Into<String>, error: E) -> DataFusionError
where
    E: Error + Send + Sync + 'static,
{
    DataFusionError::External(Box::new(error)).context(context)
}

fn filter_field_matches_partition_column(filter_field: &str, partition_column: &str) -> bool {
    filter_field == partition_column
        || filter_field
            .rsplit_once('.')
            .is_some_and(|(_, name)| name == partition_column)
}

/// Create a `HudiDataSource`.
/// Used for Datafusion to query Hudi tables
///
/// # Examples
///
/// ```rust,no_run
/// use std::sync::Arc;
///
/// use datafusion::error::Result;
/// use datafusion::prelude::{DataFrame, SessionContext};
/// use hudi_datafusion::HudiDataSource;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Initialize a new DataFusion session context
///     let ctx = SessionContext::new();
///
///     // Create a new HudiDataSource with specific read options
///     let hudi = HudiDataSource::new_with_options(
///         "/tmp/trips_table",
///         [("hoodie.read.input.partitions", "5")]).await?;
///
///     // Register the Hudi table with the session context
///     ctx.register_table("trips_table", Arc::new(hudi))?;
///     let df: DataFrame = ctx.sql("SELECT * from trips_table where city = 'san_francisco'").await?;
///     df.show().await?;
///     Ok(())
/// }
/// ```
/// A DataFusion table provider for Apache Hudi tables.
#[derive(Clone)]
pub struct HudiDataSource {
    table: Arc<HudiTable>,
    /// Cached table schema (with meta fields) for synchronous access in `TableProvider::schema()`.
    /// This provider is a construction-time metadata snapshot; create a new
    /// provider to observe schema/stat changes from later commits.
    schema: SchemaRef,
    /// Cached partition schema for determining partition columns.
    /// This is cached at construction since partition schema rarely changes
    /// and is needed synchronously in `supports_filters_pushdown`.
    partition_schema: Schema,
    /// Cached table-level statistics for join ordering and broadcast decisions.
    ///
    /// Consumed in two places:
    /// 1. `TableProvider::statistics()` - returned to the optimizer (note:
    ///    DataFusion's main planner does not currently consult this method;
    ///    see the trait docs).
    /// 2. `scan_parquet` - passed to `FileScanConfigBuilder::with_statistics(...)`,
    ///    which IS consumed by the optimizer for join planning on the
    ///    Parquet/COW fast path.
    ///
    /// The `HudiScanExec` path (Lance and MOR snapshot) derives statistics
    /// per-execution from `FileSlice` metadata via `aggregate_partitions`
    /// rather than reusing this table-level cache, since per-partition stats
    /// are more useful for that path.
    cached_stats: Option<Statistics>,
    /// Number of input partitions for scan planning, extracted from read options.
    input_partitions: usize,
    /// Read-optimized mode requested when constructing the provider.
    read_optimized_mode: bool,
    /// Maximum number of file-slice streams polled concurrently within a scan partition.
    file_slice_read_concurrency: usize,
    /// Explicit base file format from table config, if present.
    base_file_format: Option<BaseFileFormatValue>,
}

impl std::fmt::Debug for HudiDataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HudiDataSource")
            .field("table", &self.table)
            .field(
                "partition_columns",
                &self
                    .partition_schema
                    .fields()
                    .iter()
                    .map(|field| field.name())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
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
        let all_options: Vec<(String, String)> = options
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.into()))
            .collect();
        let input_partitions: usize = match all_options
            .iter()
            .find(|(k, _)| k == InputPartitions.as_ref())
        {
            Some((_, v)) => v.parse().map_err(|_| {
                Execution(format!(
                    "Invalid value '{v}' for {}: expected a non-negative integer",
                    InputPartitions.as_ref()
                ))
            })?,
            None => 0,
        };
        let read_optimized_mode: bool = match all_options
            .iter()
            .find(|(k, _)| k == UseReadOptimizedMode.as_ref())
        {
            Some((_, v)) => v.parse().map_err(|e| {
                Execution(format!(
                    "Invalid value '{v}' for {}: {e}",
                    UseReadOptimizedMode.as_ref()
                ))
            })?,
            None => false,
        };
        let file_slice_read_concurrency: usize = match all_options
            .iter()
            .find(|(k, _)| k == FileSliceReadConcurrency.as_ref())
        {
            Some((_, v)) => {
                let parsed = v.parse().map_err(|_| {
                    Execution(format!(
                        "Invalid value '{v}' for {}: expected a positive integer",
                        FileSliceReadConcurrency.as_ref()
                    ))
                })?;
                if parsed == 0 {
                    return Err(Execution(format!(
                        "Invalid value '0' for {}: expected a positive integer",
                        FileSliceReadConcurrency.as_ref()
                    )));
                }
                parsed
            }
            None => default_file_slice_read_concurrency(),
        };
        let table = HudiTable::new_with_options(base_uri, all_options)
            .await
            .map_err(|e| external_error("Failed to create Hudi table", e))?;

        let base_file_format =
            BaseFileFormatValue::from_configs(&table.hudi_configs).map_err(|e| {
                external_error(
                    format!(
                        "Invalid {} config",
                        HudiTableConfig::BaseFileFormat.as_ref()
                    ),
                    e,
                )
            })?;
        if matches!(base_file_format, Some(BaseFileFormatValue::HFile)) {
            return Err(Execution(
                "HFile is only supported for Hudi metadata tables, not regular DataFusion scans"
                    .to_string(),
            ));
        }

        // Cache schema with meta fields at construction for synchronous access
        let schema = table
            .get_schema_with_meta_fields()
            .await
            .map(SchemaRef::from)
            .unwrap_or_else(|e| {
                warn!("Failed to get table schema, using empty schema: {e}");
                SchemaRef::from(Schema::empty())
            });

        // Cache partition schema at construction for use in supports_filters_pushdown
        let partition_schema = match table.get_partition_schema().await {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to get partition schema, using empty schema: {e}");
                Schema::empty()
            }
        };

        // Compute table-level statistics for join ordering and broadcast decisions.
        // Uses MDT files partition for base-file sizes and, for Parquet tables, one
        // sampled footer to infer row counts and byte sizes without loading all file groups.
        // Falls back to None if statistics cannot be derived.
        let cached_stats = match table.compute_table_stats(None).await {
            Some((num_rows, total_byte_size)) => {
                let num_fields = schema.fields().len();
                Some(Statistics {
                    num_rows: inexact_usize_from_u64(num_rows),
                    total_byte_size: inexact_usize_from_u64(total_byte_size),
                    column_statistics: vec![
                        datafusion_common::ColumnStatistics::new_unknown();
                        num_fields
                    ],
                })
            }
            None => None,
        };

        Ok(Self {
            table: Arc::new(table),
            schema,
            partition_schema,
            cached_stats,
            input_partitions,
            read_optimized_mode,
            file_slice_read_concurrency,
            base_file_format,
        })
    }

    fn get_input_partitions(&self) -> usize {
        self.input_partitions
    }

    #[cfg(test)]
    fn get_file_slice_read_concurrency(&self) -> usize {
        self.file_slice_read_concurrency
    }

    /// Returns the effective `UseReadOptimizedMode` value cached from the
    /// table options at construction time. Used by [`Self::use_parquet_source`]
    /// to decide whether MOR snapshot semantics are required.
    fn effective_read_optimized(&self) -> bool {
        self.read_optimized_mode
    }

    fn scan_read_options(
        &self,
        pushdown_filters: Vec<(String, String, String)>,
        read_optimized: bool,
    ) -> Result<ReadOptions> {
        let mut read_options = ReadOptions::new()
            .with_filters(pushdown_filters)
            .map_err(|e| external_error("Invalid pushdown filter", e))?;
        if read_optimized {
            read_options = read_options.with_hudi_option(UseReadOptimizedMode.as_ref(), "true");
        }
        Ok(read_options)
    }

    /// Build the [`ReadOptions`] passed to `HudiScanExec` for per-slice reads.
    ///
    /// Hudi table-level planning has already applied partition filters. When
    /// partition fields are dropped from data files, passing those filters to
    /// `FileGroupReader` would fail its strict batch-schema validation.
    fn read_options_for_hudi_exec(
        hudi_configs: &HudiConfigs,
        options: &ReadOptions,
    ) -> ReadOptions {
        let drops_partition_columns: bool = hudi_configs
            .get_or_default(HudiTableConfig::DropsPartitionFields)
            .into();
        if !drops_partition_columns || options.filters.is_empty() {
            return options.clone();
        }

        let partition_columns: Vec<String> = hudi_configs
            .get_or_default(HudiTableConfig::PartitionFields)
            .into();
        let mut applicable = options.clone();
        applicable.filters = options
            .filters
            .iter()
            .filter(|filter| {
                !partition_columns
                    .iter()
                    .any(|p| filter_field_matches_partition_column(&filter.field, p))
            })
            .cloned()
            .collect();
        applicable
    }

    /// Returns true iff every file slice has a `.parquet` base file.
    /// Empty input returns `false` so the scan is routed to `HudiScanExec`,
    /// which handles the empty case via `RecordBatchStreamAdapter::new(.., empty())`.
    fn file_slices_are_parquet(file_slices: &[FileSlice]) -> Result<bool> {
        if file_slices.is_empty() {
            return Ok(false);
        }
        for file_slice in file_slices {
            let relative_path = file_slice.base_file_relative_path().map_err(|e| {
                external_error(
                    format!("Failed to get base file relative path for {file_slice:?}"),
                    e,
                )
            })?;
            if !BaseFileFormatValue::Parquet.matches_extension(&relative_path) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Decides whether a scan can be served by DataFusion's native
    /// `ParquetSource` (cheap row-group / page pruning) or must go through
    /// [`HudiScanExec`].
    ///
    /// Routing matrix:
    /// - Parquet COW → `ParquetSource`
    /// - Parquet MOR read-optimized → `ParquetSource`
    /// - Parquet MOR snapshot → `HudiScanExec` (base + log merging)
    /// - Lance and any other base format → `HudiScanExec`
    fn use_parquet_source(
        &self,
        read_options: &ReadOptions,
        file_slices: &[FileSlice],
    ) -> Result<bool> {
        let parquet_base_files = match &self.base_file_format {
            Some(format) => matches!(format, BaseFileFormatValue::Parquet),
            None => Self::file_slices_are_parquet(file_slices)?,
        };
        if !parquet_base_files {
            return Ok(false);
        }
        if !self.table.is_mor() {
            return Ok(true);
        }
        read_options
            .is_read_optimized()
            .map_err(|e| external_error("Invalid read-optimized option", e))
    }

    /// Check if the given expression can be pushed down to the Hudi table.
    ///
    /// The expression can be pushed down if it is:
    /// - A binary expression with a supported operator and operands
    /// - A NOT expression wrapping a pushable expression
    /// - An AND compound expression where at least one side can be pushed down
    /// - A BETWEEN expression with column and literals
    fn can_push_down(&self, expr: &Expr) -> bool {
        Self::can_push_down_expr(self.schema.as_ref(), expr)
    }

    fn can_push_down_expr(schema: &Schema, expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(binary_expr) => {
                let left = &binary_expr.left;
                let op = &binary_expr.op;
                let right = &binary_expr.right;

                match op {
                    Operator::And => {
                        // AND is pushable if at least one side is pushable
                        Self::can_push_down_expr(schema, left)
                            || Self::can_push_down_expr(schema, right)
                    }
                    Operator::Or => {
                        // OR cannot be pushed down with current filter model
                        false
                    }
                    _ => {
                        Self::is_supported_operator(op)
                            && Self::is_supported_operand(schema, left)
                            && Self::is_supported_operand(schema, right)
                    }
                }
            }
            Expr::Not(inner_expr) => {
                // Recursively check if the inner expression can be pushed down
                Self::can_push_down_expr(schema, inner_expr)
            }
            Expr::Between(between) => {
                // BETWEEN can be pushed if expr is a column and bounds are literals
                !between.negated
                    && matches!(&*between.expr, Expr::Column(col) if schema.column_with_name(&col.name).is_some())
                    && matches!(&*between.low, Expr::Literal(..))
                    && matches!(&*between.high, Expr::Literal(..))
            }
            Expr::InList(in_list) => {
                !in_list.list.is_empty()
                    && matches!(in_list.expr.as_ref(), Expr::Column(col) if schema.column_with_name(&col.name).is_some())
                    && in_list
                        .list
                        .iter()
                        .all(|expr| matches!(expr, Expr::Literal(..)))
            }
            _ => false,
        }
    }

    fn is_supported_operator(op: &Operator) -> bool {
        matches!(
            op,
            Operator::Eq
                | Operator::NotEq
                | Operator::Gt
                | Operator::Lt
                | Operator::GtEq
                | Operator::LtEq
        )
    }

    fn is_supported_operand(schema: &Schema, expr: &Expr) -> bool {
        match expr {
            Expr::Column(col) => schema.column_with_name(&col.name).is_some(),
            Expr::Literal(..) => true,
            _ => false,
        }
    }

    /// Returns partition column names from partition schema.
    fn get_partition_columns(&self) -> Vec<String> {
        self.partition_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    fn is_exact_partition_equality_filter(expr: &Expr, partition_cols: &[String]) -> bool {
        match expr {
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Eq => {
                match (&*binary_expr.left, &*binary_expr.right) {
                    (Expr::Column(col), Expr::Literal(..))
                    | (Expr::Literal(..), Expr::Column(col)) => partition_cols.contains(&col.name),
                    _ => false,
                }
            }
            _ => false,
        }
    }

    fn filter_pushdown_support(
        table_schema: &Schema,
        partition_cols: &[String],
        expr: &Expr,
    ) -> TableProviderFilterPushDown {
        let conjuncts = split_conjunction(expr);
        let has_pushable_conjunct = conjuncts
            .iter()
            .any(|conjunct| Self::can_push_down_expr(table_schema, conjunct));

        if !has_pushable_conjunct {
            return TableProviderFilterPushDown::Unsupported;
        }

        let all_conjuncts_are_exact_partition_eq = conjuncts.iter().all(|conjunct| {
            Self::can_push_down_expr(table_schema, conjunct)
                && Self::is_exact_partition_equality_filter(conjunct, partition_cols)
        });

        if all_conjuncts_are_exact_partition_eq {
            TableProviderFilterPushDown::Exact
        } else {
            TableProviderFilterPushDown::Inexact
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn scan_parquet(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        flat_slices: Vec<FileSlice>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_partitions = self.get_input_partitions_for_scan(state);
        let file_slices =
            hudi_core::util::collection::split_into_chunks(flat_slices, input_partitions);
        let base_url = self.table.base_url();
        let mut parquet_file_groups: Vec<Vec<PartitionedFile>> = Vec::new();
        for file_slice_vec in file_slices {
            let mut parquet_file_group_vec = Vec::new();
            for f in file_slice_vec {
                let relative_path = f.base_file_relative_path().map_err(|e| {
                    external_error(
                        format!("Failed to get base file relative path for {f:?}"),
                        e,
                    )
                })?;
                let url = join_url_segments(&base_url, &[relative_path.as_str()])
                    .map_err(|e| external_error("Failed to join URL segments", e))?;
                let size = f.base_file.file_metadata.as_ref().map_or(0, |m| m.size);
                let partitioned_file = PartitionedFile::new(url.path(), size);
                parquet_file_group_vec.push(partitioned_file);
            }
            parquet_file_groups.push(parquet_file_group_vec)
        }

        let url = ObjectStoreUrl::parse(get_scheme_authority(&base_url))?;
        let parquet_opts = TableParquetOptions {
            global: state.config_options().execution.parquet.clone(),
            column_specific_options: Default::default(),
            key_value_metadata: Default::default(),
            crypto: Default::default(),
        };
        let table_schema = self.schema();
        let mut parquet_source = ParquetSource::new(table_schema.clone())
            .with_table_parquet_options(parquet_opts)
            .with_pushdown_filters(true)
            .with_reorder_filters(true)
            .with_enable_page_index(true);
        let filter = filters.iter().cloned().reduce(|acc, new| acc.and(new));
        if let Some(expr) = filter {
            let df_schema = DFSchema::try_from(table_schema.clone())?;
            let predicate = create_physical_expr(&expr, &df_schema, state.execution_props())?;
            parquet_source = parquet_source.with_predicate(predicate)
        }

        let file_groups: Vec<FileGroup> = parquet_file_groups
            .into_iter()
            .map(FileGroup::from)
            .collect();

        let mut fsc_builder = FileScanConfigBuilder::new(url, Arc::new(parquet_source))
            .with_file_groups(file_groups)
            .with_projection_indices(projection.cloned())?
            .with_limit(limit);

        if let Some(stats) = &self.cached_stats {
            // DataFusion's FileScanConfig stores unprojected table statistics
            // and applies the source projection inside partition_statistics().
            fsc_builder = fsc_builder.with_statistics(stats.clone());
        }

        let fsc = fsc_builder.build();
        Ok(Arc::new(DataSourceExec::new(Arc::new(fsc))))
    }

    async fn scan_hudi(
        &self,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
        input_partitions: usize,
        flat_slices: Vec<FileSlice>,
        read_options: ReadOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let file_slices =
            hudi_core::util::collection::split_into_chunks(flat_slices, input_partitions);

        // The reader is built with the caller's full options so table-level
        // setup sees the same query context. Dropped partition filters are only
        // stripped from the per-slice options passed into HudiScanExec below,
        // before FileGroupReader validates base-file batch schemas.
        let file_group_reader = Arc::new(
            self.table
                .create_file_group_reader_with_options(Some(&read_options), empty_options())
                .map_err(|e| external_error("Failed to create FileGroupReader", e))?,
        );

        let mut hudi_read_options =
            Self::read_options_for_hudi_exec(&self.table.hudi_configs, &read_options);
        if let Some(proj) = projection {
            let col_names: Vec<String> = proj
                .iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect();
            hudi_read_options = hudi_read_options.with_projection(col_names);
        }

        Ok(Arc::new(HudiScanExec::new(
            file_slices,
            file_group_reader,
            hudi_read_options,
            input_partitions,
            self.file_slice_read_concurrency,
            self.schema.clone(),
            projection.cloned(),
            limit,
        )))
    }

    fn get_input_partitions_for_scan(&self, state: &dyn Session) -> usize {
        match self.get_input_partitions() {
            0 => state.config_options().execution.target_partitions,
            n => n,
        }
    }
}

#[async_trait]
impl TableProvider for HudiDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<Statistics> {
        self.cached_stats.clone()
    }

    /// Builds the scan `ExecutionPlan` for this Hudi table.
    ///
    /// Per DataFusion's [custom table provider guide], `scan()` is meant to
    /// run during planning and stay cheap. This implementation deviates from
    /// that ideal: it calls `Table::get_file_slices(&read_options)`, which
    /// performs I/O proportional to the partition count: directory listing
    /// for storage-listing tables, or metadata-table HFile reads when MDT is
    /// enabled. The cost is unavoidable for partition pruning at plan time
    /// and is the same pattern DataFusion's own `ListingTable` uses.
    ///
    /// Callers that issue many `scan()` calls per session against the same
    /// table (e.g. ad-hoc SQL across the same dataset) may see planning
    /// latency dominated by the file listing. Future work could cache the
    /// resolved file-slice set keyed by `(filters, as_of_timestamp)`.
    ///
    /// We implement the legacy `scan()` rather than `scan_with_args()`. The
    /// default `scan_with_args()` impl delegates to `scan()`, so today's
    /// behavior is identical. Revisit if a future DataFusion version adds
    /// optimization hints to `scan_with_args()` that we'd want to consume.
    ///
    /// [custom table provider guide]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Idempotent: registers our object store with the session's runtime
        // so the Parquet path (DataSourceExec) can resolve `s3://`/`gs://`/`az://`
        // URIs. Per-scan invocation is intentional: `TableProvider` has no
        // registration hook called by the SessionContext, and direct callers
        // (`HudiDataSource::new`) wouldn't have a chance to call it otherwise.
        self.table.register_storage(state.runtime_env().clone());

        let input_partitions = self.get_input_partitions_for_scan(state);

        let pushdown_exprs: Vec<Expr> = filters
            .iter()
            .flat_map(|expr| split_conjunction(expr).into_iter())
            .filter(|expr| self.can_push_down(expr))
            .cloned()
            .collect();
        let pushdown_filters = exprs_to_filters(&pushdown_exprs);

        let read_optimized = self.effective_read_optimized();
        let read_options = self.scan_read_options(pushdown_filters, read_optimized)?;
        let flat_slices = self
            .table
            .get_file_slices(&read_options)
            .await
            .map_err(|e| external_error("Failed to get file slices from Hudi table", e))?;

        if self.use_parquet_source(&read_options, &flat_slices)? {
            self.scan_parquet(state, projection, filters, limit, flat_slices)
                .await
        } else {
            self.scan_hudi(
                projection,
                limit,
                input_partitions,
                flat_slices,
                read_options,
            )
            .await
        }
    }

    /// Reports partition equality predicates as `Exact`; all other pushed
    /// filters are `Inexact` so DataFusion retains a residual `FilterExec`.
    /// `scan()` splits conjunctions before converting them to Hudi filters, so
    /// pushable atoms inside mixed `AND` predicates still help pruning.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let partition_cols = self.get_partition_columns();

        filters
            .iter()
            .map(|expr| {
                Ok(Self::filter_pushdown_support(
                    self.schema.as_ref(),
                    &partition_cols,
                    expr,
                ))
            })
            .collect()
    }
}

/// `HudiTableFactory` is responsible for creating and configuring Hudi tables.
///
/// This factory handles the initialization of Hudi tables by creating configuration
/// options from both session state and table creation commands.
///
/// # Examples
///
/// Creating a new `HudiTableFactory` instance:
///
/// ```rust,no_run
/// use datafusion::prelude::SessionContext;
/// use datafusion::catalog::TableProviderFactory;
/// use datafusion::sql::parser::CreateExternalTable;
/// use hudi_datafusion::HudiTableFactory;
///
/// #[tokio::main]
/// async fn main() -> datafusion::error::Result<()> {
///     // Initialize a new HudiTableFactory
///     let factory = HudiTableFactory::new();
///     
///     // Initialize a new DataFusion session context
///     let ctx = SessionContext::new();
///     
///     // Register table using SQL command
///     let create_table_sql =
///         "CREATE EXTERNAL TABLE trips_table STORED AS HUDI LOCATION '/tmp/trips_table'";
///     ctx.sql(create_table_sql).await?;
///     
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct HudiTableFactory {}

impl HudiTableFactory {
    pub fn new() -> Self {
        Self {}
    }

    fn resolve_options(
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<HashMap<String, String>> {
        let mut options: HashMap<_, _> = state
            .config_options()
            .entries()
            .iter()
            .filter_map(|e| {
                let value = e.value.as_ref().filter(|v| !v.is_empty())?;
                Some((e.key.clone(), value.clone()))
            })
            .collect();

        // options from the command take precedence
        options.extend(cmd.options.iter().map(|(k, v)| (k.clone(), v.clone())));

        Ok(options)
    }
}

impl Default for HudiTableFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableProviderFactory for HudiTableFactory {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let options = HudiTableFactory::resolve_options(state, cmd)?;
        let base_uri = cmd.location.as_str();
        let table_provider = HudiDataSource::new_with_options(base_uri, options).await?;
        Ok(Arc::new(table_provider))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use datafusion_common::{Column, ScalarValue};
    use hudi_core::config::internal::HudiInternalConfig;
    use hudi_core::config::table::{BaseFileFormatValue, HudiTableConfig};
    use std::fs::canonicalize;
    use std::path::Path;
    use url::Url;

    use datafusion::logical_expr::BinaryExpr;
    use hudi_test::SampleTable::{V6Nonpartitioned, V6SimplekeygenNonhivestyle};

    use crate::HudiDataSource;

    #[tokio::test]
    async fn get_default_input_partitions() {
        let base_url =
            Url::from_file_path(canonicalize(Path::new("tests/data/table_props_valid")).unwrap())
                .unwrap();
        let hudi = HudiDataSource::new(base_url.as_str()).await.unwrap();
        assert_eq!(hudi.get_input_partitions(), 0);
        assert_eq!(
            hudi.get_file_slice_read_concurrency(),
            default_file_slice_read_concurrency()
        );
        assert_eq!(hudi.table_type(), TableType::Base);
        assert_eq!(hudi.statistics(), None);
    }

    #[tokio::test]
    async fn test_new_with_options_sets_file_slice_read_concurrency() {
        let hudi = HudiDataSource::new_with_options(
            V6Nonpartitioned.path_to_cow().as_str(),
            [(FileSliceReadConcurrency.as_ref(), "2")],
        )
        .await
        .unwrap();

        assert_eq!(hudi.get_file_slice_read_concurrency(), 2);
    }

    #[tokio::test]
    async fn test_new_with_options_rejects_invalid_file_slice_read_concurrency() {
        for invalid in ["0", "abc"] {
            let result = HudiDataSource::new_with_options(
                V6Nonpartitioned.path_to_cow().as_str(),
                [(FileSliceReadConcurrency.as_ref(), invalid)],
            )
            .await;

            assert!(result.is_err());
            let error = result.unwrap_err().to_string();
            assert!(error.contains(FileSliceReadConcurrency.as_ref()));
            assert!(error.contains(invalid));
        }
    }

    #[test]
    fn test_file_slices_are_parquet_empty_is_false() {
        assert!(!HudiDataSource::file_slices_are_parquet(&[]).unwrap());
    }

    #[tokio::test]
    async fn test_new_with_options_rejects_hfile_format_for_regular_scan() {
        let result = HudiDataSource::new_with_options(
            V6Nonpartitioned.path_to_cow().as_str(),
            [
                (
                    HudiTableConfig::BaseFileFormat.as_ref(),
                    BaseFileFormatValue::HFile.as_ref(),
                ),
                (HudiInternalConfig::SkipConfigValidation.as_ref(), "true"),
            ],
        )
        .await;

        assert!(
            result.is_err(),
            "HFile format should be rejected for regular DataFusion scans"
        );
        assert!(result.unwrap_err().to_string().contains("HFile"));
    }

    #[tokio::test]
    async fn test_new_with_options_rejects_invalid_base_file_format_config() {
        let result = HudiDataSource::new_with_options(
            V6Nonpartitioned.path_to_cow().as_str(),
            [
                (HudiTableConfig::BaseFileFormat.as_ref(), "orc"),
                (HudiInternalConfig::SkipConfigValidation.as_ref(), "true"),
            ],
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("orc"));
    }

    fn pushdown_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("byteField", DataType::Int8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("intField", DataType::Int32, true),
        ])
    }

    fn partition_cols() -> Vec<String> {
        vec!["byteField".to_string()]
    }

    fn col_lit(name: &str, op: Operator, lit: ScalarValue) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name(name.to_string()))),
            op,
            right: Box::new(Expr::Literal(lit, None)),
        })
    }

    fn pushdown_support(
        schema: &Schema,
        partition_cols: &[String],
        expr: &Expr,
    ) -> TableProviderFilterPushDown {
        HudiDataSource::filter_pushdown_support(schema, partition_cols, expr)
    }

    #[test]
    fn test_filter_pushdown_support_for_non_partitioned_schema() {
        let schema = pushdown_test_schema();
        let partition_cols = vec![];
        let filters = [
            col_lit(
                "name",
                Operator::Eq,
                ScalarValue::Utf8(Some("Alice".to_string())),
            ),
            col_lit("intField", Operator::Gt, ScalarValue::Int32(Some(20000))),
            col_lit(
                "nonexistent_column",
                Operator::Eq,
                ScalarValue::Int32(Some(1)),
            ),
            col_lit(
                "name",
                Operator::NotEq,
                ScalarValue::Utf8(Some("Diana".to_string())),
            ),
            Expr::Literal(ScalarValue::Int32(Some(10)), None),
            Expr::Not(Box::new(col_lit(
                "intField",
                Operator::Gt,
                ScalarValue::Int32(Some(20000)),
            ))),
        ];

        let result = filters
            .iter()
            .map(|expr| pushdown_support(&schema, &partition_cols, expr))
            .collect::<Vec<_>>();

        assert_eq!(
            result,
            vec![
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Inexact,
            ]
        );
    }

    #[test]
    fn test_filter_pushdown_exact_only_for_partition_equality() {
        let schema = pushdown_test_schema();
        let partition_cols = partition_cols();

        let partition_eq = col_lit("byteField", Operator::Eq, ScalarValue::Int8(Some(1)));
        let partition_gt = col_lit("byteField", Operator::Gt, ScalarValue::Int8(Some(1)));
        let non_partition_eq = col_lit(
            "name",
            Operator::Eq,
            ScalarValue::Utf8(Some("Alice".to_string())),
        );

        assert_eq!(
            pushdown_support(&schema, &partition_cols, &partition_eq),
            TableProviderFilterPushDown::Exact
        );
        assert_eq!(
            pushdown_support(&schema, &partition_cols, &partition_gt),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            pushdown_support(&schema, &partition_cols, &non_partition_eq),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn test_filter_pushdown_splits_conjunctions_for_classification() {
        let schema = pushdown_test_schema();
        let partition_cols = partition_cols();

        let partition_eq = col_lit("byteField", Operator::Eq, ScalarValue::Int8(Some(1)));
        let second_partition_eq = col_lit("byteField", Operator::Eq, ScalarValue::Int8(Some(2)));
        let non_partition_eq = col_lit(
            "name",
            Operator::Eq,
            ScalarValue::Utf8(Some("Alice".to_string())),
        );
        let unsupported = Expr::Literal(ScalarValue::Boolean(Some(true)), None);

        assert_eq!(
            pushdown_support(
                &schema,
                &partition_cols,
                &partition_eq.clone().and(second_partition_eq)
            ),
            TableProviderFilterPushDown::Exact
        );
        assert_eq!(
            pushdown_support(
                &schema,
                &partition_cols,
                &partition_eq.clone().and(non_partition_eq)
            ),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            pushdown_support(&schema, &partition_cols, &partition_eq.and(unsupported)),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn test_filter_pushdown_between_in_list_and_or() {
        let schema = pushdown_test_schema();
        let partition_cols = partition_cols();

        let partition_between = Expr::Between(datafusion_expr::Between::new(
            Box::new(Expr::Column(Column::from_name("byteField".to_string()))),
            false,
            Box::new(Expr::Literal(ScalarValue::Int8(Some(1)), None)),
            Box::new(Expr::Literal(ScalarValue::Int8(Some(3)), None)),
        ));
        let partition_in = Expr::InList(datafusion_expr::expr::InList::new(
            Box::new(Expr::Column(Column::from_name("byteField".to_string()))),
            vec![
                Expr::Literal(ScalarValue::Int8(Some(1)), None),
                Expr::Literal(ScalarValue::Int8(Some(2)), None),
            ],
            false,
        ));
        let or_expr = col_lit(
            "name",
            Operator::Eq,
            ScalarValue::Utf8(Some("Alice".to_string())),
        )
        .or(col_lit(
            "name",
            Operator::Eq,
            ScalarValue::Utf8(Some("Bob".to_string())),
        ));

        assert_eq!(
            pushdown_support(&schema, &partition_cols, &partition_between),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            pushdown_support(&schema, &partition_cols, &partition_in),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            pushdown_support(&schema, &partition_cols, &or_expr),
            TableProviderFilterPushDown::Unsupported
        );
    }

    #[test]
    fn test_read_options_for_hudi_exec_strips_dropped_partition_filters() {
        let hudi_configs = HudiConfigs::new([
            (HudiTableConfig::DropsPartitionFields, "true"),
            (HudiTableConfig::PartitionFields, "region,country"),
        ]);
        let read_options = ReadOptions::new()
            .with_filters([
                ("region", "=", "us"),
                ("amount", ">", "10"),
                ("txns.country", "=", "ca"),
            ])
            .unwrap()
            .with_projection(["txn_id", "amount"]);

        let actual = HudiDataSource::read_options_for_hudi_exec(&hudi_configs, &read_options);

        assert_eq!(actual.filters.len(), 1);
        assert_eq!(actual.filters[0].field, "amount");
        assert_eq!(actual.projection, read_options.projection);
        assert_eq!(actual.hudi_options, read_options.hudi_options);
    }

    #[tokio::test]
    async fn test_scan_hudi_keeps_inexact_non_partition_filters() {
        let hudi = HudiDataSource::new(V6SimplekeygenNonhivestyle.url_to_mor_parquet().as_str())
            .await
            .unwrap();
        let read_options = hudi
            .scan_read_options(
                vec![("id".to_string(), ">".to_string(), "1".to_string())],
                false,
            )
            .unwrap();
        let flat_slices = hudi.table.get_file_slices(&read_options).await.unwrap();

        let plan = hudi
            .scan_hudi(None, None, 1, flat_slices, read_options)
            .await
            .unwrap();
        let exec = plan
            .as_any()
            .downcast_ref::<HudiScanExec>()
            .expect("MOR snapshot scan should use HudiScanExec");

        assert_eq!(exec.read_options().filters.len(), 1);
        let filter = &exec.read_options().filters[0];
        assert_eq!(filter.field, "id");
        assert_eq!(filter.values, vec!["1".to_string()]);
    }
}
