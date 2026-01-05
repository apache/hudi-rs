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

pub(crate) mod util;

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::thread;

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
use datafusion_expr::{CreateExternalTable, Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::create_physical_expr;
use log::warn;

use crate::util::expr::exprs_to_filters;
use hudi_core::config::read::HudiReadConfig::InputPartitions;
use hudi_core::config::util::empty_options;
use hudi_core::storage::util::{get_scheme_authority, join_url_segments};
use hudi_core::table::Table as HudiTable;

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
    /// Cached partition schema for determining partition columns.
    /// This is cached at construction since partition schema rarely changes
    /// and is needed synchronously in `supports_filters_pushdown`.
    partition_schema: Schema,
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
        let table = HudiTable::new_with_options(base_uri, options)
            .await
            .map_err(|e| Execution(format!("Failed to create Hudi table: {e}")))?;

        // Cache partition schema at construction for use in supports_filters_pushdown
        let partition_schema = match table.get_partition_schema().await {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to get partition schema, using empty schema: {e}");
                Schema::empty()
            }
        };

        Ok(Self {
            table: Arc::new(table),
            partition_schema,
        })
    }

    fn get_input_partitions(&self) -> usize {
        self.table
            .hudi_configs
            .get_or_default(InputPartitions)
            .into()
    }

    /// Check if the given expression can be pushed down to the Hudi table.
    ///
    /// The expression can be pushed down if it is:
    /// - A binary expression with a supported operator and operands
    /// - A NOT expression wrapping a pushable expression
    /// - An AND compound expression where at least one side can be pushed down
    /// - A BETWEEN expression with column and literals
    fn can_push_down(&self, expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(binary_expr) => {
                let left = &binary_expr.left;
                let op = &binary_expr.op;
                let right = &binary_expr.right;

                match op {
                    Operator::And => {
                        // AND is pushable if at least one side is pushable
                        self.can_push_down(left) || self.can_push_down(right)
                    }
                    Operator::Or => {
                        // OR cannot be pushed down with current filter model
                        false
                    }
                    _ => {
                        self.is_supported_operator(op)
                            && self.is_supported_operand(left)
                            && self.is_supported_operand(right)
                    }
                }
            }
            Expr::Not(inner_expr) => {
                // Recursively check if the inner expression can be pushed down
                self.can_push_down(inner_expr)
            }
            Expr::Between(between) => {
                // BETWEEN can be pushed if expr is a column and bounds are literals
                !between.negated
                    && matches!(&*between.expr, Expr::Column(_))
                    && matches!(&*between.low, Expr::Literal(..))
                    && matches!(&*between.high, Expr::Literal(..))
            }
            _ => false,
        }
    }

    fn is_supported_operator(&self, op: &Operator) -> bool {
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

    fn is_supported_operand(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Column(col) => self.schema().column_with_name(&col.name).is_some(),
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

    /// Checks if expression filters on a partition column.
    ///
    /// Partition column filters can be marked as `Exact` because they are
    /// fully handled by partition pruning and don't need post-filtering.
    fn is_partition_column_filter(expr: &Expr, partition_cols: &[String]) -> bool {
        match expr {
            Expr::BinaryExpr(binary_expr) => {
                match binary_expr.op {
                    Operator::And => {
                        // For AND, check if both sides are partition filters
                        Self::is_partition_column_filter(&binary_expr.left, partition_cols)
                            && Self::is_partition_column_filter(&binary_expr.right, partition_cols)
                    }
                    _ => {
                        // For partition filters, one side must be a partition column
                        // and the other side must be a literal value
                        match (&*binary_expr.left, &*binary_expr.right) {
                            (Expr::Column(col), Expr::Literal(..))
                                if partition_cols.contains(&col.name) =>
                            {
                                true
                            }
                            (Expr::Literal(..), Expr::Column(col))
                                if partition_cols.contains(&col.name) =>
                            {
                                true
                            }
                            _ => false,
                        }
                    }
                }
            }
            Expr::Not(inner) => Self::is_partition_column_filter(inner, partition_cols),
            Expr::Between(between) => {
                matches!(&*between.expr, Expr::Column(col) if partition_cols.contains(&col.name))
            }
            _ => false,
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
        let result = handle.join().unwrap().unwrap_or_else(|_| Schema::empty());
        SchemaRef::from(result)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.table.register_storage(state.runtime_env().clone());

        // Convert Datafusion `Expr` to `Filter`
        let pushdown_filters = exprs_to_filters(filters);
        let file_slices = self
            .table
            .get_file_slices_splits(self.get_input_partitions(), pushdown_filters)
            .await
            .map_err(|e| Execution(format!("Failed to get file slices from Hudi table: {e}")))?;
        let base_url = self.table.base_url();
        let mut parquet_file_groups: Vec<Vec<PartitionedFile>> = Vec::new();
        for file_slice_vec in file_slices {
            let mut parquet_file_group_vec = Vec::new();
            for f in file_slice_vec {
                let relative_path = f.base_file_relative_path().map_err(|e| {
                    Execution(format!(
                        "Failed to get base file relative path for {f:?} due to {e:?}"
                    ))
                })?;
                let url = join_url_segments(&base_url, &[relative_path.as_str()])
                    .map_err(|e| Execution(format!("Failed to join URL segments: {e:?}")))?;
                let size = f.base_file.file_metadata.as_ref().map_or(0, |m| m.size);
                let partitioned_file = PartitionedFile::new(url.path(), size);
                parquet_file_group_vec.push(partitioned_file);
            }
            parquet_file_groups.push(parquet_file_group_vec)
        }

        let base_url = self.table.base_url();
        let url = ObjectStoreUrl::parse(get_scheme_authority(&base_url))?;

        let parquet_opts = TableParquetOptions {
            global: state.config_options().execution.parquet.clone(),
            column_specific_options: Default::default(),
            key_value_metadata: Default::default(),
            crypto: Default::default(),
        };
        let table_schema = self.schema();
        let mut parquet_source = ParquetSource::new(parquet_opts);
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

        let fsc = FileScanConfigBuilder::new(url, table_schema, Arc::new(parquet_source))
            .with_file_groups(file_groups)
            .with_projection_indices(projection.cloned())
            .with_limit(limit)
            .build();

        Ok(Arc::new(DataSourceExec::new(Arc::new(fsc))))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let partition_cols = self.get_partition_columns();

        filters
            .iter()
            .map(|expr| {
                if !self.can_push_down(expr) {
                    return Ok(TableProviderFilterPushDown::Unsupported);
                }

                // Partition column filters are fully handled by partition pruning
                if Self::is_partition_column_filter(expr, &partition_cols) {
                    Ok(TableProviderFilterPushDown::Exact)
                } else {
                    Ok(TableProviderFilterPushDown::Inexact)
                }
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
    use datafusion_common::{Column, ScalarValue};
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
        assert_eq!(hudi.get_input_partitions(), 0)
    }

    #[tokio::test]
    async fn test_supports_filters_pushdown() {
        let table_provider = HudiDataSource::new_with_options(
            V6Nonpartitioned.path_to_cow().as_str(),
            empty_options(),
        )
        .await
        .unwrap();

        let expr0 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("name".to_string()))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("Alice".to_string())),
                None,
            )),
        });

        let expr1 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("intField".to_string()))),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Int32(Some(20000)), None)),
        });

        let expr2 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name(
                "nonexistent_column".to_string(),
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int32(Some(1)), None)),
        });

        let expr3 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("name".to_string()))),
            op: Operator::NotEq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("Diana".to_string())),
                None,
            )),
        });

        let expr4 = Expr::Literal(ScalarValue::Int32(Some(10)), None);

        let expr5 = Expr::Not(Box::new(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("intField".to_string()))),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Int32(Some(20000)), None)),
        })));

        let filters = vec![&expr0, &expr1, &expr2, &expr3, &expr4, &expr5];
        let result = table_provider.supports_filters_pushdown(&filters).unwrap();

        assert_eq!(result.len(), 6);
        // Non-partitioned table - all filters are Inexact (no partition columns)
        assert_eq!(result[0], TableProviderFilterPushDown::Inexact);
        assert_eq!(result[1], TableProviderFilterPushDown::Inexact);
        assert_eq!(result[2], TableProviderFilterPushDown::Unsupported);
        assert_eq!(result[3], TableProviderFilterPushDown::Inexact);
        assert_eq!(result[4], TableProviderFilterPushDown::Unsupported);
        assert_eq!(result[5], TableProviderFilterPushDown::Inexact);
    }

    #[tokio::test]
    async fn test_supports_filters_pushdown_exact_for_partition_columns() {
        // Use a partitioned table - byteField is the partition column
        let table_provider = HudiDataSource::new_with_options(
            V6SimplekeygenNonhivestyle.path_to_cow().as_str(),
            empty_options(),
        )
        .await
        .unwrap();

        // Filter on partition column (byteField) - should be Exact
        let partition_filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("byteField".to_string()))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int8(Some(1)), None)),
        });

        // Filter on non-partition column (name) - should be Inexact
        let non_partition_filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("name".to_string()))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("Alice".to_string())),
                None,
            )),
        });

        let filters = vec![&partition_filter, &non_partition_filter];
        let result = table_provider.supports_filters_pushdown(&filters).unwrap();

        assert_eq!(result.len(), 2);
        // Partition column filter is Exact
        assert_eq!(result[0], TableProviderFilterPushDown::Exact);
        // Non-partition column filter is Inexact
        assert_eq!(result[1], TableProviderFilterPushDown::Inexact);
    }

    #[tokio::test]
    async fn test_supports_filters_pushdown_and_between() {
        let table_provider = HudiDataSource::new_with_options(
            V6Nonpartitioned.path_to_cow().as_str(),
            empty_options(),
        )
        .await
        .unwrap();

        // AND expression: name = 'Alice' AND intField > 100
        let left = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("name".to_string()))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("Alice".to_string())),
                None,
            )),
        });
        let right = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("intField".to_string()))),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Int32(Some(100)), None)),
        });
        let and_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::And,
            right: Box::new(right),
        });

        // BETWEEN expression: intField BETWEEN 10 AND 100
        let between_expr = Expr::Between(datafusion_expr::Between::new(
            Box::new(Expr::Column(Column::from_name("intField".to_string()))),
            false,
            Box::new(Expr::Literal(ScalarValue::Int32(Some(10)), None)),
            Box::new(Expr::Literal(ScalarValue::Int32(Some(100)), None)),
        ));

        // OR expression - should be unsupported
        let or_left = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("name".to_string()))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("Alice".to_string())),
                None,
            )),
        });
        let or_right = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("name".to_string()))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("Bob".to_string())),
                None,
            )),
        });
        let or_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(or_left),
            op: Operator::Or,
            right: Box::new(or_right),
        });

        let filters = vec![&and_expr, &between_expr, &or_expr];
        let result = table_provider.supports_filters_pushdown(&filters).unwrap();

        assert_eq!(result.len(), 3);
        // AND expression is supported
        assert_eq!(result[0], TableProviderFilterPushDown::Inexact);
        // BETWEEN expression is supported
        assert_eq!(result[1], TableProviderFilterPushDown::Inexact);
        // OR expression is not supported
        assert_eq!(result[2], TableProviderFilterPushDown::Unsupported);
    }
}
