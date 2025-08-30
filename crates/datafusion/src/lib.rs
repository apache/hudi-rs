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
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::source::ParquetSource;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::DFSchema;
use datafusion_common::DataFusionError::Execution;
use datafusion_expr::{CreateExternalTable, Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::create_physical_expr;

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
/// ```rust
/// use std::sync::Arc;
///
/// use datafusion::error::Result;
/// use datafusion::prelude::{DataFrame, SessionContext};
/// use hudi_datafusion::HudiDataSource;
///
/// // Initialize a new DataFusion session context
/// let ctx = SessionContext::new();
///
/// // Create a new HudiDataSource with specific read options
/// let hudi = HudiDataSource::new_with_options(
///     "/tmp/trips_table",
///     [("hoodie.read.input.partitions", 5)]).await?;
///
/// // Register the Hudi table with the session context
/// ctx.register_table("trips_table", Arc::new(hudi))?;
/// let df: DataFrame = ctx.sql("SELECT * from trips_table where city = 'san_francisco'").await?;
/// df.show().await?;
///
/// ```
#[derive(Clone, Debug)]
pub struct HudiDataSource {
    table: Arc<HudiTable>,
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
        match HudiTable::new_with_options(base_uri, options).await {
            Ok(t) => Ok(Self { table: Arc::new(t) }),
            Err(e) => Err(Execution(format!("Failed to create Hudi table: {}", e))),
        }
    }

    fn get_input_partitions(&self) -> usize {
        self.table
            .hudi_configs
            .get_or_default(InputPartitions)
            .into()
    }

    /// Check if the given expression can be pushed down to the Hudi table.
    ///
    /// The expression can be pushed down if it is a binary expression with a supported operator and operands.
    fn can_push_down(&self, expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(binary_expr) => {
                let left = &binary_expr.left;
                let op = &binary_expr.op;
                let right = &binary_expr.right;
                self.is_supported_operator(op)
                    && self.is_supported_operand(left)
                    && self.is_supported_operand(right)
            }
            Expr::Not(inner_expr) => {
                // Recursively check if the inner expression can be pushed down
                self.can_push_down(inner_expr)
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
            Expr::Literal(_) => true,
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
            .map_err(|e| Execution(format!("Failed to get file slices from Hudi table: {}", e)))?;
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
                let partitioned_file = PartitionedFile::new(url.path(), size as u64);
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
        };
        let table_schema = self.schema();
        let mut parquet_source = ParquetSource::new(parquet_opts);
        let filter = filters.iter().cloned().reduce(|acc, new| acc.and(new));
        if let Some(expr) = filter {
            let df_schema = DFSchema::try_from(table_schema.clone())?;
            let predicate = create_physical_expr(&expr, &df_schema, state.execution_props())?;
            parquet_source = parquet_source.with_predicate(table_schema.clone(), predicate)
        }

        let fsc = Arc::new(
            FileScanConfig::new(url, table_schema, Arc::new(parquet_source))
                .with_file_groups(parquet_file_groups)
                .with_projection(projection.cloned())
                .with_limit(limit),
        );

        Ok(Arc::new(DataSourceExec::new(fsc)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|expr| {
                if self.can_push_down(expr) {
                    Ok(TableProviderFilterPushDown::Inexact)
                } else {
                    Ok(TableProviderFilterPushDown::Unsupported)
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
/// ```rust
/// use datafusion::prelude::SessionContext;
/// use hudi_datafusion::HudiTableFactory;
///
/// // Initialize a new HudiTableFactory
/// let factory = HudiTableFactory::new();
///     
/// // The factory can be used to create Hudi tables
/// let table = factory.create_table(...)?;
///
///
/// // Using `CREATE EXTERNAL TABLE` to register Hudi table:
/// let test_table =  factory.create_table(...)?; // Table with path + url
/// let ctx = SessionContext::new();
///
/// // Register table in session using `CREATE EXTERNAL TABLE` command
/// let create_table_sql = format!(
///     "CREATE EXTERNAL TABLE {} STORED AS HUDI LOCATION '{}' {}",
///     test_table.as_ref(),
///     test_table.path(),
///     concat_as_sql_options(options)
/// );
/// ctx.sql(create_table_sql.as_str()).await?; // Query against this table
///
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
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_common::{Column, DataFusionError, ScalarValue};
    use std::fs::canonicalize;
    use std::path::Path;
    use std::sync::Arc;
    use url::Url;

    use datafusion::logical_expr::BinaryExpr;
    use hudi_core::config::read::HudiReadConfig::InputPartitions;
    use hudi_core::metadata::meta_field::MetaField;
    use hudi_test::assert_arrow_field_names_eq;
    use hudi_test::SampleTable::{
        V6ComplexkeygenHivestyle, V6Empty, V6Nonpartitioned, V6SimplekeygenHivestyleNoMetafields,
        V6SimplekeygenNonhivestyle, V6SimplekeygenNonhivestyleOverwritetable,
        V6TimebasedkeygenNonhivestyle,
    };
    use hudi_test::{util, SampleTable};
    use util::{get_bool_column, get_i32_column, get_str_column};

    use crate::HudiDataSource;
    use crate::HudiTableFactory;

    #[tokio::test]
    async fn get_default_input_partitions() {
        let base_url =
            Url::from_file_path(canonicalize(Path::new("tests/data/table_props_valid")).unwrap())
                .unwrap();
        let hudi = HudiDataSource::new(base_url.as_str()).await.unwrap();
        assert_eq!(hudi.get_input_partitions(), 0)
    }

    #[tokio::test]
    async fn test_get_create_schema_from_empty_table() {
        let table_provider =
            HudiDataSource::new_with_options(V6Empty.path_to_cow().as_str(), empty_options())
                .await
                .unwrap();
        let schema = table_provider.schema();
        assert_arrow_field_names_eq!(
            schema,
            [MetaField::field_names(), vec!["id", "name", "isActive"]].concat()
        );
    }

    async fn register_test_table_with_session<I, K, V>(
        test_table: &SampleTable,
        options: I,
        use_sql: bool,
    ) -> Result<SessionContext, DataFusionError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let ctx = create_test_session().await;
        if use_sql {
            let create_table_sql = format!(
                "CREATE EXTERNAL TABLE {} STORED AS HUDI LOCATION '{}' {}",
                test_table.as_ref(),
                test_table.path_to_cow(),
                concat_as_sql_options(options)
            );
            ctx.sql(create_table_sql.as_str()).await?;
        } else {
            let base_url = test_table.url_to_cow();
            let hudi = HudiDataSource::new_with_options(base_url.as_str(), options).await?;
            ctx.register_table(test_table.as_ref(), Arc::new(hudi))?;
        }
        Ok(ctx)
    }

    async fn create_test_session() -> SessionContext {
        let config = SessionConfig::new().set(
            "datafusion.sql_parser.enable_ident_normalization",
            &ScalarValue::from(false),
        );
        let table_factory: Arc<dyn TableProviderFactory> = Arc::new(HudiTableFactory::default());

        let session_state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_table_factories(HashMap::from([("HUDI".to_string(), table_factory)]))
            .build();

        SessionContext::new_with_state(session_state)
    }

    fn concat_as_sql_options<I, K, V>(options: I) -> String
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let kv_pairs: Vec<String> = options
            .into_iter()
            .map(|(k, v)| format!("'{}' '{}'", k.as_ref(), v.into()))
            .collect();

        if kv_pairs.is_empty() {
            String::new()
        } else {
            format!("OPTIONS ({})", kv_pairs.join(", "))
        }
    }

    #[tokio::test]
    async fn test_create_table_with_unknown_format() {
        let test_table = V6Nonpartitioned;
        let invalid_format = "UNKNOWN_FORMAT";
        let create_table_sql = format!(
            "CREATE EXTERNAL TABLE {} STORED AS {} LOCATION '{}'",
            test_table.as_ref(),
            invalid_format,
            test_table.path_to_cow()
        );

        let ctx = create_test_session().await;
        let result = ctx.sql(create_table_sql.as_str()).await;
        assert!(result.is_err());
    }

    async fn verify_plan(
        ctx: &SessionContext,
        sql: &str,
        table_name: &str,
        planned_input_partitioned: &i32,
    ) {
        let explaining_df = ctx.sql(sql).await.unwrap().explain(false, true).unwrap();
        let explaining_rb = explaining_df.collect().await.unwrap();
        let explaining_rb = explaining_rb.first().unwrap();
        let plan = get_str_column(explaining_rb, "plan").join("");
        let plan_lines: Vec<&str> = plan.lines().map(str::trim).collect();
        assert!(plan_lines[1].starts_with("SortExec: TopK(fetch=10)"));
        assert!(plan_lines[2].starts_with(&format!(
            "ProjectionExec: expr=[id@0 as id, name@1 as name, isActive@2 as isActive, \
            get_field(structField@3, field2) as {}.structField[field2]]",
            table_name
        )));
        assert!(plan_lines[4].starts_with(
            "FilterExec: CAST(id@0 AS Int64) % 2 = 0 AND name@1 != Alice AND get_field(structField@3, field2) > 30"
        ));
        assert!(plan_lines[5].contains(&format!("input_partitions={}", planned_input_partitioned)));
    }

    async fn verify_data(ctx: &SessionContext, sql: &str, table_name: &str) {
        let df = ctx.sql(sql).await.unwrap();
        let rb = df.collect().await.unwrap();
        let rb = rb.first().unwrap();
        assert_eq!(get_i32_column(rb, "id"), &[2, 4]);
        assert_eq!(get_str_column(rb, "name"), &["Bob", "Diana"]);
        assert_eq!(get_bool_column(rb, "isActive"), &[false, true]);
        assert_eq!(
            get_i32_column(rb, &format!("{}.structField[field2]", table_name)),
            &[40, 50]
        );
    }

    #[tokio::test]
    async fn test_datafusion_read_hudi_table_with_partition_filter_pushdown() {
        for (test_table, use_sql, planned_input_partitions) in &[
            (V6ComplexkeygenHivestyle, true, 2),
            (V6Nonpartitioned, true, 1),
            (V6SimplekeygenNonhivestyle, false, 2),
            (V6SimplekeygenHivestyleNoMetafields, true, 2),
            (V6TimebasedkeygenNonhivestyle, false, 2),
        ] {
            println!(">>> testing for {}", test_table.as_ref());
            let options = [(InputPartitions, "2")];
            let ctx = register_test_table_with_session(test_table, options, *use_sql)
                .await
                .unwrap();

            let sql = format!(
                r#"
            SELECT id, name, isActive, structField.field2
            FROM {} WHERE id % 2 = 0 AND name != 'Alice'
            AND structField.field2 > 30 ORDER BY name LIMIT 10"#,
                test_table.as_ref()
            );

            verify_plan(&ctx, &sql, test_table.as_ref(), planned_input_partitions).await;
            verify_data(&ctx, &sql, test_table.as_ref()).await
        }
    }

    async fn verify_data_with_replacecommits(ctx: &SessionContext, sql: &str, table_name: &str) {
        let df = ctx.sql(sql).await.unwrap();
        let rb = df.collect().await.unwrap();
        let rb = rb.first().unwrap();
        assert_eq!(get_i32_column(rb, "id"), &[4]);
        assert_eq!(get_str_column(rb, "name"), &["Diana"]);
        assert_eq!(get_bool_column(rb, "isActive"), &[false]);
        assert_eq!(
            get_i32_column(rb, &format!("{}.structField[field2]", table_name)),
            &[50]
        );
    }

    #[tokio::test]
    async fn test_datafusion_read_hudi_table_with_replacecommits_with_partition_filter_pushdown() {
        for (test_table, use_sql, planned_input_partitions) in
            &[(V6SimplekeygenNonhivestyleOverwritetable, true, 1)]
        {
            println!(">>> testing for {}", test_table.as_ref());
            let ctx =
                register_test_table_with_session(test_table, [(InputPartitions, "2")], *use_sql)
                    .await
                    .unwrap();

            let sql = format!(
                r#"
            SELECT id, name, isActive, structField.field2
            FROM {} WHERE id % 2 = 0 AND name != 'Alice'
            AND structField.field2 > 30 ORDER BY name LIMIT 10"#,
                test_table.as_ref()
            );

            verify_plan(&ctx, &sql, test_table.as_ref(), planned_input_partitions).await;
            verify_data_with_replacecommits(&ctx, &sql, test_table.as_ref()).await
        }
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
            right: Box::new(Expr::Literal(ScalarValue::Utf8(Some("Alice".to_string())))),
        });

        let expr1 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("intField".to_string()))),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Int32(Some(20000)))),
        });

        let expr2 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name(
                "nonexistent_column".to_string(),
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int32(Some(1)))),
        });

        let expr3 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("name".to_string()))),
            op: Operator::NotEq,
            right: Box::new(Expr::Literal(ScalarValue::Utf8(Some("Diana".to_string())))),
        });

        let expr4 = Expr::Literal(ScalarValue::Int32(Some(10)));

        let expr5 = Expr::Not(Box::new(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("intField".to_string()))),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Int32(Some(20000)))),
        })));

        let filters = vec![&expr0, &expr1, &expr2, &expr3, &expr4, &expr5];
        let result = table_provider.supports_filters_pushdown(&filters).unwrap();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0], TableProviderFilterPushDown::Inexact);
        assert_eq!(result[1], TableProviderFilterPushDown::Inexact);
        assert_eq!(result[2], TableProviderFilterPushDown::Unsupported);
        assert_eq!(result[3], TableProviderFilterPushDown::Inexact);
        assert_eq!(result[4], TableProviderFilterPushDown::Unsupported);
        assert_eq!(result[5], TableProviderFilterPushDown::Inexact);
    }
}
