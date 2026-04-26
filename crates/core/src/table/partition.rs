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
use crate::Result;
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig;
use crate::error::CoreError::{self, InvalidPartitionPath};
use crate::expr::filter::{Filter, SchemableFilter};
use crate::keygen::KeyGeneratorFilterTransformer;
use crate::keygen::timestamp_based::TimestampBasedKeyGenerator;

use arrow_array::{ArrayRef, Scalar};
use arrow_schema::{Field, Schema};

use crate::config::table::HudiTableConfig::{KeyGeneratorClass, KeyGeneratorType, PartitionFields};
use crate::keygen::is_timestamp_based_keygen;
use crate::metadata::meta_field::MetaField;
use std::collections::HashMap;
use std::sync::Arc;

pub const PARTITION_METAFIELD_PREFIX: &str = ".hoodie_partition_metadata";
pub const EMPTY_PARTITION_PATH: &str = "";

/// Build the partition [Schema] in the order declared by `partition_field_names`.
///
/// The resulting schema order must match the on-disk partition path segment order
/// (which follows the `hoodie.table.partition.fields` config), not the arbitrary
/// column order of the underlying Parquet schema. Returns an error if any declared
/// partition field is not present in `table_schema`: silently dropping such a field
/// would make the schema and on-disk path lengths disagree, which `parse_segments`
/// rejects and `should_include` then treats as fail-open (full-table scan).
pub(crate) fn project_partition_schema(
    table_schema: &Schema,
    partition_field_names: &[String],
) -> Result<Schema> {
    let fields: Vec<Arc<Field>> = partition_field_names
        .iter()
        .map(|name| {
            table_schema
                .field_with_name(name)
                .map(|f| Arc::new(f.clone()))
                .map_err(|_| {
                    CoreError::Schema(format!(
                        "Partition field `{name}` declared in \
                         `hoodie.table.partition.fields` is not present in the table schema"
                    ))
                })
        })
        .collect::<Result<_>>()?;
    Ok(Schema::new(fields))
}

pub fn is_table_partitioned(hudi_configs: &HudiConfigs) -> bool {
    let has_partition_fields = {
        let partition_fields: Vec<String> = hudi_configs.get_or_default(PartitionFields).into();
        !partition_fields.is_empty()
    };

    let uses_non_partitioned_key_gen = hudi_configs
        .try_get(KeyGeneratorClass)
        .map(|key_gen| {
            let key_gen_str: String = key_gen.into();
            key_gen_str == "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
        })
        .unwrap_or(false);

    let uses_non_partitioned_type = hudi_configs
        .try_get(KeyGeneratorType)
        .map(|v| {
            let s: String = v.into();
            let upper = s.to_uppercase();
            upper == "NON_PARTITION" || upper == "NON_PARTITION_AVRO"
        })
        .unwrap_or(false);

    has_partition_fields && !uses_non_partitioned_key_gen && !uses_non_partitioned_type
}

/// A partition pruner that filters partitions based on the partition path and its filters.
#[derive(Debug, Clone)]
pub struct PartitionPruner {
    schema: Arc<Schema>,
    is_hive_style: bool,
    is_url_encoded: bool,
    is_partitioned: bool,
    and_filters: Vec<SchemableFilter>,
}

impl PartitionPruner {
    pub fn new(
        and_filters: &[Filter],
        partition_schema: &Schema,
        hudi_configs: &HudiConfigs,
    ) -> Result<Self> {
        // Transform filters based on key generator configuration
        let transformed_filters =
            Self::transform_filters_for_keygen(and_filters, partition_schema, hudi_configs)?;

        let and_filters: Vec<SchemableFilter> = transformed_filters
            .iter()
            .filter_map(|filter| SchemableFilter::try_from((filter.clone(), partition_schema)).ok())
            .collect();

        let schema = Arc::new(partition_schema.clone());
        let is_hive_style: bool = hudi_configs
            .get_or_default(HudiTableConfig::IsHiveStylePartitioning)
            .into();
        let is_url_encoded: bool = hudi_configs
            .get_or_default(HudiTableConfig::IsPartitionPathUrlencoded)
            .into();
        let is_partitioned = is_table_partitioned(hudi_configs);
        Ok(PartitionPruner {
            schema,
            is_hive_style,
            is_url_encoded,
            is_partitioned,
            and_filters,
        })
    }

    /// Creates an empty partition pruner that does not filter any partitions.
    pub fn empty() -> Self {
        PartitionPruner {
            schema: Arc::new(Schema::empty()),
            is_hive_style: false,
            is_url_encoded: false,
            is_partitioned: false,
            and_filters: Vec::new(),
        }
    }

    /// Returns `true` if the partition pruner does not have any filters.
    pub fn is_empty(&self) -> bool {
        self.and_filters.is_empty()
    }

    /// Returns `true` if the table is partitioned.
    pub fn is_table_partitioned(&self) -> bool {
        self.is_partitioned
    }

    /// Returns `true` if the partition path should be included based on the filters.
    pub fn should_include(&self, partition_path: &str) -> bool {
        let segments = match self.parse_segments(partition_path) {
            Ok(s) => s,
            Err(_) => return true, // Include the partition regardless of parsing error
        };

        self.and_filters.iter().all(|filter| {
            match segments.get(filter.field.name()) {
                Some(segment_value) => {
                    match filter.apply_comparison(segment_value) {
                        Ok(scalar) => scalar.value(0),
                        Err(_) => true, // Include the partition when comparison error occurs
                    }
                }
                None => true, // Include the partition when filtering field does not match any field in the partition
            }
        })
    }

    /// Transforms user filters on data columns to filters on partition path columns
    /// based on the configured key generator.
    fn transform_filters_for_keygen(
        filters: &[Filter],
        _partition_schema: &Schema,
        hudi_configs: &HudiConfigs,
    ) -> Result<Vec<Filter>> {
        if is_timestamp_based_keygen(hudi_configs) {
            match TimestampBasedKeyGenerator::from_configs(hudi_configs) {
                Ok(transformer) => {
                    return Self::apply_transformer_to_filters(filters, &transformer);
                }
                Err(e) => {
                    log::warn!(
                        "Failed to create TimestampBasedKeyGenerator: {e}. \
                         Filters will not be transformed."
                    );
                }
            }
        }

        Ok(filters.to_vec())
    }

    fn apply_transformer_to_filters(
        filters: &[Filter],
        transformer: &dyn KeyGeneratorFilterTransformer,
    ) -> Result<Vec<Filter>> {
        let mut transformed = Vec::new();
        for filter in filters {
            let partition_filters = transformer.transform_filter(filter)?;
            transformed.extend(partition_filters);
        }
        Ok(transformed)
    }

    fn parse_segments(&self, partition_path: &str) -> Result<HashMap<String, Scalar<ArrayRef>>> {
        let partition_path = if self.is_url_encoded {
            percent_encoding::percent_decode(partition_path.as_bytes())
                .decode_utf8()?
                .into_owned()
        } else {
            partition_path.to_string()
        };

        // Special case: single _hoodie_partition_path field uses the raw path as-is
        if self.schema.fields().len() == 1
            && self.schema.field(0).name() == MetaField::PartitionPath.as_ref()
        {
            let scalar = SchemableFilter::cast_value(
                &[partition_path.as_str()],
                &arrow_schema::DataType::Utf8,
            )?;
            return Ok(HashMap::from([(
                MetaField::PartitionPath.as_ref().to_string(),
                scalar,
            )]));
        }

        let parts: Vec<&str> = partition_path.split('/').collect();

        if parts.len() != self.schema.fields().len() {
            return Err(InvalidPartitionPath(format!(
                "Partition path should have {} part(s) but got {}",
                self.schema.fields().len(),
                parts.len()
            )));
        }

        self.schema
            .fields()
            .iter()
            .zip(parts)
            .map(|(field, part)| {
                let value = if self.is_hive_style {
                    let (name, value) = part.split_once('=').ok_or(InvalidPartitionPath(
                        format!("Partition path should be hive-style but got {part}"),
                    ))?;
                    if name != field.name() {
                        return Err(InvalidPartitionPath(format!(
                            "Partition path should contain {} but got {}",
                            field.name(),
                            name
                        )));
                    }
                    value
                } else {
                    part
                };
                let scalar = SchemableFilter::cast_value(&[value], field.data_type())?;
                Ok((field.name().to_string(), scalar))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig::{
        IsHiveStylePartitioning, IsPartitionPathUrlencoded,
    };
    use crate::expr::ExprOperator;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::Date32Array;
    use std::str::FromStr;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ])
    }

    fn create_hudi_configs(is_hive_style: bool, is_url_encoded: bool) -> HudiConfigs {
        HudiConfigs::new([
            (IsHiveStylePartitioning, is_hive_style.to_string()),
            (IsPartitionPathUrlencoded, is_url_encoded.to_string()),
        ])
    }

    #[test]
    fn project_partition_schema_preserves_config_order() {
        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("integration_id", DataType::Utf8, false),
            Field::new("resource_type", DataType::Utf8, false),
            Field::new("org", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, false),
        ]);
        let partition_field_names = vec![
            "org".to_string(),
            "resource_type".to_string(),
            "integration_id".to_string(),
        ];

        let projected = project_partition_schema(&table_schema, &partition_field_names).unwrap();

        assert_eq!(projected.fields().len(), 3);
        assert_eq!(projected.field(0).name(), "org");
        assert_eq!(projected.field(1).name(), "resource_type");
        assert_eq!(projected.field(2).name(), "integration_id");
    }

    #[test]
    fn project_partition_schema_errors_when_field_missing_from_table_schema() {
        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("org", DataType::Utf8, false),
        ]);
        let partition_field_names = vec!["org".to_string(), "not_in_schema".to_string()];

        let err = project_partition_schema(&table_schema, &partition_field_names).unwrap_err();

        assert!(matches!(err, CoreError::Schema(_)));
        assert!(err.to_string().contains("not_in_schema"));
    }

    #[test]
    fn project_partition_schema_empty_config_returns_empty_schema() {
        let table_schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let projected = project_partition_schema(&table_schema, &[]).unwrap();
        assert_eq!(projected.fields().len(), 0);
    }

    #[test]
    fn test_partition_pruner_new() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);

        let filter_gt_date = Filter::try_from(("date", ">", "2023-01-01")).unwrap();
        let filter_eq_a = Filter::try_from(("category", "=", "A")).unwrap();

        let pruner = PartitionPruner::new(&[filter_gt_date, filter_eq_a], &schema, &configs);
        assert!(pruner.is_ok());

        let pruner = pruner.unwrap();
        assert_eq!(pruner.and_filters.len(), 2);
        assert!(pruner.is_hive_style);
        assert!(!pruner.is_url_encoded);
    }

    #[test]
    fn test_partition_pruner_empty() {
        let pruner = PartitionPruner::empty();
        assert!(pruner.is_empty());
        assert!(!pruner.is_hive_style);
        assert!(!pruner.is_url_encoded);
    }

    #[test]
    fn test_partition_pruner_is_empty() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(false, false);

        let pruner_empty = PartitionPruner::new(&[], &schema, &configs).unwrap();
        assert!(pruner_empty.is_empty());

        let filter_gt_date = Filter::try_from(("date", ">", "2023-01-01")).unwrap();
        let pruner_non_empty = PartitionPruner::new(&[filter_gt_date], &schema, &configs).unwrap();
        assert!(!pruner_non_empty.is_empty());
    }

    #[test]
    fn test_partition_pruner_should_include() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);

        let filter_gt_date = Filter::try_from(("date", ">", "2023-01-01")).unwrap();
        let filter_eq_a = Filter::try_from(("category", "=", "A")).unwrap();
        let filter_lte_100 = Filter::try_from(("count", "<=", "100")).unwrap();

        let pruner = PartitionPruner::new(
            &[filter_gt_date, filter_eq_a, filter_lte_100],
            &schema,
            &configs,
        )
        .unwrap();

        assert!(pruner.should_include("date=2023-02-01/category=A/count=10"));
        assert!(pruner.should_include("date=2023-02-01/category=A/count=100"));
        assert!(!pruner.should_include("date=2022-12-31/category=A/count=10"));
        assert!(!pruner.should_include("date=2023-02-01/category=B/count=10"));
    }

    #[test]
    fn test_partition_pruner_parse_segments() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);
        let pruner = PartitionPruner::new(&[], &schema, &configs).unwrap();

        let segments = pruner
            .parse_segments("date=2023-02-01/category=A/count=10")
            .unwrap();
        assert_eq!(segments.len(), 3);
        assert!(segments.contains_key("date"));
        assert!(segments.contains_key("category"));
        assert!(segments.contains_key("count"));
    }

    #[test]
    fn test_partition_pruner_url_encoded() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, true);
        let pruner = PartitionPruner::new(&[], &schema, &configs).unwrap();

        let segments = pruner
            .parse_segments("date%3D2023-02-01%2Fcategory%3DA%2Fcount%3D10")
            .unwrap();
        assert_eq!(segments.len(), 3);
        assert!(segments.contains_key("date"));
        assert!(segments.contains_key("category"));
        assert!(segments.contains_key("count"));
    }

    #[test]
    fn test_partition_pruner_invalid_path() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);
        let pruner = PartitionPruner::new(&[], &schema, &configs).unwrap();

        let result = pruner.parse_segments("date=2023-02-01/category=A/count=10/extra");
        assert!(matches!(result.unwrap_err(), InvalidPartitionPath(_)));

        let result = pruner.parse_segments("date=2023-02-01/category=A/10");
        assert!(matches!(result.unwrap_err(), InvalidPartitionPath(_)));

        let result = pruner.parse_segments("date=2023-02-01/category=A/non_exist_field=10");
        assert!(matches!(result.unwrap_err(), InvalidPartitionPath(_)));
    }

    #[test]
    fn test_partition_filter_try_from_valid() {
        let schema = create_test_schema();
        let filter = Filter {
            field_name: "date".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["2023-01-01".to_string()],
        };

        let partition_filter = SchemableFilter::try_from((filter, &schema)).unwrap();
        assert_eq!(partition_filter.field.name(), "date");
        assert_eq!(partition_filter.operator, ExprOperator::Eq);

        let value_inner = partition_filter.values[0].clone().into_inner();

        let date_array = value_inner.as_any().downcast_ref::<Date32Array>().unwrap();

        let date_value = date_array.value_as_date(0).unwrap();
        assert_eq!(date_value.to_string(), "2023-01-01");
    }

    #[test]
    fn test_partition_filter_try_from_invalid_field() {
        let schema = create_test_schema();
        let filter = Filter {
            field_name: "invalid_field".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["2023-01-01".to_string()],
        };
        let result = SchemableFilter::try_from((filter, &schema));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Field invalid_field not found in schema")
        );
    }

    #[test]
    fn test_partition_filter_try_from_invalid_value() {
        let schema = create_test_schema();
        let filter = Filter {
            field_name: "count".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["not_a_number".to_string()],
        };
        let result = SchemableFilter::try_from((filter, &schema));
        assert!(result.is_err());
    }

    #[test]
    fn test_partition_filter_try_from_all_operators() {
        let schema = create_test_schema();
        for (op, op_enum) in ExprOperator::TOKEN_OP_PAIRS {
            let filter = Filter::new("count".to_string(), op_enum, vec!["5".to_string()]).unwrap();
            let partition_filter = SchemableFilter::try_from((filter, &schema));
            let filter = partition_filter.unwrap();
            assert_eq!(filter.field.name(), "count");
            assert_eq!(filter.operator, ExprOperator::from_str(op).unwrap());
        }
    }

    #[test]
    fn test_transform_filters_for_keygen_timestamp_based() {
        let partition_schema = Schema::new(vec![Field::new(
            MetaField::PartitionPath.as_ref(),
            DataType::Utf8,
            false,
        )]);

        // Range filter: DATE_STRING Gte → single _hoodie_partition_path Gte
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts_str"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "DATE_STRING"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);

        let user_filter = Filter {
            field_name: "ts_str".to_string(),
            operator: ExprOperator::Gte,
            values: vec!["2023-04-15T12:00:00.000Z".to_string()],
        };

        let transformed = PartitionPruner::transform_filters_for_keygen(
            &[user_filter],
            &partition_schema,
            &configs,
        )
        .unwrap();

        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].field_name, MetaField::PartitionPath.as_ref());
        assert_eq!(transformed[0].operator, ExprOperator::Gte);
        assert_eq!(transformed[0].values[0], "year=2023/month=04/day=15");

        // Equality filter: UNIX_TIMESTAMP Eq → single path
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "event_time"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "UNIX_TIMESTAMP"),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "false"),
        ]);

        // 2024-01-25 00:00:00 UTC = 1706140800 seconds
        let user_filter = Filter {
            field_name: "event_time".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["1706140800".to_string()],
        };

        let transformed = PartitionPruner::transform_filters_for_keygen(
            &[user_filter],
            &partition_schema,
            &configs,
        )
        .unwrap();

        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].field_name, MetaField::PartitionPath.as_ref());
        assert_eq!(transformed[0].values[0], "2024/01/25");

        // v8 detection via keygenerator.type=TIMESTAMP (no keygenerator.class)
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts_str"),
            ("hoodie.table.keygenerator.type", "TIMESTAMP"),
            ("hoodie.keygen.timebased.timestamp.type", "DATE_STRING"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);

        let user_filter = Filter {
            field_name: "ts_str".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["2023-04-15T12:00:00.000Z".to_string()],
        };

        let transformed = PartitionPruner::transform_filters_for_keygen(
            &[user_filter],
            &partition_schema,
            &configs,
        )
        .unwrap();

        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].values[0], "year=2023/month=04/day=15");
    }

    #[test]
    fn test_transform_filters_for_keygen_no_transformation() {
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "region"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.SimpleKeyGenerator",
            ),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);

        let partition_schema = Schema::new(vec![Field::new("region", DataType::Utf8, false)]);

        let user_filter = Filter {
            field_name: "region".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["us-west".to_string()],
        };

        let transformed = PartitionPruner::transform_filters_for_keygen(
            &[user_filter.clone()],
            &partition_schema,
            &configs,
        )
        .unwrap();

        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].field_name, user_filter.field_name);
        assert_eq!(transformed[0].values[0], user_filter.values[0]);
    }

    #[test]
    fn test_partition_pruner_with_timestamp_keygen() {
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "DATE_STRING"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd'T'HH:mm:ssZ",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
            ("hoodie.datasource.write.partitionpath.urlencode", "false"),
        ]);

        let partition_schema = Schema::new(vec![Field::new(
            MetaField::PartitionPath.as_ref(),
            DataType::Utf8,
            false,
        )]);

        let user_filter = Filter {
            field_name: "ts".to_string(),
            operator: ExprOperator::Gte,
            values: vec!["2024-01-15T00:00:00Z".to_string()],
        };

        let pruner = PartitionPruner::new(&[user_filter], &partition_schema, &configs).unwrap();

        assert!(!pruner.is_empty());

        // Full path string comparison on _hoodie_partition_path
        assert!(pruner.should_include("year=2024/month=01/day=15"));
        assert!(pruner.should_include("year=2024/month=06/day=30"));
        assert!(pruner.should_include("year=2025/month=01/day=01"));

        // Should exclude partitions < 2024/01/15
        assert!(!pruner.should_include("year=2023/month=12/day=31"));
        assert!(!pruner.should_include("year=2022/month=01/day=01"));

        // Non-hive-style: verify the same logic works
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "DATE_STRING"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd'T'HH:mm:ssZ",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "false"),
        ]);

        let user_filter = Filter {
            field_name: "ts".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["2024-01-15T00:00:00Z".to_string()],
        };

        let pruner = PartitionPruner::new(&[user_filter], &partition_schema, &configs).unwrap();
        assert!(pruner.should_include("2024/01/15"));
        assert!(!pruner.should_include("2024/01/16"));
        assert!(!pruner.should_include("2023/12/31"));
    }
}
