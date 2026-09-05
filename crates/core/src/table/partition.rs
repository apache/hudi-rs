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

pub fn is_table_partitioned(hudi_configs: &HudiConfigs) -> Result<bool> {
    let has_partition_fields = {
        let partition_fields: Vec<String> = hudi_configs.get_or_default(PartitionFields).into();
        !partition_fields.is_empty()
    };

    let uses_non_partitioned_key_gen = hudi_configs
        .try_get(KeyGeneratorClass)?
        .map(|key_gen| {
            let key_gen_str: String = key_gen.into();
            key_gen_str == "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
        })
        .unwrap_or(false);

    let uses_non_partitioned_type = hudi_configs
        .try_get(KeyGeneratorType)?
        .map(|v| {
            let s: String = v.into();
            let upper = s.to_uppercase();
            upper == "NON_PARTITION" || upper == "NON_PARTITION_AVRO"
        })
        .unwrap_or(false);

    Ok(has_partition_fields && !uses_non_partitioned_key_gen && !uses_non_partitioned_type)
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
        let is_partitioned = is_table_partitioned(hudi_configs)?;
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
        if is_timestamp_based_keygen(hudi_configs)? {
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
    fn test_hoodie_partition_path_whole_path_pruning() {
        // A timestamp key generator rewrites a source-column predicate into a predicate on
        // the opaque whole-path meta field, so the pruner holds a single-field schema and
        // must compare the path as one string rather than split it into segments.
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
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd/HH"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
            ("hoodie.datasource.write.partitionpath.urlencode", "false"),
        ]);
        let partition_schema = Schema::new(vec![Field::new(
            MetaField::PartitionPath.as_ref(),
            DataType::Utf8,
            false,
        )]);
        let filter = Filter::try_from(("ts_str", "=", "2023-04-01T12:01:00.123Z")).unwrap();

        let pruner = PartitionPruner::new(&[filter], &partition_schema, &configs).unwrap();
        assert!(!pruner.is_empty());
        assert!(pruner.should_include("ts_str=2023/04/01/12"));
        assert!(!pruner.should_include("ts_str=2023/05/01/08"));
    }

    #[test]
    fn test_hoodie_partition_path_whole_path_pruning_url_encoded() {
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
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd/HH"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
            ("hoodie.datasource.write.partitionpath.urlencode", "true"),
        ]);
        let partition_schema = Schema::new(vec![Field::new(
            MetaField::PartitionPath.as_ref(),
            DataType::Utf8,
            false,
        )]);
        let filter = Filter::try_from(("ts_str", "=", "2023-04-01T12:01:00.123Z")).unwrap();

        let pruner = PartitionPruner::new(&[filter], &partition_schema, &configs).unwrap();
        // The encoded path decodes to `ts_str=2023/04/01/12` before comparison.
        assert!(pruner.should_include("ts_str%3D2023%2F04%2F01%2F12"));
        assert!(!pruner.should_include("ts_str%3D2023%2F05%2F01%2F08"));
    }

    #[test]
    fn test_is_table_partitioned_keygen_declarations() {
        // Declaring partition fields alone keeps the table partitioned.
        let configs = HudiConfigs::new([("hoodie.table.partition.fields", "ts")]);
        assert!(is_table_partitioned(&configs).unwrap());

        // The non-partitioned key generator class cancels the partition fields.
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
            ),
        ]);
        assert!(!is_table_partitioned(&configs).unwrap());

        // So does the short key-generator type, in either accepted spelling.
        for keygen_type in ["non_partition", "NON_PARTITION_AVRO"] {
            let configs = HudiConfigs::new([
                ("hoodie.table.partition.fields", "ts"),
                ("hoodie.table.keygenerator.type", keygen_type),
            ]);
            assert!(!is_table_partitioned(&configs).unwrap(), "{keygen_type}");
        }
    }

    #[test]
    fn test_should_include_tolerates_foreign_field_and_comparison_error() {
        // Fail-open arms of should_include: a filter whose field is not among the path
        // segments, and a comparison that fails on a type mismatch, must both keep the
        // partition rather than silently dropping data.
        let bound = SchemableFilter::try_from((
            Filter::try_from(("count", ">", "5")).unwrap(),
            &create_test_schema(),
        ))
        .unwrap();

        // The bound field `count` is not part of this path's segment map.
        let foreign_field = PartitionPruner {
            schema: Arc::new(Schema::new(vec![Field::new(
                "date",
                DataType::Date32,
                false,
            )])),
            is_hive_style: false,
            is_url_encoded: false,
            is_partitioned: true,
            and_filters: vec![bound.clone()],
        };
        assert!(foreign_field.should_include("2023-02-01"));

        // The same Int32-bound filter meets a Utf8 segment value: the arrow kernel
        // rejects the comparison and the partition is kept.
        let mismatched = PartitionPruner {
            schema: Arc::new(Schema::new(vec![Field::new(
                "count",
                DataType::Utf8,
                false,
            )])),
            is_hive_style: false,
            is_url_encoded: false,
            is_partitioned: true,
            and_filters: vec![bound],
        };
        assert!(mismatched.should_include("not_a_number"));
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
            field: "date".to_string(),
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
            field: "invalid_field".to_string(),
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
            field: "count".to_string(),
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
            field: "ts_str".to_string(),
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
        assert_eq!(transformed[0].field, MetaField::PartitionPath.as_ref());
        assert_eq!(transformed[0].operator, ExprOperator::Gte);
        assert_eq!(transformed[0].values[0], "ts_str=2023/04/15");

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
            field: "event_time".to_string(),
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
        assert_eq!(transformed[0].field, MetaField::PartitionPath.as_ref());
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
            field: "ts_str".to_string(),
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
        assert_eq!(transformed[0].values[0], "ts_str=2023/04/15");
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
            field: "region".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["us-west".to_string()],
        };

        let transformed = PartitionPruner::transform_filters_for_keygen(
            std::slice::from_ref(&user_filter),
            &partition_schema,
            &configs,
        )
        .unwrap();

        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].field, user_filter.field);
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
            field: "ts".to_string(),
            operator: ExprOperator::Gte,
            values: vec!["2024-01-15T00:00:00Z".to_string()],
        };

        let pruner = PartitionPruner::new(&[user_filter], &partition_schema, &configs).unwrap();

        assert!(!pruner.is_empty());

        // Full path string comparison on _hoodie_partition_path
        assert!(pruner.should_include("ts=2024/01/15"));
        assert!(pruner.should_include("ts=2024/06/30"));
        assert!(pruner.should_include("ts=2025/01/01"));

        // Should exclude partitions < 2024/01/15
        assert!(!pruner.should_include("ts=2023/12/31"));
        assert!(!pruner.should_include("ts=2022/01/01"));

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
            field: "ts".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["2024-01-15T00:00:00Z".to_string()],
        };

        let pruner = PartitionPruner::new(&[user_filter], &partition_schema, &configs).unwrap();
        assert!(pruner.should_include("2024/01/15"));
        assert!(!pruner.should_include("2024/01/16"));
        assert!(!pruner.should_include("2023/12/31"));
    }

    /// A negated predicate must not drop a partition. The transform reduces a timestamp to
    /// a day, so `2024-03-01` holds every row of that day and all but one instant of it
    /// satisfies `ts != 2024-03-01T14:30:00Z`. Pruning the partition would lose them all.
    #[test]
    fn ne_does_not_prune_a_partition_that_holds_matching_rows() {
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
            ("hoodie.keygen.timebased.output.dateformat", "yyyy-MM-dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "false"),
        ]);
        let partition_schema = Schema::new(vec![Field::new(
            MetaField::PartitionPath.as_ref(),
            DataType::Utf8,
            false,
        )]);

        let pruner = PartitionPruner::new(
            &[Filter {
                field: "ts".to_string(),
                operator: ExprOperator::Ne,
                values: vec!["2024-03-01T14:30:00Z".to_string()],
            }],
            &partition_schema,
            &configs,
        )
        .unwrap();

        assert!(
            pruner.should_include("2024-03-01"),
            "the day containing the excluded instant still holds matching rows"
        );
        assert!(pruner.should_include("2024-03-02"));
    }

    // ---------------------------------------------------------------------------------
    // Hive-style timestamp-keygen pruning. Every timestamp-keygen fixture in
    // crates/test/data is non-hive-style, so this branch previously had no table-backed
    // coverage at all.
    // ---------------------------------------------------------------------------------

    fn hive_style_ts_configs(output_dateformat: &str) -> HudiConfigs {
        HudiConfigs::new([
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
            (
                "hoodie.keygen.timebased.output.dateformat",
                output_dateformat,
            ),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
            ("hoodie.datasource.write.partitionpath.urlencode", "false"),
        ])
    }

    fn partition_path_schema() -> Schema {
        Schema::new(vec![Field::new(
            MetaField::PartitionPath.as_ref(),
            DataType::Utf8,
            false,
        )])
    }

    fn ts_filter(operator: ExprOperator, value: &str) -> Filter {
        Filter {
            field: "ts".to_string(),
            operator,
            values: vec![value.to_string()],
        }
    }

    /// The hive prefix is the declared partition field, applied once to the whole formatted
    /// value. It never comes from the format tokens.
    #[test]
    fn hive_style_path_uses_declared_field_name_not_format_tokens() {
        for (output_dateformat, expected) in [
            ("yyyy-MM-dd", "ts=2024-03-01"),
            ("yyyy/MM/dd", "ts=2024/03/01"),
            ("yyyy-MM-dd-HH", "ts=2024-03-01-08"),
            ("yyyyMMdd", "ts=20240301"),
        ] {
            let keygen =
                TimestampBasedKeyGenerator::from_configs(&hive_style_ts_configs(output_dateformat))
                    .unwrap();
            let transformed = keygen
                .transform_filter(&ts_filter(ExprOperator::Eq, "2024-03-01T08:00:00Z"))
                .unwrap();
            assert_eq!(
                transformed[0].values[0], expected,
                "output.dateformat {output_dateformat}"
            );
        }
    }

    /// A predicate no partition can satisfy must exclude every partition.
    #[test]
    fn impossible_predicate_excludes_every_day_partition() {
        let pruner = PartitionPruner::new(
            &[ts_filter(ExprOperator::Eq, "1900-01-01T00:00:00Z")],
            &partition_path_schema(),
            &hive_style_ts_configs("yyyy-MM-dd"),
        )
        .unwrap();

        for path in ["ts=2024-03-01", "ts=2024-03-02", "ts=2023-12-31"] {
            assert!(!pruner.should_include(path), "{path} should be excluded");
        }
    }

    #[test]
    fn day_granularity_range_selects_only_days_in_range() {
        let pruner = PartitionPruner::new(
            &[
                ts_filter(ExprOperator::Gte, "2024-03-01T00:00:00Z"),
                ts_filter(ExprOperator::Lt, "2024-03-04T00:00:00Z"),
            ],
            &partition_path_schema(),
            &hive_style_ts_configs("yyyy-MM-dd"),
        )
        .unwrap();

        for included in ["ts=2024-03-01", "ts=2024-03-02", "ts=2024-03-03"] {
            assert!(pruner.should_include(included), "{included}");
        }
        for excluded in ["ts=2024-02-29", "ts=2024-03-05", "ts=2025-01-01"] {
            assert!(!pruner.should_include(excluded), "{excluded}");
        }
    }

    /// A half-open upper bound widens from `<` to `<=` at partition granularity, so the
    /// boundary partition is retained. That is deliberate: `ts < 2024-03-02T06:00:00Z`
    /// genuinely has matching rows inside `ts=2024-03-02`, and the transform has already
    /// discarded the time of day, so the partition cannot be ruled out here. Row-level
    /// evaluation removes the rows that do not match.
    #[test]
    fn half_open_upper_bound_retains_the_boundary_partition() {
        let pruner = PartitionPruner::new(
            &[
                ts_filter(ExprOperator::Gte, "2024-03-01T00:00:00Z"),
                ts_filter(ExprOperator::Lt, "2024-03-02T00:00:00Z"),
            ],
            &partition_path_schema(),
            &hive_style_ts_configs("yyyy-MM-dd"),
        )
        .unwrap();

        assert!(pruner.should_include("ts=2024-03-01"));
        assert!(pruner.should_include("ts=2024-03-02"));
        assert!(!pruner.should_include("ts=2024-03-03"));
        assert!(!pruner.should_include("ts=2024-02-29"));
    }

    #[test]
    fn hour_granularity_range_spans_a_day_boundary() {
        let pruner = PartitionPruner::new(
            &[
                ts_filter(ExprOperator::Gte, "2024-03-01T22:00:00Z"),
                ts_filter(ExprOperator::Lte, "2024-03-02T01:00:00Z"),
            ],
            &partition_path_schema(),
            &hive_style_ts_configs("yyyy-MM-dd-HH"),
        )
        .unwrap();

        for included in [
            "ts=2024-03-01-22",
            "ts=2024-03-01-23",
            "ts=2024-03-02-00",
            "ts=2024-03-02-01",
        ] {
            assert!(pruner.should_include(included), "{included}");
        }
        for excluded in ["ts=2024-03-01-21", "ts=2024-03-02-02"] {
            assert!(!pruner.should_include(excluded), "{excluded}");
        }
    }

    /// Leap day and year rollover are ordinary calendar arithmetic, but a format that
    /// zero-padded inconsistently would break the ordered string comparison, so pin them.
    #[test]
    fn leap_day_and_year_rollover_compare_correctly() {
        let configs = hive_style_ts_configs("yyyy-MM-dd");
        let pruner = PartitionPruner::new(
            &[
                ts_filter(ExprOperator::Gte, "2024-02-28T00:00:00Z"),
                ts_filter(ExprOperator::Lte, "2024-03-01T00:00:00Z"),
            ],
            &partition_path_schema(),
            &configs,
        )
        .unwrap();
        assert!(pruner.should_include("ts=2024-02-29"), "leap day");
        assert!(!pruner.should_include("ts=2023-02-28"));

        let pruner = PartitionPruner::new(
            &[
                ts_filter(ExprOperator::Gte, "2024-12-31T00:00:00Z"),
                ts_filter(ExprOperator::Lte, "2025-01-01T00:00:00Z"),
            ],
            &partition_path_schema(),
            &configs,
        )
        .unwrap();
        assert!(pruner.should_include("ts=2024-12-31"));
        assert!(pruner.should_include("ts=2025-01-01"));
        assert!(!pruner.should_include("ts=2024-12-30"));
        assert!(!pruner.should_include("ts=2025-01-02"));
    }
}
