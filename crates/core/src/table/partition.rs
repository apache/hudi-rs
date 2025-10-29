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
use crate::config::table::HudiTableConfig;
use crate::config::HudiConfigs;
use crate::error::CoreError::InvalidPartitionPath;
use crate::expr::filter::{Filter, SchemableFilter};
use crate::keygen::timestamp_based::TimestampBasedKeyGenerator;
use crate::keygen::KeyGeneratorFilterTransformer;
use crate::Result;

use arrow_array::{ArrayRef, Scalar};
use arrow_schema::Schema;

use crate::config::table::HudiTableConfig::{KeyGeneratorClass, PartitionFields};
use std::collections::HashMap;
use std::sync::Arc;

pub const PARTITION_METAFIELD_PREFIX: &str = ".hoodie_partition_metadata";
pub const EMPTY_PARTITION_PATH: &str = "";

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

    has_partition_fields && !uses_non_partitioned_key_gen
}

/// A partition pruner that filters partitions based on the partition path and its filters.
#[derive(Debug, Clone)]
pub struct PartitionPruner {
    schema: Arc<Schema>,
    is_hive_style: bool,
    is_url_encoded: bool,
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

        // Convert to SchemableFilter
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
        Ok(PartitionPruner {
            schema,
            is_hive_style,
            is_url_encoded,
            and_filters,
        })
    }

    /// Transforms user filters on data columns to filters on partition path columns
    /// based on the configured key generator.
    ///
    /// For example, with TimestampBasedKeyGenerator:
    /// - Input filter: `ts_str >= "2023-04-01T12:01:00.123Z"`
    /// - Output filters: `year >= "2023"` (and potentially month, day, hour filters)
    ///
    /// For other key generators (Simple, Complex, etc.), filters are passed through unchanged (TODO).
    fn transform_filters_for_keygen(
        filters: &[Filter],
        _partition_schema: &Schema,
        hudi_configs: &HudiConfigs,
    ) -> Result<Vec<Filter>> {
        // Check if key generator is configured
        let keygen_class: Option<String> =
            hudi_configs.try_get(KeyGeneratorClass).map(|v| v.into());

        // If TimestampBasedKeyGenerator is configured, transform filters
        if let Some(class) = keygen_class {
            if class.contains("TimestampBasedKeyGenerator") {
                // Try to create the transformer
                match TimestampBasedKeyGenerator::from_configs(hudi_configs) {
                    Ok(transformer) => {
                        return Self::apply_transformer_to_filters(filters, &transformer);
                    }
                    Err(e) => {
                        // Log the error but continue without transformation
                        eprintln!(
                            "Warning: Failed to create TimestampBasedKeyGenerator: {}. \
                             Filters will not be transformed.",
                            e
                        );
                    }
                }
            }
        }

        // No transformation needed - return filters as-is
        Ok(filters.to_vec())
    }

    /// Applies the key generator transformer to all filters.
    fn apply_transformer_to_filters(
        filters: &[Filter],
        transformer: &dyn KeyGeneratorFilterTransformer,
    ) -> Result<Vec<Filter>> {
        let mut transformed = Vec::new();

        for filter in filters {
            // Transform filter (may produce multiple partition filters)
            let partition_filters = transformer.transform_filter(filter)?;
            transformed.extend(partition_filters);
        }

        Ok(transformed)
    }

    /// Creates an empty partition pruner that does not filter any partitions.
    pub fn empty() -> Self {
        PartitionPruner {
            schema: Arc::new(Schema::empty()),
            is_hive_style: false,
            is_url_encoded: false,
            and_filters: Vec::new(),
        }
    }

    /// Returns `true` if the partition pruner does not have any filters.
    pub fn is_empty(&self) -> bool {
        self.and_filters.is_empty()
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
                    match filter.apply_comparsion(segment_value) {
                        Ok(scalar) => scalar.value(0),
                        Err(_) => true, // Include the partition when comparison error occurs
                    }
                }
                None => true, // Include the partition when filtering field does not match any field in the partition
            }
        })
    }

    fn parse_segments(&self, partition_path: &str) -> Result<HashMap<String, Scalar<ArrayRef>>> {
        let partition_path = if self.is_url_encoded {
            percent_encoding::percent_decode(partition_path.as_bytes())
                .decode_utf8()?
                .into_owned()
        } else {
            partition_path.to_string()
        };

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
                        format!("Partition path should be hive-style but got {}", part),
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
            field_value: "2023-01-01".to_string(),
        };

        let partition_filter = SchemableFilter::try_from((filter, &schema)).unwrap();
        assert_eq!(partition_filter.field.name(), "date");
        assert_eq!(partition_filter.operator, ExprOperator::Eq);

        let value_inner = partition_filter.value.into_inner();

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
            field_value: "2023-01-01".to_string(),
        };
        let result = SchemableFilter::try_from((filter, &schema));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Field invalid_field not found in schema"));
    }

    #[test]
    fn test_partition_filter_try_from_invalid_value() {
        let schema = create_test_schema();
        let filter = Filter {
            field_name: "count".to_string(),
            operator: ExprOperator::Eq,
            field_value: "not_a_number".to_string(),
        };
        let result = SchemableFilter::try_from((filter, &schema));
        assert!(result.is_err());
    }

    #[test]
    fn test_partition_filter_try_from_all_operators() {
        let schema = create_test_schema();
        for (op, _) in ExprOperator::TOKEN_OP_PAIRS {
            let filter = Filter {
                field_name: "count".to_string(),
                operator: ExprOperator::from_str(op).unwrap(),
                field_value: "5".to_string(),
            };
            let partition_filter = SchemableFilter::try_from((filter, &schema));
            let filter = partition_filter.unwrap();
            assert_eq!(filter.field.name(), "count");
            assert_eq!(filter.operator, ExprOperator::from_str(op).unwrap());
        }
    }

    #[test]
    fn test_transform_filters_for_keygen_timestamp_based() {
        use arrow::datatypes::{DataType, Field, Schema};

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

        let partition_schema = Schema::new(vec![
            Field::new("year", DataType::Utf8, false),
            Field::new("month", DataType::Utf8, false),
            Field::new("day", DataType::Utf8, false),
        ]);

        let user_filter = Filter {
            field_name: "ts_str".to_string(),
            operator: ExprOperator::Gte,
            field_value: "2023-04-15T12:00:00.000Z".to_string(),
        };

        let transformed = PartitionPruner::transform_filters_for_keygen(
            &[user_filter],
            &partition_schema,
            &configs,
        )
        .unwrap();

        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].field_name, "year");
        assert_eq!(transformed[0].operator, ExprOperator::Gte);
        assert_eq!(transformed[0].field_value, "2023");
    }

    #[test]
    fn test_transform_filters_for_keygen_equality() {
        use arrow::datatypes::{DataType, Field, Schema};

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

        let partition_schema = Schema::new(vec![
            Field::new("yyyy", DataType::Utf8, false),
            Field::new("MM", DataType::Utf8, false),
            Field::new("dd", DataType::Utf8, false),
        ]);

        let user_filter = Filter {
            field_name: "event_time".to_string(),
            operator: ExprOperator::Eq,
            field_value: "1706140800000".to_string(), // 2024-01-25 00:00:00
        };

        let transformed = PartitionPruner::transform_filters_for_keygen(
            &[user_filter],
            &partition_schema,
            &configs,
        )
        .unwrap();

        assert_eq!(transformed.len(), 3);
        assert_eq!(transformed[0].field_name, "yyyy");
        assert_eq!(transformed[0].field_value, "2024");
        assert_eq!(transformed[1].field_name, "MM");
        assert_eq!(transformed[1].field_value, "01");
        assert_eq!(transformed[2].field_name, "dd");
        assert_eq!(transformed[2].field_value, "25");
    }

    #[test]
    fn test_transform_filters_for_keygen_no_transformation() {
        use arrow::datatypes::{DataType, Field, Schema};

        // Setup: SimpleKeyGenerator (no transformation needed)
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
            field_value: "us-west".to_string(),
        };

        let transformed = PartitionPruner::transform_filters_for_keygen(
            &[user_filter.clone()],
            &partition_schema,
            &configs,
        )
        .unwrap();

        // Should return filter unchanged
        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].field_name, user_filter.field_name);
        assert_eq!(transformed[0].field_value, user_filter.field_value);
    }

    #[test]
    fn test_partition_pruner_with_timestamp_keygen() {
        use arrow::datatypes::{DataType, Field, Schema};

        // Full integration test: PartitionPruner with TimestampBasedKeyGenerator
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

        let partition_schema = Schema::new(vec![
            Field::new("year", DataType::Utf8, false),
            Field::new("month", DataType::Utf8, false),
            Field::new("day", DataType::Utf8, false),
        ]);

        let user_filter = Filter {
            field_name: "ts".to_string(),
            operator: ExprOperator::Gte,
            field_value: "2024-01-15T00:00:00Z".to_string(),
        };

        let pruner = PartitionPruner::new(&[user_filter], &partition_schema, &configs).unwrap();

        // Verify filters were transformed and applied
        assert!(!pruner.is_empty());

        // Should include partitions >= 2024
        assert!(pruner.should_include("year=2024/month=01/day=15"));
        assert!(pruner.should_include("year=2024/month=06/day=30"));
        assert!(pruner.should_include("year=2025/month=01/day=01"));

        // Should exclude partitions < 2024
        assert!(!pruner.should_include("year=2023/month=12/day=31"));
        assert!(!pruner.should_include("year=2022/month=01/day=01"));
    }
}
