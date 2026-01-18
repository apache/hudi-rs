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

//! Partition-level pruner for filtering partitions based on partition values and statistics.

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig;
use crate::error::CoreError::InvalidPartitionPath;
use crate::expr::filter::{Filter, SchemableFilter};
use crate::statistics::StatisticsContainer;
use crate::table::is_table_partitioned;

use arrow_array::{ArrayRef, Scalar};
use arrow_schema::Schema;

use std::collections::HashMap;
use std::sync::Arc;

use super::StatsPruner;

/// A partition pruner that filters partitions based on the partition path and its filters.
///
/// The pruner supports two levels of filtering:
/// 1. **Partition column values**: Filters based on the actual partition column values
///    parsed from the partition path (e.g., `city=chennai` → `city = "chennai"`)
/// 2. **Partition statistics** (optional): Filters based on aggregated column statistics
///    at the partition level, loaded from the metadata table's `partition_stats` partition
#[derive(Debug, Clone)]
pub struct PartitionPruner {
    schema: Arc<Schema>,
    is_hive_style: bool,
    is_url_encoded: bool,
    is_partitioned: bool,
    /// Filters on partition columns (for partition path value filtering).
    and_filters: Vec<SchemableFilter>,
    /// Filters on data columns (for partition stats filtering).
    data_filters: Vec<SchemableFilter>,
    /// Optional partition-level statistics for enhanced pruning.
    partition_stats: Option<HashMap<String, StatisticsContainer>>,
}

impl PartitionPruner {
    pub fn new(
        and_filters: &[Filter],
        partition_schema: &Schema,
        hudi_configs: &HudiConfigs,
    ) -> Result<Self> {
        let and_filters: Vec<SchemableFilter> = and_filters
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
            data_filters: Vec::new(),
            partition_stats: None,
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
            data_filters: Vec::new(),
            partition_stats: None,
        }
    }

    /// Set partition statistics for enhanced pruning.
    pub fn with_partition_stats(mut self, stats: HashMap<String, StatisticsContainer>) -> Self {
        self.partition_stats = Some(stats);
        self
    }

    /// Set data column filters for stats-based pruning.
    pub fn with_data_filters(mut self, filters: Vec<SchemableFilter>) -> Self {
        self.data_filters = filters;
        self
    }

    /// Check if partition stats are available.
    pub fn has_partition_stats(&self) -> bool {
        self.partition_stats.is_some()
    }

    /// Returns `true` if the partition pruner does not have any filters.
    pub fn is_empty(&self) -> bool {
        self.and_filters.is_empty() && self.data_filters.is_empty()
    }

    /// Returns `true` if the table is partitioned.
    pub fn is_table_partitioned(&self) -> bool {
        self.is_partitioned
    }

    /// Returns `true` if the partition path should be included based on the filters.
    ///
    /// This method performs two levels of filtering:
    /// 1. Partition column value filtering - checks if partition path values match filters
    /// 2. Partition stats filtering - if partition_stats are available, uses aggregated
    ///    column statistics to prune partitions based on data column filters
    pub fn should_include(&self, partition_path: &str) -> bool {
        // Level 1: Partition column value filtering
        let segments = match self.parse_segments(partition_path) {
            Ok(s) => s,
            Err(_) => return true,
        };

        let partition_filter_pass =
            self.and_filters
                .iter()
                .all(|filter| match segments.get(filter.field.name()) {
                    Some(segment_value) => match filter.apply_comparison(segment_value) {
                        Ok(scalar) => scalar.value(0),
                        Err(_) => true,
                    },
                    None => true,
                });

        if !partition_filter_pass {
            return false;
        }

        // Level 2: Partition stats filtering (if available)
        if let Some(ref stats_map) = self.partition_stats {
            if let Some(stats) = stats_map.get(partition_path) {
                for filter in &self.data_filters {
                    let col_name = filter.field.name();
                    if let Some(col_stats) = stats.columns.get(col_name) {
                        if StatsPruner::can_prune_by_filter(filter, col_stats) {
                            return false;
                        }
                    }
                }
            }
        }

        true
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
    use crate::statistics::{ColumnStatistics, StatsGranularity};
    use arrow::datatypes::{DataType, Field};
    use arrow_array::{Date32Array, Int64Array};
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
    fn test_partition_pruner_with_partition_stats() {
        let partition_schema = Schema::new(vec![Field::new("city", DataType::Utf8, false)]);
        let data_schema = Schema::new(vec![Field::new("amount", DataType::Int64, false)]);
        let configs = create_hudi_configs(true, false);

        let make_stats = |min: i64, max: i64| {
            let mut stats = StatisticsContainer::new(StatsGranularity::File);
            stats.columns.insert(
                "amount".to_string(),
                ColumnStatistics {
                    column_name: "amount".to_string(),
                    data_type: DataType::Int64,
                    min_value: Some(Arc::new(Int64Array::from(vec![min])) as ArrayRef),
                    max_value: Some(Arc::new(Int64Array::from(vec![max])) as ArrayRef),
                },
            );
            stats
        };

        let mut stats_map = HashMap::new();
        stats_map.insert("city=A".to_string(), make_stats(100, 300));
        stats_map.insert("city=B".to_string(), make_stats(400, 800));

        let data_filter = Filter::try_from(("amount", ">", "500")).unwrap();
        let data_schemable = SchemableFilter::try_from((data_filter, &data_schema)).unwrap();

        let pruner = PartitionPruner::new(&[], &partition_schema, &configs)
            .unwrap()
            .with_partition_stats(stats_map)
            .with_data_filters(vec![data_schemable]);

        assert!(pruner.has_partition_stats());
        assert!(!pruner.should_include("city=A")); // max=300 <= 500, pruned
        assert!(pruner.should_include("city=B")); // max=800 > 500, included
    }
}
