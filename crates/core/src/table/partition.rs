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
use crate::exprs::{HudiOperator, PartitionFilter};
use anyhow::anyhow;
use anyhow::Result;
use arrow_array::{ArrayRef, Scalar};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::Schema;

use std::collections::HashMap;
use std::sync::Arc;

/// A partition pruner that filters partitions based on the partition path and its filters.
#[derive(Debug, Clone)]
pub struct PartitionPruner {
    schema: Arc<Schema>,
    is_hive_style: bool,
    is_url_encoded: bool,
    and_filters: Vec<PartitionFilter>,
}

impl PartitionPruner {
    pub fn new(
        and_filters: &[PartitionFilter],
        partition_schema: &Schema,
        hudi_configs: &HudiConfigs,
    ) -> Result<Self> {
        let and_filters = and_filters.to_vec();

        let schema = Arc::new(partition_schema.clone());
        let is_hive_style: bool = hudi_configs
            .get_or_default(HudiTableConfig::IsHiveStylePartitioning)
            .to();
        let is_url_encoded: bool = hudi_configs
            .get_or_default(HudiTableConfig::IsPartitionPathUrlencoded)
            .to();
        Ok(PartitionPruner {
            schema,
            is_hive_style,
            is_url_encoded,
            and_filters,
        })
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
                    let comparison_result = match filter.operator {
                        HudiOperator::Eq => eq(segment_value, &filter.value),
                        HudiOperator::Ne => neq(segment_value, &filter.value),
                        HudiOperator::Lt => lt(segment_value, &filter.value),
                        HudiOperator::Lte => lt_eq(segment_value, &filter.value),
                        HudiOperator::Gt => gt(segment_value, &filter.value),
                        HudiOperator::Gte => gt_eq(segment_value, &filter.value),
                    };

                    match comparison_result {
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
            return Err(anyhow!(
                "Partition path should have {} part(s) but got {}",
                self.schema.fields().len(),
                parts.len()
            ));
        }

        self.schema
            .fields()
            .iter()
            .zip(parts)
            .map(|(field, part)| {
                let value = if self.is_hive_style {
                    let (name, value) = part.split_once('=').ok_or_else(|| {
                        anyhow!("Partition path should be hive-style but got {}", part)
                    })?;
                    if name != field.name() {
                        return Err(anyhow!(
                            "Partition path should contain {} but got {}",
                            field.name(),
                            name
                        ));
                    }
                    value
                } else {
                    part
                };
                let scalar = PartitionFilter::cast_value(&[value], field.data_type())?;
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
    use arrow::datatypes::{DataType, Field, Schema};
    use hudi_tests::assert_not;

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

        let filter_gt_date = PartitionFilter::try_from((("date", ">", "2023-01-01"), &schema))
            .map_err(|e| anyhow!("Failed to create PartitionFilter: {}", e))
            .unwrap();
        let filter_eq_a = PartitionFilter::try_from((("category", "=", "A"), &schema))
            .map_err(|e| anyhow!("Failed to create PartitionFilter: {}", e))
            .unwrap();

        let pruner = PartitionPruner::new(&[filter_gt_date, filter_eq_a], &schema, &configs);
        assert!(pruner.is_ok());

        let pruner = pruner.unwrap();
        assert_eq!(pruner.and_filters.len(), 2);
        assert!(pruner.is_hive_style);
        assert_not!(pruner.is_url_encoded);
    }

    #[test]
    fn test_partition_pruner_empty() {
        let pruner = PartitionPruner::empty();
        assert!(pruner.is_empty());
        assert_not!(pruner.is_hive_style);
        assert_not!(pruner.is_url_encoded);
    }

    #[test]
    fn test_partition_pruner_is_empty() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(false, false);

        let pruner_empty = PartitionPruner::new(&[], &schema, &configs).unwrap();
        assert!(pruner_empty.is_empty());

        let filter_gt_date = PartitionFilter::try_from((("date", ">", "2023-01-01"), &schema))
            .map_err(|e| anyhow!("Failed to create PartitionFilter: {}", e))
            .unwrap();
        let pruner_non_empty = PartitionPruner::new(&[filter_gt_date], &schema, &configs).unwrap();
        assert_not!(pruner_non_empty.is_empty());
    }

    #[test]
    fn test_partition_pruner_should_include() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);

        let filter_gt_date = PartitionFilter::try_from((("date", ">", "2023-01-01"), &schema))
            .map_err(|e| anyhow!("Failed to create PartitionFilter: {}", e))
            .unwrap();
        let filter_eq_a = PartitionFilter::try_from((("category", "=", "A"), &schema))
            .map_err(|e| anyhow!("Failed to create PartitionFilter: {}", e))
            .unwrap();
        let filter_lte_100 = PartitionFilter::try_from((("count", "<=", "100"), &schema))
            .map_err(|e| anyhow!("Failed to create PartitionFilter: {}", e))
            .unwrap();

        let pruner = PartitionPruner::new(
            &[filter_gt_date, filter_eq_a, filter_lte_100],
            &schema,
            &configs,
        )
        .unwrap();

        assert!(pruner.should_include("date=2023-02-01/category=A/count=10"));
        assert!(pruner.should_include("date=2023-02-01/category=A/count=100"));
        assert_not!(pruner.should_include("date=2022-12-31/category=A/count=10"));
        assert_not!(pruner.should_include("date=2023-02-01/category=B/count=10"));
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

        assert!(pruner.parse_segments("invalid/path").is_err());
    }
}
