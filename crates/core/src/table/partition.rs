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
use anyhow::Result;
use anyhow::{anyhow, Context};
use arrow_array::{ArrayRef, Scalar, StringArray};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{DataType, Field, Schema};
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

/// A partition pruner that filters partitions based on the partition path and its filters.
#[derive(Debug, Clone)]
pub struct PartitionPruner {
    schema: Arc<Schema>,
    is_hive_style: bool,
    is_url_encoded: bool,
    and_filters: Vec<PartitionFilter>,
}

impl TryFrom<(&[(&str, &str, &str)], &Schema, &HudiConfigs)> for PartitionPruner {
    type Error = anyhow::Error;

    fn try_from(
        (and_filters, partition_schema, hudi_configs): (
            &[(&str, &str, &str)],
            &Schema,
            &HudiConfigs,
        ),
    ) -> Result<Self> {
        let and_filters = and_filters
            .iter()
            .map(|filter| PartitionFilter::try_from((*filter, partition_schema)))
            .collect::<Result<Vec<PartitionFilter>>>()?;

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
}

impl TryFrom<(&[(&str, &str, &[&str])], &Schema, &HudiConfigs)> for PartitionPruner {
    type Error = anyhow::Error;

    fn try_from(
        (and_filters, partition_schema, hudi_configs): (
            &[(&str, &str, &[&str])],
            &Schema,
            &HudiConfigs,
        ),
    ) -> Result<Self> {
        let and_filters = and_filters
            .iter()
            .map(|filter| PartitionFilter::try_from((*filter, partition_schema)))
            .collect::<Result<Vec<PartitionFilter>>>()?;

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
}

impl PartitionPruner {
    pub fn new(
        and_filters: Vec<PartitionFilter>,
        partition_schema: &Schema,
        hudi_configs: &HudiConfigs,
    ) -> Result<Self> {
        // let and_filters = and_filters
        //     .iter()
        //     .map(|filter| PartitionFilter::try_from((*filter, partition_schema)))
        //     .collect::<Result<Vec<PartitionFilter>>>()?;

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
            match filter {
                PartitionFilter::Eq { field, value } => {
                    if let Some(segment_value) = segments.get(field.name()) {
                        match eq(segment_value, value) {
                            Ok(scalar) => scalar.value(0),
                            Err(_) => true, // Include the partition when comparison error occurs
                        }
                    } else {
                        true // Field not found, include partition
                    }
                }
                PartitionFilter::Ne { field, value } => {
                    if let Some(segment_value) = segments.get(field.name()) {
                        match neq(segment_value, value) {
                            Ok(scalar) => scalar.value(0),
                            Err(_) => true, // Include the partition when comparison error occurs
                        }
                    } else {
                        true // Field not found, include partition
                    }
                }
                PartitionFilter::Lt { field, value } => {
                    if let Some(segment_value) = segments.get(field.name()) {
                        match lt(segment_value, value) {
                            Ok(scalar) => scalar.value(0),
                            Err(_) => true, // Include the partition when comparison error occurs
                        }
                    } else {
                        true // Field not found, include partition
                    }
                }
                PartitionFilter::Lte { field, value } => {
                    if let Some(segment_value) = segments.get(field.name()) {
                        match lt_eq(segment_value, value) {
                            Ok(scalar) => scalar.value(0),
                            Err(_) => true, // Include the partition when comparison error occurs
                        }
                    } else {
                        true // Field not found, include partition
                    }
                }
                PartitionFilter::Gt { field, value } => {
                    if let Some(segment_value) = segments.get(field.name()) {
                        match gt(segment_value, value) {
                            Ok(scalar) => scalar.value(0),
                            Err(_) => true, // Include the partition when comparison error occurs
                        }
                    } else {
                        true // Field not found, include partition
                    }
                }
                PartitionFilter::Gte { field, value } => {
                    if let Some(segment_value) = segments.get(field.name()) {
                        match gt_eq(segment_value, value) {
                            Ok(scalar) => scalar.value(0),
                            Err(_) => true, // Include the partition when comparison error occurs
                        }
                    } else {
                        true // Field not found, include partition
                    }
                }
                PartitionFilter::In { field, value } => {
                    if let Some(segment_value) = segments.get(field.name()) {
                        value.iter().any(|val| match eq(segment_value, val) {
                            Ok(scalar) => scalar.value(0),
                            Err(_) => true, // Include the partition when comparison error occurs
                        })
                    } else {
                        true // Field not found, include partition
                    }
                }
                PartitionFilter::NotIn { field, value } => {
                    if let Some(segment_value) = segments.get(field.name()) {
                        !value.iter().any(|val| match eq(segment_value, val) {
                            Ok(scalar) => scalar.value(0),
                            Err(_) => true, // Include the partition when comparison error occurs
                        })
                    } else {
                        true // Field not found, include partition
                    }
                }
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

/// An operator that represents a comparison operation used in a partition filter expression.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Operator {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    In,
    NotIn,
}

impl Operator {
    const TOKEN_OP_PAIRS: [(&'static str, Operator); 8] = [
        ("=", Operator::Eq),
        ("!=", Operator::Ne),
        ("<", Operator::Lt),
        ("<=", Operator::Lte),
        (">", Operator::Gt),
        (">=", Operator::Gte),
        ("in", Operator::In),
        ("not in", Operator::NotIn),
    ];
}

impl FromStr for Operator {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        Operator::TOKEN_OP_PAIRS
            .iter()
            .find_map(|&(token, op)| if token == s { Some(op) } else { None })
            .ok_or_else(|| anyhow!("Unsupported operator: {}", s))
    }
}

/// A partition filter that represents a filter expression for partition pruning.
#[derive(Debug, Clone)]
pub enum PartitionFilter {
    Eq {
        field: Field,
        value: Scalar<ArrayRef>,
    },
    Ne {
        field: Field,
        value: Scalar<ArrayRef>,
    },
    Lt {
        field: Field,
        value: Scalar<ArrayRef>,
    },
    Lte {
        field: Field,
        value: Scalar<ArrayRef>,
    },
    Gt {
        field: Field,
        value: Scalar<ArrayRef>,
    },
    Gte {
        field: Field,
        value: Scalar<ArrayRef>,
    },
    In {
        field: Field,
        value: Vec<Scalar<ArrayRef>>,
    },
    NotIn {
        field: Field,
        value: Vec<Scalar<ArrayRef>>,
    },
}

impl TryFrom<((&str, &str, &str), &Schema)> for PartitionFilter {
    type Error = anyhow::Error;

    fn try_from((filter, partition_schema): ((&str, &str, &str), &Schema)) -> Result<Self> {
        let (field_name, operator_str, value_str) = filter;

        let field: &Field = partition_schema
            .field_with_name(field_name)
            .with_context(|| format!("Field '{}' not found in partition schema", field_name))?;

        let operator = Operator::from_str(operator_str)
            .with_context(|| format!("Unsupported operator: {}", operator_str))?;

        let value = &[value_str];
        let value = Self::cast_value(value, field.data_type())
            .with_context(|| format!("Unable to cast {:?} as {:?}", value, field.data_type()))?;

        let field = field.clone();

        let partition_filter = match operator {
            Operator::Eq => PartitionFilter::Eq { field, value },
            Operator::Ne => PartitionFilter::Ne { field, value },
            Operator::Lt => PartitionFilter::Lt { field, value },
            Operator::Lte => PartitionFilter::Lte { field, value },
            Operator::Gt => PartitionFilter::Gt { field, value },
            Operator::Gte => PartitionFilter::Gte { field, value },
            _ => panic!("Not supported"),
        };
        Ok(partition_filter)
    }
}

impl TryFrom<((&str, &str, &[&str]), &Schema)> for PartitionFilter {
    type Error = anyhow::Error;

    fn try_from((filter, partition_schema): ((&str, &str, &[&str]), &Schema)) -> Result<Self> {
        let (field_name, operator_str, value_vec) = filter;

        let field: &Field = partition_schema
            .field_with_name(field_name)
            .with_context(|| format!("Field '{}' not found in partition schema", field_name))?;

        let operator = Operator::from_str(operator_str)
            .with_context(|| format!("Unsupported operator: {}", operator_str))?;

        let value_vec = value_vec
            .iter()
            .map(|value| {
                Self::cast_value(&[value], field.data_type()).with_context(|| {
                    format!("Unable to cast {:?} as {:?}", value, field.data_type())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let field = field.clone();
        let partition_filter = match operator {
            Operator::In => PartitionFilter::In {
                field,
                value: value_vec,
            },
            Operator::NotIn => PartitionFilter::NotIn {
                field,
                value: value_vec,
            },
            _ => panic!("Not supported"),
        };
        Ok(partition_filter)
    }
}

impl PartitionFilter {
    fn cast_value(value: &[&str; 1], data_type: &DataType) -> Result<Scalar<ArrayRef>> {
        let cast_options = CastOptions {
            safe: false,
            format_options: Default::default(),
        };

        let value = StringArray::from(Vec::from(value));

        Ok(Scalar::new(cast_with_options(
            &value,
            data_type,
            &cast_options,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig::{
        IsHiveStylePartitioning, IsPartitionPathUrlencoded,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::Datum;
    use hudi_tests::assert_not;
    use std::str::FromStr;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ])
    }

    #[test]
    fn test_partition_filter_try_from_valid() {
        let schema = create_test_schema();
        let filter_tuple = ("date", "=", "2023-01-01");
        let filter = PartitionFilter::try_from((filter_tuple, &schema));
        assert!(filter.is_ok());
        let filter = filter.unwrap();
        let (field, value) = match filter {
            PartitionFilter::Eq {
                ref field,
                ref value,
            } => (field, value),
            _ => panic!("Cannot unpack partition filter to values"),
        };
        assert!(matches!(filter, PartitionFilter::Eq { .. }));
        assert_eq!(field.name(), "date");
        assert_eq!(value.get().0.len(), 1);

        let filter_tuple = ("category", "!=", "foo");
        let filter = PartitionFilter::try_from((filter_tuple, &schema));
        assert!(filter.is_ok());
        let filter = filter.unwrap();
        let (field, value) = match filter {
            PartitionFilter::Ne {
                ref field,
                ref value,
            } => (field, value),
            _ => panic!("Cannot unpack partition filter to values"),
        };
        assert!(matches!(filter, PartitionFilter::Ne { .. }));
        assert_eq!(field.name(), "category");

        assert_eq!(value.get().0.len(), 1);
        assert_eq!(StringArray::from(value.get().0.to_data()).value(0), "foo")
    }

    #[test]
    fn test_partition_filter_try_from_invalid_field() {
        let schema = create_test_schema();
        let filter_tuple = ("invalid_field", "=", "2023-01-01");
        let filter = PartitionFilter::try_from((filter_tuple, &schema));
        assert!(filter.is_err());
        assert!(filter
            .unwrap_err()
            .to_string()
            .contains("not found in partition schema"));
    }

    #[test]
    fn test_partition_filter_try_from_invalid_operator() {
        let schema = create_test_schema();
        let filter_tuple = ("date", "??", "2023-01-01");
        let filter = PartitionFilter::try_from((filter_tuple, &schema));
        assert!(filter.is_err());
        assert!(filter
            .unwrap_err()
            .to_string()
            .contains("Unsupported operator: ??"));
    }

    #[test]
    fn test_partition_filter_try_from_invalid_value() {
        let schema = create_test_schema();
        let filter_tuple = ("count", "=", "not_a_number");
        let filter = PartitionFilter::try_from((filter_tuple, &schema));
        assert!(filter.is_err());
        assert!(filter.unwrap_err().to_string().contains("Unable to cast"));
    }

    #[test]
    fn test_partition_filter_try_from_scalar_operators() {
        let schema = create_test_schema();

        for (op_str, op) in &Operator::TOKEN_OP_PAIRS[0..6] {
            let filter_tuple = ("count", *op_str, "10");
            let filter = PartitionFilter::try_from((filter_tuple, &schema));
            assert!(filter.is_ok(), "Failed for operator: {}", op_str);
            let filter = filter.unwrap();
            let (field, operator) = match filter {
                PartitionFilter::Eq { field, value: _ } => (field, Operator::Eq),
                PartitionFilter::Ne { field, value: _ } => (field, Operator::Ne),
                PartitionFilter::Lt { field, value: _ } => (field, Operator::Lt),
                PartitionFilter::Lte { field, value: _ } => (field, Operator::Lte),
                PartitionFilter::Gt { field, value: _ } => (field, Operator::Gt),
                PartitionFilter::Gte { field, value: _ } => (field, Operator::Gte),
                _ => panic!(),
            };
            assert_eq!(field.name(), "count");
            assert_eq!(operator, *op);
        }
    }

    #[test]
    fn test_partition_filter_try_from_list_operators() {
        let schema = create_test_schema();
        // let tokens =
        for (op_str, op) in &Operator::TOKEN_OP_PAIRS[6..8] {
            let filter_tuple = ("count", *op_str, &["10"] as &[&str]);
            let filter = PartitionFilter::try_from((filter_tuple, &schema));
            assert!(filter.is_ok(), "Failed for operator: {}", op_str);
            let filter = filter.unwrap();
            let (field, operator) = match filter {
                PartitionFilter::In { field, value: _ } => (field, Operator::In),
                PartitionFilter::NotIn { field, value: _ } => (field, Operator::NotIn),
                _ => panic!(),
            };
            assert_eq!(field.name(), "count");
            assert_eq!(operator, *op);
        }
    }

    #[test]
    fn test_operator_from_str() {
        assert_eq!(Operator::from_str("=").unwrap(), Operator::Eq);
        assert_eq!(Operator::from_str("!=").unwrap(), Operator::Ne);
        assert_eq!(Operator::from_str("<").unwrap(), Operator::Lt);
        assert_eq!(Operator::from_str("<=").unwrap(), Operator::Lte);
        assert_eq!(Operator::from_str(">").unwrap(), Operator::Gt);
        assert_eq!(Operator::from_str(">=").unwrap(), Operator::Gte);
        assert!(Operator::from_str("??").is_err());
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
        let filters: &[(&str, &str, &str)] = &[("date", ">", "2023-01-01"), ("category", "=", "A")];

        let pruner = PartitionPruner::try_from((filters, &schema, &configs));
        assert!(pruner.is_ok());

        let pruner = pruner.unwrap();
        assert_eq!(pruner.and_filters.len(), 2);
        assert!(pruner.is_hive_style);
        assert_not!(pruner.is_url_encoded);
    }

    #[test]
    fn test_partition_pruner_in_filter() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);
        let filters: &[(&str, &str, &[&str])] = &[("date", "in", &["2023-01-01", "2023-01-02"])];

        let pruner = PartitionPruner::try_from((filters, &schema, &configs));
        assert!(pruner.is_ok());

        let pruner = pruner.unwrap();
        assert_eq!(pruner.and_filters.len(), 1);
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
        let filters: &[(&str, &str, &str)] = &[];
        let pruner_empty = PartitionPruner::try_from((filters, &schema, &configs)).unwrap();
        assert!(pruner_empty.is_empty());

        let filters: &[(&str, &str, &str)] = &[("date", ">", "2023-01-01")];
        let pruner_non_empty = PartitionPruner::try_from((filters, &schema, &configs)).unwrap();
        assert_not!(pruner_non_empty.is_empty());
    }

    #[test]
    fn test_partition_pruner_should_include() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);
        let filters: &[(&str, &str, &str)] = &[
            ("date", ">", "2023-01-01"),
            ("category", "=", "A"),
            ("count", "<=", "100"),
        ];

        let pruner = PartitionPruner::try_from((filters, &schema, &configs)).unwrap();

        assert!(pruner.should_include("date=2023-02-01/category=A/count=10"));
        assert!(pruner.should_include("date=2023-02-01/category=A/count=100"));
        assert_not!(pruner.should_include("date=2022-12-31/category=A/count=10"));
        assert_not!(pruner.should_include("date=2023-02-01/category=B/count=10"));
    }

    #[test]
    fn test_partition_pruner_should_include_values_in() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);
        let filters: &[(&str, &str, &[&str])] = &[
            ("date", "in", &["2023-02-01", "2022-12-31"]),
        ];

        let pruner = PartitionPruner::try_from((filters, &schema, &configs)).unwrap();

        assert!(pruner.should_include("date=2023-02-01/category=A/count=10"));
        assert!(pruner.should_include("date=2022-12-31/category=A/count=10"));
        assert_not!(pruner.should_include("date=2021-12-31/category=A/count=10"));
    }

    #[test]
    fn test_partition_pruner_should_include_values_not_in() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);
        let filters: &[(&str, &str, &[&str])] = &[
            ("date", "not in", &["2023-02-01", "2022-12-31"]),
        ];

        let pruner = PartitionPruner::try_from((filters, &schema, &configs)).unwrap();

        assert_not!(pruner.should_include("date=2023-02-01/category=A/count=10"));
        assert_not!(pruner.should_include("date=2022-12-31/category=A/count=10"));
        assert!(pruner.should_include("date=2021-12-31/category=A/count=10"));
    }

    #[test]
    fn test_partition_pruner_parse_segments() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);
        let filters: &[(&str, &str, &str)] = &[];
        let pruner = PartitionPruner::try_from((filters, &schema, &configs)).unwrap();

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
        let filters: &[(&str, &str, &str)] = &[];
        let pruner = PartitionPruner::try_from((filters, &schema, &configs)).unwrap();

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
        let filters: &[(&str, &str, &str)] = &[];
        let pruner = PartitionPruner::try_from((filters, &schema, &configs)).unwrap();

        assert!(pruner.parse_segments("invalid/path").is_err());
    }
}
