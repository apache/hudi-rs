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
use once_cell::sync::Lazy;
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

impl PartitionPruner {
    pub fn new(
        and_filters: &[&str],
        partition_schema: &Schema,
        hudi_configs: &HudiConfigs,
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
                        Operator::Eq => eq(segment_value, &filter.value),
                        Operator::Ne => neq(segment_value, &filter.value),
                        Operator::Lt => lt(segment_value, &filter.value),
                        Operator::Lte => lt_eq(segment_value, &filter.value),
                        Operator::Gt => gt(segment_value, &filter.value),
                        Operator::Gte => gt_eq(segment_value, &filter.value),
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

/// An operator that represents a comparison operation used in a partition filter expression.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Operator {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl Operator {
    const TOKEN_OP_PAIRS: [(&'static str, Operator); 6] = [
        ("=", Operator::Eq),
        ("!=", Operator::Ne),
        ("<", Operator::Lt),
        ("<=", Operator::Lte),
        (">", Operator::Gt),
        (">=", Operator::Gte),
    ];

    /// Returns the supported operator tokens. Note that the tokens are sorted by length in descending order to facilitate parsing.
    fn supported_tokens() -> &'static [&'static str] {
        static TOKENS: Lazy<Vec<&'static str>> = Lazy::new(|| {
            let mut tokens: Vec<&'static str> = Operator::TOKEN_OP_PAIRS
                .iter()
                .map(|&(token, _)| token)
                .collect();
            tokens.sort_by_key(|t| std::cmp::Reverse(t.len()));
            tokens
        });
        &TOKENS
    }
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
pub struct PartitionFilter {
    field: Field,
    operator: Operator,
    value: Scalar<ArrayRef>,
}

impl TryFrom<(&str, &Schema)> for PartitionFilter {
    type Error = anyhow::Error;

    fn try_from((s, partition_schema): (&str, &Schema)) -> Result<Self> {
        let (field_name, operator_str, value_str) = Self::parse_to_parts(s)?;

        let field: &Field = partition_schema
            .field_with_name(field_name)
            .with_context(|| format!("Field '{}' not found in partition schema", field_name))?;

        let operator = Operator::from_str(operator_str)
            .with_context(|| format!("Unsupported operator: {}", operator_str))?;

        let value = &[value_str];
        let value = Self::cast_value(value, field.data_type())
            .with_context(|| format!("Unable to cast {:?} as {:?}", value, field.data_type()))?;

        let field = field.clone();
        Ok(PartitionFilter {
            field,
            operator,
            value,
        })
    }
}

impl PartitionFilter {
    fn parse_to_parts(s: &str) -> Result<(&str, &str, &str)> {
        let s = s.trim();

        let (index, op) = Operator::supported_tokens()
            .iter()
            .filter_map(|&op| s.find(op).map(|index| (index, op)))
            .min_by_key(|(index, _)| *index)
            .ok_or_else(|| anyhow!("No valid operator found in the filter string"))?;

        let (field, rest) = s.split_at(index);
        let (_, value) = rest.split_at(op.len());

        let field = field.trim();
        let value = value.trim();

        if field.is_empty() || value.is_empty() {
            return Err(anyhow!(
                "Invalid filter format: missing field name or value"
            ));
        }

        Ok((field, op, value))
    }

    fn cast_value(value: &[&str; 1], data_type: &DataType) -> Result<Scalar<ArrayRef>> {
        let cast_options = CastOptions {
            safe: false,
            format_options: Default::default(),
        };

        let value = match data_type {
            DataType::Date32 => Self::trim_single_quotes(value),
            DataType::Date64 => Self::trim_single_quotes(value),
            DataType::Utf8 => Self::trim_single_quotes(value),
            DataType::LargeUtf8 => Self::trim_single_quotes(value),
            DataType::Utf8View => Self::trim_single_quotes(value),
            _ => *value,
        };

        let value = StringArray::from(Vec::from(value));

        Ok(Scalar::new(cast_with_options(
            &value,
            data_type,
            &cast_options,
        )?))
    }

    fn trim_single_quotes<'a>(s: &'a [&'a str; 1]) -> [&'a str; 1] {
        let trimmed = s[0]
            .strip_prefix('\'')
            .unwrap_or(s[0])
            .strip_suffix('\'')
            .unwrap_or(s[0]);

        [trimmed]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig::{
        IsHiveStylePartitioning, IsPartitionPathUrlencoded,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Array, Datum};
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
        let filter_str = "date = 2023-01-01";
        let filter = PartitionFilter::try_from((filter_str, &schema));
        assert!(filter.is_ok());
        let filter = filter.unwrap();
        assert_eq!(filter.field.name(), "date");
        assert_eq!(filter.operator, Operator::Eq);
        assert_eq!(filter.value.get().0.len(), 1);

        let filter_str = "category!=foo";
        let filter = PartitionFilter::try_from((filter_str, &schema));
        assert!(filter.is_ok());
        let filter = filter.unwrap();
        assert_eq!(filter.field.name(), "category");
        assert_eq!(filter.operator, Operator::Ne);
        assert_eq!(filter.value.get().0.len(), 1);
        assert_eq!(
            StringArray::from(filter.value.into_inner().to_data()).value(0),
            "foo"
        )
    }

    #[test]
    fn test_partition_filter_try_from_invalid_field() {
        let schema = create_test_schema();
        let filter_str = "invalid_field = 2023-01-01";
        let filter = PartitionFilter::try_from((filter_str, &schema));
        assert!(filter.is_err());
        assert!(filter
            .unwrap_err()
            .to_string()
            .contains("not found in partition schema"));
    }

    #[test]
    fn test_partition_filter_try_from_invalid_operator() {
        let schema = create_test_schema();
        let filter_str = "date ?? 2023-01-01";
        let filter = PartitionFilter::try_from((filter_str, &schema));
        assert!(filter.is_err());
        assert!(filter
            .unwrap_err()
            .to_string()
            .contains("No valid operator found"));
    }

    #[test]
    fn test_partition_filter_try_from_invalid_value() {
        let schema = create_test_schema();
        let filter_str = "count = not_a_number";
        let filter = PartitionFilter::try_from((filter_str, &schema));
        assert!(filter.is_err());
        assert!(filter.unwrap_err().to_string().contains("Unable to cast"));
    }

    #[test]
    fn test_parse_to_parts_valid() {
        let result = PartitionFilter::parse_to_parts("date = 2023-01-01");
        assert!(result.is_ok());
        let (field, operator, value) = result.unwrap();
        assert_eq!(field, "date");
        assert_eq!(operator, "=");
        assert_eq!(value, "2023-01-01");
    }

    #[test]
    fn test_parse_to_parts_no_operator() {
        let result = PartitionFilter::parse_to_parts("date 2023-01-01");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No valid operator found"));
    }

    #[test]
    fn test_parse_to_parts_multiple_operators() {
        let result = PartitionFilter::parse_to_parts("count >= 10 <= 20");
        assert!(result.is_ok());
        let (field, operator, value) = result.unwrap();
        assert_eq!(field, "count");
        assert_eq!(operator, ">=");
        assert_eq!(value, "10 <= 20");
    }

    #[test]
    fn test_partition_filter_try_from_all_operators() {
        let schema = create_test_schema();
        for &op in Operator::supported_tokens() {
            let filter_str = format!("count {} 10", op);
            let filter = PartitionFilter::try_from((filter_str.as_str(), &schema));
            assert!(filter.is_ok(), "Failed for operator: {}", op);
            let filter = filter.unwrap();
            assert_eq!(filter.field.name(), "count");
            assert_eq!(filter.operator, Operator::from_str(op).unwrap());
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

    #[test]
    fn test_operator_supported_tokens() {
        assert_eq!(
            Operator::supported_tokens(),
            &["!=", "<=", ">=", "=", "<", ">"]
        );
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
        let filters = vec!["date > 2023-01-01", "category = A"];

        let pruner = PartitionPruner::new(&filters, &schema, &configs);
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

        let pruner_non_empty =
            PartitionPruner::new(&["date > 2023-01-01"], &schema, &configs).unwrap();
        assert_not!(pruner_non_empty.is_empty());
    }

    #[test]
    fn test_partition_pruner_should_include() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);
        let filters = vec!["date > 2023-01-01", "category = A", "count <= 100"];

        let pruner = PartitionPruner::new(&filters, &schema, &configs).unwrap();

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

    #[test]
    fn test_strip_single_quote_for_date_filter() {
        for data_type in [DataType::Date32, DataType::Date64] {
            let schema = Schema::new(vec![Field::new("date", data_type.clone(), false)]);

            let cast_options = CastOptions {
                safe: false,
                format_options: Default::default(),
            };

            let value = StringArray::from(vec!["2023-01-01"]);
            let value = cast_with_options(&value, &data_type, &cast_options).unwrap();

            let filter_str = "date = '2023-01-01'";
            let filter = PartitionFilter::try_from((filter_str, &schema));
            assert!(filter.is_ok());
            let filter = filter.unwrap();
            assert_eq!(filter.field.name(), "date");
            assert_eq!(filter.operator, Operator::Eq);
            assert_eq!(filter.value.get().0.len(), 1);
            assert_eq!(filter.value.get().0, value.get().0);
        }
    }

    #[test]
    fn test_strip_single_quote_for_string_filter() {
        for data_type in [DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8] {
            let schema = Schema::new(vec![Field::new("category", data_type.clone(), false)]);

            let cast_options = CastOptions {
                safe: false,
                format_options: Default::default(),
            };

            let value = StringArray::from(vec!["foo"]);
            let value = cast_with_options(&value, &data_type, &cast_options).unwrap();

            let filter_str = "category!='foo'";
            let filter = PartitionFilter::try_from((filter_str, &schema));
            assert!(filter.is_ok());
            let filter = filter.unwrap();
            assert_eq!(filter.field.name(), "category");
            assert_eq!(filter.operator, Operator::Ne);
            assert_eq!(filter.value.get().0.len(), 1);
            assert_eq!(filter.value.get().0, value.get().0)
        }
    }
}
