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
use anyhow::anyhow;
use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use serde::{Serialize, Serializer};
use std::collections::HashMap;

/// A special value used in Hive to represent the null partition in partitioned tables
pub const NULL_PARTITION_VALUE_DATA_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

/// A Enum used for selecting the partition value operation when filtering a HudiTable partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionValue {
    /// The partition value with the equal operator
    Equal(ScalarValue),
    /// The partition value with the not equal operator
    NotEqual(ScalarValue),
    /// The partition value with the greater than operator
    GreaterThan(ScalarValue),
    /// The partition value with the greater than or equal operator
    GreaterThanOrEqual(ScalarValue),
    /// The partition value with the less than operator
    LessThan(ScalarValue),
    /// The partition value with the less than or equal operator
    LessThanOrEqual(ScalarValue),
    /// The partition values with the in operator
    In(Vec<ScalarValue>),
    /// The partition values with the not in operator
    NotIn(Vec<ScalarValue>),
}

/// A Struct used for filtering a HudiTable partition by key and value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionFilter {
    /// The key of the PartitionFilter
    pub key: String,
    /// The value of the PartitionFilter
    pub value: PartitionValue,
}

impl PartitionFilter {
    /// Indicates if a HudiTable partition matches with the partition filter by key and value.
    pub fn match_partition(&self, partition: &HudiTablePartition) -> bool {
        if self.key != partition.key {
            return false;
        }

        match &self.value {
            PartitionValue::Equal(value) => value.eq(&partition.value),
            PartitionValue::NotEqual(value) => !value.eq(&partition.value),
            PartitionValue::GreaterThan(value) => partition.value.gt(value),
            PartitionValue::GreaterThanOrEqual(value) => partition.value.ge(value),
            PartitionValue::LessThan(value) => partition.value.lt(value),
            PartitionValue::LessThanOrEqual(value) => partition.value.le(value),
            PartitionValue::In(value) => value.contains(&partition.value),
            PartitionValue::NotIn(value) => !value.contains(&partition.value),
        }
    }

    /// Indicates if one of the HudiTable partition among the list
    /// matches with the partition filter.
    pub fn match_partitions(&self, partitions: &[HudiTablePartition]) -> bool {
        partitions
            .iter()
            .any(|partition| self.match_partition(partition))
    }
}

/// Create desired string representation for PartitionFilter.
/// Used in places like predicate in operationParameters, etc.
impl Serialize for PartitionFilter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match &self.value {
            PartitionValue::Equal(value) => format!("{} = '{}'", self.key, value),
            PartitionValue::NotEqual(value) => format!("{} != '{}'", self.key, value),
            PartitionValue::GreaterThan(value) => format!("{} > '{}'", self.key, value),
            PartitionValue::GreaterThanOrEqual(value) => format!("{} >= '{}'", self.key, value),
            PartitionValue::LessThan(value) => format!("{} < '{}'", self.key, value),
            PartitionValue::LessThanOrEqual(value) => format!("{} <= '{}'", self.key, value),
            // used upper case for IN and NOT similar to SQL
            PartitionValue::In(values) => {
                let quoted_values: Vec<String> =
                    values.iter().map(|v| format!("'{}'", v)).collect();
                format!("{} IN ({})", self.key, quoted_values.join(", "))
            }
            PartitionValue::NotIn(values) => {
                let quoted_values: Vec<String> =
                    values.iter().map(|v| format!("'{}'", v)).collect();
                format!("{} NOT IN ({})", self.key, quoted_values.join(", "))
            }
        };
        serializer.serialize_str(&s)
    }
}

/// Create a PartitionFilter from a filter Tuple with the structure (key, operation, value).
impl TryFrom<(&str, &str, &str, &DataType)> for PartitionFilter {
    type Error = anyhow::Error;

    /// Try to create a PartitionFilter from a Tuple of (key, operation, value).
    /// Returns an Error in case of a malformed filter.
    fn try_from(filter: (&str, &str, &str, &DataType)) -> Result<Self, anyhow::Error> {
        match filter {
            (key, "=", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::Equal(
                    ScalarValue::try_from_string(value.to_string(), data_type).unwrap(),
                ),
            }),
            (key, "!=", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::NotEqual(
                    ScalarValue::try_from_string(value.to_string(), data_type).unwrap(),
                ),
            }),
            (key, ">", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::GreaterThan(
                    ScalarValue::try_from_string(value.to_string(), data_type).unwrap(),
                ),
            }),
            (key, ">=", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::GreaterThanOrEqual(
                    ScalarValue::try_from_string(value.to_string(), data_type).unwrap(),
                ),
            }),
            (key, "<", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::LessThan(
                    ScalarValue::try_from_string(value.to_string(), data_type).unwrap(),
                ),
            }),
            (key, "<=", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::LessThanOrEqual(
                    ScalarValue::try_from_string(value.to_string(), data_type).unwrap(),
                ),
            }),
            (_, _, _, _) => Err(anyhow!("Invalid partition filter found: {}.", filter.1)),
        }
    }
}

/// Create a PartitionFilter from a filter Tuple with the structure (key, operation, list(value)).
impl TryFrom<(&str, &str, &[&str], &DataType)> for PartitionFilter {
    type Error = anyhow::Error;

    /// Try to create a PartitionFilter from a Tuple of (key, operation, list(value)).
    /// Returns an Error in case of a malformed filter.
    fn try_from(filter: (&str, &str, &[&str], &DataType)) -> Result<Self, anyhow::Error> {
        match filter {
            (key, "in", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::In(
                    value
                        .iter()
                        .map(|x| ScalarValue::try_from_string(x.to_string(), data_type).unwrap())
                        .collect(),
                ),
            }),
            (key, "not in", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::NotIn(
                    value
                        .iter()
                        .map(|x| ScalarValue::try_from_string(x.to_string(), data_type).unwrap())
                        .collect(),
                ),
            }),
            (_, _, _, _) => Err(anyhow!("Invalid partition filter found: {}.", filter.1)),
        }
    }
}

/// A Struct HudiTablePartition used to represent a partition of a HudiTable.
#[derive(Clone, Debug, PartialEq)]
pub struct HudiTablePartition {
    /// The key of the HudiTable partition.
    pub key: String,
    /// The value of the HudiTable partition.
    pub value: ScalarValue,
}

impl Eq for HudiTablePartition {}

impl HudiTablePartition {
    /// Create a HudiTable partition from a Tuple of (key, value).
    pub fn from_partition_value(partition_value: (&str, &ScalarValue)) -> Self {
        let (k, v) = partition_value;
        HudiTablePartition {
            key: k.to_owned(),
            value: v.to_owned(),
        }
    }
}

/// A HivePartition string is represented by a "key=value" format.
impl TryFrom<(&str, &DataType)> for HudiTablePartition {
    type Error = anyhow::Error;

    /// Try to create a HudiTable partition from a HivePartition string.
    /// Returns a Error if the string is not in the form of a HivePartition.
    fn try_from(partition: (&str, &DataType)) -> Result<Self, anyhow::Error> {
        let partition_splitted: Vec<&str> = partition.0.split('=').collect();
        match partition_splitted {
            partition_splitted if partition_splitted.len() == 2 => Ok(HudiTablePartition {
                key: partition_splitted[0].to_owned(),
                value: ScalarValue::try_from_string(
                    partition_splitted[1].to_owned().to_owned(),
                    partition.1,
                )
                .unwrap(),
            }),
            _ => Err(anyhow!(
                "This partition is not formatted with key=value: {}",
                partition.0
            )),
        }
    }
}

impl TryFrom<(&str, &HashMap<String, DataType>)> for HudiTablePartition {
    type Error = anyhow::Error;

    fn try_from(partition: (&str, &HashMap<String, DataType>)) -> Result<Self, anyhow::Error> {
        let partition_splitted: Vec<&str> = partition.0.split('=').collect();
        match partition_splitted {
            partition_splitted if partition_splitted.len() == 2 => Ok(HudiTablePartition {
                key: partition_splitted[0].to_owned(),
                value: ScalarValue::try_from_string(
                    partition_splitted[1].to_owned().to_owned(),
                    partition.1.get(partition_splitted[0]).unwrap(),
                )
                .unwrap(),
            }),
            _ => Err(anyhow!(
                "This partition is not formatted with key=value: {}",
                partition.0
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn check_json_serialize(filter: PartitionFilter, expected_json: &str) {
        assert_eq!(serde_json::to_value(filter).unwrap(), json!(expected_json))
    }

    #[test]
    fn test_serialize_partition_filter() {
        check_json_serialize(
            PartitionFilter::try_from(("date", "=", "2022-05-22", &DataType::Utf8)).unwrap(),
            "date = '2022-05-22'",
        );
        check_json_serialize(
            PartitionFilter::try_from(("date", "!=", "2022-05-22", &DataType::Utf8)).unwrap(),
            "date != '2022-05-22'",
        );
        check_json_serialize(
            PartitionFilter::try_from(("date", ">", "2022-05-22", &DataType::Utf8)).unwrap(),
            "date > '2022-05-22'",
        );
        check_json_serialize(
            PartitionFilter::try_from(("date", ">=", "2022-05-22", &DataType::Utf8)).unwrap(),
            "date >= '2022-05-22'",
        );
        check_json_serialize(
            PartitionFilter::try_from(("date", "<", "2022-05-22", &DataType::Utf8)).unwrap(),
            "date < '2022-05-22'",
        );
        check_json_serialize(
            PartitionFilter::try_from(("date", "<=", "2022-05-22", &DataType::Utf8)).unwrap(),
            "date <= '2022-05-22'",
        );
        check_json_serialize(
            PartitionFilter::try_from((
                "date",
                "in",
                vec!["2023-11-04", "2023-06-07"].as_slice(),
                &DataType::Utf8,
            ))
            .unwrap(),
            "date IN ('2023-11-04', '2023-06-07')",
        );
        check_json_serialize(
            PartitionFilter::try_from((
                "date",
                "not in",
                vec!["2023-11-04", "2023-06-07"].as_slice(),
                &DataType::Utf8,
            ))
            .unwrap(),
            "date NOT IN ('2023-11-04', '2023-06-07')",
        );
    }

    #[test]
    fn tryfrom_invalid() {
        let buf = "2024";
        let partition = HudiTablePartition::try_from((buf, &DataType::Utf8));
        assert!(partition.is_err());
    }

    #[test]
    fn tryfrom_valid() {
        let buf = "ds=2024";
        let partition = HudiTablePartition::try_from((buf, &DataType::Utf8));
        assert!(partition.is_ok());
        let partition = partition.unwrap();
        assert_eq!(partition.key, "ds");
        assert_eq!(partition.value, ScalarValue::new_utf8("2024"));
    }
}
