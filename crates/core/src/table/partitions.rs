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
use anyhow::Result;
use arrow_array::{ArrayRef, BooleanArray, Scalar, StringArray};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::DataType;
use std::borrow::Cow;
use std::collections::HashMap;
use std::iter::repeat;

/// A special value used in Hive to represent the null partition in partitioned tables
pub const NULL_PARTITION_VALUE_DATA_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

/// A Enum used for selecting the partition value operation when filtering a HudiTable partition.
#[derive(Clone, Debug)]
pub enum PartitionValue {
    /// The partition value with the equal operator
    Equal(Scalar<ArrayRef>),
    /// The partition value with the not equal operator
    NotEqual(Scalar<ArrayRef>),
    /// The partition value with the greater than operator
    GreaterThan(Scalar<ArrayRef>),
    /// The partition value with the greater than or equal operator
    GreaterThanOrEqual(Scalar<ArrayRef>),
    /// The partition value with the less than operator
    LessThan(Scalar<ArrayRef>),
    /// The partition value with the less than or equal operator
    LessThanOrEqual(Scalar<ArrayRef>),
    /// The partition values with the in operator
    In(Vec<Scalar<ArrayRef>>),
    /// The partition values with the not in operator
    NotIn(Vec<Scalar<ArrayRef>>),
}

/// A Struct used for filtering a HudiTable partition by key and value.
#[derive(Clone, Debug)]
pub struct PartitionFilter {
    /// The key of the PartitionFilter
    pub key: String,
    /// The value of the PartitionFilter
    pub value: PartitionValue,
}

impl PartitionFilter {
    /// Indicates if a HudiTable partition matches with the partition filter by key and value.
    pub fn match_partition(&self, partition: &HudiTablePartition) -> BooleanArray {
        if self.key != partition.key {
            return BooleanArray::new_scalar(false).into_inner();
        }

        match &self.value {
            PartitionValue::Equal(value) => eq(&partition.value, value).unwrap(),
            PartitionValue::NotEqual(value) => neq(&partition.value, value).unwrap(),
            PartitionValue::GreaterThan(value) => gt(&partition.value, value).unwrap(),
            PartitionValue::GreaterThanOrEqual(value) => gt_eq(&partition.value, value).unwrap(),
            PartitionValue::LessThan(value) => lt(&partition.value, value).unwrap(),
            PartitionValue::LessThanOrEqual(value) => lt_eq(&partition.value, value).unwrap(),
            PartitionValue::In(value) => BooleanArray::new_scalar(
                value
                    .iter()
                    .any(|s| eq(&partition.value, s).unwrap().value(0)),
            )
            .into_inner(),
            PartitionValue::NotIn(value) => BooleanArray::new_scalar(
                !value
                    .iter()
                    .any(|s| eq(&partition.value, s).unwrap().value(0)),
            )
            .into_inner(),
        }
    }

    /// Indicates if one of the HudiTable partition among the list
    /// matches with the partition filter.
    pub fn match_partitions(&self, partitions: &[HudiTablePartition]) -> bool {
        partitions
            .iter()
            .any(|partition| self.match_partition(partition).value(0))
    }
}

/// Create a PartitionFilter from a filter Tuple with the structure (key, operation, value).
impl TryFrom<(&str, &str, &str, &DataType)> for PartitionFilter {
    type Error = anyhow::Error;

    /// Try to create a PartitionFilter from a Tuple of (key, operation, value).
    /// Returns an Error in case of a malformed filter.
    fn try_from(filter: (&str, &str, &str, &DataType)) -> Result<Self> {
        match filter {
            (key, "=", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::Equal(try_from_string(value.to_string(), data_type)),
            }),
            (key, "!=", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::NotEqual(try_from_string(value.to_string(), data_type)),
            }),
            (key, ">", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::GreaterThan(try_from_string(value.to_string(), data_type)),
            }),
            (key, ">=", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::GreaterThanOrEqual(try_from_string(
                    value.to_string(),
                    data_type,
                )),
            }),
            (key, "<", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::LessThan(try_from_string(value.to_string(), data_type)),
            }),
            (key, "<=", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::LessThanOrEqual(try_from_string(
                    value.to_string(),
                    data_type,
                )),
            }),
            (_, _, _, _) => Err(anyhow!("Invalid partition filter found: {}.", filter.1)),
        }
    }
}

fn try_from_string(value: String, target_type: &DataType) -> Scalar<ArrayRef> {
    let cast_options = CastOptions {
        safe: false,
        format_options: Default::default(),
    };
    let value = StringArray::from_iter_values(repeat(value).take(1));
    let array_ref = cast_with_options(&value, target_type, &cast_options).unwrap();
    Scalar::new(array_ref)
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
                        .map(|x| try_from_string(x.to_string(), data_type))
                        .collect(),
                ),
            }),
            (key, "not in", value, data_type) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::NotIn(
                    value
                        .iter()
                        .map(|x| try_from_string(x.to_string(), data_type))
                        .collect(),
                ),
            }),
            (_, _, _, _) => Err(anyhow!("Invalid partition filter found: {}.", filter.1)),
        }
    }
}

/// A Struct HudiTablePartition used to represent a partition of a HudiTable.
#[derive(Clone, Debug)]
pub struct HudiTablePartition {
    pub key: String,
    pub value: Scalar<ArrayRef>,
}

impl HudiTablePartition {
    /// Create a HudiTable partition from a Tuple of (key, value).
    pub fn new<K, V>(key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<Scalar<ArrayRef>>,
    {
        HudiTablePartition {
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn from_partition_value<'a, K, V>(partition_value: (K, V)) -> Self
    where
        K: Into<Cow<'a, str>>,
        V: Into<Scalar<ArrayRef>>,
    {
        let (k, v) = partition_value;
        HudiTablePartition {
            key: k.into().into_owned(),
            value: v.into(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &Scalar<ArrayRef> {
        &self.value
    }
}

/// A HivePartition string is represented by a "key=value" format.
impl TryFrom<(&str, &DataType)> for HudiTablePartition {
    type Error = anyhow::Error;

    /// Try to create a HudiTable partition from a HivePartition string.
    /// Returns a Error if the string is not in the form of a HivePartition.
    fn try_from(partition: (&str, &DataType)) -> Result<Self> {
        let partition_splitted: Vec<&str> = partition.0.split('=').collect();
        match partition_splitted {
            partition_splitted if partition_splitted.len() == 2 => Ok(HudiTablePartition {
                key: partition_splitted[0].to_owned(),
                value: try_from_string(partition_splitted[1].to_string(), partition.1),
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

    fn try_from(partition: (&str, &HashMap<String, DataType>)) -> Result<Self> {
        let partition_splitted: Vec<&str> = partition.0.split('=').collect();
        match partition_splitted {
            partition_splitted if partition_splitted.len() == 2 => Ok(HudiTablePartition {
                key: partition_splitted[0].to_owned(),
                value: try_from_string(
                    partition_splitted[1].to_string(),
                    partition.1.get(partition_splitted[0]).unwrap(),
                ),
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
        let cast_options = CastOptions {
            safe: false,
            format_options: Default::default(),
        };
        let target = StringArray::from_iter_values(repeat("2024").take(1));
        let array_ref = cast_with_options(&target, &DataType::Utf8, &cast_options).unwrap();
        assert!(eq(&partition.value, &Scalar::new(array_ref))
            .unwrap()
            .value(0));
    }
}
