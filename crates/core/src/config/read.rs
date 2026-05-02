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
//! Hudi read configurations.

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

use strum_macros::{AsRefStr, EnumIter, IntoStaticStr};

use crate::config::Result;
use crate::config::error::ConfigError;
use crate::config::error::ConfigError::{InvalidValue, NotFound, ParseBool, ParseInt};
use crate::config::{ConfigParser, HudiConfigValue};

/// Config value for [`HudiReadConfig::QueryType`]. Canonical strings are
/// `snapshot` and `incremental`; [`FromStr`] accepts case-insensitive forms.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, AsRefStr)]
pub enum QueryType {
    /// Latest table state at one commit (the latest by default; an explicit
    /// `as_of_timestamp` for time-travel).
    #[default]
    #[strum(serialize = "snapshot")]
    Snapshot,
    /// Records changed in the half-open range (`start_timestamp`, `end_timestamp`].
    #[strum(serialize = "incremental")]
    Incremental,
}

impl Display for QueryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl FromStr for QueryType {
    type Err = ConfigError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "snapshot" => Ok(Self::Snapshot),
            "incremental" => Ok(Self::Incremental),
            v => Err(InvalidValue(v.to_string())),
        }
    }
}

/// Configurations for reading Hudi tables.
///
/// **Example**
///
/// ```rust
/// use hudi_core::config::read::HudiReadConfig::InputPartitions;
/// use hudi_core::table::Table as HudiTable;
///
/// # #[tokio::main]
/// # async fn main() {
/// let options = [(InputPartitions, "2")];
/// HudiTable::new_with_options("/tmp/hudi_data", options).await;
/// # }
/// ```
///

#[derive(Clone, Debug, PartialEq, Eq, Hash, EnumIter, IntoStaticStr)]
pub enum HudiReadConfig {
    /// Selects the read semantic. Accepted values: `snapshot` (default), `incremental`.
    /// See [`crate::table::QueryType`].
    QueryType,

    /// Snapshot/time-travel timestamp. Reads return the table state at this commit.
    AsOfTimestamp,

    /// Start timestamp (exclusive) for [FileGroup] to filter records.
    StartTimestamp,

    /// End timestamp (inclusive) for [FileGroup] to filter records.
    EndTimestamp,

    /// Number of input partitions to read the data in parallel.
    ///
    /// For processing 100 files, [InputPartitions] being 5 will produce 5 partitions, with each partition having 20 files.
    InputPartitions,

    /// Parallelism for listing files on storage.
    ListingParallelism,

    /// When set to true, only [BaseFile]s will be read for optimized reads.
    /// This is only applicable to Merge-On-Read (MOR) tables.
    UseReadOptimizedMode,

    /// Target number of rows per batch for streaming reads.
    /// This controls the batch size when using streaming APIs.
    StreamBatchSize,
}

impl AsRef<str> for HudiReadConfig {
    fn as_ref(&self) -> &str {
        match self {
            Self::QueryType => "hoodie.read.query.type",
            Self::AsOfTimestamp => "hoodie.read.as.of.timestamp",
            Self::StartTimestamp => "hoodie.read.start.timestamp",
            Self::EndTimestamp => "hoodie.read.end.timestamp",
            Self::InputPartitions => "hoodie.read.input.partitions",
            Self::ListingParallelism => "hoodie.read.listing.parallelism",
            Self::UseReadOptimizedMode => "hoodie.read.use.read_optimized.mode",
            Self::StreamBatchSize => "hoodie.read.stream.batch_size",
        }
    }
}

impl Display for HudiReadConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl ConfigParser for HudiReadConfig {
    type Output = HudiConfigValue;

    fn default_value(&self) -> Option<HudiConfigValue> {
        match self {
            HudiReadConfig::QueryType => Some(HudiConfigValue::String(
                QueryType::default().as_ref().to_string(),
            )),
            HudiReadConfig::InputPartitions => Some(HudiConfigValue::UInteger(0usize)),
            HudiReadConfig::ListingParallelism => Some(HudiConfigValue::UInteger(10usize)),
            HudiReadConfig::UseReadOptimizedMode => Some(HudiConfigValue::Boolean(false)),
            HudiReadConfig::StreamBatchSize => Some(HudiConfigValue::UInteger(1024usize)),
            _ => None,
        }
    }

    fn parse_value(&self, configs: &HashMap<String, String>) -> Result<Self::Output> {
        let get_result = configs
            .get(self.as_ref())
            .map(|v| v.as_str())
            .ok_or(NotFound(self.key()));

        match self {
            Self::QueryType => get_result
                .and_then(QueryType::from_str)
                .map(|v| HudiConfigValue::String(v.as_ref().to_string())),
            Self::AsOfTimestamp => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::StartTimestamp => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::EndTimestamp => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::InputPartitions => get_result
                .and_then(|v| {
                    usize::from_str(v).map_err(|e| ParseInt(self.key(), v.to_string(), e))
                })
                .map(HudiConfigValue::UInteger),
            Self::ListingParallelism => get_result
                .and_then(|v| {
                    usize::from_str(v).map_err(|e| ParseInt(self.key(), v.to_string(), e))
                })
                .map(HudiConfigValue::UInteger),
            Self::UseReadOptimizedMode => get_result
                .and_then(|v| {
                    bool::from_str(v).map_err(|e| ParseBool(self.key(), v.to_string(), e))
                })
                .map(HudiConfigValue::Boolean),
            Self::StreamBatchSize => get_result
                .and_then(|v| {
                    usize::from_str(v).map_err(|e| ParseInt(self.key(), v.to_string(), e))
                })
                .map(HudiConfigValue::UInteger),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::read::HudiReadConfig::{
        InputPartitions, ListingParallelism, UseReadOptimizedMode,
    };

    #[test]
    fn parse_valid_config_value() {
        let options = HashMap::from([
            (InputPartitions.as_ref().to_string(), "100".to_string()),
            (ListingParallelism.as_ref().to_string(), "100".to_string()),
            (
                UseReadOptimizedMode.as_ref().to_string(),
                "true".to_string(),
            ),
        ]);
        let actual: usize = InputPartitions.parse_value(&options).unwrap().into();
        assert_eq!(actual, 100);
        let actual: usize = ListingParallelism.parse_value(&options).unwrap().into();
        assert_eq!(actual, 100);
        let actual: bool = UseReadOptimizedMode.parse_value(&options).unwrap().into();
        assert!(actual);
    }

    #[test]
    fn parse_invalid_config_value() {
        let options = HashMap::from([
            (InputPartitions.as_ref().to_string(), "foo".to_string()),
            (ListingParallelism.as_ref().to_string(), "_100".to_string()),
            (UseReadOptimizedMode.as_ref().to_string(), "1".to_string()),
        ]);
        assert!(matches!(
            InputPartitions.parse_value(&options).unwrap_err(),
            ParseInt(_, _, _)
        ));
        let actual: usize = InputPartitions.parse_value_or_default(&options).into();
        assert_eq!(actual, 0);
        assert!(matches!(
            ListingParallelism.parse_value(&options).unwrap_err(),
            ParseInt(_, _, _)
        ));
        let actual: usize = ListingParallelism.parse_value_or_default(&options).into();
        assert_eq!(actual, 10);
        assert!(matches!(
            UseReadOptimizedMode.parse_value(&options).unwrap_err(),
            ParseBool(_, _, _)
        ));
        let actual: bool = UseReadOptimizedMode.parse_value_or_default(&options).into();
        assert!(!actual)
    }
}
