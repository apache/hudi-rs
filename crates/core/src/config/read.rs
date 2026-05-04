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
/// use hudi_core::config::read::HudiReadConfig;
/// use hudi_core::table::ReadOptions;
///
/// let options = ReadOptions::new()
///     .with_hudi_option(HudiReadConfig::StreamBatchSize.as_ref(), "2048");
/// ```
///

#[derive(Clone, Debug, PartialEq, Eq, Hash, EnumIter, IntoStaticStr)]
pub enum HudiReadConfig {
    /// Selects the read semantic. Accepted values: `snapshot` (default), `incremental`.
    /// See [`crate::table::QueryType`].
    QueryType,

    /// Snapshot/time-travel timestamp. Reads return the table state at this commit.
    AsOfTimestamp,

    /// Start timestamp (exclusive) used by file-group readers to filter records.
    StartTimestamp,

    /// End timestamp (inclusive) used by file-group readers to filter records.
    EndTimestamp,

    /// Number of input partitions to read the data in parallel.
    ///
    /// For processing 100 files, [InputPartitions] being 5 will produce 5 partitions, with each partition having 20 files.
    InputPartitions,

    /// When set to true, only base files will be read for optimized reads.
    /// This is only applicable to Merge-On-Read (MOR) tables.
    UseReadOptimizedMode,

    /// Target number of rows per batch for streaming reads.
    /// This controls the batch size when using streaming APIs.
    StreamBatchSize,
}

impl HudiReadConfig {
    /// `&'static str` form of the config key. `const fn` so callers can use it in
    /// `const` contexts (e.g. building static lookup tables of `hoodie.*` keys
    /// without duplicating the literal strings).
    pub const fn key_str(&self) -> &'static str {
        match self {
            Self::QueryType => "hoodie.read.query.type",
            Self::AsOfTimestamp => "hoodie.read.as.of.timestamp",
            Self::StartTimestamp => "hoodie.read.start.timestamp",
            Self::EndTimestamp => "hoodie.read.end.timestamp",
            Self::InputPartitions => "hoodie.read.input.partitions",
            Self::UseReadOptimizedMode => "hoodie.read.use.read_optimized.mode",
            Self::StreamBatchSize => "hoodie.read.stream.batch_size",
        }
    }
}

impl AsRef<str> for HudiReadConfig {
    fn as_ref(&self) -> &str {
        self.key_str()
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
            Self::UseReadOptimizedMode => get_result
                .and_then(|v| {
                    bool::from_str(v).map_err(|e| ParseBool(self.key(), v.to_string(), e))
                })
                .map(HudiConfigValue::Boolean),
            Self::StreamBatchSize => get_result
                .and_then(|v| {
                    let key = self.key();
                    let parsed =
                        usize::from_str(v).map_err(|e| ParseInt(key.clone(), v.to_string(), e))?;
                    if parsed == 0 {
                        return Err(InvalidValue(format!("{key}=0 (must be > 0)")));
                    }
                    Ok(parsed)
                })
                .map(HudiConfigValue::UInteger),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::read::HudiReadConfig::{
        AsOfTimestamp, EndTimestamp, InputPartitions, QueryType as QueryTypeKey, StartTimestamp,
        StreamBatchSize, UseReadOptimizedMode,
    };

    #[test]
    fn parse_valid_config_value() {
        let options = HashMap::from([
            (QueryTypeKey.as_ref().to_string(), "Incremental".to_string()),
            (AsOfTimestamp.as_ref().to_string(), "20240101".to_string()),
            (StartTimestamp.as_ref().to_string(), "20240102".to_string()),
            (EndTimestamp.as_ref().to_string(), "20240103".to_string()),
            (InputPartitions.as_ref().to_string(), "100".to_string()),
            (
                UseReadOptimizedMode.as_ref().to_string(),
                "true".to_string(),
            ),
            (StreamBatchSize.as_ref().to_string(), "2048".to_string()),
        ]);
        let actual: String = QueryTypeKey.parse_value(&options).unwrap().into();
        assert_eq!(actual, "incremental");
        let actual: String = AsOfTimestamp.parse_value(&options).unwrap().into();
        assert_eq!(actual, "20240101");
        let actual: String = StartTimestamp.parse_value(&options).unwrap().into();
        assert_eq!(actual, "20240102");
        let actual: String = EndTimestamp.parse_value(&options).unwrap().into();
        assert_eq!(actual, "20240103");
        let actual: usize = InputPartitions.parse_value(&options).unwrap().into();
        assert_eq!(actual, 100);
        let actual: bool = UseReadOptimizedMode.parse_value(&options).unwrap().into();
        assert!(actual);
        let actual: usize = StreamBatchSize.parse_value(&options).unwrap().into();
        assert_eq!(actual, 2048);
    }

    #[test]
    fn parse_invalid_config_value() {
        let options = HashMap::from([
            (QueryTypeKey.as_ref().to_string(), "bogus".to_string()),
            (InputPartitions.as_ref().to_string(), "foo".to_string()),
            (UseReadOptimizedMode.as_ref().to_string(), "1".to_string()),
            (StreamBatchSize.as_ref().to_string(), "abc".to_string()),
        ]);
        assert!(matches!(
            QueryTypeKey.parse_value(&options).unwrap_err(),
            InvalidValue(_)
        ));
        let actual: String = QueryTypeKey.parse_value_or_default(&options).into();
        assert_eq!(actual, "snapshot");
        assert!(matches!(
            InputPartitions.parse_value(&options).unwrap_err(),
            ParseInt(_, _, _)
        ));
        let actual: usize = InputPartitions.parse_value_or_default(&options).into();
        assert_eq!(actual, 0);
        assert!(matches!(
            UseReadOptimizedMode.parse_value(&options).unwrap_err(),
            ParseBool(_, _, _)
        ));
        let actual: bool = UseReadOptimizedMode.parse_value_or_default(&options).into();
        assert!(!actual);
        assert!(matches!(
            StreamBatchSize.parse_value(&options).unwrap_err(),
            ParseInt(_, _, _)
        ));
        let actual: usize = StreamBatchSize.parse_value_or_default(&options).into();
        assert_eq!(actual, 1024);
    }

    #[test]
    fn timestamp_keys_have_no_default_value() {
        assert!(AsOfTimestamp.default_value().is_none());
        assert!(StartTimestamp.default_value().is_none());
        assert!(EndTimestamp.default_value().is_none());
    }

    #[test]
    fn query_type_from_str_accepts_case_insensitive_and_rejects_invalid() {
        assert_eq!(
            QueryType::from_str("snapshot").unwrap(),
            QueryType::Snapshot
        );
        assert_eq!(
            QueryType::from_str("SNAPSHOT").unwrap(),
            QueryType::Snapshot
        );
        assert_eq!(
            QueryType::from_str("Incremental").unwrap(),
            QueryType::Incremental
        );
        assert!(matches!(
            QueryType::from_str("bogus").unwrap_err(),
            InvalidValue(_)
        ));
    }

    #[test]
    fn display_impls_match_canonical_keys() {
        assert_eq!(format!("{}", QueryType::Snapshot), "snapshot");
        assert_eq!(format!("{}", QueryType::Incremental), "incremental");
        assert_eq!(
            format!("{}", HudiReadConfig::StreamBatchSize),
            "hoodie.read.stream.batch_size"
        );
        assert_eq!(
            format!("{}", HudiReadConfig::QueryType),
            "hoodie.read.query.type"
        );
    }
}
