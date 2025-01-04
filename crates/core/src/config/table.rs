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
//! Hudi table configurations.

use std::collections::HashMap;
use std::str::FromStr;

use strum_macros::{AsRefStr, EnumIter};

use crate::config::error::ConfigError;
use crate::config::error::ConfigError::{
    InvalidValue, NotFound, ParseBool, ParseInt, UnsupportedValue,
};
use crate::config::Result;
use crate::config::{ConfigParser, HudiConfigValue};

/// Configurations for Hudi tables, most of them are persisted in `hoodie.properties`.
///
/// **Example**
///
/// ```rust
/// use hudi_core::config::table::HudiTableConfig::BaseFileFormat;
/// use hudi_core::table::Table as HudiTable;
///
/// let options = [(BaseFileFormat, "parquet")];
/// HudiTable::new_with_options("/tmp/hudi_data", options);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, EnumIter)]
pub enum HudiTableConfig {
    /// Base file format
    ///
    /// Currently only parquet is supported.
    BaseFileFormat,

    /// Base path to the table.
    BasePath,

    /// Table checksum is used to guard against partial writes in HDFS.
    /// It is added as the last entry in hoodie.properties and then used to validate while reading table config.
    Checksum,

    /// Database name that will be used for incremental query.
    /// If different databases have the same table name during incremental query,
    /// we can set it to limit the table name under a specific database
    DatabaseName,

    /// When set to true, will not write the partition columns into hudi. By default, false.
    DropsPartitionFields,

    /// Flag to indicate whether to use Hive style partitioning.
    /// If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
    /// By default false (the names of partition folders are only partition values)
    IsHiveStylePartitioning,

    /// Should we url encode the partition path value, before creating the folder structure.
    IsPartitionPathUrlencoded,

    /// Key Generator class property for the hoodie table
    KeyGeneratorClass,

    /// Fields used to partition the table. Concatenated values of these fields are used as
    /// the partition path, by invoking toString().
    /// These fields also include the partition type which is used by custom key generators
    PartitionFields,

    /// Field used in preCombining before actual write. By default, when two records have the same key value,
    /// the largest value for the precombine field determined by Object.compareTo(..), is picked.
    PrecombineField,

    /// When enabled, populates all meta fields. When disabled, no meta fields are populated
    /// and incremental queries will not be functional. This is only meant to be used for append only/immutable data for batch processing
    PopulatesMetaFields,

    /// Columns used to uniquely identify the table.
    /// Concatenated values of these fields are used as the record key component of HoodieKey.
    RecordKeyFields,

    /// Table name that will be used for registering with Hive. Needs to be same across runs.
    TableName,

    /// The table type for the underlying data, for this write. This can’t change between writes.
    TableType,

    /// Version of table, used for running upgrade/downgrade steps between releases with potentially
    /// breaking/backwards compatible changes.
    TableVersion,

    /// Version of timeline used, by the table.
    TimelineLayoutVersion,

    /// Timezone of the timeline timestamps. Default to UTC.
    TimelineTimezone,
}

impl AsRef<str> for HudiTableConfig {
    fn as_ref(&self) -> &str {
        match self {
            Self::BaseFileFormat => "hoodie.table.base.file.format",
            Self::BasePath => "hoodie.base.path",
            Self::Checksum => "hoodie.table.checksum",
            Self::DatabaseName => "hoodie.database.name",
            Self::DropsPartitionFields => "hoodie.datasource.write.drop.partition.columns",
            Self::IsHiveStylePartitioning => "hoodie.datasource.write.hive_style_partitioning",
            Self::IsPartitionPathUrlencoded => "hoodie.datasource.write.partitionpath.urlencode",
            Self::KeyGeneratorClass => "hoodie.table.keygenerator.class",
            Self::PartitionFields => "hoodie.table.partition.fields",
            Self::PrecombineField => "hoodie.table.precombine.field",
            Self::PopulatesMetaFields => "hoodie.populate.meta.fields",
            Self::RecordKeyFields => "hoodie.table.recordkey.fields",
            Self::TableName => "hoodie.table.name",
            Self::TableType => "hoodie.table.type",
            Self::TableVersion => "hoodie.table.version",
            Self::TimelineLayoutVersion => "hoodie.timeline.layout.version",
            Self::TimelineTimezone => "hoodie.table.timeline.timezone",
        }
    }
}

impl ConfigParser for HudiTableConfig {
    type Output = HudiConfigValue;

    fn default_value(&self) -> Option<Self::Output> {
        match self {
            Self::DatabaseName => Some(HudiConfigValue::String("default".to_string())),
            Self::DropsPartitionFields => Some(HudiConfigValue::Boolean(false)),
            Self::PartitionFields => Some(HudiConfigValue::List(vec![])),
            Self::PopulatesMetaFields => Some(HudiConfigValue::Boolean(true)),
            Self::TimelineTimezone => Some(HudiConfigValue::String(
                TimelineTimezoneValue::UTC.as_ref().to_string(),
            )),
            _ => None,
        }
    }

    fn is_required(&self) -> bool {
        matches!(self, Self::TableName | Self::TableType | Self::TableVersion)
    }

    fn parse_value(&self, configs: &HashMap<String, String>) -> Result<Self::Output> {
        let get_result = configs
            .get(self.as_ref())
            .map(|v| v.as_str())
            .ok_or(NotFound(self.key()));

        match self {
            Self::BaseFileFormat => get_result
                .and_then(BaseFileFormatValue::from_str)
                .map(|v| HudiConfigValue::String(v.as_ref().to_string())),
            Self::BasePath => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::Checksum => get_result
                .and_then(|v| {
                    isize::from_str(v).map_err(|e| ParseInt(self.key(), v.to_string(), e))
                })
                .map(HudiConfigValue::Integer),
            Self::DatabaseName => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::DropsPartitionFields => get_result
                .and_then(|v| {
                    bool::from_str(v).map_err(|e| ParseBool(self.key(), v.to_string(), e))
                })
                .map(HudiConfigValue::Boolean),
            Self::IsHiveStylePartitioning => get_result
                .and_then(|v| {
                    bool::from_str(v).map_err(|e| ParseBool(self.key(), v.to_string(), e))
                })
                .map(HudiConfigValue::Boolean),
            Self::IsPartitionPathUrlencoded => get_result
                .and_then(|v| {
                    bool::from_str(v).map_err(|e| ParseBool(self.key(), v.to_string(), e))
                })
                .map(HudiConfigValue::Boolean),
            Self::KeyGeneratorClass => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::PartitionFields => get_result
                .map(|v| HudiConfigValue::List(v.split(',').map(str::to_string).collect())),
            Self::PrecombineField => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::PopulatesMetaFields => get_result
                .and_then(|v| {
                    bool::from_str(v).map_err(|e| ParseBool(self.key(), v.to_string(), e))
                })
                .map(HudiConfigValue::Boolean),
            Self::RecordKeyFields => get_result
                .map(|v| HudiConfigValue::List(v.split(',').map(str::to_string).collect())),
            Self::TableName => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::TableType => get_result
                .and_then(TableTypeValue::from_str)
                .map(|v| HudiConfigValue::String(v.as_ref().to_string())),
            Self::TableVersion => get_result
                .and_then(|v| {
                    isize::from_str(v).map_err(|e| ParseInt(self.key(), v.to_string(), e))
                })
                .map(HudiConfigValue::Integer),
            Self::TimelineLayoutVersion => get_result
                .and_then(|v| {
                    isize::from_str(v).map_err(|e| ParseInt(self.key(), v.to_string(), e))
                })
                .map(HudiConfigValue::Integer),
            Self::TimelineTimezone => get_result
                .and_then(TimelineTimezoneValue::from_str)
                .map(|v| HudiConfigValue::String(v.as_ref().to_string())),
        }
    }
}

/// Config value for [HudiTableConfig::TableType].
#[derive(Clone, Debug, PartialEq, AsRefStr)]
pub enum TableTypeValue {
    #[strum(serialize = "COPY_ON_WRITE")]
    CopyOnWrite,
    #[strum(serialize = "MERGE_ON_READ")]
    MergeOnRead,
}

impl FromStr for TableTypeValue {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "copy_on_write" | "copy-on-write" | "cow" => Ok(Self::CopyOnWrite),
            "merge_on_read" | "merge-on-read" | "mor" => Ok(Self::MergeOnRead),
            v => Err(InvalidValue(v.to_string())),
        }
    }
}

/// Config value for [HudiTableConfig::BaseFileFormat].
#[derive(Clone, Debug, PartialEq, AsRefStr)]
pub enum BaseFileFormatValue {
    #[strum(serialize = "parquet")]
    Parquet,
}

impl FromStr for BaseFileFormatValue {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "parquet" => Ok(Self::Parquet),
            "orc" => Err(UnsupportedValue(s.to_string())),
            v => Err(InvalidValue(v.to_string())),
        }
    }
}

/// Config value for [HudiTableConfig::TimelineTimezone].
#[derive(Clone, Debug, PartialEq, AsRefStr)]
pub enum TimelineTimezoneValue {
    #[strum(serialize = "utc")]
    UTC,
    #[strum(serialize = "local")]
    Local,
}

impl FromStr for TimelineTimezoneValue {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "utc" => Ok(Self::UTC),
            "local" => Ok(Self::Local),
            v => Err(InvalidValue(v.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_table_type() {
        assert_eq!(
            TableTypeValue::from_str("cow").unwrap(),
            TableTypeValue::CopyOnWrite
        );
        assert_eq!(
            TableTypeValue::from_str("copy_on_write").unwrap(),
            TableTypeValue::CopyOnWrite
        );
        assert_eq!(
            TableTypeValue::from_str("COPY-ON-WRITE").unwrap(),
            TableTypeValue::CopyOnWrite
        );
        assert_eq!(
            TableTypeValue::from_str("MOR").unwrap(),
            TableTypeValue::MergeOnRead
        );
        assert_eq!(
            TableTypeValue::from_str("Merge_on_read").unwrap(),
            TableTypeValue::MergeOnRead
        );
        assert_eq!(
            TableTypeValue::from_str("Merge-on-read").unwrap(),
            TableTypeValue::MergeOnRead
        );
        assert!(matches!(
            TableTypeValue::from_str("").unwrap_err(),
            InvalidValue(_)
        ));
        assert!(matches!(
            TableTypeValue::from_str("copyonwrite").unwrap_err(),
            InvalidValue(_)
        ));
        assert!(matches!(
            TableTypeValue::from_str("MERGEONREAD").unwrap_err(),
            InvalidValue(_)
        ));
        assert!(matches!(
            TableTypeValue::from_str("foo").unwrap_err(),
            InvalidValue(_)
        ));
    }

    #[test]
    fn create_base_file_format() {
        assert_eq!(
            BaseFileFormatValue::from_str("parquet").unwrap(),
            BaseFileFormatValue::Parquet
        );
        assert_eq!(
            BaseFileFormatValue::from_str("PArquet").unwrap(),
            BaseFileFormatValue::Parquet
        );
        assert!(matches!(
            BaseFileFormatValue::from_str("").unwrap_err(),
            InvalidValue(_)
        ));
        assert!(matches!(
            BaseFileFormatValue::from_str("orc").unwrap_err(),
            UnsupportedValue(_)
        ));
    }

    #[test]
    fn create_timeline_timezone() {
        assert_eq!(
            TimelineTimezoneValue::from_str("utc").unwrap(),
            TimelineTimezoneValue::UTC
        );
        assert_eq!(
            TimelineTimezoneValue::from_str("uTc").unwrap(),
            TimelineTimezoneValue::UTC
        );
        assert_eq!(
            TimelineTimezoneValue::from_str("local").unwrap(),
            TimelineTimezoneValue::Local
        );
        assert_eq!(
            TimelineTimezoneValue::from_str("LOCAL").unwrap(),
            TimelineTimezoneValue::Local
        );
        assert!(matches!(
            TimelineTimezoneValue::from_str("").unwrap_err(),
            InvalidValue(_)
        ));
        assert!(matches!(
            TimelineTimezoneValue::from_str("foo").unwrap_err(),
            InvalidValue(_)
        ));
    }
}
