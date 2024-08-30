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

use crate::{
    config::{ConfigParser, HudiConfigValue},
    Error::{self, ConfNotFound, InvalidConf, Unsupported},
    Result,
};

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
            .ok_or(ConfNotFound(self.as_ref().to_string()));

        match self {
            Self::BaseFileFormat => get_result
                .and_then(BaseFileFormatValue::from_str)
                .map(|v| HudiConfigValue::String(v.as_ref().to_string())),
            Self::BasePath => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::Checksum => get_result
                .and_then(|v| {
                    isize::from_str(v).map_err(|e| InvalidConf {
                        item: Self::Checksum.as_ref(),
                        source: Box::new(e),
                    })
                })
                .map(HudiConfigValue::Integer),
            Self::DatabaseName => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::DropsPartitionFields => get_result
                .and_then(|v| {
                    bool::from_str(v).map_err(|e| InvalidConf {
                        item: Self::DropsPartitionFields.as_ref(),
                        source: Box::new(e),
                    })
                })
                .map(HudiConfigValue::Boolean),
            Self::IsHiveStylePartitioning => get_result
                .and_then(|v| {
                    bool::from_str(v).map_err(|e| InvalidConf {
                        item: Self::IsHiveStylePartitioning.as_ref(),
                        source: Box::new(e),
                    })
                })
                .map(HudiConfigValue::Boolean),
            Self::IsPartitionPathUrlencoded => get_result
                .and_then(|v| {
                    bool::from_str(v).map_err(|e| InvalidConf {
                        item: Self::IsPartitionPathUrlencoded.as_ref(),
                        source: Box::new(e),
                    })
                })
                .map(HudiConfigValue::Boolean),
            Self::KeyGeneratorClass => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::PartitionFields => get_result
                .map(|v| HudiConfigValue::List(v.split(',').map(str::to_string).collect())),
            Self::PrecombineField => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::PopulatesMetaFields => get_result
                .and_then(|v| {
                    bool::from_str(v).map_err(|e| InvalidConf {
                        item: Self::PopulatesMetaFields.as_ref(),
                        source: Box::new(e),
                    })
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
                    isize::from_str(v).map_err(|e| InvalidConf {
                        item: Self::TableVersion.as_ref(),
                        source: Box::new(e),
                    })
                })
                .map(HudiConfigValue::Integer),
            Self::TimelineLayoutVersion => get_result
                .and_then(|v| {
                    isize::from_str(v).map_err(|e| InvalidConf {
                        item: Self::TimelineLayoutVersion.as_ref(),
                        source: Box::new(e),
                    })
                })
                .map(HudiConfigValue::Integer),
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
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "copy_on_write" | "copy-on-write" | "cow" => Ok(Self::CopyOnWrite),
            "merge_on_read" | "merge-on-read" | "mor" => Ok(Self::MergeOnRead),
            v => Err(Unsupported(format!("unsupported table type {}", v))),
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
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "parquet" => Ok(Self::Parquet),
            v => Err(Unsupported(format!("unsupported base file format {}", v))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::config::table::{BaseFileFormatValue, TableTypeValue};

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
        assert!(TableTypeValue::from_str("").is_err());
        assert!(TableTypeValue::from_str("copyonwrite").is_err());
        assert!(TableTypeValue::from_str("MERGEONREAD").is_err());
        assert!(TableTypeValue::from_str("foo").is_err());
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
        assert!(TableTypeValue::from_str("").is_err());
        assert!(
            TableTypeValue::from_str("orc").is_err(),
            "orc is not yet supported."
        );
    }
}
