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

use std::collections::HashMap;
use std::str::FromStr;

use anyhow::anyhow;
use anyhow::Result;
use strum_macros::{AsRefStr, EnumIter};

use crate::config::{ConfigParser, HudiConfigValue};

#[derive(Clone, Debug, PartialEq, Eq, Hash, EnumIter)]
pub enum HudiTableConfig {
    BaseFileFormat,
    Checksum,
    DatabaseName,
    DropsPartitionFields,
    IsHiveStylePartitioning,
    IsPartitionPathUrlencoded,
    KeyGeneratorClass,
    PartitionFields,
    PrecombineField,
    PopulatesMetaFields,
    RecordKeyFields,
    TableName,
    TableType,
    TableVersion,
    TimelineLayoutVersion,
}

impl AsRef<str> for HudiTableConfig {
    fn as_ref(&self) -> &str {
        match self {
            Self::BaseFileFormat => "hoodie.table.base.file.format",
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
            .ok_or(anyhow!("Config '{}' not found", self.as_ref()));

        match self {
            Self::BaseFileFormat => get_result
                .and_then(BaseFileFormatValue::from_str)
                .map(|v| HudiConfigValue::String(v.as_ref().to_string())),
            Self::Checksum => get_result
                .and_then(|v| isize::from_str(v).map_err(|e| anyhow!(e)))
                .map(HudiConfigValue::Integer),
            Self::DatabaseName => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::DropsPartitionFields => get_result
                .and_then(|v| bool::from_str(v).map_err(|e| anyhow!(e)))
                .map(HudiConfigValue::Boolean),
            Self::IsHiveStylePartitioning => get_result
                .and_then(|v| bool::from_str(v).map_err(|e| anyhow!(e)))
                .map(HudiConfigValue::Boolean),
            Self::IsPartitionPathUrlencoded => get_result
                .and_then(|v| bool::from_str(v).map_err(|e| anyhow!(e)))
                .map(HudiConfigValue::Boolean),
            Self::KeyGeneratorClass => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::PartitionFields => get_result
                .map(|v| HudiConfigValue::List(v.split(',').map(str::to_string).collect())),
            Self::PrecombineField => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::PopulatesMetaFields => get_result
                .and_then(|v| bool::from_str(v).map_err(|e| anyhow!(e)))
                .map(HudiConfigValue::Boolean),
            Self::RecordKeyFields => get_result
                .map(|v| HudiConfigValue::List(v.split(',').map(str::to_string).collect())),
            Self::TableName => get_result.map(|v| HudiConfigValue::String(v.to_string())),
            Self::TableType => get_result
                .and_then(TableTypeValue::from_str)
                .map(|v| HudiConfigValue::String(v.as_ref().to_string())),
            Self::TableVersion => get_result
                .and_then(|v| isize::from_str(v).map_err(|e| anyhow!(e)))
                .map(HudiConfigValue::Integer),
            Self::TimelineLayoutVersion => get_result
                .and_then(|v| isize::from_str(v).map_err(|e| anyhow!(e)))
                .map(HudiConfigValue::Integer),
        }
    }
}

#[derive(Clone, Debug, PartialEq, AsRefStr)]
pub enum TableTypeValue {
    #[strum(serialize = "COPY_ON_WRITE")]
    CopyOnWrite,
    #[strum(serialize = "MERGE_ON_READ")]
    MergeOnRead,
}

impl FromStr for TableTypeValue {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "copy_on_write" | "copy-on-write" | "cow" => Ok(Self::CopyOnWrite),
            "merge_on_read" | "merge-on-read" | "mor" => Ok(Self::MergeOnRead),
            _ => Err(anyhow!("Unsupported table type: {}", s)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, AsRefStr)]
pub enum BaseFileFormatValue {
    #[strum(serialize = "parquet")]
    Parquet,
}

impl FromStr for BaseFileFormatValue {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "parquet" => Ok(Self::Parquet),
            _ => Err(anyhow!("Unsupported base file format: {}", s)),
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
