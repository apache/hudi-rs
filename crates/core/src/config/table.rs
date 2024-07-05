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

use std::str::FromStr;

use anyhow::anyhow;
use anyhow::Result;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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

#[derive(Debug, PartialEq)]
pub enum TableTypeValue {
    CopyOnWrite,
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

#[derive(Debug, PartialEq)]
pub enum BaseFileFormatValue {
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

pub trait TablePropsProvider {
    fn base_file_format(&self) -> BaseFileFormatValue;

    fn checksum(&self) -> Option<i64>;

    fn database_name(&self) -> Option<String>;

    fn drops_partition_fields(&self) -> Option<bool>;

    fn is_hive_style_partitioning(&self) -> Option<bool>;

    fn is_partition_path_urlencoded(&self) -> Option<bool>;

    fn is_partitioned(&self) -> Option<bool>;

    fn key_generator_class(&self) -> Option<String>;

    fn location(&self) -> String;

    fn partition_fields(&self) -> Option<Vec<String>>;

    fn precombine_field(&self) -> Option<String>;

    fn populates_meta_fields(&self) -> Option<bool>;

    fn record_key_fields(&self) -> Option<Vec<String>>;

    fn table_name(&self) -> String;

    fn table_type(&self) -> TableTypeValue;

    fn table_version(&self) -> i32;

    fn timeline_layout_version(&self) -> Option<i32>;
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
