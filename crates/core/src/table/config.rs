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
use crate::error::HudiCoreError;

pub enum ConfigKey {
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
    TimelineTimezone,
}

impl AsRef<str> for ConfigKey {
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
            Self::TimelineTimezone => "hoodie.table.timeline.timezone",
        }
    }
}

pub enum TableType {
    CopyOnWrite,
    MergeOnRead,
}

impl FromStr for TableType {
    type Err = HudiCoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "copy_on_write" => Ok(Self::CopyOnWrite),
            "cow" => Ok(Self::CopyOnWrite),
            "merge_on_read" => Ok(Self::MergeOnRead),
            "mor" => Ok(Self::MergeOnRead),
            _ => Err(HudiCoreError::LoadTablePropertiesError)
        }
    }
}

pub enum BaseFileFormat {
    Parquet,
}


impl FromStr for BaseFileFormat {
    type Err = HudiCoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "parquet" => Ok(Self::Parquet),
            _ => Err(HudiCoreError::LoadTablePropertiesError)
        }
    }
}