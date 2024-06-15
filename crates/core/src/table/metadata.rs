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
use crate::table::config::{BaseFileFormat, TableType};

pub trait ProvidesTableMetadata {

    fn base_file_format(&self) -> BaseFileFormat;

    fn checksum(&self) -> i64;

    fn database_name(&self) -> String;

    fn drops_partition_fields(&self) -> bool;

    fn is_hive_style_partitioning(&self) -> bool;

    fn is_partition_path_urlencoded(&self) -> bool;

    fn is_partitioned(&self) -> bool;

    fn key_generator_class(&self) -> String;

    fn location(&self) -> String;

    fn partition_fields(&self) -> Vec<String>;

    fn precombine_field(&self) -> String;

    fn populates_meta_fields(&self) -> bool;

    fn record_key_fields(&self) -> Vec<String>;

    fn table_name(&self) -> String;

    fn table_type(&self) -> TableType;

    fn table_version(&self) -> u32;

    fn timeline_layout_version(&self) -> u32;

    fn timeline_timezone(&self) -> String;
}