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
use std::sync::Arc;

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::table::{HudiTableConfig, TableTypeValue};
use crate::error::CoreError;
use crate::metadata::HUDI_METADATA_DIR;
use crate::storage::Storage;
use crate::table::Table;
use crate::write::metadata::bootstrap_metadata_table;

/// Builder for creating a new Hudi table on storage (pyIceberg-style create).
///
/// Defaults target Phase A append-only COW: table version 6, timeline layout 1,
/// parquet base files, `hoodie.populate.meta.fields=false`.
#[derive(Debug, Clone)]
pub struct TableCreateBuilder {
    base_uri: String,
    table_name: Option<String>,
    table_type: TableTypeValue,
    table_version: isize,
    timeline_layout_version: isize,
    populates_meta_fields: bool,
    record_key_fields: Vec<String>,
    partition_fields: Vec<String>,
    metadata_enabled: bool,
    options: HashMap<String, String>,
    storage_options: HashMap<String, String>,
}

impl TableCreateBuilder {
    pub fn new(base_uri: impl Into<String>) -> Self {
        Self {
            base_uri: base_uri.into(),
            table_name: None,
            table_type: TableTypeValue::CopyOnWrite,
            table_version: 6,
            timeline_layout_version: 1,
            populates_meta_fields: false,
            record_key_fields: Vec::new(),
            partition_fields: Vec::new(),
            metadata_enabled: false,
            options: HashMap::new(),
            storage_options: HashMap::new(),
        }
    }

    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.table_name = Some(name.into());
        self
    }

    pub fn with_table_type(mut self, table_type: TableTypeValue) -> Self {
        self.table_type = table_type;
        self
    }

    pub fn with_table_version(mut self, version: isize) -> Self {
        self.table_version = version;
        self
    }

    pub fn with_timeline_layout_version(mut self, version: isize) -> Self {
        self.timeline_layout_version = version;
        self
    }

    pub fn with_populates_meta_fields(mut self, value: bool) -> Self {
        self.populates_meta_fields = value;
        self
    }

    pub fn with_record_key_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.record_key_fields = fields.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_partition_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.partition_fields = fields.into_iter().map(Into::into).collect();
        self
    }

    /// Enable the internal metadata table with the `files` partition.
    pub fn with_metadata(mut self, enabled: bool) -> Self {
        self.metadata_enabled = enabled;
        if enabled {
            self.table_version = 8;
            self.timeline_layout_version = 2;
        }
        self
    }

    pub fn with_option(mut self, key: impl AsRef<str>, value: impl Into<String>) -> Self {
        self.options.insert(key.as_ref().to_string(), value.into());
        self
    }

    pub fn with_storage_option(mut self, key: impl AsRef<str>, value: impl Into<String>) -> Self {
        self.storage_options
            .insert(key.as_ref().to_string(), value.into());
        self
    }

    /// Create the table layout on storage and open a [`Table`] handle.
    pub async fn create(self) -> Result<Table> {
        let table_name = self.table_name.ok_or_else(|| {
            CoreError::Write("Table name is required to create a Hudi table".to_string())
        })?;

        if self.table_version >= 8 && self.timeline_layout_version != 2 {
            return Err(CoreError::Write(format!(
                "Table version {} requires timeline layout version 2",
                self.table_version
            )));
        }
        if self.table_version < 8 && self.timeline_layout_version != 1 {
            return Err(CoreError::Write(format!(
                "Table version {} requires timeline layout version 1",
                self.table_version
            )));
        }

        let mut props: HashMap<String, String> = HashMap::new();
        props.insert(
            HudiTableConfig::TableName.as_ref().to_string(),
            table_name.clone(),
        );
        props.insert(
            HudiTableConfig::TableType.as_ref().to_string(),
            self.table_type.as_ref().to_string(),
        );
        props.insert(
            HudiTableConfig::TableVersion.as_ref().to_string(),
            self.table_version.to_string(),
        );
        props.insert(
            HudiTableConfig::TimelineLayoutVersion.as_ref().to_string(),
            self.timeline_layout_version.to_string(),
        );
        props.insert(
            HudiTableConfig::PopulatesMetaFields.as_ref().to_string(),
            self.populates_meta_fields.to_string(),
        );
        props.insert(
            HudiTableConfig::BaseFileFormat.as_ref().to_string(),
            "parquet".to_string(),
        );
        if self.metadata_enabled {
            props.insert(
                HudiTableConfig::MetadataTableEnabled.as_ref().to_string(),
                "true".to_string(),
            );
            props.insert(
                HudiTableConfig::MetadataTablePartitions
                    .as_ref()
                    .to_string(),
                "files".to_string(),
            );
        }
        if !self.record_key_fields.is_empty() {
            props.insert(
                HudiTableConfig::RecordKeyFields.as_ref().to_string(),
                self.record_key_fields.join(","),
            );
        }
        if !self.partition_fields.is_empty() {
            props.insert(
                HudiTableConfig::PartitionFields.as_ref().to_string(),
                self.partition_fields.join(","),
            );
        }
        for (k, v) in self.options {
            props.insert(k, v);
        }

        let mut hudi_for_storage = props.clone();
        hudi_for_storage.insert(
            HudiTableConfig::BasePath.as_ref().to_string(),
            self.base_uri.clone(),
        );
        let storage = Storage::new(
            Arc::new(self.storage_options.clone()),
            Arc::new(HudiConfigs::new(hudi_for_storage)),
        )?;

        let properties_path = format!("{HUDI_METADATA_DIR}/hoodie.properties");
        if storage.exists(&properties_path).await? {
            return Err(CoreError::Write(format!(
                "Cannot create table at '{}': {properties_path} already exists",
                self.base_uri
            )));
        }

        let mut body = String::from(
            "# Generated by hudi-rs\n# Licensed under the Apache License, Version 2.0\n",
        );
        let mut keys: Vec<_> = props.keys().cloned().collect();
        keys.sort();
        for key in keys {
            let value = &props[&key];
            body.push_str(&format!("{key}={value}\n"));
        }
        storage
            .put_file(&properties_path, body.into_bytes())
            .await?;

        // Ensure layout-v2 timeline directory exists (empty marker via properties is enough for v1).
        if self.timeline_layout_version == 2 {
            let timeline_path: String = props
                .get(HudiTableConfig::TimelinePath.as_ref())
                .cloned()
                .unwrap_or_else(|| "timeline".to_string());
            let marker = format!("{HUDI_METADATA_DIR}/{timeline_path}/.keep");
            storage.put_file(&marker, b"".as_slice()).await?;
        }
        if self.metadata_enabled {
            bootstrap_metadata_table(&storage, &table_name, "00000000000000000").await?;
        }

        Table::new_with_options(&self.base_uri, self.storage_options).await
    }
}
