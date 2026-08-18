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

/// Builder for creating a new Hudi table on storage.
///
/// Defaults: metadata table on, record index on, table version 9 (1.1.x/1.2
/// format; use `with_table_version(8)` for 1.0.x) / timeline layout 2,
/// hive-style partitioning on, partition columns retained in data files.
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
    ordering_fields: Vec<String>,
    metadata_enabled: bool,
    record_index_enabled: bool,
    column_stats_enabled: bool,
    partition_stats_enabled: bool,
    hive_style_partitioning: bool,
    options: HashMap<String, String>,
    storage_options: HashMap<String, String>,
}

impl TableCreateBuilder {
    pub fn new(base_uri: impl Into<String>) -> Self {
        Self {
            base_uri: base_uri.into(),
            table_name: None,
            table_type: TableTypeValue::CopyOnWrite,
            table_version: 9,
            timeline_layout_version: 2,
            // Match Java HoodieTableConfig default (Spark writers persist true).
            populates_meta_fields: true,
            record_key_fields: Vec::new(),
            partition_fields: Vec::new(),
            ordering_fields: Vec::new(),
            metadata_enabled: true,
            record_index_enabled: true,
            // Match Spark engine defaults: column stats on when MDT is on.
            column_stats_enabled: true,
            partition_stats_enabled: true,
            hive_style_partitioning: true,
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

    /// Set the field used to order records with the same key during merges.
    pub fn with_ordering_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.ordering_fields = fields.into_iter().map(Into::into).collect();
        self
    }

    /// Enable/disable the internal metadata table (`files`, and optionally indexes).
    ///
    /// Defaults to enabled. Disabling also turns off record/column/partition stats
    /// indexes and falls back to table version 6 / timeline layout 1 unless the
    /// caller overrides versions later.
    pub fn with_metadata(mut self, enabled: bool) -> Self {
        self.metadata_enabled = enabled;
        if enabled {
            // MDT requires v8+; never downgrade an explicit v9 choice.
            if self.table_version < 8 {
                self.table_version = 8;
            }
            self.timeline_layout_version = 2;
        } else {
            self.record_index_enabled = false;
            self.column_stats_enabled = false;
            self.partition_stats_enabled = false;
            self.table_version = 6;
            self.timeline_layout_version = 1;
        }
        self
    }

    /// Enable/disable the metadata-table record-level index used for upsert/delete tagging.
    ///
    /// Defaults to enabled. When disabled, writers use [`crate::index::SimpleIndex`].
    /// Requires metadata table enabled.
    pub fn with_record_index(mut self, enabled: bool) -> Self {
        self.record_index_enabled = enabled;
        self
    }

    /// Enable/disable MDT `column_stats` (file-level column ranges).
    ///
    /// Defaults to enabled when metadata is on (Spark-aligned). Disabling also
    /// disables `partition_stats`.
    pub fn with_column_stats(mut self, enabled: bool) -> Self {
        self.column_stats_enabled = enabled;
        if !enabled {
            self.partition_stats_enabled = false;
        }
        self
    }

    /// Enable/disable MDT `partition_stats` (requires `column_stats`).
    ///
    /// Defaults to enabled when metadata is on. Ignored for non-partitioned
    /// tables, matching Java's metadata writer.
    pub fn with_partition_stats(mut self, enabled: bool) -> Self {
        self.partition_stats_enabled = enabled;
        self
    }

    /// Use hive-style partition paths (`city=sf`) when partition fields are set.
    ///
    /// Defaults to true. Partition column values are always retained in data files.
    pub fn with_hive_style_partitioning(mut self, enabled: bool) -> Self {
        self.hive_style_partitioning = enabled;
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

        if self.record_index_enabled && !self.metadata_enabled {
            return Err(CoreError::Write(
                "record index requires the metadata table; call with_metadata(true) or with_record_index(false)"
                    .to_string(),
            ));
        }
        if self.column_stats_enabled && !self.metadata_enabled {
            return Err(CoreError::Write(
                "column_stats requires the metadata table; call with_metadata(true) or with_column_stats(false)"
                    .to_string(),
            ));
        }
        if self.partition_stats_enabled && !self.column_stats_enabled {
            return Err(CoreError::Write(
                "partition_stats requires column_stats; call with_column_stats(true) or with_partition_stats(false)"
                    .to_string(),
            ));
        }
        // Java HoodieBackedTableMetadataWriter removes PARTITION_STATS for
        // non-partitioned tables; mirror that instead of erroring.
        let partition_stats_enabled =
            self.partition_stats_enabled && !self.partition_fields.is_empty();

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
            HudiTableConfig::DatabaseName.as_ref().to_string(),
            "default".to_string(),
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
            "hoodie.table.initial.version".to_string(),
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
        // Java persists uppercase PARQUET (HoodieFileFormat enum name).
        props.insert(
            HudiTableConfig::BaseFileFormat.as_ref().to_string(),
            "PARQUET".to_string(),
        );
        props.insert(
            HudiTableConfig::IsHiveStylePartitioning
                .as_ref()
                .to_string(),
            self.hive_style_partitioning.to_string(),
        );
        // Partition columns stay in data files (never strip hive-style values).
        props.insert(
            HudiTableConfig::DropsPartitionFields.as_ref().to_string(),
            "false".to_string(),
        );
        props.insert(
            HudiTableConfig::IsPartitionPathUrlencoded
                .as_ref()
                .to_string(),
            "false".to_string(),
        );
        props.insert(
            HudiTableConfig::TimelinePath.as_ref().to_string(),
            "timeline".to_string(),
        );
        props.insert(
            HudiTableConfig::TimelineHistoryPath.as_ref().to_string(),
            "history".to_string(),
        );
        props.insert(
            HudiTableConfig::ArchiveLogFolder.as_ref().to_string(),
            "history".to_string(),
        );
        // LOCAL like the Spark writer default: Spark table services mint
        // LOCAL-time instants regardless of the table property, so declaring
        // (and minting) LOCAL keeps mixed-writer timelines ordered.
        let timeline_timezone = self
            .options
            .get(HudiTableConfig::TimelineTimezone.as_ref())
            .cloned()
            .unwrap_or_else(|| "LOCAL".to_string());
        crate::write::set_commit_timezone(&timeline_timezone);
        props.insert(
            HudiTableConfig::TimelineTimezone.as_ref().to_string(),
            timeline_timezone,
        );
        props.insert("hoodie.table.cdc.enabled".to_string(), "false".to_string());
        props.insert(
            "hoodie.partition.metafile.use.base.format".to_string(),
            "false".to_string(),
        );
        props.insert(
            "hoodie.table.metadata.partitions.inflight".to_string(),
            String::new(),
        );
        props.insert(
            "hoodie.table.multiple.base.file.formats.enable".to_string(),
            "false".to_string(),
        );
        // Canonical MDT signal is hoodie.table.metadata.partitions (Java create).
        // Do not invent hoodie.metadata.enable / hoodie.metadata.record.index.enable
        // into hoodie.properties — those are write-side configs in Java.
        if self.metadata_enabled {
            let mut partitions = vec!["files".to_string()];
            if self.record_index_enabled {
                partitions.push("record_index".to_string());
            }
            if self.column_stats_enabled {
                partitions.push("column_stats".to_string());
            }
            if partition_stats_enabled {
                partitions.push("partition_stats".to_string());
            }
            // Java HoodieTableConfig persists the list alphabetically sorted.
            partitions.sort();
            props.insert(
                HudiTableConfig::MetadataTablePartitions
                    .as_ref()
                    .to_string(),
                partitions.join(","),
            );
        }
        if !self.record_key_fields.is_empty() {
            props.insert(
                HudiTableConfig::RecordKeyFields.as_ref().to_string(),
                self.record_key_fields.join(","),
            );
            props.insert(
                HudiTableConfig::KeyGeneratorType.as_ref().to_string(),
                if self.partition_fields.is_empty() {
                    "NON_PARTITION".to_string()
                } else if self.record_key_fields.len() == 1 && self.partition_fields.len() == 1 {
                    "SIMPLE".to_string()
                } else {
                    "COMPLEX".to_string()
                },
            );
        } else {
            // Auto record-key generation (Java AutoRecordGenWrapper*).
            props.insert(
                HudiTableConfig::KeyGeneratorType.as_ref().to_string(),
                if self.partition_fields.is_empty() {
                    "NON_PARTITION".to_string()
                } else if self.partition_fields.len() == 1 {
                    "SIMPLE".to_string()
                } else {
                    "COMPLEX".to_string()
                },
            );
        }
        if !self.partition_fields.is_empty() {
            props.insert(
                HudiTableConfig::PartitionFields.as_ref().to_string(),
                self.partition_fields.join(","),
            );
        }
        // Java v8+ always persists merge mode. Ordering/precombine is optional.
        if self.populates_meta_fields {
            if !self.ordering_fields.is_empty() {
                props.insert(
                    HudiTableConfig::OrderingFields.as_ref().to_string(),
                    self.ordering_fields.join(","),
                );
                // tv9 replaces the deprecated precombine key with ordering
                // fields (Java EightToNineUpgradeHandler removes it).
                if self.table_version < 9 {
                    props.insert(
                        "hoodie.table.precombine.field".to_string(),
                        self.ordering_fields.join(","),
                    );
                }
                props.insert(
                    "hoodie.record.merge.mode".to_string(),
                    "EVENT_TIME_ORDERING".to_string(),
                );
                props.insert(
                    "hoodie.record.merge.strategy.id".to_string(),
                    "eeb8d96f-b1e4-49fd-bbf8-28ac514178e5".to_string(),
                );
            } else {
                props.insert(
                    "hoodie.record.merge.mode".to_string(),
                    "COMMIT_TIME_ORDERING".to_string(),
                );
                props.insert(
                    "hoodie.record.merge.strategy.id".to_string(),
                    "ce9acb64-bde0-424c-9b91-f6ebba25356d".to_string(),
                );
            }
            // Payload classes are deprecated in favor of merge mode / strategy; do not persist them.
        }
        for (k, v) in self.options {
            props.insert(k, v);
        }

        // Checksum last, matching Java OrderedProperties / storeProperties.
        let database = props
            .get(HudiTableConfig::DatabaseName.as_ref())
            .cloned()
            .unwrap_or_default();
        let checksum = table_checksum(&database, &table_name);
        props.insert(
            HudiTableConfig::Checksum.as_ref().to_string(),
            checksum.to_string(),
        );

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
        // Emit checksum last like Java HoodieTableConfig.storeProperties.
        keys.retain(|k| k != HudiTableConfig::Checksum.as_ref());
        for key in keys {
            let value = &props[&key];
            body.push_str(&format!("{key}={value}\n"));
        }
        body.push_str(&format!(
            "{}={}\n",
            HudiTableConfig::Checksum.as_ref(),
            props[HudiTableConfig::Checksum.as_ref()]
        ));
        storage
            .put_file(&properties_path, body.into_bytes())
            .await?;

        if self.timeline_layout_version == 2 {
            let timeline_path: String = props
                .get(HudiTableConfig::TimelinePath.as_ref())
                .cloned()
                .unwrap_or_else(|| "timeline".to_string());
            let marker = format!("{HUDI_METADATA_DIR}/{timeline_path}/.keep");
            storage.put_file(&marker, b"".as_slice()).await?;
        }
        if self.metadata_enabled {
            bootstrap_metadata_table(
                &storage,
                &table_name,
                self.table_version,
                self.record_index_enabled,
                self.column_stats_enabled,
                partition_stats_enabled,
            )
            .await?;
        }

        Table::new_with_options(&self.base_uri, self.storage_options).await
    }
}

/// Java `HoodieTableConfig.generateChecksum`: CRC32 of `{database}.{table}`.
fn table_checksum(database: &str, table: &str) -> u32 {
    crc32(format!("{database}.{table}").as_bytes())
}

/// IEEE CRC-32 matching `java.util.zip.CRC32` / `BinaryUtil.generateChecksum`.
fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff_u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xedb8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_matches_java_binary_util() {
        assert_eq!(table_checksum("default", "trips"), 2_200_697_520);
        assert_eq!(table_checksum("", "trips"), 3_761_586_722);
        assert_eq!(table_checksum("", "trips_metadata"), 1_249_152_950);
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use crate::config::table::TableTypeValue;
    use crate::table::Table;

    /// Default create: COW tv9, partitioned, event-time ordering, MDT with
    /// files/RLI/col-stats/partition-stats. Exercises the full property and
    /// bootstrap body from inside the crate (tarpaulin registers lib-internal
    /// runs reliably; integration binaries are inconsistently profiled).
    #[tokio::test]
    async fn test_create_cow_default_full_surface() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("trips")
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_ordering_fields(["ts"])
            .create()
            .await
            .unwrap();
        assert!(table.is_metadata_table_enabled());
        let props = std::fs::read_to_string(dir.path().join(".hoodie/hoodie.properties")).unwrap();
        for expected in [
            "hoodie.table.name=trips",
            "hoodie.table.type=COPY_ON_WRITE",
            "hoodie.table.version=9",
            "hoodie.timeline.layout.version=2",
            "hoodie.table.recordkey.fields=id",
            "hoodie.table.partition.fields=city",
            "hoodie.record.merge.mode=EVENT_TIME_ORDERING",
            "hoodie.table.checksum=",
        ] {
            assert!(
                props.contains(expected),
                "missing `{expected}` in:\n{props}"
            );
        }
        let mdt_props = std::fs::read_to_string(
            dir.path()
                .join(".hoodie/metadata/.hoodie/hoodie.properties"),
        )
        .unwrap();
        assert!(mdt_props.contains("hoodie.table.name=trips_metadata"));
        assert!(mdt_props.contains("hoodie.table.type=MERGE_ON_READ"));
    }

    /// tv8 MOR with commit-time ordering, no hive-style, no meta fields:
    /// the other half of the property matrix.
    #[tokio::test]
    async fn test_create_mor_tv8_variants() {
        let dir = tempfile::tempdir().unwrap();
        let _ = Table::create(dir.path().to_str().unwrap())
            .with_table_name("trips8")
            .with_table_type(TableTypeValue::MergeOnRead)
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_hive_style_partitioning(false)
            .with_table_version(8)
            .create()
            .await
            .unwrap();
        let props = std::fs::read_to_string(dir.path().join(".hoodie/hoodie.properties")).unwrap();
        assert!(props.contains("hoodie.table.version=8"));
        assert!(props.contains("hoodie.table.type=MERGE_ON_READ"));
        assert!(props.contains("hoodie.record.merge.mode=COMMIT_TIME_ORDERING"));
        assert!(props.contains("hoodie.datasource.write.hive_style_partitioning=false"));

        // Auto keys + MDT off + custom option passthrough + no meta fields.
        let dir = tempfile::tempdir().unwrap();
        let _ = Table::create(dir.path().to_str().unwrap())
            .with_table_name("nometa")
            .with_metadata(false)
            .with_record_index(false)
            .with_column_stats(false)
            .with_populates_meta_fields(false)
            .with_option("hoodie.parquet.max.file.size", "1048576")
            .create()
            .await
            .unwrap();
        let props = std::fs::read_to_string(dir.path().join(".hoodie/hoodie.properties")).unwrap();
        assert!(props.contains("hoodie.populate.meta.fields=false"));
        assert!(props.contains("hoodie.parquet.max.file.size=1048576"));
        assert!(!dir.path().join(".hoodie/metadata").exists());
    }
}
