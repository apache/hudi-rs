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

use paste::paste;
use std::collections::HashMap;
use std::env;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;
use strum::IntoEnumIterator;

use crate::config::internal::HudiInternalConfig::SkipConfigValidation;
use crate::config::read::HudiReadConfig;
use crate::config::table::HudiTableConfig;
use crate::config::table::HudiTableConfig::{
    DropsPartitionFields, TableVersion, TimelineLayoutVersion,
};
use crate::config::util::{parse_data_for_options, split_hudi_options_from_others};
use crate::config::{HudiConfigs, HUDI_CONF_DIR};
use crate::error::CoreError;
use crate::merge::record_merger::RecordMerger;
use crate::storage::Storage;
use crate::table::fs_view::FileSystemView;
use crate::table::Table;
use crate::timeline::Timeline;
use crate::Result;

/// Builder for creating a [Table] instance.
#[derive(Debug, Clone)]
pub struct TableBuilder {
    base_uri: String,
    hudi_options: HashMap<String, String>,
    storage_options: HashMap<String, String>,
    options: HashMap<String, String>,
}

macro_rules! impl_with_options {
    ($struct_name:ident, $($field:ident, $singular:ident),+) => {
        impl $struct_name {
            $(
                paste! {
                    #[doc = "Add " $singular " to the builder."]
                    #[doc = "Subsequent calls overwrite the previous values if the key already exists."]
                    pub fn [<with_ $singular>]<K, V>(mut self, k: K, v: V) -> Self
                    where
                        K: AsRef<str>,
                        V: Into<String>,
                    {
                        self.$field.insert(k.as_ref().to_string(), v.into());
                        self
                    }

                    #[doc = "Add " $field " to the builder."]
                    #[doc = "Subsequent calls overwrite the previous values if the key already exists."]
                    pub fn [<with_ $field>]<I, K, V>(mut self, options: I) -> Self
                    where
                        I: IntoIterator<Item = (K, V)>,
                        K: AsRef<str>,
                        V: Into<String>,
                    {
                        self.$field.extend(options.into_iter().map(|(k, v)| (k.as_ref().to_string(), v.into())));
                        self
                    }
                }
            )+
        }
    };
}

impl_with_options!(
    TableBuilder,
    hudi_options,
    hudi_option,
    storage_options,
    storage_option,
    options,
    option
);

impl TableBuilder {
    /// Create Hudi table builder from base table uri
    pub fn from_base_uri(base_uri: &str) -> Self {
        TableBuilder {
            base_uri: base_uri.to_string(),
            storage_options: HashMap::new(),
            hudi_options: HashMap::new(),
            options: HashMap::new(),
        }
    }

    pub async fn build(&mut self) -> Result<Table> {
        self.resolve_options().await?;

        let hudi_configs = HudiConfigs::new(self.hudi_options.iter());

        Self::validate_configs(&hudi_configs).expect("Hudi configs are not valid.");

        let hudi_configs = Arc::from(hudi_configs);
        let storage_options = Arc::from(self.storage_options.clone());

        let timeline =
            Timeline::new_from_storage(hudi_configs.clone(), storage_options.clone()).await?;

        let file_system_view =
            FileSystemView::new(hudi_configs.clone(), storage_options.clone()).await?;

        Ok(Table {
            hudi_configs,
            storage_options,
            timeline,
            file_system_view,
        })
    }

    /// Resolve all options by combining the ones from hoodie.properties, user-provided options,
    /// env vars, and global Hudi configs. The precedence order is as follows:
    ///
    /// 1. hoodie.properties
    /// 2. Explicit options provided by the user
    /// 3. Generic options provided by the user
    /// 4. Env vars
    /// 5. Global Hudi configs
    ///
    /// [note] Error may occur when 1 and 2 have conflicts.
    async fn resolve_options(&mut self) -> Result<()> {
        self.resolve_user_provided_options();

        // if any user-provided options are intended for cloud storage and in uppercase,
        // convert them to lowercase. This is to allow `object_store` to pick them up.
        self.resolve_cloud_env_vars();

        // At this point, we have resolved the storage options needed for accessing the storage layer.
        // We can now resolve the hudi options
        self.resolve_hudi_options().await
    }

    fn resolve_user_provided_options(&mut self) {
        // Insert the base path into hudi options since it is explicitly provided
        self.hudi_options.insert(
            HudiTableConfig::BasePath.as_ref().to_string(),
            self.base_uri.clone(),
        );

        let (generic_hudi_opts, generic_other_opts) =
            split_hudi_options_from_others(self.options.iter());

        // Combine generic options (lower precedence) with explicit options.
        // Note that we treat all non-Hudi options as storage options
        Self::extend_if_absent(&mut self.hudi_options, &generic_hudi_opts);
        Self::extend_if_absent(&mut self.storage_options, &generic_other_opts)
    }

    fn resolve_cloud_env_vars(&mut self) {
        for (key, value) in env::vars() {
            if Storage::CLOUD_STORAGE_PREFIXES
                .iter()
                .any(|prefix| key.starts_with(prefix))
                && !self.storage_options.contains_key(&key.to_ascii_lowercase())
            {
                self.storage_options.insert(key.to_ascii_lowercase(), value);
            }
        }
    }

    async fn resolve_hudi_options(&mut self) -> Result<()> {
        // create a [Storage] instance to load properties from storage layer.
        let storage = Storage::new(
            Arc::new(self.storage_options.clone()),
            Arc::new(HudiConfigs::new(self.hudi_options.iter())),
        )?;

        let hudi_options = &mut self.hudi_options;
        Self::imbue_table_properties(hudi_options, storage.clone()).await?;

        // TODO support imbuing Hudi options from env vars HOODIE_ENV.*
        // (see https://hudi.apache.org/docs/next/s3_hoodie)
        // before loading global configs

        Self::imbue_global_hudi_configs_if_absent(hudi_options, storage.clone()).await
    }

    async fn imbue_table_properties(
        options: &mut HashMap<String, String>,
        storage: Arc<Storage>,
    ) -> Result<()> {
        let bytes = storage.get_file_data(".hoodie/hoodie.properties").await?;
        let table_properties = parse_data_for_options(&bytes, "=")?;

        // We currently treat all table properties as the highest precedence, which is valid for most cases.
        // TODO: handle the case where the same key is present in both table properties and options
        for (k, v) in table_properties {
            options.insert(k.to_string(), v.to_string());
        }

        Ok(())
    }

    async fn imbue_global_hudi_configs_if_absent(
        options: &mut HashMap<String, String>,
        storage: Arc<Storage>,
    ) -> Result<()> {
        let global_config_path = env::var(HUDI_CONF_DIR)
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/etc/hudi/conf"))
            .join("hudi-defaults.conf");

        if let Ok(bytes) = storage
            .get_file_data_from_absolute_path(global_config_path.to_str().unwrap())
            .await
        {
            if let Ok(global_configs) = parse_data_for_options(&bytes, " \t=") {
                for (key, value) in global_configs {
                    if key.starts_with("hoodie.") && !options.contains_key(&key) {
                        options.insert(key.to_string(), value.to_string());
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_configs(hudi_configs: &HudiConfigs) -> Result<()> {
        if hudi_configs
            .get_or_default(SkipConfigValidation)
            .to::<bool>()
        {
            return Ok(());
        }

        for conf in HudiTableConfig::iter() {
            hudi_configs.validate(conf)?
        }

        for conf in HudiReadConfig::iter() {
            hudi_configs.validate(conf)?
        }

        // additional validation
        let table_version = hudi_configs.get(TableVersion)?.to::<isize>();
        if !(5..=6).contains(&table_version) {
            return Err(CoreError::Unsupported(
                "Only support table version 5 and 6.".to_string(),
            ));
        }

        let timeline_layout_version = hudi_configs.get(TimelineLayoutVersion)?.to::<isize>();
        if timeline_layout_version != 1 {
            return Err(CoreError::Unsupported(
                "Only support timeline layout version 1.".to_string(),
            ));
        }

        let drops_partition_cols = hudi_configs
            .get_or_default(DropsPartitionFields)
            .to::<bool>();
        if drops_partition_cols {
            return Err(CoreError::Unsupported(format!(
                "Only support when `{}` is disabled",
                DropsPartitionFields.as_ref()
            )));
        }

        RecordMerger::validate_configs(hudi_configs)?;

        Ok(())
    }

    fn extend_if_absent<K, V>(target: &mut HashMap<K, V>, source: &HashMap<K, V>)
    where
        K: Eq + Hash + Clone,
        V: Clone,
    {
        for (key, value) in source {
            target.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_table_builder() -> TableBuilder {
        TableBuilder {
            base_uri: "test_uri".to_string(),
            hudi_options: HashMap::new(),
            storage_options: HashMap::new(),
            options: HashMap::new(),
        }
    }

    #[test]
    fn test_with_hudi_option() {
        let builder = create_table_builder();

        let updated = builder.with_hudi_option("key", "value");
        assert_eq!(updated.hudi_options["key"], "value")
    }

    #[test]
    fn test_with_hudi_options() {
        let builder = create_table_builder();

        let options = [("key1", "value1"), ("key2", "value2")];
        let updated = builder.with_hudi_options(options);
        assert_eq!(updated.hudi_options["key1"], "value1");
        assert_eq!(updated.hudi_options["key2"], "value2")
    }

    #[test]
    fn test_with_storage_option() {
        let builder = create_table_builder();

        let updated = builder.with_storage_option("key", "value");
        assert_eq!(updated.storage_options["key"], "value")
    }

    #[test]
    fn test_with_storage_options() {
        let builder = create_table_builder();

        let options = [("key1", "value1"), ("key2", "value2")];
        let updated = builder.with_storage_options(options);
        assert_eq!(updated.storage_options["key1"], "value1");
        assert_eq!(updated.storage_options["key2"], "value2");
    }

    #[test]
    fn test_with_option() {
        let builder = create_table_builder();

        let updated = builder.with_option("key", "value");
        assert_eq!(updated.options["key"], "value")
    }

    #[test]
    fn test_with_options() {
        let builder = create_table_builder();

        let options = [("key1", "value1"), ("key2", "value2")];
        let updated = builder.with_options(options);
        assert_eq!(updated.options["key1"], "value1");
        assert_eq!(updated.options["key2"], "value2")
    }

    #[test]
    fn test_builder_resolve_user_provided_options_should_apply_precedence_order() {
        let mut builder = TableBuilder::from_base_uri("/tmp/hudi_data")
            .with_hudi_option("hoodie.option1", "value1")
            .with_option("hoodie.option2", "'value2")
            .with_hudi_options([
                ("hoodie.option1", "value1-1"),
                ("hoodie.option3", "value3"),
                ("hoodie.option1", "value1-2"),
            ])
            .with_storage_option("AWS_REGION", "us-east-2")
            .with_storage_options([
                ("AWS_REGION", "us-east-1"),
                ("AWS_ENDPOINT", "s3.us-east-1.amazonaws.com"),
            ])
            .with_option("AWS_REGION", "us-west-1")
            .with_options([
                ("hoodie.option3", "value3-1"),
                ("hoodie.option2", "value2-1"),
            ]);

        builder.resolve_user_provided_options();

        assert_eq!(builder.hudi_options.len(), 4);
        assert_eq!(builder.hudi_options["hoodie.base.path"], "/tmp/hudi_data");
        assert_eq!(builder.hudi_options["hoodie.option1"], "value1-2");
        assert_eq!(builder.hudi_options["hoodie.option2"], "value2-1");
        assert_eq!(builder.hudi_options["hoodie.option3"], "value3");
        assert_eq!(builder.storage_options.len(), 2);
        assert_eq!(builder.storage_options["AWS_REGION"], "us-east-1");
        assert_eq!(
            builder.storage_options["AWS_ENDPOINT"],
            "s3.us-east-1.amazonaws.com"
        );
    }
}
