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

use anyhow::{anyhow, Context, Result};
use paste::paste;
use std::collections::HashMap;
use std::env;
use std::hash::Hash;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use strum::IntoEnumIterator;

use crate::config::internal::HudiInternalConfig::SkipConfigValidation;
use crate::config::read::HudiReadConfig;
use crate::config::table::HudiTableConfig::{DropsPartitionFields, TableType, TableVersion};
use crate::config::table::TableTypeValue::CopyOnWrite;
use crate::config::table::{HudiTableConfig, TableTypeValue};
use crate::config::utils::{parse_data_for_options, split_hudi_options_from_others};
use crate::config::{HudiConfigs, HUDI_CONF_DIR};
use crate::storage::Storage;
use crate::table::fs_view::FileSystemView;
use crate::table::timeline::Timeline;
use crate::table::Table;

/// Builder for creating a [Table] instance.
#[derive(Debug, Clone)]
pub struct TableBuilder {
    base_uri: String,
    hudi_options: HashMap<String, String>,
    storage_options: HashMap<String, String>,
    options: HashMap<String, String>,
}

macro_rules! impl_with_options {
    ($struct_name:ident, $($field:ident),+) => {
        impl $struct_name {
            $(
                paste! {
                    /// Add options to the builder.
                    /// Subsequent calls overwrite the previous values if the key already exists.
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

impl_with_options!(TableBuilder, hudi_options, storage_options, options);

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

        let timeline = Timeline::new(hudi_configs.clone(), storage_options.clone())
            .await
            .context("Failed to load timeline")?;

        let file_system_view = FileSystemView::new(hudi_configs.clone(), storage_options.clone())
            .await
            .context("Failed to load file system view")?;

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
        Self::extend_if_absent(&mut self.storage_options, &generic_other_opts);

        // if any user-provided options are intended for cloud storage and in uppercase,
        // convert them to lowercase. This is to allow `object_store` to pick them up.
        // Note that we do not need to look up env vars for storage as `object_store` does that for us.
        Self::format_cloud_env_vars(&mut self.storage_options);

        // At this point, we have resolved the storage options needed for accessing the storage layer.
        // We can now resolve the hudi options
        Self::resolve_hudi_options(&self.storage_options, &mut self.hudi_options).await
    }

    fn format_cloud_env_vars(options: &mut HashMap<String, String>) {
        const PREFIXES: [&str; 3] = ["AWS_", "AZURE_", "GOOGLE_"];

        for (key, value) in env::vars() {
            if PREFIXES.iter().any(|prefix| key.starts_with(prefix))
                && !options.contains_key(&key.to_ascii_lowercase())
            {
                options.insert(key.to_ascii_lowercase(), value);
            }
        }
    }

    async fn resolve_hudi_options(
        storage_options: &HashMap<String, String>,
        hudi_options: &mut HashMap<String, String>,
    ) -> Result<()> {
        // create a [Storage] instance to load properties from storage layer.
        let storage = Storage::new(
            Arc::new(storage_options.clone()),
            Arc::new(HudiConfigs::new(hudi_options.iter())),
        )?;

        Self::imbue_table_properties(hudi_options, storage.clone()).await?;

        // TODO load Hudi configs from env vars here before loading global configs

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
        let table_type = hudi_configs.get(TableType)?.to::<String>();
        if TableTypeValue::from_str(&table_type)? != CopyOnWrite {
            return Err(anyhow!("Only support copy-on-write table."));
        }

        let table_version = hudi_configs.get(TableVersion)?.to::<isize>();
        if !(5..=6).contains(&table_version) {
            return Err(anyhow!("Only support table version 5 and 6."));
        }

        let drops_partition_cols = hudi_configs
            .get_or_default(DropsPartitionFields)
            .to::<bool>();
        if drops_partition_cols {
            return Err(anyhow!(
                "Only support when `{}` is disabled",
                DropsPartitionFields.as_ref()
            ));
        }

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

    #[test]
    fn test_build_from_explicit_options() {
        let hudi_options = [("hoodie.option1", "value1"), ("hoodie.option3", "value3")];
        let storage_options = [
            ("AWS_REGION", "us-east-1"),
            ("AWS_ENDPOINT", "s3.us-east-1.amazonaws.com"),
        ];
        let builder = TableBuilder::from_base_uri("/tmp/hudi_data")
            .with_hudi_options(hudi_options)
            .with_storage_options(storage_options);
        let hudi_options = &builder.hudi_options;
        let storage_options = &builder.storage_options;
        assert_eq!(hudi_options.len(), 2);
        assert_eq!(hudi_options["hoodie.option1"], "value1");
        assert_eq!(hudi_options["hoodie.option3"], "value3");
        assert_eq!(storage_options.len(), 2);
        assert_eq!(storage_options["AWS_REGION"], "us-east-1");
        assert_eq!(
            storage_options["AWS_ENDPOINT"],
            "s3.us-east-1.amazonaws.com"
        );
    }

    #[test]
    fn test_build_from_explicit_options_chained() {
        let builder = TableBuilder::from_base_uri("/tmp/hudi_data")
            .with_hudi_options([("hoodie.option1", "value1")])
            .with_hudi_options([("hoodie.option1", "value1-1")])
            .with_hudi_options([("hoodie.option3", "value3")])
            .with_storage_options([("AWS_REGION", "us-east-2")])
            .with_storage_options([("AWS_REGION", "us-east-1")])
            .with_storage_options([("AWS_ENDPOINT", "s3.us-east-1.amazonaws.com")]);
        let hudi_options = &builder.hudi_options.clone();
        let storage_options = &builder.storage_options.clone();
        assert_eq!(hudi_options.len(), 2);
        assert_eq!(hudi_options["hoodie.option1"], "value1-1");
        assert_eq!(hudi_options["hoodie.option3"], "value3");
        assert_eq!(storage_options.len(), 2);
        assert_eq!(storage_options["AWS_REGION"], "us-east-1");
        assert_eq!(
            storage_options["AWS_ENDPOINT"],
            "s3.us-east-1.amazonaws.com"
        );
    }
}
