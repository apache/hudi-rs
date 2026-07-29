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
use std::path::PathBuf;
use std::sync::Arc;

use crate::Result;
use crate::config::read::{HudiReadConfig, ReadConfigScope};
use crate::config::table::HudiTableConfig;
use crate::config::util::{parse_data_for_options, split_hudi_options_from_others};
use crate::config::{HUDI_CONF_DIR, HudiConfigs};
use crate::storage::Storage;
use crate::table::Table;
use crate::table::fs_view::FileSystemView;
use crate::table::validation::validate_configs;
use crate::timeline::Timeline;
use crate::util::collection::extend_if_absent;

/// Builder for creating a [Table] instance.
#[derive(Debug, Clone)]
pub struct TableBuilder {
    option_resolver: OptionResolver,
}

/// Resolver for options including Hudi options, storage options, and generic options.
#[derive(Debug, Clone)]
pub struct OptionResolver {
    pub base_uri: String,
    pub hudi_options: HashMap<String, String>,
    pub storage_options: HashMap<String, String>,
    pub options: HashMap<String, String>,
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
                        let option_resolver = &mut self.option_resolver;
                        option_resolver.$field.insert(k.as_ref().to_string(), v.into());
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
                        let option_resolver = &mut self.option_resolver;
                        option_resolver.$field.extend(options.into_iter().map(|(k, v)| (k.as_ref().to_string(), v.into())));
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
        let option_resolver = OptionResolver::new(base_uri);
        TableBuilder { option_resolver }
    }

    pub async fn build(&mut self) -> Result<Table> {
        let option_resolver = &mut self.option_resolver;

        option_resolver.resolve_options().await?;

        // A read config is dropped from the table's own configs only when it
        // selects WHICH read to perform — those are meaningful per call, and one
        // baked at table level would silently redirect every later read. The rest
        // describe HOW to read (which file group reader version, read-optimized,
        // batch size) and are exactly the kind of thing an operator sets once, in
        // `hoodie.properties` or `hudi-defaults.conf`; keeping them makes a
        // table-level setting the default that `ReadOptions` then overrides.
        //
        // Dropping them all — which a `hoodie.read.` prefix test did — meant
        // `hoodie.read.file.group.reader.version` never reached the reader, so
        // the strict version parse it exists to protect could not fire and a typo
        // read with the other version after all.
        let hudi_configs = Arc::from(HudiConfigs::new(
            option_resolver
                .hudi_options
                .iter()
                .filter(|(key, _)| Self::keep_at_table_level(key)),
        ));

        let storage_options = Arc::from(self.option_resolver.storage_options.clone());

        let timeline =
            Timeline::new_from_storage(hudi_configs.clone(), storage_options.clone()).await?;

        let file_system_view =
            FileSystemView::new(hudi_configs.clone(), storage_options.clone()).await?;

        Ok(Table {
            hudi_configs,
            storage_options,
            timeline,
            file_system_view,
            cached_estimator: std::sync::Arc::new(tokio::sync::OnceCell::new()),
        })
    }

    /// Whether `key` may live on the table's own configs.
    ///
    /// Anything that is not a read config is kept. A read config is kept when its
    /// [`ReadConfigScope`] says it describes how to read; a per-read-only one is
    /// dropped with a warning, because it was almost certainly meant for
    /// [`ReadOptions`](crate::table::ReadOptions) and would otherwise be ignored
    /// in silence.
    ///
    /// Dropping rather than erroring: `hudi_options` is a merge of the user's
    /// options, `hoodie.properties`, and `hudi-defaults.conf`, so an error here
    /// would make a table unopenable because of a file the caller may not
    /// control.
    fn keep_at_table_level(key: &str) -> bool {
        match HudiReadConfig::scope_of_key(key) {
            None | Some(ReadConfigScope::TableOrRead) => true,
            Some(ReadConfigScope::ReadOnly) => {
                log::warn!(
                    "ignoring '{key}' set at table level: it selects which read to perform, so \
                     it is only meaningful per read — pass it through ReadOptions instead"
                );
                false
            }
        }
    }
}

impl OptionResolver {
    /// Create a new [OptionResolver] with the given base URI.
    pub fn new(base_uri: &str) -> Self {
        Self {
            base_uri: base_uri.to_string(),
            hudi_options: HashMap::new(),
            storage_options: HashMap::new(),
            options: HashMap::new(),
        }
    }

    /// Create a new [OptionResolver] with the given base URI and options.
    pub fn new_with_options<I, K, V>(base_uri: &str, options: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let options = options
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.into()))
            .collect();
        Self {
            base_uri: base_uri.to_string(),
            hudi_options: HashMap::new(),
            storage_options: HashMap::new(),
            options,
        }
    }

    /// Resolve all options by combining the ones from hoodie.properties, user-provided options,
    /// env vars, and global Hudi configs. The precedence order is as follows:
    ///
    /// 1. hoodie.properties
    /// 2. User-provided options
    ///    - Explicit Hudi options provided by the user
    ///    - Generic options provided by the user
    /// 3. Env vars for storage options
    ///    - With HOODIE_ENV_ prefix
    ///    - Cloud storage options specified via env vars
    /// 4. Global Hudi configs
    ///
    /// [note] Error may occur when 1 and 2 have conflicts.
    pub async fn resolve_options(&mut self) -> Result<()> {
        self.resolve_user_provided_options();

        // If any user-provided options are intended for cloud storage and in uppercase,
        // convert them to lowercase. This is to allow `object_store` to pick them up.
        self.resolve_env_vars();

        // At this point, we have resolved the storage options needed for accessing the storage layer.
        // We can now resolve the hudi options
        self.resolve_hudi_options().await?;

        // Validate the resolved Hudi options
        let hudi_configs = HudiConfigs::new(self.hudi_options.iter());
        validate_configs(&hudi_configs)
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
        extend_if_absent(&mut self.hudi_options, &generic_hudi_opts);
        extend_if_absent(&mut self.storage_options, &generic_other_opts)
    }

    /// Resolve env vars for keys starting with `HOODIE_ENV_` or cloud storage options.
    ///
    /// For example: `HOODIE_ENV_fs_DOT_s3a_DOT_access_DOT_key` will be converted to `fs.s3a.access.key`.
    ///
    /// Also supports standard cloud storage env vars like `AWS_ACCESS_KEY_ID`, `GOOGLE_APPLICATION_CREDENTIALS`, `AZURE_STORAGE_ACCOUNT_KEY`, etc.
    ///
    /// [note] All keys will be converted to lowercase.
    fn resolve_env_vars(&mut self) {
        for (env_key, env_value) in std::env::vars() {
            let lower_option_key = if let Some(stripped) = env_key.strip_prefix("HOODIE_ENV_") {
                Some(stripped.replace("_DOT_", ".").to_ascii_lowercase())
            } else if Storage::CLOUD_STORAGE_PREFIXES
                .iter()
                .any(|prefix| env_key.starts_with(prefix))
            {
                Some(env_key.to_ascii_lowercase())
            } else {
                None
            };
            if let Some(key) = lower_option_key {
                self.storage_options.entry(key).or_insert(env_value);
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

        // Table properties on storage (hoodie.properties) should have the highest precedence,
        // except for writer-changeable properties like enabling metadata table/indexes.
        // TODO: return err when user-provided options conflict with table properties
        for (k, v) in table_properties {
            options.insert(k.to_string(), v.to_string());
        }

        Ok(())
    }

    async fn imbue_global_hudi_configs_if_absent(
        options: &mut HashMap<String, String>,
        storage: Arc<Storage>,
    ) -> Result<()> {
        let global_config_path = std::env::var(HUDI_CONF_DIR)
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/etc/hudi/conf"))
            .join("hudi-defaults.conf");

        if let Ok(bytes) = storage
            .get_file_data_from_absolute_path(global_config_path.to_str().unwrap())
            .await
            && let Ok(global_configs) = parse_data_for_options(&bytes, " \t=")
        {
            for (key, value) in global_configs {
                if key.starts_with("hoodie.") && !options.contains_key(&key) {
                    options.insert(key.to_string(), value.to_string());
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn create_table_builder() -> TableBuilder {
        let option_resolver = OptionResolver::new("test_uri");
        TableBuilder { option_resolver }
    }

    #[test]
    fn test_with_hudi_option() {
        let builder = create_table_builder();

        let updated = builder.with_hudi_option("key", "value").option_resolver;
        assert_eq!(updated.hudi_options["key"], "value")
    }

    #[test]
    fn test_with_hudi_options() {
        let builder = create_table_builder();

        let options = [("key1", "value1"), ("key2", "value2")];
        let updated = builder.with_hudi_options(options).option_resolver;
        assert_eq!(updated.hudi_options["key1"], "value1");
        assert_eq!(updated.hudi_options["key2"], "value2")
    }

    #[test]
    fn test_with_storage_option() {
        let builder = create_table_builder();

        let updated = builder.with_storage_option("key", "value").option_resolver;
        assert_eq!(updated.storage_options["key"], "value")
    }

    #[test]
    fn test_with_storage_options() {
        let builder = create_table_builder();

        let options = [("key1", "value1"), ("key2", "value2")];
        let updated = builder.with_storage_options(options).option_resolver;
        assert_eq!(updated.storage_options["key1"], "value1");
        assert_eq!(updated.storage_options["key2"], "value2");
    }

    #[test]
    fn test_with_option() {
        let builder = create_table_builder();

        let updated = builder.with_option("key", "value").option_resolver;
        assert_eq!(updated.options["key"], "value")
    }

    #[test]
    fn test_with_options() {
        let builder = create_table_builder();

        let options = [("key1", "value1"), ("key2", "value2")];
        let updated = builder.with_options(options).option_resolver;
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

        let resolver = &mut builder.option_resolver;
        resolver.resolve_user_provided_options();

        assert_eq!(resolver.hudi_options.len(), 4);
        assert_eq!(resolver.hudi_options["hoodie.base.path"], "/tmp/hudi_data");
        assert_eq!(resolver.hudi_options["hoodie.option1"], "value1-2");
        assert_eq!(resolver.hudi_options["hoodie.option2"], "value2-1");
        assert_eq!(resolver.hudi_options["hoodie.option3"], "value3");
        assert_eq!(resolver.storage_options.len(), 2);
        assert_eq!(resolver.storage_options["AWS_REGION"], "us-east-1");
        assert_eq!(
            resolver.storage_options["AWS_ENDPOINT"],
            "s3.us-east-1.amazonaws.com"
        );
    }

    #[test]
    #[serial(env_vars)]
    fn test_resolve_cloud_env_vars_with_hudi_style() {
        unsafe {
            std::env::remove_var("HOODIE_ENV_fs_DOT_s3a_DOT_access_DOT_key");
            std::env::remove_var("HOODIE_ENV_fs_DOT_s3a_DOT_secret_DOT_key");

            std::env::set_var(
                "HOODIE_ENV_fs_DOT_s3a_DOT_access_DOT_key",
                "test_access_key",
            );
            std::env::set_var(
                "HOODIE_ENV_fs_DOT_s3a_DOT_secret_DOT_key",
                "test_secret_key",
            );
        }

        let mut resolver = OptionResolver::new("test_uri");
        resolver.resolve_env_vars();

        assert_eq!(
            resolver.storage_options.get("fs.s3a.access.key"),
            Some(&"test_access_key".to_string())
        );
        assert_eq!(
            resolver.storage_options.get("fs.s3a.secret.key"),
            Some(&"test_secret_key".to_string())
        );

        unsafe {
            std::env::remove_var("HOODIE_ENV_fs_DOT_s3a_DOT_access_DOT_key");
            std::env::remove_var("HOODIE_ENV_fs_DOT_s3a_DOT_secret_DOT_key");
        }
    }

    #[test]
    #[serial(env_vars)]
    fn test_resolve_cloud_env_vars_precedence() {
        unsafe {
            std::env::remove_var("HOODIE_ENV_fs_DOT_s3a_DOT_access_DOT_key");
            std::env::remove_var("AWS_ACCESS_KEY_ID");

            // Test that manually set storage options take precedence over env vars
            std::env::set_var("HOODIE_ENV_fs_DOT_s3a_DOT_access_DOT_key", "env_access_key");
            std::env::set_var("AWS_ACCESS_KEY_ID", "standard_access_key");
        }

        let mut resolver = OptionResolver::new("test_uri");
        resolver.storage_options.insert(
            "fs.s3a.access.key".to_string(),
            "manual_access_key".to_string(),
        );
        resolver.resolve_env_vars();

        assert_eq!(
            resolver.storage_options.get("fs.s3a.access.key"),
            Some(&"manual_access_key".to_string())
        );

        unsafe {
            std::env::remove_var("HOODIE_ENV_fs_DOT_s3a_DOT_access_DOT_key");
            std::env::remove_var("AWS_ACCESS_KEY_ID");
        }
    }

    /// Regression test: a read config describing HOW to read must survive onto the
    /// table's own configs; only one selecting WHICH read is dropped.
    ///
    /// Everything under `hoodie.read.` used to be filtered out, so
    /// `hoodie.read.file.group.reader.version` set on the table, in
    /// `hoodie.properties`, or in `hudi-defaults.conf` never reached the file
    /// group reader — and because it never reached the reader, the strict parse
    /// guarding against a typoed version could not fire either.
    #[test]
    fn test_keep_at_table_level_keeps_how_to_read_and_drops_which_read() {
        for config in [
            HudiReadConfig::FileGroupReaderVersion,
            HudiReadConfig::UseReadOptimizedMode,
            HudiReadConfig::MergeUseRecordPositions,
            HudiReadConfig::StreamBatchSize,
            HudiReadConfig::InputPartitions,
            HudiReadConfig::FileSliceReadConcurrency,
        ] {
            assert!(
                TableBuilder::keep_at_table_level(config.as_ref()),
                "{config} describes how to read and must reach the reader"
            );
        }

        for config in [
            HudiReadConfig::QueryType,
            HudiReadConfig::AsOfTimestamp,
            HudiReadConfig::StartTimestamp,
            HudiReadConfig::EndTimestamp,
        ] {
            assert!(
                !TableBuilder::keep_at_table_level(config.as_ref()),
                "{config} selects which read to perform and must not be baked onto the table"
            );
        }

        // Non-read configs are untouched.
        assert!(TableBuilder::keep_at_table_level(
            HudiTableConfig::TableType.as_ref()
        ));
        assert!(TableBuilder::keep_at_table_level(
            "hoodie.memory.merge.max.size"
        ));
    }

    /// Every read config must state a scope, so a new one cannot silently
    /// inherit whatever a prefix rule would have done with its key.
    #[test]
    fn test_every_read_config_declares_a_scope() {
        use strum::IntoEnumIterator;
        for config in HudiReadConfig::iter() {
            assert!(
                HudiReadConfig::scope_of_key(config.as_ref()).is_some(),
                "{config} must be reachable by key lookup for the table-level filter to see it"
            );
        }
    }
}
