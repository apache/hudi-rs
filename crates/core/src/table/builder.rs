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

//! [`TableBuilder`] opens a [`Table`] with options.

use paste::paste;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use crate::Result;
use crate::config::error::ConfigError;
use crate::config::properties::parse_properties;
use crate::config::read::{HudiReadConfig, ReadConfigScope};
use crate::config::table::HudiTableConfig;
use crate::config::util::{parse_data_for_options, split_hudi_options_from_others};
use crate::config::{ConfigParser, HUDI_CONF_DIR, HudiConfigs};
use crate::storage::Storage;
use crate::table::Table;
use crate::table::fs_view::FileSystemView;
use crate::table::validation::validate_configs;
use crate::timeline::Timeline;
use crate::util::collection::extend_if_absent;

/// Builder that opens an existing [`Table`] with options.
///
/// `with_hudi_option(s)` set Hudi configs, `with_storage_option(s)` set storage options, and
/// `with_option(s)` take either, routing keys that start with `hoodie.` to Hudi configs.
/// [`OptionResolver::resolve_options`] describes how these combine with the table's own configs.
#[derive(Debug, Clone)]
pub struct TableBuilder {
    option_resolver: OptionResolver,
}

/// Controls whether user-provided options are checked against persisted table properties.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConflictCheck {
    /// Reject user options that conflict with values in `hoodie.properties`.
    Enforce,
    /// Skip conflict validation; persisted properties take precedence.
    Skip,
}

/// Resolver for options including Hudi options, storage options, and generic options.
#[derive(Debug, Clone)]
pub struct OptionResolver {
    /// The table's base URI.
    pub base_uri: String,
    /// Hudi configs (`hoodie.*`).
    pub hudi_options: HashMap<String, String>,
    /// Storage options, such as cloud credentials.
    pub storage_options: HashMap<String, String>,
    /// Options not yet split into Hudi configs and storage options.
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

    /// Resolves the options, validates the table configs, and loads the timeline.
    ///
    /// Read configs that select which read to perform (query type and timestamps) are dropped
    /// with a warning; pass them in [`ReadOptions`](crate::table::ReadOptions) instead.
    ///
    /// # Errors
    ///
    /// Fails, among other cases, when `.hoodie/hoodie.properties` cannot be read, or the table
    /// configs are invalid or unsupported (for example a table version other than 6, 8, or 9),
    /// or when a user-provided option conflicts with a persisted table property.
    ///
    /// Options are resolved on a copy of this builder's state, so the builder itself is left
    /// unchanged and may be reused; each call re-reads `hoodie.properties`.
    pub async fn build(&mut self) -> Result<Table> {
        let mut option_resolver = self.option_resolver.clone();
        Self::build_with_resolver(&mut option_resolver, ConflictCheck::Enforce).await
    }

    /// Reopens a table from resolved options for internal background work.
    ///
    /// Skips checking the supplied options against `hoodie.properties`: they were resolved
    /// when this table was opened, so a property changed since then must not fail the reopen.
    pub(crate) async fn build_with_resolved_options(&mut self) -> Result<Table> {
        let mut option_resolver = self.option_resolver.clone();
        Self::build_with_resolver(&mut option_resolver, ConflictCheck::Skip).await
    }

    async fn build_with_resolver(
        option_resolver: &mut OptionResolver,
        conflict_check: ConflictCheck,
    ) -> Result<Table> {
        option_resolver
            .resolve_options_with_conflict_check(conflict_check)
            .await?;

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

        let storage_options = Arc::from(option_resolver.storage_options.clone());

        let timeline =
            Timeline::new_from_storage(hudi_configs.clone(), storage_options.clone()).await?;

        let file_system_view =
            FileSystemView::new(hudi_configs.clone(), storage_options.clone()).await?;

        Ok(Table {
            hudi_configs,
            storage_options,
            timeline,
            file_system_view,
            cached_metadata_table: std::sync::Arc::new(tokio::sync::OnceCell::new()),
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

/// Add storage options from env vars for keys starting with `HOODIE_ENV_` or cloud storage
/// prefixes, without overriding options already present.
///
/// For example: `HOODIE_ENV_fs_DOT_s3a_DOT_access_DOT_key` will be converted to `fs.s3a.access.key`.
///
/// Also supports standard cloud storage env vars like `AWS_ACCESS_KEY_ID`, `GOOGLE_APPLICATION_CREDENTIALS`, `AZURE_STORAGE_ACCOUNT_KEY`, etc.
///
/// [note] All keys will be converted to lowercase.
pub(crate) fn resolve_storage_env_vars(storage_options: &mut HashMap<String, String>) {
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
            storage_options.entry(key).or_insert(env_value);
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
    /// Note: an error may occur when 1 and 2 have conflicts.
    pub async fn resolve_options(&mut self) -> Result<()> {
        self.resolve_options_with_conflict_check(ConflictCheck::Enforce)
            .await
    }

    async fn resolve_options_with_conflict_check(
        &mut self,
        conflict_check: ConflictCheck,
    ) -> Result<()> {
        self.resolve_user_provided_options();

        // If any user-provided options are intended for cloud storage and in uppercase,
        // convert them to lowercase. This is to allow `object_store` to pick them up.
        self.resolve_env_vars();

        // At this point, we have resolved the storage options needed for accessing the storage layer.
        // We can now resolve the hudi options
        self.resolve_hudi_options(conflict_check).await?;

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

    fn resolve_env_vars(&mut self) {
        resolve_storage_env_vars(&mut self.storage_options);
    }

    async fn resolve_hudi_options(&mut self, conflict_check: ConflictCheck) -> Result<()> {
        // create a [Storage] instance to load properties from storage layer.
        let storage = Storage::new(
            Arc::new(self.storage_options.clone()),
            Arc::new(HudiConfigs::new(self.hudi_options.iter())),
        )?;

        let hudi_options = &mut self.hudi_options;
        Self::imbue_table_properties(hudi_options, storage.clone(), conflict_check).await?;

        // TODO support imbuing Hudi options from env vars HOODIE_ENV.*
        // (see https://hudi.apache.org/docs/next/s3_hoodie)
        // before loading global configs

        Self::imbue_global_hudi_configs_if_absent(hudi_options, storage.clone()).await
    }

    async fn imbue_table_properties(
        options: &mut HashMap<String, String>,
        storage: Arc<Storage>,
        conflict_check: ConflictCheck,
    ) -> Result<()> {
        let bytes = storage.get_file_data(".hoodie/hoodie.properties").await?;
        let table_properties = parse_properties(&bytes)?;
        Self::merge_table_properties(options, table_properties, conflict_check)
    }

    fn merge_table_properties(
        options: &mut HashMap<String, String>,
        table_properties: HashMap<String, String>,
        conflict_check: ConflictCheck,
    ) -> Result<()> {
        if conflict_check == ConflictCheck::Enforce {
            Self::validate_no_conflicts_with_table_properties(options, &table_properties)?;
        }

        // Overlay persisted values on user options; writer-managed metadata may differ.
        options.extend(table_properties);

        Ok(())
    }

    /// Persisted metadata table keys that `HudiTableConfig` does not model, such as
    /// `hoodie.table.metadata.partitions.inflight`. Writers update them while building metadata
    /// partitions, so a differing user value is not a conflict. The modeled writer-changeable
    /// configs are exempted separately in [`Self::is_exempt`].
    fn is_writer_managed_key(key: &str) -> bool {
        key.starts_with("hoodie.table.metadata.")
    }

    /// The key and value under which `config` is persisted: the primary key if present, else
    /// the first alias present, mirroring [`ConfigParser::resolve_raw_value`].
    fn persisted_entry<'a, 'b>(
        config: &'a HudiTableConfig,
        table_properties: &'b HashMap<String, String>,
    ) -> Option<(&'a str, &'b str)> {
        std::iter::once(config.as_ref())
            .chain(config.aliases().iter().map(|alias| alias.key))
            .find_map(|key| table_properties.get(key).map(|value| (key, value.as_str())))
    }

    /// Configs whose user value may legitimately differ from the persisted one.
    ///
    /// - `BasePath`, `Checksum` and `CreateSchema` are supplied by the builder or derived.
    /// - `MetadataTableEnabled` and `MetadataTablePartitions` are changed by writers.
    /// - `DatabaseName` is exempt only while the persisted value is empty (i.e. unset).
    fn is_exempt(config: &HudiTableConfig, table_raw: &str) -> bool {
        match config {
            HudiTableConfig::BasePath
            | HudiTableConfig::Checksum
            | HudiTableConfig::CreateSchema
            | HudiTableConfig::MetadataTableEnabled
            | HudiTableConfig::MetadataTablePartitions => true,
            HudiTableConfig::DatabaseName => table_raw.trim().is_empty(),
            _ => false,
        }
    }

    /// Collect conflicts for persisted properties, including aliases and untyped keys.
    fn validate_no_conflicts_with_table_properties(
        user_options: &HashMap<String, String>,
        table_properties: &HashMap<String, String>,
    ) -> Result<()> {
        use strum::IntoEnumIterator;
        let configs: Vec<HudiTableConfig> = HudiTableConfig::iter().collect();
        let mut conflicts = Vec::new();

        // Modeled configs: compare parsed values, falling back to a trimmed,
        // case-insensitive comparison of the raw strings when either side fails to parse.
        for config in &configs {
            let Some((table_key, table_raw)) = Self::persisted_entry(config, table_properties)
            else {
                continue;
            };
            if Self::is_exempt(config, table_raw) {
                continue;
            }

            // The only error is `NotFound`: the user did not set this config.
            let Ok(user_raw) = config.resolve_raw_value(user_options) else {
                continue;
            };
            let same = match (
                config.parse_value(user_options),
                config.parse_value(table_properties),
            ) {
                (Ok(user), Ok(table)) => user == table,
                _ => user_raw.trim().eq_ignore_ascii_case(table_raw.trim()),
            };
            if !same {
                conflicts.push((
                    table_key.to_string(),
                    user_raw.to_string(),
                    table_raw.to_string(),
                ));
            }
        }

        // Keys that `HudiTableConfig` does not model: compare the raw strings.
        let modeled_keys: HashSet<&str> = configs
            .iter()
            .flat_map(|config| {
                std::iter::once(config.as_ref()).chain(config.aliases().iter().map(|a| a.key))
            })
            .collect();
        for (key, table_raw) in table_properties {
            if modeled_keys.contains(key.as_str()) || Self::is_writer_managed_key(key) {
                continue;
            }
            if let Some(user_raw) = user_options.get(key)
                && user_raw != table_raw
            {
                conflicts.push((key.clone(), user_raw.clone(), table_raw.clone()));
            }
        }

        if !conflicts.is_empty() {
            conflicts.sort();
            let details = conflicts
                .into_iter()
                .map(|(key, user, persisted)| {
                    format!("{key}: user '{user}', persisted '{persisted}'")
                })
                .collect::<Vec<_>>()
                .join("; ");
            return Err(ConfigError::ConflictingTableProperties(details).into());
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
    use hudi_test::SampleTable;
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

    fn map(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_conflict_with_table_property_is_error() {
        let table = map(&[("hoodie.table.type", "COPY_ON_WRITE")]);
        let user = map(&[("hoodie.table.type", "MERGE_ON_READ")]);

        let err =
            OptionResolver::validate_no_conflicts_with_table_properties(&user, &table).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("hoodie.table.type"), "{msg}");
        assert!(msg.contains("MERGE_ON_READ"), "{msg}");
        assert!(msg.contains("COPY_ON_WRITE"), "{msg}");
    }

    #[test]
    fn test_equivalent_spellings_are_not_conflicts() {
        let table = map(&[("hoodie.populate.meta.fields", "true")]);
        let user = map(&[("hoodie.populate.meta.fields", "TRUE")]);
        assert!(OptionResolver::validate_no_conflicts_with_table_properties(&user, &table).is_ok());
    }

    #[test]
    fn test_unparseable_value_that_differs_is_a_conflict() {
        // "maybe" does not parse as a boolean, so the raw-string fallback decides.
        let table = map(&[("hoodie.populate.meta.fields", "true")]);
        let user = map(&[("hoodie.populate.meta.fields", "maybe")]);

        let msg = OptionResolver::validate_no_conflicts_with_table_properties(&user, &table)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("hoodie.populate.meta.fields"), "{msg}");
    }

    #[test]
    fn test_string_configs_are_compared_exactly() {
        let key = "hoodie.table.keygenerator.class";
        let table = map(&[(key, "org.apache.hudi.keygen.SimpleKeyGenerator")]);

        for variant in [
            "org.apache.hudi.keygen.simplekeygenerator",
            " org.apache.hudi.keygen.SimpleKeyGenerator",
        ] {
            let user = map(&[(key, variant)]);
            assert!(
                OptionResolver::validate_no_conflicts_with_table_properties(&user, &table).is_err(),
                "{variant:?} should conflict"
            );
        }
    }

    #[test]
    fn test_conflict_is_detected_through_alias() {
        let alias = HudiTableConfig::OrderingFields.aliases()[0].key;
        let table = map(&[(alias, "ts")]);

        let same = map(&[(HudiTableConfig::OrderingFields.as_ref(), "ts")]);
        assert!(OptionResolver::validate_no_conflicts_with_table_properties(&same, &table).is_ok());

        let different = map(&[(HudiTableConfig::OrderingFields.as_ref(), "other_ts")]);
        assert!(
            OptionResolver::validate_no_conflicts_with_table_properties(&different, &table)
                .is_err()
        );
    }

    #[test]
    fn test_untyped_table_property_conflict_is_detected() {
        let table = map(&[("hoodie.record.merge.mode", "EVENT_TIME_ORDERING")]);
        let user = map(&[("hoodie.record.merge.mode", "COMMIT_TIME_ORDERING")]);
        let err =
            OptionResolver::validate_no_conflicts_with_table_properties(&user, &table).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("hoodie.record.merge.mode"), "{msg}");
        assert!(msg.contains("EVENT_TIME_ORDERING"), "{msg}");
        assert!(msg.contains("COMMIT_TIME_ORDERING"), "{msg}");
    }

    #[test]
    fn test_all_conflicts_are_reported_in_sorted_order() {
        let table = map(&[
            ("hoodie.table.type", "COPY_ON_WRITE"),
            ("hoodie.record.merge.mode", "EVENT_TIME_ORDERING"),
        ]);
        let user = map(&[
            ("hoodie.table.type", "MERGE_ON_READ"),
            ("hoodie.record.merge.mode", "COMMIT_TIME_ORDERING"),
        ]);
        let msg = OptionResolver::validate_no_conflicts_with_table_properties(&user, &table)
            .unwrap_err()
            .to_string();
        assert!(msg.find("hoodie.record.merge.mode") < msg.find("hoodie.table.type"));
    }

    #[test]
    fn test_keys_set_on_one_side_only_do_not_conflict() {
        // `hoodie.table.type` is missing from the user options, and `hoodie.table.name` from
        // the table properties.
        let table = map(&[("hoodie.table.type", "COPY_ON_WRITE")]);
        let user = map(&[("hoodie.table.name", "t")]);
        assert!(OptionResolver::validate_no_conflicts_with_table_properties(&user, &table).is_ok());
    }

    #[test]
    fn test_table_properties_take_precedence_over_exempt_user_options() {
        let mut options = map(&[
            ("hoodie.table.metadata.partitions", "files,record_index"),
            ("hoodie.table.type", "COPY_ON_WRITE"),
        ]);
        let table_properties = map(&[
            ("hoodie.table.metadata.partitions", "files"),
            ("hoodie.table.type", "COPY_ON_WRITE"),
        ]);

        OptionResolver::merge_table_properties(
            &mut options,
            table_properties,
            ConflictCheck::Enforce,
        )
        .unwrap();

        assert_eq!(options["hoodie.table.metadata.partitions"], "files");
    }

    #[test]
    fn test_merge_leaves_options_untouched_on_conflict() {
        let mut options = map(&[("hoodie.table.type", "MERGE_ON_READ")]);
        let table_properties = map(&[("hoodie.table.type", "COPY_ON_WRITE")]);

        assert!(
            OptionResolver::merge_table_properties(
                &mut options,
                table_properties,
                ConflictCheck::Enforce
            )
            .is_err()
        );
        assert_eq!(options["hoodie.table.type"], "MERGE_ON_READ");
    }

    #[test]
    fn test_merge_skip_lets_table_properties_win_without_error() {
        let mut options = map(&[("hoodie.table.type", "MERGE_ON_READ")]);
        let table_properties = map(&[("hoodie.table.type", "COPY_ON_WRITE")]);

        OptionResolver::merge_table_properties(&mut options, table_properties, ConflictCheck::Skip)
            .unwrap();

        assert_eq!(options["hoodie.table.type"], "COPY_ON_WRITE");
    }

    #[test]
    fn test_metadata_and_empty_database_name_are_exempt() {
        let table = map(&[
            ("hoodie.metadata.enable", "false"),
            ("hoodie.table.metadata.partitions", "files"),
            ("hoodie.database.name", ""),
        ]);
        let user = map(&[
            ("hoodie.metadata.enable", "true"),
            ("hoodie.table.metadata.partitions", "files,record_index"),
            ("hoodie.database.name", "analytics"),
        ]);
        assert!(OptionResolver::validate_no_conflicts_with_table_properties(&user, &table).is_ok());
    }

    #[test]
    fn test_non_empty_database_name_conflict_is_detected() {
        let table = map(&[("hoodie.database.name", "sales")]);
        let user = map(&[("hoodie.database.name", "analytics")]);
        let msg = OptionResolver::validate_no_conflicts_with_table_properties(&user, &table)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("hoodie.database.name"), "{msg}");
    }

    #[test]
    fn test_unmodeled_writer_managed_keys_are_exempt() {
        let table = map(&[("hoodie.table.metadata.partitions.inflight", "")]);
        let user = map(&[("hoodie.table.metadata.partitions.inflight", "column_stats")]);
        assert!(OptionResolver::validate_no_conflicts_with_table_properties(&user, &table).is_ok());
    }

    #[test]
    fn test_user_alias_against_persisted_primary_key() {
        let primary = HudiTableConfig::OrderingFields.as_ref();
        let alias = HudiTableConfig::OrderingFields.aliases()[0].key;

        let table = map(&[(primary, "ts")]);
        assert!(
            OptionResolver::validate_no_conflicts_with_table_properties(
                &map(&[(alias, "ts")]),
                &table
            )
            .is_ok()
        );
        assert!(
            OptionResolver::validate_no_conflicts_with_table_properties(
                &map(&[(alias, "other_ts")]),
                &table
            )
            .is_err()
        );
    }

    #[test]
    fn test_persisted_primary_key_takes_precedence_over_alias() {
        let primary = HudiTableConfig::OrderingFields.as_ref();
        let alias = HudiTableConfig::OrderingFields.aliases()[0].key;
        let table = map(&[(primary, "ts"), (alias, "legacy_ts")]);

        assert!(
            OptionResolver::validate_no_conflicts_with_table_properties(
                &map(&[(primary, "ts")]),
                &table
            )
            .is_ok()
        );

        let msg = OptionResolver::validate_no_conflicts_with_table_properties(
            &map(&[(primary, "other_ts")]),
            &table,
        )
        .unwrap_err()
        .to_string();
        assert!(msg.contains(&format!("{primary}:")), "{msg}");
        assert_eq!(msg.matches(&format!("{primary}:")).count(), 1, "{msg}");
        assert!(!msg.contains(&format!("{alias}:")), "{msg}");
    }

    #[tokio::test]
    async fn test_build_rejects_option_conflicting_with_table_properties() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let mut builder = TableBuilder::from_base_uri(base_url.path())
            .with_hudi_option("hoodie.table.type", "MERGE_ON_READ");

        let Err(err) = builder.build().await else {
            panic!("a conflicting hoodie.table.type must be rejected");
        };
        let msg = err.to_string();
        assert!(msg.contains("hoodie.table.type"), "{msg}");
        assert!(msg.contains("MERGE_ON_READ"), "{msg}");
        assert!(msg.contains("COPY_ON_WRITE"), "{msg}");
    }

    #[tokio::test]
    async fn test_build_accepts_option_matching_table_properties() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let mut builder = TableBuilder::from_base_uri(base_url.path())
            .with_hudi_option("hoodie.table.type", "COPY_ON_WRITE");

        assert!(builder.build().await.is_ok(), "first build");
        assert!(
            builder.build().await.is_ok(),
            "second build on the same builder"
        );
    }

    #[tokio::test]
    async fn test_build_with_resolved_options_skips_conflict_check() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let mut builder = TableBuilder::from_base_uri(base_url.path())
            .with_hudi_option("hoodie.table.type", "MERGE_ON_READ");

        assert!(builder.build_with_resolved_options().await.is_ok());
    }

    #[tokio::test]
    async fn test_build_detects_conflict_through_alias() {
        // The fixture persists the ordering field under the legacy
        // `hoodie.table.precombine.field` key.
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let ordering_key = HudiTableConfig::OrderingFields.as_ref();

        let mut same = TableBuilder::from_base_uri(base_url.path())
            .with_hudi_option(ordering_key, "longField");
        assert!(same.build().await.is_ok());

        let mut different =
            TableBuilder::from_base_uri(base_url.path()).with_hudi_option(ordering_key, "intField");
        assert!(different.build().await.is_err());
    }

    #[test]
    fn test_builder_supplied_and_derived_configs_are_exempt() {
        let table = map(&[
            ("hoodie.base.path", "/persisted/path"),
            ("hoodie.table.checksum", "123"),
            ("hoodie.table.create.schema", "{\"type\":\"record\"}"),
        ]);
        let user = map(&[
            ("hoodie.base.path", "/user/path"),
            ("hoodie.table.checksum", "456"),
            ("hoodie.table.create.schema", "{ \"type\": \"record\" }"),
        ]);
        assert!(OptionResolver::validate_no_conflicts_with_table_properties(&user, &table).is_ok());
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
