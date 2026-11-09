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

use chrono::{DateTime, Utc};
use paste::paste;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::Result;
use crate::config::properties::parse_properties;
use crate::config::read::{HudiReadConfig, ReadConfigScope};
use crate::config::table::HudiTableConfig;
use crate::config::util::{parse_data_for_options, split_hudi_options_from_others};
use crate::config::{HUDI_CONF_DIR, HudiConfigs};
use crate::storage::Storage;
use crate::table::Table;
use crate::table::fs_view::FileSystemView;
use crate::table::validation::validate_configs;
use crate::timeline::Timeline;
use crate::timeline::loader::{PrelistedTimeline, layout_two_active_timeline_dir};
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
    /// configs are invalid or unsupported (for example a table version other than 6, 8, or 9).
    pub async fn build(&mut self) -> Result<Table> {
        let option_resolver = &mut self.option_resolver;

        let prelisted_timeline = option_resolver
            .resolve_options_with_timeline_listing()
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

        let storage_options = Arc::from(self.option_resolver.storage_options.clone());

        let timeline = Timeline::new_from_storage(
            hudi_configs.clone(),
            storage_options.clone(),
            prelisted_timeline,
        )
        .await?;

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
        let storage = self.resolve_storage()?;
        self.resolve_hudi_options(storage).await.map(|_| ())
    }

    /// [`Self::resolve_options`], while listing the layout-2 active timeline
    /// directory concurrently with the `hoodie.properties` read.
    ///
    /// Which directory holds the active timeline is only known once the
    /// properties are read, so the listing is a guess: it is returned only when
    /// it succeeded and is known to postdate the properties that were read, and
    /// the timeline loader uses it only when the resolved configs point at the
    /// same directory. On a layout-1 table it is one unused LIST.
    pub(crate) async fn resolve_options_with_timeline_listing(
        &mut self,
    ) -> Result<Option<PrelistedTimeline>> {
        let storage = self.resolve_storage()?;
        let dir = layout_two_active_timeline_dir(&storage.hudi_configs);
        let (resolved, listing) = futures::future::join(
            self.resolve_hudi_options(storage.clone()),
            storage.list_files_with_latest_modified(Some(&dir)),
        )
        .await;
        let properties_last_modified = resolved?;
        let Ok((files, latest_modified)) = listing else {
            return Ok(None);
        };

        // A writer changes the timeline before it rewrites `hoodie.properties`
        // (a layout upgrade migrates the timeline first), so the listing is known
        // to reflect the properties that were read only when it holds a file the
        // store stamped after them. Both times are the store's, so the reader's
        // clock does not enter; equal times are ambiguous at the store's
        // granularity (a second on S3) and do not count. A store that keeps no
        // last-modified time reports the Unix epoch, which orders nothing.
        if properties_last_modified <= DateTime::UNIX_EPOCH
            || latest_modified.is_none_or(|latest| latest <= properties_last_modified)
        {
            return Ok(None);
        }
        Ok(Some(PrelistedTimeline {
            base_url: storage.base_url.clone(),
            dir,
            files,
        }))
    }

    /// Resolves the user-provided options and env vars, and creates the
    /// [`Storage`] that the Hudi options are then read through.
    fn resolve_storage(&mut self) -> Result<Arc<Storage>> {
        self.resolve_user_provided_options();

        // If any user-provided options are intended for cloud storage and in uppercase,
        // convert them to lowercase. This is to allow `object_store` to pick them up.
        self.resolve_env_vars();

        // At this point, we have resolved the storage options needed for accessing the storage layer.
        Ok(Storage::new(
            Arc::new(self.storage_options.clone()),
            Arc::new(HudiConfigs::new(self.hudi_options.iter())),
        )?)
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

    /// Returns the last-modified time of `hoodie.properties`.
    async fn resolve_hudi_options(&mut self, storage: Arc<Storage>) -> Result<DateTime<Utc>> {
        let hudi_options = &mut self.hudi_options;
        let properties_last_modified =
            Self::imbue_table_properties(hudi_options, storage.clone()).await?;

        // TODO support imbuing Hudi options from env vars HOODIE_ENV.*
        // (see https://hudi.apache.org/docs/next/s3_hoodie)
        // before loading global configs

        Self::imbue_global_hudi_configs_if_absent(hudi_options, storage).await?;

        // Validate the resolved Hudi options
        let hudi_configs = HudiConfigs::new(self.hudi_options.iter());
        validate_configs(&hudi_configs)?;
        Ok(properties_last_modified)
    }

    async fn imbue_table_properties(
        options: &mut HashMap<String, String>,
        storage: Arc<Storage>,
    ) -> Result<DateTime<Utc>> {
        let (bytes, last_modified) = storage
            .get_file_data_and_last_modified(".hoodie/hoodie.properties")
            .await?;
        let table_properties = parse_properties(&bytes)?;

        // Table properties on storage (hoodie.properties) should have the highest precedence,
        // except for writer-changeable properties like enabling metadata table/indexes.
        // TODO: return err when user-provided options conflict with table properties
        for (k, v) in table_properties {
            options.insert(k.to_string(), v.to_string());
        }

        Ok(last_modified)
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
    use crate::storage::recording::{
        RequestKind, record_storage_requests, record_storage_requests_around_first_list,
    };
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

    const REQUEST_DELAY: std::time::Duration = std::time::Duration::from_millis(100);

    /// Asserts the opened timeline equals one loaded with no pre-listing.
    async fn assert_timeline_as_listed_on_open(table: &Table) {
        let listed = Timeline::new_from_storage(
            table.hudi_configs.clone(),
            table.storage_options.clone(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(table.timeline.completed_commits, listed.completed_commits);
        assert_eq!(table.timeline.pending_instants, listed.pending_instants);
        assert_eq!(
            table.timeline.earliest_active_instant,
            listed.earliest_active_instant
        );
    }

    fn set_modified(path: &std::path::Path, time: std::time::SystemTime) {
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(time)
            .unwrap();
    }

    /// The files directly under `dir`, sorted by name; none if it is missing.
    fn files_in(dir: &std::path::Path) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_file())
            .collect();
        files.sort();
        files
    }

    /// A private copy of `table` whose `hoodie.properties` the store stamped a
    /// day ago and whose timeline files it stamped since, as on a table that has
    /// committed after its properties last changed.
    fn table_with_settled_properties(table: hudi_test::SampleTable) -> String {
        let base_uri = table.path_to_cow_fresh();
        let hoodie = PathBuf::from(&base_uri).join(".hoodie");
        let now = std::time::SystemTime::now();
        set_modified(
            &hoodie.join("hoodie.properties"),
            now - std::time::Duration::from_secs(86_400),
        );
        for file in files_in(&hoodie.join("timeline")) {
            set_modified(&file, now - std::time::Duration::from_secs(3_600));
        }
        base_uri
    }

    /// Regression test: the active timeline LIST of a layout-2 table overlaps
    /// the `hoodie.properties` GET instead of waiting for it, and is not
    /// repeated once the properties confirm the directory.
    #[tokio::test]
    async fn test_build_lists_layout_two_timeline_while_reading_properties() {
        let base_uri = table_with_settled_properties(hudi_test::SampleTable::V8Nonpartitioned);
        let recording = record_storage_requests(REQUEST_DELAY, false);
        let table = TableBuilder::from_base_uri(&base_uri)
            .build()
            .await
            .unwrap();
        let properties = recording
            .log
            .matching(RequestKind::Get, ".hoodie/hoodie.properties");
        let timeline_lists = recording
            .log
            .matching(RequestKind::List, ".hoodie/timeline");
        drop(recording);

        assert_eq!(properties.len(), 1);
        assert_eq!(timeline_lists.len(), 1, "{timeline_lists:?}");
        assert!(
            timeline_lists[0].start < properties[0].end,
            "the timeline LIST started only after the properties GET ended: \
             {timeline_lists:?} {properties:?}"
        );
        assert!(!table.timeline.completed_commits.is_empty());
        assert_timeline_as_listed_on_open(&table).await;
    }

    /// A layout-1 table ignores the `.hoodie/timeline` listing, whether it
    /// succeeded or failed, and lists `.hoodie` as before; a layout-2 table
    /// whose listing failed lists its timeline again. The properties are
    /// settled, and the layout-1 table has a stray `.hoodie/timeline` file
    /// newer than them, so a successful listing would be returned for reuse.
    #[tokio::test]
    async fn test_build_ignores_unusable_timeline_listing() {
        for (table, fail_listing) in [
            (hudi_test::SampleTable::V6Nonpartitioned, false),
            (hudi_test::SampleTable::V6Nonpartitioned, true),
            (hudi_test::SampleTable::V8Nonpartitioned, true),
            (hudi_test::SampleTable::V9TxnsSimpleMeta, true),
        ] {
            let base_uri = table_with_settled_properties(table);
            let timeline = PathBuf::from(&base_uri).join(".hoodie/timeline");
            if !timeline.exists() {
                std::fs::create_dir(&timeline).unwrap();
                std::fs::write(timeline.join("not_an_instant"), b"").unwrap();
            }
            let recording = record_storage_requests(REQUEST_DELAY, fail_listing);
            let table = TableBuilder::from_base_uri(&base_uri)
                .build()
                .await
                .unwrap();
            let requests = recording.log.requests();
            drop(recording);

            assert!(
                !table.timeline.completed_commits.is_empty(),
                "{base_uri}: {requests:?}"
            );
            assert_timeline_as_listed_on_open(&table).await;
        }
    }

    /// A timeline path given as an option is overridden by the one in
    /// `hoodie.properties`, so the listing taken of it must not be used even
    /// though it holds a file newer than the properties.
    #[tokio::test]
    async fn test_build_ignores_listing_of_overridden_timeline_path() {
        let base_uri = table_with_settled_properties(hudi_test::SampleTable::V8Nonpartitioned);
        let not_the_timeline = PathBuf::from(&base_uri).join(".hoodie/not_the_timeline");
        std::fs::create_dir(&not_the_timeline).unwrap();
        std::fs::write(not_the_timeline.join("not_an_instant"), b"").unwrap();

        let recording = record_storage_requests(REQUEST_DELAY, false);
        let table = TableBuilder::from_base_uri(&base_uri)
            .with_hudi_option(HudiTableConfig::TimelinePath, "not_the_timeline")
            .build()
            .await
            .unwrap();
        let speculative = recording
            .log
            .matching(RequestKind::List, ".hoodie/not_the_timeline");
        drop(recording);

        assert_eq!(speculative.len(), 1);
        assert!(!table.timeline.completed_commits.is_empty());
        assert_timeline_as_listed_on_open(&table).await;
    }

    /// Regression test: a layout upgrade migrates the timeline and only then
    /// rewrites `hoodie.properties`, so a timeline LIST served before the
    /// migration finished must not be used with the upgraded properties that a
    /// GET served after it returns.
    ///
    /// Run with the store's clock in step with this host and a day behind it:
    /// the decision compares the store's timestamps only, so a reader clock far
    /// ahead of the store must not let the partial listing through.
    #[tokio::test]
    async fn test_build_discards_timeline_listing_older_than_properties() {
        let day = std::time::Duration::from_secs(86_400);
        for store_clock_behind in [std::time::Duration::ZERO, day] {
            let base_uri = table_with_settled_properties(hudi_test::SampleTable::V8Nonpartitioned);
            let hoodie = PathBuf::from(&base_uri).join(".hoodie");
            let timeline = hoodie.join("timeline");
            let migrated = PathBuf::from(&base_uri).join("migrated_timeline");
            let upgraded_properties = std::fs::read(hoodie.join("hoodie.properties")).unwrap();
            let store_now = std::time::SystemTime::now() - store_clock_behind;

            // Mid-upgrade: only the first timeline file has been migrated.
            std::fs::create_dir(&migrated).unwrap();
            let files = files_in(&timeline);
            for file in &files {
                set_modified(file, store_now - std::time::Duration::from_secs(10));
            }
            for file in &files[1..] {
                std::fs::rename(file, migrated.join(file.file_name().unwrap())).unwrap();
            }

            let recording = record_storage_requests_around_first_list(REQUEST_DELAY, move || {
                for file in files_in(&migrated) {
                    std::fs::rename(&file, timeline.join(file.file_name().unwrap())).unwrap();
                }
                let properties = hoodie.join("hoodie.properties");
                std::fs::write(&properties, upgraded_properties).unwrap();
                set_modified(&properties, store_now);
            });
            let table = TableBuilder::from_base_uri(&base_uri)
                .build()
                .await
                .unwrap();
            let timeline_lists = recording
                .log
                .matching(RequestKind::List, ".hoodie/timeline");
            drop(recording);

            assert!(
                !table.timeline.completed_commits.is_empty(),
                "store clock behind by {store_clock_behind:?}: the timeline was read from \
                 the listing taken mid-upgrade"
            );
            assert_timeline_as_listed_on_open(&table).await;
            assert_eq!(
                timeline_lists.len(),
                2,
                "the listing taken before the properties rewrite must be listed again: \
                 {timeline_lists:?}"
            );
        }
    }

    /// A listing whose files are stamped no later than `hoodie.properties`, as
    /// on a table copied in bulk, cannot be shown to postdate them, so the
    /// timeline is listed again.
    #[tokio::test]
    async fn test_build_lists_again_when_timeline_is_not_newer_than_properties() {
        let base_uri = table_with_settled_properties(hudi_test::SampleTable::V8Nonpartitioned);
        let hoodie = PathBuf::from(&base_uri).join(".hoodie");
        let copied_at = std::time::SystemTime::now() - std::time::Duration::from_secs(3_600);
        set_modified(&hoodie.join("hoodie.properties"), copied_at);
        for file in files_in(&hoodie.join("timeline")) {
            set_modified(&file, copied_at);
        }

        let recording = record_storage_requests(REQUEST_DELAY, false);
        let table = TableBuilder::from_base_uri(&base_uri)
            .build()
            .await
            .unwrap();
        let timeline_lists = recording
            .log
            .matching(RequestKind::List, ".hoodie/timeline");
        drop(recording);

        assert_eq!(timeline_lists.len(), 2, "{timeline_lists:?}");
        assert!(!table.timeline.completed_commits.is_empty());
        assert_timeline_as_listed_on_open(&table).await;
    }

    /// A `hoodie.properties` stamped at the Unix epoch, as a store reports when
    /// it keeps no last-modified time, cannot be ordered against the listing,
    /// so the timeline is listed again even though its files are newer.
    #[tokio::test]
    async fn test_build_lists_again_when_properties_have_no_last_modified_time() {
        let base_uri = table_with_settled_properties(hudi_test::SampleTable::V8Nonpartitioned);
        set_modified(
            &PathBuf::from(&base_uri).join(".hoodie/hoodie.properties"),
            std::time::SystemTime::UNIX_EPOCH,
        );

        let recording = record_storage_requests(REQUEST_DELAY, false);
        let table = TableBuilder::from_base_uri(&base_uri)
            .build()
            .await
            .unwrap();
        let timeline_lists = recording
            .log
            .matching(RequestKind::List, ".hoodie/timeline");
        drop(recording);

        assert_eq!(timeline_lists.len(), 2, "{timeline_lists:?}");
        assert!(!table.timeline.completed_commits.is_empty());
        assert_timeline_as_listed_on_open(&table).await;
    }

    /// A table whose properties cannot be read fails with the same error as
    /// resolving its options alone, not with the failed timeline listing.
    #[tokio::test]
    async fn test_build_without_properties_fails_with_properties_error() {
        let dir = tempfile::tempdir().unwrap();
        let base_uri = dir.path().to_str().unwrap();
        let expected = OptionResolver::new(base_uri)
            .resolve_options()
            .await
            .unwrap_err()
            .to_string();

        let recording = record_storage_requests(REQUEST_DELAY, true);
        let err = TableBuilder::from_base_uri(base_uri)
            .build()
            .await
            .unwrap_err();
        drop(recording);

        assert_eq!(err.to_string(), expected);
        assert!(expected.contains("hoodie.properties"), "{expected}");
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
