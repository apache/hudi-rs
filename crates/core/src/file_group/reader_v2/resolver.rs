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

//! Derives a [`ReaderContext`] from a table's resolved configs.

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::error::ConfigError;
use crate::config::read::HudiReadConfig;
use crate::config::table::{BaseFileFormatValue, HudiTableConfig};
use crate::error::CoreError;
use crate::file_group::reader_v2::reader_context::{MergeMode, ReaderContext};
use crate::file_group::reader_v2::record_context::RecordContext;
use crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler;
use crate::merge::RecordMergeStrategyValue;
use crate::timeline::selector::InstantRange;
use std::collections::HashMap;
use std::str::FromStr;

/// Resolve the MOR reader context from `hudi_configs`, which the caller has
/// already merged read options into.
#[allow(dead_code)]
pub(crate) fn resolve_reader_context(
    hudi_configs: &HudiConfigs,
    has_log_files: bool,
) -> Result<ReaderContext> {
    let table_path: String = hudi_configs.get(HudiTableConfig::BasePath)?.into();

    // Resolved by `Table::prepare_reader_options` for table-level reads. A
    // `FileGroupReader` built straight from a base URI never loads the timeline,
    // so the caller must supply it; defaulting here would silently widen the read.
    let latest_commit_time: String = hudi_configs
        .try_get(HudiReadConfig::EndTimestamp)?
        .ok_or_else(|| ConfigError::NotFound(HudiReadConfig::EndTimestamp.as_ref().to_string()))?
        .into();

    let merge_mode = resolve_merge_mode(hudi_configs)?;
    let base_file_format = BaseFileFormatValue::resolve_from_configs(hudi_configs, None)?;
    let instant_range = resolve_instant_range(hudi_configs)?;
    let (table_config, hoodie_reader_config) = partition_configs(hudi_configs);

    Ok(ReaderContext {
        table_path,
        latest_commit_time,
        merge_mode: merge_mode.as_ref().to_string(),
        base_file_format: base_file_format.as_ref().to_string(),
        has_log_files,
        instant_range: Some(instant_range),
        table_config,
        hoodie_reader_config,
        // Log blocks carry record positions, but no code reads them yet.
        // Merging by key is what the legacy reader does, so that is what a v2
        // read must do until the position-based merge lands.
        should_merge_use_record_position: false,
        // Only one iterator mode is implemented.
        iterator_mode: "ENGINE_RECORD".to_string(),
        // Dispatch is on `merge_mode`; the strategy id is carried, not consulted.
        merge_strategy_id: String::new(),
        // No bootstrap support in this crate.
        has_bootstrap_base_file: false,
        needs_bootstrap_merge: false,
        enable_logical_timestamp_field_repair: false,
        // Predicate pushdown into the merge path has no caller here, so no
        // filter is installed and the primary-key-safety gate is irrelevant.
        row_filter_builder: None,
        mor_pk_safe: false,
        // The table-version < 8 completion gate needs a timeline the caller
        // has not loaded; leaving it unset keeps the gate a no-op.
        completion_gate_inputs: None,
        record_context: RecordContext::default(),
        schema_handler: FileGroupReaderSchemaHandler::new(),
    })
}

/// How a v9 table states its merge semantics. Not modelled as a
/// [`HudiTableConfig`] yet — this crate does not otherwise read it — so it is
/// looked up by name.
#[allow(dead_code)]
const RECORD_MERGE_MODE: &str = "hoodie.record.merge.mode";

/// Prefix identifying a per-read config.
#[allow(dead_code)]
const READ_CONFIG_PREFIX: &str = "hoodie.read.";

/// Prefixes identifying configs that steer this crate's own behavior. They are
/// neither something the table declares about itself nor a per-read override,
/// so they reach the reader through neither map.
#[allow(dead_code)]
const CRATE_CONFIG_PREFIXES: [&str; 2] = ["hoodie.internal.", "hoodie.plan."];

/// Split the merged config bag into the table's own properties and the
/// per-read overrides, so the merge code can tell one from the other.
///
/// Anything belonging to neither is dropped rather than swept into the table
/// properties: a reader has no way to tell a swept-in crate config from
/// something the table actually declared.
#[allow(dead_code)]
fn partition_configs(
    hudi_configs: &HudiConfigs,
) -> (HashMap<String, String>, HashMap<String, String>) {
    let mut table_config = HashMap::new();
    let mut reader_config = HashMap::new();

    for (key, value) in hudi_configs.as_options() {
        if key.starts_with(READ_CONFIG_PREFIX) {
            reader_config.insert(key, value);
        } else if !CRATE_CONFIG_PREFIXES
            .iter()
            .any(|prefix| key.starts_with(prefix))
        {
            table_config.insert(key, value);
        }
    }

    (table_config, reader_config)
}

/// Build the window bounding which instants the log scan admits.
///
/// Mirrors the bounds the legacy file group reader applies, so a v2 read sees
/// the same log blocks: exclusive at the start, inclusive at the pinned commit.
#[allow(dead_code)]
fn resolve_instant_range(hudi_configs: &HudiConfigs) -> Result<InstantRange> {
    let timezone: String = hudi_configs
        .get_or_default(HudiTableConfig::TimelineTimezone)
        .into();
    let start_timestamp = hudi_configs
        .try_get(HudiReadConfig::StartTimestamp)?
        .map(|v| -> String { v.into() });
    let end_timestamp = hudi_configs
        .try_get(HudiReadConfig::EndTimestamp)?
        .map(|v| -> String { v.into() });

    Ok(InstantRange::new(
        timezone,
        start_timestamp,
        end_timestamp,
        false,
        true,
    ))
}

/// Map the table's record merge strategy onto a MOR [`MergeMode`].
///
/// | `hoodie.table.record.merge.strategy` | [`MergeMode`]           |
/// |-------------------------------------|-------------------------|
/// | `overwrite_with_latest`             | `EventTimeOrdering`     |
/// | `append_only`                       | unsupported — see below |
///
/// `append_only` has no MOR counterpart: the reader always merges by record
/// key, so there is no mode that reproduces "keep every version". The table
/// config derives `append_only` whenever meta fields are disabled or no
/// ordering field is set, which makes it reachable for ordinary tables — so
/// this returns an error rather than picking a mode that would change which
/// rows a query returns.
#[allow(dead_code)]
fn resolve_merge_mode(hudi_configs: &HudiConfigs) -> Result<MergeMode> {
    // A v9 table states its merge semantics directly. Prefer that over
    // inferring them: the inference below predates the key and gets a
    // commit-time-ordered table with no ordering field wrong.
    if let Some(mode) = hudi_configs.as_options().get(RECORD_MERGE_MODE) {
        return match mode.to_ascii_uppercase().as_str() {
            "COMMIT_TIME_ORDERING" => Ok(MergeMode::CommitTimeOrdering),
            "EVENT_TIME_ORDERING" => Ok(MergeMode::EventTimeOrdering),
            other => Err(CoreError::Unsupported(format!(
                "Record merge mode '{other}' is not supported."
            ))),
        };
    }

    let strategy: String = hudi_configs
        .get_or_default(HudiTableConfig::RecordMergeStrategy)
        .into();

    match RecordMergeStrategyValue::from_str(&strategy)? {
        RecordMergeStrategyValue::OverwriteWithLatest => Ok(MergeMode::EventTimeOrdering),
        RecordMergeStrategyValue::AppendOnly => Err(CoreError::Unsupported(format!(
            "Record merge strategy '{}' has no merge-on-read equivalent. \
             The merge-on-read reader merges by record key, which does not \
             preserve every record version.",
            RecordMergeStrategyValue::AppendOnly.as_ref(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::internal::HudiInternalConfig;
    use crate::config::plan::HudiPlanConfig;
    use crate::file_group::reader::FileGroupReader;
    use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
    use std::sync::Arc;

    /// Configs with just enough set for the resolver to succeed. Includes an
    /// ordering field so the table derives `overwrite_with_latest` rather than
    /// the append-only fallback exercised in its own tests below.
    fn minimal_configs() -> Vec<(String, String)> {
        vec![
            (
                HudiTableConfig::BasePath.as_ref().to_string(),
                "file:///tmp/t".to_string(),
            ),
            (
                HudiReadConfig::EndTimestamp.as_ref().to_string(),
                "20240101000000000".to_string(),
            ),
            (
                HudiTableConfig::OrderingFields.as_ref().to_string(),
                "ts".to_string(),
            ),
        ]
    }

    fn configs_without(key: &str) -> HudiConfigs {
        HudiConfigs::new(
            minimal_configs()
                .into_iter()
                .filter(|(k, _)| k != key)
                .collect::<Vec<_>>(),
        )
    }

    /// [`resolve_instant_range`] deliberately reproduces the bounds the legacy
    /// file group reader computes, so that a read through either path admits
    /// the same log blocks. Nothing but this test enforces that: the two live
    /// in different modules and neither calls the other, so an edit to one
    /// would otherwise drift silently.
    ///
    /// Compares the `Debug` rendering rather than the fields because
    /// `InstantRange`'s fields are private — which also means a field added to
    /// the struct is covered here for free, instead of being silently skipped
    /// by an explicit field-by-field comparison.
    #[test]
    fn instant_range_matches_the_legacy_reader() {
        let mut options = minimal_configs();
        options.push((
            HudiReadConfig::StartTimestamp.as_ref().to_string(),
            "20230101000000000".to_string(),
        ));
        let configs = HudiConfigs::new(options);

        let legacy = FileGroupReader::new_with_overrides(
            Arc::new(configs.clone()),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap()
        .create_instant_range_for_log_file_scan()
        .unwrap();

        let resolved = resolve_reader_context(&configs, true)
            .unwrap()
            .instant_range
            .expect("the resolver always derives a range");

        assert_eq!(format!("{legacy:?}"), format!("{resolved:?}"));
    }

    #[test]
    fn resolves_table_path_from_base_path_config() {
        let configs = HudiConfigs::new(minimal_configs());

        let ctx = resolve_reader_context(&configs, false).unwrap();

        assert_eq!(ctx.table_path, "file:///tmp/t");
    }

    #[test]
    fn resolves_latest_commit_time_from_end_timestamp() {
        let configs = HudiConfigs::new(minimal_configs());

        let ctx = resolve_reader_context(&configs, false).unwrap();

        assert_eq!(ctx.latest_commit_time, "20240101000000000");
    }

    /// `latest_commit_time` gates which log blocks are visible, so a missing
    /// value must fail loudly rather than silently reading everything.
    #[test]
    fn errors_when_end_timestamp_is_absent() {
        let configs = configs_without(HudiReadConfig::EndTimestamp.as_ref());

        let err = resolve_reader_context(&configs, false).unwrap_err();

        assert!(
            err.to_string()
                .contains(HudiReadConfig::EndTimestamp.as_ref()),
            "error should name the missing key, got: {err}"
        );
    }

    #[test]
    fn resolves_has_log_files_from_the_caller() {
        let configs = HudiConfigs::new(minimal_configs());

        assert!(
            resolve_reader_context(&configs, true)
                .unwrap()
                .has_log_files
        );
        assert!(
            !resolve_reader_context(&configs, false)
                .unwrap()
                .has_log_files
        );
    }

    #[test]
    fn defaults_base_file_format_to_parquet() {
        let configs = HudiConfigs::new(minimal_configs());

        let ctx = resolve_reader_context(&configs, true).unwrap();

        assert_eq!(ctx.base_file_format, BaseFileFormatValue::Parquet.as_ref());
    }

    /// The log scan is bounded by the same window the legacy reader uses, so a
    /// v2 read sees exactly the log blocks a legacy read would.
    #[test]
    fn bounds_instant_range_by_the_read_window() {
        let mut options = minimal_configs();
        options.push((
            HudiReadConfig::StartTimestamp.as_ref().to_string(),
            "20230101000000000".to_string(),
        ));
        let configs = HudiConfigs::new(options);

        let ctx = resolve_reader_context(&configs, true).unwrap();

        let rendered = format!("{:?}", ctx.instant_range.expect("always derived"));
        assert!(rendered.contains("20230101000000000"), "got: {rendered}");
        assert!(rendered.contains("20240101000000000"), "got: {rendered}");
        assert!(
            rendered.contains("start_inclusive: false"),
            "incremental reads are exclusive at the start, got: {rendered}"
        );
        assert!(
            rendered.contains("end_inclusive: true"),
            "the pinned commit is included, got: {rendered}"
        );
    }

    /// The reader is handed the table's own configs and the per-read overrides
    /// as separate maps, so a read config can never be mistaken for a table
    /// property (or vice versa) once it reaches the merge code.
    #[test]
    fn separates_table_configs_from_read_configs() {
        let configs = HudiConfigs::new(minimal_configs());

        let ctx = resolve_reader_context(&configs, true).unwrap();

        assert_eq!(
            ctx.table_config
                .get(HudiTableConfig::OrderingFields.as_ref()),
            Some(&"ts".to_string())
        );
        assert!(
            !ctx.table_config
                .contains_key(HudiReadConfig::EndTimestamp.as_ref()),
            "read configs must not leak into table config"
        );

        assert_eq!(
            ctx.hoodie_reader_config
                .get(HudiReadConfig::EndTimestamp.as_ref()),
            Some(&"20240101000000000".to_string())
        );
        assert!(
            !ctx.hoodie_reader_config
                .contains_key(HudiTableConfig::OrderingFields.as_ref()),
            "table configs must not leak into reader config"
        );
    }

    /// Neither map is a catch-all. Configs that steer this crate's own behavior
    /// are not table properties and are not per-read overrides, so they belong
    /// in neither — sweeping them into the table properties would leave a
    /// reader unable to tell them from something the table declared about
    /// itself.
    #[test]
    fn drops_configs_that_are_neither_table_nor_read() {
        let crate_configs = [
            HudiInternalConfig::SkipConfigValidation.as_ref(),
            HudiInternalConfig::TimelineArchivedReadEnabled.as_ref(),
            HudiPlanConfig::ListingParallelism.as_ref(),
        ];
        let mut options = minimal_configs();
        for key in crate_configs {
            options.push((key.to_string(), "1".to_string()));
        }
        let configs = HudiConfigs::new(options);

        let ctx = resolve_reader_context(&configs, true).unwrap();

        for key in crate_configs {
            assert!(
                !ctx.table_config.contains_key(key),
                "{key} is not a table property"
            );
            assert!(
                !ctx.hoodie_reader_config.contains_key(key),
                "{key} is not a per-read override"
            );
        }
    }

    /// Log blocks carry record positions, but nothing reads them yet. Keeping
    /// this off means a v2 read merges by key exactly as a legacy read does;
    /// the position-based merge turns it on when it lands.
    #[test]
    fn leaves_position_based_merge_off() {
        let configs = HudiConfigs::new(minimal_configs());

        let ctx = resolve_reader_context(&configs, true).unwrap();

        assert!(!ctx.should_merge_use_record_position);
    }

    /// Defaults chosen to match what the legacy reader returns today: deletes
    /// are applied but never emitted, output keeps merge order, and only
    /// completed instants are read.
    #[test]
    fn defaults_reader_parameters_to_legacy_behavior() {
        let params = ReaderParameters::default();

        assert!(!params.emit_delete);
        assert!(!params.sort_output);
        assert!(!params.allow_inflight_instants);
    }

    #[test]
    fn maps_overwrite_with_latest_to_event_time_ordering() {
        let configs = HudiConfigs::new(minimal_configs());

        let ctx = resolve_reader_context(&configs, true).unwrap();

        assert_eq!(ctx.merge_mode, MergeMode::EventTimeOrdering.as_ref());
    }

    /// `append_only` has no counterpart in the MOR merge modes — the reader
    /// always merges by record key. Mapping it to a merge mode silently drops
    /// rows, so the resolver refuses rather than guessing.
    #[test]
    fn rejects_append_only_derived_from_missing_ordering_fields() {
        let configs = configs_without(HudiTableConfig::OrderingFields.as_ref());

        let err = resolve_reader_context(&configs, true).unwrap_err();

        assert!(
            err.to_string().contains("append_only"),
            "error should name the unmapped strategy, got: {err}"
        );
    }

    #[test]
    fn rejects_append_only_derived_from_disabled_meta_fields() {
        let mut options = minimal_configs();
        options.push((
            HudiTableConfig::PopulatesMetaFields.as_ref().to_string(),
            "false".to_string(),
        ));
        let configs = HudiConfigs::new(options);

        let err = resolve_reader_context(&configs, true).unwrap_err();

        assert!(
            err.to_string().contains("append_only"),
            "error should name the unmapped strategy, got: {err}"
        );
    }
}
