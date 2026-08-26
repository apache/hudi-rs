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
use crate::file_group::reader_v2::buffer::spillable_map;
use crate::file_group::reader_v2::metadata_merger::resolve_custom_merger;
use crate::file_group::reader_v2::reader_context::{CONFIG_MERGE_TYPE, MergeMode, ReaderContext};
use crate::file_group::reader_v2::record_context::RecordContext;
use crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler;
use crate::timeline::selector::InstantRange;
use std::collections::HashMap;

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

    // A slice with no log files consults no merger, so an unsupported merge
    // mode (CUSTOM, or one inferred from a custom payload class) must not
    // refuse it: `version_two_unsupported_reason` deliberately serves such
    // reads. Commit-time ordering stands in as the mode nothing reads.
    let merge_mode = match resolve_merge_mode(hudi_configs) {
        Ok(mode) => mode,
        Err(CoreError::Unsupported(_)) if !has_log_files => MergeMode::CommitTimeOrdering,
        Err(e) => return Err(e),
    };
    let base_file_format = BaseFileFormatValue::resolve_from_configs(hudi_configs, None)?;
    let instant_range = resolve_instant_range(hudi_configs)?;
    let (table_config, hoodie_reader_config) = partition_configs(hudi_configs);
    let merge_strategy_id = RECORD_MERGE_STRATEGY_ID_KEYS
        .iter()
        .find_map(|k| table_config.get(*k))
        .cloned()
        .unwrap_or_default();

    Ok(ReaderContext {
        table_path,
        latest_commit_time,
        merge_mode: merge_mode.as_ref().to_string(),
        base_file_format: base_file_format.as_ref().to_string(),
        has_log_files,
        instant_range: Some(instant_range),
        table_config,
        hoodie_reader_config,
        should_merge_use_record_position: resolve_use_record_position(hudi_configs)?,
        // Only one iterator mode is implemented.
        iterator_mode: "ENGINE_RECORD".to_string(),
        // Carried so a CUSTOM table's merger selection can confirm the id defers
        // to the payload class rather than naming a merger of its own.
        merge_strategy_id,
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

/// Per-read configs Hudi does *not* spell `hoodie.read.*`.
///
/// The reader looks these up on `ReaderContext::hoodie_reader_config`, so the
/// prefix rule alone would route them to the table properties instead and every
/// one of them would read as unset — the merge budget silently back at its 1 GiB
/// default, the spill directory back at `/tmp`, the hard memory cap off, and
/// `skip_merge` undetected (so a read asking for unmerged output would quietly
/// get merged output rather than the unsupported-mode error).
///
/// Listed by reference to the constants that consume them so a rename cannot
/// leave this behind.
#[allow(dead_code)]
const READER_CONFIG_KEYS: [&str; 5] = [
    spillable_map::CONFIG_MERGE_MAX_SIZE,
    spillable_map::CONFIG_MAX_PEAK_MEMORY,
    spillable_map::CONFIG_SPILLABLE_MAP_PATH,
    spillable_map::CONFIG_DISKMAP_TYPE,
    CONFIG_MERGE_TYPE,
];

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
        if key.starts_with(READ_CONFIG_PREFIX) || READER_CONFIG_KEYS.contains(&key.as_str()) {
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
/// Mirrors the bounds file group reader version 1 applies, so a version 2 read
/// sees the same log blocks: exclusive at the start, inclusive at the pinned
/// commit.
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

/// Whether this read should match log records to base rows by position.
///
/// Read strictly rather than through `get_or_default`, which falls back to the
/// default when a value fails to parse: a typo would silently merge by key and
/// return rows that look ordinary, which is exactly what this setting exists to
/// change.
#[allow(dead_code)]
fn resolve_use_record_position(hudi_configs: &HudiConfigs) -> Result<bool> {
    Ok(hudi_configs
        .try_get(HudiReadConfig::MergeUseRecordPositions)?
        .map(|v| -> bool { v.into() })
        .unwrap_or(false))
}

/// Keys Hudi writes the merge inputs under. None is modelled as a
/// [`HudiTableConfig`] — this crate does not otherwise read them.
/// Both spellings of the strategy id, newest first. Table version 8 renamed it;
/// a pre-v8 table — the only kind that reaches the inference without stating a
/// merge mode — writes the older one, so reading only the new key means the
/// strategy is always empty for exactly the tables it exists to serve.
pub(crate) const RECORD_MERGE_STRATEGY_ID_KEYS: [&str; 2] = [
    "hoodie.record.merge.strategy.id",
    "hoodie.compaction.record.merger.strategy",
];
pub(crate) const PAYLOAD_CLASS_KEYS: [&str; 3] = [
    "hoodie.compaction.payload.class",
    "hoodie.datasource.write.payload.class",
    "hoodie.table.legacy.payload.class",
];

/// The strategy ids Hudi assigns to its built-in mergers.
const EVENT_TIME_STRATEGY_ID: &str = "eeb8d96f-b1e4-49fd-bbf8-28ac514178e5";
const COMMIT_TIME_STRATEGY_ID: &str = "ce9acb64-bde0-424c-9b91-f6ebba25356d";

/// Payload classes that imply one of the built-in orderings.
const EVENT_TIME_PAYLOADS: [&str; 2] = [
    "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
    "org.apache.hudi.common.model.EventTimeAvroPayload",
];
const COMMIT_TIME_PAYLOAD: &str = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload";

/// What a merge mode can be inferred to, including the case this reader cannot
/// serve.
#[derive(Debug, PartialEq, Eq)]
enum InferredMode {
    CommitTime,
    EventTime,
    /// A merger of the table's own. Not something this reader can reproduce.
    Custom,
}

impl InferredMode {
    /// The name Hudi uses, so a log line reads the same whether the mode was
    /// stated by the table or worked out here.
    fn as_hudi_name(&self) -> &'static str {
        match self {
            Self::CommitTime => "COMMIT_TIME_ORDERING",
            Self::EventTime => "EVENT_TIME_ORDERING",
            Self::Custom => "CUSTOM",
        }
    }
}

/// The mode implied by a payload class, if it implies one.
fn mode_from_payload_class(payload_class: &str) -> Option<InferredMode> {
    if payload_class.is_empty() {
        return None;
    }
    if EVENT_TIME_PAYLOADS.contains(&payload_class) {
        Some(InferredMode::EventTime)
    } else if payload_class == COMMIT_TIME_PAYLOAD {
        Some(InferredMode::CommitTime)
    } else {
        Some(InferredMode::Custom)
    }
}

/// The mode implied by a merge strategy id, if it implies one.
fn mode_from_strategy_id(strategy_id: &str) -> Option<InferredMode> {
    if strategy_id.is_empty() {
        return None;
    }
    match strategy_id {
        EVENT_TIME_STRATEGY_ID => Some(InferredMode::EventTime),
        COMMIT_TIME_STRATEGY_ID => Some(InferredMode::CommitTime),
        _ => Some(InferredMode::Custom),
    }
}

/// Work out how a table that predates `hoodie.record.merge.mode` merges.
///
/// Mirrors Java's `HoodieTableConfig.inferMergingConfigsForPreV9Table`, which is
/// what the engine integrations call for the same tables:
///
/// - nothing set at all → the ordering field decides. A table with one orders by
///   event time; a table without one orders by commit time.
/// - a payload class or a strategy id → the mode each implies, and anything not
///   built in is a merger of the table's own.
/// - from table version 8 the strategy id is authoritative; before that the
///   payload class is.
fn infer_merge_mode(hudi_configs: &HudiConfigs) -> Result<InferredMode> {
    let options = hudi_configs.as_options();
    let payload_class = PAYLOAD_CLASS_KEYS
        .iter()
        .find_map(|key| options.get(*key))
        .map(String::as_str)
        .unwrap_or_default();
    let strategy_id = RECORD_MERGE_STRATEGY_ID_KEYS
        .iter()
        .find_map(|key| options.get(*key))
        .map(String::as_str)
        .unwrap_or_default();

    if payload_class.is_empty() && strategy_id.is_empty() {
        let has_ordering_field = hudi_configs
            .try_get(HudiTableConfig::OrderingFields)?
            .map(|v| -> Vec<String> { v.into() })
            .is_some_and(|fields| fields.iter().any(|f| !f.trim().is_empty()));
        let mode = if has_ordering_field {
            InferredMode::EventTime
        } else {
            InferredMode::CommitTime
        };
        let ordering_desc = if has_ordering_field {
            "having"
        } else {
            "not having"
        };
        // Per file slice: `resolve_reader_context` runs once per slice, so this
        // repeats for a table-level constant.
        log::debug!(
            "merge mode {}: the table names no payload class or merge strategy, so it was \
             inferred from {ordering_desc} an ordering field",
            mode.as_hudi_name()
        );
        return Ok(mode);
    }

    let from_payload = mode_from_payload_class(payload_class);
    let from_strategy = mode_from_strategy_id(strategy_id);
    let table_version: isize = hudi_configs
        .try_get(HudiTableConfig::TableVersion)?
        .map(|v| v.into())
        .unwrap_or(6);

    let (inferred, source) = if table_version >= 8 {
        match from_strategy {
            Some(mode) => (Some(mode), "merge strategy"),
            None => (from_payload, "payload class"),
        }
    } else {
        match from_payload {
            Some(mode) => (Some(mode), "payload class"),
            None => (from_strategy, "merge strategy"),
        }
    };
    if let Some(ref mode) = inferred {
        log::debug!(
            "merge mode {}: inferred from the {source} of a version {table_version} table \
             (payload class {payload_class:?}, merge strategy {strategy_id:?})",
            mode.as_hudi_name()
        );
    }
    inferred.ok_or_else(|| {
        CoreError::Unsupported(format!(
            "Cannot infer a merge mode from payload class '{payload_class}' \
             or merge strategy '{strategy_id}'."
        ))
    })
}

/// How this table merges records.
///
/// A table from version 9 on states it outright. Older ones are inferred the way
/// Java infers them, from the payload class, the merge strategy id, and finally
/// whether an ordering field is set.
fn resolve_merge_mode(hudi_configs: &HudiConfigs) -> Result<MergeMode> {
    if let Some(mode) = hudi_configs.as_options().get(RECORD_MERGE_MODE) {
        log::debug!("merge mode {mode}: stated by the table");
        return match mode.to_ascii_uppercase().as_str() {
            "COMMIT_TIME_ORDERING" => Ok(MergeMode::CommitTimeOrdering),
            "EVENT_TIME_ORDERING" => Ok(MergeMode::EventTimeOrdering),
            // A CUSTOM table is served only when its payload class names a merger
            // this crate implements. The strategy id cannot make this call: every
            // payload-based custom table carries the same all-zeros sentinel.
            "CUSTOM" if resolve_custom_merger(&hudi_configs.as_options()).is_some() => {
                Ok(MergeMode::Custom)
            }
            other => Err(CoreError::Unsupported(format!(
                "Record merge mode '{other}' is not supported. Set \
                 hoodie.read.file.group.reader.version=1 to read with the \
                 reader that served this table before"
            ))),
        };
    }

    match infer_merge_mode(hudi_configs)? {
        InferredMode::CommitTime => Ok(MergeMode::CommitTimeOrdering),
        InferredMode::EventTime => Ok(MergeMode::EventTimeOrdering),
        // A table with its own merger cannot be reproduced by merging on key and
        // ordering value alone. Engines that know better — gluten remaps a
        // Debezium payload to event-time ordering, having injected the delete
        // marker its merge needs) says so by setting the merge mode outright,
        // which is handled above. Inferring the same thing here would read such
        // a table without those configs and drop its deletes silently.
        // A payload this crate implements a merger for is reproducible, so it is
        // served rather than refused. Anything else keeps the refusal.
        InferredMode::Custom if resolve_custom_merger(&hudi_configs.as_options()).is_some() => {
            Ok(MergeMode::Custom)
        }
        InferredMode::Custom => Err(CoreError::Unsupported(
            "This table merges with a merger of its own, which the merge-on-read \
             reader cannot reproduce. Set hoodie.read.file.group.reader.version=1 \
             to read with the reader that served it before, which merges without \
             that merger"
                .to_string(),
        )),
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

    /// [`resolve_instant_range`] deliberately reproduces the bounds version 1
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
    fn instant_range_matches_file_group_reader_version_one() {
        let mut options = minimal_configs();
        options.push((
            HudiReadConfig::StartTimestamp.as_ref().to_string(),
            "20230101000000000".to_string(),
        ));
        let configs = HudiConfigs::new(options);

        let version_one = FileGroupReader::new_with_overrides(
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

        assert_eq!(format!("{version_one:?}"), format!("{resolved:?}"));
    }

    #[test]
    fn resolves_table_path_from_base_path_config() {
        let configs = HudiConfigs::new(minimal_configs());

        let ctx = resolve_reader_context(&configs, false).unwrap();

        assert_eq!(ctx.table_path, "file:///tmp/t");
    }

    /// Regression test: a slice with no log files consults no merger, so a merge
    /// mode this reader cannot serve must not refuse it:
    /// `version_two_unsupported_reason` deliberately serves such reads.
    #[test]
    fn resolves_a_base_only_slice_of_a_custom_merge_table() {
        let mut options = minimal_configs();
        options.push(("hoodie.record.merge.mode".to_string(), "CUSTOM".to_string()));
        let ctx = resolve_reader_context(&HudiConfigs::new(options), false).unwrap();
        assert_eq!(ctx.merge_mode, "COMMIT_TIME_ORDERING");
    }

    /// The same table is still refused once log files make a merge real, and
    /// the error names the version that reads it.
    #[test]
    fn refuses_a_merging_slice_of_a_custom_merge_table_naming_the_way_back() {
        let mut options = minimal_configs();
        options.push(("hoodie.record.merge.mode".to_string(), "CUSTOM".to_string()));
        let err = resolve_reader_context(&HudiConfigs::new(options), true).unwrap_err();
        assert!(
            err.to_string()
                .contains("hoodie.read.file.group.reader.version=1"),
            "the error must name the way back, got: {err}"
        );
    }

    /// A custom payload class infers a CUSTOM merge, which follows the same
    /// rule: served with nothing to merge, refused with log files to merge.
    #[test]
    fn custom_payload_class_follows_the_same_no_merge_rule() {
        let mut options = minimal_configs();
        options.push((
            "hoodie.compaction.payload.class".to_string(),
            "com.example.MyPayload".to_string(),
        ));
        let configs = HudiConfigs::new(options);
        let ctx = resolve_reader_context(&configs, false).unwrap();
        assert_eq!(ctx.merge_mode, "COMMIT_TIME_ORDERING");

        let err = resolve_reader_context(&configs, true).unwrap_err();
        assert!(
            err.to_string()
                .contains("hoodie.read.file.group.reader.version=1"),
            "the error must name the way back, got: {err}"
        );
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

    /// The log scan is bounded by the same window version 1 uses, so a version 2
    /// read sees exactly the log blocks a version 1 read would.
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

    /// Regression test: every per-read key Hudi spells without the `hoodie.read.`
    /// prefix must still reach the reader config.
    ///
    /// The split used to key on that prefix alone, so the merge budget, the
    /// spill directory, the hard memory cap and the merge type all landed in the
    /// table properties and read as unset — the memory limits were unreachable
    /// from configuration, and `skip_merge` was undetected so an unmerged read
    /// silently returned merged output instead of the unsupported-mode error.
    ///
    /// This asserts through `resolve_reader_context` on purpose: the buffer tests
    /// insert straight into `hoodie_reader_config`, which proves the map honors
    /// the key but not that the key ever arrives.
    #[test]
    fn routes_the_non_read_prefixed_per_read_keys_to_the_reader_config() {
        let mut options = minimal_configs();
        let expected = [
            (spillable_map::CONFIG_MERGE_MAX_SIZE, "104857600"),
            (spillable_map::CONFIG_MAX_PEAK_MEMORY, "209715200"),
            (spillable_map::CONFIG_SPILLABLE_MAP_PATH, "/scratch/spill"),
            (spillable_map::CONFIG_DISKMAP_TYPE, "ROCKS_DB"),
            (CONFIG_MERGE_TYPE, "skip_merge"),
        ];
        for (key, value) in expected {
            options.push((key.to_string(), value.to_string()));
        }
        let configs = HudiConfigs::new(options);

        let ctx = resolve_reader_context(&configs, true).unwrap();

        for (key, value) in expected {
            assert_eq!(
                ctx.hoodie_reader_config.get(key),
                Some(&value.to_string()),
                "{key} is a per-read override and must reach the reader config"
            );
            assert!(
                !ctx.table_config.contains_key(key),
                "{key} is not something the table declares about itself"
            );
        }

        // And the spill config the buffer builds from that map must carry the
        // configured values, not its defaults — this is what was broken.
        let spill = spillable_map::SpillConfig::from_config(&ctx.hoodie_reader_config).unwrap();
        assert_eq!(
            spill.max_in_memory_size,
            (104857600.0 * spillable_map::SPILL_TRIGGER_FRACTION) as u64,
            "the configured merge budget must be honored"
        );
        assert_eq!(spill.spill_path, std::path::PathBuf::from("/scratch/spill"));
        assert_eq!(spill.max_peak_in_memory_size, Some(209715200));
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

    /// Position-based merge is off unless asked for, so a v2 read merges by key
    /// exactly as a version 1 read does by default.
    #[test]
    fn leaves_position_based_merge_off() {
        let configs = HudiConfigs::new(minimal_configs());

        let ctx = resolve_reader_context(&configs, true).unwrap();

        assert!(!ctx.should_merge_use_record_position);
    }

    #[test]
    fn turns_position_based_merge_on_from_hudis_own_config_key() {
        let mut options = minimal_configs();
        options.push((
            HudiReadConfig::MergeUseRecordPositions.as_ref().to_string(),
            "true".to_string(),
        ));

        let ctx = resolve_reader_context(&HudiConfigs::new(options), true).unwrap();

        assert!(ctx.should_merge_use_record_position);
    }

    /// A value that is not a boolean fails the read rather than falling back to
    /// the default — a typo would otherwise merge by key and return rows that
    /// look ordinary, which is the one outcome this setting exists to change.
    #[test]
    fn rejects_a_non_boolean_position_merge_setting() {
        let mut options = minimal_configs();
        options.push((
            HudiReadConfig::MergeUseRecordPositions.as_ref().to_string(),
            "yes".to_string(),
        ));

        let err = resolve_reader_context(&HudiConfigs::new(options), true).unwrap_err();

        assert!(
            err.to_string()
                .contains("hoodie.merge.use.record.positions"),
            "the error should name the offending key, got: {err}"
        );
    }

    /// Defaults chosen to match what version 1 returns today: deletes
    /// are applied but never emitted, output keeps merge order, and only
    /// completed instants are read.
    #[test]
    fn defaults_reader_parameters_to_version_one_behavior() {
        let params = ReaderParameters::default();

        assert!(!params.emit_delete);
        assert!(!params.sort_output);
        assert!(!params.allow_inflight_instants);
    }

    /// An ordering field and nothing else resolves to event-time ordering,
    /// through the whole `resolve_reader_context` build rather than through
    /// `infer_merge_mode` alone — the payload-class and strategy-id mappings are
    /// `infers_the_mode_java_infers`'s job, and this config sets neither.
    #[test]
    fn resolves_event_time_ordering_from_an_ordering_field() {
        let configs = HudiConfigs::new(minimal_configs());

        let ctx = resolve_reader_context(&configs, true).unwrap();

        assert_eq!(ctx.merge_mode, MergeMode::EventTimeOrdering.as_ref());
    }

    /// With nothing else set, the ordering field decides — and its absence
    /// means commit-time ordering, not "cannot merge".
    ///
    /// This is the case that refused every v6 table: the strategy this crate
    /// used to derive has no counterpart in Hudi's merge modes, and the reader
    /// rejected it. Java infers commit-time ordering here, which is a table this
    /// reader can serve.
    #[test]
    fn infers_commit_time_ordering_without_an_ordering_field() {
        let configs = configs_without(HudiTableConfig::OrderingFields.as_ref());

        let ctx = resolve_reader_context(&configs, true).unwrap();

        assert_eq!(ctx.merge_mode, MergeMode::CommitTimeOrdering.as_ref());
    }

    /// Whether meta fields are populated says nothing about how records merge.
    /// A virtual-key table with an ordering field is ordered by event time like
    /// any other.
    #[test]
    fn infers_event_time_ordering_for_a_virtual_key_table() {
        let mut options = minimal_configs();
        options.push((
            HudiTableConfig::PopulatesMetaFields.as_ref().to_string(),
            "false".to_string(),
        ));
        let configs = HudiConfigs::new(options);

        let ctx = resolve_reader_context(&configs, true).unwrap();

        assert_eq!(ctx.merge_mode, MergeMode::EventTimeOrdering.as_ref());
    }

    /// The inputs Hudi actually writes, and what each implies. Mirrors the cases
    /// Java's own inference is tested against.
    #[test]
    fn infers_the_mode_java_infers() {
        // (payload class, strategy id, ordering field, table version, expected)
        let cases: Vec<(&str, &str, Option<&str>, &str, InferredMode)> = vec![
            // nothing set — the ordering field decides
            ("", "", None, "6", InferredMode::CommitTime),
            ("", "", Some("ts"), "6", InferredMode::EventTime),
            // built-in payload classes
            (
                "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
                "",
                None,
                "6",
                InferredMode::EventTime,
            ),
            (
                "org.apache.hudi.common.model.EventTimeAvroPayload",
                "",
                None,
                "6",
                InferredMode::EventTime,
            ),
            (
                "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
                "",
                None,
                "6",
                InferredMode::CommitTime,
            ),
            // a payload class of the table's own
            ("com.example.MyPayload", "", None, "6", InferredMode::Custom),
            // built-in strategy ids
            (
                "",
                EVENT_TIME_STRATEGY_ID,
                None,
                "8",
                InferredMode::EventTime,
            ),
            (
                "",
                COMMIT_TIME_STRATEGY_ID,
                None,
                "8",
                InferredMode::CommitTime,
            ),
            // the payload-based sentinel is a merger of the table's own
            (
                "",
                "00000000-0000-0000-0000-000000000000",
                None,
                "8",
                InferredMode::Custom,
            ),
            // from version 8 the strategy id wins over the payload class
            (
                "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
                EVENT_TIME_STRATEGY_ID,
                None,
                "8",
                InferredMode::EventTime,
            ),
            // before it, the payload class does
            (
                "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
                EVENT_TIME_STRATEGY_ID,
                None,
                "6",
                InferredMode::CommitTime,
            ),
        ];

        for (payload, strategy, ordering, version, expected) in cases {
            let mut options = vec![
                (
                    HudiTableConfig::BasePath.as_ref().to_string(),
                    "file:///tmp/t".to_string(),
                ),
                (
                    HudiTableConfig::TableVersion.as_ref().to_string(),
                    version.to_string(),
                ),
            ];
            if !payload.is_empty() {
                options.push((PAYLOAD_CLASS_KEYS[0].to_string(), payload.to_string()));
            }
            if !strategy.is_empty() {
                options.push((
                    RECORD_MERGE_STRATEGY_ID_KEYS[0].to_string(),
                    strategy.to_string(),
                ));
            }
            if let Some(field) = ordering {
                options.push((
                    HudiTableConfig::OrderingFields.as_ref().to_string(),
                    field.to_string(),
                ));
            }

            let inferred = infer_merge_mode(&HudiConfigs::new(options)).unwrap();
            assert_eq!(
                inferred, expected,
                "payload={payload:?} strategy={strategy:?} ordering={ordering:?} version={version}"
            );
        }
    }

    /// A real table this reader used to refuse now resolves.
    ///
    /// Its meta fields are off, which the previous rule read as "append only" —
    /// a strategy with no merge-mode counterpart, so the read was rejected.
    /// Nothing about meta fields says how records merge; its payload class does,
    /// and it says commit-time ordering.
    #[tokio::test]
    async fn resolves_a_v6_table_that_was_refused_before() {
        use hudi_test::SampleTable;
        let url = SampleTable::V6SimplekeygenHivestyleNoMetafields.url_to_mor_parquet();
        let table = crate::table::Table::new(url.as_ref()).await.unwrap();

        let mut options = table.hudi_configs.as_options();
        options.insert(
            "hoodie.read.end.timestamp".to_string(),
            "99991231235959999".to_string(),
        );
        let ctx = resolve_reader_context(&HudiConfigs::new(options), true).unwrap();

        assert_eq!(ctx.merge_mode, MergeMode::CommitTimeOrdering.as_ref());
    }

    /// A table with its own merger is refused rather than merged as if it had
    /// none. An engine that knows the merge is reproducible says so by setting
    /// the mode outright, which is read before any of this runs.
    #[test]
    fn refuses_a_table_with_its_own_merger() {
        let mut options = minimal_configs();
        options.push((
            PAYLOAD_CLASS_KEYS[0].to_string(),
            "com.example.MyPayload".to_string(),
        ));
        let configs = HudiConfigs::new(options);

        let err = resolve_reader_context(&configs, true).unwrap_err();

        assert!(
            err.to_string().contains("merger of its own"),
            "error should say why, got: {err}"
        );
    }
}
