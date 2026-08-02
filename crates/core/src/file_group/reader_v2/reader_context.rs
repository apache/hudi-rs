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

//! Ported from the merge-on-read reader. Nothing consumes it yet, so its
//! items are unreachable from the crate's call graph until the reader wires in.
#![allow(dead_code)]

//! Mirrors Java's `HoodieReaderContext<T>` — the engine-agnostic reader context
//! that flows through the entire file group reader call stack.
//!
//! In Java, `HoodieReaderContext<T>` is an engine-specific object (Spark, Flink)
//! that carries merge mode, instant range, schema handler, etc. In Rust, we work
//! directly with Arrow `RecordBatch`es, so this is a plain data struct carrying
//! the structured configuration that the reader stack needs.

use super::record_context::RecordContext;
use super::schema_handler::FileGroupReaderSchemaHandler;
use crate::config::table::HudiTableConfig;
use crate::storage::RowFilterBuilder;
use crate::timeline::selector::InstantRange;
use std::collections::{HashMap, HashSet};

/// Owned inputs for the Gate-3 completed/inflight check (C-INFLIGHT-DELTA).
///
/// Mirrors the SET check Java's `BaseHoodieLogRecordReader.scanInternalV1` performs for
/// `tableVersion < 8`: a data/delete block is skipped when its instant is in
/// `filterInflights()` OR not in `filterCompletedInstants()` (with archived instants treated as
/// committed). Held by the reader so it can be built once from the active timeline and borrowed for
/// each scan.
///
/// The gate is only meaningful for tables with a v1 timeline layout (table version < 8). For v8+
/// tables the per-delta-commit log files are already excluded at the completion-time file-slice
/// level, so callers leave this `None` (see `forward_scan_pass1`), preserving prior behavior.
///
/// Lives here rather than with the scan that consumes it: [`ReaderContext`] holds it as a field, so
/// defining it alongside the scanner would make the context and the scanner import each other.
#[derive(Debug, Clone, Default)]
pub struct CompletionGateInputs {
    /// Active completed-commit instant times (Java `filterCompletedInstants()`).
    pub completed_instants: HashSet<String>,
    /// Active inflight/requested instant times (Java `filterInflights()`).
    pub inflight_instants: HashSet<String>,
    /// First active-timeline instant. An instant strictly before it is archived; archival only
    /// removes completed instants, so an archived instant is committed. `None` disables the
    /// archived fallback (every candidate must then be in `completed_instants`).
    pub archived_boundary: Option<String>,
}
/// Reader context that flows through the file group reader call stack.
///
/// Mirrors Java's `HoodieReaderContext<T>`, carrying structured reader
/// configuration instead of raw config key-value maps.
///
/// ## Java counterpart
///
/// | Java field / method                        | Rust field                              |
/// |--------------------------------------------|-----------------------------------------|
/// | `readerContext.getTablePath()`              | `table_path`                            |
/// | `readerContext.getLatestCommitTime()`       | `latest_commit_time`                    |
/// | `readerContext.getMergeMode()`              | `merge_mode`                            |
/// | `readerContext.getInstantRange()`           | `instant_range`                         |
/// | `readerContext.getRecordContext().format()`  | `base_file_format`                      |
/// | `readerContext.getHasLogFiles()`            | `has_log_files`                         |
/// | `readerContext.getRecordContext()`          | `record_context`                        |
/// | `readerContext.getSchemaHandler()`          | `schema_handler`                        |
/// | `metaClient.getTableConfig()` (config map)  | `table_config`                          |
/// | `props` (hoodie reader config overrides)    | `hoodie_reader_config`                  |
#[derive(Clone)]
pub struct ReaderContext {
    pub table_path: String,
    pub latest_commit_time: String,
    pub base_file_format: String,
    pub has_log_files: bool,
    pub has_bootstrap_base_file: bool,
    pub needs_bootstrap_merge: bool,
    pub should_merge_use_record_position: bool,
    pub enable_logical_timestamp_field_repair: bool,
    pub iterator_mode: String,
    pub merge_mode: String,
    pub merge_strategy_id: String,
    pub instant_range: Option<InstantRange>,
    /// The engine-specific record context for record-level operations.
    ///
    /// Mirrors Java's `HoodieReaderContext.recordContext` field.
    /// In Java this is a persistent mutable field set at construction and
    /// shared across all consumers. In Rust it is set once and shared via
    /// `Arc<ReaderContext>`.
    ///
    /// Constructed from `table_config` + `partition_path`, mirroring Java's
    /// `RecordContext(tableConfig, typeConverter)`.
    pub record_context: RecordContext,
    /// Schema management for the read pipeline.
    ///
    /// Mirrors Java's `HoodieReaderContext.schemaHandler` field
    /// (`FileGroupReaderSchemaHandler<T>`).
    pub schema_handler: FileGroupReaderSchemaHandler,
    pub table_config: HashMap<String, String>,
    pub hoodie_reader_config: HashMap<String, String>,
    /// Optional parquet `RowFilter` builder for predicate pushdown into base
    /// parquet files and parquet-format log blocks. Set by the FFI bridge from
    /// the decoded substrait predicate; `None` when no predicate was pushed.
    ///
    /// Whether it actually gets installed is gated by [`Self::mor_pk_safe`] +
    /// table type (see callers in `file_group::reader::HoodieFileGroupReader`
    /// and `file_group::log_file::content::Decoder`).
    pub row_filter_builder: Option<RowFilterBuilder>,
    /// True when [`Self::row_filter_builder`] references only primary-key
    /// columns and is therefore safe to push into MERGE_ON_READ base + log
    /// readers. Mirrors Java's `morFilters` filter set, which is computed via
    /// `filterIsSafeForPrimaryKey` and applied to both base parquet files and
    /// parquet log blocks via the same `readerContext.getFileRecordIterator`
    /// entry point.
    ///
    /// On COPY_ON_WRITE tables this flag is irrelevant — all filters push
    /// because no merge happens. On MERGE_ON_READ, only filters with this
    /// flag true are pushed; filters touching non-PK columns wait for
    /// post-merge evaluation (handled by the caller — Velox/Spark — above
    /// the FG reader).
    pub mor_pk_safe: bool,
    /// C-INFLIGHT-DELTA Gate-3 inputs (completed/inflight/archived sets).
    /// Carried here mirroring [`Self::instant_range`]; `Some` only for table
    /// version < 8 snapshot reads (set by the FFI bridge when the planner
    /// enabled the gate), `None` otherwise (v8+/incremental/no-timeline) which
    /// leaves Gate 3 a no-op, preserving prior behavior. Consumed by the log
    /// scan builder in `buffer::loader::scan_log_files`.
    pub completion_gate_inputs: Option<CompletionGateInputs>,
}

// Manual Debug — RowFilterBuilder is a closure (Arc<dyn Fn ...>), not Debug.
// We elide it the same way `ParquetReadOptions` does in `storage::mod`.
impl std::fmt::Debug for ReaderContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReaderContext")
            .field("table_path", &self.table_path)
            .field("latest_commit_time", &self.latest_commit_time)
            .field("base_file_format", &self.base_file_format)
            .field("has_log_files", &self.has_log_files)
            .field("has_bootstrap_base_file", &self.has_bootstrap_base_file)
            .field("needs_bootstrap_merge", &self.needs_bootstrap_merge)
            .field(
                "should_merge_use_record_position",
                &self.should_merge_use_record_position,
            )
            .field(
                "enable_logical_timestamp_field_repair",
                &self.enable_logical_timestamp_field_repair,
            )
            .field("iterator_mode", &self.iterator_mode)
            .field("merge_mode", &self.merge_mode)
            .field("merge_strategy_id", &self.merge_strategy_id)
            .field("instant_range", &self.instant_range)
            .field("record_context", &self.record_context)
            .field("schema_handler", &self.schema_handler)
            .field("table_config", &self.table_config)
            .field("hoodie_reader_config", &self.hoodie_reader_config)
            .field(
                "row_filter_builder",
                &self.row_filter_builder.as_ref().map(|_| "<closure>"),
            )
            .field("mor_pk_safe", &self.mor_pk_safe)
            .field("completion_gate_inputs", &self.completion_gate_inputs)
            .finish()
    }
}

impl ReaderContext {
    /// Get the record context for record-level operations.
    ///
    /// Mirrors Java's `HoodieReaderContext.getRecordContext()`.
    ///
    /// In Java, `RecordContext<T>` is a persistent field on the reader context,
    /// shared across all consumers. In Rust, it is stored as a field and
    /// returned by reference.
    pub fn get_record_context(&self) -> &RecordContext {
        &self.record_context
    }

    /// Convenience accessor — mirrors Java's
    /// `readerContext.getRecordContext().recordKeyField`.
    pub fn record_key_field(&self) -> &str {
        &self.record_context.record_key_field
    }

    /// Return ALL record key field names for schema computation.
    ///
    /// Mirrors Java's `getMandatoryFieldsForMerging()` (lines 250-258):
    /// ```java
    /// if (cfg.populateMetaFields()) {
    ///     requiredFields.add(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    /// } else {
    ///     requiredFields.addAll(Arrays.asList(cfg.getRecordKeyFields().get()));
    /// }
    /// ```
    ///
    /// When `populateMetaFields` is true, returns `["_hoodie_record_key"]`.
    /// When false (virtual keys), returns ALL configured record key fields
    /// (supports composite keys like `["pk1", "pk2"]`).
    ///
    /// This differs from `record_key_field()` which returns only the first
    /// field (used for per-record key extraction in the buffer).
    pub fn record_key_fields(&self) -> Vec<String> {
        Self::record_key_fields_from(&self.table_config)
    }

    /// Static version of [`Self::record_key_fields`]. Used at FFI entry when
    /// a `ReaderContext` doesn't yet exist (we need PK fields to decide
    /// `mor_pk_safe` before constructing the context).
    pub fn record_key_fields_from(table_config: &HashMap<String, String>) -> Vec<String> {
        // Delegate to the canonical parse on RecordContext so the two types can never
        // disagree on the key-field list.
        RecordContext::record_key_fields_from(table_config)
    }

    /// Convenience accessor — mirrors Java's ordering field names used by
    /// the merge pipeline.
    pub fn ordering_field_names(&self) -> &[String] {
        &self.record_context.ordering_field_names
    }

    /// Convenience accessor for the partition path stored on record_context.
    pub fn partition_path(&self) -> &str {
        &self.record_context.partition_path
    }

    /// Get the timeline timezone from table config, defaulting to "utc".
    pub fn timezone(&self) -> String {
        self.table_config
            .get(HudiTableConfig::TimelineTimezone.as_ref())
            .cloned()
            .unwrap_or_else(|| "utc".to_string())
    }

    /// Rebuild the stored `record_context` from the current `table_config`
    /// and the given `partition_path`.
    ///
    /// Use this after mutating `table_config` to keep `record_context` in sync.
    pub fn rebuild_record_context(&mut self, partition_path: String) {
        self.record_context = RecordContext::new(&self.table_config, partition_path);
    }

    /// Create an empty reader context (for legacy/test code).
    pub fn empty() -> Self {
        Self {
            table_path: String::new(),
            latest_commit_time: String::new(),
            base_file_format: String::new(),
            has_log_files: false,
            has_bootstrap_base_file: false,
            needs_bootstrap_merge: false,
            should_merge_use_record_position: false,
            enable_logical_timestamp_field_repair: false,
            iterator_mode: String::new(),
            merge_mode: String::new(),
            merge_strategy_id: String::new(),
            instant_range: None,
            record_context: RecordContext::default(),
            schema_handler: FileGroupReaderSchemaHandler::new(),
            table_config: HashMap::new(),
            hoodie_reader_config: HashMap::new(),
            row_filter_builder: None,
            mor_pk_safe: false,
            completion_gate_inputs: None,
        }
    }

    /// Returns true iff the table is COPY_ON_WRITE per `hoodie.table.type`.
    /// Defaults to `false` (treat as MOR) on missing/unparseable values so
    /// callers err on the side of NOT pushing predicates down.
    pub fn is_cow(&self) -> bool {
        use crate::config::table::TableTypeValue;
        use std::str::FromStr;
        self.table_config
            .get("hoodie.table.type")
            .and_then(|v| TableTypeValue::from_str(v).ok())
            .map(|t| matches!(t, TableTypeValue::CopyOnWrite))
            .unwrap_or(false)
    }

    /// Returns true iff this context allows installing the parquet `RowFilter`
    /// for the current scan. Either the table is CoW (the merge can't flip
    /// predicate outcomes) or the filter is PK-safe (PKs are immutable across
    /// upserts, so the predicate's outcome is stable across the base+log
    /// merge). Mirrors the gate Java applies via the `morFilters`/`allFilters`
    /// selection in `SparkFileFormatInternalRowReaderContext`.
    pub fn can_push_row_filter(&self) -> bool {
        self.is_cow() || self.mor_pk_safe
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_with_table_type(t: &str) -> ReaderContext {
        let mut ctx = ReaderContext::empty();
        ctx.table_config
            .insert("hoodie.table.type".to_string(), t.to_string());
        ctx
    }

    #[test]
    fn is_cow_true_for_copy_on_write() {
        assert!(ctx_with_table_type("COPY_ON_WRITE").is_cow());
    }

    #[test]
    fn is_cow_false_for_merge_on_read() {
        assert!(!ctx_with_table_type("MERGE_ON_READ").is_cow());
    }

    #[test]
    fn is_cow_false_when_table_type_missing() {
        // Defaults to MOR (conservative) so non-PK predicates don't sneak in.
        assert!(!ReaderContext::empty().is_cow());
    }

    #[test]
    fn can_push_row_filter_cow_unconditionally() {
        // CoW: pushdown is always safe regardless of mor_pk_safe.
        let mut ctx = ctx_with_table_type("COPY_ON_WRITE");
        ctx.mor_pk_safe = false;
        assert!(ctx.can_push_row_filter());
        ctx.mor_pk_safe = true;
        assert!(ctx.can_push_row_filter());
    }

    #[test]
    fn can_push_row_filter_mor_only_when_pk_safe() {
        let mut ctx = ctx_with_table_type("MERGE_ON_READ");
        ctx.mor_pk_safe = false;
        assert!(!ctx.can_push_row_filter());
        ctx.mor_pk_safe = true;
        assert!(ctx.can_push_row_filter());
    }

    #[test]
    fn record_key_fields_from_default_meta_field_mode() {
        // Default (populate.meta.fields absent → true): record_key_fields()
        // returns the meta-field name regardless of recordkey.fields setting.
        let mut tc = HashMap::new();
        tc.insert(
            "hoodie.table.recordkey.fields".to_string(),
            "id".to_string(),
        );
        let fields = ReaderContext::record_key_fields_from(&tc);
        assert_eq!(fields, vec!["_hoodie_record_key".to_string()]);
    }

    #[test]
    fn record_key_fields_from_virtual_key_mode_single() {
        let mut tc = HashMap::new();
        tc.insert(
            "hoodie.populate.meta.fields".to_string(),
            "false".to_string(),
        );
        tc.insert(
            "hoodie.table.recordkey.fields".to_string(),
            "id".to_string(),
        );
        let fields = ReaderContext::record_key_fields_from(&tc);
        assert_eq!(fields, vec!["id".to_string()]);
    }

    #[test]
    fn record_key_fields_from_virtual_key_mode_composite() {
        // Composite PK — comma-separated, with optional whitespace.
        let mut tc = HashMap::new();
        tc.insert(
            "hoodie.populate.meta.fields".to_string(),
            "false".to_string(),
        );
        tc.insert(
            "hoodie.table.recordkey.fields".to_string(),
            "id, ts".to_string(),
        );
        let fields = ReaderContext::record_key_fields_from(&tc);
        assert_eq!(fields, vec!["id".to_string(), "ts".to_string()]);
    }
}

/// How the reader picks a winner among versions of the same record key.
///
/// The resolver works in this type and converts to the string form
/// [`ReaderContext::merge_mode`] carries, which matches the merge modes the
/// Hudi Java reader names.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MergeMode {
    /// Latest commit wins.
    ///
    /// Nothing selects this yet. A table with no ordering field derives
    /// `append_only`, which the resolver rejects rather than mapping — see
    /// `resolver::resolve_merge_mode`.
    CommitTimeOrdering,
    /// Highest ordering-field value wins.
    EventTimeOrdering,
}

impl AsRef<str> for MergeMode {
    fn as_ref(&self) -> &str {
        match self {
            MergeMode::CommitTimeOrdering => "COMMIT_TIME_ORDERING",
            MergeMode::EventTimeOrdering => "EVENT_TIME_ORDERING",
        }
    }
}
