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

//! Mirrors Java's `HoodieReaderContext<T>` — the engine-agnostic reader context
//! that flows through the entire file group reader call stack.
//!
//! In Java, `HoodieReaderContext<T>` is an engine-specific object (Spark, Flink)
//! that carries merge mode, instant range, schema handler, etc. In Rust, we work
//! directly with Arrow `RecordBatch`es, so this is a plain data struct carrying
//! the structured configuration that the reader stack needs.

use crate::config::table::HudiTableConfig;
use crate::expression::predicate::Predicate;
use crate::timeline::selector::InstantRange;
use super::record_context::RecordContext;
use super::schema_handler::FileGroupReaderSchemaHandler;
use std::collections::HashMap;
use std::sync::Arc;

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
/// | `readerContext.getKeyFilterOpt()`           | `key_filter_opt`                        |
#[derive(Debug, Clone)]
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
    /// Mirrors Java `HoodieReaderContext.keyFilterOpt` — an optional
    /// predicate (typically `In` or `StringStartsWithAny`) used to
    /// narrow which records are scanned. None by default. See spec §5.1.
    pub key_filter_opt: Option<Arc<dyn Predicate>>,
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
        let populate = self
            .table_config
            .get(HudiTableConfig::PopulatesMetaFields.as_ref())
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        if populate {
            vec![crate::metadata::meta_field::MetaField::RecordKey
                .as_ref()
                .to_string()]
        } else {
            self.table_config
                .get(HudiTableConfig::RecordKeyFields.as_ref())
                .map(|fields| {
                    fields
                        .split(',')
                        .map(|f| f.trim().to_string())
                        .collect()
                })
                .unwrap_or_else(|| {
                    vec![crate::metadata::meta_field::MetaField::RecordKey
                        .as_ref()
                        .to_string()]
                })
        }
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

    /// Mirrors Java `HoodieReaderContext.getKeyFilterOpt()`.
    pub fn get_key_filter_opt(&self) -> Option<Arc<dyn Predicate>> {
        self.key_filter_opt.clone()
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
            key_filter_opt: None,
        }
    }
}

#[cfg(test)]
mod key_filter_opt_tests {
    use super::*;
    use crate::expression::predicates::predicates_factory;
    use crate::expression::{Expression, Literal, NameReference};
    use std::sync::Arc;

    #[test]
    fn empty_reader_context_has_no_key_filter() {
        let ctx = ReaderContext::empty();
        assert!(ctx.get_key_filter_opt().is_none());
    }

    #[test]
    fn key_filter_opt_can_be_set_and_retrieved() {
        let mut ctx = ReaderContext::empty();
        let pred = predicates_factory::in_(
            Box::new(NameReference::new("_hoodie_record_key")),
            vec![Box::new(Literal::string("k1")) as Box<dyn Expression>],
        );
        ctx.key_filter_opt = Some(Arc::new(pred));
        assert!(ctx.get_key_filter_opt().is_some());
    }
}
