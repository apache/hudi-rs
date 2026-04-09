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
use crate::timeline::selector::InstantRange;
use super::record_context::RecordContext;
use std::collections::HashMap;

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
/// | `metaClient.getTableConfig()` (config map)  | `table_config`                          |
/// | `props` (hoodie reader config overrides)    | `hoodie_reader_config`                  |
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
    /// Record key field name (e.g. `_hoodie_record_key`).
    pub record_key_field: String,
    pub table_config: HashMap<String, String>,
    pub hoodie_reader_config: HashMap<String, String>,
}

impl ReaderContext {
    /// Get the record context for record-level operations.
    ///
    /// Mirrors Java's `HoodieReaderContext.getRecordContext()`.
    ///
    /// In Java, `RecordContext<T>` is engine-specific (Spark, Flink).
    /// In Rust/Arrow, there is a single concrete `RecordContext`.
    pub fn get_record_context(&self) -> RecordContext {
        RecordContext
    }

    /// Get the timeline timezone from table config, defaulting to "utc".
    pub fn timezone(&self) -> String {
        self.table_config
            .get(HudiTableConfig::TimelineTimezone.as_ref())
            .cloned()
            .unwrap_or_else(|| "utc".to_string())
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
            record_key_field: "_hoodie_record_key".to_string(),
            table_config: HashMap::new(),
            hoodie_reader_config: HashMap::new(),
        }
    }
}
