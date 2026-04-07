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

//! Rich Rust types that mirror the Substrait `HudiReadOptions` proto hierarchy
//! 1-to-1.  The FFI layer ([`crate::ffi::FfiReaderContext`]) is a flat CXX
//! struct; this module reconstructs the proper nesting using [`Option`],
//! [`Vec`], [`HashMap`], and [`Box`].
//!
//! ## Proto → Rust mapping
//!
//! | Proto message                                     | Rust struct                   |
//! |---------------------------------------------------|-------------------------------|
//! | `HudiReadOptions.HoodieFileGroupReaderContext`    | [`FileGroupReaderContext`]    |
//! | `HudiReadOptions.BaseFile`                        | [`BaseFile`]                  |
//! | `HudiReadOptions.LogFile`                         | [`LogFile`]                   |
//! | `HudiReadOptions.HoodieSchema`                    | [`HoodieSchema`]              |
//! | `HudiReadOptions.InstantRange`                    | [`InstantRange`]              |
//! | `HudiReadOptions.ReaderContext`                   | [`ReaderContext`]             |

use crate::ffi::FfiReaderContext;
use std::collections::HashMap;

// ── Proto message: BaseFile ─────────────────────────────────────────────────

/// Mirrors `HudiReadOptions.BaseFile` proto.
///
/// ```proto
/// message BaseFile {
///   string path = 1;
///   string file_name = 2;
///   int64  file_size = 3;
///   string file_id = 4;
///   string commit_time = 5;
///   BaseFile bootstrap_base_file = 6;  // recursive
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct BaseFile {
    pub path: String,
    pub file_name: String,
    pub file_size: i64,
    pub file_id: String,
    pub commit_time: String,
    pub bootstrap_base_file: Option<Box<BaseFile>>,
}

// ── Proto message: LogFile ──────────────────────────────────────────────────

/// Mirrors `HudiReadOptions.LogFile` proto.
///
/// ```proto
/// message LogFile {
///   string path_str = 1;
///   string file_id = 2;
///   string delta_commit_time = 3;
///   int32  log_version = 4;
///   string log_write_token = 5;
///   string file_extension = 6;
///   string suffix = 7;
///   int64  file_size = 8;
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct LogFile {
    pub path_str: String,
    pub file_id: String,
    pub delta_commit_time: String,
    pub log_version: i32,
    pub log_write_token: String,
    pub file_extension: String,
    pub suffix: String,
    pub file_size: i64,
}

// ── Proto message: HoodieSchema ─────────────────────────────────────────────

/// Mirrors `HudiReadOptions.HoodieSchema` proto.
///
/// ```proto
/// message HoodieSchema {
///   string avro_schema_json = 1;
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct HoodieSchema {
    pub avro_schema_json: String,
}

// ── Proto message: InstantRange ─────────────────────────────────────────────

/// Mirrors `HudiReadOptions.InstantRange` proto.
///
/// ```proto
/// message InstantRange {
///   string start_instant = 1;
///   string end_instant = 2;
///   string range_type = 3;
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct InstantRange {
    pub start_instant: String,
    pub end_instant: String,
    pub range_type: String,
}

// ── Proto message: ReaderContext ─────────────────────────────────────────────

/// Mirrors `HudiReadOptions.ReaderContext` proto (inner hudi-rs reader context).
///
/// ```proto
/// message ReaderContext {
///   string table_path = 1;
///   string latest_commit_time = 2;
///   string base_file_format = 3;
///   bool   has_log_files = 4;
///   bool   has_bootstrap_base_file = 5;
///   bool   needs_bootstrap_merge = 6;
///   bool   should_merge_use_record_position = 7;
///   bool   enable_logical_timestamp_field_repair = 8;
///   string iterator_mode = 9;
///   string merge_mode = 10;
///   string merge_strategy_id = 11;
///   InstantRange instant_range = 12;
///   map<string, string> table_config = 13;
///   map<string, string> hoodie_reader_config = 14;
/// }
/// ```
#[derive(Debug, Clone, Default)]
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
    pub table_config: HashMap<String, String>,
    pub hoodie_reader_config: HashMap<String, String>,
}

// ── Proto message: HoodieFileGroupReaderContext ───────────────────────────────

/// Mirrors `HudiReadOptions.HoodieFileGroupReaderContext` proto (outer context).
///
/// ```proto
/// message HoodieFileGroupReaderContext {
///   string table_path = 1;
///   string partition_path = 2;
///   string latest_commit_time = 3;
///   int64  start = 4;
///   int64  length = 5;
///   bool   should_use_record_position = 6;
///   bool   allow_inflight_instants = 7;
///   bool   emit_delete = 8;
///   bool   sort_output = 9;
///   map<string, string> props = 10;
///   BaseFile base_file = 11;
///   repeated LogFile log_files = 12;
///   HoodieSchema data_schema = 13;
///   HoodieSchema requested_schema = 14;
///   ReaderContext reader_context = 15;
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct FileGroupReaderContext {
    // ── outer primitives ────────────────────────────────────────────────
    pub table_path: String,
    pub partition_path: String,
    pub latest_commit_time: String,
    pub start: i64,
    pub length: i64,
    pub should_use_record_position: bool,
    pub allow_inflight_instants: bool,
    pub emit_delete: bool,
    pub sort_output: bool,

    // ── props (TypedProperties → Map<String, String>) ───────────────────
    pub props: HashMap<String, String>,

    // ── base_file ────────────────────────────────────────────────────────
    pub base_file: Option<BaseFile>,

    // ── log_files ────────────────────────────────────────────────────────
    pub log_files: Vec<LogFile>,

    // ── schemas ──────────────────────────────────────────────────────────
    pub data_schema: Option<HoodieSchema>,
    pub requested_schema: Option<HoodieSchema>,

    // ── reader_context (inner hudi-rs ReaderContext) ─────────────────────
    pub reader_context: Option<ReaderContext>,
}

// ── Conversion: FfiReaderContext → FileGroupReaderContext ────────────────────

/// Zips parallel key/value vectors into a [`HashMap`].
fn zip_kv(keys: Vec<String>, values: Vec<String>) -> HashMap<String, String> {
    keys.into_iter().zip(values).collect()
}

impl From<FfiReaderContext> for FileGroupReaderContext {
    fn from(ffi: FfiReaderContext) -> Self {
        // ── inner ReaderContext (proto field 15) ─────────────────────────
        let reader_context = ReaderContext {
            table_path: ffi.table_path.clone(),
            latest_commit_time: ffi.latest_commit_time.clone(),
            base_file_format: ffi.base_file_format,
            has_log_files: ffi.has_log_files,
            has_bootstrap_base_file: ffi.has_bootstrap_base_file,
            needs_bootstrap_merge: ffi.needs_bootstrap_merge,
            should_merge_use_record_position: ffi.should_merge_use_record_position,
            enable_logical_timestamp_field_repair: ffi.enable_logical_timestamp_field_repair,
            iterator_mode: ffi.iterator_mode,
            merge_mode: ffi.merge_mode,
            merge_strategy_id: ffi.merge_strategy_id,
            instant_range: if ffi.has_instant_range {
                Some(InstantRange {
                    start_instant: ffi.instant_range_start,
                    end_instant: ffi.instant_range_end,
                    range_type: ffi.instant_range_type,
                })
            } else {
                None
            },
            table_config: zip_kv(ffi.table_config_keys, ffi.table_config_values),
            hoodie_reader_config: zip_kv(
                ffi.hoodie_reader_config_keys,
                ffi.hoodie_reader_config_values,
            ),
        };

        // ── base_file (proto field 11) ──────────────────────────────────
        let base_file = if ffi.has_base_file {
            let bootstrap = if ffi.base_file_has_bootstrap {
                Some(Box::new(BaseFile {
                    path: ffi.base_file_bootstrap_path,
                    file_name: ffi.base_file_bootstrap_file_name,
                    file_size: ffi.base_file_bootstrap_file_size,
                    file_id: ffi.base_file_bootstrap_file_id,
                    commit_time: ffi.base_file_bootstrap_commit_time,
                    bootstrap_base_file: None, // proto recursion stops at depth 1
                }))
            } else {
                None
            };
            Some(BaseFile {
                path: ffi.base_file_path,
                file_name: ffi.base_file_file_name,
                file_size: ffi.base_file_file_size,
                file_id: ffi.base_file_file_id,
                commit_time: ffi.base_file_commit_time,
                bootstrap_base_file: bootstrap,
            })
        } else {
            None
        };

        // ── log_files (proto field 12) ──────────────────────────────────
        let log_files: Vec<LogFile> = ffi
            .log_file_details
            .into_iter()
            .map(|lf| LogFile {
                path_str: lf.path_str,
                file_id: lf.file_id,
                delta_commit_time: lf.delta_commit_time,
                log_version: lf.log_version,
                log_write_token: lf.log_write_token,
                file_extension: lf.file_extension,
                suffix: lf.suffix,
                file_size: lf.file_size,
            })
            .collect();

        // ── schemas (proto fields 13–14) ────────────────────────────────
        let data_schema = if ffi.data_schema_json.is_empty() {
            None
        } else {
            Some(HoodieSchema {
                avro_schema_json: ffi.data_schema_json,
            })
        };
        let requested_schema = if ffi.requested_schema_json.is_empty() {
            None
        } else {
            Some(HoodieSchema {
                avro_schema_json: ffi.requested_schema_json,
            })
        };

        FileGroupReaderContext {
            // ── outer primitives ────────────────────────────────────────
            table_path: ffi.table_path,
            partition_path: ffi.partition_path.clone(),
            latest_commit_time: ffi.latest_commit_time,
            start: ffi.start,
            length: ffi.length,
            should_use_record_position: ffi.should_use_record_position,
            allow_inflight_instants: ffi.allow_inflight_instants,
            emit_delete: ffi.emit_delete,
            sort_output: ffi.sort_output,
            props: zip_kv(ffi.props_keys, ffi.props_values),
            base_file,
            log_files,
            data_schema,
            requested_schema,
            reader_context: Some(reader_context),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::{FfiLogFile, FfiReaderContext};

    #[test]
    fn test_empty_context_converts() {
        let ffi = FfiReaderContext::default();
        let ctx: FileGroupReaderContext = ffi.into();
        assert!(ctx.base_file.is_none());
        assert!(ctx.log_files.is_empty());
        assert!(ctx.data_schema.is_none());
        assert!(ctx.requested_schema.is_none());
        assert!(ctx.props.is_empty());
        let rc = ctx.reader_context.unwrap();
        assert!(rc.instant_range.is_none());
        assert!(rc.table_config.is_empty());
        assert!(rc.hoodie_reader_config.is_empty());
    }

    #[test]
    fn test_full_context_converts() {
        let ffi = FfiReaderContext {
            table_path: "s3://bucket/table".into(),
            latest_commit_time: "20240101120000000".into(),
            base_file_format: "PARQUET".into(),
            has_log_files: true,
            has_bootstrap_base_file: false,
            needs_bootstrap_merge: false,
            should_merge_use_record_position: true,
            enable_logical_timestamp_field_repair: false,
            iterator_mode: "ENGINE_RECORD".into(),
            merge_mode: "COMMIT_TIME_ORDERING".into(),
            merge_strategy_id: "".into(),
            has_instant_range: true,
            instant_range_start: "20240101000000000".into(),
            instant_range_end: "20240101120000000".into(),
            instant_range_type: "CLOSED_CLOSED".into(),
            table_config_keys: vec!["hoodie.table.name".into()],
            table_config_values: vec!["test".into()],
            hoodie_reader_config_keys: vec!["hoodie.read.key".into()],
            hoodie_reader_config_values: vec!["val".into()],
            props_keys: vec!["hoodie.props.key".into()],
            props_values: vec!["propval".into()],
            data_schema_json: r#"{"type":"record"}"#.into(),
            requested_schema_json: r#"{"type":"record"}"#.into(),
            partition_path: "year=2024".into(),
            base_file_name: "abc.parquet".into(),
            log_file_names: vec![".abc.log.1".into()],
            start: 0,
            length: 1024,
            should_use_record_position: true,
            allow_inflight_instants: false,
            emit_delete: false,
            sort_output: false,
            has_base_file: true,
            base_file_path: "s3://bucket/table/year=2024/abc.parquet".into(),
            base_file_file_name: "abc.parquet".into(),
            base_file_file_size: 1024,
            base_file_file_id: "abc".into(),
            base_file_commit_time: "20240101120000000".into(),
            base_file_has_bootstrap: false,
            base_file_bootstrap_path: "".into(),
            base_file_bootstrap_file_name: "".into(),
            base_file_bootstrap_file_size: 0,
            base_file_bootstrap_file_id: "".into(),
            base_file_bootstrap_commit_time: "".into(),
            log_file_details: vec![FfiLogFile {
                path_str: ".abc.log.1".into(),
                file_id: "abc".into(),
                delta_commit_time: "20240101120000000".into(),
                log_version: 1,
                log_write_token: "1-0-1".into(),
                file_extension: ".log".into(),
                suffix: "".into(),
                file_size: 512,
            }],
        };

        let ctx: FileGroupReaderContext = ffi.into();
        assert_eq!(ctx.table_path, "s3://bucket/table");
        assert_eq!(ctx.partition_path, "year=2024");
        assert_eq!(ctx.latest_commit_time, "20240101120000000");
        assert_eq!(ctx.start, 0);
        assert_eq!(ctx.length, 1024);
        assert!(ctx.should_use_record_position);

        // props
        assert_eq!(ctx.props.get("hoodie.props.key").unwrap(), "propval");

        // BaseFile
        let bf = ctx.base_file.as_ref().unwrap();
        assert_eq!(bf.file_id, "abc");
        assert!(bf.bootstrap_base_file.is_none());

        // LogFile
        assert_eq!(ctx.log_files.len(), 1);
        assert_eq!(ctx.log_files[0].log_version, 1);

        // Schemas
        assert!(ctx.data_schema.is_some());
        assert!(ctx.requested_schema.is_some());

        // ReaderContext (inner)
        let rc = ctx.reader_context.unwrap();
        assert_eq!(rc.table_path, "s3://bucket/table");
        assert!(rc.has_log_files);
        let ir = rc.instant_range.unwrap();
        assert_eq!(ir.range_type, "CLOSED_CLOSED");
        assert_eq!(rc.table_config.get("hoodie.table.name").unwrap(), "test");
    }
}
