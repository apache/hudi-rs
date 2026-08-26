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

//! The merge-on-read file group reader engine, mirroring Java's
//! `org.apache.hudi.common.table.read` package.
//!
//! `engine` orchestrates a read: the `resolver` derives the reader context
//! from a table's configs, the log scanner and record buffer merge log records
//! over the base file, and `merge_iterator` streams the merged output.
//! `file_group::reader::FileGroupReader` reaches all of this through
//! `adapter` (see its `read_via_v2` / `stream_via_v2`).
//!
//! Some surface exists only for Java parity or is reached only by the test
//! harness; it carries targeted `#[allow(dead_code)]` allows rather than a
//! module-wide blanket, so an item that is mistakenly dead still warns.

/// Far-future instant standing in for "no upper bound on the timeline": every
/// real instant sorts before it, so a read watermarked here sees the whole
/// snapshot and no log block counts as a future block.
///
/// Comparison is lexicographic on purpose — instant times are zero-padded
/// fixed-width strings.
#[allow(dead_code)]
pub(crate) const MAX_INSTANT_TIME: &str = "99991231235959999";

pub(crate) mod adapter;
pub(crate) mod buffer;
pub(crate) mod buffered_record;
pub(crate) mod buffered_record_converter;
pub(crate) mod delete_context;
pub(crate) mod engine;
pub(crate) mod gaps;
#[cfg(test)]
mod harness;
#[cfg(test)]
mod harness_tests;
pub(crate) mod input_split;
pub(crate) mod iterator_mode;
pub(crate) mod log_record_reader;
#[cfg(test)]
mod memory_limit_tests;
pub(crate) mod merge_iterator;
pub(crate) mod merged_log_record_reader;
pub(crate) mod metadata_merger;
pub(crate) mod output_converter;
pub(crate) mod profiling;
pub(crate) mod read_stats;
pub(crate) mod reader_context;
pub(crate) mod reader_parameters;
pub(crate) mod record_context;
pub(crate) mod record_merger;
pub(crate) mod resolver;
/// Arrow IPC serialization for the spill tier.
///
/// Only the RocksDB-backed merge map serializes anything, so without
/// `spill-rocksdb` there is no caller and the module is not built.
#[cfg(feature = "spill-rocksdb")]
pub(crate) mod row_serde;
pub(crate) mod schema_handler;
pub(crate) mod update_processor;
