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

//! Inputs the merge-on-read file group reader needs for one file slice.

use crate::config::table::BaseFileFormatValue;
use crate::timeline::selector::InstantRange;
use std::collections::HashMap;

/// How the reader picks a winner among versions of the same record key.
///
/// The string forms match the merge modes the Hudi Java reader uses, so the
/// values survive a round trip through engine integrations unchanged.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MergeMode {
    /// Latest commit wins.
    ///
    /// Nothing selects this yet. A table with no ordering field derives
    /// `append_only`, which the resolver rejects rather than mapping — see
    /// `resolver::resolve_merge_mode`. Settling that question is what makes
    /// this reachable, since commit-time ordering is what such a table wants.
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

/// Everything the MOR reader needs that is derived from table state rather
/// than from the file slice itself.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct ReaderContext {
    /// Table base path, as configured.
    pub table_path: String,
    /// Timestamp the read is pinned to. Log blocks at or before this instant
    /// are visible; later ones are not.
    pub latest_commit_time: String,
    /// How to pick a winner among versions of the same record key.
    pub merge_mode: MergeMode,
    /// Format of the base file backing the slice.
    pub base_file_format: BaseFileFormatValue,
    /// Whether the slice has log files to merge. `false` reduces the read to a
    /// plain base-file scan.
    pub has_log_files: bool,
    /// Window bounding which instants the log scan admits.
    pub instant_range: InstantRange,
    /// The table's own properties, keyed by their `hoodie.*` config names.
    pub table_config: HashMap<String, String>,
    /// Per-read overrides, keyed by their `hoodie.read.*` config names.
    pub hoodie_reader_config: HashMap<String, String>,
    /// Whether to merge log records onto base rows by record position rather
    /// than by record key.
    pub should_merge_use_record_position: bool,
}

/// Flags controlling what the reader emits, independent of table state.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReaderParameters {
    /// Emit delete records to the caller instead of only applying them.
    pub emit_delete: bool,
    /// Sort the merged output.
    pub sort_output: bool,
    /// Admit log blocks from instants that have not completed.
    pub allow_inflight_instants: bool,
}
