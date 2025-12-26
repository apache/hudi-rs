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

//! Completion time query view for looking up completion timestamps from request timestamps.
//!
//! This module provides the [`TimelineViewByCompletionTime`] trait and implementations
//! for mapping request timestamps to completion timestamps. This is essential for
//! v8+ tables where file names contain request timestamps but file ordering and
//! association must be based on completion timestamps.
//!
//! # Table Version Differences
//!
//! - **v6 tables**: Completion timestamps are not tracked. Use [`V6CompletionTimeView`]
//!   which returns `None` for all lookups (completion_timestamp remains unset).
//!
//! - **v8+ tables**: Request and completion timestamps are different. The completion timestamp
//!   is stored in the timeline instant filename as `{request}_{completion}.{action}`.
//!   Use [`CompletionTimeView`] to look up completion timestamps from a map.

use crate::timeline::instant::Instant;
use std::collections::HashMap;

/// A view for querying completion timestamps from request timestamps.
///
/// This trait abstracts the completion time lookup logic to support both
/// v6 tables (where completion time is not tracked) and v8+ tables (where
/// request and completion timestamps differ).
pub trait TimelineViewByCompletionTime {
    /// Get the completion timestamp for a given request timestamp.
    ///
    /// Returns `Some(completion_timestamp)` if the request timestamp corresponds
    /// to a completed commit, `None` if the commit is pending/unknown or if
    /// completion time is not tracked (v6 tables).
    fn get_completion_time(&self, request_timestamp: &str) -> Option<&str>;

    /// Returns true if uncommitted files should be filtered out.
    ///
    /// For v8+ tables, this returns true because we track completion times
    /// and can distinguish committed from uncommitted files.
    /// For v6 tables, this returns false because completion time is not tracked.
    fn should_filter_uncommitted(&self) -> bool {
        false
    }
}

/// Completion time view for v6 tables.
///
/// In v6, completion time is not tracked separately from request time.
/// This view always returns `None`, meaning the caller should use the
/// request timestamp directly for any completion-time-based logic.
#[derive(Debug, Default)]
pub struct V6CompletionTimeView;

impl V6CompletionTimeView {
    pub fn new() -> Self {
        Self
    }
}

impl TimelineViewByCompletionTime for V6CompletionTimeView {
    fn get_completion_time(&self, _request_timestamp: &str) -> Option<&str> {
        None
    }
}

/// Completion time view for v8+ tables backed by a HashMap.
///
/// This view is built from timeline instants and maps request timestamps
/// to their corresponding completion timestamps.
#[derive(Debug)]
pub struct CompletionTimeView {
    /// Map from request timestamp to completion timestamp
    request_to_completion: HashMap<String, String>,
}

impl CompletionTimeView {
    /// Create a new completion time view from timeline instants.
    ///
    /// Only completed instants with a completion timestamp are included.
    pub fn from_instants<'a, I>(instants: I) -> Self
    where
        I: IntoIterator<Item = &'a Instant>,
    {
        let request_to_completion = instants
            .into_iter()
            .filter_map(|instant| {
                instant
                    .completion_timestamp
                    .as_ref()
                    .map(|completion_ts| (instant.timestamp.clone(), completion_ts.clone()))
            })
            .collect();

        Self {
            request_to_completion,
        }
    }

    /// Create a new empty completion time view.
    pub fn empty() -> Self {
        Self {
            request_to_completion: HashMap::new(),
        }
    }

    /// Check if this view has any completion time mappings.
    pub fn is_empty(&self) -> bool {
        self.request_to_completion.is_empty()
    }

    /// Get the number of completion time mappings.
    pub fn len(&self) -> usize {
        self.request_to_completion.len()
    }
}

impl TimelineViewByCompletionTime for CompletionTimeView {
    fn get_completion_time(&self, request_timestamp: &str) -> Option<&str> {
        self.request_to_completion
            .get(request_timestamp)
            .map(|s| s.as_str())
    }

    fn should_filter_uncommitted(&self) -> bool {
        // Only filter if we have completion time data (v8+ table)
        !self.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timeline::instant::{Action, State};

    fn create_instant(request_ts: &str, completion_ts: Option<&str>) -> Instant {
        Instant {
            timestamp: request_ts.to_string(),
            completion_timestamp: completion_ts.map(|s| s.to_string()),
            action: Action::Commit,
            state: State::Completed,
            epoch_millis: 0,
        }
    }

    #[test]
    fn test_v6_completion_time_view() {
        let view = V6CompletionTimeView::new();

        // V6 view always returns None for completion time
        // (caller should use request time directly for v6 tables)
        assert!(view.get_completion_time("20240101120000000").is_none());
        assert!(view.get_completion_time("any_timestamp").is_none());
    }

    #[test]
    fn test_timeline_completion_time_view_from_instants() {
        let instants = vec![
            create_instant("20240101120000000", Some("20240101120005000")),
            create_instant("20240101130000000", Some("20240101130010000")),
            create_instant("20240101140000000", None), // Pending instant
        ];

        let view = CompletionTimeView::from_instants(&instants);

        // Completed instants should have completion times
        assert_eq!(
            view.get_completion_time("20240101120000000"),
            Some("20240101120005000")
        );
        assert_eq!(
            view.get_completion_time("20240101130000000"),
            Some("20240101130010000")
        );

        // Pending instant should not have completion time
        assert!(view.get_completion_time("20240101140000000").is_none());

        // Unknown timestamp should return None
        assert!(view.get_completion_time("unknown").is_none());
    }

    #[test]
    fn test_timeline_completion_time_view_pending_and_unknown() {
        let instants = vec![
            create_instant("20240101120000000", Some("20240101120005000")),
            create_instant("20240101130000000", None), // Pending
        ];

        let view = CompletionTimeView::from_instants(&instants);

        // Completed instant has completion time
        assert!(view.get_completion_time("20240101120000000").is_some());
        // Pending instant has no completion time
        assert!(view.get_completion_time("20240101130000000").is_none());
        // Unknown timestamp has no completion time
        assert!(view.get_completion_time("unknown").is_none());
    }

    #[test]
    fn test_timeline_completion_time_view_empty() {
        let view = CompletionTimeView::empty();

        assert!(view.is_empty());
        assert_eq!(view.len(), 0);
        assert!(view.get_completion_time("any").is_none());
    }

    #[test]
    fn test_timeline_completion_time_view_len() {
        let instants = vec![
            create_instant("20240101120000000", Some("20240101120005000")),
            create_instant("20240101130000000", Some("20240101130010000")),
        ];

        let view = CompletionTimeView::from_instants(&instants);

        assert!(!view.is_empty());
        assert_eq!(view.len(), 2);
    }
}
