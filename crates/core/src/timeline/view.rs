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

//! Timeline view for filtering file slices.
//!
//! This module provides the [`TimelineView`] struct which encapsulates all
//! timeline-derived context needed for file slice queries:
//! - Query timestamp (as of)
//! - Start timestamp (for incremental queries)
//! - Completion time mappings
//! - File groups to be excluded (e.g., replaced by clustering)
//!
//! [`TimelineView`] implements [`CompletionTimeView`] trait and is the main
//! type used for completion time lookups throughout the codebase.

use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig::TimelineLayoutVersion;
use crate::file_group::FileGroup;
use crate::timeline::completion_time::CompletionTimeView;
use crate::timeline::instant::Instant;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Timeline view for filtering file slices.
///
/// See module-level documentation for details.
#[derive(Debug)]
pub struct TimelineView {
    /// The "as of" timestamp for the snapshot query.
    /// It is also the end timestamp for incremental queries.
    as_of_timestamp: String,

    /// The start timestamp when the view is used for incremental queries.
    #[allow(dead_code)]
    start_timestamp: Option<String>,

    /// File groups to exclude from the query result.
    ///
    /// These are file groups that have been replaced by clustering
    /// or insert overwrite operations before the query timestamp.
    excluding_file_groups: HashSet<FileGroup>,

    /// Map from request timestamp to completion timestamp.
    ///
    /// Populated for timeline layout v2. Empty for v1.
    request_to_completion: HashMap<String, String>,

    /// Whether this table uses timeline layout v2 (completion time tracking).
    is_timeline_layout_v2: bool,
}

impl TimelineView {
    /// Create a new timeline view.
    ///
    /// # Arguments
    /// * `as_of_timestamp` - The "as of" timestamp for the snapshot and time-travel query; also the end timestamp for incremental queries
    /// * `start_timestamp` - The start timestamp for incremental queries
    /// * `completed_commits` - Iterator over completed commit instants to build the view from
    /// * `excluding_file_groups` - File groups to exclude (e.g., replaced by clustering)
    /// * `hudi_configs` - The shared Hudi configurations
    pub fn new<'a, I>(
        as_of_timestamp: String,
        start_timestamp: Option<String>,
        completed_commits: I,
        excluding_file_groups: HashSet<FileGroup>,
        hudi_configs: &Arc<HudiConfigs>,
    ) -> Self
    where
        I: IntoIterator<Item = &'a Instant>,
    {
        // Only build completion time map for timeline layout v2
        let timeline_layout_version: isize = hudi_configs
            .get(TimelineLayoutVersion)
            .map(|v| v.into())
            .unwrap_or(0);

        let is_timeline_layout_v2 = timeline_layout_version >= 2;
        let request_to_completion = if is_timeline_layout_v2 {
            Self::build_completion_time_map(completed_commits)
        } else {
            HashMap::new()
        };

        Self {
            as_of_timestamp,
            start_timestamp,
            excluding_file_groups,
            request_to_completion,
            is_timeline_layout_v2,
        }
    }

    /// Build the completion time map from instants.
    fn build_completion_time_map<'a, I>(instants: I) -> HashMap<String, String>
    where
        I: IntoIterator<Item = &'a Instant>,
    {
        instants
            .into_iter()
            .filter_map(|instant| {
                instant
                    .completion_timestamp
                    .as_ref()
                    .map(|completion_ts| (instant.timestamp.clone(), completion_ts.clone()))
            })
            .collect()
    }

    /// Get the "as of" timestamp for this view.
    #[inline]
    pub fn as_of_timestamp(&self) -> &str {
        &self.as_of_timestamp
    }

    /// Get the file groups to exclude from the query.
    #[inline]
    pub fn excluding_file_groups(&self) -> &HashSet<FileGroup> {
        &self.excluding_file_groups
    }
}

impl CompletionTimeView for TimelineView {
    fn get_completion_time(&self, request_timestamp: &str) -> Option<&str> {
        self.request_to_completion
            .get(request_timestamp)
            .map(|s| s.as_str())
    }

    fn should_filter_uncommitted(&self) -> bool {
        self.is_timeline_layout_v2
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HudiConfigs;
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

    fn create_layout_v1_configs() -> Arc<HudiConfigs> {
        Arc::new(HudiConfigs::new([("hoodie.timeline.layout.version", "1")]))
    }

    fn create_layout_v2_configs() -> Arc<HudiConfigs> {
        Arc::new(HudiConfigs::new([("hoodie.timeline.layout.version", "2")]))
    }

    #[test]
    fn test_snapshot_view_creation_layout_v2() {
        let instants = vec![
            create_instant("20240101120000000", Some("20240101120005000")),
            create_instant("20240101130000000", Some("20240101130010000")),
        ];
        let configs = create_layout_v2_configs();

        let view = TimelineView::new(
            "20240101130000000".to_string(),
            None,
            &instants,
            HashSet::new(),
            &configs,
        );

        assert_eq!(view.as_of_timestamp(), "20240101130000000");
        assert!(view.excluding_file_groups().is_empty());
        // Layout v2 should have completion time map populated
        assert!(view.should_filter_uncommitted());
    }

    #[test]
    fn test_snapshot_view_creation_layout_v1() {
        let instants = vec![
            create_instant("20240101120000000", Some("20240101120005000")),
            create_instant("20240101130000000", Some("20240101130010000")),
        ];
        let configs = create_layout_v1_configs();

        let view = TimelineView::new(
            "20240101130000000".to_string(),
            None,
            &instants,
            HashSet::new(),
            &configs,
        );

        assert_eq!(view.as_of_timestamp(), "20240101130000000");
        // Layout v1 should NOT have completion time map (empty)
        assert!(!view.should_filter_uncommitted());
        assert!(view.get_completion_time("20240101120000000").is_none());
    }

    #[test]
    fn test_completion_time_lookup_layout_v2() {
        let instants = vec![
            create_instant("20240101120000000", Some("20240101120005000")),
            create_instant("20240101130000000", Some("20240101130010000")),
            create_instant("20240101140000000", None), // Pending
        ];
        let configs = create_layout_v2_configs();

        let view = TimelineView::new(
            "20240101140000000".to_string(),
            None,
            &instants,
            HashSet::new(),
            &configs,
        );

        // Completed instants have completion time
        assert_eq!(
            view.get_completion_time("20240101120000000"),
            Some("20240101120005000")
        );
        assert_eq!(
            view.get_completion_time("20240101130000000"),
            Some("20240101130010000")
        );

        // Pending instant has no completion time
        assert!(view.get_completion_time("20240101140000000").is_none());

        // Unknown timestamp returns None
        assert!(view.get_completion_time("unknown").is_none());
    }

    #[test]
    fn test_should_filter_uncommitted_layout_v2() {
        let instants = vec![create_instant(
            "20240101120000000",
            Some("20240101120005000"),
        )];
        let configs = create_layout_v2_configs();

        let view = TimelineView::new(
            "20240101120000000".to_string(),
            None,
            &instants,
            HashSet::new(),
            &configs,
        );

        // Layout v2 should filter uncommitted
        assert!(view.should_filter_uncommitted());
    }

    #[test]
    fn test_should_not_filter_uncommitted_layout_v1() {
        // Layout v1 - even with instants that have completion timestamps,
        // the map is not built, so should_filter_uncommitted returns false
        let instants = vec![create_instant(
            "20240101120000000",
            Some("20240101120005000"),
        )];
        let configs = create_layout_v1_configs();

        let view = TimelineView::new(
            "20240101120000000".to_string(),
            None,
            &instants,
            HashSet::new(),
            &configs,
        );

        // Layout v1 does not track completion time
        assert!(!view.should_filter_uncommitted());
    }

    #[test]
    fn test_excluding_file_groups() {
        let instants: Vec<Instant> = vec![];
        let configs = create_layout_v2_configs();
        let mut excludes = HashSet::new();
        excludes.insert(FileGroup::new("file-id-1".to_string(), "p1".to_string()));
        excludes.insert(FileGroup::new("file-id-2".to_string(), "p2".to_string()));

        let view = TimelineView::new(
            "20240101120000000".to_string(),
            None,
            &instants,
            excludes,
            &configs,
        );

        assert_eq!(view.excluding_file_groups().len(), 2);
    }
}
