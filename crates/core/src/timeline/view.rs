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

    /// Request timestamps of every completed commit in the active timeline.
    ///
    /// Populated for both layouts, unlike [`Self::request_to_completion`] —
    /// layout v1 records no completion timestamps, so membership is the only
    /// thing that can answer "did this commit complete?" there.
    completed_requests: HashSet<String>,

    /// The archival boundary: a request timestamp below this belongs to an
    /// archived — hence completed — commit. See
    /// [`Timeline::earliest_active_instant`](crate::timeline::Timeline).
    earliest_active_instant: Option<String>,
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
        Self::new_with_archival_boundary(
            as_of_timestamp,
            start_timestamp,
            completed_commits,
            excluding_file_groups,
            hudi_configs,
            None,
        )
    }

    /// As [`Self::new`], plus the archival boundary that
    /// [`CompletionTimeView::is_committed`] needs to tell an archived commit from
    /// one that never completed.
    ///
    /// `None` disables the archived half of that test, which is the conservative
    /// direction — a caller without the boundary treats archived files as
    /// uncommitted rather than treating uncommitted files as archived.
    pub fn new_with_archival_boundary<'a, I>(
        as_of_timestamp: String,
        start_timestamp: Option<String>,
        completed_commits: I,
        excluding_file_groups: HashSet<FileGroup>,
        hudi_configs: &Arc<HudiConfigs>,
        earliest_active_instant: Option<String>,
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
        let mut request_to_completion = HashMap::new();
        let mut completed_requests = HashSet::new();
        for instant in completed_commits {
            completed_requests.insert(instant.timestamp.clone());
            if is_timeline_layout_v2
                && let Some(completion_ts) = instant.completion_timestamp.as_ref()
            {
                request_to_completion.insert(instant.timestamp.clone(), completion_ts.clone());
            }
        }

        Self {
            as_of_timestamp,
            start_timestamp,
            excluding_file_groups,
            request_to_completion,
            completed_requests,
            earliest_active_instant,
        }
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

    fn is_committed(&self, request_timestamp: &str) -> bool {
        if self.completed_requests.contains(request_timestamp) {
            return true;
        }
        // Below where the active timeline starts, so archived — and archival only
        // moves completed instants, and never past the oldest pending one.
        match &self.earliest_active_instant {
            Some(boundary) => request_timestamp < boundary.as_str(),
            None => false,
        }
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
        assert_eq!(
            view.get_completion_time("20240101120000000"),
            Some("20240101120005000")
        );
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
        // Layout v1 tracks no completion times...
        assert!(view.get_completion_time("20240101120000000").is_none());
        // ...but must still know the commit completed.
        assert!(view.is_committed("20240101120000000"));
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

    /// REGRESSION: uncommitted files must be filtered on BOTH timeline layouts.
    ///
    /// The predicate used to be `should_filter_uncommitted() && completion_time
    /// .is_none()`, gated on layout v2 — so on a v1 table nothing was filtered
    /// and a snapshot read returned rows from a commit that never completed
    /// (a crashed writer, a concurrent writer mid-flight, or the window inside a
    /// rollback). Layout v1 records no completion times, so membership in the
    /// completed set is the only thing that can answer the question there.
    #[test]
    fn test_is_committed_filters_pending_commits_on_both_layouts() {
        let instants = vec![
            create_instant("20240101120000000", Some("20240101120005000")),
            create_instant("20240101140000000", Some("20240101140005000")),
        ];

        for configs in [create_layout_v1_configs(), create_layout_v2_configs()] {
            let view = TimelineView::new_with_archival_boundary(
                "20240101140000000".to_string(),
                None,
                &instants,
                HashSet::new(),
                &configs,
                Some("20240101120000000".to_string()),
            );

            assert!(view.is_committed("20240101120000000"), "completed commit");
            assert!(view.is_committed("20240101140000000"), "completed commit");
            // Between two completed commits and absent from the set: in flight.
            assert!(
                !view.is_committed("20240101130000000"),
                "a commit that never completed must not be readable"
            );
            // Above the newest commit: also in flight.
            assert!(!view.is_committed("20240101150000000"), "in flight");
        }
    }

    /// REGRESSION: a file whose commit was ARCHIVED is still committed.
    ///
    /// The completion map is built from the ACTIVE timeline, so testing for a
    /// completion timestamp discarded files from commits that had merely been
    /// archived — silent row loss on any table old enough to have archived,
    /// which is routine maintenance rather than an edge case. Java admits them
    /// via the second half of `containsInstant(ts) || isBeforeTimelineStarts(ts)`.
    #[test]
    fn test_is_committed_admits_archived_commits() {
        let instants = vec![create_instant(
            "20240101120000000",
            Some("20240101120005000"),
        )];

        for configs in [create_layout_v1_configs(), create_layout_v2_configs()] {
            let view = TimelineView::new_with_archival_boundary(
                "20240101120000000".to_string(),
                None,
                &instants,
                HashSet::new(),
                &configs,
                Some("20240101120000000".to_string()),
            );

            // Below where the active timeline starts => archived => committed.
            assert!(
                view.is_committed("20231231000000000"),
                "an archived commit's files must stay readable"
            );
            // At or above the boundary and not in the set => in flight.
            assert!(!view.is_committed("20240101130000000"));
        }
    }

    /// Without a boundary the archived half is disabled, and the failure has to
    /// land on the safe side: treat archived files as uncommitted (lose rows)
    /// rather than treating uncommitted files as archived (read dirty rows).
    #[test]
    fn test_is_committed_without_a_boundary_is_conservative() {
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

        assert!(view.is_committed("20240101120000000"));
        assert!(!view.is_committed("20231231000000000"));
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
