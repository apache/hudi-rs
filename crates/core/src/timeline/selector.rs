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
use crate::Result;
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig;
use crate::error::CoreError;
use crate::timeline::Timeline;
use crate::timeline::instant::{Action, Instant, State};
use chrono::{DateTime, Utc};
use std::collections::HashSet;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct InstantRange {
    timezone: String,
    start_timestamp: Option<String>,
    end_timestamp: Option<String>,
    start_inclusive: bool,
    end_inclusive: bool,
    /// Explicit membership, mirroring Java's `RangeType::EXACT_MATCH`.
    ///
    /// A window cannot express the set the metadata table filters its log blocks
    /// by: that set has **holes** -- a pending data instant is excluded while
    /// instants on either side of it are included -- and members from outside the
    /// data timeline entirely, such as metadata-only indexing delta commits.
    /// Approximating it with bounds would admit blocks written for a pending
    /// instant, and for every partition except `files` those records are used
    /// as-is: wrong column statistics prune away files that hold matching rows,
    /// which is a silently wrong query result rather than a failure.
    ///
    /// When set, bounds are ignored entirely -- membership is the whole test, as
    /// it is in Java.
    explicit_instants: Option<HashSet<String>>,
}

impl InstantRange {
    pub fn new(
        timezone: String,
        start_timestamp: Option<String>,
        end_timestamp: Option<String>,
        start_inclusive: bool,
        end_inclusive: bool,
    ) -> Self {
        Self {
            timezone,
            start_timestamp,
            end_timestamp,
            start_inclusive,
            end_inclusive,
            explicit_instants: None,
        }
    }

    /// A range that admits exactly the instants in `instants`, and nothing else.
    ///
    /// See [`Self::explicit_instants`] for why a window cannot stand in for this.
    pub fn exact_match<I, S>(instants: I, timezone: &str) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            timezone: timezone.to_string(),
            start_timestamp: None,
            end_timestamp: None,
            start_inclusive: false,
            end_inclusive: false,
            explicit_instants: Some(instants.into_iter().map(Into::into).collect()),
        }
    }

    /// Whether this range admits by explicit membership rather than by bounds.
    pub fn is_exact_match(&self) -> bool {
        self.explicit_instants.is_some()
    }

    /// Whether this range excludes nothing.
    ///
    /// No lower bound, an upper bound at or above the maximum instant, **and no
    /// explicit instant set** — then every instant is admitted, so filtering
    /// against it cannot remove a row. The merge-on-read path builds exactly this
    /// range by default (`with_unbounded_end_timestamp`) and materializes the
    /// whole base file to apply it; this lets that path tell the difference.
    ///
    /// The explicit-set clause is load-bearing: [`Self::exact_match`] leaves both
    /// timestamps `None`, so a bounds-only test would call it unbounded and skip
    /// the filter for the metadata table, which is the one caller that admits by
    /// membership.
    pub fn admits_all(&self) -> bool {
        self.explicit_instants.is_none()
            && self.start_timestamp.is_none()
            && self
                .end_timestamp
                .as_deref()
                .is_none_or(|e| e >= crate::file_group::reader_v2::MAX_INSTANT_TIME)
    }

    /// Create a new [InstantRange] with a closed end timestamp range.
    pub fn up_to(end_timestamp: &str, timezone: &str) -> Self {
        Self::new(
            timezone.to_string(),
            None,
            Some(end_timestamp.to_string()),
            false,
            true,
        )
    }

    /// Create a new [InstantRange] with an open timestamp range.
    pub fn within(start_timestamp: &str, end_timestamp: &str, timezone: &str) -> Self {
        Self::new(
            timezone.to_string(),
            Some(start_timestamp.to_string()),
            Some(end_timestamp.to_string()),
            false,
            false,
        )
    }

    /// Lexicographic variant of [`Self::is_in_range`], with the same
    /// inclusivity, for instants that cannot be parsed as datetimes.
    ///
    /// Mirrors the JVM reader's `InstantComparison`, which compares instant
    /// strings directly and never parses. Hudi instants are fixed-format
    /// numeric strings, so lexicographic order matches chronological order, and
    /// short instants still bound correctly rather than being kept
    /// unconditionally.
    pub fn is_in_range_lexicographic(&self, timestamp: &str) -> bool {
        if let Some(allowed) = &self.explicit_instants {
            return allowed.contains(timestamp);
        }
        if let Some(start) = self.start_timestamp.as_deref() {
            if self.start_inclusive {
                if timestamp < start {
                    return false;
                }
            } else if timestamp <= start {
                return false;
            }
        }
        if let Some(end) = self.end_timestamp.as_deref() {
            if self.end_inclusive {
                if timestamp > end {
                    return false;
                }
            } else if timestamp >= end {
                return false;
            }
        }
        true
    }
    /// Create a new [InstantRange] with an open start and closed end timestamp range.
    pub fn within_open_closed(start_timestamp: &str, end_timestamp: &str, timezone: &str) -> Self {
        Self::new(
            timezone.to_string(),
            Some(start_timestamp.to_string()),
            Some(end_timestamp.to_string()),
            false,
            true,
        )
    }

    pub fn timezone(&self) -> &str {
        &self.timezone
    }

    pub fn start_timestamp(&self) -> Result<Option<DateTime<Utc>>> {
        self.start_timestamp
            .as_deref()
            .map(|timestamp| Instant::parse_datetime(timestamp, &self.timezone))
            .transpose()
    }

    pub fn end_timestamp(&self) -> Result<Option<DateTime<Utc>>> {
        self.end_timestamp
            .as_deref()
            .map(|timestamp| Instant::parse_datetime(timestamp, &self.timezone))
            .transpose()
    }

    pub fn is_in_range(&self, timestamp: &str, timezone: &str) -> Result<bool> {
        // Membership short-circuits before any parsing: an exact-match set may
        // hold instants the bounds would reject, and parsing would be wasted
        // work besides.
        if let Some(allowed) = &self.explicit_instants {
            return Ok(allowed.contains(timestamp));
        }
        let t = Instant::parse_datetime(timestamp, timezone)?;
        if let Some(start) = self.start_timestamp()? {
            if self.start_inclusive {
                if t < start {
                    return Ok(false);
                }
            } else if t <= start {
                return Ok(false);
            }
        }

        if let Some(end) = self.end_timestamp()? {
            if self.end_inclusive {
                if t > end {
                    return Ok(false);
                }
            } else if t >= end {
                return Ok(false);
            }
        }

        Ok(true)
    }

    pub fn not_in_range(&self, timestamp: &str, timezone: &str) -> Result<bool> {
        Ok(!self.is_in_range(timestamp, timezone)?)
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub struct TimelineSelector {
    timezone: String,
    start_datetime: Option<DateTime<Utc>>,
    end_datetime: Option<DateTime<Utc>>,
    /// The raw range bounds, kept alongside the parsed ones because a layout-v2
    /// range is applied to *completion* timestamps, which are compared as
    /// strings — see [`Self::select`].
    start_timestamp: Option<String>,
    end_timestamp: Option<String>,
    /// Whether the range bounds **completion** times rather than requested times.
    ///
    /// Only an incremental window does. A snapshot or time-travel bound is a
    /// requested-time bound by construction — it comes from an instant's own
    /// timestamp — so ranging it on completion time would exclude the newest
    /// commit from itself, and with it the replace-commit that prunes overwritten
    /// file groups.
    range_on_completion_time: bool,
    states: Vec<State>,
    actions: Vec<Action>,
    /// Timeline layout version determines instant format validation:
    /// - Layout 1 (pre-v8): expects `{timestamp}.{action}` for completed instants
    /// - Layout 2 (v8+): expects `{requestedTimestamp}_{completedTimestamp}.{action}` for completed instants
    timeline_layout_version: isize,
}

#[allow(dead_code)]
impl TimelineSelector {
    fn get_timezone_from_configs(hudi_configs: &HudiConfigs) -> String {
        hudi_configs
            .get_or_default(HudiTableConfig::TimelineTimezone)
            .into()
    }

    fn get_timeline_layout_version_from_configs(hudi_configs: &HudiConfigs) -> Result<isize> {
        if let Some(layout_version) =
            hudi_configs.try_get(HudiTableConfig::TimelineLayoutVersion)?
        {
            Ok(layout_version.into())
        } else {
            let table_version: isize = hudi_configs
                .try_get(HudiTableConfig::TableVersion)?
                .map(|v| v.into())
                .unwrap_or(6);
            Ok(if table_version >= 8 { 2 } else { 1 })
        }
    }

    fn parse_datetime(timezone: &str, timestamp: Option<&str>) -> Result<Option<DateTime<Utc>>> {
        timestamp
            .map(|e| Instant::parse_datetime(e, timezone))
            .transpose()
    }

    pub fn completed_actions_in_range(
        actions: &[Action],
        hudi_configs: Arc<HudiConfigs>,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<Self> {
        Self::actions_in_range(actions, &[State::Completed], hudi_configs, start, end)
    }

    /// As [`Self::completed_actions_in_range`], but the bounds apply to
    /// **completion** times — the semantics an incremental window needs. See
    /// [`Self::select_by_completion_time`].
    ///
    /// Falls back to requested-time ranging on timeline layout v1, which records
    /// no completion times.
    pub fn completed_actions_in_completion_time_range(
        actions: &[Action],
        hudi_configs: Arc<HudiConfigs>,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<Self> {
        let mut selector =
            Self::actions_in_range(actions, &[State::Completed], hudi_configs, start, end)?;
        selector.range_on_completion_time = true;
        Ok(selector)
    }

    /// Select `actions` in any of `states`.
    ///
    /// The all-states form exists so one listing of the timeline directory can
    /// answer two questions: which commits completed, and where the active
    /// timeline *starts*. The second needs pending instants too — archival never
    /// moves past the oldest pending instant, so the earliest instant of any
    /// state is the archival boundary, and taking it over completed instants
    /// alone would place the boundary above a pending instant and let that
    /// instant's files read as archived (i.e. as committed).
    pub fn actions_in_range(
        actions: &[Action],
        states: &[State],
        hudi_configs: Arc<HudiConfigs>,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<Self> {
        let timezone = Self::get_timezone_from_configs(&hudi_configs);
        let timeline_layout_version =
            Self::get_timeline_layout_version_from_configs(&hudi_configs)?;
        let start_datetime = Self::parse_datetime(&timezone, start)?;
        let end_datetime = Self::parse_datetime(&timezone, end)?;
        Ok(Self {
            timezone,
            start_datetime,
            end_datetime,
            start_timestamp: start.map(str::to_string),
            end_timestamp: end.map(str::to_string),
            range_on_completion_time: false,
            states: states.to_vec(),
            actions: actions.to_vec(),
            timeline_layout_version,
        })
    }

    pub fn completed_commits_in_range(
        hudi_configs: Arc<HudiConfigs>,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<Self> {
        Self::completed_actions_in_range(&[Action::Commit], hudi_configs, start, end)
    }

    pub fn completed_deltacommits_in_range(
        hudi_configs: Arc<HudiConfigs>,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<Self> {
        Self::completed_actions_in_range(&[Action::DeltaCommit], hudi_configs, start, end)
    }

    pub fn completed_replacecommits_in_range(
        hudi_configs: Arc<HudiConfigs>,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<Self> {
        Self::completed_actions_in_range(&[Action::ReplaceCommit], hudi_configs, start, end)
    }

    /// Whether the selector has any time filter (start or end) applied.
    pub fn has_time_filter(&self) -> bool {
        self.start_datetime.is_some() || self.end_datetime.is_some()
    }

    pub fn should_include_action(&self, action: &Action) -> bool {
        self.actions.is_empty() || self.actions.contains(action)
    }

    pub fn should_include_state(&self, state: &State) -> bool {
        self.states.is_empty() || self.states.contains(state)
    }

    pub fn try_create_instant(&self, file_name: &str) -> Result<Instant> {
        let (timestamp_part, action_suffix) = file_name.split_once('.').ok_or_else(|| {
            CoreError::Timeline(format!(
                "Instant not created due to invalid file name: {file_name}"
            ))
        })?;

        let (action, state) = Instant::parse_action_and_state(action_suffix)?;

        if !self.should_include_action(&action) {
            return Err(CoreError::Timeline(format!(
                "Instant not created for due to unmatched action: {file_name}"
            )));
        }

        if !self.should_include_state(&state) {
            return Err(CoreError::Timeline(format!(
                "Instant not created for due to unmatched state: {file_name}"
            )));
        }

        // Handle v8+ completed instant format: {requestedTimestamp}_{completionTimestamp}.{action}
        // Validate format based on timeline layout version and instant state
        let (timestamp, completion_timestamp) = if let Some((requested_ts, completed_ts)) =
            timestamp_part.split_once('_')
        {
            // Found underscore format - this should be a v8+ (layout 2) completed instant
            if self.timeline_layout_version == 1 && state == State::Completed {
                return Err(CoreError::Timeline(format!(
                    "Unexpected v8+ instant format in timeline layout v1: {file_name}"
                )));
            }

            // Validate both timestamps
            if requested_ts.len() != 17 && requested_ts.len() != 14 {
                return Err(CoreError::Timeline(format!(
                    "Invalid requested timestamp in v8+ format: {file_name}"
                )));
            }
            if completed_ts.len() != 17 && completed_ts.len() != 14 {
                return Err(CoreError::Timeline(format!(
                    "Invalid completed timestamp in v8+ format: {file_name}"
                )));
            }
            (requested_ts, Some(completed_ts.to_string()))
        } else {
            // No underscore format - this should be a pre-v8 instant OR a non-completed v8+ instant
            if self.timeline_layout_version == 2 && state == State::Completed {
                return Err(CoreError::Timeline(format!(
                    "Expected v8+ instant format (with completion timestamp) in timeline layout v2 for completed instant: {file_name}"
                )));
            }
            (timestamp_part, None)
        };

        let dt = Instant::parse_datetime(timestamp, &self.timezone)?;
        if let Some(start) = self.start_datetime
            && dt < start
        {
            return Err(CoreError::Timeline(format!(
                "Instant not created for due to timestamp before start datetime: {file_name}"
            )));
        }

        if let Some(end) = self.end_datetime
            && dt >= end
        {
            return Err(CoreError::Timeline(format!(
                "Instant not created for due to timestamp after or at end datetime: {file_name}"
            )));
        }

        Ok(Instant {
            timestamp: timestamp.to_string(),
            completion_timestamp,
            epoch_millis: dt.timestamp_millis(),
            action,
            state,
        })
    }

    /// Select loaded instants based on the selector's properties.
    ///
    /// The range is `(start, end]`. Which timestamp it applies to depends on the
    /// timeline layout — see [`Self::select_by_completion_time`].
    pub fn select(&self, timeline: &Timeline) -> Result<Vec<Instant>> {
        if self.range_on_completion_time
            && self.timeline_layout_version >= 2
            && (self.start_timestamp.is_some() || self.end_timestamp.is_some())
        {
            return Ok(self.select_by_completion_time(timeline));
        }
        self.select_by_requested_time(timeline)
    }

    /// Range a layout-v2 timeline on **completion** time.
    ///
    /// A commit becomes visible when it completes, not when it was requested, so
    /// that is what an incremental window has to bound. Hudi 1.x does the same:
    /// `CompletionTimeQueryViewV2.getInstantTimes` filters an
    /// `instantTime -> completionTime` map by the window.
    ///
    /// Ranging on the requested time instead — which this did — silently skips
    /// any commit requested before the window that completed inside it. That is
    /// the normal shape of a concurrent or simply slow write, and a consumer that
    /// advances its checkpoint by completion time never comes back for the commit
    /// it missed.
    ///
    /// Linear rather than binary: `completed_commits` is sorted by requested
    /// time, and completion order does not follow it — that reordering is the
    /// whole point. Java scans its map for the same reason. The active timeline
    /// is bounded by `hoodie.keep.min/max.commits`, so this is a short scan.
    ///
    /// Commits archived below the active timeline cannot be considered at all;
    /// `Table::warn_if_window_predates_active_timeline` reports that shortfall.
    fn select_by_completion_time(&self, timeline: &Timeline) -> Vec<Instant> {
        timeline
            .completed_commits
            .iter()
            .filter(|instant| {
                // A completed layout-v2 instant always carries a completion time
                // (it is the second half of its file name). Falling back to the
                // requested time keeps a malformed one in range-by-requested-time
                // rather than dropping it silently.
                let effective = instant
                    .completion_timestamp
                    .as_deref()
                    .unwrap_or(instant.timestamp.as_str());
                if let Some(start) = self.start_timestamp.as_deref()
                    && effective <= start
                {
                    return false;
                }
                if let Some(end) = self.end_timestamp.as_deref()
                    && effective > end
                {
                    return false;
                }
                self.should_include_action(&instant.action)
                    && self.should_include_state(&instant.state)
            })
            .cloned()
            .collect()
    }

    /// Range on the requested time, by binary search over the sorted vector.
    ///
    /// Correct for layout v1, which records no completion timestamps at all —
    /// Hudi's own `CompletionTimeQueryViewV1` is in the same position.
    fn select_by_requested_time(&self, timeline: &Timeline) -> Result<Vec<Instant>> {
        let time_pruned_instants = if let Some(start) = self.start_datetime {
            // Find first instant > start using binary search
            let start_pos = timeline
                .completed_commits
                .partition_point(|instant| instant.epoch_millis <= start.timestamp_millis());

            if let Some(end) = self.end_datetime {
                // Find first instant > end using binary search
                let end_pos = timeline.completed_commits[start_pos..]
                    .partition_point(|instant| instant.epoch_millis <= end.timestamp_millis());
                &timeline.completed_commits[start_pos..start_pos + end_pos]
            } else {
                &timeline.completed_commits[start_pos..]
            }
        } else if let Some(end) = self.end_datetime {
            // Find first instant > end using binary search
            let end_pos = timeline
                .completed_commits
                .partition_point(|instant| instant.epoch_millis <= end.timestamp_millis());
            &timeline.completed_commits[..end_pos]
        } else {
            &timeline.completed_commits[..]
        };

        Ok(time_pruned_instants
            .iter()
            .filter(|instant| {
                if !self.should_include_action(&instant.action) {
                    return false;
                }
                if !self.should_include_state(&instant.state) {
                    return false;
                }
                true
            })
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HudiConfigs;
    use crate::config::table::HudiTableConfig;
    use crate::storage::Storage;
    use crate::timeline::Timeline;
    use crate::timeline::builder::TimelineBuilder;
    use crate::timeline::instant::{Action, Instant, State};
    use chrono::{DateTime, Utc};
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;

    #[test]
    fn test_new_instant_range() {
        let range = InstantRange::new(
            "UTC".to_string(),
            Some("20240101000000000".to_string()),
            Some("20241231235959999".to_string()),
            true,
            false,
        );

        assert_eq!(range.timezone(), "UTC");
        assert_eq!(range.start_timestamp.as_deref(), Some("20240101000000000"));
        assert_eq!(range.end_timestamp.as_deref(), Some("20241231235959999"));
        assert!(range.start_inclusive);
        assert!(!range.end_inclusive);
    }

    #[test]
    fn test_up_to() {
        let range = InstantRange::up_to("20241231235959999", "UTC");

        assert_eq!(range.timezone(), "UTC");
        assert!(range.start_timestamp.is_none());
        assert_eq!(range.end_timestamp.as_deref(), Some("20241231235959999"));
        assert!(!range.start_inclusive);
        assert!(range.end_inclusive);
    }

    #[test]
    fn test_within() {
        let range = InstantRange::within("20240101000000000", "20241231235959999", "UTC");

        assert_eq!(range.timezone(), "UTC");
        assert_eq!(range.start_timestamp.as_deref(), Some("20240101000000000"));
        assert_eq!(range.end_timestamp.as_deref(), Some("20241231235959999"));
        assert!(!range.start_inclusive);
        assert!(!range.end_inclusive);
    }

    #[test]
    fn test_within_open_closed() {
        let range =
            InstantRange::within_open_closed("20240101000000000", "20241231235959999", "UTC");

        assert_eq!(range.timezone(), "UTC");
        assert_eq!(range.start_timestamp.as_deref(), Some("20240101000000000"));
        assert_eq!(range.end_timestamp.as_deref(), Some("20241231235959999"));
        assert!(!range.start_inclusive);
        assert!(range.end_inclusive);
    }

    #[test]
    fn test_is_in_range_inclusive_bounds() {
        let range = InstantRange::new(
            "UTC".to_string(),
            Some("20240101000000000".to_string()),
            Some("20241231235959999".to_string()),
            true,
            true,
        );

        // Test exact bounds
        assert!(range.is_in_range("20240101000000000", "UTC").unwrap());
        assert!(range.is_in_range("20241231235959999", "UTC").unwrap());

        // Test inside range
        assert!(range.is_in_range("20240615120000000", "UTC").unwrap());

        // Test outside range
        assert!(!range.is_in_range("20231231235959999", "UTC").unwrap());
        assert!(!range.is_in_range("20250101000000000", "UTC").unwrap());
    }

    #[test]
    fn test_is_in_range_exclusive_bounds() {
        let range = InstantRange::new(
            "UTC".to_string(),
            Some("20240101000000000".to_string()),
            Some("20241231235959999".to_string()),
            false,
            false,
        );

        // Test exact bounds
        assert!(!range.is_in_range("20240101000000000", "UTC").unwrap());
        assert!(!range.is_in_range("20241231235959999", "UTC").unwrap());

        // Test inside range
        assert!(range.is_in_range("20240615120000000", "UTC").unwrap());
    }

    #[test]
    fn test_not_in_range() {
        let range = InstantRange::new(
            "UTC".to_string(),
            Some("20240101000000000".to_string()),
            Some("20241231235959999".to_string()),
            true,
            true,
        );

        assert!(!range.not_in_range("20240615120000000", "UTC").unwrap());
        assert!(range.not_in_range("20231231235959999", "UTC").unwrap());
    }

    #[test]
    fn test_invalid_timestamp_format() {
        let range = InstantRange::new(
            "UTC".to_string(),
            Some("20240101000000000".to_string()),
            Some("20241231235959999".to_string()),
            true,
            true,
        );

        assert!(range.is_in_range("invalid_timestamp", "UTC").is_err());
    }

    #[test]
    fn test_invalid_timezone() {
        let range = InstantRange::new(
            "Invalid/Timezone".to_string(),
            Some("20240101000000000".to_string()),
            Some("20241231235959999".to_string()),
            true,
            true,
        );

        assert!(range.is_in_range("20240615120000000", "UTC").is_err());
    }

    #[test]
    fn test_millisecond_precision() {
        let range = InstantRange::new(
            "UTC".to_string(),
            Some("20240101000000000".to_string()),
            Some("20240101000000999".to_string()),
            true,
            true,
        );

        assert!(range.is_in_range("20240101000000000", "UTC").unwrap());
        assert!(range.is_in_range("20240101000000500", "UTC").unwrap());
        assert!(range.is_in_range("20240101000000999", "UTC").unwrap());
        assert!(!range.is_in_range("20240101000001000", "UTC").unwrap());
    }

    fn create_test_selector(
        actions: &[Action],
        states: &[State],
        start_datetime: Option<DateTime<Utc>>,
        end_datetime: Option<DateTime<Utc>>,
    ) -> TimelineSelector {
        TimelineSelector {
            timezone: "UTC".to_string(),
            start_datetime,
            end_datetime,
            start_timestamp: None,
            end_timestamp: None,
            range_on_completion_time: false,
            states: states.to_vec(),
            actions: actions.to_vec(),
            timeline_layout_version: 1, // Default to layout v1 for tests
        }
    }

    #[test]
    fn test_try_create_instant() {
        let selector = create_test_selector(&[Action::Commit], &[State::Completed], None, None);
        assert!(
            selector.try_create_instant("20240103153030999").is_err(),
            "Should fail to create instant as file name is invalid"
        );

        let instant_file_name = "20240103153030999.commit";

        let selector = create_test_selector(&[Action::Commit], &[State::Completed], None, None);
        assert!(selector.try_create_instant(instant_file_name).is_ok());

        let selector = create_test_selector(&[Action::Commit], &[State::Requested], None, None);
        assert!(
            selector.try_create_instant(instant_file_name).is_err(),
            "Should fail to create instant as state is different"
        );

        let selector =
            create_test_selector(&[Action::ReplaceCommit], &[State::Completed], None, None);
        assert!(
            selector.try_create_instant(instant_file_name).is_err(),
            "Should fail to create instant as action is different"
        );

        let selector = create_test_selector(
            &[Action::Commit],
            &[State::Completed],
            Instant::parse_datetime("20240103153031", "UTC").ok(),
            None,
        );
        assert!(
            selector.try_create_instant(instant_file_name).is_err(),
            "Should fail to create instant as timestamp is before start"
        );

        let selector = create_test_selector(
            &[Action::Commit],
            &[State::Completed],
            None,
            Instant::parse_datetime("20240103153030999", "UTC").ok(),
        );
        assert!(
            selector.try_create_instant(instant_file_name).is_err(),
            "Should fail to create instant as timestamp is at the end timestamp (exclusive)"
        );
    }

    async fn create_test_timeline() -> Timeline {
        let storage = Storage::new(
            Arc::new(HashMap::new()),
            Arc::new(HudiConfigs::new([
                (HudiTableConfig::BasePath, "file:///tmp/base"),
                (HudiTableConfig::TableVersion, "6"),
            ])),
        )
        .unwrap();
        let mut timeline = TimelineBuilder::new(
            Arc::new(HudiConfigs::new([
                (HudiTableConfig::BasePath, "file:///tmp/base"),
                (HudiTableConfig::TableVersion, "6"),
            ])),
            storage,
        )
        .build()
        .await
        .unwrap();
        timeline.completed_commits = vec![
            Instant::from_str("20240103153000.commit").unwrap(),
            Instant::from_str("20240103153010999.commit").unwrap(),
            Instant::from_str("20240103153020999.commit.requested").unwrap(),
            Instant::from_str("20240103153020999.inflight").unwrap(),
            Instant::from_str("20240103153020999.commit").unwrap(),
            Instant::from_str("20240103153030999.commit").unwrap(),
        ];
        timeline
    }

    /// Regression test: a layout-v2 incremental window bounds COMPLETION times.
    ///
    /// It used to bound requested times, which silently skipped any commit
    /// requested before the window that completed inside it — the normal shape of
    /// a slow or contended write. Both halves matter: the commit that only
    /// completion-time bounds admit must be returned, and the commit that only
    /// requested-time bounds would admit must not be.
    #[tokio::test]
    async fn test_completion_time_range_admits_by_completion_not_request() {
        fn instant(requested: &str, completed: &str) -> Instant {
            Instant {
                timestamp: requested.to_string(),
                completion_timestamp: Some(completed.to_string()),
                action: Action::Commit,
                state: State::Completed,
                epoch_millis: Instant::parse_datetime(requested, "UTC")
                    .unwrap()
                    .timestamp_millis(),
            }
        }

        // `early` was requested before the window and completed inside it;
        // `late` was requested inside the window and completed after it.
        let early = instant("20240101120000000", "20240101123000000");
        let late = instant("20240101124000000", "20240101130000000");

        let configs = Arc::new(HudiConfigs::new([
            (HudiTableConfig::BasePath.as_ref(), "file:///tmp/t"),
            (HudiTableConfig::TimelineLayoutVersion.as_ref(), "2"),
        ]));
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone()).unwrap();
        let mut timeline = TimelineBuilder::new(configs.clone(), storage)
            .build()
            .await
            .unwrap();
        timeline.completed_commits = vec![early.clone(), late.clone()];

        let window = TimelineSelector::completed_actions_in_completion_time_range(
            &[Action::Commit],
            configs.clone(),
            Some("20240101122000000"),
            Some("20240101125000000"),
        )
        .unwrap();
        let selected: Vec<String> = window
            .select(&timeline)
            .unwrap()
            .into_iter()
            .map(|i| i.timestamp)
            .collect();
        assert_eq!(
            selected,
            vec![early.timestamp.clone()],
            "the window admits the commit that COMPLETED inside it, and only that one"
        );

        // The same bounds read as requested times would have picked the other one.
        let by_request = TimelineSelector::completed_actions_in_range(
            &[Action::Commit],
            configs,
            Some("20240101122000000"),
            Some("20240101125000000"),
        )
        .unwrap();
        let by_request: Vec<String> = by_request
            .select(&timeline)
            .unwrap()
            .into_iter()
            .map(|i| i.timestamp)
            .collect();
        assert_eq!(
            by_request,
            vec![late.timestamp],
            "requested-time bounds pick the other commit — which is the bug"
        );
    }

    #[tokio::test]
    async fn test_select_no_instants() {
        let timeline = create_test_timeline().await;
        assert!(!timeline.completed_commits.is_empty());

        let selector = TimelineSelector {
            actions: vec![Action::ReplaceCommit],
            states: vec![State::Completed, State::Requested],
            start_datetime: None,
            end_datetime: None,
            start_timestamp: None,
            end_timestamp: None,
            range_on_completion_time: false,
            timezone: "UTC".to_string(),
            timeline_layout_version: 1,
        };
        assert!(selector.select(&timeline).unwrap().is_empty());
    }

    fn create_test_active_completed_selector(
        start: Option<&str>,
        end: Option<&str>,
    ) -> TimelineSelector {
        TimelineSelector {
            start_timestamp: None,
            end_timestamp: None,
            range_on_completion_time: false,
            states: vec![State::Completed],
            actions: vec![Action::Commit, Action::ReplaceCommit],
            start_datetime: start.map(|s| Instant::parse_datetime(s, "UTC").unwrap()),
            end_datetime: end.map(|s| Instant::parse_datetime(s, "UTC").unwrap()),
            timezone: "UTC".to_string(),
            timeline_layout_version: 1,
        }
    }

    #[test]
    fn test_layout_version_validation() {
        // Test layout v1 - should reject v8+ format for completed instants
        let selector_v1 = TimelineSelector {
            timezone: "UTC".to_string(),
            start_datetime: None,
            end_datetime: None,
            start_timestamp: None,
            end_timestamp: None,
            range_on_completion_time: false,
            states: vec![State::Completed],
            actions: vec![Action::DeltaCommit],
            timeline_layout_version: 1,
        };

        // v8+ format should be rejected for layout v1
        let result = selector_v1.try_create_instant("20240103153000_20240103153001.deltacommit");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unexpected v8+ instant format in timeline layout v1")
        );

        // pre-v8 format should work for layout v1
        assert!(
            selector_v1
                .try_create_instant("20240103153000.deltacommit")
                .is_ok()
        );

        // Test layout v2 - should reject pre-v8 format for completed instants
        let selector_v2 = TimelineSelector {
            timezone: "UTC".to_string(),
            start_datetime: None,
            end_datetime: None,
            start_timestamp: None,
            end_timestamp: None,
            range_on_completion_time: false,
            states: vec![State::Completed],
            actions: vec![Action::DeltaCommit],
            timeline_layout_version: 2,
        };

        // pre-v8 format should be rejected for layout v2 completed instants
        let result = selector_v2.try_create_instant("20240103153000.deltacommit");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Expected v8+ instant format")
        );

        // v8+ format should work for layout v2
        assert!(
            selector_v2
                .try_create_instant("20240103153000_20240103153001.deltacommit")
                .is_ok()
        );

        // Non-completed instants (inflight, requested) should work with standard format in both layouts
        let selector_v2_inflight = TimelineSelector {
            timezone: "UTC".to_string(),
            start_datetime: None,
            end_datetime: None,
            start_timestamp: None,
            end_timestamp: None,
            range_on_completion_time: false,
            states: vec![State::Inflight],
            actions: vec![Action::DeltaCommit],
            timeline_layout_version: 2,
        };

        assert!(
            selector_v2_inflight
                .try_create_instant("20240103153000.deltacommit.inflight")
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_timestamp_filtering() -> Result<()> {
        let timeline = create_test_timeline().await;

        let selector = create_test_active_completed_selector(None, None);
        let selected = selector.select(&timeline)?;
        assert_eq!(
            selected.iter().map(|i| &i.timestamp).collect::<Vec<_>>(),
            &[
                "20240103153000",
                "20240103153010999",
                "20240103153020999",
                "20240103153030999",
            ]
        );

        // starting from the earliest timestamp (exclusive)
        let selector = create_test_active_completed_selector(Some("20240103153000000"), None);
        let selected = selector.select(&timeline)?;
        assert_eq!(
            selected.iter().map(|i| &i.timestamp).collect::<Vec<_>>(),
            &[
                "20240103153010999",
                "20240103153020999",
                "20240103153030999",
            ]
        );

        // ending at the latest timestamp (inclusive)
        let selector = create_test_active_completed_selector(None, Some("20240103153030999"));
        let selected = selector.select(&timeline)?;
        assert_eq!(
            selected.iter().map(|i| &i.timestamp).collect::<Vec<_>>(),
            &[
                "20240103153000",
                "20240103153010999",
                "20240103153020999",
                "20240103153030999"
            ]
        );

        // start and end in the middle
        let selector = create_test_active_completed_selector(
            Some("20240103153010999"),
            Some("20240103153020999"),
        );
        let selected = selector.select(&timeline)?;
        assert_eq!(
            selected.iter().map(|i| &i.timestamp).collect::<Vec<_>>(),
            &["20240103153020999"]
        );
        Ok(())
    }

    /// The property a window cannot have: a set with a hole in it.
    ///
    /// The metadata table's valid-instant set excludes a *pending* data instant
    /// while including instants on either side. Any bounded range that admits
    /// both neighbours necessarily admits the pending one too, so this is the
    /// test that separates the two representations rather than merely exercising
    /// the new constructor.
    #[test]
    fn an_exact_match_set_can_exclude_an_instant_between_two_it_admits() {
        let range = InstantRange::exact_match(["20250101000000000", "20250103000000000"], "UTC");

        assert!(range.is_in_range("20250101000000000", "UTC").unwrap());
        assert!(range.is_in_range("20250103000000000", "UTC").unwrap());
        assert!(
            !range.is_in_range("20250102000000000", "UTC").unwrap(),
            "the instant between two admitted ones must be excluded -- this is \
             exactly what a bounded range cannot express"
        );

        // And the equivalent window really would admit it, which is what makes
        // the assertion above meaningful rather than trivially true.
        let window =
            InstantRange::within_open_closed("20241231000000000", "20250103000000000", "UTC");
        assert!(
            window.is_in_range("20250102000000000", "UTC").unwrap(),
            "a window covering both endpoints admits the hole, by construction"
        );
    }

    /// Membership ignores bounds entirely, as Java's EXACT_MATCH does.
    #[test]
    fn an_exact_match_set_admits_an_instant_no_window_would() {
        // An instant from outside the data timeline -- a metadata-only indexing
        // delta commit -- is admitted purely because it is in the set.
        let range = InstantRange::exact_match(["00000000000000000"], "UTC");
        assert!(range.is_in_range("00000000000000000", "UTC").unwrap());
        assert!(range.is_exact_match());
    }

    /// The lexicographic predicate must agree with the parsing one, since the
    /// log-block gate falls back to it for instants that will not parse.
    #[test]
    fn both_predicates_agree_on_an_exact_match_set() {
        let range = InstantRange::exact_match(["20250101000000000"], "UTC");
        for (instant, expected) in [("20250101000000000", true), ("20250102000000000", false)] {
            assert_eq!(
                range.is_in_range(instant, "UTC").unwrap(),
                expected,
                "parsing predicate disagreed on {instant}"
            );
            assert_eq!(
                range.is_in_range_lexicographic(instant),
                expected,
                "lexicographic predicate disagreed on {instant}"
            );
        }
    }

    /// An empty set admits nothing. A range that silently admitted everything
    /// when handed no instants would turn a filter into a no-op.
    #[test]
    fn an_empty_exact_match_set_admits_nothing() {
        let range = InstantRange::exact_match(Vec::<String>::new(), "UTC");
        assert!(!range.is_in_range("20250101000000000", "UTC").unwrap());
        assert!(!range.is_in_range_lexicographic("20250101000000000"));
    }
    /// `admits_all` is the whole safety argument for skipping the base file's
    /// materialization, so it is pinned directly.
    ///
    /// The merge-on-read path materializes the entire base file to apply an
    /// instant-range gate. Skipping that is safe only when the range excludes
    /// nothing — and on a table with `populate.meta.fields = false` the
    /// downstream row filter does nothing, so this gate is the only enforcement
    /// there. A predicate that answered `true` for a range that actually bounds
    /// something would silently drop that enforcement.
    #[test]
    fn admits_all_is_true_only_for_a_range_that_bounds_nothing() {
        let tz = "UTC".to_string();
        let range = |start: Option<&str>, end: Option<&str>| {
            InstantRange::new(
                tz.clone(),
                start.map(str::to_string),
                end.map(str::to_string),
                false,
                true,
            )
        };
        const MAX: &str = crate::file_group::reader_v2::MAX_INSTANT_TIME;

        // Bounds nothing.
        assert!(range(None, None).admits_all(), "no bounds at all");
        assert!(
            range(None, Some(MAX)).admits_all(),
            "the default the read path builds: unbounded end"
        );

        // Bounds something — each of these must materialize and filter.
        assert!(
            !range(Some("20240101000000000"), None).admits_all(),
            "a lower bound excludes earlier commits"
        );
        assert!(
            !range(Some("20240101000000000"), Some(MAX)).admits_all(),
            "a lower bound still bounds when the end is unbounded"
        );
        assert!(
            !range(None, Some("20240101000000000")).admits_all(),
            "an upper bound below the maximum excludes later commits"
        );
    }
}
