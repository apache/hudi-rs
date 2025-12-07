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
use crate::config::table::HudiTableConfig;
use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::timeline::instant::{Action, Instant, State};
use crate::timeline::Timeline;
use crate::Result;
use chrono::{DateTime, Utc};
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct InstantRange {
    timezone: String,
    start_timestamp: Option<String>,
    end_timestamp: Option<String>,
    start_inclusive: bool,
    end_inclusive: bool,
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
        }
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
    states: Vec<State>,
    actions: Vec<Action>,
    include_archived: bool,
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

    fn get_timeline_layout_version_from_configs(hudi_configs: &HudiConfigs) -> isize {
        // Try to get layout version from config, otherwise infer from table version
        if let Some(layout_version) = hudi_configs.try_get(HudiTableConfig::TimelineLayoutVersion) {
            layout_version.into()
        } else {
            // Apply same default logic as TimelineBuilder:
            // v8+ tables default to layout 2, earlier versions default to layout 1
            let table_version: isize = hudi_configs
                .try_get(HudiTableConfig::TableVersion)
                .map(|v| v.into())
                .unwrap_or(6); // Conservative default if table version is somehow missing
            if table_version >= 8 {
                2
            } else {
                1
            }
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
        let timezone = Self::get_timezone_from_configs(&hudi_configs);
        let timeline_layout_version = Self::get_timeline_layout_version_from_configs(&hudi_configs);
        let start_datetime = Self::parse_datetime(&timezone, start)?;
        let end_datetime = Self::parse_datetime(&timezone, end)?;
        Ok(Self {
            timezone,
            start_datetime,
            end_datetime,
            states: vec![State::Completed],
            actions: actions.to_vec(),
            include_archived: false,
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

        // Handle v8+ completed instant format: {requestedTimestamp}_{completedTimestamp}.{action}
        // Validate format based on timeline layout version and instant state
        let (timestamp, completed_timestamp) = if let Some((requested_ts, completed_ts)) =
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
                    "Expected v8+ instant format (with completed timestamp) in timeline layout v2 for completed instant: {file_name}"
                )));
            }
            (timestamp_part, None)
        };

        let dt = Instant::parse_datetime(timestamp, &self.timezone)?;
        if let Some(start) = self.start_datetime {
            if dt < start {
                return Err(CoreError::Timeline(format!(
                    "Instant not created for due to timestamp before start datetime: {}",
                    file_name
                )));
            }
        }

        if let Some(end) = self.end_datetime {
            if dt >= end {
                return Err(CoreError::Timeline(format!(
                    "Instant not created for due to timestamp after or at end datetime: {}",
                    file_name
                )));
            }
        }

        Ok(Instant {
            timestamp: timestamp.to_string(),
            completed_timestamp,
            epoch_millis: dt.timestamp_millis(),
            action,
            state,
        })
    }

    /// Select loaded instants based on the selector's properties.
    ///
    /// Instants timestamps should be in the range from start (exclusive) to end (inclusive).
    pub fn select(&self, timeline: &Timeline) -> Result<Vec<Instant>> {
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
    use crate::config::table::HudiTableConfig;
    use crate::config::HudiConfigs;
    use crate::storage::Storage;
    use crate::timeline::builder::TimelineBuilder;
    use crate::timeline::instant::{Action, Instant, State};
    use crate::timeline::Timeline;
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
            states: states.to_vec(),
            actions: actions.to_vec(),
            include_archived: false,
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

    #[tokio::test]
    async fn test_select_no_instants() {
        let timeline = create_test_timeline().await;
        assert!(!timeline.completed_commits.is_empty());

        let selector = TimelineSelector {
            actions: vec![Action::ReplaceCommit],
            states: vec![State::Completed, State::Requested],
            start_datetime: None,
            end_datetime: None,
            timezone: "UTC".to_string(),
            include_archived: false,
            timeline_layout_version: 1,
        };
        assert!(selector.select(&timeline).unwrap().is_empty());
    }

    fn create_test_active_completed_selector(
        start: Option<&str>,
        end: Option<&str>,
    ) -> TimelineSelector {
        TimelineSelector {
            states: vec![State::Completed],
            actions: vec![Action::Commit, Action::ReplaceCommit],
            start_datetime: start.map(|s| Instant::parse_datetime(s, "UTC").unwrap()),
            end_datetime: end.map(|s| Instant::parse_datetime(s, "UTC").unwrap()),
            timezone: "UTC".to_string(),
            include_archived: false,
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
            states: vec![State::Completed],
            actions: vec![Action::DeltaCommit],
            include_archived: false,
            timeline_layout_version: 1,
        };

        // v8+ format should be rejected for layout v1
        let result = selector_v1.try_create_instant("20240103153000_20240103153001.deltacommit");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unexpected v8+ instant format in timeline layout v1"));

        // pre-v8 format should work for layout v1
        assert!(selector_v1
            .try_create_instant("20240103153000.deltacommit")
            .is_ok());

        // Test layout v2 - should reject pre-v8 format for completed instants
        let selector_v2 = TimelineSelector {
            timezone: "UTC".to_string(),
            start_datetime: None,
            end_datetime: None,
            states: vec![State::Completed],
            actions: vec![Action::DeltaCommit],
            include_archived: false,
            timeline_layout_version: 2,
        };

        // pre-v8 format should be rejected for layout v2 completed instants
        let result = selector_v2.try_create_instant("20240103153000.deltacommit");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected v8+ instant format"));

        // v8+ format should work for layout v2
        assert!(selector_v2
            .try_create_instant("20240103153000_20240103153001.deltacommit")
            .is_ok());

        // Non-completed instants (inflight, requested) should work with standard format in both layouts
        let selector_v2_inflight = TimelineSelector {
            timezone: "UTC".to_string(),
            start_datetime: None,
            end_datetime: None,
            states: vec![State::Inflight],
            actions: vec![Action::DeltaCommit],
            include_archived: false,
            timeline_layout_version: 2,
        };

        assert!(selector_v2_inflight
            .try_create_instant("20240103153000.deltacommit.inflight")
            .is_ok());
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
}
