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
#[derive(Debug, Clone, PartialEq)]
pub struct TimelineSelector {
    timezone: String,
    start_datetime: Option<DateTime<Utc>>,
    end_datetime: Option<DateTime<Utc>>,
    states: Vec<State>,
    actions: Vec<Action>,
    include_archived: bool,
}

#[allow(dead_code)]
impl TimelineSelector {
    fn get_timezone_from_configs(hudi_configs: Arc<HudiConfigs>) -> String {
        hudi_configs
            .get_or_default(HudiTableConfig::TimelineTimezone)
            .to::<String>()
    }

    pub fn completed_commits(hudi_configs: Arc<HudiConfigs>) -> Result<Self> {
        Self::completed_commits_in_range(hudi_configs, None, None)
    }

    pub fn completed_commits_in_range(
        hudi_configs: Arc<HudiConfigs>,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<Self> {
        let timezone = Self::get_timezone_from_configs(hudi_configs);
        let start_datetime = start
            .map(|s| Instant::parse_datetime(s, &timezone))
            .transpose()?;
        let end_datetime = end
            .map(|e| Instant::parse_datetime(e, &timezone))
            .transpose()?;
        Ok(Self {
            timezone,
            start_datetime,
            end_datetime,
            states: vec![State::Completed],
            actions: vec![Action::Commit, Action::DeltaCommit, Action::ReplaceCommit],
            include_archived: false,
        })
    }

    pub fn completed_replacecommits(hudi_configs: Arc<HudiConfigs>) -> Self {
        Self {
            timezone: Self::get_timezone_from_configs(hudi_configs),
            start_datetime: None,
            end_datetime: None,
            states: vec![State::Completed],
            actions: vec![Action::ReplaceCommit],
            include_archived: false,
        }
    }

    pub fn should_include_action(&self, action: &Action) -> bool {
        self.actions.is_empty() || self.actions.contains(action)
    }

    pub fn should_include_state(&self, state: &State) -> bool {
        self.states.is_empty() || self.states.contains(state)
    }

    pub fn try_create_instant(&self, file_name: &str) -> Result<Instant> {
        let (timestamp, action_suffix) = file_name.split_once('.').ok_or_else(|| {
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
    use hudi_tests::assert_not;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;

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
        let instants = vec![
            Instant::from_str("20240103153000.commit").unwrap(),
            Instant::from_str("20240103153010999.commit").unwrap(),
            Instant::from_str("20240103153020999.commit.requested").unwrap(),
            Instant::from_str("20240103153020999.inflight").unwrap(),
            Instant::from_str("20240103153020999.commit").unwrap(),
            Instant::from_str("20240103153030999.commit").unwrap(),
        ];
        Timeline::new_from_completed_commits(
            Arc::new(HudiConfigs::new([(
                HudiTableConfig::BasePath,
                "file:///tmp/base",
            )])),
            Arc::new(HashMap::new()),
            instants,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_select_no_instants() {
        let timeline = create_test_timeline().await;
        assert_not!(timeline.completed_commits.is_empty());

        let selector = TimelineSelector {
            actions: vec![Action::ReplaceCommit],
            states: vec![State::Completed, State::Requested],
            start_datetime: None,
            end_datetime: None,
            timezone: "UTC".to_string(),
            include_archived: false,
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
        }
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
