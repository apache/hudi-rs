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
use crate::config::table::{HudiTableConfig, TimelineTimezoneValue};
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

impl Default for TimelineSelector {
    fn default() -> Self {
        Self::empty()
    }
}

#[allow(dead_code)]
impl TimelineSelector {
    /// An empty [TimelineSelector] that selects no instants.
    pub fn empty() -> Self {
        Self {
            timezone: TimelineTimezoneValue::default().as_ref().to_string(),
            start_datetime: None,
            end_datetime: None,
            states: vec![],
            actions: vec![],
            include_archived: false,
        }
    }

    pub fn active_completed_commits(hudi_configs: Arc<HudiConfigs>) -> Self {
        let timezone = hudi_configs
            .get_or_default(HudiTableConfig::TimelineTimezone)
            .to::<String>();

        Self {
            timezone,
            start_datetime: None,
            end_datetime: None,
            states: vec![State::Completed],
            actions: vec![Action::Commit, Action::ReplaceCommit],
            include_archived: false,
        }
    }

    pub fn try_create_instant(&self, file_name: &str) -> Result<Instant> {
        let (timestamp, action_suffix) = file_name.split_once('.').ok_or_else(|| {
            CoreError::Timeline(format!(
                "Instant not created due to invalid file name: {}",
                file_name
            ))
        })?;

        let (action, state) = Instant::parse_action_and_state(action_suffix)?;

        if !self.actions.contains(&action) {
            return Err(CoreError::Timeline(format!(
                "Instant not created for due to unmatched action: {}",
                file_name
            )));
        }

        if !self.states.contains(&state) {
            return Err(CoreError::Timeline(format!(
                "Instant not created for due to unmatched state: {}",
                file_name
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

    /// Select loaded instants based on the given criteria.
    pub fn select(&self, timeline: &Timeline) -> Result<Vec<Instant>> {
        if self.states.is_empty() || self.actions.is_empty() {
            return Ok(vec![]);
        }

        let time_pruned_instants = if let Some(start) = self.start_datetime {
            // Find first instant >= start using binary search
            let start_pos = timeline
                .instants
                .partition_point(|instant| instant.epoch_millis < start.timestamp_millis());

            if let Some(end) = self.end_datetime {
                // Find first instant >= end using binary search
                let end_pos = timeline.instants[start_pos..]
                    .partition_point(|instant| instant.epoch_millis < end.timestamp_millis());
                &timeline.instants[start_pos..start_pos + end_pos]
            } else {
                &timeline.instants[start_pos..]
            }
        } else if let Some(end) = self.end_datetime {
            let end_pos = timeline
                .instants
                .partition_point(|instant| instant.epoch_millis < end.timestamp_millis());
            &timeline.instants[..end_pos]
        } else {
            &timeline.instants[..]
        };

        Ok(time_pruned_instants
            .iter()
            .filter(|instant| {
                self.actions.contains(&instant.action) && self.states.contains(&instant.state)
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
    use std::sync::Arc;

    fn create_test_instant(file_name: &str) -> Instant {
        Instant::try_from(file_name).unwrap()
    }

    async fn create_test_timeline() -> Timeline {
        let instants = vec![
            create_test_instant("20240103153000.commit"),
            create_test_instant("20240103153010999.commit"),
            create_test_instant("20240103153020999.commit.requested"),
            create_test_instant("20240103153020999.inflight"),
            create_test_instant("20240103153020999.commit"),
            create_test_instant("20240103153030999.commit"),
        ];
        Timeline::new_from_instants(
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

    #[test]
    fn test_default() {
        assert_eq!(TimelineSelector::default(), TimelineSelector::empty());
    }

    #[test]
    fn test_should_select() {
        let instant_file_name = "20240103153030999.commit";

        let selector = TimelineSelector {
            states: vec![State::Completed],
            actions: vec![Action::Commit],
            start_datetime: None,
            end_datetime: None,
            timezone: "UTC".to_string(),
            include_archived: false,
        };
        assert!(selector.try_create_instant(instant_file_name).is_ok());

        let selector = TimelineSelector {
            states: vec![State::Requested],
            actions: vec![Action::Commit],
            start_datetime: None,
            end_datetime: None,
            timezone: "UTC".to_string(),
            include_archived: false,
        };
        assert!(
            selector.try_create_instant(instant_file_name).is_err(),
            "Should fail to create instant as state is different"
        );

        let selector = TimelineSelector {
            states: vec![State::Completed],
            actions: vec![Action::ReplaceCommit],
            start_datetime: None,
            end_datetime: None,
            timezone: "UTC".to_string(),
            include_archived: false,
        };
        assert!(
            selector.try_create_instant(instant_file_name).is_err(),
            "Should fail to create instant as action is different"
        );

        let selector = TimelineSelector {
            states: vec![State::Completed],
            actions: vec![Action::Commit],
            start_datetime: Instant::parse_datetime("20240103153031", "UTC").ok(),
            end_datetime: None,
            timezone: "UTC".to_string(),
            include_archived: false,
        };
        assert!(
            selector.try_create_instant(instant_file_name).is_err(),
            "Should fail to create instant as timestamp is before start"
        );

        let selector = TimelineSelector {
            states: vec![State::Completed],
            actions: vec![Action::Commit],
            start_datetime: None,
            end_datetime: Instant::parse_datetime("20240103153030999", "UTC").ok(),
            timezone: "UTC".to_string(),
            include_archived: false,
        };
        assert!(
            selector.try_create_instant(instant_file_name).is_err(),
            "Should fail to create instant as timestamp is at the end timestamp (exclusive)"
        );
    }

    #[tokio::test]
    async fn test_select_no_instants() {
        let timeline = create_test_timeline().await;
        assert_not!(timeline.instants.is_empty());

        let selector = TimelineSelector::empty();
        assert!(selector.select(&timeline).unwrap().is_empty());

        let selector = TimelineSelector {
            states: vec![],
            actions: vec![Action::Commit],
            start_datetime: None,
            end_datetime: None,
            timezone: "UTC".to_string(),
            include_archived: false,
        };
        assert!(selector.select(&timeline).unwrap().is_empty());

        let selector = TimelineSelector {
            states: vec![State::Completed],
            actions: vec![],
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

        // starting from the earliest timestamp (inclusive)
        let selector = create_test_active_completed_selector(Some("20240103153000000"), None);
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

        // ending until the latest timestamp (exclusive)
        let selector = create_test_active_completed_selector(None, Some("20240103153030999"));
        let selected = selector.select(&timeline)?;
        assert_eq!(
            selected.iter().map(|i| &i.timestamp).collect::<Vec<_>>(),
            &["20240103153000", "20240103153010999", "20240103153020999",]
        );

        // start and end in the middle
        let selector = create_test_active_completed_selector(
            Some("20240103153010000"),
            Some("20240103153030000"),
        );
        let selected = selector.select(&timeline)?;
        assert_eq!(
            selected.iter().map(|i| &i.timestamp).collect::<Vec<_>>(),
            &["20240103153010999", "20240103153020999",]
        );
        Ok(())
    }
}
