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
use crate::config::table::TimelineTimezoneValue;
use crate::error::CoreError;
use crate::metadata::HUDI_METADATA_DIR;
use crate::storage::error::StorageError;
use crate::Result;
use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Timelike, Utc};
use std::cmp::Ordering;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Action {
    Commit,
    DeltaCommit,
    ReplaceCommit,
}

impl FromStr for Action {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "commit" => Ok(Action::Commit),
            "deltacommit" => Ok(Action::DeltaCommit),
            "replacecommit" => Ok(Action::ReplaceCommit),
            _ => Err(CoreError::Timeline(format!("Invalid action: {}", s))),
        }
    }
}

impl AsRef<str> for Action {
    fn as_ref(&self) -> &str {
        match self {
            Action::Commit => "commit",
            Action::DeltaCommit => "deltacommit",
            Action::ReplaceCommit => "replacecommit",
        }
    }
}

impl Action {
    pub fn is_replacecommit(&self) -> bool {
        self == &Action::ReplaceCommit
    }
}

/// The [State] of an [Instant] represents the status of the action performed on the table.
#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum State {
    Requested,
    Inflight,
    Completed,
}

impl FromStr for State {
    type Err = CoreError;

    fn from_str(suffix: &str) -> Result<Self> {
        match suffix {
            "requested" => Ok(State::Requested),
            "inflight" => Ok(State::Inflight),
            "" => Ok(State::Completed),
            _ => Err(CoreError::Timeline(format!(
                "Invalid state suffix: {}",
                suffix
            ))),
        }
    }
}

impl AsRef<str> for State {
    fn as_ref(&self) -> &str {
        match self {
            State::Requested => "requested",
            State::Inflight => "inflight",
            State::Completed => "",
        }
    }
}

/// An [Instant] represents a point in time when an action was performed on the table.
#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Instant {
    pub timestamp: String,
    pub action: Action,
    pub state: State,
    pub epoch_millis: i64,
}

impl PartialOrd for Instant {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Instant {
    fn cmp(&self, other: &Self) -> Ordering {
        self.epoch_millis
            .cmp(&other.epoch_millis)
            .then_with(|| self.state.cmp(&other.state))
    }
}

impl FromStr for Instant {
    type Err = CoreError;

    /// Parse a timeline file name into an [Instant]. Timezone is assumed to be UTC.
    fn from_str(file_name: &str) -> Result<Self, Self::Err> {
        Self::try_from_file_name_and_timezone(file_name, "UTC")
    }
}

impl Instant {
    pub fn try_from_file_name_and_timezone(file_name: &str, timezone: &str) -> Result<Self> {
        let (timestamp, action_suffix) = file_name
            .split_once('.')
            .ok_or_else(|| CoreError::Timeline(format!("Invalid file name: {}", file_name)))?;
        Self::validate_timestamp(timestamp)?;
        let dt = Self::parse_datetime(timestamp, timezone)?;
        let (action, state) = Self::parse_action_and_state(action_suffix)?;

        Ok(Self {
            timestamp: timestamp.to_string(),
            state,
            action,
            epoch_millis: dt.timestamp_millis(),
        })
    }

    fn validate_timestamp(timestamp: &str) -> Result<()> {
        if !matches!(timestamp.len(), 14 | 17) {
            return Err(CoreError::Timeline(format!(
                "Timestamp must be in format yyyyMMddHHmmss or yyyyMMddHHmmssSSS, but got: {}",
                timestamp
            )));
        }
        Ok(())
    }

    pub fn parse_datetime(timestamp: &str, timezone: &str) -> Result<DateTime<Utc>> {
        let naive_dt = Self::parse_naive_datetime(timestamp)?;
        Self::convert_to_timezone(naive_dt, timezone)
    }

    fn parse_naive_datetime(timestamp: &str) -> Result<NaiveDateTime> {
        let naive_dt = NaiveDateTime::parse_from_str(&timestamp[..14], "%Y%m%d%H%M%S")
            .map_err(|e| CoreError::Timeline(format!("Failed to parse timestamp: {}", e)))?;

        if timestamp.len() == 17 {
            let millis: u32 = timestamp[14..]
                .parse()
                .map_err(|e| CoreError::Timeline(format!("Failed to parse milliseconds: {}", e)))?;
            naive_dt
                .with_nanosecond(millis * 1_000_000)
                .ok_or_else(|| CoreError::Timeline("Invalid milliseconds".to_string()))
        } else {
            Ok(naive_dt)
        }
    }

    fn convert_to_timezone(naive_dt: NaiveDateTime, timezone: &str) -> Result<DateTime<Utc>> {
        match TimelineTimezoneValue::from_str(timezone)? {
            TimelineTimezoneValue::UTC => Ok(DateTime::from_naive_utc_and_offset(naive_dt, Utc)),
            TimelineTimezoneValue::Local => Ok(Local
                .from_local_datetime(&naive_dt)
                .single()
                .ok_or_else(|| CoreError::Timeline("Invalid local datetime".to_string()))?
                .to_utc()),
        }
    }

    pub fn parse_action_and_state(action_suffix: &str) -> Result<(Action, State)> {
        match action_suffix.split_once('.') {
            Some((action_str, state_str)) => {
                Ok((Action::from_str(action_str)?, State::from_str(state_str)?))
            }
            None => match action_suffix {
                "inflight" => Ok((Action::Commit, State::Inflight)),
                action_str => Ok((Action::from_str(action_str)?, State::Completed)),
            },
        }
    }

    pub fn file_name(&self) -> String {
        match (&self.action, &self.state) {
            (_, State::Completed) => format!("{}.{}", self.timestamp, self.action.as_ref()),
            (Action::Commit, State::Inflight) => {
                format!("{}.{}", self.timestamp, self.state.as_ref())
            }
            _ => format!(
                "{}.{}.{}",
                self.timestamp,
                self.action.as_ref(),
                self.state.as_ref()
            ),
        }
    }

    pub fn relative_path(&self) -> Result<String> {
        let mut commit_file_path = PathBuf::from(HUDI_METADATA_DIR);
        commit_file_path.push(self.file_name());
        commit_file_path
            .to_str()
            .ok_or(StorageError::InvalidPath(format!(
                "Failed to get file path for {:?}",
                self
            )))
            .map_err(CoreError::Storage)
            .map(|s| s.to_string())
    }

    pub fn is_replacecommit(&self) -> bool {
        self.action.is_replacecommit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_methods() {
        assert_eq!(Action::Commit.as_ref(), "commit");
        assert_eq!(Action::ReplaceCommit.as_ref(), "replacecommit");

        assert!(!Action::Commit.is_replacecommit());
        assert!(Action::ReplaceCommit.is_replacecommit());
    }

    #[test]
    fn test_action_from_str() {
        assert_eq!(Action::from_str("commit").unwrap(), Action::Commit);
        assert_eq!(
            Action::from_str("replacecommit").unwrap(),
            Action::ReplaceCommit
        );
        assert!(Action::from_str("invalid").is_err());
    }

    #[test]
    fn test_state_methods() {
        assert_eq!(State::Requested.as_ref(), "requested");
        assert_eq!(State::Inflight.as_ref(), "inflight");
        assert_eq!(State::Completed.as_ref(), "");
    }

    #[test]
    fn test_state_from_str() {
        assert_eq!(State::from_str("requested").unwrap(), State::Requested);
        assert_eq!(State::from_str("inflight").unwrap(), State::Inflight);
        assert_eq!(State::from_str("").unwrap(), State::Completed);
        assert!(State::from_str("invalid").is_err());
    }

    #[test]
    fn test_state_ordering() {
        assert!(State::Requested < State::Inflight);
        assert!(State::Inflight < State::Completed);
        assert!(State::Requested < State::Completed);

        assert!(State::Completed > State::Inflight);
        assert!(State::Inflight > State::Requested);
        assert!(State::Completed > State::Requested);
    }

    #[test]
    fn test_instant_from_file_name() -> Result<()> {
        // Test completed commit
        let instant = Instant::from_str("20240103153000.commit")?;
        assert_eq!(instant.timestamp, "20240103153000");
        assert_eq!(instant.action, Action::Commit);
        assert_eq!(instant.state, State::Completed);

        // Test inflight replacecommit with milliseconds
        let instant = Instant::from_str("20240103153000123.replacecommit.inflight")?;
        assert_eq!(instant.timestamp, "20240103153000123");
        assert_eq!(instant.action, Action::ReplaceCommit);
        assert_eq!(instant.state, State::Inflight);

        Ok(())
    }

    #[test]
    fn test_invalid_file_names() {
        // Invalid timestamp format
        assert!(Instant::from_str("2024010315.commit").is_err());

        // Invalid action
        assert!(Instant::from_str("20240103153000.invalid").is_err());

        // Invalid state
        assert!(Instant::from_str("20240103153000.commit.invalid").is_err());

        // No dot separator
        assert!(Instant::from_str("20240103153000commit").is_err());
    }

    #[test]
    fn test_file_name_roundtrip() -> Result<()> {
        let test_cases = vec![
            "20240103153000.commit",
            "20240103153000.commit.requested",
            "20240103153000.replacecommit",
            "20240103153000123.inflight",
        ];

        for original_name in test_cases {
            let instant = Instant::from_str(original_name)?;
            assert_eq!(instant.file_name(), original_name);
        }

        Ok(())
    }

    #[test]
    fn test_instant_ordering() -> Result<()> {
        let instant1 = Instant::from_str("20240103153000.commit")?;
        let instant2 = Instant::from_str("20240103153000001.commit")?;
        let instant3 = Instant::from_str("20240103153000999.commit.requested")?;
        let instant4 = Instant::from_str("20240103153000999.inflight")?;
        let instant5 = Instant::from_str("20240103153000999.commit")?;
        let instant6 = Instant::from_str("20240103153001.commit")?;

        assert!(instant1 < instant2);
        assert!(instant2 < instant3);
        assert!(instant3 < instant4);
        assert!(instant4 < instant5);
        assert!(instant5 < instant6);

        Ok(())
    }

    #[test]
    fn test_relative_path() {
        let instant = Instant::from_str("20240103153000.commit").unwrap();
        assert_eq!(
            instant.relative_path().unwrap(),
            ".hoodie/20240103153000.commit"
        );
    }

    #[test]
    fn test_create_instant_using_local_timezone() {
        // Set a fixed timezone for consistent testing
        let original_tz = std::env::var("TZ").ok();
        std::env::set_var("TZ", "Etc/GMT+5"); // UTC-5 fixed timezone with no DST

        let file_name = "20240103153000.commit";
        let instant_local = Instant::try_from_file_name_and_timezone(file_name, "local").unwrap();
        let instant_utc = Instant::try_from_file_name_and_timezone(file_name, "utc").unwrap();
        let offset_seconds = (instant_local.epoch_millis - instant_utc.epoch_millis) / 1000;

        // Expected: In UTC+5, the offset should be exactly 5 hours
        assert_eq!(offset_seconds, 5 * 3600);

        // Restore original TZ
        match original_tz {
            Some(tz) => std::env::set_var("TZ", tz),
            None => std::env::remove_var("TZ"),
        }
    }
}
