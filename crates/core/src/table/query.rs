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

use std::fmt;

/// Standardized query types for Hudi table operations
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum QueryType {
    Snapshot,
    TimeTravel {
        timestamp: String,
    },
    Incremental {
        start_timestamp: String,
        end_timestamp: Option<String>,
    },
    ReadOptimized,
}

impl fmt::Display for QueryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot => write!(f, "snapshot"),
            Self::TimeTravel { timestamp } => write!(f, "time_travel({})", timestamp),
            Self::Incremental {
                start_timestamp,
                end_timestamp,
            } => match end_timestamp {
                Some(end) => write!(f, "incremental({}, {})", start_timestamp, end),
                None => write!(f, "incremental({}, latest)", start_timestamp),
            },
            Self::ReadOptimized => write!(f, "read_optimized"),
        }
    }
}

impl QueryType {
    pub fn requires_timestamp(&self) -> bool {
        matches!(self, Self::TimeTravel { .. } | Self::Incremental { .. })
    }

    pub fn is_incremental(&self) -> bool {
        matches!(self, Self::Incremental { .. })
    }

    pub fn is_read_optimized(&self) -> bool {
        matches!(self, Self::ReadOptimized)
    }

    pub fn start_timestamp(&self) -> Option<&str> {
        match self {
            Self::Incremental {
                start_timestamp, ..
            } => Some(start_timestamp),
            _ => None,
        }
    }

    pub fn end_timestamp(&self) -> Option<&str> {
        match self {
            Self::TimeTravel { timestamp } => Some(timestamp),
            Self::Incremental { end_timestamp, .. } => end_timestamp.as_deref(),
            _ => None,
        }
    }
}

/// Helper constants for commonly used query types
impl QueryType {
    pub const SNAPSHOT: Self = Self::Snapshot;
    pub const READ_OPTIMIZED: Self = Self::ReadOptimized;
    pub fn time_travel(timestamp: impl Into<String>) -> Self {
        Self::TimeTravel {
            timestamp: timestamp.into(),
        }
    }
    pub fn incremental(
        start_timestamp: impl Into<String>,
        end_timestamp: Option<impl Into<String>>,
    ) -> Self {
        Self::Incremental {
            start_timestamp: start_timestamp.into(),
            end_timestamp: end_timestamp.map(Into::into),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_type_display() {
        assert_eq!(QueryType::SNAPSHOT.to_string(), "snapshot");
        assert_eq!(
            QueryType::time_travel("20231201120000").to_string(),
            "time_travel(20231201120000)"
        );
        assert_eq!(
            QueryType::incremental("20231201000000", Some("20231201235959")).to_string(),
            "incremental(20231201000000, 20231201235959)"
        );
        assert_eq!(
            QueryType::incremental("20231201000000", None::<String>).to_string(),
            "incremental(20231201000000, latest)"
        );
    }

    #[test]
    fn test_query_type_properties() {
        assert!(!QueryType::SNAPSHOT.requires_timestamp());
        assert!(QueryType::time_travel("20231201120000").requires_timestamp());
        assert!(QueryType::incremental("start", None::<String>).is_incremental());
        assert!(QueryType::READ_OPTIMIZED.is_read_optimized());
    }
}
