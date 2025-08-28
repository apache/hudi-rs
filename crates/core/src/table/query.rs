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
//! Standardized query types for Hudi table read operations.

use std::str::FromStr;
use strum_macros::AsRefStr;

use crate::config::error::ConfigError;
use crate::config::error::ConfigError::InvalidValue;

/// The kind of read query a caller intends to perform against a Hudi table.
///
/// Use this enum to communicate intent across API boundaries; the timestamps
/// associated with time-travel or incremental reads are passed as separate
/// parameters to the corresponding [`crate::table::Table`] read APIs.
#[derive(Clone, Debug, PartialEq, AsRefStr)]
pub enum QueryType {
    /// Read the latest committed records, optionally as of a given timestamp
    /// (time-travel) when supported by the caller's API.
    #[strum(serialize = "snapshot")]
    Snapshot,
    /// Read only the base files, skipping unmerged log files. Applicable to
    /// Merge-on-Read tables.
    #[strum(serialize = "read_optimized")]
    ReadOptimized,
    /// Read only the records inserted or updated within a given time range.
    #[strum(serialize = "incremental")]
    Incremental,
}

impl FromStr for QueryType {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "snapshot" => Ok(Self::Snapshot),
            "read_optimized" | "read-optimized" => Ok(Self::ReadOptimized),
            "incremental" => Ok(Self::Incremental),
            v => Err(InvalidValue(v.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_ref_returns_canonical_string() {
        assert_eq!(QueryType::Snapshot.as_ref(), "snapshot");
        assert_eq!(QueryType::ReadOptimized.as_ref(), "read_optimized");
        assert_eq!(QueryType::Incremental.as_ref(), "incremental");
    }

    #[test]
    fn from_str_accepts_aliases_case_insensitively() {
        assert_eq!(
            QueryType::from_str("snapshot").unwrap(),
            QueryType::Snapshot
        );
        assert_eq!(
            QueryType::from_str("SNAPSHOT").unwrap(),
            QueryType::Snapshot
        );
        assert_eq!(
            QueryType::from_str("read_optimized").unwrap(),
            QueryType::ReadOptimized
        );
        assert_eq!(
            QueryType::from_str("Read-Optimized").unwrap(),
            QueryType::ReadOptimized
        );
        assert_eq!(
            QueryType::from_str("Incremental").unwrap(),
            QueryType::Incremental
        );
    }

    #[test]
    fn from_str_rejects_unknown_or_empty_value() {
        assert!(matches!(
            QueryType::from_str("").unwrap_err(),
            InvalidValue(_)
        ));
        assert!(matches!(
            QueryType::from_str("foo").unwrap_err(),
            InvalidValue(_)
        ));
        assert!(matches!(
            QueryType::from_str("snapshotx").unwrap_err(),
            InvalidValue(_)
        ));
    }
}
