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
use crate::error::CoreError;
use crate::storage::file_metadata::FileMetadata;
use crate::timeline::completion_time::CompletionTimeView;
use std::cmp::Ordering;
use std::fmt::Display;
use std::str::FromStr;

mod avro;
mod content;
mod log_block;
mod log_format;
pub mod reader;
pub mod scanner;

/// Represents a Hudi log file (delta log).
///
/// The `timestamp` field is the timestamp embedded in the log file name:
/// - For v6 tables: base commit timestamp (matches the base file's commit timestamp).
/// - For v8+ tables: request instant timestamp of the deltacommit.
///
/// The `completion_timestamp` field is used to determine the correct ordering and
/// file slice association:
/// - For v6 tables: This is always `None` (v6 does not track completion timestamps).
/// - For v8+ tables: Set from the timeline when the commit is completed.
///   If `None`, the commit is still pending and the file should not be included in queries.
///
/// Ordering is based on `completion_timestamp` (if available), falling back to `timestamp`
/// for uncommitted files. Completed files are ordered before uncommitted ones.
#[derive(Clone, Debug)]
pub struct LogFile {
    pub file_id: String,
    /// The timestamp embedded in the log file name.
    pub timestamp: String,
    /// The completion timestamp of the commit that wrote this log file.
    ///
    /// For v6 tables: This is always `None` (v6 does not track completion timestamps).
    /// For v8+ tables: Set from the timeline; `None` if commit is pending.
    pub completion_timestamp: Option<String>,
    pub extension: String,
    /// Log file version number. Starts at 1 and increments when the log file rolls over
    /// (e.g., reaches size limit) within the same delta commit by the same writer.
    pub version: u32,
    pub write_token: String,
    pub file_metadata: Option<FileMetadata>,
}

const LOG_FILE_PREFIX: char = '.';

impl LogFile {
    /// Parse a log file's name into parts.
    ///
    /// File name format:
    ///
    /// ```text
    /// .[File Id]_[Base commit or deltacommit's timestamp].[Log File Extension].[Log File Version]_[File Write Token]
    /// ```
    /// TODO support `.cdc` suffix
    fn parse_file_name(file_name: &str) -> Result<(String, String, String, u32, String)> {
        let err_msg = format!("Failed to parse file name '{file_name}' for log file.");

        if !file_name.starts_with(LOG_FILE_PREFIX) {
            return Err(CoreError::FileGroup(err_msg));
        }

        let file_name = &file_name[LOG_FILE_PREFIX.len_utf8()..];

        let (file_id, rest) = file_name
            .split_once('_')
            .ok_or_else(|| CoreError::FileGroup(err_msg.clone()))?;

        let (middle, file_write_token) = rest
            .rsplit_once('_')
            .ok_or_else(|| CoreError::FileGroup(err_msg.clone()))?;

        let parts: Vec<&str> = middle.split('.').collect();
        if parts.len() != 3 {
            return Err(CoreError::FileGroup(err_msg.clone()));
        }

        let timestamp = parts[0];
        let log_file_extension = parts[1];
        let log_file_version_str = parts[2];

        if file_id.is_empty()
            || timestamp.is_empty()
            || log_file_extension.is_empty()
            || log_file_version_str.is_empty()
            || file_write_token.is_empty()
        {
            return Err(CoreError::FileGroup(err_msg.clone()));
        }

        let log_file_version = log_file_version_str
            .parse::<u32>()
            .map_err(|_| CoreError::FileGroup(err_msg.clone()))?;

        Ok((
            file_id.to_string(),
            timestamp.to_string(),
            log_file_extension.to_string(),
            log_file_version,
            file_write_token.to_string(),
        ))
    }

    #[inline]
    pub fn file_name(&self) -> String {
        format!(
            "{prefix}{file_id}_{timestamp}.{extension}.{version}_{write_token}",
            prefix = LOG_FILE_PREFIX,
            file_id = self.file_id,
            timestamp = self.timestamp,
            extension = self.extension,
            version = self.version,
            write_token = self.write_token
        )
    }

    /// Returns true if this log file has a completion timestamp (i.e., the commit is completed).
    #[inline]
    pub fn is_completed(&self) -> bool {
        self.completion_timestamp.is_some()
    }

    /// Set the completion timestamp from a completion time view.
    ///
    /// Looks up the completion timestamp using this file's `timestamp`
    /// (request time) and sets `completion_timestamp` if found.
    ///
    /// For v6 tables, the view returns `None` and this is a no-op.
    /// For v8+ tables, this sets the completion timestamp for completed commits.
    pub fn set_completion_time<V: CompletionTimeView>(&mut self, view: &V) {
        self.completion_timestamp = view
            .get_completion_time(&self.timestamp)
            .map(|s| s.to_string());
    }
}

impl FromStr for LogFile {
    type Err = CoreError;

    /// Parse a log file name into a [LogFile].
    ///
    /// Note: `completion_timestamp` is set to `None` by default. For v6 tables,
    /// it should remain `None` (v6 does not track completion times). For v8+ tables,
    /// the caller should set it from the timeline.
    fn from_str(file_name: &str) -> Result<Self, Self::Err> {
        let (file_id, timestamp, extension, version, write_token) =
            Self::parse_file_name(file_name)?;
        Ok(LogFile {
            file_id,
            timestamp,
            completion_timestamp: None,
            extension,
            version,
            write_token,
            file_metadata: None,
        })
    }
}

impl TryFrom<FileMetadata> for LogFile {
    type Error = CoreError;

    fn try_from(metadata: FileMetadata) -> Result<Self> {
        let file_name = metadata.name.as_str();
        let (file_id, timestamp, extension, version, write_token) =
            Self::parse_file_name(file_name)?;
        Ok(LogFile {
            file_id,
            timestamp,
            completion_timestamp: None,
            extension,
            version,
            write_token,
            file_metadata: Some(metadata),
        })
    }
}

impl Display for LogFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogFile: {}", self.file_name())
    }
}

impl PartialEq for LogFile {
    fn eq(&self, other: &Self) -> bool {
        self.file_name() == other.file_name()
    }
}

impl Eq for LogFile {}

impl PartialOrd for LogFile {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LogFile {
    fn cmp(&self, other: &Self) -> Ordering {
        // For ordering, use completion_timestamp when available.
        // Files with completion_timestamp are considered earlier than those without.
        // If both have completion_timestamp, compare by completion_timestamp.
        // If both lack completion_timestamp, compare by request timestamp.
        // TODO support `.cdc` suffix
        match (&self.completion_timestamp, &other.completion_timestamp) {
            (Some(ct1), Some(ct2)) => {
                // Both completed: compare by completion timestamp, then version, then write_token
                ct1.cmp(ct2)
                    .then(self.version.cmp(&other.version))
                    .then(self.write_token.cmp(&other.write_token))
            }
            (Some(_), None) => {
                // Self is completed, other is pending: self comes first
                Ordering::Less
            }
            (None, Some(_)) => {
                // Self is pending, other is completed: other comes first
                Ordering::Greater
            }
            (None, None) => {
                // Both pending or both v6 (no completion_timestamp set yet):
                // compare by request timestamp for deterministic ordering
                self.timestamp
                    .cmp(&other.timestamp)
                    .then(self.version.cmp(&other.version))
                    .then(self.write_token.cmp(&other.write_token))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_file_name_in_formatted_str() {
        let filename = ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.1_0-51-115";
        let log_file = LogFile::from_str(filename).unwrap();
        assert!(format!("{log_file}").contains(filename));
    }

    #[test]
    fn test_valid_filename_parsing() {
        let filename = ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.1_0-51-115";
        let log_file = LogFile::from_str(filename).unwrap();

        assert_eq!(log_file.file_id, "54e9a5e9-ee5d-4ed2-acee-720b5810d380-0");
        assert_eq!(log_file.timestamp, "20250109233025121");
        assert_eq!(log_file.extension, "log");
        assert_eq!(log_file.version, 1);
        assert_eq!(log_file.write_token, "0-51-115");
        // Completion timestamp is None after parsing (set separately from timeline)
        assert_eq!(log_file.completion_timestamp, None);
        assert!(!log_file.is_completed());
    }

    #[test]
    fn test_filename_reconstruction() {
        let original = ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.1_0-51-115";
        let log_file = LogFile::from_str(original).unwrap();
        assert_eq!(log_file.file_name(), original);
    }

    #[test]
    fn test_missing_dot_prefix() {
        let filename = "myfile_20250109233025121.log.v1_abc123";
        assert!(matches!(
            LogFile::from_str(filename),
            Err(CoreError::FileGroup(_))
        ));
    }

    #[test]
    fn test_missing_first_underscore() {
        let filename = ".myfile20250109233025121.log.v1_abc123";
        assert!(matches!(
            LogFile::from_str(filename),
            Err(CoreError::FileGroup(_))
        ));
    }

    #[test]
    fn test_missing_last_underscore() {
        let filename = ".myfile_20250109233025121.log.v1abc123";
        assert!(matches!(
            LogFile::from_str(filename),
            Err(CoreError::FileGroup(_))
        ));
    }

    #[test]
    fn test_incorrect_dot_parts() {
        let filename = ".myfile_20250109233025121.log.v1.extra_abc123";
        assert!(matches!(
            LogFile::from_str(filename),
            Err(CoreError::FileGroup(_))
        ));
    }

    #[test]
    fn test_empty_components() {
        let filenames = vec![
            "._20250109233025121.log.v1_abc123",     // empty file_id
            ".myfile_.log.v1_abc123",                // empty timestamp
            ".myfile_20250109233025121..v1_abc123",  // empty extension
            ".myfile_20250109233025121.log._abc123", // empty version
            ".myfile_20250109233025121.log.v1_",     // empty token
        ];

        for filename in filenames {
            assert!(matches!(
                LogFile::from_str(filename),
                Err(CoreError::FileGroup(_))
            ));
        }
    }

    #[test]
    fn test_log_file_ordering_no_completion_timestamp() {
        // When no completion_timestamp is set, ordering falls back to request timestamp
        // This simulates v6 table behavior or uncommitted files in v8+ tables
        let log1 = LogFile {
            file_id: "ee2ace10-7667-40f5-9848-0a144b5ea064-0".to_string(),
            timestamp: "20250113230302428".to_string(),
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            file_metadata: None,
        };

        let log2 = LogFile {
            file_id: "ee2ace10-7667-40f5-9848-0a144b5ea064-0".to_string(),
            timestamp: "20250113230302428".to_string(),
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 2,
            write_token: "0-188-387".to_string(),
            file_metadata: None,
        };

        // Different timestamp
        let log3 = LogFile {
            file_id: "ee2ace10-7667-40f5-9848-0a144b5ea064-0".to_string(),
            timestamp: "20250113230424191".to_string(),
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            file_metadata: None,
        };

        // Same timestamp and version, different write token
        let log4 = LogFile {
            file_id: "ee2ace10-7667-40f5-9848-0a144b5ea064-0".to_string(),
            timestamp: "20250113230302428".to_string(),
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 1,
            write_token: "1-188-387".to_string(),
            file_metadata: None,
        };

        // Test ordering by timestamp, then version, then write_token
        assert!(log1 < log2, "version ordering failed");
        assert!(log1 < log3, "timestamp ordering failed");
        assert!(log2 < log3, "timestamp ordering failed");
        assert!(log1 < log4, "write token ordering failed");

        // Test sorting
        let mut logs = vec![log3.clone(), log4.clone(), log1.clone(), log2.clone()];
        logs.sort();
        assert_eq!(logs, vec![log1, log4, log2, log3]);

        // Test version 10 > version 2 (integer ordering, not string ordering)
        let log_v2 = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230302428".to_string(),
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 2,
            write_token: "0-188-387".to_string(),
            file_metadata: None,
        };

        let log_v10 = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230302428".to_string(),
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 10,
            write_token: "0-188-387".to_string(),
            file_metadata: None,
        };

        assert!(
            log_v2 < log_v10,
            "version 2 should be less than version 10 (integer ordering)"
        );
    }

    #[test]
    fn test_log_file_ordering_by_completion_time() {
        // Log file with earlier completion timestamp
        let log1 = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230302428".to_string(),
            completion_timestamp: Some("20250113230310000".to_string()),
            extension: "log".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            file_metadata: None,
        };

        // Log file with later completion timestamp (but earlier request timestamp)
        let log2 = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230300000".to_string(), // Earlier request time
            completion_timestamp: Some("20250113230320000".to_string()), // Later completion time
            extension: "log".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            file_metadata: None,
        };

        // Uncommitted log file (no completion timestamp)
        let log3 = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230305000".to_string(),
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            file_metadata: None,
        };

        // Test ordering: completed files come before uncommitted, ordered by completion time
        assert!(
            log1 < log2,
            "log1 should be before log2 (earlier completion)"
        );
        assert!(
            log1 < log3,
            "completed log1 should be before uncommitted log3"
        );
        assert!(
            log2 < log3,
            "completed log2 should be before uncommitted log3"
        );

        // Test sorting
        let mut logs = vec![log3.clone(), log1.clone(), log2.clone()];
        logs.sort();
        assert_eq!(logs, vec![log1, log2, log3]);
    }
}
