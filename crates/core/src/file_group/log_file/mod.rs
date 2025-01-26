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
use crate::error::CoreError;
use crate::storage::file_metadata::FileMetadata;
use crate::Result;
use std::cmp::Ordering;
use std::str::FromStr;

mod log_block;
mod log_format;
pub mod reader;
pub mod scanner;

#[derive(Clone, Debug)]
pub struct LogFile {
    pub file_id: String,
    pub base_commit_timestamp: String,
    pub extension: String,
    pub version: String,
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
    /// .[File Id]_[Base Commit Timestamp].[Log File Extension].[Log File Version]_[File Write Token]
    /// ```
    /// TODO support `.cdc` suffix
    fn parse_file_name(file_name: &str) -> Result<(String, String, String, String, String)> {
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

        let base_commit_timestamp = parts[0];
        let log_file_extension = parts[1];
        let log_file_version = parts[2];

        if file_id.is_empty()
            || base_commit_timestamp.is_empty()
            || log_file_extension.is_empty()
            || log_file_version.is_empty()
            || file_write_token.is_empty()
        {
            return Err(CoreError::FileGroup(err_msg.clone()));
        }

        Ok((
            file_id.to_string(),
            base_commit_timestamp.to_string(),
            log_file_extension.to_string(),
            log_file_version.to_string(),
            file_write_token.to_string(),
        ))
    }

    #[inline]
    pub fn file_name(&self) -> String {
        format!(
            "{prefix}{file_id}_{base_commit_timestamp}.{extension}.{version}_{write_token}",
            prefix = LOG_FILE_PREFIX,
            file_id = self.file_id,
            base_commit_timestamp = self.base_commit_timestamp,
            extension = self.extension,
            version = self.version,
            write_token = self.write_token
        )
    }
}

impl FromStr for LogFile {
    type Err = CoreError;

    /// Parse a log file name into a [LogFile].
    fn from_str(file_name: &str) -> Result<Self, Self::Err> {
        let (file_id, base_commit_timestamp, extension, version, write_token) =
            Self::parse_file_name(file_name)?;
        Ok(LogFile {
            file_id,
            base_commit_timestamp,
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
        let (file_id, base_commit_timestamp, extension, version, write_token) =
            Self::parse_file_name(file_name)?;
        Ok(LogFile {
            file_id,
            base_commit_timestamp,
            extension,
            version,
            write_token,
            file_metadata: Some(metadata),
        })
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
        // Compare fields in order: base_commit_timestamp, version, write_token
        // TODO support `.cdc` suffix
        self.base_commit_timestamp
            .cmp(&other.base_commit_timestamp)
            .then(self.version.cmp(&other.version))
            .then(self.write_token.cmp(&other.write_token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_filename_parsing() {
        let filename = ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.1_0-51-115";
        let log_file = LogFile::from_str(filename).unwrap();

        assert_eq!(log_file.file_id, "54e9a5e9-ee5d-4ed2-acee-720b5810d380-0");
        assert_eq!(log_file.base_commit_timestamp, "20250109233025121");
        assert_eq!(log_file.extension, "log");
        assert_eq!(log_file.version, "1");
        assert_eq!(log_file.write_token, "0-51-115");
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
    fn test_log_file_ordering() {
        // Same timestamp, different version
        let log1 = LogFile {
            file_id: "ee2ace10-7667-40f5-9848-0a144b5ea064-0".to_string(),
            base_commit_timestamp: "20250113230302428".to_string(),
            extension: "log".to_string(),
            version: "1".to_string(),
            write_token: "0-188-387".to_string(),
            file_metadata: None,
        };

        let log2 = LogFile {
            file_id: "ee2ace10-7667-40f5-9848-0a144b5ea064-0".to_string(),
            base_commit_timestamp: "20250113230302428".to_string(),
            extension: "log".to_string(),
            version: "2".to_string(),
            write_token: "0-188-387".to_string(),
            file_metadata: None,
        };

        // Different timestamp
        let log3 = LogFile {
            file_id: "ee2ace10-7667-40f5-9848-0a144b5ea064-0".to_string(),
            base_commit_timestamp: "20250113230424191".to_string(),
            extension: "log".to_string(),
            version: "1".to_string(),
            write_token: "0-188-387".to_string(),
            file_metadata: None,
        };

        // Same timestamp and version, different write token
        let log4 = LogFile {
            file_id: "ee2ace10-7667-40f5-9848-0a144b5ea064-0".to_string(),
            base_commit_timestamp: "20250113230302428".to_string(),
            extension: "log".to_string(),
            version: "1".to_string(),
            write_token: "1-188-387".to_string(),
            file_metadata: None,
        };

        // Test ordering
        assert!(log1 < log2, "version ordering failed"); // version comparison
        assert!(log1 < log3, "timestamp ordering failed"); // timestamp comparison
        assert!(log2 < log3, "timestamp ordering failed"); // timestamp comparison
        assert!(log1 < log4, "write token ordering failed"); // write token comparison

        // Test sorting a vector of log files
        let mut logs = vec![log3.clone(), log4.clone(), log1.clone(), log2.clone()];
        logs.sort();
        assert_eq!(logs, vec![log1, log4, log2, log3]);
    }
}
