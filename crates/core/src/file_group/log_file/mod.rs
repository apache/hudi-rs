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
use crate::Result;
use std::str::FromStr;

mod log_block;
mod log_format;
pub mod reader;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogFile {
    file_id: String,
    base_commit_timestamp: String,
    log_file_extension: String,
    log_file_version: String,
    file_write_token: String,
}

const LOG_FILE_PREFIX: char = '.';

impl FromStr for LogFile {
    type Err = CoreError;

    /// Parse a log file name into a [LogFile].
    ///
    /// File name format:
    ///
    /// ```text
    /// .[File Id]_[Base Commit Timestamp].[Log File Extension].[Log File Version]_[File Write Token]
    /// ```
    fn from_str(file_name: &str) -> Result<Self, Self::Err> {
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

        Ok(LogFile {
            file_id: file_id.to_string(),
            base_commit_timestamp: base_commit_timestamp.to_string(),
            log_file_extension: log_file_extension.to_string(),
            log_file_version: log_file_version.to_string(),
            file_write_token: file_write_token.to_string(),
        })
    }
}

impl LogFile {
    /// Returns the file name of the log file.
    #[inline]
    pub fn file_name(&self) -> String {
        format!(
            "{prefix}{file_id}_{base_commit_timestamp}.{log_file_extension}.{log_file_version}_{file_write_token}",
            prefix = LOG_FILE_PREFIX,
            file_id = self.file_id,
            base_commit_timestamp = self.base_commit_timestamp,
            log_file_extension = self.log_file_extension,
            log_file_version = self.log_file_version,
            file_write_token = self.file_write_token
        )
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
        assert_eq!(log_file.log_file_extension, "log");
        assert_eq!(log_file.log_file_version, "1");
        assert_eq!(log_file.file_write_token, "0-51-115");
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
}
