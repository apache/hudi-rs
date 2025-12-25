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
use crate::timeline::completion_time::TimelineViewByCompletionTime;
use crate::Result;
use std::fmt::Display;
use std::str::FromStr;

/// Hudi Base file, part of a [FileSlice].
#[derive(Clone, Debug)]
pub struct BaseFile {
    /// The id of the enclosing [FileGroup].
    pub file_id: String,

    /// Monotonically increasing token for every attempt to write the [BaseFile].
    pub write_token: String,

    /// The timestamp of the commit instant in the Timeline that created the [BaseFile].
    ///
    /// For v6 tables: This is the commit timestamp (v6 does not track completion time separately).
    /// For v8+ tables: This is the request instant timestamp.
    pub commit_timestamp: String,

    /// The completion timestamp of the commit that created this [BaseFile].
    ///
    /// For v6 tables: This is always `None` (v6 does not track completion timestamps).
    /// For v8+ tables: This is set from the timeline when the commit is completed.
    ///   If `None`, the commit is still pending and the file should not be included in queries.
    pub completion_timestamp: Option<String>,

    /// File extension that matches to [crate::config::table::HudiTableConfig::BaseFileFormat].
    ///
    /// See also [crate::config::table::BaseFileFormatValue].
    pub extension: String,

    /// The metadata about the file.
    pub file_metadata: Option<FileMetadata>,
}

impl BaseFile {
    /// Parse a base file's name into parts.
    ///
    /// File name format:
    ///
    /// ```text
    /// [File Id]_[File Write Token]_[Commit timestamp].[File Extension]
    /// ```
    fn parse_file_name(file_name: &str) -> Result<(String, String, String, String)> {
        let err_msg = format!("Failed to parse file name '{file_name}' for base file.");
        let (stem, extension) = file_name
            .rsplit_once('.')
            .ok_or_else(|| CoreError::FileGroup(err_msg.clone()))?;
        let parts: Vec<&str> = stem.split('_').collect();
        let file_id = parts
            .first()
            .ok_or_else(|| CoreError::FileGroup(err_msg.clone()))?
            .to_string();
        let write_token = parts
            .get(1)
            .ok_or_else(|| CoreError::FileGroup(err_msg.clone()))?
            .to_string();
        let commit_timestamp = parts
            .get(2)
            .ok_or_else(|| CoreError::FileGroup(err_msg.clone()))?
            .to_string();
        Ok((
            file_id,
            write_token,
            commit_timestamp,
            extension.to_string(),
        ))
    }

    #[inline]
    pub fn file_name(&self) -> String {
        format!(
            "{file_id}_{write_token}_{commit_timestamp}.{extension}",
            file_id = self.file_id,
            write_token = self.write_token,
            commit_timestamp = self.commit_timestamp,
            extension = self.extension,
        )
    }

    /// Set the completion timestamp from a completion time view.
    ///
    /// Looks up the completion timestamp using this file's `commit_timestamp`
    /// (request time) and sets `completion_timestamp` if found.
    ///
    /// For v6 tables, the view returns `None` and this is a no-op.
    /// For v8+ tables, this sets the completion timestamp for completed commits.
    pub fn set_completion_time<V: TimelineViewByCompletionTime>(&mut self, view: &V) {
        self.completion_timestamp = view
            .get_completion_time(&self.commit_timestamp)
            .map(|s| s.to_string());
    }
}

impl Display for BaseFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BaseFile: {}", self.file_name())
    }
}

impl PartialEq for BaseFile {
    fn eq(&self, other: &Self) -> bool {
        self.file_name() == other.file_name()
    }
}

impl Eq for BaseFile {}

impl FromStr for BaseFile {
    type Err = CoreError;

    fn from_str(file_name: &str) -> Result<Self, Self::Err> {
        let (file_id, write_token, commit_timestamp, extension) = Self::parse_file_name(file_name)?;
        Ok(Self {
            file_id,
            write_token,
            commit_timestamp,
            completion_timestamp: None,
            extension,
            file_metadata: None,
        })
    }
}

impl TryFrom<FileMetadata> for BaseFile {
    type Error = CoreError;

    fn try_from(metadata: FileMetadata) -> Result<Self> {
        let file_name = metadata.name.as_str();
        let (file_id, write_token, commit_timestamp, extension) = Self::parse_file_name(file_name)?;
        Ok(Self {
            file_id,
            write_token,
            commit_timestamp,
            completion_timestamp: None,
            extension,
            file_metadata: Some(metadata),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_base_file_from_file_name() {
        let file_name = "5a226868-2934-4f84-a16f-55124630c68d-0_0-7-24_20240402144910683.parquet";
        let base_file = BaseFile::from_str(file_name).unwrap();
        assert_eq!(base_file.file_id, "5a226868-2934-4f84-a16f-55124630c68d-0");
        assert_eq!(base_file.commit_timestamp, "20240402144910683");
        assert!(base_file.file_metadata.is_none());
    }

    #[test]
    fn test_create_base_file_from_metadata() {
        let metadata = FileMetadata::new(
            "5a226868-2934-4f84-a16f-55124630c68d-0_0-7-24_20240402144910683.parquet",
            1024,
        );
        let base_file = BaseFile::try_from(metadata).unwrap();
        assert_eq!(base_file.file_id, "5a226868-2934-4f84-a16f-55124630c68d-0");
        assert_eq!(base_file.commit_timestamp, "20240402144910683");
        let file_metadata = base_file.file_metadata.unwrap();
        assert_eq!(file_metadata.size, 1024);
        assert!(!file_metadata.fully_populated);
    }

    #[test]
    fn create_a_base_file_returns_error() {
        let result = BaseFile::from_str("no_file_extension");
        assert!(matches!(result.unwrap_err(), CoreError::FileGroup(_)));

        let result = BaseFile::from_str(".parquet");
        assert!(matches!(result.unwrap_err(), CoreError::FileGroup(_)));

        let metadata = FileMetadata::new("no-valid-delimiter.parquet", 1024);
        let result = BaseFile::try_from(metadata);
        assert!(matches!(result.unwrap_err(), CoreError::FileGroup(_)));
    }
}
