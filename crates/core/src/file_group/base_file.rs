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

/// Hudi Base file, part of a [FileSlice].
#[derive(Clone, Debug)]
pub struct BaseFile {
    /// The file name of the base file.
    pub file_name: String,

    /// The id of the enclosing file group.
    pub file_group_id: String,

    /// The associated instant time of the base file.
    pub instant_time: String,

    /// The metadata about the file.
    pub file_metadata: Option<FileMetadata>,
}

impl BaseFile {
    /// Parse file name and extract `file_group_id` and `instant_time`.
    fn parse_file_name(file_name: &str) -> Result<(String, String)> {
        let err_msg = format!("Failed to parse file name '{file_name}' for base file.");
        let (name, _) = file_name
            .rsplit_once('.')
            .ok_or_else(|| CoreError::FileGroup(err_msg.clone()))?;
        let parts: Vec<&str> = name.split('_').collect();
        let file_group_id = parts
            .first()
            .ok_or_else(|| CoreError::FileGroup(err_msg.clone()))?
            .to_string();
        let instant_time = parts
            .get(2)
            .ok_or_else(|| CoreError::FileGroup(err_msg.clone()))?
            .to_string();
        Ok((file_group_id, instant_time))
    }
}

impl TryFrom<&str> for BaseFile {
    type Error = CoreError;

    fn try_from(file_name: &str) -> Result<Self> {
        let (file_group_id, instant_time) = Self::parse_file_name(file_name)?;
        Ok(Self {
            file_name: file_name.to_string(),
            file_group_id,
            instant_time,
            file_metadata: None,
        })
    }
}

impl TryFrom<FileMetadata> for BaseFile {
    type Error = CoreError;

    fn try_from(metadata: FileMetadata) -> Result<Self> {
        let file_name = metadata.name.clone();
        let (file_group_id, instant_time) = Self::parse_file_name(&file_name)?;
        Ok(Self {
            file_name,
            file_group_id,
            instant_time,
            file_metadata: Some(metadata),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hudi_tests::assert_not;

    #[test]
    fn test_create_base_file_from_file_name() {
        let file_name = "5a226868-2934-4f84-a16f-55124630c68d-0_0-7-24_20240402144910683.parquet";
        let base_file = BaseFile::try_from(file_name).unwrap();
        assert_eq!(
            base_file.file_group_id,
            "5a226868-2934-4f84-a16f-55124630c68d-0"
        );
        assert_eq!(base_file.instant_time, "20240402144910683");
        assert!(base_file.file_metadata.is_none());
    }

    #[test]
    fn test_create_base_file_from_metadata() {
        let metadata = FileMetadata::new(
            "5a226868-2934-4f84-a16f-55124630c68d-0_0-7-24_20240402144910683.parquet",
            1024,
        );
        let base_file = BaseFile::try_from(metadata).unwrap();
        assert_eq!(
            base_file.file_group_id,
            "5a226868-2934-4f84-a16f-55124630c68d-0"
        );
        assert_eq!(base_file.instant_time, "20240402144910683");
        let file_metadata = base_file.file_metadata.unwrap();
        assert_eq!(file_metadata.size, 1024);
        assert_not!(file_metadata.fully_populated);
    }

    #[test]
    fn create_a_base_file_returns_error() {
        let result = BaseFile::try_from("no_file_extension");
        assert!(matches!(result.unwrap_err(), CoreError::FileGroup(_)));

        let result = BaseFile::try_from(".parquet");
        assert!(matches!(result.unwrap_err(), CoreError::FileGroup(_)));

        let metadata = FileMetadata::new("no-valid-delimiter.parquet", 1024);
        let result = BaseFile::try_from(metadata);
        assert!(matches!(result.unwrap_err(), CoreError::FileGroup(_)));
    }
}
