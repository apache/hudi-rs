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
use crate::file_group::base_file::BaseFile;
use crate::file_group::log_file::LogFile;
use crate::storage::Storage;
use crate::Result;
use std::collections::BTreeSet;
use std::path::PathBuf;

/// Within a [crate::file_group::FileGroup],
/// a [FileSlice] is a logical group of [BaseFile] and [LogFile]s.
#[derive(Clone, Debug)]
pub struct FileSlice {
    pub base_file: BaseFile,
    pub log_files: BTreeSet<LogFile>,
    pub partition_path: String,
}

impl FileSlice {
    pub fn new(base_file: BaseFile, partition_path: String) -> Self {
        Self {
            base_file,
            log_files: BTreeSet::new(),
            partition_path,
        }
    }

    #[inline]
    pub fn has_log_file(&self) -> bool {
        !self.log_files.is_empty()
    }

    fn relative_path_for_file(&self, file_name: &str) -> Result<String> {
        let path = PathBuf::from(self.partition_path.as_str()).join(file_name);
        path.to_str().map(|s| s.to_string()).ok_or_else(|| {
            CoreError::FileGroup(format!("Failed to get relative path for file: {file_name}",))
        })
    }

    /// Returns the relative path of the [BaseFile] in the [FileSlice].
    pub fn base_file_relative_path(&self) -> Result<String> {
        let file_name = &self.base_file.file_name();
        self.relative_path_for_file(file_name)
    }

    /// Returns the relative path of the given [LogFile] in the [FileSlice].
    pub fn log_file_relative_path(&self, log_file: &LogFile) -> Result<String> {
        let file_name = &log_file.file_name();
        self.relative_path_for_file(file_name)
    }

    /// Returns the enclosing [FileGroup]'s id.
    #[inline]
    pub fn file_id(&self) -> &str {
        &self.base_file.file_id
    }

    /// Returns the instant time that marks the [FileSlice] creation.
    ///
    /// This is also an instant time stored in the [Timeline].
    #[inline]
    pub fn creation_instant_time(&self) -> &str {
        &self.base_file.commit_timestamp
    }

    /// Load [FileMetadata] from storage layer for the [BaseFile] if `file_metadata` is [None]
    /// or if `file_metadata` is not fully populated.
    pub async fn load_metadata_if_needed(&mut self, storage: &Storage) -> Result<()> {
        if let Some(metadata) = &self.base_file.file_metadata {
            if metadata.fully_populated {
                return Ok(());
            }
        }
        let relative_path = self.base_file_relative_path()?;
        let fetched_metadata = storage.get_file_metadata(&relative_path).await?;
        self.base_file.file_metadata = Some(fetched_metadata);
        Ok(())
    }
}
