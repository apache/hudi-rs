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
mod instant;
mod selector;

use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::file_group::builder::{build_file_groups, build_replaced_file_groups};
use crate::file_group::FileGroup;
use crate::storage::Storage;
use crate::timeline::selector::TimelineSelector;
use crate::Result;
use arrow_schema::Schema;
use instant::Instant;
use log::debug;
use parquet::arrow::parquet_to_arrow_schema;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

/// A [Timeline] contains transaction logs of all actions performed on the table at different [Instant]s of time.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct Timeline {
    hudi_configs: Arc<HudiConfigs>,
    pub(crate) storage: Arc<Storage>,
    pub completed_commits: Vec<Instant>,
}

impl Timeline {
    #[cfg(test)]
    pub async fn new_from_completed_commits(
        hudi_configs: Arc<HudiConfigs>,
        storage_options: Arc<HashMap<String, String>>,
        completed_commits: Vec<Instant>,
    ) -> Result<Self> {
        let storage = Storage::new(storage_options.clone(), hudi_configs.clone())?;
        Ok(Self {
            storage,
            hudi_configs,
            completed_commits,
        })
    }

    pub async fn new_from_storage(
        hudi_configs: Arc<HudiConfigs>,
        storage_options: Arc<HashMap<String, String>>,
    ) -> Result<Self> {
        let storage = Storage::new(storage_options.clone(), hudi_configs.clone())?;
        let selector = TimelineSelector::completed_commits(hudi_configs.clone())?;
        let completed_commits = Self::load_instants(&selector, &storage).await?;
        Ok(Self {
            storage,
            hudi_configs,
            completed_commits,
        })
    }

    async fn load_instants(selector: &TimelineSelector, storage: &Storage) -> Result<Vec<Instant>> {
        let files = storage.list_files(Some(".hoodie")).await?;

        // For most cases, we load completed instants, so we can pre-allocate the vector with a
        // capacity of 1/3 of the total number of listed files,
        // ignoring requested and inflight instants.
        let mut instants = Vec::with_capacity(files.len() / 3);

        for file_info in files {
            match selector.try_create_instant(file_info.name.as_str()) {
                Ok(instant) => instants.push(instant),
                Err(e) => {
                    // Ignore files that are not valid or desired instants.
                    debug!(
                        "Instant not created from file {:?} due to: {:?}",
                        file_info, e
                    );
                }
            }
        }

        instants.sort_unstable();

        // As of current impl., we don't mutate instants once timeline is created,
        // so we can save some memory by shrinking the capacity.
        instants.shrink_to_fit();

        Ok(instants)
    }

    pub fn get_latest_commit_timestamp(&self) -> Option<&str> {
        self.completed_commits
            .iter()
            .next_back()
            .map(|instant| instant.timestamp.as_str())
    }

    async fn get_latest_commit_metadata(&self) -> Result<Map<String, Value>> {
        match self.completed_commits.iter().next_back() {
            Some(instant) => self.get_commit_metadata(instant).await,
            None => Ok(Map::new()),
        }
    }

    async fn get_commit_metadata(&self, instant: &Instant) -> Result<Map<String, Value>> {
        let path = instant.relative_path()?;
        let bytes = self.storage.get_file_data(path.as_str()).await?;

        serde_json::from_slice(&bytes)
            .map_err(|e| CoreError::Timeline(format!("Failed to get commit metadata: {}", e)))
    }

    pub async fn get_latest_schema(&self) -> Result<Schema> {
        let commit_metadata = self.get_latest_commit_metadata().await?;

        let parquet_path = commit_metadata
            .get("partitionToWriteStats")
            .and_then(|v| v.as_object())
            .and_then(|obj| obj.values().next())
            .and_then(|value| value.as_array())
            .and_then(|arr| arr.first())
            .and_then(|first_value| first_value["path"].as_str());

        if let Some(path) = parquet_path {
            let parquet_meta = self.storage.get_parquet_file_metadata(path).await?;

            Ok(parquet_to_arrow_schema(
                parquet_meta.file_metadata().schema_descr(),
                None,
            )?)
        } else {
            Err(CoreError::Timeline(
                "Failed to resolve the latest schema: no file path found".to_string(),
            ))
        }
    }

    pub async fn get_replaced_file_groups(&self) -> Result<HashSet<FileGroup>> {
        let mut file_groups: HashSet<FileGroup> = HashSet::new();
        let selector = TimelineSelector::completed_replacecommits(self.hudi_configs.clone());
        for instant in selector.select(self)? {
            let commit_metadata = self.get_commit_metadata(&instant).await?;
            file_groups.extend(build_replaced_file_groups(&commit_metadata)?);
        }

        // TODO: return file group and instants, and handle multi-writer fg id conflicts

        Ok(file_groups)
    }

    /// Get file groups in the timeline ranging from start (exclusive) to end (inclusive).
    /// File groups are as of the [end] timestamp or the latest if not given.
    pub async fn get_incremental_file_groups(
        &self,
        start_timestamp: Option<&str>,
        end_timestamp: Option<&str>,
    ) -> Result<HashSet<FileGroup>> {
        let mut file_groups: HashSet<FileGroup> = HashSet::new();
        let mut replaced_file_groups: HashSet<FileGroup> = HashSet::new();
        let selector = TimelineSelector::completed_commits_in_range(
            self.hudi_configs.clone(),
            start_timestamp,
            end_timestamp,
        )?;
        let commits = selector.select(self)?;
        for commit in commits {
            let commit_metadata = self.get_commit_metadata(&commit).await?;
            file_groups.extend(build_file_groups(&commit_metadata)?);

            if commit.is_replacecommit() {
                replaced_file_groups.extend(build_replaced_file_groups(&commit_metadata)?);
            }
        }

        Ok(file_groups
            .difference(&replaced_file_groups)
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs::canonicalize;
    use std::path::Path;
    use std::str::FromStr;
    use std::sync::Arc;

    use url::Url;

    use hudi_tests::TestTable;

    use crate::config::table::HudiTableConfig;

    async fn create_test_timeline(base_url: Url) -> Timeline {
        Timeline::new_from_storage(
            Arc::new(HudiConfigs::new([(HudiTableConfig::BasePath, base_url)])),
            Arc::new(HashMap::new()),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn timeline_read_latest_schema() {
        let base_url = TestTable::V6Nonpartitioned.url();
        let timeline = create_test_timeline(base_url).await;
        let table_schema = timeline.get_latest_schema().await.unwrap();
        assert_eq!(table_schema.fields.len(), 21)
    }

    #[tokio::test]
    async fn timeline_read_latest_schema_from_empty_table() {
        let base_url = TestTable::V6Empty.url();
        let timeline = create_test_timeline(base_url).await;
        let table_schema = timeline.get_latest_schema().await;
        assert!(table_schema.is_err());
        assert_eq!(
            table_schema.err().unwrap().to_string(),
            "Timeline error: Failed to resolve the latest schema: no file path found"
        )
    }

    #[tokio::test]
    async fn init_commits_timeline() {
        let base_url = Url::from_file_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let timeline = create_test_timeline(base_url).await;
        assert_eq!(
            timeline.completed_commits,
            vec![
                Instant::from_str("20240402123035233.commit").unwrap(),
                Instant::from_str("20240402144910683.commit").unwrap(),
            ]
        )
    }

    #[tokio::test]
    async fn get_commit_metadata_returns_error() {
        let base_url = Url::from_file_path(
            canonicalize(Path::new(
                "tests/data/timeline/commits_with_invalid_content",
            ))
            .unwrap(),
        )
        .unwrap();
        let timeline = create_test_timeline(base_url).await;
        let instant = Instant::from_str("20240402123035233.commit").unwrap();

        // Test error when reading empty commit metadata file
        let result = timeline.get_commit_metadata(&instant).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CoreError::Timeline(_)));
        assert!(err.to_string().contains("Failed to get commit metadata"));

        let instant = Instant::from_str("20240402144910683.commit").unwrap();

        // Test error when reading a commit metadata file with invalid JSON
        let result = timeline.get_commit_metadata(&instant).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CoreError::Timeline(_)));
        assert!(err.to_string().contains("Failed to get commit metadata"));
    }
}
