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
pub mod builder;
pub mod instant;
pub mod loader;
pub mod lsm_tree;
pub(crate) mod selector;
pub(crate) mod util;

use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::file_group::builder::{build_file_groups, build_replaced_file_groups, FileGroupMerger};
use crate::file_group::FileGroup;

use crate::schema::resolver::{
    resolve_avro_schema_from_commit_metadata, resolve_schema_from_commit_metadata,
};
use crate::storage::Storage;
use crate::timeline::builder::TimelineBuilder;
use crate::timeline::instant::Action;
use crate::timeline::loader::TimelineLoader;
use crate::timeline::selector::TimelineSelector;
use crate::Result;
use arrow_schema::Schema;
use instant::Instant;

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
    active_loader: TimelineLoader,
    archived_loader: Option<TimelineLoader>,
    pub completed_commits: Vec<Instant>,
}

pub const EARLIEST_START_TIMESTAMP: &str = "19700101000000000";
pub const DEFAULT_LOADING_ACTIONS: &[Action] =
    &[Action::Commit, Action::DeltaCommit, Action::ReplaceCommit];

impl Timeline {
    pub(crate) fn new(
        hudi_configs: Arc<HudiConfigs>,
        storage: Arc<Storage>,
        active_loader: TimelineLoader,
        archived_loader: Option<TimelineLoader>,
    ) -> Self {
        Self {
            hudi_configs,
            storage,
            active_loader,
            archived_loader,
            completed_commits: Vec::new(),
        }
    }

    pub(crate) async fn new_from_storage(
        hudi_configs: Arc<HudiConfigs>,
        storage_options: Arc<HashMap<String, String>>,
    ) -> Result<Self> {
        let storage = Storage::new(storage_options.clone(), hudi_configs.clone())?;
        let mut timeline = TimelineBuilder::new(hudi_configs, storage).build().await?;
        let selector = TimelineSelector::completed_actions_in_range(
            DEFAULT_LOADING_ACTIONS,
            timeline.hudi_configs.clone(),
            None,
            None,
        )?;
        timeline.completed_commits = timeline.load_instants(&selector, false).await?;
        Ok(timeline)
    }

    pub async fn load_instants(
        &self,
        selector: &TimelineSelector,
        desc: bool,
    ) -> Result<Vec<Instant>> {
        self.active_loader.load_instants(selector, desc).await
    }

    pub async fn load_instants_with_archive(
        &self,
        selector: &TimelineSelector,
        desc: bool,
        include_archived: bool,
    ) -> Result<Vec<Instant>> {
        // Always try active first
        let mut instants = self.active_loader.load_instants(selector, desc).await?;

        // Load archived/compacted if flag is set
        if include_archived {
            if let Some(archived_loader) = &self.archived_loader {
                let archived_instants = archived_loader
                    .load_archived_instants(selector, desc)
                    .await?;
                instants.extend(archived_instants);

                // Re-sort after merging
                instants.sort_unstable();
                if desc {
                    instants.reverse();
                }
            }
        }
        Ok(instants)
    }

    /// Get the completed commit [Instant]s in the timeline.
    ///
    /// * For Copy-on-write tables, this includes commit instants.
    /// * For Merge-on-read tables, this includes compaction commit instants.
    ///
    /// # Arguments
    ///
    /// * `desc` - If true, the [Instant]s are sorted in descending order.
    pub async fn get_completed_commits(&self, desc: bool) -> Result<Vec<Instant>> {
        let selector =
            TimelineSelector::completed_commits_in_range(self.hudi_configs.clone(), None, None)?;
        self.load_instants_with_archive(&selector, desc, false)
            .await
    }

    /// Get the completed deltacommit [Instant]s in the timeline.
    ///
    /// Only applicable for Merge-on-read tables. Empty vector will be returned for Copy-on-write tables.
    ///
    /// # Arguments
    ///
    /// * `desc` - If true, the [Instant]s are sorted in descending order.
    pub async fn get_completed_deltacommits(&self, desc: bool) -> Result<Vec<Instant>> {
        let selector = TimelineSelector::completed_deltacommits_in_range(
            self.hudi_configs.clone(),
            None,
            None,
        )?;
        self.load_instants_with_archive(&selector, desc, false)
            .await
    }

    /// Get the completed replacecommit [Instant]s in the timeline.
    ///
    /// # Arguments
    ///
    /// * `desc` - If true, the [Instant]s are sorted in descending order.
    pub async fn get_completed_replacecommits(&self, desc: bool) -> Result<Vec<Instant>> {
        let selector = TimelineSelector::completed_replacecommits_in_range(
            self.hudi_configs.clone(),
            None,
            None,
        )?;
        self.load_instants_with_archive(&selector, desc, false)
            .await
    }

    /// Get the completed clustering commit [Instant]s in the timeline.
    ///
    /// # Arguments
    ///
    /// * `desc` - If true, the [Instant]s are sorted in descending order.
    pub async fn get_completed_clustering_commits(&self, desc: bool) -> Result<Vec<Instant>> {
        let selector = TimelineSelector::completed_replacecommits_in_range(
            self.hudi_configs.clone(),
            None,
            None,
        )?;
        let instants = self
            .load_instants_with_archive(&selector, desc, false)
            .await?;
        let mut clustering_instants = Vec::new();
        for instant in instants {
            let metadata = self.get_instant_metadata(&instant).await?;
            let op_type = metadata
                .get("operationType")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    CoreError::CommitMetadata("Failed to get operation type".to_string())
                })?;
            if op_type == "cluster" {
                clustering_instants.push(instant);
            }
        }
        Ok(clustering_instants)
    }

    async fn get_instant_metadata(&self, instant: &Instant) -> Result<Map<String, Value>> {
        let path = instant.relative_path()?;
        let bytes = self.storage.get_file_data(path.as_str()).await?;

        serde_json::from_slice(&bytes)
            .map_err(|e| CoreError::Timeline(format!("Failed to get commit metadata: {}", e)))
    }

    /// Get the instant metadata in JSON format.
    pub async fn get_instant_metadata_in_json(&self, instant: &Instant) -> Result<String> {
        let path = instant.relative_path()?;
        let bytes = self.storage.get_file_data(path.as_str()).await?;
        String::from_utf8(bytes.to_vec())
            .map_err(|e| CoreError::Timeline(format!("Failed to get commit metadata: {}", e)))
    }

    pub(crate) async fn get_latest_commit_metadata(&self) -> Result<Map<String, Value>> {
        match self.completed_commits.iter().next_back() {
            Some(instant) => self.get_instant_metadata(instant).await,
            None => Err(CoreError::TimelineNoCommit),
        }
    }

    pub(crate) fn get_latest_commit_timestamp_as_option(&self) -> Option<&str> {
        self.completed_commits
            .iter()
            .next_back()
            .map(|instant| instant.timestamp.as_str())
    }

    /// Get the latest commit timestamp from the [Timeline].
    ///
    /// Only completed commits are considered.
    pub fn get_latest_commit_timestamp(&self) -> Result<String> {
        self.get_latest_commit_timestamp_as_option()
            .map_or_else(|| Err(CoreError::TimelineNoCommit), |t| Ok(t.to_string()))
    }

    /// Get the latest [apache_avro::schema::Schema] as [String] from the [Timeline].
    ///
    /// ### Note
    /// This API behaves differently from [crate::table::Table::get_avro_schema],
    /// which additionally looks for [HudiTableConfig::CreateSchema] in the table config.
    pub async fn get_latest_avro_schema(&self) -> Result<String> {
        let commit_metadata = self.get_latest_commit_metadata().await?;
        resolve_avro_schema_from_commit_metadata(&commit_metadata)
    }

    /// Get the latest [arrow_schema::Schema] from the [Timeline].
    ///
    /// ### Note
    /// This API behaves differently from [crate::table::Table::get_schema],
    /// which additionally looks for [HudiTableConfig::CreateSchema] in the table config.
    pub async fn get_latest_schema(&self) -> Result<Schema> {
        let commit_metadata = self.get_latest_commit_metadata().await?;
        resolve_schema_from_commit_metadata(&commit_metadata, self.storage.clone()).await
    }

    pub(crate) async fn get_replaced_file_groups_as_of(
        &self,
        timestamp: &str,
    ) -> Result<HashSet<FileGroup>> {
        let mut file_groups: HashSet<FileGroup> = HashSet::new();
        let selector = TimelineSelector::completed_replacecommits_in_range(
            self.hudi_configs.clone(),
            None,
            Some(timestamp),
        )?;
        for instant in selector.select(self)? {
            let commit_metadata = self.get_instant_metadata(&instant).await?;
            file_groups.extend(build_replaced_file_groups(&commit_metadata)?);
        }

        // TODO: return file group and instants, and handle multi-writer fg id conflicts

        Ok(file_groups)
    }

    /// Get file groups in the timeline ranging from start (exclusive) to end (inclusive).
    /// File groups are as of the [end] timestamp or the latest if not given.
    pub(crate) async fn get_file_groups_between(
        &self,
        start_timestamp: Option<&str>,
        end_timestamp: Option<&str>,
    ) -> Result<HashSet<FileGroup>> {
        let mut file_groups: HashSet<FileGroup> = HashSet::new();
        let mut replaced_file_groups: HashSet<FileGroup> = HashSet::new();
        let selector = TimelineSelector::completed_actions_in_range(
            DEFAULT_LOADING_ACTIONS,
            self.hudi_configs.clone(),
            start_timestamp,
            end_timestamp,
        )?;
        let commits = selector.select(self)?;
        for commit in commits {
            let commit_metadata = self.get_instant_metadata(&commit).await?;
            file_groups.merge(build_file_groups(&commit_metadata)?)?;

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

    use hudi_test::{assert_arrow_field_names_eq, assert_avro_field_names_eq, SampleTable};

    use crate::config::table::HudiTableConfig;
    use crate::metadata::meta_field::MetaField;
    #[tokio::test]
    async fn test_timeline_v8_nonpartitioned() {
        let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
        let timeline = create_test_timeline(base_url).await;
        assert_eq!(timeline.completed_commits.len(), 2);
        assert!(matches!(
            timeline.active_loader,
            TimelineLoader::LayoutTwoActive(_)
        ));
        assert!(matches!(
            timeline.archived_loader,
            Some(TimelineLoader::LayoutTwoCompacted(_))
        ));
    }

    async fn create_test_timeline(base_url: Url) -> Timeline {
        let storage = Storage::new(
            Arc::new(HashMap::new()),
            Arc::new(HudiConfigs::new([(
                HudiTableConfig::BasePath,
                base_url.to_string(),
            )])),
        )
        .unwrap();

        let hudi_configs = HudiConfigs::new([(HudiTableConfig::BasePath, base_url.to_string())]);
        let table_properties = crate::config::util::parse_data_for_options(
            &storage
                .get_file_data(".hoodie/hoodie.properties")
                .await
                .unwrap(),
            "=",
        )
        .unwrap();
        let mut hudi_configs_map = hudi_configs.as_options();
        hudi_configs_map.extend(table_properties);
        let hudi_configs = Arc::new(HudiConfigs::new(hudi_configs_map));

        let mut timeline = TimelineBuilder::new(hudi_configs, storage)
            .build()
            .await
            .unwrap();

        let selector = TimelineSelector::completed_actions_in_range(
            DEFAULT_LOADING_ACTIONS,
            timeline.hudi_configs.clone(),
            None,
            None,
        )
        .unwrap();
        timeline.completed_commits = timeline.load_instants(&selector, false).await.unwrap();
        timeline
    }

    #[tokio::test]
    async fn timeline_read_latest_schema() {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let timeline = create_test_timeline(base_url).await;
        let table_schema = timeline.get_latest_schema().await.unwrap();
        assert_eq!(table_schema.fields.len(), 21)
    }

    #[tokio::test]
    async fn timeline_read_latest_schema_from_empty_table() {
        let base_url = SampleTable::V6Empty.url_to_cow();
        let timeline = create_test_timeline(base_url).await;
        let table_schema = timeline.get_latest_schema().await;
        assert!(table_schema.is_err());
        assert!(matches!(
            table_schema.unwrap_err(),
            CoreError::TimelineNoCommit
        ))
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
        let result = timeline.get_instant_metadata(&instant).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CoreError::Timeline(_)));
        assert!(err.to_string().contains("Failed to get commit metadata"));

        let instant = Instant::from_str("20240402144910683.commit").unwrap();

        // Test error when reading a commit metadata file with invalid JSON
        let result = timeline.get_instant_metadata(&instant).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CoreError::Timeline(_)));
        assert!(err.to_string().contains("Failed to get commit metadata"));
    }

    #[tokio::test]
    async fn timeline_get_schema_returns_error_for_no_schema_and_write_stats() {
        let base_url = Url::from_file_path(
            canonicalize(Path::new(
                "tests/data/timeline/commits_with_no_schema_and_write_stats",
            ))
            .unwrap(),
        )
        .unwrap();
        let timeline = create_test_timeline(base_url).await;

        // Check Arrow schema
        let arrow_schema = timeline.get_latest_schema().await;
        assert!(arrow_schema.is_err());
        assert!(matches!(arrow_schema.unwrap_err(), CoreError::CommitMetadata(_)), "Getting Arrow schema includes base file lookup, therefore expect CommitMetadata error when write stats are missing");

        // Check Avro schema
        let avro_schema = timeline.get_latest_avro_schema().await;
        assert!(avro_schema.is_err());
        assert!(matches!(avro_schema.unwrap_err(), CoreError::SchemaNotFound(_)), "Getting Avro schema does not include base file lookup, therefore expect SchemaNotFound error when `extraMetadata.schema` is missing");
    }

    #[tokio::test]
    async fn timeline_get_schema_from_commit_metadata() {
        let base_url = Url::from_file_path(
            canonicalize(Path::new(
                "tests/data/timeline/commits_with_valid_schema_in_commit_metadata",
            ))
            .unwrap(),
        )
        .unwrap();
        let timeline = create_test_timeline(base_url).await;

        // Check Arrow schema
        let arrow_schema = timeline.get_latest_schema().await;
        assert!(arrow_schema.is_ok());
        let arrow_schema = arrow_schema.unwrap();
        assert_arrow_field_names_eq!(
            arrow_schema,
            [
                MetaField::field_names(),
                vec!["ts", "uuid", "rider", "driver", "fare", "city"]
            ]
            .concat()
        );

        // Check Avro schema
        let avro_schema = timeline.get_latest_avro_schema().await;
        assert!(avro_schema.is_ok());
        let avro_schema = avro_schema.unwrap();
        assert_avro_field_names_eq!(
            &avro_schema,
            ["ts", "uuid", "rider", "driver", "fare", "city"]
        );
    }

    #[tokio::test]
    async fn timeline_get_schema_from_empty_commit_metadata() {
        let base_url = Url::from_file_path(
            canonicalize(Path::new(
                "tests/data/timeline/commits_with_empty_commit_metadata",
            ))
            .unwrap(),
        )
        .unwrap();
        let timeline = create_test_timeline(base_url).await;

        // Check Arrow schema
        let result = timeline.get_latest_schema().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::CommitMetadata(_)));

        // Check Avro schema
        let result = timeline.get_latest_avro_schema().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::CommitMetadata(_)));
    }

    #[tokio::test]
    async fn timeline_get_schema_from_base_file() {
        let timeline_base_urls = [
            "tests/data/timeline/commits_load_schema_from_base_file_cow",
            "tests/data/timeline/commits_load_schema_from_base_file_mor",
        ];
        for base_url in timeline_base_urls {
            let base_url = Url::from_file_path(canonicalize(Path::new(base_url)).unwrap()).unwrap();
            let timeline = create_test_timeline(base_url).await;

            let arrow_schema = timeline.get_latest_schema().await;
            assert!(arrow_schema.is_ok());
            let arrow_schema = arrow_schema.unwrap();
            assert_arrow_field_names_eq!(
                arrow_schema,
                [
                    MetaField::field_names(),
                    vec!["ts", "uuid", "rider", "driver", "fare", "city"]
                ]
                .concat()
            );
        }
    }
}
