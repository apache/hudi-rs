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
pub mod completion_time;
pub mod instant;
pub mod loader;
pub mod lsm_tree;
pub(crate) mod selector;
pub(crate) mod util;

use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::file_group::builder::{
    file_groups_from_commit_metadata, replaced_file_groups_from_replace_commit, FileGroupMerger,
};
use crate::file_group::FileGroup;
use crate::schema::resolver::{
    resolve_avro_schema_from_commit_metadata, resolve_schema_from_commit_metadata,
};
use crate::storage::Storage;
use crate::timeline::builder::TimelineBuilder;
use crate::timeline::completion_time::CompletionTimeView;
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

    /// Load instants from the timeline based on the selector criteria.
    ///
    /// # Archived Timeline Loading
    ///
    /// Archived instants are loaded only when BOTH conditions are met:
    /// 1. The selector has a time filter (start or end timestamp)
    /// 2. `TimelineArchivedReadEnabled` config is set to `true`
    ///
    /// This double-gate design ensures:
    /// - Queries without time filters only read active timeline (optimization)
    /// - Historical time-range queries can include archived data when explicitly enabled
    ///
    /// # Arguments
    ///
    /// * `selector` - The criteria for selecting instants (actions, states, time range)
    /// * `desc` - If true, return instants in descending order by timestamp
    pub async fn load_instants(
        &self,
        selector: &TimelineSelector,
        desc: bool,
    ) -> Result<Vec<Instant>> {
        // If a time filter is present and we have an archived loader, include archived as well.
        if selector.has_time_filter() {
            let mut instants = self.active_loader.load_instants(selector, desc).await?;
            if let Some(archived_loader) = &self.archived_loader {
                let mut archived = archived_loader
                    .load_archived_instants(selector, desc)
                    .await?;
                if !archived.is_empty() {
                    // Both sides already sorted by loaders; append is fine for now.
                    instants.append(&mut archived);
                }
            }
            Ok(instants)
        } else {
            self.active_loader.load_instants(selector, desc).await
        }
    }

    async fn load_instants_internal(
        &self,
        selector: &TimelineSelector,
        desc: bool,
    ) -> Result<Vec<Instant>> {
        // For now, just load active. Archived support will be added internally later
        // based on selector ranges.
        self.active_loader.load_instants(selector, desc).await
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
        self.load_instants_internal(&selector, desc).await
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
        self.load_instants_internal(&selector, desc).await
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
        self.load_instants_internal(&selector, desc).await
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
        let instants = self.load_instants_internal(&selector, desc).await?;
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
        self.active_loader.load_instant_metadata(instant).await
    }

    /// Get the instant metadata in JSON format.
    pub async fn get_instant_metadata_in_json(&self, instant: &Instant) -> Result<String> {
        self.active_loader
            .load_instant_metadata_as_json(instant)
            .await
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

    /// Create a [CompletionTimeView] from the completed commits.
    ///
    /// This view maps request timestamps to completion timestamps, enabling
    /// correct file association for v8+ tables where request and completion
    /// timestamps differ.
    ///
    /// For v6 tables, the view will be empty since completed_timestamp is None
    /// for all instants, and the caller should use the request timestamp directly.
    pub fn create_completion_time_view(&self) -> CompletionTimeView {
        CompletionTimeView::from_instants(&self.completed_commits)
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
            file_groups.extend(replaced_file_groups_from_replace_commit(&commit_metadata)?);
        }

        // TODO: return file group and instants, and handle multi-writer fg id conflicts

        Ok(file_groups)
    }

    /// Get file groups in the timeline ranging from start (exclusive) to end (inclusive).
    /// File groups are as of the [end] timestamp or the latest if not given.
    ///
    /// For v8+ tables, the completion timestamps from the timeline instants are used
    /// to set the completion_timestamp on base files and log files, enabling correct
    /// file slice association based on completion order rather than request order.
    ///
    /// For v6 tables, completion_timestamp is None (v6 does not track completion times).
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

        // Build completion time view from all commits for v8+ tables.
        // Each file (base or log) may have a different completion timestamp,
        // looked up from its own request timestamp.
        let completion_time_view = CompletionTimeView::from_instants(&commits);

        for commit in commits {
            let commit_metadata = self.get_instant_metadata(&commit).await?;
            file_groups.merge(file_groups_from_commit_metadata(
                &commit_metadata,
                &completion_time_view,
            )?)?;

            if commit.is_replacecommit() {
                replaced_file_groups
                    .extend(replaced_file_groups_from_replace_commit(&commit_metadata)?);
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
    use crate::timeline::instant::{Action, State};
    #[tokio::test]
    async fn test_timeline_v8_nonpartitioned() {
        let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
        let timeline = create_test_timeline(base_url).await;
        assert_eq!(timeline.completed_commits.len(), 2);
        assert!(timeline.active_loader.is_layout_two_active());
        // Archived loader should be None when TimelineArchivedReadEnabled is false (default)
        assert!(timeline.archived_loader.is_none());
    }

    #[tokio::test]
    async fn test_timeline_v8_with_archived_enabled() {
        use crate::config::internal::HudiInternalConfig::TimelineArchivedReadEnabled;

        let base_url = SampleTable::V8Nonpartitioned.url_to_cow();

        // Build initial configs with base path and archived read enabled
        let mut options_map = HashMap::new();
        options_map.insert(
            HudiTableConfig::BasePath.as_ref().to_string(),
            base_url.to_string(),
        );
        options_map.insert(
            TimelineArchivedReadEnabled.as_ref().to_string(),
            "true".to_string(),
        );

        let storage = Storage::new(
            Arc::new(HashMap::new()),
            Arc::new(HudiConfigs::new(options_map.clone())),
        )
        .unwrap();

        let table_properties = crate::config::util::parse_data_for_options(
            &storage
                .get_file_data(".hoodie/hoodie.properties")
                .await
                .unwrap(),
            "=",
        )
        .unwrap();
        options_map.extend(table_properties);
        let hudi_configs = Arc::new(HudiConfigs::new(options_map));

        let timeline = TimelineBuilder::new(hudi_configs, storage)
            .build()
            .await
            .unwrap();

        // When TimelineArchivedReadEnabled is true, archived loader should be created
        assert!(timeline.active_loader.is_layout_two_active());
        assert!(timeline
            .archived_loader
            .as_ref()
            .map(|l| l.is_layout_two_archived())
            .unwrap_or(false));
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
        // Error message changed to be more specific about JSON parsing
        assert!(
            err.to_string()
                .contains("Failed to parse JSON commit metadata")
                || err.to_string().contains("EOF while parsing")
        );

        let instant = Instant::from_str("20240402144910683.commit").unwrap();

        // Test error when reading a commit metadata file with invalid JSON
        let result = timeline.get_instant_metadata(&instant).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CoreError::Timeline(_)));
        // Error message changed to be more specific about JSON parsing
        assert!(
            err.to_string()
                .contains("Failed to parse JSON commit metadata")
                || err.to_string().contains("expected value")
        );
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

    #[tokio::test]
    async fn test_get_completed_commits() {
        let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
        let timeline = create_test_timeline(base_url).await;

        let commits = timeline.get_completed_commits(false).await.unwrap();
        assert!(!commits.is_empty());
        // All should be commits in completed state
        for instant in &commits {
            assert_eq!(instant.action, Action::Commit);
            assert_eq!(instant.state, State::Completed);
        }
    }

    #[tokio::test]
    async fn test_get_completed_deltacommits() {
        let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
        let timeline = create_test_timeline(base_url).await;

        let deltacommits = timeline.get_completed_deltacommits(false).await.unwrap();
        // All should be deltacommits (or empty if none exist)
        for instant in &deltacommits {
            assert_eq!(instant.action, Action::DeltaCommit);
            assert_eq!(instant.state, State::Completed);
        }
    }

    #[tokio::test]
    async fn test_get_completed_replacecommits() {
        let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
        let timeline = create_test_timeline(base_url).await;

        let replacecommits = timeline.get_completed_replacecommits(false).await.unwrap();
        // All should be replacecommits (or empty if none exist)
        for instant in &replacecommits {
            assert!(instant.action.is_replacecommit());
            assert_eq!(instant.state, State::Completed);
        }
    }

    #[tokio::test]
    async fn test_get_commits_descending_order() {
        let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
        let timeline = create_test_timeline(base_url).await;

        let commits_asc = timeline.get_completed_commits(false).await.unwrap();
        let commits_desc = timeline.get_completed_commits(true).await.unwrap();

        assert_eq!(commits_asc.len(), commits_desc.len());
        if !commits_asc.is_empty() {
            // Verify descending order is reverse of ascending
            assert_eq!(commits_asc.first(), commits_desc.last());
            assert_eq!(commits_asc.last(), commits_desc.first());
        }
    }

    #[tokio::test]
    async fn test_get_instant_metadata_in_json() {
        let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
        let timeline = create_test_timeline(base_url).await;

        let commits = timeline.get_completed_commits(false).await.unwrap();
        if let Some(instant) = commits.first() {
            let json = timeline
                .get_instant_metadata_in_json(instant)
                .await
                .unwrap();
            // Should be valid JSON
            assert!(serde_json::from_str::<serde_json::Value>(&json).is_ok());
        }
    }

    #[tokio::test]
    async fn test_get_latest_commit_timestamp() {
        let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
        let timeline = create_test_timeline(base_url).await;

        let timestamp = timeline.get_latest_commit_timestamp().unwrap();
        assert!(!timestamp.is_empty());
        // Should be in timeline timestamp format
        assert!(timestamp.len() >= 14);
    }

    #[tokio::test]
    async fn test_get_latest_commit_timestamp_as_option() {
        let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
        let timeline = create_test_timeline(base_url).await;

        let timestamp = timeline.get_latest_commit_timestamp_as_option();
        assert!(timestamp.is_some());
        assert!(!timestamp.unwrap().is_empty());
    }
}
