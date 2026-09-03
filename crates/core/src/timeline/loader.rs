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
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig::{ArchiveLogFolder, TimelineHistoryPath, TimelinePath};
use crate::error::CoreError;
use crate::metadata::HUDI_METADATA_DIR;
use crate::metadata::commit::HoodieCommitMetadata;
use crate::storage::Storage;
use crate::timeline::instant::Instant;
use crate::timeline::selector::TimelineSelector;
use log::debug;
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TimelineLoader {
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
    layout: TimelineLayout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimelineLayout {
    V1Active,
    V1Archived,
    V2Active,
    V2Archived,
}

#[allow(dead_code)]
impl TimelineLoader {
    /// Create a new Layout One Active loader
    pub fn new_layout_one_active(hudi_configs: Arc<HudiConfigs>, storage: Arc<Storage>) -> Self {
        Self {
            hudi_configs,
            storage,
            layout: TimelineLayout::V1Active,
        }
    }

    /// Create a new Layout One Archived loader
    pub fn new_layout_one_archived(hudi_configs: Arc<HudiConfigs>, storage: Arc<Storage>) -> Self {
        Self {
            hudi_configs,
            storage,
            layout: TimelineLayout::V1Archived,
        }
    }

    /// Create a new Layout Two Active loader
    pub fn new_layout_two_active(hudi_configs: Arc<HudiConfigs>, storage: Arc<Storage>) -> Self {
        Self {
            hudi_configs,
            storage,
            layout: TimelineLayout::V2Active,
        }
    }

    /// Create a new Layout Two Archived loader
    pub fn new_layout_two_archived(hudi_configs: Arc<HudiConfigs>, storage: Arc<Storage>) -> Self {
        Self {
            hudi_configs,
            storage,
            layout: TimelineLayout::V2Archived,
        }
    }

    /// Returns the storage for this loader.
    fn storage(&self) -> &Arc<Storage> {
        &self.storage
    }

    /// Check if this is a Layout Two Active loader (for testing/assertions)
    #[cfg(test)]
    pub(crate) fn is_layout_two_active(&self) -> bool {
        matches!(self.layout, TimelineLayout::V2Active)
    }

    /// Check if this is a Layout Two Archived loader (for testing/assertions)
    #[cfg(test)]
    pub(crate) fn is_layout_two_archived(&self) -> bool {
        matches!(self.layout, TimelineLayout::V2Archived)
    }

    /// Returns the directory for active timeline instants.
    ///
    /// - Layout One (v6-v8): `.hoodie/`
    /// - Layout Two (v8+): `.hoodie/{timeline_path}` (configurable via `hoodie.timeline.path`, default: `timeline`)
    fn get_active_timeline_dir(&self) -> String {
        match self.layout {
            TimelineLayout::V1Active | TimelineLayout::V1Archived => HUDI_METADATA_DIR.to_string(),
            TimelineLayout::V2Active | TimelineLayout::V2Archived => {
                let timeline_path: String = self.hudi_configs.get_or_default(TimelinePath).into();
                format!("{HUDI_METADATA_DIR}/{timeline_path}")
            }
        }
    }

    /// Returns the directory for archived timeline instants.
    ///
    /// - Layout One (v6-v8): configurable via `hoodie.archivelog.folder` (default: `.hoodie/archived`)
    /// - Layout Two (v8+): `.hoodie/{timeline_path}/{history_path}` (LSM history)
    fn get_archived_timeline_dir(&self) -> String {
        match self.layout {
            TimelineLayout::V1Active | TimelineLayout::V1Archived => {
                // Layout 1 uses hoodie.archivelog.folder for archived timeline
                self.hudi_configs.get_or_default(ArchiveLogFolder).into()
            }
            TimelineLayout::V2Active | TimelineLayout::V2Archived => {
                // Layout 2 uses LSM history directory
                let timeline_path: String = self.hudi_configs.get_or_default(TimelinePath).into();
                let history_path: String =
                    self.hudi_configs.get_or_default(TimelineHistoryPath).into();
                format!("{HUDI_METADATA_DIR}/{timeline_path}/{history_path}")
            }
        }
    }

    /// Returns the appropriate timeline directory based on loader type (active vs archived).
    ///
    /// This is a convenience method that delegates to either `get_active_timeline_dir`
    /// or `get_archived_timeline_dir` depending on the layout.
    pub fn get_timeline_dir(&self) -> String {
        match self.layout {
            TimelineLayout::V1Active | TimelineLayout::V2Active => self.get_active_timeline_dir(),
            TimelineLayout::V1Archived | TimelineLayout::V2Archived => {
                self.get_archived_timeline_dir()
            }
        }
    }

    /// Every instant time that has a `requested` or `inflight` file and no
    /// completed one, for **any** action.
    ///
    /// Deliberately not built on [`TimelineSelector`], which resolves the action
    /// through [`Action`](crate::timeline::instant::Action) and so can only see
    /// the three commit actions the enum names. Java's `getValidInstantTimestamps`
    /// takes its `datasetPendingInstants` from `filterInflightsAndRequested()`
    /// over the whole active timeline, which includes compaction, clean,
    /// clustering, indexing and savepoint. A metadata delta commit whose data
    /// instant is a `.compaction.inflight` is one Java excludes and a
    /// commit-action-only view admits.
    ///
    /// Only the timestamp is needed, never the action, so the file name is
    /// parsed structurally instead: the state is the suffix, and the instant
    /// time is what precedes the first `.` — minus the completion time that
    /// timeline layout 2 appends after a `_`.
    pub async fn list_pending_instant_times(&self) -> Result<HashSet<String>> {
        let dir = match self.layout {
            TimelineLayout::V1Active => HUDI_METADATA_DIR.to_string(),
            TimelineLayout::V2Active => self.get_timeline_dir(),
            _ => {
                return Err(CoreError::Unsupported(
                    "Pending instants can only be listed from an active timeline.".to_string(),
                ));
            }
        };

        let mut pending: HashSet<String> = HashSet::new();
        let mut completed: HashSet<String> = HashSet::new();
        for file_info in self.storage.list_files(Some(&dir)).await? {
            let name = file_info.name.as_str();
            if name.starts_with("history/") || name.ends_with(".crc") {
                continue;
            }
            let Some((timestamp_part, suffix)) = name.split_once('.') else {
                continue;
            };
            // The instant time only; layout 2 completed files append the
            // completion time after a `_`.
            let timestamp = timestamp_part
                .split_once('_')
                .map_or(timestamp_part, |(requested, _)| requested);
            if timestamp.is_empty() {
                continue;
            }
            // `{ts}.inflight` with no action is a legacy commit inflight.
            match suffix.rsplit_once('.').map_or(suffix, |(_, state)| state) {
                "requested" | "inflight" => pending.insert(timestamp.to_string()),
                _ => completed.insert(timestamp.to_string()),
            };
        }

        // An instant is listed once per state file it has, so one that completed
        // also has a requested and an inflight file. Pending means it reached no
        // completed state at all.
        Ok(&pending - &completed)
    }

    pub async fn load_instants(
        &self,
        selector: &TimelineSelector,
        desc: bool,
    ) -> Result<Vec<Instant>> {
        match self.layout {
            TimelineLayout::V1Active => {
                let files = self.storage.list_files(Some(HUDI_METADATA_DIR)).await?;
                let mut instants = Vec::with_capacity(files.len() / 3);

                for file_info in files {
                    match selector.try_create_instant(file_info.name.as_str()) {
                        Ok(instant) => instants.push(instant),
                        Err(e) => {
                            debug!("Instant not created from file {file_info:?} due to: {e:?}");
                        }
                    }
                }

                instants.sort_unstable();
                instants.shrink_to_fit();

                if desc {
                    Ok(instants.into_iter().rev().collect())
                } else {
                    Ok(instants)
                }
            }
            TimelineLayout::V2Active => {
                let timeline_dir = self.get_timeline_dir();
                let files = self.storage.list_files(Some(&timeline_dir)).await?;
                let mut instants = Vec::new();

                for file_info in files {
                    // TODO: make `storage.list_files` api support such filtering, like ignore crc and return files only
                    if file_info.name.starts_with("history/") || file_info.name.ends_with(".crc") {
                        continue;
                    }
                    match selector.try_create_instant(file_info.name.as_str()) {
                        Ok(instant) => instants.push(instant),
                        Err(e) => {
                            debug!("Instant not created from file {file_info:?} due to: {e:?}");
                        }
                    }
                }

                instants.sort_unstable();
                instants.shrink_to_fit();

                if desc {
                    Ok(instants.into_iter().rev().collect())
                } else {
                    Ok(instants)
                }
            }
            _ => Err(CoreError::Unsupported(
                "Loading from this timeline layout is not implemented yet.".to_string(),
            )),
        }
    }

    /// Load archived timeline instants based on selector criteria.
    ///
    /// # Behavior
    ///
    /// - Returns empty Vec if this is an active loader (not an archived loader)
    /// - Attempts to load archived instants, propagating any errors
    ///
    /// Note: This method assumes the archived loader was created only when
    /// `TimelineArchivedReadEnabled` is true. The config check is done in the builder.
    ///
    /// # Not yet implemented
    ///
    /// Neither layout reads archived instants today. V2 returns empty outright.
    /// V1 lists the archive folder and parses file *names* as instants, but Hudi
    /// stores archived instants as records inside Avro archive logs
    /// (`.commits_.archive.*`) rather than as one file per instant, so that
    /// listing yields nothing either.
    ///
    /// Callers must therefore not treat an empty result as "no archived instants
    /// in range" — see `Table::warn_if_window_predates_active_timeline`, which
    /// reports the shortfall rather than letting a range query return quietly
    /// short.
    ///
    /// # Arguments
    ///
    /// * `selector` - The criteria for selecting instants (actions, states, time range)
    /// * `desc` - If true, return instants in descending order by timestamp
    pub(crate) async fn load_archived_instants(
        &self,
        selector: &TimelineSelector,
        desc: bool,
    ) -> Result<Vec<Instant>> {
        // Early return for active loaders - they don't have archived parts
        match self.layout {
            TimelineLayout::V1Active | TimelineLayout::V2Active => return Ok(Vec::new()),
            _ => {}
        }

        match self.layout {
            TimelineLayout::V1Archived => {
                // Resolve archive folder from configs or fallback
                let archive_dir: String = self.hudi_configs.get_or_default(ArchiveLogFolder).into();

                // List files and try creating instants through selector
                let files = self.storage.list_files(Some(&archive_dir)).await?;
                let mut instants = Vec::new();
                for file_info in files {
                    if let Ok(instant) = selector.try_create_instant(file_info.name.as_str()) {
                        instants.push(instant);
                    }
                }
                instants.sort_unstable();
                if desc {
                    instants.reverse();
                }
                Ok(instants)
            }
            TimelineLayout::V2Archived => {
                // TODO: Implement v2 LSM history reader. For now, return empty.
                let _ = (selector, desc);
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Load instant metadata from storage and parse based on the layout version.
    ///
    /// Layout Version 1 (v6-v8): JSON format
    /// Layout Version 2 (v8+): Avro format
    ///
    /// Returns the metadata as a JSON Map for uniform processing.
    /// The raw bytes of one instant file.
    ///
    /// `load_instant_metadata` decodes commit metadata; a rollback, restore or
    /// plan is a different Avro record, so its reader needs the bytes rather
    /// than a decoded commit.
    pub(crate) async fn load_instant_bytes(&self, instant: &Instant) -> Result<Vec<u8>> {
        let timeline_dir = self.get_timeline_dir();
        let path = instant.relative_path_with_base(&timeline_dir)?;
        Ok(self.storage.get_file_data(path.as_str()).await?.to_vec())
    }

    pub(crate) async fn load_instant_metadata(
        &self,
        instant: &Instant,
    ) -> Result<Map<String, Value>> {
        let timeline_dir = self.get_timeline_dir();
        let path = instant.relative_path_with_base(&timeline_dir)?;
        let bytes = self.storage.get_file_data(path.as_str()).await?;

        match self.layout {
            TimelineLayout::V1Active | TimelineLayout::V1Archived => {
                // Layout 1: JSON format
                serde_json::from_slice(&bytes).map_err(|e| {
                    CoreError::Timeline(format!("Failed to parse JSON commit metadata: {e}"))
                })
            }
            TimelineLayout::V2Active | TimelineLayout::V2Archived => {
                // Layout 2: Avro format
                let metadata = HoodieCommitMetadata::from_avro_bytes(&bytes)?;
                metadata.to_json_map()
            }
        }
    }

    /// Load instant metadata and return as a JSON string.
    ///
    /// Layout Version 1 (v6-v8): Return raw JSON bytes
    /// Layout Version 2 (v8+): Parse Avro and serialize to JSON
    pub(crate) async fn load_instant_metadata_as_json(&self, instant: &Instant) -> Result<String> {
        let timeline_dir = self.get_timeline_dir();
        let path = instant.relative_path_with_base(&timeline_dir)?;
        let bytes = self.storage.get_file_data(path.as_str()).await?;

        match self.layout {
            TimelineLayout::V1Active | TimelineLayout::V1Archived => {
                // Layout 1: JSON format - return raw bytes as string
                String::from_utf8(bytes.to_vec()).map_err(|e| {
                    CoreError::Timeline(format!("Failed to convert JSON bytes to string: {e}"))
                })
            }
            TimelineLayout::V2Active | TimelineLayout::V2Archived => {
                // Layout 2: Avro format - deserialize then serialize to JSON
                let metadata = HoodieCommitMetadata::from_avro_bytes(&bytes)?;
                serde_json::to_string(&metadata).map_err(|e| {
                    CoreError::Timeline(format!("Failed to serialize metadata to JSON: {e}"))
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HudiConfigs;
    use crate::config::table::HudiTableConfig;
    use std::collections::HashMap;

    /// An archived instant is enumerated when the archived loader is used.
    ///
    /// Nothing covered this: the existing tests assert only that the loader is
    /// *constructed* when `hoodie.internal.timeline.archived.enabled` is set,
    /// never that it finds anything. Without this, the opt-in could resolve a
    /// directory and return nothing and every test would still pass.
    ///
    /// Layout v1 keeps archived instants as ordinarily-named files under
    /// `hoodie.archivelog.folder`, so the timeline can be laid out directly
    /// rather than generated: what is under test is the enumeration, not the
    /// file format.
    #[tokio::test]
    async fn test_archived_instants_are_enumerated_from_the_archive_folder() {
        use crate::timeline::selector::TimelineSelector;

        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        std::fs::create_dir_all(base.join(".hoodie/archived")).unwrap();
        // One archived instant, and one active instant the archived loader must
        // NOT pick up — it reads the archive folder, not the table's timeline.
        std::fs::write(base.join(".hoodie/archived/20240101000000000.commit"), "{}").unwrap();
        std::fs::write(base.join(".hoodie/20240301000000000.commit"), "{}").unwrap();

        let configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath.as_ref().to_string(),
            format!("file://{}", base.display()),
        )]));
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone()).unwrap();

        // A window covering both instants, so exclusion cannot be an artifact of
        // the range.
        let selector = TimelineSelector::completed_commits_in_range(
            configs.clone(),
            Some("20240101000000000"),
            Some("20240401000000000"),
        )
        .unwrap();
        assert!(
            selector.has_time_filter(),
            "the archived timeline is only consulted for a time-filtered read"
        );

        let archived = TimelineLoader::new_layout_one_archived(configs.clone(), storage.clone());
        let found = archived
            .load_archived_instants(&selector, false)
            .await
            .unwrap();
        let timestamps: Vec<&str> = found.iter().map(|i| i.timestamp.as_str()).collect();
        assert_eq!(
            timestamps,
            vec!["20240101000000000"],
            "the archived loader must return the archived instant and only that"
        );

        // An ACTIVE loader has no archived part and must return nothing, which is
        // what makes the opt-in the thing that changes the answer.
        let active = TimelineLoader::new_layout_one_active(configs, storage);
        assert!(
            active
                .load_archived_instants(&selector, false)
                .await
                .unwrap()
                .is_empty(),
            "an active loader must not read the archive folder"
        );
    }

    fn create_test_configs() -> Arc<HudiConfigs> {
        let mut options = HashMap::new();
        options.insert(
            HudiTableConfig::BasePath.as_ref().to_string(),
            "/tmp/test".to_string(),
        );
        Arc::new(HudiConfigs::new(options))
    }

    fn create_test_storage(configs: Arc<HudiConfigs>) -> Arc<Storage> {
        Storage::new(Arc::new(HashMap::new()), configs).unwrap()
    }

    #[test]
    fn test_layout_one_active_directory() {
        let configs = create_test_configs();
        let storage = create_test_storage(configs.clone());
        let loader = TimelineLoader::new_layout_one_active(configs, storage);

        assert_eq!(loader.get_active_timeline_dir(), HUDI_METADATA_DIR);
        assert_eq!(loader.get_timeline_dir(), HUDI_METADATA_DIR);
    }

    #[test]
    fn test_layout_one_archived_directory() {
        let configs = create_test_configs();
        let storage = create_test_storage(configs.clone());
        let loader = TimelineLoader::new_layout_one_archived(configs, storage);

        // Default archived folder
        let expected = ".hoodie/archived";
        assert_eq!(loader.get_archived_timeline_dir(), expected);
        assert_eq!(loader.get_timeline_dir(), expected);
    }

    #[test]
    fn test_layout_two_active_directory() {
        let configs = create_test_configs();
        let storage = create_test_storage(configs.clone());
        let loader = TimelineLoader::new_layout_two_active(configs, storage);

        // Default timeline path
        let expected = format!("{HUDI_METADATA_DIR}/timeline");
        assert_eq!(loader.get_active_timeline_dir(), expected);
        assert_eq!(loader.get_timeline_dir(), expected);
    }

    #[test]
    fn test_layout_two_archived_directory() {
        let configs = create_test_configs();
        let storage = create_test_storage(configs.clone());
        let loader = TimelineLoader::new_layout_two_archived(configs, storage);

        // Default timeline path and history path
        let expected = format!("{HUDI_METADATA_DIR}/timeline/history");
        assert_eq!(loader.get_archived_timeline_dir(), expected);
        assert_eq!(loader.get_timeline_dir(), expected);
    }

    #[test]
    fn test_custom_archive_folder() {
        let mut options = HashMap::new();
        options.insert(
            HudiTableConfig::BasePath.as_ref().to_string(),
            "/tmp/test".to_string(),
        );
        options.insert(
            HudiTableConfig::ArchiveLogFolder.as_ref().to_string(),
            ".hoodie/custom_archive".to_string(),
        );
        let configs = Arc::new(HudiConfigs::new(options));
        let storage = create_test_storage(configs.clone());
        let loader = TimelineLoader::new_layout_one_archived(configs, storage);

        assert_eq!(loader.get_archived_timeline_dir(), ".hoodie/custom_archive");
    }

    #[test]
    fn test_custom_timeline_paths() {
        let mut options = HashMap::new();
        options.insert(
            HudiTableConfig::BasePath.as_ref().to_string(),
            "/tmp/test".to_string(),
        );
        options.insert(
            HudiTableConfig::TimelinePath.as_ref().to_string(),
            "custom_timeline".to_string(),
        );
        options.insert(
            HudiTableConfig::TimelineHistoryPath.as_ref().to_string(),
            "custom_history".to_string(),
        );
        let configs = Arc::new(HudiConfigs::new(options));
        let storage = create_test_storage(configs.clone());

        let loader = TimelineLoader::new_layout_two_active(configs.clone(), storage.clone());
        assert_eq!(
            loader.get_active_timeline_dir(),
            format!("{HUDI_METADATA_DIR}/custom_timeline")
        );

        let archived_loader = TimelineLoader::new_layout_two_archived(configs, storage);
        assert_eq!(
            archived_loader.get_archived_timeline_dir(),
            format!("{HUDI_METADATA_DIR}/custom_timeline/custom_history")
        );
    }

    #[test]
    fn test_layout_type_checks() {
        let configs = create_test_configs();
        let storage = create_test_storage(configs.clone());

        let v2_active = TimelineLoader::new_layout_two_active(configs.clone(), storage.clone());
        assert!(v2_active.is_layout_two_active());
        assert!(!v2_active.is_layout_two_archived());

        let v2_archived = TimelineLoader::new_layout_two_archived(configs, storage);
        assert!(!v2_archived.is_layout_two_active());
        assert!(v2_archived.is_layout_two_archived());
    }

    #[test]
    fn test_storage_access() {
        let configs = create_test_configs();
        let storage = create_test_storage(configs.clone());
        let storage_ptr = Arc::as_ptr(&storage);

        let loader = TimelineLoader::new_layout_one_active(configs, storage);

        // Verify storage is accessible and is the same instance
        assert_eq!(Arc::as_ptr(loader.storage()), storage_ptr);
    }

    /// Every action counts toward pending, and only instants with no completed
    /// file at all.
    ///
    /// Layout v1, where each instant is a plain `{ts}.{action}[.{state}]` file
    /// under `.hoodie`. The actions here are deliberately ones
    /// [`Action`](crate::timeline::instant::Action) cannot represent — a
    /// selector-based listing drops them before anything can count them, which
    /// is the whole reason this method parses file names itself.
    #[tokio::test]
    async fn pending_instants_span_every_action_and_exclude_the_completed() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        std::fs::create_dir_all(base.join(".hoodie")).unwrap();

        for name in [
            // Pending, under actions the Action enum does not name.
            "20240101000000001.compaction.inflight",
            "20240101000000002.clean.requested",
            "20240101000000003.indexing.inflight",
            // Pending, under actions it does.
            "20240101000000004.deltacommit.requested",
            // A legacy bare inflight, which means a commit.
            "20240101000000005.inflight",
            // Completed, and so not pending -- even though it also has the
            // requested and inflight files every completed instant leaves.
            "20240101000000006.commit",
            "20240101000000006.commit.inflight",
            "20240101000000006.commit.requested",
            // A completed clean, likewise not pending.
            "20240101000000007.clean",
            "20240101000000007.clean.requested",
        ] {
            std::fs::write(base.join(".hoodie").join(name), "{}").unwrap();
        }

        let configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath.as_ref().to_string(),
            format!("file://{}", base.display()),
        )]));
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone()).unwrap();
        let loader = TimelineLoader::new_layout_one_active(configs, storage);

        let pending = loader.list_pending_instant_times().await.unwrap();
        assert_eq!(
            pending,
            [
                "20240101000000001",
                "20240101000000002",
                "20240101000000003",
                "20240101000000004",
                "20240101000000005",
            ]
            .into_iter()
            .map(str::to_string)
            .collect::<HashSet<String>>()
        );
    }

    /// Layout 2 appends the completion time to a completed instant's name, so
    /// the instant time is what precedes the `_`. Reading the whole
    /// `{ts}_{completionTs}` as the instant time would leave the completed
    /// instant's own requested file counted as pending forever.
    #[tokio::test]
    async fn a_layout_two_completed_instant_cancels_its_pending_files() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        std::fs::create_dir_all(base.join(".hoodie/timeline")).unwrap();

        for name in [
            "20240101000000001_20240101000000009.deltacommit",
            "20240101000000001.deltacommit.inflight",
            "20240101000000001.deltacommit.requested",
            "20240101000000002.compaction.inflight",
        ] {
            std::fs::write(base.join(".hoodie/timeline").join(name), "{}").unwrap();
        }

        let configs = Arc::new(HudiConfigs::new([
            (
                HudiTableConfig::BasePath.as_ref().to_string(),
                format!("file://{}", base.display()),
            ),
            (
                HudiTableConfig::TimelineLayoutVersion.as_ref().to_string(),
                "2".to_string(),
            ),
        ]));
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone()).unwrap();
        let loader = TimelineLoader::new_layout_two_active(configs, storage);

        let pending = loader.list_pending_instant_times().await.unwrap();
        assert_eq!(
            pending,
            ["20240101000000002"]
                .into_iter()
                .map(str::to_string)
                .collect::<HashSet<String>>(),
            "the completed delta commit must cancel its own requested/inflight files"
        );
    }
}
