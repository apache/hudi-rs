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

use crate::config::table::HudiTableConfig::{ArchiveLogFolder, TimelineHistoryPath, TimelinePath};
use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::metadata::commit::HoodieCommitMetadata;
use crate::metadata::HUDI_METADATA_DIR;
use crate::storage::Storage;
use crate::timeline::instant::Instant;
use crate::timeline::selector::TimelineSelector;
use crate::Result;
use log::debug;
use serde_json::{Map, Value};
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
                format!("{}/{}", HUDI_METADATA_DIR, timeline_path)
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
                format!("{}/{}/{}", HUDI_METADATA_DIR, timeline_path, history_path)
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
                            debug!(
                                "Instant not created from file {:?} due to: {:?}",
                                file_info, e
                            );
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
                            debug!(
                                "Instant not created from file {:?} due to: {:?}",
                                file_info, e
                            );
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
                    CoreError::Timeline(format!("Failed to parse JSON commit metadata: {}", e))
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
                    CoreError::Timeline(format!("Failed to convert JSON bytes to string: {}", e))
                })
            }
            TimelineLayout::V2Active | TimelineLayout::V2Archived => {
                // Layout 2: Avro format - deserialize then serialize to JSON
                let metadata = HoodieCommitMetadata::from_avro_bytes(&bytes)?;
                serde_json::to_string(&metadata).map_err(|e| {
                    CoreError::Timeline(format!("Failed to serialize metadata to JSON: {}", e))
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig;
    use crate::config::HudiConfigs;
    use std::collections::HashMap;

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
        let expected = format!("{}/timeline", HUDI_METADATA_DIR);
        assert_eq!(loader.get_active_timeline_dir(), expected);
        assert_eq!(loader.get_timeline_dir(), expected);
    }

    #[test]
    fn test_layout_two_archived_directory() {
        let configs = create_test_configs();
        let storage = create_test_storage(configs.clone());
        let loader = TimelineLoader::new_layout_two_archived(configs, storage);

        // Default timeline path and history path
        let expected = format!("{}/timeline/history", HUDI_METADATA_DIR);
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
            format!("{}/custom_timeline", HUDI_METADATA_DIR)
        );

        let archived_loader = TimelineLoader::new_layout_two_archived(configs, storage);
        assert_eq!(
            archived_loader.get_archived_timeline_dir(),
            format!("{}/custom_timeline/custom_history", HUDI_METADATA_DIR)
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
}
