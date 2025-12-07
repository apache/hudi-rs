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

use crate::config::internal::HudiInternalConfig::{
    TimelineArchivedReadEnabled, TimelineArchivedUnavailableBehavior,
};
use crate::config::table::HudiTableConfig::{ArchiveLogFolder, TimelineHistoryPath, TimelinePath};
use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::metadata::HUDI_METADATA_DIR;
use crate::storage::Storage;
use crate::timeline::instant::Instant;
use crate::timeline::selector::TimelineSelector;
use crate::Result;
use log::debug;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum TimelineLoader {
    LayoutOneActive(Arc<Storage>),
    LayoutOneArchived(Arc<Storage>),
    LayoutTwoActive(Arc<Storage>),
    LayoutTwoArchived(Arc<Storage>),
}

#[allow(dead_code)]
impl TimelineLoader {
    /// Returns the storage for this loader.
    fn storage(&self) -> &Arc<Storage> {
        match self {
            TimelineLoader::LayoutOneActive(s)
            | TimelineLoader::LayoutOneArchived(s)
            | TimelineLoader::LayoutTwoActive(s)
            | TimelineLoader::LayoutTwoArchived(s) => s,
        }
    }

    /// Returns the base directory for timeline instants based on the loader type.
    /// - Layout One (pre-v8): `.hoodie/`
    /// - Layout Two (v8+): configurable via `hoodie.timeline.path` (default: `.hoodie/timeline/`)
    pub fn get_timeline_dir(&self) -> String {
        match self {
            TimelineLoader::LayoutOneActive(_) | TimelineLoader::LayoutOneArchived(_) => {
                HUDI_METADATA_DIR.to_string()
            }
            TimelineLoader::LayoutTwoActive(storage)
            | TimelineLoader::LayoutTwoArchived(storage) => {
                let timeline_path: String =
                    storage.hudi_configs.get_or_default(TimelinePath).into();
                format!("{}/{}", HUDI_METADATA_DIR, timeline_path)
            }
        }
    }

    /// Returns the history directory for v8+ (Layout Two) loaders.
    /// Resolves from configs: `.hoodie/{timeline_path}/{history_path}`
    fn get_history_dir(&self) -> Option<String> {
        match self {
            TimelineLoader::LayoutTwoActive(storage)
            | TimelineLoader::LayoutTwoArchived(storage) => {
                let timeline_path: String =
                    storage.hudi_configs.get_or_default(TimelinePath).into();
                let history_path: String = storage
                    .hudi_configs
                    .get_or_default(TimelineHistoryPath)
                    .into();
                Some(format!(
                    "{}/{}/{}",
                    HUDI_METADATA_DIR, timeline_path, history_path
                ))
            }
            _ => None,
        }
    }

    pub async fn load_instants(
        &self,
        selector: &TimelineSelector,
        desc: bool,
    ) -> Result<Vec<Instant>> {
        match self {
            TimelineLoader::LayoutOneActive(storage) => {
                let files = storage.list_files(Some(HUDI_METADATA_DIR)).await?;
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
            TimelineLoader::LayoutTwoActive(storage) => {
                let timeline_dir = self.get_timeline_dir();
                let files = storage.list_files(Some(&timeline_dir)).await?;
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

    pub(crate) async fn load_archived_instants(
        &self,
        selector: &TimelineSelector,
        desc: bool,
    ) -> Result<Vec<Instant>> {
        // Config wiring: if archived read not enabled, return behavior based on policy
        let storage = match self {
            TimelineLoader::LayoutOneArchived(storage)
            | TimelineLoader::LayoutTwoArchived(storage) => storage.clone(),
            _ => return Ok(Vec::new()), // Active loaders don't have archived parts
        };

        let configs: Arc<HudiConfigs> = storage.hudi_configs.clone();
        let enabled: bool = configs.get_or_default(TimelineArchivedReadEnabled).into();
        if !enabled {
            let behavior: String = configs
                .get_or_default(TimelineArchivedUnavailableBehavior)
                .into();
            let behavior = behavior.to_ascii_lowercase();
            return match behavior.as_str() {
                "error" => Err(CoreError::Unsupported(
                    "Archived timeline read is disabled; shorten time range or enable archived read"
                        .to_string(),
                )),
                _ => Ok(Vec::new()), // continue silently with empty archived
            };
        }

        match self {
            TimelineLoader::LayoutOneArchived(storage) => {
                // Resolve archive folder from configs or fallback
                let archive_dir: String = configs.get_or_default(ArchiveLogFolder).into();

                // List files and try creating instants through selector
                let files = storage.list_files(Some(&archive_dir)).await?;
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
            TimelineLoader::LayoutTwoArchived(_) => {
                // TODO: Implement v2 LSM history reader. For now, return empty.
                let _ = (selector, desc);
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }
}
