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

use crate::config::internal::HudiInternalConfig::TimelineArchivedReadEnabled;
use crate::config::table::HudiTableConfig::{TableVersion, TimelineLayoutVersion};
use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::storage::Storage;
use crate::timeline::loader::TimelineLoader;
use crate::timeline::Timeline;
use crate::Result;
use std::sync::Arc;

pub struct TimelineBuilder {
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
}

impl TimelineBuilder {
    pub fn new(hudi_configs: Arc<HudiConfigs>, storage: Arc<Storage>) -> Self {
        Self {
            hudi_configs,
            storage,
        }
    }

    pub async fn build(self) -> Result<Timeline> {
        let (active_loader, archived_loader) = self.resolve_loader_config()?;
        let timeline = Timeline::new(
            self.hudi_configs,
            self.storage,
            active_loader,
            archived_loader,
        );
        Ok(timeline)
    }

    fn resolve_loader_config(&self) -> Result<(TimelineLoader, Option<TimelineLoader>)> {
        // Check if archived timeline reading is enabled
        let archived_enabled: bool = self
            .hudi_configs
            .get_or_default(TimelineArchivedReadEnabled)
            .into();

        let table_version: isize = match self.hudi_configs.get(TableVersion) {
            Ok(v) => v.into(),
            Err(_) => {
                let archived_loader = if archived_enabled {
                    Some(TimelineLoader::new_layout_one_archived(
                        self.hudi_configs.clone(),
                        self.storage.clone(),
                    ))
                } else {
                    None
                };
                return Ok((
                    TimelineLoader::new_layout_one_active(
                        self.hudi_configs.clone(),
                        self.storage.clone(),
                    ),
                    archived_loader,
                ));
            }
        };

        let layout_version: isize = self
            .hudi_configs
            .try_get(TimelineLayoutVersion)
            .map(|v| v.into())
            .unwrap_or_else(|| if table_version == 8 { 2isize } else { 1isize });

        match layout_version {
            1 => {
                if !(6..=8).contains(&table_version) {
                    return Err(CoreError::Unsupported(format!(
                        "Unsupported table version {} with timeline layout version {}",
                        table_version, layout_version
                    )));
                }
                let archived_loader = if archived_enabled {
                    Some(TimelineLoader::new_layout_one_archived(
                        self.hudi_configs.clone(),
                        self.storage.clone(),
                    ))
                } else {
                    None
                };
                Ok((
                    TimelineLoader::new_layout_one_active(
                        self.hudi_configs.clone(),
                        self.storage.clone(),
                    ),
                    archived_loader,
                ))
            }
            2 => {
                if table_version < 8 {
                    return Err(CoreError::Unsupported(format!(
                        "Unsupported table version {} with timeline layout version {}",
                        table_version, layout_version
                    )));
                }
                let archived_loader = if archived_enabled {
                    Some(TimelineLoader::new_layout_two_archived(
                        self.hudi_configs.clone(),
                        self.storage.clone(),
                    ))
                } else {
                    None
                };
                Ok((
                    TimelineLoader::new_layout_two_active(
                        self.hudi_configs.clone(),
                        self.storage.clone(),
                    ),
                    archived_loader,
                ))
            }
            _ => Err(CoreError::Unsupported(format!(
                "Unsupported timeline layout version: {}",
                layout_version
            ))),
        }
    }
}
