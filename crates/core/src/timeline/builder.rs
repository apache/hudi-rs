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
        let table_version = match self.hudi_configs.get(TableVersion) {
            Ok(v) => v.to::<isize>(),
            Err(_) => {
                return Ok((
                    TimelineLoader::LayoutOneActive(self.storage.clone()),
                    Some(TimelineLoader::LayoutOneArchived(self.storage.clone())),
                ))
            }
        };

        let layout_version = self
            .hudi_configs
            .try_get(TimelineLayoutVersion)
            .map(|v| v.to::<isize>())
            .unwrap_or_else(|| if table_version == 8 { 2 } else { 1 });

        match layout_version {
            1 => {
                if !(6..=8).contains(&table_version) {
                    return Err(CoreError::Unsupported(format!(
                        "Unsupported table version {} with timeline layout version {}",
                        table_version, layout_version
                    )));
                }
                Ok((
                    TimelineLoader::LayoutOneActive(self.storage.clone()),
                    Some(TimelineLoader::LayoutOneArchived(self.storage.clone())),
                ))
            }
            2 => {
                if table_version != 8 {
                    return Err(CoreError::Unsupported(format!(
                        "Unsupported table version {} with timeline layout version {}",
                        table_version, layout_version
                    )));
                }
                Ok((
                    TimelineLoader::LayoutTwoActive(self.storage.clone()),
                    Some(TimelineLoader::LayoutTwoArchived(self.storage.clone())),
                ))
            }
            _ => Err(CoreError::Unsupported(format!(
                "Unsupported timeline layout version: {}",
                layout_version
            ))),
        }
    }
}
