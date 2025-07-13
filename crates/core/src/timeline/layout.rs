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

use crate::config::HudiConfigs;
use crate::storage::Storage;
use crate::timeline::instant::Instant;
use crate::timeline::selector::TimelineSelector;
use crate::Result;
use std::sync::Arc;

/// Timeline layout implementations for different table versions.
#[derive(Debug, Clone)]
pub enum TimelineLayoutType {
    Active(super::active::ActiveTimeline),
    Lsm(super::lsm::LsmTimeline),
}

impl TimelineLayoutType {
    /// Create layout instance based on table configuration.
    pub fn create_layout(
        hudi_configs: Arc<HudiConfigs>,
        storage: Arc<Storage>,
    ) -> Result<Self> {
        use crate::config::table::HudiTableConfig::{TableVersion, TimelineLayoutVersion};
        
        let table_version = match hudi_configs.get(TableVersion) {
            Ok(v) => v.to::<isize>(),
            Err(_) => {
                // Fallback to default active timeline layout for invalid table versions
                return Ok(TimelineLayoutType::Active(super::active::ActiveTimeline::new(hudi_configs, storage)));
            }
        };
        let layout_version = hudi_configs
            .try_get(TimelineLayoutVersion)
            .map(|v| v.to::<isize>())
            .unwrap_or(1);

        match (table_version, layout_version) {
            (8, 2) => Ok(TimelineLayoutType::Lsm(super::lsm::LsmTimeline::new(hudi_configs, storage))),
            (5..=8, 1) => Ok(TimelineLayoutType::Active(super::active::ActiveTimeline::new(hudi_configs, storage))),
            _ => Err(crate::error::CoreError::Unsupported(format!(
                "Unsupported table version {} with timeline layout version {}",
                table_version, layout_version
            ))),
        }
    }

    pub async fn load_instants(
        &self,
        selector: &TimelineSelector,
        desc: bool,
    ) -> Result<Vec<Instant>> {
        match self {
            TimelineLayoutType::Active(layout) => layout.load_instants(selector, desc).await,
            TimelineLayoutType::Lsm(layout) => layout.load_instants(selector, desc).await,
        }
    }
}

/// Factory for creating timeline layout implementations.
pub struct TimelineLayoutFactory;

impl TimelineLayoutFactory {
    pub fn create_layout(
        hudi_configs: Arc<HudiConfigs>,
        storage: Arc<Storage>,
    ) -> Result<TimelineLayoutType> {
        TimelineLayoutType::create_layout(hudi_configs, storage)
    }
}
