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

use crate::config::table::HudiTableConfig::{TimelineHistoryPath, TimelinePath};
use crate::error::CoreError;
use crate::metadata::HUDI_METADATA_DIR;
use crate::storage::Storage;
use crate::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineManifest {
    pub version: i64,
    pub entries: Vec<ManifestEntry>,
}

/// Entry in an LSM timeline manifest.
/// Each entry describes a compacted timeline file covering a time range
/// in the LSM history directory.
/// - `file_name`: relative path of the compacted timeline file under history
/// - `min_instant`/`max_instant`: smallest/largest instant timestamps covered
/// - `level`: LSM level where this file resides
/// - `file_size`: size in bytes of the file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub file_name: String,
    pub min_instant: String,
    pub max_instant: String,
    pub level: i32,
    pub file_size: i64,
}

/// LSM tree for v8+ timeline history management.
/// The paths are resolved from configs on-the-fly via `hoodie.timeline.path` and `hoodie.timeline.history.path`.
pub struct LSMTree {
    storage: Arc<Storage>,
}

impl LSMTree {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    /// Returns the timeline directory path, resolved from configs.
    pub fn timeline_dir(&self) -> String {
        let timeline_path: String = self.storage.hudi_configs.get_or_default(TimelinePath).into();
        format!("{}/{}", HUDI_METADATA_DIR, timeline_path)
    }

    /// Returns the history directory path, resolved from configs.
    pub fn history_dir(&self) -> String {
        let timeline_path: String = self.storage.hudi_configs.get_or_default(TimelinePath).into();
        let history_path: String = self
            .storage
            .hudi_configs
            .get_or_default(TimelineHistoryPath)
            .into();
        format!("{}/{}/{}", HUDI_METADATA_DIR, timeline_path, history_path)
    }

    pub async fn read_manifest(&self) -> Result<Option<TimelineManifest>> {
        let history_dir = self.history_dir();
        let version_path = format!("{}/_version_", history_dir);
        if let Ok(data) = self.storage.get_file_data(&version_path).await {
            let version_str =
                String::from_utf8(data.to_vec()).map_err(|e| CoreError::Timeline(e.to_string()))?;
            let version = version_str
                .trim()
                .parse::<i64>()
                .map_err(|e| CoreError::Timeline(e.to_string()))?;
            let manifest_path = format!("{}/manifest_{}", history_dir, version);
            let data = self.storage.get_file_data(&manifest_path).await?;
            let manifest: TimelineManifest =
                serde_json::from_slice(&data).map_err(|e| CoreError::Timeline(e.to_string()))?;
            Ok(Some(manifest))
        } else {
            Ok(None)
        }
    }
}
