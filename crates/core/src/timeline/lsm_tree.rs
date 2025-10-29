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

use crate::error::CoreError;
use crate::storage::Storage;
use crate::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const LSM_TIMELINE_DIR: &str = ".hoodie/timeline";

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

pub struct LSMTree {
    storage: Arc<Storage>,
}

impl LSMTree {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    pub async fn read_manifest(&self) -> Result<Option<TimelineManifest>> {
        let version_path = format!("{}/history/_version_", LSM_TIMELINE_DIR);
        if let Ok(data) = self.storage.get_file_data(&version_path).await {
            let version_str =
                String::from_utf8(data.to_vec()).map_err(|e| CoreError::Timeline(e.to_string()))?;
            let version = version_str
                .trim()
                .parse::<i64>()
                .map_err(|e| CoreError::Timeline(e.to_string()))?;
            let manifest_path = format!("{}/history/manifest_{}", LSM_TIMELINE_DIR, version);
            let data = self.storage.get_file_data(&manifest_path).await?;
            let manifest: TimelineManifest =
                serde_json::from_slice(&data).map_err(|e| CoreError::Timeline(e.to_string()))?;
            Ok(Some(manifest))
        } else {
            Ok(None)
        }
    }
}
