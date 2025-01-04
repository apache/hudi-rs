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
mod instant;
mod selector;

use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::file_group::FileGroup;
use crate::storage::Storage;
use crate::timeline::selector::TimelineSelector;
use crate::Result;
use arrow_schema::Schema;
use instant::Instant;
use log::debug;
use parquet::arrow::parquet_to_arrow_schema;
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
    pub instants: Vec<Instant>,
}

impl Timeline {
    #[cfg(test)]
    pub async fn new_from_instants(
        hudi_configs: Arc<HudiConfigs>,
        storage_options: Arc<HashMap<String, String>>,
        instants: Vec<Instant>,
    ) -> Result<Self> {
        let storage = Storage::new(storage_options.clone(), hudi_configs.clone())?;
        Ok(Self {
            storage,
            hudi_configs,
            instants,
        })
    }

    pub async fn new_from_storage(
        hudi_configs: Arc<HudiConfigs>,
        storage_options: Arc<HashMap<String, String>>,
    ) -> Result<Self> {
        let storage = Storage::new(storage_options.clone(), hudi_configs.clone())?;
        let selector = TimelineSelector::active_completed_commits(hudi_configs.clone());
        let instants = Self::load_instants(&selector, &storage).await?;
        Ok(Self {
            storage,
            hudi_configs,
            instants,
        })
    }

    async fn load_instants(selector: &TimelineSelector, storage: &Storage) -> Result<Vec<Instant>> {
        let files = storage.list_files(Some(".hoodie")).await?;

        // For most cases, we load completed instants, so we can pre-allocate the vector with a
        // capacity of 1/3 of the total number of listed files,
        // ignoring requested and inflight instants.
        let mut instants = Vec::with_capacity(files.len() / 3);

        for file_info in files {
            match selector.try_create_instant(file_info.name.as_str()) {
                Ok(instant) => instants.push(instant),
                Err(e) => {
                    // Ignore files that are not valid or desired instants.
                    debug!(
                        "Instant not created from file {:?} due to: {:?}",
                        file_info, e
                    );
                }
            }
        }

        instants.sort_unstable();

        // As of current impl., we don't mutate instants once timeline is created,
        // so we can save some memory by shrinking the capacity.
        instants.shrink_to_fit();

        Ok(instants)
    }

    pub fn get_latest_commit_timestamp(&self) -> Option<&str> {
        self.instants
            .iter()
            .next_back()
            .map(|instant| instant.timestamp.as_str())
    }

    async fn get_commit_metadata(&self, instant: &Instant) -> Result<Map<String, Value>> {
        let bytes = self
            .storage
            .get_file_data(instant.relative_path()?.as_str())
            .await?;
        let err_msg = format!("Failed to get commit metadata for {:?}", instant);
        let json: Value = serde_json::from_slice(&bytes)
            .map_err(|e| CoreError::Timeline(format!("{}: {:?}", err_msg, e)))?;
        let commit_metadata = json
            .as_object()
            .ok_or(CoreError::Timeline(format!(
                "{}: not a JSON object",
                err_msg
            )))?
            .clone();
        Ok(commit_metadata)
    }

    async fn get_latest_commit_metadata(&self) -> Result<Map<String, Value>> {
        match self.instants.iter().next_back() {
            Some(instant) => self.get_commit_metadata(instant).await,
            None => Ok(Map::new()),
        }
    }

    pub async fn get_latest_schema(&self) -> Result<Schema> {
        let commit_metadata = self.get_latest_commit_metadata().await?;

        let parquet_path = commit_metadata
            .get("partitionToWriteStats")
            .and_then(|v| v.as_object())
            .and_then(|obj| obj.values().next())
            .and_then(|value| value.as_array())
            .and_then(|arr| arr.first())
            .and_then(|first_value| first_value["path"].as_str());

        if let Some(path) = parquet_path {
            let parquet_meta = self.storage.get_parquet_file_metadata(path).await?;

            Ok(parquet_to_arrow_schema(
                parquet_meta.file_metadata().schema_descr(),
                None,
            )?)
        } else {
            Err(CoreError::Timeline(
                "Failed to resolve the latest schema: no file path found".to_string(),
            ))
        }
    }

    pub async fn get_replaced_file_groups(&self) -> Result<HashSet<FileGroup>> {
        let mut fgs: HashSet<FileGroup> = HashSet::new();
        for instant in self.instants.iter().filter(|i| i.is_replacecommit()) {
            let commit_metadata = self.get_commit_metadata(instant).await?;
            if let Some(ptn_to_replaced) = commit_metadata.get("partitionToReplaceFileIds") {
                for (ptn, fg_ids) in ptn_to_replaced
                    .as_object()
                    .expect("partitionToReplaceFileIds should be a map")
                {
                    let fg_ids = fg_ids
                        .as_array()
                        .expect("file group ids should be an array")
                        .iter()
                        .map(|fg_id| fg_id.as_str().expect("file group id should be a string"));

                    let ptn = Some(ptn.to_string()).filter(|s| !s.is_empty());

                    for fg_id in fg_ids {
                        fgs.insert(FileGroup::new(fg_id.to_string(), ptn.clone()));
                    }
                }
            }
        }

        // TODO: return file group and instants, and handle multi-writer fg id conflicts

        Ok(fgs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs::canonicalize;
    use std::path::Path;
    use std::sync::Arc;

    use url::Url;

    use hudi_tests::TestTable;

    use crate::config::table::HudiTableConfig;

    async fn create_test_timeline(base_url: Url) -> Timeline {
        Timeline::new_from_storage(
            Arc::new(HudiConfigs::new([(HudiTableConfig::BasePath, base_url)])),
            Arc::new(HashMap::new()),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn timeline_read_latest_schema() {
        let base_url = TestTable::V6Nonpartitioned.url();
        let timeline = create_test_timeline(base_url).await;
        let table_schema = timeline.get_latest_schema().await.unwrap();
        assert_eq!(table_schema.fields.len(), 21)
    }

    #[tokio::test]
    async fn timeline_read_latest_schema_from_empty_table() {
        let base_url = TestTable::V6Empty.url();
        let timeline = create_test_timeline(base_url).await;
        let table_schema = timeline.get_latest_schema().await;
        assert!(table_schema.is_err());
        assert_eq!(
            table_schema.err().unwrap().to_string(),
            "Timeline error: Failed to resolve the latest schema: no file path found"
        )
    }

    #[tokio::test]
    async fn init_commits_timeline() {
        let base_url = Url::from_file_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let timeline = create_test_timeline(base_url).await;
        assert_eq!(
            timeline.instants,
            vec![
                Instant::try_from("20240402123035233.commit").unwrap(),
                Instant::try_from("20240402144910683.commit").unwrap(),
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
        let instant = Instant::try_from("20240402123035233.commit").unwrap();

        // Test error when reading empty commit metadata file
        let result = timeline.get_commit_metadata(&instant).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CoreError::Timeline(_)));
        assert!(err.to_string().contains("Failed to get commit metadata"));

        let instant = Instant::try_from("20240402144910683.commit").unwrap();

        // Test error when reading a commit metadata file with invalid JSON
        let result = timeline.get_commit_metadata(&instant).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CoreError::Timeline(_)));
        assert!(err.to_string().contains("not a JSON object"));
    }
}
