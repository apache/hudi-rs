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

use std::cmp::{Ordering, PartialOrd};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow_schema::Schema;
use parquet::arrow::parquet_to_arrow_schema;
use serde_json::{Map, Value};
use url::Url;

use crate::config::HudiConfigs;
use crate::file_group::FileGroup;
use crate::storage::utils::split_filename;
use crate::storage::Storage;

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum State {
    Requested,
    Inflight,
    Completed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Instant {
    state: State,
    action: String,
    timestamp: String,
}

impl PartialOrd for Instant {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.timestamp.cmp(&other.timestamp))
    }
}

impl Ord for Instant {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl Instant {
    pub fn state_suffix(&self) -> String {
        match self.state {
            State::Requested => ".requested".to_owned(),
            State::Inflight => ".inflight".to_owned(),
            State::Completed => "".to_owned(),
        }
    }

    pub fn file_name(&self) -> String {
        format!("{}.{}{}", self.timestamp, self.action, self.state_suffix())
    }

    pub fn relative_path(&self) -> Result<String> {
        let mut commit_file_path = PathBuf::from(".hoodie");
        commit_file_path.push(self.file_name());
        commit_file_path
            .to_str()
            .ok_or(anyhow!("Failed to get file path for {:?}", self))
            .map(|s| s.to_string())
    }

    pub fn is_replacecommit(&self) -> bool {
        self.action == "replacecommit"
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct Timeline {
    configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
    pub instants: Vec<Instant>,
}

impl Timeline {
    pub async fn new(
        base_url: Arc<Url>,
        storage_options: Arc<HashMap<String, String>>,
        configs: Arc<HudiConfigs>,
    ) -> Result<Self> {
        let storage = Storage::new(base_url, &storage_options)?;
        let instants = Self::load_completed_commits(&storage).await?;
        Ok(Self {
            storage,
            configs,
            instants,
        })
    }

    async fn load_completed_commits(storage: &Storage) -> Result<Vec<Instant>> {
        let mut completed_commits = Vec::new();
        for file_info in storage.list_files(Some(".hoodie")).await? {
            let (file_stem, file_ext) = split_filename(file_info.name.as_str())?;
            if matches!(file_ext.as_str(), "commit" | "replacecommit") {
                completed_commits.push(Instant {
                    state: State::Completed,
                    timestamp: file_stem,
                    action: file_ext.to_owned(),
                })
            }
        }
        completed_commits.sort();
        Ok(completed_commits)
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
        let json: Value = serde_json::from_slice(&bytes)?;
        let commit_metadata = json
            .as_object()
            .ok_or_else(|| anyhow!("Expected JSON object"))?
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
            let parquet_meta = self
                .storage
                .get_parquet_file_metadata(path)
                .await
                .context("Failed to get parquet file metadata")?;

            parquet_to_arrow_schema(parquet_meta.file_metadata().schema_descr(), None)
                .context("Failed to resolve the latest schema")
        } else {
            Err(anyhow!(
                "Failed to resolve the latest schema: no file path found"
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
    use std::collections::HashMap;
    use std::fs::canonicalize;
    use std::path::Path;
    use std::sync::Arc;

    use url::Url;

    use hudi_tests::TestTable;

    use crate::config::HudiConfigs;
    use crate::table::timeline::{Instant, State, Timeline};

    #[tokio::test]
    async fn timeline_read_latest_schema() {
        let base_url = TestTable::V6Nonpartitioned.url();
        let timeline = Timeline::new(
            Arc::new(base_url),
            Arc::new(HashMap::new()),
            Arc::new(HudiConfigs::empty()),
        )
        .await
        .unwrap();
        let table_schema = timeline.get_latest_schema().await.unwrap();
        assert_eq!(table_schema.fields.len(), 21)
    }

    #[tokio::test]
    async fn timeline_read_latest_schema_from_empty_table() {
        let base_url = TestTable::V6Empty.url();
        let timeline = Timeline::new(
            Arc::new(base_url),
            Arc::new(HashMap::new()),
            Arc::new(HudiConfigs::empty()),
        )
        .await
        .unwrap();
        let table_schema = timeline.get_latest_schema().await;
        assert!(table_schema.is_err());
        assert_eq!(
            table_schema.err().unwrap().to_string(),
            "Failed to resolve the latest schema: no file path found"
        )
    }

    #[tokio::test]
    async fn init_commits_timeline() {
        let base_url = Url::from_file_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let timeline = Timeline::new(
            Arc::new(base_url),
            Arc::new(HashMap::new()),
            Arc::new(HudiConfigs::empty()),
        )
        .await
        .unwrap();
        assert_eq!(
            timeline.instants,
            vec![
                Instant {
                    state: State::Completed,
                    action: "commit".to_owned(),
                    timestamp: "20240402123035233".to_owned(),
                },
                Instant {
                    state: State::Completed,
                    action: "commit".to_owned(),
                    timestamp: "20240402144910683".to_owned(),
                },
            ]
        )
    }
}
