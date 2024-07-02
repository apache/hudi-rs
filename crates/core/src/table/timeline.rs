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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow_schema::Schema;
use parquet::arrow::parquet_to_arrow_schema;
use serde_json::{Map, Value};
use url::Url;

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
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct Timeline {
    props: Arc<HashMap<String, String>>,
    storage: Arc<Storage>,
    pub instants: Vec<Instant>,
}

impl Timeline {
    pub async fn new(
        base_url: Arc<Url>,
        storage_options: Arc<HashMap<String, String>>,
        props: Arc<HashMap<String, String>>,
    ) -> Result<Self> {
        let storage = Storage::new(base_url, storage_options)?;
        let instants = Self::load_completed_commit_instants(&storage).await?;
        Ok(Self {
            storage,
            props,
            instants,
        })
    }

    async fn load_completed_commit_instants(storage: &Storage) -> Result<Vec<Instant>> {
        let mut completed_commits = Vec::new();
        for file_info in storage.list_files(Some(".hoodie")).await {
            let (file_stem, file_ext) = split_filename(file_info.name.as_str())?;
            if file_ext == "commit" {
                completed_commits.push(Instant {
                    state: State::Completed,
                    timestamp: file_stem,
                    action: "commit".to_owned(),
                })
            }
        }
        completed_commits.sort();
        Ok(completed_commits)
    }

    async fn get_latest_commit_metadata(&self) -> Result<Map<String, Value>> {
        match self.instants.iter().next_back() {
            Some(instant) => {
                let mut commit_file_path = PathBuf::from(".hoodie");
                commit_file_path.push(instant.file_name());
                let relative_path = commit_file_path.to_str().ok_or(anyhow!(
                    "Failed to get commit file path for instant: {:?}",
                    instant
                ));
                let bytes = self.storage.get_file_data(relative_path?).await;
                let json: Value = serde_json::from_slice(&bytes)?;
                let commit_metadata = json
                    .as_object()
                    .ok_or_else(|| anyhow!("Expected JSON object"))?
                    .clone();
                Ok(commit_metadata)
            }
            None => Ok(Map::new()),
        }
    }

    pub async fn get_latest_schema(&self) -> Result<Schema> {
        let commit_metadata = self.get_latest_commit_metadata().await?;
        if let Some(partition_to_write_stats) = commit_metadata["partitionToWriteStats"].as_object()
        {
            if let Some((_, value)) = partition_to_write_stats.iter().next() {
                if let Some(first_value) = value.as_array().and_then(|arr| arr.first()) {
                    if let Some(path) = first_value["path"].as_str() {
                        let parquet_meta = self.storage.get_parquet_file_metadata(path).await;
                        let arrow_schema = parquet_to_arrow_schema(
                            parquet_meta.file_metadata().schema_descr(),
                            None,
                        )?;
                        return Ok(arrow_schema);
                    }
                }
            }
        }
        Err(anyhow!("Failed to resolve schema."))
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

    use crate::table::timeline::{Instant, State, Timeline};

    #[tokio::test]
    async fn read_latest_schema() {
        let base_url = TestTable::V6Nonpartitioned.url();
        let timeline = Timeline::new(
            Arc::new(base_url),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
        )
        .await
        .unwrap();
        let table_schema = timeline.get_latest_schema().await.unwrap();
        assert_eq!(table_schema.fields.len(), 21)
    }

    #[tokio::test]
    async fn init_commits_timeline() {
        let base_url =
            Url::from_file_path(canonicalize(Path::new("fixtures/timeline/commits_stub")).unwrap())
                .unwrap();
        let timeline = Timeline::new(
            Arc::new(base_url),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
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
