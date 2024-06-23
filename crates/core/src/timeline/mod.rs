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

use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use arrow_schema::SchemaRef;
use parquet::arrow::parquet_to_arrow_schema;
use serde_json::{Map, Value};

use crate::storage::file_metadata::split_filename;
use crate::storage::Storage;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum State {
    Requested,
    Inflight,
    Completed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Instant {
    state: State,
    action: String,
    timestamp: String,
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

#[derive(Debug, Clone)]
pub struct Timeline {
    pub base_path: String,
    pub instants: Vec<Instant>,
}

impl Timeline {
    pub async fn new(base_path: &str) -> Result<Self> {
        let instants = Self::load_completed_commit_instants(base_path).await?;
        Ok(Self {
            base_path: base_path.to_string(),
            instants,
        })
    }

    async fn load_completed_commit_instants(base_path: &str) -> Result<Vec<Instant>> {
        let storage = Storage::new(base_path, HashMap::new());
        let mut completed_commits = Vec::new();
        for file_metadata in storage.list_files(Some(".hoodie")).await {
            let (file_stem, file_ext) = split_filename(file_metadata.name.as_str())?;
            if file_ext == "commit" {
                completed_commits.push(Instant {
                    state: State::Completed,
                    timestamp: file_stem,
                    action: "commit".to_owned(),
                })
            }
        }
        // TODO: encapsulate sorting within Instant
        completed_commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        Ok(completed_commits)
    }

    async fn get_latest_commit_metadata(&self) -> Result<Map<String, Value>> {
        match self.instants.iter().next_back() {
            Some(instant) => {
                let mut commit_file_path = PathBuf::from(".hoodie");
                commit_file_path.push(instant.file_name());
                let storage = Storage::new(&self.base_path, HashMap::new());
                let bytes = storage
                    .get_file_data(commit_file_path.to_str().unwrap())
                    .await;
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

    pub async fn get_latest_schema(&self) -> Result<SchemaRef> {
        let commit_metadata = self.get_latest_commit_metadata().await.unwrap();
        if let Some(partition_to_write_stats) = commit_metadata["partitionToWriteStats"].as_object()
        {
            if let Some((_, value)) = partition_to_write_stats.iter().next() {
                if let Some(first_value) = value.as_array().and_then(|arr| arr.first()) {
                    if let Some(path) = first_value["path"].as_str() {
                        let storage = Storage::new(&self.base_path, HashMap::new());
                        let parquet_meta = storage.get_parquet_file_metadata(path).await;
                        let arrow_schema = parquet_to_arrow_schema(
                            parquet_meta.file_metadata().schema_descr(),
                            None,
                        )?;
                        return Ok(SchemaRef::from(arrow_schema));
                    }
                }
            }
        }
        Err(anyhow!("Failed to resolve schema."))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::canonicalize;
    use std::path::Path;

    use crate::test_utils::extract_test_table;
    use crate::timeline::{Instant, State, Timeline};

    #[tokio::test]
    async fn read_latest_schema() {
        let fixture_path = Path::new("fixtures/table/0.x_cow_partitioned.zip");
        let target_table_path = extract_test_table(fixture_path);
        let base_path = canonicalize(target_table_path).unwrap();
        let timeline = Timeline::new(base_path.to_str().unwrap()).await.unwrap();
        let table_schema = timeline.get_latest_schema().await.unwrap();
        assert_eq!(table_schema.fields.len(), 11)
    }

    #[tokio::test]
    async fn init_commits_timeline() {
        let base_path = canonicalize(Path::new("fixtures/timeline/commits_stub")).unwrap();
        let timeline = Timeline::new(base_path.to_str().unwrap()).await.unwrap();
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
