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

use hudi_fs::file_name_without_ext;
use std::collections::HashMap;

use arrow_schema::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::{fs, io};

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
    pub base_path: PathBuf,
    pub instants: Vec<Instant>,
}

impl Timeline {
    pub fn new(base_path: &Path) -> Result<Self, io::Error> {
        let instants = Self::load_completed_commit_instants(base_path)?;
        Ok(Self {
            base_path: base_path.to_path_buf(),
            instants,
        })
    }

    fn load_completed_commit_instants(base_path: &Path) -> Result<Vec<Instant>, io::Error> {
        let mut completed_commits = Vec::new();
        let mut timeline_path = base_path.to_path_buf();
        timeline_path.push(".hoodie");
        for entry in fs::read_dir(timeline_path)? {
            let p = entry?.path();
            if p.is_file() && p.extension().and_then(|e| e.to_str()) == Some("commit") {
                completed_commits.push(Instant {
                    state: State::Completed,
                    timestamp: file_name_without_ext(p.file_name()),
                    action: "commit".to_owned(),
                })
            }
        }
        // TODO: encapsulate sorting within Instant
        completed_commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        Ok(completed_commits)
    }

    pub fn get_latest_commit_metadata(&self) -> Result<HashMap<String, Value>, io::Error> {
        match self.instants.iter().next_back() {
            Some(instant) => {
                let mut latest_instant_file_path = self.base_path.to_path_buf();
                latest_instant_file_path.push(".hoodie");
                latest_instant_file_path.push(instant.file_name());
                let mut f = File::open(latest_instant_file_path)?;
                let mut content = String::new();
                f.read_to_string(&mut content)?;
                let commit_metadata = serde_json::from_str(&content)?;
                Ok(commit_metadata)
            }
            None => Ok(HashMap::new()),
        }
    }

    pub fn get_latest_schema(&self) -> Result<SchemaRef, io::Error> {
        let commit_metadata = self.get_latest_commit_metadata()?;
        if let Some(partition_to_write_stats) = commit_metadata["partitionToWriteStats"].as_object()
        {
            if let Some((_, value)) = partition_to_write_stats.iter().next() {
                if let Some(first_value) = value.as_array().and_then(|arr| arr.first()) {
                    if let Some(path) = first_value["path"].as_str() {
                        let mut base_file_path = PathBuf::from(&self.base_path);
                        base_file_path.push(path);
                        let file = File::open(base_file_path)?;
                        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
                        return Ok(builder.schema().to_owned());
                    }
                }
            }
        }
        Err(io::Error::new(
            ErrorKind::InvalidData,
            "Failed to resolve schema.",
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::timeline::{Instant, State, Timeline};
    use hudi_fs::test_utils::extract_test_table;
    use std::path::Path;

    #[test]
    fn read_latest_schema() {
        let fixture_path = Path::new("fixtures/table/0.x_cow_partitioned.zip");
        let target_table_path = extract_test_table(fixture_path);
        let timeline = Timeline::new(target_table_path.as_path()).unwrap();
        let table_schema = timeline.get_latest_schema().unwrap();
        assert_eq!(table_schema.fields.len(), 11)
    }

    #[test]
    fn init_commits_timeline() {
        let fixture_path = Path::new("fixtures/timeline/commits_stub");
        let timeline = Timeline::new(fixture_path).unwrap();
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
