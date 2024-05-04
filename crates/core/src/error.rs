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

use std::error::Error;
use std::fmt::Debug;
use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum HudiFileGroupError {
    #[error("Base File {0} has unsupported format: {1}")]
    UnsupportedBaseFileFormat(String, String),
    #[error("Commit time {0} is already present in File Group {1}")]
    CommitTimeAlreadyExists(String, String),
}

#[derive(Debug, Error)]
pub enum HudiTimelineError {
    #[error("Error in reading commit metadata: {0}")]
    FailToReadCommitMetadata(io::Error),
}

#[derive(Debug, Error)]
pub enum HudiFileSystemViewError {
    #[error("Error in loading partitions: {0}")]
    FailToLoadPartitions(Box<dyn Error>),
}

#[derive(Debug, Error)]
pub enum HudiCoreError {
    #[error("Failed to load file group")]
    FailToLoadFileGroup(#[from] HudiFileGroupError),
    #[error("Failed to init timeline")]
    FailToInitTimeline(#[from] HudiTimelineError),
    #[error("Failed to build file system view")]
    FailToBuildFileSystemView(#[from] HudiFileSystemViewError),
}
