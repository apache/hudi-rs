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
mod ordering;
pub mod record_batch_merger;

use crate::Result;
use crate::config::error;
use crate::config::error::ConfigError;
use crate::config::error::ConfigError::InvalidValue;
use crate::file_group::record_batches::RecordBatches;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::str::FromStr;
use strum_macros::AsRefStr;

/// Merges materialized data and delete batches into a single output batch.
///
/// Implementations are configured when constructed. Callers provide the union of
/// base-file and log-file data batches plus delete batches; the merger returns a
/// batch using [`RecordMerger::output_schema`]. Errors are returned for invalid
/// merge configuration, missing required fields, or Arrow compute failures.
pub trait RecordMerger: Send + Sync + std::fmt::Debug {
    /// Merges the provided record batches into one output batch.
    ///
    /// The input must contain batches compatible with this merger's output
    /// schema and configured Hudi merge strategy. Implementations apply deletes
    /// and choose the latest version for each record key according to their
    /// strategy-specific ordering rules.
    fn merge(&self, inputs: RecordBatches) -> Result<RecordBatch>;

    /// Returns the schema used by batches produced by this merger.
    fn output_schema(&self) -> &SchemaRef;
}

/// Config value for [crate::config::table::HudiTableConfig::RecordMergeStrategy].
#[derive(Clone, Debug, PartialEq, AsRefStr)]
pub enum RecordMergeStrategyValue {
    #[strum(serialize = "append_only")]
    AppendOnly,
    #[strum(serialize = "overwrite_with_latest")]
    OverwriteWithLatest,
}

impl FromStr for RecordMergeStrategyValue {
    type Err = ConfigError;

    fn from_str(s: &str) -> error::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "append_only" => Ok(Self::AppendOnly),
            "overwrite_with_latest" => Ok(Self::OverwriteWithLatest),
            v => Err(InvalidValue(v.to_string())),
        }
    }
}
