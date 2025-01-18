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
pub mod record_merger;

use crate::config::error;
use crate::config::error::ConfigError;
use crate::config::error::ConfigError::InvalidValue;
use std::str::FromStr;
use strum_macros::AsRefStr;

/// Config value for [crate::config::table::HudiTableConfig::RecordMergeStrategy].
#[derive(Clone, Debug, PartialEq, AsRefStr)]
pub enum RecordMergeStrategyValue {
    #[strum(serialize = "append_only")]
    AppendOnly,
    #[strum(serialize = "overwrite_with_latest")]
    OverwriteWithLatest,
}

impl Default for RecordMergeStrategyValue {
    fn default() -> Self {
        Self::OverwriteWithLatest
    }
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
