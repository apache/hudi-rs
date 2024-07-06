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
use std::str::FromStr;

use anyhow::{anyhow, Result};
use strum_macros::EnumIter;

use crate::config::{ConfigParser, HudiConfigValue};

#[derive(Clone, Debug, PartialEq, Eq, Hash, EnumIter)]
pub enum HudiInternalConfig {
    SkipConfigValidation,
}

impl AsRef<str> for HudiInternalConfig {
    fn as_ref(&self) -> &str {
        match self {
            Self::SkipConfigValidation => "hoodie.internal.skip.config.validation",
        }
    }
}

impl ConfigParser for HudiInternalConfig {
    type Output = HudiConfigValue;

    fn default_value(&self) -> Option<HudiConfigValue> {
        match self {
            Self::SkipConfigValidation => Some(HudiConfigValue::Boolean(false)),
        }
    }

    fn parse_value(&self, configs: &HashMap<String, String>) -> Result<Self::Output> {
        let get_result = configs
            .get(self.as_ref())
            .map(|v| v.as_str())
            .ok_or(anyhow!("Config '{}' not found", self.as_ref()));

        match self {
            Self::SkipConfigValidation => get_result
                .and_then(|v| bool::from_str(v).map_err(|e| anyhow!(e)))
                .map(HudiConfigValue::Boolean),
        }
    }
}
