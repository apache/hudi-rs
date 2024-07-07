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

use crate::config::{ConfigParser, HudiConfigValue};
use anyhow::{anyhow, Result};
use strum_macros::EnumIter;

#[derive(Clone, Debug, PartialEq, Eq, Hash, EnumIter)]
pub enum HudiReadConfig {
    InputPartitions,
    AsOfTimestamp,
}

impl AsRef<str> for HudiReadConfig {
    fn as_ref(&self) -> &str {
        match self {
            Self::InputPartitions => "hoodie.read.input.partitions",
            Self::AsOfTimestamp => "hoodie.read.as.of.timestamp",
        }
    }
}

impl ConfigParser for HudiReadConfig {
    type Output = HudiConfigValue;

    fn default_value(&self) -> Option<HudiConfigValue> {
        match self {
            HudiReadConfig::InputPartitions => Some(HudiConfigValue::UInteger(0usize)),
            _ => None,
        }
    }

    fn parse_value(&self, configs: &HashMap<String, String>) -> Result<Self::Output> {
        let get_result = configs
            .get(self.as_ref())
            .map(|v| v.as_str())
            .ok_or(anyhow!("Config '{}' not found", self.as_ref()));

        match self {
            Self::InputPartitions => get_result
                .and_then(|v| usize::from_str(v).map_err(|e| anyhow!(e)))
                .map(HudiConfigValue::UInteger),
            Self::AsOfTimestamp => get_result.map(|v| HudiConfigValue::String(v.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::read::HudiReadConfig::InputPartitions;
    use crate::config::ConfigParser;
    use std::collections::HashMap;
    use std::num::ParseIntError;

    #[test]
    fn parse_valid_config_value() {
        let options = HashMap::from([(InputPartitions.as_ref().to_string(), "100".to_string())]);
        let value = InputPartitions.parse_value(&options).unwrap().to::<usize>();
        assert_eq!(value, 100usize);
    }

    #[test]
    fn parse_invalid_config_value() {
        let options = HashMap::from([(InputPartitions.as_ref().to_string(), "foo".to_string())]);
        let value = InputPartitions.parse_value(&options);
        assert!(value.err().unwrap().is::<ParseIntError>());
        assert_eq!(
            InputPartitions
                .parse_value_or_default(&options)
                .to::<usize>(),
            0
        );
    }
}
