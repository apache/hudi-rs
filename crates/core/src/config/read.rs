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

use anyhow::{anyhow, Context};

use crate::config::{HudiConfigValue, OptionsParser};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum HudiReadConfig {
    InputPartitions,
}

impl HudiReadConfig {
    fn default_value(&self) -> Option<HudiConfigValue> {
        match self {
            Self::InputPartitions => Some(HudiConfigValue::Integer(0)),
        }
    }
}

impl AsRef<str> for HudiReadConfig {
    fn as_ref(&self) -> &str {
        match self {
            Self::InputPartitions => "hoodie.read.input.partitions",
        }
    }
}

impl OptionsParser for HudiReadConfig {
    type Output = HudiConfigValue;

    fn parse_value(&self, options: &HashMap<String, String>) -> anyhow::Result<Self::Output> {
        match self {
            HudiReadConfig::InputPartitions => options.get(self.as_ref()).map_or_else(
                || Err(anyhow!("Config '{}' not found", self.as_ref())),
                |v| {
                    v.parse::<isize>()
                        .map(HudiConfigValue::Integer)
                        .with_context(|| {
                            format!("Failed to parse '{}' for config '{}'", v, self.as_ref())
                        })
                },
            ),
        }
    }

    fn parse_value_or_default(&self, options: &HashMap<String, String>) -> Self::Output {
        self.parse_value(options).unwrap_or_else(|_| {
            self.default_value()
                .unwrap_or_else(|| panic!("No default value for config '{}'", self.as_ref()))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::config::read::HudiReadConfig::InputPartitions;
    use crate::config::OptionsParser;

    #[test]
    fn parse_valid_config_value() {
        let options = HashMap::from([(InputPartitions.as_ref().to_string(), "100".to_string())]);
        let value = InputPartitions
            .parse_value(&options)
            .unwrap()
            .cast::<usize>();
        assert_eq!(value, 100usize);
    }

    #[test]
    fn parse_invalid_config_value() {
        let options = HashMap::from([(InputPartitions.as_ref().to_string(), "foo".to_string())]);
        let value = InputPartitions.parse_value(&options);
        assert_eq!(
            value.err().unwrap().to_string(),
            format!(
                "Failed to parse 'foo' for config '{}'",
                InputPartitions.as_ref()
            )
        );
        assert_eq!(
            InputPartitions
                .parse_value_or_default(&options)
                .cast::<isize>(),
            0
        );
    }
}
