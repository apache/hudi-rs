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

use anyhow::{anyhow, Context, Result};

pub trait OptionsParser {
    type Output;

    fn parse_value(&self, options: &HashMap<String, String>) -> Result<Self::Output>;

    fn parse_value_or_default(&self, options: &HashMap<String, String>) -> Self::Output;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum HudiConfig {
    ReadInputPartitions,
}

#[derive(Debug)]
pub enum HudiConfigValue {
    Integer(isize),
}

impl HudiConfigValue {
    pub fn cast<T: 'static + TryFrom<isize> + TryFrom<usize> + std::fmt::Debug>(&self) -> T {
        match self {
            HudiConfigValue::Integer(value) => T::try_from(*value).unwrap_or_else(|_| {
                panic!("Failed to convert isize to {}", std::any::type_name::<T>())
            }),
        }
    }
}

impl HudiConfig {
    fn default_value(&self) -> Option<HudiConfigValue> {
        match self {
            Self::ReadInputPartitions => Some(HudiConfigValue::Integer(0)),
        }
    }
}

impl AsRef<str> for HudiConfig {
    fn as_ref(&self) -> &str {
        match self {
            Self::ReadInputPartitions => "hoodie.read.input.partitions",
        }
    }
}

impl OptionsParser for HudiConfig {
    type Output = HudiConfigValue;

    fn parse_value(&self, options: &HashMap<String, String>) -> Result<Self::Output> {
        match self {
            HudiConfig::ReadInputPartitions => options.get(self.as_ref()).map_or_else(
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
    use crate::config::HudiConfig::ReadInputPartitions;
    use crate::config::OptionsParser;
    use std::collections::HashMap;

    #[test]
    fn parse_invalid_config_value() {
        let options =
            HashMap::from([(ReadInputPartitions.as_ref().to_string(), "foo".to_string())]);
        let value = ReadInputPartitions.parse_value(&options);
        assert_eq!(
            value.err().unwrap().to_string(),
            format!(
                "Failed to parse 'foo' for config '{}'",
                ReadInputPartitions.as_ref()
            )
        );
        assert_eq!(
            ReadInputPartitions
                .parse_value_or_default(&options)
                .cast::<isize>(),
            0
        );
    }
}
