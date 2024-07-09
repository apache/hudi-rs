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
use std::any::type_name;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

pub mod internal;
pub mod read;
pub mod table;

#[derive(Debug)]
pub enum ConfigError {
    NotFound,
    ParseError(String),
    Other(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::NotFound => write!(f, "Configuration not found"),
            ConfigError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            ConfigError::Other(msg) => write!(f, "Other error: {}", msg),
        }
    }
}

impl Error for ConfigError {}

pub trait ConfigParser: AsRef<str> {
    type Output;

    fn default_value(&self) -> Option<Self::Output>;

    fn is_required(&self) -> bool {
        false
    }

    fn validate(&self, configs: &HashMap<String, String>) -> Result<(), ConfigError> {
        match self.parse_value(configs) {
            Ok(_) => Ok(()),
            Err(e) => {
                if !self.is_required() && matches!(e, ConfigError::NotFound) {
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    fn parse_value(&self, configs: &HashMap<String, String>) -> Result<Self::Output, ConfigError>;

    fn parse_value_or_default(&self, configs: &HashMap<String, String>) -> Self::Output {
        self.parse_value(configs).unwrap_or_else(|_| {
            self.default_value()
                .unwrap_or_else(|| panic!("No default value for config '{}'", self.as_ref()))
        })
    }
}

#[derive(Clone, Debug)]
pub enum HudiConfigValue {
    Boolean(bool),
    Integer(isize),
    UInteger(usize),
    String(String),
    List(Vec<String>),
}

impl HudiConfigValue {
    pub fn to<T: 'static + std::fmt::Debug + From<HudiConfigValue>>(self) -> T {
        T::from(self)
    }
}

impl From<HudiConfigValue> for bool {
    fn from(value: HudiConfigValue) -> Self {
        match value {
            HudiConfigValue::Boolean(v) => v,
            _ => panic!("Cannot cast {:?} to {}", value, type_name::<Self>()),
        }
    }
}

impl From<HudiConfigValue> for isize {
    fn from(value: HudiConfigValue) -> Self {
        match value {
            HudiConfigValue::Integer(v) => v,
            _ => panic!("Cannot cast {:?} to {}", value, type_name::<Self>()),
        }
    }
}

impl From<HudiConfigValue> for usize {
    fn from(value: HudiConfigValue) -> Self {
        match value {
            HudiConfigValue::UInteger(v) => v,
            _ => panic!("Cannot cast {:?} to {}", value, type_name::<Self>()),
        }
    }
}

impl From<HudiConfigValue> for String {
    fn from(value: HudiConfigValue) -> Self {
        match value {
            HudiConfigValue::Boolean(v) => v.to_string(),
            HudiConfigValue::Integer(v) => v.to_string(),
            HudiConfigValue::UInteger(v) => v.to_string(),
            HudiConfigValue::String(v) => v,
            _ => panic!("Cannot cast {:?} to {}", value, type_name::<Self>()),
        }
    }
}

impl From<HudiConfigValue> for Vec<String> {
    fn from(value: HudiConfigValue) -> Self {
        match value {
            HudiConfigValue::List(v) => v,
            _ => panic!("Cannot cast {:?} to {}", value, type_name::<Self>()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HudiConfigs {
    pub raw_configs: Arc<HashMap<String, String>>,
}

impl HudiConfigs {
    pub fn new(raw_configs: HashMap<String, String>) -> Self {
        Self {
            raw_configs: Arc::new(raw_configs),
        }
    }

    pub fn empty() -> Self {
        Self {
            raw_configs: Arc::new(HashMap::new()),
        }
    }

    pub fn validate(
        &self,
        parser: impl ConfigParser<Output = HudiConfigValue>,
    ) -> Result<(), ConfigError> {
        parser.validate(&self.raw_configs)
    }

    pub fn get(
        &self,
        parser: impl ConfigParser<Output = HudiConfigValue>,
    ) -> Result<HudiConfigValue, ConfigError> {
        parser.parse_value(&self.raw_configs)
    }

    pub fn get_or_default(
        &self,
        parser: impl ConfigParser<Output = HudiConfigValue>,
    ) -> HudiConfigValue {
        parser.parse_value_or_default(&self.raw_configs)
    }

    pub fn try_get(
        &self,
        parser: impl ConfigParser<Output = HudiConfigValue>,
    ) -> Option<HudiConfigValue> {
        match parser.parse_value(&self.raw_configs) {
            Ok(v) => Some(v),
            Err(_) => parser.default_value(),
        }
    }
}
