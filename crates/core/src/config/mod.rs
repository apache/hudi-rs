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
//! Hudi Configurations.
use std::any::type_name;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};

pub mod internal;
pub mod read;
pub mod table;

pub const HUDI_CONF_DIR: &str = "HUDI_CONF_DIR";

/// This defines some common APIs for working with configurations in Hudi.
pub trait ConfigParser: AsRef<str> {
    /// Configuration value type.
    type Output;

    /// Supplies the default value of the configuration.
    fn default_value(&self) -> Option<Self::Output>;

    /// To indicate if the configuration is required or not, this helps in validation.
    fn is_required(&self) -> bool {
        false
    }

    /// Validate the configuration by parsing the given [String] value and check if it is required.
    fn validate(&self, configs: &HashMap<String, String>) -> Result<()> {
        match self.parse_value(configs) {
            Ok(_) => Ok(()),
            Err(e) => {
                if !self.is_required() && e.to_string().ends_with("not found") {
                    // TODO: introduce error type to avoid checking "not found"
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Parse the [String] value to [Self::Output].
    fn parse_value(&self, configs: &HashMap<String, String>) -> Result<Self::Output>;

    /// Parse the [String] value to [Self::Output], or return the default value.
    ///
    /// Panic if the default value is not defined.
    fn parse_value_or_default(&self, configs: &HashMap<String, String>) -> Self::Output {
        self.parse_value(configs).unwrap_or_else(|_| {
            self.default_value()
                .unwrap_or_else(|| panic!("No default value for config '{}'", self.as_ref()))
        })
    }
}

/// All possible data types for Hudi Configuration values.
#[derive(Clone, Debug)]
pub enum HudiConfigValue {
    Boolean(bool),
    Integer(isize),
    UInteger(usize),
    String(String),
    List(Vec<String>),
}

impl HudiConfigValue {
    /// Covert [HudiConfigValue] logical type to the representing data type in Rust.
    ///
    /// - [`HudiConfigValue::Boolean`] -> [bool]
    /// - [`HudiConfigValue::Integer`] -> [isize]
    /// - [`HudiConfigValue::UInteger`] -> [usize]
    /// - [`HudiConfigValue::String`] -> [String]
    /// - [`HudiConfigValue::List`] -> [`Vec<String>`]
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

/// Hudi configuration container.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HudiConfigs {
    pub raw_configs: Arc<HashMap<String, String>>,
}

impl HudiConfigs {
    /// Create [HudiConfigs] with key-value pairs of [String]s.
    pub fn new(raw_configs: HashMap<String, String>) -> Self {
        Self {
            raw_configs: Arc::new(raw_configs),
        }
    }

    /// Create empty [HudiConfigs].
    pub fn empty() -> Self {
        Self {
            raw_configs: Arc::new(HashMap::new()),
        }
    }

    pub fn validate(&self, parser: impl ConfigParser<Output = HudiConfigValue>) -> Result<()> {
        parser.validate(&self.raw_configs)
    }

    pub fn get(
        &self,
        parser: impl ConfigParser<Output = HudiConfigValue>,
    ) -> Result<HudiConfigValue> {
        parser.parse_value(&self.raw_configs)
    }

    /// Get value or default value. If the config has no default value, this will panic.
    pub fn get_or_default(
        &self,
        parser: impl ConfigParser<Output = HudiConfigValue>,
    ) -> HudiConfigValue {
        parser.parse_value_or_default(&self.raw_configs)
    }

    /// Get value or default value. If the config has no default value, this will return [None].
    pub fn try_get(
        &self,
        parser: impl ConfigParser<Output = HudiConfigValue>,
    ) -> Option<HudiConfigValue> {
        let res = parser.parse_value(&self.raw_configs);
        match res {
            Ok(v) => Some(v),
            Err(_) => parser.default_value(),
        }
    }
}
