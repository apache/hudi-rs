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
//! Hudi plan configurations.

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

use strum_macros::{EnumIter, IntoStaticStr};

use crate::config::Result;
use crate::config::error::ConfigError::{NotFound, ParseInt};
use crate::config::{ConfigParser, HudiConfigValue};

/// Configurations for query planning in Hudi.
///
/// These control how the engine plans and executes operations like file listing.
/// Unlike [`super::table::HudiTableConfig`], these are not persisted in `hoodie.properties`.
/// Unlike [`super::read::HudiReadConfig`], these are not per-query read semantics.
///
/// **Example**
///
/// ```rust
/// use hudi_core::config::plan::HudiPlanConfig;
/// use hudi_core::table::Table as HudiTable;
///
/// # #[tokio::main]
/// # async fn main() {
/// let options = [(HudiPlanConfig::ListingParallelism, "20")];
/// HudiTable::new_with_options("/tmp/hudi_data", options).await;
/// # }
/// ```
///
#[derive(Clone, Debug, PartialEq, Eq, Hash, EnumIter, IntoStaticStr)]
pub enum HudiPlanConfig {
    /// Parallelism for listing files on storage.
    ListingParallelism,
}

impl HudiPlanConfig {
    pub const fn key_str(&self) -> &'static str {
        match self {
            Self::ListingParallelism => "hoodie.plan.listing.parallelism",
        }
    }
}

impl AsRef<str> for HudiPlanConfig {
    fn as_ref(&self) -> &str {
        self.key_str()
    }
}

impl Display for HudiPlanConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl ConfigParser for HudiPlanConfig {
    type Output = HudiConfigValue;

    fn default_value(&self) -> Option<HudiConfigValue> {
        match self {
            Self::ListingParallelism => Some(HudiConfigValue::UInteger(10usize)),
        }
    }

    fn parse_value(&self, configs: &HashMap<String, String>) -> Result<Self::Output> {
        let get_result = configs
            .get(self.as_ref())
            .map(|v| v.as_str())
            .ok_or(NotFound(self.key()));

        match self {
            Self::ListingParallelism => get_result
                .and_then(|v| {
                    usize::from_str(v).map_err(|e| ParseInt(self.key(), v.to_string(), e))
                })
                .map(HudiConfigValue::UInteger),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::plan::HudiPlanConfig::ListingParallelism;

    #[test]
    fn parse_valid_config_value() {
        let options = HashMap::from([(ListingParallelism.as_ref().to_string(), "20".to_string())]);
        let actual: usize = ListingParallelism.parse_value(&options).unwrap().into();
        assert_eq!(actual, 20);
    }

    #[test]
    fn parse_invalid_config_value() {
        let options =
            HashMap::from([(ListingParallelism.as_ref().to_string(), "_100".to_string())]);
        assert!(matches!(
            ListingParallelism.parse_value(&options).unwrap_err(),
            ParseInt(_, _, _)
        ));
        let actual: usize = ListingParallelism.parse_value_or_default(&options).into();
        assert_eq!(actual, 10);
    }

    #[test]
    fn default_value() {
        let empty = HashMap::new();
        let actual: usize = ListingParallelism.parse_value_or_default(&empty).into();
        assert_eq!(actual, 10);
    }

    #[test]
    fn display_matches_key() {
        assert_eq!(
            format!("{ListingParallelism}"),
            "hoodie.plan.listing.parallelism"
        );
    }
}
