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

//! Key generator implementations for transforming user filters to partition filters.

pub mod timestamp_based;

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig::{KeyGeneratorClass, KeyGeneratorType};
use crate::expr::filter::Filter;

/// Returns true if the table uses a timestamp-based key generator,
/// checking both `hoodie.table.keygenerator.class` and
/// `hoodie.table.keygenerator.type`.
pub fn is_timestamp_based_keygen(hudi_configs: &HudiConfigs) -> Result<bool> {
    let by_class = hudi_configs
        .try_get(KeyGeneratorClass)?
        .map(|v| {
            let s: String = v.into();
            s.contains("TimestampBasedKeyGenerator")
        })
        .unwrap_or(false);

    if by_class {
        return Ok(true);
    }

    Ok(hudi_configs
        .try_get(KeyGeneratorType)?
        .map(|v| {
            let s: String = v.into();
            let upper = s.to_uppercase();
            upper == "TIMESTAMP" || upper == "TIMESTAMP_AVRO"
        })
        .unwrap_or(false))
}

/// Trait for key generators that can transform user filters on data columns
/// to filters on partition path columns.
pub trait KeyGeneratorFilterTransformer {
    /// Returns the source field name that this key generator operates on.
    fn source_field(&self) -> &str;

    /// Transforms a filter on the source field to one or more filters on partition fields.
    fn transform_filter(&self, filter: &Filter) -> Result<Vec<Filter>>;
}
