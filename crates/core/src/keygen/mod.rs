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

use crate::expr::filter::Filter;
use crate::Result;

/// Trait for key generators that can transform user filters on data columns
/// to filters on partition path columns.
pub trait KeyGeneratorFilterTransformer {
    /// Returns the source field name that this key generator operates on.
    /// For example, "event_timestamp" or "ts_str".
    fn source_field(&self) -> &str;

    /// Transforms a filter on the source field to one or more filters on partition fields.
    ///
    /// # Arguments
    /// * `filter` - The user-provided filter on the source data column
    ///
    /// # Returns
    /// A vector of filters on partition path fields that are equivalent to the input filter.
    ///
    /// # Example
    /// ```text
    /// Input:  Filter { field: "event_timestamp", op: ">=", value: "2024-01-25T00:00:00.000Z" }
    /// Output: [
    ///   Filter { field: "year", op: ">=", value: "2024" },
    ///   Filter { field: "month", op: ">=", value: "01" },
    ///   Filter { field: "day", op: ">=", value: "25" },
    /// ]
    /// ```
    fn transform_filter(&self, filter: &Filter) -> Result<Vec<Filter>>;
}
