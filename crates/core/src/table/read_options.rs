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
//! Read options shared by all read APIs (eager and streaming).

use std::collections::HashMap;

/// The query type a read targets. Drives dispatch in [`crate::table::Table::read`],
/// [`crate::table::Table::read_stream`], and [`crate::table::Table::get_file_slices`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QueryType {
    /// Latest table state at one commit (the latest by default; an explicit
    /// `as_of_timestamp` for time-travel).
    #[default]
    Snapshot,
    /// Records changed in the half-open range (`start_timestamp`, `end_timestamp`].
    Incremental,
}

/// Options for all Hudi read APIs (snapshot, time-travel, incremental, eager and streaming).
///
/// `query_type` selects the read semantic; the timestamp fields are interpreted by
/// the chosen type:
/// - `Snapshot` uses `as_of_timestamp` (defaulting to the latest commit when unset).
/// - `Incremental` uses `start_timestamp` and `end_timestamp` (defaulting to earliest
///   and latest respectively when unset).
///
/// All query types honor `filters`, `projection`, and `batch_size` where applicable.
/// `hudi_options` carries ad-hoc Hudi configs that override the table-level defaults
/// for this single read (for example, `hoodie.read.use.read_optimized.mode`).
///
/// # Example
///
/// ```ignore
/// use hudi::table::{ReadOptions, QueryType};
///
/// // Snapshot read with a column filter on the partition column
/// let options = ReadOptions::new()
///     .with_filters([("city", "=", "san_francisco")])
///     .with_projection(["id", "name", "city"]);
///
/// // Time-travel snapshot
/// let options = ReadOptions::new().with_as_of_timestamp("20240101000000000");
///
/// // Incremental read between two commits
/// let options = ReadOptions::new()
///     .with_query_type(QueryType::Incremental)
///     .with_start_timestamp("20240101000000000")
///     .with_end_timestamp("20240201000000000");
///
/// // Read-optimized snapshot (skip log files)
/// let options = ReadOptions::new()
///     .with_hudi_option("hoodie.read.use.read_optimized.mode", "true");
/// ```
#[derive(Clone, Debug, Default)]
pub struct ReadOptions {
    /// Selects the query semantic. Defaults to [`QueryType::Snapshot`].
    pub query_type: QueryType,

    /// Column filters. Each filter is a tuple of `(field, operator, value)` where
    /// `field` is any column name (partition or data).
    ///
    /// Filters drive both **pruning** and **row-level filtering**:
    /// - When the field is a **partition column**, the filter prunes whole partitions.
    /// - When the field is a **data column**, the filter prunes whole files via column
    ///   statistics (min/max) when available.
    /// - All filters are also applied as a row-level mask after reading, so callers
    ///   only get rows that match.
    pub filters: Vec<(String, String, String)>,

    /// Column names to project (select). If None, all columns are read.
    pub projection: Option<Vec<String>>,

    /// Target number of rows per batch.
    pub batch_size: Option<usize>,

    /// Timestamp for snapshot/time-travel queries. Ignored for other query types.
    pub as_of_timestamp: Option<String>,

    /// Lower-bound timestamp (exclusive) for incremental queries. Ignored otherwise.
    pub start_timestamp: Option<String>,

    /// Upper-bound timestamp (inclusive) for incremental queries. Ignored otherwise.
    pub end_timestamp: Option<String>,

    /// Per-read Hudi configs that override table-level defaults. Keys are
    /// `hoodie.*` config names. Useful for one-off mode toggles like
    /// `hoodie.read.use.read_optimized.mode = true` without mutating the table
    /// instance.
    pub hudi_options: HashMap<String, String>,
}

impl ReadOptions {
    /// Creates a new ReadOptions with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the query type.
    pub fn with_query_type(mut self, query_type: QueryType) -> Self {
        self.query_type = query_type;
        self
    }

    /// Sets column filters.
    ///
    /// Filters may target any column — partition or data. Depending on the column and
    /// available metadata they drive partition pruning, file-level stats pruning, and
    /// row-level filtering. See the field docs on [`ReadOptions::filters`].
    pub fn with_filters<I, S1, S2, S3>(mut self, filters: I) -> Self
    where
        I: IntoIterator<Item = (S1, S2, S3)>,
        S1: Into<String>,
        S2: Into<String>,
        S3: Into<String>,
    {
        self.filters = filters
            .into_iter()
            .map(|(f, o, v)| (f.into(), o.into(), v.into()))
            .collect();
        self
    }

    /// Sets the column projection (which columns to read).
    pub fn with_projection<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.projection = Some(columns.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Sets the target batch size (rows per batch).
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }

    /// Sets the as-of timestamp for snapshot/time-travel queries.
    pub fn with_as_of_timestamp<S: AsRef<str>>(mut self, timestamp: S) -> Self {
        self.as_of_timestamp = Some(timestamp.as_ref().to_string());
        self
    }

    /// Sets the lower-bound timestamp (exclusive) for incremental queries.
    pub fn with_start_timestamp<S: AsRef<str>>(mut self, timestamp: S) -> Self {
        self.start_timestamp = Some(timestamp.as_ref().to_string());
        self
    }

    /// Sets the upper-bound timestamp (inclusive) for incremental queries.
    pub fn with_end_timestamp<S: AsRef<str>>(mut self, timestamp: S) -> Self {
        self.end_timestamp = Some(timestamp.as_ref().to_string());
        self
    }

    /// Sets a single Hudi config that applies to this read only.
    pub fn with_hudi_option<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.hudi_options.insert(key.into(), value.into());
        self
    }

    /// Sets a batch of Hudi configs that apply to this read only.
    pub fn with_hudi_options<I, K, V>(mut self, opts: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (k, v) in opts {
            self.hudi_options.insert(k.into(), v.into());
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_projection() {
        let options = ReadOptions::new().with_projection(["col1", "col2", "col3"]);

        assert_eq!(
            options.projection,
            Some(vec![
                "col1".to_string(),
                "col2".to_string(),
                "col3".to_string()
            ])
        );
    }

    #[test]
    fn test_with_as_of_timestamp() {
        let options = ReadOptions::new().with_as_of_timestamp("20240101120000000");

        assert_eq!(
            options.as_of_timestamp,
            Some("20240101120000000".to_string())
        );
    }

    #[test]
    fn test_with_start_and_end_timestamp() {
        let options = ReadOptions::new()
            .with_start_timestamp("20240101000000000")
            .with_end_timestamp("20240201000000000");

        assert_eq!(
            options.start_timestamp,
            Some("20240101000000000".to_string())
        );
        assert_eq!(options.end_timestamp, Some("20240201000000000".to_string()));
    }

    #[test]
    fn test_with_query_type() {
        let snapshot = ReadOptions::new();
        assert_eq!(snapshot.query_type, QueryType::Snapshot);

        let incr = ReadOptions::new().with_query_type(QueryType::Incremental);
        assert_eq!(incr.query_type, QueryType::Incremental);
    }

    #[test]
    fn test_with_hudi_options() {
        let options = ReadOptions::new()
            .with_hudi_option("hoodie.read.use.read_optimized.mode", "true")
            .with_hudi_options([("a", "1"), ("b", "2")]);
        assert_eq!(
            options.hudi_options.get("hoodie.read.use.read_optimized.mode"),
            Some(&"true".to_string())
        );
        assert_eq!(options.hudi_options.get("a"), Some(&"1".to_string()));
        assert_eq!(options.hudi_options.get("b"), Some(&"2".to_string()));
    }

    #[test]
    fn test_debug_format() {
        let options = ReadOptions::new()
            .with_filters([("city", "=", "sf")])
            .with_projection(["id"])
            .with_batch_size(1000);

        let debug_str = format!("{options:?}");
        assert!(debug_str.contains("ReadOptions"));
        assert!(debug_str.contains("filters"));
        assert!(debug_str.contains("projection"));
        assert!(debug_str.contains("batch_size"));
        assert!(debug_str.contains("query_type"));
    }
}
