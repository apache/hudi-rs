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
use std::str::FromStr;

use crate::config::error::ConfigError;
use crate::config::read::HudiReadConfig;
pub use crate::config::read::QueryType;

/// Options for all Hudi read APIs (snapshot, time-travel, incremental, etc).
///
/// `filters` and `projection` are typed because they don't have a Hudi config equivalent.
/// Every other knob — query type, timestamps, batch size, ad-hoc Hudi configs — is stored
/// in `hudi_options` under its `HudiReadConfig` key. Builder methods like
/// [`Self::with_query_type`], [`Self::with_as_of_timestamp`], [`Self::with_start_timestamp`],
/// [`Self::with_end_timestamp`], and [`Self::with_batch_size`] are convenience wrappers that
/// insert into `hudi_options` under the right key. Typed accessors
/// ([`Self::query_type`], [`Self::as_of_timestamp`], etc.) read from the bag.
///
/// One source of truth: the bag. Builders + accessors are the ergonomic API on top.
///
/// # Example
///
/// ```ignore
/// use hudi::table::{ReadOptions, QueryType};
///
/// let options = ReadOptions::new()
///     .with_filters([("city", "=", "san_francisco")])
///     .with_projection(["id", "name", "city"])
///     .with_batch_size(4096);
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
    /// Column filters. Each filter is a tuple of `(field, operator, value)` where
    /// `field` is any column name (partition or data).
    ///
    /// Filters drive both **pruning** and **row-level filtering**:
    /// - When the field is a **partition column**, the filter prunes whole partitions
    ///   for both snapshot and incremental queries.
    /// - When the field is a **data column** with column statistics (min/max) in the
    ///   metadata table, the filter prunes whole files **for snapshot queries only**.
    ///   Incremental file planning currently does partition pruning only — data-column
    ///   filters apply at the row-level mask but do not prune files.
    /// - All filters are applied as a row-level mask after reading, so callers
    ///   always get only rows that match regardless of the planning path.
    pub filters: Vec<(String, String, String)>,

    /// Column names to project (select). If None, all columns are read.
    pub projection: Option<Vec<String>>,

    /// Resolved bag of Hudi configs for this read. Populated by typed builders
    /// (e.g. [`Self::with_query_type`], [`Self::with_start_timestamp`]) and by
    /// [`Self::with_hudi_option`] for any keys that don't have a typed builder.
    /// Keys are `HudiReadConfig` strings (`hoodie.*`).
    pub hudi_options: HashMap<String, String>,
}

impl ReadOptions {
    /// Creates a new ReadOptions with default values.
    pub fn new() -> Self {
        Self::default()
    }

    // ---- typed builders (insert into hudi_options under the right key) ----

    /// Sets the query type. Stored as [`HudiReadConfig::QueryType`].
    pub fn with_query_type(mut self, query_type: QueryType) -> Self {
        self.hudi_options.insert(
            HudiReadConfig::QueryType.as_ref().to_string(),
            query_type.as_ref().to_string(),
        );
        self
    }

    /// Sets the as-of timestamp for snapshot/time-travel queries.
    /// Stored as [`HudiReadConfig::AsOfTimestamp`].
    pub fn with_as_of_timestamp<S: AsRef<str>>(mut self, timestamp: S) -> Self {
        self.hudi_options.insert(
            HudiReadConfig::AsOfTimestamp.as_ref().to_string(),
            timestamp.as_ref().to_string(),
        );
        self
    }

    /// Sets the lower-bound timestamp (exclusive) for incremental queries.
    /// Stored as [`HudiReadConfig::StartTimestamp`].
    pub fn with_start_timestamp<S: AsRef<str>>(mut self, timestamp: S) -> Self {
        self.hudi_options.insert(
            HudiReadConfig::StartTimestamp.as_ref().to_string(),
            timestamp.as_ref().to_string(),
        );
        self
    }

    /// Sets the upper-bound timestamp (inclusive) for incremental queries.
    /// Stored as [`HudiReadConfig::EndTimestamp`].
    pub fn with_end_timestamp<S: AsRef<str>>(mut self, timestamp: S) -> Self {
        self.hudi_options.insert(
            HudiReadConfig::EndTimestamp.as_ref().to_string(),
            timestamp.as_ref().to_string(),
        );
        self
    }

    /// Sets the target batch size (rows per batch).
    /// Stored as [`HudiReadConfig::StreamBatchSize`].
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.hudi_options.insert(
            HudiReadConfig::StreamBatchSize.as_ref().to_string(),
            size.to_string(),
        );
        self
    }

    /// Sets column filters.
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

    // ---- typed accessors (read from hudi_options) ----

    /// The query type (defaults to [`QueryType::Snapshot`] when unset). Errors if
    /// the stored string is not a recognized variant.
    pub fn query_type(&self) -> crate::Result<QueryType> {
        match self.hudi_options.get(HudiReadConfig::QueryType.as_ref()) {
            Some(s) => Ok(QueryType::from_str(s)?),
            None => Ok(QueryType::default()),
        }
    }

    /// The as-of timestamp for snapshot/time-travel queries, if set.
    pub fn as_of_timestamp(&self) -> Option<&str> {
        self.hudi_options
            .get(HudiReadConfig::AsOfTimestamp.as_ref())
            .map(|s| s.as_str())
    }

    /// The start timestamp (exclusive) for incremental queries, if set.
    pub fn start_timestamp(&self) -> Option<&str> {
        self.hudi_options
            .get(HudiReadConfig::StartTimestamp.as_ref())
            .map(|s| s.as_str())
    }

    /// The end timestamp (inclusive) for incremental queries, if set.
    pub fn end_timestamp(&self) -> Option<&str> {
        self.hudi_options
            .get(HudiReadConfig::EndTimestamp.as_ref())
            .map(|s| s.as_str())
    }

    /// The target batch size (rows per batch) for streaming reads, if set.
    /// Errors if the stored string is not a valid `usize` or if the value is `0`
    /// (a zero-row batch yields no batches at the parquet stream reader and is
    /// almost certainly a caller mistake).
    pub fn batch_size(&self) -> crate::Result<Option<usize>> {
        let key = HudiReadConfig::StreamBatchSize.as_ref();
        match self.hudi_options.get(key) {
            Some(s) => {
                let parsed = s
                    .parse::<usize>()
                    .map_err(|e| ConfigError::ParseInt(key.to_string(), s.clone(), e))?;
                if parsed == 0 {
                    return Err(
                        ConfigError::InvalidValue(format!("{key} must be > 0, got 0")).into(),
                    );
                }
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
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
    fn test_with_query_type_round_trip() -> crate::Result<()> {
        let snapshot = ReadOptions::new();
        assert_eq!(snapshot.query_type()?, QueryType::Snapshot);

        let incr = ReadOptions::new().with_query_type(QueryType::Incremental);
        assert_eq!(incr.query_type()?, QueryType::Incremental);
        assert_eq!(
            incr.hudi_options
                .get(HudiReadConfig::QueryType.as_ref())
                .map(String::as_str),
            Some("incremental")
        );
        Ok(())
    }

    #[test]
    fn test_with_timestamps_round_trip() -> crate::Result<()> {
        let opts = ReadOptions::new()
            .with_as_of_timestamp("20240101120000000")
            .with_start_timestamp("20240101000000000")
            .with_end_timestamp("20240201000000000");
        assert_eq!(opts.as_of_timestamp(), Some("20240101120000000"));
        assert_eq!(opts.start_timestamp(), Some("20240101000000000"));
        assert_eq!(opts.end_timestamp(), Some("20240201000000000"));
        Ok(())
    }

    #[test]
    fn test_with_batch_size_round_trip() -> crate::Result<()> {
        let opts = ReadOptions::new();
        assert_eq!(opts.batch_size()?, None);

        let opts = ReadOptions::new().with_batch_size(2048);
        assert_eq!(opts.batch_size()?, Some(2048));
        assert_eq!(
            opts.hudi_options
                .get(HudiReadConfig::StreamBatchSize.as_ref())
                .map(String::as_str),
            Some("2048")
        );

        let opts = ReadOptions::new()
            .with_hudi_option(HudiReadConfig::StreamBatchSize.as_ref(), "not_a_number");
        let err = opts.batch_size().unwrap_err();
        assert!(err.to_string().contains("not_a_number"));
        Ok(())
    }

    #[test]
    fn test_with_hudi_options() {
        let options = ReadOptions::new()
            .with_hudi_option("hoodie.read.use.read_optimized.mode", "true")
            .with_hudi_options([("a", "1"), ("b", "2")]);
        assert_eq!(
            options
                .hudi_options
                .get("hoodie.read.use.read_optimized.mode"),
            Some(&"true".to_string())
        );
        assert_eq!(options.hudi_options.get("a"), Some(&"1".to_string()));
        assert_eq!(options.hudi_options.get("b"), Some(&"2".to_string()));
    }

    #[test]
    fn test_query_type_from_str_invalid_errors() {
        let opts =
            ReadOptions::new().with_hudi_option(HudiReadConfig::QueryType.as_ref(), "garbage");
        let err = opts.query_type().unwrap_err();
        assert!(err.to_string().contains("garbage"));
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
        assert!(debug_str.contains("hudi_options"));
    }
}
