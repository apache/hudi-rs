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
//! Read options for configuring table read operations.

use crate::expr::filter::Filter;

/// Options for reading data from a Hudi table.
///
/// This struct provides a builder-style API for configuring various aspects
/// of table read operations, including time travel, incremental reads, partition
/// filtering, column projection, row-level predicates, and batch size control.
///
/// # Examples
///
/// ## Snapshot read with partition filter
/// ```ignore
/// use hudi_core::table::ReadOptions;
/// use hudi_core::expr::filter::col;
///
/// let options = ReadOptions::new()
///     .with_partition_filter(col("date").eq("2024-01-01"))
///     .with_column(&["id", "name", "value"])
///     .with_batch_size(4096);
///
/// let mut stream = table.read_snapshot(options).await?;
/// ```
///
/// ## Time-travel read
/// ```ignore
/// let options = ReadOptions::new()
///     .as_of("2024-01-01T12:00:00Z");
///
/// let mut stream = table.read_snapshot(options).await?;
/// ```
///
/// ## Incremental read
/// ```ignore
/// let options = ReadOptions::new()
///     .from_timestamp("20240101120000000")
///     .to_timestamp("20240102120000000");
///
/// let mut stream = table.read_incremental(options).await?;
/// ```
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    /// Timestamp for time-travel queries (snapshot reads).
    /// If `None`, reads the latest snapshot.
    /// For file slice reads, this controls log file reading "up to" this instant.
    pub as_of_timestamp: Option<String>,

    /// Start timestamp for incremental reads (exclusive).
    /// Only records committed after this timestamp will be returned.
    pub start_timestamp: Option<String>,

    /// End timestamp for incremental reads (inclusive).
    /// Only records committed before or at this timestamp will be returned.
    /// If `None`, reads up to the latest commit.
    pub end_timestamp: Option<String>,

    /// Partition filters for file slice pruning.
    /// These filters are applied during partition/file slice selection.
    pub partition_filter: Vec<Filter>,

    /// Column names to project. If `None`, all columns are read.
    /// Column projection is applied during parquet decoding for efficiency.
    pub projection: Option<Vec<String>>,

    /// Row-level filter predicate.
    /// This filter is converted to a parquet RowFilter and applied during decode.
    pub row_predicate: Option<Filter>,

    /// Target number of rows per batch for streaming reads.
    /// If `None`, uses the configured default (typically 8192).
    pub batch_size: Option<usize>,
}

impl ReadOptions {
    /// Creates a new `ReadOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the timestamp for time-travel queries (snapshot reads).
    ///
    /// When set, reads data as of this timestamp instead of the latest snapshot.
    /// For file slice reads, this also controls log file reading "up to" this instant.
    ///
    /// Note: This is for snapshot/time-travel reads. For incremental reads,
    /// use `from_timestamp` and `to_timestamp` instead.
    pub fn as_of(mut self, timestamp: impl Into<String>) -> Self {
        self.as_of_timestamp = Some(timestamp.into());
        self
    }

    /// Sets the start timestamp for incremental reads (exclusive).
    ///
    /// Only records committed after this timestamp will be returned.
    /// Use with `to_timestamp` to define the incremental read range.
    pub fn from_timestamp(mut self, timestamp: impl Into<String>) -> Self {
        self.start_timestamp = Some(timestamp.into());
        self
    }

    /// Sets the end timestamp for incremental reads (inclusive).
    ///
    /// Only records committed before or at this timestamp will be returned.
    /// If not set, reads up to the latest commit.
    pub fn to_timestamp(mut self, timestamp: impl Into<String>) -> Self {
        self.end_timestamp = Some(timestamp.into());
        self
    }

    /// Adds a partition filter for file slice pruning.
    ///
    /// Multiple partition filters are combined with AND logic.
    pub fn with_partition_filter(mut self, filter: Filter) -> Self {
        self.partition_filter.push(filter);
        self
    }

    /// Adds multiple partition filters for file slice pruning.
    ///
    /// Multiple partition filters are combined with AND logic.
    pub fn with_partition_filters<I>(mut self, filters: I) -> Self
    where
        I: IntoIterator<Item = Filter>,
    {
        self.partition_filter.extend(filters);
        self
    }

    /// Sets the columns to project (select).
    ///
    /// Only the specified columns will be read from parquet files,
    /// improving read performance when only a subset of columns is needed.
    pub fn with_column<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.projection = Some(columns.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Sets a row-level filter predicate.
    ///
    /// This filter is pushed down to the parquet reader and applied
    /// during decoding, filtering out rows that don't match.
    pub fn with_row_predicate(mut self, predicate: Filter) -> Self {
        self.row_predicate = Some(predicate);
        self
    }

    /// Sets the target batch size for streaming reads.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Returns whether a time-travel timestamp is specified.
    pub fn has_as_of_timestamp(&self) -> bool {
        self.as_of_timestamp.is_some()
    }

    /// Returns whether any partition filters are specified.
    pub fn has_partition_filter(&self) -> bool {
        !self.partition_filter.is_empty()
    }

    /// Returns whether column projection is specified.
    pub fn has_projection(&self) -> bool {
        self.projection.is_some()
    }

    /// Returns whether a row predicate is specified.
    pub fn has_row_predicate(&self) -> bool {
        self.row_predicate.is_some()
    }

    /// Returns whether incremental read timestamps are specified.
    pub fn is_incremental(&self) -> bool {
        self.start_timestamp.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::filter::col;

    #[test]
    fn test_read_options_default() {
        let options = ReadOptions::new();
        assert!(options.as_of_timestamp.is_none());
        assert!(options.start_timestamp.is_none());
        assert!(options.end_timestamp.is_none());
        assert!(options.partition_filter.is_empty());
        assert!(options.projection.is_none());
        assert!(options.row_predicate.is_none());
        assert!(options.batch_size.is_none());
        assert!(!options.is_incremental());
    }

    #[test]
    fn test_read_options_as_of() {
        let options = ReadOptions::new().as_of("2024-01-01T12:00:00Z");
        assert!(options.has_as_of_timestamp());
        assert_eq!(
            options.as_of_timestamp,
            Some("2024-01-01T12:00:00Z".to_string())
        );
        assert!(!options.is_incremental());
    }

    #[test]
    fn test_read_options_incremental() {
        let options = ReadOptions::new()
            .from_timestamp("20240101120000000")
            .to_timestamp("20240102120000000");

        assert!(options.is_incremental());
        assert_eq!(
            options.start_timestamp,
            Some("20240101120000000".to_string())
        );
        assert_eq!(
            options.end_timestamp,
            Some("20240102120000000".to_string())
        );
    }

    #[test]
    fn test_read_options_with_partition_filter() {
        let options = ReadOptions::new()
            .with_partition_filter(col("date").eq("2024-01-01"))
            .with_partition_filter(col("region").eq("us-east"));

        assert_eq!(options.partition_filter.len(), 2);
        assert!(options.has_partition_filter());
    }

    #[test]
    fn test_read_options_with_column() {
        let options = ReadOptions::new().with_column(["id", "name", "value"]);

        assert!(options.has_projection());
        let cols = options.projection.unwrap();
        assert_eq!(cols, vec!["id", "name", "value"]);
    }

    #[test]
    fn test_read_options_with_row_predicate() {
        let options = ReadOptions::new().with_row_predicate(col("amount").gt("100"));

        assert!(options.has_row_predicate());
    }

    #[test]
    fn test_read_options_with_batch_size() {
        let options = ReadOptions::new().with_batch_size(4096);

        assert_eq!(options.batch_size, Some(4096));
    }

    #[test]
    fn test_read_options_builder_chain() {
        let options = ReadOptions::new()
            .as_of("2024-01-01")
            .with_partition_filter(col("date").eq("2024-01-01"))
            .with_column(["id", "name"])
            .with_row_predicate(col("status").eq("active"))
            .with_batch_size(2048);

        assert!(options.has_as_of_timestamp());
        assert!(options.has_partition_filter());
        assert!(options.has_projection());
        assert!(options.has_row_predicate());
        assert_eq!(options.batch_size, Some(2048));
    }
}
