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
//! Read options for streaming reads.

use arrow_array::{BooleanArray, RecordBatch};

/// A row-level predicate function for filtering records.
pub type RowPredicate = Box<dyn Fn(&RecordBatch) -> crate::Result<BooleanArray> + Send + Sync>;

/// A partition filter tuple: (field_name, operator, value).
/// Example: ("city", "=", "san_francisco")
pub type PartitionFilter = (String, String, String);

/// Options for reading file slices with streaming APIs.
///
/// This struct provides configuration for:
/// - Partition filters (filtering partitions)
/// - Column projection (which columns to read)
/// - Row-level predicates (filtering rows)
/// - Batch size control (rows per batch)
/// - Time travel (as-of timestamp)
///
/// # Current Limitations
///
/// Not all options are supported in all streaming APIs:
/// - `batch_size` and `partition_filters` are fully supported.
/// - `projection` is passed through but not yet applied at the parquet read level.
/// - `row_predicate` is not yet implemented in streaming reads.
///
/// # Example
///
/// ```ignore
/// use hudi::table::ReadOptions;
///
/// let options = ReadOptions::new()
///     .with_filters([("city", "=", "san_francisco")])
///     .with_batch_size(4096);
/// ```
#[derive(Default)]
pub struct ReadOptions {
    /// Partition filters. Each filter is a tuple of (field, operator, value).
    pub partition_filters: Vec<PartitionFilter>,

    /// Column names to project (select). If None, all columns are read.
    pub projection: Option<Vec<String>>,

    /// Row-level filter predicate. Applied after reading each batch.
    pub row_predicate: Option<RowPredicate>,

    /// Target number of rows per batch.
    pub batch_size: Option<usize>,

    /// Timestamp for time travel queries (as-of).
    pub as_of_timestamp: Option<String>,
}

impl ReadOptions {
    /// Creates a new ReadOptions with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets partition filters.
    ///
    /// # Arguments
    /// * `filters` - Partition filters as tuples of (field, operator, value)
    pub fn with_filters<I, S1, S2, S3>(mut self, filters: I) -> Self
    where
        I: IntoIterator<Item = (S1, S2, S3)>,
        S1: Into<String>,
        S2: Into<String>,
        S3: Into<String>,
    {
        self.partition_filters = filters
            .into_iter()
            .map(|(f, o, v)| (f.into(), o.into(), v.into()))
            .collect();
        self
    }

    /// Sets the column projection (which columns to read).
    ///
    /// # Arguments
    /// * `columns` - Column names to project
    pub fn with_projection<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.projection = Some(columns.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Sets the row-level predicate for filtering records.
    ///
    /// # Arguments
    /// * `predicate` - A function that takes a RecordBatch and returns a BooleanArray mask
    pub fn with_row_predicate<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&RecordBatch) -> crate::Result<BooleanArray> + Send + Sync + 'static,
    {
        self.row_predicate = Some(Box::new(predicate));
        self
    }

    /// Sets the target batch size (rows per batch).
    ///
    /// # Arguments
    /// * `size` - Target number of rows per batch
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }

    /// Sets the as-of timestamp for time travel queries.
    ///
    /// # Arguments
    /// * `timestamp` - The timestamp to query as of
    pub fn with_as_of_timestamp<S: Into<String>>(mut self, timestamp: S) -> Self {
        self.as_of_timestamp = Some(timestamp.into());
        self
    }
}

impl std::fmt::Debug for ReadOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadOptions")
            .field("filters", &self.partition_filters)
            .field("projection", &self.projection)
            .field(
                "row_predicate",
                &self.row_predicate.as_ref().map(|_| "<predicate>"),
            )
            .field("batch_size", &self.batch_size)
            .field("as_of_timestamp", &self.as_of_timestamp)
            .finish()
    }
}
