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

/// Options for reading file slices with streaming APIs.
///
/// This struct provides configuration for:
/// - Column projection (which columns to read)
/// - Row-level predicates (filtering rows)
/// - Batch size control (rows per batch)
/// - Time travel (as-of timestamp)
///
/// # Example
///
/// ```ignore
/// use hudi::table::ReadOptions;
///
/// let options = ReadOptions::new()
///     .with_projection(&["id", "name"])
///     .with_batch_size(4096);
/// ```
#[derive(Default)]
pub struct ReadOptions {
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
