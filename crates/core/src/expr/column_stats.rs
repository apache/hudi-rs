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

//! Column statistics for data skipping optimization.

use crate::expr::filter::Filter;
use crate::expr::ExprOperator;
use crate::Result;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use std::collections::HashMap;

/// Column statistics for a single column in a file.
///
/// Contains min/max values that can be used to prune files
/// when evaluating filters.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStats {
    /// Column name
    pub column_name: String,

    /// Minimum value in the column (as string for generic comparison)
    pub min_value: Option<String>,

    /// Maximum value in the column (as string for generic comparison)
    pub max_value: Option<String>,

    /// Number of null values
    pub null_count: Option<i64>,

    /// Total number of values
    pub value_count: Option<i64>,
}

impl ColumnStats {
    /// Creates column statistics.
    pub fn new(
        column_name: String,
        min_value: Option<String>,
        max_value: Option<String>,
        null_count: Option<i64>,
        value_count: Option<i64>,
    ) -> Self {
        Self {
            column_name,
            min_value,
            max_value,
            null_count,
            value_count,
        }
    }

    /// Checks if this column's min/max range can satisfy a filter.
    ///
    /// Returns `None` if insufficient stats, `Some(true)` if might match, `Some(false)` to prune.
    pub fn can_satisfy_filter(&self, filter: &Filter) -> Option<bool> {
        if filter.field_name != self.column_name {
            return None;
        }

        let min = self.min_value.as_ref()?;
        let max = self.max_value.as_ref()?;
        let value = &filter.field_value;

        // Try numeric comparison first, fallback to string comparison
        let (min_cmp, max_cmp, eq_cmp) = if let (Ok(min_num), Ok(max_num), Ok(val_num)) =
            (min.parse::<f64>(), max.parse::<f64>(), value.parse::<f64>())
        {
            (
                val_num > min_num,                        // value > min
                val_num < max_num,                        // value < max
                val_num >= min_num && val_num <= max_num, // value in [min, max]
            )
        } else {
            (value > min, value < max, value >= min && value <= max)
        };

        match filter.operator {
            ExprOperator::Eq => Some(eq_cmp),
            ExprOperator::Ne => None, // Unsupported: can't prove all values != target
            ExprOperator::Lt => Some(min_cmp),
            ExprOperator::Lte => Some(eq_cmp || min_cmp),
            ExprOperator::Gt => Some(max_cmp),
            ExprOperator::Gte => Some(eq_cmp || max_cmp),
        }
    }
}

/// File-level column statistics for all columns.
///
/// Used to evaluate filters against a file's statistics
/// to determine if the file should be read.
#[derive(Debug, Clone, Default)]
pub struct FileColumnStats {
    /// Statistics for each column, keyed by column name
    stats: HashMap<String, ColumnStats>,
}

impl FileColumnStats {
    /// Creates a new empty file column statistics.
    pub fn new() -> Self {
        Self {
            stats: HashMap::new(),
        }
    }

    /// Adds column statistics for a column.
    pub fn add_column_stats(&mut self, stats: ColumnStats) {
        self.stats.insert(stats.column_name.clone(), stats);
    }

    /// Gets statistics for a specific column.
    pub fn get_column_stats(&self, column_name: &str) -> Option<&ColumnStats> {
        self.stats.get(column_name)
    }

    /// Checks if file can satisfy a filter based on column stats.
    pub fn can_satisfy_filter(&self, filter: &Filter) -> Option<bool> {
        self.stats
            .get(&filter.field_name)
            .and_then(|stats| stats.can_satisfy_filter(filter))
    }

    /// Checks if file can satisfy all filters (AND logic). Returns false to prune file.
    pub fn can_satisfy_filters(&self, filters: &[Filter]) -> bool {
        for filter in filters {
            match self.can_satisfy_filter(filter) {
                Some(false) => return false,   // Definitely cannot satisfy this filter
                Some(true) | None => continue, // Maybe can satisfy, or unknown
            }
        }
        true // All filters might be satisfiable
    }

    /// Returns true if file should be included. Conservative: includes file when uncertain.
    pub fn should_include_file(&self, filters: &[Filter]) -> bool {
        self.can_satisfy_filters(filters)
    }

    /// Extracts column statistics from Parquet file metadata.
    ///
    /// Reads min/max values from Parquet column statistics and converts them to strings
    /// for generic comparison.
    ///
    /// # Arguments
    /// * `parquet_metadata` - Parquet file metadata containing column statistics
    ///
    /// # Returns
    /// `FileColumnStats` with statistics for all columns that have min/max available
    pub fn from_parquet_metadata(parquet_metadata: &ParquetMetaData) -> Result<Self> {
        let mut file_stats = Self::new();

        // Get the schema to map column indices to names
        let schema = parquet_metadata.file_metadata().schema_descr();

        // Iterate over all row groups to collect min/max across the file
        for row_group in parquet_metadata.row_groups() {
            for (col_idx, column_chunk) in row_group.columns().iter().enumerate() {
                let column_descr = schema.column(col_idx);
                let column_name = column_descr.name().to_string();

                // Get statistics if available
                if let Some(stats) = column_chunk.statistics() {
                    let (min_val, max_val) = match stats {
                        Statistics::Boolean(s) => (
                            s.min_opt().map(|v| v.to_string()),
                            s.max_opt().map(|v| v.to_string()),
                        ),
                        Statistics::Int32(s) => (
                            s.min_opt().map(|v| v.to_string()),
                            s.max_opt().map(|v| v.to_string()),
                        ),
                        Statistics::Int64(s) => (
                            s.min_opt().map(|v| v.to_string()),
                            s.max_opt().map(|v| v.to_string()),
                        ),
                        Statistics::Int96(s) => (
                            s.min_opt().map(|v| format!("{:?}", v)),
                            s.max_opt().map(|v| format!("{:?}", v)),
                        ),
                        Statistics::Float(s) => (
                            s.min_opt().map(|v| v.to_string()),
                            s.max_opt().map(|v| v.to_string()),
                        ),
                        Statistics::Double(s) => (
                            s.min_opt().map(|v| v.to_string()),
                            s.max_opt().map(|v| v.to_string()),
                        ),
                        Statistics::ByteArray(s) => (
                            s.min_opt()
                                .and_then(|v| String::from_utf8(v.data().to_vec()).ok()),
                            s.max_opt()
                                .and_then(|v| String::from_utf8(v.data().to_vec()).ok()),
                        ),
                        Statistics::FixedLenByteArray(s) => (
                            s.min_opt()
                                .and_then(|v| String::from_utf8(v.data().to_vec()).ok()),
                            s.max_opt()
                                .and_then(|v| String::from_utf8(v.data().to_vec()).ok()),
                        ),
                    };

                    let null_count = stats.null_count_opt();
                    let value_count = stats.distinct_count_opt().map(|n| n as i64);

                    // Update or insert stats (take min of mins, max of maxes across row groups)
                    if let Some(existing_stats) = file_stats.stats.get_mut(&column_name) {
                        // Update with min of mins, max of maxes
                        if let (Some(new_min), Some(existing_min)) =
                            (&min_val, &existing_stats.min_value)
                        {
                            if new_min < existing_min {
                                existing_stats.min_value = min_val.clone();
                            }
                        }
                        if let (Some(new_max), Some(existing_max)) =
                            (&max_val, &existing_stats.max_value)
                        {
                            if new_max > existing_max {
                                existing_stats.max_value = max_val.clone();
                            }
                        }
                        // Aggregate null counts
                        if let (Some(new_null), Some(existing_null)) =
                            (null_count, existing_stats.null_count)
                        {
                            existing_stats.null_count = Some(existing_null + new_null as i64);
                        }
                    } else {
                        // First time seeing this column
                        file_stats.add_column_stats(ColumnStats::new(
                            column_name.clone(),
                            min_val,
                            max_val,
                            null_count.map(|n| n as i64),
                            value_count,
                        ));
                    }
                }
            }
        }

        Ok(file_stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_filter(field: &str, op: ExprOperator, value: &str) -> Filter {
        Filter {
            field_name: field.to_string(),
            operator: op,
            field_value: value.to_string(),
        }
    }

    #[test]
    fn test_column_stats_eq() {
        let stats = ColumnStats::new(
            "age".to_string(),
            Some("10".to_string()),
            Some("50".to_string()),
            None,
            None,
        );

        // Value in range
        let filter = create_filter("age", ExprOperator::Eq, "25");
        assert_eq!(stats.can_satisfy_filter(&filter), Some(true));

        // Value below range
        let filter = create_filter("age", ExprOperator::Eq, "5");
        assert_eq!(stats.can_satisfy_filter(&filter), Some(false));

        // Value above range
        let filter = create_filter("age", ExprOperator::Eq, "100");
        assert_eq!(stats.can_satisfy_filter(&filter), Some(false));
    }

    #[test]
    fn test_column_stats_gt() {
        let stats = ColumnStats::new(
            "age".to_string(),
            Some("10".to_string()),
            Some("50".to_string()),
            None,
            None,
        );

        // Need age > 5, max=50, so possible
        let filter = create_filter("age", ExprOperator::Gt, "5");
        assert_eq!(stats.can_satisfy_filter(&filter), Some(true));

        // Need age > 50, max=50, not possible
        let filter = create_filter("age", ExprOperator::Gt, "50");
        assert_eq!(stats.can_satisfy_filter(&filter), Some(false));

        // Need age > 100, max=50, not possible
        let filter = create_filter("age", ExprOperator::Gt, "100");
        assert_eq!(stats.can_satisfy_filter(&filter), Some(false));
    }

    #[test]
    fn test_column_stats_lt() {
        let stats = ColumnStats::new(
            "age".to_string(),
            Some("10".to_string()),
            Some("50".to_string()),
            None,
            None,
        );

        // Need age < 100, min=10, so possible
        let filter = create_filter("age", ExprOperator::Lt, "100");
        assert_eq!(stats.can_satisfy_filter(&filter), Some(true));

        // Need age < 10, min=10, not possible
        let filter = create_filter("age", ExprOperator::Lt, "10");
        assert_eq!(stats.can_satisfy_filter(&filter), Some(false));

        // Need age < 5, min=10, not possible
        let filter = create_filter("age", ExprOperator::Lt, "5");
        assert_eq!(stats.can_satisfy_filter(&filter), Some(false));
    }

    #[test]
    fn test_column_stats_not_equal_unsupported() {
        let stats = ColumnStats::new(
            "age".to_string(),
            Some("10".to_string()),
            Some("50".to_string()),
            None,
            None,
        );

        // Not-equal operator is not supported, should return None
        let filter = create_filter("age", ExprOperator::Ne, "25");
        assert_eq!(stats.can_satisfy_filter(&filter), None);
    }

    #[test]
    fn test_column_stats_wrong_column() {
        let stats = ColumnStats::new(
            "age".to_string(),
            Some("10".to_string()),
            Some("50".to_string()),
            None,
            None,
        );

        let filter = create_filter("name", ExprOperator::Eq, "John");
        assert_eq!(stats.can_satisfy_filter(&filter), None);
    }

    #[test]
    fn test_file_column_stats_single_filter() {
        let mut file_stats = FileColumnStats::new();
        file_stats.add_column_stats(ColumnStats::new(
            "fare".to_string(),
            Some("10".to_string()),
            Some("100".to_string()),
            None,
            None,
        ));

        // Should include: fare might be > 50
        let filter = create_filter("fare", ExprOperator::Gt, "50");
        assert!(file_stats.should_include_file(&[filter]));

        // Should prune: fare cannot be > 200
        let filter = create_filter("fare", ExprOperator::Gt, "200");
        assert!(!file_stats.should_include_file(&[filter]));
    }

    #[test]
    fn test_file_column_stats_multiple_filters() {
        let mut file_stats = FileColumnStats::new();
        file_stats.add_column_stats(ColumnStats::new(
            "fare".to_string(),
            Some("10".to_string()),
            Some("100".to_string()),
            None,
            None,
        ));
        file_stats.add_column_stats(ColumnStats::new(
            "passengers".to_string(),
            Some("1".to_string()),
            Some("4".to_string()),
            None,
            None,
        ));

        // Both conditions can be satisfied (AND logic)
        let filters = vec![
            create_filter("fare", ExprOperator::Gt, "20"),
            create_filter("passengers", ExprOperator::Eq, "2"),
        ];
        assert!(file_stats.should_include_file(&filters));

        // First condition fails (AND logic - all must pass)
        let filters = vec![
            create_filter("fare", ExprOperator::Gt, "200"),
            create_filter("passengers", ExprOperator::Eq, "2"),
        ];
        assert!(!file_stats.should_include_file(&filters));
    }

    #[test]
    fn test_file_column_stats_conservative() {
        let file_stats = FileColumnStats::new(); // No stats

        // Unknown column - conservative, include file
        let filter = create_filter("unknown", ExprOperator::Gt, "100");
        assert!(file_stats.should_include_file(&[filter]));
    }
}
