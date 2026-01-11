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
//! Statistics module for column statistics pruning.
//!
//! This module provides abstractions for extracting, aggregating, and using
//! column statistics from Parquet files for query pruning at different granularity levels.

use std::collections::HashMap;
use std::str::FromStr;

use arrow_schema::{DataType, Schema};
use datafusion_common::ScalarValue;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics as ParquetStatistics;

/// Column statistics pruning granularity level.
///
/// Controls how fine-grained the statistics-based pruning is.
/// Each level offers different trade-offs between memory overhead and pruning effectiveness.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum StatsGranularity {
    /// File-level stats (aggregated from all row groups).
    ///
    /// Coarsest granularity, lowest memory overhead.
    /// Can skip entire files that don't match predicates.
    /// Stats are computed by aggregating row group stats:
    /// - `file_min = min(row_group_mins)`
    /// - `file_max = max(row_group_maxs)`
    /// - `file_null_count = sum(row_group_null_counts)`
    #[default]
    File,

    /// Row group level stats (directly from Parquet footer).
    ///
    /// Balanced granularity, moderate memory.
    /// Can skip row groups within files (typically 64-128MB chunks).
    /// Stats are read directly from Parquet footer's ColumnChunkMetaData.
    RowGroup,

    /// Page level stats (from ColumnIndex, requires Parquet 1.11+).
    ///
    /// Finest granularity, highest memory.
    /// Can skip individual pages (typically ~1MB chunks).
    /// Most effective when data is sorted by filter columns.
    /// Requires Parquet files to be written with page index enabled.
    Page,
}

impl FromStr for StatsGranularity {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "file" => Ok(Self::File),
            "row_group" | "rowgroup" => Ok(Self::RowGroup),
            "page" => Ok(Self::Page),
            _ => Err(format!(
                "Invalid stats granularity: '{s}'. Valid options: file, row_group, page"
            )),
        }
    }
}

impl std::fmt::Display for StatsGranularity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File => write!(f, "file"),
            Self::RowGroup => write!(f, "row_group"),
            Self::Page => write!(f, "page"),
        }
    }
}

/// Statistics for a single column at a given granularity.
#[derive(Clone, Debug)]
pub struct ColumnStatistics {
    /// Column name
    pub column_name: String,
    /// Arrow data type
    pub data_type: DataType,
    /// Minimum value (as ScalarValue for compatibility with DataFusion)
    pub min_value: Option<ScalarValue>,
    /// Maximum value (as ScalarValue for compatibility with DataFusion)
    pub max_value: Option<ScalarValue>,
    /// Number of null values
    pub null_count: Option<i64>,
    /// Number of distinct values (if available)
    pub distinct_count: Option<u64>,
}

impl ColumnStatistics {
    /// Create a new ColumnStatistics with the given column name and data type.
    pub fn new(column_name: String, data_type: DataType) -> Self {
        Self {
            column_name,
            data_type,
            min_value: None,
            max_value: None,
            null_count: None,
            distinct_count: None,
        }
    }

    /// Create from Parquet row group statistics.
    ///
    /// Converts Parquet's `Statistics` to `ScalarValue` based on the Arrow data type.
    pub fn from_parquet_statistics(
        column_name: &str,
        data_type: &DataType,
        stats: &ParquetStatistics,
    ) -> Self {
        let (min_value, max_value) = parquet_stats_to_scalar(stats, data_type);
        let null_count = stats.null_count_opt().map(|n| n as i64);

        Self {
            column_name: column_name.to_string(),
            data_type: data_type.clone(),
            min_value,
            max_value,
            null_count,
            distinct_count: None,
        }
    }

    /// Merge with another ColumnStatistics (for aggregation).
    ///
    /// Takes min of mins, max of maxs, sums null counts.
    /// Used when aggregating row group stats to file-level stats.
    pub fn merge(&mut self, other: &ColumnStatistics) {
        // Merge min values (take the smaller one)
        self.min_value = match (&self.min_value, &other.min_value) {
            (Some(a), Some(b)) => scalar_min(a, b),
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (None, None) => None,
        };

        // Merge max values (take the larger one)
        self.max_value = match (&self.max_value, &other.max_value) {
            (Some(a), Some(b)) => scalar_max(a, b),
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (None, None) => None,
        };

        // Sum null counts
        self.null_count = match (self.null_count, other.null_count) {
            (Some(a), Some(b)) => Some(a + b),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        // Distinct count cannot be accurately merged, so we set it to None
        self.distinct_count = None;
    }
}

/// Container for statistics at a specific granularity level.
#[derive(Clone, Debug)]
pub struct StatisticsContainer {
    /// Granularity of these statistics
    pub granularity: StatsGranularity,
    /// Number of rows covered by these statistics
    pub num_rows: Option<i64>,
    /// Column statistics by column name
    pub columns: HashMap<String, ColumnStatistics>,
}

impl StatisticsContainer {
    /// Create an empty statistics container.
    pub fn new(granularity: StatsGranularity) -> Self {
        Self {
            granularity,
            num_rows: None,
            columns: HashMap::new(),
        }
    }

    /// Create file-level stats by aggregating row group stats from Parquet metadata.
    ///
    /// This iterates through all row groups, extracts stats for each column,
    /// and aggregates them to file-level statistics.
    pub fn from_parquet_metadata(metadata: &ParquetMetaData, schema: &Schema) -> Self {
        let mut container = Self::new(StatsGranularity::File);

        // Sum up total rows across all row groups
        let total_rows: i64 = metadata.row_groups().iter().map(|rg| rg.num_rows()).sum();
        container.num_rows = Some(total_rows);

        // Iterate through row groups and aggregate stats
        for row_group in metadata.row_groups() {
            let rg_stats = Self::from_row_group(row_group, schema);

            // Merge row group stats into file-level stats
            for (col_name, col_stats) in rg_stats.columns {
                container
                    .columns
                    .entry(col_name.clone())
                    .and_modify(|existing| existing.merge(&col_stats))
                    .or_insert(col_stats);
            }
        }

        // Ensure all schema columns have an entry (even if no stats)
        for field in schema.fields() {
            let col_name = field.name();
            if !container.columns.contains_key(col_name) {
                container.columns.insert(
                    col_name.clone(),
                    ColumnStatistics::new(col_name.clone(), field.data_type().clone()),
                );
            }
        }

        container
    }

    /// Create row-group-level stats from a single row group.
    pub fn from_row_group(row_group: &RowGroupMetaData, schema: &Schema) -> Self {
        let mut container = Self::new(StatsGranularity::RowGroup);
        container.num_rows = Some(row_group.num_rows());

        // Build a map of column name to Arrow data type
        let column_types: HashMap<&str, &DataType> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type()))
            .collect();

        // Extract stats for each column in the row group
        for col_chunk in row_group.columns() {
            // Get column name from the column descriptor.
            // We use `.last()` to get the leaf column name for nested columns.
            // NOTE: This means nested columns like "struct.field" will be matched by their
            // leaf name "field" only. Full path matching for nested columns would require
            // additional schema traversal logic.
            let col_path = col_chunk.column_descr().path();
            let Some(col_name) = col_path.parts().last().map(|s| s.as_str()) else {
                // Skip columns without a valid name (shouldn't happen in practice)
                continue;
            };

            // Skip if we don't have type info for this column
            let Some(&data_type) = column_types.get(col_name) else {
                continue;
            };

            // Extract statistics if available
            if let Some(stats) = col_chunk.statistics() {
                let col_stats =
                    ColumnStatistics::from_parquet_statistics(col_name, data_type, stats);
                container.columns.insert(col_name.to_string(), col_stats);
            } else {
                // No stats available, create empty entry
                container.columns.insert(
                    col_name.to_string(),
                    ColumnStatistics::new(col_name.to_string(), data_type.clone()),
                );
            }
        }

        container
    }

    /// Converts this statistics container to DataFusion's `Statistics` struct for query planning.
    ///
    /// This method transforms the collected Hudi column statistics into the format expected
    /// by DataFusion's query optimizer. The optimizer uses these statistics for:
    /// - Cardinality estimation (predicting row counts after filters)
    /// - Join ordering (choosing optimal join strategies)
    /// - Predicate pushdown decisions
    ///
    /// # Arguments
    /// * `schema` - The Arrow schema to use for mapping column statistics. Statistics are
    ///   returned in schema field order.
    ///
    /// # Returns
    /// A `datafusion_common::Statistics` struct with:
    /// - `num_rows`: Total row count with `Precision::Exact` if known
    /// - `column_statistics`: Per-column min/max/null_count with appropriate precision
    /// - `total_byte_size`: Always `Precision::Absent` (not tracked)
    pub fn to_datafusion_statistics(&self, schema: &Schema) -> datafusion_common::Statistics {
        use datafusion_common::ColumnStatistics as DFColStats;
        use datafusion_common::Statistics;
        use datafusion_common::stats::Precision;

        Statistics {
            num_rows: self
                .num_rows
                .map(|n| Precision::Exact(n as usize))
                .unwrap_or(Precision::Absent),
            total_byte_size: Precision::Absent,
            column_statistics: schema
                .fields()
                .iter()
                .map(|field| {
                    self.columns
                        .get(field.name())
                        .map(|col_stats| DFColStats {
                            null_count: col_stats
                                .null_count
                                .map(|n| Precision::Exact(n as usize))
                                .unwrap_or(Precision::Absent),
                            min_value: col_stats
                                .min_value
                                .clone()
                                .map(Precision::Exact)
                                .unwrap_or(Precision::Absent),
                            max_value: col_stats
                                .max_value
                                .clone()
                                .map(Precision::Exact)
                                .unwrap_or(Precision::Absent),
                            sum_value: Precision::Absent,
                            distinct_count: col_stats
                                .distinct_count
                                .map(|n| Precision::Exact(n as usize))
                                .unwrap_or(Precision::Absent),
                        })
                        .unwrap_or_default()
                })
                .collect(),
        }
    }
}

/// Convert Parquet statistics to ScalarValue min/max pair.
fn parquet_stats_to_scalar(
    stats: &ParquetStatistics,
    data_type: &DataType,
) -> (Option<ScalarValue>, Option<ScalarValue>) {
    match stats {
        ParquetStatistics::Boolean(s) => {
            let min = s.min_opt().map(|v| ScalarValue::Boolean(Some(*v)));
            let max = s.max_opt().map(|v| ScalarValue::Boolean(Some(*v)));
            (min, max)
        }
        ParquetStatistics::Int32(s) => {
            // Int32 in Parquet can map to various Arrow types
            match data_type {
                DataType::Int32 => {
                    let min = s.min_opt().map(|v| ScalarValue::Int32(Some(*v)));
                    let max = s.max_opt().map(|v| ScalarValue::Int32(Some(*v)));
                    (min, max)
                }
                DataType::Int16 => {
                    // Use try_from to safely convert, returning None on overflow
                    let min = s
                        .min_opt()
                        .and_then(|v| i16::try_from(*v).ok())
                        .map(|v| ScalarValue::Int16(Some(v)));
                    let max = s
                        .max_opt()
                        .and_then(|v| i16::try_from(*v).ok())
                        .map(|v| ScalarValue::Int16(Some(v)));
                    (min, max)
                }
                DataType::Int8 => {
                    let min = s
                        .min_opt()
                        .and_then(|v| i8::try_from(*v).ok())
                        .map(|v| ScalarValue::Int8(Some(v)));
                    let max = s
                        .max_opt()
                        .and_then(|v| i8::try_from(*v).ok())
                        .map(|v| ScalarValue::Int8(Some(v)));
                    (min, max)
                }
                DataType::UInt32 => {
                    let min = s
                        .min_opt()
                        .and_then(|v| u32::try_from(*v).ok())
                        .map(|v| ScalarValue::UInt32(Some(v)));
                    let max = s
                        .max_opt()
                        .and_then(|v| u32::try_from(*v).ok())
                        .map(|v| ScalarValue::UInt32(Some(v)));
                    (min, max)
                }
                DataType::UInt16 => {
                    let min = s
                        .min_opt()
                        .and_then(|v| u16::try_from(*v).ok())
                        .map(|v| ScalarValue::UInt16(Some(v)));
                    let max = s
                        .max_opt()
                        .and_then(|v| u16::try_from(*v).ok())
                        .map(|v| ScalarValue::UInt16(Some(v)));
                    (min, max)
                }
                DataType::UInt8 => {
                    let min = s
                        .min_opt()
                        .and_then(|v| u8::try_from(*v).ok())
                        .map(|v| ScalarValue::UInt8(Some(v)));
                    let max = s
                        .max_opt()
                        .and_then(|v| u8::try_from(*v).ok())
                        .map(|v| ScalarValue::UInt8(Some(v)));
                    (min, max)
                }
                DataType::Date32 => {
                    let min = s.min_opt().map(|v| ScalarValue::Date32(Some(*v)));
                    let max = s.max_opt().map(|v| ScalarValue::Date32(Some(*v)));
                    (min, max)
                }
                _ => {
                    // Default to Int32
                    let min = s.min_opt().map(|v| ScalarValue::Int32(Some(*v)));
                    let max = s.max_opt().map(|v| ScalarValue::Int32(Some(*v)));
                    (min, max)
                }
            }
        }
        ParquetStatistics::Int64(s) => match data_type {
            DataType::Int64 => {
                let min = s.min_opt().map(|v| ScalarValue::Int64(Some(*v)));
                let max = s.max_opt().map(|v| ScalarValue::Int64(Some(*v)));
                (min, max)
            }
            DataType::UInt64 => {
                // Safely convert i64 to u64, returning None for negative values
                let min = s
                    .min_opt()
                    .and_then(|v| u64::try_from(*v).ok())
                    .map(|v| ScalarValue::UInt64(Some(v)));
                let max = s
                    .max_opt()
                    .and_then(|v| u64::try_from(*v).ok())
                    .map(|v| ScalarValue::UInt64(Some(v)));
                (min, max)
            }
            DataType::Date64 => {
                let min = s.min_opt().map(|v| ScalarValue::Date64(Some(*v)));
                let max = s.max_opt().map(|v| ScalarValue::Date64(Some(*v)));
                (min, max)
            }
            DataType::Timestamp(unit, tz) => {
                use arrow_schema::TimeUnit;
                let min = s.min_opt().map(|v| match unit {
                    TimeUnit::Second => ScalarValue::TimestampSecond(Some(*v), tz.clone()),
                    TimeUnit::Millisecond => {
                        ScalarValue::TimestampMillisecond(Some(*v), tz.clone())
                    }
                    TimeUnit::Microsecond => {
                        ScalarValue::TimestampMicrosecond(Some(*v), tz.clone())
                    }
                    TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(*v), tz.clone()),
                });
                let max = s.max_opt().map(|v| match unit {
                    TimeUnit::Second => ScalarValue::TimestampSecond(Some(*v), tz.clone()),
                    TimeUnit::Millisecond => {
                        ScalarValue::TimestampMillisecond(Some(*v), tz.clone())
                    }
                    TimeUnit::Microsecond => {
                        ScalarValue::TimestampMicrosecond(Some(*v), tz.clone())
                    }
                    TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(*v), tz.clone()),
                });
                (min, max)
            }
            _ => {
                let min = s.min_opt().map(|v| ScalarValue::Int64(Some(*v)));
                let max = s.max_opt().map(|v| ScalarValue::Int64(Some(*v)));
                (min, max)
            }
        },
        ParquetStatistics::Int96(_) => {
            // Int96 is deprecated, typically used for timestamps in legacy Parquet
            // We don't support it for statistics
            (None, None)
        }
        ParquetStatistics::Float(s) => {
            let min = s.min_opt().map(|v| ScalarValue::Float32(Some(*v)));
            let max = s.max_opt().map(|v| ScalarValue::Float32(Some(*v)));
            (min, max)
        }
        ParquetStatistics::Double(s) => {
            let min = s.min_opt().map(|v| ScalarValue::Float64(Some(*v)));
            let max = s.max_opt().map(|v| ScalarValue::Float64(Some(*v)));
            (min, max)
        }
        ParquetStatistics::ByteArray(s) => match data_type {
            DataType::Utf8 | DataType::LargeUtf8 => {
                let min = s
                    .min_opt()
                    .and_then(|b| std::str::from_utf8(b.data()).ok())
                    .map(|s| ScalarValue::Utf8(Some(s.to_string())));
                let max = s
                    .max_opt()
                    .and_then(|b| std::str::from_utf8(b.data()).ok())
                    .map(|s| ScalarValue::Utf8(Some(s.to_string())));
                (min, max)
            }
            DataType::Binary | DataType::LargeBinary => {
                let min = s
                    .min_opt()
                    .map(|b| ScalarValue::Binary(Some(b.data().to_vec())));
                let max = s
                    .max_opt()
                    .map(|b| ScalarValue::Binary(Some(b.data().to_vec())));
                (min, max)
            }
            _ => (None, None),
        },
        ParquetStatistics::FixedLenByteArray(s) => match data_type {
            DataType::FixedSizeBinary(size) => {
                let min = s
                    .min_opt()
                    .map(|b| ScalarValue::FixedSizeBinary(*size, Some(b.data().to_vec())));
                let max = s
                    .max_opt()
                    .map(|b| ScalarValue::FixedSizeBinary(*size, Some(b.data().to_vec())));
                (min, max)
            }
            _ => (None, None),
        },
    }
}

/// Compare two ScalarValues and return the smaller one.
fn scalar_min(a: &ScalarValue, b: &ScalarValue) -> Option<ScalarValue> {
    match a.partial_cmp(b) {
        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => Some(a.clone()),
        Some(std::cmp::Ordering::Greater) => Some(b.clone()),
        None => None,
    }
}

/// Compare two ScalarValues and return the larger one.
fn scalar_max(a: &ScalarValue, b: &ScalarValue) -> Option<ScalarValue> {
    match a.partial_cmp(b) {
        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal) => Some(a.clone()),
        Some(std::cmp::Ordering::Less) => Some(b.clone()),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_granularity_from_str() {
        assert_eq!(
            StatsGranularity::from_str("file").unwrap(),
            StatsGranularity::File
        );
        assert_eq!(
            StatsGranularity::from_str("FILE").unwrap(),
            StatsGranularity::File
        );
        assert_eq!(
            StatsGranularity::from_str("row_group").unwrap(),
            StatsGranularity::RowGroup
        );
        assert_eq!(
            StatsGranularity::from_str("rowgroup").unwrap(),
            StatsGranularity::RowGroup
        );
        assert_eq!(
            StatsGranularity::from_str("page").unwrap(),
            StatsGranularity::Page
        );
        assert!(StatsGranularity::from_str("invalid").is_err());
    }

    #[test]
    fn test_stats_granularity_display() {
        assert_eq!(format!("{}", StatsGranularity::File), "file");
        assert_eq!(format!("{}", StatsGranularity::RowGroup), "row_group");
        assert_eq!(format!("{}", StatsGranularity::Page), "page");
    }

    #[test]
    fn test_stats_granularity_default() {
        assert_eq!(StatsGranularity::default(), StatsGranularity::File);
    }

    #[test]
    fn test_column_statistics_merge() {
        let mut stats1 = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::Int32,
            min_value: Some(ScalarValue::Int32(Some(10))),
            max_value: Some(ScalarValue::Int32(Some(50))),
            null_count: Some(5),
            distinct_count: Some(10),
        };

        let stats2 = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::Int32,
            min_value: Some(ScalarValue::Int32(Some(5))),
            max_value: Some(ScalarValue::Int32(Some(100))),
            null_count: Some(3),
            distinct_count: Some(20),
        };

        stats1.merge(&stats2);

        assert_eq!(stats1.min_value, Some(ScalarValue::Int32(Some(5))));
        assert_eq!(stats1.max_value, Some(ScalarValue::Int32(Some(100))));
        assert_eq!(stats1.null_count, Some(8));
        // Distinct count cannot be accurately merged
        assert_eq!(stats1.distinct_count, None);
    }

    #[test]
    fn test_column_statistics_merge_with_none() {
        let mut stats1 = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::Int32,
            min_value: Some(ScalarValue::Int32(Some(10))),
            max_value: None,
            null_count: Some(5),
            distinct_count: None,
        };

        let stats2 = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::Int32,
            min_value: None,
            max_value: Some(ScalarValue::Int32(Some(100))),
            null_count: None,
            distinct_count: None,
        };

        stats1.merge(&stats2);

        assert_eq!(stats1.min_value, Some(ScalarValue::Int32(Some(10))));
        assert_eq!(stats1.max_value, Some(ScalarValue::Int32(Some(100))));
        assert_eq!(stats1.null_count, Some(5));
    }

    #[test]
    fn test_statistics_container_new() {
        let container = StatisticsContainer::new(StatsGranularity::File);
        assert_eq!(container.granularity, StatsGranularity::File);
        assert_eq!(container.num_rows, None);
        assert!(container.columns.is_empty());
    }

    #[test]
    fn test_scalar_min_max() {
        let a = ScalarValue::Int32(Some(10));
        let b = ScalarValue::Int32(Some(20));

        assert_eq!(scalar_min(&a, &b), Some(a.clone()));
        assert_eq!(scalar_max(&a, &b), Some(b.clone()));
    }
}
