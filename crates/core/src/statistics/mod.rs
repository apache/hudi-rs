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
//!
//! The core types (`StatScalar`, `ColumnStatistics`, `StatisticsContainer`) use only
//! Parquet physical types, keeping hudi-core independent of query engine implementations.
//! Conversions to query engine types (e.g., DataFusion) are feature-gated and use the
//! Arrow DataType stored alongside statistics for proper logical type interpretation.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::str::FromStr;

use arrow_schema::{DataType, Schema};
#[cfg(feature = "datafusion")]
use arrow_schema::TimeUnit;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics as ParquetStatistics;

/// Column statistics pruning granularity level.
///
/// Controls how fine-grained the statistics-based pruning is.
/// Each level offers different trade-offs between memory overhead and pruning effectiveness.
///
/// String parsing is case-insensitive and accepts "row_group" or "rowgroup" for RowGroup.
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
    /// Can skip row groups within files.
    /// Stats are read directly from Parquet footer's ColumnChunkMetaData.
    RowGroup,

    /// Page level stats (from ColumnIndex, requires Parquet 1.11+).
    ///
    /// Finest granularity, highest memory.
    /// Can skip individual pages.
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

/// A minimal scalar value representation matching Parquet physical types.
///
/// This enum provides a query-engine-agnostic way to store scalar values
/// extracted from Parquet statistics. It uses Parquet's physical types rather
/// than Arrow's logical types, minimizing maintenance burden while preserving
/// correct comparison semantics.
///
/// Logical type interpretation (e.g., Int32 → Date32) is deferred to query
/// engine conversion using the Arrow DataType stored in [`ColumnStatistics`].
#[derive(Clone, Debug, PartialEq)]
pub enum StatScalar {
    /// Null value
    Null,
    /// Boolean value (Parquet BOOLEAN)
    Boolean(bool),
    /// 32-bit signed integer (Parquet INT32)
    /// Covers: Int8, Int16, Int32, UInt8, UInt16, Date32
    Int32(i32),
    /// 64-bit signed integer (Parquet INT64)
    /// Covers: Int64, UInt32, UInt64, Date64, Timestamp, Time
    Int64(i64),
    /// 32-bit floating point (Parquet FLOAT)
    Float32(f32),
    /// 64-bit floating point (Parquet DOUBLE)
    Float64(f64),
    /// Binary data (Parquet BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY)
    /// Covers: Utf8, LargeUtf8, Binary, LargeBinary, FixedSizeBinary
    Binary(Vec<u8>),
}

impl PartialOrd for StatScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use StatScalar::*;
        match (self, other) {
            (Null, Null) => Some(Ordering::Equal),
            (Null, _) => Some(Ordering::Less),
            (_, Null) => Some(Ordering::Greater),
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),
            (Int32(a), Int32(b)) => a.partial_cmp(b),
            (Int64(a), Int64(b)) => a.partial_cmp(b),
            (Float32(a), Float32(b)) => a.partial_cmp(b),
            (Float64(a), Float64(b)) => a.partial_cmp(b),
            (Binary(a), Binary(b)) => a.partial_cmp(b),
            _ => None, // Different types are not comparable
        }
    }
}

/// Convert StatScalar to DataFusion ScalarValue using Arrow DataType for interpretation.
#[cfg(feature = "datafusion")]
pub fn to_scalar_value(scalar: &StatScalar, data_type: &DataType) -> datafusion_common::ScalarValue {
    use datafusion_common::ScalarValue;

    match (scalar, data_type) {
        (StatScalar::Null, _) => ScalarValue::Null,
        (StatScalar::Boolean(v), _) => ScalarValue::Boolean(Some(*v)),

        // Int32 physical type → various logical types
        (StatScalar::Int32(v), DataType::Int32) => ScalarValue::Int32(Some(*v)),
        (StatScalar::Int32(v), DataType::Int16) => ScalarValue::Int16(Some(*v as i16)),
        (StatScalar::Int32(v), DataType::Int8) => ScalarValue::Int8(Some(*v as i8)),
        (StatScalar::Int32(v), DataType::UInt32) => ScalarValue::UInt32(Some(*v as u32)),
        (StatScalar::Int32(v), DataType::UInt16) => ScalarValue::UInt16(Some(*v as u16)),
        (StatScalar::Int32(v), DataType::UInt8) => ScalarValue::UInt8(Some(*v as u8)),
        (StatScalar::Int32(v), DataType::Date32) => ScalarValue::Date32(Some(*v)),
        (StatScalar::Int32(v), _) => ScalarValue::Int32(Some(*v)), // fallback

        // Int64 physical type → various logical types
        (StatScalar::Int64(v), DataType::Int64) => ScalarValue::Int64(Some(*v)),
        (StatScalar::Int64(v), DataType::UInt64) => ScalarValue::UInt64(Some(*v as u64)),
        (StatScalar::Int64(v), DataType::Date64) => ScalarValue::Date64(Some(*v)),
        (StatScalar::Int64(v), DataType::Timestamp(TimeUnit::Second, tz)) => {
            ScalarValue::TimestampSecond(Some(*v), tz.clone())
        }
        (StatScalar::Int64(v), DataType::Timestamp(TimeUnit::Millisecond, tz)) => {
            ScalarValue::TimestampMillisecond(Some(*v), tz.clone())
        }
        (StatScalar::Int64(v), DataType::Timestamp(TimeUnit::Microsecond, tz)) => {
            ScalarValue::TimestampMicrosecond(Some(*v), tz.clone())
        }
        (StatScalar::Int64(v), DataType::Timestamp(TimeUnit::Nanosecond, tz)) => {
            ScalarValue::TimestampNanosecond(Some(*v), tz.clone())
        }
        (StatScalar::Int64(v), _) => ScalarValue::Int64(Some(*v)), // fallback

        // Float types
        (StatScalar::Float32(v), _) => ScalarValue::Float32(Some(*v)),
        (StatScalar::Float64(v), _) => ScalarValue::Float64(Some(*v)),

        // Binary physical type → string or binary logical types
        (StatScalar::Binary(v), DataType::Utf8 | DataType::LargeUtf8) => {
            ScalarValue::Utf8(Some(String::from_utf8_lossy(v).into_owned()))
        }
        (StatScalar::Binary(v), DataType::FixedSizeBinary(size)) => {
            ScalarValue::FixedSizeBinary(*size, Some(v.clone()))
        }
        (StatScalar::Binary(v), _) => ScalarValue::Binary(Some(v.clone())),
    }
}

/// Statistics for a single column at a given granularity.
#[derive(Clone, Debug)]
pub struct ColumnStatistics {
    /// Column name
    pub column_name: String,
    /// Arrow data type (used for logical type interpretation during query engine conversion)
    pub data_type: DataType,
    /// Minimum value (stored as Parquet physical type)
    pub min_value: Option<StatScalar>,
    /// Maximum value (stored as Parquet physical type)
    pub max_value: Option<StatScalar>,
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
    /// Extracts min/max as physical types; the Arrow data_type is stored for
    /// later conversion to query engine types.
    pub fn from_parquet_statistics(
        column_name: &str,
        data_type: &DataType,
        stats: &ParquetStatistics,
    ) -> Self {
        let (min_value, max_value) = parquet_stats_to_scalar(stats);
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
    ///
    /// Uses the stored Arrow DataType for correct comparison of unsigned integers,
    /// which are stored as signed values in Parquet but need unsigned comparison semantics.
    pub fn merge(&mut self, other: &ColumnStatistics) {
        // Merge min values (take the smaller one)
        // Use take() to avoid cloning self's value when other is None
        self.min_value = match (self.min_value.take(), &other.min_value) {
            (Some(a), Some(b)) => scalar_min(a, b.clone(), &self.data_type),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b.clone()),
            (None, None) => None,
        };

        // Merge max values (take the larger one)
        self.max_value = match (self.max_value.take(), &other.max_value) {
            (Some(a), Some(b)) => scalar_max(a, b.clone(), &self.data_type),
            (Some(a), None) => Some(a),
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
                    .entry(col_name)
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
    #[cfg(feature = "datafusion")]
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
                                .as_ref()
                                .map(|v| {
                                    Precision::Exact(to_scalar_value(v, &col_stats.data_type))
                                })
                                .unwrap_or(Precision::Absent),
                            max_value: col_stats
                                .max_value
                                .as_ref()
                                .map(|v| {
                                    Precision::Exact(to_scalar_value(v, &col_stats.data_type))
                                })
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

/// Convert Parquet statistics to StatScalar min/max pair (physical types only).
///
/// This extracts statistics as Parquet physical types without any logical type
/// interpretation. The Arrow DataType stored in ColumnStatistics is used later
/// for proper conversion to query engine types.
fn parquet_stats_to_scalar(stats: &ParquetStatistics) -> (Option<StatScalar>, Option<StatScalar>) {
    match stats {
        ParquetStatistics::Boolean(s) => {
            let min = s.min_opt().map(|v| StatScalar::Boolean(*v));
            let max = s.max_opt().map(|v| StatScalar::Boolean(*v));
            (min, max)
        }
        ParquetStatistics::Int32(s) => {
            let min = s.min_opt().map(|v| StatScalar::Int32(*v));
            let max = s.max_opt().map(|v| StatScalar::Int32(*v));
            (min, max)
        }
        ParquetStatistics::Int64(s) => {
            let min = s.min_opt().map(|v| StatScalar::Int64(*v));
            let max = s.max_opt().map(|v| StatScalar::Int64(*v));
            (min, max)
        }
        ParquetStatistics::Int96(_) => {
            // Int96 is deprecated, typically used for timestamps in legacy Parquet
            (None, None)
        }
        ParquetStatistics::Float(s) => {
            let min = s.min_opt().map(|v| StatScalar::Float32(*v));
            let max = s.max_opt().map(|v| StatScalar::Float32(*v));
            (min, max)
        }
        ParquetStatistics::Double(s) => {
            let min = s.min_opt().map(|v| StatScalar::Float64(*v));
            let max = s.max_opt().map(|v| StatScalar::Float64(*v));
            (min, max)
        }
        ParquetStatistics::ByteArray(s) => {
            let min = s.min_opt().map(|b| StatScalar::Binary(b.data().to_vec()));
            let max = s.max_opt().map(|b| StatScalar::Binary(b.data().to_vec()));
            (min, max)
        }
        ParquetStatistics::FixedLenByteArray(s) => {
            let min = s.min_opt().map(|b| StatScalar::Binary(b.data().to_vec()));
            let max = s.max_opt().map(|b| StatScalar::Binary(b.data().to_vec()));
            (min, max)
        }
    }
}

/// Compare two StatScalar values and return the smaller one.
///
/// Uses the Arrow DataType for correct comparison of unsigned integers,
/// which are stored as signed in Parquet but need unsigned comparison semantics.
fn scalar_min(a: StatScalar, b: StatScalar, data_type: &DataType) -> Option<StatScalar> {
    let ordering = compare_scalars(&a, &b, data_type)?;
    match ordering {
        Ordering::Less | Ordering::Equal => Some(a),
        Ordering::Greater => Some(b),
    }
}

/// Compare two StatScalar values and return the larger one.
///
/// Uses the Arrow DataType for correct comparison of unsigned integers,
/// which are stored as signed in Parquet but need unsigned comparison semantics.
fn scalar_max(a: StatScalar, b: StatScalar, data_type: &DataType) -> Option<StatScalar> {
    let ordering = compare_scalars(&a, &b, data_type)?;
    match ordering {
        Ordering::Greater | Ordering::Equal => Some(a),
        Ordering::Less => Some(b),
    }
}

/// Compare two StatScalar values using the logical DataType for correct semantics.
///
/// This handles the case where unsigned integers are stored as signed in Parquet
/// but need to be compared as unsigned values.
fn compare_scalars(a: &StatScalar, b: &StatScalar, data_type: &DataType) -> Option<Ordering> {
    match (a, b, data_type) {
        // Null handling
        (StatScalar::Null, StatScalar::Null, _) => Some(Ordering::Equal),
        (StatScalar::Null, _, _) => Some(Ordering::Less),
        (_, StatScalar::Null, _) => Some(Ordering::Greater),

        // Unsigned integers stored as Int32 - reinterpret bits as unsigned for comparison
        (StatScalar::Int32(av), StatScalar::Int32(bv), DataType::UInt32) => {
            (*av as u32).partial_cmp(&(*bv as u32))
        }
        (StatScalar::Int32(av), StatScalar::Int32(bv), DataType::UInt16) => {
            (*av as u16).partial_cmp(&(*bv as u16))
        }
        (StatScalar::Int32(av), StatScalar::Int32(bv), DataType::UInt8) => {
            (*av as u8).partial_cmp(&(*bv as u8))
        }

        // Unsigned integers stored as Int64 - reinterpret bits as unsigned for comparison
        (StatScalar::Int64(av), StatScalar::Int64(bv), DataType::UInt64) => {
            (*av as u64).partial_cmp(&(*bv as u64))
        }

        // For all other types, use physical comparison (which is correct)
        _ => a.partial_cmp(b),
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
    fn test_stat_scalar_comparison() {
        // Test integer comparison
        let a = StatScalar::Int32(10);
        let b = StatScalar::Int32(20);
        assert!(a < b);
        assert!(b > a);

        // Test binary comparison (covers string data)
        let s1 = StatScalar::Binary(b"apple".to_vec());
        let s2 = StatScalar::Binary(b"banana".to_vec());
        assert!(s1 < s2);

        // Test null comparison
        let null = StatScalar::Null;
        assert!(null < a);

        // Test incompatible types
        assert_eq!(a.partial_cmp(&s1), None);
    }

    #[test]
    fn test_column_statistics_merge() {
        // Test 1: Both stats have values - takes min of mins, max of maxs
        let mut stats1 = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::Int32,
            min_value: Some(StatScalar::Int32(10)),
            max_value: Some(StatScalar::Int32(50)),
            null_count: Some(5),
            distinct_count: Some(10),
        };
        let stats2 = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::Int32,
            min_value: Some(StatScalar::Int32(5)),
            max_value: Some(StatScalar::Int32(100)),
            null_count: Some(3),
            distinct_count: Some(20),
        };
        stats1.merge(&stats2);
        assert_eq!(stats1.min_value, Some(StatScalar::Int32(5)));
        assert_eq!(stats1.max_value, Some(StatScalar::Int32(100)));
        assert_eq!(stats1.null_count, Some(8));
        assert_eq!(stats1.distinct_count, None); // Cannot merge distinct counts

        // Test 2: One side has None values - preserves the Some value
        let mut stats3 = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::Int32,
            min_value: Some(StatScalar::Int32(10)),
            max_value: None,
            null_count: Some(5),
            distinct_count: None,
        };
        let stats4 = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::Int32,
            min_value: None,
            max_value: Some(StatScalar::Int32(100)),
            null_count: None,
            distinct_count: None,
        };
        stats3.merge(&stats4);
        assert_eq!(stats3.min_value, Some(StatScalar::Int32(10)));
        assert_eq!(stats3.max_value, Some(StatScalar::Int32(100)));
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn test_stat_scalar_to_datafusion() {
        use datafusion_common::ScalarValue;

        // Physical Int32 → logical Int32
        assert_eq!(
            to_scalar_value(&StatScalar::Int32(42), &DataType::Int32),
            ScalarValue::Int32(Some(42))
        );
        // Physical Int32 → logical Date32
        assert_eq!(
            to_scalar_value(&StatScalar::Int32(19000), &DataType::Date32),
            ScalarValue::Date32(Some(19000))
        );
        // Physical Binary → logical Utf8
        assert_eq!(
            to_scalar_value(&StatScalar::Binary(b"hello".to_vec()), &DataType::Utf8),
            ScalarValue::Utf8(Some("hello".to_string()))
        );
    }

    #[test]
    fn test_unsigned_integer_overflow_merge() {
        // UInt32: values > i32::MAX are stored as negative i32, need unsigned comparison
        let mut uint32_stats = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::UInt32,
            min_value: Some(StatScalar::Int32(100)),
            max_value: Some(StatScalar::Int32(200)),
            null_count: None,
            distinct_count: None,
        };
        let uint32_overflow = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::UInt32,
            min_value: Some(StatScalar::Int32(3_000_000_000u32 as i32)), // stored as negative
            max_value: Some(StatScalar::Int32(4_000_000_000u32 as i32)),
            null_count: None,
            distinct_count: None,
        };
        uint32_stats.merge(&uint32_overflow);
        assert_eq!(uint32_stats.min_value, Some(StatScalar::Int32(100))); // 100 < 3B
        assert_eq!(
            uint32_stats.max_value,
            Some(StatScalar::Int32(4_000_000_000u32 as i32))
        ); // 4B > 200

        // UInt64: same pattern for 64-bit overflow
        let large = StatScalar::Int64(10_000_000_000_000_000_000u64 as i64);
        let small = StatScalar::Int64(100);
        assert_eq!(
            scalar_min(large.clone(), small.clone(), &DataType::UInt64),
            Some(StatScalar::Int64(100))
        );
        assert_eq!(
            scalar_max(large.clone(), small.clone(), &DataType::UInt64),
            Some(large)
        );
    }
}
