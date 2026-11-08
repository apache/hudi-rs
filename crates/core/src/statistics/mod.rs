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
//! Core types:
//! - [`ColumnStatistics`]: Per-column statistics (min, max) for range-based pruning
//! - [`StatisticsContainer`]: Container for all column statistics at a given granularity
//!
//! Min/max values are stored as single-element Arrow arrays (`ArrayRef`), enabling
//! direct comparison using `arrow_ord::cmp` functions without custom enum types.

pub(crate) mod estimator;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_ord::cmp;
use arrow_schema::{DataType, Schema, TimeUnit};
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
    #[default]
    File,

    /// Row group level stats (directly from Parquet footer).
    ///
    /// Balanced granularity, moderate memory.
    /// Can skip row groups within files.
    /// Stats are read directly from Parquet footer's ColumnChunkMetaData.
    RowGroup,
}

impl FromStr for StatsGranularity {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "file" => Ok(Self::File),
            "row_group" | "rowgroup" => Ok(Self::RowGroup),
            _ => Err(format!(
                "Invalid stats granularity: '{s}'. Valid options: file, row_group"
            )),
        }
    }
}

impl std::fmt::Display for StatsGranularity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File => write!(f, "file"),
            Self::RowGroup => write!(f, "row_group"),
        }
    }
}

/// Statistics for a single column at a given granularity.
///
/// Tracks min and max values from Parquet footer statistics for range-based pruning.
/// Values are stored as single-element Arrow arrays for direct comparison using `arrow_ord::cmp`.
#[derive(Clone, Debug)]
pub struct ColumnStatistics {
    /// Column name
    pub column_name: String,
    /// Arrow data type
    pub data_type: DataType,
    /// Minimum value (stored as a single-element Arrow array)
    pub min_value: Option<ArrayRef>,
    /// Maximum value (stored as a single-element Arrow array)
    pub max_value: Option<ArrayRef>,
}

impl ColumnStatistics {
    /// Create a new ColumnStatistics with the given column name and data type.
    pub fn new(column_name: String, data_type: DataType) -> Self {
        Self {
            column_name,
            data_type,
            min_value: None,
            max_value: None,
        }
    }

    /// Create from Parquet row group statistics.
    ///
    /// Extracts min/max as single-element Arrow arrays using the Arrow data_type
    /// for correct logical type representation.
    pub fn from_parquet_statistics(
        column_name: &str,
        data_type: &DataType,
        stats: &ParquetStatistics,
    ) -> Self {
        let (min_value, max_value) = parquet_stats_to_min_max_arrays(stats, data_type);

        Self {
            column_name: column_name.to_string(),
            data_type: data_type.clone(),
            min_value,
            max_value,
        }
    }

    /// Merge with another ColumnStatistics (for aggregation).
    ///
    /// Takes min of mins, max of maxs. Used when aggregating row group stats to
    /// file-level stats. A missing bound means the values on that side are unknown,
    /// so the merged bound is missing as well: a bound built from the other side alone
    /// would not bound the values the unknown side holds.
    pub fn merge(&mut self, other: &ColumnStatistics) {
        self.min_value = match (self.min_value.take(), &other.min_value) {
            (Some(a), Some(b)) => {
                if is_less_than(&a, b) {
                    Some(a)
                } else {
                    Some(Arc::clone(b))
                }
            }
            _ => None,
        };

        self.max_value = match (self.max_value.take(), &other.max_value) {
            (Some(a), Some(b)) => {
                if is_greater_than(&a, b) {
                    Some(a)
                } else {
                    Some(Arc::clone(b))
                }
            }
            _ => None,
        };
    }
}

/// Returns true if `a < b` using arrow-ord comparison.
fn is_less_than(a: &ArrayRef, b: &ArrayRef) -> bool {
    cmp::lt(a, b).map(|result| result.value(0)).unwrap_or(false)
}

/// Returns true if `a > b` using arrow-ord comparison.
fn is_greater_than(a: &ArrayRef, b: &ArrayRef) -> bool {
    cmp::gt(a, b).map(|result| result.value(0)).unwrap_or(false)
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

        // Get total rows directly from file metadata
        container.num_rows = Some(metadata.file_metadata().num_rows());

        // A row group holding only nulls has no min/max to contribute and cannot
        // widen the file-level bounds, so it is left out of the aggregation; a row
        // group whose bounds are unknown clears them through `merge`.
        for row_group in metadata.row_groups() {
            for (col_name, col_stats, all_null) in row_group_column_stats(row_group, schema) {
                if all_null {
                    continue;
                }
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
        for (col_name, col_stats, _) in row_group_column_stats(row_group, schema) {
            container.columns.insert(col_name, col_stats);
        }
        container
    }
}

/// Statistics of every top-level column chunk in `row_group` that `schema` knows,
/// with whether the chunk is known to hold only nulls.
fn row_group_column_stats<'a>(
    row_group: &'a RowGroupMetaData,
    schema: &'a Schema,
) -> impl Iterator<Item = (String, ColumnStatistics, bool)> + 'a {
    let column_types: HashMap<&str, &DataType> = schema
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();

    row_group.columns().iter().filter_map(move |col_chunk| {
        let col_path = col_chunk.column_descr().path();
        // Nested columns (multi-part paths like "struct.field") are not tracked.
        if col_path.parts().len() > 1 {
            return None;
        }
        let col_name = col_path.parts().first().map(|s| s.as_str())?;
        let data_type = *column_types.get(col_name)?;

        let (col_stats, all_null) = match col_chunk.statistics() {
            Some(stats) => (
                ColumnStatistics::from_parquet_statistics(col_name, data_type, stats),
                stats
                    .null_count_opt()
                    .is_some_and(|nulls| Some(nulls) == u64::try_from(col_chunk.num_values()).ok()),
            ),
            None => (
                ColumnStatistics::new(col_name.to_string(), data_type.clone()),
                false,
            ),
        };
        Some((col_name.to_string(), col_stats, all_null))
    })
}

/// Convert Parquet statistics to Arrow single-element arrays.
///
/// Uses the Arrow DataType to create appropriately typed arrays from Parquet
/// physical type statistics.
fn parquet_stats_to_min_max_arrays(
    stats: &ParquetStatistics,
    data_type: &DataType,
) -> (Option<ArrayRef>, Option<ArrayRef>) {
    match stats {
        ParquetStatistics::Boolean(s) => {
            let min = s
                .min_opt()
                .map(|v| Arc::new(BooleanArray::from(vec![*v])) as ArrayRef);
            let max = s
                .max_opt()
                .map(|v| Arc::new(BooleanArray::from(vec![*v])) as ArrayRef);
            (min, max)
        }
        ParquetStatistics::Int32(s) => {
            // Create arrays based on the Arrow logical type
            let min = s.min_opt().map(|v| int32_to_array(*v, data_type));
            let max = s.max_opt().map(|v| int32_to_array(*v, data_type));
            (min, max)
        }
        ParquetStatistics::Int64(s) => {
            let min = s.min_opt().map(|v| int64_to_array(*v, data_type));
            let max = s.max_opt().map(|v| int64_to_array(*v, data_type));
            (min, max)
        }
        ParquetStatistics::Int96(_) => {
            // Int96 is deprecated, typically used for timestamps in legacy Parquet
            log::debug!("Int96 statistics not supported - legacy Parquet timestamp format");
            (None, None)
        }
        ParquetStatistics::Float(s) => {
            let min = s
                .min_opt()
                .map(|v| Arc::new(Float32Array::from(vec![*v])) as ArrayRef);
            let max = s
                .max_opt()
                .map(|v| Arc::new(Float32Array::from(vec![*v])) as ArrayRef);
            (min, max)
        }
        ParquetStatistics::Double(s) => {
            let min = s
                .min_opt()
                .map(|v| Arc::new(Float64Array::from(vec![*v])) as ArrayRef);
            let max = s
                .max_opt()
                .map(|v| Arc::new(Float64Array::from(vec![*v])) as ArrayRef);
            (min, max)
        }
        ParquetStatistics::ByteArray(s) => {
            let min = s.min_opt().map(|b| bytes_to_array(b.data(), data_type));
            let max = s.max_opt().map(|b| bytes_to_array(b.data(), data_type));
            (min, max)
        }
        ParquetStatistics::FixedLenByteArray(s) => {
            let min = s.min_opt().map(|b| bytes_to_array(b.data(), data_type));
            let max = s.max_opt().map(|b| bytes_to_array(b.data(), data_type));
            (min, max)
        }
    }
}

/// Convert Parquet Int32 physical value to Arrow array based on logical type.
fn int32_to_array(value: i32, data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Int8 => Arc::new(Int8Array::from(vec![value as i8])) as ArrayRef,
        DataType::Int16 => Arc::new(Int16Array::from(vec![value as i16])) as ArrayRef,
        DataType::Int32 => Arc::new(Int32Array::from(vec![value])) as ArrayRef,
        DataType::UInt8 => Arc::new(UInt8Array::from(vec![value as u8])) as ArrayRef,
        DataType::UInt16 => Arc::new(UInt16Array::from(vec![value as u16])) as ArrayRef,
        DataType::UInt32 => Arc::new(UInt32Array::from(vec![value as u32])) as ArrayRef,
        DataType::Date32 => Arc::new(Date32Array::from(vec![value])) as ArrayRef,
        _ => Arc::new(Int32Array::from(vec![value])) as ArrayRef, // fallback
    }
}

/// Convert Parquet Int64 physical value to Arrow array based on logical type.
fn int64_to_array(value: i64, data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Int64 => Arc::new(Int64Array::from(vec![value])) as ArrayRef,
        DataType::UInt64 => Arc::new(UInt64Array::from(vec![value as u64])) as ArrayRef,
        DataType::Timestamp(time_unit, tz) => {
            match time_unit {
                TimeUnit::Second => {
                    Arc::new(TimestampSecondArray::from(vec![value]).with_timezone_opt(tz.clone()))
                        as ArrayRef
                }
                TimeUnit::Millisecond => Arc::new(
                    TimestampMillisecondArray::from(vec![value]).with_timezone_opt(tz.clone()),
                ) as ArrayRef,
                TimeUnit::Microsecond => Arc::new(
                    TimestampMicrosecondArray::from(vec![value]).with_timezone_opt(tz.clone()),
                ) as ArrayRef,
                TimeUnit::Nanosecond => Arc::new(
                    TimestampNanosecondArray::from(vec![value]).with_timezone_opt(tz.clone()),
                ) as ArrayRef,
            }
        }
        _ => Arc::new(Int64Array::from(vec![value])) as ArrayRef, // fallback
    }
}

/// Convert Parquet byte array to Arrow array based on logical type.
fn bytes_to_array(data: &[u8], data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let s = String::from_utf8_lossy(data).into_owned();
            Arc::new(StringArray::from(vec![s])) as ArrayRef
        }
        _ => Arc::new(BinaryArray::from_vec(vec![data])) as ArrayRef,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::Field;
    use parquet::basic::{Repetition, Type as PhysicalType};
    use parquet::file::metadata::{
        ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData,
    };
    use parquet::schema::types::{SchemaDescriptor, Type};

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
        assert!(StatsGranularity::from_str("invalid").is_err());
    }

    #[test]
    fn test_stats_granularity_display() {
        assert_eq!(format!("{}", StatsGranularity::File), "file");
        assert_eq!(format!("{}", StatsGranularity::RowGroup), "row_group");
    }

    #[test]
    fn test_stats_granularity_default() {
        assert_eq!(StatsGranularity::default(), StatsGranularity::File);
    }

    /// Helper to create a single-element Int32 array.
    fn int32_array(value: i32) -> ArrayRef {
        Arc::new(Int32Array::from(vec![value])) as ArrayRef
    }

    /// Helper to create a single-element UInt32 array.
    fn uint32_array(value: u32) -> ArrayRef {
        Arc::new(UInt32Array::from(vec![value])) as ArrayRef
    }

    /// Helper to check array value equality for Int32.
    fn get_int32(arr: &ArrayRef) -> i32 {
        arr.as_any().downcast_ref::<Int32Array>().unwrap().value(0)
    }

    /// Helper to check array value equality for UInt32.
    fn get_uint32(arr: &ArrayRef) -> u32 {
        arr.as_any().downcast_ref::<UInt32Array>().unwrap().value(0)
    }

    #[test]
    fn test_column_statistics_merge() {
        // Test 1: Both stats have values - takes min of mins, max of maxs
        let mut stats1 = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::Int32,
            min_value: Some(int32_array(10)),
            max_value: Some(int32_array(50)),
        };
        let stats2 = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::Int32,
            min_value: Some(int32_array(5)),
            max_value: Some(int32_array(100)),
        };
        stats1.merge(&stats2);
        assert_eq!(get_int32(stats1.min_value.as_ref().unwrap()), 5);
        assert_eq!(get_int32(stats1.max_value.as_ref().unwrap()), 100);

        // Test 2: a missing bound on either side is unknown and clears the merged bound
        let mut stats3 = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::Int32,
            min_value: Some(int32_array(10)),
            max_value: None,
        };
        let stats4 = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::Int32,
            min_value: None,
            max_value: Some(int32_array(100)),
        };
        stats3.merge(&stats4);
        assert!(stats3.min_value.is_none());
        assert!(stats3.max_value.is_none());
    }

    /// Arrow schema of the file built by [`id_file_metadata`].
    pub(crate) fn id_schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Int64, true)])
    }

    /// Footer of a file with one optional Int64 column `id` and one 10-row row group
    /// per entry of `row_group_stats`; `None` writes the chunk without statistics.
    pub(crate) fn id_file_metadata(
        row_group_stats: Vec<Option<ParquetStatistics>>,
    ) -> ParquetMetaData {
        let id = Type::primitive_type_builder("id", PhysicalType::INT64)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();
        let schema = Type::group_type_builder("schema")
            .with_fields(vec![Arc::new(id)])
            .build()
            .unwrap();
        let descr = Arc::new(SchemaDescriptor::new(Arc::new(schema)));

        let row_groups: Vec<RowGroupMetaData> = row_group_stats
            .into_iter()
            .map(|stats| {
                let mut chunk = ColumnChunkMetaData::builder(descr.column(0)).set_num_values(10);
                if let Some(stats) = stats {
                    chunk = chunk.set_statistics(stats);
                }
                RowGroupMetaData::builder(descr.clone())
                    .set_num_rows(10)
                    .set_column_metadata(vec![chunk.build().unwrap()])
                    .build()
                    .unwrap()
            })
            .collect();
        let num_rows = 10 * row_groups.len() as i64;
        ParquetMetaData::new(
            FileMetaData::new(1, num_rows, None, None, descr, None),
            row_groups,
        )
    }

    fn file_stats(row_group_stats: Vec<Option<ParquetStatistics>>) -> ColumnStatistics {
        StatisticsContainer::from_parquet_metadata(&id_file_metadata(row_group_stats), &id_schema())
            .columns
            .remove("id")
            .unwrap()
    }

    fn get_int64(arr: &ArrayRef) -> i64 {
        arr.as_any().downcast_ref::<Int64Array>().unwrap().value(0)
    }

    fn bounded_50_100() -> Option<ParquetStatistics> {
        Some(ParquetStatistics::int64(
            Some(50),
            Some(100),
            None,
            Some(0),
            false,
        ))
    }

    #[test]
    fn test_from_parquet_metadata_row_group_without_stats_clears_file_bounds() {
        let no_min_max = Some(ParquetStatistics::int64(None, None, None, Some(3), false));

        let stats = file_stats(vec![bounded_50_100(), None]);
        assert!(stats.min_value.is_none());
        assert!(stats.max_value.is_none());

        let stats = file_stats(vec![no_min_max, bounded_50_100()]);
        assert!(stats.min_value.is_none());
        assert!(stats.max_value.is_none());

        let no_null_count = Some(ParquetStatistics::int64(None, None, None, None, false));
        let stats = file_stats(vec![bounded_50_100(), no_null_count]);
        assert!(stats.min_value.is_none());
        assert!(stats.max_value.is_none());
    }

    #[test]
    fn test_from_parquet_metadata_all_null_row_group_keeps_file_bounds() {
        let all_null = Some(ParquetStatistics::int64(None, None, None, Some(10), false));

        let stats = file_stats(vec![all_null.clone(), bounded_50_100()]);
        assert_eq!(get_int64(stats.min_value.as_ref().unwrap()), 50);
        assert_eq!(get_int64(stats.max_value.as_ref().unwrap()), 100);

        // Only nulls in the file: nothing is known about non-null values.
        let stats = file_stats(vec![all_null]);
        assert!(stats.min_value.is_none());
        assert!(stats.max_value.is_none());
    }

    #[test]
    fn test_unsigned_integer_merge() {
        // Test that UInt32 values are correctly compared using unsigned semantics
        let mut uint32_stats = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::UInt32,
            min_value: Some(uint32_array(100)),
            max_value: Some(uint32_array(200)),
        };
        let uint32_large = ColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::UInt32,
            min_value: Some(uint32_array(3_000_000_000)),
            max_value: Some(uint32_array(4_000_000_000)),
        };
        uint32_stats.merge(&uint32_large);
        // 100 < 3B in unsigned comparison
        assert_eq!(get_uint32(uint32_stats.min_value.as_ref().unwrap()), 100);
        // 4B > 200 in unsigned comparison
        assert_eq!(
            get_uint32(uint32_stats.max_value.as_ref().unwrap()),
            4_000_000_000
        );
    }

    #[test]
    fn test_arrow_ord_comparisons() {
        // Test arrow-ord comparisons directly
        let a = int32_array(10);
        let b = int32_array(20);

        assert!(is_less_than(&a, &b));
        assert!(!is_less_than(&b, &a));
        assert!(is_greater_than(&b, &a));
        assert!(!is_greater_than(&a, &b));

        // Test unsigned comparisons
        let small = uint32_array(100);
        let large = uint32_array(3_000_000_000);
        assert!(is_less_than(&small, &large));
        assert!(is_greater_than(&large, &small));
    }
}
