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

//! Statistics conversion utilities for DataFusion integration.
//!
//! This module provides functions to convert Hudi's internal statistics types
//! to DataFusion's statistics types for query optimization.

use std::collections::HashMap;

use arrow_array::ArrayRef;
use arrow_schema::Schema;
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics as DFColumnStatistics, ScalarValue, Statistics};
use hudi_core::statistics::ColumnStatistics as HudiColumnStatistics;
#[cfg(test)]
use hudi_core::statistics::StatisticsContainer;
use hudi_core::storage::file_metadata::FileMetadata;

/// Convert Hudi StatisticsContainer to DataFusion Statistics.
///
/// This function maps Hudi's internal statistics representation to DataFusion's
/// statistics types, enabling the query optimizer to use file-level statistics
/// for pruning and cardinality estimation.
#[cfg(test)]
fn hudi_stats_to_datafusion_stats(
    hudi_stats: &StatisticsContainer,
    schema: &Schema,
    file_size: Option<u64>,
) -> Statistics {
    let num_rows = hudi_stats
        .num_rows
        .map(|n| Precision::Exact(n as usize))
        .unwrap_or(Precision::Absent);

    let total_byte_size = file_size
        .map(|s| Precision::Exact(s as usize))
        .unwrap_or(Precision::Absent);

    let column_statistics: Vec<DFColumnStatistics> = schema
        .fields()
        .iter()
        .map(|field| {
            hudi_stats
                .columns
                .get(field.name())
                .map(convert_column_statistics)
                .unwrap_or_else(DFColumnStatistics::new_unknown)
        })
        .collect();

    Statistics {
        num_rows,
        total_byte_size,
        column_statistics,
    }
}

/// Convert FileMetadata to DataFusion Statistics.
///
/// This function converts Hudi's FileMetadata (containing file-level metadata
/// and optional column statistics) to DataFusion Statistics for query optimization.
///
/// # Arguments
/// * `metadata` - The FileMetadata from a base file
/// * `schema` - The table schema (used to order column statistics)
///
/// # Returns
/// DataFusion Statistics with num_rows, total_byte_size (using byte_size for memory
/// planning), and per-column statistics if available.
pub fn file_metadata_to_datafusion_stats(metadata: &FileMetadata, schema: &Schema) -> Statistics {
    let num_rows = if metadata.num_records > 0 {
        Precision::Exact(metadata.num_records as usize)
    } else {
        Precision::Absent
    };

    // Use byte_size (uncompressed) for DataFusion's total_byte_size - better for memory planning
    let total_byte_size = if metadata.byte_size > 0 {
        Precision::Exact(metadata.byte_size as usize)
    } else {
        Precision::Absent
    };

    let column_statistics = column_stats_to_datafusion(
        metadata.column_statistics.as_ref(),
        schema,
    );

    Statistics {
        num_rows,
        total_byte_size,
        column_statistics,
    }
}

/// Convert column statistics map to DataFusion column statistics vector.
fn column_stats_to_datafusion(
    column_stats: Option<&HashMap<String, HudiColumnStatistics>>,
    schema: &Schema,
) -> Vec<DFColumnStatistics> {
    schema
        .fields()
        .iter()
        .map(|field| {
            column_stats
                .and_then(|cols| cols.get(field.name()))
                .map(convert_column_statistics)
                .unwrap_or_else(DFColumnStatistics::new_unknown)
        })
        .collect()
}

/// Convert Hudi ColumnStatistics to DataFusion ColumnStatistics.
fn convert_column_statistics(hudi_stats: &HudiColumnStatistics) -> DFColumnStatistics {
    let min_value = hudi_stats
        .min_value
        .as_ref()
        .and_then(|arr| scalar_from_array(arr))
        .map(Precision::Exact)
        .unwrap_or(Precision::Absent);

    let max_value = hudi_stats
        .max_value
        .as_ref()
        .and_then(|arr| scalar_from_array(arr))
        .map(Precision::Exact)
        .unwrap_or(Precision::Absent);

    DFColumnStatistics::new_unknown()
        .with_min_value(min_value)
        .with_max_value(max_value)
}

/// Convert a single-element Arrow array to a ScalarValue.
///
/// This handles the conversion from Hudi's array-based min/max storage
/// to DataFusion's ScalarValue representation.
fn scalar_from_array(arr: &ArrayRef) -> Option<ScalarValue> {
    if arr.is_empty() || arr.is_null(0) {
        return None;
    }
    ScalarValue::try_from_array(arr, 0).ok()
}

/// Merge multiple Statistics into a single aggregated Statistics.
///
/// This is used to compute table-level statistics by aggregating
/// file-level statistics from multiple files.
pub fn merge_statistics(stats_list: &[Statistics], schema: &Schema) -> Statistics {
    if stats_list.is_empty() {
        return Statistics::new_unknown(schema);
    }

    Statistics::try_merge_iter(stats_list, schema).unwrap_or_else(|_| Statistics::new_unknown(schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ])
    }

    #[test]
    fn test_scalar_from_array_int32() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![42]));
        let scalar = scalar_from_array(&arr);
        assert!(scalar.is_some());
        assert_eq!(scalar.unwrap(), ScalarValue::Int32(Some(42)));
    }

    #[test]
    fn test_scalar_from_array_int64() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![123456789i64]));
        let scalar = scalar_from_array(&arr);
        assert!(scalar.is_some());
        assert_eq!(scalar.unwrap(), ScalarValue::Int64(Some(123456789)));
    }

    #[test]
    fn test_scalar_from_array_string() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["hello"]));
        let scalar = scalar_from_array(&arr);
        assert!(scalar.is_some());
        assert_eq!(
            scalar.unwrap(),
            ScalarValue::Utf8(Some("hello".to_string()))
        );
    }

    #[test]
    fn test_scalar_from_array_empty() {
        let arr: ArrayRef = Arc::new(Int32Array::from(Vec::<i32>::new()));
        let scalar = scalar_from_array(&arr);
        assert!(scalar.is_none());
    }

    #[test]
    fn test_scalar_from_array_null() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![None]));
        let scalar = scalar_from_array(&arr);
        assert!(scalar.is_none());
    }

    #[test]
    fn test_convert_column_statistics() {
        let hudi_stats = HudiColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::Int32,
            min_value: Some(Arc::new(Int32Array::from(vec![10])) as ArrayRef),
            max_value: Some(Arc::new(Int32Array::from(vec![100])) as ArrayRef),
        };

        let df_stats = convert_column_statistics(&hudi_stats);

        assert_eq!(
            df_stats.min_value,
            Precision::Exact(ScalarValue::Int32(Some(10)))
        );
        assert_eq!(
            df_stats.max_value,
            Precision::Exact(ScalarValue::Int32(Some(100)))
        );
        assert_eq!(df_stats.null_count, Precision::Absent);
        assert_eq!(df_stats.distinct_count, Precision::Absent);
    }

    #[test]
    fn test_convert_column_statistics_no_values() {
        let hudi_stats = HudiColumnStatistics {
            column_name: "test".to_string(),
            data_type: DataType::Int32,
            min_value: None,
            max_value: None,
        };

        let df_stats = convert_column_statistics(&hudi_stats);

        assert_eq!(df_stats.min_value, Precision::Absent);
        assert_eq!(df_stats.max_value, Precision::Absent);
    }

    #[test]
    fn test_hudi_stats_to_datafusion_stats() {
        let schema = create_test_schema();

        let mut columns = HashMap::new();
        columns.insert(
            "id".to_string(),
            HudiColumnStatistics {
                column_name: "id".to_string(),
                data_type: DataType::Int32,
                min_value: Some(Arc::new(Int32Array::from(vec![1])) as ArrayRef),
                max_value: Some(Arc::new(Int32Array::from(vec![1000])) as ArrayRef),
            },
        );
        columns.insert(
            "value".to_string(),
            HudiColumnStatistics {
                column_name: "value".to_string(),
                data_type: DataType::Int64,
                min_value: Some(Arc::new(Int64Array::from(vec![0i64])) as ArrayRef),
                max_value: Some(Arc::new(Int64Array::from(vec![999999i64])) as ArrayRef),
            },
        );

        let hudi_stats = StatisticsContainer {
            granularity: hudi_core::statistics::StatsGranularity::File,
            num_rows: Some(500),
            columns,
        };

        let df_stats = hudi_stats_to_datafusion_stats(&hudi_stats, &schema, Some(1024));

        assert_eq!(df_stats.num_rows, Precision::Exact(500));
        assert_eq!(df_stats.total_byte_size, Precision::Exact(1024));
        assert_eq!(df_stats.column_statistics.len(), 3);

        // Check "id" column (index 0)
        assert_eq!(
            df_stats.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int32(Some(1)))
        );
        assert_eq!(
            df_stats.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(1000)))
        );

        // Check "name" column (index 1) - no stats provided
        assert_eq!(df_stats.column_statistics[1].min_value, Precision::Absent);
        assert_eq!(df_stats.column_statistics[1].max_value, Precision::Absent);

        // Check "value" column (index 2)
        assert_eq!(
            df_stats.column_statistics[2].min_value,
            Precision::Exact(ScalarValue::Int64(Some(0)))
        );
        assert_eq!(
            df_stats.column_statistics[2].max_value,
            Precision::Exact(ScalarValue::Int64(Some(999999)))
        );
    }

    #[test]
    fn test_hudi_stats_to_datafusion_stats_no_rows() {
        let schema = create_test_schema();

        let hudi_stats = StatisticsContainer {
            granularity: hudi_core::statistics::StatsGranularity::File,
            num_rows: None,
            columns: HashMap::new(),
        };

        let df_stats = hudi_stats_to_datafusion_stats(&hudi_stats, &schema, None);

        assert_eq!(df_stats.num_rows, Precision::Absent);
        assert_eq!(df_stats.total_byte_size, Precision::Absent);
        assert_eq!(df_stats.column_statistics.len(), 3);
    }

    #[test]
    fn test_merge_statistics_empty() {
        let schema = create_test_schema();
        let merged = merge_statistics(&[], &schema);

        assert_eq!(merged.num_rows, Precision::Absent);
        assert_eq!(merged.column_statistics.len(), 3);
    }

    #[test]
    fn test_merge_statistics_single() {
        let schema = create_test_schema();
        let stats = Statistics::default()
            .with_num_rows(Precision::Exact(100))
            .with_total_byte_size(Precision::Exact(1000));

        let merged = merge_statistics(&[stats.clone()], &schema);

        assert_eq!(merged.num_rows, Precision::Exact(100));
        assert_eq!(merged.total_byte_size, Precision::Exact(1000));
    }

    #[test]
    fn test_merge_statistics_multiple() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let stats1 = Statistics::default()
            .with_num_rows(Precision::Exact(100))
            .with_total_byte_size(Precision::Exact(1000))
            .add_column_statistics(
                DFColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(1))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(50)))),
            );

        let stats2 = Statistics::default()
            .with_num_rows(Precision::Exact(200))
            .with_total_byte_size(Precision::Exact(2000))
            .add_column_statistics(
                DFColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(25))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(100)))),
            );

        let merged = merge_statistics(&[stats1, stats2], &schema);

        assert_eq!(merged.num_rows, Precision::Exact(300));
        assert_eq!(merged.total_byte_size, Precision::Exact(3000));
        assert_eq!(
            merged.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int32(Some(1)))
        );
        assert_eq!(
            merged.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(100)))
        );
    }
}
