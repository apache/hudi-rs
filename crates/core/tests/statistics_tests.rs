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
//! Integration tests for the statistics module.
//!
//! These tests generate Parquet files with known data, then verify that
//! the statistics extraction correctly reads min/max values, null counts, etc.

use std::fs::File;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt32Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use tempfile::tempdir;

use hudi_core::statistics::{StatScalar, StatsGranularity, StatisticsContainer};

/// Helper to write a RecordBatch to a Parquet file and return the path.
fn write_parquet_file(batch: &RecordBatch, path: &std::path::Path) {
    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

/// Helper to write multiple RecordBatches to a single Parquet file (multiple row groups).
fn write_parquet_file_multiple_row_groups(batches: &[RecordBatch], path: &std::path::Path) {
    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .set_max_row_group_size(3) // Force smaller row groups
        .build();
    let mut writer = ArrowWriter::try_new(file, batches[0].schema(), Some(props)).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.close().unwrap();
}

/// Helper to read Parquet metadata from a file.
fn read_parquet_metadata(
    path: &std::path::Path,
) -> parquet::file::metadata::ParquetMetaData {
    let file = File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    reader.metadata().clone()
}

mod file_level_statistics {
    use super::*;

    #[test]
    fn test_int32_column_stats() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("int32_test.parquet");

        // Create test data with known min/max
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
        let values = Int32Array::from(vec![10, 50, 30, 20, 40]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        // Read and verify statistics
        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        assert_eq!(stats.granularity, StatsGranularity::File);
        assert_eq!(stats.num_rows, Some(5));

        let col_stats = stats.columns.get("value").unwrap();
        assert_eq!(col_stats.min_value, Some(StatScalar::Int32(10)));
        assert_eq!(col_stats.max_value, Some(StatScalar::Int32(50)));
        assert_eq!(col_stats.null_count, Some(0));
    }

    #[test]
    fn test_int64_column_stats() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("int64_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
        let values = Int64Array::from(vec![100_i64, 500, 300, 200, 400]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let col_stats = stats.columns.get("value").unwrap();
        assert_eq!(col_stats.min_value, Some(StatScalar::Int64(100)));
        assert_eq!(col_stats.max_value, Some(StatScalar::Int64(500)));
    }

    #[test]
    fn test_int8_and_int16_column_stats() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("small_int_test.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("int8_col", DataType::Int8, false),
            Field::new("int16_col", DataType::Int16, false),
        ]));
        let int8_values = Int8Array::from(vec![-10_i8, 50, 30, -20, 40]);
        let int16_values = Int16Array::from(vec![100_i16, 500, -300, 200, 400]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(int8_values) as ArrayRef,
                Arc::new(int16_values) as ArrayRef,
            ],
        )
        .unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let int8_stats = stats.columns.get("int8_col").unwrap();
        assert_eq!(int8_stats.min_value, Some(StatScalar::Int32(-20)));
        assert_eq!(int8_stats.max_value, Some(StatScalar::Int32(50)));

        let int16_stats = stats.columns.get("int16_col").unwrap();
        assert_eq!(int16_stats.min_value, Some(StatScalar::Int32(-300)));
        assert_eq!(int16_stats.max_value, Some(StatScalar::Int32(500)));
    }

    #[test]
    fn test_float_column_stats() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("float_test.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("float32_col", DataType::Float32, false),
            Field::new("float64_col", DataType::Float64, false),
        ]));
        let float32_values = Float32Array::from(vec![1.5_f32, 3.5, 2.5, 0.5, 4.5]);
        let float64_values = Float64Array::from(vec![10.5_f64, 30.5, 20.5, 5.5, 40.5]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(float32_values) as ArrayRef,
                Arc::new(float64_values) as ArrayRef,
            ],
        )
        .unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let float32_stats = stats.columns.get("float32_col").unwrap();
        assert_eq!(float32_stats.min_value, Some(StatScalar::Float32(0.5)));
        assert_eq!(float32_stats.max_value, Some(StatScalar::Float32(4.5)));

        let float64_stats = stats.columns.get("float64_col").unwrap();
        assert_eq!(float64_stats.min_value, Some(StatScalar::Float64(5.5)));
        assert_eq!(float64_stats.max_value, Some(StatScalar::Float64(40.5)));
    }

    #[test]
    fn test_string_column_stats() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("string_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let names = StringArray::from(vec!["charlie", "alice", "bob", "diana", "eve"]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(names) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let col_stats = stats.columns.get("name").unwrap();
        assert_eq!(
            col_stats.min_value,
            Some(StatScalar::Binary(b"alice".to_vec()))
        );
        assert_eq!(
            col_stats.max_value,
            Some(StatScalar::Binary(b"eve".to_vec()))
        );
    }

    #[test]
    fn test_boolean_column_stats() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("bool_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "is_active",
            DataType::Boolean,
            false,
        )]));
        let values = BooleanArray::from(vec![true, false, true, false, true]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let col_stats = stats.columns.get("is_active").unwrap();
        assert_eq!(col_stats.min_value, Some(StatScalar::Boolean(false)));
        assert_eq!(col_stats.max_value, Some(StatScalar::Boolean(true)));
    }

    #[test]
    fn test_date32_column_stats() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("date32_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_date",
            DataType::Date32,
            false,
        )]));
        // Days since epoch: 19000 = ~2022-01-01, 19100 = ~2022-04-11, etc.
        let values = Date32Array::from(vec![19000, 19100, 19050, 18900, 19200]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let col_stats = stats.columns.get("event_date").unwrap();
        assert_eq!(col_stats.min_value, Some(StatScalar::Int32(18900)));
        assert_eq!(col_stats.max_value, Some(StatScalar::Int32(19200)));
    }

    #[test]
    fn test_timestamp_column_stats() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("timestamp_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));
        // Microseconds since epoch
        let values = TimestampMicrosecondArray::from(vec![
            1_000_000_i64,
            5_000_000,
            3_000_000,
            2_000_000,
            4_000_000,
        ]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let col_stats = stats.columns.get("event_time").unwrap();
        assert_eq!(
            col_stats.min_value,
            Some(StatScalar::Int64(1_000_000))
        );
        assert_eq!(
            col_stats.max_value,
            Some(StatScalar::Int64(5_000_000))
        );
    }

    #[test]
    fn test_uint32_column_stats() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("uint32_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt32,
            false,
        )]));
        let values = UInt32Array::from(vec![100_u32, 500, 300, 200, 400]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let col_stats = stats.columns.get("count").unwrap();
        assert_eq!(col_stats.min_value, Some(StatScalar::Int32(100)));
        assert_eq!(col_stats.max_value, Some(StatScalar::Int32(500)));
    }
}

mod null_handling {
    use super::*;

    #[test]
    fn test_nullable_column_with_nulls() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("nullable_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, true)]));
        let values = Int32Array::from(vec![Some(10), None, Some(30), None, Some(20)]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let col_stats = stats.columns.get("value").unwrap();
        assert_eq!(col_stats.min_value, Some(StatScalar::Int32(10)));
        assert_eq!(col_stats.max_value, Some(StatScalar::Int32(30)));
        assert_eq!(col_stats.null_count, Some(2));
    }

    #[test]
    fn test_nullable_string_with_nulls() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("nullable_string_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let names = StringArray::from(vec![
            Some("alice"),
            None,
            Some("charlie"),
            None,
            Some("bob"),
        ]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(names) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let col_stats = stats.columns.get("name").unwrap();
        assert_eq!(
            col_stats.min_value,
            Some(StatScalar::Binary(b"alice".to_vec()))
        );
        assert_eq!(
            col_stats.max_value,
            Some(StatScalar::Binary(b"charlie".to_vec()))
        );
        assert_eq!(col_stats.null_count, Some(2));
    }

    #[test]
    fn test_all_nulls_column() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("all_nulls_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, true)]));
        let values: Int32Array = vec![None, None, None].into_iter().collect();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let col_stats = stats.columns.get("value").unwrap();
        // For all nulls, min/max should be None (or the statistics may not have min/max set)
        assert_eq!(col_stats.null_count, Some(3));
    }
}

mod row_group_statistics {
    use super::*;

    #[test]
    fn test_single_row_group_stats() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("single_rg_test.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let values = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids) as ArrayRef, Arc::new(values) as ArrayRef],
        )
        .unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        assert_eq!(metadata.num_row_groups(), 1);

        let row_group = &metadata.row_groups()[0];
        let rg_stats = StatisticsContainer::from_row_group(row_group, &schema);

        assert_eq!(rg_stats.granularity, StatsGranularity::RowGroup);
        assert_eq!(rg_stats.num_rows, Some(5));

        let id_stats = rg_stats.columns.get("id").unwrap();
        assert_eq!(id_stats.min_value, Some(StatScalar::Int32(1)));
        assert_eq!(id_stats.max_value, Some(StatScalar::Int32(5)));

        let value_stats = rg_stats.columns.get("value").unwrap();
        assert_eq!(value_stats.min_value, Some(StatScalar::Float64(10.0)));
        assert_eq!(value_stats.max_value, Some(StatScalar::Float64(50.0)));
    }

    #[test]
    fn test_multiple_row_groups_aggregation() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("multi_rg_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, true)]));

        // Create batches that will span multiple row groups
        // Batch 1: values 100-110
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![100, 105, 110])) as ArrayRef],
        )
        .unwrap();

        // Batch 2: values 1-10 (includes global min)
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 5, 10])) as ArrayRef],
        )
        .unwrap();

        // Batch 3: values 200-220 (includes global max)
        let batch3 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![200, 210, 220])) as ArrayRef],
        )
        .unwrap();

        write_parquet_file_multiple_row_groups(&[batch1, batch2, batch3], &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);

        // Verify we have multiple row groups
        assert!(metadata.num_row_groups() >= 2, "Expected multiple row groups");

        // Get file-level stats (aggregated from row groups)
        let file_stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        assert_eq!(file_stats.granularity, StatsGranularity::File);
        assert_eq!(file_stats.num_rows, Some(9)); // 3 + 3 + 3

        let col_stats = file_stats.columns.get("value").unwrap();
        // File-level min should be global min across all row groups
        assert_eq!(col_stats.min_value, Some(StatScalar::Int32(1)));
        // File-level max should be global max across all row groups
        assert_eq!(col_stats.max_value, Some(StatScalar::Int32(220)));
    }

    #[test]
    fn test_row_group_with_nulls_aggregation() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("rg_nulls_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, true)]));

        // Batch with some nulls
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(10), None, Some(20)])) as ArrayRef],
        )
        .unwrap();

        // Batch with more nulls
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![None, Some(30), None])) as ArrayRef],
        )
        .unwrap();

        write_parquet_file_multiple_row_groups(&[batch1, batch2], &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let file_stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let col_stats = file_stats.columns.get("value").unwrap();
        assert_eq!(col_stats.min_value, Some(StatScalar::Int32(10)));
        assert_eq!(col_stats.max_value, Some(StatScalar::Int32(30)));
        // Null count should be aggregated across row groups
        assert_eq!(col_stats.null_count, Some(3)); // 1 + 2
    }
}

mod multiple_columns {
    use super::*;

    #[test]
    fn test_mixed_type_columns() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("mixed_types_test.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
            Field::new("is_active", DataType::Boolean, false),
        ]));

        let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let names = StringArray::from(vec!["alice", "bob", "charlie", "diana", "eve"]);
        let scores = Float64Array::from(vec![Some(85.5), Some(92.0), None, Some(78.5), Some(95.0)]);
        let is_active = BooleanArray::from(vec![true, false, true, true, false]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ids) as ArrayRef,
                Arc::new(names) as ArrayRef,
                Arc::new(scores) as ArrayRef,
                Arc::new(is_active) as ArrayRef,
            ],
        )
        .unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        // Verify all columns have statistics
        assert_eq!(stats.columns.len(), 4);

        // Check int32 column
        let id_stats = stats.columns.get("id").unwrap();
        assert_eq!(id_stats.min_value, Some(StatScalar::Int32(1)));
        assert_eq!(id_stats.max_value, Some(StatScalar::Int32(5)));
        assert_eq!(id_stats.null_count, Some(0));

        // Check string column
        let name_stats = stats.columns.get("name").unwrap();
        assert_eq!(
            name_stats.min_value,
            Some(StatScalar::Binary(b"alice".to_vec()))
        );
        assert_eq!(
            name_stats.max_value,
            Some(StatScalar::Binary(b"eve".to_vec()))
        );

        // Check float64 column with null
        let score_stats = stats.columns.get("score").unwrap();
        assert_eq!(score_stats.min_value, Some(StatScalar::Float64(78.5)));
        assert_eq!(score_stats.max_value, Some(StatScalar::Float64(95.0)));
        assert_eq!(score_stats.null_count, Some(1));

        // Check boolean column
        let active_stats = stats.columns.get("is_active").unwrap();
        assert_eq!(active_stats.min_value, Some(StatScalar::Boolean(false)));
        assert_eq!(active_stats.max_value, Some(StatScalar::Boolean(true)));
    }

    #[test]
    fn test_schema_column_ordering() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("ordering_test.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("z_col", DataType::Int32, false),
            Field::new("a_col", DataType::Int32, false),
            Field::new("m_col", DataType::Int32, false),
        ]));

        let z_values = Int32Array::from(vec![1, 2, 3]);
        let a_values = Int32Array::from(vec![10, 20, 30]);
        let m_values = Int32Array::from(vec![100, 200, 300]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(z_values) as ArrayRef,
                Arc::new(a_values) as ArrayRef,
                Arc::new(m_values) as ArrayRef,
            ],
        )
        .unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        // All columns should have stats regardless of their name ordering
        assert!(stats.columns.contains_key("z_col"));
        assert!(stats.columns.contains_key("a_col"));
        assert!(stats.columns.contains_key("m_col"));

        // Verify values
        let z_stats = stats.columns.get("z_col").unwrap();
        assert_eq!(z_stats.min_value, Some(StatScalar::Int32(1)));
        assert_eq!(z_stats.max_value, Some(StatScalar::Int32(3)));

        let a_stats = stats.columns.get("a_col").unwrap();
        assert_eq!(a_stats.min_value, Some(StatScalar::Int32(10)));
        assert_eq!(a_stats.max_value, Some(StatScalar::Int32(30)));

        let m_stats = stats.columns.get("m_col").unwrap();
        assert_eq!(m_stats.min_value, Some(StatScalar::Int32(100)));
        assert_eq!(m_stats.max_value, Some(StatScalar::Int32(300)));
    }
}

mod edge_cases {
    use super::*;

    #[test]
    fn test_single_row() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("single_row_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
        let values = Int32Array::from(vec![42]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        assert_eq!(stats.num_rows, Some(1));

        let col_stats = stats.columns.get("value").unwrap();
        // For single row, min == max
        assert_eq!(col_stats.min_value, Some(StatScalar::Int32(42)));
        assert_eq!(col_stats.max_value, Some(StatScalar::Int32(42)));
    }

    #[test]
    fn test_identical_values() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("identical_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
        let values = Int32Array::from(vec![100, 100, 100, 100, 100]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let col_stats = stats.columns.get("value").unwrap();
        // When all values are identical, min == max
        assert_eq!(col_stats.min_value, Some(StatScalar::Int32(100)));
        assert_eq!(col_stats.max_value, Some(StatScalar::Int32(100)));
    }

    #[test]
    fn test_negative_numbers() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("negative_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
        let values = Int32Array::from(vec![-100, -50, 0, 50, 100]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let col_stats = stats.columns.get("value").unwrap();
        assert_eq!(col_stats.min_value, Some(StatScalar::Int32(-100)));
        assert_eq!(col_stats.max_value, Some(StatScalar::Int32(100)));
    }

    #[test]
    fn test_empty_string() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("empty_string_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let names = StringArray::from(vec!["", "apple", "banana"]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(names) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let col_stats = stats.columns.get("name").unwrap();
        // Empty string should be the minimum (lexicographically smallest)
        assert_eq!(col_stats.min_value, Some(StatScalar::Binary(b"".to_vec())));
        assert_eq!(
            col_stats.max_value,
            Some(StatScalar::Binary(b"banana".to_vec()))
        );
    }

    #[test]
    fn test_special_float_values() {
        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("special_float_test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        // Test with various float values including very small and very large
        let values = Float64Array::from(vec![
            f64::MIN_POSITIVE,
            1.0,
            f64::MAX / 2.0,
            -f64::MAX / 2.0,
            0.0,
        ]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as ArrayRef]).unwrap();

        write_parquet_file(&batch, &parquet_path);

        let metadata = read_parquet_metadata(&parquet_path);
        let stats = StatisticsContainer::from_parquet_metadata(&metadata, &schema);

        let col_stats = stats.columns.get("value").unwrap();
        assert_eq!(
            col_stats.min_value,
            Some(StatScalar::Float64(-f64::MAX / 2.0))
        );
        assert_eq!(
            col_stats.max_value,
            Some(StatScalar::Float64(f64::MAX / 2.0))
        );
    }
}
