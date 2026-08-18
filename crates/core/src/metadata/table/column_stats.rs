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
//! Extract column-range metadata from Parquet footers for MDT writes.

use std::collections::HashMap;

use arrow_schema::{DataType, Schema, TimeUnit};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::statistics::Statistics as ParquetStatistics;

use crate::Result;
use crate::error::CoreError;
use crate::metadata::meta_field::MetaField;
use crate::metadata::table::encode::{ColumnStatValue, ColumnStatsMetadata};

/// Per-column range extracted from a written Parquet base file.
#[derive(Debug, Clone)]
pub struct ColumnRangeStats {
    pub column_name: String,
    pub min_value: Option<ColumnStatValue>,
    pub max_value: Option<ColumnStatValue>,
    pub value_count: i64,
    pub null_count: i64,
    pub total_size: i64,
    pub total_uncompressed_size: i64,
}

/// Read file-level column ranges from in-memory Parquet bytes (Java `ParquetUtils`).
///
/// Covers exactly [`columns_to_index`]: the three always-indexed meta fields plus
/// the first 32 supported data fields. Nested (multi-part path) columns and types
/// Spark does not index (bytes/fixed/decimal) are omitted rather than mis-encoded.
pub fn column_ranges_from_parquet_bytes(bytes: &[u8]) -> Result<Vec<ColumnRangeStats>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .map_err(|e| CoreError::Write(format!("Failed to open Parquet for column stats: {e}")))?;
    let schema = builder.schema().clone();
    let metadata = builder.metadata().clone();
    Ok(column_ranges_from_parquet_metadata(
        metadata.as_ref(),
        schema.as_ref(),
    ))
}

fn column_ranges_from_parquet_metadata(
    metadata: &parquet::file::metadata::ParquetMetaData,
    schema: &Schema,
) -> Vec<ColumnRangeStats> {
    let type_by_name: HashMap<&str, &DataType> = schema
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();
    let indexed_columns: std::collections::HashSet<String> =
        columns_to_index(schema).into_iter().collect();

    #[derive(Default)]
    struct Acc {
        min: Option<ColumnStatValue>,
        max: Option<ColumnStatValue>,
        value_count: i64,
        null_count: i64,
        total_size: i64,
        total_uncompressed_size: i64,
        data_type: Option<DataType>,
    }

    let mut by_col: HashMap<String, Acc> = HashMap::new();
    for row_group in metadata.row_groups() {
        for col_chunk in row_group.columns() {
            let path = col_chunk.column_descr().path();
            if path.parts().len() != 1 {
                continue;
            }
            let Some(col_name) = path.parts().first().map(|s| s.as_str()) else {
                continue;
            };
            if !indexed_columns.contains(col_name) {
                continue;
            }
            let Some(&data_type) = type_by_name.get(col_name) else {
                continue;
            };

            let acc = by_col.entry(col_name.to_string()).or_default();
            acc.data_type = Some(data_type.clone());
            acc.value_count += col_chunk.num_values();
            acc.total_size += col_chunk.compressed_size();
            acc.total_uncompressed_size += col_chunk.uncompressed_size();

            if let Some(stats) = col_chunk.statistics() {
                let nulls = if stats_is_empty(stats) {
                    col_chunk.num_values()
                } else {
                    stats.null_count_opt().unwrap_or(0) as i64
                };
                acc.null_count += nulls;
                if let Some((min, max)) = parquet_min_max_to_stat_values(stats, data_type) {
                    acc.min = merge_min(acc.min.take(), Some(min));
                    acc.max = merge_max(acc.max.take(), Some(max));
                }
            } else {
                acc.null_count += col_chunk.num_values();
            }
        }
    }

    let mut out: Vec<ColumnRangeStats> = by_col
        .into_iter()
        .map(|(column_name, acc)| ColumnRangeStats {
            column_name,
            min_value: acc.min,
            max_value: acc.max,
            value_count: acc.value_count,
            null_count: acc.null_count,
            total_size: acc.total_size,
            total_uncompressed_size: acc.total_uncompressed_size,
        })
        .collect();
    out.sort_by(|a, b| a.column_name.cmp(&b.column_name));
    out
}

/// Build MDT column_stats payloads for a written (or deleted) file.
pub fn column_stats_for_file(
    file_name: &str,
    ranges: &[ColumnRangeStats],
    is_deleted: bool,
) -> Vec<ColumnStatsMetadata> {
    ranges
        .iter()
        .map(|r| ColumnStatsMetadata {
            file_name: file_name.to_string(),
            column_name: r.column_name.clone(),
            min_value: if is_deleted {
                None
            } else {
                r.min_value.clone()
            },
            max_value: if is_deleted {
                None
            } else {
                r.max_value.clone()
            },
            value_count: if is_deleted { 0 } else { r.value_count },
            null_count: if is_deleted { 0 } else { r.null_count },
            total_size: if is_deleted { 0 } else { r.total_size },
            total_uncompressed_size: if is_deleted {
                0
            } else {
                r.total_uncompressed_size
            },
            is_deleted,
            is_tight_bound: false,
            decoded_value_type_ordinal: None,
        })
        .collect()
}

/// Tombstone column_stats for a deleted file when footer ranges are unavailable.
pub fn column_stats_tombstones(
    file_name: &str,
    column_names: &[String],
) -> Vec<ColumnStatsMetadata> {
    column_names
        .iter()
        .map(|column_name| ColumnStatsMetadata {
            file_name: file_name.to_string(),
            column_name: column_name.clone(),
            min_value: None,
            max_value: None,
            value_count: 0,
            null_count: 0,
            total_size: 0,
            total_uncompressed_size: 0,
            is_deleted: true,
            is_tight_bound: false,
            decoded_value_type_ordinal: None,
        })
        .collect()
}

/// Aggregate file-level ranges into partition-level stats (same column name).
pub fn aggregate_partition_stats(
    partition_path: &str,
    ranges_by_file: &[Vec<ColumnRangeStats>],
    is_tight_bound: bool,
) -> Vec<ColumnStatsMetadata> {
    #[derive(Default)]
    struct Acc {
        min: Option<ColumnStatValue>,
        max: Option<ColumnStatValue>,
        value_count: i64,
        null_count: i64,
        total_size: i64,
        total_uncompressed_size: i64,
    }
    let mut by_col: HashMap<String, Acc> = HashMap::new();
    for ranges in ranges_by_file {
        for r in ranges {
            let acc = by_col.entry(r.column_name.clone()).or_default();
            acc.min = merge_min(acc.min.take(), r.min_value.clone());
            acc.max = merge_max(acc.max.take(), r.max_value.clone());
            acc.value_count += r.value_count;
            acc.null_count += r.null_count;
            acc.total_size += r.total_size;
            acc.total_uncompressed_size += r.total_uncompressed_size;
        }
    }
    // Java partition_stats stores the partition path in fileName.
    let file_name = if partition_path.is_empty() {
        ".".to_string()
    } else {
        partition_path.to_string()
    };
    let mut out: Vec<ColumnStatsMetadata> = by_col
        .into_iter()
        .map(|(column_name, acc)| ColumnStatsMetadata {
            file_name: file_name.clone(),
            column_name,
            min_value: acc.min,
            max_value: acc.max,
            value_count: acc.value_count,
            null_count: acc.null_count,
            total_size: acc.total_size,
            total_uncompressed_size: acc.total_uncompressed_size,
            is_deleted: false,
            is_tight_bound,
            decoded_value_type_ordinal: None,
        })
        .collect();
    out.sort_by(|a, b| a.column_name.cmp(&b.column_name));
    out
}

/// Java `HoodieMetadataConfig.COLUMN_STATS_INDEX_MAX_COLUMNS` default: only the
/// first 32 supported data fields (schema order) are indexed.
pub const MAX_COLUMNS_TO_INDEX: usize = 32;

/// Columns eligible for MDT indexing from a table/data schema.
///
/// Java `HoodieTableMetadataUtil.getColumnsToIndex`: the three always-indexed
/// meta fields (when present, i.e. `populateMetaFields`) plus the first
/// [`MAX_COLUMNS_TO_INDEX`] supported non-meta fields in schema order.
pub fn columns_to_index(schema: &Schema) -> Vec<String> {
    // Java META_COLS_TO_ALWAYS_INDEX_SCHEMA_MAP is a TreeMap: alphabetical order.
    let mut cols: Vec<String> = [
        MetaField::CommitTime,
        MetaField::PartitionPath,
        MetaField::RecordKey,
    ]
    .iter()
    .map(|m| m.as_ref().to_string())
    .filter(|name| schema.fields().iter().any(|f| f.name() == name))
    .collect();
    cols.extend(
        schema
            .fields()
            .iter()
            .filter(|f| !MetaField::is_meta_field(f.name()) && is_indexable_type(f.data_type()))
            .take(MAX_COLUMNS_TO_INDEX)
            .map(|f| f.name().clone()),
    );
    cols
}

/// Java `isColumnTypeSupported` for the SPARK record type: nested types, enums,
/// raw bytes/fixed — and hence decimals, stored as bytes/fixed — are not indexed.
fn is_indexable_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Date32
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Timestamp(_, _)
    )
}

fn stats_is_empty(stats: &ParquetStatistics) -> bool {
    // parquet-rs: missing min/max with null_count == num values → empty-ish.
    match stats {
        ParquetStatistics::Boolean(s) => s.min_opt().is_none() && s.max_opt().is_none(),
        ParquetStatistics::Int32(s) => s.min_opt().is_none() && s.max_opt().is_none(),
        ParquetStatistics::Int64(s) => s.min_opt().is_none() && s.max_opt().is_none(),
        ParquetStatistics::Int96(_) => true,
        ParquetStatistics::Float(s) => s.min_opt().is_none() && s.max_opt().is_none(),
        ParquetStatistics::Double(s) => s.min_opt().is_none() && s.max_opt().is_none(),
        ParquetStatistics::ByteArray(s) => s.min_opt().is_none() && s.max_opt().is_none(),
        ParquetStatistics::FixedLenByteArray(s) => s.min_opt().is_none() && s.max_opt().is_none(),
    }
}

fn parquet_min_max_to_stat_values(
    stats: &ParquetStatistics,
    data_type: &DataType,
) -> Option<(ColumnStatValue, ColumnStatValue)> {
    match (stats, data_type) {
        (ParquetStatistics::Boolean(s), DataType::Boolean) => Some((
            ColumnStatValue::Boolean(*s.min_opt()?),
            ColumnStatValue::Boolean(*s.max_opt()?),
        )),
        (ParquetStatistics::Int32(s), DataType::Date32) => Some((
            ColumnStatValue::Date(*s.min_opt()?),
            ColumnStatValue::Date(*s.max_opt()?),
        )),
        (ParquetStatistics::Int32(s), _) => Some((
            ColumnStatValue::Int(*s.min_opt()?),
            ColumnStatValue::Int(*s.max_opt()?),
        )),
        (ParquetStatistics::Int64(s), DataType::Timestamp(unit, tz)) => {
            let min = to_micros(*s.min_opt()?, *unit)?;
            let max = to_micros(*s.max_opt()?, *unit)?;
            // tz-less arrow timestamps are Java LOCAL_TIMESTAMP_* semantics.
            if tz.is_some() {
                Some((
                    ColumnStatValue::TimestampMicros(min),
                    ColumnStatValue::TimestampMicros(max),
                ))
            } else {
                Some((
                    ColumnStatValue::LocalTimestampMicros(min),
                    ColumnStatValue::LocalTimestampMicros(max),
                ))
            }
        }
        (ParquetStatistics::Int64(s), DataType::Time64(TimeUnit::Microsecond)) => Some((
            ColumnStatValue::TimeMicros(*s.min_opt()?),
            ColumnStatValue::TimeMicros(*s.max_opt()?),
        )),
        (ParquetStatistics::Int64(s), _) => Some((
            ColumnStatValue::Long(*s.min_opt()?),
            ColumnStatValue::Long(*s.max_opt()?),
        )),
        (ParquetStatistics::Float(s), _) => Some((
            ColumnStatValue::Float(*s.min_opt()?),
            ColumnStatValue::Float(*s.max_opt()?),
        )),
        (ParquetStatistics::Double(s), _) => Some((
            ColumnStatValue::Double(*s.min_opt()?),
            ColumnStatValue::Double(*s.max_opt()?),
        )),
        (ParquetStatistics::ByteArray(s), DataType::Utf8 | DataType::LargeUtf8) => {
            let min = String::from_utf8_lossy(s.min_opt()?.data()).into_owned();
            let max = String::from_utf8_lossy(s.max_opt()?.data()).into_owned();
            Some((ColumnStatValue::String(min), ColumnStatValue::String(max)))
        }
        (ParquetStatistics::ByteArray(s), _) => Some((
            ColumnStatValue::Bytes(s.min_opt()?.data().to_vec()),
            ColumnStatValue::Bytes(s.max_opt()?.data().to_vec()),
        )),
        (ParquetStatistics::FixedLenByteArray(s), _) => Some((
            ColumnStatValue::Bytes(s.min_opt()?.data().to_vec()),
            ColumnStatValue::Bytes(s.max_opt()?.data().to_vec()),
        )),
        _ => None,
    }
}

fn to_micros(value: i64, unit: TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => value.checked_mul(1_000_000),
        TimeUnit::Millisecond => value.checked_mul(1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond => Some(value / 1_000),
    }
}

fn merge_min(a: Option<ColumnStatValue>, b: Option<ColumnStatValue>) -> Option<ColumnStatValue> {
    match (a, b) {
        (Some(x), Some(y)) => Some(if x <= y { x } else { y }),
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (None, None) => None,
    }
}

fn merge_max(a: Option<ColumnStatValue>, b: Option<ColumnStatValue>) -> Option<ColumnStatValue> {
    match (a, b) {
        (Some(x), Some(y)) => Some(if x >= y { x } else { y }),
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (None, None) => None,
    }
}

/// A decoded stat value as a single-element Arrow array of `target` type.
///
/// Builds the value in its natural Arrow type, then casts to the schema's
/// column type; `None` when the cast is not possible (callers treat that as
/// "no stats" and include the file — the conservative direction).
pub(crate) fn stat_value_to_array(
    value: &ColumnStatValue,
    target: &arrow_schema::DataType,
) -> Option<arrow_array::ArrayRef> {
    use arrow_array::ArrayRef;
    use std::sync::Arc;
    let natural: ArrayRef = match value {
        ColumnStatValue::Boolean(v) => Arc::new(arrow_array::BooleanArray::from(vec![*v])),
        ColumnStatValue::Int(v) => Arc::new(arrow_array::Int32Array::from(vec![*v])),
        ColumnStatValue::Long(v) => Arc::new(arrow_array::Int64Array::from(vec![*v])),
        ColumnStatValue::Float(v) => Arc::new(arrow_array::Float32Array::from(vec![*v])),
        ColumnStatValue::Double(v) => Arc::new(arrow_array::Float64Array::from(vec![*v])),
        ColumnStatValue::Bytes(v) => Arc::new(arrow_array::BinaryArray::from(vec![v.as_slice()])),
        ColumnStatValue::String(v) => Arc::new(arrow_array::StringArray::from(vec![v.as_str()])),
        ColumnStatValue::Date(v) => Arc::new(arrow_array::Date32Array::from(vec![*v])),
        ColumnStatValue::TimeMicros(v) => {
            Arc::new(arrow_array::Time64MicrosecondArray::from(vec![*v]))
        }
        ColumnStatValue::TimestampMicros(v) => {
            Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![*v]).with_timezone_utc())
        }
        ColumnStatValue::LocalTimestampMicros(v) => {
            Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![*v]))
        }
    };
    if natural.data_type() == target {
        return Some(natural);
    }
    arrow::compute::cast(&natural, target).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;

    #[test]
    fn test_columns_to_index_meta_fields_and_first_32_cap() {
        let mut fields = vec![
            Field::new("_hoodie_commit_time", DataType::Utf8, false),
            Field::new("_hoodie_commit_seqno", DataType::Utf8, false),
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("_hoodie_partition_path", DataType::Utf8, false),
            Field::new("_hoodie_file_name", DataType::Utf8, false),
        ];
        for i in 0..40 {
            fields.push(Field::new(format!("col{i:02}"), DataType::Int64, false));
        }
        let schema = Schema::new(fields);
        let cols = columns_to_index(&schema);

        // Three always-indexed meta fields in Java's TreeMap order, then the
        // first 32 data fields in schema order.
        assert_eq!(
            &cols[..3],
            &[
                "_hoodie_commit_time",
                "_hoodie_partition_path",
                "_hoodie_record_key"
            ]
        );
        assert_eq!(cols.len(), 3 + MAX_COLUMNS_TO_INDEX);
        assert_eq!(cols[3], "col00");
        assert_eq!(cols[3 + MAX_COLUMNS_TO_INDEX - 1], "col31");
        assert!(!cols.iter().any(|c| c == "col32"));
        assert!(!cols.iter().any(|c| c == "_hoodie_commit_seqno"));
        assert!(!cols.iter().any(|c| c == "_hoodie_file_name"));
    }

    #[test]
    fn test_columns_to_index_skips_unsupported_types() {
        let schema = Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            // Spark record type never indexes bytes/fixed/decimal columns.
            Field::new("b", DataType::Binary, false),
            Field::new("d", DataType::Decimal128(10, 2), false),
            Field::new(
                "nested",
                DataType::Struct(vec![Field::new("x", DataType::Int64, false)].into()),
                false,
            ),
        ]);
        assert_eq!(columns_to_index(&schema), vec!["s".to_string()]);
    }
}
