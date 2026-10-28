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
use crate::metadata::table::encode::{
    ColumnStatValue, ColumnStatsMetadata, StatsIndexVersion, V1_DECIMAL_PRECISION,
    V1_DECIMAL_SCALE, i128_from_be_bytes, rescale_decimal,
};

/// Per-column range extracted from a written Parquet base file.
#[derive(Debug, Clone)]
pub struct ColumnRangeStats {
    pub column_name: String,
    pub min_value: Option<ColumnStatValue>,
    pub max_value: Option<ColumnStatValue>,
    /// False when the parquet footer gave truncated/absent statistics, so the
    /// bounds are UNKNOWN rather than absent. Readers treat a published record
    /// with null min/max as "all nulls" and prune the file, so a record with
    /// an unknown range must not be published at all (the file then falls into
    /// the reader's not-indexed rescue set).
    pub range_known: bool,
    pub value_count: i64,
    pub null_count: i64,
    pub total_size: i64,
    pub total_uncompressed_size: i64,
}

/// Read file-level column ranges from in-memory Parquet bytes (Java `ParquetUtils`).
///
/// Covers every top-level column of a type any index version supports; the
/// metadata writer narrows that to [`columns_to_index`] for the table.
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

    #[derive(Default)]
    struct Acc {
        min: Option<ColumnStatValue>,
        max: Option<ColumnStatValue>,
        value_count: i64,
        null_count: i64,
        total_size: i64,
        total_uncompressed_size: i64,
        data_type: Option<DataType>,
        /// A chunk carried no statistics at all, so the accumulated range does
        /// not cover the whole column and must not be published as a bound.
        /// Truncated bounds do NOT set this: parquet truncates min downward
        /// and max upward, so they stay valid (just not tight) bounds, and
        /// Java publishes them as-is (ParquetUtils.readColumnStatsFromMetadata).
        range_incomplete: bool,
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
            let Some(&data_type) = type_by_name.get(col_name) else {
                continue;
            };
            if !is_indexable_type(data_type, StatsIndexVersion::V2) {
                continue;
            }

            let acc = by_col.entry(col_name.to_string()).or_default();
            acc.data_type = Some(data_type.clone());
            acc.value_count += col_chunk.num_values();
            acc.total_size += col_chunk.compressed_size();
            acc.total_uncompressed_size += col_chunk.uncompressed_size();

            if let Some(stats) = col_chunk.statistics() {
                // Null count only when actually recorded; never fabricate
                // "all null" for chunks with stats disabled.
                acc.null_count += stats.null_count_opt().unwrap_or(0) as i64;
                match parquet_min_max_to_stat_values(stats, data_type) {
                    // Truncated bounds are still bounds; publish them (the
                    // record is never marked tight).
                    Some((min, max)) => {
                        acc.min = merge_min(acc.min.take(), Some(min));
                        acc.max = merge_max(acc.max.take(), Some(max));
                    }
                    // Parquet emits stubbed stats for an all-null chunk: no
                    // bounds exist, and none are needed — it cannot widen the
                    // column's range (Java equates nulls to the value count).
                    None if stats
                        .null_count_opt()
                        .is_some_and(|nulls| nulls as i64 == col_chunk.num_values()) => {}
                    None => acc.range_incomplete = true,
                }
            } else {
                // Stats disabled for the chunk: range and null count unknown.
                acc.range_incomplete = true;
            }
        }
    }

    let mut out: Vec<ColumnRangeStats> = by_col
        .into_iter()
        .map(|(column_name, acc)| ColumnRangeStats {
            column_name,
            range_known: !acc.range_incomplete,
            min_value: (!acc.range_incomplete).then_some(acc.min).flatten(),
            max_value: (!acc.range_incomplete).then_some(acc.max).flatten(),
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
        .filter(|r| is_deleted || r.range_known || r.null_count == r.value_count)
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
///
/// Ranges must already be expressed in their column's type (see
/// [`coerce_stat_value`]); values of different types fail the aggregation
/// rather than producing a bound ordered by type instead of by value.
pub fn aggregate_partition_stats(
    partition_path: &str,
    ranges_by_file: &[Vec<ColumnRangeStats>],
    is_tight_bound: bool,
) -> Result<Vec<ColumnStatsMetadata>> {
    #[derive(Default)]
    struct Acc {
        min: Option<ColumnStatValue>,
        max: Option<ColumnStatValue>,
        value_count: i64,
        null_count: i64,
        total_size: i64,
        total_uncompressed_size: i64,
        range_incomplete: bool,
    }
    let mut by_col: HashMap<String, Acc> = HashMap::new();
    for ranges in ranges_by_file {
        for r in ranges {
            let acc = by_col.entry(r.column_name.clone()).or_default();
            // One file with unknown bounds makes the partition aggregate a
            // non-tight (advisory) bound: publishing it tight would let
            // pruning skip a partition whose unknown file may match.
            if !r.range_known && r.null_count != r.value_count {
                acc.range_incomplete = true;
            }
            for value in [&r.min_value, &r.max_value].into_iter().flatten() {
                if let Some(seen) = acc.min.as_ref().or(acc.max.as_ref())
                    && std::mem::discriminant(seen) != std::mem::discriminant(value)
                {
                    return Err(CoreError::MetadataTable(format!(
                        "cannot aggregate partition stats for column '{}' in partition '{}': \
                         {seen:?} and {value:?} are not comparable",
                        r.column_name, partition_path
                    )));
                }
            }
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
        // A partition record with null bounds reads as "all nulls" and Spark's
        // partition pruning has no not-indexed rescue set, so it would prune
        // the whole partition. Publish nothing instead — unless the column
        // really is all nulls, where null bounds are the correct encoding.
        .filter(|(_, acc)| {
            acc.min.is_some() || acc.max.is_some() || acc.null_count == acc.value_count
        })
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
            is_tight_bound: is_tight_bound && !acc.range_incomplete,
            decoded_value_type_ordinal: None,
        })
        .collect();
    out.sort_by(|a, b| a.column_name.cmp(&b.column_name));
    Ok(out)
}

/// Express a stat value in the variant footer extraction produces for a
/// column of `data_type` (Java `ValueMetadata.standardizeJavaTypeAndPromote`),
/// applying the Avro type promotions. `None` when no conversion exists.
///
/// Stats decoded from the MDT do not always come back as that variant: V1
/// records carry no logical type, so a tz-less timestamp decodes as a UTC one,
/// and Spark's V1 timestamps are bare longs in the column's own unit.
pub(crate) fn coerce_stat_value(
    value: ColumnStatValue,
    data_type: &DataType,
) -> Option<ColumnStatValue> {
    use ColumnStatValue as V;
    let timestamp = |micros: i64, tz: &Option<std::sync::Arc<str>>| {
        if tz.is_some() {
            V::TimestampMicros(micros)
        } else {
            V::LocalTimestampMicros(micros)
        }
    };
    Some(match (value, data_type) {
        (V::Boolean(v), DataType::Boolean) => V::Boolean(v),
        (V::Int(v) | V::Date(v), DataType::Date32) => V::Date(v),
        (
            V::Int(v),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16,
        ) => V::Int(v),
        (V::Int(v), DataType::Int64) => V::Long(i64::from(v)),
        (V::Long(v), DataType::Int64) => V::Long(v),
        (V::Int(v), DataType::Float32) => V::Float(v as f32),
        (V::Long(v), DataType::Float32) => V::Float(v as f32),
        (V::Float(v), DataType::Float32) => V::Float(v),
        (V::Int(v), DataType::Float64) => V::Double(f64::from(v)),
        (V::Long(v), DataType::Float64) => V::Double(v as f64),
        (V::Float(v), DataType::Float64) => V::Double(f64::from(v)),
        (V::Double(v), DataType::Float64) => V::Double(v),
        (V::String(v), DataType::Utf8 | DataType::LargeUtf8) => V::String(v),
        (V::Bytes(v), DataType::Binary | DataType::LargeBinary) => V::Bytes(v),
        (V::TimeMicros(v) | V::Long(v), DataType::Time64(TimeUnit::Microsecond)) => {
            V::TimeMicros(v)
        }
        (V::TimestampMicros(v) | V::LocalTimestampMicros(v), DataType::Timestamp(_, tz)) => {
            timestamp(v, tz)
        }
        (V::Long(v), DataType::Timestamp(unit, tz)) => timestamp(to_micros(v, *unit)?, tz),
        (
            V::Decimal {
                unscaled, scale, ..
            },
            DataType::Decimal128(precision, to_scale),
        ) => decimal(
            rescale_decimal(unscaled, scale, *to_scale)?,
            *precision,
            *to_scale,
        ),
        _ => return None,
    })
}

/// Java `HoodieMetadataConfig.COLUMN_STATS_INDEX_MAX_COLUMNS` default: only the
/// first 32 supported data fields (schema order) are indexed.
pub const MAX_COLUMNS_TO_INDEX: usize = 32;

/// Java `HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS`.
pub const COLUMN_STATS_COLUMN_LIST_KEY: &str = "hoodie.metadata.index.column.stats.column.list";

/// Java `HoodieMetadataConfig.COLUMN_STATS_INDEX_MAX_COLUMNS`.
pub const COLUMN_STATS_MAX_COLUMNS_KEY: &str =
    "hoodie.metadata.index.column.stats.max.columns.to.index";

/// Columns eligible for MDT indexing from a table/data schema.
///
/// Java `HoodieTableMetadataUtil.getColumnsToIndex`: the three always-indexed
/// meta fields (when present, i.e. `populateMetaFields`), then either the
/// supported columns of `column_list` in list order, or, when the list is
/// empty, the first `max_columns` supported non-meta fields in schema order.
/// Only top-level columns are indexed; nested names in the list are skipped.
pub fn columns_to_index(
    schema: &Schema,
    version: StatsIndexVersion,
    column_list: &[String],
    max_columns: usize,
) -> Vec<String> {
    // Java META_COLS_TO_ALWAYS_INDEX_SCHEMA_MAP is a TreeMap: alphabetical order.
    let always: Vec<String> = [
        MetaField::CommitTime,
        MetaField::PartitionPath,
        MetaField::RecordKey,
    ]
    .iter()
    .map(|m| m.as_ref().to_string())
    .collect();
    let mut cols: Vec<String> = always
        .iter()
        .filter(|name| schema.fields().iter().any(|f| f.name() == *name))
        .cloned()
        .collect();
    if column_list.is_empty() {
        cols.extend(
            schema
                .fields()
                .iter()
                .filter(|f| {
                    !MetaField::is_meta_field(f.name()) && is_indexable_type(f.data_type(), version)
                })
                .take(max_columns)
                .map(|f| f.name().clone()),
        );
    } else {
        for name in column_list {
            if always.contains(name) || cols.contains(name) {
                continue;
            }
            if let Ok(field) = schema.field_with_name(name)
                && is_indexable_type(field.data_type(), version)
            {
                cols.push(name.clone());
            }
        }
    }
    cols
}

/// Java `HoodieTableMetadataUtil.isColumnTypeSupported` for the AVRO record
/// type, which Spark datasource writes use: nested types are never indexed;
/// V1 (table version 8) also skips decimals whose value would not fit the
/// `decimal(30,15)` V1 wrapper.
pub(crate) fn is_indexable_type(data_type: &DataType, version: StatsIndexVersion) -> bool {
    if let DataType::Decimal128(precision, scale) = data_type {
        return *scale >= 0
            && (version == StatsIndexVersion::V2
                || (*scale <= V1_DECIMAL_SCALE
                    && i32::from(*precision) + i32::from(V1_DECIMAL_SCALE - *scale)
                        <= i32::from(V1_DECIMAL_PRECISION)));
    }
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
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Timestamp(_, _)
    )
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
        (ParquetStatistics::Int32(s), DataType::Decimal128(precision, scale)) => Some((
            decimal(i128::from(*s.min_opt()?), *precision, *scale),
            decimal(i128::from(*s.max_opt()?), *precision, *scale),
        )),
        (ParquetStatistics::Int64(s), DataType::Decimal128(precision, scale)) => Some((
            decimal(i128::from(*s.min_opt()?), *precision, *scale),
            decimal(i128::from(*s.max_opt()?), *precision, *scale),
        )),
        (ParquetStatistics::FixedLenByteArray(s), DataType::Decimal128(precision, scale)) => {
            Some((
                decimal(i128_from_be_bytes(s.min_opt()?.data())?, *precision, *scale),
                decimal(i128_from_be_bytes(s.max_opt()?.data())?, *precision, *scale),
            ))
        }
        (ParquetStatistics::ByteArray(s), DataType::Decimal128(precision, scale)) => Some((
            decimal(i128_from_be_bytes(s.min_opt()?.data())?, *precision, *scale),
            decimal(i128_from_be_bytes(s.max_opt()?.data())?, *precision, *scale),
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

fn decimal(unscaled: i128, precision: u8, scale: i8) -> ColumnStatValue {
    ColumnStatValue::Decimal {
        unscaled,
        precision,
        scale,
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
        ColumnStatValue::Decimal {
            unscaled,
            precision,
            scale,
        } => Arc::new(
            arrow_array::Decimal128Array::from(vec![*unscaled])
                .with_precision_and_scale(*precision, *scale)
                .ok()?,
        ),
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
    use std::sync::Arc;

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
        let cols = columns_to_index(&schema, StatsIndexVersion::V2, &[], MAX_COLUMNS_TO_INDEX);

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

    /// Java `isColumnTypeSupported` for the AVRO record type: V2 indexes every
    /// non-nested column; V1 also skips decimals outside `decimal(30,15)`.
    #[test]
    fn test_columns_to_index_type_support_per_index_version() {
        let schema = Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("b", DataType::Binary, false),
            Field::new("d", DataType::Decimal128(10, 2), false),
            Field::new("wide", DataType::Decimal128(38, 10), false),
            Field::new("fine", DataType::Decimal128(20, 16), false),
            Field::new(
                "nested",
                DataType::Struct(vec![Field::new("x", DataType::Int64, false)].into()),
                false,
            ),
            Field::new(
                "list",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            ),
        ]);
        let names = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            columns_to_index(&schema, StatsIndexVersion::V1, &[], MAX_COLUMNS_TO_INDEX),
            names(&["s", "b", "d"])
        );
        assert_eq!(
            columns_to_index(&schema, StatsIndexVersion::V2, &[], MAX_COLUMNS_TO_INDEX),
            names(&["s", "b", "d", "wide", "fine"])
        );
    }

    /// `hoodie.metadata.index.column.stats.column.list` picks the supported
    /// listed columns in list order after the always-indexed meta fields;
    /// without it, `max.columns.to.index` caps the schema-order prefix.
    #[test]
    fn test_columns_to_index_column_list_and_max_columns() {
        let schema = Schema::new(vec![
            Field::new("_hoodie_commit_time", DataType::Utf8, false),
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("_hoodie_partition_path", DataType::Utf8, false),
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new(
                "nested",
                DataType::Struct(vec![Field::new("x", DataType::Int64, false)].into()),
                false,
            ),
            Field::new("c", DataType::Decimal128(38, 10), false),
        ]);
        let names = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let meta = [
            "_hoodie_commit_time",
            "_hoodie_partition_path",
            "_hoodie_record_key",
        ];
        let list = names(&["c", "missing", "nested", "_hoodie_record_key", "b", "c"]);
        let mut expected = names(&meta);
        expected.extend(names(&["c", "b"]));
        assert_eq!(
            columns_to_index(&schema, StatsIndexVersion::V2, &list, 1),
            expected
        );
        let mut expected = names(&meta);
        expected.push("b".to_string());
        assert_eq!(
            columns_to_index(&schema, StatsIndexVersion::V1, &list, 1),
            expected
        );
        let mut expected = names(&meta);
        expected.extend(names(&["a", "b"]));
        assert_eq!(
            columns_to_index(&schema, StatsIndexVersion::V2, &[], 2),
            expected
        );
    }

    /// Decimal footer ranges come back as the column's decimal for every
    /// physical parquet encoding arrow-rs picks by precision.
    #[test]
    fn test_column_ranges_from_parquet_decimals() {
        use arrow_array::{ArrayRef, Decimal128Array, RecordBatch};
        let columns = [("d9", 9u8), ("d18", 18), ("d38", 38)];
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, p)| Field::new(*name, DataType::Decimal128(*p, 2), false))
                .collect::<Vec<_>>(),
        ));
        let arrays: Vec<ArrayRef> = columns
            .iter()
            .map(|(_, p)| {
                Arc::new(
                    Decimal128Array::from(vec![10_100_i128, -250, 10_300])
                        .with_precision_and_scale(*p, 2)
                        .unwrap(),
                ) as ArrayRef
            })
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let mut bytes = Vec::new();
        {
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let ranges = column_ranges_from_parquet_bytes(&bytes).unwrap();
        for (name, precision) in columns {
            let range = ranges.iter().find(|r| r.column_name == name).unwrap();
            let expected = |unscaled| {
                Some(ColumnStatValue::Decimal {
                    unscaled,
                    precision,
                    scale: 2,
                })
            };
            assert_eq!(range.min_value, expected(-250), "{name}");
            assert_eq!(range.max_value, expected(10_300), "{name}");
        }
    }

    /// A V2 stats record shaped like Spark writes it: a primitive `LongWrapper`
    /// (union branch 3) plus `valueType` carrying the logical type ordinal.
    fn spark_v2_long_stats_bytes(min: i64, max: i64, type_ordinal: i32) -> Vec<u8> {
        use apache_avro::types::Value;
        let wrap = |v: i64| {
            Value::Union(
                3,
                Box::new(Value::Record(vec![("value".to_string(), Value::Long(v))])),
            )
        };
        let some = |v: Value| Value::Union(1, Box::new(v));
        let null = || Value::Union(0, Box::new(Value::Null));
        let stats = Value::Record(vec![
            (
                "fileName".to_string(),
                some(Value::String("f.parquet".into())),
            ),
            ("columnName".to_string(), some(Value::String("ts".into()))),
            ("minValue".to_string(), wrap(min)),
            ("maxValue".to_string(), wrap(max)),
            ("valueCount".to_string(), some(Value::Long(1))),
            ("nullCount".to_string(), some(Value::Long(0))),
            ("totalSize".to_string(), some(Value::Long(1))),
            ("totalUncompressedSize".to_string(), some(Value::Long(1))),
            ("isDeleted".to_string(), Value::Boolean(false)),
            ("isTightBound".to_string(), Value::Boolean(false)),
            (
                "valueType".to_string(),
                some(Value::Record(vec![
                    ("typeOrdinal".to_string(), Value::Int(type_ordinal)),
                    ("additionalInfo".to_string(), null()),
                ])),
            ),
        ]);
        let record = Value::Record(vec![
            ("key".to_string(), Value::String(String::new())),
            ("type".to_string(), Value::Int(3)),
            ("filesystemMetadata".to_string(), null()),
            ("BloomFilterMetadata".to_string(), null()),
            ("ColumnStatsMetadata".to_string(), some(stats)),
            ("recordIndexMetadata".to_string(), null()),
            ("SecondaryIndexMetadata".to_string(), null()),
        ]);
        let schema = crate::schema::avsc::hoodie_metadata_schema().unwrap();
        apache_avro::to_avro_datum(schema, record).unwrap()
    }

    /// A Spark-written TIMESTAMP_MILLIS survivor merged with a footer range of
    /// the same column must compare by instant, so min <= max.
    #[test]
    fn test_partition_stats_merge_spark_timestamp_millis_survivor() {
        use crate::metadata::table::encode::decode_column_stats_entry;

        // Survivor spans [5s, 6s] in millis; the new file holds 1s in micros.
        let bytes = spark_v2_long_stats_bytes(5_000, 6_000, 15);
        let schema = crate::schema::avsc::hoodie_metadata_schema().unwrap();
        let survivor = decode_column_stats_entry(&bytes, Some(schema))
            .unwrap()
            .unwrap();
        let survivor = ColumnRangeStats {
            column_name: "ts".to_string(),
            min_value: survivor.min_value,
            max_value: survivor.max_value,
            range_known: true,
            value_count: 1,
            null_count: 0,
            total_size: 1,
            total_uncompressed_size: 1,
        };
        let footer = ColumnRangeStats {
            min_value: Some(ColumnStatValue::TimestampMicros(1_000_000)),
            max_value: Some(ColumnStatValue::TimestampMicros(1_000_000)),
            ..survivor.clone()
        };
        let merged = aggregate_partition_stats("p", &[vec![survivor], vec![footer]], true).unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(
            (merged[0].min_value.clone(), merged[0].max_value.clone()),
            (
                Some(ColumnStatValue::TimestampMicros(1_000_000)),
                Some(ColumnStatValue::TimestampMicros(6_000_000))
            )
        );
        assert!(merged[0].is_tight_bound);
    }

    /// Values of different types are never merged into a bound.
    #[test]
    fn test_partition_stats_merge_rejects_incomparable_values() {
        let range = |v: ColumnStatValue| ColumnRangeStats {
            column_name: "c".to_string(),
            min_value: Some(v.clone()),
            max_value: Some(v),
            range_known: true,
            value_count: 1,
            null_count: 0,
            total_size: 1,
            total_uncompressed_size: 1,
        };
        let result = aggregate_partition_stats(
            "p",
            &[
                vec![range(ColumnStatValue::String("a".into()))],
                vec![range(ColumnStatValue::Long(1))],
            ],
            true,
        );
        assert!(result.is_err(), "{result:?}");
    }

    #[test]
    fn test_coerce_stat_value_to_column_type() {
        use crate::metadata::table::encode::ColumnStatValue as V;
        let ts_utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let ts_local = DataType::Timestamp(TimeUnit::Microsecond, None);
        let ts_millis = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let time_micros = DataType::Time64(TimeUnit::Microsecond);
        let cases: Vec<(V, DataType, Option<V>)> = vec![
            // V1 decodes every timestamp wrapper as UTC.
            (
                V::TimestampMicros(7),
                ts_local.clone(),
                Some(V::LocalTimestampMicros(7)),
            ),
            (
                V::LocalTimestampMicros(7),
                ts_utc.clone(),
                Some(V::TimestampMicros(7)),
            ),
            // A bare long is in the column's own unit (Spark's V1 LongWrapper).
            (V::Long(7), ts_utc, Some(V::TimestampMicros(7))),
            (V::Long(7), ts_local, Some(V::LocalTimestampMicros(7))),
            (V::Long(7), ts_millis, Some(V::TimestampMicros(7_000))),
            (V::Long(7), time_micros, Some(V::TimeMicros(7))),
            (V::Int(7), DataType::Date32, Some(V::Date(7))),
            // Avro promotions.
            (V::Int(7), DataType::Int64, Some(V::Long(7))),
            (V::Int(7), DataType::Float64, Some(V::Double(7.0))),
            (V::Long(7), DataType::Float64, Some(V::Double(7.0))),
            (V::Float(1.5), DataType::Float64, Some(V::Double(1.5))),
            (V::Int(7), DataType::Int16, Some(V::Int(7))),
            (
                V::String("s".into()),
                DataType::Utf8,
                Some(V::String("s".into())),
            ),
            (V::Bytes(vec![1]), DataType::Binary, Some(V::Bytes(vec![1]))),
            // A V1 decimal(30,15) value rescales to the column's scale.
            (
                V::Decimal {
                    unscaled: 101_000_000_000_000_000,
                    precision: 30,
                    scale: 15,
                },
                DataType::Decimal128(10, 2),
                Some(V::Decimal {
                    unscaled: 10_100,
                    precision: 10,
                    scale: 2,
                }),
            ),
            // No conversion exists.
            (
                V::Decimal {
                    unscaled: 10_123,
                    precision: 10,
                    scale: 3,
                },
                DataType::Decimal128(10, 2),
                None,
            ),
            (V::String("s".into()), DataType::Int64, None),
            (V::Long(7), DataType::Int32, None),
            (V::Double(1.0), DataType::Int64, None),
        ];
        for (value, data_type, expected) in cases {
            assert_eq!(
                coerce_stat_value(value.clone(), &data_type),
                expected,
                "{value:?} as {data_type:?}"
            );
        }
    }

    #[test]
    fn test_stat_value_to_array_matrix() {
        use super::stat_value_to_array;
        use crate::metadata::table::encode::ColumnStatValue as V;
        use arrow_schema::{DataType, TimeUnit};

        let cases: Vec<(V, DataType)> = vec![
            (V::Boolean(true), DataType::Boolean),
            (V::Int(7), DataType::Int32),
            (V::Long(9), DataType::Int64),
            (V::Float(1.5), DataType::Float32),
            (V::Double(2.5), DataType::Float64),
            (V::Bytes(vec![1, 2]), DataType::Binary),
            (V::String("s".into()), DataType::Utf8),
            (V::Date(19000), DataType::Date32),
            (V::TimeMicros(1), DataType::Time64(TimeUnit::Microsecond)),
            (
                V::TimestampMicros(1),
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            (
                V::LocalTimestampMicros(1),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
        ];
        for (value, target) in &cases {
            let arr = stat_value_to_array(value, target)
                .unwrap_or_else(|| panic!("natural conversion must succeed for {target:?}"));
            assert_eq!(arr.data_type(), target);
            assert_eq!(arr.len(), 1);
        }

        // Widening casts: Int stat onto an Int64 column, Long onto Float64.
        let widened = stat_value_to_array(&V::Int(5), &DataType::Int64).unwrap();
        assert_eq!(widened.data_type(), &DataType::Int64);
        let widened = stat_value_to_array(&V::Long(5), &DataType::Float64).unwrap();
        assert_eq!(widened.data_type(), &DataType::Float64);

        // Impossible cast: bytes onto a boolean column -> None (conservative).
        assert!(stat_value_to_array(&V::Bytes(vec![1]), &DataType::Boolean).is_none());
    }

    /// Footer range extraction across the type matrix: every supported type
    /// yields min/max/null counts; unsupported columns are skipped.
    #[test]
    fn test_column_ranges_from_parquet_type_matrix() {
        use arrow_array::{
            ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
            StringArray, TimestampMicrosecondArray,
        };
        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("i", DataType::Int32, false),
            Field::new("l", DataType::Int64, false),
            Field::new("f", DataType::Float64, false),
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Date32, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![false, true])) as ArrayRef,
                Arc::new(Int32Array::from(vec![3, 1])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Float64Array::from(vec![2.5, 1.5])),
                Arc::new(StringArray::from(vec![Some("b"), None])),
                Arc::new(Date32Array::from(vec![100, 200])),
                Arc::new(TimestampMicrosecondArray::from(vec![7, 9]).with_timezone("UTC")),
            ],
        )
        .unwrap();

        let props = parquet::file::properties::WriterProperties::builder().build();
        let mut bytes = Vec::new();
        {
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let ranges = column_ranges_from_parquet_bytes(&bytes).unwrap();
        let by_name: std::collections::HashMap<&str, &ColumnRangeStats> =
            ranges.iter().map(|r| (r.column_name.as_str(), r)).collect();

        let int_range = by_name["i"];
        assert_eq!(int_range.min_value, Some(ColumnStatValue::Int(1)));
        assert_eq!(int_range.max_value, Some(ColumnStatValue::Int(3)));
        assert_eq!(by_name["l"].max_value, Some(ColumnStatValue::Long(20)));
        assert_eq!(by_name["f"].min_value, Some(ColumnStatValue::Double(1.5)));
        assert_eq!(
            by_name["s"].min_value,
            Some(ColumnStatValue::String("b".to_string()))
        );
        assert_eq!(by_name["s"].null_count, 1);
        assert_eq!(by_name["d"].min_value, Some(ColumnStatValue::Date(100)));
        assert_eq!(
            by_name["ts"].max_value,
            Some(ColumnStatValue::TimestampMicros(9))
        );
        let bool_range = by_name["b"];
        assert_eq!(bool_range.min_value, Some(ColumnStatValue::Boolean(false)));
    }
}
