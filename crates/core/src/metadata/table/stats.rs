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

//! Column and partition statistics reading from the metadata table.

use std::collections::HashMap;

use arrow_array::ArrayRef;
use arrow_schema::{DataType, Schema};

use crate::Result;
use crate::config::read::HudiReadConfig;
use crate::error::CoreError;
use crate::expr::filter::from_str_tuples;
use crate::metadata::METADATA_TABLE_PARTITION_FIELD;
use crate::statistics::{ColumnStatistics, StatisticsContainer, StatsGranularity};
use crate::table::{FilePruner, PartitionPruner, Table};
use crate::util::hash::get_column_stats_key;

use super::records::{ColumnStatsRecord, PartitionStatsRecord, WrappedStatValue};

impl Table {
    /// Read column statistics for specific files from the metadata table.
    ///
    /// This method reads column statistics from the `column_stats` partition of the
    /// metadata table and returns them grouped by file name.
    ///
    /// # Arguments
    /// * `file_names` - List of file names to get stats for (without path)
    /// * `column_names` - List of column names to get stats for
    /// * `partition_path` - The partition path these files belong to
    ///
    /// # Returns
    /// A map from file_name to `StatisticsContainer` containing column stats.
    ///
    /// # Errors
    /// Returns an error if called on a metadata table instead of a data table.
    pub async fn read_column_stats_for_files(
        &self,
        file_names: &[&str],
        column_names: &[&str],
        partition_path: &str,
    ) -> Result<HashMap<String, StatisticsContainer>> {
        if !self.has_column_stats_partition() {
            return Err(CoreError::MetadataTable(
                "column_stats partition not available".to_string(),
            ));
        }

        let metadata_table = self.new_metadata_table().await?;

        // Generate keys for all (column, file) combinations
        let keys: Vec<String> = column_names
            .iter()
            .flat_map(|col| {
                file_names
                    .iter()
                    .map(move |file| get_column_stats_key(col, partition_path, file))
            })
            .collect();

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

        // Read from column_stats partition using the metadata table
        let records = metadata_table.fetch_column_stats_records(&key_refs).await?;

        // Convert to StatisticsContainer grouped by file
        column_stats_records_to_stats_map(records)
    }

    /// Read partition-level statistics from the metadata table.
    ///
    /// This method reads partition statistics from the `partition_stats` partition of the
    /// metadata table and returns them grouped by partition path.
    ///
    /// # Arguments
    /// * `partition_paths` - List of partition paths to get stats for
    /// * `column_names` - List of column names to get stats for
    ///
    /// # Returns
    /// A map from partition_path to `StatisticsContainer` containing column stats.
    ///
    /// # Errors
    /// Returns an error if called on a metadata table instead of a data table.
    pub async fn read_partition_stats(
        &self,
        partition_paths: &[&str],
        column_names: &[&str],
    ) -> Result<HashMap<String, StatisticsContainer>> {
        if !self.has_partition_stats_partition() {
            return Err(CoreError::MetadataTable(
                "partition_stats partition not available".to_string(),
            ));
        }

        let metadata_table = self.new_metadata_table().await?;

        // Generate keys for all (column, partition) combinations
        let keys: Vec<String> = column_names
            .iter()
            .flat_map(|col| {
                partition_paths
                    .iter()
                    .map(move |part| crate::util::hash::get_partition_stats_key(col, part))
            })
            .collect();

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

        // Read from partition_stats partition using the metadata table
        let records = metadata_table
            .fetch_partition_stats_records(&key_refs)
            .await?;

        // Convert to StatisticsContainer grouped by partition
        partition_stats_records_to_stats_map(records)
    }

    /// Fetch column stats records from this metadata table's column_stats partition.
    ///
    /// # Arguments
    /// * `keys` - The lookup keys for column stats records
    ///
    /// # Errors
    /// Returns an error if called on a data table instead of a metadata table.
    pub async fn fetch_column_stats_records(
        &self,
        keys: &[&str],
    ) -> Result<Vec<ColumnStatsRecord>> {
        self.require_metadata_table()?;

        let Some(timestamp) = self.timeline.get_latest_commit_timestamp_as_option() else {
            return Ok(vec![]);
        };

        let timeline_view = self.timeline.create_view_as_of(timestamp).await?;

        let filters = from_str_tuples([(
            METADATA_TABLE_PARTITION_FIELD,
            "=",
            ColumnStatsRecord::PARTITION_NAME,
        )])?;
        let partition_schema = self.get_partition_schema().await?;
        let partition_pruner =
            PartitionPruner::new(&filters, &partition_schema, self.hudi_configs.as_ref())?;

        let file_pruner = FilePruner::empty();
        let table_schema = Schema::empty();

        let file_slices = match self
            .file_system_view
            .get_file_slices_by_storage_listing(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
            )
            .await
        {
            Ok(slices) => slices,
            Err(e) => {
                // Handle case where column_stats partition has log files without matching base files
                // This can happen before compaction creates the base HFile
                log::warn!(
                    "Failed to get file slices for column_stats partition: {e}. \
                    This may indicate the partition has not been compacted yet."
                );
                return Ok(vec![]);
            }
        };

        if file_slices.is_empty() {
            return Ok(vec![]);
        }

        let fg_reader = self.create_file_group_reader_with_options([(
            HudiReadConfig::FileGroupEndTimestamp,
            timestamp,
        )])?;

        // Read from all file slices and merge results
        let mut all_results = Vec::new();
        for file_slice in &file_slices {
            match fg_reader
                .read_metadata_table_column_stats_partition(file_slice, keys)
                .await
            {
                Ok(records) => all_results.extend(records),
                Err(e) => {
                    log::warn!(
                        "Failed to read column stats from file slice {:?}: {e}",
                        file_slice.base_file_relative_path()
                    );
                }
            }
        }

        Ok(all_results)
    }

    /// Fetch partition stats records from this metadata table's partition_stats partition.
    ///
    /// # Arguments
    /// * `keys` - The lookup keys for partition stats records
    ///
    /// # Errors
    /// Returns an error if called on a data table instead of a metadata table.
    pub async fn fetch_partition_stats_records(
        &self,
        keys: &[&str],
    ) -> Result<Vec<PartitionStatsRecord>> {
        self.require_metadata_table()?;

        let Some(timestamp) = self.timeline.get_latest_commit_timestamp_as_option() else {
            return Ok(vec![]);
        };

        let timeline_view = self.timeline.create_view_as_of(timestamp).await?;

        let filters = from_str_tuples([(
            METADATA_TABLE_PARTITION_FIELD,
            "=",
            PartitionStatsRecord::PARTITION_NAME,
        )])?;
        let partition_schema = self.get_partition_schema().await?;
        let partition_pruner =
            PartitionPruner::new(&filters, &partition_schema, self.hudi_configs.as_ref())?;

        let file_pruner = FilePruner::empty();
        let table_schema = Schema::empty();

        let file_slices = match self
            .file_system_view
            .get_file_slices_by_storage_listing(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
            )
            .await
        {
            Ok(slices) => slices,
            Err(e) => {
                // Handle case where partition_stats partition has log files without matching base files
                // This can happen before compaction creates the base HFile
                log::warn!(
                    "Failed to get file slices for partition_stats partition: {e}. \
                    This may indicate the partition has not been compacted yet."
                );
                return Ok(vec![]);
            }
        };

        if file_slices.is_empty() {
            return Ok(vec![]);
        }

        let fg_reader = self.create_file_group_reader_with_options([(
            HudiReadConfig::FileGroupEndTimestamp,
            timestamp,
        )])?;

        // Read from all file slices and merge results
        let mut all_results = Vec::new();
        for file_slice in &file_slices {
            match fg_reader
                .read_metadata_table_partition_stats_partition(file_slice, keys)
                .await
            {
                Ok(records) => all_results.extend(records),
                Err(e) => {
                    log::warn!(
                        "Failed to read partition stats from file slice {:?}: {e}",
                        file_slice.base_file_relative_path()
                    );
                }
            }
        }

        Ok(all_results)
    }
}

// ============================================================================
// Conversion helpers (public for use by FileSystemView)
// ============================================================================

/// Convert column stats records to a map of file name -> StatisticsContainer.
pub fn column_stats_records_to_stats_map(
    records: Vec<ColumnStatsRecord>,
) -> Result<HashMap<String, StatisticsContainer>> {
    let mut result: HashMap<String, StatisticsContainer> = HashMap::new();

    for record in records {
        if record.is_deleted {
            continue;
        }

        let container = result
            .entry(record.file_name.clone())
            .or_insert_with(|| StatisticsContainer::new(StatsGranularity::File));

        if let Some(col_stats) = convert_to_column_statistics(&record) {
            container
                .columns
                .insert(record.column_name.clone(), col_stats);
        }
    }

    Ok(result)
}

/// Convert partition stats records to a map of partition path -> StatisticsContainer.
pub fn partition_stats_records_to_stats_map(
    records: Vec<PartitionStatsRecord>,
) -> Result<HashMap<String, StatisticsContainer>> {
    let mut result: HashMap<String, StatisticsContainer> = HashMap::new();

    for record in records {
        if record.is_deleted {
            continue;
        }

        let container = result
            .entry(record.partition_path.clone())
            .or_insert_with(|| StatisticsContainer::new(StatsGranularity::File));

        if let Some(col_stats) = convert_partition_stats_to_column_statistics(&record) {
            container
                .columns
                .insert(record.column_name.clone(), col_stats);
        }
    }

    Ok(result)
}

// ============================================================================
// Conversion helpers
// ============================================================================

/// Convert a ColumnStatsRecord to ColumnStatistics.
fn convert_to_column_statistics(record: &ColumnStatsRecord) -> Option<ColumnStatistics> {
    let data_type = infer_data_type_from_wrapped_value(
        record.min_value.as_ref().or(record.max_value.as_ref())?,
    )?;

    Some(ColumnStatistics {
        column_name: record.column_name.clone(),
        data_type: data_type.clone(),
        min_value: record
            .min_value
            .as_ref()
            .and_then(|v| wrapped_value_to_arrow_array(v, &data_type)),
        max_value: record
            .max_value
            .as_ref()
            .and_then(|v| wrapped_value_to_arrow_array(v, &data_type)),
    })
}

/// Convert a PartitionStatsRecord to ColumnStatistics.
fn convert_partition_stats_to_column_statistics(
    record: &PartitionStatsRecord,
) -> Option<ColumnStatistics> {
    let data_type = infer_data_type_from_wrapped_value(
        record.min_value.as_ref().or(record.max_value.as_ref())?,
    )?;

    Some(ColumnStatistics {
        column_name: record.column_name.clone(),
        data_type: data_type.clone(),
        min_value: record
            .min_value
            .as_ref()
            .and_then(|v| wrapped_value_to_arrow_array(v, &data_type)),
        max_value: record
            .max_value
            .as_ref()
            .and_then(|v| wrapped_value_to_arrow_array(v, &data_type)),
    })
}

/// Infer Arrow DataType from a WrappedStatValue.
fn infer_data_type_from_wrapped_value(value: &WrappedStatValue) -> Option<DataType> {
    match value {
        WrappedStatValue::Null => None,
        WrappedStatValue::Boolean(_) => Some(DataType::Boolean),
        WrappedStatValue::Int(_) => Some(DataType::Int32),
        WrappedStatValue::Long(_) => Some(DataType::Int64),
        WrappedStatValue::Float(_) => Some(DataType::Float32),
        WrappedStatValue::Double(_) => Some(DataType::Float64),
        WrappedStatValue::String(_) => Some(DataType::Utf8),
        WrappedStatValue::Bytes(_) => Some(DataType::Binary),
        WrappedStatValue::Date(_) => Some(DataType::Date32),
        WrappedStatValue::TimeMicros(_) => {
            Some(DataType::Time64(arrow_schema::TimeUnit::Microsecond))
        }
        WrappedStatValue::TimestampMicros(_) => Some(DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            None,
        )),
        WrappedStatValue::LocalDate(_) => Some(DataType::Date32),
        WrappedStatValue::Decimal {
            precision, scale, ..
        } => Some(DataType::Decimal128(*precision, *scale)),
        WrappedStatValue::Array(_) => {
            // Array statistics are complex; return None for now as pruning on arrays
            // is not commonly supported
            None
        }
    }
}

/// Convert a WrappedStatValue to a single-element Arrow array.
fn wrapped_value_to_arrow_array(
    value: &WrappedStatValue,
    _data_type: &DataType,
) -> Option<ArrayRef> {
    use arrow_array::{
        BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
        StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    };
    use std::sync::Arc;

    match value {
        WrappedStatValue::Null => None,
        WrappedStatValue::Boolean(b) => Some(Arc::new(BooleanArray::from(vec![*b])) as ArrayRef),
        WrappedStatValue::Int(n) => Some(Arc::new(Int32Array::from(vec![*n])) as ArrayRef),
        WrappedStatValue::Long(n) => Some(Arc::new(Int64Array::from(vec![*n])) as ArrayRef),
        WrappedStatValue::Float(f) => Some(Arc::new(Float32Array::from(vec![*f])) as ArrayRef),
        WrappedStatValue::Double(d) => Some(Arc::new(Float64Array::from(vec![*d])) as ArrayRef),
        WrappedStatValue::String(s) => {
            Some(Arc::new(StringArray::from(vec![s.as_str()])) as ArrayRef)
        }
        WrappedStatValue::Bytes(b) => {
            Some(Arc::new(BinaryArray::from(vec![b.as_slice()])) as ArrayRef)
        }
        WrappedStatValue::Date(d) => Some(Arc::new(Date32Array::from(vec![*d])) as ArrayRef),
        WrappedStatValue::TimeMicros(t) => {
            Some(Arc::new(Time64MicrosecondArray::from(vec![*t])) as ArrayRef)
        }
        WrappedStatValue::TimestampMicros(t) => {
            Some(Arc::new(TimestampMicrosecondArray::from(vec![*t])) as ArrayRef)
        }
        WrappedStatValue::LocalDate(d) => Some(Arc::new(Date32Array::from(vec![*d])) as ArrayRef),
        WrappedStatValue::Decimal {
            value: bytes,
            precision,
            scale,
        } => {
            // Convert bytes to i128
            // Decimal bytes are big-endian two's complement
            if bytes.len() > 16 {
                return None;
            }
            let mut padded = [0u8; 16];
            let start = 16 - bytes.len();
            // Handle sign extension
            if !bytes.is_empty() && (bytes[0] & 0x80) != 0 {
                padded.fill(0xff);
            }
            padded[start..].copy_from_slice(bytes);
            let value = i128::from_be_bytes(padded);

            use arrow_array::Decimal128Array;
            let array = Decimal128Array::from(vec![value])
                .with_precision_and_scale(*precision, *scale)
                .ok()?;
            Some(Arc::new(array) as ArrayRef)
        }
        WrappedStatValue::Array(_) => {
            // Array statistics cannot be converted to a single-element array for pruning
            None
        }
    }
}
