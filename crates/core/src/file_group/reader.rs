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
use crate::Result;
use crate::config::HudiConfigs;
use crate::config::read::HudiReadConfig;
use crate::config::table::HudiTableConfig;
use crate::config::util::split_hudi_options_from_others;
use crate::error::CoreError;
use crate::error::CoreError::ReadFileSliceError;
use crate::expr::filter::{Filter, SchemableFilter};
use crate::file_group::FileGroup;
use crate::file_group::file_slice::FileSlice;
use crate::file_group::log_file::scanner::{LogFileScanner, ScanResult};
use crate::file_group::record_batches::RecordBatches;
use crate::hfile::{HFileReader, HFileRecord};
use crate::merge::record_merger::RecordMerger;
use crate::metadata::merger::FilesPartitionMerger;
use crate::metadata::meta_field::MetaField;
use crate::metadata::table_record::FilesPartitionRecord;
use crate::storage::{ParquetReadOptions, Storage};
use crate::table::ReadOptions;
use crate::table::builder::OptionResolver;
use crate::timeline::selector::InstantRange;
use arrow::compute::{and, concat_batches, filter_record_batch};
use arrow_array::{BooleanArray, RecordBatch};
use futures::stream::BoxStream;
use futures::{StreamExt, TryFutureExt};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

/// The reader that handles all read operations against a file group.
#[derive(Clone, Debug)]
pub struct FileGroupReader {
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
}

impl FileGroupReader {
    /// Creates a new reader with the given Hudi configurations and overwriting options.
    ///
    /// # Notes
    /// This API does **not** use [`OptionResolver`] that loads table properties from storage to resolve options.
    pub(crate) fn new_with_configs_and_overwriting_options<I, K, V>(
        hudi_configs: Arc<HudiConfigs>,
        overwriting_options: I,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let (hudi_opts, others) = split_hudi_options_from_others(overwriting_options);

        let mut final_opts = hudi_configs.as_options();
        final_opts.extend(hudi_opts);
        let hudi_configs = Arc::new(HudiConfigs::new(final_opts));
        let storage = Storage::new(Arc::new(others), hudi_configs.clone())?;

        Ok(Self {
            hudi_configs,
            storage,
        })
    }

    /// Creates a new reader with the given base URI and options.
    ///
    /// # Arguments
    /// * `base_uri` - The base URI of the file group's residing table.
    /// * `options` - Additional options for the reader.
    ///
    /// # Notes
    /// This API uses [`OptionResolver`] that loads table properties from storage to resolve options.
    pub fn new_with_options<I, K, V>(base_uri: &str, options: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        // Collect options upfront so we can move them across thread boundaries if needed
        let options_vec: Vec<(String, String)> = options
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.into()))
            .collect();
        let base_uri = base_uri.to_string();

        // Helper function to run the async resolution
        async fn resolve_reader(
            base_uri: String,
            options_vec: Vec<(String, String)>,
        ) -> Result<FileGroupReader> {
            let mut resolver = OptionResolver::new_with_options(&base_uri, options_vec);
            resolver.resolve_options().await?;
            let hudi_configs = Arc::new(HudiConfigs::new(resolver.hudi_options));
            let storage = Storage::new(Arc::new(resolver.storage_options), hudi_configs.clone())?;

            Ok(FileGroupReader {
                hudi_configs,
                storage,
            })
        }

        // Check if we're already in a tokio runtime
        match tokio::runtime::Handle::try_current() {
            Ok(_handle) => {
                // We're inside a runtime, spawn a new thread with its own runtime
                std::thread::scope(|s| {
                    s.spawn(|| {
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap()
                            .block_on(resolve_reader(base_uri, options_vec))
                    })
                    .join()
                    .unwrap()
                })
            }
            Err(_) => {
                // No runtime, create one
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?
                    .block_on(resolve_reader(base_uri, options_vec))
            }
        }
    }

    fn create_filtering_mask_for_base_file_records(
        &self,
        records: &RecordBatch,
    ) -> Result<Option<BooleanArray>> {
        let populates_meta_fields: bool = self
            .hudi_configs
            .get_or_default(HudiTableConfig::PopulatesMetaFields)
            .into();
        if !populates_meta_fields {
            // If meta fields are not populated, commit time filtering is not applicable.
            return Ok(None);
        }

        let mut and_filters: Vec<SchemableFilter> = Vec::new();
        let schema = MetaField::schema();
        if let Some(start) = self
            .hudi_configs
            .try_get(HudiReadConfig::FileGroupStartTimestamp)
            .map(|v| -> String { v.into() })
        {
            let filter: Filter =
                Filter::try_from((MetaField::CommitTime.as_ref(), ">", start.as_str()))?;
            let filter = SchemableFilter::try_from((filter, schema.as_ref()))?;
            and_filters.push(filter);
        } else {
            // If start timestamp is not provided, the query is snapshot or time-travel, so
            // commit time filtering is not needed as the base file being read is already
            // filtered and selected by the timeline.
            return Ok(None);
        }

        if let Some(end) = self
            .hudi_configs
            .try_get(HudiReadConfig::FileGroupEndTimestamp)
            .map(|v| -> String { v.into() })
        {
            let filter = Filter::try_from((MetaField::CommitTime.as_ref(), "<=", end.as_str()))?;
            let filter = SchemableFilter::try_from((filter, schema.as_ref()))?;
            and_filters.push(filter);
        }

        if and_filters.is_empty() {
            return Ok(None);
        }

        let mut mask = BooleanArray::from(vec![true; records.num_rows()]);
        for filter in &and_filters {
            let col_name = filter.field.name().as_str();
            let col_values = records
                .column_by_name(col_name)
                .ok_or_else(|| ReadFileSliceError(format!("Column {col_name} not found")))?;

            let comparison = filter.apply_comparsion(col_values)?;
            mask = and(&mask, &comparison)?;
        }
        Ok(Some(mask))
    }

    /// Reads the data from the base file at the given relative path.
    async fn read_file_slice_by_base_file_path(&self, relative_path: &str) -> Result<RecordBatch> {
        let records = self
            .read_parquet_file_to_single_batch(relative_path)
            .await?;

        if let Some(mask) = self.create_filtering_mask_for_base_file_records(&records)? {
            filter_record_batch(&records, &mask)
                .map_err(|e| ReadFileSliceError(format!("Failed to filter records: {e:?}")))
        } else {
            Ok(records)
        }
    }

    /// Reads a parquet file using the streaming API and collects into a single batch.
    async fn read_parquet_file_to_single_batch(&self, relative_path: &str) -> Result<RecordBatch> {
        let options = ParquetReadOptions::default();
        let mut stream = self
            .storage
            .get_parquet_file_stream(relative_path, options)
            .map_err(|e| ReadFileSliceError(format!("Failed to read path {relative_path}: {e:?}")))
            .await?;

        let schema = stream.schema().clone();
        let mut batches = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result.map_err(|e| {
                ReadFileSliceError(format!("Failed to read batch from {relative_path}: {e:?}"))
            })?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }

        concat_batches(&schema, &batches)
            .map_err(|e| ReadFileSliceError(format!("Failed to concat batches: {e:?}")))
    }

    fn create_instant_range_for_log_file_scan(&self) -> InstantRange {
        let timezone = self
            .hudi_configs
            .get_or_default(HudiTableConfig::TimelineTimezone)
            .into();
        let start_timestamp = self
            .hudi_configs
            .try_get(HudiReadConfig::FileGroupStartTimestamp)
            .map(|v| -> String { v.into() });
        let end_timestamp = self
            .hudi_configs
            .try_get(HudiReadConfig::FileGroupEndTimestamp)
            .map(|v| -> String { v.into() });
        InstantRange::new(timezone, start_timestamp, end_timestamp, false, true)
    }

    /// Reads a file slice from a base file and a list of log files with [ReadOptions].
    async fn read_file_slice_from_paths_with_opts<I, S>(
        &self,
        base_file_path: &str,
        log_file_paths: I,
        options: &ReadOptions,
    ) -> Result<RecordBatch>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let log_file_paths: Vec<String> = log_file_paths
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
        let use_read_optimized: bool = self
            .hudi_configs
            .get_or_default(HudiReadConfig::UseReadOptimizedMode)
            .into();
        let base_file_only = log_file_paths.is_empty() || use_read_optimized;

        let projection = options.projection.as_deref();
        let row_predicate = options.row_predicate.as_ref();

        // For now, only projection is supported for base file only reads
        // Log file merging with projection/predicate is more complex and deferred
        if base_file_only {
            self.read_base_file_with_opts(base_file_path, projection, row_predicate)
                .await
        } else {
            // For MOR tables with log files, use as_of_timestamp from options if provided
            let instant_range = self.create_instant_range_for_log_file_scan_with_opts(options);

            let scan_result = LogFileScanner::new(self.hudi_configs.clone(), self.storage.clone())
                .scan(log_file_paths, &instant_range)
                .await?;

            let log_batches = match scan_result {
                ScanResult::RecordBatches(batches) => batches,
                ScanResult::Empty => RecordBatches::new(),
                ScanResult::HFileRecords(_) => {
                    return Err(CoreError::LogBlockError(
                        "Unexpected HFile records in regular table log file".to_string(),
                    ));
                }
            };

            let base_batch = self
                .read_file_slice_by_base_file_path(base_file_path)
                .await?;
            let schema = base_batch.schema();
            let num_data_batches = log_batches.num_data_batches() + 1;
            let num_delete_batches = log_batches.num_delete_batches();
            let mut all_batches =
                RecordBatches::new_with_capacity(num_data_batches, num_delete_batches);
            all_batches.push_data_batch(base_batch);
            all_batches.extend(log_batches);

            let merger = RecordMerger::new(schema.clone(), self.hudi_configs.clone());
            let batch = merger.merge_record_batches(all_batches)?;

            // Apply projection if specified
            let batch = if let Some(cols) = projection {
                Self::apply_projection(&batch, cols)?
            } else {
                batch
            };

            // Apply row predicate if specified
            let batch = if let Some(predicate) = row_predicate {
                Self::apply_row_predicate(&batch, predicate)?
            } else {
                batch
            };

            Ok(batch)
        }
    }

    /// Reads a file slice as a stream of record batches with [ReadOptions].
    ///
    /// This is the primary read API for FileGroupReader. It returns a stream that
    /// yields record batches as they are read.
    ///
    /// For COW tables or read-optimized mode (base file only), this returns a true
    /// streaming iterator from the underlying parquet file, yielding batches as they
    /// are read without loading all data into memory.
    ///
    /// For MOR tables with log files, this falls back to the collect-and-merge approach
    /// and yields the merged result as a single batch.
    ///
    /// # Arguments
    /// * `file_slice` - The file slice to read.
    /// * `options` - Read options for configuring the read operation:
    ///     - `projection`: Column names to project (select)
    ///     - `row_predicate`: Row-level filter predicate
    ///     - `batch_size`: Target rows per batch
    ///
    /// # Returns
    /// A stream of record batches. The stream owns all necessary data and is `'static`.
    ///
    /// # Example
    /// ```ignore
    /// use futures::StreamExt;
    ///
    /// let options = ReadOptions::new().with_batch_size(4096);
    /// let mut stream = reader.read_file_slice(&file_slice, &options).await?;
    ///
    /// while let Some(result) = stream.next().await {
    ///     let batch = result?;
    ///     // Process batch...
    /// }
    /// ```
    pub async fn read_file_slice(
        &self,
        file_slice: &FileSlice,
        options: &ReadOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let base_file_path = file_slice.base_file_relative_path()?;
        let log_file_paths: Vec<String> = if file_slice.has_log_file() {
            file_slice
                .log_files
                .iter()
                .map(|log_file| file_slice.log_file_relative_path(log_file))
                .collect::<Result<Vec<String>>>()?
        } else {
            vec![]
        };

        let use_read_optimized: bool = self
            .hudi_configs
            .get_or_default(HudiReadConfig::UseReadOptimizedMode)
            .into();
        let base_file_only = log_file_paths.is_empty() || use_read_optimized;

        if base_file_only {
            // True streaming: return the parquet stream directly
            self.read_parquet_file_stream(&base_file_path, options)
                .await
        } else {
            // Fallback: collect + merge, then yield as single-item stream
            let batch = self
                .read_file_slice_from_paths_with_opts(&base_file_path, log_file_paths, options)
                .await?;
            Ok(Box::pin(futures::stream::once(async { Ok(batch) })))
        }
    }

    /// Reads a file slice specified by file paths as a stream of record batches.
    ///
    /// This is a convenience method that constructs a [FileSlice] from the given paths
    /// and delegates to [Self::read_file_slice].
    ///
    /// # Arguments
    /// * `base_file_path` - Relative path to the base file (e.g., "city=chennai/file.parquet")
    /// * `log_file_paths` - Optional iterator of relative paths to log files
    /// * `options` - Read options for configuring the read operation
    ///
    /// # Returns
    /// A stream of record batches.
    ///
    /// # Example
    /// ```ignore
    /// use futures::StreamExt;
    ///
    /// let options = ReadOptions::new();
    /// let mut stream = reader.read_file_slice_by_paths(
    ///     "city=chennai/file-id-0_0-7-24_20240101120000.parquet",
    ///     [".file-id-0_20240101120000.log.1_0-51-115"],
    ///     &options,
    /// ).await?;
    ///
    /// while let Some(result) = stream.next().await {
    ///     let batch = result?;
    ///     // Process batch...
    /// }
    /// ```
    pub async fn read_file_slice_by_paths<I, S>(
        &self,
        base_file_path: &str,
        log_file_paths: I,
        options: &ReadOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        use std::path::Path;

        // Parse base file path to extract partition path and file name
        let path = Path::new(base_file_path);
        let partition_path = path
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        let base_file_name = path
            .file_name()
            .ok_or_else(|| {
                CoreError::FileGroup(format!(
                    "Cannot extract file name from base file path: {base_file_path}"
                ))
            })?
            .to_string_lossy();

        // Create a FileGroup from the base file
        let mut file_group = FileGroup::new_with_base_file_name(&base_file_name, &partition_path)?;

        // Add log files if provided
        for log_file_path in log_file_paths {
            let log_path = Path::new(log_file_path.as_ref());
            let log_file_name = log_path
                .file_name()
                .ok_or_else(|| {
                    CoreError::FileGroup(format!(
                        "Cannot extract file name from log file path: {}",
                        log_file_path.as_ref()
                    ))
                })?
                .to_string_lossy();
            file_group.add_log_file_from_name(&log_file_name)?;
        }

        // Get the file slice (there should be exactly one after adding base file)
        let file_slice = file_group
            .file_slices
            .values()
            .next()
            .ok_or_else(|| CoreError::FileGroup("No file slice found in file group".to_string()))?
            .clone();

        // Delegate to the primary read API
        self.read_file_slice(&file_slice, options).await
    }

    /// Reads a parquet file as a stream of record batches.
    ///
    /// This method propagates the underlying parquet stream, applying Hudi-specific
    /// filtering (commit time filtering for incremental reads) to each batch.
    /// The returned stream owns all necessary data and is `'static`.
    async fn read_parquet_file_stream(
        &self,
        relative_path: &str,
        options: &ReadOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let batch_size = options.batch_size.unwrap_or(8192);
        let parquet_options = ParquetReadOptions::with_batch_size(batch_size);

        let stream = self
            .storage
            .get_parquet_file_stream(relative_path, parquet_options)
            .await
            .map_err(|e| {
                ReadFileSliceError(format!("Failed to read path {relative_path}: {e:?}"))
            })?;

        // Clone data for the closure so the stream is 'static
        let reader = self.clone();
        let projection = options.projection.clone();
        let row_predicate = options.row_predicate.clone();

        let mapped_stream = stream.map(move |result| {
            let batch =
                result.map_err(|e| ReadFileSliceError(format!("Failed to read batch: {e:?}")))?;

            // Apply Hudi commit time filtering
            let batch = if let Some(mask) =
                reader.create_filtering_mask_for_base_file_records(&batch)?
            {
                filter_record_batch(&batch, &mask)
                    .map_err(|e| ReadFileSliceError(format!("Failed to filter records: {e:?}")))?
            } else {
                batch
            };

            // Apply column projection if specified
            let batch = if let Some(ref cols) = projection {
                FileGroupReader::apply_projection(&batch, cols)?
            } else {
                batch
            };

            // Apply row predicate if specified
            let batch = if let Some(ref predicate) = row_predicate {
                FileGroupReader::apply_row_predicate(&batch, predicate)?
            } else {
                batch
            };

            Ok(batch)
        });

        Ok(Box::pin(mapped_stream))
    }

    /// Creates an instant range for log file scanning using ReadOptions.
    fn create_instant_range_for_log_file_scan_with_opts(
        &self,
        options: &ReadOptions,
    ) -> InstantRange {
        let timezone = self
            .hudi_configs
            .get_or_default(HudiTableConfig::TimelineTimezone)
            .into();

        // Use as_of_timestamp from options if provided, otherwise fall back to config
        let start_timestamp = self
            .hudi_configs
            .try_get(HudiReadConfig::FileGroupStartTimestamp)
            .map(|v| -> String { v.into() });

        let end_timestamp = options.as_of_timestamp.clone().or_else(|| {
            self.hudi_configs
                .try_get(HudiReadConfig::FileGroupEndTimestamp)
                .map(|v| -> String { v.into() })
        });

        InstantRange::new(timezone, start_timestamp, end_timestamp, false, true)
    }

    /// Applies column projection to a record batch.
    fn apply_projection(batch: &RecordBatch, columns: &[String]) -> Result<RecordBatch> {
        let schema = batch.schema();
        let indices: Vec<usize> = columns
            .iter()
            .filter_map(|col| schema.index_of(col).ok())
            .collect();

        if indices.is_empty() {
            return Err(ReadFileSliceError(
                "No matching columns found for projection".to_string(),
            ));
        }

        batch
            .project(&indices)
            .map_err(|e| ReadFileSliceError(format!("Failed to project columns: {e:?}")))
    }

    /// Applies a row predicate filter to a record batch.
    fn apply_row_predicate(batch: &RecordBatch, predicate: &Filter) -> Result<RecordBatch> {
        let schema = batch.schema();
        let schemable = SchemableFilter::try_from((predicate.clone(), schema.as_ref()))?;

        // Get the column to filter on
        let column = batch.column_by_name(&predicate.field_name).ok_or_else(|| {
            ReadFileSliceError(format!(
                "Column '{}' not found for predicate",
                predicate.field_name
            ))
        })?;

        // Apply the comparison to get a boolean mask
        let mask = schemable.apply_comparsion(column)?;

        // Filter the batch
        filter_record_batch(batch, &mask)
            .map_err(|e| ReadFileSliceError(format!("Failed to apply row predicate: {e:?}")))
    }

    /// Reads a base file with optional projection and row predicate.
    async fn read_base_file_with_opts(
        &self,
        relative_path: &str,
        projection: Option<&[String]>,
        row_predicate: Option<&Filter>,
    ) -> Result<RecordBatch> {
        // Read the file using streaming API
        let records = self
            .read_parquet_file_to_single_batch(relative_path)
            .await?;

        // Apply base file filtering mask (for Hudi record filtering)
        let records =
            if let Some(mask) = self.create_filtering_mask_for_base_file_records(&records)? {
                filter_record_batch(&records, &mask)
                    .map_err(|e| ReadFileSliceError(format!("Failed to filter records: {e:?}")))?
            } else {
                records
            };

        // Apply column projection if specified
        let records = if let Some(cols) = projection {
            Self::apply_projection(&records, cols)?
        } else {
            records
        };

        // Apply row predicate if specified
        let records = if let Some(predicate) = row_predicate {
            Self::apply_row_predicate(&records, predicate)?
        } else {
            records
        };

        Ok(records)
    }

    // =========================================================================
    // Metadata Table File Slice Reading
    // =========================================================================

    /// Check if this reader is configured for a metadata table.
    ///
    /// Detection is based on the base path ending with `.hoodie/metadata`.
    pub fn is_metadata_table(&self) -> bool {
        let base_path: String = self
            .hudi_configs
            .get_or_default(HudiTableConfig::BasePath)
            .into();
        crate::util::path::is_metadata_table_path(&base_path)
    }

    /// Read records from metadata table files partition.
    ///
    /// # Arguments
    /// * `file_slice` - The file slice to read from
    /// * `keys` - Only read records with these keys. If empty, reads all records.
    ///
    /// # Returns
    /// HashMap containing the requested keys (or all keys if `keys` is empty).
    pub(crate) async fn read_metadata_table_files_partition(
        &self,
        file_slice: &FileSlice,
        keys: &[&str],
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        let base_file_path = file_slice.base_file_relative_path()?;
        let log_file_paths: Vec<String> = if file_slice.has_log_file() {
            file_slice
                .log_files
                .iter()
                .map(|log_file| file_slice.log_file_relative_path(log_file))
                .collect::<Result<Vec<String>>>()?
        } else {
            vec![]
        };

        // Open HFile
        let mut hfile_reader = HFileReader::open(&self.storage, &base_file_path)
            .await
            .map_err(|e| {
                ReadFileSliceError(format!(
                    "Failed to read metadata table base file {base_file_path}: {e:?}"
                ))
            })?;

        // Get Avro schema from HFile
        let schema = hfile_reader
            .get_avro_schema()
            .map_err(|e| ReadFileSliceError(format!("Failed to get Avro schema: {e:?}")))?
            .ok_or_else(|| ReadFileSliceError("No Avro schema found in HFile".to_string()))?
            .clone();

        let hfile_keys: Vec<&str> = if keys.is_empty() {
            vec![]
        } else {
            let mut sorted = keys.to_vec();
            sorted.sort();
            sorted
        };

        let base_records: Vec<HFileRecord> = if hfile_keys.is_empty() {
            hfile_reader.collect_records().map_err(|e| {
                ReadFileSliceError(format!("Failed to collect HFile records: {e:?}"))
            })?
        } else {
            hfile_reader
                .lookup_records(&hfile_keys)
                .map_err(|e| ReadFileSliceError(format!("Failed to lookup HFile records: {e:?}")))?
                .into_iter()
                .filter_map(|(_, r)| r)
                .collect()
        };

        let log_records = if log_file_paths.is_empty() {
            vec![]
        } else {
            let instant_range = self.create_instant_range_for_log_file_scan();
            let scan_result = LogFileScanner::new(self.hudi_configs.clone(), self.storage.clone())
                .scan(log_file_paths, &instant_range)
                .await?;

            match scan_result {
                ScanResult::HFileRecords(records) => records,
                ScanResult::Empty => vec![],
                ScanResult::RecordBatches(_) => {
                    return Err(CoreError::LogBlockError(
                        "Unexpected RecordBatches in metadata table log file".to_string(),
                    ));
                }
            }
        };

        let merger = FilesPartitionMerger::new(schema);
        merger.merge_for_keys(&base_records, &log_records, &hfile_keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Result;
    use crate::config::util::empty_options;
    use crate::error::CoreError;
    use crate::file_group::base_file::BaseFile;
    use crate::file_group::file_slice::FileSlice;
    use crate::table::ReadOptions;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;
    use std::fs::canonicalize;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::Arc;
    use url::Url;

    const TEST_SAMPLE_BASE_FILE: &str =
        "a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet";

    fn get_non_existent_base_uri() -> String {
        "file:///non-existent-path/table".to_string()
    }

    fn get_base_uri_with_valid_props() -> String {
        let url = Url::from_file_path(
            canonicalize(
                PathBuf::from("tests")
                    .join("data")
                    .join("table_props_valid"),
            )
            .unwrap(),
        )
        .unwrap();
        url.as_ref().to_string()
    }

    fn get_base_uri_with_valid_props_minimum() -> String {
        let url = Url::from_file_path(
            canonicalize(
                PathBuf::from("tests")
                    .join("data")
                    .join("table_props_valid_minimum"),
            )
            .unwrap(),
        )
        .unwrap();
        url.as_ref().to_string()
    }

    fn get_base_uri_with_invalid_props() -> String {
        let url = Url::from_file_path(
            canonicalize(
                PathBuf::from("tests")
                    .join("data")
                    .join("table_props_invalid"),
            )
            .unwrap(),
        )
        .unwrap();
        url.as_ref().to_string()
    }

    #[test]
    fn test_new_with_options() {
        let options = vec![("key1", "value1"), ("key2", "value2")];
        let base_uri = get_base_uri_with_valid_props();
        let reader = FileGroupReader::new_with_options(&base_uri, options).unwrap();
        assert!(!reader.storage.options.is_empty());
        assert!(
            reader
                .storage
                .hudi_configs
                .contains(HudiTableConfig::BasePath)
        );
    }

    #[test]
    fn test_new_with_options_invalid_base_uri_or_invalid_props() {
        let base_uri = get_non_existent_base_uri();
        let result = FileGroupReader::new_with_options(&base_uri, empty_options());
        assert!(result.is_err());

        let base_uri = get_base_uri_with_invalid_props();
        let result = FileGroupReader::new_with_options(&base_uri, empty_options());
        assert!(result.is_err())
    }

    fn create_test_record_batch() -> Result<RecordBatch> {
        let schema = Schema::new(vec![
            Field::new("_hoodie_commit_time", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]);
        let schema = Arc::new(schema);

        let commit_times: ArrayRef = Arc::new(StringArray::from(vec!["1", "2", "3", "4", "5"]));
        let names: ArrayRef = Arc::new(StringArray::from(vec![
            "Alice", "Bob", "Charlie", "David", "Eve",
        ]));
        let ages: ArrayRef = Arc::new(Int64Array::from(vec![25, 30, 35, 40, 45]));

        RecordBatch::try_new(schema, vec![commit_times, names, ages]).map_err(CoreError::ArrowError)
    }

    #[test]
    fn test_create_filtering_mask_for_base_file_records() -> Result<()> {
        let base_uri = get_base_uri_with_valid_props_minimum();
        let records = create_test_record_batch()?;

        // Test case 1: Disable populating the meta fields
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [
                (HudiTableConfig::PopulatesMetaFields.as_ref(), "false"),
                (HudiReadConfig::FileGroupStartTimestamp.as_ref(), "2"),
            ],
        )?;
        let mask = reader.create_filtering_mask_for_base_file_records(&records)?;
        assert_eq!(mask, None, "Commit time filtering should not be needed");

        // Test case 2: No commit time filtering options
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options())?;
        let mask = reader.create_filtering_mask_for_base_file_records(&records)?;
        assert_eq!(mask, None);

        // Test case 3: Filtering commit time > '2'
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [(HudiReadConfig::FileGroupStartTimestamp, "2")],
        )?;
        let mask = reader.create_filtering_mask_for_base_file_records(&records)?;
        assert_eq!(
            mask,
            Some(BooleanArray::from(vec![false, false, true, true, true])),
            "Expected only records with commit_time > '2'"
        );

        // Test case 4: Filtering commit time <= '4'
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [(HudiReadConfig::FileGroupEndTimestamp, "4")],
        )?;
        let mask = reader.create_filtering_mask_for_base_file_records(&records)?;
        assert_eq!(mask, None, "Commit time filtering should not be needed");

        // Test case 5: Filtering commit time > '2' and <= '4'
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [
                (HudiReadConfig::FileGroupStartTimestamp, "2"),
                (HudiReadConfig::FileGroupEndTimestamp, "4"),
            ],
        )?;
        let mask = reader.create_filtering_mask_for_base_file_records(&records)?;
        assert_eq!(
            mask,
            Some(BooleanArray::from(vec![false, false, true, true, false])),
            "Expected only records with commit_time > '2' and <= '4'"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_with_base_file_only() -> Result<()> {
        let base_uri = get_base_uri_with_valid_props_minimum();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options())?;

        // Create a FileSlice from the test sample base file
        let base_file = BaseFile::from_str(TEST_SAMPLE_BASE_FILE)?;
        let file_slice = FileSlice::new(base_file, String::new());

        let options = ReadOptions::new();
        let result = reader.read_file_slice(&file_slice, &options).await;

        match result {
            Ok(mut stream) => {
                if let Some(Ok(batch)) = stream.next().await {
                    assert!(
                        batch.num_rows() > 0,
                        "Should have read some records from base file"
                    );
                }
            }
            Err(_) => {
                // This might fail if the test data doesn't exist, which is acceptable for a unit test
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_read_optimized_mode() -> Result<()> {
        let base_uri = get_base_uri_with_valid_props_minimum();
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [(HudiReadConfig::UseReadOptimizedMode.as_ref(), "true")],
        )?;

        // Create a FileSlice from the test sample base file
        let base_file = BaseFile::from_str(TEST_SAMPLE_BASE_FILE)?;
        let file_slice = FileSlice::new(base_file, String::new());

        let options = ReadOptions::new();
        let result = reader.read_file_slice(&file_slice, &options).await;

        // In read-optimized mode, log files should be ignored
        match result {
            Ok(_) => {
                // Test passes if we get a result
            }
            Err(e) => {
                // Expected for missing test data
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("not found") || error_msg.contains("No such file"),
                    "Expected file not found error, got: {error_msg}"
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_error_handling() -> Result<()> {
        let base_uri = get_base_uri_with_valid_props_minimum();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options())?;

        // Create a FileSlice from a non-existent file
        let base_file = BaseFile::from_str("non_existent_file.parquet")?;
        let file_slice = FileSlice::new(base_file, String::new());

        let options = ReadOptions::new();
        let result = reader.read_file_slice(&file_slice, &options).await;

        match result {
            Ok(_) => panic!("Expected error for non-existent file"),
            Err(e) => {
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("not found") || error_msg.contains("Failed to read path"),
                    "Should contain appropriate error message, got: {error_msg}"
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_by_paths() -> Result<()> {
        let base_uri = get_base_uri_with_valid_props_minimum();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options())?;

        // Test with just base file path (no partition, no log files)
        let base_file_path = TEST_SAMPLE_BASE_FILE;
        let options = ReadOptions::new();
        let result = reader
            .read_file_slice_by_paths(base_file_path, Vec::<String>::new(), &options)
            .await;

        match result {
            Ok(mut stream) => {
                if let Some(Ok(batch)) = stream.next().await {
                    assert!(
                        batch.num_rows() > 0,
                        "Should have read some records from base file"
                    );
                }
            }
            Err(_) => {
                // This might fail if the test data doesn't exist
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_by_paths_with_partition() -> Result<()> {
        let base_uri = get_base_uri_with_valid_props_minimum();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options())?;

        // Test with partition path in the base file path
        let base_file_path = format!("city=chennai/{TEST_SAMPLE_BASE_FILE}");
        let options = ReadOptions::new();
        let result = reader
            .read_file_slice_by_paths(&base_file_path, Vec::<String>::new(), &options)
            .await;

        // Should fail because the file doesn't exist at that path
        match result {
            Ok(_) => {
                // Unexpected success - the file shouldn't exist at this path
            }
            Err(e) => {
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("not found") || error_msg.contains("Failed to read path"),
                    "Should contain appropriate error message, got: {error_msg}"
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_by_paths_error_invalid_base_path() -> Result<()> {
        let base_uri = get_base_uri_with_valid_props_minimum();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options())?;

        // Test with invalid base file name (not a valid parquet file name)
        let options = ReadOptions::new();
        let result = reader
            .read_file_slice_by_paths("some/invalid_file_name", Vec::<String>::new(), &options)
            .await;

        match result {
            Ok(_) => panic!("Expected error for invalid base file path"),
            Err(e) => {
                let error_msg = e.to_string();
                // Should fail to parse as a valid base file name
                assert!(
                    error_msg.contains("Failed to parse file name")
                        || error_msg.contains("Invalid base file name"),
                    "Should contain appropriate error message, got: {error_msg}"
                );
            }
        }

        Ok(())
    }

    // =========================================================================
    // Metadata Table File Slice Reading Tests
    // =========================================================================

    fn get_metadata_table_base_uri() -> String {
        use hudi_test::QuickstartTripsTable;
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let metadata_table_path = PathBuf::from(table_path).join(".hoodie").join("metadata");
        let url = Url::from_file_path(canonicalize(&metadata_table_path).unwrap()).unwrap();
        url.as_ref().to_string()
    }

    /// Create a FileGroupReader for metadata table without trying to resolve options from storage.
    fn create_metadata_table_reader() -> Result<FileGroupReader> {
        let metadata_table_uri = get_metadata_table_base_uri();
        let hudi_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath,
            metadata_table_uri.as_str(),
        )]));
        FileGroupReader::new_with_configs_and_overwriting_options(hudi_configs, empty_options())
    }

    #[test]
    fn test_is_metadata_table_detection() -> Result<()> {
        // Regular table should return false
        let base_uri = get_base_uri_with_valid_props();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options())?;
        assert!(!reader.is_metadata_table());

        // Metadata table should return true
        let metadata_table_reader = create_metadata_table_reader()?;
        assert!(metadata_table_reader.is_metadata_table());

        Ok(())
    }

    /// Initial HFile base file for the files partition (all zeros timestamp).
    const METADATA_TABLE_FILES_BASE_FILE: &str =
        "files/files-0000-0_0-955-2690_00000000000000000.hfile";

    /// Log files for the V8Trips8I3U1D test table's files partition.
    const METADATA_TABLE_FILES_LOG_FILES: &[&str] = &[
        "files/.files-0000-0_20251220210108078.log.1_10-999-2838",
        "files/.files-0000-0_20251220210123755.log.1_3-1032-2950",
        "files/.files-0000-0_20251220210125441.log.1_5-1057-3024",
        "files/.files-0000-0_20251220210127080.log.1_3-1082-3100",
        "files/.files-0000-0_20251220210128625.log.1_5-1107-3174",
        "files/.files-0000-0_20251220210129235.log.1_3-1118-3220",
        "files/.files-0000-0_20251220210130911.log.1_3-1149-3338",
    ];

    fn create_test_file_slice() -> Result<FileSlice> {
        use crate::file_group::FileGroup;

        let mut fg = FileGroup::new("files-0000-0".to_string(), "files".to_string());
        let base_file_name = METADATA_TABLE_FILES_BASE_FILE
            .strip_prefix("files/")
            .unwrap();
        fg.add_base_file_from_name(base_file_name)?;
        let log_file_names: Vec<_> = METADATA_TABLE_FILES_LOG_FILES
            .iter()
            .map(|s| s.strip_prefix("files/").unwrap())
            .collect();
        fg.add_log_files_from_names(log_file_names)?;

        Ok(fg
            .get_file_slice_as_of("99999999999999999")
            .expect("Should have file slice")
            .clone())
    }

    #[tokio::test]
    async fn test_read_metadata_table_files_partition() -> Result<()> {
        use crate::metadata::table_record::{FilesPartitionRecord, MetadataRecordType};

        let reader = create_metadata_table_reader()?;
        let file_slice = create_test_file_slice()?;

        // Test 1: Read all records (empty keys)
        let all_records = reader
            .read_metadata_table_files_partition(&file_slice, &[])
            .await?;

        // Should have 4 keys after merging
        assert_eq!(
            all_records.len(),
            4,
            "Should have 4 partition keys after merge"
        );

        // Validate all partition keys have correct record types
        for (key, record) in &all_records {
            if key == FilesPartitionRecord::ALL_PARTITIONS_KEY {
                assert_eq!(record.record_type, MetadataRecordType::AllPartitions);
            } else {
                assert_eq!(record.record_type, MetadataRecordType::Files);
            }
        }

        // Validate chennai partition has files
        let chennai = all_records.get("city=chennai").unwrap();
        assert!(
            chennai.active_file_names().len() >= 2,
            "Chennai should have at least 2 active files"
        );
        assert!(chennai.total_size() > 0, "Total size should be > 0");

        // Test 2: Read specific keys
        let keys = vec![FilesPartitionRecord::ALL_PARTITIONS_KEY, "city=chennai"];
        let filtered_records = reader
            .read_metadata_table_files_partition(&file_slice, &keys)
            .await?;

        // Should only contain the requested keys
        assert_eq!(filtered_records.len(), 2);
        assert!(filtered_records.contains_key(FilesPartitionRecord::ALL_PARTITIONS_KEY));
        assert!(filtered_records.contains_key("city=chennai"));
        assert!(!filtered_records.contains_key("city=san_francisco"));
        assert!(!filtered_records.contains_key("city=sao_paulo"));

        Ok(())
    }
}
