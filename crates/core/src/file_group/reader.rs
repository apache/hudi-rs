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
use crate::config::read::{HudiReadConfig, MergeEngineValue};
use crate::config::table::{BaseFileFormatValue, HudiTableConfig};
use crate::error::CoreError;
use crate::error::CoreError::ReadFileSliceError;
use crate::expr::filter::{
    Filter, SchemableFilter, filters_to_row_mask, validate_fields_against_schemas,
};
use crate::file_group::base_file::reader::{
    BaseFileReadOptions, BaseFileReader, create_base_file_reader,
};
use crate::file_group::file_slice::FileSlice;
use crate::file_group::log_file::scanner::{LogFileScanner, ScanResult};
use crate::file_group::record_batches::RecordBatches;
use crate::merge::record_merger::RecordMerger;
use crate::metadata::meta_field::MetaField;
use crate::storage::Storage;
use crate::storage::error::StorageError;
use crate::table::ReadOptions;
use crate::table::builder::OptionResolver;
use crate::timeline::selector::InstantRange;
use crate::util::arrow::project_batch_by_names;
use arrow::compute::and;
use arrow::compute::filter_record_batch;
use arrow_array::{BooleanArray, RecordBatch};
use futures::stream::BoxStream;
use futures::{StreamExt, TryFutureExt};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::FromStr;
use std::sync::Arc;

/// The reader that handles all read operations against a file group.
#[derive(Clone)]
pub struct FileGroupReader {
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
    base_file_format: BaseFileFormatValue,
    base_file_reader: Option<Arc<dyn BaseFileReader>>,
    /// The schema to read a slice with, when the caller knows the table's
    /// current one.
    ///
    /// The base file's own schema is stale whenever a later writer widened a
    /// column or added one: its values would be forced back into the narrower
    /// base types. A caller holding the timeline knows better; one reading from
    /// paths alone (the cxx bridge) does not, and falls back to the base file.
    data_schema_override: Option<arrow_schema::SchemaRef>,
}

impl std::fmt::Debug for FileGroupReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileGroupReader")
            .field("hudi_configs", &self.hudi_configs)
            .field("storage", &self.storage)
            .field("base_file_format", &self.base_file_format)
            .finish_non_exhaustive()
    }
}

impl FileGroupReader {
    /// Creates a new reader from base Hudi configs plus pre-split per-call
    /// overrides — Hudi configs and storage options live in separate maps so
    /// callers can't accidentally cross the streams.
    ///
    /// `extra_hudi_opts` extends `hudi_configs` (last-writer-wins). `storage_opts`
    /// is the full storage option set for this reader (table-level + any overrides
    /// the caller has already merged in).
    ///
    /// This API does **not** use [`OptionResolver`] that loads table properties
    /// from storage to resolve options — callers supply final configs.
    pub(crate) fn new_with_overrides(
        hudi_configs: Arc<HudiConfigs>,
        extra_hudi_opts: HashMap<String, String>,
        storage_opts: HashMap<String, String>,
    ) -> Result<Self> {
        let mut final_opts = hudi_configs.as_options();
        final_opts.extend(extra_hudi_opts);
        let hudi_configs = Arc::new(HudiConfigs::new(final_opts));
        let storage = Storage::new(Arc::new(storage_opts), hudi_configs.clone())?;
        let format = BaseFileFormatValue::resolve_from_configs(&hudi_configs, None)?;
        let base_file_reader = Self::create_optional_base_file_reader(&storage, &format)?;

        Ok(Self {
            hudi_configs,
            storage,
            base_file_format: format,
            base_file_reader,
            data_schema_override: None,
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
    pub async fn new_with_options<I, K, V>(base_uri: &str, options: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let mut resolver = OptionResolver::new_with_options(base_uri, options);
        resolver.resolve_options().await?;
        let hudi_configs = Arc::new(HudiConfigs::new(resolver.hudi_options));
        let storage = Storage::new(Arc::new(resolver.storage_options), hudi_configs.clone())?;
        let format = BaseFileFormatValue::resolve_from_configs(&hudi_configs, None)?;
        let base_file_reader = Self::create_optional_base_file_reader(&storage, &format)?;

        Ok(Self {
            hudi_configs,
            storage,
            base_file_format: format,
            base_file_reader,
            data_schema_override: None,
        })
    }

    fn resolve_read_options(&self, options: &ReadOptions) -> Result<ReadOptions> {
        options.with_defaults_from(&self.hudi_configs)
    }

    fn create_optional_base_file_reader(
        storage: &Arc<Storage>,
        format: &BaseFileFormatValue,
    ) -> Result<Option<Arc<dyn BaseFileReader>>> {
        match create_base_file_reader(storage, format) {
            Ok(reader) => Ok(Some(reader)),
            Err(StorageError::UnsupportedBaseFileFormat(_))
                if matches!(format, BaseFileFormatValue::HFile) =>
            {
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Returns the base-file reader for a path, reusing the cached reader when
    /// the path resolves to the same format selected at construction time.
    fn reader_for_path(&self, relative_path: &str) -> Result<Arc<dyn BaseFileReader>> {
        let format =
            BaseFileFormatValue::resolve_from_configs(&self.hudi_configs, Some(relative_path))?;

        if format == self.base_file_format
            && let Some(reader) = &self.base_file_reader
        {
            return Ok(reader.clone());
        }

        create_base_file_reader(&self.storage, &format)
            .map_err(|e| ReadFileSliceError(format!("{e}")))
    }

    /// Internal: read base file + apply commit-time filter, no [`ReadOptions`] applied.
    /// Used by the merge path so options aren't applied prematurely before merging
    /// with log files.
    async fn read_base_file_eager(&self, relative_path: &str) -> Result<RecordBatch> {
        // A file slice whose records live entirely in log files has no base
        // file, and reports its path as empty.
        if relative_path.is_empty() {
            return Ok(RecordBatch::new_empty(MetaField::schema()));
        }
        let reader = self.reader_for_path(relative_path)?;
        let records: RecordBatch = reader
            .read_data(relative_path, BaseFileReadOptions::default())
            .map_err(|e| ReadFileSliceError(format!("Failed to read path {relative_path}: {e:?}")))
            .await?;
        apply_commit_time_filter(&self.hudi_configs, records)
    }

    /// Visible to the crate so the merge-on-read context resolver can assert it
    /// derives the same range; see `reader_v2::resolver`.
    pub(crate) fn create_instant_range_for_log_file_scan(&self) -> Result<InstantRange> {
        let timezone = self
            .hudi_configs
            .get_or_default(HudiTableConfig::TimelineTimezone)
            .into();
        let start_timestamp = self
            .hudi_configs
            .try_get(HudiReadConfig::StartTimestamp)?
            .map(|v| -> String { v.into() });
        let end_timestamp = self
            .hudi_configs
            .try_get(HudiReadConfig::EndTimestamp)?
            .map(|v| -> String { v.into() });
        Ok(InstantRange::new(
            timezone,
            start_timestamp,
            end_timestamp,
            false,
            true,
        ))
    }

    /// Which merge implementation should serve this read.
    ///
    /// Defaults to the one that has always served it. A metadata table always
    /// uses that one regardless of the setting: its base files and log blocks
    /// are HFile, which the merge-on-read reader cannot read at all.
    fn merge_engine(&self) -> Result<MergeEngineValue> {
        if self.is_metadata_table() {
            return Ok(MergeEngineValue::Legacy);
        }
        // Read the raw value rather than going through `get_or_default`, which
        // falls back to the default when a value fails to parse. A typo in the
        // engine name would then silently read with the other engine, which is
        // the one outcome this switch must not produce.
        match self
            .hudi_configs
            .as_options()
            .get(HudiReadConfig::MergeEngine.as_ref())
        {
            Some(raw) => MergeEngineValue::from_str(raw).map_err(CoreError::Config),
            None => Ok(MergeEngineValue::default()),
        }
    }

    /// Read slices with `schema` rather than whatever the base file carries.
    pub(crate) fn set_data_schema(&mut self, schema: arrow_schema::SchemaRef) {
        self.data_schema_override = Some(schema);
    }

    /// The schema the merge-on-read reader needs up front, taken from the base
    /// file itself.
    ///
    /// This is what the existing path effectively reads with, so the two engines
    /// start from the same types. It is also what the data actually has: under
    /// schema evolution `hoodie.table.create.schema` records the table as it was
    /// created, and the engine evolves each batch to the required schema anyway.
    ///
    /// Reading the footer costs one request. The engine reads it again when it
    /// opens the file; collapsing the two is worth doing but is not this change.
    async fn resolved_data_schema(
        &self,
        base_file_path: &str,
    ) -> Result<Option<arrow_schema::SchemaRef>> {
        if let Some(schema) = &self.data_schema_override {
            return Ok(Some(schema.clone()));
        }
        let stream = self
            .reader_for_path(base_file_path)?
            .read_stream(base_file_path, BaseFileReadOptions::default())
            .await
            .map_err(|e| {
                ReadFileSliceError(format!(
                    "Failed to read base file schema '{base_file_path}': {e:?}"
                ))
            })?;
        Ok(Some(stream.schema().clone()))
    }

    /// Read one slice through the merge-on-read reader.
    ///
    /// Filters and projection are not applied here: the caller runs the result
    /// through the same `apply_eager_options` the other paths use, so the two
    /// engines cannot disagree about what a filter means.
    async fn read_via_v2(
        &self,
        base_file_path: &str,
        log_file_paths: Vec<String>,
    ) -> Result<RecordBatch> {
        let data_schema = self.resolved_data_schema(base_file_path).await?;
        if let Some(reason) = crate::file_group::reader_v2::adapter::refuse_reason(
            self.is_metadata_table(),
            data_schema.as_ref(),
        ) {
            return Err(reason);
        }

        // The partition a slice lives in is the directory its base file sits
        // in; a non-partitioned table yields an empty path, which is correct.
        let partition_path = std::path::Path::new(base_file_path)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();

        log::info!("reading '{base_file_path}' with the merge-on-read engine");
        let merged = crate::file_group::reader_v2::adapter::read_file_slice(
            self.hudi_configs.clone(),
            self.storage.clone(),
            base_file_path,
            log_file_paths,
            partition_path,
            data_schema,
        )
        .await?;

        // An incremental read wants the rows that changed in its window, and
        // the engine decides that per file rather than per row: a base file
        // written by compaction carries records from every commit it merged, so
        // admitting the file admits all of them. The same mask the existing
        // reader applies narrows it back to the window.
        //
        // Applied after the merge rather than before it, because a row's commit
        // time is whichever record won. A base row updated inside the window
        // keeps the update's time and stays; one updated outside it keeps the
        // base's time and goes.
        apply_commit_time_filter(&self.hudi_configs, merged)
    }

    /// Reads the data from the given file slice.
    ///
    /// See [`Self::read_file_slice_from_paths`] for how `options` is applied.
    pub async fn read_file_slice(
        &self,
        file_slice: &FileSlice,
        options: &ReadOptions,
    ) -> Result<RecordBatch> {
        let base_file_path = file_slice.base_file_relative_path()?;
        let log_file_paths = if file_slice.has_log_file() {
            file_slice
                .log_files
                .iter()
                .map(|log_file| file_slice.log_file_relative_path(log_file))
                .collect::<Result<Vec<String>>>()?
        } else {
            vec![]
        };
        // A slice with no base file reads entirely from its logs; the engine
        // takes an empty base path for that.
        self.read_file_slice_from_paths(
            base_file_path.as_deref().unwrap_or(""),
            log_file_paths,
            options,
        )
        .await
    }

    /// Reads a file slice from a base file and a list of log files.
    ///
    /// `options.filters` are applied as a row-level mask after reading;
    /// `options.projection` selects columns. Both apply to the merged result.
    /// Other fields (`as_of_timestamp`, `start_timestamp`, `end_timestamp`, `batch_size`)
    /// are not meaningful for eager reads and are ignored.
    pub async fn read_file_slice_from_paths<I, S>(
        &self,
        base_file_path: &str,
        log_file_paths: I,
        options: &ReadOptions,
    ) -> Result<RecordBatch>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let options = self.resolve_read_options(options)?;
        let log_file_paths: Vec<String> = log_file_paths
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
        let base_file_only = log_file_paths.is_empty() || options.is_read_optimized()?;

        // Read-optimized means the log files are not read at all, so hand the
        // engine none rather than letting it merge them. A slice with no log
        // files reduces to a base file read either way.
        let merged = if self.merge_engine()? == MergeEngineValue::V2 {
            let log_file_paths = if base_file_only {
                Vec::new()
            } else {
                log_file_paths
            };
            self.read_via_v2(base_file_path, log_file_paths).await?
        } else if base_file_only {
            self.read_base_file_eager(base_file_path).await?
        } else {
            let instant_range = self.create_instant_range_for_log_file_scan()?;
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

            let base_batch = self.read_base_file_eager(base_file_path).await?;
            let schema = base_batch.schema();
            let num_data_batches = log_batches.num_data_batches() + 1;
            let num_delete_batches = log_batches.num_delete_batches();
            let mut all_batches =
                RecordBatches::new_with_capacity(num_data_batches, num_delete_batches);
            all_batches.push_data_batch(base_batch);
            all_batches.extend(log_batches);

            let merger = RecordMerger::new(schema.clone(), self.hudi_configs.clone());
            merger.merge_record_batches(all_batches)?
        };

        apply_eager_options(&options, merged)
    }

    // =========================================================================
    // Streaming Read APIs
    // =========================================================================

    /// Reads a file slice as a stream of record batches.
    ///
    /// This is the streaming version of [FileGroupReader::read_file_slice].
    /// It returns a stream that yields record batches as they are read.
    ///
    /// For COW tables or read-optimized mode (base file only), this returns a true
    /// streaming iterator from the underlying base file (Parquet or Lance), yielding
    /// batches as they are read without loading all data into memory.
    ///
    /// For MOR tables with log files, this falls back to the collect-and-merge approach
    /// and yields the merged result as a single batch. Streaming merge of base files
    /// with log files is not yet implemented.
    ///
    /// # Arguments
    /// * `file_slice` - The file slice to read.
    /// * `options` - Read options for configuring the read operation.
    ///
    /// # Returns
    /// A stream of record batches. The stream owns all necessary data and is `'static`.
    ///
    /// # Example
    /// ```ignore
    /// use futures::StreamExt;
    ///
    /// let options = ReadOptions::new().with_batch_size(4096);
    /// let mut stream = reader.read_file_slice_stream(&file_slice, &options).await?;
    ///
    /// while let Some(result) = stream.next().await {
    ///     let batch = result?;
    ///     // Process batch...
    /// }
    /// ```
    pub async fn read_file_slice_stream(
        &self,
        file_slice: &FileSlice,
        options: &ReadOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let base_file_path = file_slice.base_file_relative_path()?;
        let known_base_file_size = file_slice
            .base_file
            .as_ref()
            .and_then(|f| f.file_metadata.as_ref())
            .map(|metadata| metadata.size);
        let log_file_paths: Vec<String> = if file_slice.has_log_file() {
            file_slice
                .log_files
                .iter()
                .map(|log_file| file_slice.log_file_relative_path(log_file))
                .collect::<Result<Vec<String>>>()?
        } else {
            vec![]
        };

        self.read_file_slice_from_paths_stream_inner(
            base_file_path.as_deref().unwrap_or(""),
            log_file_paths,
            options,
            known_base_file_size,
        )
        .await
    }

    /// Reads a file slice from paths as a stream of record batches.
    ///
    /// This is the streaming version of [FileGroupReader::read_file_slice_from_paths].
    ///
    /// # Arguments
    /// * `base_file_path` - Relative path to the base file.
    /// * `log_file_paths` - Iterator of relative paths to log files.
    /// * `options` - Read options for configuring the read operation.
    ///
    /// # Returns
    /// A stream of record batches.
    pub async fn read_file_slice_from_paths_stream<I, S>(
        &self,
        base_file_path: &str,
        log_file_paths: I,
        options: &ReadOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.read_file_slice_from_paths_stream_inner(base_file_path, log_file_paths, options, None)
            .await
    }

    async fn read_file_slice_from_paths_stream_inner<I, S>(
        &self,
        base_file_path: &str,
        log_file_paths: I,
        options: &ReadOptions,
        known_base_file_size: Option<u64>,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let options = self.resolve_read_options(options)?;
        if options.is_read_optimized()? {
            return self
                .read_base_file_stream(base_file_path, &options, known_base_file_size)
                .await;
        }

        let log_file_paths: Vec<String> = log_file_paths
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();

        if log_file_paths.is_empty() {
            self.read_base_file_stream(base_file_path, &options, known_base_file_size)
                .await
        } else {
            // Fallback: collect + merge, then yield as single-item stream
            let batch = self
                .read_file_slice_from_paths(base_file_path, log_file_paths, &options)
                .await?;
            Ok(Box::pin(futures::stream::once(async { Ok(batch) })))
        }
    }

    /// Reads a base file as a stream of record batches.
    ///
    /// Supports the following [ReadOptions]:
    /// - `batch_size`: Controls the number of rows per batch
    /// - `projection`: Pushes column selection to the base-file reader level
    /// - `filters`: Applied as a row-level mask after reading each batch (in addition to
    ///   any pruning that already happened upstream)
    async fn read_base_file_stream(
        &self,
        relative_path: &str,
        options: &ReadOptions,
        known_file_size: Option<u64>,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let default_batch_size: usize = self
            .hudi_configs
            .get_or_default(HudiReadConfig::StreamBatchSize)
            .into();
        let batch_size = options.batch_size()?.unwrap_or(default_batch_size);
        let mut read_options = BaseFileReadOptions::default().with_batch_size(batch_size);
        if let Some(size) = known_file_size {
            read_options = read_options.with_known_file_size(size);
        }

        // If projection is set, widen the base file read to also include any columns
        // we need post-read but the user didn't request:
        //   - filter fields, so the row-level mask can evaluate them
        //   - `_hoodie_commit_time`, when commit-time filtering is active
        //     (PopulatesMetaFields + StartTimestamp)
        // The widened columns are dropped by the final projection step below.
        //
        // We only exclude partition column filter fields from widening when
        // `hoodie.datasource.write.drop.partition.columns` is enabled — otherwise
        // partition columns are still present in parquet (e.g. with timestamp-based
        // keygen, the source data column is also configured as a partition field).
        // Excluding them unconditionally would silently drop legitimate row filters
        // on those columns.
        let drops_partition_columns: bool = self
            .hudi_configs
            .get_or_default(HudiTableConfig::DropsPartitionFields)
            .into();
        let dropped_partition_columns: Vec<String> = if drops_partition_columns {
            self.hudi_configs
                .get_or_default(HudiTableConfig::PartitionFields)
                .into()
        } else {
            Vec::new()
        };
        let needs_commit_time_col: bool = {
            let populates_meta_fields: bool = self
                .hudi_configs
                .get_or_default(HudiTableConfig::PopulatesMetaFields)
                .into();
            let has_start_ts = self
                .hudi_configs
                .try_get(HudiReadConfig::StartTimestamp)?
                .is_some();
            populates_meta_fields && has_start_ts
        };
        let final_projection = options.projection.clone();
        let read_projection = options.projection.as_ref().map(|proj| {
            let mut combined: Vec<String> = proj.clone();
            for filter in &options.filters {
                let field = filter.field.as_str();
                if dropped_partition_columns.iter().any(|p| p == field) {
                    continue;
                }
                if !combined.iter().any(|c| c == field) {
                    combined.push(field.to_string());
                }
            }
            if needs_commit_time_col {
                let commit_time = MetaField::CommitTime.as_ref().to_string();
                if !combined.iter().any(|c| c == &commit_time) {
                    combined.push(commit_time);
                }
            }
            combined
        });
        if let Some(ref cols) = read_projection {
            read_options = read_options.with_projection(cols.clone());
        }

        let hudi_configs = self.hudi_configs.clone();
        let path = relative_path.to_string();
        let filters = Arc::new(options.filters.clone());
        let final_projection = Arc::new(final_projection);
        // Validate once on first batch so typoed filter columns surface as errors
        // rather than silent no-ops in `filters_to_row_mask`.
        let validated = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let reader = self.reader_for_path(&path)?;
        let base_stream = reader
            .read_stream(&path, read_options)
            .map_err(|e| ReadFileSliceError(format!("Failed to read path {path}: {e:?}")))
            .await?;

        // Apply filtering: commit time → structured filters → final projection.
        let stream = base_stream.into_stream().filter_map(move |result| {
            let hudi_configs = hudi_configs.clone();
            let filters = filters.clone();
            let final_projection = final_projection.clone();
            let validated = validated.clone();
            async move {
                match result {
                    Err(e) => Some(Err(ReadFileSliceError(format!(
                        "Failed to read batch: {e:?}"
                    )))),
                    Ok(batch) => {
                        if !validated.load(std::sync::atomic::Ordering::Relaxed) {
                            if let Err(e) =
                                validate_fields_against_schemas(&filters, [batch.schema().as_ref()])
                            {
                                return Some(Err(e));
                            }
                            validated.store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                        let batch = match apply_commit_time_filter(&hudi_configs, batch) {
                            Err(e) => return Some(Err(e)),
                            Ok(b) if b.num_rows() == 0 => return None,
                            Ok(b) => b,
                        };
                        let batch = match apply_filter_mask(&filters, batch) {
                            Err(e) => return Some(Err(e)),
                            Ok(b) if b.num_rows() == 0 => return None,
                            Ok(b) => b,
                        };
                        // Project down to the user's requested columns (no-op if we
                        // didn't have to widen the read projection).
                        let batch = match project_batch_by_names(batch, final_projection.as_deref())
                        {
                            Err(e) => return Some(Err(e)),
                            Ok(b) => b,
                        };
                        Some(Ok(batch))
                    }
                }
            }
        });

        Ok(Box::pin(stream))
    }

    // =========================================================================
    // Metadata table
    //
    // Only the predicate lives here. Reading a metadata table file slice is a
    // different job — HFile base files and HFile log blocks — and lives in
    // `metadata::table::reader`. This stays because it is public API, and
    // because a caller choosing a read path needs to ask the question.
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
}

/// Creates a commit time filtering mask based on the provided configs.
///
/// Returns `None` if no filtering is needed (meta fields disabled or no start timestamp).
fn create_commit_time_filter_mask(
    hudi_configs: &HudiConfigs,
    batch: &RecordBatch,
) -> Result<Option<BooleanArray>> {
    let populates_meta_fields: bool = hudi_configs
        .get_or_default(HudiTableConfig::PopulatesMetaFields)
        .into();
    if !populates_meta_fields {
        return Ok(None);
    }

    let start_ts: Option<String> = hudi_configs
        .try_get(HudiReadConfig::StartTimestamp)?
        .map(|v| v.into());
    if start_ts.is_none() {
        return Ok(None);
    }

    let mut and_filters: Vec<SchemableFilter> = Vec::new();
    let schema = MetaField::schema();

    if let Some(start) = start_ts {
        let filter = Filter::try_from((MetaField::CommitTime.as_ref(), ">", start.as_str()))?;
        and_filters.push(SchemableFilter::try_from((filter, schema.as_ref()))?);
    }

    if let Some(end) = hudi_configs
        .try_get(HudiReadConfig::EndTimestamp)?
        .map(|v| -> String { v.into() })
    {
        let filter = Filter::try_from((MetaField::CommitTime.as_ref(), "<=", end.as_str()))?;
        and_filters.push(SchemableFilter::try_from((filter, schema.as_ref()))?);
    }

    if and_filters.is_empty() {
        return Ok(None);
    }

    let mut mask = BooleanArray::from(vec![true; batch.num_rows()]);
    for filter in &and_filters {
        let col_name = filter.field.name().as_str();
        let col_values = batch
            .column_by_name(col_name)
            .ok_or_else(|| ReadFileSliceError(format!("Column {col_name} not found")))?;
        let comparison = filter.apply_comparison(col_values)?;
        mask = and(&mask, &comparison)?;
    }

    Ok(Some(mask))
}

/// Apply structured filters and projection to an eager [`RecordBatch`].
///
/// All `options.filters` must target columns present in the batch — at file-group
/// level no upstream partition pruning has happened, so a filter on a column that
/// isn't in the batch can never apply and is rejected with a schema error. Callers
/// going through `Table` strip filters on dropped partition columns before reaching
/// here; direct `FileGroupReader` callers must not pass such filters.
fn apply_eager_options(options: &ReadOptions, batch: RecordBatch) -> Result<RecordBatch> {
    validate_fields_against_schemas(&options.filters, [batch.schema().as_ref()])?;
    let batch = apply_filter_mask(&options.filters, batch)?;
    project_batch_by_names(batch, options.projection.as_deref())
}

/// Apply commit time filtering to a record batch.
fn apply_commit_time_filter(hudi_configs: &HudiConfigs, batch: RecordBatch) -> Result<RecordBatch> {
    match create_commit_time_filter_mask(hudi_configs, &batch)? {
        Some(mask) => filter_record_batch(&batch, &mask)
            .map_err(|e| ReadFileSliceError(format!("Failed to filter records: {e:?}"))),
        None => Ok(batch),
    }
}

/// Apply structured filters as a row mask on the batch.
///
/// Filters whose field is not present in the batch (e.g., partition columns already
/// pruned upstream) are skipped — see [`crate::expr::filter::filters_to_row_mask`].
fn apply_filter_mask(filters: &[Filter], batch: RecordBatch) -> Result<RecordBatch> {
    if filters.is_empty() {
        return Ok(batch);
    }
    let mask = filters_to_row_mask(filters, &batch)?;
    filter_record_batch(&batch, &mask)
        .map_err(|e| ReadFileSliceError(format!("Failed to apply filter mask: {e:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Result;
    use crate::config::util::empty_options;
    use crate::error::CoreError;
    use crate::file_group::base_file::BaseFile;
    use crate::file_group::file_slice::FileSlice;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
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

    #[tokio::test]
    async fn test_new_with_options() {
        let options = vec![("key1", "value1"), ("key2", "value2")];
        let base_uri = get_base_uri_with_valid_props();
        let reader = FileGroupReader::new_with_options(&base_uri, options)
            .await
            .unwrap();
        assert!(!reader.storage.options.is_empty());
        assert!(
            reader
                .storage
                .hudi_configs
                .contains(HudiTableConfig::BasePath)
        );
    }

    #[tokio::test]
    async fn test_new_with_options_resolves_table_properties_from_storage() {
        // The minimum-props fixture's hoodie.properties carries TableType,
        // TableName, and TableVersion. With empty user options, the
        // OptionResolver must read them off storage and seed hudi_configs —
        // otherwise downstream commit-time / merge logic would fall back to
        // defaults and silently misbehave on real tables.
        let base_uri = get_base_uri_with_valid_props_minimum();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options())
            .await
            .unwrap();

        let table_type: String = reader
            .hudi_configs
            .get(HudiTableConfig::TableType)
            .unwrap()
            .into();
        assert_eq!(table_type, "COPY_ON_WRITE");
        let table_name: String = reader
            .hudi_configs
            .get(HudiTableConfig::TableName)
            .unwrap()
            .into();
        assert_eq!(table_name, "trips");
        let table_version: isize = reader
            .hudi_configs
            .get(HudiTableConfig::TableVersion)
            .unwrap()
            .into();
        assert_eq!(table_version, 6);
    }

    #[tokio::test]
    async fn test_new_with_options_invalid_base_uri_or_invalid_props() {
        let base_uri = get_non_existent_base_uri();
        let result = FileGroupReader::new_with_options(&base_uri, empty_options()).await;
        assert!(result.is_err());

        let base_uri = get_base_uri_with_invalid_props();
        let result = FileGroupReader::new_with_options(&base_uri, empty_options()).await;
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

    #[tokio::test]
    async fn test_create_commit_time_filter_mask() -> Result<()> {
        let base_uri = get_base_uri_with_valid_props_minimum();
        let records = create_test_record_batch()?;

        // Test case 1: Disable populating the meta fields
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [
                (HudiTableConfig::PopulatesMetaFields.as_ref(), "false"),
                (HudiReadConfig::StartTimestamp.as_ref(), "2"),
            ],
        )
        .await?;
        let mask = create_commit_time_filter_mask(&reader.hudi_configs, &records)?;
        assert_eq!(mask, None, "Commit time filtering should not be needed");

        // Test case 2: No commit time filtering options
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options()).await?;
        let mask = create_commit_time_filter_mask(&reader.hudi_configs, &records)?;
        assert_eq!(mask, None);

        // Test case 3: Filtering commit time > '2'
        let reader =
            FileGroupReader::new_with_options(&base_uri, [(HudiReadConfig::StartTimestamp, "2")])
                .await?;
        let mask = create_commit_time_filter_mask(&reader.hudi_configs, &records)?;
        assert_eq!(
            mask,
            Some(BooleanArray::from(vec![false, false, true, true, true])),
            "Expected only records with commit_time > '2'"
        );

        // Test case 4: Filtering commit time <= '4'
        let reader =
            FileGroupReader::new_with_options(&base_uri, [(HudiReadConfig::EndTimestamp, "4")])
                .await?;
        let mask = create_commit_time_filter_mask(&reader.hudi_configs, &records)?;
        assert_eq!(mask, None, "Commit time filtering should not be needed");

        // Test case 5: Filtering commit time > '2' and <= '4'
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [
                (HudiReadConfig::StartTimestamp, "2"),
                (HudiReadConfig::EndTimestamp, "4"),
            ],
        )
        .await?;
        let mask = create_commit_time_filter_mask(&reader.hudi_configs, &records)?;
        assert_eq!(
            mask,
            Some(BooleanArray::from(vec![false, false, true, true, false])),
            "Expected only records with commit_time > '2' and <= '4'"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_from_paths_eager_with_real_base_file() -> Result<()> {
        // Real-fixture eager read covers: option resolution, base-file-only branch,
        // reader_for_path delegation, and apply_eager_options pass-through (no
        // filters, no projection, no commit-time mask).
        let (base_uri, base_file_name) = v8np_base_uri_and_first_parquet();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options()).await?;

        let batch = reader
            .read_file_slice_from_paths(&base_file_name, Vec::<&str>::new(), &ReadOptions::new())
            .await?;
        assert!(batch.num_rows() > 0, "expected at least one row");
        let schema = batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"id"));
        assert!(field_names.contains(&"name"));

        // Same path via FileSlice; results must agree row-for-row with the
        // direct paths variant (covers the FileSlice → paths conversion).
        let base_file = BaseFile::from_str(&base_file_name)?;
        let file_slice = FileSlice::new(base_file, String::new());
        let via_slice = reader
            .read_file_slice(&file_slice, &ReadOptions::new())
            .await?;
        assert_eq!(via_slice.num_rows(), batch.num_rows());
        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_from_paths_read_optimized_ignores_log_files() -> Result<()> {
        // In read-optimized mode the log file paths must be ignored. We pass a
        // bogus log path; the call would error if it were not skipped.
        let (base_uri, base_file_name) = v8np_base_uri_and_first_parquet();
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [(HudiReadConfig::UseReadOptimizedMode.as_ref(), "true")],
        )
        .await?;
        let bogus_log = vec![".does-not-exist.log.1_0-0-0".to_string()];

        let batch = reader
            .read_file_slice_from_paths(&base_file_name, bogus_log, &ReadOptions::new())
            .await?;
        assert!(batch.num_rows() > 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_from_paths_error_handling() -> Result<()> {
        let base_uri = get_base_uri_with_valid_props_minimum();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options()).await?;

        // Test with non-existent base file
        let base_file_path = "non_existent_file.parquet";
        let log_file_paths: Vec<&str> = vec![];

        let result = reader
            .read_file_slice_from_paths(base_file_path, log_file_paths, &ReadOptions::new())
            .await;

        assert!(result.is_err(), "Should return error for non-existent file");

        let error_msg = result
            .expect_err("Expected file not found error")
            .to_string();
        assert!(
            error_msg.contains("not found") || error_msg.contains("Failed to read path"),
            "Should contain appropriate error message, got: {error_msg}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hfile_base_file_read_reports_unsupported_reader() -> Result<()> {
        let base_uri = get_base_uri_with_valid_props_minimum();
        let hudi_configs = Arc::new(HudiConfigs::new([
            (HudiTableConfig::BasePath, base_uri.as_str()),
            (
                HudiTableConfig::BaseFileFormat,
                BaseFileFormatValue::HFile.as_ref(),
            ),
        ]));
        let reader =
            FileGroupReader::new_with_overrides(hudi_configs, HashMap::new(), HashMap::new())?;

        let result = reader
            .read_file_slice_from_paths(
                "fileid_0-0-1_20240418173551906.hfile",
                Vec::<&str>::new(),
                &ReadOptions::new(),
            )
            .await;

        let error_msg = result
            .expect_err("Expected unsupported HFile reader error")
            .to_string();
        assert!(
            error_msg.contains("Unsupported base file format")
                && error_msg.contains("hfile is only supported"),
            "Expected explicit unsupported HFile reader error, got: {error_msg}"
        );

        Ok(())
    }

    #[test]
    fn test_reader_for_path_reuses_cached_default_parquet_reader() -> Result<()> {
        let base_uri = get_base_uri_with_valid_props_minimum();
        let reader = create_test_reader(&base_uri)?;
        let cached_reader = reader
            .base_file_reader
            .as_ref()
            .expect("default Parquet reader should be cached");

        let resolved_reader = reader.reader_for_path(TEST_SAMPLE_BASE_FILE)?;

        assert!(
            Arc::ptr_eq(cached_reader, &resolved_reader),
            "no-config Parquet path should reuse the cached base-file reader"
        );

        let hfile_error = match reader.reader_for_path("fileid_0-0-1_20240418173551906.hfile") {
            Ok(_) => panic!("no-config HFile path should still use extension detection"),
            Err(err) => err.to_string(),
        };
        assert!(
            hfile_error.contains("Unsupported base file format")
                && hfile_error.contains("hfile is only supported"),
            "Expected no-config HFile path to report unsupported reader, got: {hfile_error}"
        );

        Ok(())
    }

    // =========================================================================
    // Streaming API Tests
    // =========================================================================

    /// Helper to create a FileGroupReader without using block_on (safe for async tests).
    fn create_test_reader(base_uri: &str) -> Result<FileGroupReader> {
        let hudi_configs = Arc::new(HudiConfigs::new([(HudiTableConfig::BasePath, base_uri)]));
        FileGroupReader::new_with_overrides(hudi_configs, HashMap::new(), HashMap::new())
    }

    /// Returns a (base_uri, base_file_name) tuple for the V8Nonpartitioned
    /// fixture. The file name is the lexically smallest `.parquet` at the
    /// table root so the choice is deterministic across platforms.
    fn v8np_base_uri_and_first_parquet() -> (String, String) {
        use hudi_test::SampleTable;
        let table_path = SampleTable::V8Nonpartitioned.path_to_cow();
        let base_url = Url::from_directory_path(&table_path).unwrap();
        let mut parquet_names: Vec<String> = std::fs::read_dir(&table_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        parquet_names.sort();
        let first = parquet_names
            .into_iter()
            .next()
            .expect("V8Nonpartitioned fixture must contain at least one parquet file");
        (base_url.as_str().to_string(), first)
    }

    #[tokio::test]
    async fn test_read_file_slice_stream_with_real_base_file_and_small_batches() -> Result<()> {
        use futures::StreamExt;

        // Real-fixture streaming read covers the full pipeline:
        //   read_file_slice_stream → read_file_slice_from_paths_stream_inner →
        //   read_base_file_stream (no projection, no filters, no commit-time mask).
        // A batch_size of 1 forces lance/parquet to yield multiple batches and
        // verifies row totals match an eager read.
        let (base_uri, base_file_name) = v8np_base_uri_and_first_parquet();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options()).await?;
        let base_file = BaseFile::from_str(&base_file_name)?;
        let file_slice = FileSlice::new(base_file, String::new());

        let eager = reader
            .read_file_slice(&file_slice, &ReadOptions::new())
            .await?;
        let expected_rows = eager.num_rows();
        assert!(expected_rows > 0);

        let options = ReadOptions::new().with_batch_size(1)?;
        let mut stream = reader.read_file_slice_stream(&file_slice, &options).await?;
        let mut batches = Vec::new();
        while let Some(batch_result) = stream.next().await {
            batches.push(batch_result?);
        }
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, expected_rows);
        assert!(
            batches.len() > 1,
            "batch_size=1 should split into multiple batches"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_stream_uses_known_base_file_size_when_present() -> Result<()> {
        use crate::storage::file_metadata::FileMetadata;
        use futures::StreamExt;

        // Populating `base_file.file_metadata` causes `read_file_slice_stream`
        // to plumb `known_base_file_size` through the inner method into
        // `BaseFileReadOptions::with_known_file_size`. This test exercises that
        // branch end-to-end and asserts the stream still produces the same rows
        // as a metadata-less read.
        let (base_uri, base_file_name) = v8np_base_uri_and_first_parquet();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options()).await?;

        let mut base_file = BaseFile::from_str(&base_file_name)?;
        // Real on-disk size, so the parquet reader treats the cached size as truth.
        let table_path = hudi_test::SampleTable::V8Nonpartitioned.path_to_cow();
        let real_size = std::fs::metadata(PathBuf::from(&table_path).join(&base_file_name))
            .unwrap()
            .len();
        base_file.file_metadata = Some(FileMetadata::new(&base_file_name, real_size));
        let file_slice = FileSlice::new(base_file, String::new());

        let options = ReadOptions::new();
        let mut stream = reader.read_file_slice_stream(&file_slice, &options).await?;
        let mut batches = Vec::new();
        while let Some(batch_result) = stream.next().await {
            batches.push(batch_result?);
        }
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0);

        // Sanity-check: same call without populated metadata reads the same rows.
        let mut bare_slice = file_slice.clone();
        bare_slice.base_file.as_mut().unwrap().file_metadata = None;
        let bare_total: usize = {
            let mut s = reader.read_file_slice_stream(&bare_slice, &options).await?;
            let mut sum = 0;
            while let Some(b) = s.next().await {
                sum += b?.num_rows();
            }
            sum
        };
        assert_eq!(total_rows, bare_total);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_base_file_stream_widens_projection_for_filter_columns() -> Result<()> {
        use futures::StreamExt;

        // The streaming read widens the read projection to include any column
        // referenced by `options.filters` so the row-mask can evaluate against
        // it; the user-requested projection is then applied as a final step.
        // Here we project only `id` but filter on `intField`, then assert the
        // emitted batches expose only `id` and the filter actually pruned rows.
        let (base_uri, base_file_name) = v8np_base_uri_and_first_parquet();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options()).await?;

        // Establish baseline row count from an unfiltered read.
        let base_file = BaseFile::from_str(&base_file_name)?;
        let file_slice = FileSlice::new(base_file, String::new());
        let baseline = reader
            .read_file_slice(&file_slice, &ReadOptions::new())
            .await?;
        assert!(baseline.num_rows() >= 2);

        let options = ReadOptions::new()
            .with_projection(["id"])
            .with_filters([("intField", "=", "15000")])?;
        let mut stream = reader.read_file_slice_stream(&file_slice, &options).await?;

        let mut batches = Vec::new();
        while let Some(b) = stream.next().await {
            batches.push(b?);
        }
        assert!(!batches.is_empty());
        for batch in &batches {
            assert_eq!(
                batch.num_columns(),
                1,
                "final projection must drop the widened column"
            );
            assert_eq!(batch.schema().field(0).name(), "id");
        }
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(
            total_rows < baseline.num_rows(),
            "filter on intField=15000 should prune some rows; got {total_rows} of {baseline_rows}",
            baseline_rows = baseline.num_rows()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_stream_error_on_invalid_file() -> Result<()> {
        let base_uri = get_base_uri_with_valid_props_minimum();
        let reader = create_test_reader(&base_uri)?;

        // Valid file name format but path doesn't exist; covers the error
        // pathway from `reader_for_path → read_stream`.
        let base_file = BaseFile::from_str(
            "00000000-0000-0000-0000-000000000000-0_0-0-0_00000000000000000.parquet",
        )?;
        let file_slice = FileSlice::new(base_file, String::new());

        let result = reader
            .read_file_slice_stream(&file_slice, &ReadOptions::default())
            .await;
        let err = match result {
            Ok(_) => panic!("Should return error for non-existent file"),
            Err(e) => e,
        };
        let error_msg = err.to_string();
        assert!(
            error_msg.contains("Failed to read path")
                || error_msg.contains("not found")
                || error_msg.contains("No such file")
                || error_msg.contains("Object at location"),
            "Expected file not found error, got: {error_msg}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_stream_rejects_unknown_filter_field() -> Result<()> {
        // Covers the per-batch `validate_fields_against_schemas` guard in the
        // streaming path: a filter on a column that the read projection
        // resolves to but isn't actually in the file's schema must surface a
        // schema error from the first batch instead of silently no-op'ing.
        use futures::StreamExt;

        let (base_uri, base_file_name) = v8np_base_uri_and_first_parquet();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options()).await?;
        let base_file = BaseFile::from_str(&base_file_name)?;
        let file_slice = FileSlice::new(base_file, String::new());

        let options = ReadOptions::new().with_filters([("definitely_not_a_column", "=", "x")])?;
        let mut stream = reader.read_file_slice_stream(&file_slice, &options).await?;
        let first = stream
            .next()
            .await
            .expect("stream should yield at least one item (the validation error)");
        let err = first.expect_err("validation must fail for unknown filter column");
        assert!(
            err.to_string().contains("definitely_not_a_column"),
            "expected schema-validation error mentioning the unknown column, got: {err}"
        );
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

    #[tokio::test]
    async fn test_is_metadata_table_detection() -> Result<()> {
        // Regular table should return false
        let base_uri = get_base_uri_with_valid_props();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options()).await?;
        assert!(!reader.is_metadata_table());

        // Metadata table should return true
        let metadata_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath,
            get_metadata_table_base_uri().as_str(),
        )]));
        let metadata_table_reader =
            FileGroupReader::new_with_overrides(metadata_configs, HashMap::new(), HashMap::new())?;
        assert!(metadata_table_reader.is_metadata_table());

        Ok(())
    }

    /// Locate a (partition, base_file, single_log_file) triple for a MOR
    /// file slice in the V8Trips8I3U1D fixture so we can read it through
    /// `FileGroupReader` directly.
    fn v8_trips_mor_first_slice() -> (String, String, String, String) {
        use hudi_test::QuickstartTripsTable;
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let base_url = Url::from_directory_path(&table_path).unwrap();
        // San Francisco partition has both an initial parquet base file and a
        // `.log.1_0-...` log file written before the compaction commit.
        let partition = "city=san_francisco";
        let partition_dir = PathBuf::from(&table_path).join(partition);
        let mut parquet_files: Vec<String> = std::fs::read_dir(&partition_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".parquet") && !name.starts_with('.'))
            .collect();
        parquet_files.sort();
        let base_file_name = parquet_files
            .into_iter()
            .next()
            .expect("san_francisco partition must contain a base parquet file");
        let log_file_name = std::fs::read_dir(&partition_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .find(|name| {
                name.starts_with('.') && name.contains(".log.") && !name.starts_with(".hoodie")
            })
            .expect("san_francisco partition must contain a log file");
        (
            base_url.as_str().to_string(),
            partition.to_string(),
            base_file_name,
            log_file_name,
        )
    }

    #[test]
    fn test_filegroupreader_debug_format_includes_struct_fields() -> Result<()> {
        // Exercises the manual Debug impl on FileGroupReader (the auto-derive
        // is opted out so internal trait objects aren't formatted).
        let base_uri = get_base_uri_with_valid_props_minimum();
        let reader = create_test_reader(&base_uri)?;
        let formatted = format!("{reader:?}");
        assert!(formatted.contains("FileGroupReader"));
        assert!(formatted.contains("hudi_configs"));
        assert!(formatted.contains("storage"));
        assert!(formatted.contains("base_file_format"));
        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_eager_with_real_log_files_merges() -> Result<()> {
        // Eager MOR read with at least one log file exercises the merge branch
        // of `read_file_slice_from_paths`: log scanning, RecordBatches scan
        // result, and the RecordMerger pass that combines base + log batches.
        let (base_uri, partition, base_file_name, log_file_name) = v8_trips_mor_first_slice();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options()).await?;

        let base_path = format!("{partition}/{base_file_name}");
        let log_path = format!("{partition}/{log_file_name}");

        let merged = reader
            .read_file_slice_from_paths(&base_path, vec![log_path], &ReadOptions::new())
            .await?;
        assert!(merged.num_rows() > 0);
        let schema = merged.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"uuid"));
        assert!(field_names.contains(&"rider"));
        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_stream_with_log_files_yields_single_merged_batch() -> Result<()> {
        use futures::StreamExt;

        // Streaming read with log files falls back to the eager collect+merge
        // path and yields the merged result as a single-item stream — covers
        // the `else` branch of `read_file_slice_from_paths_stream_inner`.
        let (base_uri, partition, base_file_name, log_file_name) = v8_trips_mor_first_slice();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options()).await?;

        let base_path = format!("{partition}/{base_file_name}");
        let log_path = format!("{partition}/{log_file_name}");

        let mut stream = reader
            .read_file_slice_from_paths_stream(&base_path, vec![log_path], &ReadOptions::new())
            .await?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        assert_eq!(
            batches.len(),
            1,
            "MOR stream fallback must emit exactly one merged batch"
        );
        assert!(batches[0].num_rows() > 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_from_paths_stream_no_log_files_streams_base_file() -> Result<()> {
        use futures::StreamExt;

        // Public `read_file_slice_from_paths_stream` entry point with no log
        // files takes the streaming `read_base_file_stream` branch (rather
        // than the collect+merge fallback).
        let (base_uri, base_file_name) = v8np_base_uri_and_first_parquet();
        let reader = FileGroupReader::new_with_options(&base_uri, empty_options()).await?;

        let mut stream = reader
            .read_file_slice_from_paths_stream(
                &base_file_name,
                Vec::<&str>::new(),
                &ReadOptions::new().with_batch_size(1)?,
            )
            .await?;
        let mut total_rows = 0;
        let mut batch_count = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            total_rows += batch.num_rows();
            batch_count += 1;
        }
        assert!(total_rows > 0);
        assert!(
            batch_count > 1,
            "no-log-files stream with batch_size=1 should split into multiple batches"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_from_paths_stream_read_optimized_skips_log_files() -> Result<()> {
        use futures::StreamExt;

        // With read-optimized mode set, the streaming path must short-circuit
        // through `read_base_file_stream` even when log paths are supplied —
        // the bogus log path here would otherwise fail the call.
        let (base_uri, base_file_name) = v8np_base_uri_and_first_parquet();
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [(HudiReadConfig::UseReadOptimizedMode.as_ref(), "true")],
        )
        .await?;
        let bogus_log = vec![".does-not-exist.log.1_0-0-0".to_string()];

        let mut stream = reader
            .read_file_slice_from_paths_stream(&base_file_name, bogus_log, &ReadOptions::new())
            .await?;
        let mut total_rows = 0;
        while let Some(batch) = stream.next().await {
            total_rows += batch?.num_rows();
        }
        assert!(total_rows > 0);
        Ok(())
    }

    /// The default must keep using the reader that has always served reads. If
    /// this ever flips by accident, every other test still passes — they do not
    /// say which engine produced their result — so it is asserted directly.
    #[tokio::test]
    async fn merge_engine_defaults_to_legacy() -> Result<()> {
        use hudi_test::SampleTable;
        let base_url = SampleTable::V9TxnsSimpleMeta.url_to_mor_avro();
        let reader = FileGroupReader::new_with_options(base_url.as_ref(), empty_options()).await?;
        assert_eq!(reader.merge_engine()?, MergeEngineValue::Legacy);
        Ok(())
    }

    /// A metadata table ignores the setting. Its base files and log blocks are
    /// HFile, which the merge-on-read reader cannot read, so honoring `v2`
    /// there would fail somewhere deeper and less clearly.
    #[tokio::test]
    async fn metadata_table_stays_on_the_legacy_engine() -> Result<()> {
        let configs = Arc::new(HudiConfigs::new([
            (
                HudiTableConfig::BasePath.as_ref(),
                get_metadata_table_base_uri(),
            ),
            (HudiReadConfig::MergeEngine.as_ref(), "v2".to_string()),
        ]));
        let reader = FileGroupReader::new_with_overrides(configs, HashMap::new(), HashMap::new())?;
        assert!(reader.is_metadata_table());
        assert_eq!(reader.merge_engine()?, MergeEngineValue::Legacy);
        Ok(())
    }

    /// Asking for the merge-on-read engine routes to it. Without this the
    /// switch could be wired to nothing and every test would still pass.
    #[tokio::test]
    async fn v2_engine_is_selected_when_asked_for() -> Result<()> {
        use hudi_test::SampleTable;
        let base_url = SampleTable::V9TxnsSimpleMeta.url_to_mor_avro();
        let reader = FileGroupReader::new_with_options(
            base_url.as_ref(),
            [(HudiReadConfig::MergeEngine.as_ref(), "v2")],
        )
        .await?;
        assert_eq!(reader.merge_engine()?, MergeEngineValue::V2);
        Ok(())
    }

    /// Selecting the merge-on-read engine must not change what a base-file-only
    /// read returns.
    ///
    /// Both engines now serve this read — the merge-on-read one reduces to a
    /// base file read — so this compares their output directly. It is the one
    /// comparison they can be held to without settling the merge-mode question
    /// first: with no log files there is no merge to disagree about.
    #[tokio::test]
    async fn asking_for_v2_does_not_change_a_base_file_only_read() -> Result<()> {
        let (base_uri, base_file_name) = v8np_base_uri_and_first_parquet();

        let legacy = FileGroupReader::new_with_options(&base_uri, empty_options())
            .await?
            .read_file_slice_from_paths(&base_file_name, Vec::<&str>::new(), &ReadOptions::new())
            .await?;

        let v2_reader = FileGroupReader::new_with_options(
            &base_uri,
            [(HudiReadConfig::MergeEngine.as_ref(), "v2")],
        )
        .await?;
        assert_eq!(v2_reader.merge_engine()?, MergeEngineValue::V2);
        let v2 = v2_reader
            .read_file_slice_from_paths(&base_file_name, Vec::<&str>::new(), &ReadOptions::new())
            .await?;

        assert_eq!(v2.num_rows(), legacy.num_rows());
        assert_eq!(v2.schema(), legacy.schema());
        assert_eq!(v2, legacy);
        Ok(())
    }

    /// Read-optimized mode means the log files are not read, and asking for the
    /// merge-on-read engine must not resurrect them.
    #[tokio::test]
    async fn asking_for_v2_does_not_defeat_read_optimized_mode() -> Result<()> {
        let (base_uri, base_file_name) = v8np_base_uri_and_first_parquet();
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [
                (HudiReadConfig::MergeEngine.as_ref(), "v2"),
                (HudiReadConfig::UseReadOptimizedMode.as_ref(), "true"),
            ],
        )
        .await?;

        // A path that cannot be read: the call only succeeds if the log files
        // were dropped rather than handed to the engine.
        let bogus_log = vec![".does-not-exist.log.1_0-0-0".to_string()];
        let batch = reader
            .read_file_slice_from_paths(&base_file_name, bogus_log, &ReadOptions::new())
            .await?;
        assert!(batch.num_rows() > 0);
        Ok(())
    }

    /// A slice with log files, read with the merge-on-read engine, actually
    /// reads.
    ///
    /// Selecting an engine and reaching it are different things: every other
    /// test here asserts the selection, which a dispatch wired to nothing would
    /// still satisfy. This one goes through the dispatch to the engine and back
    /// out through `apply_eager_options`.
    #[tokio::test]
    async fn v2_reads_a_slice_with_log_files_through_the_dispatch() -> Result<()> {
        let (base_uri, partition, base_file_name, log_file_name) = v8_trips_mor_first_slice();
        let base_path = format!("{partition}/{base_file_name}");
        let log_path = format!("{partition}/{log_file_name}");

        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [(HudiReadConfig::MergeEngine.as_ref(), "v2")],
        )
        .await?;
        let batch = reader
            .read_file_slice_from_paths(&base_path, vec![log_path.clone()], &ReadOptions::new())
            .await?;

        assert!(batch.num_rows() > 0, "expected a merged result");

        let legacy = FileGroupReader::new_with_options(&base_uri, empty_options())
            .await?
            .read_file_slice_from_paths(&base_path, vec![log_path.clone()], &ReadOptions::new())
            .await?;
        assert_eq!(batch.num_rows(), legacy.num_rows(), "row count differs");
        Ok(())
    }

    /// No row an incremental read returns may sit outside its window.
    ///
    /// The slice is a compacted base file with a later log file. Compaction
    /// merges records from many commits into one file while keeping each
    /// record's own commit time, so a window that admits the *file* still has to
    /// exclude most of its rows — the merge-on-read engine decides per file and
    /// would otherwise return them all.
    ///
    /// This asserts the property rather than proving the difference: the
    /// fixture's compacted slice holds a single record, and the log record in
    /// the window replaces it, so the stale row is collapsed by the merge either
    /// way. No fixture here has a compacted base row that survives unmatched,
    /// which is what would make the two engines visibly disagree.
    #[tokio::test]
    async fn incremental_reads_return_nothing_outside_the_window() -> Result<()> {
        use arrow_array::Array;
        use hudi_test::QuickstartTripsTable;
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let base_uri = Url::from_directory_path(&table_path).unwrap().to_string();

        // Compaction wrote this base file; the log file came after it.
        let base_path = "city=sao_paulo/\
8aa68f7e-afd6-4c94-b86c-8a886552e08d-0_2-1112-3192_20251220210129235.parquet";
        let log_path = "city=sao_paulo/\
.8aa68f7e-afd6-4c94-b86c-8a886552e08d-0_20251220210130911.log.1_0-1139-3316";

        // Opens just before the compaction, so the base file is in range while
        // the records it merged from earlier commits are not.
        let start = "20251220210129000";
        let end = "20251220210131000";

        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [
                (HudiReadConfig::MergeEngine.as_ref(), "v2"),
                (HudiReadConfig::StartTimestamp.as_ref(), start),
                (HudiReadConfig::EndTimestamp.as_ref(), end),
            ],
        )
        .await?;
        let batch = reader
            .read_file_slice_from_paths(base_path, vec![log_path], &ReadOptions::new())
            .await?;

        assert!(batch.num_rows() > 0, "the window has to contain something");
        let times = batch
            .column_by_name(MetaField::CommitTime.as_ref())
            .expect("commit time column");
        let times = times
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("commit time is a string");
        for i in 0..times.len() {
            assert!(
                times.value(i) > start && times.value(i) <= end,
                "row {i} has commit time {} outside ({start}, {end}]",
                times.value(i)
            );
        }
        Ok(())
    }

    /// An unknown value fails at the read rather than silently falling back to
    /// a different engine than the caller asked for.
    #[tokio::test]
    async fn unknown_merge_engine_is_an_error() -> Result<()> {
        use hudi_test::SampleTable;
        let base_url = SampleTable::V9TxnsSimpleMeta.url_to_mor_avro();
        let reader = FileGroupReader::new_with_options(
            base_url.as_ref(),
            [(HudiReadConfig::MergeEngine.as_ref(), "turbo")],
        )
        .await?;
        let err = reader.merge_engine().unwrap_err();
        assert!(
            err.to_string().contains("turbo"),
            "error should name the value, got: {err}"
        );
        Ok(())
    }
}
