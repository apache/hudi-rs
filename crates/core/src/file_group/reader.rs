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
use crate::config::internal::HudiInternalConfig;
use crate::config::read::{FileGroupReaderVersion, HudiReadConfig};
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
use crate::file_group::reader_v2::metadata_merger::resolve_custom_merger;
use crate::file_group::reader_v2::reader_context::CompletionGateInputs;
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
    /// The committed/inflight sets the log-block scan gates on.
    ///
    /// A log file is admitted to a slice on its own instant, but its blocks
    /// carry theirs — including a writer still inflight when a later one
    /// committed. Without these the scan merges such a block, because it sorts
    /// below the latest instant and passes every other gate. Only a caller
    /// holding the timeline can supply them; one reading from paths alone (the
    /// cxx bridge) leaves the gate off, as it always was.
    completion_gate_inputs: Option<Arc<CompletionGateInputs>>,
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
            completion_gate_inputs: None,
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
            completion_gate_inputs: None,
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

    /// Whether this reader can gate the log scan on instant state.
    #[cfg(test)]
    pub(crate) fn has_completion_gate_inputs(&self) -> bool {
        self.completion_gate_inputs.is_some()
    }

    /// Gate the log-block scan on these committed/inflight sets.
    pub(crate) fn set_completion_gate_inputs(&mut self, inputs: CompletionGateInputs) {
        self.completion_gate_inputs = Some(Arc::new(inputs));
    }

    /// Read slices with `schema` rather than whatever the base file carries.
    pub(crate) fn set_data_schema(&mut self, schema: arrow_schema::SchemaRef) {
        self.data_schema_override = Some(schema);
    }

    /// The schema the merge-on-read reader needs up front.
    ///
    /// The table's own, whenever the caller could supply one — which is every
    /// caller that holds a timeline, including [`Table`](crate::table::Table),
    /// the DataFusion scan and the Python bindings. A base file's own schema is
    /// stale the moment a later writer widens a column or adds one, and reading
    /// with it forces the newer records back into the older shape: an i64 read as
    /// i32, an f64 as f32, an added column dropped. Wrong values, not an error.
    ///
    /// Only a caller reading from paths with no timeline at all — the cxx bridge —
    /// falls back to the base file. Reading its footer costs one request; the
    /// engine reads it again when it opens the file, and collapsing the two
    /// into one request is not yet done.
    async fn resolved_data_schema(&self, base_file_path: &str) -> Result<arrow_schema::SchemaRef> {
        if let Some(schema) = &self.data_schema_override {
            return Ok(schema.clone());
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
        Ok(stream.schema().clone())
    }

    /// The schema a streamed base-file batch must be reconciled to, or `None`
    /// when there is nothing to reconcile.
    ///
    /// `None` for a caller that supplied no table schema (the cxx bridge), and
    /// for a base file whose columns already match it — the overwhelmingly common
    /// case, where reconciliation would be a per-batch no-op.
    ///
    /// With a projection the result keeps the projection's columns in the
    /// projection's order, so a column the base file lacks is null-filled rather
    /// than dropped. Without one it is the table's schema entire.
    fn stream_target_schema(
        &self,
        base_file_schema: Option<&arrow_schema::SchemaRef>,
        read_projection: &Option<Vec<String>>,
    ) -> Option<arrow_schema::Schema> {
        let table_schema = self.data_schema_override.as_ref()?;
        let base_file_schema = base_file_schema?;

        let target = match read_projection {
            None => arrow_schema::Schema::new(table_schema.fields().clone()),
            Some(cols) => {
                let fields: Vec<arrow_schema::FieldRef> = cols
                    .iter()
                    .filter_map(|name| {
                        table_schema
                            .field_with_name(name)
                            .ok()
                            .or_else(|| base_file_schema.field_with_name(name).ok())
                            .map(|field| Arc::new(field.clone()))
                    })
                    .collect();
                arrow_schema::Schema::new(fields)
            }
        };

        // Nothing to do when the base file already presents these columns
        // exactly as the table declares them — skip the per-batch pass entirely.
        //
        // Nullability counts, not just the type: Hudi's meta fields are
        // non-nullable in the table schema and nullable in the file, and the
        // eager path returns the table's view of them. Comparing types alone
        // would leave the streamed batches labelled the file's way, so the two
        // paths would disagree — on a field a `RecordBatchReader` consumer is
        // entitled to trust, since it is what the scan declared up front. Metadata is
        // deliberately not compared: parquet files routinely carry writer
        // metadata the table schema does not, and reconciling over that would
        // cost a pass on every read while changing no value.
        let unchanged = target.fields().iter().all(|field| {
            base_file_schema
                .field_with_name(field.name())
                .is_ok_and(|base| {
                    base.data_type() == field.data_type()
                        && base.is_nullable() == field.is_nullable()
                })
        });
        if unchanged { None } else { Some(target) }
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
            Some(&data_schema),
        ) {
            return Err(reason);
        }

        // The partition a slice lives in is the directory its base file sits
        // in; a non-partitioned table yields an empty path, which is correct.
        let partition_path = std::path::Path::new(base_file_path)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();

        // Per file slice, so debug: a wide read would repeat this once per
        // slice, and which reader served the table is not news N times over.
        log::debug!("reading '{base_file_path}' with file group reader version 2");
        let merged = crate::file_group::reader_v2::adapter::read_file_slice(
            self.hudi_configs.clone(),
            self.storage.clone(),
            base_file_path,
            log_file_paths,
            partition_path,
            Some(data_schema),
            self.completion_gate_inputs.clone(),
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

    /// Which merge implementation serves this read.
    ///
    /// A metadata table is always served by version 1 whatever the
    /// setting says: its base files and log blocks are HFile, which the
    /// file group reader version 2 has no support for. That is permanent, not
    /// transitional.
    ///
    /// The value is read raw rather than through `get_or_default`, which falls
    /// back to the default when a value fails to parse. A typo in the version
    /// would then silently read with the other version, leaving a caller
    /// convinced they had exercised it — the one outcome this switch must not
    /// produce.
    fn file_group_reader_version(&self) -> Result<FileGroupReaderVersion> {
        if self.is_metadata_table() {
            return Ok(FileGroupReaderVersion::One);
        }
        // `try_get` rather than `get_or_default`: the latter returns the default
        // when a value fails to parse, so a typo would silently read with the
        // other version and leave a caller convinced they had exercised the one
        // they asked for. Borrowing, so no copy of the config map per read.
        match self
            .hudi_configs
            .try_get(HudiReadConfig::FileGroupReaderVersion)?
        {
            Some(value) => {
                FileGroupReaderVersion::try_from(usize::from(value)).map_err(CoreError::Config)
            }
            None => Ok(FileGroupReaderVersion::default()),
        }
    }

    /// Why file group reader version 2 cannot serve this read, if it cannot.
    ///
    /// This is a capability check, decided from config and the slice's own path
    /// before any I/O — never a catch-all on error. Both inputs are known
    /// without touching storage, which is what lets the answer be settled before
    /// a read starts. A read that fails *inside* version 2 propagates:
    /// retrying it on version 1 would make a bug look like a success,
    /// make results depend on which reader happened to win, and leave the
    /// differential tests unable to see anything.
    ///
    /// Every reason here but one means version 1 serves the read instead, so
    /// selecting a version cannot turn a working read into a failing one. Each
    /// reason is logged, because a fallback nobody can observe is
    /// indistinguishable from a reader that is never used.
    ///
    /// The exception is a `CUSTOM` record merge mode with a merge to perform,
    /// which errors. That one *does* refuse a read version 1 would serve:
    /// version 2 is the default, so a merge-on-read table declaring `CUSTOM`
    /// errors instead of reading. It is deliberate — falling back would
    /// merge with version 1's own derivation, which drops deletes, and wrong
    /// rows are worse than a refusal — and the
    /// error names the way back.
    ///
    /// Refusals are the exception, not the fallthrough: version 2 is the reader
    /// the gold fixtures compare against Hudi's own output, so a read it has no
    /// stated reason to refuse is one it serves.
    fn version_two_unsupported_reason(
        &self,
        base_file_path: &str,
        base_file_only: bool,
    ) -> Result<Option<&'static str>> {
        // Deliberately an error rather than a fallback: falling back would use
        // version 1's own merge derivation, which drops deletes on a
        // commit-time-ordered table. Wrong rows are worse than a refusal.
        //
        // Only when a merge actually happens, though. A slice with nothing to
        // merge — copy-on-write, or read-optimized — returns base rows without
        // consulting a merger at all, so refusing it would break reads that work
        // today over a mode they never reach.
        if !base_file_only
            // Read by raw key: this crate has no typed config for it yet, and
            // adding one belongs with the reader that acts on it. Borrowed, so
            // no copy of the option map per read.
            && let Some(mode) = self.hudi_configs.get_raw("hoodie.record.merge.mode")
            && mode.eq_ignore_ascii_case("CUSTOM")
            // Unless the table's payload class names a merger version 2
            // implements. The same resolution decides this gate and the four
            // inside the reader, so they admit or refuse together.
            && resolve_custom_merger(&self.hudi_configs.as_options()).is_none()
        {
            return Err(CoreError::Unsupported(
                "A table with a CUSTOM record merge mode needs its own merger, \
                 which no reader here implements for this table's payload class. \
                 Set hoodie.read.file.group.reader.version=1 to read it with the \
                 reader that served it before, which merges without that merger"
                    .to_string(),
            ));
        }

        // Unreachable while `file_group_reader_version` routes a metadata table
        // to version 1 above; kept so a future change to that routing fails
        // loudly here rather than reaching a reader that cannot read HFile.
        if self.is_metadata_table() {
            return Err(CoreError::Unsupported(
                "File group reader version 2 cannot read a metadata table's HFile \
                 base files and log blocks"
                    .to_string(),
            ));
        }

        // Version 2 resolves the base file format from config alone, so it
        // reads a file whose format is only knowable from its extension as
        // parquet and fails on the footer. Version 1 resolves per path and
        // reads it, so this falls back rather than refusing. Decided from the
        // path, which is a string — still no I/O.
        if BaseFileFormatValue::resolve_from_configs(&self.hudi_configs, Some(base_file_path))?
            != BaseFileFormatValue::Parquet
        {
            return Ok(Some(
                "file group reader version 2 reads parquet base files only",
            ));
        }

        // A table that drops its partition columns from the data files leaves
        // them knowable only from the partition path, and neither reader
        // reconstructs them. Version 1 fails the projection, which is the
        // honest answer; version 2 treats the column as one a later writer
        // added and null-fills it, which is a wrong value rather than a
        // refusal. Falling back keeps the refusal. (`Table` rejects this
        // configuration outright unless config validation is skipped, so this
        // guards the skipped case.)
        if self
            .hudi_configs
            .get_or_default(HudiTableConfig::DropsPartitionFields)
            .into()
        {
            return Ok(Some(
                "file group reader version 2 cannot reconstruct partition columns \
                 dropped from the data files",
            ));
        }

        Ok(None)
    }

    /// Whether version 2 serves this read, logging the reason when it does not.
    ///
    /// The one place the version and the capability check are combined, so the
    /// eager and streaming paths cannot come to different answers about which
    /// reader serves a slice.
    fn serve_with_version_two(&self, base_file_path: &str, base_file_only: bool) -> Result<bool> {
        if self.file_group_reader_version()? != FileGroupReaderVersion::Two {
            return Ok(false);
        }
        match self.version_two_unsupported_reason(base_file_path, base_file_only)? {
            None => Ok(true),
            Some(reason) => {
                log::debug!(
                    "reading '{base_file_path}' with file group reader version 1: {reason}"
                );
                Ok(false)
            }
        }
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

        let merged = if self.serve_with_version_two(base_file_path, base_file_only)? {
            // Read-optimized means the log files are not read at all, so hand the
            // engine none rather than letting it merge them. A slice with no log
            // files reduces to a base file read either way.
            let log_file_paths = if base_file_only {
                Vec::new()
            } else {
                log_file_paths
            };
            self.read_via_v2(base_file_path, log_file_paths).await?
        } else if base_file_only {
            // Nothing to merge — a copy-on-write slice, or a read-optimized read
            // that ignores the log files. Served by the base file reader, not by
            // either file group reader, so the version above does not reach it.
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
            let merged = merger.merge_record_batches(all_batches)?;

            // Narrow to the read's window again, now that the merge has decided
            // which record won each key. `read_base_file_eager` above applied it
            // to the base file only, so a log record outside the window survived
            // into the result — the log batches are admitted by instant range at
            // scan time, which is a per-block decision, not a per-row one.
            //
            // Idempotent for the base rows, which already passed it, and it is
            // where version 2 applies the same filter.
            apply_commit_time_filter(&self.hudi_configs, merged)?
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
    /// For MOR tables with log files, file group reader version 2 streams the merge
    /// too: the base file is decoded one row group at a time. Version 1 sorts and
    /// dedups whole batches at once and has no incremental form, so it still
    /// collects the merge and yields it as a single batch.
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
            return self
                .read_base_file_stream(base_file_path, &options, known_base_file_size)
                .await;
        }

        // Both early returns above are base-file-only reads served by neither
        // file group reader, so anything reaching here has log files to merge.
        if self.serve_with_version_two(base_file_path, false)? {
            return self
                .stream_via_v2(base_file_path, log_file_paths, &options)
                .await;
        }

        // Version 1's merge sorts and dedups whole batches at once, so it has
        // no incremental form to stream: collect it and yield the one batch.
        let batch = self
            .read_file_slice_from_paths(base_file_path, log_file_paths, &options)
            .await?;
        Ok(Box::pin(futures::stream::once(async { Ok(batch) })))
    }

    /// Stream one merged slice through the merge-on-read reader.
    ///
    /// The eager sibling of this — [`Self::read_via_v2`] — holds the whole merged
    /// slice in memory, so `batch_size` means nothing to it and a `LIMIT` still
    /// pays for the entire merge. This decodes the base file one row group at a
    /// time and applies the same post-merge steps per batch, so the two agree row
    /// for row while peak memory tracks a row group.
    async fn stream_via_v2(
        &self,
        base_file_path: &str,
        log_file_paths: Vec<String>,
        options: &ReadOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let data_schema = self.resolved_data_schema(base_file_path).await?;
        if let Some(reason) = crate::file_group::reader_v2::adapter::refuse_reason(
            self.is_metadata_table(),
            Some(&data_schema),
        ) {
            return Err(reason);
        }

        let partition_path = std::path::Path::new(base_file_path)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();

        // Per file slice — see the eager sibling.
        log::debug!("streaming '{base_file_path}' with file group reader version 2");
        let merged = crate::file_group::reader_v2::adapter::read_file_slice_stream(
            self.hudi_configs.clone(),
            self.storage.clone(),
            base_file_path,
            log_file_paths,
            partition_path,
            Some(data_schema),
            self.completion_gate_inputs.clone(),
        )
        .await?;

        // Per batch, in the same order the eager path applies them to the whole
        // slice: narrow an incremental read back to its window, then the caller's
        // filters and projection. Splitting the merge into batches must not
        // change what any of the three mean.
        let hudi_configs = self.hudi_configs.clone();
        let options = options.clone();
        let stream = merged.map(move |batch| {
            let batch = apply_commit_time_filter(&hudi_configs, batch?)?;
            apply_eager_options(&options, batch)
        });
        Ok(Box::pin(stream))
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
        // The base file's own columns, needed only when this read has to be
        // reconciled to the table's schema — see `stream_target_schema`. Reading
        // the footer costs one request; the eager merge-on-read path already pays
        // the same one for the same reason.
        let base_file_schema = match (&self.data_schema_override, relative_path.is_empty()) {
            (Some(_), false) => Some(
                self.reader_for_path(relative_path)?
                    .read_stream(relative_path, BaseFileReadOptions::default())
                    .await
                    .map_err(|e| {
                        ReadFileSliceError(format!(
                            "Failed to read base file schema '{relative_path}': {e:?}"
                        ))
                    })?
                    .schema()
                    .clone(),
            ),
            _ => None,
        };

        if let Some(ref cols) = read_projection {
            // A column the table has but this base file does not cannot be
            // selected from it — the parquet reader rejects an unknown column
            // outright. It is null-filled by the reconciliation below instead.
            let selectable: Vec<String> = match &base_file_schema {
                Some(schema) => cols
                    .iter()
                    .filter(|name| schema.index_of(name).is_ok())
                    .cloned()
                    .collect(),
                None => cols.clone(),
            };
            read_options = read_options.with_projection(selectable);
        }

        // What each batch must look like once read: the table's types for the
        // columns this read selected. Without this the streaming path returns the
        // base file's own schema while the eager path returns the table's, so the
        // two disagree on an evolved table — `Table::read` gives i64 where
        // `Table::read_stream` gives i32, and a caller that declared the table's
        // schema up front (the DataFusion scan does) is handed batches that do
        // not match it.
        let target_schema = self
            .stream_target_schema(base_file_schema.as_ref(), &read_projection)
            .map(Arc::new);

        let hudi_configs = self.hudi_configs.clone();
        let path = relative_path.to_string();
        let filters = Arc::new(options.filters.clone());
        let final_projection = Arc::new(final_projection);
        let target_schema = Arc::new(target_schema);
        // Validate once on first batch so typoed filter columns surface as errors
        // rather than silent no-ops in `filters_to_row_mask`.
        let validated = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let reader = self.reader_for_path(&path)?;
        let base_stream = reader
            .read_stream(&path, read_options)
            .map_err(|e| ReadFileSliceError(format!("Failed to read path {path}: {e:?}")))
            .await?;

        // Apply: reconcile to the table's schema → commit time → structured
        // filters → final projection. Reconciliation comes first so a filter and
        // the commit-time mask compare against the same types the eager path
        // gives them.
        let stream = base_stream.into_stream().filter_map(move |result| {
            let hudi_configs = hudi_configs.clone();
            let filters = filters.clone();
            let final_projection = final_projection.clone();
            let validated = validated.clone();
            let target_schema = target_schema.clone();
            async move {
                match result {
                    Err(e) => Some(Err(ReadFileSliceError(format!(
                        "Failed to read batch: {e:?}"
                    )))),
                    Ok(batch) => {
                        let batch = match target_schema.as_ref() {
                            Some(target) => {
                                match crate::schema::batch_evolution::project_batch_to_schema(
                                    &batch, target,
                                ) {
                                    Err(e) => return Some(Err(e)),
                                    Ok(b) => b,
                                }
                            }
                            None => batch,
                        };
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
/// A mask keeping rows whose `_hoodie_commit_time` is one of `instant_times`
/// (comma-separated).
///
/// An empty list admits nothing, which is correct: it means the window resolved
/// to no commits, so the read has no changes to report.
fn commit_time_membership_mask(instant_times: &str, batch: &RecordBatch) -> Result<BooleanArray> {
    let admitted: std::collections::HashSet<&str> = instant_times
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();

    let col_name = MetaField::CommitTime.as_ref();
    let column = batch
        .column_by_name(col_name)
        .ok_or_else(|| ReadFileSliceError(format!("Column {col_name} not found")))?;
    let commit_times = column
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or_else(|| ReadFileSliceError(format!("Column {col_name} is not a string column")))?;

    use arrow_array::Array;
    Ok((0..batch.num_rows())
        .map(|i| !commit_times.is_null(i) && admitted.contains(commit_times.value(i)))
        .collect::<BooleanArray>())
}

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

    // An incremental read that resolved its window against completion times hands
    // the admitted instant times down explicitly, because `_hoodie_commit_time`
    // holds the REQUESTED time and comparing that against completion-time bounds
    // asks a different question. Match by membership instead.
    if let Some(instant_times) =
        hudi_configs.try_get(HudiInternalConfig::IncrementalInstantTimes)?
    {
        let instant_times: String = instant_times.into();
        return commit_time_membership_mask(&instant_times, batch).map(Some);
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

    /// A missing base file is an error under either reader version, and the
    /// error names the file.
    ///
    /// The two versions reach it by different routes — version 1 fails opening
    /// the file, version 2 fails resolving its schema first — so the wording
    /// differs and only what a caller can act on is asserted: which file, and
    /// that it was not there.
    #[tokio::test]
    async fn test_read_file_slice_from_paths_missing_base_file_is_an_error() -> Result<()> {
        let base_uri = get_base_uri_with_valid_props_minimum();
        let base_file_path = "non_existent_file.parquet";

        for reader_version in ["1", "2"] {
            let reader = FileGroupReader::new_with_options(
                &base_uri,
                [(
                    HudiReadConfig::FileGroupReaderVersion.as_ref(),
                    reader_version,
                )],
            )
            .await?;

            let error_msg = reader
                .read_file_slice_from_paths(base_file_path, Vec::<&str>::new(), &ReadOptions::new())
                .await
                .expect_err("a missing base file must be an error")
                .to_string();

            assert!(
                error_msg.contains(base_file_path),
                "reader version {reader_version}: the error must name the file, got: {error_msg}"
            );
            // Spaces removed so `NotFound` and `not found` both match: the two
            // versions surface the same object-store condition, one through a
            // typed variant name and one through prose.
            let squashed = error_msg.to_lowercase().replace(' ', "");
            assert!(
                squashed.contains("notfound"),
                "reader version {reader_version}: the error must say the file was not \
                 found, got: {error_msg}"
            );
        }

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

    pub(super) fn get_metadata_table_base_uri() -> String {
        use hudi_test::QuickstartTripsTable;
        use std::fs::canonicalize;
        use std::path::PathBuf;
        use url::Url;
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

    /// Version 2 must not change what a base-file-only read returns.
    ///
    /// Both versions serve this read — version 2 reduces to a base file read —
    /// so this compares their output directly. With no log files there is no
    /// merge for them to disagree about, which makes any difference here a
    /// difference in the base file path itself.
    #[tokio::test]
    async fn version_two_does_not_change_a_base_file_only_read() -> Result<()> {
        let (base_uri, base_file_name) = v8np_base_uri_and_first_parquet();

        let version_one = FileGroupReader::new_with_options(
            &base_uri,
            [(HudiReadConfig::FileGroupReaderVersion.as_ref(), "1")],
        )
        .await?
        .read_file_slice_from_paths(&base_file_name, Vec::<&str>::new(), &ReadOptions::new())
        .await?;

        let version_two_reader = FileGroupReader::new_with_options(
            &base_uri,
            [(HudiReadConfig::FileGroupReaderVersion.as_ref(), "2")],
        )
        .await?;
        assert_eq!(
            version_two_reader.file_group_reader_version()?,
            FileGroupReaderVersion::Two
        );
        let version_two = version_two_reader
            .read_file_slice_from_paths(&base_file_name, Vec::<&str>::new(), &ReadOptions::new())
            .await?;

        assert_eq!(version_two.num_rows(), version_one.num_rows());
        assert_eq!(version_two.schema(), version_one.schema());
        assert_eq!(version_two, version_one);
        Ok(())
    }

    /// Read-optimized mode means the log files are not read, and version 2 must
    /// not resurrect them.
    #[tokio::test]
    async fn version_two_does_not_defeat_read_optimized_mode() -> Result<()> {
        let (base_uri, base_file_name) = v8np_base_uri_and_first_parquet();
        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [
                (HudiReadConfig::FileGroupReaderVersion.as_ref(), "2"),
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

    /// A slice with log files, read with version 2, actually reads.
    ///
    /// Selecting a version and reaching it are different things: a test that
    /// only asserts the selection is satisfied by a dispatch wired to nothing.
    /// This one goes through the dispatch to version 2 and back out through
    /// `apply_eager_options`.
    #[tokio::test]
    async fn version_two_reads_a_slice_with_log_files_through_the_dispatch() -> Result<()> {
        let (base_uri, partition, base_file_name, log_file_name) = v8_trips_mor_first_slice();
        let base_path = format!("{partition}/{base_file_name}");
        let log_path = format!("{partition}/{log_file_name}");

        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [(HudiReadConfig::FileGroupReaderVersion.as_ref(), "2")],
        )
        .await?;
        let batch = reader
            .read_file_slice_from_paths(&base_path, vec![log_path.clone()], &ReadOptions::new())
            .await?;

        assert!(batch.num_rows() > 0, "expected a merged result");

        let version_one = FileGroupReader::new_with_options(
            &base_uri,
            [(HudiReadConfig::FileGroupReaderVersion.as_ref(), "1")],
        )
        .await?
        .read_file_slice_from_paths(&base_path, vec![log_path.clone()], &ReadOptions::new())
        .await?;
        assert_eq!(
            batch.num_rows(),
            version_one.num_rows(),
            "row count differs"
        );
        Ok(())
    }

    /// Regression test: no row an incremental read returns may sit outside its
    /// window, including a compacted base row that no log record replaces.
    ///
    /// Compaction merges records from many commits into one base file while
    /// keeping each record's own commit time, so a window admitting the *file*
    /// still has to exclude most of its rows. Version 2 decides per file and
    /// would return them all without the post-merge commit-time filter.
    ///
    /// The fixture is chosen so removing that filter is visible: the compacted
    /// base holds `c` and `d` at the same pre-window commit, and only `c` is
    /// updated by an in-window log record. `d` therefore survives the merge
    /// unmatched, carrying its out-of-window time — a slice whose every stale
    /// row is replaced by the merge would pass even with the filter deleted.
    #[tokio::test]
    async fn test_read_file_slice_from_paths_incremental_excludes_out_of_window_rows() -> Result<()>
    {
        use arrow_array::Array;
        use hudi_test::QuickstartTripsTable;
        let table_path = QuickstartTripsTable::V9MorCompactedIncremental.path_to_mor_avro();
        let base_uri = Url::from_directory_path(&table_path).unwrap().to_string();

        // Compaction wrote this base file, merging `a`@…526409, `b`@…528666 and
        // `c`/`d`@…522627; the log file below updates `c` after it.
        let base_path = "71682de1-30cf-46a9-ae33-f473d7b0960c-0_0-17-24_20260807223529164.parquet";
        let log_path = ".71682de1-30cf-46a9-ae33-f473d7b0960c-0_20260807223530452.log.1_0-22-29";

        // Opens after `c`/`d`'s commit and closes after the log's, so `a`, `b`
        // and the updated `c` are in range while `d` is not.
        let start = "20260807223525000";
        let end = "20260807223531000";

        let reader = FileGroupReader::new_with_options(
            &base_uri,
            [
                (HudiReadConfig::FileGroupReaderVersion.as_ref(), "2"),
                (HudiReadConfig::StartTimestamp.as_ref(), start),
                (HudiReadConfig::EndTimestamp.as_ref(), end),
            ],
        )
        .await?;
        let batch = reader
            .read_file_slice_from_paths(base_path, vec![log_path], &ReadOptions::new())
            .await?;

        let uuids = batch
            .column_by_name("uuid")
            .expect("uuid column")
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("uuid is a string");
        let mut got: Vec<&str> = (0..uuids.len()).map(|i| uuids.value(i)).collect();
        got.sort_unstable();
        assert_eq!(
            got,
            vec!["a", "b", "c"],
            "`d` is a compacted base row from before the window and no log \
             record replaces it, so only the filter can exclude it"
        );

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
}

#[cfg(test)]
mod file_group_reader_version_tests {
    use super::*;
    use hudi_test::SampleTable;

    async fn reader_with(
        options: impl IntoIterator<Item = (&'static str, String)>,
    ) -> Result<FileGroupReader> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_mor_parquet();
        FileGroupReader::new_with_options(base_url.as_ref(), options).await
    }

    /// File group reader version 2 is what a caller who sets nothing gets.
    /// Asserted directly because no other test says which version produced its
    /// result, so a default that flipped by accident would go unnoticed.
    #[tokio::test]
    async fn test_file_group_reader_version_unset_returns_two() -> Result<()> {
        let reader = reader_with(Vec::<(&'static str, String)>::new()).await?;
        assert_eq!(
            reader.file_group_reader_version()?,
            FileGroupReaderVersion::Two
        );
        Ok(())
    }

    /// Version 1 remains reachable, so a caller can opt out of version 2
    /// entirely rather than relying on it to keep falling back.
    #[tokio::test]
    async fn test_file_group_reader_version_one_returns_one() -> Result<()> {
        let reader = reader_with([(
            HudiReadConfig::FileGroupReaderVersion.as_ref(),
            "1".to_string(),
        )])
        .await?;
        assert_eq!(
            reader.file_group_reader_version()?,
            FileGroupReaderVersion::One
        );
        Ok(())
    }

    /// The guard inside the check, reached only if the dispatch's metadata
    /// routing were ever removed. Asserted directly because the dispatch answers
    /// metadata tables before the check runs, so no read can reach it today —
    /// which is exactly why it must keep erroring rather than fall through to a
    /// reader that cannot read HFile.
    #[tokio::test]
    async fn test_version_two_unsupported_reason_metadata_table_returns_error() -> Result<()> {
        use crate::config::HudiConfigs;
        use crate::config::table::HudiTableConfig;
        use std::collections::HashMap;
        use std::sync::Arc;

        let configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath.as_ref(),
            super::tests::get_metadata_table_base_uri(),
        )]));
        let reader = FileGroupReader::new_with_overrides(configs, HashMap::new(), HashMap::new())?;
        let err = reader
            .version_two_unsupported_reason("f.parquet", false)
            .unwrap_err();
        assert!(
            matches!(err, CoreError::Unsupported(ref m) if m.contains("metadata table")),
            "expected an unsupported error naming the metadata table, got {err:?}"
        );
        Ok(())
    }

    /// A typo must not read with the other version. `get_or_default` would have
    /// swallowed this and left the caller believing they had exercised version 2.
    #[tokio::test]
    async fn test_file_group_reader_version_unrecognised_returns_config_error() -> Result<()> {
        let reader = reader_with([(
            HudiReadConfig::FileGroupReaderVersion.as_ref(),
            "9".to_string(),
        )])
        .await?;
        let err = reader.file_group_reader_version().unwrap_err();
        assert!(
            matches!(err, CoreError::Config(_)),
            "expected a config error, got {err:?}"
        );
        assert!(
            err.to_string().contains("9"),
            "the error must name the value"
        );
        Ok(())
    }

    /// A merging read on an ordinary table has no reason to be refused, so
    /// version 2 serves it. The refusals below are the exceptions; this asserts
    /// they have not quietly become the rule.
    #[tokio::test]
    async fn test_version_two_unsupported_reason_ordinary_merging_read_returns_none() -> Result<()>
    {
        let reader = reader_with([(
            HudiReadConfig::FileGroupReaderVersion.as_ref(),
            "2".to_string(),
        )])
        .await?;
        assert_eq!(
            reader.file_group_reader_version()?,
            FileGroupReaderVersion::Two
        );

        assert_eq!(
            reader.version_two_unsupported_reason("f.parquet", false)?,
            None,
            "an ordinary merging read must be served by version 2"
        );
        Ok(())
    }

    /// The default returns what version 1 returns, cell for cell.
    ///
    /// Version 2 serves this read, so the two are different code paths and the
    /// comparison is a real differential one.
    #[tokio::test]
    async fn test_read_file_slice_from_paths_default_version_matches_version_one() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_mor_parquet();
        let table = crate::table::Table::new(base_url.path()).await?;
        let slices = table.get_file_slices(&ReadOptions::new()).await?;
        assert!(!slices.is_empty(), "fixture must have a file slice to read");

        let read_with = async |version: Option<&str>| -> Result<Vec<String>> {
            let options: Vec<(&str, String)> = match version {
                Some(e) => vec![(
                    HudiReadConfig::FileGroupReaderVersion.as_ref(),
                    e.to_string(),
                )],
                None => Vec::new(),
            };
            let reader = FileGroupReader::new_with_options(base_url.as_ref(), options).await?;
            // Rendered cell by cell rather than counted: a row count would
            // match even if the columns or the values differed.
            let mut rendered: Vec<String> = Vec::new();
            for slice in &slices {
                let batch = reader.read_file_slice(slice, &ReadOptions::new()).await?;
                let names: Vec<String> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                rendered.push(names.join(","));
                for row in 0..batch.num_rows() {
                    let cells: Vec<String> = batch
                        .columns()
                        .iter()
                        .map(|col| {
                            arrow_cast::display::array_value_to_string(col.as_ref(), row)
                                .unwrap_or_else(|_| "<unrenderable>".to_string())
                        })
                        .collect();
                    rendered.push(cells.join(" | "));
                }
            }
            rendered.sort();
            Ok(rendered)
        };

        assert_eq!(
            read_with(None).await?,
            read_with(Some("1")).await?,
            "the default must return what an explicit version 1 read returns"
        );
        Ok(())
    }

    /// A table declaring a CUSTOM record merge mode still reads when there is
    /// nothing to merge.
    ///
    /// The refusal exists because merging without the table's own merger drops
    /// deletes on a commit-time-ordered table. But a copy-on-write slice and a
    /// read-optimized read never consult a merger, so refusing them would break
    /// reads that work today over a mode they never reach — and version 2 being
    /// the default means nobody opted in to that.
    #[tokio::test]
    async fn test_version_two_unsupported_reason_custom_merge_mode_without_merge_returns_none()
    -> Result<()> {
        let reader = reader_with([("hoodie.record.merge.mode", "CUSTOM".to_string())]).await?;

        assert_eq!(
            reader.version_two_unsupported_reason("f.parquet", true)?,
            None,
            "a read with nothing to merge must not be refused for a merge mode"
        );
        Ok(())
    }

    /// The same table is refused once a merge is actually involved.
    #[tokio::test]
    async fn test_version_two_unsupported_reason_custom_merge_mode_with_merge_returns_error()
    -> Result<()> {
        let reader = reader_with([("hoodie.record.merge.mode", "CUSTOM".to_string())]).await?;

        let err = reader
            .version_two_unsupported_reason("f.parquet", false)
            .unwrap_err();
        assert!(
            matches!(err, CoreError::Unsupported(_)),
            "expected a refusal, got {err:?}"
        );
        assert!(
            err.to_string().contains("CUSTOM"),
            "the error must name why"
        );
        Ok(())
    }

    /// The CUSTOM refusal is narrow: it no longer fires for a table whose payload
    /// class names a merger version 2 implements.
    ///
    /// Read on the real metadata table, which is the only such table there is: it
    /// states `hoodie.record.merge.mode=CUSTOM` and names `HoodieMetadataPayload`.
    /// The read is still refused, by the *metadata table* gate one check later, and
    /// that is what this asserts — the reason changed, which is exactly the scope
    /// of this change. Lifting the metadata-table routing is separate work, so
    /// until then the merger stays unreachable through this entry point.
    ///
    /// Asserting on which refusal fires, rather than that none does, keeps the
    /// test honest about that: a test demanding `is_ok()` here could only pass by
    /// also lifting a gate this change deliberately leaves alone.
    #[tokio::test]
    async fn test_the_custom_gate_no_longer_refuses_the_metadata_payload() -> Result<()> {
        let url = super::tests::get_metadata_table_base_uri();
        let reader =
            FileGroupReader::new_with_options(&url, crate::config::util::empty_options()).await?;

        assert_eq!(
            reader.hudi_configs.get_raw("hoodie.record.merge.mode"),
            Some("CUSTOM"),
            "the fixture must really be a CUSTOM table, or this test is vacuous"
        );

        let err = reader
            .version_two_unsupported_reason("f.hfile", false)
            .expect_err("the metadata table is still refused, by a later gate");
        let message = err.to_string();
        assert!(
            !message.contains("CUSTOM record merge mode"),
            "the CUSTOM gate must no longer be the one refusing this table, got: {message}"
        );
        assert!(
            message.contains("metadata table"),
            "the refusal must come from the metadata-table gate, got: {message}"
        );
        Ok(())
    }

    /// Regression test: the gate's promise held at the gate but not behind it: the
    /// engine refused the merge mode it was never going to consult, so a
    /// base-file-only read of a CUSTOM table failed end to end while the
    /// streaming path served it. Read the whole table both ways to pin the
    /// promise where it broke.
    #[tokio::test]
    async fn test_custom_merge_mode_reads_base_only_slices_end_to_end() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let table = crate::table::Table::new(base_url.path()).await?;
        let slices = table.get_file_slices(&ReadOptions::new()).await?;
        assert!(!slices.is_empty(), "fixture must have a file slice to read");

        let read_all = async |options: Vec<(&str, String)>| -> Result<usize> {
            let reader = FileGroupReader::new_with_options(base_url.as_ref(), options).await?;
            let mut rows = 0;
            for slice in &slices {
                rows += reader
                    .read_file_slice(slice, &ReadOptions::new())
                    .await?
                    .num_rows();
            }
            Ok(rows)
        };

        let custom = ("hoodie.record.merge.mode", "CUSTOM".to_string());
        let default_rows = read_all(vec![custom.clone()]).await?;
        let v1_rows = read_all(vec![
            custom,
            (
                HudiReadConfig::FileGroupReaderVersion.as_ref(),
                "1".to_string(),
            ),
        ])
        .await?;
        assert!(default_rows > 0, "the fixture has rows");
        assert_eq!(
            default_rows, v1_rows,
            "both versions must read the same rows"
        );
        Ok(())
    }

    /// A read-optimized read ignores the log files, so it follows the same
    /// rule as a copy-on-write slice: served, whatever the merge mode says.
    #[tokio::test]
    async fn test_custom_merge_mode_reads_read_optimized_end_to_end() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_mor_parquet();
        let table = crate::table::Table::new(base_url.path()).await?;
        let slices = table.get_file_slices(&ReadOptions::new()).await?;
        assert!(!slices.is_empty(), "fixture must have a file slice to read");

        let reader = FileGroupReader::new_with_options(
            base_url.as_ref(),
            [("hoodie.record.merge.mode", "CUSTOM".to_string())],
        )
        .await?;
        let options = ReadOptions::new().with_hudi_option(
            HudiReadConfig::UseReadOptimizedMode.as_ref(),
            "true".to_string(),
        );
        let mut rows = 0;
        for slice in &slices {
            rows += reader.read_file_slice(slice, &options).await?.num_rows();
        }
        assert!(rows > 0, "a read-optimized read must be served");
        Ok(())
    }

    /// The same table errors once a merge is real, and the error names the
    /// version that reads it.
    #[tokio::test]
    async fn test_custom_merge_mode_merging_read_errors_end_to_end() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_mor_parquet();
        let table = crate::table::Table::new(base_url.path()).await?;
        let slices = table.get_file_slices(&ReadOptions::new()).await?;
        let slice = slices
            .iter()
            .find(|s| s.has_log_file())
            .expect("fixture must have a slice with log files");

        let reader = FileGroupReader::new_with_options(
            base_url.as_ref(),
            [("hoodie.record.merge.mode", "CUSTOM".to_string())],
        )
        .await?;
        let err = reader
            .read_file_slice(slice, &ReadOptions::new())
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("hoodie.read.file.group.reader.version=1"),
            "the error must name the way back, got: {err}"
        );
        Ok(())
    }

    /// Regression test: a base file whose format is only knowable from its extension
    /// falls back to version 1 rather than being read as parquet.
    ///
    /// Version 2 resolves the format from `hoodie.table.base.file.format`
    /// alone, so a Lance table that never sets it — the extension-fallback case
    /// version 1 handles — reached the parquet reader and failed on the footer.
    /// Version 2 being the default made that every such read.
    #[tokio::test]
    async fn test_version_two_unsupported_reason_non_parquet_base_file_returns_reason() -> Result<()>
    {
        let reader = reader_with(Vec::<(&'static str, String)>::new()).await?;

        assert!(
            reader
                .version_two_unsupported_reason("part/f.lance", false)?
                .is_some_and(|reason| reason.contains("parquet")),
            "a non-parquet base file must fall back to version 1"
        );
        assert_eq!(
            reader.version_two_unsupported_reason("part/f.parquet", false)?,
            None,
            "a parquet base file must still be served by version 2"
        );
        Ok(())
    }

    /// Regression test: a table that drops its partition columns from the data files
    /// falls back to version 1 rather than null-filling them.
    ///
    /// Neither reader reconstructs a dropped partition column from the
    /// partition path. Version 1 fails a projection naming one; version 2 sees
    /// a column the base file lacks and null-fills it, turning a refusal into a
    /// wrong value. Version 2 being the default made that the answer for every
    /// such table.
    #[tokio::test]
    async fn test_version_two_unsupported_reason_dropped_partition_fields_returns_reason()
    -> Result<()> {
        use crate::config::HudiConfigs;
        use crate::config::table::HudiTableConfig;
        use std::collections::HashMap;
        use std::sync::Arc;

        // Built from configs rather than resolved from storage: the fixture's
        // own `hoodie.properties` says `false`, and it wins over an option
        // passed to the builder.
        let base_url = SampleTable::V6Nonpartitioned.url_to_mor_parquet();
        let configs = Arc::new(HudiConfigs::new([
            (HudiTableConfig::BasePath.as_ref(), base_url.to_string()),
            (
                HudiTableConfig::DropsPartitionFields.as_ref(),
                "true".to_string(),
            ),
        ]));
        let reader = FileGroupReader::new_with_overrides(configs, HashMap::new(), HashMap::new())?;

        assert!(
            reader
                .version_two_unsupported_reason("part/f.parquet", false)?
                .is_some_and(|reason| reason.contains("partition columns")),
            "a table dropping its partition columns must fall back to version 1"
        );
        Ok(())
    }

    /// A metadata table is served by version 1 whatever the setting
    /// says, so setting the version globally cannot make one unreadable — table
    /// listing itself reads one.
    #[tokio::test]
    async fn test_file_group_reader_version_metadata_table_returns_one() -> Result<()> {
        use crate::config::HudiConfigs;
        use crate::config::table::HudiTableConfig;
        use std::collections::HashMap;
        use std::sync::Arc;

        // Built from configs rather than resolved from storage: a metadata table
        // has no `hoodie.properties` of its own to load.
        let configs = Arc::new(HudiConfigs::new([
            (
                HudiTableConfig::BasePath.as_ref(),
                super::tests::get_metadata_table_base_uri(),
            ),
            (
                HudiReadConfig::FileGroupReaderVersion.as_ref(),
                "2".to_string(),
            ),
        ]));
        let reader = FileGroupReader::new_with_overrides(configs, HashMap::new(), HashMap::new())?;
        assert!(reader.is_metadata_table());
        assert_eq!(
            reader.file_group_reader_version()?,
            FileGroupReaderVersion::One
        );
        Ok(())
    }
}
