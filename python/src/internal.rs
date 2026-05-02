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

use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::collections::HashMap;
use std::convert::From;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

#[cfg(feature = "datafusion")]
use datafusion::error::DataFusionError;

use hudi::config::read::HudiReadConfig;
use hudi::config::table::HudiTableConfig;
use hudi::error::CoreError;
use hudi::error::Result as HudiResult;
use hudi::file_group::FileGroup;
use hudi::file_group::file_slice::FileSlice;
use hudi::file_group::reader::FileGroupReader;
use hudi::storage::error::StorageError;
use hudi::table::builder::TableBuilder;
use hudi::table::{QueryType, ReadOptions, Table};
use hudi::timeline::Timeline;
use hudi::timeline::instant::Instant;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::{PyErr, PyResult, Python, create_exception, pyclass, pyfunction, pymethods};
use std::error::Error;

type RecordBatchBoxStream = BoxStream<'static, HudiResult<RecordBatch>>;

create_exception!(_internal, HudiCoreError, PyException);

fn convert_to_py_err<I>(err: I) -> PyErr
where
    I: Error,
{
    // TODO(xushiyan): match and map all sub types
    HudiCoreError::new_err(err.to_string())
}

#[derive(thiserror::Error, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum PythonError {
    #[error("Error in Hudi core: {0}")]
    HudiCore(#[from] CoreError),
    #[cfg(feature = "datafusion")]
    #[error("Error in Datafusion core: {0}")]
    DataFusionCore(#[from] DataFusionError),
}

impl From<PythonError> for PyErr {
    fn from(err: PythonError) -> PyErr {
        match err {
            PythonError::HudiCore(err) => convert_to_py_err(err),
            #[cfg(feature = "datafusion")]
            PythonError::DataFusionCore(err) => convert_to_py_err(err),
        }
    }
}

/// Python wrapper around [`hudi::table::QueryType`].
#[cfg(not(tarpaulin_include))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[pyclass(eq)]
pub struct HudiQueryType {
    inner: QueryType,
}

#[cfg(not(tarpaulin_include))]
#[pymethods]
impl HudiQueryType {
    #[classattr]
    #[pyo3(name = "Snapshot")]
    fn snapshot() -> Self {
        QueryType::Snapshot.into()
    }

    #[classattr]
    #[pyo3(name = "Incremental")]
    fn incremental() -> Self {
        QueryType::Incremental.into()
    }

    fn __repr__(&self) -> String {
        format!("HudiQueryType.{}", self.name())
    }

    #[getter]
    fn name(&self) -> String {
        format!("{:?}", self.inner)
    }

    #[getter]
    fn value(&self) -> String {
        self.inner.as_ref().to_string()
    }
}

#[cfg(not(tarpaulin_include))]
#[derive(Clone, Debug, Default)]
#[pyclass]
pub struct HudiReadOptions {
    inner: ReadOptions,
}

impl From<QueryType> for HudiQueryType {
    fn from(inner: QueryType) -> Self {
        Self { inner }
    }
}

#[cfg(not(tarpaulin_include))]
#[pymethods]
impl HudiReadOptions {
    /// Construct read options. Mirrors the Rust `ReadOptions` struct shape:
    /// only the three stored fields are accepted directly. All other knobs
    /// (`query_type`, timestamps, `batch_size`) are set via the chainable
    /// `with_*` builders, matching the Rust API.
    ///
    /// `filters` are parsed and cardinality-validated here; an unrecognized
    /// operator or empty `IN`/`NOT IN` value list raises immediately.
    #[new]
    #[pyo3(signature = (filters=None, projection=None, hudi_options=None))]
    fn new(
        filters: Option<Vec<(String, String, String)>>,
        projection: Option<Vec<String>>,
        hudi_options: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let mut inner = ReadOptions::new()
            .with_filters(filters.unwrap_or_default())
            .map_err(PythonError::from)?
            .with_hudi_options(hudi_options.unwrap_or_default());
        if let Some(projection) = projection {
            inner = inner.with_projection(projection);
        }
        Ok(Self { inner })
    }

    fn __repr__(&self) -> String {
        format!(
            "HudiReadOptions(filters={:?}, projection={:?}, hudi_options={:?})",
            self.filters(),
            self.inner.projection,
            self.inner.hudi_options,
        )
    }

    #[getter]
    fn filters(&self) -> Vec<(String, String, String)> {
        self.inner
            .filters
            .iter()
            .cloned()
            .map(|f| f.into())
            .collect()
    }

    #[getter]
    fn projection(&self) -> Option<Vec<String>> {
        self.inner.projection.clone()
    }

    #[getter]
    fn hudi_options(&self) -> HashMap<String, String> {
        self.inner.hudi_options.clone()
    }

    // ---- typed builders (return a new instance for chaining) ----

    /// Sets the query type. Stored under `hoodie.read.query.type`.
    fn with_query_type(&self, query_type: &HudiQueryType) -> Self {
        Self {
            inner: self.inner.clone().with_query_type(query_type.inner),
        }
    }

    /// Sets the as-of timestamp for snapshot/time-travel queries.
    fn with_as_of_timestamp(&self, timestamp: &str) -> Self {
        Self {
            inner: self.inner.clone().with_as_of_timestamp(timestamp),
        }
    }

    /// Sets the lower-bound timestamp (exclusive) for incremental queries.
    fn with_start_timestamp(&self, timestamp: &str) -> Self {
        Self {
            inner: self.inner.clone().with_start_timestamp(timestamp),
        }
    }

    /// Sets the upper-bound timestamp (inclusive) for incremental queries.
    fn with_end_timestamp(&self, timestamp: &str) -> Self {
        Self {
            inner: self.inner.clone().with_end_timestamp(timestamp),
        }
    }

    /// Sets the target batch size (rows per batch) for streaming reads.
    /// Raises if `size == 0` — the parquet stream reader yields no batches
    /// for a zero-row target, so this is almost certainly a caller mistake.
    fn with_batch_size(&self, size: usize) -> PyResult<Self> {
        Ok(Self {
            inner: self
                .inner
                .clone()
                .with_batch_size(size)
                .map_err(PythonError::from)?,
        })
    }

    /// Sets column filters. Parses and cardinality-validates here; an
    /// unrecognized operator or empty `IN`/`NOT IN` value list raises.
    fn with_filters(&self, filters: Vec<(String, String, String)>) -> PyResult<Self> {
        Ok(Self {
            inner: self
                .inner
                .clone()
                .with_filters(filters)
                .map_err(PythonError::from)?,
        })
    }

    /// Sets the column projection (which columns to read).
    fn with_projection(&self, columns: Vec<String>) -> Self {
        Self {
            inner: self.inner.clone().with_projection(columns),
        }
    }

    /// Sets a single Hudi config that applies to this read only.
    fn with_hudi_option(&self, key: &str, value: &str) -> Self {
        Self {
            inner: self.inner.clone().with_hudi_option(key, value),
        }
    }

    /// Sets a batch of Hudi configs that apply to this read only.
    fn with_hudi_options(&self, opts: HashMap<String, String>) -> Self {
        Self {
            inner: self.inner.clone().with_hudi_options(opts),
        }
    }

    // ---- typed accessors (read from hudi_options) ----

    /// The query type (defaults to `Snapshot` when unset). Raises on bad strings.
    fn query_type(&self) -> PyResult<HudiQueryType> {
        self.inner
            .query_type()
            .map(HudiQueryType::from)
            .map_err(PythonError::from)
            .map_err(PyErr::from)
    }

    /// The as-of timestamp for snapshot/time-travel queries, if set.
    fn as_of_timestamp(&self) -> Option<String> {
        self.inner.as_of_timestamp().map(String::from)
    }

    /// The start timestamp (exclusive) for incremental queries, if set.
    fn start_timestamp(&self) -> Option<String> {
        self.inner.start_timestamp().map(String::from)
    }

    /// The end timestamp (inclusive) for incremental queries, if set.
    fn end_timestamp(&self) -> Option<String> {
        self.inner.end_timestamp().map(String::from)
    }

    /// The target batch size (rows per batch) for streaming reads, if set.
    /// Raises if the stored value is not a valid integer or is `0` (a zero-row
    /// batch yields no batches at the parquet stream reader).
    fn batch_size(&self) -> PyResult<Option<usize>> {
        self.inner
            .batch_size()
            .map_err(PythonError::from)
            .map_err(PyErr::from)
    }
}

impl HudiReadOptions {
    fn to_inner(&self) -> ReadOptions {
        self.inner.clone()
    }
}

#[cfg(not(tarpaulin_include))]
#[pyclass]
pub struct HudiRecordBatchStream {
    inner: Arc<Mutex<RecordBatchBoxStream>>,
}

#[cfg(not(tarpaulin_include))]
#[pymethods]
impl HudiRecordBatchStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(slf: PyRef<'_, Self>, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let stream = slf.inner.clone();
        let result = py.detach(|| {
            rt().block_on(async move {
                let mut stream = stream.lock().await;
                stream.next().await
            })
        });

        match result {
            Some(Ok(batch)) => Ok(Some(batch.to_pyarrow(py)?.unbind())),
            Some(Err(e)) => Err(PythonError::from(e).into()),
            None => Ok(None),
        }
    }
}

impl HudiRecordBatchStream {
    fn from_stream(stream: RecordBatchBoxStream) -> Self {
        Self {
            inner: Arc::new(Mutex::new(stream)),
        }
    }
}

#[cfg(not(tarpaulin_include))]
#[derive(Clone, Debug)]
#[pyclass]
pub struct HudiFileGroupReader {
    inner: FileGroupReader,
}

#[cfg(not(tarpaulin_include))]
#[pymethods]
impl HudiFileGroupReader {
    #[new]
    #[pyo3(signature = (base_uri, options=None))]
    fn new_with_options(
        py: Python,
        base_uri: &str,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let inner = py.detach(|| {
            rt().block_on(FileGroupReader::new_with_options(
                base_uri,
                options.unwrap_or_default(),
            ))
            .map_err(PythonError::from)
        })?;
        Ok(HudiFileGroupReader { inner })
    }

    #[pyo3(signature = (file_slice, options=None))]
    fn read_file_slice(
        &self,
        file_slice: &HudiFileSlice,
        options: Option<HudiReadOptions>,
        py: Python,
    ) -> PyResult<Py<PyAny>> {
        let read_options = options.unwrap_or_default().to_inner();
        let mut file_group = FileGroup::new_with_base_file_name(
            &file_slice.base_file_name,
            &file_slice.partition_path,
        )
        .map_err(PythonError::from)?;
        let log_file_names = &file_slice.log_file_names;
        file_group
            .add_log_files_from_names(log_file_names)
            .map_err(PythonError::from)?;
        let (_, file_slice) = file_group
            .file_slices
            .iter()
            .next()
            .ok_or_else(|| {
                CoreError::FileGroup(format!(
                    "Failed to get file slice from file group: {file_group:?}"
                ))
            })
            .map_err(PythonError::from)?;
        py.detach(|| {
            rt().block_on(self.inner.read_file_slice(file_slice, &read_options))
                .map_err(PythonError::from)
        })?
        .to_pyarrow(py)
        .map(|b| b.unbind())
    }

    #[pyo3(signature = (base_file_path, log_file_paths, options=None))]
    fn read_file_slice_from_paths(
        &self,
        base_file_path: &str,
        log_file_paths: Vec<String>,
        options: Option<HudiReadOptions>,
        py: Python,
    ) -> PyResult<Py<PyAny>> {
        let read_options = options.unwrap_or_default().to_inner();
        py.detach(|| {
            rt().block_on(self.inner.read_file_slice_from_paths(
                base_file_path,
                log_file_paths,
                &read_options,
            ))
            .map_err(PythonError::from)
        })?
        .to_pyarrow(py)
        .map(|b| b.unbind())
    }

    #[pyo3(signature = (file_slice, options=None))]
    fn read_file_slice_stream(
        &self,
        file_slice: &HudiFileSlice,
        options: Option<HudiReadOptions>,
        py: Python,
    ) -> PyResult<HudiRecordBatchStream> {
        let read_options = options.unwrap_or_default().to_inner();
        let mut file_group = FileGroup::new_with_base_file_name(
            &file_slice.base_file_name,
            &file_slice.partition_path,
        )
        .map_err(PythonError::from)?;
        file_group
            .add_log_files_from_names(&file_slice.log_file_names)
            .map_err(PythonError::from)?;
        let inner_reader = self.inner.clone();
        let stream = py.detach(|| {
            rt().block_on(async move {
                let (_, fs) = file_group.file_slices.iter().next().ok_or_else(|| {
                    CoreError::FileGroup(format!(
                        "Failed to get file slice from file group: {file_group:?}"
                    ))
                })?;
                inner_reader.read_file_slice_stream(fs, &read_options).await
            })
            .map_err(PythonError::from)
        })?;
        Ok(HudiRecordBatchStream::from_stream(stream))
    }

    #[pyo3(signature = (base_file_path, log_file_paths, options=None))]
    fn read_file_slice_from_paths_stream(
        &self,
        base_file_path: &str,
        log_file_paths: Vec<String>,
        options: Option<HudiReadOptions>,
        py: Python,
    ) -> PyResult<HudiRecordBatchStream> {
        let read_options = options.unwrap_or_default().to_inner();
        let stream = py.detach(|| {
            rt().block_on(self.inner.read_file_slice_from_paths_stream(
                base_file_path,
                log_file_paths,
                &read_options,
            ))
            .map_err(PythonError::from)
        })?;
        Ok(HudiRecordBatchStream::from_stream(stream))
    }

    /// Whether this reader targets a metadata table (its base path ends with `.hoodie/metadata`).
    #[getter]
    fn is_metadata_table(&self) -> bool {
        self.inner.is_metadata_table()
    }
}

#[cfg(not(tarpaulin_include))]
#[derive(Clone, Debug)]
#[pyclass]
pub struct HudiFileSlice {
    #[pyo3(get)]
    file_id: String,
    #[pyo3(get)]
    partition_path: String,
    #[pyo3(get)]
    creation_instant_time: String,
    #[pyo3(get)]
    base_file_name: String,
    #[pyo3(get)]
    base_file_size: u64,
    #[pyo3(get)]
    base_file_byte_size: i64,
    #[pyo3(get)]
    log_file_names: Vec<String>,
    #[pyo3(get)]
    num_records: i64,
}

#[cfg(not(tarpaulin_include))]
#[pymethods]
impl HudiFileSlice {
    fn base_file_relative_path(&self) -> PyResult<String> {
        let path = PathBuf::from(&self.partition_path)
            .join(&self.base_file_name)
            .to_str()
            .map(String::from)
            .ok_or_else(|| {
                StorageError::InvalidPath(format!(
                    "Failed to get base file relative path for file slice: {self:?}"
                ))
            })
            .map_err(CoreError::from)
            .map_err(PythonError::from)?;
        Ok(path)
    }
    fn log_files_relative_paths(&self) -> PyResult<Vec<String>> {
        let mut paths = Vec::<String>::new();
        for name in self.log_file_names.iter() {
            let p = PathBuf::from(&self.partition_path)
                .join(name)
                .to_str()
                .map(String::from)
                .ok_or_else(|| {
                    StorageError::InvalidPath(format!(
                        "Failed to get log file relative path for file slice: {self:?}"
                    ))
                })
                .map_err(CoreError::from)
                .map_err(PythonError::from)?;
            paths.push(p)
        }
        Ok(paths)
    }
}

#[cfg(not(tarpaulin_include))]
impl From<&FileSlice> for HudiFileSlice {
    fn from(f: &FileSlice) -> Self {
        let file_id = f.file_id().to_string();
        let partition_path = f.partition_path.to_string();
        let creation_instant_time = f.creation_instant_time().to_string();
        let base_file_name = f.base_file.file_name();
        let file_metadata = f.base_file.file_metadata.clone().unwrap_or_default();
        let base_file_size = file_metadata.size;
        let base_file_byte_size = file_metadata.byte_size;
        let log_file_names = f.log_files.iter().map(|l| l.file_name()).collect();
        let num_records = file_metadata.num_records;
        HudiFileSlice {
            file_id,
            partition_path,
            creation_instant_time,
            base_file_name,
            base_file_size,
            base_file_byte_size,
            log_file_names,
            num_records,
        }
    }
}

#[cfg(not(tarpaulin_include))]
#[derive(Clone, Debug)]
#[pyclass]
pub struct HudiInstant {
    inner: Instant,
}

#[cfg(not(tarpaulin_include))]
#[pymethods]
impl HudiInstant {
    #[getter]
    pub fn timestamp(&self) -> String {
        self.inner.timestamp.to_string()
    }

    #[getter]
    pub fn action(&self) -> String {
        self.inner.action.as_ref().to_string()
    }

    #[getter]
    pub fn state(&self) -> String {
        self.inner.state.as_ref().to_string()
    }

    #[getter]
    pub fn epoch_mills(&self) -> i64 {
        self.inner.epoch_millis
    }
}

impl From<&Instant> for HudiInstant {
    fn from(i: &Instant) -> Self {
        HudiInstant {
            inner: i.to_owned(),
        }
    }
}

#[cfg(not(tarpaulin_include))]
#[pyclass]
pub struct HudiTable {
    inner: Table,
}

#[cfg(not(tarpaulin_include))]
#[pymethods]
impl HudiTable {
    #[new]
    #[pyo3(signature = (base_uri, options=None))]
    fn new_with_options(
        py: Python,
        base_uri: &str,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let inner: Table = py.detach(|| {
            rt().block_on(Table::new_with_options(
                base_uri,
                options.unwrap_or_default(),
            ))
            .map_err(PythonError::from)
        })?;
        Ok(HudiTable { inner })
    }

    fn hudi_options(&self) -> HashMap<String, String> {
        self.inner.hudi_options()
    }

    fn storage_options(&self) -> HashMap<String, String> {
        self.inner.storage_options()
    }

    #[getter]
    fn table_name(&self) -> String {
        self.inner.table_name()
    }

    #[getter]
    fn table_type(&self) -> String {
        self.inner.table_type()
    }

    #[getter]
    fn is_mor(&self) -> bool {
        self.inner.is_mor()
    }

    #[getter]
    fn timezone(&self) -> String {
        self.inner.timezone()
    }

    fn get_schema_in_avro_str(&self, py: Python) -> PyResult<String> {
        py.detach(|| {
            let avro_schema = rt()
                .block_on(self.inner.get_schema_in_avro_str())
                .map_err(PythonError::from)?;
            Ok(avro_schema)
        })
    }

    fn get_schema_in_avro_str_with_meta_fields(&self, py: Python) -> PyResult<String> {
        py.detach(|| {
            let avro_schema = rt()
                .block_on(self.inner.get_schema_in_avro_str_with_meta_fields())
                .map_err(PythonError::from)?;
            Ok(avro_schema)
        })
    }

    fn get_schema(&self, py: Python) -> PyResult<Py<PyAny>> {
        py.detach(|| {
            rt().block_on(self.inner.get_schema())
                .map_err(PythonError::from)
        })?
        .to_pyarrow(py)
        .map(|b| b.unbind())
    }

    fn get_schema_with_meta_fields(&self, py: Python) -> PyResult<Py<PyAny>> {
        py.detach(|| {
            rt().block_on(self.inner.get_schema_with_meta_fields())
                .map_err(PythonError::from)
        })?
        .to_pyarrow(py)
        .map(|b| b.unbind())
    }

    fn get_partition_schema(&self, py: Python) -> PyResult<Py<PyAny>> {
        py.detach(|| {
            rt().block_on(self.inner.get_partition_schema())
                .map_err(PythonError::from)
        })?
        .to_pyarrow(py)
        .map(|b| b.unbind())
    }

    fn get_timeline(&self, py: Python) -> HudiTimeline {
        py.detach(|| {
            let timeline = self.inner.get_timeline();
            HudiTimeline::from(timeline)
        })
    }

    #[pyo3(signature = (options=None))]
    fn get_file_slices(
        &self,
        options: Option<HudiReadOptions>,
        py: Python,
    ) -> PyResult<Vec<HudiFileSlice>> {
        let read_options = options.unwrap_or_default().to_inner();
        py.detach(|| {
            let file_slices = rt()
                .block_on(self.inner.get_file_slices(&read_options))
                .map_err(PythonError::from)?;
            Ok(file_slices.iter().map(HudiFileSlice::from).collect())
        })
    }

    #[pyo3(signature = (read_options=None, extra_hudi_overrides=None, extra_storage_overrides=None))]
    fn create_file_group_reader_with_options(
        &self,
        read_options: Option<HudiReadOptions>,
        extra_hudi_overrides: Option<HashMap<String, String>>,
        extra_storage_overrides: Option<HashMap<String, String>>,
    ) -> PyResult<HudiFileGroupReader> {
        let read_options = read_options.map(|o| o.to_inner());
        let fg_reader = self
            .inner
            .create_file_group_reader_with_options(
                read_options.as_ref(),
                extra_hudi_overrides.unwrap_or_default(),
                extra_storage_overrides.unwrap_or_default(),
            )
            .map_err(PythonError::from)?;
        Ok(HudiFileGroupReader { inner: fg_reader })
    }

    #[pyo3(signature = (options=None))]
    fn read(&self, options: Option<HudiReadOptions>, py: Python) -> PyResult<Py<PyAny>> {
        let read_options = options.unwrap_or_default().to_inner();
        py.detach(|| {
            rt().block_on(self.inner.read(&read_options))
                .map_err(PythonError::from)
        })?
        .to_pyarrow(py)
        .map(|b| b.unbind())
    }

    #[getter]
    fn base_url(&self) -> String {
        self.inner.base_url().to_string()
    }

    fn compute_table_stats(&self, py: Python) -> Option<(u64, u64)> {
        py.detach(|| rt().block_on(self.inner.compute_table_stats()))
    }

    #[pyo3(signature = (options=None))]
    fn read_stream(
        &self,
        options: Option<HudiReadOptions>,
        py: Python,
    ) -> PyResult<HudiRecordBatchStream> {
        let read_options = options.unwrap_or_default().to_inner();
        let stream = py.detach(|| {
            rt().block_on(self.inner.read_stream(&read_options))
                .map_err(PythonError::from)
        })?;
        Ok(HudiRecordBatchStream::from_stream(stream))
    }
}

#[cfg(not(tarpaulin_include))]
#[pyclass]
pub struct HudiTimeline {
    inner: Timeline,
}

#[cfg(not(tarpaulin_include))]
#[pymethods]
impl HudiTimeline {
    #[pyo3(signature = (desc=false))]
    pub fn get_completed_commits(&self, desc: bool, py: Python) -> PyResult<Vec<HudiInstant>> {
        py.detach(|| {
            let instants = rt()
                .block_on(self.inner.get_completed_commits(desc))
                .map_err(PythonError::from)?;
            Ok(instants.iter().map(HudiInstant::from).collect())
        })
    }

    #[pyo3(signature = (desc=false))]
    pub fn get_completed_deltacommits(&self, desc: bool, py: Python) -> PyResult<Vec<HudiInstant>> {
        py.detach(|| {
            let instants = rt()
                .block_on(self.inner.get_completed_deltacommits(desc))
                .map_err(PythonError::from)?;
            Ok(instants.iter().map(HudiInstant::from).collect())
        })
    }

    #[pyo3(signature = (desc=false))]
    pub fn get_completed_replacecommits(
        &self,
        desc: bool,
        py: Python,
    ) -> PyResult<Vec<HudiInstant>> {
        py.detach(|| {
            let instants = rt()
                .block_on(self.inner.get_completed_replacecommits(desc))
                .map_err(PythonError::from)?;
            Ok(instants.iter().map(HudiInstant::from).collect())
        })
    }

    #[pyo3(signature = (desc=false))]
    pub fn get_completed_clustering_commits(
        &self,
        desc: bool,
        py: Python,
    ) -> PyResult<Vec<HudiInstant>> {
        py.detach(|| {
            let instants = rt()
                .block_on(self.inner.get_completed_clustering_commits(desc))
                .map_err(PythonError::from)?;
            Ok(instants.iter().map(HudiInstant::from).collect())
        })
    }

    pub fn get_instant_metadata_in_json(
        &self,
        instant: &HudiInstant,
        py: Python,
    ) -> PyResult<String> {
        py.detach(|| {
            let commit_metadata = rt()
                .block_on(self.inner.get_instant_metadata_in_json(&instant.inner))
                .map_err(PythonError::from)?;
            Ok(commit_metadata)
        })
    }

    pub fn get_latest_commit_timestamp(&self, py: Python) -> PyResult<String> {
        py.detach(|| {
            let commit_timestamp = self
                .inner
                .get_latest_commit_timestamp()
                .map_err(PythonError::from)?;
            Ok(commit_timestamp)
        })
    }

    pub fn get_latest_avro_schema(&self, py: Python) -> PyResult<String> {
        py.detach(|| {
            let schema = rt()
                .block_on(self.inner.get_latest_avro_schema())
                .map_err(PythonError::from)?;
            Ok(schema)
        })
    }

    pub fn get_latest_schema(&self, py: Python) -> PyResult<Py<PyAny>> {
        py.detach(|| {
            rt().block_on(self.inner.get_latest_schema())
                .map_err(PythonError::from)
        })?
        .to_pyarrow(py)
        .map(|b| b.unbind())
    }
}

impl From<&Timeline> for HudiTimeline {
    fn from(t: &Timeline) -> Self {
        HudiTimeline {
            inner: t.to_owned(),
        }
    }
}

#[cfg(not(tarpaulin_include))]
#[pyfunction]
#[pyo3(signature = (base_uri, hudi_options=None, storage_options=None, options=None))]
pub fn build_hudi_table(
    py: Python,
    base_uri: String,
    hudi_options: Option<HashMap<String, String>>,
    storage_options: Option<HashMap<String, String>>,
    options: Option<HashMap<String, String>>,
) -> PyResult<HudiTable> {
    let inner = py.detach(|| {
        rt().block_on(
            TableBuilder::from_base_uri(&base_uri)
                .with_hudi_options(hudi_options.unwrap_or_default())
                .with_storage_options(storage_options.unwrap_or_default())
                .with_options(options.unwrap_or_default())
                .build(),
        )
        .map_err(PythonError::from)
    })?;
    Ok(HudiTable { inner })
}

#[cfg(not(tarpaulin_include))]
#[pyfunction]
pub fn _config_keys() -> HashMap<String, Vec<(String, String)>> {
    fn collect<E>() -> Vec<(String, String)>
    where
        E: ::strum::IntoEnumIterator + AsRef<str>,
        for<'a> &'a E: Into<&'static str>,
    {
        E::iter()
            .map(|v| {
                let pascal: &'static str = (&v).into();
                (pascal_to_screaming_snake(pascal), v.as_ref().to_string())
            })
            .collect()
    }

    let mut out = HashMap::new();
    out.insert("HudiTableConfig".to_string(), collect::<HudiTableConfig>());
    out.insert("HudiReadConfig".to_string(), collect::<HudiReadConfig>());
    out
}

fn pascal_to_screaming_snake(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 4);
    let chars: Vec<char> = input.chars().collect();
    for (i, &ch) in chars.iter().enumerate() {
        let is_boundary = i > 0
            && ch.is_uppercase()
            && (chars[i - 1].is_lowercase()
                || (i + 1 < chars.len() && chars[i + 1].is_lowercase()));
        if is_boundary {
            out.push('_');
        }
        for upper_ch in ch.to_uppercase() {
            out.push(upper_ch);
        }
    }
    out
}

#[cfg(not(tarpaulin_include))]
pub fn rt() -> &'static Runtime {
    static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();
    TOKIO_RT.get_or_init(|| Runtime::new().expect("Failed to create a tokio runtime."))
}

#[cfg(test)]
mod tests {
    use super::pascal_to_screaming_snake;

    #[test]
    fn pascal_to_screaming_snake_basic() {
        assert_eq!(
            pascal_to_screaming_snake("BaseFileFormat"),
            "BASE_FILE_FORMAT"
        );
        assert_eq!(pascal_to_screaming_snake("TableName"), "TABLE_NAME");
        assert_eq!(pascal_to_screaming_snake("URLEncoded"), "URL_ENCODED");
        assert_eq!(pascal_to_screaming_snake("A"), "A");
    }
}
