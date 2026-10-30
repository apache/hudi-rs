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

use arrow::array::{
    Array, ArrayData, ArrayRef, AsArray, GenericListArray, OffsetSizeTrait, make_array,
};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::TimestampNanosecondType;
use arrow::datatypes::{DataType, FieldRef, Schema, TimeUnit};
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use futures::StreamExt;
use futures::stream::BoxStream;
use std::collections::HashMap;
use std::convert::From;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

#[cfg(feature = "datafusion")]
use datafusion::error::DataFusionError;

use hudi::config::plan::HudiPlanConfig;
use hudi::config::read::HudiReadConfig;
use hudi::config::table::{HudiTableConfig, TableTypeValue};
use hudi::error::CoreError;
use hudi::error::Result as HudiResult;
use hudi::file_group::FileGroup;
use hudi::file_group::file_slice::FileSlice;
use hudi::file_group::reader::FileGroupReader;
use hudi::storage::error::StorageError;
use hudi::table::CompactionResult;
use hudi::table::builder::TableBuilder;
use hudi::table::{QueryType, ReadOptions, Table};
use hudi::timeline::Timeline;
use hudi::timeline::instant::Instant;
use hudi::{HoodieKey, UpsertOptions, WriteResult};
use pyo3::exceptions::PyException;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList, PyString, PyTuple};
use pyo3::{PyErr, PyResult, Python, create_exception, intern, pyclass, pyfunction, pymethods};
use std::error::Error;

type RecordBatchBoxStream = BoxStream<'static, HudiResult<RecordBatch>>;

create_exception!(hudi._internal, HudiCoreError, PyException);

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
#[pyclass(eq, from_py_object)]
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
#[pyclass(from_py_object)]
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
#[pyclass(from_py_object)]
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
#[pyclass(from_py_object)]
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
    log_file_sizes: Vec<u64>,
    #[pyo3(get)]
    num_records: i64,
    column_stats: Option<hudi::statistics::StatisticsContainer>,
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
    /// Total on-disk size (base + log files) in bytes.
    /// Use for I/O cost estimation and split sizing.
    fn total_size_bytes(&self) -> u64 {
        self.base_file_size + self.log_file_sizes.iter().sum::<u64>()
    }

    fn has_log_files(&self) -> bool {
        !self.log_file_names.is_empty()
    }

    /// Column statistics (min, max) from the base file Parquet footer.
    ///
    /// Returns a dict mapping column names to (min_array, max_array) tuples,
    /// where each is a single-element PyArrow array or None.
    /// Only populated when data-column filters trigger footer-based pruning
    /// on COW tables or MOR read-optimized mode.
    fn base_file_column_stats(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        use pyo3::types::PyDict;

        let Some(stats) = &self.column_stats else {
            return Ok(None);
        };
        let dict = PyDict::new(py);
        for (name, col_stats) in &stats.columns {
            let min_val = col_stats
                .min_value
                .as_ref()
                .map(|arr| arr.to_data().to_pyarrow(py).map(|b| b.unbind()))
                .transpose()?;
            let max_val = col_stats
                .max_value
                .as_ref()
                .map(|arr| arr.to_data().to_pyarrow(py).map(|b| b.unbind()))
                .transpose()?;
            dict.set_item(name, (min_val, max_val))?;
        }
        Ok(Some(dict.into()))
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
        // A slice with no base file reports an empty name and zero sizes rather
        // than changing the shape of what Python already receives.
        let base_file_name = f
            .base_file
            .as_ref()
            .map(|b| b.file_name())
            .unwrap_or_default();
        let file_metadata = f
            .base_file
            .as_ref()
            .and_then(|b| b.file_metadata.clone())
            .unwrap_or_default();
        let base_file_size = file_metadata.size;
        let base_file_byte_size = file_metadata.byte_size;
        let log_file_names = f.log_files.iter().map(|l| l.file_name()).collect();
        let log_file_sizes = f
            .log_files
            .iter()
            .map(|lf| lf.file_metadata.as_ref().map(|m| m.size).unwrap_or(0))
            .collect();
        let num_records = file_metadata.num_records;
        let column_stats = f.base_file_column_stats.clone();
        HudiFileSlice {
            file_id,
            partition_path,
            creation_instant_time,
            base_file_name,
            base_file_size,
            base_file_byte_size,
            log_file_names,
            log_file_sizes,
            num_records,
            column_stats,
        }
    }
}

#[cfg(not(tarpaulin_include))]
#[derive(Clone, Debug)]
#[pyclass(from_py_object)]
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

    #[pyo3(signature = (read_options=None, extra_storage_overrides=None))]
    fn create_file_group_reader_with_options(
        &self,
        read_options: Option<HudiReadOptions>,
        extra_storage_overrides: Option<HashMap<String, String>>,
        py: Python,
    ) -> PyResult<HudiFileGroupReader> {
        let read_options = read_options.map(|o| o.to_inner());
        // Resolving the table's data schema reads the timeline, so this blocks;
        // release the GIL for it like the other I/O-bound methods here.
        let fg_reader = py.detach(|| {
            rt().block_on(self.inner.create_file_group_reader_with_options(
                read_options.as_ref(),
                extra_storage_overrides.unwrap_or_default(),
            ))
            .map_err(PythonError::from)
        })?;
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

    #[pyo3(signature = (options=None))]
    fn compute_table_stats(
        &self,
        options: Option<HudiReadOptions>,
        py: Python,
    ) -> Option<(u64, u64)> {
        let read_options = options.map(|o| o.to_inner());
        py.detach(|| rt().block_on(self.inner.compute_table_stats(read_options.as_ref())))
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

    #[staticmethod]
    #[pyo3(signature = (
        base_uri,
        *,
        record_key,
        partition_by=None,
        ordering_fields=None,
        table_type="cow",
        table_name=None,
        table_version=None,
        metadata_table_enabled=None,
        record_index_enabled=None,
        column_stats_enabled=None,
        partition_stats_enabled=None,
        hive_style_partitioning=None,
        hudi_options=None,
        storage_options=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn create(
        py: Python,
        base_uri: &str,
        record_key: OneOrMany,
        partition_by: Option<OneOrMany>,
        ordering_fields: Option<OneOrMany>,
        table_type: &str,
        table_name: Option<String>,
        table_version: Option<isize>,
        metadata_table_enabled: Option<bool>,
        record_index_enabled: Option<bool>,
        column_stats_enabled: Option<bool>,
        partition_stats_enabled: Option<bool>,
        hive_style_partitioning: Option<bool>,
        hudi_options: Option<HashMap<String, String>>,
        storage_options: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let table_type = TableTypeValue::from_str(table_type).map_err(|_| {
            PyValueError::new_err(format!(
                "invalid table_type '{table_type}': expected 'cow' (COPY_ON_WRITE) or 'mor' (MERGE_ON_READ)"
            ))
        })?;
        let table_name = table_name.unwrap_or_else(|| last_path_segment(base_uri));
        let mut builder = Table::create(base_uri)
            .with_table_name(table_name)
            .with_table_type(table_type)
            .with_record_key_fields(record_key.into_vec());
        if let Some(fields) = partition_by {
            builder = builder.with_partition_fields(fields.into_vec());
        }
        if let Some(fields) = ordering_fields {
            builder = builder.with_ordering_fields(fields.into_vec());
        }
        // Order matters: the version and metadata setters adjust the index
        // flags that follow them, so explicit index choices must come last.
        if let Some(version) = table_version {
            builder = builder.with_table_version(version);
        }
        if let Some(enabled) = metadata_table_enabled {
            builder = builder.with_metadata_table_enabled(enabled);
        }
        if let Some(enabled) = record_index_enabled {
            builder = builder.with_record_index_enabled(enabled);
        }
        if let Some(enabled) = column_stats_enabled {
            builder = builder.with_column_stats_enabled(enabled);
        }
        if let Some(enabled) = partition_stats_enabled {
            builder = builder.with_partition_stats_enabled(enabled);
        }
        if let Some(enabled) = hive_style_partitioning {
            builder = builder.with_hive_style_partitioning(enabled);
        }
        for (k, v) in hudi_options.unwrap_or_default() {
            builder = builder.with_option(k, v);
        }
        for (k, v) in storage_options.unwrap_or_default() {
            builder = builder.with_storage_option(k, v);
        }
        let inner = py.detach(|| rt().block_on(builder.create()).map_err(PythonError::from))?;
        Ok(HudiTable { inner })
    }

    fn append(&mut self, data: &Bound<'_, PyAny>, py: Python) -> PyResult<HudiWriteResult> {
        let batches = to_batches(data)?;
        let result = py.detach(|| {
            rt().block_on(self.inner.append(batches))
                .map_err(PythonError::from)
        })?;
        Ok(HudiWriteResult::from(result))
    }

    #[pyo3(signature = (data, update_columns=None))]
    fn upsert(
        &mut self,
        data: &Bound<'_, PyAny>,
        update_columns: Option<Vec<String>>,
        py: Python,
    ) -> PyResult<HudiWriteResult> {
        let batches = to_batches(data)?;
        let options = UpsertOptions { update_columns };
        let result = py.detach(|| {
            rt().block_on(self.inner.upsert_with(batches, options))
                .map_err(PythonError::from)
        })?;
        Ok(HudiWriteResult::from(result))
    }

    fn update(
        &mut self,
        predicate: &str,
        updates: &Bound<'_, PyAny>,
        py: Python,
    ) -> PyResult<HudiWriteResult> {
        let updates = if let Ok(dict) = updates.cast::<PyDict>() {
            let schema = py.detach(|| {
                rt().block_on(self.inner.get_schema())
                    .map_err(PythonError::from)
            })?;
            one_row_batch_from_dict(py, &schema, dict)?
        } else {
            let batches = to_batches(updates)?;
            let Some(first) = batches.first() else {
                return Err(PyValueError::new_err(
                    "`updates` must contain exactly one row, got no batches",
                ));
            };
            concat_batches(&first.schema(), &batches).map_err(|e| {
                PyValueError::new_err(format!("failed to combine `updates` batches: {e}"))
            })?
        };
        let result = py.detach(|| {
            rt().block_on(self.inner.update(predicate, updates))
                .map_err(PythonError::from)
        })?;
        Ok(HudiWriteResult::from(result))
    }

    fn delete(&mut self, predicate: &str, py: Python) -> PyResult<HudiWriteResult> {
        let result = py.detach(|| {
            rt().block_on(self.inner.delete(predicate))
                .map_err(PythonError::from)
        })?;
        Ok(HudiWriteResult::from(result))
    }

    fn delete_keys(&mut self, keys: &Bound<'_, PyAny>, py: Python) -> PyResult<HudiWriteResult> {
        let keys = to_hoodie_keys(keys)?;
        let result = py.detach(|| {
            rt().block_on(self.inner.delete_keys(keys))
                .map_err(PythonError::from)
        })?;
        Ok(HudiWriteResult::from(result))
    }

    fn overwrite(&mut self, data: &Bound<'_, PyAny>, py: Python) -> PyResult<HudiWriteResult> {
        let batches = to_batches(data)?;
        let result = py.detach(|| {
            rt().block_on(self.inner.overwrite(batches))
                .map_err(PythonError::from)
        })?;
        Ok(HudiWriteResult::from(result))
    }

    fn dynamic_partition_overwrite(
        &mut self,
        data: &Bound<'_, PyAny>,
        py: Python,
    ) -> PyResult<HudiWriteResult> {
        let batches = to_batches(data)?;
        let result = py.detach(|| {
            rt().block_on(self.inner.dynamic_partition_overwrite(batches))
                .map_err(PythonError::from)
        })?;
        Ok(HudiWriteResult::from(result))
    }

    fn compact(&mut self, py: Python) -> PyResult<HudiCompactionResult> {
        let result = py.detach(|| {
            rt().block_on(self.inner.compact())
                .map_err(PythonError::from)
        })?;
        Ok(HudiCompactionResult::from(result))
    }

    fn await_compaction(&mut self, py: Python) -> PyResult<Option<HudiCompactionResult>> {
        let result = py.detach(|| {
            rt().block_on(self.inner.await_compaction())
                .map_err(PythonError::from)
        })?;
        Ok(result.map(HudiCompactionResult::from))
    }
}

/// Result of a compaction.
#[cfg(not(tarpaulin_include))]
#[pyclass(frozen, get_all)]
pub struct HudiCompactionResult {
    instant_time: Option<String>,
    completion_time: Option<String>,
    num_file_groups: usize,
    num_log_files: usize,
    num_writes: usize,
}

#[cfg(not(tarpaulin_include))]
#[pymethods]
impl HudiCompactionResult {
    fn __repr__(&self) -> String {
        let quoted = |t: &Option<String>| {
            t.as_ref()
                .map_or_else(|| "None".to_string(), |t| format!("'{t}'"))
        };
        format!(
            "HudiCompactionResult(instant_time={}, completion_time={}, num_file_groups={}, num_log_files={}, num_writes={})",
            quoted(&self.instant_time),
            quoted(&self.completion_time),
            self.num_file_groups,
            self.num_log_files,
            self.num_writes,
        )
    }
}

impl From<CompactionResult> for HudiCompactionResult {
    fn from(r: CompactionResult) -> Self {
        Self {
            instant_time: committed(r.instant),
            completion_time: r.completion_time,
            num_file_groups: r.num_file_groups,
            num_log_files: r.num_log_files,
            num_writes: r.num_writes,
        }
    }
}

/// The core reports a compaction that wrote nothing with an empty instant.
fn committed(instant: String) -> Option<String> {
    (!instant.is_empty()).then_some(instant)
}

/// Result of a table write, shared by every write verb.
#[cfg(not(tarpaulin_include))]
#[pyclass(frozen, get_all)]
pub struct HudiWriteResult {
    instant_time: Option<String>,
    completion_time: Option<String>,
    num_writes: usize,
    num_updates: usize,
    num_inserts: usize,
    num_deletes: usize,
}

#[cfg(not(tarpaulin_include))]
#[pymethods]
impl HudiWriteResult {
    fn __repr__(&self) -> String {
        let quoted = |t: &Option<String>| {
            t.as_ref()
                .map_or_else(|| "None".to_string(), |t| format!("'{t}'"))
        };
        format!(
            "HudiWriteResult(instant_time={}, completion_time={}, num_writes={}, num_updates={}, num_inserts={}, num_deletes={})",
            quoted(&self.instant_time),
            quoted(&self.completion_time),
            self.num_writes,
            self.num_updates,
            self.num_inserts,
            self.num_deletes,
        )
    }
}

impl From<WriteResult> for HudiWriteResult {
    fn from(r: WriteResult) -> Self {
        Self {
            instant_time: r.requested_time,
            completion_time: r.completion_time,
            num_writes: r.num_writes,
            num_updates: r.num_updates,
            num_inserts: r.num_inserts,
            num_deletes: r.num_deletes,
        }
    }
}

/// A Python argument that is either one string or a list of strings.
#[cfg(not(tarpaulin_include))]
#[derive(FromPyObject)]
enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

#[cfg(not(tarpaulin_include))]
impl OneOrMany {
    fn into_vec(self) -> Vec<String> {
        match self {
            OneOrMany::One(s) => vec![s],
            OneOrMany::Many(v) => v,
        }
    }
}

fn last_path_segment(base_uri: &str) -> String {
    base_uri
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_string()
}

/// Convert any supported Python tabular object into record batches.
///
/// Objects implementing the Arrow PyCapsule interface (`__arrow_c_stream__`,
/// `__arrow_c_array__`) are imported zero-copy, which covers pyarrow, polars
/// and pandas >= 2.2. The pyarrow class checks serve pyarrow releases that
/// predate the PyCapsule interface.
#[cfg(not(tarpaulin_include))]
fn to_batches(data: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    collect_batches(data)?
        .into_iter()
        .map(|batch| normalize_batch(batch).map_err(PyValueError::new_err))
        .collect()
}

/// Rewrite a batch into types the writer accepts: drop the columns pandas
/// adds for an unnamed index, and cast view and large string, binary and
/// list types (Polars exports `Utf8View`, pandas exports `LargeUtf8`) and
/// dictionaries to their plain forms at any nesting depth.
///
/// Errors are plain strings so the function runs without an interpreter.
fn normalize_batch(batch: RecordBatch) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let from_pandas = schema.metadata().contains_key("pandas");
    let mut changed = false;
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        if from_pandas && is_unnamed_pandas_index(field.name()) {
            changed = true;
            continue;
        }
        match writable_field(field) {
            Some(target) => {
                // The cast to microseconds truncates silently.
                if has_sub_microsecond(column.as_ref()) {
                    return Err(format!(
                        "column '{}' has timestamps with sub-microsecond precision, \
                         which Hudi tables cannot store",
                        field.name()
                    ));
                }
                let casted = cast(column.as_ref(), target.data_type()).map_err(|e| {
                    format!(
                        "failed to cast column '{}' from {} to {}: {e}",
                        field.name(),
                        field.data_type(),
                        target.data_type()
                    )
                })?;
                fields.push(target);
                columns.push(casted);
                changed = true;
            }
            None => {
                fields.push(field.clone());
                columns.push(column.clone());
            }
        }
    }
    if !changed {
        return Ok(batch);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
        &options,
    )
    .map_err(|e| format!("failed to rebuild record batch: {e}"))
}

/// Whether a nanosecond timestamp anywhere in `array` is not a whole microsecond.
///
/// Only values the array references are checked: a sliced list keeps its
/// whole child buffer and a dictionary may hold unused values.
fn has_sub_microsecond(array: &dyn Array) -> bool {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_primitive::<TimestampNanosecondType>()
            .iter()
            .flatten()
            .any(|v| v % 1000 != 0),
        DataType::List(_) => {
            has_sub_microsecond(referenced_values(array.as_list::<i32>()).as_ref())
        }
        DataType::LargeList(_) => {
            has_sub_microsecond(referenced_values(array.as_list::<i64>()).as_ref())
        }
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .any(|c| has_sub_microsecond(c.as_ref())),
        DataType::Dictionary(_, value) => {
            cast(array, value).is_ok_and(|plain| has_sub_microsecond(plain.as_ref()))
        }
        _ => false,
    }
}

fn referenced_values<O: OffsetSizeTrait>(list: &GenericListArray<O>) -> ArrayRef {
    let offsets = list.value_offsets();
    let start = offsets[0].as_usize();
    let end = offsets[offsets.len() - 1].as_usize();
    list.values().slice(start, end - start)
}

/// pandas exports an unnamed index as `__index_level_N__`; a named index
/// keeps its name and may be the record key.
fn is_unnamed_pandas_index(name: &str) -> bool {
    name.strip_prefix("__index_level_")
        .and_then(|rest| rest.strip_suffix("__"))
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

/// The writable form of `field`, or `None` when it is already writable.
/// Names, nullability and metadata are kept, including list item names.
fn writable_field(field: &FieldRef) -> Option<FieldRef> {
    writable_type(field.data_type()).map(|dt| Arc::new(field.as_ref().clone().with_data_type(dt)))
}

fn writable_type(dt: &DataType) -> Option<DataType> {
    match dt {
        DataType::Utf8View | DataType::LargeUtf8 => Some(DataType::Utf8),
        DataType::BinaryView | DataType::LargeBinary => Some(DataType::Binary),
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, tz.clone()))
        }
        DataType::Dictionary(_, value) => {
            Some(writable_type(value).unwrap_or_else(|| value.as_ref().clone()))
        }
        DataType::List(item) => writable_field(item).map(DataType::List),
        DataType::LargeList(item) => Some(DataType::List(
            writable_field(item).unwrap_or_else(|| item.clone()),
        )),
        DataType::Struct(children) => {
            let mapped: Vec<Option<FieldRef>> = children.iter().map(writable_field).collect();
            if mapped.iter().all(Option::is_none) {
                return None;
            }
            Some(DataType::Struct(
                children
                    .iter()
                    .zip(mapped)
                    .map(|(child, m)| m.unwrap_or_else(|| child.clone()))
                    .collect(),
            ))
        }
        _ => None,
    }
}

#[cfg(not(tarpaulin_include))]
fn collect_batches(data: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    let py = data.py();
    if let Ok(list) = data.cast::<PyList>() {
        return list
            .iter()
            .enumerate()
            .map(|(i, item)| {
                RecordBatch::from_pyarrow_bound(&item).map_err(|e| {
                    PyTypeError::new_err(format!(
                        "data[{i}] is not a pyarrow.RecordBatch or an Arrow array-capsule object: {e}"
                    ))
                })
            })
            .collect();
    }
    if data.hasattr(intern!(py, "__arrow_c_stream__"))? {
        return collect_stream(data);
    }
    if data.hasattr(intern!(py, "__arrow_c_array__"))? {
        return Ok(vec![RecordBatch::from_pyarrow_bound(data)?]);
    }
    let pa = py.import(intern!(py, "pyarrow"))?;
    if data.is_instance(&pa.getattr(intern!(py, "RecordBatchReader"))?)? {
        return collect_stream(data);
    }
    if data.is_instance(&pa.getattr(intern!(py, "RecordBatch"))?)? {
        return Ok(vec![RecordBatch::from_pyarrow_bound(data)?]);
    }
    if data.is_instance(&pa.getattr(intern!(py, "Table"))?)? {
        return collect_batches(&data.call_method0(intern!(py, "to_batches"))?);
    }
    Err(PyTypeError::new_err(format!(
        "unsupported data type '{}': expected a pyarrow Table, RecordBatch, RecordBatchReader, \
         a list of RecordBatch, or an object implementing __arrow_c_stream__",
        data.get_type().name()?
    )))
}

#[cfg(not(tarpaulin_include))]
fn collect_stream(data: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    ArrowArrayStreamReader::from_pyarrow_bound(data)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| PyValueError::new_err(format!("failed to read Arrow stream: {e}")))
}

/// Build the one-row `SET` batch for `update`, typing each value by the
/// table schema so that e.g. a Python int lands in a float64 column.
#[cfg(not(tarpaulin_include))]
fn one_row_batch_from_dict(
    py: Python,
    schema: &Schema,
    updates: &Bound<'_, PyDict>,
) -> PyResult<RecordBatch> {
    if updates.is_empty() {
        return Err(PyValueError::new_err(
            "`updates` must name at least one column",
        ));
    }
    let pa = py.import(intern!(py, "pyarrow"))?;
    let mut fields = Vec::with_capacity(updates.len());
    let mut columns = Vec::with_capacity(updates.len());
    for (key, value) in updates.iter() {
        let name: String = key.extract().map_err(|_| {
            PyValueError::new_err(format!(
                "update column names must be str, got {}",
                key.repr().map(|r| r.to_string()).unwrap_or_default()
            ))
        })?;
        let field = schema.field_with_name(&name).map_err(|_| {
            PyValueError::new_err(format!("update column '{name}' is not in the table schema"))
        })?;
        let value = unwrap_scalars(&value, &pa.getattr(intern!(py, "Scalar"))?)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item(intern!(py, "type"), field.data_type().to_pyarrow(py)?)?;
        let array = pa
            .call_method(
                intern!(py, "array"),
                (PyList::new(py, [&value])?,),
                Some(&kwargs),
            )
            .map_err(|e| {
                PyValueError::new_err(format!(
                    "cannot convert update value for column '{name}' to {}: {e}",
                    field.data_type()
                ))
            })?;
        let stored = array
            .get_item(0)
            .and_then(|v| v.call_method0(intern!(py, "as_py")))
            .map_err(|e| {
                PyValueError::new_err(format!(
                    "cannot read back update value for column '{name}' as {}: {e}",
                    field.data_type()
                ))
            })?;
        if let Some(reason) = misfit(field.data_type(), &value, &stored)? {
            return Err(PyValueError::new_err(format!(
                "update value {} for column '{name}' does not fit {} exactly: {reason} \
                 (stored as {})",
                value.repr()?,
                field.data_type(),
                stored.repr()?
            )));
        }
        columns.push(make_array(ArrayData::from_pyarrow_bound(&array)?));
        fields.push(field.clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| PyValueError::new_err(format!("invalid `updates`: {e}")))
}

/// `value` with every `pyarrow.Scalar`, at any depth of dicts, lists and
/// tuples, replaced by its `as_py()`.
#[cfg(not(tarpaulin_include))]
fn unwrap_scalars<'py>(
    value: &Bound<'py, PyAny>,
    scalar: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let py = value.py();
    if value.is_instance(scalar)? {
        return value.call_method0(intern!(py, "as_py"));
    }
    if let Ok(dict) = value.cast::<PyDict>() {
        let out = PyDict::new(py);
        for (k, v) in dict.iter() {
            out.set_item(k, unwrap_scalars(&v, scalar)?)?;
        }
        return Ok(out.into_any());
    }
    let is_tuple = value.is_instance_of::<PyTuple>();
    if is_tuple || value.is_instance_of::<PyList>() {
        let items = value
            .try_iter()?
            .map(|v| unwrap_scalars(&v?, scalar))
            .collect::<PyResult<Vec<_>>>()?;
        return Ok(if is_tuple {
            PyTuple::new(py, items)?.into_any()
        } else {
            PyList::new(py, items)?.into_any()
        });
    }
    Ok(value.clone())
}

/// Why `value` does not survive the typed `pa.array` constructor for `dt`,
/// given `stored`, the `as_py()` of what it produced; `None` when it fits.
///
/// The constructor coerces silently: 1.5 into int64 stores 1, a numpy bool
/// into int64 stores 1, a datetime into date32 drops the time, a number into
/// a timestamp is read as epoch units, a coarser timestamp unit drops
/// sub-unit digits, and unknown struct keys are dropped. Python equality
/// accepts exact conversions (3 == 3.0);
/// timestamps are not compared whole because a naive input never equals its
/// UTC-aware stored form.
#[cfg(not(tarpaulin_include))]
fn misfit(
    dt: &DataType,
    value: &Bound<'_, PyAny>,
    stored: &Bound<'_, PyAny>,
) -> PyResult<Option<String>> {
    if value.is_none() || stored.is_none() {
        return Ok(None);
    }
    let py = value.py();
    match dt {
        DataType::Struct(children) => {
            let by_name = value.cast::<PyDict>().ok();
            if let Some(d) = &by_name {
                for key in d.keys() {
                    let known = key
                        .extract::<String>()
                        .is_ok_and(|k| children.iter().any(|c| c.name() == &k));
                    if !known {
                        return Ok(Some(format!("unknown struct field {}", key.repr()?)));
                    }
                }
            }
            let mut positional = match by_name {
                Some(_) => None,
                None => Some(value.try_iter()?),
            };
            for child in children {
                let v = match (&by_name, positional.as_mut()) {
                    (Some(d), _) => d.get_item(child.name())?,
                    (None, Some(it)) => it.next().transpose()?,
                    (None, None) => None,
                };
                if let Some(v) = v {
                    let s = stored.get_item(child.name())?;
                    if let Some(reason) = misfit(child.data_type(), &v, &s)? {
                        return Ok(Some(reason));
                    }
                }
            }
            Ok(None)
        }
        DataType::List(item) | DataType::LargeList(item) => {
            for (v, s) in value.try_iter()?.zip(stored.try_iter()?) {
                if let Some(reason) = misfit(item.data_type(), &v?, &s?)? {
                    return Ok(Some(reason));
                }
            }
            Ok(None)
        }
        dt if dt.is_numeric() && is_bool(value)? => Ok(Some("a bool is not a number".into())),
        DataType::Float16 | DataType::Float32 => {
            let (v, s): (f64, f64) = (value.extract()?, stored.extract()?);
            Ok((v.is_finite() && !s.is_finite())
                .then(|| "the value overflows the float type".into()))
        }
        dt if dt.is_numeric() => {
            let both_nan = stored.ne(stored)? && value.ne(value)?;
            Ok((!(stored.eq(value)? || both_nan)).then(|| "the value would change".into()))
        }
        DataType::Date32 | DataType::Date64 => {
            let datetime = py.import(intern!(py, "datetime"))?;
            let is_date = value.is_instance(&datetime.getattr(intern!(py, "date"))?)?;
            let is_datetime = value.is_instance(&datetime.getattr(intern!(py, "datetime"))?)?;
            Ok((!is_date || is_datetime).then(|| "expected a datetime.date".into()))
        }
        DataType::Timestamp(unit, tz) => {
            let datetime = py
                .import(intern!(py, "datetime"))?
                .getattr(intern!(py, "datetime"))?;
            if !value.is_instance(&datetime)? {
                return Ok(Some("expected a datetime.datetime".into()));
            }
            if tz.is_none() && !value.getattr(intern!(py, "tzinfo"))?.is_none() {
                return Ok(Some(
                    "a tz-aware datetime is ambiguous for a timestamp without a time zone".into(),
                ));
            }
            let nanos = intern!(py, "nanosecond");
            let micros = intern!(py, "microsecond");
            let truncated = (*unit != TimeUnit::Nanosecond
                && value.hasattr(nanos)?
                && !value.getattr(nanos)?.eq(0)?)
                || !stored.getattr(micros)?.eq(value.getattr(micros)?)?;
            Ok(truncated.then(|| "the timestamp unit would truncate the value".into()))
        }
        _ => Ok(None),
    }
}

#[cfg(not(tarpaulin_include))]
fn is_bool(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if value.is_instance_of::<PyBool>() {
        return Ok(true);
    }
    let ty = value.get_type();
    let module: String = ty.getattr(intern!(value.py(), "__module__"))?.extract()?;
    let name = ty.name()?;
    Ok(module == "numpy" && (name == "bool" || name == "bool_"))
}

/// Accept record keys as `str` (empty partition path) or `(record_key, partition_path)`.
#[cfg(not(tarpaulin_include))]
fn to_hoodie_keys(keys: &Bound<'_, PyAny>) -> PyResult<Vec<HoodieKey>> {
    if keys.is_instance_of::<PyString>() {
        return Err(PyTypeError::new_err(
            "keys must be a list of record keys, not a single str",
        ));
    }
    keys.try_iter()?
        .map(|item| {
            let item = item?;
            if let Ok(record_key) = item.extract::<String>() {
                return Ok(HoodieKey {
                    record_key,
                    partition_path: String::new(),
                });
            }
            let (record_key, partition_path) =
                item.extract::<(String, String)>().map_err(|_| {
                    PyTypeError::new_err(format!(
                        "each key must be a str or a (record_key, partition_path) tuple, got {}",
                        item.get_type()
                            .name()
                            .map(|n| n.to_string())
                            .unwrap_or_default()
                    ))
                })?;
            Ok(HoodieKey {
                record_key,
                partition_path,
            })
        })
        .collect()
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
    out.insert("HudiPlanConfig".to_string(), collect::<HudiPlanConfig>());
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
    use super::{
        HudiWriteResult, has_sub_microsecond, is_unnamed_pandas_index, last_path_segment,
        normalize_batch, pascal_to_screaming_snake, writable_field, writable_type,
    };
    use arrow::array::{
        Array, ArrayRef, DictionaryArray, Int8Array, Int64Array, LargeListArray, LargeStringArray,
        ListArray, StringArray, StructArray, TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use hudi::WriteResult;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn writable_field_rewrites_nested_types_and_keeps_names_and_metadata() {
        let metadata = HashMap::from([("k".to_string(), "v".to_string())]);
        let item =
            Arc::new(Field::new("item", DataType::Utf8View, true).with_metadata(metadata.clone()));
        let field = Arc::new(
            Field::new(
                "f",
                DataType::Struct(Fields::from(vec![
                    Field::new("tags", DataType::LargeList(item), true),
                    Field::new(
                        "kind",
                        DataType::Dictionary(
                            Box::new(DataType::Int8),
                            Box::new(DataType::LargeUtf8),
                        ),
                        true,
                    ),
                    Field::new("n", DataType::Int64, true),
                ])),
                false,
            )
            .with_metadata(metadata.clone()),
        );

        let written = writable_field(&field).unwrap();
        assert_eq!(written.name(), "f");
        assert!(!written.is_nullable());
        assert_eq!(written.metadata(), &metadata);
        let DataType::Struct(children) = written.data_type() else {
            panic!("expected struct, got {}", written.data_type());
        };
        let DataType::List(item) = children[0].data_type() else {
            panic!("expected list, got {}", children[0].data_type());
        };
        assert_eq!(item.name(), "item");
        assert_eq!(item.data_type(), &DataType::Utf8);
        assert_eq!(item.metadata(), &metadata);
        assert_eq!(children[1].data_type(), &DataType::Utf8);
        assert_eq!(children[2].data_type(), &DataType::Int64);

        let plain = Arc::new(Field::new(
            "n",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ));
        assert!(writable_field(&plain).is_none());
    }

    #[test]
    fn unnamed_pandas_index_detection() {
        assert!(is_unnamed_pandas_index("__index_level_0__"));
        assert!(is_unnamed_pandas_index("__index_level_12__"));
        assert!(!is_unnamed_pandas_index("__index_level___"));
        assert!(!is_unnamed_pandas_index("__index_level_x__"));
        assert!(!is_unnamed_pandas_index("id"));
    }

    #[test]
    fn writable_type_maps_leaf_types() {
        let utc: Option<Arc<str>> = Some("UTC".into());
        for (from, to) in [
            (DataType::LargeBinary, Some(DataType::Binary)),
            (DataType::BinaryView, Some(DataType::Binary)),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, utc.clone()),
                Some(DataType::Timestamp(TimeUnit::Microsecond, utc.clone())),
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int64)),
                Some(DataType::Int64),
            ),
            (DataType::Timestamp(TimeUnit::Microsecond, utc), None),
            (DataType::Binary, None),
            (DataType::Int64, None),
            (
                DataType::Struct(Fields::from(vec![Field::new("n", DataType::Int64, true)])),
                None,
            ),
        ] {
            assert_eq!(writable_type(&from), to, "for {from}");
        }

        // A large list narrows to a list even when its item is already writable.
        let item = Arc::new(Field::new("element", DataType::Int64, false));
        assert_eq!(
            writable_type(&DataType::LargeList(item.clone())),
            Some(DataType::List(item))
        );
    }

    fn nanos(values: Vec<Option<i64>>) -> TimestampNanosecondArray {
        TimestampNanosecondArray::from(values)
    }

    fn list_of(values: TimestampNanosecondArray) -> ArrayRef {
        let field = Arc::new(Field::new("item", values.data_type().clone(), true));
        let offsets = OffsetBuffer::from_lengths([values.len()]);
        Arc::new(ListArray::new(field, offsets, Arc::new(values), None))
    }

    fn large_list_of(values: TimestampNanosecondArray) -> ArrayRef {
        let field = Arc::new(Field::new("item", values.data_type().clone(), true));
        let offsets = OffsetBuffer::from_lengths([values.len()]);
        Arc::new(LargeListArray::new(field, offsets, Arc::new(values), None))
    }

    fn struct_of(values: TimestampNanosecondArray) -> ArrayRef {
        let field = Arc::new(Field::new("ts", values.data_type().clone(), true));
        Arc::new(StructArray::from(vec![(
            field,
            Arc::new(values) as ArrayRef,
        )]))
    }

    fn dictionary_of(values: TimestampNanosecondArray) -> ArrayRef {
        let keys = Int8Array::from_iter_values(0..values.len() as i8);
        Arc::new(DictionaryArray::new(keys, Arc::new(values)))
    }

    #[test]
    fn has_sub_microsecond_checks_values_at_any_depth() {
        let whole = || nanos(vec![Some(2_000), None]);
        let partial = || nanos(vec![Some(2_000), Some(2_001)]);
        assert!(!has_sub_microsecond(&whole()));
        assert!(has_sub_microsecond(&partial()));
        for wrap in [list_of, large_list_of, struct_of, dictionary_of] {
            assert!(!has_sub_microsecond(wrap(whole()).as_ref()));
            assert!(has_sub_microsecond(wrap(partial()).as_ref()));
        }
        // Values outside a slice and unused dictionary values are ignored.
        let lists = ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )),
            OffsetBuffer::from_lengths([1, 1]),
            Arc::new(nanos(vec![Some(1), Some(2_000)])),
            None,
        );
        assert!(!has_sub_microsecond(&lists.slice(1, 1)));
        assert!(has_sub_microsecond(&lists.slice(0, 1)));
        let unused = DictionaryArray::new(
            Int8Array::from(vec![1]),
            Arc::new(nanos(vec![Some(1), Some(2_000)])),
        );
        assert!(!has_sub_microsecond(&unused));
        // Only nanosecond timestamps can hold sub-microsecond digits.
        assert!(!has_sub_microsecond(&TimestampMicrosecondArray::from(
            vec![1]
        )));
        assert!(!has_sub_microsecond(&Int64Array::from(vec![1])));
    }

    fn batch(columns: Vec<(&str, ArrayRef)>, pandas: bool) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, c)| Field::new(*name, c.data_type().clone(), true))
            .collect();
        let metadata = if pandas {
            HashMap::from([("pandas".to_string(), "{}".to_string())])
        } else {
            HashMap::new()
        };
        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
        RecordBatch::try_new(schema, columns.into_iter().map(|(_, c)| c).collect()).unwrap()
    }

    #[test]
    fn normalize_batch_returns_a_writable_batch_unchanged() {
        let input = batch(
            vec![
                ("id", Arc::new(StringArray::from(vec!["a"]))),
                ("__index_level_0__", Arc::new(Int64Array::from(vec![0]))),
            ],
            false,
        );
        let output = normalize_batch(input.clone()).unwrap();
        assert!(Arc::ptr_eq(&input.schema(), &output.schema()));
    }

    #[test]
    fn normalize_batch_drops_pandas_index_and_casts_columns() {
        let input = batch(
            vec![
                ("__index_level_0__", Arc::new(Int64Array::from(vec![5]))),
                ("id", Arc::new(LargeStringArray::from(vec!["a"]))),
                ("ts", list_of(nanos(vec![Some(3_000), None]))),
            ],
            true,
        );
        let output = normalize_batch(input.clone()).unwrap();
        assert_eq!(output.schema().metadata(), input.schema().metadata());
        let names: Vec<_> = output
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, ["id", "ts"]);
        assert_eq!(output.column(0).data_type(), &DataType::Utf8);
        let DataType::List(item) = output.column(1).data_type() else {
            panic!("expected list, got {}", output.column(1).data_type());
        };
        assert_eq!(
            item.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let values = output
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = values
            .values()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), [Some(3), None]);
    }

    #[test]
    fn normalize_batch_keeps_row_count_when_only_the_index_was_present() {
        let input = batch(
            vec![(
                "__index_level_0__",
                Arc::new(Int64Array::from(vec![0, 1, 2])),
            )],
            true,
        );
        let output = normalize_batch(input).unwrap();
        assert_eq!((output.num_columns(), output.num_rows()), (0, 3));
    }

    #[test]
    fn normalize_batch_rejects_sub_microsecond_timestamps() {
        for column in [
            Arc::new(nanos(vec![Some(1)])) as ArrayRef,
            struct_of(nanos(vec![Some(1_000_001)])),
        ] {
            let err = normalize_batch(batch(vec![("ts", column)], false)).unwrap_err();
            assert!(
                err.contains("column 'ts' has timestamps with sub-microsecond"),
                "{err}"
            );
        }
    }

    #[test]
    fn write_results_convert_to_python_counts() {
        let rewrite = HudiWriteResult::from(WriteResult {
            requested_time: Some("20240101000000000".to_string()),
            completion_time: None,
            num_writes: 4,
            num_updates: 1,
            num_inserts: 2,
            num_deletes: 3,
        });
        assert_eq!(rewrite.instant_time.as_deref(), Some("20240101000000000"));
        assert_eq!(
            (
                rewrite.num_writes,
                rewrite.num_updates,
                rewrite.num_inserts,
                rewrite.num_deletes
            ),
            (4, 1, 2, 3)
        );
    }

    #[test]
    fn last_path_segment_names_the_table_directory() {
        assert_eq!(last_path_segment("/data/trips"), "trips");
        assert_eq!(last_path_segment("file:///data/trips//"), "trips");
        assert_eq!(last_path_segment("s3://bucket/warehouse/trips"), "trips");
        assert_eq!(last_path_segment("trips"), "trips");
    }

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
