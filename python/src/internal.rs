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
use std::collections::HashMap;
use std::convert::From;
use std::path::PathBuf;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

use hudi::error::CoreError;
use hudi::file_group::file_slice::FileSlice;
use hudi::file_group::reader::FileGroupReader;
use hudi::file_group::FileGroup;
use hudi::storage::error::StorageError;
use hudi::table::builder::TableBuilder;
use hudi::table::Table;
use hudi::timeline::instant::Instant;
use hudi::timeline::Timeline;
use hudi::util::StrTupleRef;
use pyo3::exceptions::PyException;
use pyo3::{create_exception, pyclass, pyfunction, pymethods, PyErr, PyObject, PyResult, Python};

create_exception!(_internal, HudiCoreError, PyException);

fn convert_to_py_err(err: CoreError) -> PyErr {
    // TODO(xushiyan): match and map all sub types
    HudiCoreError::new_err(err.to_string())
}

#[derive(thiserror::Error, Debug)]
pub enum PythonError {
    #[error("Error in Hudi core: {0}")]
    HudiCore(#[from] CoreError),
}

impl From<PythonError> for PyErr {
    fn from(err: PythonError) -> PyErr {
        match err {
            PythonError::HudiCore(err) => convert_to_py_err(err),
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
    fn new(base_uri: &str, options: Option<HashMap<String, String>>) -> PyResult<Self> {
        let inner = FileGroupReader::new_with_options(base_uri, options.unwrap_or_default())
            .map_err(PythonError::from)?;
        Ok(HudiFileGroupReader { inner })
    }

    fn read_file_slice_by_base_file_path(
        &self,
        relative_path: &str,
        py: Python,
    ) -> PyResult<PyObject> {
        rt().block_on(self.inner.read_file_slice_by_base_file_path(relative_path))
            .map_err(PythonError::from)?
            .to_pyarrow(py)
    }
    fn read_file_slice(&self, file_slice: &HudiFileSlice, py: Python) -> PyResult<PyObject> {
        let mut file_group = FileGroup::new(
            file_slice.file_id.clone(),
            file_slice.partition_path.clone(),
        );
        file_group
            .add_base_file_from_name(&file_slice.base_file_name)
            .map_err(PythonError::from)?;
        for name in file_slice.log_file_names.iter() {
            file_group
                .add_log_file_from_name(name)
                .map_err(PythonError::from)?;
        }
        let (_, inner_file_slice) = file_group
            .file_slices
            .iter()
            .next()
            .ok_or_else(|| {
                CoreError::FileGroup(format!(
                    "Failed to get file slice from file group: {:?}",
                    file_group
                ))
            })
            .map_err(PythonError::from)?;
        rt().block_on(self.inner.read_file_slice(inner_file_slice))
            .map_err(PythonError::from)?
            .to_pyarrow(py)
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
    base_file_size: usize,
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
                    "Failed to get base file relative path for file slice: {:?}",
                    self
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
                        "Failed to get log file relative path for file slice: {:?}",
                        self
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
        self.inner.timestamp.to_owned()
    }

    // TODO impl other properties
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
        base_uri: &str,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let inner: Table = rt()
            .block_on(Table::new_with_options(
                base_uri,
                options.unwrap_or_default(),
            ))
            .map_err(PythonError::from)?;
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

    fn get_avro_schema(&self, py: Python) -> PyResult<String> {
        py.allow_threads(|| {
            let avro_schema = rt()
                .block_on(self.inner.get_avro_schema())
                .map_err(PythonError::from)?;
            Ok(avro_schema)
        })
    }

    fn get_schema(&self, py: Python) -> PyResult<PyObject> {
        rt().block_on(self.inner.get_schema())
            .map_err(PythonError::from)?
            .to_pyarrow(py)
    }

    fn get_partition_schema(&self, py: Python) -> PyResult<PyObject> {
        rt().block_on(self.inner.get_partition_schema())
            .map_err(PythonError::from)?
            .to_pyarrow(py)
    }

    fn get_timeline(&self, py: Python) -> HudiTimeline {
        py.allow_threads(|| {
            let timeline = self.inner.get_timeline();
            HudiTimeline::from(timeline)
        })
    }

    #[pyo3(signature = (n, filters=None))]
    fn get_file_slices_splits(
        &self,
        n: usize,
        filters: Option<Vec<(String, String, String)>>,
        py: Python,
    ) -> PyResult<Vec<Vec<HudiFileSlice>>> {
        let filters = filters.unwrap_or_default();

        py.allow_threads(|| {
            let file_slices = rt()
                .block_on(self.inner.get_file_slices_splits(n, &filters.as_strs()))
                .map_err(PythonError::from)?;
            Ok(file_slices
                .iter()
                .map(|inner_vec| inner_vec.iter().map(HudiFileSlice::from).collect())
                .collect())
        })
    }

    #[pyo3(signature = (n, timestamp, filters=None))]
    fn get_file_slices_splits_as_of(
        &self,
        n: usize,
        timestamp: &str,
        filters: Option<Vec<(String, String, String)>>,
        py: Python,
    ) -> PyResult<Vec<Vec<HudiFileSlice>>> {
        let filters = filters.unwrap_or_default();

        py.allow_threads(|| {
            let file_slices = rt()
                .block_on(
                    self.inner
                        .get_file_slices_splits_as_of(n, timestamp, &filters.as_strs()),
                )
                .map_err(PythonError::from)?;
            Ok(file_slices
                .iter()
                .map(|inner_vec| inner_vec.iter().map(HudiFileSlice::from).collect())
                .collect())
        })
    }

    #[pyo3(signature = (filters=None))]
    fn get_file_slices(
        &self,
        filters: Option<Vec<(String, String, String)>>,
        py: Python,
    ) -> PyResult<Vec<HudiFileSlice>> {
        let filters = filters.unwrap_or_default();

        py.allow_threads(|| {
            let file_slices = rt()
                .block_on(self.inner.get_file_slices(&filters.as_strs()))
                .map_err(PythonError::from)?;
            Ok(file_slices.iter().map(HudiFileSlice::from).collect())
        })
    }

    #[pyo3(signature = (timestamp, filters=None))]
    fn get_file_slices_as_of(
        &self,
        timestamp: &str,
        filters: Option<Vec<(String, String, String)>>,
        py: Python,
    ) -> PyResult<Vec<HudiFileSlice>> {
        let filters = filters.unwrap_or_default();

        py.allow_threads(|| {
            let file_slices = rt()
                .block_on(
                    self.inner
                        .get_file_slices_as_of(timestamp, &filters.as_strs()),
                )
                .map_err(PythonError::from)?;
            Ok(file_slices.iter().map(HudiFileSlice::from).collect())
        })
    }

    #[pyo3(signature = (start_timestamp=None, end_timestamp=None))]
    fn get_file_slices_between(
        &self,
        start_timestamp: Option<&str>,
        end_timestamp: Option<&str>,
        py: Python,
    ) -> PyResult<Vec<HudiFileSlice>> {
        py.allow_threads(|| {
            let file_slices = rt()
                .block_on(
                    self.inner
                        .get_file_slices_between(start_timestamp, end_timestamp),
                )
                .map_err(PythonError::from)?;
            Ok(file_slices.iter().map(HudiFileSlice::from).collect())
        })
    }

    #[pyo3(signature = (options=None))]
    fn create_file_group_reader_with_options(
        &self,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<HudiFileGroupReader> {
        let fg_reader = self
            .inner
            .create_file_group_reader_with_options(options.unwrap_or_default())
            .map_err(PythonError::from)?;
        Ok(HudiFileGroupReader { inner: fg_reader })
    }

    #[pyo3(signature = (filters=None))]
    fn read_snapshot(
        &self,
        filters: Option<Vec<(String, String, String)>>,
        py: Python,
    ) -> PyResult<PyObject> {
        let filters = filters.unwrap_or_default();

        rt().block_on(self.inner.read_snapshot(&filters.as_strs()))
            .map_err(PythonError::from)?
            .to_pyarrow(py)
    }

    #[pyo3(signature = (timestamp, filters=None))]
    fn read_snapshot_as_of(
        &self,
        timestamp: &str,
        filters: Option<Vec<(String, String, String)>>,
        py: Python,
    ) -> PyResult<PyObject> {
        let filters = filters.unwrap_or_default();

        rt().block_on(
            self.inner
                .read_snapshot_as_of(timestamp, &filters.as_strs()),
        )
        .map_err(PythonError::from)?
        .to_pyarrow(py)
    }

    #[pyo3(signature = (start_timestamp, end_timestamp=None))]
    fn read_incremental_records(
        &self,
        start_timestamp: &str,
        end_timestamp: Option<&str>,
        py: Python,
    ) -> PyResult<PyObject> {
        rt().block_on(
            self.inner
                .read_incremental_records(start_timestamp, end_timestamp),
        )
        .map_err(PythonError::from)?
        .to_pyarrow(py)
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
        py.allow_threads(|| {
            let instants = rt()
                .block_on(self.inner.get_completed_commits(desc))
                .map_err(PythonError::from)?;
            Ok(instants.iter().map(HudiInstant::from).collect())
        })
    }

    #[pyo3(signature = (desc=false))]
    pub fn get_completed_deltacommits(&self, desc: bool, py: Python) -> PyResult<Vec<HudiInstant>> {
        py.allow_threads(|| {
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
        py.allow_threads(|| {
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
        py.allow_threads(|| {
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
        py.allow_threads(|| {
            let commit_metadata = rt()
                .block_on(self.inner.get_instant_metadata_in_json(&instant.inner))
                .map_err(PythonError::from)?;
            Ok(commit_metadata)
        })
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
    base_uri: String,
    hudi_options: Option<HashMap<String, String>>,
    storage_options: Option<HashMap<String, String>>,
    options: Option<HashMap<String, String>>,
) -> PyResult<HudiTable> {
    let inner = rt()
        .block_on(
            TableBuilder::from_base_uri(&base_uri)
                .with_hudi_options(hudi_options.unwrap_or_default())
                .with_storage_options(storage_options.unwrap_or_default())
                .with_options(options.unwrap_or_default())
                .build(),
        )
        .map_err(PythonError::from)?;
    Ok(HudiTable { inner })
}

#[cfg(not(tarpaulin_include))]
fn rt() -> &'static Runtime {
    static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();
    TOKIO_RT.get_or_init(|| Runtime::new().expect("Failed to create a tokio runtime."))
}
