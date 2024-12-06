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

use std::collections::HashMap;
use std::convert::From;
use std::path::PathBuf;
use std::sync::OnceLock;

use arrow::pyarrow::ToPyArrow;
use tokio::runtime::Runtime;

use hudi::exprs::filter::Filter;
use hudi::error::CoreError;
use hudi::file_group::reader::FileGroupReader;
use hudi::file_group::FileSlice;
use hudi::storage::error::StorageError;
use hudi::table::builder::TableBuilder;
use hudi::table::Table;
use hudi::util::convert_vec_to_slice;
use hudi::util::vec_to_slice;
use pyo3::{create_exception, pyclass, pyfunction, pymethods, PyErr, PyObject, PyResult, Python};
use pyo3::exceptions::{PyException, PyValueError};

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

#[cfg(not(tarpaulin))]
#[derive(Clone, Debug)]
#[pyclass]
pub struct HudiFileGroupReader {
    inner: FileGroupReader,
}

#[cfg(not(tarpaulin))]
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
}

#[cfg(not(tarpaulin))]
#[derive(Clone, Debug)]
#[pyclass]
pub struct HudiFileSlice {
    #[pyo3(get)]
    file_group_id: String,
    #[pyo3(get)]
    partition_path: String,
    #[pyo3(get)]
    commit_time: String,
    #[pyo3(get)]
    base_file_name: String,
    #[pyo3(get)]
    base_file_size: usize,
    #[pyo3(get)]
    num_records: i64,
    #[pyo3(get)]
    size_bytes: i64,
}

#[cfg(not(tarpaulin))]
#[pymethods]
impl HudiFileSlice {
    fn base_file_relative_path(&self) -> PyResult<String> {
        let path = PathBuf::from(&self.partition_path)
            .join(&self.base_file_name)
            .to_str()
            .map(String::from)
            .ok_or(StorageError::InvalidPath(format!(
                "Failed to get base file relative path for file slice: {:?}",
                self
            )))
            .map_err(CoreError::from)
            .map_err(PythonError::from)?;
        Ok(path)
    }
}

#[cfg(not(tarpaulin))]
fn convert_file_slice(f: &FileSlice) -> HudiFileSlice {
    let file_group_id = f.file_group_id().to_string();
    let partition_path = f.partition_path.as_deref().unwrap_or_default().to_string();
    let commit_time = f.base_file.commit_time.to_string();
    let base_file_name = f.base_file.info.name.clone();
    let base_file_size = f.base_file.info.size;
    let stats = f.base_file.stats.clone().unwrap_or_default();
    let num_records = stats.num_records;
    let size_bytes = stats.size_bytes;
    HudiFileSlice {
        file_group_id,
        partition_path,
        commit_time,
        base_file_name,
        base_file_size,
        num_records,
        size_bytes,
    }
}

#[cfg(not(tarpaulin))]
#[pyclass]
pub struct HudiTable {
    inner: Table,
}

#[cfg(not(tarpaulin))]
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

    #[pyo3(signature = (n, filters=None))]
    fn get_file_slices_splits(
        &self,
        n: usize,
        filters: Option<Vec<(String, String, String)>>,
        py: Python,
    ) -> PyResult<Vec<Vec<HudiFileSlice>>> {
        let filters = convert_filters(filters)?;

        py.allow_threads(|| {
            let file_slices = rt()
                .block_on(
                    self.inner
                        .get_file_slices_splits(n, vec_to_slice!(filters.unwrap_or_default())),
                )
                .map_err(PythonError::from)?;
            Ok(file_slices
                .iter()
                .map(|inner_vec| inner_vec.iter().map(convert_file_slice).collect())
                .collect())
        })
    }

    #[pyo3(signature = (filters=None))]
    fn get_file_slices(
        &self,
        filters: Option<Vec<(String, String, String)>>,
        py: Python,
    ) -> PyResult<Vec<HudiFileSlice>> {
        let filters = convert_filters(filters)?;

        py.allow_threads(|| {
            let file_slices = rt()
                .block_on(
                    self.inner
                        .get_file_slices(vec_to_slice!(filters.unwrap_or_default())),
                )
                .map_err(PythonError::from)?;
            Ok(file_slices.iter().map(convert_file_slice).collect())
        })
    }

    fn create_file_group_reader(&self) -> PyResult<HudiFileGroupReader> {
        let fg_reader = self.inner.create_file_group_reader();
        Ok(HudiFileGroupReader { inner: fg_reader })
    }

    #[pyo3(signature = (filters=None))]
    fn read_snapshot(
        &self,
        filters: Option<Vec<(String, String, String)>>,
        py: Python,
    ) -> PyResult<PyObject> {
        let filters = convert_filters(filters)?;

        rt().block_on(
            self.inner
                .read_snapshot(vec_to_slice!(filters.unwrap_or_default())),
        )
        .map_err(PythonError::from)?
        .to_pyarrow(py)
    }
}

fn convert_filters(filters: Option<Vec<(String, String, String)>>) -> PyResult<Vec<Filter>> {
    filters
        .unwrap_or_default()
        .into_iter()
        .map(|(field, op, value)| {
            Filter::try_from((field.as_str(), op.as_str(), value.as_str())).map_err(|e| {
                PyValueError::new_err(format!(
                    "Invalid filter ({}, {}, {}): {}",
                    field, op, value, e
                ))
            })
        })
        .collect()
}

#[cfg(not(tarpaulin))]
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

#[cfg(not(tarpaulin))]
fn rt() -> &'static Runtime {
    static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();
    TOKIO_RT.get_or_init(|| Runtime::new().expect("Failed to create a tokio runtime."))
}
