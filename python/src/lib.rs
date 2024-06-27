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
use std::path::PathBuf;

use arrow::pyarrow::ToPyArrow;
use pyo3::prelude::*;

use hudi::file_group::FileSlice;
use hudi::HudiTable;

#[pyclass]
struct HudiFileSlice {
    #[pyo3(get)]
    file_group_id: String,
    #[pyo3(get)]
    partition_path: String,
    #[pyo3(get)]
    commit_time: String,
    #[pyo3(get)]
    base_file_name: String,
    #[pyo3(get)]
    base_file_path: String,
    #[pyo3(get)]
    base_file_size: usize,
    #[pyo3(get)]
    num_records: i64,
}

impl HudiFileSlice {
    pub fn from_file_slice(f: FileSlice) -> Self {
        let partition_path = f.partition_path.clone().unwrap_or("".to_string());
        let mut p = PathBuf::from(&partition_path);
        p.push(f.base_file.info.name.clone());
        let base_file_path = p.to_str().unwrap().to_string();
        Self {
            file_group_id: f.file_group_id().to_string(),
            partition_path,
            commit_time: f.base_file.commit_time,
            base_file_name: f.base_file.info.name,
            base_file_path,
            base_file_size: f.base_file.info.size,
            num_records: f.base_file.stats.unwrap().num_records,
        }
    }
}

#[pyclass]
struct BindingHudiTable {
    _table: HudiTable,
}

#[pymethods]
impl BindingHudiTable {
    #[new]
    #[pyo3(signature = (table_uri, storage_options = None))]
    fn new(
        py: Python,
        table_uri: &str,
        storage_options: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(BindingHudiTable {
                _table: HudiTable::new(table_uri, storage_options.unwrap_or_default()),
            })
        })
    }

    pub fn schema(&self, py: Python) -> PyResult<PyObject> {
        self._table.get_latest_schema().to_pyarrow(py)
    }

    pub fn get_latest_file_slices(&mut self) -> PyResult<Vec<HudiFileSlice>> {
        match self._table.get_latest_file_slices() {
            Ok(file_slices) => Ok(file_slices
                .into_iter()
                .map(HudiFileSlice::from_file_slice)
                .collect()),
            Err(_e) => {
                panic!("Failed to retrieve the latest file slices.")
            }
        }
    }

    pub fn read_file_slice(&mut self, relative_path: &str, py: Python) -> PyResult<PyObject> {
        self._table.read_file_slice(relative_path).to_pyarrow(py)
    }
}

#[pyfunction]
fn rust_core_version() -> &'static str {
    hudi::crate_version()
}

#[pymodule]
fn _internal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(rust_core_version, m)?)?;

    m.add_class::<HudiFileSlice>()?;
    m.add_class::<BindingHudiTable>()?;
    Ok(())
}
