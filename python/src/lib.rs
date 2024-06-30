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
use std::sync::OnceLock;

use arrow::pyarrow::ToPyArrow;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

use hudi::file_group::FileSlice;
use hudi::HudiTable;

#[cfg(not(tarpaulin))]
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

#[cfg(not(tarpaulin))]
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

#[cfg(not(tarpaulin))]
#[pyclass]
struct BindingHudiTable {
    _table: HudiTable,
}

#[cfg(not(tarpaulin))]
#[pymethods]
impl BindingHudiTable {
    #[new]
    #[pyo3(signature = (table_uri, storage_options = None))]
    fn new(table_uri: &str, storage_options: Option<HashMap<String, String>>) -> PyResult<Self> {
        let _table = rt().block_on(HudiTable::new(
            table_uri,
            storage_options.unwrap_or_default(),
        ));
        Ok(BindingHudiTable { _table })
    }

    pub fn schema(&self, py: Python) -> PyResult<PyObject> {
        rt().block_on(self._table.get_latest_schema())
            .to_pyarrow(py)
    }

    pub fn get_latest_file_slices(&mut self, py: Python) -> PyResult<Vec<HudiFileSlice>> {
        py.allow_threads(|| {
            let res = rt().block_on(self._table.get_latest_file_slices());
            match res {
                Ok(file_slices) => Ok(file_slices
                    .into_iter()
                    .map(HudiFileSlice::from_file_slice)
                    .collect()),
                Err(_e) => {
                    panic!("Failed to retrieve the latest file slices.")
                }
            }
        })
    }

    pub fn read_file_slice(&mut self, relative_path: &str, py: Python) -> PyResult<PyObject> {
        rt().block_on(self._table.read_file_slice(relative_path))
            .to_pyarrow(py)
    }
}

#[cfg(not(tarpaulin))]
#[pyfunction]
fn rust_core_version() -> &'static str {
    hudi::crate_version()
}

#[cfg(not(tarpaulin))]
#[pymodule]
fn _internal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(rust_core_version, m)?)?;

    m.add_class::<HudiFileSlice>()?;
    m.add_class::<BindingHudiTable>()?;
    Ok(())
}

#[cfg(not(tarpaulin))]
pub fn rt() -> &'static Runtime {
    static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();
    TOKIO_RT.get_or_init(|| Runtime::new().expect("Failed to create a tokio runtime."))
}
