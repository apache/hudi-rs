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

use hudi::table::Table;
use pyo3::prelude::*;
use std::collections::HashMap;

#[pyclass]
struct BindingHudiTableMetaData {
    #[pyo3(get)]
    path: String,
    #[pyo3(get)]
    table_name: String,
    #[pyo3(get)]
    table_type: String,
    #[pyo3(get)]
    table_props: HashMap<String, Option<String>>,
}

#[pyclass]
struct BindingHudiTable {
    _table: hudi::HudiTable,
}

#[pymethods]
impl BindingHudiTable {
    #[new]
    #[pyo3(signature = (table_uri))]
    fn new(py: Python, table_uri: &str) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(BindingHudiTable {
                _table: Table::new(table_uri),
            })
        })
    }

    pub fn get_snapshot_file_paths(&self) -> PyResult<Vec<String>> {
        match self._table.get_snapshot_file_paths() {
            Ok(paths) => Ok(paths),
            Err(e) => {
                panic!("Failed to retrieve snapshot files.")
            }
        }
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

    m.add_class::<BindingHudiTableMetaData>()?;
    m.add_class::<BindingHudiTable>()?;
    Ok(())
}
