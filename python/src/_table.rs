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
use std::sync::OnceLock;

use arrow::pyarrow::ToPyArrow;
use pyo3::{pyclass, pymethods, PyObject, PyResult, Python};
use tokio::runtime::Runtime;

use hudi::table::Table;

use crate::_file_group::{convert_file_slice, HudiFileSlice};

#[cfg(not(tarpaulin))]
#[pyclass]
pub struct HudiTable {
    _table: Table,
}

#[cfg(not(tarpaulin))]
#[pymethods]
impl HudiTable {
    #[new]
    #[pyo3(signature = (table_uri, options = None))]
    fn new(table_uri: &str, options: Option<HashMap<String, String>>) -> PyResult<Self> {
        let _table = rt().block_on(Table::new_with_options(
            table_uri,
            options.unwrap_or_default(),
        ))?;
        Ok(HudiTable { _table })
    }

    pub fn get_schema(&self, py: Python) -> PyResult<PyObject> {
        rt().block_on(self._table.get_schema())?.to_pyarrow(py)
    }

    pub fn split_file_slices(&self, n: usize, py: Python) -> PyResult<Vec<Vec<HudiFileSlice>>> {
        py.allow_threads(|| {
            let file_slices = rt().block_on(self._table.split_file_slices(n))?;
            Ok(file_slices
                .iter()
                .map(|inner_vec| inner_vec.iter().map(convert_file_slice).collect())
                .collect())
        })
    }

    pub fn get_file_slices(&self, py: Python) -> PyResult<Vec<HudiFileSlice>> {
        py.allow_threads(|| {
            let file_slices = rt().block_on(self._table.get_file_slices())?;
            Ok(file_slices.iter().map(convert_file_slice).collect())
        })
    }

    pub fn read_file_slice(&self, relative_path: &str, py: Python) -> PyResult<PyObject> {
        rt().block_on(self._table.read_file_slice_by_path(relative_path))?
            .to_pyarrow(py)
    }

    pub fn read_snapshot(&self, py: Python) -> PyResult<PyObject> {
        rt().block_on(self._table.read_snapshot())?.to_pyarrow(py)
    }
}

#[cfg(not(tarpaulin))]
pub fn rt() -> &'static Runtime {
    static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();
    TOKIO_RT.get_or_init(|| Runtime::new().expect("Failed to create a tokio runtime."))
}
