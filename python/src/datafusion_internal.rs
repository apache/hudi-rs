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

use crate::internal::{rt, PythonError};
use hudi::HudiDataSource as InternalDataFusionHudiDataSource;
use pyo3::{pyclass, pymethods, PyErr, PyResult, Python};
use pyo3::{types::PyCapsule, Bound};

#[cfg(not(tarpaulin_include))]
#[pyclass(name = "HudiDataFusionDataSource")]
#[derive(Clone)]
pub struct HudiDataFusionDataSource {
    table: InternalDataFusionHudiDataSource,
}

#[cfg(not(tarpaulin_include))]
#[pymethods]
impl HudiDataFusionDataSource {
    #[new]
    #[pyo3(signature = (base_uri, options=None))]
    fn new(base_uri: &str, options: Option<Vec<(String, String)>>) -> PyResult<Self> {
        let inner: InternalDataFusionHudiDataSource = rt()
            .block_on(InternalDataFusionHudiDataSource::new_with_options(
                base_uri,
                options.unwrap_or_default(),
            ))
            .map_err(PythonError::from)?;
        Ok(HudiDataFusionDataSource { table: inner })
    }

    #[pyo3(signature = ())]
    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        use datafusion_ffi::table_provider::FFI_TableProvider;
        use std::ffi::CString;
        use std::sync::Arc;
        let capsule_name = CString::new("datafusion_table_provider").map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid capsule name: {}", e))
        })?;

        // Clone the inner data source and wrap it in an Arc
        let provider = Arc::new(self.table.clone());

        // Create the FFI wrapper
        let ffi_provider = FFI_TableProvider::new(provider, false, None);

        // Create and return the PyCapsule
        PyCapsule::new(py, ffi_provider, Some(capsule_name))
    }
}
