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

use std::sync::Arc;

use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::table_provider::FFI_TableProvider;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyCapsuleMethods};

use crate::internal::{PythonError, rt};
use hudi::HudiDataSource as InternalDataFusionHudiDataSource;

/// Extract the `FFI_LogicalExtensionCodec` from a DataFusion session object.
///
/// The session object is expected to have a `__datafusion_logical_extension_codec__`
/// method that returns a PyCapsule containing the codec.
fn extract_codec(session: Bound<PyAny>) -> PyResult<FFI_LogicalExtensionCodec> {
    let capsule_obj = if session.hasattr("__datafusion_logical_extension_codec__")? {
        session
            .getattr("__datafusion_logical_extension_codec__")?
            .call0()?
    } else {
        session
    };
    let capsule = capsule_obj.cast::<PyCapsule>()?;
    // `pointer_checked` is the name check and the pointer read in one step: it
    // refuses any capsule not carrying exactly this name, which is what makes
    // the cast below sound. An unnamed capsule no longer passes, where the
    // previous name-then-read pair let one through.
    let codec = capsule
        .pointer_checked(Some(c"datafusion_logical_extension_codec"))
        .map_err(|e| {
            PyValueError::new_err(format!(
                "Expected a PyCapsule named 'datafusion_logical_extension_codec': {e}"
            ))
        })?;
    // SAFETY: DataFusion puts that name only on a capsule holding an
    // `FFI_LogicalExtensionCodec`, and the check above just confirmed it.
    let codec = unsafe { codec.cast::<FFI_LogicalExtensionCodec>().as_ref() };
    Ok(codec.clone())
}

#[cfg(not(tarpaulin_include))]
#[pyclass(name = "HudiDataFusionDataSource", from_py_object)]
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

    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let provider = Arc::new(self.table.clone());
        let codec = extract_codec(session)?;
        let ffi_provider = FFI_TableProvider::new_with_ffi_codec(provider, false, None, codec);
        PyCapsule::new(py, ffi_provider, Some(cr"datafusion_table_provider".into()))
    }
}
