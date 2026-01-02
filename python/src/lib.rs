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
use pyo3::prelude::*;

mod internal;

#[cfg(feature = "datafusion")]
mod datafusion_internal;

#[cfg(not(tarpaulin_include))]
#[pymodule]
fn _internal(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    use internal::{
        HudiFileGroupReader, HudiFileSlice, HudiInstant, HudiTable, HudiTimeline, PyHudiReadConfig,
        PyHudiTableConfig,
    };
    m.add_class::<HudiFileGroupReader>()?;
    m.add_class::<HudiFileSlice>()?;
    m.add_class::<HudiInstant>()?;
    m.add_class::<PyHudiReadConfig>()?;
    m.add_class::<HudiTable>()?;
    m.add_class::<PyHudiTableConfig>()?;
    m.add_class::<HudiTimeline>()?;

    let table_config_type = m.getattr("HudiTableConfig")?;
    for (name, instance) in PyHudiTableConfig::get_class_attributes() {
        table_config_type.setattr(name, instance)?;
    }

    let read_config_type = m.getattr("HudiReadConfig")?;
    for (name, instance) in PyHudiReadConfig::get_class_attributes() {
        read_config_type.setattr(name, instance)?;
    }

    #[cfg(feature = "datafusion")]
    {
        use datafusion_internal::HudiDataFusionDataSource;
        m.add_class::<HudiDataFusionDataSource>()?;
    }

    use internal::build_hudi_table;
    m.add_function(wrap_pyfunction!(build_hudi_table, m)?)?;
    Ok(())
}
