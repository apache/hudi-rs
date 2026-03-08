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

use hudi_test::{QuickstartTripsTable, SampleTable};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::internal::rt;

#[pyfunction]
#[pyo3(signature = (table_name, table_type))]
pub fn get_test_table_path(table_name: &str, table_type: &str) -> PyResult<String> {
    if let Ok(table) = table_name.parse::<SampleTable>() {
        return match table_type {
            "cow" => Ok(table.path_to_cow()),
            "mor_avro" => Ok(table.path_to_mor_avro()),
            "mor_parquet" => Ok(table.path_to_mor_parquet()),
            _ => Err(PyValueError::new_err(format!(
                "Unknown table type: {table_type}. Use 'cow', 'mor_avro', or 'mor_parquet'"
            ))),
        };
    }

    if let Ok(table) = table_name.parse::<QuickstartTripsTable>() {
        return match table_type {
            "mor_avro" => Ok(table.path_to_mor_avro()),
            _ => Err(PyValueError::new_err(format!(
                "QuickstartTripsTable only supports 'mor_avro'. Got: {table_type}"
            ))),
        };
    }

    Err(PyValueError::new_err(format!(
        "Unknown test table: {table_name}"
    )))
}

#[pyfunction]
#[pyo3(signature = (table_name, cow, partitioned))]
pub fn verify_v9_txns_table(table_name: &str, cow: bool, partitioned: bool) -> PyResult<()> {
    let table: SampleTable = table_name
        .parse()
        .map_err(|_| PyValueError::new_err(format!("Unknown sample table: {table_name}")))?;
    rt().block_on(hudi_test::v9_verification::verify_v9_txns_table(
        &table,
        cow,
        partitioned,
    ));
    Ok(())
}
