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
use crate::config::HudiConfigs;
use crate::record::{extract_ordering_values, extract_record_keys};
use crate::Result;
use arrow_array::RecordBatch;
use arrow_row::{OwnedRow, Row, RowConverter};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MaxOrderingInfo {
    ordering_value: OwnedRow,
    source_is_delete: bool,
}

impl MaxOrderingInfo {
    pub fn ordering_value(&self) -> Row {
        self.ordering_value.row()
    }
}

pub fn process_batch_for_max_orderings(
    batch: &RecordBatch,
    max_ordering: &mut HashMap<OwnedRow, MaxOrderingInfo>,
    is_delete_batch: bool,
    key_converter: &RowConverter,
    ordering_converter: &RowConverter,
    hudi_configs: Arc<HudiConfigs>,
) -> Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    println!("processing batch for max orderings: {:?}", batch);

    let keys = extract_record_keys(key_converter, batch)?;
    let orderings = extract_ordering_values(ordering_converter, batch, hudi_configs.clone())?;
    for i in 0..batch.num_rows() {
        let key = keys.row(i).owned();
        let ordering = orderings.row(i).owned();

        match max_ordering.get_mut(&key) {
            Some(info) => {
                if ordering > info.ordering_value {
                    info.ordering_value = ordering;
                    info.source_is_delete = is_delete_batch;
                }
            }
            None => {
                max_ordering.insert(
                    key,
                    MaxOrderingInfo {
                        ordering_value: ordering,
                        source_is_delete: is_delete_batch,
                    },
                );
            }
        }
    }

    Ok(())
}
