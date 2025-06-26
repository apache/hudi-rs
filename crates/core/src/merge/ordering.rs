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
use crate::config::table::HudiTableConfig;
use crate::config::HudiConfigs;
use crate::record::{
    extract_commit_time_ordering_values, extract_event_time_ordering_values, extract_record_keys,
};
use crate::Result;
use arrow_array::{
    Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use arrow_row::{OwnedRow, Row, RowConverter};
use arrow_schema::DataType;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MaxOrderingInfo {
    event_time_ordering: OwnedRow,
    commit_time_ordering: OwnedRow,
    is_event_time_zero: bool,
}

impl MaxOrderingInfo {
    pub fn is_greater_than(&self, event_time: Row, commit_time: Row) -> bool {
        if self.is_event_time_zero {
            self.commit_time_ordering.row() > commit_time
        } else {
            self.event_time_ordering.row() > event_time
                || (self.event_time_ordering.row() == event_time
                    && self.commit_time_ordering.row() > commit_time)
        }
    }
}

pub fn process_batch_for_max_orderings(
    batch: &RecordBatch,
    max_ordering: &mut HashMap<OwnedRow, MaxOrderingInfo>,
    key_converter: &RowConverter,
    event_time_converter: &RowConverter,
    commit_time_converter: &RowConverter,
    hudi_configs: Arc<HudiConfigs>,
) -> Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }

    let ordering_field = hudi_configs
        .get(HudiTableConfig::PrecombineField)?
        .to::<String>();

    let keys = extract_record_keys(key_converter, batch)?;
    let event_times =
        extract_event_time_ordering_values(event_time_converter, batch, &ordering_field)?;
    let commit_times = extract_commit_time_ordering_values(commit_time_converter, batch)?;
    for i in 0..batch.num_rows() {
        let key = keys.row(i).owned();
        let event_time = event_times.row(i).owned();
        let commit_time = commit_times.row(i).owned();
        let is_event_time_zero = is_event_time_zero(event_time.row(), event_time_converter)?;

        match max_ordering.get_mut(&key) {
            Some(info) => {
                if event_time > info.event_time_ordering {
                    info.event_time_ordering = event_time;
                    info.is_event_time_zero = is_event_time_zero;
                }
                if commit_time > info.commit_time_ordering {
                    info.commit_time_ordering = commit_time;
                }
            }
            None => {
                max_ordering.insert(
                    key,
                    MaxOrderingInfo {
                        event_time_ordering: event_time,
                        commit_time_ordering: commit_time,
                        is_event_time_zero,
                    },
                );
            }
        }
    }

    Ok(())
}

pub fn is_event_time_zero(
    event_time_row: Row,
    event_time_converter: &RowConverter,
) -> Result<bool> {
    let event_times = event_time_converter.convert_rows([event_time_row])?;
    assert_eq!(
        event_times.len(),
        1,
        "Expected exactly one row for event time conversion"
    );
    let event_time = &event_times[0];

    let is_zero = match event_time.data_type() {
        DataType::Int8 => {
            event_time
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0)
                == 0
        }
        DataType::Int16 => {
            event_time
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(0)
                == 0
        }
        DataType::Int32 => {
            event_time
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
                == 0
        }
        DataType::Int64 => {
            event_time
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
                == 0
        }
        DataType::UInt8 => {
            event_time
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(0)
                == 0
        }
        DataType::UInt16 => {
            event_time
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(0)
                == 0
        }
        DataType::UInt32 => {
            event_time
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(0)
                == 0
        }
        DataType::UInt64 => {
            event_time
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0)
                == 0
        }
        _ => false, // not an integer type
    };

    Ok(is_zero)
}
