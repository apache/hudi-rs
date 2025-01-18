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

use crate::Result;
use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow_array::{Array, UInt32Array};
use arrow_row::{RowConverter, SortField};
use arrow_schema::ArrowError;

pub trait ColumnAsArray {
    fn get_array(&self, column_name: &str) -> Result<ArrayRef>;

    fn get_string_array(&self, column_name: &str) -> Result<StringArray>;
}

impl ColumnAsArray for RecordBatch {
    fn get_array(&self, column_name: &str) -> Result<ArrayRef> {
        let index = self.schema().index_of(column_name)?;
        let array = self.column(index);
        Ok(array.clone())
    }

    fn get_string_array(&self, column_name: &str) -> Result<StringArray> {
        let array = self.get_array(column_name)?;
        let array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ArrowError::CastError(format!(
                    "Column {column_name} cannot be cast to StringArray."
                ))
            })?;
        Ok(array.clone())
    }
}

pub fn lexsort_to_indices(arrays: &[ArrayRef], desc: bool) -> UInt32Array {
    let fields = arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields).unwrap();
    let rows = converter.convert_columns(arrays).unwrap();
    let mut sort: Vec<_> = rows.iter().enumerate().collect();
    if desc {
        sort.sort_unstable_by(|(_, a), (_, b)| b.cmp(a));
    } else {
        sort.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));
    }
    UInt32Array::from_iter_values(sort.iter().map(|(i, _)| *i as u32))
}
