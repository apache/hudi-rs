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

use crate::error::CoreError;
use crate::Result;
use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow_array::{Array, UInt32Array};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{ArrowError, SchemaRef};

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

pub fn create_row_converter<I, S>(schema: SchemaRef, column_names: I) -> Result<RowConverter>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let sort_fields: Result<Vec<_>> = column_names
        .into_iter()
        .map(|col| {
            let (_, field) = schema
                .column_with_name(col.as_ref())
                .ok_or_else(|| CoreError::Schema(format!("Column {} not found", col.as_ref())))?;
            Ok(SortField::new(field.data_type().clone()))
        })
        .collect();
    RowConverter::new(sort_fields?).map_err(CoreError::ArrowError)
}

pub fn get_column_arrays<I, S>(batch: &RecordBatch, column_names: I) -> Result<Vec<ArrayRef>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    column_names
        .into_iter()
        .map(|col| {
            batch
                .column_by_name(col.as_ref())
                .cloned()
                .ok_or_else(|| CoreError::Schema(format!("Column {} not found", col.as_ref())))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow_array::Float64Array;
    use std::sync::Arc;

    #[test]
    fn test_basic_int_sort() {
        let arr = Int32Array::from(vec![3, 1, 4, 1, 5]);
        let arrays = vec![Arc::new(arr) as ArrayRef];

        // Test ascending
        let result = lexsort_to_indices(&arrays, false);
        assert_eq!(
            result.values(),
            &[1, 3, 0, 2, 4] // Indices that would sort to [1,1,3,4,5]
        );

        // Test descending
        let result = lexsort_to_indices(&arrays, true);
        assert_eq!(
            result.values(),
            &[4, 2, 0, 1, 3] // Indices that would sort to [5,4,3,1,1]
        );
    }

    #[test]
    fn test_multiple_columns() {
        let arr1 = Int32Array::from(vec![1, 1, 2, 2]);
        let arr2 = StringArray::from(vec!["b", "a", "b", "a"]);
        let arrays = vec![Arc::new(arr1) as ArrayRef, Arc::new(arr2) as ArrayRef];

        let result = lexsort_to_indices(&arrays, false);
        assert_eq!(
            result.values(),
            &[1, 0, 3, 2] // Should sort by first column then second
        );
    }

    #[test]
    fn test_edge_cases() {
        // Empty array
        assert_eq!(lexsort_to_indices(&[], false).len(), 0);

        // Array of empty array
        let arr = Int32Array::from(vec![] as Vec<i32>);
        let arrays = vec![Arc::new(arr) as ArrayRef];
        let result = lexsort_to_indices(&arrays, false);
        assert_eq!(result.len(), 0);

        // Single element
        let arr = Int32Array::from(vec![1]);
        let arrays = vec![Arc::new(arr) as ArrayRef];
        let result = lexsort_to_indices(&arrays, false);
        assert_eq!(result.values(), &[0]);

        // All equal values
        let arr = Int32Array::from(vec![5, 5, 5, 5]);
        let arrays = vec![Arc::new(arr) as ArrayRef];
        let result = lexsort_to_indices(&arrays, false);
        assert_eq!(result.values(), &[0, 1, 2, 3]);
    }

    #[test]
    fn test_different_types() {
        let int_arr = Int32Array::from(vec![1, 2, 1]);
        let str_arr = StringArray::from(vec!["a", "b", "c"]);
        let float_arr = Float64Array::from(vec![1.0, 2.0, 3.0]);

        let arrays = vec![
            Arc::new(int_arr) as ArrayRef,
            Arc::new(str_arr) as ArrayRef,
            Arc::new(float_arr) as ArrayRef,
        ];

        let result = lexsort_to_indices(&arrays, false);
        assert_eq!(result.values(), &[0, 2, 1]);
    }
}
