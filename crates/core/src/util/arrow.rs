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
use crate::error::CoreError;
use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::array::{AsArray, make_array};
use arrow_array::{Array, UInt32Array};
use arrow_buffer::NullBuffer;
use arrow_row::{RowConverter, SortField};
use arrow_schema::{ArrowError, DataType, FieldRef, Fields, SchemaRef};

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
    // Ties break on the original position, in both directions. The sort is
    // unstable, so without that the permutation over equal rows is whatever the
    // algorithm happened to do — and `desc` is not simply the reverse of `asc`,
    // since reversing would also reverse tied rows among themselves.
    if desc {
        sort.sort_unstable_by(|(ia, a), (ib, b)| b.cmp(a).then(ia.cmp(ib)));
    } else {
        sort.sort_unstable_by(|(ia, a), (ib, b)| a.cmp(b).then(ia.cmp(ib)));
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

/// The child index at each level from a top-level field down to the field at
/// `path`, a dotted path through struct fields (`meta.ts`) split the way Java
/// splits a nested ordering field (`HoodieUnsafeRowUtils.composeNestedFieldPath`).
/// A top-level field of that exact name wins, because the version 1 merger's
/// delete batch names its flat ordering column by the full path. `None` when
/// any segment is missing or a non-leaf segment is not a struct.
fn nested_field_indices(fields: &Fields, path: &str) -> Option<Vec<usize>> {
    if let Some((index, _)) = fields.find(path) {
        return Some(vec![index]);
    }
    let mut indices = Vec::new();
    let mut level = Some(fields);
    for segment in path.split('.') {
        let (index, field) = level?.find(segment)?;
        indices.push(index);
        level = match field.data_type() {
            DataType::Struct(children) => Some(children),
            _ => None,
        };
    }
    Some(indices)
}

/// The field at `path`, a top-level name or a dotted path through struct
/// fields, resolved as `nested_field_indices` describes.
pub fn nested_field<'a>(fields: &'a Fields, path: &str) -> Option<&'a FieldRef> {
    let indices = nested_field_indices(fields, path)?;
    let (first, rest) = indices.split_first()?;
    let mut field = &fields[*first];
    for &index in rest {
        let DataType::Struct(children) = field.data_type() else {
            return None;
        };
        field = &children[index];
    }
    Some(field)
}

/// The column at `path`, resolved as [`nested_field`] resolves it. A row whose
/// enclosing struct is null is null in the result, since a struct's children
/// carry no validity of their own for its null rows.
pub fn nested_column(batch: &RecordBatch, path: &str) -> Result<Option<ArrayRef>> {
    let Some(indices) = nested_field_indices(batch.schema_ref().fields(), path) else {
        return Ok(None);
    };
    let Some((first, rest)) = indices.split_first() else {
        return Ok(None);
    };
    let mut column = batch.column(*first).clone();
    let mut ancestor_nulls: Option<NullBuffer> = None;
    for &index in rest {
        let Some(parent) = column.as_struct_opt() else {
            return Ok(None);
        };
        ancestor_nulls = NullBuffer::union(ancestor_nulls.as_ref(), parent.nulls());
        column = parent.column(index).clone();
    }
    let Some(ancestor_nulls) = ancestor_nulls else {
        return Ok(Some(column));
    };
    let nulls = NullBuffer::union(Some(&ancestor_nulls), column.nulls());
    let data = column.to_data().into_builder().nulls(nulls).build()?;
    Ok(Some(make_array(data)))
}

/// Project a [`RecordBatch`] to a subset of columns by name. Returns the batch
/// unchanged when `projection` is `None`. Errors when any name is not present in
/// the batch's schema.
pub fn project_batch_by_names(
    batch: RecordBatch,
    projection: Option<&[String]>,
) -> Result<RecordBatch> {
    let Some(cols) = projection else {
        return Ok(batch);
    };
    let indices: Vec<usize> = cols
        .iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map_err(|e| CoreError::Schema(format!("Projection column not found: {e:?}")))
        })
        .collect::<Result<_>>()?;
    batch.project(&indices).map_err(CoreError::ArrowError)
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

    #[test]
    fn test_project_batch_by_names() {
        use arrow_schema::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.5, 2.5])) as ArrayRef,
            ],
        )
        .unwrap();

        // None returns input unchanged.
        let same = project_batch_by_names(batch.clone(), None).unwrap();
        assert_eq!(same.num_columns(), 3);
        assert_eq!(same.schema().field(0).name(), "a");

        // Subset projects in the requested order.
        let cols = vec!["c".to_string(), "a".to_string()];
        let projected = project_batch_by_names(batch.clone(), Some(&cols)).unwrap();
        assert_eq!(projected.num_columns(), 2);
        assert_eq!(projected.schema().field(0).name(), "c");
        assert_eq!(projected.schema().field(1).name(), "a");

        // Unknown column errors with a Schema error.
        let bad = vec!["a".to_string(), "missing".to_string()];
        let err = project_batch_by_names(batch, Some(&bad)).unwrap_err();
        assert!(matches!(err, CoreError::Schema(_)));
        assert!(err.to_string().contains("missing"));
    }

    /// A nested path resolves through two struct levels on a sliced batch,
    /// folding every ancestor's nulls into the leaf; a top-level column whose
    /// name contains the dot wins over the path.
    #[test]
    fn test_nested_column_resolves_path_with_ancestor_nulls() {
        use arrow_array::{Int64Array, StructArray};
        use arrow_schema::{Field, Schema};
        let ts = Arc::new(Field::new("ts", DataType::Int64, true));
        let inner = StructArray::try_new(
            vec![ts].into(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef],
            Some(NullBuffer::from(vec![true, true, false, true])),
        )
        .unwrap();
        let inner_field = Arc::new(Field::new("inner", inner.data_type().clone(), true));
        let outer = StructArray::try_new(
            vec![inner_field].into(),
            vec![Arc::new(inner) as ArrayRef],
            Some(NullBuffer::from(vec![true, false, true, true])),
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("outer", outer.data_type().clone(), true),
                Field::new("outer.inner", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(outer) as ArrayRef,
                Arc::new(StringArray::from(vec!["w", "x", "y", "z"])),
            ],
        )
        .unwrap();

        let sliced = batch.slice(1, 3);
        let leaf = nested_column(&sliced, "outer.inner.ts").unwrap().unwrap();
        let leaf = leaf.as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(leaf.iter().collect::<Vec<_>>(), vec![None, None, Some(4)]);
        assert_eq!(
            nested_field(batch.schema_ref().fields(), "outer.inner.ts").map(|f| f.name().as_str()),
            Some("ts")
        );

        let literal = nested_column(&batch, "outer.inner").unwrap().unwrap();
        assert_eq!(literal.data_type(), &DataType::Utf8);
        assert!(nested_column(&batch, "outer.absent").unwrap().is_none());
    }
}
