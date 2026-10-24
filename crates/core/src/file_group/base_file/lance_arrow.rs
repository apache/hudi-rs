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
//! Carries Lance's record batches into the workspace's arrow through the
//! Arrow C data interface.
//!
//! Lance is compiled against a different arrow major than the rest of
//! hudi-core, and to rustc the two crates' `RecordBatch` types are unrelated,
//! so a batch cannot be handed across directly. The C data interface structs
//! (`ArrowArray`, `ArrowSchema`) are `#[repr(C)]` with a layout the Arrow
//! specification fixes for every implementation, so a struct one arrow fills
//! can be read by the other. The imported array wraps the exporter's buffers
//! (`from_ffi` copies only a buffer that misses arrow's alignment, which
//! Lance's decoders do not produce) and frees them through the exporter's
//! `release` callback when the host batch drops.

use std::mem::transmute;

use arrow::array::{RecordBatch, RecordBatchOptions, make_array};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow_array_lance::RecordBatch as LanceRecordBatch;
use arrow_array_lance::ffi::{FFI_ArrowArray as LanceFfiArray, to_ffi};
use arrow_schema_lance::Schema as LanceSchema;
use arrow_schema_lance::ffi::FFI_ArrowSchema as LanceFfiSchema;

use crate::storage::error::Result;

/// Converts a schema from Lance's arrow into the workspace's arrow.
///
/// Field names, nullability, nested types and metadata (including extension
/// type names) all travel in the C `ArrowSchema` struct.
pub(super) fn schema_to_host(schema: &LanceSchema) -> Result<Schema> {
    let exported = LanceFfiSchema::try_from(schema).map_err(export_error)?;
    let imported = cross_schema(exported);
    Ok(Schema::try_from(&imported)?)
}

/// Converts a batch from Lance's arrow into the workspace's arrow, sharing
/// the column buffers.
///
/// `schema` is the stream's schema, crossed once by [`schema_to_host`]; every
/// batch of the stream carries it, so the batch's own schema is not crossed
/// again. A column that does not match it fails the batch.
pub(super) fn batch_to_host(batch: LanceRecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        let (array, array_schema) = to_ffi(&column.to_data()).map_err(export_error)?;
        let array_schema = cross_schema(array_schema);
        let array = cross_array(array);
        // SAFETY: `array` and `array_schema` were produced together by `to_ffi`
        // from one valid array, and `cross_*` only relabeled their types.
        let data = unsafe { from_ffi(array, &array_schema)? };
        columns.push(make_array(data));
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        schema, columns, &options,
    )?)
}

/// Reads a C `ArrowSchema` struct filled by Lance's arrow as the workspace
/// arrow's struct of the same name.
fn cross_schema(schema: LanceFfiSchema) -> FFI_ArrowSchema {
    // SAFETY: both types are `#[repr(C)]` definitions of the `ArrowSchema`
    // struct from the Arrow C data interface specification, whose layout is
    // fixed, so the bytes of one are a valid value of the other. `transmute`
    // moves `schema` without running its `Drop`, so the `release` callback is
    // owned, and later invoked exactly once, by the returned struct.
    unsafe { transmute::<LanceFfiSchema, FFI_ArrowSchema>(schema) }
}

/// Reads a C `ArrowArray` struct filled by Lance's arrow as the workspace
/// arrow's struct of the same name.
fn cross_array(array: LanceFfiArray) -> FFI_ArrowArray {
    // SAFETY: as in `cross_schema`, for the `ArrowArray` struct.
    unsafe { transmute::<LanceFfiArray, FFI_ArrowArray>(array) }
}

/// Lance's arrow has its own `ArrowError` type; carry its message into ours.
fn export_error(e: arrow_schema_lance::ArrowError) -> ArrowError {
    ArrowError::CDataInterface(format!("Failed to export a Lance array: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{DataType, Field, Int32Type, TimeUnit};
    use arrow_array_lance::builder::{ListBuilder, StringBuilder, StructBuilder};
    use arrow_array_lance::types::Int32Type as LanceInt32Type;
    use arrow_array_lance::{
        ArrayRef as LanceArrayRef, DictionaryArray as LanceDictionaryArray,
        Int32Array as LanceInt32Array, StringArray as LanceStringArray,
        TimestampMicrosecondArray as LanceTimestampMicrosecondArray,
    };
    use arrow_schema_lance::{
        DataType as LanceDataType, Field as LanceField, Fields as LanceFields,
        TimeUnit as LanceTimeUnit,
    };

    /// Crosses the batch's own schema first, as the Lance reader does once per
    /// stream.
    fn to_host(batch: LanceRecordBatch) -> Result<RecordBatch> {
        let schema = Arc::new(schema_to_host(batch.schema().as_ref())?);
        batch_to_host(batch, schema)
    }

    /// A batch with the type shapes that take different paths through the C
    /// data interface: a primitive with nulls, a variable-width string, a
    /// timestamp with timezone, a dictionary, a list of structs, plus field
    /// and schema metadata.
    fn lance_batch() -> LanceRecordBatch {
        let id = LanceInt32Array::from(vec![Some(1), None, Some(3)]);
        let name = LanceStringArray::from(vec![Some("a"), Some("bb"), None]);
        let ts = LanceTimestampMicrosecondArray::from(vec![Some(1_000_000), Some(2_000_000), None])
            .with_timezone("UTC");
        let keys = LanceInt32Array::from(vec![Some(0), Some(1), Some(0)]);
        let values = LanceStringArray::from(vec!["x", "y"]);
        let dict = LanceDictionaryArray::<LanceInt32Type>::try_new(keys, Arc::new(values)).unwrap();

        let item_fields = LanceFields::from(vec![
            LanceField::new("k", LanceDataType::Utf8, true),
            LanceField::new("v", LanceDataType::Int32, true),
        ]);
        let mut items = ListBuilder::new(StructBuilder::from_fields(item_fields.clone(), 4));
        for row in 0..3 {
            for i in 0..row {
                let s = items.values();
                s.field_builder::<StringBuilder>(0)
                    .unwrap()
                    .append_value(format!("k{i}"));
                s.field_builder::<arrow_array_lance::builder::Int32Builder>(1)
                    .unwrap()
                    .append_value(i);
                s.append(true);
            }
            items.append(row != 1);
        }
        let items = items.finish();

        let mut field_meta = HashMap::new();
        field_meta.insert("ARROW:extension:name".to_string(), "hudi.test".to_string());
        let schema = Arc::new(LanceSchema::new_with_metadata(
            vec![
                LanceField::new("id", LanceDataType::Int32, true).with_metadata(field_meta),
                LanceField::new("name", LanceDataType::Utf8, true),
                LanceField::new(
                    "ts",
                    LanceDataType::Timestamp(LanceTimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                LanceField::new(
                    "dict",
                    LanceDataType::Dictionary(
                        Box::new(LanceDataType::Int32),
                        Box::new(LanceDataType::Utf8),
                    ),
                    true,
                ),
                LanceField::new(
                    "items",
                    LanceDataType::List(Arc::new(LanceField::new(
                        "item",
                        LanceDataType::Struct(item_fields),
                        true,
                    ))),
                    true,
                ),
            ],
            HashMap::from([("table".to_string(), "t1".to_string())]),
        ));
        let columns: Vec<LanceArrayRef> = vec![
            Arc::new(id),
            Arc::new(name),
            Arc::new(ts),
            Arc::new(dict),
            Arc::new(items),
        ];
        LanceRecordBatch::try_new(schema, columns).unwrap()
    }

    #[test]
    fn test_schema_to_host_preserves_fields_types_and_metadata() {
        let batch = lance_batch();
        let schema = schema_to_host(batch.schema().as_ref()).unwrap();

        assert_eq!(
            schema.metadata().get("table").map(String::as_str),
            Some("t1")
        );
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id", "name", "ts", "dict", "items"]);

        let id = schema.field(0);
        assert_eq!(id.data_type(), &DataType::Int32);
        assert!(id.is_nullable());
        assert_eq!(
            id.metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("hudi.test")
        );
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            schema.field(3).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        let DataType::List(item) = schema.field(4).data_type() else {
            panic!("items should be a list");
        };
        let DataType::Struct(item_fields) = item.data_type() else {
            panic!("list item should be a struct");
        };
        assert_eq!(item_fields.len(), 2);
        assert_eq!(item_fields[0].name(), "k");
        assert_eq!(item_fields[1].data_type(), &DataType::Int32);
    }

    #[test]
    fn test_batch_to_host_preserves_values_and_nulls() {
        let host = to_host(lance_batch()).unwrap();
        assert_eq!(host.num_rows(), 3);
        assert_eq!(host.num_columns(), 5);
        assert_eq!(host.schema().field(0).name(), "id");

        let id = host.column(0).as_primitive::<Int32Type>();
        assert_eq!(id.values(), &[1, 0, 3][..]);
        assert!(id.is_null(1));

        let name = host.column(1).as_string::<i32>();
        assert_eq!(name.value(0), "a");
        assert_eq!(name.value(1), "bb");
        assert!(name.is_null(2));

        let ts = host
            .column(2)
            .as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
        assert_eq!(ts.value(1), 2_000_000);
        assert!(ts.is_null(2));

        let dict = host.column(3).as_dictionary::<Int32Type>();
        let dict_values = dict.values().as_string::<i32>();
        assert_eq!(dict.keys().values(), &[0, 1, 0][..]);
        assert_eq!(dict_values.value(1), "y");

        let items = host.column(4).as_list::<i32>();
        assert_eq!(items.value_length(0), 0);
        assert!(items.is_null(1));
        assert_eq!(items.value_length(2), 2);
        // Row 2 holds the items (k0, 0) and (k1, 1).
        let last = items.value(2);
        let structs = last.as_struct();
        assert_eq!(structs.column(0).as_string::<i32>().value(1), "k1");
        assert_eq!(structs.column(1).as_primitive::<Int32Type>().value(1), 1);
    }

    #[test]
    fn test_batch_to_host_carries_the_given_schema() {
        let batch = lance_batch();
        let schema = Arc::new(schema_to_host(batch.schema().as_ref()).unwrap());
        let host = batch_to_host(batch, schema.clone()).unwrap();
        assert!(
            Arc::ptr_eq(&host.schema(), &schema),
            "the batch must share the stream's schema rather than cross its own"
        );
    }

    #[test]
    fn test_batch_to_host_rejects_a_schema_the_columns_do_not_match() {
        let batch = lance_batch();
        let other = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        assert!(batch_to_host(batch, other).is_err());
    }

    #[test]
    fn test_batch_to_host_shares_column_buffers() {
        let batch = lance_batch();
        let source_ptr = batch.column(0).to_data().buffers()[0].as_ptr();
        let host = to_host(batch).unwrap();
        let host_ptr = host.column(0).to_data().buffers()[0].as_ptr();
        assert_eq!(
            source_ptr, host_ptr,
            "the crossing must not copy column data"
        );
    }

    #[test]
    fn test_batch_to_host_keeps_buffers_alive_after_source_drops() {
        let host = to_host(lance_batch()).unwrap();
        // The source batch is gone; the imported arrays hold its buffers
        // through the exporter's `release` callback.
        let name = host.column(1).as_string::<i32>();
        assert_eq!(name.value(1), "bb");
        drop(host);
    }

    #[test]
    fn test_batch_to_host_zero_rows_and_zero_columns() {
        let one_field = Arc::new(LanceSchema::new(vec![LanceField::new(
            "id",
            LanceDataType::Int32,
            true,
        )]));
        let no_rows = LanceRecordBatch::try_new(
            one_field,
            vec![Arc::new(LanceInt32Array::from(Vec::<i32>::new()))],
        )
        .unwrap();
        let host = to_host(no_rows).unwrap();
        assert_eq!(host.num_rows(), 0);
        assert_eq!(host.num_columns(), 1);

        let no_columns = LanceRecordBatch::try_new_with_options(
            Arc::new(LanceSchema::empty()),
            vec![],
            &arrow_array_lance::RecordBatchOptions::new().with_row_count(Some(7)),
        )
        .unwrap();
        let host = to_host(no_columns).unwrap();
        assert_eq!(host.num_rows(), 7);
        assert_eq!(host.num_columns(), 0);
    }

    #[test]
    fn test_batch_to_host_handles_sliced_wide_and_fixed_width_types() {
        use arrow_array_lance::{
            Decimal128Array as LanceDecimal128Array, FixedSizeListArray as LanceFixedSizeListArray,
            LargeStringArray as LanceLargeStringArray, StructArray as LanceStructArray,
        };

        // A slice carries a non-zero offset through the C interface.
        let sliced = LanceInt32Array::from(vec![Some(1), None, Some(3), Some(4)]).slice(1, 3);
        let large = LanceLargeStringArray::from(vec![Some("x"), None, Some("zz")]);
        let decimal = LanceDecimal128Array::from(vec![Some(12345_i128), None, Some(-1)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let fixed = LanceFixedSizeListArray::try_new(
            Arc::new(LanceField::new("item", LanceDataType::Int32, true)),
            2,
            Arc::new(LanceInt32Array::from(vec![1, 2, 3, 4, 5, 6])),
            None,
        )
        .unwrap();
        let child_fields =
            LanceFields::from(vec![LanceField::new("n", LanceDataType::Int32, false)]);
        let strukt = LanceStructArray::try_new(
            child_fields.clone(),
            vec![Arc::new(LanceInt32Array::from(vec![7, 8, 9])) as LanceArrayRef],
            None,
        )
        .unwrap();
        let schema = Arc::new(LanceSchema::new(vec![
            LanceField::new("sliced", LanceDataType::Int32, true),
            LanceField::new("large", LanceDataType::LargeUtf8, true),
            LanceField::new("decimal", LanceDataType::Decimal128(10, 2), true),
            LanceField::new(
                "fixed",
                LanceDataType::FixedSizeList(
                    Arc::new(LanceField::new("item", LanceDataType::Int32, true)),
                    2,
                ),
                true,
            ),
            LanceField::new("strukt", LanceDataType::Struct(child_fields), true),
        ]));
        let batch = LanceRecordBatch::try_new(
            schema,
            vec![
                Arc::new(sliced),
                Arc::new(large),
                Arc::new(decimal),
                Arc::new(fixed),
                Arc::new(strukt),
            ],
        )
        .unwrap();

        let host = to_host(batch).unwrap();
        assert_eq!(host.num_rows(), 3);

        let sliced = host.column(0).as_primitive::<Int32Type>();
        assert!(sliced.is_null(0));
        assert_eq!(sliced.value(1), 3);
        assert_eq!(sliced.value(2), 4);

        let large = host.column(1).as_string::<i64>();
        assert_eq!(large.value(2), "zz");
        assert!(large.is_null(1));

        let decimal = host
            .column(2)
            .as_primitive::<arrow::datatypes::Decimal128Type>();
        assert_eq!(decimal.value(0), 12345);
        assert_eq!(
            host.schema().field(2).data_type(),
            &DataType::Decimal128(10, 2)
        );

        let fixed = host.column(3).as_fixed_size_list();
        assert_eq!(fixed.value_length(), 2);
        assert_eq!(
            fixed.value(2).as_primitive::<Int32Type>().values(),
            &[5, 6][..]
        );

        let strukt = host.column(4).as_struct();
        assert_eq!(strukt.column(0).as_primitive::<Int32Type>().value(2), 9);
        let host_schema = host.schema();
        let DataType::Struct(fields) = host_schema.field(4).data_type() else {
            panic!("strukt should be a struct");
        };
        assert!(!fields[0].is_nullable(), "child nullability must survive");
    }

    #[test]
    fn test_export_error_names_the_lance_side() {
        let err = export_error(arrow_schema_lance::ArrowError::CDataInterface(
            "bad".to_string(),
        ));
        let msg = err.to_string();
        assert!(msg.contains("Failed to export a Lance array"), "{msg}");
        assert!(msg.contains("bad"), "{msg}");
    }

    #[test]
    fn test_host_field_type_round_trips_through_the_workspace_arrow() {
        // A field built in the workspace arrow equals the crossed field, so the
        // two arrows agree on the type encoding the C interface carries.
        let host = to_host(lance_batch()).unwrap();
        let expected = Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        );
        assert_eq!(host.schema().field(2), &expected);
    }
}
