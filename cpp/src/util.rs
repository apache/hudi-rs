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
use crate::ffi;
use arrow::datatypes::SchemaRef;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, RecordBatchIterator};

pub fn create_raw_pointer_for_record_batches(
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
) -> *mut ffi::ArrowArrayStream {
    let batches = batches.into_iter().map(Ok);
    let batch_iterator = RecordBatchIterator::new(batches, schema);
    let ffi_array_stream = FFI_ArrowArrayStream::new(Box::new(batch_iterator));
    let raw_ptr = Box::into_raw(Box::new(ffi_array_stream));
    raw_ptr as *mut ffi::ArrowArrayStream
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::ffi_stream::ArrowArrayStreamReader;
    use arrow_array::{Int32Array, StringArray};
    use std::sync::Arc;

    /// The C++ caller only ever sees batches as a C Data Interface stream, so an
    /// arrow upgrade that changed how that stream is exported would break the
    /// binding without failing anything else: running the C++ side needs Arrow
    /// C++, which the Rust test suite does not have. Importing the exported
    /// pointer back keeps the surface covered here instead.
    #[test]
    fn exported_stream_round_trips_through_the_c_data_interface() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let raw = create_raw_pointer_for_record_batches(vec![batch.clone()], schema);
        // SAFETY: the pointer is the one box `create_raw_pointer_for_record_batches`
        // leaks, taken back here rather than by the C++ caller that normally frees it.
        let stream = unsafe { Box::from_raw(raw as *mut FFI_ArrowArrayStream) };

        let mut reader = ArrowArrayStreamReader::try_new(*stream).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), batch);
        assert!(reader.next().is_none());
    }
}
