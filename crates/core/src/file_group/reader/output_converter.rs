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

//! Mirrors Java's `Option<UnaryOperator<T>> outputConverter` field
//! on `HoodieFileGroupReader`.
//!
//! The output converter applies a final transformation (typically schema
//! projection) to each record returned by the reader. In Java it is
//! obtained from `readerContext.getSchemaHandler().getOutputConverter()`.

use crate::Result;
use crate::error::CoreError;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

/// A converter that transforms output records (e.g., schema projection).
///
/// Mirrors Java's `UnaryOperator<T>` used as `outputConverter` in
/// `HoodieFileGroupReader`. Obtained from `FileGroupReaderSchemaHandler`.
pub trait OutputConverter: Send + Sync + std::fmt::Debug {
    /// Apply the conversion to a record batch.
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch>;
}

/// Projects a `RecordBatch` from `required_schema` down to `requested_schema`.
///
/// Mirrors Java's `readerContext.getRecordContext().projectRecord(requiredSchema, requestedSchema)`.
/// Selects only the columns present in the target schema, in the order they appear.
#[derive(Debug)]
pub struct ProjectionConverter {
    /// Column names to project to (the requested schema columns), in order.
    target_columns: Vec<String>,
}

impl ProjectionConverter {
    pub fn new(target_schema: &SchemaRef) -> Self {
        let target_columns = target_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        Self { target_columns }
    }
}

impl OutputConverter for ProjectionConverter {
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let indices: Vec<usize> = self
            .target_columns
            .iter()
            .map(|name| {
                batch.schema().index_of(name).map_err(|_| {
                    CoreError::ReadFileSliceError(format!(
                        "Output projection: column '{}' not found in batch schema. \
                         Available: [{}]",
                        name,
                        batch
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        batch
            .project(&indices)
            .map_err(|e| CoreError::ReadFileSliceError(format!("Output projection failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Test that ProjectionConverter correctly narrows a RecordBatch.
    #[test]
    fn test_output_converter_projects_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, true),
            Field::new("col_b", DataType::Int64, true),
            Field::new("col_c", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a1", "a2"])),
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["c1", "c2"])),
            ],
        )
        .unwrap();

        // Project to only col_a and col_c.
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, true),
            Field::new("col_c", DataType::Utf8, true),
        ]));
        let converter = ProjectionConverter::new(&target_schema);
        let projected = converter.apply(batch).unwrap();

        assert_eq!(projected.num_columns(), 2);
        assert_eq!(projected.schema().field(0).name(), "col_a");
        assert_eq!(projected.schema().field(1).name(), "col_c");
        assert_eq!(projected.num_rows(), 2);
    }

    /// Test that ProjectionConverter returns all columns when target matches source.
    #[test]
    fn test_output_converter_noop_when_same_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, true),
            Field::new("col_b", DataType::Int64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a1"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();

        let converter = ProjectionConverter::new(&schema);
        let projected = converter.apply(batch).unwrap();

        assert_eq!(projected.num_columns(), 2);
    }

    /// Test that ProjectionConverter errors on missing column.
    #[test]
    fn test_output_converter_error_on_missing_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("col_a", DataType::Utf8, true)]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a1"]))]).unwrap();

        let target_schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, true),
            Field::new("col_missing", DataType::Utf8, true),
        ]));
        let converter = ProjectionConverter::new(&target_schema);
        let result = converter.apply(batch);

        assert!(result.is_err());
    }
}
