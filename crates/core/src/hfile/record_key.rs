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

//! Supplying a decoded record's key from the HFile entry it came from.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};

use crate::hfile::error::{HFileError, Result};

/// The field a writer may leave empty because the HFile entry key already holds
/// the record key.
///
/// COUPLING, stated because it is one: this name is the metadata table's schema,
/// not a general Hudi concept. Upstream reads the same literal in
/// `HoodieAvroHFileReaderImplBase` and carries its own note to break the
/// coupling; mirroring it keeps the two readers agreeing on which records have a
/// usable key, at the cost of inheriting that wart.
pub(crate) const ENTRY_KEY_FIELD: &str = "key";

/// Fill an empty [`ENTRY_KEY_FIELD`] from the entry key of the record that row
/// was decoded from.
///
/// A populated value is left alone: the writer meant it, and an entry key that
/// disagreed with it would be a different problem than this one. A batch whose
/// schema has no such field is returned untouched.
///
/// `entry_keys` is positional, so it must be the keys of the records that
/// produced `batch`, in the order they were decoded.
pub(crate) fn fill_empty_entry_keys(
    batch: RecordBatch,
    entry_keys: &[&str],
) -> Result<RecordBatch> {
    let Ok(index) = batch.schema().index_of(ENTRY_KEY_FIELD) else {
        return Ok(batch);
    };
    if batch.num_rows() != entry_keys.len() {
        return Err(HFileError::InvalidFormat(format!(
            "{} rows decoded from {} HFile records; the key of each row cannot be identified",
            batch.num_rows(),
            entry_keys.len()
        )));
    }

    let column = batch.column(index);
    let Some(existing) = column.as_any().downcast_ref::<StringArray>() else {
        // Not a string column, so it is not the key field this convention means.
        return Ok(batch);
    };

    let mut filled: Vec<Option<String>> = Vec::with_capacity(existing.len());
    let mut any_change = false;
    for (row, key) in entry_keys.iter().enumerate() {
        if existing.is_null(row) || !existing.value(row).is_empty() {
            filled.push((!existing.is_null(row)).then(|| existing.value(row).to_string()));
        } else {
            filled.push(Some((*key).to_string()));
            any_change = true;
        }
    }
    if !any_change {
        return Ok(batch);
    }

    let replacement: ArrayRef = Arc::new(StringArray::from(filled));
    let mut columns = batch.columns().to_vec();
    columns[index] = replacement;
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|e| HFileError::InvalidFormat(format!("Failed to rebuild the key column: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    fn batch(keys: Vec<Option<&str>>, other: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(ENTRY_KEY_FIELD, DataType::Utf8, true),
            Field::new("n", DataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(arrow::array::Int32Array::from(other)),
            ],
        )
        .unwrap()
    }

    fn key_col(b: &RecordBatch) -> Vec<Option<String>> {
        b.column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    #[test]
    fn an_empty_key_is_filled_and_a_populated_one_is_not() {
        let b = batch(vec![Some(""), Some("written"), Some("")], vec![1, 2, 3]);
        let out = fill_empty_entry_keys(b, &["e0", "e1", "e2"]).unwrap();
        assert_eq!(
            key_col(&out),
            vec![
                Some("e0".to_string()),
                Some("written".to_string()),
                Some("e2".to_string())
            ],
            "only the empty rows take the entry key"
        );
    }

    #[test]
    fn a_null_key_is_left_alone() {
        // Null is not empty: the writer said nothing rather than saying "".
        let b = batch(vec![None, Some("")], vec![1, 2]);
        let out = fill_empty_entry_keys(b, &["e0", "e1"]).unwrap();
        assert_eq!(key_col(&out), vec![None, Some("e1".to_string())]);
    }

    #[test]
    fn a_batch_without_the_field_is_untouched() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let out = fill_empty_entry_keys(b, &["e0", "e1"]).unwrap();
        assert_eq!(out.num_columns(), 1);
    }

    /// The fill is positional, so a length disagreement means the mapping from
    /// row to entry key is unknown. Guessing it would assign keys to the wrong
    /// records, so it is refused.
    #[test]
    fn a_row_count_mismatch_is_refused() {
        let b = batch(vec![Some(""), Some("")], vec![1, 2]);
        let err = fill_empty_entry_keys(b, &["only-one"]).unwrap_err();
        assert!(
            err.to_string().contains("cannot be identified"),
            "expected the mismatch to be named, got: {err}"
        );
    }
}
