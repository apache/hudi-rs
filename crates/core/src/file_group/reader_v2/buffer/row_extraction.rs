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

//! Ported from the merge-on-read reader. Nothing consumes it yet, so its
//! items are unreachable from the crate's call graph until the reader wires in.
#![allow(dead_code)]

//! Utility functions to bridge Arrow columnar data with per-record operations.
//!
//! Record-level operations (key extraction, ordering value extraction, batch-to-record
//! conversion) have moved to [`RecordContext`](crate::file_group::reader_v2::record_context::RecordContext).
//!
//! This module retains Arrow batch-level utilities that don't need RecordContext:
//! - `slice_row` — zero-copy single-row extraction
//! - `records_to_batch` — reassemble BufferedRecords into a RecordBatch
//! - `reconcile_batch_to_schema` — handle Avro/Parquet schema differences

use crate::Result;
use crate::error::CoreError;
use crate::file_group::reader_v2::buffered_record::{BufferedRecord, RecordPayload};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use std::collections::HashMap;
use std::sync::Arc;

/// Slice a single row from a RecordBatch as a new single-row RecordBatch.
///
/// This is zero-copy (just adjusts array offsets in Arrow).
pub fn slice_row(batch: &RecordBatch, row_index: usize) -> RecordBatch {
    batch.slice(row_index, 1)
}

/// Reassemble a collection of BufferedRecords back into a single RecordBatch
/// via the Arrow `interleave` kernel (A2 drain path).
///
/// Each surviving (non-delete) record addresses one row of a source batch
/// (`BatchRef`) or holds its own single-row batch (`Owned`). Rather than
/// slicing every row to a 1-row batch and concatenating (the old per-row path,
/// which for binary payloads paid an IPC decode per row), this groups the
/// records by distinct source batch, reconciles each distinct batch to the
/// target `schema` ONCE, and emits the output with a single
/// [`arrow::compute::interleave_record_batch`] call over `(slot, row)` indices.
/// Delete records contribute no output row.
///
/// Schema consistency (A2 risk #3): `interleave` requires all input batches share
/// a schema. Distinct source batches are reconciled to `schema` up front, so the
/// interleave inputs are uniform.
pub fn records_to_batch(records: Vec<BufferedRecord>, schema: SchemaRef) -> Result<RecordBatch> {
    // Distinct source batches (reconciled to `schema`) and the per-record index
    // into them. `BatchRef`s sharing one `Arc` map to a single reconciled slot
    // (grouped by `Arc::as_ptr`); each `Owned` record becomes its own slot.
    let mut sources: Vec<RecordBatch> = Vec::new();
    let mut slot_of_ptr: HashMap<*const RecordBatch, usize> = HashMap::new();
    let mut indices: Vec<(usize, usize)> = Vec::with_capacity(records.len());

    for record in records {
        match record.payload {
            RecordPayload::Delete => continue,
            RecordPayload::BatchRef { batch, row_idx } => {
                let ptr = Arc::as_ptr(&batch);
                let slot = match slot_of_ptr.get(&ptr) {
                    Some(&slot) => slot,
                    None => {
                        let reconciled = reconcile_to_schema(batch.as_ref(), &schema)?;
                        let slot = sources.len();
                        sources.push(reconciled);
                        slot_of_ptr.insert(ptr, slot);
                        slot
                    }
                };
                indices.push((slot, row_idx));
            }
            RecordPayload::Owned(batch) => {
                debug_assert_eq!(
                    batch.num_rows(),
                    1,
                    "Owned payload must be single-row; the drain emits only row 0",
                );
                let reconciled = reconcile_to_schema(&batch, &schema)?;
                let slot = sources.len();
                sources.push(reconciled);
                indices.push((slot, 0));
            }
        }
    }

    if indices.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let source_refs: Vec<&RecordBatch> = sources.iter().collect();
    arrow::compute::interleave_record_batch(&source_refs, &indices).map_err(|e| {
        CoreError::ReadFileSliceError(format!("Failed to interleave record batches: {e}"))
    })
}

/// Reconcile a source batch to the target schema if they differ (Avro vs Parquet
/// child field names — see [`reconcile_batch_to_schema`]); otherwise return it
/// unchanged.
fn reconcile_to_schema(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    if &batch.schema() == schema {
        Ok(batch.clone())
    } else {
        reconcile_batch_to_schema(batch, schema)
    }
}

/// Reconcile a RecordBatch to a target schema (name-metadata only).
///
/// After the schema-on-write evolution work (design D5), every batch reaching the
/// merge is already REQUIRED-SHAPED: it has exactly the target's columns with the
/// target's types. The only remaining legitimate job of this function is field-NAME
/// metadata reconciliation between Avro-derived schemas (from log files) and
/// Parquet-derived schemas (from base files). For example:
/// - List child field: arrow-avro uses "item", Parquet uses "element"
/// - Map entries field: arrow-avro uses "entries", Parquet uses "key_value"
///
/// Such differences change the [`DataType`](arrow_schema::DataType) *tag* (which
/// embeds child field names) but not the underlying buffers, so they are expressed
/// by rebuilding the column's `ArrayData` under the target `DataType` — the child
/// `ArrayData` is carried over unchanged.
///
/// Anything beyond that — a missing column, or a genuine type mismatch that the
/// ArrayData rebuild rejects — is an upstream evolution bug and is surfaced as a
/// loud error rather than silently papered over.
pub(crate) fn reconcile_batch_to_schema(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = target_schema
        .fields()
        .iter()
        .map(|target_field| -> Result<ArrayRef> {
            let idx = batch.schema().index_of(target_field.name()).map_err(|_| {
                CoreError::ReadFileSliceError(format!(
                    "reconcile: column '{}' missing from batch — upstream evolution bug \
                     (batches must be required-shaped before merge)",
                    target_field.name()
                ))
            })?;
            let source_col = batch.column(idx);
            if source_col.data_type() == target_field.data_type() {
                return Ok(source_col.clone());
            }
            // A base file written before a column was promoted holds the narrow
            // type, and the target is the table's current one. That is a real
            // value conversion, not a naming difference, so cast it the way the
            // evolution path does rather than reinterpreting the buffer.
            if crate::schema::batch_evolution::is_promotion(
                source_col.data_type(),
                target_field.data_type(),
            ) {
                return crate::schema::batch_evolution::evolve_array(source_col, target_field);
            }
            // ONLY name-metadata reconciliation (arrow-avro "item"/"entries" vs
            // parquet "element"/"key_value" child field names, plus avro.name/
            // avro.namespace metadata on nested fields): rebuild ArrayData under
            // the target DataType *recursively*, since these differences live in
            // CHILD field definitions (e.g. a Map's entries struct). A real
            // layout mismatch is an upstream bug → error.
            let rebuilt =
                rebuild_array_data_to_type(source_col.to_data(), target_field.data_type())
                    .map_err(|e| {
                        CoreError::ReadFileSliceError(format!(
                            "reconcile: column '{}' type {} incompatible with target {} \
                         (not a name-metadata difference): {e}",
                            target_field.name(),
                            source_col.data_type(),
                            target_field.data_type(),
                        ))
                    })?;
            Ok(arrow_array::make_array(rebuilt))
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(target_schema.clone(), columns)
        .map_err(|e| CoreError::ReadFileSliceError(format!("reconcile rebuild: {e}")))
}

/// Recursively rebuild `data` under `target_type`, replacing field names and
/// metadata at every nesting level (name-metadata reconciliation only — the
/// physical layout must already match; any layout mismatch fails loudly in
/// `build()`).
///
/// Why recursion: a single-level `into_builder().data_type(t).build()` only
/// rewrites the top-level `ArrayData`'s `DataType` tag, then validates that each
/// child `ArrayData`'s own `DataType` equals the target's expected child type.
/// For Avro-vs-Parquet differences buried in CHILD fields (a Map's entries
/// struct renamed "entries"→"key_value", or an entries field carrying
/// `avro.name`/`avro.namespace` metadata) the child `ArrayData` still reports its
/// own metadata-laden type, so validation rejects it. Descending into
/// `child_data` and rewriting each level's `DataType` resolves this.
///
/// Union/Dictionary types are not expected here; they take the no-children path
/// (empty target child types) and would fail loudly in `build()` on any layout
/// mismatch — which is the desired behavior.
fn rebuild_array_data_to_type(
    data: arrow::array::ArrayData,
    target_type: &DataType,
) -> std::result::Result<arrow::array::ArrayData, arrow_schema::ArrowError> {
    // Determine the target child types in `child_data` order.
    let target_child_types: Vec<DataType> = match target_type {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            vec![f.data_type().clone()]
        }
        DataType::Struct(fields) => fields.iter().map(|f| f.data_type().clone()).collect(),
        DataType::Map(entries, _) => vec![entries.data_type().clone()],
        _ => Vec::new(),
    };

    if target_child_types.is_empty() {
        // Leaf (or Union/Dictionary): just rewrite this level's type tag.
        return data.into_builder().data_type(target_type.clone()).build();
    }

    // Snapshot children BEFORE consuming `data` into the builder (the builder
    // does not expose a mutable child accessor on this arrow fork).
    let children = data.child_data().to_vec();

    // Reconcile, not evolution: child counts must match exactly. A mismatch
    // means an upstream evolution bug — fail loudly rather than silently
    // truncating via `zip`.
    if children.len() != target_child_types.len() {
        return Err(arrow_schema::ArrowError::InvalidArgumentError(format!(
            "reconcile child-count mismatch for {target_type}: source had {} children but target expects {}",
            children.len(),
            target_child_types.len(),
        )));
    }

    let rebuilt: Vec<arrow::array::ArrayData> = children
        .into_iter()
        .zip(target_child_types.iter())
        .map(|(c, t)| rebuild_array_data_to_type(c, t))
        .collect::<std::result::Result<_, _>>()?;

    data.into_builder()
        .data_type(target_type.clone())
        .child_data(rebuilt)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::reader_v2::record_context::RecordContext;
    use arrow_array::{Float64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Repro for the MOR read failure on arrays with NULL elements
    /// (TestMORFileSliceLayoutsExtendedTypes "2. NULL elements in containers"):
    ///
    /// The merge target schema is derived from the table's Avro schema, where
    /// array items are the nullable union `["null","int"]`. The former vendored
    /// Avro→Arrow converter used to force list elements non-nullable
    /// (`List(non-null Int32, field: 'array')`), so a log record holding
    /// `[1, NULL, 3]` could not be reconciled to the target and
    /// `concat_batches` failed with "It is not possible to concatenate arrays
    /// of different data types (List(Int32), List(non-null Int32, field:
    /// 'array'))". The arrow-avro-based derivation keeps elements nullable,
    /// and the merge must preserve the NULL element.
    #[test]
    fn test_records_to_batch_preserves_null_list_elements() {
        use arrow_array::{Array, Int32Array, ListArray};

        // Target schema exactly as the file group reader derives it: from the
        // table's Avro schema where array elements are nullable.
        let avro_json = r#"{
            "type": "record", "name": "rec", "fields": [
                {"name": "_hoodie_record_key", "type": ["null", "string"], "default": null},
                {"name": "arr", "type": ["null", {"type": "array", "items": ["null", "int"]}], "default": null}
            ]
        }"#;
        // Upstream routes this through arrow-avro; here it goes through the
        // crate's own Avro-to-Arrow conversion, which is the same shape.
        let avro = apache_avro::Schema::parse_str(avro_json).unwrap();
        let target: SchemaRef = Arc::new(crate::avro_to_arrow::to_arrow_schema(&avro).unwrap());

        // Log-file-like row (arrow-avro decode): element field "item", nullable,
        // data [1, NULL, 3].
        let log_arr =
            ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(vec![Some(
                vec![Some(1), None, Some(3)],
            )]);
        let log_schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, true),
            Field::new("arr", log_arr.data_type().clone(), true),
        ]));
        let log_batch = RecordBatch::try_new(
            log_schema,
            vec![
                Arc::new(StringArray::from(vec![Some("1")])),
                Arc::new(log_arr),
            ],
        )
        .unwrap();

        // Base-file-like row (parquet decode): element field "element", nullable,
        // data [10, 20] — no NULL elements.
        let base_values = Int32Array::from(vec![Some(10), Some(20)]);
        let base_arr = ListArray::new(
            Arc::new(Field::new("element", DataType::Int32, true)),
            arrow_buffer::OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(base_values),
            None,
        );
        let base_schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, true),
            Field::new("arr", base_arr.data_type().clone(), true),
        ]));
        let base_batch = RecordBatch::try_new(
            base_schema,
            vec![
                Arc::new(StringArray::from(vec![Some("2")])),
                Arc::new(base_arr),
            ],
        )
        .unwrap();

        let records = vec![
            BufferedRecord::new_data("1".to_string(), log_batch, None),
            BufferedRecord::new_data("2".to_string(), base_batch, None),
        ];

        let result = records_to_batch(records, target);
        let batch = result.expect("merge must preserve NULL list elements from log records");
        assert_eq!(batch.num_rows(), 2);

        let arr_col = batch
            .column(batch.schema().index_of("arr").unwrap())
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("arr must be a ListArray")
            .clone();

        // Row 0 (log record): [1, NULL, 3]
        let row0 = arr_col.value(0);
        let row0 = row0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(row0.len(), 3);
        assert_eq!(row0.value(0), 1);
        assert!(row0.is_null(1), "NULL list element must survive the merge");
        assert_eq!(row0.value(2), 3);

        // Row 1 (base record): [10, 20]
        let row1 = arr_col.value(1);
        let row1 = row1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(row1.len(), 2);
        assert_eq!(row1.value(0), 10);
        assert_eq!(row1.value(1), 20);
    }

    /// After the evolution work, every batch reaching records_to_batch is
    /// required-shaped. A genuinely missing column is an upstream bug → loud error,
    /// not panic (was: .expect panic).
    #[test]
    fn test_records_to_batch_missing_column_errors_loudly() {
        let log_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let log_batch = RecordBatch::try_new(
            log_schema,
            vec![Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef],
        )
        .unwrap();
        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("missing", DataType::Utf8, true),
        ]));
        let records = vec![BufferedRecord::new_data("k".to_string(), log_batch, None)];
        let result = records_to_batch(records, target);
        assert!(
            result.is_err(),
            "missing column must be an error, not a panic"
        );
    }

    /// Repro for the reconcile regression (commit 45bdceb): a Map column decoded
    /// by arrow-avro carries (a) the entries field named "entries" and (b)
    /// avro.name/avro.namespace metadata on the entries struct field, while the
    /// parquet-derived target uses "key_value" and no metadata. Both are genuine
    /// name-metadata differences, but they live in CHILD field definitions, so the
    /// single-level `ArrayData::into_builder().data_type(..).build()` rejected them
    /// with "Child type mismatch ... Expected Struct(..) but child data had
    /// Struct(.., metadata: {..})". The recursive rebuild must reconcile them and
    /// the data must round-trip.
    #[test]
    fn test_reconcile_map_entries_name_metadata_recurses() {
        use arrow_array::{Array, Int32Array, MapArray, StructArray};
        use std::collections::HashMap;

        // value struct (inner) — same physical layout in both source and target.
        let value_fields = arrow_schema::Fields::from(vec![
            Field::new("nested_int", DataType::Int32, true),
            Field::new("level", DataType::Utf8, true),
        ]);

        // --- SOURCE (arrow-avro shape): entries name "entries", child struct
        // fields carry avro.name/avro.namespace metadata.
        let keys = StringArray::from(vec!["k0", "k1"]);
        let nested_int = Int32Array::from(vec![Some(10), Some(20)]);
        let level = StringArray::from(vec![Some("a"), Some("b")]);
        let value_struct = StructArray::new(
            value_fields.clone(),
            vec![
                Arc::new(nested_int) as ArrayRef,
                Arc::new(level) as ArrayRef,
            ],
            None,
        );
        // Build the entries StructArray so its OWN data_type carries the avro
        // metadata — this mirrors arrow-avro, where the child ArrayData of the
        // MapArray reports `Struct(.., metadata: {avro.name..})`. We achieve this
        // by giving the struct fields metadata.
        let mut key_md = HashMap::new();
        key_md.insert("avro.name".to_string(), "nullable_map_field".to_string());
        key_md.insert(
            "avro.namespace".to_string(),
            "hoodie.t.t_record".to_string(),
        );
        let entries_fields = arrow_schema::Fields::from(vec![
            Field::new("key", DataType::Utf8, false).with_metadata(key_md),
            Field::new("value", DataType::Struct(value_fields.clone()), true),
        ]);
        let entries_struct = StructArray::new(
            entries_fields.clone(),
            vec![
                Arc::new(keys) as ArrayRef,
                Arc::new(value_struct) as ArrayRef,
            ],
            None,
        );
        let src_entry_struct_field = Field::new("entries", DataType::Struct(entries_fields), false);
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 2].into());
        let src_map = MapArray::new(
            Arc::new(src_entry_struct_field),
            offsets,
            entries_struct,
            None,
            false,
        );
        let src_schema = Arc::new(Schema::new(vec![Field::new(
            "nullable_map_field",
            src_map.data_type().clone(),
            true,
        )]));
        let src_batch =
            RecordBatch::try_new(src_schema, vec![Arc::new(src_map) as ArrayRef]).unwrap();

        // --- TARGET (parquet shape): entries name "key_value", no metadata.
        let tgt_entry_struct_field = Field::new(
            "key_value",
            DataType::Struct(arrow_schema::Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Struct(value_fields.clone()), true),
            ])),
            false,
        );
        let tgt_map_type = DataType::Map(Arc::new(tgt_entry_struct_field), false);
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "nullable_map_field",
            tgt_map_type,
            true,
        )]));

        // Reconcile must now SUCCEED.
        let reconciled = reconcile_batch_to_schema(&src_batch, &target)
            .expect("recursive name-metadata reconcile must succeed for nested map entries");
        assert_eq!(reconciled.schema(), target);
        assert_eq!(reconciled.num_rows(), 1);

        // Data must round-trip: read a key/value back.
        let map_col = reconciled
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("must be a MapArray");
        let row_keys = map_col
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(row_keys.value(0), "k0");
        assert_eq!(row_keys.value(1), "k1");
        let row_vals = map_col
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let vi = row_vals
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(vi.value(0), 10);
        assert_eq!(vi.value(1), 20);
    }

    fn make_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
        ]));

        let keys: Vec<String> = (0..num_rows).map(|i| format!("key_{i}")).collect();
        let names: Vec<Option<String>> = (0..num_rows).map(|i| Some(format!("name_{i}"))).collect();
        let values: Vec<f64> = (0..num_rows).map(|i| i as f64).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_slice_row() {
        let batch = make_test_batch(3);
        let row = slice_row(&batch, 1);
        assert_eq!(row.num_rows(), 1);
        let ctx = RecordContext::default();
        let keys = ctx.get_record_keys(&row).unwrap();
        assert_eq!(keys, vec!["key_1"]);
    }

    /// Extract `(key, value)` tuples from a make_test_batch-shaped batch, in row
    /// order, for full-data assertions (guide §5).
    fn extract_kv(batch: &RecordBatch) -> Vec<(String, i64)> {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| (keys.value(i).to_string(), values.value(i) as i64))
            .collect()
    }

    #[test]
    fn test_records_to_batch_roundtrip() {
        let batch = Arc::new(make_test_batch(3));
        let schema = batch.schema();
        let ctx = RecordContext::default();
        let records = ctx.batch_to_buffered_records(&batch, None).unwrap();
        let buffered: Vec<BufferedRecord> = records.into_iter().map(|(_, r)| r).collect();

        let result = records_to_batch(buffered, schema.clone()).unwrap();
        assert_eq!(result.schema(), schema);
        // Full-data assert: interleave output preserves every row in order.
        assert_eq!(
            extract_kv(&result),
            vec![
                ("key_0".into(), 0),
                ("key_1".into(), 1),
                ("key_2".into(), 2)
            ]
        );
    }

    #[test]
    fn test_records_to_batch_empty() {
        let schema = make_test_batch(0).schema();
        let result = records_to_batch(vec![], schema.clone()).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    /// A2 drain: interleave over BatchRefs that point at NON-contiguous rows of a
    /// shared source batch produces exactly those rows in the order given (not
    /// the source order) — proving the (slot,row) index path, not a naive slice.
    #[test]
    fn test_records_to_batch_interleaves_shared_batch_rows() {
        let src = Arc::new(make_test_batch(5)); // key_0..key_4 / values 0..4
        let schema = src.schema();
        // Emit rows 3, 1, 4 (out of order) — all referencing the same Arc.
        let records = vec![
            BufferedRecord::new_batch_ref("key_3".into(), src.clone(), 3, None),
            BufferedRecord::new_batch_ref("key_1".into(), src.clone(), 1, None),
            BufferedRecord::new_batch_ref("key_4".into(), src.clone(), 4, None),
        ];
        let out = records_to_batch(records, schema).unwrap();
        assert_eq!(
            extract_kv(&out),
            vec![
                ("key_3".into(), 3),
                ("key_1".into(), 1),
                ("key_4".into(), 4)
            ],
            "interleave must emit the addressed rows in record order"
        );
    }

    /// A2 drain: mixed BatchRef + Owned + Delete payloads — deletes drop out, the
    /// rest interleave in order with exact data, across two distinct source Arcs.
    #[test]
    fn test_records_to_batch_mixed_payloads_and_deletes() {
        let src_a = Arc::new(make_test_batch(3)); // key_0,key_1,key_2
        let schema = src_a.schema();
        let owned = slice_row(&make_test_batch(10), 7); // key_7 / value 7
        let records = vec![
            BufferedRecord::new_batch_ref("key_2".into(), src_a.clone(), 2, None),
            BufferedRecord::new_delete("gone".into(), None),
            BufferedRecord::new_data("key_7".into(), owned, None),
            BufferedRecord::new_batch_ref("key_0".into(), src_a.clone(), 0, None),
        ];
        let out = records_to_batch(records, schema).unwrap();
        assert_eq!(
            extract_kv(&out),
            vec![
                ("key_2".into(), 2),
                ("key_7".into(), 7),
                ("key_0".into(), 0)
            ],
            "deletes are skipped; BatchRef + Owned rows interleave in order"
        );
    }

    /// A2 drain: an all-deletes record set produces an empty batch with the
    /// target schema (no interleave call, no panic).
    #[test]
    fn test_records_to_batch_all_deletes_is_empty() {
        let schema = make_test_batch(0).schema();
        let records = vec![
            BufferedRecord::new_delete("a".into(), None),
            BufferedRecord::new_delete("b".into(), None),
        ];
        let out = records_to_batch(records, schema.clone()).unwrap();
        assert_eq!(out.num_rows(), 0);
        assert_eq!(out.schema(), schema);
    }
}
