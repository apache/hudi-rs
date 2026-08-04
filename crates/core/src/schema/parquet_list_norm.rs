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

//! ENG-44975 (I-3): normalize legacy 2-level `array<map>` parquet encodings so
//! parquet-rs's parquet→arrow schema builder accepts them.
//!
//! # The problem
//!
//! The parquet-avro writer defaults to `write-old-list-structure=true`, which
//! encodes an `array<map>` column as a **2-level legacy list** whose element is
//! a **REPEATED** map group:
//!
//! ```text
//! required group obj_ids (LIST) {
//!   repeated group array (MAP) {           // <-- element IS the map, and REPEATED
//!     repeated group key_value (MAP_KEY_VALUE) { required key; required value; }
//!   }
//! }
//! ```
//!
//! When parquet-rs converts this to arrow it walks the LIST node, sees `array`
//! has a single repeated child, treats `array` as the list wrapper and
//! `key_value` as the element, then dispatches `key_value` (MAP_KEY_VALUE) to
//! `visit_map`, which rejects any REPEATED map **unconditionally**:
//! `arrow_err!("Map cannot be repeated")`
//! (`parquet/src/arrow/schema/complex.rs`). The whole read fails with
//! `Arrow: Map cannot be repeated`. Two independent arrow implementations
//! (arrow-rs and arrow-cpp/pyarrow) reject the same physical schema, so the
//! encoding is genuinely legacy — but the JVM Hudi reader accepts it, so
//! hudi-rs must too (already-written tables must stay readable).
//!
//! # The fix
//!
//! parquet-rs's reject is unconditional, so we cannot fix it at the arrow
//! level. Instead we pre-normalize the **parquet `Type` tree** before the arrow
//! build: rewrite each legacy `REPEATED group X (MAP)` that is the single child
//! of a LIST group into the standard 3-level form
//!
//! ```text
//! group obj_ids (LIST) {
//!   repeated group list {                  // synthetic wrapper carries the repetition
//!     required group X (MAP) { ... }        // map demoted to REQUIRED -> visit_map accepts
//!   }
//! }
//! ```
//!
//! This is a pure schema rewrite: the physical column chunks are untouched. The
//! transform preserves every leaf's definition/repetition level (the REPEATED
//! `array` becomes the REPEATED `list` wrapper — same level contribution — and
//! the inserted REQUIRED `X` adds no level) and the leaf DFS order/count, so the
//! parquet reader decodes the exact same bytes into the exact same values. The
//! arrow reader keys leaf decoding off the rewritten field's def/rep levels and
//! the leaf column index — never the row-group schema descriptor — so we can
//! reuse the original row groups verbatim.

use std::sync::Arc;

use parquet::basic::{ConvertedType, LogicalType, Repetition};
use parquet::file::metadata::{FileMetaData, ParquetMetaData, ParquetMetaDataBuilder};
use parquet::schema::types::{SchemaDescriptor, Type, TypePtr};

fn is_map_group(t: &Type) -> bool {
    if t.is_primitive() {
        return false;
    }
    let bi = t.get_basic_info();
    matches!(
        bi.converted_type(),
        ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE
    ) || matches!(bi.logical_type_ref(), Some(LogicalType::Map))
}

fn is_list_group(t: &Type) -> bool {
    if t.is_primitive() {
        return false;
    }
    let bi = t.get_basic_info();
    bi.converted_type() == ConvertedType::LIST
        || matches!(bi.logical_type_ref(), Some(LogicalType::List))
}

/// Rebuild a group `Type`, preserving name / converted_type / logical_type / id,
/// with the supplied child `fields`. If `repetition` is `Some`, override the
/// node's repetition; otherwise keep the source node's repetition (a root
/// message type has none).
fn rebuild_group(src: &Type, fields: Vec<TypePtr>, repetition: Option<Repetition>) -> TypePtr {
    let bi = src.get_basic_info();
    let mut builder = Type::group_type_builder(bi.name())
        .with_converted_type(bi.converted_type())
        .with_logical_type(bi.logical_type_ref().cloned())
        .with_fields(fields);
    if bi.has_id() {
        builder = builder.with_id(Some(bi.id()));
    }
    let rep = repetition.or_else(|| bi.has_repetition().then(|| bi.repetition()));
    if let Some(r) = rep {
        builder = builder.with_repetition(r);
    }
    Arc::new(
        builder
            .build()
            .expect("rebuilt group type is well-formed by construction"),
    )
}

/// Recursively rewrite legacy repeated-map lists in a parquet `Type` subtree.
///
/// Returns `Some(new_type)` if any rewrite occurred within this subtree, or
/// `None` if the subtree is already spec-compliant (letting the caller keep the
/// original `Arc` without cloning).
fn normalize_type(t: &TypePtr) -> Option<TypePtr> {
    if t.is_primitive() {
        return None;
    }

    // Normalize children first so a nested legacy list is fixed even when the
    // parent must also be rebuilt.
    let mut child_changed = false;
    let mut new_children: Vec<TypePtr> = Vec::with_capacity(t.get_fields().len());
    for c in t.get_fields() {
        match normalize_type(c) {
            Some(nc) => {
                child_changed = true;
                new_children.push(nc);
            }
            None => new_children.push(c.clone()),
        }
    }

    // Detect the legacy 2-level `array<map>` shape at THIS node: a LIST group
    // whose single child is a REPEATED map group.
    if is_list_group(t) && new_children.len() == 1 {
        let child = &new_children[0];
        if !child.is_primitive()
            && child.get_basic_info().has_repetition()
            && child.get_basic_info().repetition() == Repetition::REPEATED
            && is_map_group(child)
        {
            // Demote the repeated map to a REQUIRED element; its (already
            // normalized) key_value child is preserved verbatim.
            let demoted_map = rebuild_group(
                child,
                child.get_fields().to_vec(),
                Some(Repetition::REQUIRED),
            );
            // Wrap it in a synthetic REPEATED `list` group. The name must NOT be
            // `array` or `<list>_tuple`, or parquet-rs's 2-level back-compat
            // branch would treat the wrapper itself as the element.
            let wrapper = Type::group_type_builder("list")
                .with_repetition(Repetition::REPEATED)
                .with_fields(vec![demoted_map])
                .build()
                .expect("synthetic list wrapper is well-formed");
            let new_list = rebuild_group(t, vec![Arc::new(wrapper)], None);
            return Some(new_list);
        }
    }

    if child_changed {
        Some(rebuild_group(t, new_children, None))
    } else {
        None
    }
}

/// Normalize a parquet [`SchemaDescriptor`], rewriting any legacy repeated-map
/// list. Returns `None` when nothing needed rewriting.
pub fn normalize_schema_descriptor(desc: &SchemaDescriptor) -> Option<SchemaDescriptor> {
    let root = desc.root_schema_ptr();
    normalize_type(&root).map(SchemaDescriptor::new)
}

/// Return `ParquetMetaData` whose schema has legacy repeated-map lists
/// normalized to the 3-level form. The original row groups, column index, and
/// offset index are preserved unchanged (the arrow reader decodes leaves by
/// column index + the rewritten field's def/rep levels, never consulting the
/// row-group schema descriptor). When the schema is already spec-compliant the
/// input `Arc` is returned unchanged.
pub fn normalize_parquet_metadata(meta: Arc<ParquetMetaData>) -> Arc<ParquetMetaData> {
    let fmd = meta.file_metadata();
    let Some(new_desc) = normalize_schema_descriptor(fmd.schema_descr()) else {
        return meta;
    };

    let new_file_md = FileMetaData::new(
        fmd.version(),
        fmd.num_rows(),
        fmd.created_by().map(str::to_string),
        fmd.key_value_metadata().cloned(),
        Arc::new(new_desc),
        fmd.column_orders().cloned(),
    );

    // Move the (schema-independent) row groups and page indexes over unchanged.
    let mut src = ParquetMetaDataBuilder::from((*meta).clone());
    let row_groups = src.take_row_groups();
    let column_index = src.take_column_index();
    let offset_index = src.take_offset_index();

    let new_meta = ParquetMetaDataBuilder::new(new_file_md)
        .set_row_groups(row_groups)
        .set_column_index(column_index)
        .set_offset_index(offset_index)
        .build();

    Arc::new(new_meta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
    };
    use parquet::arrow::parquet_to_arrow_schema;
    use parquet::file::metadata::ParquetMetaDataReader;

    // A real parquet log block written by the Hudi AVRO write path with the
    // default `write-old-list-structure=true`, carrying an `array<map<string,
    // string>>` column (`obj_ids`) in the rejected 2-level encoding. Captured
    // as I-3 evidence (evidence/i3-avro-block.parquet).
    const LEGACY_2LEVEL: &[u8] =
        include_bytes!("../../tests/data/i3/legacy_2level_repeated_map.parquet");

    fn raw_metadata(bytes: &[u8]) -> Arc<ParquetMetaData> {
        // Footer-only parse: this does NOT build the arrow schema, so it
        // succeeds even on the legacy encoding.
        Arc::new(
            ParquetMetaDataReader::new()
                .parse_and_finish(&Bytes::from(bytes.to_vec()))
                .expect("footer parse"),
        )
    }

    /// Documents the bug: the raw (un-normalized) schema is rejected by the
    /// parquet→arrow builder with exactly the runtime error we see in gluten.
    #[test]
    fn legacy_schema_is_rejected_before_normalization() {
        let meta = raw_metadata(LEGACY_2LEVEL);
        let err = parquet_to_arrow_schema(meta.file_metadata().schema_descr(), None)
            .expect_err("legacy repeated-map schema must be rejected by parquet-rs");
        assert!(
            err.to_string().contains("Map cannot be repeated"),
            "unexpected error: {err}"
        );
    }

    /// The fix: after normalization the schema converts to arrow cleanly and the
    /// legacy list surfaces as a List of Map.
    #[test]
    fn normalized_schema_converts_to_arrow_list_of_map() {
        let meta = raw_metadata(LEGACY_2LEVEL);
        let new_desc = normalize_schema_descriptor(meta.file_metadata().schema_descr())
            .expect("legacy schema should be rewritten");
        let arrow = parquet_to_arrow_schema(&new_desc, None)
            .expect("normalized schema must convert to arrow");

        let field = arrow
            .field_with_name("obj_ids")
            .expect("obj_ids column present");
        match field.data_type() {
            arrow_schema::DataType::List(inner) | arrow_schema::DataType::LargeList(inner) => {
                assert!(
                    matches!(inner.data_type(), arrow_schema::DataType::Map(_, _)),
                    "list element should be a Map, got {:?}",
                    inner.data_type()
                );
            }
            other => panic!("obj_ids should be a List<Map>, got {other:?}"),
        }
    }

    /// End-to-end: a reader built on the normalized metadata actually decodes the
    /// physical column chunks and returns the rows (no data loss, correct shape).
    #[test]
    fn reader_on_normalized_metadata_returns_rows() {
        let content = Bytes::from(LEGACY_2LEVEL.to_vec());
        let raw = raw_metadata(LEGACY_2LEVEL);
        let expected_rows = raw.file_metadata().num_rows();
        assert!(expected_rows > 0, "fixture must contain rows");

        let normalized = normalize_parquet_metadata(raw);
        let arm = ArrowReaderMetadata::try_new(normalized, ArrowReaderOptions::new())
            .expect("arrow metadata builds on normalized schema");
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(content, arm)
            .build()
            .expect("reader builds");

        let mut rows = 0i64;
        let mut saw_map_col = false;
        for batch in reader {
            let batch = batch.expect("batch decodes");
            rows += batch.num_rows() as i64;
            if let Ok(col) = batch.column_by_name("obj_ids").ok_or(()) {
                saw_map_col = matches!(col.data_type(), arrow_schema::DataType::List(_));
            }
        }
        assert_eq!(rows, expected_rows, "all physical rows must be returned");
        assert!(saw_map_col, "obj_ids must decode as a List column");
    }
}
