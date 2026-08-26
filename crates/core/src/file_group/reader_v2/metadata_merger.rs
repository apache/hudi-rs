//! Hudi metadata payload merge semantics for the CUSTOM record merge mode.
//!
//! The metadata table states `hoodie.record.merge.mode=CUSTOM` and names
//! `HoodieMetadataPayload` as its payload class. Its merge rule is the payload's
//! `preCombine`, which dispatches on the record's `type` column: three of the six
//! partition types take the newer record whole, one folds a map, and two share a
//! rule that combines statistics.
//!
//! Selection is on the payload class, not on the merge strategy id. Every
//! payload-based custom table carries the same all-zeros strategy id, so the id
//! says only that the payload decides; it cannot say which payload.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array, MapArray, RecordBatch, StructArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Fields};

use crate::Result;
use crate::error::CoreError;
use crate::file_group::reader_v2::buffered_record::{BufferedRecord, DeleteRecord, RecordPayload};
use crate::file_group::reader_v2::record_merger::BufferedRecordMerger;
use crate::file_group::reader_v2::resolver::{PAYLOAD_CLASS_KEYS, RECORD_MERGE_STRATEGY_ID_KEYS};

/// The payload class the metadata table names.
const METADATA_PAYLOAD_CLASS: &str = "org.apache.hudi.metadata.HoodieMetadataPayload";

/// Java's `HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID`: the strategy id
/// that defers the merge to the payload class rather than naming a merger.
const PAYLOAD_BASED_STRATEGY_ID: &str = "00000000-0000-0000-0000-000000000000";

/// The record's partition type, from the `type` column. Codes are Java's
/// `MetadataPartitionType.getRecordType()`.
const TYPE_ALL_PARTITIONS: i32 = 1;
const TYPE_FILES: i32 = 2;
const TYPE_COLUMN_STATS: i32 = 3;
const TYPE_PARTITION_STATS: i32 = 6;

const TYPE_COLUMN: &str = "type";
const FILESYSTEM_METADATA_COLUMN: &str = "filesystemMetadata";
const COLUMN_STATS_COLUMN: &str = "ColumnStatsMetadata";
const MIN_VALUE_FIELD: &str = "minValue";
const MAX_VALUE_FIELD: &str = "maxValue";
const IS_TIGHT_BOUND_FIELD: &str = "isTightBound";
const WRAPPED_VALUE_FIELD: &str = "value";
/// The four counters that are summed rather than chosen.
const COUNTER_FIELDS: [&str; 4] = [
    "valueCount",
    "nullCount",
    "totalSize",
    "totalUncompressedSize",
];

const FILE_SIZE_FIELD: &str = "size";
const FILE_IS_DELETED_FIELD: &str = "isDeleted";

/// A custom merge mode this crate knows how to serve.
///
/// CUSTOM is refused unless it resolves to one of these, so a table naming some
/// other payload keeps erroring rather than being merged by the wrong rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CustomMerger {
    /// `org.apache.hudi.metadata.HoodieMetadataPayload`.
    MetadataPayload,
}

/// Which custom merger a table's config selects, if any.
///
/// The payload class is the discriminator. The strategy id is consulted only to
/// confirm it defers to the payload: an id naming a merger of its own is not
/// something this crate implements, so it resolves to `None` and CUSTOM stays
/// refused.
pub fn resolve_custom_merger(table_config: &HashMap<String, String>) -> Option<CustomMerger> {
    let lookup = |keys: &[&str]| -> String {
        keys.iter()
            .find_map(|k| table_config.get(*k))
            .map(|s| s.trim().to_string())
            .unwrap_or_default()
    };
    let payload_class = lookup(&PAYLOAD_CLASS_KEYS);
    let strategy_id = lookup(&RECORD_MERGE_STRATEGY_ID_KEYS);

    let defers_to_payload =
        strategy_id.is_empty() || strategy_id.eq_ignore_ascii_case(PAYLOAD_BASED_STRATEGY_ID);
    if !defers_to_payload {
        return None;
    }
    match payload_class.as_str() {
        METADATA_PAYLOAD_CLASS => Some(CustomMerger::MetadataPayload),
        _ => None,
    }
}

/// Hudi's metadata payload merge, as a [`BufferedRecordMerger`].
///
/// Mirrors `HoodieMetadataPayload.preCombine` and the per-type
/// `MetadataPartitionType.combineMetadataPayloads`.
#[derive(Debug)]
pub struct MetadataPayloadMerger;

impl BufferedRecordMerger for MetadataPayloadMerger {
    fn delta_merge(
        &self,
        new_record: &BufferedRecord,
        existing_record: Option<&BufferedRecord>,
    ) -> Result<Option<BufferedRecord>> {
        match existing_record {
            Some(existing) => Ok(Some(self.final_merge(existing, new_record)?)),
            None => Ok(Some(new_record.clone())),
        }
    }

    fn delta_merge_delete(
        &self,
        delete_record: &DeleteRecord,
        _existing_record: Option<&BufferedRecord>,
    ) -> Result<Option<DeleteRecord>> {
        // `preCombine` short-circuits on `isDeletedRecord` before reaching any
        // per-type rule: a delete cancels what came before it whatever the type.
        Ok(Some(delete_record.clone()))
    }

    fn final_merge(
        &self,
        older_record: &BufferedRecord,
        newer_record: &BufferedRecord,
    ) -> Result<BufferedRecord> {
        // A tombstone on either side ends the merge, matching the two
        // short-circuits at the top of `preCombine`.
        if newer_record.is_delete() || older_record.is_delete() {
            return Ok(newer_record.clone());
        }
        let (Some(older), Some(newer)) = (
            older_record.payload.get_record(),
            newer_record.payload.get_record(),
        ) else {
            return Ok(newer_record.clone());
        };

        match record_type(&newer)? {
            TYPE_ALL_PARTITIONS | TYPE_FILES => {
                let folded = fold_filesystem_metadata(&older, &newer)?;
                Ok(BufferedRecord {
                    record_key: newer_record.record_key.clone(),
                    payload: RecordPayload::Owned(folded),
                    ordering_value: newer_record.ordering_value.clone(),
                })
            }
            TYPE_COLUMN_STATS | TYPE_PARTITION_STATS => {
                let folded = fold_column_stats(&older, &newer)?;
                Ok(BufferedRecord {
                    record_key: newer_record.record_key.clone(),
                    payload: RecordPayload::Owned(folded),
                    ordering_value: newer_record.ordering_value.clone(),
                })
            }
            // Bloom filters, record index and secondary index all take the newer
            // record: the first is documented as additive, the other two inherit
            // `combineMetadataPayloads`'s default.
            _ => Ok(newer_record.clone()),
        }
    }
}

/// Read the record's partition type from its `type` column.
fn record_type(batch: &RecordBatch) -> Result<i32> {
    let column = batch.column_by_name(TYPE_COLUMN).ok_or_else(|| {
        CoreError::Unsupported(format!(
            "A metadata record must carry a '{TYPE_COLUMN}' column to be merged."
        ))
    })?;
    let typed = column.as_primitive_opt::<Int32Type>().ok_or_else(|| {
        CoreError::Unsupported(format!(
            "The metadata '{TYPE_COLUMN}' column must be Int32, found {:?}.",
            column.data_type()
        ))
    })?;
    if typed.is_empty() || typed.is_null(0) {
        return Err(CoreError::Unsupported(
            "A metadata record's partition type is null.".to_string(),
        ));
    }
    Ok(typed.value(0))
}

/// One `filesystemMetadata` entry: the file size and whether it is a tombstone.
#[derive(Clone, Copy)]
struct FileInfo {
    size: i64,
    is_deleted: bool,
}

/// Fold the newer record's `filesystemMetadata` into the older record's, and
/// return the newer row with only that column replaced.
///
/// Mirrors `MetadataPartitionType.combineFileSystemMetadata`.
fn fold_filesystem_metadata(older: &RecordBatch, newer: &RecordBatch) -> Result<RecordBatch> {
    let Some((column_idx, _)) = newer
        .schema()
        .column_with_name(FILESYSTEM_METADATA_COLUMN)
        .map(|(i, f)| (i, f.clone()))
    else {
        // A projection that dropped the map has nothing to fold; the newer row
        // is what Java would return for every other column anyway.
        return Ok(newer.clone());
    };
    let newer_column = newer.column(column_idx);
    let older_column = older.column_by_name(FILESYSTEM_METADATA_COLUMN);

    let mut merged: BTreeMap<String, FileInfo> = match older_column {
        Some(column) => read_file_map(column)?,
        None => BTreeMap::new(),
    };
    for (name, new_info) in read_file_map(newer_column)? {
        match merged.get(&name) {
            Some(old_info) => {
                if new_info.is_deleted {
                    if old_info.is_deleted {
                        // Both tombstones: the newer one stands.
                        merged.insert(name, new_info);
                    } else {
                        // A deletion cancels the entry rather than marking it.
                        merged.remove(&name);
                    }
                } else {
                    merged.insert(
                        name,
                        FileInfo {
                            size: old_info.size.max(new_info.size),
                            is_deleted: false,
                        },
                    );
                }
            }
            None => {
                merged.insert(name, new_info);
            }
        }
    }

    let folded = build_file_map(newer_column.data_type(), &merged)?;
    let mut columns = newer.columns().to_vec();
    columns[column_idx] = folded;
    RecordBatch::try_new(newer.schema(), columns).map_err(CoreError::ArrowError)
}

/// Read row 0 of a `filesystemMetadata` map column.
fn read_file_map(column: &ArrayRef) -> Result<BTreeMap<String, FileInfo>> {
    let map = column.as_map_opt().ok_or_else(|| {
        CoreError::Unsupported(format!(
            "'{FILESYSTEM_METADATA_COLUMN}' must be a Map column, found {:?}.",
            column.data_type()
        ))
    })?;
    let mut entries = BTreeMap::new();
    if map.is_empty() || map.is_null(0) {
        return Ok(entries);
    }
    let row = map.value(0);
    let keys = row.column(0).as_string_opt::<i32>().ok_or_else(|| {
        CoreError::Unsupported(format!(
            "'{FILESYSTEM_METADATA_COLUMN}' keys must be Utf8 file names."
        ))
    })?;
    let values = row.column(1).as_struct_opt().ok_or_else(|| {
        CoreError::Unsupported("'{FILESYSTEM_METADATA_COLUMN}' values must be structs.".to_string())
    })?;
    let sizes = struct_field(values, FILE_SIZE_FIELD)?
        .as_primitive_opt::<arrow_array::types::Int64Type>()
        .ok_or_else(|| {
            CoreError::Unsupported(format!("'{FILE_SIZE_FIELD}' must be Int64.").to_string())
        })?
        .clone();
    let deleted = struct_field(values, FILE_IS_DELETED_FIELD)?
        .as_boolean_opt()
        .ok_or_else(|| {
            CoreError::Unsupported(
                format!("'{FILE_IS_DELETED_FIELD}' must be Boolean.").to_string(),
            )
        })?
        .clone();

    for i in 0..keys.len() {
        if keys.is_null(i) {
            continue;
        }
        entries.insert(
            keys.value(i).to_string(),
            FileInfo {
                size: if sizes.is_null(i) { 0 } else { sizes.value(i) },
                is_deleted: !deleted.is_null(i) && deleted.value(i),
            },
        );
    }
    Ok(entries)
}

/// Look a field up in a struct array by name.
fn struct_field<'a>(values: &'a StructArray, name: &str) -> Result<&'a ArrayRef> {
    values.column_by_name(name).ok_or_else(|| {
        CoreError::Unsupported(format!(
            "A '{FILESYSTEM_METADATA_COLUMN}' value must carry a '{name}' field."
        ))
    })
}

/// Build a single-row map column holding `entries`, keeping the source column's
/// exact field names, nullability and ordering so the batch schema is unchanged.
fn build_file_map(data_type: &DataType, entries: &BTreeMap<String, FileInfo>) -> Result<ArrayRef> {
    let DataType::Map(entries_field, sorted) = data_type else {
        return Err(CoreError::Unsupported(format!(
            "'{FILESYSTEM_METADATA_COLUMN}' must be a Map column, found {data_type:?}."
        )));
    };
    let DataType::Struct(entry_fields) = entries_field.data_type() else {
        return Err(CoreError::Unsupported(
            "A Map column's entries must be a struct.".to_string(),
        ));
    };
    let key_field = entry_fields[0].clone();
    let value_field = entry_fields[1].clone();
    let DataType::Struct(value_fields) = value_field.data_type() else {
        return Err(CoreError::Unsupported(format!(
            "'{FILESYSTEM_METADATA_COLUMN}' values must be structs."
        )));
    };

    let names: Vec<&str> = entries.keys().map(String::as_str).collect();
    let sizes: Vec<i64> = entries.values().map(|f| f.size).collect();
    let deleted: Vec<bool> = entries.values().map(|f| f.is_deleted).collect();

    let mut value_columns: Vec<ArrayRef> = Vec::with_capacity(value_fields.len());
    for field in value_fields.iter() {
        match field.name().as_str() {
            FILE_SIZE_FIELD => value_columns.push(Arc::new(Int64Array::from(sizes.clone()))),
            FILE_IS_DELETED_FIELD => {
                value_columns.push(Arc::new(BooleanArray::from(deleted.clone())))
            }
            other => {
                return Err(CoreError::Unsupported(format!(
                    "Unexpected '{FILESYSTEM_METADATA_COLUMN}' value field '{other}'."
                )));
            }
        }
    }
    let values = StructArray::try_new(value_fields.clone(), value_columns, None)
        .map_err(CoreError::ArrowError)?;
    let keys = arrow_array::StringArray::from(names);

    let entry_struct = StructArray::try_new(
        Fields::from(vec![key_field, value_field]),
        vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
        None,
    )
    .map_err(CoreError::ArrowError)?;

    let offsets = OffsetBuffer::new(vec![0i32, entries.len() as i32].into());
    let map = MapArray::try_new(
        Arc::new(Field::clone(entries_field)),
        offsets,
        entry_struct,
        None,
        *sorted,
    )
    .map_err(CoreError::ArrowError)?;
    Ok(Arc::new(map))
}

/// Fold the newer record's `ColumnStatsMetadata` into the older record's, and
/// return the newer row with only that column replaced.
///
/// Mirrors `HoodieTableMetadataUtil.mergeColumnStatsRecords`. Column stats and
/// partition stats share this rule.
fn fold_column_stats(older: &RecordBatch, newer: &RecordBatch) -> Result<RecordBatch> {
    let Some((column_idx, _)) = newer
        .schema()
        .column_with_name(COLUMN_STATS_COLUMN)
        .map(|(i, f)| (i, f.clone()))
    else {
        return Ok(newer.clone());
    };
    let newer_stats = stats_struct(newer.column(column_idx))?;
    let Some(older_column) = older.column_by_name(COLUMN_STATS_COLUMN) else {
        return Ok(newer.clone());
    };
    let older_stats = stats_struct(older_column)?;

    // A struct that is null on either side carries no statistics to combine.
    let (Some(newer_stats), Some(older_stats)) = (newer_stats, older_stats) else {
        return Ok(newer.clone());
    };

    // Two short-circuits from Java, in its order: a tombstone on either side
    // overwrites, and a tight-bound newer record supersedes the older outright
    // rather than widening to cover it.
    if flag(&newer_stats, FILE_IS_DELETED_FIELD) || flag(&older_stats, FILE_IS_DELETED_FIELD) {
        return Ok(newer.clone());
    }
    if flag(&newer_stats, IS_TIGHT_BOUND_FIELD) {
        return Ok(newer.clone());
    }

    // Every field is carried from the newer record except the two bounds, which
    // are chosen, and the four counters, which are summed.
    let fields = match newer_stats.data_type() {
        DataType::Struct(fields) => fields.clone(),
        other => {
            return Err(CoreError::Unsupported(format!(
                "'{COLUMN_STATS_COLUMN}' must be a struct, found {other:?}."
            )));
        }
    };
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for (idx, field) in fields.iter().enumerate() {
        let name = field.name().as_str();
        let column = if name == MIN_VALUE_FIELD {
            pick_bound(&older_stats, &newer_stats, name, std::cmp::Ordering::Less)?
        } else if name == MAX_VALUE_FIELD {
            pick_bound(
                &older_stats,
                &newer_stats,
                name,
                std::cmp::Ordering::Greater,
            )?
        } else if COUNTER_FIELDS.contains(&name) {
            sum_counter(&older_stats, &newer_stats, name)?
        } else {
            newer_stats.column(idx).clone()
        };
        columns.push(column);
    }

    let folded = StructArray::try_new(fields, columns, newer_stats.nulls().cloned())
        .map_err(CoreError::ArrowError)?;
    let mut batch_columns = newer.columns().to_vec();
    batch_columns[column_idx] = Arc::new(folded);
    RecordBatch::try_new(newer.schema(), batch_columns).map_err(CoreError::ArrowError)
}

/// The single-row `ColumnStatsMetadata` struct, or `None` when it is null.
fn stats_struct(column: &ArrayRef) -> Result<Option<StructArray>> {
    let stats = column.as_struct_opt().ok_or_else(|| {
        CoreError::Unsupported(format!(
            "'{COLUMN_STATS_COLUMN}' must be a struct column, found {:?}.",
            column.data_type()
        ))
    })?;
    if stats.is_empty() || stats.is_null(0) {
        return Ok(None);
    }
    Ok(Some(stats.clone()))
}

/// Read a boolean field, treating a missing or null field as false. Both flags
/// this reads are short-circuits, so absence must not trigger one.
fn flag(stats: &StructArray, name: &str) -> bool {
    stats
        .column_by_name(name)
        .and_then(|c| c.as_boolean_opt().cloned())
        .is_some_and(|values| !values.is_null(0) && values.value(0))
}

/// Sum a counter across both records, keeping null only when both are null.
fn sum_counter(older: &StructArray, newer: &StructArray, name: &str) -> Result<ArrayRef> {
    let read = |stats: &StructArray| -> Option<i64> {
        stats
            .column_by_name(name)?
            .as_primitive_opt::<arrow_array::types::Int64Type>()
            .filter(|values| !values.is_null(0))
            .map(|values| values.value(0))
    };
    let summed = match (read(older), read(newer)) {
        (None, None) => None,
        (a, b) => Some(a.unwrap_or(0).saturating_add(b.unwrap_or(0))),
    };
    Ok(Arc::new(Int64Array::from(vec![summed])))
}

/// Choose the lower `minValue` or the higher `maxValue` across the two records.
///
/// The bound is an Avro union of typed wrappers, so the comparison happens
/// inside one branch: a null branch loses to a value, and two different branches
/// are a type change this crate does not reconcile.
fn pick_bound(
    older: &StructArray,
    newer: &StructArray,
    name: &str,
    want: std::cmp::Ordering,
) -> Result<ArrayRef> {
    let (Some(older_bound), Some(newer_bound)) =
        (older.column_by_name(name), newer.column_by_name(name))
    else {
        return Err(CoreError::Unsupported(format!(
            "'{COLUMN_STATS_COLUMN}' must carry a '{name}' field to be merged."
        )));
    };
    let older_union = bound_union(older_bound, name)?;
    let newer_union = bound_union(newer_bound, name)?;

    let older_value = wrapped_value(older_union, name)?;
    let newer_value = wrapped_value(newer_union, name)?;

    // Both bounds are already single-row arrays, so the winner is returned whole
    // rather than rebuilt, which keeps the union's branch layout exactly as the
    // writer produced it.
    match (older_value, newer_value) {
        (None, _) => Ok(newer_bound.clone()),
        (_, None) => Ok(older_bound.clone()),
        (Some((older_values, older_idx)), Some((newer_values, newer_idx))) => {
            if older_values.data_type() != newer_values.data_type() {
                return Err(CoreError::Unsupported(format!(
                    "'{name}' changes type between the two records being merged                      ({:?} and {:?}); this crate does not promote column statistics                      across types.",
                    older_values.data_type(),
                    newer_values.data_type()
                )));
            }
            let compare = arrow_ord::ord::make_comparator(
                older_values.as_ref(),
                newer_values.as_ref(),
                arrow_schema::SortOptions::default(),
            )
            .map_err(CoreError::ArrowError)?;
            if compare(older_idx, newer_idx) == want {
                Ok(older_bound.clone())
            } else {
                Ok(newer_bound.clone())
            }
        }
    }
}

/// A bound column as a union.
fn bound_union<'a>(bound: &'a ArrayRef, name: &str) -> Result<&'a arrow_array::UnionArray> {
    bound
        .as_any()
        .downcast_ref::<arrow_array::UnionArray>()
        .ok_or_else(|| {
            CoreError::Unsupported(format!(
                "'{name}' must be a union of typed wrappers, found {:?}.",
                bound.data_type()
            ))
        })
}

/// The array and index holding a bound's value, or `None` when the bound is the
/// union's null branch or the wrapped value itself is null.
fn wrapped_value(bound: &arrow_array::UnionArray, name: &str) -> Result<Option<(ArrayRef, usize)>> {
    if bound.is_empty() {
        return Ok(None);
    }
    let type_id = bound.type_id(0);
    let offset = bound.value_offset(0);
    let child = bound.child(type_id);
    if matches!(child.data_type(), DataType::Null) || child.is_null(offset) {
        return Ok(None);
    }
    let wrapper = child.as_struct_opt().ok_or_else(|| {
        CoreError::Unsupported(format!(
            "'{name}' union branches must wrap their value in a struct, found {:?}.",
            child.data_type()
        ))
    })?;
    let values = wrapper.column_by_name(WRAPPED_VALUE_FIELD).ok_or_else(|| {
        CoreError::Unsupported(format!(
            "'{name}' union branches must carry a '{WRAPPED_VALUE_FIELD}' field."
        ))
    })?;
    if values.is_null(offset) {
        return Ok(None);
    }
    Ok(Some((values.clone(), offset)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::Schema;

    /// The `filesystemMetadata` column type, matching what an HFile-backed
    /// metadata record decodes to: a map of file name to `{size, isDeleted}`.
    fn map_type() -> DataType {
        let value = DataType::Struct(Fields::from(vec![
            Field::new(FILE_SIZE_FIELD, DataType::Int64, false),
            Field::new(FILE_IS_DELETED_FIELD, DataType::Boolean, false),
        ]));
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", value, false),
                ])),
                false,
            )),
            false,
        )
    }

    /// A one-row metadata record of the given partition type, carrying `entries`.
    fn record(record_type: i32, entries: &[(&str, i64, bool)]) -> RecordBatch {
        let map_type = map_type();
        let map = build_file_map(
            &map_type,
            &entries
                .iter()
                .map(|(name, size, is_deleted)| {
                    (
                        (*name).to_string(),
                        FileInfo {
                            size: *size,
                            is_deleted: *is_deleted,
                        },
                    )
                })
                .collect(),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new(TYPE_COLUMN, DataType::Int32, false),
            Field::new(FILESYSTEM_METADATA_COLUMN, map_type, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["city=chennai"])),
                Arc::new(Int32Array::from(vec![record_type])),
                map,
            ],
        )
        .unwrap()
    }

    /// The folded entries, sorted, so an assertion reads as file names and sizes.
    fn folded(
        older: &[(&str, i64, bool)],
        newer: &[(&str, i64, bool)],
    ) -> Vec<(String, i64, bool)> {
        let merged =
            fold_filesystem_metadata(&record(TYPE_FILES, older), &record(TYPE_FILES, newer))
                .unwrap();
        read_file_map(merged.column_by_name(FILESYSTEM_METADATA_COLUMN).unwrap())
            .unwrap()
            .into_iter()
            .map(|(name, info)| (name, info.size, info.is_deleted))
            .collect()
    }

    /// A file present on both sides keeps the larger size. The metadata table
    /// writes a growing file's size as it grows, so the newer entry is not always
    /// the larger one and `max` is not the same rule as "take the newer".
    #[test]
    fn a_file_on_both_sides_keeps_the_larger_size() {
        assert_eq!(
            folded(&[("a.parquet", 900, false)], &[("a.parquet", 100, false)]),
            vec![("a.parquet".to_string(), 900, false)]
        );
        assert_eq!(
            folded(&[("a.parquet", 100, false)], &[("a.parquet", 900, false)]),
            vec![("a.parquet".to_string(), 900, false)]
        );
    }

    /// A tombstone over a live entry removes it outright rather than marking it,
    /// so the file disappears from the listing instead of appearing as deleted.
    #[test]
    fn a_tombstone_cancels_a_live_entry() {
        assert_eq!(
            folded(&[("a.parquet", 900, false)], &[("a.parquet", 0, true)]),
            vec![]
        );
    }

    /// A tombstone over a tombstone keeps the newer one, which is what lets a
    /// later re-delete carry the newer size.
    #[test]
    fn a_tombstone_over_a_tombstone_keeps_the_newer() {
        assert_eq!(
            folded(&[("a.parquet", 1, true)], &[("a.parquet", 2, true)]),
            vec![("a.parquet".to_string(), 2, true)]
        );
    }

    /// A live entry after a tombstone comes back, since a file can be rewritten
    /// under the same name.
    #[test]
    fn a_live_entry_over_a_tombstone_revives_the_file() {
        assert_eq!(
            folded(&[("a.parquet", 5, true)], &[("a.parquet", 900, false)]),
            vec![("a.parquet".to_string(), 900, false)]
        );
    }

    /// Entries on only one side pass through untouched, including a tombstone
    /// the older side never saw.
    #[test]
    fn entries_on_one_side_only_pass_through() {
        assert_eq!(
            folded(
                &[("old.parquet", 10, false)],
                &[("new.parquet", 20, false), ("gone.parquet", 0, true)]
            ),
            vec![
                ("gone.parquet".to_string(), 0, true),
                ("new.parquet".to_string(), 20, false),
                ("old.parquet".to_string(), 10, false),
            ]
        );
    }

    /// The three selection types return the newer record untouched, with no fold
    /// and no allocation.
    #[test]
    fn selection_types_take_the_newer_record() {
        for record_type in [
            4, /* bloom */
            5, /* record index */
            7, /* secondary */
        ] {
            let older = BufferedRecord::new_data(
                "k".to_string(),
                record(record_type, &[("old.parquet", 1, false)]),
                None,
            );
            let newer = BufferedRecord::new_data(
                "k".to_string(),
                record(record_type, &[("new.parquet", 2, false)]),
                None,
            );
            let merged = MetadataPayloadMerger.final_merge(&older, &newer).unwrap();
            let batch = merged.payload.get_record().unwrap();
            assert_eq!(
                read_file_map(batch.column_by_name(FILESYSTEM_METADATA_COLUMN).unwrap())
                    .unwrap()
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>(),
                vec!["new.parquet".to_string()],
                "partition type {record_type} must take the newer record whole"
            );
        }
    }

    // ---- column and partition statistics -----------------------------------

    /// The three union branches these tests need: the null branch every Avro
    /// union carries, a long wrapper, and a string wrapper.
    fn stats_union_fields() -> arrow_schema::UnionFields {
        arrow_schema::UnionFields::try_new(
            vec![0i8, 3, 7],
            vec![
                Field::new("null", DataType::Null, true),
                Field::new(
                    "LongWrapper",
                    DataType::Struct(Fields::from(vec![Field::new(
                        WRAPPED_VALUE_FIELD,
                        DataType::Int64,
                        true,
                    )])),
                    false,
                ),
                Field::new(
                    "StringWrapper",
                    DataType::Struct(Fields::from(vec![Field::new(
                        WRAPPED_VALUE_FIELD,
                        DataType::Utf8,
                        true,
                    )])),
                    false,
                ),
            ],
        )
        .unwrap()
    }

    /// A one-row dense union holding `value` in the branch its type selects, or
    /// the null branch when it is `None`.
    fn bound(value: Option<Bound>) -> ArrayRef {
        let (type_id, long, string) = match value {
            None => (0i8, None, None),
            Some(Bound::Long(v)) => (3, Some(v), None),
            Some(Bound::Text(v)) => (7, None, Some(v)),
        };
        let long_child: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new(WRAPPED_VALUE_FIELD, DataType::Int64, true)]),
                vec![Arc::new(Int64Array::from(long.map_or(vec![], |v| vec![v])))],
                None,
            )
            .unwrap(),
        );
        let string_child: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new(WRAPPED_VALUE_FIELD, DataType::Utf8, true)]),
                vec![Arc::new(StringArray::from(
                    string.map_or(vec![], |v| vec![v]),
                ))],
                None,
            )
            .unwrap(),
        );
        let null_child: ArrayRef = Arc::new(arrow_array::NullArray::new(if type_id == 0 {
            1
        } else {
            0
        }));
        Arc::new(
            arrow_array::UnionArray::try_new(
                stats_union_fields(),
                vec![type_id].into(),
                Some(vec![0i32].into()),
                vec![null_child, long_child, string_child],
            )
            .unwrap(),
        )
    }

    #[derive(Clone, Copy)]
    enum Bound {
        Long(i64),
        Text(&'static str),
    }

    /// A one-row column-statistics record.
    fn stats_record(
        min: Option<Bound>,
        max: Option<Bound>,
        counters: [i64; 4],
        is_deleted: bool,
        is_tight_bound: bool,
    ) -> RecordBatch {
        let union_type = DataType::Union(stats_union_fields(), arrow_schema::UnionMode::Dense);
        let mut fields = vec![
            Field::new(MIN_VALUE_FIELD, union_type.clone(), true),
            Field::new(MAX_VALUE_FIELD, union_type, true),
        ];
        let mut columns: Vec<ArrayRef> = vec![bound(min), bound(max)];
        for (name, value) in COUNTER_FIELDS.iter().zip(counters) {
            fields.push(Field::new(*name, DataType::Int64, true));
            columns.push(Arc::new(Int64Array::from(vec![value])));
        }
        fields.push(Field::new(FILE_IS_DELETED_FIELD, DataType::Boolean, false));
        columns.push(Arc::new(BooleanArray::from(vec![is_deleted])));
        fields.push(Field::new(IS_TIGHT_BOUND_FIELD, DataType::Boolean, false));
        columns.push(Arc::new(BooleanArray::from(vec![is_tight_bound])));

        let stats = StructArray::try_new(Fields::from(fields), columns, None).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new(TYPE_COLUMN, DataType::Int32, false),
            Field::new(COLUMN_STATS_COLUMN, stats.data_type().clone(), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![TYPE_COLUMN_STATS])),
                Arc::new(stats),
            ],
        )
        .unwrap()
    }

    /// The folded statistics, read back as (min, max, counters).
    fn folded_stats(
        older: &RecordBatch,
        newer: &RecordBatch,
    ) -> (Option<String>, Option<String>, Vec<i64>) {
        let merged = fold_column_stats(older, newer).unwrap();
        let stats = merged
            .column_by_name(COLUMN_STATS_COLUMN)
            .unwrap()
            .as_struct()
            .clone();
        let render = |name: &str| -> Option<String> {
            let union = bound_union(stats.column_by_name(name).unwrap(), name).unwrap();
            wrapped_value(union, name).unwrap().map(|(values, idx)| {
                arrow_cast::display::array_value_to_string(&values, idx).unwrap()
            })
        };
        let counters = COUNTER_FIELDS
            .iter()
            .map(|name| {
                stats
                    .column_by_name(name)
                    .unwrap()
                    .as_primitive::<arrow_array::types::Int64Type>()
                    .value(0)
            })
            .collect();
        (render(MIN_VALUE_FIELD), render(MAX_VALUE_FIELD), counters)
    }

    /// The bounds widen to cover both records and the four counters are summed.
    /// The newer record is not simply taken, which is what separates this from
    /// commit-time ordering.
    #[test]
    fn stats_bounds_widen_and_counters_sum() {
        let older = stats_record(
            Some(Bound::Long(5)),
            Some(Bound::Long(50)),
            [1, 2, 3, 4],
            false,
            false,
        );
        let newer = stats_record(
            Some(Bound::Long(9)),
            Some(Bound::Long(90)),
            [10, 20, 30, 40],
            false,
            false,
        );
        assert_eq!(
            folded_stats(&older, &newer),
            (
                Some("5".to_string()),
                Some("90".to_string()),
                vec![11, 22, 33, 44]
            )
        );
        // And the other way round, so the rule is min/max rather than "take the
        // older bound" or "take the newer".
        assert_eq!(
            folded_stats(&newer, &older),
            (
                Some("5".to_string()),
                Some("90".to_string()),
                vec![11, 22, 33, 44]
            )
        );
    }

    /// A string bound compares lexicographically, through the same union branch.
    #[test]
    fn stats_bounds_compare_within_the_union_branch() {
        let older = stats_record(
            Some(Bound::Text("m")),
            Some(Bound::Text("m")),
            [0; 4],
            false,
            false,
        );
        let newer = stats_record(
            Some(Bound::Text("b")),
            Some(Bound::Text("z")),
            [0; 4],
            false,
            false,
        );
        let (min, max, _) = folded_stats(&older, &newer);
        assert_eq!((min, max), (Some("b".to_string()), Some("z".to_string())));
    }

    /// A bound on the union's null branch loses to a real value rather than
    /// making the merged bound null.
    #[test]
    fn a_null_bound_loses_to_a_value() {
        let older = stats_record(None, None, [0; 4], false, false);
        let newer = stats_record(
            Some(Bound::Long(7)),
            Some(Bound::Long(7)),
            [0; 4],
            false,
            false,
        );
        assert_eq!(
            folded_stats(&older, &newer).0,
            Some("7".to_string()),
            "a null older bound must not erase the newer value"
        );
        assert_eq!(
            folded_stats(&newer, &older).0,
            Some("7".to_string()),
            "a null newer bound must not erase the older value"
        );
    }

    /// A tight-bound newer record supersedes the older outright: no widening and
    /// no summing. This is the rule the metadata table's partition statistics
    /// actually take, since every record it writes is tight-bound.
    #[test]
    fn a_tight_bound_newer_record_supersedes() {
        let older = stats_record(
            Some(Bound::Long(1)),
            Some(Bound::Long(100)),
            [9, 9, 9, 9],
            false,
            false,
        );
        let newer = stats_record(
            Some(Bound::Long(5)),
            Some(Bound::Long(50)),
            [1, 1, 1, 1],
            false,
            true,
        );
        assert_eq!(
            folded_stats(&older, &newer),
            (
                Some("5".to_string()),
                Some("50".to_string()),
                vec![1, 1, 1, 1]
            )
        );
    }

    /// A tombstone on either side overwrites, ahead of the tight-bound rule.
    #[test]
    fn a_deleted_stats_record_on_either_side_overwrites() {
        let live = stats_record(
            Some(Bound::Long(1)),
            Some(Bound::Long(100)),
            [9, 9, 9, 9],
            false,
            false,
        );
        let deleted = stats_record(
            Some(Bound::Long(5)),
            Some(Bound::Long(50)),
            [1, 1, 1, 1],
            true,
            false,
        );
        for (older, newer) in [(&live, &deleted), (&deleted, &live)] {
            let (_, _, counters) = folded_stats(older, newer);
            assert_eq!(
                counters,
                COUNTER_FIELDS
                    .iter()
                    .enumerate()
                    .map(|(i, _)| {
                        newer
                            .column_by_name(COLUMN_STATS_COLUMN)
                            .unwrap()
                            .as_struct()
                            .column(2 + i)
                            .as_primitive::<arrow_array::types::Int64Type>()
                            .value(0)
                    })
                    .collect::<Vec<_>>(),
                "a tombstone must overwrite rather than sum"
            );
        }
    }

    /// A bound whose union branch differs between the two records is a type
    /// change this crate does not reconcile, and must error rather than pick one
    /// side and report a bound of the wrong type.
    #[test]
    fn a_bound_that_changes_type_is_refused() {
        let older = stats_record(
            Some(Bound::Long(5)),
            Some(Bound::Long(5)),
            [0; 4],
            false,
            false,
        );
        let newer = stats_record(
            Some(Bound::Text("b")),
            Some(Bound::Text("b")),
            [0; 4],
            false,
            false,
        );
        let err = fold_column_stats(&older, &newer).expect_err("a type change must be refused");
        assert!(
            format!("{err:?}").contains("changes type between the two records"),
            "the error must name the type change, got: {err:?}"
        );
    }

    /// The payload class is the discriminator, not the strategy id: every
    /// payload-based custom table carries the same all-zeros id.
    #[test]
    fn only_the_metadata_payload_selects_a_merger() {
        let with = |pairs: &[(&str, &str)]| -> Option<CustomMerger> {
            resolve_custom_merger(
                &pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
            )
        };
        assert_eq!(
            with(&[
                ("hoodie.compaction.payload.class", METADATA_PAYLOAD_CLASS),
                ("hoodie.record.merge.strategy.id", PAYLOAD_BASED_STRATEGY_ID),
            ]),
            Some(CustomMerger::MetadataPayload)
        );
        // The same all-zeros id with someone else's payload must not select it.
        assert_eq!(
            with(&[
                ("hoodie.compaction.payload.class", "com.example.Payload"),
                ("hoodie.record.merge.strategy.id", PAYLOAD_BASED_STRATEGY_ID),
            ]),
            None
        );
        // An id naming a merger of its own is not deferring to the payload.
        assert_eq!(
            with(&[
                ("hoodie.compaction.payload.class", METADATA_PAYLOAD_CLASS),
                (
                    "hoodie.record.merge.strategy.id",
                    "eeb8d96f-b1e4-49fd-bbf8-28ac514178e5"
                ),
            ]),
            None
        );
        // No id at all still defers to the payload.
        assert_eq!(
            with(&[("hoodie.compaction.payload.class", METADATA_PAYLOAD_CLASS)]),
            Some(CustomMerger::MetadataPayload)
        );
        assert_eq!(with(&[]), None);
        // A table written before version 8 states the strategy under the older
        // key. Reading only the newer one makes a real custom merger look like
        // no merger at all, which admits a table this crate must refuse.
        assert_eq!(
            with(&[
                ("hoodie.compaction.payload.class", METADATA_PAYLOAD_CLASS),
                (
                    "hoodie.compaction.record.merger.strategy",
                    "eeb8d96f-b1e4-49fd-bbf8-28ac514178e5"
                ),
            ]),
            None
        );
        // And the payload class under the keys a pre-v8 table writes it under.
        for key in [
            "hoodie.datasource.write.payload.class",
            "hoodie.table.legacy.payload.class",
        ] {
            assert_eq!(
                with(&[(key, METADATA_PAYLOAD_CLASS)]),
                Some(CustomMerger::MetadataPayload),
                "the payload class must be read from '{key}' too"
            );
        }
    }
}
