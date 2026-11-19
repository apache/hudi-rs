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
//! Avro encoding for metadata table records written by hudi-rs.
//!
//! Uses the vendored Java `HoodieMetadata.avsc` (`org.apache.hudi.avro.model`).

use std::collections::HashMap;
use std::sync::OnceLock;

use apache_avro::Schema;
use apache_avro::types::Value;

use crate::Result;
use crate::error::CoreError;
use crate::metadata::meta_field::MetaField;
use crate::metadata::table::records::{FilesPartitionRecord, MetadataRecordType};
use crate::schema::avsc::strip_avro_line_comments;

/// JSON text of the metadata table writer schema, embedded into HFile file-info
/// under key `schema` and into log block headers.
///
/// Matches the schema Java writes metadata table files with:
/// `HoodieMetadataRecord.SCHEMA$` with the five `_hoodie_*` meta fields
/// prepended as nullable strings, always null. Readers merge a file slice to its
/// base file's schema, so every block in a slice must carry the same fields. String fields and map key types of the record carry
/// `"avro.java.string":"String"` so Spark's `GenericDatumReader` materializes
/// `java.lang.String` instead of `Utf8` (required by `constructFilesMetadataPayload`
/// and `fetchBaseFileRecordsByKeys`).
pub fn hoodie_metadata_schema_json() -> Result<&'static str> {
    static JSON: OnceLock<std::result::Result<String, String>> = OnceLock::new();
    JSON.get_or_init(|| {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/HoodieMetadata.avsc"
        ));
        let cleaned = strip_avro_line_comments(raw);
        let json = cleaned
            .find('{')
            .map(|i| cleaned[i..].trim().to_string())
            .unwrap_or(cleaned);
        inject_avro_java_string_props(&json).and_then(|json| prepend_meta_fields(&json))
    })
    .as_deref()
    .map_err(|e| {
        crate::error::CoreError::MetadataTable(format!(
            "vendored HoodieMetadata.avsc failed to annotate: {e}"
        ))
    })
}

/// Prepend the meta fields in Java `HoodieAvroUtils.addMetadataFields` form.
fn prepend_meta_fields(schema_json: &str) -> std::result::Result<String, String> {
    let mut value: serde_json::Value =
        serde_json::from_str(schema_json).map_err(|e| e.to_string())?;
    let fields = value
        .get_mut("fields")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("metadata schema has no fields")?;
    let meta_fields = MetaField::field_names().into_iter().map(|name| {
        serde_json::json!({"name": name, "type": ["null", "string"], "doc": "", "default": null})
    });
    fields.splice(0..0, meta_fields);
    serde_json::to_string(&value).map_err(|e| e.to_string())
}

/// Annotate Avro JSON so Java readers decode strings as `java.lang.String`.
fn inject_avro_java_string_props(schema_json: &str) -> std::result::Result<String, String> {
    let mut value: serde_json::Value =
        serde_json::from_str(schema_json).map_err(|e| e.to_string())?;
    inject_avro_java_string_props_value(&mut value);
    serde_json::to_string(&value).map_err(|e| e.to_string())
}

fn inject_avro_java_string_props_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            match map.get("type").cloned() {
                Some(serde_json::Value::String(t)) if t == "string" => {
                    // Avoid re-expanding `{"type":"string","avro.java.string":"String"}`.
                    if !map.contains_key("avro.java.string") {
                        map.insert(
                            "type".to_string(),
                            serde_json::json!({"type": "string", "avro.java.string": "String"}),
                        );
                    }
                }
                Some(serde_json::Value::String(t)) if t == "map" => {
                    map.entry("avro.java.string".to_string())
                        .or_insert_with(|| serde_json::Value::String("String".to_string()));
                }
                _ => {}
            }
            for child in map.values_mut() {
                inject_avro_java_string_props_value(child);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                if item.as_str() == Some("string") {
                    *item = serde_json::json!({"type": "string", "avro.java.string": "String"});
                } else {
                    inject_avro_java_string_props_value(item);
                }
            }
        }
        _ => {}
    }
}

/// Alias kept for call sites that embed the files-partition schema into HFiles.
pub fn files_metadata_avro_schema_json() -> Result<&'static str> {
    hoodie_metadata_schema_json()
}

/// Alias kept for call sites that embed the record_index schema into HFiles.
pub fn record_index_metadata_avro_schema_json() -> Result<&'static str> {
    hoodie_metadata_schema_json()
}

pub(crate) fn metadata_schema() -> Result<&'static Schema> {
    static SCHEMA: OnceLock<std::result::Result<Schema, String>> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            hoodie_metadata_schema_json()
                .map_err(|e| e.to_string())
                .and_then(|json| Schema::parse_str(json).map_err(|e| e.to_string()))
        })
        .as_ref()
        .map_err(|e| CoreError::MetadataTable(format!("metadata writer schema: {e}")))
}

/// Encode a `HoodieMetadataRecord` with null meta fields, per [`metadata_schema`].
fn encode_metadata_record(fields: Vec<(String, Value)>) -> Result<Vec<u8>> {
    let record = MetaField::field_names()
        .into_iter()
        .map(|name| (name.to_string(), null_union()))
        .chain(fields)
        .collect();
    apache_avro::to_avro_datum(metadata_schema()?, Value::Record(record)).map_err(CoreError::from)
}

fn null_union() -> Value {
    Value::Union(0, Box::new(Value::Null))
}

/// A file entry stored in a files-partition metadata record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesMetadataEntry {
    /// File name or partition name.
    pub name: String,
    /// File size in bytes.
    pub size: i64,
    /// Whether this entry is a deletion marker.
    pub is_deleted: bool,
}

/// A comparable column statistic value wrapped for MDT Avro payloads.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum ColumnStatValue {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    String(String),
    Date(i32),
    TimeMicros(i64),
    TimestampMicros(i64),
    /// Timezone-less timestamp micros (Java `LOCAL_TIMESTAMP_MICROS`).
    LocalTimestampMicros(i64),
    /// Decimal as its unscaled value (Java `BigDecimal`).
    Decimal {
        unscaled: i128,
        precision: u8,
        scale: i8,
    },
}

/// MDT stats index version: V1 (tv8, logical wrappers, no `valueType`) vs V2
/// (tv9+, primitive wrappers + `HoodieValueTypeInfo`). Java
/// `HoodieIndexVersion.getCurrentVersion`: COLUMN_STATS/PARTITION_STATS are V2
/// from table version 9.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsIndexVersion {
    V1,
    V2,
}

impl ColumnStatValue {
    /// Java `ValueType` enum ordinal (stats/ValueType.java declaration order):
    /// V1=0 NULL=1 BOOLEAN=2 INT=3 LONG=4 FLOAT=5 DOUBLE=6 STRING=7 BYTES=8
    /// FIXED=9 DECIMAL=10 UUID=11 DATE=12 TIME_MILLIS=13 TIME_MICROS=14
    /// TIMESTAMP_MILLIS=15 TIMESTAMP_MICROS=16 TIMESTAMP_NANOS=17
    /// LOCAL_TIMESTAMP_{MILLIS,MICROS,NANOS}=18/19/20.
    fn value_type_ordinal(&self) -> i32 {
        match self {
            ColumnStatValue::Boolean(_) => 2,
            ColumnStatValue::Int(_) => 3,
            ColumnStatValue::Long(_) => 4,
            ColumnStatValue::Float(_) => 5,
            ColumnStatValue::Double(_) => 6,
            ColumnStatValue::String(_) => 7,
            ColumnStatValue::Bytes(_) => 8,
            ColumnStatValue::Date(_) => 12,
            ColumnStatValue::TimeMicros(_) => 14,
            ColumnStatValue::TimestampMicros(_) => 16,
            ColumnStatValue::LocalTimestampMicros(_) => 19,
            ColumnStatValue::Decimal { .. } => 10,
        }
    }
}

/// Column / partition statistics payload (Avro `HoodieMetadataColumnStats`).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStatsMetadata {
    pub file_name: String,
    pub column_name: String,
    pub min_value: Option<ColumnStatValue>,
    pub max_value: Option<ColumnStatValue>,
    pub value_count: i64,
    pub null_count: i64,
    pub total_size: i64,
    pub total_uncompressed_size: i64,
    pub is_deleted: bool,
    pub is_tight_bound: bool,
    /// Java `ValueType` ordinal decoded from a V2 record's `valueType`;
    /// `None` for V1 records. On encode, only a millisecond type is kept (its
    /// values are written back as millis); any other is derived from the values.
    pub decoded_value_type_ordinal: Option<i32>,
}

/// Encode the `__all_partitions__` files-partition record.
pub fn encode_all_partitions(partitions: impl IntoIterator<Item = String>) -> Result<Vec<u8>> {
    let entries: Vec<FilesMetadataEntry> = partitions
        .into_iter()
        .map(|name| FilesMetadataEntry {
            name,
            size: 0,
            is_deleted: false,
        })
        .collect();
    encode_files_record(
        FilesPartitionRecord::ALL_PARTITIONS_KEY,
        MetadataRecordType::AllPartitions,
        entries,
    )
}

/// Encode a files-partition metadata record.
///
/// `key` is the HFile row key and is intentionally **not** written into the Avro
/// payload. Java's `HoodieAvroHFileWriter` clears the Avro `key` field to `""` and
/// stores the real key only in the HFile key; on read,
/// `HoodieAvroHFileReaderImplBase.deserialize` reinjects a Java `String` from the
/// HFile key. Embedding a non-empty Avro key makes GenericDatumReader produce
/// `Utf8`, and Spark then `ClassCastException`s in `fetchBaseFileRecordsByKeys`.
pub fn encode_files_record(
    key: &str,
    record_type: MetadataRecordType,
    entries: impl IntoIterator<Item = FilesMetadataEntry>,
) -> Result<Vec<u8>> {
    let _hfile_row_key = key;
    let mut filesystem_metadata = HashMap::new();
    for entry in entries {
        filesystem_metadata.insert(
            entry.name,
            Value::Record(vec![
                ("size".to_string(), Value::Long(entry.size)),
                ("isDeleted".to_string(), Value::Boolean(entry.is_deleted)),
            ]),
        );
    }
    encode_metadata_record(vec![
        ("key".to_string(), Value::String(String::new())),
        ("type".to_string(), Value::Int(record_type as i32)),
        (
            "filesystemMetadata".to_string(),
            Value::Union(1, Box::new(Value::Map(filesystem_metadata))),
        ),
        ("BloomFilterMetadata".to_string(), null_union()),
        ("ColumnStatsMetadata".to_string(), null_union()),
        ("recordIndexMetadata".to_string(), null_union()),
        ("SecondaryIndexMetadata".to_string(), null_union()),
    ])
}

/// A location stored in the record_index metadata partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordIndexEntry {
    pub record_key: String,
    pub partition_path: String,
    pub file_id: String,
    /// Instant time as epoch millis (Hudi RLI wire format).
    pub instant_time_millis: i64,
    /// When true, this entry deletes the key from RLI.
    pub is_deleted: bool,
}

/// Encode a record-index metadata record (type = 5).
///
/// Deletes must not use this path — emit a delete log block instead. Null
/// `recordIndexMetadata` NPEs Java's RLI reader.
///
/// Written straight into Avro binary rather than through a generic `Value`
/// tree, since the record's shape is fixed and a write encodes one entry per
/// row. The tests hold this encoding to the generic `Value` encoding.
pub fn encode_record_index_entry(entry: &RecordIndexEntry) -> Result<Vec<u8>> {
    if entry.is_deleted {
        return Err(CoreError::MetadataTable(
            "RLI deletes must be written as delete log blocks, not null payloads".to_string(),
        ));
    }
    let (high, low, file_index, file_id_str, encoding) =
        encode_file_id_for_record_index(&entry.file_id);
    // Field order and union branches follow HoodieMetadata.avsc: a union is
    // its branch index then the value, `null` encodes as nothing.
    let mut out = Vec::with_capacity(56 + entry.partition_path.len() + file_id_str.len());
    for _ in MetaField::field_names() {
        put_int(&mut out, 0); // _hoodie_* meta fields: null
    }
    put_string(&mut out, ""); // key: lives on the HFile key only
    put_int(&mut out, MetadataRecordType::RecordIndex as i32);
    put_int(&mut out, 0); // filesystemMetadata: null
    put_int(&mut out, 0); // BloomFilterMetadata: null
    put_int(&mut out, 0); // ColumnStatsMetadata: null
    put_int(&mut out, 1); // recordIndexMetadata: HoodieRecordIndexInfo
    put_int(&mut out, 1);
    put_string(&mut out, &entry.partition_path);
    put_int(&mut out, 1);
    put_long(&mut out, high);
    put_int(&mut out, 1);
    put_long(&mut out, low);
    put_int(&mut out, 1);
    put_int(&mut out, file_index);
    put_int(&mut out, 1);
    put_string(&mut out, &file_id_str);
    put_int(&mut out, 1);
    put_long(&mut out, entry.instant_time_millis);
    put_int(&mut out, encoding);
    put_int(&mut out, 0); // position: null
    put_int(&mut out, 0); // SecondaryIndexMetadata: null
    Ok(out)
}

/// Avro zig-zag varint for a `long`.
fn put_long(out: &mut Vec<u8>, value: i64) {
    let mut zigzag = ((value << 1) ^ (value >> 63)) as u64;
    loop {
        let byte = (zigzag & 0x7f) as u8;
        zigzag >>= 7;
        if zigzag == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Avro zig-zag varint for an `int`.
fn put_int(out: &mut Vec<u8>, value: i32) {
    put_long(out, i64::from(value));
}

/// Avro `string`: byte length as a varint, then the UTF-8 bytes.
fn put_string(out: &mut Vec<u8>, value: &str) {
    put_long(out, value.len() as i64);
    out.extend_from_slice(value.as_bytes());
}

/// The same record through the generic Avro encoder; the reference for
/// [`encode_record_index_entry`].
#[cfg(test)]
fn encode_record_index_entry_generic(entry: &RecordIndexEntry) -> Result<Vec<u8>> {
    let (high, low, file_index, file_id_str, encoding) =
        encode_file_id_for_record_index(&entry.file_id);
    let rli_payload = Value::Union(
        1,
        Box::new(Value::Record(vec![
            (
                "partitionName".to_string(),
                Value::Union(1, Box::new(Value::String(entry.partition_path.clone()))),
            ),
            (
                "fileIdHighBits".to_string(),
                Value::Union(1, Box::new(Value::Long(high))),
            ),
            (
                "fileIdLowBits".to_string(),
                Value::Union(1, Box::new(Value::Long(low))),
            ),
            (
                "fileIndex".to_string(),
                Value::Union(1, Box::new(Value::Int(file_index))),
            ),
            (
                "fileId".to_string(),
                Value::Union(1, Box::new(Value::String(file_id_str))),
            ),
            (
                "instantTime".to_string(),
                Value::Union(1, Box::new(Value::Long(entry.instant_time_millis))),
            ),
            // 0 = UUID bits; 1 = raw fileId string (Java HoodieRecordIndexInfo).
            ("fileIdEncoding".to_string(), Value::Int(encoding)),
            ("position".to_string(), null_union()),
        ])),
    );
    // Empty Avro key — real key lives only on the HFile key (see encode_files_record).
    encode_metadata_record(vec![
        ("key".to_string(), Value::String(String::new())),
        (
            "type".to_string(),
            Value::Int(MetadataRecordType::RecordIndex as i32),
        ),
        ("filesystemMetadata".to_string(), null_union()),
        ("BloomFilterMetadata".to_string(), null_union()),
        ("ColumnStatsMetadata".to_string(), null_union()),
        ("recordIndexMetadata".to_string(), rli_payload),
        ("SecondaryIndexMetadata".to_string(), null_union()),
    ])
}

/// Match Java `HoodieMetadataPayload.createRecordIndexUpdate` fileId packing.
fn encode_file_id_for_record_index(file_id: &str) -> (i64, i64, i32, String, i32) {
    if let Some((uuid, file_index)) = parse_uuid_file_id(file_id) {
        let (high, low) = uuid.as_u64_pair();
        (high as i64, low as i64, file_index, String::new(), 0)
    } else {
        (-1, -1, -1, file_id.to_string(), 1)
    }
}

/// Parse `{uuid}` or `{uuid}-{fileIndex}` as used by Java FSUtils file ids.
fn parse_uuid_file_id(file_id: &str) -> Option<(uuid::Uuid, i32)> {
    if file_id.len() == 36 {
        return uuid::Uuid::parse_str(file_id).ok().map(|u| (u, -1));
    }
    let idx = file_id.rfind('-')?;
    let uuid = uuid::Uuid::parse_str(&file_id[..idx]).ok()?;
    let file_index: i32 = file_id[idx + 1..].parse().ok()?;
    Some((uuid, file_index))
}

/// Decode a record-index Avro payload. Returns `None` when the entry is a delete.
///
/// `writer_schema` is the schema of the container the bytes were read from
/// (mixed-writer tables have differing HoodieMetadataRecord schemas per
/// container); falls back to our vendored schema when unknown.
///
/// When that writer schema has the same record-index binary layout as the
/// vendored `HoodieMetadata.avsc` (Java `HoodieRecordIndexInfo` field order
/// and types), entries are read straight from the Avro bytes. Otherwise, and
/// if the direct read fails, the generic `from_avro_datum` decoder is used.
pub fn decode_record_index_entry(
    key: &str,
    bytes: &[u8],
    writer_schema: Option<&Schema>,
) -> Result<Option<RecordIndexEntry>> {
    let schema = match writer_schema {
        Some(s) => s,
        None if bytes.is_empty() => return Ok(None),
        None => metadata_schema()?,
    };
    decode_record_index_entry_with_layout(
        key,
        bytes,
        schema,
        record_index_writer_layout_matches(schema),
    )
}

/// Decode using a layout-check result already computed for this container.
pub(crate) fn decode_record_index_entry_with_layout(
    key: &str,
    bytes: &[u8],
    schema: &Schema,
    layout_matches: bool,
) -> Result<Option<RecordIndexEntry>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    if layout_matches && let Ok(entry) = decode_record_index_entry_direct(key, bytes) {
        return Ok(entry);
    }
    decode_record_index_entry_generic(key, bytes, schema)
}

/// Whether `schema` has the same record-index layout as the vendored
/// `HoodieMetadata.avsc`.
///
/// Field names and order are taken from the vendored schema. Typed top-level
/// fields use `Schema` equality, which in apache-avro compares field types in
/// order and ignores names, doc and default; names are compared separately.
/// The other index payloads only need a null-leading union: a null branch is
/// one byte regardless of the other branch, and record-index records leave
/// those payloads null. `recordIndexMetadata` compares each
/// `HoodieRecordIndexInfo` field name and type, because `Schema` equality
/// would accept a swap of `fileIdHighBits` and `fileIdLowBits`.
pub(crate) fn record_index_writer_layout_matches(schema: &Schema) -> bool {
    let Ok(vendored) = metadata_schema() else {
        return false;
    };
    if std::ptr::eq(schema, vendored) {
        return true;
    }
    let (Schema::Record(got), Schema::Record(want)) = (schema, vendored) else {
        return false;
    };
    if got.fields.len() != want.fields.len() {
        return false;
    }
    got.fields.iter().zip(&want.fields).all(|(got, want)| {
        if got.name != want.name {
            return false;
        }
        match got.name.as_str() {
            "filesystemMetadata"
            | "BloomFilterMetadata"
            | "ColumnStatsMetadata"
            | "SecondaryIndexMetadata" => is_null_leading_union(&got.schema),
            "recordIndexMetadata" => record_index_info_layout_matches(&got.schema, &want.schema),
            _ => got.schema == want.schema,
        }
    })
}

fn is_null_leading_union(schema: &Schema) -> bool {
    match schema {
        Schema::Union(u) => {
            let variants = u.variants();
            !variants.is_empty() && matches!(variants[0], Schema::Null)
        }
        _ => false,
    }
}

fn record_index_info_layout_matches(got: &Schema, want: &Schema) -> bool {
    let (Some(got_fields), Some(want_fields)) = (
        hoodie_record_index_info_fields(got),
        hoodie_record_index_info_fields(want),
    ) else {
        return false;
    };
    got_fields.len() == want_fields.len()
        && got_fields
            .iter()
            .zip(want_fields)
            .all(|(got, want)| got.name == want.name && got.schema == want.schema)
}

fn hoodie_record_index_info_fields(schema: &Schema) -> Option<&[apache_avro::schema::RecordField]> {
    let Schema::Union(u) = schema else {
        return None;
    };
    let variants = u.variants();
    if variants.len() != 2 || !matches!(variants[0], Schema::Null) {
        return None;
    }
    match &variants[1] {
        Schema::Record(record) => Some(record.fields.as_slice()),
        _ => None,
    }
}

/// Inverse of [`put_long`].
fn get_long(src: &mut &[u8]) -> Result<i64> {
    let mut zigzag: u64 = 0;
    let mut shift = 0;
    loop {
        let Some((&byte, rest)) = src.split_first() else {
            return Err(CoreError::MetadataTable(
                "truncated Avro varint in record_index payload".to_string(),
            ));
        };
        *src = rest;
        zigzag |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(((zigzag >> 1) as i64) ^ -((zigzag & 1) as i64));
        }
        shift += 7;
        if shift >= 64 {
            return Err(CoreError::MetadataTable(
                "Avro varint in record_index payload exceeds a long".to_string(),
            ));
        }
    }
}

fn get_int(src: &mut &[u8]) -> Result<i32> {
    let value = get_long(src)?;
    i32::try_from(value).map_err(|_| {
        CoreError::MetadataTable(format!(
            "Avro int in record_index payload is out of range: {value}"
        ))
    })
}

fn get_string(src: &mut &[u8]) -> Result<String> {
    let len = get_long(src)?;
    let len = usize::try_from(len).map_err(|_| {
        CoreError::MetadataTable(format!(
            "Avro string length in record_index payload is negative: {len}"
        ))
    })?;
    if src.len() < len {
        return Err(CoreError::MetadataTable(
            "truncated Avro string in record_index payload".to_string(),
        ));
    }
    let (head, rest) = src.split_at(len);
    *src = rest;
    String::from_utf8(head.to_vec()).map_err(|e| {
        CoreError::MetadataTable(format!("record_index Avro string is not valid UTF-8: {e}"))
    })
}

/// Read a `[null, T]` union: branch 0 is `None`, branch 1 is `T`.
fn get_nullable<T>(
    src: &mut &[u8],
    read: impl FnOnce(&mut &[u8]) -> Result<T>,
) -> Result<Option<T>> {
    match get_int(src)? {
        0 => Ok(None),
        1 => read(src).map(Some),
        branch => Err(CoreError::MetadataTable(format!(
            "record_index union branch {branch} is not [null, value]"
        ))),
    }
}

fn skip_nullable_string(src: &mut &[u8]) -> Result<()> {
    get_nullable(src, |s| get_string(s).map(|_| ())).map(|_| ())
}

fn skip_nullable_long(src: &mut &[u8]) -> Result<()> {
    get_nullable(src, |s| get_long(s).map(|_| ())).map(|_| ())
}

fn expect_null_union(src: &mut &[u8], name: &str) -> Result<()> {
    match get_int(src)? {
        0 => Ok(()),
        branch => Err(CoreError::MetadataTable(format!(
            "direct record_index decode expected {name} to be null, got union branch {branch}"
        ))),
    }
}

fn resolve_file_id(
    file_id: String,
    file_id_encoding: i32,
    file_id_high: Option<i64>,
    file_id_low: Option<i64>,
    file_index: Option<i32>,
) -> String {
    if file_id.is_empty()
        && file_id_encoding == 0
        && let (Some(high), Some(low), Some(idx)) = (file_id_high, file_id_low, file_index)
    {
        let uuid = uuid_from_u64_pair(high as u64, low as u64);
        if idx >= 0 {
            format!("{uuid}-{idx}")
        } else {
            uuid
        }
    } else {
        file_id
    }
}

/// Decode against the vendored record-index layout, without resolving a schema.
fn decode_record_index_entry_direct(key: &str, bytes: &[u8]) -> Result<Option<RecordIndexEntry>> {
    let mut src = bytes;
    // Five `_hoodie_*` meta fields prepended by `prepend_meta_fields`.
    for _ in 0..5 {
        skip_nullable_string(&mut src)?;
    }
    let _avro_key = get_string(&mut src)?;
    let _typ = get_int(&mut src)?;
    expect_null_union(&mut src, "filesystemMetadata")?;
    expect_null_union(&mut src, "BloomFilterMetadata")?;
    expect_null_union(&mut src, "ColumnStatsMetadata")?;
    let entry = match get_int(&mut src)? {
        0 => {
            expect_null_union(&mut src, "SecondaryIndexMetadata")?;
            None
        }
        1 => {
            let partition_path = get_nullable(&mut src, get_string)?.unwrap_or_default();
            let file_id_high = get_nullable(&mut src, get_long)?;
            let file_id_low = get_nullable(&mut src, get_long)?;
            let file_index = get_nullable(&mut src, get_int)?;
            let file_id = get_nullable(&mut src, get_string)?.unwrap_or_default();
            let instant_time_millis = get_nullable(&mut src, get_long)?.unwrap_or(0);
            let file_id_encoding = get_int(&mut src)?;
            skip_nullable_long(&mut src)?; // position is unused on lookup
            expect_null_union(&mut src, "SecondaryIndexMetadata")?;
            Some(RecordIndexEntry {
                record_key: key.to_string(),
                partition_path,
                file_id: resolve_file_id(
                    file_id,
                    file_id_encoding,
                    file_id_high,
                    file_id_low,
                    file_index,
                ),
                instant_time_millis,
                is_deleted: false,
            })
        }
        branch => {
            return Err(CoreError::MetadataTable(format!(
                "recordIndexMetadata union branch {branch} is not [null, HoodieRecordIndexInfo]"
            )));
        }
    };
    if !src.is_empty() {
        return Err(CoreError::MetadataTable(format!(
            "record_index payload has {} unexpected trailing bytes",
            src.len()
        )));
    }
    Ok(entry)
}

fn decode_record_index_entry_generic(
    key: &str,
    bytes: &[u8],
    schema: &Schema,
) -> Result<Option<RecordIndexEntry>> {
    let value = apache_avro::from_avro_datum(schema, &mut &bytes[..], None)?;
    let Value::Record(fields) = value else {
        return Err(CoreError::MetadataTable(
            "record_index payload must be an Avro record".to_string(),
        ));
    };
    let mut partition_path = String::new();
    let mut file_id = String::new();
    let mut file_id_high = None;
    let mut file_id_low = None;
    let mut file_index = None;
    let mut file_id_encoding = 0_i32;
    let mut instant_time_millis = 0_i64;
    let mut has_info = false;
    for (name, field_value) in fields {
        if name != "recordIndexMetadata" {
            continue;
        }
        match field_value {
            Value::Union(0, _) | Value::Null => return Ok(None),
            Value::Union(1, inner) => {
                let Value::Record(info_fields) = *inner else {
                    return Err(CoreError::MetadataTable(
                        "recordIndexMetadata must be a record".to_string(),
                    ));
                };
                has_info = true;
                for (fname, fval) in info_fields {
                    match (fname.as_str(), fval) {
                        ("partitionName", Value::Union(1, v)) => {
                            if let Value::String(s) = *v {
                                partition_path = s;
                            }
                        }
                        ("fileId", Value::Union(1, v)) => {
                            if let Value::String(s) = *v {
                                file_id = s;
                            }
                        }
                        ("fileIdHighBits", Value::Union(1, v)) => {
                            if let Value::Long(v) = *v {
                                file_id_high = Some(v);
                            }
                        }
                        ("fileIdLowBits", Value::Union(1, v)) => {
                            if let Value::Long(v) = *v {
                                file_id_low = Some(v);
                            }
                        }
                        ("fileIndex", Value::Union(1, v)) => {
                            if let Value::Int(v) = *v {
                                file_index = Some(v);
                            }
                        }
                        ("fileIdEncoding", Value::Int(v)) => file_id_encoding = v,
                        ("instantTime", Value::Union(1, v)) => {
                            if let Value::Long(ts) = *v {
                                instant_time_millis = ts;
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    if !has_info {
        return Ok(None);
    }
    Ok(Some(RecordIndexEntry {
        record_key: key.to_string(),
        partition_path,
        file_id: resolve_file_id(
            file_id,
            file_id_encoding,
            file_id_high,
            file_id_low,
            file_index,
        ),
        instant_time_millis,
        is_deleted: false,
    }))
}

/// Decode a column_stats / partition_stats Avro payload.
///
/// Returns `None` when the record carries no `ColumnStatsMetadata` (e.g. a
/// files or record_index record read from a mixed scan).
pub fn decode_column_stats_entry(
    bytes: &[u8],
    writer_schema: Option<&Schema>,
) -> Result<Option<ColumnStatsMetadata>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let schema = match writer_schema {
        Some(s) => s,
        None => metadata_schema()?,
    };
    let value = apache_avro::from_avro_datum(schema, &mut &bytes[..], None)?;
    let Value::Record(fields) = value else {
        return Err(CoreError::MetadataTable(
            "column_stats payload must be an Avro record".to_string(),
        ));
    };
    for (name, field_value) in fields {
        if name != "ColumnStatsMetadata" {
            continue;
        }
        match field_value {
            Value::Union(0, _) | Value::Null => return Ok(None),
            Value::Union(1, inner) => {
                let Value::Record(stat_fields) = *inner else {
                    return Err(CoreError::MetadataTable(
                        "ColumnStatsMetadata must be a record".to_string(),
                    ));
                };
                let mut out = ColumnStatsMetadata {
                    file_name: String::new(),
                    column_name: String::new(),
                    min_value: None,
                    max_value: None,
                    value_count: 0,
                    null_count: 0,
                    total_size: 0,
                    total_uncompressed_size: 0,
                    is_deleted: false,
                    is_tight_bound: false,
                    decoded_value_type_ordinal: None,
                };
                let mut value_type_ordinal: Option<i32> = None;
                let mut value_type_info: Option<String> = None;
                for (fname, fval) in stat_fields {
                    match (fname.as_str(), fval) {
                        ("fileName", Value::Union(1, v)) => {
                            if let Value::String(s) = *v {
                                out.file_name = s;
                            }
                        }
                        ("valueType", Value::Union(1, v)) => {
                            if let Value::Record(info) = *v {
                                for (iname, ival) in info {
                                    match (iname.as_str(), ival) {
                                        ("typeOrdinal", Value::Int(ordinal)) => {
                                            value_type_ordinal = Some(ordinal);
                                        }
                                        ("additionalInfo", Value::Union(1, info)) => {
                                            if let Value::String(info) = *info {
                                                value_type_info = Some(info);
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        ("columnName", Value::Union(1, v)) => {
                            if let Value::String(s) = *v {
                                out.column_name = s;
                            }
                        }
                        ("minValue", v) => out.min_value = unwrap_stat_value(v),
                        ("maxValue", v) => out.max_value = unwrap_stat_value(v),
                        ("valueCount", Value::Union(1, v)) => {
                            if let Value::Long(n) = *v {
                                out.value_count = n;
                            }
                        }
                        ("nullCount", Value::Union(1, v)) => {
                            if let Value::Long(n) = *v {
                                out.null_count = n;
                            }
                        }
                        ("totalSize", Value::Union(1, v)) => {
                            if let Value::Long(n) = *v {
                                out.total_size = n;
                            }
                        }
                        ("totalUncompressedSize", Value::Union(1, v)) => {
                            if let Value::Long(n) = *v {
                                out.total_uncompressed_size = n;
                            }
                        }
                        ("isDeleted", Value::Boolean(b)) => out.is_deleted = b,
                        ("isTightBound", Value::Boolean(b)) => out.is_tight_bound = b,
                        _ => {}
                    }
                }
                // V2 records carry logical types in valueType over primitive
                // wrappers — lift them back to typed stat values.
                if let Some(ordinal) = value_type_ordinal {
                    let info = value_type_info.as_deref();
                    out.min_value = out.min_value.map(|v| lift_stat_value(v, ordinal, info));
                    out.max_value = out.max_value.map(|v| lift_stat_value(v, ordinal, info));
                    out.decoded_value_type_ordinal = Some(ordinal);
                }
                return Ok(Some(out));
            }
            _ => {}
        }
    }
    Ok(None)
}

/// Lift a V2 primitive-wrapped value to its logical type per the Java
/// `ValueType` ordinal carried in `valueType`.
///
/// Millisecond types (Spark writes them for `*-millis` Avro columns) are
/// lifted to the micros variants footer extraction produces, so stats of one
/// column compare by value whichever writer produced them.
fn lift_stat_value(value: ColumnStatValue, ordinal: i32, info: Option<&str>) -> ColumnStatValue {
    let millis_to_micros = |v: i64| v.checked_mul(1_000);
    match (value, ordinal) {
        // DECIMAL carries "precision,scale" in additionalInfo.
        (ColumnStatValue::Bytes(bytes), 10) => {
            let decimal = info.and_then(|info| {
                let (precision, scale) = info.split_once(',')?;
                Some(ColumnStatValue::Decimal {
                    unscaled: i128_from_be_bytes(&bytes)?,
                    precision: precision.trim().parse().ok()?,
                    scale: scale.trim().parse().ok()?,
                })
            });
            decimal.unwrap_or(ColumnStatValue::Bytes(bytes))
        }
        (ColumnStatValue::Int(v), 12) => ColumnStatValue::Date(v),
        (ColumnStatValue::Int(v), 13) => ColumnStatValue::TimeMicros(i64::from(v) * 1_000),
        (ColumnStatValue::Long(v), 14) => ColumnStatValue::TimeMicros(v),
        (ColumnStatValue::Long(v), 15) => match millis_to_micros(v) {
            Some(micros) => ColumnStatValue::TimestampMicros(micros),
            None => ColumnStatValue::Long(v),
        },
        (ColumnStatValue::Long(v), 16) => ColumnStatValue::TimestampMicros(v),
        (ColumnStatValue::Long(v), 18) => match millis_to_micros(v) {
            Some(micros) => ColumnStatValue::LocalTimestampMicros(micros),
            None => ColumnStatValue::Long(v),
        },
        (ColumnStatValue::Long(v), 19) => ColumnStatValue::LocalTimestampMicros(v),
        (value, _) => value,
    }
}

/// The millisecond `ValueType` ordinal a decoded record carried, when its
/// values were lifted from it to micros by [`lift_stat_value`]. Java's merge
/// keeps the newer record's value type, so a re-encoded record writes it back,
/// but only while every bound is a whole millisecond: a bound merged in from a
/// micros record would otherwise round, and a max rounded down prunes files
/// that hold matching rows.
fn millis_value_type(
    decoded: Option<i32>,
    min: Option<&ColumnStatValue>,
    max: Option<&ColumnStatValue>,
) -> Option<i32> {
    let whole_millis = |value: &ColumnStatValue| match value {
        ColumnStatValue::TimeMicros(v)
        | ColumnStatValue::TimestampMicros(v)
        | ColumnStatValue::LocalTimestampMicros(v) => v.rem_euclid(1_000) == 0,
        _ => false,
    };
    let ordinal = match (decoded?, min.or(max)?) {
        (13, ColumnStatValue::TimeMicros(_))
        | (15, ColumnStatValue::TimestampMicros(_))
        | (18, ColumnStatValue::LocalTimestampMicros(_)) => decoded,
        _ => None,
    };
    ordinal.filter(|_| min.into_iter().chain(max).all(whole_millis))
}

/// Inverse of the millisecond lifts in [`lift_stat_value`]: `TIME_MILLIS` is
/// an int, the timestamp types a long. Values are whole milliseconds here.
fn micros_to_millis(value: &ColumnStatValue) -> Result<ColumnStatValue> {
    Ok(match value {
        ColumnStatValue::TimeMicros(v) => {
            ColumnStatValue::Int(i32::try_from(v / 1_000).map_err(|_| {
                CoreError::MetadataTable(format!("time {v} micros does not fit TIME_MILLIS"))
            })?)
        }
        ColumnStatValue::TimestampMicros(v) | ColumnStatValue::LocalTimestampMicros(v) => {
            ColumnStatValue::Long(v / 1_000)
        }
        other => other.clone(),
    })
}

/// Inverse of [`wrap_stat_value`]: union branch → typed stat value.
fn unwrap_stat_value(value: Value) -> Option<ColumnStatValue> {
    let Value::Union(branch, inner) = value else {
        return None;
    };
    let Value::Record(fields) = *inner else {
        return None;
    };
    let (_, inner_value) = fields.into_iter().find(|(name, _)| name == "value")?;
    match (branch, inner_value) {
        (1, Value::Boolean(v)) => Some(ColumnStatValue::Boolean(v)),
        (2, Value::Int(v)) => Some(ColumnStatValue::Int(v)),
        (3, Value::Long(v)) => Some(ColumnStatValue::Long(v)),
        (4, Value::Float(v)) => Some(ColumnStatValue::Float(v)),
        (5, Value::Double(v)) => Some(ColumnStatValue::Double(v)),
        (6, Value::Bytes(v)) => Some(ColumnStatValue::Bytes(v)),
        (7, Value::String(v)) => Some(ColumnStatValue::String(v)),
        // DateWrapper and LocalDateWrapper both carry epoch days.
        (8 | 12, Value::Int(v) | Value::Date(v)) => Some(ColumnStatValue::Date(v)),
        (10, Value::Long(v) | Value::TimeMicros(v)) => Some(ColumnStatValue::TimeMicros(v)),
        (11, Value::Long(v) | Value::TimestampMicros(v)) => {
            Some(ColumnStatValue::TimestampMicros(v))
        }
        (9, Value::Decimal(v)) => Some(ColumnStatValue::Decimal {
            unscaled: i128_from_be_bytes(&Vec::<u8>::try_from(&v).ok()?)?,
            precision: V1_DECIMAL_PRECISION,
            scale: V1_DECIMAL_SCALE,
        }),
        (9, Value::Bytes(v)) => Some(ColumnStatValue::Decimal {
            unscaled: i128_from_be_bytes(&v)?,
            precision: V1_DECIMAL_PRECISION,
            scale: V1_DECIMAL_SCALE,
        }),
        _ => None,
    }
}

/// Precision and scale of the V1 `DecimalWrapper` value (`decimal(30,15)`).
pub(crate) const V1_DECIMAL_PRECISION: u8 = 30;
pub(crate) const V1_DECIMAL_SCALE: i8 = 15;

/// Java `BigInteger.toByteArray`: minimal big-endian two's complement.
pub(crate) fn i128_to_be_bytes(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let mut start = 0;
    while start < bytes.len() - 1 {
        let (lead, next) = (bytes[start], bytes[start + 1]);
        if (lead == 0x00 && next & 0x80 == 0) || (lead == 0xff && next & 0x80 != 0) {
            start += 1;
        } else {
            break;
        }
    }
    bytes[start..].to_vec()
}

/// Big-endian two's complement bytes as an `i128`; `None` beyond 16 bytes.
pub(crate) fn i128_from_be_bytes(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    let fill = if bytes[0] & 0x80 != 0 { 0xff } else { 0x00 };
    let mut buf = [fill; 16];
    buf[16 - bytes.len()..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(buf))
}

/// `unscaled * 10^(to - from)`; `None` on overflow or when lowering the scale
/// would drop non-zero digits.
pub(crate) fn rescale_decimal(unscaled: i128, from: i8, to: i8) -> Option<i128> {
    let diff = i32::from(to) - i32::from(from);
    let factor = 10_i128.checked_pow(diff.unsigned_abs())?;
    if diff >= 0 {
        unscaled.checked_mul(factor)
    } else {
        (unscaled % factor == 0).then(|| unscaled / factor)
    }
}

fn uuid_from_u64_pair(high: u64, low: u64) -> String {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&high.to_be_bytes());
    bytes[8..].copy_from_slice(&low.to_be_bytes());
    let (a, b, c, d, e) = (
        u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        u16::from_be_bytes([bytes[4], bytes[5]]),
        u16::from_be_bytes([bytes[6], bytes[7]]),
        u16::from_be_bytes([bytes[8], bytes[9]]),
        &bytes[10..16],
    );
    format!(
        "{a:08x}-{b:04x}-{c:04x}-{d:04x}-{}",
        e.iter().map(|x| format!("{x:02x}")).collect::<String>()
    )
}

/// Encode a column_stats metadata record (type = 3).
pub fn encode_column_stats(
    metadata: &ColumnStatsMetadata,
    version: StatsIndexVersion,
) -> Result<Vec<u8>> {
    encode_column_stats_record(metadata, MetadataRecordType::ColumnStats, version)
}

/// Encode a partition_stats metadata record (type = 6).
///
/// Uses the same `ColumnStatsMetadata` Avro shape as column_stats.
pub fn encode_partition_stats(
    metadata: &ColumnStatsMetadata,
    version: StatsIndexVersion,
) -> Result<Vec<u8>> {
    encode_column_stats_record(metadata, MetadataRecordType::PartitionStats, version)
}

fn encode_column_stats_record(
    metadata: &ColumnStatsMetadata,
    record_type: MetadataRecordType,
    version: StatsIndexVersion,
) -> Result<Vec<u8>> {
    let sample = metadata.min_value.as_ref().or(metadata.max_value.as_ref());
    let millis_ordinal = match version {
        StatsIndexVersion::V1 => None,
        StatsIndexVersion::V2 => millis_value_type(
            metadata.decoded_value_type_ordinal,
            metadata.min_value.as_ref(),
            metadata.max_value.as_ref(),
        ),
    };
    let bound = |value: Option<&ColumnStatValue>| -> Result<Value> {
        match (millis_ordinal, value) {
            (Some(_), Some(value)) => wrap_stat_value(Some(&micros_to_millis(value)?), version),
            _ => wrap_stat_value(value, version),
        }
    };
    let payload = Value::Union(
        1,
        Box::new(Value::Record(vec![
            (
                "fileName".to_string(),
                Value::Union(1, Box::new(Value::String(metadata.file_name.clone()))),
            ),
            (
                "columnName".to_string(),
                Value::Union(1, Box::new(Value::String(metadata.column_name.clone()))),
            ),
            ("minValue".to_string(), bound(metadata.min_value.as_ref())?),
            ("maxValue".to_string(), bound(metadata.max_value.as_ref())?),
            (
                "valueCount".to_string(),
                Value::Union(1, Box::new(Value::Long(metadata.value_count))),
            ),
            (
                "nullCount".to_string(),
                Value::Union(1, Box::new(Value::Long(metadata.null_count))),
            ),
            (
                "totalSize".to_string(),
                Value::Union(1, Box::new(Value::Long(metadata.total_size))),
            ),
            (
                "totalUncompressedSize".to_string(),
                Value::Union(1, Box::new(Value::Long(metadata.total_uncompressed_size))),
            ),
            ("isDeleted".to_string(), Value::Boolean(metadata.is_deleted)),
            (
                "isTightBound".to_string(),
                Value::Boolean(metadata.is_tight_bound),
            ),
            // V1 leaves valueType null (Java ValueMetadata.V1EmptyMetadata);
            // V2 (tv9+) carries HoodieValueTypeInfo. Ordinal derives from the
            // stat values, or is the millisecond type they were decoded from;
            // all-null columns fall back to NULL (ordinal 1).
            (
                "valueType".to_string(),
                match version {
                    StatsIndexVersion::V1 => null_union(),
                    StatsIndexVersion::V2 => {
                        let ordinal = millis_ordinal.unwrap_or_else(|| {
                            sample.map(ColumnStatValue::value_type_ordinal).unwrap_or(1)
                        });
                        let additional_info = match sample {
                            Some(ColumnStatValue::Decimal {
                                precision, scale, ..
                            }) => Value::Union(
                                1,
                                Box::new(Value::String(format!("{precision},{scale}"))),
                            ),
                            _ => null_union(),
                        };
                        Value::Union(
                            1,
                            Box::new(Value::Record(vec![
                                ("typeOrdinal".to_string(), Value::Int(ordinal)),
                                ("additionalInfo".to_string(), additional_info),
                            ])),
                        )
                    }
                },
            ),
        ])),
    );
    encode_metadata_record(vec![
        ("key".to_string(), Value::String(String::new())),
        ("type".to_string(), Value::Int(record_type as i32)),
        ("filesystemMetadata".to_string(), null_union()),
        ("BloomFilterMetadata".to_string(), null_union()),
        ("ColumnStatsMetadata".to_string(), payload),
        ("recordIndexMetadata".to_string(), null_union()),
        ("SecondaryIndexMetadata".to_string(), null_union()),
    ])
}

/// Avro union branch indices for minValue/maxValue wrappers (null = 0).
///
/// V2 (tv9+) writes logical types via PRIMITIVE wrappers — the logical type
/// lives in `valueType` instead (Java `ValueType.primitiveWrapperType`):
/// Date → IntWrapper; TimeMicros / TimestampMicros / LocalTimestampMicros →
/// LongWrapper.
fn wrap_stat_value(value: Option<&ColumnStatValue>, version: StatsIndexVersion) -> Result<Value> {
    let Some(value) = value else {
        return Ok(null_union());
    };
    if version == StatsIndexVersion::V2 {
        let (branch, inner) = match value {
            ColumnStatValue::Boolean(v) => (1, Value::Boolean(*v)),
            ColumnStatValue::Int(v) | ColumnStatValue::Date(v) => (2, Value::Int(*v)),
            ColumnStatValue::Long(v)
            | ColumnStatValue::TimeMicros(v)
            | ColumnStatValue::TimestampMicros(v)
            | ColumnStatValue::LocalTimestampMicros(v) => (3, Value::Long(*v)),
            ColumnStatValue::Float(v) => (4, Value::Float(*v)),
            ColumnStatValue::Double(v) => (5, Value::Double(*v)),
            ColumnStatValue::Bytes(v) => (6, Value::Bytes(v.clone())),
            ColumnStatValue::String(v) => (7, Value::String(v.clone())),
            ColumnStatValue::Decimal { unscaled, .. } => {
                (6, Value::Bytes(i128_to_be_bytes(*unscaled)))
            }
        };
        return Ok(Value::Union(
            branch,
            Box::new(Value::Record(vec![("value".to_string(), inner)])),
        ));
    }
    let (branch, inner) = match value {
        ColumnStatValue::Boolean(v) => (
            1,
            Value::Record(vec![("value".to_string(), Value::Boolean(*v))]),
        ),
        ColumnStatValue::Int(v) => (
            2,
            Value::Record(vec![("value".to_string(), Value::Int(*v))]),
        ),
        ColumnStatValue::Long(v) => (
            3,
            Value::Record(vec![("value".to_string(), Value::Long(*v))]),
        ),
        ColumnStatValue::Float(v) => (
            4,
            Value::Record(vec![("value".to_string(), Value::Float(*v))]),
        ),
        ColumnStatValue::Double(v) => (
            5,
            Value::Record(vec![("value".to_string(), Value::Double(*v))]),
        ),
        ColumnStatValue::Bytes(v) => (
            6,
            Value::Record(vec![("value".to_string(), Value::Bytes(v.clone()))]),
        ),
        ColumnStatValue::String(v) => (
            7,
            Value::Record(vec![("value".to_string(), Value::String(v.clone()))]),
        ),
        ColumnStatValue::Date(v) => (
            8,
            Value::Record(vec![("value".to_string(), Value::Int(*v))]),
        ),
        // DecimalWrapper holds decimal(30,15): Java upcasts every value to
        // that scale (HoodieTableMetadataUtil.tryUpcastDecimal).
        ColumnStatValue::Decimal {
            unscaled, scale, ..
        } => {
            let upcast = rescale_decimal(*unscaled, *scale, V1_DECIMAL_SCALE).ok_or_else(|| {
                CoreError::MetadataTable(format!(
                    "decimal {unscaled} at scale {scale} does not fit the V1 DecimalWrapper"
                ))
            })?;
            (
                9,
                Value::Record(vec![(
                    "value".to_string(),
                    Value::Decimal(apache_avro::Decimal::from(i128_to_be_bytes(upcast))),
                )]),
            )
        }
        ColumnStatValue::TimeMicros(v) => (
            10,
            Value::Record(vec![("value".to_string(), Value::Long(*v))]),
        ),
        // V1 has no local-timestamp wrapper; both map to TimestampMicros.
        ColumnStatValue::TimestampMicros(v) | ColumnStatValue::LocalTimestampMicros(v) => (
            11,
            Value::Record(vec![("value".to_string(), Value::Long(*v))]),
        ),
    };
    Ok(Value::Union(branch, Box::new(inner)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_column_stats_v1_test(m: &ColumnStatsMetadata) -> Result<Vec<u8>> {
        encode_column_stats(m, StatsIndexVersion::V1)
    }

    fn encode_partition_stats_v1_test(m: &ColumnStatsMetadata) -> Result<Vec<u8>> {
        encode_partition_stats(m, StatsIndexVersion::V1)
    }

    fn avro_key_field(bytes: &[u8]) -> String {
        let value = apache_avro::from_avro_datum(metadata_schema().unwrap(), &mut &bytes[..], None)
            .unwrap();
        let Value::Record(fields) = value else {
            panic!("expected record");
        };
        for (name, field) in fields {
            if name == "key" {
                return match field {
                    Value::String(s) => s,
                    other => panic!("unexpected key value: {other:?}"),
                };
            }
        }
        panic!("missing key field");
    }

    #[test]
    fn embedded_metadata_schema_has_java_string_props() {
        let json = hoodie_metadata_schema_json().unwrap();
        assert!(
            json.contains("avro.java.string"),
            "HFile-embedded schema must request Java String decoding"
        );
        assert!(
            json.contains("\"type\":\"map\"") && json.contains("avro.java.string"),
            "map types need java.string for map-key typing: {json}"
        );
    }

    #[test]
    fn metadata_records_carry_null_meta_fields_like_java() {
        let schema: serde_json::Value =
            serde_json::from_str(hoodie_metadata_schema_json().unwrap()).unwrap();
        assert_eq!(schema["name"], "HoodieMetadataRecord");
        let fields = schema["fields"].as_array().unwrap();
        for (field, name) in fields.iter().zip(MetaField::field_names()) {
            assert_eq!(
                field,
                &serde_json::json!({"name": name, "type": ["null", "string"], "doc": "", "default": null})
            );
        }
        assert_eq!(fields[5]["name"], "key");

        // Byte for byte what Spark 1.1.1 writes for an empty `__all_partitions__`:
        // five null meta fields, empty key, type 1, empty map, four null indexes.
        assert_eq!(
            encode_all_partitions(Vec::new()).unwrap(),
            [0, 0, 0, 0, 0, 0, 2, 2, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn encode_files_record_clears_avro_key_for_hfile_interop() {
        let bytes = encode_files_record(
            "city=sf",
            MetadataRecordType::Files,
            [FilesMetadataEntry {
                name: "f.parquet".to_string(),
                size: 10,
                is_deleted: false,
            }],
        )
        .unwrap();
        assert_eq!(avro_key_field(&bytes), "");
    }

    #[test]
    fn encode_record_index_matches_generic_avro_encoding() {
        for entry in &sample_record_index_entries() {
            let fast = encode_record_index_entry(entry).unwrap();
            let generic = encode_record_index_entry_generic(entry).unwrap();
            assert_eq!(fast, generic, "entry {entry:?}");
            let decoded = decode_record_index_entry("k", &fast, None)
                .unwrap()
                .unwrap();
            assert_eq!(decoded.file_id, entry.file_id);
            assert_eq!(decoded.partition_path, entry.partition_path);
            assert_eq!(decoded.instant_time_millis, entry.instant_time_millis);
        }
    }

    #[test]
    fn encode_record_index_clears_avro_key_for_hfile_interop() {
        let bytes = encode_record_index_entry(&RecordIndexEntry {
            record_key: "id-1".to_string(),
            partition_path: String::new(),
            file_id: "fg-0".to_string(),
            instant_time_millis: 1,
            is_deleted: false,
        })
        .unwrap();
        assert_eq!(avro_key_field(&bytes), "");
    }

    #[test]
    fn encode_column_stats_clears_avro_key_and_sets_type() {
        let bytes = encode_column_stats_v1_test(&ColumnStatsMetadata {
            file_name: "f.parquet".to_string(),
            column_name: "id".to_string(),
            min_value: Some(ColumnStatValue::Long(1)),
            max_value: Some(ColumnStatValue::Long(10)),
            value_count: 10,
            null_count: 0,
            total_size: 100,
            total_uncompressed_size: 200,
            is_deleted: false,
            is_tight_bound: false,
            decoded_value_type_ordinal: None,
        })
        .unwrap();
        assert_eq!(avro_key_field(&bytes), "");
        let value = apache_avro::from_avro_datum(metadata_schema().unwrap(), &mut &bytes[..], None)
            .unwrap();
        let Value::Record(fields) = value else {
            panic!("expected record");
        };
        let mut saw_type = false;
        let mut saw_payload = false;
        for (name, field) in fields {
            match (name.as_str(), field) {
                ("type", Value::Int(3)) => saw_type = true,
                ("ColumnStatsMetadata", Value::Union(1, _)) => saw_payload = true,
                _ => {}
            }
        }
        assert!(saw_type && saw_payload);
    }

    #[test]
    fn encode_partition_stats_uses_type_six() {
        let bytes = encode_partition_stats_v1_test(&ColumnStatsMetadata {
            file_name: ".".to_string(),
            column_name: "id".to_string(),
            min_value: Some(ColumnStatValue::Int(1)),
            max_value: Some(ColumnStatValue::Int(2)),
            value_count: 2,
            null_count: 0,
            total_size: 10,
            total_uncompressed_size: 20,
            is_deleted: false,
            is_tight_bound: true,
            decoded_value_type_ordinal: None,
        })
        .unwrap();
        let value = apache_avro::from_avro_datum(metadata_schema().unwrap(), &mut &bytes[..], None)
            .unwrap();
        let Value::Record(fields) = value else {
            panic!("expected record");
        };
        assert!(
            fields
                .iter()
                .any(|(n, v)| n == "type" && matches!(v, Value::Int(6)))
        );
    }

    #[test]
    fn encode_record_index_uses_uuid_bits_when_file_id_is_uuid() {
        let file_id = "184e0720-0b37-4e8b-b23a-5646f6c2fe94-0";
        let bytes = encode_record_index_entry(&RecordIndexEntry {
            record_key: "id-1".to_string(),
            partition_path: "city=sf".to_string(),
            file_id: file_id.to_string(),
            instant_time_millis: 1,
            is_deleted: false,
        })
        .unwrap();
        let decoded = decode_record_index_entry("id-1", &bytes, None)
            .unwrap()
            .unwrap();
        assert_eq!(decoded.file_id, file_id);
    }

    fn sample_entry(partition: &str, file_id: &str, instant: i64) -> RecordIndexEntry {
        RecordIndexEntry {
            record_key: "k".to_string(),
            partition_path: partition.to_string(),
            file_id: file_id.to_string(),
            instant_time_millis: instant,
            is_deleted: false,
        }
    }

    fn sample_record_index_entries() -> [RecordIndexEntry; 4] {
        [
            sample_entry(
                "",
                "3f2504e0-4f89-11d3-9a0c-0305e82c3301-0",
                1_700_000_000_123,
            ),
            sample_entry(
                "city=san_francisco",
                "3f2504e0-4f89-11d3-9a0c-0305e82c3301",
                0,
            ),
            sample_entry(
                &format!("city={}", "x".repeat(200)),
                "3f2504e0-4f89-11d3-9a0c-0305e82c3301-7",
                i64::MIN,
            ),
            sample_entry("p=\u{e9}t\u{e9}", "not-a-uuid-file-id", -1),
        ]
    }

    // High/low bits of the RFC 4122 example UUID 3f2504e0-4f89-11d3-9a0c-0305e82c3301.
    const RFC4122_HIGH: i64 = 0x3f2504e04f8911d3_u64 as i64;
    const RFC4122_LOW: i64 = 0x9a0c0305e82c3301_u64 as i64;
    const RFC4122_FILE_ID: &str = "3f2504e0-4f89-11d3-9a0c-0305e82c3301-0";
    fn present(value: Value) -> Value {
        Value::Union(1, Box::new(value))
    }

    fn rli_info(
        partition: Value,
        file_index: Value,
        file_id: Value,
        instant: Value,
        encoding: i32,
    ) -> Value {
        present(Value::Record(vec![
            ("partitionName".to_string(), partition),
            (
                "fileIdHighBits".to_string(),
                present(Value::Long(RFC4122_HIGH)),
            ),
            (
                "fileIdLowBits".to_string(),
                present(Value::Long(RFC4122_LOW)),
            ),
            ("fileIndex".to_string(), file_index),
            ("fileId".to_string(), file_id),
            ("instantTime".to_string(), instant),
            ("fileIdEncoding".to_string(), Value::Int(encoding)),
            ("position".to_string(), null_union()),
        ]))
    }

    fn rli_default() -> Value {
        rli_info(
            null_union(),
            present(Value::Int(0)),
            null_union(),
            null_union(),
            0,
        )
    }

    fn encode_rli(
        schema: &Schema,
        record_index: Value,
        secondary: Value,
        extras: &[(&str, Value)],
    ) -> Vec<u8> {
        let mut fields: Vec<(String, Value)> = MetaField::field_names()
            .into_iter()
            .map(|name| (name.to_string(), null_union()))
            .chain([
                ("key".to_string(), Value::String(String::new())),
                (
                    "type".to_string(),
                    Value::Int(MetadataRecordType::RecordIndex as i32),
                ),
                ("filesystemMetadata".to_string(), null_union()),
                ("BloomFilterMetadata".to_string(), null_union()),
                ("ColumnStatsMetadata".to_string(), null_union()),
                ("recordIndexMetadata".to_string(), record_index),
                ("SecondaryIndexMetadata".to_string(), secondary),
            ])
            .collect();
        fields.extend(extras.iter().map(|(n, v)| (n.to_string(), v.clone())));
        apache_avro::to_avro_datum(schema, Value::Record(fields)).unwrap()
    }

    fn encode_record_index_payload(record_index: Value, secondary: Value) -> Vec<u8> {
        encode_rli(metadata_schema().unwrap(), record_index, secondary, &[])
    }

    fn parse_mutated_metadata_schema(mutate: impl FnOnce(&mut serde_json::Value)) -> Schema {
        let mut json: serde_json::Value =
            serde_json::from_str(hoodie_metadata_schema_json().unwrap()).unwrap();
        mutate(&mut json);
        Schema::parse_str(&json.to_string()).unwrap()
    }

    fn json_fields(json: &mut serde_json::Value) -> &mut Vec<serde_json::Value> {
        json.get_mut("fields")
            .and_then(serde_json::Value::as_array_mut)
            .unwrap()
    }

    fn json_named<'a>(
        fields: &'a mut [serde_json::Value],
        name: &str,
    ) -> &'a mut serde_json::Value {
        fields.iter_mut().find(|f| f["name"] == name).unwrap()
    }

    fn json_union_record_fields<'a>(
        field: &'a mut serde_json::Value,
        record_name: &str,
    ) -> &'a mut Vec<serde_json::Value> {
        field
            .get_mut("type")
            .and_then(serde_json::Value::as_array_mut)
            .unwrap()
            .iter_mut()
            .find(|t| t["name"] == record_name)
            .unwrap()
            .get_mut("fields")
            .and_then(serde_json::Value::as_array_mut)
            .unwrap()
    }

    fn json_rli_info_fields(json: &mut serde_json::Value) -> &mut Vec<serde_json::Value> {
        json_union_record_fields(
            json_named(json_fields(json), "recordIndexMetadata"),
            "HoodieRecordIndexInfo",
        )
    }

    fn json_swap_named(fields: &mut [serde_json::Value], left: &str, right: &str) {
        let i = fields.iter().position(|f| f["name"] == left).unwrap();
        let j = fields.iter().position(|f| f["name"] == right).unwrap();
        fields.swap(i, j);
    }

    fn set_rli_type(json: &mut serde_json::Value, name: &str, ty: serde_json::Value) {
        json_named(json_rli_info_fields(json), name)["type"] = ty;
    }

    fn assert_no_panic_and_eq(name: &str, input: &[u8], schema: &Schema) {
        let public = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            decode_record_index_entry("k", input, Some(schema))
        }));
        let generic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            decode_record_index_entry_generic("k", input, schema)
        }));
        let public = public.unwrap_or_else(|_| panic!("public decode panicked on {name}"));
        let generic = generic.unwrap_or_else(|_| panic!("generic decode panicked on {name}"));
        match (public, generic) {
            (Ok(p), Ok(g)) => assert_eq!(p, g, "{name}"),
            (Err(_), Err(_)) => {}
            (p, g) => panic!("{name}: public {p:?} != generic {g:?}"),
        }
    }

    fn check_layout(
        name: &str,
        mutate: impl FnOnce(&mut serde_json::Value),
        info: Value,
        extras: &[(&str, Value)],
        should_match: bool,
        extra: Option<fn(&[u8], &Schema)>,
    ) {
        let schema = parse_mutated_metadata_schema(mutate);
        assert_eq!(
            record_index_writer_layout_matches(&schema),
            should_match,
            "{name}"
        );
        let bytes = encode_rli(&schema, info, null_union(), extras);
        assert_no_panic_and_eq(name, &bytes, &schema);
        if let Some(extra) = extra {
            extra(&bytes, &schema);
        }
    }

    fn patch(mut bytes: Vec<u8>, at: usize, was: u8, to: u8) -> Vec<u8> {
        assert_eq!(bytes[at], was);
        bytes[at] = to;
        bytes
    }

    fn assert_direct_eq_generic(bytes: &[u8], schema: &Schema) -> RecordIndexEntry {
        let generic = decode_record_index_entry_generic("k", bytes, schema)
            .unwrap()
            .unwrap();
        assert_eq!(
            decode_record_index_entry_direct("k", bytes)
                .unwrap()
                .unwrap(),
            generic
        );
        generic
    }

    fn decode_direct_and_generic(
        bytes: &[u8],
        schema: &Schema,
    ) -> (RecordIndexEntry, RecordIndexEntry) {
        (
            decode_record_index_entry_direct("k", bytes)
                .unwrap()
                .unwrap(),
            decode_record_index_entry_generic("k", bytes, schema)
                .unwrap()
                .unwrap(),
        )
    }

    /// Expected values come from generic decode, including null and default fields.
    #[test]
    fn decode_record_index_direct_matches_generic() {
        let schema = metadata_schema().unwrap();
        for entry in &sample_record_index_entries() {
            let bytes = encode_record_index_entry(entry).unwrap();
            let generic = assert_direct_eq_generic(&bytes, schema);
            assert_eq!(
                decode_record_index_entry("k", &bytes, Some(schema)).unwrap(),
                Some(generic),
                "{entry:?}"
            );
        }

        let bytes = encode_record_index_payload(rli_default(), null_union());
        let generic = assert_direct_eq_generic(&bytes, schema);
        assert_eq!(generic.partition_path, "");
        assert_eq!(generic.instant_time_millis, 0);
        assert_eq!(generic.file_id, RFC4122_FILE_ID);

        // encoding=1 must keep fileId empty even when UUID bits are present.
        let bytes = encode_record_index_payload(
            rli_info(
                null_union(),
                present(Value::Int(0)),
                null_union(),
                null_union(),
                1,
            ),
            null_union(),
        );
        let generic = assert_direct_eq_generic(&bytes, schema);
        assert_eq!(generic.file_id, "");
    }

    /// Empty bytes and a null `recordIndexMetadata` payload are tombstones.
    #[test]
    fn decode_record_index_empty_or_null_payload_is_delete() {
        assert_eq!(decode_record_index_entry("k", &[], None).unwrap(), None);
        let schema = metadata_schema().unwrap();
        let bytes = encode_record_index_payload(null_union(), null_union());
        assert_eq!(
            decode_record_index_entry("k", &bytes, Some(schema)).unwrap(),
            None
        );
    }

    /// Layout check ignores column-stats union changes and rejects mismatches.
    /// Expected values come from generic decode against the writer schema.
    #[test]
    fn record_index_writer_layout_matches_rli_not_column_stats() {
        let schema = metadata_schema().unwrap();
        assert!(record_index_writer_layout_matches(schema));
        let default_info = rli_info(
            null_union(),
            present(Value::Int(0)),
            null_union(),
            present(Value::Long(12345)),
            0,
        );

        check_layout(
            "column-stats union only",
            |json| {
                json_fields(json)
                    .iter_mut()
                    .find(|f| f["name"] == "ColumnStatsMetadata")
                    .unwrap()
                    .get_mut("type")
                    .and_then(serde_json::Value::as_array_mut)
                    .unwrap()
                    .push(serde_json::json!({
                        "type": "record",
                        "name": "ExtraStatsMarker",
                        "fields": [{"name": "ignored", "type": "boolean"}]
                    }));
            },
            default_info.clone(),
            &[],
            true,
            Some(|bytes, schema| {
                assert_eq!(
                    decode_record_index_entry_direct("k", bytes).unwrap(),
                    decode_record_index_entry_generic("k", bytes, schema).unwrap()
                );
            }),
        );
        check_layout(
            "swap fileIdHighBits and fileIdLowBits",
            |json| {
                json_swap_named(
                    json_rli_info_fields(json),
                    "fileIdHighBits",
                    "fileIdLowBits",
                )
            },
            default_info.clone(),
            &[],
            false,
            Some(|bytes, schema| {
                let (direct, generic) = decode_direct_and_generic(bytes, schema);
                assert_eq!(generic.file_id, RFC4122_FILE_ID);
                assert_eq!(direct.file_id, "9a0c0305-e82c-3301-3f25-04e04f8911d3-0");
            }),
        );
        check_layout(
            "fileIndex is long",
            |json| set_rli_type(json, "fileIndex", serde_json::json!(["null", "long"])),
            rli_info(
                null_union(),
                present(Value::Long(0)),
                null_union(),
                present(Value::Long(12345)),
                0,
            ),
            &[],
            false,
            Some(|bytes, schema| {
                let (direct, generic) = decode_direct_and_generic(bytes, schema);
                assert_eq!(generic.file_id, "");
                assert_eq!(direct.file_id, RFC4122_FILE_ID);
            }),
        );
        check_layout(
            "instantTime is int",
            |json| set_rli_type(json, "instantTime", serde_json::json!(["null", "int"])),
            rli_info(
                null_union(),
                present(Value::Int(0)),
                null_union(),
                present(Value::Int(12345)),
                0,
            ),
            &[],
            false,
            Some(|bytes, schema| {
                let (direct, generic) = decode_direct_and_generic(bytes, schema);
                assert_eq!(generic.instant_time_millis, 0);
                assert_eq!(direct.instant_time_millis, 12345);
            }),
        );
        check_layout(
            "extra field before recordIndexMetadata",
            |json| {
                let fields = json_fields(json);
                let i = fields.iter().position(|f| f["name"] == "type").unwrap();
                fields.insert(
                    i + 1,
                    serde_json::json!({
                        "name": "extraBeforeRecordIndex",
                        "type": ["null", "string"],
                        "default": null
                    }),
                );
            },
            default_info.clone(),
            &[("extraBeforeRecordIndex", null_union())],
            false,
            None,
        );
        check_layout(
            "extra field at the end of the top level",
            |json| {
                json_fields(json).push(serde_json::json!({
                    "name": "extraAfterSecondary",
                    "type": ["null", "string"],
                    "default": null
                }));
            },
            default_info.clone(),
            &[("extraAfterSecondary", null_union())],
            false,
            None,
        );
        check_layout(
            "swap BloomFilterMetadata and ColumnStatsMetadata",
            |json| {
                json_swap_named(
                    json_fields(json),
                    "BloomFilterMetadata",
                    "ColumnStatsMetadata",
                )
            },
            default_info,
            &[],
            false,
            None,
        );
    }

    /// Public decode must not panic and must match generic decode on prefixes and fixed inputs.
    #[test]
    fn decode_record_index_corrupt_bytes_match_generic() {
        let schema = metadata_schema().unwrap();
        for (i, entry) in sample_record_index_entries().iter().enumerate() {
            let bytes = encode_record_index_entry(entry).unwrap();
            for len in 1..=bytes.len() {
                assert_no_panic_and_eq(&format!("entry {i} prefix {len}"), &bytes[..len], schema);
            }
        }

        let sample0 = encode_record_index_entry(&sample_record_index_entries()[0]).unwrap();
        let rli_branch_at = 5 + 1 + 1 + 3;
        let mut trailing = sample0.clone();
        trailing.push(0);
        let mut invalid_utf8 = encode_record_index_payload(
            rli_info(
                present(Value::String("X".to_string())),
                present(Value::Int(0)),
                null_union(),
                null_union(),
                0,
            ),
            null_union(),
        );
        let utf8_at = invalid_utf8.iter().position(|&b| b == b'X').unwrap();
        invalid_utf8[utf8_at] = 0xff;
        let mut eleven_byte_varint = vec![0x80; 10];
        eleven_byte_varint.push(0);
        let mut overlong_string = vec![0; 5];
        overlong_string.push(40); // Avro zigzag for length 20
        let long_index_schema = parse_mutated_metadata_schema(|json| {
            set_rli_type(json, "fileIndex", serde_json::json!(["null", "long"]));
        });
        let fallback_when_direct_fails = encode_record_index_payload(
            rli_default(),
            present(Value::Record(vec![(
                "isDeleted".to_string(),
                Value::Boolean(false),
            )])),
        );
        assert!(record_index_writer_layout_matches(schema));
        assert!(decode_record_index_entry_direct("k", &fallback_when_direct_fails).is_err());
        let generic = decode_record_index_entry_generic("k", &fallback_when_direct_fails, schema)
            .unwrap()
            .unwrap();
        assert_eq!(
            decode_record_index_entry("k", &fallback_when_direct_fails, Some(schema)).unwrap(),
            Some(generic)
        );

        let fixed = [
            ("trailing byte", trailing),
            (
                "illegal union branch",
                patch(sample0.clone(), rli_branch_at, 2, 14),
            ),
            ("invalid utf-8", invalid_utf8),
            ("11-byte varint", eleven_byte_varint),
            ("string length past end", overlong_string),
            (
                "fileIndex out of i32",
                encode_rli(
                    &long_index_schema,
                    rli_info(
                        null_union(),
                        present(Value::Long(1i64 << 31)),
                        null_union(),
                        null_union(),
                        0,
                    ),
                    null_union(),
                    &[],
                ),
            ),
            (
                "get_nullable illegal branch",
                patch(
                    encode_record_index_entry(&sample_record_index_entries()[1]).unwrap(),
                    rli_branch_at + 1,
                    2,
                    4,
                ),
            ),
            (
                "expect_null_union illegal branch",
                patch(sample0, 5 + 1 + 1, 0, 14),
            ),
            (
                "direct decode fails after layout check",
                fallback_when_direct_fails,
            ),
        ];
        for (name, input) in fixed {
            assert_no_panic_and_eq(name, &input, schema);
        }
    }

    /// Spark-compacted record-index HFile: direct decode matches generic decode.
    #[test]
    fn decode_record_index_spark_hfile_matches_generic() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../test/data/metadata_multi_block_hfile/record-index-0009-0_10-71-248_20260830075534042.hfile",
        );
        let mut reader = crate::hfile::HFileReader::new(std::fs::read(&path).unwrap()).unwrap();
        let schema = reader
            .get_avro_schema()
            .unwrap()
            .expect("fixture HFile embeds a writer schema")
            .clone();
        assert!(record_index_writer_layout_matches(&schema));
        let records = reader.collect_records().unwrap();
        assert!(!records.is_empty());
        for rec in &records {
            let key = rec.key_as_str().unwrap();
            let generic = decode_record_index_entry_generic(key, rec.value(), &schema).unwrap();
            let direct = decode_record_index_entry_direct(key, rec.value()).unwrap();
            assert_eq!(direct, generic, "key {key}");
        }
    }

    /// V1 (tv8) and V2 (tv9) encodings must both decode back to the same
    /// logical stat values for every wrapper variant.
    #[test]
    fn test_column_stats_encode_decode_roundtrip_all_variants() {
        let variants = vec![
            ColumnStatValue::Boolean(true),
            ColumnStatValue::Int(7),
            ColumnStatValue::Long(9),
            ColumnStatValue::Float(1.5),
            ColumnStatValue::Double(2.5),
            ColumnStatValue::Bytes(vec![1, 2, 3]),
            ColumnStatValue::String("s".to_string()),
            ColumnStatValue::Date(19000),
            ColumnStatValue::TimeMicros(11),
            ColumnStatValue::TimestampMicros(22),
            ColumnStatValue::LocalTimestampMicros(33),
            ColumnStatValue::Decimal {
                unscaled: -10_100,
                precision: 10,
                scale: 2,
            },
        ];
        for version in [StatsIndexVersion::V1, StatsIndexVersion::V2] {
            for value in &variants {
                let meta = ColumnStatsMetadata {
                    file_name: "f.parquet".to_string(),
                    column_name: "c".to_string(),
                    min_value: Some(value.clone()),
                    max_value: Some(value.clone()),
                    value_count: 3,
                    null_count: 1,
                    total_size: 100,
                    total_uncompressed_size: 200,
                    is_deleted: false,
                    is_tight_bound: true,
                    decoded_value_type_ordinal: None,
                };
                let bytes = encode_column_stats(&meta, version)
                    .unwrap_or_else(|e| panic!("{version:?} encode {value:?}: {e}"));
                let schema = metadata_schema().unwrap();
                let decoded = decode_column_stats_entry(&bytes, Some(schema))
                    .unwrap_or_else(|e| panic!("{version:?} decode {value:?}: {e}"))
                    .unwrap_or_else(|| panic!("{version:?} decode {value:?}: tombstone"));
                // V1 has no `valueType`, so a tz-less timestamp is
                // indistinguishable from a UTC one after decode — the reason
                // the V2 index exists.
                let expected = match (version, value) {
                    (StatsIndexVersion::V1, ColumnStatValue::LocalTimestampMicros(v)) => {
                        ColumnStatValue::TimestampMicros(*v)
                    }
                    // The V1 wrapper is decimal(30,15) whatever the column's type.
                    (StatsIndexVersion::V1, ColumnStatValue::Decimal { unscaled, .. }) => {
                        ColumnStatValue::Decimal {
                            unscaled: unscaled * 10_i128.pow(13),
                            precision: 30,
                            scale: 15,
                        }
                    }
                    _ => value.clone(),
                };
                assert_eq!(decoded.min_value.as_ref(), Some(&expected), "{version:?}");
                assert_eq!(decoded.max_value.as_ref(), Some(&expected), "{version:?}");
                assert_eq!(decoded.value_count, 3);
                assert_eq!(decoded.null_count, 1);
            }
        }
    }

    #[test]
    fn test_i128_be_bytes_match_java_big_integer() {
        let cases: [(i128, &[u8]); 7] = [
            (0, &[0x00]),
            (-1, &[0xff]),
            (127, &[0x7f]),
            (128, &[0x00, 0x80]),
            (-129, &[0xff, 0x7f]),
            (10_100, &[0x27, 0x74]),
            (
                i128::MIN,
                &[0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
        ];
        for (value, bytes) in cases {
            assert_eq!(i128_to_be_bytes(value), bytes, "{value}");
            assert_eq!(i128_from_be_bytes(bytes), Some(value), "{value}");
        }
    }

    fn column_stats_payload_fields(bytes: &[u8]) -> Vec<(String, Value)> {
        let value = apache_avro::from_avro_datum(metadata_schema().unwrap(), &mut &bytes[..], None)
            .unwrap();
        let Value::Record(fields) = value else {
            panic!("expected record");
        };
        let (_, payload) = fields
            .into_iter()
            .find(|(name, _)| name == "ColumnStatsMetadata")
            .unwrap();
        let Value::Union(1, payload) = payload else {
            panic!("expected stats payload");
        };
        let Value::Record(payload) = *payload else {
            panic!("expected stats record");
        };
        payload
    }

    /// Decimal stats are encoded the way Spark writes them: V2 as a
    /// `BytesWrapper` of the unscaled value with DECIMAL `valueType` and
    /// "precision,scale"; V1 as a `DecimalWrapper` at scale 15.
    #[test]
    fn test_decimal_stats_match_spark_encoding() {
        let meta = ColumnStatsMetadata {
            file_name: "f.parquet".to_string(),
            column_name: "amount".to_string(),
            // 101.00 as decimal(10,2).
            min_value: Some(ColumnStatValue::Decimal {
                unscaled: 10_100,
                precision: 10,
                scale: 2,
            }),
            max_value: Some(ColumnStatValue::Decimal {
                unscaled: 10_300,
                precision: 10,
                scale: 2,
            }),
            value_count: 2,
            null_count: 0,
            total_size: 1,
            total_uncompressed_size: 1,
            is_deleted: false,
            is_tight_bound: false,
            decoded_value_type_ordinal: None,
        };
        let v2 = column_stats_payload_fields(
            &encode_column_stats(&meta, StatsIndexVersion::V2).unwrap(),
        );
        let field = |fields: &[(String, Value)], name: &str| {
            fields.iter().find(|(n, _)| n == name).unwrap().1.clone()
        };
        assert_eq!(
            field(&v2, "minValue"),
            Value::Union(
                6,
                Box::new(Value::Record(vec![(
                    "value".to_string(),
                    Value::Bytes(vec![0x27, 0x74])
                )]))
            )
        );
        assert_eq!(
            field(&v2, "valueType"),
            Value::Union(
                1,
                Box::new(Value::Record(vec![
                    ("typeOrdinal".to_string(), Value::Int(10)),
                    (
                        "additionalInfo".to_string(),
                        Value::Union(1, Box::new(Value::String("10,2".to_string())))
                    ),
                ]))
            )
        );

        let v1 = column_stats_payload_fields(
            &encode_column_stats(&meta, StatsIndexVersion::V1).unwrap(),
        );
        let Value::Union(9, wrapper) = field(&v1, "minValue") else {
            panic!("V1 decimal must use DecimalWrapper");
        };
        let Value::Record(wrapper) = *wrapper else {
            panic!("expected wrapper record");
        };
        let Value::Decimal(decimal) = wrapper[0].1.clone() else {
            panic!("expected decimal value");
        };
        // Spark reads this back as 101.000000000000000.
        assert_eq!(
            Vec::<u8>::try_from(&decimal).unwrap(),
            i128_to_be_bytes(101 * 10_i128.pow(15))
        );
    }

    /// Tombstones round-trip as deletes for both index versions.
    #[test]
    fn test_column_stats_tombstone_roundtrip() {
        for version in [StatsIndexVersion::V1, StatsIndexVersion::V2] {
            let meta = ColumnStatsMetadata {
                file_name: "f.parquet".to_string(),
                column_name: "c".to_string(),
                min_value: None,
                max_value: None,
                value_count: 0,
                null_count: 0,
                total_size: 0,
                total_uncompressed_size: 0,
                is_deleted: true,
                is_tight_bound: false,
                decoded_value_type_ordinal: None,
            };
            let bytes = encode_column_stats(&meta, version).unwrap();
            let schema = metadata_schema().unwrap();
            let decoded = decode_column_stats_entry(&bytes, Some(schema))
                .unwrap()
                .expect("tombstone still decodes to a record");
            assert!(decoded.is_deleted, "{version:?}");
        }
    }

    /// A V2 record decoded from a millisecond type is re-encoded with that
    /// type and its millisecond values, so a decode and encode leaves its
    /// bytes as they were.
    #[test]
    fn test_v2_millis_value_type_survives_reencoding() {
        let schema = metadata_schema().unwrap();
        for (ordinal, min, max) in [
            (
                13,
                ColumnStatValue::TimeMicros(1_000),
                ColumnStatValue::TimeMicros(86_399_999_000),
            ),
            (
                15,
                ColumnStatValue::TimestampMicros(-1_000),
                ColumnStatValue::TimestampMicros(1_700_000_000_000_000),
            ),
            (
                18,
                ColumnStatValue::LocalTimestampMicros(0),
                ColumnStatValue::LocalTimestampMicros(2_000),
            ),
        ] {
            let meta = ColumnStatsMetadata {
                file_name: "f.parquet".to_string(),
                column_name: "c".to_string(),
                min_value: Some(min),
                max_value: Some(max),
                value_count: 2,
                null_count: 0,
                total_size: 10,
                total_uncompressed_size: 10,
                is_deleted: false,
                is_tight_bound: false,
                decoded_value_type_ordinal: Some(ordinal),
            };
            let bytes = encode_column_stats(&meta, StatsIndexVersion::V2).unwrap();
            let decoded = decode_column_stats_entry(&bytes, Some(schema))
                .unwrap()
                .unwrap();
            assert_eq!(decoded, meta, "ordinal {ordinal}");
            assert_eq!(
                encode_column_stats(&decoded, StatsIndexVersion::V2).unwrap(),
                bytes,
                "ordinal {ordinal}"
            );
        }
    }

    /// A bound that is not a whole millisecond (merged in from a micros
    /// record) keeps the micros type instead of rounding.
    #[test]
    fn test_v2_millis_value_type_yields_to_sub_millisecond_bounds() {
        let meta = ColumnStatsMetadata {
            file_name: "f.parquet".to_string(),
            column_name: "c".to_string(),
            min_value: Some(ColumnStatValue::TimestampMicros(1_000)),
            max_value: Some(ColumnStatValue::TimestampMicros(1_500)),
            value_count: 2,
            null_count: 0,
            total_size: 10,
            total_uncompressed_size: 10,
            is_deleted: false,
            is_tight_bound: false,
            decoded_value_type_ordinal: Some(15),
        };
        let bytes = encode_column_stats(&meta, StatsIndexVersion::V2).unwrap();
        let decoded = decode_column_stats_entry(&bytes, Some(metadata_schema().unwrap()))
            .unwrap()
            .unwrap();
        assert_eq!(decoded.decoded_value_type_ordinal, Some(16));
        assert_eq!(
            decoded.max_value,
            Some(ColumnStatValue::TimestampMicros(1_500))
        );
    }
}
