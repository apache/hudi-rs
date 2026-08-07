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

//! Mirrors `org.apache.hudi.common.engine.RecordContext<T>`.
//!
//! In Java Hudi, `RecordContext<T>` is an abstract class with engine-specific
//! implementations (Spark, Flink, Avro). It provides APIs for record operations
//! including key extraction, ordering value extraction, delete detection,
//! and schema management. Binary conversion (`toBinaryRow`) and sealing (`seal`)
//! live in [`row_serde`](super::row_serde), since they need no reader state.
//!
//! In hudi-rs, the engine is always Arrow, so `RecordContext` is a concrete
//! struct.

use crate::Result;
use crate::config::table::HudiTableConfig;
use crate::error::CoreError;
use crate::file_group::reader_v2::buffered_record::{BufferedRecord, OrderingValue};
use crate::file_group::reader_v2::delete_context::DeleteContext;
use crate::metadata::meta_field::MetaField;
use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StringArray, StructArray, UnionArray};
use arrow_schema::{DataType, SchemaRef};
use std::collections::HashMap;
use std::sync::Arc;

/// Largest decimal scale hudi-rs decodes as an ordering value. Matches the Arrow
/// `Decimal128` / Parquet / Avro decimal domain (precision `1..=38`, so
/// `scale ∈ 0..=38`). A `Decimal128` column whose scale falls outside this range
/// is out-of-contract; rather than risk a non-exact (overflowing) value-based
/// rescale in [`cmp_decimal`](super::buffered_record::OrderingValue), such a column
/// is treated as an unsupported ordering type — silently on the read path,
/// LOUDLY on the delete path (matching every other unsupported ordering type).
const MAX_DECIMAL_ORDERING_SCALE: i8 = 38;

/// `_hoodie_operation` meta-column value for a DELETE change. This is the
/// `HoodieOperation` enum NAME (not the enum constant's Java identifier) as
/// persisted by the writer — `HoodieOperation.DELETE.getName()` == `"D"`. Java's
/// `RecordContext.isDeleteHoodieOperation` resolves it via
/// `HoodieOperation.fromName`, which only accepts these short names.
const HOODIE_OPERATION_DELETE: &str = "D";

/// `_hoodie_operation` meta-column value for an UPDATE_BEFORE change
/// (`HoodieOperation.UPDATE_BEFORE.getName()` == `"-U"`). A changelog
/// UPDATE_BEFORE row is the pre-image of an update and is treated as a delete on
/// read, matching Java `HoodieOperation.isUpdateBefore`.
const HOODIE_OPERATION_UPDATE_BEFORE: &str = "-U";

/// Lazy, single-row ordering-value accessor over one batch column.
///
/// Produced by [`RecordContext::ordering_accessor`]; the column is resolved and
/// downcast once, and an [`OrderingValue`] is constructed only for the row index
/// passed to [`Self::value_at`]. See `ordering_accessor` for why this exists.
pub enum OrderingAccessor<'b> {
    /// No ordering field configured, the field is absent from the batch, or the
    /// column is an unsupported ordering type — every row reports `None`.
    None,
    Int64(&'b arrow_array::Int64Array),
    Int32(&'b arrow_array::Int32Array),
    Int16(&'b arrow_array::Int16Array),
    Int8(&'b arrow_array::Int8Array),
    Boolean(&'b arrow_array::BooleanArray),
    Date32(&'b arrow_array::Date32Array),
    Date64(&'b arrow_array::Date64Array),
    TsSecond(&'b arrow_array::TimestampSecondArray),
    TsMilli(&'b arrow_array::TimestampMillisecondArray),
    TsMicro(&'b arrow_array::TimestampMicrosecondArray),
    TsNano(&'b arrow_array::TimestampNanosecondArray),
    Float64(&'b arrow_array::Float64Array),
    Float32(&'b arrow_array::Float32Array),
    Utf8(&'b StringArray),
    LargeUtf8(&'b arrow_array::LargeStringArray),
    /// Fixed-point decimal ordering column (`decimal(p,s)`). Carries the whole
    /// array so `value_at` can read both the `i128` unscaled value and the
    /// column's `scale` per row → [`OrderingValue::Decimal`].
    Decimal(&'b arrow_array::Decimal128Array),
    /// Multi-field (composite) ordering — `OrderingValue::Composite` cannot be
    /// built lazily from a single column, so it is precomputed eagerly via
    /// [`RecordContext::get_ordering_values`] and indexed by row. Only composite
    /// ordering pays the full-batch allocation.
    Precomputed(Vec<Option<OrderingValue>>),
}

impl<'b> OrderingAccessor<'b> {
    /// Resolve a single ordering column to a typed accessor, downcasting **once**.
    /// The scalar variants make [`Self::value_at`] downcast-free (one match on the
    /// enum discriminant), so both the lazy base-record path and the eager
    /// [`RecordContext::column_to_ordering_values`] path pay the downcast a single
    /// time per column — not once per row. An unsupported column type resolves to
    /// [`Self::None`], matching the eager path's silent "no ordering value".
    ///
    /// This is the single source of truth for which scalar types carry an ordering
    /// value on the READ path (integers/dates/timestamps/boolean → `Long`,
    /// `Float64`/`Float32` → `Double`, `Decimal128` → `Decimal`,
    /// `Utf8`/`LargeUtf8` → `String`). The DELETE path's `scalar_ordering_value`
    /// is intentionally separate: it errors loudly on an unsupported/complex
    /// ordering type rather than falling back silently.
    fn from_column(col: &'b dyn Array) -> Self {
        use arrow_schema::TimeUnit;
        macro_rules! dc {
            ($arr_ty:ty, $variant:ident) => {
                col.as_any()
                    .downcast_ref::<$arr_ty>()
                    .map(OrderingAccessor::$variant)
                    .unwrap_or(OrderingAccessor::None)
            };
        }
        match col.data_type() {
            DataType::Int64 => dc!(arrow_array::Int64Array, Int64),
            DataType::Int32 => dc!(arrow_array::Int32Array, Int32),
            DataType::Int16 => dc!(arrow_array::Int16Array, Int16),
            DataType::Int8 => dc!(arrow_array::Int8Array, Int8),
            DataType::Boolean => dc!(arrow_array::BooleanArray, Boolean),
            DataType::Date32 => dc!(arrow_array::Date32Array, Date32),
            DataType::Date64 => dc!(arrow_array::Date64Array, Date64),
            DataType::Timestamp(TimeUnit::Second, _) => {
                dc!(arrow_array::TimestampSecondArray, TsSecond)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                dc!(arrow_array::TimestampMillisecondArray, TsMilli)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                dc!(arrow_array::TimestampMicrosecondArray, TsMicro)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                dc!(arrow_array::TimestampNanosecondArray, TsNano)
            }
            DataType::Float64 => dc!(arrow_array::Float64Array, Float64),
            DataType::Float32 => dc!(arrow_array::Float32Array, Float32),
            DataType::Utf8 => dc!(StringArray, Utf8),
            DataType::LargeUtf8 => dc!(arrow_array::LargeStringArray, LargeUtf8),
            // Only in-domain scales (`0..=38`) are decoded; an out-of-contract
            // scale falls through to `None` (unsupported), so `cmp_decimal` never
            // sees a scale it cannot rescale exactly.
            DataType::Decimal128(_, scale) if (0..=MAX_DECIMAL_ORDERING_SCALE).contains(scale) => {
                dc!(arrow_array::Decimal128Array, Decimal)
            }
            _ => OrderingAccessor::None,
        }
    }

    /// Build the [`OrderingValue`] for row `idx`. A NULL cell in a supported
    /// ordering column yields [`OrderingValue::Default`] (the null-coerced default,
    /// Java `OrderingValues.getDefault()` == `Integer(0)`) — NOT `None` — so a
    /// null-ordering record does not auto-win a merge (G-B). `None` is returned
    /// only for [`OrderingAccessor::None`] (no ordering field configured, the field
    /// is absent from the batch, or the column is an unsupported ordering type),
    /// which the caller treats as "no ordering value". Downcast-free — the column
    /// was downcast once in [`Self::from_column`].
    pub fn value_at(&self, idx: usize) -> Option<OrderingValue> {
        // Integer-backed comparable primitives (integers/dates/timestamps/boolean)
        // map to `Long`; the stored i64 ordering is monotonic within a field. A
        // null cell coerces to `Default` (Java coerces a null field value to
        // `OrderingValues.getDefault()` per `RecordContext.getOrderingValue`).
        macro_rules! long_at {
            ($a:expr) => {
                if $a.is_null(idx) {
                    Some(OrderingValue::Default)
                } else {
                    Some(OrderingValue::Long($a.value(idx) as i64))
                }
            };
        }
        macro_rules! string_at {
            ($a:expr) => {
                if $a.is_null(idx) {
                    Some(OrderingValue::Default)
                } else {
                    Some(OrderingValue::String($a.value(idx).to_string()))
                }
            };
        }
        // An `int` (Arrow `Int32`) field's value of `0` is Java `Integer(0)`, which
        // IS the default ordering value (`OrderingValues.isDefault` ==
        // `Integer(0).equals(x)`) — GAP-2. Every OTHER integer-domain type
        // (`Int64`/`Int16`/`Int8`/`Date`/`Timestamp`/`Boolean`) boxes to a class
        // that is NOT `Integer`, so its `0` is a GENUINE value (`Long(0)`), NOT the
        // default — a genuine `bigint 0` must be ordering-compared, not treated as
        // natural-order.
        macro_rules! int32_at {
            ($a:expr) => {
                if $a.is_null(idx) || $a.value(idx) == 0 {
                    Some(OrderingValue::Default)
                } else {
                    Some(OrderingValue::Long($a.value(idx) as i64))
                }
            };
        }
        match self {
            OrderingAccessor::None => None,
            OrderingAccessor::Int64(a) => long_at!(a),
            OrderingAccessor::Int32(a) => int32_at!(a),
            OrderingAccessor::Int16(a) => long_at!(a),
            OrderingAccessor::Int8(a) => long_at!(a),
            OrderingAccessor::Boolean(a) => long_at!(a),
            OrderingAccessor::Date32(a) => long_at!(a),
            OrderingAccessor::Date64(a) => long_at!(a),
            OrderingAccessor::TsSecond(a) => long_at!(a),
            OrderingAccessor::TsMilli(a) => long_at!(a),
            OrderingAccessor::TsMicro(a) => long_at!(a),
            OrderingAccessor::TsNano(a) => long_at!(a),
            OrderingAccessor::Float64(a) => {
                if a.is_null(idx) {
                    Some(OrderingValue::Default)
                } else {
                    Some(OrderingValue::Double(a.value(idx)))
                }
            }
            OrderingAccessor::Float32(a) => {
                if a.is_null(idx) {
                    Some(OrderingValue::Default)
                } else {
                    Some(OrderingValue::Double(a.value(idx) as f64))
                }
            }
            OrderingAccessor::Utf8(a) => string_at!(a),
            OrderingAccessor::LargeUtf8(a) => string_at!(a),
            OrderingAccessor::Decimal(a) => {
                if a.is_null(idx) {
                    Some(OrderingValue::Default)
                } else {
                    Some(OrderingValue::Decimal {
                        unscaled: a.value(idx),
                        scale: a.scale(),
                    })
                }
            }
            OrderingAccessor::Precomputed(vals) => vals.get(idx).cloned().flatten(),
        }
    }
}

/// Record context for Arrow engine record operations.
///
/// Mirrors Java's `org.apache.hudi.common.engine.RecordContext<T>`.
///
/// In Java, `RecordContext` is the engine-specific "glue" that lets the
/// engine-agnostic `FileGroupRecordBuffer` and `BufferedRecordMerger`
/// infrastructure manipulate engine records without knowing the type system.
///
/// ## Instance methods (record-level operations needing config):
/// - `get_record_keys` — mirrors `RecordContext.getRecordKey(T, Schema)`
/// - `get_ordering_values` — mirrors `RecordContext.getOrderingValue(T, Schema, List<String>)`
/// - `batch_to_buffered_records` — combines key + ordering extraction
/// - `delete_batch_to_keys` — extracts record keys from delete batches
/// - `is_delete_record` — mirrors `RecordContext.isDeleteRecord(T, DeleteContext)`
///
/// ## Associated functions (engine-level, no config needed):
/// - `to_binary_row` — mirrors `RecordContext.toBinaryRow(Schema, T)`
/// - `seal` — mirrors `RecordContext.seal(T)`
/// - `from_binary` — Arrow-specific IPC deserialization
/// - `get_schema_from_buffer_record` — mirrors `RecordContext.getSchemaFromBufferRecord`
#[derive(Debug, Clone)]
pub struct RecordContext {
    /// Which column contains the record key (e.g. `_hoodie_record_key` or a PK column).
    /// Mirrors Java's `recordKeyExtractor` strategy. For a composite virtual key this is
    /// the FIRST field only; the full merge key is built from [`Self::record_key_fields`].
    pub record_key_field: String,

    /// ALL record-key field columns used to build the merge key. `["_hoodie_record_key"]`
    /// for meta-field tables; the single PK column for a simple virtual key; or every
    /// configured record-key field for a COMPOSITE virtual key
    /// (`hoodie.populate.meta.fields=false` with a multi-field `recordkey.fields`). When it
    /// has >1 entry, [`Self::record_key_array`] reconstructs `field:val,field:val` per row,
    /// mirroring Java `KeyGenerator.constructRecordKey`. Kept alongside
    /// [`Self::record_key_field`] so single-field call sites (schema naming, tests) are
    /// unchanged.
    pub record_key_fields: Vec<String>,

    /// Ordering (precombine) field names for conflict resolution.
    /// Mirrors Java's `orderingFieldNames` used in `getOrderingValue()`.
    pub ordering_field_names: Vec<String>,

    /// Whether meta fields are populated (affects key extraction strategy).
    /// When true, reads `_hoodie_record_key` directly.
    /// When false, computes key from configured record key columns.
    pub populate_meta_fields: bool,

    /// Partition path for constructing delete rows.
    /// Mirrors Java's `RecordContext.partitionPath`.
    pub partition_path: String,

    /// Whether a SINGLE-field virtual key must be encoded as `<field>:<value>`
    /// (rather than the bare `<value>`), matching a `ComplexKeyGenerator` /
    /// `ComplexAvroKeyGenerator` writer. Mirrors Java
    /// `KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen`
    /// (`tableVersion >= NINE || !COMPLEX_KEYGEN_NEW_ENCODING`, default `true`) gated
    /// on the table's key-generator class and single-record-key-field shape. Only
    /// meaningful for virtual-key tables (`hoodie.populate.meta.fields=false`);
    /// meta-field tables read the encoded key straight from `_hoodie_record_key`.
    /// When set, [`Self::record_key_array`] reconstructs the `field:value` key so it
    /// matches the writer's stored key (else a delete keyed `id:42` would not match a
    /// base row keyed `42`, and the deleted row would resurface — G-C).
    pub encode_single_key_field: bool,
}

impl Default for RecordContext {
    fn default() -> Self {
        Self::new(&HashMap::new(), String::new())
    }
}

impl RecordContext {
    /// Composite-record-key separators + placeholders, matching Java `KeyGenerator`
    /// (`DEFAULT_COLUMN_VALUE_SEPARATOR` `:`, `DEFAULT_RECORD_KEY_PARTS_SEPARATOR` `,`,
    /// `NULL_RECORDKEY_PLACEHOLDER`, `EMPTY_RECORDKEY_PLACEHOLDER`). These MUST stay in sync
    /// with the writer's key generator so a key reconstructed at read time equals the one
    /// the writer stored.
    const COMPOSITE_KEY_COLUMN_VALUE_SEPARATOR: char = ':';
    const COMPOSITE_KEY_PARTS_SEPARATOR: char = ',';
    const NULL_RECORDKEY_PLACEHOLDER: &'static str = "__null__";
    const EMPTY_RECORDKEY_PLACEHOLDER: &'static str = "__empty__";

    /// Canonical parse of the table's record-key field list — the single source of truth
    /// shared with [`ReaderContext::record_key_fields_from`] (which delegates here) so the
    /// reader can't disagree with the record context on what the key fields are.
    ///
    /// Meta-field tables (`hoodie.populate.meta.fields=true`, the default) key on the single
    /// meta column `_hoodie_record_key`. Virtual-key tables return EVERY configured
    /// `hoodie.table.recordkey.fields` entry (a list of length >1 => composite key). Entries
    /// are trimmed and empties dropped (a stray trailing comma must not yield a `""` field);
    /// if the config is missing or empty it falls back to the meta key.
    pub(crate) fn record_key_fields_from(table_config: &HashMap<String, String>) -> Vec<String> {
        let populate_meta_fields = table_config
            .get(HudiTableConfig::PopulatesMetaFields.as_ref())
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        if populate_meta_fields {
            return vec![MetaField::RecordKey.as_ref().to_string()];
        }
        table_config
            .get(HudiTableConfig::RecordKeyFields.as_ref())
            .map(|fields| {
                fields
                    .split(',')
                    .map(|f| f.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| vec![MetaField::RecordKey.as_ref().to_string()])
    }

    /// Create a new RecordContext from table configuration.
    ///
    /// Mirrors Java's `RecordContext(HoodieTableConfig tableConfig, JavaTypeConverter typeConverter)`.
    ///
    /// In Java, the constructor reads `tableConfig.populateMetaFields()` to choose
    /// the key extraction strategy (metadata vs virtual keys), and later
    /// `initOrderingValueConverter(schema, orderingFieldNames)` is called via
    /// `setSchemaHandler()`.
    ///
    /// In hudi-rs, we derive all fields from the `table_config` map in one shot:
    /// - `populate_meta_fields` from `hoodie.populate.meta.fields` (default: true)
    /// - `record_key_field` from `_hoodie_record_key` (meta) or `hoodie.table.recordkey.fields` (virtual)
    /// - `ordering_field_names` from `hoodie.table.precombine.field` or `hoodie.table.ordering.fields`
    pub fn new(table_config: &HashMap<String, String>, partition_path: String) -> Self {
        let populate_meta_fields = table_config
            .get(HudiTableConfig::PopulatesMetaFields.as_ref())
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);

        // ALL record-key fields (canonical parse shared with ReaderContext). Meta-field tables
        // key on `_hoodie_record_key`; virtual-key tables use every configured
        // `hoodie.table.recordkey.fields` (>1 => composite).
        let record_key_fields = Self::record_key_fields_from(table_config);
        // First field — used for single-field key extraction and schema naming; the composite
        // merge key (when there are >1 fields) is built in `record_key_array`.
        let record_key_field = record_key_fields
            .first()
            .cloned()
            .unwrap_or_else(|| MetaField::RecordKey.as_ref().to_string());

        // `ordering.fields` is the primary config here, with the deprecated
        // `precombine.field` still honored — the reverse of upstream, which
        // predates that rename.
        let ordering_field_names = table_config
            .get(HudiTableConfig::OrderingFields.as_ref())
            .or_else(|| table_config.get("hoodie.table.precombine.field"))
            // ordering.fields is comma-separated; split so a multi-field config is
            // represented as a real list (rather than one bogus "a,b" column name).
            .map(|f| {
                f.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let encode_single_key_field = Self::should_encode_single_complex_key_field(
            table_config,
            populate_meta_fields,
            record_key_fields.len(),
        );

        Self {
            record_key_field,
            record_key_fields,
            ordering_field_names,
            populate_meta_fields,
            partition_path,
            encode_single_key_field,
        }
    }

    /// Table version at or above which a `ComplexKeyGenerator` ALWAYS encodes the
    /// single record-key field name (Java `HoodieTableVersion.NINE`).
    const COMPLEX_KEYGEN_ENCODE_MIN_TABLE_VERSION: i32 = 9;
    /// Write config controlling single-field complex-keygen encoding for table
    /// versions below NINE. Java `HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING`
    /// (default `false`). When absent/false, the field name IS encoded.
    const COMPLEX_KEYGEN_NEW_ENCODING_KEY: &'static str =
        "hoodie.write.complex.keygen.new.encoding";

    /// Whether a single-field VIRTUAL key is stored as `<field>:<value>` by the
    /// writer's key generator, so the reader must reconstruct the same shape (G-C).
    ///
    /// Mirrors Java: a `ComplexKeyGenerator`/`ComplexAvroKeyGenerator` over a SINGLE
    /// record-key field prefixes the field name when
    /// `KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen` is true —
    /// `tableVersion >= NINE || !COMPLEX_KEYGEN_NEW_ENCODING` (the config defaults to
    /// `false`, so v9 always encodes and v6/v8 encode by default). Only applies to
    /// virtual-key tables; meta-field tables read the already-encoded key from
    /// `_hoodie_record_key`.
    fn should_encode_single_complex_key_field(
        table_config: &HashMap<String, String>,
        populate_meta_fields: bool,
        num_record_key_fields: usize,
    ) -> bool {
        if populate_meta_fields || num_record_key_fields != 1 {
            return false;
        }
        let is_complex_keygen = table_config
            .get(HudiTableConfig::KeyGeneratorClass.as_ref())
            .map(|c| c.ends_with("ComplexKeyGenerator") || c.ends_with("ComplexAvroKeyGenerator"))
            .unwrap_or(false);
        if !is_complex_keygen {
            return false;
        }
        // `tableVersion >= NINE || !COMPLEX_KEYGEN_NEW_ENCODING`. An unparseable /
        // absent version falls back to the new-encoding config alone (which defaults
        // to `false` → encode), matching the common default.
        let table_version = table_config
            .get(HudiTableConfig::TableVersion.as_ref())
            .and_then(|v| v.trim().parse::<i32>().ok());
        let new_encoding = table_config
            .get(Self::COMPLEX_KEYGEN_NEW_ENCODING_KEY)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        table_version.is_some_and(|v| v >= Self::COMPLEX_KEYGEN_ENCODE_MIN_TABLE_VERSION)
            || !new_encoding
    }

    // =========================================================================
    // Instance methods — record-level operations (mirrors Java RecordContext<T>)
    // =========================================================================

    /// Extract record key strings from a RecordBatch column.
    ///
    /// Mirrors Java's `RecordContext.getRecordKey(T record, Schema schema)`,
    /// applied to each row in the batch.
    ///
    /// Uses `self.record_key_field` to locate the key column.
    pub fn get_record_keys(&self, batch: &RecordBatch) -> Result<Vec<String>> {
        let key_array = self.record_key_array(batch)?;
        Ok((0..key_array.len())
            .map(|i| key_array.value(i).to_string())
            .collect())
    }

    /// Materialize the record-key column as a [`StringArray`] of merge keys.
    ///
    /// Meta-field tables key on `_hoodie_record_key`, which is always `Utf8`, so
    /// that path is an O(1) clone (Arc-backed buffers — no per-row `String`
    /// allocation, preserving the vectorized base-merge kernel's hot path). With
    /// VIRTUAL KEYS (`hoodie.populate.meta.fields=false`) the key field is a real
    /// data column whose Arrow type is NOT necessarily `Utf8` — e.g. an
    /// `id INT` primary key. Such a column is stringified via
    /// [`Self::stringify_record_key_column`], mirroring Hudi's
    /// `SimpleKeyGenerator`, which derives the record key from the field's string
    /// value. Both the base-file side (here) and the log side
    /// ([`Self::get_record_keys`] via `batch_to_buffered_records`) go through this
    /// method, so their merge keys are stringified identically and match by
    /// construction.
    ///
    /// # Errors
    /// Returns [`CoreError::ReadFileSliceError`] if the key field is absent from
    /// `batch`, and [`CoreError::Unsupported`] if a virtual-key column has a type
    /// hudi-rs does not derive record keys from (see
    /// [`Self::stringify_record_key_column`]).
    pub fn record_key_array(&self, batch: &RecordBatch) -> Result<StringArray> {
        // Composite virtual key (>1 record-key field): reconstruct `field:val,field:val`
        // per row, mirroring Java `KeyGenerator.constructRecordKey`. Both the base-file side
        // and the log side route through this method, so their merge keys match by
        // construction.
        //
        // A SINGLE-field virtual key written by a ComplexKeyGenerator is encoded as
        // `field:val` too (G-C — see [`Self::encode_single_key_field`]); it routes
        // through the same builder, which for one field yields `field:val` (no
        // trailing separator) and errors on a null/empty key (matching Java's
        // single-field `HoodieKeyException`).
        if self.record_key_fields.len() > 1 || self.encode_single_key_field {
            return self.build_composite_record_key_array(batch);
        }
        let col_idx = batch
            .schema()
            .index_of(&self.record_key_field)
            .map_err(|e| {
                CoreError::ReadFileSliceError(format!(
                    "Key field '{}' not found in schema: {e}",
                    self.record_key_field
                ))
            })?;

        Self::stringify_key_field_column(batch.column(col_idx), &self.record_key_field)
    }

    /// Stringify one record-key field's column into a [`StringArray`], the shared building
    /// block for both the single-field key and every field of a composite key. A `Utf8`
    /// column is cloned (a cheap Arc-buffer bump, not a data copy); any other type goes
    /// through [`Self::stringify_record_key_column`] (integral types supported, others
    /// rejected loudly). Using one helper guarantees the single-field, composite, base, and
    /// log sides all derive keys identically.
    fn stringify_key_field_column(column: &ArrayRef, field: &str) -> Result<StringArray> {
        match column.data_type() {
            DataType::Utf8 => Ok(column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 column downcasts to StringArray")
                .clone()),
            _ => Self::stringify_record_key_column(column.as_ref(), field),
        }
    }

    /// Build the merge key for a COMPOSITE virtual key (>1 record-key field), mirroring Java
    /// `KeyGenerator.constructRecordKey`: for each field append `<field>:<value>`, join the
    /// parts with `,`, substitute `__null__` / `__empty__` for null / empty values, and error
    /// if every field is null/empty (matching Java's `HoodieKeyException`). Each field column
    /// is stringified through the same path as the single-field key (`Utf8` clone or
    /// [`Self::stringify_record_key_column`]), so a key over e.g. `id INT, name STRING` is
    /// keyed identically on the base and log sides. All record-key fields are present in the
    /// batch because the schema handler adds every one to the required schema.
    ///
    /// Note: `,` / `:` occurring INSIDE field values are NOT escaped, matching Java's
    /// `KeyGenerator.constructRecordKey` bug-for-bug (e.g. `name = "a,b:c"` yields an
    /// ambiguous key). That ambiguity is inherited by design: both the base and log sides
    /// construct the key identically through this method, so base/log pairing is unaffected.
    fn build_composite_record_key_array(&self, batch: &RecordBatch) -> Result<StringArray> {
        // Stringify each key field's column once, preserving nulls.
        let mut per_field: Vec<StringArray> = Vec::with_capacity(self.record_key_fields.len());
        for field in &self.record_key_fields {
            let col_idx = batch.schema().index_of(field).map_err(|e| {
                CoreError::ReadFileSliceError(format!(
                    "Composite key field '{field}' not found in schema: {e}"
                ))
            })?;
            per_field.push(Self::stringify_key_field_column(
                batch.column(col_idx),
                field,
            )?);
        }

        let num_rows = batch.num_rows();
        let mut keys: Vec<String> = Vec::with_capacity(num_rows);
        for row in 0..num_rows {
            let mut key = String::new();
            let mut all_null_or_empty = true;
            for (i, field) in self.record_key_fields.iter().enumerate() {
                if i > 0 {
                    key.push(Self::COMPOSITE_KEY_PARTS_SEPARATOR);
                }
                key.push_str(field);
                key.push(Self::COMPOSITE_KEY_COLUMN_VALUE_SEPARATOR);
                let col = &per_field[i];
                if col.is_null(row) {
                    key.push_str(Self::NULL_RECORDKEY_PLACEHOLDER);
                } else {
                    let value = col.value(row);
                    if value.is_empty() {
                        key.push_str(Self::EMPTY_RECORDKEY_PLACEHOLDER);
                    } else {
                        key.push_str(value);
                        all_null_or_empty = false;
                    }
                }
            }
            if all_null_or_empty {
                return Err(CoreError::ReadFileSliceError(format!(
                    "composite record key is entirely null/empty for fields {:?}; a record \
                     key cannot be null or empty (mirrors Java HoodieKeyException)",
                    self.record_key_fields
                )));
            }
            keys.push(key);
        }
        Ok(StringArray::from(keys))
    }

    /// Convert a non-`Utf8` virtual record-key column into a [`StringArray`],
    /// mirroring Hudi's `SimpleKeyGenerator` (the key is the field value's string
    /// form). Supports string and integral key fields — the types Hudi allows as
    /// a simple record key in practice. Any other type is rejected LOUDLY with
    /// [`CoreError::Unsupported`] rather than guessing a string form that might
    /// not match the writer's key generator (which would silently mis-key the
    /// merge). For a COMPOSITE virtual key, [`Self::build_composite_record_key_array`]
    /// calls this per field, so the same per-column type handling is shared.
    fn stringify_record_key_column(column: &dyn Array, field: &str) -> Result<StringArray> {
        match column.data_type() {
            DataType::LargeUtf8
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => {
                let string_column = arrow_cast::cast(column, &DataType::Utf8).map_err(|e| {
                    CoreError::ReadFileSliceError(format!(
                        "failed to stringify virtual record-key column '{field}' of type {:?}: {e}",
                        column.data_type()
                    ))
                })?;
                Ok(string_column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("cast to Utf8 yields a StringArray")
                    .clone())
            }
            other => Err(CoreError::Unsupported(format!(
                "virtual record-key column '{field}' has type {other:?}; hudi-rs \
                 derives record keys only from string and integral key fields (matching \
                 SimpleKeyGenerator). Refusing to guess a string form that may not match \
                 the writer's key generator."
            ))),
        }
    }

    /// Extract ordering values from the first ordering field column.
    ///
    /// Mirrors Java's `RecordContext.getOrderingValue(T, Schema, List<String>)`,
    /// applied to each row in the batch.
    ///
    /// Uses `self.ordering_field_names` to locate the ordering column.
    /// Supports Int32, Int64, Float32, Float64, and Utf8 column types.
    /// Returns None if no ordering fields specified or column not found.
    pub fn get_ordering_values(
        &self,
        batch: &RecordBatch,
    ) -> Result<Option<Vec<Option<OrderingValue>>>> {
        match self.ordering_field_names.as_slice() {
            // No precombine/ordering field configured → no ordering values.
            [] => Ok(None),
            [field_name] => match batch.schema().index_of(field_name) {
                Ok(col_idx) => Ok(Self::column_to_ordering_values(
                    batch.column(col_idx).as_ref(),
                )),
                // Ordering field not present in this batch's schema → no ordering value.
                Err(_) => Ok(None),
            },
            // Multi-field (composite) ordering, e.g. MySql Debezium's
            // `(_event_bin_file, _event_pos)`. Build one `OrderingValue::Composite`
            // per row by zipping the per-field scalar values, compared
            // lexicographically field-by-field (mirrors Java `OrderingValues`).
            // A field missing from this batch's schema, or an unsupported field
            // type, yields no ordering value (matching the scalar path's silent
            // fallback). A NULL in an individual field coerces that element to
            // `OrderingValue::Default` (Java coerces each null ordering field to
            // `OrderingValues.getDefault()` per element — see
            // `OrderingValues.create(List, Function)`), so a null composite element
            // does not nullify the whole key.
            fields => {
                let mut per_field: Vec<Vec<Option<OrderingValue>>> =
                    Vec::with_capacity(fields.len());
                for field_name in fields {
                    match batch.schema().index_of(field_name) {
                        Ok(col_idx) => {
                            match Self::column_to_ordering_values(batch.column(col_idx).as_ref()) {
                                Some(vals) => per_field.push(vals),
                                // Unsupported field type → no composite ordering.
                                None => return Ok(None),
                            }
                        }
                        // A composite ordering field absent from this batch → none.
                        Err(_) => return Ok(None),
                    }
                }
                let num_rows = batch.num_rows();
                let composite: Vec<Option<OrderingValue>> = (0..num_rows)
                    .map(|row| {
                        let mut elems = Vec::with_capacity(fields.len());
                        for field_vals in &per_field {
                            match &field_vals[row] {
                                Some(v) => elems.push(v.clone()),
                                // Any null field → no ordering value for this row.
                                None => return None,
                            }
                        }
                        Some(OrderingValue::Composite(elems))
                    })
                    .collect();
                Ok(Some(composite))
            }
        }
    }

    /// Lazy counterpart to [`Self::get_ordering_values`]: resolve the ordering
    /// column + downcast **once**, then build an [`OrderingValue`] only for the
    /// row index actually requested (via [`OrderingAccessor::value_at`]).
    ///
    /// The eager `get_ordering_values` allocates a full-batch
    /// `Vec<Option<OrderingValue>>` (plus a `String` per row for Utf8 ordering)
    /// even though, in the base-merge kernel, ordering is consulted only on the
    /// rare rows that have a conflicting log entry. This accessor pays the
    /// per-row cost only on those rows, leaving the no-conflict hot path
    /// allocation-free.
    ///
    /// Type support and semantics mirror [`Self::column_to_ordering_values`]
    /// exactly, because the single-column case and the eager path both resolve the
    /// column through [`OrderingAccessor::from_column`]: integers/dates/timestamps/
    /// boolean → `Long`, `Float64`/`Float32` → `Double`, `Utf8`/`LargeUtf8` →
    /// `String`; null or unsupported type → `None`; multi-column → eager
    /// `Composite` via [`Self::get_ordering_values`].
    pub fn ordering_accessor<'b>(&self, batch: &'b RecordBatch) -> Result<OrderingAccessor<'b>> {
        match self.ordering_field_names.as_slice() {
            [] => Ok(OrderingAccessor::None),
            // Single ordering column → typed accessor, downcast once via
            // `from_column`. `value_at` is then downcast-free and covers EVERY
            // scalar type (Float/Timestamp/Date/... included, not just
            // Int64/Int32/Utf8 — that gap silently degraded EVENT_TIME MOR merges
            // over e.g. a `Double` `weight` field to commit-time/log-always-wins).
            // An unsupported column type resolves to `None`.
            [field_name] => match batch.schema().index_of(field_name) {
                Ok(col_idx) => Ok(OrderingAccessor::from_column(
                    batch.column(col_idx).as_ref(),
                )),
                Err(_) => Ok(OrderingAccessor::None),
            },
            // Multi-field (composite) ordering: build the per-row composite
            // values eagerly via the shared path, then index into them. Returns
            // `OrderingAccessor::None` when no composite applies (field absent /
            // unsupported element type / no rows), matching `get_ordering_values`.
            _ => match self.get_ordering_values(batch)? {
                Some(vals) => Ok(OrderingAccessor::Precomputed(vals)),
                None => Ok(OrderingAccessor::None),
            },
        }
    }

    /// Convert one Arrow column into per-row ordering values for the general
    /// (non-delete) read path. Handles every comparable primitive Hudi allows as
    /// an ordering/precombine field: integers, dates, timestamps, and boolean →
    /// `Long`; `Float64`/`Float32` → `Double`; `Utf8`/`LargeUtf8` → `String`.
    ///
    /// Returns `None` for unsupported column types (struct/array/map/...), which
    /// the caller treats as "no ordering value".
    ///
    /// LIMITATION (ENG-38318 B2): this is intentionally SILENT (not a loud error)
    /// on the general path. `get_ordering_values` runs for every record
    /// regardless of merge mode, so erroring here would falsely fail a
    /// COMMIT_TIME table that merely carries an unsupported precombine field it
    /// never uses; and a complex type is not a valid EVENT_TIME ordering key
    /// anyway (ordering needs a Comparable scalar). The DELETE path DOES reject
    /// unsupported/composite ordering values loudly — both the union decode
    /// ([`scalar_ordering_value`] / [`decode_delete_wrapper_ordering`]) and the
    /// bare-primitive fallback in [`delete_batch_to_keys_with_ordering`] — since
    /// a dropped delete ordering value silently corrupts the merge.
    fn column_to_ordering_values(col: &dyn Array) -> Option<Vec<Option<OrderingValue>>> {
        // One downcast via `OrderingAccessor::from_column`, then a downcast-free
        // per-row `value_at` (NOT a per-row downcast). An unsupported column type
        // resolves to `OrderingAccessor::None` → outer `None` here (the caller
        // treats it as "no ordering value"). Shares the scalar type table with the
        // lazy accessor, so the eager (log-side) and lazy (base-side) results agree
        // by construction — the single read-path source of truth.
        match OrderingAccessor::from_column(col) {
            OrderingAccessor::None => None,
            acc => Some((0..col.len()).map(|i| acc.value_at(i)).collect()),
        }
    }

    /// Decode one row of a Hudi delete-block `orderingVal` union into an
    /// [`OrderingValue`].
    ///
    /// The union variants are the `org.apache.hudi.avro.model.*Wrapper` records
    /// (each a `Struct{value: <primitive>}`), plus a `null` variant at index 0.
    /// The `null` variant — or a null inner value — yields `None` (the
    /// natural-order default delete). The inner value is decoded by
    /// [`scalar_ordering_value`], which maps integral/string types and LOUDLY
    /// rejects any other type rather than silently dropping the ordering value
    /// (which would make the delete win unconditionally — the ENG-38318 bug).
    fn decode_delete_wrapper_ordering(
        union: &UnionArray,
        row: usize,
    ) -> Result<Option<OrderingValue>> {
        let wrapper = union.value(row); // length-1 array of the active variant
        if wrapper.is_null(0) {
            return Ok(None); // null variant — natural-order (default) delete
        }
        let st = wrapper
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                CoreError::ReadFileSliceError(
                    "delete orderingVal union variant is not a wrapper struct".to_string(),
                )
            })?;
        match st.column_by_name("value") {
            Some(value) => Self::scalar_ordering_value(value.as_ref(), 0),
            // No scalar `value` field => a composite wrapper (`ArrayWrapper`), a
            // MULTI-FIELD ordering value (e.g. MySql Debezium's
            // `(_event_bin_file, _event_pos)`). The Avro `array<union<*Wrapper>>`
            // `wrappedValues` decodes to Arrow `List<Union<Struct{value}>>`;
            // decode each element's scalar (reusing this same wrapper logic) and
            // build an `OrderingValue::Composite`. Mirrors Java `OrderingValues`
            // for a multi-field key. Any null component → no ordering value (the
            // natural-order default), matching the scalar path.
            None => Self::decode_composite_wrapper_ordering(st),
        }
    }

    /// Decode an `ArrayWrapper` (composite/multi-field ordering) struct into an
    /// [`OrderingValue::Composite`]. The `wrappedValues` field is an Avro
    /// `array<union<*Wrapper>>` → Arrow `List<Union<Struct{value}>>`; each list
    /// element is itself a wrapper union, decoded by recursing into
    /// [`decode_delete_wrapper_ordering`]. Any null component yields `None` (the
    /// natural-order default), matching the scalar path. Diagnostic errors carry
    /// the observed Arrow type so an unexpected shape is identifiable.
    fn decode_composite_wrapper_ordering(st: &StructArray) -> Result<Option<OrderingValue>> {
        let wrapped = st.column_by_name("wrappedValues").ok_or_else(|| {
            CoreError::ReadFileSliceError(format!(
                "delete ArrayWrapper missing scalar `value` and `wrappedValues`; struct type: {:?}",
                st.data_type()
            ))
        })?;
        let list = wrapped
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                CoreError::ReadFileSliceError(format!(
                    "delete ArrayWrapper `wrappedValues` is not a List; type: {:?}",
                    wrapped.data_type()
                ))
            })?;
        // `wrapper` (the active union variant) is a length-1 array, so the
        // composite components live at list row 0. Guard the index: a 0-row
        // list would panic in `ListArray::value`, so treat an empty/absent
        // array as the natural-order default rather than crashing the reader.
        if list.is_empty() || list.is_null(0) {
            return Ok(None);
        }
        let elements = list.value(0);
        let inner = elements.as_any().downcast_ref::<UnionArray>().ok_or_else(|| {
            CoreError::ReadFileSliceError(format!(
                "delete ArrayWrapper `wrappedValues` element is not a wrapper union; type: {:?}",
                elements.data_type()
            ))
        })?;
        let mut parts = Vec::with_capacity(inner.len());
        for j in 0..inner.len() {
            match Self::decode_delete_wrapper_ordering(inner, j)? {
                Some(v) => parts.push(v),
                None => return Ok(None),
            }
        }
        if parts.is_empty() {
            Ok(None)
        } else {
            Ok(Some(OrderingValue::Composite(parts)))
        }
    }

    /// Decode a single scalar ordering value at `row` from a primitive column.
    ///
    /// Integral/temporal/boolean types → [`OrderingValue::Long`] (covers Int/Long
    /// and the physically-integral Date/Timestamp wrappers); `Float32`/`Float64` →
    /// [`OrderingValue::Double`]; `Decimal128` → [`OrderingValue::Decimal`];
    /// `Utf8`/`LargeUtf8` → [`OrderingValue::String`]; a null cell → `None`. Any
    /// other type is rejected with a loud [`CoreError::Unsupported`]: silently
    /// dropping it would make an EVENT_TIME delete win regardless of its ordering
    /// value.
    fn scalar_ordering_value(value: &dyn Array, row: usize) -> Result<Option<OrderingValue>> {
        if value.is_null(row) {
            return Ok(None);
        }
        // Integer-backed comparable primitives → Long (see column_to_ordering_values).
        macro_rules! long_at {
            ($arr_ty:ty) => {{
                OrderingValue::Long(
                    value
                        .as_any()
                        .downcast_ref::<$arr_ty>()
                        .ok_or_else(|| {
                            CoreError::ReadFileSliceError(
                                "ordering column downcast failed".to_string(),
                            )
                        })?
                        .value(row) as i64,
                )
            }};
        }
        use arrow_schema::TimeUnit;
        let ordering = match value.data_type() {
            DataType::Int64 => long_at!(arrow_array::Int64Array),
            // An `int` (Int32) wrapper value of `0` is Java `Integer(0)` — the
            // default ordering value (`OrderingValues.isDefault`), e.g. a global-index
            // relocate DELETE carrying `HoodieRecord.DEFAULT_ORDERING_VALUE`, which is
            // serialized via `IntWrapper`. A genuine `bigint 0` uses `LongWrapper`
            // (`Int64` above) and is NOT the default (GAP-2).
            DataType::Int32 => {
                let v = value
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .ok_or_else(|| {
                        CoreError::ReadFileSliceError("Int32 ordering downcast".to_string())
                    })?
                    .value(row);
                if v == 0 {
                    OrderingValue::Default
                } else {
                    OrderingValue::Long(v as i64)
                }
            }
            DataType::Int16 => long_at!(arrow_array::Int16Array),
            DataType::Int8 => long_at!(arrow_array::Int8Array),
            DataType::Boolean => long_at!(arrow_array::BooleanArray),
            DataType::Date32 => long_at!(arrow_array::Date32Array),
            DataType::Date64 => long_at!(arrow_array::Date64Array),
            DataType::Timestamp(TimeUnit::Second, _) => long_at!(arrow_array::TimestampSecondArray),
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                long_at!(arrow_array::TimestampMillisecondArray)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                long_at!(arrow_array::TimestampMicrosecondArray)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                long_at!(arrow_array::TimestampNanosecondArray)
            }
            DataType::Float64 => OrderingValue::Double(
                value
                    .as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                    .ok_or_else(|| CoreError::ReadFileSliceError("Float64 downcast".to_string()))?
                    .value(row),
            ),
            DataType::Float32 => OrderingValue::Double(
                value
                    .as_any()
                    .downcast_ref::<arrow_array::Float32Array>()
                    .ok_or_else(|| CoreError::ReadFileSliceError("Float32 downcast".to_string()))?
                    .value(row) as f64,
            ),
            DataType::Utf8 => OrderingValue::String(
                value
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| CoreError::ReadFileSliceError("Utf8 downcast".to_string()))?
                    .value(row)
                    .to_string(),
            ),
            DataType::LargeUtf8 => OrderingValue::String(
                value
                    .as_any()
                    .downcast_ref::<arrow_array::LargeStringArray>()
                    .ok_or_else(|| CoreError::ReadFileSliceError("LargeUtf8 downcast".to_string()))?
                    .value(row)
                    .to_string(),
            ),
            // `decimal(p,s)` (e.g. a Hudi `DecimalWrapper` delete-block ordering
            // value, or a decimal precombine column) → `Decimal`, preserving the
            // i128 unscaled value + scale for a value-based compare. Mirrors Java
            // Hudi, whose `DecimalWrapper`/`BigDecimal` is a valid ordering key.
            // Guarded to the in-domain scale range (`0..=38`): an out-of-contract
            // scale falls through to the loud `other` arm rather than risk a
            // non-exact rescale that would silently mis-pick the merge winner.
            DataType::Decimal128(_, scale) if (0..=MAX_DECIMAL_ORDERING_SCALE).contains(scale) => {
                let arr = value
                    .as_any()
                    .downcast_ref::<arrow_array::Decimal128Array>()
                    .ok_or_else(|| {
                        CoreError::ReadFileSliceError("Decimal128 downcast".to_string())
                    })?;
                OrderingValue::Decimal {
                    unscaled: arr.value(row),
                    scale: arr.scale(),
                }
            }
            other => {
                return Err(CoreError::Unsupported(format!(
                    "EVENT_TIME ordering value of type {other:?} is not supported — only \
                     integral, floating-point, decimal, temporal, boolean, and string ordering \
                     fields are decoded; refusing to silently drop the ordering value (which \
                     would make a delete win unconditionally)"
                )));
            }
        };
        Ok(Some(ordering))
    }

    /// Convert a decoded source batch into a Vec of (key, BufferedRecord) pairs.
    ///
    /// Each row becomes an individual `BufferedRecord` whose payload is a
    /// zero-copy [`RecordPayload::BatchRef`](crate::file_group::reader_v2::buffered_record::RecordPayload::BatchRef)
    /// into the shared `batch` (A2) — no per-row slice or copy is made here. The
    /// caller MUST intern the source batch into a single `Arc` (one `Arc` per
    /// decoded block batch) before calling, so that the resulting `BatchRef`s all
    /// share that `Arc` and `Arc::as_ptr` identity is a valid compaction grouping
    /// key. Combines `get_record_keys` + `get_ordering_values` + `is_delete_record`
    /// per row.
    ///
    /// Mirrors the Java pattern in `KeyBasedFileGroupRecordBuffer.processDataBlock()`:
    /// ```text
    /// for each record in dataBlock.getEngineRecordIterator():
    ///     key = recordContext.getRecordKey(record, schema)
    ///     ordering = recordContext.getOrderingValue(record, schema, orderingFields)
    ///     isDelete = recordContext.isDeleteRecord(record, deleteContext)
    ///     buffered = BufferedRecords.fromEngineRecord(record, key, ordering, isDelete)
    /// ```
    pub fn batch_to_buffered_records(
        &self,
        batch: &Arc<RecordBatch>,
        delete_context: Option<&DeleteContext>,
    ) -> Result<Vec<(String, BufferedRecord)>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        let keys = self.get_record_keys(batch)?;
        let ordering_values = self.get_ordering_values(batch)?;
        let mut records = Vec::with_capacity(batch.num_rows());

        for (row_idx, key) in keys.into_iter().enumerate() {
            let ordering_value = ordering_values
                .as_ref()
                .and_then(|vals| vals.get(row_idx).cloned())
                .flatten();
            // Mirrors Java: boolean isDelete = recordContext.isDeleteRecord(nextRecord, deleteContext)
            let is_delete = self.is_delete_record(batch, row_idx, delete_context);
            let record = if is_delete {
                BufferedRecord::new_delete(key.clone(), ordering_value)
            } else {
                // A2: zero-copy reference into the shared source batch; no slice
                // or copy until the drain interleaves survivors.
                BufferedRecord::new_batch_ref(key.clone(), batch.clone(), row_idx, ordering_value)
            };
            records.push((key, record));
        }

        Ok(records)
    }

    /// Extract the record keys from a delete RecordBatch.
    ///
    /// Delete batches have schema: (recordKey, partitionPath, orderingVal). The
    /// only field the key-based buffer's `process_delete_block` consumes is the
    /// record key — it constructs its own `DeleteRecord` per key. We therefore
    /// extract keys only and skip building a `BufferedRecord` per row (review A3:
    /// the per-row `new_delete` allocations were wasted — the caller discarded
    /// them). Ordering/partition are unreachable-post-gate (D-P3-1): EVENT_TIME
    /// delete ordering is rejected at buffer/loader.rs before any merge runs.
    pub fn delete_batch_to_keys(&self, batch: &RecordBatch) -> Result<Vec<String>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        // Delete batch schema: recordKey (col 0), partitionPath (col 1), orderingVal (col 2)
        let key_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                CoreError::ReadFileSliceError(
                    "Delete batch column 0 is not a StringArray".to_string(),
                )
            })?;

        Ok((0..key_array.len())
            .map(|i| key_array.value(i).to_string())
            .collect())
    }

    /// Like [`delete_batch_to_keys`] but also extracts each delete record's
    /// ordering value from the delete batch (col 2).
    ///
    /// Required for `EVENT_TIME_ORDERING`: a delete only supersedes an existing
    /// record when its ordering value is `>=` the existing record's, so a delete
    /// carrying a *lower* ordering value must NOT remove the row. Dropping the
    /// ordering value here (as the key-only [`delete_batch_to_keys`] does) makes
    /// every delete unconditionally win — correct for `COMMIT_TIME_ORDERING`
    /// (delete always wins) but wrong for `EVENT_TIME_ORDERING`.
    ///
    /// Falls back to `None` ordering per row when col 2 is absent or its type is
    /// unsupported (degrades to delete-always-wins rather than failing).
    pub fn delete_batch_to_keys_with_ordering(
        &self,
        batch: &RecordBatch,
    ) -> Result<Vec<(String, Option<OrderingValue>)>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        // Delete batch schema: recordKey (col 0), partitionPath (col 1), orderingVal (col 2)
        let key_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                CoreError::ReadFileSliceError(
                    "Delete batch column 0 is not a StringArray".to_string(),
                )
            })?;

        let orderings: Vec<Option<OrderingValue>> = if batch.num_columns() > 2 {
            let col = batch.column(2);
            match col.data_type() {
                // Hudi delete blocks store orderingVal as a union of avro wrapper
                // structs; decode each active variant (loud error on unsupported
                // inner types rather than silently dropping the ordering value).
                DataType::Union(_, _) => {
                    let union = col.as_any().downcast_ref::<UnionArray>().ok_or_else(|| {
                        CoreError::ReadFileSliceError(
                            "Delete batch orderingVal column is not a UnionArray".to_string(),
                        )
                    })?;
                    (0..union.len())
                        .map(|i| Self::decode_delete_wrapper_ordering(union, i))
                        .collect::<Result<Vec<_>>>()?
                }
                // Fallback: a bare primitive orderingVal column (older format /
                // tests). An UNSUPPORTED bare primitive type must reject loudly
                // (restores fix 34cc3f4, reverted during the vectorized-merge
                // work) — never silently degrade to all-`None`, which would make
                // a delete unconditionally win under EVENT_TIME (silent-wrong).
                // Matches the union path and the loud-rejection contract.
                _ => match Self::column_to_ordering_values(col.as_ref()) {
                    Some(v) => v,
                    None => {
                        return Err(CoreError::Unsupported(format!(
                            "delete orderingVal column has unsupported type {:?}; \
                             cannot extract a comparable ordering value for the delete merge",
                            col.data_type()
                        )));
                    }
                },
            }
        } else {
            vec![None; key_array.len()]
        };

        Ok((0..key_array.len())
            .map(|i| {
                (
                    key_array.value(i).to_string(),
                    orderings.get(i).cloned().flatten(),
                )
            })
            .collect())
    }

    /// Check if a record is a delete record.
    ///
    /// Mirrors Java's `RecordContext.isDeleteRecord(T record, DeleteContext deleteContext)`.
    ///
    /// Checks (in order, matching Java):
    /// 1. Built-in: `_hoodie_is_deleted` field is true
    /// 2. Operation: `_hoodie_operation` field is DELETE or UPDATE_BEFORE
    /// 3. Custom: user-configured delete marker field matches configured value
    ///
    /// When `delete_context` is `Some`, uses pre-computed field positions and
    /// custom delete marker config from the context. When `None`, falls back
    /// to inline schema lookups (no custom marker support).
    pub fn is_delete_record(
        &self,
        batch: &RecordBatch,
        row_idx: usize,
        delete_context: Option<&DeleteContext>,
    ) -> bool {
        self.is_built_in_delete_record(batch, row_idx, delete_context)
            || self.is_delete_hoodie_operation(batch, row_idx, delete_context)
            || self.is_custom_delete_record(batch, row_idx, delete_context)
    }

    /// Check 1: Built-in `_hoodie_is_deleted` field.
    ///
    /// Mirrors Java's `RecordContext.isBuiltInDeleteRecord(T, DeleteContext)`.
    fn is_built_in_delete_record(
        &self,
        batch: &RecordBatch,
        row_idx: usize,
        delete_context: Option<&DeleteContext>,
    ) -> bool {
        // Fast path: if DeleteContext says the field doesn't exist, skip
        if let Some(ctx) = delete_context
            && !ctx.has_built_in_delete_field
        {
            return false;
        }

        let schema = batch.schema();
        if let Some((idx, _)) = schema.column_with_name("_hoodie_is_deleted")
            && let Some(arr) = batch
                .column(idx)
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
            && !arr.is_null(row_idx)
            && arr.value(row_idx)
        {
            return true;
        }
        false
    }

    /// Check 2: `_hoodie_operation` field is DELETE or UPDATE_BEFORE.
    ///
    /// Mirrors Java's `RecordContext.isDeleteHoodieOperation(T, DeleteContext)`.
    fn is_delete_hoodie_operation(
        &self,
        batch: &RecordBatch,
        row_idx: usize,
        delete_context: Option<&DeleteContext>,
    ) -> bool {
        // Use pre-computed position from DeleteContext if available
        let col_idx = if let Some(ctx) = delete_context {
            match ctx.hoodie_operation_pos {
                Some(pos) => pos,
                None => return false, // field not in schema
            }
        } else {
            // Fallback: inline schema lookup
            match batch.schema().column_with_name("_hoodie_operation") {
                Some((idx, _)) => idx,
                None => return false,
            }
        };

        if let Some(arr) = batch.column(col_idx).as_any().downcast_ref::<StringArray>()
            && !arr.is_null(row_idx)
        {
            // The persisted value is the HoodieOperation NAME ("D" / "-U"),
            // NOT the enum identifier ("DELETE" / "UPDATE_BEFORE"). Java maps
            // it back with HoodieOperation.fromName; comparing against the
            // long names silently misses every operation-field delete and
            // resurrects the deleted rows.
            let op = arr.value(row_idx);
            if op == HOODIE_OPERATION_DELETE || op == HOODIE_OPERATION_UPDATE_BEFORE {
                return true;
            }
        }
        false
    }

    /// Check 3: Custom delete marker field matches configured value.
    ///
    /// Mirrors Java's `RecordContext.isCustomDeleteRecord(T, DeleteContext)`.
    /// Only applies when DeleteContext has a custom_delete_marker configured.
    fn is_custom_delete_record(
        &self,
        batch: &RecordBatch,
        row_idx: usize,
        delete_context: Option<&DeleteContext>,
    ) -> bool {
        let (key_field, marker_value) = match delete_context {
            Some(ctx) => match &ctx.custom_delete_marker {
                Some((k, v)) => (k.as_str(), v.as_str()),
                None => return false,
            },
            None => return false, // No custom marker without DeleteContext
        };

        let schema = batch.schema();
        if let Some((idx, _)) = schema.column_with_name(key_field) {
            let col = batch.column(idx);
            // String column
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>()
                && !arr.is_null(row_idx)
                && arr.value(row_idx) == marker_value
            {
                return true;
            }
            // Int32 column — compare stringified value against marker
            if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int32Array>()
                && !arr.is_null(row_idx)
                && arr.value(row_idx).to_string() == marker_value
            {
                return true;
            }
            // Int64 column — compare stringified value against marker
            if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>()
                && !arr.is_null(row_idx)
                && arr.value(row_idx).to_string() == marker_value
            {
                return true;
            }
        }
        false
    }

    // =========================================================================
    // Associated functions — engine-level operations (no config needed)
    // =========================================================================

    /// Get the schema from a buffered record's data.
    ///
    /// Mirrors Java's `RecordContext.getSchemaFromBufferRecord(BufferedRecord)`.
    ///
    /// In Java, this decodes the schema from the record's schema ID via a
    /// schema registry. In Arrow, we extract the schema directly from the
    /// record's payload batch (returns `None` for delete tombstones).
    pub fn get_schema_from_buffer_record(record: &BufferedRecord) -> Option<SchemaRef> {
        record.get_record().map(|batch| batch.schema())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::reader_v2::delete_context::DeleteContext;
    use arrow_array::{BooleanArray, Int32Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["k1"])),
                Arc::new(Int64Array::from(vec![42])),
            ],
        )
        .unwrap()
    }

    fn make_record_context() -> RecordContext {
        let table_config = HashMap::from([
            (
                HudiTableConfig::PopulatesMetaFields.as_ref().to_string(),
                "true".to_string(),
            ),
            (
                HudiTableConfig::OrderingFields.as_ref().to_string(),
                "ts".to_string(),
            ),
        ]);
        RecordContext::new(&table_config, "partition/path".to_string())
    }

    fn make_keyed_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, false),
        ]));
        let keys: Vec<String> = (0..num_rows).map(|i| format!("key_{i}")).collect();
        let names: Vec<Option<String>> = (0..num_rows).map(|i| Some(format!("name_{i}"))).collect();
        let timestamps: Vec<i64> = (0..num_rows).map(|i| i as i64).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(timestamps)),
            ],
        )
        .unwrap()
    }

    // =========================================================================
    // Associated function tests (IPC roundtrip, schema extraction)
    // =========================================================================

    #[test]
    fn test_get_schema_from_buffer_record() {
        let batch = make_test_batch();
        let schema = batch.schema();
        let record = BufferedRecord::new_data("k1".to_string(), batch, None);
        let extracted = RecordContext::get_schema_from_buffer_record(&record);
        assert_eq!(extracted.unwrap(), schema);
    }

    #[test]
    fn test_get_schema_from_delete_record_returns_none() {
        let record = BufferedRecord::new_delete("k1".to_string(), None);
        let extracted = RecordContext::get_schema_from_buffer_record(&record);
        assert!(extracted.is_none());
    }

    // =========================================================================
    // Instance method tests (key extraction, ordering, batch conversion)
    // =========================================================================

    #[test]
    fn test_get_record_keys() {
        let ctx = make_record_context();
        let batch = make_keyed_batch(3);
        let keys = ctx.get_record_keys(&batch).unwrap();
        assert_eq!(keys, vec!["key_0", "key_1", "key_2"]);
    }

    #[test]
    fn test_get_record_keys_missing_field() {
        // Virtual keys mode with a nonexistent field
        let table_config = HashMap::from([
            (
                HudiTableConfig::PopulatesMetaFields.as_ref().to_string(),
                "false".to_string(),
            ),
            (
                HudiTableConfig::RecordKeyFields.as_ref().to_string(),
                "nonexistent".to_string(),
            ),
        ]);
        let ctx = RecordContext::new(&table_config, String::new());
        let batch = make_keyed_batch(1);
        assert!(ctx.get_record_keys(&batch).is_err());
    }

    /// Virtual-key base batch: NO meta columns, real key column `id` (INT32) plus
    /// a `longField` precombine (matching the gluten `TestInsertTable2` virtual-key
    /// bulk-insert fixture — `primaryKey=id`, `preCombineField`, no meta fields).
    fn make_virtual_key_base_batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("longField", DataType::Int64, false),
        ]));
        let names: Vec<Option<String>> = ids.iter().map(|i| Some(format!("name_{i}"))).collect();
        let longs: Vec<i64> = ids.iter().map(|i| *i as i64 * 100).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(longs)),
            ],
        )
        .unwrap()
    }

    fn virtual_key_context() -> RecordContext {
        let table_config = HashMap::from([
            (
                HudiTableConfig::PopulatesMetaFields.as_ref().to_string(),
                "false".to_string(),
            ),
            (
                HudiTableConfig::RecordKeyFields.as_ref().to_string(),
                "id".to_string(),
            ),
            (
                HudiTableConfig::OrderingFields.as_ref().to_string(),
                "longField".to_string(),
            ),
        ]);
        RecordContext::new(&table_config, String::new())
    }

    /// Regression (lin/virtual-key-base-read): a virtual-key table whose record
    /// key is a non-string data column (`id INT`) must extract string keys, NOT
    /// error with "is not a StringArray". Before the fix, `get_record_keys` /
    /// `record_key_array` downcast the key column to `StringArray` unconditionally,
    /// erroring on the INT column; in the buffer base-merge that error dropped
    /// every base row (silent-wrong: 0 rows instead of the actual data).
    #[test]
    fn test_get_record_keys_virtual_key_int_column() {
        let ctx = virtual_key_context();
        assert_eq!(ctx.record_key_field, "id");
        let batch = make_virtual_key_base_batch(&[1, 42, 7]);
        let keys = ctx.get_record_keys(&batch).unwrap();
        assert_eq!(
            keys,
            vec!["1".to_string(), "42".to_string(), "7".to_string()]
        );
    }

    /// The materialized key array carries the same stringified INT keys, so the
    /// vectorized base-merge kernel can probe the log map by `&str`.
    #[test]
    fn test_record_key_array_virtual_key_int_column() {
        let ctx = virtual_key_context();
        let batch = make_virtual_key_base_batch(&[1, 42]);
        let key_array = ctx.record_key_array(&batch).unwrap();
        assert_eq!(key_array.len(), 2);
        assert_eq!(key_array.value(0), "1");
        assert_eq!(key_array.value(1), "42");
    }

    /// G-C: a SINGLE-field VIRTUAL key written by a `ComplexKeyGenerator` is stored
    /// as `field:value` (Java `KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen`,
    /// default `true`). The reader must reconstruct the SAME shape so a delete keyed
    /// `id:42` matches its base row (else the deleted row resurfaces).
    ///
    /// Discriminating: `SimpleKeyGenerator` (or no keygen) yields the bare `42`;
    /// `ComplexKeyGenerator` yields `id:42`. A v8 table with the legacy
    /// `hoodie.write.complex.keygen.new.encoding=true` opts OUT and yields `42`.
    #[test]
    fn test_record_key_array_single_field_complex_keygen_encodes_field_name() {
        let make_ctx = |extra: &[(&str, &str)]| {
            let mut cfg = HashMap::from([
                (
                    HudiTableConfig::PopulatesMetaFields.as_ref().to_string(),
                    "false".to_string(),
                ),
                (
                    HudiTableConfig::RecordKeyFields.as_ref().to_string(),
                    "id".to_string(),
                ),
            ]);
            for (k, v) in extra {
                cfg.insert(k.to_string(), v.to_string());
            }
            RecordContext::new(&cfg, String::new())
        };
        let keys = |ctx: &RecordContext| {
            let batch = make_virtual_key_base_batch(&[1, 42]);
            let arr = ctx.record_key_array(&batch).unwrap();
            (0..arr.len())
                .map(|i| arr.value(i).to_string())
                .collect::<Vec<_>>()
        };

        // ComplexKeyGenerator, single field → `id:val` (encode by default).
        let complex = make_ctx(&[(
            HudiTableConfig::KeyGeneratorClass.as_ref(),
            "org.apache.hudi.keygen.ComplexKeyGenerator",
        )]);
        assert!(complex.encode_single_key_field);
        assert_eq!(
            keys(&complex),
            vec!["id:1".to_string(), "id:42".to_string()]
        );

        // ComplexAvroKeyGenerator behaves identically.
        let complex_avro = make_ctx(&[(
            HudiTableConfig::KeyGeneratorClass.as_ref(),
            "org.apache.hudi.keygen.ComplexAvroKeyGenerator",
        )]);
        assert_eq!(
            keys(&complex_avro),
            vec!["id:1".to_string(), "id:42".to_string()]
        );

        // SimpleKeyGenerator → bare value (no field-name prefix).
        let simple = make_ctx(&[(
            HudiTableConfig::KeyGeneratorClass.as_ref(),
            "org.apache.hudi.keygen.SimpleKeyGenerator",
        )]);
        assert!(!simple.encode_single_key_field);
        assert_eq!(keys(&simple), vec!["1".to_string(), "42".to_string()]);

        // No keygen configured → default SimpleKeyGenerator behavior (bare value).
        let none = make_ctx(&[]);
        assert!(!none.encode_single_key_field);
        assert_eq!(keys(&none), vec!["1".to_string(), "42".to_string()]);

        // v8 table with the legacy new-encoding opt-out → bare value.
        let v8_new_encoding = make_ctx(&[
            (
                HudiTableConfig::KeyGeneratorClass.as_ref(),
                "org.apache.hudi.keygen.ComplexKeyGenerator",
            ),
            (HudiTableConfig::TableVersion.as_ref(), "8"),
            ("hoodie.write.complex.keygen.new.encoding", "true"),
        ]);
        assert!(!v8_new_encoding.encode_single_key_field);
        assert_eq!(
            keys(&v8_new_encoding),
            vec!["1".to_string(), "42".to_string()]
        );

        // v9 table ALWAYS encodes, even with new-encoding=true (>= NINE wins).
        let v9 = make_ctx(&[
            (
                HudiTableConfig::KeyGeneratorClass.as_ref(),
                "org.apache.hudi.keygen.ComplexKeyGenerator",
            ),
            (HudiTableConfig::TableVersion.as_ref(), "9"),
            ("hoodie.write.complex.keygen.new.encoding", "true"),
        ]);
        assert!(v9.encode_single_key_field);
        assert_eq!(keys(&v9), vec!["id:1".to_string(), "id:42".to_string()]);

        // A multi-field ComplexKeyGen is unaffected by the single-field flag — it
        // always encodes each field (`encode_single_key_field` stays false).
        let multi = {
            let mut cfg = HashMap::from([
                (
                    HudiTableConfig::PopulatesMetaFields.as_ref().to_string(),
                    "false".to_string(),
                ),
                (
                    HudiTableConfig::RecordKeyFields.as_ref().to_string(),
                    "id,name".to_string(),
                ),
                (
                    HudiTableConfig::KeyGeneratorClass.as_ref().to_string(),
                    "org.apache.hudi.keygen.ComplexKeyGenerator".to_string(),
                ),
            ]);
            cfg.remove("unused");
            RecordContext::new(&cfg, String::new())
        };
        assert!(!multi.encode_single_key_field);
    }

    /// Meta-field tables key on `_hoodie_record_key` (always `Utf8`): the fast
    /// clone path must keep returning the string keys verbatim (no regression).
    #[test]
    fn test_record_key_array_meta_field_string_column() {
        let ctx = make_record_context();
        let batch = make_keyed_batch(3);
        let key_array = ctx.record_key_array(&batch).unwrap();
        assert_eq!(
            (0..key_array.len())
                .map(|i| key_array.value(i).to_string())
                .collect::<Vec<_>>(),
            vec!["key_0", "key_1", "key_2"]
        );
    }

    /// A virtual-key column of a type hudi-rs does not derive record keys from
    /// (here `Float64`) must be rejected LOUDLY with `CoreError::Unsupported`,
    /// never silently producing a key form that could mis-match the writer's key
    /// generator.
    #[test]
    fn test_record_key_array_virtual_key_unsupported_type_errors() {
        let table_config = HashMap::from([
            (
                HudiTableConfig::PopulatesMetaFields.as_ref().to_string(),
                "false".to_string(),
            ),
            (
                HudiTableConfig::RecordKeyFields.as_ref().to_string(),
                "id".to_string(),
            ),
        ]);
        let ctx = RecordContext::new(&table_config, String::new());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let err = ctx.record_key_array(&batch).unwrap_err();
        assert!(
            matches!(err, CoreError::Unsupported(_)),
            "expected Unsupported, got: {err:?}"
        );
    }

    /// Build a base batch for a COMPOSITE virtual key over `id INT, name STRING`.
    /// `name` values are passed as `Option` so null/empty placeholder cases can be
    /// exercised.
    fn make_composite_key_batch(rows: &[(Option<i32>, Option<&str>)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ids: Vec<Option<i32>> = rows.iter().map(|(i, _)| *i).collect();
        let names: Vec<Option<String>> =
            rows.iter().map(|(_, n)| n.map(|s| s.to_string())).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    fn composite_key_context() -> RecordContext {
        let table_config = HashMap::from([
            (
                HudiTableConfig::PopulatesMetaFields.as_ref().to_string(),
                "false".to_string(),
            ),
            (
                HudiTableConfig::RecordKeyFields.as_ref().to_string(),
                "id,name".to_string(),
            ),
        ]);
        RecordContext::new(&table_config, String::new())
    }

    /// The constructor parses every `hoodie.table.recordkey.fields` entry into
    /// `record_key_fields`, keeping the first as the single-field `record_key_field`.
    #[test]
    fn test_composite_key_fields_parsed_from_config() {
        let ctx = composite_key_context();
        assert_eq!(ctx.record_key_fields, vec!["id", "name"]);
        assert_eq!(ctx.record_key_field, "id");
    }

    /// ENG-45059: a composite virtual key (>1 record-key field) with meta fields off
    /// must reconstruct the full `field:val,field:val` merge key per row — mirroring
    /// Java `KeyGenerator.constructRecordKey` (ComplexKeyGenerator) — so records that
    /// share the first field but differ on a later one get distinct keys and no longer
    /// collide. A non-string key column (`id INT`) is stringified, same as the
    /// single-field path.
    #[test]
    fn test_composite_virtual_key_two_fields() {
        let ctx = composite_key_context();
        let batch = make_composite_key_batch(&[
            (Some(1), Some("alice")),
            (Some(1), Some("bob")),
            (Some(2), Some("alice")),
        ]);
        let keys = ctx.record_key_array(&batch).unwrap();
        assert_eq!(
            (0..keys.len()).map(|i| keys.value(i)).collect::<Vec<_>>(),
            vec!["id:1,name:alice", "id:1,name:bob", "id:2,name:alice"],
        );
    }

    /// Null and empty field values map to the Java placeholders `__null__` /
    /// `__empty__` (KeyGenUtils), so a null and an empty string never collapse to the
    /// same key and stay distinguishable across the base/log sides.
    #[test]
    fn test_composite_virtual_key_null_and_empty_placeholders() {
        let ctx = composite_key_context();
        let batch = make_composite_key_batch(&[
            (Some(1), None),       // null name
            (Some(2), Some("")),   // empty name
            (None, Some("carol")), // null id
        ]);
        let keys = ctx.record_key_array(&batch).unwrap();
        assert_eq!(
            (0..keys.len()).map(|i| keys.value(i)).collect::<Vec<_>>(),
            vec![
                "id:1,name:__null__",
                "id:2,name:__empty__",
                "id:__null__,name:carol",
            ],
        );
    }

    /// A row whose every composite-key field is null or empty is rejected loudly
    /// (mirrors Java `HoodieKeyException`): a record key cannot be entirely null/empty,
    /// and silently keying such a row would merge unrelated records.
    #[test]
    fn test_composite_virtual_key_all_null_or_empty_errors() {
        let ctx = composite_key_context();
        let batch = make_composite_key_batch(&[(None, Some(""))]);
        let err = ctx.record_key_array(&batch).unwrap_err();
        assert!(
            matches!(err, CoreError::ReadFileSliceError(_)),
            "expected ReadFileSliceError for all-null/empty composite key, got: {err:?}"
        );
    }

    #[test]
    fn test_get_ordering_values_int64() {
        let ctx = make_record_context();
        let batch = make_keyed_batch(3);
        let values = ctx.get_ordering_values(&batch).unwrap().unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Some(OrderingValue::Long(0)));
        assert_eq!(values[1], Some(OrderingValue::Long(1)));
        assert_eq!(values[2], Some(OrderingValue::Long(2)));
    }

    #[test]
    fn test_get_ordering_values_double() {
        // ENG-38318 / A3c: a floating-point precombine field (e.g. `weight`, a
        // `double`) is a valid EVENT_TIME ordering key. The data-record read path
        // must surface it as `OrderingValue::Double` — not silently drop it to
        // `None`, which would degrade EVENT_TIME merge to latest-wins.
        let table_config = HashMap::from([
            (
                HudiTableConfig::PopulatesMetaFields.as_ref().to_string(),
                "true".to_string(),
            ),
            (
                "hoodie.table.ordering.fields".to_string(),
                "weight".to_string(),
            ),
        ]);
        let ctx = RecordContext::new(&table_config, String::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("weight", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["k0", "k1", "k2"])),
                Arc::new(arrow_array::Float64Array::from(vec![
                    Some(1.5),
                    Some(0.25),
                    None,
                ])),
            ],
        )
        .unwrap();
        let values = ctx.get_ordering_values(&batch).unwrap().unwrap();
        assert_eq!(values[0], Some(OrderingValue::Double(1.5)));
        assert_eq!(values[1], Some(OrderingValue::Double(0.25)));
        assert_eq!(
            values[2],
            Some(OrderingValue::Default),
            "null weight → null-coerced Default ordering value (G-B)"
        );
        // The higher weight must compare greater (drives EVENT_TIME winner pick).
        assert!(values[0] > values[1]);
    }

    #[test]
    fn test_get_ordering_values_temporal_integral_and_large_string() {
        // ENG-38318: every integer-backed comparable primitive is a valid
        // EVENT_TIME ordering key and must resolve (timestamp/date/byte/short/
        // boolean → Long; large-utf8 → String) instead of silently dropping to
        // `None` (which degrades EVENT_TIME merge to latest-wins).
        use arrow_array::{
            ArrayRef, BooleanArray, Date32Array, Int16Array, LargeStringArray,
            TimestampMicrosecondArray,
        };
        let run = |field: &str, dt: DataType, col: ArrayRef| -> Vec<Option<OrderingValue>> {
            let cfg = HashMap::from([
                (
                    HudiTableConfig::PopulatesMetaFields.as_ref().to_string(),
                    "true".to_string(),
                ),
                (
                    "hoodie.table.ordering.fields".to_string(),
                    field.to_string(),
                ),
            ]);
            let schema = Arc::new(Schema::new(vec![
                Field::new("_hoodie_record_key", DataType::Utf8, false),
                Field::new(field, dt, true),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(vec!["k0", "k1"])), col],
            )
            .unwrap();
            RecordContext::new(&cfg, String::new())
                .get_ordering_values(&batch)
                .unwrap()
                .unwrap()
        };

        // Timestamp(µs) → Long(raw i64); higher instant compares greater.
        let ts = run(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(100), Some(200)])),
        );
        assert_eq!(ts[0], Some(OrderingValue::Long(100)));
        assert!(ts[1] > ts[0]);

        // Date32 → Long(days since epoch).
        let d = run(
            "d",
            DataType::Date32,
            Arc::new(Date32Array::from(vec![Some(19000), None])),
        );
        assert_eq!(d[0], Some(OrderingValue::Long(19000)));
        assert_eq!(
            d[1],
            Some(OrderingValue::Default),
            "null date → null-coerced Default ordering value (G-B)"
        );

        // Int16 (short) → Long.
        let s = run(
            "s",
            DataType::Int16,
            Arc::new(Int16Array::from(vec![Some(7i16), Some(-3i16)])),
        );
        assert_eq!(s[0], Some(OrderingValue::Long(7)));
        assert!(s[0] > s[1]);

        // Boolean → Long(0/1).
        let b = run(
            "flag",
            DataType::Boolean,
            Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
        );
        assert_eq!(b[0], Some(OrderingValue::Long(1)));
        assert_eq!(b[1], Some(OrderingValue::Long(0)));

        // LargeUtf8 → String.
        let ls = run(
            "name",
            DataType::LargeUtf8,
            Arc::new(LargeStringArray::from(vec![Some("a"), Some("b")])),
        );
        assert_eq!(ls[0], Some(OrderingValue::String("a".to_string())));
        assert!(ls[1] > ls[0]);
    }

    #[test]
    fn test_get_ordering_values_no_fields() {
        // No precombine/ordering field → empty ordering_field_names
        let ctx = RecordContext::new(&HashMap::new(), String::new());
        let batch = make_keyed_batch(3);
        assert!(ctx.get_ordering_values(&batch).unwrap().is_none());
    }

    #[test]
    fn test_get_ordering_values_multi_column_composite() {
        // A table configured with multiple ordering fields (e.g. MySql Debezium's
        // `_event_bin_file,_event_pos`) builds an `OrderingValue::Composite` per
        // row, compared lexicographically field-by-field. The comma-separated
        // `ordering.fields` is split into a real list at construction.
        let table_config = HashMap::from([
            (
                HudiTableConfig::PopulatesMetaFields.as_ref().to_string(),
                "true".to_string(),
            ),
            (
                "hoodie.table.ordering.fields".to_string(),
                "file,pos".to_string(),
            ),
        ]);
        let ctx = RecordContext::new(&table_config, "partition/path".to_string());
        assert_eq!(
            ctx.ordering_field_names,
            vec!["file".to_string(), "pos".to_string()],
            "ordering.fields must be split into a real list"
        );

        // Batch with the two ordering columns: file (Utf8) + pos (Int64).
        let schema = Arc::new(Schema::new(vec![
            Field::new("file", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["bin.0001", "bin.0002"])),
                Arc::new(Int64Array::from(vec![500i64, 10i64])),
            ],
        )
        .unwrap();

        let values = ctx.get_ordering_values(&batch).unwrap().unwrap();
        assert_eq!(
            values[0],
            Some(OrderingValue::Composite(vec![
                OrderingValue::String("bin.0001".to_string()),
                OrderingValue::Long(500),
            ]))
        );
        assert_eq!(
            values[1],
            Some(OrderingValue::Composite(vec![
                OrderingValue::String("bin.0002".to_string()),
                OrderingValue::Long(10),
            ]))
        );
        // Lexicographic: row 1 (file "bin.0002") > row 0 (file "bin.0001"),
        // even though row 1's pos (10) < row 0's pos (500) — file dominates.
        assert!(values[1] > values[0], "first field dominates the ordering");
    }

    /// Both ordering-value entry points — the lazy per-row [`OrderingAccessor`]
    /// (base records) and the eager whole-column
    /// [`RecordContext::get_ordering_values`] (log records) — must extract the
    /// SAME, CORRECT `OrderingValue` for every supported scalar type. Asserted
    /// against independent expected constants (NOT lazy-vs-eager parity, which is
    /// tautological now that both resolve the column through
    /// [`OrderingAccessor::from_column`]). Widening beyond Int64/Utf8 is the fix: a
    /// `Double`/`Timestamp`/... precombine field previously extracted a value on
    /// the log side but `None` on the base side, silently degrading EVENT_TIME MOR
    /// merges to commit-time (log-always-wins).
    #[test]
    fn test_ordering_value_extraction_lazy_and_eager() {
        // Assert BOTH entry points yield `expected` for a single-ordering-column
        // batch — against an independent hand-written expectation, so a wrong
        // mapping cannot hide behind lazy==eager (both call `from_column`).
        fn assert_extracts(
            field: &str,
            array: arrow_array::ArrayRef,
            expected: &[Option<OrderingValue>],
        ) {
            let cfg = HashMap::from([(
                HudiTableConfig::OrderingFields.as_ref().to_string(),
                field.to_string(),
            )]);
            let ctx = RecordContext::new(&cfg, String::new());
            let keys: Vec<String> = (0..array.len()).map(|i| i.to_string()).collect();
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("_hoodie_record_key", DataType::Utf8, false),
                    Field::new(field, array.data_type().clone(), true),
                ])),
                vec![Arc::new(StringArray::from(keys)), array],
            )
            .unwrap();
            assert_eq!(
                ctx.get_ordering_values(&batch).unwrap().as_deref(),
                Some(expected),
                "eager get_ordering_values for `{field}`",
            );
            let acc = ctx.ordering_accessor(&batch).unwrap();
            for (i, exp) in expected.iter().enumerate() {
                assert_eq!(&acc.value_at(i), exp, "lazy value_at for `{field}` row {i}");
            }
        }
        // Parity helper retained for the composite case (whose values are validated
        // by `test_get_ordering_values_multi_column_composite`).
        fn assert_parity(ctx: &RecordContext, batch: &RecordBatch) {
            let eager = ctx.get_ordering_values(batch).unwrap().unwrap();
            let acc = ctx.ordering_accessor(batch).unwrap();
            for (i, expected) in eager.iter().enumerate() {
                assert_eq!(
                    &acc.value_at(i),
                    expected,
                    "composite lazy vs eager row {i}"
                );
            }
        }
        use OrderingValue::{Default, Double, Long, String as OStr};

        // Scalar types — absolute expected values (a wrong mapping cannot hide
        // behind lazy==eager, since both entry points call `from_column`).
        // A NULL cell coerces to `Default` (G-B); an `Int32` value of `0` also
        // coerces to `Default` (Java `Integer(0)` is the default; GAP-2).
        assert_extracts(
            "i64",
            Arc::new(Int64Array::from(vec![Some(5i64), None, Some(7i64)])),
            &[Some(Long(5)), Some(Default), Some(Long(7))],
        );
        assert_extracts(
            "i32",
            Arc::new(arrow_array::Int32Array::from(vec![3i32, -2i32, 0i32])),
            &[Some(Long(3)), Some(Long(-2)), Some(Default)],
        );
        // A genuine bigint (Int64) `0` is NOT the default — it stays `Long(0)` (GAP-2).
        assert_extracts(
            "i64zero",
            Arc::new(Int64Array::from(vec![Some(0i64), Some(2i64)])),
            &[Some(Long(0)), Some(Long(2))],
        );
        assert_extracts(
            "i16",
            Arc::new(arrow_array::Int16Array::from(vec![Some(3i16), None])),
            &[Some(Long(3)), Some(Default)],
        );
        assert_extracts(
            "flag",
            Arc::new(arrow_array::BooleanArray::from(vec![true, false])),
            &[Some(Long(1)), Some(Long(0))],
        );
        assert_extracts(
            "d32",
            Arc::new(arrow_array::Date32Array::from(vec![10, 5])),
            &[Some(Long(10)), Some(Long(5))],
        );
        assert_extracts(
            "tsmicro",
            Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![
                100i64, 50i64,
            ])),
            &[Some(Long(100)), Some(Long(50))],
        );
        // Float64/Float32 → Double: the regression case (previously `None` on the
        // base-record path, so a lower-ordering log update wrongly won the merge).
        assert_extracts(
            "f64",
            Arc::new(arrow_array::Float64Array::from(vec![
                Some(0.9f64),
                None,
                Some(0.2f64),
            ])),
            &[Some(Double(0.9)), Some(Default), Some(Double(0.2))],
        );
        assert_extracts(
            "f32",
            Arc::new(arrow_array::Float32Array::from(vec![1.5f32, 2.5f32])),
            &[Some(Double(1.5)), Some(Double(2.5))],
        );
        assert_extracts(
            "name",
            Arc::new(StringArray::from(vec!["x", "y"])),
            &[Some(OStr("x".to_string())), Some(OStr("y".to_string()))],
        );
        // Remaining Int-like/Utf8 variants so every `from_column`/`value_at` arm
        // is exercised (a swapped downcast or Timestamp unit would fail here).
        assert_extracts(
            "i8",
            Arc::new(arrow_array::Int8Array::from(vec![1i8, -1i8])),
            &[Some(Long(1)), Some(Long(-1))],
        );
        assert_extracts(
            "d64",
            Arc::new(arrow_array::Date64Array::from(vec![1000i64, 2000i64])),
            &[Some(Long(1000)), Some(Long(2000))],
        );
        assert_extracts(
            "tssec",
            Arc::new(arrow_array::TimestampSecondArray::from(vec![7i64, 3i64])),
            &[Some(Long(7)), Some(Long(3))],
        );
        assert_extracts(
            "tsmilli",
            Arc::new(arrow_array::TimestampMillisecondArray::from(vec![
                8i64, 4i64,
            ])),
            &[Some(Long(8)), Some(Long(4))],
        );
        assert_extracts(
            "tsnano",
            Arc::new(arrow_array::TimestampNanosecondArray::from(vec![
                9i64, 5i64,
            ])),
            &[Some(Long(9)), Some(Long(5))],
        );
        assert_extracts(
            "lname",
            Arc::new(arrow_array::LargeStringArray::from(vec!["p", "q"])),
            &[Some(OStr("p".to_string())), Some(OStr("q".to_string()))],
        );
        // Decimal128 → Decimal, preserving the i128 unscaled value + scale (the
        // mirror of Java's DecimalWrapper/BigDecimal ordering key). 123.45, null,
        // -0.50 at scale 2.
        assert_extracts(
            "amount",
            Arc::new(
                arrow_array::Decimal128Array::from(vec![Some(12345i128), None, Some(-50i128)])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            &[
                Some(OrderingValue::Decimal {
                    unscaled: 12345,
                    scale: 2,
                }),
                Some(Default),
                Some(OrderingValue::Decimal {
                    unscaled: -50,
                    scale: 2,
                }),
            ],
        );

        // Multi-field composite → the eager `Precomputed` accessor arm. Its values
        // are validated by `test_get_ordering_values_multi_column_composite`; here
        // we confirm it IS a Composite and that the lazy accessor indexes the same
        // composite the eager path builds.
        let comp_ctx = RecordContext::new(
            &HashMap::from([(
                "hoodie.table.ordering.fields".to_string(),
                "file,pos".to_string(),
            )]),
            String::new(),
        );
        let comp_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("file", DataType::Utf8, false),
                Field::new("pos", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["bin.1", "bin.2"])),
                Arc::new(Int64Array::from(vec![500i64, 10i64])),
            ],
        )
        .unwrap();
        assert!(
            matches!(
                comp_ctx.get_ordering_values(&comp_batch).unwrap().unwrap()[0],
                Some(OrderingValue::Composite(_))
            ),
            "multi-field ordering must build a Composite",
        );
        assert_parity(&comp_ctx, &comp_batch);
    }

    #[test]
    fn test_batch_to_buffered_records() {
        let ctx = make_record_context();
        let batch = Arc::new(make_keyed_batch(3));
        let records = ctx.batch_to_buffered_records(&batch, None).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].0, "key_0");
        assert_eq!(records[1].0, "key_1");
        assert_eq!(records[2].0, "key_2");
        assert!(!records[0].1.is_delete());
        assert!(!records[0].1.is_empty());
        // Check ordering values were extracted
        assert_eq!(records[0].1.ordering_value, Some(OrderingValue::Long(0)));
        // A2: payloads are zero-copy BatchRefs into the shared source batch, and
        // each addresses its own row — full data must round-trip.
        let r0 = records[0].1.get_record().unwrap();
        let key0 = r0
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        assert_eq!(key0, "key_0");
    }

    #[test]
    fn test_batch_to_buffered_records_empty() {
        let ctx = make_record_context();
        let batch = Arc::new(make_keyed_batch(0));
        let records = ctx.batch_to_buffered_records(&batch, None).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_is_delete_record_hoodie_is_deleted() {
        let ctx = RecordContext::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("_hoodie_is_deleted", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["k1", "k2"])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
            ],
        )
        .unwrap();

        assert!(ctx.is_delete_record(&batch, 0, None));
        assert!(!ctx.is_delete_record(&batch, 1, None));
    }

    /// The `_hoodie_operation` meta column stores the HoodieOperation NAME as the
    /// writer persists it: DELETE="D", UPDATE_BEFORE="-U", UPDATE_AFTER="U",
    /// INSERT="I". A DELETE or UPDATE_BEFORE row is a delete; UPDATE_AFTER and
    /// INSERT are live rows.
    ///
    /// Discriminating: before the fix hudi-rs compared against the long enum
    /// identifiers ("DELETE"/"UPDATE_BEFORE"), which never match the persisted
    /// short names, so operation-field deletes were silently ignored and the
    /// deleted rows resurfaced (TestDataSourceReadWithDeletes: 4 rows, not 2).
    #[test]
    fn test_is_delete_record_hoodie_operation() {
        let ctx = RecordContext::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("_hoodie_operation", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["k1", "k2", "k3", "k4"])),
                Arc::new(StringArray::from(vec![
                    Some("D"),  // DELETE -> delete
                    Some("-U"), // UPDATE_BEFORE -> delete
                    Some("U"),  // UPDATE_AFTER -> live
                    Some("I"),  // INSERT -> live
                ])),
            ],
        )
        .unwrap();

        assert!(
            ctx.is_delete_record(&batch, 0, None),
            "D (DELETE) is a delete"
        );
        assert!(
            ctx.is_delete_record(&batch, 1, None),
            "-U (UPDATE_BEFORE) is a delete"
        );
        assert!(
            !ctx.is_delete_record(&batch, 2, None),
            "U (UPDATE_AFTER) is a live row"
        );
        assert!(
            !ctx.is_delete_record(&batch, 3, None),
            "I (INSERT) is a live row"
        );
    }

    #[test]
    fn test_delete_batch_to_keys_with_ordering_extracts_ordering() {
        // Mirrors a HoodieDeleteBlock batch: recordKey (0), partitionPath (1),
        // orderingVal (2). Regression guard for ENG-38318: the ordering value
        // MUST survive extraction so EVENT_TIME delete merge can reject a
        // lower-ordering delete. Row "9" has a null ordering → the null-coerced
        // `Default` (G-B); `Default` and `None` are both the natural-order default.
        let ctx = RecordContext::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("recordKey", DataType::Utf8, false),
            Field::new("partitionPath", DataType::Utf8, true),
            Field::new("orderingVal", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["2", "5", "9"])),
                Arc::new(StringArray::from(vec![Some(""), Some(""), Some("")])),
                Arc::new(Int64Array::from(vec![Some(99), Some(101), None])),
            ],
        )
        .unwrap();

        let entries = ctx.delete_batch_to_keys_with_ordering(&batch).unwrap();
        assert_eq!(
            entries,
            vec![
                ("2".to_string(), Some(OrderingValue::Long(99))),
                ("5".to_string(), Some(OrderingValue::Long(101))),
                ("9".to_string(), Some(OrderingValue::Default)),
            ]
        );
    }

    #[test]
    fn test_delete_batch_to_keys_with_ordering_unsupported_bare_primitive_is_loud() {
        // Restores fix 34cc3f4 (reverted during the vectorized-merge work): a
        // bare-primitive orderingVal column of an unsupported type must reject
        // LOUDLY, not silently degrade to all-`None` — which would make the
        // delete win unconditionally under EVENT_TIME (silent-wrong). `Binary`
        // is used (not the original's Float64) so the test stays valid as the
        // supported scalar set widens upstack (#61 adds Float, #66 Decimal):
        // Binary is not a comparable ordering scalar in ANY version of
        // `column_to_ordering_values`, so the reject-loudly contract holds after
        // the fix is rebased up the stack.
        use arrow_array::BinaryArray;
        let ctx = RecordContext::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("recordKey", DataType::Utf8, false),
            Field::new("partitionPath", DataType::Utf8, true),
            Field::new("orderingVal", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["2"])),
                Arc::new(StringArray::from(vec![Some("")])),
                Arc::new(BinaryArray::from_vec(vec![&b"xy"[..]])),
            ],
        )
        .unwrap();
        assert!(
            matches!(
                ctx.delete_batch_to_keys_with_ordering(&batch),
                Err(crate::error::CoreError::Unsupported(_))
            ),
            "unsupported bare-primitive delete ordering type must reject loudly"
        );
    }

    #[test]
    fn test_scalar_ordering_value_supported_types() {
        use arrow_array::{Int32Array, Int64Array};
        assert_eq!(
            RecordContext::scalar_ordering_value(&Int64Array::from(vec![99i64]), 0).unwrap(),
            Some(OrderingValue::Long(99))
        );
        assert_eq!(
            RecordContext::scalar_ordering_value(&Int32Array::from(vec![7i32]), 0).unwrap(),
            Some(OrderingValue::Long(7))
        );
        assert_eq!(
            RecordContext::scalar_ordering_value(&StringArray::from(vec!["x"]), 0).unwrap(),
            Some(OrderingValue::String("x".to_string()))
        );
        // Floating-point ordering fields (e.g. a `double`/`float` precombine).
        assert_eq!(
            RecordContext::scalar_ordering_value(&arrow_array::Float64Array::from(vec![1.5f64]), 0)
                .unwrap(),
            Some(OrderingValue::Double(1.5))
        );
        assert_eq!(
            RecordContext::scalar_ordering_value(&arrow_array::Float32Array::from(vec![0.5f32]), 0)
                .unwrap(),
            Some(OrderingValue::Double(0.5))
        );
        // Temporal / integral / boolean / large-string ordering fields all decode
        // on the delete path too (integer-backed → Long).
        assert_eq!(
            RecordContext::scalar_ordering_value(
                &arrow_array::TimestampMicrosecondArray::from(vec![1_700_000_000_000_000i64]),
                0
            )
            .unwrap(),
            Some(OrderingValue::Long(1_700_000_000_000_000))
        );
        assert_eq!(
            RecordContext::scalar_ordering_value(
                &arrow_array::Date32Array::from(vec![19_000i32]),
                0
            )
            .unwrap(),
            Some(OrderingValue::Long(19_000))
        );
        assert_eq!(
            RecordContext::scalar_ordering_value(&arrow_array::Int16Array::from(vec![7i16]), 0)
                .unwrap(),
            Some(OrderingValue::Long(7))
        );
        assert_eq!(
            RecordContext::scalar_ordering_value(&BooleanArray::from(vec![true]), 0).unwrap(),
            Some(OrderingValue::Long(1))
        );
        assert_eq!(
            RecordContext::scalar_ordering_value(
                &arrow_array::LargeStringArray::from(vec!["x"]),
                0
            )
            .unwrap(),
            Some(OrderingValue::String("x".to_string()))
        );
        // Decimal ordering field (e.g. a Hudi `DecimalWrapper` delete-block value):
        // 123.45 at scale 2 → Decimal { unscaled: 12345, scale: 2 }.
        assert_eq!(
            RecordContext::scalar_ordering_value(
                &arrow_array::Decimal128Array::from(vec![12345i128])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
                0
            )
            .unwrap(),
            Some(OrderingValue::Decimal {
                unscaled: 12345,
                scale: 2
            })
        );
        // A null cell carries no ordering (natural-order default).
        assert_eq!(
            RecordContext::scalar_ordering_value(&Int64Array::from(vec![None::<i64>]), 0).unwrap(),
            None
        );
    }

    #[test]
    fn test_scalar_ordering_value_out_of_domain_decimal_scale_is_loud() {
        // A Decimal128 whose scale is outside the supported `0..=38` domain is
        // out-of-contract: the value-based rescale in `cmp_decimal` cannot be
        // exact, so the DELETE path must refuse it LOUDLY (not silently drop the
        // ordering → delete-always-wins). Negative scale is the arrow-constructible
        // out-of-domain case. In-domain decimals are covered by
        // `test_scalar_ordering_value_supported_types`.
        let out_of_domain = arrow_array::Decimal128Array::from(vec![12345i128])
            .with_precision_and_scale(20, -1)
            .expect("arrow permits a negative decimal scale");
        assert!(
            RecordContext::scalar_ordering_value(&out_of_domain, 0).is_err(),
            "out-of-domain decimal scale must error, not silently return None"
        );
    }

    #[test]
    fn test_scalar_ordering_value_unsupported_type_is_loud() {
        // A genuinely unrepresentable ordering type must be a LOUD error — silently
        // dropping it would make an EVENT_TIME delete win unconditionally (ENG-38318
        // regression guard). Binary is not a valid Comparable ordering field, so it
        // stands in for the "no representation" case. NB: integral, temporal,
        // boolean, float, decimal, and string types are all SUPPORTED — see
        // test_scalar_ordering_value_supported_types.
        let binary = arrow_array::BinaryArray::from(vec![b"blob".as_ref()]);
        let result = RecordContext::scalar_ordering_value(&binary, 0);
        assert!(
            result.is_err(),
            "unsupported ordering type must error, not silently return None"
        );
    }

    #[test]
    fn test_decode_delete_wrapper_ordering_union() {
        // Pins the REAL production path: a Hudi delete-block `orderingVal` is an
        // Arrow DenseUnion of avro wrapper structs. Build one with a scalar
        // `LongWrapper{value:i64}` and a composite `ArrayWrapper{wrappedValues}`
        // (multi-field ordering, no `value` field) and assert the scalar decodes
        // while the composite is a LOUD error (not a silent drop → delete-wins).
        use arrow_array::{ArrayRef, Int64Array, StructArray, UnionArray};
        use arrow_buffer::ScalarBuffer;
        use arrow_schema::{Field, UnionFields};

        let long_wrapper = StructArray::from(vec![(
            Arc::new(Field::new("value", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![99i64])) as ArrayRef,
        )]);
        let array_wrapper = StructArray::from(vec![(
            Arc::new(Field::new("wrappedValues", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![0i64])) as ArrayRef,
        )]);
        let union_fields = UnionFields::try_new(
            vec![0_i8, 1_i8],
            vec![
                Field::new("LongWrapper", long_wrapper.data_type().clone(), false),
                Field::new("ArrayWrapper", array_wrapper.data_type().clone(), false),
            ],
        )
        .unwrap();
        let type_ids = ScalarBuffer::<i8>::from(vec![0_i8, 1_i8]);
        let offsets = ScalarBuffer::<i32>::from(vec![0_i32, 0_i32]);
        let children: Vec<ArrayRef> = vec![Arc::new(long_wrapper), Arc::new(array_wrapper)];
        let union = UnionArray::try_new(union_fields, type_ids, Some(offsets), children).unwrap();

        assert_eq!(
            RecordContext::decode_delete_wrapper_ordering(&union, 0).unwrap(),
            Some(OrderingValue::Long(99)),
            "LongWrapper variant must decode to Long(99)"
        );
        assert!(
            RecordContext::decode_delete_wrapper_ordering(&union, 1).is_err(),
            "composite (multi-field) ArrayWrapper ordering must be a loud error"
        );
    }

    #[test]
    fn test_delete_batch_to_keys_with_ordering_missing_ordering_col() {
        // No orderingVal column → every delete falls back to None ordering
        // (delete-always-wins), matching the key-only extractor.
        let ctx = RecordContext::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("recordKey", DataType::Utf8, false),
            Field::new("partitionPath", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["2"])),
                Arc::new(StringArray::from(vec![Some("")])),
            ],
        )
        .unwrap();
        let entries = ctx.delete_batch_to_keys_with_ordering(&batch).unwrap();
        assert_eq!(entries, vec![("2".to_string(), None)]);
    }

    #[test]
    fn test_is_delete_record_no_markers() {
        let ctx = RecordContext::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["k1"])),
                Arc::new(Int64Array::from(vec![42])),
            ],
        )
        .unwrap();

        assert!(!ctx.is_delete_record(&batch, 0, None));
    }

    #[test]
    fn test_is_custom_delete_record_int32_marker() {
        let ctx = RecordContext::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("counter", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k1", "k2"])),
                Arc::new(arrow_array::Int32Array::from(vec![3, 1])),
            ],
        )
        .unwrap();
        let delete_ctx = DeleteContext {
            custom_delete_marker: Some(("counter".to_string(), "3".to_string())),
            has_built_in_delete_field: false,
            hoodie_operation_pos: None,
            reader_schema: schema,
        };
        // counter=3 matches marker → delete
        assert!(ctx.is_delete_record(&batch, 0, Some(&delete_ctx)));
        // counter=1 does not match → not delete
        assert!(!ctx.is_delete_record(&batch, 1, Some(&delete_ctx)));
    }

    #[test]
    fn test_default_record_context() {
        let ctx = RecordContext::default();
        assert_eq!(ctx.record_key_field, "_hoodie_record_key");
        assert!(ctx.ordering_field_names.is_empty());
        assert!(ctx.populate_meta_fields);
        assert!(ctx.partition_path.is_empty());
    }
}
