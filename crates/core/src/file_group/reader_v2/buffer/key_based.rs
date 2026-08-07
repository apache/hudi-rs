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

//! Mirrors `org.apache.hudi.common.table.read.buffer.KeyBasedFileGroupRecordBuffer`.
//!
//! The default record buffer. Deduplicates by record key using a HashMap,
//! resolves conflicts via ordering value comparison (delta merge).
//!
//! ## How it's selected (in `DefaultFileGroupRecordBufferLoader`):
//! ```text
//! !is_skip_merge && !sort_outputs && !(use_record_position && base_file_present)
//!   → KeyBasedFileGroupRecordBuffer (DEFAULT)
//! ```
//!
//! ## Method mapping (Java → Rust):
//! - `processDataBlock` → `process_data_block`
//! - `processNextDataRecord` → `process_next_data_record`
//! - `processDeleteBlock` → `process_delete_block`
//! - `processNextDeletedRecord` → `process_next_deleted_record`
//! - `doHasNext` → `do_has_next`
//! - `hasNextBaseRecord` → `has_next_base_record`

use crate::Result;
use crate::file_group::log_file::log_block::{LogBlock, LogBlockContent};
use crate::file_group::reader_v2::buffer::record_buffer::FileGroupRecordBuffer;
use crate::file_group::reader_v2::buffer::record_positions::base_row_position_array;
use crate::file_group::reader_v2::buffer::row_extraction::{
    reconcile_batch_to_schema, records_to_batch,
};
use crate::file_group::reader_v2::buffer::spillable_map::{
    CONFIG_MERGE_MAX_SIZE, MergeMap, SpillConfig, SpillableRecordMap,
};
use crate::file_group::reader_v2::buffer::{BufferType, HoodieFileGroupRecordBuffer};
use crate::file_group::reader_v2::buffered_record::{
    BufferedRecord, BufferedRecords, DeleteRecord, OrderingValue,
};
use crate::file_group::reader_v2::merge_iterator::DEFAULT_BATCH_SIZE;
// `CoreError` / `RecordPayload` are used only by the test-only compaction
// primitive and the unit tests (A6e moved production compaction onto the map).
#[cfg(test)]
use crate::error::CoreError;
#[cfg(test)]
use crate::file_group::reader_v2::buffered_record::RecordPayload;
use crate::file_group::reader_v2::reader_context::ReaderContext;
use crate::file_group::reader_v2::record_context::RecordContext;
use crate::file_group::reader_v2::record_merger::{
    BufferedRecordMergerFactory, should_keep_newer_record,
};
use crate::file_group::reader_v2::update_processor::{UpdateStats, create_update_processor};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray,
};
use arrow_buffer::BooleanBufferBuilder;
use arrow_schema::DataType;
use arrow_schema::{Schema, SchemaRef};
use std::collections::HashMap;
use std::sync::Arc;

// The compaction live-row threshold is a single source of truth in
// `spillable_map` (A6e shares it between over-budget eviction and the
// end-of-scan safety valve). Re-imported here for the free-function compaction
// primitive's tests and the trait delegate.
use crate::file_group::reader_v2::buffer::spillable_map::COMPACTION_LIVE_RATIO;

#[cfg(test)]
use crate::config::table::HudiTableConfig;

/// Key-based file group record buffer.
///
/// Mirrors Java's `KeyBasedFileGroupRecordBuffer<T>`.
///
/// ## Per-record merge during log scanning:
/// ```text
/// processDataBlock(batch):
///   for each row → processNextDataRecord(record, key)
///     → records.get(key) → deltaMerge(new, existing) → records.put(key, merged)
///
/// processDeleteBlock(batch):
///   for each row → processNextDeletedRecord(deleteRecord, key)
///     → records.get(key) → deltaMergeDelete(delete, existing) → records.put(key, merged)
/// ```
///
/// ## Base-vs-log merge at read time:
/// ```text
/// doHasNext():
///   while baseIterator.hasNext():
///     if hasNextBaseRecord(base): return true
///   return hasNextLogRecord()
///
/// hasNextBaseRecord(base):
///   key = extractKey(base)
///   logRecord = records.remove(key)
///   return super.hasNextBaseRecord(base, logRecord)
/// ```
/// How a base-file row is matched to a buffered log record during the merge.
///
/// The merge kernels are identical except for the per-row lookup key: key-based
/// merge looks up the row's record key; position-based merge looks up the row's
/// physical base-file position (read from the synthetic row-index column). This
/// enum lets [`KeyBasedFileGroupRecordBuffer`]'s kernels serve both, so the
/// position buffer reuses them rather than duplicating the vectorized/scalar
/// merge logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BaseMatch {
    /// Match by record key (key-based merge).
    RecordKey,
    /// Match by base-file row position, read from the row-index column
    /// ([`ROW_INDEX_TEMPORARY_COLUMN_NAME`](super::record_positions::ROW_INDEX_TEMPORARY_COLUMN_NAME)).
    Position,
}

#[derive(Debug)]
pub struct KeyBasedFileGroupRecordBuffer {
    /// Common buffer state.
    pub base: FileGroupRecordBuffer,

    /// Reader context (mirrors Java's readerContext).
    pub reader_context: Arc<ReaderContext>,

    /// Record context for record-level operations (key extraction, ordering, etc.).
    /// Mirrors Java's `readerContext.getRecordContext()`.
    pub record_context: RecordContext,

    /// Partial-update (`FILL_UNAVAILABLE`) sentinel. `Some(v)` when the table's
    /// payload uses a Debezium-style "unavailable value" marker (e.g. Postgres
    /// TOAST: `__debezium_unavailable_value`): a winning log column equal to `v`
    /// is replaced with the prior base value during merge. `None` disables the
    /// rule (the common case) so non-partial-update reads pay nothing. Read from
    /// `hoodie.record.merge.property.partial.update.unavailable.value`.
    unavailable_value: Option<String>,

    /// Partial-update `IGNORE_DEFAULTS` flag. `true` when the table's
    /// `hoodie.table.partial.update.mode` is `IGNORE_DEFAULTS` (e.g. a table
    /// written with `PartialUpdateAvroPayload`): a winning full-schema log
    /// column whose value equals the field's declared default keeps the prior
    /// (base/older) value during merge instead of overwriting with the default.
    /// `false` (the common case) disables the rule so non-partial-update reads
    /// pay nothing. Mirrors Java `PartialUpdateHandler.reconcileDefaultValues`.
    ignore_defaults: bool,

    /// Per-field IGNORE_DEFAULTS retention rule, keyed by field name, derived
    /// from the table's declared Avro field defaults (see [`DefaultRetain`]).
    /// Populated only when [`Self::ignore_defaults`] is set and the Avro schema
    /// JSON is available; empty otherwise. A field absent from the map falls
    /// back to [`DefaultRetain::OnNull`] (the behavior for a field with no
    /// declared default).
    default_retains: HashMap<String, DefaultRetain>,
}

/// Per-field IGNORE_DEFAULTS retention rule, mirroring Java
/// `PartialUpdateHandler.reconcileDefaultValues`, whose
/// `toJavaDefaultValue(field) == newValue` is a *reference* equality (`==`) on
/// the boxed values `RecordContext.getValue` returns:
///  - no default, or a null default → retain the prior value when the incoming
///    value is NULL (`null == null`);
///  - an integer-family default within the JVM Integer/Long cache
///    (`-128..=127`) → retain when the incoming value equals it (autoboxing
///    reuses the cached instance only inside that range, so `==` holds);
///  - a boolean default → retain when the incoming value equals it (`Boolean`
///    boxes are always cached);
///  - anything else — string/bytes/float/double/enum/record/array/map, or an
///    out-of-cache integer → never retain, because Java's `==` compares two
///    distinct boxed objects and is always false.
#[derive(Clone, Debug, PartialEq)]
enum DefaultRetain {
    /// Retain the prior value when the incoming value is null.
    OnNull,
    /// Retain when the incoming integer value equals this (cache-range) default.
    OnInt(i64),
    /// Retain when the incoming boolean value equals this default.
    OnBool(bool),
    /// Never retain — the default can't be matched by Java's reference `==`.
    Never,
}

/// Reader-config key carrying the Debezium-style partial-update "unavailable
/// value" sentinel (set by Hudi's `HoodieTableConfig` for payloads like
/// `PostgresDebeziumAvroPayload`). Mirrors Java's
/// `RECORD_MERGE_PROPERTY_PREFIX + "partial.update.unavailable.value"`.
const PARTIAL_UPDATE_UNAVAILABLE_VALUE: &str =
    "hoodie.record.merge.property.partial.update.unavailable.value";

/// Table-config key carrying the partial-update mode
/// (`hoodie.table.partial.update.mode`; set by Hudi's `HoodieTableConfig`, e.g.
/// for `PartialUpdateAvroPayload`). When its value is
/// [`PARTIAL_UPDATE_MODE_IGNORE_DEFAULTS`], a winning full-schema log column
/// whose value is the field default keeps the prior value during merge. Mirrors
/// Java `HoodieTableConfig.PARTIAL_UPDATE_MODE`.
const PARTIAL_UPDATE_MODE: &str = "hoodie.table.partial.update.mode";

/// `PARTIAL_UPDATE_MODE` value selecting the IGNORE_DEFAULTS blend (a default
/// column in the incoming record retains the prior value). Mirrors Java
/// `PartialUpdateMode.IGNORE_DEFAULTS`.
const PARTIAL_UPDATE_MODE_IGNORE_DEFAULTS: &str = "IGNORE_DEFAULTS";

/// Name prefix of Hudi's writer-managed meta columns (`_hoodie_record_key`,
/// `_hoodie_commit_time`, ...). Used to exempt meta columns from the
/// partial-update blends.
const HOODIE_META_COLUMN_PREFIX: &str = "_hoodie_";

/// True if `name` is a record-key column (member of `key_field_names`) or a
/// `_hoodie_`-prefixed meta column — the columns the partial-update blends
/// ([`fill_unavailable_from_base`] / [`reconcile_defaults_from_prior`]) must
/// never substitute from the prior row.
fn is_key_or_meta_column(name: &str, key_field_names: &[String]) -> bool {
    name.starts_with(HOODIE_META_COLUMN_PREFIX) || key_field_names.iter().any(|k| k == name)
}

impl KeyBasedFileGroupRecordBuffer {
    /// Fill a partial-update merge WINNER's default/unavailable columns from a
    /// `source` row (the loser or the prior record), dispatching on the table's
    /// partial-update mode: IGNORE_DEFAULTS → [`reconcile_defaults_from_prior`],
    /// FILL_UNAVAILABLE → [`fill_unavailable_from_base`]. Mirrors Java
    /// `PartialUpdateHandler.partialMerge`. A no-op (`Ok((target, false))`) when
    /// neither mode is configured. `source_idx` selects the row in `source`
    /// (log-vs-log passes a single-row batch at index 0).
    ///
    /// Returns the (possibly rewritten) record and a `bool` that is `true` only
    /// when a default/sentinel column was actually substituted — callers on the
    /// base-vs-log kept-row fast path use it to avoid materialising an unchanged
    /// replacement.
    fn fill_partial(
        &self,
        target: BufferedRecord,
        source: &RecordBatch,
        source_idx: usize,
    ) -> Result<(BufferedRecord, bool)> {
        if self.ignore_defaults {
            reconcile_defaults_from_prior(
                target,
                source,
                source_idx,
                &self.default_retains,
                &self.record_context.record_key_fields,
            )
        } else if let Some(sentinel) = self.unavailable_value.as_deref() {
            fill_unavailable_from_base(
                target,
                source,
                source_idx,
                sentinel,
                &self.record_context.record_key_fields,
            )
        } else {
            Ok((target, false))
        }
    }

    pub fn new(
        reader_context: Arc<ReaderContext>,
        merge_mode: String,
        emit_delete: bool,
    ) -> crate::Result<Self> {
        let merger = BufferedRecordMergerFactory::create(&merge_mode)?;
        let update_processor = create_update_processor(emit_delete);
        // Get the shared RecordContext from ReaderContext (mirrors Java's
        // readerContext.getRecordContext() returning the same instance).
        let record_context = reader_context.get_record_context().clone();

        // Extract reader_schema from schema_handler.required_schema at
        // construction time. Mirrors Java's FileGroupRecordBuffer constructor:
        //   this.readerSchema = readerContext.getSchemaHandler().getRequiredSchema()
        let reader_schema = reader_context.schema_handler.required_schema.clone();

        // Get the canonical DeleteContext from the schema handler (single source
        // of truth). Mirrors Java's `readerContext.getSchemaHandler().getDeleteContext()`.
        //
        // The schema handler creates the DeleteContext during
        // `prepare_required_schema()` and stores it. HoodieFileGroupReader
        // propagates the prepared schema_handler onto reader_context before
        // the buffer is created, so `reader_context.schema_handler.delete_context()`
        // always returns the canonical instance.
        //
        // Ordering invariant: the only construction path runs
        // `prepare_required_schema` first, so a `None` here means an upstream
        // wiring bug. This constructor already returns `Result`, so surface it
        // as a typed error rather than panicking in library code.
        let delete_context = reader_context
            .schema_handler
            .delete_context()
            .cloned()
            .ok_or_else(|| {
                crate::error::CoreError::ReadFileSliceError(
                    "DeleteContext must be set on schema_handler by prepare_required_schema() \
                     before constructing the record buffer"
                        .to_string(),
                )
            })?;

        // Enrich DeleteContext with reader schema at construction time.
        // Mirrors Java's: deleteContext = readerContext.getSchemaHandler()
        //     .getDeleteContext().withReaderSchema(this.readerSchema)
        let delete_context = if let Some(ref schema) = reader_schema {
            delete_context.with_reader_schema(schema.clone())
        } else {
            delete_context
        };

        // Build the size-tracked, RocksDB-spillable merge map (A1) from the
        // reader config. Parses hoodie.memory.merge.max.size / spillable.map.path
        // / spillable.diskmap.type (BITCASK and ROCKS_DB both → RocksDB backend).
        let spill_config = SpillConfig::from_config(&reader_context.hoodie_reader_config)?;
        // Diagnostic (ENG-45062/I-33): the resolved in-memory spill threshold and whether the
        // merge-memory budget key reached this map. gluten now forwards
        // `hoodie.memory.merge.max.size` in hoodie_reader_config, so `budget_key_present=true`
        // and the threshold tracks the operator/computed budget; a `false` here means the budget
        // was dropped and the threshold fell back to the 0.8×1 GiB default (OOM risk).
        log::debug!(
            "[hudi-rs-spill] SpillConfig resolved: max_in_memory_size={} bytes, \
             budget_key_present={} (key '{}')",
            spill_config.max_in_memory_size,
            reader_context
                .hoodie_reader_config
                .contains_key(CONFIG_MERGE_MAX_SIZE),
            CONFIG_MERGE_MAX_SIZE,
        );
        let records = SpillableRecordMap::with_config(spill_config);

        let mut base = FileGroupRecordBuffer::new(
            merge_mode,
            merger,
            update_processor,
            reader_schema,
            records,
        );
        base.delete_context = Some(delete_context);

        // Partial-update (FILL_UNAVAILABLE) sentinel, if the table's payload
        // configures one (Debezium toasted/unavailable values). Checked in the
        // reader config first, then the table config; empty → disabled.
        let unavailable_value = reader_context
            .hoodie_reader_config
            .get(PARTIAL_UPDATE_UNAVAILABLE_VALUE)
            .or_else(|| {
                reader_context
                    .table_config
                    .get(PARTIAL_UPDATE_UNAVAILABLE_VALUE)
            })
            .filter(|v| !v.is_empty())
            .cloned();

        // Partial-update IGNORE_DEFAULTS flag, if the table configures it (e.g.
        // PartialUpdateAvroPayload). Reader config first, then table config —
        // same precedence as the unavailable-value sentinel above.
        let ignore_defaults = reader_context
            .hoodie_reader_config
            .get(PARTIAL_UPDATE_MODE)
            .or_else(|| reader_context.table_config.get(PARTIAL_UPDATE_MODE))
            .is_some_and(|v| v.eq_ignore_ascii_case(PARTIAL_UPDATE_MODE_IGNORE_DEFAULTS));

        // Per-field IGNORE_DEFAULTS retention rules from the table's declared Avro
        // field defaults. Prefer the data (writer/table) schema JSON, which carries
        // the declared defaults; fall back to the required-schema JSON. Only parsed
        // when the mode is active; a parse failure / absent JSON yields an empty map
        // (every field then falls back to OnNull, the no-declared-default behavior).
        let default_retains = if ignore_defaults {
            reader_context
                .schema_handler
                .data_schema_json
                .as_deref()
                .or(reader_context
                    .schema_handler
                    .required_schema_json
                    .as_deref())
                .map(parse_default_retains)
                .unwrap_or_default()
        } else {
            HashMap::new()
        };

        Ok(Self {
            base,
            reader_context,
            record_context,
            unavailable_value,
            ignore_defaults,
            default_retains,
        })
    }

    /// Mirrors Java's `KeyBasedFileGroupRecordBuffer.hasNextBaseRecord(T baseRecord)`.
    ///
    /// Looks up the record key in the records map, removes it if found,
    /// then delegates to `FileGroupRecordBuffer.has_next_base_record(base, log)`.
    pub(super) fn has_next_base_record_at(
        &mut self,
        base_batch: &Arc<RecordBatch>,
        row_idx: usize,
        base_match: BaseMatch,
    ) -> Result<bool> {
        // Key extraction needs a single-row view; slicing here is cheap and the
        // slice is discarded after key extraction (the stored base record is a
        // zero-copy BatchRef into the shared batch, not this slice).
        let row = base_batch.slice(row_idx, 1);
        let keys = self.record_context.get_record_keys(&row)?;
        // The base record always carries its record key (needed for update
        // stats and any key-based fallback). The map LOOKUP key is the record
        // key (key-based) or the row's physical position (position-based).
        let record_key = keys[0].clone();
        let lookup_key = match base_match {
            BaseMatch::RecordKey => record_key.clone(),
            BaseMatch::Position => base_row_position_array(base_batch)?
                .value(row_idx)
                .to_string(),
        };
        let log_record = self.base.records.remove(&lookup_key)?;

        // Base-record ordering value (ENG-38318): EVENT_TIME final_merge compares
        // base vs log by ordering value, so the base record must carry it —
        // extracted from the same single-row slice used for the key, the same way
        // log records get theirs (record_context.get_ordering_values, used by
        // batch_to_buffered_records). COMMIT_TIME's merger ignores it, so this is
        // safe (and cheap: single-row) for both modes.
        let base_ordering = self
            .record_context
            .get_ordering_values(&row)?
            .and_then(|mut v| v.drain(..).next().flatten());

        // A2: base record is a zero-copy BatchRef into the shared base batch,
        // removing the per-row `base_row.clone()` allocation.
        let base_record =
            BufferedRecord::new_batch_ref(record_key, base_batch.clone(), row_idx, base_ordering);

        self.base
            .has_next_base_record(&base_record, log_record.as_ref())
    }

    /// Mirrors Java's `KeyBasedFileGroupRecordBuffer.doHasNext()`, parameterized
    /// by how base rows are matched to log records ([`BaseMatch`]).
    ///
    /// First iterates base file records, merging with log records.
    /// Then iterates remaining log-only records.
    pub(super) fn do_has_next_matched(&mut self, base_match: BaseMatch) -> Result<bool> {
        // Handle merging: iterate base file rows. `?` propagates a base source
        // error instead of ending iteration early (which would drop rows).
        while let Some((base_batch, row_idx)) = self.base.next_base_row()? {
            if self.has_next_base_record_at(&base_batch, row_idx, base_match)? {
                return Ok(true);
            }
        }

        // Handle records solely from log files
        self.base.has_next_log_record()
    }

    /// Vectorized base-batch pull + merge, parameterized by [`BaseMatch`]. The
    /// key-based trait entry point calls this with [`BaseMatch::RecordKey`]; the
    /// position buffer calls it with [`BaseMatch::Position`].
    pub(super) fn pull_and_merge_next_base_batch(
        &mut self,
        target_schema: &SchemaRef,
        base_match: BaseMatch,
    ) -> Result<Option<RecordBatch>> {
        loop {
            let source = match self.base.base_file_source.as_mut() {
                Some(s) => s,
                None => return Ok(None),
            };
            let base_batch = match source.next() {
                None => {
                    self.base.base_file_source = None;
                    return Ok(None);
                }
                Some(Ok(b)) => b,
                Some(Err(e)) => {
                    self.base.base_file_source = None;
                    log::error!(
                        "[KeyBasedBuffer] base file source error in \
                         pull_and_merge_next_base_batch: {e}"
                    );
                    return Err(crate::error::CoreError::ReadFileSliceError(format!(
                        "base file source error: {e}"
                    )));
                }
            };
            if base_batch.num_rows() == 0 {
                continue; // skip empty source batches
            }
            return self.merge_one_base_batch_kernel(&base_batch, target_schema, base_match);
        }
    }

    /// Vectorized merge of one base RecordBatch against the in-buffer log map.
    ///
    /// Walks the keys once, builds a `BooleanArray` keep-mask + a sparse
    /// `Vec<BufferedRecord>` of log-winning replacements. Returns the kept
    /// base rows (filtered in one Arrow kernel call) concatenated with the
    /// replacement rows (decoded via `records_to_batch`).
    ///
    /// Per-row allocation is paid only on the rows that actually have a log
    /// entry — the no-conflict path is one bit and no allocation. This is
    /// the hot path that replaces the legacy
    /// `next_base_row` + per-row `has_next_base_record` + 4096-batch
    /// `concat_batches` chain in `next_get_next_us`.
    ///
    /// Duplicate-key semantics match the legacy path: `HashMap::remove`
    /// consumes the log entry on the first base row whose key matches;
    /// subsequent duplicates within the same base batch find `None` and
    /// pass through. (Duplicate base keys within a single file group are
    /// already a writer invariant violation.)
    pub(super) fn merge_one_base_batch_kernel(
        &mut self,
        base: &RecordBatch,
        target_schema: &SchemaRef,
        base_match: BaseMatch,
    ) -> Result<Option<RecordBatch>> {
        let num_rows = base.num_rows();
        if num_rows == 0 {
            return Ok(None);
        }

        // Per-row lookup key source. Key-based: borrow keys as `&str` straight
        // from the contiguous key column (no per-row `String` alloc).
        // Position-based: read the row's physical position from the row-index
        // column and use its decimal string as the map key. The ordering column
        // is resolved once; ordering values are built lazily, only for the rare
        // conflict rows.
        let key_array = match base_match {
            BaseMatch::RecordKey => Some(self.record_context.record_key_array(base)?),
            BaseMatch::Position => None,
        };
        let position_array = match base_match {
            BaseMatch::Position => Some(base_row_position_array(base)?),
            BaseMatch::RecordKey => None,
        };
        let orderings = self.record_context.ordering_accessor(base)?;

        // Bit-packed keep-mask (1 bit/row) built directly, vs a `Vec<bool>`
        // (1 byte/row) that arrow would then re-pack into a bitmap.
        let mut keep = BooleanBufferBuilder::new(num_rows);
        let mut replacements: Vec<BufferedRecord> = Vec::new();
        // Tracks whether any base row is actually dropped; drives the all-keep
        // short-circuit below.
        let mut any_dropped = false;
        let merge_mode = self.base.record_merge_mode.as_str();
        // Partial-update sentinel (cloned once per batch; `None` on the common
        // path so the hot loop skips the toasted-value blend entirely).
        let unavailable_value = self.unavailable_value.clone();
        let ignore_defaults = self.ignore_defaults;
        let default_retains = self.default_retains.clone();
        // Record-key field name(s) — the columns the partial-update blends skip.
        let key_field_names = self.record_context.record_key_fields.clone();

        for idx in 0..num_rows {
            // Position-based lookup keys are owned decimal strings; key-based
            // keys are borrowed from the key column. `position_key_buf` holds the
            // owned string alive for the borrow within this iteration.
            let position_key_buf;
            let key: &str = match base_match {
                BaseMatch::RecordKey => key_array.as_ref().unwrap().value(idx),
                BaseMatch::Position => {
                    position_key_buf = position_array.as_ref().unwrap().value(idx).to_string();
                    position_key_buf.as_str()
                }
            };
            let log_rec_opt = self.base.records.remove(key)?;
            match log_rec_opt {
                None => keep.append(true),
                Some(log_rec) => {
                    // Build the base ordering value only now — on a conflict row.
                    let base_ordering = orderings.value_at(idx);
                    let winner = pick_winner(merge_mode, &log_rec, &base_ordering);
                    // Mirror the per-record counting the legacy row path performs
                    // in `RecordBuffer::has_next_base_record`: a key present in
                    // both base and log is an update unless the merged result is a
                    // delete (`Winner::LogDelete`). Base-only rows (the `None`
                    // arm) are intentionally not counted, matching gold.
                    self.base.update_processor.process_update(
                        key,
                        Some(&log_rec),
                        &log_rec,
                        matches!(winner, Winner::LogDelete),
                    )?;
                    match winner {
                        Winner::Base => {
                            // Base wins the ordering. For a partial-update table
                            // the winning base row must still be back-filled from
                            // the LOSING log record: Java's
                            // `EventTimePartialRecordMerger.finalMerge` fills the
                            // winner from the loser in BOTH directions (when the
                            // OLDER/base record wins, `partialMerge(base, log)`).
                            // A default/sentinel column on the base row therefore
                            // takes the log record's real value. Non-partial
                            // tables — and a losing delete tombstone, which has no
                            // columns to fill from — emit the base row unchanged
                            // (the common all-keep fast path).
                            let filled = match log_rec.get_record() {
                                Some(loser) if ignore_defaults => {
                                    let base_rec = BufferedRecord::new_data(
                                        key.to_string(),
                                        base.slice(idx, 1),
                                        base_ordering.clone(),
                                    );
                                    let (rec, changed) = reconcile_defaults_from_prior(
                                        base_rec,
                                        &loser,
                                        0,
                                        &default_retains,
                                        &key_field_names,
                                    )?;
                                    changed.then_some(rec)
                                }
                                Some(loser) => match unavailable_value.as_deref() {
                                    Some(sentinel) => {
                                        let base_rec = BufferedRecord::new_data(
                                            key.to_string(),
                                            base.slice(idx, 1),
                                            base_ordering.clone(),
                                        );
                                        let (rec, changed) = fill_unavailable_from_base(
                                            base_rec,
                                            &loser,
                                            0,
                                            sentinel,
                                            &key_field_names,
                                        )?;
                                        changed.then_some(rec)
                                    }
                                    None => None,
                                },
                                // Losing log record is a delete tombstone (no
                                // columns) → nothing to fill from.
                                None => None,
                            };
                            match filled {
                                Some(rec) => {
                                    keep.append(false);
                                    any_dropped = true;
                                    replacements.push(rec);
                                }
                                None => keep.append(true),
                            }
                        }
                        Winner::LogDelete => {
                            keep.append(false);
                            any_dropped = true;
                        }
                        Winner::LogData => {
                            keep.append(false);
                            any_dropped = true;
                            // Partial-update (IS_PARTIAL / KEEP_VALUES): a log record
                            // that carries only a subset of columns overlays its present
                            // columns onto this base row, keeping the base value for every
                            // column it omits. Detected by the record's narrow schema.
                            // Falls through to FILL_UNAVAILABLE (below) for full-schema
                            // records carrying the toasted/unavailable sentinel.
                            let log_batch = log_rec.get_record();
                            let rec = match log_batch {
                                Some(lb) if schema_is_partial(&lb.schema(), target_schema) => {
                                    let merged = overlay_partial_over_prior(
                                        &lb,
                                        &base.slice(idx, 1),
                                        target_schema,
                                    )?;
                                    BufferedRecord::new_data(
                                        log_rec.record_key,
                                        merged,
                                        log_rec.ordering_value,
                                    )
                                }
                                // Full-schema winning log record — apply the table's
                                // partial-update blend against the base row, if any. A
                                // table configures at most one mode, so these are mutually
                                // exclusive; IGNORE_DEFAULTS is checked first.
                                //
                                // IGNORE_DEFAULTS: a column whose value is the field default
                                // (null for the common nullable columns) keeps the prior base
                                // value (PartialUpdateAvroPayload).
                                _ if ignore_defaults => {
                                    reconcile_defaults_from_prior(
                                        log_rec,
                                        base,
                                        idx,
                                        &default_retains,
                                        &key_field_names,
                                    )?
                                    .0
                                }
                                // FILL_UNAVAILABLE: a column equal to the toasted/unavailable
                                // sentinel keeps the prior base value (Postgres-Debezium).
                                _ => match unavailable_value.as_deref() {
                                    Some(sentinel) => {
                                        fill_unavailable_from_base(
                                            log_rec,
                                            base,
                                            idx,
                                            sentinel,
                                            &key_field_names,
                                        )?
                                        .0
                                    }
                                    None => log_rec,
                                },
                            };
                            replacements.push(rec);
                        }
                    }
                }
            }
        }

        // All-keep fast path: no base row dropped and no log replacements means
        // the output equals the base batch. Return it directly (an `Arc` clone,
        // zero row copies) instead of running `filter_record_batch`, which deep-
        // copies every column even for an all-true mask. This is the common MOR
        // case where a base batch has no overlapping log keys. The map mutation
        // and update counting above have already happened, so this is sound.
        if !any_dropped && replacements.is_empty() {
            return Ok(Some(if base.schema() == *target_schema {
                base.clone()
            } else {
                reconcile_batch_to_schema(base, target_schema)?
            }));
        }

        // Apply the keep-mask in ONE Arrow kernel call (vs N per-row slices).
        let mask = BooleanArray::new(keep.finish(), None);
        let kept = arrow::compute::filter_record_batch(base, &mask).map_err(|e| {
            crate::error::CoreError::ReadFileSliceError(format!(
                "filter_record_batch failed in merge_one_base_batch_kernel: {e}"
            ))
        })?;

        // Reconcile kept rows to target_schema if the source's schema disagrees
        // on nested-type child field names (e.g. Avro "element" vs Parquet
        // "array"). Common case for projected reads: schema matches exactly,
        // this is a no-op.
        let kept = if kept.schema() == *target_schema {
            kept
        } else {
            reconcile_batch_to_schema(&kept, target_schema)?
        };

        if replacements.is_empty() {
            return Ok(Some(kept));
        }

        // Build the replacement batch from the sparse log-winning records.
        let repl_batch = records_to_batch(replacements, target_schema.clone())?;
        if repl_batch.num_rows() == 0 {
            return Ok(Some(kept));
        }

        // ONE concat of two batches — vs the legacy path's concat of 4096
        // single-row batches per chunk.
        let refs: Vec<&RecordBatch> = vec![&kept, &repl_batch];
        arrow::compute::concat_batches(target_schema, refs)
            .map(Some)
            .map_err(|e| {
                crate::error::CoreError::ReadFileSliceError(format!(
                    "concat_batches failed in merge_one_base_batch_kernel: {e}"
                ))
            })
    }
}

/// Outcome of a base-vs-log row conflict in [`KeyBasedFileGroupRecordBuffer::merge_one_base_batch_kernel`].
#[derive(Debug)]
enum Winner {
    /// Base row wins; emit base as-is (drop the log entry).
    Base,
    /// Log data wins; drop the base row, emit the log record.
    LogData,
    /// Log delete wins; drop both.
    LogDelete,
}

/// Decide the winner of a base-vs-log conflict without constructing a
/// `BufferedRecord` for the base row.
///
/// Mirrors the semantics of `BufferedRecordMerger::final_merge` for SAME-class
/// ordering values (the only case the writers produce — a file group carries a
/// single ordering type):
/// - `COMMIT_TIME_ORDERING` → log always wins (`CommitTimeRecordMerger`).
/// - Otherwise (e.g. `EVENT_TIME_ORDERING`) → higher ordering value wins,
///   ties go to the log side (`EventTimeRecordMerger`'s `new >= old`).
///   Missing ordering on either side → log wins (latest-write-wins fallback).
///
/// A CROSS-class comparison (e.g. `Long` base vs `Double` log, as can arise if a
/// file group's ordering-value type changes across commits) applies the SAME
/// `is_same_class` guard as `final_merge`/`should_keep_newer_record` and keeps the
/// log (newer) record, instead of comparing by `OrderingValue`'s arbitrary
/// cross-variant `Ord` rank. The vectorized kernel and the row path therefore
/// agree on cross-class handling.
///
/// Keeping this inline (rather than calling the merger) avoids the per-row
/// `BufferedRecord::new_data` + `RecordBatch::slice` cost on the dominant
/// no-conflict path. For the conflict path the cost is identical — the
/// `BufferedRecord` only materialises if there *is* a log entry to match.
fn pick_winner(
    merge_mode: &str,
    log: &BufferedRecord,
    base_ordering: &Option<OrderingValue>,
) -> Winner {
    // Deletes follow distinct semantics from data records. Mirrors Java
    // `BufferedRecordMergerFactory.deltaMergeDeleteRecord` /
    // `CommitTimeRecordMerger.deltaMerge(DeleteRecord, ...)`:
    // - COMMIT_TIME → the delete always applies.
    // - EVENT_TIME  → a delete carrying the DEFAULT (0 / natural-order) ordering
    //   value always applies (`isCommitTimeOrderingDelete`); a delete with a
    //   NON-default ordering is obsolete (base kept) only if the base carries a
    //   same-class ordering value that strictly exceeds the delete's.
    // The plain `log >= base` data-record test must NOT be used for deletes: a
    // relocate/CDC delete legitimately carries ordering 0, which would otherwise
    // lose to any positive base ordering and silently retain a deleted row.
    if log.is_delete() {
        let delete_obsolete = merge_mode != "COMMIT_TIME_ORDERING"
            && match (&log.ordering_value, base_ordering) {
                (Some(d), Some(b)) => !d.is_default() && d.is_same_class(b) && b > d,
                _ => false,
            };
        return if delete_obsolete {
            Winner::Base
        } else {
            Winner::LogDelete
        };
    }

    let log_wins = if merge_mode == "COMMIT_TIME_ORDERING" {
        true
    } else {
        match (&log.ordering_value, base_ordering) {
            // Same-class compare by value; cross-class (e.g. a Double base vs a
            // Long log after a precombine-field type change) keeps the newer
            // record rather than comparing by the arbitrary cross-variant `Ord`
            // rank -- mirrors `should_keep_newer_record` so the vectorized kernel
            // and the row path agree.
            (Some(l), Some(b)) => !l.is_same_class(b) || l >= b,
            _ => true,
        }
    };
    if log_wins {
        Winner::LogData
    } else {
        Winner::Base
    }
}

/// True if `arr`'s single (row-0) value is a non-null STRING/BYTES value equal
/// to the Debezium `sentinel`. Mirrors `PostgresDebeziumAvroPayload`'s
/// `containsStringToastedValues` / `containsBytesToastedValues` (STRING compares
/// the text; BYTES compares the UTF-8 bytes). Non-string/bytes types never match.
fn array_value_equals_sentinel(arr: &dyn Array, sentinel: &str) -> bool {
    if arr.is_null(0) {
        return false;
    }
    match arr.data_type() {
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .is_some_and(|a| a.value(0) == sentinel),
        DataType::LargeUtf8 => arr
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .is_some_and(|a| a.value(0) == sentinel),
        DataType::Binary => arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .is_some_and(|a| a.value(0) == sentinel.as_bytes()),
        DataType::LargeBinary => arr
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .is_some_and(|a| a.value(0) == sentinel.as_bytes()),
        _ => false,
    }
}

/// Whole-column variant of [`array_value_equals_sentinel`] (which inspects row 0
/// only): returns true if ANY row of `arr` equals the toasted `sentinel`. Same
/// STRING/BYTES type coverage; short-circuits on the first match.
fn column_contains_sentinel(arr: &dyn Array, sentinel: &str) -> bool {
    match arr.data_type() {
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .is_some_and(|a| a.iter().any(|v| v == Some(sentinel))),
        DataType::LargeUtf8 => arr
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .is_some_and(|a| a.iter().any(|v| v == Some(sentinel))),
        DataType::Binary => arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .is_some_and(|a| a.iter().any(|v| v == Some(sentinel.as_bytes()))),
        DataType::LargeBinary => arr
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .is_some_and(|a| a.iter().any(|v| v == Some(sentinel.as_bytes()))),
        _ => false,
    }
}

/// Fail-closed guard against harm H7 (CDC unavailable-placeholder leakage): a
/// `FILL_UNAVAILABLE` sentinel must never survive into a log-only-insert output
/// batch. Such a record had no base row and no prior log record to fill from
/// (the base-vs-log kernel and the log-vs-log blend in `process_next_data_record`
/// both fill only against a *present* prior), so the toasted placeholder would
/// otherwise be emitted as a real column value.
///
/// On the snapshot read path this is unreachable — a Debezium `op=u` (the only op
/// that carries the sentinel) always has a reachable prior in the same file slice
/// (base or an earlier log block). This guard turns a silent leak into a loud
/// error if a future reader (incremental / skip_merge over a partial log range)
/// makes the no-prior path reachable without adding explicit toasted-insert
/// handling. Callers invoke it only when a sentinel is configured, so
/// non-Debezium tables pay nothing.
fn guard_no_unavailable_sentinel(
    batch: &RecordBatch,
    sentinel: &str,
    key_field_names: &[String],
) -> Result<()> {
    for (i, field) in batch.schema().fields().iter().enumerate() {
        if is_key_or_meta_column(field.name(), key_field_names) {
            continue;
        }
        if column_contains_sentinel(batch.column(i).as_ref(), sentinel) {
            return Err(crate::error::CoreError::ReadFileSliceError(format!(
                "FILL_UNAVAILABLE sentinel leaked into a log-only insert (column '{}'): a toasted \
                 record reached output with no prior value to fill from (harm H7). Unreachable on \
                 the snapshot read path; if hit, a reader made the no-prior path reachable and needs \
                 explicit toasted-insert handling.",
                field.name()
            )));
        }
    }
    Ok(())
}

/// Partial-update (`FILL_UNAVAILABLE`) blend for the merge WINNER: each
/// STRING/BYTES column of `log_rec` whose value equals the toasted `sentinel` (a
/// column Debezium couldn't capture) is replaced with the loser row's value for
/// that same-named column. Mirrors
/// `PostgresDebeziumAvroPayload.mergeToastedValuesIfPresent`.
///
/// Direction-agnostic: `log_rec` is the WINNER (kept) record and `base` is the
/// LOSER row to fill from. The base-vs-log kernel uses it both ways — when the
/// log wins (`log_rec` = log, `base` = base row) and when the base wins
/// (`log_rec` = base row, `base` = losing log record). Java's
/// `EventTimePartialRecordMerger.finalMerge` fills the winner from the loser in
/// BOTH directions.
///
/// Returns `(record, changed)`; the fast path (no toasted column present) returns
/// `(log_rec, false)` untouched, so a partial-update table pays the rebuild cost
/// only on the rows that actually carry a sentinel.
fn fill_unavailable_from_base(
    log_rec: BufferedRecord,
    base: &RecordBatch,
    base_row_idx: usize,
    sentinel: &str,
    key_field_names: &[String],
) -> Result<(BufferedRecord, bool)> {
    let log_batch = match log_rec.get_record() {
        Some(b) => b,
        None => return Ok((log_rec, false)),
    };
    let base_row = base.slice(base_row_idx, 1);
    let base_schema = base_row.schema();
    let log_schema = log_batch.schema();

    let mut changed = false;
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(log_batch.num_columns());
    for (i, field) in log_schema.fields().iter().enumerate() {
        let log_col = log_batch.column(i);
        // Skip the record-key column(s) and `_hoodie_` meta columns: base and
        // log rows are matched BY key, so these are provably equal on both
        // sides (substitution was a no-op), but the explicit skip documents
        // the invariant and guards future call sites.
        if is_key_or_meta_column(field.name(), key_field_names) {
            cols.push(log_col.clone());
            continue;
        }
        if array_value_equals_sentinel(log_col.as_ref(), sentinel)
            && let Ok(bidx) = base_schema.index_of(field.name())
        {
            let base_col = base_row.column(bidx);
            // Only substitute when the base carries the same-typed column;
            // otherwise leave the (sentinel) log value rather than risk a
            // type-mismatched batch.
            if base_col.data_type() == field.data_type() {
                cols.push(base_col.clone());
                changed = true;
                continue;
            }
        }
        cols.push(log_col.clone());
    }

    if !changed {
        return Ok((log_rec, false));
    }
    let blended = RecordBatch::try_new(log_schema.clone(), cols).map_err(|e| {
        crate::error::CoreError::ReadFileSliceError(format!(
            "fill_unavailable_from_base failed to rebuild record: {e}"
        ))
    })?;
    Ok((
        BufferedRecord::new_data(log_rec.record_key, blended, log_rec.ordering_value),
        true,
    ))
}

/// Partial-update (`IGNORE_DEFAULTS`) blend for the merge WINNER: each column of
/// `log_rec` whose value equals the field's declared default is replaced with the
/// `prior` row's value for that same-named column. `default_retains` carries the
/// per-field rule (see [`DefaultRetain`]); a field absent from it falls back to
/// `OnNull`. Mirrors Java `PartialUpdateHandler.reconcileDefaultValues`.
///
/// Direction-agnostic: `log_rec` is the WINNER (kept) record and `prior` is the
/// LOSER row to fill from. Used by every merge path:
///  - log-vs-log: winner is the higher-ordering log record, `prior` the other one;
///  - base-vs-log, log wins: `log_rec` = log record, `prior` = base row;
///  - base-vs-log, base wins: `log_rec` = base row, `prior` = losing log record.
///
/// Java's `EventTimePartialRecordMerger.finalMerge`/`deltaMerge` fill the winner
/// from the loser in BOTH directions, so "prior" means the loser on either path,
/// not specifically the base file.
///
/// Returns `(record, changed)`; the fast path (no default-valued column present)
/// returns `(log_rec, false)` untouched, so a partial-update table pays the
/// rebuild cost only on rows that actually carry a default column.
fn reconcile_defaults_from_prior(
    log_rec: BufferedRecord,
    prior: &RecordBatch,
    prior_row_idx: usize,
    default_retains: &HashMap<String, DefaultRetain>,
    key_field_names: &[String],
) -> Result<(BufferedRecord, bool)> {
    let log_batch = match log_rec.get_record() {
        Some(b) => b,
        None => return Ok((log_rec, false)),
    };
    let prior_row = prior.slice(prior_row_idx, 1);
    let prior_schema = prior_row.schema();
    let log_schema = log_batch.schema();

    let mut changed = false;
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(log_batch.num_columns());
    for (i, field) in log_schema.fields().iter().enumerate() {
        let log_col = log_batch.column(i);
        // Skip the record-key column(s) and `_hoodie_` meta columns: prior and
        // incoming rows are matched BY key, so these are provably equal on
        // both sides (substitution was a no-op), but the explicit skip
        // documents the invariant and guards future call sites.
        if is_key_or_meta_column(field.name(), key_field_names) {
            cols.push(log_col.clone());
            continue;
        }
        // A field with no declared default retains on null (OnNull); one with a
        // declared default retains when the incoming value equals that default.
        let retain = default_retains
            .get(field.name())
            .unwrap_or(&DefaultRetain::OnNull);
        if value_is_default(log_col.as_ref(), retain)
            && let Ok(pidx) = prior_schema.index_of(field.name())
        {
            let prior_col = prior_row.column(pidx);
            // Only substitute when the prior carries the same-typed column;
            // otherwise leave the (default) log value rather than risk a
            // type-mismatched batch.
            if prior_col.data_type() == field.data_type() {
                cols.push(prior_col.clone());
                changed = true;
                continue;
            }
        }
        cols.push(log_col.clone());
    }

    if !changed {
        return Ok((log_rec, false));
    }
    let blended = RecordBatch::try_new(log_schema.clone(), cols).map_err(|e| {
        crate::error::CoreError::ReadFileSliceError(format!(
            "reconcile_defaults_from_prior failed to rebuild record: {e}"
        ))
    })?;
    Ok((
        BufferedRecord::new_data(log_rec.record_key, blended, log_rec.ordering_value),
        true,
    ))
}

/// True if the array's value at row 0 equals the field's declared default under
/// `retain` (so IGNORE_DEFAULTS should retain the prior value for this column).
fn value_is_default(arr: &dyn Array, retain: &DefaultRetain) -> bool {
    match retain {
        DefaultRetain::OnNull => arr.is_null(0),
        DefaultRetain::Never => false,
        DefaultRetain::OnBool(b) => {
            !arr.is_null(0)
                && arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .is_some_and(|a| a.value(0) == *b)
        }
        DefaultRetain::OnInt(d) => {
            if arr.is_null(0) {
                return false;
            }
            // Avro int → Arrow Int32, Avro long → Arrow Int64. (No Avro byte/short.)
            match arr.data_type() {
                DataType::Int32 => arr
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .is_some_and(|a| i64::from(a.value(0)) == *d),
                DataType::Int64 => arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .is_some_and(|a| a.value(0) == *d),
                _ => false,
            }
        }
    }
}

/// Build the per-field IGNORE_DEFAULTS retention rules from a record Avro schema
/// JSON. A field absent from the returned map (or a schema that fails to parse /
/// isn't a record) is treated as [`DefaultRetain::OnNull`] by the caller.
fn parse_default_retains(avro_json: &str) -> HashMap<String, DefaultRetain> {
    let mut out = HashMap::new();
    let schema = match apache_avro::Schema::parse_str(avro_json) {
        Ok(s) => s,
        Err(_) => return out,
    };
    if let apache_avro::Schema::Record(record) = schema {
        for field in &record.fields {
            out.insert(field.name.clone(), default_retain_for(field));
        }
    }
    out
}

/// Derive a field's [`DefaultRetain`] from its declared Avro default, faithful to
/// Java's `toJavaDefaultValue` + reference `==` (see [`DefaultRetain`] docs).
fn default_retain_for(field: &apache_avro::schema::RecordField) -> DefaultRetain {
    use serde_json::Value;
    match &field.default {
        None | Some(Value::Null) => DefaultRetain::OnNull,
        Some(Value::Bool(b)) => DefaultRetain::OnBool(*b),
        Some(Value::Number(n)) => {
            // Only integer-family defaults in the JVM cache range can match via
            // `==`; float/double are never cached, and Avro has no byte/short.
            match (field_is_integer(&field.schema), n.as_i64()) {
                (true, Some(v)) if (-128..=127).contains(&v) => DefaultRetain::OnInt(v),
                _ => DefaultRetain::Never,
            }
        }
        // string / bytes / array / object defaults: Java's `==` never matches.
        Some(_) => DefaultRetain::Never,
    }
}

/// True if the field's schema (or the non-null branch of a nullable union) is an
/// Avro integer family type (`int`/`long`) — the types the JVM caches boxes for.
fn field_is_integer(schema: &apache_avro::Schema) -> bool {
    use apache_avro::Schema as S;
    match schema {
        S::Int | S::Long => true,
        S::Union(union) => union
            .variants()
            .iter()
            .any(|v| matches!(v, S::Int | S::Long)),
        _ => false,
    }
}

/// True if `record_schema` is a partial-update (`IS_PARTIAL` / KEEP_VALUES) shape
/// relative to `target_schema`: it is missing at least one of the target's
/// fields. Such a record carries only the columns an upsert actually changed;
/// the absent columns must be filled from the prior (base/older) record at merge
/// time rather than treated as nulls. Mirrors Java
/// `SparkRecordMergingUtils.isPartial` (`schema != mergedSchema`), specialized to
/// "the record is missing at least one of the reader schema's fields" (it may
/// still carry extra meta columns the reader projection pruned).
///
/// A non-partial log block is resolved up to the reader schema at decode time, so
/// only `IS_PARTIAL` blocks (decoded writer-only) reach the merge with a narrow
/// schema — making this field-set check equivalent to the block's IS_PARTIAL flag.
///
/// Detected by field SET, not field count. An `IS_PARTIAL` block carries Hudi
/// meta columns (`_hoodie_record_key`, `_hoodie_commit_time`, …) alongside the
/// updated data columns, while the projected reader (target) schema often prunes
/// those meta columns. So a partial record can have AS MANY or MORE fields than
/// the target yet still be missing real data columns (e.g. the `id` key) — a
/// field-count short-circuit (`record.len() >= target.len() ⇒ not partial`) would
/// misclassify it as full, skip the overlay, and fail downstream in
/// `reconcile_batch_to_schema` with "column '<x>' missing". The sound check is
/// simply "any target field absent from the record"; for full records every
/// target field is present, so it returns false after a bounded name scan.
fn schema_is_partial(record_schema: &Schema, target_schema: &Schema) -> bool {
    target_schema
        .fields()
        .iter()
        .any(|f| record_schema.index_of(f.name()).is_err())
}

/// Overlay a partial-update record's present columns onto a prior row, producing
/// a full `target_schema` single-row batch: for each target field, take the
/// PARTIAL record's column when it carries that field, otherwise the PRIOR row's
/// column (the "keep previous value" semantics of KEEP_VALUES). A field absent
/// from BOTH (e.g. the prior was itself written with an older schema) becomes a
/// typed null. Mirrors Java `SparkRecordMergingUtils.mergePartialRecords`.
///
/// `partial` and `prior_row` are single-row batches (`prior_row` is the caller's
/// `prior.slice(idx, 1)`); the result is one row in `target_schema`.
fn overlay_partial_over_prior(
    partial: &RecordBatch,
    prior_row: &RecordBatch,
    target_schema: &SchemaRef,
) -> Result<RecordBatch> {
    // Reconcile via the kernel's name-metadata reconcile (`reconcile_batch_to_schema`)
    // rather than an exact `DataType` compare (#66 review). The old exact-compare
    // mishandled a reconcilable NESTED-type difference (Avro-vs-Parquet child field
    // names — same physical layout, different `DataType` tag) two ways: a *partial*
    // column that differed spuriously HARD-ERRORED, and a *prior/base* column that
    // differed was SILENTLY dropped to null. Name-reconcile keeps such columns; a
    // genuine physical mismatch (e.g. Int64 vs Int32) still fails loudly inside the
    // rebuild. (By the D5 design every batch reaching the merge is already required-
    // typed, so only nested child NAMES legitimately differ here — a scalar type
    // mismatch is an upstream bug and stays loud, same as the rest of the kernel.)
    let partial_schema = partial.schema();
    let prior_schema = prior_row.schema();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        // Present in the partial → overlay it. Exact type: take as-is. A nested
        // child-NAME-only difference (Avro "item"/"entries" vs Parquet
        // "element"/"key_value" — same physical layout) is name-reconciled. A
        // genuine physical mismatch is a LOUD error: a present value must never be
        // silently dropped OR unsafely reinterpreted (both `reconcile_batch_to_schema`
        // and `project_batch_to_schema` would reinterpret/narrow an Int64-vs-Int32
        // buffer — hence the explicit `is_name_reconcilable` gate).
        if let Ok(pidx) = partial_schema.index_of(field.name()) {
            let col = partial.column(pidx);
            if col.data_type() == field.data_type() {
                cols.push(col.clone());
            } else if is_name_reconcilable(col.data_type(), field.data_type()) {
                cols.push(reconcile_one_column(partial, pidx, field)?);
            } else {
                return Err(crate::error::CoreError::Unsupported(format!(
                    "partial-update column '{}' has type {:?} but the table schema expects {:?}",
                    field.name(),
                    col.data_type(),
                    field.data_type()
                )));
            }
            continue;
        }
        // Absent from the partial → keep the prior row's value. Same rules, except a
        // genuine prior-side mismatch falls back to a typed null (unchanged
        // prior-side behavior); a nested child-name-only difference is now
        // reconciled-and-KEPT (previously silently nulled).
        if let Ok(bidx) = prior_schema.index_of(field.name()) {
            let col = prior_row.column(bidx);
            if col.data_type() == field.data_type() {
                cols.push(col.clone());
                continue;
            } else if is_name_reconcilable(col.data_type(), field.data_type()) {
                cols.push(reconcile_one_column(prior_row, bidx, field)?);
                continue;
            }
        }
        // Absent from both, or a genuine prior-side type mismatch → typed null.
        cols.push(arrow_array::new_null_array(field.data_type(), 1));
    }
    RecordBatch::try_new(target_schema.clone(), cols).map_err(|e| {
        crate::error::CoreError::ReadFileSliceError(format!(
            "overlay_partial_over_prior failed to rebuild record: {e}"
        ))
    })
}

/// True iff `source` can be reconciled to `target` by name-metadata ALONE — i.e.
/// they are structurally identical apart from nested child-FIELD names (arrow-avro
/// "item"/"entries" vs Parquet "element"/"key_value") or field metadata, so the
/// underlying buffers are byte-compatible. A primitive or layout difference
/// (e.g. Int64 vs Int32, List vs LargeList) is NOT reconcilable — reconciling it
/// would reinterpret or drop bytes — so it returns false and the caller treats it
/// as a genuine mismatch. Conservative by construction: anything unrecognized is
/// false.
fn is_name_reconcilable(source: &arrow_schema::DataType, target: &arrow_schema::DataType) -> bool {
    use arrow_schema::DataType::{LargeList, List, Map, Struct};
    if source == target {
        return true;
    }
    match (source, target) {
        // List/LargeList/Map wrapper field NAME (+ metadata) may differ; recurse
        // into the element/entries type. Map also requires the sorted flag to match.
        (List(a), List(b)) | (LargeList(a), LargeList(b)) => {
            is_name_reconcilable(a.data_type(), b.data_type())
        }
        (Map(a, sa), Map(b, sb)) => sa == sb && is_name_reconcilable(a.data_type(), b.data_type()),
        // Struct field NAMES are user data (must match); recurse into each field's
        // type in order.
        (Struct(fa), Struct(fb)) => {
            fa.len() == fb.len()
                && fa.iter().zip(fb.iter()).all(|(x, y)| {
                    x.name() == y.name() && is_name_reconcilable(x.data_type(), y.data_type())
                })
        }
        _ => false,
    }
}

/// Name-reconcile a single source column (`src.column(src_idx)`) to
/// `target_field`'s type — nested child-field-name differences only (the same
/// [`reconcile_batch_to_schema`] name-metadata reconcile the drain uses). Callers
/// MUST gate with [`is_name_reconcilable`] first, since the underlying rebuild
/// reinterprets buffers and is only byte-safe when the physical layouts match.
fn reconcile_one_column(
    src: &RecordBatch,
    src_idx: usize,
    target_field: &arrow_schema::FieldRef,
) -> Result<ArrayRef> {
    let one = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![
            src.schema().field(src_idx).clone(),
        ])),
        vec![src.column(src_idx).clone()],
    )
    .map_err(|e| {
        crate::error::CoreError::ReadFileSliceError(format!("overlay reconcile setup: {e}"))
    })?;
    let target_one: SchemaRef = Arc::new(arrow_schema::Schema::new(vec![target_field.clone()]));
    let recon = reconcile_batch_to_schema(&one, &target_one)?;
    Ok(recon.column(0).clone())
}

/// Pad a partial-update record that has NO prior to merge against (a log-only
/// insert touching only a subset of columns) up to the full `target_schema`,
/// filling every absent field with a typed null. Equivalent to overlaying onto
/// an all-null prior row.
fn pad_partial_to_target(partial: &RecordBatch, target_schema: &SchemaRef) -> Result<RecordBatch> {
    let partial_schema = partial.schema();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        match partial_schema.index_of(field.name()) {
            // Present column → take it; a type mismatch is a loud error (a present
            // value must never be silently dropped to null).
            Ok(pidx) => {
                let col = partial.column(pidx);
                if col.data_type() != field.data_type() {
                    return Err(crate::error::CoreError::Unsupported(format!(
                        "partial-update column '{}' has type {:?} but the table schema expects {:?}",
                        field.name(),
                        col.data_type(),
                        field.data_type()
                    )));
                }
                cols.push(col.clone());
            }
            // Absent column → typed null (no prior value for a log-only insert).
            Err(_) => cols.push(arrow_array::new_null_array(field.data_type(), 1)),
        }
    }
    RecordBatch::try_new(target_schema.clone(), cols).map_err(|e| {
        crate::error::CoreError::ReadFileSliceError(format!(
            "pad_partial_to_target failed to rebuild record: {e}"
        ))
    })
}

/// Schema union of a partial `incoming` record over a `prior` record: the
/// prior's fields followed by any incoming field the prior lacks. Used for the
/// log-vs-log partial merge so the buffered result carries the columns from BOTH
/// log updates (and stays narrower than the reader schema only if their union
/// still omits some — letting base-vs-log fill the remainder).
fn union_schema(incoming: &Schema, prior: &Schema) -> SchemaRef {
    let mut fields: Vec<arrow_schema::FieldRef> = prior.fields().iter().cloned().collect();
    for f in incoming.fields() {
        if prior.index_of(f.name()).is_err() {
            fields.push(f.clone());
        }
    }
    Arc::new(Schema::new(fields))
}

impl HoodieFileGroupRecordBuffer for KeyBasedFileGroupRecordBuffer {
    fn get_buffer_type(&self) -> BufferType {
        BufferType::KeyBasedMerge
    }

    /// Mirrors Java's `KeyBasedFileGroupRecordBuffer.processDataBlock(HoodieDataBlock, Option<KeySpec>)`.
    ///
    /// Inflates the block on demand (matching Java where inflate/deserialize/deflate
    /// happens inside the block triggered by `getRecordsIterator`), then iterates
    /// each record, extracts the key, creates a BufferedRecord, and calls
    /// `process_next_data_record`.
    fn process_data_block(&mut self, block: &mut LogBlock) -> Result<()> {
        // Mirrors Java: getRecordsIterator → getEngineRecordIterator
        //   → readRecordsFromBlockPayload → inflate → deserializeRecords → deflate
        let decode_start = std::time::Instant::now();
        // Upstream fetches a lazy block's content here. This crate's log file
        // reader returns blocks with their content already read, so there is
        // nothing to fetch and the take below finds it present.
        self.base.stage_decode_ms += decode_start.elapsed().as_millis() as u64;

        if let LogBlockContent::Records(record_batches) = std::mem::take(&mut block.content) {
            let total_rows: usize = record_batches
                .data_batches
                .iter()
                .map(|b| b.num_rows())
                .sum();
            log::debug!(
                "[KeyBasedBuffer] processDataBlock: {} data batches, {} total rows",
                record_batches.data_batches.len(),
                total_rows,
            );
            for batch in record_batches.data_batches {
                // A2: intern the decoded block batch into a single `Arc` here
                // (the one mint point per block batch). Every BufferedRecord
                // produced from it shares this `Arc`, so the map pins one source
                // batch per block — and `Arc::as_ptr` is a valid compaction
                // grouping key. The map then pins only the block batches that
                // have a surviving key (dropped otherwise when this loop ends).
                let batch = Arc::new(batch);
                let records = self
                    .record_context
                    .batch_to_buffered_records(&batch, self.base.delete_context.as_ref())?;
                for (key, record) in records {
                    self.process_next_data_record(record, &key)?;
                }
            }
        }
        // No explicit deflate: the `std::mem::take(&mut block.content)` above
        // already replaced the content with `LogBlockContent::Empty` (its
        // Default), which is exactly what `deflate()` does — a trailing
        // `block.deflate()` here was a dead no-op (review C8).
        Ok(())
    }

    /// Mirrors Java's `KeyBasedFileGroupRecordBuffer.processNextDataRecord(BufferedRecord, Serializable)`.
    ///
    /// ```java
    /// BufferedRecord<T> existingRecord = records.get(recordKey);
    /// totalLogRecords++;
    /// bufferedRecordMerger.deltaMerge(record, existingRecord)
    ///     .ifPresent(merged -> records.put(recordKey, merged.toBinary(recordContext)));
    /// ```
    fn process_next_data_record(&mut self, record: BufferedRecord, key: &str) -> Result<()> {
        self.base.total_log_records += 1;

        // Two mutually-exclusive log-vs-log fix-ups can rewrite the incoming record
        // before the merge — a record is either a narrow KEEP_VALUES record or a
        // full-schema FILL_UNAVAILABLE one, so partial is checked first and toasted
        // is the `else`:
        //
        // Partial-update (IS_PARTIAL / KEEP_VALUES): the incoming record carries only
        // a subset of columns; overlay its present columns onto the prior buffered
        // record for this key, producing the UNION of their columns. The result
        // stays narrower than the reader schema until a base row or later update
        // supplies the rest, so base-vs-log can still fill the gaps. With no prior
        // data record it stays narrow and is padded at the base/drain step.
        let inc_is_partial = match (record.get_record(), self.base.reader_schema.as_ref()) {
            (Some(ib), Some(target)) => schema_is_partial(&ib.schema(), target),
            _ => false,
        };
        if inc_is_partial {
            let record = match self.base.records.get(key)? {
                Some(prior) => match (record.get_record(), prior.get_record()) {
                    (Some(ib), Some(prior_batch)) => {
                        let union = union_schema(&ib.schema(), &prior_batch.schema());
                        let merged = overlay_partial_over_prior(&ib, &prior_batch, &union)?;
                        BufferedRecord::new_data(record.record_key, merged, record.ordering_value)
                    }
                    // Prior is a delete tombstone (no columns) → keep incoming as-is.
                    _ => record,
                },
                None => record,
            };
            // Single-probe merge (perf): probe `key` once via `merge_in_place` and
            // overwrite in place, instead of get(probe+clone) → delta_merge → insert
            // (probe+clone+probe). The merger only reads `existing` by reference. A2
            // semantics preserved: the merged BatchRef payload is stored in-memory
            // (IPC serialization deferred to spill only).
            let merger = &self.base.buffered_record_merger;
            self.base
                .records
                .merge_in_place(key, |existing| merger.delta_merge(&record, existing))?;
        } else if self.ignore_defaults || self.unavailable_value.is_some() {
            // Full-schema partial-update (IGNORE_DEFAULTS or FILL_UNAVAILABLE),
            // log-vs-log blend. Mirrors Java `EventTimePartialRecordMerger.deltaMerge`,
            // which fills the merge WINNER from the loser in BOTH directions:
            //   - new wins      → fill new from the prior (existing), store new.
            //   - existing wins → fill existing from the new (incoming), store existing.
            //
            // Rationale: when ≥2 log updates to a key never touch the base, a
            // default (IGNORE_DEFAULTS) / sentinel (FILL_UNAVAILABLE) column must be
            // filled from the OTHER record, or it silently overwrites/retains the
            // wrong value (Class-C silent-wrong). Filling only new-from-prior (the
            // previous behavior) missed the case where the HIGHER-ordering record
            // arrives FIRST and wins: a later lower-ordering update carrying the real
            // value was discarded, leaving the winner's default/sentinel column
            // un-backfilled (P3/P4).
            //
            // The fill only rewrites default/sentinel columns and never touches
            // `ordering_value`, so it cannot change the winner; a same-ordering tie
            // keeps the incoming (new) as the winner, matching `should_keep_newer_record`.
            let stored = match self.base.records.get(key)? {
                None => record, // no prior: nothing to fill from; store as-is.
                Some(prior) => {
                    let new_wins = self.base.record_merge_mode == "COMMIT_TIME_ORDERING"
                        || should_keep_newer_record(&prior, &record);
                    if new_wins {
                        match prior.get_record() {
                            Some(prior_batch) => self.fill_partial(record, &prior_batch, 0)?.0,
                            // Prior is a delete tombstone → nothing to fill from.
                            None => record,
                        }
                    } else {
                        // Existing wins: back-fill existing from the incoming loser.
                        match record.get_record() {
                            Some(incoming_batch) => self.fill_partial(prior, &incoming_batch, 0)?.0,
                            // Incoming is a tombstone (no columns) → keep existing.
                            None => prior,
                        }
                    }
                }
            };
            self.base.records.insert(key.to_string(), stored)?;
        } else {
            // Plain (non-partial) log-vs-log merge. Single-probe merge (perf): probe
            // `key` once via `merge_in_place` and overwrite in place. The merger only
            // reads `existing` by reference. A2 semantics preserved: the merged
            // BatchRef payload is stored in-memory (IPC serialization deferred to
            // spill only).
            let merger = &self.base.buffered_record_merger;
            self.base
                .records
                .merge_in_place(key, |existing| merger.delta_merge(&record, existing))?;
        }

        // Stage stat (02/D3): track peak merge-map size.
        let len = self.base.records.len() as u64;
        if len > self.base.merge_map_peak_entries {
            self.base.merge_map_peak_entries = len;
        }

        Ok(())
    }

    /// Mirrors Java's `KeyBasedFileGroupRecordBuffer.processDeleteBlock(HoodieDeleteBlock)`.
    ///
    /// Inflates the block on demand, then iterates delete records and calls
    /// `process_next_deleted_record` for each.
    fn process_delete_block(&mut self, block: &mut LogBlock) -> Result<()> {
        let decode_start = std::time::Instant::now();
        // Upstream fetches a lazy block's content here. This crate's log file
        // reader returns blocks with their content already read, so there is
        // nothing to fetch and the take below finds it present.
        self.base.stage_decode_ms += decode_start.elapsed().as_millis() as u64;

        if let LogBlockContent::Records(record_batches) = std::mem::take(&mut block.content) {
            let total_deletes: usize = record_batches
                .delete_batches
                .iter()
                .map(|(b, _)| b.num_rows())
                .sum();
            log::debug!(
                "[KeyBasedBuffer] processDeleteBlock: {} delete batches, {} total deletes",
                record_batches.delete_batches.len(),
                total_deletes,
            );
            for (batch, _inst) in record_batches.delete_batches {
                // Extract key AND ordering value per delete record. EVENT_TIME_ORDERING
                // delete merge compares the delete's ordering value against the existing
                // record's: a delete with a lower ordering value must NOT remove the row
                // (ENG-38318). The previous key-only extraction left ordering_value=None,
                // which made delta_merge_delete fall through to "delete always wins" and
                // silently dropped records whose ordering value out-ranked a late delete.
                // partition_path stays empty (cosmetic for single-partition reads).
                let delete_entries = self
                    .record_context
                    .delete_batch_to_keys_with_ordering(&batch)?;
                for (key, ordering_value) in delete_entries {
                    let delete_record = DeleteRecord {
                        record_key: key.clone(),
                        partition_path: String::new(),
                        ordering_value,
                    };
                    self.process_next_deleted_record(delete_record, &key)?;
                }
            }
        }
        // No explicit deflate — see process_data_block (review C8): the
        // `std::mem::take` above already left the content `Empty`.
        Ok(())
    }

    /// Mirrors Java's `KeyBasedFileGroupRecordBuffer.processNextDeletedRecord(DeleteRecord, Serializable)`.
    ///
    /// ```java
    /// BufferedRecord<T> existingRecord = records.get(recordIdentifier);
    /// totalLogRecords++;
    /// bufferedRecordMerger.deltaMerge(deleteRecord, existingRecord)
    ///     .ifPresent(deleteRec -> records.put(recordIdentifier,
    ///         BufferedRecords.fromDeleteRecord(deleteRec, recordContext)));
    /// ```
    fn process_next_deleted_record(
        &mut self,
        delete_record: DeleteRecord,
        key: &str,
    ) -> Result<()> {
        self.base.total_log_records += 1;

        // Single-probe merge (perf), mirroring process_next_data_record: probe once,
        // overwrite in place. On "delete wins" the slot becomes the surviving delete
        // tombstone; on "existing survives" the merger returns None → slot untouched.
        let merger = &self.base.buffered_record_merger;
        self.base.records.merge_in_place(key, |existing| {
            Ok(merger
                .delta_merge_delete(&delete_record, existing)?
                .map(|sd| BufferedRecords::from_delete_record(&sd)))
        })?;
        Ok(())
    }

    fn contains_log_record(&self, record_key: &str) -> bool {
        // A spill-backend (RocksDB) error is treated as "not present" for this
        // boolean predicate; the authoritative get/remove on the merge path
        // surfaces such errors as `Result`.
        self.base.records.contains_key(record_key).unwrap_or(false)
    }

    fn size(&self) -> usize {
        self.base.records.len()
    }

    fn get_total_log_records(&self) -> u64 {
        self.base.total_log_records
    }

    fn stage_decode_ms(&self) -> u64 {
        self.base.stage_decode_ms
    }

    fn merge_map_peak_entries(&self) -> u64 {
        self.base.merge_map_peak_entries
    }

    fn merge_map_spilled(&self) -> bool {
        self.base.records.spill_fired()
    }

    fn merge_map_peak_in_memory_bytes(&self) -> u64 {
        self.base.records.peak_in_memory_size()
    }

    fn current_in_memory_bytes(&self) -> u64 {
        self.base.records.current_in_memory_bytes()
    }

    fn update_stats_snapshot(&self) -> UpdateStats {
        // Mirrors `merge_and_collect_with_stats`'s drain, but without consuming
        // the buffer — the streaming iterator owns the buffer and reads the
        // counts back after exhausting it (ENG-42991).
        self.base.update_processor.read_stats_counts()
    }

    fn get_log_records(&self) -> &MergeMap {
        // Returns the IN-MEMORY tier only (the schema-fallback / log-only callers
        // just need *some* live record's schema, which the in-memory tier always
        // has unless every entry spilled — a degenerate case handled by the base
        // file or required schema). Spilled entries are not materialized here to
        // avoid an unbounded reload.
        self.base.records.in_memory()
    }

    fn set_reader_schema(&mut self, schema: SchemaRef) {
        self.base.reader_schema = Some(schema.clone());
        // Phase 2: Enrich DeleteContext with the reader schema.
        // Mirrors Java's `deleteContext.withReaderSchema(this.readerSchema)`
        // in FileGroupRecordBuffer constructor.
        if let Some(ctx) = self.base.delete_context.take() {
            self.base.delete_context = Some(ctx.with_reader_schema(schema));
        }
    }

    fn set_base_file_source(&mut self, source: Box<dyn arrow_array::RecordBatchReader + Send>) {
        // A3 (ENG-42992): the lazy streaming source replaces the eager
        // `Vec<RecordBatch>`. `next_base_row` pulls one row-group at a time and
        // interns it into an `Arc` (A2) so base records remain zero-copy
        // `BatchRef`s — one `Arc` per streamed batch keeps `Arc::as_ptr`
        // identity stable for compaction grouping.
        self.base.base_file_source = Some(source);
        self.base.current_base_batch = None;
        self.base.base_row_idx = 0;
    }
    // `set_base_file_iterator(Vec<RecordBatch>)` is now the default impl
    // on the trait — it just wraps the Vec in a `RecordBatchIterator` and
    // calls `set_base_file_source`. All existing test sites that use
    // `buffer.set_base_file_iterator(vec![...])` continue to compile.

    /// Compact the merge map's sparsely-pinned source batches (A2 safety valve).
    ///
    /// See [`HoodieFileGroupRecordBuffer::compact_pinned_batches`]. Operates on
    /// `self.base.records`, grouping `BatchRef` entries by `Arc::as_ptr`
    /// (invariant, A2 risk #2: each decoded block batch is interned to exactly
    /// one `Arc` in `process_data_block`, so the pointer uniquely identifies a
    /// source batch). `Owned` and `Delete` payloads are left untouched (they pin
    /// nothing shared).
    fn compact_pinned_batches(&mut self) -> Result<()> {
        // A6e: delegate to the spillable map's accounting-aware compaction so the
        // pinned-bytes trackers (and the peak stat) stay in sync. Compaction only
        // ever touches the in-memory tier (spilled entries are `Owned` and pin
        // nothing shared). Under A6e the over-budget eviction already compacts /
        // spills sparse / dense batches DURING the scan; this end-of-scan pass is
        // the residual safety valve for sparse batches that never tripped the
        // budget.
        self.base
            .records
            .compact_sparse_batches(COMPACTION_LIVE_RATIO)
    }

    /// Mirrors Java's `hasNext()` template method:
    /// `nextRecord != null || doHasNext()`
    fn has_next(&mut self) -> Result<bool> {
        if self.base.next_record.is_some() {
            return Ok(true);
        }
        self.do_has_next_matched(BaseMatch::RecordKey)
    }

    /// Mirrors Java's `next()`:
    /// Take and return `nextRecord`, set to None.
    fn next(&mut self) -> Option<BufferedRecord> {
        self.base.next_record.take()
    }

    /// Consume the buffer and produce merged output as a RecordBatch.
    ///
    /// Drives the `has_next()`/`next()` iterator to completion and
    /// collects all records into a single batch.
    fn merge_and_collect_with_stats(mut self: Box<Self>) -> Result<(RecordBatch, UpdateStats)> {
        // A3 (ENG-42992): base_rows is no longer summed up front — the base
        // file is a lazy stream now, so counting it would force a full decode.
        let log_records = self.base.records.len();
        log::debug!(
            "[KeyBasedBuffer] merge_and_collect: log_records_in_map={log_records} \
             total_log_records_processed={}",
            self.base.total_log_records,
        );

        // Use reader_schema (= required_schema) as the output schema when set,
        // since it represents the merge-compatible schema. The base file may be
        // projected to fewer columns, while log records have the full writer schema.
        // reader_schema is the common ground that both are reconciled to.
        let schema = if let Some(schema) = &self.base.reader_schema {
            schema.clone()
        } else if let Some(source) = &self.base.base_file_source {
            // ENG-42992 — the base file source exposes its schema via
            // `RecordBatchReader::schema()` without forcing a parquet
            // row-group decode. This replaces the old `base_file_batches[0]
            // .schema()` lookup that required the batches to be eagerly
            // loaded.
            source.schema()
        } else {
            // Fallback: find any non-delete record in the buffer's in-memory
            // tier. HashMap iteration order is non-deterministic, so we must
            // search all in-memory records — the first entry could be a delete.
            let any_data_record = self
                .base
                .records
                .in_memory()
                .values()
                .find_map(|r| r.get_record());
            match any_data_record.as_ref() {
                Some(batch) => batch.schema(),
                None => {
                    return Err(crate::error::CoreError::ReadFileSliceError(
                        "No schema available for merge output".to_string(),
                    ));
                }
            }
        };

        let mut output_records: Vec<BufferedRecord> = Vec::new();
        while self.has_next()? {
            if let Some(record) = self.next() {
                // Deletes are dropped by the update processor (emit_delete is
                // gated off), so any record reaching here is a data record.
                output_records.push(record);
            }
        }

        // Drain the insert / update / delete counts the update processor
        // accumulated during the merge iteration. Mirrors gold, where
        // StandardUpdateProcessor increments HoodieReadStats as a side effect.
        let stats = self.base.update_processor.read_stats_counts();

        log::debug!(
            "[KeyBasedBuffer] merge_and_collect output: {} data records, \
             inserts={} updates={} deletes={}",
            output_records.len(),
            stats.num_inserts,
            stats.num_updates,
            stats.num_deletes,
        );

        let batch = records_to_batch(output_records, schema)?;
        Ok((batch, stats))
    }

    /// Vectorized streaming entry point. Pulls the next non-empty base batch
    /// from `self.base.base_file_source`, runs the vectorized merge kernel,
    /// returns the merged batch. `None` when the base source is exhausted —
    /// caller then calls [`Self::drain_log_only_inserts`] for the final
    /// flush.
    fn next_merged_base_batch(&mut self, target_schema: &SchemaRef) -> Result<Option<RecordBatch>> {
        self.pull_and_merge_next_base_batch(target_schema, BaseMatch::RecordKey)
    }

    /// Drain any log records that were never matched by a base row, as one
    /// final batch of inserts. Deletes are filtered out (they contribute
    /// no output rows). Idempotent: subsequent calls return `Ok(None)`.
    fn drain_log_only_inserts(&mut self, target_schema: &SchemaRef) -> Result<Option<RecordBatch>> {
        // Lazily start the drain on the first call: move the (possibly-spilled) map
        // into a streaming `SpillDrainIter`. Chunking below bounds drain-time memory
        // to ~one output batch rather than re-materializing the whole spilled map at
        // once (the drain-time OOM the spill exists to prevent -- ENG-42993). The
        // merge iterator stays in its `DrainingLogInserts` state and calls this
        // repeatedly until it returns `Ok(None)`, so successive chunks stream out.
        if self.base.log_drain_iter.is_none() {
            if self.base.records.is_empty() {
                return Ok(None);
            }
            // `take` leaves `records` empty for the rest of the drain. This assumes no
            // ingestion (`process_data_block`) is interleaved once the drain has begun:
            // the merge iterator's `DrainingLogInserts` state is terminal (it only ever
            // calls this until `Ok(None)`), so nothing repopulates `records` mid-drain.
            self.base.log_drain_iter = Some(std::mem::take(&mut self.base.records).drain_iter());
        }

        // Pull up to `DEFAULT_BATCH_SIZE` non-delete records for this chunk. Mirror the
        // per-record counting the legacy row path performs in
        // `RecordBuffer::has_next_log_record`: each remaining (log-only) record is an
        // insert unless it is a delete; deletes are still processed but contribute no
        // output row (so we keep pulling through them without counting toward the chunk).
        // `Vec::new()` (not `with_capacity`): the chunk is capped at `DEFAULT_BATCH_SIZE`
        // but small file groups drain far fewer rows, so grow on demand rather than
        // eagerly reserving 4096 slots on every call.
        let mut records: Vec<BufferedRecord> = Vec::new();
        while records.len() < DEFAULT_BATCH_SIZE {
            let next = self
                .base
                .log_drain_iter
                .as_mut()
                .expect("log_drain_iter initialized above")
                .next();
            match next {
                Some(Ok(r)) => {
                    let is_delete = r.is_delete();
                    self.base.update_processor.process_update(
                        &r.record_key,
                        None,
                        &r,
                        is_delete,
                    )?;
                    if !is_delete {
                        // A log-only partial-update record (IS_PARTIAL insert with no base
                        // row and no full prior to overlay) is still narrow; pad its absent
                        // columns to typed nulls so it reconciles to the output schema (the
                        // omitted columns have no prior value, so null is correct).
                        let r = match r.get_record() {
                            Some(b) if schema_is_partial(&b.schema(), target_schema) => {
                                let padded = pad_partial_to_target(&b, target_schema)?;
                                BufferedRecord::new_data(r.record_key, padded, r.ordering_value)
                            }
                            _ => r,
                        };
                        records.push(r);
                    }
                }
                Some(Err(e)) => {
                    // A drain error is terminal. Release the iterator so the buffer is
                    // left in a clean state — a retry would otherwise resume a
                    // partially-consumed iterator (skipping the errored record) rather
                    // than surfacing a stable failure (#76 review).
                    self.base.log_drain_iter = None;
                    return Err(e);
                }
                None => {
                    // Drain exhausted — release the drain iterator NOW so the
                    // RocksDB handle + spill temp dir are freed immediately
                    // (RAII drop), instead of staying pinned as an exhausted
                    // `Some(..)` until the whole buffer is dropped (#76 review).
                    // A subsequent call re-enters with `records` already empty
                    // and returns `Ok(None)` via the guard above.
                    self.base.log_drain_iter = None;
                    break;
                }
            }
        }
        if records.is_empty() {
            // Drain exhausted (fully consumed, or only deletes remained).
            return Ok(None);
        }
        let batch = records_to_batch(records, target_schema.clone())?;
        // Harm-H7 guard: a FILL_UNAVAILABLE sentinel must never survive into a
        // log-only insert (a record with no base row and no prior to fill from).
        // Debezium-only — gated on a configured sentinel, so non-Debezium tables
        // pay nothing; one scan of this (log-only) batch when it is set.
        if let Some(sentinel) = self.unavailable_value.as_deref() {
            guard_no_unavailable_sentinel(
                &batch,
                sentinel,
                &self.record_context.record_key_fields,
            )?;
        }
        if batch.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }
}

/// Re-batch survivors of any source batch whose live-row ratio is below
/// `live_ratio_threshold`, releasing the dead-row memory the sparse survivors
/// pinned (A2 safety valve).
///
/// For each distinct shared source batch in `records`, count how many entries
/// reference it. If `live_rows / source.num_rows() < live_ratio_threshold`,
/// build one compact owned batch from exactly the live rows (via the Arrow
/// `interleave` kernel) and repoint those entries to it as
/// [`RecordPayload::Owned`]. Once the original `Arc` has no remaining `BatchRef`,
/// it is dropped and its dead rows freed.
///
/// Invariants honored (A2 risks):
/// - **Arc identity (#2):** grouping by `Arc::as_ptr` is sound because each
///   decoded block batch is interned to one `Arc` (`process_data_block`).
/// - **Schema consistency (#3):** survivors of a single source batch all share
///   that batch's schema, so the single-input `interleave` is uniform.
/// - **Slice pins parent (#4):** we materialize a fresh `Owned` batch and repoint
///   rather than slicing, so the parent is actually released.
///
/// Test-only: production code now drives compaction through the spillable map's
/// accounting-aware [`SpillableRecordMap::compact_sparse_batches`] (A6e). This
/// standalone primitive is retained to unit-test the interleave/repoint logic on
/// a raw map without the surrounding tier accounting.
#[cfg(test)]
fn compact_pinned_batches(records: &mut MergeMap, live_ratio_threshold: f64) -> Result<()> {
    // Pass 1: group the keys of each shared source batch (+ keep one Arc handle
    // and the batch's row count for the live-ratio test).
    struct Group {
        batch: Arc<RecordBatch>,
        num_rows: usize,
        keys: Vec<String>,
    }
    // Grouping key is the source batch pointer (`Arc::as_ptr`); see invariant #2.
    let mut groups: HashMap<*const RecordBatch, Group> = HashMap::new();
    for (key, record) in records.iter() {
        if let RecordPayload::BatchRef { batch, .. } = &record.payload {
            groups
                .entry(Arc::as_ptr(batch))
                .or_insert_with(|| Group {
                    batch: batch.clone(),
                    num_rows: batch.num_rows(),
                    keys: Vec::new(),
                })
                .keys
                .push(key.clone());
        }
    }

    // Pass 2: for each under-threshold group, build a compact owned batch from
    // the live rows and repoint the entries.
    for group in groups.into_values() {
        let live = group.keys.len();
        if group.num_rows == 0 {
            continue;
        }
        let ratio = live as f64 / group.num_rows as f64;
        if ratio >= live_ratio_threshold {
            continue;
        }

        // Row index of each surviving key within the source batch.
        let rows: Vec<usize> = group
            .keys
            .iter()
            .map(|k| match &records[k].payload {
                RecordPayload::BatchRef { row_idx, .. } => *row_idx,
                // Unreachable: `group.keys` were collected from BatchRef entries
                // and `records` is not mutated until below.
                _ => unreachable!("compaction group key must reference a BatchRef payload"),
            })
            .collect();

        let indices: Vec<(usize, usize)> = rows.iter().map(|&r| (0usize, r)).collect();
        let compact = arrow::compute::interleave_record_batch(&[group.batch.as_ref()], &indices)
            .map_err(|e| {
                CoreError::ReadFileSliceError(format!(
                    "compaction: failed to interleave {live} survivors of a pinned source batch: {e}"
                ))
            })?;
        // Intern the compact batch once and share it across all survivors, so the
        // repointed entries pin exactly this one batch (and drop the original).
        let compact = Arc::new(compact);

        // Repoint each surviving entry to its position in the compact batch.
        // `group.keys[i]` was the i-th row fed to interleave, so it now lives at
        // compact row `i`.
        for (compact_row, key) in group.keys.iter().enumerate() {
            if let Some(record) = records.get_mut(key) {
                record.payload = RecordPayload::BatchRef {
                    batch: Arc::clone(&compact),
                    row_idx: compact_row,
                };
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::log_file::log_block::{BlockMetadataKey, BlockType, LogBlockContent};
    use crate::file_group::log_file::log_format::LogFormatVersion;
    use crate::file_group::reader_v2::buffered_record::OrderingValue;
    use crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler;
    use crate::file_group::record_batches::RecordBatches;
    use arrow_array::{Int32Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    /// `array_value_equals_sentinel` underpins toasted-value detection. It must
    /// match the sentinel for STRING (Utf8/LargeUtf8) and BYTES
    /// (Binary/LargeBinary) columns, reject null and non-matching values, and
    /// never match a non-string/bytes column type.
    #[test]
    fn test_array_value_equals_sentinel_across_types() {
        use arrow_array::{BinaryArray, Float64Array, LargeBinaryArray, LargeStringArray};
        const S: &str = "__debezium_unavailable_value";

        // STRING variants match on text equality.
        assert!(array_value_equals_sentinel(
            &StringArray::from(vec![Some(S)]),
            S
        ));
        assert!(array_value_equals_sentinel(
            &LargeStringArray::from(vec![Some(S)]),
            S
        ));
        assert!(!array_value_equals_sentinel(
            &StringArray::from(vec![Some("other")]),
            S
        ));

        // BYTES variants match on the sentinel's UTF-8 bytes.
        assert!(array_value_equals_sentinel(
            &BinaryArray::from(vec![Some(S.as_bytes())]),
            S
        ));
        assert!(array_value_equals_sentinel(
            &LargeBinaryArray::from(vec![Some(S.as_bytes())]),
            S
        ));
        assert!(!array_value_equals_sentinel(
            &BinaryArray::from(vec![Some(b"other".as_ref())]),
            S
        ));

        // Null row-0 never matches.
        assert!(!array_value_equals_sentinel(
            &StringArray::from(vec![None::<&str>]),
            S
        ));

        // Non-string/bytes column types never match.
        assert!(!array_value_equals_sentinel(
            &Float64Array::from(vec![Some(1.0)]),
            S
        ));
        assert!(!array_value_equals_sentinel(
            &Int64Array::from(vec![Some(0)]),
            S
        ));
    }

    const TOASTED_SENTINEL: &str = "__debezium_unavailable_value";

    /// Schema with a toastable STRING `payload` column (the standard test schema
    /// has only numeric value columns, which can't carry a string sentinel).
    fn toasted_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, false),
        ]))
    }

    fn toasted_row(key: &str, payload: &str, ts: i64) -> BufferedRecord {
        let batch = RecordBatch::try_new(
            toasted_schema(),
            vec![
                Arc::new(StringArray::from(vec![key])) as _,
                Arc::new(StringArray::from(vec![payload])) as _,
                Arc::new(Int64Array::from(vec![ts])) as _,
            ],
        )
        .unwrap();
        BufferedRecord::new_data(key.to_string(), batch, Some(OrderingValue::Long(ts)))
    }

    /// Build an EVENT_TIME buffer over [`toasted_schema`] with the Debezium
    /// FILL_UNAVAILABLE sentinel configured.
    fn build_toasted_buffer() -> KeyBasedFileGroupRecordBuffer {
        let merge_mode = "EVENT_TIME_ORDERING";
        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "ts".to_string(),
        );
        ctx.hoodie_reader_config.insert(
            PARTIAL_UPDATE_UNAVAILABLE_VALUE.to_string(),
            TOASTED_SENTINEL.to_string(),
        );
        ctx.rebuild_record_context(String::new());
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(toasted_schema())
            .with_data_schema(toasted_schema());
        let key_field = ctx.record_key_field().to_string();
        let ordering = ctx.record_context.ordering_field_names.clone();
        handler
            .prepare_required_schema(
                true,
                &[key_field],
                &ordering,
                &ctx.table_config,
                false,
                merge_mode,
            )
            .unwrap();
        ctx.schema_handler = handler;
        KeyBasedFileGroupRecordBuffer::new(Arc::new(ctx), merge_mode.to_string(), false).unwrap()
    }

    /// Two log updates to the same key, NO base file: the second update toasts
    /// `payload` (sentinel). The merged output must carry the prior log record's
    /// payload, not the sentinel — the log-vs-log analog of the base-vs-log
    /// toasted blend. Before the fix the blend ran only against the base file, so
    /// this purely-in-log case leaked the sentinel (Class-C silent-wrong).
    #[test]
    fn test_log_vs_log_toasted_value_filled_from_prior_buffered_record() {
        let mut buffer = build_toasted_buffer();
        buffer
            .process_next_data_record(toasted_row("1", "full-value", 1), "1")
            .unwrap();
        buffer
            .process_next_data_record(toasted_row("1", TOASTED_SENTINEL, 2), "1")
            .unwrap();
        buffer.set_base_file_iterator(vec![]);

        let out = Box::new(buffer).merge_and_collect().unwrap();
        assert_eq!(out.num_rows(), 1, "one key survives");
        let keys = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let payloads = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tss = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(keys.value(0), "1");
        assert_eq!(
            payloads.value(0),
            "full-value",
            "toasted payload must be filled from the prior log record, not left as the sentinel"
        );
        assert_eq!(
            tss.value(0),
            2,
            "the newer ordering (ts=2) record wins the merge"
        );
    }

    /// Sanity counterpart: when the newer update carries a REAL payload (no
    /// sentinel), it overwrites the prior value as usual — the blend must not
    /// interfere with ordinary log-vs-log updates.
    #[test]
    fn test_log_vs_log_non_toasted_update_overwrites_normally() {
        let mut buffer = build_toasted_buffer();
        buffer
            .process_next_data_record(toasted_row("1", "v1", 1), "1")
            .unwrap();
        buffer
            .process_next_data_record(toasted_row("1", "v2", 2), "1")
            .unwrap();
        buffer.set_base_file_iterator(vec![]);

        let out = Box::new(buffer).merge_and_collect().unwrap();
        let payloads = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(payloads.value(0), "v2", "newer real value wins unchanged");
    }

    /// A `payload` value; `None` produces a null (the IGNORE_DEFAULTS trigger).
    fn ignore_defaults_row(key: &str, payload: Option<&str>, ts: i64) -> BufferedRecord {
        let batch = RecordBatch::try_new(
            toasted_schema(),
            vec![
                Arc::new(StringArray::from(vec![key])) as _,
                Arc::new(StringArray::from(vec![payload])) as _,
                Arc::new(Int64Array::from(vec![ts])) as _,
            ],
        )
        .unwrap();
        BufferedRecord::new_data(key.to_string(), batch, Some(OrderingValue::Long(ts)))
    }

    /// Build an EVENT_TIME buffer over [`toasted_schema`] with
    /// `partial.update.mode = IGNORE_DEFAULTS` configured (the PartialUpdateAvroPayload
    /// path). Mirrors [`build_toasted_buffer`] but selects the default-value blend.
    fn build_ignore_defaults_buffer() -> KeyBasedFileGroupRecordBuffer {
        let merge_mode = "EVENT_TIME_ORDERING";
        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "ts".to_string(),
        );
        ctx.table_config.insert(
            PARTIAL_UPDATE_MODE.to_string(),
            PARTIAL_UPDATE_MODE_IGNORE_DEFAULTS.to_string(),
        );
        ctx.rebuild_record_context(String::new());
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(toasted_schema())
            .with_data_schema(toasted_schema());
        let key_field = ctx.record_key_field().to_string();
        let ordering = ctx.record_context.ordering_field_names.clone();
        handler
            .prepare_required_schema(
                true,
                &[key_field],
                &ordering,
                &ctx.table_config,
                false,
                merge_mode,
            )
            .unwrap();
        ctx.schema_handler = handler;
        KeyBasedFileGroupRecordBuffer::new(Arc::new(ctx), merge_mode.to_string(), false).unwrap()
    }

    /// Buffer over `(_hoodie_record_key, val, _event_lsn)` with
    /// `partial.update.mode = IGNORE_DEFAULTS` — the PartialUpdateAvroPayload
    /// shape — under the given `merge_mode`. Mirrors
    /// [`build_debezium_event_time_buffer`] but selects the default-value blend
    /// instead of the toasted-sentinel blend.
    fn build_ignore_defaults_buffer_with_mode(merge_mode: &str) -> KeyBasedFileGroupRecordBuffer {
        let schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("val", DataType::Utf8, true),
            Field::new("_event_lsn", DataType::Int64, false),
        ]));
        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "_event_lsn".to_string(),
        );
        ctx.table_config.insert(
            PARTIAL_UPDATE_MODE.to_string(),
            PARTIAL_UPDATE_MODE_IGNORE_DEFAULTS.to_string(),
        );
        ctx.rebuild_record_context(String::new());
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(schema.clone())
            .with_data_schema(schema.clone())
            .with_requested_schema(schema.clone());
        let key_field = ctx.record_key_field().to_string();
        let ordering = ctx.record_context.ordering_field_names.clone();
        handler
            .prepare_required_schema(
                true,
                &[key_field],
                &ordering,
                &ctx.table_config,
                false,
                merge_mode,
            )
            .unwrap();
        ctx.schema_handler = handler;
        KeyBasedFileGroupRecordBuffer::new(Arc::new(ctx), merge_mode.to_string(), false).unwrap()
    }

    /// A Debezium-shaped batch whose `val` may be null (the IGNORE_DEFAULTS trigger).
    fn ignore_defaults_batch(rows: &[(&str, Option<&str>, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("val", DataType::Utf8, true),
            Field::new("_event_lsn", DataType::Int64, false),
        ]));
        let keys: Vec<&str> = rows.iter().map(|r| r.0).collect();
        let vals: Vec<Option<&str>> = rows.iter().map(|r| r.1).collect();
        let lsns: Vec<i64> = rows.iter().map(|r| r.2).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(vals)),
                Arc::new(Int64Array::from(lsns)),
            ],
        )
        .unwrap()
    }

    /// Base-vs-log IGNORE_DEFAULTS through the vectorized kernel (the production
    /// path — the ENG-45058 / G-1 repro shape): base `(k1,"orig_k1",10)` plus a
    /// winning log update `(k1, NULL, 20)` that leaves `val` at its default (null).
    /// The merged row must KEEP the prior base value "orig_k1"; an ordinary
    /// non-null update (k2) still overwrites wholesale. Mirrors Java
    /// `PartialUpdateHandler.reconcileDefaultValues`.
    #[test]
    fn ignore_defaults_null_keeps_prior_base_value_event_time() {
        assert_ignore_defaults_null_keeps_prior("EVENT_TIME_ORDERING");
    }

    /// COMMIT_TIME_ORDERING counterpart — the blend runs in the LogData-winner
    /// branch and is independent of the ordering mode, but Java covers both, so
    /// pin it here too. The base file predates the log, so the log always wins.
    #[test]
    fn ignore_defaults_null_keeps_prior_base_value_commit_time() {
        assert_ignore_defaults_null_keeps_prior("COMMIT_TIME_ORDERING");
    }

    fn assert_ignore_defaults_null_keeps_prior(merge_mode: &str) {
        let mut buffer = build_ignore_defaults_buffer_with_mode(merge_mode);
        // Log records carry the newer LSN (20) so they win:
        //   k1 — default update: `val` is NULL (only `_event_lsn` changed)
        //   k2 — ordinary update: a real new `val`
        let log = ignore_defaults_batch(&[("k1", None, 20), ("k2", Some("updated_k2"), 20)]);
        buffer
            .process_data_block(&mut make_data_block_inline(log, "i1"))
            .unwrap();
        let base =
            ignore_defaults_batch(&[("k1", Some("orig_k1"), 10), ("k2", Some("orig_k2"), 10)]);
        let src_schema = base.schema();
        buffer.set_base_file_source(Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(base)].into_iter(),
            src_schema,
        )));

        let required = buffer
            .reader_context
            .schema_handler
            .required_schema
            .clone()
            .unwrap();
        let requested = buffer
            .reader_context
            .schema_handler
            .requested_schema
            .clone()
            .unwrap();
        let conv = buffer.reader_context.schema_handler.get_output_converter();
        let iter = new_buffered_test(
            Box::new(buffer),
            required,
            requested.clone(),
            conv,
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);
        let concat = arrow::compute::concat_batches(&requested, &batches).unwrap();

        let keys = concat
            .column(concat.schema().index_of("_hoodie_record_key").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vals = concat
            .column(concat.schema().index_of("val").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut got: Vec<(String, Option<String>)> = (0..concat.num_rows())
            .map(|i| {
                (
                    keys.value(i).to_string(),
                    if vals.is_null(i) {
                        None
                    } else {
                        Some(vals.value(i).to_string())
                    },
                )
            })
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                // IGNORE_DEFAULTS: k1's null (default) `val` KEEPS the prior base value.
                ("k1".to_string(), Some("orig_k1".to_string())),
                // ordinary update: log wins wholesale.
                ("k2".to_string(), Some("updated_k2".to_string())),
            ],
            "IGNORE_DEFAULTS: k1's null val must retain the prior base value, not \
             silently overwrite it with the null default"
        );
    }

    /// Base-vs-log IGNORE_DEFAULTS where the BASE row WINS the ordering (its
    /// `_event_lsn` out-ranks the log record's) yet carries a DEFAULT (null)
    /// column: the winning base value must be BACK-FILLED from the LOSING log
    /// record. Mirrors Java `EventTimePartialRecordMerger.finalMerge`, which fills
    /// the winner from the loser in BOTH directions — when the older/base record
    /// wins it runs `partialMerge(base, log)`.
    ///
    /// This is the sweep P3/P4 base-vs-log repro
    /// (`testPartialUpdateBackfillsWinnerFromLoser`): the ts=2 upsert lands in a
    /// new base file (null `name`) and out-ranks a later ts=1 log update carrying
    /// the real value.
    ///
    /// Discriminating: before the fix the `Winner::Base` branch emitted the base
    /// row unchanged, so the winner's null column was never back-filled and the
    /// merged value stayed null (Class-C silent-wrong). The base's higher ordering
    /// is preserved; only its default column takes the loser's real value. A
    /// control key whose winning base value is non-null is emitted untouched.
    #[test]
    fn ignore_defaults_base_wins_backfills_default_from_losing_log() {
        let mut buffer = build_ignore_defaults_buffer_with_mode("EVENT_TIME_ORDERING");
        // Log records carry the LOWER lsn (10) so the base (lsn 20) WINS:
        //   k1 — base `val` is NULL (default); losing log carries the real value.
        //   k2 — base `val` is real; losing log value must be ignored (base kept).
        let log = ignore_defaults_batch(&[
            ("k1", Some("real_from_log"), 10),
            ("k2", Some("stale_log"), 10),
        ]);
        buffer
            .process_data_block(&mut make_data_block_inline(log, "i1"))
            .unwrap();
        let base = ignore_defaults_batch(&[("k1", None, 20), ("k2", Some("orig_k2"), 20)]);
        let src_schema = base.schema();
        buffer.set_base_file_source(Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(base)].into_iter(),
            src_schema,
        )));

        let required = buffer
            .reader_context
            .schema_handler
            .required_schema
            .clone()
            .unwrap();
        let requested = buffer
            .reader_context
            .schema_handler
            .requested_schema
            .clone()
            .unwrap();
        let conv = buffer.reader_context.schema_handler.get_output_converter();
        let iter = new_buffered_test(
            Box::new(buffer),
            required,
            requested.clone(),
            conv,
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);
        let concat = arrow::compute::concat_batches(&requested, &batches).unwrap();

        let keys = concat
            .column(concat.schema().index_of("_hoodie_record_key").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vals = concat
            .column(concat.schema().index_of("val").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut got: Vec<(String, Option<String>)> = (0..concat.num_rows())
            .map(|i| {
                (
                    keys.value(i).to_string(),
                    if vals.is_null(i) {
                        None
                    } else {
                        Some(vals.value(i).to_string())
                    },
                )
            })
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                // Base wins on ordering but its null (default) val is back-filled
                // from the losing log record (P3/P4 both-direction fill).
                ("k1".to_string(), Some("real_from_log".to_string())),
                // Base wins and already holds a real value → losing log ignored.
                ("k2".to_string(), Some("orig_k2".to_string())),
            ],
            "IGNORE_DEFAULTS: a winning base row's default column must be back-filled from the \
             losing log record (Java finalMerge fills the winner from the loser both directions)"
        );
    }

    /// Base-vs-log IGNORE_DEFAULTS with a NON-NULL declared default (the Java
    /// `TestBufferedRecordMerger` case: `age: [int,null] default 0`). A winning
    /// log update carrying `age = 0` (the declared default) must retain the prior
    /// base `age = 25`; a real value (`age = 40`) still overwrites. Exercises the
    /// `DefaultRetain::OnInt` path parsed from the table's Avro schema JSON.
    #[test]
    fn ignore_defaults_nonnull_int_default_keeps_prior_base_value() {
        let avro_json = r#"{"type":"record","name":"TestRecord","fields":[
            {"name":"_hoodie_record_key","type":"string"},
            {"name":"age","type":["int","null"],"default":0},
            {"name":"_event_lsn","type":"long"}
        ]}"#;
        let arrow_schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
            Field::new("_event_lsn", DataType::Int64, false),
        ]));
        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "_event_lsn".to_string(),
        );
        ctx.table_config.insert(
            PARTIAL_UPDATE_MODE.to_string(),
            PARTIAL_UPDATE_MODE_IGNORE_DEFAULTS.to_string(),
        );
        ctx.rebuild_record_context(String::new());
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(arrow_schema.clone())
            .with_data_schema(arrow_schema.clone())
            .with_requested_schema(arrow_schema.clone())
            .with_data_schema_json(avro_json.to_string());
        let key_field = ctx.record_key_field().to_string();
        let ordering = ctx.record_context.ordering_field_names.clone();
        handler
            .prepare_required_schema(
                true,
                &[key_field],
                &ordering,
                &ctx.table_config,
                false,
                "EVENT_TIME_ORDERING",
            )
            .unwrap();
        ctx.schema_handler = handler;
        let mut buffer = KeyBasedFileGroupRecordBuffer::new(
            Arc::new(ctx),
            "EVENT_TIME_ORDERING".to_string(),
            false,
        )
        .unwrap();

        let int_row = |key: &str, age: Option<i32>, lsn: i64| {
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![key])) as _,
                    Arc::new(Int32Array::from(vec![age])) as _,
                    Arc::new(Int64Array::from(vec![lsn])) as _,
                ],
            )
            .unwrap()
        };
        // Winning log updates (lsn 20): k1 sets age to the declared default 0
        // (must keep prior 25); k2 sets a real age 40 (must overwrite).
        let log = arrow::compute::concat_batches(
            &arrow_schema,
            &[int_row("k1", Some(0), 20), int_row("k2", Some(40), 20)],
        )
        .unwrap();
        buffer
            .process_data_block(&mut make_data_block_inline(log, "i1"))
            .unwrap();
        let base = arrow::compute::concat_batches(
            &arrow_schema,
            &[int_row("k1", Some(25), 10), int_row("k2", Some(15), 10)],
        )
        .unwrap();
        let src_schema = base.schema();
        buffer.set_base_file_source(Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(base)].into_iter(),
            src_schema,
        )));

        let required = buffer
            .reader_context
            .schema_handler
            .required_schema
            .clone()
            .unwrap();
        let requested = buffer
            .reader_context
            .schema_handler
            .requested_schema
            .clone()
            .unwrap();
        let conv = buffer.reader_context.schema_handler.get_output_converter();
        let iter = new_buffered_test(
            Box::new(buffer),
            required,
            requested.clone(),
            conv,
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);
        let concat = arrow::compute::concat_batches(&requested, &batches).unwrap();
        let keys = concat
            .column(concat.schema().index_of("_hoodie_record_key").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ages = concat
            .column(concat.schema().index_of("age").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut got: Vec<(String, i32)> = (0..concat.num_rows())
            .map(|i| (keys.value(i).to_string(), ages.value(i)))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![("k1".to_string(), 25), ("k2".to_string(), 40)],
            "IGNORE_DEFAULTS: age==declared default 0 keeps prior 25; a real age 40 overwrites"
        );
    }

    #[test]
    fn ignore_defaults_parse_retain_rules_from_avro() {
        let json = r#"{"type":"record","name":"R","fields":[
            {"name":"a","type":["null","string"],"default":null},
            {"name":"b","type":["int","null"],"default":0},
            {"name":"c","type":["long","null"],"default":5},
            {"name":"d","type":"boolean","default":true},
            {"name":"e","type":["int","null"],"default":200},
            {"name":"f","type":["double","null"],"default":0.0},
            {"name":"g","type":["string","null"],"default":"x"},
            {"name":"h","type":"int"}
        ]}"#;
        let m = parse_default_retains(json);
        assert_eq!(m["a"], DefaultRetain::OnNull, "null default → OnNull");
        assert_eq!(
            m["b"],
            DefaultRetain::OnInt(0),
            "int default 0 in cache range"
        );
        assert_eq!(
            m["c"],
            DefaultRetain::OnInt(5),
            "long default 5 in cache range"
        );
        assert_eq!(m["d"], DefaultRetain::OnBool(true), "boolean default");
        assert_eq!(
            m["e"],
            DefaultRetain::Never,
            "int 200 is out of the JVM cache range"
        );
        assert_eq!(
            m["f"],
            DefaultRetain::Never,
            "double defaults are never == matched"
        );
        assert_eq!(
            m["g"],
            DefaultRetain::Never,
            "string defaults are never == matched"
        );
        assert_eq!(
            m["h"],
            DefaultRetain::OnNull,
            "no declared default → OnNull"
        );
        // A malformed schema yields an empty map (caller falls back to OnNull).
        assert!(parse_default_retains("not json").is_empty());
    }

    #[test]
    fn ignore_defaults_value_is_default_rules() {
        let null_i = Int32Array::from(vec![None::<i32>]);
        let zero_i = Int32Array::from(vec![Some(0)]);
        let five_l = Int64Array::from(vec![Some(5_i64)]);
        let t = BooleanArray::from(vec![Some(true)]);
        // OnNull matches only a null value.
        assert!(value_is_default(&null_i, &DefaultRetain::OnNull));
        assert!(!value_is_default(&zero_i, &DefaultRetain::OnNull));
        // OnInt matches the exact value, not null, not a different value.
        assert!(value_is_default(&zero_i, &DefaultRetain::OnInt(0)));
        assert!(!value_is_default(&zero_i, &DefaultRetain::OnInt(1)));
        assert!(!value_is_default(&null_i, &DefaultRetain::OnInt(0)));
        assert!(value_is_default(&five_l, &DefaultRetain::OnInt(5)));
        // OnBool matches the exact boolean.
        assert!(value_is_default(&t, &DefaultRetain::OnBool(true)));
        assert!(!value_is_default(&t, &DefaultRetain::OnBool(false)));
        // Never matches nothing.
        assert!(!value_is_default(&zero_i, &DefaultRetain::Never));
    }

    /// Log-vs-log IGNORE_DEFAULTS: two updates to one key, NO base file. The second
    /// update leaves `payload` null (default); the merged output must carry the
    /// prior log record's `payload`, not null — the purely-in-log analog (≥2 log
    /// updates never touch the base), which would otherwise be Class-C silent-wrong.
    #[test]
    fn test_log_vs_log_ignore_defaults_null_filled_from_prior_buffered_record() {
        let mut buffer = build_ignore_defaults_buffer();
        buffer
            .process_next_data_record(ignore_defaults_row("1", Some("full-value"), 1), "1")
            .unwrap();
        buffer
            .process_next_data_record(ignore_defaults_row("1", None, 2), "1")
            .unwrap();
        buffer.set_base_file_iterator(vec![]);

        let out = Box::new(buffer).merge_and_collect().unwrap();
        assert_eq!(out.num_rows(), 1, "one key survives");
        let payloads = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tss = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            payloads.value(0),
            "full-value",
            "null (default) payload must be filled from the prior log record"
        );
        assert_eq!(tss.value(0), 2, "the newer ordering (ts=2) record wins");
    }

    /// P3/P4: Log-vs-log IGNORE_DEFAULTS where the HIGHER-ordering record (carrying a
    /// DEFAULT/null column) arrives FIRST and WINS, and a later LOWER-ordering record
    /// carries the REAL value. The winner (existing) must be BACK-FILLED from the
    /// incoming loser — mirroring Java `EventTimePartialRecordMerger.deltaMerge`,
    /// which fills the winner from the loser in BOTH directions.
    ///
    /// Discriminating: before the fix, hudi-rs filled only new-from-prior and
    /// returned the winning existing record UNMODIFIED, so the winner's null
    /// `payload` was never back-filled and the merged output was `null` (Class-C
    /// silent-wrong). The winner's ordering (ts=2) is preserved; its default column
    /// takes the loser's real value.
    #[test]
    fn test_log_vs_log_ignore_defaults_existing_winner_backfilled_from_incoming() {
        let mut buffer = build_ignore_defaults_buffer();
        // Higher-ordering (ts=2) record with a DEFAULT (null) payload arrives FIRST.
        buffer
            .process_next_data_record(ignore_defaults_row("1", None, 2), "1")
            .unwrap();
        // Lower-ordering (ts=1) record carrying the REAL payload arrives SECOND.
        buffer
            .process_next_data_record(ignore_defaults_row("1", Some("real-value"), 1), "1")
            .unwrap();
        buffer.set_base_file_iterator(vec![]);

        let out = Box::new(buffer).merge_and_collect().unwrap();
        assert_eq!(out.num_rows(), 1, "one key survives");
        let payloads = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tss = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            payloads.value(0),
            "real-value",
            "P3/P4: the winning (ts=2) record's default payload is back-filled from \
             the lower-ordering (ts=1) incoming record's real value"
        );
        assert_eq!(
            tss.value(0),
            2,
            "the higher ordering (ts=2) record still wins; only its default column is filled"
        );
    }

    /// Sanity counterpart: when the newer update carries a REAL (non-null) payload,
    /// it overwrites the prior value as usual — the IGNORE_DEFAULTS blend must not
    /// interfere with ordinary updates.
    #[test]
    fn test_log_vs_log_ignore_defaults_non_null_update_overwrites_normally() {
        let mut buffer = build_ignore_defaults_buffer();
        buffer
            .process_next_data_record(ignore_defaults_row("1", Some("v1"), 1), "1")
            .unwrap();
        buffer
            .process_next_data_record(ignore_defaults_row("1", Some("v2"), 2), "1")
            .unwrap();
        buffer.set_base_file_iterator(vec![]);

        let out = Box::new(buffer).merge_and_collect().unwrap();
        let payloads = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(payloads.value(0), "v2", "newer real value wins unchanged");
    }

    /// End-to-end buffer regression (lin/virtual-key-base-read): a VIRTUAL-KEY
    /// base file (no meta columns, `id INT` primary key) merged with NO log
    /// records must return every base row. Before the fix the base-merge kernel's
    /// `record_key_array` downcast the INT key column to `StringArray`, errored,
    /// and the error propagated out of `merge_one_base_batch_kernel` — dropping the
    /// entire base batch (0 rows: the gluten `TestInsertTable2` virtual-key
    /// bulk-insert silent-wrong symptom).
    #[test]
    fn test_virtual_key_int_base_only_merge_returns_rows() {
        let merge_mode = "COMMIT_TIME_ORDERING";
        let base_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("longField", DataType::Int64, false),
        ]));

        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            "hoodie.populate.meta.fields".to_string(),
            "false".to_string(),
        );
        ctx.table_config.insert(
            "hoodie.table.recordkey.fields".to_string(),
            "id".to_string(),
        );
        ctx.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "longField".to_string(),
        );
        ctx.rebuild_record_context(String::new());
        assert_eq!(ctx.record_key_field(), "id");

        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(base_schema.clone())
            .with_data_schema(base_schema.clone());
        let key_fields = ctx.record_key_fields();
        let ordering = ctx.record_context.ordering_field_names.clone();
        handler
            .prepare_required_schema(
                true,
                &key_fields,
                &ordering,
                &ctx.table_config,
                false,
                merge_mode,
            )
            .unwrap();
        ctx.schema_handler = handler;

        let mut buffer =
            KeyBasedFileGroupRecordBuffer::new(Arc::new(ctx), merge_mode.to_string(), false)
                .unwrap();
        let base = RecordBatch::try_new(
            base_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();
        buffer.set_base_file_iterator(vec![base]);

        let out = Box::new(buffer).merge_and_collect().unwrap();
        assert_eq!(out.num_rows(), 2, "both virtual-key base rows must survive");
        let ids = out
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut got: Vec<i32> = (0..ids.len()).map(|i| ids.value(i)).collect();
        got.sort_unstable();
        assert_eq!(got, vec![1, 2]);
    }

    /// Build an EVENT_TIME buffer over a VIRTUAL-KEY table
    /// (`hoodie.populate.meta.fields = false`, key drawn from data column(s)
    /// named by `key_fields_csv`) with the given partial-update `table_config`
    /// entries (IGNORE_DEFAULTS mode or a FILL_UNAVAILABLE sentinel) and an
    /// optional Avro data-schema JSON carrying the declared field defaults.
    /// The precombine (ordering) field is `ts`.
    fn build_virtual_key_partial_update_buffer(
        schema: &Arc<Schema>,
        key_fields_csv: &str,
        avro_json: Option<&str>,
        table_config: &[(&str, &str)],
    ) -> KeyBasedFileGroupRecordBuffer {
        let merge_mode = "EVENT_TIME_ORDERING";
        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            "hoodie.populate.meta.fields".to_string(),
            "false".to_string(),
        );
        ctx.table_config.insert(
            "hoodie.table.recordkey.fields".to_string(),
            key_fields_csv.to_string(),
        );
        ctx.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "ts".to_string(),
        );
        for (k, v) in table_config {
            ctx.table_config.insert((*k).to_string(), (*v).to_string());
        }
        ctx.rebuild_record_context(String::new());
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(schema.clone())
            .with_data_schema(schema.clone())
            .with_requested_schema(schema.clone());
        if let Some(json) = avro_json {
            handler = handler.with_data_schema_json(json.to_string());
        }
        let key_fields = ctx.record_key_fields();
        let ordering = ctx.record_context.ordering_field_names.clone();
        handler
            .prepare_required_schema(
                true,
                &key_fields,
                &ordering,
                &ctx.table_config,
                false,
                merge_mode,
            )
            .unwrap();
        ctx.schema_handler = handler;
        KeyBasedFileGroupRecordBuffer::new(Arc::new(ctx), merge_mode.to_string(), false).unwrap()
    }

    /// Drive a populated-buffer base-vs-log merge through the production
    /// streaming path (log block → base source → `FileGroupMergeIterator`)
    /// and return the concatenated output in the requested schema.
    fn merge_log_block_with_base_streaming(
        mut buffer: KeyBasedFileGroupRecordBuffer,
        log: RecordBatch,
        base: RecordBatch,
    ) -> RecordBatch {
        buffer
            .process_data_block(&mut make_data_block_inline(log, "i1"))
            .unwrap();
        let src_schema = base.schema();
        buffer.set_base_file_source(Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(base)].into_iter(),
            src_schema,
        )));
        let required = buffer
            .reader_context
            .schema_handler
            .required_schema
            .clone()
            .unwrap();
        let requested = buffer
            .reader_context
            .schema_handler
            .requested_schema
            .clone()
            .unwrap();
        let conv = buffer.reader_context.schema_handler.get_output_converter();
        let iter = new_buffered_test(
            Box::new(buffer),
            required,
            requested.clone(),
            conv,
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);
        arrow::compute::concat_batches(&requested, &batches).unwrap()
    }

    /// IGNORE_DEFAULTS on a VIRTUAL-KEY table (populateMetaFields=false, single
    /// INT key column `id`): a winning log update carrying `age` at its declared
    /// Avro default (0) must retain the prior base `age`; the key column is
    /// untouched (base and log are matched by key, so it is provably equal on
    /// both sides). A real `age` still overwrites.
    #[test]
    fn test_ignore_defaults_with_virtual_int_key() {
        let avro_json = r#"{"type":"record","name":"R","fields":[
            {"name":"id","type":"int"},
            {"name":"age","type":["int","null"],"default":0},
            {"name":"ts","type":"long"}
        ]}"#;
        let schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("age", DataType::Int32, true),
            Field::new("ts", DataType::Int64, false),
        ]));
        let buffer = build_virtual_key_partial_update_buffer(
            &schema,
            "id",
            Some(avro_json),
            &[(PARTIAL_UPDATE_MODE, PARTIAL_UPDATE_MODE_IGNORE_DEFAULTS)],
        );

        let row = |id: i32, age: Option<i32>, ts: i64| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![id])) as _,
                    Arc::new(Int32Array::from(vec![age])) as _,
                    Arc::new(Int64Array::from(vec![ts])) as _,
                ],
            )
            .unwrap()
        };
        // Winning log updates (ts 20): id=1 sets age to the declared default 0
        // (must keep prior 25); id=2 sets a real age 40 (must overwrite).
        let log =
            arrow::compute::concat_batches(&schema, &[row(1, Some(0), 20), row(2, Some(40), 20)])
                .unwrap();
        let base =
            arrow::compute::concat_batches(&schema, &[row(1, Some(25), 10), row(2, Some(15), 10)])
                .unwrap();

        let out = merge_log_block_with_base_streaming(buffer, log, base);
        let ids = out
            .column(out.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let ages = out
            .column(out.schema().index_of("age").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let tss = out
            .column(out.schema().index_of("ts").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut got: Vec<(i32, i32, i64)> = (0..out.num_rows())
            .map(|i| (ids.value(i), ages.value(i), tss.value(i)))
            .collect();
        got.sort_unstable();
        assert_eq!(
            got,
            vec![(1, 25, 20), (2, 40, 20)],
            "virtual INT key: age==default 0 retains prior 25, real age 40 overwrites; \
             key column and ts come from the winning log rows untouched"
        );
    }

    /// IGNORE_DEFAULTS on a COMPOSITE virtual key (`id INT + name STRING`, merge
    /// key `id:1,name:a`): both sides must pair on the composite key (exactly one
    /// output row for the shared key) and a winning log `age` at its declared
    /// default retains the prior base `age`; both key columns are untouched.
    #[test]
    fn test_ignore_defaults_with_composite_virtual_key() {
        let avro_json = r#"{"type":"record","name":"R","fields":[
            {"name":"id","type":"int"},
            {"name":"name","type":"string"},
            {"name":"age","type":["int","null"],"default":0},
            {"name":"ts","type":"long"}
        ]}"#;
        let schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
            Field::new("ts", DataType::Int64, false),
        ]));
        let buffer = build_virtual_key_partial_update_buffer(
            &schema,
            "id,name",
            Some(avro_json),
            &[(PARTIAL_UPDATE_MODE, PARTIAL_UPDATE_MODE_IGNORE_DEFAULTS)],
        );

        let row = |id: i32, name: &str, age: Option<i32>, ts: i64| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![id])) as _,
                    Arc::new(StringArray::from(vec![name])) as _,
                    Arc::new(Int32Array::from(vec![age])) as _,
                    Arc::new(Int64Array::from(vec![ts])) as _,
                ],
            )
            .unwrap()
        };
        // Base (id:1,name:a) age=25; winning log update for the SAME composite
        // key sets age to the declared default 0 → prior 25 retained.
        let log = row(1, "a", Some(0), 20);
        let base = row(1, "a", Some(25), 10);

        let out = merge_log_block_with_base_streaming(buffer, log, base);
        assert_eq!(
            out.num_rows(),
            1,
            "base and log must pair on the composite key 'id:1,name:a' → one merged row"
        );
        let ids = out
            .column(out.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = out
            .column(out.schema().index_of("name").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ages = out
            .column(out.schema().index_of("age").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let tss = out
            .column(out.schema().index_of("ts").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            (ids.value(0), names.value(0), ages.value(0), tss.value(0)),
            (1, "a", 25, 20),
            "composite virtual key: key columns untouched, age==default retains prior 25, \
             ts comes from the winning log row"
        );
    }

    /// FILL_UNAVAILABLE (Debezium toasted sentinel) on a VIRTUAL-KEY table: a
    /// winning log update whose non-key `payload` carries the sentinel is filled
    /// from the prior base row; the INT key column is untouched. A real payload
    /// still overwrites.
    #[test]
    fn test_fill_unavailable_with_virtual_key() {
        let schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, false),
        ]));
        let buffer = build_virtual_key_partial_update_buffer(
            &schema,
            "id",
            None,
            &[(PARTIAL_UPDATE_UNAVAILABLE_VALUE, TOASTED_SENTINEL)],
        );

        let row = |id: i32, payload: &str, ts: i64| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![id])) as _,
                    Arc::new(StringArray::from(vec![payload])) as _,
                    Arc::new(Int64Array::from(vec![ts])) as _,
                ],
            )
            .unwrap()
        };
        // Winning log updates (ts 20): id=1 arrives toasted (sentinel payload →
        // fill "full-value" from base); id=2 carries a real payload (overwrite).
        let log = arrow::compute::concat_batches(
            &schema,
            &[row(1, TOASTED_SENTINEL, 20), row(2, "new-2", 20)],
        )
        .unwrap();
        let base = arrow::compute::concat_batches(
            &schema,
            &[row(1, "full-value", 10), row(2, "orig-2", 10)],
        )
        .unwrap();

        let out = merge_log_block_with_base_streaming(buffer, log, base);
        let ids = out
            .column(out.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let payloads = out
            .column(out.schema().index_of("payload").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tss = out
            .column(out.schema().index_of("ts").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut got: Vec<(i32, String, i64)> = (0..out.num_rows())
            .map(|i| (ids.value(i), payloads.value(i).to_string(), tss.value(i)))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                (1, "full-value".to_string(), 20),
                (2, "new-2".to_string(), 20),
            ],
            "virtual key + FILL_UNAVAILABLE: toasted payload filled from base, key column \
             untouched, real payload overwrites"
        );
    }

    /// The IGNORE_DEFAULTS retain decision at the JVM Integer/Long-cache range
    /// boundaries: Java's `toJavaDefaultValue(field) == newValue` is reference
    /// equality, and autoboxing reuses cached instances only for `-128..=127` —
    /// so retain fires ONLY for defaults inside that range. `-129` and `128`
    /// must NEVER retain, even when the incoming value equals the default.
    #[test]
    fn test_default_retain_cache_range_boundaries() {
        let json = r#"{"type":"record","name":"R","fields":[
            {"name":"below_int","type":["int","null"],"default":-129},
            {"name":"min_int","type":["int","null"],"default":-128},
            {"name":"max_long","type":["long","null"],"default":127},
            {"name":"above_long","type":["long","null"],"default":128}
        ]}"#;
        let m = parse_default_retains(json);
        assert_eq!(
            m["below_int"],
            DefaultRetain::Never,
            "int default -129 is below the JVM cache range → never retained"
        );
        assert_eq!(
            m["min_int"],
            DefaultRetain::OnInt(-128),
            "int default -128 is the cache-range lower bound → retained"
        );
        assert_eq!(
            m["max_long"],
            DefaultRetain::OnInt(127),
            "long default 127 is the cache-range upper bound → retained"
        );
        assert_eq!(
            m["above_long"],
            DefaultRetain::Never,
            "long default 128 is above the JVM cache range → never retained"
        );

        // The decision fn: a matching incoming value retains ONLY inside the range.
        assert!(
            value_is_default(&Int32Array::from(vec![Some(-128)]), &m["min_int"]),
            "incoming -128 matches the cached -128 default"
        );
        assert!(
            value_is_default(&Int64Array::from(vec![Some(127_i64)]), &m["max_long"]),
            "incoming 127 matches the cached 127 default"
        );
        assert!(
            !value_is_default(&Int32Array::from(vec![Some(-129)]), &m["below_int"]),
            "incoming -129 equals the declared default but must NOT retain (out of cache)"
        );
        assert!(
            !value_is_default(&Int64Array::from(vec![Some(128_i64)]), &m["above_long"]),
            "incoming 128 equals the declared default but must NOT retain (out of cache)"
        );
    }

    /// A string column whose winning value equals its declared Avro STRING
    /// default must NOT retain the prior value: Java's reference `==` never
    /// matches two distinct String instances, so the new value wins wholesale.
    #[test]
    fn test_string_default_never_retained() {
        let avro_json = r#"{"type":"record","name":"R","fields":[
            {"name":"_hoodie_record_key","type":"string"},
            {"name":"val","type":["string","null"],"default":"unknown"},
            {"name":"_event_lsn","type":"long"}
        ]}"#;
        assert_eq!(
            parse_default_retains(avro_json)["val"],
            DefaultRetain::Never,
            "a declared string default parses to Never (Java string == never matches)"
        );

        let schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("val", DataType::Utf8, true),
            Field::new("_event_lsn", DataType::Int64, false),
        ]));
        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "_event_lsn".to_string(),
        );
        ctx.table_config.insert(
            PARTIAL_UPDATE_MODE.to_string(),
            PARTIAL_UPDATE_MODE_IGNORE_DEFAULTS.to_string(),
        );
        ctx.rebuild_record_context(String::new());
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(schema.clone())
            .with_data_schema(schema.clone())
            .with_requested_schema(schema.clone())
            .with_data_schema_json(avro_json.to_string());
        let key_field = ctx.record_key_field().to_string();
        let ordering = ctx.record_context.ordering_field_names.clone();
        handler
            .prepare_required_schema(
                true,
                &[key_field],
                &ordering,
                &ctx.table_config,
                false,
                "EVENT_TIME_ORDERING",
            )
            .unwrap();
        ctx.schema_handler = handler;
        let buffer = KeyBasedFileGroupRecordBuffer::new(
            Arc::new(ctx),
            "EVENT_TIME_ORDERING".to_string(),
            false,
        )
        .unwrap();

        let row = |key: &str, val: &str, lsn: i64| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![key])) as _,
                    Arc::new(StringArray::from(vec![val])) as _,
                    Arc::new(Int64Array::from(vec![lsn])) as _,
                ],
            )
            .unwrap()
        };
        // Winning log update sets `val` to the declared string default "unknown":
        // it must OVERWRITE the prior "orig", not retain it.
        let log = row("k1", "unknown", 20);
        let base = row("k1", "orig", 10);

        let out = merge_log_block_with_base_streaming(buffer, log, base);
        assert_eq!(out.num_rows(), 1, "one key → one merged row");
        let keys = out
            .column(out.schema().index_of("_hoodie_record_key").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vals = out
            .column(out.schema().index_of("val").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let lsns = out
            .column(out.schema().index_of("_event_lsn").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            (keys.value(0), vals.value(0), lsns.value(0)),
            ("k1", "unknown", 20),
            "string default 'unknown' must NOT retain the prior 'orig' — new value wins"
        );
    }

    // =========================================================================
    // Test helper infrastructure (mirrors Java's BaseTestFileGroupRecordBuffer)
    // =========================================================================

    /// Wrap a RecordBatch in a LogBlock with content already populated.
    /// Used by tests so they can call `process_data_block(&mut block)` directly.
    fn make_data_block(batch: RecordBatch, instant: &str) -> LogBlock {
        let mut header = HashMap::new();
        header.insert(BlockMetadataKey::InstantTime, instant.to_string());
        LogBlock::new(
            LogFormatVersion::V1,
            BlockType::ParquetData,
            header,
            LogBlockContent::Records(RecordBatches::new_with_data_batches(vec![batch])),
            HashMap::new(),
        )
    }

    /// Wrap a delete batch (recordKey, partitionPath, orderingVal) in a delete
    /// LogBlock so tests can drive `process_delete_block(&mut block)` — the real
    /// delete-block path (as opposed to calling `process_next_deleted_record`
    /// with a hand-built `DeleteRecord`, which bypasses ordering extraction).
    fn make_delete_block(entries: &[(&str, Option<i64>)], instant: &str) -> LogBlock {
        let schema = Arc::new(Schema::new(vec![
            Field::new("recordKey", DataType::Utf8, false),
            Field::new("partitionPath", DataType::Utf8, true),
            Field::new("orderingVal", DataType::Int64, true),
        ]));
        let keys: Vec<&str> = entries.iter().map(|(k, _)| *k).collect();
        let parts: Vec<&str> = entries.iter().map(|_| "").collect();
        let ords: Vec<Option<i64>> = entries.iter().map(|(_, o)| *o).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(parts)),
                Arc::new(Int64Array::from(ords)),
            ],
        )
        .unwrap();
        let mut batches = RecordBatches::new();
        batches.push_delete_batch(batch, instant.to_string());
        let mut header = HashMap::new();
        header.insert(BlockMetadataKey::InstantTime, instant.to_string());
        LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Delete,
            header,
            LogBlockContent::Records(batches),
            HashMap::new(),
        )
    }

    /// Like [`make_delete_block`] but with an `Int32` (`int`) `orderingVal` column.
    /// This is the faithful shape for a delete carrying
    /// `HoodieRecord.DEFAULT_ORDERING_VALUE` (a Java `int 0`, serialized via
    /// `IntWrapper`) — e.g. a global-index relocate DELETE. An `Int32` value of `0`
    /// decodes to [`OrderingValue::Default`] (the natural-order default), whereas the
    /// `Int64` (`bigint`) column in [`make_delete_block`] decodes a genuine `0` to
    /// `Long(0)` (GAP-2 — a real ordering value, ordering-compared, not default).
    fn make_delete_block_i32(entries: &[(&str, Option<i32>)], instant: &str) -> LogBlock {
        let schema = Arc::new(Schema::new(vec![
            Field::new("recordKey", DataType::Utf8, false),
            Field::new("partitionPath", DataType::Utf8, true),
            Field::new("orderingVal", DataType::Int32, true),
        ]));
        let keys: Vec<&str> = entries.iter().map(|(k, _)| *k).collect();
        let parts: Vec<&str> = entries.iter().map(|_| "").collect();
        let ords: Vec<Option<i32>> = entries.iter().map(|(_, o)| *o).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(parts)),
                Arc::new(Int32Array::from(ords)),
            ],
        )
        .unwrap();
        let mut batches = RecordBatches::new();
        batches.push_delete_batch(batch, instant.to_string());
        let mut header = HashMap::new();
        header.insert(BlockMetadataKey::InstantTime, instant.to_string());
        LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Delete,
            header,
            LogBlockContent::Records(batches),
            HashMap::new(),
        )
    }

    /// Schema: _hoodie_record_key (Utf8), counter (Int32), ts (Int64)
    /// Matches Java test schema: record_key, counter, ts
    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("counter", DataType::Int32, false),
            Field::new("ts", DataType::Int64, false),
        ]))
    }

    /// Create a multi-row RecordBatch from a list of (key, counter, ts).
    fn create_test_batch(records: &[(&str, i32, i64)]) -> RecordBatch {
        let schema = create_test_schema();
        let keys: Vec<&str> = records.iter().map(|(k, _, _)| *k).collect();
        let counters: Vec<i32> = records.iter().map(|(_, c, _)| *c).collect();
        let timestamps: Vec<i64> = records.iter().map(|(_, _, t)| *t).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int32Array::from(counters)),
                Arc::new(Int64Array::from(timestamps)),
            ],
        )
        .unwrap()
    }

    /// Build a KeyBasedFileGroupRecordBuffer with the given merge mode.
    fn build_key_based_buffer(merge_mode: &str) -> KeyBasedFileGroupRecordBuffer {
        build_key_based_buffer_with_reader_config(merge_mode, &[])
    }

    /// Build a buffer with the given merge mode and extra `hoodie_reader_config`
    /// entries (e.g. `hoodie.memory.merge.max.size` to force spilling).
    fn build_key_based_buffer_with_reader_config(
        merge_mode: &str,
        reader_config: &[(&str, &str)],
    ) -> KeyBasedFileGroupRecordBuffer {
        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "ts".to_string(),
        );
        for (k, v) in reader_config {
            ctx.hoodie_reader_config
                .insert((*k).to_string(), (*v).to_string());
        }
        ctx.rebuild_record_context(String::new());
        // Prepare the schema handler so it creates and stores a DeleteContext
        // (required by KeyBasedFileGroupRecordBuffer::new).
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(create_test_schema())
            .with_data_schema(create_test_schema());
        let key_field = ctx.record_key_field().to_string();
        let ordering = ctx.record_context.ordering_field_names.clone();
        handler
            .prepare_required_schema(
                true,
                &[key_field],
                &ordering,
                &ctx.table_config,
                false,
                merge_mode,
            )
            .unwrap();
        ctx.schema_handler = handler;
        let ctx = Arc::new(ctx);
        KeyBasedFileGroupRecordBuffer::new(ctx, merge_mode.to_string(), false).unwrap()
    }

    /// Extract (key, counter, ts) tuples from a RecordBatch, sorted by key.
    fn extract_records(batch: &RecordBatch) -> Vec<(String, i32, i64)> {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counters = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let timestamps = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut result: Vec<(String, i32, i64)> = (0..batch.num_rows())
            .map(|i| {
                (
                    keys.value(i).to_string(),
                    counters.value(i),
                    timestamps.value(i),
                )
            })
            .collect();
        result.sort_by(|a, b| a.0.cmp(&b.0));
        result
    }

    // =========================================================================
    // A2 — compaction safety valve (compact_pinned_batches) +
    //      drain-equivalence over the batch-ref / interleave path.
    // =========================================================================

    /// Build a wide source batch of `(key_i, i, i)` rows so the sparse-survivor
    /// pinning case has real per-row memory to release.
    fn wide_source_batch(num_rows: usize) -> Arc<RecordBatch> {
        let rows: Vec<(String, i32, i64)> = (0..num_rows)
            .map(|i| (format!("k{i:05}"), i as i32, i as i64))
            .collect();
        let refs: Vec<(&str, i32, i64)> =
            rows.iter().map(|(k, c, t)| (k.as_str(), *c, *t)).collect();
        Arc::new(create_test_batch(&refs))
    }

    /// Insert `n` BatchRefs into the map, all sharing `src`, at strided rows so
    /// only a small fraction of `src`'s rows survive — the pathology compaction
    /// targets. Returns the inserted `(key, expected (key,counter,ts))` rows.
    fn insert_strided_survivors(
        records: &mut MergeMap,
        src: &Arc<RecordBatch>,
        stride: usize,
    ) -> Vec<(String, i32, i64)> {
        let all = extract_records(src);
        let mut expected = Vec::new();
        for row in (0..src.num_rows()).step_by(stride) {
            // Row index `row` of src maps to value `(k{row}, row, row)` because
            // wide_source_batch is built in order; recover the tuple from src.
            let key = all
                .iter()
                .find(|(_, c, _)| *c == row as i32)
                .cloned()
                .unwrap();
            records.insert(
                key.0.clone(),
                BufferedRecord::new_batch_ref(key.0.clone(), src.clone(), row, None),
            );
            expected.push(key);
        }
        expected
    }

    /// Sum of `get_array_memory_size` over the DISTINCT pinned source batches
    /// referenced by the map (the real heap the BatchRefs hold alive).
    fn distinct_pinned_bytes(records: &MergeMap) -> usize {
        let mut seen: std::collections::HashSet<*const RecordBatch> =
            std::collections::HashSet::new();
        let mut total = 0;
        for r in records.values() {
            if let RecordPayload::BatchRef { batch, .. } = &r.payload
                && seen.insert(Arc::as_ptr(batch))
            {
                total += batch.get_array_memory_size();
            }
        }
        total
    }

    /// Read the (key, counter, ts) tuple a record currently resolves to (via its
    /// payload), for full-data identity assertions across a repoint.
    fn record_tuple(record: &BufferedRecord) -> (String, i32, i64) {
        let b = record.get_record().expect("data record");
        let recs = extract_records(&b);
        assert_eq!(recs.len(), 1);
        recs.into_iter().next().unwrap()
    }

    /// Sparse-survivor compaction: 16 survivors out of a 512-row batch (3.1% live,
    /// < 0.5 threshold). After `compact_pinned_batches` the pinned Arrow bytes
    /// must drop sharply (dead rows released) AND every survivor must still
    /// resolve to byte-identical data (guide §5: assert full data, not counts).
    #[test]
    fn test_compaction_releases_dead_rows_and_preserves_data() {
        let mut records: MergeMap = HashMap::default();
        let src = wide_source_batch(512);
        let mut expected = insert_strided_survivors(&mut records, &src, 32); // 16 survivors
        expected.sort();

        // Drop our own strong ref so the map's BatchRefs are what pin `src`.
        let before_bytes = distinct_pinned_bytes(&records);
        let src_full_bytes = src.get_array_memory_size();
        drop(src);

        // Pre-compaction the 16 survivors pin the WHOLE 512-row batch.
        assert_eq!(
            before_bytes, src_full_bytes,
            "before compaction the survivors pin the full source batch"
        );

        compact_pinned_batches(&mut records, COMPACTION_LIVE_RATIO).unwrap();

        let after_bytes = distinct_pinned_bytes(&records);
        assert!(
            after_bytes * 4 < before_bytes,
            "compaction must release dead-row memory: before={before_bytes} after={after_bytes}"
        );

        // Full-data identity: every survivor still resolves to its original tuple.
        let mut got: Vec<(String, i32, i64)> = records.values().map(record_tuple).collect();
        got.sort();
        assert_eq!(got, expected, "data must survive the repoint byte-for-byte");

        // The compact batch holds exactly the live rows, no more.
        let compact_rows: std::collections::HashSet<usize> = records
            .values()
            .map(|r| match &r.payload {
                RecordPayload::BatchRef { batch, .. } => batch.num_rows(),
                _ => panic!("expected BatchRef after compaction"),
            })
            .collect();
        assert_eq!(
            compact_rows,
            std::collections::HashSet::from([expected.len()]),
            "the compact batch has exactly {} rows (the survivors)",
            expected.len()
        );
    }

    /// Densely-populated source (> 0.5 live) must NOT be compacted — the safety
    /// valve is a rare-firing guard, not an always-on cost. The entries keep
    /// referencing the original source batch (same Arc pointer) and the data is
    /// unchanged.
    #[test]
    fn test_compaction_skips_dense_batches() {
        let mut records: MergeMap = HashMap::default();
        let src = wide_source_batch(10);
        // 8 of 10 rows survive → 0.8 live, above the 0.5 threshold.
        let mut expected = Vec::new();
        for row in 0..8 {
            let all = extract_records(&src);
            let t = all.iter().find(|(_, c, _)| *c == row).cloned().unwrap();
            records.insert(
                t.0.clone(),
                BufferedRecord::new_batch_ref(t.0.clone(), src.clone(), row as usize, None),
            );
            expected.push(t);
        }
        expected.sort();
        let src_ptr = Arc::as_ptr(&src);

        compact_pinned_batches(&mut records, COMPACTION_LIVE_RATIO).unwrap();

        // Still pointing at the ORIGINAL batch (not recompacted).
        for r in records.values() {
            match &r.payload {
                RecordPayload::BatchRef { batch, .. } => {
                    assert_eq!(
                        Arc::as_ptr(batch),
                        src_ptr,
                        "dense batch must not be recompacted"
                    );
                }
                _ => panic!("expected BatchRef"),
            }
        }
        let mut got: Vec<(String, i32, i64)> = records.values().map(record_tuple).collect();
        got.sort();
        assert_eq!(got, expected);
    }

    /// Compaction is idempotent: a second pass over already-compacted survivors
    /// changes nothing (data identical, no error).
    #[test]
    fn test_compaction_is_idempotent() {
        let mut records: MergeMap = HashMap::default();
        let src = wide_source_batch(256);
        let mut expected = insert_strided_survivors(&mut records, &src, 64); // 4 survivors
        expected.sort();
        drop(src);

        compact_pinned_batches(&mut records, COMPACTION_LIVE_RATIO).unwrap();
        let after_first = distinct_pinned_bytes(&records);
        compact_pinned_batches(&mut records, COMPACTION_LIVE_RATIO).unwrap();
        let after_second = distinct_pinned_bytes(&records);
        assert_eq!(
            after_first, after_second,
            "second compaction pass is a no-op on the size"
        );
        let mut got: Vec<(String, i32, i64)> = records.values().map(record_tuple).collect();
        got.sort();
        assert_eq!(got, expected);
    }

    /// Compaction leaves Delete payloads untouched (they pin nothing shared) and
    /// does not error on a mixed map.
    #[test]
    fn test_compaction_ignores_deletes() {
        let mut records: MergeMap = HashMap::default();
        let src = wide_source_batch(128);
        insert_strided_survivors(&mut records, &src, 64); // 2 sparse survivors
        records.insert(
            "del".to_string(),
            BufferedRecord::new_delete("del".into(), None),
        );
        drop(src);

        compact_pinned_batches(&mut records, COMPACTION_LIVE_RATIO).unwrap();

        assert!(
            records["del"].is_delete(),
            "delete tombstone must remain a delete after compaction"
        );
    }

    /// End-to-end: a buffer whose log scan leaves sparse survivors across several
    /// log blocks, then `compact_pinned_batches`, then merge — the merged output
    /// is byte-identical to the same scan WITHOUT compaction. Compaction must be
    /// output-invariant (guide §5: full-data equivalence).
    #[test]
    fn test_compaction_is_output_invariant_end_to_end() {
        // Build a buffer with several single-row log blocks (so each minted Arc
        // batch holds one row → trivially "dense"; use multi-row blocks where
        // only some keys later survive to force sparse pins).
        fn populate() -> KeyBasedFileGroupRecordBuffer {
            let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");
            // One wide log block; base will overwrite-merge a subset, leaving the
            // rest log-only. All 8 keys survive here, but only as refs into the
            // single block batch.
            let block = create_test_batch(&[
                ("a", 1, 1),
                ("b", 2, 2),
                ("c", 3, 3),
                ("d", 4, 4),
                ("e", 5, 5),
                ("f", 6, 6),
                ("g", 7, 7),
                ("h", 8, 8),
            ]);
            buffer
                .process_data_block(&mut make_data_block(block, "i1"))
                .unwrap();
            buffer.set_base_file_iterator(vec![create_test_batch(&[("z", 0, 0)])]);
            buffer
        }

        let no_compact = Box::new(populate()).merge_and_collect().unwrap();
        let mut expected = extract_records(&no_compact);
        expected.sort();

        let mut compacted_buf = populate();
        // Force compaction regardless of ratio by calling the free fn with a
        // threshold of 1.0 (everything below 100% live compacts) — proves output
        // invariance even when compaction definitely fires.
        compact_pinned_batches(compacted_buf.base.records.in_memory_mut(), 1.0).unwrap();
        let with_compact = Box::new(compacted_buf).merge_and_collect().unwrap();
        let mut got = extract_records(&with_compact);
        got.sort();

        assert_eq!(
            got, expected,
            "compaction must not change the merged output"
        );
    }

    /// Base-record BatchRef path: a key present ONLY in the base file (no log
    /// record) is emitted from a zero-copy BatchRef into the base batch, with
    /// correct data — exercises `has_next_base_record_keyed`'s batch-ref build.
    #[test]
    fn test_base_record_batch_ref_path() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");
        // Multi-row base, no log records → every output row is a base BatchRef.
        let base = create_test_batch(&[("b0", 10, 100), ("b1", 11, 101), ("b2", 12, 102)]);
        buffer.set_base_file_iterator(vec![base]);
        let result = Box::new(buffer).merge_and_collect().unwrap();
        let recs = extract_records(&result);
        assert_eq!(
            recs,
            vec![
                ("b0".to_string(), 10, 100),
                ("b1".to_string(), 11, 101),
                ("b2".to_string(), 12, 102),
            ],
            "base-only rows must pass through the BatchRef path with exact data"
        );
    }

    /// End-to-end EVENT_TIME_ORDERING MOR merge over an **Int64** ordering column:
    /// a lower-ordering log update must lose to a higher-ordering base record, and a
    /// higher-ordering update must win. Ordering is extracted from the `ts` column
    /// through the real merge kernel (`merge_one_base_batch_kernel` → `pick_winner`),
    /// not hand-set — this is the discriminating base-vs-log merge coverage the
    /// prior MOR event-time e2e lacked. NOTE: the Int64 path is unchanged by the
    /// Float64/Float32 accessor fix; the type-coverage regression is guarded by
    /// `record_context::tests::test_ordering_value_extraction_lazy_and_eager`.
    #[test]
    fn test_event_time_mor_lower_ordering_log_update_loses_to_base() {
        let mut buffer = build_key_based_buffer("EVENT_TIME_ORDERING");
        // Log update for k1 with LOWER ts (50) than the base (100), plus k3 with a
        // HIGHER ts (200) as a sanity that a genuinely-newer update DOES win.
        let mut lower = make_data_block(create_test_batch(&[("k1", 2, 50)]), "002");
        buffer.process_data_block(&mut lower).unwrap();
        let mut higher = make_data_block(create_test_batch(&[("k3", 2, 200)]), "002");
        buffer.process_data_block(&mut higher).unwrap();
        // Base file: k1 ts=100 (should beat the ts=50 update), k2 ts=100 (untouched),
        // k3 ts=100 (should lose to the ts=200 update).
        let base = create_test_batch(&[("k1", 1, 100), ("k2", 1, 100), ("k3", 1, 100)]);
        buffer.set_base_file_iterator(vec![base]);
        let out = Box::new(buffer).merge_and_collect().unwrap();
        let recs = extract_records(&out);
        assert_eq!(
            recs,
            vec![
                ("k1".to_string(), 1, 100), // base wins: ts 100 > 50 (lower update loses)
                ("k2".to_string(), 1, 100), // untouched base
                ("k3".to_string(), 2, 200), // update wins: ts 200 > 100 (higher update wins)
            ],
            "EVENT_TIME MOR: base with higher ts must beat a lower-ts log update; \
             a higher-ts update must beat the base"
        );
    }

    // =========================================================================
    // A1 — spillable merge map acceptance (churn under a low merge budget):
    //      output byte-identical to the no-spill baseline AND spill fired.
    // =========================================================================

    /// Drive a churn-like workload (every key updated several times across log
    /// blocks, then a base file) through the buffer and return the sorted merged
    /// output. `reader_config` lets the caller set `hoodie.memory.merge.max.size`
    /// to force spilling. Also returns whether the merge map spilled.
    fn run_churn_merge(reader_config: &[(&str, &str)]) -> (Vec<(String, i32, i64)>, bool) {
        let mut buffer =
            build_key_based_buffer_with_reader_config("COMMIT_TIME_ORDERING", reader_config);
        const N: usize = 200;
        const ROUNDS: i32 = 4;
        // Several update rounds: every key rewritten each round (commit-time:
        // last writer wins → final round's value survives).
        for round in 0..ROUNDS {
            let rows: Vec<(String, i32, i64)> = (0..N)
                .map(|i| (format!("k{i:04}"), round * 1000 + i as i32, round as i64))
                .collect();
            let refs: Vec<(&str, i32, i64)> =
                rows.iter().map(|(k, c, t)| (k.as_str(), *c, *t)).collect();
            buffer
                .process_data_block(&mut make_data_block(
                    create_test_batch(&refs),
                    &format!("instant{round}"),
                ))
                .unwrap();
        }
        // Base file holds an older value for every key (the log must win).
        let base_rows: Vec<(String, i32, i64)> =
            (0..N).map(|i| (format!("k{i:04}"), -1, -1)).collect();
        let base_refs: Vec<(&str, i32, i64)> = base_rows
            .iter()
            .map(|(k, c, t)| (k.as_str(), *c, *t))
            .collect();
        buffer.set_base_file_iterator(vec![create_test_batch(&base_refs)]);

        let spilled = buffer.base.records.spill_fired();
        let out = Box::new(buffer).merge_and_collect().unwrap();
        let mut recs = extract_records(&out);
        recs.sort();
        (recs, spilled)
    }

    /// M3 acceptance signal: a churn workload under a `merge.max.size` low enough
    /// to force spilling produces output BYTE-IDENTICAL to the no-spill baseline,
    /// AND the spill actually fires. This is the unit-level equivalent of the
    /// fg-bench churn-under-low-budget e2e (the buffer is the spill chokepoint;
    /// the surrounding reader plumbing is unchanged by A1).
    #[test]
    fn test_churn_spill_output_matches_no_spill_baseline_and_spill_fires() {
        // Baseline: default 1 GiB budget → never spills for this small workload.
        let (baseline, baseline_spilled) = run_churn_merge(&[]);
        assert!(
            !baseline_spilled,
            "baseline (default budget) must not spill for this small workload"
        );
        assert_eq!(baseline.len(), 200, "200 distinct keys survive the merge");
        // Every key resolves to its FINAL update round value (round 3 = 3000+i).
        for (i, (key, counter, ts)) in baseline.iter().enumerate() {
            assert_eq!(key, &format!("k{i:04}"));
            assert_eq!(*counter, 3000 + i as i32, "log's last writer must win");
            assert_eq!(*ts, 3);
        }

        // Spill-engaged: a tiny budget (1 KiB → 0.8 KiB − reserved saturates to
        // 0) forces every NEW key to spill to RocksDB.
        let (spilled_out, did_spill) = run_churn_merge(&[("hoodie.memory.merge.max.size", "1024")]);
        assert!(
            did_spill,
            "a 1 KiB merge budget must engage the RocksDB spill tier"
        );

        // Byte-identical output despite the spill round-trip (Arrow IPC through
        // RocksDB and back): full-data equivalence, not just counts (guide §5).
        assert_eq!(
            spilled_out, baseline,
            "spilled output must be byte-identical to the no-spill baseline"
        );
    }

    /// A delete in a churn workload still tombstones correctly through the spill
    /// tier (the deleted key is absent from the output), matching the no-spill
    /// path.
    #[test]
    fn test_churn_spill_preserves_deletes() {
        // Build two buffers (spill + no-spill) with one log delete among updates.
        fn run(reader_config: &[(&str, &str)]) -> (Vec<(String, i32, i64)>, bool) {
            let mut buffer =
                build_key_based_buffer_with_reader_config("COMMIT_TIME_ORDERING", reader_config);
            // 10 keys updated, then key "k0005" deleted via a tombstone.
            let rows: Vec<(String, i32, i64)> =
                (0..10i32).map(|i| (format!("k{i:04}"), i, 1)).collect();
            let refs: Vec<(&str, i32, i64)> =
                rows.iter().map(|(k, c, t)| (k.as_str(), *c, *t)).collect();
            buffer
                .process_data_block(&mut make_data_block(create_test_batch(&refs), "i0"))
                .unwrap();
            // Spill the running map by directly inserting a tombstone after the
            // budget is exhausted (process path).
            buffer
                .process_next_deleted_record(
                    DeleteRecord {
                        record_key: "k0005".to_string(),
                        partition_path: String::new(),
                        ordering_value: None,
                    },
                    "k0005",
                )
                .unwrap();
            let spilled = buffer.base.records.spill_fired();
            buffer.set_base_file_iterator(vec![]);
            let out = Box::new(buffer).merge_and_collect().unwrap();
            let mut recs = extract_records(&out);
            recs.sort();
            (recs, spilled)
        }
        let (baseline, _) = run(&[]);
        let (spilled, did_spill) = run(&[("hoodie.memory.merge.max.size", "1024")]);
        assert!(did_spill, "tiny budget must spill");
        assert!(
            !spilled.iter().any(|(k, _, _)| k == "k0005"),
            "deleted key must be absent from spilled output"
        );
        assert_eq!(
            spilled, baseline,
            "spilled output (with delete) must match no-spill baseline"
        );
    }

    // =========================================================================
    // Part B: Tests matching Java's TestKeyBasedFileGroupRecordBuffer
    // =========================================================================

    /// Build a KeyBasedFileGroupRecordBuffer with a custom delete marker.
    /// Mirrors Java's `buildKeyBasedFileGroupRecordBuffer(..., deleteMarkerKeyValue)`.
    fn build_key_based_buffer_with_delete_marker(
        merge_mode: &str,
        delete_key: &str,
        delete_marker_value: &str,
    ) -> KeyBasedFileGroupRecordBuffer {
        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "ts".to_string(),
        );
        ctx.table_config.insert(
            "hoodie.record.merge.property.hoodie.payload.delete.field".to_string(),
            delete_key.to_string(),
        );
        ctx.table_config.insert(
            "hoodie.record.merge.property.hoodie.payload.delete.marker".to_string(),
            delete_marker_value.to_string(),
        );
        ctx.rebuild_record_context(String::new());
        // Prepare the schema handler so it creates and stores a DeleteContext.
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(create_test_schema())
            .with_data_schema(create_test_schema());
        let key_field = ctx.record_key_field().to_string();
        let ordering = ctx.record_context.ordering_field_names.clone();
        handler
            .prepare_required_schema(
                true,
                &[key_field],
                &ordering,
                &ctx.table_config,
                false,
                merge_mode,
            )
            .unwrap();
        ctx.schema_handler = handler;
        let ctx = Arc::new(ctx);
        KeyBasedFileGroupRecordBuffer::new(ctx, merge_mode.to_string(), false).unwrap()
    }

    /// Java: TestKeyBasedFileGroupRecordBuffer.readWithCommitTimeOrdering
    ///
    /// COMMIT_TIME_ORDERING with custom delete marker (counter=3).
    /// Last writer always wins. Record with counter=3 detected as delete.
    /// Expected: 2 output rows (key=3 deleted by custom marker).
    #[test]
    fn test_read_with_commit_time_ordering() {
        let mut buffer =
            build_key_based_buffer_with_delete_marker("COMMIT_TIME_ORDERING", "counter", "3");

        // DataBlock1: testRecord1UpdateWithSameTime, testRecord2Update, testRecord2EarlierUpdate
        let block1 = create_test_batch(&[
            ("1", 2, 1), // record1UpdateWithSameTime
            ("2", 1, 2), // record2Update
            ("2", 1, 0), // record2EarlierUpdate (overwrites in commit-time)
        ]);
        buffer
            .process_data_block(&mut make_data_block(block1, "instant1"))
            .unwrap();

        // DataBlock2: testRecord2EarlierUpdate, testRecord3Update, testRecord3DeleteByFieldValue
        let block2 = create_test_batch(&[
            ("2", 1, 0), // record2EarlierUpdate (same value, overwrites)
            ("3", 1, 2), // record3Update
            ("3", 3, 1), // record3DeleteByFieldValue (counter=3 → delete, overwrites)
        ]);
        buffer
            .process_data_block(&mut make_data_block(block2, "instant2"))
            .unwrap();

        let base = create_test_batch(&[("1", 1, 1), ("2", 1, 1), ("3", 1, 1)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);

        // Java: assertEquals(Arrays.asList(testRecord1UpdateWithSameTime,
        //       testRecord2EarlierUpdate), actualRecords);
        assert_eq!(result.num_rows(), 2);
        assert_eq!(records[0], ("1".to_string(), 2, 1)); // record1UpdateWithSameTime
        assert_eq!(records[1], ("2".to_string(), 1, 0)); // record2EarlierUpdate (last write)
    }

    /// Java: TestKeyBasedFileGroupRecordBuffer.readWithEventTimeOrdering
    ///
    /// Given: EVENT_TIME_ORDERING, ordering_field="ts"
    ///        base=[record1(k=1,c=1,ts=1), record2(k=2,c=1,ts=1), record3(k=3,c=1,ts=1)]
    /// When:  DataBlock=[record1_update(k=1,c=2,ts=1), record2_update(k=2,c=1,ts=2),
    ///                   record2_earlier(k=2,c=1,ts=0), record3_update(k=3,c=1,ts=2),
    ///                   record3_delete(k=3,c=3,ts=1)]
    /// Then:  Event-time: higher ts wins.
    ///        k=1: update ts=1 >= existing ts=1 → update wins
    ///        k=2: update ts=2 > ts=1, earlier ts=0 < ts=2 → update(ts=2) wins
    ///        k=3: update ts=2 > ts=1, delete ts=1 < ts=2 → update(ts=2) wins
    ///        Expected: 3 records (all updates survive), 0 deletes
    #[test]
    fn test_read_with_event_time_ordering() {
        let mut buffer = build_key_based_buffer("EVENT_TIME_ORDERING");

        let block = create_test_batch(&[
            ("1", 2, 1), // record1 update (same time → new wins)
            ("2", 1, 2), // record2 update (ts=2)
            ("2", 1, 0), // record2 earlier update (ts=0 < ts=2 → ignored)
            ("3", 1, 2), // record3 update (ts=2)
            ("3", 3, 1), // record3 "delete" (ts=1 < ts=2 → ignored)
        ]);
        buffer
            .process_data_block(&mut make_data_block(block, "instant1"))
            .unwrap();

        let base = create_test_batch(&[("1", 1, 1), ("2", 1, 1), ("3", 1, 1)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);

        assert_eq!(result.num_rows(), 3);
        assert_eq!(records[0], ("1".to_string(), 2, 1)); // updated
        assert_eq!(records[1], ("2".to_string(), 1, 2)); // update with ts=2 wins
        assert_eq!(records[2], ("3".to_string(), 1, 2)); // update with ts=2 wins over "delete"
    }

    // =========================================================================
    // Part E: Phase A/B/C tests from FS_logBlockConsumption.md
    // =========================================================================

    /// Phase A: Data record overwrites existing in commit-time mode.
    ///
    /// Given: Empty buffer, COMMIT_TIME_ORDERING
    /// When:  process_data_block with 2 records for same key: ("k", v1=1) then ("k", v2=2)
    /// Then:  records["k"] has v2 (last writer wins)
    #[test]
    fn test_phase_a_data_record_overwrites_existing() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // Two records for same key in one block
        let block = create_test_batch(&[("k", 1, 10), ("k", 2, 20)]);
        buffer
            .process_data_block(&mut make_data_block(block, "instant1"))
            .unwrap();

        assert_eq!(buffer.size(), 1);
        let record = buffer.base.records.get("k").unwrap().unwrap();
        // In commit-time: last writer wins → second record (counter=2, ts=20)
        assert!(!record.is_delete());
    }

    /// Phase B: Key exists in both base file and log map.
    ///
    /// Given: Base=[k1=(c=1,ts=1)], log records=[k1=(c=2,ts=2)]
    /// When:  merge_and_collect()
    /// Then:  Output has k1 with log values (log wins in final merge)
    #[test]
    fn test_phase_b_key_in_both_base_and_log() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        let log_block = create_test_batch(&[("k1", 2, 2)]);
        buffer
            .process_data_block(&mut make_data_block(log_block, "instant1"))
            .unwrap();

        let base = create_test_batch(&[("k1", 1, 1)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);

        assert_eq!(result.num_rows(), 1);
        assert_eq!(records[0], ("k1".to_string(), 2, 2)); // log wins
    }

    /// Phase B: Key only in base file (no matching log record).
    ///
    /// Given: Base=[k2=(c=1,ts=1)], log records={}
    /// When:  merge_and_collect()
    /// Then:  Output has k2 with base values (emitted as-is)
    #[test]
    fn test_phase_b_key_only_in_base() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");
        // No log records

        let base = create_test_batch(&[("k2", 1, 1)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);

        assert_eq!(result.num_rows(), 1);
        assert_eq!(records[0], ("k2".to_string(), 1, 1)); // base as-is
    }

    /// Phase B: Delete in log map removes base record.
    ///
    /// Given: Base=[k3=(c=1,ts=1)], log records=[k3=DELETE]
    /// When:  merge_and_collect()
    /// Then:  k3 NOT in output (deleted by log)
    #[test]
    fn test_phase_b_delete_in_log() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // Insert a delete record directly into the map
        buffer
            .base
            .records
            .insert(
                "k3".to_string(),
                BufferedRecord::new_delete("k3".to_string(), None),
            )
            .unwrap();

        let base = create_test_batch(&[("k3", 1, 1)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();

        // Delete record merged with base → delete wins → filtered out
        // (BufferedRecord with is_delete=true has data=None, so records_to_batch skips it)
        assert_eq!(result.num_rows(), 0);
    }

    /// Phase C: Log-only record emitted as INSERT.
    ///
    /// Given: Base=[], log records=[k4=(c=1,ts=1)]
    /// When:  merge_and_collect()
    /// Then:  Output has k4 as INSERT from log only
    #[test]
    fn test_phase_c_log_only_insert() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        let log_block = create_test_batch(&[("k4", 1, 1)]);
        buffer
            .process_data_block(&mut make_data_block(log_block, "instant1"))
            .unwrap();

        // No base file
        buffer.set_base_file_iterator(vec![]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);

        assert_eq!(result.num_rows(), 1);
        assert_eq!(records[0], ("k4".to_string(), 1, 1)); // insert from log
    }

    /// #76 review — the log-only drain releases its `log_drain_iter` (and thus the
    /// RocksDB handle + spill temp dir) as soon as the drain is exhausted, rather
    /// than pinning an exhausted iterator until the whole buffer is dropped.
    #[test]
    fn test_drain_log_only_releases_iter_on_exhaustion() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");
        buffer
            .process_data_block(&mut make_data_block(
                create_test_batch(&[("k1", 1, 1), ("k2", 2, 2)]),
                "instant1",
            ))
            .unwrap();
        let schema = create_test_schema();
        // First call drains all log-only inserts (2 rows < DEFAULT_BATCH_SIZE), so
        // the drain is exhausted within this call.
        let chunk = buffer.drain_log_only_inserts(&schema).unwrap();
        assert!(chunk.is_some(), "log-only inserts drained as a chunk");
        assert!(
            buffer.base.log_drain_iter.is_none(),
            "exhausted drain iterator must be released (RocksDB handle + temp dir freed), \
             not held as an exhausted Some(..)"
        );
        // A subsequent call is a clean Ok(None).
        assert!(buffer.drain_log_only_inserts(&schema).unwrap().is_none());
    }

    /// Phase C: Empty log map — base records pass through.
    ///
    /// Given: Base=[k1, k2], log records={}
    /// When:  merge_and_collect()
    /// Then:  Output has k1, k2 from base (no log merging)
    #[test]
    fn test_phase_c_empty_log_map() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");
        // No log records

        let base = create_test_batch(&[("k1", 1, 1), ("k2", 2, 2)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);

        assert_eq!(result.num_rows(), 2);
        assert_eq!(records[0], ("k1".to_string(), 1, 1));
        assert_eq!(records[1], ("k2".to_string(), 2, 2));
    }

    /// Phase C: Empty base — all log records are INSERTs.
    ///
    /// Given: Base=[], log records=[k1, k2]
    /// When:  merge_and_collect()
    /// Then:  Output has k1, k2 as INSERTs
    #[test]
    fn test_phase_c_empty_base() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        let log_block = create_test_batch(&[("k1", 1, 1), ("k2", 2, 2)]);
        buffer
            .process_data_block(&mut make_data_block(log_block, "instant1"))
            .unwrap();

        buffer.set_base_file_iterator(vec![]);

        let result = Box::new(buffer).merge_and_collect().unwrap();

        assert_eq!(result.num_rows(), 2);
    }

    /// Multiple writes to same key across blocks (commit-time ordering).
    ///
    /// Given: 3 blocks writing same key "k": v1(i1), v2(i2), then a third value
    /// When:  Full scan + merge
    /// Then:  records["k"] = last-written value
    #[test]
    fn test_multiple_writes_same_key_commit_time() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // Block 1: key "k" with counter=1
        let block1 = create_test_batch(&[("k", 1, 10)]);
        buffer
            .process_data_block(&mut make_data_block(block1, "i1"))
            .unwrap();

        // Block 2: same key with counter=2
        let block2 = create_test_batch(&[("k", 2, 20)]);
        buffer
            .process_data_block(&mut make_data_block(block2, "i2"))
            .unwrap();

        // Block 3: same key with counter=3
        let block3 = create_test_batch(&[("k", 3, 30)]);
        buffer
            .process_data_block(&mut make_data_block(block3, "i3"))
            .unwrap();

        assert_eq!(buffer.size(), 1);

        buffer.set_base_file_iterator(vec![]);
        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);

        assert_eq!(records[0], ("k".to_string(), 3, 30)); // last write wins
    }

    /// Mixed base + log + deletes scenario.
    ///
    /// Given: Base=[k1, k2, k3], log updates for k1 and k2, log delete for k3, log insert k4
    /// When:  merge_and_collect()
    /// Then:  k1=updated, k2=updated, k3=deleted(absent), k4=inserted
    #[test]
    fn test_mixed_base_log_delete_insert() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // Log: update k1, update k2
        let log_block = create_test_batch(&[("k1", 10, 100), ("k2", 20, 200)]);
        buffer
            .process_data_block(&mut make_data_block(log_block, "i1"))
            .unwrap();

        // Log: delete k3 (insert as delete record directly)
        buffer
            .base
            .records
            .insert(
                "k3".to_string(),
                BufferedRecord::new_delete("k3".to_string(), None),
            )
            .unwrap();

        // Log: insert k4 (new key not in base)
        let log_insert = create_test_batch(&[("k4", 40, 400)]);
        buffer
            .process_data_block(&mut make_data_block(log_insert, "i2"))
            .unwrap();

        // Base file
        let base = create_test_batch(&[("k1", 1, 1), ("k2", 2, 2), ("k3", 3, 3)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);

        // k1=updated, k2=updated, k3=deleted(absent), k4=inserted
        assert_eq!(result.num_rows(), 3);
        assert_eq!(records[0], ("k1".to_string(), 10, 100));
        assert_eq!(records[1], ("k2".to_string(), 20, 200));
        assert_eq!(records[2], ("k4".to_string(), 40, 400));
    }

    /// Stats wiring: a merge of known base + log records yields the expected
    /// num_inserts / num_updates / num_deletes via the update processor.
    ///
    /// Base=[k1, k2, k3]; log updates k1+k2 (2 updates), deletes k3 (1 delete),
    /// inserts k4 (1 insert). Mirrors gold StandardUpdateProcessor counting
    /// (UpdateProcessor.java:88-119): delete→numDeletes, prev present→numUpdates,
    /// prev absent→numInserts. A pure-base record with no log touch is NOT
    /// counted (it bypasses processUpdate — gold KeyBasedFileGroupRecordBuffer:248).
    #[test]
    fn test_merge_stats_counts_inserts_updates_deletes() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // Log: update k1, update k2.
        let log_block = create_test_batch(&[("k1", 10, 100), ("k2", 20, 200)]);
        buffer
            .process_data_block(&mut make_data_block(log_block, "i1"))
            .unwrap();

        // Log: delete k3.
        buffer
            .base
            .records
            .insert(
                "k3".to_string(),
                BufferedRecord::new_delete("k3".to_string(), None),
            )
            .unwrap();

        // Log: insert k4 (new key not in base).
        let log_insert = create_test_batch(&[("k4", 40, 400)]);
        buffer
            .process_data_block(&mut make_data_block(log_insert, "i2"))
            .unwrap();

        // Base file: k1, k2, k3.
        let base = create_test_batch(&[("k1", 1, 1), ("k2", 2, 2), ("k3", 3, 3)]);
        buffer.set_base_file_iterator(vec![base]);

        let (result, stats) = Box::new(buffer).merge_and_collect_with_stats().unwrap();

        assert_eq!(result.num_rows(), 3, "k1, k2 updated + k4 inserted");
        assert_eq!(stats.num_updates, 2, "k1 and k2 are base+log updates");
        assert_eq!(stats.num_deletes, 1, "k3 deleted");
        assert_eq!(stats.num_inserts, 1, "k4 is a log-only insert");
    }

    /// Process next data record increments total_log_records.
    #[test]
    fn test_process_next_data_record_increments_counter() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        let block = create_test_batch(&[("k1", 1, 1), ("k2", 2, 2)]);
        buffer
            .process_data_block(&mut make_data_block(block, "i1"))
            .unwrap();

        assert_eq!(buffer.get_total_log_records(), 2);
    }

    /// contains_log_record checks key existence.
    #[test]
    fn test_contains_log_record() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        let block = create_test_batch(&[("k1", 1, 1)]);
        buffer
            .process_data_block(&mut make_data_block(block, "i1"))
            .unwrap();

        assert!(buffer.contains_log_record("k1"));
        assert!(!buffer.contains_log_record("k2"));
    }

    /// Java: TestKeyBasedFileGroupRecordBuffer.readWithCommitTimeOrderingWithRecords
    ///
    /// COMMIT_TIME_ORDERING with pre-built records including deletes.
    /// In commit-time mode, log always wins over base.
    /// Expected: 5 output rows (keys 5,6 deleted).
    #[test]
    fn test_read_with_commit_time_ordering_with_records() {
        let mut buffer =
            build_key_based_buffer_with_delete_marker("COMMIT_TIME_ORDERING", "counter", "3");

        // Data records (log updates)
        let updates = create_test_batch(&[
            ("1", 2, 1), // record1UpdateWithSameTime
            ("2", 1, 2), // record2Update
            ("3", 1, 2), // record3Update
            ("4", 1, 0), // record4EarlierUpdate
            ("7", 1, 5), // record7 (log-only insert)
        ]);
        buffer
            .process_data_block(&mut make_data_block(updates, "instant2"))
            .unwrap();

        // Delete records with default ordering (0) for commit-time mode
        // In Java: convertToHoodieRecordsListForDeletes(..., true) → ordering=0 (default)
        let delete_5 = DeleteRecord {
            record_key: "5".to_string(),
            partition_path: String::new(),
            ordering_value: Some(OrderingValue::Long(0)),
        };
        buffer.process_next_deleted_record(delete_5, "5").unwrap();

        let delete_6 = DeleteRecord {
            record_key: "6".to_string(),
            partition_path: String::new(),
            ordering_value: Some(OrderingValue::Long(0)),
        };
        buffer.process_next_deleted_record(delete_6, "6").unwrap();

        // Base file: testRecord1-6
        let base = create_test_batch(&[
            ("1", 1, 1),
            ("2", 1, 1),
            ("3", 1, 1),
            ("4", 2, 1), // testRecord4 (counter=2)
            ("5", 1, 1),
            ("6", 1, 5), // testRecord6 (ts=5)
        ]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);

        // Commit-time: log always wins over base
        // key=1-4: log data wins → updated values
        // key=5,6: log delete wins → DELETED
        // key=7: log-only → INSERT
        // Java: assertEquals(Stream.of(testRecord1UpdateWithSameTime, testRecord2Update,
        //       testRecord3Update, testRecord4EarlierUpdate, testRecord7), actualRecords);
        assert_eq!(result.num_rows(), 5);
        assert_eq!(records[0], ("1".to_string(), 2, 1));
        assert_eq!(records[1], ("2".to_string(), 1, 2));
        assert_eq!(records[2], ("3".to_string(), 1, 2));
        assert_eq!(records[3], ("4".to_string(), 1, 0));
        assert_eq!(records[4], ("7".to_string(), 1, 5));
    }

    // =========================================================================
    // Java: TestKeyBasedFileGroupRecordBuffer.readWithEventTimeOrderingAndDeleteBlock
    // =========================================================================

    /// Java: readWithEventTimeOrderingAndDeleteBlock
    ///
    /// Given: EVENT_TIME_ORDERING, ordering_field="ts"
    ///        Base: records 1,2,3
    ///        DataBlock1: update key 1(ts=1), update key 2(ts=2), key 3(ts=1)
    ///        DeleteBlock: delete key 3(no ordering), delete key 2(ts=-1), delete key 1(ts=2)
    ///        DataBlock2: update key 2(ts=0, earlier), update key 3(ts=2)
    /// When:  Process all blocks, set base, merge
    /// Then:  After log scanning:
    ///        key 1: update(ts=1), then delete(ts=2) wins → DELETE
    ///        key 2: update(ts=2), then delete(ts=-1) loses, then earlier(ts=0) loses → keeps update(ts=2)
    ///        key 3: record(ts=1), then delete(no ordering) wins, then update(ts=2) wins → keeps update(ts=2)
    ///        Final: key1=deleted, key2=update(ts=2), key3=update(ts=2)
    ///        Output: 2 rows (key2, key3), 1 delete (key1 absent)
    #[test]
    fn test_read_with_event_time_ordering_and_delete_block() {
        let mut buffer = build_key_based_buffer("EVENT_TIME_ORDERING");

        // DataBlock1
        let block1 = create_test_batch(&[
            ("1", 2, 1), // key 1 update (ts=1)
            ("2", 2, 2), // key 2 update (ts=2)
            ("3", 1, 1), // key 3 (ts=1)
        ]);
        buffer
            .process_data_block(&mut make_data_block(block1, "instant1"))
            .unwrap();

        // DeleteBlock: delete key "1" with ts=2 (wins over existing ts=1)
        // For EVENT_TIME_ORDERING, delete with higher ts wins
        let delete_record_1 = DeleteRecord {
            record_key: "1".to_string(),
            partition_path: String::new(),
            ordering_value: Some(OrderingValue::Long(2)),
        };
        buffer
            .process_next_deleted_record(delete_record_1, "1")
            .unwrap();

        // Delete key "2" with ts=-1 (loses to existing ts=2)
        let delete_record_2 = DeleteRecord {
            record_key: "2".to_string(),
            partition_path: String::new(),
            ordering_value: Some(OrderingValue::Long(-1)),
        };
        buffer
            .process_next_deleted_record(delete_record_2, "2")
            .unwrap();

        // Delete key "3" with no ordering (wins by default)
        let delete_record_3 = DeleteRecord {
            record_key: "3".to_string(),
            partition_path: String::new(),
            ordering_value: None,
        };
        buffer
            .process_next_deleted_record(delete_record_3, "3")
            .unwrap();

        // DataBlock2
        let block2 = create_test_batch(&[
            ("2", 1, 0), // key 2 earlier update (ts=0, loses to ts=2)
            ("3", 1, 2), // key 3 update (ts=2, wins over delete with no ordering)
        ]);
        buffer
            .process_data_block(&mut make_data_block(block2, "instant2"))
            .unwrap();

        // Base file
        let base = create_test_batch(&[("1", 1, 1), ("2", 1, 1), ("3", 1, 1)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);

        // key1: deleted (absent), key2: update(ts=2), key3: update(ts=2)
        assert_eq!(result.num_rows(), 2, "Expected 2 rows: key1 deleted");
        assert_eq!(records[0], ("2".to_string(), 2, 2)); // key2 with ts=2 update
        assert_eq!(records[1], ("3".to_string(), 1, 2)); // key3 with ts=2 update
    }

    #[test]
    fn test_event_time_delete_block_rejects_lower_ordering_delete() {
        // ENG-38318 regression: a delete LOG BLOCK whose ordering value is LOWER
        // than the existing record must NOT remove the row under
        // EVENT_TIME_ORDERING. Drives process_delete_block (the real path) —
        // before the fix the ordering value was dropped (hardcoded None) so the
        // delete unconditionally won and the row silently vanished. Mirrors the
        // MERGE INTO ... DELETE case in TestMergeModeEventTimeOrdering (delete
        // id=2 @ ts=99 against an existing id=2 @ ts=100).
        let mut buffer = build_key_based_buffer("EVENT_TIME_ORDERING");

        // Existing records (log data block): keys 2,5,6 all at ts=100.
        let data = create_test_batch(&[("2", 20, 100), ("5", 50, 100), ("6", 60, 100)]);
        buffer
            .process_data_block(&mut make_data_block(data, "instant1"))
            .unwrap();

        // Delete block: key 2 @ ts=99 (lower → row survives),
        //               key 5 @ ts=100 (equal → deleted),
        //               key 6 @ ts=101 (higher → deleted).
        buffer
            .process_delete_block(&mut make_delete_block(
                &[("2", Some(99)), ("5", Some(100)), ("6", Some(101))],
                "instant2",
            ))
            .unwrap();

        buffer.set_base_file_iterator(vec![]);
        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);
        assert_eq!(
            result.num_rows(),
            1,
            "only key 2 survives: its ts=100 out-ranks the ts=99 delete"
        );
        assert_eq!(records[0], ("2".to_string(), 20, 100));
    }

    #[test]
    fn test_event_time_delete_block_vs_base_file_rejects_lower_ordering() {
        // Mirrors the REAL gluten scenario: records live in the BASE FILE, the
        // delete sits in a log block. The decisive comparison is base-vs-buffered
        // -delete via has_next_base_record (final_merge), NOT delta_merge_delete.
        // delete id=2 @ ts=99 must lose to base id=2 @ ts=100.
        let mut buffer = build_key_based_buffer("EVENT_TIME_ORDERING");

        // Delete block FIRST (base not yet seen → delete buffered with its ts).
        buffer
            .process_delete_block(&mut make_delete_block(
                &[("2", Some(99)), ("5", Some(100)), ("6", Some(101))],
                "instant1",
            ))
            .unwrap();

        // Base file: keys 2,5,6 all at ts=100.
        let base = create_test_batch(&[("2", 20, 100), ("5", 50, 100), ("6", 60, 100)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);
        assert_eq!(
            result.num_rows(),
            1,
            "only key 2 survives: base ts=100 out-ranks the ts=99 delete"
        );
        assert_eq!(records[0], ("2".to_string(), 20, 100));
    }

    #[test]
    fn test_event_time_default_ordering_delete_applies_against_higher_base() {
        // Regression for the global-index cross-partition-update gap (2026-06-15):
        // a GLOBAL index `update.partition.path=true` relocate writes a DELETE to
        // the OLD partition carrying orderingVal = HoodieRecord.DEFAULT_ORDERING_VALUE
        // (a Java `int 0`, serialized via `IntWrapper` → Arrow `Int32`, "natural
        // order"). Such a delete must ALWAYS apply even though the base row carries a
        // higher ordering value — mirrors Java
        // `BufferedRecordMergerFactory.deltaMergeDeleteRecord` (the `!isDefault`
        // short-circuit). An `Int32` `0` decodes to `OrderingValue::Default` (GAP-2:
        // only the `Integer(0)` default, not a genuine `bigint 0`, is natural-order).
        let mut buffer = build_key_based_buffer("EVENT_TIME_ORDERING");

        // Delete block: key "1" @ orderingVal=Int32(0) (the Integer default).
        buffer
            .process_delete_block(&mut make_delete_block_i32(&[("1", Some(0))], "instant2"))
            .unwrap();

        // Base file: key "1" @ ts=1000 (higher than the delete), key "2" untouched.
        let base = create_test_batch(&[("1", 11, 1000), ("2", 22, 1000)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);
        assert_eq!(
            result.num_rows(),
            1,
            "key 1 must be deleted: a default(0)-ordering delete always applies"
        );
        assert_eq!(records[0], ("2".to_string(), 22, 1000));
    }

    /// GAP-2: EVENT_TIME, a delete carrying a GENUINE `bigint` ordering value of `0`
    /// (`Int64` → `Long(0)`, NOT the natural-order default) against a base row with a
    /// higher ordering value → the delete is STALE and the row is KEPT. Mirrors Java
    /// `OrderingValues.isDefault` == `Integer(0).equals(Long(0))` == `false`.
    ///
    /// Discriminating: before the fix `Long(0)` was treated as the default and the
    /// delete applied unconditionally, wrongly removing a live row whose event-time
    /// (100) is newer than the delete's (0). Contrast
    /// `test_event_time_default_ordering_delete_applies_against_higher_base`, where an
    /// `Int32(0)` (the real `Integer` default) DOES apply.
    #[test]
    fn test_event_time_genuine_bigint_zero_delete_is_stale_row_kept() {
        let mut buffer = build_key_based_buffer("EVENT_TIME_ORDERING");
        // Delete key "1" @ genuine bigint orderingVal=0 (Int64 → Long(0)).
        buffer
            .process_delete_block(&mut make_delete_block(&[("1", Some(0))], "instant2"))
            .unwrap();
        let base = create_test_batch(&[("1", 11, 100), ("2", 22, 100)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);
        assert_eq!(
            result.num_rows(),
            2,
            "GAP-2: a genuine bigint-0 delete (< base ts=100) is stale; the row is kept"
        );
        assert_eq!(records[0], ("1".to_string(), 11, 100));
        assert_eq!(records[1], ("2".to_string(), 22, 100));
    }

    /// EVENT_TIME, NON-default delete that is STALE (base ordering strictly
    /// exceeds it, same class) → the delete is obsolete, the base row is kept.
    /// This is the one branch where a delete loses; it must not over-delete.
    #[test]
    fn test_event_time_stale_nondefault_delete_is_obsolete_base_kept() {
        let mut buffer = build_key_based_buffer("EVENT_TIME_ORDERING");
        // Delete key "1" @ orderingVal=5 (non-default but stale vs base ts=1000).
        buffer
            .process_delete_block(&mut make_delete_block(&[("1", Some(5))], "instant2"))
            .unwrap();
        let base = create_test_batch(&[("1", 11, 1000), ("2", 22, 1000)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);
        assert_eq!(
            result.num_rows(),
            2,
            "stale non-default delete (5 < base 1000) must NOT delete the base row"
        );
        assert_eq!(records[0], ("1".to_string(), 11, 1000));
        assert_eq!(records[1], ("2".to_string(), 22, 1000));
    }

    /// EVENT_TIME, NON-default delete that is NEWER than the base (delete
    /// ordering strictly exceeds base) → the delete applies.
    #[test]
    fn test_event_time_newer_nondefault_delete_applies() {
        let mut buffer = build_key_based_buffer("EVENT_TIME_ORDERING");
        // Delete key "1" @ orderingVal=2000 (newer than base ts=1000).
        buffer
            .process_delete_block(&mut make_delete_block(&[("1", Some(2000))], "instant2"))
            .unwrap();
        let base = create_test_batch(&[("1", 11, 1000), ("2", 22, 1000)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);
        assert_eq!(
            result.num_rows(),
            1,
            "newer non-default delete (2000 > base 1000) must delete the base row"
        );
        assert_eq!(records[0], ("2".to_string(), 22, 1000));
    }

    /// EVENT_TIME, delete with ABSENT (None) ordering → the `(_, _)` arm of the
    /// obsolescence test is `false`, so the delete always applies (matches the
    /// default-ordering short-circuit: a delete with no ordering is not stale).
    #[test]
    fn test_event_time_none_ordering_delete_applies() {
        let mut buffer = build_key_based_buffer("EVENT_TIME_ORDERING");
        // Delete key "1" with no orderingVal (None).
        buffer
            .process_delete_block(&mut make_delete_block(&[("1", None)], "instant2"))
            .unwrap();
        let base = create_test_batch(&[("1", 11, 1000), ("2", 22, 1000)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);
        assert_eq!(
            result.num_rows(),
            1,
            "a delete with absent ordering is never stale → must apply"
        );
        assert_eq!(records[0], ("2".to_string(), 22, 1000));
    }

    /// COMMIT_TIME_ORDERING: a delete ALWAYS applies regardless of ordering —
    /// even a stale non-default one that would be obsolete under EVENT_TIME.
    #[test]
    fn test_commit_time_delete_always_applies_regardless_of_ordering() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");
        // orderingVal=5 would be "stale" vs base 1000 under EVENT_TIME, but
        // COMMIT_TIME ignores ordering entirely for deletes.
        buffer
            .process_delete_block(&mut make_delete_block(&[("1", Some(5))], "instant2"))
            .unwrap();
        let base = create_test_batch(&[("1", 11, 1000), ("2", 22, 1000)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);
        assert_eq!(
            result.num_rows(),
            1,
            "COMMIT_TIME delete must apply regardless of ordering value"
        );
        assert_eq!(records[0], ("2".to_string(), 22, 1000));
    }

    // =========================================================================
    // Running Map Pattern Tests (from FS_logBlockConsumption.md)
    //
    // Validates that the records map is a running accumulator: every record
    // from every log block immediately mutates it in-place.
    // =========================================================================

    /// Full chain: single record → map update.
    ///
    /// Given: Empty buffer, COMMIT_TIME_ORDERING
    /// When:  process_data_block with 1 record (key="k1", counter=1, ts=10)
    /// Then:  records map contains exactly {"k1": BufferedRecord}
    ///
    /// Validates: processDataBlock → processNextDataRecord → records.put(key, record)
    #[test]
    fn test_running_map_single_record_updates_map() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        assert!(buffer.base.records.is_empty(), "Map should start empty");

        let block = create_test_batch(&[("k1", 1, 10)]);
        buffer
            .process_data_block(&mut make_data_block(block, "instant1"))
            .unwrap();

        // Map now has exactly 1 entry
        assert_eq!(buffer.base.records.len(), 1);
        assert!(buffer.base.records.contains_key("k1").unwrap());
        assert_eq!(buffer.base.total_log_records, 1);
    }

    /// Running map: second record for same key overwrites first inline.
    ///
    /// Given: Buffer with k1=(counter=1,ts=10) already in map
    /// When:  process_data_block with (key="k1", counter=2, ts=20)
    /// Then:  Map still has 1 entry, but value is updated
    ///
    /// Validates: records.get(key) → deltaMerge(new, existing) → records.put(key, merged)
    /// For COMMIT_TIME_ORDERING: new always wins regardless of ordering value.
    #[test]
    fn test_running_map_same_key_overwrites_inline() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // First record
        let block1 = create_test_batch(&[("k1", 1, 10)]);
        buffer
            .process_data_block(&mut make_data_block(block1, "i1"))
            .unwrap();
        assert_eq!(buffer.base.records.len(), 1);

        // Second record for same key — overwrites
        let block2 = create_test_batch(&[("k1", 2, 20)]);
        buffer
            .process_data_block(&mut make_data_block(block2, "i2"))
            .unwrap();

        // Still 1 entry (same key), but value updated
        assert_eq!(buffer.base.records.len(), 1);
        assert_eq!(buffer.base.total_log_records, 2);
    }

    /// Running map: accumulates across multiple blocks from different instants.
    ///
    /// Given: 3 blocks from instants i1, i2, i3 (processed oldest→newest)
    ///        Block i1: key=K with value v1
    ///        Block i2: key=K with value v2  (overwrites v1)
    ///        Block i3: key=K with value v3  (overwrites v2)
    /// When:  Process blocks in order
    /// Then:  Final map: {"K": v3} (last writer wins)
    ///        total_log_records = 3 (all processed, even if overwritten)
    ///
    /// This validates the "running accumulator" pattern from the reference:
    ///   deltaMerge(t1_record, null)        → records["K"] = t1_record
    ///   deltaMerge(t2_record, t1_record)   → records["K"] = t2_record
    ///   deltaMerge(t3_record, t2_record)   → records["K"] = t3_record
    #[test]
    fn test_running_map_across_three_instants() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // Simulate oldest→newest processing order (as pollLast produces)
        let block_i1 = create_test_batch(&[("K", 1, 10)]); // oldest
        buffer
            .process_data_block(&mut make_data_block(block_i1, "i1"))
            .unwrap();
        assert_eq!(buffer.base.records.len(), 1);

        let block_i2 = create_test_batch(&[("K", 2, 20)]);
        buffer
            .process_data_block(&mut make_data_block(block_i2, "i2"))
            .unwrap();
        assert_eq!(buffer.base.records.len(), 1); // still 1 key

        let block_i3 = create_test_batch(&[("K", 3, 30)]); // newest
        buffer
            .process_data_block(&mut make_data_block(block_i3, "i3"))
            .unwrap();
        assert_eq!(buffer.base.records.len(), 1);

        // total_log_records counts ALL records processed, not just unique keys
        assert_eq!(buffer.base.total_log_records, 3);

        // Verify final map state: newest wins
        buffer.set_base_file_iterator(vec![]);
        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);
        assert_eq!(records[0], ("K".to_string(), 3, 30)); // i3 value
    }

    /// Running map: delete overwrites existing data record.
    ///
    /// Given: Buffer with k1=(counter=1,ts=10) in map
    /// When:  process_next_deleted_record for key "k1"
    /// Then:  Map entry for k1 is now a delete (is_delete=true)
    ///        When merged with base, k1 is absent from output.
    #[test]
    fn test_running_map_delete_overwrites_data() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // Data record first
        let block = create_test_batch(&[("k1", 1, 10)]);
        buffer
            .process_data_block(&mut make_data_block(block, "i1"))
            .unwrap();
        assert!(!buffer.base.records.get("k1").unwrap().unwrap().is_delete());

        // Delete for same key
        let delete = DeleteRecord {
            record_key: "k1".to_string(),
            partition_path: String::new(),
            ordering_value: None,
        };
        buffer.process_next_deleted_record(delete, "k1").unwrap();

        // Map entry is now a delete
        assert!(buffer.base.records.get("k1").unwrap().unwrap().is_delete());
        assert_eq!(buffer.base.total_log_records, 2);

        // When merged with base, k1 should be absent
        let base = create_test_batch(&[("k1", 1, 1)]);
        buffer.set_base_file_iterator(vec![base]);
        let result = Box::new(buffer).merge_and_collect().unwrap();
        assert_eq!(result.num_rows(), 0, "Deleted key should be absent");
    }

    /// Full Phase A→B→C chain: 3 blocks, 2 keys, 1 delete.
    ///
    /// Given: COMMIT_TIME_ORDERING
    ///   Block i1 (oldest): key=A value=v1, key=B value=v1
    ///   Block i2: key=A value=v2 (overwrites)
    ///   Block i3 (newest): delete key=B
    ///   Base file: key=A base_val, key=B base_val, key=C base_val
    /// When:  Full scan + merge
    /// Then:  Phase A map: {A: v2, B: DELETE}
    ///        Phase B: A → merge(base, v2) → emit v2
    ///                 B → merge(base, DELETE) → absent
    ///                 C → no log entry → emit base
    ///        Phase C: nothing remaining
    ///        Output: A=v2, C=base_val (2 rows)
    #[test]
    fn test_full_chain_phase_a_b_c() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // Phase A: process log blocks oldest→newest
        let block_i1 = create_test_batch(&[("A", 1, 10), ("B", 1, 10)]);
        buffer
            .process_data_block(&mut make_data_block(block_i1, "i1"))
            .unwrap();

        let block_i2 = create_test_batch(&[("A", 2, 20)]);
        buffer
            .process_data_block(&mut make_data_block(block_i2, "i2"))
            .unwrap();

        // Delete B
        let delete_b = DeleteRecord {
            record_key: "B".to_string(),
            partition_path: String::new(),
            ordering_value: None,
        };
        buffer.process_next_deleted_record(delete_b, "B").unwrap();

        // Verify Phase A map state
        assert_eq!(buffer.base.records.len(), 2); // A and B
        assert!(!buffer.base.records.get("A").unwrap().unwrap().is_delete());
        assert!(buffer.base.records.get("B").unwrap().unwrap().is_delete());

        // Phase B + C
        let base = create_test_batch(&[("A", 0, 0), ("B", 0, 0), ("C", 9, 99)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);

        assert_eq!(result.num_rows(), 2, "A=updated, B=deleted, C=base");
        assert_eq!(records[0], ("A".to_string(), 2, 20)); // log wins
        assert_eq!(records[1], ("C".to_string(), 9, 99)); // base passthrough
    }

    // =========================================================================
    // Commit-time merge semantics (UT parity P3 — Task 6 / D-P3-4).
    //
    // The gold commit-time cases (TestKeyBasedFileGroupRecordBuffer
    // .readWithCommitTimeOrdering[WithRecords]) are @Disabled there because they
    // exercise custom delete payloads. Their commit-time-relevant invariants —
    // last-block-wins, delete/reinsert interleaving across blocks — are NOT
    // payload-specific. These tests mirror those invariants at the unit level
    // under the supported matrix (v9, COMMIT_TIME_ORDERING). Last-writer-wins is
    // CommitTimeRecordMerger::delta_merge returning the new record unconditionally
    // and delta_merge_delete returning the delete unconditionally (record_merger.rs).
    // =========================================================================

    /// Commit-time: a delete in an earlier block, then a data record (reinsert)
    /// for the SAME key in a later block → the reinsert wins (record present).
    ///
    /// Gold parity: commit-time `deltaMerge(newRecord, existing)` ignores the
    /// existing record entirely (last block wins), so a data record processed
    /// after a delete resurrects the key. This is the cross-block delete→insert
    /// interleaving that the disabled gold cases assert structurally.
    #[test]
    fn test_commit_time_delete_then_reinsert_across_blocks() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // Block i1: insert key "k".
        let block1 = create_test_batch(&[("k", 1, 10)]);
        buffer
            .process_data_block(&mut make_data_block(block1, "i1"))
            .unwrap();

        // Delete "k" (processed between blocks, as a delete block at i2).
        let delete = DeleteRecord {
            record_key: "k".to_string(),
            partition_path: String::new(),
            ordering_value: None,
        };
        buffer.process_next_deleted_record(delete, "k").unwrap();
        assert!(
            buffer.base.records.get("k").unwrap().unwrap().is_delete(),
            "after delete the running-map entry must be a delete"
        );

        // Block i3 (latest): reinsert "k" with a new value.
        let block3 = create_test_batch(&[("k", 9, 90)]);
        buffer
            .process_data_block(&mut make_data_block(block3, "i3"))
            .unwrap();
        assert!(
            !buffer.base.records.get("k").unwrap().unwrap().is_delete(),
            "the later data block must overwrite the delete (last block wins)"
        );

        // Base file has the original "k". The reinsert (log) wins over base.
        let base = create_test_batch(&[("k", 0, 0)]);
        buffer.set_base_file_iterator(vec![base]);
        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);

        assert_eq!(
            result.num_rows(),
            1,
            "key reinserted after delete must survive"
        );
        assert_eq!(records[0], ("k".to_string(), 9, 90));
    }

    /// Commit-time: a delete for a key that exists in NEITHER base nor any log
    /// data block is a no-op for output — no error, the key is simply absent and
    /// no surviving record is emitted.
    ///
    /// Gold parity: `processNextDeletedRecord` always records the delete in the
    /// running map (commit-time delete wins), but with no matching base or log
    /// data record the tombstone produces no output row.
    #[test]
    fn test_commit_time_delete_of_nonexistent_key() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // One real key in a data block.
        let block = create_test_batch(&[("present", 1, 1)]);
        buffer
            .process_data_block(&mut make_data_block(block, "i1"))
            .unwrap();

        // Delete a key that was never written.
        let delete = DeleteRecord {
            record_key: "ghost".to_string(),
            partition_path: String::new(),
            ordering_value: None,
        };
        buffer.process_next_deleted_record(delete, "ghost").unwrap();

        // The tombstone is recorded but matches no base/log data record.
        assert!(
            buffer
                .base
                .records
                .get("ghost")
                .unwrap()
                .unwrap()
                .is_delete()
        );

        let base = create_test_batch(&[("present", 0, 0)]);
        buffer.set_base_file_iterator(vec![base]);
        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);

        assert_eq!(
            result.num_rows(),
            1,
            "only the real key survives; the ghost delete emits nothing"
        );
        assert_eq!(records[0], ("present".to_string(), 1, 1));
    }

    /// Commit-time: a delete in an earlier block, then an update for the SAME key
    /// in a later block → the update wins ACROSS blocks (last block wins),
    /// contrasted with delete-after-update (test_running_map_delete_overwrites_data)
    /// where the later delete wins. Both directions confirm strict last-block-wins.
    #[test]
    fn test_commit_time_update_after_delete_across_blocks() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // Earlier block: delete "k".
        let delete = DeleteRecord {
            record_key: "k".to_string(),
            partition_path: String::new(),
            ordering_value: None,
        };
        buffer.process_next_deleted_record(delete, "k").unwrap();
        assert!(buffer.base.records.get("k").unwrap().unwrap().is_delete());

        // Later block: update "k" — must overwrite the delete.
        let block = create_test_batch(&[("k", 7, 70)]);
        buffer
            .process_data_block(&mut make_data_block(block, "i2"))
            .unwrap();
        assert!(
            !buffer.base.records.get("k").unwrap().unwrap().is_delete(),
            "later update must overwrite earlier delete"
        );

        buffer.set_base_file_iterator(vec![]);
        let result = Box::new(buffer).merge_and_collect().unwrap();
        let records = extract_records(&result);

        assert_eq!(result.num_rows(), 1);
        assert_eq!(records[0], ("k".to_string(), 7, 70));
    }

    // =========================================================================
    // ENG-42991 — streaming output via FileGroupMergeIterator.
    //
    // These tests compare the streaming iterator's output against the
    // legacy `merge_and_collect` path on the same buffer fixture. The
    // contract: total rows and per-row contents must match; chunk count
    // must respect `batch_size`. Per codeQuality/guide.md §5 they assert the
    // full output data (sorted by key), not just row counts.
    // =========================================================================

    use crate::file_group::reader_v2::merge_iterator::{
        FileGroupMergeIterator, new_stream_stats_handle,
    };

    /// Test shim: build a streaming iterator with a throwaway stats handle.
    /// The vectorized base-merge path (ENG-43009) drives chunking off the base
    /// source's cadence and ignores `batch_size`; the `stream_stats` handle is a
    /// production concern (FFI timing/update-count reporting) the streaming-parity
    /// tests don't assert on, so it's defaulted here.
    fn new_buffered_test(
        buffer: Box<dyn crate::file_group::reader_v2::buffer::HoodieFileGroupRecordBuffer>,
        merge_schema: SchemaRef,
        output_schema: SchemaRef,
        output_converter: Option<
            Box<dyn crate::file_group::reader_v2::output_converter::OutputConverter>,
        >,
        batch_size: usize,
    ) -> FileGroupMergeIterator {
        FileGroupMergeIterator::new_buffered(
            buffer,
            merge_schema,
            output_schema,
            output_converter,
            batch_size,
            new_stream_stats_handle(),
        )
    }

    /// Build the same buffer state used by `test_read_with_commit_time_ordering`
    /// — three keys after a base + two log blocks with deletes. After merge the
    /// surviving output is key "1" (base+log) and key "2" (delete marker for the
    /// log makes it a passthrough of base value 1 / log value 0); key "3" is the
    /// reinserted record.
    fn build_commit_time_ordering_fixture() -> (KeyBasedFileGroupRecordBuffer, SchemaRef) {
        let mut buffer =
            build_key_based_buffer_with_delete_marker("COMMIT_TIME_ORDERING", "counter", "3");
        let block1 = create_test_batch(&[("1", 2, 1), ("2", 1, 2), ("2", 1, 0)]);
        buffer
            .process_data_block(&mut make_data_block(block1, "instant1"))
            .unwrap();
        let block2 = create_test_batch(&[("2", 1, 0), ("3", 1, 2), ("3", 3, 1)]);
        buffer
            .process_data_block(&mut make_data_block(block2, "instant2"))
            .unwrap();
        let base = create_test_batch(&[("1", 1, 1), ("2", 1, 1), ("3", 1, 1)]);
        buffer.set_base_file_iterator(vec![base]);
        let schema = create_test_schema();
        (buffer, schema)
    }

    /// The legacy single-batch output for the fixture, sorted by key — the
    /// reference every streaming variant must reproduce exactly.
    fn legacy_records_sorted() -> Vec<(String, i32, i64)> {
        let (buffer, _) = build_commit_time_ordering_fixture();
        let legacy = Box::new(buffer).merge_and_collect().unwrap();
        let mut recs = extract_records(&legacy);
        recs.sort_by(|a, b| a.0.cmp(&b.0));
        recs
    }

    fn collect_stream(
        buffer: KeyBasedFileGroupRecordBuffer,
        schema: SchemaRef,
        batch_size: usize,
    ) -> Vec<RecordBatch> {
        let it = FileGroupMergeIterator::new_buffered(
            Box::new(buffer),
            schema.clone(),
            schema,
            None,
            batch_size,
            new_stream_stats_handle(),
        );
        it.map(|r| r.unwrap()).collect()
    }

    /// Streaming with batch_size larger than the merged-row count → one chunk;
    /// full data matches the legacy single-batch output.
    #[test]
    fn stream_single_chunk_matches_legacy() {
        let (buffer, schema) = build_commit_time_ordering_fixture();
        let chunks = collect_stream(buffer, schema.clone(), 4096);
        assert_eq!(chunks.len(), 1, "should fit in one chunk");
        let chunk_refs: Vec<&RecordBatch> = chunks.iter().collect();
        let concat = arrow::compute::concat_batches(&schema, chunk_refs).unwrap();
        let mut recs = extract_records(&concat);
        recs.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(recs, legacy_records_sorted());
    }

    /// Streaming with a small `batch_size` hint → concatenation equals the
    /// legacy result row-for-row. (ENG-43009 vectorized: chunk cadence follows
    /// the base source, not `batch_size`, so assert total-row + content parity
    /// rather than one-row-per-chunk.)
    #[test]
    fn stream_one_row_per_chunk_matches_legacy() {
        let (buffer, schema) = build_commit_time_ordering_fixture();
        let legacy = legacy_records_sorted();
        let chunks = collect_stream(buffer, schema.clone(), 1);
        let total_rows: usize = chunks.iter().map(|c| c.num_rows()).sum();
        assert_eq!(
            total_rows,
            legacy.len(),
            "all output rows emitted across chunks"
        );
        let chunk_refs: Vec<&RecordBatch> = chunks.iter().collect();
        let concat = arrow::compute::concat_batches(&schema, chunk_refs).unwrap();
        let mut recs = extract_records(&concat);
        recs.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            recs, legacy,
            "chunk-of-1 output must equal legacy row-for-row"
        );
    }

    /// Buffered chunking with batch_size = 2: row counts per chunk plus a
    /// trailing partial chunk, and the concatenation equals legacy.
    #[test]
    fn stream_buffered_chunking_row_counts_and_partial_tail() {
        let (buffer, schema) = build_commit_time_ordering_fixture();
        let legacy = legacy_records_sorted();
        let total = legacy.len();
        let chunks = collect_stream(buffer, schema.clone(), 2);

        // Expected chunk shape: ceil(total / 2) chunks; all but the last hold
        // exactly 2 rows; the last holds total % 2 (or 2 if it divides evenly).
        let expected_chunks = total.div_ceil(2);
        assert_eq!(chunks.len(), expected_chunks, "chunk count = ceil(total/2)");
        let summed: usize = chunks.iter().map(|c| c.num_rows()).sum();
        assert_eq!(summed, total, "no rows dropped across chunks");
        for c in &chunks[..chunks.len() - 1] {
            assert_eq!(c.num_rows(), 2, "non-final chunks are full (2 rows)");
        }
        let last = chunks.last().unwrap().num_rows();
        let expected_last = if total.is_multiple_of(2) {
            2
        } else {
            total % 2
        };
        assert_eq!(last, expected_last, "final chunk is the partial remainder");

        let chunk_refs: Vec<&RecordBatch> = chunks.iter().collect();
        let concat = arrow::compute::concat_batches(&schema, chunk_refs).unwrap();
        let mut recs = extract_records(&concat);
        recs.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(recs, legacy);
    }

    /// A3 (ENG-42992) — the production chunked streaming iterator
    /// (`FileGroupMergeIterator::Buffered`) driving a base source split across
    /// MULTIPLE row-groups produces output byte-identical to the eager single
    /// base-batch fixture, at every chunk size. This is the end-to-end proof
    /// that the streamed base (decoded row-group-at-a-time, fed into A2
    /// BatchRefs) merges identically to the old collect+concat base — covering
    /// the streamed-base × BatchRef interaction through the real drain path.
    #[test]
    fn stream_multi_row_group_base_matches_eager_across_chunk_sizes() {
        let legacy = legacy_records_sorted();

        // Build the same log state as the eager fixture, but hand the base file
        // in as THREE row-groups via set_base_file_source (forces next_base_row
        // to pull a 2nd and 3rd batch mid-merge). Same logical base rows as the
        // eager fixture's single batch [("1",1,1),("2",1,1),("3",1,1)].
        let build_multi_rg_buffer = || {
            let mut buffer =
                build_key_based_buffer_with_delete_marker("COMMIT_TIME_ORDERING", "counter", "3");
            buffer
                .process_data_block(&mut make_data_block(
                    create_test_batch(&[("1", 2, 1), ("2", 1, 2), ("2", 1, 0)]),
                    "instant1",
                ))
                .unwrap();
            buffer
                .process_data_block(&mut make_data_block(
                    create_test_batch(&[("2", 1, 0), ("3", 1, 2), ("3", 3, 1)]),
                    "instant2",
                ))
                .unwrap();
            let schema = create_test_schema();
            let reader = arrow_array::RecordBatchIterator::new(
                vec![
                    Ok(create_test_batch(&[("1", 1, 1)])),
                    Ok(create_test_batch(&[("2", 1, 1)])),
                    Ok(create_test_batch(&[("3", 1, 1)])),
                ]
                .into_iter(),
                schema,
            );
            buffer.set_base_file_source(Box::new(reader));
            buffer
        };

        for bs in [1usize, 2, 3, 4096] {
            let schema = create_test_schema();
            let chunks = collect_stream(build_multi_rg_buffer(), schema.clone(), bs);
            let total: usize = chunks.iter().map(|c| c.num_rows()).sum();
            assert_eq!(total, legacy.len(), "no rows dropped at batch_size={bs}");
            let chunk_refs: Vec<&RecordBatch> = chunks.iter().collect();
            let concat = arrow::compute::concat_batches(&schema, chunk_refs).unwrap();
            let mut recs = extract_records(&concat);
            recs.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(
                recs, legacy,
                "multi-row-group streamed base must equal eager output at batch_size={bs}"
            );
        }
    }

    /// Empty file group (no base, no log records) → the stream yields no chunks.
    #[test]
    fn stream_empty_file_group_yields_no_chunks() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");
        buffer.set_base_file_iterator(vec![]);
        let schema = create_test_schema();
        let chunks = collect_stream(buffer, schema, 4096);
        assert!(chunks.is_empty(), "empty FG emits zero chunks");
    }

    /// Deletes-only: a delete tombstone with no surviving base/log data record
    /// → no output rows, matching `merge_and_collect`.
    #[test]
    fn stream_deletes_only_yields_no_rows() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");
        let delete = DeleteRecord {
            record_key: "gone".to_string(),
            partition_path: String::new(),
            ordering_value: None,
        };
        buffer.process_next_deleted_record(delete, "gone").unwrap();
        buffer.set_base_file_iterator(vec![]);
        let schema = create_test_schema();

        // Legacy reference.
        let mut legacy_buf = build_key_based_buffer("COMMIT_TIME_ORDERING");
        let del2 = DeleteRecord {
            record_key: "gone".to_string(),
            partition_path: String::new(),
            ordering_value: None,
        };
        legacy_buf
            .process_next_deleted_record(del2, "gone")
            .unwrap();
        legacy_buf.set_base_file_iterator(vec![]);
        let legacy = Box::new(legacy_buf).merge_and_collect().unwrap();
        assert_eq!(legacy.num_rows(), 0, "legacy deletes-only output is empty");

        let chunks = collect_stream(buffer, schema, 4096);
        let rows: usize = chunks.iter().map(|c| c.num_rows()).sum();
        assert_eq!(rows, 0, "deletes-only stream emits no rows");
    }

    /// The Buffered stream drains the update-processor insert/update/delete
    /// counts into the shared stats sink once exhausted (ENG-42991).
    #[test]
    fn stream_snapshots_update_stats_on_exhaustion() {
        let (buffer, schema) = build_commit_time_ordering_fixture();
        let stats = new_stream_stats_handle();
        let it = FileGroupMergeIterator::new_buffered(
            Box::new(buffer),
            schema.clone(),
            schema,
            None,
            4096,
            stats.clone(),
        );
        let chunks: Vec<RecordBatch> = it.map(|r| r.unwrap()).collect();
        assert_eq!(chunks.len(), 1, "should fit in one chunk");
        let records = extract_records(&chunks[0]);
        assert_eq!(chunks[0].num_rows(), 2);
        assert_eq!(records[0], ("1".to_string(), 2, 1));
        assert_eq!(records[1], ("2".to_string(), 1, 0));
    }

    /// ENG-43009 vectorized — chunk count is now determined by the base source's
    /// cadence (one chunk per non-empty source batch), not by a per-row
    /// batch_size hint. This test now verifies row-content parity with the
    /// legacy `merge_and_collect` path regardless of chunk count.
    #[test]
    fn stream_matches_legacy_row_content() {
        let (buffer1, schema1) = build_commit_time_ordering_fixture();
        let it = new_buffered_test(
            Box::new(buffer1),
            schema1.clone(),
            schema1.clone(),
            None,
            1, // batch_size ignored on the new path
        );
        let chunks: Vec<RecordBatch> = it.map(|r| r.unwrap()).collect();
        let total_rows: usize = chunks.iter().map(|c| c.num_rows()).sum();
        assert_eq!(total_rows, 2, "two output rows total");

        // Concatenate chunks and verify they equal the legacy result row-for-row.
        let (buffer2, _) = build_commit_time_ordering_fixture();
        let legacy = Box::new(buffer2).merge_and_collect().unwrap();
        let chunk_refs: Vec<&RecordBatch> = chunks.iter().collect();
        let concat = arrow::compute::concat_batches(&schema1, chunk_refs).unwrap();
        assert_eq!(extract_records(&concat), extract_records(&legacy));
    }

    /// Streaming `RecordBatchReader::schema()` reports the constructor-time
    /// output schema (i.e. matches what the FFI side will see before any
    /// `get_next` call).
    #[test]
    fn stream_schema_is_constructor_schema() {
        let (buffer, schema) = build_commit_time_ordering_fixture();
        let it = FileGroupMergeIterator::new_buffered(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            1,
            new_stream_stats_handle(),
        );
        use arrow_array::RecordBatchReader as _;
        assert_eq!(it.schema(), schema);
    }

    /// ENG-42992 — lazy base source: instead of `set_base_file_iterator(vec![...])`
    /// (which the default-impl wraps in a RecordBatchIterator), call
    /// `set_base_file_source` directly with a multi-batch
    /// `RecordBatchIterator`. The merge result must be row-for-row
    /// identical to the eager Vec input.
    #[test]
    fn lazy_base_source_matches_eager_vec() {
        // Eager reference run.
        let (eager_buffer, schema) = build_commit_time_ordering_fixture();
        let eager_result = Box::new(eager_buffer).merge_and_collect().unwrap();
        let eager_records = extract_records(&eager_result);

        // Lazy run — same blocks, but base file split across TWO batches
        // and handed to the buffer via the new `set_base_file_source`
        // entry point. Forces `next_base_row` to pull a second batch
        // mid-merge.
        let mut lazy_buffer =
            build_key_based_buffer_with_delete_marker("COMMIT_TIME_ORDERING", "counter", "3");
        lazy_buffer
            .process_data_block(&mut make_data_block(
                create_test_batch(&[("1", 2, 1), ("2", 1, 2), ("2", 1, 0)]),
                "instant1",
            ))
            .unwrap();
        lazy_buffer
            .process_data_block(&mut make_data_block(
                create_test_batch(&[("2", 1, 0), ("3", 1, 2), ("3", 3, 1)]),
                "instant2",
            ))
            .unwrap();

        // Split the base file across two RecordBatches → tests the
        // multi-batch source path through next_base_row.
        let base1 = create_test_batch(&[("1", 1, 1), ("2", 1, 1)]);
        let base2 = create_test_batch(&[("3", 1, 1)]);
        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(base1), Ok(base2)].into_iter(),
            schema.clone(),
        );
        lazy_buffer.set_base_file_source(Box::new(reader));

        let lazy_result = Box::new(lazy_buffer).merge_and_collect().unwrap();
        let lazy_records = extract_records(&lazy_result);

        assert_eq!(lazy_records, eager_records);
    }

    /// ENG-42992 — a base source error mid-stream PROPAGATES as an `Err` from
    /// `merge_and_collect`; it must NOT be swallowed as end-of-base-iteration
    /// (that would drop the rest of the base file → truncated output, data
    /// loss). Parity with the vectorized `next_merged_base_batch`.
    #[test]
    fn lazy_base_source_error_propagates() {
        let mut buffer =
            build_key_based_buffer_with_delete_marker("COMMIT_TIME_ORDERING", "counter", "3");
        // One log block introducing key "A" and key "Z".
        buffer
            .process_data_block(&mut make_data_block(
                create_test_batch(&[("A", 5, 10), ("Z", 9, 9)]),
                "instant1",
            ))
            .unwrap();
        let schema = create_test_schema();
        // Base: one good batch ("A"), then an error.
        let base_ok = create_test_batch(&[("A", 1, 1)]);
        let reader = arrow_array::RecordBatchIterator::new(
            vec![
                Ok(base_ok),
                Err(arrow_schema::ArrowError::ExternalError(
                    "simulated base read failure".into(),
                )),
            ]
            .into_iter(),
            schema.clone(),
        );
        buffer.set_base_file_source(Box::new(reader));

        // The base source error must surface as an Err, not a silently-truncated
        // partial result that happens to still contain some rows.
        let err = Box::new(buffer)
            .merge_and_collect()
            .expect_err("base source error must propagate, not be swallowed");
        assert!(
            matches!(err, crate::error::CoreError::ReadFileSliceError(_)),
            "base source error propagates as the expected variant (not silently dropped): {err:?}"
        );
    }

    // =========================================================================
    // T1-T4: probe the STREAMING output path (FileGroupMergeIterator::new_buffered)
    //
    // The tests above (Part B / Phase A-C) all exercise `merge_and_collect`,
    // which is the legacy bulk-materialise API. The streaming API is what the
    // FFI now uses (ENG-42991 + ENG-42992). T1-T4 verify that driving the same
    // populated buffer through `FileGroupMergeIterator::new_buffered` produces
    // the same record counts.
    // =========================================================================

    use crate::file_group::reader_v2::merge_iterator::DEFAULT_BATCH_SIZE;

    /// Helper: drain a `FileGroupMergeIterator` into a Vec<RecordBatch>,
    /// panicking on any iteration error. Mirrors how the FFI driver consumes
    /// the stream (no error swallowing).
    fn drain_streaming(iter: FileGroupMergeIterator) -> Vec<RecordBatch> {
        iter.map(|r| r.expect("streaming iterator yielded error"))
            .collect()
    }

    /// T1 — pins B1/B2.
    /// Populated records map + empty base source → the buffer must drain all
    /// log-only records as inserts. If output = 0, either `has_next_log_record`
    /// is broken or `update_processor` is filtering everything out.
    #[test]
    fn t1_buffered_log_only_drains_map() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // Push 5 records via the real process_data_block path so they carry
        // the same metadata/ordering values production records would.
        let log_block = create_test_batch(&[
            ("k1", 1, 1),
            ("k2", 2, 2),
            ("k3", 3, 3),
            ("k4", 4, 4),
            ("k5", 5, 5),
        ]);
        buffer
            .process_data_block(&mut make_data_block(log_block, "i1"))
            .unwrap();
        assert_eq!(buffer.size(), 5, "5 records should be in the buffer");

        // Empty base source (mimics a streaming parquet reader that finds no
        // base file, or a row group that yields zero batches).
        let schema = create_test_schema();
        let empty_reader =
            arrow_array::RecordBatchIterator::new(std::iter::empty(), schema.clone());
        buffer.set_base_file_source(Box::new(empty_reader));

        let iter = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(
            total_rows, 5,
            "expected all 5 log-only records to emerge as inserts (B1/B2)"
        );
    }

    /// T2 — pins B4 + base streaming.
    /// Empty records map + multi-batch base source → output should equal the
    /// concatenation of base batches. Verifies `next_base_row` advances across
    /// batch boundaries correctly.
    #[test]
    fn t2_buffered_empty_log_streams_base() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");
        assert_eq!(buffer.size(), 0);

        let schema = create_test_schema();
        let b1 = create_test_batch(&[("k1", 1, 1), ("k2", 2, 2)]);
        let b2 = create_test_batch(&[("k3", 3, 3)]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(b1), Ok(b2)].into_iter(), schema.clone());
        buffer.set_base_file_source(Box::new(reader));

        let iter = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 3,
            "expected 3 base rows preserved across the 2-batch source"
        );
    }

    /// T3 — pins the base-vs-log merge correctness for the streaming path.
    /// k1 in both base AND log map → log values must win; k2, k3 base-only.
    #[test]
    fn t3_buffered_base_with_log_overlap() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        let log_block = create_test_batch(&[("k1", 99, 100)]);
        buffer
            .process_data_block(&mut make_data_block(log_block, "i1"))
            .unwrap();
        assert_eq!(buffer.size(), 1);

        let schema = create_test_schema();
        let base = create_test_batch(&[("k1", 1, 1), ("k2", 2, 2), ("k3", 3, 3)]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(base)].into_iter(), schema.clone());
        buffer.set_base_file_source(Box::new(reader));

        let iter = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "k1 (updated) + k2 + k3 base passthrough");

        let concat = arrow::compute::concat_batches(&schema, &batches).unwrap();
        let records = extract_records(&concat);
        assert_eq!(
            records[0],
            ("k1".to_string(), 99, 100),
            "k1 must reflect the log-side update"
        );
        assert_eq!(records[1], ("k2".to_string(), 2, 2));
        assert_eq!(records[2], ("k3".to_string(), 3, 3));
    }

    /// Build a single-row partial-update log record: a narrow batch carrying
    /// `_hoodie_record_key` + only the named numeric columns (a subset of the
    /// full test schema), mirroring an IS_PARTIAL block that omits columns.
    fn partial_counter_record(key: &str, counter: i32) -> BufferedRecord {
        let narrow_schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("counter", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            narrow_schema,
            vec![
                Arc::new(StringArray::from(vec![key])) as _,
                Arc::new(Int32Array::from(vec![counter])) as _,
            ],
        )
        .unwrap();
        BufferedRecord::new_data(key.to_string(), batch, None)
    }

    /// Partial-update (IS_PARTIAL / KEEP_VALUES) base-vs-log overlay: a log record
    /// carrying only a SUBSET of columns overlays its present columns onto the
    /// base row, keeping the base value for every omitted column. The partial
    /// update sets `counter=99` for k1 but omits `ts`, so merged k1 must be
    /// (counter=99 from the log, ts=1 kept from base); k2 passes through.
    #[test]
    fn test_partial_update_base_vs_log_overlay() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");
        buffer
            .process_next_data_record(partial_counter_record("k1", 99), "k1")
            .unwrap();

        let schema = create_test_schema();
        let base = create_test_batch(&[("k1", 1, 1), ("k2", 2, 2)]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(base)].into_iter(), schema.clone());
        buffer.set_base_file_source(Box::new(reader));

        let iter = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );
        let concat = arrow::compute::concat_batches(&schema, &drain_streaming(iter)).unwrap();
        let records = extract_records(&concat);
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0],
            ("k1".to_string(), 99, 1),
            "counter overlaid from partial log; ts kept from base"
        );
        assert_eq!(records[1], ("k2".to_string(), 2, 2), "untouched base row");
    }

    /// Single-row `List<Int32>` column whose CHILD field is named `child`
    /// (arrow-avro uses "item", Parquet "element") — same physical layout,
    /// different `DataType` tag: a name-reconcilable nested difference.
    fn list_i32_col(child: &str, vals: &[i32]) -> ArrayRef {
        use arrow_array::{Int32Array, ListArray};
        use arrow_buffer::OffsetBuffer;
        Arc::new(ListArray::new(
            Arc::new(Field::new(child, DataType::Int32, true)),
            OffsetBuffer::new(vec![0, vals.len() as i32].into()),
            Arc::new(Int32Array::from(vals.to_vec())),
            None,
        ))
    }

    fn list_i32_type(child: &str) -> DataType {
        DataType::List(Arc::new(Field::new(child, DataType::Int32, true)))
    }

    /// #66 review — a PRIOR/base column differing from the target only in nested
    /// child-field NAMES (Avro "item" vs Parquet "element") must be name-
    /// reconciled and KEPT, not silently dropped to null (the old exact-DataType-
    /// or-null bug).
    #[test]
    fn test_overlay_keeps_prior_column_with_reconcilable_nested_name() {
        use arrow_array::{Int32Array, ListArray};
        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("tags", list_i32_type("element"), true),
        ]));
        // Partial carries only `key`; `tags` must come from the prior row.
        let partial = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["k"]))],
        )
        .unwrap();
        let prior = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("tags", list_i32_type("item"), true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["k"])),
                list_i32_col("item", &[7, 8]),
            ],
        )
        .unwrap();
        let out = overlay_partial_over_prior(&partial, &prior, &target).unwrap();
        assert_eq!(
            out.schema(),
            target,
            "output uses the target (element) child name"
        );
        let tags = out.column(1).as_any().downcast_ref::<ListArray>().unwrap();
        assert!(
            !tags.is_null(0),
            "reconcilable-nested base column KEPT, not nulled"
        );
        let inner = tags.value(0);
        let inner = inner.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(
            inner.values().to_vec(),
            vec![7, 8],
            "base list values preserved through name reconcile"
        );
    }

    /// #66 review — a PARTIAL column differing from the target only in nested
    /// child-field NAMES must be name-reconciled and OVERLAID, not spuriously
    /// hard-errored (the old exact-DataType compare).
    #[test]
    fn test_overlay_reconciles_partial_column_nested_name() {
        use arrow_array::{Int32Array, ListArray};
        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("tags", list_i32_type("element"), true),
        ]));
        let partial = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("tags", list_i32_type("item"), true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["k"])),
                list_i32_col("item", &[9]),
            ],
        )
        .unwrap();
        let prior = RecordBatch::try_new(
            target.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k"])),
                list_i32_col("element", &[1]),
            ],
        )
        .unwrap();
        let out = overlay_partial_over_prior(&partial, &prior, &target).unwrap();
        let tags = out.column(1).as_any().downcast_ref::<ListArray>().unwrap();
        let inner = tags.value(0);
        let inner = inner.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(
            inner.values().to_vec(),
            vec![9],
            "partial list overlaid (name-reconciled), not errored"
        );
    }

    /// Output schema with a NULLABLE column `b` (so a partial record that omits
    /// it can be null-filled when there is no prior value). `a` is the
    /// non-nullable always-present column.
    fn nb_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]))
    }

    /// A COMMIT_TIME buffer over [`nb_schema`].
    fn build_partial_buffer() -> KeyBasedFileGroupRecordBuffer {
        let merge_mode = "COMMIT_TIME_ORDERING";
        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "a".to_string(),
        );
        ctx.rebuild_record_context(String::new());
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(nb_schema())
            .with_data_schema(nb_schema());
        let key_field = ctx.record_key_field().to_string();
        let ordering = ctx.record_context.ordering_field_names.clone();
        handler
            .prepare_required_schema(
                true,
                &[key_field],
                &ordering,
                &ctx.table_config,
                false,
                merge_mode,
            )
            .unwrap();
        ctx.schema_handler = handler;
        KeyBasedFileGroupRecordBuffer::new(Arc::new(ctx), merge_mode.to_string(), false).unwrap()
    }

    /// A partial record carrying `_hoodie_record_key` + exactly one of `a`/`b`.
    fn nb_partial(key: &str, field: &str, val: i32) -> BufferedRecord {
        let s = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new(field, DataType::Int32, field == "b"),
        ]));
        let batch = RecordBatch::try_new(
            s,
            vec![
                Arc::new(StringArray::from(vec![key])) as _,
                Arc::new(Int32Array::from(vec![val])) as _,
            ],
        )
        .unwrap();
        BufferedRecord::new_data(key.to_string(), batch, None)
    }

    fn nb_row(c: &RecordBatch, col: usize) -> (Option<i32>, Option<i32>) {
        let a = c.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let b = c.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        let av = if a.is_null(col) {
            None
        } else {
            Some(a.value(col))
        };
        let bv = if b.is_null(col) {
            None
        } else {
            Some(b.value(col))
        };
        (av, bv)
    }

    /// Partial-update log-only INSERT (no base, no prior): omitted columns have
    /// no prior value, so a NULLABLE one is null-filled to the output schema.
    /// k9 is inserted with only `a=42`; `b` must come out null.
    #[test]
    fn test_partial_update_log_only_insert_null_fills() {
        let mut buffer = build_partial_buffer();
        buffer
            .process_next_data_record(nb_partial("k9", "a", 42), "k9")
            .unwrap();
        let schema = nb_schema();
        buffer.set_base_file_source(Box::new(arrow_array::RecordBatchIterator::new(
            std::iter::empty(),
            schema.clone(),
        )));
        let iter = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );
        let concat = arrow::compute::concat_batches(&schema, &drain_streaming(iter)).unwrap();
        assert_eq!(concat.num_rows(), 1);
        assert_eq!(
            nb_row(&concat, 0),
            (Some(42), None),
            "a kept; omitted b null-filled"
        );
    }

    /// Partial-update log-vs-log UNION: two partial updates to the same key with
    /// DISJOINT columns (no base) merge into the union of their columns. update1
    /// sets a=1 (omits b), update2 sets b=5 (omits a) → merged (a=1, b=5).
    #[test]
    fn test_partial_update_log_vs_log_union() {
        let mut buffer = build_partial_buffer();
        buffer
            .process_next_data_record(nb_partial("k1", "a", 1), "k1")
            .unwrap();
        buffer
            .process_next_data_record(nb_partial("k1", "b", 5), "k1")
            .unwrap();
        let schema = nb_schema();
        buffer.set_base_file_source(Box::new(arrow_array::RecordBatchIterator::new(
            std::iter::empty(),
            schema.clone(),
        )));
        let iter = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );
        let concat = arrow::compute::concat_batches(&schema, &drain_streaming(iter)).unwrap();
        assert_eq!(concat.num_rows(), 1);
        assert_eq!(
            nb_row(&concat, 0),
            (Some(1), Some(5)),
            "a from update1, b from update2 (union of two disjoint partials)"
        );
    }

    // ── EVENT_TIME partial-update (ordering decides the winner, then overlay) ──

    /// Schema with an ordering column `ts` and a NULLABLE `note` a partial may omit.
    fn pu_event_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("a", DataType::Int32, false),
            Field::new("note", DataType::Utf8, true),
        ]))
    }

    fn build_pu_event_buffer() -> KeyBasedFileGroupRecordBuffer {
        let merge_mode = "EVENT_TIME_ORDERING";
        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "ts".to_string(),
        );
        ctx.rebuild_record_context(String::new());
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(pu_event_schema())
            .with_data_schema(pu_event_schema());
        let key_field = ctx.record_key_field().to_string();
        let ordering = ctx.record_context.ordering_field_names.clone();
        handler
            .prepare_required_schema(
                true,
                &[key_field],
                &ordering,
                &ctx.table_config,
                false,
                merge_mode,
            )
            .unwrap();
        ctx.schema_handler = handler;
        KeyBasedFileGroupRecordBuffer::new(Arc::new(ctx), merge_mode.to_string(), false).unwrap()
    }

    /// A partial record carrying `_hoodie_record_key` + `ts` + `a` (omits `note`),
    /// with ordering value = `ts`.
    fn pu_event_partial(key: &str, ts: i64, a: i32) -> BufferedRecord {
        let s = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("a", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            s,
            vec![
                Arc::new(StringArray::from(vec![key])) as _,
                Arc::new(Int64Array::from(vec![ts])) as _,
                Arc::new(Int32Array::from(vec![a])) as _,
            ],
        )
        .unwrap();
        BufferedRecord::new_data(key.to_string(), batch, Some(OrderingValue::Long(ts)))
    }

    fn pu_event_base() -> RecordBatch {
        RecordBatch::try_new(
            pu_event_schema(),
            vec![
                Arc::new(StringArray::from(vec!["k1"])) as _,
                Arc::new(Int64Array::from(vec![5i64])) as _,
                Arc::new(Int32Array::from(vec![10])) as _,
                Arc::new(StringArray::from(vec![Some("keep")])) as _,
            ],
        )
        .unwrap()
    }

    fn pu_event_drain(buffer: KeyBasedFileGroupRecordBuffer) -> (i64, i32, Option<String>) {
        let schema = pu_event_schema();
        let mut buffer = buffer;
        buffer.set_base_file_source(Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(pu_event_base())].into_iter(),
            schema.clone(),
        )));
        let iter = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );
        let c = arrow::compute::concat_batches(&schema, &drain_streaming(iter)).unwrap();
        assert_eq!(c.num_rows(), 1);
        let ts = c
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let a = c
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0);
        let note_col = c.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        let note = if note_col.is_null(0) {
            None
        } else {
            Some(note_col.value(0).to_string())
        };
        (ts, a, note)
    }

    /// EVENT_TIME: a NEWER partial update (ts=9 > base ts=5) wins the ordering and
    /// overlays — `a` updated, omitted `note` kept from base.
    #[test]
    fn test_partial_update_event_time_newer_wins_and_overlays() {
        let mut buffer = build_pu_event_buffer();
        buffer
            .process_next_data_record(pu_event_partial("k1", 9, 99), "k1")
            .unwrap();
        assert_eq!(
            pu_event_drain(buffer),
            (9, 99, Some("keep".to_string())),
            "newer partial wins; a overlaid, note kept from base"
        );
    }

    /// EVENT_TIME: a STALE partial update (ts=2 < base ts=5) loses the ordering, so
    /// the base row is kept unchanged and the partial is NOT overlaid.
    #[test]
    fn test_partial_update_event_time_stale_loses_no_overlay() {
        let mut buffer = build_pu_event_buffer();
        buffer
            .process_next_data_record(pu_event_partial("k1", 2, 99), "k1")
            .unwrap();
        assert_eq!(
            pu_event_drain(buffer),
            (5, 10, Some("keep".to_string())),
            "stale partial loses; base row kept intact"
        );
    }

    /// A partial column whose type disagrees with the table schema is a LOUD error
    /// (not a silent null) — a present value must never be dropped.
    #[test]
    fn test_partial_update_type_mismatch_is_loud() {
        let target = nb_schema(); // a: Int32
        // Partial carries `a` as Int64 — wrong type for the table's Int32 `a`.
        let bad = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("_hoodie_record_key", DataType::Utf8, false),
                Field::new("a", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["k1"])) as _,
                Arc::new(Int64Array::from(vec![1i64])) as _,
            ],
        )
        .unwrap();
        let prior = RecordBatch::try_new(
            target.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k1"])) as _,
                Arc::new(Int32Array::from(vec![10])) as _,
                Arc::new(Int32Array::from(vec![20])) as _,
            ],
        )
        .unwrap();
        let err = overlay_partial_over_prior(&bad, &prior, &target).unwrap_err();
        assert!(
            matches!(err, crate::error::CoreError::Unsupported(_)),
            "type-mismatched partial column must error loudly, got: {err:?}"
        );
        assert!(
            pad_partial_to_target(&bad, &target).is_err(),
            "pad must also reject it"
        );
    }

    /// An `IS_PARTIAL` log block carries Hudi meta columns
    /// (`_hoodie_commit_time`, `_hoodie_record_key`, …) alongside the updated data
    /// column. The projected reader (target) schema may have pruned those meta
    /// columns, so the partial record can have AS MANY or MORE fields than the
    /// target while still missing real data columns (here the `id` key). A
    /// field-COUNT short-circuit misclassifies it as full, skips the overlay, and
    /// fails in `reconcile_batch_to_schema` with "column 'id' missing" — the
    /// `TestMergeIntoTable` "matched columns cast-ed" failure (`UPDATE SET ts`
    /// only). `schema_is_partial` must detect it by field SET.
    #[test]
    fn test_schema_is_partial_detects_missing_field_despite_extra_meta_columns() {
        // Target (projected reader) schema: 3 data columns, key meta pruned out.
        let target = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("ts", DataType::Int32, true),
        ]);
        // Partial record: only the updated `ts`, plus meta columns the block
        // carries — 4 fields (MORE than the target's 3), yet missing `id`/`name`.
        let partial = Schema::new(vec![
            Field::new("_hoodie_commit_time", DataType::Utf8, true),
            Field::new("_hoodie_record_key", DataType::Utf8, true),
            Field::new("_hoodie_partition_path", DataType::Utf8, true),
            Field::new("ts", DataType::Int32, true),
        ]);
        assert!(
            schema_is_partial(&partial, &target),
            "a record missing target columns (id, name) is partial even though its \
             field count is >= the target's (extra meta columns)"
        );
    }

    /// A full `nb_schema` base batch from `(key, a, b)` rows (`b` nullable).
    fn nb_base(rows: &[(&str, i32, Option<i32>)]) -> RecordBatch {
        RecordBatch::try_new(
            nb_schema(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as _,
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )) as _,
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )) as _,
            ],
        )
        .unwrap()
    }

    /// A narrow `(_hoodie_record_key, a)` partial batch (omits `b`).
    fn nb_narrow_batch(key: &str, a: i32) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("_hoodie_record_key", DataType::Utf8, false),
                Field::new("a", DataType::Int32, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![key])) as _,
                Arc::new(Int32Array::from(vec![a])) as _,
            ],
        )
        .unwrap()
    }

    /// `(key, a, b)` rows of an `nb_schema` batch, sorted by key.
    fn nb_rows(c: &RecordBatch) -> Vec<(String, Option<i32>, Option<i32>)> {
        let keys = c.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let a = c.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let b = c.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        let mut out: Vec<_> = (0..c.num_rows())
            .map(|i| {
                let av = if a.is_null(i) { None } else { Some(a.value(i)) };
                let bv = if b.is_null(i) { None } else { Some(b.value(i)) };
                (keys.value(i).to_string(), av, bv)
            })
            .collect();
        out.sort();
        out
    }

    /// End-to-end through the buffer entry point: a partial batch fed via
    /// `process_data_block` (not a hand-built BufferedRecord) flows through
    /// `batch_to_buffered_records` into the merge and overlays onto the base row.
    /// k1's `a` is updated from the narrow log block; its omitted `b` is kept from
    /// base; k2 passes through untouched.
    #[test]
    fn test_partial_update_end_to_end_via_process_data_block() {
        let mut buffer = build_partial_buffer();
        buffer
            .process_data_block(&mut make_data_block(nb_narrow_batch("k1", 99), "i1"))
            .unwrap();
        let schema = nb_schema();
        buffer.set_base_file_source(Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(nb_base(&[("k1", 1, Some(2)), ("k2", 3, Some(4))]))].into_iter(),
            schema.clone(),
        )));
        let iter = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );
        let c = arrow::compute::concat_batches(&schema, &drain_streaming(iter)).unwrap();
        assert_eq!(
            nb_rows(&c),
            vec![
                ("k1".to_string(), Some(99), Some(2)),
                ("k2".to_string(), Some(3), Some(4)),
            ],
            "k1: a overlaid from partial log block, b kept from base; k2 passthrough"
        );
    }

    /// Partial-update + delete: a key with a buffered partial update that then
    /// receives a (commit-time) delete is dropped from the output — the delete
    /// short-circuits the overlay (deletes carry no columns to merge). k2 survives.
    #[test]
    fn test_partial_update_then_delete_drops_key() {
        let mut buffer = build_partial_buffer();
        buffer
            .process_data_block(&mut make_data_block(nb_narrow_batch("k1", 99), "i1"))
            .unwrap();
        buffer
            .process_delete_block(&mut make_delete_block(&[("k1", None)], "i2"))
            .unwrap();
        let schema = nb_schema();
        buffer.set_base_file_source(Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(nb_base(&[("k1", 1, Some(2)), ("k2", 3, Some(4))]))].into_iter(),
            schema.clone(),
        )));
        let iter = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );
        let c = arrow::compute::concat_batches(&schema, &drain_streaming(iter)).unwrap();
        assert_eq!(
            nb_rows(&c),
            vec![("k2".to_string(), Some(3), Some(4))],
            "k1 deleted despite the prior partial update; k2 survives"
        );
    }

    /// T4 — pins B5 (chunking math at the batch boundary).
    /// Push DEFAULT_BATCH_SIZE+1 log-only records → iterator must emit exactly
    /// two chunks: one of size DEFAULT_BATCH_SIZE, one of size 1.
    #[test]
    fn t4_buffered_chunks_at_batch_boundary() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        let n = DEFAULT_BATCH_SIZE + 1;
        let owned_keys: Vec<String> = (0..n).map(|i| format!("k{i:05}")).collect();
        let recs: Vec<(&str, i32, i64)> = owned_keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.as_str(), i as i32, i as i64))
            .collect();
        let log_block = create_test_batch(&recs);
        buffer
            .process_data_block(&mut make_data_block(log_block, "i1"))
            .unwrap();
        assert_eq!(buffer.size(), n);

        let schema = create_test_schema();
        let empty_reader =
            arrow_array::RecordBatchIterator::new(std::iter::empty(), schema.clone());
        buffer.set_base_file_source(Box::new(empty_reader));

        let iter = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, n, "no records lost in the drain phase");

        // Log-only inserts drain in bounded `DEFAULT_BATCH_SIZE` chunks (ENG-42993 /
        // #42 review): still vectorized like ENG-43009 (each chunk is a single
        // `records_to_batch`, NOT the old per-row model), but capped so the drain no
        // longer re-materializes the whole (possibly-spilled) map into one giant
        // batch at once -- which would OOM at drain the very budget the spill
        // enforces. n = DEFAULT_BATCH_SIZE + 1 -> two chunks: a full batch + 1 row.
        assert_eq!(
            batches.len(),
            2,
            "log-only drain emits bounded DEFAULT_BATCH_SIZE chunks, not one giant batch"
        );
        assert_eq!(batches[0].num_rows(), DEFAULT_BATCH_SIZE);
        assert_eq!(batches[1].num_rows(), 1);
        assert!(
            batches.iter().all(|b| b.num_rows() <= DEFAULT_BATCH_SIZE),
            "no drain chunk exceeds DEFAULT_BATCH_SIZE"
        );

        // Content equivalence across the chunk boundary: every input row must
        // survive with its exact payload. A count-only check would pass a
        // boundary defect that dropped one row while duplicating another, or
        // corrupted a value — assert the full concatenated content instead.
        let concat = arrow::compute::concat_batches(&schema, batches.iter()).unwrap();
        let got = extract_records(&concat); // already sorted by key
        let expected: Vec<(String, i32, i64)> = (0..n)
            .map(|i| (format!("k{i:05}"), i as i32, i as i64))
            .collect();
        assert_eq!(
            got, expected,
            "chunked drain preserves every row's exact payload, no dup/drop/corruption"
        );
    }

    // =========================================================================
    // T5-T7: continue narrowing the q99 bug. T1-T4 ruled out the merge state
    // machine itself. The remaining suspects are (a) the OutputConverter step
    // production uses but my synthetic tests bypassed, (b) `records_to_batch`
    // schema reconciliation when merge_schema disagrees with the records'
    // actual schema, (c) EVENT_TIME_ORDERING (production's likely merge mode)
    // through the streaming path.
    // =========================================================================

    use crate::file_group::reader_v2::output_converter::ProjectionConverter;

    /// T5 — does the path through `OutputConverter::apply` lose rows?
    ///
    /// Production passes a `ProjectionConverter` to project the merge_schema
    /// down to the requested columns. None of T1-T4 supplied a converter. T5
    /// repeats T3's scenario (base + log merge) with a real converter that
    /// drops the `ts` column, and verifies the merge rows survive the
    /// projection step.
    #[test]
    fn t5_buffered_with_projection_output_converter() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        let log_block = create_test_batch(&[("k1", 99, 100)]);
        buffer
            .process_data_block(&mut make_data_block(log_block, "i1"))
            .unwrap();

        let merge_schema = create_test_schema();
        let base = create_test_batch(&[("k1", 1, 1), ("k2", 2, 2), ("k3", 3, 3)]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(base)].into_iter(), merge_schema.clone());
        buffer.set_base_file_source(Box::new(reader));

        // Project the 3-column merge schema down to 2 columns
        // (_hoodie_record_key + counter, drop ts). Matches the shape of the
        // ProjectionConverter production hands `new_buffered`.
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("counter", DataType::Int32, false),
        ]));
        let converter = Box::new(ProjectionConverter::new(&target_schema));

        let iter = new_buffered_test(
            Box::new(buffer),
            merge_schema.clone(),
            target_schema.clone(),
            Some(converter),
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 3,
            "projection must not drop rows — expected k1+k2+k3"
        );

        // Each chunk now has the projected (2-column) schema.
        for b in &batches {
            assert_eq!(
                b.num_columns(),
                2,
                "every chunk must have the projected schema"
            );
            assert_eq!(b.schema().field(0).name(), "_hoodie_record_key");
            assert_eq!(b.schema().field(1).name(), "counter");
        }
    }

    /// T6 — what happens if `merge_schema` has a column the records don't?
    ///
    /// `records_to_batch` (called inside the Buffered iterator each chunk)
    /// calls `reconcile_batch_to_schema` whenever a record's batch schema
    /// disagrees with the target. The reconcile path does
    /// `batch.schema().index_of(target_field.name()).expect("column name must
    /// exist in source batch")`. So a mismatch *panics*. T6 verifies whether
    /// this is reachable from the streaming iterator (i.e. — would q99 see a
    /// panic in production rather than 0 rows?).
    #[test]
    fn t6_buffered_merge_schema_extra_column_fails_loudly() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        let log_block = create_test_batch(&[("k1", 1, 1), ("k2", 2, 2)]);
        buffer
            .process_data_block(&mut make_data_block(log_block, "i1"))
            .unwrap();
        assert_eq!(buffer.size(), 2);

        // merge_schema has 4 columns, but the records carry only 3. Triggers
        // the reconcile path (records' batch.schema() != merge_schema).
        let bogus_schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("counter", DataType::Int32, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("col_that_does_not_exist", DataType::Utf8, true),
        ]));

        let schema_for_empty_source = create_test_schema();
        let empty_reader =
            arrow_array::RecordBatchIterator::new(std::iter::empty(), schema_for_empty_source);
        buffer.set_base_file_source(Box::new(empty_reader));

        let iter = new_buffered_test(
            Box::new(buffer),
            bogus_schema.clone(),
            bogus_schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );

        // Catch the panic from records_to_batch → reconcile_batch_to_schema.
        // Either a panic OR an Err — but NOT a silent 0-row stream.
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| iter.collect::<Vec<_>>()));

        match result {
            Ok(chunks) => {
                // Did not panic. Then we want to see an explicit Err in the
                // stream, NOT a silent 0-row pass-through. If we see 0 chunks
                // with no error — THAT is the q99 bug pattern.
                let any_err = chunks.iter().any(|r| r.is_err());
                let total_rows: usize = chunks
                    .iter()
                    .filter_map(|r| r.as_ref().ok())
                    .map(|b| b.num_rows())
                    .sum();
                assert!(
                    any_err || total_rows > 0,
                    "schema mismatch produced 0 rows and 0 errors — this IS the silent-loss pattern"
                );
            }
            Err(_) => {
                // Panic propagated up. Production would see this as an
                // executor crash, not 0 rows. So this is NOT the q99 bug.
            }
        }
    }

    // =========================================================================
    // T9 — pin the q99 subset-projection bug: wider table_schema, subset
    // requested_schema, base batches in the production parquet-sorted-projection
    // order, log batches in the writer (= full table) schema. Verify the
    // streaming output preserves rows AND column values match by name.
    // Production symptom (q99 date_dim, ReadSchema=[d_date_sk,d_month_seq] vs
    // table=34 cols): hudi-rs's stage 47 inputRows=0; Java's same plan: 73,049.
    // =========================================================================

    fn build_wide_schema_buffer() -> KeyBasedFileGroupRecordBuffer {
        // Mimics a real Hudi table with hudi meta cols + 4 data cols.
        let wide_schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("_hoodie_commit_time", DataType::Utf8, true),
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("_hoodie_partition_path", DataType::Utf8, true),
            Field::new("a_id", DataType::Int32, false),
            Field::new("b_count", DataType::Int32, false),
            Field::new("c_label", DataType::Utf8, true),
            Field::new("d_ts", DataType::Int64, false),
        ]));
        // The requested schema is a STRICT subset of the table, in a
        // different order than the table layout.
        let requested_schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("a_id", DataType::Int32, false),
            Field::new("b_count", DataType::Int32, false),
        ]));
        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "d_ts".to_string(),
        );
        ctx.rebuild_record_context(String::new());
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(wide_schema.clone())
            .with_data_schema(wide_schema.clone())
            .with_requested_schema(requested_schema.clone());
        let key_field = ctx.record_key_field().to_string();
        let ordering = ctx.record_context.ordering_field_names.clone();
        handler
            .prepare_required_schema(
                true,
                &[key_field],
                &ordering,
                &ctx.table_config,
                false,
                "COMMIT_TIME_ORDERING",
            )
            .unwrap();
        ctx.schema_handler = handler;
        let ctx = Arc::new(ctx);
        KeyBasedFileGroupRecordBuffer::new(ctx, "COMMIT_TIME_ORDERING".to_string(), false).unwrap()
    }

    fn make_wide_log_batch(rows: &[(&str, i32, i32, &str, i64)]) -> RecordBatch {
        // 7-col batch matching the wide writer schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_commit_time", DataType::Utf8, true),
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("_hoodie_partition_path", DataType::Utf8, true),
            Field::new("a_id", DataType::Int32, false),
            Field::new("b_count", DataType::Int32, false),
            Field::new("c_label", DataType::Utf8, true),
            Field::new("d_ts", DataType::Int64, false),
        ]));
        let commits: Vec<&str> = rows.iter().map(|_| "instant_log").collect();
        let keys: Vec<&str> = rows.iter().map(|r| r.0).collect();
        let parts: Vec<&str> = rows.iter().map(|_| "p=1").collect();
        let a_ids: Vec<i32> = rows.iter().map(|r| r.1).collect();
        let b_counts: Vec<i32> = rows.iter().map(|r| r.2).collect();
        let c_labels: Vec<&str> = rows.iter().map(|r| r.3).collect();
        let d_tss: Vec<i64> = rows.iter().map(|r| r.4).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(commits)),
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(parts)),
                Arc::new(Int32Array::from(a_ids)),
                Arc::new(Int32Array::from(b_counts)),
                Arc::new(StringArray::from(c_labels)),
                Arc::new(Int64Array::from(d_tss)),
            ],
        )
        .unwrap()
    }

    fn make_projected_base_batch(rows: &[(&str, i32, i32)]) -> RecordBatch {
        // Mimics what the parquet reader emits in parquet-physical-index sorted
        // order for required_schema = [a_id, b_count, _hoodie_record_key].
        // In the table's column order, _hoodie_record_key is at index 1,
        // a_id at index 3, b_count at index 4. Sorted: 1, 3, 4 →
        // post-projection schema is [_hoodie_record_key, a_id, b_count].
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("a_id", DataType::Int32, false),
            Field::new("b_count", DataType::Int32, false),
        ]));
        let keys: Vec<&str> = rows.iter().map(|r| r.0).collect();
        let a_ids: Vec<i32> = rows.iter().map(|r| r.1).collect();
        let b_counts: Vec<i32> = rows.iter().map(|r| r.2).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int32Array::from(a_ids)),
                Arc::new(Int32Array::from(b_counts)),
            ],
        )
        .unwrap()
    }

    /// T9 — base + log merge with subset-projection schema.
    /// Production shape: log batches in writer (= full 7-col) schema, base
    /// batches in parquet-sorted-projection (3-col) schema. Output goes
    /// through OutputConverter projecting required→requested.
    /// EXPECTED: 3 rows out, with a_id+b_count matching the LOG values
    /// (commit-time-ordering, log wins) for overlapping keys.
    /// If FAILS at 0 rows → reproduces the q99 production symptom.
    #[test]
    fn t9_subset_projection_log_plus_base_merge() {
        let mut buffer = build_wide_schema_buffer();

        // Push 2 log records via the real process_data_block path. These
        // carry the FULL 7-col schema (writer schema).
        let log_block = make_wide_log_batch(&[
            ("k1", 1001, 11, "log-k1", 1_000_000),
            ("k2", 1002, 12, "log-k2", 1_000_001),
        ]);
        buffer
            .process_data_block(&mut make_data_block_inline(log_block, "i1"))
            .unwrap();
        assert_eq!(buffer.size(), 2, "two records in the buffer's map");

        // Base file: 3 rows in parquet-sorted-projection (3-col) schema:
        // [_hoodie_record_key, a_id, b_count]. k1+k2 overlap with the log
        // (log should win), k3 is base-only.
        let base = make_projected_base_batch(&[("k1", 1, 1), ("k2", 2, 2), ("k3", 3, 3)]);
        let source_schema = base.schema();
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(base)].into_iter(), source_schema);
        buffer.set_base_file_source(Box::new(reader));

        // Pull required_schema + the output converter that production uses.
        // These come from the same schema_handler that the buffer was built
        // from (cloned into reader_context).
        let required_schema = buffer
            .reader_context
            .schema_handler
            .required_schema
            .clone()
            .unwrap();
        let output_converter = buffer.reader_context.schema_handler.get_output_converter();
        let requested_schema = buffer
            .reader_context
            .schema_handler
            .requested_schema
            .clone()
            .unwrap();

        let iter = new_buffered_test(
            Box::new(buffer),
            required_schema,
            requested_schema.clone(),
            output_converter,
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(
            total_rows, 3,
            "all 3 unique keys must emerge (log-wins for k1+k2, base for k3)"
        );

        // Concat + spot-check the projected column values.
        let concat = arrow::compute::concat_batches(&requested_schema, &batches).unwrap();
        // Pull a_id and b_count cols by name.
        let a_id_col = concat
            .column(concat.schema().index_of("a_id").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("a_id column should be Int32");
        let b_count_col = concat
            .column(concat.schema().index_of("b_count").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("b_count column should be Int32");

        // Build (a_id, b_count) sorted lists for stable comparison
        // (HashMap iter order is non-deterministic).
        let mut pairs: Vec<(i32, i32)> = (0..concat.num_rows())
            .map(|i| (a_id_col.value(i), b_count_col.value(i)))
            .collect();
        pairs.sort();
        // Expected: k1→(1001,11) from log, k2→(1002,12) from log, k3→(3,3) base.
        assert_eq!(
            pairs,
            vec![(3, 3), (1001, 11), (1002, 12)],
            "projected a_id/b_count VALUES must match source (log wins for k1+k2, base for k3)"
        );
    }

    /// Wrapper around `make_data_block` that doesn't shadow `super::*` names.
    fn make_data_block_inline(batch: RecordBatch, instant: &str) -> LogBlock {
        make_data_block(batch, instant)
    }

    // =========================================================================
    // T10 — pin the production q99 symptom: does Buffered iterator emit more
    // than one chunk when the base file has > DEFAULT_BATCH_SIZE rows?
    //
    // Production observation (lin-diag-mor-nowarmup-q99):
    //   date_dim base file has 73,049 rows
    //   FG-SUMMARY shows chunks_in=1 rows_in=4096
    //
    // i.e. PostMergePredicateFilter saw the iterator emit EXACTLY ONE chunk
    // of 4096 rows then stop. We need to know:
    //   (a) does FileGroupMergeIterator::Buffered correctly continue past
    //       the first chunk when base_file_source has more rows?
    //   (b) if YES — the production stop is downstream (Velox/FFI/Drop).
    //   (c) if NO — the bug is here in the Buffered iterator.
    //
    // Strategy: empty log records map + base file source carrying
    // DEFAULT_BATCH_SIZE * 2 + 100 = 8292 rows. Drive Buffered iterator,
    // count chunks. Correct behavior = 3 chunks (4096 + 4096 + 100).
    // =========================================================================
    #[test]
    fn t10_buffered_emits_multiple_chunks_when_base_exceeds_batch_size() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        let n = DEFAULT_BATCH_SIZE * 2 + 100;
        let owned_keys: Vec<String> = (0..n).map(|i| format!("k{i:05}")).collect();
        let recs: Vec<(&str, i32, i64)> = owned_keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.as_str(), i as i32, i as i64))
            .collect();
        let base = create_test_batch(&recs);
        let schema = create_test_schema();
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(base)].into_iter(), schema.clone());
        buffer.set_base_file_source(Box::new(reader));
        assert_eq!(buffer.size(), 0, "log map must be empty");

        let iter = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, n,
            "all base rows must emerge (correctness — no row loss)"
        );
        // ENG-43009 vectorized — chunk count is dictated by the base source's
        // own batch cadence. Test fixture provides a single 8292-row batch,
        // so output is one chunk of 8292. (Production parquet stream emits
        // row-group-by-row-group at ~4096 rows per group → output mirrors
        // that natural cadence.)
        assert_eq!(
            batches.len(),
            1,
            "one source batch → one merged chunk (vectorized path)"
        );
        assert_eq!(batches[0].num_rows(), n);
    }

    /// T11 — companion to T10. Wraps the same Buffered iterator in a
    /// minimal "always-false filter" — Rust analogue of
    /// `cpp/src/lib.rs::PostMergePredicateFilter` with a 0-rows-pass filter.
    /// Verifies the wrapper STILL emits chunks corresponding to each inner
    /// chunk (just empty), and the inner iterator is driven to completion.
    ///
    /// If T11 emits exactly 3 (empty) chunks → wrapper + iterator are fine,
    /// so production bug is in the FFI consumer (Velox `HudiSplitReader::next`
    /// caller stopping on empty arrow batch). If T11 emits only 1 → bug
    /// reproduces locally and is in the wrapper or iterator interaction.
    #[test]
    fn t11_wrapper_with_always_false_filter_drives_iterator_to_completion() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        let n = DEFAULT_BATCH_SIZE * 2 + 100;
        let owned_keys: Vec<String> = (0..n).map(|i| format!("k{i:05}")).collect();
        let recs: Vec<(&str, i32, i64)> = owned_keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.as_str(), i as i32, i as i64))
            .collect();
        let base = create_test_batch(&recs);
        let schema = create_test_schema();
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(base)].into_iter(), schema.clone());
        buffer.set_base_file_source(Box::new(reader));

        let inner = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );

        // Minimal Rust analogue of PostMergePredicateFilter that returns
        // an EMPTY batch for every inner chunk (i.e. filter rejects all).
        struct AlwaysFalseFilter<I> {
            inner: I,
            chunks_in: usize,
            chunks_out: usize,
        }
        impl<I> Iterator for AlwaysFalseFilter<I>
        where
            I: Iterator<Item = Result<RecordBatch, arrow_schema::ArrowError>>,
        {
            type Item = Result<RecordBatch, arrow_schema::ArrowError>;
            fn next(&mut self) -> Option<Self::Item> {
                let batch = self.inner.next()?.ok()?;
                self.chunks_in += 1;
                let empty = RecordBatch::new_empty(batch.schema());
                self.chunks_out += 1;
                Some(Ok(empty))
            }
        }

        let mut wrapped = AlwaysFalseFilter {
            inner,
            chunks_in: 0,
            chunks_out: 0,
        };
        let mut emitted = 0usize;
        for item in wrapped.by_ref() {
            let b = item.expect("no errors");
            assert_eq!(b.num_rows(), 0, "filter rejected all rows in this chunk");
            emitted += 1;
        }
        // ENG-43009 vectorized — chunk count now mirrors the base source's
        // cadence. Test fixture is one 8292-row source batch, so the inner
        // iterator emits exactly one chunk, and the wrapper sees one too.
        assert_eq!(
            wrapped.chunks_in, 1,
            "inner iterator called once for the single source batch"
        );
        assert_eq!(emitted, 1);
    }

    // =========================================================================
    // V1, V2 — ENG-43009 vectorized merge regression tests.
    //
    // These pin the new `next_merged_base_batch` + `drain_log_only_inserts`
    // path. The legacy `merge_and_collect` path (covered by Phase A/B/C +
    // T1-T9 above) is still in place for back-compat, but the FFI streaming
    // path now goes through the vectorized kernel.
    // =========================================================================

    /// V1 — multi-batch base source preserves rowcount AND values across
    /// chunk boundaries on the vectorized path.
    ///
    /// Setup: 3 base batches, second-largest in the middle, log map carries
    /// updates for one key in each batch + one log-only insert.
    /// Expectation: output rowcount = (sum of base batch rows) + 1 log-only.
    /// Per-key contents: log values win for the overlapped keys.
    #[test]
    fn v1_vectorized_multi_batch_base_with_log_overlap() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // Log map: overlap with key in each base batch + one log-only insert.
        let log_block = create_test_batch(&[
            ("a", 100, 100), // overlaps base batch 1
            ("c", 300, 300), // overlaps base batch 2
            ("e", 500, 500), // overlaps base batch 3
            ("z", 999, 999), // log-only insert (no base match)
        ]);
        buffer
            .process_data_block(&mut make_data_block(log_block, "i1"))
            .unwrap();

        let schema = create_test_schema();
        let b1 = create_test_batch(&[("a", 1, 1), ("b", 2, 2)]);
        let b2 = create_test_batch(&[("c", 3, 3), ("d", 4, 4)]);
        let b3 = create_test_batch(&[("e", 5, 5), ("f", 6, 6)]);
        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(b1), Ok(b2), Ok(b3)].into_iter(),
            schema.clone(),
        );
        buffer.set_base_file_source(Box::new(reader));

        let iter = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);

        // 3 base chunks + 1 drain chunk = 4 chunks.
        assert_eq!(batches.len(), 4);

        let concat = arrow::compute::concat_batches(&schema, &batches).unwrap();
        let records = extract_records(&concat);

        // a=log(100), b=base, c=log(300), d=base, e=log(500), f=base, z=log(insert)
        assert_eq!(records.len(), 7);
        assert_eq!(records[0], ("a".to_string(), 100, 100)); // log wins
        assert_eq!(records[1], ("b".to_string(), 2, 2)); // base passthrough
        assert_eq!(records[2], ("c".to_string(), 300, 300));
        assert_eq!(records[3], ("d".to_string(), 4, 4));
        assert_eq!(records[4], ("e".to_string(), 500, 500));
        assert_eq!(records[5], ("f".to_string(), 6, 6));
        assert_eq!(records[6], ("z".to_string(), 999, 999)); // log-only insert
    }

    /// V2 — a base batch entirely dropped by log-side deletes still
    /// produces NO output chunk (vectorized path correctly skips empty
    /// post-filter batches; iterator's loop falls through to the next
    /// source batch).
    #[test]
    fn v2_vectorized_base_batch_all_deleted_no_chunk_emitted() {
        let mut buffer = build_key_based_buffer("COMMIT_TIME_ORDERING");

        // Put one delete in the log map for every key in base batch 1.
        for k in &["a", "b"] {
            buffer
                .base
                .records
                .insert(
                    (*k).to_string(),
                    BufferedRecord::new_delete((*k).to_string(), None),
                )
                .unwrap();
        }

        let schema = create_test_schema();
        // batch 1: entirely deleted; batch 2: passes through
        let b1 = create_test_batch(&[("a", 1, 1), ("b", 2, 2)]);
        let b2 = create_test_batch(&[("c", 3, 3)]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(b1), Ok(b2)].into_iter(), schema.clone());
        buffer.set_base_file_source(Box::new(reader));

        let iter = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);

        // Only one non-empty chunk: batch 2 with c=base. Batch 1 was fully
        // deleted (every base row matched a log delete) so the iterator
        // produced no chunk for it.
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "only base batch 2's row 'c' survives");
        let concat = arrow::compute::concat_batches(&schema, &batches).unwrap();
        let records = extract_records(&concat);
        assert_eq!(records[0], ("c".to_string(), 3, 3));
    }

    /// T7 — drive the streaming path with EVENT_TIME_ORDERING (production's
    /// likely merge mode). Mirrors the existing `test_read_with_event_time_ordering`
    /// (which uses `merge_and_collect`) but through `new_buffered`. If the
    /// streaming path filters records differently from the bulk path under
    /// event-time ordering, this will surface it.
    #[test]
    fn t7_buffered_event_time_ordering_matches_bulk() {
        let mut buffer = build_key_based_buffer("EVENT_TIME_ORDERING");

        // Same block as test_read_with_event_time_ordering.
        let block = create_test_batch(&[
            ("1", 2, 1),
            ("2", 1, 2),
            ("2", 1, 0),
            ("3", 1, 2),
            ("3", 3, 1),
        ]);
        buffer
            .process_data_block(&mut make_data_block(block, "instant1"))
            .unwrap();

        let schema = create_test_schema();
        let base = create_test_batch(&[("1", 1, 1), ("2", 1, 1), ("3", 1, 1)]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(base)].into_iter(), schema.clone());
        buffer.set_base_file_source(Box::new(reader));

        let iter = new_buffered_test(
            Box::new(buffer),
            schema.clone(),
            schema.clone(),
            None,
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);
        let concat = arrow::compute::concat_batches(&schema, &batches).unwrap();
        let records = extract_records(&concat);

        // Same expectations as test_read_with_event_time_ordering.
        assert_eq!(concat.num_rows(), 3);
        assert_eq!(records[0], ("1".to_string(), 2, 1));
        assert_eq!(records[1], ("2".to_string(), 1, 2));
        assert_eq!(records[2], ("3".to_string(), 1, 2));
    }

    // =========================================================================
    // GAP repro — PostgresDebeziumAvroPayload "toasted" / unavailable-value
    // partial update (v6 postgres-payload support).
    //
    // PostgresDebeziumAvroPayload decomposes into three merge rules:
    //   1. delete:  `_change_operation_type` == "d"  (≈ custom delete marker — supported)
    //   2. ordering: precombine by `_event_lsn` (Long, higher wins) (≈ EVENT_TIME — supported)
    //   3. TOASTED: if an incoming STRING/BYTES column equals the sentinel
    //      `__debezium_unavailable_value`, the merged row must KEEP the prior
    //      (base) value for that column (Postgres TOAST / FILL_UNAVAILABLE).
    //
    // Rule 3 is implemented via `fill_unavailable_from_base` (gated on the
    // `hoodie.record.merge.property.partial.update.unavailable.value` config):
    // when a winning log column equals the sentinel, the prior base value is
    // kept. This test asserts that Postgres semantics. (Before the rule, the log
    // won wholesale and the sentinel string leaked into the output — Class-C
    // silent-wrong.) See the postgres-payload-v6 gap writeup.
    // =========================================================================

    /// EVENT_TIME (LSN-ordered) buffer over a minimal Debezium-shaped table:
    /// `(_hoodie_record_key, val, _event_lsn)`, precombine = `_event_lsn`.
    fn build_debezium_event_time_buffer() -> KeyBasedFileGroupRecordBuffer {
        let schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("val", DataType::Utf8, true),
            Field::new("_event_lsn", DataType::Int64, false),
        ]));
        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "_event_lsn".to_string(),
        );
        // PostgresDebeziumAvroPayload partial-update config (as Hudi's
        // HoodieTableConfig auto-derives it): toasted columns carrying this
        // sentinel keep the prior base value.
        ctx.hoodie_reader_config.insert(
            "hoodie.record.merge.property.partial.update.unavailable.value".to_string(),
            "__debezium_unavailable_value".to_string(),
        );
        ctx.rebuild_record_context(String::new());
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(schema.clone())
            .with_data_schema(schema.clone())
            .with_requested_schema(schema.clone());
        let key_field = ctx.record_key_field().to_string();
        let ordering = ctx.record_context.ordering_field_names.clone();
        handler
            .prepare_required_schema(
                true,
                &[key_field],
                &ordering,
                &ctx.table_config,
                false,
                "EVENT_TIME_ORDERING",
            )
            .unwrap();
        ctx.schema_handler = handler;
        let ctx = Arc::new(ctx);
        KeyBasedFileGroupRecordBuffer::new(ctx, "EVENT_TIME_ORDERING".to_string(), false).unwrap()
    }

    fn debezium_batch(rows: &[(&str, &str, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("val", DataType::Utf8, true),
            Field::new("_event_lsn", DataType::Int64, false),
        ]));
        let keys: Vec<&str> = rows.iter().map(|r| r.0).collect();
        let vals: Vec<&str> = rows.iter().map(|r| r.1).collect();
        let lsns: Vec<i64> = rows.iter().map(|r| r.2).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(vals)),
                Arc::new(Int64Array::from(lsns)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn gap_postgres_debezium_toasted_value_keeps_prior() {
        const SENTINEL: &str = "__debezium_unavailable_value";
        let mut buffer = build_debezium_event_time_buffer();

        // Log records carry the newer LSN (20) so they win the merge:
        //   k1 — TOASTED update: `val` is the sentinel (only `_event_lsn` changed)
        //   k2 — ordinary update: a real new `val`
        let log = debezium_batch(&[("k1", SENTINEL, 20), ("k2", "updated_k2", 20)]);
        buffer
            .process_data_block(&mut make_data_block_inline(log, "i1"))
            .unwrap();

        // Base file holds the prior (LSN 10) values.
        let base = debezium_batch(&[("k1", "orig_k1", 10), ("k2", "orig_k2", 10)]);
        let src_schema = base.schema();
        buffer.set_base_file_source(Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(base)].into_iter(),
            src_schema,
        )));

        let required = buffer
            .reader_context
            .schema_handler
            .required_schema
            .clone()
            .unwrap();
        let requested = buffer
            .reader_context
            .schema_handler
            .requested_schema
            .clone()
            .unwrap();
        let conv = buffer.reader_context.schema_handler.get_output_converter();
        let iter = new_buffered_test(
            Box::new(buffer),
            required,
            requested.clone(),
            conv,
            DEFAULT_BATCH_SIZE,
        );
        let batches = drain_streaming(iter);
        let concat = arrow::compute::concat_batches(&requested, &batches).unwrap();

        let keys = concat
            .column(concat.schema().index_of("_hoodie_record_key").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vals = concat
            .column(concat.schema().index_of("val").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut got: Vec<(String, String)> = (0..concat.num_rows())
            .map(|i| (keys.value(i).to_string(), vals.value(i).to_string()))
            .collect();
        got.sort();

        assert_eq!(
            got,
            vec![
                // TOASTED: Postgres semantics KEEP the prior base value.
                ("k1".to_string(), "orig_k1".to_string()),
                // ordinary update: log wins (this part already works).
                ("k2".to_string(), "updated_k2".to_string()),
            ],
            "toasted `val` for k1 must retain the prior base value, not the \
             '{SENTINEL}' sentinel that the log record carried"
        );
    }

    /// H7 (PR #95 re-review, harm catalog): a FILL_UNAVAILABLE toasted record
    /// that is a LOG-ONLY insert — no base row and no earlier log record to fill
    /// the sentinel from — must FAIL LOUDLY, never emit the
    /// `__debezium_unavailable_value` placeholder as a real column value. On the
    /// snapshot path this is unreachable (a Debezium `op=u` always has a reachable
    /// prior), so the guard protects a future reader (incremental / skip_merge)
    /// that makes the no-prior path reachable. Discriminating: pre-guard code
    /// emitted `k_new` with `val` = sentinel (silent leak) instead of erroring.
    #[test]
    fn h7_toasted_log_only_insert_with_no_prior_fails_loudly() {
        const SENTINEL: &str = "__debezium_unavailable_value";
        let debz_schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("val", DataType::Utf8, true),
            Field::new("_event_lsn", DataType::Int64, false),
        ]));

        // Leak case: toasted log-only insert over an EMPTY base → no prior.
        let mut buffer = build_debezium_event_time_buffer();
        buffer
            .process_data_block(&mut make_data_block_inline(
                debezium_batch(&[("k_new", SENTINEL, 20)]),
                "i1",
            ))
            .unwrap();
        buffer.set_base_file_source(Box::new(arrow_array::RecordBatchIterator::new(
            std::iter::empty(),
            debz_schema.clone(),
        )));
        let required = buffer
            .reader_context
            .schema_handler
            .required_schema
            .clone()
            .unwrap();
        let requested = buffer
            .reader_context
            .schema_handler
            .requested_schema
            .clone()
            .unwrap();
        let conv = buffer.reader_context.schema_handler.get_output_converter();
        let iter = new_buffered_test(
            Box::new(buffer),
            required,
            requested,
            conv,
            DEFAULT_BATCH_SIZE,
        );
        let collected: std::result::Result<Vec<RecordBatch>, arrow_schema::ArrowError> =
            iter.collect();
        let err = collected.expect_err(
            "toasted log-only insert with no prior must fail loudly, not leak sentinel",
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("sentinel leaked") && msg.contains("val"),
            "expected the H7 guard error naming the leaked column, got: {msg}"
        );

        // Control: a real (non-sentinel) log-only insert drains cleanly.
        let mut buffer2 = build_debezium_event_time_buffer();
        buffer2
            .process_data_block(&mut make_data_block_inline(
                debezium_batch(&[("k_new", "real_value", 20)]),
                "i1",
            ))
            .unwrap();
        buffer2.set_base_file_source(Box::new(arrow_array::RecordBatchIterator::new(
            std::iter::empty(),
            debz_schema.clone(),
        )));
        let required2 = buffer2
            .reader_context
            .schema_handler
            .required_schema
            .clone()
            .unwrap();
        let requested2 = buffer2
            .reader_context
            .schema_handler
            .requested_schema
            .clone()
            .unwrap();
        let conv2 = buffer2.reader_context.schema_handler.get_output_converter();
        let iter2 = new_buffered_test(
            Box::new(buffer2),
            required2,
            requested2,
            conv2,
            DEFAULT_BATCH_SIZE,
        );
        let batches: Vec<RecordBatch> = iter2
            .collect::<std::result::Result<Vec<RecordBatch>, arrow_schema::ArrowError>>()
            .expect("a clean (non-sentinel) log-only insert must drain without error");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "the real-value insert should emit exactly one row");
    }

    // =========================================================================
    // Composite (multi-field) ordering — MySqlDebeziumAvroPayload's
    // `(_event_bin_file, _event_pos)`. The merge must compare the ordering
    // lexicographically field-by-field (file first, then pos), so a record with
    // a higher binlog file wins even if its position is lower. Exercises the
    // `OrderingValue::Composite` path end-to-end through the vectorized merge.
    // =========================================================================

    /// EVENT_TIME buffer over `(_hoodie_record_key, val, _event_bin_file, _event_pos)`
    /// with a two-field ordering — the MySql Debezium shape.
    fn build_mysql_composite_buffer() -> KeyBasedFileGroupRecordBuffer {
        let schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("val", DataType::Utf8, true),
            Field::new("_event_bin_file", DataType::Utf8, false),
            Field::new("_event_pos", DataType::Int64, false),
        ]));
        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            "hoodie.table.ordering.fields".to_string(),
            "_event_bin_file,_event_pos".to_string(),
        );
        ctx.rebuild_record_context(String::new());
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(schema.clone())
            .with_data_schema(schema.clone())
            .with_requested_schema(schema.clone());
        let key_field = ctx.record_key_field().to_string();
        let ordering = ctx.record_context.ordering_field_names.clone();
        handler
            .prepare_required_schema(
                true,
                &[key_field],
                &ordering,
                &ctx.table_config,
                false,
                "EVENT_TIME_ORDERING",
            )
            .unwrap();
        ctx.schema_handler = handler;
        let ctx = Arc::new(ctx);
        KeyBasedFileGroupRecordBuffer::new(ctx, "EVENT_TIME_ORDERING".to_string(), false).unwrap()
    }

    fn mysql_batch(rows: &[(&str, &str, &str, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("val", DataType::Utf8, true),
            Field::new("_event_bin_file", DataType::Utf8, false),
            Field::new("_event_pos", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn composite_ordering_compares_file_then_pos() {
        let mut buffer = build_mysql_composite_buffer();

        // Log records:
        //   k1 — OLDER than base (same file, lower pos) → base must win
        //   k2 — NEWER than base (higher file, even though lower pos) → log wins
        let log = mysql_batch(&[
            ("k1", "log_k1", "bin.0001", 499),
            ("k2", "log_k2", "bin.0002", 10),
        ]);
        buffer
            .process_data_block(&mut make_data_block_inline(log, "i1"))
            .unwrap();

        let base = mysql_batch(&[
            ("k1", "base_k1", "bin.0001", 500),
            ("k2", "base_k2", "bin.0001", 999),
        ]);
        let src_schema = base.schema();
        buffer.set_base_file_source(Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(base)].into_iter(),
            src_schema,
        )));

        let required = buffer
            .reader_context
            .schema_handler
            .required_schema
            .clone()
            .unwrap();
        let requested = buffer
            .reader_context
            .schema_handler
            .requested_schema
            .clone()
            .unwrap();
        let conv = buffer.reader_context.schema_handler.get_output_converter();
        let iter = new_buffered_test(
            Box::new(buffer),
            required,
            requested.clone(),
            conv,
            DEFAULT_BATCH_SIZE,
        );
        let concat = arrow::compute::concat_batches(&requested, &drain_streaming(iter)).unwrap();

        let keys = concat
            .column(concat.schema().index_of("_hoodie_record_key").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vals = concat
            .column(concat.schema().index_of("val").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut got: Vec<(String, String)> = (0..concat.num_rows())
            .map(|i| (keys.value(i).to_string(), vals.value(i).to_string()))
            .collect();
        got.sort();

        assert_eq!(
            got,
            vec![
                // same file, log pos 499 < base 500 → base wins
                ("k1".to_string(), "base_k1".to_string()),
                // log file bin.0002 > base bin.0001 → log wins despite lower pos
                ("k2".to_string(), "log_k2".to_string()),
            ],
            "composite ordering must compare _event_bin_file before _event_pos"
        );
    }
}
