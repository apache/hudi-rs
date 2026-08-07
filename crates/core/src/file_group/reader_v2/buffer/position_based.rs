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

//! Position-based file-group record buffer.
//!
//! Mirrors Java `org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer`,
//! which `extends KeyBasedFileGroupRecordBuffer`. Instead of keying the merge
//! map by record key, it keys it by each log record's **position in the base
//! file** — the row index recorded in the log block's `RECORD_POSITIONS` header.
//! Base rows are matched to log records by their physical position (read from
//! the synthetic row-index column attached to the base read), which is cheaper
//! than key extraction + string hashing and is the layout Spark writes when
//! `hoodie.merge.use.record.positions` is enabled.
//!
//! ## Composition
//!
//! Java extends the key-based buffer; here we **compose** it
//! ([`inner`](PositionBasedFileGroupRecordBuffer::inner)) and reuse its
//! vectorized/scalar merge kernels via the [`BaseMatch`] parameter, so the merge
//! logic (winner selection, keep-mask, reconcile, update stats, spill map) is
//! shared rather than duplicated. Positions are encoded as decimal-string map
//! keys in the existing `String`-keyed spillable map; the map holds a single key
//! domain at any instant — positions before fallback, record keys after.
//!
//! ## Fallback to key-based
//!
//! When a log block has no valid positions (missing / mismatched base-file
//! instant, or an empty bitmap), the buffer permanently switches to key-based
//! merge for the rest of the scan: it re-keys every already-buffered record from
//! its position to its record key and delegates subsequent processing to the
//! key-based super methods. This mirrors Java `fallbackToKeyBasedBuffer`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::Result;
use crate::error::CoreError;
use crate::file_group::log_file::log_block::{LogBlock, LogBlockContent};
use crate::file_group::reader_v2::buffer::key_based::{BaseMatch, KeyBasedFileGroupRecordBuffer};
use crate::file_group::reader_v2::buffer::record_positions::{
    base_row_position_array, decode_record_positions,
};
use crate::file_group::reader_v2::buffer::row_extraction::records_to_batch;
use crate::file_group::reader_v2::buffer::spillable_map::{SpillConfig, SpillableRecordMap};
use crate::file_group::reader_v2::buffer::{BufferType, HoodieFileGroupRecordBuffer};
use crate::file_group::reader_v2::buffered_record::{BufferedRecord, DeleteRecord};
use crate::file_group::reader_v2::merge_iterator::DEFAULT_BATCH_SIZE;
use crate::file_group::reader_v2::reader_context::ReaderContext;
use crate::file_group::reader_v2::update_processor::UpdateStats;

/// A merge buffer that matches base rows to log records by base-file row
/// position. See the module docs. Wraps a [`KeyBasedFileGroupRecordBuffer`] and
/// falls back to it when positions are unavailable.
#[derive(Debug)]
pub struct PositionBasedFileGroupRecordBuffer {
    /// The key-based buffer whose merge kernels/state this reuses. After a
    /// fallback its records map holds record-key entries (position entries
    /// otherwise).
    inner: KeyBasedFileGroupRecordBuffer,
    /// Commit/instant time of the base file being merged. Positions in a log
    /// block are only usable when the block's
    /// `BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS` header equals this.
    base_file_instant_time: String,
    /// Whether to still merge by position. Set true at construction; flipped
    /// false permanently on the first fallback (mirrors Java
    /// `readerContext.setShouldMergeUseRecordPosition(false)`).
    should_use_position: bool,
    /// Set during fallback when a position-only delete (a delete record without
    /// a usable record key) could not be re-keyed and must keep being matched by
    /// position. Effectively unreachable in hudi-rs (delete blocks always carry
    /// record keys), but tracked for parity with Java's hybrid strategy.
    needs_hybrid_strategy: bool,
    /// Position-only deletes retained across a fallback (see
    /// [`needs_hybrid_strategy`](Self::needs_hybrid_strategy)).
    hybrid_deletes: HashMap<u64, BufferedRecord>,
}

impl PositionBasedFileGroupRecordBuffer {
    /// Construct a position-based buffer. `base_file_instant_time` is the commit
    /// time of the base file in this file slice (Java passes
    /// `baseFile.getCommitTime()`), used to validate that a block's positions
    /// were computed against this base file.
    pub fn new(
        reader_context: Arc<ReaderContext>,
        merge_mode: String,
        emit_delete: bool,
        base_file_instant_time: String,
    ) -> Result<Self> {
        let inner = KeyBasedFileGroupRecordBuffer::new(reader_context, merge_mode, emit_delete)?;
        Ok(Self {
            inner,
            base_file_instant_time,
            should_use_position: true,
            needs_hybrid_strategy: false,
            hybrid_deletes: HashMap::new(),
        })
    }

    /// The map key for a base-file position: its decimal string. Positions and
    /// record keys never share the map at the same time (positions before
    /// fallback, record keys after), so the string domains do not collide.
    fn position_key(position: u64) -> String {
        position.to_string()
    }

    /// Extract the block's record positions, validating the base-file instant.
    ///
    /// Mirrors Java `extractRecordPositions`: returns `Ok(None)` (→ fallback)
    /// when the block's `BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS` header is
    /// missing / empty / different from this file group's base file instant, or
    /// when the `RECORD_POSITIONS` bitmap is missing / empty.
    fn extract_positions(&self, block: &LogBlock) -> Result<Option<Vec<u64>>> {
        match block.base_file_instant_time_of_positions() {
            Some(t) if !t.is_empty() && t == self.base_file_instant_time => {}
            _ => {
                log::debug!(
                    "[PositionBasedBuffer] falling back: block base-file instant of positions \
                     absent or != {}",
                    self.base_file_instant_time,
                );
                return Ok(None);
            }
        }
        match block.record_positions_header() {
            Some(encoded) if !encoded.is_empty() => {
                // Writer-side invariant: `decode_record_positions` yields the
                // Roaring64 bitmap's positions in ASCENDING order, and the
                // caller zips them index-by-index with the block's records in
                // file order. This matches Java's
                // `PositionBasedFileGroupRecordBuffer`
                // (`recordPositions.get(recordIndex++)`) and is only correct
                // because the Hudi writer emits records within a block in
                // ascending base-position order.
                let positions = decode_record_positions(encoded)?;
                if positions.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(positions))
                }
            }
            _ => Ok(None),
        }
    }

    /// Permanently switch to key-based merge, re-keying every already-buffered
    /// record from its position to its record key. Mirrors Java
    /// `fallbackToKeyBasedBuffer`.
    fn fallback_to_key_based(&mut self) -> Result<()> {
        if !self.should_use_position {
            return Ok(());
        }
        self.should_use_position = false;

        // Rebuild the (possibly-spilled) position-keyed map as a record-key-keyed
        // map. `drain_iter` yields values; the position keys are dropped.
        //
        // A record's `record_key` field is NOT trustworthy here: the spill tier
        // reconstructs a drained record's `record_key` from its MAP key (see
        // `RocksDbDiskMap::record_from_entry`), which in this buffer is the
        // POSITION string, not the record key. So for data records the true key
        // is re-extracted from the payload row itself. A delete tombstone has no
        // payload to re-extract from — its `record_key` is only correct if the
        // tombstone never spilled (hudi-rs delete blocks always carry record
        // keys, and `process_next_deleted_record` preserves them in the
        // in-memory tier). A delete without a usable record key cannot be
        // re-keyed — Java keeps it position-keyed under the hybrid strategy, but
        // that requires the position, which `drain_iter` does not surface; fail
        // loudly rather than silently drop a delete (which would resurrect a
        // deleted row). A SPILLED position-keyed delete comes back with its
        // position string as `record_key` and is indistinguishable from a real
        // numeric key — persisting the record key through the spill tier is the
        // real fix (tracked as a follow-up outside this buffer).
        let fresh = SpillableRecordMap::with_config(SpillConfig::from_config(
            &self.inner.reader_context.hoodie_reader_config,
        )?);
        let old = std::mem::replace(&mut self.inner.base.records, fresh);
        for entry in old.drain_iter() {
            let mut record = entry?;
            if let Some(row) = record.get_record() {
                let keys = self.inner.record_context.get_record_keys(&row)?;
                let key = keys[0].clone();
                record.record_key = key.clone();
                self.inner.base.records.insert(key, record)?;
            } else if !record.record_key.is_empty() {
                let key = record.record_key.clone();
                self.inner.base.records.insert(key, record)?;
            } else if record.is_delete() {
                return Err(CoreError::Unsupported(
                    "position-based merge fallback encountered a delete record without a record \
                     key (Java's hybrid position-only-delete strategy). hudi-rs delete blocks \
                     always carry record keys, so this path is unexpected."
                        .to_string(),
                ));
            }
            // A non-delete record with no payload and an empty key is a
            // malformed record; drop it (it cannot be addressed by any base
            // row) — matches Java, which only special-cases the delete branch.
        }
        log::debug!(
            "[PositionBasedBuffer] fell back to key-based merge; re-keyed {} record(s)",
            self.inner.base.records.len(),
        );
        Ok(())
    }

    /// Which matching mode the base scan should use right now.
    fn base_match(&self) -> BaseMatch {
        if self.should_use_position {
            BaseMatch::Position
        } else {
            BaseMatch::RecordKey
        }
    }

    /// Scalar base-scan step (drives `has_next`/`next` and the hybrid path).
    /// Mirrors Java `PositionBasedFileGroupRecordBuffer.hasNextBaseRecord` +
    /// `doHasNextFallbackBaseRecord`.
    fn do_has_next_position(&mut self) -> Result<bool> {
        let base_match = self.base_match();
        while let Some((base_batch, row_idx)) = self.inner.base.next_base_row()? {
            if self.needs_hybrid_strategy && self.apply_hybrid_delete(&base_batch, row_idx)? {
                // A position-only delete applied to this base row: drop it.
                continue;
            }
            if self
                .inner
                .has_next_base_record_at(&base_batch, row_idx, base_match)?
            {
                return Ok(true);
            }
        }
        self.inner.base.has_next_log_record()
    }

    /// Hybrid strategy (post-fallback): if this base row's position matches a
    /// retained position-only delete, the newest version of the row is a delete
    /// — drop the row and also remove any key-based entry for it. Returns whether
    /// the row was deleted. Mirrors Java `doHasNextFallbackBaseRecord`.
    fn apply_hybrid_delete(
        &mut self,
        base_batch: &Arc<RecordBatch>,
        row_idx: usize,
    ) -> Result<bool> {
        let pos = position_at(base_batch, row_idx)?;
        if self.hybrid_deletes.remove(&pos).is_some() {
            let row = base_batch.slice(row_idx, 1);
            let keys = self.inner.record_context.get_record_keys(&row)?;
            let _ = self.inner.base.records.remove(&keys[0])?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Vectorized base-batch assembly for the rare hybrid-strategy case (base
    /// rows only; log-only inserts are drained separately). Kept scalar because
    /// the hybrid per-row delete check does not vectorize cleanly, and this path
    /// is effectively unreachable in hudi-rs.
    fn next_hybrid_base_batch(&mut self, target_schema: &SchemaRef) -> Result<Option<RecordBatch>> {
        let mut records: Vec<BufferedRecord> = Vec::new();
        while records.len() < DEFAULT_BATCH_SIZE {
            let Some((base_batch, row_idx)) = self.inner.base.next_base_row()? else {
                break;
            };
            if self.apply_hybrid_delete(&base_batch, row_idx)? {
                continue;
            }
            if self
                .inner
                .has_next_base_record_at(&base_batch, row_idx, BaseMatch::RecordKey)?
                && let Some(record) = self.inner.base.next_record.take()
            {
                records.push(record);
            }
        }
        if records.is_empty() {
            return Ok(None);
        }
        let batch = records_to_batch(records, target_schema.clone())?;
        if batch.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }
}

/// Read the base row's physical position from the row-index column.
fn position_at(base_batch: &RecordBatch, row_idx: usize) -> Result<u64> {
    let arr = base_row_position_array(base_batch)?;
    let raw = arr.value(row_idx);
    u64::try_from(raw).map_err(|_| {
        CoreError::ReadFileSliceError(format!(
            "position-based merge: negative base-file row position {raw}"
        ))
    })
}

impl HoodieFileGroupRecordBuffer for PositionBasedFileGroupRecordBuffer {
    fn get_buffer_type(&self) -> BufferType {
        if self.should_use_position {
            BufferType::PositionBasedMerge
        } else {
            self.inner.get_buffer_type()
        }
    }

    fn process_data_block(&mut self, block: &mut LogBlock) -> Result<()> {
        if !self.should_use_position {
            return self.inner.process_data_block(block);
        }
        let positions = match self.extract_positions(block)? {
            Some(p) => p,
            None => {
                self.fallback_to_key_based()?;
                return self.inner.process_data_block(block);
            }
        };

        let decode_start = std::time::Instant::now();
        // Blocks arrive with their content read; nothing to fetch here.
        self.inner.base.stage_decode_ms += decode_start.elapsed().as_millis() as u64;

        if let LogBlockContent::Records(record_batches) = std::mem::take(&mut block.content) {
            let mut pos_idx = 0usize;
            for batch in record_batches.data_batches {
                let batch = Arc::new(batch);
                // `record_context` and `base.delete_context` are disjoint fields
                // of `inner`, so these two immutable borrows coexist; the owned
                // `records` Vec ends both borrows before the `&mut` calls below.
                let records = self
                    .inner
                    .record_context
                    .batch_to_buffered_records(&batch, self.inner.base.delete_context.as_ref())?;
                // Index-zip: `positions[pos_idx]` is the base-file position of
                // the `pos_idx`-th record in file order. `decode_record_positions`
                // returns the Roaring64 bitmap ASCENDING, so this pairing is only
                // correct because the Hudi writer emits records within a block in
                // ascending base-position order — the same invariant Java's
                // `PositionBasedFileGroupRecordBuffer` relies on with
                // `recordPositions.get(recordIndex++)`.
                for (_record_key, record) in records {
                    let position = *positions.get(pos_idx).ok_or_else(|| {
                        CoreError::LogBlockError(format!(
                            "position-based merge: data block has more records than positions \
                             ({} positions for record index {pos_idx})",
                            positions.len(),
                        ))
                    })?;
                    pos_idx += 1;
                    self.inner
                        .process_next_data_record(record, &Self::position_key(position))?;
                }
            }
            if pos_idx != positions.len() {
                return Err(CoreError::LogBlockError(format!(
                    "position-based merge: data block record count {pos_idx} != position count {}",
                    positions.len(),
                )));
            }
        }
        Ok(())
    }

    fn process_next_data_record(&mut self, record: BufferedRecord, key: &str) -> Result<()> {
        self.inner.process_next_data_record(record, key)
    }

    fn process_delete_block(&mut self, block: &mut LogBlock) -> Result<()> {
        if !self.should_use_position {
            return self.inner.process_delete_block(block);
        }
        let positions = match self.extract_positions(block)? {
            Some(p) => p,
            None => {
                self.fallback_to_key_based()?;
                return self.inner.process_delete_block(block);
            }
        };

        let decode_start = std::time::Instant::now();
        // Blocks arrive with their content read; nothing to fetch here.
        self.inner.base.stage_decode_ms += decode_start.elapsed().as_millis() as u64;

        if let LogBlockContent::Records(record_batches) = std::mem::take(&mut block.content) {
            let mut pos_idx = 0usize;
            for (batch, _inst) in record_batches.delete_batches {
                let delete_entries = self
                    .inner
                    .record_context
                    .delete_batch_to_keys_with_ordering(&batch)?;
                // Same ascending-order index-zip invariant as in
                // `process_data_block` above.
                for (record_key, ordering_value) in delete_entries {
                    let position = *positions.get(pos_idx).ok_or_else(|| {
                        CoreError::LogBlockError(format!(
                            "position-based merge: delete block has more records than positions \
                             ({} positions for record index {pos_idx})",
                            positions.len(),
                        ))
                    })?;
                    pos_idx += 1;
                    // Java sets the record key on the delete so a later fallback
                    // can re-key it; `DeleteRecord` carries it, and
                    // `process_next_deleted_record` keys the map by position.
                    let delete_record = DeleteRecord {
                        record_key,
                        partition_path: String::new(),
                        ordering_value,
                    };
                    self.inner.process_next_deleted_record(
                        delete_record,
                        &Self::position_key(position),
                    )?;
                }
            }
            if pos_idx != positions.len() {
                return Err(CoreError::LogBlockError(format!(
                    "position-based merge: delete block record count {pos_idx} != position count {}",
                    positions.len(),
                )));
            }
        }
        Ok(())
    }

    fn process_next_deleted_record(
        &mut self,
        delete_record: DeleteRecord,
        key: &str,
    ) -> Result<()> {
        self.inner.process_next_deleted_record(delete_record, key)
    }

    /// Whether a log record with `record_key` is buffered. The map is
    /// position-keyed while `should_use_position`, so scan values by their
    /// record key (mirrors Java's overridden `containsLogRecord`). Only the
    /// in-memory tier is scanned; a spilled position entry is not visible here
    /// (a limitation shared with the key-based `get_log_records`).
    fn contains_log_record(&self, record_key: &str) -> bool {
        if !self.should_use_position {
            return self.inner.contains_log_record(record_key);
        }
        self.inner
            .base
            .records
            .in_memory()
            .values()
            .any(|r| !r.is_delete() && r.record_key == record_key)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn get_total_log_records(&self) -> u64 {
        self.inner.get_total_log_records()
    }

    fn stage_decode_ms(&self) -> u64 {
        self.inner.stage_decode_ms()
    }

    fn merge_map_peak_entries(&self) -> u64 {
        self.inner.merge_map_peak_entries()
    }

    fn merge_map_spilled(&self) -> bool {
        self.inner.merge_map_spilled()
    }

    fn merge_map_peak_in_memory_bytes(&self) -> u64 {
        self.inner.merge_map_peak_in_memory_bytes()
    }

    fn current_in_memory_bytes(&self) -> u64 {
        self.inner.current_in_memory_bytes()
    }

    fn update_stats_snapshot(&self) -> UpdateStats {
        self.inner.update_stats_snapshot()
    }

    fn get_log_records(&self) -> &crate::file_group::reader_v2::buffer::MergeMap {
        self.inner.get_log_records()
    }

    fn set_reader_schema(&mut self, schema: SchemaRef) {
        self.inner.set_reader_schema(schema)
    }

    fn set_base_file_source(&mut self, source: Box<dyn arrow_array::RecordBatchReader + Send>) {
        self.inner.set_base_file_source(source)
    }

    fn compact_pinned_batches(&mut self) -> Result<()> {
        self.inner.compact_pinned_batches()
    }

    fn has_next(&mut self) -> Result<bool> {
        if self.inner.base.next_record.is_some() {
            return Ok(true);
        }
        self.do_has_next_position()
    }

    fn next(&mut self) -> Option<BufferedRecord> {
        self.inner.base.next_record.take()
    }

    fn merge_and_collect_with_stats(mut self: Box<Self>) -> Result<(RecordBatch, UpdateStats)> {
        // Output schema selection mirrors the key-based buffer.
        let schema = if let Some(schema) = &self.inner.base.reader_schema {
            schema.clone()
        } else if let Some(source) = &self.inner.base.base_file_source {
            source.schema()
        } else {
            let any_data_record = self
                .inner
                .base
                .records
                .in_memory()
                .values()
                .find_map(|r| r.get_record());
            match any_data_record.as_ref() {
                Some(batch) => batch.schema(),
                None => {
                    return Err(CoreError::ReadFileSliceError(
                        "No schema available for merge output".to_string(),
                    ));
                }
            }
        };

        let mut output_records: Vec<BufferedRecord> = Vec::new();
        while self.has_next()? {
            if let Some(record) = self.next() {
                output_records.push(record);
            }
        }
        let stats = self.inner.base.update_processor.read_stats_counts();
        let batch = records_to_batch(output_records, schema)?;
        Ok((batch, stats))
    }

    fn next_merged_base_batch(&mut self, target_schema: &SchemaRef) -> Result<Option<RecordBatch>> {
        if self.needs_hybrid_strategy {
            return self.next_hybrid_base_batch(target_schema);
        }
        let base_match = self.base_match();
        self.inner
            .pull_and_merge_next_base_batch(target_schema, base_match)
    }

    fn drain_log_only_inserts(&mut self, target_schema: &SchemaRef) -> Result<Option<RecordBatch>> {
        self.inner.drain_log_only_inserts(target_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::log_file::log_block::{BlockMetadataKey, BlockType};
    use crate::file_group::log_file::log_format::LogFormatVersion;
    use crate::file_group::reader_v2::buffer::record_positions::ROW_INDEX_TEMPORARY_COLUMN_NAME;
    use crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler;
    use crate::file_group::record_batches::RecordBatches;
    use arrow_array::{Int32Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use roaring::RoaringTreemap;

    const BASE_INSTANT: &str = "20240101000000000";

    /// Table schema: `_hoodie_record_key` (Utf8), `counter` (Int32), `ts` (Int64).
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("counter", DataType::Int32, false),
            Field::new("ts", DataType::Int64, false),
        ]))
    }

    /// Base-file read schema = table schema + the synthetic row-index column.
    fn base_read_schema() -> Arc<Schema> {
        let mut fields: Vec<Field> = schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(
            ROW_INDEX_TEMPORARY_COLUMN_NAME,
            DataType::Int64,
            false,
        ));
        Arc::new(Schema::new(fields))
    }

    /// A base batch of `(key, counter, ts)` rows at physical positions
    /// `0..rows.len()` — the row-index column mirrors what the parquet virtual
    /// RowNumber column would produce for a full, unfiltered base read.
    fn base_batch(rows: &[(&str, i32, i64)]) -> RecordBatch {
        base_batch_at(rows, 0)
    }

    /// Like [`base_batch`], but the row-index column starts at
    /// `start_position` — models one batch (row group) of a larger base file,
    /// where positions are absolute offsets within the whole file, not
    /// per-batch offsets.
    fn base_batch_at(rows: &[(&str, i32, i64)], start_position: u64) -> RecordBatch {
        let keys: Vec<&str> = rows.iter().map(|(k, _, _)| *k).collect();
        let counters: Vec<i32> = rows.iter().map(|(_, c, _)| *c).collect();
        let ts: Vec<i64> = rows.iter().map(|(_, _, t)| *t).collect();
        let positions: Vec<i64> = (0..rows.len() as i64)
            .map(|i| start_position as i64 + i)
            .collect();
        RecordBatch::try_new(
            base_read_schema(),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int32Array::from(counters)),
                Arc::new(Int64Array::from(ts)),
                Arc::new(Int64Array::from(positions)),
            ],
        )
        .unwrap()
    }

    /// Encode positions the way Hudi's Java writer does (portable Roaring64 +
    /// standard base64), for the `RECORD_POSITIONS` header.
    fn encode_positions(positions: &[u64]) -> String {
        let mut bitmap = RoaringTreemap::new();
        for &p in positions {
            bitmap.insert(p);
        }
        let mut buf = Vec::with_capacity(bitmap.serialized_size());
        bitmap.serialize_into(&mut buf).unwrap();
        STANDARD.encode(buf)
    }

    /// A data LogBlock carrying `RECORD_POSITIONS` + the base-file-instant header.
    /// `positions[i]` is the base-file position of data row `i`.
    fn data_block_with_positions(
        rows: &[(&str, i32, i64)],
        positions: &[u64],
        base_instant_of_positions: &str,
    ) -> LogBlock {
        data_block_with_positions_at_instant(
            rows,
            positions,
            base_instant_of_positions,
            "20240101000001000",
        )
    }

    /// Like [`data_block_with_positions`], but with an explicit block instant
    /// time, for tests that scan multiple data blocks from distinct commits.
    fn data_block_with_positions_at_instant(
        rows: &[(&str, i32, i64)],
        positions: &[u64],
        base_instant_of_positions: &str,
        instant_time: &str,
    ) -> LogBlock {
        let keys: Vec<&str> = rows.iter().map(|(k, _, _)| *k).collect();
        let counters: Vec<i32> = rows.iter().map(|(_, c, _)| *c).collect();
        let ts: Vec<i64> = rows.iter().map(|(_, _, t)| *t).collect();
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int32Array::from(counters)),
                Arc::new(Int64Array::from(ts)),
            ],
        )
        .unwrap();
        let mut header = HashMap::new();
        header.insert(BlockMetadataKey::InstantTime, instant_time.to_string());
        header.insert(
            BlockMetadataKey::RecordPositions,
            encode_positions(positions),
        );
        header.insert(
            BlockMetadataKey::BaseFileInstantTimeOfRecordPositions,
            base_instant_of_positions.to_string(),
        );
        LogBlock::new(
            LogFormatVersion::V1,
            BlockType::ParquetData,
            header,
            LogBlockContent::Records(RecordBatches::new_with_data_batches(vec![batch])),
            HashMap::new(),
        )
    }

    /// A delete LogBlock carrying `RECORD_POSITIONS` + the base-file-instant header.
    fn delete_block_with_positions(
        keys: &[&str],
        positions: &[u64],
        base_instant_of_positions: &str,
    ) -> LogBlock {
        let delete_schema = Arc::new(Schema::new(vec![
            Field::new("recordKey", DataType::Utf8, false),
            Field::new("partitionPath", DataType::Utf8, true),
            Field::new("orderingVal", DataType::Int64, true),
        ]));
        let parts: Vec<&str> = keys.iter().map(|_| "").collect();
        let ords: Vec<Option<i64>> = keys.iter().map(|_| None).collect();
        let batch = RecordBatch::try_new(
            delete_schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(StringArray::from(parts)),
                Arc::new(Int64Array::from(ords)),
            ],
        )
        .unwrap();
        let mut batches = RecordBatches::new();
        batches.push_delete_batch(batch, "20240101000001000".to_string());
        let mut header = HashMap::new();
        header.insert(
            BlockMetadataKey::InstantTime,
            "20240101000001000".to_string(),
        );
        header.insert(
            BlockMetadataKey::RecordPositions,
            encode_positions(positions),
        );
        header.insert(
            BlockMetadataKey::BaseFileInstantTimeOfRecordPositions,
            base_instant_of_positions.to_string(),
        );
        LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Delete,
            header,
            LogBlockContent::Records(batches),
            HashMap::new(),
        )
    }

    fn build_buffer() -> PositionBasedFileGroupRecordBuffer {
        build_buffer_with_reader_config(&[])
    }

    /// Build a buffer with extra `hoodie_reader_config` entries (e.g.
    /// `hoodie.memory.merge.max.size` to force the merge map to spill), same
    /// pattern as key_based's `build_key_based_buffer_with_reader_config`.
    fn build_buffer_with_reader_config(
        reader_config: &[(&str, &str)],
    ) -> PositionBasedFileGroupRecordBuffer {
        let merge_mode = "COMMIT_TIME_ORDERING";
        let mut ctx = ReaderContext::empty();
        for (k, v) in reader_config {
            ctx.hoodie_reader_config
                .insert((*k).to_string(), (*v).to_string());
        }
        ctx.rebuild_record_context(String::new());
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(schema())
            .with_data_schema(schema());
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
        PositionBasedFileGroupRecordBuffer::new(
            Arc::new(ctx),
            merge_mode.to_string(),
            false,
            BASE_INSTANT.to_string(),
        )
        .unwrap()
    }

    /// Extract `(key, counter, ts)` sorted by key for full-data assertions.
    fn extract(batch: &RecordBatch) -> Vec<(String, i32, i64)> {
        let keys = batch
            .column(batch.schema().index_of("_hoodie_record_key").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counters = batch
            .column(batch.schema().index_of("counter").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let ts = batch
            .column(batch.schema().index_of("ts").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut out: Vec<(String, i32, i64)> = (0..batch.num_rows())
            .map(|i| (keys.value(i).to_string(), counters.value(i), ts.value(i)))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    #[test]
    fn buffer_type_is_position_based_until_fallback() {
        let buffer = build_buffer();
        assert_eq!(buffer.get_buffer_type(), BufferType::PositionBasedMerge);
    }

    #[test]
    fn position_merge_updates_matched_base_row() {
        let mut buffer = build_buffer();
        // Log updates the row at base position 1 (key "b") to counter 99.
        let block = data_block_with_positions(&[("b", 99, 5)], &[1], BASE_INSTANT);
        let mut block = block;
        buffer.process_data_block(&mut block).unwrap();
        buffer.set_base_file_iterator(vec![base_batch(&[
            ("a", 10, 1),
            ("b", 20, 2),
            ("c", 30, 3),
        ])]);

        let (out, stats) = Box::new(buffer).merge_and_collect_with_stats().unwrap();
        assert_eq!(
            extract(&out),
            vec![
                ("a".into(), 10, 1),
                ("b".into(), 99, 5), // replaced by the log record at position 1
                ("c".into(), 30, 3),
            ]
        );
        assert_eq!(stats.num_updates, 1);
        // Output must NOT carry the internal row-index column.
        assert!(
            out.schema()
                .index_of(ROW_INDEX_TEMPORARY_COLUMN_NAME)
                .is_err()
        );
    }

    #[test]
    fn position_merge_deletes_matched_base_row() {
        let mut buffer = build_buffer();
        // Delete the row at base position 1 (key "b").
        let mut block = delete_block_with_positions(&["b"], &[1], BASE_INSTANT);
        buffer.process_delete_block(&mut block).unwrap();
        buffer.set_base_file_iterator(vec![base_batch(&[
            ("a", 10, 1),
            ("b", 20, 2),
            ("c", 30, 3),
        ])]);

        let (out, stats) = Box::new(buffer).merge_and_collect_with_stats().unwrap();
        assert_eq!(
            extract(&out),
            vec![("a".into(), 10, 1), ("c".into(), 30, 3)]
        );
        assert_eq!(stats.num_deletes, 1);
    }

    #[test]
    fn position_merge_emits_log_only_insert_beyond_base() {
        let mut buffer = build_buffer();
        // Log record at position 3 — beyond the 3-row base file → an insert.
        let mut block = data_block_with_positions(&[("d", 40, 4)], &[3], BASE_INSTANT);
        buffer.process_data_block(&mut block).unwrap();
        buffer.set_base_file_iterator(vec![base_batch(&[
            ("a", 10, 1),
            ("b", 20, 2),
            ("c", 30, 3),
        ])]);

        let (out, _stats) = Box::new(buffer).merge_and_collect_with_stats().unwrap();
        assert_eq!(
            extract(&out),
            vec![
                ("a".into(), 10, 1),
                ("b".into(), 20, 2),
                ("c".into(), 30, 3),
                ("d".into(), 40, 4),
            ]
        );
    }

    #[test]
    fn falls_back_to_key_based_on_instant_mismatch() {
        let mut buffer = build_buffer();
        // Positions were computed against a DIFFERENT base file → must fall back
        // to key-based merge and match by record key instead of position.
        let mut block = data_block_with_positions(&[("b", 99, 5)], &[0], "19990101000000000");
        buffer.process_data_block(&mut block).unwrap();
        assert_eq!(buffer.get_buffer_type(), BufferType::KeyBasedMerge);
        buffer.set_base_file_iterator(vec![base_batch(&[
            ("a", 10, 1),
            ("b", 20, 2),
            ("c", 30, 3),
        ])]);

        let (out, _stats) = Box::new(buffer).merge_and_collect_with_stats().unwrap();
        // Key-based: the log record (key "b") updates base row "b" regardless of
        // the position header (which pointed at 0 / key "a").
        assert_eq!(
            extract(&out),
            vec![
                ("a".into(), 10, 1),
                ("b".into(), 99, 5),
                ("c".into(), 30, 3),
            ]
        );
    }

    #[test]
    fn falls_back_to_key_based_on_missing_positions() {
        let mut buffer = build_buffer();
        // A block with no RECORD_POSITIONS header at all → fall back.
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(vec!["b"])),
                Arc::new(Int32Array::from(vec![99])),
                Arc::new(Int64Array::from(vec![5])),
            ],
        )
        .unwrap();
        let mut header = HashMap::new();
        header.insert(
            BlockMetadataKey::InstantTime,
            "20240101000001000".to_string(),
        );
        let mut block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::ParquetData,
            header,
            LogBlockContent::Records(RecordBatches::new_with_data_batches(vec![batch])),
            HashMap::new(),
        );
        buffer.process_data_block(&mut block).unwrap();
        assert_eq!(buffer.get_buffer_type(), BufferType::KeyBasedMerge);
        buffer.set_base_file_iterator(vec![base_batch(&[("a", 10, 1), ("b", 20, 2)])]);

        let (out, _stats) = Box::new(buffer).merge_and_collect_with_stats().unwrap();
        assert_eq!(
            extract(&out),
            vec![("a".into(), 10, 1), ("b".into(), 99, 5)]
        );
    }

    /// Drive the vectorized streaming path (`next_merged_base_batch` +
    /// `drain_log_only_inserts`) that production uses, not just the scalar
    /// `merge_and_collect`.
    #[test]
    fn vectorized_path_updates_and_inserts_by_position() {
        let mut buffer = build_buffer();
        // Update position 0 (key "a") and an insert at position 5.
        let mut block =
            data_block_with_positions(&[("a", 11, 9), ("x", 55, 7)], &[0, 5], BASE_INSTANT);
        buffer.process_data_block(&mut block).unwrap();
        buffer.set_base_file_iterator(vec![base_batch(&[("a", 10, 1), ("b", 20, 2)])]);

        let target = schema();
        let mut all: Vec<RecordBatch> = Vec::new();
        while let Some(b) = buffer.next_merged_base_batch(&target).unwrap() {
            all.push(b);
        }
        while let Some(b) = buffer.drain_log_only_inserts(&target).unwrap() {
            all.push(b);
        }
        let merged = arrow_select::concat::concat_batches(&target, &all).unwrap();
        assert_eq!(
            extract(&merged),
            vec![
                ("a".into(), 11, 9), // updated at position 0
                ("b".into(), 20, 2), // untouched
                ("x".into(), 55, 7), // inserted (position 5, beyond base)
            ]
        );
    }

    /// Two data blocks from consecutive commits both target the SAME base
    /// position; under COMMIT_TIME_ORDERING the later block's value must win
    /// for the base row at that position.
    #[test]
    fn test_position_duplicate_across_blocks_later_wins() {
        let mut buffer = build_buffer();
        // instant1 updates position 1 (key "b") to counter 50 ...
        let mut block1 = data_block_with_positions_at_instant(
            &[("b", 50, 10)],
            &[1],
            BASE_INSTANT,
            "20240101000001000",
        );
        buffer.process_data_block(&mut block1).unwrap();
        // ... then instant2 updates the SAME position 1 to counter 99.
        let mut block2 = data_block_with_positions_at_instant(
            &[("b", 99, 5)],
            &[1],
            BASE_INSTANT,
            "20240101000002000",
        );
        buffer.process_data_block(&mut block2).unwrap();
        buffer.set_base_file_iterator(vec![base_batch(&[
            ("a", 10, 1),
            ("b", 20, 2),
            ("c", 30, 3),
        ])]);

        let (out, stats) = Box::new(buffer).merge_and_collect_with_stats().unwrap();
        assert_eq!(
            extract(&out),
            vec![
                ("a".into(), 10, 1),
                ("b".into(), 99, 5), // instant2 (the later commit) wins at position 1
                ("c".into(), 30, 3),
            ]
        );
        assert_eq!(stats.num_updates, 1, "one base row updated exactly once");
    }

    /// A log record whose position is >= the base row count never matches a
    /// base row: it must drain as a log-only insert with its exact payload,
    /// and every base row must pass through unaffected. Drives the vectorized
    /// streaming path so base rows and inserts are asserted separately.
    #[test]
    fn test_position_out_of_range_drains_as_log_only_insert() {
        let mut buffer = build_buffer();
        // Base has 3 rows (positions 0..3); position 7 is out of range.
        let mut block = data_block_with_positions(&[("z", 77, 9)], &[7], BASE_INSTANT);
        buffer.process_data_block(&mut block).unwrap();
        buffer.set_base_file_iterator(vec![base_batch(&[
            ("a", 10, 1),
            ("b", 20, 2),
            ("c", 30, 3),
        ])]);

        let target = schema();
        let mut base_out: Vec<RecordBatch> = Vec::new();
        while let Some(b) = buffer.next_merged_base_batch(&target).unwrap() {
            base_out.push(b);
        }
        let base_merged = arrow_select::concat::concat_batches(&target, &base_out).unwrap();
        assert_eq!(
            extract(&base_merged),
            vec![
                ("a".into(), 10, 1),
                ("b".into(), 20, 2),
                ("c".into(), 30, 3),
            ],
            "no base row may be touched by the out-of-range position"
        );

        let mut insert_out: Vec<RecordBatch> = Vec::new();
        while let Some(b) = buffer.drain_log_only_inserts(&target).unwrap() {
            insert_out.push(b);
        }
        let inserts = arrow_select::concat::concat_batches(&target, &insert_out).unwrap();
        assert_eq!(
            extract(&inserts),
            vec![("z".into(), 77, 9)],
            "the out-of-range record must drain as a log-only insert with its exact payload"
        );
    }

    /// Base handed over as MULTIPLE batches through the streaming source
    /// (`set_base_file_source` with a `RecordBatchIterator`), like a
    /// multi-row-group parquet read. Log positions are absolute within the
    /// base FILE, so updates targeting rows in the 2nd/3rd batch must land on
    /// the correct global rows — this pins the global-offset bookkeeping (the
    /// row-index column carries absolute positions, not per-batch ones).
    #[test]
    fn test_position_merge_across_multi_row_group_base() {
        let mut buffer = build_buffer();
        // Update global position 3 (batch 2, key "d") and 5 (batch 3, key "f"),
        // and delete global position 2 (batch 2, key "c").
        let mut block =
            data_block_with_positions(&[("d", 44, 9), ("f", 66, 9)], &[3, 5], BASE_INSTANT);
        buffer.process_data_block(&mut block).unwrap();
        let mut delete = delete_block_with_positions(&["c"], &[2], BASE_INSTANT);
        buffer.process_delete_block(&mut delete).unwrap();

        let batches = vec![
            base_batch_at(&[("a", 10, 1), ("b", 20, 2)], 0),
            base_batch_at(&[("c", 30, 3), ("d", 40, 4)], 2),
            base_batch_at(&[("e", 50, 5), ("f", 60, 6)], 4),
        ];
        let base_schema = batches[0].schema();
        buffer.set_base_file_source(Box::new(arrow_array::RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            base_schema,
        )));

        let (out, stats) = Box::new(buffer).merge_and_collect_with_stats().unwrap();
        assert_eq!(
            extract(&out),
            vec![
                ("a".into(), 10, 1), // batch 1: untouched
                ("b".into(), 20, 2), // batch 1: untouched
                ("d".into(), 44, 9), // batch 2: updated at global position 3
                ("e".into(), 50, 5), // batch 3: untouched
                ("f".into(), 66, 9), // batch 3: updated at global position 5
            ],
            "updates must land on absolute base-file positions; \"c\" (position 2) deleted"
        );
        assert_eq!(stats.num_updates, 2);
        assert_eq!(stats.num_deletes, 1);
    }

    /// The key-based fallback must re-key EVERY buffered entry — including ones
    /// that spilled to the on-disk tier under a tiny merge budget. The merged
    /// output must be identical to the same scenario without spill.
    #[test]
    fn test_position_fallback_with_spilled_map_rekeys_all() {
        const N: usize = 200;

        /// Run the scenario: one big position-keyed block (spills under a tiny
        /// budget), then a block whose base-instant header mismatches →
        /// fallback re-keys everything; merge against a base holding stale
        /// values for every key. Returns (sorted output, spill fired).
        fn run(reader_config: &[(&str, &str)]) -> (Vec<(String, i32, i64)>, bool) {
            let mut buffer = build_buffer_with_reader_config(reader_config);
            // Block 1 (valid positions 0..N): every key updated to (1000+i, 1).
            let rows: Vec<(String, i32, i64)> = (0..N)
                .map(|i| (format!("k{i:04}"), 1000 + i as i32, 1))
                .collect();
            let refs: Vec<(&str, i32, i64)> =
                rows.iter().map(|(k, c, t)| (k.as_str(), *c, *t)).collect();
            let positions: Vec<u64> = (0..N as u64).collect();
            let mut block1 = data_block_with_positions_at_instant(
                &refs,
                &positions,
                BASE_INSTANT,
                "20240101000001000",
            );
            buffer.process_data_block(&mut block1).unwrap();
            let spilled = buffer.inner.base.records.spill_fired();

            // Block 2: positions computed against a DIFFERENT base file →
            // fallback re-keys all N buffered entries (spilled ones included),
            // then processes this block key-based.
            let mut block2 = data_block_with_positions_at_instant(
                &[("k0005", 9999, 2), ("zznew", 7, 2)],
                &[5, 200],
                "19990101000000000",
                "20240101000002000",
            );
            buffer.process_data_block(&mut block2).unwrap();
            assert_eq!(buffer.get_buffer_type(), BufferType::KeyBasedMerge);

            // Base: stale (-1, -1) values for every key — the log must win all.
            let base_rows: Vec<(String, i32, i64)> =
                (0..N).map(|i| (format!("k{i:04}"), -1, -1)).collect();
            let base_refs: Vec<(&str, i32, i64)> = base_rows
                .iter()
                .map(|(k, c, t)| (k.as_str(), *c, *t))
                .collect();
            buffer.set_base_file_iterator(vec![base_batch(&base_refs)]);

            let (out, _stats) = Box::new(buffer).merge_and_collect_with_stats().unwrap();
            (extract(&out), spilled)
        }

        // Baseline: default 1 GiB budget → no spill.
        let (baseline, baseline_spilled) = run(&[]);
        assert!(
            !baseline_spilled,
            "baseline (default budget) must not spill for this workload"
        );
        // Full-data expectation: every key carries block 1's value, except
        // "k0005" (overwritten key-based by block 2) plus the new "zznew".
        let mut expected: Vec<(String, i32, i64)> = (0..N)
            .map(|i| (format!("k{i:04}"), 1000 + i as i32, 1))
            .collect();
        expected[5] = ("k0005".to_string(), 9999, 2);
        expected.push(("zznew".to_string(), 7, 2));
        assert_eq!(baseline, expected);

        // Spill-engaged: a 1 KiB budget forces position-keyed entries to the
        // on-disk tier BEFORE the fallback re-keys them.
        let (spilled_out, did_spill) = run(&[("hoodie.memory.merge.max.size", "1024")]);
        assert!(
            did_spill,
            "a 1 KiB merge budget must spill position-keyed entries before the fallback"
        );
        assert_eq!(
            spilled_out, baseline,
            "fallback over a spilled map must produce output identical to the no-spill run"
        );
    }

    /// REGRESSION (PR #95 review major, fixed): a position-keyed DELETE tombstone
    /// that spills to the RocksDB tier must still suppress its target row after
    /// key-based fallback. A delete's disk entry stores no payload, so before the
    /// fix `spillable_map::record_from_entry` reconstructed `record_key` from the
    /// MAP key — which in this buffer is the POSITION string (e.g. "3"), not the
    /// record key ("k00003"). `fallback_to_key_based` then re-inserted the
    /// tombstone under "3"; it never matched its target base row, so the deleted
    /// row silently reappeared. Fixed by persisting the true `record_key` in the
    /// delete's spill entry (`DiskLoc::Delete { record_key }`) and returning it
    /// from `record_from_entry`. This asserts the deleted row stays deleted in
    /// BOTH the spill and no-spill runs (Java parity); the no-spill control also
    /// guards against a change that would mask the spill-specific path.
    #[test]
    fn test_spilled_position_delete_survives_fallback_repro() {
        /// Run the scenario: a position-keyed delete for base position 3
        /// (record key "k00003" — key ≠ its position string "3"), then a data
        /// block whose base-instant header mismatches → fallback re-keys the
        /// buffered tombstone; merge against a base containing the deleted key.
        /// Returns (sorted output, spill fired).
        fn run(reader_config: &[(&str, &str)]) -> (Vec<(String, i32, i64)>, bool) {
            let mut buffer = build_buffer_with_reader_config(reader_config);
            // Delete the row at base position 3 (record key "k00003").
            let mut delete = delete_block_with_positions(&["k00003"], &[3], BASE_INSTANT);
            buffer.process_delete_block(&mut delete).unwrap();
            let spilled = buffer.inner.base.records.spill_fired();

            // A later block whose positions were computed against a DIFFERENT
            // base file → fallback_to_key_based re-keys the tombstone, then
            // this block is processed key-based (updates "k00001").
            let mut block = data_block_with_positions_at_instant(
                &[("k00001", 99, 5)],
                &[1],
                "19990101000000000",
                "20240101000002000",
            );
            buffer.process_data_block(&mut block).unwrap();
            assert_eq!(buffer.get_buffer_type(), BufferType::KeyBasedMerge);

            buffer.set_base_file_iterator(vec![base_batch(&[
                ("k00000", 10, 1),
                ("k00001", 20, 2),
                ("k00002", 30, 3),
                ("k00003", 40, 4),
            ])]);

            let (out, _stats) = Box::new(buffer).merge_and_collect_with_stats().unwrap();
            (extract(&out), spilled)
        }

        // Java-correct expectation: "k00003" (deleted at position 3) is ABSENT;
        // "k00001" carries the post-fallback key-based update.
        let expected: Vec<(String, i32, i64)> = vec![
            ("k00000".into(), 10, 1),
            ("k00001".into(), 99, 5),
            ("k00002".into(), 30, 3),
        ];

        // Control: default (1 GiB) budget → the tombstone stays in the
        // in-memory tier, whose entries keep their true record_key; the
        // fallback re-keys it correctly and the delete lands. This must pass —
        // it proves the bug is spill-specific.
        let (control, control_spilled) = run(&[]);
        assert!(!control_spilled, "control (default budget) must not spill");
        assert_eq!(
            control, expected,
            "no-spill control: the position-keyed delete must suppress \"k00003\" after fallback"
        );

        // Spill-engaged: a 1 KiB budget routes the tombstone straight to the
        // RocksDB tier before the fallback. EXPECTED TO FAIL against current
        // code: the tombstone comes back keyed "3" and "k00003" is resurrected.
        let (spilled_out, did_spill) = run(&[("hoodie.memory.merge.max.size", "1024")]);
        assert!(
            did_spill,
            "a 1 KiB merge budget must spill the position-keyed delete tombstone"
        );
        assert_eq!(
            spilled_out, expected,
            "spilled position-keyed delete tombstone must still suppress its target row after \
             key-based fallback (Java parity) — if this row is present, the tombstone was \
             re-keyed by its position string (spillable_map::record_from_entry) and silently \
             failed to delete"
        );
    }
}
