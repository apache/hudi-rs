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
//! ## Vectorized position-based merge
//!
//! Under commit-time or event-time ordering, with no partial-update rule and no
//! delete marker other than `_hoodie_is_deleted`, a log record at a position
//! either replaces the base row there whole or leaves it, and a delete either
//! removes it or is obsolete. While that holds, log blocks are kept as they
//! arrive instead of being turned into records one by one. When the base is
//! read, they are folded in log order into one sorted position list: under
//! commit-time ordering a later block wins, under event-time ordering the fold
//! follows the event-time merger's rules on each record's ordering value. Each
//! base batch is then merged by finding that list's positions among the batch's
//! row positions. Under event-time ordering the base row's ordering value is
//! read only at those matched rows and compared with the log record's. The rows
//! the log wins are masked out and the log rows that replace them are gathered
//! in one call. The record map is never touched. Anything outside
//! those conditions (a partial block, a block without usable positions, a
//! caller using the row-at-a-time path) replays the kept blocks into the map
//! and continues the ordinary way.
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

use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_buffer::BooleanBufferBuilder;
use arrow_schema::SchemaRef;

use crate::Result;
use crate::error::CoreError;
use crate::file_group::log_file::content::PARTIAL_UPDATE_SCHEMA_KEY;
use crate::file_group::log_file::log_block::{LogBlock, LogBlockContent};
use crate::file_group::reader_v2::buffer::key_based::{BaseMatch, KeyBasedFileGroupRecordBuffer};
use crate::file_group::reader_v2::buffer::record_positions::{
    base_row_position_array, decode_record_positions,
};
use crate::file_group::reader_v2::buffer::row_extraction::{
    reconcile_batch_to_schema, records_to_batch,
};
use crate::file_group::reader_v2::buffer::spillable_map::{SpillConfig, SpillableRecordMap};
use crate::file_group::reader_v2::buffer::{BufferType, HoodieFileGroupRecordBuffer};
use crate::file_group::reader_v2::buffered_record::{BufferedRecord, DeleteRecord, OrderingValue};
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
    /// The vectorized merge's state while it applies (see the module docs);
    /// `None` once the buffer merges through the record map.
    vectorized: Option<VectorizedPositionMerge>,
}

/// Log blocks kept for the vectorized merge, and the position list built from them.
#[derive(Debug)]
struct VectorizedPositionMerge {
    blocks: Vec<PositionMergeBlock>,
    index: Option<PositionMergeIndex>,
    /// Bytes of log batches kept so far.
    bytes: usize,
    /// The merge map's in-memory budget. The kept blocks can't spill, so past
    /// this they are handed to the record map, which can.
    budget: u64,
}

/// One log block as it arrived, with its validated record positions.
#[derive(Debug)]
enum PositionMergeBlock {
    Data {
        positions: Vec<u64>,
        batches: Vec<RecordBatch>,
    },
    Delete {
        positions: Vec<u64>,
        batches: Vec<(RecordBatch, String)>,
    },
}

/// What the newest log record at a position does to the base row there.
#[derive(Clone, Copy, Debug)]
enum PositionMergeSlot {
    /// Replace it with row `row` of `PositionMergeIndex::data[batch]`.
    Update { batch: u32, row: u32 },
    /// Remove it.
    Delete,
}

/// The kept log blocks folded into one list, sorted by position.
#[derive(Debug)]
struct PositionMergeIndex {
    entries: Vec<(u64, PositionMergeSlot)>,
    /// Whether entry `i` met its base row; an update that never did is emitted
    /// on its own when the base is exhausted, as the record map would.
    consumed: Vec<bool>,
    /// The log data batches, reconciled to the merge schema.
    data: Vec<RecordBatch>,
    /// Under event-time ordering, each entry's ordering value (aligned with
    /// `entries`); empty under commit-time ordering, which never compares.
    orderings: Vec<Option<OrderingValue>>,
    /// The next entry the drain of unmatched updates looks at.
    drain_from: usize,
    /// Counts for the read's update statistics, as the record map keeps them.
    stats: UpdateStats,
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
        let mut inner =
            KeyBasedFileGroupRecordBuffer::new(reader_context, merge_mode, emit_delete)?;
        inner.position_filter = Some(roaring::RoaringTreemap::new());
        let vectorized = if vectorized_merge_applies(&inner, emit_delete) {
            let budget = SpillConfig::from_config(&inner.reader_context.hoodie_reader_config)?
                .max_in_memory_size;
            Some(VectorizedPositionMerge {
                blocks: Vec::new(),
                index: None,
                bytes: 0,
                budget,
            })
        } else {
            None
        };
        Ok(Self {
            inner,
            base_file_instant_time,
            should_use_position: true,
            needs_hybrid_strategy: false,
            hybrid_deletes: HashMap::new(),
            vectorized,
        })
    }

    /// The map key for a base-file position: its decimal string. Positions and
    /// record keys never share the map at the same time (positions before
    /// fallback, record keys after), so the string domains do not collide.
    fn position_key(position: u64) -> String {
        position.to_string()
    }

    /// Record that `position` has a buffered log record, so the merge kernel
    /// looks base rows at that position up in the merge map.
    fn note_position(&mut self, position: u64) {
        if let Some(filter) = self.inner.position_filter.as_mut() {
            filter.insert(position);
        }
    }

    /// [`Self::note_position`] for a map key handed in from outside the
    /// block loops. While the buffer merges by position every key is a
    /// decimal position. A key that does not parse can't be skipped safely,
    /// so the filter is dropped and every base row is looked up.
    fn note_position_key(&mut self, key: &str) {
        if !self.should_use_position {
            return;
        }
        match key.parse::<u64>() {
            Ok(position) => self.note_position(position),
            Err(_) => self.inner.position_filter = None,
        }
    }

    /// Buffer a data block's records in the record map, keyed by position.
    fn ingest_data_records(&mut self, positions: &[u64], batches: Vec<RecordBatch>) -> Result<()> {
        let mut pos_idx = 0usize;
        for batch in batches {
            let batch = Arc::new(batch);
            // `record_context` and `base.delete_context` are disjoint fields of
            // `inner`, so these two immutable borrows coexist; the owned
            // `records` Vec ends both borrows before the `&mut` calls below.
            let records = self
                .inner
                .record_context
                .batch_to_buffered_records(&batch, self.inner.base.delete_context.as_ref())?;
            // Index-zip: `positions[pos_idx]` is the base-file position of the
            // `pos_idx`-th record in file order. `decode_record_positions`
            // returns the Roaring64 bitmap ascending, so this pairing is only
            // correct because the Hudi writer emits records within a block in
            // ascending base-position order, the same invariant Java's
            // `PositionBasedFileGroupRecordBuffer` relies on with
            // `recordPositions.get(recordIndex++)`. The caller checked that the
            // counts agree.
            for (_record_key, record) in records {
                let position = positions[pos_idx];
                pos_idx += 1;
                self.note_position(position);
                self.inner
                    .process_next_data_record(record, &Self::position_key(position))?;
            }
        }
        Ok(())
    }

    /// Buffer a delete block's records in the record map, keyed by position.
    fn ingest_delete_records(
        &mut self,
        positions: &[u64],
        batches: Vec<(RecordBatch, String)>,
    ) -> Result<()> {
        let mut pos_idx = 0usize;
        for (batch, _inst) in batches {
            let delete_entries = self
                .inner
                .record_context
                .delete_batch_to_keys_with_ordering(&batch)?;
            // Same ascending-order index-zip invariant as for data records.
            for (record_key, ordering_value) in delete_entries {
                let position = positions[pos_idx];
                pos_idx += 1;
                // Java sets the record key on the delete so a later fallback
                // can re-key it; `DeleteRecord` carries it, and
                // `process_next_deleted_record` keys the map by position.
                let delete_record = DeleteRecord {
                    record_key,
                    ordering_value,
                };
                self.note_position(position);
                self.inner
                    .process_next_deleted_record(delete_record, &Self::position_key(position))?;
            }
        }
        Ok(())
    }

    /// Keep a block for the vectorized merge, or buffer it in the record map when
    /// the vectorized merge no longer applies. Past the merge map's budget the
    /// kept blocks move to the record map, which can spill.
    fn keep_or_ingest(&mut self, block: PositionMergeBlock, bytes: usize) -> Result<()> {
        if let Some(vectorized) = self.vectorized.as_mut() {
            vectorized.bytes += bytes;
            vectorized.blocks.push(block);
            if vectorized.bytes as u64 > vectorized.budget {
                self.leave_position_merge()?;
            }
            return Ok(());
        }
        match block {
            PositionMergeBlock::Data { positions, batches } => {
                self.ingest_data_records(&positions, batches)
            }
            PositionMergeBlock::Delete { positions, batches } => {
                self.ingest_delete_records(&positions, batches)
            }
        }
    }

    /// Leave the vectorized merge: buffer the kept blocks in the record map, in the
    /// order they arrived, and merge through it from here on. Only valid while
    /// the blocks are still kept: once they are folded into the position list
    /// (the first base batch, or the drain, folds them), there are no blocks
    /// left to replay and leaving is an error.
    fn leave_position_merge(&mut self) -> Result<()> {
        let Some(vectorized) = self.vectorized.take() else {
            return Ok(());
        };
        if vectorized.index.is_some() {
            return Err(CoreError::ReadFileSliceError(
                "internal: the vectorized position-based merge was left after its position list was built"
                    .to_string(),
            ));
        }
        for block in vectorized.blocks {
            match block {
                PositionMergeBlock::Data { positions, batches } => {
                    self.ingest_data_records(&positions, batches)?
                }
                PositionMergeBlock::Delete { positions, batches } => {
                    self.ingest_delete_records(&positions, batches)?
                }
            }
        }
        Ok(())
    }

    /// Fold the kept blocks into the sorted position list, the first time the
    /// base is merged. Returns `false`, after leaving the vectorized merge, when a
    /// data block can't take the vectorized merge: one that lacks a column of the
    /// merge schema is partial in all but name.
    fn build_position_merge_index(&mut self, target_schema: &SchemaRef) -> Result<bool> {
        let Some(vectorized) = self.vectorized.as_ref() else {
            return Ok(false);
        };
        if vectorized.index.is_some() {
            return Ok(true);
        }
        let complete = vectorized.blocks.iter().all(|block| match block {
            PositionMergeBlock::Data { batches, .. } => batches.iter().all(|b| {
                let schema = b.schema();
                target_schema
                    .fields()
                    .iter()
                    .all(|f| schema.column_with_name(f.name()).is_some())
            }),
            PositionMergeBlock::Delete { .. } => true,
        });
        if !complete {
            self.leave_position_merge()?;
            return Ok(false);
        }
        let vectorized = self.vectorized.as_mut().expect("checked above");
        let total: usize = vectorized
            .blocks
            .iter()
            .map(|b| match b {
                PositionMergeBlock::Data { positions, .. }
                | PositionMergeBlock::Delete { positions, .. } => positions.len(),
            })
            .sum();
        let event_time = self.inner.base.record_merge_mode == "EVENT_TIME_ORDERING";
        let record_context = &self.inner.record_context;
        let mut slots: HashMap<u64, (PositionMergeSlot, Option<OrderingValue>)> =
            HashMap::with_capacity(total);
        let mut data: Vec<RecordBatch> = Vec::new();
        for block in &vectorized.blocks {
            match block {
                PositionMergeBlock::Data { positions, batches } => {
                    let mut pos_idx = 0usize;
                    for batch in batches {
                        let deleted = built_in_delete_mask(batch);
                        let orderings = if event_time {
                            Some(record_context.ordering_accessor(batch)?)
                        } else {
                            None
                        };
                        let batch_idx = data.len() as u32;
                        data.push(reconcile_batch_to_schema(batch, target_schema)?);
                        for row in 0..batch.num_rows() {
                            let slot = match &deleted {
                                Some(mask) if mask.is_valid(row) && mask.value(row) => {
                                    PositionMergeSlot::Delete
                                }
                                _ => PositionMergeSlot::Update {
                                    batch: batch_idx,
                                    row: row as u32,
                                },
                            };
                            let ordering = orderings.as_ref().and_then(|o| o.value_at(row));
                            fold_slot(
                                &mut slots,
                                positions[pos_idx],
                                slot,
                                ordering,
                                event_time,
                                LogSource::DataBlock,
                            );
                            pos_idx += 1;
                        }
                    }
                }
                PositionMergeBlock::Delete { positions, batches } => {
                    if event_time {
                        let mut pos_idx = 0usize;
                        for (batch, _) in batches {
                            for (_, ordering) in
                                record_context.delete_batch_to_keys_with_ordering(batch)?
                            {
                                fold_slot(
                                    &mut slots,
                                    positions[pos_idx],
                                    PositionMergeSlot::Delete,
                                    ordering,
                                    true,
                                    LogSource::DeleteBlock,
                                );
                                pos_idx += 1;
                            }
                        }
                    } else {
                        for &position in positions {
                            slots.insert(position, (PositionMergeSlot::Delete, None));
                        }
                    }
                }
            }
        }
        let mut folded: Vec<(u64, (PositionMergeSlot, Option<OrderingValue>))> =
            slots.into_iter().collect();
        folded.sort_unstable_by_key(|(position, _)| *position);
        let mut entries: Vec<(u64, PositionMergeSlot)> = Vec::with_capacity(folded.len());
        let mut orderings: Vec<Option<OrderingValue>> = if event_time {
            Vec::with_capacity(folded.len())
        } else {
            Vec::new()
        };
        for (position, (slot, ordering)) in folded {
            entries.push((position, slot));
            if event_time {
                orderings.push(ordering);
            }
        }
        let consumed = vec![false; entries.len()];
        vectorized.blocks.clear();
        vectorized.index = Some(PositionMergeIndex {
            entries,
            consumed,
            data,
            orderings,
            drain_from: 0,
            stats: UpdateStats::default(),
        });
        Ok(true)
    }

    /// Merge one base batch by position: mask out the rows the position list
    /// names and append the log rows that replace them.
    fn vectorized_position_merge(
        &mut self,
        base: &RecordBatch,
        target_schema: &SchemaRef,
    ) -> Result<Option<RecordBatch>> {
        let num_rows = base.num_rows();
        if num_rows == 0 {
            return Ok(None);
        }
        let positions = base_row_position_array(base)?;
        let base_orderings = if self.inner.base.record_merge_mode == "EVENT_TIME_ORDERING" {
            Some(self.inner.record_context.ordering_accessor(base)?)
        } else {
            None
        };
        let index = self
            .vectorized
            .as_mut()
            .and_then(|d| d.index.as_mut())
            .expect("the vectorized index is built before the base is merged");
        let values = positions.values();
        let ascending = positions.null_count() == 0 && values.windows(2).all(|w| w[0] <= w[1]);
        let (lo, hi) = match (
            arrow::compute::min(positions),
            arrow::compute::max(positions),
        ) {
            (Some(lo), Some(hi)) => (lo.max(0) as u64, hi.max(0) as u64),
            _ => (0, 0),
        };
        let mut keep: Option<BooleanBufferBuilder> = None;
        let mut picks: Vec<(usize, usize)> = Vec::new();
        let start = index.entries.partition_point(|(p, _)| *p < lo);
        for i in start..index.entries.len() {
            let (position, slot) = index.entries[i];
            if position > hi {
                break;
            }
            let wanted = position as i64;
            let row = if ascending {
                values.binary_search(&wanted).ok()
            } else {
                (0..num_rows).find(|&r| positions.is_valid(r) && values[r] == wanted)
            };
            let Some(row) = row else {
                continue;
            };
            if let Some(base_orderings) = &base_orderings
                && !log_beats_base(slot, &index.orderings[i], &base_orderings.value_at(row))
            {
                // The base row is newer: it stays, and the log record is spent
                // (the record map would have removed it on this match).
                index.consumed[i] = true;
                index.stats.num_updates += 1;
                continue;
            }
            let mask = keep.get_or_insert_with(|| {
                let mut b = BooleanBufferBuilder::new(num_rows);
                b.append_n(num_rows, true);
                b
            });
            mask.set_bit(row, false);
            index.consumed[i] = true;
            match slot {
                PositionMergeSlot::Update { batch, row } => {
                    index.stats.num_updates += 1;
                    picks.push((batch as usize, row as usize));
                }
                PositionMergeSlot::Delete => index.stats.num_deletes += 1,
            }
        }
        let Some(mut keep) = keep else {
            return Ok(Some(if base.schema() == *target_schema {
                base.clone()
            } else {
                reconcile_batch_to_schema(base, target_schema)?
            }));
        };
        let mask = BooleanArray::new(keep.finish(), None);
        let kept = arrow::compute::filter_record_batch(base, &mask).map_err(|e| {
            CoreError::ReadFileSliceError(format!(
                "vectorized position-based merge: filter failed: {e}"
            ))
        })?;
        let kept = reconcile_batch_to_schema(&kept, target_schema)?;
        if picks.is_empty() {
            return Ok(Some(kept));
        }
        let updates = gather(&index.data, &picks)?;
        arrow::compute::concat_batches(target_schema, [&kept, &updates])
            .map(Some)
            .map_err(|e| {
                CoreError::ReadFileSliceError(format!(
                    "vectorized position-based merge: concat failed: {e}"
                ))
            })
    }

    /// The updates whose base row the base read never produced, once the base
    /// is exhausted, at most [`DEFAULT_BATCH_SIZE`] rows per call. The record
    /// map emits such records as log-only inserts in chunks of the same size.
    fn drain_position_merge(&mut self, target_schema: &SchemaRef) -> Result<Option<RecordBatch>> {
        if self.vectorized.as_ref().is_some_and(|d| d.index.is_none()) {
            self.build_position_merge_index(target_schema)?;
        }
        let Some(index) = self.vectorized.as_mut().and_then(|d| d.index.as_mut()) else {
            return self.inner.drain_log_only_inserts(target_schema);
        };
        let mut picks: Vec<(usize, usize)> = Vec::new();
        while index.drain_from < index.entries.len() && picks.len() < DEFAULT_BATCH_SIZE {
            let i = index.drain_from;
            index.drain_from += 1;
            if let (_, PositionMergeSlot::Update { batch, row }) = index.entries[i]
                && !index.consumed[i]
            {
                picks.push((batch as usize, row as usize));
            }
        }
        index.stats.num_inserts += picks.len() as u64;
        if picks.is_empty() {
            return Ok(None);
        }
        gather(&index.data, &picks).map(Some)
    }

    /// Log records kept for the vectorized merge.
    fn position_merge_record_count(&self) -> usize {
        match &self.vectorized {
            Some(VectorizedPositionMerge {
                index: Some(index), ..
            }) => index.entries.len(),
            Some(VectorizedPositionMerge { blocks, .. }) => blocks
                .iter()
                .map(|b| match b {
                    PositionMergeBlock::Data { positions, .. }
                    | PositionMergeBlock::Delete { positions, .. } => positions.len(),
                })
                .sum(),
            None => 0,
        }
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
        self.inner.position_filter = None;

        // Rebuild the (possibly-spilled) position-keyed map as a record-key-keyed
        // map. `drain_iter` yields values; the position keys are dropped.
        //
        // A DATA record's `record_key` field is NOT trustworthy here: the spill
        // tier reconstructs a drained data record's `record_key` from its MAP
        // key (see `RocksDbDiskMap::record_from_entry`), which in this buffer is
        // the POSITION string, not the record key. So for data records the true
        // key is re-extracted from the payload row itself. A DELETE tombstone
        // has no payload to re-extract from, but its `record_key` IS
        // trustworthy: hudi-rs delete blocks always carry record keys,
        // `process_next_deleted_record` preserves them in the in-memory tier,
        // and the spill tier persists a delete's real key
        // (`DiskLoc::Delete { record_key }`) and restores it on drain. A delete
        // that still lacks a record key cannot be re-keyed — Java keeps it
        // position-keyed under the hybrid strategy, but that requires the
        // position, which `drain_iter` does not surface — so fail loudly rather
        // than silently drop a delete (which would resurrect a deleted row).
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
                self.reinsert_rekeyed(key, record)?;
            } else if !record.record_key.is_empty() {
                let key = record.record_key.clone();
                self.reinsert_rekeyed(key, record)?;
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

    /// Re-insert a record under its record key during a fallback, refusing to
    /// drop one on the floor.
    ///
    /// Two position-keyed entries can only carry the same record key when the
    /// base file holds that key at more than one position. A plain insert then
    /// overwrites, and since the map's iteration order decides which one survives,
    /// a committed update disappears from the read — nondeterministically, and
    /// with no error. Java's `fallbackToKeyBasedBuffer` overwrites here; refusing
    /// is a deliberate difference, because the operator can act on an error
    /// (compact the file group, or turn position merge off) and cannot act on rows
    /// that are quietly missing.
    fn reinsert_rekeyed(&mut self, key: String, record: BufferedRecord) -> Result<()> {
        if self.inner.base.records.contains_key(&key)? {
            return Err(CoreError::ReadFileSliceError(format!(
                "position-based merge fell back to key-based, but record key '{key}' occupies \
                 more than one position in the base file, so re-keying would discard one of the \
                 log records buffered for it. Merging by key cannot say which base row each \
                 belongs to. Compact this file group, or read it with \
                 hoodie.merge.use.record.positions=false to merge by key from the start."
            )));
        }
        self.inner.base.records.insert(key, record)
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

/// Whether the vectorized merge can apply to this buffer at all: commit-time or
/// event-time ordering (a log record wins or loses whole, decided from ordering
/// values alone), no partial-update rule, deletes marked only by delete blocks
/// or `_hoodie_is_deleted`, and no emitted deletes. Every other merge mode,
/// including a custom merger or payload that needs the full rows, merges row by
/// row through the record map.
fn vectorized_merge_applies(inner: &KeyBasedFileGroupRecordBuffer, emit_delete: bool) -> bool {
    let delete_markers_ok =
        inner.base.delete_context.as_ref().is_none_or(|ctx| {
            ctx.custom_delete_marker.is_none() && ctx.hoodie_operation_pos.is_none()
        });
    !emit_delete
        && matches!(
            inner.base.record_merge_mode.as_str(),
            "COMMIT_TIME_ORDERING" | "EVENT_TIME_ORDERING"
        )
        && !inner.has_partial_update_rules()
        && delete_markers_ok
}

/// Rows of a log data batch whose `_hoodie_is_deleted` is true, if it has
/// that column as a boolean.
fn built_in_delete_mask(batch: &RecordBatch) -> Option<BooleanArray> {
    batch
        .column_by_name("_hoodie_is_deleted")
        .and_then(|c| c.as_any().downcast_ref::<BooleanArray>().cloned())
}

/// The kind of log block a record came from. The record map folds a data
/// block's records, including rows whose `_hoodie_is_deleted` is true, through
/// `delta_merge`, and a delete block's records through `delta_merge_delete`, so
/// the fold has to know which one it is replaying.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LogSource {
    DataBlock,
    DeleteBlock,
}

/// Fold one log record into the position list. Under commit-time ordering a
/// later record replaces an earlier one at the same position. Under event-time
/// ordering the fold mirrors `EventTimeRecordMerger`: a record from a data
/// block (a deleted row included) replaces the earlier entry when
/// `should_keep_newer_record` would keep it, and a record from a delete block
/// replaces it unless `delta_merge_delete` would keep the earlier entry.
fn fold_slot(
    slots: &mut HashMap<u64, (PositionMergeSlot, Option<OrderingValue>)>,
    position: u64,
    slot: PositionMergeSlot,
    ordering: Option<OrderingValue>,
    event_time: bool,
    source: LogSource,
) {
    match slots.entry(position) {
        std::collections::hash_map::Entry::Vacant(e) => {
            e.insert((slot, ordering));
        }
        std::collections::hash_map::Entry::Occupied(mut e) => {
            let (old_slot, old_ordering) = e.get();
            if !event_time || newer_log_wins(*old_slot, old_ordering, slot, &ordering, source) {
                e.insert((slot, ordering));
            }
        }
    }
}

/// Whether a newer log record replaces an older one at the same position under
/// event-time ordering.
fn newer_log_wins(
    old_slot: PositionMergeSlot,
    old_ordering: &Option<OrderingValue>,
    new_slot: PositionMergeSlot,
    new_ordering: &Option<OrderingValue>,
    source: LogSource,
) -> bool {
    let old_is_commit_time_delete =
        matches!(old_slot, PositionMergeSlot::Delete) && is_default_ordering(old_ordering);
    match source {
        // `delta_merge_delete`: an earlier commit-time-ordering delete stays; the
        // earlier record survives only when the delete has a non-default ordering
        // value of the same class that it strictly exceeds.
        LogSource::DeleteBlock => {
            if old_is_commit_time_delete {
                return false;
            }
            is_default_ordering(new_ordering)
                || !matches!((new_ordering, old_ordering),
                    (Some(d), Some(o)) if d.is_same_class(o) && o > d)
        }
        // `should_keep_newer_record`: a commit-time-ordering delete on either side
        // keeps the newer record; otherwise the newer wins on `>=` or a
        // cross-class pair.
        LogSource::DataBlock => {
            let new_is_commit_time_delete =
                matches!(new_slot, PositionMergeSlot::Delete) && is_default_ordering(new_ordering);
            if new_is_commit_time_delete || old_is_commit_time_delete {
                return true;
            }
            match (new_ordering, old_ordering) {
                (Some(n), Some(o)) => !n.is_same_class(o) || n >= o,
                _ => true,
            }
        }
    }
}

/// Whether the folded log record at a position beats the base row there under
/// event-time ordering. Mirrors `pick_winner`: a data record wins on `>=`, a
/// cross-class pair or a missing ordering value; a delete applies unless it has
/// a non-default ordering value of the base's class that the base strictly
/// exceeds.
fn log_beats_base(
    slot: PositionMergeSlot,
    log_ordering: &Option<OrderingValue>,
    base_ordering: &Option<OrderingValue>,
) -> bool {
    match slot {
        PositionMergeSlot::Delete => !matches!((log_ordering, base_ordering),
            (Some(d), Some(b)) if !d.is_default() && d.is_same_class(b) && b > d),
        PositionMergeSlot::Update { .. } => match (log_ordering, base_ordering) {
            (Some(l), Some(b)) => !l.is_same_class(b) || l >= b,
            _ => true,
        },
    }
}

/// `None` or the null-coerced default: the natural-order ordering value.
fn is_default_ordering(ordering: &Option<OrderingValue>) -> bool {
    ordering.as_ref().is_none_or(|o| o.is_default())
}

/// Gather rows `(batch, row)` of `data` into one batch, in that order.
fn gather(data: &[RecordBatch], picks: &[(usize, usize)]) -> Result<RecordBatch> {
    let refs: Vec<&RecordBatch> = data.iter().collect();
    arrow_select::interleave::interleave_record_batch(&refs, picks).map_err(|e| {
        CoreError::ReadFileSliceError(format!(
            "vectorized position-based merge: gather failed: {e}"
        ))
    })
}

/// The error for a block whose record count differs from its position count.
fn record_count_mismatch(kind: &str, rows: usize, positions: usize) -> CoreError {
    if rows > positions {
        CoreError::LogBlockError(format!(
            "position-based merge: {kind} block has more records than positions \
             ({positions} positions for {rows} records)"
        ))
    } else {
        CoreError::LogBlockError(format!(
            "position-based merge: {kind} block record count {rows} != position count {positions}"
        ))
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
                self.leave_position_merge()?;
                self.fallback_to_key_based()?;
                return self.inner.process_data_block(block);
            }
        };

        if let LogBlockContent::Records(record_batches) = std::mem::take(&mut block.content) {
            let batches = record_batches.data_batches;
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            if rows != positions.len() {
                return Err(record_count_mismatch("data", rows, positions.len()));
            }
            let partial = batches.iter().any(|b| {
                b.schema()
                    .metadata()
                    .contains_key(PARTIAL_UPDATE_SCHEMA_KEY)
            });
            if partial {
                self.leave_position_merge()?;
            }
            let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
            self.keep_or_ingest(PositionMergeBlock::Data { positions, batches }, bytes)?;
        }
        Ok(())
    }

    fn process_next_data_record(&mut self, record: BufferedRecord, key: &str) -> Result<()> {
        self.leave_position_merge()?;
        self.note_position_key(key);
        self.inner.process_next_data_record(record, key)
    }

    fn process_delete_block(&mut self, block: &mut LogBlock) -> Result<()> {
        if !self.should_use_position {
            return self.inner.process_delete_block(block);
        }
        let positions = match self.extract_positions(block)? {
            Some(p) => p,
            None => {
                self.leave_position_merge()?;
                self.fallback_to_key_based()?;
                return self.inner.process_delete_block(block);
            }
        };

        if let LogBlockContent::Records(record_batches) = std::mem::take(&mut block.content) {
            let batches = record_batches.delete_batches;
            let rows: usize = batches.iter().map(|(b, _)| b.num_rows()).sum();
            if rows != positions.len() {
                return Err(record_count_mismatch("delete", rows, positions.len()));
            }
            let bytes: usize = batches.iter().map(|(b, _)| b.get_array_memory_size()).sum();
            self.keep_or_ingest(PositionMergeBlock::Delete { positions, batches }, bytes)?;
        }
        Ok(())
    }

    fn process_next_deleted_record(
        &mut self,
        delete_record: DeleteRecord,
        key: &str,
    ) -> Result<()> {
        self.leave_position_merge()?;
        self.note_position_key(key);
        self.inner.process_next_deleted_record(delete_record, key)
    }

    /// Whether a log record with `record_key` is buffered. The map is
    /// position-keyed while `should_use_position`, so scan values by their
    /// record key (mirrors Java's overridden `containsLogRecord`). Only the
    /// in-memory tier is scanned; a spilled position entry is not visible here
    /// (a limitation shared with the key-based `get_log_records`).
    fn contains_log_record(&self, record_key: &str) -> bool {
        if self.vectorized.is_some() {
            // Kept blocks hold records by position; a record key is not indexed
            // until they reach the record map.
            return false;
        }
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
        self.inner.size() + self.position_merge_record_count()
    }

    fn get_total_log_records(&self) -> u64 {
        self.inner.get_total_log_records() + self.position_merge_record_count() as u64
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
        let mut stats = self.inner.update_stats_snapshot();
        if let Some(index) = self.vectorized.as_ref().and_then(|d| d.index.as_ref()) {
            stats.num_inserts += index.stats.num_inserts;
            stats.num_updates += index.stats.num_updates;
            stats.num_deletes += index.stats.num_deletes;
        }
        stats
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
        self.leave_position_merge()?;
        if self.inner.base.next_record.is_some() {
            return Ok(true);
        }
        self.do_has_next_position()
    }

    fn next(&mut self) -> Option<BufferedRecord> {
        self.inner.base.next_record.take()
    }

    fn merge_and_collect_with_stats(mut self: Box<Self>) -> Result<(RecordBatch, UpdateStats)> {
        self.leave_position_merge()?;
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
        self.leave_position_merge()?;
        if self.needs_hybrid_strategy {
            return self.next_hybrid_base_batch(target_schema);
        }
        let base_match = self.base_match();
        self.inner
            .pull_and_merge_next_base_batch(target_schema, base_match)
    }

    /// Merge one caller-supplied base batch. See
    /// [`HoodieFileGroupRecordBuffer::merge_base_batch`].
    ///
    /// The hybrid strategy is refused rather than quietly merged the ordinary
    /// way. It resolves position-only deletes row by row against a source this
    /// method does not have, so answering without it would drop deletes and
    /// return rows that should not exist. Nothing sets `needs_hybrid_strategy`
    /// today; this is here so switching it on fails loudly instead.
    fn merge_base_batch(
        &mut self,
        base: &RecordBatch,
        target_schema: &SchemaRef,
    ) -> Result<Option<RecordBatch>> {
        if self.needs_hybrid_strategy {
            return Err(CoreError::Unsupported(
                "the position buffer fell back to the hybrid strategy, which resolves \
                 position-only deletes row by row and has no batch-at-a-time form"
                    .to_string(),
            ));
        }
        if self.should_use_position && self.build_position_merge_index(target_schema)? {
            return self.vectorized_position_merge(base, target_schema);
        }
        let base_match = self.base_match();
        self.inner
            .merge_one_base_batch_kernel(base, target_schema, base_match)
    }

    /// The vectorized merge matches base rows by position alone, so it needs no
    /// record key from the base file. A key field that is also an ordering
    /// field is still needed: event-time ordering compares the base row's value.
    fn base_columns_unneeded(&mut self, merge_schema: &SchemaRef) -> Result<Vec<String>> {
        if self.should_use_position && self.build_position_merge_index(merge_schema)? {
            let ordering = &self.inner.record_context.ordering_field_names;
            return Ok(self
                .inner
                .record_context
                .record_key_fields
                .iter()
                .filter(|k| !ordering.iter().any(|o| o.eq_ignore_ascii_case(k)))
                .cloned()
                .collect());
        }
        Ok(Vec::new())
    }

    fn merges_base_by_position(&mut self, merge_schema: &SchemaRef) -> Result<bool> {
        Ok(self.should_use_position && self.build_position_merge_index(merge_schema)?)
    }

    fn drain_log_only_inserts(&mut self, target_schema: &SchemaRef) -> Result<Option<RecordBatch>> {
        if self.vectorized.is_some() {
            return self.drain_position_merge(target_schema);
        }
        self.inner.drain_log_only_inserts(target_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::log_file::log_block::{BlockMetadataKey, BlockType};
    use crate::file_group::log_file::log_format::LogFormatVersion;
    use crate::file_group::reader_v2::buffer::record_positions::ROW_INDEX_TEMPORARY_COLUMN_NAME;
    use crate::file_group::reader_v2::buffer::spillable_map::CONFIG_MERGE_MAX_SIZE;
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
        let ords: Vec<Option<i64>> = keys.iter().map(|_| None).collect();
        delete_block_with_positions_ordered(keys, &ords, positions, base_instant_of_positions)
    }

    /// Like [`delete_block_with_positions`], with each delete's ordering value.
    fn delete_block_with_positions_ordered(
        keys: &[&str],
        ords: &[Option<i64>],
        positions: &[u64],
        base_instant_of_positions: &str,
    ) -> LogBlock {
        let delete_schema = Arc::new(Schema::new(vec![
            Field::new("recordKey", DataType::Utf8, false),
            Field::new("partitionPath", DataType::Utf8, true),
            Field::new("orderingVal", DataType::Int64, true),
        ]));
        let parts: Vec<&str> = keys.iter().map(|_| "").collect();
        let batch = RecordBatch::try_new(
            delete_schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(StringArray::from(parts)),
                Arc::new(Int64Array::from(ords.to_vec())),
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
        build_buffer_with("COMMIT_TIME_ORDERING", &[], reader_config)
    }

    /// Build a buffer under `merge_mode`, with extra table-config and reader-config
    /// entries.
    fn build_buffer_with(
        merge_mode: &str,
        table_config: &[(&str, &str)],
        reader_config: &[(&str, &str)],
    ) -> PositionBasedFileGroupRecordBuffer {
        build_buffer_for(schema(), merge_mode, table_config, reader_config)
    }

    /// [`build_buffer_with`] over another table schema.
    fn build_buffer_for(
        table_schema: SchemaRef,
        merge_mode: &str,
        table_config: &[(&str, &str)],
        reader_config: &[(&str, &str)],
    ) -> PositionBasedFileGroupRecordBuffer {
        let mut ctx = ReaderContext::empty();
        for (k, v) in table_config {
            ctx.table_config.insert((*k).to_string(), (*v).to_string());
        }
        for (k, v) in reader_config {
            ctx.hoodie_reader_config
                .insert((*k).to_string(), (*v).to_string());
        }
        ctx.rebuild_record_context(String::new());
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(table_schema.clone())
            .with_data_schema(table_schema);
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

    /// How a merge test drives the buffer.
    #[derive(Clone, Copy, Debug)]
    enum MergeWay {
        /// The vectorized position-based merge over the kept blocks.
        Vectorized,
        /// The record map keyed by position, with the position filter.
        PositionMap,
        /// The record map keyed by record key.
        RecordKey,
    }

    /// Update and delete blocks across two commits, merged over a base of two
    /// batches. The vectorized merge must return what both record-map merges do:
    /// a later block wins at a position, a delete removes the row, and a base
    /// batch no block touches comes back whole.
    #[test]
    fn vectorized_merge_matches_position_and_key_merges() {
        fn run(way: MergeWay) -> Vec<(String, i32, i64)> {
            let mut buffer = build_buffer();
            if matches!(way, MergeWay::RecordKey) {
                buffer.vectorized = None;
                buffer.fallback_to_key_based().unwrap();
            }
            // Commit 1 updates positions 2 and 7 and deletes position 5.
            let mut update1 = data_block_with_positions_at_instant(
                &[("k2", 200, 9), ("k7", 700, 9)],
                &[2, 7],
                BASE_INSTANT,
                "20240101000001000",
            );
            buffer.process_data_block(&mut update1).unwrap();
            let mut delete1 = delete_block_with_positions(&["k5"], &[5], BASE_INSTANT);
            buffer.process_delete_block(&mut delete1).unwrap();
            // Commit 2 updates position 7 again, deletes position 2 and
            // updates position 12, in the second base batch.
            let mut update2 = data_block_with_positions_at_instant(
                &[("k7", 777, 9), ("k12", 1200, 9)],
                &[7, 12],
                BASE_INSTANT,
                "20240101000002000",
            );
            buffer.process_data_block(&mut update2).unwrap();
            let mut delete2 = delete_block_with_positions(&["k2"], &[2], BASE_INSTANT);
            buffer.process_delete_block(&mut delete2).unwrap();
            match way {
                MergeWay::Vectorized => assert!(buffer.vectorized.is_some()),
                MergeWay::PositionMap => {
                    buffer.leave_position_merge().unwrap();
                    let filter = buffer.inner.position_filter.as_ref().unwrap();
                    assert_eq!(filter.iter().collect::<Vec<_>>(), vec![2, 5, 7, 12]);
                }
                MergeWay::RecordKey => {
                    assert_eq!(buffer.get_buffer_type(), BufferType::KeyBasedMerge)
                }
            }
            let keys: Vec<String> = (0..20).map(|i| format!("k{i}")).collect();
            let rows: Vec<(&str, i32, i64)> = keys
                .iter()
                .enumerate()
                .map(|(i, k)| (k.as_str(), i as i32, 1))
                .collect();
            let target = schema();
            let mut all: Vec<RecordBatch> = Vec::new();
            for (range, start) in [(0..10, 0u64), (10..15, 10), (15..20, 15)] {
                let batch = base_batch_at(&rows[range], start);
                if let Some(out) = buffer.merge_base_batch(&batch, &target).unwrap() {
                    all.push(out);
                }
            }
            while let Some(b) = buffer.drain_log_only_inserts(&target).unwrap() {
                all.push(b);
            }
            let merged = arrow_select::concat::concat_batches(&target, &all).unwrap();
            let mut out = extract(&merged);
            out.sort();
            out
        }
        let vectorized = run(MergeWay::Vectorized);
        assert_eq!(vectorized, run(MergeWay::PositionMap));
        assert_eq!(vectorized, run(MergeWay::RecordKey));
        assert_eq!(vectorized.len(), 18, "positions 2 and 5 are deleted");
        assert!(
            vectorized.contains(&("k7".to_string(), 777, 9)),
            "the later update wins"
        );
        assert!(vectorized.contains(&("k12".to_string(), 1200, 9)));
        assert!(!vectorized.iter().any(|(k, _, _)| k == "k2" || k == "k5"));
    }

    /// Under event-time ordering the vectorized merge returns what the position map
    /// and the key merge return: base rows that outrank the log stay, ties go to
    /// the log, a later but lower-ordered log record loses the fold, and a delete
    /// with a lower non-default ordering value is obsolete while a default one
    /// always applies.
    #[test]
    fn vectorized_merge_event_time_matches_position_and_key_merges() {
        fn run(way: MergeWay) -> Vec<(String, i32, i64)> {
            let event = "EVENT_TIME_ORDERING";
            let ordering = [("hoodie.table.ordering.fields", "ts")];
            let mut buffer = build_buffer_with(event, &ordering, &[]);
            if matches!(way, MergeWay::RecordKey) {
                buffer.vectorized = None;
                buffer.fallback_to_key_based().unwrap();
            }
            // Base rows all carry ts 5.
            // Commit 1: k1 older (3), k2 tied (5), k3 and k7 newer (9), and an
            // update past the base (k40).
            let mut update1 = data_block_with_positions_at_instant(
                &[
                    ("k1", 100, 3),
                    ("k2", 200, 5),
                    ("k3", 300, 9),
                    ("k7", 700, 9),
                    ("k40", 4000, 1),
                ],
                &[1, 2, 3, 7, 40],
                BASE_INSTANT,
                "20240101000001000",
            );
            buffer.process_data_block(&mut update1).unwrap();
            // k4's delete is older than the base, k6's has no ordering value,
            // k8's is newer.
            let mut delete1 = delete_block_with_positions_ordered(
                &["k4", "k6", "k8"],
                &[Some(3), None, Some(7)],
                &[4, 6, 8],
                BASE_INSTANT,
            );
            buffer.process_delete_block(&mut delete1).unwrap();
            // Commit 2: k7 again but older than commit 1's k7, k12 older than
            // its base row, k15 newer.
            let mut update2 = data_block_with_positions_at_instant(
                &[("k7", 777, 6), ("k12", 1200, 1), ("k15", 1500, 8)],
                &[7, 12, 15],
                BASE_INSTANT,
                "20240101000002000",
            );
            buffer.process_data_block(&mut update2).unwrap();
            // k3's delete is older than commit 1's k3, k9's is newer than base.
            let mut delete2 = delete_block_with_positions_ordered(
                &["k3", "k9"],
                &[Some(8), Some(10)],
                &[3, 9],
                BASE_INSTANT,
            );
            buffer.process_delete_block(&mut delete2).unwrap();
            match way {
                MergeWay::Vectorized => assert!(buffer.vectorized.is_some()),
                MergeWay::PositionMap => buffer.leave_position_merge().unwrap(),
                MergeWay::RecordKey => {
                    assert_eq!(buffer.get_buffer_type(), BufferType::KeyBasedMerge)
                }
            }
            let keys: Vec<String> = (0..20).map(|i| format!("k{i}")).collect();
            let rows: Vec<(&str, i32, i64)> = keys
                .iter()
                .enumerate()
                .map(|(i, k)| (k.as_str(), i as i32, 5))
                .collect();
            let target = schema();
            let mut all: Vec<RecordBatch> = Vec::new();
            for (range, start) in [(0..10, 0u64), (10..15, 10), (15..20, 15)] {
                let batch = base_batch_at(&rows[range], start);
                if let Some(out) = buffer.merge_base_batch(&batch, &target).unwrap() {
                    all.push(out);
                }
            }
            while let Some(b) = buffer.drain_log_only_inserts(&target).unwrap() {
                all.push(b);
            }
            let merged = arrow_select::concat::concat_batches(&target, &all).unwrap();
            let mut out = extract(&merged);
            out.sort();
            out
        }
        let vectorized = run(MergeWay::Vectorized);
        assert_eq!(vectorized, run(MergeWay::PositionMap));
        assert_eq!(vectorized, run(MergeWay::RecordKey));
        let has = |k: &str, c: i32, t: i64| vectorized.contains(&(k.to_string(), c, t));
        assert!(has("k1", 1, 5), "an older log record loses to the base row");
        assert!(has("k2", 200, 5), "a tie goes to the log");
        assert!(
            has("k3", 300, 9),
            "an older delete loses to the newer log update"
        );
        assert!(
            has("k4", 4, 5),
            "a delete older than the base row is obsolete"
        );
        assert!(
            has("k7", 700, 9),
            "the higher-ordered log record wins the fold"
        );
        assert!(has("k12", 12, 5));
        assert!(has("k15", 1500, 8));
        assert!(has("k40", 4000, 1), "an update past the base is emitted");
        for gone in ["k6", "k8", "k9"] {
            assert!(
                !vectorized.iter().any(|(k, _, _)| k == gone),
                "{gone} is deleted"
            );
        }
    }

    /// A data block that would reach the vectorized merge in each fallback case
    /// below: it updates position 2 and is followed by a delete of position 5.
    fn fallback_blocks(base_instant_of_positions: &str, partial: bool) -> (LogBlock, LogBlock) {
        let mut update = data_block_with_positions_at_instant(
            &[("k2", 200, 9)],
            &[2],
            base_instant_of_positions,
            "20240101000001000",
        );
        if partial && let LogBlockContent::Records(records) = &mut update.content {
            for batch in records.data_batches.iter_mut() {
                let mut metadata = batch.schema().metadata().clone();
                metadata.insert(PARTIAL_UPDATE_SCHEMA_KEY.to_string(), "true".to_string());
                let schema = Arc::new(batch.schema().as_ref().clone().with_metadata(metadata));
                *batch = batch.clone().with_schema(schema).unwrap();
            }
        }
        let delete = delete_block_with_positions(&["k5"], &[5], base_instant_of_positions);
        (update, delete)
    }

    /// Merge `blocks` into a 10-row base through `buffer`'s batch path and
    /// return the sorted rows.
    fn merge_ten_rows(
        mut buffer: PositionBasedFileGroupRecordBuffer,
        blocks: (LogBlock, LogBlock),
    ) -> (Vec<(String, i32, i64)>, PositionBasedFileGroupRecordBuffer) {
        let (mut update, mut delete) = blocks;
        buffer.process_data_block(&mut update).unwrap();
        buffer.process_delete_block(&mut delete).unwrap();
        let keys: Vec<String> = (0..10).map(|i| format!("k{i}")).collect();
        let rows: Vec<(&str, i32, i64)> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.as_str(), i as i32, 1))
            .collect();
        let target = schema();
        let mut all = Vec::new();
        if let Some(out) = buffer
            .merge_base_batch(&base_batch_at(&rows, 0), &target)
            .unwrap()
        {
            all.push(out);
        }
        while let Some(b) = buffer.drain_log_only_inserts(&target).unwrap() {
            all.push(b);
        }
        let merged = arrow_select::concat::concat_batches(&target, &all).unwrap();
        let mut out = extract(&merged);
        out.sort();
        (out, buffer)
    }

    /// The same blocks merged by record key, the reference every fallback must
    /// match.
    fn key_merged(
        merge_mode: &str,
        table_config: &[(&str, &str)],
        blocks: (LogBlock, LogBlock),
    ) -> Vec<(String, i32, i64)> {
        let mut buffer = build_buffer_with(merge_mode, table_config, &[]);
        buffer.vectorized = None;
        buffer.fallback_to_key_based().unwrap();
        merge_ten_rows(buffer, blocks).0
    }

    /// Whether `buffer` still needs the record key from the base file.
    fn reads_key(buffer: &mut dyn HoodieFileGroupRecordBuffer) -> bool {
        let reads = buffer.base_columns_unneeded(&schema()).unwrap().is_empty();
        // Only the merge by position alone does without the key, and that is
        // the merge whose base batches may be read larger.
        assert_eq!(buffer.merges_base_by_position(&schema()).unwrap(), !reads);
        reads
    }

    /// Each case where the vectorized merge does not apply falls back on its own,
    /// returns what the key merge returns, and reads the record key from the
    /// base file again. The vectorized case, for contrast, does without it.
    #[test]
    fn vectorized_merge_falls_back_where_it_does_not_apply() {
        let expected: Vec<(String, i32, i64)> = (0..10)
            .filter(|i| *i != 5)
            .map(|i| {
                if i == 2 {
                    ("k2".to_string(), 200, 9)
                } else {
                    (format!("k{i}"), i, 1)
                }
            })
            .collect();
        let commit = "COMMIT_TIME_ORDERING";

        // Vectorized: positions match the base instant.
        let vectorized = build_buffer();
        assert!(vectorized.vectorized.is_some());
        let (out, mut vectorized) =
            merge_ten_rows(vectorized, fallback_blocks(BASE_INSTANT, false));
        let reference = key_merged(commit, &[], fallback_blocks(BASE_INSTANT, false));
        assert_eq!(out, expected);
        assert_eq!(out, reference);
        assert!(vectorized.vectorized.is_some());
        assert!(!reads_key(&mut vectorized));

        // No record positions in the block: the empty bitmap falls back to
        // the key merge.
        let mut no_positions = build_buffer();
        let mut update = data_block_with_positions(&[("k2", 200, 9)], &[], BASE_INSTANT);
        no_positions.process_data_block(&mut update).unwrap();
        assert!(no_positions.vectorized.is_none());
        assert_eq!(no_positions.get_buffer_type(), BufferType::KeyBasedMerge);
        assert!(reads_key(&mut no_positions));

        // Positions computed against another base file.
        let other = "19990101000000000";
        let (out, mut mismatched) = merge_ten_rows(build_buffer(), fallback_blocks(other, false));
        let reference = key_merged(commit, &[], fallback_blocks(other, false));
        assert!(mismatched.vectorized.is_none());
        assert_eq!(mismatched.get_buffer_type(), BufferType::KeyBasedMerge);
        assert_eq!(out, expected);
        assert_eq!(out, reference);
        assert!(reads_key(&mut mismatched));

        // A partial-update block: still merged by position, through the map.
        let blocks = fallback_blocks(BASE_INSTANT, true);
        let (out, mut partial) = merge_ten_rows(build_buffer(), blocks);
        let reference = key_merged(commit, &[], fallback_blocks(BASE_INSTANT, true));
        assert!(partial.vectorized.is_none());
        assert_eq!(partial.get_buffer_type(), BufferType::PositionBasedMerge);
        assert_eq!(out, reference);
        assert!(reads_key(&mut partial));

        // Event-time ordering takes the vectorized merge too, and matches the key
        // merge without reading the record key.
        let event = "EVENT_TIME_ORDERING";
        let ordering = [("hoodie.table.ordering.fields", "ts")];
        let event_buffer = build_buffer_with(event, &ordering, &[]);
        assert!(event_buffer.vectorized.is_some());
        let blocks = fallback_blocks(BASE_INSTANT, false);
        let (out, mut event_buffer) = merge_ten_rows(event_buffer, blocks);
        let reference = key_merged(event, &ordering, fallback_blocks(BASE_INSTANT, false));
        assert_eq!(out, reference);
        assert!(!reads_key(&mut event_buffer));

        // Event-time ordering with positions from another base file falls back.
        let blocks = fallback_blocks(other, false);
        let (out, mut event_fallback) =
            merge_ten_rows(build_buffer_with(event, &ordering, &[]), blocks);
        let reference = key_merged(event, &ordering, fallback_blocks(other, false));
        assert!(event_fallback.vectorized.is_none());
        assert_eq!(out, reference);
        assert!(reads_key(&mut event_fallback));

        // A custom merger decides from whole rows, so it stays on the row path.
        let custom = build_buffer_with(
            "CUSTOM",
            &[
                (
                    "hoodie.compaction.payload.class",
                    "org.apache.hudi.metadata.HoodieMetadataPayload",
                ),
                (
                    "hoodie.record.merge.strategy.id",
                    "00000000-0000-0000-0000-000000000000",
                ),
            ],
            &[],
        );
        assert!(custom.vectorized.is_none());

        // `hoodie.merge.use.record.positions=false` builds a key-based buffer,
        // which needs every merge column from the base file.
        let mut key_buffer = build_buffer().inner;
        assert!(reads_key(&mut key_buffer));
    }

    /// A record key field that is also the ordering field stays read from the
    /// base file: event-time ordering compares the base row's value.
    #[test]
    fn a_key_field_used_for_ordering_is_never_skipped() {
        let ordering = [("hoodie.table.ordering.fields", "_hoodie_record_key")];
        let mut buffer = build_buffer_with("EVENT_TIME_ORDERING", &ordering, &[]);
        assert!(buffer.vectorized.is_some());
        let mut update = data_block_with_positions(&[("k2", 200, 9)], &[2], BASE_INSTANT);
        buffer.process_data_block(&mut update).unwrap();
        assert!(buffer.base_columns_unneeded(&schema()).unwrap().is_empty());
    }

    /// An update whose base row the base read never produces is emitted once
    /// the base is exhausted, and a block that would outgrow the merge map's
    /// budget hands the kept blocks to the map.
    #[test]
    fn vectorized_merge_drains_unmatched_updates_and_respects_budget() {
        let mut buffer = build_buffer();
        let mut block =
            data_block_with_positions(&[("k1", 100, 9), ("k40", 4000, 9)], &[1, 40], BASE_INSTANT);
        buffer.process_data_block(&mut block).unwrap();
        let target = schema();
        let base = base_batch_at(&[("k0", 0, 1), ("k1", 1, 1), ("k2", 2, 1)], 0);
        let merged = buffer.merge_base_batch(&base, &target).unwrap().unwrap();
        let mut rows = extract(&merged);
        rows.sort();
        assert_eq!(
            rows,
            vec![
                ("k0".to_string(), 0, 1),
                ("k1".to_string(), 100, 9),
                ("k2".to_string(), 2, 1)
            ]
        );
        let drained = buffer.drain_log_only_inserts(&target).unwrap().unwrap();
        assert_eq!(extract(&drained), vec![("k40".to_string(), 4000, 9)]);
        assert!(buffer.drain_log_only_inserts(&target).unwrap().is_none());

        let mut tiny = build_buffer_with_reader_config(&[(CONFIG_MERGE_MAX_SIZE, "1")]);
        let mut block = data_block_with_positions(&[("k1", 100, 9)], &[1], BASE_INSTANT);
        tiny.process_data_block(&mut block).unwrap();
        assert!(
            tiny.vectorized.is_none(),
            "past the budget the blocks go to the map"
        );
        assert_eq!(tiny.size(), 1);
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
    // Exercises the on-disk spill tier, which only exists with the backend.
    #[cfg(feature = "spill-rocksdb")]
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

    /// Regression test: a position-keyed DELETE tombstone
    /// that spills to the RocksDB tier must still suppress its target row after
    /// key-based fallback. A delete's disk entry stores no payload, so if
    /// `spillable_map::record_from_entry` reconstructed `record_key` from the
    /// MAP key — which in this buffer is the POSITION string (e.g. "3"), not the
    /// record key ("k00003") — `fallback_to_key_based` would re-insert the
    /// tombstone under "3"; it would never match its target base row, and the
    /// deleted row would silently reappear. Guarded by persisting the true
    /// `record_key` in the delete's spill entry (`DiskLoc::Delete { record_key }`)
    /// and returning it from `record_from_entry`. This asserts the deleted row
    /// stays deleted in BOTH the spill and no-spill runs (Java parity); the
    /// no-spill control also guards against a change that would mask the
    /// spill-specific path.
    // Exercises the on-disk spill tier, which only exists with the backend.
    #[cfg(feature = "spill-rocksdb")]
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

        // Spill-engaged: a 1-byte budget leaves a zero in-memory allowance, so
        // even the lone tombstone spills to the RocksDB tier before the
        // fallback. The smallest possible budget is used deliberately: what
        // this test needs is that the entry lands on the disk tier, and a
        // larger "plausible" budget could keep it in memory since the RocksDB
        // reserve is charged only once the disk tier opens.
        let (spilled_out, did_spill) = run(&[("hoodie.memory.merge.max.size", "1")]);
        assert!(
            did_spill,
            "a zero in-memory allowance must spill the position-keyed delete tombstone"
        );
        assert_eq!(
            spilled_out, expected,
            "spilled position-keyed delete tombstone must still suppress its target row after \
             key-based fallback (Java parity) — if this row is present, the tombstone was \
             re-keyed by its position string (spillable_map::record_from_entry) and silently \
             failed to delete"
        );
    }

    /// A delete block carrying more records than positions is refused.
    ///
    /// The block's records and its position bitmap are zipped index by index —
    /// correct only because Hudi's writer emits both in ascending order. If the
    /// two ever disagree in length, every pairing after the shortfall is a guess
    /// about which base row a delete applies to, so it stops rather than
    /// deleting an arbitrary row.
    #[test]
    fn test_delete_block_with_more_records_than_positions_is_refused() {
        let mut buffer = build_buffer();
        // Three deletes, two positions.
        let mut block = delete_block_with_positions(&["k0", "k1", "k2"], &[0, 1], BASE_INSTANT);
        let err = buffer
            .process_delete_block(&mut block)
            .expect_err("a delete block longer than its position list must be refused");
        assert!(
            err.to_string().contains("more records than positions"),
            "the error must say which invariant broke, got: {err}"
        );
    }

    /// The mirror case: more positions than records leaves the zip short, which
    /// is the same disagreement seen from the other side.
    #[test]
    fn test_delete_block_with_more_positions_than_records_is_refused() {
        let mut buffer = build_buffer();
        let mut block = delete_block_with_positions(&["k0"], &[0, 1, 2], BASE_INSTANT);
        let err = buffer
            .process_delete_block(&mut block)
            .expect_err("a position list longer than the delete block must be refused");
        assert!(
            err.to_string().contains("!= position count"),
            "the error must say which invariant broke, got: {err}"
        );
    }

    /// An empty position bitmap is not a position-merged block, so the buffer
    /// falls back to key-based rather than treating "no positions" as "no rows
    /// to match" — which would silently drop the block's deletes.
    #[test]
    fn test_empty_position_bitmap_falls_back_to_key_based() {
        let mut buffer = build_buffer();
        assert_eq!(buffer.get_buffer_type(), BufferType::PositionBasedMerge);

        let mut block = delete_block_with_positions(&["k0"], &[], BASE_INSTANT);
        buffer
            .process_delete_block(&mut block)
            .expect("an empty bitmap falls back rather than failing");

        assert_eq!(
            buffer.get_buffer_type(),
            BufferType::KeyBasedMerge,
            "an empty position bitmap must switch the buffer to key-based"
        );
        assert!(
            buffer.contains_log_record("k0"),
            "the delete must survive the fallback, keyed by record key"
        );
    }

    /// Regression test: a fallback that would collapse two positions of the same
    /// record key must refuse, not silently drop one of the updates.
    ///
    /// With `dup` at base positions 1 and 2 and a log block updating both by
    /// position, the position merge lands both. A later block whose positions
    /// were computed against a different base file forces a fallback; re-keying
    /// then maps both entries onto "dup", and a plain insert would keep whichever
    /// the map happened to yield last:
    ///
    /// ```text
    /// position merge:      [a=10/1, dup=100/7, dup=200/8]
    /// after the fallback:  [a=11/9, dup=100/7, dup=30/3]   <- (200,8) gone
    /// ```
    ///
    /// (30,3) is the untouched base row, so a committed update would simply
    /// vanish — and which of the two vanished would depend on `HashMap`
    /// iteration order.
    #[test]
    fn test_fallback_refuses_to_collapse_duplicate_keys() {
        let base = &[("a", 10, 1), ("dup", 20, 2), ("dup", 30, 3)];

        // Control: no fallback, so both positions are merged independently and
        // both updates survive. This is the answer the read is supposed to give.
        let mut buffer = build_buffer();
        let mut block =
            data_block_with_positions(&[("dup", 100, 7), ("dup", 200, 8)], &[1, 2], BASE_INSTANT);
        buffer.process_data_block(&mut block).unwrap();
        buffer.set_base_file_iterator(vec![base_batch(base)]);
        let (out, _) = Box::new(buffer).merge_and_collect_with_stats().unwrap();
        assert_eq!(
            extract(&out),
            vec![
                ("a".into(), 10, 1),
                ("dup".into(), 100, 7),
                ("dup".into(), 200, 8)
            ],
            "merging by position reaches both duplicate rows"
        );

        // Same buffered updates, then a block that forces the fallback.
        let mut buffer = build_buffer();
        let mut block = data_block_with_positions_at_instant(
            &[("dup", 100, 7), ("dup", 200, 8)],
            &[1, 2],
            BASE_INSTANT,
            "20240101000001000",
        );
        buffer.process_data_block(&mut block).unwrap();
        let mut mismatched = data_block_with_positions_at_instant(
            &[("a", 11, 9)],
            &[0],
            "19990101000000000",
            "20240101000002000",
        );
        let err = buffer
            .process_data_block(&mut mismatched)
            .expect_err("collapsing two positions of one key must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("dup"),
            "the error must name the offending record key, got: {msg}"
        );
        assert!(
            msg.contains("hoodie.merge.use.record.positions"),
            "the error must say how to get a correct read instead, got: {msg}"
        );
    }

    /// The same fallback over UNIQUE keys must still succeed — the refusal above
    /// must not turn every fallback into an error.
    #[test]
    fn test_fallback_over_unique_keys_still_succeeds() {
        let mut buffer = build_buffer();
        let mut block = data_block_with_positions_at_instant(
            &[("a", 100, 7), ("b", 200, 8)],
            &[0, 1],
            BASE_INSTANT,
            "20240101000001000",
        );
        buffer.process_data_block(&mut block).unwrap();
        let mut mismatched = data_block_with_positions_at_instant(
            &[("c", 300, 9)],
            &[2],
            "19990101000000000",
            "20240101000002000",
        );
        buffer.process_data_block(&mut mismatched).unwrap();
        assert_eq!(buffer.get_buffer_type(), BufferType::KeyBasedMerge);

        buffer.set_base_file_iterator(vec![base_batch(&[("a", 1, 1), ("b", 2, 2), ("c", 3, 3)])]);
        let (out, _) = Box::new(buffer).merge_and_collect_with_stats().unwrap();
        assert_eq!(
            extract(&out),
            vec![
                ("a".into(), 100, 7),
                ("b".into(), 200, 8),
                ("c".into(), 300, 9)
            ],
            "every re-keyed update must land when the keys are unique"
        );
    }

    /// Merge cases over a table with a nullable ordering field and the
    /// `_hoodie_is_deleted` marker, run through the vectorized merge, the
    /// position map and the key merge.
    mod wide {
        use super::*;
        use arrow_array::{ArrayRef, BooleanArray};

        /// `(key, counter, ts, _hoodie_is_deleted)`; `ts` is `None` for null.
        type Row<'a> = (&'a str, i32, Option<i64>, bool);

        /// `_hoodie_record_key`, `counter`, a nullable `ts` of `ts_type` (Int64
        /// or Int32) and a nullable `_hoodie_is_deleted`.
        fn wide_schema(ts_type: DataType) -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("_hoodie_record_key", DataType::Utf8, false),
                Field::new("counter", DataType::Int32, false),
                Field::new("ts", ts_type, true),
                Field::new("_hoodie_is_deleted", DataType::Boolean, true),
            ]))
        }

        fn ts_array(schema: &SchemaRef, rows: &[Row]) -> ArrayRef {
            let values = rows.iter().map(|r| r.2);
            match schema.field_with_name("ts").unwrap().data_type() {
                DataType::Int32 => {
                    Arc::new(Int32Array::from_iter(values.map(|v| v.map(|v| v as i32))))
                }
                _ => Arc::new(Int64Array::from_iter(values)),
            }
        }

        fn wide_batch(schema: &SchemaRef, rows: &[Row]) -> RecordBatch {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
                    Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.1))),
                    ts_array(schema, rows),
                    Arc::new(BooleanArray::from_iter(rows.iter().map(|r| Some(r.3)))),
                ],
            )
            .unwrap()
        }

        /// A base batch whose row-index column holds `positions`.
        fn wide_base(schema: &SchemaRef, rows: &[Row], positions: &[Option<i64>]) -> RecordBatch {
            let batch = wide_batch(schema, rows);
            let mut fields: Vec<Field> =
                schema.fields().iter().map(|f| f.as_ref().clone()).collect();
            fields.push(Field::new(
                ROW_INDEX_TEMPORARY_COLUMN_NAME,
                DataType::Int64,
                true,
            ));
            let mut columns = batch.columns().to_vec();
            columns.push(Arc::new(Int64Array::from(positions.to_vec())));
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
        }

        /// A data block of `rows` at `positions`, written at `instant`.
        fn wide_data_block(
            schema: &SchemaRef,
            rows: &[Row],
            positions: &[u64],
            instant: &str,
        ) -> LogBlock {
            let mut header = HashMap::new();
            header.insert(BlockMetadataKey::InstantTime, instant.to_string());
            if !positions.is_empty() {
                header.insert(
                    BlockMetadataKey::RecordPositions,
                    encode_positions(positions),
                );
            }
            header.insert(
                BlockMetadataKey::BaseFileInstantTimeOfRecordPositions,
                BASE_INSTANT.to_string(),
            );
            LogBlock::new(
                LogFormatVersion::V1,
                BlockType::ParquetData,
                header,
                LogBlockContent::Records(RecordBatches::new_with_data_batches(vec![wide_batch(
                    schema, rows,
                )])),
                HashMap::new(),
            )
        }

        /// `(key, counter, ts)` of every row, sorted, whatever `ts`'s type.
        fn wide_rows(batch: &RecordBatch) -> Vec<(String, i32, Option<i64>)> {
            let keys = batch
                .column_by_name("_hoodie_record_key")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let counters = batch
                .column_by_name("counter")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let ts = batch.column_by_name("ts").unwrap();
            let ts_at = |i: usize| -> Option<i64> {
                if ts.is_null(i) {
                    return None;
                }
                if let Some(a) = ts.as_any().downcast_ref::<Int32Array>() {
                    Some(a.value(i) as i64)
                } else {
                    Some(ts.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                }
            };
            let mut out: Vec<(String, i32, Option<i64>)> = (0..batch.num_rows())
                .map(|i| (keys.value(i).to_string(), counters.value(i), ts_at(i)))
                .collect();
            out.sort();
            out
        }

        /// One log block of a case: data rows at positions, or a delete block.
        enum Block<'a> {
            Data(Vec<Row<'a>>, Vec<u64>, &'a str),
            Delete(Vec<&'a str>, Vec<Option<i64>>, Vec<u64>),
        }

        /// Merge `blocks` over `base` (batches with their positions) one way, and
        /// return the sorted rows and the buffer.
        fn run(
            way: MergeWay,
            schema: &SchemaRef,
            merge_mode: &str,
            reader_config: &[(&str, &str)],
            blocks: &[Block],
            base: &[(Vec<Row>, Vec<Option<i64>>)],
        ) -> (
            Vec<(String, i32, Option<i64>)>,
            PositionBasedFileGroupRecordBuffer,
        ) {
            let ordering = [("hoodie.table.ordering.fields", "ts")];
            let table_config: &[(&str, &str)] = if merge_mode == "EVENT_TIME_ORDERING" {
                &ordering
            } else {
                &[]
            };
            let mut buffer =
                build_buffer_for(schema.clone(), merge_mode, table_config, reader_config);
            if matches!(way, MergeWay::RecordKey) {
                buffer.vectorized = None;
                buffer.fallback_to_key_based().unwrap();
            }
            for block in blocks {
                match block {
                    Block::Data(rows, positions, instant) => {
                        let mut b = wide_data_block(schema, rows, positions, instant);
                        buffer.process_data_block(&mut b).unwrap();
                    }
                    Block::Delete(keys, ords, positions) => {
                        let mut b = delete_block_with_positions_ordered(
                            keys,
                            ords,
                            positions,
                            BASE_INSTANT,
                        );
                        buffer.process_delete_block(&mut b).unwrap();
                    }
                }
            }
            if matches!(way, MergeWay::PositionMap) {
                buffer.leave_position_merge().unwrap();
            }
            let mut all = Vec::new();
            for (rows, positions) in base {
                let batch = wide_base(schema, rows, positions);
                if let Some(out) = buffer.merge_base_batch(&batch, schema).unwrap() {
                    all.push(out);
                }
            }
            while let Some(b) = buffer.drain_log_only_inserts(schema).unwrap() {
                assert!(b.num_rows() <= DEFAULT_BATCH_SIZE);
                all.push(b);
            }
            let merged = arrow_select::concat::concat_batches(schema, &all).unwrap();
            (wide_rows(&merged), buffer)
        }

        /// Runs `blocks` all three ways, asserts they agree and that the
        /// vectorized way kept its index, and returns the rows.
        fn agree(
            schema: &SchemaRef,
            merge_mode: &str,
            blocks: &[Block],
            base: &[(Vec<Row>, Vec<Option<i64>>)],
        ) -> Vec<(String, i32, Option<i64>)> {
            let (vectorized, buffer) =
                run(MergeWay::Vectorized, schema, merge_mode, &[], blocks, base);
            assert!(buffer.vectorized.is_some(), "the vectorized merge applied");
            let (map, _) = run(MergeWay::PositionMap, schema, merge_mode, &[], blocks, base);
            let (key, _) = run(MergeWay::RecordKey, schema, merge_mode, &[], blocks, base);
            assert_eq!(vectorized, key, "vectorized vs key merge");
            assert_eq!(map, key, "position map vs key merge");
            vectorized
        }

        fn base_rows(n: usize, ts: Option<i64>) -> (Vec<Row<'static>>, Vec<Option<i64>>) {
            let keys: Vec<&'static str> = (0..n)
                .map(|i| &*Box::leak(format!("k{i}").into_boxed_str()))
                .collect();
            let rows = keys
                .iter()
                .enumerate()
                .map(|(i, k)| (*k, i as i32, ts, false))
                .collect();
            (rows, (0..n as i64).map(Some).collect())
        }

        /// A row marked deleted in a data block folds as a data record, not a
        /// delete-block record. After a delete block with no ordering value (a
        /// commit-time delete), a later deleted row with a lower ordering than
        /// the base still loses to the base row, as in the key merge.
        #[test]
        fn event_time_deleted_rows_in_data_blocks_fold_like_the_key_merge() {
            let schema = wide_schema(DataType::Int64);
            let base = vec![base_rows(8, Some(10))];
            let blocks = vec![
                // k3: commit-time delete, then a deleted row older than the base.
                // k4: commit-time delete, then a deleted row newer than the base.
                // k5: a deleted row with no ordering value (a commit-time delete).
                // k6: an update, then an older deleted row.
                Block::Delete(vec!["k3", "k4"], vec![None, None], vec![3, 4]),
                Block::Data(
                    vec![
                        ("k3", 0, Some(5), true),
                        ("k4", 0, Some(20), true),
                        ("k5", 0, None, true),
                        ("k6", 600, Some(30), false),
                    ],
                    vec![3, 4, 5, 6],
                    "20240101000001000",
                ),
                Block::Data(
                    vec![("k6", 0, Some(25), true)],
                    vec![6],
                    "20240101000002000",
                ),
                // k7: a deleted row newer than the base, then an older delete block.
                Block::Data(
                    vec![("k7", 0, Some(40), true)],
                    vec![7],
                    "20240101000003000",
                ),
                Block::Delete(vec!["k7"], vec![Some(35)], vec![7]),
            ];
            let rows = agree(&schema, "EVENT_TIME_ORDERING", &blocks, &base);
            let keys: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
            assert!(
                keys.contains(&"k3"),
                "the older deleted row loses to the base"
            );
            assert!(!keys.contains(&"k4"));
            assert!(!keys.contains(&"k5"));
            assert!(rows.contains(&("k6".to_string(), 600, Some(30))));
            assert!(!keys.contains(&"k7"));
        }

        /// Null and Int32 `0` ordering values (both the default ordering value)
        /// on the base and log sides merge as the key merge does.
        #[test]
        fn event_time_null_and_zero_orderings_match_the_key_merge() {
            for ts_type in [DataType::Int32, DataType::Int64] {
                let schema = wide_schema(ts_type);
                let base = vec![(
                    vec![
                        ("k0", 0, None, false),
                        ("k1", 1, Some(0), false),
                        ("k2", 2, Some(5), false),
                        ("k3", 3, None, false),
                        ("k4", 4, Some(0), false),
                        ("k5", 5, Some(5), false),
                        ("k6", 6, None, false),
                    ],
                    (0..7).map(Some).collect(),
                )];
                let blocks = vec![
                    Block::Data(
                        vec![
                            ("k0", 100, Some(1), false),
                            ("k1", 101, None, false),
                            ("k2", 102, Some(0), false),
                            ("k3", 103, None, false),
                        ],
                        vec![0, 1, 2, 3],
                        "20240101000001000",
                    ),
                    Block::Delete(vec!["k4", "k5"], vec![Some(0), Some(0)], vec![4, 5]),
                    Block::Data(vec![("k6", 0, Some(0), true)], vec![6], "20240101000002000"),
                ];
                agree(&schema, "EVENT_TIME_ORDERING", &blocks, &base);
            }
        }

        /// Base batches whose positions are not ascending are matched row by
        /// row and merge as the key merge does, and a row whose position is null
        /// is never matched by position.
        #[test]
        fn non_ascending_and_null_base_positions() {
            let schema = wide_schema(DataType::Int64);
            let rows: Vec<Row> = vec![
                ("k3", 3, Some(1), false),
                ("k1", 1, Some(1), false),
                ("k2", 2, Some(1), false),
                ("k0", 0, Some(1), false),
            ];
            let base = vec![(rows.clone(), vec![Some(3), Some(1), Some(2), Some(0)])];
            let blocks = vec![
                Block::Data(
                    vec![("k1", 100, Some(9), false), ("k3", 300, Some(9), false)],
                    vec![1, 3],
                    "20240101000001000",
                ),
                Block::Delete(vec!["k2"], vec![None], vec![2]),
            ];
            for mode in ["COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"] {
                let out = agree(&schema, mode, &blocks, &base);
                assert_eq!(
                    out,
                    vec![
                        ("k0".to_string(), 0, Some(1)),
                        ("k1".to_string(), 100, Some(9)),
                        ("k3".to_string(), 300, Some(9)),
                    ]
                );
            }

            let null_base = vec![(rows, vec![Some(3), None, Some(2), Some(0)])];
            let (out, _) = run(
                MergeWay::Vectorized,
                &schema,
                "COMMIT_TIME_ORDERING",
                &[],
                &blocks,
                &null_base,
            );
            assert!(
                out.contains(&("k1".to_string(), 1, Some(1))),
                "a null position is not matched"
            );
            assert!(
                out.contains(&("k1".to_string(), 100, Some(9))),
                "its update is drained as a log-only record"
            );
        }

        /// A read whose kept blocks outgrow the budget and then meets a block
        /// without positions ends on the key merge with the key merge's rows.
        #[test]
        fn budget_handoff_then_a_block_without_positions() {
            let schema = wide_schema(DataType::Int64);
            let base = vec![base_rows(6, Some(1))];
            let blocks = vec![
                Block::Data(
                    vec![("k1", 100, Some(9), false), ("k2", 200, Some(9), false)],
                    vec![1, 2],
                    "20240101000001000",
                ),
                Block::Delete(vec!["k3"], vec![None], vec![3]),
                Block::Data(
                    vec![("k4", 400, Some(9), false)],
                    vec![],
                    "20240101000002000",
                ),
            ];
            for mode in ["COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"] {
                let tiny = [(CONFIG_MERGE_MAX_SIZE, "1")];
                let (out, buffer) = run(MergeWay::Vectorized, &schema, mode, &tiny, &blocks, &base);
                assert!(buffer.vectorized.is_none());
                assert_eq!(buffer.get_buffer_type(), BufferType::KeyBasedMerge);
                let (key, _) = run(MergeWay::RecordKey, &schema, mode, &[], &blocks, &base);
                assert_eq!(out, key);
                assert!(out.contains(&("k4".to_string(), 400, Some(9))));
                assert!(!out.iter().any(|r| r.0 == "k3"));
            }
        }

        /// Unmatched updates past one batch's worth drain in chunks of at most
        /// `DEFAULT_BATCH_SIZE` rows (checked in [`run`]) and all come back.
        #[test]
        fn drain_larger_than_a_batch_is_chunked() {
            let schema = wide_schema(DataType::Int64);
            let n = DEFAULT_BATCH_SIZE * 2 + 7;
            let keys: Vec<String> = (0..n).map(|i| format!("u{i:06}")).collect();
            let rows: Vec<Row> = keys
                .iter()
                .map(|k| (k.as_str(), 1, Some(1), false))
                .collect();
            let positions: Vec<u64> = (0..n as u64).map(|i| 100 + i).collect();
            let blocks = vec![Block::Data(rows, positions, "20240101000001000")];
            let base = vec![base_rows(3, Some(1))];
            let out = agree(&schema, "COMMIT_TIME_ORDERING", &blocks, &base);
            assert_eq!(out.len(), n + 3);
            let mut buffer = build_buffer_for(schema.clone(), "COMMIT_TIME_ORDERING", &[], &[]);
            let Block::Data(rows, positions, instant) = &blocks[0] else {
                unreachable!()
            };
            let mut block = wide_data_block(&schema, rows, positions, instant);
            buffer.process_data_block(&mut block).unwrap();
            let mut chunks = Vec::new();
            while let Some(b) = buffer.drain_log_only_inserts(&schema).unwrap() {
                chunks.push(b.num_rows());
            }
            assert_eq!(chunks, vec![DEFAULT_BATCH_SIZE, DEFAULT_BATCH_SIZE, 7]);
        }
    }
}
