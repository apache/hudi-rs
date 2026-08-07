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

//! Mirrors `org.apache.hudi.common.table.log.BaseHoodieLogRecordReader`.
//!
//! The 2-pass log scanning engine that reads log blocks, filters them through
//! 5 gates, resolves compaction, and dispatches to the record buffer.
//!
//! ## 3-pass algorithm (matching Java's `scanInternal`):
//!
//! **Pass 1 — Forward scan with 5 gates:**
//! - Gate 1: Corrupt → skip
//! - Gate 2: Future (instant > latestInstantTime) → skip
//! - Gate 3: Completed/inflight → skip uncommitted/inflight data/delete blocks (table version < 8;
//!   applied only when the completion sets are supplied)
//! - Gate 4: Instant range filter → skip
//! - Rollback command → remove target instant
//! - Data/Delete → add to `instant_to_blocks_map`
//!
//! **Pass 2 — Reverse iteration with compaction resolution:**
//! Iterate instants newest→oldest, `Collections.reverse(logBlocks)` then `addLast`.
//! Result: deque has latest-first at head, earliest-last at tail.
//!
//! **Pass 3 — `processQueuedBlocksForInstant`:**
//! Drain deque via `pollLast` (tail-first) = oldest instant processed first.

use crate::Result;
use crate::file_group::log_file::log_block::{BlockMetadataKey, BlockType, LogBlock};
use crate::file_group::log_file::reader::LogFileReader;
use crate::file_group::reader_v2::buffer::HoodieFileGroupRecordBuffer;
use crate::file_group::reader_v2::profiling::profile_once;
use crate::file_group::reader_v2::reader_context::{CompletionGateInputs, ReaderContext};
use crate::storage::Storage;
use crate::timeline::selector::InstantRange;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

// =========================================================================
// Pass 1 result
// =========================================================================

/// Output of Pass 1 (forward scan with 5 gates).
#[derive(Debug)]
pub struct Pass1Result {
    /// Instants in first-seen order (no duplicates).
    pub ordered_instants_list: Vec<String>,
    /// instant time → blocks for that instant (in file-read order).
    pub instant_to_blocks_map: HashMap<String, Vec<LogBlock>>,
    /// Stats
    pub total_log_blocks: u64,
    pub total_corrupt_blocks: u64,
    pub total_rollbacks: u64,
}

/// Borrowed view of [`CompletionGateInputs`] used inside a single scan pass.
pub struct CompletionGate<'a> {
    completed_instants: &'a HashSet<String>,
    inflight_instants: &'a HashSet<String>,
    archived_boundary: Option<&'a str>,
}

impl<'a> CompletionGate<'a> {
    /// Build a borrowing gate from owned inputs.
    pub fn new(inputs: &'a CompletionGateInputs) -> Self {
        Self {
            completed_instants: &inputs.completed_instants,
            inflight_instants: &inputs.inflight_instants,
            archived_boundary: inputs.archived_boundary.as_deref(),
        }
    }

    /// Whether a data/delete block at `instant_time` is admitted (committed and not inflight).
    ///
    /// Mirrors `TimelineView::is_committed` (completed-set membership OR older than the archived
    /// boundary) plus the explicit inflight exclusion, matching Java's scanInternalV1 filter.
    ///
    /// # No-positive-information fallback
    ///
    /// When the gate carries NO positive completion information — an empty
    /// `completed_instants` set AND no `archived_boundary` — it cannot establish
    /// committedness for any instant, so it admits everything (a no-op) rather
    /// than excluding all blocks. Java always builds the gate from a non-empty
    /// active timeline (`filterCompletedInstants()`), so an empty set here means
    /// the inputs were not populated by the caller (e.g. the FFI bridge forwarded
    /// an empty list). Excluding on that basis would silently drop EVERY log delta
    /// — including committed ones — and return base-file-only data (the
    /// C-INFLIGHT silent-wrong: a committed later delta wrongly dropped). Deferring
    /// to the other gates preserves the pre-gate behavior for a mis-wired gate; a
    /// correctly-populated gate is unaffected because its completed set is non-empty.
    fn admits(&self, instant_time: &str) -> bool {
        if self.is_unpopulated() {
            return true;
        }
        let is_committed = self.completed_instants.contains(instant_time)
            || self
                .archived_boundary
                .is_some_and(|boundary| instant_time < boundary);
        is_committed && !self.inflight_instants.contains(instant_time)
    }

    /// Whether the gate carries NO positive completion information — an empty
    /// `completed_instants` set AND no `archived_boundary`. Such a gate cannot establish
    /// committedness for any instant, so [`admits`](Self::admits) degrades to admit-all.
    ///
    /// When the gate was supplied deliberately (the caller passes `Some` only for a table
    /// version < 8 snapshot), being unpopulated is a mis-wire signal, not a genuine empty
    /// timeline: Java always builds the gate from a non-empty active timeline
    /// (`filterCompletedInstants()`), and a v1 snapshot always has >= 1 completed instant.
    /// `forward_scan_pass1` warns once when this holds so a gate whose inputs were dropped
    /// across the FFI boundary is diagnosable rather than a silent no-op.
    fn is_unpopulated(&self) -> bool {
        self.completed_instants.is_empty() && self.archived_boundary.is_none()
    }
}

/// Run Pass 1: forward scan with 5 gates.
///
/// Processes a flat list of blocks (from all log files, in file-read order)
/// and classifies them into `instant_to_blocks_map` + `ordered_instants_list`.
///
/// ## 5 Gates (per-block):
/// 1. Corrupt → skip
/// 2. Future (instant > latest_instant_time) → skip
/// 3. Completed/inflight → skip data/delete blocks whose instant is uncommitted or inflight
///    (C-INFLIGHT-DELTA). Applied only when `completion_gate` is `Some` — callers pass it for
///    table version < 8 and leave it `None` for v8+ (and when no timeline is available), which
///    preserves the prior behavior of admitting such blocks.
/// 4. Instant range → skip if not in range
/// 5. Rollback command → remove target instant from maps
pub fn forward_scan_pass1(
    all_blocks: Vec<LogBlock>,
    latest_instant_time: &str,
    instant_range: &Option<InstantRange>,
    timezone: &str,
    completion_gate: Option<&CompletionGate>,
) -> Result<Pass1Result> {
    let mut target_rollback_instants: HashSet<String> = HashSet::new();
    let mut instant_to_blocks_map: HashMap<String, Vec<LogBlock>> = HashMap::new();
    let mut ordered_instants_list: Vec<String> = Vec::new();
    let mut total_log_blocks: u64 = 0;
    let mut total_corrupt_blocks: u64 = 0;
    let mut total_rollbacks: u64 = 0;

    log::debug!(
        "[Pass1] forward_scan: {} total blocks, latest_instant_time={}, has_instant_range={}",
        all_blocks.len(),
        latest_instant_time,
        instant_range.is_some(),
    );

    // The completion gate was supplied (table version < 8 snapshot) but carries no positive
    // completion info — its inputs were not populated across the FFI boundary. Rather than
    // silently degrade to admit-all (Gate 3 becomes a no-op, so a straddling uncommitted delta
    // could be merged), warn once so the mis-wire is diagnosable. Behavior is unchanged (still
    // fail-open); this only makes the condition visible.
    if completion_gate.is_some_and(CompletionGate::is_unpopulated) {
        log::warn!(
            "[Pass1] completion gate supplied but unpopulated (empty completed set, no archived \
             boundary): Gate 3 (C-INFLIGHT) admits all delta blocks. A table version < 8 snapshot \
             always has >= 1 completed instant, so this indicates the gate inputs were dropped \
             across the FFI boundary. latest_instant_time={latest_instant_time}"
        );
    }

    for block in all_blocks {
        total_log_blocks += 1;

        // Gate 1: Corrupt blocks → skip
        if block.block_type == BlockType::Corrupted {
            log::debug!("[Pass1] Gate1: corrupt block #{total_log_blocks} skipped");
            total_corrupt_blocks += 1;
            continue;
        }

        let instant_time = match block.instant_time() {
            Ok(t) => t.to_string(),
            Err(_) => {
                log::debug!("[Pass1] block #{total_log_blocks} has no instant time, skipping");
                continue;
            }
        };

        // Gate 2: Future blocks → skip (instant > latestInstantTime)
        if block.block_type != BlockType::Command && instant_time.as_str() > latest_instant_time {
            log::debug!(
                "[Pass1] Gate2: future block #{total_log_blocks} instant={instant_time} > {latest_instant_time}, skipped"
            );
            continue;
        }

        // Gate 3: Completed/inflight check (Java scanInternalV1, tableVersion < 8).
        // Applied only when the caller supplies the timeline sets (None for v8+ or
        // when no timeline is available). A data/delete block whose instant is not
        // committed, or is inflight, is skipped -- without this an uncommitted
        // "straddling" instant (time < latest completed, no in-log rollback block)
        // is wrongly merged (C-INFLIGHT-DELTA). Command (rollback) blocks are never
        // gated here so they can still remove their target instant below.
        if block.block_type != BlockType::Command
            && let Some(gate) = completion_gate
            && !gate.admits(&instant_time)
        {
            log::debug!(
                "[Pass1] Gate3: block #{total_log_blocks} instant={instant_time} uncommitted/inflight, skipped"
            );
            continue;
        }

        // Gate 4: Instant range filter (not for command blocks)
        if block.block_type != BlockType::Command
            && let Some(range) = instant_range
            && range.not_in_range(&instant_time, timezone)?
        {
            log::debug!(
                "[Pass1] Gate4: block #{total_log_blocks} instant={instant_time} out of range, skipped"
            );
            continue;
        }

        log::debug!(
            "[Pass1] block #{total_log_blocks} passed all gates: type={:?} instant={instant_time}",
            block.block_type,
        );

        // Classify the block
        match block.block_type {
            BlockType::AvroData | BlockType::ParquetData | BlockType::Delete => {
                let blocks_list = instant_to_blocks_map
                    .entry(instant_time.clone())
                    .or_default();
                if blocks_list.is_empty() {
                    ordered_instants_list.push(instant_time.clone());
                }
                blocks_list.push(block);
            }
            BlockType::Command => {
                if block.is_rollback_block() {
                    total_rollbacks += 1;
                    if let Ok(target) = block.target_instant_time() {
                        let target = target.to_string();
                        log::debug!("[Pass1] ROLLBACK: removing instant={target}");
                        target_rollback_instants.insert(target.clone());
                        ordered_instants_list.retain(|t| t != &target);
                        instant_to_blocks_map.remove(&target);
                    }
                }
            }
            _ => {}
        }
    }

    log::debug!(
        "[Pass1] complete: ordered_instants={ordered_instants_list:?} \
         total_blocks={total_log_blocks} corrupt={total_corrupt_blocks} \
         rollbacks={total_rollbacks}",
    );
    for (instant, blocks) in &instant_to_blocks_map {
        log::debug!(
            "[Pass1]   instant={instant}: {} block(s) [{:?}]",
            blocks.len(),
            blocks
                .iter()
                .map(|b| format!("{:?}", b.block_type))
                .collect::<Vec<_>>(),
        );
    }

    Ok(Pass1Result {
        ordered_instants_list,
        instant_to_blocks_map,
        total_log_blocks,
        total_corrupt_blocks,
        total_rollbacks,
    })
}

// =========================================================================
// Pass 2 result
// =========================================================================

/// Output of Pass 2 (reverse iteration with compaction resolution).
#[derive(Debug)]
pub struct Pass2Result {
    /// The final ordered deque of blocks to be processed.
    /// Latest instant at head (front), earliest at tail (back).
    /// Drain via `pop_back` for oldest-first processing.
    pub current_instant_log_blocks: VecDeque<LogBlock>,
    /// Dedup guard — instants whose blocks are enqueued.
    pub instant_times_included: HashSet<String>,
    /// Which instant times actually contributed blocks (ordered output).
    pub valid_block_instants: Vec<String>,
}

/// Run Pass 2: reverse iteration with compaction resolution.
///
/// Iterates `ordered_instants_list` from newest→oldest, applying:
/// - `Collections.reverse(logBlocks)` then `addLast` (Rust: `reverse()` + `push_back`)
/// - Compaction: if block has `COMPACTED_BLOCK_TIMES` header, register mapping
/// - Deduplication: `instant_times_included` prevents double-enqueue
///
/// ## Invariants (from FS_logFileReadInputs_fileOrderRequestTime.md):
///
/// **Invariant 1**: `instant_times_included` and `valid_block_instants` contain
/// exactly the same instant strings (Set vs List).
///
/// **Invariant 2**: Every instant in `instant_times_included`/`valid_block_instants`
/// has its blocks in `current_instant_log_blocks`, and vice versa.
///
/// **Invariant 3**: Each instant appears in `instant_times_included` at most once (dedup).
///
/// **Invariant 4**: `current_instant_log_blocks` is ordered latest-first (reverse chronological).
/// Drain via `pop_back` produces oldest-first processing order.
pub fn reverse_scan_pass2(pass1: &mut Pass1Result) -> Pass2Result {
    log::debug!(
        "[Pass2] reverse_scan: {} instants to process (newest→oldest)",
        pass1.ordered_instants_list.len(),
    );

    let mut current_instant_log_blocks: VecDeque<LogBlock> = VecDeque::new();
    let mut instant_times_included: HashSet<String> = HashSet::new();
    let mut valid_block_instants: Vec<String> = Vec::new();
    let mut block_time_to_compaction_block_time_map: HashMap<String, String> = HashMap::new();

    for i in (0..pass1.ordered_instants_list.len()).rev() {
        let instant_time = &pass1.ordered_instants_list[i];
        let instants_blocks = match pass1.instant_to_blocks_map.get(instant_time) {
            Some(blocks) if !blocks.is_empty() => blocks,
            _ => continue,
        };

        let first_block = &instants_blocks[0];

        // Check for compacted blocks (has COMPACTED_BLOCK_TIMES header)
        if let Some(compacted_times) = first_block
            .header
            .get(&BlockMetadataKey::CompactedBlockTimes)
        {
            for original_instant in compacted_times.split(',') {
                let original_instant = original_instant.trim().to_string();
                let final_instant = block_time_to_compaction_block_time_map
                    .get(instant_time)
                    .cloned()
                    .unwrap_or_else(|| instant_time.clone());
                block_time_to_compaction_block_time_map.insert(original_instant, final_instant);
            }
        } else {
            // Normal data block — check if it's been compacted
            let compacted_final = block_time_to_compaction_block_time_map.get(instant_time);

            if let Some(final_instant) = compacted_final {
                let final_instant = final_instant.clone();
                if instant_times_included.contains(&final_instant) {
                    continue;
                }
                if let Some(blocks) = pass1.instant_to_blocks_map.get(&final_instant) {
                    let mut reversed = blocks.clone();
                    reversed.reverse();
                    for block in reversed {
                        current_instant_log_blocks.push_back(block);
                    }
                }
                instant_times_included.insert(final_instant.clone());
                valid_block_instants.push(final_instant);
            } else {
                // Not compacted — add blocks directly
                // Java: Collections.reverse(logBlocks) then forEach(addLast)
                let blocks = pass1
                    .instant_to_blocks_map
                    .get(instant_time)
                    .cloned()
                    .unwrap_or_default();
                let mut reversed = blocks;
                reversed.reverse();
                for block in reversed {
                    current_instant_log_blocks.push_back(block);
                }
                instant_times_included.insert(instant_time.clone());
                valid_block_instants.push(instant_time.clone());
            }
        }
    }

    log::debug!(
        "[Pass2] complete: {} blocks in deque, valid_instants={:?}",
        current_instant_log_blocks.len(),
        valid_block_instants,
    );

    Pass2Result {
        current_instant_log_blocks,
        instant_times_included,
        valid_block_instants,
    }
}

// =========================================================================
// BaseHoodieLogRecordReader
// =========================================================================

/// The base log record reader implementing the 3-pass scanning algorithm.
///
/// Mirrors Java's `BaseHoodieLogRecordReader<T>` (abstract base class).
///
/// ## Java hierarchy:
/// ```text
/// BaseHoodieLogRecordReader<T>  (abstract — this struct)
///   └─ HoodieMergedLogRecordReader<T>  (concrete — wraps this via composition)
/// ```
///
/// ## Fields matching Java:
/// - `reader_context` ↔ `readerContext`
/// - `storage` ↔ `storage`
/// - `log_file_paths` ↔ `logFiles`
/// - `latest_instant_time` ↔ `latestInstantTime`
/// - `instant_range` ↔ `instantRange`
/// - `force_full_scan` ↔ `forceFullScan`
/// - `record_buffer` ↔ `recordBuffer`
/// - `allow_inflight_instants` ↔ `allowInflightInstants`
pub struct BaseHoodieLogRecordReader {
    pub reader_context: Arc<ReaderContext>,
    pub storage: Arc<Storage>,
    pub log_file_paths: Vec<String>,
    pub latest_instant_time: String,
    /// Mirrors Java's `private final Option<InstantRange> instantRange`.
    pub instant_range: Option<InstantRange>,
    /// Mirrors Java's `protected final boolean forceFullScan`.
    /// When true, scanning happens eagerly in the constructor.
    pub force_full_scan: bool,
    pub record_buffer: Box<dyn HoodieFileGroupRecordBuffer>,
    pub allow_inflight_instants: bool,
    /// Inputs for the Gate-3 completed/inflight check (C-INFLIGHT-DELTA). `Some` only for
    /// table version < 8 (v1 timeline layout); `None` for v8+ and when no timeline is available,
    /// in which case Gate 3 is a no-op. Populated by the builder / FFI wiring from the active
    /// timeline (completed + inflight instant sets + the first active instant).
    pub completion_gate_inputs: Option<CompletionGateInputs>,

    // ── Stats / state (mirrors Java's AtomicLong counters + progress) ──
    pub valid_block_instants: Vec<String>,
    pub total_log_files: u64,
    pub total_log_blocks: u64,
    pub total_log_records: u64,
    pub total_corrupt_blocks: u64,
    pub total_rollbacks: u64,
    /// Mirrors Java's `private float progress` (0.0 → 1.0).
    pub progress: f32,

    // ── Stage timings (perf harness) ──────────────────────
    /// Wall ms spent reading log-block metadata + bytes off storage (Pass 1).
    pub log_block_read_ms: u64,
    /// Wall ms spent in Pass-3 block dispatch (inflate/decode + merge insert).
    pub merge_insert_ms: u64,
}

impl BaseHoodieLogRecordReader {
    /// Mirrors Java's `scanInternal(Option<KeySpec> keySpecOpt, boolean skipProcessingBlocks)`.
    ///
    /// # Arguments
    /// - `skip_processing_blocks` — when `true`, Pass 3 (block processing) is skipped.
    ///   Java's `HoodieMergedLogRecordReader.scan(true)` uses this to collect block
    ///   metadata without actually merging records.
    pub async fn scan_internal(&mut self, skip_processing_blocks: bool) -> Result<()> {
        // Reset state (mirrors Java: currentInstantLogBlocks = new ArrayDeque<>(), progress = 0.0f, ...)
        self.valid_block_instants.clear();
        self.progress = 0.0;
        self.total_log_files = 0;
        self.total_log_blocks = 0;
        self.total_log_records = 0;
        self.total_corrupt_blocks = 0;
        self.total_rollbacks = 0;

        let timezone = self.reader_context.timezone();

        // Read all blocks as metadata-only (no content decoding).
        // Mirrors Java's Pass 1 where content bytes are skipped via seek().
        // Content is loaded lazily via inflate() during Pass 3.
        let mut all_blocks: Vec<LogBlock> = Vec::new();
        let hudi_configs = Arc::new(crate::config::HudiConfigs::new(
            self.reader_context.table_config.clone(),
        ));
        let unbounded_range =
            InstantRange::new(self.reader_context.timezone(), None, None, false, true);
        profile_once!(self.log_block_read_ms, {
            for path in &self.log_file_paths.clone() {
                // A5 / R5: stream the log file in bounded windows for the Pass-1
                // header sweep instead of buffering the whole file. Each block
                // gets a ranged fetcher so Pass-3 inflate reads only its content.
                // Upstream sweeps block headers in bounded windows and inflates
                // each block's content later. This crate's log file reader is
                // whole-file, so the blocks come back with their content already
                // read — same block set, more memory, and no more than the
                // existing read path already uses. The range is left unbounded so
                // the gates below decide what is admitted, rather than filtering
                // twice with different rules.
                // A log record can update a row, so a predicate may only be
                // evaluated before the merge when the merge cannot change its
                // answer. Log blocks exist only on merge-on-read, so the gate
                // reduces to a predicate over primary keys, which are immutable
                // across upserts. Anything else is left for the post-merge
                // filter.
                let row_filter = if self.reader_context.can_push_row_filter() {
                    self.reader_context.row_filter_builder.clone()
                } else {
                    None
                };
                let mut reader =
                    LogFileReader::new(hudi_configs.clone(), self.storage.clone(), path)
                        .await?
                        .with_row_filter(row_filter)
                        .with_required_schema(
                            self.reader_context
                                .schema_handler
                                .required_schema_json
                                .clone(),
                        );
                let blocks = reader.read_all_blocks(&unbounded_range)?;
                self.total_log_files += 1;
                all_blocks.extend(blocks);
            }
        });

        // Pass 1: Forward scan with 5 gates. The completed/inflight gate (Gate 3)
        // is applied only when the timeline sets were supplied (table version < 8).
        let completion_gate = self
            .completion_gate_inputs
            .as_ref()
            .map(CompletionGate::new);
        let mut pass1 = forward_scan_pass1(
            all_blocks,
            &self.latest_instant_time,
            &self.instant_range,
            &timezone,
            completion_gate.as_ref(),
        )?;

        self.total_log_blocks = pass1.total_log_blocks;
        self.total_corrupt_blocks = pass1.total_corrupt_blocks;
        self.total_rollbacks = pass1.total_rollbacks;

        // Pass 2: Reverse iteration with compaction resolution
        let mut pass2 = reverse_scan_pass2(&mut pass1);

        self.valid_block_instants = pass2.valid_block_instants;

        log::debug!(
            "Pass 1+2 complete: {} blocks queued, {} valid instants",
            pass2.current_instant_log_blocks.len(),
            self.valid_block_instants.len()
        );

        // Pass 3: Process queued blocks (oldest → newest via pop_back).
        // Mirrors Java: if (!currentInstantLogBlocks.isEmpty() && !skipProcessingBlocks) { ... }
        if !pass2.current_instant_log_blocks.is_empty() && !skip_processing_blocks {
            log::debug!("Merging the final data blocks");
            profile_once!(
                self.merge_insert_ms,
                self.process_queued_blocks_for_instant(&mut pass2.current_instant_log_blocks)
            )?;
        }

        // Done
        self.progress = 1.0;
        self.total_log_records = self.record_buffer.get_total_log_records();

        Ok(())
    }

    /// Mirrors Java's `shouldLookupRecords()`.
    ///
    /// Point-wise record lookups are only enabled when scanner is NOT in
    /// full-scan mode.
    pub fn should_lookup_records(&self) -> bool {
        !self.force_full_scan
    }

    // ── Getters (mirrors Java's getter methods on BaseHoodieLogRecordReader) ──

    pub fn get_progress(&self) -> f32 {
        self.progress
    }

    pub fn get_total_log_files(&self) -> u64 {
        self.total_log_files
    }

    pub fn get_total_log_records(&self) -> u64 {
        self.total_log_records
    }

    pub fn get_total_log_blocks(&self) -> u64 {
        self.total_log_blocks
    }

    pub fn get_total_rollbacks(&self) -> u64 {
        self.total_rollbacks
    }

    pub fn get_total_corrupt_blocks(&self) -> u64 {
        self.total_corrupt_blocks
    }

    pub fn get_valid_block_instants(&self) -> &[String] {
        &self.valid_block_instants
    }

    /// Mirrors Java's `processQueuedBlocksForInstant(Deque<HoodieLogBlock>, ...)`.
    ///
    /// Drains the deque from the back (oldest→newest via `pop_back`/`pollLast`).
    /// Dispatches each block to the record buffer — the buffer is responsible for
    /// inflating, extracting records, and deflating (matching Java's design where
    /// `processQueuedBlocksForInstant` does NOT call `inflate()`).
    fn process_queued_blocks_for_instant(
        &mut self,
        log_blocks: &mut VecDeque<LogBlock>,
    ) -> Result<()> {
        log::debug!(
            "[Pass3] processQueuedBlocksForInstant: {} blocks to process (pop_back = oldest first)",
            log_blocks.len(),
        );
        let mut block_num = 0u64;
        // The same settings the sweep would have decoded with, so an inflated
        // block is identical to one that was read eagerly.
        let block_decoder = crate::file_group::log_file::content::Decoder::new(Arc::new(
            crate::config::HudiConfigs::new(self.reader_context.table_config.clone()),
        ))
        .with_row_filter(if self.reader_context.can_push_row_filter() {
            self.reader_context.row_filter_builder.clone()
        } else {
            None
        })
        .with_required_schema(
            self.reader_context
                .schema_handler
                .required_schema_json
                .clone(),
        );

        while let Some(mut block) = log_blocks.pop_back() {
            block_num += 1;
            let instant_time = block.instant_time().unwrap_or("unknown").to_string();
            log::debug!(
                "[Pass3] block #{block_num}: type={:?} instant={instant_time}",
                block.block_type,
            );

            // Read this block's content now that it is known to be admitted. A
            // block that already has content passes straight through.
            match block.block_type {
                BlockType::AvroData | BlockType::ParquetData | BlockType::Delete => {
                    block.inflate(&block_decoder)?;
                }
                _ => {}
            }

            match block.block_type {
                BlockType::AvroData | BlockType::ParquetData => {
                    self.record_buffer.process_data_block(&mut block)?;
                    log::debug!(
                        "[Pass3] after processing data block #{block_num}: buffer size={}",
                        self.record_buffer.size(),
                    );
                }
                BlockType::Delete => {
                    self.record_buffer.process_delete_block(&mut block)?;
                }
                BlockType::Corrupted => {
                    log::warn!("Found corrupt block not rolled back");
                }
                _ => {}
            }
        }
        Ok(())
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::log_file::log_block::{CommandBlock, LogBlockContent};
    use crate::file_group::log_file::log_format::LogFormatVersion;
    use crate::file_group::reader_v2::MAX_INSTANT_TIME;

    // ── Helpers ──────────────────────────────────────────────────────────

    /// Create a data block with given instant time and a tag for identification.
    fn make_data_block(instant_time: &str) -> LogBlock {
        let mut header = HashMap::new();
        header.insert(BlockMetadataKey::InstantTime, instant_time.to_string());
        LogBlock::new(
            LogFormatVersion::V1,
            BlockType::AvroData,
            header,
            LogBlockContent::Empty,
            HashMap::new(),
        )
    }

    /// Create a delete block with given instant time.
    fn make_delete_block(instant_time: &str) -> LogBlock {
        let mut header = HashMap::new();
        header.insert(BlockMetadataKey::InstantTime, instant_time.to_string());
        LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Delete,
            header,
            LogBlockContent::Empty,
            HashMap::new(),
        )
    }

    /// Create a corrupt block.
    fn make_corrupt_block() -> LogBlock {
        LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Corrupted,
            HashMap::new(),
            LogBlockContent::Empty,
            HashMap::new(),
        )
    }

    /// Create a rollback command block targeting the given instant.
    fn make_rollback_block(instant_time: &str, target_instant: &str) -> LogBlock {
        let mut header = HashMap::new();
        header.insert(BlockMetadataKey::InstantTime, instant_time.to_string());
        header.insert(
            BlockMetadataKey::TargetInstantTime,
            target_instant.to_string(),
        );
        header.insert(
            BlockMetadataKey::CommandBlockType,
            (CommandBlock::Rollback as u32).to_string(),
        );
        LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Command,
            header,
            LogBlockContent::Empty,
            HashMap::new(),
        )
    }

    /// Create a compacted block with COMPACTED_BLOCK_TIMES header.
    fn make_compacted_block(instant_time: &str, compacted_times: &str) -> LogBlock {
        let mut header = HashMap::new();
        header.insert(BlockMetadataKey::InstantTime, instant_time.to_string());
        header.insert(
            BlockMetadataKey::CompactedBlockTimes,
            compacted_times.to_string(),
        );
        LogBlock::new(
            LogFormatVersion::V1,
            BlockType::AvroData,
            header,
            LogBlockContent::Empty,
            HashMap::new(),
        )
    }

    fn get_instant(block: &LogBlock) -> String {
        block.instant_time().unwrap().to_string()
    }

    // =====================================================================
    // Pass 1: 4-Gate Filtering Tests
    // =====================================================================

    /// Gate 1: Corrupt blocks are skipped.
    ///
    /// Given: 1 corrupt block + 1 data block
    /// When:  forward_scan_pass1()
    /// Then:  total_corrupt_blocks=1, only data block in map
    #[test]
    fn test_pass1_gate1_corrupt_block_skipped() {
        let blocks = vec![make_corrupt_block(), make_data_block("20250101")];

        let result = forward_scan_pass1(blocks, MAX_INSTANT_TIME, &None, "utc", None).unwrap();

        assert_eq!(result.total_corrupt_blocks, 1);
        assert_eq!(result.ordered_instants_list, vec!["20250101"]);
        assert_eq!(result.instant_to_blocks_map.len(), 1);
    }

    /// Gate 2: Future blocks (instant > latestInstantTime) are skipped.
    ///
    /// Given: latest_instant_time="20250103", block at "20250105"
    /// When:  forward_scan_pass1()
    /// Then:  future block not in map
    #[test]
    fn test_pass1_gate2_future_block_skipped() {
        let blocks = vec![
            make_data_block("20250101"),
            make_data_block("20250105"), // future
        ];

        let result = forward_scan_pass1(blocks, "20250103", &None, "utc", None).unwrap();

        assert_eq!(result.ordered_instants_list, vec!["20250101"]);
        assert!(!result.instant_to_blocks_map.contains_key("20250105"));
    }

    /// Gate 4: Instant range filter skips out-of-range blocks.
    ///
    /// Given: instant_range up to "20250102000000000", block at "20250103000000000"
    /// When:  forward_scan_pass1()
    /// Then:  out-of-range block skipped
    #[test]
    fn test_pass1_gate4_instant_range_filter() {
        let blocks = vec![
            make_data_block("20250101000000000"),
            make_data_block("20250103000000000"), // out of range
        ];
        let range = Some(InstantRange::up_to("20250102000000000", "utc"));

        let result = forward_scan_pass1(blocks, MAX_INSTANT_TIME, &range, "utc", None).unwrap();

        assert_eq!(result.ordered_instants_list, vec!["20250101000000000"]);
    }

    /// C-INFLIGHT-DELTA fix (table version < 8): an UNCOMMITTED "straddling" instant -- one whose
    /// time is BELOW the latest completed instant (the high-watermark) and which has NO in-log
    /// rollback command block (a pending/failed write, or a concurrent writer's inflight commit) --
    /// must be EXCLUDED (not merged) once the completed/inflight sets are supplied.
    ///
    /// Conceptual timeline:
    ///   completed:               20250101, 20250103  (latest completed = 20250103 = high-watermark)
    ///   uncommitted (inflight):  20250102            <- straddles between the two completed ones
    ///
    /// The JVM reader (BaseHoodieLogRecordReader.scanInternalV1, tableVersion < 8) skips 20250102
    /// because it is not in filterCompletedInstants() (and is in filterInflights()). With Gate 3
    /// wired, hudi-rs matches: Gate 2 admits it (20250102 <= 20250103), but Gate 3 rejects it
    /// (uncommitted + inflight), and -- unlike test_pass1_rollback_removes_instant -- there is no
    /// rollback command block targeting it.
    #[test]
    fn test_pass1_gate3_excludes_uncommitted_straddling_instant_c_inflight_delta() {
        let blocks = vec![
            make_data_block("20250101"), // committed
            make_data_block("20250102"), // UNCOMMITTED (inflight/failed), straddling, no rollback
            make_data_block("20250103"), // committed, latest
        ];

        let gate_inputs = CompletionGateInputs {
            completed_instants: ["20250101".to_string(), "20250103".to_string()]
                .into_iter()
                .collect(),
            inflight_instants: ["20250102".to_string()].into_iter().collect(),
            archived_boundary: None,
        };
        let gate = CompletionGate::new(&gate_inputs);

        // high-watermark = latest completed = 20250103; no incremental range (snapshot read).
        let result = forward_scan_pass1(blocks, "20250103", &None, "utc", Some(&gate)).unwrap();

        // FIXED: the uncommitted straddling instant is excluded; only committed instants remain.
        assert!(
            !result.instant_to_blocks_map.contains_key("20250102"),
            "C-INFLIGHT-DELTA: uncommitted straddling instant 20250102 must be excluded"
        );
        assert_eq!(
            result.ordered_instants_list,
            vec!["20250101", "20250103"],
            "only the two committed instants remain"
        );
    }

    /// Without the completion gate (v8+ tables, or no timeline available) Gate 3 is a no-op, so the
    /// straddling instant is still admitted -- proving the fix is scoped to table version < 8 and is
    /// purely additive (prior callers pass `None` and see unchanged behavior). v8+ safety is
    /// enforced elsewhere: per-delta-commit log files are excluded at the completion-time file-slice
    /// level, so the same block never reaches Pass 1.
    #[test]
    fn test_pass1_gate3_absent_gate_admits_straddling_instant() {
        let blocks = vec![
            make_data_block("20250101"),
            make_data_block("20250102"), // uncommitted, but no gate supplied
            make_data_block("20250103"),
        ];

        let result = forward_scan_pass1(blocks, "20250103", &None, "utc", None).unwrap();

        assert!(result.instant_to_blocks_map.contains_key("20250102"));
        assert_eq!(
            result.ordered_instants_list,
            vec!["20250101", "20250102", "20250103"]
        );
    }

    /// Gate 3 treats an instant older than the archived boundary as committed (archival only removes
    /// completed instants), so an old archived data block is admitted while a genuinely inflight one
    /// is still excluded.
    #[test]
    fn test_pass1_gate3_archived_boundary_is_committed() {
        let blocks = vec![
            make_data_block("20250101"), // archived (before boundary) -> committed
            make_data_block("20250102"), // inflight -> excluded
            make_data_block("20250103"), // completed
        ];

        let gate_inputs = CompletionGateInputs {
            completed_instants: ["20250103".to_string()].into_iter().collect(),
            inflight_instants: ["20250102".to_string()].into_iter().collect(),
            archived_boundary: Some("20250102".to_string()),
        };
        let gate = CompletionGate::new(&gate_inputs);

        let result = forward_scan_pass1(blocks, "20250103", &None, "utc", Some(&gate)).unwrap();

        assert_eq!(
            result.ordered_instants_list,
            vec!["20250101", "20250103"],
            "archived 20250101 admitted, inflight 20250102 excluded"
        );
    }

    /// Focused `admits()` audit (defect-3 requirement (a)): given a
    /// correctly-populated completed set, a committed instant below the
    /// high-watermark is admitted while a separate uncommitted straddling instant
    /// is excluded. This is the unit-level counterpart of the Pass-1 integration
    /// test and pins the winner-selection logic directly.
    #[test]
    fn test_completion_gate_admits_committed_excludes_uncommitted_straddling() {
        let gate_inputs = CompletionGateInputs {
            completed_instants: ["20250101".to_string(), "20250103".to_string()]
                .into_iter()
                .collect(),
            inflight_instants: ["20250102".to_string()].into_iter().collect(),
            archived_boundary: None,
        };
        let gate = CompletionGate::new(&gate_inputs);

        assert!(
            gate.admits("20250101"),
            "a committed instant below the watermark is admitted"
        );
        assert!(
            gate.admits("20250103"),
            "the latest committed instant is admitted"
        );
        assert!(
            !gate.admits("20250102"),
            "the uncommitted (inflight) straddling instant is excluded"
        );
        assert!(
            !gate.admits("20250104"),
            "an unknown instant absent from the completed set is excluded"
        );
    }

    /// Defect-3 requirement (b): an empty completed set with no archived boundary
    /// carries no positive completion information, so the gate is a no-op and
    /// admits every instant — it must NOT exclude everything (which would drop all
    /// committed deltas and return base-only data, the C-INFLIGHT silent-wrong).
    #[test]
    fn test_completion_gate_empty_completed_set_admits_all() {
        let gate_inputs = CompletionGateInputs::default();
        let gate = CompletionGate::new(&gate_inputs);
        assert!(
            gate.admits("20250102"),
            "an empty/None gate must be a no-op (admit-all), not exclude-all"
        );

        // An empty completed set is still a no-op even if an (inconsistent)
        // inflight set is present, because there is no committed reference to gate
        // against; excluding would risk dropping committed deltas.
        let with_inflight = CompletionGateInputs {
            completed_instants: HashSet::new(),
            inflight_instants: ["20250102".to_string()].into_iter().collect(),
            archived_boundary: None,
        };
        let gate = CompletionGate::new(&with_inflight);
        assert!(
            gate.admits("20250102"),
            "no completed reference -> admit-all fallback (do not silently drop committed data)"
        );

        // But an archived boundary alone IS positive information: instants at/after
        // it (and absent from the empty completed set) are correctly excluded.
        let with_boundary = CompletionGateInputs {
            completed_instants: HashSet::new(),
            inflight_instants: HashSet::new(),
            archived_boundary: Some("20250101".to_string()),
        };
        let gate = CompletionGate::new(&with_boundary);
        assert!(
            gate.admits("20250100"),
            "archived (< boundary) -> committed"
        );
        assert!(
            !gate.admits("20250102"),
            "at/after the boundary and not in the (empty) completed set -> excluded"
        );
    }

    /// `is_unpopulated` distinguishes a mis-wired (enabled-but-empty) gate — which
    /// `forward_scan_pass1` warns about — from a gate carrying positive completion info.
    #[test]
    fn test_completion_gate_is_unpopulated() {
        let empty = CompletionGateInputs::default();
        assert!(
            CompletionGate::new(&empty).is_unpopulated(),
            "empty completed set + no boundary -> unpopulated (mis-wire signal)"
        );

        // An inflight-only set is still unpopulated: it carries no committed reference.
        let inflight_only = CompletionGateInputs {
            completed_instants: HashSet::new(),
            inflight_instants: ["20250102".to_string()].into_iter().collect(),
            archived_boundary: None,
        };
        assert!(
            CompletionGate::new(&inflight_only).is_unpopulated(),
            "inflight-only (no completed, no boundary) -> unpopulated"
        );

        // Either a non-empty completed set OR an archived boundary is positive info.
        let with_completed = CompletionGateInputs {
            completed_instants: ["20250101".to_string()].into_iter().collect(),
            inflight_instants: HashSet::new(),
            archived_boundary: None,
        };
        assert!(
            !CompletionGate::new(&with_completed).is_unpopulated(),
            "a non-empty completed set is positive info -> populated"
        );
        let with_boundary = CompletionGateInputs {
            completed_instants: HashSet::new(),
            inflight_instants: HashSet::new(),
            archived_boundary: Some("20250101".to_string()),
        };
        assert!(
            !CompletionGate::new(&with_boundary).is_unpopulated(),
            "an archived boundary is positive info -> populated"
        );
    }

    /// Rollback command removes target instant from maps atomically.
    ///
    /// Given: B1(i=20250101), B2(i=20250102), R1(rollback target=20250102)
    /// When:  forward_scan_pass1()
    /// Then:  instant 20250102 removed, only 20250101 remains, total_rollbacks=1
    #[test]
    fn test_pass1_rollback_removes_instant() {
        let blocks = vec![
            make_data_block("20250101"),
            make_data_block("20250102"),
            make_rollback_block("20250103", "20250102"),
        ];

        let result = forward_scan_pass1(blocks, MAX_INSTANT_TIME, &None, "utc", None).unwrap();

        assert_eq!(result.total_rollbacks, 1);
        assert_eq!(result.ordered_instants_list, vec!["20250101"]);
        assert!(!result.instant_to_blocks_map.contains_key("20250102"));
    }

    /// orderedInstantsList has no duplicates even when same instant appears in multiple blocks.
    ///
    /// Given: Two blocks with same instant time
    /// When:  forward_scan_pass1()
    /// Then:  orderedInstantsList has one entry, map has 2 blocks
    #[test]
    fn test_pass1_ordered_instants_no_duplicates() {
        let blocks = vec![
            make_data_block("20250101"),
            make_data_block("20250101"), // same instant
        ];

        let result = forward_scan_pass1(blocks, MAX_INSTANT_TIME, &None, "utc", None).unwrap();

        assert_eq!(result.ordered_instants_list, vec!["20250101"]);
        assert_eq!(result.instant_to_blocks_map["20250101"].len(), 2);
    }

    /// orderedInstantsList and instantToBlocksMap key sets match.
    ///
    /// Given: Blocks from 3 instants
    /// When:  forward_scan_pass1()
    /// Then:  Both have exactly the same set of instant strings
    #[test]
    fn test_pass1_list_and_map_key_parity() {
        let blocks = vec![
            make_data_block("20250101"),
            make_data_block("20250102"),
            make_data_block("20250103"),
        ];

        let result = forward_scan_pass1(blocks, MAX_INSTANT_TIME, &None, "utc", None).unwrap();

        let list_set: HashSet<&String> = result.ordered_instants_list.iter().collect();
        let map_set: HashSet<&String> = result.instant_to_blocks_map.keys().collect();
        assert_eq!(list_set, map_set);
    }

    // =====================================================================
    // Pass 2: Reverse Iteration & Deque Building
    // =====================================================================

    /// Invariant 1: instant_times_included and valid_block_instants contain
    /// exactly the same instant strings.
    ///
    /// Given: 3 instants [t1, t2, t3], no compaction
    /// When:  reverse_scan_pass2()
    /// Then:  instant_times_included == valid_block_instants (as sets)
    #[test]
    fn test_pass2_invariant1_included_equals_valid() {
        let blocks = vec![
            make_data_block("t1"),
            make_data_block("t2"),
            make_data_block("t3"),
        ];
        let mut pass1 = forward_scan_pass1(blocks, "z", &None, "utc", None).unwrap();
        let pass2 = reverse_scan_pass2(&mut pass1);

        let included: HashSet<&String> = pass2.instant_times_included.iter().collect();
        let valid: HashSet<&String> = pass2.valid_block_instants.iter().collect();
        assert_eq!(included, valid, "Invariant 1: included == valid");
    }

    /// Invariant 2: Every instant in included/valid has blocks in deque, and vice versa.
    ///
    /// Given: 3 instants with data blocks
    /// When:  reverse_scan_pass2()
    /// Then:  All blocks in deque belong to instants in valid_block_instants
    #[test]
    fn test_pass2_invariant2_blocks_match_instants() {
        let blocks = vec![
            make_data_block("t1"),
            make_data_block("t2"),
            make_data_block("t2"), // 2 blocks for t2
            make_data_block("t3"),
        ];
        let mut pass1 = forward_scan_pass1(blocks, "z", &None, "utc", None).unwrap();
        let pass2 = reverse_scan_pass2(&mut pass1);

        // All blocks in deque belong to valid instants
        for block in &pass2.current_instant_log_blocks {
            let instant = get_instant(block);
            assert!(
                pass2.instant_times_included.contains(&instant),
                "Block instant {instant} should be in instant_times_included"
            );
        }
        // Every valid instant has at least one block in deque
        for instant in &pass2.valid_block_instants {
            let has_block = pass2
                .current_instant_log_blocks
                .iter()
                .any(|b| get_instant(b) == *instant);
            assert!(
                has_block,
                "Valid instant {instant} should have blocks in deque"
            );
        }
    }

    /// Invariant 3: Compaction deduplication prevents double-enqueue.
    ///
    /// Given: orderedInstants=[i1, i2, M1], M1.CompactedBlockTimes="i1,i2"
    /// When:  reverse_scan_pass2()
    /// Then:  M1's blocks used once, i1 and i2 blocks skipped.
    ///        valid_block_instants contains only M1 (+ potentially i4 etc.)
    #[test]
    fn test_pass2_invariant3_compaction_dedup() {
        let blocks = vec![
            make_data_block("i1"),
            make_data_block("i2"),
            make_compacted_block("i3", "i1,i2"),
        ];
        let mut pass1 = forward_scan_pass1(blocks, "z", &None, "utc", None).unwrap();
        let pass2 = reverse_scan_pass2(&mut pass1);

        // Only i3 (compacted) should be in valid_block_instants
        assert_eq!(pass2.valid_block_instants, vec!["i3"]);
        assert_eq!(pass2.instant_times_included.len(), 1);
        assert!(pass2.instant_times_included.contains("i3"));

        // Deque should only have i3's block
        assert_eq!(pass2.current_instant_log_blocks.len(), 1);
        let block = &pass2.current_instant_log_blocks[0];
        assert_eq!(get_instant(block), "i3");
    }

    /// Invariant 4: Deque is ordered latest-first. pollLast produces oldest-first.
    ///
    /// Given: orderedInstantsList = [t1, t2, t3]
    /// When:  reverse_scan_pass2()
    /// Then:  Deque front=t3, back=t1.
    ///        pop_back order: t1 → t2 → t3 (oldest first)
    #[test]
    fn test_pass2_invariant4_deque_ordering() {
        let blocks = vec![
            make_data_block("t1"),
            make_data_block("t2"),
            make_data_block("t3"),
        ];
        let mut pass1 = forward_scan_pass1(blocks, "z", &None, "utc", None).unwrap();
        let mut pass2 = reverse_scan_pass2(&mut pass1);

        // pop_back drains tail-first = oldest first
        let first = pass2.current_instant_log_blocks.pop_back().unwrap();
        assert_eq!(
            get_instant(&first),
            "t1",
            "First pop_back should be oldest (t1)"
        );

        let second = pass2.current_instant_log_blocks.pop_back().unwrap();
        assert_eq!(get_instant(&second), "t2", "Second pop_back should be t2");

        let third = pass2.current_instant_log_blocks.pop_back().unwrap();
        assert_eq!(
            get_instant(&third),
            "t3",
            "Third pop_back should be newest (t3)"
        );

        assert!(pass2.current_instant_log_blocks.is_empty());
    }

    /// Multi-block instant: within-instant order preserved after double reversal.
    ///
    /// Given: t1→[A], t2→[B,C] (B seen before C in forward scan)
    /// When:  reverse_scan_pass2(), then drain via pop_back
    /// Then:  pop_back order: A(t1) → B(t2) → C(t2)
    ///        (original file-read order restored within each instant)
    #[test]
    fn test_pass2_multi_block_instant_order() {
        // t1: 1 block, t2: 2 blocks (B seen first, then C)
        let blocks = vec![
            make_data_block("t1"),   // A
            make_data_block("t2"),   // B
            make_delete_block("t2"), // C (delete block for same instant)
        ];
        let mut pass1 = forward_scan_pass1(blocks, "z", &None, "utc", None).unwrap();

        // Verify pass1 accumulated correctly
        assert_eq!(pass1.instant_to_blocks_map["t2"].len(), 2);

        let mut pass2 = reverse_scan_pass2(&mut pass1);

        // Drain via pop_back: should be t1's blocks, then t2's in original order
        let b1 = pass2.current_instant_log_blocks.pop_back().unwrap();
        assert_eq!(get_instant(&b1), "t1");
        assert_eq!(b1.block_type, BlockType::AvroData);

        let b2 = pass2.current_instant_log_blocks.pop_back().unwrap();
        assert_eq!(get_instant(&b2), "t2");
        assert_eq!(b2.block_type, BlockType::AvroData); // B was first

        let b3 = pass2.current_instant_log_blocks.pop_back().unwrap();
        assert_eq!(get_instant(&b3), "t2");
        assert_eq!(b3.block_type, BlockType::Delete); // C was second
    }

    /// Concrete trace from howLogBlocksAreFiletered.md:
    ///
    /// Given: log files with B1(i=20250101), B2(i=20250102), B3(i=20250102),
    ///        B4(i=20250103), R1(rollback target=20250102)
    /// When:  Pass 1 then Pass 2
    /// Then:  After Pass 1: orderedInstantsList=[20250101, 20250103]
    ///        After Pass 2: pop_back order = B1(20250101), B4(20250103)
    #[test]
    fn test_concrete_trace_from_docs() {
        let blocks = vec![
            make_data_block("20250101"),                 // B1
            make_data_block("20250102"),                 // B2
            make_data_block("20250102"),                 // B3
            make_data_block("20250103"),                 // B4
            make_rollback_block("20250104", "20250102"), // R1: rollback 20250102
        ];

        let mut pass1 = forward_scan_pass1(blocks, MAX_INSTANT_TIME, &None, "utc", None).unwrap();

        assert_eq!(pass1.ordered_instants_list, vec!["20250101", "20250103"]);
        assert!(!pass1.instant_to_blocks_map.contains_key("20250102"));

        let mut pass2 = reverse_scan_pass2(&mut pass1);

        // pop_back: B1(20250101) first, then B4(20250103)
        let first = pass2.current_instant_log_blocks.pop_back().unwrap();
        assert_eq!(get_instant(&first), "20250101");

        let second = pass2.current_instant_log_blocks.pop_back().unwrap();
        assert_eq!(get_instant(&second), "20250103");

        assert!(pass2.current_instant_log_blocks.is_empty());
    }

    /// From FS_logFileReadInputs_fileOrderRequestTime.md: 5-block example.
    ///
    /// Given: t1→[A,B], t2→[C], t3→[D,E]
    /// When:  Pass 2 reverse + pop_back drain
    /// Then:  pop_back order: A, B, C, D, E (oldest→newest, within-instant order preserved)
    #[test]
    fn test_five_block_example_from_docs() {
        // Simulate: t1→[A,B], t2→[C], t3→[D,E]
        let blocks = vec![
            make_data_block("t1"),   // A
            make_data_block("t1"),   // B
            make_data_block("t2"),   // C
            make_data_block("t3"),   // D
            make_delete_block("t3"), // E (different block type to distinguish)
        ];

        let mut pass1 = forward_scan_pass1(blocks, "z", &None, "utc", None).unwrap();
        assert_eq!(pass1.ordered_instants_list, vec!["t1", "t2", "t3"]);
        assert_eq!(pass1.instant_to_blocks_map["t1"].len(), 2);
        assert_eq!(pass1.instant_to_blocks_map["t2"].len(), 1);
        assert_eq!(pass1.instant_to_blocks_map["t3"].len(), 2);

        let mut pass2 = reverse_scan_pass2(&mut pass1);

        // Deque has 5 blocks. pop_back should produce: A,B,C,D,E
        assert_eq!(pass2.current_instant_log_blocks.len(), 5);

        let a = pass2.current_instant_log_blocks.pop_back().unwrap();
        assert_eq!(get_instant(&a), "t1");
        assert_eq!(a.block_type, BlockType::AvroData); // A

        let b = pass2.current_instant_log_blocks.pop_back().unwrap();
        assert_eq!(get_instant(&b), "t1");
        assert_eq!(b.block_type, BlockType::AvroData); // B

        let c = pass2.current_instant_log_blocks.pop_back().unwrap();
        assert_eq!(get_instant(&c), "t2"); // C

        let d = pass2.current_instant_log_blocks.pop_back().unwrap();
        assert_eq!(get_instant(&d), "t3");
        assert_eq!(d.block_type, BlockType::AvroData); // D

        let e = pass2.current_instant_log_blocks.pop_back().unwrap();
        assert_eq!(get_instant(&e), "t3");
        assert_eq!(e.block_type, BlockType::Delete); // E
    }

    // =====================================================================
    // Commit-Time Ordering: Full chain from log file order → deque →
    // processNextDataRecord → running map → correct final state
    //
    // From FS_logFileReadInputs_fileOrderRequestTime.md:
    //   "The double-reversal (reverse iteration of instants + addLast
    //    in the deque + pollLast to consume) produces oldest-first
    //    processing. The last writer to records.put(key, ...) is
    //    always the newest deltaCommitTime."
    // =====================================================================

    use crate::config::table::HudiTableConfig;
    use crate::file_group::reader_v2::buffer::key_based::KeyBasedFileGroupRecordBuffer;
    use crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler;
    use crate::file_group::record_batches::RecordBatches;
    use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_schema() -> std::sync::Arc<Schema> {
        std::sync::Arc::new(Schema::new(vec![
            Field::new("_hoodie_record_key", DataType::Utf8, false),
            Field::new("counter", DataType::Int32, false),
            Field::new("ts", DataType::Int64, false),
        ]))
    }

    fn test_batch(records: &[(&str, i32, i64)]) -> RecordBatch {
        let schema = test_schema();
        let keys: Vec<&str> = records.iter().map(|(k, _, _)| *k).collect();
        let counters: Vec<i32> = records.iter().map(|(_, c, _)| *c).collect();
        let timestamps: Vec<i64> = records.iter().map(|(_, _, t)| *t).collect();
        RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(StringArray::from(keys)),
                std::sync::Arc::new(Int32Array::from(counters)),
                std::sync::Arc::new(Int64Array::from(timestamps)),
            ],
        )
        .unwrap()
    }

    /// Create a data block with actual RecordBatch content for a given instant.
    fn make_data_block_with_content(instant_time: &str, records: &[(&str, i32, i64)]) -> LogBlock {
        let mut header = HashMap::new();
        header.insert(BlockMetadataKey::InstantTime, instant_time.to_string());
        let batch = test_batch(records);
        let mut rb = RecordBatches::new();
        rb.push_data_batch(batch);
        LogBlock::new(
            LogFormatVersion::V1,
            BlockType::AvroData,
            header,
            LogBlockContent::Records(rb),
            HashMap::new(),
        )
    }

    fn make_test_buffer() -> Box<KeyBasedFileGroupRecordBuffer> {
        let mut ctx = ReaderContext::empty();
        ctx.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "ts".to_string(),
        );
        ctx.rebuild_record_context(String::new());
        // Prepare schema handler with DeleteContext so buffer construction succeeds.
        let mut handler = FileGroupReaderSchemaHandler::new()
            .with_table_schema(test_schema())
            .with_data_schema(test_schema());
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
        let ctx = std::sync::Arc::new(ctx);
        Box::new(
            KeyBasedFileGroupRecordBuffer::new(ctx, "COMMIT_TIME_ORDERING".to_string(), false)
                .unwrap(),
        )
    }

    /// Commit-time ordering: oldest instant processed first, newest last.
    ///
    /// Given: 3 instants (t1, t2, t3) each writing key "K" with different values
    ///        t1 writes K=(counter=1, ts=10)  ← oldest
    ///        t2 writes K=(counter=2, ts=20)
    ///        t3 writes K=(counter=3, ts=30)  ← newest
    /// When:  forward_scan_pass1 → reverse_scan_pass2 → process via pop_back
    /// Then:  pop_back order: t1 → t2 → t3 (oldest first)
    ///        After processing: records["K"] = (counter=3, ts=30) (newest wins)
    ///
    /// This validates the full chain from FS_logFileReadInputs_fileOrderRequestTime.md:
    ///   deltaMerge(t1_record, null)        → records["K"] = t1_record
    ///   deltaMerge(t2_record, t1_record)   → records["K"] = t2_record
    ///   deltaMerge(t3_record, t2_record)   → records["K"] = t3_record (newest wins)
    #[test]
    fn test_commit_time_ordering_newest_instant_wins() {
        let blocks = vec![
            make_data_block_with_content("t1", &[("K", 1, 10)]),
            make_data_block_with_content("t2", &[("K", 2, 20)]),
            make_data_block_with_content("t3", &[("K", 3, 30)]),
        ];

        let mut pass1 = forward_scan_pass1(blocks, "z", &None, "utc", None).unwrap();
        assert_eq!(pass1.ordered_instants_list, vec!["t1", "t2", "t3"]);

        let mut pass2 = reverse_scan_pass2(&mut pass1);

        // Verify deque ordering: pop_back gives t1, t2, t3 (oldest first)
        let mut buffer = make_test_buffer();
        let mut order = Vec::new();
        while let Some(mut block) = pass2.current_instant_log_blocks.pop_back() {
            let instant = block.instant_time().unwrap().to_string();
            order.push(instant.clone());
            // Process through the buffer (same as processQueuedBlocksForInstant)
            buffer.process_data_block(&mut block).unwrap();
        }

        // Verify processing order: oldest → newest
        assert_eq!(order, vec!["t1", "t2", "t3"]);

        // Verify running map final state: newest instant (t3) wins
        assert_eq!(buffer.size(), 1);
        let record = buffer.get_log_records().get("K").unwrap();
        // For COMMIT_TIME_ORDERING, the last writer (t3) always wins
        assert!(!record.is_delete());

        // Verify through merge_and_collect
        buffer.set_base_file_iterator(vec![]);
        let result = buffer.merge_and_collect().unwrap();
        let keys = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counters = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let timestamps = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(keys.value(0), "K");
        assert_eq!(counters.value(0), 3); // t3's value
        assert_eq!(timestamps.value(0), 30); // t3's timestamp
    }

    /// Multi-block instant: within-instant blocks processed in file-read order.
    ///
    /// Given: t1→[blockA(K=v1), blockB(K=v2)], t2→[blockC(K=v3)]
    ///        blockA written before blockB in the same log file
    /// When:  Full pass1 → pass2 → process via pop_back
    /// Then:  Within t1: blockA processed first, then blockB (file-read order)
    ///        Then t2: blockC processed last
    ///        Final: records["K"] = v3 (t2, newest instant)
    ///
    /// From the reference: "Collections.reverse + addLast + pollLast pattern is
    /// effectively a double-reversal that restores original file-read order within
    /// each instant."
    #[test]
    fn test_commit_time_within_instant_file_read_order() {
        let blocks = vec![
            make_data_block_with_content("t1", &[("K", 1, 10)]), // blockA
            make_data_block_with_content("t1", &[("K", 2, 20)]), // blockB (same instant)
            make_data_block_with_content("t2", &[("K", 3, 30)]), // blockC
        ];

        let mut pass1 = forward_scan_pass1(blocks, "z", &None, "utc", None).unwrap();
        assert_eq!(pass1.instant_to_blocks_map["t1"].len(), 2);

        let mut pass2 = reverse_scan_pass2(&mut pass1);

        let mut buffer = make_test_buffer();
        let mut order_instants = Vec::new();
        let mut order_values = Vec::new();
        while let Some(mut block) = pass2.current_instant_log_blocks.pop_back() {
            let instant = block.instant_time().unwrap().to_string();
            order_instants.push(instant.clone());
            // Peek at content to record ordering before processing
            if let LogBlockContent::Records(ref rb) = block.content {
                for batch in &rb.data_batches {
                    let counters = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    order_values.push(counters.value(0));
                }
            }
            buffer.process_data_block(&mut block).unwrap();
        }

        // Processing order: blockA(t1) → blockB(t1) → blockC(t2)
        assert_eq!(order_instants, vec!["t1", "t1", "t2"]);
        assert_eq!(order_values, vec![1, 2, 3]); // file-read order within t1

        // Final map state: t2's blockC wins (newest instant)
        buffer.set_base_file_iterator(vec![]);
        let result = buffer.merge_and_collect().unwrap();
        let counters = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(counters.value(0), 3); // t2's value wins
    }

    /// Delta merge chain: t1_record → t2_record → t3_record for same key.
    ///
    /// Given: COMMIT_TIME_ORDERING, 3 instants writing key "K"
    /// When:  Process oldest → newest
    /// Then:  deltaMerge called 3 times:
    ///        1. deltaMerge(t1_record, null)        → records["K"] = t1_record
    ///        2. deltaMerge(t2_record, t1_record)   → records["K"] = t2_record
    ///        3. deltaMerge(t3_record, t2_record)   → records["K"] = t3_record
    ///        total_log_records = 3
    #[test]
    fn test_delta_merge_chain_three_instants() {
        let mut buffer = make_test_buffer();

        // Simulate oldest→newest processing (as pollLast produces)
        let mut block_t1 = make_data_block_with_content("t1", &[("K", 1, 10)]);
        buffer.process_data_block(&mut block_t1).unwrap();
        assert_eq!(buffer.size(), 1);

        let mut block_t2 = make_data_block_with_content("t2", &[("K", 2, 20)]);
        buffer.process_data_block(&mut block_t2).unwrap();
        assert_eq!(buffer.size(), 1); // same key, overwritten

        let mut block_t3 = make_data_block_with_content("t3", &[("K", 3, 30)]);
        buffer.process_data_block(&mut block_t3).unwrap();
        assert_eq!(buffer.size(), 1);

        // All 3 records were processed
        assert_eq!(buffer.get_total_log_records(), 3);

        // Final value is from t3 (newest)
        buffer.set_base_file_iterator(vec![]);
        let result = buffer.merge_and_collect().unwrap();
        let counters = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(counters.value(0), 3);
    }

    /// Multiple keys across instants: each key gets newest-instant value.
    ///
    /// Given: t1 writes {A=1, B=1}, t2 writes {A=2, C=1}, t3 writes {B=3}
    /// When:  Process oldest → newest
    /// Then:  records = {A: t2_value(2), B: t3_value(3), C: t2_value(1)}
    #[test]
    fn test_commit_time_multiple_keys_across_instants() {
        let blocks = vec![
            make_data_block_with_content("t1", &[("A", 1, 10), ("B", 1, 10)]),
            make_data_block_with_content("t2", &[("A", 2, 20), ("C", 1, 20)]),
            make_data_block_with_content("t3", &[("B", 3, 30)]),
        ];

        let mut pass1 = forward_scan_pass1(blocks, "z", &None, "utc", None).unwrap();
        let mut pass2 = reverse_scan_pass2(&mut pass1);

        let mut buffer = make_test_buffer();
        while let Some(mut block) = pass2.current_instant_log_blocks.pop_back() {
            buffer.process_data_block(&mut block).unwrap();
        }

        // Verify map state
        assert_eq!(buffer.size(), 3); // A, B, C

        buffer.set_base_file_iterator(vec![]);
        let result = buffer.merge_and_collect().unwrap();
        assert_eq!(result.num_rows(), 3);

        let keys = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counters = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Sort by key for deterministic checking
        let mut kv: Vec<(String, i32)> = (0..result.num_rows())
            .map(|i| (keys.value(i).to_string(), counters.value(i)))
            .collect();
        kv.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(kv[0], ("A".to_string(), 2)); // t2 wins for A
        assert_eq!(kv[1], ("B".to_string(), 3)); // t3 wins for B
        assert_eq!(kv[2], ("C".to_string(), 1)); // only in t2
    }

    /// Full chain with base file merge: log overwrites base for matching keys.
    ///
    /// Given: Base=[K=base_val(0)], t1 writes K=1, t2 writes K=2
    /// When:  Process logs (oldest→newest), then merge with base
    /// Then:  Output: K=2 (newest log instant wins over both base and t1)
    #[test]
    fn test_commit_time_full_chain_with_base_merge() {
        let blocks = vec![
            make_data_block_with_content("t1", &[("K", 1, 10)]),
            make_data_block_with_content("t2", &[("K", 2, 20)]),
        ];

        let mut pass1 = forward_scan_pass1(blocks, "z", &None, "utc", None).unwrap();
        let mut pass2 = reverse_scan_pass2(&mut pass1);

        let mut buffer = make_test_buffer();
        while let Some(mut block) = pass2.current_instant_log_blocks.pop_back() {
            buffer.process_data_block(&mut block).unwrap();
        }

        // Set base file with original value for K
        let base = test_batch(&[("K", 0, 0)]);
        buffer.set_base_file_iterator(vec![base]);

        let result = buffer.merge_and_collect().unwrap();
        assert_eq!(result.num_rows(), 1);

        let counters = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        // Log record from t2 wins over both base and t1
        assert_eq!(counters.value(0), 2);
    }

    /// C-INFLIGHT-DELTA, value-level end-to-end: proves hudi-rs does NOT drop a COMMITTED
    /// straddling delta once it is actually fed the correct completed/inflight sets and the
    /// committed log block. This is the faithful standalone reproduction of the
    /// `TestMORSnapshotExcludesUncommitted#snapshotExcludesStraddlingUncommittedDelta` scenario,
    /// isolated to hudi-rs (no gluten, no sweep):
    ///
    ///   base file (t0, committed):        id=1, age=1000
    ///   log block t1 (INFLIGHT, straddling below the watermark): id=2, age=9999
    ///   log block t2 (COMMITTED, latest): id=1, age=1500
    ///   gate: completed={t0, t2}, inflight={t1}
    ///
    /// Correct snapshot semantics: {(1, 1500)} -- t2's committed delta overrides the base for
    /// id=1, and id=2 (from the inflight t1 delta) is excluded.
    ///
    /// This test PASSES on current hudi-rs code, which localizes the sweep failure: given correct
    /// inputs (the committed t2 log block actually reaching the reader), hudi-rs is correct, so the
    /// committed delta being dropped in the sweep is an INPUT problem -- the base-only file slice
    /// that reaches the reader -- NOT a Gate-3 over-exclusion. (For the specific v6 base-merge
    /// layout in that test, the hudi-internal FileSystemView assigns t2's committed log to the
    /// uncommitted t1 base-instant slice and drops the whole slice, so the log never arrives; both
    /// the JVM reader and hudi-rs then read base-only.)
    #[test]
    fn test_c_inflight_committed_straddling_delta_applied_value_level() {
        // File-read order: t1 (older) then t2 (newer).
        let blocks = vec![
            make_data_block_with_content("20250102", &[("2", 9999, 20)]), // t1 INFLIGHT (straddling)
            make_data_block_with_content("20250103", &[("1", 1500, 30)]), // t2 COMMITTED (latest)
        ];

        let gate_inputs = CompletionGateInputs {
            completed_instants: ["20250101".to_string(), "20250103".to_string()]
                .into_iter()
                .collect(),
            inflight_instants: ["20250102".to_string()].into_iter().collect(),
            archived_boundary: None,
        };
        let gate = CompletionGate::new(&gate_inputs);

        // high-watermark = latest completed = 20250103 (snapshot read, no incremental range).
        let mut pass1 = forward_scan_pass1(blocks, "20250103", &None, "utc", Some(&gate)).unwrap();
        // Gate 3 drops the inflight straddling t1, keeps the committed t2.
        assert_eq!(pass1.ordered_instants_list, vec!["20250103"]);
        assert!(!pass1.instant_to_blocks_map.contains_key("20250102"));

        let mut pass2 = reverse_scan_pass2(&mut pass1);
        let mut buffer = make_test_buffer();
        while let Some(mut block) = pass2.current_instant_log_blocks.pop_back() {
            buffer.process_data_block(&mut block).unwrap();
        }

        // Base file (t0): id=1 age=1000.
        let base = test_batch(&[("1", 1000, 0)]);
        buffer.set_base_file_iterator(vec![base]);
        let result = buffer.merge_and_collect().unwrap();

        let keys = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counters = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let kv: Vec<(String, i32)> = (0..result.num_rows())
            .map(|i| (keys.value(i).to_string(), counters.value(i)))
            .collect();

        // Only id=1 remains, with t2's committed value 1500 (NOT the base 1000, NOT dropped).
        assert_eq!(result.num_rows(), 1, "only the committed key must survive");
        assert_eq!(
            kv[0],
            ("1".to_string(), 1500),
            "committed straddling delta t2 must override the base (1000 -> 1500)"
        );
        assert!(
            !kv.iter().any(|(k, _)| k == "2"),
            "inflight straddling delta t1 (id=2) must be excluded"
        );
    }
}
