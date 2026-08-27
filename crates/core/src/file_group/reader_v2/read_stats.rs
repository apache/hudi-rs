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

//! Mirrors `org.apache.hudi.common.table.read.HoodieReadStats`.
//!
//! Mutable accumulator for read statistics, written during log scanning
//! and buffer processing, read by the caller after the read completes.

/// Mutable accumulator for file group read statistics.
#[derive(Debug, Clone, Default)]
pub struct HoodieReadStats {
    pub num_inserts: u64,
    pub num_updates: u64,
    pub num_deletes: u64,
    pub total_log_read_time_us: u64,
    pub total_log_records: u64,
    pub total_log_files_compacted: u64,
    pub total_log_blocks: u64,
    pub total_corrupt_log_blocks: u64,
    pub total_rollback_blocks: u64,

    // ── Stage timings (perf harness) ───────────────────────
    // Cheap monotonic `Instant`-based accumulators wired at the matching
    // code sites. Always-on, accumulated per block/batch (never per row).
    // Used by `benchmark/filegroup` (fg-bench) to attribute wall time across
    // the read pipeline. Zero behavioral effect — instrumentation only.
    //
    /// Wall us spent reading + projecting the base parquet file
    /// (`HoodieFileGroupReader::make_base_file_batches`).
    pub base_read_us: u64,
    /// Wall us spent reading log-block metadata + bytes off storage during the
    /// log scan Pass-1 (`BaseHoodieLogRecordReader::scan_internal`).
    pub log_block_read_us: u64,
    /// Wall us spent fetching admitted log blocks' content off storage
    /// (`fetch_window`, Pass 3's batched prefetch).
    ///
    /// Separate from `log_block_read_us`, which is Pass 1's headers-only walk.
    ///
    /// **Zero is the expected reading on a small log file**, and does not mean the
    /// content was free. When the walk's window already covered a block's content
    /// the walk keeps those bytes, so there is nothing left for Pass 3 to fetch and
    /// the transfer is charged to `log_block_read_us` instead. A before-and-after
    /// comparison across that change reads as the fetch becoming free when it has
    /// only moved.
    pub log_block_fetch_us: u64,
    /// Wall us spent decoding fetched log-block bytes into arrow batches
    /// (`LogBlock::decode_fetched`, called from `merge_blocks`).
    ///
    /// Timed where the decode happens. It used to be timed in the record buffer,
    /// around a span that had become empty when the fetch moved into Pass 3, so
    /// the counter read zero however long a decode took.
    pub log_block_decode_us: u64,
    /// Wall us spent upserting decoded records into the merge map
    /// (`process_data_block` / `process_delete_block`).
    ///
    /// One of the three parts of `merge_insert_us`, which wraps the whole of Pass
    /// 3 and therefore spans fetch, decode and upsert together. Subtracting
    /// `log_block_fetch_us` and `log_block_decode_us` from it leaves this plus the
    /// per-block dispatch around them; do not read the remainder as upsert.
    pub merge_upsert_us: u64,
    /// Wall us spent in the whole of Pass 3: fetching admitted blocks' content,
    /// decoding it, and upserting into the merge map.
    ///
    /// `log_block_fetch_us`, `log_block_decode_us` and `merge_upsert_us` are its
    /// parts. It is kept because their sum is not the total: the per-block dispatch
    /// around them is real time that belongs to no one of the three.
    pub merge_insert_us: u64,
    /// Wall us spent in the final base+log merge collect
    /// (`merge_and_collect_with_stats`).
    pub final_merge_us: u64,
    /// Wall us spent building/projecting the output batch
    /// (`apply_output_converter` + base-only concat path).
    pub output_build_us: u64,
    /// Peak number of entries held in the merge map during the log scan.
    pub merge_map_peak_entries: u64,

    // ── Spillable merge map ──────────────────────────────
    /// True if the size-tracked merge map spilled any entry to disk (RocksDB)
    /// during the scan — i.e. the in-memory budget was exceeded. A low
    /// `hoodie.memory.merge.max.size` should set it.
    pub merge_map_spilled: bool,
    /// Peak TRUE-retained in-memory bytes the merge map held during the scan:
    /// the sum of the distinct pinned source batches' `get_array_memory_size`
    /// plus owned/key/overhead bytes. Counting whole pinned batches rather than
    /// a per-row share keeps the stat honest for dense spread-key `BatchRef`
    /// maps, where a per-row share vastly under-counts the retained RSS.
    /// Bounded by `0.8 × hoodie.memory.merge.max.size − rocksdb reserved` via
    /// source-batch eviction, so a benchmark can confirm the in-memory
    /// footprint stayed within budget while the rest spilled.
    pub merge_map_peak_in_memory_bytes: u64,
}
