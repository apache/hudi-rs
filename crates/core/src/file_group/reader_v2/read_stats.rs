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
    pub total_log_read_time_ms: u64,
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
    /// Wall ms spent reading + projecting the base parquet file
    /// (`HoodieFileGroupReader::make_base_file_batches`).
    pub base_read_ms: u64,
    /// Wall ms spent reading log-block metadata + bytes off storage during the
    /// log scan Pass-1 (`BaseHoodieLogRecordReader::scan_internal`).
    pub log_block_read_ms: u64,
    /// Wall ms spent inflating/decoding log blocks into arrow batches
    /// (`LogBlock::inflate`, accumulated inside the record buffer).
    /// NOTE: in hudi-rs inflate/decode is triggered lazily inside the buffer's
    /// `process_data_block`, so this is measured there and is a SUBSET of the
    /// `merge_insert_ms` window below (decode is nested inside merge insert).
    pub log_block_decode_ms: u64,
    /// Wall ms spent dispatching decoded log records into the merge map
    /// (`process_queued_blocks_for_instant`: inflate/decode + per-key upsert).
    pub merge_insert_ms: u64,
    /// Wall ms spent in the final base+log merge collect
    /// (`merge_and_collect_with_stats`).
    pub final_merge_ms: u64,
    /// Wall ms spent building/projecting the output batch
    /// (`apply_output_converter` + base-only concat path).
    pub output_build_ms: u64,
    /// Peak number of entries held in the merge map during the log scan.
    pub merge_map_peak_entries: u64,

    // ── Spillable merge map (A1, ENG-42993) ──────────────────────────────
    /// True if the size-tracked merge map spilled any entry to disk (RocksDB)
    /// during the scan — i.e. the in-memory budget was exceeded. This is the M3
    /// acceptance signal: a low `hoodie.memory.merge.max.size` should set it.
    pub merge_map_spilled: bool,
    /// Peak TRUE-retained in-memory bytes the merge map held during the scan
    /// (A6e): the sum of the distinct pinned source batches' `get_array_memory_size`
    /// plus owned/key/overhead bytes — NOT the pre-A6 per-row share, which
    /// under-counted dense spread-key `BatchRef` maps (M5: stat read ~budget while
    /// RSS was 21.9 GB). Bounded by `0.8 × hoodie.memory.merge.max.size − rocksdb
    /// reserved` once A6e eviction is active, so a benchmark can confirm the
    /// in-memory footprint stayed within budget while the rest spilled.
    pub merge_map_peak_in_memory_bytes: u64,
}
