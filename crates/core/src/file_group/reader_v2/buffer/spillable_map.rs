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

//! Size-tracked, RocksDB-spillable merge map (A1, ENG-42993).
//!
//! Mirrors `org.apache.hudi.common.util.collection.ExternalSpillableMap`: an
//! unbounded merge map that keeps entries in memory until a byte budget is hit,
//! then spills NEW entries to disk (RocksDB). This replaces the previously
//! unbounded in-memory `HashMap<String, BufferedRecord>` in
//! [`FileGroupRecordBuffer`](super::record_buffer::FileGroupRecordBuffer) and is
//! the OOM-prevention fix that aligns hudi-rs with the Java reader (GAP-10).
//!
//! ## Tiers and iteration order
//!
//! - **In-memory tier:** a `HashMap` holding entries while
//!   `current_in_memory_size < max_in_memory_size`.
//! - **Disk tier:** a lazily-created RocksDB instance ([`RocksDbDiskMap`]) that
//!   receives NEW entries once the in-memory budget is exhausted. Existing
//!   in-memory entries stay in memory (Java parity).
//!
//! Iteration ([`SpillableRecordMap::drain_iter`]) yields all in-memory entries
//! first, then disk entries — matching Java's `ExternalSpillableMap` iterator.
//!
//! ## Size accounting (A6e — true retained bytes)
//!
//! The budget trigger uses the **true retained heap** of the in-memory tier, not
//! a per-row share. A `BatchRef` entry pins its WHOLE source `Arc<RecordBatch>`
//! alive until its LAST live ref is dropped, so charging it only
//! `array_bytes / num_rows` (the pre-A6 estimate) drastically under-counted dense
//! spread-key workloads (M5: 21.9 GB resident while the per-row estimate read
//! under budget). A6e fixes this:
//!
//! - **Pinned source batches** are tracked in [`PinnedBatches`]: a map keyed by
//!   `Arc::as_ptr` (one `Arc` per decoded source batch — invariant established in
//!   `key_based.rs::process_data_block`) → `{batch_bytes, live_refs}`. Inserting a
//!   `BatchRef` bumps its batch's `live_refs` (initializing `batch_bytes =
//!   get_array_memory_size` on the first ref); removing/spilling/compacting one
//!   decrements it, dropping the entry (and subtracting its bytes) at zero refs.
//!   `current_pinned_bytes` is the sum of the DISTINCT batches' `batch_bytes`.
//! - **Owned / key / overhead bytes** (`Owned` payloads, `Delete` = 0, plus
//!   `key.len() + `[`ENTRY_OVERHEAD_BYTES`] per entry) are summed in
//!   `current_owned_and_overhead_bytes`.
//! - **`true_retained_bytes` = `current_pinned_bytes` + `current_owned_and_overhead_bytes`**,
//!   and [`over_budget`](SpillableRecordMap::over_budget) compares THAT against
//!   the budget. The RocksDB engine's own memory ([`ROCKSDB_RESERVED_BYTES`]) is
//!   subtracted from the budget up front so the disk tier's footprint is counted
//!   inside `hoodie.memory.merge.max.size`.
//!
//! ## Eviction by source batch (A6e)
//!
//! When an insert pushes `true_retained_bytes` to/over the budget, the map evicts
//! whole source batches (largest-bytes first) until back under budget — spilling
//! individual entries would not free a batch unless ALL its refs leave memory. For
//! the chosen batch: if its live-row ratio is below [`COMPACTION_LIVE_RATIO`] it
//! is **compacted** (interleave the live rows into a small owned batch, repoint
//! those entries, drop the original `Arc` — no IO); otherwise it is **spilled**
//! (serialize each live entry to the RocksDB tier, remove the in-memory entries so
//! the `Arc` drops — frees the whole batch, costs IO). The zero-copy `BatchRef`
//! hot path is unchanged while under budget.
//!
//! ## Lifecycle
//!
//! The spill directory is created lazily on the first spill (Java parity) under
//! a per-read uuid subdirectory of `hoodie.memory.spillable.map.path`. The
//! RocksDB handle and the directory are closed/removed on drop (RAII via
//! [`tempfile::TempDir`]); drop is double-drop safe and panic-safe.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use rocksdb::{BlockBasedOptions, Cache, DB, DBCompressionType, Options, WriteBatch, WriteOptions};

use crate::Result;
use crate::error::CoreError;
use crate::file_group::reader_v2::buffered_record::{BufferedRecord, OrderingValue, RecordPayload};
use crate::file_group::reader_v2::row_serde;

// ── Merge-map hasher ─────────────────────────────────────────────────────────
// The merge map hashes a short String record key on every insert / get / remove
// (the base-merge probe), so this is a latency-bound hot-loop hasher. `foldhash`
// (folded 64×64→128 multiply) beats std SipHash-1-3 here — measured ~6% wall on
// plain-smoke and ~9% on churn-smoke (merge-heavy) — and, unlike `ahash`, needs no
// `target-feature=+aes` to do so. `fast` mode drops DoS protection, which is fine:
// the keys are our own trusted data within a single read, not adversary-controlled.
pub type MergeHasher = foldhash::fast::RandomState;
/// Merge map: record key → buffered record, with the configurable [`MergeHasher`].
pub type MergeMap = std::collections::HashMap<String, BufferedRecord, MergeHasher>;

// ── Budget / accounting constants ──────────────────────────────────────────

/// Fraction of `hoodie.memory.merge.max.size` retained in memory before NEW
/// entries spill to disk. Matches Java `ExternalSpillableMap`'s gold 0.8 factor
/// (it keeps a 20% headroom for the merge work that runs alongside the map).
pub const SPILL_TRIGGER_FRACTION: f64 = 0.8;

/// Default `hoodie.memory.merge.max.size` when unset: 1 GiB, matching the Java
/// reader's default merge memory budget.
pub const DEFAULT_MERGE_MAX_SIZE_BYTES: u64 = 1024 * 1024 * 1024;

/// Per-entry bookkeeping overhead added to the size estimate: the HashMap node,
/// the `BufferedRecord` struct, and the `String` key header. ~64 bytes is a
/// conservative estimate that keeps the trigger from under-counting small
/// entries (which would let the in-memory map grow past the intended budget).
pub const ENTRY_OVERHEAD_BYTES: u64 = 64;

/// RocksDB engine memory reserved out of the merge budget up front: the live
/// memtables ([`ROCKSDB_WRITE_BUFFER_SIZE`] × [`ROCKSDB_MAX_WRITE_BUFFERS`] =
/// 32 MiB) plus the block cache ([`ROCKSDB_BLOCK_CACHE_BYTES`] = 8 MiB), so the
/// disk tier's own memory is counted inside `hoodie.memory.merge.max.size`. The
/// spike (05-a2a1-design.md Part 2) measured RSS staying within this budget.
pub const ROCKSDB_RESERVED_BYTES: u64 = ROCKSDB_WRITE_BUFFER_SIZE as u64
    * ROCKSDB_MAX_WRITE_BUFFERS as u64
    + ROCKSDB_BLOCK_CACHE_BYTES as u64;

/// Live-row ratio below which an over-budget pinned source batch is COMPACTED
/// (re-batched in place) rather than SPILLED (serialized to disk). A `BatchRef`
/// entry pins its WHOLE source batch alive; when the budget is exceeded the map
/// evicts whole source batches (A6e). For a batch whose surviving rows are a
/// small fraction of its total rows, compaction (interleave the live rows into a
/// small owned batch, drop the original `Arc`) reclaims most of the bytes with
/// **zero IO**; for a dense batch (≥ this ratio live) compaction would free
/// little, so we spill its live entries to RocksDB instead (frees the whole
/// batch, costs IO).
///
/// Set to 0.5 per the spike (05-a2a1-design.md Part 1): in realistic churn most
/// batches stay > 50% live, so the dense (spill) branch is taken there; the
/// pathological sparse-survivor case (191 MiB held for 1.5% live data) takes the
/// cheap compaction branch.
pub const COMPACTION_LIVE_RATIO: f64 = 0.5;

/// When the budget is exceeded, evict-by-source-batch frees down to this fraction
/// of the budget (a LOW-WATER mark), not merely back to the budget. Evicting a
/// margin amortizes the single O(in-memory) grouping scan across the many
/// subsequent inserts that then fit without triggering another scan — turning
/// what would be a per-insert O(N) cost near the budget boundary into an
/// occasional batched pass. 0.8 leaves a 20% margin; the peak RSS still tracks
/// the budget (it is the *trigger* that bounds the high-water mark).
pub const EVICTION_LOW_WATER_FRACTION: f64 = 0.8;

// ── RocksDB tuning constants (documented for memory accounting) ─────────────

/// RocksDB write-buffer (memtable) size: 16 MiB. With
/// [`ROCKSDB_MAX_WRITE_BUFFERS`] this caps live memtable memory at 32 MiB.
pub const ROCKSDB_WRITE_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// Maximum number of live RocksDB write buffers (memtables): 2. Bounds memtable
/// memory at [`ROCKSDB_WRITE_BUFFER_SIZE`] × 2 = 32 MiB.
pub const ROCKSDB_MAX_WRITE_BUFFERS: i32 = 2;

/// RocksDB LRU block-cache size: 8 MiB. Small and shared; index/filter blocks
/// are also counted against it (`cache_index_and_filter_blocks`) so the total
/// engine memory stays bounded.
pub const ROCKSDB_BLOCK_CACHE_BYTES: usize = 8 * 1024 * 1024;

// ── Config keys / defaults ──────────────────────────────────────────────────

/// Config key: total merge memory budget (bytes). `0.8×` of it is the spill
/// threshold. Default [`DEFAULT_MERGE_MAX_SIZE_BYTES`] (1 GiB).
pub const CONFIG_MERGE_MAX_SIZE: &str = "hoodie.memory.merge.max.size";
/// Config key: HARD peak cap (bytes) on the merge map's tracked in-memory
/// footprint. Distinct from [`CONFIG_MERGE_MAX_SIZE`], which is the *soft* spill
/// trigger (exceeding it spills NEW entries to disk). This is an *absolute*
/// ceiling: when the tracked in-memory footprint would exceed it AND spilling /
/// eviction cannot bring it back down (e.g. a single oversized record or pinned
/// source batch), the insert fails loudly with
/// [`CoreError::MemoryLimitExceeded`] instead of continuing to allocate and
/// risking a silent executor OOM. Unset (the default) → no cap, preserving the
/// pre-existing spill-only behavior. This is the hudi-rs foundation the velox
/// memory-reservation work (ENG-44436/44437) builds on.
///
/// OPERATIONAL NOTE: because the cap is opt-in, the loud-OOM protection is
/// inert unless the embedding engine sets this key — deployments relying on it
/// (e.g. the Quanton MOR rollout) must set it explicitly alongside
/// `hoodie.memory.merge.max.size`; the soft budget alone only controls
/// spilling, not the hard in-memory ceiling.
pub const CONFIG_MAX_PEAK_MEMORY: &str = "hoodie.memory.merge.max.peak.size";
/// Config key: parent directory for spill files. Default [`DEFAULT_SPILL_PATH`].
pub const CONFIG_SPILLABLE_MAP_PATH: &str = "hoodie.memory.spillable.map.path";
/// Config key: spill backend selector. `ROCKS_DB` and `BITCASK` (Java's default)
/// both resolve to the RocksDB backend; any other value is rejected. See
/// [`DiskMapType`] for why BITCASK is aliased rather than rejected.
pub const CONFIG_DISKMAP_TYPE: &str = "hoodie.common.spillable.diskmap.type";

/// Fallback parent directory for spill files when [`CONFIG_SPILLABLE_MAP_PATH`]
/// is unset. `/tmp` mirrors Java's `java.io.tmpdir` fallback, but production
/// Hudi deployments point the spillable map at a real scratch volume — `/tmp` is
/// frequently `tmpfs` (RAM-backed), where the "disk" spill tier provides no
/// memory relief. Spilling to this default is logged at WARN on first spill (see
/// `SpillableRecordMap::ensure_disk`).
pub const DEFAULT_SPILL_PATH: &str = "/tmp";

/// The on-disk spill backend selected by `hoodie.common.spillable.diskmap.type`.
///
/// RocksDB is this reader's only spill backend. `BITCASK` — Java Hudi's *default*
/// value for this key — is accepted as an alias for [`RocksDb`](DiskMapType::RocksDb)
/// rather than rejected: the diskmap type selects only the on-disk key/value store
/// used to hold spilled merge entries, which is an internal mechanism with no bearing
/// on the merged read output (the same rows come back either way). Rejecting BITCASK
/// would loudly fail — or, if gated at the offload layer, disable native offload for —
/// essentially every read, since it is the persisted default. Only a genuinely unknown
/// value (neither BITCASK nor ROCKS_DB) is rejected, matching Java's `valueOf` throw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskMapType {
    /// RocksDB-backed spill (the only implemented backend; BITCASK aliases here).
    RocksDb,
}

impl DiskMapType {
    /// Parse the `hoodie.common.spillable.diskmap.type` value.
    ///
    /// Unset, `ROCKS_DB`, or `BITCASK` (all case-insensitive) →
    /// [`DiskMapType::RocksDb`]; BITCASK is aliased because the backend choice does
    /// not affect read output (see the type-level docs). Any other value →
    /// [`CoreError::Unsupported`] naming it, matching Java's `valueOf` behavior.
    pub fn parse(raw: Option<&str>) -> Result<Self> {
        match raw {
            None => Ok(DiskMapType::RocksDb),
            Some(v) if v.eq_ignore_ascii_case("ROCKS_DB") => Ok(DiskMapType::RocksDb),
            Some(v) if v.eq_ignore_ascii_case("BITCASK") => {
                // Java's default; alias to RocksDB (output-equivalent). debug! not
                // warn!: BITCASK is the persisted default, so this fires on nearly
                // every read — a per-read WARN would be pure log noise.
                log::debug!(
                    "spillable diskmap type '{v}' is not natively implemented; \
                     using the RocksDB spill backend (output-equivalent)"
                );
                Ok(DiskMapType::RocksDb)
            }
            Some(v) => Err(CoreError::Unsupported(format!(
                "unknown spillable diskmap type '{v}'; only ROCKS_DB and BITCASK are recognized"
            ))),
        }
    }
}

/// Parsed spill configuration for a single read.
///
/// Built from the reader's config map via [`SpillConfig::from_config`]. The
/// fields document their defaults and where they come from
/// (05-a2a1-design.md Part 3 config table).
#[derive(Debug, Clone)]
pub struct SpillConfig {
    /// `0.8 × hoodie.memory.merge.max.size − `[`ROCKSDB_RESERVED_BYTES`], the
    /// in-memory byte budget before NEW entries spill to disk.
    pub max_in_memory_size: u64,
    /// Parent directory for spill files (`hoodie.memory.spillable.map.path`).
    /// A per-read uuid subdirectory is created under it on the first spill.
    pub spill_path: PathBuf,
    /// The spill backend (always [`DiskMapType::RocksDb`] once parsed).
    pub diskmap_type: DiskMapType,
    /// HARD ceiling (bytes) on the tracked in-memory footprint
    /// ([`CONFIG_MAX_PEAK_MEMORY`]). `None` (default) → no cap. When `Some`, an
    /// insert whose footprint exceeds this and cannot be reduced by spilling
    /// fails with [`CoreError::MemoryLimitExceeded`] rather than growing further.
    pub max_peak_in_memory_size: Option<u64>,
}

impl SpillConfig {
    /// Parse the spill configuration from the reader config map.
    ///
    /// - `hoodie.memory.merge.max.size` → bytes (default
    ///   [`DEFAULT_MERGE_MAX_SIZE_BYTES`]); invalid values are rejected with a
    ///   typed [`CoreError::InvalidValue`].
    /// - `hoodie.memory.spillable.map.path` → parent dir (default
    ///   [`DEFAULT_SPILL_PATH`]).
    /// - `hoodie.common.spillable.diskmap.type` → backend
    ///   ([`DiskMapType::parse`]; BITCASK and ROCKS_DB both → RocksDB).
    /// - `hoodie.memory.merge.max.peak.size` → HARD in-memory cap (default
    ///   unset → `None`, no cap); invalid values are rejected with a typed
    ///   [`CoreError::InvalidValue`].
    pub fn from_config(config: &HashMap<String, String>) -> Result<Self> {
        let merge_max_size = match config.get(CONFIG_MERGE_MAX_SIZE) {
            None => DEFAULT_MERGE_MAX_SIZE_BYTES,
            Some(v) => v.trim().parse::<u64>().map_err(|_| {
                CoreError::InvalidValue(format!(
                    "{CONFIG_MERGE_MAX_SIZE} must be a positive integer byte count, got '{v}'"
                ))
            })?,
        };
        let max_peak_in_memory_size = match config.get(CONFIG_MAX_PEAK_MEMORY) {
            None => None,
            Some(v) => Some(v.trim().parse::<u64>().map_err(|_| {
                CoreError::InvalidValue(format!(
                    "{CONFIG_MAX_PEAK_MEMORY} must be a positive integer byte count, got '{v}'"
                ))
            })?),
        };
        let mut spill_config = Self::from_parts(
            merge_max_size,
            config
                .get(CONFIG_SPILLABLE_MAP_PATH)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(DEFAULT_SPILL_PATH)),
            DiskMapType::parse(config.get(CONFIG_DISKMAP_TYPE).map(String::as_str))?,
        )?;
        spill_config.max_peak_in_memory_size = max_peak_in_memory_size;
        Ok(spill_config)
    }

    /// Build a [`SpillConfig`] from explicit parts (used by tests and by
    /// [`from_config`](Self::from_config)). Computes `max_in_memory_size` as
    /// `0.8 × merge_max_size − `[`ROCKSDB_RESERVED_BYTES`], saturating at 0.
    /// Leaves `max_peak_in_memory_size` unset (no hard cap); `from_config`
    /// populates it from [`CONFIG_MAX_PEAK_MEMORY`] when present.
    pub fn from_parts(
        merge_max_size: u64,
        spill_path: PathBuf,
        diskmap_type: DiskMapType,
    ) -> Result<Self> {
        let budget = (merge_max_size as f64 * SPILL_TRIGGER_FRACTION) as u64;
        let max_in_memory_size = budget.saturating_sub(ROCKSDB_RESERVED_BYTES);
        Ok(Self {
            max_in_memory_size,
            spill_path,
            diskmap_type,
            max_peak_in_memory_size: None,
        })
    }
}

/// Per-source-batch pin accounting (A6e).
///
/// One entry per DISTINCT `Arc<RecordBatch>` that the in-memory tier's
/// `BatchRef` entries reference, keyed by `Arc::as_ptr` cast to `usize`. The
/// `Arc::as_ptr`-as-identity is valid because each decoded source batch is
/// interned to exactly one `Arc` (`key_based.rs::process_data_block` /
/// `next_base_row`), so the pointer uniquely identifies a source batch for its
/// lifetime — and a `usize` key avoids holding a second `Arc` clone (which would
/// itself keep the batch alive and defeat the accounting).
#[derive(Debug)]
struct PinnedBatch {
    /// `get_array_memory_size()` of the source batch (its true retained heap).
    batch_bytes: u64,
    /// Number of live in-memory `BatchRef` entries referencing this batch
    /// (O(1) to maintain — the eviction set is materialized lazily by a single
    /// grouping scan only when the budget is actually exceeded).
    live_refs: u64,
}

/// Tracks the distinct source batches pinned by the in-memory tier and their
/// summed true retained bytes (A6e). See [`PinnedBatch`].
///
/// Accounting (`add_ref`/`remove_ref`) is O(1) per in-memory insert/remove. The
/// eviction policy ([`SpillableRecordMap::evict_source_batches_until`]) pays a
/// single O(in-memory) grouping scan ONLY when over budget, and evicts enough
/// batches in that one pass to get well under budget — so the scan cost is
/// amortized across many inserts rather than paid per eviction.
#[derive(Debug, Default)]
struct PinnedBatches {
    /// `Arc::as_ptr as usize` → pin record.
    by_ptr: HashMap<usize, PinnedBatch>,
    /// Running sum of the DISTINCT batches' `batch_bytes` — the true retained
    /// heap of all pinned source batches.
    total_bytes: u64,
}

impl PinnedBatches {
    /// Register one more live ref to `batch`. Initializes the pin record (and
    /// adds `batch_bytes` to the total) on the FIRST ref to a batch.
    fn add_ref(&mut self, batch: &Arc<RecordBatch>) {
        let ptr = Arc::as_ptr(batch) as usize;
        match self.by_ptr.get_mut(&ptr) {
            Some(pin) => pin.live_refs += 1,
            None => {
                let batch_bytes = batch.get_array_memory_size() as u64;
                self.total_bytes = self.total_bytes.saturating_add(batch_bytes);
                self.by_ptr.insert(
                    ptr,
                    PinnedBatch {
                        batch_bytes,
                        live_refs: 1,
                    },
                );
            }
        }
    }

    /// Drop one live ref to the batch at `ptr`. When the last ref leaves,
    /// subtract its bytes and remove the pin record.
    fn remove_ref(&mut self, ptr: usize) {
        if let Some(pin) = self.by_ptr.get_mut(&ptr) {
            pin.live_refs -= 1;
            if pin.live_refs == 0 {
                self.total_bytes = self.total_bytes.saturating_sub(pin.batch_bytes);
                self.by_ptr.remove(&ptr);
            }
        }
    }

    /// Convenience: drop one live ref to `batch` (by its pointer).
    fn remove_ref_to(&mut self, batch: &Arc<RecordBatch>) {
        self.remove_ref(Arc::as_ptr(batch) as usize);
    }

    /// Sum of the distinct pinned batches' bytes (the true retained heap of
    /// pinned source batches).
    fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

/// Size-tracked, RocksDB-spillable map of record key → [`BufferedRecord`].
///
/// See the module docs for tiers, iteration order, size accounting, and
/// lifecycle. Replaces the unbounded in-memory `HashMap` in the merge buffer.
///
/// ## Contract
///
/// - [`insert`](Self::insert): routes to the in-memory tier while within budget,
///   otherwise to disk; updates the size estimate; lazily creates the spill dir
///   + RocksDB on the first spill.
/// - [`get`](Self::get) / [`remove`](Self::remove) / [`contains_key`](Self::contains_key):
///   check the in-memory tier first, then the disk tier.
/// - [`drain_iter`](Self::drain_iter): yields in-memory entries first, then disk.
/// - Side effect: a spill creates a temp directory; it is removed on drop.
pub struct SpillableRecordMap {
    /// In-memory tier (entries below the budget). Public so the merge buffer's
    /// schema-fallback / log-only path can iterate in-memory records cheaply.
    in_memory: MergeMap,
    /// Disk tier — lazily created on first spill.
    disk: Option<RocksDbDiskMap>,
    /// Distinct source batches pinned by the in-memory tier and their summed
    /// true retained bytes (A6e). The TRUE-retained budget trigger uses this.
    pinned: PinnedBatches,
    /// Sum of `Owned`-payload bytes + per-entry `key.len() + `
    /// [`ENTRY_OVERHEAD_BYTES`] over all in-memory entries. The non-pinned-batch
    /// portion of the in-memory tier's true retained heap.
    current_owned_and_overhead_bytes: u64,
    /// In-memory byte budget (`0.8 × merge.max.size − rocksdb reserved`). The
    /// SOFT spill trigger: exceeding it routes/evicts NEW entries to disk.
    max_in_memory_size: u64,
    /// HARD ceiling on the tracked in-memory footprint ([`CONFIG_MAX_PEAK_MEMORY`]).
    /// `None` → no cap (default). When `Some`, an insert whose footprint exceeds
    /// this and cannot be reduced by spilling fails loudly with
    /// [`CoreError::MemoryLimitExceeded`] (see [`Self::enforce_peak_cap`]).
    max_peak_in_memory_size: Option<u64>,
    /// Parent dir for the lazily-created per-read spill subdirectory.
    spill_path: PathBuf,
    /// Spill backend (always RocksDB once constructed).
    diskmap_type: DiskMapType,
    /// True once any entry has spilled to disk (M3 acceptance signal). Surfaced
    /// to `HoodieReadStats.merge_map_spilled`.
    spill_fired: bool,
    /// Peak TRUE-retained in-memory bytes observed (diagnostic / stats). Surfaced
    /// to `HoodieReadStats.merge_map_peak_in_memory_bytes` so the stat reflects
    /// the real resident set, not the pre-A6 per-row under-count.
    peak_in_memory_size: u64,
}

impl std::fmt::Debug for SpillableRecordMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpillableRecordMap")
            .field("in_memory_len", &self.in_memory.len())
            .field("disk_len", &self.disk.as_ref().map(|d| d.len()))
            .field("true_retained_bytes", &self.true_retained_bytes())
            .field("current_pinned_bytes", &self.pinned.total_bytes())
            .field("max_in_memory_size", &self.max_in_memory_size)
            .field("spill_fired", &self.spill_fired)
            .finish()
    }
}

impl SpillableRecordMap {
    /// Create an empty map with the default (1 GiB) merge budget and `/tmp`
    /// spill path. Used by call sites that have no reader config (e.g. tests
    /// that drive the buffer directly).
    pub fn new() -> Self {
        let config = SpillConfig::from_parts(
            DEFAULT_MERGE_MAX_SIZE_BYTES,
            PathBuf::from(DEFAULT_SPILL_PATH),
            DiskMapType::RocksDb,
        )
        .expect("default spill config is always valid");
        Self::with_config(config)
    }

    /// Create an empty map with an explicit [`SpillConfig`].
    pub fn with_config(config: SpillConfig) -> Self {
        Self {
            in_memory: HashMap::default(),
            disk: None,
            pinned: PinnedBatches::default(),
            current_owned_and_overhead_bytes: 0,
            max_in_memory_size: config.max_in_memory_size,
            max_peak_in_memory_size: config.max_peak_in_memory_size,
            spill_path: config.spill_path,
            diskmap_type: config.diskmap_type,
            spill_fired: false,
            peak_in_memory_size: 0,
        }
    }

    /// Per-entry `key.len() + `[`ENTRY_OVERHEAD_BYTES`] (the bookkeeping that is
    /// charged for EVERY in-memory entry, independent of its payload kind).
    fn entry_key_overhead_bytes(key: &str) -> u64 {
        key.len() as u64 + ENTRY_OVERHEAD_BYTES
    }

    /// Owned-payload bytes charged for an in-memory entry (NOT counting the
    /// pinned source batch of a `BatchRef`, which is accounted separately in
    /// [`PinnedBatches`]). `Owned` charges its own array bytes; `BatchRef` and
    /// `Delete` charge nothing here.
    fn owned_payload_bytes(record: &BufferedRecord) -> u64 {
        match &record.payload {
            RecordPayload::Owned(batch) => batch.get_array_memory_size() as u64,
            RecordPayload::BatchRef { .. } | RecordPayload::Delete => 0,
        }
    }

    /// The TRUE retained heap of the in-memory tier (A6e): the sum of the
    /// distinct pinned source batches' bytes plus the owned/key/overhead bytes.
    /// This — not a per-row share — drives [`over_budget`](Self::over_budget) and
    /// the peak stat, so the bound holds for dense spread-key `BatchRef` maps.
    pub fn true_retained_bytes(&self) -> u64 {
        self.pinned
            .total_bytes()
            .saturating_add(self.current_owned_and_overhead_bytes)
    }

    /// The merge map's CURRENT tracked in-memory footprint, in bytes.
    ///
    /// This is the live resident heap the in-memory tier is holding right now
    /// (an alias of [`true_retained_bytes`](Self::true_retained_bytes), named for
    /// external callers): the distinct pinned source batches plus owned-payload,
    /// key, and per-entry-overhead bytes. It is the quantity a host memory
    /// manager (e.g. velox's `MemoryPool`) needs in order to RESERVE against the
    /// hudi-rs reader's footprint (ENG-44436), and the value the hard peak cap
    /// ([`CONFIG_MAX_PEAK_MEMORY`]) is enforced against.
    ///
    /// # What is and isn't included
    ///
    /// - **Included:** the in-memory tier's pinned Arrow source batches and its
    ///   owned/key/overhead bytes.
    /// - **NOT included:** the RocksDB spill tier's engine memory (memtables +
    ///   block cache) — that is a fixed [`ROCKSDB_RESERVED_BYTES`] already
    ///   subtracted from the budget up front, not tracked per-op here; the
    ///   drain-time decoded-batch LRU ([`SPILL_BATCH_CACHE`]); and the Arrow
    ///   output batch produced once the map is drained (by then the map has been
    ///   consumed, so this returns 0). Callers wanting a whole-reader figure must
    ///   add those bounded, separately-documented overheads.
    #[must_use]
    pub fn current_in_memory_bytes(&self) -> u64 {
        self.true_retained_bytes()
    }

    /// True once the in-memory budget is exhausted, measured on the TRUE retained
    /// heap (A6e). New entries that push over this must evict whole source
    /// batches (compact/spill) to get back under, or spill themselves.
    fn over_budget(&self) -> bool {
        self.true_retained_bytes() >= self.max_in_memory_size
    }

    /// Account a newly-added in-memory entry into the size trackers (pinned-batch
    /// ref + owned/overhead bytes). Call AFTER inserting into `in_memory`.
    fn account_added(&mut self, key: &str, record: &BufferedRecord) {
        if let RecordPayload::BatchRef { batch, .. } = &record.payload {
            self.pinned.add_ref(batch);
        }
        self.current_owned_and_overhead_bytes = self
            .current_owned_and_overhead_bytes
            .saturating_add(Self::entry_key_overhead_bytes(key))
            .saturating_add(Self::owned_payload_bytes(record));
    }

    /// Un-account an in-memory entry that is being removed/spilled/compacted-away
    /// (drops its pinned-batch ref + owned/overhead bytes). Call with the entry
    /// that WAS in `in_memory`.
    fn account_removed(&mut self, key: &str, record: &BufferedRecord) {
        if let RecordPayload::BatchRef { batch, .. } = &record.payload {
            self.pinned.remove_ref_to(batch);
        }
        self.current_owned_and_overhead_bytes = self
            .current_owned_and_overhead_bytes
            .saturating_sub(Self::entry_key_overhead_bytes(key))
            .saturating_sub(Self::owned_payload_bytes(record));
    }

    /// Insert (or overwrite) a record.
    ///
    /// If the key already lives in a tier, the value is updated in that tier
    /// (in-memory updates re-account the size delta; disk updates overwrite).
    ///
    /// For a NEW key the routing depends on what is over budget:
    /// - **`BatchRef` payload** (the A2 hot path): add it to the in-memory tier
    ///   and account it, then evict whole source batches (compact sparse / spill
    ///   dense — A6e) until back under budget. The incoming entry's own batch is a
    ///   valid eviction victim, so this terminates even if it is the sole offender.
    /// - **`Owned` / `Delete` payload** (no shared source batch to evict): preserve
    ///   the A1 semantics — while over budget the NEW entry goes straight to the
    ///   disk tier (existing in-memory entries stay), since there is no batch to
    ///   free by eviction.
    ///
    /// Updates the size trackers and the spill-fired flag.
    pub fn insert(&mut self, key: String, value: BufferedRecord) -> Result<()> {
        // Update in place if the key already lives in the in-memory tier.
        if let Some(existing) = self.in_memory.get(&key) {
            let existing = existing.clone();
            self.account_removed(&key, &existing);
            self.account_added(&key, &value);
            self.in_memory.insert(key, value);
            self.track_peak();
            self.enforce_memory_limits()?;
            return Ok(());
        }
        if let Some(disk) = self.disk.as_mut()
            && disk.contains_key(&key)?
        {
            disk.put(&key, &value)?;
            return Ok(());
        }

        // New key. A `BatchRef` participates in evict-by-source-batch (A6e); an
        // `Owned`/`Delete` entry has no shared batch to free, so when over budget
        // it spills directly (A1 parity: existing entries stay, new puts go to
        // disk).
        let is_batch_ref = matches!(value.payload, RecordPayload::BatchRef { .. });
        if !is_batch_ref && self.over_budget() {
            let disk = self.ensure_disk()?;
            disk.put(&key, &value)?;
            return Ok(());
        }

        self.account_added(&key, &value);
        self.in_memory.insert(key, value);
        self.track_peak();
        self.enforce_memory_limits()?;
        Ok(())
    }

    /// Get a clone of the record for `key`, checking memory then disk.
    pub fn get(&self, key: &str) -> Result<Option<BufferedRecord>> {
        if let Some(r) = self.in_memory.get(key) {
            return Ok(Some(r.clone()));
        }
        match self.disk.as_ref() {
            Some(disk) => disk.get(key),
            None => Ok(None),
        }
    }

    /// Single-probe merge for the in-memory tier. Probes `key` ONCE via `get_mut`,
    /// hands the existing record to `f`, and overwrites the slot in place — replacing
    /// the merge hot path's `get`(probe+clone) + `insert`(probe+clone + probe) with one
    /// probe and no clones. Returns whether an existing record was present.
    ///
    /// Vacant keys and the disk tier fall back to the correct `get`/`insert` path
    /// (which preserves spill routing + the budget/eviction logic for those cases);
    /// the fast path is the common merge-into-existing-in-memory-key case (churn).
    pub fn merge_in_place<F>(&mut self, key: &str, f: F) -> Result<bool>
    where
        F: FnOnce(Option<&BufferedRecord>) -> Result<Option<BufferedRecord>>,
    {
        if self.disk.is_none() {
            if let Some(slot) = self.in_memory.get_mut(key) {
                if let Some(new) = f(Some(&*slot))? {
                    // Inline of account_removed(old) + account_added(new): touches the
                    // disjoint `pinned` / byte-counter fields directly so it can run
                    // while `slot` borrows `in_memory`.
                    if let RecordPayload::BatchRef { batch, .. } = &slot.payload {
                        self.pinned.remove_ref_to(batch);
                    }
                    let removed = Self::entry_key_overhead_bytes(key)
                        .saturating_add(Self::owned_payload_bytes(&*slot));
                    if let RecordPayload::BatchRef { batch, .. } = &new.payload {
                        self.pinned.add_ref(batch);
                    }
                    let added = Self::entry_key_overhead_bytes(key)
                        .saturating_add(Self::owned_payload_bytes(&new));
                    self.current_owned_and_overhead_bytes = self
                        .current_owned_and_overhead_bytes
                        .saturating_sub(removed)
                        .saturating_add(added);
                    *slot = new;
                }
                self.track_peak();
                self.enforce_memory_limits()?;
                return Ok(true);
            }
            // Vacant: no existing record. The get_mut borrow has ended.
            if let Some(new) = f(None)? {
                self.insert(key.to_string(), new)?;
            }
            return Ok(false);
        }
        // Disk tier present: correctness-first fallback (clone-get + routed insert).
        let existing = self.get(key)?;
        let had = existing.is_some();
        if let Some(new) = f(existing.as_ref())? {
            self.insert(key.to_string(), new)?;
        }
        Ok(had)
    }

    /// Remove and return the record for `key`, checking memory then disk.
    /// Un-accounts the in-memory size trackers (pinned-batch ref + owned/overhead)
    /// when an in-memory entry is removed.
    pub fn remove(&mut self, key: &str) -> Result<Option<BufferedRecord>> {
        if let Some(record) = self.in_memory.remove(key) {
            self.account_removed(key, &record);
            return Ok(Some(record));
        }
        match self.disk.as_mut() {
            Some(disk) => disk.remove(key),
            None => Ok(None),
        }
    }

    /// True if `key` is present in either tier.
    pub fn contains_key(&self, key: &str) -> Result<bool> {
        if self.in_memory.contains_key(key) {
            return Ok(true);
        }
        match self.disk.as_ref() {
            Some(disk) => disk.contains_key(key),
            None => Ok(false),
        }
    }

    /// Total number of entries across both tiers.
    pub fn len(&self) -> usize {
        self.in_memory.len() + self.disk.as_ref().map(|d| d.len()).unwrap_or(0)
    }

    /// True if both tiers are empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Immutable view of the in-memory tier (used by the buffer's schema-fallback
    /// / log-only path, which only needs *some* in-memory record's schema). Does
    /// NOT include spilled entries.
    pub fn in_memory(&self) -> &MergeMap {
        &self.in_memory
    }

    /// Mutable view of the in-memory tier, for the A2 compaction safety valve
    /// (`compact_pinned_batches`), which re-batches sparsely-pinned source
    /// batches in place. Spilled (disk-tier) entries are already `Owned` and pin
    /// nothing shared, so compaction only ever touches the in-memory tier.
    ///
    /// Note: compaction runs end-of-scan (after all inserts, before drain), so
    /// the resulting payload-size shift does not affect any spill decision — the
    /// size estimate no longer drives routing at that point.
    pub fn in_memory_mut(&mut self) -> &mut MergeMap {
        &mut self.in_memory
    }

    /// True once any entry has been spilled to disk during this read (the M3
    /// acceptance signal).
    pub fn spill_fired(&self) -> bool {
        self.spill_fired
    }

    /// Peak TRUE-retained in-memory bytes observed during the read (A6e). The
    /// stat that `merge_map_peak_in_memory_bytes` surfaces — it now reflects the
    /// real resident set, not the pre-A6 per-row under-count.
    pub fn peak_in_memory_size(&self) -> u64 {
        self.peak_in_memory_size
    }

    /// Current count of distinct pinned source batches (diagnostic).
    pub fn pinned_batch_count(&self) -> usize {
        self.pinned.by_ptr.len()
    }

    /// Drain the map, yielding in-memory entries first then disk entries (Java
    /// iteration order). Consumes the map; the disk tier (and its temp dir) is
    /// dropped when the returned iterator is dropped.
    pub fn drain_iter(self) -> SpillDrainIter {
        let in_memory: Vec<BufferedRecord> = self.in_memory.into_values().collect();
        SpillDrainIter {
            in_memory: in_memory.into_iter(),
            disk_iter: self.disk.map(|d| d.into_drain_iter()),
        }
    }

    /// Track the peak TRUE-retained in-memory bytes (A6e).
    fn track_peak(&mut self) {
        let cur = self.true_retained_bytes();
        if cur > self.peak_in_memory_size {
            self.peak_in_memory_size = cur;
        }
    }

    /// Lazily create the disk tier (and its per-read spill directory) on first
    /// spill, then return it. Sets the spill-fired flag.
    fn ensure_disk(&mut self) -> Result<&mut RocksDbDiskMap> {
        if self.disk.is_none() {
            debug_assert_eq!(self.diskmap_type, DiskMapType::RocksDb);
            let disk = RocksDbDiskMap::create(&self.spill_path)?;
            self.disk = Some(disk);
            self.spill_fired = true;
            log::debug!(
                "[SpillableRecordMap] spill engaged: in-memory budget {} bytes exhausted, \
                 routing new entries to RocksDB under {:?}",
                self.max_in_memory_size,
                self.spill_path,
            );
            // The UNCONFIGURED default (`hoodie.memory.spillable.map.path` unset
            // → exactly `/tmp`) is often tmpfs (RAM-backed) on systemd-default
            // distros, containers, and k8s pods. There the "disk" tier is
            // actually RAM, so spilling provides no memory relief and can OOM
            // faster. Warn on first spill so this is visible in prod logs (debug!
            // above is typically filtered out). Only the bare default warns — an
            // operator who explicitly points the config at a real volume under
            // /tmp (e.g. /tmp/nvme-scratch) has made a deliberate choice and is
            // not nagged.
            if self.spill_path.as_path() == Path::new(DEFAULT_SPILL_PATH) {
                log::warn!(
                    "[SpillableRecordMap] spilling under {:?}; if this is tmpfs (RAM-backed) \
                     the RocksDB spill tier gives no memory relief and may OOM faster. Set {} \
                     to a real scratch volume.",
                    self.spill_path,
                    CONFIG_SPILLABLE_MAP_PATH,
                );
            }
        }
        Ok(self.disk.as_mut().expect("disk just created"))
    }

    /// Bring the in-memory tier back within its memory limits after a grow
    /// (insert / merge) — the orchestrator that runs from every grow path.
    ///
    /// Two limits, in order:
    /// 1. the SOFT spill budget (`max_in_memory_size`): if
    ///    [`over_budget`](Self::over_budget), evict whole source batches down to
    ///    [`EVICTION_LOW_WATER_FRACTION`] of the budget (A6e);
    /// 2. the HARD peak cap ([`CONFIG_MAX_PEAK_MEMORY`]) via
    ///    [`enforce_peak_cap`](Self::enforce_peak_cap), which spills further
    ///    toward the cap and fails loudly if it cannot get under it.
    ///
    /// With no cap set (the default) step 2 is a no-op, so this preserves the
    /// pre-existing spill-only behavior exactly.
    fn enforce_memory_limits(&mut self) -> Result<()> {
        if self.over_budget() {
            let low_water = (self.max_in_memory_size as f64 * EVICTION_LOW_WATER_FRACTION) as u64;
            self.evict_source_batches_until(low_water)?;
        }
        self.enforce_peak_cap()
    }

    /// Evict whole pinned source batches, largest bytes first, until the TRUE
    /// retained heap drops to `target` bytes or no pinned source batch remains to
    /// evict (A6e — the core of the memory-bound fix). A single O(in-memory)
    /// grouping scan.
    ///
    /// Spilling individual entries does NOT free a pinned source batch unless ALL
    /// its refs leave memory, so we instead pick the pinned batch with the most
    /// bytes and evict ALL of its in-memory entries together — dropping the last
    /// ref so the `Arc` (and its dead-row memory) is actually released. For each
    /// chosen batch:
    ///
    /// - **sparse** (live-row ratio < [`COMPACTION_LIVE_RATIO`]): COMPACT — the
    ///   live rows are re-batched into one small owned batch and the entries
    ///   repointed (no IO).
    /// - **dense** (≥ ratio): SPILL — each live entry is serialized to the RocksDB
    ///   tier and removed from memory (frees the whole batch, costs IO).
    ///
    /// Evicting to a low-water `target` (rather than merely back under budget)
    /// amortizes the single grouping scan across the many subsequent inserts that
    /// then fit without re-scanning. A residual heap above `target` made entirely
    /// of `Owned`/overhead bytes (no pinned batch left to free) is left as-is —
    /// the caller decides whether that is acceptable (the budget path tolerates
    /// it; the cap path fails loudly).
    fn evict_source_batches_until(&mut self, target: u64) -> Result<()> {
        // ── ONE grouping scan: bucket every live in-memory BatchRef entry by its
        // source-batch pointer, capturing keys + row indices + an Arc handle. ──
        struct Victim {
            batch: Arc<RecordBatch>,
            batch_bytes: u64,
            keys: Vec<String>,
            rows: Vec<usize>,
        }
        let mut victims: HashMap<usize, Victim> = HashMap::new();
        for (key, record) in self.in_memory.iter() {
            if let RecordPayload::BatchRef { batch, row_idx } = &record.payload {
                let ptr = Arc::as_ptr(batch) as usize;
                let v = victims.entry(ptr).or_insert_with(|| Victim {
                    batch: batch.clone(),
                    batch_bytes: batch.get_array_memory_size() as u64,
                    keys: Vec::new(),
                    rows: Vec::new(),
                });
                v.keys.push(key.clone());
                v.rows.push(*row_idx);
            }
        }

        // Order: largest bytes first (frees the most per eviction), deterministic
        // tie-break by pointer.
        let mut ordered: Vec<(usize, Victim)> = victims.into_iter().collect();
        ordered.sort_by(|a, b| b.1.batch_bytes.cmp(&a.1.batch_bytes).then(a.0.cmp(&b.0)));

        for (_ptr, victim) in ordered {
            if self.true_retained_bytes() <= target {
                break;
            }
            let num_rows = victim.batch.num_rows();
            let live = victim.keys.len();
            let live_ratio = if num_rows == 0 {
                1.0
            } else {
                live as f64 / num_rows as f64
            };
            if live_ratio < COMPACTION_LIVE_RATIO {
                self.compact_victim_batch(&victim.batch, &victim.keys, &victim.rows)?;
            } else {
                self.spill_victim_batch(&victim.keys)?;
            }
        }
        self.track_peak();
        Ok(())
    }

    /// Enforce the HARD peak-memory cap ([`CONFIG_MAX_PEAK_MEMORY`]).
    ///
    /// No cap set (the default) → a no-op, preserving the pre-existing spill-only
    /// behavior. When a cap is set and the tracked in-memory footprint exceeds
    /// it, first try to get back under by spilling/compacting whole source
    /// batches down to [`EVICTION_LOW_WATER_FRACTION`] `× cap` (the same
    /// low-water amortization the soft-budget path uses — evicting past the cap
    /// in one pass lets the many subsequent inserts fit without re-scanning,
    /// rather than re-triggering eviction on every insert that nudges back over
    /// the exact cap); if the footprint STILL exceeds the cap (spilling cannot
    /// help — e.g. a single oversized `Owned` record, or one pinned batch whose
    /// live rows alone exceed the cap), return [`CoreError::MemoryLimitExceeded`]
    /// rather than continuing to allocate. This converts a silent executor OOM
    /// into a bounded, loud failure.
    ///
    /// NOTE: eviction only spills whole pinned *source batches*
    /// ([`evict_source_batches_until`](Self::evict_source_batches_until)); it does
    /// NOT spill `Owned`/updated records (those are merged results with no source
    /// batch to release). A footprint made irreducible by such records is exactly
    /// the case that fails loud here — intended: the cap's contract is a bounded
    /// loud failure, not spilling every possible byte.
    ///
    /// # Errors
    ///
    /// [`CoreError::MemoryLimitExceeded`] when the footprint cannot be reduced to
    /// the cap by spilling.
    fn enforce_peak_cap(&mut self) -> Result<()> {
        let Some(cap) = self.max_peak_in_memory_size else {
            return Ok(());
        };
        if self.true_retained_bytes() <= cap {
            return Ok(());
        }
        // Evict whole source batches down to the low-water mark (a fraction of
        // the cap), not merely back to the exact cap — this amortizes the single
        // grouping scan across subsequent inserts, mirroring the soft-budget
        // path. The loud error below still fires only if we cannot even get back
        // under the cap itself.
        let low_water = (cap as f64 * EVICTION_LOW_WATER_FRACTION) as u64;
        self.evict_source_batches_until(low_water)?;
        let footprint = self.true_retained_bytes();
        if footprint > cap {
            return Err(CoreError::MemoryLimitExceeded(format!(
                "hudi-rs merge map in-memory footprint {footprint} bytes exceeds the configured \
                 hard cap {cap} bytes ({CONFIG_MAX_PEAK_MEMORY}) and cannot be reduced by \
                 spilling (a single record or pinned source batch alone exceeds the cap); \
                 failing loudly instead of risking a silent executor OOM"
            )));
        }
        Ok(())
    }

    /// COMPACT one over-budget source batch: interleave its live rows into one
    /// small owned batch and repoint the entries to it, dropping the original
    /// `Arc`. No IO. (A6e sparse branch — mirrors the A2 compaction primitive.)
    fn compact_victim_batch(
        &mut self,
        victim_batch: &Arc<RecordBatch>,
        keys: &[String],
        rows: &[usize],
    ) -> Result<()> {
        let indices: Vec<(usize, usize)> = rows.iter().map(|&r| (0usize, r)).collect();
        let compact = arrow::compute::interleave_record_batch(&[victim_batch.as_ref()], &indices)
            .map_err(|e| {
            CoreError::ReadFileSliceError(format!(
                "spill/compact: failed to interleave {} survivors of a pinned source batch: {e}",
                keys.len()
            ))
        })?;
        let compact = Arc::new(compact);

        // Repoint each surviving entry to its row in the compact batch. `keys[i]`
        // was the i-th row fed to interleave, so it now lives at compact row `i`.
        // Re-account: drop the old BatchRef's pin (all keys reference the single
        // victim batch), add a pin to the compact batch. The borrows on
        // `self.in_memory` and `self.pinned` are kept disjoint by mutating the
        // payload first, then the pin accounting.
        for (compact_row, key) in keys.iter().enumerate() {
            let repointed = if let Some(record) = self.in_memory.get_mut(key) {
                record.payload = RecordPayload::BatchRef {
                    batch: Arc::clone(&compact),
                    row_idx: compact_row,
                };
                true
            } else {
                false
            };
            if repointed {
                // Old ref to the victim batch is gone; account the move.
                self.pinned.remove_ref_to(victim_batch);
                self.pinned.add_ref(&compact);
            }
        }
        Ok(())
    }

    /// SPILL one over-budget source batch: serialize each of its live entries to
    /// the RocksDB tier and remove them from memory so the last ref drops and the
    /// whole batch is freed (A6e dense branch — uses the A1 IPC spill path).
    fn spill_victim_batch(&mut self, keys: &[String]) -> Result<()> {
        // Remove the entries from memory first (un-accounting their pins) so the
        // Arc's last ref is gone before we hold any borrow on `self.disk`.
        let mut spilled: Vec<(String, BufferedRecord)> = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(record) = self.in_memory.remove(key) {
                self.account_removed(key, &record);
                spilled.push((key.clone(), record));
            }
        }
        let disk = self.ensure_disk()?;
        for (key, record) in &spilled {
            disk.put(key, record)?;
        }
        // Flush the staged rows to disk NOW so the dense branch actually delivers
        // on its contract ("the whole batch is freed"). `put` stages each record
        // as-is, and a staged `BatchRef` clone re-pins its ENTIRE source batch
        // (a 1-row slice still references the parent's buffers). Without an
        // immediate flush, a dense victim with fewer than `SPILL_BATCH_ROWS` live
        // rows would keep its parent batch resident until an unrelated flush
        // trips — so a wide-row table under a tight budget could hold pinned
        // batches well past the budget (A6e/GAP-10). Flushing materializes the
        // staged rows into one compact on-disk batch and drops the slices,
        // releasing the parent. (Eviction is the cold/over-budget path, so the
        // extra small on-disk batch is an acceptable trade for the bound.)
        disk.flush()?;
        Ok(())
    }

    /// Compact every in-memory source batch whose live-row ratio is below
    /// `live_ratio_threshold` (the A2 end-of-scan safety valve), keeping the
    /// pinned-bytes accounting in sync.
    ///
    /// This is the accounting-aware replacement for the old free-function
    /// `compact_pinned_batches`: it groups in-memory `BatchRef` entries by source
    /// batch, and for any batch below the threshold re-batches its survivors into
    /// one compact owned batch and repoints the entries (releasing the original).
    /// Runs once after log scanning, before the drain — by then the size trackers
    /// no longer drive any routing decision, but keeping them accurate keeps the
    /// peak stat and the drift detector honest.
    pub fn compact_sparse_batches(&mut self, live_ratio_threshold: f64) -> Result<()> {
        // Group keys + a representative Arc by source-batch pointer.
        struct Group {
            batch: Arc<RecordBatch>,
            num_rows: usize,
            keys: Vec<String>,
            rows: Vec<usize>,
        }
        let mut groups: HashMap<usize, Group> = HashMap::new();
        for (key, record) in self.in_memory.iter() {
            if let RecordPayload::BatchRef { batch, row_idx } = &record.payload {
                let g = groups
                    .entry(Arc::as_ptr(batch) as usize)
                    .or_insert_with(|| Group {
                        batch: batch.clone(),
                        num_rows: batch.num_rows(),
                        keys: Vec::new(),
                        rows: Vec::new(),
                    });
                g.keys.push(key.clone());
                g.rows.push(*row_idx);
            }
        }
        for group in groups.into_values() {
            if group.num_rows == 0 {
                continue;
            }
            let ratio = group.keys.len() as f64 / group.num_rows as f64;
            if ratio >= live_ratio_threshold {
                continue;
            }
            self.compact_victim_batch(&group.batch, &group.keys, &group.rows)?;
        }
        self.track_peak();
        Ok(())
    }
}

impl Default for SpillableRecordMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Draining iterator over a [`SpillableRecordMap`]: in-memory entries first,
/// then disk entries (Java iteration order). Errors from the disk tier are
/// surfaced per item.
pub struct SpillDrainIter {
    in_memory: std::vec::IntoIter<BufferedRecord>,
    disk_iter: Option<RocksDbDrainIter>,
}

impl Iterator for SpillDrainIter {
    type Item = Result<BufferedRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(r) = self.in_memory.next() {
            return Some(Ok(r));
        }
        match self.disk_iter.as_mut() {
            Some(it) => it.next(),
            None => None,
        }
    }
}

/// RocksDB-backed disk tier for spilled merge-map entries.
///
/// Single column family, WAL disabled (the spill map is rebuildable from the
/// logs), no compression, bounded memtable + block-cache memory. The temp dir
/// is removed on drop (RAII via [`tempfile::TempDir`]); the `DB` handle is
/// closed when this struct drops. See module docs for the option rationale.
/// Rows packed into one spilled Arrow batch (B5 multi-row batching). Amortizes the
/// per-record RocksDB op + IPC framing/decode over many rows — turning the spill
/// from N single-row blobs into N/`SPILL_BATCH_ROWS` multi-row blobs.
const SPILL_BATCH_ROWS: usize = 1024;
/// Decoded-batch LRU capacity. A batch-sequential drain (and clustered probes)
/// then pays one RocksDB get + one decode per batch, shared across its rows.
///
/// Worst-case resident footprint: up to `SPILL_BATCH_CACHE × SPILL_BATCH_ROWS`
/// (= 8 × 1024 = 8,192) fully-decoded rows, i.e. `8,192 × row_width` bytes. This
/// is a fixed overhead *on top of* `hoodie.memory.merge.max.size` and is not
/// counted against that budget, so for very wide schemas it is a bounded but
/// non-trivial RAM spike during drain (tracked follow-up: size by row width or
/// count against the budget).
const SPILL_BATCH_CACHE: usize = 8;
/// Batch blob tag: body-only (decode against the cached schema).
const SPILL_BATCH_TAG_BODY: u8 = 0x01;
/// Batch blob tag: self-describing IPC stream (fallback when body-only can't
/// encode, e.g. dictionary columns).
const SPILL_BATCH_TAG_FULL: u8 = 0x02;

/// Location of a flushed record: a row inside a spilled batch, or a delete tombstone.
enum DiskLoc {
    Data {
        batch_id: u64,
        row_idx: u32,
    },
    /// A delete tombstone (no batch row). Carries the record's TRUE `record_key`
    /// so a spilled delete round-trips it faithfully: this map may be keyed by
    /// base-file POSITION (position-based merge), not by record key, so
    /// `record_from_entry` must NOT reconstruct a delete's key from the map key —
    /// doing so lands the tombstone under a position string and it silently fails
    /// to suppress its target row after key-based fallback (PR #95 review).
    Delete {
        record_key: String,
    },
}

/// Per-key index entry for a flushed (on-disk) record. The ordering value is held
/// here (not in the batch) so a row can be reconstructed without re-extracting it.
struct DiskEntry {
    ordering_value: Option<OrderingValue>,
    loc: DiskLoc,
}

/// Disk tier of the spillable merge map, with B5 multi-row batching.
///
/// Records accumulate in `staging` (in RAM, ≤ `SPILL_BATCH_ROWS`); on flush they
/// are packed into ONE Arrow batch written under a synthetic `batch_id` key, and
/// `index` records `key → (ordering, batch_id, row_idx)`. Reads resolve via the
/// index + a small decoded-batch LRU. Overwrites just repoint the index (the stale
/// row becomes dead — never referenced). Deletes live in the index (no batch row).
pub struct RocksDbDiskMap {
    db: DB,
    /// Owns the on-disk directory; removed on drop (RAII).
    dir: tempfile::TempDir,
    /// Live (distinct-key) entry count across staging + index.
    len: usize,
    /// Interned schemas seen across spilled batches, addressed by id (= index).
    /// Each body-only batch blob records its schema id, so a flush can pack a
    /// MIX of schemas (e.g. narrow partial-update records alongside full ones)
    /// into separate per-schema batches and decode each against the right schema
    /// — without the per-record framing self-describing IPC would cost. Mirrors
    /// Java's `BufferedRecord.schemaId` + `AvroSchemaCache`. Almost always holds
    /// one entry (the reader schema); partial-update tables add a few narrow ones.
    schemas: Vec<SchemaRef>,
    /// Data records not yet flushed to a batch (≤ `SPILL_BATCH_ROWS`). Kept as full
    /// records so `get`/`contains_key` see them without forcing a flush.
    staging: MergeMap,
    /// Flushed records: `key → (ordering, location)`.
    index: HashMap<String, DiskEntry>,
    /// Monotonic id for the next flushed batch (the RocksDB key).
    next_batch_id: u64,
    /// Decoded-batch LRU (interior-mutable so `get` stays `&self`).
    cache: RefCell<VecDeque<(u64, Arc<RecordBatch>)>>,
}

impl RocksDbDiskMap {
    /// Create a fresh RocksDB under a uuid subdirectory of `parent`.
    ///
    /// The parent directory is created if missing. The DB directory is owned by
    /// a [`tempfile::TempDir`] so it is removed on drop even on panic.
    pub fn create(parent: &Path) -> Result<Self> {
        std::fs::create_dir_all(parent).map_err(|e| {
            CoreError::ReadFileSliceError(format!(
                "spill: failed to create spill parent dir {parent:?}: {e}"
            ))
        })?;
        let dir = tempfile::Builder::new()
            .prefix("hudi-spill-")
            .tempdir_in(parent)
            .map_err(|e| {
                CoreError::ReadFileSliceError(format!(
                    "spill: failed to create spill temp dir under {parent:?}: {e}"
                ))
            })?;
        let db = DB::open(&Self::options(), dir.path()).map_err(|e| {
            CoreError::ReadFileSliceError(format!(
                "spill: failed to open RocksDB at {:?}: {e}",
                dir.path()
            ))
        })?;
        Ok(Self {
            db,
            dir,
            len: 0,
            schemas: Vec::new(),
            staging: HashMap::default(),
            index: HashMap::new(),
            next_batch_id: 0,
            cache: RefCell::new(VecDeque::new()),
        })
    }

    /// Build the bounded-memory RocksDB options (see module docs / constants).
    fn options() -> Options {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size(ROCKSDB_WRITE_BUFFER_SIZE);
        opts.set_max_write_buffer_number(ROCKSDB_MAX_WRITE_BUFFERS);
        // Spill values are already compact single-row IPC blobs; no compression
        // (trade revisited in B5 for the large-scale disk footprint).
        opts.set_compression_type(DBCompressionType::None);

        let cache = Cache::new_lru_cache(ROCKSDB_BLOCK_CACHE_BYTES);
        let mut bbt = BlockBasedOptions::default();
        bbt.set_block_cache(&cache);
        // Count index/filter blocks against the (tiny) cache so engine memory
        // stays bounded.
        bbt.set_cache_index_and_filter_blocks(true);
        opts.set_block_based_table_factory(&bbt);
        opts
    }

    /// `WriteOptions` with WAL disabled (the spill map is rebuildable).
    fn write_options() -> WriteOptions {
        let mut wo = WriteOptions::default();
        wo.disable_wal(true);
        wo
    }

    /// Spill (or overwrite) a record at `key`. Data records accumulate in
    /// `staging` and are flushed in `SPILL_BATCH_ROWS`-row batches (B5); deletes go
    /// straight to the index. Overwrites repoint the index (stale rows go dead).
    pub fn put(&mut self, key: &str, value: &BufferedRecord) -> Result<()> {
        let was_present = self.staging.contains_key(key) || self.index.contains_key(key);
        if value.is_delete() {
            // Delete tombstone: no batch row. Drop any staged data for the key so
            // `get` sees the delete (the index Delete also shadows a flushed row).
            self.staging.remove(key);
            self.index.insert(
                key.to_string(),
                DiskEntry {
                    ordering_value: value.ordering_value.clone(),
                    loc: DiskLoc::Delete {
                        record_key: value.record_key.clone(),
                    },
                },
            );
        } else {
            // Data: stage it. `get` prefers staging, and the eventual flush repoints
            // the index (overwriting any prior Data/Delete entry for this key).
            self.staging.insert(key.to_string(), value.clone());
        }
        if !was_present {
            self.len += 1;
        }
        if self.staging.len() >= SPILL_BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    /// Intern a schema, returning its stable id (= index in `self.schemas`).
    /// Linear scan: the cache holds one entry for a normal table and only a few
    /// for partial-update tables, so this is cheaper than hashing a schema.
    fn intern_schema(&mut self, schema: &SchemaRef) -> u32 {
        if let Some(i) = self.schemas.iter().position(|s| s == schema) {
            return i as u32;
        }
        self.schemas.push(schema.clone());
        (self.schemas.len() - 1) as u32
    }

    /// Pack staged data records into Arrow batches — ONE per distinct schema —
    /// write each under a fresh `batch_id`, and repoint the index
    /// `key → (batch_id, row_idx)`. Records may carry DIFFERENT schemas (a narrow
    /// partial-update record alongside full ones), so they are grouped by interned
    /// schema id; each batch's blob records that id so it decodes against the right
    /// schema. The common case (all reader-schema) yields exactly one group/batch.
    fn flush(&mut self) -> Result<()> {
        if self.staging.is_empty() {
            return Ok(());
        }
        let staged: Vec<(String, BufferedRecord)> = self.staging.drain().collect();
        // Group by interned schema id, preserving first-seen order.
        #[allow(clippy::type_complexity)]
        let mut groups: Vec<(u32, Vec<RecordBatch>, Vec<(String, Option<OrderingValue>)>)> =
            Vec::new();
        for (k, rec) in staged {
            // Staging holds data only (deletes go to the index in `put`).
            let batch = rec.get_record().ok_or_else(|| {
                CoreError::ReadFileSliceError("spill flush: staged record had no data".to_string())
            })?;
            let sid = self.intern_schema(&batch.schema());
            match groups.iter_mut().find(|(g, _, _)| *g == sid) {
                Some((_, rows, keys)) => {
                    rows.push(batch);
                    keys.push((k, rec.ordering_value.clone()));
                }
                None => groups.push((sid, vec![batch], vec![(k, rec.ordering_value.clone())])),
            }
        }
        for (sid, rows, keys) in groups {
            let schema = rows[0].schema();
            let batch =
                arrow_select::concat::concat_batches(&schema, rows.iter()).map_err(|e| {
                    CoreError::ReadFileSliceError(format!(
                        "spill flush: concat_batches failed: {e}"
                    ))
                })?;
            // Body-only (decode against schema `sid`); else self-describing fallback
            // (e.g. dictionary columns), which carries its own schema so needs no id.
            let blob = match row_serde::to_binary_row_body(&batch) {
                Some(body) => {
                    let mut v = Vec::with_capacity(1 + 4 + body.len());
                    v.push(SPILL_BATCH_TAG_BODY);
                    v.extend_from_slice(&sid.to_le_bytes());
                    v.extend_from_slice(&body);
                    v
                }
                None => {
                    let full = row_serde::to_binary_row(&batch.schema(), &batch);
                    let mut v = Vec::with_capacity(1 + full.len());
                    v.push(SPILL_BATCH_TAG_FULL);
                    v.extend_from_slice(&full);
                    v
                }
            };
            let batch_id = self.next_batch_id;
            self.next_batch_id += 1;
            let mut wb = WriteBatch::default();
            wb.put(batch_id.to_le_bytes(), blob);
            self.db.write_opt(wb, &Self::write_options()).map_err(|e| {
                CoreError::ReadFileSliceError(format!("spill: RocksDB batch write failed: {e}"))
            })?;
            for (row_idx, (k, ov)) in keys.into_iter().enumerate() {
                self.index.insert(
                    k,
                    DiskEntry {
                        ordering_value: ov,
                        loc: DiskLoc::Data {
                            batch_id,
                            row_idx: row_idx as u32,
                        },
                    },
                );
            }
        }
        Ok(())
    }

    /// Decode a spilled batch blob (tag-dispatched: body-only vs self-describing).
    fn decode_batch(&self, raw: &[u8]) -> Result<RecordBatch> {
        let (tag, body) = raw
            .split_first()
            .ok_or_else(|| CoreError::ReadFileSliceError("spill: empty batch blob".to_string()))?;
        match *tag {
            SPILL_BATCH_TAG_BODY => {
                // [schema_id: u32 LE][ipc body]
                let id_bytes = body.get(0..4).ok_or_else(|| {
                    CoreError::ReadFileSliceError(
                        "spill: body-only batch blob too short for schema id".to_string(),
                    )
                })?;
                let sid = u32::from_le_bytes(id_bytes.try_into().unwrap()) as usize;
                let schema = self.schemas.get(sid).cloned().ok_or_else(|| {
                    CoreError::ReadFileSliceError(format!(
                        "spill: body-only batch references unknown schema id {sid}"
                    ))
                })?;
                row_serde::from_binary_body(&body[4..], schema)
            }
            SPILL_BATCH_TAG_FULL => row_serde::from_binary(body),
            other => Err(CoreError::ReadFileSliceError(format!(
                "spill: unknown batch tag {other:#04x}"
            ))),
        }
    }

    /// Load a spilled batch by id, via the decoded-batch LRU (one RocksDB get +
    /// one decode per batch, shared across its rows).
    fn load_batch(&self, batch_id: u64) -> Result<Arc<RecordBatch>> {
        if let Some((_, b)) = self.cache.borrow().iter().find(|(id, _)| *id == batch_id) {
            return Ok(b.clone());
        }
        let raw = self
            .db
            .get(batch_id.to_le_bytes())
            .map_err(|e| CoreError::ReadFileSliceError(format!("spill: batch get failed: {e}")))?
            .ok_or_else(|| {
                CoreError::ReadFileSliceError(format!("spill: batch {batch_id} missing"))
            })?;
        let batch = Arc::new(self.decode_batch(&raw)?);
        let mut cache = self.cache.borrow_mut();
        cache.push_back((batch_id, batch.clone()));
        if cache.len() > SPILL_BATCH_CACHE {
            cache.pop_front();
        }
        Ok(batch)
    }

    /// Reconstruct the record for a flushed index entry.
    fn record_from_entry(&self, key: &str, entry: &DiskEntry) -> Result<BufferedRecord> {
        match &entry.loc {
            // Use the PERSISTED record key, not the map key: this map may be keyed
            // by base-file position (position-based merge), so reconstructing a
            // delete's key from the map key would silently drop the delete after
            // key-based fallback (PR #95 review).
            DiskLoc::Delete { record_key } => Ok(BufferedRecord::new_delete(
                record_key.clone(),
                entry.ordering_value.clone(),
            )),
            DiskLoc::Data { batch_id, row_idx } => {
                let batch = self.load_batch(*batch_id)?;
                let row = batch.slice(*row_idx as usize, 1);
                Ok(BufferedRecord::new_data(
                    key.to_string(),
                    row,
                    entry.ordering_value.clone(),
                ))
            }
        }
    }

    /// Read back the record at `key`, if present (staging tier preferred — newest).
    pub fn get(&self, key: &str) -> Result<Option<BufferedRecord>> {
        if let Some(r) = self.staging.get(key) {
            return Ok(Some(r.clone()));
        }
        match self.index.get(key) {
            Some(entry) => Ok(Some(self.record_from_entry(key, entry)?)),
            None => Ok(None),
        }
    }

    /// Remove and return the record at `key`, if present. The on-disk batch row (if
    /// any) is left as a dead row — never referenced once the index entry is gone.
    pub fn remove(&mut self, key: &str) -> Result<Option<BufferedRecord>> {
        let existing = self.get(key)?;
        if existing.is_some() {
            self.staging.remove(key);
            self.index.remove(key);
            self.len -= 1;
        }
        Ok(existing)
    }

    /// True if `key` is present in either tier.
    pub fn contains_key(&self, key: &str) -> Result<bool> {
        Ok(self.staging.contains_key(key) || self.index.contains_key(key))
    }

    /// Live entry count.
    pub fn len(&self) -> usize {
        self.len
    }

    /// True if there are no spilled entries.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Consume the disk map into a drain iterator. Staged records are flushed first,
    /// then entries are yielded in `(batch_id, row_idx)` order so each batch is
    /// read and decoded ONCE (deletes last). NOTE: this changes the drain's output
    /// order from the old key-sorted order to batch/insertion order — fine for the
    /// MOR snapshot read (records are a set; the merge already resolved versions).
    fn into_drain_iter(mut self) -> RocksDbDrainIter {
        let flush_err = self.flush().err();
        let mut entries: Vec<(String, DiskEntry)> = self.index.drain().collect();
        entries.sort_by_key(|(_, e)| match e.loc {
            DiskLoc::Data { batch_id, row_idx } => (batch_id, row_idx),
            DiskLoc::Delete { .. } => (u64::MAX, 0),
        });
        RocksDbDrainIter {
            order: entries.into_iter(),
            flush_err,
            // Hold the disk map (DB handle + temp dir + batch cache) alive for the
            // lifetime of the iterator.
            disk: self,
        }
    }
}

impl Drop for RocksDbDiskMap {
    fn drop(&mut self) {
        // RAII: `tempfile::TempDir` removes the directory on its own drop, and
        // the `DB` handle closes on drop. Double-drop safe (each field drops
        // exactly once). We log for operability.
        log::debug!(
            "[RocksDbDiskMap] dropping spill DB at {:?} ({} entries)",
            self.dir.path(),
            self.len
        );
    }
}

/// Forward drain iterator over the RocksDB disk tier. Yields reconstructed
/// [`BufferedRecord`]s in `(batch_id, row_idx)` order (deletes last) so each
/// spilled batch is read + decoded once (via the disk map's batch LRU). Holds the
/// disk map alive until dropped.
pub struct RocksDbDrainIter {
    /// Flushed index entries, pre-sorted by batch location.
    order: std::vec::IntoIter<(String, DiskEntry)>,
    /// A deferred error from the final `flush()` in `into_drain_iter`, surfaced
    /// on the first `next()`.
    flush_err: Option<CoreError>,
    /// Keeps the DB handle + temp dir + batch cache alive until the drain completes.
    disk: RocksDbDiskMap,
}

impl Iterator for RocksDbDrainIter {
    type Item = Result<BufferedRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(e) = self.flush_err.take() {
            return Some(Err(e));
        }
        let (key, entry) = self.order.next()?;
        Some(self.disk.record_from_entry(&key, &entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::reader_v2::buffered_record::OrderingValue;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]))
    }

    /// A single-row owned record of (`key`, `v`) with a Long ordering value.
    fn record(key: &str, v: i32) -> BufferedRecord {
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(vec![key])) as _,
                Arc::new(Int32Array::from(vec![v])) as _,
            ],
        )
        .unwrap();
        BufferedRecord::new_data(key.to_string(), batch, Some(OrderingValue::Long(v as i64)))
    }

    fn tuple(record: &BufferedRecord) -> (String, i32) {
        let b = record.get_record().expect("data record");
        let keys = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let vs = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        (keys.value(0).to_string(), vs.value(0))
    }

    /// A config whose budget keeps everything in memory: max_in_memory_size huge.
    fn mem_only_config() -> SpillConfig {
        SpillConfig {
            max_in_memory_size: u64::MAX,
            spill_path: std::env::temp_dir(),
            diskmap_type: DiskMapType::RocksDb,
            max_peak_in_memory_size: None,
        }
    }

    /// A config that forces spilling after a couple of entries.
    fn tiny_budget_config(max_in_memory_size: u64) -> SpillConfig {
        SpillConfig {
            max_in_memory_size,
            spill_path: std::env::temp_dir(),
            diskmap_type: DiskMapType::RocksDb,
            max_peak_in_memory_size: None,
        }
    }

    /// A single-column (key-only) record — a NARROW schema distinct from the
    /// 2-column `record()` fixture. Stands in for a partial-update log record.
    fn narrow_record(key: &str) -> BufferedRecord {
        let s = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(s, vec![Arc::new(StringArray::from(vec![key])) as _]).unwrap();
        BufferedRecord::new_data(key.to_string(), batch, None)
    }

    /// Mixed-schema spill: a full 2-column record and a narrow 1-column record
    /// spilled together must EACH round-trip against their own schema via the
    /// schema-id cache — not a single shared schema (which would make the flush
    /// `concat_batches` fail on the schema mismatch). Drains both back and checks
    /// each kept its own column count.
    #[test]
    fn spill_mixed_schemas_round_trip_via_schema_id_cache() {
        let mut map = SpillableRecordMap::with_config(tiny_budget_config(0));
        map.insert("full".to_string(), record("full", 7)).unwrap();
        map.insert("part".to_string(), narrow_record("part"))
            .unwrap();

        let mut got: Vec<(String, usize)> = map
            .drain_iter()
            .map(|r| {
                let rec = r.unwrap();
                let cols = rec.get_record().unwrap().num_columns();
                (rec.record_key, cols)
            })
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![("full".to_string(), 2), ("part".to_string(), 1)],
            "full record decodes to 2 columns, narrow partial to 1 — each via its own cached schema"
        );
    }

    /// True-retained bytes a single `Owned` (key, record) entry contributes to
    /// the in-memory tier: `key.len() + ENTRY_OVERHEAD_BYTES + owned array bytes`.
    /// (Test helper — the `record()` fixture always builds an `Owned` payload.)
    fn entry_bytes(key: &str, record: &BufferedRecord) -> u64 {
        SpillableRecordMap::entry_key_overhead_bytes(key)
            + SpillableRecordMap::owned_payload_bytes(record)
    }

    // ── Config parsing ────────────────────────────────────────────────────

    #[test]
    fn bitcask_diskmap_type_aliases_to_rocksdb() {
        // BITCASK is Java's default and output-equivalent, so it is accepted as an
        // alias for the RocksDB spill backend rather than rejected (case-insensitive).
        assert_eq!(
            DiskMapType::parse(Some("BITCASK")).unwrap(),
            DiskMapType::RocksDb
        );
        assert_eq!(
            DiskMapType::parse(Some("bitcask")).unwrap(),
            DiskMapType::RocksDb
        );
    }

    #[test]
    fn unknown_diskmap_type_is_rejected_with_value_in_message() {
        let err = DiskMapType::parse(Some("LEVEL_DB")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("LEVEL_DB"), "error must name the value: {msg}");
        assert!(matches!(err, CoreError::Unsupported(_)));
    }

    #[test]
    fn rocksdb_and_unset_diskmap_type_are_ok() {
        assert_eq!(DiskMapType::parse(None).unwrap(), DiskMapType::RocksDb);
        assert_eq!(
            DiskMapType::parse(Some("ROCKS_DB")).unwrap(),
            DiskMapType::RocksDb
        );
        assert_eq!(
            DiskMapType::parse(Some("rocks_db")).unwrap(),
            DiskMapType::RocksDb
        );
    }

    #[test]
    fn spill_config_defaults_and_budget_math() {
        let cfg = SpillConfig::from_config(&HashMap::new()).unwrap();
        // default 1 GiB → 0.8 GiB − reserved.
        let expected = (DEFAULT_MERGE_MAX_SIZE_BYTES as f64 * SPILL_TRIGGER_FRACTION) as u64
            - ROCKSDB_RESERVED_BYTES;
        assert_eq!(cfg.max_in_memory_size, expected);
        assert_eq!(cfg.spill_path, PathBuf::from(DEFAULT_SPILL_PATH));
        assert_eq!(cfg.diskmap_type, DiskMapType::RocksDb);
    }

    #[test]
    fn spill_config_rejects_bad_merge_size() {
        let mut cfg = HashMap::new();
        cfg.insert(
            CONFIG_MERGE_MAX_SIZE.to_string(),
            "not-a-number".to_string(),
        );
        assert!(matches!(
            SpillConfig::from_config(&cfg),
            Err(CoreError::InvalidValue(_))
        ));
    }

    #[test]
    fn spill_config_tiny_budget_saturates_at_zero() {
        // merge_max_size so small that 0.8× < reserved → budget saturates to 0.
        let cfg =
            SpillConfig::from_parts(1024, PathBuf::from("/tmp"), DiskMapType::RocksDb).unwrap();
        assert_eq!(cfg.max_in_memory_size, 0);
    }

    // ── Spill trigger + size accounting ───────────────────────────────────

    #[test]
    fn stays_in_memory_below_budget_no_spill() {
        let mut map = SpillableRecordMap::with_config(mem_only_config());
        for i in 0..50 {
            map.insert(format!("k{i}"), record(&format!("k{i}"), i))
                .unwrap();
        }
        assert_eq!(map.len(), 50);
        assert!(!map.spill_fired(), "must not spill below budget");
        assert!(map.disk.is_none());
        // Size tracking is exact: sum of per-entry estimates.
        let expected: u64 = (0..50)
            .map(|i| {
                let k = format!("k{i}");
                entry_bytes(&k, &record(&k, i))
            })
            .sum();
        assert_eq!(map.true_retained_bytes(), expected);
    }

    #[test]
    fn spills_at_budget_boundary() {
        // Budget = exactly 2 entries' worth. After two inserts the running size
        // equals the budget, so `over_budget` (>=) is true and the 3rd spills.
        let one = entry_bytes("k0", &record("k0", 0));
        let mut map = SpillableRecordMap::with_config(tiny_budget_config(one * 2));

        map.insert("k0".to_string(), record("k0", 0)).unwrap();
        map.insert("k1".to_string(), record("k1", 1)).unwrap();
        assert!(!map.spill_fired(), "first two entries fit in memory");
        assert_eq!(map.in_memory.len(), 2);

        // Now over budget → next NEW key spills.
        map.insert("k2".to_string(), record("k2", 2)).unwrap();
        assert!(map.spill_fired(), "third entry must spill");
        assert_eq!(map.in_memory.len(), 2, "existing entries stay in memory");
        assert_eq!(map.len(), 3, "all three are counted across tiers");
        assert!(map.contains_key("k2").unwrap());
    }

    /// B5 batched spill must cross the `SPILL_BATCH_ROWS` (1024) flush boundary:
    /// with budget 0 every entry spills to disk staging, and once more than 1024
    /// are staged they are flushed into multi-row batches. Every record must
    /// still read back with its exact data across that boundary.
    #[test]
    fn spill_flush_batches_round_trip_across_boundary() {
        let mut map = SpillableRecordMap::with_config(tiny_budget_config(0));
        const N: i32 = 1024 + 200; // one full flush batch + a partial
        for i in 0..N {
            let key = format!("k{i}");
            map.insert(key.clone(), record(&key, i)).unwrap();
        }
        assert!(map.spill_fired(), "budget 0 spills everything to disk");
        assert_eq!(map.len(), N as usize, "all N entries counted across tiers");
        // Round-trip a sample spanning the 1024-row flush boundary.
        for i in [0, 1, 1023, 1024, 1025, N - 1] {
            let key = format!("k{i}");
            assert_eq!(
                tuple(&map.get(&key).unwrap().unwrap()),
                (key.clone(), i),
                "exact round-trip across the flush boundary at {i}"
            );
        }
    }

    /// Body-only spill blobs decode by POSITION against the schema cached from the
    /// first record. A later flush batch whose schema differs (an evolved /
    /// reconciled batch mid-merge) must NOT be body-only encoded — it would
    /// silently decode to the wrong columns. The encode falls back to the
    /// self-describing TAG_FULL form on a schema mismatch, so the mismatched batch
    /// still round-trips EXACTLY. (A single flush batch can't mix schemas —
    /// `concat_batches` would error — so the mismatch only arises across flushes;
    /// the first 1024 records fill batch-1 and cache its schema, then the
    /// differently-shaped record flushes as batch-2.)
    #[test]
    fn spill_mismatched_schema_batch_round_trips_via_full_tag() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};

        let mut map = SpillableRecordMap::with_config(tiny_budget_config(0)); // budget 0 → all spill
        // Batch 1: 1024 records of the [key: Utf8, v: Int32] schema. Record 0
        // caches spill_schema; the batch matches it (body-only path).
        for i in 0..SPILL_BATCH_ROWS as i32 {
            let key = format!("k{i}");
            map.insert(key.clone(), record(&key, i)).unwrap();
        }
        // Batch 2: one record with a DIFFERENT schema ([key: Utf8, v: Int64,
        // extra: Int32]) — mismatches the cached schema, so it must take TAG_FULL.
        let alt_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("v", DataType::Int64, true),
            Field::new("extra", DataType::Int32, true),
        ]));
        let alt_batch = RecordBatch::try_new(
            alt_schema,
            vec![
                Arc::new(StringArray::from(vec!["alt"])) as _,
                Arc::new(Int64Array::from(vec![99_i64])) as _,
                Arc::new(Int32Array::from(vec![42])) as _,
            ],
        )
        .unwrap();
        map.insert(
            "alt".to_string(),
            BufferedRecord::new_data("alt".to_string(), alt_batch, Some(OrderingValue::Long(99))),
        )
        .unwrap();
        assert!(map.spill_fired(), "budget 0 spills everything");

        // Drain to exercise the encode/decode path. `into_drain_iter` FLUSHES the
        // lone staged 'alt' to disk first: its schema mismatches the cached
        // [Utf8, Int32], so the guard MUST fall back to TAG_FULL; the drain then
        // reads it back through `decode_batch`. (A plain `get("alt")` would return
        // it straight from the in-RAM staging map, never touching the guard or
        // decode — so this test drains rather than gets.) Without the guard, 'alt'
        // would be TAG_BODY-encoded and decoded against the 2-column cached schema,
        // producing wrong/short columns instead of the 3 below.
        let drained: Vec<BufferedRecord> = map.drain_iter().collect::<Result<_>>().unwrap();
        assert_eq!(
            drained.len(),
            SPILL_BATCH_ROWS + 1,
            "all records drained across both flush batches"
        );
        let alt = drained
            .iter()
            .find(|r| r.record_key == "alt")
            .expect("'alt' record drained from disk");
        let b = alt.get_record().expect("data record");
        assert_eq!(
            b.num_columns(),
            3,
            "mismatched-schema batch kept its own 3 columns (via TAG_FULL), not the cached 2"
        );
        assert_eq!(
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "alt"
        );
        assert_eq!(
            b.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            99
        );
        assert_eq!(
            b.column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            42
        );
    }

    // ── Exact-data round-trip across tiers ────────────────────────────────

    #[test]
    fn get_returns_exact_data_from_both_tiers() {
        let one = entry_bytes("k0", &record("k0", 0));
        let mut map = SpillableRecordMap::with_config(tiny_budget_config(one));
        map.insert("mem".to_string(), record("mem", 10)).unwrap();
        // over budget now
        map.insert("disk".to_string(), record("disk", 20)).unwrap();
        assert!(map.spill_fired());

        assert_eq!(
            tuple(&map.get("mem").unwrap().unwrap()),
            ("mem".to_string(), 10)
        );
        assert_eq!(
            tuple(&map.get("disk").unwrap().unwrap()),
            ("disk".to_string(), 20)
        );
        assert_eq!(
            map.get("disk").unwrap().unwrap().ordering_value,
            Some(OrderingValue::Long(20))
        );
        assert!(map.get("absent").unwrap().is_none());
    }

    #[test]
    fn spilled_delete_tombstone_round_trips() {
        let mut map = SpillableRecordMap::with_config(tiny_budget_config(0));
        // budget 0 → everything spills.
        map.insert(
            "del".to_string(),
            BufferedRecord::new_delete("del".to_string(), Some(OrderingValue::Long(3))),
        )
        .unwrap();
        assert!(map.spill_fired());
        let back = map.get("del").unwrap().unwrap();
        assert!(back.is_delete(), "spilled tombstone reloads as a delete");
        assert_eq!(back.ordering_value, Some(OrderingValue::Long(3)));
    }

    // ── Iteration order: memory first, then disk ──────────────────────────

    #[test]
    fn drain_iter_yields_memory_then_disk_with_all_data() {
        let one = entry_bytes("k0", &record("k0", 0));
        let mut map = SpillableRecordMap::with_config(tiny_budget_config(one * 2));
        map.insert("m0".to_string(), record("m0", 0)).unwrap();
        map.insert("m1".to_string(), record("m1", 1)).unwrap();
        // these spill
        map.insert("d0".to_string(), record("d0", 2)).unwrap();
        map.insert("d1".to_string(), record("d1", 3)).unwrap();
        assert!(map.spill_fired());

        let drained: Vec<BufferedRecord> = map.drain_iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(drained.len(), 4, "all entries drained");

        // First two must be the in-memory keys (order within a tier is
        // unspecified, but the mem tier precedes the disk tier).
        let first_two: std::collections::HashSet<String> =
            drained[..2].iter().map(|r| r.record_key.clone()).collect();
        assert_eq!(
            first_two,
            std::collections::HashSet::from(["m0".to_string(), "m1".to_string()]),
            "in-memory entries come first"
        );
        let last_two: std::collections::HashSet<String> =
            drained[2..].iter().map(|r| r.record_key.clone()).collect();
        assert_eq!(
            last_two,
            std::collections::HashSet::from(["d0".to_string(), "d1".to_string()]),
            "disk entries come after"
        );

        // Full-data check: every key resolves to its exact tuple.
        let mut got: Vec<(String, i32)> = drained.iter().map(tuple).collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("d0".to_string(), 2),
                ("d1".to_string(), 3),
                ("m0".to_string(), 0),
                ("m1".to_string(), 1),
            ]
        );
    }

    // ── Remove across both tiers ──────────────────────────────────────────

    #[test]
    fn remove_across_tiers_decrements_and_absents() {
        let one = entry_bytes("k0", &record("k0", 0));
        let mut map = SpillableRecordMap::with_config(tiny_budget_config(one));
        map.insert("mem".to_string(), record("mem", 1)).unwrap();
        map.insert("disk".to_string(), record("disk", 2)).unwrap();
        assert!(map.spill_fired());

        let size_before = map.true_retained_bytes();
        let removed_mem = map.remove("mem").unwrap().unwrap();
        assert_eq!(tuple(&removed_mem), ("mem".to_string(), 1));
        assert!(
            map.true_retained_bytes() < size_before,
            "removing an in-memory entry decrements the size estimate"
        );
        assert!(!map.contains_key("mem").unwrap());

        let removed_disk = map.remove("disk").unwrap().unwrap();
        assert_eq!(tuple(&removed_disk), ("disk".to_string(), 2));
        assert!(!map.contains_key("disk").unwrap());
        assert_eq!(map.len(), 0);

        // Removing absent keys is a clean None.
        assert!(map.remove("nope").unwrap().is_none());
    }

    // ── Overwrite semantics ───────────────────────────────────────────────

    #[test]
    fn overwriting_in_memory_key_updates_in_place() {
        let mut map = SpillableRecordMap::with_config(mem_only_config());
        map.insert("k".to_string(), record("k", 1)).unwrap();
        map.insert("k".to_string(), record("k", 2)).unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(tuple(&map.get("k").unwrap().unwrap()), ("k".to_string(), 2));
    }

    #[test]
    fn overwriting_spilled_key_updates_on_disk() {
        let mut map = SpillableRecordMap::with_config(tiny_budget_config(0));
        map.insert("k".to_string(), record("k", 1)).unwrap();
        map.insert("k".to_string(), record("k", 2)).unwrap();
        assert!(map.spill_fired());
        assert_eq!(map.len(), 1, "overwrite must not double-count on disk");
        assert_eq!(tuple(&map.get("k").unwrap().unwrap()), ("k".to_string(), 2));
    }

    // ── Lifecycle: temp dir removed on drop ───────────────────────────────

    #[test]
    fn spill_dir_is_removed_on_drop() {
        let dir_path;
        {
            let mut map = SpillableRecordMap::with_config(tiny_budget_config(0));
            map.insert("k".to_string(), record("k", 1)).unwrap();
            dir_path = map.disk.as_ref().unwrap().dir.path().to_path_buf();
            assert!(dir_path.exists(), "spill dir exists while map is alive");
        }
        assert!(
            !dir_path.exists(),
            "spill dir must be removed on drop (RAII cleanup)"
        );
    }

    // ══════════════════════════════════════════════════════════════════════
    // A6e — pinned-bytes accounting + evict-by-source-batch + detector #1
    // ══════════════════════════════════════════════════════════════════════

    /// A wide multi-row source batch of `(k{base+i}, v=base+i)` rows, interned
    /// once. `base` lets callers mint batches with disjoint data.
    fn wide_batch_from(base: usize, num_rows: usize) -> Arc<RecordBatch> {
        let keys: Vec<String> = (0..num_rows).map(|i| format!("k{:06}", base + i)).collect();
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let vs: Vec<i32> = (0..num_rows).map(|i| (base + i) as i32).collect();
        Arc::new(
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(StringArray::from(key_refs)) as _,
                    Arc::new(Int32Array::from(vs)) as _,
                ],
            )
            .unwrap(),
        )
    }

    /// A wide multi-row source batch of `(k{i}, v=i)` rows, interned once.
    fn wide_batch(num_rows: usize) -> Arc<RecordBatch> {
        wide_batch_from(0, num_rows)
    }

    /// The (data key, value) tuple a source row resolves to under `wide_batch_from`.
    fn row_data(base: usize, row: usize) -> (String, i32) {
        (format!("k{:06}", base + row), (base + row) as i32)
    }

    /// Independent ground truth: sum of `get_array_memory_size` over the DISTINCT
    /// live `Arc<RecordBatch>` referenced by the in-memory tier (the real heap the
    /// BatchRefs pin), plus owned-payload + key/overhead bytes — i.e. the same
    /// quantity the map claims to track, computed from scratch.
    fn true_retained_from_scratch(map: &SpillableRecordMap) -> u64 {
        let mut seen: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut pinned = 0u64;
        let mut owned_overhead = 0u64;
        for (key, r) in map.in_memory.iter() {
            owned_overhead += key.len() as u64 + ENTRY_OVERHEAD_BYTES;
            match &r.payload {
                RecordPayload::BatchRef { batch, .. } => {
                    if seen.insert(Arc::as_ptr(batch) as usize) {
                        pinned += batch.get_array_memory_size() as u64;
                    }
                }
                RecordPayload::Owned(b) => owned_overhead += b.get_array_memory_size() as u64,
                RecordPayload::Delete => {}
            }
        }
        pinned + owned_overhead
    }

    /// **Detector #1 — the highest-value guard.** Build a map of N entries sharing
    /// M source batches with SPREAD keys, drop the source Vec so only the map pins
    /// the batches, and assert the map's tracked retained bytes equals the TRUE
    /// retained bytes (independently summed `get_array_memory_size` over distinct
    /// live Arcs). The PRE-A6 per-row accounting (`array_bytes / num_rows`) would
    /// fail this by a large factor; A6e is exact.
    #[test]
    fn detector_1_tracked_retained_bytes_matches_truth_for_shared_batches() {
        // Budget high enough that nothing spills/compacts — we are testing the
        // accounting invariant on the pure in-memory tier.
        let mut map = SpillableRecordMap::with_config(mem_only_config());

        const M: usize = 8; // distinct source batches
        const ROWS: usize = 1024; // rows per source batch
        const LIVE_PER_BATCH: usize = 4; // spread survivors (1 in 256 → very sparse)

        let mut sources: Vec<Arc<RecordBatch>> = Vec::new();
        for b in 0..M {
            let src = wide_batch(ROWS);
            // Insert LIVE_PER_BATCH spread rows from this batch under unique keys.
            for j in 0..LIVE_PER_BATCH {
                let row = j * (ROWS / LIVE_PER_BATCH);
                let key = format!("b{b}_r{row}");
                map.insert(
                    key.clone(),
                    BufferedRecord::new_batch_ref(key, src.clone(), row, None),
                )
                .unwrap();
            }
            sources.push(src);
        }
        assert_eq!(map.in_memory.len(), M * LIVE_PER_BATCH);
        assert_eq!(map.pinned_batch_count(), M, "all M batches pinned");

        // Drop our own strong refs so ONLY the map's BatchRefs pin the batches.
        drop(sources);

        let tracked = map.true_retained_bytes();
        let truth = true_retained_from_scratch(&map);
        assert_eq!(
            tracked, truth,
            "A6e accounting must equal the true retained heap (distinct pinned \
             Arcs + owned/overhead). tracked={tracked} truth={truth}"
        );

        // And it must reflect WHOLE batches, not a per-row share. The pre-A6 bug
        // would have charged only LIVE_PER_BATCH/ROWS of each batch.
        let per_row_undercount: u64 = (LIVE_PER_BATCH as u64) * M as u64; // ~rows charged
        assert!(
            map.true_retained_bytes() > per_row_undercount * 100,
            "true retained must be the whole-batch heap, far above the per-row share"
        );
    }

    /// Over a tight budget, a SPARSE pinned batch is COMPACTED (no spill IO): the
    /// tracked retained bytes drop sharply, every survivor still resolves to its
    /// exact data, and no entry spilled to disk.
    #[test]
    fn evict_compacts_sparse_pinned_batch_no_spill() {
        let src = wide_batch(2048);
        let one_batch_bytes = src.get_array_memory_size() as u64;
        // Budget below one full source batch → over budget once it is pinned, but
        // the survivors are sparse so compaction (not spill) reclaims it.
        let mut map = SpillableRecordMap::with_config(tiny_budget_config(one_batch_bytes / 2));

        // 8 spread survivors out of 2048 rows (0.4% live → sparse).
        let mut expected: Vec<(String, i32)> = Vec::new();
        for j in 0..8 {
            let row = j * 256;
            let key = format!("s{row}");
            map.insert(
                key.clone(),
                BufferedRecord::new_batch_ref(key.clone(), src.clone(), row, None),
            )
            .unwrap();
            expected.push(row_data(0, row));
        }
        drop(src);
        expected.sort();

        assert!(
            !map.spill_fired(),
            "sparse pins must be compacted, never spilled (no IO)"
        );
        assert!(
            map.true_retained_bytes() <= map.max_in_memory_size,
            "after compaction the tracked heap is back under budget"
        );
        // Accounting still matches truth post-compaction.
        assert_eq!(map.true_retained_bytes(), true_retained_from_scratch(&map));

        // Full-data identity across the repoint.
        let mut got: Vec<(String, i32)> = map.in_memory.values().map(tuple).collect();
        got.sort();
        assert_eq!(
            got, expected,
            "compaction must preserve every survivor's data"
        );
    }

    /// Over a tight budget, a DENSE pinned batch is SPILLED (frees the whole Arc):
    /// spill fires, the tracked heap is bounded, and the data round-trips exactly.
    #[test]
    fn evict_spills_dense_pinned_batch_and_bounds_memory() {
        let src = wide_batch(256);
        let one_batch_bytes = src.get_array_memory_size() as u64;
        // Two source batches will be pinned; budget ~ one of them. The first
        // (fully live → dense) must spill so the second fits.
        let mut map = SpillableRecordMap::with_config(tiny_budget_config(one_batch_bytes + 1024));

        let mut expected: Vec<(String, i32)> = Vec::new();
        // Batch 1: all 256 rows live (dense).
        for row in 0..256 {
            let key = format!("a{row}");
            map.insert(
                key.clone(),
                BufferedRecord::new_batch_ref(key, src.clone(), row, None),
            )
            .unwrap();
            expected.push(row_data(0, row));
        }
        // Batch 2: disjoint data (base 1000), all 256 rows live → forces the
        // budget over, evicting batch 1.
        let src2 = wide_batch_from(1000, 256);
        for row in 0..256 {
            let key = format!("c{row}");
            map.insert(
                key.clone(),
                BufferedRecord::new_batch_ref(key, src2.clone(), row, None),
            )
            .unwrap();
            expected.push(row_data(1000, row));
        }
        drop(src);
        drop(src2);
        expected.sort();

        assert!(map.spill_fired(), "dense over-budget batch must spill");
        assert!(
            map.true_retained_bytes() <= map.max_in_memory_size,
            "true retained heap must be bounded by the budget after eviction: {} > {}",
            map.true_retained_bytes(),
            map.max_in_memory_size
        );

        // Full-data equivalence across both tiers (drain mem-then-disk).
        let mut got: Vec<(String, i32)> = map.drain_iter().map(|r| tuple(&r.unwrap())).collect();
        got.sort();
        assert_eq!(
            got, expected,
            "spilled + in-memory entries together reproduce all data byte-exact"
        );
    }

    /// A config with a huge soft budget (spill never triggered by the budget) but
    /// an explicit HARD peak cap — isolates the [`CONFIG_MAX_PEAK_MEMORY`] path.
    fn capped_config(max_peak: u64) -> SpillConfig {
        SpillConfig {
            max_in_memory_size: u64::MAX,
            spill_path: std::env::temp_dir(),
            diskmap_type: DiskMapType::RocksDb,
            max_peak_in_memory_size: Some(max_peak),
        }
    }

    /// ENG-44437 — the hard peak cap is satisfied by SPILLING when it can be: a
    /// dense BatchRef that pushes the footprint over the cap is spilled to disk to
    /// get back under (NOT an error), and every row still round-trips. This is the
    /// positive branch complementing the loud-error case (which fires only when
    /// spilling cannot help — see the repro integration test).
    #[test]
    fn peak_cap_spills_to_stay_under_cap_without_error() {
        let src = wide_batch(256);
        let one_batch_bytes = src.get_array_memory_size() as u64;
        // Cap ~ one batch plus slack: the second (disjoint) batch pushes over the
        // cap, so eviction must spill one dense batch to get back under it.
        let mut map = SpillableRecordMap::with_config(capped_config(one_batch_bytes + 4096));

        let mut expected: Vec<(String, i32)> = Vec::new();
        for row in 0..256 {
            let key = format!("a{row}");
            map.insert(
                key.clone(),
                BufferedRecord::new_batch_ref(key, src.clone(), row, None),
            )
            .expect("under cap so far");
            expected.push(row_data(0, row));
        }
        let src2 = wide_batch_from(1000, 256);
        for row in 0..256 {
            let key = format!("c{row}");
            // These push over the cap; the map must SPILL (not error) to comply.
            map.insert(
                key.clone(),
                BufferedRecord::new_batch_ref(key, src2.clone(), row, None),
            )
            .expect("cap satisfied by spilling a dense batch, not an error");
            expected.push(row_data(1000, row));
        }
        drop(src);
        drop(src2);
        expected.sort();

        assert!(
            map.spill_fired(),
            "the cap must be met by spilling a dense batch to disk"
        );
        assert!(
            map.current_in_memory_bytes() <= one_batch_bytes + 4096,
            "the tracked footprint must be held at or below the hard cap: {} > {}",
            map.current_in_memory_bytes(),
            one_batch_bytes + 4096
        );

        let mut got: Vec<(String, i32)> = map.drain_iter().map(|r| tuple(&r.unwrap())).collect();
        got.sort();
        assert_eq!(
            got, expected,
            "spilling to satisfy the cap must preserve every row's data byte-exact"
        );
    }

    /// Output is identical across budget settings (unbounded / mid / tiny) for the
    /// same spread-key input — eviction order must not change the resolved data
    /// (the §4 correctness flag: a full-data equivalence across budgets).
    #[test]
    fn output_is_budget_invariant_across_unbounded_mid_tiny() {
        fn build(budget: u64) -> Vec<(String, i32)> {
            let mut map = SpillableRecordMap::with_config(tiny_budget_config(budget));
            // 4 disjoint source batches, spread survivors per batch.
            for b in 0..4 {
                let src = wide_batch_from(b * 10_000, 512);
                for j in 0..64 {
                    let row = j * 8;
                    let key = format!("b{b}_{row}");
                    map.insert(
                        key.clone(),
                        BufferedRecord::new_batch_ref(key, src.clone(), row, None),
                    )
                    .unwrap();
                }
            }
            let mut out: Vec<(String, i32)> =
                map.drain_iter().map(|r| tuple(&r.unwrap())).collect();
            out.sort();
            out
        }
        let unbounded = build(u64::MAX);
        let mid = build(wide_batch(512).get_array_memory_size() as u64);
        let tiny = build(4096);
        assert_eq!(unbounded.len(), 4 * 64);
        assert_eq!(
            unbounded, mid,
            "mid-budget eviction must not change the merged data"
        );
        assert_eq!(
            unbounded, tiny,
            "tiny-budget eviction must not change the merged data"
        );
    }
}
