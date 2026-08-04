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

//! Spill-budget config propagation (ENG-45062 / I-33 / G-16).
//!
//! `SpillConfig::from_config` derives the in-memory spill threshold from the
//! reader config map only (`reader_context.hoodie_reader_config`, read at
//! `crates/core/src/file_group/reader/buffer/key_based.rs`). The gluten adapter
//! forwards `hoodie.memory.merge.max.size` (the computed `maxMemoryPerCompaction`)
//! into that map, so the operator/computed budget reaches spill sizing.
//!
//! Regression guard for I-33: gluten previously put the budget only in the props
//! map, which hudi-rs never sees, so `SpillConfig` fell back to the 1 GiB default
//! on every read (in-memory threshold pinned at ~779 MiB regardless of the
//! operator's setting -> OOM risk). This asserts `from_config` honors the budget
//! when it is present (as gluten now forwards it) and only defaults when it is
//! genuinely absent.
//!
//! ## Peak-memory hard cap (ENG-44436 / 44437)
//!
//! The second group of tests here covers the hudi-rs-side foundation for the
//! velox memory-reservation work: a queryable current-footprint getter
//! ([`SpillableRecordMap::current_in_memory_bytes`]) and a configurable HARD
//! peak cap ([`CONFIG_MAX_PEAK_MEMORY`]) that fails loudly with
//! [`CoreError::MemoryLimitExceeded`] instead of letting the executor OOM. These
//! are cargo-only (no gluten/velox bundle); the FFI + velox reservation wiring
//! is a later increment.
//!
//! Run: `cargo test -p hudi-core --test nonfunctional_gaps_repro -- --nocapture`

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::CoreError;
use crate::file_group::reader_v2::buffer::spillable_map::{
    CONFIG_MAX_PEAK_MEMORY, CONFIG_MERGE_MAX_SIZE, DEFAULT_MERGE_MAX_SIZE_BYTES, DiskMapType,
    ENTRY_OVERHEAD_BYTES, SpillConfig, SpillableRecordMap,
};
use crate::file_group::reader_v2::buffered_record::{BufferedRecord, OrderingValue};
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

/// The merge-type key gluten forwards in `hoodieReaderConfig`
/// (`HoodieReaderConfig.MERGE_TYPE.key()`).
const MERGE_TYPE_KEY: &str = "hoodie.datasource.merge.type";
/// Default merge type gluten resolves when unset (`REALTIME_PAYLOAD_COMBINE`).
const MERGE_TYPE_PAYLOAD_COMBINE: &str = "payload_combine";

const MIB: u64 = 1024 * 1024;

/// Threshold `SpillConfig` derives from a given `hoodie_reader_config` map.
fn threshold_bytes(config: &HashMap<String, String>) -> u64 {
    SpillConfig::from_config(config)
        .expect("SpillConfig::from_config")
        .max_in_memory_size
}

/// I-33 / G-16 — with the gluten fix, `hoodie.memory.merge.max.size` is forwarded
/// in `hoodie_reader_config`, so `SpillConfig` honors the operator/computed budget
/// instead of collapsing to the 1 GiB default.
#[test]
fn i33_merge_budget_honored_when_forwarded() {
    // The gluten-realistic reader config POST-FIX: MERGE_TYPE plus the forwarded
    // budget (operator lowers merge memory to 64 MiB to bound executor RSS).
    let gluten_reader_config: HashMap<String, String> = HashMap::from([
        (
            MERGE_TYPE_KEY.to_string(),
            MERGE_TYPE_PAYLOAD_COMBINE.to_string(),
        ),
        (CONFIG_MERGE_MAX_SIZE.to_string(), (64 * MIB).to_string()),
    ]);
    let honored = threshold_bytes(&gluten_reader_config);
    println!(
        "[I-33] gluten hoodie_reader_config (budget forwarded, 64 MiB) -> \
         max_in_memory_size = {honored} bytes ({} MiB)",
        honored / MIB
    );

    // A map WITHOUT the budget key (pre-fix gluten map / genuinely unset) still
    // falls back to the 1 GiB default — this is what the fix avoids.
    let no_budget: HashMap<String, String> = HashMap::from([(
        MERGE_TYPE_KEY.to_string(),
        MERGE_TYPE_PAYLOAD_COMBINE.to_string(),
    )]);
    let default_path = threshold_bytes(&no_budget);
    println!(
        "[I-33] no budget key -> max_in_memory_size = {default_path} bytes ({} MiB); \
         DEFAULT_MERGE_MAX_SIZE_BYTES = {} MiB",
        default_path / MIB,
        DEFAULT_MERGE_MAX_SIZE_BYTES / MIB
    );

    // The forwarded 64 MiB budget is honored: 0.8*64 MiB - 40 MiB RocksDB reserve.
    assert_eq!(
        honored, 11_744_051,
        "SpillConfig must honor the forwarded 64 MiB budget (0.8x64MiB - RocksDB reserve)"
    );
    // ...and it is NOT the unset 1 GiB default (0.8*1GiB - 40 MiB = 779 MiB).
    assert_eq!(
        default_path, 817_050_419,
        "an absent budget key must fall back to the 0.8x1GiB - reserve default"
    );
    assert!(
        honored < default_path,
        "the forwarded budget must lower the threshold below the default; \
         if these were equal the budget would be silently ignored (the I-33 gap)"
    );
    println!(
        "[I-33] FIX OK: forwarded budget honored ({} MiB) != unset default ({} MiB); \
         the operator's merge-memory budget reaches SpillConfig.",
        honored / MIB,
        default_path / MIB
    );
}

// ── ENG-44436 / 44437: current-footprint getter + hard peak cap ──────────────

/// The 2-column `(key: Utf8, v: Int32)` fixture schema for owned records.
fn owned_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]))
}

/// A single-row OWNED record and the array bytes its payload contributes.
/// Owned (not `BatchRef`) so the map has no shared source batch to evict — the
/// footprint can only be reduced by spilling the entry itself, exercising the
/// "cannot be reduced by spilling" hard-cap branch.
fn owned_record(key: &str, v: i32) -> (BufferedRecord, u64) {
    let batch = RecordBatch::try_new(
        owned_schema(),
        vec![
            Arc::new(StringArray::from(vec![key])) as _,
            Arc::new(Int32Array::from(vec![v])) as _,
        ],
    )
    .expect("build single-row batch");
    let owned_bytes = batch.get_array_memory_size() as u64;
    (
        BufferedRecord::new_data(key.to_string(), batch, Some(OrderingValue::Long(v as i64))),
        owned_bytes,
    )
}

/// True-retained bytes a single owned entry contributes:
/// `key.len() + ENTRY_OVERHEAD_BYTES + owned array bytes`.
fn owned_entry_bytes(key: &str, owned_bytes: u64) -> u64 {
    key.len() as u64 + ENTRY_OVERHEAD_BYTES + owned_bytes
}

/// A [`SpillConfig`] with an explicit soft budget and optional hard peak cap,
/// spilling to the OS temp dir.
fn spill_config(max_in_memory_size: u64, max_peak: Option<u64>) -> SpillConfig {
    SpillConfig {
        max_in_memory_size,
        spill_path: std::env::temp_dir(),
        diskmap_type: DiskMapType::RocksDb,
        max_peak_in_memory_size: max_peak,
    }
}

/// ENG-44436 — the current-footprint getter reflects the map's live in-memory
/// bytes. With a budget that keeps everything resident, the getter must equal
/// the summed per-entry footprint (key + overhead + owned array bytes) exactly.
#[test]
fn oom_current_footprint_getter_reflects_in_memory_bytes() {
    let mut map = SpillableRecordMap::with_config(spill_config(u64::MAX, None));
    assert_eq!(
        map.current_in_memory_bytes(),
        0,
        "an empty map reports a zero footprint"
    );

    let mut expected = 0u64;
    for i in 0..10 {
        let key = format!("k{i}");
        let (record, owned_bytes) = owned_record(&key, i);
        expected += owned_entry_bytes(&key, owned_bytes);
        map.insert(key, record).unwrap();
    }
    assert_eq!(map.len(), 10);
    assert_eq!(
        map.current_in_memory_bytes(),
        expected,
        "getter must equal the summed per-entry footprint (key + overhead + owned array bytes)"
    );
}

/// ENG-44437 — with the hard peak cap set below a single entry's footprint, an
/// over-cap OWNED insertion (which spilling cannot relieve) fails loudly with
/// the specific `CoreError::MemoryLimitExceeded`, naming the cap and its config
/// key — instead of silently growing toward an executor OOM.
#[test]
fn oom_peak_cap_rejects_oversized_insertion_loudly() {
    // Probe one entry's footprint, then set the cap one byte below it so even a
    // single insertion exceeds the cap and cannot be reduced.
    let (_probe, owned_bytes) = owned_record("k0", 0);
    let one_entry = owned_entry_bytes("k0", owned_bytes);
    let cap = one_entry - 1;

    // Budget huge (the soft spill trigger never fires) — only the hard cap acts.
    let mut map = SpillableRecordMap::with_config(spill_config(u64::MAX, Some(cap)));
    let (record, _) = owned_record("k0", 0);
    let err = map
        .insert("k0".to_string(), record)
        .expect_err("over-cap insertion must fail");

    assert!(
        matches!(err, CoreError::MemoryLimitExceeded(_)),
        "over-cap insertion must return MemoryLimitExceeded, got {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains(CONFIG_MAX_PEAK_MEMORY),
        "the loud error must name the config key ({CONFIG_MAX_PEAK_MEMORY}): {msg}"
    );
    assert!(
        msg.contains("hard cap"),
        "the loud error must explain it is a hard cap: {msg}"
    );
}

/// ENG-44437 — with NO cap set (the default), inserts that would blow past any
/// cap-test threshold still succeed: behavior is unchanged and no error is ever
/// raised on memory growth.
#[test]
fn oom_no_cap_preserves_unchanged_behavior() {
    let mut map = SpillableRecordMap::with_config(spill_config(u64::MAX, None));
    for i in 0..100 {
        let key = format!("k{i}");
        let (record, _) = owned_record(&key, i);
        assert!(
            map.insert(key, record).is_ok(),
            "with no cap set, inserts never fail on memory growth"
        );
    }
    assert_eq!(map.len(), 100, "all inserts land with no cap");
    assert!(
        map.current_in_memory_bytes() > 0,
        "footprint still tracked even without a cap"
    );
}

/// ENG-44437 — `SpillConfig::from_config` parses the hard-cap key: absent →
/// `None` (unchanged behavior), present → the parsed byte count, invalid → a
/// typed `CoreError::InvalidValue`.
#[test]
fn oom_peak_cap_config_parses_and_defaults_to_none() {
    let unset = SpillConfig::from_config(&HashMap::new()).unwrap();
    assert_eq!(
        unset.max_peak_in_memory_size, None,
        "an absent hard-cap key must leave the cap unset (no behavior change)"
    );

    let with_cap = HashMap::from([(CONFIG_MAX_PEAK_MEMORY.to_string(), (256 * MIB).to_string())]);
    let parsed = SpillConfig::from_config(&with_cap).unwrap();
    assert_eq!(
        parsed.max_peak_in_memory_size,
        Some(256 * MIB),
        "a present hard-cap key must be parsed into the cap"
    );

    let bad = HashMap::from([(
        CONFIG_MAX_PEAK_MEMORY.to_string(),
        "not-a-number".to_string(),
    )]);
    assert!(
        matches!(
            SpillConfig::from_config(&bad),
            Err(CoreError::InvalidValue(_))
        ),
        "an invalid hard-cap value must be rejected with a typed InvalidValue error"
    );
}
