# keyFilterOpt e2e coverage expansion — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 8 new e2e tests across 3 fixtures so each KeySpec variant, each filter call site, and the no-op corner case all have explicit AB-pattern validation.

**Architecture:** All tests live in the existing `crates/core/tests/file_group_reader_tests.rs` (append-only). Each test calls a shared `ab_read_with_filter` driver that reads twice (no-filter baseline + filtered) and validates via `FilterAbResult` assertion methods. Schema-specific row extractors handle the three different fixture schemas.

**Tech Stack:** Rust 2024, Tokio, Arrow `RecordBatch`, `hudi_test::QuickstartTripsTable`.

**Spec reference:** `docs/superpowers/specs/2026-05-07-keyfilteropt-e2e-coverage-design.md`

**Working from:** `/home/ubuntu/ws3/hudi-rs` on branch `davis/phase3migrateFilterOptCode`.

**Key fixture facts (verified before plan):**
- `V9Mor8I4UCommitTime` city=sf base file: `fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet`; log: `.fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73`. Schema: `id INT, name STRING, age INT, ts STRING, city STRING`.
- `V9MorNonpart3Commits` base file: `960a29a0-0f78-401d-85b1-1cbc44b34121-0_0-846-1597_20260409002001492.parquet`; logs: `.960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002002957.log.1_0-868-1644` (delete block), `.960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002003963.log.1_0-890-1691` (update block). Schema: `id INT, name STRING, price DOUBLE, ts LONG`. Baseline merged content: 4 rows — `(3,"C",30.0)`, `(4,"D2",45.0)`, `(5,"E2",55.0)`, `(6,"F2",65.0)`. Ids 0/1/2 are deleted by log; ids 4/5/6 are updated to D2/E2/F2.
- `MorLayoutLogOnly` (no base file) logs: `.7787bafe-f674-4382-85f7-a94177194136-0_20260409030525348.log.1_0-102-176`, `.7787bafe-f674-4382-85f7-a94177194136-0_20260409030527298.log.1_0-116-202`, `.7787bafe-f674-4382-85f7-a94177194136-0_20260409030528554.log.1_0-130-231`. Schema uses a `key` column (string), not `id`.

---

## File structure

Single file modified throughout (append only):

```
crates/core/tests/file_group_reader_tests.rs
└── (NEW append-only sections, in order)
    ├── FilterAbResult struct + impl                    ← Task A.1
    ├── ab_read_with_filter driver                      ← Task A.1
    ├── 3 fixture locators                              ← Task A.1
    │     sf_file_group, nonpart_3commits_file_group, log_only_file_group
    ├── 2 schema-specific extractors                    ← Tasks C.1 / D.1
    │     extract_row_with_id_opt_v9nonpart, extract_row_with_id_opt_log_only
    ├── Test 1: fg_filter_in_log_updated_key            ← Task A.1 (renamed from existing fg_reader_with_key_filter_filters_rows; refactored to use AB helper)
    ├── Test 2: fg_filter_in_base_only_key              ← Task B.1
    ├── Test 3: fg_filter_in_no_match                   ← Task B.2
    ├── Test 4: fg_filter_starts_with_any_prefix        ← Task B.3
    ├── Test 5: fg_filter_unsupported_predicate_is_noop ← Task B.4
    ├── Test 6: fg_filter_in_with_delete_block          ← Task C.1
    ├── Test 7: fg_filter_in_log_updated_key_nonpart    ← Task C.2
    └── Test 8: fg_filter_in_log_only_filegroup         ← Task D.1
```

---

## Phase A — Shared helpers + refactor existing test

### Task A.1: Add FilterAbResult, ab_read_with_filter, fixture locators; refactor existing test

**Files:**
- Modify: `crates/core/tests/file_group_reader_tests.rs` (append helpers + locators; rename and refactor the existing keyFilterOpt test)

This task replaces the existing `fg_reader_with_key_filter_filters_rows` test with `fg_filter_in_log_updated_key` that uses the new `FilterAbResult` helper. The behavior tested is identical; only the structure changes.

- [ ] **Step 1: Append helpers + locators** at the bottom of `crates/core/tests/file_group_reader_tests.rs`.

```rust
// =============================================================================
// keyFilterOpt e2e coverage — shared helpers (FilterAbResult + ab_read_with_filter)
//
// Each test in this section ABs a no-filter read against a filtered read of
// the same file group, then asserts via FilterAbResult that the filter is
// real (or, for unsupported predicates, that it's a no-op).
// =============================================================================

/// Result of an AB read: same file group read once with no filter and once
/// with `Some(filter)`.
struct FilterAbResult {
    baseline: arrow_array::RecordBatch,
    filtered: arrow_array::RecordBatch,
}

impl FilterAbResult {
    /// Assert filter is real, not a no-op: filtered must have STRICTLY fewer
    /// rows than baseline.
    fn assert_filter_narrowed(&self) {
        assert!(
            self.filtered.num_rows() < self.baseline.num_rows(),
            "filter did not narrow rows: baseline={}, filtered={} \
             (this means the filter was a no-op — bug)",
            self.baseline.num_rows(),
            self.filtered.num_rows(),
        );
    }

    /// Assert filter is a no-op: filtered.num_rows == baseline.num_rows AND
    /// every row in filtered also appears in baseline (compared by sorted
    /// id-only since row order may differ between two reads).
    fn assert_filter_was_noop(&self) {
        assert_eq!(
            self.filtered.num_rows(),
            self.baseline.num_rows(),
            "expected filter to be no-op (predicate not In/StringStartsWithAny), \
             but row counts differ: baseline={}, filtered={}",
            self.baseline.num_rows(),
            self.filtered.num_rows(),
        );
        let baseline_ids: std::collections::BTreeSet<i32> = ids_in_batch(&self.baseline);
        let filtered_ids: std::collections::BTreeSet<i32> = ids_in_batch(&self.filtered);
        assert_eq!(
            baseline_ids, filtered_ids,
            "expected same id set after no-op filter"
        );
    }

    /// Assert that filtered.num_rows == 0 AND baseline has rows (filter
    /// narrowed everything away).
    fn assert_filtered_empty(&self) {
        assert!(
            self.baseline.num_rows() > 0,
            "baseline must have rows for the empty-filter assertion to be meaningful"
        );
        assert_eq!(
            self.filtered.num_rows(),
            0,
            "expected filter to drop all rows, got {}",
            self.filtered.num_rows()
        );
    }

    /// Assert the exact set of `id`s present in filtered (sorted).
    fn assert_filtered_ids_eq(&self, expected: &[i32]) {
        let actual: Vec<i32> = ids_in_batch(&self.filtered).into_iter().collect();
        let mut expected_sorted = expected.to_vec();
        expected_sorted.sort();
        assert_eq!(actual, expected_sorted, "filtered id set mismatch");
    }
}

/// Helper: collect `id` column values from a RecordBatch as a sorted set.
fn ids_in_batch(batch: &arrow_array::RecordBatch) -> std::collections::BTreeSet<i32> {
    let id_col = batch
        .column_by_name("id")
        .expect("id column missing from batch")
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .expect("id column should be Int32");
    (0..batch.num_rows()).map(|i| id_col.value(i)).collect()
}

/// Shared driver: read once with None, once with Some(filter), return both.
async fn ab_read_with_filter(
    table_path: &str,
    partition: &str,
    base_file: &str,
    log_files: Vec<&str>,
    filter: StdArc<dyn Predicate>,
) -> Result<FilterAbResult> {
    let baseline = read_file_group_with_key_filter(
        table_path,
        partition,
        base_file,
        log_files.clone(),
        None,
    )
    .await?;
    let filtered = read_file_group_with_key_filter(
        table_path,
        partition,
        base_file,
        log_files,
        Some(filter),
    )
    .await?;
    Ok(FilterAbResult { baseline, filtered })
}

// =============================================================================
// Fixture locators
// =============================================================================

/// Fixture locator: V9Mor8I4UCommitTime city=sf file group.
/// Same filenames as test_e2e_v9_mor_commit_time_sf_merge.
fn sf_file_group() -> (String, &'static str, &'static str, Vec<&'static str>) {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();
    (
        table_path,
        "city=sf",
        "fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet",
        vec![".fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"],
    )
}

/// Fixture locator: V9MorNonpart3Commits — non-partitioned, single file group,
/// 1 base + 2 log files (delete block + update block).
/// Schema: id INT, name STRING, price DOUBLE, ts LONG.
fn nonpart_3commits_file_group() -> (String, &'static str, &'static str, Vec<&'static str>) {
    let table_path = QuickstartTripsTable::V9MorNonpart3Commits.path_to_mor_avro();
    (
        table_path,
        "",
        "960a29a0-0f78-401d-85b1-1cbc44b34121-0_0-846-1597_20260409002001492.parquet",
        vec![
            ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002002957.log.1_0-868-1644",
            ".960a29a0-0f78-401d-85b1-1cbc44b34121-0_20260409002003963.log.1_0-890-1691",
        ],
    )
}

/// Fixture locator: MorLayoutLogOnly — no base file, 3 log files.
fn log_only_file_group() -> (String, &'static str, &'static str, Vec<&'static str>) {
    let table_path = QuickstartTripsTable::MorLayoutLogOnly.path_to_mor_avro();
    (
        table_path,
        "",
        "",
        vec![
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030525348.log.1_0-102-176",
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030527298.log.1_0-116-202",
            ".7787bafe-f674-4382-85f7-a94177194136-0_20260409030528554.log.1_0-130-231",
        ],
    )
}
```

- [ ] **Step 2: Replace the existing test** `fg_reader_with_key_filter_filters_rows`.

Find the existing test (around the section labeled "Phase 3 e2e smoke test — keyFilterOpt actually filters rows"). Replace its body with the AB-helper version below. Keep the `#[tokio::test]` attribute; rename the function to `fg_filter_in_log_updated_key`.

```rust
/// Test 1: filter on a key updated in log → 1 row, value from log.
///
/// Validates: filter applies during BOTH log scan and base read. id=1 is
/// updated in log → filtered output should have the log value (Alice-V2).
#[tokio::test]
async fn fg_filter_in_log_updated_key() -> Result<()> {
    let (table_path, partition, base_file, log_files) = sf_file_group();

    // First read no-filter to discover id=1's record key dynamically.
    let baseline_for_key_lookup = read_file_group_with_key_filter(
        &table_path, partition, base_file, log_files.clone(), None,
    ).await?;
    let key_for_id1 = lookup_record_key(&baseline_for_key_lookup, 1);

    let filter: StdArc<dyn Predicate> = StdArc::new(predicates_factory::in_(
        Box::new(NameReference::new("_hoodie_record_key")),
        vec![Box::new(Literal::string(key_for_id1)) as Box<dyn Expression>],
    ));

    let ab = ab_read_with_filter(&table_path, partition, base_file, log_files, filter).await?;

    ab.assert_filter_narrowed();         // 1 < 2
    ab.assert_filtered_ids_eq(&[1]);     // exactly id=1

    // Cross-validate: filtered id=1 row equals baseline id=1 row.
    let expected = extract_row_with_id_opt(&ab.baseline, 1).expect("id=1 in baseline");
    let actual = extract_row_with_id_opt(&ab.filtered, 1).expect("id=1 in filtered");
    assert_eq!(expected, actual, "filtered id=1 must equal baseline id=1 (Alice-V2)");

    Ok(())
}
```

- [ ] **Step 3: Run the renamed test + the two existing keyFilterOpt-related references**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --test file_group_reader_tests fg_filter_in_log_updated_key 2>&1 | tail -10`
Expected: 1 test passes.

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --test file_group_reader_tests 2>&1 | tail -10`
Expected: all integration tests pass; the renamed test runs and passes; no compile errors.

- [ ] **Step 4: Commit**

```bash
cd /home/ubuntu/ws3/hudi-rs && git add crates/core/tests/file_group_reader_tests.rs && git commit -m "$(cat <<'EOF'
add FilterAbResult helper + refactor keyFilterOpt e2e test 1

Adds:
- FilterAbResult struct + 4 assertion methods (narrowed/noop/empty/ids_eq)
- ab_read_with_filter shared driver
- 3 fixture locators: sf_file_group, nonpart_3commits_file_group, log_only_file_group
- ids_in_batch helper

Renames fg_reader_with_key_filter_filters_rows → fg_filter_in_log_updated_key,
refactored to use the AB helper. Behavior unchanged.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase B — Tests 2-5 on V9Mor8I4UCommitTime

### Task B.1: Test 2 — `fg_filter_in_base_only_key`

**Files:**
- Modify: `crates/core/tests/file_group_reader_tests.rs` (append one test)

Filter on `id=2`'s key. id=2 has no log update for `city=sf` (only id=1 is updated). The log scan finds no records matching this key, so the base-file filter is the path doing the work. Validates the base-file filter (`apply_key_filter_to_batch`) in isolation.

- [ ] **Step 1: Append the test** at the bottom of the file.

```rust
/// Test 2: filter on a key that exists ONLY in the base file (no log update).
///
/// Validates: ISOLATES the base-file filter (apply_key_filter_to_batch).
/// id=2 in city=sf has no log update — log scan finds no records for it,
/// only the base-file filter narrows the output.
#[tokio::test]
async fn fg_filter_in_base_only_key() -> Result<()> {
    let (table_path, partition, base_file, log_files) = sf_file_group();

    let baseline_for_key_lookup = read_file_group_with_key_filter(
        &table_path, partition, base_file, log_files.clone(), None,
    ).await?;
    let key_for_id2 = lookup_record_key(&baseline_for_key_lookup, 2);

    let filter: StdArc<dyn Predicate> = StdArc::new(predicates_factory::in_(
        Box::new(NameReference::new("_hoodie_record_key")),
        vec![Box::new(Literal::string(key_for_id2)) as Box<dyn Expression>],
    ));

    let ab = ab_read_with_filter(&table_path, partition, base_file, log_files, filter).await?;

    ab.assert_filter_narrowed();         // 1 < 2
    ab.assert_filtered_ids_eq(&[2]);     // exactly id=2

    // Cross-validate: filtered id=2 row equals baseline id=2 row (Bob, base value).
    let expected = extract_row_with_id_opt(&ab.baseline, 2).expect("id=2 in baseline");
    let actual = extract_row_with_id_opt(&ab.filtered, 2).expect("id=2 in filtered");
    assert_eq!(expected, actual, "filtered id=2 must equal baseline id=2 (Bob)");

    Ok(())
}
```

- [ ] **Step 2: Run the test**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --test file_group_reader_tests fg_filter_in_base_only_key 2>&1 | tail -10`
Expected: 1 test passes.

- [ ] **Step 3: Commit**

```bash
cd /home/ubuntu/ws3/hudi-rs && git add crates/core/tests/file_group_reader_tests.rs && git commit -m "$(cat <<'EOF'
add e2e test fg_filter_in_base_only_key (isolates base-file filter)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task B.2: Test 3 — `fg_filter_in_no_match`

Filter on a record key that doesn't exist in the data. Validates the filter actually drops everything.

**Files:**
- Modify: `crates/core/tests/file_group_reader_tests.rs` (append one test)

- [ ] **Step 1: Append the test**

```rust
/// Test 3: filter on a non-existent key → 0 rows.
///
/// Validates: filter actually drops everything when nothing matches.
/// Catches a bug where filter is silently a no-op.
#[tokio::test]
async fn fg_filter_in_no_match() -> Result<()> {
    let (table_path, partition, base_file, log_files) = sf_file_group();

    let filter: StdArc<dyn Predicate> = StdArc::new(predicates_factory::in_(
        Box::new(NameReference::new("_hoodie_record_key")),
        vec![Box::new(Literal::string("definitely-not-a-real-record-key")) as Box<dyn Expression>],
    ));

    let ab = ab_read_with_filter(&table_path, partition, base_file, log_files, filter).await?;

    ab.assert_filtered_empty();          // baseline > 0, filtered == 0

    Ok(())
}
```

- [ ] **Step 2: Run + commit**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --test file_group_reader_tests fg_filter_in_no_match 2>&1 | tail -10`
Expected: 1 test passes.

```bash
cd /home/ubuntu/ws3/hudi-rs && git add crates/core/tests/file_group_reader_tests.rs && git commit -m "$(cat <<'EOF'
add e2e test fg_filter_in_no_match (filter drops everything)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task B.3: Test 4 — `fg_filter_starts_with_any_prefix`

Filter on a prefix that matches id=1's record key but not id=2's. Validates the `PrefixKeys` KeySpec path (currently 0 e2e coverage).

**Files:**
- Modify: `crates/core/tests/file_group_reader_tests.rs` (append one test)

This test derives a useful prefix from the actual id=1 key. Most realistic case: if the keys look like `"id:1"` and `"id:2"`, the prefix `"id:1"` matches exactly id=1. If the keys are simply `"1"` and `"2"`, the prefix `"1"` matches id=1 but not id=2. Either way, taking the **full key string** of id=1 as the prefix is a safe choice — a prefix that equals one specific key matches exactly that key.

- [ ] **Step 1: Append the test**

```rust
/// Test 4: filter via StringStartsWithAny → PrefixKeys path.
///
/// Validates: exercises the PrefixKeys KeySpec branch of create_key_spec
/// (Predicates::StringStartsWithAny → KeySpec::PrefixKeys). Uses id=1's
/// full record-key string as the prefix; this yields a single-row match
/// (a prefix that equals one key matches exactly that key).
#[tokio::test]
async fn fg_filter_starts_with_any_prefix() -> Result<()> {
    let (table_path, partition, base_file, log_files) = sf_file_group();

    let baseline_for_key_lookup = read_file_group_with_key_filter(
        &table_path, partition, base_file, log_files.clone(), None,
    ).await?;
    let prefix_for_id1 = lookup_record_key(&baseline_for_key_lookup, 1);

    let filter: StdArc<dyn Predicate> = StdArc::new(predicates_factory::starts_with_any(
        Box::new(NameReference::new("_hoodie_record_key")),
        vec![Box::new(Literal::string(prefix_for_id1)) as Box<dyn Expression>],
    ));

    let ab = ab_read_with_filter(&table_path, partition, base_file, log_files, filter).await?;

    ab.assert_filter_narrowed();         // 1 < 2
    ab.assert_filtered_ids_eq(&[1]);

    Ok(())
}
```

- [ ] **Step 2: Run + commit**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --test file_group_reader_tests fg_filter_starts_with_any_prefix 2>&1 | tail -10`
Expected: 1 test passes.

```bash
cd /home/ubuntu/ws3/hudi-rs && git add crates/core/tests/file_group_reader_tests.rs && git commit -m "$(cat <<'EOF'
add e2e test fg_filter_starts_with_any_prefix (PrefixKeys path)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task B.4: Test 5 — `fg_filter_unsupported_predicate_is_noop`

Filter with `Predicates::eq` (BinaryComparison). Since `create_key_spec` only matches `In` and `StringStartsWithAny`, this falls through to `None` and the filter must be a no-op (no rows dropped).

**Files:**
- Modify: `crates/core/tests/file_group_reader_tests.rs` (append one test)

- [ ] **Step 1: Append the test**

```rust
/// Test 5: unsupported predicate (eq) → filter is a no-op.
///
/// Validates: regression guard. create_key_spec only handles In and
/// StringStartsWithAny — anything else returns None, which means the
/// filter must NOT drop rows. This catches a bug where an unsupported
/// predicate accidentally drops rows.
#[tokio::test]
async fn fg_filter_unsupported_predicate_is_noop() -> Result<()> {
    let (table_path, partition, base_file, log_files) = sf_file_group();

    // BinaryComparison (eq) is not In/StringStartsWithAny → create_key_spec returns None.
    let filter: StdArc<dyn Predicate> = StdArc::new(predicates_factory::eq(
        Box::new(NameReference::new("_hoodie_record_key")),
        Box::new(Literal::string("anything")),
    ));

    let ab = ab_read_with_filter(&table_path, partition, base_file, log_files, filter).await?;

    ab.assert_filter_was_noop();         // baseline.num_rows == filtered.num_rows; same id set

    Ok(())
}
```

- [ ] **Step 2: Run + commit**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --test file_group_reader_tests fg_filter_unsupported_predicate_is_noop 2>&1 | tail -10`
Expected: 1 test passes.

```bash
cd /home/ubuntu/ws3/hudi-rs && git add crates/core/tests/file_group_reader_tests.rs && git commit -m "$(cat <<'EOF'
add e2e test fg_filter_unsupported_predicate_is_noop (regression guard)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase C — Tests 6-7 on V9MorNonpart3Commits

This fixture has a **different schema**: `id INT, name STRING, price DOUBLE, ts LONG` (no `age`, no `city`). The existing `extract_row_with_id_opt` returns `(id, name, age)` — wrong shape here. Add a fixture-specific extractor.

The fixture has 7 base rows (ids 0-6); log 1 deletes ids 0/1/2; log 2 updates ids 4/5/6. Baseline merged = 4 rows: `(3,C,30.0)`, `(4,D2,45.0)`, `(5,E2,55.0)`, `(6,F2,65.0)`.

### Task C.1: Add fixture-specific extractor + Test 6 (`fg_filter_in_with_delete_block`)

Filter on id=0's record key. id=0 is deleted by the log delete block, so it's not in the baseline. We need to look up id=0's record key BEFORE the log merge — read the base file alone (no log files) to get an unmerged view that still contains id=0.

**Files:**
- Modify: `crates/core/tests/file_group_reader_tests.rs` (append extractor + test)

- [ ] **Step 1: Append the extractor**

```rust
/// Schema-specific extractor for V9MorNonpart3Commits.
/// Schema: id INT, name STRING, price DOUBLE, ts LONG.
fn extract_row_with_id_opt_v9nonpart(
    batch: &arrow_array::RecordBatch,
    id: i32,
) -> Option<(i32, String, f64, i64)> {
    let id_col = batch.column_by_name("id")?
        .as_any().downcast_ref::<arrow_array::Int32Array>()?;
    let name_col = batch.column_by_name("name")?
        .as_any().downcast_ref::<arrow_array::StringArray>()?;
    let price_col = batch.column_by_name("price")?
        .as_any().downcast_ref::<arrow_array::Float64Array>()?;
    let ts_col = batch.column_by_name("ts")?
        .as_any().downcast_ref::<arrow_array::Int64Array>()?;

    for i in 0..batch.num_rows() {
        if id_col.value(i) == id {
            return Some((
                id_col.value(i),
                name_col.value(i).to_string(),
                price_col.value(i),
                ts_col.value(i),
            ));
        }
    }
    None
}
```

- [ ] **Step 2: Append Test 6**

```rust
/// Test 6: filter on a deleted-by-log key → 0 rows in filtered output.
///
/// Validates: filter passes through process_delete_block correctly.
/// V9MorNonpart3Commits: id=0 is deleted by log block. To look up id=0's
/// record key, read the base file alone (no logs) — that unmerged view
/// still contains id=0.
#[tokio::test]
async fn fg_filter_in_with_delete_block() -> Result<()> {
    let (table_path, partition, base_file, log_files) = nonpart_3commits_file_group();

    // Read base-only (no log files) to recover id=0's record key.
    let base_only = read_file_group_with_key_filter(
        &table_path, partition, base_file, vec![], None,
    ).await?;
    let key_for_id0 = lookup_record_key(&base_only, 0);

    let filter: StdArc<dyn Predicate> = StdArc::new(predicates_factory::in_(
        Box::new(NameReference::new("_hoodie_record_key")),
        vec![Box::new(Literal::string(key_for_id0)) as Box<dyn Expression>],
    ));

    let ab = ab_read_with_filter(&table_path, partition, base_file, log_files, filter).await?;

    // Baseline already excludes id=0 (deleted by log).
    assert!(extract_row_with_id_opt_v9nonpart(&ab.baseline, 0).is_none(),
        "baseline must NOT contain id=0 (deleted by log)");

    // Filtered must also exclude id=0 (filter applied to deleted key → 0 rows).
    assert!(extract_row_with_id_opt_v9nonpart(&ab.filtered, 0).is_none(),
        "filtered must NOT contain id=0");

    // Stronger: filtered should be empty since the only key in the filter is id=0,
    // which is deleted, so no rows survive the filter.
    ab.assert_filtered_empty();

    Ok(())
}
```

- [ ] **Step 3: Run + commit**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --test file_group_reader_tests fg_filter_in_with_delete_block 2>&1 | tail -15`
Expected: 1 test passes.

If FAILS with "id=0 not found in baseline batch" inside `lookup_record_key` — confirm that `read_file_group_with_key_filter(..., vec![], None)` returns the base file unmerged. If `lookup_record_key`'s implementation panics because `_hoodie_record_key` column is missing, replace `key_for_id0 = lookup_record_key(&base_only, 0)` with the literal key value `"0"` (the SimpleKeyGenerator default for an `id` field is the integer-as-string). Re-run and re-validate.

If FAILS with `assert_filtered_empty` — investigate: maybe id=0 wasn't actually deleted, or the filter didn't apply. Inspect `ab.baseline` and `ab.filtered` row content via `dbg!`.

```bash
cd /home/ubuntu/ws3/hudi-rs && git add crates/core/tests/file_group_reader_tests.rs && git commit -m "$(cat <<'EOF'
add e2e test fg_filter_in_with_delete_block (delete-block path) +
extract_row_with_id_opt_v9nonpart helper

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task C.2: Test 7 — `fg_filter_in_log_updated_key_nonpart`

Filter on id=5 (updated in log). Baseline = 4 rows. Filtered = 1 row, value updated to E2.

**Files:**
- Modify: `crates/core/tests/file_group_reader_tests.rs` (append one test)

- [ ] **Step 1: Append the test**

```rust
/// Test 7: filter on a log-updated key on the non-partitioned fixture.
///
/// Validates: same as Test 1 but on V9MorNonpart3Commits (non-partitioned,
/// 2 log files). id=5 is updated by log 2 → filtered output should reflect
/// the log update (E2/55.0).
#[tokio::test]
async fn fg_filter_in_log_updated_key_nonpart() -> Result<()> {
    let (table_path, partition, base_file, log_files) = nonpart_3commits_file_group();

    let baseline_for_key_lookup = read_file_group_with_key_filter(
        &table_path, partition, base_file, log_files.clone(), None,
    ).await?;
    let key_for_id5 = lookup_record_key(&baseline_for_key_lookup, 5);

    let filter: StdArc<dyn Predicate> = StdArc::new(predicates_factory::in_(
        Box::new(NameReference::new("_hoodie_record_key")),
        vec![Box::new(Literal::string(key_for_id5)) as Box<dyn Expression>],
    ));

    let ab = ab_read_with_filter(&table_path, partition, base_file, log_files, filter).await?;

    ab.assert_filter_narrowed();         // 1 < 4
    ab.assert_filtered_ids_eq(&[5]);

    // Cross-validate: filtered id=5 row should be the log-updated value (E2, 55.0).
    let expected = extract_row_with_id_opt_v9nonpart(&ab.baseline, 5)
        .expect("id=5 in baseline");
    let actual = extract_row_with_id_opt_v9nonpart(&ab.filtered, 5)
        .expect("id=5 in filtered");
    assert_eq!(expected, actual, "filtered id=5 must equal baseline id=5 (log-updated)");
    // Sanity: name should be "E2" (the log update value).
    assert_eq!(expected.1, "E2", "id=5 baseline value should be log update (E2)");

    Ok(())
}
```

- [ ] **Step 2: Run + commit**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --test file_group_reader_tests fg_filter_in_log_updated_key_nonpart 2>&1 | tail -10`
Expected: 1 test passes.

```bash
cd /home/ubuntu/ws3/hudi-rs && git add crates/core/tests/file_group_reader_tests.rs && git commit -m "$(cat <<'EOF'
add e2e test fg_filter_in_log_updated_key_nonpart

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase D — Test 8 on MorLayoutLogOnly

This fixture has **no base file** and uses a `key` column (string) instead of `id`. The existing test (`test_e2e_v9_mor_log_only`) compares against a gold parquet and explicitly filters out `_hoodie_*` columns. We need to verify whether `_hoodie_record_key` is in the merged output before writing assertions.

### Task D.1: Verify schema, add log-only extractor + Test 8

**Files:**
- Modify: `crates/core/tests/file_group_reader_tests.rs` (append a probe test, then the real test)

- [ ] **Step 1: Verify the fixture schema and key column**

Add a temporary probe test (will be deleted in Step 4 below):

```rust
/// Temporary probe — DO NOT COMMIT. Used during plan implementation only.
#[tokio::test]
#[ignore = "probe-only — used during impl to inspect fixture schema"]
async fn probe_log_only_schema() -> Result<()> {
    let (table_path, partition, base_file, log_files) = log_only_file_group();
    let batch = read_file_group_with_key_filter(
        &table_path, partition, base_file, log_files, None,
    ).await?;

    eprintln!("MorLayoutLogOnly schema:");
    for f in batch.schema().fields() {
        eprintln!("  {} ({:?})", f.name(), f.data_type());
    }
    eprintln!("Row count: {}", batch.num_rows());
    if batch.num_rows() > 0 {
        // Print first row of each column.
        for col_idx in 0..batch.num_columns() {
            let name = batch.schema().field(col_idx).name();
            eprintln!("  col '{}'[0] = {:?}", name, batch.column(col_idx).slice(0, 1));
        }
    }
    Ok(())
}
```

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --test file_group_reader_tests probe_log_only_schema -- --ignored --nocapture 2>&1 | tail -50`

Read the printed schema. Note:
- Whether `_hoodie_record_key` is present.
- Whether there's a `key` column or some other unique-id-like column.
- The data type of the `key`-equivalent column.
- The baseline row count.

- [ ] **Step 2: Append `extract_row_with_id_opt_log_only` based on findings**

If schema has `key` (String) as the unique-id column, append:

```rust
/// Schema-specific row lookup for MorLayoutLogOnly.
/// The fixture uses a `key` column (String) as the row identity.
/// Returns the row's (key, _full_row_index) for a given key, or None.
fn extract_row_index_by_key_log_only(
    batch: &arrow_array::RecordBatch,
    key: &str,
) -> Option<usize> {
    let key_col = batch.column_by_name("key")?
        .as_any().downcast_ref::<arrow_array::StringArray>()?;
    for i in 0..batch.num_rows() {
        if key_col.value(i) == key {
            return Some(i);
        }
    }
    None
}

/// Variant of ids_in_batch for the log-only fixture's `key` column.
fn keys_in_batch(batch: &arrow_array::RecordBatch) -> std::collections::BTreeSet<String> {
    let key_col = batch
        .column_by_name("key")
        .expect("key column missing")
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("key column should be String");
    (0..batch.num_rows()).map(|i| key_col.value(i).to_string()).collect()
}
```

If the schema has a different unique-id column, adapt accordingly — replace `key` with the actual column name throughout.

- [ ] **Step 3: Append Test 8**

```rust
/// Test 8: filter on a log-only file group (no base file).
///
/// Validates: ISOLATES the log-scan filter (KeyBasedFileGroupRecordBuffer
/// process_data_block / process_delete_block). All output flows through
/// the log-scan path because there is no base file.
///
/// Picks one key from the baseline batch and filters on it; expects the
/// filtered output to contain exactly that key's row (and no others).
#[tokio::test]
async fn fg_filter_in_log_only_filegroup() -> Result<()> {
    let (table_path, partition, base_file, log_files) = log_only_file_group();

    // Read baseline first to discover one valid record key.
    let baseline_for_key_lookup = read_file_group_with_key_filter(
        &table_path, partition, base_file, log_files.clone(), None,
    ).await?;
    assert!(baseline_for_key_lookup.num_rows() >= 2,
        "log-only baseline must have ≥2 rows for the filter to be a real narrow");

    // Use the _hoodie_record_key of the first row (or a different identity column
    // if the probe revealed _hoodie_record_key is missing — adjust based on Step 1).
    let hoodie_key_col = baseline_for_key_lookup
        .column_by_name("_hoodie_record_key")
        .expect("MorLayoutLogOnly should expose _hoodie_record_key in merged output")
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("_hoodie_record_key should be String");
    let target_key = hoodie_key_col.value(0).to_string();

    let filter: StdArc<dyn Predicate> = StdArc::new(predicates_factory::in_(
        Box::new(NameReference::new("_hoodie_record_key")),
        vec![Box::new(Literal::string(target_key.clone())) as Box<dyn Expression>],
    ));

    let ab = ab_read_with_filter(&table_path, partition, base_file, log_files, filter).await?;

    ab.assert_filter_narrowed();         // 1 < N

    // Verify exactly the target key remains.
    let filtered_keys: Vec<String> = (0..ab.filtered.num_rows())
        .map(|i| {
            ab.filtered
                .column_by_name("_hoodie_record_key").unwrap()
                .as_any().downcast_ref::<arrow_array::StringArray>().unwrap()
                .value(i).to_string()
        })
        .collect();
    assert_eq!(filtered_keys, vec![target_key.clone()],
        "filtered keys should be exactly [target_key]");

    Ok(())
}
```

If the probe in Step 1 revealed that `_hoodie_record_key` is NOT present in the merged output for this fixture, mark the test `#[ignore = "MorLayoutLogOnly does not expose _hoodie_record_key — TODO: add support or change fixture"]` and document the gap in the commit message.

- [ ] **Step 4: Remove the temporary probe test**

Delete the `probe_log_only_schema` test added in Step 1. It is not committed.

- [ ] **Step 5: Run + commit**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --test file_group_reader_tests fg_filter_in_log_only_filegroup 2>&1 | tail -10`
Expected: 1 test passes (or `ignored` with TODO if the schema doesn't support the assertion).

```bash
cd /home/ubuntu/ws3/hudi-rs && git add crates/core/tests/file_group_reader_tests.rs && git commit -m "$(cat <<'EOF'
add e2e test fg_filter_in_log_only_filegroup (isolates log-scan filter)

Reads MorLayoutLogOnly with and without an In filter on
_hoodie_record_key. Since the fixture has no base file, ALL output
flows through KeyBasedFileGroupRecordBuffer's process_data_block /
process_delete_block — this test isolates the log-scan filter.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Final verification

### Task E.1: Run the full test suite

**Files:** None modified — verification only.

- [ ] **Step 1: Run all keyFilterOpt tests**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core --test file_group_reader_tests fg_filter 2>&1 | tail -15`
Expected: 8 tests pass (or 7 + 1 ignored if Test 8 hit a fixture gap).

- [ ] **Step 2: Run full hudi-core**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo test -p hudi-core 2>&1 | tail -10`
Expected: all unit + integration tests pass; no regressions.

- [ ] **Step 3: Run clippy on the test file**

Run: `cd /home/ubuntu/ws3/hudi-rs && cargo clippy -p hudi-core --test file_group_reader_tests 2>&1 | grep -E "(error|warning).*file_group_reader" | head -10`

Address any new warnings introduced by this work (the existing test file has pre-existing warnings — only fix what this work added).

- [ ] **Step 4: Optional final cleanup commit**

If clippy fixes are needed:

```bash
cd /home/ubuntu/ws3/hudi-rs && git add crates/core/tests/file_group_reader_tests.rs && git commit -m "$(cat <<'EOF'
keyFilterOpt e2e coverage: clippy cleanup

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

If no cleanup needed, no commit.

---

## Test catalog summary (from spec §3 — for cross-reference)

| # | Test | Fixture | Filter | Expected | Validates |
|---|------|---------|--------|----------|-----------|
| 1 | `fg_filter_in_log_updated_key` | V9Mor8I4UCommitTime sf | In([key_for_id1]) | 1 row, Alice-V2 | log+base together; log-updated path |
| 2 | `fg_filter_in_base_only_key` | V9Mor8I4UCommitTime sf | In([key_for_id2]) | 1 row, Bob | **isolates base-file filter** |
| 3 | `fg_filter_in_no_match` | V9Mor8I4UCommitTime sf | In([nonexistent]) | 0 rows | filter actually drops everything |
| 4 | `fg_filter_starts_with_any_prefix` | V9Mor8I4UCommitTime sf | StringStartsWithAny([key_for_id1]) | 1 row | **PrefixKeys** path |
| 5 | `fg_filter_unsupported_predicate_is_noop` | V9Mor8I4UCommitTime sf | eq(...) | == baseline | regression: unsupported predicate must be no-op |
| 6 | `fg_filter_in_with_delete_block` | V9MorNonpart3Commits | In([key_for_id0]) | 0 rows | **delete-block** path through buffer |
| 7 | `fg_filter_in_log_updated_key_nonpart` | V9MorNonpart3Commits | In([key_for_id5]) | 1 row, E2/55.0 | log update on non-partitioned fixture |
| 8 | `fg_filter_in_log_only_filegroup` | MorLayoutLogOnly | In([first key]) | 1 row | **isolates log-scan filter** (no base file) |

## Out-of-plan future work (from spec §8)

- Combined-predicate coverage (`And(In, In)`, `Or(In, In)`) once `create_key_spec` is extended.
- Bootstrap merge filter coverage (currently rejected at construction time).
- COW-table filter coverage when COW write path lands.
- FFI-bridge filter pushdown (out of scope per the original port spec).
