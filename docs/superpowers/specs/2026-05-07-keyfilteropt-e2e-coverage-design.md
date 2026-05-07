# keyFilterOpt e2e test coverage expansion — design spec

**Date:** 2026-05-07
**Owner:** Davis Zhang
**Builds on:** `docs/superpowers/specs/2026-05-07-keyfilteropt-port-design.md` (the keyFilterOpt port that landed in 28 commits ending at `35d9603`)

## 1. Goal

Expand e2e test coverage of `key_filter_opt` so that each KeySpec variant is exercised, both call sites (log-scan filter via `KeyBasedFileGroupRecordBuffer::process_*`; base-file filter via `apply_key_filter_to_batch`) are independently validated, and every test proves the filter is **real**, not a no-op, via an explicit baseline-vs-filtered AB comparison.

The current state (after the port): a single e2e test `fg_reader_with_key_filter_filters_rows` covers `In`/FullKeys on `V9Mor8I4UCommitTime` `city=sf`. It validates that filtering reduces 2 rows to 1, and that the surviving row matches the baseline content for `id=1`. Coverage gaps:

1. `StringStartsWithAny` → `PrefixKeys` KeySpec path has zero e2e coverage.
2. The base-file filter (`apply_key_filter_to_batch`) and log-scan filter (`KeyBasedFileGroupRecordBuffer::process_*`) are exercised together but not isolated. A regression in either is currently undetectable independently.
3. "Filter is a no-op for unsupported predicates" (e.g. `eq`) is not regression-guarded.
4. Empty-filter-result corner case (filter matches nothing) is untested.
5. Delete-block path through `process_delete_block` is not exercised by any filter test.
6. Log-only file groups (no base file) are not exercised by any filter test.

## 2. Scope

In-scope:
- 8 new e2e tests across 3 fixtures.
- Shared AB-test helper (`FilterAbResult` + `ab_read_with_filter`) so every test consistently compares baseline vs filtered.
- Fixture locators for two new fixtures (`V9MorNonpart3Commits`, `MorLayoutLogOnly`).
- Schema-specific row extractors for fixtures whose columns differ from `V9Mor8I4UCommitTime`.

Out-of-scope:
- COW tables (no log files; only base-file filter path, which is covered).
- Merge modes other than `COMMIT_TIME_ORDERING`.
- The `MorLayoutLogCompaction` fixture (compacted log block). The per-record filter applies the same way; redundant with tests 6-7.
- Multi-file-group reads (the reader operates on one file group at a time).
- Combined predicate trees (`And`/`Or`/`Not` wrapping `In`). `create_key_spec` only matches top-level `In` or `StringStartsWithAny`; combined trees route through the no-op path that test 5 covers.

## 3. Test catalog

8 tests, each AB (read with `key_filter_opt = None`, then with `Some(filter)`).

### Group A — `V9Mor8I4UCommitTime`, partition `city=sf`

| # | Test name | Filter | Expected | Validates |
|---|-----------|--------|----------|-----------|
| 1 | `fg_filter_in_log_updated_key` (renamed from existing `fg_reader_with_key_filter_filters_rows`) | `In([key_for_id1])` — id=1 is updated in log | filtered=1 row, value=Alice-V2 | Filter applies during BOTH log scan and base read; log update path. |
| 2 | `fg_filter_in_base_only_key` | `In([key_for_id2])` — id=2 has no log update | filtered=1 row, value=Bob (base) | **Isolates base-file filter** — log scan finds no records for this key. |
| 3 | `fg_filter_in_no_match` | `In(["nonexistent_key"])` | filtered=0 rows, baseline=2 | Filter actually drops everything when nothing matches; catches a "filter silently no-op" bug. |
| 4 | `fg_filter_starts_with_any_prefix` | `StringStartsWithAny([<prefix matching id=1's key>])` | filtered=1 row | Exercises `PrefixKeys` KeySpec path. |
| 5 | `fg_filter_unsupported_predicate_is_noop` | `eq(name_ref, lit("anything"))` (Eq → `create_key_spec` returns None) | filtered.num_rows == baseline.num_rows; row content equal | Regression guard: unsupported predicate must NOT drop rows. |

### Group B — `V9MorNonpart3Commits`

| # | Test name | Filter | Expected | Validates |
|---|-----------|--------|----------|-----------|
| 6 | `fg_filter_in_with_delete_block` | `In([key_for_id0])` — id=0 is deleted in log block | filtered=0 rows; baseline excludes id=0 too (id 0 deleted) | Filter passes through `process_delete_block` correctly. |
| 7 | `fg_filter_in_log_updated_key_nonpart` | `In([key_for_id5])` — id=5 updated in log | filtered=1 row with updated value | Same as test 1 on a non-partitioned + multi-log-file fixture. |

### Group C — `MorLayoutLogOnly`

| # | Test name | Filter | Expected | Validates |
|---|-----------|--------|----------|-----------|
| 8 | `fg_filter_in_log_only_filegroup` | `In([key_for_one_id])` | filtered ⊊ baseline | **Isolates log-scan filter** — no base file present, so all output flows through `KeyBasedFileGroupRecordBuffer::process_*` where Task 2.3's `key_spec_opt` filter lives. |

## 4. Validation pattern

Every test uses `FilterAbResult` for assertions. Helpers added at the bottom of `crates/core/tests/file_group_reader_tests.rs`:

```rust
/// AB-test result. Used by every keyFilterOpt e2e test.
struct FilterAbResult {
    baseline: arrow_array::RecordBatch,
    filtered: arrow_array::RecordBatch,
}

impl FilterAbResult {
    /// Assert filter is real, not a no-op: filtered must have STRICTLY fewer rows than baseline.
    fn assert_filter_narrowed(&self) { … }

    /// Assert filter is a no-op (used by test 5 for unsupported predicates).
    fn assert_filter_was_noop(&self) { … }

    /// Assert that filtered's id-set is a subset of baseline's id-set.
    fn assert_filtered_ids_subset(&self) { … }

    /// Assert the exact set of ids present in filtered (sorted).
    fn assert_filtered_ids_eq(&self, expected_ids: &[i32]) { … }
}

/// Shared driver: read once with None, once with Some(filter), return both.
async fn ab_read_with_filter(
    table_path: &str,
    partition: &str,
    base_file: &str,
    log_files: Vec<&str>,
    filter: Arc<dyn Predicate>,
) -> Result<FilterAbResult> { … }
```

`assert_filter_was_noop` sorts both batches by id before comparing, since two reads of the same file group may produce rows in different orders.

For test 3 (filter matches nothing), `assert_filter_narrowed` is augmented: assert `filtered.num_rows() == 0` AND `baseline.num_rows() > 0`.

For tests on fixtures other than `V9Mor8I4UCommitTime`, schema-specific row extractors are added:
- `extract_row_with_id_opt_v9nonpart(batch, id) -> Option<(i32, String, f64, i64)>` — returns `(id, name, price, ts)` for `V9MorNonpart3Commits`.
- `extract_row_with_id_opt_log_only(batch, id) -> Option<(i32, ...)>` — schema TBD at impl time after inspecting the fixture.

## 5. File layout

Single file gets all the new tests, helpers, and locators (append only — no refactor of existing code):

```
crates/core/tests/file_group_reader_tests.rs   (existing — append only)
├── (existing) read_file_group, helpers, ~17 existing tests
├── (existing Phase 3) read_file_group_with_key_filter, lookup_record_key,
│                       extract_row_with_id_opt
├── fg_reader_with_key_filter_filters_rows   ← RENAMED to fg_filter_in_log_updated_key
└── (NEW)
    ├── FilterAbResult struct + impl (4 assertion methods)
    ├── ab_read_with_filter(...) async fn
    ├── 3 fixture locators: sf_file_group, nonpart_3commits_file_group, log_only_file_group
    ├── 2 schema-specific extractors: extract_row_with_id_opt_v9nonpart,
    │                                  extract_row_with_id_opt_log_only
    ├── tests 1-5 on V9Mor8I4UCommitTime city=sf
    ├── tests 6-7 on V9MorNonpart3Commits
    └── test 8 on MorLayoutLogOnly
```

The existing `fg_reader_with_key_filter_filters_rows` is renamed to `fg_filter_in_log_updated_key` (test 1) so naming is consistent across the new set. Its body is updated to use `FilterAbResult` for symmetry, but its assertions are unchanged.

## 6. Risks

1. **Fixture filename discovery.** Two new fixtures (`v9_mor_nonpart_3commits`, `table_log_only`) need filenames resolved at impl time via:
   ```bash
   find crates/test/data/tables/v9_mor_nonpart_3commits -type f | grep -v '\.hoodie'
   find crates/test/data/tables/table_log_only -type f | grep -v '\.hoodie'
   ```
   Followed by a one-shot read of the baseline batch with column/schema inspection before writing assertions.

2. **`_hoodie_record_key` column may not be present.** The existing test relies on it. If a new fixture uses virtual keys / different keygen, the lookup field changes. The pre-write check (read baseline + inspect schema) catches this.

3. **`id` and other column names may differ across fixtures.**
   - `V9Mor8I4UCommitTime`: `id INT, name STRING, age INT, ts STRING, city STRING`.
   - `V9MorNonpart3Commits`: `id INT, name STRING, price DOUBLE, ts LONG`.
   - `MorLayoutLogOnly`: schema TBD at impl time.

   Per-fixture extractors handle this. If `id` is missing on `MorLayoutLogOnly`, fall back to whatever unique-id-like column exists (or use `_hoodie_record_key` itself for row identity).

4. **Test 6 (delete block) baseline assumption.** `V9MorNonpart3Commits` deletes ids 0/1/2 in log. Baseline (no filter) should NOT contain id=0. Filtered with `In([key_for_id0])` should yield 0 rows. **Open question:** how do we look up `key_for_id0` if id=0 isn't in the merged output? Answer: read the **base file directly** (separately, without log merge) to find id=0's `_hoodie_record_key`, OR use the deterministic key encoding (e.g. just `"0"` if SimpleKeyGenerator). At impl time, if the lookup is awkward, simplify by using `lit("0")` directly and asserting that this produces 0 rows. The point of test 6 is to validate the filter passes through the delete-block code path, not to recover deleted-key lookups.

5. **Order non-determinism in `assert_filter_was_noop`.** Two reads of the same file group may emit rows in different orders. Sort by id before equality comparison.

6. **Adding new fixtures might require unzipping or generating data.** If `crates/test/data/tables/v9_mor_nonpart_3commits` or `crates/test/data/tables/table_log_only` doesn't exist as expected, the corresponding tests get `#[ignore]` with a TODO comment + follow-up filed. Plan task 8 (final verification) flags this as a known incomplete.

## 7. Implementation order

Three phases, each independently testable:

| Phase | Tasks |
|-------|-------|
| A | Add `FilterAbResult` + `ab_read_with_filter` helper. Refactor existing test to use them. Verify still passes. |
| B | Add tests 2-5 on `V9Mor8I4UCommitTime` (existing fixture, no new locator needed). |
| C | Add `nonpart_3commits_file_group` locator + `extract_row_with_id_opt_v9nonpart` + tests 6-7. |
| D | Add `log_only_file_group` locator + extractor + test 8. |

Phases are sequential; each ends with a commit. If a phase hits a fixture issue (e.g., column missing), mark its test `#[ignore]` and proceed.

## 8. Out-of-plan future work

- Combined-predicate coverage (`And(In, In)`, `Or(In, In)`) once `create_key_spec` is extended to handle them.
- Bootstrap merge filter coverage (currently rejected at construction time per `HoodieFileGroupReader::new` panic).
- COW-table filter coverage when COW write path lands.
- Filter pushdown via the FFI bridge (currently out of scope per the original port spec).
