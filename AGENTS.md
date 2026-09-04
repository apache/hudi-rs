# AGENTS.md

## Project

Native Rust implementation of [Apache Hudi](https://hudi.apache.org) with Python (PyO3) and C++
([`cxx`](https://cxx.rs)) bindings. Apache 2.0. Rust workspace, edition `2024`. MSRV is `1.94.1`;
the dev toolchain is pinned to Rust `1.94`. Python `>=3.10`. Key traits: async-first (tokio),
Arrow-native, `object_store` for all I/O, timeline-based MVCC.

```
crates/
  core/         hudi-core — avro_to_arrow, config, expr, file_group, hfile, keygen, merge, metadata,
                record, schema, statistics, storage, table, timeline, util
  datafusion/   hudi-datafusion — DataFusion TableProvider (feature: datafusion)
  hudi/         public umbrella crate; re-exports core + (optional) datafusion
  jvm-ffi/      hudi-jvm-ffi — plain C ABI for JVM callers via the Arrow C Data Interface
  test/         shared test fixtures
python/         PyO3 bindings (module hudi._internal); tests in python/tests
cpp/            cxx bindings; bridge in cpp/src/lib.rs
benchmark/tpch/ TPC-H benchmark harness
benchmark/filegroup/ file-group read benchmark (fg-bench / fg-gen)
```

## Commands

The [`Makefile`](./Makefile) is the canonical command surface — **prefer `make <target>`** so agents
and humans run what CI runs. Cargo / maturin go through [`build-wrapper.sh`](./build-wrapper.sh)
(macOS 26+ SDK env vars).

```bash
make setup-venv && source .venv/bin/activate
make develop                 # build workspace + install Python binding via maturin
make format check test       # the pre-PR loop CI runs
cargo +1.94.1 check --workspace --all-targets --all-features  # optional MSRV sanity check

cargo test -p hudi-core                                        # one crate
cargo test -p hudi-core table::tests::hudi_table_get_schema    # one test
pytest python/tests/test_table_read.py -s -k "<expr>"          # one Python test
make coverage-rust                                             # tarpaulin HTML at cov-reports/

# hudi-core without the spill backend — CI runs this leg, so check it before a PR
# that touches the merge map or its dependencies.
cargo clippy -p hudi-core --lib --no-default-features -- -D warnings
cargo test -p hudi-core --lib --no-default-features
```

## Conventions

### Features

`spill-rocksdb` carries the merge-on-read merge map's on-disk tier. It is separable because
`rocksdb` bundles RocksDB and runs `bindgen`, so leaving it unconditional puts libclang and a C++
toolchain in front of every consumer, for a tier that only engages when a merge exceeds
`hoodie.memory.merge.max.size`.

It is **default-on in `hudi-core`, `hudi-datafusion` and `hudi`**, and the latter two forward it
rather than taking `hudi-core` with its defaults: a consumer can only opt out of a feature its
direct dependency exposes, and unification through `hudi-datafusion` would otherwise hand the tier
back. The opt-out is therefore `default-features = false` on `hudi`, and it holds with the
`datafusion` feature on. **The Python wheel and the cxx bridge do not opt out** — both take `hudi`
with defaults and build RocksDB. Dropping the feature removes the tier, and a merge past the budget
then fails with a `CoreError::Unsupported` naming the config rather than spilling, which is why the
published artifacts keep it.

A test that needs the disk tier must be `#[cfg(feature = "spill-rocksdb")]`; the
`--no-default-features` CI leg installs no libclang, so a change that reintroduces the native
dependency fails to compile there rather than passing quietly.

### Dependencies

Prefer stdlib or existing workspace dependencies before adding new crates. Keep `Cargo.lock`
changes intentional — don't `cargo add` without justification. A dependency needed by one module
for an optional capability belongs behind a feature, not in the unconditional set.

### Language-specific

- [`crates/AGENTS.md`](./crates/AGENTS.md) — Rust
- [`python/AGENTS.md`](./python/AGENTS.md) — Python / PyO3
- [`cpp/AGENTS.md`](./cpp/AGENTS.md) — C++ / cxx

## Testing

Cover happy and error paths. New features and bug fixes **must** add tests; for bug fixes, add a
regression test that would have caught the bug. Avoid redundant coverage — each test should have a
unique purpose.

## Pull requests

1. **Title**: [Conventional Commits](https://www.conventionalcommits.org)
   (`<type>(<scope>): <description>`). Allowed types per
   [`.commitlintrc.yaml`](./.commitlintrc.yaml):
   `build chore ci docs feat fix perf refactor revert style test`. Header ≤ 100 chars; lower-case
   type; no trailing period; no sentence/start/upper/pascal case in the subject.
   Example: `feat(core): add support for MOR table reads`.
2. **Diff size**: `max(added, deleted) < 1000 lines` or justify in the description.
3. **Tests required** for new features and bug fixes.
4. **Comments**: comment non-obvious WHY only (constraints, invariants, workarounds). Don't
   reference internal plans or external roadmaps; for unimplemented work prefer "not yet implemented".
5. **Cross-binding impact**: changes to `crates/core` public API may cascade to `crates/datafusion`,
   `python/`, and `cpp/`. Verify all bindings still build; document breaking changes.
6. **No secrets**. Cloud credentials come from env vars (`AWS_*`, `AZURE_*`, `GOOGLE_*`) or table
   options. Don't bypass `make check` / pre-commit hooks (`--no-verify`) without justification.

## Reader semantics

### Commit visibility

A data file is readable only when the commit that wrote it is committed —
`CompletionTimeView::is_committed`, which mirrors Java's
`containsInstant(ts) || isBeforeTimelineStarts(ts)`. Both halves are load-bearing:
membership in the active completed set, **or** below the archival boundary
(`Timeline::earliest_active_instant`), since archival only ever moves completed
instants. Testing for a completion timestamp instead is wrong twice over — layout
v1 records none, and the completion map is built from the active timeline, so it
also discards files from archived commits.

### Incremental windows bound completion time

On timeline layout v2, `hoodie.read.start/end.timestamp` bound a commit's
**completion** time (Hudi 1.x parity). Layout v1 has no completion times and bounds
requested times. The translation happens in exactly one place,
`Table::resolve_incremental_window`, which resolves the window to the instant times
it admits and re-expresses the bounds over those commits' *requested* times —
because everything below the row mask (which base file, which log blocks) is a
requested-time decision. Translating twice selects nothing; that is the bug the
single translation point exists to prevent.

## Cloud storage & config

Storage backends route by URI scheme (`file://`, `s3://`, `az://`, `gs://`) through
[`object_store`](https://docs.rs/object_store) — don't hand-roll per-scheme paths. Table options
are typed: `HudiTableConfig`, `HudiReadConfig`, `HudiPlanConfig` (also Python enums). Prefer enum members over raw
string keys; bulk variants (`with_hudi_options` / `with_options`) currently expect string keys.

## Code review

See [`.github/instructions/code-review.instructions.md`](./.github/instructions/code-review.instructions.md)
for the full rubric, severity tags, and checklists.

## Maintenance

When you change a `make` target, a coding convention, the MSRV, or the workspace layout, **update
this file in the same PR**. Stale agent guidance produces stale code.
