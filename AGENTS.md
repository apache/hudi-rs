# AGENTS.md

## Project

Native Rust implementation of [Apache Hudi](https://hudi.apache.org) with Python (PyO3) and C++
([`cxx`](https://cxx.rs)) bindings. Apache 2.0. Rust workspace, edition `2024`, MSRV `1.88`.
Python `>=3.10`. Key traits: async-first (tokio), Arrow-native, `object_store` for all I/O,
timeline-based MVCC.

```
crates/
  core/         hudi-core — config, expr, file_group, merge, metadata, schema, storage, table, timeline, hfile
  datafusion/   hudi-datafusion — DataFusion TableProvider (feature: datafusion)
  hudi/         public umbrella crate; re-exports core + (optional) datafusion
  test/         shared test fixtures
python/         PyO3 bindings (module hudi._internal); tests in python/tests
cpp/            cxx bindings; bridge in cpp/src/lib.rs
benchmark/tpch/ TPC-H benchmark harness
```

## Commands

The [`Makefile`](./Makefile) is the canonical command surface — **prefer `make <target>`** so agents
and humans run what CI runs. Cargo / maturin go through [`build-wrapper.sh`](./build-wrapper.sh)
(macOS 26+ SDK env vars).

```bash
make setup-venv && source .venv/bin/activate
make develop                 # build workspace + install Python binding via maturin
make format check test       # the pre-PR loop CI runs

cargo test -p hudi-core                                        # one crate
cargo test -p hudi-core table::tests::hudi_table_get_schema    # one test
pytest python/tests/test_table_read.py -s -k "<expr>"          # one Python test
make coverage-rust                                             # tarpaulin HTML at cov-reports/
```

## Conventions

### Dependencies

Prefer stdlib or existing workspace dependencies before adding new crates. Keep `Cargo.lock`
changes intentional — don't `cargo add` without justification.

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

## Cloud storage & config

Storage backends route by URI scheme (`file://`, `s3://`, `az://`, `gs://`) through
[`object_store`](https://docs.rs/object_store) — don't hand-roll per-scheme paths. Table options
are typed: `HudiTableConfig`, `HudiReadConfig` (also Python enums). Prefer enum members over raw
string keys; bulk variants (`with_hudi_options` / `with_options`) currently expect string keys.

## Code review

See [`.github/instructions/code-review.instructions.md`](./.github/instructions/code-review.instructions.md)
for the full rubric, severity tags, and checklists.

## Maintenance

When you change a `make` target, a coding convention, the MSRV, or the workspace layout, **update
this file in the same PR**. Stale agent guidance produces stale code.
