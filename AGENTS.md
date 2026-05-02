# AGENTS.md

Guidance for AI coding agents working on this repository — the [agents.md](https://agents.md) format.
Humans should start with [`README.md`](./README.md) and [`CONTRIBUTING.md`](./CONTRIBUTING.md).

- [`CLAUDE.md`](./CLAUDE.md) imports this file via `@AGENTS.md` (Claude Code reads `CLAUDE.md`).
- [`.github/copilot-instructions.md`](./.github/copilot-instructions.md) is loaded by GitHub Copilot
  alongside this file and carries Copilot-specific notes.
- [`.github/instructions/*.instructions.md`](./.github/instructions) are path-scoped Copilot rules
  (via `applyTo` frontmatter); their content is summarized below so any agent applies the same standards.

## Project

Native Rust implementation of [Apache Hudi](https://hudi.apache.org) with Python (PyO3) and C++
([`cxx`](https://cxx.rs)) bindings. Apache 2.0. Rust workspace, edition `2024`, MSRV `1.88`.
Python `>=3.10`. Distributed as [`hudi`](https://crates.io/crates/hudi) on crates.io and
[`hudi`](https://pypi.org/project/hudi/) on PyPI.

```
crates/
  core/         hudi-core — config, expr, file_group, merge, metadata, schema, storage, table, timeline, hfile
  datafusion/   hudi-datafusion — DataFusion TableProvider (feature: datafusion)
  hudi/         public umbrella crate; re-exports core + (optional) datafusion
  test/         shared test fixtures
python/         PyO3 bindings (module hudi._internal); tests in python/tests
cpp/            cxx bindings; bridge in cpp/src/lib.rs
benchmark/tpch/ TPC-H benchmark harness
.github/instructions/   path-scoped Copilot rules (rust, python, code-review)
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

### Rust ([details](./.github/instructions/rust.instructions.md))

- **No `.unwrap()` / `.expect()` / `panic!()`** in non-test code (🔴 Critical). `unreachable!()`
  only with a comment justifying the invariant.
- **Errors carry context** — typed `thiserror` variants or `anyhow::Context`; messages name the
  offending value. Avoid bare `.map_err(Into::into)`.
- **No blocking I/O in async** (`std::fs::*`, `std::thread::sleep`); use Tokio or
  `tokio::task::spawn_blocking`. Async functions must return `Send` futures.
- **Avoid unnecessary `.clone()`** on `RecordBatch` / `Schema` / `Vec<_>`. Prefer `&str`, `&[T]`,
  `Cow<'_, str>` in parameters. Prefer Arrow compute kernels over hand-rolled loops.
- **Builder pattern** for many-optional types: consume `self`, return `Self`, finalize with `build()`.
- **Public items must have doc comments** (examples for non-trivial APIs; note panics, errors, safety).
- **Inline format args** (Rust 1.88+): `format!("{x}")`, not `format!("{}", x)`.
- **Don't widen the `cxx` FFI surface** to expose internal Hudi types — keep the bridge narrow.

### Python ([details](./.github/instructions/python.instructions.md))

- `ruff` (`E4 E7 E9 F I`, target `py310`) + `mypy --strict` over `hudi/*.py`. `snake_case`,
  docstrings on public APIs, type hints in `python/hudi/_internal.pyi`.
- **PyO3**: convert Rust errors to specific Python exceptions
  (`PyRuntimeError::new_err(format!("Failed to …: {e}"))`); never let a panic surface to Python.
  Release the GIL with `py.allow_threads(...)` for blocking I/O. Use `arrow::pyarrow::ToPyArrow`
  for zero-copy Arrow ↔ PyArrow.

### C++

`cxx` bridge in `cpp/src/lib.rs` — keep thin; push logic into `crates/core`. Functions may throw
`rust::Error`; document the error semantics on the C++ side.

## Testing

Rust unit tests are colocated under `#[cfg(test)] mod tests`; use `#[tokio::test]` for async.
Naming: `test_<function>_<scenario>_<expected>`. Shared fixtures in `crates/test`. Python tests in
`python/tests/`. Cover happy and error paths. New features and bug fixes **must** add tests; for
bug fixes, add a regression test that would have caught the bug. Avoid redundant coverage — each
test should have a unique purpose. Coverage tracked via [Codecov](https://app.codecov.io/github/apache/hudi-rs).

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

## Code review rubric

Full rubric: [`.github/instructions/code-review.instructions.md`](./.github/instructions/code-review.instructions.md).
Severity tags:

- 🔴 **Critical** — `.unwrap()/.expect()/panic!()` in lib code, blocking calls in async, hardcoded
  secrets, breaking public-API changes without docs.
- 🟠 **Important** — missing error context, unnecessary clones, missing doc comments on `pub`,
  missing tests for new behavior.
- 🟡 **Suggestion** — iterator chains over loops, `?` over nested `match` on `Result`.
- 💬 **Question** — clarification.

On updated PRs, focus on the latest commits; do not re-raise issues already fixed. For
`crates/core` public-API changes, also inspect `crates/datafusion`, `python/`, and `cpp/`.

## Pointers

- [`README.md`](./README.md) — usage examples
- [`CONTRIBUTING.md`](./CONTRIBUTING.md) — full contributor workflow
- [`Makefile`](./Makefile) — every supported dev/CI command
- [`Cargo.toml`](./Cargo.toml), [`python/pyproject.toml`](./python/pyproject.toml) — version pins
- [`CHANGELOG.md`](./CHANGELOG.md) — release history (driven by `cliff.toml`)
- [Apache Hudi docs](https://hudi.apache.org/docs/overview), [issue tracker](https://github.com/apache/hudi-rs/issues), [Slack](https://hudi.apache.org/slack)

## Maintenance

When you change a `make` target, a coding convention, the MSRV, or the workspace layout, **update
this file in the same PR**. Stale agent guidance produces stale code.
