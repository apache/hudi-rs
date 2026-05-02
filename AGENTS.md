<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# AGENTS.md

Canonical guidance for AI coding agents (ChatGPT/Codex, GitHub Copilot, Cursor, Aider, Zed, etc.)
working on this repository — the [`agents.md`](https://agents.md) format. Humans should start with
[`README.md`](./README.md) and [`CONTRIBUTING.md`](./CONTRIBUTING.md).

- [`CLAUDE.md`](./CLAUDE.md) imports this file via `@AGENTS.md` (Claude Code reads `CLAUDE.md`, not
  `AGENTS.md`).
- [`.github/copilot-instructions.md`](./.github/copilot-instructions.md) is loaded by GitHub Copilot
  alongside this file — both are read; it carries Copilot-specific notes.
- [`.github/instructions/*.instructions.md`](./.github/instructions) hold path-scoped Copilot rules
  via `applyTo` frontmatter; their content is summarized below so any agent applies the same
  standards.

## Project at a glance

- Native Rust implementation of [Apache Hudi](https://hudi.apache.org), with Python (PyO3) and C++
  ([`cxx`](https://cxx.rs)) bindings. Apache 2.0; ASF project.
- Rust workspace, edition `2024`, MSRV `1.88`. Python `>=3.10`. See [`Cargo.toml`](./Cargo.toml)
  and [`python/pyproject.toml`](./python/pyproject.toml) for authoritative version pins.
- Distributed as [`hudi` on crates.io](https://crates.io/crates/hudi) and
  [`hudi` on PyPI](https://pypi.org/project/hudi/).

## Repository layout

```
crates/
  core/         hudi-core — config, expr, file_group, merge, metadata, schema, storage, table, timeline, hfile
  datafusion/   hudi-datafusion — DataFusion TableProvider (feature: datafusion)
  hudi/         public umbrella crate; re-exports core + (optional) datafusion
  test/         shared test fixtures
python/         PyO3 bindings (module hudi._internal); tests in python/tests
cpp/            cxx-based C++ bindings; bridge in cpp/src/lib.rs
benchmark/tpch/ TPC-H benchmark harness (datafusion / spark)
demo/           docker-compose demos
.github/
  workflows/        ci.yml, code.yml, pr.yml, release.yml
  instructions/     path-scoped Copilot rules (rust, python, code-review)
```

## Setup & common commands

The [`Makefile`](./Makefile) is the canonical command surface. **Prefer `make <target>`** so agents
and humans run exactly what CI runs. Cargo / maturin invocations go through
[`build-wrapper.sh`](./build-wrapper.sh) (sets macOS SDK env vars on macOS 26+).

```bash
make setup-venv && source .venv/bin/activate
make develop                 # build workspace + install Python binding via maturin
make format check test       # the pre-PR loop CI runs

# Targeted runs
cargo test -p hudi-core                                        # one crate
cargo test -p hudi-core table::tests::hudi_table_get_schema    # one test
pytest python/tests/test_table_read.py -s -k "test_read_table_has_correct_schema"
make coverage-rust                                             # tarpaulin HTML at cov-reports/
make coverage-check                                            # fails if < 60% (COV_THRESHOLD)
```

## Coding conventions

Full per-language rules live in [`.github/instructions/`](./.github/instructions). Highlights:

### Rust ([rules](./.github/instructions/rust.instructions.md))

- **No `.unwrap()` / `.expect()` / `panic!()`** in non-test code (treated as 🔴 Critical in review).
  `unreachable!()` is OK only with a comment justifying the invariant.
- **Errors carry context** — typed `thiserror` variants or `anyhow::Context`; messages name the
  offending value (path, key, timestamp). Avoid bare `.map_err(Into::into)`.
- **No blocking I/O in async** (`std::fs::*`, `std::thread::sleep`). Use Tokio equivalents or
  `tokio::task::spawn_blocking` for CPU-bound work. Async functions must return `Send` futures (no
  `Rc` / `RefCell` across `.await`).
- **Avoid unnecessary `.clone()`** on `RecordBatch`, `Schema`, `Vec<_>`. Prefer `&str`, `&[T]`, or
  `Cow<'_, str>` in parameter positions when ownership isn't required. Prefer Arrow compute kernels
  over hand-rolled loops.
- **Builder pattern** for many-optional types: consume `self`, return `Self`, finalize with
  `build()` / `build().await`.
- **Public items must have doc comments**, with examples for non-trivial APIs and explicit notes on
  panics, errors, and safety.
- **Inline format args** (Rust 1.88+): `format!("{x}")`, not `format!("{}", x)`.
- **Don't widen the `cxx` FFI surface** to expose internal Hudi types — keep the bridge narrow.

### Python ([rules](./.github/instructions/python.instructions.md))

- Linted with `ruff` (`E4 E7 E9 F I`, `target-version = py310`); type-checked with `mypy --strict`
  over `hudi/*.py`. `snake_case`, docstrings on public APIs, type hints in `python/hudi/_internal.pyi`.
- **PyO3**: convert Rust errors into specific Python exceptions
  (`PyRuntimeError::new_err(format!("Failed to …: {e}"))`); never let a panic surface to Python.
  Release the GIL with `py.allow_threads(...)` for blocking I/O. Use `arrow::pyarrow::ToPyArrow`
  for zero-copy Arrow ↔ PyArrow conversion.

### C++

The FFI surface is generated by [`cxx`](https://cxx.rs); the Rust bridge lives in `cpp/src/lib.rs`.
Keep it thin — push real logic into `crates/core` and expose narrow shims. Functions may throw
`rust::Error`; document the error semantics on the C++ side.

## Testing

- Rust unit tests are colocated under `#[cfg(test)] mod tests`. Use `#[tokio::test]` for async.
  Naming: `test_<function>_<scenario>_<expected>`. Shared fixtures live in `crates/test`.
- Python tests are in `python/tests/` and run under `pytest`. Cover happy and error paths.
- New features and bug fixes **must** add tests; for bug fixes, add a regression test that would
  have caught the bug. Avoid redundant coverage — each test should have a unique purpose.
- Coverage tracked via [Codecov](https://app.codecov.io/github/apache/hudi-rs)
  ([`codecov.yml`](./codecov.yml)).

## Pull request conventions

1. **Title** must follow [Conventional Commits](https://www.conventionalcommits.org)
   (`<type>(<scope>): <description>`). Allowed types per
   [`.commitlintrc.yaml`](./.commitlintrc.yaml):
   `build chore ci docs feat fix perf refactor revert style test`. Header ≤ 100 chars; lower-case
   type; no trailing period; no sentence/start/upper/pascal case in the subject. Examples:
   - `feat(core): add support for MOR table reads`
   - `fix(python): handle null partition values correctly`
2. **Diff size**: keep `max(added, deleted) < 1000 lines` or justify in the PR description.
3. **Tests required** for new features and bug fixes.
4. **Comments**: comment non-obvious WHY only (constraints, invariants, workarounds). Don't
   reference internal plans, phases, or external roadmaps; for unimplemented work prefer
   "not yet implemented" over TODOs that point at planning docs.
5. **Cross-binding impact**: changes to `crates/core` public API may cascade to `crates/datafusion`,
   `python/`, and `cpp/`. Verify all bindings still build and document any breaking changes.
6. **No secrets** in code, tests, fixtures, or CI logs. Cloud credentials come from environment
   variables (`AWS_*`, `AZURE_*`, `GOOGLE_*`) or table options.
7. **Don't bypass `make check` / pre-commit hooks** (`--no-verify`) without justification — fix
   the underlying issue.
8. **License headers**: every new source file needs the ASF header
   ([`.licenserc.yaml`](./.licenserc.yaml) lists ignored paths; CI fails without it).

## Cloud storage & configuration

- Storage backends are routed by URI scheme (`file://`, `s3://`, `az://`, `gs://`) through
  [`object_store`](https://docs.rs/object_store). Don't hand-roll per-scheme path handling.
- Table options are typed: `HudiTableConfig`, `HudiReadConfig` (also exported as Python enums).
  Prefer the typed enum over raw string keys. Singular setters (`with_hudi_option` / `with_option`)
  accept enum members directly; bulk variants (`with_hudi_options` / `with_options`) currently
  expect string keys (or `member.value`).

## Code review (rubric)

Full rubric: [`.github/instructions/code-review.instructions.md`](./.github/instructions/code-review.instructions.md). Severity tags to use in comments:

- 🔴 **Critical** — must fix: `.unwrap()/.expect()/panic!()` in lib code, blocking calls in async,
  hardcoded secrets, breaking public-API changes without docs.
- 🟠 **Important** — should fix: missing error context, unnecessary clones, missing doc comments
  on `pub` items, missing tests for new behavior.
- 🟡 **Suggestion** — iterator chains over imperative loops, `?` over nested `match` on `Result`.
- 💬 **Question** — clarification.

On updated PRs, focus on the latest commits; do not re-raise issues fixed by follow-up commits.
For `crates/core` public API changes, also inspect `crates/datafusion`, `python/`, and `cpp/`.

## Pointers

| Document                                              | Purpose                                                            |
| ----------------------------------------------------- | ------------------------------------------------------------------ |
| [`README.md`](./README.md)                            | usage examples (Python / Rust / C++) and feature overview          |
| [`CONTRIBUTING.md`](./CONTRIBUTING.md)                | full human contributor workflow                                    |
| [`Makefile`](./Makefile)                              | every supported dev/CI command                                     |
| [`Cargo.toml`](./Cargo.toml)                          | workspace members and dependency pins                              |
| [`python/pyproject.toml`](./python/pyproject.toml)    | Python build / lint / type-check configuration                     |
| [`CHANGELOG.md`](./CHANGELOG.md)                      | release history (driven by `cliff.toml` from Conventional Commits) |
| [Apache Hudi docs](https://hudi.apache.org/docs/overview) | timeline, file groups, MOR/COW, schema evolution               |
| [Issue tracker](https://github.com/apache/hudi-rs/issues) | bugs, features                                                 |
| [Slack](https://hudi.apache.org/slack)                | community chat (`#hudi`)                                           |

## Maintenance

When you add or rename a `make` target, change a coding convention, bump the MSRV, or restructure
the workspace, **update this file in the same PR**. Stale agent guidance produces stale code.
