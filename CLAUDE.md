# CLAUDE.md

Guidance for Claude Code when working in the hudi-rs repo.

## Toolchain

- **Rust:** `1.88` (edition 2024), pinned in `rust-toolchain.toml` and `Cargo.toml` (`workspace.package.rust-version`). Rustup auto-installs this when you run cargo in the repo.
- **Python:** managed via [uv](https://github.com/astral-sh/uv). See `python/pyproject.toml` for the supported version range.
- **Maturin:** used to build the Python bindings; version is pinned in `python/pyproject.toml` (`requires = ...`).

## Workspace layout

Cargo workspace (`resolver = "2"`) with these members:

- `crates/core` — core Hudi logic (`hudi-core`)
- `crates/datafusion` — DataFusion integration
- `crates/hudi` — umbrella crate published as `hudi` on crates.io
- `crates/test` — shared test utilities
- `cpp` — C++ bindings via cxx
- `python` — PyO3/Maturin bindings, published as `hudi` on PyPI
- `benchmark/tpch` — TPC-H benchmark harness

## Build & test (Rust)

Prefer the Makefile targets; they wrap the canonical commands.

```shell
# All Rust tests (workspace, all targets, all features)
make test-rust
# equivalent: cargo test --no-fail-fast --all-targets --all-features --workspace

# Single crate
cargo test -p hudi-core

# Single test
cargo test -p hudi-core table::tests::hudi_table_get_schema

# Format / lint (matches CI)
make format-rust     # cargo fmt --all
make check-rust      # cargo clippy --all-targets --all-features --workspace --no-deps -- -D warnings
                     # + cargo fmt --all -- --check
```

Note: Rust commands are invoked through `./build-wrapper.sh` in the Makefile. Running `cargo` directly works fine for local dev.

## Build & test (Python)

```shell
make setup-venv                   # uv venv .venv
source .venv/bin/activate
make develop                      # maturin develop --extras=devel,datafusion --features datafusion,testing
make test-python                  # uv run pytest -s python
pytest python/tests/test_table_read.py -s -k "test_read_table_has_correct_schema"
make check-python                 # ruff format --check, ruff check, mypy
```

## Before opening a PR

```shell
make format check test
```

This runs format + check + test for both Rust and Python. Same checks CI runs.

## Coverage

```shell
make coverage-rust       # HTML report at ./cov-reports/tarpaulin-report.html
make coverage-check      # fails if below COV_THRESHOLD (default 60)
```
Requires `cargo install cargo-tarpaulin`.

## Benchmarks

TPC-H harness lives in `benchmark/tpch`. See `make tpch-generate`, `make tpch-create-tables`, `make bench-tpch`.

## PR conventions (from CONTRIBUTING.md)

- Title must follow [conventional commits](https://www.conventionalcommits.org).
- Keep diffs under ~1000 lines when feasible.
- New features / bug fixes require tests.
- Prefer self-explanatory code over comments; only comment non-obvious logic.
