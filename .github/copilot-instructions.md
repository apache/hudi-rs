# GitHub Copilot Instructions — Apache Hudi-rs

GitHub Copilot loads **both** this file and [`AGENTS.md`](../AGENTS.md) at the root of the repo;
they are concatenated, not alternatives. Treat `AGENTS.md` as the source of truth for project
overview, build commands, coding conventions, testing, PR rules, and the review rubric — this file
adds Copilot-specific notes only.

Path-scoped rules under [`./instructions/`](./instructions) are loaded automatically when files
match their `applyTo` glob and remain authoritative for those files.

## Quick orientation

- Native Rust implementation of Apache Hudi with Python (PyO3) and C++ (`cxx`) bindings.
- Workspace: `crates/{core,datafusion,hudi,test}`, plus `python/`, `cpp/`, `benchmark/tpch/`.
- Toolchain: Rust edition `2024` / MSRV `1.88`; Python `>=3.10`; managed via `uv` and `maturin`.
- Pre-PR check: `make format check test`.

## Path-scoped rules (loaded by `applyTo` frontmatter)

| Glob              | File                                                                                       | Topic                                                              |
| ----------------- | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------ |
| `**/*.rs`         | [`instructions/rust.instructions.md`](./instructions/rust.instructions.md)                 | Rust error handling, async, performance, API design, doc comments  |
| `python/**`       | [`instructions/python.instructions.md`](./instructions/python.instructions.md)             | PyO3 patterns, GIL management, PyArrow conversion, Python tests    |
| `**/*` (review)   | [`instructions/code-review.instructions.md`](./instructions/code-review.instructions.md)   | Review rubric, severity tags, multi-round behavior, cross-file impact |

## PR title format

PR titles must follow [Conventional Commits](https://www.conventionalcommits.org)
(`<type>(<scope>): <description>`). Allowed types per
[`.commitlintrc.yaml`](../.commitlintrc.yaml):
`build chore ci docs feat fix perf refactor revert style test`. Examples:

- `feat(core): add support for MOR table reads`
- `fix(python): handle null partition values correctly`
- `docs: update API documentation for HudiTable`

## Copilot review behavior

- Focus on the latest commits in updated PRs; don't re-raise issues already fixed.
- Use the severity tags from [`AGENTS.md` → Code review](../AGENTS.md#code-review-rubric)
  (🔴 / 🟠 / 🟡 / 💬).
- For `crates/core` public-API changes, check `crates/datafusion`, `python/`, and `cpp/` for
  downstream impact before approving.
- Flag `.unwrap()` / `.expect()` / `panic!()` in non-test code as 🔴 Critical.
- Flag blocking I/O in async functions as 🔴 Critical.
- Flag hardcoded credentials or secrets as 🔴 Critical.

For everything else — build commands, coding conventions, testing, security expectations — see
[`AGENTS.md`](../AGENTS.md).
