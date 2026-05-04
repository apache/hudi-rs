# GitHub Copilot Instructions — Apache Hudi-rs

GitHub Copilot loads **both** this file and [`AGENTS.md`](../AGENTS.md). Treat `AGENTS.md` files
as the source of truth — this file adds Copilot-specific notes only.

Language-specific conventions live in sub-directory AGENTS.md files:

- [`crates/AGENTS.md`](../crates/AGENTS.md) — Rust
- [`python/AGENTS.md`](../python/AGENTS.md) — Python / PyO3
- [`cpp/AGENTS.md`](../cpp/AGENTS.md) — C++ / cxx

Path-scoped rules under [`./instructions/`](./instructions) reference the sub-directory AGENTS.md
files and are loaded automatically when files match their `applyTo` glob.

For code review behavior, see
[`code-review.instructions.md`](./instructions/code-review.instructions.md).

For everything else — build commands, testing, PR rules — see [`AGENTS.md`](../AGENTS.md).
