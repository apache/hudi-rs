# Rust Conventions

## Error Handling

- **No `.unwrap()` / `.expect()` / `panic!()`** in non-test code (Critical). `unreachable!()`
  only with a comment justifying the invariant.
- **Errors carry context** — typed `thiserror` variants or `anyhow::Context`; messages name the
  offending value. Avoid bare `.map_err(Into::into)`.

```rust
// GOOD — with context
let file = File::open(&path)
    .with_context(|| format!("Failed to open table metadata at {}", path.display()))?;

// BAD — no context
let file = File::open(&path).map_err(|e| HudiError::Io(e))?;
```

## Async

- **No blocking I/O in async** (`std::fs::*`, `std::thread::sleep`); use Tokio or
  `tokio::task::spawn_blocking`. Async functions must return `Send` futures (no `Rc` or `RefCell`
  across await points).

## Memory & Performance

- **Avoid unnecessary `.clone()`** on `RecordBatch` / `Schema` / `Vec<_>`. Prefer `&str`, `&[T]`,
  `Cow<'_, str>` in parameters.
- **Prefer streaming over collecting** — don't collect streams of `RecordBatch` into `Vec` when
  you can process them incrementally.
- Prefer Arrow compute kernels over hand-rolled loops. Use `arrow::compute::concat_batches` for
  combining batches.

## API Design

- **Builder pattern** for many-optional types: consume `self`, return `Self`, finalize with
  `build()`.

```rust
pub struct TableBuilder {
    base_uri: String,
    options: HashMap<String, String>,
}

impl TableBuilder {
    pub fn from_base_uri(uri: impl Into<String>) -> Self { ... }
    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self { ... }
    pub async fn build(self) -> Result<Table> { ... }
}
```

- **Public items must have doc comments** (examples for non-trivial APIs; note panics, errors,
  safety).

## Testing

Unit tests are colocated under `#[cfg(test)] mod tests`; use `#[tokio::test]` for async.
Naming: `test_<function>_<scenario>_<expected>`. Shared fixtures in `crates/test`.

## Style

Run `make format-rust check-rust` before submitting to match CI.

- **Inline format args** (Rust 1.88+): `format!("{x}")`, not `format!("{}", x)` — expressions
  like `path.display()` still require positional args.
