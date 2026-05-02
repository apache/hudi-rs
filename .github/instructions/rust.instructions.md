---
applyTo: "**/*.rs"
---

# Rust Instructions for Apache Hudi-rs

## Error Handling (Critical)

- Flag `.unwrap()` / `.expect()` in non-test code as **Critical**
- Flag `panic!()` in library code as **Critical**
- Exception: `unreachable!()` is acceptable with a comment explaining why
- Prefer `anyhow::Context` or custom error types with context over bare `.map_err()`
- Error messages should be actionable and include relevant values

```rust
// GOOD - with context
let file = File::open(&path)
    .with_context(|| format!("Failed to open table metadata at {}", path.display()))?;

// BAD - no context
let file = File::open(&path).map_err(|e| HudiError::Io(e))?;
```

## Memory and Performance

- Flag unnecessary `.clone()`, especially on large types like `Vec<RecordBatch>`
- Prefer `&str` / `&[T]` over owned types in parameters when ownership isn't needed
- Use `Cow<'_, str>` when a function might or might not need to allocate
- Prefer Arrow compute kernels over manual iteration on arrays
- Use `arrow::compute::concat_batches` for combining batches

## Async Code Patterns

- Ensure async functions return `Send` futures (no `Rc`, `RefCell` across await points)
- Flag blocking I/O (`std::fs`, `std::thread::sleep`) in async functions
- Use `tokio::task::spawn_blocking` for CPU-intensive work

## Style

- Use inline format args (Rust 1.88+): `format!("{x}")`, not `format!("{}", x)` — expressions
  like `path.display()` still require positional args

## API Design

### Builder Pattern

- Use builder pattern for types with many optional parameters
- Builders should consume `self` and return `Self` for chaining

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

### Public API Documentation

- All public items must have doc comments
- Include examples in doc comments for complex APIs
- Document panics, errors, and safety requirements
