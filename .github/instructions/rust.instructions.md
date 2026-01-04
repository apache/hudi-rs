---
applyTo: "**/*.rs"
---

# Rust Instructions for Apache Hudi-rs

## Error Handling Patterns (Critical)

### Must Use Result Over Panic

- Flag any `.unwrap()` or `.expect()` in non-test code as **Critical**
- Flag any `panic!()` macro in library code as **Critical**
- Exception: `unreachable!()` for truly impossible states is acceptable with a comment explaining why

```rust
// GOOD
fn parse_config(input: &str) -> Result<Config, HudiError> {
    serde_json::from_str(input).map_err(HudiError::from)
}

// BAD - Will panic on invalid input
fn parse_config(input: &str) -> Config {
    serde_json::from_str(input).unwrap()
}
```

### Error Context

- Prefer `anyhow::Context` or custom error types with context over bare `.map_err()`
- Error messages should be actionable and include relevant values

```rust
// GOOD
let file = File::open(&path)
    .with_context(|| format!("Failed to open table metadata at {}", path.display()))?;

// LESS GOOD
let file = File::open(&path).map_err(|e| HudiError::Io(e))?;
```

## Memory and Performance

### Avoid Unnecessary Allocations

- Flag unnecessary `.clone()` calls, especially on large types like `Vec<RecordBatch>`
- Prefer `&str` over `String` in function parameters when ownership isn't needed
- Use `Cow<'_, str>` when a function might or might not need to allocate

```rust
// GOOD - Takes reference
fn filter_partitions(partitions: &[String], predicate: &str) -> Vec<&String> { ... }

// LESS EFFICIENT - Clones unnecessarily
fn filter_partitions(partitions: Vec<String>, predicate: String) -> Vec<String> { ... }
```

### Arrow Memory Patterns

- Prefer Arrow compute kernels over manual iteration
- Use `arrow::compute::concat_batches` for combining batches
- Be mindful of memory when working with large datasets

```rust
// GOOD - Uses Arrow compute kernel
use arrow::compute::filter;
let filtered = filter(&batch, &predicate)?;

// AVOID - Manual iteration on Arrow arrays
let mut result = Vec::new();
for i in 0..array.len() {
    if predicate.value(i) {
        result.push(array.value(i));
    }
}
```

## Async Code Patterns

### Future Send Bounds

- Ensure async functions return `Send` futures for compatibility with multi-threaded executors
- Flag holding non-Send types (like `Rc`, `RefCell`) across await points

```rust
// GOOD - Future is Send
pub async fn read_files(paths: Vec<PathBuf>) -> Result<Vec<RecordBatch>> {
    let futures = paths.into_iter().map(|p| async move {
        read_file(&p).await
    });
    futures::future::try_join_all(futures).await
}
```

### Avoid Blocking in Async Context

- Flag blocking I/O operations in async functions
- Use `tokio::task::spawn_blocking` for CPU-intensive work

```rust
// GOOD
let result = tokio::task::spawn_blocking(move || {
    expensive_computation(&data)
}).await?;

// BAD - Blocks the async runtime
let result = expensive_computation(&data);  // In async function
```

## API Design

### Builder Pattern

- Use builder pattern for types with many optional parameters
- Builders should consume `self` and return `Self` for chaining

```rust
// GOOD - Follows project convention
pub struct TableBuilder {
    base_uri: String,
    options: HashMap<String, String>,
}

impl TableBuilder {
    pub fn from_base_uri(uri: impl Into<String>) -> Self { ... }

    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    pub async fn build(self) -> Result<Table> { ... }
}
```

### Public API Documentation

- All public items must have doc comments
- Include examples in doc comments for complex APIs
- Document panics, errors, and safety requirements

```rust
/// Reads a snapshot of the Hudi table.
///
/// # Arguments
///
/// * `filters` - Optional partition filters in the format `(column, op, value)`
///
/// # Errors
///
/// Returns an error if:
/// - The table path is invalid
/// - The table metadata cannot be read
/// - Schema parsing fails
///
/// # Examples
///
/// ```rust
/// let batches = table.read_snapshot(&[("city", "=", "san_francisco")]).await?;
/// ```
pub async fn read_snapshot(&self, filters: &[(&str, &str, &str)]) -> Result<Vec<RecordBatch>> {
    // ...
}
```

## Testing

### Test Organization

- Unit tests in `#[cfg(test)] mod tests` at bottom of file
- Integration tests in dedicated test modules
- Use `#[tokio::test]` for async tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_read_snapshot_with_filters() {
        let table = create_test_table().await;
        let batches = table.read_snapshot(&[("city", "=", "test")]).await.unwrap();
        assert!(!batches.is_empty());
    }

    #[test]
    fn test_parse_config_invalid_json() {
        let result = parse_config("not json");
        assert!(result.is_err());
    }
}
```

### Test Both Paths

- Test success cases AND error cases
- Test edge cases (empty input, boundary values)
- Test that errors contain useful information
