# Python Conventions

## Linting & Formatting

Run `make format-python check-python` before submitting to match CI.

`ruff` (`E4 E7 E9 F I`, target `py310`) + `mypy --strict` over `hudi/*.py`. `snake_case`,
docstrings on public APIs, type hints in `python/hudi/_internal.pyi`.

## PyO3 Patterns

### Error Handling

Convert Rust errors to specific Python exceptions; never let a panic surface to Python.

```rust
// GOOD
#[pyfunction]
fn read_table(path: &str) -> PyResult<PyObject> {
    let result = hudi_core::read_table(path)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to read table: {e}")))?;
    // ...
}

// BAD — panics on error
#[pyfunction]
fn read_table(path: &str) -> PyObject {
    let result = hudi_core::read_table(path).unwrap();
    // ...
}
```

### GIL Management

Release the GIL with `py.allow_threads(...)` for blocking I/O.

```rust
fn read_files(&self, py: Python<'_>, paths: Vec<String>) -> PyResult<Vec<PyObject>> {
    let batches = py.allow_threads(|| {
        self.inner.read_files_blocking(&paths)
    })?;
    // ...
}
```

### PyArrow Integration

Use `arrow::pyarrow::ToPyArrow` for zero-copy Arrow <-> PyArrow.

## Testing

Tests in `python/tests/`.

```python
import pytest
from hudi import HudiTableBuilder

def test_read_snapshot_with_filters():
    table = HudiTableBuilder.from_base_uri("/tmp/test").build()
    batches = table.read_snapshot(filters=[("city", "=", "test")])
    assert len(batches) > 0

def test_invalid_path_raises():
    with pytest.raises(RuntimeError, match="Failed to"):
        HudiTableBuilder.from_base_uri("/nonexistent").build()
```
