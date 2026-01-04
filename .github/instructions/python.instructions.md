---
applyTo: "python/**"
---

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

# Python Bindings Instructions

## PyO3 Patterns

### Error Handling

- Convert Rust errors to Python exceptions properly
- Use appropriate Python exception types

```rust
// GOOD
#[pyfunction]
fn read_table(path: &str) -> PyResult<PyObject> {
    let result = hudi_core::read_table(path)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to read table: {}", e)))?;
    // ...
}

// BAD - Panics on error
#[pyfunction]
fn read_table(path: &str) -> PyObject {
    let result = hudi_core::read_table(path).unwrap();  // Will panic!
    // ...
}
```

### GIL Management

- Release GIL for long-running operations
- Be careful with Python object access outside GIL

```rust
// GOOD - Release GIL for I/O
fn read_files(&self, py: Python<'_>, paths: Vec<String>) -> PyResult<Vec<PyObject>> {
    let batches = py.allow_threads(|| {
        // This runs without GIL
        self.inner.read_files_blocking(&paths)
    })?;
    // Convert to Python with GIL held
    // ...
}
```

### PyArrow Integration

- Use proper Arrow<->PyArrow conversion
- Leverage zero-copy when possible

```rust
// Use arrow's Python integration
use arrow::pyarrow::ToPyArrow;

fn to_pyarrow_batch(py: Python<'_>, batch: &RecordBatch) -> PyResult<PyObject> {
    batch.to_pyarrow(py)
}
```

## Python API Design

### Consistency with Python Conventions

- Use `snake_case` for function/method names
- Use docstrings for all public functions

### Type Hints

- Add type hints to Python stub files (`.pyi`)
- Ensure compatibility with type checkers

## Testing Python Bindings

### Test Patterns

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

## Review Checklist for Python Changes

- [ ] Rust panics don't propagate to Python (caught and converted)
- [ ] GIL is released for blocking operations
- [ ] Type stubs updated for API changes
- [ ] Python tests added for new functionality
- [ ] Memory management is correct (no leaks)
