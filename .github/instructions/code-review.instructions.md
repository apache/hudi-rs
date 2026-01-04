---
applyTo: "**/*"
excludeAgent: "coding-agent"
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

# Code Review Instructions for Apache Hudi-rs

## Multi-Round Review Behavior

### Handling Subsequent Commits

When reviewing a PR that has been updated with new commits after a previous review:

1. **Check if previous comments are addressed**:
   - Compare the current code state against previous review comments
   - If a comment has been addressed by new changes, acknowledge resolution
   - Do NOT re-raise issues that have been fixed

2. **Focus on incremental changes**:
   - Prioritize reviewing code added or modified in the latest commits
   - Identify any new issues introduced by the fixes
   - Check if fixes introduced regressions elsewhere

3. **Comment resolution signals**:
   - If code now follows the suggested pattern → Comment is resolved
   - If test coverage has been added as requested → Comment is resolved
   - If documentation has been added → Comment is resolved
   - If error handling has been improved → Comment is resolved

4. **Acknowledge progress**:
   - When significant improvements have been made, acknowledge them
   - Be encouraging about progress while maintaining standards

### Review Comment Format

When leaving comments, use this severity classification:

- **🔴 Critical**: Must fix before merge (correctness, safety, breaking changes)
- **🟠 Important**: Should fix before merge (error handling, testing, docs)
- **🟡 Suggestion**: Nice to have, not blocking (style, optimization)
- **💬 Question**: Clarification needed, not necessarily an issue

## Apache Hudi-rs Specific Review Criteria

### Hudi Semantics

- Verify correct handling of Hudi timeline operations
- Check proper partition pruning logic
- Ensure file group/file slice handling is correct
- Validate commit timestamp parsing and comparison

### Arrow Integration

- Verify schema compatibility between Hudi and Arrow types
- Check proper memory management with RecordBatches
- Ensure efficient use of Arrow compute kernels
- Validate proper null handling

### DataFusion Integration

- Check TableProvider implementation correctness
- Verify filter pushdown implementation
- Ensure proper async execution patterns
- Validate scan parallelization

### Cloud Storage

- Check object_store usage patterns
- Verify proper credential handling (no hardcoded secrets)
- Ensure retry logic for transient failures
- Validate path handling across storage backends

## Review Checklist

### Before Approving, Verify:

- [ ] All CI checks pass
- [ ] Tests cover the changed functionality
- [ ] Public APIs have documentation
- [ ] Error handling uses Result, not panic
- [ ] No `.unwrap()` in non-test code
- [ ] Breaking changes are documented
- [ ] PR diff is under 1000 lines (or justified)

### For New Features:

- [ ] Feature is behind appropriate configuration if experimental
- [ ] Integration tests demonstrate the feature works end-to-end
- [ ] Python bindings updated if applicable
- [ ] README/docs updated if user-facing

### For Bug Fixes:

- [ ] Root cause is identified and explained
- [ ] Test case added that would have caught the bug
- [ ] Related areas checked for similar issues

## Patterns to Flag

### Always Flag (Critical)

```rust
// Unwrap in library code
let value = result.unwrap();  // 🔴 Use ? or proper error handling

// Panic in library code
panic!("unexpected state");   // 🔴 Return Result with error

// Blocking in async
std::thread::sleep(duration); // 🔴 Use tokio::time::sleep

// Hardcoded credentials
let key = "AKIAXXXXXXXX";     // 🔴 Security issue
```

### Usually Flag (Important)

```rust
// Missing error context
.map_err(HudiError::from)?   // 🟠 Add context about what failed

// Clone when borrow would work
fn process(data: Vec<u8>) { } // 🟠 Consider &[u8] if not consuming

// Large type on stack
let buffer: [u8; 1_000_000];  // 🟠 Use Vec or Box for large allocations

// Missing docs on pub items
pub fn important_function() { } // 🟠 Add doc comment
```

### Sometimes Flag (Suggestion)

```rust
// Could use iterator methods
let mut result = Vec::new();
for item in items {
    if predicate(&item) {
        result.push(transform(item));
    }
}
// 🟡 Consider: items.into_iter().filter(predicate).map(transform).collect()

// Nested Result handling
match result {
    Ok(inner) => match inner { ... }  // 🟡 Consider using and_then or ?
    Err(e) => ...
}
```

## Cross-File Impact Assessment

When changes touch these areas, expand review scope:

- **Core table implementation**: Check impacts on Python bindings
- **Public API surface (`hudi` crate)**: Check for breaking API changes
- **DataFusion integration**: Verify DataFusion compatibility
- **Schema types or conversions**: Check all serialization/deserialization paths
- **Configuration structs**: Verify backward compatibility
