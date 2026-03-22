---
applyTo: "**/*"
excludeAgent: "coding-agent"
---

# Code Review Instructions for Apache Hudi-rs

## Multi-Round Review Behavior

When reviewing updated PRs:

- Do NOT re-raise issues addressed by new commits
- Focus on code added or modified in latest commits
- Check if fixes introduced regressions elsewhere
- Acknowledge progress while maintaining standards

### Severity Classification

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

- 🔴 `.unwrap()` / `.expect()` / `panic!()` in non-test code
- 🔴 Blocking calls (`std::thread::sleep`, `std::fs`) in async context
- 🔴 Hardcoded credentials or secrets
- 🟠 Missing error context (bare `.map_err()` without message)
- 🟠 Unnecessary `.clone()`, taking ownership when borrow suffices
- 🟠 Missing doc comments on public items
- 🟡 Imperative loops replaceable with iterator chains
- 🟡 Nested `match` on `Result` replaceable with `and_then` or `?`

## Cross-File Impact

- **Core table changes** → check Python/C++ bindings
- **Public API (`hudi` crate)** → check for breaking changes
- **DataFusion integration** → verify compatibility
- **Schema/type conversions** → check serialization paths
- **Configuration structs** → verify backward compatibility
