# GitHub Copilot Instructions for Apache Hudi-rs

## Project Overview

Apache Hudi-rs is the native Rust implementation of Apache Hudi, providing Python and C++ API bindings. The project focuses on standardizing core Apache Hudi APIs and broadening Hudi integration in the data ecosystem.

### Key Dependencies & Ecosystem

- **Apache Arrow**: In-memory columnar format (via `arrow-rs` crate)
- **Apache DataFusion**: Query execution engine integration
- **Apache Parquet**: Columnar storage format
- **PyO3**: Python bindings
- **CXX**: C++ FFI bindings
- **Tokio**: Async runtime

## Commit Conventions

This project uses [Conventional Commits](https://www.conventionalcommits.org). PR titles must follow this format:

```
<type>(<scope>): <description>
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`

Examples:
- `feat(core): add support for MOR table reads`
- `fix(python): handle null partition values correctly`
- `docs: update API documentation for HudiTable`

## Development Setup

See [CONTRIBUTING.md](../CONTRIBUTING.md) for full details.

### Prerequisites

- Rust (see `rust-toolchain.toml`)
- [uv](https://docs.astral.sh/uv/) - Python package manager
- Python (see `python/pyproject.toml` for version)

### Quick Start

```bash
# Setup Python virtual environment
make setup-venv
source .venv/bin/activate

# Install for development (builds Rust + Python bindings)
make develop

# Format and lint
make format check

# Run all tests
make test
```

## Code Review Guidelines

### Multi-Round Review Behavior

When reviewing subsequent commits on a PR:

1. **Auto-resolve addressed comments**: If a new commit addresses a previous review comment, acknowledge it is resolved rather than re-raising the same issue.
2. **Focus on new changes**: Prioritize reviewing newly added or modified code in the latest commits.
3. **Track incremental progress**: Recognize when feedback has been incorporated and provide confirmation.
4. **Avoid duplicate comments**: Do not repeat comments that have been addressed in subsequent commits.

### Review Priority (High to Low)

1. **Correctness**: Logic errors, data races, memory safety
2. **API Design**: Public API consistency, breaking changes
3. **Error Handling**: Proper use of `Result`, meaningful error messages
4. **Performance**: Unnecessary allocations, inefficient patterns
5. **Testing**: Test coverage, edge cases
6. **Documentation**: Public API docs, complex logic explanation
7. **Style**: Idiomatic Rust, consistency with codebase

## What to Flag in Reviews

### Critical Issues (Must Fix)

- Panics in library code (use `Result` instead)
- Unhandled errors (`.unwrap()` / `.expect()` in non-test code)
- Data races or unsafe code without justification
- Breaking public API changes without deprecation
- Missing tests for new functionality

### Important Issues (Should Fix)

- Inefficient patterns (unnecessary `.clone()`, allocations in hot paths)
- Missing documentation on public APIs
- Large functions that should be split
- Missing `#[must_use]` on functions returning important values
- Blocking calls in async context

### Suggestions (Nice to Have)

- More idiomatic Rust patterns
- Additional test cases for edge cases
- Performance optimizations
- Code organization improvements

## Rust Conventions

### Error Handling

- Use `Result<T, E>` for fallible operations, never panic in library code
- Propagate errors with `?` operator
- Add context to errors when crossing module boundaries
- Use `thiserror` for defining error types in library crates

### Async Patterns

- Use `tokio` as the async runtime
- Prefer `async fn` over returning `impl Future`
- Use `#[tokio::test]` for async tests
- Avoid blocking calls (`std::fs`, `std::thread::sleep`) in async code

### Testing

- Unit tests in the same file as the code (`#[cfg(test)]` module)
- Integration tests in dedicated test modules or test crate
- Test both success and error paths
- Use descriptive test names: `test_<function>_<scenario>_<expected_behavior>`
