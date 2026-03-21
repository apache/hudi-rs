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

## Testing

- Unit tests in the same file as the code (`#[cfg(test)]` module)
- Integration tests in dedicated test modules or test crate
- Test both success and error paths
- Use descriptive test names: `test_<function>_<scenario>_<expected_behavior>`
- Use `#[tokio::test]` for async tests
