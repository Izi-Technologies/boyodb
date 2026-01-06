# Contributing to BoyoDB

Thank you for your interest in contributing to BoyoDB! This document provides guidelines and information for contributors.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Code Style](#code-style)
- [Submitting Changes](#submitting-changes)
- [Release Process](#release-process)

## Code of Conduct

This project follows a standard code of conduct. Be respectful, inclusive, and constructive in all interactions.

## Getting Started

### Prerequisites

- **Rust**: 1.75+ (install via [rustup](https://rustup.rs/))
- **Go**: 1.21+ (for Go bindings)
- **Node.js**: 18+ (for Node.js bindings)
- **Python**: 3.9+ (for Python driver)
- **Docker**: Optional, for containerized testing

### Quick Start

```bash
# Clone the repository
git clone https://github.com/loreste/boyodb.git
cd boyodb

# Build everything
make build-all

# Run tests
cargo test

# Start a development server
cargo run -p boyodb-server -- /tmp/boyodb-dev 127.0.0.1:8765
```

## Development Setup

### Building the Project

```bash
# Build core library only
cargo build -p boyodb-core

# Build server
cargo build -p boyodb-server

# Build CLI
cargo build -p boyodb-cli

# Build all Rust crates (release mode)
cargo build --release

# Build with all features
cargo build --all-features
```

### Environment Variables

```bash
# Logging level
export RUST_LOG=info          # Options: trace, debug, info, warn, error

# Backtrace for debugging
export RUST_BACKTRACE=1

# Number of Rayon threads (parallel queries)
export RAYON_NUM_THREADS=8
```

### IDE Setup

#### VS Code

Recommended extensions:
- rust-analyzer
- CodeLLDB (debugging)
- Even Better TOML
- crates

`.vscode/settings.json`:
```json
{
  "rust-analyzer.cargo.features": "all",
  "rust-analyzer.checkOnSave.command": "clippy"
}
```

#### IntelliJ/CLion

Install the Rust plugin and configure:
- Use rustfmt for formatting
- Enable clippy for linting

## Project Structure

```
boyodb/
├── crates/
│   ├── boyodb-core/          # Core engine library
│   │   ├── src/
│   │   │   ├── engine.rs    # Main database engine
│   │   │   ├── sql.rs       # SQL parser
│   │   │   ├── auth.rs      # Authentication/authorization
│   │   │   ├── cluster/     # HA cluster module
│   │   │   ├── replication.rs
│   │   │   └── wal.rs       # Write-ahead log
│   │   └── Cargo.toml
│   │
│   ├── boyodb-server/        # TCP/TLS server
│   │   ├── src/main.rs
│   │   └── tests/
│   │
│   ├── boyodb-cli/           # Command-line interface
│   │   └── src/shell.rs     # Interactive shell
│   │
│   └── boyodb-bench/         # Benchmarking tools
│
├── bindings/
│   ├── go/                  # CGO bindings
│   └── node/                # NAPI-RS bindings
│
├── drivers/                 # Language drivers (pure implementations)
│   ├── go/
│   ├── nodejs/
│   ├── python/
│   ├── rust/
│   ├── csharp/
│   └── php/
│
├── docs/                    # Documentation
├── .github/                 # CI/CD workflows
└── Makefile                 # Build automation
```

## Making Changes

### Branching Strategy

- `main` - Stable release branch
- `feature/*` - New features
- `fix/*` - Bug fixes
- `docs/*` - Documentation updates

### Commit Messages

Follow conventional commits format:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `perf`: Performance improvement
- `refactor`: Code refactoring
- `test`: Adding tests
- `chore`: Maintenance

Examples:
```
feat(sql): add window function support
fix(engine): resolve race condition in segment cache
docs(readme): update driver installation instructions
perf(query): optimize bloom filter lookup
```

### Code Organization

1. **Engine changes** (`boyodb-core/src/engine.rs`):
   - Add new query operations
   - Modify storage format
   - Update manifest structure

2. **SQL changes** (`boyodb-core/src/sql.rs`):
   - Add new SQL syntax
   - Modify parser behavior
   - Add new statement types

3. **Server changes** (`boyodb-server/src/main.rs`):
   - Add new request handlers
   - Modify wire protocol
   - Add new configuration options

4. **CLI changes** (`boyodb-cli/src/shell.rs`):
   - Add new commands
   - Modify output formats
   - Add completion support

## Testing

### Running Tests

```bash
# Run all tests
cargo test

# Run specific crate tests
cargo test -p boyodb-core
cargo test -p boyodb-server

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_name

# Run integration tests
cargo test --test integration_tests

# Run benchmarks
cargo bench
```

### Writing Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_feature() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), EngineConfig::default()).unwrap();

        // Test implementation
        assert!(result.is_ok());
    }

    #[test]
    fn test_error_case() {
        let result = some_function("invalid_input");
        assert!(matches!(result, Err(EngineError::InvalidArgument(_))));
    }
}
```

### Test Categories

1. **Unit tests**: Test individual functions
2. **Integration tests**: Test component interactions
3. **End-to-end tests**: Test full request/response cycles
4. **Performance tests**: Benchmark critical paths

## Code Style

### Rust Style

Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/):

```rust
// Use descriptive names
fn calculate_segment_size(entries: &[ManifestEntry]) -> u64

// Prefer explicit error handling
fn load_segment(path: &Path) -> Result<Segment, EngineError>

// Document public APIs
/// Creates a new database with the given name.
///
/// # Arguments
/// * `name` - The database name (must be valid identifier)
///
/// # Returns
/// * `Ok(())` on success
/// * `Err(EngineError::InvalidArgument)` if name is invalid
///
/// # Example
/// ```
/// db.create_database("analytics")?;
/// ```
pub fn create_database(&self, name: &str) -> Result<(), EngineError>
```

### Formatting

```bash
# Format code
cargo fmt

# Check formatting
cargo fmt -- --check

# Run clippy lints
cargo clippy -- -D warnings

# Fix clippy warnings automatically
cargo clippy --fix
```

### Clippy Configuration

The project uses these clippy lints (`.clippy.toml`):
```toml
avoid-breaking-exported-api = false
cognitive-complexity-threshold = 30
```

## Submitting Changes

### Pull Request Process

1. **Fork** the repository
2. **Create** a feature branch
3. **Make** your changes
4. **Test** thoroughly
5. **Format** and lint
6. **Push** to your fork
7. **Open** a pull request

### PR Checklist

- [ ] Tests pass (`cargo test`)
- [ ] Code is formatted (`cargo fmt`)
- [ ] No clippy warnings (`cargo clippy`)
- [ ] Documentation updated
- [ ] CHANGELOG updated (if applicable)
- [ ] Commit messages follow convention

### Review Process

1. Automated CI checks must pass
2. At least one maintainer review required
3. All comments must be addressed
4. Squash merge into main

## Release Process

### Version Numbering

We use [Semantic Versioning](https://semver.org/):
- MAJOR: Breaking API changes
- MINOR: New features (backward compatible)
- PATCH: Bug fixes

### Release Checklist

1. Update version in `Cargo.toml` files
2. Update CHANGELOG.md
3. Create release commit
4. Tag with version: `git tag v0.1.0`
5. Push tag: `git push origin v0.1.0`
6. CI builds and publishes releases

### Publishing

```bash
# Publish core library
cargo publish -p boyodb-core

# Publish CLI
cargo publish -p boyodb-cli
```

## Getting Help

- **Issues**: Open a GitHub issue for bugs or features
- **Discussions**: Use GitHub Discussions for questions
- **Security**: Report security issues privately

## Recognition

Contributors are recognized in:
- CHANGELOG.md for each release
- GitHub contributors page
- Release notes

Thank you for contributing!
