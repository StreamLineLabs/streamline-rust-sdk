# Contributing to Streamline Rust SDK

Thank you for your interest in contributing to the Streamline Rust SDK! This guide will help you get started.

## Getting Started

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests and linting
5. Commit your changes (`git commit -m "Add my feature"`)
6. Push to your fork (`git push origin feature/my-feature`)
7. Open a Pull Request

## Prerequisites

- Rust 1.75 or later (install via [rustup](https://rustup.rs/))

## Development Setup

```bash
# Clone your fork
git clone https://github.com/<your-username>/streamline-rust-sdk.git
cd streamline-rust-sdk

# Build
cargo build

# Run tests
cargo test
```

## Running Tests

```bash
# Run all tests
cargo test

# Run with output visible
cargo test -- --nocapture

# Run a specific test
cargo test test_producer_send

# Run with verbose output
cargo test -- --show-output
```

### Integration Tests

Integration tests require a running Streamline server:

```bash
# Start the server
docker compose -f docker-compose.test.yml up -d

# Run integration tests
cargo test --features integration-tests

# Stop the server
docker compose -f docker-compose.test.yml down
```

## Linting & Formatting

```bash
# Format code
cargo fmt

# Check formatting (CI mode)
cargo fmt --all -- --check

# Run clippy lints
cargo clippy --all-targets -- -D warnings

# Build docs
cargo doc --no-deps
```

## Code Style

- Follow Rust conventions and the existing code patterns
- Use `thiserror` for error types
- Propagate errors with `?` — avoid `.unwrap()` in library code
- Add doc comments (`///`) for all public items
- Default to private visibility; use `pub(crate)` for internal sharing

## Pull Request Guidelines

- Write clear commit messages
- Add tests for new functionality
- Update documentation if needed
- Ensure `cargo fmt`, `cargo clippy`, and `cargo test` pass before submitting

## Reporting Issues

- Use the **Bug Report** or **Feature Request** issue templates
- Search existing issues before creating a new one
- Include reproduction steps for bugs

## Code of Conduct

All contributors are expected to follow our [Code of Conduct](https://github.com/streamlinelabs/.github/blob/main/CODE_OF_CONDUCT.md).

## License

By contributing, you agree that your contributions will be licensed under the Apache-2.0 License.
