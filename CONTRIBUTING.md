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

- Rust 1.80 or later (install via [rustup](https://rustup.rs/))
- Rust 1.88 or later when changing the `testcontainers/` companion crate

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

# Verify the minimal and complete feature graphs
cargo check --no-default-features
cargo check --all-features

# Run with output visible
cargo test -- --nocapture

# Run the offline public-API conformance suite
cargo test --test conformance

# Run with verbose output
cargo test -- --show-output
```

### Integration and live conformance tests

Both suites require Docker (or a reachable broker) and an **explicit image
reference**. There is no default image and no fallback: a missing reference is
a hard error, never a skip.

Container smoke test:

```bash
STREAMLINE_TEST_IMAGE=ghcr.io/streamlinelabs/streamline@sha256:<64 hex digits> \
  cargo test --manifest-path testcontainers/Cargo.toml \
  tests::container_starts_and_exposes_ports -- --ignored --exact
```

Live SDK conformance (produce/fetch round trip, fail-closed behaviour) against
a running broker:

```bash
STREAMLINE_BOOTSTRAP_SERVERS=127.0.0.1:9092 \
  cargo test --test live_conformance -- --ignored
```

Tagged releases run the live suite against a container started from the
immutable digest in the `STREAMLINE_TEST_IMAGE` repository variable, and
verify that the number of executed live tests matches the number declared in
`tests/live_conformance.rs`. Do not convert that gate into an ignored or
non-blocking step.

These tests still cannot provision isolated topics through this crate, because
the Kafka Admin APIs remain intentionally unsupported; the topic must exist or
be auto-created by the broker.

## Linting & Formatting

```bash
# Format code
cargo fmt

# Check formatting (CI mode)
cargo fmt --all -- --check

# Run clippy lints
cargo clippy --all-targets -- -D warnings
cargo clippy --all-features --all-targets -- -D warnings

# Build docs
cargo doc --no-deps

# Verify release packaging
cargo package
```

SDK releases use a `v<streamline-client-version>` tag, and publication is
gated on the live conformance job described above.

The companion `streamline-testcontainers` crate is **source-only**: it sets
`publish = false`, is not on crates.io, and has no release tag or publish
workflow. It depends on `streamline-client` by path, so publishing it would
require a dependency-first release order that this repository does not
implement. `.github/workflows/verify-testcontainers.yml` only builds, lints,
and tests it from source, and fails if `publish = false` is removed or if the
documentation starts advertising a registry dependency.

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
