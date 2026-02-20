# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-02-18

### Added
- `StreamlineClient` with builder pattern and Tokio async runtime
- Generic `Producer<K, V>` and `Consumer<K, V>` with type-safe keys/values
- `Admin` client for topic and group management
- Custom `Error` type with kind, message, hint, and source
- Feature flags for compression (lz4, zstd, snappy) and TLS
- Testcontainers integration for testing
- Examples for producer and consumer usage

### Infrastructure
- CI pipeline with cargo test, clippy, fmt, and coverage reporting
- CodeQL security scanning
- Release workflow with crates.io publishing
- Release drafter for automated release notes
- Dependabot for dependency updates
- CONTRIBUTING.md with development setup guide
- Security policy (SECURITY.md)
- EditorConfig for consistent formatting
- Issue templates for bug reports and feature requests

## [0.1.0] - 2026-02-18

### Added
- Initial release of Streamline Rust SDK
- Tokio async runtime with full feature support
- Type-safe generic producer and consumer
- Apache 2.0 license
