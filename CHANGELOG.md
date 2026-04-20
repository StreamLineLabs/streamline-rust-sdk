# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]


## [0.3.0] - 2026-04-20

### Added
- New `moonshot` Cargo feature exposing async HTTP clients for the Streamline
  Moonshot control plane (port `9094`):
  - `BranchesClient`, `ContractsClient`, `AttestationClient`, `SearchClient`,
    `MemoryClient` under `streamline_sdk::moonshot`.
- Shared `MoonshotOptions` + `MoonshotError`.

### Added
- `HttpAdmin` client for expanded admin operations via HTTP REST API (reqwest-based)
- `HttpAdmin::cluster_info()` — cluster overview including broker list
- `HttpAdmin::consumer_group_lag()` / `consumer_group_topic_lag()` — consumer group lag monitoring
- `HttpAdmin::inspect_messages()` / `latest_messages()` — message inspection by offset
- `HttpAdmin::metrics_history()` — server metrics history
- Model types: `ClusterInfo`, `ClusterBrokerInfo`, `ConsumerGroupLag`, `ConsumerLag`, `InspectedMessage`, `MetricPoint`

- feat: add TLS configuration support for producer
- feat: add circuit breaker pattern (`CircuitBreaker`) with configurable thresholds
- feat: wire circuit breaker into Producer for automatic failure protection
- fix: resolve tokio runtime panic on drop (2026-03-05)
- refactor: simplify error type hierarchy (2026-03-06)
- test: add integration tests for TLS connections (2026-03-06)
- **Changed**: update Cargo.toml dependency versions
- **Changed**: extract protocol codec into separate module
- **Testing**: add integration tests for TLS connections
- **Fixed**: resolve lifetime issue in consumer iterator
- **Added**: add async producer with tokio runtime

### Fixed
- Correct timeout handling in connection pool


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
- feat: implement telemetry span propagation in producer
- refactor: consolidate error handling types in consumer module
- feat: add metrics histogram for tail latency tracking
- refactor: clean up query builder public API surface
- fix: correct metrics overflow at high message throughput
