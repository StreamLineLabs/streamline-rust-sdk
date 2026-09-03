# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Security
- Redact SASL credentials from `Debug` output.
- Reject unimplemented TLS, SASL, compression, transaction, consumer-group,
  latest-offset, Kafka Admin, and SQL execution paths instead of simulating
  success.
- Validate producer correlation IDs, broker errors, response routing, and
  offsets before returning successful metadata.
- Validate consumer fetch response correlation IDs and reject responses
  reporting a broker-wide error instead of treating them as "no messages".
- Return per-partition fetch errors as errors instead of an empty successful
  poll, so leader changes, offset-out-of-range, and authorization failures can
  no longer be mistaken for "no new messages".
- Parse fetch responses with fully fallible bounds, count, and length checks;
  validate every record batch CRC-32C; reject truncated responses and trailing
  bytes at the response, record-set, batch, and record levels; and return
  `ErrorKind::Unsupported` for compressed record batches rather than
  mis-parsing compressed payloads as records.
- Distinguish nullable and non-null Fetch arrays: `-1` is rejected for required
  response/partition arrays and accepted only for nullable aborted-transaction
  arrays. Ten-byte varints reject a final payload greater than one.
- Resolve the default `earliest` consumer position with Kafka ListOffsets
  instead of assuming retained logs begin at offset zero. Fetch
  `OFFSET_OUT_OF_RANGE` has its own non-evicting error kind.
- Bind attestation verification to the record it travels with: the envelope's
  topic, partition, offset, and SHA-256 payload digest must match the consumed
  record, and the envelope's `key_id` must resolve through an explicitly
  trusted key map or `KeyResolver`. Added `Verifier::require_verified` for
  callers that want a hard error instead of an unverified result.
- Apply the configured `request_timeout` to complete producer and consumer
  broker exchanges, and evict pooled connections after a timeout, protocol
  violation, or I/O failure so a desynchronized socket is never reused.
- Make exchanges cancellation-safe by taking the stream out of its pool slot
  and restoring it only after one complete valid response. Dropped futures
  discard the partial stream instead of returning it to the pool.
- Build HTTP admin, schema registry, semantic search, moonshot, and query URLs
  with `reqwest::Url` path and query APIs, percent-encoding caller-supplied
  identifiers and rejecting `.`/`..` path segments everywhere.
- Skip transaction control batches instead of decoding their markers as
  application records, and reject fetch responses that contain a partition the
  request never asked for.
- Rename `VerificationResult::contract_id` to `unverified_contract_id`: the
  field travels in the envelope but is not covered by the attestation
  signature, so it must not be used for trust decisions.

### Fixed
- Encode producer headers and honor uniform explicit batch partitions.
- Decode consumer record headers from fetch responses instead of discarding
  them, so headers round-trip through produce and consume.
- Drop records below the offset actually requested for their partition.
  Fetches are record-batch aligned, so a broker returns the whole batch
  containing the requested offset; those redeliveries were previously handed
  to callers. Poll offsets now advance past fully filtered batches, so a poll
  still makes progress.
- Correct two Fetch v11 request wire fields that shifted every following byte
  or sent a null where the protocol requires a value: `current_leader_epoch`
  is now an INT32 (was INT64) and `rack_id` is now an empty non-null STRING
  (was the null marker). Added golden byte-level tests.
- An explicit `seek` before the first poll now removes that partition from
  pending earliest-offset resolution, so the requested offset is fetched
  directly instead of being overwritten by a ListOffsets lookup.
- Testcontainers readiness now probes `GET /health/live` on port 9094 instead
  of waiting for a nonexistent `Server started` stdout line.
- Repair minimal/all-feature builds, release gates, CodeQL, SBOM/provenance,
  MSRV policy, and testcontainers publication metadata.

### Changed
- `QueryClient::query_url` and `QueryClient::explain_url` now return
  `Result<String>` so an invalid base URL fails closed.
- `Verifier::new` now takes the trusted key ID alongside the public key;
  `VerificationResult` gained a `failure_reason` field.
- Tagged publication is gated on a live conformance suite
  (`tests/live_conformance.rs`) run against a container started from the
  immutable image digest in the `STREAMLINE_TEST_IMAGE` repository variable.
  A missing image, a mutable tag, an unpullable image, or an executed-test
  count that does not match the declared suite hard-blocks the release.
- `streamline-testcontainers` is explicitly source-only (`publish = false`).
  Its crates.io install and release instructions were removed, and the
  tag-triggered publish workflow was replaced by a source verification
  workflow, because publishing it would require a dependency-first release
  order this repository does not implement.
- `StreamlineImage` requires an explicit image reference: `Default` was
  removed, `StreamlineImageBuilder::build` returns `Result`, digests are
  validated, and `StreamlineImage::from_env` reads `STREAMLINE_TEST_IMAGE`
  without falling back to a default image or the `0.3.0` tag.

## [0.3.0] - 2026-04-20

### Added
- New `moonshot` Cargo feature exposing async HTTP clients for the Streamline
  Moonshot control plane (port `9094`):
  - `BranchesClient`, `ContractsClient`, `AttestationClient`, `SearchClient`,
    `MemoryClient` under `streamline_client::moonshot`.
- Shared `MoonshotOptions` + `MoonshotError`.

### Added
- `HttpAdmin` client for expanded admin operations via HTTP REST API (reqwest-based)
- `HttpAdmin::cluster_info()` — cluster overview including broker list
- `HttpAdmin::consumer_group_lag()` / `consumer_group_topic_lag()` — consumer group lag monitoring
- `HttpAdmin::inspect_messages()` / `latest_messages()` — message inspection by offset
- `HttpAdmin::metrics_history()` — server metrics history
- Model types: `ClusterInfo`, `ClusterBrokerInfo`, `ConsumerGroupLag`, `ConsumerLag`, `InspectedMessage`, `MetricPoint`

- feat: add circuit breaker pattern (`CircuitBreaker`) with configurable thresholds
- feat: wire circuit breaker into Producer for automatic failure protection
- fix: resolve tokio runtime panic on drop (2026-03-05)
- refactor: simplify error type hierarchy (2026-03-06)
- **Changed**: update Cargo.toml dependency versions
- **Changed**: extract protocol codec into separate module
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
