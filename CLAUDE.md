# CLAUDE.md — Streamline Rust SDK

## Overview
Async Rust client SDK for [Streamline](https://github.com/streamlinelabs/streamline), built on Tokio. Communicates via the Kafka wire protocol on port 9092.

## Build & Test
```bash
cargo build               # Build
cargo test                # Run tests
cargo fmt --all -- --check  # Check formatting
cargo clippy --all-targets -- -D warnings  # Lint
```

## Architecture
```
src/
├── lib.rs                # Public API exports, header types
├── client.rs             # Streamline client with builder pattern
├── config.rs             # ClientConfig with builder
├── connection.rs         # Connection pool with auto-reconnect
├── producer.rs           # Producer with batching & compression
├── consumer.rs           # Consumer with group coordination
├── error.rs              # ErrorKind enum + Error struct with hints
├── telemetry.rs          # OpenTelemetry integration (feature-gated)
```

## Coding Conventions
- **Builder pattern**: `Streamline::builder().bootstrap_servers("...").build().await?`
- **Error handling**: Use `ErrorKind` enum + `Error::new()` with `.with_hint()`, `.with_source()`
- **No `.unwrap()` in production**: `#[warn(clippy::unwrap_used)]` enforced via `[lints.clippy]`
- **Async**: All I/O uses Tokio. Use `tokio::time::sleep`, never `std::thread::sleep`
- **Visibility**: Default private, `pub(crate)` for internal, `pub` only for API surface
- **Feature gates**: Compression, TLS, telemetry are optional features

## Error Handling Pattern
```rust
use streamline_client::{Streamline, Error, ErrorKind};

match client.produce("topic", b"hello").await {
    Ok(offset) => println!("Produced at {}", offset),
    Err(e) if e.is_retryable() => { /* retry */ },
    Err(e) => eprintln!("{}: {}", e, e.hint().unwrap_or_default()),
}
```

## Features
- `tokio-runtime` (default) — Full Tokio runtime
- `compression-lz4`, `compression-zstd`, `compression-snappy` — Compression codecs
- `tls` — TLS/mTLS via rustls
- `telemetry` — OpenTelemetry tracing (zero-cost when disabled)

## Testing
- Unit tests: Inline `#[cfg(test)] mod tests` in source files
- Integration tests: `tests/` directory
- Testcontainers: `testcontainers/` with Docker Compose
