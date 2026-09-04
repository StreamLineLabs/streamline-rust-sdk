# Streamline Rust Client

[![CI](https://github.com/streamlinelabs/streamline-rust-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/streamlinelabs/streamline-rust-sdk/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/streamline-client.svg)](https://crates.io/crates/streamline-client)
[![docs.rs](https://docs.rs/streamline-client/badge.svg)](https://docs.rs/streamline-client)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.80-orange.svg)](https://www.rust-lang.org/)

Async Rust client for the [Streamline](https://github.com/streamlinelabs/streamline)
streaming platform. Version `0.4.0` uses Tokio and the Kafka wire protocol for
plaintext produce and direct-partition consume operations.

## Release-readiness status

The SDK fails closed when a public surface is not backed by complete protocol
support. Version `0.4.0` does **not** claim support for:

- Kafka Admin operations
- consumer group coordination or offset commits
- latest-offset lookup or `seek_to_end`
- Kafka transactions
- idempotent production
- broker TLS or SASL authentication
- record compression (producing or consuming compressed batches)
- StreamQL query execution

Those calls return `ErrorKind::Unsupported` before reporting success. The
`tls`, `sasl`, and `compression-*` Cargo features remain as compatibility
feature names, but they do not enable transport or codec implementations.

## Supported functionality

- Plaintext Kafka v7 produce requests with broker acknowledgments
- Producer headers and uniform explicit partition assignment for batches
- Bounds-checked produce response validation, including correlation and broker
  error codes
- Direct consumer fetches from explicitly assigned partitions. The default
  `earliest` policy resolves each partition's retained log start with Kafka
  ListOffsets before the first fetch; callers can also supply an offset with
  `seek`
- Fully bounds-checked fetch response parsing: record batch CRC-32C validation,
  rejection of truncated or trailing bytes, and `ErrorKind::Unsupported` for
  compressed record batches (consumer-side decompression is not implemented)
- Consumer fetch responses decode record headers and validate correlation IDs;
  response-wide *and* per-partition broker errors are returned as errors, never
  as an empty poll
- Producer and consumer broker exchanges are bounded by the configured
  `request_timeout`, and connections that time out or violate the protocol are
  evicted from the pool instead of being reused. Cancellation also drops an
  in-flight stream, preventing partial responses from desynchronizing the next
  exchange
- HTTP paths and query strings are built with `reqwest::Url`, so topic,
  subject, group, and branch identifiers are percent-encoded and `.`/`..` path
  segments are rejected
- HTTP Admin APIs through `HttpAdmin` (default `http-admin` feature)
- Schema Registry APIs (default `schema-registry` feature)
- Circuit breaker, metrics, and optional OpenTelemetry instrumentation
- Optional Moonshot HTTP clients and attestation verification

## Installation

```toml
[dependencies]
streamline-client = "0.4.0"
```

For a minimal core build without HTTP clients or a full Tokio runtime:

```toml
[dependencies]
streamline-client = { version = "0.4.0", default-features = false }
```

### Feature flags

| Feature | Default | Status |
|---|---:|---|
| `tokio-runtime` | yes | Enables Tokio's full runtime feature set |
| `http-admin` | yes | Enables implemented HTTP Admin operations |
| `schema-registry` | yes | Enables the HTTP Schema Registry client |
| `schema-registry-tls` | no | Adds rustls HTTPS support to Schema Registry |
| `telemetry` | no | Enables OpenTelemetry integration |
| `moonshot` | no | Enables Moonshot HTTP clients |
| `attestation` | no | Enables Ed25519 attestation verification |
| `tls`, `sasl` | no | Compatibility gates; broker transport rejects use |
| `compression-lz4`, `compression-zstd`, `compression-snappy` | no | Compatibility gates; producer rejects use |

## Producer

Topics must already exist because Kafka Admin operations are not implemented.

```rust,no_run
use streamline_client::Streamline;

#[tokio::main]
async fn main() -> Result<(), streamline_client::Error> {
    let client = Streamline::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .await?;

    let metadata = client.produce("events", "event-1", "hello").await?;
    println!(
        "produced to {}:{} at offset {}",
        metadata.topic, metadata.partition, metadata.offset
    );
    Ok(())
}
```

### Headers

```rust,no_run
use streamline_client::{Headers, Streamline};

# async fn example(client: &Streamline) -> streamline_client::Result<()> {
let headers = Headers::builder()
    .add("trace-id", b"abc-123")
    .add("content-type", b"application/json")
    .build();

client
    .produce_with_headers("events", "event-1", "{}", headers)
    .await?;
# Ok(())
# }
```

### Batches and explicit partitions

A batch can leave every partition unassigned (partition `0` is used) or assign
the same nonnegative partition to every record. Mixed, conflicting, or negative
assignments are rejected.

```rust,no_run
use streamline_client::{ProducerRecord, Streamline};

# async fn example(client: &Streamline) -> streamline_client::Result<()> {
let producer = client.producer::<String, String>();
let records = vec![
    ProducerRecord::new("a".to_string(), "one".to_string()).with_partition(2),
    ProducerRecord::new("b".to_string(), "two".to_string()).with_partition(2),
];

let metadata = producer.send_batch("events", records).await?;
assert!(metadata.iter().all(|record| record.partition == 2));
# Ok(())
# }
```

Only `ProducerConfig::compression = "none"` and
`ProducerConfig::idempotent = false` are supported in `0.4.0`.

## Direct consumer

Consumers require explicit partition assignment, `auto_offset_reset("earliest")`,
and disabled auto-commit. Group IDs, automatic commits, and latest-offset
resolution return `ErrorKind::Unsupported`. The `earliest` policy uses
ListOffsets to resolve the actual retained log start rather than assuming
offset zero.

```rust,no_run
use std::time::Duration;
use streamline_client::Streamline;

#[tokio::main]
async fn main() -> Result<(), streamline_client::Error> {
    let client = Streamline::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .await?;

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>("events")
        .partitions(vec![0])
        .auto_offset_reset("earliest")
        .enable_auto_commit(false)
        .build()
        .await?;

    consumer.subscribe().await?;
    let records = consumer.poll(Duration::from_secs(1)).await?;
    for record in records {
        println!("{}:{}", record.partition, record.offset);
    }
    Ok(())
}
```

Use `consumer.seek(partition, offset).await` for an explicit starting offset.

## HTTP Admin

`HttpAdmin` is enabled by the default `http-admin` feature and communicates with
the Streamline HTTP API. It is separate from `client.admin()`, whose Kafka Admin
methods intentionally return `ErrorKind::Unsupported`.

```rust,no_run
use streamline_client::HttpAdmin;

# async fn example() -> streamline_client::Result<()> {
let admin = HttpAdmin::new("http://localhost:9094");
let cluster = admin.cluster_info().await?;
println!("cluster {} has {} brokers", cluster.cluster_id, cluster.brokers.len());
# Ok(())
# }
```

## Schema Registry

```rust,no_run
use streamline_client::schema::{SchemaRegistryClient, SchemaType};

# async fn example() -> streamline_client::Result<()> {
let registry = SchemaRegistryClient::new("http://localhost:9094");
let schema_id = registry
    .register(
        "events-value",
        r#"{"type":"object","properties":{"id":{"type":"integer"}}}"#,
        SchemaType::Json,
    )
    .await?;
println!("registered schema {schema_id}");
# Ok(())
# }
```

Disable the default features if these HTTP surfaces are not needed.

## Explicit unsupported errors

```rust
use streamline_client::{ErrorKind, ProducerRecord, Streamline};

# async fn example() -> streamline_client::Result<()> {
let client = Streamline::builder()
    .bootstrap_servers("localhost:9092")
    .build()
    .await?;
let mut producer = client.producer::<String, String>();

let error = producer.begin_transaction().unwrap_err();
assert_eq!(error.kind, ErrorKind::Unsupported);

let query = streamline_client::query::QueryClient::new("http://localhost:9094");
let request = streamline_client::query::QueryRequest::new("SELECT 1");
let error = query.execute(&request).await.unwrap_err();
assert_eq!(error.kind, ErrorKind::Unsupported);

let _record = ProducerRecord::new("key".to_string(), "value".to_string());
# Ok(())
# }
```

## Errors and secret handling

Errors contain an `ErrorKind`, message, optional remediation hint, and optional
source error. `SaslConfig` and nested `StreamlineConfig` debug output redact
both usernames and passwords.

```rust
use streamline_client::{ErrorKind, Streamline};

# async fn example() -> streamline_client::Result<()> {
let client = Streamline::builder()
    .bootstrap_servers("localhost:9092")
    .build()
    .await?;

match client.produce("events", "key", "value").await {
    Ok(metadata) => println!("offset {}", metadata.offset),
    Err(error) if error.is_retryable() => eprintln!("retryable: {error}"),
    Err(error) if error.kind == ErrorKind::Unsupported => {
        eprintln!("unsupported: {error}")
    }
    Err(error) => eprintln!("produce failed: {error}"),
}
# Ok(())
# }
```

## Testcontainers

The companion `streamline-testcontainers` crate lives in `testcontainers/`. It is
**source-only: it is not published to crates.io** (`publish = false`), because it
depends on `streamline-client` by path and this repository does not implement a
dependency-first release workflow. Depend on it by path or git revision:

```toml
[dev-dependencies]
streamline-testcontainers = { path = "path/to/streamline-rust-sdk/testcontainers" }
testcontainers = "=0.28.0"
tokio = { version = "1", features = ["full"] }
```

The image reference is always explicit: there is no default image, no default
tag, and no `Default` implementation, so nothing silently pulls an image that may
not exist. Pin an immutable digest:

```rust,ignore
let image = streamline_testcontainers::StreamlineImage::builder()
    .image("ghcr.io/streamlinelabs/streamline@sha256:<64 hex digits>")
    .build()?;
```

See [`testcontainers/README.md`](testcontainers/README.md). The live smoke test
requires Docker and a digest-pinned image:

```bash
STREAMLINE_TEST_IMAGE=ghcr.io/streamlinelabs/streamline@sha256:<64 hex digits> \
  cargo test --manifest-path testcontainers/Cargo.toml \
  tests::container_starts_and_exposes_ports -- --ignored --exact
```

## Live conformance and release gating

`tests/live_conformance.rs` exercises produce, fetch, and fail-closed behaviour
against a real broker. The tests are `#[ignore]`d by default and require
`STREAMLINE_BOOTSTRAP_SERVERS`:

```bash
STREAMLINE_BOOTSTRAP_SERVERS=127.0.0.1:9092 cargo test --test live_conformance -- --ignored
```

Tagged publication (`.github/workflows/release.yml`) runs this suite against a
container started from the immutable digest in the `STREAMLINE_TEST_IMAGE`
repository variable, and only then packages and publishes. The gate fails —
rather than skipping — when the variable is unset, when it is a mutable tag
instead of a `@sha256:` digest, when the image cannot be pulled, or when the
number of executed live tests does not match the number declared in the suite.

## Development and release checks

The main SDK's minimum supported Rust version is **1.80**. The companion
`streamline-testcontainers` crate requires Rust **1.88** because it follows the
current secure `testcontainers` dependency line.

```bash
cargo fmt --all -- --check
cargo check --no-default-features
cargo check --all-features
cargo clippy --no-default-features --lib --tests -- -D warnings
cargo clippy --all-features --all-targets -- -D warnings
cargo test --no-default-features --lib --tests
cargo test --all-features --all-targets
cargo test --all-features --doc
cargo package
```

Security policy checks use:

```bash
cargo audit
cargo deny check
```

## Security

Report vulnerabilities according to [`SECURITY.md`](SECURITY.md). Do not open a
public issue for an undisclosed vulnerability.

## License

Apache-2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
