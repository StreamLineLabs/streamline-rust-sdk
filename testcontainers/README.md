# Streamline Testcontainers (Rust)

Testcontainers module for [Streamline](https://github.com/streamlinelabs/streamline).

> **Source-only crate.** `streamline-testcontainers` is **not published to crates.io** and has no
> release workflow. `publish = false` is set in `Cargo.toml`. Depend on it by path or git revision
> from this repository.

## Features

- Kafka-compatible container for integration testing
- Builder pattern for configuration
- Async-first API with Tokio
- Health, metrics, and info endpoint helpers over the container's HTTP port

## Requirements

- Rust 1.88 or later (this crate follows the current `testcontainers` dependency line)
- A running Docker daemon for anything that actually starts a container
- **An explicit image reference.** There is no default image, no default tag, and no `Default`
  implementation. This crate does not know which Streamline image exists in your registry, so it
  refuses to guess.

## Adding the dependency

By path, from a checkout of this repository:

```toml
[dev-dependencies]
streamline-testcontainers = { path = "../streamline-rust-sdk/testcontainers" }
testcontainers = "=0.28.0"
tokio = { version = "1", features = ["full"] }
```

Or by git revision:

```toml
[dev-dependencies]
streamline-testcontainers = { git = "https://github.com/streamlinelabs/streamline-rust-sdk", rev = "<commit-sha>" }
```

To also pull in the Streamline client SDK, enable the `client` feature.

## Usage

### Basic test setup

The image reference must be supplied explicitly, and an immutable digest is strongly preferred
because a tag can be repointed at different content:

```rust
use streamline_testcontainers::{StreamlineImage, bootstrap_servers};
use testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn test_with_streamline() {
    let image = StreamlineImage::builder()
        .image("ghcr.io/streamlinelabs/streamline@sha256:<64 hex digits>")
        .build()
        .expect("explicit, immutable image reference");

    // The container is removed automatically when dropped.
    let container = image
        .start()
        .await
        .expect("failed to start Streamline container");

    let bs = bootstrap_servers(&container)
        .await
        .expect("failed to get bootstrap servers");

    println!("Kafka available at: {bs}");
}
```

In CI, read the reference from the environment instead of hard-coding it. `StreamlineImage::from_env`
reads `STREAMLINE_TEST_IMAGE` and returns an error when it is unset — it never falls back to a
default image:

```rust
use streamline_testcontainers::StreamlineImage;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let image = StreamlineImage::from_env()?;
assert!(image.is_pinned_by_digest(), "pin STREAMLINE_TEST_IMAGE to a digest");
# Ok(())
# }
```

### Getting connection URLs

```rust
use streamline_testcontainers::{
    StreamlineImage, bootstrap_servers, http_url, health_url, metrics_url,
};
use testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn test_connection_urls() {
    let container = StreamlineImage::from_env()
        .unwrap()
        .start()
        .await
        .unwrap();

    // Kafka bootstrap servers (e.g. "127.0.0.1:55123")
    let kafka = bootstrap_servers(&container).await.unwrap();

    // HTTP API base URL (e.g. "http://127.0.0.1:55124")
    let http = http_url(&container).await.unwrap();

    // Health check endpoint
    let health = health_url(&container).await.unwrap();

    // Prometheus metrics endpoint
    let metrics = metrics_url(&container).await.unwrap();

    println!("Kafka:   {kafka}");
    println!("HTTP:    {http}");
    println!("Health:  {health}");
    println!("Metrics: {metrics}");
}
```

### Custom configuration with the builder

```rust
use streamline_testcontainers::StreamlineImage;
use testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn test_with_custom_config() {
    let image = StreamlineImage::builder()
        .image("ghcr.io/streamlinelabs/streamline")  // repository (required)
        .tag("0.4.0")            // mutable tag: prefer .digest(...) when possible
        .log_level("debug")      // set server log level
        .playground(true)        // enable pre-loaded demo topics
        .in_memory(true)         // disable disk persistence
        .env("MY_VAR", "value")  // arbitrary environment variable
        .build()
        .expect("explicit image reference");

    let container = image.start().await.unwrap();

    // ...
}
```

`build()` returns `Err(Error::InvalidConfiguration)` when no repository is set, when neither a tag
nor a digest is set, or when a digest is not 64 hexadecimal characters.

### Debug and trace logging shortcuts

```rust
use streamline_testcontainers::StreamlineImage;

// Debug logging
let image = StreamlineImage::builder()
    .image("ghcr.io/streamlinelabs/streamline")
    .tag("0.4.0")
    .debug_logging()
    .build()?;

// Trace logging
let image = StreamlineImage::builder()
    .image("ghcr.io/streamlinelabs/streamline")
    .tag("0.4.0")
    .trace_logging()
    .build()?;
```

### Cleanup

Containers are automatically removed when the `ContainerAsync` value is dropped. No explicit cleanup is needed. If you want to remove the container eagerly, call `container.rm().await`.

```rust
use streamline_testcontainers::StreamlineImage;
use testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn test_explicit_cleanup() {
    let container = StreamlineImage::from_env().unwrap().start().await.unwrap();

    // ... run your test ...

    // Eagerly remove the container instead of waiting for drop.
    container.rm().await.unwrap();
}
```

### Direct port access

If you need the raw host and port values instead of formatted strings:

```rust
use streamline_testcontainers::StreamlineImage;
use testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn test_raw_ports() {
    let container = StreamlineImage::from_env().unwrap().start().await.unwrap();

    let host = container.get_host().await.unwrap();
    let kafka_port = container
        .get_host_port_ipv4(StreamlineImage::KAFKA_PORT)
        .await
        .unwrap();
    let http_port = container
        .get_host_port_ipv4(StreamlineImage::HTTP_PORT)
        .await
        .unwrap();

    println!("Host: {host}, Kafka port: {kafka_port}, HTTP port: {http_port}");
}
```

## API Reference

### `StreamlineImage`

| Constant / Method | Description |
|---|---|
| `KAFKA_PORT` | Kafka protocol port (`9092`) |
| `HTTP_PORT` | HTTP API port (`9094`) |
| `IMAGE_ENV_VAR` | Name of the image environment variable (`STREAMLINE_TEST_IMAGE`) |
| `StreamlineImage::builder()` | Returns a `StreamlineImageBuilder` |
| `StreamlineImage::from_env()` | Builds from `STREAMLINE_TEST_IMAGE`; errors when unset |
| `.reference()` | Full image reference (`repo@sha256:...` or `repo:tag`) |
| `.is_pinned_by_digest()` | Whether the reference is an immutable digest |

There is no `StreamlineImage::default()`: the image reference is always explicit.

### `StreamlineImageBuilder`

| Method | Description |
|---|---|
| `.image(reference)` | Set repository, `repo:tag`, or `repo@sha256:<digest>` (required) |
| `.tag(tag)` | Set a mutable Docker image tag |
| `.digest(digest)` | Pin an immutable digest (with or without the `sha256:` prefix) |
| `.log_level(level)` | Set log level (trace/debug/info/warn/error) |
| `.debug_logging()` | Shorthand for `.log_level("debug")` |
| `.trace_logging()` | Shorthand for `.log_level("trace")` |
| `.playground(bool)` | Enable/disable playground mode |
| `.in_memory(bool)` | Enable/disable in-memory storage |
| `.env(key, value)` | Add an arbitrary environment variable |
| `.build()` | Consume builder, return `Result<StreamlineImage>` |

### Free functions

| Function | Description |
|---|---|
| `bootstrap_servers(&container)` | Returns `"host:port"` for Kafka clients |
| `http_url(&container)` | Returns `"http://host:port"` base URL |
| `health_url(&container)` | Returns the live health endpoint (`/health/live`) |
| `metrics_url(&container)` | Returns Prometheus metrics endpoint URL |
| `info_url(&container)` | Returns server info endpoint URL |

## Running tests

Unit tests (no Docker required):

```bash
cargo test --manifest-path testcontainers/Cargo.toml
```

Integration test (requires Docker and an explicit, ideally digest-pinned image):

```bash
STREAMLINE_TEST_IMAGE=ghcr.io/streamlinelabs/streamline@sha256:<64 hex digits> \
  cargo test --manifest-path testcontainers/Cargo.toml \
  tests::container_starts_and_exposes_ports -- --ignored --exact
```

The test fails if `STREAMLINE_TEST_IMAGE` is unset or is not pinned to a digest.

## License

Apache-2.0
