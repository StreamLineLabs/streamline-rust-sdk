# Streamline Testcontainers (Rust)

Testcontainers module for [Streamline](https://github.com/streamlinelabs/streamline) -- The Redis of Streaming.

## Features

- Kafka-compatible container for integration testing
- Fast startup (~100ms vs seconds for Kafka)
- Low memory footprint (<50MB)
- No ZooKeeper or KRaft required
- Built-in health checks and metrics via HTTP API
- Builder pattern for configuration
- Async-first API with Tokio

## Installation

Add to your `Cargo.toml`:

```toml
[dev-dependencies]
streamline-testcontainers = "0.2"
testcontainers = "0.23"
tokio = { version = "1", features = ["full"] }
```

To also pull in the Streamline client SDK:

```toml
[dev-dependencies]
streamline-testcontainers = { version = "0.2", features = ["client"] }
```

## Usage

### Basic test setup

```rust
use streamline_testcontainers::{StreamlineImage, bootstrap_servers};
use testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn test_with_streamline() {
    // Start a Streamline container -- it is automatically removed when dropped.
    let container = StreamlineImage::default()
        .start()
        .await
        .expect("failed to start Streamline container");

    // Obtain the Kafka-compatible bootstrap servers address.
    let bs = bootstrap_servers(&container)
        .await
        .expect("failed to get bootstrap servers");

    println!("Kafka available at: {bs}");

    // Use `bs` with any Kafka client crate (e.g. rdkafka, kafka-protocol, etc.)
}
```

### Getting connection URLs

```rust
use streamline_testcontainers::{
    StreamlineImage, bootstrap_servers, http_url, health_url, metrics_url,
};
use testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn test_connection_urls() {
    let container = StreamlineImage::default()
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
        .tag("0.2.0")            // pin a specific image version
        .log_level("debug")      // set server log level
        .playground(true)        // enable pre-loaded demo topics
        .in_memory(true)         // disable disk persistence
        .env("MY_VAR", "value")  // arbitrary environment variable
        .build();

    let container = image.start().await.unwrap();

    // ...
}
```

### Debug and trace logging shortcuts

```rust
use streamline_testcontainers::StreamlineImage;

// Debug logging
let image = StreamlineImage::builder().debug_logging().build();

// Trace logging
let image = StreamlineImage::builder().trace_logging().build();
```

### Cleanup

Containers are automatically removed when the `ContainerAsync` value is dropped. No explicit cleanup is needed. If you want to remove the container eagerly, call `container.rm().await`.

```rust
use streamline_testcontainers::StreamlineImage;
use testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn test_explicit_cleanup() {
    let container = StreamlineImage::default().start().await.unwrap();

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
    let container = StreamlineImage::default().start().await.unwrap();

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
| `StreamlineImage::default()` | Image with default settings |
| `StreamlineImage::builder()` | Returns a `StreamlineImageBuilder` |

### `StreamlineImageBuilder`

| Method | Description |
|---|---|
| `.tag(tag)` | Set the Docker image tag |
| `.log_level(level)` | Set log level (trace/debug/info/warn/error) |
| `.debug_logging()` | Shorthand for `.log_level("debug")` |
| `.trace_logging()` | Shorthand for `.log_level("trace")` |
| `.playground(bool)` | Enable/disable playground mode |
| `.in_memory(bool)` | Enable/disable in-memory storage |
| `.env(key, value)` | Add an arbitrary environment variable |
| `.build()` | Consume builder, return `StreamlineImage` |

### Free functions

| Function | Description |
|---|---|
| `bootstrap_servers(&container)` | Returns `"host:port"` for Kafka clients |
| `http_url(&container)` | Returns `"http://host:port"` base URL |
| `health_url(&container)` | Returns health check endpoint URL |
| `metrics_url(&container)` | Returns Prometheus metrics endpoint URL |
| `info_url(&container)` | Returns server info endpoint URL |

## Running tests

Unit tests (no Docker required):

```bash
cargo test --package streamline-testcontainers
```

Integration tests (requires Docker):

```bash
cargo test --package streamline-testcontainers -- --ignored
```

## License

Apache-2.0
