# Streamline Rust Client

[![CI](https://github.com/streamlinelabs/streamline-rust-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/streamlinelabs/streamline-rust-sdk/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.80%2B-orange.svg)](https://www.rust-lang.org/)
[![Docs](https://img.shields.io/badge/docs-streamlinelabs.dev-blue.svg)](https://streamlinelabs.dev/docs/sdks/rust)

Native Rust client library for Streamline streaming platform.

## Features

- Async/await with Tokio
- Connection pooling
- Automatic reconnection
- Producer and consumer APIs
- Compression support (LZ4, Zstd, Snappy)
- TLS support

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
streamline-client = "0.1"
```

With optional features:

```toml
[dependencies]
streamline-client = { version = "0.1", features = ["compression-lz4", "tls"] }
```

### With OpenTelemetry Tracing

```toml
[dependencies]
streamline-client = { version = "0.1", features = ["telemetry"] }
```

## OpenTelemetry Tracing

The SDK supports optional OpenTelemetry tracing gated behind the `telemetry` Cargo
feature. When enabled, produce and consume operations can be wrapped in OTel-compatible
spans. When disabled, all tracing functions compile to zero-overhead no-ops.

### Usage

```rust
use streamline_client::telemetry;
use streamline_client::Headers;

// Trace a produce operation
let metadata = telemetry::trace_produce("orders", || async {
    producer.send("orders", "key".to_string(), "value".to_string(), Headers::new()).await
}).await?;

// Trace a consume operation
let records = telemetry::trace_consume("events", || async {
    consumer.poll(Duration::from_millis(100)).await
}).await?;

// Trace individual record processing
for record in &records {
    telemetry::trace_process(
        &record.topic,
        record.partition,
        record.offset,
        || async { process(&record) }
    ).await;
}

// Inject context into headers for propagation
let mut headers = Headers::new();
telemetry::inject_context(&mut headers);
```

### Span Conventions

| Attribute | Value |
|-----------|-------|
| Span name | `{topic} {operation}` (e.g., "orders produce") |
| `messaging.system` | `streamline` |
| `messaging.destination.name` | Topic name |
| `messaging.operation` | `produce`, `consume`, or `process` |
| Span kind | `PRODUCER` for produce, `CONSUMER` for consume |

Trace context is propagated through message headers using W3C TraceContext format.

## Quick Start

### Producer

```rust
use streamline_client::{Streamline, StreamlineConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client
    let client = Streamline::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .await?;

    // Produce a message
    let metadata = client.produce("my-topic", "key", "Hello, Streamline!").await?;
    println!("Produced to partition {} at offset {}", metadata.partition, metadata.offset);

    Ok(())
}
```

### Consumer

```rust
use streamline_client::{Streamline, ConsumerConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Streamline::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .await?;

    let consumer = client.consumer::<String, String>("my-topic")
        .group_id("my-group")
        .auto_offset_reset("earliest")
        .build()
        .await?;

    consumer.subscribe().await?;

    loop {
        let records = consumer.poll(Duration::from_millis(100)).await?;
        for record in records {
            println!("Received: {} at offset {}", record.value, record.offset);
        }
    }
}
```

### Producer with Headers

```rust
use streamline_client::Headers;

let headers = Headers::builder()
    .add("source", "my-app")
    .add("version", "1.0")
    .build();

client.produce_with_headers("my-topic", "key", "value", headers).await?;
```

### Batch Production

```rust
use streamline_client::ProducerRecord;

let records = vec![
    ProducerRecord::new("key1", "value1"),
    ProducerRecord::new("key2", "value2"),
    ProducerRecord::new("key3", "value3"),
];

let results = client.produce_batch("my-topic", records).await?;
```

## Configuration

### Client Configuration

```rust
let client = Streamline::builder()
    .bootstrap_servers("localhost:9092")
    .connection_pool_size(4)
    .connect_timeout(Duration::from_secs(30))
    .request_timeout(Duration::from_secs(30))
    .build()
    .await?;
```

### Producer Configuration

```rust
let producer = client.producer::<String, String>()
    .batch_size(16384)
    .linger_ms(1)
    .compression("lz4")
    .retries(3)
    .build();
```

### Consumer Configuration

```rust
let consumer = client.consumer::<String, String>("my-topic")
    .group_id("my-group")
    .auto_offset_reset("earliest")
    .enable_auto_commit(true)
    .max_poll_records(500)
    .session_timeout(Duration::from_secs(30))
    .build()
    .await?;
```

## Error Handling

```rust
use streamline_client::{StreamlineError, ErrorKind};

match client.produce("my-topic", "key", "value").await {
    Ok(metadata) => println!("Success: {:?}", metadata),
    Err(StreamlineError { kind: ErrorKind::TopicNotFound, hint, .. }) => {
        eprintln!("Topic not found. {}", hint.unwrap_or_default());
    }
    Err(e) => eprintln!("Error: {}", e),
}
```

## TLS Configuration

```rust
let client = Streamline::builder()
    .bootstrap_servers("localhost:9092")
    .tls_config(TlsConfig::builder()
        .ca_cert("ca.crt")
        .client_cert("client.crt")
        .client_key("client.key")
        .build()?)
    .build()
    .await?;
```

## Testing

### With Testcontainers (recommended)

The [`streamline-testcontainers`](./testcontainers/) crate provides a
[Testcontainers](https://testcontainers.org/) module that spins up a disposable Streamline
server inside your tests -- no manual Docker setup required.

```toml
[dev-dependencies]
streamline-testcontainers = "0.2"
testcontainers = "0.23"
tokio = { version = "1", features = ["full"] }
```

```rust
use streamline_testcontainers::{StreamlineImage, bootstrap_servers};
use testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn test_produce() {
    let container = StreamlineImage::default().start().await.unwrap();
    let bs = bootstrap_servers(&container).await.unwrap();
    // Use `bs` with any Kafka client ...
}
```

See the [testcontainers README](./testcontainers/README.md) for full documentation.

### With Docker Compose

Alternatively, start a local Streamline server manually:

```bash
docker compose -f docker-compose.test.yml up -d
```

Run tests:

```bash
cargo test
```

## API Reference

### Client

| Method | Description |
|--------|-------------|
| `Streamline::builder()` | Create a new client builder |
| `client.produce(topic, key, value).await` | Send a message |
| `client.produce_with_headers(topic, key, value, headers).await` | Send with headers |
| `client.producer()` | Create a producer instance |
| `client.consumer(topic)` | Create a consumer builder |
| `client.is_healthy().await` | Check health status |
| `client.config()` | Get client configuration |

### Producer

| Method | Description |
|--------|-------------|
| `producer.send(topic, key, value, headers).await` | Send a message |
| `producer.send_batch(topic, records).await` | Send a batch of messages |
| `producer.flush().await` | Flush buffered messages |
| `producer.config()` | Get producer configuration |

### Consumer

| Method | Description |
|--------|-------------|
| `consumer.subscribe().await` | Subscribe to topic |
| `consumer.poll(timeout).await` | Poll for messages |
| `consumer.commit().await` | Commit offsets synchronously |
| `consumer.commit_async().await` | Commit offsets asynchronously |
| `consumer.seek_to_beginning().await` | Seek to start |
| `consumer.seek_to_end().await` | Seek to end |
| `consumer.seek(partition, offset).await` | Seek to specific offset |
| `consumer.position(partition).await` | Get current position |
| `consumer.pause()` | Pause consuming |
| `consumer.resume()` | Resume consuming |

## License

Apache 2.0
