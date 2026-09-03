# Examples

## Prerequisites

- Rust 1.80+
- A running Streamline server (default: `localhost:9092`)

## Running

Start Streamline:

```bash
# Via Docker
docker run -p 9092:9092 -p 9094:9094 ghcr.io/streamlinelabs/streamline:0.2.0 --playground

# Or via Homebrew
streamline --playground
```

Run the example:

```bash
cargo run --example producer
```

The producer and consumer examples require pre-existing topics. The Rust SDK
0.4.0 Kafka Admin API intentionally returns `ErrorKind::Unsupported`.

Examples with optional features:

```bash
cargo run --example schema_registry --features schema-registry
cargo run --example agent_memory --features moonshot
cargo run --example security --features tls,sasl
```

## Configuration

Set `STREAMLINE_BOOTSTRAP_SERVERS` to connect to a non-local server:

```bash
export STREAMLINE_BOOTSTRAP_SERVERS=my-server:9092
cargo run --example producer
```
