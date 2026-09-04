//! Live conformance tests against a running Streamline broker.
//!
//! Unlike `tests/conformance.rs` (which is entirely offline), every test in
//! this file talks to a real broker over the Kafka wire protocol. They are
//! `#[ignore]`d so ordinary `cargo test` runs stay hermetic, and are executed
//! explicitly by the release gate:
//!
//! ```bash
//! STREAMLINE_BOOTSTRAP_SERVERS=127.0.0.1:9092 \
//!   cargo test --test live_conformance -- --ignored
//! ```
//!
//! The gate additionally asserts that the number of executed tests matches the
//! number declared here, so a filter typo or a silently skipped suite cannot
//! be mistaken for a passing conformance run.
//!
//! Required environment:
//! * `STREAMLINE_BOOTSTRAP_SERVERS` — `host:port` of the broker's Kafka port.
//! * `STREAMLINE_CONFORMANCE_TOPIC` — optional; defaults to
//!   `rust-sdk-conformance`. The topic must exist, or the broker must create
//!   topics on first produce.
//!
//! Cross-repository contract: these tests assume the broker accepts Produce
//! v3-style requests and Fetch v11 requests as encoded by this SDK, and that a
//! fetch response never contains a truncated trailing record batch.

use std::time::Duration;

use streamline_client::{ErrorKind, Headers, Streamline};

fn bootstrap_servers() -> String {
    std::env::var("STREAMLINE_BOOTSTRAP_SERVERS").expect(
        "STREAMLINE_BOOTSTRAP_SERVERS must be set for live conformance tests; \
         these tests never fall back to a default broker address",
    )
}

fn topic() -> String {
    std::env::var("STREAMLINE_CONFORMANCE_TOPIC")
        .unwrap_or_else(|_| "rust-sdk-conformance".to_string())
}

async fn client() -> Streamline {
    Streamline::builder()
        .bootstrap_servers(&bootstrap_servers())
        .connect_timeout(Duration::from_secs(10))
        .request_timeout(Duration::from_secs(10))
        .build()
        .await
        .expect("client construction must succeed")
}

fn unique_value(prefix: &str) -> Vec<u8> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{prefix}-{nanos}").into_bytes()
}

/// Produce a record and read it back, exercising the Produce and Fetch v11
/// wire encodings and the strict fetch response parser end to end.
#[tokio::test]
#[ignore]
async fn live_produce_and_fetch_round_trip() {
    let client = client().await;
    let topic = topic();

    let key = unique_value("key");
    let value = unique_value("value");
    let mut headers = Headers::new();
    headers.add("conformance", b"rust-sdk");

    let producer = client.producer::<Vec<u8>, Vec<u8>>();
    let metadata = producer
        .send(&topic, key.clone(), value.clone(), headers)
        .await
        .expect("produce must succeed against a live broker");
    assert!(metadata.offset >= 0, "offset must be non-negative");
    assert_eq!(metadata.topic, topic);

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .partitions(vec![metadata.partition])
        .build()
        .await
        .expect("consumer construction must succeed");
    consumer.subscribe().await.expect("subscribe");
    consumer
        .seek(metadata.partition, metadata.offset)
        .await
        .expect("seek to the produced offset");

    let mut found = None;
    for _ in 0..20 {
        let records = consumer
            .poll(Duration::from_millis(500))
            .await
            .expect("fetch must parse cleanly against a live broker");
        if let Some(record) = records
            .into_iter()
            .find(|record| record.offset == metadata.offset)
        {
            found = Some(record);
            break;
        }
    }

    let record = found.expect("the produced record must be fetched back");
    assert_eq!(record.topic, topic);
    assert_eq!(record.partition, metadata.partition);
    assert_eq!(record.offset, metadata.offset);
    assert_eq!(record.value, value);
    assert_eq!(record.key.as_deref(), Some(key.as_slice()));
    assert_eq!(
        record.headers.get("conformance"),
        Some(b"rust-sdk".as_slice()),
        "headers must round-trip through produce and fetch"
    );
}

/// A fetch against a topic that does not exist must surface a broker error,
/// never an empty successful poll.
#[tokio::test]
#[ignore]
async fn live_fetch_unknown_topic_reports_an_error() {
    let client = client().await;
    let missing = format!(
        "rust-sdk-missing-{}",
        String::from_utf8_lossy(&unique_value("t"))
    );

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&missing)
        .partitions(vec![0])
        .build()
        .await
        .expect("consumer construction must succeed");
    consumer.subscribe().await.expect("subscribe");

    let error = consumer
        .poll(Duration::from_millis(500))
        .await
        .expect_err("an unknown topic must not be reported as an empty poll");
    assert!(
        matches!(
            error.kind,
            ErrorKind::TopicNotFound | ErrorKind::Protocol | ErrorKind::Server
        ),
        "unexpected error kind {:?}: {error}",
        error.kind
    );
}

/// Operations that are not implemented must keep failing closed even when a
/// broker is reachable.
#[tokio::test]
#[ignore]
async fn live_unsupported_operations_still_fail_closed() {
    let client = client().await;
    let topic = topic();

    assert_eq!(
        client.admin().list_topics().await.unwrap_err().kind,
        ErrorKind::Unsupported
    );

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .partitions(vec![0])
        .build()
        .await
        .expect("consumer construction must succeed");
    consumer.subscribe().await.expect("subscribe");

    assert_eq!(
        consumer.commit().await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        consumer.seek_to_end().await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
}

/// The connection pool must report a live broker as healthy after a
/// successful exchange, proving the pooled connection was not evicted by a
/// clean request/response round trip.
#[tokio::test]
#[ignore]
async fn live_healthy_connection_is_retained_in_the_pool() {
    let client = client().await;
    let topic = topic();

    let producer = client.producer::<Vec<u8>, Vec<u8>>();
    producer
        .send(
            &topic,
            unique_value("pool-key"),
            unique_value("pool-value"),
            Headers::new(),
        )
        .await
        .expect("produce must succeed against a live broker");

    assert!(
        client.is_healthy().await,
        "a successful exchange must leave a live connection in the pool"
    );
}
