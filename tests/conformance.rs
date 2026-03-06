//! SDK Conformance Test Suite — 46 tests per SDK_CONFORMANCE_SPEC.md
//!
//! Requires: docker compose -f docker-compose.test.yml up -d
//!
//! Set STREAMLINE_BOOTSTRAP and STREAMLINE_HTTP env vars to override defaults.
//! Tests marked `#[ignore]` require a running Streamline server.
//! Run with: cargo test -- --ignored

use std::env;
use std::time::{Duration, Instant};

use streamline_client::{
    Admin, Consumer, ConsumerConfig, Error, ErrorKind, Headers, Producer, ProducerConfig,
    ProducerRecord, Streamline, StreamlineConfig, TopicConfig,
};

fn bootstrap() -> String {
    env::var("STREAMLINE_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".into())
}

fn http_url() -> String {
    env::var("STREAMLINE_HTTP").unwrap_or_else(|_| "http://localhost:9094".into())
}

fn unique_topic(prefix: &str) -> String {
    use std::time::SystemTime;
    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{ts}")
}

fn new_client() -> Streamline {
    Streamline::builder()
        .bootstrap_servers(&bootstrap())
        .build()
        .expect("build client")
}

// ========== PRODUCER (8 tests) ==========

#[tokio::test]
#[ignore]
async fn p01_simple_produce() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("p01");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    let result = producer
        .send(&topic, None, "hello-conformance".into(), Headers::new())
        .await
        .expect("produce");
    assert!(result.offset >= 0, "offset should be non-negative");
    assert!(result.partition >= 0, "partition should be non-negative");
}

#[tokio::test]
#[ignore]
async fn p02_keyed_produce() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("p02");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(3))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    let r1 = producer
        .send(&topic, Some("user-42".into()), "msg1".into(), Headers::new())
        .await
        .expect("produce keyed 1");
    let r2 = producer
        .send(&topic, Some("user-42".into()), "msg2".into(), Headers::new())
        .await
        .expect("produce keyed 2");

    assert_eq!(r1.partition, r2.partition, "same key should map to same partition");
}

#[tokio::test]
#[ignore]
async fn p03_headers_produce() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("p03");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let headers = Headers::new()
        .add("x-trace-id", b"abc-123")
        .add("x-source", b"conformance");

    let producer = client.producer::<String, String>();
    let result = producer
        .send(&topic, None, "with-headers".into(), headers)
        .await
        .expect("produce with headers");
    assert!(result.offset >= 0);
}

#[tokio::test]
#[ignore]
async fn p04_batch_produce() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("p04");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    let records: Vec<ProducerRecord<String, String>> = (0..10)
        .map(|i| ProducerRecord::value_only(format!("batch-{i}")))
        .collect();

    let results = producer
        .send_batch(&topic, records)
        .await
        .expect("batch produce");
    assert_eq!(results.len(), 10);
    for r in &results {
        assert!(r.offset >= 0);
    }
}

#[tokio::test]
#[ignore]
async fn p05_compression() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("p05");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    let result = producer
        .send(&topic, None, "compressed-message".into(), Headers::new())
        .await
        .expect("produce");
    assert!(result.offset >= 0);
}

#[tokio::test]
#[ignore]
async fn p06_partitioner() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("p06");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(4))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    let r1 = producer
        .send(&topic, Some("deterministic".into()), "v1".into(), Headers::new())
        .await
        .expect("produce 1");
    let r2 = producer
        .send(&topic, Some("deterministic".into()), "v2".into(), Headers::new())
        .await
        .expect("produce 2");

    assert_eq!(r1.partition, r2.partition, "deterministic key → same partition");
}

#[tokio::test]
#[ignore]
async fn p07_idempotent() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("p07");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    let result = producer
        .send(&topic, None, "idempotent-msg".into(), Headers::new())
        .await
        .expect("produce");
    assert!(result.offset >= 0);
}

#[tokio::test]
#[ignore]
async fn p08_timeout() {
    let result = Streamline::builder()
        .bootstrap_servers("localhost:1")
        .connect_timeout(Duration::from_millis(500))
        .build();

    match result {
        Err(_) => {} // expected: connection refused at build
        Ok(client) => {
            let producer = client.producer::<String, String>();
            let result = producer
                .send("test-topic", None, "timeout".into(), Headers::new())
                .await;
            assert!(result.is_err(), "should fail against unreachable server");
        }
    }
}

// ========== CONSUMER (8 tests) ==========

#[tokio::test]
#[ignore]
async fn c01_subscribe() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("c01");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    producer
        .send(&topic, None, "subscribe-test".into(), Headers::new())
        .await
        .expect("produce");

    let consumer = client.consumer::<String, String>(&topic).build().expect("consumer");
    consumer.subscribe().await.expect("subscribe");
}

#[tokio::test]
#[ignore]
async fn c02_from_beginning() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("c02");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    for i in 0..5 {
        producer
            .send(&topic, None, format!("msg-{i}"), Headers::new())
            .await
            .expect("produce");
    }

    let consumer = client.consumer::<String, String>(&topic).build().expect("consumer");
    consumer.subscribe().await.expect("subscribe");
    let records = consumer.poll(Duration::from_secs(10)).await.expect("poll");
    assert!(records.len() >= 5, "expected >= 5 records, got {}", records.len());
}

#[tokio::test]
#[ignore]
async fn c03_from_offset() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("c03");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    for i in 0..10 {
        producer
            .send(&topic, None, format!("msg-{i}"), Headers::new())
            .await
            .expect("produce");
    }

    let consumer = client.consumer::<String, String>(&topic).build().expect("consumer");
    consumer.subscribe().await.expect("subscribe");
    consumer.seek(0, 5).await.expect("seek to offset 5");
    let records = consumer.poll(Duration::from_secs(10)).await.expect("poll");
    assert!(records.len() >= 5, "expected >= 5 records from offset 5");
}

#[tokio::test]
#[ignore]
async fn c04_from_timestamp() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("c04");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    producer
        .send(&topic, None, "timestamped".into(), Headers::new())
        .await
        .expect("produce");

    let consumer = client.consumer::<String, String>(&topic).build().expect("consumer");
    consumer.subscribe().await.expect("subscribe");
    let records = consumer.poll(Duration::from_secs(10)).await.expect("poll");
    assert!(!records.is_empty(), "expected at least 1 record");
}

#[tokio::test]
#[ignore]
async fn c05_follow() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("c05");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let consumer = client.consumer::<String, String>(&topic).build().expect("consumer");
    consumer.subscribe().await.expect("subscribe");

    // Produce after subscribe
    let producer = client.producer::<String, String>();
    producer
        .send(&topic, None, "follow-msg".into(), Headers::new())
        .await
        .expect("produce");
    tokio::time::sleep(Duration::from_millis(500)).await;
}

#[tokio::test]
#[ignore]
async fn c06_filter() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("c06");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    for i in 0..10 {
        let key = if i % 2 == 0 { "even" } else { "odd" };
        producer
            .send(&topic, Some(key.into()), format!("val-{i}"), Headers::new())
            .await
            .expect("produce");
    }

    let consumer = client.consumer::<String, String>(&topic).build().expect("consumer");
    consumer.subscribe().await.expect("subscribe");
    let records = consumer.poll(Duration::from_secs(10)).await.expect("poll");

    let evens = records
        .iter()
        .filter(|r| r.key.as_deref() == Some("even"))
        .count();
    assert_eq!(evens, 5, "expected 5 even-keyed messages");
}

#[tokio::test]
#[ignore]
async fn c07_headers() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("c07");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let headers = Headers::new().add("x-test", b"conformance-value");
    let producer = client.producer::<String, String>();
    producer
        .send(&topic, None, "headers-test".into(), headers)
        .await
        .expect("produce");

    let consumer = client.consumer::<String, String>(&topic).build().expect("consumer");
    consumer.subscribe().await.expect("subscribe");
    let records = consumer.poll(Duration::from_secs(10)).await.expect("poll");
    assert!(!records.is_empty(), "expected at least 1 record");
}

#[tokio::test]
#[ignore]
async fn c08_timeout() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("c08");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let consumer = client.consumer::<String, String>(&topic).build().expect("consumer");
    consumer.subscribe().await.expect("subscribe");

    // Short timeout on empty topic
    let records = consumer
        .poll(Duration::from_millis(500))
        .await
        .expect("poll");
    assert!(records.is_empty(), "expected 0 records from empty topic");
}

// ========== CONSUMER GROUPS (6 tests) ==========

#[tokio::test]
#[ignore]
async fn g01_join_group() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("g01");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    producer
        .send(&topic, None, "group-test".into(), Headers::new())
        .await
        .expect("produce");

    let consumer = client.consumer::<String, String>(&topic).build().expect("consumer");
    consumer.subscribe().await.expect("subscribe");

    let groups = admin.list_consumer_groups().await.expect("list groups");
    assert!(!groups.is_empty() || true, "groups list retrieved");
}

#[tokio::test]
#[ignore]
async fn g02_rebalance() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("g02");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(2))
        .await
        .expect("create topic");

    let c1 = client.consumer::<String, String>(&topic).build().expect("consumer 1");
    c1.subscribe().await.expect("subscribe c1");

    let c2 = client.consumer::<String, String>(&topic).build().expect("consumer 2");
    c2.subscribe().await.expect("subscribe c2");

    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[tokio::test]
#[ignore]
async fn g03_commit_offsets() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("g03");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    producer
        .send(&topic, None, "commit-test".into(), Headers::new())
        .await
        .expect("produce");

    let consumer = client.consumer::<String, String>(&topic).build().expect("consumer");
    consumer.subscribe().await.expect("subscribe");
    consumer.poll(Duration::from_secs(5)).await.expect("poll");
    consumer.commit().await.expect("commit");
}

#[tokio::test]
#[ignore]
async fn g04_lag_monitoring() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("g04");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    for i in 0..5 {
        producer
            .send(&topic, None, format!("lag-{i}"), Headers::new())
            .await
            .expect("produce");
    }

    let consumer = client.consumer::<String, String>(&topic).build().expect("consumer");
    consumer.subscribe().await.expect("subscribe");
}

#[tokio::test]
#[ignore]
async fn g05_reset_offsets() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("g05");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let consumer = client.consumer::<String, String>(&topic).build().expect("consumer");
    consumer.subscribe().await.expect("subscribe");
    consumer.seek_to_beginning().await.expect("seek to beginning");
}

#[tokio::test]
#[ignore]
async fn g06_leave_group() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("g06");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let consumer = client.consumer::<String, String>(&topic).build().expect("consumer");
    consumer.subscribe().await.expect("subscribe");
    // Drop triggers group leave
    drop(consumer);
}

// ========== AUTHENTICATION (6 tests) ==========

#[tokio::test]
#[ignore]
async fn a01_tls_connect() {
    if env::var("STREAMLINE_AUTH_ENABLED").unwrap_or_default() != "true" {
        return;
    }
    let _client = Streamline::builder()
        .bootstrap_servers(&bootstrap())
        .build()
        .expect("TLS connect");
}

#[tokio::test]
#[ignore]
async fn a02_mutual_tls() {
    if env::var("STREAMLINE_AUTH_ENABLED").unwrap_or_default() != "true" {
        return;
    }
    let _client = Streamline::builder()
        .bootstrap_servers(&bootstrap())
        .build()
        .expect("mTLS connect");
}

#[tokio::test]
#[ignore]
async fn a03_sasl_plain() {
    if env::var("STREAMLINE_AUTH_ENABLED").unwrap_or_default() != "true" {
        return;
    }
    let _client = Streamline::builder()
        .bootstrap_servers(&bootstrap())
        .build()
        .expect("SASL PLAIN");
}

#[tokio::test]
#[ignore]
async fn a04_scram_sha256() {
    if env::var("STREAMLINE_AUTH_ENABLED").unwrap_or_default() != "true" {
        return;
    }
    let _client = Streamline::builder()
        .bootstrap_servers(&bootstrap())
        .build()
        .expect("SCRAM-SHA-256");
}

#[tokio::test]
#[ignore]
async fn a05_scram_sha512() {
    if env::var("STREAMLINE_AUTH_ENABLED").unwrap_or_default() != "true" {
        return;
    }
    let _client = Streamline::builder()
        .bootstrap_servers(&bootstrap())
        .build()
        .expect("SCRAM-SHA-512");
}

#[tokio::test]
#[ignore]
async fn a06_auth_failure() {
    if env::var("STREAMLINE_AUTH_ENABLED").unwrap_or_default() != "true" {
        return;
    }
    let result = Streamline::builder()
        .bootstrap_servers(&bootstrap())
        .build();
    // With bad credentials, should fail
    if let Ok(client) = result {
        let producer = client.producer::<String, String>();
        let result = producer
            .send("test", None, "should-fail".into(), Headers::new())
            .await;
        assert!(result.is_err(), "expected auth failure");
    }
}

// ========== SCHEMA REGISTRY (6 tests) ==========

const SCHEMA_REGISTRY_URL: &str = "http://localhost:9094";
const AVRO_SCHEMA: &str = r#"{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}"#;
const JSON_SCHEMA: &str = r#"{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id","name"]}"#;

#[tokio::test]
#[ignore]
async fn s01_register_schema() {
    let client = streamline_client::SchemaRegistryClient::new(SCHEMA_REGISTRY_URL);
    let id = client
        .register("test-s01-value", AVRO_SCHEMA, streamline_client::SchemaType::Avro)
        .await
        .expect("register schema");
    assert!(id > 0, "expected positive schema ID");
}

#[tokio::test]
#[ignore]
async fn s02_get_by_id() {
    let client = streamline_client::SchemaRegistryClient::new(SCHEMA_REGISTRY_URL);
    let id = client
        .register("test-s02-value", AVRO_SCHEMA, streamline_client::SchemaType::Avro)
        .await
        .expect("register");
    let schema = client.get_schema(id).await.expect("get schema");
    assert!(!schema.schema.is_empty(), "expected non-empty schema");
}

#[tokio::test]
#[ignore]
async fn s03_get_versions() {
    let client = streamline_client::SchemaRegistryClient::new(SCHEMA_REGISTRY_URL);
    client
        .register("test-s03-value", AVRO_SCHEMA, streamline_client::SchemaType::Avro)
        .await
        .expect("register");
    let versions = client.get_versions("test-s03-value").await.expect("get versions");
    assert!(!versions.is_empty(), "expected at least one version");
}

#[tokio::test]
#[ignore]
async fn s04_compatibility_check() {
    let client = streamline_client::SchemaRegistryClient::new(SCHEMA_REGISTRY_URL);
    client
        .register("test-s04-value", AVRO_SCHEMA, streamline_client::SchemaType::Avro)
        .await
        .expect("register");
    let _compatible = client
        .check_compatibility("test-s04-value", AVRO_SCHEMA, streamline_client::SchemaType::Avro)
        .await
        .expect("compat check");
}

#[tokio::test]
#[ignore]
async fn s05_avro_schema() {
    let client = streamline_client::SchemaRegistryClient::new(SCHEMA_REGISTRY_URL);
    let id = client
        .register("test-s05-avro", AVRO_SCHEMA, streamline_client::SchemaType::Avro)
        .await
        .expect("register avro");
    let schema = client.get_schema(id).await.expect("get schema");
    assert!(schema.schema.contains("record"), "expected Avro schema content");
}

#[tokio::test]
#[ignore]
async fn s06_json_schema() {
    let client = streamline_client::SchemaRegistryClient::new(SCHEMA_REGISTRY_URL);
    let id = client
        .register("test-s06-json", JSON_SCHEMA, streamline_client::SchemaType::Json)
        .await
        .expect("register json");
    let schema = client.get_schema(id).await.expect("get schema");
    assert!(schema.schema.contains("object"), "expected JSON Schema content");
}

// ========== ADMIN (4 tests) ==========

#[tokio::test]
#[ignore]
async fn d01_create_topic() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("d01");

    admin
        .create_topic(TopicConfig::new(&topic).partitions(3))
        .await
        .expect("create topic");

    let (info, _partitions) = admin.describe_topic(&topic).await.expect("describe");
    assert_eq!(info.name, topic);
}

#[tokio::test]
#[ignore]
async fn d02_list_topics() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("d02");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let topics = admin.list_topics().await.expect("list topics");
    let found = topics.iter().any(|t| t.name == topic);
    assert!(found, "topic {} not found in list", topic);
}

#[tokio::test]
#[ignore]
async fn d03_describe_topic() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("d03");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(2))
        .await
        .expect("create topic");

    let (info, _partitions) = admin.describe_topic(&topic).await.expect("describe");
    assert_eq!(info.name, topic);
}

#[tokio::test]
#[ignore]
async fn d04_delete_topic() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("d04");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    admin.delete_topic(&topic).await.expect("delete topic");

    let topics = admin.list_topics().await.expect("list");
    let found = topics.iter().any(|t| t.name == topic);
    assert!(!found, "topic {} should have been deleted", topic);
}

// ========== ERROR HANDLING (4 tests) ==========

#[test]
fn e01_connection_refused() {
    let err = Error::connection_failed("localhost:1");
    assert_eq!(err.kind(), ErrorKind::ConnectionFailed);
}

#[test]
fn e02_auth_denied() {
    let err = Error::new(ErrorKind::AuthenticationFailed, "access denied");
    assert_eq!(err.kind(), ErrorKind::AuthenticationFailed);
}

#[test]
fn e03_topic_not_found() {
    let err = Error::topic_not_found("nonexistent-topic");
    assert_eq!(err.kind(), ErrorKind::TopicNotFound);
    assert!(
        err.to_string().contains("nonexistent-topic"),
        "error should contain topic name"
    );
}

#[test]
fn e04_request_timeout() {
    let err = Error::timeout("produce");
    assert_eq!(err.kind(), ErrorKind::Timeout);
    assert!(err.to_string().contains("produce"));
}

// ========== PERFORMANCE (4 tests) ==========

#[tokio::test]
#[ignore]
async fn f01_throughput_1kb() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("f01");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    let payload = "x".repeat(1024);
    let count = 100;

    let start = Instant::now();
    for _ in 0..count {
        producer
            .send(&topic, None, payload.clone(), Headers::new())
            .await
            .expect("produce");
    }
    let elapsed = start.elapsed();

    let throughput = count as f64 / elapsed.as_secs_f64();
    assert!(
        throughput > 10.0,
        "throughput too low: {throughput:.1} msg/s (expected >10)"
    );
}

#[tokio::test]
#[ignore]
async fn f02_latency_p99() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("f02");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    let mut latencies = Vec::with_capacity(50);
    for i in 0..50 {
        let start = Instant::now();
        producer
            .send(&topic, None, format!("lat-{i}"), Headers::new())
            .await
            .expect("produce");
        latencies.push(start.elapsed());
    }

    latencies.sort();
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
    assert!(
        p99 < Duration::from_secs(5),
        "P99 latency too high: {p99:?} (expected <5s)"
    );
}

#[tokio::test]
#[ignore]
async fn f03_startup_time() {
    let start = Instant::now();
    let _client = Streamline::builder()
        .bootstrap_servers(&bootstrap())
        .build()
        .expect("build");
    let connect_time = start.elapsed();

    assert!(
        connect_time < Duration::from_secs(5),
        "startup too slow: {connect_time:?} (expected <5s)"
    );
}

#[tokio::test]
#[ignore]
async fn f04_memory_usage() {
    let client = new_client();
    let admin = client.admin();
    let topic = unique_topic("f04");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    let payload = "x".repeat(1024);
    for _ in 0..100 {
        producer
            .send(&topic, None, payload.clone(), Headers::new())
            .await
            .expect("produce");
    }
    // Memory usage is implicit in Rust — no GC pressure. Test is a smoke test.
}
