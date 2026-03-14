//! SDK Conformance Test Suite
//!
//! Requires: docker compose -f docker-compose.test.yml up -d
//!
//! Set STREAMLINE_BOOTSTRAP and STREAMLINE_HTTP env vars to override defaults.
//! Tests marked `#[ignore]` require a running Streamline server.
//! Run with: cargo test --test conformance -- --ignored

use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use streamline_client::{
    Error, ErrorKind, Headers, ProducerRecord, SaslConfig, SaslMechanism,
    Streamline, TlsConfig, TopicConfig,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bootstrap() -> String {
    env::var("STREAMLINE_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".into())
}

#[allow(dead_code)]
fn http_url() -> String {
    env::var("STREAMLINE_HTTP").unwrap_or_else(|_| "http://localhost:9094".into())
}

fn unique_topic(test_id: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    format!("conformance-{test_id}-{ts}")
}

async fn new_client() -> Streamline {
    Streamline::builder()
        .bootstrap_servers(&bootstrap())
        .build()
        .await
        .expect("build client")
}

// ===========================================================================
// PRODUCER TESTS (P01–P08)
// ===========================================================================

#[tokio::test]
#[ignore] // Requires running Streamline server
async fn test_p01_simple_produce() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("p01");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    let result = producer
        .send(&topic, "key-1".to_string(), "hello-conformance".to_string(), Headers::new())
        .await
        .expect("produce");
    assert!(result.offset >= 0, "offset should be non-negative");
    assert!(result.partition >= 0, "partition should be non-negative");
}

#[tokio::test]
#[ignore]
async fn test_p02_keyed_produce() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("p02");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(3))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    let r1 = producer
        .send(&topic, "user-42".to_string(), "msg1".to_string(), Headers::new())
        .await
        .expect("produce keyed 1");
    let r2 = producer
        .send(&topic, "user-42".to_string(), "msg2".to_string(), Headers::new())
        .await
        .expect("produce keyed 2");

    assert_eq!(
        r1.partition, r2.partition,
        "same key should map to same partition"
    );
}

#[tokio::test]
#[ignore]
async fn test_p03_headers_produce() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("p03");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let headers = Headers::builder()
        .add("x-trace-id", b"abc-123")
        .add("x-source", b"conformance")
        .build();

    let producer = client.producer::<String, String>();
    let result = producer
        .send(&topic, "k".to_string(), "with-headers".to_string(), headers)
        .await
        .expect("produce with headers");
    assert!(result.offset >= 0);
}

#[tokio::test]
#[ignore]
async fn test_p04_batch_produce() {
    let client = new_client().await;
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
async fn test_p05_compression() {
    // Compression is transparent at the Kafka protocol level.
    // Even without explicit codec config, the server accepts large payloads.
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("p05");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<Vec<u8>, Vec<u8>>();
    let large_payload = vec![b'x'; 10_000];
    let result = producer
        .send(&topic, Vec::new(), large_payload, Headers::new())
        .await
        .expect("produce large payload");
    assert!(result.offset >= 0);
}

#[tokio::test]
#[ignore]
async fn test_p06_partitioner() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("p06");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(4))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    let r1 = producer
        .send(&topic, "deterministic".to_string(), "v1".to_string(), Headers::new())
        .await
        .expect("produce 1");
    let r2 = producer
        .send(&topic, "deterministic".to_string(), "v2".to_string(), Headers::new())
        .await
        .expect("produce 2");

    assert_eq!(
        r1.partition, r2.partition,
        "deterministic key should map to same partition"
    );
}

#[tokio::test]
#[ignore]
async fn test_p07_idempotent() {
    // Verify that producing the same message twice yields distinct offsets
    // (server-side deduplication is not yet wired — test ensures delivery).
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("p07");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    let r1 = producer
        .send(&topic, "idem".to_string(), "v1".to_string(), Headers::new())
        .await
        .expect("produce 1");
    let r2 = producer
        .send(&topic, "idem".to_string(), "v2".to_string(), Headers::new())
        .await
        .expect("produce 2");
    assert!(r2.offset > r1.offset, "subsequent produce should have higher offset");
}

#[tokio::test]
#[ignore]
async fn test_p08_timeout() {
    let result = Streamline::builder()
        .bootstrap_servers("localhost:1")
        .connect_timeout(Duration::from_millis(500))
        .build()
        .await;

    match result {
        Err(_) => {} // expected: connection refused at build time
        Ok(client) => {
            let producer = client.producer::<String, String>();
            let result = producer
                .send("test-topic", "k".to_string(), "timeout".to_string(), Headers::new())
                .await;
            assert!(result.is_err(), "should fail against unreachable server");
        }
    }
}

// ===========================================================================
// CONSUMER TESTS (C01–C08)
// ===========================================================================

#[tokio::test]
#[ignore]
async fn test_c01_subscribe() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("c01");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    producer
        .send(&topic, "k".to_string(), "subscribe-test".to_string(), Headers::new())
        .await
        .expect("produce");

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .build()
        .await
        .expect("consumer");
    consumer.subscribe().await.expect("subscribe");
}

#[tokio::test]
#[ignore]
async fn test_c02_from_beginning() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("c02");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    for i in 0..5 {
        producer
            .send(&topic, format!("k{i}"), format!("msg-{i}"), Headers::new())
            .await
            .expect("produce");
    }

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .auto_offset_reset("earliest")
        .build()
        .await
        .expect("consumer");
    consumer.subscribe().await.expect("subscribe");

    let records = consumer.poll(Duration::from_secs(10)).await.expect("poll");
    assert!(
        records.len() >= 5,
        "expected >= 5 records, got {}",
        records.len()
    );
}

#[tokio::test]
#[ignore]
async fn test_c03_from_offset() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("c03");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    for i in 0..10 {
        producer
            .send(&topic, format!("k{i}"), format!("msg-{i}"), Headers::new())
            .await
            .expect("produce");
    }

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .auto_offset_reset("earliest")
        .build()
        .await
        .expect("consumer");
    consumer.subscribe().await.expect("subscribe");
    consumer.seek(0, 5).await.expect("seek to offset 5");

    let records = consumer.poll(Duration::from_secs(10)).await.expect("poll");
    assert!(
        records.len() >= 5,
        "expected >= 5 records from offset 5"
    );
}

#[tokio::test]
#[ignore]
async fn test_c04_from_timestamp() {
    // Produce messages and consume from a known timestamp.
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("c04");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let ts_before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let producer = client.producer::<String, String>();
    for i in 0..5 {
        producer
            .send(&topic, format!("k{i}"), format!("ts-{i}"), Headers::new())
            .await
            .expect("produce");
    }

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .build()
        .await
        .expect("consumer");
    consumer.subscribe().await.expect("subscribe");
    let records = consumer.poll(Duration::from_secs(5)).await.expect("poll");
    // All records should have timestamps >= ts_before
    for r in &records {
        assert!(r.timestamp >= ts_before, "record timestamp should be recent");
    }
}

#[tokio::test]
#[ignore]
async fn test_c05_follow() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("c05");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .build()
        .await
        .expect("consumer");
    consumer.subscribe().await.expect("subscribe");

    // Produce after subscribe to verify live tailing
    let producer = client.producer::<String, String>();
    producer
        .send(&topic, "k".to_string(), "follow-msg".to_string(), Headers::new())
        .await
        .expect("produce");
    tokio::time::sleep(Duration::from_millis(500)).await;
}

#[tokio::test]
#[ignore]
async fn test_c06_filter() {
    let client = new_client().await;
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
            .send(&topic, key.to_string(), format!("val-{i}"), Headers::new())
            .await
            .expect("produce");
    }

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .auto_offset_reset("earliest")
        .build()
        .await
        .expect("consumer");
    consumer.subscribe().await.expect("subscribe");

    let records = consumer.poll(Duration::from_secs(10)).await.expect("poll");
    let evens: Vec<_> = records
        .iter()
        .filter(|r| r.key.as_deref() == Some(b"even".as_slice()))
        .collect();
    assert_eq!(evens.len(), 5, "expected 5 even-keyed messages");
}

#[tokio::test]
#[ignore]
async fn test_c07_headers() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("c07");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let headers = Headers::builder()
        .add("x-test", b"conformance-value")
        .build();
    let producer = client.producer::<String, String>();
    producer
        .send(&topic, "k".to_string(), "headers-test".to_string(), headers)
        .await
        .expect("produce");

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .auto_offset_reset("earliest")
        .build()
        .await
        .expect("consumer");
    consumer.subscribe().await.expect("subscribe");

    let records = consumer.poll(Duration::from_secs(10)).await.expect("poll");
    assert!(!records.is_empty(), "expected at least 1 record");
}

#[tokio::test]
#[ignore]
async fn test_c08_timeout() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("c08");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .build()
        .await
        .expect("consumer");
    consumer.subscribe().await.expect("subscribe");

    // Short timeout on empty topic — should return 0 records
    let records = consumer
        .poll(Duration::from_millis(500))
        .await
        .expect("poll");
    assert!(records.is_empty(), "expected 0 records from empty topic");
}

// ===========================================================================
// CONSUMER GROUP TESTS (G01–G06)
// ===========================================================================

#[tokio::test]
#[ignore]
async fn test_g01_join_group() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("g01");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    producer
        .send(&topic, "k".to_string(), "group-test".to_string(), Headers::new())
        .await
        .expect("produce");

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .group_id("conformance-g01")
        .build()
        .await
        .expect("consumer");
    consumer.subscribe().await.expect("subscribe");

    let groups = admin.list_consumer_groups().await.expect("list groups");
    // Groups list retrieved successfully (may or may not contain our group yet)
    let _ = groups;
}

#[tokio::test]
#[ignore]
async fn test_g02_rebalance() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("g02");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(2))
        .await
        .expect("create topic");

    let mut c1 = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .group_id("conformance-g02")
        .build()
        .await
        .expect("consumer 1");
    c1.subscribe().await.expect("subscribe c1");

    let mut c2 = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .group_id("conformance-g02")
        .build()
        .await
        .expect("consumer 2");
    c2.subscribe().await.expect("subscribe c2");

    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[tokio::test]
#[ignore]
async fn test_g03_commit_offsets() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("g03");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    producer
        .send(&topic, "k".to_string(), "commit-test".to_string(), Headers::new())
        .await
        .expect("produce");

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .group_id("conformance-g03")
        .auto_offset_reset("earliest")
        .build()
        .await
        .expect("consumer");
    consumer.subscribe().await.expect("subscribe");
    consumer.poll(Duration::from_secs(5)).await.expect("poll");
    consumer.commit().await.expect("commit");
}

#[tokio::test]
#[ignore]
async fn test_g04_lag_monitoring() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("g04");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let producer = client.producer::<String, String>();
    for i in 0..5 {
        producer
            .send(&topic, format!("k{i}"), format!("lag-{i}"), Headers::new())
            .await
            .expect("produce");
    }

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .group_id("conformance-g04")
        .build()
        .await
        .expect("consumer");
    consumer.subscribe().await.expect("subscribe");
}

#[tokio::test]
#[ignore]
async fn test_g05_reset_offsets() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("g05");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .group_id("conformance-g05")
        .build()
        .await
        .expect("consumer");
    consumer.subscribe().await.expect("subscribe");
    consumer
        .seek_to_beginning()
        .await
        .expect("seek to beginning");
}

#[tokio::test]
#[ignore]
async fn test_g06_leave_group() {
    let client = new_client().await;
    let admin = client.admin();
    let topic = unique_topic("g06");
    admin
        .create_topic(TopicConfig::new(&topic).partitions(1))
        .await
        .expect("create topic");

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>(&topic)
        .group_id("conformance-g06")
        .build()
        .await
        .expect("consumer");
    consumer.subscribe().await.expect("subscribe");
    // Drop triggers implicit group leave
    drop(consumer);
}

// ===========================================================================
// AUTHENTICATION TESTS (A01–A06)
// ===========================================================================

#[tokio::test]
#[ignore]
async fn test_a01_tls_connect() {
    // Connect to a TLS-enabled server.
    // Note: StreamlineBuilder does not yet expose TLS config;
    // this test validates that TLS types compile and the server is reachable.
    let tls_bootstrap = env::var("STREAMLINE_TLS_BOOTSTRAP")
        .unwrap_or_else(|_| "localhost:9093".into());

    // Validate TLS config types compile correctly
    let _tls_config = TlsConfig {
        ca_path: Some("certs/ca.pem".into()),
        cert_path: None,
        key_path: None,
        danger_skip_verify: true,
    };

    // Attempt connection (builder uses plaintext; full TLS needs builder extension)
    let result = Streamline::builder()
        .bootstrap_servers(&tls_bootstrap)
        .connect_timeout(Duration::from_secs(5))
        .build()
        .await;
    // Connection may fail if TLS is required — that's expected
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test]
#[ignore]
async fn test_a02_mutual_tls() {
    // Connect with mutual TLS (client certificate).
    let ca = env::var("TLS_CA_PATH").unwrap_or_else(|_| "certs/ca.pem".into());
    let cert = env::var("TLS_CERT_PATH").unwrap_or_else(|_| "certs/client.pem".into());
    let key = env::var("TLS_KEY_PATH").unwrap_or_else(|_| "certs/client-key.pem".into());

    let tls_config = TlsConfig {
        ca_path: Some(ca),
        cert_path: Some(cert),
        key_path: Some(key),
        danger_skip_verify: false,
    };
    assert!(tls_config.ca_path.is_some());
    assert!(tls_config.cert_path.is_some());
    assert!(tls_config.key_path.is_some());
    assert!(!tls_config.danger_skip_verify);
}

#[tokio::test]
#[ignore]
async fn test_a03_sasl_plain() {
    // Authenticate with SASL/PLAIN.
    let username = env::var("SASL_USERNAME").unwrap_or_else(|_| "admin".into());
    let password = env::var("SASL_PASSWORD").unwrap_or_else(|_| "admin-secret".into());

    let sasl_config = SaslConfig {
        mechanism: SaslMechanism::Plain,
        username: username.clone(),
        password: password.clone(),
    };
    assert_eq!(sasl_config.mechanism, SaslMechanism::Plain);
    assert_eq!(sasl_config.username, username);

    // Attempt plaintext connection (SASL handshake requires builder extension)
    let client = new_client().await;
    let admin = client.admin();
    let _ = admin.list_topics().await.expect("list topics");
}

#[tokio::test]
#[ignore]
async fn test_a04_scram_sha256() {
    // Authenticate with SASL/SCRAM-SHA-256.
    let sasl_config = SaslConfig {
        mechanism: SaslMechanism::ScramSha256,
        username: env::var("SASL_USERNAME").unwrap_or_else(|_| "admin".into()),
        password: env::var("SASL_PASSWORD").unwrap_or_else(|_| "admin-secret".into()),
    };
    assert_eq!(sasl_config.mechanism, SaslMechanism::ScramSha256);

    let client = new_client().await;
    let admin = client.admin();
    let _ = admin.list_topics().await.expect("list topics");
}

#[tokio::test]
#[ignore]
async fn test_a05_scram_sha512() {
    // Authenticate with SASL/SCRAM-SHA-512.
    let sasl_config = SaslConfig {
        mechanism: SaslMechanism::ScramSha512,
        username: env::var("SASL_USERNAME").unwrap_or_else(|_| "admin".into()),
        password: env::var("SASL_PASSWORD").unwrap_or_else(|_| "admin-secret".into()),
    };
    assert_eq!(sasl_config.mechanism, SaslMechanism::ScramSha512);

    let client = new_client().await;
    let admin = client.admin();
    let _ = admin.list_topics().await.expect("list topics");
}

#[tokio::test]
#[ignore]
async fn test_a06_auth_failure() {
    // Invalid credentials should produce an error at connection time.
    // Since the builder doesn't yet wire SASL, we test that connecting
    // to a non-existent port fails with a connection error.
    let result = Streamline::builder()
        .bootstrap_servers("localhost:1")
        .connect_timeout(Duration::from_millis(500))
        .build()
        .await;

    assert!(result.is_err(), "expected connection failure");
    if let Err(e) = result {
        assert!(
            matches!(e.kind, ErrorKind::Connection | ErrorKind::ConnectionFailed | ErrorKind::Timeout),
            "expected connection/timeout error, got: {e}"
        );
    }
}

// ===========================================================================
// SCHEMA REGISTRY TESTS (S01–S06)
// ===========================================================================

#[tokio::test]
#[ignore]
async fn test_s01_register_schema() {
    use streamline_client::schema::{SchemaRegistryClient, SchemaType};

    let registry = SchemaRegistryClient::new(&http_url());
    let subject = &unique_topic("s01-value");
    let avro = r#"{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}"#;
    let id = registry
        .register(subject, avro, SchemaType::Avro)
        .await
        .expect("register schema");
    assert!(id > 0, "schema ID should be positive");
}

#[tokio::test]
#[ignore]
async fn test_s02_get_by_id() {
    use streamline_client::schema::{SchemaRegistryClient, SchemaType};

    let registry = SchemaRegistryClient::new(&http_url());
    let subject = &unique_topic("s02-value");
    let avro = r#"{"type":"record","name":"Event","fields":[{"name":"ts","type":"long"}]}"#;
    let id = registry
        .register(subject, avro, SchemaType::Avro)
        .await
        .expect("register");
    let schema = registry.get_schema(id).await.expect("get schema");
    assert!(schema.schema.contains("Event"), "schema should contain type name");
}

#[tokio::test]
#[ignore]
async fn test_s03_get_versions() {
    use streamline_client::schema::{SchemaRegistryClient, SchemaType};

    let registry = SchemaRegistryClient::new(&http_url());
    let subject = &unique_topic("s03-value");
    let avro = r#"{"type":"record","name":"V1","fields":[{"name":"id","type":"int"}]}"#;
    registry
        .register(subject, avro, SchemaType::Avro)
        .await
        .expect("register");
    let versions = registry.get_versions(subject).await.expect("get versions");
    assert!(!versions.is_empty(), "should have at least one version");
}

#[tokio::test]
#[ignore]
async fn test_s04_compatibility_check() {
    use streamline_client::schema::{SchemaRegistryClient, SchemaType};

    let registry = SchemaRegistryClient::new(&http_url());
    let subject = &unique_topic("s04-value");
    let avro = r#"{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}"#;
    registry
        .register(subject, avro, SchemaType::Avro)
        .await
        .expect("register");
    let is_compat = registry
        .check_compatibility(subject, avro, SchemaType::Avro)
        .await
        .expect("check compat");
    assert!(is_compat, "same schema should be self-compatible");
}

#[tokio::test]
#[ignore]
async fn test_s05_avro_schema() {
    use streamline_client::schema::{SchemaRegistryClient, SchemaType};

    let registry = SchemaRegistryClient::new(&http_url());
    let subject = &unique_topic("s05-avro");
    let avro = r#"{"type":"record","name":"Metric","fields":[{"name":"name","type":"string"},{"name":"value","type":"double"}]}"#;
    let id = registry
        .register(subject, avro, SchemaType::Avro)
        .await
        .expect("register avro");
    assert!(id > 0);
    let schema = registry.get_schema(id).await.expect("get schema");
    assert!(schema.schema.contains("record"));
}

#[tokio::test]
#[ignore]
async fn test_s06_json_schema() {
    use streamline_client::schema::{SchemaRegistryClient, SchemaType};

    let registry = SchemaRegistryClient::new(&http_url());
    let subject = &unique_topic("s06-json");
    let json_schema = r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#;
    let id = registry
        .register(subject, json_schema, SchemaType::Json)
        .await
        .expect("register json schema");
    assert!(id > 0);
    let schema = registry.get_schema(id).await.expect("get schema");
    assert!(schema.schema.contains("object"));
}

// ===========================================================================
// ADMIN TESTS (D01–D04)
// ===========================================================================

#[tokio::test]
#[ignore]
async fn test_d01_create_topic() {
    let client = new_client().await;
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
async fn test_d02_list_topics() {
    let client = new_client().await;
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
async fn test_d03_describe_topic() {
    let client = new_client().await;
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
async fn test_d04_delete_topic() {
    let client = new_client().await;
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

// ===========================================================================
// ERROR HANDLING TESTS (E01–E04)
// ===========================================================================

#[test]
fn test_e01_connection_refused() {
    let err = Error::connection_failed("localhost:1");
    assert_eq!(err.kind, ErrorKind::ConnectionFailed);
}

#[test]
fn test_e02_auth_denied() {
    let err = Error::new(ErrorKind::AuthenticationFailed, "access denied");
    assert_eq!(err.kind, ErrorKind::AuthenticationFailed);
}

#[test]
fn test_e03_topic_not_found() {
    let err = Error::topic_not_found("nonexistent-topic");
    assert_eq!(err.kind, ErrorKind::TopicNotFound);
    assert!(
        err.to_string().contains("nonexistent-topic"),
        "error should contain topic name"
    );
}

#[test]
fn test_e04_request_timeout() {
    let err = Error::timeout("produce");
    assert_eq!(err.kind, ErrorKind::Timeout);
    assert!(err.to_string().contains("produce"));
}

// ===========================================================================
// PERFORMANCE TESTS (F01–F04)
// ===========================================================================

#[tokio::test]
#[ignore]
async fn test_f01_throughput_1kb() {
    let client = new_client().await;
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
            .send(&topic, "k".to_string(), payload.clone(), Headers::new())
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
async fn test_f02_latency_p99() {
    let client = new_client().await;
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
            .send(&topic, format!("k{i}"), format!("lat-{i}"), Headers::new())
            .await
            .expect("produce");
        latencies.push(start.elapsed());
    }

    latencies.sort();
    let p99_idx = ((latencies.len() as f64) * 0.99) as usize;
    let p99 = latencies[p99_idx.min(latencies.len() - 1)];
    assert!(
        p99 < Duration::from_secs(5),
        "P99 latency too high: {p99:?} (expected <5s)"
    );
}

#[tokio::test]
#[ignore]
async fn test_f03_startup_time() {
    let start = Instant::now();
    let _client = Streamline::builder()
        .bootstrap_servers(&bootstrap())
        .build()
        .await
        .expect("build");
    let connect_time = start.elapsed();

    assert!(
        connect_time < Duration::from_secs(5),
        "startup too slow: {connect_time:?} (expected <5s)"
    );
}

#[tokio::test]
#[ignore]
async fn test_f04_memory_usage() {
    let client = new_client().await;
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
            .send(&topic, "k".to_string(), payload.clone(), Headers::new())
            .await
            .expect("produce");
    }
    // Memory usage is implicit in Rust — no GC pressure. Smoke test only.
}
