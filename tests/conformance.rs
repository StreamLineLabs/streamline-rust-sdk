//! Offline release conformance tests.
//!
//! These tests exercise the public API without requiring a live Streamline
//! cluster. Protocol-level integration remains a separate, explicitly
//! provisioned concern; this suite verifies that version 0.4.0 never reports
//! simulated success for unsupported operations.

use std::time::Duration;

use streamline_client::query::{QueryClient, QueryRequest};
use streamline_client::{
    ConsumerConfig, ErrorKind, Headers, ProducerConfig, ProducerRecord, SaslConfig, SaslMechanism,
    SecurityProtocol, Streamline, StreamlineConfig, TopicConfig,
};

async fn client() -> Streamline {
    Streamline::builder()
        .bootstrap_servers("127.0.0.1:1")
        .connect_timeout(Duration::from_millis(50))
        .build()
        .await
        .expect("plaintext client construction should not perform I/O")
}

#[test]
fn config_debug_never_exposes_sasl_credentials() {
    let config = StreamlineConfig {
        security_protocol: SecurityProtocol::SaslPlaintext,
        sasl: Some(SaslConfig {
            mechanism: SaslMechanism::ScramSha256,
            username: "conformance-username-secret".to_string(),
            password: "conformance-password-secret".to_string(),
        }),
        ..Default::default()
    };

    let debug = format!("{config:?}");
    assert!(!debug.contains("conformance-username-secret"));
    assert!(!debug.contains("conformance-password-secret"));
    assert!(debug.contains("[REDACTED]"));
}

#[tokio::test]
async fn direct_consumer_requires_explicit_partitions() {
    let client = client().await;
    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>("events")
        .partitions(vec![0])
        .auto_offset_reset("earliest")
        .enable_auto_commit(false)
        .build()
        .await
        .expect("direct consumer configuration should be supported");

    consumer
        .subscribe()
        .await
        .expect("explicit subscription should not perform metadata I/O");
    assert_eq!(consumer.assigned_partitions(), &[0]);

    let missing_partition_error = client
        .consumer::<Vec<u8>, Vec<u8>>("events")
        .build()
        .await
        .expect("consumer construction should remain lazy")
        .subscribe()
        .await
        .unwrap_err();
    assert_eq!(missing_partition_error.kind, ErrorKind::Unsupported);
}

#[tokio::test]
async fn consumer_groups_commits_and_latest_offsets_fail_closed() {
    let client = client().await;

    let group_error = client
        .consumer::<Vec<u8>, Vec<u8>>("events")
        .group_id("release-group")
        .partitions(vec![0])
        .build()
        .await
        .err()
        .expect("consumer groups must be rejected");
    assert_eq!(group_error.kind, ErrorKind::Unsupported);

    let auto_commit_error = client
        .consumer::<Vec<u8>, Vec<u8>>("events")
        .enable_auto_commit(true)
        .partitions(vec![0])
        .build()
        .await
        .err()
        .expect("automatic commits must be rejected");
    assert_eq!(auto_commit_error.kind, ErrorKind::Unsupported);

    let latest_error = client
        .consumer::<Vec<u8>, Vec<u8>>("events")
        .auto_offset_reset("latest")
        .partitions(vec![0])
        .build()
        .await
        .err()
        .expect("latest offsets must be rejected");
    assert_eq!(latest_error.kind, ErrorKind::Unsupported);

    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>("events")
        .partitions(vec![0])
        .build()
        .await
        .expect("direct consumer should build");
    consumer.subscribe().await.expect("subscribe");
    assert_eq!(
        consumer.commit().await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        consumer.commit_async().unwrap_err().kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        consumer.seek_to_end().await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        consumer.seek(0, -1).await.unwrap_err().kind,
        ErrorKind::InvalidConfiguration
    );
}

#[tokio::test]
async fn kafka_admin_surface_fails_closed_without_connecting() {
    let client = client().await;
    let admin = client.admin();

    assert_eq!(
        admin
            .create_topic(TopicConfig::new("events"))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        admin.delete_topic("events").await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        admin.list_topics().await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        admin.describe_topic("events").await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        admin.add_partitions("events", 2).await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        admin.list_consumer_groups().await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        admin
            .describe_consumer_group("group")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        admin.delete_consumer_group("group").await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        admin.list_brokers().await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
    assert!(!client.is_healthy().await);
}

#[tokio::test]
async fn producer_transactions_and_compression_fail_closed() {
    let client = client().await;
    let mut producer = client.producer::<Vec<u8>, Vec<u8>>();

    assert_eq!(
        producer.begin_transaction().unwrap_err().kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        producer
            .send_transactional(
                "events",
                ProducerRecord::new(b"key".to_vec(), b"value".to_vec()),
            )
            .unwrap_err()
            .kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        producer.commit_transaction().await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        producer.abort_transaction().unwrap_err().kind,
        ErrorKind::Unsupported
    );

    let compressed = client.producer_with_config::<Vec<u8>, Vec<u8>>(ProducerConfig {
        compression: "zstd".to_string(),
        ..Default::default()
    });
    let compression_error = compressed
        .send("events", b"key".to_vec(), b"value".to_vec(), Headers::new())
        .await
        .unwrap_err();
    assert_eq!(compression_error.kind, ErrorKind::Unsupported);
    assert!(!client.is_healthy().await);
}

#[tokio::test]
async fn query_execution_fails_closed() {
    let query = QueryClient::new("http://127.0.0.1:1");
    let error = query
        .execute(&QueryRequest::new("SELECT 1"))
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Unsupported);
}

#[test]
fn consumer_defaults_do_not_claim_automatic_commits() {
    let config = ConsumerConfig::default();
    assert_eq!(config.auto_offset_reset, "earliest");
    assert!(!config.enable_auto_commit);
    assert!(config.group_id.is_none());
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn tls_configuration_fails_before_network_io() {
    let error = Streamline::builder()
        .bootstrap_servers("127.0.0.1:1")
        .tls_config(Default::default())
        .build()
        .await
        .err()
        .expect("TLS must be rejected");
    assert_eq!(error.kind, ErrorKind::Unsupported);
}

#[cfg(feature = "sasl")]
#[tokio::test]
async fn sasl_configuration_fails_before_network_io() {
    let error = Streamline::builder()
        .bootstrap_servers("127.0.0.1:1")
        .sasl_config(SaslConfig {
            mechanism: SaslMechanism::Plain,
            username: String::new(),
            password: String::new(),
        })
        .build()
        .await
        .err()
        .expect("SASL must be rejected");
    assert_eq!(error.kind, ErrorKind::Unsupported);
}
