use std::time::Duration;
use streamline_client::{ConsumerConfig, ProducerConfig, StreamlineConfig};

#[test]
fn test_streamline_config_default() {
    let config = StreamlineConfig::default();
    assert_eq!(config.bootstrap_servers, "localhost:9092");
    assert_eq!(config.connection_pool_size, 4);
    assert_eq!(config.connect_timeout, Duration::from_secs(30));
    assert_eq!(config.request_timeout, Duration::from_secs(30));
}

#[test]
fn test_streamline_config_custom() {
    let config = StreamlineConfig {
        bootstrap_servers: "broker1:9092,broker2:9092".to_string(),
        connection_pool_size: 8,
        connect_timeout: Duration::from_secs(10),
        request_timeout: Duration::from_secs(60),
        ..Default::default()
    };
    assert_eq!(config.bootstrap_servers, "broker1:9092,broker2:9092");
    assert_eq!(config.connection_pool_size, 8);
    assert_eq!(config.connect_timeout, Duration::from_secs(10));
    assert_eq!(config.request_timeout, Duration::from_secs(60));
}

#[test]
fn test_producer_config_default() {
    let config = ProducerConfig::default();
    assert_eq!(config.batch_size, 16384);
    assert_eq!(config.linger_ms, 1);
    assert_eq!(config.max_request_size, 1048576);
    assert_eq!(config.compression, "none");
    assert_eq!(config.retries, 3);
    assert_eq!(config.retry_backoff_ms, 100);
    assert!(!config.idempotent);
}

#[test]
fn test_producer_config_custom_batch_size() {
    let config = ProducerConfig {
        batch_size: 65536,
        ..Default::default()
    };
    assert_eq!(config.batch_size, 65536);
    // Other fields retain defaults
    assert_eq!(config.linger_ms, 1);
    assert_eq!(config.retries, 3);
}

#[test]
fn test_producer_config_compression() {
    for compression in &["none", "gzip", "lz4", "snappy", "zstd"] {
        let config = ProducerConfig {
            compression: compression.to_string(),
            ..Default::default()
        };
        assert_eq!(config.compression, *compression);
    }
}

#[test]
fn test_consumer_config_default() {
    let config = ConsumerConfig::default();
    assert!(config.group_id.is_none());
    assert_eq!(config.auto_offset_reset, "earliest");
    assert!(config.enable_auto_commit);
    assert_eq!(config.auto_commit_interval, Duration::from_secs(5));
    assert_eq!(config.session_timeout, Duration::from_secs(30));
    assert_eq!(config.heartbeat_interval, Duration::from_secs(3));
    assert_eq!(config.max_poll_records, 500);
}

#[test]
fn test_consumer_config_custom_group() {
    let config = ConsumerConfig {
        group_id: Some("my-consumer-group".to_string()),
        ..Default::default()
    };
    assert_eq!(config.group_id, Some("my-consumer-group".to_string()));
}

#[test]
fn test_consumer_config_auto_offset_reset() {
    let earliest = ConsumerConfig {
        auto_offset_reset: "earliest".to_string(),
        ..Default::default()
    };
    assert_eq!(earliest.auto_offset_reset, "earliest");

    let latest = ConsumerConfig {
        auto_offset_reset: "latest".to_string(),
        ..Default::default()
    };
    assert_eq!(latest.auto_offset_reset, "latest");
}

#[test]
fn test_streamline_config_clone() {
    let config = StreamlineConfig {
        bootstrap_servers: "custom:9092".to_string(),
        connection_pool_size: 16,
        ..Default::default()
    };
    let cloned = config.clone();
    assert_eq!(config.bootstrap_servers, cloned.bootstrap_servers);
    assert_eq!(config.connection_pool_size, cloned.connection_pool_size);
    assert_eq!(config.connect_timeout, cloned.connect_timeout);
    assert_eq!(config.request_timeout, cloned.request_timeout);
}

#[test]
fn test_producer_config_clone() {
    let config = ProducerConfig {
        batch_size: 32768,
        compression: "lz4".to_string(),
        idempotent: true,
        ..Default::default()
    };
    let cloned = config.clone();
    assert_eq!(config.batch_size, cloned.batch_size);
    assert_eq!(config.compression, cloned.compression);
    assert_eq!(config.idempotent, cloned.idempotent);
}

#[test]
fn test_consumer_config_clone() {
    let config = ConsumerConfig {
        group_id: Some("group-1".to_string()),
        enable_auto_commit: false,
        max_poll_records: 1000,
        ..Default::default()
    };
    let cloned = config.clone();
    assert_eq!(config.group_id, cloned.group_id);
    assert_eq!(config.enable_auto_commit, cloned.enable_auto_commit);
    assert_eq!(config.max_poll_records, cloned.max_poll_records);
}

#[test]
fn test_producer_config_debug() {
    let config = ProducerConfig::default();
    let debug = format!("{:?}", config);
    assert!(debug.contains("batch_size"));
    assert!(debug.contains("compression"));
}

#[test]
fn test_consumer_config_debug() {
    let config = ConsumerConfig::default();
    let debug = format!("{:?}", config);
    assert!(debug.contains("auto_offset_reset"));
    assert!(debug.contains("max_poll_records"));
}

#[test]
fn test_streamline_config_debug() {
    let config = StreamlineConfig::default();
    let debug = format!("{:?}", config);
    assert!(debug.contains("bootstrap_servers"));
    assert!(debug.contains("connection_pool_size"));
}

#[test]
fn test_consumer_config_custom_timeouts() {
    let config = ConsumerConfig {
        session_timeout: Duration::from_secs(60),
        heartbeat_interval: Duration::from_secs(10),
        auto_commit_interval: Duration::from_secs(15),
        ..Default::default()
    };
    assert_eq!(config.session_timeout, Duration::from_secs(60));
    assert_eq!(config.heartbeat_interval, Duration::from_secs(10));
    assert_eq!(config.auto_commit_interval, Duration::from_secs(15));
}

#[test]
fn test_producer_config_idempotent() {
    let config = ProducerConfig {
        idempotent: true,
        ..Default::default()
    };
    assert!(config.idempotent);
}

#[test]
fn test_producer_config_retry_settings() {
    let config = ProducerConfig {
        retries: 10,
        retry_backoff_ms: 500,
        ..Default::default()
    };
    assert_eq!(config.retries, 10);
    assert_eq!(config.retry_backoff_ms, 500);
}
