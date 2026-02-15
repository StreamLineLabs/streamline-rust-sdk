//! Configuration types for the Streamline client.

use std::time::Duration;

/// Client configuration.
#[derive(Debug, Clone)]
pub struct StreamlineConfig {
    /// Bootstrap servers
    pub bootstrap_servers: String,
    /// Connection pool size
    pub connection_pool_size: usize,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
}

impl Default for StreamlineConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            connection_pool_size: 4,
            connect_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
        }
    }
}

/// Producer configuration.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Batch size in bytes
    pub batch_size: usize,
    /// Linger time in milliseconds
    pub linger_ms: u64,
    /// Maximum request size
    pub max_request_size: usize,
    /// Compression type (none, gzip, lz4, snappy, zstd)
    pub compression: String,
    /// Number of retries
    pub retries: u32,
    /// Retry backoff in milliseconds
    pub retry_backoff_ms: u64,
    /// Enable idempotent producer
    pub idempotent: bool,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            batch_size: 16384,
            linger_ms: 1,
            max_request_size: 1048576,
            compression: "none".to_string(),
            retries: 3,
            retry_backoff_ms: 100,
            idempotent: false,
        }
    }
}

/// Consumer configuration.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Consumer group ID
    pub group_id: Option<String>,
    /// Auto offset reset (earliest, latest)
    pub auto_offset_reset: String,
    /// Enable auto-commit
    pub enable_auto_commit: bool,
    /// Auto-commit interval
    pub auto_commit_interval: Duration,
    /// Session timeout
    pub session_timeout: Duration,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Maximum records per poll
    pub max_poll_records: usize,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: None,
            auto_offset_reset: "earliest".to_string(),
            enable_auto_commit: true,
            auto_commit_interval: Duration::from_secs(5),
            session_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(3),
            max_poll_records: 500,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streamline_config_default() {
        let config = StreamlineConfig::default();
        assert_eq!(config.bootstrap_servers, "localhost:9092");
        assert_eq!(config.connection_pool_size, 4);
        assert_eq!(config.connect_timeout, Duration::from_secs(30));
        assert_eq!(config.request_timeout, Duration::from_secs(30));
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
    fn test_config_clone() {
        let config = StreamlineConfig {
            bootstrap_servers: "broker1:9092".to_string(),
            ..Default::default()
        };
        let cloned = config.clone();
        assert_eq!(config.bootstrap_servers, cloned.bootstrap_servers);
    }
}
