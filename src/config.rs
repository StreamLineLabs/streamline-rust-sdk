//! Configuration types for the Streamline client.

use std::time::Duration;

/// Client configuration.
#[derive(Debug, Clone)]
pub struct StreamlineConfig {
    /// Bootstrap servers
    pub bootstrap_servers: String,
    /// HTTP endpoint for REST API operations (default: derived from bootstrap_servers on port 9094)
    pub http_endpoint: Option<String>,
    /// Connection pool size
    pub connection_pool_size: usize,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// Security protocol
    pub security_protocol: SecurityProtocol,
    /// TLS configuration (used when security_protocol is Ssl or SaslSsl)
    pub tls: Option<TlsConfig>,
    /// SASL configuration (used when security_protocol is SaslPlaintext or SaslSsl)
    pub sasl: Option<SaslConfig>,
}

impl Default for StreamlineConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: std::env::var("STREAMLINE_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".to_string()),
            http_endpoint: std::env::var("STREAMLINE_HTTP").ok(),
            connection_pool_size: 4,
            connect_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
            security_protocol: SecurityProtocol::default(),
            tls: None,
            sasl: None,
        }
    }
}

impl StreamlineConfig {
    /// Returns the HTTP endpoint URL, falling back to deriving it from
    /// `bootstrap_servers` on port 9094.
    pub fn http_base_url(&self) -> String {
        if let Some(ref url) = self.http_endpoint {
            return url.trim_end_matches('/').to_string();
        }
        let host = self
            .bootstrap_servers
            .split(',')
            .next()
            .unwrap_or("localhost")
            .split(':')
            .next()
            .unwrap_or("localhost");
        format!("http://{}:9094", host)
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

/// TLS configuration for secure connections.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to CA certificate file (PEM format)
    pub ca_path: Option<String>,
    /// Path to client certificate file (PEM format)
    pub cert_path: Option<String>,
    /// Path to client private key file (PEM format)
    pub key_path: Option<String>,
    /// Skip server certificate verification (NOT recommended for production)
    pub danger_skip_verify: bool,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            ca_path: None,
            cert_path: None,
            key_path: None,
            danger_skip_verify: false,
        }
    }
}

/// SASL mechanism for authentication.
#[derive(Debug, Clone, PartialEq)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

/// SASL authentication configuration.
#[derive(Debug, Clone)]
pub struct SaslConfig {
    /// SASL mechanism
    pub mechanism: SaslMechanism,
    /// Username
    pub username: String,
    /// Password
    pub password: String,
}

/// Security protocol for connections.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum SecurityProtocol {
    #[default]
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streamline_config_default() {
        let config = StreamlineConfig::default();
        assert_eq!(config.bootstrap_servers, "localhost:9092");
        assert!(config.http_endpoint.is_none());
        assert_eq!(config.connection_pool_size, 4);
        assert_eq!(config.connect_timeout, Duration::from_secs(30));
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.security_protocol, SecurityProtocol::Plaintext);
        assert!(config.tls.is_none());
        assert!(config.sasl.is_none());
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

    #[test]
    fn test_security_config() {
        let config = StreamlineConfig {
            security_protocol: SecurityProtocol::SaslSsl,
            tls: Some(TlsConfig {
                ca_path: Some("/path/to/ca.pem".to_string()),
                cert_path: Some("/path/to/cert.pem".to_string()),
                key_path: Some("/path/to/key.pem".to_string()),
                danger_skip_verify: false,
            }),
            sasl: Some(SaslConfig {
                mechanism: SaslMechanism::ScramSha256,
                username: "admin".to_string(),
                password: "secret".to_string(),
            }),
            ..Default::default()
        };
        assert_eq!(config.security_protocol, SecurityProtocol::SaslSsl);
        let tls = config.tls.unwrap();
        assert_eq!(tls.ca_path.as_deref(), Some("/path/to/ca.pem"));
        assert!(!tls.danger_skip_verify);
        let sasl = config.sasl.unwrap();
        assert_eq!(sasl.mechanism, SaslMechanism::ScramSha256);
        assert_eq!(sasl.username, "admin");
    }

    #[test]
    fn test_http_base_url_from_config() {
        let config = StreamlineConfig {
            http_endpoint: Some("http://custom:8080".to_string()),
            ..Default::default()
        };
        assert_eq!(config.http_base_url(), "http://custom:8080");
    }

    #[test]
    fn test_http_base_url_strips_trailing_slash() {
        let config = StreamlineConfig {
            http_endpoint: Some("http://custom:8080/".to_string()),
            ..Default::default()
        };
        assert_eq!(config.http_base_url(), "http://custom:8080");
    }

    #[test]
    fn test_http_base_url_derived_from_bootstrap() {
        let config = StreamlineConfig {
            bootstrap_servers: "broker1:9092".to_string(),
            http_endpoint: None,
            ..Default::default()
        };
        assert_eq!(config.http_base_url(), "http://broker1:9094");
    }

    #[test]
    fn test_http_base_url_derived_from_first_bootstrap() {
        let config = StreamlineConfig {
            bootstrap_servers: "host1:9092,host2:9092".to_string(),
            http_endpoint: None,
            ..Default::default()
        };
        assert_eq!(config.http_base_url(), "http://host1:9094");
    }
}
