//! Main Streamline client.

use crate::admin::Admin;
use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::config::{
    ConsumerConfig, ProducerConfig, SaslConfig, SecurityProtocol, StreamlineConfig, TlsConfig,
};
use crate::connection::ConnectionPool;
use crate::consumer::Consumer;
use crate::error::{Error, Result};
use crate::producer::{Producer, RecordMetadata};
use crate::Headers;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Main entry point for the Streamline client.
///
/// # Example
///
/// ```rust,no_run
/// use streamline_client::Streamline;
///
/// #[tokio::main]
/// async fn main() -> Result<(), streamline_client::Error> {
///     let client = Streamline::builder()
///         .bootstrap_servers("localhost:9092")
///         .build()
///         .await?;
///
///     client.produce("my-topic", "key", "value").await?;
///     Ok(())
/// }
/// ```
pub struct Streamline {
    config: Arc<StreamlineConfig>,
    pool: Arc<ConnectionPool>,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
}

impl Streamline {
    /// Creates a new builder.
    pub fn builder() -> StreamlineBuilder {
        StreamlineBuilder::default()
    }

    /// Produces a message to a topic.
    pub async fn produce(&self, topic: &str, key: &str, value: &str) -> Result<RecordMetadata> {
        self.produce_with_headers(topic, key, value, Headers::new())
            .await
    }

    /// Produces a message with headers.
    pub async fn produce_with_headers(
        &self,
        topic: &str,
        key: &str,
        value: &str,
        headers: Headers,
    ) -> Result<RecordMetadata> {
        let producer = self.producer::<String, String>();
        producer
            .send(topic, key.to_string(), value.to_string(), headers)
            .await
    }

    /// Creates a producer with default configuration.
    pub fn producer<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send>(&self) -> Producer<K, V> {
        self.producer_with_config(ProducerConfig::default())
    }

    /// Creates a producer with custom configuration.
    pub fn producer_with_config<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send>(
        &self,
        config: ProducerConfig,
    ) -> Producer<K, V> {
        match &self.circuit_breaker {
            Some(cb) => Producer::with_circuit_breaker(
                self.config.clone(),
                self.pool.clone(),
                config,
                cb.clone(),
            ),
            None => Producer::new(self.config.clone(), self.pool.clone(), config),
        }
    }

    /// Creates a consumer builder for a topic.
    pub fn consumer<K, V>(&self, topic: &str) -> ConsumerBuilder<K, V> {
        ConsumerBuilder::new(self.config.clone(), self.pool.clone(), topic.to_string())
    }

    /// Returns the client configuration.
    pub fn config(&self) -> &StreamlineConfig {
        &self.config
    }

    /// Checks if the client is connected and healthy.
    ///
    /// Returns `true` if at least one pooled connection is currently alive.
    /// Note: before any produce/consume call, connections are lazily
    /// initialized, so this may return `false` on a freshly-built client.
    pub async fn is_healthy(&self) -> bool {
        self.pool.is_healthy().await
    }

    /// Returns a reference to the connection pool.
    pub fn pool(&self) -> &ConnectionPool {
        &self.pool
    }

    /// Creates an admin client for cluster management operations.
    pub fn admin(&self) -> Admin {
        Admin::new(self.config.clone(), self.pool.clone())
    }
}

/// Builder for Streamline client.
#[derive(Default)]
pub struct StreamlineBuilder {
    bootstrap_servers: Option<String>,
    http_endpoint: Option<String>,
    connection_pool_size: Option<usize>,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
    security_protocol: SecurityProtocol,
    tls: Option<TlsConfig>,
    sasl: Option<SaslConfig>,
    circuit_breaker: Option<CircuitBreakerConfig>,
}

impl StreamlineBuilder {
    /// Sets the bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: &str) -> Self {
        self.bootstrap_servers = Some(servers.to_string());
        self
    }

    /// Sets the HTTP endpoint URL for REST API operations.
    ///
    /// If not set, the HTTP endpoint is derived from `bootstrap_servers` on port 9094.
    pub fn http_endpoint(mut self, url: &str) -> Self {
        self.http_endpoint = Some(url.to_string());
        self
    }

    /// Sets the connection pool size.
    pub fn connection_pool_size(mut self, size: usize) -> Self {
        self.connection_pool_size = Some(size);
        self
    }

    /// Sets the connection timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Sets the request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Configures TLS for the broker connection.
    ///
    /// TLS transport is not implemented in version 0.4.0. Calling this method
    /// records the requested configuration so [`build`](Self::build) can fail
    /// explicitly instead of silently opening a plaintext connection.
    #[cfg(feature = "tls")]
    pub fn tls_config(mut self, config: TlsConfig) -> Self {
        self.security_protocol = match self.security_protocol {
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl => {
                SecurityProtocol::SaslSsl
            }
            SecurityProtocol::Plaintext | SecurityProtocol::Ssl => SecurityProtocol::Ssl,
        };
        self.tls = Some(config);
        self
    }

    /// Configures SASL authentication for the broker connection.
    ///
    /// SASL authentication is not implemented in version 0.4.0. Calling this
    /// method records the requested configuration so [`build`](Self::build)
    /// can fail explicitly instead of silently connecting without
    /// authentication.
    #[cfg(feature = "sasl")]
    pub fn sasl_config(mut self, config: SaslConfig) -> Self {
        self.security_protocol = match self.security_protocol {
            SecurityProtocol::Ssl | SecurityProtocol::SaslSsl => SecurityProtocol::SaslSsl,
            SecurityProtocol::Plaintext | SecurityProtocol::SaslPlaintext => {
                SecurityProtocol::SaslPlaintext
            }
        };
        self.sasl = Some(config);
        self
    }

    /// Enables circuit breaker with default configuration.
    pub fn with_circuit_breaker(mut self) -> Self {
        self.circuit_breaker = Some(CircuitBreakerConfig::default());
        self
    }

    /// Enables circuit breaker with custom configuration.
    pub fn with_circuit_breaker_config(mut self, config: CircuitBreakerConfig) -> Self {
        self.circuit_breaker = Some(config);
        self
    }

    /// Builds the client.
    pub async fn build(self) -> Result<Streamline> {
        let bootstrap_servers = self.bootstrap_servers.ok_or_else(|| {
            Error::new(
                crate::error::ErrorKind::InvalidConfiguration,
                "bootstrap_servers is required",
            )
        })?;

        match &self.security_protocol {
            SecurityProtocol::Plaintext => {}
            SecurityProtocol::Ssl => {
                return Err(Error::unsupported("TLS broker transport"));
            }
            SecurityProtocol::SaslPlaintext => {
                return Err(Error::unsupported("SASL broker authentication"));
            }
            SecurityProtocol::SaslSsl => {
                return Err(Error::unsupported(
                    "TLS broker transport with SASL authentication",
                ));
            }
        }

        let config = StreamlineConfig {
            bootstrap_servers: bootstrap_servers.clone(),
            http_endpoint: self.http_endpoint,
            connection_pool_size: self.connection_pool_size.unwrap_or(4),
            connect_timeout: self.connect_timeout.unwrap_or(Duration::from_secs(30)),
            request_timeout: self.request_timeout.unwrap_or(Duration::from_secs(30)),
            security_protocol: self.security_protocol,
            tls: self.tls,
            sasl: self.sasl,
        };

        let config = Arc::new(config);
        let pool = Arc::new(ConnectionPool::new(&config));
        let circuit_breaker = self
            .circuit_breaker
            .map(|cb_config| Arc::new(CircuitBreaker::new(cb_config)));

        info!(
            "Streamline client created for {} (pool_size={}, circuit_breaker={})",
            bootstrap_servers,
            pool.size(),
            circuit_breaker.is_some(),
        );

        Ok(Streamline {
            config,
            pool,
            circuit_breaker,
        })
    }
}

/// Builder for consumers.
pub struct ConsumerBuilder<K, V> {
    client_config: Arc<StreamlineConfig>,
    pool: Arc<ConnectionPool>,
    topic: String,
    config: ConsumerConfig,
    partitions: Vec<i32>,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> ConsumerBuilder<K, V> {
    fn new(client_config: Arc<StreamlineConfig>, pool: Arc<ConnectionPool>, topic: String) -> Self {
        Self {
            client_config,
            pool,
            topic,
            config: ConsumerConfig::default(),
            partitions: Vec::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Sets the consumer group ID.
    pub fn group_id(mut self, group_id: &str) -> Self {
        self.config.group_id = Some(group_id.to_string());
        self
    }

    /// Sets the auto offset reset policy.
    pub fn auto_offset_reset(mut self, policy: &str) -> Self {
        self.config.auto_offset_reset = policy.to_string();
        self
    }

    /// Enables or disables auto-commit.
    pub fn enable_auto_commit(mut self, enable: bool) -> Self {
        self.config.enable_auto_commit = enable;
        self
    }

    /// Sets the maximum records per poll.
    pub fn max_poll_records(mut self, max: usize) -> Self {
        self.config.max_poll_records = max;
        self
    }

    /// Sets the session timeout.
    pub fn session_timeout(mut self, timeout: Duration) -> Self {
        self.config.session_timeout = timeout;
        self
    }

    /// Sets the partitions to consume from.
    ///
    /// Version 0.4.0 requires at least one explicit partition because topic
    /// metadata discovery is not implemented.
    pub fn partitions(mut self, partitions: Vec<i32>) -> Self {
        self.partitions = partitions;
        self
    }

    /// Builds the consumer.
    pub async fn build(self) -> Result<Consumer<K, V>> {
        if self.config.group_id.is_some() {
            return Err(Error::unsupported("consumer group coordination"));
        }
        if self.config.enable_auto_commit {
            return Err(Error::unsupported("consumer automatic offset commits"));
        }
        match self.config.auto_offset_reset.as_str() {
            "earliest" => {}
            "latest" => {
                return Err(Error::unsupported("consumer latest-offset resolution"));
            }
            policy => {
                return Err(Error::new(
                    crate::error::ErrorKind::InvalidConfiguration,
                    format!(
                        "Unsupported auto_offset_reset policy '{policy}'; expected 'earliest' or 'latest'"
                    ),
                ));
            }
        }
        if self.partitions.iter().any(|partition| *partition < 0) {
            return Err(Error::new(
                crate::error::ErrorKind::InvalidConfiguration,
                "Consumer partitions must be non-negative",
            ));
        }

        Ok(Consumer::new(
            self.client_config,
            self.pool,
            self.topic,
            self.config,
            self.partitions,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "sasl")]
    use crate::config::SaslMechanism;
    use crate::error::ErrorKind;

    #[tokio::test]
    async fn test_consumer_group_configuration_fails_closed() {
        let client = Streamline::builder()
            .bootstrap_servers("localhost:9092")
            .build()
            .await
            .unwrap();

        let error = client
            .consumer::<Vec<u8>, Vec<u8>>("events")
            .group_id("group")
            .partitions(vec![0])
            .build()
            .await
            .err()
            .expect("consumer groups must be rejected");

        assert_eq!(error.kind, ErrorKind::Unsupported);
    }

    #[tokio::test]
    async fn test_latest_offset_configuration_fails_closed() {
        let client = Streamline::builder()
            .bootstrap_servers("localhost:9092")
            .build()
            .await
            .unwrap();

        let error = client
            .consumer::<Vec<u8>, Vec<u8>>("events")
            .auto_offset_reset("latest")
            .partitions(vec![0])
            .build()
            .await
            .err()
            .expect("latest offsets must be rejected");

        assert_eq!(error.kind, ErrorKind::Unsupported);
    }

    #[tokio::test]
    async fn test_auto_commit_configuration_fails_closed() {
        let client = Streamline::builder()
            .bootstrap_servers("localhost:9092")
            .build()
            .await
            .unwrap();

        let error = client
            .consumer::<Vec<u8>, Vec<u8>>("events")
            .enable_auto_commit(true)
            .partitions(vec![0])
            .build()
            .await
            .err()
            .expect("automatic commits must be rejected");

        assert_eq!(error.kind, ErrorKind::Unsupported);
    }

    #[tokio::test]
    async fn test_invalid_consumer_configuration_is_rejected() {
        let client = Streamline::builder()
            .bootstrap_servers("localhost:9092")
            .build()
            .await
            .unwrap();

        let policy_error = client
            .consumer::<Vec<u8>, Vec<u8>>("events")
            .auto_offset_reset("middle")
            .partitions(vec![0])
            .build()
            .await
            .err()
            .expect("invalid reset policy must be rejected");
        assert_eq!(policy_error.kind, ErrorKind::InvalidConfiguration);

        let partition_error = client
            .consumer::<Vec<u8>, Vec<u8>>("events")
            .partitions(vec![-1])
            .build()
            .await
            .err()
            .expect("negative partitions must be rejected");
        assert_eq!(partition_error.kind, ErrorKind::InvalidConfiguration);
    }

    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn test_tls_configuration_fails_closed() {
        let error = Streamline::builder()
            .bootstrap_servers("localhost:9093")
            .tls_config(TlsConfig::default())
            .build()
            .await
            .err()
            .expect("TLS must be rejected");

        assert_eq!(error.kind, ErrorKind::Unsupported);
    }

    #[cfg(feature = "sasl")]
    #[tokio::test]
    async fn test_sasl_configuration_fails_closed() {
        let error = Streamline::builder()
            .bootstrap_servers("localhost:9092")
            .sasl_config(SaslConfig {
                mechanism: SaslMechanism::Plain,
                username: "user".to_string(),
                password: "password".to_string(),
            })
            .build()
            .await
            .err()
            .expect("SASL must be rejected");

        assert_eq!(error.kind, ErrorKind::Unsupported);
    }
}
