//! Main Streamline client.

use crate::admin::Admin;
use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::config::{ConsumerConfig, ProducerConfig, SecurityProtocol, StreamlineConfig};
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
        self.produce_with_headers(topic, key, value, Headers::new()).await
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
    pub fn producer_with_config<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send>(&self, config: ProducerConfig) -> Producer<K, V> {
        match &self.circuit_breaker {
            Some(cb) => Producer::with_circuit_breaker(self.config.clone(), self.pool.clone(), config, cb.clone()),
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
    connection_pool_size: Option<usize>,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
    circuit_breaker: Option<CircuitBreakerConfig>,
}

impl StreamlineBuilder {
    /// Sets the bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: &str) -> Self {
        self.bootstrap_servers = Some(servers.to_string());
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
        let bootstrap_servers = self
            .bootstrap_servers
            .ok_or_else(|| Error::new(crate::error::ErrorKind::InvalidConfiguration, "bootstrap_servers is required"))?;

        let config = StreamlineConfig {
            bootstrap_servers: bootstrap_servers.clone(),
            connection_pool_size: self.connection_pool_size.unwrap_or(4),
            connect_timeout: self.connect_timeout.unwrap_or(Duration::from_secs(30)),
            request_timeout: self.request_timeout.unwrap_or(Duration::from_secs(30)),
            security_protocol: SecurityProtocol::default(),
            tls: None,
            sasl: None,
        };

        let config = Arc::new(config);
        let pool = Arc::new(ConnectionPool::new(&config));
        let circuit_breaker = self.circuit_breaker.map(|cb_config| Arc::new(CircuitBreaker::new(cb_config)));

        info!(
            "Streamline client created for {} (pool_size={}, circuit_breaker={})",
            bootstrap_servers,
            pool.size(),
            circuit_breaker.is_some(),
        );

        Ok(Streamline { config, pool, circuit_breaker })
    }
}

/// Builder for consumers.
pub struct ConsumerBuilder<K, V> {
    client_config: Arc<StreamlineConfig>,
    pool: Arc<ConnectionPool>,
    topic: String,
    config: ConsumerConfig,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> ConsumerBuilder<K, V> {
    fn new(client_config: Arc<StreamlineConfig>, pool: Arc<ConnectionPool>, topic: String) -> Self {
        Self {
            client_config,
            pool,
            topic,
            config: ConsumerConfig::default(),
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

    /// Builds the consumer.
    pub async fn build(self) -> Result<Consumer<K, V>> {
        Ok(Consumer::new(
            self.client_config,
            self.pool,
            self.topic,
            self.config,
        ))
    }
}
