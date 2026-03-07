//! Connection pool for managing reusable broker connections.

use crate::config::StreamlineConfig;
use crate::error::{Error, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// A single connection to a Streamline/Kafka broker.
pub(crate) struct KafkaConnection {
    stream: Option<TcpStream>,
    server: String,
    connect_timeout: Duration,
}

impl KafkaConnection {
    fn new(server: String, connect_timeout: Duration) -> Self {
        Self {
            stream: None,
            server,
            connect_timeout,
        }
    }

    /// Establishes the TCP connection to the broker.
    async fn connect(&mut self) -> Result<()> {
        let stream = tokio::time::timeout(self.connect_timeout, TcpStream::connect(&self.server))
            .await
            .map_err(|_| Error::timeout("connect"))?
            .map_err(|e| Error::connection_failed(&self.server).with_source(e))?;

        stream.set_nodelay(true).ok();
        self.stream = Some(stream);
        debug!("Connected to {}", self.server);
        Ok(())
    }

    /// Returns a mutable reference to the underlying stream, connecting lazily
    /// or reconnecting if the previous connection was lost.
    #[allow(dead_code)]
    pub(crate) async fn ensure_connected(&mut self) -> Result<&mut TcpStream> {
        if self.stream.is_none() {
            self.connect().await?;
        }
        // Safe: we just ensured the stream is Some above.
        Ok(self.stream.as_mut().unwrap())
    }

    /// Returns whether this connection currently holds an open stream.
    pub(crate) fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    /// Drops the underlying stream so the next call to [`ensure_connected`]
    /// will re-establish a fresh TCP connection.
    #[allow(dead_code)]
    pub(crate) fn disconnect(&mut self) {
        if self.stream.take().is_some() {
            debug!("Disconnected from {}", self.server);
        }
    }
}

/// Round-robin pool of broker connections.
///
/// Connections are created lazily on first use and are reused across
/// produce/consume calls. If a connection is lost, it will be
/// re-established transparently.
pub struct ConnectionPool {
    connections: Vec<Arc<Mutex<KafkaConnection>>>,
    #[allow(dead_code)]
    next: AtomicUsize,
}

impl ConnectionPool {
    /// Creates a new pool with `connection_pool_size` slots (lazy, not yet connected).
    pub(crate) fn new(config: &StreamlineConfig) -> Self {
        let pool_size = config.connection_pool_size.max(1);
        let connections = (0..pool_size)
            .map(|_| {
                Arc::new(Mutex::new(KafkaConnection::new(
                    config.bootstrap_servers.clone(),
                    config.connect_timeout,
                )))
            })
            .collect();

        debug!("Connection pool created with {} slots", pool_size);

        Self {
            connections,
            next: AtomicUsize::new(0),
        }
    }

    /// Returns a handle to the next connection in round-robin order.
    ///
    /// The connection is lazily established on first access. If a previous
    /// connection was marked disconnected (e.g. after an I/O error), it will
    /// be re-established automatically.
    #[allow(dead_code)]
    pub(crate) async fn get(&self) -> Result<ConnectionHandle> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        let conn = self.connections[idx].clone();

        // Eagerly ensure the connection is alive so callers get a clear
        // error rather than discovering it mid-request.
        {
            let mut guard = conn.lock().await;
            if !guard.is_connected() {
                guard.connect().await.map_err(|e| {
                    warn!("Pool: failed to connect slot {}: {}", idx, e);
                    e
                })?;
            }
        }

        Ok(ConnectionHandle { inner: conn })
    }

    /// Returns the number of slots in the pool.
    pub fn size(&self) -> usize {
        self.connections.len()
    }

    /// Returns `true` if at least one slot currently holds a live connection.
    pub async fn is_healthy(&self) -> bool {
        for conn in &self.connections {
            let guard = conn.lock().await;
            if guard.is_connected() {
                return true;
            }
        }
        false
    }

    // -- Admin operations (delegated from Admin client) --

    pub(crate) async fn create_topic(
        &self,
        name: &str,
        num_partitions: i32,
        replication_factor: i16,
        config: &std::collections::HashMap<String, String>,
    ) -> Result<()> {
        let _conn = self.get().await?;
        // TODO: send CreateTopics request via Kafka protocol
        debug!("create_topic: {} (partitions={}, rf={}, config_entries={})", name, num_partitions, replication_factor, config.len());
        Ok(())
    }

    pub(crate) async fn delete_topic(&self, name: &str) -> Result<()> {
        let _conn = self.get().await?;
        debug!("delete_topic: {}", name);
        Ok(())
    }

    pub(crate) async fn list_topics(&self) -> Result<Vec<crate::admin::TopicInfo>> {
        let _conn = self.get().await?;
        debug!("list_topics");
        Ok(vec![])
    }

    pub(crate) async fn describe_topic(&self, name: &str) -> Result<(crate::admin::TopicInfo, Vec<crate::admin::PartitionInfo>)> {
        let _conn = self.get().await?;
        debug!("describe_topic: {}", name);
        Err(Error::topic_not_found(name))
    }

    pub(crate) async fn add_partitions(&self, name: &str, total_count: i32) -> Result<()> {
        let _conn = self.get().await?;
        debug!("add_partitions: {} -> {}", name, total_count);
        Ok(())
    }

    pub(crate) async fn list_consumer_groups(&self) -> Result<Vec<String>> {
        let _conn = self.get().await?;
        debug!("list_consumer_groups");
        Ok(vec![])
    }

    pub(crate) async fn describe_consumer_group(&self, group_id: &str) -> Result<crate::admin::ConsumerGroupInfo> {
        let _conn = self.get().await?;
        debug!("describe_consumer_group: {}", group_id);
        Err(Error::new(crate::error::ErrorKind::Internal, format!("Consumer group not found: {}", group_id)))
    }

    pub(crate) async fn delete_consumer_group(&self, group_id: &str) -> Result<()> {
        let _conn = self.get().await?;
        debug!("delete_consumer_group: {}", group_id);
        Ok(())
    }

    pub(crate) async fn list_brokers(&self) -> Result<Vec<crate::admin::BrokerInfo>> {
        let _conn = self.get().await?;
        debug!("list_brokers");
        Ok(vec![])
    }
}

/// A handle to a pooled connection.
///
/// Lock the inner mutex to obtain mutable access to the [`KafkaConnection`].
/// The connection is returned to the pool automatically when the handle is
/// dropped (no extra bookkeeping required since we use round-robin indexing).
pub(crate) struct ConnectionHandle {
    #[allow(dead_code)]
    inner: Arc<Mutex<KafkaConnection>>,
}

impl ConnectionHandle {
    /// Locks the connection for exclusive use.
    #[allow(dead_code)]
    pub(crate) async fn lock(&self) -> tokio::sync::MutexGuard<'_, KafkaConnection> {
        self.inner.lock().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StreamlineConfig;

    fn test_config(pool_size: usize) -> StreamlineConfig {
        StreamlineConfig {
            connection_pool_size: pool_size,
            ..Default::default()
        }
    }

    #[test]
    fn test_pool_creation_default_size() {
        let pool = ConnectionPool::new(&StreamlineConfig::default());
        assert_eq!(pool.size(), 4);
    }

    #[test]
    fn test_pool_creation_custom_size() {
        let pool = ConnectionPool::new(&test_config(8));
        assert_eq!(pool.size(), 8);
    }

    #[test]
    fn test_pool_creation_zero_clamps_to_one() {
        let pool = ConnectionPool::new(&test_config(0));
        assert_eq!(pool.size(), 1);
    }

    #[tokio::test]
    async fn test_pool_is_healthy_when_empty() {
        let pool = ConnectionPool::new(&test_config(2));
        // No connections established yet, so not healthy.
        assert!(!pool.is_healthy().await);
    }

    #[test]
    fn test_kafka_connection_initial_state() {
        let conn = KafkaConnection::new("localhost:9092".into(), Duration::from_secs(5));
        assert!(!conn.is_connected());
    }

    #[test]
    fn test_kafka_connection_disconnect_noop_when_not_connected() {
        let mut conn = KafkaConnection::new("localhost:9092".into(), Duration::from_secs(5));
        conn.disconnect(); // should not panic
        assert!(!conn.is_connected());
    }

    #[tokio::test]
    async fn test_pool_get_returns_error_when_no_server() {
        let config = StreamlineConfig {
            // Invalid address so connection will fail.
            bootstrap_servers: "192.0.2.1:1".to_string(),
            connection_pool_size: 1,
            connect_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let pool = ConnectionPool::new(&config);
        let result = pool.get().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_round_robin_index_wraps() {
        let pool = ConnectionPool::new(&test_config(3));
        // Simulate 7 gets and verify the internal counter wraps.
        for _ in 0..7 {
            let _ = pool.next.fetch_add(1, Ordering::Relaxed);
        }
        let idx = pool.next.load(Ordering::Relaxed) % pool.connections.len();
        assert_eq!(idx, 7 % 3);
    }
}

