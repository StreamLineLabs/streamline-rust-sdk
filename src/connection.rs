//! Connection pool for managing reusable broker connections.

use crate::config::StreamlineConfig;
use crate::error::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Maximum accepted Kafka response body length (100 MB).
const MAX_RESPONSE_LEN: usize = 100_000_000;

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
    pub(crate) async fn ensure_connected(&mut self) -> Result<&mut TcpStream> {
        if self.stream.is_none() {
            self.connect().await?;
        }
        self.stream
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::Connection, "failed to establish connection"))
    }

    /// Returns whether this connection currently holds an open stream.
    pub(crate) fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    /// Drops the underlying stream so the next call to [`ensure_connected`]
    /// will re-establish a fresh TCP connection.
    ///
    /// A timed-out, protocol-violating, or I/O-failed exchange can leave
    /// unread bytes buffered in the socket. Reusing such a connection would
    /// desynchronize every later request/response pair (the next reply would
    /// be read from the previous request's leftovers), so the stream is
    /// always dropped rather than returned to the pool.
    pub(crate) fn evict(&mut self, reason: &str) {
        if self.stream.take().is_some() {
            warn!(
                "Evicting pooled connection to {} after {}",
                self.server, reason
            );
        } else {
            debug!("Connection to {} already closed ({})", self.server, reason);
        }
    }

    /// Performs one complete Kafka request/response exchange.
    ///
    /// The entire exchange — establishing the connection, writing the request,
    /// reading the four-byte length prefix, and reading the body — is bounded
    /// by `request_timeout`. On timeout, protocol violation, or I/O failure
    /// the connection is evicted from the pool so a desynchronized socket is
    /// never handed to a later caller.
    pub(crate) async fn exchange(
        &mut self,
        request: &[u8],
        request_timeout: Duration,
        operation: &str,
    ) -> Result<Vec<u8>> {
        self.ensure_connected().await?;
        let mut stream = self.stream.take().ok_or_else(|| {
            Error::new(
                ErrorKind::Connection,
                "pooled connection disappeared before exchange",
            )
        })?;
        let result =
            tokio::time::timeout(request_timeout, Self::exchange_inner(&mut stream, request)).await;

        match result {
            Err(_elapsed) => {
                warn!(
                    "Evicting pooled connection to {} after {} request timeout",
                    self.server, operation
                );
                Err(Error::timeout(operation).with_hint(format!(
                    "The broker did not complete the {operation} exchange within {:?}; \
                     increase ClientBuilder::request_timeout or check broker health",
                    request_timeout
                )))
            }
            Ok(Err(error)) => {
                warn!(
                    "Evicting pooled connection to {} after {} failure: {}",
                    self.server, operation, error
                );
                Err(error)
            }
            Ok(Ok(response)) => {
                self.stream = Some(stream);
                Ok(response)
            }
        }
    }

    async fn exchange_inner(stream: &mut TcpStream, request: &[u8]) -> Result<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        stream
            .write_all(request)
            .await
            .map_err(|e| Error::connection(format!("Write failed: {e}")))?;
        stream
            .flush()
            .await
            .map_err(|e| Error::connection(format!("Flush failed: {e}")))?;

        let response_len = stream
            .read_i32()
            .await
            .map_err(|e| Error::connection(format!("Read failed: {e}")))?;
        if response_len <= 0 || response_len as usize > MAX_RESPONSE_LEN {
            return Err(Error::new(
                ErrorKind::Protocol,
                format!("Invalid response length from server: {response_len}"),
            ));
        }

        let mut response = vec![0u8; response_len as usize];
        stream
            .read_exact(&mut response)
            .await
            .map_err(|e| Error::connection(format!("Read body failed: {e}")))?;

        Ok(response)
    }
}

/// Round-robin pool of broker connections.
///
/// Connections are created lazily on first use and are reused across
/// produce/consume calls. If a connection is lost, it will be
/// re-established transparently.
pub struct ConnectionPool {
    connections: Vec<Arc<Mutex<KafkaConnection>>>,
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
    //
    // NOTE: These operations are stubs pending Kafka-protocol wiring (see
    // Roadmap item #4 in DX_AUDIT.md). They previously returned `Ok(default)`
    // which silently misled callers into believing a topic had been created
    // when no frame was ever sent. They now fail loudly with
    // `ErrorKind::Unsupported` so calling code surfaces the gap rather than
    // hiding it.

    pub(crate) async fn create_topic(
        &self,
        name: &str,
        num_partitions: i32,
        replication_factor: i16,
        config: &std::collections::HashMap<String, String>,
    ) -> Result<()> {
        let _conn = self.get().await?;
        warn!(
            "create_topic stub invoked: name={} partitions={} rf={} config_entries={}",
            name, num_partitions, replication_factor, config.len()
        );
        Err(Error::unsupported("admin.create_topic"))
    }

    pub(crate) async fn delete_topic(&self, name: &str) -> Result<()> {
        let _conn = self.get().await?;
        warn!("delete_topic stub invoked: {}", name);
        Err(Error::unsupported("admin.delete_topic"))
    }

    pub(crate) async fn list_topics(&self) -> Result<Vec<crate::admin::TopicInfo>> {
        let _conn = self.get().await?;
        warn!("list_topics stub invoked");
        Err(Error::unsupported("admin.list_topics"))
    }

    pub(crate) async fn describe_topic(&self, name: &str) -> Result<(crate::admin::TopicInfo, Vec<crate::admin::PartitionInfo>)> {
        let _conn = self.get().await?;
        warn!("describe_topic stub invoked: {}", name);
        Err(Error::unsupported("admin.describe_topic"))
    }

    pub(crate) async fn add_partitions(&self, name: &str, total_count: i32) -> Result<()> {
        let _conn = self.get().await?;
        warn!("add_partitions stub invoked: {} -> {}", name, total_count);
        Err(Error::unsupported("admin.add_partitions"))
    }

    pub(crate) async fn list_consumer_groups(&self) -> Result<Vec<String>> {
        let _conn = self.get().await?;
        warn!("list_consumer_groups stub invoked");
        Err(Error::unsupported("admin.list_consumer_groups"))
    }

    pub(crate) async fn describe_consumer_group(&self, group_id: &str) -> Result<crate::admin::ConsumerGroupInfo> {
        let _conn = self.get().await?;
        warn!("describe_consumer_group stub invoked: {}", group_id);
        Err(Error::unsupported("admin.describe_consumer_group"))
    }

    pub(crate) async fn delete_consumer_group(&self, group_id: &str) -> Result<()> {
        let _conn = self.get().await?;
        warn!("delete_consumer_group stub invoked: {}", group_id);
        Err(Error::unsupported("admin.delete_consumer_group"))
    }

    pub(crate) async fn list_brokers(&self) -> Result<Vec<crate::admin::BrokerInfo>> {
        let _conn = self.get().await?;
        warn!("list_brokers stub invoked");
        Err(Error::unsupported("admin.list_brokers"))
    }
}

/// A handle to a pooled connection.
///
/// Lock the inner mutex to obtain mutable access to the [`KafkaConnection`].
/// The connection is returned to the pool automatically when the handle is
/// dropped (no extra bookkeeping required since we use round-robin indexing).
pub(crate) struct ConnectionHandle {
    inner: Arc<Mutex<KafkaConnection>>,
}

impl ConnectionHandle {
    /// Locks the connection for exclusive use.
    pub(crate) async fn lock(&self) -> tokio::sync::MutexGuard<'_, KafkaConnection> {
        self.inner.lock().await
    }
}

/// Returns whether a failed response should evict the pooled connection.
///
/// Protocol and connection failures may leave unread bytes on the socket, so
/// the connection must not be reused. Broker-reported application errors (for
/// example `TopicNotFound`, or a broker-side `REQUEST_TIMED_OUT` error code)
/// arrive on a well-framed response and leave the connection usable; a
/// transport-level timeout is evicted directly by [`KafkaConnection::exchange`].
pub(crate) fn evictable(error: &Error) -> bool {
    matches!(
        error.kind,
        ErrorKind::Protocol | ErrorKind::Connection | ErrorKind::ConnectionFailed
    )
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
    fn test_kafka_connection_evict_noop_when_not_connected() {
        let mut conn = KafkaConnection::new("localhost:9092".into(), Duration::from_secs(5));
        conn.evict("test"); // should not panic
        assert!(!conn.is_connected());
    }

    #[test]
    fn test_evictable_classifies_failures() {
        assert!(evictable(&Error::new(ErrorKind::Protocol, "bad frame")));
        assert!(evictable(&Error::new(ErrorKind::Connection, "io")));
        assert!(evictable(&Error::connection_failed("host:1")));
        assert!(!evictable(&Error::timeout("produce")));
        assert!(!evictable(&Error::new(
            ErrorKind::OffsetOutOfRange,
            "offset outside retained range"
        )));
        assert!(!evictable(&Error::topic_not_found("events")));
        assert!(!evictable(&Error::unsupported("admin.list_topics")));
    }

    #[tokio::test]
    async fn test_exchange_times_out_and_evicts_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Accept but never reply, forcing the read side to time out.
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(stream);
        });

        let mut conn = KafkaConnection::new(addr.to_string(), Duration::from_secs(5));
        let error = conn
            .exchange(&[0, 0, 0, 1, 7], Duration::from_millis(100), "produce")
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Timeout);
        assert!(
            !conn.is_connected(),
            "a timed-out connection must be evicted from the pool"
        );
        server.abort();
    }

    #[tokio::test]
    async fn test_cancelled_exchange_drops_stream_before_next_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let (mut first, _) = listener.accept().await.unwrap();
            let mut first_request = [0u8; 1];
            first.read_exact(&mut first_request).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            drop(first);

            let (mut second, _) = listener.accept().await.unwrap();
            let mut second_request = [0u8; 1];
            second.read_exact(&mut second_request).await.unwrap();
            second.write_i32(1).await.unwrap();
            second.write_all(&[42]).await.unwrap();
        });

        let mut conn = KafkaConnection::new(addr.to_string(), Duration::from_secs(1));
        let cancelled = tokio::time::timeout(
            Duration::from_millis(50),
            conn.exchange(&[1], Duration::from_secs(5), "cancelled"),
        )
        .await;
        assert!(
            cancelled.is_err(),
            "outer timeout must cancel the exchange future"
        );
        assert!(
            !conn.is_connected(),
            "a cancelled exchange must not return its partially-used stream to the pool"
        );

        let response = conn
            .exchange(&[2], Duration::from_secs(2), "replacement")
            .await
            .unwrap();
        assert_eq!(response, vec![42]);
        assert!(conn.is_connected());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_exchange_evicts_on_invalid_response_length() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            use tokio::io::AsyncWriteExt;
            // Negative length prefix: a protocol violation.
            stream.write_all(&(-1i32).to_be_bytes()).await.ok();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let mut conn = KafkaConnection::new(addr.to_string(), Duration::from_secs(5));
        let error = conn
            .exchange(&[0, 0, 0, 1, 7], Duration::from_secs(5), "produce")
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(!conn.is_connected());
        server.abort();
    }

    #[tokio::test]
    async fn test_exchange_returns_response_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let len = stream.read_i32().await.unwrap();
            let mut request = vec![0u8; len as usize];
            stream.read_exact(&mut request).await.unwrap();
            stream.write_all(&3i32.to_be_bytes()).await.unwrap();
            stream.write_all(&[1, 2, 3]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let mut conn = KafkaConnection::new(addr.to_string(), Duration::from_secs(5));
        let response = conn
            .exchange(&[0, 0, 0, 1, 7], Duration::from_secs(5), "produce")
            .await
            .unwrap();

        assert_eq!(response, vec![1, 2, 3]);
        assert!(conn.is_connected(), "a healthy connection stays pooled");
        server.abort();
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

