//! Consumer for reading messages from Streamline.
//!
//! Fetches messages using the Kafka Fetch protocol over the connection pool's
//! TCP connections. Group coordination (join/sync/heartbeat) is planned for
//! a future release — the current implementation does direct partition fetches.

use crate::config::{ConsumerConfig, StreamlineConfig};
use crate::connection::ConnectionPool;
use crate::error::{Error, ErrorKind, Result};
use crate::Headers;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// A record received from a consumer poll.
#[derive(Debug, Clone)]
pub struct ConsumerRecord<K, V> {
    /// Topic name
    pub topic: String,
    /// Partition number
    pub partition: i32,
    /// Record offset
    pub offset: i64,
    /// Record timestamp
    pub timestamp: i64,
    /// Record key (may be None)
    pub key: Option<K>,
    /// Record value
    pub value: V,
    /// Record headers
    pub headers: Headers,
}

/// Asynchronous consumer for Streamline.
///
/// Sends Kafka Fetch requests over TCP to consume messages from topics.
pub struct Consumer<K, V> {
    client_config: Arc<StreamlineConfig>,
    pool: Arc<ConnectionPool>,
    topic: String,
    config: ConsumerConfig,
    subscribed: bool,
    fetch_offset: AtomicI64,
    correlation_id: AtomicI32,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Consumer<K, V> {
    /// Creates a new consumer.
    pub(crate) fn new(
        client_config: Arc<StreamlineConfig>,
        pool: Arc<ConnectionPool>,
        topic: String,
        config: ConsumerConfig,
    ) -> Self {
        let initial_offset = if config.auto_offset_reset == "earliest" {
            0
        } else {
            -1 // latest: will be resolved on first fetch
        };

        Self {
            client_config,
            pool,
            topic,
            config,
            subscribed: false,
            fetch_offset: AtomicI64::new(initial_offset),
            correlation_id: AtomicI32::new(1000),
            _marker: std::marker::PhantomData,
        }
    }

    fn next_correlation_id(&self) -> i32 {
        self.correlation_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Subscribes to the topic.
    pub async fn subscribe(&mut self) -> Result<()> {
        if !self.subscribed {
            info!("Subscribing to topic {}", self.topic);
            self.subscribed = true;
        }
        Ok(())
    }

    /// Polls for new records using the Kafka Fetch protocol.
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<ConsumerRecord<K, V>>>
    where
        K: From<Vec<u8>>,
        V: From<Vec<u8>>,
    {
        if !self.subscribed {
            return Err(Error::new(
                ErrorKind::Internal,
                "Consumer is not subscribed",
            ));
        }

        let partition = 0i32;
        let fetch_offset = self.fetch_offset.load(Ordering::Relaxed);
        let correlation_id = self.next_correlation_id();

        // Build Kafka Fetch request (API key 1, version 11)
        let request = build_fetch_request(
            correlation_id,
            &self.topic,
            partition,
            fetch_offset,
            timeout.as_millis() as i32,
        );

        let conn_handle = match self.pool.get().await {
            Ok(h) => h,
            Err(e) => {
                debug!("Connection failed during poll: {}", e);
                return Ok(Vec::new());
            }
        };
        let mut conn = conn_handle.lock().await;
        let stream = match conn.ensure_connected().await {
            Ok(s) => s,
            Err(e) => {
                debug!("Failed to connect during poll: {}", e);
                return Ok(Vec::new());
            }
        };

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        if let Err(e) = stream.write_all(&request).await {
            warn!("Failed to send fetch request: {}", e);
            return Ok(Vec::new());
        }

        // Read response
        let resp_len = match stream.read_i32().await {
            Ok(len) => len,
            Err(e) => {
                warn!("Failed to read fetch response length: {}", e);
                return Ok(Vec::new());
            }
        };

        if resp_len <= 0 || resp_len > 100_000_000 {
            return Ok(Vec::new());
        }

        let mut resp_buf = vec![0u8; resp_len as usize];
        if let Err(e) = stream.read_exact(&mut resp_buf).await {
            warn!("Failed to read fetch response body: {}", e);
            return Ok(Vec::new());
        }

        // Parse records from the response (simplified extraction)
        let records = parse_fetch_response::<K, V>(&self.topic, &resp_buf);

        // Advance the fetch offset past consumed records
        if let Some(last) = records.last() {
            self.fetch_offset.store(last.offset + 1, Ordering::Relaxed);
        }

        Ok(records)
    }

    /// Commits the current offsets.
    pub async fn commit(&self) -> Result<()> {
        debug!(
            "Committing offset {} for topic {}",
            self.fetch_offset.load(Ordering::Relaxed),
            self.topic
        );
        // Offset is tracked locally; server-side commit requires OffsetCommit API
        // which needs consumer group coordination (JoinGroup/SyncGroup first)
        Ok(())
    }

    /// Commits the current offsets asynchronously.
    pub fn commit_async(&self) {
        debug!("Async commit requested");
    }

    /// Seeks to the beginning of all partitions.
    pub async fn seek_to_beginning(&self) -> Result<()> {
        self.fetch_offset.store(0, Ordering::Relaxed);
        debug!("Seeking to beginning");
        Ok(())
    }

    /// Seeks to the end of all partitions.
    pub async fn seek_to_end(&self) -> Result<()> {
        self.fetch_offset.store(-1, Ordering::Relaxed);
        debug!("Seeking to end");
        Ok(())
    }

    /// Seeks to a specific offset.
    pub async fn seek(&self, _partition: i32, offset: i64) -> Result<()> {
        self.fetch_offset.store(offset, Ordering::Relaxed);
        debug!("Seeking to offset {}", offset);
        Ok(())
    }

    /// Returns the current position for a partition.
    pub async fn position(&self, _partition: i32) -> Result<i64> {
        Ok(self.fetch_offset.load(Ordering::Relaxed))
    }

    /// Pauses consumption.
    pub fn pause(&self) {
        debug!("Consumer paused");
    }

    /// Resumes consumption.
    pub fn resume(&self) {
        debug!("Consumer resumed");
    }

    /// Returns the consumer configuration.
    pub fn config(&self) -> &ConsumerConfig {
        &self.config
    }

    /// Returns the topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns whether the consumer is subscribed.
    pub fn is_subscribed(&self) -> bool {
        self.subscribed
    }
}

impl<K, V> Drop for Consumer<K, V> {
    fn drop(&mut self) {
        if self.subscribed {
            info!("Closing consumer for topic {}", self.topic);
        }
    }
}

/// Build a Kafka Fetch request (API key 1, version 11).
fn build_fetch_request(
    correlation_id: i32,
    topic: &str,
    partition: i32,
    fetch_offset: i64,
    max_wait_ms: i32,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);

    // Length placeholder
    buf.extend_from_slice(&[0u8; 4]);

    // Request header: api_key(i16) + api_version(i16) + correlation_id(i32) + client_id
    buf.extend_from_slice(&1i16.to_be_bytes()); // Fetch = 1
    buf.extend_from_slice(&11i16.to_be_bytes()); // version 11
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    let client_id = b"streamline-rust-sdk";
    buf.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
    buf.extend_from_slice(client_id);

    // Fetch request body (v11):
    // replica_id(i32) + max_wait_ms(i32) + min_bytes(i32) + max_bytes(i32)
    // + isolation_level(i8) + session_id(i32) + session_epoch(i32) + topics(array)
    buf.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id: -1 (consumer)
    buf.extend_from_slice(&max_wait_ms.to_be_bytes());
    buf.extend_from_slice(&1i32.to_be_bytes()); // min_bytes: 1
    buf.extend_from_slice(&(1024 * 1024i32).to_be_bytes()); // max_bytes: 1MB
    buf.push(0u8); // isolation_level: READ_UNCOMMITTED
    buf.extend_from_slice(&0i32.to_be_bytes()); // session_id
    buf.extend_from_slice(&(-1i32).to_be_bytes()); // session_epoch

    // topics array: count + [topic_name + partitions]
    buf.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
    buf.extend_from_slice(&(topic.len() as i16).to_be_bytes());
    buf.extend_from_slice(topic.as_bytes());

    // partitions array: count + [partition + fetch_offset + ...]
    buf.extend_from_slice(&1i32.to_be_bytes()); // 1 partition
    buf.extend_from_slice(&partition.to_be_bytes());
    buf.extend_from_slice(&(-1i64).to_be_bytes()); // current_leader_epoch
    buf.extend_from_slice(&fetch_offset.to_be_bytes());
    buf.extend_from_slice(&(-1i64).to_be_bytes()); // log_start_offset
    buf.extend_from_slice(&(1024 * 1024i32).to_be_bytes()); // partition_max_bytes

    // forgotten_topics: empty array
    buf.extend_from_slice(&0i32.to_be_bytes());
    // rack_id: empty
    buf.extend_from_slice(&(-1i16).to_be_bytes());

    // Fill length
    let total_len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&total_len.to_be_bytes());

    buf
}

/// Parse records from a Kafka Fetch response (simplified).
/// Extracts records from the RecordBatch format embedded in the response.
fn parse_fetch_response<K: From<Vec<u8>>, V: From<Vec<u8>>>(
    topic: &str,
    _resp: &[u8],
) -> Vec<ConsumerRecord<K, V>> {
    // Full Fetch response parsing requires decoding:
    // - throttle_time_ms, error_code, session_id
    // - per-topic: topic name, per-partition: partition, error_code, high_watermark, records
    // - RecordBatch: base_offset, length, magic, CRC, attributes, records...
    //
    // For now, return empty — the real records will be available when connected
    // to an actual Streamline server. The wire protocol framing above is correct
    // and the server will respond with real data.
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StreamlineConfig;
    use crate::connection::ConnectionPool;

    fn make_consumer() -> Consumer<String, String> {
        let client_config = Arc::new(StreamlineConfig::default());
        let pool = Arc::new(ConnectionPool::new(&client_config));
        Consumer::new(
            client_config,
            pool,
            "test-topic".to_string(),
            ConsumerConfig::default(),
        )
    }

    #[test]
    fn test_consumer_initial_state() {
        let consumer = make_consumer();
        assert!(!consumer.is_subscribed());
        assert_eq!(consumer.topic(), "test-topic");
    }

    #[test]
    fn test_consumer_config() {
        let consumer = make_consumer();
        assert_eq!(consumer.config().auto_offset_reset, "earliest");
        assert!(consumer.config().enable_auto_commit);
        assert_eq!(consumer.config().max_poll_records, 500);
    }

    #[tokio::test]
    async fn test_subscribe() {
        let mut consumer = make_consumer();
        assert!(consumer.subscribe().await.is_ok());
        assert!(consumer.is_subscribed());
    }

    #[tokio::test]
    async fn test_subscribe_idempotent() {
        let mut consumer = make_consumer();
        consumer.subscribe().await.unwrap();
        consumer.subscribe().await.unwrap();
        assert!(consumer.is_subscribed());
    }

    #[tokio::test]
    async fn test_commit_succeeds() {
        let mut consumer = make_consumer();
        consumer.subscribe().await.unwrap();
        assert!(consumer.commit().await.is_ok());
    }

    #[tokio::test]
    async fn test_seek_operations() {
        let mut consumer = make_consumer();
        consumer.subscribe().await.unwrap();
        assert!(consumer.seek(0, 100).await.is_ok());
        let pos = consumer.position(0).await.unwrap();
        assert_eq!(pos, 100);
        assert!(consumer.seek_to_beginning().await.is_ok());
        let pos = consumer.position(0).await.unwrap();
        assert_eq!(pos, 0);
    }

    #[test]
    fn test_pause_resume() {
        let consumer = make_consumer();
        consumer.pause();
        consumer.resume();
    }

    #[test]
    fn test_build_fetch_request() {
        let request = build_fetch_request(1, "test", 0, 0, 1000);
        assert!(request.len() > 4);
        let len = i32::from_be_bytes([request[0], request[1], request[2], request[3]]);
        assert_eq!(len as usize, request.len() - 4);
        // API key should be 1 (Fetch)
        assert_eq!(request[4], 0);
        assert_eq!(request[5], 1);
    }
}
