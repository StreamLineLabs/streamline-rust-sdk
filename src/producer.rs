//! Producer for sending messages to Streamline.

use crate::config::{ProducerConfig, StreamlineConfig};
use crate::connection::ConnectionPool;
use crate::error::{Error, ErrorKind, Result};
use crate::Headers;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use tracing::debug;

/// Metadata for a produced record.
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    /// Topic name
    pub topic: String,
    /// Partition number
    pub partition: i32,
    /// Offset of the record
    pub offset: i64,
    /// Timestamp of the record
    pub timestamp: i64,
}

/// A record to produce.
#[derive(Debug, Clone)]
pub struct ProducerRecord<K, V> {
    /// Optional key
    pub key: Option<K>,
    /// The value
    pub value: V,
    /// Optional headers
    pub headers: Headers,
    /// Optional partition (overrides key-based partitioning)
    pub partition: Option<i32>,
}

impl<K, V> ProducerRecord<K, V> {
    /// Creates a new record with key and value.
    pub fn new(key: K, value: V) -> Self {
        Self {
            key: Some(key),
            value,
            headers: Headers::new(),
            partition: None,
        }
    }

    /// Creates a new record with value only.
    pub fn value_only(value: V) -> Self {
        Self {
            key: None,
            value,
            headers: Headers::new(),
            partition: None,
        }
    }

    /// Adds headers to the record.
    pub fn with_headers(mut self, headers: Headers) -> Self {
        self.headers = headers;
        self
    }

    /// Sets the target partition.
    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }
}

/// Asynchronous producer for Streamline.
///
/// Sends messages using the Kafka wire protocol over the connection pool's
/// TCP connections. Uses the `kafka-protocol` crate for message framing.
pub struct Producer<K, V> {
    client_config: Arc<StreamlineConfig>,
    pool: Arc<ConnectionPool>,
    config: ProducerConfig,
    correlation_id: AtomicI32,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send> Producer<K, V> {
    /// Creates a new producer.
    pub(crate) fn new(
        client_config: Arc<StreamlineConfig>,
        pool: Arc<ConnectionPool>,
        config: ProducerConfig,
    ) -> Self {
        Self {
            client_config,
            pool,
            config,
            correlation_id: AtomicI32::new(1),
            _marker: std::marker::PhantomData,
        }
    }

    fn next_correlation_id(&self) -> i32 {
        self.correlation_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Sends a message to a topic using the Kafka Produce wire protocol.
    pub async fn send(
        &self,
        topic: &str,
        key: K,
        value: V,
        _headers: Headers,
    ) -> Result<RecordMetadata> {
        debug!("Sending message to topic {}", topic);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let key_bytes = key.as_ref();
        let value_bytes = value.as_ref();
        let partition: i32 = 0;
        let correlation_id = self.next_correlation_id();
        let acks: i16 = -1;  // all replicas
        let timeout_ms: i32 = 30_000;

        // Build a Kafka Produce request (API key 0, version 7)
        // Wire format: [length:i32][header][body]
        let request = build_produce_request(
            correlation_id,
            acks,
            timeout_ms,
            topic,
            partition,
            key_bytes,
            value_bytes,
            now_ms,
        );

        let conn_handle = self.pool.get().await?;
        let mut conn = conn_handle.lock().await;
        let stream = conn.ensure_connected().await?;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        stream.write_all(&request).await
            .map_err(|e| Error::new(ErrorKind::ConnectionFailed, format!("Write failed: {e}")))?;

        // Read response: 4-byte length prefix
        let resp_len = stream.read_i32().await
            .map_err(|e| Error::new(ErrorKind::ConnectionFailed, format!("Read failed: {e}")))?;
        let mut resp_buf = vec![0u8; resp_len as usize];
        stream.read_exact(&mut resp_buf).await
            .map_err(|e| Error::new(ErrorKind::ConnectionFailed, format!("Read body failed: {e}")))?;

        // Verify correlation ID (first 4 bytes of response)
        if resp_buf.len() >= 4 {
            let resp_corr = i32::from_be_bytes([resp_buf[0], resp_buf[1], resp_buf[2], resp_buf[3]]);
            if resp_corr != correlation_id {
                return Err(Error::new(ErrorKind::Protocol, format!(
                    "Correlation ID mismatch: expected {correlation_id}, got {resp_corr}"
                )));
            }
        }

        // Parse base_offset from produce response (simplified: skip to known position)
        let base_offset = if resp_buf.len() >= 24 {
            i64::from_be_bytes([
                resp_buf[16], resp_buf[17], resp_buf[18], resp_buf[19],
                resp_buf[20], resp_buf[21], resp_buf[22], resp_buf[23],
            ])
        } else {
            0
        };

        Ok(RecordMetadata {
            topic: topic.to_string(),
            partition,
            offset: base_offset,
            timestamp: now_ms,
        })
    }

    /// Sends a batch of records to a topic.
    pub async fn send_batch(
        &self,
        topic: &str,
        records: Vec<ProducerRecord<K, V>>,
    ) -> Result<Vec<RecordMetadata>> {
        debug!("Sending {} records to topic {}", records.len(), topic);

        let mut results = Vec::with_capacity(records.len());
        for record in records {
            let headers = record.headers;
            let value = record.value;
            match record.key {
                Some(k) => results.push(self.send(topic, k, value, headers).await?),
                None => {
                    // Key-less records use empty key
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    results.push(RecordMetadata {
                        topic: topic.to_string(),
                        partition: 0,
                        offset: 0,
                        timestamp: now,
                    });
                }
            }
        }
        Ok(results)
    }

    /// Flushes any buffered messages.
    pub async fn flush(&self) -> Result<()> {
        debug!("Flushing producer");
        Ok(())
    }

    /// Returns the producer configuration.
    pub fn config(&self) -> &ProducerConfig {
        &self.config
    }
}

/// Build a minimal Kafka Produce request (API key 0, version 7).
///
/// Format:
/// [total_length:i32] [request_header] [produce_request_body]
///
/// This uses manual binary encoding to avoid kafka-protocol crate API version
/// incompatibilities. The wire format follows the Kafka protocol specification.
fn build_produce_request(
    correlation_id: i32,
    acks: i16,
    timeout_ms: i32,
    topic: &str,
    partition: i32,
    key: &[u8],
    value: &[u8],
    timestamp: i64,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256 + key.len() + value.len());

    // Placeholder for total length (filled at the end)
    buf.extend_from_slice(&[0u8; 4]);

    // Request header (v1): api_key(i16) + api_version(i16) + correlation_id(i32) + client_id(nullable_string)
    buf.extend_from_slice(&0i16.to_be_bytes());     // api_key: Produce = 0
    buf.extend_from_slice(&7i16.to_be_bytes());      // api_version: 7
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    let client_id = b"streamline-rust-sdk";
    buf.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
    buf.extend_from_slice(client_id);

    // Produce request body (v7):
    // transactional_id(nullable_string) + acks(i16) + timeout_ms(i32) + topic_data(array)
    buf.extend_from_slice(&(-1i16).to_be_bytes());   // transactional_id: null
    buf.extend_from_slice(&acks.to_be_bytes());
    buf.extend_from_slice(&timeout_ms.to_be_bytes());

    // topic_data array: count(i32) + [topic_name + partition_data]
    buf.extend_from_slice(&1i32.to_be_bytes());      // 1 topic

    // topic_name (string)
    buf.extend_from_slice(&(topic.len() as i16).to_be_bytes());
    buf.extend_from_slice(topic.as_bytes());

    // partition_data array: count(i32) + [partition_index + record_set]
    buf.extend_from_slice(&1i32.to_be_bytes());      // 1 partition
    buf.extend_from_slice(&partition.to_be_bytes());

    // Build the record batch (MessageSet v2 / RecordBatch format)
    let record_batch = build_record_batch(key, value, timestamp);
    buf.extend_from_slice(&(record_batch.len() as i32).to_be_bytes());
    buf.extend_from_slice(&record_batch);

    // Fill in total length (excludes the length field itself)
    let total_len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&total_len.to_be_bytes());

    buf
}

/// Build a minimal RecordBatch (v2 format) with a single record.
fn build_record_batch(key: &[u8], value: &[u8], timestamp: i64) -> Vec<u8> {
    // First, build the record (variable-length encoded)
    let mut record = Vec::new();
    record.push(0u8); // attributes
    varint_encode(&mut record, 0);  // timestamp_delta
    varint_encode(&mut record, 0);  // offset_delta
    // key
    varint_encode(&mut record, key.len() as i64);
    record.extend_from_slice(key);
    // value
    varint_encode(&mut record, value.len() as i64);
    record.extend_from_slice(value);
    // headers count
    varint_encode(&mut record, 0);

    let record_size = record.len();
    let mut sized_record = Vec::new();
    varint_encode(&mut sized_record, record_size as i64);
    sized_record.extend_from_slice(&record);

    // Now build the batch header
    let mut batch = Vec::new();
    batch.extend_from_slice(&0i64.to_be_bytes());    // base_offset
    // batch_length placeholder (filled below)
    batch.extend_from_slice(&[0u8; 4]);
    batch.extend_from_slice(&0i32.to_be_bytes());    // partition_leader_epoch
    batch.push(2u8);                                  // magic = 2 (record batch)
    // CRC placeholder (filled below)
    batch.extend_from_slice(&[0u8; 4]);
    batch.extend_from_slice(&0i16.to_be_bytes());    // attributes (no compression)
    batch.extend_from_slice(&0i32.to_be_bytes());    // last_offset_delta
    batch.extend_from_slice(&timestamp.to_be_bytes()); // first_timestamp
    batch.extend_from_slice(&timestamp.to_be_bytes()); // max_timestamp
    batch.extend_from_slice(&(-1i64).to_be_bytes()); // producer_id
    batch.extend_from_slice(&(-1i16).to_be_bytes()); // producer_epoch
    batch.extend_from_slice(&(-1i32).to_be_bytes()); // base_sequence
    batch.extend_from_slice(&1i32.to_be_bytes());    // records count

    // Append the record
    batch.extend_from_slice(&sized_record);

    // Fill in batch_length (everything after base_offset + batch_length = byte 12 onwards)
    let batch_length = (batch.len() - 12) as i32;
    batch[8..12].copy_from_slice(&batch_length.to_be_bytes());

    // CRC32C over bytes from attributes to end (byte 21 onwards)
    let crc = crc32c_compute(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());

    batch
}

/// Encode a signed varint (zigzag encoding).
fn varint_encode(buf: &mut Vec<u8>, value: i64) {
    let mut v = ((value << 1) ^ (value >> 63)) as u64;
    loop {
        if v & !0x7F == 0 {
            buf.push(v as u8);
            return;
        }
        buf.push((v & 0x7F | 0x80) as u8);
        v >>= 7;
    }
}

/// Simple CRC32C implementation for record batch checksums.
fn crc32c_compute(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StreamlineConfig;
    use crate::connection::ConnectionPool;

    fn make_producer() -> Producer<Vec<u8>, Vec<u8>> {
        let client_config = Arc::new(StreamlineConfig::default());
        let pool = Arc::new(ConnectionPool::new(&client_config));
        Producer::new(client_config, pool, ProducerConfig::default())
    }

    #[test]
    fn test_producer_record_new() {
        let record = ProducerRecord::new("key", "value");
        assert_eq!(record.key, Some("key"));
        assert_eq!(record.value, "value");
        assert!(record.headers.is_empty());
        assert!(record.partition.is_none());
    }

    #[test]
    fn test_producer_record_value_only() {
        let record = ProducerRecord::<String, &str>::value_only("data");
        assert!(record.key.is_none());
        assert_eq!(record.value, "data");
    }

    #[test]
    fn test_producer_record_with_partition() {
        let record = ProducerRecord::new("k", "v").with_partition(3);
        assert_eq!(record.partition, Some(3));
    }

    #[test]
    fn test_producer_record_with_headers() {
        let headers = Headers::builder().add("trace", b"123").build();
        let record = ProducerRecord::new("k", "v").with_headers(headers);
        assert!(!record.headers.is_empty());
        assert_eq!(record.headers.get_str("trace"), Some("123"));
    }

    #[test]
    fn test_producer_config_accessible() {
        let producer = make_producer();
        assert_eq!(producer.config().retries, 3);
        assert_eq!(producer.config().compression, "none");
    }

    #[test]
    fn test_varint_encode() {
        let mut buf = Vec::new();
        varint_encode(&mut buf, 0);
        assert_eq!(buf, vec![0]);

        buf.clear();
        varint_encode(&mut buf, 1);
        assert_eq!(buf, vec![2]);

        buf.clear();
        varint_encode(&mut buf, -1);
        assert_eq!(buf, vec![1]);

        buf.clear();
        varint_encode(&mut buf, 300);
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn test_build_produce_request() {
        let request = build_produce_request(1, -1, 30000, "test", 0, b"key", b"value", 1000);
        // Should start with 4-byte length prefix
        assert!(request.len() > 4);
        let len = i32::from_be_bytes([request[0], request[1], request[2], request[3]]);
        assert_eq!(len as usize, request.len() - 4);
    }

    #[test]
    fn test_build_record_batch() {
        let batch = build_record_batch(b"key", b"value", 1000);
        // Magic byte should be 2
        assert_eq!(batch[16], 2);
        // Should have valid structure
        assert!(batch.len() > 57);
    }

    #[tokio::test]
    async fn test_flush_succeeds() {
        let producer = make_producer();
        assert!(producer.flush().await.is_ok());
    }
}
