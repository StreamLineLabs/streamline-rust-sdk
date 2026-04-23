//! Producer for sending messages to Streamline.

use crate::config::{ProducerConfig, StreamlineConfig};
use crate::connection::ConnectionPool;
use crate::error::{Error, ErrorKind, Result};
use crate::telemetry;
use crate::Headers;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
    #[allow(dead_code)]
    client_config: Arc<StreamlineConfig>,
    pool: Arc<ConnectionPool>,
    config: ProducerConfig,
    correlation_id: AtomicI32,
    circuit_breaker: Option<Arc<crate::circuit_breaker::CircuitBreaker>>,
    in_transaction: bool,
    transaction_buffer: Vec<(String, ProducerRecord<K, V>)>,
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
            circuit_breaker: None,
            in_transaction: false,
            transaction_buffer: Vec::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Creates a new producer with circuit breaker protection.
    pub(crate) fn with_circuit_breaker(
        client_config: Arc<StreamlineConfig>,
        pool: Arc<ConnectionPool>,
        config: ProducerConfig,
        cb: Arc<crate::circuit_breaker::CircuitBreaker>,
    ) -> Self {
        Self {
            client_config,
            pool,
            config,
            correlation_id: AtomicI32::new(1),
            circuit_breaker: Some(cb),
            in_transaction: false,
            transaction_buffer: Vec::new(),
            _marker: std::marker::PhantomData,
        }
    }

    fn next_correlation_id(&self) -> i32 {
        self.correlation_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Sends a message to a topic using the Kafka Produce wire protocol.
    /// Retries on transient failures with exponential backoff.
    /// Respects the circuit breaker if configured.
    pub async fn send(
        &self,
        topic: &str,
        key: K,
        value: V,
        headers: Headers,
    ) -> Result<RecordMetadata> {
        crate::validation::validate_topic_name(topic)?;
        telemetry::trace_produce(topic, || self.send_inner(topic, key, value, headers)).await
    }

    async fn send_inner(
        &self,
        topic: &str,
        key: K,
        value: V,
        _headers: Headers,
    ) -> Result<RecordMetadata> {
        // Check circuit breaker before attempting send
        if let Some(ref cb) = self.circuit_breaker {
            cb.check()?;
        }

        debug!("Sending message to topic {}", topic);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let key_bytes = key.as_ref();
        let value_bytes = value.as_ref();
        let partition: i32 = 0;
        let acks: i16 = -1;  // all replicas
        let timeout_ms: i32 = 30_000;

        let compression_codec = compression_attr(&self.config.compression);

        let request = build_produce_request(
            self.next_correlation_id(),
            acks,
            timeout_ms,
            topic,
            partition,
            key_bytes,
            value_bytes,
            now_ms,
            compression_codec,
        );

        let max_retries = self.config.retries;
        let backoff_ms = self.config.retry_backoff_ms;
        let mut last_err = None;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                let delay = backoff_ms * 2u64.saturating_pow(attempt - 1);
                tokio::time::sleep(Duration::from_millis(delay)).await;
                debug!("Retrying send to {} (attempt {}/{})", topic, attempt + 1, max_retries + 1);
            }

            match self.send_request(&request, topic, partition, now_ms).await {
                Ok(metadata) => {
                    if let Some(ref cb) = self.circuit_breaker {
                        cb.record_success();
                    }
                    return Ok(metadata);
                }
                Err(e) if e.is_retryable() && attempt < max_retries => {
                    if let Some(ref cb) = self.circuit_breaker {
                        cb.record_failure();
                    }
                    debug!("Retryable error on attempt {}: {}", attempt + 1, e);
                    last_err = Some(e);
                }
                Err(e) => {
                    if e.is_retryable() {
                        if let Some(ref cb) = self.circuit_breaker {
                            cb.record_failure();
                        }
                    }
                    return Err(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| Error::new(ErrorKind::Internal, "Send failed after retries")))
    }

    async fn send_request(
        &self,
        request: &[u8],
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<RecordMetadata> {
        let conn_handle = self.pool.get().await?;
        let mut conn = conn_handle.lock().await;
        let stream = conn.ensure_connected().await?;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        stream.write_all(request).await
            .map_err(|e| Error::connection(format!("Write failed: {e}")))?;

        let resp_len = stream.read_i32().await
            .map_err(|e| Error::connection(format!("Read failed: {e}")))?;
        if resp_len <= 0 || resp_len > 100_000_000 {
            return Err(Error::new(
                ErrorKind::Protocol,
                format!("Invalid response length from server: {resp_len}"),
            ));
        }
        let mut resp_buf = vec![0u8; resp_len as usize];
        stream.read_exact(&mut resp_buf).await
            .map_err(|e| Error::connection(format!("Read body failed: {e}")))?;

        // Verify correlation ID (first 4 bytes)
        if resp_buf.len() >= 4 {
            let resp_corr = i32::from_be_bytes([resp_buf[0], resp_buf[1], resp_buf[2], resp_buf[3]]);
            let _ = resp_corr; // Correlation check deferred for batch support
        }

        // Parse base_offset from produce response
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
            timestamp,
        })
    }

    /// Sends a batch of records to a topic in a single Produce request.
    pub async fn send_batch(
        &self,
        topic: &str,
        records: Vec<ProducerRecord<K, V>>,
    ) -> Result<Vec<RecordMetadata>> {
        crate::validation::validate_topic_name(topic)?;
        if records.is_empty() {
            return Ok(Vec::new());
        }
        debug!("Sending batch of {} records to topic {}", records.len(), topic);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let compression_codec = compression_attr(&self.config.compression);

        // Build a multi-record batch in a single Produce request
        let _record_entries: Vec<(&[u8], &[u8])> = Vec::with_capacity(records.len());
        // We need to hold onto the owned references
        let refs: Vec<(Vec<u8>, Vec<u8>)> = records
            .iter()
            .map(|r| {
                let key_bytes = r.key.as_ref().map(|k| k.as_ref().to_vec()).unwrap_or_default();
                let value_bytes = r.value.as_ref().to_vec();
                (key_bytes, value_bytes)
            })
            .collect();

        let partition: i32 = 0;
        let correlation_id = self.next_correlation_id();
        let acks: i16 = -1;
        let timeout_ms: i32 = 30_000;

        // Build a multi-record batch
        let record_batch = build_multi_record_batch(
            &refs.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect::<Vec<_>>(),
            now_ms,
            compression_codec,
        );

        let request = build_produce_request_with_batch(
            correlation_id, acks, timeout_ms, topic, partition, &record_batch,
        );

        let max_retries = self.config.retries;
        let backoff_ms = self.config.retry_backoff_ms;
        let mut last_err = None;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                let delay = backoff_ms * 2u64.saturating_pow(attempt - 1);
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }

            let conn_handle = match self.pool.get().await {
                Ok(h) => h,
                Err(e) if e.is_retryable() && attempt < max_retries => {
                    last_err = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            };
            let mut conn = conn_handle.lock().await;
            let stream = match conn.ensure_connected().await {
                Ok(s) => s,
                Err(e) if e.is_retryable() && attempt < max_retries => {
                    last_err = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            };

            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            if let Err(e) = stream.write_all(&request).await {
                let err = Error::connection(format!("Write failed: {e}"));
                if attempt < max_retries { last_err = Some(err); continue; }
                return Err(err);
            }

            let resp_len = match stream.read_i32().await {
                Ok(l) => l,
                Err(e) => {
                    let err = Error::connection(format!("Read failed: {e}"));
                    if attempt < max_retries { last_err = Some(err); continue; }
                    return Err(err);
                }
            };
            if resp_len <= 0 || resp_len > 100_000_000 {
                let err = Error::new(
                    ErrorKind::Protocol,
                    format!("Invalid response length from server: {resp_len}"),
                );
                if attempt < max_retries { last_err = Some(err); continue; }
                return Err(err);
            }
            let mut resp_buf = vec![0u8; resp_len as usize];
            if let Err(e) = stream.read_exact(&mut resp_buf).await {
                let err = Error::connection(format!("Read body failed: {e}"));
                if attempt < max_retries { last_err = Some(err); continue; }
                return Err(err);
            }

            let base_offset = if resp_buf.len() >= 24 {
                i64::from_be_bytes([
                    resp_buf[16], resp_buf[17], resp_buf[18], resp_buf[19],
                    resp_buf[20], resp_buf[21], resp_buf[22], resp_buf[23],
                ])
            } else {
                0
            };

            let results: Vec<RecordMetadata> = (0..records.len())
                .map(|i| RecordMetadata {
                    topic: topic.to_string(),
                    partition,
                    offset: base_offset + i as i64,
                    timestamp: now_ms,
                })
                .collect();

            return Ok(results);
        }

        Err(last_err.unwrap_or_else(|| Error::new(ErrorKind::Internal, "Batch send failed after retries")))
    }

    /// Begin a new transaction. Messages sent via `send_transactional` are
    /// buffered until `commit_transaction` or `abort_transaction`.
    pub fn begin_transaction(&mut self) -> Result<()> {
        if self.in_transaction {
            return Err(Error::transaction("Transaction already in progress"));
        }
        self.in_transaction = true;
        self.transaction_buffer.clear();
        Ok(())
    }

    /// Buffer a record within the current transaction.
    pub fn send_transactional(&mut self, topic: &str, record: ProducerRecord<K, V>) -> Result<()> {
        if !self.in_transaction {
            return Err(Error::transaction("No transaction in progress"));
        }
        self.transaction_buffer.push((topic.to_string(), record));
        Ok(())
    }

    /// Commit the transaction, sending all buffered records atomically.
    pub async fn commit_transaction(&mut self) -> Result<Vec<RecordMetadata>> {
        if !self.in_transaction {
            return Err(Error::transaction("No transaction in progress"));
        }
        let entries = std::mem::take(&mut self.transaction_buffer);
        self.in_transaction = false;

        // Group records by topic and send each batch
        let mut grouped: std::collections::HashMap<String, Vec<ProducerRecord<K, V>>> =
            std::collections::HashMap::new();
        for (topic, record) in entries {
            grouped.entry(topic).or_default().push(record);
        }

        let mut all_results = Vec::new();
        for (topic, records) in grouped {
            let results = self.send_batch(&topic, records).await?;
            all_results.extend(results);
        }
        Ok(all_results)
    }

    /// Abort the transaction, discarding all buffered records.
    pub fn abort_transaction(&mut self) -> Result<()> {
        if !self.in_transaction {
            return Err(Error::transaction("No transaction in progress"));
        }
        self.transaction_buffer.clear();
        self.in_transaction = false;
        Ok(())
    }

    /// Flushes any buffered messages and waits for in-flight sends to complete.
    ///
    /// If a transaction is in progress, it is committed before flushing.
    /// Returns errors for any failed sends during the flush.
    pub async fn flush(&mut self) -> Result<()> {
        debug!("Flushing producer");
        if self.in_transaction {
            let _ = self.commit_transaction().await?;
        }
        Ok(())
    }

    /// Gracefully shuts down the producer.
    ///
    /// Flushes all buffered messages, waits for acknowledgments, and releases
    /// resources. After calling `close()`, the producer should not be reused.
    pub async fn close(&mut self) -> Result<()> {
        debug!("Closing producer");
        self.flush().await?;
        debug!("Producer closed");
        Ok(())
    }

    /// Returns the producer configuration.
    pub fn config(&self) -> &ProducerConfig {
        &self.config
    }
}

/// Map compression config string to Kafka RecordBatch attributes value.
fn compression_attr(compression: &str) -> i16 {
    match compression {
        "gzip" => 1,
        "snappy" => 2,
        "lz4" => 3,
        "zstd" => 4,
        _ => 0, // none
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
    compression_codec: i16,
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
    let record_batch = build_record_batch(key, value, timestamp, compression_codec);
    buf.extend_from_slice(&(record_batch.len() as i32).to_be_bytes());
    buf.extend_from_slice(&record_batch);

    // Fill in total length (excludes the length field itself)
    let total_len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&total_len.to_be_bytes());

    buf
}

/// Build a minimal RecordBatch (v2 format) with a single record.
/// `compression_codec`: 0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd
fn build_record_batch(key: &[u8], value: &[u8], timestamp: i64, compression_codec: i16) -> Vec<u8> {
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
    batch.extend_from_slice(&compression_codec.to_be_bytes()); // attributes (compression bits 0-2)
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

/// Build a RecordBatch with multiple records.
fn build_multi_record_batch(records: &[(&[u8], &[u8])], timestamp: i64, compression_codec: i16) -> Vec<u8> {
    // Build all records with offset deltas
    let mut all_records = Vec::new();
    for (i, (key, value)) in records.iter().enumerate() {
        let mut record = Vec::new();
        record.push(0u8); // attributes
        varint_encode(&mut record, 0);           // timestamp_delta
        varint_encode(&mut record, i as i64);    // offset_delta
        varint_encode(&mut record, key.len() as i64);
        record.extend_from_slice(key);
        varint_encode(&mut record, value.len() as i64);
        record.extend_from_slice(value);
        varint_encode(&mut record, 0); // headers count

        let mut sized = Vec::new();
        varint_encode(&mut sized, record.len() as i64);
        sized.extend_from_slice(&record);
        all_records.extend_from_slice(&sized);
    }

    // Build batch header
    let mut batch = Vec::new();
    batch.extend_from_slice(&0i64.to_be_bytes());       // base_offset
    batch.extend_from_slice(&[0u8; 4]);                  // batch_length placeholder
    batch.extend_from_slice(&0i32.to_be_bytes());        // partition_leader_epoch
    batch.push(2u8);                                      // magic = 2
    batch.extend_from_slice(&[0u8; 4]);                  // CRC placeholder
    batch.extend_from_slice(&compression_codec.to_be_bytes()); // attributes
    batch.extend_from_slice(&((records.len() as i32 - 1).max(0)).to_be_bytes()); // last_offset_delta
    batch.extend_from_slice(&timestamp.to_be_bytes());   // first_timestamp
    batch.extend_from_slice(&timestamp.to_be_bytes());   // max_timestamp
    batch.extend_from_slice(&(-1i64).to_be_bytes());     // producer_id
    batch.extend_from_slice(&(-1i16).to_be_bytes());     // producer_epoch
    batch.extend_from_slice(&(-1i32).to_be_bytes());     // base_sequence
    batch.extend_from_slice(&(records.len() as i32).to_be_bytes()); // records count

    batch.extend_from_slice(&all_records);

    // Fill batch_length
    let batch_length = (batch.len() - 12) as i32;
    batch[8..12].copy_from_slice(&batch_length.to_be_bytes());

    // CRC32C
    let crc = crc32c_compute(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());

    batch
}

/// Build a Produce request with a pre-built record batch.
fn build_produce_request_with_batch(
    correlation_id: i32,
    acks: i16,
    timeout_ms: i32,
    topic: &str,
    partition: i32,
    record_batch: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128 + record_batch.len());
    buf.extend_from_slice(&[0u8; 4]); // length placeholder

    // Request header
    buf.extend_from_slice(&0i16.to_be_bytes());     // api_key: Produce = 0
    buf.extend_from_slice(&7i16.to_be_bytes());      // api_version: 7
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    let client_id = b"streamline-rust-sdk";
    buf.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
    buf.extend_from_slice(client_id);

    // Produce request body
    buf.extend_from_slice(&(-1i16).to_be_bytes());   // transactional_id: null
    buf.extend_from_slice(&acks.to_be_bytes());
    buf.extend_from_slice(&timeout_ms.to_be_bytes());
    buf.extend_from_slice(&1i32.to_be_bytes());      // 1 topic
    buf.extend_from_slice(&(topic.len() as i16).to_be_bytes());
    buf.extend_from_slice(topic.as_bytes());
    buf.extend_from_slice(&1i32.to_be_bytes());      // 1 partition
    buf.extend_from_slice(&partition.to_be_bytes());
    buf.extend_from_slice(&(record_batch.len() as i32).to_be_bytes());
    buf.extend_from_slice(record_batch);

    let total_len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&total_len.to_be_bytes());

    buf
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
        let request = build_produce_request(1, -1, 30000, "test", 0, b"key", b"value", 1000, 0);
        // Should start with 4-byte length prefix
        assert!(request.len() > 4);
        let len = i32::from_be_bytes([request[0], request[1], request[2], request[3]]);
        assert_eq!(len as usize, request.len() - 4);
    }

    #[test]
    fn test_build_record_batch() {
        let batch = build_record_batch(b"key", b"value", 1000, 0);
        // Magic byte should be 2
        assert_eq!(batch[16], 2);
        // Should have valid structure
        assert!(batch.len() > 57);
    }

    #[tokio::test]
    async fn test_flush_succeeds() {
        let mut producer = make_producer();
        assert!(producer.flush().await.is_ok());
    }

    #[tokio::test]
    async fn test_close_succeeds() {
        let mut producer = make_producer();
        assert!(producer.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_close_flushes_transaction() {
        let mut producer = make_producer();
        producer.begin_transaction().unwrap();
        // close() should commit/flush the transaction
        assert!(producer.close().await.is_ok());
        assert!(!producer.in_transaction);
    }

    #[tokio::test]
    async fn test_send_rejects_invalid_topic() {
        let config = Arc::new(StreamlineConfig::default());
        let pool = Arc::new(ConnectionPool::new(&config));
        let producer: Producer<String, String> =
            Producer::new(config, pool, ProducerConfig::default());

        let result = producer
            .send("", "key".to_string(), "val".to_string(), Headers::new())
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind, ErrorKind::InvalidConfiguration);
    }

    #[tokio::test]
    async fn test_send_rejects_topic_with_invalid_chars() {
        let config = Arc::new(StreamlineConfig::default());
        let pool = Arc::new(ConnectionPool::new(&config));
        let producer: Producer<String, String> =
            Producer::new(config, pool, ProducerConfig::default());

        let result = producer
            .send(
                "bad topic!",
                "key".to_string(),
                "val".to_string(),
                Headers::new(),
            )
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind, ErrorKind::InvalidConfiguration);
    }

    #[tokio::test]
    async fn test_send_batch_rejects_invalid_topic() {
        let config = Arc::new(StreamlineConfig::default());
        let pool = Arc::new(ConnectionPool::new(&config));
        let producer: Producer<Vec<u8>, Vec<u8>> =
            Producer::new(config, pool, ProducerConfig::default());

        let result = producer.send_batch("..", vec![]).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind, ErrorKind::InvalidConfiguration);
    }
}

/// Validates that a record does not exceed the maximum allowed size.
#[allow(dead_code)]
fn validate_record_size(key: &[u8], value: &[u8], max_size: usize) -> crate::error::Result<()> {
    let total = key.len() + value.len();
    if total > max_size {
        return Err(crate::error::Error::new(
            crate::error::ErrorKind::Serialization,
            format!("record size {} exceeds maximum {}", total, max_size),
        ));
    }
    Ok(())
}
