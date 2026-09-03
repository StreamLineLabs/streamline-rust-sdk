//! Producer for sending messages to Streamline.

use crate::config::{ProducerConfig, StreamlineConfig};
use crate::connection::{evictable, ConnectionPool};
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
    client_config: Arc<StreamlineConfig>,
    pool: Arc<ConnectionPool>,
    config: ProducerConfig,
    correlation_id: AtomicI32,
    circuit_breaker: Option<Arc<crate::circuit_breaker::CircuitBreaker>>,
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
        headers: Headers,
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
        if self.config.idempotent {
            return Err(Error::unsupported("idempotent producer"));
        }
        validate_record_size(
            Some(key_bytes),
            value_bytes,
            &headers,
            self.config.max_request_size,
        )?;
        let partition: i32 = 0;
        let acks: i16 = -1; // all replicas
        let timeout_ms: i32 = 30_000;

        let compression_codec = compression_attr(&self.config.compression)?;
        let correlation_id = self.next_correlation_id();

        let request = build_produce_request(ProduceRequestParams {
            correlation_id,
            acks,
            timeout_ms,
            topic,
            partition,
            key: key_bytes,
            value: value_bytes,
            timestamp: now_ms,
            compression_codec,
            headers: &headers,
        });
        validate_request_size(&request, self.config.max_request_size)?;

        let max_retries = self.config.retries;
        let backoff_ms = self.config.retry_backoff_ms;
        let mut last_err = None;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                let delay = backoff_ms * 2u64.saturating_pow(attempt - 1);
                tokio::time::sleep(Duration::from_millis(delay)).await;
                debug!(
                    "Retrying send to {} (attempt {}/{})",
                    topic,
                    attempt + 1,
                    max_retries + 1
                );
            }

            match self
                .send_request(&request, correlation_id, topic, partition, now_ms)
                .await
            {
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

        Err(last_err
            .unwrap_or_else(|| Error::new(ErrorKind::Internal, "Send failed after retries")))
    }

    async fn send_request(
        &self,
        request: &[u8],
        expected_correlation_id: i32,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<RecordMetadata> {
        let conn_handle = self.pool.get().await?;
        let mut conn = conn_handle.lock().await;

        // The complete broker exchange (connect, write, read) is bounded by
        // the configured request timeout, and a failed exchange evicts the
        // pooled connection instead of leaving a desynchronized socket behind.
        let resp_buf = conn
            .exchange(request, self.client_config.request_timeout, "produce")
            .await?;

        parse_produce_response(
            &resp_buf,
            expected_correlation_id,
            topic,
            partition,
            timestamp,
        )
        .inspect_err(|error| {
            if evictable(error) {
                conn.evict(&format!("produce response error: {error}"));
            }
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
        if self.config.idempotent {
            return Err(Error::unsupported("idempotent producer"));
        }
        debug!(
            "Sending batch of {} records to topic {}",
            records.len(),
            topic
        );

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let compression_codec = compression_attr(&self.config.compression)?;
        let partition = resolve_batch_partition(&records)?;

        let mut total_size = 0usize;
        for record in &records {
            let key = record.key.as_ref().map(|key| key.as_ref());
            let record_size = record_payload_size(key, record.value.as_ref(), &record.headers)?;
            total_size = total_size.checked_add(record_size).ok_or_else(|| {
                Error::new(
                    ErrorKind::Serialization,
                    "Batch record size overflowed the platform limit",
                )
            })?;
        }
        if total_size > self.config.max_request_size {
            return Err(Error::new(
                ErrorKind::Serialization,
                format!(
                    "batch record size {total_size} exceeds maximum {}",
                    self.config.max_request_size
                ),
            ));
        }

        let refs: Vec<EncodedRecordRef<'_>> = records
            .iter()
            .map(|record| {
                (
                    record.key.as_ref().map(|key| key.as_ref()),
                    record.value.as_ref(),
                    &record.headers,
                )
            })
            .collect();

        let correlation_id = self.next_correlation_id();
        let acks: i16 = -1;
        let timeout_ms: i32 = 30_000;

        // Build a multi-record batch
        let record_batch = build_multi_record_batch(&refs, now_ms, compression_codec);

        let request = build_produce_request_with_batch(
            correlation_id,
            acks,
            timeout_ms,
            topic,
            partition,
            &record_batch,
        );
        validate_request_size(&request, self.config.max_request_size)?;

        let max_retries = self.config.retries;
        let backoff_ms = self.config.retry_backoff_ms;
        let mut last_err = None;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                let delay = backoff_ms * 2u64.saturating_pow(attempt - 1);
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }

            match self
                .send_request(&request, correlation_id, topic, partition, now_ms)
                .await
            {
                Ok(response) => {
                    let results: Vec<RecordMetadata> = (0..records.len())
                        .map(|i| RecordMetadata {
                            topic: topic.to_string(),
                            partition,
                            offset: response.offset + i as i64,
                            timestamp: response.timestamp,
                        })
                        .collect();

                    return Ok(results);
                }
                Err(e) if e.is_retryable() && attempt < max_retries => {
                    last_err = Some(e);
                }
                Err(e) => return Err(e),
            }
        }

        Err(last_err
            .unwrap_or_else(|| Error::new(ErrorKind::Internal, "Batch send failed after retries")))
    }

    /// Returns an explicit unsupported error.
    ///
    /// Kafka transactions require broker-coordinated producer IDs, epochs,
    /// and transaction markers. Version 0.4.0 does not implement that
    /// protocol and never substitutes non-atomic client-side batching.
    pub fn begin_transaction(&mut self) -> Result<()> {
        Err(Error::unsupported("Kafka producer transactions"))
    }

    /// Returns an explicit unsupported error without buffering the record.
    pub fn send_transactional(
        &mut self,
        _topic: &str,
        _record: ProducerRecord<K, V>,
    ) -> Result<()> {
        Err(Error::unsupported("Kafka producer transactions"))
    }

    /// Returns an explicit unsupported error without sending records.
    pub async fn commit_transaction(&mut self) -> Result<Vec<RecordMetadata>> {
        Err(Error::unsupported("Kafka producer transactions"))
    }

    /// Returns an explicit unsupported error.
    pub fn abort_transaction(&mut self) -> Result<()> {
        Err(Error::unsupported("Kafka producer transactions"))
    }

    /// Flushes any buffered messages and waits for in-flight sends to complete.
    ///
    /// Returns errors for any failed sends during the flush.
    pub async fn flush(&mut self) -> Result<()> {
        debug!("Flushing producer");
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

/// Validate the configured compression mode.
///
/// Record batch compression must transform the encoded records in addition to
/// setting the Kafka attributes bits. Until that encoding is implemented,
/// every compressed mode is rejected rather than sending corrupt batches.
fn compression_attr(compression: &str) -> Result<i16> {
    match compression {
        "none" => Ok(0),
        "gzip" | "snappy" | "lz4" | "zstd" => Err(Error::unsupported(&format!(
            "producer compression codec '{compression}'"
        ))),
        _ => Err(Error::new(
            ErrorKind::InvalidConfiguration,
            format!(
                "Unknown producer compression codec '{compression}'; expected 'none', 'gzip', 'snappy', 'lz4', or 'zstd'"
            ),
        )),
    }
}

/// Build a minimal Kafka Produce request (API key 0, version 7).
///
/// Format:
/// [total_length:i32] [request_header] [produce_request_body]
///
/// This uses manual binary encoding to avoid kafka-protocol crate API version
/// incompatibilities. The wire format follows the Kafka protocol specification.
struct ProduceRequestParams<'a> {
    correlation_id: i32,
    acks: i16,
    timeout_ms: i32,
    topic: &'a str,
    partition: i32,
    key: &'a [u8],
    value: &'a [u8],
    timestamp: i64,
    compression_codec: i16,
    headers: &'a Headers,
}

type EncodedRecordRef<'a> = (Option<&'a [u8]>, &'a [u8], &'a Headers);

fn build_produce_request(params: ProduceRequestParams<'_>) -> Vec<u8> {
    let ProduceRequestParams {
        correlation_id,
        acks,
        timeout_ms,
        topic,
        partition,
        key,
        value,
        timestamp,
        compression_codec,
        headers,
    } = params;
    let mut buf = Vec::with_capacity(256 + key.len() + value.len());

    // Placeholder for total length (filled at the end)
    buf.extend_from_slice(&[0u8; 4]);

    // Request header (v1): api_key(i16) + api_version(i16) + correlation_id(i32) + client_id(nullable_string)
    buf.extend_from_slice(&0i16.to_be_bytes()); // api_key: Produce = 0
    buf.extend_from_slice(&7i16.to_be_bytes()); // api_version: 7
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    let client_id = b"streamline-rust-sdk";
    buf.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
    buf.extend_from_slice(client_id);

    // Produce request body (v7):
    // transactional_id(nullable_string) + acks(i16) + timeout_ms(i32) + topic_data(array)
    buf.extend_from_slice(&(-1i16).to_be_bytes()); // transactional_id: null
    buf.extend_from_slice(&acks.to_be_bytes());
    buf.extend_from_slice(&timeout_ms.to_be_bytes());

    // topic_data array: count(i32) + [topic_name + partition_data]
    buf.extend_from_slice(&1i32.to_be_bytes()); // 1 topic

    // topic_name (string)
    buf.extend_from_slice(&(topic.len() as i16).to_be_bytes());
    buf.extend_from_slice(topic.as_bytes());

    // partition_data array: count(i32) + [partition_index + record_set]
    buf.extend_from_slice(&1i32.to_be_bytes()); // 1 partition
    buf.extend_from_slice(&partition.to_be_bytes());

    // Build the record batch (MessageSet v2 / RecordBatch format)
    let record_batch = build_record_batch(key, value, timestamp, compression_codec, headers);
    buf.extend_from_slice(&(record_batch.len() as i32).to_be_bytes());
    buf.extend_from_slice(&record_batch);

    // Fill in total length (excludes the length field itself)
    let total_len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&total_len.to_be_bytes());

    buf
}

/// Build a minimal RecordBatch (v2 format) with a single record.
fn build_record_batch(
    key: &[u8],
    value: &[u8],
    timestamp: i64,
    compression_codec: i16,
    headers: &Headers,
) -> Vec<u8> {
    let sized_record = build_record(Some(key), value, headers, 0);

    // Now build the batch header
    let mut batch = Vec::new();
    batch.extend_from_slice(&0i64.to_be_bytes()); // base_offset
                                                  // batch_length placeholder (filled below)
    batch.extend_from_slice(&[0u8; 4]);
    batch.extend_from_slice(&0i32.to_be_bytes()); // partition_leader_epoch
    batch.push(2u8); // magic = 2 (record batch)
                     // CRC placeholder (filled below)
    batch.extend_from_slice(&[0u8; 4]);
    batch.extend_from_slice(&compression_codec.to_be_bytes()); // attributes (compression bits 0-2)
    batch.extend_from_slice(&0i32.to_be_bytes()); // last_offset_delta
    batch.extend_from_slice(&timestamp.to_be_bytes()); // first_timestamp
    batch.extend_from_slice(&timestamp.to_be_bytes()); // max_timestamp
    batch.extend_from_slice(&(-1i64).to_be_bytes()); // producer_id
    batch.extend_from_slice(&(-1i16).to_be_bytes()); // producer_epoch
    batch.extend_from_slice(&(-1i32).to_be_bytes()); // base_sequence
    batch.extend_from_slice(&1i32.to_be_bytes()); // records count

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

fn build_record(key: Option<&[u8]>, value: &[u8], headers: &Headers, offset_delta: i64) -> Vec<u8> {
    let mut record = Vec::new();
    record.push(0u8); // attributes
    varint_encode(&mut record, 0); // timestamp_delta
    varint_encode(&mut record, offset_delta);
    match key {
        Some(key) => {
            varint_encode(&mut record, key.len() as i64);
            record.extend_from_slice(key);
        }
        None => varint_encode(&mut record, -1),
    }
    varint_encode(&mut record, value.len() as i64);
    record.extend_from_slice(value);
    varint_encode(&mut record, headers.iter().count() as i64);
    for (header_key, header_value) in headers.iter() {
        varint_encode(&mut record, header_key.len() as i64);
        record.extend_from_slice(header_key.as_bytes());
        varint_encode(&mut record, header_value.len() as i64);
        record.extend_from_slice(header_value);
    }

    let record_size = record.len();
    let mut sized_record = Vec::new();
    varint_encode(&mut sized_record, record_size as i64);
    sized_record.extend_from_slice(&record);
    sized_record
}

/// Build a RecordBatch with multiple records.
fn build_multi_record_batch(
    records: &[EncodedRecordRef<'_>],
    timestamp: i64,
    compression_codec: i16,
) -> Vec<u8> {
    // Build all records with offset deltas
    let mut all_records = Vec::new();
    for (i, (key, value, headers)) in records.iter().enumerate() {
        all_records.extend_from_slice(&build_record(*key, value, headers, i as i64));
    }

    // Build batch header
    let mut batch = Vec::new();
    batch.extend_from_slice(&0i64.to_be_bytes()); // base_offset
    batch.extend_from_slice(&[0u8; 4]); // batch_length placeholder
    batch.extend_from_slice(&0i32.to_be_bytes()); // partition_leader_epoch
    batch.push(2u8); // magic = 2
    batch.extend_from_slice(&[0u8; 4]); // CRC placeholder
    batch.extend_from_slice(&compression_codec.to_be_bytes()); // attributes
    batch.extend_from_slice(&((records.len() as i32 - 1).max(0)).to_be_bytes()); // last_offset_delta
    batch.extend_from_slice(&timestamp.to_be_bytes()); // first_timestamp
    batch.extend_from_slice(&timestamp.to_be_bytes()); // max_timestamp
    batch.extend_from_slice(&(-1i64).to_be_bytes()); // producer_id
    batch.extend_from_slice(&(-1i16).to_be_bytes()); // producer_epoch
    batch.extend_from_slice(&(-1i32).to_be_bytes()); // base_sequence
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
    buf.extend_from_slice(&0i16.to_be_bytes()); // api_key: Produce = 0
    buf.extend_from_slice(&7i16.to_be_bytes()); // api_version: 7
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    let client_id = b"streamline-rust-sdk";
    buf.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
    buf.extend_from_slice(client_id);

    // Produce request body
    buf.extend_from_slice(&(-1i16).to_be_bytes()); // transactional_id: null
    buf.extend_from_slice(&acks.to_be_bytes());
    buf.extend_from_slice(&timeout_ms.to_be_bytes());
    buf.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
    buf.extend_from_slice(&(topic.len() as i16).to_be_bytes());
    buf.extend_from_slice(topic.as_bytes());
    buf.extend_from_slice(&1i32.to_be_bytes()); // 1 partition
    buf.extend_from_slice(&partition.to_be_bytes());
    buf.extend_from_slice(&(record_batch.len() as i32).to_be_bytes());
    buf.extend_from_slice(record_batch);

    let total_len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&total_len.to_be_bytes());

    buf
}

fn resolve_batch_partition<K, V>(records: &[ProducerRecord<K, V>]) -> Result<i32> {
    let mut explicit_partition = None;
    let mut has_unassigned_record = false;

    for record in records {
        match record.partition {
            Some(partition) if partition < 0 => {
                return Err(Error::new(
                    ErrorKind::InvalidConfiguration,
                    format!("Producer partition must be non-negative, got {partition}"),
                ));
            }
            Some(partition) => match explicit_partition {
                Some(expected) if expected != partition => {
                    return Err(Error::new(
                        ErrorKind::InvalidConfiguration,
                        "A single producer batch cannot target multiple partitions",
                    )
                    .with_hint("Split records into one send_batch call per partition"));
                }
                None => explicit_partition = Some(partition),
                Some(_) => {}
            },
            None => has_unassigned_record = true,
        }
    }

    if has_unassigned_record && explicit_partition.is_some() {
        return Err(Error::new(
            ErrorKind::InvalidConfiguration,
            "A producer batch cannot mix explicitly assigned and unassigned partitions",
        )
        .with_hint("Assign the same partition to every record or leave every record unassigned"));
    }

    Ok(explicit_partition.unwrap_or(0))
}

fn record_payload_size(key: Option<&[u8]>, value: &[u8], headers: &Headers) -> Result<usize> {
    let mut total = key.map_or(0, <[u8]>::len);
    total = total.checked_add(value.len()).ok_or_else(|| {
        Error::new(
            ErrorKind::Serialization,
            "Record size overflowed the platform limit",
        )
    })?;

    for (header_key, header_value) in headers.iter() {
        total = total
            .checked_add(header_key.len())
            .and_then(|size| size.checked_add(header_value.len()))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Serialization,
                    "Record header size overflowed the platform limit",
                )
            })?;
    }

    Ok(total)
}

fn validate_record_size(
    key: Option<&[u8]>,
    value: &[u8],
    headers: &Headers,
    max_size: usize,
) -> Result<()> {
    let total = record_payload_size(key, value, headers)?;
    if total > max_size {
        return Err(Error::new(
            ErrorKind::Serialization,
            format!("record size {total} exceeds maximum {max_size}"),
        ));
    }
    Ok(())
}

fn validate_request_size(request: &[u8], max_size: usize) -> Result<()> {
    if request.len() > max_size {
        return Err(Error::new(
            ErrorKind::Serialization,
            format!(
                "encoded produce request size {} exceeds maximum {max_size}",
                request.len()
            ),
        ));
    }
    Ok(())
}

struct ResponseCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> ResponseCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize, field: &str) -> Result<&'a [u8]> {
        let end = self.position.checked_add(length).ok_or_else(|| {
            Error::new(
                ErrorKind::Protocol,
                format!("Produce response position overflow while reading {field}"),
            )
        })?;
        if end > self.bytes.len() {
            return Err(Error::new(
                ErrorKind::Protocol,
                format!("Truncated produce response while reading {field}"),
            ));
        }

        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }

    fn read_i16(&mut self, field: &str) -> Result<i16> {
        let bytes = self.take(2, field)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_i32(&mut self, field: &str) -> Result<i32> {
        let bytes = self.take(4, field)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i64(&mut self, field: &str) -> Result<i64> {
        let bytes = self.take(8, field)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_string(&mut self, field: &str) -> Result<&'a str> {
        let length = self.read_i16(field)?;
        if length < 0 {
            return Err(Error::new(
                ErrorKind::Protocol,
                format!("Produce response {field} cannot be null"),
            ));
        }
        let bytes = self.take(length as usize, field)?;
        std::str::from_utf8(bytes).map_err(|error| {
            Error::new(
                ErrorKind::Protocol,
                format!("Produce response {field} is not valid UTF-8"),
            )
            .with_source(error)
        })
    }
}

fn parse_produce_response(
    response: &[u8],
    expected_correlation_id: i32,
    expected_topic: &str,
    expected_partition: i32,
    request_timestamp: i64,
) -> Result<RecordMetadata> {
    let mut cursor = ResponseCursor::new(response);
    let correlation_id = cursor.read_i32("correlation ID")?;
    if correlation_id != expected_correlation_id {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!(
                "Produce response correlation ID {correlation_id} did not match request {expected_correlation_id}"
            ),
        ));
    }

    let topic_count = cursor.read_i32("topic count")?;
    if topic_count <= 0 {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!("Produce response contained invalid topic count {topic_count}"),
        ));
    }

    let mut matched_partition = None;
    for _ in 0..topic_count {
        let topic = cursor.read_string("topic name")?;
        let partition_count = cursor.read_i32("partition count")?;
        if partition_count <= 0 {
            return Err(Error::new(
                ErrorKind::Protocol,
                format!(
                    "Produce response for topic '{topic}' contained invalid partition count {partition_count}"
                ),
            ));
        }

        for _ in 0..partition_count {
            let partition = cursor.read_i32("partition index")?;
            let error_code = cursor.read_i16("partition error code")?;
            let base_offset = cursor.read_i64("base offset")?;
            let log_append_time = cursor.read_i64("log append time")?;
            let _log_start_offset = cursor.read_i64("log start offset")?;

            if topic == expected_topic && partition == expected_partition {
                if matched_partition.is_some() {
                    return Err(Error::new(
                        ErrorKind::Protocol,
                        format!(
                            "Produce response contained duplicate partition {expected_topic}:{expected_partition}"
                        ),
                    ));
                }
                matched_partition = Some((error_code, base_offset, log_append_time));
            }
        }
    }

    let _throttle_time_ms = cursor.read_i32("throttle time")?;
    if cursor.position != response.len() {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!(
                "Produce response contained {} unexpected trailing bytes",
                response.len() - cursor.position
            ),
        ));
    }

    let Some((error_code, base_offset, log_append_time)) = matched_partition else {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!(
                "Produce response did not contain requested partition {expected_topic}:{expected_partition}"
            ),
        ));
    };

    if error_code != 0 {
        return Err(kafka_produce_error(
            error_code,
            expected_topic,
            expected_partition,
        ));
    }
    if base_offset < 0 {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!(
                "Produce response returned invalid base offset {base_offset} for {expected_topic}:{expected_partition}"
            ),
        ));
    }

    Ok(RecordMetadata {
        topic: expected_topic.to_string(),
        partition: expected_partition,
        offset: base_offset,
        timestamp: if log_append_time >= 0 {
            log_append_time
        } else {
            request_timestamp
        },
    })
}

fn kafka_produce_error(error_code: i16, topic: &str, partition: i32) -> Error {
    let kind = match error_code {
        7 => ErrorKind::Timeout,
        29 | 31 => ErrorKind::AuthorizationFailed,
        5 | 6 | 19 | 20 | 56 => ErrorKind::Server,
        _ => ErrorKind::Protocol,
    };

    Error::new(
        kind,
        format!(
            "Broker rejected produce for {topic}:{partition} with Kafka error code {error_code}"
        ),
    )
    .with_hint("Check the broker error code and server logs before retrying the record")
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

/// Simple CRC32C (Castagnoli) implementation for record batch checksums.
///
/// Shared with the consumer, which validates the CRC of every record batch it
/// decodes before trusting the batch contents.
pub(crate) fn crc32c_compute(data: &[u8]) -> u32 {
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
        let headers = Headers::new();
        let request = build_produce_request(ProduceRequestParams {
            correlation_id: 1,
            acks: -1,
            timeout_ms: 30000,
            topic: "test",
            partition: 0,
            key: b"key",
            value: b"value",
            timestamp: 1000,
            compression_codec: 0,
            headers: &headers,
        });
        // Should start with 4-byte length prefix
        assert!(request.len() > 4);
        let len = i32::from_be_bytes([request[0], request[1], request[2], request[3]]);
        assert_eq!(len as usize, request.len() - 4);
    }

    #[test]
    fn test_build_record_batch() {
        let batch = build_record_batch(b"key", b"value", 1000, 0, &Headers::new());
        // Magic byte should be 2
        assert_eq!(batch[16], 2);
        // Should have valid structure
        assert!(batch.len() > 57);
    }

    #[test]
    fn test_build_record_encodes_headers_and_null_key() {
        let headers = Headers::builder().add("trace-id", b"trace-value").build();
        let record = build_record(None, b"payload", &headers, 0);
        let mut position = 0;

        let record_length = decode_test_varint(&record, &mut position);
        assert_eq!(record_length as usize, record.len() - position);
        position += 1; // attributes
        assert_eq!(decode_test_varint(&record, &mut position), 0);
        assert_eq!(decode_test_varint(&record, &mut position), 0);
        assert_eq!(decode_test_varint(&record, &mut position), -1);

        let value_length = decode_test_varint(&record, &mut position) as usize;
        assert_eq!(&record[position..position + value_length], b"payload");
        position += value_length;

        assert_eq!(decode_test_varint(&record, &mut position), 1);
        let header_key_length = decode_test_varint(&record, &mut position) as usize;
        assert_eq!(&record[position..position + header_key_length], b"trace-id");
        position += header_key_length;
        let header_value_length = decode_test_varint(&record, &mut position) as usize;
        assert_eq!(
            &record[position..position + header_value_length],
            b"trace-value"
        );
    }

    #[test]
    fn test_batch_partition_resolution_is_explicit() {
        let records = vec![
            ProducerRecord::new("k1", "v1").with_partition(3),
            ProducerRecord::new("k2", "v2").with_partition(3),
        ];
        assert_eq!(resolve_batch_partition(&records).unwrap(), 3);

        let mixed_partitions = vec![
            ProducerRecord::new("k1", "v1").with_partition(1),
            ProducerRecord::new("k2", "v2").with_partition(2),
        ];
        assert_eq!(
            resolve_batch_partition(&mixed_partitions).unwrap_err().kind,
            ErrorKind::InvalidConfiguration
        );

        let partially_assigned = vec![
            ProducerRecord::new("k1", "v1").with_partition(1),
            ProducerRecord::new("k2", "v2"),
        ];
        assert_eq!(
            resolve_batch_partition(&partially_assigned)
                .unwrap_err()
                .kind,
            ErrorKind::InvalidConfiguration
        );

        let negative = vec![ProducerRecord::new("k", "v").with_partition(-1)];
        assert_eq!(
            resolve_batch_partition(&negative).unwrap_err().kind,
            ErrorKind::InvalidConfiguration
        );
    }

    #[test]
    fn test_parse_produce_response_validates_success() {
        let response = build_test_produce_response(42, "events", 3, 0, 99, 1234);
        let metadata = parse_produce_response(&response, 42, "events", 3, 1000).unwrap();

        assert_eq!(metadata.topic, "events");
        assert_eq!(metadata.partition, 3);
        assert_eq!(metadata.offset, 99);
        assert_eq!(metadata.timestamp, 1234);
    }

    #[test]
    fn test_parse_produce_response_rejects_unvalidated_success() {
        let response = build_test_produce_response(42, "events", 3, 0, 99, -1);

        let correlation_error =
            parse_produce_response(&response, 41, "events", 3, 1000).unwrap_err();
        assert_eq!(correlation_error.kind, ErrorKind::Protocol);

        let topic_error = parse_produce_response(&response, 42, "other", 3, 1000).unwrap_err();
        assert_eq!(topic_error.kind, ErrorKind::Protocol);

        let partition_error = parse_produce_response(&response, 42, "events", 2, 1000).unwrap_err();
        assert_eq!(partition_error.kind, ErrorKind::Protocol);

        let truncated_error =
            parse_produce_response(&response[..response.len() - 1], 42, "events", 3, 1000)
                .unwrap_err();
        assert_eq!(truncated_error.kind, ErrorKind::Protocol);

        let mut response_with_trailing_bytes = response;
        response_with_trailing_bytes.push(0);
        let trailing_error =
            parse_produce_response(&response_with_trailing_bytes, 42, "events", 3, 1000)
                .unwrap_err();
        assert_eq!(trailing_error.kind, ErrorKind::Protocol);
    }

    #[test]
    fn test_parse_produce_response_rejects_broker_error_and_invalid_offset() {
        let broker_error = build_test_produce_response(42, "events", 0, 29, -1, -1);
        let error = parse_produce_response(&broker_error, 42, "events", 0, 1000).unwrap_err();
        assert_eq!(error.kind, ErrorKind::AuthorizationFailed);

        let invalid_offset = build_test_produce_response(42, "events", 0, 0, -1, -1);
        let error = parse_produce_response(&invalid_offset, 42, "events", 0, 1000).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
    }

    #[tokio::test]
    async fn test_send_batch_retries_retryable_broker_error() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            for (error_code, offset) in [(7, -1), (0, 42)] {
                let request_length = stream.read_i32().await.unwrap();
                let mut request = vec![0; request_length as usize];
                stream.read_exact(&mut request).await.unwrap();

                let response = build_test_produce_response(1, "events", 0, error_code, offset, -1);
                stream.write_i32(response.len() as i32).await.unwrap();
                stream.write_all(&response).await.unwrap();
            }
        });

        let client_config = Arc::new(StreamlineConfig {
            bootstrap_servers: address.to_string(),
            connection_pool_size: 1,
            ..Default::default()
        });
        let pool = Arc::new(ConnectionPool::new(&client_config));
        let producer = Producer::new(
            client_config,
            pool,
            ProducerConfig {
                retries: 1,
                retry_backoff_ms: 0,
                ..Default::default()
            },
        );

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            producer.send_batch(
                "events",
                vec![ProducerRecord::new(b"key".to_vec(), b"value".to_vec())],
            ),
        )
        .await
        .unwrap()
        .unwrap();

        server.await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].offset, 42);
    }

    #[tokio::test]
    async fn test_send_applies_request_timeout_and_evicts_connection() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // Accept the connection but never answer, so only the request timeout
        // can end the exchange.
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(stream);
        });

        let client_config = Arc::new(StreamlineConfig {
            bootstrap_servers: address.to_string(),
            connection_pool_size: 1,
            request_timeout: Duration::from_millis(150),
            ..Default::default()
        });
        let pool = Arc::new(ConnectionPool::new(&client_config));
        let producer = Producer::new(
            client_config,
            pool.clone(),
            ProducerConfig {
                retries: 0,
                retry_backoff_ms: 0,
                ..Default::default()
            },
        );

        let error = tokio::time::timeout(
            Duration::from_secs(5),
            producer.send("events", b"key".to_vec(), b"value".to_vec(), Headers::new()),
        )
        .await
        .expect("request timeout must bound the exchange")
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Timeout);
        assert!(
            !pool.is_healthy().await,
            "a timed-out connection must be evicted from the pool"
        );
        server.abort();
    }

    #[tokio::test]
    async fn test_send_evicts_connection_after_protocol_error() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request_length = stream.read_i32().await.unwrap();
            let mut request = vec![0; request_length as usize];
            stream.read_exact(&mut request).await.unwrap();
            // Well-framed but semantically invalid: wrong correlation ID.
            let response = build_test_produce_response(9999, "events", 0, 0, 1, -1);
            stream.write_i32(response.len() as i32).await.unwrap();
            stream.write_all(&response).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let client_config = Arc::new(StreamlineConfig {
            bootstrap_servers: address.to_string(),
            connection_pool_size: 1,
            request_timeout: Duration::from_secs(2),
            ..Default::default()
        });
        let pool = Arc::new(ConnectionPool::new(&client_config));
        let producer = Producer::new(
            client_config,
            pool.clone(),
            ProducerConfig {
                retries: 0,
                retry_backoff_ms: 0,
                ..Default::default()
            },
        );

        let error = producer
            .send("events", b"key".to_vec(), b"value".to_vec(), Headers::new())
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(
            !pool.is_healthy().await,
            "a protocol-violating connection must be evicted from the pool"
        );
        server.abort();
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

    #[test]
    fn test_transactions_fail_closed() {
        let mut producer = make_producer();
        let record = ProducerRecord::new(b"key".to_vec(), b"value".to_vec());

        assert_eq!(
            producer.begin_transaction().unwrap_err().kind,
            ErrorKind::Unsupported
        );
        assert_eq!(
            producer
                .send_transactional("events", record)
                .unwrap_err()
                .kind,
            ErrorKind::Unsupported
        );
        assert_eq!(
            producer.abort_transaction().unwrap_err().kind,
            ErrorKind::Unsupported
        );
    }

    #[tokio::test]
    async fn test_commit_transaction_fails_closed() {
        let mut producer = make_producer();
        let error = producer.commit_transaction().await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unsupported);
    }

    #[test]
    fn test_compression_validation_fails_closed() {
        assert_eq!(compression_attr("none").unwrap(), 0);

        for codec in ["gzip", "snappy", "lz4", "zstd"] {
            let error = compression_attr(codec).unwrap_err();
            assert_eq!(error.kind, ErrorKind::Unsupported);
            assert!(error.message.contains(codec));
        }

        let error = compression_attr("brotli").unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidConfiguration);
    }

    #[tokio::test]
    async fn test_send_rejects_compression_before_connecting() {
        let client_config = Arc::new(StreamlineConfig {
            bootstrap_servers: "192.0.2.1:1".to_string(),
            ..Default::default()
        });
        let pool = Arc::new(ConnectionPool::new(&client_config));
        let config = ProducerConfig {
            compression: "lz4".to_string(),
            ..Default::default()
        };
        let producer = Producer::new(client_config, pool, config);

        let error = producer
            .send("events", b"key".to_vec(), b"value".to_vec(), Headers::new())
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Unsupported);
    }

    #[tokio::test]
    async fn test_send_rejects_idempotence_before_connecting() {
        let client_config = Arc::new(StreamlineConfig {
            bootstrap_servers: "192.0.2.1:1".to_string(),
            ..Default::default()
        });
        let pool = Arc::new(ConnectionPool::new(&client_config));
        let config = ProducerConfig {
            idempotent: true,
            ..Default::default()
        };
        let producer = Producer::new(client_config, pool, config);

        let error = producer
            .send("events", b"key".to_vec(), b"value".to_vec(), Headers::new())
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Unsupported);
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

    fn decode_test_varint(bytes: &[u8], position: &mut usize) -> i64 {
        let mut result = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = bytes[*position];
            *position += 1;
            result |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        ((result >> 1) as i64) ^ (-((result & 1) as i64))
    }

    fn build_test_produce_response(
        correlation_id: i32,
        topic: &str,
        partition: i32,
        error_code: i16,
        base_offset: i64,
        log_append_time: i64,
    ) -> Vec<u8> {
        let mut response = Vec::new();
        response.extend_from_slice(&correlation_id.to_be_bytes());
        response.extend_from_slice(&1i32.to_be_bytes()); // topic count
        response.extend_from_slice(&(topic.len() as i16).to_be_bytes());
        response.extend_from_slice(topic.as_bytes());
        response.extend_from_slice(&1i32.to_be_bytes()); // partition count
        response.extend_from_slice(&partition.to_be_bytes());
        response.extend_from_slice(&error_code.to_be_bytes());
        response.extend_from_slice(&base_offset.to_be_bytes());
        response.extend_from_slice(&log_append_time.to_be_bytes());
        response.extend_from_slice(&0i64.to_be_bytes()); // log_start_offset
        response.extend_from_slice(&0i32.to_be_bytes()); // throttle_time_ms
        response
    }
}
