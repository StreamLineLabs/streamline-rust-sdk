//! Consumer for reading messages from Streamline.
//!
//! Fetches messages using the Kafka Fetch protocol over the connection pool's
//! TCP connections. Group coordination (join/sync/heartbeat) is planned for
//! a future release — the current implementation does direct partition fetches.

use crate::config::{ConsumerConfig, StreamlineConfig};
use crate::connection::ConnectionPool;
use crate::error::{Error, ErrorKind, Result};
use crate::telemetry;
use crate::Headers;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info};

fn lock_mutex<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex.lock().map_err(|_: PoisonError<MutexGuard<'_, T>>| {
        Error::new(
            ErrorKind::Internal,
            "Internal lock poisoned — a previous operation panicked",
        )
        .with_hint("This indicates a bug in the client. Please report it.")
    })
}

/// A single search result from a topic.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SearchResult {
    /// Partition number of the matching record.
    pub partition: i32,
    /// Record offset.
    pub offset: i64,
    /// Similarity score (higher = more relevant).
    pub score: f64,
    /// Record value, if returned by the server.
    pub value: Option<serde_json::Value>,
}

/// Internal response from the search API.
#[cfg(any(feature = "schema-registry", feature = "moonshot"))]
#[derive(Debug, serde::Deserialize)]
struct SearchResponse {
    #[serde(default)]
    hits: Vec<SearchResult>,
    #[serde(default)]
    #[allow(dead_code)]
    took_ms: u64,
}

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
    partitions: Vec<i32>,
    partition_offsets: Mutex<HashMap<i32, i64>>,
    unresolved_earliest: AsyncMutex<HashSet<i32>>,
    paused_partitions: Mutex<HashSet<i32>>,
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
        partitions: Vec<i32>,
    ) -> Self {
        let partition_offsets: HashMap<i32, i64> = partitions.iter().map(|&p| (p, 0)).collect();
        let unresolved_earliest = if config.auto_offset_reset == "earliest" {
            partitions.iter().copied().collect()
        } else {
            HashSet::new()
        };

        Self {
            client_config,
            pool,
            topic,
            config,
            subscribed: false,
            partitions,
            partition_offsets: Mutex::new(partition_offsets),
            unresolved_earliest: AsyncMutex::new(unresolved_earliest),
            paused_partitions: Mutex::new(HashSet::new()),
            correlation_id: AtomicI32::new(1000),
            _marker: std::marker::PhantomData,
        }
    }

    fn next_correlation_id(&self) -> i32 {
        self.correlation_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Deadline for a complete fetch exchange.
    ///
    /// A fetch legitimately blocks on the broker for up to the caller's poll
    /// timeout (`max_wait_ms`), so the exchange deadline is that wait plus the
    /// configured `request_timeout` allowance for broker processing and I/O.
    fn fetch_deadline(&self, poll_timeout: Duration) -> Duration {
        poll_timeout.saturating_add(self.client_config.request_timeout)
    }

    /// Subscribes to the topic.
    ///
    /// Direct partition assignment is required in version 0.4.0 because topic
    /// metadata discovery and consumer group coordination are not implemented.
    pub async fn subscribe(&mut self) -> Result<()> {
        if !self.subscribed {
            crate::validation::validate_topic_name(&self.topic)?;
            info!("Subscribing to topic {}", self.topic);

            if self.partitions.is_empty() {
                return Err(Error::unsupported("consumer partition discovery").with_hint(
                    "Assign partitions explicitly with ConsumerBuilder::partitions, for example .partitions(vec![0]).",
                ));
            }

            let mut offsets = lock_mutex(&self.partition_offsets)?;
            for &p in &self.partitions {
                offsets.entry(p).or_insert(0);
            }

            self.subscribed = true;
        }
        Ok(())
    }

    async fn resolve_pending_earliest_offsets(&self) -> Result<()> {
        let mut unresolved = self.unresolved_earliest.lock().await;
        if unresolved.is_empty() {
            return Ok(());
        }
        let partitions: Vec<i32> = unresolved.iter().copied().collect();
        let resolved = self
            .resolve_offsets_for_timestamp(&partitions, LIST_OFFSETS_EARLIEST_TIMESTAMP)
            .await?;
        {
            let mut offsets = lock_mutex(&self.partition_offsets)?;
            for (&partition, &offset) in &resolved {
                offsets.insert(partition, offset);
                unresolved.remove(&partition);
            }
        }
        Ok(())
    }

    async fn resolve_offsets_for_timestamp(
        &self,
        partitions: &[i32],
        timestamp: i64,
    ) -> Result<HashMap<i32, i64>> {
        let correlation_id = self.next_correlation_id();
        let request =
            build_list_offsets_request(correlation_id, &self.topic, partitions, timestamp);
        let conn_handle = self.pool.get().await?;
        let mut conn = conn_handle.lock().await;
        let response = conn
            .exchange(&request, self.client_config.request_timeout, "list offsets")
            .await?;
        parse_list_offsets_response(correlation_id, &self.topic, partitions, &response).inspect_err(
            |error| {
                if crate::connection::evictable(error) {
                    conn.evict(&format!("list offsets response error: {error}"));
                }
            },
        )
    }

    /// Polls for new records using the Kafka Fetch protocol.
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<ConsumerRecord<K, V>>>
    where
        K: From<Vec<u8>>,
        V: From<Vec<u8>>,
    {
        telemetry::trace_consume(&self.topic, || self.poll_inner(timeout)).await
    }

    async fn poll_inner(&self, timeout: Duration) -> Result<Vec<ConsumerRecord<K, V>>>
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

        self.resolve_pending_earliest_offsets().await?;

        // Collect active (non-paused) partitions with their current offsets
        let active_partitions: Vec<(i32, i64)> = {
            let paused = lock_mutex(&self.paused_partitions)?;
            let offsets = lock_mutex(&self.partition_offsets)?;
            self.partitions
                .iter()
                .filter(|p| !paused.contains(p))
                .map(|&p| (p, offsets.get(&p).copied().unwrap_or(0)))
                .collect()
        };

        if active_partitions.is_empty() {
            return Ok(Vec::new());
        }

        let correlation_id = self.next_correlation_id();

        // Clamp the caller's long-poll wait into the INT32 the wire format
        // uses, so an oversized Duration cannot become a negative max_wait_ms.
        let max_wait_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);

        // Build Kafka Fetch request (API key 1, version 11) for all active partitions
        let request =
            build_fetch_request(correlation_id, &self.topic, &active_partitions, max_wait_ms);

        let conn_handle = self.pool.get().await?;
        let mut conn = conn_handle.lock().await;

        // The complete broker exchange (connect, write, read) is bounded by
        // the configured request timeout — including the broker-side
        // `max_wait_ms` this poll asked for — and a failed exchange evicts the
        // pooled connection instead of leaving a desynchronized socket behind.
        let resp_buf = conn
            .exchange(&request, self.fetch_deadline(timeout), "fetch")
            .await?;

        // Parse records from the response, verifying the correlation ID and
        // rejecting responses that report a broker-side error rather than
        // silently treating them as "no messages".
        let outcome = parse_fetch_response::<K, V>(
            correlation_id,
            &self.topic,
            &active_partitions,
            &resp_buf,
        )
        .inspect_err(|error| {
            if crate::connection::evictable(error) {
                conn.evict(&format!("fetch response error: {error}"));
            }
        })?;
        drop(conn);

        // Advance per-partition fetch offsets. The parser reports the offset
        // after the last batch it consumed, so a poll still makes progress
        // when every record in a batch was filtered out (a redelivered or
        // control batch).
        {
            let mut offsets = lock_mutex(&self.partition_offsets)?;
            for (&partition, &next_offset) in &outcome.next_offsets {
                let entry = offsets.entry(partition).or_insert(0);
                if next_offset > *entry {
                    *entry = next_offset;
                }
            }
            for record in &outcome.records {
                let entry = offsets.entry(record.partition).or_insert(0);
                let next = record.offset.saturating_add(1);
                if next > *entry {
                    *entry = next;
                }
            }
        }

        Ok(outcome.records)
    }

    /// Returns an explicit unsupported error.
    ///
    /// Offset commits require consumer group coordination, which is not
    /// implemented in version 0.4.0.
    pub async fn commit(&self) -> Result<()> {
        Err(Error::unsupported("consumer offset commits"))
    }

    /// Returns an explicit unsupported error.
    pub fn commit_async(&self) -> Result<()> {
        Err(Error::unsupported("asynchronous consumer offset commits"))
    }

    /// Seeks to the beginning of all partitions.
    pub async fn seek_to_beginning(&self) -> Result<()> {
        {
            let mut unresolved = self.unresolved_earliest.lock().await;
            unresolved.extend(self.partitions.iter().copied());
        }
        let mut offsets = lock_mutex(&self.partition_offsets)?;
        for offset in offsets.values_mut() {
            *offset = 0;
        }
        debug!("Seeking to beginning for all partitions");
        Ok(())
    }

    /// Seeks to the end of all partitions.
    pub async fn seek_to_end(&self) -> Result<()> {
        Err(Error::unsupported("consumer latest-offset resolution"))
    }

    /// Seeks to a specific offset for a partition.
    pub async fn seek(&self, partition: i32, offset: i64) -> Result<()> {
        if !self.partitions.contains(&partition) {
            return Err(Error::partition_not_found(&self.topic, partition));
        }
        if offset < 0 {
            return Err(Error::new(
                ErrorKind::InvalidConfiguration,
                format!("Consumer offset must be non-negative, got {offset}"),
            )
            .with_hint("Provide a non-negative absolute offset"));
        }

        let mut unresolved = self.unresolved_earliest.lock().await;
        let mut offsets = lock_mutex(&self.partition_offsets)?;
        offsets.insert(partition, offset);
        unresolved.remove(&partition);
        debug!("Seeking partition {} to offset {}", partition, offset);
        Ok(())
    }

    /// Returns the current position for a partition.
    pub async fn position(&self, partition: i32) -> Result<i64> {
        let offsets = lock_mutex(&self.partition_offsets)?;
        Ok(offsets.get(&partition).copied().unwrap_or(0))
    }

    /// Pauses consumption for the specified partitions.
    /// Paused partitions are skipped during `poll()`.
    pub fn pause(&self, partitions: &[i32]) -> Result<()> {
        let mut paused = lock_mutex(&self.paused_partitions)?;
        for &p in partitions {
            paused.insert(p);
        }
        debug!("Paused partitions: {:?}", *paused);
        Ok(())
    }

    /// Resumes consumption for the specified partitions.
    pub fn resume(&self, partitions: &[i32]) -> Result<()> {
        let mut paused = lock_mutex(&self.paused_partitions)?;
        for &p in partitions {
            paused.remove(&p);
        }
        debug!("Resumed partitions, still paused: {:?}", *paused);
        Ok(())
    }

    /// Returns the set of currently paused partitions.
    pub fn paused(&self) -> Result<HashSet<i32>> {
        Ok(lock_mutex(&self.paused_partitions)?.clone())
    }

    /// Returns the list of assigned partitions.
    pub fn assigned_partitions(&self) -> &[i32] {
        &self.partitions
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

    /// Performs a semantic search against a topic via the HTTP API.
    ///
    /// Sends a `POST /api/v1/topics/{topic}/search` request to the Streamline
    /// HTTP admin port (default 9094). Requires the `schema-registry` or
    /// `moonshot` feature to be enabled (both pull in `reqwest`).
    ///
    /// # Arguments
    /// * `topic` – Topic to search.
    /// * `query` – Free-text search query.
    /// * `k` – Maximum number of results.
    ///
    /// # Errors
    /// Returns an error if the HTTP request fails or the server returns a
    /// non-200 status.
    #[cfg(any(feature = "schema-registry", feature = "moonshot"))]
    pub async fn search(&self, topic: &str, query: &str, k: usize) -> Result<Vec<SearchResult>> {
        crate::validation::validate_topic_name(topic)?;
        let base_url = self.client_config.http_base_url();
        let url =
            crate::http_url::build_url(&base_url, &["api", "v1", "topics", topic, "search"], &[])?;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| Error::new(ErrorKind::Connection, format!("HTTP client error: {}", e)))?;

        let body = serde_json::json!({
            "query": query,
            "k": k,
        });

        let resp = client.post(url).json(&body).send().await.map_err(|e| {
            Error::new(
                ErrorKind::Connection,
                format!("Search request failed: {}", e),
            )
        })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::new(
                ErrorKind::Server,
                format!("Search failed (HTTP {}): {}", status, text),
            ));
        }

        let data: SearchResponse = resp.json().await.map_err(|e| {
            Error::new(
                ErrorKind::Serialization,
                format!("JSON decode failed: {}", e),
            )
        })?;

        Ok(data.hits)
    }
}

impl<K, V> Drop for Consumer<K, V> {
    fn drop(&mut self) {
        if self.subscribed {
            info!("Closing consumer for topic {}", self.topic);
        }
    }
}

/// Client identifier sent in the Kafka request header.
const CLIENT_ID: &[u8] = b"streamline-rust-sdk";

/// Maximum bytes requested per fetch response and per partition.
const FETCH_MAX_BYTES: i32 = 1024 * 1024;

/// Kafka ListOffsets timestamp selecting each partition's retained log start.
const LIST_OFFSETS_EARLIEST_TIMESTAMP: i64 = -2;

/// Build a Kafka ListOffsets request (API key 2, version 5).
fn build_list_offsets_request(
    correlation_id: i32,
    topic: &str,
    partitions: &[i32],
    timestamp: i64,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(96);
    buf.extend_from_slice(&[0u8; 4]);
    buf.extend_from_slice(&2i16.to_be_bytes());
    buf.extend_from_slice(&5i16.to_be_bytes());
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    buf.extend_from_slice(&(CLIENT_ID.len() as i16).to_be_bytes());
    buf.extend_from_slice(CLIENT_ID);

    buf.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
    buf.push(0u8); // isolation_level: READ_UNCOMMITTED
    buf.extend_from_slice(&1i32.to_be_bytes()); // topics count
    buf.extend_from_slice(&(topic.len() as i16).to_be_bytes());
    buf.extend_from_slice(topic.as_bytes());
    buf.extend_from_slice(&(partitions.len() as i32).to_be_bytes());
    for &partition in partitions {
        buf.extend_from_slice(&partition.to_be_bytes());
        buf.extend_from_slice(&(-1i32).to_be_bytes()); // current_leader_epoch
        buf.extend_from_slice(&timestamp.to_be_bytes());
    }

    let total_len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&total_len.to_be_bytes());
    buf
}

/// Build a Kafka Fetch request (API key 1, version 11).
///
/// Wire layout (all integers big-endian), per the Kafka protocol spec for
/// `Fetch` version 11:
///
/// ```text
/// Header: api_key:INT16 api_version:INT16 correlation_id:INT32 client_id:STRING
/// Body:   replica_id:INT32 max_wait_ms:INT32 min_bytes:INT32 max_bytes:INT32
///         isolation_level:INT8 session_id:INT32 session_epoch:INT32
///         topics:[ topic:STRING partitions:[
///             partition:INT32 current_leader_epoch:INT32 fetch_offset:INT64
///             log_start_offset:INT64 partition_max_bytes:INT32 ] ]
///         forgotten_topics_data:[ topic:STRING partitions:[INT32] ]
///         rack_id:STRING
/// ```
///
/// Two field encodings are easy to get wrong and are asserted by
/// `test_build_fetch_request_golden_bytes`:
///
/// * `current_leader_epoch` is an **INT32** (`-1` meaning "unknown"), not an
///   INT64. Encoding it as eight bytes shifts every following field.
/// * `rack_id` is a **non-nullable STRING** in v11, so "no rack" is the empty
///   string (`0x00 0x00`), not the null marker (`0xFF 0xFF`).
fn build_fetch_request(
    correlation_id: i32,
    topic: &str,
    partitions: &[(i32, i64)],
    max_wait_ms: i32,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);

    // Length placeholder
    buf.extend_from_slice(&[0u8; 4]);

    // Request header: api_key(i16) + api_version(i16) + correlation_id(i32) + client_id
    buf.extend_from_slice(&1i16.to_be_bytes()); // Fetch = 1
    buf.extend_from_slice(&11i16.to_be_bytes()); // version 11
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    buf.extend_from_slice(&(CLIENT_ID.len() as i16).to_be_bytes());
    buf.extend_from_slice(CLIENT_ID);

    // Fetch request body (v11)
    buf.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id: -1 (consumer)
    buf.extend_from_slice(&max_wait_ms.to_be_bytes());
    buf.extend_from_slice(&1i32.to_be_bytes()); // min_bytes: 1
    buf.extend_from_slice(&FETCH_MAX_BYTES.to_be_bytes()); // max_bytes
    buf.push(0u8); // isolation_level: READ_UNCOMMITTED
    buf.extend_from_slice(&0i32.to_be_bytes()); // session_id
    buf.extend_from_slice(&(-1i32).to_be_bytes()); // session_epoch

    // topics array: count + [topic_name + partitions]
    buf.extend_from_slice(&1i32.to_be_bytes()); // 1 topic
    buf.extend_from_slice(&(topic.len() as i16).to_be_bytes());
    buf.extend_from_slice(topic.as_bytes());

    // partitions array
    buf.extend_from_slice(&(partitions.len() as i32).to_be_bytes());
    for &(partition, fetch_offset) in partitions {
        buf.extend_from_slice(&partition.to_be_bytes());
        buf.extend_from_slice(&(-1i32).to_be_bytes()); // current_leader_epoch: INT32
        buf.extend_from_slice(&fetch_offset.to_be_bytes());
        buf.extend_from_slice(&(-1i64).to_be_bytes()); // log_start_offset
        buf.extend_from_slice(&FETCH_MAX_BYTES.to_be_bytes()); // partition_max_bytes
    }

    // forgotten_topics_data: empty array
    buf.extend_from_slice(&0i32.to_be_bytes());
    // rack_id: empty, non-null STRING
    buf.extend_from_slice(&0i16.to_be_bytes());

    // Fill length
    let total_len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&total_len.to_be_bytes());

    buf
}

/// Minimum number of bytes in a v2 record batch after `batch_length`:
/// partition_leader_epoch(4) + magic(1) + crc(4) + attributes(2)
/// + last_offset_delta(4) + first_timestamp(8) + max_timestamp(8)
/// + producer_id(8) + producer_epoch(2) + base_sequence(4) + record_count(4).
const RECORD_BATCH_MIN_BODY_LEN: usize = 49;

/// Byte offset, within a record batch body, of the first CRC-covered byte
/// (`attributes`): partition_leader_epoch(4) + magic(1) + crc(4).
const RECORD_BATCH_CRC_COVERED_START: usize = 9;

/// Bit mask selecting the compression codec from record batch attributes.
const RECORD_BATCH_COMPRESSION_MASK: i16 = 0x07;

/// Attributes bit marking a control batch (transaction commit/abort markers).
/// Control batches are internal bookkeeping and must never be surfaced as
/// application records.
const RECORD_BATCH_CONTROL_MASK: i16 = 0x20;

/// Records decoded from a fetch response, together with the next offset to
/// request per partition.
///
/// The next offset is derived from each batch's `base_offset +
/// last_offset_delta + 1`, not only from the records handed to the caller, so
/// a poll still makes progress across batches whose records are all filtered
/// out (already-consumed records redelivered because fetches are batch
/// aligned, or skipped control batches).
#[derive(Debug)]
struct FetchOutcome<K, V> {
    records: Vec<ConsumerRecord<K, V>>,
    next_offsets: HashMap<i32, i64>,
}

impl<K, V> FetchOutcome<K, V> {
    fn new() -> Self {
        Self {
            records: Vec::new(),
            next_offsets: HashMap::new(),
        }
    }

    fn advance(&mut self, partition: i32, next_offset: i64) {
        let entry = self.next_offsets.entry(partition).or_insert(next_offset);
        if next_offset > *entry {
            *entry = next_offset;
        }
    }
}

/// A strictly bounds-checked reader over a Kafka Fetch response.
///
/// Every read is fallible: a short buffer, a negative length, a non-UTF-8
/// string, or an over-long varint produces [`ErrorKind::Protocol`] rather
/// than a silently zeroed value. This mirrors `producer::ResponseCursor`.
struct FetchCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> FetchCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn take(&mut self, length: usize, field: &str) -> Result<&'a [u8]> {
        let end = self.position.checked_add(length).ok_or_else(|| {
            Error::new(
                ErrorKind::Protocol,
                format!("Fetch response position overflow while reading {field}"),
            )
        })?;
        if end > self.bytes.len() {
            return Err(Error::new(
                ErrorKind::Protocol,
                format!(
                    "Truncated fetch response while reading {field}: needed {length} byte(s), {} remaining",
                    self.remaining()
                ),
            ));
        }
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }

    fn read_i8(&mut self, field: &str) -> Result<i8> {
        Ok(self.take(1, field)?[0] as i8)
    }

    fn read_i16(&mut self, field: &str) -> Result<i16> {
        let bytes = self.take(2, field)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_i32(&mut self, field: &str) -> Result<i32> {
        let bytes = self.take(4, field)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u32(&mut self, field: &str) -> Result<u32> {
        let bytes = self.take(4, field)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i64(&mut self, field: &str) -> Result<i64> {
        let bytes = self.take(8, field)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    /// Reads a non-nullable Kafka STRING (INT16 length + UTF-8 bytes).
    fn read_string(&mut self, field: &str) -> Result<&'a str> {
        let length = self.read_i16(field)?;
        if length < 0 {
            return Err(Error::new(
                ErrorKind::Protocol,
                format!("Fetch response {field} cannot be null"),
            ));
        }
        let bytes = self.take(length as usize, field)?;
        std::str::from_utf8(bytes).map_err(|error| {
            Error::new(
                ErrorKind::Protocol,
                format!("Fetch response {field} is not valid UTF-8"),
            )
            .with_source(error)
        })
    }

    /// Reads a non-null Kafka array length. The nullable-array marker (`-1`)
    /// is invalid for fields whose schema does not permit null.
    fn read_array_len(&mut self, field: &str) -> Result<usize> {
        let count = self.read_i32(field)?;
        if count < 0 {
            return Err(Error::new(
                ErrorKind::Protocol,
                format!("Fetch response {field} had invalid count {count}"),
            ));
        }
        Ok(count as usize)
    }

    /// Reads a nullable Kafka array length. `-1` is the only null marker;
    /// other negative values are malformed.
    fn read_nullable_array_len(&mut self, field: &str) -> Result<Option<usize>> {
        let count = self.read_i32(field)?;
        match count {
            -1 => Ok(None),
            count if count < -1 => Err(Error::new(
                ErrorKind::Protocol,
                format!("Fetch response {field} had invalid count {count}"),
            )),
            count => Ok(Some(count as usize)),
        }
    }

    /// Decodes a zigzag-encoded signed varint.
    fn read_varint(&mut self, field: &str) -> Result<i64> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            let byte = self.take(1, field)?[0] as u64;
            if shift == 63 && byte & 0x7E != 0 {
                return Err(varint_overflow(field));
            }
            result |= (byte & 0x7F)
                .checked_shl(shift)
                .ok_or_else(|| varint_overflow(field))?;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift > 63 {
                return Err(varint_overflow(field));
            }
        }
        Ok(((result >> 1) as i64) ^ (-((result & 1) as i64)))
    }

    /// Reads a varint-prefixed byte block. `-1` denotes a null block; any
    /// other negative length is rejected.
    fn read_varint_bytes(&mut self, field: &str) -> Result<Option<&'a [u8]>> {
        let length = self.read_varint(field)?;
        match length {
            -1 => Ok(None),
            length if length < 0 => Err(Error::new(
                ErrorKind::Protocol,
                format!("Fetch response {field} had invalid length {length}"),
            )),
            length => {
                let length = usize::try_from(length).map_err(|_| {
                    Error::new(
                        ErrorKind::Protocol,
                        format!(
                            "Fetch response {field} length {length} exceeds addressable memory"
                        ),
                    )
                })?;
                Ok(Some(self.take(length, field)?))
            }
        }
    }
}

fn varint_overflow(field: &str) -> Error {
    Error::new(
        ErrorKind::Protocol,
        format!("Fetch response {field} contained an over-long varint"),
    )
}

fn trailing_bytes_error(context: &str, count: usize) -> Error {
    Error::new(
        ErrorKind::Protocol,
        format!("{context} contained {count} unexpected trailing byte(s)"),
    )
    .with_hint("The broker response did not match the Kafka Fetch v11 wire format")
}

fn parse_list_offsets_response(
    expected_correlation_id: i32,
    topic: &str,
    requested_partitions: &[i32],
    response: &[u8],
) -> Result<HashMap<i32, i64>> {
    let mut cursor = FetchCursor::new(response);
    let correlation_id = cursor.read_i32("ListOffsets correlation ID")?;
    if correlation_id != expected_correlation_id {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!(
                "ListOffsets response correlation ID {correlation_id} did not match request {expected_correlation_id}"
            ),
        ));
    }
    let _throttle_time_ms = cursor.read_i32("ListOffsets throttle time")?;
    let topic_count = cursor.read_array_len("ListOffsets topic count")?;
    if topic_count != 1 {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!("ListOffsets returned {topic_count} topics; expected exactly one"),
        ));
    }
    let response_topic = cursor.read_string("ListOffsets topic name")?;
    if response_topic != topic {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!("ListOffsets returned topic '{response_topic}' but '{topic}' was requested"),
        ));
    }

    let partition_count = cursor.read_array_len("ListOffsets partition count")?;
    let requested: HashSet<i32> = requested_partitions.iter().copied().collect();
    let mut offsets = HashMap::with_capacity(partition_count);
    for _ in 0..partition_count {
        let partition = cursor.read_i32("ListOffsets partition index")?;
        if !requested.contains(&partition) || offsets.contains_key(&partition) {
            return Err(Error::new(
                ErrorKind::Protocol,
                format!("ListOffsets returned unexpected or duplicate partition {partition}"),
            ));
        }
        let error_code = cursor.read_i16("ListOffsets partition error code")?;
        if error_code != 0 {
            return Err(kafka_list_offsets_error(error_code, topic, partition));
        }
        let _timestamp = cursor.read_i64("ListOffsets timestamp")?;
        let offset = cursor.read_i64("ListOffsets offset")?;
        let _leader_epoch = cursor.read_i32("ListOffsets leader epoch")?;
        if offset < 0 {
            return Err(Error::new(
                ErrorKind::Protocol,
                format!(
                    "ListOffsets returned invalid negative offset {offset} for {topic}:{partition}"
                ),
            ));
        }
        offsets.insert(partition, offset);
    }
    if offsets.len() != requested.len() {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!(
                "ListOffsets returned {} of {} requested partitions",
                offsets.len(),
                requested.len()
            ),
        ));
    }
    if !cursor.is_empty() {
        return Err(trailing_bytes_error(
            "ListOffsets response",
            cursor.remaining(),
        ));
    }
    Ok(offsets)
}

fn kafka_list_offsets_error(error_code: i16, topic: &str, partition: i32) -> Error {
    let kind = match error_code {
        1 => ErrorKind::OffsetOutOfRange,
        3 => ErrorKind::TopicNotFound,
        7 => ErrorKind::Timeout,
        29 | 31 => ErrorKind::AuthorizationFailed,
        5 | 6 | 19 | 20 | 56 => ErrorKind::Server,
        _ => ErrorKind::Protocol,
    };
    Error::new(
        kind,
        format!(
            "Broker rejected ListOffsets for topic '{topic}' partition {partition} with Kafka error code {error_code}"
        ),
    )
}

/// Parse records from a Kafka Fetch response (v11).
///
/// Response layout after `correlation_id`:
///
/// ```text
/// throttle_time_ms:INT32 error_code:INT16 session_id:INT32
/// responses:[ topic:STRING partitions:[
///     partition_index:INT32 error_code:INT16 high_watermark:INT64
///     last_stable_offset:INT64 log_start_offset:INT64
///     aborted_transactions:[ producer_id:INT64 first_offset:INT64 ]
///     preferred_read_replica:INT32 records:BYTES ] ]
/// ```
///
/// Parsing is fail-closed throughout:
///
/// * The correlation ID, the response-wide error code, and every
///   per-partition error code are validated. A per-partition error is
///   returned as an [`Error`] — it is never reported as "no messages",
///   because that would silently mask leader changes, offset-out-of-range,
///   and authorization failures while the consumer keeps polling.
/// * Every length, count, and offset is bounds-checked; nothing is inferred
///   from a truncated or oversized field.
/// * Record batches must be complete and their CRC-32C must match.
/// * Compressed record batches are rejected with [`ErrorKind::Unsupported`]
///   until decompression is implemented, rather than mis-parsing the
///   compressed payload as records.
/// * Trailing bytes at any level (response, record set, batch, record) are
///   rejected.
///
/// Fetches are record-batch aligned, so a broker legitimately returns the
/// whole batch containing the requested offset. Records below the offset that
/// was actually requested for their partition are dropped, and a partition
/// the request did not ask for is rejected.
///
/// Cross-repository contract: the Streamline broker must not emit partially
/// truncated trailing record batches in a fetch response. Classic Kafka
/// brokers may truncate the last batch once `max_bytes` is reached; this
/// parser rejects that rather than guessing, so the server must bound
/// responses on record batch boundaries.
fn parse_fetch_response<K: From<Vec<u8>>, V: From<Vec<u8>>>(
    expected_correlation_id: i32,
    topic: &str,
    requested: &[(i32, i64)],
    resp: &[u8],
) -> Result<FetchOutcome<K, V>> {
    let mut outcome = FetchOutcome::new();
    let mut cursor = FetchCursor::new(resp);

    let correlation_id = cursor.read_i32("correlation ID")?;
    if correlation_id != expected_correlation_id {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!(
                "Fetch response correlation ID {correlation_id} did not match request {expected_correlation_id}"
            ),
        ));
    }

    let _throttle_time_ms = cursor.read_i32("throttle time")?;
    let response_error_code = cursor.read_i16("response error code")?;
    if response_error_code != 0 {
        return Err(kafka_fetch_error(response_error_code, topic, None));
    }
    let _session_id = cursor.read_i32("session ID")?;

    let responses_count = cursor.read_array_len("response count")?;
    for _ in 0..responses_count {
        let topic_name = cursor.read_string("topic name")?;
        if topic_name != topic {
            return Err(Error::new(
                ErrorKind::Protocol,
                format!("Fetch response returned topic '{topic_name}' but '{topic}' was requested"),
            ));
        }

        let partitions_count = cursor.read_array_len("partition count")?;
        for _ in 0..partitions_count {
            let partition = cursor.read_i32("partition index")?;
            let error_code = cursor.read_i16("partition error code")?;
            if error_code != 0 {
                return Err(kafka_fetch_error(error_code, topic, Some(partition)));
            }

            let Some(&(_, fetch_offset)) = requested.iter().find(|(index, _)| *index == partition)
            else {
                return Err(Error::new(
                    ErrorKind::Protocol,
                    format!(
                        "Fetch response returned partition {partition} of '{topic}', which was not requested"
                    ),
                ));
            };

            let _high_watermark = cursor.read_i64("high watermark")?;
            let _last_stable_offset = cursor.read_i64("last stable offset")?;
            let _log_start_offset = cursor.read_i64("log start offset")?;

            if let Some(aborted_count) =
                cursor.read_nullable_array_len("aborted transaction count")?
            {
                for _ in 0..aborted_count {
                    let _producer_id = cursor.read_i64("aborted transaction producer ID")?;
                    let _first_offset = cursor.read_i64("aborted transaction first offset")?;
                }
            }

            let _preferred_read_replica = cursor.read_i32("preferred read replica")?;

            let record_set_len = cursor.read_i32("record set size")?;
            let record_set = match record_set_len {
                -1 => &[][..],
                len if len < 0 => {
                    return Err(Error::new(
                        ErrorKind::Protocol,
                        format!("Fetch response record set size {len} is invalid"),
                    ))
                }
                len => cursor.take(len as usize, "record set")?,
            };

            outcome.advance(partition, fetch_offset);
            parse_record_set(record_set, topic, partition, fetch_offset, &mut outcome)?;
        }
    }

    if !cursor.is_empty() {
        return Err(trailing_bytes_error("Fetch response", cursor.remaining()));
    }

    Ok(outcome)
}

/// Parses every record batch inside a single partition's record set.
fn parse_record_set<K: From<Vec<u8>>, V: From<Vec<u8>>>(
    record_set: &[u8],
    topic: &str,
    partition: i32,
    fetch_offset: i64,
    outcome: &mut FetchOutcome<K, V>,
) -> Result<()> {
    let mut cursor = FetchCursor::new(record_set);

    while !cursor.is_empty() {
        let base_offset = cursor.read_i64("record batch base offset")?;
        let batch_length = cursor.read_i32("record batch length")?;
        let batch_length = usize::try_from(batch_length).map_err(|_| {
            Error::new(
                ErrorKind::Protocol,
                format!("Record batch length {batch_length} is invalid"),
            )
        })?;
        if batch_length < RECORD_BATCH_MIN_BODY_LEN {
            return Err(Error::new(
                ErrorKind::Protocol,
                format!(
                    "Record batch length {batch_length} is shorter than the {RECORD_BATCH_MIN_BODY_LEN}-byte v2 batch header"
                ),
            ));
        }

        let body = cursor.take(batch_length, "record batch body")?;
        parse_record_batch(body, base_offset, topic, partition, fetch_offset, outcome)?;
    }

    Ok(())
}

/// Parses one v2 record batch body (everything after `batch_length`).
fn parse_record_batch<K: From<Vec<u8>>, V: From<Vec<u8>>>(
    body: &[u8],
    base_offset: i64,
    topic: &str,
    partition: i32,
    fetch_offset: i64,
    outcome: &mut FetchOutcome<K, V>,
) -> Result<()> {
    let mut cursor = FetchCursor::new(body);

    let _partition_leader_epoch = cursor.read_i32("partition leader epoch")?;
    let magic = cursor.read_i8("record batch magic")?;
    if magic != 2 {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!("Unsupported record batch magic {magic}; only v2 (magic 2) is supported"),
        )
        .with_hint("Configure the broker to store records in the v2 record batch format"));
    }

    let expected_crc = cursor.read_u32("record batch CRC")?;
    let covered = body.get(RECORD_BATCH_CRC_COVERED_START..).ok_or_else(|| {
        Error::new(
            ErrorKind::Protocol,
            "Record batch is too short to contain CRC-covered data",
        )
    })?;
    let actual_crc = crate::producer::crc32c_compute(covered);
    if actual_crc != expected_crc {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!(
                "Record batch CRC mismatch for {topic}:{partition}: broker sent {expected_crc:#010x}, computed {actual_crc:#010x}"
            ),
        )
        .with_hint("The record batch was corrupted in transit or on disk"));
    }

    let attributes = cursor.read_i16("record batch attributes")?;
    let is_control_batch = attributes & RECORD_BATCH_CONTROL_MASK != 0;
    let compression = attributes & RECORD_BATCH_COMPRESSION_MASK;
    if compression != 0 {
        return Err(Error::unsupported(&format!(
            "consuming compressed record batches (codec {compression})"
        ))
        .with_hint("Produce uncompressed records, or wait for consumer decompression support"));
    }

    let last_offset_delta = cursor.read_i32("last offset delta")?;
    if last_offset_delta < 0 {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!("Record batch declared negative last offset delta {last_offset_delta}"),
        ));
    }
    // The batch as a whole is consumed once parsed, even when every record in
    // it is filtered out, so the next fetch must start after it.
    let batch_next_offset = base_offset
        .checked_add(i64::from(last_offset_delta))
        .and_then(|last| last.checked_add(1))
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Protocol,
                format!("Record batch end offset overflowed for {topic}:{partition}"),
            )
        })?;
    outcome.advance(partition, batch_next_offset);

    let first_timestamp = cursor.read_i64("first timestamp")?;
    let _max_timestamp = cursor.read_i64("max timestamp")?;
    let _producer_id = cursor.read_i64("producer ID")?;
    let _producer_epoch = cursor.read_i16("producer epoch")?;
    let _base_sequence = cursor.read_i32("base sequence")?;

    let record_count = cursor.read_i32("record count")?;
    if record_count < 0 {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!("Record batch declared invalid record count {record_count}"),
        ));
    }
    let record_count = record_count as usize;
    // Every record occupies at least one byte, so a count larger than the
    // remaining batch bytes is malformed and must not drive a long loop.
    if record_count > cursor.remaining() {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!(
                "Record batch declared {record_count} record(s) but only {} byte(s) remain",
                cursor.remaining()
            ),
        ));
    }

    for _ in 0..record_count {
        let record_bytes = cursor
            .read_varint_bytes("record length")?
            .ok_or_else(|| Error::new(ErrorKind::Protocol, "Record length cannot be null"))?;
        let record = parse_record(
            record_bytes,
            base_offset,
            first_timestamp,
            last_offset_delta,
            topic,
            partition,
        )?;

        // Control batches carry transaction markers, not application data:
        // they are still parsed (so a malformed one is rejected rather than
        // skipped) but never surfaced to the caller. Records below the
        // requested offset are redeliveries caused by batch-aligned fetches.
        if is_control_batch || record.offset < fetch_offset {
            continue;
        }
        outcome.records.push(record);
    }

    if !cursor.is_empty() {
        return Err(trailing_bytes_error("Record batch", cursor.remaining()));
    }

    Ok(())
}

/// Parses a single record from its length-delimited body.
fn parse_record<K: From<Vec<u8>>, V: From<Vec<u8>>>(
    record_bytes: &[u8],
    base_offset: i64,
    first_timestamp: i64,
    last_offset_delta: i32,
    topic: &str,
    partition: i32,
) -> Result<ConsumerRecord<K, V>> {
    let mut cursor = FetchCursor::new(record_bytes);

    let _attributes = cursor.read_i8("record attributes")?;
    let timestamp_delta = cursor.read_varint("record timestamp delta")?;
    let offset_delta = cursor.read_varint("record offset delta")?;
    if offset_delta < 0 || offset_delta > i64::from(last_offset_delta) {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!(
                "Record offset delta {offset_delta} is outside the batch range 0..={last_offset_delta}"
            ),
        ));
    }
    let offset = base_offset.checked_add(offset_delta).ok_or_else(|| {
        Error::new(
            ErrorKind::Protocol,
            format!("Record offset overflowed for {topic}:{partition}"),
        )
    })?;
    if offset < 0 {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!("Record offset {offset} is negative for {topic}:{partition}"),
        ));
    }
    let timestamp = first_timestamp
        .checked_add(timestamp_delta)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Protocol,
                format!("Record timestamp overflowed for {topic}:{partition}"),
            )
        })?;

    let key = cursor
        .read_varint_bytes("record key")?
        .map(|bytes| K::from(bytes.to_vec()));
    let value = cursor
        .read_varint_bytes("record value")?
        .map(<[u8]>::to_vec)
        .unwrap_or_default();

    let header_count = cursor.read_varint("record header count")?;
    if header_count < 0 {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!("Record declared invalid header count {header_count}"),
        ));
    }
    let header_count = usize::try_from(header_count).map_err(|_| {
        Error::new(
            ErrorKind::Protocol,
            format!("Record header count {header_count} exceeds addressable memory"),
        )
    })?;
    if header_count > cursor.remaining() {
        return Err(Error::new(
            ErrorKind::Protocol,
            format!(
                "Record declared {header_count} header(s) but only {} byte(s) remain",
                cursor.remaining()
            ),
        ));
    }

    let mut headers = Headers::new();
    for _ in 0..header_count {
        let key_bytes = cursor
            .read_varint_bytes("record header key")?
            .ok_or_else(|| Error::new(ErrorKind::Protocol, "Record header key cannot be null"))?;
        let header_key = std::str::from_utf8(key_bytes).map_err(|error| {
            Error::new(ErrorKind::Protocol, "Record header key is not valid UTF-8")
                .with_source(error)
        })?;
        let header_value = cursor
            .read_varint_bytes("record header value")?
            .map(<[u8]>::to_vec)
            .unwrap_or_default();
        headers.add(header_key, header_value);
    }

    if !cursor.is_empty() {
        return Err(trailing_bytes_error("Record", cursor.remaining()));
    }

    Ok(ConsumerRecord {
        topic: topic.to_string(),
        partition,
        offset,
        timestamp,
        key,
        value: V::from(value),
        headers,
    })
}

/// Maps a Kafka Fetch error code to an [`Error`], mirroring
/// `producer::kafka_produce_error`. `partition` is `None` for response-wide
/// errors and `Some(index)` for per-partition errors.
fn kafka_fetch_error(error_code: i16, topic: &str, partition: Option<i32>) -> Error {
    let kind = match error_code {
        1 => ErrorKind::OffsetOutOfRange,
        3 => ErrorKind::TopicNotFound,
        7 => ErrorKind::Timeout,
        29 | 31 => ErrorKind::AuthorizationFailed,
        5 | 6 | 19 | 20 | 56 => ErrorKind::Server,
        _ => ErrorKind::Protocol,
    };

    let scope = match partition {
        Some(partition) => format!("topic '{topic}' partition {partition}"),
        None => format!("topic '{topic}'"),
    };

    Error::new(
        kind,
        format!("Broker rejected fetch for {scope} with Kafka error code {error_code}"),
    )
    .with_hint("Check the broker error code and server logs before retrying the fetch")
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
            vec![0],
        )
    }

    fn make_multi_partition_consumer() -> Consumer<String, String> {
        let client_config = Arc::new(StreamlineConfig::default());
        let pool = Arc::new(ConnectionPool::new(&client_config));
        Consumer::new(
            client_config,
            pool,
            "test-topic".to_string(),
            ConsumerConfig::default(),
            vec![0, 1, 2],
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
        assert!(!consumer.config().enable_auto_commit);
        assert_eq!(consumer.config().max_poll_records, 500);
    }

    #[tokio::test]
    async fn test_poll_applies_request_timeout_and_evicts_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // Accept the fetch but never answer it.
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
        let mut consumer: Consumer<Vec<u8>, Vec<u8>> = Consumer::new(
            client_config,
            pool.clone(),
            "events".to_string(),
            ConsumerConfig::default(),
            vec![0],
        );
        consumer.unresolved_earliest.lock().await.clear();
        consumer.subscribe().await.unwrap();

        let error = tokio::time::timeout(
            Duration::from_secs(5),
            consumer.poll(Duration::from_millis(10)),
        )
        .await
        .expect("request timeout must bound the fetch exchange")
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Timeout);
        assert!(
            !pool.is_healthy().await,
            "a timed-out connection must be evicted from the pool"
        );
        server.abort();
    }

    #[tokio::test]
    async fn test_earliest_offset_resolution_uses_list_offsets_log_start() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut stream, _) = listener.accept().await.unwrap();
            let request_length = stream.read_i32().await.unwrap();
            let mut request = vec![0; request_length as usize];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 2);

            let response = build_test_list_offsets_response(1000, "events", &[(0, 0, 41)]);
            stream.write_i32(response.len() as i32).await.unwrap();
            stream.write_all(&response).await.unwrap();
        });

        let client_config = Arc::new(StreamlineConfig {
            bootstrap_servers: address.to_string(),
            connection_pool_size: 1,
            request_timeout: Duration::from_secs(2),
            ..Default::default()
        });
        let pool = Arc::new(ConnectionPool::new(&client_config));
        let consumer: Consumer<Vec<u8>, Vec<u8>> = Consumer::new(
            client_config,
            pool.clone(),
            "events".to_string(),
            ConsumerConfig::default(),
            vec![0],
        );

        consumer.resolve_pending_earliest_offsets().await.unwrap();
        assert_eq!(consumer.position(0).await.unwrap(), 41);
        assert!(consumer.unresolved_earliest.lock().await.is_empty());
        assert!(pool.is_healthy().await);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_seek_before_first_poll_skips_earliest_offset_resolution() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut stream, _) = listener.accept().await.unwrap();
            let request_length = stream.read_i32().await.unwrap();
            let mut request = vec![0; request_length as usize];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(
                i16::from_be_bytes([request[0], request[1]]),
                1,
                "an explicit seek must make Fetch, not ListOffsets, the first request"
            );

            let response = build_test_fetch_response(1000, 0, "events", 0, 0, &[], 0, 0);
            stream.write_i32(response.len() as i32).await.unwrap();
            stream.write_all(&response).await.unwrap();
        });

        let client_config = Arc::new(StreamlineConfig {
            bootstrap_servers: address.to_string(),
            connection_pool_size: 1,
            request_timeout: Duration::from_secs(2),
            ..Default::default()
        });
        let pool = Arc::new(ConnectionPool::new(&client_config));
        let mut consumer: Consumer<Vec<u8>, Vec<u8>> = Consumer::new(
            client_config,
            pool,
            "events".to_string(),
            ConsumerConfig::default(),
            vec![0],
        );
        consumer.subscribe().await.unwrap();
        assert!(consumer.unresolved_earliest.lock().await.contains(&0));

        consumer.seek(0, 17).await.unwrap();

        assert_eq!(consumer.position(0).await.unwrap(), 17);
        assert!(!consumer.unresolved_earliest.lock().await.contains(&0));
        assert!(consumer
            .poll(Duration::from_millis(10))
            .await
            .unwrap()
            .is_empty());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_poll_evicts_connection_after_protocol_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut stream, _) = listener.accept().await.unwrap();
            let request_length = stream.read_i32().await.unwrap();
            let mut request = vec![0; request_length as usize];
            stream.read_exact(&mut request).await.unwrap();
            // Well-framed response carrying a mismatched correlation ID.
            let response = build_test_fetch_response(999_999, 0, "events", 0, 0, &[], 0, 0);
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
        let mut consumer: Consumer<Vec<u8>, Vec<u8>> = Consumer::new(
            client_config,
            pool.clone(),
            "events".to_string(),
            ConsumerConfig::default(),
            vec![0],
        );
        consumer.unresolved_earliest.lock().await.clear();
        consumer.subscribe().await.unwrap();

        let error = consumer.poll(Duration::from_millis(10)).await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(
            !pool.is_healthy().await,
            "a protocol-violating connection must be evicted from the pool"
        );
        server.abort();
    }

    #[tokio::test]
    async fn test_poll_surfaces_partition_error_instead_of_empty_batch() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut stream, _) = listener.accept().await.unwrap();
            let request_length = stream.read_i32().await.unwrap();
            let mut request = vec![0; request_length as usize];
            stream.read_exact(&mut request).await.unwrap();
            let correlation_id =
                i32::from_be_bytes([request[4], request[5], request[6], request[7]]);
            // Error code 3 = UNKNOWN_TOPIC_OR_PARTITION at partition level.
            let response = build_test_fetch_response(correlation_id, 0, "events", 0, 3, &[], 0, 0);
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
        let mut consumer: Consumer<Vec<u8>, Vec<u8>> = Consumer::new(
            client_config,
            pool.clone(),
            "events".to_string(),
            ConsumerConfig::default(),
            vec![0],
        );
        consumer.unresolved_earliest.lock().await.clear();
        consumer.subscribe().await.unwrap();

        let error = consumer.poll(Duration::from_millis(10)).await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::TopicNotFound);
        // A broker application error leaves the framed connection usable.
        assert!(pool.is_healthy().await);
        server.abort();
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
    async fn test_commit_fails_closed() {
        let mut consumer = make_consumer();
        consumer.subscribe().await.unwrap();
        assert_eq!(
            consumer.commit().await.unwrap_err().kind,
            ErrorKind::Unsupported
        );
        assert_eq!(
            consumer.commit_async().unwrap_err().kind,
            ErrorKind::Unsupported
        );
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
        assert_eq!(
            consumer.seek_to_end().await.unwrap_err().kind,
            ErrorKind::Unsupported
        );
        let invalid_offset = consumer.seek(0, -1).await.unwrap_err();
        assert_eq!(invalid_offset.kind, ErrorKind::InvalidConfiguration);
        assert!(invalid_offset.to_string().contains("got -1"));
        assert!(!invalid_offset.to_string().contains("unresolved_earliest"));
        assert_eq!(
            consumer.seek(9, 0).await.unwrap_err().kind,
            ErrorKind::PartitionNotFound
        );
    }

    #[test]
    fn test_pause_resume() {
        let consumer = make_consumer();
        assert!(consumer.paused().unwrap().is_empty());

        consumer.pause(&[0]).unwrap();
        assert!(consumer.paused().unwrap().contains(&0));

        consumer.resume(&[0]).unwrap();
        assert!(consumer.paused().unwrap().is_empty());
    }

    #[test]
    fn test_pause_resume_multi_partition() {
        let consumer = make_multi_partition_consumer();
        assert_eq!(consumer.assigned_partitions(), &[0, 1, 2]);

        consumer.pause(&[0, 2]).unwrap();
        assert!(consumer.paused().unwrap().contains(&0));
        assert!(!consumer.paused().unwrap().contains(&1));
        assert!(consumer.paused().unwrap().contains(&2));

        consumer.resume(&[0]).unwrap();
        assert!(!consumer.paused().unwrap().contains(&0));
        assert!(consumer.paused().unwrap().contains(&2));

        consumer.resume(&[2]).unwrap();
        assert!(consumer.paused().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_seek_multi_partition() {
        let mut consumer = make_multi_partition_consumer();
        consumer.subscribe().await.unwrap();

        consumer.seek(1, 50).await.unwrap();
        assert_eq!(consumer.position(1).await.unwrap(), 50);
        assert_eq!(consumer.position(0).await.unwrap(), 0);
        assert_eq!(consumer.position(2).await.unwrap(), 0);

        consumer.seek_to_beginning().await.unwrap();
        assert_eq!(consumer.position(0).await.unwrap(), 0);
        assert_eq!(consumer.position(1).await.unwrap(), 0);
        assert_eq!(consumer.position(2).await.unwrap(), 0);
    }

    #[test]
    fn test_assigned_partitions() {
        let consumer = make_multi_partition_consumer();
        assert_eq!(consumer.assigned_partitions(), &[0, 1, 2]);
    }

    #[test]
    fn test_build_fetch_request() {
        let request = build_fetch_request(1, "test", &[(0, 0)], 1000);
        assert!(request.len() > 4);
        let len = i32::from_be_bytes([request[0], request[1], request[2], request[3]]);
        assert_eq!(len as usize, request.len() - 4);
        // API key should be 1 (Fetch)
        assert_eq!(request[4], 0);
        assert_eq!(request[5], 1);
    }

    #[test]
    fn test_build_list_offsets_request_uses_earliest_timestamp() {
        let request =
            build_list_offsets_request(7, "events", &[0, 2], LIST_OFFSETS_EARLIEST_TIMESTAMP);
        assert_eq!(i16::from_be_bytes([request[4], request[5]]), 2);
        assert_eq!(i16::from_be_bytes([request[6], request[7]]), 5);
        assert!(
            request
                .windows(8)
                .any(|window| window == LIST_OFFSETS_EARLIEST_TIMESTAMP.to_be_bytes()),
            "request must contain the Kafka earliest timestamp marker"
        );
    }

    #[test]
    fn test_parse_list_offsets_response_returns_log_start_offsets() {
        let response = build_test_list_offsets_response(7, "events", &[(0, 0, 41), (2, 0, 99)]);
        let offsets = parse_list_offsets_response(7, "events", &[0, 2], &response).unwrap();
        assert_eq!(offsets.get(&0), Some(&41));
        assert_eq!(offsets.get(&2), Some(&99));
    }

    #[test]
    fn test_parse_list_offsets_response_maps_offset_out_of_range() {
        let response = build_test_list_offsets_response(7, "events", &[(0, 1, -1)]);
        let error = parse_list_offsets_response(7, "events", &[0], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::OffsetOutOfRange);
    }

    #[test]
    fn test_build_fetch_request_multi_partition() {
        let single = build_fetch_request(1, "test", &[(0, 0)], 1000);
        let multi = build_fetch_request(1, "test", &[(0, 0), (1, 100), (2, 200)], 1000);
        // Multi-partition request should be larger due to additional partition entries
        assert!(multi.len() > single.len());
        let len = i32::from_be_bytes([multi[0], multi[1], multi[2], multi[3]]);
        assert_eq!(len as usize, multi.len() - 4);
    }

    #[test]
    fn test_build_fetch_request_golden_bytes() {
        // Golden encoding of a Fetch v11 request for topic "ev", partition 7,
        // fetch offset 42, max_wait 500ms, correlation ID 9.
        let request = build_fetch_request(9, "ev", &[(7, 42)], 500);

        let mut expected: Vec<u8> = Vec::new();
        expected.extend_from_slice(&0i32.to_be_bytes()); // length placeholder
        expected.extend_from_slice(&1i16.to_be_bytes()); // api_key = Fetch
        expected.extend_from_slice(&11i16.to_be_bytes()); // api_version = 11
        expected.extend_from_slice(&9i32.to_be_bytes()); // correlation_id
        expected.extend_from_slice(&(CLIENT_ID.len() as i16).to_be_bytes());
        expected.extend_from_slice(CLIENT_ID);
        expected.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
        expected.extend_from_slice(&500i32.to_be_bytes()); // max_wait_ms
        expected.extend_from_slice(&1i32.to_be_bytes()); // min_bytes
        expected.extend_from_slice(&FETCH_MAX_BYTES.to_be_bytes()); // max_bytes
        expected.push(0u8); // isolation_level
        expected.extend_from_slice(&0i32.to_be_bytes()); // session_id
        expected.extend_from_slice(&(-1i32).to_be_bytes()); // session_epoch
        expected.extend_from_slice(&1i32.to_be_bytes()); // topics count
        expected.extend_from_slice(&2i16.to_be_bytes()); // topic name length
        expected.extend_from_slice(b"ev");
        expected.extend_from_slice(&1i32.to_be_bytes()); // partitions count
        expected.extend_from_slice(&7i32.to_be_bytes()); // partition index
        expected.extend_from_slice(&(-1i32).to_be_bytes()); // current_leader_epoch: INT32
        expected.extend_from_slice(&42i64.to_be_bytes()); // fetch_offset
        expected.extend_from_slice(&(-1i64).to_be_bytes()); // log_start_offset
        expected.extend_from_slice(&FETCH_MAX_BYTES.to_be_bytes()); // partition_max_bytes
        expected.extend_from_slice(&0i32.to_be_bytes()); // forgotten_topics_data
        expected.extend_from_slice(&0i16.to_be_bytes()); // rack_id: empty, non-null
        let length = (expected.len() - 4) as i32;
        expected[0..4].copy_from_slice(&length.to_be_bytes());

        assert_eq!(request, expected);
    }

    #[test]
    fn test_build_fetch_request_encodes_leader_epoch_as_int32() {
        let request = build_fetch_request(1, "t", &[(0, 0)], 100);
        // Offset of current_leader_epoch: 4 (length) + 2 + 2 + 4 (header)
        // + 2 + 1 (client id) + 4 + 4 + 4 + 4 + 1 + 4 + 4 (body)
        // + 4 + 2 + 1 (topics) + 4 (partition count) + 4 (partition index).
        let leader_epoch_offset = 4 + 8 + 2 + CLIENT_ID.len() + 25 + 4 + 2 + 1 + 4 + 4;
        let epoch = i32::from_be_bytes([
            request[leader_epoch_offset],
            request[leader_epoch_offset + 1],
            request[leader_epoch_offset + 2],
            request[leader_epoch_offset + 3],
        ]);
        assert_eq!(epoch, -1);

        // Fetch offset immediately follows the four-byte leader epoch.
        let fetch_offset_start = leader_epoch_offset + 4;
        let fetch_offset = i64::from_be_bytes(
            request[fetch_offset_start..fetch_offset_start + 8]
                .try_into()
                .unwrap(),
        );
        assert_eq!(fetch_offset, 0);
    }

    #[test]
    fn test_build_fetch_request_rack_id_is_empty_not_null() {
        let request = build_fetch_request(1, "t", &[(0, 0)], 100);
        let rack_id = i16::from_be_bytes([request[request.len() - 2], request[request.len() - 1]]);
        assert_eq!(rack_id, 0, "rack_id must be an empty, non-null string");
    }

    // -- parse_fetch_response test support ----------------------------------
    //
    // These helpers hand-encode a Kafka Fetch response (v11), mirroring the
    // wire format built by `producer::build_record` / `build_record_batch`,
    // so `parse_fetch_response` can be exercised without a live broker.

    fn encode_test_varint(buf: &mut Vec<u8>, value: i64) {
        let mut v = ((value << 1) ^ (value >> 63)) as u64;
        loop {
            let mut byte = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                byte |= 0x80;
            }
            buf.push(byte);
            if v == 0 {
                break;
            }
        }
    }

    fn build_test_record(
        key: Option<&[u8]>,
        value: &[u8],
        headers: &[(&str, &[u8])],
        offset_delta: i64,
    ) -> Vec<u8> {
        let mut record = Vec::new();
        record.push(0u8); // attributes
        encode_test_varint(&mut record, 0); // timestamp_delta
        encode_test_varint(&mut record, offset_delta);
        match key {
            Some(k) => {
                encode_test_varint(&mut record, k.len() as i64);
                record.extend_from_slice(k);
            }
            None => encode_test_varint(&mut record, -1),
        }
        encode_test_varint(&mut record, value.len() as i64);
        record.extend_from_slice(value);
        encode_test_varint(&mut record, headers.len() as i64);
        for (header_key, header_value) in headers {
            encode_test_varint(&mut record, header_key.len() as i64);
            record.extend_from_slice(header_key.as_bytes());
            encode_test_varint(&mut record, header_value.len() as i64);
            record.extend_from_slice(header_value);
        }

        let mut sized = Vec::new();
        encode_test_varint(&mut sized, record.len() as i64);
        sized.extend_from_slice(&record);
        sized
    }

    /// Builds a well-formed record batch (magic 2) with a valid CRC-32C.
    fn build_test_batch(
        records: &[Vec<u8>],
        base_offset: i64,
        first_timestamp: i64,
        attributes: i16,
    ) -> Vec<u8> {
        let mut all_records = Vec::new();
        for record in records {
            all_records.extend_from_slice(record);
        }

        let mut batch = Vec::new();
        batch.extend_from_slice(&base_offset.to_be_bytes());
        batch.extend_from_slice(&[0u8; 4]); // batch_length placeholder
        batch.extend_from_slice(&0i32.to_be_bytes()); // partition_leader_epoch
        batch.push(2u8); // magic = 2
        batch.extend_from_slice(&[0u8; 4]); // CRC placeholder
        batch.extend_from_slice(&attributes.to_be_bytes());
        batch.extend_from_slice(&((records.len() as i32 - 1).max(0)).to_be_bytes());
        batch.extend_from_slice(&first_timestamp.to_be_bytes());
        batch.extend_from_slice(&first_timestamp.to_be_bytes());
        batch.extend_from_slice(&(-1i64).to_be_bytes()); // producer_id
        batch.extend_from_slice(&(-1i16).to_be_bytes()); // producer_epoch
        batch.extend_from_slice(&(-1i32).to_be_bytes()); // base_sequence
        batch.extend_from_slice(&(records.len() as i32).to_be_bytes());
        batch.extend_from_slice(&all_records);

        let batch_length = (batch.len() - 12) as i32;
        batch[8..12].copy_from_slice(&batch_length.to_be_bytes());
        let crc = crate::producer::crc32c_compute(&batch[21..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());
        batch
    }

    /// Builds a single-topic, single-partition Fetch response wrapping the
    /// supplied record set bytes verbatim.
    fn build_test_fetch_response_with_record_set(
        correlation_id: i32,
        response_error_code: i16,
        topic: &str,
        partition: i32,
        partition_error_code: i16,
        record_set: &[u8],
    ) -> Vec<u8> {
        let mut resp = Vec::new();
        resp.extend_from_slice(&correlation_id.to_be_bytes());
        resp.extend_from_slice(&0i32.to_be_bytes()); // throttle_time_ms
        resp.extend_from_slice(&response_error_code.to_be_bytes());
        resp.extend_from_slice(&0i32.to_be_bytes()); // session_id
        resp.extend_from_slice(&1i32.to_be_bytes()); // responses_count
        resp.extend_from_slice(&(topic.len() as i16).to_be_bytes());
        resp.extend_from_slice(topic.as_bytes());
        resp.extend_from_slice(&1i32.to_be_bytes()); // partitions_count
        resp.extend_from_slice(&partition.to_be_bytes());
        resp.extend_from_slice(&partition_error_code.to_be_bytes());
        resp.extend_from_slice(&0i64.to_be_bytes()); // high_watermark
        resp.extend_from_slice(&0i64.to_be_bytes()); // last_stable_offset
        resp.extend_from_slice(&0i64.to_be_bytes()); // log_start_offset
        resp.extend_from_slice(&0i32.to_be_bytes()); // aborted_txns_count
        resp.extend_from_slice(&0i32.to_be_bytes()); // preferred_read_replica
        resp.extend_from_slice(&(record_set.len() as i32).to_be_bytes());
        resp.extend_from_slice(record_set);
        resp
    }

    fn build_test_list_offsets_response(
        correlation_id: i32,
        topic: &str,
        partitions: &[(i32, i16, i64)],
    ) -> Vec<u8> {
        let mut response = Vec::new();
        response.extend_from_slice(&correlation_id.to_be_bytes());
        response.extend_from_slice(&0i32.to_be_bytes());
        response.extend_from_slice(&1i32.to_be_bytes());
        response.extend_from_slice(&(topic.len() as i16).to_be_bytes());
        response.extend_from_slice(topic.as_bytes());
        response.extend_from_slice(&(partitions.len() as i32).to_be_bytes());
        for &(partition, error_code, offset) in partitions {
            response.extend_from_slice(&partition.to_be_bytes());
            response.extend_from_slice(&error_code.to_be_bytes());
            response.extend_from_slice(&0i64.to_be_bytes());
            response.extend_from_slice(&offset.to_be_bytes());
            response.extend_from_slice(&(-1i32).to_be_bytes());
        }
        response
    }

    #[allow(clippy::too_many_arguments)]
    fn build_test_fetch_response(
        correlation_id: i32,
        response_error_code: i16,
        topic: &str,
        partition: i32,
        partition_error_code: i16,
        records: &[Vec<u8>],
        base_offset: i64,
        first_timestamp: i64,
    ) -> Vec<u8> {
        let record_set = if records.is_empty() {
            Vec::new()
        } else {
            build_test_batch(records, base_offset, first_timestamp, 0)
        };
        build_test_fetch_response_with_record_set(
            correlation_id,
            response_error_code,
            topic,
            partition,
            partition_error_code,
            &record_set,
        )
    }

    /// Parses a fetch response and returns just the records, defaulting the
    /// requested offsets so existing golden cases stay readable.
    fn parse_records_for_test(
        correlation_id: i32,
        topic: &str,
        requested: &[(i32, i64)],
        response: &[u8],
    ) -> Result<Vec<ConsumerRecord<Vec<u8>, Vec<u8>>>> {
        parse_fetch_response::<Vec<u8>, Vec<u8>>(correlation_id, topic, requested, response)
            .map(|outcome| outcome.records)
    }

    #[test]
    fn test_crc32c_matches_known_vector() {
        assert_eq!(crate::producer::crc32c_compute(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn test_parse_fetch_response_decodes_key_value_and_headers() {
        let record = build_test_record(
            Some(b"record-key"),
            b"record-value",
            &[("trace-id", b"trace-value")],
            0,
        );
        let response = build_test_fetch_response(7, 0, "events", 3, 0, &[record], 100, 1_000);

        let records = parse_records_for_test(7, "events", &[(3, 0)], &response)
            .expect("well-formed response should parse");

        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.topic, "events");
        assert_eq!(record.partition, 3);
        assert_eq!(record.offset, 100);
        assert_eq!(record.timestamp, 1_000);
        assert_eq!(record.key.as_deref(), Some(b"record-key".as_slice()));
        assert_eq!(record.value, b"record-value");
        assert_eq!(
            record.headers.get("trace-id"),
            Some(b"trace-value".as_slice())
        );
    }

    #[test]
    fn test_parse_fetch_response_decodes_multiple_records() {
        let records = vec![
            build_test_record(Some(b"k0"), b"v0", &[], 0),
            build_test_record(Some(b"k1"), b"v1", &[], 1),
        ];
        let response = build_test_fetch_response(1, 0, "events", 0, 0, &records, 10, 5);
        let parsed = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].offset, 10);
        assert_eq!(parsed[1].offset, 11);
    }

    #[test]
    fn test_parse_fetch_response_handles_null_key_and_no_headers() {
        let record = build_test_record(None, b"value-only", &[], 0);
        let response = build_test_fetch_response(1, 0, "events", 0, 0, &[record], 0, 0);

        let records = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, None);
        assert!(records[0].headers.is_empty());
    }

    #[test]
    fn test_parse_fetch_response_rejects_correlation_mismatch() {
        let response = build_test_fetch_response(5, 0, "events", 0, 0, &[], 0, 0);
        let error = parse_records_for_test(6, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
    }

    #[test]
    fn test_parse_fetch_response_rejects_response_level_broker_error() {
        // Error code 3 = UNKNOWN_TOPIC_OR_PARTITION.
        let response = build_test_fetch_response(1, 3, "events", 0, 0, &[], 0, 0);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::TopicNotFound);
    }

    #[test]
    fn test_parse_fetch_response_maps_partition_error_to_error() {
        // Error code 7 = REQUEST_TIMED_OUT. A per-partition error must surface
        // as an error, never as an empty successful poll.
        let response = build_test_fetch_response(1, 0, "events", 4, 7, &[], 0, 0);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Timeout);
        assert!(error.message.contains("partition 4"), "{}", error.message);
    }

    #[test]
    fn test_parse_fetch_response_maps_partition_error_codes() {
        for (code, expected) in [
            (3i16, ErrorKind::TopicNotFound),
            (6, ErrorKind::Server),
            (29, ErrorKind::AuthorizationFailed),
            (1, ErrorKind::OffsetOutOfRange),
        ] {
            let response = build_test_fetch_response(1, 0, "events", 0, code, &[], 0, 0);
            let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
            assert_eq!(error.kind, expected, "error code {code}");
        }
    }

    #[test]
    fn test_parse_fetch_response_empty_when_no_records() {
        let response = build_test_fetch_response(1, 0, "events", 0, 0, &[], 0, 0);
        let records = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_parse_fetch_response_rejects_truncated_response() {
        let error = parse_records_for_test(1, "events", &[(0, 0)], &[0u8; 4]).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
    }

    #[test]
    fn test_parse_fetch_response_rejects_response_truncated_mid_batch() {
        let record = build_test_record(Some(b"k"), b"v", &[], 0);
        let mut response = build_test_fetch_response(1, 0, "events", 0, 0, &[record], 0, 0);
        response.truncate(response.len() - 5);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(error.message.contains("Truncated"), "{}", error.message);
    }

    #[test]
    fn test_parse_fetch_response_rejects_trailing_bytes() {
        let mut response = build_test_fetch_response(1, 0, "events", 0, 0, &[], 0, 0);
        response.extend_from_slice(&[0xAA, 0xBB]);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(error.message.contains("trailing"), "{}", error.message);
    }

    #[test]
    fn test_parse_fetch_response_rejects_trailing_bytes_in_record_set() {
        let record = build_test_record(Some(b"k"), b"v", &[], 0);
        let mut record_set = build_test_batch(&[record], 0, 0, 0);
        record_set.extend_from_slice(&[0x01, 0x02, 0x03]);
        let response = build_test_fetch_response_with_record_set(1, 0, "events", 0, 0, &record_set);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
    }

    #[test]
    fn test_parse_fetch_response_rejects_crc_mismatch() {
        let record = build_test_record(Some(b"key"), b"value", &[], 0);
        let mut record_set = build_test_batch(&[record], 0, 0, 0);
        // Corrupt a CRC-covered byte (the value payload) without fixing the CRC.
        let last = record_set.len() - 1;
        record_set[last] ^= 0xFF;
        let response = build_test_fetch_response_with_record_set(1, 0, "events", 0, 0, &record_set);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(error.message.contains("CRC mismatch"), "{}", error.message);
    }

    #[test]
    fn test_parse_fetch_response_rejects_compressed_batch() {
        // attributes = 2 -> snappy; any nonzero codec must fail closed.
        let record = build_test_record(Some(b"key"), b"value", &[], 0);
        let record_set = build_test_batch(&[record], 0, 0, 2);
        let response = build_test_fetch_response_with_record_set(1, 0, "events", 0, 0, &record_set);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert!(error.message.contains("compressed"), "{}", error.message);
    }

    #[test]
    fn test_parse_fetch_response_rejects_bad_magic() {
        let record = build_test_record(Some(b"key"), b"value", &[], 0);
        let mut record_set = build_test_batch(&[record], 0, 0, 0);
        record_set[16] = 1; // magic byte
        let response = build_test_fetch_response_with_record_set(1, 0, "events", 0, 0, &record_set);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(error.message.contains("magic"), "{}", error.message);
    }

    #[test]
    fn test_parse_fetch_response_rejects_short_batch_length() {
        let mut record_set = Vec::new();
        record_set.extend_from_slice(&0i64.to_be_bytes()); // base_offset
        record_set.extend_from_slice(&10i32.to_be_bytes()); // batch_length far too small
        record_set.extend_from_slice(&[0u8; 10]);
        let response = build_test_fetch_response_with_record_set(1, 0, "events", 0, 0, &record_set);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
    }

    #[test]
    fn test_parse_fetch_response_rejects_negative_batch_length() {
        let mut record_set = Vec::new();
        record_set.extend_from_slice(&0i64.to_be_bytes());
        record_set.extend_from_slice(&(-1i32).to_be_bytes());
        let response = build_test_fetch_response_with_record_set(1, 0, "events", 0, 0, &record_set);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
    }

    #[test]
    fn test_parse_fetch_response_rejects_inflated_record_count() {
        let record = build_test_record(Some(b"key"), b"value", &[], 0);
        let mut record_set = build_test_batch(&[record], 0, 0, 0);
        // record_count lives at bytes 57..61 of the batch; inflate it and
        // recompute the CRC so the count check (not the CRC) is exercised.
        record_set[57..61].copy_from_slice(&1_000_000i32.to_be_bytes());
        let crc = crate::producer::crc32c_compute(&record_set[21..]);
        record_set[17..21].copy_from_slice(&crc.to_be_bytes());
        let response = build_test_fetch_response_with_record_set(1, 0, "events", 0, 0, &record_set);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(error.message.contains("record(s)"), "{}", error.message);
    }

    #[test]
    fn test_parse_fetch_response_rejects_unexpected_topic() {
        let response = build_test_fetch_response(1, 0, "other", 0, 0, &[], 0, 0);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(error.message.contains("other"), "{}", error.message);
    }

    #[test]
    fn test_parse_fetch_response_rejects_negative_counts() {
        let mut response = Vec::new();
        response.extend_from_slice(&1i32.to_be_bytes()); // correlation_id
        response.extend_from_slice(&0i32.to_be_bytes()); // throttle
        response.extend_from_slice(&0i16.to_be_bytes()); // error code
        response.extend_from_slice(&0i32.to_be_bytes()); // session id
        response.extend_from_slice(&(-7i32).to_be_bytes()); // responses count
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
    }

    #[test]
    fn test_parse_fetch_response_rejects_null_non_nullable_response_array() {
        let mut response = Vec::new();
        response.extend_from_slice(&1i32.to_be_bytes());
        response.extend_from_slice(&0i32.to_be_bytes());
        response.extend_from_slice(&0i16.to_be_bytes());
        response.extend_from_slice(&0i32.to_be_bytes());
        response.extend_from_slice(&(-1i32).to_be_bytes());

        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(error.message.contains("response count"));
    }

    #[test]
    fn test_parse_fetch_response_rejects_null_non_nullable_partition_array() {
        let mut response = Vec::new();
        response.extend_from_slice(&1i32.to_be_bytes());
        response.extend_from_slice(&0i32.to_be_bytes());
        response.extend_from_slice(&0i16.to_be_bytes());
        response.extend_from_slice(&0i32.to_be_bytes());
        response.extend_from_slice(&1i32.to_be_bytes());
        response.extend_from_slice(&6i16.to_be_bytes());
        response.extend_from_slice(b"events");
        response.extend_from_slice(&(-1i32).to_be_bytes());

        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(error.message.contains("partition count"));
    }

    #[test]
    fn test_parse_fetch_response_accepts_null_aborted_transactions_array() {
        let mut response = Vec::new();
        response.extend_from_slice(&1i32.to_be_bytes());
        response.extend_from_slice(&0i32.to_be_bytes());
        response.extend_from_slice(&0i16.to_be_bytes());
        response.extend_from_slice(&0i32.to_be_bytes());
        response.extend_from_slice(&1i32.to_be_bytes());
        response.extend_from_slice(&6i16.to_be_bytes());
        response.extend_from_slice(b"events");
        response.extend_from_slice(&1i32.to_be_bytes());
        response.extend_from_slice(&0i32.to_be_bytes());
        response.extend_from_slice(&0i16.to_be_bytes());
        response.extend_from_slice(&0i64.to_be_bytes());
        response.extend_from_slice(&0i64.to_be_bytes());
        response.extend_from_slice(&0i64.to_be_bytes());
        response.extend_from_slice(&(-1i32).to_be_bytes());
        response.extend_from_slice(&(-1i32).to_be_bytes());
        response.extend_from_slice(&(-1i32).to_be_bytes());

        let outcome = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap();
        assert!(outcome.is_empty());
    }

    #[test]
    fn test_parse_fetch_response_rejects_negative_record_set_size() {
        let mut response = Vec::new();
        response.extend_from_slice(&1i32.to_be_bytes());
        response.extend_from_slice(&0i32.to_be_bytes());
        response.extend_from_slice(&0i16.to_be_bytes());
        response.extend_from_slice(&0i32.to_be_bytes());
        response.extend_from_slice(&1i32.to_be_bytes()); // responses count
        response.extend_from_slice(&6i16.to_be_bytes());
        response.extend_from_slice(b"events");
        response.extend_from_slice(&1i32.to_be_bytes()); // partitions count
        response.extend_from_slice(&0i32.to_be_bytes()); // partition
        response.extend_from_slice(&0i16.to_be_bytes()); // error code
        response.extend_from_slice(&0i64.to_be_bytes());
        response.extend_from_slice(&0i64.to_be_bytes());
        response.extend_from_slice(&0i64.to_be_bytes());
        response.extend_from_slice(&0i32.to_be_bytes()); // aborted txns
        response.extend_from_slice(&0i32.to_be_bytes()); // preferred replica
        response.extend_from_slice(&(-5i32).to_be_bytes()); // record set size
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
    }

    #[test]
    fn test_parse_fetch_response_rejects_record_with_trailing_bytes() {
        // Append a stray byte inside the record body while keeping the record
        // length varint consistent, so only the record-level check can catch it.
        let mut record_body = Vec::new();
        record_body.push(0u8); // attributes
        encode_test_varint(&mut record_body, 0); // timestamp_delta
        encode_test_varint(&mut record_body, 0); // offset_delta
        encode_test_varint(&mut record_body, -1); // null key
        encode_test_varint(&mut record_body, 1); // value length
        record_body.push(b'v');
        encode_test_varint(&mut record_body, 0); // header count
        record_body.push(0xEE); // stray trailing byte

        let mut sized = Vec::new();
        encode_test_varint(&mut sized, record_body.len() as i64);
        sized.extend_from_slice(&record_body);

        let record_set = build_test_batch(&[sized], 0, 0, 0);
        let response = build_test_fetch_response_with_record_set(1, 0, "events", 0, 0, &record_set);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(error.message.contains("trailing"), "{}", error.message);
    }

    #[test]
    fn test_parse_fetch_response_rejects_out_of_range_offset_delta() {
        let record = build_test_record(Some(b"k"), b"v", &[], 5);
        let record_set = build_test_batch(&[record], 0, 0, 0);
        let response = build_test_fetch_response_with_record_set(1, 0, "events", 0, 0, &record_set);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(error.message.contains("offset delta"), "{}", error.message);
    }

    #[test]
    fn test_parse_fetch_response_drops_records_below_the_requested_offset() {
        // Fetches are batch aligned: the broker returns the whole batch that
        // contains the requested offset, including already-consumed records.
        let records = vec![
            build_test_record(Some(b"k0"), b"v0", &[], 0),
            build_test_record(Some(b"k1"), b"v1", &[], 1),
            build_test_record(Some(b"k2"), b"v2", &[], 2),
        ];
        let response = build_test_fetch_response(1, 0, "events", 0, 0, &records, 10, 0);

        let outcome =
            parse_fetch_response::<Vec<u8>, Vec<u8>>(1, "events", &[(0, 12)], &response).unwrap();

        assert_eq!(outcome.records.len(), 1);
        assert_eq!(outcome.records[0].offset, 12);
        assert_eq!(outcome.next_offsets.get(&0), Some(&13));
    }

    #[test]
    fn test_parse_fetch_response_skips_control_batches() {
        // attributes bit 5 (0x20) marks a transaction control batch, which is
        // internal bookkeeping and must never reach the caller.
        let record = build_test_record(None, b"\x00\x00\x00\x00", &[], 0);
        let record_set = build_test_batch(&[record], 40, 0, 0x20);
        let response = build_test_fetch_response_with_record_set(1, 0, "events", 0, 0, &record_set);

        let outcome =
            parse_fetch_response::<Vec<u8>, Vec<u8>>(1, "events", &[(0, 40)], &response).unwrap();

        assert!(
            outcome.records.is_empty(),
            "control records must be skipped"
        );
        // The batch is still consumed, so the poll makes progress.
        assert_eq!(outcome.next_offsets.get(&0), Some(&41));
    }

    #[test]
    fn test_parse_fetch_response_advances_past_fully_filtered_batches() {
        let records = vec![
            build_test_record(Some(b"k0"), b"v0", &[], 0),
            build_test_record(Some(b"k1"), b"v1", &[], 1),
        ];
        let response = build_test_fetch_response(1, 0, "events", 0, 0, &records, 100, 0);

        let outcome =
            parse_fetch_response::<Vec<u8>, Vec<u8>>(1, "events", &[(0, 102)], &response).unwrap();

        assert!(outcome.records.is_empty());
        assert_eq!(outcome.next_offsets.get(&0), Some(&102));
    }

    #[test]
    fn test_parse_fetch_response_rejects_unrequested_partition() {
        let response = build_test_fetch_response(1, 0, "events", 5, 0, &[], 0, 0);
        let error = parse_fetch_response::<Vec<u8>, Vec<u8>>(1, "events", &[(0, 0)], &response)
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(error.message.contains("not requested"), "{}", error.message);
    }

    #[test]
    fn test_parse_fetch_response_rejects_negative_last_offset_delta() {
        let record = build_test_record(Some(b"k"), b"v", &[], 0);
        let mut record_set = build_test_batch(&[record], 0, 0, 0);
        record_set[23..27].copy_from_slice(&(-1i32).to_be_bytes());
        let crc = crate::producer::crc32c_compute(&record_set[21..]);
        record_set[17..21].copy_from_slice(&crc.to_be_bytes());
        let response = build_test_fetch_response_with_record_set(1, 0, "events", 0, 0, &record_set);
        let error = parse_fetch_response::<Vec<u8>, Vec<u8>>(1, "events", &[(0, 0)], &response)
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
    }

    #[test]
    fn test_parse_fetch_response_rejects_overlong_varint() {
        // A record whose length varint never terminates.
        let mut record_set_records = vec![0x80u8; 12];
        record_set_records.push(0x80);
        let mut batch_records = Vec::new();
        batch_records.extend_from_slice(&record_set_records);

        let record_set = build_test_batch(&[batch_records], 0, 0, 0);
        let response = build_test_fetch_response_with_record_set(1, 0, "events", 0, 0, &record_set);
        let error = parse_records_for_test(1, "events", &[(0, 0)], &response).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
    }

    #[test]
    fn test_fetch_cursor_rejects_tenth_varint_byte_with_payload_above_one() {
        let mut encoded = vec![0x80; 9];
        encoded.push(0x02);
        let mut cursor = FetchCursor::new(&encoded);

        let error = cursor.read_varint("test varint").unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(error.message.contains("over-long"));
    }
}
