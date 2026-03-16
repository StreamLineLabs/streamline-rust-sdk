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
use tracing::{debug, info, warn};

fn lock_mutex<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex.lock().map_err(|_: PoisonError<MutexGuard<'_, T>>| {
        Error::new(
            ErrorKind::Internal,
            "Internal lock poisoned — a previous operation panicked",
        )
        .with_hint("This indicates a bug in the client. Please report it.")
    })
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
    #[allow(dead_code)]
    client_config: Arc<StreamlineConfig>,
    pool: Arc<ConnectionPool>,
    topic: String,
    config: ConsumerConfig,
    subscribed: bool,
    partitions: Vec<i32>,
    partition_offsets: Mutex<HashMap<i32, i64>>,
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
        let initial_offset = if config.auto_offset_reset == "earliest" {
            0
        } else {
            -1 // latest: will be resolved on first fetch
        };

        let partition_offsets: HashMap<i32, i64> = partitions
            .iter()
            .map(|&p| (p, initial_offset))
            .collect();

        Self {
            client_config,
            pool,
            topic,
            config,
            subscribed: false,
            partitions,
            partition_offsets: Mutex::new(partition_offsets),
            paused_partitions: Mutex::new(HashSet::new()),
            correlation_id: AtomicI32::new(1000),
            _marker: std::marker::PhantomData,
        }
    }

    fn next_correlation_id(&self) -> i32 {
        self.correlation_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Subscribes to the topic.
    ///
    /// If no partitions were configured via the builder, attempts to discover
    /// them via topic metadata, falling back to partition 0.
    pub async fn subscribe(&mut self) -> Result<()> {
        if !self.subscribed {
            info!("Subscribing to topic {}", self.topic);

            // If no partitions were configured, discover them via metadata
            if self.partitions.is_empty() {
                match self.pool.describe_topic(&self.topic).await {
                    Ok((topic_info, partition_infos)) => {
                        self.partitions = if partition_infos.is_empty() {
                            (0..topic_info.partitions).collect()
                        } else {
                            partition_infos.iter().map(|p| p.id).collect()
                        };
                        info!(
                            "Discovered {} partitions for topic {}",
                            self.partitions.len(),
                            self.topic
                        );
                    }
                    Err(_) => {
                        debug!(
                            "Metadata discovery failed for topic {}, defaulting to partition 0",
                            self.topic
                        );
                        self.partitions = vec![0];
                    }
                }
            }

            // Initialize offsets for partitions that don't have one yet
            let initial_offset = if self.config.auto_offset_reset == "earliest" {
                0
            } else {
                -1
            };
            let mut offsets = lock_mutex(&self.partition_offsets)?;
            for &p in &self.partitions {
                offsets.entry(p).or_insert(initial_offset);
            }

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

        // Build Kafka Fetch request (API key 1, version 11) for all active partitions
        let request = build_fetch_request(
            correlation_id,
            &self.topic,
            &active_partitions,
            timeout.as_millis() as i32,
        );

        let conn_handle = self.pool.get().await?;
        let mut conn = conn_handle.lock().await;
        let stream = conn.ensure_connected().await?;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        stream.write_all(&request).await
            .map_err(|e| Error::connection(format!("Fetch write failed: {e}")))?;

        // Read response
        let resp_len = stream.read_i32().await
            .map_err(|e| Error::connection(format!("Fetch read failed: {e}")))?;

        if resp_len <= 0 || resp_len > 100_000_000 {
            return Ok(Vec::new());
        }

        let mut resp_buf = vec![0u8; resp_len as usize];
        stream.read_exact(&mut resp_buf).await
            .map_err(|e| Error::connection(format!("Fetch read body failed: {e}")))?;

        // Parse records from the response (simplified extraction)
        let records = parse_fetch_response::<K, V>(&self.topic, &resp_buf);

        // Advance per-partition fetch offsets past consumed records
        if !records.is_empty() {
            let mut offsets = lock_mutex(&self.partition_offsets)?;
            for record in &records {
                let entry = offsets.entry(record.partition).or_insert(0);
                if record.offset + 1 > *entry {
                    *entry = record.offset + 1;
                }
            }
        }

        Ok(records)
    }

    /// Commits the current offsets to the broker via the OffsetCommit API.
    /// Requires a consumer group ID to be configured.
    pub async fn commit(&self) -> Result<()> {
        let group_id = match &self.config.group_id {
            Some(g) => g.clone(),
            None => {
                debug!("No group_id configured — offsets tracked locally only");
                return Ok(());
            }
        };

        let offsets: HashMap<i32, i64> = lock_mutex(&self.partition_offsets)?.clone();

        for (&partition, &offset) in &offsets {
            debug!(
                "Committing offset {} for topic {} partition {} (group {})",
                offset, self.topic, partition, group_id
            );

            let correlation_id = self.next_correlation_id();
            let request = build_offset_commit_request(
                correlation_id,
                &group_id,
                &self.topic,
                partition,
                offset,
            );

            let conn_handle = self.pool.get().await?;
            let mut conn = conn_handle.lock().await;
            let stream = conn.ensure_connected().await?;

            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            stream.write_all(&request).await
                .map_err(|e| Error::connection(format!("Commit write failed: {e}")))?;

            let resp_len = stream.read_i32().await
                .map_err(|e| Error::connection(format!("Commit read failed: {e}")))?;
            if resp_len <= 0 || resp_len > 100_000_000 {
                return Err(Error::new(
                    ErrorKind::Protocol,
                    format!("Invalid commit response length from server: {resp_len}"),
                ));
            }
            let mut resp_buf = vec![0u8; resp_len as usize];
            stream.read_exact(&mut resp_buf).await
                .map_err(|e| Error::connection(format!("Commit read body failed: {e}")))?;

            // Check for error in response (simplified: check error code at known offset)
            if resp_buf.len() >= 14 {
                let error_code = i16::from_be_bytes([resp_buf[12], resp_buf[13]]);
                if error_code != 0 {
                    return Err(Error::new(
                        ErrorKind::Protocol,
                        format!(
                            "OffsetCommit failed for partition {} with error code {}",
                            partition, error_code
                        ),
                    ).with_hint("Ensure the consumer group exists and the client is authorized"));
                }
            }
        }

        Ok(())
    }

    /// Commits the current offsets asynchronously by spawning a background task.
    /// Errors are logged but not propagated to the caller.
    pub fn commit_async(&self)
    where
        K: Send + 'static,
        V: Send + 'static,
    {
        let group_id = match &self.config.group_id {
            Some(g) => g.clone(),
            None => {
                debug!("Async commit: no group_id — offsets tracked locally");
                return;
            }
        };
        let offsets: HashMap<i32, i64> = match lock_mutex(&self.partition_offsets) {
            Ok(guard) => guard.clone(),
            Err(e) => {
                warn!("Async commit: failed to acquire offsets lock: {}", e);
                return;
            }
        };
        let topic = self.topic.clone();
        let pool = self.pool.clone();
        let correlation_id = self.next_correlation_id();

        tokio::spawn(async move {
            for (&partition, &offset) in &offsets {
                let request = build_offset_commit_request(
                    correlation_id,
                    &group_id,
                    &topic,
                    partition,
                    offset,
                );
                match pool.get().await {
                    Ok(conn_handle) => {
                        let mut conn = conn_handle.lock().await;
                        match conn.ensure_connected().await {
                            Ok(stream) => {
                                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                                if let Err(e) = stream.write_all(&request).await {
                                    debug!("Async commit write failed for partition {}: {}", partition, e);
                                    continue;
                                }
                                match stream.read_i32().await {
                                    Ok(resp_len) if resp_len > 0 && resp_len <= 100_000_000 => {
                                        let mut buf = vec![0u8; resp_len as usize];
                                        let _ = stream.read_exact(&mut buf).await;
                                    }
                                    _ => {}
                                }
                                debug!(
                                    "Async commit succeeded for partition {} offset {} (group {})",
                                    partition, offset, group_id
                                );
                            }
                            Err(e) => debug!("Async commit connection failed: {}", e),
                        }
                    }
                    Err(e) => debug!("Async commit pool failed: {}", e),
                }
            }
        });
    }

    /// Seeks to the beginning of all partitions.
    pub async fn seek_to_beginning(&self) -> Result<()> {
        let mut offsets = lock_mutex(&self.partition_offsets)?;
        for offset in offsets.values_mut() {
            *offset = 0;
        }
        debug!("Seeking to beginning for all partitions");
        Ok(())
    }

    /// Seeks to the end of all partitions.
    pub async fn seek_to_end(&self) -> Result<()> {
        let mut offsets = lock_mutex(&self.partition_offsets)?;
        for offset in offsets.values_mut() {
            *offset = -1;
        }
        debug!("Seeking to end for all partitions");
        Ok(())
    }

    /// Seeks to a specific offset for a partition.
    pub async fn seek(&self, partition: i32, offset: i64) -> Result<()> {
        let mut offsets = lock_mutex(&self.partition_offsets)?;
        offsets.insert(partition, offset);
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

    // partitions array
    buf.extend_from_slice(&(partitions.len() as i32).to_be_bytes());
    for &(partition, fetch_offset) in partitions {
        buf.extend_from_slice(&partition.to_be_bytes());
        buf.extend_from_slice(&(-1i64).to_be_bytes()); // current_leader_epoch
        buf.extend_from_slice(&fetch_offset.to_be_bytes());
        buf.extend_from_slice(&(-1i64).to_be_bytes()); // log_start_offset
        buf.extend_from_slice(&(1024 * 1024i32).to_be_bytes()); // partition_max_bytes
    }

    // forgotten_topics: empty array
    buf.extend_from_slice(&0i32.to_be_bytes());
    // rack_id: empty
    buf.extend_from_slice(&(-1i16).to_be_bytes());

    // Fill length
    let total_len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&total_len.to_be_bytes());

    buf
}

/// Parse records from a Kafka Fetch response (v11).
///
/// Response layout after correlation_id:
/// [throttle_time_ms:i32][error_code:i16][session_id:i32]
/// [responses_count:i32] [
///   [topic_name:string][partitions_count:i32] [
///     [partition:i32][error_code:i16][high_watermark:i64]
///     [last_stable_offset:i64][log_start_offset:i64]
///     [aborted_txns_count:i32][...][record_set_size:i32][record_batch...]
///   ]
/// ]
#[allow(unused_assignments)] // pos is intentionally advanced past parsed sections
fn parse_fetch_response<K: From<Vec<u8>>, V: From<Vec<u8>>>(
    topic: &str,
    resp: &[u8],
) -> Vec<ConsumerRecord<K, V>> {
    let mut records = Vec::new();

    // Minimum response: correlation_id(4) + throttle(4) + error(2) + session(4) + count(4) = 18
    if resp.len() < 18 {
        return records;
    }

    let mut pos: usize = 4; // skip correlation_id
    pos += 4; // throttle_time_ms
    pos += 2; // error_code
    pos += 4; // session_id

    let responses_count = read_i32(resp, &mut pos);
    if responses_count <= 0 {
        return records;
    }

    for _ in 0..responses_count {
        // topic name (string: i16 length + bytes)
        let topic_len = read_i16(resp, &mut pos) as usize;
        if pos + topic_len > resp.len() { break; }
        let _topic_name = &resp[pos..pos + topic_len];
        pos += topic_len;

        let partitions_count = read_i32(resp, &mut pos);
        for _ in 0..partitions_count {
            if pos + 28 > resp.len() { break; }
            let partition = read_i32(resp, &mut pos);
            let error_code = read_i16(resp, &mut pos);
            let _high_watermark = read_i64(resp, &mut pos);
            let _last_stable = read_i64(resp, &mut pos);
            let _log_start = read_i64(resp, &mut pos);

            // Skip aborted transactions
            let aborted_count = read_i32(resp, &mut pos);
            if aborted_count > 0 {
                for _ in 0..aborted_count {
                    pos += 16; // producer_id(8) + first_offset(8)
                    if pos > resp.len() { return records; }
                }
            }

            // Skip preferred_read_replica (v11)
            if pos + 4 <= resp.len() {
                pos += 4;
            }

            // Record set size
            if pos + 4 > resp.len() { break; }
            let record_set_size = read_i32(resp, &mut pos);
            if record_set_size <= 0 || error_code != 0 {
                if record_set_size > 0 { pos += record_set_size as usize; }
                continue;
            }

            let batch_end = pos + record_set_size as usize;
            if batch_end > resp.len() { break; }

            // Parse RecordBatch(es) within the record set
            while pos + 57 <= batch_end {
                let base_offset = read_i64(resp, &mut pos);
                let batch_length = read_i32(resp, &mut pos);
                if batch_length <= 0 || pos + batch_length as usize > batch_end {
                    pos = batch_end;
                    break;
                }
                let batch_data_end = pos + batch_length as usize;

                pos += 4; // partition_leader_epoch
                let magic = if pos < batch_data_end { resp[pos] } else { 0 };
                pos += 1;
                pos += 4; // CRC
                let _attributes = read_i16(resp, &mut pos);
                let _last_offset_delta = read_i32(resp, &mut pos);
                let first_timestamp = read_i64(resp, &mut pos);
                let _max_timestamp = read_i64(resp, &mut pos);
                pos += 8; // producer_id
                pos += 2; // producer_epoch
                pos += 4; // base_sequence

                let num_records = read_i32(resp, &mut pos);

                if magic != 2 || num_records <= 0 {
                    pos = batch_data_end;
                    continue;
                }

                for _ in 0..num_records {
                    if pos >= batch_data_end { break; }

                    let record_size = varint_decode(resp, &mut pos);
                    if record_size <= 0 { continue; }
                    let record_end = pos + record_size as usize;
                    if record_end > batch_data_end { pos = batch_data_end; break; }

                    let _rec_attrs = if pos < record_end { resp[pos] } else { 0 };
                    pos += 1;
                    let timestamp_delta = varint_decode(resp, &mut pos);
                    let offset_delta = varint_decode(resp, &mut pos);

                    // Key
                    let key_len = varint_decode(resp, &mut pos);
                    let key = if key_len > 0 && pos + key_len as usize <= record_end {
                        let k = resp[pos..pos + key_len as usize].to_vec();
                        pos += key_len as usize;
                        Some(K::from(k))
                    } else {
                        if key_len > 0 { pos += key_len as usize; }
                        None
                    };

                    // Value
                    let value_len = varint_decode(resp, &mut pos);
                    let value = if value_len > 0 && pos + value_len as usize <= record_end {
                        let v = resp[pos..pos + value_len as usize].to_vec();
                        pos += value_len as usize;
                        V::from(v)
                    } else {
                        if value_len > 0 { pos += value_len as usize; }
                        V::from(Vec::new())
                    };

                    // Skip headers
                    pos = record_end;

                    records.push(ConsumerRecord {
                        topic: topic.to_string(),
                        partition,
                        offset: base_offset + offset_delta,
                        timestamp: first_timestamp + timestamp_delta,
                        key,
                        value,
                        headers: Headers::new(),
                    });
                }

                pos = batch_data_end;
            }

            pos = batch_end;
        }
    }

    records
}

fn read_i16(buf: &[u8], pos: &mut usize) -> i16 {
    if *pos + 2 > buf.len() { *pos = buf.len(); return 0; }
    let v = i16::from_be_bytes([buf[*pos], buf[*pos + 1]]);
    *pos += 2;
    v
}

fn read_i32(buf: &[u8], pos: &mut usize) -> i32 {
    if *pos + 4 > buf.len() { *pos = buf.len(); return 0; }
    let v = i32::from_be_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]);
    *pos += 4;
    v
}

fn read_i64(buf: &[u8], pos: &mut usize) -> i64 {
    if *pos + 8 > buf.len() { *pos = buf.len(); return 0; }
    let v = i64::from_be_bytes([
        buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3],
        buf[*pos + 4], buf[*pos + 5], buf[*pos + 6], buf[*pos + 7],
    ]);
    *pos += 8;
    v
}

/// Decode a zigzag-encoded varint from the buffer.
fn varint_decode(buf: &[u8], pos: &mut usize) -> i64 {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        if *pos >= buf.len() { return 0; }
        let byte = buf[*pos] as u64;
        *pos += 1;
        result |= (byte & 0x7F) << shift;
        if byte & 0x80 == 0 { break; }
        shift += 7;
        if shift > 63 { return 0; }
    }
    // Zigzag decode
    ((result >> 1) as i64) ^ (-((result & 1) as i64))
}

/// Build a Kafka OffsetCommit request (API key 8, version 7).
fn build_offset_commit_request(
    correlation_id: i32,
    group_id: &str,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);
    buf.extend_from_slice(&[0u8; 4]); // length placeholder

    // Request header
    buf.extend_from_slice(&8i16.to_be_bytes());      // api_key: OffsetCommit = 8
    buf.extend_from_slice(&7i16.to_be_bytes());       // api_version: 7
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    let client_id = b"streamline-rust-sdk";
    buf.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
    buf.extend_from_slice(client_id);

    // OffsetCommit request body (v7):
    // group_id + generation_id + member_id + group_instance_id + topics
    buf.extend_from_slice(&(group_id.len() as i16).to_be_bytes());
    buf.extend_from_slice(group_id.as_bytes());
    buf.extend_from_slice(&(-1i32).to_be_bytes());    // generation_id: -1 (simple consumer)
    buf.extend_from_slice(&(-1i16).to_be_bytes());    // member_id: null
    buf.extend_from_slice(&(-1i16).to_be_bytes());    // group_instance_id: null

    // topics array
    buf.extend_from_slice(&1i32.to_be_bytes());       // 1 topic
    buf.extend_from_slice(&(topic.len() as i16).to_be_bytes());
    buf.extend_from_slice(topic.as_bytes());

    // partitions array
    buf.extend_from_slice(&1i32.to_be_bytes());       // 1 partition
    buf.extend_from_slice(&partition.to_be_bytes());
    buf.extend_from_slice(&offset.to_be_bytes());
    buf.extend_from_slice(&(-1i32).to_be_bytes());    // committed_leader_epoch
    buf.extend_from_slice(&(-1i16).to_be_bytes());    // metadata: null

    // Fill length
    let total_len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&total_len.to_be_bytes());

    buf
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
    fn test_build_fetch_request_multi_partition() {
        let single = build_fetch_request(1, "test", &[(0, 0)], 1000);
        let multi = build_fetch_request(1, "test", &[(0, 0), (1, 100), (2, 200)], 1000);
        // Multi-partition request should be larger due to additional partition entries
        assert!(multi.len() > single.len());
        let len = i32::from_be_bytes([multi[0], multi[1], multi[2], multi[3]]);
        assert_eq!(len as usize, multi.len() - 4);
    }
}
