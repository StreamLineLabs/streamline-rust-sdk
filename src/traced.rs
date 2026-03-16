//! Traced wrappers for Producer and Consumer.
//!
//! [`TracedProducer`] and [`TracedConsumer`] delegate all operations to the
//! underlying producer/consumer and add tracing spans around I/O operations
//! (`send`, `send_batch`, `poll`). Non-I/O methods (transactions, commit,
//! seek, etc.) are pure passthrough with zero tracing overhead.
//!
//! # Example
//!
//! ```rust,ignore
//! use streamline_client::traced::{TracedProducer, TracedConsumer};
//! use streamline_client::Headers;
//! use std::time::Duration;
//!
//! // Wrap an existing producer
//! let traced = TracedProducer::new(producer);
//! let metadata = traced.send("orders", "key", "value", Headers::new()).await?;
//!
//! // Wrap an existing consumer
//! let mut traced = TracedConsumer::new(consumer);
//! traced.subscribe().await?;
//! let records = traced.poll(Duration::from_millis(100)).await?;
//! ```

use crate::consumer::{Consumer, ConsumerRecord};
use crate::error::Result;
use crate::producer::{Producer, ProducerRecord, RecordMetadata};
use crate::telemetry::{trace_consume, trace_produce};
use crate::{ConsumerConfig, Headers, ProducerConfig};
use std::collections::HashSet;
use std::time::Duration;

/// A producer wrapper that adds tracing spans around send operations.
///
/// All I/O methods ([`send`](Self::send), [`send_batch`](Self::send_batch))
/// are traced via [`trace_produce`]. Non-I/O methods (transactions, flush,
/// config) are pure delegation.
pub struct TracedProducer<K, V> {
    inner: Producer<K, V>,
}

impl<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send> TracedProducer<K, V> {
    /// Wraps an existing producer with tracing.
    pub fn new(producer: Producer<K, V>) -> Self {
        Self { inner: producer }
    }

    /// Sends a message with a tracing span.
    pub async fn send(
        &self,
        topic: &str,
        key: K,
        value: V,
        headers: Headers,
    ) -> Result<RecordMetadata> {
        trace_produce(topic, || self.inner.send(topic, key, value, headers)).await
    }

    /// Sends a batch of records with a tracing span.
    pub async fn send_batch(
        &self,
        topic: &str,
        records: Vec<ProducerRecord<K, V>>,
    ) -> Result<Vec<RecordMetadata>> {
        trace_produce(topic, || self.inner.send_batch(topic, records)).await
    }

    /// Begins a new transaction.
    pub fn begin_transaction(&mut self) -> Result<()> {
        self.inner.begin_transaction()
    }

    /// Buffers a record within the current transaction.
    pub fn send_transactional(
        &mut self,
        topic: &str,
        record: ProducerRecord<K, V>,
    ) -> Result<()> {
        self.inner.send_transactional(topic, record)
    }

    /// Commits the current transaction, sending all buffered records.
    pub async fn commit_transaction(&mut self) -> Result<Vec<RecordMetadata>> {
        self.inner.commit_transaction().await
    }

    /// Aborts the current transaction, discarding all buffered records.
    pub fn abort_transaction(&mut self) -> Result<()> {
        self.inner.abort_transaction()
    }

    /// Flushes any buffered messages.
    pub async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }

    /// Returns the producer configuration.
    pub fn config(&self) -> &ProducerConfig {
        self.inner.config()
    }

    /// Returns a reference to the inner producer.
    pub fn inner(&self) -> &Producer<K, V> {
        &self.inner
    }

    /// Consumes the wrapper, returning the inner producer.
    pub fn into_inner(self) -> Producer<K, V> {
        self.inner
    }
}

/// A consumer wrapper that adds tracing spans around poll operations.
///
/// The [`poll`](Self::poll) method is traced via [`trace_consume`]. All other
/// methods (subscribe, commit, seek, etc.) are pure delegation.
pub struct TracedConsumer<K, V> {
    inner: Consumer<K, V>,
}

impl<K, V> TracedConsumer<K, V> {
    /// Wraps an existing consumer with tracing.
    pub fn new(consumer: Consumer<K, V>) -> Self {
        Self { inner: consumer }
    }

    /// Subscribes to the topic.
    pub async fn subscribe(&mut self) -> Result<()> {
        self.inner.subscribe().await
    }

    /// Polls for records with a tracing span.
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<ConsumerRecord<K, V>>>
    where
        K: From<Vec<u8>>,
        V: From<Vec<u8>>,
    {
        trace_consume(self.inner.topic(), || self.inner.poll(timeout)).await
    }

    /// Commits the current offsets.
    pub async fn commit(&self) -> Result<()> {
        self.inner.commit().await
    }

    /// Commits offsets asynchronously in a background task.
    pub fn commit_async(&self)
    where
        K: Send + 'static,
        V: Send + 'static,
    {
        self.inner.commit_async()
    }

    /// Seeks to the beginning of all partitions.
    pub async fn seek_to_beginning(&self) -> Result<()> {
        self.inner.seek_to_beginning().await
    }

    /// Seeks to the end of all partitions.
    pub async fn seek_to_end(&self) -> Result<()> {
        self.inner.seek_to_end().await
    }

    /// Seeks to a specific offset for a partition.
    pub async fn seek(&self, partition: i32, offset: i64) -> Result<()> {
        self.inner.seek(partition, offset).await
    }

    /// Returns the current position for a partition.
    pub async fn position(&self, partition: i32) -> Result<i64> {
        self.inner.position(partition).await
    }

    /// Pauses consumption for the specified partitions.
    pub fn pause(&self, partitions: &[i32]) -> Result<()> {
        self.inner.pause(partitions)
    }

    /// Resumes consumption for the specified partitions.
    pub fn resume(&self, partitions: &[i32]) -> Result<()> {
        self.inner.resume(partitions)
    }

    /// Returns the set of currently paused partitions.
    pub fn paused(&self) -> Result<HashSet<i32>> {
        self.inner.paused()
    }

    /// Returns the list of assigned partitions.
    pub fn assigned_partitions(&self) -> &[i32] {
        self.inner.assigned_partitions()
    }

    /// Returns the consumer configuration.
    pub fn config(&self) -> &ConsumerConfig {
        self.inner.config()
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        self.inner.topic()
    }

    /// Returns whether the consumer is subscribed.
    pub fn is_subscribed(&self) -> bool {
        self.inner.is_subscribed()
    }

    /// Returns a reference to the inner consumer.
    pub fn inner(&self) -> &Consumer<K, V> {
        &self.inner
    }

    /// Consumes the wrapper, returning the inner consumer.
    pub fn into_inner(self) -> Consumer<K, V> {
        self.inner
    }
}
