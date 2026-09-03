//! Traced wrappers for Producer and Consumer.
//!
//! [`TracedProducer`] and [`TracedConsumer`] delegate all operations to the
//! underlying producer/consumer and add tracing spans around I/O operations
//! (`send`, `send_batch`, `poll`). Other methods are passthroughs, including
//! the explicit unsupported errors returned for transactions, consumer group
//! commits, and latest-offset lookup.
//!
//! # Example
//!
//! ```rust,no_run
//! use streamline_client::traced::{TracedProducer, TracedConsumer};
//! use streamline_client::{Headers, Streamline};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), streamline_client::Error> {
//!     let client = Streamline::builder()
//!         .bootstrap_servers("localhost:9092")
//!         .build()
//!         .await?;
//!
//!     let producer = client.producer::<String, String>();
//!     let traced = TracedProducer::new(producer);
//!     traced
//!         .send(
//!             "orders",
//!             "key".to_string(),
//!             "value".to_string(),
//!             Headers::new(),
//!         )
//!         .await?;
//!
//!     let consumer = client
//!         .consumer::<Vec<u8>, Vec<u8>>("events")
//!         .partitions(vec![0])
//!         .build()
//!         .await?;
//!     let mut traced = TracedConsumer::new(consumer);
//!     traced.subscribe().await?;
//!     let _records = traced.poll(Duration::from_millis(100)).await?;
//!     Ok(())
//! }
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
/// are traced via [`trace_produce`]. Other methods are pure delegation.
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

    /// Returns an unsupported error because transactions are not implemented.
    pub fn begin_transaction(&mut self) -> Result<()> {
        self.inner.begin_transaction()
    }

    /// Returns an unsupported error without buffering the record.
    pub fn send_transactional(&mut self, topic: &str, record: ProducerRecord<K, V>) -> Result<()> {
        self.inner.send_transactional(topic, record)
    }

    /// Returns an unsupported error without sending records.
    pub async fn commit_transaction(&mut self) -> Result<Vec<RecordMetadata>> {
        self.inner.commit_transaction().await
    }

    /// Returns an unsupported error because transactions are not implemented.
    pub fn abort_transaction(&mut self) -> Result<()> {
        self.inner.abort_transaction()
    }

    /// Flushes any buffered messages.
    pub async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await
    }

    /// Gracefully shuts down the producer.
    pub async fn close(&mut self) -> Result<()> {
        self.inner.close().await
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

    /// Returns an unsupported error because consumer groups are not implemented.
    pub async fn commit(&self) -> Result<()> {
        self.inner.commit().await
    }

    /// Returns an unsupported error because consumer groups are not implemented.
    pub fn commit_async(&self) -> Result<()> {
        self.inner.commit_async()
    }

    /// Seeks to the beginning of all partitions.
    pub async fn seek_to_beginning(&self) -> Result<()> {
        self.inner.seek_to_beginning().await
    }

    /// Returns an unsupported error because latest-offset lookup is not implemented.
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
