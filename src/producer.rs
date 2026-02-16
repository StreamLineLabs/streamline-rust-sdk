//! Producer for sending messages to Streamline.

use crate::config::{ProducerConfig, StreamlineConfig};
use crate::connection::ConnectionPool;
use crate::error::Result;
use crate::Headers;
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
pub struct Producer<K, V> {
    #[allow(dead_code)]
    client_config: Arc<StreamlineConfig>,
    #[allow(dead_code)]
    pool: Arc<ConnectionPool>,
    config: ProducerConfig,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Producer<K, V> {
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
            _marker: std::marker::PhantomData,
        }
    }

    /// Sends a message to a topic.
    pub async fn send(
        &self,
        topic: &str,
        key: K,
        value: V,
        headers: Headers,
    ) -> Result<RecordMetadata> {
        debug!("Sending message to topic {}", topic);

        // TODO: Implement actual Kafka protocol message sending
        // For now, return a simulated successful response
        Ok(RecordMetadata {
            topic: topic.to_string(),
            partition: 0,
            offset: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        })
    }

    /// Sends a batch of records to a topic.
    pub async fn send_batch(
        &self,
        topic: &str,
        records: Vec<ProducerRecord<K, V>>,
    ) -> Result<Vec<RecordMetadata>> {
        debug!("Sending {} records to topic {}", records.len(), topic);

        // TODO: Implement batch sending
        let mut results = Vec::with_capacity(records.len());
        for (i, _record) in records.into_iter().enumerate() {
            results.push(RecordMetadata {
                topic: topic.to_string(),
                partition: 0,
                offset: i as i64,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            });
        }
        Ok(results)
    }

    /// Flushes any buffered messages.
    pub async fn flush(&self) -> Result<()> {
        debug!("Flushing producer");
        // TODO: Implement batching and flushing
        Ok(())
    }

    /// Returns the producer configuration.
    pub fn config(&self) -> &ProducerConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StreamlineConfig;
    use crate::connection::ConnectionPool;

    fn make_producer() -> Producer<String, String> {
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

    #[tokio::test]
    async fn test_send_returns_metadata() {
        let producer = make_producer();
        let result = producer
            .send("test-topic", "key".to_string(), "value".to_string(), Headers::new())
            .await;
        assert!(result.is_ok());
        let metadata = result.unwrap();
        assert_eq!(metadata.topic, "test-topic");
        assert_eq!(metadata.partition, 0);
    }

    #[tokio::test]
    async fn test_send_batch_returns_correct_count() {
        let producer = make_producer();
        let records = vec![
            ProducerRecord::value_only("a".to_string()),
            ProducerRecord::value_only("b".to_string()),
            ProducerRecord::value_only("c".to_string()),
        ];
        let results = producer.send_batch("topic", records).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn test_flush_succeeds() {
        let producer = make_producer();
        assert!(producer.flush().await.is_ok());
    }
}
