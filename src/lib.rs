//! # Streamline Rust Client
//!
//! Native Rust client for Streamline streaming platform.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use streamline_client::Streamline;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), streamline_client::Error> {
//!     let client = Streamline::builder()
//!         .bootstrap_servers("localhost:9092")
//!         .build()
//!         .await?;
//!
//!     // Produce a message
//!     let metadata = client.produce("my-topic", "key", "value").await?;
//!     println!("Produced at offset {}", metadata.offset);
//!
//!     Ok(())
//! }
//! ```

mod client;
mod config;
mod connection;
mod consumer;
mod error;
mod producer;
pub mod admin;
pub mod circuit_breaker;
pub mod metrics;
pub mod telemetry;

pub use client::Streamline;
pub use config::{StreamlineConfig, ConsumerConfig, ProducerConfig, TlsConfig, SaslConfig, SaslMechanism, SecurityProtocol};
pub mod schema;
pub mod query;
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitState};
pub use connection::ConnectionPool;
pub use consumer::{Consumer, ConsumerRecord};
pub use error::{Error, ErrorKind, Result};
pub use producer::{Producer, ProducerRecord, RecordMetadata};
pub use metrics::{ClientMetrics, MetricsSnapshot};
pub use admin::{Admin, TopicConfig, TopicInfo, PartitionInfo, BrokerInfo, ConsumerGroupInfo, HttpAdmin, ClusterInfo, ClusterBrokerInfo, ConsumerGroupLag, ConsumerLag, InspectedMessage, MetricPoint};

/// Message headers.
pub mod headers {
    use std::collections::HashMap;

    /// Collection of message headers.
    #[derive(Debug, Clone, Default)]
    pub struct Headers {
        inner: HashMap<String, Vec<u8>>,
    }

    impl Headers {
        /// Creates an empty headers collection.
        pub fn new() -> Self {
            Self::default()
        }

        /// Creates a builder for headers.
        pub fn builder() -> HeadersBuilder {
            HeadersBuilder::new()
        }

        /// Adds a header.
        pub fn add(&mut self, key: impl Into<String>, value: impl AsRef<[u8]>) {
            self.inner.insert(key.into(), value.as_ref().to_vec());
        }

        /// Gets a header value.
        pub fn get(&self, key: &str) -> Option<&[u8]> {
            self.inner.get(key).map(|v| v.as_slice())
        }

        /// Gets a header value as a string.
        pub fn get_str(&self, key: &str) -> Option<&str> {
            self.inner.get(key).and_then(|v| std::str::from_utf8(v).ok())
        }

        /// Returns an iterator over headers.
        pub fn iter(&self) -> impl Iterator<Item = (&String, &Vec<u8>)> {
            self.inner.iter()
        }

        /// Returns true if headers are empty.
        pub fn is_empty(&self) -> bool {
            self.inner.is_empty()
        }
    }

    /// Builder for headers.
    #[derive(Debug, Default)]
    pub struct HeadersBuilder {
        headers: Headers,
    }

    impl HeadersBuilder {
        /// Creates a new builder.
        pub fn new() -> Self {
            Self::default()
        }

        /// Adds a header.
        pub fn add(mut self, key: impl Into<String>, value: impl AsRef<[u8]>) -> Self {
            self.headers.add(key, value);
            self
        }

        /// Builds the headers.
        pub fn build(self) -> Headers {
            self.headers
        }
    }
}

pub use headers::Headers;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_headers_new_is_empty() {
        let headers = Headers::new();
        assert!(headers.is_empty());
    }

    #[test]
    fn test_headers_add_and_get() {
        let mut headers = Headers::new();
        headers.add("trace-id", b"abc-123");
        assert!(!headers.is_empty());
        assert_eq!(headers.get("trace-id"), Some(b"abc-123".as_ref()));
    }

    #[test]
    fn test_headers_get_str() {
        let mut headers = Headers::new();
        headers.add("key", b"value");
        assert_eq!(headers.get_str("key"), Some("value"));
    }

    #[test]
    fn test_headers_get_nonexistent() {
        let headers = Headers::new();
        assert!(headers.get("missing").is_none());
        assert!(headers.get_str("missing").is_none());
    }

    #[test]
    fn test_headers_builder() {
        let headers = Headers::builder()
            .add("k1", b"v1")
            .add("k2", b"v2")
            .build();
        assert_eq!(headers.get_str("k1"), Some("v1"));
        assert_eq!(headers.get_str("k2"), Some("v2"));
    }

    #[test]
    fn test_headers_iter() {
        let headers = Headers::builder()
            .add("a", b"1")
            .add("b", b"2")
            .build();
        let count = headers.iter().count();
        assert_eq!(count, 2);
    }
}

