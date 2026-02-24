//! Administrative operations for Streamline.
//!
//! The [`Admin`] client provides topic management, consumer group
//! inspection, and cluster metadata operations.
//!
//! # Example
//!
//! ```rust,no_run
//! use streamline_client::Streamline;
//! use streamline_client::admin::{Admin, TopicConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), streamline_client::Error> {
//!     let client = Streamline::builder()
//!         .bootstrap_servers("localhost:9092")
//!         .build()
//!         .await?;
//!
//!     let admin = client.admin();
//!
//!     // Create a topic
//!     admin.create_topic(TopicConfig {
//!         name: "events".to_string(),
//!         num_partitions: 3,
//!         replication_factor: 1,
//!         config: Default::default(),
//!     }).await?;
//!
//!     // List topics
//!     let topics = admin.list_topics().await?;
//!     for topic in &topics {
//!         println!("{} ({} partitions)", topic.name, topic.partitions);
//!     }
//!
//!     Ok(())
//! }
//! ```

use crate::config::StreamlineConfig;
use crate::connection::ConnectionPool;
use crate::error::{Error, ErrorKind, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration for creating a topic.
#[derive(Debug, Clone)]
pub struct TopicConfig {
    /// Topic name.
    pub name: String,
    /// Number of partitions.
    pub num_partitions: i32,
    /// Replication factor.
    pub replication_factor: i16,
    /// Additional topic configuration entries.
    pub config: HashMap<String, String>,
}

impl TopicConfig {
    /// Creates a new topic config with defaults (3 partitions, replication factor 1).
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            num_partitions: 3,
            replication_factor: 1,
            config: HashMap::new(),
        }
    }

    /// Sets the number of partitions.
    pub fn partitions(mut self, n: i32) -> Self {
        self.num_partitions = n;
        self
    }

    /// Sets the replication factor.
    pub fn replication_factor(mut self, n: i16) -> Self {
        self.replication_factor = n;
        self
    }

    /// Adds a configuration entry.
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.insert(key.into(), value.into());
        self
    }
}

/// Information about a topic.
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// Topic name.
    pub name: String,
    /// Number of partitions.
    pub partitions: i32,
    /// Replication factor.
    pub replication_factor: i16,
    /// Whether this is an internal topic.
    pub internal: bool,
}

/// Information about a topic partition.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Partition ID.
    pub id: i32,
    /// Leader broker ID.
    pub leader: i32,
    /// Replica broker IDs.
    pub replicas: Vec<i32>,
    /// In-sync replica broker IDs.
    pub isr: Vec<i32>,
}

/// Information about a broker node.
#[derive(Debug, Clone)]
pub struct BrokerInfo {
    /// Broker ID.
    pub id: i32,
    /// Hostname.
    pub host: String,
    /// Port.
    pub port: i32,
    /// Rack identifier (if configured).
    pub rack: Option<String>,
}

/// Information about a consumer group.
#[derive(Debug, Clone)]
pub struct ConsumerGroupInfo {
    /// Group ID.
    pub group_id: String,
    /// Group state (e.g., "Stable", "PreparingRebalance").
    pub state: String,
    /// Number of members.
    pub members: usize,
}

/// Administrative client for Streamline cluster operations.
///
/// Provides topic management, consumer group inspection, and cluster
/// metadata queries. Obtained via [`Streamline::admin()`](crate::Streamline::admin).
pub struct Admin {
    _config: Arc<StreamlineConfig>,
    pool: Arc<ConnectionPool>,
}

impl Admin {
    pub(crate) fn new(config: Arc<StreamlineConfig>, pool: Arc<ConnectionPool>) -> Self {
        Self { _config: config, pool: pool }
    }

    /// Creates a new topic.
    pub async fn create_topic(&self, config: TopicConfig) -> Result<()> {
        if config.name.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidConfiguration,
                "Topic name cannot be empty",
            ));
        }
        if config.num_partitions < 1 {
            return Err(Error::new(
                ErrorKind::InvalidConfiguration,
                "Number of partitions must be at least 1",
            ));
        }
        self.pool.create_topic(&config.name, config.num_partitions, config.replication_factor, &config.config).await
    }

    /// Deletes a topic.
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidConfiguration,
                "Topic name cannot be empty",
            ));
        }
        self.pool.delete_topic(name).await
    }

    /// Lists all topics.
    pub async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        self.pool.list_topics().await
    }

    /// Describes a topic, returning partition-level details.
    pub async fn describe_topic(&self, name: &str) -> Result<(TopicInfo, Vec<PartitionInfo>)> {
        self.pool.describe_topic(name).await
    }

    /// Adds partitions to a topic.
    pub async fn add_partitions(&self, name: &str, total_count: i32) -> Result<()> {
        if total_count < 1 {
            return Err(Error::new(
                ErrorKind::InvalidConfiguration,
                "Partition count must be at least 1",
            ));
        }
        self.pool.add_partitions(name, total_count).await
    }

    /// Lists all consumer groups.
    pub async fn list_consumer_groups(&self) -> Result<Vec<String>> {
        self.pool.list_consumer_groups().await
    }

    /// Describes a consumer group.
    pub async fn describe_consumer_group(&self, group_id: &str) -> Result<ConsumerGroupInfo> {
        self.pool.describe_consumer_group(group_id).await
    }

    /// Deletes a consumer group.
    pub async fn delete_consumer_group(&self, group_id: &str) -> Result<()> {
        self.pool.delete_consumer_group(group_id).await
    }

    /// Lists brokers in the cluster.
    pub async fn list_brokers(&self) -> Result<Vec<BrokerInfo>> {
        self.pool.list_brokers().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_config_new() {
        let config = TopicConfig::new("events");
        assert_eq!(config.name, "events");
        assert_eq!(config.num_partitions, 3);
        assert_eq!(config.replication_factor, 1);
        assert!(config.config.is_empty());
    }

    #[test]
    fn test_topic_config_builder() {
        let config = TopicConfig::new("orders")
            .partitions(6)
            .replication_factor(3)
            .config("retention.ms", "86400000");
        assert_eq!(config.name, "orders");
        assert_eq!(config.num_partitions, 6);
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.config.get("retention.ms").unwrap(), "86400000");
    }

    #[test]
    fn test_topic_info_debug() {
        let info = TopicInfo {
            name: "test".to_string(),
            partitions: 3,
            replication_factor: 1,
            internal: false,
        };
        assert_eq!(info.name, "test");
        assert!(!info.internal);
    }

    #[test]
    fn test_partition_info() {
        let info = PartitionInfo {
            id: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
        };
        assert_eq!(info.id, 0);
        assert_eq!(info.replicas.len(), 3);
        assert_eq!(info.isr.len(), 2);
    }

    #[test]
    fn test_broker_info() {
        let info = BrokerInfo {
            id: 1,
            host: "broker-1".to_string(),
            port: 9092,
            rack: Some("us-east-1a".to_string()),
        };
        assert_eq!(info.id, 1);
        assert_eq!(info.rack.as_deref(), Some("us-east-1a"));
    }

    #[test]
    fn test_consumer_group_info() {
        let info = ConsumerGroupInfo {
            group_id: "my-group".to_string(),
            state: "Stable".to_string(),
            members: 3,
        };
        assert_eq!(info.group_id, "my-group");
        assert_eq!(info.members, 3);
    }
}
