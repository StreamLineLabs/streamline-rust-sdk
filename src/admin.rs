//! Administrative operations for Streamline.
//!
//! The Kafka-protocol [`Admin`] surface is reserved for topic management,
//! consumer group inspection, and cluster metadata operations. In version
//! 0.4.0 these methods return [`ErrorKind::Unsupported`] without opening a
//! broker connection. The `http-admin` feature provides implemented HTTP
//! operations.
//!
//! # Example
//!
//! ```rust,no_run
//! use streamline_client::Streamline;
//! use streamline_client::ErrorKind;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), streamline_client::Error> {
//!     let client = Streamline::builder()
//!         .bootstrap_servers("localhost:9092")
//!         .build()
//!         .await?;
//!
//!     let error = client.admin().list_topics().await.unwrap_err();
//!     assert_eq!(error.kind, ErrorKind::Unsupported);
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
/// Reserved Kafka-protocol admin surface.
///
/// Version 0.4.0 returns [`ErrorKind::Unsupported`] from every operation
/// without opening a broker connection. Obtained via
/// [`Streamline::admin()`](crate::Streamline::admin).
pub struct Admin {
    _config: Arc<StreamlineConfig>,
    pool: Arc<ConnectionPool>,
}

impl Admin {
    pub(crate) fn new(config: Arc<StreamlineConfig>, pool: Arc<ConnectionPool>) -> Self {
        Self {
            _config: config,
            pool,
        }
    }

    /// Creates a new topic.
    pub async fn create_topic(&self, config: TopicConfig) -> Result<()> {
        crate::validation::validate_topic_name(&config.name)?;
        if config.num_partitions < 1 {
            return Err(Error::new(
                ErrorKind::InvalidConfiguration,
                "Number of partitions must be at least 1",
            ));
        }
        self.pool
            .create_topic(
                &config.name,
                config.num_partitions,
                config.replication_factor,
                &config.config,
            )
            .await
    }

    /// Deletes a topic.
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        crate::validation::validate_topic_name(name)?;
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

// ── HTTP-based Admin Operations ─────────────────────────────────────────────
// These methods communicate via the Streamline HTTP REST API for operations
// not available through the Kafka wire protocol.

/// Consumer lag information for a single partition.
#[cfg(feature = "http-admin")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ConsumerLag {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Current consumer offset.
    pub current_offset: i64,
    /// End (log-end) offset.
    pub end_offset: i64,
    /// Offset lag.
    pub lag: i64,
}

/// Aggregated consumer group lag.
#[cfg(feature = "http-admin")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ConsumerGroupLag {
    /// Consumer group ID.
    pub group_id: String,
    /// Per-partition lag details.
    pub partitions: Vec<ConsumerLag>,
    /// Total lag across all partitions.
    pub total_lag: i64,
}

/// Cluster overview information.
#[cfg(feature = "http-admin")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ClusterInfo {
    /// Cluster identifier.
    pub cluster_id: String,
    /// ID of the responding broker.
    pub broker_id: i32,
    /// List of brokers in the cluster.
    pub brokers: Vec<ClusterBrokerInfo>,
    /// ID of the controller broker.
    pub controller: i32,
}

/// Broker information from the cluster info endpoint.
#[cfg(feature = "http-admin")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ClusterBrokerInfo {
    /// Broker ID.
    pub id: i32,
    /// Broker hostname.
    pub host: String,
    /// Broker port.
    pub port: i32,
    /// Optional rack identifier.
    pub rack: Option<String>,
}

/// A message returned by the inspection API.
#[cfg(feature = "http-admin")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct InspectedMessage {
    /// Message offset.
    pub offset: i64,
    /// Message key (optional).
    pub key: Option<String>,
    /// Message value.
    pub value: String,
    /// Message timestamp.
    pub timestamp: i64,
    /// Partition index.
    pub partition: i32,
    /// Message headers.
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

/// A single metric data point.
#[cfg(feature = "http-admin")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct MetricPoint {
    /// Metric name.
    pub name: String,
    /// Metric value.
    pub value: f64,
    /// Metric labels.
    #[serde(default)]
    pub labels: HashMap<String, String>,
    /// Metric timestamp.
    pub timestamp: i64,
}

/// Information about a copy-on-write topic branch (M5, Experimental).
#[cfg(feature = "http-admin")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BranchInfo {
    /// Branch name.
    pub name: String,
    /// The base topic this branch forks from.
    pub base_topic: String,
    /// Branch state (`active`, `discarded`, `merged`).
    #[serde(default = "default_branch_state")]
    pub state: String,
    /// Creation timestamp (epoch milliseconds).
    #[serde(default)]
    pub created_at: u64,
}

#[cfg(feature = "http-admin")]
fn default_branch_state() -> String {
    "active".to_string()
}

/// HTTP-based admin client for expanded operations.
///
/// Communicates with the Streamline HTTP REST API (default port 9094)
/// for operations like cluster info, consumer lag monitoring, message
/// inspection, and metrics history.
///
/// # Example
///
/// ```rust,no_run
/// use streamline_client::admin::HttpAdmin;
///
/// #[tokio::main]
/// async fn main() -> Result<(), streamline_client::Error> {
///     let admin = HttpAdmin::new("http://localhost:9094");
///     let info = admin.cluster_info().await?;
///     println!("Cluster: {}", info.cluster_id);
///     Ok(())
/// }
/// ```
#[cfg(feature = "http-admin")]
pub struct HttpAdmin {
    base_url: String,
    client: reqwest::Client,
}

#[cfg(feature = "http-admin")]
impl HttpAdmin {
    /// Creates a new HTTP admin client.
    ///
    /// The base URL is validated when a request is built, so an invalid or
    /// non-HTTP base surfaces as [`ErrorKind::InvalidConfiguration`] from the
    /// call rather than being silently concatenated into a request target.
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
        }
    }

    /// Returns the configured base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Builds a request URL from percent-encoded path segments and query
    /// parameters. Dot segments and empty segments are rejected.
    fn url(&self, segments: &[&str], query: &[(&str, String)]) -> Result<reqwest::Url> {
        crate::http_url::build_url(&self.base_url, segments, query)
    }

    /// Returns cluster overview including broker list.
    pub async fn cluster_info(&self) -> Result<ClusterInfo> {
        self.get(self.url(&["v1", "cluster"], &[])?).await
    }

    /// Returns consumer lag for a specific consumer group.
    pub async fn consumer_group_lag(&self, group_id: &str) -> Result<ConsumerGroupLag> {
        crate::http_url::validate_path_segment("Consumer group ID", group_id)?;
        self.get(self.url(&["v1", "consumer-groups", group_id, "lag"], &[])?)
            .await
    }

    /// Returns consumer lag for a specific topic within a group.
    pub async fn consumer_group_topic_lag(
        &self,
        group_id: &str,
        topic: &str,
    ) -> Result<ConsumerGroupLag> {
        crate::http_url::validate_path_segment("Consumer group ID", group_id)?;
        crate::validation::validate_topic_name(topic)?;
        self.get(self.url(&["v1", "consumer-groups", group_id, "lag", topic], &[])?)
            .await
    }

    /// Browses messages from a topic partition.
    pub async fn inspect_messages(
        &self,
        topic: &str,
        partition: i32,
        offset: Option<i64>,
        limit: usize,
    ) -> Result<Vec<InspectedMessage>> {
        crate::validation::validate_topic_name(topic)?;
        let mut query = vec![
            ("partition", partition.to_string()),
            ("limit", limit.to_string()),
        ];
        if let Some(offset) = offset {
            query.push(("offset", offset.to_string()));
        }
        self.get(self.url(&["v1", "inspect", topic], &query)?).await
    }

    /// Returns the most recent messages from a topic.
    pub async fn latest_messages(
        &self,
        topic: &str,
        count: usize,
    ) -> Result<Vec<InspectedMessage>> {
        crate::validation::validate_topic_name(topic)?;
        self.get(self.url(
            &["v1", "inspect", topic, "latest"],
            &[("count", count.to_string())],
        )?)
        .await
    }

    /// Returns metrics history from the server.
    pub async fn metrics_history(&self) -> Result<Vec<MetricPoint>> {
        self.get(self.url(&["v1", "metrics", "history"], &[])?)
            .await
    }

    /// Creates a copy-on-write branch of a topic (M5).
    pub async fn create_branch(
        &self,
        name: &str,
        base_topic: &str,
        base_offsets: Option<&HashMap<i32, i64>>,
    ) -> Result<BranchInfo> {
        crate::http_url::validate_path_segment("Branch name", name)?;
        crate::validation::validate_topic_name(base_topic)?;
        let mut body = serde_json::json!({
            "name": name,
            "base_topic": base_topic,
        });
        if let Some(offsets) = base_offsets {
            body["base_offsets"] = serde_json::to_value(offsets).map_err(|error| {
                Error::new(
                    ErrorKind::Serialization,
                    format!("Failed to encode base offsets: {error}"),
                )
            })?;
        }
        self.post(self.url(&["v1", "branches"], &[])?, &body).await
    }

    /// Lists copy-on-write topic branches (M5).
    pub async fn list_branches(&self, topic: Option<&str>) -> Result<Vec<BranchInfo>> {
        let query = match topic {
            Some(topic) => {
                crate::validation::validate_topic_name(topic)?;
                vec![("topic", topic.to_string())]
            }
            None => Vec::new(),
        };
        self.get(self.url(&["v1", "branches"], &query)?).await
    }

    /// Discards (deletes) a copy-on-write topic branch (M5).
    pub async fn discard_branch(&self, branch_id: &str) -> Result<()> {
        crate::http_url::validate_path_segment("Branch ID", branch_id)?;
        self.delete(self.url(&["v1", "branches", branch_id], &[])?)
            .await
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, url: reqwest::Url) -> Result<T> {
        let resp = self.client.get(url).send().await.map_err(|e| {
            Error::new(ErrorKind::Connection, format!("HTTP request failed: {}", e))
        })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::new(
                ErrorKind::Server,
                format!("HTTP {}: {}", status, body),
            ));
        }

        resp.json().await.map_err(|e| {
            Error::new(
                ErrorKind::Serialization,
                format!("JSON decode failed: {}", e),
            )
        })
    }

    async fn post<T: serde::de::DeserializeOwned, B: serde::Serialize>(
        &self,
        url: reqwest::Url,
        body: &B,
    ) -> Result<T> {
        let resp = self.client.post(url).json(body).send().await.map_err(|e| {
            Error::new(ErrorKind::Connection, format!("HTTP request failed: {}", e))
        })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::new(
                ErrorKind::Server,
                format!("HTTP {}: {}", status, text),
            ));
        }

        resp.json().await.map_err(|e| {
            Error::new(
                ErrorKind::Serialization,
                format!("JSON decode failed: {}", e),
            )
        })
    }

    async fn delete(&self, url: reqwest::Url) -> Result<()> {
        let resp = self.client.delete(url).send().await.map_err(|e| {
            Error::new(ErrorKind::Connection, format!("HTTP request failed: {}", e))
        })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::new(
                ErrorKind::Server,
                format!("HTTP {}: {}", status, text),
            ));
        }

        Ok(())
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

    #[cfg(feature = "http-admin")]
    #[test]
    fn test_branch_info() {
        let info = BranchInfo {
            name: "experiment-a".to_string(),
            base_topic: "orders".to_string(),
            state: "active".to_string(),
            created_at: 1700000000000,
        };
        assert_eq!(info.name, "experiment-a");
        assert_eq!(info.base_topic, "orders");
        assert_eq!(info.state, "active");
    }

    #[cfg(feature = "http-admin")]
    #[test]
    fn test_http_admin_encodes_reserved_characters_in_path() {
        let admin = HttpAdmin::new("http://localhost:9094");
        let url = admin
            .url(
                &["v1", "consumer-groups", "grp/../secret?x=1#f", "lag"],
                &[],
            )
            .unwrap();
        assert_eq!(
            url.as_str(),
            "http://localhost:9094/v1/consumer-groups/grp%2F..%2Fsecret%3Fx=1%23f/lag"
        );
        assert!(url.query().is_none());
        assert_eq!(url.path_segments().unwrap().count(), 4);
    }

    #[cfg(feature = "http-admin")]
    #[test]
    fn test_http_admin_encodes_query_parameters() {
        let admin = HttpAdmin::new("http://localhost:9094");
        let url = admin
            .url(
                &["v1", "inspect", "events"],
                &[
                    ("partition", "0".to_string()),
                    ("cursor", "a&b=c /d".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(url.path(), "/v1/inspect/events");
        assert_eq!(url.query(), Some("partition=0&cursor=a%26b%3Dc+%2Fd"));
    }

    #[cfg(feature = "http-admin")]
    #[tokio::test]
    async fn test_http_admin_rejects_dot_segments_before_any_request() {
        // Base URL points at an unroutable address: a rejected argument must
        // fail before any network attempt, so these return immediately.
        let admin = HttpAdmin::new("http://192.0.2.1:9094");

        for id in [".", ".."] {
            let error = admin.discard_branch(id).await.unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidConfiguration);

            let error = admin.consumer_group_lag(id).await.unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidConfiguration);

            let error = admin.latest_messages(id, 10).await.unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidConfiguration);

            let error = admin.inspect_messages(id, 0, None, 10).await.unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidConfiguration);
        }

        let error = admin.consumer_group_lag("").await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidConfiguration);
    }

    #[cfg(feature = "http-admin")]
    #[tokio::test]
    async fn test_http_admin_rejects_invalid_base_url() {
        let admin = HttpAdmin::new("not-a-url");
        let error = admin.cluster_info().await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidConfiguration);
    }
}
