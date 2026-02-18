//! # Streamline Testcontainers
//!
//! [Testcontainers](https://testcontainers.org/) module for
//! [Streamline](https://github.com/streamlinelabs/streamline) -- **The Redis of Streaming**.
//!
//! Streamline is a Kafka-protocol-compatible, single-binary streaming platform that starts in
//! milliseconds, uses less than 50 MB of memory, and requires zero configuration. This crate
//! makes it trivial to spin up a disposable Streamline server inside Rust integration tests.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use streamline_testcontainers::StreamlineImage;
//! use testcontainers::runners::AsyncRunner;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Start a Streamline container with default settings.
//! let container = StreamlineImage::default().start().await?;
//!
//! // Obtain the Kafka-compatible bootstrap servers address.
//! let host = container.get_host().await?;
//! let port = container.get_host_port_ipv4(StreamlineImage::KAFKA_PORT).await?;
//! let bootstrap_servers = format!("{host}:{port}");
//!
//! // ... use `bootstrap_servers` with any Kafka client crate ...
//! # Ok(())
//! # }
//! ```
//!
//! # Builder pattern
//!
//! Use [`StreamlineImageBuilder`] for full control over the container configuration:
//!
//! ```rust,no_run
//! use streamline_testcontainers::StreamlineImage;
//!
//! let image = StreamlineImage::builder()
//!     .tag("0.2.0")
//!     .log_level("debug")
//!     .playground(true)
//!     .in_memory(true)
//!     .env("STREAMLINE_CUSTOM_KEY", "custom-value")
//!     .build();
//! ```
//!
//! # Features
//!
//! - Implements [`testcontainers::Image`] so you can use the standard `start()` / `AsyncRunner`
//!   workflow.
//! - Exposes Kafka (9092) and HTTP (9094) ports.
//! - Waits for the server to emit `"Server started"` on stdout before returning.
//! - Configurable via environment variables (log level, in-memory mode, playground mode).
//! - Optional `client` feature re-exports the `streamline-client` crate.

use std::borrow::Cow;
use std::collections::HashMap;

use testcontainers::core::WaitFor;
use testcontainers::Image;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default Docker image name.
const DEFAULT_IMAGE_NAME: &str = "ghcr.io/streamlinelabs/streamline";

/// Default image tag.
const DEFAULT_TAG: &str = "latest";

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors that can occur when building or interacting with a Streamline container.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An invalid configuration value was supplied.
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, Error>;

// ---------------------------------------------------------------------------
// StreamlineImage
// ---------------------------------------------------------------------------

/// A [`testcontainers::Image`] implementation for the Streamline server.
///
/// The image is published at `ghcr.io/streamlinelabs/streamline` and exposes two ports:
///
/// | Port | Protocol | Purpose |
/// |------|----------|---------|
/// | 9092 | Kafka    | Client connections (produce / consume) |
/// | 9094 | HTTP     | Health checks, metrics, admin API |
///
/// # Examples
///
/// ```rust,no_run
/// use streamline_testcontainers::StreamlineImage;
/// use testcontainers::runners::AsyncRunner;
///
/// # async fn run() -> std::result::Result<(), Box<dyn std::error::Error>> {
/// let container = StreamlineImage::default().start().await?;
/// let host = container.get_host().await?;
/// let kafka_port = container.get_host_port_ipv4(StreamlineImage::KAFKA_PORT).await?;
/// println!("Kafka available at {host}:{kafka_port}");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct StreamlineImage {
    tag: String,
    env_vars: HashMap<String, String>,
}

impl StreamlineImage {
    /// Kafka protocol port exposed by the container.
    pub const KAFKA_PORT: u16 = 9092;

    /// HTTP API port exposed by the container.
    pub const HTTP_PORT: u16 = 9094;

    /// Creates a new [`StreamlineImageBuilder`].
    pub fn builder() -> StreamlineImageBuilder {
        StreamlineImageBuilder::default()
    }
}

impl Default for StreamlineImage {
    fn default() -> Self {
        Self::builder().build()
    }
}

// ---------------------------------------------------------------------------
// Image trait
// ---------------------------------------------------------------------------

impl Image for StreamlineImage {
    fn name(&self) -> &str {
        DEFAULT_IMAGE_NAME
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("Server started")]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        self.env_vars.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    fn expose_ports(&self) -> &[testcontainers::core::ContainerPort] {
        &[
            testcontainers::core::ContainerPort::Tcp(Self::KAFKA_PORT),
            testcontainers::core::ContainerPort::Tcp(Self::HTTP_PORT),
        ]
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builder for [`StreamlineImage`].
///
/// Provides a fluent API for configuring the Streamline container image before starting it.
///
/// # Examples
///
/// ```rust
/// use streamline_testcontainers::StreamlineImage;
///
/// let image = StreamlineImage::builder()
///     .tag("0.2.0")
///     .log_level("debug")
///     .playground(true)
///     .in_memory(true)
///     .env("MY_VAR", "my_value")
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct StreamlineImageBuilder {
    tag: String,
    env_vars: HashMap<String, String>,
}

impl Default for StreamlineImageBuilder {
    fn default() -> Self {
        let mut env_vars = HashMap::new();
        env_vars.insert(
            "STREAMLINE_LISTEN_ADDR".to_string(),
            format!("0.0.0.0:{}", StreamlineImage::KAFKA_PORT),
        );
        env_vars.insert(
            "STREAMLINE_HTTP_ADDR".to_string(),
            format!("0.0.0.0:{}", StreamlineImage::HTTP_PORT),
        );

        Self {
            tag: DEFAULT_TAG.to_string(),
            env_vars,
        }
    }
}

impl StreamlineImageBuilder {
    /// Sets the Docker image tag (e.g. `"0.2.0"`, `"latest"`).
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = tag.into();
        self
    }

    /// Sets the Streamline server log level.
    ///
    /// Valid values: `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`.
    pub fn log_level(mut self, level: impl Into<String>) -> Self {
        self.env_vars
            .insert("STREAMLINE_LOG_LEVEL".to_string(), level.into());
        self
    }

    /// Enables debug logging (shorthand for `.log_level("debug")`).
    pub fn debug_logging(self) -> Self {
        self.log_level("debug")
    }

    /// Enables trace logging (shorthand for `.log_level("trace")`).
    pub fn trace_logging(self) -> Self {
        self.log_level("trace")
    }

    /// Enables playground mode, which pre-loads demo topics.
    pub fn playground(mut self, enabled: bool) -> Self {
        if enabled {
            self.env_vars
                .insert("STREAMLINE_PLAYGROUND".to_string(), "true".to_string());
        } else {
            self.env_vars.remove("STREAMLINE_PLAYGROUND");
        }
        self
    }

    /// Enables in-memory storage mode (no disk persistence).
    pub fn in_memory(mut self, enabled: bool) -> Self {
        if enabled {
            self.env_vars
                .insert("STREAMLINE_IN_MEMORY".to_string(), "true".to_string());
        } else {
            self.env_vars.remove("STREAMLINE_IN_MEMORY");
        }
        self
    }

    /// Adds an arbitrary environment variable to the container.
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env_vars.insert(key.into(), value.into());
        self
    }

    /// Consumes the builder and returns the configured [`StreamlineImage`].
    pub fn build(self) -> StreamlineImage {
        StreamlineImage {
            tag: self.tag,
            env_vars: self.env_vars,
        }
    }
}

// ---------------------------------------------------------------------------
// Convenience helpers
// ---------------------------------------------------------------------------

/// Extension methods for a running Streamline container.
///
/// These are free functions that accept a `ContainerAsync<StreamlineImage>` reference so that
/// callers do not need to compute host/port strings manually.
///
/// # Examples
///
/// ```rust,no_run
/// use streamline_testcontainers::{StreamlineImage, bootstrap_servers, http_url};
/// use testcontainers::runners::AsyncRunner;
///
/// # async fn run() -> std::result::Result<(), Box<dyn std::error::Error>> {
/// let container = StreamlineImage::default().start().await?;
/// let bs = bootstrap_servers(&container).await?;
/// let url = http_url(&container).await?;
/// println!("Kafka: {bs}  HTTP: {url}");
/// # Ok(())
/// # }
/// ```
pub async fn bootstrap_servers(
    container: &testcontainers::ContainerAsync<StreamlineImage>,
) -> std::result::Result<String, testcontainers::TestcontainersError> {
    let host = container.get_host().await?;
    let port = container
        .get_host_port_ipv4(StreamlineImage::KAFKA_PORT)
        .await?;
    Ok(format!("{host}:{port}"))
}

/// Returns the HTTP API base URL for a running Streamline container.
pub async fn http_url(
    container: &testcontainers::ContainerAsync<StreamlineImage>,
) -> std::result::Result<String, testcontainers::TestcontainersError> {
    let host = container.get_host().await?;
    let port = container
        .get_host_port_ipv4(StreamlineImage::HTTP_PORT)
        .await?;
    Ok(format!("http://{host}:{port}"))
}

/// Returns the health check endpoint URL for a running Streamline container.
pub async fn health_url(
    container: &testcontainers::ContainerAsync<StreamlineImage>,
) -> std::result::Result<String, testcontainers::TestcontainersError> {
    let base = http_url(container).await?;
    Ok(format!("{base}/health"))
}

/// Returns the Prometheus metrics endpoint URL for a running Streamline container.
pub async fn metrics_url(
    container: &testcontainers::ContainerAsync<StreamlineImage>,
) -> std::result::Result<String, testcontainers::TestcontainersError> {
    let base = http_url(container).await?;
    Ok(format!("{base}/metrics"))
}

/// Returns the server info endpoint URL for a running Streamline container.
pub async fn info_url(
    container: &testcontainers::ContainerAsync<StreamlineImage>,
) -> std::result::Result<String, testcontainers::TestcontainersError> {
    let base = http_url(container).await?;
    Ok(format!("{base}/info"))
}

// ---------------------------------------------------------------------------
// Re-export the client crate when the `client` feature is enabled.
// ---------------------------------------------------------------------------

#[cfg(feature = "client")]
pub use streamline_client;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Unit tests (no Docker required) ------------------------------------

    #[test]
    fn default_image_has_expected_name_and_tag() {
        let image = StreamlineImage::default();
        assert_eq!(image.name(), DEFAULT_IMAGE_NAME);
        assert_eq!(image.tag(), DEFAULT_TAG);
    }

    #[test]
    fn builder_sets_custom_tag() {
        let image = StreamlineImage::builder().tag("0.2.0").build();
        assert_eq!(image.tag(), "0.2.0");
    }

    #[test]
    fn builder_sets_log_level() {
        let image = StreamlineImage::builder().log_level("debug").build();
        assert_eq!(
            image
                .env_vars
                .get("STREAMLINE_LOG_LEVEL")
                .map(String::as_str),
            Some("debug")
        );
    }

    #[test]
    fn debug_logging_shorthand() {
        let image = StreamlineImage::builder().debug_logging().build();
        assert_eq!(
            image
                .env_vars
                .get("STREAMLINE_LOG_LEVEL")
                .map(String::as_str),
            Some("debug")
        );
    }

    #[test]
    fn trace_logging_shorthand() {
        let image = StreamlineImage::builder().trace_logging().build();
        assert_eq!(
            image
                .env_vars
                .get("STREAMLINE_LOG_LEVEL")
                .map(String::as_str),
            Some("trace")
        );
    }

    #[test]
    fn builder_enables_playground_mode() {
        let image = StreamlineImage::builder().playground(true).build();
        assert_eq!(
            image
                .env_vars
                .get("STREAMLINE_PLAYGROUND")
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn builder_disables_playground_mode() {
        let image = StreamlineImage::builder()
            .playground(true)
            .playground(false)
            .build();
        assert!(!image.env_vars.contains_key("STREAMLINE_PLAYGROUND"));
    }

    #[test]
    fn builder_enables_in_memory_mode() {
        let image = StreamlineImage::builder().in_memory(true).build();
        assert_eq!(
            image
                .env_vars
                .get("STREAMLINE_IN_MEMORY")
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn builder_disables_in_memory_mode() {
        let image = StreamlineImage::builder()
            .in_memory(true)
            .in_memory(false)
            .build();
        assert!(!image.env_vars.contains_key("STREAMLINE_IN_MEMORY"));
    }

    #[test]
    fn builder_adds_custom_env_var() {
        let image = StreamlineImage::builder().env("MY_KEY", "my_value").build();
        assert_eq!(
            image.env_vars.get("MY_KEY").map(String::as_str),
            Some("my_value")
        );
    }

    #[test]
    fn default_env_vars_contain_listen_and_http_addrs() {
        let image = StreamlineImage::default();
        assert_eq!(
            image
                .env_vars
                .get("STREAMLINE_LISTEN_ADDR")
                .map(String::as_str),
            Some("0.0.0.0:9092")
        );
        assert_eq!(
            image
                .env_vars
                .get("STREAMLINE_HTTP_ADDR")
                .map(String::as_str),
            Some("0.0.0.0:9094")
        );
    }

    #[test]
    fn ready_conditions_waits_for_server_started() {
        let image = StreamlineImage::default();
        let conditions = image.ready_conditions();
        assert_eq!(conditions.len(), 1);
        // We cannot inspect the inner value of WaitFor::Log directly,
        // but we can verify exactly one condition is returned.
    }

    #[test]
    fn expose_ports_contains_kafka_and_http() {
        let image = StreamlineImage::default();
        let ports = image.expose_ports();
        assert_eq!(ports.len(), 2);
        assert!(ports.contains(&testcontainers::core::ContainerPort::Tcp(9092)));
        assert!(ports.contains(&testcontainers::core::ContainerPort::Tcp(9094)));
    }

    #[test]
    fn image_is_debug() {
        let image = StreamlineImage::default();
        let debug = format!("{:?}", image);
        assert!(debug.contains("StreamlineImage"));
    }

    #[test]
    fn image_is_clone() {
        let image = StreamlineImage::builder().tag("0.2.0").build();
        let cloned = image.clone();
        assert_eq!(image.tag(), cloned.tag());
    }

    #[test]
    fn builder_is_debug_and_clone() {
        let builder = StreamlineImage::builder().tag("test");
        let debug = format!("{:?}", builder);
        assert!(debug.contains("StreamlineImageBuilder"));
        let _cloned = builder.clone();
    }

    #[test]
    fn full_builder_chain() {
        let image = StreamlineImage::builder()
            .tag("0.2.0")
            .log_level("warn")
            .playground(true)
            .in_memory(true)
            .env("EXTRA", "val")
            .build();

        assert_eq!(image.tag(), "0.2.0");
        assert_eq!(
            image
                .env_vars
                .get("STREAMLINE_LOG_LEVEL")
                .map(String::as_str),
            Some("warn")
        );
        assert_eq!(
            image
                .env_vars
                .get("STREAMLINE_PLAYGROUND")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            image
                .env_vars
                .get("STREAMLINE_IN_MEMORY")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(image.env_vars.get("EXTRA").map(String::as_str), Some("val"));
    }

    // -- Integration test (requires Docker) ---------------------------------
    //
    // This test is ignored by default because it requires a running Docker
    // daemon and network access to pull the Streamline image. Run it with:
    //
    //     cargo test --package streamline-testcontainers -- --ignored
    //
    #[tokio::test]
    #[ignore]
    async fn container_starts_and_exposes_ports() {
        use testcontainers::runners::AsyncRunner;

        let container = StreamlineImage::builder()
            .debug_logging()
            .build()
            .start()
            .await
            .expect("failed to start Streamline container");

        let bs = bootstrap_servers(&container)
            .await
            .expect("failed to get bootstrap servers");
        assert!(!bs.is_empty(), "bootstrap servers should not be empty");
        assert!(bs.contains(':'), "bootstrap servers should contain a colon");

        let url = http_url(&container).await.expect("failed to get HTTP URL");
        assert!(
            url.starts_with("http://"),
            "HTTP URL should start with http://"
        );

        let h_url = health_url(&container)
            .await
            .expect("failed to get health URL");
        assert!(
            h_url.ends_with("/health"),
            "health URL should end with /health"
        );

        let m_url = metrics_url(&container)
            .await
            .expect("failed to get metrics URL");
        assert!(
            m_url.ends_with("/metrics"),
            "metrics URL should end with /metrics"
        );

        let i_url = info_url(&container).await.expect("failed to get info URL");
        assert!(i_url.ends_with("/info"), "info URL should end with /info");

        println!("Bootstrap servers : {bs}");
        println!("HTTP URL          : {url}");
        println!("Health URL        : {h_url}");
        println!("Metrics URL       : {m_url}");
        println!("Info URL          : {i_url}");
    }
}
