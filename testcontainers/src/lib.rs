//! # Streamline Testcontainers
//!
//! [Testcontainers](https://testcontainers.org/) module for
//! [Streamline](https://github.com/streamlinelabs/streamline).
//!
//! Streamline is a Kafka-protocol-compatible, single-binary streaming platform. This crate makes
//! it straightforward to spin up a disposable Streamline server inside Rust integration tests.
//!
//! **This crate is source-only and is not published to crates.io.** Depend on it by path or git
//! revision from this repository; see `testcontainers/README.md`.
//!
//! # The image reference is always explicit
//!
//! There is no default image, no default tag, and no [`Default`] implementation: this crate cannot
//! know which Streamline image exists in your registry, and silently defaulting to a tag that may
//! not be published would turn a configuration error into a confusing pull failure at test time.
//! Callers must supply the reference, and an immutable digest is strongly preferred:
//!
//! ```rust,no_run
//! use streamline_testcontainers::StreamlineImage;
//! use testcontainers::runners::AsyncRunner;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Pinned by immutable digest (recommended).
//! let image = StreamlineImage::builder()
//!     .image("ghcr.io/streamlinelabs/streamline@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
//!     .build()?;
//!
//! let container = image.start().await?;
//! let host = container.get_host().await?;
//! let port = container.get_host_port_ipv4(StreamlineImage::KAFKA_PORT).await?;
//! let bootstrap_servers = format!("{host}:{port}");
//! # Ok(())
//! # }
//! ```
//!
//! A mutable tag is accepted but is not reproducible:
//!
//! ```rust
//! use streamline_testcontainers::StreamlineImage;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let image = StreamlineImage::builder()
//!     .image("ghcr.io/streamlinelabs/streamline")
//!     .tag("0.4.0")
//!     .log_level("debug")
//!     .playground(true)
//!     .in_memory(true)
//!     .env("STREAMLINE_CUSTOM_KEY", "custom-value")
//!     .build()?;
//! assert!(!image.is_pinned_by_digest());
//! # Ok(())
//! # }
//! ```
//!
//! [`StreamlineImage::from_env`] reads the reference from `STREAMLINE_TEST_IMAGE`, which is how CI
//! injects a digest without hard-coding it in the source tree.
//!
//! # Features
//!
//! - Implements [`testcontainers::Image`] so you can use the standard `start()` / `AsyncRunner`
//!   workflow.
//! - Exposes Kafka (9092) and HTTP (9094) ports.
//! - Waits for `GET /health/live` on the HTTP port before returning.
//! - Configurable via environment variables (log level, in-memory mode, playground mode).
//! - Optional `client` feature re-exports the `streamline-client` crate.

use std::borrow::Cow;
use std::collections::HashMap;

use testcontainers::core::{wait::HttpWaitStrategy, ContainerPort, WaitFor};
use testcontainers::Image;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Environment variable holding the image reference used by
/// [`StreamlineImage::from_env`].
pub const IMAGE_ENV_VAR: &str = "STREAMLINE_TEST_IMAGE";

/// Digest prefix of an immutable image reference.
const DIGEST_SEPARATOR: &str = "@sha256:";

/// Length of a hex-encoded SHA-256 digest.
const SHA256_HEX_LEN: usize = 64;

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
/// Construct one with [`StreamlineImage::builder`] or
/// [`StreamlineImage::from_env`]; there is deliberately no default image
/// reference. Two ports are exposed:
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
/// let image = StreamlineImage::builder()
///     .image("ghcr.io/streamlinelabs/streamline@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
///     .build()?;
/// let container = image.start().await?;
/// let host = container.get_host().await?;
/// let kafka_port = container.get_host_port_ipv4(StreamlineImage::KAFKA_PORT).await?;
/// println!("Kafka available at {host}:{kafka_port}");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct StreamlineImage {
    /// Repository part of the reference. For a digest reference this includes
    /// the `@sha256` suffix, because `testcontainers` 0.28 composes the image
    /// descriptor as `{name}:{tag}` and a digest reference is therefore
    /// expressed as `repository@sha256` + `:` + `<hex digest>`.
    name: String,
    tag: String,
    pinned_by_digest: bool,
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

    /// Builds an image from the `STREAMLINE_TEST_IMAGE` environment variable.
    ///
    /// # Errors
    /// Returns [`Error::InvalidConfiguration`] when the variable is unset,
    /// empty, or does not contain a usable image reference. There is no
    /// fallback image: a missing reference is a hard error so tests fail with
    /// a clear message instead of pulling an unintended image.
    pub fn from_env() -> Result<Self> {
        let reference = std::env::var(IMAGE_ENV_VAR).map_err(|_| {
            Error::InvalidConfiguration(format!(
                "{IMAGE_ENV_VAR} is not set; set it to an image reference such as \
                 ghcr.io/streamlinelabs/streamline@sha256:<64 hex digits>"
            ))
        })?;
        Self::builder().image(reference).build()
    }

    /// Returns the full image reference (`repository@sha256:<digest>` or
    /// `repository:<tag>`).
    pub fn reference(&self) -> String {
        format!("{}:{}", self.name, self.tag)
    }

    /// Returns whether the image is pinned to an immutable digest.
    pub fn is_pinned_by_digest(&self) -> bool {
        self.pinned_by_digest
    }
}

// ---------------------------------------------------------------------------
// Image trait
// ---------------------------------------------------------------------------

impl Image for StreamlineImage {
    fn name(&self) -> &str {
        &self.name
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::http(
            HttpWaitStrategy::new("/health/live")
                .with_port(ContainerPort::Tcp(Self::HTTP_PORT))
                .with_expected_status_code(200u16),
        )]
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
/// The image reference is mandatory: [`StreamlineImageBuilder::build`] fails
/// when neither [`image`](StreamlineImageBuilder::image) nor a
/// repository/[`tag`](StreamlineImageBuilder::tag) pair has been supplied.
///
/// # Examples
///
/// ```rust
/// use streamline_testcontainers::StreamlineImage;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let image = StreamlineImage::builder()
///     .image("ghcr.io/streamlinelabs/streamline")
///     .tag("0.4.0")
///     .log_level("debug")
///     .playground(true)
///     .in_memory(true)
///     .env("MY_VAR", "my_value")
///     .build()?;
/// assert_eq!(image.reference(), "ghcr.io/streamlinelabs/streamline:0.4.0");
///
/// // Missing reference fails closed.
/// assert!(StreamlineImage::builder().build().is_err());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct StreamlineImageBuilder {
    repository: Option<String>,
    tag: Option<String>,
    digest: Option<String>,
    env_vars: HashMap<String, String>,
}

impl StreamlineImageBuilder {
    /// Sets the image reference.
    ///
    /// Accepts either a plain repository (`ghcr.io/streamlinelabs/streamline`,
    /// which then requires [`tag`](Self::tag)), a `repository:tag` reference,
    /// or an immutable `repository@sha256:<64 hex digits>` reference.
    /// Validation happens in [`build`](Self::build).
    pub fn image(mut self, reference: impl Into<String>) -> Self {
        let reference = reference.into();
        if let Some((repository, digest)) = reference.split_once(DIGEST_SEPARATOR) {
            self.repository = Some(repository.to_string());
            self.digest = Some(digest.to_string());
            self.tag = None;
        } else if let Some((repository, tag)) = split_repository_and_tag(&reference) {
            self.repository = Some(repository);
            self.tag = Some(tag);
            self.digest = None;
        } else {
            self.repository = Some(reference);
        }
        self
    }

    /// Sets the Docker image tag (e.g. `"0.4.0"`).
    ///
    /// A tag is mutable: prefer [`digest`](Self::digest) or a
    /// `repository@sha256:...` reference for reproducible test runs.
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = Some(tag.into());
        self.digest = None;
        self
    }

    /// Pins the image to an immutable digest, with or without the `sha256:`
    /// prefix.
    pub fn digest(mut self, digest: impl Into<String>) -> Self {
        let digest = digest.into();
        self.digest = Some(
            digest
                .strip_prefix("sha256:")
                .map(str::to_string)
                .unwrap_or(digest),
        );
        self.tag = None;
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
    ///
    /// # Errors
    /// Returns [`Error::InvalidConfiguration`] when no repository was set,
    /// when neither a tag nor a digest was set, or when the digest is not 64
    /// hexadecimal characters. There is no default image reference.
    pub fn build(self) -> Result<StreamlineImage> {
        let repository = self.repository.filter(|r| !r.trim().is_empty()).ok_or_else(|| {
            Error::InvalidConfiguration(format!(
                "no image reference configured; call StreamlineImage::builder().image(...) or set {IMAGE_ENV_VAR}"
            ))
        })?;

        let (name, tag, pinned_by_digest) = match (self.digest, self.tag) {
            (Some(digest), _) => {
                validate_digest(&digest)?;
                // testcontainers 0.28 renders the descriptor as `{name}:{tag}`,
                // so a digest reference is split across the two accessors to
                // produce `repository@sha256:<digest>`.
                (
                    format!("{repository}@sha256"),
                    digest.to_ascii_lowercase(),
                    true,
                )
            }
            (None, Some(tag)) => {
                if tag.trim().is_empty() {
                    return Err(Error::InvalidConfiguration(
                        "image tag must not be empty".to_string(),
                    ));
                }
                (repository, tag, false)
            }
            (None, None) => {
                return Err(Error::InvalidConfiguration(format!(
                    "image '{repository}' has neither a tag nor a digest; pin a digest with \
                     .image(\"{repository}@sha256:<64 hex digits>\") or set an explicit .tag(...)"
                )))
            }
        };

        let mut env_vars = self.env_vars;
        env_vars
            .entry("STREAMLINE_LISTEN_ADDR".to_string())
            .or_insert_with(|| format!("0.0.0.0:{}", StreamlineImage::KAFKA_PORT));
        env_vars
            .entry("STREAMLINE_HTTP_ADDR".to_string())
            .or_insert_with(|| format!("0.0.0.0:{}", StreamlineImage::HTTP_PORT));

        Ok(StreamlineImage {
            name,
            tag,
            pinned_by_digest,
            env_vars,
        })
    }
}

/// Splits `repository:tag`, ignoring a `:port` in a registry host (which is
/// always followed by a `/`).
fn split_repository_and_tag(reference: &str) -> Option<(String, String)> {
    let colon = reference.rfind(':')?;
    let tag = &reference[colon + 1..];
    if tag.is_empty() || tag.contains('/') {
        return None;
    }
    Some((reference[..colon].to_string(), tag.to_string()))
}

fn validate_digest(digest: &str) -> Result<()> {
    if digest.len() != SHA256_HEX_LEN || !digest.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(Error::InvalidConfiguration(format!(
            "image digest must be {SHA256_HEX_LEN} hexadecimal characters, got '{digest}'"
        )));
    }
    Ok(())
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
/// let container = StreamlineImage::from_env()?.start().await?;
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
    Ok(format!("{base}/health/live"))
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

    const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const REPOSITORY: &str = "ghcr.io/streamlinelabs/streamline";

    fn tagged_image() -> StreamlineImage {
        StreamlineImage::builder()
            .image(REPOSITORY)
            .tag("0.3.0")
            .build()
            .expect("explicit repository and tag build")
    }

    fn digest_image() -> StreamlineImage {
        StreamlineImage::builder()
            .image(format!("{REPOSITORY}@sha256:{DIGEST}"))
            .build()
            .expect("digest reference builds")
    }

    // -- Image reference handling ------------------------------------------

    #[test]
    fn build_requires_an_explicit_image_reference() {
        let error = StreamlineImage::builder().build().unwrap_err();
        assert!(
            matches!(&error, Error::InvalidConfiguration(message) if message.contains("no image reference")),
            "{error}"
        );
    }

    #[test]
    fn build_requires_a_tag_or_digest() {
        let error = StreamlineImage::builder()
            .image(REPOSITORY)
            .build()
            .unwrap_err();
        assert!(
            matches!(&error, Error::InvalidConfiguration(message) if message.contains("neither a tag nor a digest")),
            "{error}"
        );
    }

    #[test]
    fn build_rejects_empty_tag() {
        let error = StreamlineImage::builder()
            .image(REPOSITORY)
            .tag("  ")
            .build()
            .unwrap_err();
        assert!(matches!(error, Error::InvalidConfiguration(_)));
    }

    #[test]
    fn build_rejects_malformed_digest() {
        for digest in ["deadbeef", &"z".repeat(64), &format!("{DIGEST}00")] {
            let error = StreamlineImage::builder()
                .image(REPOSITORY)
                .digest(digest)
                .build()
                .unwrap_err();
            assert!(
                matches!(&error, Error::InvalidConfiguration(message) if message.contains("digest")),
                "{error}"
            );
        }
    }

    #[test]
    fn digest_reference_produces_immutable_descriptor() {
        let image = digest_image();
        // testcontainers renders `{name}:{tag}`, which must reassemble the
        // digest reference exactly.
        assert_eq!(image.name(), format!("{REPOSITORY}@sha256"));
        assert_eq!(image.tag(), DIGEST);
        assert_eq!(image.reference(), format!("{REPOSITORY}@sha256:{DIGEST}"));
        assert!(image.is_pinned_by_digest());
    }

    #[test]
    fn digest_builder_accepts_prefixed_and_bare_digests() {
        let prefixed = StreamlineImage::builder()
            .image(REPOSITORY)
            .digest(format!("sha256:{DIGEST}"))
            .build()
            .unwrap();
        let bare = StreamlineImage::builder()
            .image(REPOSITORY)
            .digest(DIGEST)
            .build()
            .unwrap();
        assert_eq!(prefixed.reference(), bare.reference());
    }

    #[test]
    fn digest_is_normalized_to_lowercase() {
        let image = StreamlineImage::builder()
            .image(REPOSITORY)
            .digest(DIGEST.to_ascii_uppercase())
            .build()
            .unwrap();
        assert_eq!(image.tag(), DIGEST);
    }

    #[test]
    fn tagged_reference_is_not_pinned() {
        let image = tagged_image();
        assert_eq!(image.name(), REPOSITORY);
        assert_eq!(image.tag(), "0.3.0");
        assert_eq!(image.reference(), format!("{REPOSITORY}:0.3.0"));
        assert!(!image.is_pinned_by_digest());
    }

    #[test]
    fn image_reference_with_inline_tag_is_split() {
        let image = StreamlineImage::builder()
            .image(format!("{REPOSITORY}:1.2.3"))
            .build()
            .unwrap();
        assert_eq!(image.name(), REPOSITORY);
        assert_eq!(image.tag(), "1.2.3");
    }

    #[test]
    fn registry_port_is_not_mistaken_for_a_tag() {
        let image = StreamlineImage::builder()
            .image("localhost:5000/streamline")
            .tag("0.3.0")
            .build()
            .unwrap();
        assert_eq!(image.name(), "localhost:5000/streamline");
        assert_eq!(image.tag(), "0.3.0");
    }

    #[test]
    fn explicit_tag_overrides_a_digest_and_vice_versa() {
        let image = StreamlineImage::builder()
            .image(format!("{REPOSITORY}@sha256:{DIGEST}"))
            .tag("0.3.0")
            .build()
            .unwrap();
        assert!(!image.is_pinned_by_digest());
        assert_eq!(image.tag(), "0.3.0");

        let image = StreamlineImage::builder()
            .image(format!("{REPOSITORY}:0.3.0"))
            .digest(DIGEST)
            .build()
            .unwrap();
        assert!(image.is_pinned_by_digest());
    }

    #[test]
    fn from_env_requires_the_variable() {
        // The variable is process-global; only assert the unset case when the
        // ambient environment does not define it.
        if std::env::var(IMAGE_ENV_VAR).is_err() {
            let error = StreamlineImage::from_env().unwrap_err();
            assert!(
                matches!(&error, Error::InvalidConfiguration(message) if message.contains(IMAGE_ENV_VAR)),
                "{error}"
            );
        }
    }

    // -- Builder configuration ---------------------------------------------

    #[test]
    fn builder_sets_log_level() {
        let image = StreamlineImage::builder()
            .image(REPOSITORY)
            .tag("0.3.0")
            .log_level("debug")
            .build()
            .unwrap();
        assert_eq!(
            image
                .env_vars
                .get("STREAMLINE_LOG_LEVEL")
                .map(String::as_str),
            Some("debug")
        );
    }

    #[test]
    fn debug_and_trace_logging_shorthands() {
        let debug_image = StreamlineImage::builder()
            .image(REPOSITORY)
            .tag("0.3.0")
            .debug_logging()
            .build()
            .unwrap();
        assert_eq!(
            debug_image
                .env_vars
                .get("STREAMLINE_LOG_LEVEL")
                .map(String::as_str),
            Some("debug")
        );

        let trace_image = StreamlineImage::builder()
            .image(REPOSITORY)
            .tag("0.3.0")
            .trace_logging()
            .build()
            .unwrap();
        assert_eq!(
            trace_image
                .env_vars
                .get("STREAMLINE_LOG_LEVEL")
                .map(String::as_str),
            Some("trace")
        );
    }

    #[test]
    fn builder_toggles_playground_mode() {
        let enabled = StreamlineImage::builder()
            .image(REPOSITORY)
            .tag("0.3.0")
            .playground(true)
            .build()
            .unwrap();
        assert_eq!(
            enabled
                .env_vars
                .get("STREAMLINE_PLAYGROUND")
                .map(String::as_str),
            Some("true")
        );

        let disabled = StreamlineImage::builder()
            .image(REPOSITORY)
            .tag("0.3.0")
            .playground(true)
            .playground(false)
            .build()
            .unwrap();
        assert!(!disabled.env_vars.contains_key("STREAMLINE_PLAYGROUND"));
    }

    #[test]
    fn builder_toggles_in_memory_mode() {
        let enabled = StreamlineImage::builder()
            .image(REPOSITORY)
            .tag("0.3.0")
            .in_memory(true)
            .build()
            .unwrap();
        assert_eq!(
            enabled
                .env_vars
                .get("STREAMLINE_IN_MEMORY")
                .map(String::as_str),
            Some("true")
        );

        let disabled = StreamlineImage::builder()
            .image(REPOSITORY)
            .tag("0.3.0")
            .in_memory(true)
            .in_memory(false)
            .build()
            .unwrap();
        assert!(!disabled.env_vars.contains_key("STREAMLINE_IN_MEMORY"));
    }

    #[test]
    fn builder_adds_custom_env_var() {
        let image = StreamlineImage::builder()
            .image(REPOSITORY)
            .tag("0.3.0")
            .env("MY_KEY", "my_value")
            .build()
            .unwrap();
        assert_eq!(
            image.env_vars.get("MY_KEY").map(String::as_str),
            Some("my_value")
        );
    }

    #[test]
    fn default_env_vars_contain_listen_and_http_addrs() {
        let image = tagged_image();
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
    fn explicit_listen_addr_is_not_overwritten() {
        let image = StreamlineImage::builder()
            .image(REPOSITORY)
            .tag("0.3.0")
            .env("STREAMLINE_LISTEN_ADDR", "0.0.0.0:19092")
            .build()
            .unwrap();
        assert_eq!(
            image
                .env_vars
                .get("STREAMLINE_LISTEN_ADDR")
                .map(String::as_str),
            Some("0.0.0.0:19092")
        );
    }

    #[test]
    fn ready_conditions_use_live_health_endpoint() {
        let conditions = tagged_image().ready_conditions();
        assert_eq!(conditions.len(), 1);
        let debug = format!("{:?}", conditions[0]);
        assert!(debug.contains("/health/live"), "{debug}");
        assert!(!debug.contains("Server started"), "{debug}");
    }

    #[test]
    fn expose_ports_contains_kafka_and_http() {
        let image = tagged_image();
        let ports = image.expose_ports();
        assert_eq!(ports.len(), 2);
        assert!(ports.contains(&testcontainers::core::ContainerPort::Tcp(9092)));
        assert!(ports.contains(&testcontainers::core::ContainerPort::Tcp(9094)));
    }

    #[test]
    fn image_and_builder_are_debug_and_clone() {
        let image = digest_image();
        assert!(format!("{image:?}").contains("StreamlineImage"));
        assert_eq!(image.clone().reference(), image.reference());

        let builder = StreamlineImage::builder().image(REPOSITORY).tag("test");
        assert!(format!("{builder:?}").contains("StreamlineImageBuilder"));
        let _cloned = builder.clone();
    }

    #[test]
    fn full_builder_chain() {
        let image = StreamlineImage::builder()
            .image(format!("{REPOSITORY}@sha256:{DIGEST}"))
            .log_level("warn")
            .playground(true)
            .in_memory(true)
            .env("EXTRA", "val")
            .build()
            .unwrap();

        assert!(image.is_pinned_by_digest());
        assert_eq!(
            image
                .env_vars
                .get("STREAMLINE_LOG_LEVEL")
                .map(String::as_str),
            Some("warn")
        );
        assert_eq!(image.env_vars.get("EXTRA").map(String::as_str), Some("val"));
    }

    // -- Integration test (requires Docker) ---------------------------------
    //
    // Ignored by default: it requires a running Docker daemon and an explicit
    // image reference in STREAMLINE_TEST_IMAGE (pin a digest). Run it with:
    //
    //     STREAMLINE_TEST_IMAGE=ghcr.io/streamlinelabs/streamline@sha256:<digest> \
    //       cargo test --manifest-path testcontainers/Cargo.toml \
    //       container_starts_and_exposes_ports -- --ignored --exact
    //
    #[tokio::test]
    #[ignore]
    async fn container_starts_and_exposes_ports() {
        use testcontainers::runners::AsyncRunner;

        let image = StreamlineImage::from_env()
            .expect("STREAMLINE_TEST_IMAGE must be set to an explicit image reference");
        assert!(
            image.is_pinned_by_digest(),
            "pin STREAMLINE_TEST_IMAGE to an immutable digest, got {}",
            image.reference()
        );

        let container = image
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
            h_url.ends_with("/health/live"),
            "health URL should end with /health/live"
        );
        let health_response = reqwest::get(&h_url)
            .await
            .expect("failed to request health endpoint");
        assert!(
            health_response.status().is_success(),
            "health endpoint returned {}",
            health_response.status()
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

        println!("Image             : {}", image_reference_of(&container));
        println!("Bootstrap servers : {bs}");
        println!("HTTP URL          : {url}");
        println!("Health URL        : {h_url}");
        println!("Metrics URL       : {m_url}");
        println!("Info URL          : {i_url}");
    }

    fn image_reference_of(container: &testcontainers::ContainerAsync<StreamlineImage>) -> String {
        container.image().reference()
    }
}
