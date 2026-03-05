//! Error types for the Streamline client.

use std::fmt;

/// Result type for Streamline operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error kinds for categorizing errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Topic not found
    TopicNotFound,
    /// Partition not found
    PartitionNotFound,
    /// Connection failed
    ConnectionFailed,
    /// General connection error
    Connection,
    /// Request timed out
    Timeout,
    /// Authentication failed
    AuthenticationFailed,
    /// Authorization failed
    AuthorizationFailed,
    /// Invalid configuration
    InvalidConfiguration,
    /// Protocol error
    Protocol,
    /// Serialization error
    Serialization,
    /// Schema registry error
    Schema,
    /// Internal error
    Internal,
}

/// Error type for Streamline client operations.
#[derive(Debug)]
pub struct Error {
    /// The error kind
    pub kind: ErrorKind,
    /// The error message
    pub message: String,
    /// A hint for resolving the error
    pub hint: Option<String>,
    /// The underlying cause
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl Error {
    /// Creates a new error.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            hint: None,
            source: None,
        }
    }

    /// Adds a hint to the error.
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Adds a source error.
    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// Creates a topic not found error.
    pub fn topic_not_found(topic: &str) -> Self {
        Self::new(ErrorKind::TopicNotFound, format!("Topic not found: {}", topic))
            .with_hint(format!(
                "Create the topic with: streamline-cli topics create {}",
                topic
            ))
    }

    /// Creates a partition not found error.
    pub fn partition_not_found(topic: &str, partition: i32) -> Self {
        Self::new(
            ErrorKind::PartitionNotFound,
            format!("Partition {} not found for topic {}", partition, topic),
        )
        .with_hint(format!(
            "Check partition count with: streamline-cli topics describe {}",
            topic
        ))
    }

    /// Creates a connection error.
    pub fn connection_failed(server: &str) -> Self {
        Self::new(ErrorKind::ConnectionFailed, format!("Failed to connect to {}", server))
            .with_hint("Check that Streamline server is running and accessible")
    }

    /// Creates a timeout error.
    pub fn timeout(operation: &str) -> Self {
        Self::new(ErrorKind::Timeout, format!("Operation timed out: {}", operation))
            .with_hint("Consider increasing timeout settings or checking server load")
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)?;
        if let Some(hint) = &self.hint {
            write!(f, "\n  hint: {}", hint)?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| e.as_ref() as _)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::new(ErrorKind::Internal, err.to_string()).with_source(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::new(ErrorKind::Serialization, err.to_string()).with_source(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_new() {
        let err = Error::new(ErrorKind::Internal, "something went wrong");
        assert_eq!(err.kind, ErrorKind::Internal);
        assert_eq!(err.message, "something went wrong");
        assert!(err.hint.is_none());
        assert!(err.source.is_none());
    }

    #[test]
    fn test_error_with_hint() {
        let err = Error::new(ErrorKind::Timeout, "timed out")
            .with_hint("increase timeout");
        assert_eq!(err.hint.as_deref(), Some("increase timeout"));
    }

    #[test]
    fn test_error_with_source() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let err = Error::new(ErrorKind::ConnectionFailed, "connect failed")
            .with_source(io_err);
        assert!(err.source.is_some());
    }

    #[test]
    fn test_topic_not_found() {
        let err = Error::topic_not_found("events");
        assert_eq!(err.kind, ErrorKind::TopicNotFound);
        assert!(err.message.contains("events"));
        assert!(err.hint.is_some());
        assert!(err.hint.as_ref().unwrap().contains("events"));
    }

    #[test]
    fn test_partition_not_found() {
        let err = Error::partition_not_found("events", 5);
        assert_eq!(err.kind, ErrorKind::PartitionNotFound);
        assert!(err.message.contains("5"));
        assert!(err.message.contains("events"));
    }

    #[test]
    fn test_connection_failed() {
        let err = Error::connection_failed("localhost:9092");
        assert_eq!(err.kind, ErrorKind::ConnectionFailed);
        assert!(err.message.contains("localhost:9092"));
    }

    #[test]
    fn test_timeout() {
        let err = Error::timeout("fetch");
        assert_eq!(err.kind, ErrorKind::Timeout);
        assert!(err.message.contains("fetch"));
    }

    #[test]
    fn test_display_without_hint() {
        let err = Error::new(ErrorKind::Internal, "bad state");
        let msg = format!("{}", err);
        assert_eq!(msg, "bad state");
    }

    #[test]
    fn test_display_with_hint() {
        let err = Error::new(ErrorKind::Internal, "bad state")
            .with_hint("restart server");
        let msg = format!("{}", err);
        assert!(msg.contains("bad state"));
        assert!(msg.contains("restart server"));
    }

    #[test]
    fn test_error_kind_equality() {
        assert_eq!(ErrorKind::TopicNotFound, ErrorKind::TopicNotFound);
        assert_ne!(ErrorKind::TopicNotFound, ErrorKind::Timeout);
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err: Error = io_err.into();
        assert_eq!(err.kind, ErrorKind::Internal);
    }

    #[test]
    fn test_std_error_trait() {
        let io_err = std::io::Error::new(std::io::ErrorKind::Other, "cause");
        let err = Error::new(ErrorKind::Internal, "wrapper").with_source(io_err);
        let source = std::error::Error::source(&err);
        assert!(source.is_some());
    }

    #[test]
    fn test_std_error_trait_no_source() {
        let err = Error::new(ErrorKind::Internal, "no cause");
        let source = std::error::Error::source(&err);
        assert!(source.is_none());
    }
}

