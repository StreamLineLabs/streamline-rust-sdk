use streamline_client::{Error, ErrorKind};

#[test]
fn test_error_display() {
    let err = Error::new(ErrorKind::Internal, "something went wrong");
    let display = format!("{}", err);
    assert_eq!(display, "something went wrong");
}

#[test]
fn test_error_kind_variants() {
    let variants = [
        ErrorKind::TopicNotFound,
        ErrorKind::PartitionNotFound,
        ErrorKind::ConnectionFailed,
        ErrorKind::Timeout,
        ErrorKind::AuthenticationFailed,
        ErrorKind::AuthorizationFailed,
        ErrorKind::InvalidConfiguration,
        ErrorKind::Protocol,
        ErrorKind::Serialization,
        ErrorKind::Internal,
    ];
    // Each variant is distinct
    for (i, a) in variants.iter().enumerate() {
        for (j, b) in variants.iter().enumerate() {
            if i == j {
                assert_eq!(a, b);
            } else {
                assert_ne!(a, b);
            }
        }
    }
}

#[test]
fn test_error_with_hint() {
    let err = Error::new(ErrorKind::Timeout, "operation timed out")
        .with_hint("increase the request timeout");
    assert_eq!(err.hint.as_deref(), Some("increase the request timeout"));
    let display = format!("{}", err);
    assert!(display.contains("operation timed out"));
    assert!(display.contains("increase the request timeout"));
}

#[test]
fn test_error_source_chain() {
    let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
    let err = Error::new(ErrorKind::ConnectionFailed, "connection failed")
        .with_source(io_err);


    // std::error::Error::source returns the underlying cause
    let source = std::error::Error::source(&err);
    assert!(source.is_some());
    assert!(source.unwrap().to_string().contains("refused"));
}

#[test]
fn test_connection_error() {
    let err = Error::connection_failed("broker1:9092");
    assert_eq!(err.kind, ErrorKind::ConnectionFailed);
    assert!(err.message.contains("broker1:9092"));
    assert!(err.hint.is_some());
    assert!(err.hint.as_ref().unwrap().contains("running"));
}

#[test]
fn test_producer_error() {
    // Simulate a producer-related error
    let err = Error::new(ErrorKind::Internal, "producer buffer full")
        .with_hint("reduce batch size or increase linger_ms");
    assert_eq!(err.kind, ErrorKind::Internal);
    assert!(err.message.contains("producer buffer full"));
    assert!(err.hint.as_ref().unwrap().contains("batch size"));
}

#[test]
fn test_consumer_error() {
    // Consumer not subscribed error
    let err = Error::new(ErrorKind::Internal, "Consumer is not subscribed");
    assert_eq!(err.kind, ErrorKind::Internal);
    assert!(err.message.contains("not subscribed"));
}

#[test]
fn test_timeout_error() {
    let err = Error::timeout("fetch");
    assert_eq!(err.kind, ErrorKind::Timeout);
    assert!(err.message.contains("fetch"));
    assert!(err.hint.is_some());
    assert!(err.hint.as_ref().unwrap().contains("timeout"));
}

#[test]
fn test_authentication_error() {
    let err = Error::new(ErrorKind::AuthenticationFailed, "invalid credentials");
    assert_eq!(err.kind, ErrorKind::AuthenticationFailed);
    assert_eq!(err.message, "invalid credentials");
}

#[test]
fn test_error_topic_not_found() {
    let err = Error::topic_not_found("my-events");
    assert_eq!(err.kind, ErrorKind::TopicNotFound);
    assert!(err.message.contains("my-events"));
    assert!(err.hint.as_ref().unwrap().contains("my-events"));
}

#[test]
fn test_error_partition_not_found() {
    let err = Error::partition_not_found("orders", 7);
    assert_eq!(err.kind, ErrorKind::PartitionNotFound);
    assert!(err.message.contains("7"));
    assert!(err.message.contains("orders"));
    assert!(err.hint.as_ref().unwrap().contains("orders"));
}

#[test]
fn test_error_display_without_hint() {
    let err = Error::new(ErrorKind::Protocol, "bad frame");
    let display = format!("{}", err);
    assert_eq!(display, "bad frame");
    assert!(!display.contains("hint"));
}

#[test]
fn test_error_display_with_hint() {
    let err = Error::new(ErrorKind::Protocol, "bad frame")
        .with_hint("check protocol version");
    let display = format!("{}", err);
    assert!(display.contains("bad frame"));
    assert!(display.contains("hint: check protocol version"));
}

#[test]
fn test_error_from_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
    let err: Error = io_err.into();
    assert_eq!(err.kind, ErrorKind::Internal);
    assert!(err.message.contains("file missing"));
    assert!(std::error::Error::source(&err).is_some());
}

#[test]
fn test_error_from_serde_json_error() {
    let json_result: std::result::Result<serde_json::Value, _> =
        serde_json::from_str("not valid json{{{");
    let json_err = json_result.unwrap_err();
    let err: Error = json_err.into();
    assert_eq!(err.kind, ErrorKind::Serialization);
    assert!(std::error::Error::source(&err).is_some());
}

#[test]
fn test_error_no_source() {
    let err = Error::new(ErrorKind::Internal, "standalone error");
    assert!(std::error::Error::source(&err).is_none());
    assert!(err.source.is_none());
}

#[test]
fn test_error_debug() {
    let err = Error::new(ErrorKind::Timeout, "timed out");
    let debug = format!("{:?}", err);
    assert!(debug.contains("Timeout"));
    assert!(debug.contains("timed out"));
}

#[test]
fn test_error_kind_copy_clone() {
    let kind = ErrorKind::ConnectionFailed;
    let copied = kind;
    let cloned = kind.clone();
    assert_eq!(kind, copied);
    assert_eq!(kind, cloned);
}

#[test]
fn test_authorization_error() {
    let err = Error::new(
        ErrorKind::AuthorizationFailed,
        "insufficient permissions for topic",
    );
    assert_eq!(err.kind, ErrorKind::AuthorizationFailed);
    assert!(err.message.contains("insufficient permissions"));
}

#[test]
fn test_invalid_configuration_error() {
    let err = Error::new(
        ErrorKind::InvalidConfiguration,
        "bootstrap_servers is required",
    );
    assert_eq!(err.kind, ErrorKind::InvalidConfiguration);
    assert!(err.message.contains("bootstrap_servers"));
}

