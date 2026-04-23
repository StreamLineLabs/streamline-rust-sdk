//! Validation utilities for Kafka protocol entities.

use crate::error::{Error, ErrorKind, Result};

/// Maximum allowed length for a Kafka topic name.
const MAX_TOPIC_NAME_LENGTH: usize = 249;

/// Validates a Kafka topic name.
///
/// A valid topic name must:
/// - Not be empty
/// - Be at most 249 characters
/// - Contain only alphanumeric characters, '.', '_', or '-'
/// - Not be exactly "." or ".."
pub fn validate_topic_name(topic: &str) -> Result<()> {
    if topic.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidConfiguration,
            "Topic name cannot be empty",
        )
        .with_hint("Provide a non-empty topic name"));
    }

    if topic.len() > MAX_TOPIC_NAME_LENGTH {
        return Err(Error::new(
            ErrorKind::InvalidConfiguration,
            format!(
                "Topic name is {} characters, exceeding the maximum of {}",
                topic.len(),
                MAX_TOPIC_NAME_LENGTH
            ),
        )
        .with_hint("Shorten the topic name to 249 characters or fewer"));
    }

    if topic == "." || topic == ".." {
        return Err(Error::new(
            ErrorKind::InvalidConfiguration,
            format!("Topic name cannot be '{topic}'"),
        )
        .with_hint("'.' and '..' are reserved and cannot be used as topic names"));
    }

    if let Some(c) = topic
        .chars()
        .find(|c| !c.is_ascii_alphanumeric() && *c != '.' && *c != '_' && *c != '-')
    {
        return Err(Error::new(
            ErrorKind::InvalidConfiguration,
            format!("Topic name contains invalid character: '{c}'"),
        )
        .with_hint("Topic names may only contain alphanumeric characters, '.', '_', or '-'"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_topic_names() {
        assert!(validate_topic_name("my-topic").is_ok());
        assert!(validate_topic_name("my.topic").is_ok());
        assert!(validate_topic_name("my_topic").is_ok());
        assert!(validate_topic_name("MyTopic123").is_ok());
        assert!(validate_topic_name("a").is_ok());
        assert!(validate_topic_name("events.v2").is_ok());
        assert!(validate_topic_name("org.example.topic-name_v1").is_ok());
    }

    #[test]
    fn test_empty_topic_name() {
        let err = validate_topic_name("").unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidConfiguration);
        assert!(err.message.contains("empty"));
    }

    #[test]
    fn test_topic_name_too_long() {
        let long_name = "a".repeat(250);
        let err = validate_topic_name(&long_name).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidConfiguration);
        assert!(err.message.contains("250"));
    }

    #[test]
    fn test_topic_name_max_length_is_valid() {
        let name = "a".repeat(249);
        assert!(validate_topic_name(&name).is_ok());
    }

    #[test]
    fn test_dot_topic_name() {
        let err = validate_topic_name(".").unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidConfiguration);
        assert!(err.message.contains("'.'"));
    }

    #[test]
    fn test_dotdot_topic_name() {
        let err = validate_topic_name("..").unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidConfiguration);
        assert!(err.message.contains("'..'"));
    }

    #[test]
    fn test_topic_name_with_invalid_characters() {
        let err = validate_topic_name("my topic").unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidConfiguration);
        assert!(err.message.contains("' '"));

        let err = validate_topic_name("my/topic").unwrap_err();
        assert!(err.message.contains("'/'"));

        let err = validate_topic_name("topic@name").unwrap_err();
        assert!(err.message.contains("'@'"));
    }

    #[test]
    fn test_dot_prefix_is_valid() {
        assert!(validate_topic_name(".hidden-topic").is_ok());
        assert!(validate_topic_name("..prefix").is_ok());
    }
}
