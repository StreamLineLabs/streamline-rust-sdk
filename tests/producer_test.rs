use streamline_client::{Headers, ProducerRecord};

#[test]
fn test_producer_record_new() {
    let record = ProducerRecord::new("my-key", "my-value");
    assert_eq!(record.key, Some("my-key"));
    assert_eq!(record.value, "my-value");
    assert!(record.headers.is_empty());
    assert!(record.partition.is_none());
}

#[test]
fn test_producer_record_with_key() {
    let record = ProducerRecord::new("user-123", "event-data");
    assert_eq!(record.key, Some("user-123"));

    let record_no_key = ProducerRecord::<String, &str>::value_only("event-data");
    assert!(record_no_key.key.is_none());
}

#[test]
fn test_producer_record_with_headers() {
    let headers = Headers::builder()
        .add("content-type", b"application/json")
        .add("trace-id", b"xyz-789")
        .build();
    let record = ProducerRecord::new("k", "v").with_headers(headers);
    assert!(!record.headers.is_empty());
    assert_eq!(
        record.headers.get_str("content-type"),
        Some("application/json")
    );
    assert_eq!(record.headers.get_str("trace-id"), Some("xyz-789"));
}

#[test]
fn test_producer_record_with_partition() {
    let record = ProducerRecord::new("k", "v").with_partition(5);
    assert_eq!(record.partition, Some(5));
}

#[test]
fn test_producer_record_topic() {
    // ProducerRecord doesn't carry a topic; topic is specified at send time.
    // Verify value_only creates a record without key.
    let record = ProducerRecord::<String, String>::value_only("payload".to_string());
    assert!(record.key.is_none());
    assert_eq!(record.value, "payload");
}

#[test]
fn test_producer_record_builder_pattern() {
    let headers = Headers::builder()
        .add("h1", b"v1")
        .build();
    let record = ProducerRecord::new("key", "value")
        .with_headers(headers)
        .with_partition(2);

    assert_eq!(record.key, Some("key"));
    assert_eq!(record.value, "value");
    assert_eq!(record.partition, Some(2));
    assert!(!record.headers.is_empty());
    assert_eq!(record.headers.get_str("h1"), Some("v1"));
}

#[test]
fn test_producer_record_value_only() {
    let record = ProducerRecord::<&str, &str>::value_only("data");
    assert!(record.key.is_none());
    assert_eq!(record.value, "data");
    assert!(record.headers.is_empty());
    assert!(record.partition.is_none());
}

#[test]
fn test_producer_record_clone() {
    let record = ProducerRecord::new("key", "value").with_partition(1);
    let cloned = record.clone();
    assert_eq!(cloned.key, Some("key"));
    assert_eq!(cloned.value, "value");
    assert_eq!(cloned.partition, Some(1));
}

#[test]
fn test_producer_record_debug() {
    let record = ProducerRecord::new("k", "v");
    let debug = format!("{:?}", record);
    assert!(debug.contains("ProducerRecord"));
}

#[test]
fn test_producer_record_typed_key_value() {
    // String types
    let str_record = ProducerRecord::new("key".to_string(), "value".to_string());
    assert_eq!(str_record.key, Some("key".to_string()));

    // Integer key, Vec<u8> value
    let int_record = ProducerRecord::new(42i64, vec![1u8, 2, 3]);
    assert_eq!(int_record.key, Some(42i64));
    assert_eq!(int_record.value, vec![1u8, 2, 3]);
}

#[test]
fn test_producer_record_with_empty_headers() {
    let headers = Headers::new();
    let record = ProducerRecord::new("k", "v").with_headers(headers);
    assert!(record.headers.is_empty());
}

#[test]
fn test_producer_record_partition_zero() {
    let record = ProducerRecord::new("k", "v").with_partition(0);
    assert_eq!(record.partition, Some(0));
}

#[test]
fn test_producer_record_large_value() {
    let large_value = "x".repeat(1_000_000);
    let record = ProducerRecord::new("k", large_value.as_str());
    assert_eq!(record.value.len(), 1_000_000);
}
