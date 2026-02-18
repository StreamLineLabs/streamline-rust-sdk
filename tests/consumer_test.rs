use streamline_client::{ConsumerRecord, Headers};

#[test]
fn test_consumer_record_fields() {
    let record: ConsumerRecord<String, String> = ConsumerRecord {
        topic: "events".to_string(),
        partition: 0,
        offset: 42,
        timestamp: 1700000000000,
        key: Some("user-1".to_string()),
        value: "payload".to_string(),
        headers: Headers::new(),
    };
    assert_eq!(record.topic, "events");
    assert_eq!(record.partition, 0);
    assert_eq!(record.offset, 42);
    assert_eq!(record.timestamp, 1700000000000);
    assert_eq!(record.key, Some("user-1".to_string()));
    assert_eq!(record.value, "payload");
    assert!(record.headers.is_empty());
}

#[test]
fn test_consumer_record_with_headers() {
    let headers = Headers::builder()
        .add("source", b"service-a")
        .add("version", b"1")
        .build();

    let record: ConsumerRecord<String, String> = ConsumerRecord {
        topic: "logs".to_string(),
        partition: 1,
        offset: 100,
        timestamp: 1700000001000,
        key: None,
        value: "log-entry".to_string(),
        headers,
    };
    assert!(!record.headers.is_empty());
    assert_eq!(record.headers.get_str("source"), Some("service-a"));
    assert_eq!(record.headers.get_str("version"), Some("1"));
}

#[test]
fn test_consumer_record_key_value_types() {
    // String key and value
    let string_record: ConsumerRecord<String, String> = ConsumerRecord {
        topic: "t".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        key: Some("key".to_string()),
        value: "value".to_string(),
        headers: Headers::new(),
    };
    assert_eq!(string_record.key, Some("key".to_string()));
    assert_eq!(string_record.value, "value");

    // Vec<u8> key and value
    let bytes_record: ConsumerRecord<Vec<u8>, Vec<u8>> = ConsumerRecord {
        topic: "t".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        key: Some(vec![1, 2, 3]),
        value: vec![4, 5, 6],
        headers: Headers::new(),
    };
    assert_eq!(bytes_record.key, Some(vec![1, 2, 3]));
    assert_eq!(bytes_record.value, vec![4, 5, 6]);

    // i64 key, String value
    let int_record: ConsumerRecord<i64, String> = ConsumerRecord {
        topic: "t".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        key: Some(12345),
        value: "data".to_string(),
        headers: Headers::new(),
    };
    assert_eq!(int_record.key, Some(12345));
}

#[test]
fn test_consumer_record_none_key() {
    let record: ConsumerRecord<String, String> = ConsumerRecord {
        topic: "topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        key: None,
        value: "value".to_string(),
        headers: Headers::new(),
    };
    assert!(record.key.is_none());
}

#[test]
fn test_consumer_record_clone() {
    let record: ConsumerRecord<String, String> = ConsumerRecord {
        topic: "t".to_string(),
        partition: 2,
        offset: 99,
        timestamp: 1700000000000,
        key: Some("k".to_string()),
        value: "v".to_string(),
        headers: Headers::new(),
    };
    let cloned = record.clone();
    assert_eq!(cloned.topic, "t");
    assert_eq!(cloned.partition, 2);
    assert_eq!(cloned.offset, 99);
    assert_eq!(cloned.timestamp, 1700000000000);
    assert_eq!(cloned.key, Some("k".to_string()));
    assert_eq!(cloned.value, "v");
}

#[test]
fn test_consumer_record_debug() {
    let record: ConsumerRecord<String, String> = ConsumerRecord {
        topic: "debug-topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        key: None,
        value: "val".to_string(),
        headers: Headers::new(),
    };
    let debug = format!("{:?}", record);
    assert!(debug.contains("ConsumerRecord"));
    assert!(debug.contains("debug-topic"));
}

#[test]
fn test_consumer_record_high_offset() {
    let record: ConsumerRecord<String, String> = ConsumerRecord {
        topic: "t".to_string(),
        partition: 0,
        offset: i64::MAX,
        timestamp: 0,
        key: None,
        value: "v".to_string(),
        headers: Headers::new(),
    };
    assert_eq!(record.offset, i64::MAX);
}

#[test]
fn test_consumer_record_multiple_partitions() {
    let records: Vec<ConsumerRecord<String, String>> = (0..10)
        .map(|i| ConsumerRecord {
            topic: "multi-part".to_string(),
            partition: i,
            offset: i as i64 * 100,
            timestamp: 1700000000000 + i as i64,
            key: Some(format!("key-{}", i)),
            value: format!("value-{}", i),
            headers: Headers::new(),
        })
        .collect();

    assert_eq!(records.len(), 10);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.partition, i as i32);
        assert_eq!(record.offset, i as i64 * 100);
    }
}
