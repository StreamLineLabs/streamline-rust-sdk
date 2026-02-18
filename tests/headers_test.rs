use streamline_client::headers::{Headers, HeadersBuilder};

#[test]
fn test_headers_builder_empty() {
    let headers = HeadersBuilder::new().build();
    assert!(headers.is_empty());
}

#[test]
fn test_headers_builder_add() {
    let headers = Headers::builder().add("key", b"value").build();
    assert_eq!(headers.get_str("key"), Some("value"));
}

#[test]
fn test_headers_builder_multiple() {
    let headers = Headers::builder()
        .add("content-type", b"application/json")
        .add("trace-id", b"abc-123")
        .add("version", b"2")
        .build();
    assert_eq!(headers.get_str("content-type"), Some("application/json"));
    assert_eq!(headers.get_str("trace-id"), Some("abc-123"));
    assert_eq!(headers.get_str("version"), Some("2"));
}

#[test]
fn test_headers_get_existing() {
    let mut headers = Headers::new();
    headers.add("x-request-id", b"req-456");
    assert_eq!(headers.get("x-request-id"), Some(b"req-456".as_ref()));
}

#[test]
fn test_headers_get_missing() {
    let headers = Headers::new();
    assert!(headers.get("nonexistent").is_none());
    assert!(headers.get_str("nonexistent").is_none());
}

#[test]
fn test_headers_contains_key() {
    let headers = Headers::builder()
        .add("present", b"yes")
        .build();
    assert!(headers.get("present").is_some());
    assert!(headers.get("absent").is_none());
}

#[test]
fn test_headers_len() {
    let headers = Headers::builder()
        .add("a", b"1")
        .add("b", b"2")
        .add("c", b"3")
        .build();
    assert_eq!(headers.iter().count(), 3);
}

#[test]
fn test_headers_is_empty() {
    let empty = Headers::new();
    assert!(empty.is_empty());

    let non_empty = Headers::builder().add("k", b"v").build();
    assert!(!non_empty.is_empty());
}

#[test]
fn test_headers_iter() {
    let headers = Headers::builder()
        .add("x", b"10")
        .add("y", b"20")
        .build();

    let mut keys: Vec<&String> = headers.iter().map(|(k, _)| k).collect();
    keys.sort();
    assert_eq!(keys, vec!["x", "y"]);

    let collected: std::collections::HashMap<&String, &Vec<u8>> = headers.iter().collect();
    assert_eq!(collected.len(), 2);
}

#[test]
fn test_headers_from_vec() {
    let pairs: Vec<(&str, &[u8])> = vec![
        ("h1", b"val1"),
        ("h2", b"val2"),
        ("h3", b"val3"),
    ];

    let mut headers = Headers::new();
    for (k, v) in pairs {
        headers.add(k, v);
    }
    assert_eq!(headers.iter().count(), 3);
    assert_eq!(headers.get_str("h1"), Some("val1"));
    assert_eq!(headers.get_str("h2"), Some("val2"));
    assert_eq!(headers.get_str("h3"), Some("val3"));
}

#[test]
fn test_headers_display() {
    // Headers derives Debug; verify debug formatting works
    let headers = Headers::builder()
        .add("key", b"value")
        .build();
    let debug_str = format!("{:?}", headers);
    assert!(debug_str.contains("key"));
}

#[test]
fn test_headers_clone() {
    let original = Headers::builder()
        .add("cloned", b"data")
        .build();
    let cloned = original.clone();
    assert_eq!(cloned.get_str("cloned"), Some("data"));
}

#[test]
fn test_headers_default() {
    let headers = Headers::default();
    assert!(headers.is_empty());
}

#[test]
fn test_headers_binary_values() {
    let mut headers = Headers::new();
    let binary_data: Vec<u8> = vec![0x00, 0xFF, 0xAB, 0xCD];
    headers.add("binary", &binary_data);
    assert_eq!(headers.get("binary"), Some(binary_data.as_slice()));
    // get_str should return None for non-UTF-8 data
    assert!(headers.get_str("binary").is_none());
}

#[test]
fn test_headers_overwrite_key() {
    let mut headers = Headers::new();
    headers.add("key", b"first");
    headers.add("key", b"second");
    assert_eq!(headers.get_str("key"), Some("second"));
}

#[test]
fn test_headers_builder_chaining() {
    let headers = Headers::builder()
        .add("a", b"1")
        .add("b", b"2")
        .add("c", b"3")
        .add("d", b"4")
        .add("e", b"5")
        .build();
    assert_eq!(headers.iter().count(), 5);
}

#[test]
fn test_headers_empty_key_and_value() {
    let headers = Headers::builder()
        .add("", b"")
        .build();
    assert!(!headers.is_empty());
    assert_eq!(headers.get(""), Some(b"".as_ref()));
}
