//! OpenTelemetry integration for Streamline Rust SDK.
//!
//! This module is gated behind the `telemetry` feature flag. When enabled,
//! it provides automatic tracing for produce and consume operations using
//! the `tracing` crate with OpenTelemetry span attributes.
//!
//! # Span Conventions
//!
//! All spans follow OTel semantic conventions for messaging:
//!
//! - **Span name**: `{topic} {operation}` (e.g., "orders produce")
//! - **Attributes**: `messaging.system=streamline`, `messaging.destination.name={topic}`,
//!   `messaging.operation={produce|consume}`
//! - **Kind**: `PRODUCER` for produce, `CONSUMER` for consume
//!
//! # Example
//!
//! ```rust,ignore
//! use streamline_client::telemetry;
//!
//! // Trace a produce operation
//! let metadata = telemetry::trace_produce("orders", || async {
//!     producer.send("orders", "key", "value", Headers::new()).await
//! }).await?;
//!
//! // Trace a consume operation
//! let records = telemetry::trace_consume("events", || async {
//!     consumer.poll(Duration::from_millis(100)).await
//! }).await?;
//! ```

#[cfg(feature = "telemetry")]
use tracing::Instrument;

/// Creates a tracing span for a produce operation and executes the given future within it.
///
/// The span includes OTel messaging semantic convention attributes:
/// - `messaging.system` = "streamline"
/// - `messaging.destination.name` = topic name
/// - `messaging.operation` = "produce"
/// - `otel.kind` = "PRODUCER"
///
/// # Arguments
///
/// * `topic` - The destination topic name
/// * `f` - An async closure that performs the produce operation
///
/// # Returns
///
/// The result of the produce operation.
#[cfg(feature = "telemetry")]
pub async fn trace_produce<F, Fut, T>(topic: &str, f: F) -> T

where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let span = tracing::info_span!(
        "produce",
        "otel.name" = %format!("{} produce", topic),
        "otel.kind" = "PRODUCER",
        "messaging.system" = "streamline",
        "messaging.destination.name" = %topic,
        "messaging.operation" = "produce",
    );
    f().instrument(span).await
}

/// Creates a tracing span for a consume/poll operation and executes the given future within it.
///
/// The span includes OTel messaging semantic convention attributes:
/// - `messaging.system` = "streamline"
/// - `messaging.destination.name` = topic name
/// - `messaging.operation` = "consume"
/// - `otel.kind` = "CONSUMER"
///
/// # Arguments
///
/// * `topic` - The source topic name
/// * `f` - An async closure that performs the consume operation
///
/// # Returns
///
/// The result of the consume operation.
#[cfg(feature = "telemetry")]
pub async fn trace_consume<F, Fut, T>(topic: &str, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let span = tracing::info_span!(
        "consume",
        "otel.name" = %format!("{} consume", topic),
        "otel.kind" = "CONSUMER",
        "messaging.system" = "streamline",
        "messaging.destination.name" = %topic,
        "messaging.operation" = "consume",
    );
    f().instrument(span).await
}

/// Creates a tracing span for processing a single consumed record.
///
/// This span includes additional attributes for partition and offset,
/// making it easy to correlate with the produce span.
///
/// # Arguments
///
/// * `topic` - The source topic name
/// * `partition` - The partition number
/// * `offset` - The record offset
/// * `f` - An async closure that processes the record
///
/// # Returns
///
/// The result of the processing operation.
#[cfg(feature = "telemetry")]
pub async fn trace_process<F, Fut, T>(topic: &str, partition: i32, offset: i64, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let span = tracing::info_span!(
        "process",
        "otel.name" = %format!("{} process", topic),
        "otel.kind" = "CONSUMER",
        "messaging.system" = "streamline",
        "messaging.destination.name" = %topic,
        "messaging.operation" = "process",
        "messaging.destination.partition.id" = %partition,
        "messaging.message.id" = %offset,
    );
    f().instrument(span).await
}

/// Injects the current trace context into message headers.
///
/// This enables distributed trace propagation from producer to consumer.
/// The W3C TraceContext format is used (traceparent/tracestate headers).
///
/// # Arguments
///
/// * `headers` - The message headers to inject context into
#[cfg(feature = "telemetry")]
pub fn inject_context(headers: &mut crate::Headers) {
    use opentelemetry::global;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let current_span = tracing::Span::current();
    let cx = current_span.context();

    let mut carrier = std::collections::HashMap::new();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut carrier);
    });

    for (key, value) in carrier {
        headers.add(key, value.as_bytes());
    }
}

/// Extracts trace context from message headers.
///
/// Returns an OpenTelemetry context that can be used to create child spans
/// linked to the producer trace.
///
/// # Arguments
///
/// * `headers` - The message headers containing trace context
#[cfg(feature = "telemetry")]
pub fn extract_context(headers: &crate::Headers) -> opentelemetry::Context {
    use opentelemetry::global;

    let mut carrier = std::collections::HashMap::new();
    for (key, value) in headers.iter() {
        if let Ok(val) = std::str::from_utf8(value) {
            carrier.insert(key.clone(), val.to_string());
        }
    }

    global::get_text_map_propagator(|propagator| propagator.extract(&carrier))
}

// ── No-op implementations when the telemetry feature is disabled ────

/// No-op version of `trace_produce` when `telemetry` feature is not enabled.
///
/// Executes the action directly without any tracing overhead.
#[cfg(not(feature = "telemetry"))]
pub async fn trace_produce<F, Fut, T>(_topic: &str, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    f().await
}

/// No-op version of `trace_consume` when `telemetry` feature is not enabled.
///
/// Executes the action directly without any tracing overhead.
#[cfg(not(feature = "telemetry"))]
pub async fn trace_consume<F, Fut, T>(_topic: &str, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    f().await
}

/// No-op version of `trace_process` when `telemetry` feature is not enabled.
///
/// Executes the action directly without any tracing overhead.
#[cfg(not(feature = "telemetry"))]
pub async fn trace_process<F, Fut, T>(_topic: &str, _partition: i32, _offset: i64, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    f().await
}

/// No-op version of `inject_context` when `telemetry` feature is not enabled.
#[cfg(not(feature = "telemetry"))]
pub fn inject_context(_headers: &mut crate::Headers) {
    // No-op
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_trace_produce_executes_action() {
        let result = trace_produce("test-topic", || async { 42 }).await;
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_trace_consume_executes_action() {
        let result = trace_consume("test-topic", || async { "consumed" }).await;
        assert_eq!(result, "consumed");
    }

    #[tokio::test]
    async fn test_trace_process_executes_action() {
        let result = trace_process("test-topic", 0, 100, || async { vec![1, 2, 3] }).await;
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_inject_context_noop() {
        let mut headers = crate::Headers::new();
        inject_context(&mut headers);
        // Without the telemetry feature, this is a no-op
        // With the feature, it would inject traceparent headers
    }

    #[tokio::test]
    async fn test_trace_produce_returns_result_type() {
        let result: Result<i32, &str> = trace_produce("topic", || async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_trace_consume_returns_result_type() {
        let result: Result<String, &str> =
            trace_consume("topic", || async { Ok("data".to_string()) }).await;
        assert_eq!(result.unwrap(), "data");
    }

    #[tokio::test]
    async fn test_trace_process_returns_result_type() {
        let result: Result<Vec<u8>, &str> =
            trace_process("topic", 0, 0, || async { Ok(vec![1, 2, 3]) }).await;
        assert_eq!(result.unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_trace_produce_with_different_topics() {
        let r1 = trace_produce("orders", || async { 1 }).await;
        let r2 = trace_produce("events", || async { 2 }).await;
        assert_eq!(r1, 1);
        assert_eq!(r2, 2);
    }

    #[tokio::test]
    async fn test_trace_process_with_partition_and_offset() {
        let result = trace_process("topic", 5, 12345, || async { "processed" }).await;
        assert_eq!(result, "processed");
    }

    #[tokio::test]
    async fn test_inject_context_does_not_modify_headers() {
        let mut headers = crate::Headers::new();
        headers.add("custom-key", b"custom-value");
        inject_context(&mut headers);
        // Original header still present
        assert_eq!(headers.get("custom-key"), Some(b"custom-value".as_slice()));
    }

    #[test]
    fn test_metrics_collector_new() {
        let m = MetricsCollector::new();
        assert_eq!(
            m.messages_produced.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            m.messages_consumed.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            m.errors_total.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_metrics_collector_default() {
        let m = MetricsCollector::default();
        assert_eq!(
            m.bytes_sent.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            m.bytes_received.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_metrics_collector_record_produce() {
        let m = MetricsCollector::new();
        m.record_produce(256);
        assert_eq!(
            m.messages_produced.load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            m.bytes_sent.load(std::sync::atomic::Ordering::Relaxed),
            256
        );
    }

    #[test]
    fn test_metrics_collector_record_consume() {
        let m = MetricsCollector::new();
        m.record_consume(512);
        assert_eq!(
            m.messages_consumed.load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            m.bytes_received.load(std::sync::atomic::Ordering::Relaxed),
            512
        );
    }

    #[test]
    fn test_metrics_collector_record_error() {
        let m = MetricsCollector::new();
        m.record_error();
        m.record_error();
        assert_eq!(
            m.errors_total.load(std::sync::atomic::Ordering::Relaxed),
            2
        );
    }

    #[test]
    fn test_metrics_collector_accumulation() {
        let m = MetricsCollector::new();
        for _ in 0..10 {
            m.record_produce(100);
        }
        assert_eq!(
            m.messages_produced.load(std::sync::atomic::Ordering::Relaxed),
            10
        );
        assert_eq!(
            m.bytes_sent.load(std::sync::atomic::Ordering::Relaxed),
            1000
        );
    }
}


/// Simple metrics collector for tracking SDK operations.
#[derive(Debug, Default)]
pub struct MetricsCollector {
    pub messages_produced: std::sync::atomic::AtomicU64,
    pub messages_consumed: std::sync::atomic::AtomicU64,
    pub errors_total: std::sync::atomic::AtomicU64,
    pub bytes_sent: std::sync::atomic::AtomicU64,
    pub bytes_received: std::sync::atomic::AtomicU64,
}

impl MetricsCollector {
    /// Creates a new metrics collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a successful produce operation.
    pub fn record_produce(&self, bytes: u64) {
        self.messages_produced.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Records a successful consume operation.
    pub fn record_consume(&self, bytes: u64) {
        self.messages_consumed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_received.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Records an error.
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}
