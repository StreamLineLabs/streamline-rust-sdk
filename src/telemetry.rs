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
//! ```rust,no_run
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
}
