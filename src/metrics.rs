//! Client-side metrics for monitoring SDK health and performance.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

/// Point-in-time snapshot of client metrics.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub messages_produced: u64,
    pub messages_consumed: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub errors_total: u64,
    pub produce_latency_avg_ms: f64,
    pub consume_latency_avg_ms: f64,
    pub uptime_ms: u64,
}

/// Client metrics collector with atomic counters.
pub struct ClientMetrics {
    messages_produced: AtomicU64,
    messages_consumed: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    errors_total: AtomicU64,
    latency: Mutex<LatencyTracker>,
    start_time: Instant,
}

struct LatencyTracker {
    produce_sum: f64,
    produce_count: u64,
    consume_sum: f64,
    consume_count: u64,
}

impl ClientMetrics {
    /// Create a new metrics collector.
    pub fn new() -> Self {
        Self {
            messages_produced: AtomicU64::new(0),
            messages_consumed: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            latency: Mutex::new(LatencyTracker {
                produce_sum: 0.0,
                produce_count: 0,
                consume_sum: 0.0,
                consume_count: 0,
            }),
            start_time: Instant::now(),
        }
    }

    /// Record a successful produce operation.
    pub fn record_produce(&self, message_count: u64, bytes: u64, latency_ms: f64) {
        self.messages_produced.fetch_add(message_count, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
        if let Ok(mut lt) = self.latency.lock() {
            lt.produce_sum += latency_ms;
            lt.produce_count += 1;
        }
    }

    /// Record a successful consume operation.
    pub fn record_consume(&self, message_count: u64, bytes: u64, latency_ms: f64) {
        self.messages_consumed.fetch_add(message_count, Ordering::Relaxed);
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
        if let Ok(mut lt) = self.latency.lock() {
            lt.consume_sum += latency_ms;
            lt.consume_count += 1;
        }
    }

    /// Record an error.
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a point-in-time snapshot.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let (produce_avg, consume_avg) = if let Ok(lt) = self.latency.lock() {
            let pa = if lt.produce_count > 0 { lt.produce_sum / lt.produce_count as f64 } else { 0.0 };
            let ca = if lt.consume_count > 0 { lt.consume_sum / lt.consume_count as f64 } else { 0.0 };
            (pa, ca)
        } else {
            (0.0, 0.0)
        };

        MetricsSnapshot {
            messages_produced: self.messages_produced.load(Ordering::Relaxed),
            messages_consumed: self.messages_consumed.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            produce_latency_avg_ms: produce_avg,
            consume_latency_avg_ms: consume_avg,
            uptime_ms: self.start_time.elapsed().as_millis() as u64,
        }
    }
}

impl Default for ClientMetrics {
    fn default() -> Self {
        Self::new()
    }
}
