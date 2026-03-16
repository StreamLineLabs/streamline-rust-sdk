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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_creates_zeroed_metrics() {
        let metrics = ClientMetrics::new();
        let snap = metrics.snapshot();
        assert_eq!(snap.messages_produced, 0);
        assert_eq!(snap.messages_consumed, 0);
        assert_eq!(snap.bytes_sent, 0);
        assert_eq!(snap.bytes_received, 0);
        assert_eq!(snap.errors_total, 0);
        assert_eq!(snap.produce_latency_avg_ms, 0.0);
        assert_eq!(snap.consume_latency_avg_ms, 0.0);
    }

    #[test]
    fn test_default_creates_zeroed_metrics() {
        let metrics = ClientMetrics::default();
        let snap = metrics.snapshot();
        assert_eq!(snap.messages_produced, 0);
        assert_eq!(snap.bytes_sent, 0);
    }

    #[test]
    fn test_record_produce_increments_counters() {
        let metrics = ClientMetrics::new();
        metrics.record_produce(5, 1024, 10.0);
        let snap = metrics.snapshot();
        assert_eq!(snap.messages_produced, 5);
        assert_eq!(snap.bytes_sent, 1024);
    }

    #[test]
    fn test_record_consume_increments_counters() {
        let metrics = ClientMetrics::new();
        metrics.record_consume(3, 512, 5.0);
        let snap = metrics.snapshot();
        assert_eq!(snap.messages_consumed, 3);
        assert_eq!(snap.bytes_received, 512);
    }

    #[test]
    fn test_record_error_increments_counter() {
        let metrics = ClientMetrics::new();
        metrics.record_error();
        metrics.record_error();
        metrics.record_error();
        let snap = metrics.snapshot();
        assert_eq!(snap.errors_total, 3);
    }

    #[test]
    fn test_snapshot_returns_correct_values() {
        let metrics = ClientMetrics::new();
        metrics.record_produce(2, 100, 10.0);
        metrics.record_consume(1, 50, 5.0);
        metrics.record_error();

        let snap = metrics.snapshot();
        assert_eq!(snap.messages_produced, 2);
        assert_eq!(snap.messages_consumed, 1);
        assert_eq!(snap.bytes_sent, 100);
        assert_eq!(snap.bytes_received, 50);
        assert_eq!(snap.errors_total, 1);
    }

    #[test]
    fn test_produce_latency_average() {
        let metrics = ClientMetrics::new();
        metrics.record_produce(1, 100, 10.0);
        metrics.record_produce(1, 100, 20.0);
        metrics.record_produce(1, 100, 30.0);
        let snap = metrics.snapshot();
        assert!((snap.produce_latency_avg_ms - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_consume_latency_average() {
        let metrics = ClientMetrics::new();
        metrics.record_consume(1, 50, 4.0);
        metrics.record_consume(1, 50, 8.0);
        let snap = metrics.snapshot();
        assert!((snap.consume_latency_avg_ms - 6.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_latency_avg_zero_when_no_records() {
        let metrics = ClientMetrics::new();
        let snap = metrics.snapshot();
        assert_eq!(snap.produce_latency_avg_ms, 0.0);
        assert_eq!(snap.consume_latency_avg_ms, 0.0);
    }

    #[test]
    fn test_multiple_operations_accumulate() {
        let metrics = ClientMetrics::new();
        for _ in 0..100 {
            metrics.record_produce(1, 64, 1.0);
        }
        for _ in 0..50 {
            metrics.record_consume(2, 128, 2.0);
        }
        for _ in 0..10 {
            metrics.record_error();
        }
        let snap = metrics.snapshot();
        assert_eq!(snap.messages_produced, 100);
        assert_eq!(snap.bytes_sent, 6400);
        assert_eq!(snap.messages_consumed, 100);
        assert_eq!(snap.bytes_received, 6400);
        assert_eq!(snap.errors_total, 10);
    }

    #[test]
    fn test_uptime_is_nonzero() {
        let metrics = ClientMetrics::new();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let snap = metrics.snapshot();
        assert!(snap.uptime_ms >= 1);
    }

    #[test]
    fn test_metrics_snapshot_clone() {
        let metrics = ClientMetrics::new();
        metrics.record_produce(1, 100, 5.0);
        let snap = metrics.snapshot();
        let snap2 = snap.clone();
        assert_eq!(snap.messages_produced, snap2.messages_produced);
        assert_eq!(snap.bytes_sent, snap2.bytes_sent);
    }

    #[test]
    fn test_thread_safety() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<ClientMetrics>();
        assert_sync::<ClientMetrics>();
    }
}
