//! Circuit breaker pattern for resilient operations.
//!
//! Tracks failures and temporarily stops requests to a failing service,
//! allowing it time to recover before retrying.
//!
//! # States
//!
//! - **Closed**: Requests flow normally. Failures are counted.
//! - **Open**: Requests are rejected immediately. After a timeout, transitions to HalfOpen.
//! - **HalfOpen**: A limited number of probe requests are allowed. Successes close the circuit;
//!   any failure re-opens it.

use crate::error::{Error, ErrorKind};
use std::fmt;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// Acquires the mutex, recovering from poison if another thread panicked.
fn acquire<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Current state of the circuit breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Requests flow normally
    Closed,
    /// Requests are rejected immediately
    Open,
    /// A limited number of probe requests are allowed
    HalfOpen,
}

impl fmt::Display for CircuitState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "CLOSED"),
            Self::Open => write!(f, "OPEN"),
            Self::HalfOpen => write!(f, "HALF_OPEN"),
        }
    }
}

/// Configuration for the circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit.
    pub failure_threshold: u32,
    /// Number of consecutive successes in half-open state required to close the circuit.
    pub success_threshold: u32,
    /// How long to wait before transitioning from open to half-open.
    pub open_timeout: Duration,
    /// Maximum number of probe requests allowed in half-open state.
    pub half_open_max_requests: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            open_timeout: Duration::from_secs(30),
            half_open_max_requests: 3,
        }
    }
}

/// Internal mutable state of the circuit breaker.
struct CircuitBreakerState {
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    half_open_count: u32,
    last_failure_at: Option<Instant>,
    last_state_change: Instant,
}

/// Circuit breaker for resilient operations.
///
/// Thread-safe via internal `Mutex`. Only retryable errors (connection, timeout)
/// should be recorded as failures — non-retryable errors (auth, not found)
/// should not trip the circuit breaker.
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    inner: Mutex<CircuitBreakerState>,
}

impl CircuitBreaker {
    /// Creates a new circuit breaker with the given configuration.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(CircuitBreakerState {
                state: CircuitState::Closed,
                failure_count: 0,
                success_count: 0,
                half_open_count: 0,
                last_failure_at: None,
                last_state_change: Instant::now(),
            }),
        }
    }

    /// Creates a circuit breaker with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(CircuitBreakerConfig::default())
    }

    /// Checks if a request should be allowed through.
    /// Returns `Ok(())` if the request can proceed, or an error if the circuit is open.
    pub fn check(&self) -> crate::Result<()> {
        let mut state = acquire(&self.inner);
        match state.state {
            CircuitState::Closed => Ok(()),
            CircuitState::Open => {
                if let Some(last_failure) = state.last_failure_at {
                    if last_failure.elapsed() >= self.config.open_timeout {
                        self.transition_locked(&mut state, CircuitState::HalfOpen);
                        state.half_open_count = 1;
                        return Ok(());
                    }
                }
                Err(Error::new(
                    ErrorKind::Connection,
                    "circuit breaker is open — too many recent failures",
                )
                .with_hint(
                    "The client detected repeated failures and is temporarily pausing requests. \
                     It will retry automatically.",
                ))
            }
            CircuitState::HalfOpen => {
                if state.half_open_count < self.config.half_open_max_requests {
                    state.half_open_count += 1;
                    Ok(())
                } else {
                    Err(Error::new(
                        ErrorKind::Connection,
                        "circuit breaker is half-open — probe limit reached",
                    )
                    .with_hint("Waiting for probe requests to succeed before allowing more."))
                }
            }
        }
    }

    /// Records a successful request.
    pub fn record_success(&self) {
        let mut state = acquire(&self.inner);
        match state.state {
            CircuitState::Closed => {
                state.failure_count = 0;
            }
            CircuitState::HalfOpen => {
                state.success_count += 1;
                if state.success_count >= self.config.success_threshold {
                    self.transition_locked(&mut state, CircuitState::Closed);
                }
            }
            CircuitState::Open => {}
        }
    }

    /// Records a failed request.
    ///
    /// Only retryable errors should be recorded — non-retryable errors (auth, not found)
    /// should not trip the circuit breaker.
    pub fn record_failure(&self) {
        let mut state = acquire(&self.inner);
        state.last_failure_at = Some(Instant::now());

        match state.state {
            CircuitState::Closed => {
                state.failure_count += 1;
                if state.failure_count >= self.config.failure_threshold {
                    self.transition_locked(&mut state, CircuitState::Open);
                }
            }
            CircuitState::HalfOpen => {
                self.transition_locked(&mut state, CircuitState::Open);
            }
            CircuitState::Open => {}
        }
    }

    /// Returns the current circuit state (auto-transitions from Open→HalfOpen if timed out).
    pub fn state(&self) -> CircuitState {
        let mut state = acquire(&self.inner);
        if state.state == CircuitState::Open {
            if let Some(last_failure) = state.last_failure_at {
                if last_failure.elapsed() >= self.config.open_timeout {
                    self.transition_locked(&mut state, CircuitState::HalfOpen);
                }
            }
        }
        state.state
    }

    /// Manually resets the circuit breaker to closed state.
    pub fn reset(&self) {
        let mut state = acquire(&self.inner);
        self.transition_locked(&mut state, CircuitState::Closed);
    }

    /// Returns the current failure and success counts.
    pub fn counts(&self) -> (u32, u32) {
        let state = acquire(&self.inner);
        (state.failure_count, state.success_count)
    }

    fn transition_locked(&self, state: &mut CircuitBreakerState, to: CircuitState) {
        if state.state == to {
            return;
        }
        state.state = to;
        state.last_state_change = Instant::now();
        state.failure_count = 0;
        state.success_count = 0;
        state.half_open_count = 0;
    }
}

impl fmt::Debug for CircuitBreaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = acquire(&self.inner);
        f.debug_struct("CircuitBreaker")
            .field("state", &state.state)
            .field("failure_count", &state.failure_count)
            .field("success_count", &state.success_count)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast_config() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            open_timeout: Duration::from_millis(50),
            half_open_max_requests: 2,
        }
    }

    #[test]
    fn test_initial_state_is_closed() {
        let cb = CircuitBreaker::with_defaults();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.check().is_ok());
    }

    #[test]
    fn test_stays_closed_below_threshold() {
        let cb = CircuitBreaker::new(fast_config());
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.check().is_ok());
    }

    #[test]
    fn test_opens_at_threshold() {
        let cb = CircuitBreaker::new(fast_config());
        for _ in 0..3 {
            cb.record_failure();
        }
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(cb.check().is_err());
    }

    #[test]
    fn test_success_resets_failure_count() {
        let cb = CircuitBreaker::new(fast_config());
        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // resets count
        cb.record_failure();
        cb.record_failure();
        // Should still be closed (only 2 consecutive failures after reset)
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_transitions_to_half_open_after_timeout() {
        let cb = CircuitBreaker::new(fast_config());
        for _ in 0..3 {
            cb.record_failure();
        }
        assert_eq!(cb.state(), CircuitState::Open);

        std::thread::sleep(Duration::from_millis(60));

        // Should auto-transition to HalfOpen
        assert_eq!(cb.state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_half_open_to_closed_on_success() {
        let cb = CircuitBreaker::new(fast_config());
        for _ in 0..3 {
            cb.record_failure();
        }
        std::thread::sleep(Duration::from_millis(60));

        // Now half-open, allow probe requests
        assert!(cb.check().is_ok());
        cb.record_success();
        cb.record_success();

        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_to_open_on_failure() {
        let cb = CircuitBreaker::new(fast_config());
        for _ in 0..3 {
            cb.record_failure();
        }
        std::thread::sleep(Duration::from_millis(60));

        assert!(cb.check().is_ok()); // transitions to half-open
        cb.record_failure(); // any failure re-opens

        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_half_open_probe_limit() {
        let cb = CircuitBreaker::new(fast_config());
        for _ in 0..3 {
            cb.record_failure();
        }
        std::thread::sleep(Duration::from_millis(60));

        assert!(cb.check().is_ok()); // probe 1 (transitions to half-open + increments)
        assert!(cb.check().is_ok()); // probe 2 (half_open_max_requests = 2)
        assert!(cb.check().is_err()); // probe 3 — rejected
    }

    #[test]
    fn test_manual_reset() {
        let cb = CircuitBreaker::new(fast_config());
        for _ in 0..3 {
            cb.record_failure();
        }
        assert_eq!(cb.state(), CircuitState::Open);

        cb.reset();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.check().is_ok());
    }

    #[test]
    fn test_counts() {
        let cb = CircuitBreaker::new(fast_config());
        cb.record_failure();
        cb.record_failure();
        let (failures, successes) = cb.counts();
        assert_eq!(failures, 2);
        assert_eq!(successes, 0);
    }

    #[test]
    fn test_display_states() {
        assert_eq!(CircuitState::Closed.to_string(), "CLOSED");
        assert_eq!(CircuitState::Open.to_string(), "OPEN");
        assert_eq!(CircuitState::HalfOpen.to_string(), "HALF_OPEN");
    }

    #[test]
    fn test_debug_output() {
        let cb = CircuitBreaker::with_defaults();
        let debug = format!("{:?}", cb);
        assert!(debug.contains("CircuitBreaker"));
        assert!(debug.contains("Closed"));
    }

    #[test]
    fn test_default_config_values() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.failure_threshold, 5);
        assert_eq!(config.success_threshold, 2);
        assert_eq!(config.open_timeout, Duration::from_secs(30));
        assert_eq!(config.half_open_max_requests, 3);
    }

    #[test]
    fn test_error_message_on_open() {
        let cb = CircuitBreaker::new(fast_config());
        for _ in 0..3 {
            cb.record_failure();
        }
        let err = cb.check().unwrap_err();
        assert_eq!(err.kind, ErrorKind::Connection);
        assert!(err.message.contains("circuit breaker"));
        assert!(err.hint.is_some());
    }
}
