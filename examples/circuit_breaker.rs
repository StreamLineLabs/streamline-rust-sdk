//! Example: Circuit breaker for resilient message production.
//!
//! The circuit breaker prevents repeated attempts against a failing server.
//! After consecutive failures it "opens" and rejects requests immediately.
//!
//! Run with:
//!   cargo run --example circuit_breaker

use std::time::Duration;

use streamline_client::{CircuitBreaker, CircuitBreakerConfig, CircuitState, Streamline};

#[tokio::main]
async fn main() -> Result<(), streamline_client::Error> {
    println!("Circuit Breaker Example");
    println!("{}", "=".repeat(40));

    let bootstrap = std::env::var("STREAMLINE_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "localhost:9092".into());

    let client = Streamline::builder()
        .bootstrap_servers(&bootstrap)
        .with_circuit_breaker_config(CircuitBreakerConfig {
            failure_threshold: 5,
            success_threshold: 2,
            open_timeout: Duration::from_secs(10),
            half_open_max_requests: 3,
        })
        .build()
        .await?;

    // Also demonstrate standalone circuit breaker usage
    let cb = CircuitBreaker::new(CircuitBreakerConfig {
        failure_threshold: 5,
        success_threshold: 2,
        open_timeout: Duration::from_secs(10),
        half_open_max_requests: 3,
    });

    println!("Connected. Circuit state: {:?}", cb.state());

    // Create a topic
    let admin = client.admin();
    let _ = admin
        .create_topic(streamline_client::TopicConfig::new("cb-example").partitions(1))
        .await;

    // Send messages through the circuit breaker
    for i in 0..20 {
        if let Err(_) = cb.check() {
            println!("  Message {i}: REJECTED (circuit open)");
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        match client.produce("cb-example", &format!("key-{i}"), &format!("message-{i}")).await {
            Ok(metadata) => {
                cb.record_success();
                println!(
                    "  Message {i}: sent to partition={} offset={} (circuit: {:?})",
                    metadata.partition, metadata.offset, cb.state()
                );
            }
            Err(e) => {
                cb.record_failure();
                println!(
                    "  Message {i}: FAILED ({e}) (circuit: {:?})",
                    cb.state()
                );
            }
        }
    }

    // Show final state
    let (successes, failures) = cb.counts();
    println!("\nFinal circuit state: {:?}", cb.state());
    println!("Successes: {successes}, Failures: {failures}");

    if cb.state() == CircuitState::Open {
        cb.reset();
        println!("Circuit manually reset to: {:?}", cb.state());
    }

    println!("Done!");
    Ok(())
}
