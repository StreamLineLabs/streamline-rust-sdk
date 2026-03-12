//! Streamline SQL Query Example
//!
//! Demonstrates using Streamline's embedded analytics engine (DuckDB)
//! to run SQL queries on streaming data.
//!
//! Prerequisites:
//!   - Streamline server running
//!   - Add `streamline-client` to Cargo.toml
//!
//! Run:
//!   cargo run --example query_usage

use streamline_client::query::{QueryClient, QueryRequest};
use streamline_client::{Headers, Streamline, TopicConfig};

#[tokio::main]
async fn main() -> streamline_client::Result<()> {
    let bootstrap =
        std::env::var("STREAMLINE_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".into());
    let http_url =
        std::env::var("STREAMLINE_HTTP").unwrap_or_else(|_| "http://localhost:9094".into());

    // Produce sample data
    let client = Streamline::builder()
        .bootstrap_servers(&bootstrap)
        .build()
        .await?;

    let admin = client.admin();
    let _ = admin
        .create_topic(TopicConfig::new("events").partitions(1))
        .await;

    let producer = client.producer::<String, String>();
    for i in 0..10 {
        let value = format!(
            r#"{{"user":"user-{i}","action":"click","value":{}}}"#,
            i * 10
        );
        let key = format!("user-{i}");
        producer
            .send("events", key, value, Headers::new())
            .await?;
    }
    println!("Produced 10 events");

    // Query the data
    let query_client = QueryClient::new(&http_url);

    // Simple SELECT
    println!("\n--- All events (limit 5) ---");
    let url = query_client.query_url();
    println!("Query endpoint: {url}");

    // Using QueryRequest
    let request = QueryRequest::new("SELECT * FROM topic('events') LIMIT 5");
    println!("SQL: {}", request.sql);

    // Query with custom options
    let request = QueryRequest::new("SELECT * FROM topic('events') ORDER BY offset DESC")
        .with_timeout(5000)
        .with_max_rows(3);
    println!(
        "\n--- With options: timeout={}ms, max_rows={} ---",
        request.timeout_ms,
        request.max_rows
    );

    // Explain query plan
    println!("\n--- Query plan endpoint ---");
    let explain_url = query_client.explain_url();
    println!("Explain endpoint: {explain_url}");

    Ok(())
}
