//! Streamline SQL query compatibility-types example.
//!
//! Version 0.4.0 exposes request/response types and endpoint helpers, but SQL
//! execution is not implemented and returns `ErrorKind::Unsupported`.
//!
//! Run:
//!   cargo run --example query_usage

use streamline_client::query::{QueryClient, QueryRequest};
use streamline_client::ErrorKind;

#[tokio::main]
async fn main() -> streamline_client::Result<()> {
    let http_url =
        std::env::var("STREAMLINE_HTTP").unwrap_or_else(|_| "http://localhost:9094".into());

    let query_client = QueryClient::new(&http_url);

    let request = QueryRequest::new("SELECT * FROM topic('events') ORDER BY offset DESC")
        .with_timeout(5000)
        .with_max_rows(3);
    println!(
        "\n--- With options: timeout={}ms, max_rows={} ---",
        request.timeout_ms, request.max_rows
    );

    println!("Query endpoint: {}", query_client.query_url()?);
    println!("Explain endpoint: {}", query_client.explain_url()?);

    let error = query_client
        .execute(&request)
        .await
        .expect_err("query execution must fail closed in version 0.4.0");
    assert_eq!(error.kind, ErrorKind::Unsupported);
    println!("{error}");

    Ok(())
}
