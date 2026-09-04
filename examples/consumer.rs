//! Example: Consuming messages from Streamline.

use std::time::Duration;
use streamline_client::Streamline;

#[tokio::main]
async fn main() -> Result<(), streamline_client::Error> {
    // Create a client
    let client = Streamline::builder()
        .bootstrap_servers(
            &std::env::var("STREAMLINE_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".into()),
        )
        .build()
        .await?;

    // Version 0.4.0 supports direct partition assignment only. Consumer
    // groups, automatic commits, and latest-offset resolution fail closed.
    let mut consumer = client
        .consumer::<Vec<u8>, Vec<u8>>("my-topic")
        .partitions(vec![0])
        .auto_offset_reset("earliest")
        .enable_auto_commit(false)
        .max_poll_records(100)
        .build()
        .await?;

    // Subscribe to the topic
    consumer.subscribe().await?;

    // Poll for messages
    loop {
        let records = consumer.poll(Duration::from_secs(1)).await?;

        if records.is_empty() {
            println!("No new messages, waiting...");
            continue;
        }

        for record in &records {
            println!(
                "Received: topic={}, partition={}, offset={}, key={:?}",
                record.topic,
                record.partition,
                record.offset,
                record.key.as_ref().map(|k| String::from_utf8_lossy(k))
            );
        }
    }
}
