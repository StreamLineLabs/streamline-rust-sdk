//! Example: Consuming messages from Streamline.

use std::time::Duration;
use streamline_client::Streamline;

#[tokio::main]
async fn main() -> Result<(), streamline_client::Error> {
    // Create a client
    let client = Streamline::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .await?;

    // Create a consumer with group ID
    let mut consumer = client
        .consumer::<String, String>("my-topic")
        .group_id("my-consumer-group")
        .auto_offset_reset("earliest")
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
                record.topic, record.partition, record.offset, record.key
            );
        }

        // Commit offsets
        consumer.commit().await?;
    }
}
