//! Example: Producing messages to Streamline.

use streamline_client::Streamline;

#[tokio::main]
async fn main() -> Result<(), streamline_client::Error> {
    // Create a client
    let client = Streamline::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .await?;

    // Produce a simple message
    let metadata = client.produce("my-topic", "key-1", "Hello, Streamline!").await?;
    println!(
        "Produced to topic={}, partition={}, offset={}",
        metadata.topic, metadata.partition, metadata.offset
    );

    // Produce with headers
    let headers = streamline_client::Headers::builder()
        .add("trace-id", b"abc-123")
        .add("source", b"example")
        .build();
    let metadata = client
        .produce_with_headers("my-topic", "key-2", r#"{"event":"test"}"#, headers)
        .await?;
    println!("Produced with headers at offset {}", metadata.offset);

    // Batch produce
    use streamline_client::ProducerRecord;
    let producer = client.producer::<String, String>();
    let records = vec![
        ProducerRecord::new("k1".into(), "v1".into()),
        ProducerRecord::new("k2".into(), "v2".into()),
        ProducerRecord::value_only("v3".into()),
    ];
    let results = producer.send_batch("my-topic", records).await?;
    println!("Produced {} messages in batch", results.len());

    Ok(())
}
