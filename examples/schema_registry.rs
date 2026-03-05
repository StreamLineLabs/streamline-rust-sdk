//! Schema Registry example demonstrating Avro schema management and
//! validated produce/consume with the Streamline Rust SDK.
//!
//! Ensure a Streamline server is running at localhost:9092 with the
//! schema registry enabled on port 9094 before running:
//!
//! ```sh
//! streamline --playground
//! cargo run --example schema_registry
//! ```

use serde::{Deserialize, Serialize};
use streamline_client::{
    schema::{SchemaRegistry, SchemaType},
    ConsumerConfig, ProducerRecord, Streamline,
};

/// User record matching the registered Avro schema.
#[derive(Debug, Serialize, Deserialize)]
struct User {
    id: i32,
    name: String,
    email: String,
    created_at: String,
}

/// Avro schema for the User record.
const USER_SCHEMA: &str = r#"{
  "type": "record",
  "name": "User",
  "namespace": "com.streamline.examples",
  "fields": [
    {"name": "id",         "type": "int"},
    {"name": "name",       "type": "string"},
    {"name": "email",      "type": "string"},
    {"name": "created_at", "type": "string"}
  ]
}"#;

const SUBJECT: &str = "users-value";
const TOPIC: &str = "users";

#[tokio::main]
async fn main() -> Result<(), streamline_client::Error> {
    let bootstrap_servers = std::env::var("STREAMLINE_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "localhost:9092".into());
    let registry_url = std::env::var("STREAMLINE_SCHEMA_REGISTRY_URL")
        .unwrap_or_else(|_| "http://localhost:9094".into());

    // === 1. Create the Streamline client ===
    let client = Streamline::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await?;

    // === 2. Create a schema registry client ===
    let registry = SchemaRegistry::new(&registry_url)?;

    // === 3. Register an Avro schema ===
    println!("=== Registering Schema ===");
    let schema_id = registry
        .register(SUBJECT, USER_SCHEMA, SchemaType::Avro)
        .await?;
    println!("Registered schema with id={schema_id} for subject={SUBJECT}");

    // Retrieve the schema back by id
    let retrieved = registry.get_schema(schema_id).await?;
    println!("Retrieved schema: {retrieved}");

    // === 4. Check schema compatibility ===
    println!("\n=== Checking Compatibility ===");
    let compatible = registry
        .check_compatibility(SUBJECT, USER_SCHEMA, SchemaType::Avro)
        .await?;
    println!("Schema compatible: {compatible}");

    // === 5. Produce messages with schema validation ===
    println!("\n=== Producing Messages with Schema ===");
    let producer = client.producer::<String, String>();

    for i in 0..5 {
        let user = User {
            id: i,
            name: format!("user-{i}"),
            email: format!("user{i}@example.com"),
            created_at: "2025-01-15T10:00:00Z".into(),
        };
        let value = serde_json::to_string(&user).expect("serialize user");

        let record = ProducerRecord::builder()
            .key(format!("user-{i}"))
            .value(value)
            .schema_id(schema_id)
            .build();

        let metadata = producer.send(TOPIC, record).await?;
        println!(
            "Produced user-{i} to partition={}, offset={}",
            metadata.partition, metadata.offset
        );
    }

    // === 6. Consume and deserialize with schema ===
    println!("\n=== Consuming Messages with Schema ===");
    let consumer_config = ConsumerConfig::builder()
        .group_id("rust-schema-group")
        .auto_offset_reset("earliest")
        .schema_registry_url(&registry_url)
        .build();

    let consumer = client.consumer::<String, String>(consumer_config).await?;
    consumer.subscribe(&[TOPIC]).await?;

    let records = consumer.poll(std::time::Duration::from_secs(5)).await?;
    for record in &records {
        let user: User = serde_json::from_str(record.value()).expect("deserialize user");
        println!(
            "Received: partition={}, offset={}, user={{id:{}, name:{}, email:{}}}",
            record.partition(),
            record.offset(),
            user.id,
            user.name,
            user.email,
        );
    }

    println!("\nDone!");
    Ok(())
}
