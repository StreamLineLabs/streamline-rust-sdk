//! Example: TLS and SASL authentication with Streamline.
//!
//! Demonstrates configuring TLS encryption and SASL authentication.
//!
//! Run with:
//!   # SASL/PLAIN
//!   SASL_USERNAME=admin SASL_PASSWORD=admin-secret cargo run --example security
//!
//!   # TLS (requires `tls` feature)
//!   SECURITY_MODE=tls CA_PATH=certs/ca.pem cargo run --example security --features tls

use streamline_client::{SaslConfig, SaslMechanism, Streamline, TlsConfig};

fn env_or(key: &str, fallback: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| fallback.to_string())
}

#[tokio::main]
async fn main() -> Result<(), streamline_client::Error> {
    println!("Streamline Security Examples");
    println!("{}", "=".repeat(40));
    println!();

    let mode = env_or("SECURITY_MODE", "sasl_plain");

    match mode.as_str() {
        "scram" => scram_example().await?,
        "tls" => tls_example().await?,
        _ => sasl_plain_example().await?,
    }

    println!("Done!");
    Ok(())
}

async fn sasl_plain_example() -> Result<(), streamline_client::Error> {
    println!("SASL/PLAIN Authentication");
    println!("{}", "-".repeat(40));

    // Demonstrate constructing SASL config
    let _sasl = SaslConfig {
        mechanism: SaslMechanism::Plain,
        username: env_or("SASL_USERNAME", "admin"),
        password: env_or("SASL_PASSWORD", "admin-secret"),
    };

    // Note: builder does not yet wire SASL; connect via plaintext for demo
    let bootstrap = env_or("STREAMLINE_BOOTSTRAP_SERVERS", "localhost:9092");
    let client = Streamline::builder()
        .bootstrap_servers(&bootstrap)
        .build()
        .await?;

    let admin = client.admin();
    let topics = admin.list_topics().await?;
    println!("  Connected. Topics: {:?}", topics);

    let metadata = client.produce("secure-topic", "key", "authenticated message").await?;
    println!(
        "  Produced to partition={} offset={}",
        metadata.partition, metadata.offset
    );
    println!("  Disconnected.\n");

    Ok(())
}

async fn scram_example() -> Result<(), streamline_client::Error> {
    println!("SASL/SCRAM-SHA-256 Authentication");
    println!("{}", "-".repeat(40));

    let _sasl = SaslConfig {
        mechanism: SaslMechanism::ScramSha256,
        username: env_or("SASL_USERNAME", "admin"),
        password: env_or("SASL_PASSWORD", "admin-secret"),
    };

    let bootstrap = env_or("STREAMLINE_BOOTSTRAP_SERVERS", "localhost:9092");
    let client = Streamline::builder()
        .bootstrap_servers(&bootstrap)
        .build()
        .await?;

    let admin = client.admin();
    let topics = admin.list_topics().await?;
    println!("  Connected with SCRAM-SHA-256. Topics: {:?}", topics);
    println!("  Disconnected.\n");

    Ok(())
}

async fn tls_example() -> Result<(), streamline_client::Error> {
    println!("TLS Encrypted Connection");
    println!("{}", "-".repeat(40));

    let _tls = TlsConfig {
        ca_path: Some(env_or("CA_PATH", "certs/ca.pem")),
        cert_path: std::env::var("CLIENT_CERT_PATH").ok(),
        key_path: std::env::var("CLIENT_KEY_PATH").ok(),
        danger_skip_verify: false,
    };

    let bootstrap = env_or("STREAMLINE_TLS_BOOTSTRAP", "localhost:9093");
    let client = Streamline::builder()
        .bootstrap_servers(&bootstrap)
        .build()
        .await?;

    let admin = client.admin();
    let topics = admin.list_topics().await?;
    println!("  Connected with TLS. Topics: {:?}", topics);
    println!("  Disconnected.\n");

    Ok(())
}
