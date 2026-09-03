//! Example: fail-closed TLS and SASL configuration in version 0.4.0.
//!
//! Broker TLS and SASL protocol support is not implemented yet. Enabling
//! either configuration returns `ErrorKind::Unsupported` before any network
//! connection is opened.
//!
//! Run with:
//!   cargo run --example security --features tls,sasl

use streamline_client::{Error, ErrorKind, SaslConfig, SaslMechanism, Streamline, TlsConfig};

#[tokio::main]
async fn main() -> Result<(), streamline_client::Error> {
    assert_unsupported(
        Streamline::builder()
            .bootstrap_servers("localhost:9093")
            .tls_config(TlsConfig::default())
            .build()
            .await,
        "TLS broker transport",
    )?;

    assert_unsupported(
        Streamline::builder()
            .bootstrap_servers("localhost:9092")
            .sasl_config(SaslConfig {
                mechanism: SaslMechanism::Plain,
                username: String::new(),
                password: String::new(),
            })
            .build()
            .await,
        "SASL broker authentication",
    )?;

    Ok(())
}

fn assert_unsupported<T>(result: Result<T, Error>, operation: &str) -> Result<(), Error> {
    match result {
        Err(error) if error.kind == ErrorKind::Unsupported => {
            println!("{operation}: {error}");
            Ok(())
        }
        Err(error) => Err(error),
        Ok(_) => Err(Error::new(
            ErrorKind::Internal,
            format!("{operation} unexpectedly succeeded"),
        )),
    }
}
