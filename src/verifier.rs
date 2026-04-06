//! Local Ed25519 attestation verifier for Streamline consumer records.
//!
//! Verifies `streamline-attest` headers locally using an Ed25519 public key
//! without any network calls.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use ed25519_dalek::{Signature, VerifyingKey, Verifier as DalekVerifier};
use serde::Deserialize;

use crate::{ConsumerRecord, Error, ErrorKind, Result};

/// Kafka header name carrying the attestation envelope.
pub const ATTEST_HEADER: &str = "streamline-attest";

/// The result of an attestation verification.
#[derive(Debug, Clone)]
pub struct VerificationResult {
    /// Whether the Ed25519 signature was valid.
    pub verified: bool,
    /// The key_id from the attestation envelope.
    pub producer_id: String,
    /// Schema id (`None` when zero / absent).
    pub schema_id: Option<i32>,
    /// Optional contract id.
    pub contract_id: Option<String>,
    /// Attestation timestamp in epoch milliseconds.
    pub timestamp_ms: i64,
}

impl VerificationResult {
    fn failed() -> Self {
        Self {
            verified: false,
            producer_id: String::new(),
            schema_id: None,
            contract_id: None,
            timestamp_ms: 0,
        }
    }
}

/// Parsed attestation envelope from the header.
#[derive(Deserialize)]
struct AttestationEnvelope {
    payload_sha256: String,
    topic: String,
    partition: i32,
    offset: i64,
    schema_id: i32,
    timestamp_ms: i64,
    key_id: String,
    signature: String,
    #[serde(default)]
    contract_id: Option<String>,
}

/// Verifies `streamline-attest` headers on consumed records using a local
/// Ed25519 public key. No network calls are made.
///
/// # Example
///
/// ```rust,no_run
/// use streamline_client::verifier::Verifier;
/// use ed25519_dalek::VerifyingKey;
///
/// let key_bytes: [u8; 32] = /* load public key bytes */;
/// # let key_bytes = [0u8; 32];
/// let pub_key = VerifyingKey::from_bytes(&key_bytes).unwrap();
/// let verifier = Verifier::new(pub_key);
///
/// // let result = verifier.verify(&consumer_record)?;
/// // if result.verified { /* ... */ }
/// ```
pub struct Verifier {
    public_key: VerifyingKey,
}

impl Verifier {
    /// Creates a verifier backed by the given Ed25519 public key.
    pub fn new(public_key: VerifyingKey) -> Self {
        Self { public_key }
    }

    /// Verify the attestation on a consumer record.
    ///
    /// Extracts the `streamline-attest` header, parses the base64-encoded JSON
    /// attestation, reconstructs the canonical bytes, and verifies the Ed25519
    /// signature.
    pub fn verify<K, V>(&self, record: &ConsumerRecord<K, V>) -> Result<VerificationResult> {
        let raw = match record.headers.get(ATTEST_HEADER) {
            Some(bytes) => bytes,
            None => return Ok(VerificationResult::failed()),
        };

        let decoded = BASE64.decode(raw).map_err(|e| {
            Error::new(ErrorKind::Serialization, format!("base64 decode failed: {e}"))
        })?;

        let env: AttestationEnvelope = serde_json::from_slice(&decoded).map_err(|e| {
            Error::new(ErrorKind::Serialization, format!("attestation JSON parse failed: {e}"))
        })?;

        let canonical = format!(
            "{}|{}|{}|{}|{}|{}|{}",
            env.topic, env.partition, env.offset, env.payload_sha256,
            env.schema_id, env.timestamp_ms, env.key_id,
        );

        let sig_bytes = match BASE64.decode(&env.signature) {
            Ok(b) => b,
            Err(_) => return Ok(VerificationResult::failed()),
        };

        let signature = match Signature::from_slice(&sig_bytes) {
            Ok(s) => s,
            Err(_) => return Ok(VerificationResult::failed()),
        };

        let verified = self
            .public_key
            .verify(canonical.as_bytes(), &signature)
            .is_ok();

        Ok(VerificationResult {
            verified,
            producer_id: env.key_id,
            schema_id: if env.schema_id != 0 { Some(env.schema_id) } else { None },
            contract_id: env.contract_id,
            timestamp_ms: env.timestamp_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::Signer;
    use sha2::{Sha256, Digest};

    fn make_attestation_header(signing_key: &SigningKey, value: &[u8]) -> (String, Vec<u8>) {
        let payload_sha256 = hex::encode(Sha256::digest(value));
        let topic = "orders";
        let partition = 0;
        let offset = 42i64;
        let schema_id = 7;
        let timestamp_ms = 1_700_000_000_000i64;
        let key_id = "key-1";

        let canonical = format!(
            "{topic}|{partition}|{offset}|{payload_sha256}|{schema_id}|{timestamp_ms}|{key_id}"
        );
        let sig = signing_key.sign(canonical.as_bytes());
        let sig_b64 = BASE64.encode(sig.to_bytes());

        let envelope = serde_json::json!({
            "payload_sha256": payload_sha256,
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "schema_id": schema_id,
            "timestamp_ms": timestamp_ms,
            "key_id": key_id,
            "signature": sig_b64,
        });

        let header_value = BASE64.encode(serde_json::to_vec(&envelope).unwrap());
        (header_value, sig.to_bytes().to_vec())
    }

    #[test]
    fn test_verify_valid() {
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let verifying_key = signing_key.verifying_key();
        let value = b"{\"amount\":100}";

        let (header_value, _) = make_attestation_header(&signing_key, value);

        let mut headers = Headers::new();
        headers.add(ATTEST_HEADER, header_value.as_bytes());

        let record: ConsumerRecord<String, Vec<u8>> = ConsumerRecord {
            topic: "orders".into(),
            partition: 0,
            offset: 42,
            timestamp: 1_700_000_000_000,
            key: None,
            value: value.to_vec(),
            headers,
        };

        let verifier = Verifier::new(verifying_key);
        let result = verifier.verify(&record).unwrap();
        assert!(result.verified);
        assert_eq!(result.producer_id, "key-1");
        assert_eq!(result.schema_id, Some(7));
        assert_eq!(result.timestamp_ms, 1_700_000_000_000);
    }

    #[test]
    fn test_verify_missing_header() {
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let verifying_key = signing_key.verifying_key();

        let record: ConsumerRecord<String, Vec<u8>> = ConsumerRecord {
            topic: "orders".into(),
            partition: 0,
            offset: 42,
            timestamp: 0,
            key: None,
            value: vec![],
            headers: Headers::new(),
        };

        let verifier = Verifier::new(verifying_key);
        let result = verifier.verify(&record).unwrap();
        assert!(!result.verified);
    }
}
