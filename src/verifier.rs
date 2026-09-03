//! Local Ed25519 attestation verifier for Streamline consumer records.
//!
//! Verifies `streamline-attest` headers locally using explicitly trusted
//! Ed25519 public keys, without any network calls.
//!
//! Verification is bound to the record it travels with. An attestation is
//! accepted only when all of the following hold:
//!
//! * the envelope's `topic`, `partition`, and `offset` match the record the
//!   header arrived on, so a valid attestation cannot be replayed onto a
//!   different record;
//! * the envelope's `payload_sha256` matches the SHA-256 of the record's
//!   actual value bytes, so the payload cannot be swapped;
//! * the envelope's `key_id` resolves to an explicitly trusted verifying key
//!   — an unknown key ID is never accepted, even if its signature is
//!   internally consistent;
//! * the Ed25519 signature over the canonical string verifies under that key.
//!
//! The signed canonical string is
//! `topic|partition|offset|payload_sha256|schema_id|timestamp_ms|key_id`.
//! Note that `contract_id` is **not** part of it: it is transported in the
//! envelope but not covered by the signature, so it is exposed as
//! [`VerificationResult::unverified_contract_id`] and must never be treated as
//! authenticated. Binding it would require a coordinated canonical-string
//! change across the broker and every SDK.
//!
//! Any failed check yields a [`VerificationResult`] with `verified == false`
//! and a machine-readable [`FailureReason`]; use
//! [`Verifier::require_verified`] to turn a failed verification into an
//! [`ErrorKind::AttestationFailed`] error instead.

use std::collections::HashMap;
use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use ed25519_dalek::{Signature, Verifier as DalekVerifier, VerifyingKey};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{ConsumerRecord, Error, ErrorKind, Result};

/// Kafka header name carrying the attestation envelope.
pub const ATTEST_HEADER: &str = "streamline-attest";

/// Why an attestation was not accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureReason {
    /// The record carried no `streamline-attest` header.
    MissingHeader,
    /// The envelope described a different topic/partition/offset than the
    /// record it arrived on.
    RecordMismatch {
        /// Human-readable description of the mismatching field.
        detail: String,
    },
    /// The envelope's payload digest did not match the record's value bytes.
    PayloadDigestMismatch,
    /// The envelope's `key_id` is not an explicitly trusted key.
    UntrustedKeyId {
        /// The key ID presented by the envelope.
        key_id: String,
    },
    /// The signature was malformed (not base64, or not a valid Ed25519 signature).
    MalformedSignature,
    /// The signature did not verify under the trusted key.
    SignatureMismatch,
}

impl std::fmt::Display for FailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingHeader => write!(f, "record has no '{ATTEST_HEADER}' header"),
            Self::RecordMismatch { detail } => {
                write!(f, "attestation does not bind this record: {detail}")
            }
            Self::PayloadDigestMismatch => {
                write!(f, "attestation payload digest does not match record value")
            }
            Self::UntrustedKeyId { key_id } => {
                write!(f, "attestation key ID '{key_id}' is not trusted")
            }
            Self::MalformedSignature => write!(f, "attestation signature is malformed"),
            Self::SignatureMismatch => write!(f, "attestation signature did not verify"),
        }
    }
}

/// The result of an attestation verification.
#[derive(Debug, Clone)]
pub struct VerificationResult {
    /// Whether the attestation was fully accepted: record binding, payload
    /// digest, trusted key ID, and Ed25519 signature all checked out.
    pub verified: bool,
    /// The key_id from the attestation envelope (empty when unavailable).
    pub producer_id: String,
    /// Schema id (`None` when zero / absent).
    pub schema_id: Option<i32>,
    /// Contract id carried by the envelope.
    ///
    /// **Not authenticated.** This field is outside the signed canonical
    /// string, so anyone who can rewrite the header can choose its value.
    /// Never make a trust decision based on it.
    pub unverified_contract_id: Option<String>,
    /// Attestation timestamp in epoch milliseconds.
    pub timestamp_ms: i64,
    /// Why verification failed, when `verified` is `false`.
    pub failure_reason: Option<FailureReason>,
}

impl VerificationResult {
    fn failed(reason: FailureReason) -> Self {
        Self {
            verified: false,
            producer_id: String::new(),
            schema_id: None,
            unverified_contract_id: None,
            timestamp_ms: 0,
            failure_reason: Some(reason),
        }
    }
}

/// Resolves an attestation `key_id` to the verifying key trusted for it.
///
/// Returning `None` rejects the attestation: there is no implicit fallback
/// key. Implementations must only return keys they explicitly trust for the
/// requested key ID.
pub trait KeyResolver: Send + Sync {
    /// Returns the trusted verifying key for `key_id`, or `None` when the key
    /// ID is unknown or untrusted.
    fn resolve(&self, key_id: &str) -> Option<VerifyingKey>;
}

impl KeyResolver for HashMap<String, VerifyingKey> {
    fn resolve(&self, key_id: &str) -> Option<VerifyingKey> {
        self.get(key_id).copied()
    }
}

impl<F> KeyResolver for F
where
    F: Fn(&str) -> Option<VerifyingKey> + Send + Sync,
{
    fn resolve(&self, key_id: &str) -> Option<VerifyingKey> {
        self(key_id)
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

/// Verifies `streamline-attest` headers on consumed records using explicitly
/// trusted local Ed25519 public keys. No network calls are made.
///
/// # Example
///
/// ```rust,no_run
/// use streamline_client::verifier::Verifier;
/// use ed25519_dalek::VerifyingKey;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let key_bytes = [0u8; 32];
/// let public_key = VerifyingKey::from_bytes(&key_bytes)?;
/// // The key ID is explicit: attestations signed by any other key ID are rejected.
/// let verifier = Verifier::new("prod-signer-1", public_key);
///
/// // let result = verifier.verify(&consumer_record)?;
/// // if result.verified { /* ... */ }
/// # Ok(())
/// # }
/// ```
pub struct Verifier {
    resolver: Arc<dyn KeyResolver>,
}

impl std::fmt::Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier")
            .field("resolver", &"<key resolver>")
            .finish()
    }
}

impl Verifier {
    /// Creates a verifier that trusts exactly one key ID and its key.
    pub fn new(trusted_key_id: impl Into<String>, public_key: VerifyingKey) -> Self {
        let mut keys = HashMap::new();
        keys.insert(trusted_key_id.into(), public_key);
        Self {
            resolver: Arc::new(keys),
        }
    }

    /// Creates a verifier from an explicit trusted key-ID map.
    ///
    /// # Errors
    /// Returns [`ErrorKind::InvalidConfiguration`] when the map is empty: a
    /// verifier with no trusted keys can never accept an attestation and is
    /// almost always a configuration mistake.
    pub fn with_trusted_keys(
        keys: impl IntoIterator<Item = (String, VerifyingKey)>,
    ) -> Result<Self> {
        let keys: HashMap<String, VerifyingKey> = keys.into_iter().collect();
        if keys.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidConfiguration,
                "Attestation verifier requires at least one trusted key ID",
            )
            .with_hint("Register the key IDs you trust with Verifier::with_trusted_keys"));
        }
        Ok(Self {
            resolver: Arc::new(keys),
        })
    }

    /// Creates a verifier backed by a custom [`KeyResolver`], for example one
    /// reading from a local trust store.
    pub fn with_resolver(resolver: Arc<dyn KeyResolver>) -> Self {
        Self { resolver }
    }

    /// Verify the attestation on a consumer record.
    ///
    /// Extracts the `streamline-attest` header, parses the base64-encoded JSON
    /// attestation, checks that it binds this record's bytes, topic,
    /// partition, and offset, resolves the trusted key for its `key_id`, and
    /// verifies the Ed25519 signature.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Serialization`] when the header is not valid
    /// base64-encoded JSON. Verification failures are reported as
    /// `Ok(VerificationResult { verified: false, .. })` with a
    /// [`FailureReason`].
    pub fn verify<K, V: AsRef<[u8]>>(
        &self,
        record: &ConsumerRecord<K, V>,
    ) -> Result<VerificationResult> {
        let raw = match record.headers.get(ATTEST_HEADER) {
            Some(bytes) => bytes,
            None => return Ok(VerificationResult::failed(FailureReason::MissingHeader)),
        };

        let decoded = BASE64.decode(raw).map_err(|e| {
            Error::new(
                ErrorKind::Serialization,
                format!("base64 decode failed: {e}"),
            )
        })?;

        let env: AttestationEnvelope = serde_json::from_slice(&decoded).map_err(|e| {
            Error::new(
                ErrorKind::Serialization,
                format!("attestation JSON parse failed: {e}"),
            )
        })?;

        // 1. The envelope must describe the record it arrived on.
        if env.topic != record.topic {
            return Ok(VerificationResult::failed(FailureReason::RecordMismatch {
                detail: format!(
                    "envelope topic '{}' != record topic '{}'",
                    env.topic, record.topic
                ),
            }));
        }
        if env.partition != record.partition {
            return Ok(VerificationResult::failed(FailureReason::RecordMismatch {
                detail: format!(
                    "envelope partition {} != record partition {}",
                    env.partition, record.partition
                ),
            }));
        }
        if env.offset != record.offset {
            return Ok(VerificationResult::failed(FailureReason::RecordMismatch {
                detail: format!(
                    "envelope offset {} != record offset {}",
                    env.offset, record.offset
                ),
            }));
        }

        // 2. The envelope must describe the record's actual value bytes.
        let actual_digest = hex_encode(&Sha256::digest(record.value.as_ref()));
        if !constant_time_eq_ignore_ascii_case(&actual_digest, &env.payload_sha256) {
            return Ok(VerificationResult::failed(
                FailureReason::PayloadDigestMismatch,
            ));
        }

        // 3. The signing key ID must be explicitly trusted.
        let Some(public_key) = self.resolver.resolve(&env.key_id) else {
            return Ok(VerificationResult::failed(FailureReason::UntrustedKeyId {
                key_id: env.key_id,
            }));
        };

        // 4. The signature must verify over the canonical, record-bound string.
        let canonical = canonical_bytes(
            &record.topic,
            record.partition,
            record.offset,
            &actual_digest,
            env.schema_id,
            env.timestamp_ms,
            &env.key_id,
        );

        let sig_bytes = match BASE64.decode(&env.signature) {
            Ok(bytes) => bytes,
            Err(_) => {
                return Ok(VerificationResult::failed(
                    FailureReason::MalformedSignature,
                ))
            }
        };
        let signature = match Signature::from_slice(&sig_bytes) {
            Ok(signature) => signature,
            Err(_) => {
                return Ok(VerificationResult::failed(
                    FailureReason::MalformedSignature,
                ))
            }
        };

        if public_key.verify(canonical.as_bytes(), &signature).is_err() {
            return Ok(VerificationResult::failed(FailureReason::SignatureMismatch));
        }

        Ok(VerificationResult {
            verified: true,
            producer_id: env.key_id,
            schema_id: if env.schema_id != 0 {
                Some(env.schema_id)
            } else {
                None
            },
            unverified_contract_id: env.contract_id,
            timestamp_ms: env.timestamp_ms,
            failure_reason: None,
        })
    }

    /// Verifies the attestation and fails closed when it is not accepted.
    ///
    /// # Errors
    /// Returns [`ErrorKind::AttestationFailed`] when verification did not
    /// succeed, so callers that use `?` cannot accidentally consume an
    /// unattested record.
    pub fn require_verified<K, V: AsRef<[u8]>>(
        &self,
        record: &ConsumerRecord<K, V>,
    ) -> Result<VerificationResult> {
        let result = self.verify(record)?;
        if !result.verified {
            let reason = result
                .failure_reason
                .map(|reason| reason.to_string())
                .unwrap_or_else(|| "attestation not verified".to_string());
            return Err(Error::new(
                ErrorKind::AttestationFailed,
                format!(
                    "Attestation rejected for {}:{} offset {}: {reason}",
                    record.topic, record.partition, record.offset
                ),
            )
            .with_hint("Reject the record, or register the producing key ID as trusted"));
        }
        Ok(result)
    }
}

/// The canonical string covered by the attestation signature.
fn canonical_bytes(
    topic: &str,
    partition: i32,
    offset: i64,
    payload_sha256: &str,
    schema_id: i32,
    timestamp_ms: i64,
    key_id: &str,
) -> String {
    format!("{topic}|{partition}|{offset}|{payload_sha256}|{schema_id}|{timestamp_ms}|{key_id}")
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from_digit((byte >> 4) as u32, 16).unwrap_or('0'));
        out.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap_or('0'));
    }
    out
}

/// Compares two ASCII hex digests without leaking the matching prefix length
/// through early exit.
fn constant_time_eq_ignore_ascii_case(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in left.as_bytes().iter().zip(right.as_bytes()) {
        diff |= a.to_ascii_lowercase() ^ b.to_ascii_lowercase();
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Headers;
    use ed25519_dalek::Signer;
    use ed25519_dalek::SigningKey;

    const KEY_ID: &str = "key-1";
    const TOPIC: &str = "orders";
    const PARTITION: i32 = 0;
    const OFFSET: i64 = 42;
    const TIMESTAMP_MS: i64 = 1_700_000_000_000;

    struct EnvelopeOverrides {
        topic: String,
        partition: i32,
        offset: i64,
        key_id: String,
        payload_sha256: Option<String>,
        signature: Option<String>,
    }

    impl Default for EnvelopeOverrides {
        fn default() -> Self {
            Self {
                topic: TOPIC.to_string(),
                partition: PARTITION,
                offset: OFFSET,
                key_id: KEY_ID.to_string(),
                payload_sha256: None,
                signature: None,
            }
        }
    }

    fn make_attestation_header(
        signing_key: &SigningKey,
        value: &[u8],
        overrides: EnvelopeOverrides,
    ) -> String {
        let payload_sha256 = overrides
            .payload_sha256
            .unwrap_or_else(|| hex_encode(&Sha256::digest(value)));
        let schema_id = 7;

        let canonical = canonical_bytes(
            &overrides.topic,
            overrides.partition,
            overrides.offset,
            &payload_sha256,
            schema_id,
            TIMESTAMP_MS,
            &overrides.key_id,
        );
        let signature = overrides
            .signature
            .unwrap_or_else(|| BASE64.encode(signing_key.sign(canonical.as_bytes()).to_bytes()));

        let envelope = serde_json::json!({
            "payload_sha256": payload_sha256,
            "topic": overrides.topic,
            "partition": overrides.partition,
            "offset": overrides.offset,
            "schema_id": schema_id,
            "timestamp_ms": TIMESTAMP_MS,
            "key_id": overrides.key_id,
            "signature": signature,
        });

        BASE64.encode(serde_json::to_vec(&envelope).expect("envelope serializes"))
    }

    fn record_with_header(
        value: &[u8],
        header_value: Option<&str>,
    ) -> ConsumerRecord<String, Vec<u8>> {
        let mut headers = Headers::new();
        if let Some(header_value) = header_value {
            headers.add(ATTEST_HEADER, header_value.as_bytes());
        }
        ConsumerRecord {
            topic: TOPIC.into(),
            partition: PARTITION,
            offset: OFFSET,
            timestamp: TIMESTAMP_MS,
            key: None,
            value: value.to_vec(),
            headers,
        }
    }

    #[test]
    fn test_hex_encode_matches_known_digest() {
        // SHA-256 of the empty string.
        assert_eq!(
            hex_encode(&Sha256::digest(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_verify_valid() {
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let value = b"{\"amount\":100}";
        let header = make_attestation_header(&signing_key, value, EnvelopeOverrides::default());
        let record = record_with_header(value, Some(&header));

        let verifier = Verifier::new(KEY_ID, signing_key.verifying_key());
        let result = verifier.verify(&record).unwrap();

        assert!(result.verified, "{:?}", result.failure_reason);
        assert_eq!(result.producer_id, KEY_ID);
        assert_eq!(result.schema_id, Some(7));
        assert_eq!(result.timestamp_ms, TIMESTAMP_MS);
        assert!(result.failure_reason.is_none());
        // contract_id is transported but not signed, so it stays absent here
        // and is surfaced under an explicitly unverified name.
        assert!(result.unverified_contract_id.is_none());
        assert!(verifier.require_verified(&record).is_ok());
    }

    #[test]
    fn test_verify_missing_header() {
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let record = record_with_header(b"", None);

        let verifier = Verifier::new(KEY_ID, signing_key.verifying_key());
        let result = verifier.verify(&record).unwrap();
        assert!(!result.verified);
        assert_eq!(result.failure_reason, Some(FailureReason::MissingHeader));
    }

    #[test]
    fn test_verify_rejects_tampered_payload() {
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let value = b"{\"amount\":100}";
        let header = make_attestation_header(&signing_key, value, EnvelopeOverrides::default());
        // Same signed envelope, different record bytes.
        let record = record_with_header(b"{\"amount\":999}", Some(&header));

        let verifier = Verifier::new(KEY_ID, signing_key.verifying_key());
        let result = verifier.verify(&record).unwrap();
        assert!(!result.verified);
        assert_eq!(
            result.failure_reason,
            Some(FailureReason::PayloadDigestMismatch)
        );

        let error = verifier.require_verified(&record).unwrap_err();
        assert_eq!(error.kind, ErrorKind::AttestationFailed);
    }

    #[test]
    fn test_verify_rejects_replay_onto_other_record_coordinates() {
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let value = b"payload";
        let verifier = Verifier::new(KEY_ID, signing_key.verifying_key());

        for (overrides, expected_fragment) in [
            (
                EnvelopeOverrides {
                    topic: "other-topic".to_string(),
                    ..Default::default()
                },
                "topic",
            ),
            (
                EnvelopeOverrides {
                    partition: 9,
                    ..Default::default()
                },
                "partition",
            ),
            (
                EnvelopeOverrides {
                    offset: 4_242,
                    ..Default::default()
                },
                "offset",
            ),
        ] {
            let header = make_attestation_header(&signing_key, value, overrides);
            let record = record_with_header(value, Some(&header));
            let result = verifier.verify(&record).unwrap();
            assert!(!result.verified);
            match result.failure_reason {
                Some(FailureReason::RecordMismatch { detail }) => {
                    assert!(detail.contains(expected_fragment), "{detail}");
                }
                other => panic!("expected record mismatch, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_verify_rejects_untrusted_key_id() {
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let value = b"payload";
        let header = make_attestation_header(
            &signing_key,
            value,
            EnvelopeOverrides {
                key_id: "rogue-key".to_string(),
                ..Default::default()
            },
        );
        let record = record_with_header(value, Some(&header));

        // The signature is internally valid, but the key ID is not trusted.
        let verifier = Verifier::new(KEY_ID, signing_key.verifying_key());
        let result = verifier.verify(&record).unwrap();
        assert!(!result.verified);
        assert_eq!(
            result.failure_reason,
            Some(FailureReason::UntrustedKeyId {
                key_id: "rogue-key".to_string()
            })
        );
    }

    #[test]
    fn test_verify_rejects_signature_from_other_key() {
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let other_key = SigningKey::generate(&mut rand::thread_rng());
        let value = b"payload";
        let header = make_attestation_header(&other_key, value, EnvelopeOverrides::default());
        let record = record_with_header(value, Some(&header));

        let verifier = Verifier::new(KEY_ID, signing_key.verifying_key());
        let result = verifier.verify(&record).unwrap();
        assert!(!result.verified);
        assert_eq!(
            result.failure_reason,
            Some(FailureReason::SignatureMismatch)
        );
    }

    #[test]
    fn test_verify_rejects_malformed_signature() {
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let value = b"payload";
        let header = make_attestation_header(
            &signing_key,
            value,
            EnvelopeOverrides {
                signature: Some("!!!not-base64!!!".to_string()),
                ..Default::default()
            },
        );
        let record = record_with_header(value, Some(&header));

        let verifier = Verifier::new(KEY_ID, signing_key.verifying_key());
        let result = verifier.verify(&record).unwrap();
        assert!(!result.verified);
        assert_eq!(
            result.failure_reason,
            Some(FailureReason::MalformedSignature)
        );
    }

    #[test]
    fn test_verify_rejects_malformed_header_encoding() {
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let record = record_with_header(b"payload", Some("!!!!"));
        let verifier = Verifier::new(KEY_ID, signing_key.verifying_key());
        let error = verifier.verify(&record).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Serialization);

        let not_json = BASE64.encode(b"definitely-not-json");
        let record = record_with_header(b"payload", Some(&not_json));
        let error = verifier.verify(&record).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Serialization);
    }

    #[test]
    fn test_with_trusted_keys_requires_at_least_one_key() {
        let error = Verifier::with_trusted_keys(Vec::new()).unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidConfiguration);
    }

    #[test]
    fn test_with_trusted_keys_and_custom_resolver() {
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let value = b"payload";
        let header = make_attestation_header(&signing_key, value, EnvelopeOverrides::default());
        let record = record_with_header(value, Some(&header));

        let verifier =
            Verifier::with_trusted_keys([(KEY_ID.to_string(), signing_key.verifying_key())])
                .unwrap();
        assert!(verifier.verify(&record).unwrap().verified);

        let verifying_key = signing_key.verifying_key();
        let resolver = move |key_id: &str| {
            if key_id == KEY_ID {
                Some(verifying_key)
            } else {
                None
            }
        };
        let verifier = Verifier::with_resolver(Arc::new(resolver));
        assert!(verifier.verify(&record).unwrap().verified);

        let deny_all = Verifier::with_resolver(Arc::new(|_: &str| None));
        assert!(!deny_all.verify(&record).unwrap().verified);
    }

    #[test]
    fn test_constant_time_compare() {
        assert!(constant_time_eq_ignore_ascii_case("ABcd", "abCD"));
        assert!(!constant_time_eq_ignore_ascii_case("abcd", "abce"));
        assert!(!constant_time_eq_ignore_ascii_case("abcd", "abc"));
    }
}
