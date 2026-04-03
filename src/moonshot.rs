//! Moonshot HTTP clients.
//!
//! Native Rust wrappers for the Streamline broker moonshot HTTP API:
//! M1 memory, M2 semantic search, M4 contracts + attestation, M5 branches.
//!
//! Enable with the `moonshot` feature.
//!
//! ```toml
//! [dependencies]
//! streamline-client = { version = "0.2", features = ["moonshot"] }
//! ```

use std::collections::HashMap;
use std::time::Duration;

use base64::Engine;
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Errors returned by the moonshot clients.
#[derive(Debug, thiserror::Error)]
pub enum MoonshotError {
    #[error("invalid argument: {0}")]
    InvalidArg(String),
    #[error("http error: status={status} body={body}")]
    Http { status: u16, body: String },
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("decode: {0}")]
    Decode(String),
}

impl MoonshotError {
    fn from_status(status: StatusCode, body: String) -> Self {
        let trimmed = if body.len() > 512 { body[..512].to_string() } else { body };
        Self::Http {
            status: status.as_u16(),
            body: trimmed,
        }
    }
}

/// Shared client options.
#[derive(Debug, Clone)]
pub struct MoonshotOptions {
    pub http_url: String,
    pub timeout: Duration,
}

impl MoonshotOptions {
    pub fn new(http_url: impl Into<String>) -> Self {
        Self {
            http_url: http_url.into().trim_end_matches('/').to_string(),
            timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone)]
struct HttpBase {
    base: String,
    client: Client,
}

impl HttpBase {
    fn new(opts: &MoonshotOptions) -> Result<Self, MoonshotError> {
        let client = Client::builder()
            .timeout(opts.timeout)
            .build()
            .map_err(MoonshotError::Transport)?;
        Ok(Self {
            base: opts.http_url.clone(),
            client,
        })
    }

    async fn request<T: for<'de> Deserialize<'de>>(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Option<T>, MoonshotError> {
        let url = format!("{}{}", self.base, path);
        let mut req = self.client.request(method, &url);
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req.send().await?;
        let status = resp.status();
        let bytes = resp.bytes().await?;
        if !status.is_success() {
            let text = String::from_utf8_lossy(&bytes).into_owned();
            return Err(MoonshotError::from_status(status, text));
        }
        if bytes.is_empty() {
            return Ok(None);
        }
        let parsed: T = serde_json::from_slice(&bytes)
            .map_err(|e| MoonshotError::Decode(e.to_string()))?;
        Ok(Some(parsed))
    }
}

fn require_non_empty(name: &str, v: &str) -> Result<(), MoonshotError> {
    if v.trim().is_empty() {
        return Err(MoonshotError::InvalidArg(format!("{} must not be empty", name)));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// M5 — Branches
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct BranchView {
    pub id: String,
    #[serde(default)]
    pub created_at_ms: u64,
    #[serde(default)]
    pub message_count: u64,
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub fork_offset: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BranchMessage {
    #[serde(default)]
    pub offset: i64,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub headers: HashMap<String, Value>,
    #[serde(default)]
    pub timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct BranchCreateOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fork_offset: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, Value>>,
}

#[derive(Debug, Clone)]
pub struct BranchAdminClient {
    http: HttpBase,
}

impl BranchAdminClient {
    pub fn new(opts: MoonshotOptions) -> Result<Self, MoonshotError> {
        Ok(Self {
            http: HttpBase::new(&opts)?,
        })
    }

    pub async fn create(
        &self,
        topic: &str,
        name: &str,
        opts: BranchCreateOptions,
    ) -> Result<BranchView, MoonshotError> {
        require_non_empty("topic", topic)?;
        require_non_empty("name", name)?;
        let mut body = json!({ "topic": topic, "name": name });
        if let Some(pid) = opts.parent_id {
            body["parent_id"] = Value::String(pid);
        }
        if let Some(off) = opts.fork_offset {
            body["fork_offset"] = json!(off);
        }
        if let Some(md) = opts.metadata {
            body["metadata"] = json!(md);
        }
        let v: BranchView = self
            .http
            .request(Method::POST, "/api/v1/branches", Some(&body))
            .await?
            .ok_or_else(|| MoonshotError::Decode("empty response".into()))?;
        Ok(v)
    }

    pub async fn list(&self, topic: Option<&str>) -> Result<Vec<BranchView>, MoonshotError> {
        let path = match topic {
            Some(t) => format!("/api/v1/branches?topic={}", urlencoding(t)),
            None => "/api/v1/branches".to_string(),
        };
        let raw: Value = self
            .http
            .request(Method::GET, &path, None)
            .await?
            .unwrap_or(Value::Null);
        Ok(parse_branch_list(&raw))
    }

    pub async fn get(&self, id: &str) -> Result<BranchView, MoonshotError> {
        require_non_empty("id", id)?;
        let path = format!("/api/v1/branches/{}", urlencoding(id));
        self.http
            .request(Method::GET, &path, None)
            .await?
            .ok_or_else(|| MoonshotError::Decode("empty response".into()))
    }

    pub async fn delete(&self, id: &str) -> Result<(), MoonshotError> {
        require_non_empty("id", id)?;
        let path = format!("/api/v1/branches/{}", urlencoding(id));
        let _: Option<Value> = self.http.request(Method::DELETE, &path, None).await?;
        Ok(())
    }

    pub async fn append(
        &self,
        id: &str,
        key: Option<&str>,
        value: &str,
    ) -> Result<BranchMessage, MoonshotError> {
        require_non_empty("id", id)?;
        let mut body = json!({ "value": value });
        if let Some(k) = key {
            body["key"] = Value::String(k.to_string());
        }
        let path = format!("/api/v1/branches/{}/messages", urlencoding(id));
        self.http
            .request(Method::POST, &path, Some(&body))
            .await?
            .ok_or_else(|| MoonshotError::Decode("empty response".into()))
    }

    pub async fn messages(
        &self,
        id: &str,
        limit: Option<u32>,
    ) -> Result<Vec<BranchMessage>, MoonshotError> {
        require_non_empty("id", id)?;
        let path = match limit {
            Some(l) => format!("/api/v1/branches/{}/messages?limit={}", urlencoding(id), l),
            None => format!("/api/v1/branches/{}/messages", urlencoding(id)),
        };
        let raw: Value = self
            .http
            .request(Method::GET, &path, None)
            .await?
            .unwrap_or(Value::Null);
        Ok(parse_branch_messages(&raw))
    }
}

fn parse_branch_list(raw: &Value) -> Vec<BranchView> {
    let arr = if raw.is_array() {
        raw.as_array().cloned().unwrap_or_default()
    } else if let Some(items) = raw.get("items").and_then(|v| v.as_array()) {
        items.clone()
    } else {
        return Vec::new();
    };
    arr.into_iter()
        .filter_map(|v| serde_json::from_value::<BranchView>(v).ok())
        .collect()
}

fn parse_branch_messages(raw: &Value) -> Vec<BranchMessage> {
    let arr = if raw.is_array() {
        raw.as_array().cloned().unwrap_or_default()
    } else if let Some(items) = raw.get("messages").and_then(|v| v.as_array()) {
        items.clone()
    } else {
        return Vec::new();
    };
    arr.into_iter()
        .filter_map(|v| serde_json::from_value::<BranchMessage>(v).ok())
        .collect()
}

// ---------------------------------------------------------------------------
// M4 — Contracts validation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct ValidationFailure {
    #[serde(default)]
    pub field_path: String,
    #[serde(default)]
    pub expected: Option<String>,
    #[serde(default)]
    pub actual: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub valid: bool,
    pub schema_id: Option<i64>,
    pub errors: Vec<ValidationFailure>,
}

#[derive(Debug, Clone)]
pub enum ContractValue {
    Json(Value),
    Bytes(Vec<u8>),
    String(String),
}

#[derive(Debug, Clone)]
pub struct ContractsClient {
    http: HttpBase,
}

impl ContractsClient {
    pub fn new(opts: MoonshotOptions) -> Result<Self, MoonshotError> {
        Ok(Self {
            http: HttpBase::new(&opts)?,
        })
    }

    pub async fn validate(
        &self,
        contract: Value,
        value: ContractValue,
    ) -> Result<ValidationResult, MoonshotError> {
        let mut body = json!({ "contract": contract });
        match value {
            ContractValue::Json(v) => body["value"] = v,
            ContractValue::String(s) => body["value_string"] = Value::String(s),
            ContractValue::Bytes(b) => body["value_string"] = Value::String(
                String::from_utf8(b).map_err(|e| MoonshotError::Decode(e.to_string()))?,
            ),
        }

        let url = format!("{}/api/v1/contracts/validate", self.http.base);
        let resp = self
            .http
            .client
            .post(&url)
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        let bytes = resp.bytes().await?;
        let valid = status.is_success();
        if !valid && status != StatusCode::BAD_REQUEST {
            let text = String::from_utf8_lossy(&bytes).into_owned();
            return Err(MoonshotError::from_status(status, text));
        }
        let parsed: Value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        let schema_id = parsed.get("schema_id").and_then(|v| v.as_i64());
        let errors: Vec<ValidationFailure> = parsed
            .get("errors")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| serde_json::from_value::<ValidationFailure>(v.clone()).ok())
                    .collect()
            })
            .unwrap_or_default();
        Ok(ValidationResult {
            valid,
            schema_id,
            errors,
        })
    }
}

// ---------------------------------------------------------------------------
// M4 — Attestation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct SignedAttestation {
    pub key_id: String,
    pub algorithm: String,
    pub timestamp_ms: i64,
    pub payload_sha256: String,
    pub signature_b64: String,
    pub header_name: String,
    pub header_value: String,
}

#[derive(Debug, Clone)]
pub struct SignParams {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub value_string: Option<String>,
    pub timestamp_ms: i64,
    pub key_id: Option<String>,
    pub algorithm: Option<String>,
}

#[derive(Debug, Clone)]
pub struct VerifyParams {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub value_string: Option<String>,
    pub timestamp_ms: i64,
    pub signature_b64: String,
    pub key_id: Option<String>,
    pub algorithm: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Attestor {
    http: HttpBase,
    default_key_id: String,
    default_algorithm: String,
}

pub const ATTESTATION_HEADER: &str = "streamline-attest";

impl Attestor {
    pub fn new(opts: MoonshotOptions) -> Result<Self, MoonshotError> {
        Self::with_defaults(opts, "broker-0", "ed25519")
    }

    pub fn with_defaults(
        opts: MoonshotOptions,
        key_id: impl Into<String>,
        algorithm: impl Into<String>,
    ) -> Result<Self, MoonshotError> {
        Ok(Self {
            http: HttpBase::new(&opts)?,
            default_key_id: key_id.into(),
            default_algorithm: algorithm.into(),
        })
    }

    pub async fn sign(&self, p: SignParams) -> Result<SignedAttestation, MoonshotError> {
        if p.value.is_some() && p.value_string.is_some() {
            return Err(MoonshotError::InvalidArg(
                "set either value or value_string, not both".into(),
            ));
        }
        require_non_empty("topic", &p.topic)?;
        let mut body = json!({
            "topic": p.topic,
            "partition": p.partition,
            "offset": p.offset,
            "timestamp_ms": p.timestamp_ms,
            "key_id": p.key_id.unwrap_or_else(|| self.default_key_id.clone()),
            "algorithm": p.algorithm.unwrap_or_else(|| self.default_algorithm.clone()),
        });
        if let Some(b) = p.key {
            body["key_b64"] = Value::String(base64::engine::general_purpose::STANDARD.encode(b));
        }
        if let Some(b) = p.value {
            body["value_b64"] = Value::String(base64::engine::general_purpose::STANDARD.encode(b));
        }
        if let Some(s) = p.value_string {
            body["value"] = Value::String(s);
        }
        self.http
            .request(Method::POST, "/api/v1/attest/sign", Some(&body))
            .await?
            .ok_or_else(|| MoonshotError::Decode("empty response".into()))
    }

    pub async fn verify(&self, p: VerifyParams) -> Result<bool, MoonshotError> {
        if p.value.is_some() && p.value_string.is_some() {
            return Err(MoonshotError::InvalidArg(
                "set either value or value_string, not both".into(),
            ));
        }
        require_non_empty("topic", &p.topic)?;
        require_non_empty("signature_b64", &p.signature_b64)?;
        let mut body = json!({
            "topic": p.topic,
            "partition": p.partition,
            "offset": p.offset,
            "timestamp_ms": p.timestamp_ms,
            "signature_b64": p.signature_b64,
            "key_id": p.key_id.unwrap_or_else(|| self.default_key_id.clone()),
            "algorithm": p.algorithm.unwrap_or_else(|| self.default_algorithm.clone()),
        });
        if let Some(b) = p.key {
            body["key_b64"] = Value::String(base64::engine::general_purpose::STANDARD.encode(b));
        }
        if let Some(b) = p.value {
            body["value_b64"] = Value::String(base64::engine::general_purpose::STANDARD.encode(b));
        }
        if let Some(s) = p.value_string {
            body["value"] = Value::String(s);
        }
        let v: Value = self
            .http
            .request(Method::POST, "/api/v1/attest/verify", Some(&body))
            .await?
            .unwrap_or(Value::Null);
        Ok(v.get("valid").and_then(|v| v.as_bool()).unwrap_or(false))
    }
}

// ---------------------------------------------------------------------------
// M2 — Semantic search
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct SearchHit {
    #[serde(default)]
    pub partition: i32,
    #[serde(default)]
    pub offset: i64,
    #[serde(default)]
    pub score: f64,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchResult {
    #[serde(default)]
    pub hits: Vec<SearchHit>,
    #[serde(default)]
    pub took_ms: u64,
    #[serde(default)]
    pub total: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct SearchOptions {
    pub k: u32,
    pub filter: Option<Value>,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self { k: 10, filter: None }
    }
}

#[derive(Debug, Clone)]
pub struct SemanticSearchClient {
    http: HttpBase,
}

impl SemanticSearchClient {
    pub fn new(opts: MoonshotOptions) -> Result<Self, MoonshotError> {
        Ok(Self {
            http: HttpBase::new(&opts)?,
        })
    }

    pub async fn search(
        &self,
        topic: &str,
        query: &str,
        opts: SearchOptions,
    ) -> Result<SearchResult, MoonshotError> {
        require_non_empty("topic", topic)?;
        require_non_empty("query", query)?;
        if opts.k == 0 || opts.k > 1000 {
            return Err(MoonshotError::InvalidArg("k must be in 1..=1000".into()));
        }
        let mut body = json!({ "query": query, "k": opts.k });
        if let Some(f) = opts.filter {
            body["filter"] = f;
        }
        let path = format!("/api/v1/topics/{}/search", urlencoding(topic));
        self.http
            .request(Method::POST, &path, Some(&body))
            .await?
            .ok_or_else(|| MoonshotError::Decode("empty response".into()))
    }
}

// ---------------------------------------------------------------------------
// M1 — Memory
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryKind {
    Observation,
    Fact,
    Procedure,
}

impl MemoryKind {
    fn wire(self) -> &'static str {
        match self {
            MemoryKind::Observation => "observation",
            MemoryKind::Fact => "fact",
            MemoryKind::Procedure => "procedure",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WrittenEntry {
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub offset: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RecalledMemory {
    #[serde(default)]
    pub tier: String,
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub offset: i64,
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    pub score: f64,
    #[serde(default)]
    pub timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct RememberParams {
    pub agent_id: String,
    pub kind: MemoryKind,
    pub content: String,
    pub salience: Option<f64>,
    pub skill: Option<String>,
    pub metadata: Option<HashMap<String, Value>>,
}

#[derive(Debug, Clone)]
pub struct RecallParams {
    pub agent_id: String,
    pub query: String,
    pub k_episodic: u32,
    pub k_semantic: u32,
}

#[derive(Debug, Clone)]
pub struct MemoryClient {
    http: HttpBase,
}

impl MemoryClient {
    pub fn new(opts: MoonshotOptions) -> Result<Self, MoonshotError> {
        Ok(Self {
            http: HttpBase::new(&opts)?,
        })
    }

    pub async fn remember(&self, p: RememberParams) -> Result<Vec<WrittenEntry>, MoonshotError> {
        require_non_empty("agent_id", &p.agent_id)?;
        require_non_empty("content", &p.content)?;
        if matches!(p.kind, MemoryKind::Procedure)
            && p.skill.as_deref().map_or(true, |s| s.trim().is_empty())
        {
            return Err(MoonshotError::InvalidArg(
                "skill is required when kind=procedure".into(),
            ));
        }
        if let Some(s) = p.salience {
            if !(0.0..=1.0).contains(&s) {
                return Err(MoonshotError::InvalidArg("salience must be in [0,1]".into()));
            }
        }
        let mut body = json!({
            "agent_id": p.agent_id,
            "kind": p.kind.wire(),
            "content": p.content,
        });
        if let Some(s) = p.salience {
            body["salience"] = json!(s);
        }
        if let Some(s) = p.skill {
            body["skill"] = Value::String(s);
        }
        if let Some(m) = p.metadata {
            body["metadata"] = json!(m);
        }
        let v: Value = self
            .http
            .request(Method::POST, "/api/v1/memory/remember", Some(&body))
            .await?
            .unwrap_or(Value::Null);
        Ok(v.get("written")
            .and_then(|x| x.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| serde_json::from_value::<WrittenEntry>(v.clone()).ok())
                    .collect()
            })
            .unwrap_or_default())
    }

    pub async fn recall(&self, p: RecallParams) -> Result<Vec<RecalledMemory>, MoonshotError> {
        require_non_empty("agent_id", &p.agent_id)?;
        require_non_empty("query", &p.query)?;
        let mut body = json!({
            "agent_id": p.agent_id,
            "query": p.query,
        });
        if p.k_episodic > 0 {
            body["k_episodic"] = json!(p.k_episodic);
        }
        if p.k_semantic > 0 {
            body["k_semantic"] = json!(p.k_semantic);
        }
        let v: Value = self
            .http
            .request(Method::POST, "/api/v1/memory/recall", Some(&body))
            .await?
            .unwrap_or(Value::Null);
        Ok(v.get("hits")
            .and_then(|x| x.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| serde_json::from_value::<RecalledMemory>(v.clone()).ok())
                    .collect()
            })
            .unwrap_or_default())
    }
}

fn urlencoding(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn mk_server() -> MockServer {
        MockServer::start().await
    }

    fn opts(server: &MockServer) -> MoonshotOptions {
        MoonshotOptions::new(server.uri())
    }

    #[tokio::test]
    async fn branches_create_returns_view() {
        let s = mk_server().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/branches"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "orders/exp-a",
                "created_at_ms": 1000,
                "message_count": 0,
            })))
            .mount(&s)
            .await;
        let c = BranchAdminClient::new(opts(&s)).unwrap();
        let v = c
            .create("orders", "exp-a", BranchCreateOptions::default())
            .await
            .unwrap();
        assert_eq!(v.id, "orders/exp-a");
        assert_eq!(v.created_at_ms, 1000);
    }

    #[tokio::test]
    async fn branches_create_rejects_empty() {
        let s = mk_server().await;
        let c = BranchAdminClient::new(opts(&s)).unwrap();
        let err = c
            .create("", "x", BranchCreateOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, MoonshotError::InvalidArg(_)));
    }

    #[tokio::test]
    async fn branches_get_404_returns_http_err() {
        let s = mk_server().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/branches/orders%2Fmissing"))
            .respond_with(ResponseTemplate::new(404).set_body_string("{\"error\":\"missing\"}"))
            .mount(&s)
            .await;
        let c = BranchAdminClient::new(opts(&s)).unwrap();
        let err = c.get("orders/missing").await.unwrap_err();
        assert!(matches!(err, MoonshotError::Http { status: 404, .. }));
    }

    #[tokio::test]
    async fn contracts_validate_200_valid() {
        let s = mk_server().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/contracts/validate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"schema_id": 7})))
            .mount(&s)
            .await;
        let c = ContractsClient::new(opts(&s)).unwrap();
        let r = c
            .validate(json!({"name": "c"}), ContractValue::Json(json!({"id": "x"})))
            .await
            .unwrap();
        assert!(r.valid);
        assert_eq!(r.schema_id, Some(7));
    }

    #[tokio::test]
    async fn contracts_validate_400_failures() {
        let s = mk_server().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/contracts/validate"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "schema_id": 7,
                "errors": [{"field_path": "id", "expected": "string", "actual": "int"}],
            })))
            .mount(&s)
            .await;
        let c = ContractsClient::new(opts(&s)).unwrap();
        let r = c
            .validate(json!({"name": "c"}), ContractValue::Json(json!({"id": 1})))
            .await
            .unwrap();
        assert!(!r.valid);
        assert_eq!(r.errors.len(), 1);
        assert_eq!(r.errors[0].field_path, "id");
    }

    #[tokio::test]
    async fn attest_sign_and_verify() {
        let s = mk_server().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/attest/sign"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "key_id": "broker-0",
                "algorithm": "ed25519",
                "timestamp_ms": 1,
                "payload_sha256": "deadbeef",
                "signature_b64": "AAAA",
                "header_name": "streamline-attest",
                "header_value": "v",
            })))
            .mount(&s)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v1/attest/verify"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"valid": true})))
            .mount(&s)
            .await;

        let a = Attestor::new(opts(&s)).unwrap();
        let sig = a
            .sign(SignParams {
                topic: "t".into(),
                partition: 0,
                offset: 1,
                key: None,
                value: Some(b"hi".to_vec()),
                value_string: None,
                timestamp_ms: 1,
                key_id: None,
                algorithm: None,
            })
            .await
            .unwrap();
        assert_eq!(sig.signature_b64, "AAAA");

        let ok = a
            .verify(VerifyParams {
                topic: "t".into(),
                partition: 0,
                offset: 1,
                key: None,
                value: Some(b"hi".to_vec()),
                value_string: None,
                timestamp_ms: 1,
                signature_b64: "AAAA".into(),
                key_id: None,
                algorithm: None,
            })
            .await
            .unwrap();
        assert!(ok);
    }

    #[tokio::test]
    async fn attest_sign_rejects_both_value_forms() {
        let s = mk_server().await;
        let a = Attestor::new(opts(&s)).unwrap();
        let err = a
            .sign(SignParams {
                topic: "t".into(),
                partition: 0,
                offset: 1,
                key: None,
                value: Some(vec![1]),
                value_string: Some("x".into()),
                timestamp_ms: 1,
                key_id: None,
                algorithm: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, MoonshotError::InvalidArg(_)));
    }

    #[tokio::test]
    async fn search_returns_hits() {
        let s = mk_server().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/topics/logs/search"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "hits": [{"partition": 1, "offset": 5, "score": 0.9}],
                "took_ms": 12,
            })))
            .mount(&s)
            .await;
        let c = SemanticSearchClient::new(opts(&s)).unwrap();
        let r = c
            .search("logs", "payment failure", SearchOptions { k: 5, filter: None })
            .await
            .unwrap();
        assert_eq!(r.took_ms, 12);
        assert_eq!(r.hits.len(), 1);
        assert!((r.hits[0].score - 0.9).abs() < 1e-9);
    }

    #[tokio::test]
    async fn search_validation() {
        let s = mk_server().await;
        let c = SemanticSearchClient::new(opts(&s)).unwrap();
        assert!(c.search("t", "", SearchOptions::default()).await.is_err());
        assert!(c
            .search(
                "t",
                "q",
                SearchOptions { k: 1001, filter: None }
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn memory_remember_returns_entries() {
        let s = mk_server().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/memory/remember"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "written": [
                    {"topic": "a-ep", "offset": 1},
                    {"topic": "a-sem", "offset": 2},
                ]
            })))
            .mount(&s)
            .await;
        let c = MemoryClient::new(opts(&s)).unwrap();
        let out = c
            .remember(RememberParams {
                agent_id: "a".into(),
                kind: MemoryKind::Fact,
                content: "x".into(),
                salience: Some(0.8),
                skill: None,
                metadata: None,
            })
            .await
            .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[1].offset, 2);
    }

    #[tokio::test]
    async fn memory_procedure_requires_skill() {
        let s = mk_server().await;
        let c = MemoryClient::new(opts(&s)).unwrap();
        let err = c
            .remember(RememberParams {
                agent_id: "a".into(),
                kind: MemoryKind::Procedure,
                content: "x".into(),
                salience: None,
                skill: None,
                metadata: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, MoonshotError::InvalidArg(_)));
    }

    #[tokio::test]
    async fn memory_recall_maps_tier() {
        let s = mk_server().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/memory/recall"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "hits": [{
                    "tier": "semantic",
                    "topic": "a",
                    "offset": 5,
                    "content": "hi",
                    "score": 0.7,
                }]
            })))
            .mount(&s)
            .await;
        let c = MemoryClient::new(opts(&s)).unwrap();
        let hits = c
            .recall(RecallParams {
                agent_id: "a".into(),
                query: "q".into(),
                k_episodic: 0,
                k_semantic: 0,
            })
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].tier, "semantic");
    }
}
