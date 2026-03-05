//! Schema Registry client for Streamline.
//!
//! Enable the `schema-registry` feature (on by default) for HTTP-based
//! schema registry operations. For HTTPS endpoints, enable
//! `schema-registry-tls` which adds rustls TLS support.
//!
//! ```toml
//! [dependencies]
//! streamline-client = { version = "0.2", features = ["schema-registry-tls"] }
//! ```

use serde::{Deserialize, Serialize};

use crate::error::{Error, ErrorKind};

/// Schema types supported by the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaType {
    #[serde(rename = "AVRO")]
    Avro,
    #[serde(rename = "PROTOBUF")]
    Protobuf,
    #[serde(rename = "JSON")]
    Json,
}

/// A schema definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Schema ID assigned by the registry
    pub id: Option<i32>,
    /// Subject this schema belongs to
    pub subject: Option<String>,
    /// Schema version
    pub version: Option<i32>,
    /// Schema type
    #[serde(rename = "schemaType")]
    pub schema_type: SchemaType,
    /// Schema definition string
    pub schema: String,
}

/// Request to register a new schema.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterSchemaRequest {
    pub schema: String,
    pub schema_type: SchemaType,
}

/// Response from registering a schema.
#[derive(Debug, Clone, Deserialize)]
pub struct RegisterSchemaResponse {
    pub id: i32,
}

/// Result of a compatibility check.
#[derive(Debug, Clone, Deserialize)]
pub struct CompatibilityResult {
    pub is_compatible: bool,
}

/// Schema Registry client that communicates with the Streamline HTTP API.
pub struct SchemaRegistryClient {
    base_url: String,
    client: reqwest::Client,
}

impl SchemaRegistryClient {
    /// Create a new Schema Registry client.
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
        }
    }

    /// Get the base URL for this client.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Register a schema under the given subject and return the schema ID.
    pub async fn register(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
    ) -> Result<i32, Error> {
        let url = format!("{}/subjects/{}/versions", self.base_url, subject);
        let req = RegisterSchemaRequest {
            schema: schema.to_string(),
            schema_type,
        };

        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Connection, format!("schema register: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::new(
                ErrorKind::Schema,
                format!("register failed (HTTP {status}): {body}"),
            ));
        }

        let result: RegisterSchemaResponse = resp
            .json()
            .await
            .map_err(|e| Error::new(ErrorKind::Schema, format!("parse response: {e}")))?;

        Ok(result.id)
    }

    /// Retrieve a schema by its global ID.
    pub async fn get_schema(&self, id: i32) -> Result<Schema, Error> {
        let url = format!("{}/schemas/ids/{}", self.base_url, id);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Connection, format!("get schema: {e}")))?;

        if resp.status().as_u16() == 404 {
            return Err(Error::new(
                ErrorKind::Schema,
                format!("schema not found: {id}"),
            ));
        }

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::new(
                ErrorKind::Schema,
                format!("get schema failed (HTTP {status}): {body}"),
            ));
        }

        resp.json()
            .await
            .map_err(|e| Error::new(ErrorKind::Schema, format!("parse response: {e}")))
    }

    /// List all version numbers registered under a subject.
    pub async fn get_versions(&self, subject: &str) -> Result<Vec<i32>, Error> {
        let url = format!("{}/subjects/{}/versions", self.base_url, subject);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Connection, format!("get versions: {e}")))?;

        if resp.status().as_u16() == 404 {
            return Ok(Vec::new());
        }

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::new(
                ErrorKind::Schema,
                format!("get versions failed (HTTP {status}): {body}"),
            ));
        }

        resp.json()
            .await
            .map_err(|e| Error::new(ErrorKind::Schema, format!("parse response: {e}")))
    }

    /// Check if a schema is compatible with the latest version under a subject.
    pub async fn check_compatibility(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
    ) -> Result<bool, Error> {
        let url = format!(
            "{}/compatibility/subjects/{}/versions/latest",
            self.base_url, subject
        );
        let req = RegisterSchemaRequest {
            schema: schema.to_string(),
            schema_type,
        };

        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Connection, format!("compatibility check: {e}")))?;

        if resp.status().as_u16() == 404 {
            return Ok(true);
        }

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::new(
                ErrorKind::Schema,
                format!("compatibility check failed (HTTP {status}): {body}"),
            ));
        }

        let result: CompatibilityResult = resp
            .json()
            .await
            .map_err(|e| Error::new(ErrorKind::Schema, format!("parse response: {e}")))?;

        Ok(result.is_compatible)
    }

    /// List all registered subjects.
    pub async fn get_subjects(&self) -> Result<Vec<String>, Error> {
        let url = format!("{}/subjects", self.base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Connection, format!("get subjects: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::new(
                ErrorKind::Schema,
                format!("get subjects failed (HTTP {status}): {body}"),
            ));
        }

        resp.json()
            .await
            .map_err(|e| Error::new(ErrorKind::Schema, format!("parse response: {e}")))
    }

    /// Delete a subject and all its versions.
    pub async fn delete_subject(&self, subject: &str) -> Result<Vec<i32>, Error> {
        let url = format!("{}/subjects/{}", self.base_url, subject);
        let resp = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Connection, format!("delete subject: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::new(
                ErrorKind::Schema,
                format!("delete subject failed (HTTP {status}): {body}"),
            ));
        }

        resp.json()
            .await
            .map_err(|e| Error::new(ErrorKind::Schema, format!("parse response: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_registry_base_url() {
        let client = SchemaRegistryClient::new("http://localhost:9094");
        assert_eq!(client.base_url(), "http://localhost:9094");
    }

    #[test]
    fn test_schema_registry_strips_trailing_slash() {
        let client = SchemaRegistryClient::new("http://localhost:9094/");
        assert_eq!(client.base_url(), "http://localhost:9094");
    }
}
