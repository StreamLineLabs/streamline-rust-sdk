//! Schema Registry client for Streamline.

use serde::{Deserialize, Serialize};

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
    pub schema_type: SchemaType,
    /// Schema definition string
    pub schema: String,
}

/// Request to register a new schema.
#[derive(Debug, Clone, Serialize)]
pub struct RegisterSchemaRequest {
    /// Schema definition string
    pub schema: String,
    /// Schema type
    pub schema_type: SchemaType,
}

/// Response from registering a schema.
#[derive(Debug, Clone, Deserialize)]
pub struct RegisterSchemaResponse {
    /// Assigned schema ID
    pub id: i32,
}

/// Result of a compatibility check.
#[derive(Debug, Clone, Deserialize)]
pub struct CompatibilityResult {
    /// Whether the schema is compatible
    pub is_compatible: bool,
}

/// Schema Registry client that communicates with the Streamline HTTP API.
/// Uses the server's /api/schemas endpoint.
pub struct SchemaRegistryClient {
    base_url: String,
}

impl SchemaRegistryClient {
    /// Create a new Schema Registry client.
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Build the full URL for a schema registry endpoint.
    pub fn url(&self, path: &str) -> String {
        format!("{}/api/schemas{}", self.base_url, path)
    }

    /// Get the base URL for this client.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_registry_url() {
        let client = SchemaRegistryClient::new("http://localhost:9094");
        assert_eq!(client.url("/subjects"), "http://localhost:9094/api/schemas/subjects");
        assert_eq!(client.base_url(), "http://localhost:9094");
    }

    #[test]
    fn test_schema_registry_strips_trailing_slash() {
        let client = SchemaRegistryClient::new("http://localhost:9094/");
        assert_eq!(client.base_url(), "http://localhost:9094");
    }
}
