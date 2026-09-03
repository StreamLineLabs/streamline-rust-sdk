//! StreamQL query client for executing SQL queries.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};

/// Query result from the Streamline API.
#[derive(Debug, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub metadata: QueryMetadata,
}

#[derive(Debug, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
}

#[derive(Debug, Deserialize)]
pub struct QueryMetadata {
    pub execution_time_ms: u64,
    pub rows_scanned: u64,
    pub rows_returned: usize,
    pub truncated: bool,
}

/// Query request to the Streamline API.
#[derive(Debug, Serialize)]
pub struct QueryRequest {
    pub sql: String,
    pub timeout_ms: u64,
    pub max_rows: usize,
    pub format: String,
}

impl QueryRequest {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            timeout_ms: 30000,
            max_rows: 10000,
            format: "json".to_string(),
        }
    }

    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn with_max_rows(mut self, max_rows: usize) -> Self {
        self.max_rows = max_rows;
        self
    }
}

/// Query client that communicates with the Streamline HTTP API.
pub struct QueryClient {
    // Only read when an HTTP feature is enabled; without one the SDK has no
    // HTTP stack and URL construction reports `Unsupported`.
    #[cfg_attr(
        not(any(
            feature = "http-admin",
            feature = "schema-registry",
            feature = "moonshot"
        )),
        allow(dead_code)
    )]
    base_url: String,
}

impl QueryClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Get the query URL.
    ///
    /// # Errors
    /// Returns [`crate::ErrorKind::InvalidConfiguration`] when the configured
    /// base URL is not an absolute `http`/`https` URL. Requires an HTTP
    /// feature (`http-admin`, `schema-registry`, or `moonshot`); without one
    /// the SDK has no HTTP stack and the call reports
    /// [`crate::ErrorKind::Unsupported`].
    pub fn query_url(&self) -> Result<String> {
        self.url(&["api", "v1", "query"])
    }

    /// Get the explain URL.
    ///
    /// # Errors
    /// Same as [`QueryClient::query_url`].
    pub fn explain_url(&self) -> Result<String> {
        self.url(&["api", "v1", "query", "explain"])
    }

    #[cfg(any(
        feature = "http-admin",
        feature = "schema-registry",
        feature = "moonshot"
    ))]
    fn url(&self, segments: &[&str]) -> Result<String> {
        Ok(crate::http_url::build_url(&self.base_url, segments, &[])?.to_string())
    }

    #[cfg(not(any(
        feature = "http-admin",
        feature = "schema-registry",
        feature = "moonshot"
    )))]
    fn url(&self, _segments: &[&str]) -> Result<String> {
        Err(
            Error::unsupported("StreamQL query URL construction").with_hint(
                "Enable the http-admin, schema-registry, or moonshot feature for HTTP support",
            ),
        )
    }

    /// Returns an explicit unsupported error.
    ///
    /// Version 0.4.0 exposes query request and response types for API
    /// compatibility, but does not execute SQL requests. This method exists so
    /// callers cannot mistake URL construction for successful query support.
    pub async fn execute(&self, _request: &QueryRequest) -> Result<QueryResult> {
        Err(Error::unsupported("StreamQL query execution"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_request_new_defaults() {
        let req = QueryRequest::new("SELECT * FROM events");
        assert_eq!(req.sql, "SELECT * FROM events");
        assert_eq!(req.timeout_ms, 30000);
        assert_eq!(req.max_rows, 10000);
        assert_eq!(req.format, "json");
    }

    #[test]
    fn test_query_request_new_from_string() {
        let sql = String::from("SELECT count(*) FROM orders");
        let req = QueryRequest::new(sql);
        assert_eq!(req.sql, "SELECT count(*) FROM orders");
    }

    #[test]
    fn test_with_timeout() {
        let req = QueryRequest::new("SELECT 1").with_timeout(5000);
        assert_eq!(req.timeout_ms, 5000);
        assert_eq!(req.max_rows, 10000); // unchanged
    }

    #[test]
    fn test_with_max_rows() {
        let req = QueryRequest::new("SELECT 1").with_max_rows(500);
        assert_eq!(req.max_rows, 500);
        assert_eq!(req.timeout_ms, 30000); // unchanged
    }

    #[test]
    fn test_builder_chaining() {
        let req = QueryRequest::new("SELECT 1")
            .with_timeout(1000)
            .with_max_rows(50);
        assert_eq!(req.timeout_ms, 1000);
        assert_eq!(req.max_rows, 50);
    }

    #[cfg(any(
        feature = "http-admin",
        feature = "schema-registry",
        feature = "moonshot"
    ))]
    mod urls {
        use super::*;

        #[test]
        fn test_query_client_new() {
            let client = QueryClient::new("http://localhost:9094");
            assert_eq!(
                client.query_url().unwrap(),
                "http://localhost:9094/api/v1/query"
            );
        }

        #[test]
        fn test_query_client_trims_trailing_slash() {
            let client = QueryClient::new("http://localhost:9094/");
            assert_eq!(
                client.query_url().unwrap(),
                "http://localhost:9094/api/v1/query"
            );
        }

        #[test]
        fn test_query_client_trims_multiple_trailing_slashes() {
            let client = QueryClient::new("http://localhost:9094///");
            // trim_end_matches strips all trailing slashes
            assert_eq!(
                client.query_url().unwrap(),
                "http://localhost:9094/api/v1/query"
            );
        }

        #[test]
        fn test_query_url() {
            let client = QueryClient::new("https://streamline.example.com");
            assert_eq!(
                client.query_url().unwrap(),
                "https://streamline.example.com/api/v1/query"
            );
        }

        #[test]
        fn test_explain_url() {
            let client = QueryClient::new("http://localhost:9094");
            assert_eq!(
                client.explain_url().unwrap(),
                "http://localhost:9094/api/v1/query/explain"
            );
        }

        #[test]
        fn test_query_url_rejects_invalid_base() {
            let client = QueryClient::new("not-a-url");
            assert_eq!(
                client.query_url().unwrap_err().kind,
                crate::ErrorKind::InvalidConfiguration
            );
        }
    }

    #[cfg(not(any(
        feature = "http-admin",
        feature = "schema-registry",
        feature = "moonshot"
    )))]
    #[test]
    fn test_query_url_requires_http_feature() {
        let client = QueryClient::new("http://localhost:9094");
        assert_eq!(
            client.query_url().unwrap_err().kind,
            crate::ErrorKind::Unsupported
        );
    }

    #[tokio::test]
    async fn test_query_execution_fails_closed() {
        let client = QueryClient::new("http://localhost:9094");
        let request = QueryRequest::new("SELECT 1");
        let error = client.execute(&request).await.unwrap_err();
        assert_eq!(error.kind, crate::ErrorKind::Unsupported);
    }

    #[test]
    fn test_query_request_serializes_to_json() {
        let req = QueryRequest::new("SELECT * FROM t")
            .with_timeout(5000)
            .with_max_rows(100);
        let json = serde_json::to_value(&req).expect("serialize");
        assert_eq!(json["sql"], "SELECT * FROM t");
        assert_eq!(json["timeout_ms"], 5000);
        assert_eq!(json["max_rows"], 100);
        assert_eq!(json["format"], "json");
    }

    #[test]
    fn test_query_result_deserializes_from_json() {
        let json = serde_json::json!({
            "columns": [
                {"name": "id", "type": "INT"},
                {"name": "name", "type": "STRING"}
            ],
            "rows": [[1, "alice"], [2, "bob"]],
            "metadata": {
                "execution_time_ms": 42,
                "rows_scanned": 1000,
                "rows_returned": 2,
                "truncated": false
            }
        });
        let result: QueryResult = serde_json::from_value(json).expect("deserialize");
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].col_type, "INT");
        assert_eq!(result.columns[1].name, "name");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.metadata.execution_time_ms, 42);
        assert_eq!(result.metadata.rows_scanned, 1000);
        assert_eq!(result.metadata.rows_returned, 2);
        assert!(!result.metadata.truncated);
    }

    #[test]
    fn test_query_result_deserializes_truncated() {
        let json = serde_json::json!({
            "columns": [],
            "rows": [],
            "metadata": {
                "execution_time_ms": 0,
                "rows_scanned": 0,
                "rows_returned": 0,
                "truncated": true
            }
        });
        let result: QueryResult = serde_json::from_value(json).expect("deserialize");
        assert!(result.metadata.truncated);
    }

    #[test]
    fn test_column_info_deserializes() {
        let json = serde_json::json!({"name": "price", "type": "DOUBLE"});
        let col: ColumnInfo = serde_json::from_value(json).expect("deserialize");
        assert_eq!(col.name, "price");
        assert_eq!(col.col_type, "DOUBLE");
    }
}
