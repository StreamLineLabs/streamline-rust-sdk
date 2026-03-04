//! StreamQL query client for executing SQL queries.

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
    base_url: String,
}

impl QueryClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Get the query URL.
    pub fn query_url(&self) -> String {
        format!("{}/api/v1/query", self.base_url)
    }

    /// Get the explain URL.
    pub fn explain_url(&self) -> String {
        format!("{}/api/v1/query/explain", self.base_url)
    }
}

