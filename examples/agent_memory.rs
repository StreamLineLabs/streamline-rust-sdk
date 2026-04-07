//! Example: Agent memory with remember/recall and multi-agent shared memory.
//!
//! Demonstrates the memory MCP tools for building agents with persistent,
//! semantically searchable memory. Shows single-agent memory and multi-agent
//! shared memory via namespaces.
//!
//! Run with:
//!   cargo run --example agent_memory

use streamline_client::{MemoryEntry, MemoryQuery, Streamline};

#[tokio::main]
async fn main() -> Result<(), streamline_client::Error> {
    let bootstrap = std::env::var("STREAMLINE_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "localhost:9092".into());
    let http_endpoint =
        std::env::var("STREAMLINE_HTTP").unwrap_or_else(|_| "http://localhost:9094".into());

    let client = Streamline::builder()
        .bootstrap_servers(&bootstrap)
        .http_endpoint(&http_endpoint)
        .build()
        .await?;

    single_agent_memory(&client).await?;
    multi_agent_shared_memory(&client).await?;

    println!("\nDone!");
    Ok(())
}

async fn single_agent_memory(client: &Streamline) -> Result<(), streamline_client::Error> {
    println!("=== Single Agent Memory ===");

    // Store architectural decisions
    client
        .memory_remember(
            MemoryEntry::builder()
                .agent_id("demo-agent")
                .content("We chose PostgreSQL for its JSONB support and mature ecosystem")
                .kind("fact")
                .importance(0.8)
                .tags(&["architecture", "database"])
                .build(),
        )
        .await?;

    client
        .memory_remember(
            MemoryEntry::builder()
                .agent_id("demo-agent")
                .content("Redis is used as a caching layer with a 15-minute TTL")
                .kind("fact")
                .importance(0.7)
                .tags(&["architecture", "caching"])
                .build(),
        )
        .await?;

    client
        .memory_remember(
            MemoryEntry::builder()
                .agent_id("demo-agent")
                .content("User requested dark mode support in the dashboard")
                .kind("preference")
                .importance(0.6)
                .tags(&["ui", "user-request"])
                .build(),
        )
        .await?;

    println!("Stored 3 memories\n");

    // Recall by semantic similarity
    println!("--- Recall: 'why did we pick our database?' ---");
    let results = client
        .memory_recall(
            MemoryQuery::new("demo-agent", "why did we pick our database?").k(5),
        )
        .await?;
    for hit in &results {
        println!("  [{}] score={:.2}: {}", hit.tier, hit.score, hit.content);
    }

    println!("\n--- Recall: 'caching strategy' ---");
    let results = client
        .memory_recall(MemoryQuery::new("demo-agent", "caching strategy").k(5))
        .await?;
    for hit in &results {
        println!("  [{}] score={:.2}: {}", hit.tier, hit.score, hit.content);
    }

    Ok(())
}

async fn multi_agent_shared_memory(client: &Streamline) -> Result<(), streamline_client::Error> {
    println!("\n=== Multi-Agent Shared Memory ===");

    // Agent A stores a decision in the shared namespace
    client
        .memory_remember(
            MemoryEntry::builder()
                .agent_id("agent-a")
                .namespace("team-shared")
                .content("Deploy target is Kubernetes on AWS EKS")
                .kind("fact")
                .importance(0.9)
                .tags(&["infra", "deployment"])
                .build(),
        )
        .await?;
    println!("Agent A stored deployment decision");

    // Agent B stores related context in the same namespace
    client
        .memory_remember(
            MemoryEntry::builder()
                .agent_id("agent-b")
                .namespace("team-shared")
                .content("CI/CD pipeline uses GitHub Actions with OIDC auth to AWS")
                .kind("fact")
                .importance(0.8)
                .tags(&["infra", "ci-cd"])
                .build(),
        )
        .await?;
    println!("Agent B stored CI/CD context");

    // Agent C recalls shared memories from the team namespace
    println!("\n--- Agent C recalls 'deployment infrastructure' from shared namespace ---");
    let results = client
        .memory_recall(
            MemoryQuery::new("agent-c", "deployment infrastructure")
                .namespace("team-shared")
                .k(5),
        )
        .await?;
    for hit in &results {
        println!("  [{}] score={:.2}: {}", hit.tier, hit.score, hit.content);
    }

    Ok(())
}
