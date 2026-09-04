//! Example: Agent memory with remember/recall via the Moonshot memory API.
//!
//! Demonstrates the M1 memory HTTP client for building agents with
//! persistent, semantically searchable memory. Shows a single agent storing
//! facts and recalling them, plus a second agent recalling its own memories.
//!
//! Requires the `moonshot` feature and a Streamline server with moonshot
//! feature flags enabled (HTTP API on port 9094).
//!
//! Run with:
//!   cargo run --example agent_memory --features moonshot

use streamline_client::moonshot::{
    MemoryClient, MemoryKind, MoonshotOptions, RecallParams, RememberParams,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let http_endpoint =
        std::env::var("STREAMLINE_HTTP").unwrap_or_else(|_| "http://localhost:9094".into());

    let memory = MemoryClient::new(MoonshotOptions::new(&http_endpoint))?;

    single_agent_memory(&memory).await?;
    second_agent_memory(&memory).await?;

    println!("\nDone!");
    Ok(())
}

async fn single_agent_memory(memory: &MemoryClient) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Single Agent Memory ===");

    // Store architectural decisions
    memory
        .remember(RememberParams {
            agent_id: "demo-agent".into(),
            kind: MemoryKind::Fact,
            content: "We chose PostgreSQL for its JSONB support and mature ecosystem".into(),
            salience: Some(0.8),
            skill: None,
            metadata: None,
        })
        .await?;

    memory
        .remember(RememberParams {
            agent_id: "demo-agent".into(),
            kind: MemoryKind::Fact,
            content: "Redis is used as a caching layer with a 15-minute TTL".into(),
            salience: Some(0.7),
            skill: None,
            metadata: None,
        })
        .await?;

    memory
        .remember(RememberParams {
            agent_id: "demo-agent".into(),
            kind: MemoryKind::Observation,
            content: "User requested dark mode support in the dashboard".into(),
            salience: Some(0.6),
            skill: None,
            metadata: None,
        })
        .await?;

    println!("Stored 3 memories\n");

    // Recall by semantic similarity
    println!("--- Recall: 'why did we pick our database?' ---");
    let hits = memory
        .recall(RecallParams {
            agent_id: "demo-agent".into(),
            query: "why did we pick our database?".into(),
            k_episodic: 5,
            k_semantic: 5,
        })
        .await?;
    for hit in &hits {
        println!("  [{}] score={:.2}: {}", hit.tier, hit.score, hit.content);
    }

    println!("\n--- Recall: 'caching strategy' ---");
    let hits = memory
        .recall(RecallParams {
            agent_id: "demo-agent".into(),
            query: "caching strategy".into(),
            k_episodic: 5,
            k_semantic: 5,
        })
        .await?;
    for hit in &hits {
        println!("  [{}] score={:.2}: {}", hit.tier, hit.score, hit.content);
    }

    Ok(())
}

async fn second_agent_memory(memory: &MemoryClient) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Second Agent Memory ===");

    // A different agent stores its own decision under its own agent_id.
    memory
        .remember(RememberParams {
            agent_id: "agent-b".into(),
            kind: MemoryKind::Fact,
            content: "Deploy target is Kubernetes on AWS EKS".into(),
            salience: Some(0.9),
            skill: None,
            metadata: None,
        })
        .await?;
    println!("Agent B stored deployment decision");

    println!("\n--- Agent B recalls 'deployment infrastructure' ---");
    let hits = memory
        .recall(RecallParams {
            agent_id: "agent-b".into(),
            query: "deployment infrastructure".into(),
            k_episodic: 5,
            k_semantic: 5,
        })
        .await?;
    for hit in &hits {
        println!("  [{}] score={:.2}: {}", hit.tier, hit.score, hit.content);
    }

    Ok(())
}
