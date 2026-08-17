use clap::{Parser, Subcommand};
use flux_core::{Event, StreamId, StreamPosition};
use flux_project::rebuild_stream;
use flux_store::{EventStore, MemoryStore};
use flux_subscribe::Subscription;
use serde_json::json;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "flux", about = "event sourcing core tooling")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands { Demo, Version }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Commands::Demo => {
            let store = Arc::new(MemoryStore::new());
            let sid = StreamId::new("account-demo");
            store.append(&sid, Some(0), vec![
                Event::new("Opened", json!({"owner": "arjun"})),
                Event::new("Deposited", json!({"amount": 100})),
                Event::new("Withdrawn", json!({"amount": 35})),
            ]).await?;
            let balance = rebuild_stream(store.as_ref(), &sid, 0i64, |bal, ev| match ev.type_name.as_str() {
                "Opened" => 0,
                "Deposited" => bal + ev.data["amount"].as_i64().unwrap_or(0),
                "Withdrawn" => bal - ev.data["amount"].as_i64().unwrap_or(0),
                _ => bal,
            }).await?;
            let mut sub = Subscription::new("demo-projection");
            let mut count = 0usize;
            sub.poll(store.as_ref(), |_| { count += 1; Ok(()) }).await?;
            let ver = store.stream_version(&sid).await?.unwrap_or(0);
            println!("stream={} version={} balance={} projected_events={}", sid, ver, balance, count);
            for e in store.read_stream(&sid, StreamPosition::START).await? {
                println!("  #{} {} {}", e.position, e.type_name, e.data);
            }
        }
        Commands::Version => println!("flux 0.1.0"),
    }
    Ok(())
}
