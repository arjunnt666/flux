use clap::{Parser, Subcommand};
use flux_core::{Event, EventEnvelope, StreamId, StreamPosition};
use flux_project::{rebuild_stream, rebuild_stream_from_snapshot};
use flux_snapshot::{snapshot_from, MemorySnapshotStore, SnapshotStore};
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
enum Commands {
    Demo,
    Version,
}

fn apply_bal(bal: i64, ev: &EventEnvelope) -> i64 {
    match ev.type_name.as_str() {
        "Opened" => 0,
        "Deposited" => bal + ev.data["amount"].as_i64().unwrap_or(0),
        "Withdrawn" => bal - ev.data["amount"].as_i64().unwrap_or(0),
        _ => bal,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Commands::Demo => {
            let store = Arc::new(MemoryStore::new());
            let snaps = MemorySnapshotStore::new();
            let sid = StreamId::new("account-demo");
            store
                .append(
                    &sid,
                    Some(0),
                    vec![
                        Event::new("Opened", json!({"owner": "arjun"})),
                        Event::new("Deposited", json!({"amount": 100})),
                    ],
                )
                .await?;
            let mid = rebuild_stream(store.as_ref(), &sid, 0i64, apply_bal).await?;
            snaps.put(snapshot_from(sid.clone(), StreamPosition(2), &mid)?)?;
            store
                .append(
                    &sid,
                    Some(2),
                    vec![
                        Event::new("Deposited", json!({"amount": 50})),
                        Event::new("Withdrawn", json!({"amount": 35})),
                    ],
                )
                .await?;
            let full = rebuild_stream(store.as_ref(), &sid, 0i64, apply_bal).await?;
            let (from_snap, stats) =
                rebuild_stream_from_snapshot(store.as_ref(), &snaps, &sid, 0i64, apply_bal).await?;
            anyhow::ensure!(full == from_snap, "snapshot rebuild diverged");
            anyhow::ensure!(stats.used_snapshot, "expected to use the snapshot");
            anyhow::ensure!(
                stats.events_applied == 2,
                "snapshot should skip the prefix, applied {}",
                stats.events_applied
            );
            let mut sub = Subscription::new("demo-projection");
            let mut count = 0usize;
            sub.poll(store.as_ref(), |_| {
                count += 1;
                Ok(())
            })
            .await?;
            let ver = store.stream_version(&sid).await?.unwrap_or(0);
            println!(
                "stream={} version={} balance={} snapshot_applied={} projected_events={}",
                sid, ver, from_snap, stats.events_applied, count
            );
            for e in store.read_stream(&sid, StreamPosition::START).await? {
                println!("  #{} {} {}", e.position, e.type_name, e.data);
            }
        }
        Commands::Version => println!("flux 0.1.0"),
    }
    Ok(())
}
