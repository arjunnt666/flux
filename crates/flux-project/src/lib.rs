//! Fold helpers and projection rebuilds.

use flux_core::{EventEnvelope, Result, StreamId, StreamPosition};
use flux_snapshot::{state_from, SnapshotStore};
use flux_store::EventStore;
use serde::{de::DeserializeOwned, Serialize};

pub fn fold<S, F>(initial: S, events: &[EventEnvelope], mut apply: F) -> S
where
    F: FnMut(S, &EventEnvelope) -> S,
{
    events.iter().fold(initial, |state, ev| apply(state, ev))
}

pub async fn rebuild_stream<S, F, Store>(
    store: &Store,
    stream_id: &StreamId,
    initial: S,
    apply: F,
) -> Result<S>
where
    Store: EventStore,
    F: FnMut(S, &EventEnvelope) -> S,
{
    let events = store.read_stream(stream_id, StreamPosition::START).await?;
    Ok(fold(initial, &events, apply))
}

/// How a snapshot-accelerated rebuild ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebuildStats {
    pub events_applied: usize,
    pub used_snapshot: bool,
    pub from_position: u64,
}

/// Rebuild a stream. If a snapshot exists, fold only events at or after
/// `snapshot.version` (version is the exclusive next position, i.e. event count).
pub async fn rebuild_stream_from_snapshot<S, F, Store, Snaps>(
    store: &Store,
    snaps: &Snaps,
    stream_id: &StreamId,
    initial: S,
    apply: F,
) -> Result<(S, RebuildStats)>
where
    Store: EventStore,
    Snaps: SnapshotStore,
    S: DeserializeOwned,
    F: FnMut(S, &EventEnvelope) -> S,
{
    let (seed, from, used) = match snaps.get(stream_id)? {
        Some(snap) => {
            let state: S = state_from(&snap)?;
            (state, StreamPosition(snap.version.0), true)
        }
        None => (initial, StreamPosition::START, false),
    };
    let events = store.read_stream(stream_id, from).await?;
    let n = events.len();
    Ok((
        fold(seed, &events, apply),
        RebuildStats {
            events_applied: n,
            used_snapshot: used,
            from_position: from.0,
        },
    ))
}

pub struct ProjectionCursor {
    pub name: String,
    pub last_global: u64,
}

impl ProjectionCursor {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            last_global: 0,
        }
    }
}

pub async fn catch_up<Store, F>(
    store: &Store,
    cursor: &mut ProjectionCursor,
    mut handler: F,
) -> Result<usize>
where
    Store: EventStore,
    F: FnMut(&EventEnvelope) -> Result<()>,
{
    let events = store.read_all_from(cursor.last_global).await?;
    let mut n = 0usize;
    for ev in &events {
        handler(ev)?;
        cursor.last_global = ev.global_position + 1;
        n += 1;
    }
    Ok(n)
}

pub fn encode_view<T: Serialize>(value: &T) -> Result<String> {
    Ok(serde_json::to_string_pretty(value)
        .map_err(|e| flux_core::FluxError::Projection(e.to_string()))?)
}

pub fn decode_view<T: DeserializeOwned>(raw: &str) -> Result<T> {
    Ok(serde_json::from_str(raw).map_err(|e| flux_core::FluxError::Projection(e.to_string()))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flux_core::Event;
    use flux_snapshot::{snapshot_from, MemorySnapshotStore, SnapshotStore};
    use flux_store::MemoryStore;
    use serde_json::json;
    use std::sync::Arc;

    fn apply_bal(bal: i64, ev: &EventEnvelope) -> i64 {
        match ev.type_name.as_str() {
            "Opened" => 0,
            "Deposited" => bal + ev.data["amount"].as_i64().unwrap_or(0),
            "Withdrawn" => bal - ev.data["amount"].as_i64().unwrap_or(0),
            _ => bal,
        }
    }

    #[tokio::test]
    async fn rebuild_balance() {
        let store = Arc::new(MemoryStore::new());
        let sid = StreamId::new("acct");
        store
            .append(
                &sid,
                Some(0),
                vec![
                    Event::new("Opened", json!({"balance": 0})),
                    Event::new("Deposited", json!({"amount": 50})),
                    Event::new("Withdrawn", json!({"amount": 20})),
                ],
            )
            .await
            .unwrap();
        let balance = rebuild_stream(store.as_ref(), &sid, 0i64, apply_bal)
            .await
            .unwrap();
        assert_eq!(balance, 30);
    }

    #[tokio::test]
    async fn snapshot_skips_prefix() {
        let store = MemoryStore::new();
        let snaps = MemorySnapshotStore::new();
        let sid = StreamId::new("acct");
        store
            .append(
                &sid,
                Some(0),
                vec![
                    Event::new("Opened", json!({})),
                    Event::new("Deposited", json!({"amount": 100})),
                    Event::new("Deposited", json!({"amount": 40})),
                ],
            )
            .await
            .unwrap();
        let mid = rebuild_stream(&store, &sid, 0i64, apply_bal).await.unwrap();
        assert_eq!(mid, 140);
        snaps
            .put(snapshot_from(sid.clone(), StreamPosition(3), &mid).unwrap())
            .unwrap();
        store
            .append(
                &sid,
                Some(3),
                vec![
                    Event::new("Deposited", json!({"amount": 10})),
                    Event::new("Withdrawn", json!({"amount": 25})),
                ],
            )
            .await
            .unwrap();
        let full = rebuild_stream(&store, &sid, 0i64, apply_bal).await.unwrap();
        let (from_snap, stats) =
            rebuild_stream_from_snapshot(&store, &snaps, &sid, 0i64, apply_bal)
                .await
                .unwrap();
        assert_eq!(full, 125);
        assert_eq!(from_snap, full);
        assert!(stats.used_snapshot);
        assert_eq!(stats.events_applied, 2);
        assert_eq!(stats.from_position, 3);
    }
}
