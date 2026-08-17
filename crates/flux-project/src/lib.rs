//! Fold helpers and projection rebuilds.

use flux_core::{EventEnvelope, Result, StreamId, StreamPosition};
use flux_store::EventStore;
use serde::{de::DeserializeOwned, Serialize};

pub fn fold<S, F>(initial: S, events: &[EventEnvelope], mut apply: F) -> S
where F: FnMut(S, &EventEnvelope) -> S {
    events.iter().fold(initial, |state, ev| apply(state, ev))
}

pub async fn rebuild_stream<S, F, Store>(store: &Store, stream_id: &StreamId, initial: S, apply: F) -> Result<S>
where Store: EventStore, F: FnMut(S, &EventEnvelope) -> S {
    let events = store.read_stream(stream_id, StreamPosition::START).await?;
    Ok(fold(initial, &events, apply))
}

pub struct ProjectionCursor {
    pub name: String,
    pub last_global: u64,
}

impl ProjectionCursor {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), last_global: 0 }
    }
}

pub async fn catch_up<Store, F>(store: &Store, cursor: &mut ProjectionCursor, mut handler: F) -> Result<usize>
where Store: EventStore, F: FnMut(&EventEnvelope) -> Result<()> {
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
    Ok(serde_json::to_string_pretty(value).map_err(|e| flux_core::FluxError::Projection(e.to_string()))?)
}

pub fn decode_view<T: DeserializeOwned>(raw: &str) -> Result<T> {
    Ok(serde_json::from_str(raw).map_err(|e| flux_core::FluxError::Projection(e.to_string()))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flux_core::Event;
    use flux_store::MemoryStore;
    use serde_json::json;
    use std::sync::Arc;
    #[tokio::test]
    async fn rebuild_balance() {
        let store = Arc::new(MemoryStore::new());
        let sid = StreamId::new("acct");
        store.append(&sid, Some(0), vec![
            Event::new("Opened", json!({"balance": 0})),
            Event::new("Deposited", json!({"amount": 50})),
            Event::new("Withdrawn", json!({"amount": 20})),
        ]).await.unwrap();
        let balance = rebuild_stream(store.as_ref(), &sid, 0i64, |bal, ev| match ev.type_name.as_str() {
            "Opened" => 0,
            "Deposited" => bal + ev.data["amount"].as_i64().unwrap_or(0),
            "Withdrawn" => bal - ev.data["amount"].as_i64().unwrap_or(0),
            _ => bal,
        }).await.unwrap();
        assert_eq!(balance, 30);
    }
}
