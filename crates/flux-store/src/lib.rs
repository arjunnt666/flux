//! Event store traits and in-memory backend.

use async_trait::async_trait;
use chrono::Utc;
use flux_core::{Event, EventEnvelope, EventId, FluxError, Result, StreamId, StreamPosition};
use parking_lot::Mutex;
use std::collections::HashMap;

#[async_trait]
pub trait EventStore: Send + Sync {
    async fn append(&self, stream_id: &StreamId, expected_version: Option<u64>, events: Vec<Event>) -> Result<Vec<EventEnvelope>>;
    async fn read_stream(&self, stream_id: &StreamId, from: StreamPosition) -> Result<Vec<EventEnvelope>>;
    async fn read_all_from(&self, global_from: u64) -> Result<Vec<EventEnvelope>>;
    async fn stream_version(&self, stream_id: &StreamId) -> Result<Option<u64>>;
}

pub struct MemoryStore {
    streams: Mutex<HashMap<String, Vec<EventEnvelope>>>,
    global: Mutex<Vec<EventEnvelope>>,
    next_global: Mutex<u64>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self { streams: Mutex::new(HashMap::new()), global: Mutex::new(Vec::new()), next_global: Mutex::new(0) }
    }
}
impl Default for MemoryStore { fn default() -> Self { Self::new() } }

#[async_trait]
impl EventStore for MemoryStore {
    async fn append(&self, stream_id: &StreamId, expected_version: Option<u64>, events: Vec<Event>) -> Result<Vec<EventEnvelope>> {
        if events.is_empty() { return Ok(Vec::new()); }
        let mut streams = self.streams.lock();
        let entry = streams.entry(stream_id.0.clone()).or_default();
        let current_len = entry.len() as u64;
        if let Some(expected) = expected_version {
            if expected != current_len {
                return Err(FluxError::VersionConflict { expected, actual: current_len });
            }
        }
        let mut written = Vec::with_capacity(events.len());
        let mut global = self.global.lock();
        let mut next_g = self.next_global.lock();
        for (i, ev) in events.into_iter().enumerate() {
            let position = StreamPosition(current_len + i as u64);
            let gpos = *next_g;
            *next_g += 1;
            let env = EventEnvelope {
                id: EventId::new(), stream_id: stream_id.clone(), position, global_position: gpos,
                type_name: ev.type_name, data: ev.data, metadata: ev.metadata, recorded_at: Utc::now(),
            };
            entry.push(env.clone());
            global.push(env.clone());
            written.push(env);
        }
        Ok(written)
    }

    async fn read_stream(&self, stream_id: &StreamId, from: StreamPosition) -> Result<Vec<EventEnvelope>> {
        let streams = self.streams.lock();
        Ok(streams.get(&stream_id.0).map(|v| v.iter().filter(|e| e.position.0 >= from.0).cloned().collect()).unwrap_or_default())
    }

    async fn read_all_from(&self, global_from: u64) -> Result<Vec<EventEnvelope>> {
        let global = self.global.lock();
        Ok(global.iter().filter(|e| e.global_position >= global_from).cloned().collect())
    }

    async fn stream_version(&self, stream_id: &StreamId) -> Result<Option<u64>> {
        Ok(self.streams.lock().get(&stream_id.0).map(|v| v.len() as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[tokio::test]
    async fn append_and_read() {
        let store = MemoryStore::new();
        let sid = StreamId::new("account-1");
        let written = store.append(&sid, Some(0), vec![Event::new("Opened", json!({"balance": 0}))]).await.unwrap();
        assert_eq!(written.len(), 1);
        assert_eq!(written[0].position.0, 0);
        let hist = store.read_stream(&sid, StreamPosition::START).await.unwrap();
        assert_eq!(hist.len(), 1);
    }
    #[tokio::test]
    async fn optimistic_concurrency() {
        let store = MemoryStore::new();
        let sid = StreamId::new("a");
        store.append(&sid, Some(0), vec![Event::new("A", json!({}))]).await.unwrap();
        let err = store.append(&sid, Some(0), vec![Event::new("B", json!({}))]).await.unwrap_err();
        match err {
            FluxError::VersionConflict { expected, actual } => { assert_eq!(expected, 0); assert_eq!(actual, 1); }
            _ => panic!("expected conflict"),
        }
    }
}
