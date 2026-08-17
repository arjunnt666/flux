//! Optional aggregate snapshots to skip full replay.

use flux_core::{Result, StreamId, StreamPosition};
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Snapshot {
    pub stream_id: StreamId,
    pub version: StreamPosition,
    pub state: Value,
}

pub trait SnapshotStore: Send + Sync {
    fn put(&self, snap: Snapshot) -> Result<()>;
    fn get(&self, stream_id: &StreamId) -> Result<Option<Snapshot>>;
}

pub struct MemorySnapshotStore {
    inner: Mutex<HashMap<String, Snapshot>>,
}

impl MemorySnapshotStore {
    pub fn new() -> Self { Self { inner: Mutex::new(HashMap::new()) } }
}
impl Default for MemorySnapshotStore { fn default() -> Self { Self::new() } }

impl SnapshotStore for MemorySnapshotStore {
    fn put(&self, snap: Snapshot) -> Result<()> {
        self.inner.lock().insert(snap.stream_id.0.clone(), snap); Ok(())
    }
    fn get(&self, stream_id: &StreamId) -> Result<Option<Snapshot>> {
        Ok(self.inner.lock().get(&stream_id.0).cloned())
    }
}

pub fn snapshot_from<T: Serialize>(stream_id: StreamId, version: StreamPosition, state: &T) -> Result<Snapshot> {
    Ok(Snapshot {
        stream_id, version,
        state: serde_json::to_value(state).map_err(|e| flux_core::FluxError::Internal(e.to_string()))?,
    })
}

pub fn state_from<T: DeserializeOwned>(snap: &Snapshot) -> Result<T> {
    Ok(serde_json::from_value(snap.state.clone()).map_err(|e| flux_core::FluxError::Internal(e.to_string()))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip() {
        let store = MemorySnapshotStore::new();
        let sid = StreamId::new("a");
        let snap = snapshot_from(sid.clone(), StreamPosition(3), &42i64).unwrap();
        store.put(snap).unwrap();
        let got = store.get(&sid).unwrap().unwrap();
        assert_eq!(state_from::<i64>(&got).unwrap(), 42);
        assert_eq!(got.version.0, 3);
    }
}
