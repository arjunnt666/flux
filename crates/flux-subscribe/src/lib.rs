//! Catch-up subscriptions from a global position.

use flux_core::{EventEnvelope, Result};
use flux_project::{catch_up, ProjectionCursor};
use flux_store::EventStore;
use tracing::info;

pub struct Subscription {
    pub cursor: ProjectionCursor,
}

impl Subscription {
    pub fn new(name: impl Into<String>) -> Self {
        Self { cursor: ProjectionCursor::new(name) }
    }

    pub async fn poll<Store, F>(&mut self, store: &Store, handler: F) -> Result<usize>
    where Store: EventStore, F: FnMut(&EventEnvelope) -> Result<()> {
        let n = catch_up(store, &mut self.cursor, handler).await?;
        if n > 0 {
            info!(name = %self.cursor.name, processed = n, last = self.cursor.last_global, "subscription caught up");
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flux_core::{Event, StreamId};
    use flux_store::MemoryStore;
    use serde_json::json;
    use std::sync::Arc;
    #[tokio::test]
    async fn catches_up() {
        let store = Arc::new(MemoryStore::new());
        store.append(&StreamId::new("s"), Some(0), vec![Event::new("X", json!({}))]).await.unwrap();
        let mut sub = Subscription::new("test");
        let mut seen = 0usize;
        let n = sub.poll(store.as_ref(), |_| { seen += 1; Ok(()) }).await.unwrap();
        assert_eq!(n, 1);
        assert_eq!(seen, 1);
        assert_eq!(sub.poll(store.as_ref(), |_| Ok(())).await.unwrap(), 0);
    }
}
