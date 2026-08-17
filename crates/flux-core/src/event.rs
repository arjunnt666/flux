use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;
use crate::stream::{StreamId, StreamPosition};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(pub Uuid);

impl EventId {
    pub fn new() -> Self { Self(Uuid::new_v4()) }
}
impl Default for EventId {
    fn default() -> Self { Self::new() }
}
impl std::fmt::Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub extra: HashMap<String, String>,
}

impl Default for EventMetadata {
    fn default() -> Self {
        Self { correlation_id: None, causation_id: None, extra: HashMap::new() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub type_name: String,
    pub data: Value,
    pub metadata: EventMetadata,
}

impl Event {
    pub fn new(type_name: impl Into<String>, data: Value) -> Self {
        Self { type_name: type_name.into(), data, metadata: EventMetadata::default() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub id: EventId,
    pub stream_id: StreamId,
    pub position: StreamPosition,
    pub global_position: u64,
    pub type_name: String,
    pub data: Value,
    pub metadata: EventMetadata,
    pub recorded_at: DateTime<Utc>,
}
