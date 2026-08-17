//! Core types for the flux event sourcing skeleton.

pub mod error;
pub mod event;
pub mod stream;

pub use error::{FluxError, Result};
pub use event::{Event, EventEnvelope, EventId, EventMetadata};
pub use stream::{StreamId, StreamPosition};
