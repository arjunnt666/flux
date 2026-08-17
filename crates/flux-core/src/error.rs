use thiserror::Error;

pub type Result<T> = std::result::Result<T, FluxError>;

#[derive(Debug, Error)]
pub enum FluxError {
    #[error("not found: {0}")] NotFound(String),
    #[error("conflict: expected version {expected}, got {actual}")]
    VersionConflict { expected: u64, actual: u64 },
    #[error("store: {0}")] Store(String),
    #[error("invalid: {0}")] Invalid(String),
    #[error("projection: {0}")] Projection(String),
    #[error("internal: {0}")] Internal(String),
}
