use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Effect lattice describing resource access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Effects {
    /// No observable side-effects.
    Pure,
    /// Read-only side-effects (e.g. cache lookups).
    ReadOnly,
    /// External side-effects (e.g. network, writes).
    Effectful,
}

impl Default for Effects {
    fn default() -> Self {
        Effects::Effectful
    }
}

/// Determinism lattice describing replay guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Determinism {
    /// Fully deterministic, no time or randomness.
    Strict,
    /// Stable under pinned resources.
    Stable,
    /// Best-effort determinism, may vary on retries.
    BestEffort,
    /// Explicitly non-deterministic.
    Nondeterministic,
}

impl Default for Determinism {
    fn default() -> Self {
        Determinism::BestEffort
    }
}

/// Canonical node error type surfaced during execution.
#[derive(Debug, Error)]
pub enum NodeError {
    /// Generic error message.
    #[error("{0}")]
    Message(String),
}

impl NodeError {
    /// Construct a node error from displayable content.
    pub fn new(message: impl Into<String>) -> Self {
        NodeError::Message(message.into())
    }
}

impl From<anyhow::Error> for NodeError {
    fn from(err: anyhow::Error) -> Self {
        NodeError::Message(err.to_string())
    }
}

/// Convenient result alias for node execution.
pub type NodeResult<T> = Result<T, NodeError>;
