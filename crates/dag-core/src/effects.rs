use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Effect lattice describing resource access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum Effects {
    /// No observable side-effects.
    Pure,
    /// Read-only side-effects (e.g. cache lookups).
    ReadOnly,
    /// External side-effects (e.g. network, writes).
    #[default]
    Effectful,
}

impl Effects {
    /// Convert effects level into a comparable rank (higher is more permissive).
    pub const fn rank(self) -> u8 {
        match self {
            Effects::Pure => 0,
            Effects::ReadOnly => 1,
            Effects::Effectful => 2,
        }
    }

    /// Returns true if `self` is at least as permissive as `minimum`.
    pub const fn is_at_least(self, minimum: Effects) -> bool {
        self.rank() >= minimum.rank()
    }

    /// Human-readable representation of the effect level.
    pub const fn as_str(self) -> &'static str {
        match self {
            Effects::Pure => "Pure",
            Effects::ReadOnly => "ReadOnly",
            Effects::Effectful => "Effectful",
        }
    }
}

/// Determinism lattice describing replay guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum Determinism {
    /// Fully deterministic, no time or randomness.
    Strict,
    /// Stable under pinned resources.
    Stable,
    /// Best-effort determinism, may vary on retries.
    #[default]
    BestEffort,
    /// Explicitly non-deterministic.
    Nondeterministic,
}

impl Determinism {
    /// Convert determinism level into a comparable rank (higher is less strict).
    pub const fn rank(self) -> u8 {
        match self {
            Determinism::Strict => 0,
            Determinism::Stable => 1,
            Determinism::BestEffort => 2,
            Determinism::Nondeterministic => 3,
        }
    }

    /// Returns true if `self` is at least as permissive as `minimum`.
    pub const fn is_at_least(self, minimum: Determinism) -> bool {
        self.rank() >= minimum.rank()
    }

    /// Human-readable representation used in diagnostics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Determinism::Strict => "Strict",
            Determinism::Stable => "Stable",
            Determinism::BestEffort => "BestEffort",
            Determinism::Nondeterministic => "Nondeterministic",
        }
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
