use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformBeginError {
    InvalidTransform,
    Busy,
    RuntimeUnavailable,
}

impl TransformBeginError {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidTransform => "invalid_transform",
            Self::Busy => "busy",
            Self::RuntimeUnavailable => "runtime_unavailable",
        }
    }
}

impl fmt::Display for TransformBeginError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::error::Error for TransformBeginError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformErrorClass {
    InvalidModule,
    InvalidAbi,
    InputTooLarge,
    Busy,
    RuntimeUnavailable,
    FuelExhausted,
    MemoryExhausted,
    WallTimeExceeded,
    Cancelled,
    OutputTooLarge,
    GuestFailed,
    InvalidOutput,
    UnsupportedDocument,
}

impl TransformErrorClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidModule => "invalid_module",
            Self::InvalidAbi => "invalid_abi",
            Self::InputTooLarge => "input_too_large",
            Self::Busy => "busy",
            Self::RuntimeUnavailable => "runtime_unavailable",
            Self::FuelExhausted => "fuel_exhausted",
            Self::MemoryExhausted => "memory_exhausted",
            Self::WallTimeExceeded => "wall_time_exceeded",
            Self::Cancelled => "cancelled",
            Self::OutputTooLarge => "output_too_large",
            Self::GuestFailed => "guest_failed",
            Self::InvalidOutput => "invalid_output",
            Self::UnsupportedDocument => "unsupported_document",
        }
    }
}

impl fmt::Display for TransformErrorClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransformBudgets {
    pub module_bytes: u64,
    pub input_bytes: u64,
    pub memory_bytes: u64,
    pub memories: u64,
    pub table_elements: u32,
    pub tables: u64,
    pub instances: u64,
    pub output_bytes: u64,
    pub wall_time: Duration,
    pub epoch_interval: Duration,
    pub stale_heartbeat: Duration,
    pub fuel: u64,
    pub fuel_yield_interval: u64,
    pub concurrency: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformTerminationClass {
    Success,
    Failure(TransformErrorClass),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransformExecutionRecord {
    pub transform_id: String,
    pub module_sha256: [u8; 32],
    pub abi_version: String,
    pub runtime_version: String,
    pub effective_budgets: TransformBudgets,
    pub input_sha256: Option<[u8; 32]>,
    pub output_sha256: Option<[u8; 32]>,
    pub termination_class: TransformTerminationClass,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransformOutcome {
    pub output: Vec<u8>,
    pub record: TransformExecutionRecord,
}

pub struct TransformFailure {
    pub class: TransformErrorClass,
    pub record: TransformExecutionRecord,
    _private_source: Option<String>,
}

impl TransformFailure {
    pub fn new(
        class: TransformErrorClass,
        record: TransformExecutionRecord,
        private_source: Option<String>,
    ) -> Self {
        Self {
            class,
            record,
            _private_source: private_source,
        }
    }

    #[cfg(any(test, debug_assertions))]
    pub fn private_source(&self) -> Option<&str> {
        self._private_source.as_deref()
    }
}

impl fmt::Display for TransformFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.class.fmt(formatter)
    }
}

impl fmt::Debug for TransformFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransformFailure")
            .field("class", &self.class)
            .field("record", &self.record)
            .finish_non_exhaustive()
    }
}

impl std::error::Error for TransformFailure {}

pub trait TransformRuntime: Send + Sync + 'static {
    fn try_begin(&self, transform_id: &str)
    -> Result<Box<dyn TransformLease>, TransformBeginError>;
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait TransformLease: Send {
    fn max_input_bytes(&self) -> u64;

    async fn run(self: Box<Self>, input: Vec<u8>) -> Result<TransformOutcome, TransformFailure>;
}

pub type TransformRuntimeHandle = Arc<dyn TransformRuntime>;

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> TransformExecutionRecord {
        TransformExecutionRecord {
            transform_id: "checked.transform".to_string(),
            module_sha256: [1; 32],
            abi_version: "abi.v1".to_string(),
            runtime_version: "runtime.v1".to_string(),
            effective_budgets: TransformBudgets {
                module_bytes: 1,
                input_bytes: 2,
                memory_bytes: 3,
                memories: 1,
                table_elements: 4,
                tables: 1,
                instances: 1,
                output_bytes: 5,
                wall_time: Duration::from_secs(1),
                epoch_interval: Duration::from_millis(10),
                stale_heartbeat: Duration::from_millis(250),
                fuel: 6,
                fuel_yield_interval: 7,
                concurrency: 2,
            },
            input_sha256: Some([2; 32]),
            output_sha256: None,
            termination_class: TransformTerminationClass::Failure(TransformErrorClass::GuestFailed),
        }
    }

    #[test]
    fn failure_formatting_never_exposes_private_source() {
        let failure = TransformFailure::new(
            TransformErrorClass::GuestFailed,
            record(),
            Some("private trap and parser path".to_string()),
        );
        assert_eq!(failure.to_string(), "guest_failed");
        assert!(!format!("{failure:?}").contains("private trap"));
        assert_eq!(
            failure.private_source(),
            Some("private trap and parser path")
        );
    }
}
