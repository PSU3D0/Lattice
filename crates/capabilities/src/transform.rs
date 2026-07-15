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
    PlatformTerminated,
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
            Self::PlatformTerminated => "platform_terminated",
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformBackend {
    Wasmtime,
    CloudflareWorkers,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformModuleIdentity {
    /// Digest measured from module bytes admitted by the native runtime.
    RuntimeVerifiedSha256([u8; 32]),
    /// Digest attested by the checked render/build pipeline for a precompiled binding.
    /// Workers do not expose binding bytes for runtime measurement.
    BuildTimeAttestedSha256([u8; 32]),
}

impl TransformModuleIdentity {
    pub const fn sha256(self) -> [u8; 32] {
        match self {
            Self::RuntimeVerifiedSha256(digest) | Self::BuildTimeAttestedSha256(digest) => digest,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformInstanceModel {
    FreshPerInvocation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeteredTransformBudgets {
    module_bytes: u64,
    input_bytes: u64,
    memory_bytes: u64,
    memories: u64,
    table_elements: u32,
    tables: u64,
    instances: u64,
    output_bytes: u64,
    wall_time: Duration,
    epoch_interval: Duration,
    stale_heartbeat: Duration,
    fuel: u64,
    fuel_yield_interval: u64,
    concurrency: u64,
}

impl MeteredTransformBudgets {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        module_bytes: u64,
        input_bytes: u64,
        memory_bytes: u64,
        memories: u64,
        table_elements: u32,
        tables: u64,
        instances: u64,
        output_bytes: u64,
        wall_time: Duration,
        epoch_interval: Duration,
        stale_heartbeat: Duration,
        fuel: u64,
        fuel_yield_interval: u64,
        concurrency: u64,
    ) -> Self {
        Self {
            module_bytes,
            input_bytes,
            memory_bytes,
            memories,
            table_elements,
            tables,
            instances,
            output_bytes,
            wall_time,
            epoch_interval,
            stale_heartbeat,
            fuel,
            fuel_yield_interval,
            concurrency,
        }
    }

    pub const fn module_bytes(&self) -> u64 {
        self.module_bytes
    }
    pub const fn input_bytes(&self) -> u64 {
        self.input_bytes
    }
    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }
    pub const fn memories(&self) -> u64 {
        self.memories
    }
    pub const fn table_elements(&self) -> u32 {
        self.table_elements
    }
    pub const fn tables(&self) -> u64 {
        self.tables
    }
    pub const fn instances(&self) -> u64 {
        self.instances
    }
    pub const fn output_bytes(&self) -> u64 {
        self.output_bytes
    }
    pub const fn wall_time(&self) -> Duration {
        self.wall_time
    }
    pub const fn epoch_interval(&self) -> Duration {
        self.epoch_interval
    }
    pub const fn stale_heartbeat(&self) -> Duration {
        self.stale_heartbeat
    }
    pub const fn fuel(&self) -> u64 {
        self.fuel
    }
    pub const fn fuel_yield_interval(&self) -> u64 {
        self.fuel_yield_interval
    }
    pub const fn concurrency(&self) -> u64 {
        self.concurrency
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlatformTransformBudgets {
    cpu_ms_limit: u64,
    isolate_memory_bytes: u64,
    guest_memory_bytes: u64,
    input_bytes: u64,
    output_bytes: u64,
    per_isolate_concurrency: u64,
    instance_model: TransformInstanceModel,
}

impl PlatformTransformBudgets {
    pub fn new(
        cpu_ms_limit: u64,
        isolate_memory_bytes: u64,
        guest_memory_bytes: u64,
        input_bytes: u64,
        output_bytes: u64,
        per_isolate_concurrency: u64,
        instance_model: TransformInstanceModel,
    ) -> Result<Self, TransformRecordError> {
        if cpu_ms_limit == 0
            || isolate_memory_bytes == 0
            || guest_memory_bytes == 0
            || input_bytes == 0
            || output_bytes == 0
        {
            return Err(TransformRecordError::ZeroPlatformBudget);
        }
        if guest_memory_bytes >= isolate_memory_bytes {
            return Err(TransformRecordError::GuestMemoryNotBelowIsolate);
        }
        if per_isolate_concurrency != 1 {
            return Err(TransformRecordError::PlatformConcurrencyMustBeOne);
        }
        Ok(Self {
            cpu_ms_limit,
            isolate_memory_bytes,
            guest_memory_bytes,
            input_bytes,
            output_bytes,
            per_isolate_concurrency,
            instance_model,
        })
    }

    /// Returns rendered platform policy, not observed CPU consumption.
    pub const fn cpu_ms_limit(&self) -> u64 {
        self.cpu_ms_limit
    }
    pub const fn isolate_memory_bytes(&self) -> u64 {
        self.isolate_memory_bytes
    }
    pub const fn guest_memory_bytes(&self) -> u64 {
        self.guest_memory_bytes
    }
    pub const fn input_bytes(&self) -> u64 {
        self.input_bytes
    }
    pub const fn output_bytes(&self) -> u64 {
        self.output_bytes
    }
    pub const fn per_isolate_concurrency(&self) -> u64 {
        self.per_isolate_concurrency
    }
    pub const fn instance_model(&self) -> TransformInstanceModel {
        self.instance_model
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformTerminationClass {
    Success,
    Failure(TransformErrorClass),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeteredTransformObservations {
    /// Native guest execution time from admitted dispatch through terminal observation.
    /// Failures rejected before guest dispatch report zero.
    duration: Duration,
    input_bytes: u64,
    output_bytes: u64,
    fuel_consumed: Option<u64>,
    peak_requested_memory_bytes: Option<u64>,
}

impl MeteredTransformObservations {
    pub const fn new(
        duration: Duration,
        input_bytes: u64,
        output_bytes: u64,
        fuel_consumed: Option<u64>,
        peak_requested_memory_bytes: Option<u64>,
    ) -> Self {
        Self {
            duration,
            input_bytes,
            output_bytes,
            fuel_consumed,
            peak_requested_memory_bytes,
        }
    }

    pub const fn duration(&self) -> Duration {
        self.duration
    }
    pub const fn input_bytes(&self) -> u64 {
        self.input_bytes
    }
    pub const fn output_bytes(&self) -> u64 {
        self.output_bytes
    }
    pub const fn fuel_consumed(&self) -> Option<u64> {
        self.fuel_consumed
    }
    pub const fn peak_requested_memory_bytes(&self) -> Option<u64> {
        self.peak_requested_memory_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlatformTransformObservations {
    /// Trusted adapter-observed service invocation duration, not guest CPU time.
    duration: Duration,
    input_bytes: u64,
    output_bytes: u64,
}

impl PlatformTransformObservations {
    pub const fn new(duration: Duration, input_bytes: u64, output_bytes: u64) -> Self {
        Self {
            duration,
            input_bytes,
            output_bytes,
        }
    }

    pub const fn duration(&self) -> Duration {
        self.duration
    }
    pub const fn input_bytes(&self) -> u64 {
        self.input_bytes
    }
    pub const fn output_bytes(&self) -> u64 {
        self.output_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformRecordError {
    EmptyTransformId,
    EmptyAbiVersion,
    EmptyRuntimeVersion,
    EmptyCompatibilityDate,
    MissingOutputHashForSuccess,
    OutputHashForFailure,
    InvalidTerminationForBackend,
    ZeroPlatformBudget,
    GuestMemoryNotBelowIsolate,
    PlatformConcurrencyMustBeOne,
    FailureClassMismatch,
    OutcomeRecordNotSuccess,
}

impl fmt::Display for TransformRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyTransformId => "empty transform id",
            Self::EmptyAbiVersion => "empty ABI version",
            Self::EmptyRuntimeVersion => "empty runtime version",
            Self::EmptyCompatibilityDate => "empty platform compatibility date",
            Self::MissingOutputHashForSuccess => "successful transform is missing output hash",
            Self::OutputHashForFailure => "failed transform has output hash",
            Self::InvalidTerminationForBackend => "termination class is invalid for backend",
            Self::ZeroPlatformBudget => "platform budgets must be nonzero",
            Self::GuestMemoryNotBelowIsolate => "guest memory must be below isolate memory",
            Self::PlatformConcurrencyMustBeOne => "platform transform concurrency must be one",
            Self::FailureClassMismatch => "failure class does not match transform record",
            Self::OutcomeRecordNotSuccess => "outcome transform record is not successful",
        })
    }
}

impl std::error::Error for TransformRecordError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeteredTransformExecutionRecord {
    transform_id: String,
    module_identity: TransformModuleIdentity,
    abi_version: String,
    runtime_version: String,
    effective_budgets: MeteredTransformBudgets,
    input_sha256: Option<[u8; 32]>,
    output_sha256: Option<[u8; 32]>,
    termination_class: TransformTerminationClass,
    observations: MeteredTransformObservations,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlatformTransformExecutionRecord {
    transform_id: String,
    module_identity: TransformModuleIdentity,
    abi_version: String,
    compatibility_date: String,
    effective_budgets: PlatformTransformBudgets,
    input_sha256: Option<[u8; 32]>,
    output_sha256: Option<[u8; 32]>,
    termination_class: TransformTerminationClass,
    observations: PlatformTransformObservations,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransformExecutionRecord {
    Metered(MeteredTransformExecutionRecord),
    Platform(PlatformTransformExecutionRecord),
}

impl TransformExecutionRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn metered(
        transform_id: impl Into<String>,
        module_sha256: [u8; 32],
        abi_version: impl Into<String>,
        runtime_version: impl Into<String>,
        effective_budgets: MeteredTransformBudgets,
        input_sha256: Option<[u8; 32]>,
        output_sha256: Option<[u8; 32]>,
        termination_class: TransformTerminationClass,
        observations: MeteredTransformObservations,
    ) -> Result<Self, TransformRecordError> {
        let transform_id = transform_id.into();
        let abi_version = abi_version.into();
        let runtime_version = runtime_version.into();
        validate_common(
            &transform_id,
            &abi_version,
            output_sha256,
            termination_class,
        )?;
        if runtime_version.is_empty() {
            return Err(TransformRecordError::EmptyRuntimeVersion);
        }
        if matches!(
            termination_class,
            TransformTerminationClass::Failure(TransformErrorClass::PlatformTerminated)
        ) {
            return Err(TransformRecordError::InvalidTerminationForBackend);
        }
        Ok(Self::Metered(MeteredTransformExecutionRecord {
            transform_id,
            module_identity: TransformModuleIdentity::RuntimeVerifiedSha256(module_sha256),
            abi_version,
            runtime_version,
            effective_budgets,
            input_sha256,
            output_sha256,
            termination_class,
            observations,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn platform(
        transform_id: impl Into<String>,
        build_time_attested_module_sha256: [u8; 32],
        abi_version: impl Into<String>,
        compatibility_date: impl Into<String>,
        effective_budgets: PlatformTransformBudgets,
        input_sha256: Option<[u8; 32]>,
        output_sha256: Option<[u8; 32]>,
        termination_class: TransformTerminationClass,
        observations: PlatformTransformObservations,
    ) -> Result<Self, TransformRecordError> {
        let transform_id = transform_id.into();
        let abi_version = abi_version.into();
        let compatibility_date = compatibility_date.into();
        validate_common(
            &transform_id,
            &abi_version,
            output_sha256,
            termination_class,
        )?;
        if compatibility_date.is_empty() {
            return Err(TransformRecordError::EmptyCompatibilityDate);
        }
        if matches!(
            termination_class,
            TransformTerminationClass::Failure(
                TransformErrorClass::FuelExhausted
                    | TransformErrorClass::MemoryExhausted
                    | TransformErrorClass::WallTimeExceeded
            )
        ) {
            return Err(TransformRecordError::InvalidTerminationForBackend);
        }
        Ok(Self::Platform(PlatformTransformExecutionRecord {
            transform_id,
            module_identity: TransformModuleIdentity::BuildTimeAttestedSha256(
                build_time_attested_module_sha256,
            ),
            abi_version,
            compatibility_date,
            effective_budgets,
            input_sha256,
            output_sha256,
            termination_class,
            observations,
        }))
    }

    pub const fn backend(&self) -> TransformBackend {
        match self {
            Self::Metered(_) => TransformBackend::Wasmtime,
            Self::Platform(_) => TransformBackend::CloudflareWorkers,
        }
    }

    pub fn transform_id(&self) -> &str {
        match self {
            Self::Metered(record) => &record.transform_id,
            Self::Platform(record) => &record.transform_id,
        }
    }

    pub const fn module_identity(&self) -> TransformModuleIdentity {
        match self {
            Self::Metered(record) => record.module_identity,
            Self::Platform(record) => record.module_identity,
        }
    }

    pub const fn module_sha256(&self) -> [u8; 32] {
        self.module_identity().sha256()
    }

    pub fn abi_version(&self) -> &str {
        match self {
            Self::Metered(record) => &record.abi_version,
            Self::Platform(record) => &record.abi_version,
        }
    }

    pub fn runtime_version(&self) -> Option<&str> {
        match self {
            Self::Metered(record) => Some(&record.runtime_version),
            Self::Platform(_) => None,
        }
    }

    /// Returns the rendered Workers compatibility date for a platform record.
    /// This is deployment policy identity, not a runtime version.
    pub fn compatibility_date(&self) -> Option<&str> {
        match self {
            Self::Metered(_) => None,
            Self::Platform(record) => Some(&record.compatibility_date),
        }
    }

    pub const fn input_sha256(&self) -> Option<[u8; 32]> {
        match self {
            Self::Metered(record) => record.input_sha256,
            Self::Platform(record) => record.input_sha256,
        }
    }

    pub const fn output_sha256(&self) -> Option<[u8; 32]> {
        match self {
            Self::Metered(record) => record.output_sha256,
            Self::Platform(record) => record.output_sha256,
        }
    }

    pub const fn termination_class(&self) -> TransformTerminationClass {
        match self {
            Self::Metered(record) => record.termination_class,
            Self::Platform(record) => record.termination_class,
        }
    }

    pub const fn duration(&self) -> Duration {
        match self {
            Self::Metered(record) => record.observations.duration,
            Self::Platform(record) => record.observations.duration,
        }
    }

    pub const fn observed_input_bytes(&self) -> u64 {
        match self {
            Self::Metered(record) => record.observations.input_bytes,
            Self::Platform(record) => record.observations.input_bytes,
        }
    }

    pub const fn observed_output_bytes(&self) -> u64 {
        match self {
            Self::Metered(record) => record.observations.output_bytes,
            Self::Platform(record) => record.observations.output_bytes,
        }
    }

    pub const fn metered_budgets(&self) -> Option<&MeteredTransformBudgets> {
        match self {
            Self::Metered(record) => Some(&record.effective_budgets),
            Self::Platform(_) => None,
        }
    }

    pub const fn platform_budgets(&self) -> Option<&PlatformTransformBudgets> {
        match self {
            Self::Metered(_) => None,
            Self::Platform(record) => Some(&record.effective_budgets),
        }
    }

    pub const fn metered_observations(&self) -> Option<&MeteredTransformObservations> {
        match self {
            Self::Metered(record) => Some(&record.observations),
            Self::Platform(_) => None,
        }
    }

    pub const fn platform_observations(&self) -> Option<&PlatformTransformObservations> {
        match self {
            Self::Metered(_) => None,
            Self::Platform(record) => Some(&record.observations),
        }
    }
}

fn validate_common(
    transform_id: &str,
    abi_version: &str,
    output_sha256: Option<[u8; 32]>,
    termination_class: TransformTerminationClass,
) -> Result<(), TransformRecordError> {
    if transform_id.is_empty() {
        return Err(TransformRecordError::EmptyTransformId);
    }
    if abi_version.is_empty() {
        return Err(TransformRecordError::EmptyAbiVersion);
    }
    match (termination_class, output_sha256) {
        (TransformTerminationClass::Success, None) => {
            return Err(TransformRecordError::MissingOutputHashForSuccess);
        }
        (TransformTerminationClass::Failure(_), Some(_)) => {
            return Err(TransformRecordError::OutputHashForFailure);
        }
        _ => {}
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransformOutcome {
    output: Vec<u8>,
    record: TransformExecutionRecord,
}

impl TransformOutcome {
    pub fn new(
        output: Vec<u8>,
        record: TransformExecutionRecord,
    ) -> Result<Self, TransformRecordError> {
        if record.termination_class() != TransformTerminationClass::Success {
            return Err(TransformRecordError::OutcomeRecordNotSuccess);
        }
        Ok(Self { output, record })
    }

    pub fn into_parts(self) -> (Vec<u8>, TransformExecutionRecord) {
        (self.output, self.record)
    }
}

pub struct TransformFailure {
    class: TransformErrorClass,
    record: TransformExecutionRecord,
    _private_source: Option<String>,
}

impl TransformFailure {
    pub fn new(
        class: TransformErrorClass,
        record: TransformExecutionRecord,
        private_source: Option<String>,
    ) -> Result<Self, TransformRecordError> {
        if record.termination_class() != TransformTerminationClass::Failure(class) {
            return Err(TransformRecordError::FailureClassMismatch);
        }
        Ok(Self {
            class,
            record,
            _private_source: private_source,
        })
    }

    #[cfg(any(test, debug_assertions))]
    pub fn private_source(&self) -> Option<&str> {
        self._private_source.as_deref()
    }

    pub const fn class(&self) -> TransformErrorClass {
        self.class
    }

    pub fn record(&self) -> &TransformExecutionRecord {
        &self.record
    }

    pub fn into_record(self) -> TransformExecutionRecord {
        self.record
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

    fn metered_budgets() -> MeteredTransformBudgets {
        MeteredTransformBudgets::new(
            1,
            2,
            3,
            1,
            4,
            1,
            1,
            5,
            Duration::from_secs(1),
            Duration::from_millis(10),
            Duration::from_millis(250),
            6,
            7,
            2,
        )
    }

    fn metered_failure(
        class: TransformErrorClass,
    ) -> Result<TransformExecutionRecord, TransformRecordError> {
        TransformExecutionRecord::metered(
            "checked.transform",
            [1; 32],
            "abi.v1",
            "runtime.v1",
            metered_budgets(),
            Some([2; 32]),
            None,
            TransformTerminationClass::Failure(class),
            MeteredTransformObservations::new(Duration::from_millis(3), 2, 0, Some(4), Some(3)),
        )
    }

    fn platform_budgets() -> PlatformTransformBudgets {
        PlatformTransformBudgets::new(
            50,
            128 * 1024 * 1024,
            64 * 1024 * 1024,
            8 * 1024 * 1024,
            4 + 512 * 1024,
            1,
            TransformInstanceModel::FreshPerInvocation,
        )
        .expect("valid platform budgets")
    }

    #[test]
    fn failure_formatting_never_exposes_private_source() {
        let failure = TransformFailure::new(
            TransformErrorClass::GuestFailed,
            metered_failure(TransformErrorClass::GuestFailed).expect("valid record"),
            Some("private trap and parser path".to_string()),
        )
        .expect("matching failure class");
        assert_eq!(failure.to_string(), "guest_failed");
        assert!(!format!("{failure:?}").contains("private trap"));
        assert_eq!(
            failure.private_source(),
            Some("private trap and parser path")
        );
    }

    #[test]
    fn failure_class_must_match_checked_terminal_record() {
        assert!(matches!(
            TransformFailure::new(
                TransformErrorClass::InvalidOutput,
                metered_failure(TransformErrorClass::GuestFailed).expect("valid record"),
                None,
            ),
            Err(TransformRecordError::FailureClassMismatch)
        ));
    }

    #[test]
    fn backend_specific_terminations_are_checked() {
        assert_eq!(
            metered_failure(TransformErrorClass::PlatformTerminated),
            Err(TransformRecordError::InvalidTerminationForBackend)
        );
        for class in [
            TransformErrorClass::FuelExhausted,
            TransformErrorClass::MemoryExhausted,
            TransformErrorClass::WallTimeExceeded,
        ] {
            assert_eq!(
                TransformExecutionRecord::platform(
                    "checked.transform",
                    [1; 32],
                    "abi.v1",
                    "2026-07-15",
                    platform_budgets(),
                    Some([2; 32]),
                    None,
                    TransformTerminationClass::Failure(class),
                    PlatformTransformObservations::new(Duration::from_millis(3), 2, 0),
                ),
                Err(TransformRecordError::InvalidTerminationForBackend)
            );
        }
    }

    #[test]
    fn success_and_failure_output_hash_invariants_are_checked() {
        assert_eq!(
            TransformExecutionRecord::metered(
                "checked.transform",
                [1; 32],
                "abi.v1",
                "runtime.v1",
                metered_budgets(),
                Some([2; 32]),
                None,
                TransformTerminationClass::Success,
                MeteredTransformObservations::new(Duration::ZERO, 2, 0, None, None),
            ),
            Err(TransformRecordError::MissingOutputHashForSuccess)
        );
        assert_eq!(
            TransformExecutionRecord::platform(
                "checked.transform",
                [1; 32],
                "abi.v1",
                "2026-07-15",
                platform_budgets(),
                Some([2; 32]),
                Some([3; 32]),
                TransformTerminationClass::Failure(TransformErrorClass::GuestFailed),
                PlatformTransformObservations::new(Duration::ZERO, 2, 1),
            ),
            Err(TransformRecordError::OutputHashForFailure)
        );
        let success = TransformExecutionRecord::platform(
            "checked.transform",
            [1; 32],
            "abi.v1",
            "2026-07-15",
            platform_budgets(),
            Some([2; 32]),
            Some([3; 32]),
            TransformTerminationClass::Success,
            PlatformTransformObservations::new(Duration::from_millis(1), 2, 1),
        )
        .expect("successful platform record with output hash");
        assert_eq!(success.output_sha256(), Some([3; 32]));
        assert_eq!(
            TransformExecutionRecord::platform(
                "checked.transform",
                [1; 32],
                "abi.v1",
                "",
                platform_budgets(),
                Some([2; 32]),
                None,
                TransformTerminationClass::Failure(TransformErrorClass::PlatformTerminated),
                PlatformTransformObservations::new(Duration::ZERO, 2, 0),
            ),
            Err(TransformRecordError::EmptyCompatibilityDate)
        );
    }

    #[test]
    fn outcomes_require_successful_terminal_records() {
        let failure_record = metered_failure(TransformErrorClass::GuestFailed)
            .expect("valid metered failure record");
        assert_eq!(
            TransformOutcome::new(Vec::new(), failure_record),
            Err(TransformRecordError::OutcomeRecordNotSuccess)
        );

        let success_record = TransformExecutionRecord::metered(
            "checked.transform",
            [1; 32],
            "abi.v1",
            "runtime.v1",
            metered_budgets(),
            Some([2; 32]),
            Some([3; 32]),
            TransformTerminationClass::Success,
            MeteredTransformObservations::new(Duration::from_millis(1), 2, 1, Some(1), Some(3)),
        )
        .expect("valid successful record");
        let (output, record) = TransformOutcome::new(vec![7], success_record)
            .expect("successful record creates an outcome")
            .into_parts();
        assert_eq!(output, vec![7]);
        assert_eq!(
            record.termination_class(),
            TransformTerminationClass::Success
        );
    }

    #[test]
    fn platform_budgets_are_nonzero_bounded_and_single_flight() {
        for (cpu, isolate, guest, input, output) in [
            (0, 128, 64, 8, 4),
            (1, 0, 64, 8, 4),
            (1, 128, 0, 8, 4),
            (1, 128, 64, 0, 4),
            (1, 128, 64, 8, 0),
        ] {
            assert_eq!(
                PlatformTransformBudgets::new(
                    cpu,
                    isolate,
                    guest,
                    input,
                    output,
                    1,
                    TransformInstanceModel::FreshPerInvocation,
                ),
                Err(TransformRecordError::ZeroPlatformBudget)
            );
        }
        assert_eq!(
            PlatformTransformBudgets::new(
                1,
                64,
                64,
                8,
                4,
                1,
                TransformInstanceModel::FreshPerInvocation,
            ),
            Err(TransformRecordError::GuestMemoryNotBelowIsolate)
        );
        assert_eq!(
            PlatformTransformBudgets::new(
                1,
                128,
                64,
                8,
                4,
                2,
                TransformInstanceModel::FreshPerInvocation,
            ),
            Err(TransformRecordError::PlatformConcurrencyMustBeOne)
        );
        let budgets = platform_budgets();
        assert_eq!(budgets.per_isolate_concurrency(), 1);
        assert_eq!(
            budgets.instance_model(),
            TransformInstanceModel::FreshPerInvocation
        );
        assert!(budgets.guest_memory_bytes() < budgets.isolate_memory_bytes());
    }

    #[test]
    fn common_getters_preserve_backend_truth() {
        let metered = metered_failure(TransformErrorClass::GuestFailed).expect("metered record");
        assert_eq!(metered.backend(), TransformBackend::Wasmtime);
        assert_eq!(metered.transform_id(), "checked.transform");
        assert_eq!(
            metered.module_identity(),
            TransformModuleIdentity::RuntimeVerifiedSha256([1; 32])
        );
        assert_eq!(metered.module_sha256(), [1; 32]);
        assert_eq!(metered.abi_version(), "abi.v1");
        assert_eq!(metered.runtime_version(), Some("runtime.v1"));
        assert_eq!(metered.compatibility_date(), None);
        assert_eq!(metered.input_sha256(), Some([2; 32]));
        assert_eq!(metered.output_sha256(), None);
        assert_eq!(
            metered.termination_class(),
            TransformTerminationClass::Failure(TransformErrorClass::GuestFailed)
        );
        assert_eq!(metered.duration(), Duration::from_millis(3));
        assert_eq!(metered.observed_input_bytes(), 2);
        assert_eq!(metered.observed_output_bytes(), 0);
        assert!(metered.metered_budgets().is_some());
        assert!(metered.platform_budgets().is_none());
        let observations = metered
            .metered_observations()
            .expect("metered observations");
        assert_eq!(observations.fuel_consumed(), Some(4));
        assert_eq!(observations.peak_requested_memory_bytes(), Some(3));
        assert!(metered.platform_observations().is_none());

        let platform = TransformExecutionRecord::platform(
            "checked.transform",
            [9; 32],
            "abi.v1",
            "2026-07-15",
            platform_budgets(),
            Some([2; 32]),
            None,
            TransformTerminationClass::Failure(TransformErrorClass::PlatformTerminated),
            PlatformTransformObservations::new(Duration::from_millis(3), 2, 0),
        )
        .expect("platform record");
        assert_eq!(platform.backend(), TransformBackend::CloudflareWorkers);
        assert_eq!(
            platform.module_identity(),
            TransformModuleIdentity::BuildTimeAttestedSha256([9; 32])
        );
        assert_eq!(platform.module_sha256(), [9; 32]);
        assert_eq!(platform.abi_version(), "abi.v1");
        assert_eq!(platform.runtime_version(), None);
        assert_eq!(platform.compatibility_date(), Some("2026-07-15"));
        assert_eq!(platform.input_sha256(), Some([2; 32]));
        assert_eq!(platform.output_sha256(), None);
        assert_eq!(
            platform.termination_class(),
            TransformTerminationClass::Failure(TransformErrorClass::PlatformTerminated)
        );
        assert_eq!(platform.duration(), Duration::from_millis(3));
        assert_eq!(platform.observed_input_bytes(), 2);
        assert_eq!(platform.observed_output_bytes(), 0);
        assert!(platform.metered_budgets().is_none());
        assert!(platform.platform_budgets().is_some());
        assert!(platform.metered_observations().is_none());
        assert!(platform.platform_observations().is_some());
    }
}
