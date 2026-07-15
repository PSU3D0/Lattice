#![cfg(not(target_arch = "wasm32"))]

use anyhow::anyhow;
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::CancellationToken;
use wasmparser::{Encoding, Parser, Payload};
mod service;

pub use service::PdfTransformRuntime;

use wasmtime::{
    Config, Engine, ExternType, Instance, Linker, Memory, Module, Mutability, ResourceLimiter,
    Store, StoreLimits, StoreLimitsBuilder, Trap, UpdateDeadline, ValType,
};

pub const ABI_VERSION: &str = "lattice.transform.v1";
pub const RUNTIME_VERSION: &str = "wasmtime-16.0.0";
pub const MAX_HOST_CONCURRENCY: usize = 2;
pub const GUEST_ERROR_UNSUPPORTED_DOCUMENT: i32 = 1;
pub const PDF_EXTRACT_TRANSFORM_ID: &str = "lattice.pdf.extract_text.v1";
const PDF_EXTRACT_MODULE_SHA256: [u8; 32] = [
    0x04, 0x8f, 0x65, 0x0a, 0xec, 0x85, 0x02, 0x65, 0x96, 0x33, 0x28, 0x9a, 0x4a, 0xce, 0x49, 0x3c,
    0x56, 0xa7, 0xbc, 0x6e, 0x95, 0xc8, 0xda, 0x3d, 0x4a, 0x34, 0xe2, 0x93, 0xe9, 0x6d, 0x4e, 0x96,
];
const PDF_EXTRACT_MODULE_BYTES: &[u8] = include_bytes!("../guests/pdf-extract/pdf_extract.wasm");
const WASM_PAGE_BYTES: u64 = 65_536;
const MIN_EPOCH_INTERVAL: Duration = Duration::from_millis(1);
const MIN_STALE_HEARTBEAT: Duration = Duration::from_millis(4);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineControls {
    pub consume_fuel: bool,
    pub epoch_interruption: bool,
    pub async_support: bool,
    pub max_wasm_stack: usize,
    pub async_stack_size: usize,
    pub nan_canonicalization: bool,
    pub relaxed_simd: bool,
    pub threads: bool,
    pub memory64: bool,
    pub multi_memory: bool,
    pub simd: bool,
    pub reference_types: bool,
    pub function_references: bool,
    pub bulk_memory: bool,
    pub multi_value: bool,
    pub tail_call: bool,
}

pub const ENGINE_CONTROLS: EngineControls = EngineControls {
    consume_fuel: true,
    epoch_interruption: true,
    async_support: true,
    max_wasm_stack: 1024 * 1024,
    async_stack_size: 2 * 1024 * 1024,
    nan_canonicalization: true,
    relaxed_simd: false,
    threads: false,
    memory64: false,
    multi_memory: false,
    simd: false,
    reference_types: false,
    function_references: false,
    bulk_memory: false,
    multi_value: false,
    tail_call: false,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessingBudgets {
    pub module_bytes: usize,
    pub input_bytes: usize,
    pub memory_bytes: usize,
    pub memories: usize,
    pub table_elements: u32,
    pub tables: usize,
    pub instances: usize,
    pub output_bytes: usize,
    pub wall_time: Duration,
    pub epoch_interval: Duration,
    pub stale_heartbeat: Duration,
    pub fuel: u64,
    pub fuel_yield_interval: u64,
    pub concurrency: usize,
}

impl Default for ProcessingBudgets {
    fn default() -> Self {
        Self {
            module_bytes: 8 * 1024 * 1024,
            input_bytes: 8 * 1024 * 1024,
            memory_bytes: 128 * 1024 * 1024,
            memories: 1,
            table_elements: 4096,
            tables: 1,
            instances: 1,
            output_bytes: 4 + 512 * 1024,
            wall_time: Duration::from_secs(2),
            epoch_interval: Duration::from_millis(10),
            stale_heartbeat: Duration::from_millis(250),
            fuel: 10_000_000,
            fuel_yield_interval: 10_000,
            concurrency: MAX_HOST_CONCURRENCY,
        }
    }
}

impl ProcessingBudgets {
    fn bounded_by(requested: Self, ceiling: &Self) -> Result<Self, InitializationError> {
        let effective = Self {
            module_bytes: requested.module_bytes.min(ceiling.module_bytes),
            input_bytes: requested.input_bytes.min(ceiling.input_bytes),
            memory_bytes: requested.memory_bytes.min(ceiling.memory_bytes),
            memories: requested.memories.min(ceiling.memories),
            table_elements: requested.table_elements.min(ceiling.table_elements),
            tables: requested.tables.min(ceiling.tables),
            instances: requested.instances.min(ceiling.instances),
            output_bytes: requested.output_bytes.min(ceiling.output_bytes),
            wall_time: requested.wall_time.min(ceiling.wall_time),
            epoch_interval: requested.epoch_interval.min(ceiling.epoch_interval),
            stale_heartbeat: requested.stale_heartbeat.min(ceiling.stale_heartbeat),
            fuel: requested.fuel.min(ceiling.fuel),
            fuel_yield_interval: requested
                .fuel_yield_interval
                .min(ceiling.fuel_yield_interval),
            concurrency: requested.concurrency.min(ceiling.concurrency),
        };
        if effective.module_bytes == 0
            || effective.input_bytes == 0
            || effective.memory_bytes == 0
            || effective.memories == 0
            || effective.tables == 0
            || effective.instances == 0
            || effective.output_bytes == 0
            || effective.wall_time.is_zero()
            || effective.epoch_interval.is_zero()
            || effective.stale_heartbeat.is_zero()
            || effective.fuel == 0
            || effective.fuel_yield_interval == 0
            || effective.concurrency == 0
            || effective.epoch_interval < MIN_EPOCH_INTERVAL
            || effective.stale_heartbeat < MIN_STALE_HEARTBEAT
            || effective.wall_time < effective.epoch_interval
            || effective.stale_heartbeat < effective.epoch_interval.saturating_mul(3)
        {
            return Err(InitializationError::configuration());
        }
        Ok(effective)
    }
}

pub struct ModuleDescriptor {
    transform_id: &'static str,
    module_sha256: [u8; 32],
    abi_version: &'static str,
    module_bytes: &'static [u8],
}

impl ModuleDescriptor {
    /// Defines a transform embedded and allowlisted by trusted host code.
    ///
    /// This is not a flow-author surface: request data can select only the
    /// `transform_id` values registered by the host's `ProcessingRuntime`.
    pub const fn host_allowlisted(
        transform_id: &'static str,
        module_sha256: [u8; 32],
        module_bytes: &'static [u8],
    ) -> Self {
        Self {
            transform_id,
            module_sha256,
            abi_version: ABI_VERSION,
            module_bytes,
        }
    }

    pub const fn transform_id(&self) -> &'static str {
        self.transform_id
    }

    pub const fn module_sha256(&self) -> [u8; 32] {
        self.module_sha256
    }

    pub const fn abi_version(&self) -> &'static str {
        self.abi_version
    }

    #[cfg(test)]
    fn synthetic(
        transform_id: &'static str,
        module_sha256: [u8; 32],
        module_bytes: &'static [u8],
    ) -> Self {
        Self::host_allowlisted(transform_id, module_sha256, module_bytes)
    }
}

pub static PDF_EXTRACT_MODULE: ModuleDescriptor = ModuleDescriptor::host_allowlisted(
    PDF_EXTRACT_TRANSFORM_ID,
    PDF_EXTRACT_MODULE_SHA256,
    PDF_EXTRACT_MODULE_BYTES,
);

/// Returns the PDF-only sandbox policy.
///
/// The 100M fuel ceiling is calibrated against the checked qpdf-encrypted,
/// FlateDecode expansion, panic-containment, one-page, and multi-page fixtures.
/// It is deliberately separate from the 10M general-processing default; rerun
/// that fixture suite before changing the PDF ceiling.
pub fn pdf_extract_processing_budgets() -> ProcessingBudgets {
    ProcessingBudgets {
        memory_bytes: 64 * 1024 * 1024,
        fuel: 100_000_000,
        ..ProcessingBudgets::default()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicError {
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

impl PublicError {
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

impl fmt::Display for PublicError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminationClass {
    Success,
    Failure(PublicError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransformRecord {
    pub transform_id: &'static str,
    pub module_sha256: [u8; 32],
    pub abi_version: &'static str,
    pub runtime_version: &'static str,
    pub effective_budgets: ProcessingBudgets,
    /// `None` only when the input was rejected by the byte ceiling before hashing.
    pub input_sha256: Option<[u8; 32]>,
    pub output_sha256: Option<[u8; 32]>,
    pub termination_class: TerminationClass,
    pub observations: TransformObservations,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransformObservations {
    pub duration: Duration,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub fuel_consumed: Option<u64>,
    pub peak_requested_memory_bytes: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessingOutcome {
    pub output: Vec<u8>,
    pub record: TransformRecord,
}

pub struct ProcessingFailure {
    pub public_error: PublicError,
    pub record: TransformRecord,
    private_source: Option<String>,
}

impl ProcessingFailure {
    #[cfg(any(test, debug_assertions))]
    pub fn private_source(&self) -> Option<&str> {
        self.private_source.as_deref()
    }
}

impl fmt::Display for ProcessingFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.public_error.fmt(formatter)
    }
}

impl fmt::Debug for ProcessingFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcessingFailure")
            .field("public_error", &self.public_error)
            .field("record", &self.record)
            .finish_non_exhaustive()
    }
}

impl std::error::Error for ProcessingFailure {}

pub struct InitializationError {
    public_error: PublicError,
    private_source: Option<String>,
}

impl InitializationError {
    fn new(public_error: PublicError, source: impl Into<String>) -> Self {
        Self {
            public_error,
            private_source: Some(source.into()),
        }
    }

    fn configuration() -> Self {
        Self::new(PublicError::InvalidModule, "invalid processing policy")
    }

    pub const fn public_error(&self) -> PublicError {
        self.public_error
    }

    #[cfg(any(test, debug_assertions))]
    pub fn private_source(&self) -> Option<&str> {
        self.private_source.as_deref()
    }
}

impl fmt::Display for InitializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.public_error.fmt(formatter)
    }
}

impl fmt::Debug for InitializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InitializationError")
            .field("public_error", &self.public_error)
            .finish_non_exhaustive()
    }
}

impl std::error::Error for InitializationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BeginError {
    Busy,
    InvalidTransform,
    RuntimeUnavailable,
}

impl fmt::Display for BeginError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Busy => "busy",
            Self::InvalidTransform => "invalid_transform",
            Self::RuntimeUnavailable => "runtime_unavailable",
        })
    }
}

impl std::error::Error for BeginError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LimitDenial {
    Memory,
    Table,
}

struct TrackingLimits {
    limits: StoreLimits,
    denial: Option<LimitDenial>,
    peak_requested_memory_bytes: usize,
}

impl ResourceLimiter for TrackingLimits {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        self.peak_requested_memory_bytes = self.peak_requested_memory_bytes.max(desired);
        let result = self.limits.memory_growing(current, desired, maximum);
        if !matches!(result, Ok(true)) {
            self.denial = Some(LimitDenial::Memory);
        }
        result
    }

    fn memory_grow_failed(&mut self, error: anyhow::Error) -> anyhow::Result<()> {
        self.denial = Some(LimitDenial::Memory);
        self.limits.memory_grow_failed(error)
    }

    fn table_growing(
        &mut self,
        current: u32,
        desired: u32,
        maximum: Option<u32>,
    ) -> anyhow::Result<bool> {
        let result = self.limits.table_growing(current, desired, maximum);
        if !matches!(result, Ok(true)) {
            self.denial = Some(LimitDenial::Table);
        }
        result
    }

    fn table_grow_failed(&mut self, error: anyhow::Error) -> anyhow::Result<()> {
        self.denial = Some(LimitDenial::Table);
        self.limits.table_grow_failed(error)
    }

    fn instances(&self) -> usize {
        self.limits.instances()
    }

    fn tables(&self) -> usize {
        self.limits.tables()
    }

    fn memories(&self) -> usize {
        self.limits.memories()
    }
}

struct StoreState {
    limits: TrackingLimits,
}

struct TimedPermit {
    permit: Mutex<Option<OwnedSemaphorePermit>>,
    cancellation: CancellationToken,
    deadline: Instant,
    started: AtomicBool,
}

impl TimedPermit {
    fn release_if_expired(&self, now: Instant) {
        if now < self.deadline {
            return;
        }
        self.cancellation.cancel();
        let mut permit = self
            .permit
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !self.started.load(Ordering::Acquire) {
            permit.take();
        }
    }

    fn start(&self, now: Instant) -> bool {
        let mut permit = self
            .permit
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if permit.is_none() || now >= self.deadline {
            self.cancellation.cancel();
            permit.take();
            return false;
        }
        self.started.store(true, Ordering::Release);
        true
    }
}

struct TickerState {
    heartbeat: Mutex<Instant>,
    leases: Mutex<Vec<Weak<TimedPermit>>>,
    stop: AtomicBool,
    #[cfg(test)]
    paused: AtomicBool,
}

struct EpochTicker {
    state: Arc<TickerState>,
    thread: Option<JoinHandle<()>>,
}

impl EpochTicker {
    fn spawn(
        engine: Engine,
        interval: Duration,
        startup_timeout: Duration,
    ) -> Result<Self, InitializationError> {
        let state = Arc::new(TickerState {
            heartbeat: Mutex::new(Instant::now()),
            leases: Mutex::new(Vec::new()),
            stop: AtomicBool::new(false),
            #[cfg(test)]
            paused: AtomicBool::new(false),
        });
        let thread_state = Arc::clone(&state);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("processing-context-epoch".to_owned())
            .spawn(move || {
                let mut first = true;
                while !thread_state.stop.load(Ordering::Acquire) {
                    #[cfg(test)]
                    let paused = thread_state.paused.load(Ordering::Acquire);
                    #[cfg(not(test))]
                    let paused = false;
                    if !paused {
                        engine.increment_epoch();
                        if let Ok(mut heartbeat) = thread_state.heartbeat.lock() {
                            *heartbeat = Instant::now();
                        }
                        if first {
                            let _ = ready_tx.send(());
                            first = false;
                        }
                    }
                    let now = Instant::now();
                    if let Ok(mut leases) = thread_state.leases.lock() {
                        leases.retain(|weak| {
                            if let Some(lease) = weak.upgrade() {
                                lease.release_if_expired(now);
                                true
                            } else {
                                false
                            }
                        });
                    }
                    std::thread::sleep(interval);
                }
            })
            .map_err(|error| {
                InitializationError::new(PublicError::RuntimeUnavailable, error.to_string())
            })?;
        if ready_rx.recv_timeout(startup_timeout).is_err() {
            state.stop.store(true, Ordering::Release);
            let _ = thread.join();
            return Err(InitializationError::new(
                PublicError::RuntimeUnavailable,
                "epoch ticker did not prove its first heartbeat",
            ));
        }
        Ok(Self {
            state,
            thread: Some(thread),
        })
    }

    fn healthy(&self, stale_after: Duration) -> bool {
        self.state
            .heartbeat
            .lock()
            .map(|heartbeat| heartbeat.elapsed() <= stale_after)
            .unwrap_or(false)
    }

    fn register(&self, lease: &Arc<TimedPermit>) {
        self.state
            .leases
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(Arc::downgrade(lease));
    }
}

impl Drop for EpochTicker {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct RuntimeInner {
    engine: Engine,
    permits: Arc<Semaphore>,
    ticker: EpochTicker,
    policy: ProcessingBudgets,
}

/// Host-owned process runtime. All contexts for a host must be created from one
/// instance so they share one engine, epoch ticker, and aggregate admission cap.
#[derive(Clone)]
pub struct ProcessingRuntime {
    inner: Arc<RuntimeInner>,
}

impl ProcessingRuntime {
    pub fn new(requested_policy: ProcessingBudgets) -> Result<Self, InitializationError> {
        Self::new_with_ceiling(requested_policy, &ProcessingBudgets::default())
    }

    /// Creates the one host-owned runtime with the calibrated PDF ceiling.
    /// All PDF and future transform contexts in that host must derive from
    /// this shared runtime so aggregate admission remains process-wide.
    pub fn new_pdf_host() -> Result<Self, InitializationError> {
        let policy = pdf_extract_processing_budgets();
        Self::new_with_ceiling(policy.clone(), &policy)
    }

    /// Compiles the checked PDF module into this host's shared runtime.
    pub fn create_pdf_extract_context(&self) -> Result<ProcessingContext, InitializationError> {
        self.create_context(&PDF_EXTRACT_MODULE, pdf_extract_processing_budgets())
    }

    fn new_with_ceiling(
        requested_policy: ProcessingBudgets,
        ceiling: &ProcessingBudgets,
    ) -> Result<Self, InitializationError> {
        let policy = ProcessingBudgets::bounded_by(requested_policy, ceiling)?;
        let engine = configured_engine()?;
        // Construction does not succeed until the ticker thread has incremented
        // the engine epoch and published a real heartbeat.
        let ticker = EpochTicker::spawn(
            engine.clone(),
            policy.epoch_interval,
            policy.stale_heartbeat,
        )?;
        Ok(Self {
            inner: Arc::new(RuntimeInner {
                engine,
                permits: Arc::new(Semaphore::new(policy.concurrency)),
                ticker,
                policy,
            }),
        })
    }

    pub fn create_context(
        &self,
        descriptor: &'static ModuleDescriptor,
        requested_budgets: ProcessingBudgets,
    ) -> Result<ProcessingContext, InitializationError> {
        let mut budgets = ProcessingBudgets::bounded_by(requested_budgets, &self.inner.policy)?;
        // These controls are owned by the shared host runtime, not individual
        // contexts; provenance must report the values that actually execute.
        budgets.concurrency = self.inner.policy.concurrency;
        budgets.epoch_interval = self.inner.policy.epoch_interval;
        budgets.stale_heartbeat = self.inner.policy.stale_heartbeat;
        admit_descriptor(descriptor, &budgets)?;
        let module =
            Module::from_binary(&self.inner.engine, descriptor.module_bytes).map_err(|error| {
                InitializationError::new(PublicError::InvalidModule, format!("{error:#}"))
            })?;
        admit_compiled_module(&module, &budgets)?;
        Ok(ProcessingContext {
            inner: Arc::new(Inner {
                runtime: Arc::clone(&self.inner),
                module,
                descriptor,
                budgets,
                observer: Mutex::new(None),
            }),
        })
    }
}

struct Inner {
    runtime: Arc<RuntimeInner>,
    module: Module,
    descriptor: &'static ModuleDescriptor,
    budgets: ProcessingBudgets,
    observer: Mutex<Option<Arc<BoundedRecordObserver>>>,
}

#[derive(Clone)]
pub struct ProcessingContext {
    inner: Arc<Inner>,
}

impl fmt::Debug for ProcessingContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcessingContext")
            .field("transform_id", &self.inner.descriptor.transform_id)
            .finish_non_exhaustive()
    }
}

impl ProcessingContext {
    pub fn concurrency_limit(&self) -> usize {
        self.inner.budgets.concurrency
    }

    pub fn try_begin(&self, transform_id: &str) -> Result<ProcessingLease, BeginError> {
        if transform_id != self.inner.descriptor.transform_id {
            return Err(BeginError::InvalidTransform);
        }
        if !self
            .inner
            .runtime
            .ticker
            .healthy(self.inner.runtime.policy.stale_heartbeat)
        {
            return Err(BeginError::RuntimeUnavailable);
        }
        let permit = Arc::clone(&self.inner.runtime.permits)
            .try_acquire_owned()
            .map_err(|error| match error {
                TryAcquireError::NoPermits => BeginError::Busy,
                TryAcquireError::Closed => BeginError::RuntimeUnavailable,
            })?;
        let deadline = Instant::now() + self.inner.budgets.wall_time;
        let timed_permit = Arc::new(TimedPermit {
            permit: Mutex::new(Some(permit)),
            cancellation: CancellationToken::new(),
            deadline,
            started: AtomicBool::new(false),
        });
        self.inner.runtime.ticker.register(&timed_permit);
        Ok(ProcessingLease {
            inner: Arc::clone(&self.inner),
            timed_permit,
        })
    }

    /// Installs a bounded metadata-only observer intended for integration tests.
    /// Records contain hashes and termination metadata, never input/output bytes.
    #[doc(hidden)]
    pub fn install_test_observer(&self, observer: Arc<BoundedRecordObserver>) {
        *self.inner.observer.lock().expect("observer lock") = Some(observer);
    }

    #[cfg(test)]
    fn pause_ticker(&self) {
        self.inner
            .runtime
            .ticker
            .state
            .paused
            .store(true, Ordering::Release);
    }
}

pub struct ProcessingLease {
    inner: Arc<Inner>,
    timed_permit: Arc<TimedPermit>,
}

impl ProcessingLease {
    /// Mark this lease active before a host adapter begins pre-run materialization.
    /// Direct `ProcessingContext` callers intentionally do not use this seam, so
    /// their unstarted leases retain the existing deadline-expiry behavior.
    pub(crate) fn activate_for_adapter(&self) -> Result<(), BeginError> {
        if self.timed_permit.start(Instant::now()) {
            Ok(())
        } else {
            Err(BeginError::RuntimeUnavailable)
        }
    }

    /// Effective input ceiling owned by this admitted lease.
    pub fn max_input_bytes(&self) -> u64 {
        u64::try_from(self.inner.budgets.input_bytes)
            .expect("supported native usize values fit the bounded-read contract")
    }

    /// Absolute admission deadline. It includes any time spent before `run`.
    pub fn deadline(&self) -> Instant {
        self.timed_permit.deadline
    }

    /// A cloneable, safe cancellation handle for queue/reader owners.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.timed_permit.cancellation.clone()
    }

    pub fn cancel(&self) {
        self.timed_permit.cancellation.cancel();
    }

    pub async fn run(self, input: Vec<u8>) -> Result<ProcessingOutcome, ProcessingFailure> {
        self.run_cancellable(input, CancellationToken::new()).await
    }

    pub async fn run_cancellable(
        self,
        input: Vec<u8>,
        cancellation: CancellationToken,
    ) -> Result<ProcessingOutcome, ProcessingFailure> {
        let input_bytes = u64::try_from(input.len()).unwrap_or(u64::MAX);
        // The size check deliberately precedes both hashing and task creation.
        if input.len() > self.inner.budgets.input_bytes {
            return Err(self.failure(
                PublicError::InputTooLarge,
                None,
                None,
                self.pre_execution_observations(input_bytes),
                "input exceeded configured ceiling",
            ));
        }
        let input_sha256 = sha256(&input);
        let now = Instant::now();
        if now >= self.deadline() {
            return Err(self.failure(
                PublicError::WallTimeExceeded,
                Some(input_sha256),
                None,
                self.pre_execution_observations(input_bytes),
                "lease deadline elapsed before execution",
            ));
        }
        if cancellation.is_cancelled() || self.timed_permit.cancellation.is_cancelled() {
            return Err(self.failure(
                PublicError::Cancelled,
                Some(input_sha256),
                None,
                self.pre_execution_observations(input_bytes),
                "processing invocation was cancelled before execution",
            ));
        }
        if !self.timed_permit.start(now) {
            return Err(self.failure(
                PublicError::WallTimeExceeded,
                Some(input_sha256),
                None,
                self.pre_execution_observations(input_bytes),
                "lease admission expired before execution",
            ));
        }
        let lease_cancellation = self.timed_permit.cancellation.clone();
        let mut guard = CancelOnDrop::new(lease_cancellation.clone());
        let failure_inner = Arc::clone(&self.inner);
        let deadline = self.deadline();
        // Duration begins only when the admitted guest task is dispatched.
        let execution_started = Instant::now();
        let task = tokio::spawn(async move {
            self.execute(
                input,
                input_sha256,
                cancellation,
                lease_cancellation,
                deadline,
                execution_started,
                input_bytes,
            )
            .await
        });
        let result = match task.await {
            Ok(result) => result,
            Err(error) => {
                return Err(joined_task_failure(
                    &failure_inner,
                    Some(input_sha256),
                    execution_started,
                    input_bytes,
                    error.to_string(),
                ));
            }
        };
        guard.disarm();
        result
    }

    async fn execute(
        self,
        input: Vec<u8>,
        input_sha256: [u8; 32],
        cancellation: CancellationToken,
        lease_cancellation: CancellationToken,
        deadline: Instant,
        execution_started: Instant,
        input_bytes: u64,
    ) -> Result<ProcessingOutcome, ProcessingFailure> {
        let post_cancellation = cancellation.clone();
        let post_lease_cancellation = lease_cancellation.clone();
        let raw = execute_guest(
            &self.inner,
            &input,
            cancellation,
            lease_cancellation,
            deadline,
        )
        .await;
        match raw {
            Ok(execution) if Instant::now() >= deadline => Err(self.failure(
                PublicError::WallTimeExceeded,
                Some(input_sha256),
                None,
                self.observations(
                    execution_started,
                    input_bytes,
                    u64::try_from(execution.output.len()).unwrap_or(u64::MAX),
                    execution.fuel_consumed,
                    execution.peak_requested_memory_bytes,
                ),
                "processing invocation deadline elapsed before completion",
            )),
            Ok(execution)
                if post_cancellation.is_cancelled() || post_lease_cancellation.is_cancelled() =>
            {
                Err(self.failure(
                    PublicError::Cancelled,
                    Some(input_sha256),
                    None,
                    self.observations(
                        execution_started,
                        input_bytes,
                        u64::try_from(execution.output.len()).unwrap_or(u64::MAX),
                        execution.fuel_consumed,
                        execution.peak_requested_memory_bytes,
                    ),
                    "processing invocation was cancelled before completion",
                ))
            }
            Ok(execution) => {
                let output_bytes = u64::try_from(execution.output.len()).unwrap_or(u64::MAX);
                let output = execution.output;
                let output_sha256 = sha256(&output);
                let observations = self.observations(
                    execution_started,
                    input_bytes,
                    output_bytes,
                    execution.fuel_consumed,
                    execution.peak_requested_memory_bytes,
                );
                let record = self.record(
                    Some(input_sha256),
                    Some(output_sha256),
                    TerminationClass::Success,
                    observations,
                );
                self.observe(&record);
                Ok(ProcessingOutcome { output, record })
            }
            Err(error) => {
                let public_error = error.failure.public_error;
                Err(self.failure(
                    public_error,
                    Some(input_sha256),
                    None,
                    self.observations(
                        execution_started,
                        input_bytes,
                        0,
                        error.fuel_consumed,
                        error.peak_requested_memory_bytes,
                    ),
                    error.failure.private_source,
                ))
            }
        }
    }

    fn pre_execution_observations(&self, input_bytes: u64) -> TransformObservations {
        TransformObservations {
            duration: Duration::ZERO,
            input_bytes,
            output_bytes: 0,
            fuel_consumed: None,
            peak_requested_memory_bytes: None,
        }
    }

    /// Guest execution duration from admitted task dispatch until terminal store/output
    /// observations are sampled. Failures rejected before dispatch report zero duration.
    fn observations(
        &self,
        started: Instant,
        input_bytes: u64,
        output_bytes: u64,
        fuel_consumed: Option<u64>,
        peak_requested_memory_bytes: Option<u64>,
    ) -> TransformObservations {
        TransformObservations {
            duration: started.elapsed(),
            input_bytes,
            output_bytes,
            fuel_consumed,
            peak_requested_memory_bytes,
        }
    }

    fn record(
        &self,
        input_sha256: Option<[u8; 32]>,
        output_sha256: Option<[u8; 32]>,
        termination_class: TerminationClass,
        observations: TransformObservations,
    ) -> TransformRecord {
        TransformRecord {
            transform_id: self.inner.descriptor.transform_id,
            module_sha256: self.inner.descriptor.module_sha256,
            abi_version: self.inner.descriptor.abi_version,
            runtime_version: RUNTIME_VERSION,
            effective_budgets: self.inner.budgets.clone(),
            input_sha256,
            output_sha256,
            termination_class,
            observations,
        }
    }

    fn failure(
        &self,
        public_error: PublicError,
        input_sha256: Option<[u8; 32]>,
        output_sha256: Option<[u8; 32]>,
        observations: TransformObservations,
        private_source: impl Into<String>,
    ) -> ProcessingFailure {
        let record = self.record(
            input_sha256,
            output_sha256,
            TerminationClass::Failure(public_error),
            observations,
        );
        self.observe(&record);
        ProcessingFailure {
            public_error,
            record,
            private_source: Some(private_source.into()),
        }
    }

    fn observe(&self, record: &TransformRecord) {
        observe_record(&self.inner, record);
    }
}

fn observe_record(inner: &Inner, record: &TransformRecord) {
    let termination = match record.termination_class {
        TerminationClass::Success => "success",
        TerminationClass::Failure(error) => error.as_str(),
    };
    let transform = record.transform_id;
    metrics::histogram!(
        "lattice.transform.duration_ms",
        "backend" => "native",
        "transform" => transform,
        "termination" => termination
    )
    .record(record.observations.duration.as_secs_f64() * 1_000.0);
    metrics::histogram!(
        "lattice.transform.input_bytes",
        "backend" => "native",
        "transform" => transform,
        "termination" => termination
    )
    .record(record.observations.input_bytes as f64);
    metrics::histogram!(
        "lattice.transform.output_bytes",
        "backend" => "native",
        "transform" => transform,
        "termination" => termination
    )
    .record(record.observations.output_bytes as f64);
    if let Some(fuel_consumed) = record.observations.fuel_consumed {
        metrics::histogram!(
            "lattice.transform.fuel_consumed",
            "backend" => "native",
            "transform" => transform,
            "termination" => termination
        )
        .record(fuel_consumed as f64);
    }
    if let Some(peak_requested_memory_bytes) = record.observations.peak_requested_memory_bytes {
        metrics::histogram!(
            "lattice.transform.peak_requested_memory_bytes",
            "backend" => "native",
            "transform" => transform,
            "termination" => termination
        )
        .record(peak_requested_memory_bytes as f64);
    }
    metrics::gauge!(
        "lattice.transform.fuel_ceiling",
        "backend" => "native",
        "transform" => transform
    )
    .set(record.effective_budgets.fuel as f64);
    metrics::gauge!(
        "lattice.transform.memory_ceiling_bytes",
        "backend" => "native",
        "transform" => transform
    )
    .set(record.effective_budgets.memory_bytes as f64);
    metrics::counter!(
        "lattice.transform.terminations_total",
        "backend" => "native",
        "transform" => transform,
        "termination" => termination
    )
    .increment(1);

    if let Some(observer) = inner.observer.lock().expect("observer lock").clone() {
        observer.push(record.clone());
    }
}

struct CancelOnDrop {
    cancellation: CancellationToken,
    armed: bool,
}

impl CancelOnDrop {
    fn new(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.cancel();
        }
    }
}

struct RawFailure {
    public_error: PublicError,
    private_source: String,
}

struct GuestFailure {
    failure: RawFailure,
    fuel_consumed: Option<u64>,
    peak_requested_memory_bytes: Option<u64>,
}

struct GuestExecution {
    output: Vec<u8>,
    fuel_consumed: Option<u64>,
    peak_requested_memory_bytes: Option<u64>,
}

async fn execute_guest(
    inner: &Inner,
    input: &[u8],
    cancellation: CancellationToken,
    lease_cancellation: CancellationToken,
    deadline: Instant,
) -> Result<GuestExecution, GuestFailure> {
    let limits = StoreLimitsBuilder::new()
        .memory_size(inner.budgets.memory_bytes)
        .memories(inner.budgets.memories)
        .table_elements(inner.budgets.table_elements)
        .tables(inner.budgets.tables)
        .instances(inner.budgets.instances)
        .trap_on_grow_failure(true)
        .build();
    let mut store = Store::new(
        &inner.runtime.engine,
        StoreState {
            limits: TrackingLimits {
                limits,
                denial: None,
                peak_requested_memory_bytes: 0,
            },
        },
    );
    store.limiter(|state| &mut state.limits);
    if let Err(error) = store.set_fuel(inner.budgets.fuel) {
        return Err(guest_failure(
            inner,
            &store,
            raw(PublicError::GuestFailed, error),
        ));
    }
    if let Err(error) = store.fuel_async_yield_interval(Some(inner.budgets.fuel_yield_interval)) {
        return Err(guest_failure(
            inner,
            &store,
            raw(PublicError::GuestFailed, error),
        ));
    }
    store.set_epoch_deadline(1);
    let callback_cancellation = cancellation.clone();
    let callback_lease_cancellation = lease_cancellation.clone();
    store.epoch_deadline_callback(move |_store| {
        if callback_cancellation.is_cancelled() || callback_lease_cancellation.is_cancelled() {
            return Err(anyhow!("processing invocation cancelled"));
        }
        if Instant::now() >= deadline {
            return Err(anyhow!("processing invocation deadline elapsed"));
        }
        Ok(UpdateDeadline::Yield(1))
    });

    // A fresh Store is fully metered before instantiation; modules with a start
    // section were already rejected during byte admission.
    let linker = Linker::new(&inner.runtime.engine);
    let instance = match linker.instantiate_async(&mut store, &inner.module).await {
        Ok(instance) => instance,
        Err(error) => {
            let failure =
                classify_store_error(error, &store, &cancellation, &lease_cancellation, deadline);
            return Err(guest_failure(inner, &store, failure));
        }
    };
    if let Some(memory) = instance.get_memory(&mut store, "memory") {
        let current = memory.data_size(&store);
        store.data_mut().limits.peak_requested_memory_bytes =
            store.data().limits.peak_requested_memory_bytes.max(current);
    }
    let result = invoke_abi(
        inner,
        &mut store,
        instance,
        input,
        cancellation,
        lease_cancellation,
        deadline,
    )
    .await;
    let fuel_consumed = observed_fuel(inner, &store);
    let peak_requested_memory_bytes =
        u64::try_from(store.data().limits.peak_requested_memory_bytes).ok();
    match result {
        Ok(output) => Ok(GuestExecution {
            output,
            fuel_consumed,
            peak_requested_memory_bytes,
        }),
        Err(failure) => Err(GuestFailure {
            failure,
            fuel_consumed,
            peak_requested_memory_bytes,
        }),
    }
}

fn observed_fuel(inner: &Inner, store: &Store<StoreState>) -> Option<u64> {
    store
        .get_fuel()
        .ok()
        .map(|remaining| inner.budgets.fuel.saturating_sub(remaining))
}

fn guest_failure(inner: &Inner, store: &Store<StoreState>, failure: RawFailure) -> GuestFailure {
    GuestFailure {
        failure,
        fuel_consumed: observed_fuel(inner, store),
        peak_requested_memory_bytes: u64::try_from(store.data().limits.peak_requested_memory_bytes)
            .ok(),
    }
}

async fn invoke_abi(
    inner: &Inner,
    store: &mut Store<StoreState>,
    instance: Instance,
    input: &[u8],
    cancellation: CancellationToken,
    lease_cancellation: CancellationToken,
    deadline: Instant,
) -> Result<Vec<u8>, RawFailure> {
    let memory = instance
        .get_memory(&mut *store, "memory")
        .ok_or_else(|| RawFailure {
            public_error: PublicError::InvalidAbi,
            private_source: "admitted memory export disappeared".to_owned(),
        })?;
    let alloc = instance
        .get_typed_func::<i32, i32>(&mut *store, "lf_alloc")
        .map_err(|error| raw(PublicError::InvalidAbi, error))?;
    let transform = instance
        .get_typed_func::<(i32, i32), i32>(&mut *store, "lf_transform")
        .map_err(|error| raw(PublicError::InvalidAbi, error))?;
    let output_ptr = instance
        .get_typed_func::<(), i32>(&mut *store, "lf_output_ptr")
        .map_err(|error| raw(PublicError::InvalidAbi, error))?;
    let output_len = instance
        .get_typed_func::<(), i32>(&mut *store, "lf_output_len")
        .map_err(|error| raw(PublicError::InvalidAbi, error))?;

    let input_len = i32::try_from(input.len()).map_err(|_| RawFailure {
        public_error: PublicError::InputTooLarge,
        private_source: "input length does not fit ABI i32".to_owned(),
    })?;
    let input_ptr = match alloc.call_async(&mut *store, input_len).await {
        Ok(pointer) => pointer,
        Err(error) => {
            return Err(classify_store_error(
                error,
                &*store,
                &cancellation,
                &lease_cancellation,
                deadline,
            ));
        }
    };
    match input_ptr {
        -1 => {
            return Err(RawFailure {
                public_error: PublicError::MemoryExhausted,
                private_source: "guest allocator reported exhaustion".to_owned(),
            });
        }
        value if value < -1 => {
            return Err(RawFailure {
                public_error: PublicError::InvalidAbi,
                private_source: "guest allocator returned an invalid negative pointer".to_owned(),
            });
        }
        _ => {}
    }
    let input_offset = nonnegative_offset(input_ptr, "input pointer")?;
    checked_memory_range(&memory, &mut *store, input_offset, input.len(), "input")?;
    memory
        .write(&mut *store, input_offset, input)
        .map_err(|error| raw(PublicError::InvalidOutput, error))?;

    let status = match transform
        .call_async(&mut *store, (input_ptr, input_len))
        .await
    {
        Ok(status) => status,
        Err(error) => {
            return Err(classify_store_error(
                error,
                &*store,
                &cancellation,
                &lease_cancellation,
                deadline,
            ));
        }
    };
    match status {
        0 => {}
        GUEST_ERROR_UNSUPPORTED_DOCUMENT => {
            return Err(RawFailure {
                public_error: PublicError::UnsupportedDocument,
                private_source: "guest returned supported failure code".to_owned(),
            });
        }
        _ => {
            return Err(RawFailure {
                public_error: PublicError::InvalidAbi,
                private_source: "guest returned invalid status code".to_owned(),
            });
        }
    }

    let ptr = match output_ptr.call_async(&mut *store, ()).await {
        Ok(pointer) => pointer,
        Err(error) => {
            return Err(classify_store_error(
                error,
                &*store,
                &cancellation,
                &lease_cancellation,
                deadline,
            ));
        }
    };
    let len = match output_len.call_async(&mut *store, ()).await {
        Ok(length) => length,
        Err(error) => {
            return Err(classify_store_error(
                error,
                &*store,
                &cancellation,
                &lease_cancellation,
                deadline,
            ));
        }
    };
    let offset = nonnegative_offset(ptr, "output pointer")?;
    let len = nonnegative_offset(len, "output length")?;
    if len > inner.budgets.output_bytes {
        return Err(RawFailure {
            public_error: PublicError::OutputTooLarge,
            private_source: "guest output exceeded configured ceiling".to_owned(),
        });
    }
    let range = checked_memory_range(&memory, &mut *store, offset, len, "output")?;
    Ok(memory.data(&*store)[range].to_vec())
}

fn nonnegative_offset(value: i32, field: &str) -> Result<usize, RawFailure> {
    usize::try_from(value).map_err(|_| RawFailure {
        public_error: PublicError::InvalidOutput,
        private_source: format!("guest returned negative {field}"),
    })
}

fn checked_memory_range(
    memory: &Memory,
    store: &mut Store<StoreState>,
    offset: usize,
    len: usize,
    field: &str,
) -> Result<std::ops::Range<usize>, RawFailure> {
    let end = offset.checked_add(len).ok_or_else(|| RawFailure {
        public_error: PublicError::InvalidOutput,
        private_source: format!("guest {field} range overflowed"),
    })?;
    if end > memory.data_size(store) {
        return Err(RawFailure {
            public_error: PublicError::InvalidOutput,
            private_source: format!("guest {field} range was outside memory"),
        });
    }
    Ok(offset..end)
}

fn classify_store_error(
    error: anyhow::Error,
    store: &Store<StoreState>,
    cancellation: &CancellationToken,
    lease_cancellation: &CancellationToken,
    deadline: Instant,
) -> RawFailure {
    let public_error = if Instant::now() >= deadline {
        PublicError::WallTimeExceeded
    } else if cancellation.is_cancelled() || lease_cancellation.is_cancelled() {
        PublicError::Cancelled
    } else if store.data().limits.denial == Some(LimitDenial::Memory) {
        PublicError::MemoryExhausted
    } else if error.downcast_ref::<Trap>() == Some(&Trap::OutOfFuel) {
        PublicError::FuelExhausted
    } else {
        PublicError::GuestFailed
    };
    RawFailure {
        public_error,
        private_source: format!("{error:#}"),
    }
}

fn raw(error: PublicError, source: impl fmt::Display) -> RawFailure {
    RawFailure {
        public_error: error,
        private_source: source.to_string(),
    }
}

fn joined_task_failure(
    inner: &Inner,
    input_sha256: Option<[u8; 32]>,
    started: Instant,
    input_bytes: u64,
    source: String,
) -> ProcessingFailure {
    let record = TransformRecord {
        transform_id: inner.descriptor.transform_id,
        module_sha256: inner.descriptor.module_sha256,
        abi_version: inner.descriptor.abi_version,
        runtime_version: RUNTIME_VERSION,
        effective_budgets: inner.budgets.clone(),
        input_sha256,
        output_sha256: None,
        termination_class: TerminationClass::Failure(PublicError::GuestFailed),
        observations: TransformObservations {
            duration: started.elapsed(),
            input_bytes,
            output_bytes: 0,
            fuel_consumed: None,
            peak_requested_memory_bytes: None,
        },
    };
    observe_record(inner, &record);
    ProcessingFailure {
        public_error: PublicError::GuestFailed,
        record,
        private_source: Some(source),
    }
}

fn configured_engine() -> Result<Engine, InitializationError> {
    let mut config = Config::new();
    config
        .consume_fuel(ENGINE_CONTROLS.consume_fuel)
        .epoch_interruption(ENGINE_CONTROLS.epoch_interruption)
        .async_support(ENGINE_CONTROLS.async_support)
        .max_wasm_stack(ENGINE_CONTROLS.max_wasm_stack)
        .async_stack_size(ENGINE_CONTROLS.async_stack_size)
        .cranelift_nan_canonicalization(ENGINE_CONTROLS.nan_canonicalization)
        .wasm_relaxed_simd(ENGINE_CONTROLS.relaxed_simd)
        .wasm_threads(ENGINE_CONTROLS.threads)
        .wasm_memory64(ENGINE_CONTROLS.memory64)
        .wasm_multi_memory(ENGINE_CONTROLS.multi_memory)
        .wasm_simd(ENGINE_CONTROLS.simd)
        .wasm_reference_types(ENGINE_CONTROLS.reference_types)
        .wasm_function_references(ENGINE_CONTROLS.function_references)
        .wasm_bulk_memory(ENGINE_CONTROLS.bulk_memory)
        .wasm_multi_value(ENGINE_CONTROLS.multi_value)
        .wasm_tail_call(ENGINE_CONTROLS.tail_call);
    Engine::new(&config)
        .map_err(|error| InitializationError::new(PublicError::InvalidModule, error.to_string()))
}

fn admit_descriptor(
    descriptor: &ModuleDescriptor,
    budgets: &ProcessingBudgets,
) -> Result<(), InitializationError> {
    if descriptor.abi_version != ABI_VERSION {
        return Err(InitializationError::new(
            PublicError::InvalidAbi,
            "descriptor ABI version mismatch",
        ));
    }
    if descriptor.transform_id.is_empty() {
        return Err(InitializationError::new(
            PublicError::InvalidModule,
            "empty transform identifier",
        ));
    }
    if descriptor.module_bytes.len() > budgets.module_bytes {
        return Err(InitializationError::new(
            PublicError::InvalidModule,
            "module exceeded configured ceiling",
        ));
    }
    if sha256(descriptor.module_bytes) != descriptor.module_sha256 {
        return Err(InitializationError::new(
            PublicError::InvalidModule,
            "allowlist hash mismatch",
        ));
    }
    reject_start_section(descriptor.module_bytes)
}

fn reject_start_section(bytes: &[u8]) -> Result<(), InitializationError> {
    for payload in Parser::new(0).parse_all(bytes) {
        match payload.map_err(|error| {
            InitializationError::new(PublicError::InvalidModule, error.to_string())
        })? {
            Payload::Version { encoding, .. } if encoding != Encoding::Module => {
                return Err(InitializationError::new(
                    PublicError::InvalidModule,
                    "component modules are not admitted",
                ));
            }
            Payload::StartSection { .. } => {
                return Err(InitializationError::new(
                    PublicError::InvalidAbi,
                    "start section is forbidden",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn admit_compiled_module(
    module: &Module,
    budgets: &ProcessingBudgets,
) -> Result<(), InitializationError> {
    if module.imports().next().is_some() {
        return Err(InitializationError::new(
            PublicError::InvalidAbi,
            "v1 modules must have zero imports",
        ));
    }

    let mut required = [false; 5];
    for export in module.exports() {
        if matches!(export.name(), "__data_end" | "__heap_base") {
            match export.ty() {
                ExternType::Global(global)
                    if *global.content() == ValType::I32
                        && global.mutability() == Mutability::Const =>
                {
                    continue;
                }
                _ => {
                    return Err(InitializationError::new(
                        PublicError::InvalidAbi,
                        "toolchain global exceeds v1 profile",
                    ));
                }
            }
        }
        let slot = match (export.name(), export.ty()) {
            ("memory", ExternType::Memory(memory)) => {
                if memory.is_64()
                    || memory.is_shared()
                    || memory.maximum().is_none()
                    || memory.minimum().saturating_mul(WASM_PAGE_BYTES)
                        > budgets.memory_bytes as u64
                    || memory
                        .maximum()
                        .unwrap_or(u64::MAX)
                        .saturating_mul(WASM_PAGE_BYTES)
                        > budgets.memory_bytes as u64
                {
                    return Err(InitializationError::new(
                        PublicError::InvalidAbi,
                        "memory export exceeds v1 profile",
                    ));
                }
                0
            }
            ("lf_alloc", ExternType::Func(function))
                if exact_function(&function, &[ValType::I32], &[ValType::I32]) =>
            {
                1
            }
            ("lf_transform", ExternType::Func(function))
                if exact_function(&function, &[ValType::I32, ValType::I32], &[ValType::I32]) =>
            {
                2
            }
            ("lf_output_ptr", ExternType::Func(function))
                if exact_function(&function, &[], &[ValType::I32]) =>
            {
                3
            }
            ("lf_output_len", ExternType::Func(function))
                if exact_function(&function, &[], &[ValType::I32]) =>
            {
                4
            }
            _ => {
                return Err(InitializationError::new(
                    PublicError::InvalidAbi,
                    "unexpected or incorrectly typed export",
                ));
            }
        };
        if std::mem::replace(&mut required[slot], true) {
            return Err(InitializationError::new(
                PublicError::InvalidAbi,
                "duplicate required export",
            ));
        }
    }
    if required.into_iter().any(|present| !present) {
        return Err(InitializationError::new(
            PublicError::InvalidAbi,
            "missing required export",
        ));
    }
    Ok(())
}

fn exact_function(function: &wasmtime::FuncType, params: &[ValType], results: &[ValType]) -> bool {
    function.params().eq(params.iter().cloned()) && function.results().eq(results.iter().cloned())
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Bounded metadata-only observer seam for P2/P3 integration tests.
#[doc(hidden)]
pub struct BoundedRecordObserver {
    capacity: usize,
    records: Mutex<VecDeque<TransformRecord>>,
}

impl BoundedRecordObserver {
    pub const MAX_CAPACITY: usize = 1024;

    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.min(Self::MAX_CAPACITY);
        Self {
            capacity,
            records: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    fn push(&self, record: TransformRecord) {
        let mut records = self.records.lock().expect("observer records lock");
        if self.capacity == 0 {
            return;
        }
        if records.len() == self.capacity {
            records.pop_front();
        }
        records.push_back(record);
    }

    pub fn records(&self) -> Vec<TransformRecord> {
        self.records
            .lock()
            .expect("observer records lock")
            .iter()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests;
