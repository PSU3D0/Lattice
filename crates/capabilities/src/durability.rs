use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::{Arc, Mutex};
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use std::time::SystemTime;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::Notify;

use crate::Capability;
use crate::blob::BlobStore;
use dag_core::{DurabilityMode, FlowId};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlowFrontier {
    pub completed: Vec<FrontierEntry>,
    pub pending: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FrontierEntry {
    pub node_alias: String,
    pub output_port: String,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SerializedState {
    pub data: JsonValue,
    #[serde(default)]
    pub blobs: Vec<BlobRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobRef {
    pub ref_id: String,
    pub size_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct IdempotencyState {
    pub consumed: HashMap<String, Vec<String>>,
    pub pending: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckpointRecord {
    pub checkpoint_id: String,
    pub flow_id: FlowId,
    pub flow_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bundle_id: Option<String>,
    pub run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    pub frontier: FlowFrontier,
    pub state: SerializedState,
    pub idempotency: IdempotencyState,
    pub created_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume_after_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointHandle {
    pub checkpoint_id: String,
    pub flow_id: FlowId,
    pub run_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lease {
    pub lease_id: String,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct CheckpointFilter {
    pub flow_id: Option<FlowId>,
    pub run_id: Option<String>,
    pub status: Option<CheckpointStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointStatus {
    Active,
    Completed,
    Expired,
}

#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    #[error("checkpoint not found")]
    NotFound,
    #[error("lease conflict: checkpoint is locked by another consumer")]
    LeaseConflict,
    #[error("lease expired")]
    LeaseExpired,
    #[error("storage error: {0}")]
    Storage(String),
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait CheckpointStore: Capability {
    async fn put(&self, record: CheckpointRecord) -> Result<CheckpointHandle, CheckpointError>;
    async fn get(&self, handle: &CheckpointHandle) -> Result<CheckpointRecord, CheckpointError>;
    async fn ack(&self, handle: &CheckpointHandle) -> Result<(), CheckpointError>;
    async fn lease(
        &self,
        handle: &CheckpointHandle,
        ttl: Duration,
    ) -> Result<Lease, CheckpointError>;
    async fn release_lease(&self, lease: Lease) -> Result<(), CheckpointError>;
    async fn list(
        &self,
        filter: CheckpointFilter,
    ) -> Result<Vec<CheckpointHandle>, CheckpointError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleId(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleStatus {
    Pending { fires_at_ms: u64 },
    Fired { fired_at_ms: u64 },
    Cancelled,
    Expired,
}

#[derive(Debug, thiserror::Error)]
pub enum ScheduleError {
    #[error("schedule not found")]
    NotFound,
    #[error("invalid delay: {0}")]
    InvalidDelay(String),
    #[error("scheduler unavailable: {0}")]
    Unavailable(String),
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait ResumeScheduler: Capability {
    async fn schedule_at(
        &self,
        handle: CheckpointHandle,
        at_ms: u64,
    ) -> Result<ScheduleId, ScheduleError>;
    async fn schedule_after(
        &self,
        handle: CheckpointHandle,
        delay: Duration,
    ) -> Result<ScheduleId, ScheduleError>;
    async fn cancel(&self, schedule_id: ScheduleId) -> Result<(), ScheduleError>;
    async fn status(&self, schedule_id: ScheduleId) -> Result<ScheduleStatus, ScheduleError>;
}

/// A validating, process-local resume scheduler for foreground local development.
///
/// Its registry is intentionally lost when the process exits.
/// [`CheckpointRecord::resume_after_ms`] remains the durable source of truth for
/// due checkpoints across CLI processes.
#[cfg(not(target_arch = "wasm32"))]
pub struct LocalResumeScheduler {
    schedules: Mutex<HashMap<String, LocalSchedule>>,
    clock: Arc<dyn LocalSchedulerClock>,
    changed: Notify,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
struct LocalSchedule {
    handle: CheckpointHandle,
    fires_at_ms: u64,
    state: LocalScheduleState,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy)]
enum LocalScheduleState {
    Pending,
    Fired { fired_at_ms: u64 },
    Cancelled,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedLocalSchedule {
    pub schedule_id: ScheduleId,
    pub handle: CheckpointHandle,
    pub fires_at_ms: u64,
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
trait LocalSchedulerClock: Send + Sync + 'static {
    fn now_ms(&self) -> u64;
    async fn sleep_until_ms(&self, target_ms: u64);
}

#[cfg(not(target_arch = "wasm32"))]
struct SystemSchedulerClock;

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl LocalSchedulerClock for SystemSchedulerClock {
    fn now_ms(&self) -> u64 {
        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        u64::try_from(millis).unwrap_or(u64::MAX)
    }

    async fn sleep_until_ms(&self, target_ms: u64) {
        tokio::time::sleep(Duration::from_millis(
            target_ms.saturating_sub(self.now_ms()),
        ))
        .await;
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl LocalResumeScheduler {
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemSchedulerClock))
    }

    fn with_clock(clock: Arc<dyn LocalSchedulerClock>) -> Self {
        Self {
            schedules: Mutex::new(HashMap::new()),
            clock,
            changed: Notify::new(),
        }
    }

    fn register(
        &self,
        handle: CheckpointHandle,
        fires_at_ms: u64,
    ) -> Result<ScheduleId, ScheduleError> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let mut schedules = self
            .schedules
            .lock()
            .map_err(|_| ScheduleError::Unavailable("local schedule registry poisoned".into()))?;
        loop {
            let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let id = format!("local-resume-{sequence}");
            if !schedules.contains_key(&id) {
                schedules.insert(
                    id.clone(),
                    LocalSchedule {
                        handle,
                        fires_at_ms,
                        state: LocalScheduleState::Pending,
                    },
                );
                self.changed.notify_one();
                return Ok(ScheduleId(id));
            }
        }
    }

    /// Wait until the earliest pending schedule is due and atomically claim it.
    ///
    /// Claiming is the only wall-clock-driven transition to `Fired`; merely
    /// querying a due schedule leaves it pending.
    pub async fn wait_and_claim_next(&self) -> Result<ClaimedLocalSchedule, ScheduleError> {
        loop {
            let changed = self.changed.notified();
            let next = {
                let schedules = self.schedules.lock().map_err(|_| {
                    ScheduleError::Unavailable("local schedule registry poisoned".into())
                })?;
                schedules
                    .iter()
                    .filter(|(_, schedule)| matches!(schedule.state, LocalScheduleState::Pending))
                    .min_by_key(|(id, schedule)| (schedule.fires_at_ms, id.as_str()))
                    .map(|(id, schedule)| (id.clone(), schedule.fires_at_ms))
            };

            let Some((id, fires_at_ms)) = next else {
                changed.await;
                continue;
            };

            let now_ms = self.clock.now_ms();
            if fires_at_ms <= now_ms {
                let mut schedules = self.schedules.lock().map_err(|_| {
                    ScheduleError::Unavailable("local schedule registry poisoned".into())
                })?;
                let Some(schedule) = schedules.get_mut(&id) else {
                    continue;
                };
                if !matches!(schedule.state, LocalScheduleState::Pending)
                    || schedule.fires_at_ms > now_ms
                {
                    continue;
                }
                schedule.state = LocalScheduleState::Fired {
                    fired_at_ms: now_ms,
                };
                return Ok(ClaimedLocalSchedule {
                    schedule_id: ScheduleId(id),
                    handle: schedule.handle.clone(),
                    fires_at_ms: schedule.fires_at_ms,
                });
            }

            tokio::select! {
                _ = self.clock.sleep_until_ms(fires_at_ms) => {}
                _ = changed => {}
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for LocalResumeScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Capability for LocalResumeScheduler {
    fn name(&self) -> &'static str {
        "resume_scheduler.local"
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl ResumeScheduler for LocalResumeScheduler {
    async fn schedule_at(
        &self,
        handle: CheckpointHandle,
        at_ms: u64,
    ) -> Result<ScheduleId, ScheduleError> {
        self.register(handle, at_ms)
    }

    async fn schedule_after(
        &self,
        handle: CheckpointHandle,
        delay: Duration,
    ) -> Result<ScheduleId, ScheduleError> {
        if delay.is_zero() {
            return Err(ScheduleError::InvalidDelay(
                "delay must be nonzero".to_string(),
            ));
        }
        let delay_ms = u64::try_from(delay.as_millis()).map_err(|_| {
            ScheduleError::InvalidDelay("delay exceeds the supported millisecond range".into())
        })?;
        let fires_at_ms = self.clock.now_ms().checked_add(delay_ms).ok_or_else(|| {
            ScheduleError::InvalidDelay("target epoch exceeds the supported range".into())
        })?;
        self.register(handle, fires_at_ms)
    }

    async fn cancel(&self, schedule_id: ScheduleId) -> Result<(), ScheduleError> {
        let mut schedules = self
            .schedules
            .lock()
            .map_err(|_| ScheduleError::Unavailable("local schedule registry poisoned".into()))?;
        let schedule = schedules
            .get_mut(&schedule_id.0)
            .ok_or(ScheduleError::NotFound)?;
        match schedule.state {
            LocalScheduleState::Pending => {
                schedule.state = LocalScheduleState::Cancelled;
                self.changed.notify_one();
                Ok(())
            }
            LocalScheduleState::Cancelled => Ok(()),
            LocalScheduleState::Fired { .. } => Err(ScheduleError::Unavailable(
                "schedule has already fired".to_string(),
            )),
        }
    }

    async fn status(&self, schedule_id: ScheduleId) -> Result<ScheduleStatus, ScheduleError> {
        let schedules = self
            .schedules
            .lock()
            .map_err(|_| ScheduleError::Unavailable("local schedule registry poisoned".into()))?;
        let schedule = schedules
            .get(&schedule_id.0)
            .ok_or(ScheduleError::NotFound)?;
        Ok(match schedule.state {
            LocalScheduleState::Pending => ScheduleStatus::Pending {
                fires_at_ms: schedule.fires_at_ms,
            },
            LocalScheduleState::Fired { fired_at_ms } => ScheduleStatus::Fired { fired_at_ms },
            LocalScheduleState::Cancelled => ScheduleStatus::Cancelled,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeToken(pub String);

#[derive(Debug, Clone)]
pub struct TokenConfig {
    pub ttl: Option<Duration>,
    pub single_use: bool,
    pub metadata: Option<JsonValue>,
}

#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error("token not found or expired")]
    NotFound,
    #[error("token already used")]
    AlreadyUsed,
    #[error("token generation failed: {0}")]
    Generation(String),
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait ResumeSignalSource: Capability {
    async fn create_token(
        &self,
        handle: &CheckpointHandle,
        config: TokenConfig,
    ) -> Result<ResumeToken, TokenError>;
    async fn resolve_token(&self, token: &ResumeToken) -> Result<CheckpointHandle, TokenError>;
    async fn revoke_token(&self, token: &ResumeToken) -> Result<(), TokenError>;
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait CheckpointBlobStore: BlobStore {
    async fn put_checkpoint_blob(
        &self,
        checkpoint_id: &str,
        field: &str,
        data: &[u8],
    ) -> Result<BlobRef, crate::blob::BlobError>;
    async fn delete_checkpoint_blobs(
        &self,
        checkpoint_id: &str,
    ) -> Result<(), crate::blob::BlobError>;
}

pub trait HostDurability {
    fn checkpoint_store(&self) -> Option<&dyn CheckpointStore>;
    fn resume_scheduler(&self) -> Option<&dyn ResumeScheduler>;
    fn resume_signal_source(&self) -> Option<&dyn ResumeSignalSource>;
    fn checkpoint_blob_store(&self) -> Option<&dyn CheckpointBlobStore>;
    fn max_durability_mode(&self) -> DurabilityMode;
}

// ─────────────────────────────────────────────────────────────────────────
// WASM guest transport (dynamic bundles)
// ─────────────────────────────────────────────────────────────────────────

/// Opcode family id reserved for durability operations.
///
/// Encoding: `(family << 16) | op_id`.
pub const OP_FAMILY_DURABILITY: u32 = 6;

pub const OP_RESUME_SCHEDULE_AT: u32 = (OP_FAMILY_DURABILITY << 16) | 1;
pub const OP_RESUME_SCHEDULE_AFTER: u32 = (OP_FAMILY_DURABILITY << 16) | 2;
pub const OP_RESUME_CANCEL: u32 = (OP_FAMILY_DURABILITY << 16) | 3;
pub const OP_RESUME_STATUS: u32 = (OP_FAMILY_DURABILITY << 16) | 4;
pub const OP_TOKEN_CREATE: u32 = (OP_FAMILY_DURABILITY << 16) | 5;
pub const OP_TOKEN_RESOLVE: u32 = (OP_FAMILY_DURABILITY << 16) | 6;
pub const OP_TOKEN_REVOKE: u32 = (OP_FAMILY_DURABILITY << 16) | 7;
pub const OP_DURABILITY_GET_CHECKPOINT_HANDLE: u32 = (OP_FAMILY_DURABILITY << 16) | 8;

// ── Transport types (used by both guest and host) ────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleAtRequest {
    pub handle: CheckpointHandle,
    pub at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleAfterRequest {
    pub handle: CheckpointHandle,
    pub delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelScheduleRequest {
    pub schedule_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleStatusRequest {
    pub schedule_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ScheduleStatusTransport {
    Pending { fires_at_ms: u64 },
    Fired { fired_at_ms: u64 },
    Cancelled,
    Expired,
}

impl From<ScheduleStatus> for ScheduleStatusTransport {
    fn from(status: ScheduleStatus) -> Self {
        match status {
            ScheduleStatus::Pending { fires_at_ms } => Self::Pending { fires_at_ms },
            ScheduleStatus::Fired { fired_at_ms } => Self::Fired { fired_at_ms },
            ScheduleStatus::Cancelled => Self::Cancelled,
            ScheduleStatus::Expired => Self::Expired,
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl ScheduleStatusTransport {
    fn into_schedule_status(self) -> ScheduleStatus {
        match self {
            Self::Pending { fires_at_ms } => ScheduleStatus::Pending { fires_at_ms },
            Self::Fired { fired_at_ms } => ScheduleStatus::Fired { fired_at_ms },
            Self::Cancelled => ScheduleStatus::Cancelled,
            Self::Expired => ScheduleStatus::Expired,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTokenRequest {
    pub handle: CheckpointHandle,
    pub ttl_ms: Option<u64>,
    pub single_use: bool,
    pub metadata: Option<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveTokenRequest {
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevokeTokenRequest {
    pub token: String,
}

// ── Response status bytes ────────────────────────────────────────────────

#[cfg(target_arch = "wasm32")]
const RESP_OK: u8 = 0;
#[cfg(target_arch = "wasm32")]
const RESP_NOT_FOUND: u8 = 1;
#[cfg(target_arch = "wasm32")]
const RESP_ERR: u8 = 2;

#[cfg(target_arch = "wasm32")]
fn decode_response<'a>(bytes: &'a [u8], label: &str) -> Result<(u8, &'a [u8]), String> {
    if bytes.is_empty() {
        return Err(format!("invalid {label} response: empty"));
    }
    Ok((bytes[0], &bytes[1..]))
}

#[cfg(target_arch = "wasm32")]
fn decode_error_message(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(msg) if !msg.is_empty() => msg.to_string(),
        _ => "durability capability error".to_string(),
    }
}

// ── RemoteResumeScheduler ────────────────────────────────────────────────

#[cfg(target_arch = "wasm32")]
pub struct RemoteResumeScheduler {
    _private: (),
}

#[cfg(target_arch = "wasm32")]
impl RemoteResumeScheduler {
    pub fn new() -> Self {
        Self { _private: () }
    }
}

#[cfg(target_arch = "wasm32")]
impl Default for RemoteResumeScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(target_arch = "wasm32")]
impl Capability for RemoteResumeScheduler {
    fn name(&self) -> &'static str {
        "resume_scheduler.remote"
    }
}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
impl ResumeScheduler for RemoteResumeScheduler {
    async fn schedule_at(
        &self,
        handle: CheckpointHandle,
        at_ms: u64,
    ) -> Result<ScheduleId, ScheduleError> {
        let req = ScheduleAtRequest { handle, at_ms };
        let req_bytes = serde_json::to_vec(&req)
            .map_err(|err| ScheduleError::Unavailable(format!("encode schedule_at: {err}")))?;
        let resp = crate::wasm_transport::cap_call(OP_RESUME_SCHEDULE_AT, &req_bytes)
            .map_err(|err| ScheduleError::Unavailable(err.to_string()))?;
        let (status, payload) =
            decode_response(&resp, "schedule_at").map_err(ScheduleError::Unavailable)?;
        match status {
            RESP_OK => {
                let id = std::str::from_utf8(payload).map_err(|err| {
                    ScheduleError::Unavailable(format!("decode schedule id: {err}"))
                })?;
                Ok(ScheduleId(id.to_string()))
            }
            RESP_ERR => Err(ScheduleError::Unavailable(decode_error_message(payload))),
            other => Err(ScheduleError::Unavailable(format!(
                "invalid schedule_at response status {other}"
            ))),
        }
    }

    async fn schedule_after(
        &self,
        handle: CheckpointHandle,
        delay: Duration,
    ) -> Result<ScheduleId, ScheduleError> {
        let req = ScheduleAfterRequest {
            handle,
            delay_ms: delay.as_millis() as u64,
        };
        let req_bytes = serde_json::to_vec(&req)
            .map_err(|err| ScheduleError::Unavailable(format!("encode schedule_after: {err}")))?;
        let resp = crate::wasm_transport::cap_call(OP_RESUME_SCHEDULE_AFTER, &req_bytes)
            .map_err(|err| ScheduleError::Unavailable(err.to_string()))?;
        let (status, payload) =
            decode_response(&resp, "schedule_after").map_err(ScheduleError::Unavailable)?;
        match status {
            RESP_OK => {
                let id = std::str::from_utf8(payload).map_err(|err| {
                    ScheduleError::Unavailable(format!("decode schedule id: {err}"))
                })?;
                Ok(ScheduleId(id.to_string()))
            }
            RESP_ERR => Err(ScheduleError::Unavailable(decode_error_message(payload))),
            other => Err(ScheduleError::Unavailable(format!(
                "invalid schedule_after response status {other}"
            ))),
        }
    }

    async fn cancel(&self, schedule_id: ScheduleId) -> Result<(), ScheduleError> {
        let req = CancelScheduleRequest {
            schedule_id: schedule_id.0,
        };
        let req_bytes = serde_json::to_vec(&req)
            .map_err(|err| ScheduleError::Unavailable(format!("encode cancel: {err}")))?;
        let resp = crate::wasm_transport::cap_call(OP_RESUME_CANCEL, &req_bytes)
            .map_err(|err| ScheduleError::Unavailable(err.to_string()))?;
        let (status, payload) =
            decode_response(&resp, "cancel").map_err(ScheduleError::Unavailable)?;
        match status {
            RESP_OK => Ok(()),
            RESP_NOT_FOUND => Err(ScheduleError::NotFound),
            RESP_ERR => Err(ScheduleError::Unavailable(decode_error_message(payload))),
            other => Err(ScheduleError::Unavailable(format!(
                "invalid cancel response status {other}"
            ))),
        }
    }

    async fn status(&self, schedule_id: ScheduleId) -> Result<ScheduleStatus, ScheduleError> {
        let req = ScheduleStatusRequest {
            schedule_id: schedule_id.0,
        };
        let req_bytes = serde_json::to_vec(&req)
            .map_err(|err| ScheduleError::Unavailable(format!("encode status: {err}")))?;
        let resp = crate::wasm_transport::cap_call(OP_RESUME_STATUS, &req_bytes)
            .map_err(|err| ScheduleError::Unavailable(err.to_string()))?;
        let (status, payload) =
            decode_response(&resp, "status").map_err(ScheduleError::Unavailable)?;
        match status {
            RESP_OK => {
                let transport: ScheduleStatusTransport = serde_json::from_slice(payload)
                    .map_err(|err| ScheduleError::Unavailable(format!("decode status: {err}")))?;
                Ok(transport.into_schedule_status())
            }
            RESP_NOT_FOUND => Err(ScheduleError::NotFound),
            RESP_ERR => Err(ScheduleError::Unavailable(decode_error_message(payload))),
            other => Err(ScheduleError::Unavailable(format!(
                "invalid status response status {other}"
            ))),
        }
    }
}

// ── RemoteResumeSignalSource ─────────────────────────────────────────────

#[cfg(target_arch = "wasm32")]
pub struct RemoteResumeSignalSource {
    _private: (),
}

#[cfg(target_arch = "wasm32")]
impl RemoteResumeSignalSource {
    pub fn new() -> Self {
        Self { _private: () }
    }
}

#[cfg(target_arch = "wasm32")]
impl Default for RemoteResumeSignalSource {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(target_arch = "wasm32")]
impl Capability for RemoteResumeSignalSource {
    fn name(&self) -> &'static str {
        "resume_signal_source.remote"
    }
}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
impl ResumeSignalSource for RemoteResumeSignalSource {
    async fn create_token(
        &self,
        handle: &CheckpointHandle,
        config: TokenConfig,
    ) -> Result<ResumeToken, TokenError> {
        let req = CreateTokenRequest {
            handle: handle.clone(),
            ttl_ms: config.ttl.map(|d| d.as_millis() as u64),
            single_use: config.single_use,
            metadata: config.metadata,
        };
        let req_bytes = serde_json::to_vec(&req)
            .map_err(|err| TokenError::Generation(format!("encode create_token: {err}")))?;
        let resp = crate::wasm_transport::cap_call(OP_TOKEN_CREATE, &req_bytes)
            .map_err(|err| TokenError::Generation(err.to_string()))?;
        let (status, payload) =
            decode_response(&resp, "create_token").map_err(TokenError::Generation)?;
        match status {
            RESP_OK => {
                let token = std::str::from_utf8(payload)
                    .map_err(|err| TokenError::Generation(format!("decode token: {err}")))?;
                Ok(ResumeToken(token.to_string()))
            }
            RESP_ERR => Err(TokenError::Generation(decode_error_message(payload))),
            other => Err(TokenError::Generation(format!(
                "invalid create_token response status {other}"
            ))),
        }
    }

    async fn resolve_token(&self, token: &ResumeToken) -> Result<CheckpointHandle, TokenError> {
        let req = ResolveTokenRequest {
            token: token.0.clone(),
        };
        let req_bytes = serde_json::to_vec(&req)
            .map_err(|err| TokenError::Generation(format!("encode resolve_token: {err}")))?;
        let resp = crate::wasm_transport::cap_call(OP_TOKEN_RESOLVE, &req_bytes)
            .map_err(|err| TokenError::Generation(err.to_string()))?;
        let (status, payload) =
            decode_response(&resp, "resolve_token").map_err(TokenError::Generation)?;
        match status {
            RESP_OK => {
                let handle: CheckpointHandle = serde_json::from_slice(payload)
                    .map_err(|err| TokenError::Generation(format!("decode handle: {err}")))?;
                Ok(handle)
            }
            RESP_NOT_FOUND => Err(TokenError::NotFound),
            RESP_ERR => Err(TokenError::Generation(decode_error_message(payload))),
            other => Err(TokenError::Generation(format!(
                "invalid resolve_token response status {other}"
            ))),
        }
    }

    async fn revoke_token(&self, token: &ResumeToken) -> Result<(), TokenError> {
        let req = RevokeTokenRequest {
            token: token.0.clone(),
        };
        let req_bytes = serde_json::to_vec(&req)
            .map_err(|err| TokenError::Generation(format!("encode revoke_token: {err}")))?;
        let resp = crate::wasm_transport::cap_call(OP_TOKEN_REVOKE, &req_bytes)
            .map_err(|err| TokenError::Generation(err.to_string()))?;
        let (status, payload) =
            decode_response(&resp, "revoke_token").map_err(TokenError::Generation)?;
        match status {
            RESP_OK => Ok(()),
            RESP_NOT_FOUND => Err(TokenError::NotFound),
            RESP_ERR => Err(TokenError::Generation(decode_error_message(payload))),
            other => Err(TokenError::Generation(format!(
                "invalid revoke_token response status {other}"
            ))),
        }
    }
}

// ── Remote checkpoint handle bridge ──────────────────────────────────────

/// Fetch the current checkpoint handle from the host via opcode.
///
/// This is the wasm32 fallback for `context::current_checkpoint_handle()` when
/// the thread-local is empty (i.e., the handle was set on the host side but not
/// propagated through the wasm boundary by `with_checkpoint_handle`).
#[cfg(target_arch = "wasm32")]
pub fn current_checkpoint_handle_remote() -> Option<CheckpointHandle> {
    let resp = crate::wasm_transport::cap_call(OP_DURABILITY_GET_CHECKPOINT_HANDLE, &[]).ok()?;
    if resp.is_empty() {
        return None;
    }
    match resp[0] {
        0 => {
            // RESP_OK — payload is JSON CheckpointHandle
            serde_json::from_slice(&resp[1..]).ok()
        }
        1 => None, // RESP_NOT_FOUND — no handle set
        _ => None, // Error or unknown
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod local_scheduler_tests {
    use super::*;

    struct ManualClock {
        now_ms: AtomicU64,
        changed: Notify,
    }

    impl ManualClock {
        fn new(now_ms: u64) -> Self {
            Self {
                now_ms: AtomicU64::new(now_ms),
                changed: Notify::new(),
            }
        }

        fn set(&self, now_ms: u64) {
            self.now_ms.store(now_ms, Ordering::Relaxed);
            self.changed.notify_one();
        }
    }

    #[async_trait]
    impl LocalSchedulerClock for ManualClock {
        fn now_ms(&self) -> u64 {
            self.now_ms.load(Ordering::Relaxed)
        }

        async fn sleep_until_ms(&self, target_ms: u64) {
            while self.now_ms() < target_ms {
                self.changed.notified().await;
            }
        }
    }

    fn handle(checkpoint_id: &str) -> CheckpointHandle {
        CheckpointHandle {
            checkpoint_id: checkpoint_id.into(),
            flow_id: FlowId("flow".into()),
            run_id: "run".into(),
        }
    }

    #[tokio::test]
    async fn due_observation_stays_pending_until_atomic_claim() {
        let clock = Arc::new(ManualClock::new(100));
        let epoch_scheduler = LocalResumeScheduler::with_clock(clock.clone());
        let epoch_id = epoch_scheduler
            .schedule_at(handle("unix-epoch"), 0)
            .await
            .unwrap();
        assert_eq!(
            epoch_scheduler.status(epoch_id.clone()).await.unwrap(),
            ScheduleStatus::Pending { fires_at_ms: 0 }
        );
        let epoch_claim = epoch_scheduler.wait_and_claim_next().await.unwrap();
        assert_eq!(epoch_claim.schedule_id, epoch_id);
        assert_eq!(epoch_claim.fires_at_ms, 0);

        let scheduler = LocalResumeScheduler::with_clock(clock.clone());
        let expected_handle = handle("checkpoint");
        let id = scheduler
            .schedule_at(expected_handle.clone(), 150)
            .await
            .unwrap();
        clock.set(151);
        assert_eq!(
            scheduler.status(id.clone()).await.unwrap(),
            ScheduleStatus::Pending { fires_at_ms: 150 }
        );

        let claimed = scheduler.wait_and_claim_next().await.unwrap();
        assert_eq!(claimed.schedule_id, id.clone());
        assert_eq!(claimed.handle, expected_handle);
        assert_eq!(claimed.fires_at_ms, 150);
        assert_eq!(
            scheduler.status(id).await.unwrap(),
            ScheduleStatus::Fired { fired_at_ms: 151 }
        );
    }

    #[tokio::test]
    async fn wait_uses_manual_clock_and_claims_earliest_pending_schedule() {
        let clock = Arc::new(ManualClock::new(1_000));
        let scheduler = Arc::new(LocalResumeScheduler::with_clock(clock.clone()));
        let later = scheduler.schedule_at(handle("later"), 1_100).await.unwrap();
        let earlier = scheduler
            .schedule_at(handle("earlier"), 1_050)
            .await
            .unwrap();

        let waiter = {
            let scheduler = Arc::clone(&scheduler);
            tokio::spawn(async move { scheduler.wait_and_claim_next().await.unwrap() })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        clock.set(1_050);
        let claimed = waiter.await.unwrap();
        assert_eq!(claimed.schedule_id, earlier);
        assert_eq!(claimed.handle, handle("earlier"));
        assert_eq!(
            scheduler.status(later).await.unwrap(),
            ScheduleStatus::Pending { fires_at_ms: 1_100 }
        );
    }

    #[tokio::test]
    async fn schedule_after_uses_clock_and_ids_are_unique() {
        let clock = Arc::new(ManualClock::new(1_000));
        let scheduler = LocalResumeScheduler::with_clock(clock);
        assert!(matches!(
            scheduler
                .schedule_after(handle("invalid"), Duration::ZERO)
                .await,
            Err(ScheduleError::InvalidDelay(_))
        ));
        let first = scheduler
            .schedule_after(handle("first"), Duration::from_millis(25))
            .await
            .unwrap();
        let second = scheduler
            .schedule_after(handle("second"), Duration::from_millis(25))
            .await
            .unwrap();
        assert_ne!(first, second);
        assert_eq!(
            scheduler.status(first).await.unwrap(),
            ScheduleStatus::Pending { fires_at_ms: 1_025 }
        );
    }

    #[tokio::test]
    async fn cancel_and_not_found_are_observable() {
        let scheduler = LocalResumeScheduler::with_clock(Arc::new(ManualClock::new(10)));
        let id = scheduler
            .schedule_at(handle("cancelled"), 20)
            .await
            .unwrap();
        scheduler.cancel(id.clone()).await.unwrap();
        scheduler.cancel(id.clone()).await.unwrap();
        assert_eq!(
            scheduler.status(id).await.unwrap(),
            ScheduleStatus::Cancelled
        );

        let fired = scheduler.schedule_at(handle("fired"), 1).await.unwrap();
        let claimed = scheduler.wait_and_claim_next().await.unwrap();
        assert_eq!(claimed.schedule_id, fired);
        assert!(matches!(
            scheduler.cancel(fired.clone()).await,
            Err(ScheduleError::Unavailable(_))
        ));
        assert!(matches!(
            scheduler.status(fired).await.unwrap(),
            ScheduleStatus::Fired { .. }
        ));

        let missing = ScheduleId("missing".into());
        assert!(matches!(
            scheduler.status(missing.clone()).await,
            Err(ScheduleError::NotFound)
        ));
        assert!(matches!(
            scheduler.cancel(missing).await,
            Err(ScheduleError::NotFound)
        ));
    }
}
