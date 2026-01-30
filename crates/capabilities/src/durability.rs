use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::blob::BlobStore;
use crate::Capability;
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
    async fn lease(&self, handle: &CheckpointHandle, ttl: Duration) -> Result<Lease, CheckpointError>;
    async fn release_lease(&self, lease: Lease) -> Result<(), CheckpointError>;
    async fn list(&self, filter: CheckpointFilter) -> Result<Vec<CheckpointHandle>, CheckpointError>;
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
    async fn delete_checkpoint_blobs(&self, checkpoint_id: &str)
        -> Result<(), crate::blob::BlobError>;
}

pub trait HostDurability {
    fn checkpoint_store(&self) -> Option<&dyn CheckpointStore>;
    fn resume_scheduler(&self) -> Option<&dyn ResumeScheduler>;
    fn resume_signal_source(&self) -> Option<&dyn ResumeSignalSource>;
    fn checkpoint_blob_store(&self) -> Option<&dyn CheckpointBlobStore>;
    fn max_durability_mode(&self) -> DurabilityMode;
}
