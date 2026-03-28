//! Workers Durable Objects-backed dedupe capability for Lattice.
//!
//! This crate wraps Cloudflare Durable Objects and implements the `DedupeStore` trait
//! from the `capabilities` crate.
//!
//! # Durable Objects Characteristics
//!
//! Cloudflare Durable Objects provide:
//!
//! - **Strong Consistency**: All operations within a single DO instance are strongly
//!   consistent and serialized.
//! - **Persistent Storage**: Key-value storage (and SQLite) that survives restarts.
//! - **Global Uniqueness**: Each DO instance is identified by a unique ID and runs
//!   in a single location worldwide.
//! - **Alarms**: Scheduled callbacks for background processing and TTL expiration.
//! - **Transactional Operations**: Storage operations within a single request are
//!   transactional.
//!
//! # Architecture
//!
//! The dedupe implementation uses DO storage to track idempotency keys:
//!
//! 1. **Client Side (`WorkersDurableObject`)**: Obtains DO stubs and forwards
//!    dedupe requests via fetch.
//! 2. **DO Side (user-defined)**: Implements the storage logic using
//!    `self.state.storage()` operations with optional alarms + SQLite.
//!
//! # Limits & Plan Constraints
//!
//! - SQLite-backed DOs: storage per object 10 GB; key + value <= 2 MB.
//! - SQL limits: 100 columns/table, row size 2 MB, statement length 100 KB,
//!   bound params <= 100, SQL function args <= 32, LIKE/GLOB pattern <= 50 bytes.
//! - CPU per request defaults to 30s and is configurable via `limits.cpu_ms`.
//! - WebSocket message size limit: 32 MiB (received messages).
//! - Free plans support SQLite-backed DOs only; key-value backend requires paid.
//!
//! Migrations are required for class changes; you cannot switch an existing
//! class to SQLite later (create a new class instead).
//!
//! # Example
//!
//! ```ignore
//! use cap_do_workers::WorkersDurableObject;
//! use capabilities::dedupe::DedupeStore;
//!
//! // In your worker handler:
//! let namespace = env.durable_object("DEDUPE_DO")?;
//! let dedupe = WorkersDurableObject::new(namespace, "flow-scope");
//!
//! // Check if key exists (returns true if newly inserted)
//! let is_new = dedupe.put_if_absent(b"idem-key-123", Duration::from_secs(3600)).await?;
//! ```

use base64::Engine;
use capabilities::Capability;
use capabilities::dedupe::{self, DedupeError, DedupeStore};
use capabilities::durability::{
    CheckpointError, CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStore, Lease,
    ResumeScheduler, ResumeSignalSource, ResumeToken, ScheduleError, ScheduleId, ScheduleStatus,
    TokenConfig, TokenError,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;

// ─────────────────────────────────────────────────────────────────────────────
// Durable Objects Limits and Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Maximum key size in bytes for DO storage (2048 bytes).
///
/// Dedupe keys are stored with a "dedupe:" prefix; validate encoded length + prefix.
pub const MAX_KEY_SIZE: usize = 2048;

/// Prefix for DO storage keys used by dedupe entries.
pub const DEDUPE_KEY_PREFIX: &str = "dedupe:";
pub const IDEM_KEY_PREFIX: &str = "idem:";
pub const LAST_SEQ_KEY: &str = "seq:last";
pub const SNAPSHOT_SEQ_KEY: &str = "seq:snapshot";

/// Maximum value size in bytes for DO storage (2 MiB).
///
/// SQLite-backed DO storage allows key + value up to 2 MiB.
pub const MAX_VALUE_SIZE: usize = 2 * 1024 * 1024;

/// Default TTL for dedupe entries if none specified (24 hours).
pub const DEFAULT_TTL_SECONDS: u64 = 24 * 60 * 60;

/// Minimum alarm granularity in milliseconds.
///
/// DO alarms have millisecond precision but scheduling too frequently
/// can cause issues.
pub const MIN_ALARM_MS: u64 = 1000;

/// Prefix for persisted durability checkpoints.
#[cfg(target_arch = "wasm32")]
pub const CHECKPOINT_KEY_PREFIX: &str = "ckpt:";

/// Prefix for checkpoint lease records.
#[cfg(target_arch = "wasm32")]
pub const CHECKPOINT_LEASE_PREFIX: &str = "ckpt_lease:";

#[cfg(target_arch = "wasm32")]
fn checkpoint_key(id: &str) -> String {
    format!("{CHECKPOINT_KEY_PREFIX}{id}")
}

#[cfg(target_arch = "wasm32")]
fn checkpoint_lease_key(id: &str) -> String {
    format!("{CHECKPOINT_LEASE_PREFIX}{id}")
}

#[cfg(target_arch = "wasm32")]
pub const SCHEDULE_KEY_PREFIX: &str = "sched:";

#[cfg(target_arch = "wasm32")]
pub const RESUME_TOKEN_KEY_PREFIX: &str = "resume_token:";

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredLease {
    lease_id: String,
    expires_at_ms: u64,
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Serialize, Deserialize)]
enum StoredScheduleStatus {
    Pending,
    Fired { fired_at_ms: u64 },
    Cancelled,
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSchedule {
    checkpoint_handle: CheckpointHandle,
    fires_at_ms: u64,
    status: StoredScheduleStatus,
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredToken {
    checkpoint_handle: CheckpointHandle,
    expires_at_ms: Option<u64>,
    single_use: bool,
    used: bool,
    metadata: Option<JsonValue>,
}

#[cfg(target_arch = "wasm32")]
fn schedule_key(id: &str) -> String {
    format!("{SCHEDULE_KEY_PREFIX}{id}")
}

#[cfg(target_arch = "wasm32")]
fn resume_token_key(token: &str) -> String {
    format!("{RESUME_TOKEN_KEY_PREFIX}{token}")
}

#[cfg(target_arch = "wasm32")]
fn now_ms_u64() -> u64 {
    js_sys::Date::now() as u64
}

#[cfg(target_arch = "wasm32")]
fn map_do_checkpoint_error(err: DoError) -> CheckpointError {
    match err {
        DoError::RuntimeUnavailable => {
            CheckpointError::Storage("durability runtime unavailable".to_string())
        }
        DoError::StorageError(msg)
        | DoError::BindingError(msg)
        | DoError::IdError(msg)
        | DoError::StubError(msg)
        | DoError::FetchError(msg)
        | DoError::AlarmError(msg)
        | DoError::SqlError(msg)
        | DoError::SerdeError(msg)
        | DoError::InvalidResponse(msg)
        | DoError::InvalidRequest(msg) => CheckpointError::Storage(msg),
        DoError::KeyTooLarge(size) => CheckpointError::Storage(format!("key too large: {size}")),
    }
}

#[cfg(target_arch = "wasm32")]
fn map_do_schedule_error(err: DoError) -> ScheduleError {
    match err {
        DoError::RuntimeUnavailable => {
            ScheduleError::Unavailable("durability runtime unavailable".to_string())
        }
        DoError::StorageError(msg)
        | DoError::BindingError(msg)
        | DoError::IdError(msg)
        | DoError::StubError(msg)
        | DoError::FetchError(msg)
        | DoError::AlarmError(msg)
        | DoError::SqlError(msg)
        | DoError::SerdeError(msg)
        | DoError::InvalidResponse(msg)
        | DoError::InvalidRequest(msg) => ScheduleError::Unavailable(msg),
        DoError::KeyTooLarge(size) => ScheduleError::Unavailable(format!("key too large: {size}")),
    }
}

#[cfg(target_arch = "wasm32")]
fn map_do_token_error(err: DoError) -> TokenError {
    match err {
        DoError::RuntimeUnavailable => {
            TokenError::Generation("durability runtime unavailable".to_string())
        }
        DoError::StorageError(msg)
        | DoError::BindingError(msg)
        | DoError::IdError(msg)
        | DoError::StubError(msg)
        | DoError::FetchError(msg)
        | DoError::AlarmError(msg)
        | DoError::SqlError(msg)
        | DoError::SerdeError(msg)
        | DoError::InvalidResponse(msg)
        | DoError::InvalidRequest(msg) => TokenError::Generation(msg),
        DoError::KeyTooLarge(size) => TokenError::Generation(format!("key too large: {size}")),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Error Types
// ─────────────────────────────────────────────────────────────────────────────

/// Errors specific to Durable Objects operations.
#[derive(Debug, thiserror::Error)]
pub enum DoError {
    /// Durable Object operations require the Workers runtime.
    #[error("durable object runtime unavailable")]
    RuntimeUnavailable,
    /// The key exceeds the maximum allowed size.
    #[error("key exceeds maximum size of {MAX_KEY_SIZE} bytes (got {0} bytes)")]
    KeyTooLarge(usize),

    /// Failed to obtain DO stub.
    #[error("failed to get durable object stub: {0}")]
    StubError(String),

    /// DO fetch request failed.
    #[error("durable object fetch failed: {0}")]
    FetchError(String),

    /// Namespace binding could not be resolved.
    #[error("durable object binding error: {0}")]
    BindingError(String),

    /// Object ID creation failed.
    #[error("durable object id error: {0}")]
    IdError(String),

    /// Storage operation failed.
    #[error("storage operation failed: {0}")]
    StorageError(String),

    /// Alarm operation failed.
    #[error("alarm operation failed: {0}")]
    AlarmError(String),

    /// SQLite operation failed.
    #[error("sqlite operation failed: {0}")]
    SqlError(String),

    /// Serialization/deserialization error.
    #[error("serialization error: {0}")]
    SerdeError(String),

    /// Invalid response from DO.
    #[error("invalid response from durable object: {0}")]
    InvalidResponse(String),

    /// Invalid or unsupported request payload.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

impl From<DoError> for DedupeError {
    fn from(err: DoError) -> Self {
        DedupeError::Other(err.to_string())
    }
}

fn validate_dedupe_key_len(encoded_len: usize) -> Result<(), DoError> {
    let storage_len = encoded_len.saturating_add(DEDUPE_KEY_PREFIX.len());
    if storage_len > MAX_KEY_SIZE {
        return Err(DoError::KeyTooLarge(storage_len));
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Durable Object Protocol Types
// ─────────────────────────────────────────────────────────────────────────────

/// Request payload sent to the Durable Object.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum DoRequest {
    /// Check and set: returns true if key was newly inserted.
    PutIfAbsent {
        /// Base64-encoded key bytes.
        key: String,
        /// TTL in seconds.
        ttl_secs: u64,
    },
    /// Remove a key from the dedupe store.
    Forget {
        /// Base64-encoded key bytes.
        key: String,
    },
    /// Read a stored value.
    StorageGet { key: String },
    /// Write a stored value.
    StoragePut {
        key: String,
        value: StorageValue,
        ttl_secs: Option<u64>,
    },
    /// Delete a stored value.
    StorageDelete { key: String },
    /// List keys with optional prefix/limit.
    StorageList {
        prefix: Option<String>,
        start: Option<String>,
        limit: Option<usize>,
    },
    /// Read the current alarm time (ms since epoch).
    AlarmGet,
    /// Schedule an alarm at a timestamp (ms since epoch).
    AlarmSet { scheduled_ms: i64 },
    /// Delete any scheduled alarm.
    AlarmDelete,
    /// Process due schedule records (alarm tick path).
    ProcessDueSchedules,
    /// Execute a SQLite statement.
    SqlExec {
        query: String,
        #[serde(default)]
        bindings: Vec<SqlValue>,
        #[serde(default)]
        mode: SqlExecMode,
    },
    /// Reserve a sequence number with idempotency tracking.
    Sequence {
        /// Idempotency key scoped to a durable object instance.
        idempotency_key: String,
        /// Payload hash for diagnostic purposes.
        payload_hash: String,
        /// TTL in seconds.
        ttl_secs: u64,
        /// Current time in ms since epoch (optional).
        now_ms: i64,
    },
}

/// Response payload from the Durable Object.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DoResponse {
    PutIfAbsent {
        inserted: bool,
    },
    StorageGet {
        value: Option<StorageValue>,
    },
    StoragePut,
    StorageDelete {
        deleted: bool,
    },
    StorageList {
        keys: Vec<String>,
    },
    AlarmGet {
        alarm_ms: Option<i64>,
    },
    AlarmSet,
    AlarmDelete,
    ProcessDueSchedules {
        dispatched: usize,
    },
    SqlExec {
        rows: Vec<JsonValue>,
    },
    SqlExecRaw {
        rows: Vec<Vec<SqlValue>>,
    },
    Sequence {
        inserted: bool,
        seq: u64,
        first_seen_ts: i64,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceResult {
    pub inserted: bool,
    pub seq: u64,
    pub first_seen_ts: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum StorageValue {
    Json(JsonValue),
    Bytes(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum SqlValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Blob(String),
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum SqlExecMode {
    #[default]
    Json,
    Raw,
}

#[derive(Debug, Clone, Default)]
pub struct StorageListOptions {
    pub prefix: Option<String>,
    pub start: Option<String>,
    pub limit: Option<usize>,
}

impl StorageListOptions {
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    pub fn with_start(mut self, start: impl Into<String>) -> Self {
        self.start = Some(start.into());
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WASM32 Implementation (actual Workers runtime)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(target_arch = "wasm32")]
mod wasm {
    use super::*;
    use async_trait::async_trait;
    use wasm_bindgen::JsValue;
    use worker::Result as WorkerResult;
    use worker::{
        DurableObject, Env, Fetch, ListOptions, Method, ObjectId, ObjectNamespace, Request,
        RequestInit, Response, ScheduledTime, SqlStorageValue, State, Stub, durable_object,
    };

    const DEFAULT_SCOPE: &str = "dedupe";

    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct StoredValue {
        value: StorageValue,
        expires_at_ms: Option<i64>,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct DedupeEntry {
        expires_at_ms: i64,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct IdemEntry {
        seq: u64,
        hash: String,
        first_seen_ts: i64,
        expires_at_ms: i64,
    }

    impl From<SqlStorageValue> for SqlValue {
        fn from(value: SqlStorageValue) -> Self {
            match value {
                SqlStorageValue::Null => SqlValue::Null,
                SqlStorageValue::Boolean(v) => SqlValue::Boolean(v),
                SqlStorageValue::Integer(v) => SqlValue::Integer(v),
                SqlStorageValue::Float(v) => SqlValue::Float(v),
                SqlStorageValue::String(v) => SqlValue::String(v),
                SqlStorageValue::Blob(v) => {
                    SqlValue::Blob(base64::engine::general_purpose::STANDARD.encode(v))
                }
            }
        }
    }

    impl TryFrom<SqlValue> for SqlStorageValue {
        type Error = DoError;

        fn try_from(value: SqlValue) -> Result<Self, Self::Error> {
            match value {
                SqlValue::Null => Ok(SqlStorageValue::Null),
                SqlValue::Boolean(v) => Ok(SqlStorageValue::Boolean(v)),
                SqlValue::Integer(v) => Ok(SqlStorageValue::Integer(v)),
                SqlValue::Float(v) => Ok(SqlStorageValue::Float(v)),
                SqlValue::String(v) => Ok(SqlStorageValue::String(v)),
                SqlValue::Blob(v) => {
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(v)
                        .map_err(|e| DoError::SerdeError(e.to_string()))?;
                    Ok(SqlStorageValue::Blob(bytes))
                }
            }
        }
    }

    /// Helpers for resolving Durable Object bindings and object IDs.
    #[derive(Clone)]
    pub struct DurableObjectBinding {
        binding: String,
        namespace: ObjectNamespace,
    }

    impl DurableObjectBinding {
        pub fn from_env(env: &Env, binding: impl Into<String>) -> Result<Self, DoError> {
            let binding = binding.into();
            let namespace = env
                .durable_object(&binding)
                .map_err(|e| DoError::BindingError(e.to_string()))?;
            Ok(Self { binding, namespace })
        }

        pub fn binding(&self) -> &str {
            &self.binding
        }

        pub fn namespace(&self) -> &ObjectNamespace {
            &self.namespace
        }

        pub fn id_from_name(&self, name: &str) -> Result<ObjectId<'_>, DoError> {
            self.namespace
                .id_from_name(name)
                .map_err(|e| DoError::IdError(e.to_string()))
        }

        pub fn id_from_string(&self, name: &str) -> Result<ObjectId<'_>, DoError> {
            self.namespace
                .id_from_string(name)
                .map_err(|e| DoError::IdError(e.to_string()))
        }

        pub fn unique_id(&self) -> Result<ObjectId<'_>, DoError> {
            self.namespace
                .unique_id()
                .map_err(|e| DoError::IdError(e.to_string()))
        }

        pub fn unique_id_with_jurisdiction(
            &self,
            jurisdiction: &str,
        ) -> Result<ObjectId<'_>, DoError> {
            self.namespace
                .unique_id_with_jurisdiction(jurisdiction)
                .map_err(|e| DoError::IdError(e.to_string()))
        }

        pub fn stub_from_name(&self, name: &str) -> Result<Stub, DoError> {
            let id = self
                .namespace
                .id_from_name(name)
                .map_err(|e| DoError::IdError(e.to_string()))?;
            id.get_stub().map_err(|e| DoError::StubError(e.to_string()))
        }

        pub fn stub_from_id(&self, id: &ObjectId) -> Result<Stub, DoError> {
            id.get_stub().map_err(|e| DoError::StubError(e.to_string()))
        }
    }

    #[durable_object]
    pub struct FlowDurableObject {
        state: State,
        env: Env,
    }

    impl DurableObject for FlowDurableObject {
        fn new(state: State, env: Env) -> Self {
            Self { state, env }
        }

        async fn fetch(&self, mut req: Request) -> WorkerResult<Response> {
            let body = req.text().await.unwrap_or_default();
            let parsed: Result<DoRequest, _> = serde_json::from_str(&body);
            let response = match parsed {
                Ok(request) => {
                    self.handle_request(request)
                        .await
                        .unwrap_or_else(|err| DoResponse::Error {
                            message: err.to_string(),
                        })
                }
                Err(err) => DoResponse::Error {
                    message: format!("invalid request body: {err}"),
                },
            };
            Response::from_json(&response)
        }

        async fn alarm(&self) -> WorkerResult<Response> {
            let removed = self.clear_expired_entries().await.unwrap_or(0);
            let dispatched = self.process_due_schedules().await.unwrap_or_else(|err| {
                tracing::warn!("failed to process due schedules in alarm: {err}");
                0
            });
            Response::from_json(&serde_json::json!({
                "expired_entries": removed,
                "dispatched": dispatched,
            }))
        }
    }

    impl FlowDurableObject {
        async fn handle_request(&self, request: DoRequest) -> Result<DoResponse, DoError> {
            match request {
                DoRequest::PutIfAbsent { key, ttl_secs } => {
                    let inserted = self.put_if_absent_internal(&key, ttl_secs).await?;
                    Ok(DoResponse::PutIfAbsent { inserted })
                }
                DoRequest::Forget { key } => {
                    let deleted = self.delete_key(&key).await?;
                    Ok(DoResponse::StorageDelete { deleted })
                }
                DoRequest::StorageGet { key } => {
                    let value = self.get_value(&key).await?;
                    Ok(DoResponse::StorageGet { value })
                }
                DoRequest::StoragePut {
                    key,
                    value,
                    ttl_secs,
                } => {
                    self.put_value(&key, value, ttl_secs).await?;
                    Ok(DoResponse::StoragePut)
                }
                DoRequest::StorageDelete { key } => {
                    let deleted = self.delete_storage_key(&key).await?;
                    Ok(DoResponse::StorageDelete { deleted })
                }
                DoRequest::StorageList {
                    prefix,
                    start,
                    limit,
                } => {
                    let keys = self
                        .list_keys(prefix.as_deref(), start.as_deref(), limit)
                        .await?;
                    Ok(DoResponse::StorageList { keys })
                }
                DoRequest::AlarmGet => {
                    let alarm_ms = self
                        .state
                        .storage()
                        .get_alarm()
                        .await
                        .map_err(|e| DoError::AlarmError(e.to_string()))?;
                    Ok(DoResponse::AlarmGet { alarm_ms })
                }
                DoRequest::AlarmSet { scheduled_ms } => {
                    self.set_alarm_at(scheduled_ms).await?;
                    Ok(DoResponse::AlarmSet)
                }
                DoRequest::AlarmDelete => {
                    self.state
                        .storage()
                        .delete_alarm()
                        .await
                        .map_err(|e| DoError::AlarmError(e.to_string()))?;
                    Ok(DoResponse::AlarmDelete)
                }
                DoRequest::ProcessDueSchedules => {
                    let dispatched = self.process_due_schedules().await?;
                    Ok(DoResponse::ProcessDueSchedules { dispatched })
                }
                DoRequest::SqlExec {
                    query,
                    bindings,
                    mode,
                } => {
                    let sql = self.state.storage().sql();
                    let bindings: Vec<SqlStorageValue> = bindings
                        .into_iter()
                        .map(SqlStorageValue::try_from)
                        .collect::<Result<_, _>>()?;
                    let cursor = sql
                        .exec(&query, Some(bindings))
                        .map_err(|e| DoError::SqlError(e.to_string()))?;
                    match mode {
                        SqlExecMode::Json => {
                            let rows = cursor
                                .to_array::<serde_json::Value>()
                                .map_err(|e| DoError::SqlError(e.to_string()))?;
                            Ok(DoResponse::SqlExec { rows })
                        }
                        SqlExecMode::Raw => {
                            let mut rows = Vec::new();
                            for row in cursor.raw() {
                                let values = row.map_err(|e| DoError::SqlError(e.to_string()))?;
                                rows.push(values.into_iter().map(SqlValue::from).collect());
                            }
                            Ok(DoResponse::SqlExecRaw { rows })
                        }
                    }
                }
                DoRequest::Sequence {
                    idempotency_key,
                    payload_hash,
                    ttl_secs,
                    now_ms,
                } => {
                    let result = self
                        .sequence_with_idempotency(idempotency_key, payload_hash, ttl_secs, now_ms)
                        .await?;
                    Ok(DoResponse::Sequence {
                        inserted: result.inserted,
                        seq: result.seq,
                        first_seen_ts: result.first_seen_ts,
                    })
                }
            }
        }

        async fn sequence_with_idempotency(
            &self,
            idempotency_key: String,
            payload_hash: String,
            ttl_secs: u64,
            now_override_ms: i64,
        ) -> Result<SequenceResult, DoError> {
            let storage = self.state.storage();
            let now_ms = if now_override_ms > 0 {
                now_override_ms
            } else {
                now_ms()
            };
            let ttl_secs = if ttl_secs == 0 {
                DEFAULT_TTL_SECONDS
            } else {
                ttl_secs
            };
            let expires_at_ms = now_ms + ttl_secs as i64 * 1000;

            let storage_key = format!("{IDEM_KEY_PREFIX}{idempotency_key}");
            if let Some(entry) = storage
                .get::<IdemEntry>(&storage_key)
                .await
                .map_err(|e| DoError::StorageError(e.to_string()))?
            {
                if entry.expires_at_ms > now_ms {
                    return Ok(SequenceResult {
                        inserted: false,
                        seq: entry.seq,
                        first_seen_ts: entry.first_seen_ts,
                    });
                }
                storage
                    .delete(&storage_key)
                    .await
                    .map_err(|e| DoError::StorageError(e.to_string()))?;
            }

            let last_seq = storage
                .get::<u64>(LAST_SEQ_KEY)
                .await
                .map_err(|e| DoError::StorageError(e.to_string()))?
                .unwrap_or(0);
            let next_seq = last_seq.saturating_add(1);

            storage
                .put(LAST_SEQ_KEY, next_seq)
                .await
                .map_err(|e| DoError::StorageError(e.to_string()))?;

            let entry = IdemEntry {
                seq: next_seq,
                hash: payload_hash,
                first_seen_ts: now_ms,
                expires_at_ms,
            };
            storage
                .put(&storage_key, entry)
                .await
                .map_err(|e| DoError::StorageError(e.to_string()))?;
            self.ensure_alarm(expires_at_ms).await?;

            Ok(SequenceResult {
                inserted: true,
                seq: next_seq,
                first_seen_ts: now_ms,
            })
        }

        async fn put_if_absent_internal(&self, key: &str, ttl_secs: u64) -> Result<bool, DoError> {
            let storage_key = format!("{DEDUPE_KEY_PREFIX}{key}");
            let now_ms = now_ms();
            let ttl_secs = if ttl_secs == 0 {
                DEFAULT_TTL_SECONDS
            } else {
                ttl_secs
            };
            let expires_at_ms = now_ms + ttl_secs as i64 * 1000;

            let storage = self.state.storage();
            if let Some(entry) = storage
                .get::<DedupeEntry>(&storage_key)
                .await
                .map_err(|e| DoError::StorageError(e.to_string()))?
            {
                if entry.expires_at_ms > now_ms {
                    return Ok(false);
                }
                storage
                    .delete(&storage_key)
                    .await
                    .map_err(|e| DoError::StorageError(e.to_string()))?;
            }

            let entry = DedupeEntry { expires_at_ms };
            storage
                .put(&storage_key, entry)
                .await
                .map_err(|e| DoError::StorageError(e.to_string()))?;
            self.ensure_alarm(expires_at_ms).await?;
            Ok(true)
        }

        async fn delete_key(&self, key: &str) -> Result<bool, DoError> {
            let storage_key = format!("{DEDUPE_KEY_PREFIX}{key}");
            self.state
                .storage()
                .delete(&storage_key)
                .await
                .map_err(|e| DoError::StorageError(e.to_string()))
        }

        async fn delete_storage_key(&self, key: &str) -> Result<bool, DoError> {
            self.state
                .storage()
                .delete(key)
                .await
                .map_err(|e| DoError::StorageError(e.to_string()))
        }

        async fn get_value(&self, key: &str) -> Result<Option<StorageValue>, DoError> {
            let storage = self.state.storage();
            if let Some(stored) = storage
                .get::<StoredValue>(key)
                .await
                .map_err(|e| DoError::StorageError(e.to_string()))?
            {
                if let Some(expires_at_ms) = stored.expires_at_ms {
                    if expires_at_ms <= now_ms() {
                        storage
                            .delete(key)
                            .await
                            .map_err(|e| DoError::StorageError(e.to_string()))?;
                        return Ok(None);
                    }
                }
                return Ok(Some(stored.value));
            }
            Ok(None)
        }

        async fn put_value(
            &self,
            key: &str,
            value: StorageValue,
            ttl_secs: Option<u64>,
        ) -> Result<(), DoError> {
            if let StorageValue::Bytes(data) = &value {
                let size = base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .map_err(|e| DoError::SerdeError(e.to_string()))?
                    .len();
                if size > MAX_VALUE_SIZE {
                    return Err(DoError::InvalidRequest(format!(
                        "value exceeds {MAX_VALUE_SIZE} bytes"
                    )));
                }
            }

            let expires_at_ms = ttl_secs.map(|ttl| now_ms() + ttl as i64 * 1000);
            let stored = StoredValue {
                value,
                expires_at_ms,
            };
            self.state
                .storage()
                .put(key, stored)
                .await
                .map_err(|e| DoError::StorageError(e.to_string()))?;

            if let Some(expires_at_ms) = expires_at_ms {
                self.ensure_alarm(expires_at_ms).await?;
            }
            Ok(())
        }

        async fn list_keys(
            &self,
            prefix: Option<&str>,
            start: Option<&str>,
            limit: Option<usize>,
        ) -> Result<Vec<String>, DoError> {
            let mut opts = ListOptions::new();
            if let Some(prefix) = prefix {
                opts = opts.prefix(prefix);
            }
            if let Some(start) = start {
                opts = opts.start(start);
            }
            if let Some(limit) = limit {
                opts = opts.limit(limit);
            }

            let map = self
                .state
                .storage()
                .list_with_options(opts)
                .await
                .map_err(|e| DoError::StorageError(e.to_string()))?;
            let keys_iter = map.keys();
            let keys_array = js_sys::Array::from(&keys_iter);
            let mut keys = Vec::with_capacity(keys_array.length() as usize);
            for key in keys_array.iter() {
                if let Some(key) = key.as_string() {
                    keys.push(key);
                }
            }
            Ok(keys)
        }

        async fn ensure_alarm(&self, scheduled_ms: i64) -> Result<(), DoError> {
            let scheduled_ms = scheduled_ms.max(0);
            if scheduled_ms == 0 {
                return Ok(());
            }
            let existing = self
                .state
                .storage()
                .get_alarm()
                .await
                .map_err(|e| DoError::AlarmError(e.to_string()))?;
            let should_set = match existing {
                Some(existing) => scheduled_ms < existing,
                None => true,
            };
            if should_set {
                let date = date_from_ms(scheduled_ms);
                self.state
                    .storage()
                    .set_alarm(ScheduledTime::new(date))
                    .await
                    .map_err(|e| DoError::AlarmError(e.to_string()))?;
            }
            Ok(())
        }

        async fn set_alarm_at(&self, scheduled_ms: i64) -> Result<(), DoError> {
            let now = now_ms();
            let schedule_ms = if scheduled_ms <= now {
                now + MIN_ALARM_MS as i64
            } else {
                scheduled_ms
            };
            let date = date_from_ms(schedule_ms);
            self.state
                .storage()
                .set_alarm(ScheduledTime::new(date))
                .await
                .map_err(|e| DoError::AlarmError(e.to_string()))?;
            Ok(())
        }

        async fn clear_expired_entries(&self) -> Result<usize, DoError> {
            let map = self
                .state
                .storage()
                .list()
                .await
                .map_err(|e| DoError::StorageError(e.to_string()))?;
            let entries = js_sys::Array::from(&map.entries());
            let now = now_ms();
            let mut removed = 0usize;
            let mut next_alarm: Option<i64> = None;

            for entry in entries.iter() {
                let pair = js_sys::Array::from(&entry);
                let key = pair.get(0).as_string().unwrap_or_default();
                let value = pair.get(1);
                if let Ok(stored) = serde_wasm_bindgen::from_value::<StoredValue>(value.clone()) {
                    if let Some(expires_at_ms) = stored.expires_at_ms {
                        if expires_at_ms <= now {
                            self.state
                                .storage()
                                .delete(&key)
                                .await
                                .map_err(|e| DoError::StorageError(e.to_string()))?;
                            removed += 1;
                        } else {
                            next_alarm = Some(match next_alarm {
                                Some(current) => current.min(expires_at_ms),
                                None => expires_at_ms,
                            });
                        }
                    }
                    continue;
                }
                if let Ok(entry) = serde_wasm_bindgen::from_value::<DedupeEntry>(value.clone()) {
                    if entry.expires_at_ms <= now {
                        self.state
                            .storage()
                            .delete(&key)
                            .await
                            .map_err(|e| DoError::StorageError(e.to_string()))?;
                        removed += 1;
                    } else {
                        next_alarm = Some(match next_alarm {
                            Some(current) => current.min(entry.expires_at_ms),
                            None => entry.expires_at_ms,
                        });
                    }
                    continue;
                }

                if let Ok(entry) = serde_wasm_bindgen::from_value::<IdemEntry>(value) {
                    if entry.expires_at_ms <= now {
                        self.state
                            .storage()
                            .delete(&key)
                            .await
                            .map_err(|e| DoError::StorageError(e.to_string()))?;
                        removed += 1;
                    } else {
                        next_alarm = Some(match next_alarm {
                            Some(current) => current.min(entry.expires_at_ms),
                            None => entry.expires_at_ms,
                        });
                    }
                }
            }

            if let Some(next_alarm) = next_alarm {
                self.ensure_alarm(next_alarm).await?;
            } else {
                self.state
                    .storage()
                    .delete_alarm()
                    .await
                    .map_err(|e| DoError::AlarmError(e.to_string()))?;
            }
            Ok(removed)
        }

        async fn process_due_schedules(&self) -> Result<usize, DoError> {
            let keys = self
                .list_keys(Some(SCHEDULE_KEY_PREFIX), None, None)
                .await
                .map_err(|err| DoError::StorageError(err.to_string()))?;
            let mut dispatched = 0usize;
            let mut next_alarm: Option<i64> = None;
            let now_ms = now_ms_u64();

            for key in keys {
                let Some(StorageValue::Json(raw_schedule)) = self.get_value(&key).await? else {
                    continue;
                };
                let mut schedule: StoredSchedule = serde_json::from_value(raw_schedule)
                    .map_err(|err| DoError::StorageError(err.to_string()))?;

                if !matches!(schedule.status, StoredScheduleStatus::Pending) {
                    continue;
                }

                if schedule.fires_at_ms <= now_ms {
                    self.spawn_resume_dispatch(
                        schedule.checkpoint_handle.clone(),
                        schedule.fires_at_ms,
                    );
                    schedule.status = StoredScheduleStatus::Fired {
                        fired_at_ms: now_ms,
                    };
                    let value = serde_json::to_value(&schedule)
                        .map(StorageValue::Json)
                        .map_err(|err| DoError::SerdeError(err.to_string()))?;
                    self.put_value(&key, value, None).await?;
                    dispatched += 1;
                } else {
                    let fire_at = i64::try_from(schedule.fires_at_ms).unwrap_or(i64::MAX);
                    next_alarm = Some(match next_alarm {
                        Some(current) => current.min(fire_at),
                        None => fire_at,
                    });
                }
            }

            if let Some(next_alarm) = next_alarm {
                self.set_alarm_at(next_alarm).await?;
            } else {
                self.state
                    .storage()
                    .delete_alarm()
                    .await
                    .map_err(|e| DoError::AlarmError(e.to_string()))?;
            }

            Ok(dispatched)
        }

        fn spawn_resume_dispatch(&self, handle: CheckpointHandle, fires_at_ms: u64) {
            let env = self.env.clone();
            wasm_bindgen_futures::spawn_local(async move {
                if let Err(err) = Self::dispatch_resume_with_env(env, &handle, fires_at_ms).await {
                    tracing::warn!(
                        "failed to dispatch scheduled resume for checkpoint {}: {err}",
                        handle.checkpoint_id
                    );
                }
            });
        }

        async fn dispatch_resume_with_env(
            env: Env,
            handle: &CheckpointHandle,
            fires_at_ms: u64,
        ) -> Result<(), DoError> {
            let dispatch_url = env
                .var("LATTICE_RESUME_DISPATCH_URL")
                .ok()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "http://internal/__lattice/resume".to_string());
            let dispatch_service = env
                .var("LATTICE_RESUME_SERVICE_BINDING")
                .ok()
                .map(|value| value.to_string());
            let internal_token = env
                .var("LATTICE_INTERNAL_RESUME_TOKEN")
                .ok()
                .map(|value| value.to_string());

            let body = serde_json::to_string(&serde_json::json!({
                "checkpoint_id": handle.checkpoint_id,
                "flow_id": handle.flow_id.as_str(),
                "run_id": handle.run_id,
                "fires_at_ms": fires_at_ms,
            }))
            .map_err(|err| DoError::SerdeError(err.to_string()))?;

            let mut init = RequestInit::new();
            init.with_method(Method::Post);
            init.with_body(Some(JsValue::from_str(&body)));
            let mut request = Request::new_with_init(&dispatch_url, &init)
                .map_err(|e| DoError::FetchError(e.to_string()))?;
            request
                .headers_mut()
                .map_err(|e| DoError::FetchError(e.to_string()))?
                .set("content-type", "application/json")
                .map_err(|e| DoError::FetchError(e.to_string()))?;
            if let Some(token) = internal_token {
                request
                    .headers_mut()
                    .map_err(|e| DoError::FetchError(e.to_string()))?
                    .set("x-lattice-internal-token", &token)
                    .map_err(|e| DoError::FetchError(e.to_string()))?;
            }

            let mut response: Response = if let Some(binding) = dispatch_service {
                let fetcher = env
                    .service(&binding)
                    .map_err(|e| DoError::BindingError(e.to_string()))?;
                let http_response: worker::HttpResponse = fetcher
                    .fetch_request(request)
                    .await
                    .map_err(|e| DoError::FetchError(e.to_string()))?;
                Response::try_from(http_response).map_err(|e| DoError::FetchError(e.to_string()))?
            } else {
                Fetch::Request(request)
                    .send()
                    .await
                    .map_err(|e| DoError::FetchError(e.to_string()))?
            };

            let status = response.status_code();
            if (200..300).contains(&status) || status == 202 {
                return Ok(());
            }

            let body = response.text().await.unwrap_or_default();
            Err(DoError::FetchError(format!(
                "resume dispatch failed with status {status}: {body}"
            )))
        }
    }

    /// Workers Durable Objects-backed dedupe capability.
    ///
    /// This struct wraps an `ObjectNamespace` obtained from the Workers runtime.
    /// Create it by passing a namespace from `env.durable_object("BINDING_NAME")`.
    ///
    /// The actual storage logic must be implemented in a Durable Object class
    /// that handles the request protocol defined by `DoRequest`/`DoResponse`.
    pub struct WorkersDurableObject {
        binding: DurableObjectBinding,
        scope: String,
    }

    impl WorkersDurableObject {
        /// Construct a new Durable Object dedupe capability.
        ///
        /// # Arguments
        ///
        /// * `namespace` - The DO namespace from `env.durable_object("BINDING_NAME")`
        /// * `scope` - Optional namespace prefix for instance derivation.
        pub fn new(namespace: ObjectNamespace, scope: impl Into<String>) -> Self {
            dedupe::ensure_registered();
            Self {
                binding: DurableObjectBinding {
                    binding: "".to_string(),
                    namespace,
                },
                scope: scope.into(),
            }
        }

        pub fn from_binding(binding: DurableObjectBinding, scope: Option<String>) -> Self {
            dedupe::ensure_registered();
            Self {
                binding,
                scope: scope.unwrap_or_else(|| DEFAULT_SCOPE.to_string()),
            }
        }

        pub fn from_env(
            env: &Env,
            binding: impl Into<String>,
            scope: Option<String>,
        ) -> Result<Self, DoError> {
            let binding = DurableObjectBinding::from_env(env, binding)?;
            Ok(Self::from_binding(binding, scope))
        }

        pub fn validate_key(key: &[u8]) -> Result<(), DoError> {
            let encoded_len = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(key)
                .len();
            validate_dedupe_key_len(encoded_len)
        }

        /// Send a request to the Durable Object and parse the response.
        async fn send_request(&self, request: DoRequest) -> Result<DoResponse, DoError> {
            // Get DO instance by name
            let stub = self.stub_for_key(&request).await?;

            // Build the fetch request
            let body =
                serde_json::to_string(&request).map_err(|e| DoError::SerdeError(e.to_string()))?;

            let mut init = RequestInit::new();
            init.with_method(worker::Method::Post);
            init.with_body(Some(worker::wasm_bindgen::JsValue::from_str(&body)));

            let req = Request::new_with_init("http://do/dedupe", &init)
                .map_err(|e| DoError::FetchError(e.to_string()))?;

            // Send to DO
            let mut response: Response = stub
                .fetch_with_request(req)
                .await
                .map_err(|e| DoError::FetchError(e.to_string()))?;

            // Parse response
            let response_text = response
                .text()
                .await
                .map_err(|e| DoError::InvalidResponse(e.to_string()))?;

            serde_json::from_str(&response_text).map_err(|e| DoError::SerdeError(e.to_string()))
        }

        pub async fn storage_get(&self, key: &str) -> Result<Option<StorageValue>, DoError> {
            let request = DoRequest::StorageGet {
                key: key.to_string(),
            };

            match self.send_request(request).await? {
                DoResponse::StorageGet { value } => Ok(value),
                DoResponse::Error { message } => Err(DoError::StorageError(message)),
                other => Err(DoError::InvalidResponse(format!(
                    "unexpected response: {other:?}"
                ))),
            }
        }

        pub async fn storage_put(
            &self,
            key: &str,
            value: StorageValue,
            ttl: Option<Duration>,
        ) -> Result<(), DoError> {
            let ttl_secs = ttl.and_then(|ttl| {
                if ttl.is_zero() {
                    None
                } else {
                    Some(ttl.as_secs().max(1))
                }
            });
            let request = DoRequest::StoragePut {
                key: key.to_string(),
                value,
                ttl_secs,
            };

            match self.send_request(request).await? {
                DoResponse::StoragePut => Ok(()),
                DoResponse::Error { message } => Err(DoError::StorageError(message)),
                other => Err(DoError::InvalidResponse(format!(
                    "unexpected response: {other:?}"
                ))),
            }
        }

        pub async fn storage_delete(&self, key: &str) -> Result<bool, DoError> {
            let request = DoRequest::StorageDelete {
                key: key.to_string(),
            };

            match self.send_request(request).await? {
                DoResponse::StorageDelete { deleted } => Ok(deleted),
                DoResponse::Error { message } => Err(DoError::StorageError(message)),
                other => Err(DoError::InvalidResponse(format!(
                    "unexpected response: {other:?}"
                ))),
            }
        }

        pub async fn storage_list(
            &self,
            options: StorageListOptions,
        ) -> Result<Vec<String>, DoError> {
            let request = DoRequest::StorageList {
                prefix: options.prefix,
                start: options.start,
                limit: options.limit,
            };

            match self.send_request(request).await? {
                DoResponse::StorageList { keys } => Ok(keys),
                DoResponse::Error { message } => Err(DoError::StorageError(message)),
                other => Err(DoError::InvalidResponse(format!(
                    "unexpected response: {other:?}"
                ))),
            }
        }

        pub async fn alarm_get(&self) -> Result<Option<i64>, DoError> {
            match self.send_request(DoRequest::AlarmGet).await? {
                DoResponse::AlarmGet { alarm_ms } => Ok(alarm_ms),
                DoResponse::Error { message } => Err(DoError::AlarmError(message)),
                other => Err(DoError::InvalidResponse(format!(
                    "unexpected response: {other:?}"
                ))),
            }
        }

        pub async fn alarm_set(&self, scheduled_ms: i64) -> Result<(), DoError> {
            let request = DoRequest::AlarmSet { scheduled_ms };
            match self.send_request(request).await? {
                DoResponse::AlarmSet => Ok(()),
                DoResponse::Error { message } => Err(DoError::AlarmError(message)),
                other => Err(DoError::InvalidResponse(format!(
                    "unexpected response: {other:?}"
                ))),
            }
        }

        pub async fn alarm_delete(&self) -> Result<(), DoError> {
            match self.send_request(DoRequest::AlarmDelete).await? {
                DoResponse::AlarmDelete => Ok(()),
                DoResponse::Error { message } => Err(DoError::AlarmError(message)),
                other => Err(DoError::InvalidResponse(format!(
                    "unexpected response: {other:?}"
                ))),
            }
        }

        pub async fn process_due_schedules(&self) -> Result<usize, DoError> {
            match self.send_request(DoRequest::ProcessDueSchedules).await? {
                DoResponse::ProcessDueSchedules { dispatched } => Ok(dispatched),
                DoResponse::Error { message } => Err(DoError::AlarmError(message)),
                other => Err(DoError::InvalidResponse(format!(
                    "unexpected response: {other:?}"
                ))),
            }
        }

        pub async fn sql_exec_json(
            &self,
            query: impl Into<String>,
            bindings: Vec<SqlValue>,
        ) -> Result<Vec<JsonValue>, DoError> {
            let request = DoRequest::SqlExec {
                query: query.into(),
                bindings,
                mode: SqlExecMode::Json,
            };

            match self.send_request(request).await? {
                DoResponse::SqlExec { rows } => Ok(rows),
                DoResponse::Error { message } => Err(DoError::SqlError(message)),
                other => Err(DoError::InvalidResponse(format!(
                    "unexpected response: {other:?}"
                ))),
            }
        }

        pub async fn sql_exec_raw(
            &self,
            query: impl Into<String>,
            bindings: Vec<SqlValue>,
        ) -> Result<Vec<Vec<SqlValue>>, DoError> {
            let request = DoRequest::SqlExec {
                query: query.into(),
                bindings,
                mode: SqlExecMode::Raw,
            };

            match self.send_request(request).await? {
                DoResponse::SqlExecRaw { rows } => Ok(rows),
                DoResponse::Error { message } => Err(DoError::SqlError(message)),
                other => Err(DoError::InvalidResponse(format!(
                    "unexpected response: {other:?}"
                ))),
            }
        }

        pub async fn sequence(
            &self,
            idempotency_key: impl Into<String>,
            payload_hash: impl Into<String>,
            ttl: Duration,
            now_ms: Option<i64>,
        ) -> Result<SequenceResult, DoError> {
            let ttl_secs = if ttl.is_zero() {
                0
            } else {
                ttl.as_secs().max(1)
            };
            let request = DoRequest::Sequence {
                idempotency_key: idempotency_key.into(),
                payload_hash: payload_hash.into(),
                ttl_secs,
                now_ms: now_ms.unwrap_or(0),
            };

            match self.send_request(request).await? {
                DoResponse::Sequence {
                    inserted,
                    seq,
                    first_seen_ts,
                } => Ok(SequenceResult {
                    inserted,
                    seq,
                    first_seen_ts,
                }),
                DoResponse::Error { message } => Err(DoError::InvalidResponse(message)),
                other => Err(DoError::InvalidResponse(format!(
                    "unexpected response: {other:?}"
                ))),
            }
        }

        async fn read_checkpoint_record(
            &self,
            key: &str,
        ) -> Result<Option<CheckpointRecord>, CheckpointError> {
            match self
                .storage_get(key)
                .await
                .map_err(map_do_checkpoint_error)?
            {
                None => Ok(None),
                Some(StorageValue::Json(value)) => serde_json::from_value(value)
                    .map(Some)
                    .map_err(|err| CheckpointError::Storage(err.to_string())),
                Some(StorageValue::Bytes(_)) => Err(CheckpointError::Storage(
                    "checkpoint payload stored as bytes".to_string(),
                )),
            }
        }

        async fn read_checkpoint_lease(
            &self,
            key: &str,
        ) -> Result<Option<StoredLease>, CheckpointError> {
            match self
                .storage_get(key)
                .await
                .map_err(map_do_checkpoint_error)?
            {
                None => Ok(None),
                Some(StorageValue::Json(value)) => serde_json::from_value(value)
                    .map(Some)
                    .map_err(|err| CheckpointError::Storage(err.to_string())),
                Some(StorageValue::Bytes(_)) => Err(CheckpointError::Storage(
                    "checkpoint lease payload stored as bytes".to_string(),
                )),
            }
        }

        async fn put_checkpoint_lease(
            &self,
            key: &str,
            lease: &StoredLease,
        ) -> Result<(), CheckpointError> {
            let value = serde_json::to_value(lease)
                .map(StorageValue::Json)
                .map_err(|err| CheckpointError::Storage(err.to_string()))?;
            self.storage_put(key, value, None)
                .await
                .map_err(map_do_checkpoint_error)
        }

        async fn read_schedule(&self, key: &str) -> Result<Option<StoredSchedule>, ScheduleError> {
            match self.storage_get(key).await.map_err(map_do_schedule_error)? {
                None => Ok(None),
                Some(StorageValue::Json(value)) => serde_json::from_value(value)
                    .map(Some)
                    .map_err(|err| ScheduleError::Unavailable(err.to_string())),
                Some(StorageValue::Bytes(_)) => Err(ScheduleError::Unavailable(
                    "schedule payload stored as bytes".to_string(),
                )),
            }
        }

        async fn put_schedule(
            &self,
            key: &str,
            schedule: &StoredSchedule,
        ) -> Result<(), ScheduleError> {
            let value = serde_json::to_value(schedule)
                .map(StorageValue::Json)
                .map_err(|err| ScheduleError::Unavailable(err.to_string()))?;
            self.storage_put(key, value, None)
                .await
                .map_err(map_do_schedule_error)
        }

        async fn refresh_schedule_alarm(&self) -> Result<(), ScheduleError> {
            let keys = self
                .storage_list(StorageListOptions::default().with_prefix(SCHEDULE_KEY_PREFIX))
                .await
                .map_err(map_do_schedule_error)?;
            let mut next_fire_ms: Option<u64> = None;

            for key in keys {
                let Some(schedule) = self.read_schedule(&key).await? else {
                    continue;
                };
                if matches!(schedule.status, StoredScheduleStatus::Pending) {
                    next_fire_ms = Some(match next_fire_ms {
                        Some(current) => current.min(schedule.fires_at_ms),
                        None => schedule.fires_at_ms,
                    });
                }
            }

            match next_fire_ms {
                Some(fire_at_ms) => self
                    .alarm_set(i64::try_from(fire_at_ms).unwrap_or(i64::MAX))
                    .await
                    .map_err(map_do_schedule_error),
                None => self.alarm_delete().await.map_err(map_do_schedule_error),
            }
        }

        async fn read_token(&self, key: &str) -> Result<Option<StoredToken>, TokenError> {
            match self.storage_get(key).await.map_err(map_do_token_error)? {
                None => Ok(None),
                Some(StorageValue::Json(value)) => serde_json::from_value(value)
                    .map(Some)
                    .map_err(|err| TokenError::Generation(err.to_string())),
                Some(StorageValue::Bytes(_)) => Err(TokenError::Generation(
                    "resume token payload stored as bytes".to_string(),
                )),
            }
        }

        async fn put_token(&self, key: &str, token: &StoredToken) -> Result<(), TokenError> {
            let value = serde_json::to_value(token)
                .map(StorageValue::Json)
                .map_err(|err| TokenError::Generation(err.to_string()))?;
            self.storage_put(key, value, None)
                .await
                .map_err(map_do_token_error)
        }

        async fn put_if_absent_local(
            &self,
            key: &[u8],
            ttl: Duration,
        ) -> Result<bool, DedupeError> {
            tracing::debug!(
                key_len = key.len(),
                ttl_secs = ttl.as_secs(),
                "cap_do_workers.put_if_absent"
            );

            let key_b64 = encode_key(key)?;
            let ttl = if ttl.is_zero() {
                Duration::from_secs(DEFAULT_TTL_SECONDS)
            } else {
                ttl
            };

            let request = DoRequest::PutIfAbsent {
                key: key_b64,
                ttl_secs: ttl.as_secs(),
            };

            match self.send_request(request).await? {
                DoResponse::PutIfAbsent { inserted } => Ok(inserted),
                DoResponse::Error { message } => Err(DedupeError::Other(message)),
                other => Err(DedupeError::Other(format!(
                    "unexpected response: {other:?}"
                ))),
            }
        }

        async fn forget_local(&self, key: &[u8]) -> Result<(), DedupeError> {
            tracing::debug!(key_len = key.len(), "cap_do_workers.forget");

            let key_b64 = encode_key(key)?;

            let request = DoRequest::Forget { key: key_b64 };

            match self.send_request(request).await? {
                DoResponse::StorageDelete { .. } => Ok(()),
                DoResponse::Error { message } => Err(DedupeError::Other(message)),
                other => Err(DedupeError::Other(format!(
                    "unexpected response: {other:?}"
                ))),
            }
        }

        async fn stub_for_key(&self, request: &DoRequest) -> Result<Stub, DoError> {
            let key = match request {
                DoRequest::PutIfAbsent { key, .. } | DoRequest::Forget { key } => Some(key),
                DoRequest::StorageGet { .. }
                | DoRequest::StoragePut { .. }
                | DoRequest::StorageDelete { .. }
                | DoRequest::StorageList { .. }
                | DoRequest::AlarmGet
                | DoRequest::AlarmSet { .. }
                | DoRequest::AlarmDelete
                | DoRequest::ProcessDueSchedules
                | DoRequest::SqlExec { .. }
                | DoRequest::Sequence { .. } => None,
            };

            if let Some(key) = key {
                let instance = instance_name_for_key(&self.scope, key);
                self.binding.stub_from_name(&instance)
            } else {
                self.binding.stub_from_name(&self.scope)
            }
        }
    }

    impl Capability for WorkersDurableObject {
        fn name(&self) -> &'static str {
            "dedupe.workers_do"
        }
    }

    #[async_trait(?Send)]
    impl DedupeStore for WorkersDurableObject {
        async fn put_if_absent(&self, key: &[u8], ttl: Duration) -> Result<bool, DedupeError> {
            self.put_if_absent_local(key, ttl).await
        }

        async fn forget(&self, key: &[u8]) -> Result<(), DedupeError> {
            self.forget_local(key).await
        }
    }

    #[async_trait(?Send)]
    impl CheckpointStore for WorkersDurableObject {
        async fn put(&self, record: CheckpointRecord) -> Result<CheckpointHandle, CheckpointError> {
            let key = checkpoint_key(&record.checkpoint_id);
            let ttl = record
                .ttl_ms
                .filter(|ttl_ms| *ttl_ms > 0)
                .map(Duration::from_millis);
            let value = serde_json::to_value(&record)
                .map(StorageValue::Json)
                .map_err(|err| CheckpointError::Storage(err.to_string()))?;
            self.storage_put(&key, value, ttl)
                .await
                .map_err(map_do_checkpoint_error)?;
            Ok(CheckpointHandle {
                checkpoint_id: record.checkpoint_id,
                flow_id: record.flow_id,
                run_id: record.run_id,
            })
        }

        async fn get(
            &self,
            handle: &CheckpointHandle,
        ) -> Result<CheckpointRecord, CheckpointError> {
            let key = checkpoint_key(&handle.checkpoint_id);
            self.read_checkpoint_record(&key)
                .await?
                .ok_or(CheckpointError::NotFound)
        }

        async fn ack(&self, handle: &CheckpointHandle) -> Result<(), CheckpointError> {
            let key = checkpoint_key(&handle.checkpoint_id);
            let deleted = self
                .storage_delete(&key)
                .await
                .map_err(map_do_checkpoint_error)?;
            let lease_key = checkpoint_lease_key(&handle.checkpoint_id);
            let _ = self
                .storage_delete(&lease_key)
                .await
                .map_err(map_do_checkpoint_error)?;
            if deleted {
                Ok(())
            } else {
                Err(CheckpointError::NotFound)
            }
        }

        async fn lease(
            &self,
            handle: &CheckpointHandle,
            ttl: Duration,
        ) -> Result<Lease, CheckpointError> {
            let key = checkpoint_key(&handle.checkpoint_id);
            if self.read_checkpoint_record(&key).await?.is_none() {
                return Err(CheckpointError::NotFound);
            }

            let lease_key = checkpoint_lease_key(&handle.checkpoint_id);
            let now_ms = now_ms_u64();
            if let Some(existing) = self.read_checkpoint_lease(&lease_key).await? {
                if existing.expires_at_ms > now_ms {
                    return Err(CheckpointError::LeaseConflict);
                }
            }

            let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1);
            let expires_at_ms = now_ms.saturating_add(ttl_ms);
            let lease = StoredLease {
                lease_id: format!("lease:{}:{expires_at_ms}", handle.checkpoint_id),
                expires_at_ms,
            };
            self.put_checkpoint_lease(&lease_key, &lease).await?;
            Ok(Lease {
                lease_id: lease.lease_id,
                expires_at_ms,
            })
        }

        async fn release_lease(&self, lease: Lease) -> Result<(), CheckpointError> {
            let keys = self
                .storage_list(StorageListOptions::default().with_prefix(CHECKPOINT_LEASE_PREFIX))
                .await
                .map_err(map_do_checkpoint_error)?;
            for key in keys {
                let Some(stored) = self.read_checkpoint_lease(&key).await? else {
                    continue;
                };
                if stored.lease_id == lease.lease_id {
                    let _ = self
                        .storage_delete(&key)
                        .await
                        .map_err(map_do_checkpoint_error)?;
                    break;
                }
            }
            Ok(())
        }

        async fn list(
            &self,
            filter: CheckpointFilter,
        ) -> Result<Vec<CheckpointHandle>, CheckpointError> {
            let keys = self
                .storage_list(StorageListOptions::default().with_prefix(CHECKPOINT_KEY_PREFIX))
                .await
                .map_err(map_do_checkpoint_error)?;
            let now_ms = now_ms_u64();
            let mut handles = Vec::new();

            for key in keys {
                let Some(record) = self.read_checkpoint_record(&key).await? else {
                    continue;
                };

                if let Some(flow_id) = &filter.flow_id {
                    if &record.flow_id != flow_id {
                        continue;
                    }
                }
                if let Some(run_id) = &filter.run_id {
                    if &record.run_id != run_id {
                        continue;
                    }
                }

                if let Some(status) = filter.status {
                    let is_expired = record
                        .ttl_ms
                        .map(|ttl_ms| record.created_at_ms.saturating_add(ttl_ms) <= now_ms)
                        .unwrap_or(false);
                    match status {
                        capabilities::durability::CheckpointStatus::Active if is_expired => {
                            continue;
                        }
                        capabilities::durability::CheckpointStatus::Expired if !is_expired => {
                            continue;
                        }
                        capabilities::durability::CheckpointStatus::Completed => continue,
                        _ => {}
                    }
                }

                handles.push(CheckpointHandle {
                    checkpoint_id: record.checkpoint_id,
                    flow_id: record.flow_id,
                    run_id: record.run_id,
                });
            }

            Ok(handles)
        }
    }

    #[async_trait(?Send)]
    impl ResumeScheduler for WorkersDurableObject {
        async fn schedule_at(
            &self,
            handle: CheckpointHandle,
            at_ms: u64,
        ) -> Result<ScheduleId, ScheduleError> {
            if at_ms == 0 {
                return Err(ScheduleError::InvalidDelay(
                    "schedule_at requires at_ms > 0".to_string(),
                ));
            }

            let schedule_id = format!(
                "sched:{}:{}:{}",
                handle.checkpoint_id,
                at_ms,
                (js_sys::Math::random() * 1_000_000_000.0) as u64
            );
            let key = schedule_key(&schedule_id);
            let schedule = StoredSchedule {
                checkpoint_handle: handle,
                fires_at_ms: at_ms,
                status: StoredScheduleStatus::Pending,
            };
            self.put_schedule(&key, &schedule).await?;
            self.refresh_schedule_alarm().await?;
            Ok(ScheduleId(schedule_id))
        }

        async fn schedule_after(
            &self,
            handle: CheckpointHandle,
            delay: Duration,
        ) -> Result<ScheduleId, ScheduleError> {
            if delay.is_zero() {
                return Err(ScheduleError::InvalidDelay(
                    "schedule_after requires non-zero delay".to_string(),
                ));
            }
            let delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
            let at_ms = now_ms_u64().saturating_add(delay_ms.max(1));
            self.schedule_at(handle, at_ms).await
        }

        async fn cancel(&self, schedule_id: ScheduleId) -> Result<(), ScheduleError> {
            let key = schedule_key(&schedule_id.0);
            let mut schedule = self
                .read_schedule(&key)
                .await?
                .ok_or(ScheduleError::NotFound)?;
            schedule.status = StoredScheduleStatus::Cancelled;
            self.put_schedule(&key, &schedule).await?;
            self.refresh_schedule_alarm().await?;
            Ok(())
        }

        async fn status(&self, schedule_id: ScheduleId) -> Result<ScheduleStatus, ScheduleError> {
            let key = schedule_key(&schedule_id.0);
            let mut schedule = self
                .read_schedule(&key)
                .await?
                .ok_or(ScheduleError::NotFound)?;
            if matches!(schedule.status, StoredScheduleStatus::Pending)
                && schedule.fires_at_ms <= now_ms_u64()
            {
                schedule.status = StoredScheduleStatus::Fired {
                    fired_at_ms: now_ms_u64(),
                };
                self.put_schedule(&key, &schedule).await?;
                self.refresh_schedule_alarm().await?;
            }

            match schedule.status {
                StoredScheduleStatus::Pending => Ok(ScheduleStatus::Pending {
                    fires_at_ms: schedule.fires_at_ms,
                }),
                StoredScheduleStatus::Fired { fired_at_ms } => {
                    Ok(ScheduleStatus::Fired { fired_at_ms })
                }
                StoredScheduleStatus::Cancelled => Ok(ScheduleStatus::Cancelled),
            }
        }
    }

    #[async_trait(?Send)]
    impl ResumeSignalSource for WorkersDurableObject {
        async fn create_token(
            &self,
            handle: &CheckpointHandle,
            config: TokenConfig,
        ) -> Result<ResumeToken, TokenError> {
            let token = format!(
                "rt:{}:{}",
                now_ms_u64(),
                (js_sys::Math::random() * 1_000_000_000.0) as u64
            );
            let key = resume_token_key(&token);
            let expires_at_ms = config.ttl.map(|ttl| {
                now_ms_u64().saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
            });
            let stored = StoredToken {
                checkpoint_handle: handle.clone(),
                expires_at_ms,
                single_use: config.single_use,
                used: false,
                metadata: config.metadata,
            };
            self.put_token(&key, &stored).await?;
            Ok(ResumeToken(token))
        }

        async fn resolve_token(&self, token: &ResumeToken) -> Result<CheckpointHandle, TokenError> {
            let key = resume_token_key(&token.0);
            let mut stored = self.read_token(&key).await?.ok_or(TokenError::NotFound)?;

            if let Some(expires_at_ms) = stored.expires_at_ms {
                if expires_at_ms <= now_ms_u64() {
                    let _ = self
                        .storage_delete(&key)
                        .await
                        .map_err(map_do_token_error)?;
                    return Err(TokenError::NotFound);
                }
            }

            if stored.single_use {
                if stored.used {
                    return Err(TokenError::AlreadyUsed);
                }
                stored.used = true;
                self.put_token(&key, &stored).await?;
            }

            Ok(stored.checkpoint_handle)
        }

        async fn revoke_token(&self, token: &ResumeToken) -> Result<(), TokenError> {
            let key = resume_token_key(&token.0);
            let deleted = self
                .storage_delete(&key)
                .await
                .map_err(map_do_token_error)?;
            if deleted {
                Ok(())
            } else {
                Err(TokenError::NotFound)
            }
        }
    }

    /// Base64-encode and validate key transport size.
    fn encode_key(data: &[u8]) -> Result<String, DoError> {
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data);
        validate_dedupe_key_len(encoded.len())?;
        Ok(encoded)
    }

    fn instance_name_for_key(scope: &str, key_b64: &str) -> String {
        if scope.is_empty() {
            key_b64.to_string()
        } else {
            format!("{scope}:{key_b64}")
        }
    }

    fn now_ms() -> i64 {
        js_sys::Date::now() as i64
    }

    fn date_from_ms(ms: i64) -> js_sys::Date {
        js_sys::Date::new(&JsValue::from_f64(ms as f64))
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Tests (run with wasm-bindgen-test)
    // ─────────────────────────────────────────────────────────────────────────────

    #[cfg(test)]
    mod tests {
        use super::*;
        use wasm_bindgen_test::wasm_bindgen_test;

        #[wasm_bindgen_test]
        fn validate_key_accepts_valid_key() {
            let max_encoded_len = MAX_KEY_SIZE.saturating_sub(DEDUPE_KEY_PREFIX.len());
            let key = vec![0u8; (max_encoded_len / 4) * 3];
            assert!(WorkersDurableObject::validate_key(&key).is_ok());
        }

        #[wasm_bindgen_test]
        fn validate_key_rejects_oversized_key() {
            let max_encoded_len = MAX_KEY_SIZE.saturating_sub(DEDUPE_KEY_PREFIX.len());
            let key = vec![0u8; (max_encoded_len / 4) * 3 + 1];
            let err = WorkersDurableObject::validate_key(&key).unwrap_err();
            assert!(matches!(err, DoError::KeyTooLarge(_)));
        }

        #[wasm_bindgen_test]
        fn do_request_serializes_correctly() {
            let req = DoRequest::PutIfAbsent {
                key: "dGVzdA==".to_string(),
                ttl_secs: 3600,
            };
            let json = serde_json::to_string(&req).unwrap();
            assert!(json.contains("put_if_absent"));
            assert!(json.contains("ttl_secs"));
        }

        #[wasm_bindgen_test]
        fn do_response_deserializes_ok() {
            let json = r#"{"status":"put_if_absent","inserted":true}"#;
            let resp: DoResponse = serde_json::from_str(json).unwrap();
            assert!(matches!(resp, DoResponse::PutIfAbsent { inserted: true }));
        }

        #[wasm_bindgen_test]
        fn do_response_deserializes_error() {
            let json = r#"{"status":"error","message":"key not found"}"#;
            let resp: DoResponse = serde_json::from_str(json).unwrap();
            assert!(matches!(resp, DoResponse::Error { .. }));
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm::{DurableObjectBinding, FlowDurableObject, WorkersDurableObject};

// ─────────────────────────────────────────────────────────────────────────────
// Non-WASM32 Stub (for cargo check on host)
// ─────────────────────────────────────────────────────────────────────────────

/// Stub implementation for non-wasm32 targets.
///
/// This allows the crate to compile on native targets for testing and
/// documentation purposes, but all operations will return errors.
#[cfg(not(target_arch = "wasm32"))]
pub struct WorkersDurableObject {
    scope: String,
}

#[cfg(not(target_arch = "wasm32"))]
impl WorkersDurableObject {
    /// Create a stub instance (only for type-checking on native targets).
    pub fn new_stub(scope: impl Into<String>) -> Self {
        dedupe::ensure_registered();
        Self {
            scope: scope.into(),
        }
    }

    /// Get the scope name.
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// Validate key size.
    pub fn validate_key(key: &[u8]) -> Result<(), DoError> {
        let encoded_len = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(key)
            .len();
        validate_dedupe_key_len(encoded_len)
    }

    pub async fn storage_get(&self, _key: &str) -> Result<Option<StorageValue>, DoError> {
        Err(DoError::RuntimeUnavailable)
    }

    pub async fn storage_put(
        &self,
        _key: &str,
        _value: StorageValue,
        _ttl: Option<Duration>,
    ) -> Result<(), DoError> {
        Err(DoError::RuntimeUnavailable)
    }

    pub async fn storage_delete(&self, _key: &str) -> Result<bool, DoError> {
        Err(DoError::RuntimeUnavailable)
    }

    pub async fn storage_list(&self, _options: StorageListOptions) -> Result<Vec<String>, DoError> {
        Err(DoError::RuntimeUnavailable)
    }

    pub async fn alarm_get(&self) -> Result<Option<i64>, DoError> {
        Err(DoError::RuntimeUnavailable)
    }

    pub async fn alarm_set(&self, _scheduled_ms: i64) -> Result<(), DoError> {
        Err(DoError::RuntimeUnavailable)
    }

    pub async fn alarm_delete(&self) -> Result<(), DoError> {
        Err(DoError::RuntimeUnavailable)
    }

    pub async fn sql_exec_json(
        &self,
        _query: impl Into<String>,
        _bindings: Vec<SqlValue>,
    ) -> Result<Vec<JsonValue>, DoError> {
        Err(DoError::RuntimeUnavailable)
    }

    pub async fn sql_exec_raw(
        &self,
        _query: impl Into<String>,
        _bindings: Vec<SqlValue>,
    ) -> Result<Vec<Vec<SqlValue>>, DoError> {
        Err(DoError::RuntimeUnavailable)
    }

    pub async fn sequence(
        &self,
        _idempotency_key: impl Into<String>,
        _payload_hash: impl Into<String>,
        _ttl: Duration,
        _now_ms: Option<i64>,
    ) -> Result<SequenceResult, DoError> {
        Err(DoError::RuntimeUnavailable)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Capability for WorkersDurableObject {
    fn name(&self) -> &'static str {
        "dedupe.workers_do"
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl DedupeStore for WorkersDurableObject {
    async fn put_if_absent(&self, _key: &[u8], _ttl: Duration) -> Result<bool, DedupeError> {
        Err(DedupeError::RuntimeUnavailable)
    }

    async fn forget(&self, _key: &[u8]) -> Result<(), DedupeError> {
        Err(DedupeError::RuntimeUnavailable)
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl CheckpointStore for WorkersDurableObject {
    async fn put(&self, _record: CheckpointRecord) -> Result<CheckpointHandle, CheckpointError> {
        Err(CheckpointError::Storage(
            "durability runtime unavailable".to_string(),
        ))
    }

    async fn get(&self, _handle: &CheckpointHandle) -> Result<CheckpointRecord, CheckpointError> {
        Err(CheckpointError::Storage(
            "durability runtime unavailable".to_string(),
        ))
    }

    async fn ack(&self, _handle: &CheckpointHandle) -> Result<(), CheckpointError> {
        Err(CheckpointError::Storage(
            "durability runtime unavailable".to_string(),
        ))
    }

    async fn lease(
        &self,
        _handle: &CheckpointHandle,
        _ttl: Duration,
    ) -> Result<Lease, CheckpointError> {
        Err(CheckpointError::Storage(
            "durability runtime unavailable".to_string(),
        ))
    }

    async fn release_lease(&self, _lease: Lease) -> Result<(), CheckpointError> {
        Err(CheckpointError::Storage(
            "durability runtime unavailable".to_string(),
        ))
    }

    async fn list(
        &self,
        _filter: CheckpointFilter,
    ) -> Result<Vec<CheckpointHandle>, CheckpointError> {
        Err(CheckpointError::Storage(
            "durability runtime unavailable".to_string(),
        ))
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl ResumeScheduler for WorkersDurableObject {
    async fn schedule_at(
        &self,
        _handle: CheckpointHandle,
        _at_ms: u64,
    ) -> Result<ScheduleId, ScheduleError> {
        Err(ScheduleError::Unavailable(
            "durability runtime unavailable".to_string(),
        ))
    }

    async fn schedule_after(
        &self,
        _handle: CheckpointHandle,
        _delay: Duration,
    ) -> Result<ScheduleId, ScheduleError> {
        Err(ScheduleError::Unavailable(
            "durability runtime unavailable".to_string(),
        ))
    }

    async fn cancel(&self, _schedule_id: ScheduleId) -> Result<(), ScheduleError> {
        Err(ScheduleError::Unavailable(
            "durability runtime unavailable".to_string(),
        ))
    }

    async fn status(&self, _schedule_id: ScheduleId) -> Result<ScheduleStatus, ScheduleError> {
        Err(ScheduleError::Unavailable(
            "durability runtime unavailable".to_string(),
        ))
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl ResumeSignalSource for WorkersDurableObject {
    async fn create_token(
        &self,
        _handle: &CheckpointHandle,
        _config: TokenConfig,
    ) -> Result<ResumeToken, TokenError> {
        Err(TokenError::Generation(
            "durability runtime unavailable".to_string(),
        ))
    }

    async fn resolve_token(&self, _token: &ResumeToken) -> Result<CheckpointHandle, TokenError> {
        Err(TokenError::Generation(
            "durability runtime unavailable".to_string(),
        ))
    }

    async fn revoke_token(&self, _token: &ResumeToken) -> Result<(), TokenError> {
        Err(TokenError::Generation(
            "durability runtime unavailable".to_string(),
        ))
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_key_accepts_valid_key() {
        let max_encoded_len = MAX_KEY_SIZE.saturating_sub(DEDUPE_KEY_PREFIX.len());
        let key = vec![0u8; (max_encoded_len / 4) * 3];
        assert!(WorkersDurableObject::validate_key(&key).is_ok());
    }

    #[test]
    fn validate_key_rejects_oversized_key() {
        let max_encoded_len = MAX_KEY_SIZE.saturating_sub(DEDUPE_KEY_PREFIX.len());
        let key = vec![0u8; (max_encoded_len / 4) * 3 + 1];
        let err = WorkersDurableObject::validate_key(&key).unwrap_err();
        assert!(matches!(err, DoError::KeyTooLarge(_)));
    }

    #[test]
    fn stub_returns_runtime_unavailable() {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let dedupe = WorkersDurableObject::new_stub("test-scope");

        rt.block_on(async {
            let result = dedupe.put_if_absent(b"key", Duration::from_secs(60)).await;
            assert!(matches!(result, Err(DedupeError::RuntimeUnavailable)));

            let result = dedupe.forget(b"key").await;
            assert!(matches!(result, Err(DedupeError::RuntimeUnavailable)));
        });
    }

    #[test]
    fn capability_name_is_correct() {
        let dedupe = WorkersDurableObject::new_stub("test");
        assert_eq!(dedupe.name(), "dedupe.workers_do");
    }
}
