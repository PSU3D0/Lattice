use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use js_sys::JsString;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use worker::durable_object;
use worker::{Bucket, Env, Method, Request, RequestInit, Response, SqlStorageValue, State};

use crate::{
    WorkersWorkspaceConfig, WorkersWorkspaceError, WorkspaceEntryMeta, WorkspaceWriteReservation,
    duration_to_millis, is_blocked_path, normalize_blocked_prefix, now_ms, path_depth,
    prefix_upper_bound, trim_slashes, workspace_object_key, workspace_scope_name,
};
use capabilities::Capability;
use capabilities::workspace::{
    Workspace, WorkspaceCompletionDisposition, WorkspaceDeleteResult, WorkspaceEntry,
    WorkspaceError, WorkspaceFactory, WorkspaceListOptions, WorkspacePolicy, WorkspaceReadResult,
    WorkspaceRunScope, WorkspaceWriteOptions, WorkspaceWriteResult,
};

const STATE_KEY: &str = "workspace:state";
const SCHEMA_VERSION: u32 = 2;
const BODY_KIND_FILE: &str = "file";
const DO_URL: &str = "http://do/workspace";
const DEBUG_RUN_RETAINED_CLEANUP_PATH: &str = "/__debug/run-retained-cleanup";
const DEBUG_SCHEMA_VERSION_PATH: &str = "/__debug/schema-version";
const DEBUG_SEED_LEGACY_STATE_PATH: &str = "/__debug/seed-legacy-state";

#[derive(Debug, Clone)]
struct EnvHandle(Env);

// SAFETY: Cloudflare Workers runs on a single-threaded wasm event loop.
unsafe impl Send for EnvHandle {}
// SAFETY: Cloudflare Workers runs on a single-threaded wasm event loop.
unsafe impl Sync for EnvHandle {}

#[derive(Debug, Clone)]
struct BucketHandle(Bucket);

// SAFETY: Cloudflare Workers runs on a single-threaded wasm event loop.
unsafe impl Send for BucketHandle {}
// SAFETY: Cloudflare Workers runs on a single-threaded wasm event loop.
unsafe impl Sync for BucketHandle {}

#[derive(Debug, Clone)]
pub struct WorkersWorkspaceFactory {
    env: EnvHandle,
    config: WorkersWorkspaceConfig,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WorkersWorkspace {
    scope: WorkspaceRunScope,
    policy: WorkspacePolicy,
    bucket: BucketHandle,
    index: WorkspaceIndexClient,
}

#[derive(Debug, Clone)]
pub struct WorkspaceIndexClient {
    env: EnvHandle,
    index_binding: String,
    scope: WorkspaceRunScope,
    bucket_binding: String,
    object_prefix: String,
    policy: WorkspacePolicy,
    blocked_prefixes: Vec<String>,
    max_path_depth: Option<u32>,
    max_path_length: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkspaceDoEnvelope {
    scope: WorkspaceRunScope,
    bucket_binding: String,
    object_prefix: String,
    policy: WorkspacePolicy,
    request: WorkspaceDoRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum WorkspaceDoRequest {
    Stat {
        path: String,
    },
    List {
        prefix: Option<String>,
    },
    PrepareWrite {
        path: String,
        new_size_bytes: u64,
        updated_at_ms: u64,
        content_hash: Option<String>,
    },
    CommitWrite {
        entry: WorkspaceEntryMeta,
    },
    Delete {
        path: String,
    },
    Complete {
        disposition: WorkspaceCompletionDisposition,
        retain_completed_for_ms: Option<u64>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WorkspaceDoResponse {
    Stat {
        entry: Option<WorkspaceEntryMeta>,
    },
    List {
        entries: Vec<WorkspaceEntryMeta>,
    },
    PrepareWrite {
        reservation: WorkspaceWriteReservation,
    },
    CommitWrite,
    Delete {
        deleted: bool,
        object_key: Option<String>,
    },
    Complete,
    DebugSchemaVersion {
        schema_version: u32,
    },
    DebugAck,
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyWorkspaceDoState {
    scope: WorkspaceRunScope,
    bucket_binding: String,
    object_prefix: String,
    policy: WorkspacePolicy,
    total_bytes: u64,
    file_count: u64,
    completed_at_ms: Option<u64>,
    retain_until_ms: Option<u64>,
    entries: BTreeMap<String, WorkspaceEntryMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkspaceStateRow {
    singleton: i64,
    schema_version: i64,
    flow_id: String,
    run_id: String,
    bucket_binding: String,
    object_prefix: String,
    max_total_bytes: Option<i64>,
    max_file_count: Option<i64>,
    max_single_file_bytes: Option<i64>,
    retain_completed_for_ms: Option<i64>,
    total_bytes: i64,
    file_count: i64,
    completed_at_ms: Option<i64>,
    retain_until_ms: Option<i64>,
}

#[derive(Debug, Clone)]
struct WorkspaceStateRecord {
    scope: WorkspaceRunScope,
    bucket_binding: String,
    object_prefix: String,
    policy: WorkspacePolicy,
    total_bytes: u64,
    file_count: u64,
    completed_at_ms: Option<u64>,
    retain_until_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkspaceEntryRow {
    path: String,
    object_key: String,
    size_bytes: i64,
    updated_at_ms: i64,
    content_hash: Option<String>,
    body_kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DebugRunRetainedCleanupBody {
    #[serde(default)]
    now_ms: Option<u64>,
}

impl WorkspaceStateRow {
    fn into_record(self) -> worker::Result<WorkspaceStateRecord> {
        Ok(WorkspaceStateRecord {
            scope: WorkspaceRunScope {
                flow_id: self.flow_id,
                run_id: self.run_id,
            },
            bucket_binding: self.bucket_binding,
            object_prefix: self.object_prefix,
            policy: WorkspacePolicy {
                max_total_bytes: opt_i64_to_u64(self.max_total_bytes, "max_total_bytes")?,
                max_file_count: opt_i64_to_u64(self.max_file_count, "max_file_count")?,
                max_single_file_bytes: opt_i64_to_u64(
                    self.max_single_file_bytes,
                    "max_single_file_bytes",
                )?,
                retain_completed_for: opt_i64_to_u64(
                    self.retain_completed_for_ms,
                    "retain_completed_for_ms",
                )?
                .map(std::time::Duration::from_millis),
            },
            total_bytes: i64_to_u64(self.total_bytes, "total_bytes")?,
            file_count: i64_to_u64(self.file_count, "file_count")?,
            completed_at_ms: opt_i64_to_u64(self.completed_at_ms, "completed_at_ms")?,
            retain_until_ms: opt_i64_to_u64(self.retain_until_ms, "retain_until_ms")?,
        })
    }
}

impl WorkspaceEntryRow {
    fn from_meta(meta: &WorkspaceEntryMeta) -> worker::Result<Self> {
        Ok(Self {
            path: meta.path.clone(),
            object_key: meta.object_key.clone(),
            size_bytes: u64_to_i64(meta.size_bytes, "size_bytes")?,
            updated_at_ms: u64_to_i64(meta.updated_at_ms, "updated_at_ms")?,
            content_hash: meta.content_hash.clone(),
            body_kind: BODY_KIND_FILE.to_string(),
        })
    }

    fn into_meta(self) -> worker::Result<WorkspaceEntryMeta> {
        Ok(WorkspaceEntryMeta {
            path: self.path,
            object_key: self.object_key,
            size_bytes: i64_to_u64(self.size_bytes, "size_bytes")?,
            updated_at_ms: i64_to_u64(self.updated_at_ms, "updated_at_ms")?,
            content_hash: self.content_hash,
        })
    }
}

impl WorkersWorkspaceFactory {
    pub fn new(env: Env, mut config: WorkersWorkspaceConfig) -> Self {
        config.blocked_prefixes = config
            .blocked_prefixes
            .iter()
            .filter_map(|prefix| normalize_blocked_prefix(prefix))
            .collect();
        Self {
            env: EnvHandle(env),
            config,
        }
    }

    pub fn config(&self) -> &WorkersWorkspaceConfig {
        &self.config
    }

    fn index_client(&self, scope: WorkspaceRunScope) -> WorkspaceIndexClient {
        WorkspaceIndexClient {
            env: self.env.clone(),
            index_binding: self.config.index_binding.clone(),
            scope,
            bucket_binding: self.config.bucket_binding.clone(),
            object_prefix: self.config.object_prefix.clone(),
            policy: self.config.policy.clone(),
            blocked_prefixes: self.config.blocked_prefixes.clone(),
            max_path_depth: self.config.max_path_depth,
            max_path_length: self.config.max_path_length,
        }
    }
}

#[async_trait(?Send)]
impl WorkspaceFactory for WorkersWorkspaceFactory {
    async fn open(&self, scope: WorkspaceRunScope) -> anyhow::Result<Arc<dyn Workspace>> {
        let bucket = self
            .env
            .0
            .bucket(&self.config.bucket_binding)
            .map_err(|err| anyhow::anyhow!(WorkersWorkspaceError::Backend(err.to_string())))?;
        let workspace = WorkersWorkspace {
            scope: scope.clone(),
            policy: self.config.policy.clone(),
            bucket: BucketHandle(bucket),
            index: self.index_client(scope),
        };
        Ok(Arc::new(workspace))
    }

    async fn complete(
        &self,
        scope: WorkspaceRunScope,
        disposition: WorkspaceCompletionDisposition,
    ) -> anyhow::Result<()> {
        let retain_completed_for_ms = self
            .config
            .policy
            .retain_completed_for
            .map(duration_to_millis);
        self.index_client(scope)
            .complete(disposition, retain_completed_for_ms)
            .await
            .map_err(anyhow::Error::from)
    }
}

impl Capability for WorkersWorkspace {
    fn name(&self) -> &'static str {
        "workspace.workers"
    }
}

#[async_trait(?Send)]
impl Workspace for WorkersWorkspace {
    async fn read_normalized(
        &self,
        normalized_path: &str,
    ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
        self.index.validate_path(normalized_path)?;
        let Some(entry) = self.index.stat(normalized_path).await? else {
            return Ok(None);
        };
        let Some(object) = self
            .bucket
            .0
            .get(entry.object_key.clone())
            .execute()
            .await
            .map_err(|err| WorkspaceError::Backend(err.to_string()))?
        else {
            return Ok(None);
        };
        let body = object.body().ok_or_else(|| {
            WorkspaceError::Backend(format!(
                "workspace object {} has no readable body",
                entry.object_key
            ))
        })?;
        let bytes = body
            .bytes()
            .await
            .map_err(|err| WorkspaceError::Backend(err.to_string()))?;
        Ok(Some(WorkspaceReadResult::Bytes(bytes)))
    }

    async fn write_normalized(
        &self,
        normalized_path: &str,
        data: &[u8],
        _options: WorkspaceWriteOptions,
    ) -> Result<WorkspaceWriteResult, WorkspaceError> {
        self.index.validate_path(normalized_path)?;
        let updated_at_ms = now_ms();
        let reservation = self
            .index
            .prepare_write(normalized_path, data.len() as u64, updated_at_ms, None)
            .await?;
        self.bucket
            .0
            .put(reservation.entry.object_key.clone(), data.to_vec())
            .execute()
            .await
            .map_err(|err| WorkspaceError::Backend(err.to_string()))?;
        self.index.commit_write(reservation.entry.clone()).await?;
        Ok(WorkspaceWriteResult {
            path: reservation.entry.path,
            size_bytes: reservation.entry.size_bytes,
            updated_at_ms: reservation.entry.updated_at_ms,
        })
    }

    async fn list_normalized(
        &self,
        options: WorkspaceListOptions,
    ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
        if let Some(prefix) = options.prefix.as_deref() {
            self.index.validate_prefix(prefix)?;
        }
        let entries = self.index.list(options.prefix.as_deref()).await?;
        Ok(entries
            .into_iter()
            .map(|entry| entry.to_workspace_entry())
            .collect())
    }

    async fn delete_normalized(
        &self,
        normalized_path: &str,
    ) -> Result<WorkspaceDeleteResult, WorkspaceError> {
        self.index.validate_path(normalized_path)?;
        let (deleted, object_key) = self.index.delete(normalized_path).await?;
        if let Some(object_key) = object_key {
            self.bucket
                .0
                .delete(object_key)
                .await
                .map_err(|err| WorkspaceError::Backend(err.to_string()))?;
        }
        Ok(WorkspaceDeleteResult { deleted })
    }
}

impl WorkspaceIndexClient {
    fn envelope(&self, request: WorkspaceDoRequest) -> WorkspaceDoEnvelope {
        WorkspaceDoEnvelope {
            scope: self.scope.clone(),
            bucket_binding: self.bucket_binding.clone(),
            object_prefix: self.object_prefix.clone(),
            policy: self.policy.clone(),
            request,
        }
    }

    fn validate_path(&self, normalized_path: &str) -> Result<(), WorkspaceError> {
        if is_blocked_path(normalized_path, &self.blocked_prefixes) {
            return Err(WorkspaceError::InvalidPath(format!(
                "workspace path is blocked by host policy: {normalized_path}"
            )));
        }
        if let Some(max_depth) = self.max_path_depth {
            let depth = path_depth(normalized_path);
            if depth > max_depth {
                return Err(WorkspaceError::InvalidPath(format!(
                    "workspace path exceeds max depth ({depth} > {max_depth})"
                )));
            }
        }
        if let Some(max_length) = self.max_path_length {
            let length = normalized_path.len() as u32;
            if length > max_length {
                return Err(WorkspaceError::InvalidPath(format!(
                    "workspace path exceeds max length ({length} > {max_length})"
                )));
            }
        }
        Ok(())
    }

    fn validate_prefix(&self, prefix: &str) -> Result<(), WorkspaceError> {
        self.validate_path(prefix)
    }

    async fn request(
        &self,
        request: WorkspaceDoRequest,
    ) -> Result<WorkspaceDoResponse, WorkersWorkspaceError> {
        let namespace = self
            .env
            .0
            .durable_object(&self.index_binding)
            .map_err(|err| WorkersWorkspaceError::Backend(err.to_string()))?;
        let id = namespace
            .id_from_name(&workspace_scope_name(&self.scope))
            .map_err(|err| WorkersWorkspaceError::Backend(err.to_string()))?;
        let stub = id
            .get_stub()
            .map_err(|err| WorkersWorkspaceError::Backend(err.to_string()))?;
        let payload = serde_json::to_string(&self.envelope(request))?;
        let mut init = RequestInit::new();
        init.with_method(Method::Post);
        init.with_body(Some(JsString::from(payload).into()));
        let req = Request::new_with_init(DO_URL, &init)
            .map_err(|err| WorkersWorkspaceError::Backend(err.to_string()))?;
        let mut resp = stub
            .fetch_with_request(req)
            .await
            .map_err(|err| WorkersWorkspaceError::Backend(err.to_string()))?;
        resp.json()
            .await
            .map_err(|err| WorkersWorkspaceError::Backend(err.to_string()))
    }

    async fn stat(&self, path: &str) -> Result<Option<WorkspaceEntryMeta>, WorkspaceError> {
        match self
            .request(WorkspaceDoRequest::Stat {
                path: path.to_string(),
            })
            .await
            .map_err(map_workspace_protocol)?
        {
            WorkspaceDoResponse::Stat { entry } => Ok(entry),
            WorkspaceDoResponse::Error { message } => Err(WorkspaceError::Backend(message)),
            response => Err(unexpected_response("stat", response)),
        }
    }

    async fn list(&self, prefix: Option<&str>) -> Result<Vec<WorkspaceEntryMeta>, WorkspaceError> {
        match self
            .request(WorkspaceDoRequest::List {
                prefix: prefix.map(ToOwned::to_owned),
            })
            .await
            .map_err(map_workspace_protocol)?
        {
            WorkspaceDoResponse::List { entries } => Ok(entries),
            WorkspaceDoResponse::Error { message } => Err(WorkspaceError::Backend(message)),
            response => Err(unexpected_response("list", response)),
        }
    }

    async fn prepare_write(
        &self,
        path: &str,
        new_size_bytes: u64,
        updated_at_ms: u64,
        content_hash: Option<String>,
    ) -> Result<WorkspaceWriteReservation, WorkspaceError> {
        match self
            .request(WorkspaceDoRequest::PrepareWrite {
                path: path.to_string(),
                new_size_bytes,
                updated_at_ms,
                content_hash,
            })
            .await
            .map_err(map_workspace_protocol)?
        {
            WorkspaceDoResponse::PrepareWrite { reservation } => Ok(reservation),
            WorkspaceDoResponse::Error { message } => Err(WorkspaceError::Backend(message)),
            response => Err(unexpected_response("prepare_write", response)),
        }
    }

    async fn commit_write(&self, entry: WorkspaceEntryMeta) -> Result<(), WorkspaceError> {
        match self
            .request(WorkspaceDoRequest::CommitWrite { entry })
            .await
            .map_err(map_workspace_protocol)?
        {
            WorkspaceDoResponse::CommitWrite => Ok(()),
            WorkspaceDoResponse::Error { message } => Err(WorkspaceError::Backend(message)),
            response => Err(unexpected_response("commit_write", response)),
        }
    }

    async fn delete(&self, path: &str) -> Result<(bool, Option<String>), WorkspaceError> {
        match self
            .request(WorkspaceDoRequest::Delete {
                path: path.to_string(),
            })
            .await
            .map_err(map_workspace_protocol)?
        {
            WorkspaceDoResponse::Delete {
                deleted,
                object_key,
            } => Ok((deleted, object_key)),
            WorkspaceDoResponse::Error { message } => Err(WorkspaceError::Backend(message)),
            response => Err(unexpected_response("delete", response)),
        }
    }

    async fn complete(
        &self,
        disposition: WorkspaceCompletionDisposition,
        retain_completed_for_ms: Option<u64>,
    ) -> Result<(), WorkersWorkspaceError> {
        match self
            .request(WorkspaceDoRequest::Complete {
                disposition,
                retain_completed_for_ms,
            })
            .await?
        {
            WorkspaceDoResponse::Complete => Ok(()),
            WorkspaceDoResponse::Error { message } => Err(WorkersWorkspaceError::Protocol(message)),
            response => Err(WorkersWorkspaceError::Protocol(format!(
                "unexpected complete response: {:?}",
                response
            ))),
        }
    }
}

#[durable_object]
pub struct WorkspaceDurableObject {
    state: State,
    env: EnvHandle,
}

impl worker::DurableObject for WorkspaceDurableObject {
    fn new(state: State, env: Env) -> Self {
        Self {
            state,
            env: EnvHandle(env),
        }
    }

    async fn fetch(&self, mut req: Request) -> worker::Result<Response> {
        let path = req.path();
        if path == DEBUG_RUN_RETAINED_CLEANUP_PATH {
            let body: DebugRunRetainedCleanupBody = req.json().await?;
            return self.debug_run_retained_cleanup(body.now_ms).await;
        }
        if path == DEBUG_SCHEMA_VERSION_PATH {
            return self.debug_schema_version().await;
        }
        if path == DEBUG_SEED_LEGACY_STATE_PATH {
            let legacy: LegacyWorkspaceDoState = req.json().await?;
            return self.debug_seed_legacy_state(legacy).await;
        }

        let envelope: WorkspaceDoEnvelope = match req.json().await {
            Ok(envelope) => envelope,
            Err(err) => {
                return Response::from_json(&WorkspaceDoResponse::Error {
                    message: err.to_string(),
                });
            }
        };
        let response = match self.handle_request(envelope).await {
            Ok(response) => response,
            Err(err) => WorkspaceDoResponse::Error {
                message: err.to_string(),
            },
        };
        Response::from_json(&response)
    }

    async fn alarm(&self) -> worker::Result<Response> {
        self.run_retained_cleanup(now_ms()).await?;
        Response::ok("ok")
    }
}

impl WorkspaceDurableObject {
    async fn handle_request(
        &self,
        envelope: WorkspaceDoEnvelope,
    ) -> worker::Result<WorkspaceDoResponse> {
        let state = self.ensure_store(&envelope).await?;
        match envelope.request {
            WorkspaceDoRequest::Stat { path } => {
                let entry = self
                    .load_entry(&path)?
                    .map(|row| row.into_meta())
                    .transpose()?;
                Ok(WorkspaceDoResponse::Stat { entry })
            }
            WorkspaceDoRequest::List { prefix } => {
                let entries = self
                    .list_entries(prefix.as_deref())?
                    .into_iter()
                    .map(|row| row.into_meta())
                    .collect::<worker::Result<Vec<_>>>()?;
                Ok(WorkspaceDoResponse::List { entries })
            }
            WorkspaceDoRequest::PrepareWrite {
                path,
                new_size_bytes,
                updated_at_ms,
                content_hash,
            } => {
                self.ensure_mutable(&state)?;
                let existing = self.load_entry(&path)?;
                self.enforce_write_policy(&state, existing.as_ref(), new_size_bytes)?;
                let reservation = WorkspaceWriteReservation {
                    entry: WorkspaceEntryMeta {
                        path: path.clone(),
                        object_key: workspace_object_key(
                            &state.object_prefix,
                            &state.scope.flow_id,
                            &state.scope.run_id,
                            &path,
                        ),
                        size_bytes: new_size_bytes,
                        updated_at_ms,
                        content_hash,
                    },
                };
                Ok(WorkspaceDoResponse::PrepareWrite { reservation })
            }
            WorkspaceDoRequest::CommitWrite { entry } => {
                self.ensure_mutable(&state)?;
                self.commit_write(&state, entry)?;
                Ok(WorkspaceDoResponse::CommitWrite)
            }
            WorkspaceDoRequest::Delete { path } => {
                self.ensure_mutable(&state)?;
                let result = self.delete_entry(&state, &path)?;
                Ok(WorkspaceDoResponse::Delete {
                    deleted: result.0,
                    object_key: result.1,
                })
            }
            WorkspaceDoRequest::Complete {
                disposition: _,
                retain_completed_for_ms,
            } => {
                self.complete_workspace(&state, retain_completed_for_ms)
                    .await?;
                Ok(WorkspaceDoResponse::Complete)
            }
        }
    }

    async fn debug_run_retained_cleanup(
        &self,
        now_ms_override: Option<u64>,
    ) -> worker::Result<Response> {
        self.run_retained_cleanup(now_ms_override.unwrap_or_else(now_ms))
            .await?;
        Response::from_json(&WorkspaceDoResponse::DebugAck)
    }

    async fn debug_schema_version(&self) -> worker::Result<Response> {
        self.ensure_schema()?;
        let schema_version = self
            .load_state()?
            .map(|row| row.schema_version)
            .unwrap_or(i64::from(SCHEMA_VERSION));
        Response::from_json(&WorkspaceDoResponse::DebugSchemaVersion {
            schema_version: i64_to_u32(schema_version, "schema_version")?,
        })
    }

    async fn debug_seed_legacy_state(
        &self,
        legacy: LegacyWorkspaceDoState,
    ) -> worker::Result<Response> {
        self.ensure_schema()?;
        self.sql_run("DELETE FROM workspace_entries", vec![])?;
        self.sql_run("DELETE FROM workspace_state", vec![])?;
        self.state.storage().delete_alarm().await?;
        self.state.storage().delete(STATE_KEY).await?;
        self.state.storage().put(STATE_KEY, legacy).await?;
        Response::from_json(&WorkspaceDoResponse::DebugAck)
    }

    async fn ensure_store(
        &self,
        envelope: &WorkspaceDoEnvelope,
    ) -> worker::Result<WorkspaceStateRecord> {
        self.ensure_schema()?;
        if let Some(row) = self.load_state()? {
            let state = row.into_record()?;
            self.validate_store(&state, envelope)?;
            return Ok(state);
        }

        if let Some(legacy) = self
            .state
            .storage()
            .get::<LegacyWorkspaceDoState>(STATE_KEY)
            .await?
        {
            self.migrate_legacy_state(legacy).await?;
            let state = self
                .load_state()?
                .ok_or_else(|| rust_error("workspace migration did not create state row"))?
                .into_record()?;
            self.validate_store(&state, envelope)?;
            return Ok(state);
        }

        self.insert_state(envelope, 0, 0, None, None)?;
        let state = self
            .load_state()?
            .ok_or_else(|| rust_error("workspace state row missing after insert"))?
            .into_record()?;
        Ok(state)
    }

    fn ensure_schema(&self) -> worker::Result<()> {
        self.sql_run(
            "CREATE TABLE IF NOT EXISTS workspace_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL,
                flow_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                bucket_binding TEXT NOT NULL,
                object_prefix TEXT NOT NULL,
                max_total_bytes INTEGER,
                max_file_count INTEGER,
                max_single_file_bytes INTEGER,
                retain_completed_for_ms INTEGER,
                total_bytes INTEGER NOT NULL DEFAULT 0,
                file_count INTEGER NOT NULL DEFAULT 0,
                completed_at_ms INTEGER,
                retain_until_ms INTEGER
            )",
            vec![],
        )?;
        self.sql_run(
            "CREATE TABLE IF NOT EXISTS workspace_entries (
                path TEXT PRIMARY KEY,
                object_key TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                content_hash TEXT,
                body_kind TEXT NOT NULL DEFAULT 'file'
            )",
            vec![],
        )?;
        self.sql_run(
            "CREATE INDEX IF NOT EXISTS idx_workspace_entries_updated_at ON workspace_entries(updated_at_ms)",
            vec![],
        )?;
        Ok(())
    }

    fn insert_state(
        &self,
        envelope: &WorkspaceDoEnvelope,
        total_bytes: u64,
        file_count: u64,
        completed_at_ms: Option<u64>,
        retain_until_ms: Option<u64>,
    ) -> worker::Result<()> {
        self.sql_run(
            "INSERT INTO workspace_state (
                singleton,
                schema_version,
                flow_id,
                run_id,
                bucket_binding,
                object_prefix,
                max_total_bytes,
                max_file_count,
                max_single_file_bytes,
                retain_completed_for_ms,
                total_bytes,
                file_count,
                completed_at_ms,
                retain_until_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            vec![
                js_i64(1)?,
                js_i64(i64::from(SCHEMA_VERSION))?,
                js_string(&envelope.scope.flow_id),
                js_string(&envelope.scope.run_id),
                js_string(&envelope.bucket_binding),
                js_string(trim_slashes(&envelope.object_prefix)),
                opt_js_i64(envelope.policy.max_total_bytes, "max_total_bytes")?,
                opt_js_i64(envelope.policy.max_file_count, "max_file_count")?,
                opt_js_i64(
                    envelope.policy.max_single_file_bytes,
                    "max_single_file_bytes",
                )?,
                opt_js_i64(
                    envelope.policy.retain_completed_for.map(duration_to_millis),
                    "retain_completed_for_ms",
                )?,
                js_i64(u64_to_i64(total_bytes, "total_bytes")?)?,
                js_i64(u64_to_i64(file_count, "file_count")?)?,
                opt_js_i64(completed_at_ms, "completed_at_ms")?,
                opt_js_i64(retain_until_ms, "retain_until_ms")?,
            ],
        )
    }

    async fn migrate_legacy_state(&self, legacy: LegacyWorkspaceDoState) -> worker::Result<()> {
        let envelope = WorkspaceDoEnvelope {
            scope: legacy.scope.clone(),
            bucket_binding: legacy.bucket_binding.clone(),
            object_prefix: legacy.object_prefix.clone(),
            policy: legacy.policy.clone(),
            request: WorkspaceDoRequest::List { prefix: None },
        };
        self.sql_run("DELETE FROM workspace_entries", vec![])?;
        self.sql_run("DELETE FROM workspace_state", vec![])?;
        self.insert_state(
            &envelope,
            legacy.total_bytes,
            legacy.file_count,
            legacy.completed_at_ms,
            legacy.retain_until_ms,
        )?;
        for entry in legacy.entries.values() {
            let row = WorkspaceEntryRow::from_meta(entry)?;
            self.sql_run(
                "INSERT INTO workspace_entries (
                    path,
                    object_key,
                    size_bytes,
                    updated_at_ms,
                    content_hash,
                    body_kind
                ) VALUES (?, ?, ?, ?, ?, ?)",
                vec![
                    js_string(&row.path),
                    js_string(&row.object_key),
                    js_i64(row.size_bytes)?,
                    js_i64(row.updated_at_ms)?,
                    opt_js_string(row.content_hash.as_deref()),
                    js_string(&row.body_kind),
                ],
            )?;
        }
        self.state.storage().delete(STATE_KEY).await?;
        Ok(())
    }

    fn validate_store(
        &self,
        state: &WorkspaceStateRecord,
        envelope: &WorkspaceDoEnvelope,
    ) -> worker::Result<()> {
        if state.scope != envelope.scope {
            return Err(rust_error(format!(
                "workspace scope mismatch: stored={:?} request={:?}",
                state.scope, envelope.scope
            )));
        }
        if state.bucket_binding != envelope.bucket_binding {
            return Err(rust_error(format!(
                "workspace bucket binding mismatch: stored={} request={}",
                state.bucket_binding, envelope.bucket_binding
            )));
        }
        if state.object_prefix != trim_slashes(&envelope.object_prefix) {
            return Err(rust_error(format!(
                "workspace object prefix mismatch: stored={} request={}",
                state.object_prefix, envelope.object_prefix
            )));
        }
        if state.policy != envelope.policy {
            return Err(rust_error(
                "workspace policy mismatch for existing workspace",
            ));
        }
        Ok(())
    }

    fn ensure_mutable(&self, state: &WorkspaceStateRecord) -> worker::Result<()> {
        if state.completed_at_ms.is_some() {
            Err(rust_error("workspace has already completed"))
        } else {
            Ok(())
        }
    }

    fn load_state(&self) -> worker::Result<Option<WorkspaceStateRow>> {
        self.sql_one(
            "SELECT
                singleton,
                schema_version,
                flow_id,
                run_id,
                bucket_binding,
                object_prefix,
                max_total_bytes,
                max_file_count,
                max_single_file_bytes,
                retain_completed_for_ms,
                total_bytes,
                file_count,
                completed_at_ms,
                retain_until_ms
             FROM workspace_state
             WHERE singleton = 1",
            vec![],
        )
    }

    fn load_entry(&self, path: &str) -> worker::Result<Option<WorkspaceEntryRow>> {
        self.sql_one(
            "SELECT path, object_key, size_bytes, updated_at_ms, content_hash, body_kind
             FROM workspace_entries
             WHERE path = ?",
            vec![js_string(path)],
        )
    }

    fn list_entries(&self, prefix: Option<&str>) -> worker::Result<Vec<WorkspaceEntryRow>> {
        if let Some(prefix) = prefix {
            if let Some(upper) = prefix_upper_bound(prefix) {
                return self.sql_all(
                    "SELECT path, object_key, size_bytes, updated_at_ms, content_hash, body_kind
                     FROM workspace_entries
                     WHERE path >= ? AND path < ?
                     ORDER BY path ASC",
                    vec![js_string(prefix), js_string(&upper)],
                );
            }
            return self.sql_all(
                "SELECT path, object_key, size_bytes, updated_at_ms, content_hash, body_kind
                 FROM workspace_entries
                 WHERE path LIKE ?
                 ORDER BY path ASC",
                vec![js_string(&format!("{prefix}%"))],
            );
        }
        self.sql_all(
            "SELECT path, object_key, size_bytes, updated_at_ms, content_hash, body_kind
             FROM workspace_entries
             ORDER BY path ASC",
            vec![],
        )
    }

    fn enforce_write_policy(
        &self,
        state: &WorkspaceStateRecord,
        existing: Option<&WorkspaceEntryRow>,
        new_size_bytes: u64,
    ) -> worker::Result<()> {
        if let Some(max_single_file_bytes) = state.policy.max_single_file_bytes {
            if new_size_bytes > max_single_file_bytes {
                return Err(rust_error(format!(
                    "workspace file exceeds max_single_file_bytes ({new_size_bytes} > {max_single_file_bytes})"
                )));
            }
        }

        let existing_size_bytes = match existing {
            Some(existing) => i64_to_u64(existing.size_bytes, "size_bytes")?,
            None => 0,
        };
        let next_total_bytes = state
            .total_bytes
            .saturating_sub(existing_size_bytes)
            .saturating_add(new_size_bytes);
        let next_file_count = if existing.is_some() {
            state.file_count
        } else {
            state.file_count.saturating_add(1)
        };

        if let Some(max_total_bytes) = state.policy.max_total_bytes {
            if next_total_bytes > max_total_bytes {
                return Err(rust_error(format!(
                    "workspace write would exceed max_total_bytes ({next_total_bytes} > {max_total_bytes})"
                )));
            }
        }
        if let Some(max_file_count) = state.policy.max_file_count {
            if next_file_count > max_file_count {
                return Err(rust_error(format!(
                    "workspace write would exceed max_file_count ({next_file_count} > {max_file_count})"
                )));
            }
        }
        Ok(())
    }

    fn commit_write(
        &self,
        state: &WorkspaceStateRecord,
        entry: WorkspaceEntryMeta,
    ) -> worker::Result<()> {
        let row = WorkspaceEntryRow::from_meta(&entry)?;
        let existing = self.load_entry(&entry.path)?;
        let existing_size = existing
            .as_ref()
            .map(|existing| i64_to_u64(existing.size_bytes, "size_bytes"))
            .transpose()?
            .unwrap_or(0);
        let next_total_bytes = state
            .total_bytes
            .saturating_sub(existing_size)
            .saturating_add(entry.size_bytes);
        let next_file_count = if existing.is_some() {
            state.file_count
        } else {
            state.file_count.saturating_add(1)
        };

        self.sql_run(
            "INSERT INTO workspace_entries (
                path,
                object_key,
                size_bytes,
                updated_at_ms,
                content_hash,
                body_kind
             ) VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(path) DO UPDATE SET
                object_key = excluded.object_key,
                size_bytes = excluded.size_bytes,
                updated_at_ms = excluded.updated_at_ms,
                content_hash = excluded.content_hash,
                body_kind = excluded.body_kind",
            vec![
                js_string(&row.path),
                js_string(&row.object_key),
                js_i64(row.size_bytes)?,
                js_i64(row.updated_at_ms)?,
                opt_js_string(row.content_hash.as_deref()),
                js_string(&row.body_kind),
            ],
        )?;
        self.sql_run(
            "UPDATE workspace_state
             SET total_bytes = ?, file_count = ?
             WHERE singleton = 1",
            vec![
                js_i64(u64_to_i64(next_total_bytes, "total_bytes")?)?,
                js_i64(u64_to_i64(next_file_count, "file_count")?)?,
            ],
        )
    }

    fn delete_entry(
        &self,
        state: &WorkspaceStateRecord,
        path: &str,
    ) -> worker::Result<(bool, Option<String>)> {
        let Some(existing) = self.load_entry(path)? else {
            return Ok((false, None));
        };
        let existing_size = i64_to_u64(existing.size_bytes, "size_bytes")?;
        let next_total_bytes = state.total_bytes.saturating_sub(existing_size);
        let next_file_count = state.file_count.saturating_sub(1);

        self.sql_run(
            "DELETE FROM workspace_entries WHERE path = ?",
            vec![js_string(path)],
        )?;
        self.sql_run(
            "UPDATE workspace_state
             SET total_bytes = ?, file_count = ?
             WHERE singleton = 1",
            vec![
                js_i64(u64_to_i64(next_total_bytes, "total_bytes")?)?,
                js_i64(u64_to_i64(next_file_count, "file_count")?)?,
            ],
        )?;
        Ok((true, Some(existing.object_key)))
    }

    async fn complete_workspace(
        &self,
        state: &WorkspaceStateRecord,
        retain_completed_for_ms: Option<u64>,
    ) -> worker::Result<()> {
        if state.completed_at_ms.is_some() {
            return Ok(());
        }

        match retain_completed_for_ms {
            Some(retain_ms) if retain_ms > 0 => {
                let completed_at_ms = now_ms();
                let retain_until_ms = completed_at_ms.saturating_add(retain_ms);
                self.sql_run(
                    "UPDATE workspace_state
                     SET completed_at_ms = ?, retain_until_ms = ?
                     WHERE singleton = 1",
                    vec![
                        js_i64(u64_to_i64(completed_at_ms, "completed_at_ms")?)?,
                        js_i64(u64_to_i64(retain_until_ms, "retain_until_ms")?)?,
                    ],
                )?;
                self.state
                    .storage()
                    .set_alarm(u64_to_i64(retain_until_ms, "retain_until_ms")?)
                    .await?;
                Ok(())
            }
            _ => {
                self.delete_workspace_objects(&state.bucket_binding).await?;
                self.clear_workspace_state().await
            }
        }
    }

    async fn run_retained_cleanup(&self, current_ms: u64) -> worker::Result<()> {
        self.ensure_schema()?;
        let Some(state) = self
            .load_state()?
            .map(WorkspaceStateRow::into_record)
            .transpose()?
        else {
            return Ok(());
        };
        let Some(retain_until_ms) = state.retain_until_ms else {
            return Ok(());
        };
        if current_ms < retain_until_ms {
            return Ok(());
        }
        self.delete_workspace_objects(&state.bucket_binding).await?;
        self.clear_workspace_state().await
    }

    async fn delete_workspace_objects(&self, bucket_binding: &str) -> worker::Result<()> {
        let bucket = self.env.0.bucket(bucket_binding)?;
        for entry in self.list_entries(None)? {
            bucket.delete(entry.object_key).await?;
        }
        Ok(())
    }

    async fn clear_workspace_state(&self) -> worker::Result<()> {
        self.state.storage().delete_alarm().await?;
        self.sql_run("DELETE FROM workspace_entries", vec![])?;
        self.sql_run("DELETE FROM workspace_state", vec![])?;
        self.state.storage().delete(STATE_KEY).await?;
        Ok(())
    }

    fn sql_run(&self, query: &str, bindings: Vec<SqlStorageValue>) -> worker::Result<()> {
        self.state.storage().sql().exec(query, bindings)?;
        Ok(())
    }

    fn sql_one<T>(&self, query: &str, bindings: Vec<SqlStorageValue>) -> worker::Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let mut rows = self
            .state
            .storage()
            .sql()
            .exec(query, bindings)?
            .to_array::<T>()?;
        Ok(rows.pop())
    }

    fn sql_all<T>(&self, query: &str, bindings: Vec<SqlStorageValue>) -> worker::Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        self.state
            .storage()
            .sql()
            .exec(query, bindings)?
            .to_array::<T>()
    }
}

fn map_workspace_protocol(error: WorkersWorkspaceError) -> WorkspaceError {
    match error {
        WorkersWorkspaceError::Protocol(message) => WorkspaceError::Backend(message),
        other => WorkspaceError::Backend(other.to_string()),
    }
}

fn unexpected_response(operation: &str, response: WorkspaceDoResponse) -> WorkspaceError {
    WorkspaceError::Backend(format!(
        "unexpected {operation} response from workspace durable object: {:?}",
        response
    ))
}

fn rust_error(message: impl Into<String>) -> worker::Error {
    worker::Error::RustError(message.into())
}

fn js_string(value: &str) -> SqlStorageValue {
    SqlStorageValue::String(value.to_string())
}

fn js_i64(value: i64) -> worker::Result<SqlStorageValue> {
    SqlStorageValue::try_from_i64(value)
}

fn opt_js_string(value: Option<&str>) -> SqlStorageValue {
    value.map(js_string).unwrap_or(SqlStorageValue::Null)
}

fn opt_js_i64(value: Option<u64>, field: &str) -> worker::Result<SqlStorageValue> {
    Ok(match value {
        Some(value) => js_i64(u64_to_i64(value, field)?)?,
        None => SqlStorageValue::Null,
    })
}

fn i64_to_u64(value: i64, field: &str) -> worker::Result<u64> {
    u64::try_from(value).map_err(|_| rust_error(format!("{field} out of range: {value}")))
}

fn i64_to_u32(value: i64, field: &str) -> worker::Result<u32> {
    u32::try_from(value).map_err(|_| rust_error(format!("{field} out of range: {value}")))
}

fn opt_i64_to_u64(value: Option<i64>, field: &str) -> worker::Result<Option<u64>> {
    value.map(|value| i64_to_u64(value, field)).transpose()
}

fn u64_to_i64(value: u64, field: &str) -> worker::Result<i64> {
    i64::try_from(value).map_err(|_| rust_error(format!("{field} out of range: {value}")))
}
