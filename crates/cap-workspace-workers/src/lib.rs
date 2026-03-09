#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

use async_trait::async_trait;
use capabilities::Capability;
use capabilities::workspace::{
    Workspace, WorkspaceCompletionDisposition, WorkspaceDeleteResult, WorkspaceEntry,
    WorkspaceError, WorkspaceFactory, WorkspaceListOptions, WorkspacePolicy, WorkspaceReadResult,
    WorkspaceRunScope, WorkspaceWriteOptions, WorkspaceWriteResult,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const DEFAULT_WORKSPACE_BUCKET_BINDING: &str = "WORKSPACE_BUCKET";
pub const DEFAULT_WORKSPACE_DO_BINDING: &str = "WORKSPACE_DO";
pub const DEFAULT_WORKSPACE_OBJECT_PREFIX: &str = "workspace";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkersWorkspaceConfig {
    pub bucket_binding: String,
    pub index_binding: String,
    pub object_prefix: String,
    pub policy: WorkspacePolicy,
}

impl Default for WorkersWorkspaceConfig {
    fn default() -> Self {
        Self {
            bucket_binding: DEFAULT_WORKSPACE_BUCKET_BINDING.to_string(),
            index_binding: DEFAULT_WORKSPACE_DO_BINDING.to_string(),
            object_prefix: DEFAULT_WORKSPACE_OBJECT_PREFIX.to_string(),
            policy: WorkspacePolicy::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceEntryMeta {
    pub path: String,
    pub object_key: String,
    pub size_bytes: u64,
    pub updated_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

impl WorkspaceEntryMeta {
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    fn to_workspace_entry(&self) -> WorkspaceEntry {
        WorkspaceEntry {
            path: self.path.clone(),
            size_bytes: self.size_bytes,
            updated_at_ms: self.updated_at_ms,
            content_hash: self.content_hash.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceWriteReservation {
    pub entry: WorkspaceEntryMeta,
}

#[derive(Debug, Error)]
pub enum WorkersWorkspaceError {
    #[error("cap-workspace-workers requires wasm32")]
    Unsupported,
    #[error("workers workspace backend error: {0}")]
    Backend(String),
    #[error("workers workspace protocol error: {0}")]
    Protocol(String),
    #[error("workers workspace serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct WorkersWorkspaceFactory {
    config: WorkersWorkspaceConfig,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WorkersWorkspace {
    scope: WorkspaceRunScope,
    policy: WorkspacePolicy,
}

#[cfg(not(target_arch = "wasm32"))]
impl WorkersWorkspaceFactory {
    pub fn new(config: WorkersWorkspaceConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &WorkersWorkspaceConfig {
        &self.config
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl WorkspaceFactory for WorkersWorkspaceFactory {
    async fn open(&self, _scope: WorkspaceRunScope) -> anyhow::Result<Arc<dyn Workspace>> {
        Err(anyhow::anyhow!(WorkersWorkspaceError::Unsupported))
    }

    async fn complete(
        &self,
        _scope: WorkspaceRunScope,
        _disposition: WorkspaceCompletionDisposition,
    ) -> anyhow::Result<()> {
        Err(anyhow::anyhow!(WorkersWorkspaceError::Unsupported))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Capability for WorkersWorkspace {
    fn name(&self) -> &'static str {
        "workspace.workers"
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl Workspace for WorkersWorkspace {
    async fn read_normalized(
        &self,
        _normalized_path: &str,
    ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
        Err(WorkspaceError::Unsupported(
            WorkersWorkspaceError::Unsupported.to_string(),
        ))
    }

    async fn write_normalized(
        &self,
        _normalized_path: &str,
        _data: &[u8],
        _options: WorkspaceWriteOptions,
    ) -> Result<WorkspaceWriteResult, WorkspaceError> {
        Err(WorkspaceError::Unsupported(
            WorkersWorkspaceError::Unsupported.to_string(),
        ))
    }

    async fn list_normalized(
        &self,
        _options: WorkspaceListOptions,
    ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
        Err(WorkspaceError::Unsupported(
            WorkersWorkspaceError::Unsupported.to_string(),
        ))
    }

    async fn delete_normalized(
        &self,
        _normalized_path: &str,
    ) -> Result<WorkspaceDeleteResult, WorkspaceError> {
        Err(WorkspaceError::Unsupported(
            WorkersWorkspaceError::Unsupported.to_string(),
        ))
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use worker::durable_object;
    use worker::{Bucket, Env, Method, Request, RequestInit, Response, State};

    const STATE_KEY: &str = "workspace:state";
    const DO_URL: &str = "http://do/workspace";

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
        Error {
            message: String,
        },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct WorkspaceStateRecord {
        flow_id: String,
        run_id: String,
        bucket_binding: String,
        object_prefix: String,
        policy: WorkspacePolicy,
        total_bytes: u64,
        file_count: u64,
        completed_at_ms: Option<u64>,
        retain_until_ms: Option<u64>,
        entries: BTreeMap<String, WorkspaceEntryMeta>,
    }

    impl WorkersWorkspaceFactory {
        pub fn new(env: Env, config: WorkersWorkspaceConfig) -> Self {
            Self {
                env: EnvHandle(env),
                config,
            }
        }

        pub fn config(&self) -> &WorkersWorkspaceConfig {
            &self.config
        }
    }

    #[async_trait(?Send)]
    impl WorkspaceFactory for WorkersWorkspaceFactory {
        async fn open(&self, scope: WorkspaceRunScope) -> anyhow::Result<Arc<dyn Workspace>> {
            let bucket = self
                .env
                .0
                .bucket(&self.config.bucket_binding)
                .map_err(|err| anyhow::anyhow!(map_worker_error(err).to_string()))?;
            let index = WorkspaceIndexClient {
                env: self.env.clone(),
                index_binding: self.config.index_binding.clone(),
                scope: scope.clone(),
                bucket_binding: self.config.bucket_binding.clone(),
                object_prefix: self.config.object_prefix.clone(),
                policy: self.config.policy.clone(),
            };
            Ok(Arc::new(WorkersWorkspace {
                scope,
                policy: self.config.policy.clone(),
                bucket: BucketHandle(bucket),
                index,
            }))
        }

        async fn complete(
            &self,
            scope: WorkspaceRunScope,
            disposition: WorkspaceCompletionDisposition,
        ) -> anyhow::Result<()> {
            let index = WorkspaceIndexClient {
                env: self.env.clone(),
                index_binding: self.config.index_binding.clone(),
                scope,
                bucket_binding: self.config.bucket_binding.clone(),
                object_prefix: self.config.object_prefix.clone(),
                policy: self.config.policy.clone(),
            };
            index
                .complete(disposition)
                .await
                .map_err(anyhow::Error::msg)
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
            let Some(entry) = self
                .index
                .stat(normalized_path)
                .await
                .map_err(workspace_backend_error)?
            else {
                return Ok(None);
            };

            let Some(object) = self
                .bucket
                .0
                .get(entry.object_key.clone())
                .execute()
                .await
                .map_err(|err| workspace_backend_error(map_worker_error(err)))?
            else {
                return Err(WorkspaceError::Backend(format!(
                    "workspace index/body mismatch for `{normalized_path}`"
                )));
            };

            let body = object.body().ok_or_else(|| {
                WorkspaceError::Backend(format!(
                    "workspace object body unavailable for `{normalized_path}`"
                ))
            })?;
            let bytes = body
                .bytes()
                .await
                .map_err(|err| workspace_backend_error(map_worker_error(err)))?;
            Ok(Some(WorkspaceReadResult::Bytes(bytes)))
        }

        async fn write_normalized(
            &self,
            normalized_path: &str,
            data: &[u8],
            _options: WorkspaceWriteOptions,
        ) -> Result<WorkspaceWriteResult, WorkspaceError> {
            let updated_at_ms = now_ms();
            let reservation = self
                .index
                .prepare_write(normalized_path, data.len() as u64, updated_at_ms, None)
                .await
                .map_err(workspace_backend_error)?;

            self.bucket
                .0
                .put(reservation.entry.object_key.clone(), data.to_vec())
                .execute()
                .await
                .map_err(|err| workspace_backend_error(map_worker_error(err)))?;

            self.index
                .commit_write(reservation.entry.clone())
                .await
                .map_err(workspace_backend_error)?;

            Ok(WorkspaceWriteResult {
                path: normalized_path.to_string(),
                size_bytes: reservation.entry.size_bytes,
                updated_at_ms: reservation.entry.updated_at_ms,
            })
        }

        async fn list_normalized(
            &self,
            options: WorkspaceListOptions,
        ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
            let entries = self
                .index
                .list(options.prefix.as_deref())
                .await
                .map_err(workspace_backend_error)?;
            Ok(entries
                .into_iter()
                .map(|entry| entry.to_workspace_entry())
                .collect())
        }

        async fn delete_normalized(
            &self,
            normalized_path: &str,
        ) -> Result<WorkspaceDeleteResult, WorkspaceError> {
            let delete = self
                .index
                .delete(normalized_path)
                .await
                .map_err(workspace_backend_error)?;
            if let Some(object_key) = delete.object_key {
                self.bucket
                    .0
                    .delete(object_key)
                    .await
                    .map_err(|err| workspace_backend_error(map_worker_error(err)))?;
            }
            Ok(WorkspaceDeleteResult {
                deleted: delete.deleted,
            })
        }
    }

    impl WorkspaceIndexClient {
        async fn stat(
            &self,
            path: &str,
        ) -> Result<Option<WorkspaceEntryMeta>, WorkersWorkspaceError> {
            match self
                .dispatch(WorkspaceDoRequest::Stat {
                    path: path.to_string(),
                })
                .await?
            {
                WorkspaceDoResponse::Stat { entry } => Ok(entry),
                response => unexpected_response("stat", response),
            }
        }

        async fn list(
            &self,
            prefix: Option<&str>,
        ) -> Result<Vec<WorkspaceEntryMeta>, WorkersWorkspaceError> {
            match self
                .dispatch(WorkspaceDoRequest::List {
                    prefix: prefix.map(str::to_string),
                })
                .await?
            {
                WorkspaceDoResponse::List { entries } => Ok(entries),
                response => unexpected_response("list", response),
            }
        }

        async fn prepare_write(
            &self,
            path: &str,
            new_size_bytes: u64,
            updated_at_ms: u64,
            content_hash: Option<String>,
        ) -> Result<WorkspaceWriteReservation, WorkersWorkspaceError> {
            match self
                .dispatch(WorkspaceDoRequest::PrepareWrite {
                    path: path.to_string(),
                    new_size_bytes,
                    updated_at_ms,
                    content_hash,
                })
                .await?
            {
                WorkspaceDoResponse::PrepareWrite { reservation } => Ok(reservation),
                response => unexpected_response("prepare_write", response),
            }
        }

        async fn commit_write(
            &self,
            entry: WorkspaceEntryMeta,
        ) -> Result<(), WorkersWorkspaceError> {
            match self
                .dispatch(WorkspaceDoRequest::CommitWrite { entry })
                .await?
            {
                WorkspaceDoResponse::CommitWrite => Ok(()),
                response => unexpected_response("commit_write", response),
            }
        }

        async fn delete(&self, path: &str) -> Result<DeleteReply, WorkersWorkspaceError> {
            match self
                .dispatch(WorkspaceDoRequest::Delete {
                    path: path.to_string(),
                })
                .await?
            {
                WorkspaceDoResponse::Delete {
                    deleted,
                    object_key,
                } => Ok(DeleteReply {
                    deleted,
                    object_key,
                }),
                response => unexpected_response("delete", response),
            }
        }

        async fn complete(
            &self,
            disposition: WorkspaceCompletionDisposition,
        ) -> Result<(), WorkersWorkspaceError> {
            let retain_completed_for_ms = self
                .policy
                .retain_completed_for
                .map(duration_to_millis)
                .filter(|value| *value > 0);
            match self
                .dispatch(WorkspaceDoRequest::Complete {
                    disposition,
                    retain_completed_for_ms,
                })
                .await?
            {
                WorkspaceDoResponse::Complete => Ok(()),
                response => unexpected_response("complete", response),
            }
        }

        async fn dispatch(
            &self,
            request: WorkspaceDoRequest,
        ) -> Result<WorkspaceDoResponse, WorkersWorkspaceError> {
            let namespace = self
                .env
                .0
                .durable_object(&self.index_binding)
                .map_err(map_worker_error)?;
            let stub = namespace
                .id_from_name(&scope_object_name(&self.scope))
                .map_err(map_worker_error)?
                .get_stub()
                .map_err(map_worker_error)?;
            let envelope = WorkspaceDoEnvelope {
                scope: self.scope.clone(),
                bucket_binding: self.bucket_binding.clone(),
                object_prefix: self.object_prefix.clone(),
                policy: self.policy.clone(),
                request,
            };
            let body = serde_json::to_string(&envelope)?;
            let mut init = RequestInit::new();
            init.with_method(Method::Post);
            init.with_body(Some(worker::wasm_bindgen::JsValue::from_str(&body)));
            let req = Request::new_with_init(DO_URL, &init).map_err(map_worker_error)?;
            req.headers()
                .set("content-type", "application/json")
                .map_err(map_worker_error)?;
            let mut response = stub
                .fetch_with_request(req)
                .await
                .map_err(map_worker_error)?;
            let bytes = response.bytes().await.map_err(map_worker_error)?;
            let parsed: WorkspaceDoResponse = serde_json::from_slice(&bytes)?;
            match parsed {
                WorkspaceDoResponse::Error { message } => {
                    Err(WorkersWorkspaceError::Protocol(message))
                }
                other => Ok(other),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct DeleteReply {
        deleted: bool,
        object_key: Option<String>,
    }

    fn unexpected_response<T>(
        op: &str,
        response: WorkspaceDoResponse,
    ) -> Result<T, WorkersWorkspaceError> {
        Err(WorkersWorkspaceError::Protocol(format!(
            "unexpected workspace DO response for `{op}`: {response:?}"
        )))
    }

    #[durable_object]
    pub struct WorkspaceDurableObject {
        state: State,
        env: Env,
    }

    impl worker::DurableObject for WorkspaceDurableObject {
        fn new(state: State, env: Env) -> Self {
            Self { state, env }
        }

        async fn fetch(&self, mut req: Request) -> worker::Result<Response> {
            let body = req.bytes().await?;
            let envelope = match serde_json::from_slice::<WorkspaceDoEnvelope>(&body) {
                Ok(envelope) => envelope,
                Err(err) => {
                    return Response::from_json(&WorkspaceDoResponse::Error {
                        message: format!("invalid workspace DO request: {err}"),
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
            if let Some(state) = self.load_state().await.map_err(worker::Error::from)? {
                self.cleanup_state(&state)
                    .await
                    .map_err(worker::Error::from)?;
            }
            Response::from_json(&serde_json::json!({ "ok": true }))
        }
    }

    impl WorkspaceDurableObject {
        async fn handle_request(
            &self,
            envelope: WorkspaceDoEnvelope,
        ) -> Result<WorkspaceDoResponse, WorkersWorkspaceError> {
            let mut state = self.ensure_state(&envelope).await?;
            match envelope.request {
                WorkspaceDoRequest::Stat { path } => Ok(WorkspaceDoResponse::Stat {
                    entry: state.entries.get(&path).cloned(),
                }),
                WorkspaceDoRequest::List { prefix } => {
                    let mut entries = state
                        .entries
                        .values()
                        .filter(|entry| {
                            prefix
                                .as_deref()
                                .map(|prefix| entry.path.starts_with(prefix))
                                .unwrap_or(true)
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    entries.sort_by(|left, right| left.path.cmp(&right.path));
                    Ok(WorkspaceDoResponse::List { entries })
                }
                WorkspaceDoRequest::PrepareWrite {
                    path,
                    new_size_bytes,
                    updated_at_ms,
                    content_hash,
                } => {
                    self.ensure_mutable(&state)?;
                    let existing_size = state
                        .entries
                        .get(&path)
                        .map(|entry| entry.size_bytes)
                        .unwrap_or(0);
                    let next_total = state
                        .total_bytes
                        .saturating_sub(existing_size)
                        .saturating_add(new_size_bytes);
                    let next_file_count = if state.entries.contains_key(&path) {
                        state.file_count
                    } else {
                        state.file_count.saturating_add(1)
                    };
                    enforce_policy(
                        &state.policy,
                        &path,
                        new_size_bytes,
                        next_total,
                        next_file_count,
                    )?;
                    let entry = WorkspaceEntryMeta {
                        path: path.clone(),
                        object_key: object_key(
                            &state.object_prefix,
                            &state.flow_id,
                            &state.run_id,
                            &path,
                        ),
                        size_bytes: new_size_bytes,
                        updated_at_ms,
                        content_hash,
                    };
                    Ok(WorkspaceDoResponse::PrepareWrite {
                        reservation: WorkspaceWriteReservation { entry },
                    })
                }
                WorkspaceDoRequest::CommitWrite { entry } => {
                    self.ensure_mutable(&state)?;
                    let previous = state.entries.insert(entry.path.clone(), entry.clone());
                    if let Some(previous) = previous {
                        state.total_bytes = state
                            .total_bytes
                            .saturating_sub(previous.size_bytes)
                            .saturating_add(entry.size_bytes);
                    } else {
                        state.total_bytes = state.total_bytes.saturating_add(entry.size_bytes);
                        state.file_count = state.file_count.saturating_add(1);
                    }
                    self.store_state(&state).await?;
                    Ok(WorkspaceDoResponse::CommitWrite)
                }
                WorkspaceDoRequest::Delete { path } => {
                    self.ensure_mutable(&state)?;
                    let removed = state.entries.remove(&path);
                    if let Some(removed) = removed {
                        state.total_bytes = state.total_bytes.saturating_sub(removed.size_bytes);
                        state.file_count = state.file_count.saturating_sub(1);
                        self.store_state(&state).await?;
                        Ok(WorkspaceDoResponse::Delete {
                            deleted: true,
                            object_key: Some(removed.object_key),
                        })
                    } else {
                        Ok(WorkspaceDoResponse::Delete {
                            deleted: false,
                            object_key: None,
                        })
                    }
                }
                WorkspaceDoRequest::Complete {
                    disposition: _disposition,
                    retain_completed_for_ms,
                } => {
                    let completed_at_ms = now_ms();
                    if let Some(retain_ms) = retain_completed_for_ms {
                        state.completed_at_ms = Some(completed_at_ms);
                        state.retain_until_ms = Some(completed_at_ms.saturating_add(retain_ms));
                        self.store_state(&state).await?;
                        if let Some(retain_until_ms) = state.retain_until_ms {
                            self.state
                                .storage()
                                .set_alarm(retain_until_ms as i64)
                                .await
                                .map_err(map_worker_error)?;
                        }
                        Ok(WorkspaceDoResponse::Complete)
                    } else {
                        self.cleanup_state(&state).await?;
                        Ok(WorkspaceDoResponse::Complete)
                    }
                }
            }
        }

        async fn ensure_state(
            &self,
            envelope: &WorkspaceDoEnvelope,
        ) -> Result<WorkspaceStateRecord, WorkersWorkspaceError> {
            if let Some(state) = self.load_state().await? {
                if state.flow_id != envelope.scope.flow_id || state.run_id != envelope.scope.run_id
                {
                    return Err(WorkersWorkspaceError::Protocol(
                        "workspace DO scope mismatch".to_string(),
                    ));
                }
                return Ok(state);
            }

            let state = WorkspaceStateRecord {
                flow_id: envelope.scope.flow_id.clone(),
                run_id: envelope.scope.run_id.clone(),
                bucket_binding: envelope.bucket_binding.clone(),
                object_prefix: envelope.object_prefix.clone(),
                policy: envelope.policy.clone(),
                total_bytes: 0,
                file_count: 0,
                completed_at_ms: None,
                retain_until_ms: None,
                entries: BTreeMap::new(),
            };
            self.store_state(&state).await?;
            Ok(state)
        }

        async fn load_state(&self) -> Result<Option<WorkspaceStateRecord>, WorkersWorkspaceError> {
            self.state
                .storage()
                .get(STATE_KEY)
                .await
                .map_err(map_worker_error)
        }

        async fn store_state(
            &self,
            state: &WorkspaceStateRecord,
        ) -> Result<(), WorkersWorkspaceError> {
            self.state
                .storage()
                .put(STATE_KEY, state)
                .await
                .map_err(map_worker_error)
        }

        async fn cleanup_state(
            &self,
            state: &WorkspaceStateRecord,
        ) -> Result<(), WorkersWorkspaceError> {
            let bucket = self
                .env
                .bucket(&state.bucket_binding)
                .map_err(map_worker_error)?;
            for entry in state.entries.values() {
                bucket
                    .delete(entry.object_key.clone())
                    .await
                    .map_err(map_worker_error)?;
            }
            self.state
                .storage()
                .delete_alarm()
                .await
                .map_err(map_worker_error)?;
            self.state
                .storage()
                .delete(STATE_KEY)
                .await
                .map_err(map_worker_error)?;
            Ok(())
        }

        fn ensure_mutable(
            &self,
            state: &WorkspaceStateRecord,
        ) -> Result<(), WorkersWorkspaceError> {
            if state.completed_at_ms.is_some() {
                return Err(WorkersWorkspaceError::Protocol(
                    "workspace is already completed".to_string(),
                ));
            }
            Ok(())
        }
    }

    impl From<WorkersWorkspaceError> for worker::Error {
        fn from(err: WorkersWorkspaceError) -> Self {
            worker::Error::RustError(err.to_string())
        }
    }

    fn map_worker_error(err: worker::Error) -> WorkersWorkspaceError {
        WorkersWorkspaceError::Backend(err.to_string())
    }

    fn workspace_backend_error(err: WorkersWorkspaceError) -> WorkspaceError {
        WorkspaceError::Backend(err.to_string())
    }

    fn enforce_policy(
        policy: &WorkspacePolicy,
        path: &str,
        new_size_bytes: u64,
        next_total_bytes: u64,
        next_file_count: u64,
    ) -> Result<(), WorkersWorkspaceError> {
        if let Some(limit) = policy.max_single_file_bytes {
            if new_size_bytes > limit {
                return Err(WorkersWorkspaceError::Protocol(format!(
                    "workspace write for `{path}` exceeds max_single_file_bytes ({new_size_bytes} > {limit})"
                )));
            }
        }
        if let Some(limit) = policy.max_total_bytes {
            if next_total_bytes > limit {
                return Err(WorkersWorkspaceError::Protocol(format!(
                    "workspace write for `{path}` exceeds max_total_bytes ({next_total_bytes} > {limit})"
                )));
            }
        }
        if let Some(limit) = policy.max_file_count {
            if next_file_count > limit {
                return Err(WorkersWorkspaceError::Protocol(format!(
                    "workspace write for `{path}` exceeds max_file_count ({next_file_count} > {limit})"
                )));
            }
        }
        Ok(())
    }

    fn object_key(object_prefix: &str, flow_id: &str, run_id: &str, path: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            trim_slashes(object_prefix),
            flow_key(flow_id),
            path_key(run_id),
            path
        )
    }

    fn scope_object_name(scope: &WorkspaceRunScope) -> String {
        format!(
            "workspace:{}:{}",
            flow_key(&scope.flow_id),
            path_key(&scope.run_id)
        )
    }

    fn flow_key(flow_id: &str) -> String {
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, flow_id.as_bytes()).to_string()
    }

    fn path_key(value: &str) -> String {
        let mut encoded = String::with_capacity(value.len());
        for byte in value.bytes() {
            let ch = byte as char;
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                encoded.push(ch);
            } else {
                encoded.push('_');
                encoded.push_str(&format!("{byte:02x}"));
            }
        }
        if encoded.is_empty() {
            "_".to_string()
        } else {
            encoded
        }
    }

    fn trim_slashes(value: &str) -> &str {
        value.trim_matches('/')
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm::{
    WorkersWorkspace, WorkersWorkspaceFactory, WorkspaceDurableObject, WorkspaceIndexClient,
};

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
fn duration_to_millis(duration: std::time::Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
fn now_ms() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        let millis = js_sys::Date::now();
        if millis.is_finite() && millis >= 0.0 {
            millis as u64
        } else {
            0
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_match_documented_bindings() {
        let config = WorkersWorkspaceConfig::default();
        assert_eq!(config.bucket_binding, DEFAULT_WORKSPACE_BUCKET_BINDING);
        assert_eq!(config.index_binding, DEFAULT_WORKSPACE_DO_BINDING);
        assert_eq!(config.object_prefix, DEFAULT_WORKSPACE_OBJECT_PREFIX);
    }

    #[test]
    fn reservation_meta_converts_to_workspace_entry() {
        let meta = WorkspaceEntryMeta {
            path: "inbox/a.txt".to_string(),
            object_key: "workspace/hash/run/inbox/a.txt".to_string(),
            size_bytes: 3,
            updated_at_ms: 42,
            content_hash: Some("abc".to_string()),
        };
        let entry = meta.to_workspace_entry();
        assert_eq!(entry.path, "inbox/a.txt");
        assert_eq!(entry.size_bytes, 3);
        assert_eq!(entry.updated_at_ms, 42);
        assert_eq!(entry.content_hash.as_deref(), Some("abc"));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_factory_is_explicitly_unavailable() {
        let factory = WorkersWorkspaceFactory::new(WorkersWorkspaceConfig::default());
        let err = match factory
            .open(WorkspaceRunScope::new("flow:test", "run:test"))
            .await
        {
            Ok(_) => panic!("native open should fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("requires wasm32"));
    }
}
