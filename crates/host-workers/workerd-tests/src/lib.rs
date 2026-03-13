//! Test worker for host-workers E2E tests.
//!
//! Provides flow entrypoints that exercise the host-workers runtime path:
//! - GET /health - basic health check
//! - POST /echo - echo request body
//! - POST /stream - streaming SSE response
//! - POST /cancel - test cancellation (long-running request)
//! - POST /timer - halt + resume via Durable Object alarm dispatch
//! - POST /workspace - workspace roundtrip + cleanup
//! - POST /workspace-resume - workspace continuity across halt/resume
//! - POST /workspace-retained - retained cleanup/alarm path
//! - POST /workspace-quota - workspace quota enforcement cases
//! - POST /workspace-invalid-path - traversal rejection cases
//! - POST /workspace-mutation - overwrite/delete accounting cases
//! - POST /workspace-blocked-prefix - blocked-prefix policy cases

use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use async_stream::stream;
use cap_do_workers::{DurableObjectBinding, WorkersDurableObject};
use cap_workspace_workers::{WorkersWorkspaceConfig, WorkersWorkspaceFactory};
use capabilities::workspace::{
    WorkspaceCompletionDisposition, WorkspaceFactory, WorkspacePolicy, WorkspaceRunScope,
};
use capabilities::{ResourceAccess, ResourceBag};
use capabilities::durability::{CheckpointFilter, CheckpointStore};
use dag_core::{DurabilityMode, NodeError, NodeResult};
use dag_macros::{def_node, node};
use futures::Stream;
use host_inproc::{FlowBundle, FlowEntrypoint, NodeContract, NodeSource};
use kernel_exec::{NodeRegistry, RegistryError};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
#[cfg(target_arch = "wasm32")]
use js_sys::JsString;
#[cfg(target_arch = "wasm32")]
use worker::{Context, Env, Method, Request, RequestInit, Response, Result, event};

pub use cap_do_workers::FlowDurableObject;
pub use cap_workspace_workers::WorkspaceDurableObject;

#[cfg(target_arch = "wasm32")]
#[event(fetch)]
async fn fetch(req: Request, env: Env, ctx: Context) -> Result<Response> {
    if req.path() == "/__test/checkpoint" {
        return handle_test_checkpoint(req, &env).await;
    }
    if req.path() == "/__test/alarm/tick" {
        return handle_test_alarm_tick(&env).await;
    }
    if req.path() == "/__test/workspace/objects" {
        return handle_test_workspace_objects(req, &env).await;
    }
    if req.path() == "/__test/workspace/run-retained-cleanup" {
        return handle_test_workspace_retained_cleanup(req, &env).await;
    }
    if req.path() == "/workspace-stdlib-write" {
        return handle_workspace_stdlib_write(req, &env).await;
    }
    if req.path() == "/workspace-stdlib-read" {
        return handle_workspace_stdlib_read(req, &env).await;
    }
    if req.path() == "/workspace-stdlib-list" {
        return handle_workspace_stdlib_list(req, &env).await;
    }
    if req.path() == "/workspace-stdlib-delete" {
        return handle_workspace_stdlib_delete(req, &env).await;
    }

    configure_resources(&env)?;
    configure_workspace_factory(req.path().as_str(), &env)?;
    host_workers::handle_fetch(req, env, ctx).await
}

#[cfg(target_arch = "wasm32")]
fn durability_binding(env: &Env) -> Result<DurableObjectBinding> {
    DurableObjectBinding::from_env(env, "FLOW_DO")
        .map_err(|err| worker::Error::RustError(err.to_string()))
}

#[cfg(target_arch = "wasm32")]
fn durability_capability(env: &Env) -> Result<Arc<WorkersDurableObject>> {
    let binding = durability_binding(env)?;
    Ok(Arc::new(WorkersDurableObject::from_binding(
        binding,
        Some("host-workers-test-durability".to_string()),
    )))
}

#[cfg(target_arch = "wasm32")]
fn configure_resources(env: &Env) -> Result<()> {
    let durability = durability_capability(env)?;
    let resources = ResourceBag::new()
        .with_checkpoint_store(Arc::clone(&durability))
        .with_resume_scheduler(Arc::clone(&durability))
        .with_resume_signal_source(Arc::clone(&durability))
        .with_max_durability_mode(DurabilityMode::Partial);
    host_workers::set_resource_bag(resources);
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn workspace_config_for_path(path: &str) -> WorkersWorkspaceConfig {
    let mut config = WorkersWorkspaceConfig {
        bucket_binding: "WORKSPACE_BUCKET".to_string(),
        index_binding: "WORKSPACE_DO".to_string(),
        object_prefix: "workspace".to_string(),
        policy: WorkspacePolicy {
            max_total_bytes: Some(64),
            max_file_count: Some(4),
            max_single_file_bytes: Some(32),
            retain_completed_for: None,
        },
        blocked_prefixes: Vec::new(),
        max_path_depth: None,
        max_path_length: None,
    };

    match path {
        "/workspace-retained" => {
            config.policy.retain_completed_for = Some(Duration::from_secs(60));
        }
        "/workspace-mutation" => {
            config.policy.max_file_count = Some(1);
        }
        "/workspace-blocked-prefix" => {
            config.blocked_prefixes = vec![
                "node_modules".to_string(),
                ".venv".to_string(),
                "target".to_string(),
            ];
            config.max_path_depth = Some(6);
            config.max_path_length = Some(96);
        }
        _ => {}
    }

    config
}

#[cfg(target_arch = "wasm32")]
fn configure_workspace_factory(path: &str, env: &Env) -> Result<()> {
    if env.bucket("WORKSPACE_BUCKET").is_err() || env.durable_object("WORKSPACE_DO").is_err() {
        return Ok(());
    }

    host_workers::set_workspace_factory(Arc::new(WorkersWorkspaceFactory::new(
        env.clone(),
        workspace_config_for_path(path),
    )));
    Ok(())
}

#[cfg(target_arch = "wasm32")]
struct WorkspaceOnlyResources {
    workspace: Arc<dyn capabilities::workspace::Workspace>,
}

#[cfg(target_arch = "wasm32")]
impl ResourceAccess for WorkspaceOnlyResources {
    fn workspace(&self) -> Option<&dyn capabilities::workspace::Workspace> {
        Some(self.workspace.as_ref())
    }
}

#[cfg(target_arch = "wasm32")]
fn stdlib_scope(run_label: &str) -> WorkspaceRunScope {
    WorkspaceRunScope::new("host-workers-stdlib", format!("run-{run_label}"))
}

#[cfg(target_arch = "wasm32")]
fn worker_rust_error(message: impl Into<String>) -> worker::Error {
    worker::Error::RustError(message.into())
}

#[cfg(target_arch = "wasm32")]
async fn with_stdlib_workspace<R, F, Fut>(
    env: &Env,
    run_label: &str,
    action: F,
) -> Result<R>
where
    F: FnOnce(Arc<dyn ResourceAccess>, Arc<dyn capabilities::workspace::Workspace>) -> Fut,
    Fut: std::future::Future<Output = NodeResult<R>>,
{
    let factory = WorkersWorkspaceFactory::new(env.clone(), workspace_config_for_path("/workspace"));
    let scope = stdlib_scope(run_label);
    let workspace = factory
        .open(scope.clone())
        .await
        .map_err(|err| worker_rust_error(err.to_string()))?;
    let resources: Arc<dyn ResourceAccess> = Arc::new(WorkspaceOnlyResources {
        workspace: Arc::clone(&workspace),
    });

    let result = capabilities::context::with_resources(Arc::clone(&resources), action(resources, Arc::clone(&workspace))).await;
    let disposition = if result.is_ok() {
        WorkspaceCompletionDisposition::Succeeded
    } else {
        WorkspaceCompletionDisposition::Failed
    };
    factory
        .complete(scope, disposition)
        .await
        .map_err(|err| worker_rust_error(err.to_string()))?;
    result.map_err(|err| worker_rust_error(err.to_string()))
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Default, Deserialize)]
struct StdlibWorkspaceWriteRequest {
    path: Option<String>,
    content: String,
}

#[cfg(target_arch = "wasm32")]
async fn handle_workspace_stdlib_write(mut req: Request, env: &Env) -> Result<Response> {
    let payload: StdlibWorkspaceWriteRequest = req.json().await?;
    let path = payload.path.unwrap_or_else(|| "stdlib/write.txt".to_string());
    let output = with_stdlib_workspace(env, "stdlib-write", move |_, _| async move {
        stdlib::workspace::workspace_write(stdlib::workspace::WorkspaceWriteInput {
            path,
            bytes: payload.content.into_bytes(),
        })
        .await
    })
    .await?;
    Response::from_json(&output)
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Default, Deserialize)]
struct StdlibWorkspaceReadRequest {
    path: Option<String>,
    content: String,
}

#[cfg(target_arch = "wasm32")]
async fn handle_workspace_stdlib_read(mut req: Request, env: &Env) -> Result<Response> {
    let payload: StdlibWorkspaceReadRequest = req.json().await?;
    let path = payload.path.unwrap_or_else(|| "stdlib/read.txt".to_string());
    let content = payload.content;
    let response = with_stdlib_workspace(env, "stdlib-read", move |_, workspace| async move {
        workspace
            .write(
                &path,
                content.as_bytes(),
                capabilities::workspace::WorkspaceWriteOptions::default(),
            )
            .await
            .map_err(|err| NodeError::new(format!("seed stdlib read artifact failed: {err}")))?;
        stdlib::workspace::workspace_read(stdlib::workspace::WorkspaceReadInput { path }).await
    })
    .await?;
    Response::from_json(&response)
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Default, Deserialize)]
struct StdlibWorkspaceListRequest {
    prefix: Option<String>,
}

#[cfg(target_arch = "wasm32")]
async fn handle_workspace_stdlib_list(mut req: Request, env: &Env) -> Result<Response> {
    let payload: StdlibWorkspaceListRequest = req.json().await?;
    let prefix = payload.prefix.unwrap_or_else(|| "stdlib/list".to_string());
    let response = with_stdlib_workspace(env, "stdlib-list", move |_, workspace| async move {
        workspace
            .write(
                &format!("{prefix}/b.txt"),
                b"bbb",
                capabilities::workspace::WorkspaceWriteOptions::default(),
            )
            .await
            .map_err(|err| NodeError::new(format!("seed stdlib list artifact failed: {err}")))?;
        workspace
            .write(
                &format!("{prefix}/a.txt"),
                b"a",
                capabilities::workspace::WorkspaceWriteOptions::default(),
            )
            .await
            .map_err(|err| NodeError::new(format!("seed stdlib list artifact failed: {err}")))?;
        stdlib::workspace::workspace_list(stdlib::workspace::WorkspaceListInput {
            prefix: Some(prefix),
        })
        .await
    })
    .await?;
    Response::from_json(&json!({
        "paths": response.entries.into_iter().map(|entry| entry.path).collect::<Vec<_>>()
    }))
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Default, Deserialize)]
struct StdlibWorkspaceDeleteRequest {
    path: Option<String>,
    content: String,
}

#[cfg(target_arch = "wasm32")]
async fn handle_workspace_stdlib_delete(mut req: Request, env: &Env) -> Result<Response> {
    let payload: StdlibWorkspaceDeleteRequest = req.json().await?;
    let path = payload.path.unwrap_or_else(|| "stdlib/delete.txt".to_string());
    let content = payload.content;
    let response = with_stdlib_workspace(env, "stdlib-delete", move |_, workspace| async move {
        workspace
            .write(
                &path,
                content.as_bytes(),
                capabilities::workspace::WorkspaceWriteOptions::default(),
            )
            .await
            .map_err(|err| NodeError::new(format!("seed stdlib delete artifact failed: {err}")))?;
        stdlib::workspace::workspace_delete(stdlib::workspace::WorkspaceDeleteInput { path }).await
    })
    .await?;
    Response::from_json(&response)
}

#[cfg(target_arch = "wasm32")]
async fn handle_test_checkpoint(req: Request, env: &Env) -> Result<Response> {
    let url = req.url()?;
    let checkpoint_id = url
        .query_pairs()
        .find(|(key, _)| key == "checkpoint_id")
        .map(|(_, value)| value.to_string());

    let checkpoint_id = match checkpoint_id {
        Some(value) if !value.is_empty() => value,
        _ => {
            return Response::from_json(&json!({ "error": "missing checkpoint_id" }))
                .map(|response| response.with_status(400));
        }
    };

    let store = durability_capability(env)?;
    let handles = store
        .list(CheckpointFilter {
            flow_id: None,
            run_id: None,
            status: None,
        })
        .await
        .map_err(|err| worker::Error::RustError(err.to_string()))?;

    let found = handles.iter().any(|handle| handle.checkpoint_id == checkpoint_id);
    Response::from_json(&json!({
        "checkpoint_id": checkpoint_id,
        "found": found,
        "count": handles.len(),
    }))
}

#[cfg(target_arch = "wasm32")]
async fn handle_test_alarm_tick(env: &Env) -> Result<Response> {
    let store = durability_capability(env)?;
    let dispatched = store
        .process_due_schedules()
        .await
        .map_err(|err| worker::Error::RustError(err.to_string()))?;

    Response::from_json(&json!({ "dispatched": dispatched }))
}

#[cfg(target_arch = "wasm32")]
async fn handle_test_workspace_objects(req: Request, env: &Env) -> Result<Response> {
    let url = req.url()?;
    let prefix = url
        .query_pairs()
        .find(|(key, _)| key == "prefix")
        .map(|(_, value)| value.to_string())
        .unwrap_or_else(|| "workspace/".to_string());

    let bucket = env.bucket("WORKSPACE_BUCKET")?;
    let mut keys = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut list = bucket.list().prefix(prefix.clone());
        if let Some(existing) = cursor.as_deref() {
            list = list.cursor(existing.to_string());
        }
        let listed = list.execute().await?;
        keys.extend(listed.objects().into_iter().map(|object| object.key()));
        if !listed.truncated() {
            break;
        }
        cursor = listed.cursor();
        if cursor.is_none() {
            break;
        }
    }

    Response::from_json(&json!({
        "prefix": prefix,
        "count": keys.len(),
        "keys": keys,
    }))
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Deserialize)]
struct WorkspaceRetainedCleanupRequest {
    object_key: String,
    now_ms: Option<u64>,
}

#[cfg(target_arch = "wasm32")]
async fn handle_test_workspace_retained_cleanup(mut req: Request, env: &Env) -> Result<Response> {
    let payload: WorkspaceRetainedCleanupRequest = req.json().await?;
    let scope_name = workspace_scope_name_from_object_key(&payload.object_key).ok_or_else(|| {
        worker::Error::RustError(format!(
            "workspace object key does not encode a scope: {}",
            payload.object_key
        ))
    })?;

    let namespace = env.durable_object("WORKSPACE_DO")?;
    let id = namespace.id_from_name(&scope_name)?;
    let stub = id.get_stub()?;
    let body = serde_json::to_string(&json!({ "now_ms": payload.now_ms }))
        .map_err(|err| worker::Error::RustError(err.to_string()))?;
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    init.with_body(Some(JsString::from(body).into()));
    let request = Request::new_with_init(
        "http://do/__debug/run-retained-cleanup",
        &init,
    )?;
    let mut response = stub.fetch_with_request(request).await?;
    let value: JsonValue = response.json().await?;
    Response::from_json(&value)
}

#[cfg(target_arch = "wasm32")]
fn workspace_scope_name_from_object_key(object_key: &str) -> Option<String> {
    let mut parts = object_key.split('/');
    let prefix = parts.next()?;
    let flow_key = parts.next()?;
    let run_key = parts.next()?;
    if prefix.is_empty() || flow_key.is_empty() || run_key.is_empty() {
        return None;
    }
    Some(format!("{prefix}/{flow_key}/{run_key}"))
}

#[derive(Clone, Debug, Serialize)]
struct StreamEvent {
    index: usize,
    message: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StreamRequest {
    count: Option<u64>,
}

struct StreamEventStream {
    inner: Mutex<Pin<Box<dyn Stream<Item = NodeResult<StreamEvent>> + Send>>>,
}

impl StreamEventStream {
    fn new(stream: impl Stream<Item = NodeResult<StreamEvent>> + Send + 'static) -> Self {
        Self {
            inner: Mutex::new(Box::pin(stream)),
        }
    }
}

impl Stream for StreamEventStream {
    type Item = NodeResult<StreamEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let mut guard = self.inner.lock().expect("stream lock poisoned");
        guard.as_mut().poll_next(cx)
    }
}

impl serde::Serialize for StreamEventStream {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_unit()
    }
}

#[def_node(
    trigger,
    name = "HealthTrigger",
    summary = "Ingress trigger for health",
    effects = "Pure",
    determinism = "Strict"
)]
async fn health_trigger(payload: JsonValue) -> NodeResult<JsonValue> {
    Ok(payload)
}

#[def_node(
    name = "HealthResponse",
    summary = "Return health response",
    effects = "Pure",
    determinism = "Strict"
)]
async fn health_response(_payload: JsonValue) -> NodeResult<JsonValue> {
    Ok(json!({ "status": "ok" }))
}

#[def_node(
    trigger,
    name = "EchoTrigger",
    summary = "Ingress trigger for echo",
    effects = "Pure",
    determinism = "Strict"
)]
async fn echo_trigger(payload: JsonValue) -> NodeResult<JsonValue> {
    Ok(payload)
}

#[def_node(
    name = "EchoResponse",
    summary = "Return echoed payload",
    effects = "Pure",
    determinism = "Strict"
)]
async fn echo_response(payload: JsonValue) -> NodeResult<JsonValue> {
    Ok(json!({ "echoed": payload }))
}

#[def_node(
    trigger,
    name = "StreamTrigger",
    summary = "Ingress trigger for stream",
    effects = "Pure",
    determinism = "Strict"
)]
async fn stream_trigger(payload: JsonValue) -> NodeResult<StreamRequest> {
    let count = payload.get("count").and_then(|value| value.as_u64());
    Ok(StreamRequest { count })
}

#[def_node(
    name = "StreamResponse",
    summary = "Emit SSE stream events",
    effects = "ReadOnly",
    determinism = "BestEffort",
    out = "StreamEvent"
)]
async fn stream_response(request: StreamRequest) -> NodeResult<StreamEventStream> {
    let count = request.count.unwrap_or(3) as usize;
    let stream = stream! {
        for idx in 0..count {
            yield Ok(StreamEvent {
                index: idx,
                message: format!("chunk {}", idx),
            });
        }
    };
    Ok(StreamEventStream::new(stream))
}

fn stream_response_stream_node_spec() -> &'static dag_core::NodeSpec {
    node!(stream_response)
}

fn stream_response_stream_register(registry: &mut NodeRegistry) -> Result<(), RegistryError> {
    registry.register_stream_fn(
        concat!(module_path!(), "::", stringify!(stream_response)),
        stream_response,
    )
}

#[def_node(
    trigger,
    name = "CancelTrigger",
    summary = "Ingress trigger for cancellation",
    effects = "Pure",
    determinism = "Strict"
)]
async fn cancel_trigger(payload: JsonValue) -> NodeResult<JsonValue> {
    Ok(payload)
}

#[def_node(
    name = "CancelResponse",
    summary = "Simulate a long-running task",
    effects = "ReadOnly",
    determinism = "BestEffort"
)]
async fn cancel_response(_payload: JsonValue) -> NodeResult<JsonValue> {
    #[cfg(target_arch = "wasm32")]
    {
        futures::future::pending::<()>().await;
    }
    Ok(json!({ "completed": true }))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LocalTimerWaitInput {
    #[serde(default, with = "humantime_serde")]
    duration: Option<Duration>,
    #[serde(default)]
    payload: JsonValue,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LocalTimerWaitOutput {
    payload: JsonValue,
    scheduled_at_ms: i64,
}

#[def_node(
    trigger,
    name = "TimerTrigger",
    summary = "Ingress trigger for timer resume",
    effects = "Pure",
    determinism = "Strict"
)]
async fn timer_trigger(payload: JsonValue) -> NodeResult<LocalTimerWaitInput> {
    serde_json::from_value(payload)
        .map_err(|err| NodeError::new(format!("invalid timer payload: {err}")))
}

#[def_node(
    name = "TimerWaitLocal",
    summary = "Pause execution for a duration and resume via scheduler",
    effects = "Pure",
    determinism = "Nondeterministic",
    halts = true
)]
async fn timer_wait_local(input: LocalTimerWaitInput) -> NodeResult<LocalTimerWaitOutput> {
    let duration = input.duration.unwrap_or(Duration::from_millis(25));
    let delay_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX).max(1);
    let scheduled_at_ms = (js_sys::Date::now() as i64).saturating_add(delay_ms as i64);

    let schedule_result: Option<Result<(), NodeError>> =
        capabilities::context::with_current_async(|resources| async move {
            let scheduler = resources
                .resume_scheduler()
                .ok_or_else(|| NodeError::new("timer_wait_local requires ResumeScheduler"))?;
            let handle = capabilities::context::current_checkpoint_handle()
                .ok_or_else(|| NodeError::new("timer_wait_local missing checkpoint handle"))?;
            scheduler
                .schedule_after(handle, duration)
                .await
                .map_err(|err| NodeError::new(format!("timer_wait_local schedule failed: {err}")))?;
            Ok(())
        })
        .await;

    if schedule_result.is_none() {
        return Err(NodeError::new("timer_wait_local missing ResourceAccess context"));
    }
    schedule_result.unwrap()?;

    Ok(LocalTimerWaitOutput {
        payload: input.payload,
        scheduled_at_ms,
    })
}

#[def_node(
    name = "TimerCapture",
    summary = "Capture resumed timer output",
    effects = "Pure",
    determinism = "Strict"
)]
async fn timer_capture(payload: LocalTimerWaitOutput) -> NodeResult<JsonValue> {
    Ok(json!({
        "resumed": true,
        "scheduled_at_ms": payload.scheduled_at_ms,
        "payload": payload.payload,
    }))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct WorkspaceRoundtripInput {
    content: String,
    #[serde(default)]
    prefix: Option<String>,
}

#[def_node(
    trigger,
    name = "WorkspaceRoundtripTrigger",
    summary = "Ingress trigger for workspace roundtrip validation",
    effects = "Pure",
    determinism = "Strict"
)]
async fn workspace_roundtrip_trigger(payload: JsonValue) -> NodeResult<WorkspaceRoundtripInput> {
    serde_json::from_value(payload)
        .map_err(|err| NodeError::new(format!("invalid workspace roundtrip payload: {err}")))
}

#[def_node(
    name = "WorkspaceRoundtripStage",
    summary = "Write, read, list, and delete workspace artifacts",
    effects = "Effectful",
    determinism = "BestEffort"
)]
async fn workspace_roundtrip_stage(input: WorkspaceRoundtripInput) -> NodeResult<JsonValue> {
    let prefix = input.prefix.unwrap_or_else(|| "artifacts".to_string());
    let original_path = format!("{prefix}/original.txt");
    let upper_path = format!("{prefix}/upper.txt");
    let content = input.content;
    let upper = content.to_uppercase();

    let result = capabilities::context::with_current_async(|resources| async move {
        let workspace = resources
            .workspace()
            .ok_or_else(|| NodeError::new("workspace_roundtrip_stage missing Workspace capability"))?;

        workspace
            .write(
                &original_path,
                content.as_bytes(),
                capabilities::workspace::WorkspaceWriteOptions::default(),
            )
            .await
            .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
        workspace
            .write(
                &upper_path,
                upper.as_bytes(),
                capabilities::workspace::WorkspaceWriteOptions::default(),
            )
            .await
            .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;

        let original_bytes = workspace
            .read(&original_path)
            .await
            .map_err(|err| NodeError::new(format!("workspace read failed: {err}")))?;
        let upper_bytes = workspace
            .read(&upper_path)
            .await
            .map_err(|err| NodeError::new(format!("workspace read failed: {err}")))?;
        let missing_read = workspace
            .read(&format!("{prefix}/missing.txt"))
            .await
            .map_err(|err| NodeError::new(format!("workspace read failed: {err}")))?;
        let listed_before = workspace
            .list(capabilities::workspace::WorkspaceListOptions::default().with_prefix(&prefix))
            .await
            .map_err(|err| NodeError::new(format!("workspace list failed: {err}")))?;
        let missing_delete = workspace
            .delete(&format!("{prefix}/missing.txt"))
            .await
            .map_err(|err| NodeError::new(format!("workspace delete failed: {err}")))?;
        let deleted_upper = workspace
            .delete(&upper_path)
            .await
            .map_err(|err| NodeError::new(format!("workspace delete failed: {err}")))?;
        let listed_after = workspace
            .list(capabilities::workspace::WorkspaceListOptions::default().with_prefix(&prefix))
            .await
            .map_err(|err| NodeError::new(format!("workspace list failed: {err}")))?;

        let original = decode_workspace_bytes(original_bytes)?;
        let upper_read = decode_workspace_bytes(upper_bytes)?;

        Ok::<JsonValue, NodeError>(json!({
            "prefix": prefix,
            "original_path": original_path,
            "upper_path": upper_path,
            "original": original,
            "upper": upper_read,
            "missing_read": missing_read.is_some(),
            "missing_delete": missing_delete.deleted,
            "deleted_upper": deleted_upper.deleted,
            "listed_paths_before_delete": listed_before.into_iter().map(|entry| entry.path).collect::<Vec<_>>(),
            "listed_paths_after_delete": listed_after.into_iter().map(|entry| entry.path).collect::<Vec<_>>(),
        }))
    })
    .await;

    match result {
        Some(result) => result,
        None => Err(NodeError::new(
            "workspace_roundtrip_stage missing ResourceAccess context",
        )),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct WorkspaceResumeInput {
    content: String,
    #[serde(default, with = "humantime_serde")]
    duration: Option<Duration>,
}

#[def_node(
    trigger,
    name = "WorkspaceResumeTrigger",
    summary = "Ingress trigger for workspace resume validation",
    effects = "Pure",
    determinism = "Strict"
)]
async fn workspace_resume_trigger(payload: JsonValue) -> NodeResult<WorkspaceResumeInput> {
    serde_json::from_value(payload)
        .map_err(|err| NodeError::new(format!("invalid workspace resume payload: {err}")))
}

#[def_node(
    name = "WorkspaceWriteBeforeWait",
    summary = "Write a workspace artifact before a halt/resume boundary",
    effects = "Effectful",
    determinism = "BestEffort"
)]
async fn workspace_write_before_wait(input: WorkspaceResumeInput) -> NodeResult<LocalTimerWaitInput> {
    let path = "resume/input.txt".to_string();
    let content = input.content;
    let duration = input.duration;

    let result = capabilities::context::with_current_async(|resources| async move {
        let workspace = resources
            .workspace()
            .ok_or_else(|| NodeError::new("workspace_write_before_wait missing Workspace capability"))?;
        workspace
            .write(
                &path,
                content.as_bytes(),
                capabilities::workspace::WorkspaceWriteOptions::default(),
            )
            .await
            .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
        Ok::<LocalTimerWaitInput, NodeError>(LocalTimerWaitInput {
            duration,
            payload: json!({
                "content": content,
                "path": path,
            }),
        })
    })
    .await;

    match result {
        Some(result) => result,
        None => Err(NodeError::new(
            "workspace_write_before_wait missing ResourceAccess context",
        )),
    }
}

#[def_node(
    name = "WorkspaceReadAfterWait",
    summary = "Read persisted workspace artifacts after resume",
    effects = "ReadOnly",
    determinism = "BestEffort"
)]
async fn workspace_read_after_wait(payload: LocalTimerWaitOutput) -> NodeResult<JsonValue> {
    let path = payload
        .payload
        .get("path")
        .and_then(|value| value.as_str())
        .ok_or_else(|| NodeError::new("workspace_read_after_wait missing payload.path"))?
        .to_string();

    let result = capabilities::context::with_current_async(|resources| async move {
        let workspace = resources
            .workspace()
            .ok_or_else(|| NodeError::new("workspace_read_after_wait missing Workspace capability"))?;
        let read_back = workspace
            .read(&path)
            .await
            .map_err(|err| NodeError::new(format!("workspace read failed: {err}")))?;
        let listed = workspace
            .list(capabilities::workspace::WorkspaceListOptions::default().with_prefix("resume"))
            .await
            .map_err(|err| NodeError::new(format!("workspace list failed: {err}")))?;
        let content = decode_workspace_bytes(read_back)?;
        Ok::<JsonValue, NodeError>(json!({
            "resumed": true,
            "scheduled_at_ms": payload.scheduled_at_ms,
            "path": path,
            "content": content,
            "listed_paths": listed.into_iter().map(|entry| entry.path).collect::<Vec<_>>(),
        }))
    })
    .await;

    match result {
        Some(result) => result,
        None => Err(NodeError::new(
            "workspace_read_after_wait missing ResourceAccess context",
        )),
    }
}

fn decode_workspace_bytes(
    value: Option<capabilities::workspace::WorkspaceReadResult>,
) -> NodeResult<String> {
    match value {
        Some(capabilities::workspace::WorkspaceReadResult::Bytes(bytes)) => String::from_utf8(bytes)
            .map_err(|err| NodeError::new(format!("invalid utf-8 workspace bytes: {err}"))),
        Some(capabilities::workspace::WorkspaceReadResult::BlobRef(reference)) => Err(NodeError::new(
            format!("unexpected blob ref workspace payload: {reference}"),
        )),
        None => Err(NodeError::new("workspace artifact missing")),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct WorkspaceQuotaInput {
    kind: String,
}

#[def_node(
    trigger,
    name = "WorkspaceQuotaTrigger",
    summary = "Ingress trigger for workspace quota validation",
    effects = "Pure",
    determinism = "Strict"
)]
async fn workspace_quota_trigger(payload: JsonValue) -> NodeResult<WorkspaceQuotaInput> {
    serde_json::from_value(payload)
        .map_err(|err| NodeError::new(format!("invalid workspace quota payload: {err}")))
}

#[def_node(
    name = "WorkspaceQuotaStage",
    summary = "Exercise workspace host policy quota failures",
    effects = "Effectful",
    determinism = "BestEffort"
)]
async fn workspace_quota_stage(input: WorkspaceQuotaInput) -> NodeResult<JsonValue> {
    let result = capabilities::context::with_current_async(|resources| async move {
        let workspace = resources
            .workspace()
            .ok_or_else(|| NodeError::new("workspace_quota_stage missing Workspace capability"))?;

        match input.kind.as_str() {
            "single_file" => {
                let payload = vec![b'x'; 40];
                workspace
                    .write(
                        "quota/too-large.txt",
                        &payload,
                        capabilities::workspace::WorkspaceWriteOptions::default(),
                    )
                    .await
                    .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
            }
            "file_count" => {
                for idx in 0..5 {
                    let path = format!("quota/count-{idx}.txt");
                    workspace
                        .write(
                            &path,
                            b"ok",
                            capabilities::workspace::WorkspaceWriteOptions::default(),
                        )
                        .await
                        .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
                }
            }
            "total_bytes" => {
                for idx in 0..3 {
                    let path = format!("quota/total-{idx}.txt");
                    let payload = vec![b'y'; 24];
                    workspace
                        .write(
                            &path,
                            &payload,
                            capabilities::workspace::WorkspaceWriteOptions::default(),
                        )
                        .await
                        .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
                }
            }
            other => {
                return Err(NodeError::new(format!(
                    "unsupported workspace quota test kind: {other}"
                )));
            }
        }

        Ok::<JsonValue, NodeError>(json!({ "ok": true, "kind": input.kind }))
    })
    .await;

    match result {
        Some(result) => result,
        None => Err(NodeError::new(
            "workspace_quota_stage missing ResourceAccess context",
        )),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct WorkspaceInvalidPathInput {
    kind: String,
}

#[def_node(
    trigger,
    name = "WorkspaceInvalidPathTrigger",
    summary = "Ingress trigger for workspace invalid path validation",
    effects = "Pure",
    determinism = "Strict"
)]
async fn workspace_invalid_path_trigger(payload: JsonValue) -> NodeResult<WorkspaceInvalidPathInput> {
    serde_json::from_value(payload)
        .map_err(|err| NodeError::new(format!("invalid workspace invalid-path payload: {err}")))
}

#[def_node(
    name = "WorkspaceInvalidPathStage",
    summary = "Exercise workspace path normalization failures",
    effects = "Effectful",
    determinism = "BestEffort"
)]
async fn workspace_invalid_path_stage(input: WorkspaceInvalidPathInput) -> NodeResult<JsonValue> {
    let result = capabilities::context::with_current_async(|resources| async move {
        let workspace = resources
            .workspace()
            .ok_or_else(|| NodeError::new("workspace_invalid_path_stage missing Workspace capability"))?;

        match input.kind.as_str() {
            "write_traversal" => {
                workspace
                    .write(
                        "../escape.txt",
                        b"bad",
                        capabilities::workspace::WorkspaceWriteOptions::default(),
                    )
                    .await
                    .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
            }
            "list_traversal" => {
                workspace
                    .list(
                        capabilities::workspace::WorkspaceListOptions::default()
                            .with_prefix("../escape"),
                    )
                    .await
                    .map_err(|err| NodeError::new(format!("workspace list failed: {err}")))?;
            }
            other => {
                return Err(NodeError::new(format!(
                    "unsupported workspace invalid-path test kind: {other}"
                )));
            }
        }

        Ok::<JsonValue, NodeError>(json!({ "ok": true, "kind": input.kind }))
    })
    .await;

    match result {
        Some(result) => result,
        None => Err(NodeError::new(
            "workspace_invalid_path_stage missing ResourceAccess context",
        )),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct WorkspaceRetainedInput {
    content: String,
    #[serde(default)]
    prefix: Option<String>,
}

#[def_node(
    trigger,
    name = "WorkspaceRetainedTrigger",
    summary = "Ingress trigger for retained workspace cleanup validation",
    effects = "Pure",
    determinism = "Strict"
)]
async fn workspace_retained_trigger(payload: JsonValue) -> NodeResult<WorkspaceRetainedInput> {
    serde_json::from_value(payload)
        .map_err(|err| NodeError::new(format!("invalid workspace retained payload: {err}")))
}

#[def_node(
    name = "WorkspaceRetainedStage",
    summary = "Write retained workspace artifacts without deleting them",
    effects = "Effectful",
    determinism = "BestEffort"
)]
async fn workspace_retained_stage(input: WorkspaceRetainedInput) -> NodeResult<JsonValue> {
    let prefix = input.prefix.unwrap_or_else(|| "retained".to_string());
    let path = format!("{prefix}/artifact.txt");
    let content = input.content;

    let result = capabilities::context::with_current_async(|resources| async move {
        let workspace = resources
            .workspace()
            .ok_or_else(|| NodeError::new("workspace_retained_stage missing Workspace capability"))?;
        workspace
            .write(
                &path,
                content.as_bytes(),
                capabilities::workspace::WorkspaceWriteOptions::default(),
            )
            .await
            .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
        let read_back = workspace
            .read(&path)
            .await
            .map_err(|err| NodeError::new(format!("workspace read failed: {err}")))?;
        let listed = workspace
            .list(capabilities::workspace::WorkspaceListOptions::default().with_prefix(&prefix))
            .await
            .map_err(|err| NodeError::new(format!("workspace list failed: {err}")))?;

        Ok::<JsonValue, NodeError>(json!({
            "prefix": prefix,
            "path": path,
            "content": decode_workspace_bytes(read_back)?,
            "listed_paths": listed.into_iter().map(|entry| entry.path).collect::<Vec<_>>(),
        }))
    })
    .await;

    match result {
        Some(result) => result,
        None => Err(NodeError::new(
            "workspace_retained_stage missing ResourceAccess context",
        )),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct WorkspaceMutationInput {
    kind: String,
}

#[def_node(
    trigger,
    name = "WorkspaceMutationTrigger",
    summary = "Ingress trigger for workspace overwrite/delete mutation validation",
    effects = "Pure",
    determinism = "Strict"
)]
async fn workspace_mutation_trigger(payload: JsonValue) -> NodeResult<WorkspaceMutationInput> {
    serde_json::from_value(payload)
        .map_err(|err| NodeError::new(format!("invalid workspace mutation payload: {err}")))
}

#[def_node(
    name = "WorkspaceMutationStage",
    summary = "Exercise overwrite and delete/rewrite accounting behavior",
    effects = "Effectful",
    determinism = "BestEffort"
)]
async fn workspace_mutation_stage(input: WorkspaceMutationInput) -> NodeResult<JsonValue> {
    let result = capabilities::context::with_current_async(|resources| async move {
        let workspace = resources
            .workspace()
            .ok_or_else(|| NodeError::new("workspace_mutation_stage missing Workspace capability"))?;

        match input.kind.as_str() {
            "overwrite_delta" => {
                workspace
                    .write(
                        "mutation/artifact.txt",
                        &vec![b'a'; 32],
                        capabilities::workspace::WorkspaceWriteOptions::default(),
                    )
                    .await
                    .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
                workspace
                    .write(
                        "mutation/artifact.txt",
                        &vec![b'b'; 16],
                        capabilities::workspace::WorkspaceWriteOptions::default(),
                    )
                    .await
                    .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
                workspace
                    .write(
                        "mutation/artifact.txt",
                        &vec![b'c'; 32],
                        capabilities::workspace::WorkspaceWriteOptions::default(),
                    )
                    .await
                    .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
            }
            "delete_rewrite" => {
                workspace
                    .write(
                        "mutation/first.txt",
                        &vec![b'd'; 16],
                        capabilities::workspace::WorkspaceWriteOptions::default(),
                    )
                    .await
                    .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
                workspace
                    .delete("mutation/first.txt")
                    .await
                    .map_err(|err| NodeError::new(format!("workspace delete failed: {err}")))?;
                workspace
                    .write(
                        "mutation/second.txt",
                        &vec![b'e'; 16],
                        capabilities::workspace::WorkspaceWriteOptions::default(),
                    )
                    .await
                    .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
            }
            other => {
                return Err(NodeError::new(format!(
                    "unsupported workspace mutation kind: {other}"
                )));
            }
        }

        let listed = workspace
            .list(capabilities::workspace::WorkspaceListOptions::default().with_prefix("mutation"))
            .await
            .map_err(|err| NodeError::new(format!("workspace list failed: {err}")))?;
        Ok::<JsonValue, NodeError>(json!({
            "ok": true,
            "kind": input.kind,
            "listed_paths": listed.into_iter().map(|entry| entry.path).collect::<Vec<_>>(),
        }))
    })
    .await;

    match result {
        Some(result) => result,
        None => Err(NodeError::new(
            "workspace_mutation_stage missing ResourceAccess context",
        )),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct WorkspaceBlockedPrefixInput {
    kind: String,
}

#[def_node(
    trigger,
    name = "WorkspaceBlockedPrefixTrigger",
    summary = "Ingress trigger for workspace blocked-prefix and path-policy validation",
    effects = "Pure",
    determinism = "Strict"
)]
async fn workspace_blocked_prefix_trigger(payload: JsonValue) -> NodeResult<WorkspaceBlockedPrefixInput> {
    serde_json::from_value(payload)
        .map_err(|err| NodeError::new(format!("invalid workspace blocked-prefix payload: {err}")))
}

#[def_node(
    name = "WorkspaceBlockedPrefixStage",
    summary = "Exercise blocked prefixes and path-depth/length host policy validation",
    effects = "Effectful",
    determinism = "BestEffort"
)]
async fn workspace_blocked_prefix_stage(input: WorkspaceBlockedPrefixInput) -> NodeResult<JsonValue> {
    let result = capabilities::context::with_current_async(|resources| async move {
        let workspace = resources
            .workspace()
            .ok_or_else(|| NodeError::new("workspace_blocked_prefix_stage missing Workspace capability"))?;

        match input.kind.as_str() {
            "write_blocked" => {
                workspace
                    .write(
                        "node_modules/pkg/index.js",
                        b"bad",
                        capabilities::workspace::WorkspaceWriteOptions::default(),
                    )
                    .await
                    .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
            }
            "list_blocked" => {
                workspace
                    .list(
                        capabilities::workspace::WorkspaceListOptions::default()
                            .with_prefix("node_modules"),
                    )
                    .await
                    .map_err(|err| NodeError::new(format!("workspace list failed: {err}")))?;
            }
            "max_depth" => {
                workspace
                    .write(
                        "a/b/c/d/e/f/g.txt",
                        b"deep",
                        capabilities::workspace::WorkspaceWriteOptions::default(),
                    )
                    .await
                    .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
            }
            "max_length" => {
                let long_name = format!("paths/{}", "x".repeat(120));
                workspace
                    .write(
                        &long_name,
                        b"long",
                        capabilities::workspace::WorkspaceWriteOptions::default(),
                    )
                    .await
                    .map_err(|err| NodeError::new(format!("workspace write failed: {err}")))?;
            }
            other => {
                return Err(NodeError::new(format!(
                    "unsupported workspace blocked-prefix kind: {other}"
                )));
            }
        }

        Ok::<JsonValue, NodeError>(json!({ "ok": true, "kind": input.kind }))
    })
    .await;

    match result {
        Some(result) => result,
        None => Err(NodeError::new(
            "workspace_blocked_prefix_stage missing ResourceAccess context",
        )),
    }
}

dag_macros::flow! {
    name: host_workers_test_flow,
    version: "0.1.0",
    profile: Web,
    summary: "Host-workers Miniflare harness flow";

    let health = node!(health_trigger);
    let health_capture = node!(health_response);
    connect!(health -> health_capture);

    let echo = node!(echo_trigger);
    let echo_capture = node!(echo_response);
    connect!(echo -> echo_capture);

    let stream = node!(stream_trigger);
    let stream_capture = stream_response_stream_node_spec();
    connect!(stream -> stream_capture);

    let cancel = node!(cancel_trigger);
    let cancel_capture = node!(cancel_response);
    connect!(cancel -> cancel_capture);

    let timer = node!(timer_trigger);
    let timer_wait = node!(timer_wait_local);
    let timer_capture = node!(timer_capture);
    connect!(timer -> timer_wait);
    connect!(timer_wait -> timer_capture);

    let workspace_roundtrip = node!(workspace_roundtrip_trigger);
    let workspace_roundtrip_capture = node!(workspace_roundtrip_stage);
    connect!(workspace_roundtrip -> workspace_roundtrip_capture);

    let workspace_resume = node!(workspace_resume_trigger);
    let workspace_resume_wait = node!(workspace_write_before_wait);
    let workspace_resume_timer_wait = node!(timer_wait_local);
    let workspace_resume_capture = node!(workspace_read_after_wait);
    connect!(workspace_resume -> workspace_resume_wait);
    connect!(workspace_resume_wait -> workspace_resume_timer_wait);
    connect!(workspace_resume_timer_wait -> workspace_resume_capture);

    let workspace_retained = node!(workspace_retained_trigger);
    let workspace_retained_capture = node!(workspace_retained_stage);
    connect!(workspace_retained -> workspace_retained_capture);

    let workspace_quota = node!(workspace_quota_trigger);
    let workspace_quota_capture = node!(workspace_quota_stage);
    connect!(workspace_quota -> workspace_quota_capture);

    let workspace_invalid_path = node!(workspace_invalid_path_trigger);
    let workspace_invalid_path_capture = node!(workspace_invalid_path_stage);
    connect!(workspace_invalid_path -> workspace_invalid_path_capture);

    let workspace_mutation = node!(workspace_mutation_trigger);
    let workspace_mutation_capture = node!(workspace_mutation_stage);
    connect!(workspace_mutation -> workspace_mutation_capture);

    let workspace_blocked_prefix = node!(workspace_blocked_prefix_trigger);
    let workspace_blocked_prefix_capture = node!(workspace_blocked_prefix_stage);
    connect!(workspace_blocked_prefix -> workspace_blocked_prefix_capture);

    entrypoint!({
        trigger: "health",
        capture: "health_capture",
        route_aliases: ["/health"],
        method: "GET",
        deadline_ms: 500,
    });

    entrypoint!({
        trigger: "echo",
        capture: "echo_capture",
        route_aliases: ["/echo"],
        method: "POST",
        deadline_ms: 1000,
    });

    entrypoint!({
        trigger: "stream",
        capture: "stream_capture",
        route_aliases: ["/stream"],
        method: "POST",
        deadline_ms: 5000,
    });

    entrypoint!({
        trigger: "cancel",
        capture: "cancel_capture",
        route_aliases: ["/cancel"],
        method: "POST",
        deadline_ms: 10000,
    });

    entrypoint!({
        trigger: "timer",
        capture: "timer_capture",
        route_aliases: ["/timer"],
        method: "POST",
        deadline_ms: 10000,
    });

    entrypoint!({
        trigger: "workspace_roundtrip",
        capture: "workspace_roundtrip_capture",
        route_aliases: ["/workspace"],
        method: "POST",
        deadline_ms: 5000,
    });

    entrypoint!({
        trigger: "workspace_resume",
        capture: "workspace_resume_capture",
        route_aliases: ["/workspace-resume"],
        method: "POST",
        deadline_ms: 10000,
    });

    entrypoint!({
        trigger: "workspace_retained",
        capture: "workspace_retained_capture",
        route_aliases: ["/workspace-retained"],
        method: "POST",
        deadline_ms: 5000,
    });

    entrypoint!({
        trigger: "workspace_quota",
        capture: "workspace_quota_capture",
        route_aliases: ["/workspace-quota"],
        method: "POST",
        deadline_ms: 5000,
    });

    entrypoint!({
        trigger: "workspace_invalid_path",
        capture: "workspace_invalid_path_capture",
        route_aliases: ["/workspace-invalid-path"],
        method: "POST",
        deadline_ms: 5000,
    });

    entrypoint!({
        trigger: "workspace_mutation",
        capture: "workspace_mutation_capture",
        route_aliases: ["/workspace-mutation"],
        method: "POST",
        deadline_ms: 5000,
    });

    entrypoint!({
        trigger: "workspace_blocked_prefix",
        capture: "workspace_blocked_prefix_capture",
        route_aliases: ["/workspace-blocked-prefix"],
        method: "POST",
        deadline_ms: 5000,
    });
}

fn bundle_with_policies() -> FlowBundle {
    let mut flow = flow();
    flow.policies.lint.allow_multiple_triggers = Some(true);
    flow.policies.durability.mode = DurabilityMode::Partial;

    let validated_ir = kernel_plan::validate(&flow).expect("flow!: flow validation failed");
    let mut registry = NodeRegistry::new();
    register_nodes(&mut registry);

    let registry = Arc::new(registry);
    let resolver: Arc<dyn kernel_exec::NodeResolver> =
        Arc::new(kernel_exec::RegistryResolver::new(registry.clone()));
    let entrypoints = vec![
        FlowEntrypoint {
            trigger_alias: "health".to_string(),
            capture_alias: "health_capture".to_string(),
            route_path: Some("/health".to_string()),
            method: Some("GET".to_string()),
            deadline: Some(Duration::from_millis(500)),
            route_aliases: vec!["/health".to_string()],
        },
        FlowEntrypoint {
            trigger_alias: "echo".to_string(),
            capture_alias: "echo_capture".to_string(),
            route_path: Some("/echo".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(1000)),
            route_aliases: vec!["/echo".to_string()],
        },
        FlowEntrypoint {
            trigger_alias: "stream".to_string(),
            capture_alias: "stream_capture".to_string(),
            route_path: Some("/stream".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(5000)),
            route_aliases: vec!["/stream".to_string()],
        },
        FlowEntrypoint {
            trigger_alias: "cancel".to_string(),
            capture_alias: "cancel_capture".to_string(),
            route_path: Some("/cancel".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(10000)),
            route_aliases: vec!["/cancel".to_string()],
        },
        FlowEntrypoint {
            trigger_alias: "timer".to_string(),
            capture_alias: "timer_capture".to_string(),
            route_path: Some("/timer".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(10000)),
            route_aliases: vec!["/timer".to_string()],
        },
        FlowEntrypoint {
            trigger_alias: "workspace_roundtrip".to_string(),
            capture_alias: "workspace_roundtrip_capture".to_string(),
            route_path: Some("/workspace".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(5000)),
            route_aliases: vec!["/workspace".to_string()],
        },
        FlowEntrypoint {
            trigger_alias: "workspace_resume".to_string(),
            capture_alias: "workspace_resume_capture".to_string(),
            route_path: Some("/workspace-resume".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(10000)),
            route_aliases: vec!["/workspace-resume".to_string()],
        },
        FlowEntrypoint {
            trigger_alias: "workspace_retained".to_string(),
            capture_alias: "workspace_retained_capture".to_string(),
            route_path: Some("/workspace-retained".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(5000)),
            route_aliases: vec!["/workspace-retained".to_string()],
        },
        FlowEntrypoint {
            trigger_alias: "workspace_quota".to_string(),
            capture_alias: "workspace_quota_capture".to_string(),
            route_path: Some("/workspace-quota".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(5000)),
            route_aliases: vec!["/workspace-quota".to_string()],
        },
        FlowEntrypoint {
            trigger_alias: "workspace_invalid_path".to_string(),
            capture_alias: "workspace_invalid_path_capture".to_string(),
            route_path: Some("/workspace-invalid-path".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(5000)),
            route_aliases: vec!["/workspace-invalid-path".to_string()],
        },
        FlowEntrypoint {
            trigger_alias: "workspace_mutation".to_string(),
            capture_alias: "workspace_mutation_capture".to_string(),
            route_path: Some("/workspace-mutation".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(5000)),
            route_aliases: vec!["/workspace-mutation".to_string()],
        },
        FlowEntrypoint {
            trigger_alias: "workspace_blocked_prefix".to_string(),
            capture_alias: "workspace_blocked_prefix_capture".to_string(),
            route_path: Some("/workspace-blocked-prefix".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(5000)),
            route_aliases: vec!["/workspace-blocked-prefix".to_string()],
        },
    ];
    let node_contracts = vec![
        node!(health_trigger),
        node!(health_response),
        node!(echo_trigger),
        node!(echo_response),
        node!(stream_trigger),
        stream_response_stream_node_spec(),
        node!(cancel_trigger),
        node!(cancel_response),
        node!(timer_trigger),
        node!(timer_wait_local),
        node!(timer_capture),
        node!(workspace_roundtrip_trigger),
        node!(workspace_roundtrip_stage),
        node!(workspace_resume_trigger),
        node!(workspace_write_before_wait),
        node!(workspace_read_after_wait),
        node!(workspace_retained_trigger),
        node!(workspace_retained_stage),
        node!(workspace_quota_trigger),
        node!(workspace_quota_stage),
        node!(workspace_invalid_path_trigger),
        node!(workspace_invalid_path_stage),
        node!(workspace_mutation_trigger),
        node!(workspace_mutation_stage),
        node!(workspace_blocked_prefix_trigger),
        node!(workspace_blocked_prefix_stage),
    ]
    .into_iter()
    .map(|spec| NodeContract {
        identifier: spec.identifier.to_string(),
        contract_hash: None,
        source: NodeSource::Local,
    })
    .collect();

    FlowBundle {
        validated_ir,
        entrypoints,
        resolver,
        node_contracts,
        environment_plugins: Vec::new(),
    }
}

fn register_nodes(registry: &mut NodeRegistry) {
    health_trigger_register(registry).expect("register health_trigger");
    health_response_register(registry).expect("register health_response");
    echo_trigger_register(registry).expect("register echo_trigger");
    echo_response_register(registry).expect("register echo_response");
    stream_trigger_register(registry).expect("register stream_trigger");
    stream_response_stream_register(registry).expect("register stream_response_stream");
    cancel_trigger_register(registry).expect("register cancel_trigger");
    cancel_response_register(registry).expect("register cancel_response");
    timer_trigger_register(registry).expect("register timer_trigger");
    timer_wait_local_register(registry).expect("register timer_wait_local");
    timer_capture_register(registry).expect("register timer_capture");
    workspace_roundtrip_trigger_register(registry)
        .expect("register workspace_roundtrip_trigger");
    workspace_roundtrip_stage_register(registry).expect("register workspace_roundtrip_stage");
    workspace_resume_trigger_register(registry).expect("register workspace_resume_trigger");
    workspace_write_before_wait_register(registry)
        .expect("register workspace_write_before_wait");
    workspace_read_after_wait_register(registry).expect("register workspace_read_after_wait");
    workspace_retained_trigger_register(registry).expect("register workspace_retained_trigger");
    workspace_retained_stage_register(registry).expect("register workspace_retained_stage");
    workspace_quota_trigger_register(registry).expect("register workspace_quota_trigger");
    workspace_quota_stage_register(registry).expect("register workspace_quota_stage");
    workspace_invalid_path_trigger_register(registry)
        .expect("register workspace_invalid_path_trigger");
    workspace_invalid_path_stage_register(registry)
        .expect("register workspace_invalid_path_stage");
    workspace_mutation_trigger_register(registry).expect("register workspace_mutation_trigger");
    workspace_mutation_stage_register(registry).expect("register workspace_mutation_stage");
    workspace_blocked_prefix_trigger_register(registry)
        .expect("register workspace_blocked_prefix_trigger");
    workspace_blocked_prefix_stage_register(registry)
        .expect("register workspace_blocked_prefix_stage");
}

#[unsafe(no_mangle)]
pub extern "Rust" fn get_bundle() -> host_inproc::FlowBundle {
    bundle_with_policies()
}
