//! Host workers adapter.

#[cfg(target_arch = "wasm32")]
use std::cell::RefCell;
#[cfg(target_arch = "wasm32")]
use std::collections::BTreeMap;
#[cfg(target_arch = "wasm32")]
use std::rc::Rc;
#[cfg(target_arch = "wasm32")]
use std::sync::Arc;

#[cfg(target_arch = "wasm32")]
use std::time::Duration;

#[cfg(target_arch = "wasm32")]
use cap_workspace_workers::{
    DEFAULT_WORKSPACE_BUCKET_BINDING, DEFAULT_WORKSPACE_DO_BINDING,
    DEFAULT_WORKSPACE_OBJECT_PREFIX, WorkersWorkspaceConfig, WorkersWorkspaceFactory,
};
#[cfg(target_arch = "wasm32")]
use capabilities::{
    ResourceAccess, ResourceBag,
    durability::ResumeToken,
    workspace::{WorkspaceFactory, WorkspacePolicy},
};
#[cfg(target_arch = "wasm32")]
use futures::channel::oneshot;
#[cfg(target_arch = "wasm32")]
use futures::future::{FutureExt, LocalBoxFuture, Shared};
#[cfg(target_arch = "wasm32")]
use futures::pin_mut;
#[cfg(target_arch = "wasm32")]
use futures::stream::{self, StreamExt};
#[cfg(target_arch = "wasm32")]
use host_inproc::{FlowBundle, FlowEntrypoint, HostRuntime, Invocation, InvocationMetadata};
#[cfg(target_arch = "wasm32")]
use kernel_exec::{ExecutionError, ExecutionResult, StreamHandle};
#[cfg(target_arch = "wasm32")]
use serde::Deserialize;
#[cfg(target_arch = "wasm32")]
use serde_json::{Value as JsonValue, json};
#[cfg(target_arch = "wasm32")]
use tokio::runtime::{Builder as RuntimeBuilder, Handle as RuntimeHandle};
#[cfg(all(target_arch = "wasm32", feature = "entrypoint"))]
use worker::event;
#[cfg(target_arch = "wasm32")]
use worker::wasm_bindgen::JsCast;
#[cfg(target_arch = "wasm32")]
use worker::wasm_bindgen::closure::Closure;
#[cfg(target_arch = "wasm32")]
use worker::{AbortController, Context, Env, Headers, Request, Response, Result};

#[cfg(target_arch = "wasm32")]
type AbortFuture = Shared<LocalBoxFuture<'static, ()>>;

#[cfg(target_arch = "wasm32")]
thread_local! {
    static RESOURCE_OVERRIDE: RefCell<Option<Arc<dyn ResourceAccess>>> = const { RefCell::new(None) };
    static WORKSPACE_FACTORY_OVERRIDE: RefCell<Option<Arc<dyn WorkspaceFactory>>> = const { RefCell::new(None) };
}

#[cfg(target_arch = "wasm32")]
pub fn set_resource_access(resources: Arc<dyn ResourceAccess>) {
    RESOURCE_OVERRIDE.with(|slot| {
        *slot.borrow_mut() = Some(resources);
    });
}

#[cfg(target_arch = "wasm32")]
pub fn set_resource_bag(bag: ResourceBag) {
    let resources: Arc<ResourceBag> = Arc::new(bag);
    set_resource_access(resources);
}

#[cfg(target_arch = "wasm32")]
pub fn set_workspace_factory(factory: Arc<dyn WorkspaceFactory>) {
    WORKSPACE_FACTORY_OVERRIDE.with(|slot| {
        *slot.borrow_mut() = Some(factory);
    });
}

#[cfg(target_arch = "wasm32")]
fn get_resource_access() -> Option<Arc<dyn ResourceAccess>> {
    RESOURCE_OVERRIDE.with(|slot| slot.borrow().clone())
}

#[cfg(target_arch = "wasm32")]
fn get_workspace_factory() -> Option<Arc<dyn WorkspaceFactory>> {
    WORKSPACE_FACTORY_OVERRIDE.with(|slot| slot.borrow().clone())
}

#[cfg(target_arch = "wasm32")]
pub async fn handle_fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    if RuntimeHandle::try_current().is_err() {
        let runtime = match RuntimeBuilder::new_current_thread().build() {
            Ok(runtime) => runtime,
            Err(err) => return Response::error(format!("tokio runtime init failed: {err}"), 500),
        };
        let _guard = runtime.enter();
        return handle_fetch_inner(req, env).await;
    }

    handle_fetch_inner(req, env).await
}

#[cfg(target_arch = "wasm32")]
async fn handle_fetch_inner(mut req: Request, env: Env) -> Result<Response> {
    let bundle = load_bundle(&env);
    if bundle.entrypoints.is_empty() {
        return Response::error("no entrypoints configured", 500);
    }

    if is_internal_resume_request(&req) {
        return handle_internal_resume(req, &env, bundle).await;
    }

    let (trigger_alias, capture_alias, deadline) =
        match select_entrypoint(&req, &bundle.entrypoints) {
            Some(entrypoint) => (
                entrypoint.trigger_alias.clone(),
                entrypoint.capture_alias.clone(),
                entrypoint.deadline,
            ),
            None => return Response::error("route not found", 404),
        };

    let payload = match read_payload(&mut req).await {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };

    let abort_bridge = AbortBridge::new(&req);

    let runtime = runtime_from_bundle(bundle, Some(&env));

    let mut invocation =
        Invocation::new(trigger_alias, capture_alias, payload).with_deadline(deadline);

    populate_http_metadata(&req, invocation.metadata_mut());
    populate_lattice_metadata(&req, invocation.metadata_mut());

    let exec_future = runtime.execute(invocation).fuse();
    let abort_future = abort_bridge.abort_future.clone().fuse();
    pin_mut!(exec_future);
    pin_mut!(abort_future);
    let exec_result = futures::select! {
        result = exec_future => result,
        _ = abort_future => Err(ExecutionError::Cancelled),
    };

    match exec_result {
        Ok(ExecutionResult::Value(value)) => Response::from_json(&value),
        Ok(ExecutionResult::Stream(stream)) => streaming_response(stream, abort_bridge),
        Ok(ExecutionResult::Halt { alias, payload }) => {
            let body = json!({
                "halted": true,
                "node": alias,
                "payload": payload,
            });
            Response::from_json(&body).map(|response| response.with_status(202))
        }
        Err(err) => {
            let wants_sse = wants_sse(&req);
            if wants_sse {
                let (_, body) = map_execution_error(err);
                return sse_error_response(body);
            }

            let (status, body) = map_execution_error(err);
            json_response(status, body)
        }
    }
}

#[cfg(all(target_arch = "wasm32", feature = "entrypoint"))]
#[event(fetch)]
pub async fn main(req: Request, env: Env, ctx: Context) -> Result<Response> {
    handle_fetch(req, env, ctx).await
}

#[cfg(target_arch = "wasm32")]
fn load_bundle(_env: &Env) -> FlowBundle {
    unsafe { get_bundle() }
}

#[cfg(target_arch = "wasm32")]
unsafe extern "Rust" {
    fn get_bundle() -> FlowBundle;
}

#[cfg(target_arch = "wasm32")]
fn runtime_from_bundle(bundle: FlowBundle, env: Option<&Env>) -> HostRuntime {
    let executor = bundle.executor();
    let ir = Arc::new(bundle.validated_ir);
    let mut runtime = if bundle.environment_plugins.is_empty() {
        HostRuntime::new(executor, Arc::clone(&ir))
    } else {
        HostRuntime::with_plugins(executor, Arc::clone(&ir), bundle.environment_plugins)
    };
    if let Some(env) = env {
        if let Some(bundle_id) = env
            .var("LATTICE_BUNDLE_ID")
            .ok()
            .map(|value| value.to_string())
        {
            runtime = runtime.with_bundle_id(bundle_id);
        }
    }
    if let Some(factory) =
        get_workspace_factory().or_else(|| env.and_then(workspace_factory_from_env))
    {
        runtime = runtime.with_workspace_factory(factory);
    }
    match get_resource_access() {
        Some(resources) => runtime.with_resource_access(resources),
        None => runtime,
    }
}

#[cfg(target_arch = "wasm32")]
fn workspace_factory_from_env(env: &Env) -> Option<Arc<dyn WorkspaceFactory>> {
    let bucket_binding = env_string(env, "LATTICE_WORKSPACE_BUCKET_BINDING")
        .unwrap_or_else(|| DEFAULT_WORKSPACE_BUCKET_BINDING.to_string());
    let index_binding = env_string(env, "LATTICE_WORKSPACE_DO_BINDING")
        .unwrap_or_else(|| DEFAULT_WORKSPACE_DO_BINDING.to_string());

    if env.bucket(&bucket_binding).is_err() || env.durable_object(&index_binding).is_err() {
        return None;
    }

    let object_prefix = env_string(env, "LATTICE_WORKSPACE_OBJECT_PREFIX")
        .unwrap_or_else(|| DEFAULT_WORKSPACE_OBJECT_PREFIX.to_string());
    let policy = WorkspacePolicy {
        max_total_bytes: env_u64(env, "LATTICE_WORKSPACE_MAX_TOTAL_BYTES"),
        max_file_count: env_u64(env, "LATTICE_WORKSPACE_MAX_FILE_COUNT"),
        max_single_file_bytes: env_u64(env, "LATTICE_WORKSPACE_MAX_SINGLE_FILE_BYTES"),
        retain_completed_for: env_u64(env, "LATTICE_WORKSPACE_RETAIN_COMPLETED_FOR_MS")
            .map(Duration::from_millis),
    };

    Some(Arc::new(WorkersWorkspaceFactory::new(
        env.clone(),
        WorkersWorkspaceConfig {
            bucket_binding,
            index_binding,
            object_prefix,
            policy,
        },
    )))
}

#[cfg(target_arch = "wasm32")]
fn env_string(env: &Env, name: &str) -> Option<String> {
    env.var(name).ok().map(|value| value.to_string())
}

#[cfg(target_arch = "wasm32")]
fn env_u64(env: &Env, name: &str) -> Option<u64> {
    env_string(env, name).and_then(|value| value.parse::<u64>().ok())
}

#[cfg(target_arch = "wasm32")]
fn is_internal_resume_request(req: &Request) -> bool {
    req.path() == "/__lattice/resume" && req.method().as_ref().eq_ignore_ascii_case("POST")
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Deserialize)]
struct InternalResumeRequest {
    #[serde(default)]
    checkpoint_id: Option<String>,
    #[serde(default)]
    token: Option<String>,
}

#[cfg(target_arch = "wasm32")]
async fn handle_internal_resume(
    mut req: Request,
    env: &Env,
    bundle: FlowBundle,
) -> Result<Response> {
    if let Some(expected) = env
        .var("LATTICE_INTERNAL_RESUME_TOKEN")
        .ok()
        .map(|value| value.to_string())
    {
        let provided = header_value(&req, "x-lattice-internal-token");
        if provided.as_deref() != Some(expected.as_str()) {
            return json_response(401, json!({ "error": "unauthorized" }));
        }
    }

    let body = req
        .bytes()
        .await
        .map_err(|err| worker::Error::RustError(format!("failed to read resume body: {err}")))?;
    let payload = if body.is_empty() {
        InternalResumeRequest {
            checkpoint_id: None,
            token: None,
        }
    } else {
        match serde_json::from_slice::<InternalResumeRequest>(&body) {
            Ok(payload) => payload,
            Err(err) => {
                return json_response(
                    400,
                    json!({ "error": format!("invalid request body: {err}") }),
                );
            }
        }
    };

    let runtime = runtime_from_bundle(bundle, Some(env));
    let checkpoint_id = if let Some(checkpoint_id) = payload.checkpoint_id {
        checkpoint_id
    } else if let Some(token) = payload.token {
        let resources = runtime.resources();
        let Some(source) = resources.resume_signal_source() else {
            return json_response(
                500,
                json!({
                    "error": "resume token resolution unavailable",
                    "code": "DAG-CKPT-003",
                }),
            );
        };
        match source.resolve_token(&ResumeToken(token)).await {
            Ok(handle) => handle.checkpoint_id,
            Err(err) => {
                return json_response(
                    400,
                    json!({ "error": format!("invalid resume token: {err}") }),
                );
            }
        }
    } else {
        return json_response(
            400,
            json!({ "error": "missing checkpoint_id or token in resume request" }),
        );
    };

    match runtime.resume(&checkpoint_id).await {
        Ok(ExecutionResult::Value(value)) => {
            json_response(200, json!({ "resumed": true, "result": value }))
        }
        Ok(ExecutionResult::Halt { alias, payload }) => json_response(
            202,
            json!({ "halted": true, "node": alias, "payload": payload }),
        ),
        Ok(ExecutionResult::Stream(mut stream)) => {
            let mut events = Vec::new();
            while let Some(item) = stream.next().await {
                let payload = match item {
                    Ok(payload) => payload,
                    Err(err) => {
                        let (status, body) = map_execution_error(err);
                        return json_response(status, body);
                    }
                };
                events.push(payload);
            }
            json_response(200, json!({ "resumed": true, "stream": events }))
        }
        Err(err) => {
            let (status, body) = map_execution_error(err);
            json_response(status, body)
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn select_entrypoint<'a>(
    req: &Request,
    entrypoints: &'a [FlowEntrypoint],
) -> Option<&'a FlowEntrypoint> {
    let path = req.path();
    let method = req.method();
    let method_str = method.as_ref();
    entrypoints.iter().find(|entry| {
        let route_path = entry.route_path.as_deref().unwrap_or("/");
        let route_method = entry.method.as_deref().unwrap_or("POST");
        route_path == path && route_method.eq_ignore_ascii_case(method_str)
    })
}

#[cfg(target_arch = "wasm32")]
async fn read_payload(req: &mut Request) -> std::result::Result<JsonValue, Response> {
    let bytes = match req.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => return Err(internal_error("body_read", err)),
    };

    if bytes.is_empty() {
        if let Ok(url) = req.url() {
            let mut map = serde_json::Map::new();
            for (key, value) in url.query_pairs() {
                match map.get_mut(key.as_ref()) {
                    Some(existing) => match existing {
                        JsonValue::Array(items) => items.push(JsonValue::String(value.to_string())),
                        JsonValue::String(prev) => {
                            let prev_value = std::mem::take(prev);
                            *existing = JsonValue::Array(vec![
                                JsonValue::String(prev_value),
                                JsonValue::String(value.to_string()),
                            ]);
                        }
                        _ => {
                            *existing =
                                JsonValue::Array(vec![JsonValue::String(value.to_string())]);
                        }
                    },
                    None => {
                        map.insert(key.to_string(), JsonValue::String(value.to_string()));
                    }
                }
            }
            if !map.is_empty() {
                return Ok(JsonValue::Object(map));
            }
        }
        return Ok(JsonValue::Null);
    }

    serde_json::from_slice(&bytes).map_err(|err| bad_request(err.to_string()))
}

#[cfg(target_arch = "wasm32")]
fn wants_sse(req: &Request) -> bool {
    req.headers()
        .get("accept")
        .ok()
        .flatten()
        .map(|value| value.contains("text/event-stream"))
        .unwrap_or(false)
}

#[cfg(target_arch = "wasm32")]
fn populate_lattice_metadata(req: &Request, metadata: &mut InvocationMetadata) {
    if let Some(value) = header_value(req, "x-request-id").or_else(|| header_value(req, "cf-ray")) {
        metadata.insert_label("lf.request_id", value);
    }

    if let Some(value) = header_value(req, "x-event-id") {
        metadata.insert_label("lf.event_id", value);
    }

    if let Some(value) = header_value(req, "x-idempotency-key") {
        metadata.insert_label("lf.idem_key", value);
    }
}

#[cfg(target_arch = "wasm32")]
fn populate_http_metadata(req: &Request, metadata: &mut InvocationMetadata) {
    metadata.insert_label("http.method", req.method().as_ref());
    metadata.insert_label("http.path", req.path());

    if let Ok(url) = req.url() {
        if let Some(query) = url.query() {
            metadata.insert_label("http.query_raw", query.to_string());
            let mut query_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for (key, value) in url.query_pairs() {
                query_map
                    .entry(key.to_string())
                    .or_default()
                    .push(value.to_string());
            }
            if !query_map.is_empty() {
                metadata.insert_extension("http.query", &query_map);
            }
        }
    }

    if let Some(value) = header_value(req, "host") {
        metadata.insert_label("http.host", value);
    }

    if let Some(cf) = req.cf() {
        metadata.insert_label("http.version", cf.http_protocol());
    }

    let mut header_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in req.headers().entries() {
        header_map.entry(name).or_default().push(value);
    }
    if !header_map.is_empty() {
        metadata.insert_extension("http.headers", &header_map);
    }

    if let Some(raw) = header_value(req, "x-auth-user") {
        if let Ok(value) = serde_json::from_str::<JsonValue>(&raw) {
            metadata.insert_extension("auth.user", value);
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn header_value(req: &Request, name: &str) -> Option<String> {
    req.headers().get(name).ok().flatten()
}

#[cfg(target_arch = "wasm32")]
fn streaming_response(stream: StreamHandle, abort_bridge: AbortBridge) -> Result<Response> {
    let abort_future = abort_bridge.abort_future.clone();
    let listener = abort_bridge.listener;
    let events = stream.map(move |item| {
        let _keep_listener = &listener;
        match item {
            Ok(payload) => match serde_json::to_string(&payload) {
                Ok(data) => Ok::<Vec<u8>, worker::Error>(sse_data(&data)),
                Err(err) => {
                    let payload =
                        json!({ "error": "serialization_failure", "message": err.to_string() });
                    Ok(sse_error(&payload.to_string()))
                }
            },
            Err(err) => {
                let payload = json!({ "error": err.to_string() });
                Ok(sse_error(&payload.to_string()))
            }
        }
    });

    let stream = events.take_until(abort_future);
    let response = Response::from_stream(stream)?;
    let headers = sse_headers()?;
    Ok(response.with_headers(headers))
}

#[cfg(target_arch = "wasm32")]
fn sse_error_response(payload: JsonValue) -> Result<Response> {
    let message = payload.to_string();
    let stream = stream::once(async move { Ok::<Vec<u8>, worker::Error>(sse_error(&message)) });
    let response = Response::from_stream(stream)?;
    let headers = sse_headers()?;
    Ok(response.with_headers(headers))
}

#[cfg(target_arch = "wasm32")]
fn sse_headers() -> Result<Headers> {
    let headers = Headers::new();
    headers.set("content-type", "text/event-stream")?;
    headers.set("cache-control", "no-cache")?;
    headers.set("connection", "keep-alive")?;
    Ok(headers)
}

#[cfg(target_arch = "wasm32")]
fn sse_data(payload: &str) -> Vec<u8> {
    format!("data: {payload}\n\n").into_bytes()
}

#[cfg(target_arch = "wasm32")]
fn sse_error(payload: &str) -> Vec<u8> {
    format!("event: error\ndata: {payload}\n\n").into_bytes()
}

#[cfg(target_arch = "wasm32")]
fn map_execution_error(err: ExecutionError) -> (u16, JsonValue) {
    match err {
        ExecutionError::DeadlineExceeded { .. } => (504, json!({ "error": "deadline exceeded" })),
        ExecutionError::NodeFailed { alias, source } => (
            500,
            json!({ "error": format!("node `{alias}` failed: {source}") }),
        ),
        ExecutionError::MissingOutput { alias } => (
            500,
            json!({ "error": format!("capture `{alias}` produced no output") }),
        ),
        ExecutionError::UnknownTrigger { alias } => (
            500,
            json!({ "error": format!("unknown trigger alias `{alias}`") }),
        ),
        ExecutionError::UnknownCapture { alias } => (
            500,
            json!({ "error": format!("unknown capture alias `{alias}`") }),
        ),
        ExecutionError::UnregisteredNode { identifier } => (
            500,
            json!({ "error": format!("no handler registered for node `{identifier}`") }),
        ),
        ExecutionError::MissingCapabilities { hints } => (
            500,
            json!({
                "error": "missing required capabilities",
                "code": "CAP101",
                "details": { "hints": hints }
            }),
        ),
        ExecutionError::MissingDurabilityServices { missing } => (
            500,
            json!({
                "error": "missing required durability services",
                "code": "DAG-CKPT-003",
                "details": { "missing": missing }
            }),
        ),
        ExecutionError::Cancelled => (503, json!({ "error": "execution cancelled" })),
        ExecutionError::UnsupportedControlSurface { id, kind } => (
            500,
            json!({
                "error": format!("unsupported control surface `{id}` ({kind})"),
                "code": "CTRL901",
                "details": { "id": id, "kind": kind }
            }),
        ),
        ExecutionError::InvalidControlSurface { id, kind, reason } => {
            let code = match kind.as_str() {
                "if" => "CTRL120",
                "switch" => "CTRL110",
                _ => "CTRL110",
            };
            (
                500,
                json!({
                    "error": format!("invalid control surface `{id}` ({kind}): {reason}"),
                    "code": code,
                    "details": { "id": id, "kind": kind }
                }),
            )
        }
        ExecutionError::CheckpointNotFound { checkpoint_id } => (
            404,
            json!({
                "error": "checkpoint not found",
                "code": "DAG-CKPT-006",
                "details": { "checkpoint_id": checkpoint_id }
            }),
        ),
        ExecutionError::CheckpointLeaseConflict { checkpoint_id } => (
            409,
            json!({
                "error": "checkpoint lease conflict",
                "code": "DAG-CKPT-007",
                "details": { "checkpoint_id": checkpoint_id }
            }),
        ),
        ExecutionError::CheckpointStateCorrupted {
            checkpoint_id,
            message,
        } => (
            500,
            json!({
                "error": "checkpoint state corrupted",
                "code": "DAG-CKPT-008",
                "details": { "checkpoint_id": checkpoint_id, "message": message }
            }),
        ),
        ExecutionError::CheckpointIncompatibleVersion {
            checkpoint_id,
            version,
        } => (
            500,
            json!({
                "error": "checkpoint version incompatible",
                "code": "DAG-CKPT-009",
                "details": { "checkpoint_id": checkpoint_id, "version": version }
            }),
        ),
        ExecutionError::CheckpointPinnedBundleUnavailable {
            checkpoint_id,
            required_bundle_id,
            runtime_bundle_id,
        } => (
            409,
            json!({
                "error": "checkpoint pinned bundle unavailable",
                "code": "DAG-CKPT-010",
                "details": {
                    "checkpoint_id": checkpoint_id,
                    "required_bundle_id": required_bundle_id,
                    "runtime_bundle_id": runtime_bundle_id,
                }
            }),
        ),
        ExecutionError::UnsupportedSpill { message } => (
            400,
            json!({ "error": "unsupported_spill", "message": message }),
        ),
        ExecutionError::SpillSetup(err) => (
            500,
            json!({ "error": format!("failed to configure spill storage: {err}") }),
        ),
        ExecutionError::HostEnvironment(err) => (
            500,
            json!({
                "error": "host environment failure",
                "message": err.to_string(),
            }),
        ),
    }
}

#[cfg(target_arch = "wasm32")]
fn json_response(status: u16, body: JsonValue) -> Result<Response> {
    Ok(Response::from_json(&body)?.with_status(status))
}

#[cfg(target_arch = "wasm32")]
fn bad_request(message: String) -> Response {
    Response::from_json(&json!({ "error": message }))
        .map(|response| response.with_status(400))
        .unwrap_or_else(|_| Response::error("bad request", 400).unwrap())
}

#[cfg(target_arch = "wasm32")]
fn internal_error(label: &str, err: impl std::fmt::Display) -> Response {
    Response::from_json(&json!({ "error": format!("{label} failed: {err}") }))
        .map(|response| response.with_status(500))
        .unwrap_or_else(|_| Response::error("internal error", 500).unwrap())
}

#[cfg(target_arch = "wasm32")]
struct AbortBridge {
    abort_future: AbortFuture,
    listener: AbortListener,
}

#[cfg(target_arch = "wasm32")]
impl AbortBridge {
    fn new(req: &Request) -> Self {
        let controller = AbortController::default();
        let (sender, receiver) = oneshot::channel();
        let listener = AbortListener::new(req, controller, sender);
        let abort_future = receiver.map(|_| ()).boxed_local().shared();
        Self {
            abort_future,
            listener,
        }
    }
}

#[cfg(target_arch = "wasm32")]
struct AbortListener {
    signal: worker::web_sys::AbortSignal,
    callback: Closure<dyn FnMut()>,
}

#[cfg(target_arch = "wasm32")]
impl AbortListener {
    fn new(req: &Request, controller: AbortController, sender: oneshot::Sender<()>) -> Self {
        let request_signal = req.inner().signal();
        let controller = Rc::new(RefCell::new(Some(controller)));
        let sender = Rc::new(RefCell::new(Some(sender)));
        let callback = {
            let controller = Rc::clone(&controller);
            let sender = Rc::clone(&sender);
            Closure::wrap(Box::new(move || {
                trigger_abort(&controller, &sender);
            }) as Box<dyn FnMut()>)
        };

        let _ = request_signal
            .add_event_listener_with_callback("abort", callback.as_ref().unchecked_ref());
        if request_signal.aborted() {
            trigger_abort(&controller, &sender);
        }

        Self {
            signal: request_signal,
            callback,
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl Drop for AbortListener {
    fn drop(&mut self) {
        let _ = self
            .signal
            .remove_event_listener_with_callback("abort", self.callback.as_ref().unchecked_ref());
    }
}

#[cfg(target_arch = "wasm32")]
fn trigger_abort(
    controller: &Rc<RefCell<Option<AbortController>>>,
    sender: &Rc<RefCell<Option<oneshot::Sender<()>>>>,
) {
    if let Some(controller) = controller.borrow_mut().take() {
        controller.abort();
    }
    if let Some(sender) = sender.borrow_mut().take() {
        let _ = sender.send(());
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn main() {
    panic!("host-workers requires wasm32-unknown-unknown");
}
