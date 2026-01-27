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
use serde_json::{Value as JsonValue, json};
#[cfg(target_arch = "wasm32")]
use worker::wasm_bindgen::closure::Closure;
#[cfg(target_arch = "wasm32")]
use worker::wasm_bindgen::JsCast;
#[cfg(target_arch = "wasm32")]
use worker::event;
#[cfg(target_arch = "wasm32")]
use worker::{AbortController, Context, Env, Headers, Request, Response, Result};

#[cfg(target_arch = "wasm32")]
type AbortFuture = Shared<LocalBoxFuture<'static, ()>>;

#[cfg(target_arch = "wasm32")]
#[event(fetch)]
pub async fn main(mut req: Request, env: Env, _ctx: Context) -> Result<Response> {
    let mut bundle = load_bundle(&env);
    if bundle.entrypoints.is_empty() {
        return Response::error("no entrypoints configured", 500);
    }

    let (trigger_alias, capture_alias, deadline) = match select_entrypoint(&req, &bundle.entrypoints)
    {
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

    let executor = bundle.executor();
    let ir = Arc::new(bundle.validated_ir);
    let runtime = if bundle.environment_plugins.is_empty() {
        HostRuntime::new(executor, Arc::clone(&ir))
    } else {
        HostRuntime::with_plugins(executor, Arc::clone(&ir), bundle.environment_plugins)
    };

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

#[cfg(target_arch = "wasm32")]
fn load_bundle(_env: &Env) -> FlowBundle {
    unsafe { get_bundle() }
}

#[cfg(target_arch = "wasm32")]
unsafe extern "Rust" {
    fn get_bundle() -> FlowBundle;
}

#[cfg(target_arch = "wasm32")]
fn select_entrypoint<'a>(req: &Request, entrypoints: &'a [FlowEntrypoint]) -> Option<&'a FlowEntrypoint> {
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
    if let Some(value) = header_value(req, "x-request-id")
        .or_else(|| header_value(req, "cf-ray"))
    {
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
                    let payload = json!({ "error": "serialization_failure", "message": err.to_string() });
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
        ExecutionError::UnsupportedSpill { message } => {
            (400, json!({ "error": "unsupported_spill", "message": message }))
        }
        ExecutionError::SpillSetup(err) => (
            500,
            json!({ "error": format!("failed to configure spill storage: {err}") }),
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
