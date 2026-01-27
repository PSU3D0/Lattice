use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::stream;
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{Method, Request, StatusCode};
use axum::response::{
    IntoResponse, Response,
    sse::{Event, KeepAlive, Sse},
};
use axum::routing::{MethodRouter, delete, get, patch, post, put};
use capabilities::ResourceBag;
use dag_core::NodeKind;
use futures::StreamExt;
use host_inproc::{EnvironmentPlugin, HostRuntime, Invocation, InvocationMetadata};
use kernel_exec::{ExecutionError, ExecutionResult, FlowExecutor, StreamHandle};
use kernel_plan::ValidatedIR;
use serde_json::{Value as JsonValue, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tower::make::Shared;
use tracing::{error, info, instrument, warn};

/// Configuration describing a single Flow IR route exposed via Axum.
#[derive(Clone)]
pub struct RouteConfig {
    pub path: String,
    pub method: Method,
    pub trigger_alias: String,
    pub capture_alias: String,
    pub deadline: Option<Duration>,
    pub resources: ResourceBag,
    pub environment_plugins: Vec<Arc<dyn EnvironmentPlugin>>,
}

impl RouteConfig {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            method: Method::POST,
            trigger_alias: "trigger".to_string(),
            capture_alias: "respond".to_string(),
            deadline: None,
            resources: ResourceBag::new(),
            environment_plugins: Vec::new(),
        }
    }

    pub fn with_method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }

    pub fn with_trigger_alias(mut self, alias: impl Into<String>) -> Self {
        self.trigger_alias = alias.into();
        self
    }

    pub fn with_capture_alias(mut self, alias: impl Into<String>) -> Self {
        self.capture_alias = alias.into();
        self
    }

    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub fn with_resources(mut self, resources: ResourceBag) -> Self {
        self.resources = resources;
        self
    }

    pub fn with_environment_plugin(mut self, plugin: Arc<dyn EnvironmentPlugin>) -> Self {
        self.environment_plugins.push(plugin);
        self
    }
}

#[derive(Debug)]
struct HostMetrics {
    host: &'static str,
    route: Arc<str>,
    flow: Arc<str>,
}

impl HostMetrics {
    fn new(host: &'static str, route: String, flow: String) -> Self {
        Self {
            host,
            route: Arc::from(route),
            flow: Arc::from(flow),
        }
    }

    fn host_label(&self) -> &'static str {
        self.host
    }

    fn route_label(&self) -> String {
        self.route.to_string()
    }

    fn flow_label(&self) -> String {
        self.flow.to_string()
    }

    fn start_request(self: &Arc<Self>) -> RequestMetricsGuard {
        self.increment_inflight();
        RequestMetricsGuard {
            metrics: Arc::clone(self),
            start: Instant::now(),
            finished: false,
        }
    }

    fn increment_inflight(&self) {
        let host_label = self.host_label();
        let flow_label = self.flow_label();
        let route_label = self.route_label();
        metrics::gauge!(
            "lattice.host.http_inflight_requests",
            "host" => host_label,
            "flow" => flow_label,
            "route" => route_label
        )
        .increment(1.0);
    }

    fn decrement_inflight(&self) {
        let host_label = self.host_label();
        let flow_label = self.flow_label();
        let route_label = self.route_label();
        metrics::gauge!(
            "lattice.host.http_inflight_requests",
            "host" => host_label,
            "flow" => flow_label,
            "route" => route_label
        )
        .decrement(1.0);
    }

    fn finish_request(&self, start: Instant, status: StatusCode, deadline_exceeded: bool) {
        self.decrement_inflight();
        let host_label = self.host_label();
        let flow_label = self.flow_label();
        let route_label = self.route_label();
        let latency_ms = start.elapsed().as_secs_f64() * 1_000.0;
        metrics::histogram!(
            "lattice.host.http_request_latency_ms",
            "host" => host_label,
            "flow" => flow_label.clone(),
            "route" => route_label.clone()
        )
        .record(latency_ms);

        let status_class = format!("{}xx", status.as_u16() / 100);
        metrics::counter!(
            "lattice.host.http_requests_total",
            "host" => host_label,
            "flow" => flow_label.clone(),
            "route" => route_label.clone(),
            "status_class" => status_class
        )
        .increment(1);

        if deadline_exceeded {
            metrics::counter!(
                "lattice.host.deadline_exceeded_total",
                "host" => host_label,
                "flow" => flow_label,
                "route" => route_label
            )
            .increment(1);
        }
    }

    fn track_sse_client(self: &Arc<Self>) -> SseClientGuard {
        let host_label = self.host_label();
        let flow_label = self.flow_label();
        let route_label = self.route_label();
        metrics::gauge!(
            "lattice.host.sse_clients",
            "host" => host_label,
            "flow" => flow_label,
            "route" => route_label
        )
        .increment(1.0);
        SseClientGuard {
            metrics: Arc::clone(self),
            active: true,
        }
    }

    fn sse_client_disconnected(&self) {
        let host_label = self.host_label();
        let flow_label = self.flow_label();
        let route_label = self.route_label();
        metrics::gauge!(
            "lattice.host.sse_clients",
            "host" => host_label,
            "flow" => flow_label,
            "route" => route_label
        )
        .decrement(1.0);
    }
}

struct RequestMetricsGuard {
    metrics: Arc<HostMetrics>,
    start: Instant,
    finished: bool,
}

impl RequestMetricsGuard {
    fn finish(mut self, status: StatusCode, deadline_exceeded: bool) {
        self.metrics
            .finish_request(self.start, status, deadline_exceeded);
        self.finished = true;
    }
}

impl Drop for RequestMetricsGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.metrics.decrement_inflight();
        }
    }
}

struct SseClientGuard {
    metrics: Arc<HostMetrics>,
    active: bool,
}

impl Drop for SseClientGuard {
    fn drop(&mut self) {
        if self.active {
            self.metrics.sse_client_disconnected();
            self.active = false;
        }
    }
}

/// Bundle exposing router/service helpers for hosting a Flow IR over HTTP.
pub struct HostHandle {
    router: Router<()>,
}

impl HostHandle {
    /// Build a new host handle with the supplied executor and validated Flow IR.
    ///
    /// Panics if the entrypoint trigger/capture aliases are invalid.
    pub fn new(executor: FlowExecutor, ir: Arc<ValidatedIR>, config: RouteConfig) -> Self {
        Self::try_new(executor, ir, config).expect("invalid trigger/capture alias")
    }

    /// Fallible constructor that validates trigger/capture wiring before serving.
    pub fn try_new(
        executor: FlowExecutor,
        ir: Arc<ValidatedIR>,
        config: RouteConfig,
    ) -> Result<Self, ExecutionError> {
        let router = try_build_router(executor, ir, config)?;
        Ok(Self { router })
    }

    /// Obtain a clone of the underlying router for further composition.
    pub fn router(&self) -> Router<()> {
        self.router.clone()
    }

    /// Convert the router into the make-service used by `axum::serve`.
    pub fn into_service(self) -> Shared<axum::routing::RouterIntoService<Body, ()>> {
        Shared::new(self.router.into_service::<Body>())
    }

    /// Spawn the host on the provided listener, returning the background task handle.
    pub fn spawn(self, listener: TcpListener) -> JoinHandle<Result<(), std::io::Error>> {
        let service = self.into_service();
        tokio::spawn(async move { axum::serve(listener, service).await })
    }
}

#[derive(Clone)]
pub struct SharedState {
    runtime: HostRuntime,
    trigger_alias: String,
    capture_alias: String,
    deadline: Option<Duration>,
    metrics: Arc<HostMetrics>,
}

/// Build an Axum router serving the supplied Flow IR using the given executor.
pub fn router(executor: FlowExecutor, ir: Arc<ValidatedIR>, config: RouteConfig) -> Router<()> {
    HostHandle::new(executor, ir, config).router()
}

/// Convenience helper returning the make-service expected by `axum::serve`.
pub fn into_service(
    executor: FlowExecutor,
    ir: Arc<ValidatedIR>,
    config: RouteConfig,
) -> Shared<axum::routing::RouterIntoService<Body, ()>> {
    HostHandle::new(executor, ir, config).into_service()
}

fn validate_entrypoint(
    ir: &ValidatedIR,
    trigger_alias: &str,
    capture_alias: &str,
) -> Result<(), ExecutionError> {
    let trigger = ir
        .flow()
        .node(trigger_alias)
        .ok_or_else(|| ExecutionError::UnknownTrigger {
            alias: trigger_alias.to_string(),
        })?;

    if trigger.kind != NodeKind::Trigger {
        return Err(ExecutionError::UnknownTrigger {
            alias: trigger_alias.to_string(),
        });
    }

    ir.flow()
        .node(capture_alias)
        .ok_or_else(|| ExecutionError::UnknownCapture {
            alias: capture_alias.to_string(),
        })?;

    Ok(())
}

fn try_build_router(
    executor: FlowExecutor,
    ir: Arc<ValidatedIR>,
    config: RouteConfig,
) -> Result<Router<()>, ExecutionError> {
    let RouteConfig {
        path,
        method,
        trigger_alias,
        capture_alias,
        deadline,
        resources,
        environment_plugins,
    } = config;

    validate_entrypoint(&ir, &trigger_alias, &capture_alias)?;

    let runtime = if environment_plugins.is_empty() {
        HostRuntime::new(executor, Arc::clone(&ir))
    } else {
        HostRuntime::with_plugins(executor, Arc::clone(&ir), environment_plugins.clone())
    }
    .with_resource_bag(resources);

    let flow_name = ir.flow().name.clone();
    let metrics = Arc::new(HostMetrics::new("web_axum", path.clone(), flow_name));
    let state = SharedState {
        runtime,
        trigger_alias,
        capture_alias,
        deadline,
        metrics: metrics.clone(),
    };

    let route = method_router(&method);
    Ok(Router::<SharedState>::new()
        .route(&path, route)
        .with_state::<()>(state))
}

fn method_router(method: &Method) -> MethodRouter<SharedState> {
    match *method {
        Method::GET => get(dispatch_request),
        Method::POST => post(dispatch_request),
        Method::PUT => put(dispatch_request),
        Method::PATCH => patch(dispatch_request),
        Method::DELETE => delete(dispatch_request),
        _ => post(dispatch_request),
    }
}

struct HandlerResult {
    response: Response,
    success: bool,
    deadline_exceeded: bool,
}

impl HandlerResult {
    fn success(response: Response) -> Self {
        Self {
            response,
            success: true,
            deadline_exceeded: false,
        }
    }

    fn error(response: Response, deadline_exceeded: bool) -> Self {
        Self {
            response,
            success: false,
            deadline_exceeded,
        }
    }
}

#[instrument(name = "host_web_axum.dispatch", skip_all, fields(path = %request.uri().path(), method = %request.method()))]
async fn dispatch_request(State(state): State<SharedState>, request: Request<Body>) -> Response {
    let log_start = Instant::now();
    let metrics = state.metrics.clone();
    let guard = metrics.start_request();
    let result = handle_request(state, request).await;
    let HandlerResult {
        response,
        success,
        deadline_exceeded,
    } = result;
    let status = response.status();
    if success {
        info!(elapsed_ms = log_start.elapsed().as_millis(), status = %status, "request completed");
    } else {
        warn!(elapsed_ms = log_start.elapsed().as_millis(), status = %status, "request failed");
    }
    guard.finish(status, deadline_exceeded);
    response
}

async fn handle_request(state: SharedState, request: Request<Body>) -> HandlerResult {
    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(err) => return HandlerResult::error(internal_error("body_read", err), false),
    };
    let payload: JsonValue = if bytes.is_empty() {
        JsonValue::Null
    } else {
        match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(err) => return HandlerResult::error(bad_request(err.to_string()), false),
        }
    };

    let mut invocation = Invocation::new(
        state.trigger_alias.clone(),
        state.capture_alias.clone(),
        payload,
    )
    .with_deadline(state.deadline);
    let wants_sse = parts
        .headers
        .get(axum::http::header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.contains("text/event-stream"))
        .unwrap_or(false);

    populate_http_metadata(&parts, invocation.metadata_mut());

    let exec_result = state.runtime.execute(invocation).await;

    match exec_result {
        Ok(ExecutionResult::Value(value)) => {
            let response = Response::builder()
                .status(StatusCode::OK)
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&value).unwrap()))
                .unwrap();
            HandlerResult::success(response)
        }
        Ok(ExecutionResult::Stream(stream)) => {
            let response = streaming_response(stream, state.metrics.clone());
            HandlerResult::success(response)
        }
        Err(err) => {
            let deadline = matches!(err, ExecutionError::DeadlineExceeded { .. });

            if wants_sse {
                let (_, body) = map_execution_error(err);
                let response = sse_error_response(body, state.metrics.clone());
                return HandlerResult::error(response, deadline);
            }

            let (status, body) = map_execution_error(err);
            let response = Response::builder()
                .status(status)
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap();
            if response.status().is_server_error() {
                error!(status = %response.status(), "request failed");
            } else if response.status().is_client_error() {
                warn!(status = %response.status(), "request returned client error");
            }
            HandlerResult::error(response, deadline)
        }
    }
}

fn populate_http_metadata(parts: &axum::http::request::Parts, metadata: &mut InvocationMetadata) {
    metadata.insert_label("http.method", parts.method.as_str());
    metadata.insert_label("http.path", parts.uri.path().to_string());
    metadata.insert_label("http.version", format!("{:?}", parts.version));

    if let Some(query) = parts.uri.query() {
        metadata.insert_label("http.query_raw", query.to_string());
        if let Ok(pairs) = serde_urlencoded::from_str::<Vec<(String, String)>>(query) {
            let mut query_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for (key, value) in pairs {
                query_map.entry(key).or_default().push(value);
            }
            if !query_map.is_empty() {
                metadata.insert_extension("http.query", &query_map);
            }
        }
    }

    let mut header_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in parts.headers.iter() {
        let entry = header_map.entry(name.as_str().to_string()).or_default();
        let as_str = value
            .to_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|_| String::from_utf8_lossy(value.as_bytes()).into_owned());
        entry.push(as_str);
    }
    if !header_map.is_empty() {
        metadata.insert_extension("http.headers", &header_map);
    }

    if let Some(host) = parts.headers.get(axum::http::header::HOST)
        && let Ok(host_str) = host.to_str()
    {
        metadata.insert_label("http.host", host_str.to_string());
    }

    if let Some(user_header) = parts.headers.get("x-auth-user")
        && let Ok(raw) = user_header.to_str()
        && let Ok(value) = serde_json::from_str::<JsonValue>(raw)
    {
        metadata.insert_extension("auth.user", value);
    }
}

fn sse_error_response(payload: JsonValue, metrics: Arc<HostMetrics>) -> Response {
    let guard = metrics.track_sse_client();

    let guarded = stream! {
        yield Ok::<Event, Infallible>(Event::default().event("error").data(payload.to_string()));
        drop(guard);
    };

    let keep_alive = KeepAlive::new()
        .interval(Duration::from_secs(15))
        .text("keepalive");

    Sse::new(guarded).keep_alive(keep_alive).into_response()
}

fn streaming_response(stream: StreamHandle, metrics: Arc<HostMetrics>) -> Response {
    let guard = metrics.track_sse_client();
    let events = stream.map(|item| match item {
        Ok(payload) => match serde_json::to_string(&payload) {
            Ok(data) => Ok::<Event, Infallible>(Event::default().data(data)),
            Err(err) => {
                error!("failed to serialise SSE payload: {err}");
                Ok::<Event, Infallible>(
                    Event::default()
                        .event("error")
                        .data(json!({ "error": "serialization_failure" }).to_string()),
                )
            }
        },
        Err(err) => {
            warn!("streaming node terminated with error: {err}");
            Ok::<Event, Infallible>(
                Event::default()
                    .event("error")
                    .data(json!({ "error": err.to_string() }).to_string()),
            )
        }
    });

    let guarded = stream! {
        let mut events = events;
        while let Some(item) = events.next().await {
            yield item;
        }
        drop(guard);
    };

    let keep_alive = KeepAlive::new()
        .interval(Duration::from_secs(15))
        .text("keepalive");

    Sse::new(guarded).keep_alive(keep_alive).into_response()
}

fn map_execution_error(err: ExecutionError) -> (StatusCode, JsonValue) {
    match err {
        ExecutionError::DeadlineExceeded { .. } => (
            StatusCode::GATEWAY_TIMEOUT,
            json!({ "error": "deadline exceeded" }),
        ),
        ExecutionError::NodeFailed { alias, source } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": format!("node `{alias}` failed: {source}") }),
        ),
        ExecutionError::MissingOutput { alias } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": format!("capture `{alias}` produced no output") }),
        ),
        ExecutionError::UnknownTrigger { alias } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": format!("unknown trigger alias `{alias}`") }),
        ),
        ExecutionError::UnknownCapture { alias } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": format!("unknown capture alias `{alias}`") }),
        ),
        ExecutionError::UnregisteredNode { identifier } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": format!("no handler registered for node `{identifier}`") }),
        ),
        ExecutionError::MissingCapabilities { hints } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "error": "missing required capabilities",
                "code": "CAP101",
                "details": { "hints": hints }
            }),
        ),
        ExecutionError::Cancelled => (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "error": "execution cancelled" }),
        ),
        ExecutionError::UnsupportedControlSurface { id, kind } => (
            StatusCode::INTERNAL_SERVER_ERROR,
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
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": format!("invalid control surface `{id}` ({kind}): {reason}"),
                    "code": code,
                    "details": { "id": id, "kind": kind }
                }),
            )
        }
        ExecutionError::UnsupportedSpill { message } => (
            StatusCode::BAD_REQUEST,
            json!({ "error": "unsupported_spill", "message": message }),
        ),
        ExecutionError::SpillSetup(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": format!("failed to configure spill storage: {err}") }),
        ),
    }
}

fn bad_request(message: String) -> Response {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "error": message }).to_string()))
        .unwrap()
}

fn internal_error(label: &str, err: impl std::fmt::Display) -> Response {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "error": format!("{label} failed: {err}") }).to_string(),
        ))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::http::Request;
    use dag_core::NodeError;
    use dag_core::prelude::*;
    use futures::stream;
    use kernel_exec::NodeRegistry;
    use kernel_plan::validate;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
    use proptest::prelude::*;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex, OnceLock};
    use tokio::runtime::Builder as RuntimeBuilder;

    fn build_flow() -> (FlowExecutor, Arc<ValidatedIR>) {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn("tests::sink", |value: JsonValue| async move { Ok(value) })
            .unwrap();

        let executor = FlowExecutor::new(Arc::new(registry));

        let mut builder = FlowBuilder::new("web_host", Version::new(1, 0, 0), Profile::Web);
        let trigger = builder
            .add_node(
                "trigger",
                &NodeSpec::inline(
                    "tests::trigger",
                    "Trigger",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let sink = builder
            .add_node(
                "respond",
                &NodeSpec::inline(
                    "tests::sink",
                    "Respond",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &sink);
        let flow = builder.build();
        let validated = validate(&flow).expect("flow should validate");
        (executor, Arc::new(validated))
    }

    fn build_streaming_flow() -> (FlowExecutor, Arc<ValidatedIR>) {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_stream_fn("tests::stream", |_value: JsonValue| async move {
                Ok(stream::iter(vec![
                    Ok(JsonValue::from(1)),
                    Ok(JsonValue::from(2)),
                ]))
            })
            .unwrap();

        let executor = FlowExecutor::new(Arc::new(registry));

        let mut builder = FlowBuilder::new("web_host_stream", Version::new(1, 0, 0), Profile::Web);
        let trigger = builder
            .add_node(
                "trigger",
                &NodeSpec::inline(
                    "tests::trigger",
                    "Trigger",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let stream_capture = builder
            .add_node(
                "stream",
                &NodeSpec::inline(
                    "tests::stream",
                    "StreamCapture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::ReadOnly,
                    Determinism::BestEffort,
                    Some("Emits incremental updates"),
                ),
            )
            .unwrap();
        builder.connect(&trigger, &stream_capture);

        let flow = builder.build();
        let validated = validate(&flow).expect("flow should validate");
        (executor, Arc::new(validated))
    }

    fn build_flow_with_unsupported_control_surface() -> (FlowExecutor, Arc<ValidatedIR>) {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn("tests::sink", |value: JsonValue| async move { Ok(value) })
            .unwrap();

        let executor = FlowExecutor::new(Arc::new(registry));

        let mut builder = FlowBuilder::new("web_host", Version::new(1, 0, 0), Profile::Web);
        let trigger = builder
            .add_node(
                "trigger",
                &NodeSpec::inline(
                    "tests::trigger",
                    "Trigger",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let sink = builder
            .add_node(
                "respond",
                &NodeSpec::inline(
                    "tests::sink",
                    "Respond",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &sink);

        let mut flow = builder.build();
        flow.control_surfaces.push(dag_core::ControlSurfaceIR {
            id: "rate_limit:0".to_string(),
            kind: dag_core::ControlSurfaceKind::RateLimit,
            targets: vec![],
            config: json!({"v": 1, "target": "trigger", "qps": 1, "burst": 1}),
        });

        let validated = validate(&flow).expect("flow should validate");
        (executor, Arc::new(validated))
    }

    fn make_state(
        executor: FlowExecutor,
        ir: Arc<ValidatedIR>,
        config: RouteConfig,
    ) -> SharedState {
        let RouteConfig {
            path,
            method: _,
            trigger_alias,
            capture_alias,
            deadline,
            resources,
            environment_plugins,
        } = config;

        let runtime = if environment_plugins.is_empty() {
            HostRuntime::new(executor, Arc::clone(&ir))
        } else {
            HostRuntime::with_plugins(executor, Arc::clone(&ir), environment_plugins)
        }
        .with_resource_bag(resources);

        let flow_name = ir.flow().name.clone();
        let metrics = Arc::new(HostMetrics::new("web_axum", path.clone(), flow_name));
        SharedState {
            runtime,
            trigger_alias,
            capture_alias,
            deadline,
            metrics,
        }
    }

    fn metrics_snapshotter() -> &'static Snapshotter {
        static SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();
        SNAPSHOTTER.get_or_init(|| {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            metrics::set_global_recorder(recorder)
                .unwrap_or_else(|_| panic!("metrics recorder already installed"));
            snapshotter
        })
    }

    fn reset_metrics() {
        let _ = metrics_snapshotter().snapshot();
    }

    #[test]
    fn sse_stream_preserves_event_sequence() {
        let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
            cases: 32,
            ..ProptestConfig::default()
        });
        let strategy = proptest::collection::vec(proptest::num::i32::ANY, 1..=6);

        runner
            .run(&strategy, |values| {
                let runtime = RuntimeBuilder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("tokio runtime");

                runtime.block_on(async move {
                    let json_events: Vec<JsonValue> =
                        values.into_iter().map(JsonValue::from).collect();

                    let mut registry = NodeRegistry::new();
                    registry
                        .register_fn(
                            "tests::trigger",
                            |value: JsonValue| async move { Ok(value) },
                        )
                        .unwrap();

                    let stream_events = Arc::new(json_events.clone());
                    registry
                        .register_stream_fn("tests::stream_prop", move |_value: JsonValue| {
                            let events = Arc::clone(&stream_events);
                            async move {
                                let items: Vec<JsonValue> = events.iter().cloned().collect();
                                Ok(stream::iter(items.into_iter().map(Ok)))
                            }
                        })
                        .unwrap();

                    let executor = FlowExecutor::new(Arc::new(registry));

                    let mut builder =
                        FlowBuilder::new("prop_stream", Version::new(1, 0, 0), Profile::Web);
                    let trigger = builder
                        .add_node(
                            "trigger",
                            &NodeSpec::inline(
                                "tests::trigger",
                                "Trigger",
                                SchemaSpec::Opaque,
                                SchemaSpec::Opaque,
                                Effects::Pure,
                                Determinism::Strict,
                                None,
                            ),
                        )
                        .unwrap();
                    let capture = builder
                        .add_node(
                            "stream",
                            &NodeSpec::inline(
                                "tests::stream_prop",
                                "StreamCapture",
                                SchemaSpec::Opaque,
                                SchemaSpec::Opaque,
                                Effects::ReadOnly,
                                Determinism::Stable,
                                Some("Property-based stream capture"),
                            ),
                        )
                        .unwrap();
                    builder.connect(&trigger, &capture);
                    let flow = builder.build();
                    let validated = validate(&flow).expect("flow should validate");

                    let config = RouteConfig::new("/prop_stream")
                        .with_method(Method::GET)
                        .with_trigger_alias("trigger")
                        .with_capture_alias("stream")
                        .with_deadline(Duration::from_millis(250));
                    let state = make_state(executor, Arc::new(validated), config);

                    let request = Request::builder()
                        .method(Method::GET)
                        .uri("/prop_stream")
                        .body(Body::empty())
                        .unwrap();

                    let response = super::dispatch_request(State(state), request).await;
                    prop_assert_eq!(response.status(), StatusCode::OK);

                    let body = to_bytes(response.into_body(), usize::MAX)
                        .await
                        .expect("body bytes");
                    let text = String::from_utf8(body.to_vec()).expect("utf-8 body");

                    let actual: Vec<JsonValue> = text
                        .lines()
                        .filter_map(|line| {
                            if let Some(data) = line.strip_prefix("data: ") {
                                let trimmed = data.trim();
                                if trimmed.is_empty() || trimmed == "keepalive" {
                                    None
                                } else {
                                    Some(
                                        serde_json::from_str::<JsonValue>(trimmed)
                                            .expect("valid json payload"),
                                    )
                                }
                            } else {
                                None
                            }
                        })
                        .collect();

                    prop_assert_eq!(
                        actual,
                        json_events,
                        "SSE payloads should preserve order and content"
                    );

                    Ok(())
                })
            })
            .unwrap();
    }

    #[tokio::test]
    async fn records_host_metrics_for_success() {
        reset_metrics();
        let (executor, ir) = build_flow();
        let config = RouteConfig::new("/echo")
            .with_method(Method::POST)
            .with_trigger_alias("trigger")
            .with_capture_alias("respond")
            .with_deadline(Duration::from_millis(250));
        let state = make_state(executor, ir, config);

        let request = Request::builder()
            .method(Method::POST)
            .uri("/echo")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "value": "ping" }).to_string()))
            .unwrap();

        let response = super::dispatch_request(State(state), request).await;
        assert_eq!(response.status(), StatusCode::OK);

        let snapshot = metrics_snapshotter().snapshot().into_vec();
        let mut saw_latency = false;
        let mut saw_requests = false;
        for (key, _unit, _desc, value) in snapshot.into_iter() {
            let name = key.key().name();
            match (name, value) {
                ("lattice.host.http_request_latency_ms", DebugValue::Histogram(vals)) => {
                    assert!(!vals.is_empty(), "latency histogram should record samples");
                    saw_latency = true;
                }
                ("lattice.host.http_requests_total", DebugValue::Counter(count)) => {
                    assert!(count > 0, "request counter should increment");
                    saw_requests = true;
                }
                _ => {}
            }
        }

        assert!(saw_latency, "expected host latency histogram to be emitted");
        assert!(saw_requests, "expected host request counter to be emitted");
    }

    #[tokio::test]
    async fn executes_flow_and_returns_json() {
        let (executor, ir) = build_flow();
        let config = RouteConfig::new("/echo")
            .with_method(Method::POST)
            .with_trigger_alias("trigger")
            .with_capture_alias("respond")
            .with_deadline(Duration::from_millis(250));
        let state = make_state(executor, ir, config);

        let request = Request::builder()
            .method(Method::POST)
            .uri("/echo")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "value": "ping" }).to_string()))
            .unwrap();

        let response = super::dispatch_request(State(state), request).await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let payload: JsonValue = serde_json::from_slice(&body).expect("json");
        assert_eq!(payload, json!({ "value": "ping" }));
    }

    #[tokio::test]
    async fn unsupported_control_surface_maps_to_ctrl901_json() {
        let (executor, ir) = build_flow_with_unsupported_control_surface();
        let config = RouteConfig::new("/unsupported")
            .with_method(Method::POST)
            .with_trigger_alias("trigger")
            .with_capture_alias("respond")
            .with_deadline(Duration::from_millis(250));
        let state = make_state(executor, ir, config);

        let request = Request::builder()
            .method(Method::POST)
            .uri("/unsupported")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "value": "ping" }).to_string()))
            .unwrap();

        let response = super::dispatch_request(State(state), request).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let payload: JsonValue = serde_json::from_slice(&body).expect("json");
        assert_eq!(payload["code"], json!("CTRL901"));
        assert_eq!(payload["details"]["id"], json!("rate_limit:0"));
        assert_eq!(payload["details"]["kind"], json!("rate_limit"));
    }

    #[tokio::test]
    async fn unsupported_control_surface_maps_to_ctrl901_sse() {
        let (executor, ir) = build_flow_with_unsupported_control_surface();
        let config = RouteConfig::new("/unsupported_sse")
            .with_method(Method::GET)
            .with_trigger_alias("trigger")
            .with_capture_alias("respond")
            .with_deadline(Duration::from_millis(250));
        let state = make_state(executor, ir, config);

        let request = Request::builder()
            .method(Method::GET)
            .uri("/unsupported_sse")
            .header(axum::http::header::ACCEPT, "text/event-stream")
            .body(Body::empty())
            .unwrap();

        let response = super::dispatch_request(State(state), request).await;
        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .expect("content-type header present");
        assert_eq!(content_type, "text/event-stream");

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let text = String::from_utf8(body.to_vec()).expect("utf-8");

        let mut error_payload: Option<JsonValue> = None;
        let mut saw_error_event = false;
        for line in text.lines() {
            if line.trim() == "event: error" {
                saw_error_event = true;
                continue;
            }
            if saw_error_event && line.starts_with("data:") {
                let data = line.strip_prefix("data:").expect("data prefix").trim();
                error_payload = Some(serde_json::from_str(data).expect("json payload"));
                break;
            }
        }

        let payload = error_payload.expect("error payload");
        assert_eq!(payload["code"], json!("CTRL901"));
        assert_eq!(payload["details"]["id"], json!("rate_limit:0"));
        assert_eq!(payload["details"]["kind"], json!("rate_limit"));
    }

    #[tokio::test]
    async fn serves_sse_stream() {
        let (executor, ir) = build_streaming_flow();
        let config = RouteConfig::new("/stream")
            .with_method(Method::GET)
            .with_trigger_alias("trigger")
            .with_capture_alias("stream")
            .with_deadline(Duration::from_millis(250));
        let state = make_state(executor, ir, config);

        let request = Request::builder()
            .method(Method::GET)
            .uri("/stream")
            .body(Body::empty())
            .unwrap();

        let response = super::dispatch_request(State(state), request).await;

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .expect("content-type header present");
        assert_eq!(content_type, "text/event-stream");

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let text = String::from_utf8(body.to_vec()).expect("utf-8");
        assert!(text.contains("data: 1"));
        assert!(text.contains("data: 2"));
    }

    #[tokio::test]
    async fn forwards_request_metadata_to_plugins() {
        struct MetadataRecorder {
            captured: Arc<Mutex<Vec<InvocationMetadata>>>,
        }

        impl EnvironmentPlugin for MetadataRecorder {
            fn before_execute(&self, metadata: &InvocationMetadata) {
                self.captured.lock().unwrap().push(metadata.clone());
            }
        }

        let (executor, ir) = build_flow();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let plugin = Arc::new(MetadataRecorder {
            captured: captured.clone(),
        });
        let config = RouteConfig::new("/meta")
            .with_method(Method::POST)
            .with_trigger_alias("trigger")
            .with_capture_alias("respond")
            .with_environment_plugin(plugin);
        let state = make_state(executor, ir, config);

        let request = Request::builder()
            .method(Method::POST)
            .uri("/meta?foo=bar&foo=baz")
            .header("content-type", "application/json")
            .header("host", "localhost")
            .header(
                "x-auth-user",
                r#"{"sub":"user-123","email":"user@example.com"}"#,
            )
            .header("x-custom", "xyz")
            .body(Body::from(json!({ "value": "ping" }).to_string()))
            .unwrap();

        let response = super::dispatch_request(State(state), request).await;
        assert_eq!(response.status(), StatusCode::OK);

        let captured = captured.lock().unwrap();
        let metadata = captured.last().expect("metadata recorded");
        assert_eq!(
            metadata.labels().get("http.method"),
            Some(&"POST".to_string())
        );
        assert_eq!(
            metadata.labels().get("http.path"),
            Some(&"/meta".to_string())
        );
        assert_eq!(
            metadata.labels().get("http.host"),
            Some(&"localhost".to_string())
        );
        assert_eq!(
            metadata.labels().get("http.query_raw"),
            Some(&"foo=bar&foo=baz".to_string())
        );

        let headers: BTreeMap<String, Vec<String>> = serde_json::from_value(
            metadata
                .extensions()
                .get("http.headers")
                .expect("headers present")
                .clone(),
        )
        .expect("headers map");
        assert_eq!(headers.get("x-custom"), Some(&vec!["xyz".to_string()]));

        let query: BTreeMap<String, Vec<String>> = serde_json::from_value(
            metadata
                .extensions()
                .get("http.query")
                .expect("query map present")
                .clone(),
        )
        .expect("query map");
        assert_eq!(
            query.get("foo"),
            Some(&vec!["bar".to_string(), "baz".to_string()])
        );

        let auth_user = metadata
            .extensions()
            .get("auth.user")
            .expect("auth user present");
        assert_eq!(
            auth_user.get("sub").and_then(JsonValue::as_str),
            Some("user-123")
        );
        assert_eq!(
            auth_user.get("email").and_then(JsonValue::as_str),
            Some("user@example.com")
        );
    }

    #[tokio::test]
    async fn maps_node_failure_to_500() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn("tests::sink", |_value: JsonValue| async move {
                Err::<JsonValue, NodeError>(NodeError::new("boom"))
            })
            .unwrap();

        let executor = FlowExecutor::new(Arc::new(registry));
        let mut builder = FlowBuilder::new("failure", Version::new(1, 0, 0), Profile::Web);
        let trigger = builder
            .add_node(
                "trigger",
                &NodeSpec::inline(
                    "tests::trigger",
                    "Trigger",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let sink = builder
            .add_node(
                "respond",
                &NodeSpec::inline(
                    "tests::sink",
                    "Respond",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &sink);
        let flow = builder.build();
        let validated = validate(&flow).expect("validated");

        let config = RouteConfig::new("/fail")
            .with_method(Method::POST)
            .with_trigger_alias("trigger")
            .with_capture_alias("respond");
        let state = make_state(executor, Arc::new(validated), config);

        let request = Request::builder()
            .method(Method::POST)
            .uri("/fail")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "value": "ping" }).to_string()))
            .unwrap();

        let response = super::dispatch_request(State(state), request).await;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn preflight_missing_capability_returns_code() {
        const KV_EFFECT_HINTS: [&str; 1] = [capabilities::kv::HINT_KV_READ];

        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::kv_node",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();

        let executor = FlowExecutor::new(Arc::new(registry));
        let mut builder = FlowBuilder::new("preflight", Version::new(1, 0, 0), Profile::Web);
        let trigger = builder
            .add_node(
                "trigger",
                &NodeSpec::inline(
                    "tests::trigger",
                    "Trigger",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let sink = builder
            .add_node(
                "respond",
                &NodeSpec::inline_with_hints(
                    "tests::kv_node",
                    "KvNode",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::ReadOnly,
                    Determinism::BestEffort,
                    None,
                    &[],
                    &KV_EFFECT_HINTS,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &sink);
        let validated = validate(&builder.build()).expect("validated");

        let config = RouteConfig::new("/preflight")
            .with_method(Method::POST)
            .with_trigger_alias("trigger")
            .with_capture_alias("respond");
        let state = make_state(executor, Arc::new(validated), config);

        let request = Request::builder()
            .method(Method::POST)
            .uri("/preflight")
            .header("content-type", "application/json")
            .body(Body::from(json!({"ok": true}).to_string()))
            .unwrap();

        let response = super::dispatch_request(State(state), request).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        let body: JsonValue = serde_json::from_slice(&bytes).expect("parse json body");
        assert_eq!(body["code"], json!("CAP101"));
        let hints = body["details"]["hints"]
            .as_array()
            .expect("details.hints array");
        assert!(hints.contains(&json!(capabilities::kv::HINT_KV_READ)));
    }

    #[tokio::test]
    async fn preflight_multiple_missing_capabilities_returns_code_and_list() {
        const KV_EFFECT_HINTS: [&str; 1] = [capabilities::kv::HINT_KV_READ];
        const HTTP_WRITE_EFFECT_HINTS: [&str; 1] = [capabilities::http::HINT_HTTP_WRITE];

        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::kv_node",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::http_node",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();

        let executor = FlowExecutor::new(Arc::new(registry));
        let mut builder = FlowBuilder::new("preflight_multi", Version::new(1, 0, 0), Profile::Web);
        let trigger = builder
            .add_node(
                "trigger",
                &NodeSpec::inline(
                    "tests::trigger",
                    "Trigger",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let kv_node = builder
            .add_node(
                "kv",
                &NodeSpec::inline_with_hints(
                    "tests::kv_node",
                    "KvNode",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::ReadOnly,
                    Determinism::BestEffort,
                    None,
                    &[],
                    &KV_EFFECT_HINTS,
                ),
            )
            .unwrap();
        let http_node = builder
            .add_node(
                "respond",
                &NodeSpec::inline_with_hints(
                    "tests::http_node",
                    "HttpNode",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Effectful,
                    Determinism::BestEffort,
                    None,
                    &[],
                    &HTTP_WRITE_EFFECT_HINTS,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &kv_node);
        builder.connect(&kv_node, &http_node);

        let mut flow = builder.build();
        flow.nodes
            .iter_mut()
            .find(|node| node.alias == "respond")
            .expect("respond node")
            .idempotency
            .key = Some("idempotency".to_string());

        let validated = validate(&flow).expect("validated");

        let config = RouteConfig::new("/preflight_multi")
            .with_method(Method::POST)
            .with_trigger_alias("trigger")
            .with_capture_alias("respond");
        let state = make_state(executor, Arc::new(validated), config);

        let request = Request::builder()
            .method(Method::POST)
            .uri("/preflight_multi")
            .header("content-type", "application/json")
            .body(Body::from(json!({"ok": true}).to_string()))
            .unwrap();

        let response = super::dispatch_request(State(state), request).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        let body: JsonValue = serde_json::from_slice(&bytes).expect("parse json body");
        assert_eq!(body["code"], json!("CAP101"));
        let hints = body["details"]["hints"]
            .as_array()
            .expect("details.hints array");
        assert!(hints.contains(&json!(capabilities::kv::HINT_KV_READ)));
        assert!(hints.contains(&json!(capabilities::http::HINT_HTTP_WRITE)));
    }

    #[tokio::test]
    async fn sse_preflight_failure_emits_error_event_with_code() {
        const KV_EFFECT_HINTS: [&str; 1] = [capabilities::kv::HINT_KV_READ];

        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_stream_fn("tests::stream", |_value: JsonValue| async move {
                Ok(stream::iter(vec![Ok(JsonValue::from(1))]))
            })
            .unwrap();

        let executor = FlowExecutor::new(Arc::new(registry));

        let mut builder = FlowBuilder::new("preflight_stream", Version::new(1, 0, 0), Profile::Web);
        let trigger = builder
            .add_node(
                "trigger",
                &NodeSpec::inline(
                    "tests::trigger",
                    "Trigger",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let stream_capture = builder
            .add_node(
                "stream",
                &NodeSpec::inline_with_hints(
                    "tests::stream",
                    "StreamCapture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::ReadOnly,
                    Determinism::BestEffort,
                    Some("Emits incremental updates"),
                    &[],
                    &KV_EFFECT_HINTS,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &stream_capture);
        let flow = builder.build();
        let validated = validate(&flow).expect("validated");

        let config = RouteConfig::new("/preflight_stream")
            .with_method(Method::GET)
            .with_trigger_alias("trigger")
            .with_capture_alias("stream");
        let state = make_state(executor, Arc::new(validated), config);

        let request = Request::builder()
            .method(Method::GET)
            .uri("/preflight_stream")
            .header(axum::http::header::ACCEPT, "text/event-stream")
            .body(Body::empty())
            .unwrap();

        let response = super::dispatch_request(State(state), request).await;
        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .expect("content-type header present");
        assert_eq!(content_type, "text/event-stream");

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let text = String::from_utf8(body.to_vec()).expect("utf-8");

        let mut error_payload: Option<JsonValue> = None;
        let mut saw_error_event = false;
        for line in text.lines() {
            if line.trim() == "event: error" {
                saw_error_event = true;
                continue;
            }
            if saw_error_event && line.starts_with("data:") {
                let data = line.strip_prefix("data:").expect("data prefix").trim();
                error_payload = Some(serde_json::from_str(data).expect("json payload"));
                break;
            }
        }

        let payload = error_payload.expect("error payload");
        assert_eq!(payload["code"], json!("CAP101"));
        let hints = payload["details"]["hints"]
            .as_array()
            .expect("details.hints array");
        assert!(hints.contains(&json!(capabilities::kv::HINT_KV_READ)));
    }
}
