use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
use capabilities::workspace::WorkspaceFactory;
use dag_core::NodeKind;
use futures::StreamExt;
use host_inproc::{
    EnvironmentPlugin, HostRuntime, IngressAttachment, IngressAttachmentPolicy, Invocation,
    InvocationMetadata,
};
use kernel_exec::{ExecutionError, ExecutionResult, FlowExecutor, StreamHandle};
use kernel_plan::ValidatedIR;
use serde_json::{Value as JsonValue, json};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::task::JoinHandle;
use tower::make::Shared;
use tracing::{error, info, instrument, warn};

/// Default maximum JSON request body accepted by the native web host.
pub const DEFAULT_JSON_BODY_LIMIT_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MULTIPART_ADMISSION_LIMIT: usize = 4;
pub const DEFAULT_MULTIPART_REQUEST_LIMIT_BYTES: usize = 10 * 1024 * 1024;
pub const DEFAULT_TRANSFORM_ADMISSION_LIMIT: usize = 2;
pub const DEFAULT_TRANSFORM_INPUT_LIMIT_BYTES: usize = 8 * 1024 * 1024;
/// Bounded route/transform input envelope: `4 * 10 MiB + 2 * 8 MiB = 56 MiB`.
/// Allocator, Wasmtime, module, and runtime bookkeeping are outside this simple envelope.
pub const DEFAULT_MULTIPART_TRANSFORM_INPUT_ENVELOPE_BYTES: usize =
    DEFAULT_MULTIPART_ADMISSION_LIMIT * DEFAULT_MULTIPART_REQUEST_LIMIT_BYTES
        + DEFAULT_TRANSFORM_ADMISSION_LIMIT * DEFAULT_TRANSFORM_INPUT_LIMIT_BYTES;

/// Explicit opt-in contract for one bounded multipart upload route.
#[derive(Clone, Debug)]
pub struct MultipartIngressConfig {
    file_field: String,
    artifact_payload_field: String,
    filename_metadata_field: Option<String>,
    expected_content_type: String,
    required_magic: Vec<u8>,
    max_total_bytes: usize,
    max_file_bytes: usize,
    max_text_bytes: usize,
}

impl MultipartIngressConfig {
    /// Build the canonical single-PDF ingress policy.
    pub fn pdf(file_field: impl Into<String>, artifact_payload_field: impl Into<String>) -> Self {
        Self {
            file_field: file_field.into(),
            artifact_payload_field: artifact_payload_field.into(),
            filename_metadata_field: None,
            expected_content_type: "application/pdf".to_string(),
            required_magic: b"%PDF-".to_vec(),
            max_total_bytes: DEFAULT_MULTIPART_REQUEST_LIMIT_BYTES,
            max_file_bytes: DEFAULT_TRANSFORM_INPUT_LIMIT_BYTES,
            max_text_bytes: 32 * 1024,
        }
    }

    pub fn with_filename_metadata_field(mut self, field: impl Into<String>) -> Self {
        self.filename_metadata_field = Some(field.into());
        self
    }

    /// Override limits for a deployment policy or focused tests.
    pub fn with_limits(
        mut self,
        max_total_bytes: usize,
        max_file_bytes: usize,
        max_text_bytes: usize,
    ) -> Self {
        self.max_total_bytes = max_total_bytes;
        self.max_file_bytes = max_file_bytes;
        self.max_text_bytes = max_text_bytes;
        self
    }

    fn validate(&self) -> Result<(), ExecutionError> {
        if self.file_field.is_empty()
            || self.artifact_payload_field.is_empty()
            || self.expected_content_type.is_empty()
            || self.required_magic.is_empty()
        {
            return Err(ExecutionError::HostEnvironment(anyhow::anyhow!(
                "multipart ingress fields, MIME type, and magic must be non-empty"
            )));
        }
        if self.filename_metadata_field.as_deref() == Some(self.artifact_payload_field.as_str()) {
            return Err(ExecutionError::HostEnvironment(anyhow::anyhow!(
                "multipart artifact and filename payload fields must be distinct"
            )));
        }
        if self.max_total_bytes == 0
            || self.max_file_bytes == 0
            || self.max_text_bytes == 0
            || self.max_file_bytes > self.max_total_bytes
        {
            return Err(ExecutionError::HostEnvironment(anyhow::anyhow!(
                "multipart ingress limits are invalid"
            )));
        }
        Ok(())
    }
}

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
    pub route_aliases: Vec<String>,
    pub workspace_factory: Option<Arc<dyn WorkspaceFactory>>,
    pub multipart_ingress: Option<MultipartIngressConfig>,
    pub multipart_admission_limit: usize,
    pub json_body_limit_bytes: usize,
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
            route_aliases: Vec::new(),
            workspace_factory: None,
            multipart_ingress: None,
            multipart_admission_limit: DEFAULT_MULTIPART_ADMISSION_LIMIT,
            json_body_limit_bytes: DEFAULT_JSON_BODY_LIMIT_BYTES,
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

    pub fn with_route_aliases(mut self, aliases: Vec<String>) -> Self {
        self.route_aliases = aliases;
        self
    }

    pub fn with_workspace_factory(mut self, factory: Arc<dyn WorkspaceFactory>) -> Self {
        self.workspace_factory = Some(factory);
        self
    }

    pub fn with_multipart_ingress(mut self, config: MultipartIngressConfig) -> Self {
        self.multipart_ingress = Some(config);
        self
    }

    /// Lower the process-local no-queue multipart admission limit from its default of four.
    pub fn with_multipart_admission_limit(mut self, limit: usize) -> Self {
        self.multipart_admission_limit = limit;
        self
    }

    pub fn with_json_body_limit(mut self, max_bytes: usize) -> Self {
        self.json_body_limit_bytes = max_bytes;
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

    fn multipart_admission(&self, outcome: &'static str) {
        metrics::counter!(
            "lattice.host.multipart_admission_total",
            "host" => self.host_label(),
            "flow" => self.flow_label(),
            "route" => self.route_label(),
            "outcome" => outcome
        )
        .increment(1);
    }

    fn multipart_rejected(&self, class: &'static str) {
        metrics::counter!(
            "lattice.host.multipart_rejected_total",
            "host" => self.host_label(),
            "flow" => self.flow_label(),
            "route" => self.route_label(),
            "outcome" => class
        )
        .increment(1);
    }

    fn multipart_request_bytes(&self, bytes: usize) {
        metrics::histogram!(
            "lattice.host.multipart_request_bytes",
            "host" => self.host_label(),
            "flow" => self.flow_label(),
            "route" => self.route_label()
        )
        .record(bytes as f64);
    }

    fn multipart_file_bytes(&self, bytes: usize) {
        metrics::histogram!(
            "lattice.host.multipart_file_bytes",
            "host" => self.host_label(),
            "flow" => self.flow_label(),
            "route" => self.route_label()
        )
        .record(bytes as f64);
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
    multipart_ingress: Option<MultipartIngressConfig>,
    multipart_admission: Option<Arc<Semaphore>>,
    json_body_limit_bytes: usize,
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
        route_aliases,
        workspace_factory,
        multipart_ingress,
        multipart_admission_limit,
        json_body_limit_bytes,
    } = config;

    validate_entrypoint(&ir, &trigger_alias, &capture_alias)?;
    if json_body_limit_bytes == 0 {
        return Err(ExecutionError::HostEnvironment(anyhow::anyhow!(
            "JSON body limit must be non-zero"
        )));
    }
    if let Some(multipart) = multipart_ingress.as_ref() {
        multipart.validate()?;
        if multipart_admission_limit == 0
            || multipart_admission_limit > DEFAULT_MULTIPART_ADMISSION_LIMIT
        {
            return Err(ExecutionError::HostEnvironment(anyhow::anyhow!(
                "multipart admission limit must be in 1..={DEFAULT_MULTIPART_ADMISSION_LIMIT}"
            )));
        }
        if workspace_factory.is_none() {
            return Err(ExecutionError::HostEnvironment(anyhow::anyhow!(
                "multipart ingress requires a host-owned workspace factory"
            )));
        }
    }

    let mut runtime = if environment_plugins.is_empty() {
        HostRuntime::new(executor, Arc::clone(&ir))
    } else {
        HostRuntime::with_plugins(executor, Arc::clone(&ir), environment_plugins.clone())
    }
    .with_resource_bag(resources);
    if let Some(factory) = workspace_factory {
        runtime = runtime.with_workspace_factory(factory);
    }

    let canonical_path = normalize_route_path(&path);
    let flow_name = ir.flow().name.clone();
    let metrics = Arc::new(HostMetrics::new(
        "web_axum",
        canonical_path.clone(),
        flow_name,
    ));
    let multipart_admission = multipart_ingress
        .as_ref()
        .map(|_| Arc::new(Semaphore::new(multipart_admission_limit)));
    let state = SharedState {
        runtime,
        trigger_alias,
        capture_alias,
        deadline,
        metrics: metrics.clone(),
        multipart_ingress,
        multipart_admission,
        json_body_limit_bytes,
    };

    let alias_paths = normalize_route_aliases(route_aliases, &canonical_path);
    let mut router = Router::<SharedState>::new().route(&canonical_path, method_router(&method));
    for alias in alias_paths {
        router = router.route(&alias, method_router(&method));
    }
    Ok(router.with_state::<()>(state))
}

fn normalize_route_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}

fn normalize_route_aliases(route_aliases: Vec<String>, canonical: &str) -> Vec<String> {
    let mut normalized = Vec::new();
    let mut push_alias = |alias: String| {
        let trimmed = alias.trim();
        if trimmed.is_empty() {
            return;
        }
        let alias = normalize_route_path(trimmed);
        if alias == canonical {
            return;
        }
        if !normalized.iter().any(|existing| existing == &alias) {
            normalized.push(alias);
        }
    };
    for alias in route_aliases {
        push_alias(alias);
    }
    normalized
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

const MAX_MULTIPART_FIELDS: usize = 32;
const MAX_MULTIPART_FIELD_NAME_BYTES: usize = 128;
const MAX_MULTIPART_FILENAME_BYTES: usize = 1024;

enum RequestPayloadError {
    BadRequest(&'static str),
    PayloadTooLarge(&'static str),
    UnsupportedMediaType(&'static str),
}

impl RequestPayloadError {
    fn class(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "bad_request",
            Self::PayloadTooLarge(_) => "payload_too_large",
            Self::UnsupportedMediaType(_) => "unsupported_media_type",
        }
    }

    fn into_response(self) -> Response {
        let class = self.class();
        let (status, message) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::PayloadTooLarge(message) => (StatusCode::PAYLOAD_TOO_LARGE, message),
            Self::UnsupportedMediaType(message) => (StatusCode::UNSUPPORTED_MEDIA_TYPE, message),
        };
        Response::builder()
            .status(status)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({ "error": message, "class": class }).to_string(),
            ))
            .unwrap()
    }
}

fn declared_content_length(
    headers: &axum::http::HeaderMap,
) -> Result<Option<usize>, RequestPayloadError> {
    let mut values = headers.get_all(axum::http::header::CONTENT_LENGTH).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(RequestPayloadError::BadRequest(
            "multiple content-length headers are not allowed",
        ));
    }
    let value = value
        .to_str()
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or(RequestPayloadError::BadRequest(
            "content-length header is malformed",
        ))?;
    Ok(Some(value))
}

fn safe_upload_filename(filename: &str) -> bool {
    !filename.is_empty()
        && filename.len() <= MAX_MULTIPART_FILENAME_BYTES
        && !filename.chars().any(char::is_control)
}

fn accepts_event_stream(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get_all(axum::http::header::ACCEPT)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|range| {
            let mut parts = range.trim().split(';');
            if parts.next().map(str::trim) != Some("text/event-stream") {
                return false;
            }
            !parts.any(|parameter| {
                let Some((name, value)) = parameter.trim().split_once('=') else {
                    return false;
                };
                name.eq_ignore_ascii_case("q") && value.trim().parse::<f32>().unwrap_or(0.0) <= 0.0
            })
        })
}

async fn parse_multipart_ingress(
    headers: &axum::http::HeaderMap,
    body: Body,
    config: &MultipartIngressConfig,
    metrics: &HostMetrics,
) -> Result<(JsonValue, IngressAttachment), RequestPayloadError> {
    if declared_content_length(headers)?.is_some_and(|length| length > config.max_total_bytes) {
        return Err(RequestPayloadError::PayloadTooLarge(
            "multipart request exceeds limit",
        ));
    }
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .ok_or(RequestPayloadError::BadRequest(
            "multipart content-type boundary is missing",
        ))?;
    let boundary = multer::parse_boundary(content_type).map_err(|_| {
        RequestPayloadError::BadRequest("multipart content-type boundary is malformed")
    })?;
    let total_bytes = Arc::new(AtomicUsize::new(0));
    let exceeded = Arc::new(AtomicBool::new(false));
    let stream_total = Arc::clone(&total_bytes);
    let stream_exceeded = Arc::clone(&exceeded);
    let max_total_bytes = config.max_total_bytes;
    let input = body.into_data_stream().map(move |result| {
        let bytes = result.map_err(|_| std::io::Error::other("request body read failed"))?;
        let previous = stream_total.fetch_add(bytes.len(), Ordering::Relaxed);
        if previous.saturating_add(bytes.len()) > max_total_bytes {
            stream_exceeded.store(true, Ordering::Relaxed);
            return Err(std::io::Error::other("multipart request exceeds limit"));
        }
        Ok(bytes)
    });
    let mut multipart = multer::Multipart::new(input, boundary);
    let mut payload = serde_json::Map::new();
    let mut attachment = None;
    let mut text_bytes = 0usize;
    let mut field_count = 0usize;

    while let Some(field) = multipart.next_field().await.map_err(|_| {
        if exceeded.load(Ordering::Relaxed) {
            RequestPayloadError::PayloadTooLarge("multipart request exceeds limit")
        } else {
            RequestPayloadError::BadRequest("malformed multipart body")
        }
    })? {
        field_count += 1;
        if field_count > MAX_MULTIPART_FIELDS {
            return Err(RequestPayloadError::PayloadTooLarge(
                "multipart field count exceeds limit",
            ));
        }
        let name = field
            .name()
            .map(str::to_string)
            .ok_or(RequestPayloadError::BadRequest(
                "multipart field name is missing",
            ))?;
        if name.is_empty() || name.len() > MAX_MULTIPART_FIELD_NAME_BYTES {
            return Err(RequestPayloadError::PayloadTooLarge(
                "multipart field name exceeds limit",
            ));
        }
        let filename = field.file_name().map(str::to_string);
        let field_content_type = field.content_type().map(ToString::to_string);

        if name == config.file_field {
            let filename = filename.ok_or(RequestPayloadError::BadRequest(
                "multipart file field requires a filename",
            ))?;
            if !safe_upload_filename(&filename) {
                return Err(RequestPayloadError::BadRequest(
                    "multipart filename is unsafe",
                ));
            }
            if attachment.is_some() {
                return Err(RequestPayloadError::BadRequest(
                    "multipart file field is duplicated",
                ));
            }
            if field_content_type.as_deref() != Some(config.expected_content_type.as_str()) {
                return Err(RequestPayloadError::UnsupportedMediaType(
                    "multipart file has an unsupported media type",
                ));
            }
            let bytes = field.bytes().await.map_err(|_| {
                if exceeded.load(Ordering::Relaxed) {
                    RequestPayloadError::PayloadTooLarge("multipart request exceeds limit")
                } else {
                    RequestPayloadError::BadRequest("malformed multipart file field")
                }
            })?;
            if bytes.len() > config.max_file_bytes {
                return Err(RequestPayloadError::PayloadTooLarge(
                    "multipart file exceeds limit",
                ));
            }
            metrics.multipart_file_bytes(bytes.len());
            if !bytes.starts_with(&config.required_magic) {
                return Err(RequestPayloadError::UnsupportedMediaType(
                    "multipart file does not match required magic",
                ));
            }
            if let Some(metadata_field) = config.filename_metadata_field.as_ref() {
                if payload.contains_key(metadata_field) {
                    return Err(RequestPayloadError::BadRequest(
                        "multipart filename metadata field is duplicated",
                    ));
                }
                payload.insert(metadata_field.clone(), JsonValue::String(filename));
            }
            attachment = Some(IngressAttachment::new(
                config.artifact_payload_field.clone(),
                config.expected_content_type.clone(),
                bytes,
            ));
            continue;
        }

        if filename.is_some()
            || field_content_type
                .as_deref()
                .is_some_and(|content_type| content_type != "text/plain")
        {
            return Err(RequestPayloadError::BadRequest(
                "multipart contains an unknown file field",
            ));
        }
        if name == config.artifact_payload_field
            || config.filename_metadata_field.as_deref() == Some(name.as_str())
            || payload.contains_key(&name)
        {
            return Err(RequestPayloadError::BadRequest(
                "multipart text field is duplicated or reserved",
            ));
        }
        let bytes = field.bytes().await.map_err(|_| {
            if exceeded.load(Ordering::Relaxed) {
                RequestPayloadError::PayloadTooLarge("multipart request exceeds limit")
            } else {
                RequestPayloadError::BadRequest("malformed multipart text field")
            }
        })?;
        text_bytes =
            text_bytes
                .checked_add(bytes.len())
                .ok_or(RequestPayloadError::PayloadTooLarge(
                    "multipart text fields exceed limit",
                ))?;
        if text_bytes > config.max_text_bytes {
            return Err(RequestPayloadError::PayloadTooLarge(
                "multipart text fields exceed limit",
            ));
        }
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| RequestPayloadError::BadRequest("multipart text field is not UTF-8"))?;
        payload.insert(name, JsonValue::String(text.to_string()));
    }

    let attachment = attachment.ok_or(RequestPayloadError::BadRequest(
        "multipart required file field is missing",
    ))?;
    metrics.multipart_request_bytes(total_bytes.load(Ordering::Relaxed));
    Ok((JsonValue::Object(payload), attachment))
}

async fn handle_request(state: SharedState, request: Request<Body>) -> HandlerResult {
    // Route admission is deliberately acquired before content-length inspection,
    // content-type parsing, or any body poll. `try_acquire_owned` has no waiter queue.
    let mut multipart_permit: Option<OwnedSemaphorePermit> =
        if let Some(admission) = state.multipart_admission.as_ref() {
            match Arc::clone(admission).try_acquire_owned() {
                Ok(permit) => {
                    state.metrics.multipart_admission("accepted");
                    Some(permit)
                }
                Err(TryAcquireError::NoPermits) => {
                    state.metrics.multipart_admission("saturated");
                    let response = Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .header(axum::http::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            json!({
                                "error": "multipart route saturated",
                                "class": "busy"
                            })
                            .to_string(),
                        ))
                        .unwrap();
                    return HandlerResult::error(response, false);
                }
                Err(TryAcquireError::Closed) => {
                    state.metrics.multipart_admission("runtime_unavailable");
                    let response = Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .header(axum::http::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            json!({
                                "error": "multipart admission unavailable",
                                "class": "runtime_unavailable"
                            })
                            .to_string(),
                        ))
                        .unwrap();
                    return HandlerResult::error(response, false);
                }
            }
        } else {
            None
        };
    let (parts, body) = request.into_parts();
    let wants_sse = accepts_event_stream(&parts.headers);

    let (payload, attachment) = if let Some(config) = state.multipart_ingress.as_ref() {
        if wants_sse {
            state.metrics.multipart_rejected("not_acceptable");
            let response = Response::builder()
                .status(StatusCode::NOT_ACCEPTABLE)
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "error": "multipart ingress does not support SSE",
                        "class": "not_acceptable"
                    })
                    .to_string(),
                ))
                .unwrap();
            return HandlerResult::error(response, false);
        }
        match parse_multipart_ingress(&parts.headers, body, config, state.metrics.as_ref()).await {
            Ok((payload, attachment)) => (payload, Some(attachment)),
            Err(err) => {
                state.metrics.multipart_rejected(err.class());
                return HandlerResult::error(err.into_response(), false);
            }
        }
    } else {
        match declared_content_length(&parts.headers) {
            Ok(Some(length)) if length > state.json_body_limit_bytes => {
                return HandlerResult::error(
                    RequestPayloadError::PayloadTooLarge("JSON request exceeds limit")
                        .into_response(),
                    false,
                );
            }
            Ok(_) => {}
            Err(err) => return HandlerResult::error(err.into_response(), false),
        }
        let bytes = match to_bytes(body, state.json_body_limit_bytes).await {
            Ok(bytes) => bytes,
            Err(_) => {
                return HandlerResult::error(
                    RequestPayloadError::PayloadTooLarge("JSON request exceeds limit")
                        .into_response(),
                    false,
                );
            }
        };
        let payload = if bytes.is_empty() {
            JsonValue::Null
        } else {
            match serde_json::from_slice(&bytes) {
                Ok(value) => value,
                Err(err) => return HandlerResult::error(bad_request(err.to_string()), false),
            }
        };
        (payload, None)
    };

    let mut invocation = Invocation::new(
        state.trigger_alias.clone(),
        state.capture_alias.clone(),
        payload,
    )
    .with_deadline(state.deadline);
    populate_http_metadata(&parts, invocation.metadata_mut());

    let exec_result = if let Some(attachment) = attachment {
        let permit = multipart_permit.take();
        state
            .runtime
            .execute_with_ingress_attachments_after_staging(
                invocation,
                vec![attachment],
                IngressAttachmentPolicy {
                    max_attachments: 1,
                    max_attachment_bytes: state
                        .multipart_ingress
                        .as_ref()
                        .expect("multipart config present")
                        .max_file_bytes as u64,
                    max_total_bytes: state
                        .multipart_ingress
                        .as_ref()
                        .expect("multipart config present")
                        .max_total_bytes as u64,
                },
                move || drop(permit),
            )
            .await
    } else {
        drop(multipart_permit.take());
        state.runtime.execute(invocation).await
    };

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
        Ok(ExecutionResult::Halt { alias, payload }) => {
            let body = json!({
                "halted": true,
                "node": alias,
                "payload": payload,
            });
            let response = Response::builder()
                .status(StatusCode::ACCEPTED)
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap();
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

fn sanitized_classified_node_failure(message: &str) -> Option<(&str, &str)> {
    let (code, rest) = message.split_once(':')?;
    let bytes = code.as_bytes();
    if bytes.len() < 3
        || bytes.len() > 32
        || !bytes[0].is_ascii_uppercase()
        || !bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        || !bytes.contains(&b'-')
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || *byte == b'-')
    {
        return None;
    }
    let class = rest.strip_suffix(']')?.rsplit_once('[')?.1;
    if !matches!(
        class,
        "resource_context_unavailable"
            | "workspace_read_unavailable"
            | "length_exceeds_limit"
            | "actual_length_unrepresentable"
            | "length_mismatch"
            | "hash_mismatch"
            | "magic_mismatch"
            | "mime_mismatch"
            | "workspace_unsupported"
            | "not_found"
            | "invalid_path"
            | "workspace_backend"
            | "invalid_handle"
            | "invalid_transform"
            | "busy"
            | "runtime_unavailable"
            | "invalid_module"
            | "invalid_abi"
            | "input_too_large"
            | "fuel_exhausted"
            | "memory_exhausted"
            | "wall_time_exceeded"
            | "platform_terminated"
            | "cancelled"
            | "output_too_large"
            | "guest_failed"
            | "invalid_output"
            | "unsupported_document"
    ) {
        return None;
    }
    Some((code, class))
}

fn map_execution_error(err: ExecutionError) -> (StatusCode, JsonValue) {
    match err {
        ExecutionError::DeadlineExceeded { .. } => (
            StatusCode::GATEWAY_TIMEOUT,
            json!({ "error": "deadline exceeded" }),
        ),
        ExecutionError::NodeFailed { alias, source } => {
            let message = source.to_string();
            if let Some((code, class)) = sanitized_classified_node_failure(&message) {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json!({
                        "error": "node operation failed",
                        "node": alias,
                        "code": code,
                        "class": class,
                    }),
                )
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json!({ "error": format!("node `{alias}` failed: {message}") }),
                )
            }
        }
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
        ExecutionError::MissingDurabilityServices { missing } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "error": "missing required durability services",
                "code": "DAG-CKPT-003",
                "details": { "missing": missing }
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
        ExecutionError::CheckpointNotFound { checkpoint_id } => (
            StatusCode::NOT_FOUND,
            json!({
                "error": "checkpoint not found",
                "code": "DAG-CKPT-006",
                "details": { "checkpoint_id": checkpoint_id }
            }),
        ),
        ExecutionError::CheckpointLeaseConflict { checkpoint_id } => (
            StatusCode::CONFLICT,
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
            StatusCode::INTERNAL_SERVER_ERROR,
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
            StatusCode::INTERNAL_SERVER_ERROR,
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
            StatusCode::CONFLICT,
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
            StatusCode::BAD_REQUEST,
            json!({ "error": "unsupported_spill", "message": message }),
        ),
        ExecutionError::SpillSetup(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": format!("failed to configure spill storage: {err}") }),
        ),
        ExecutionError::HostEnvironment(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": format!("host environment error: {err}") }),
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
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
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, Mutex, OnceLock};
    use tower::ServiceExt;

    use capabilities::workspace::{
        Workspace, WorkspaceCompletionDisposition, WorkspaceDeleteResult, WorkspaceEntry,
        WorkspaceError, WorkspaceFactory, WorkspaceListOptions, WorkspaceReadResult,
        WorkspaceRunScope, WorkspaceWriteOptions, WorkspaceWriteResult,
    };
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
        let mut flow = builder.build();
        // Durability defaults to `Partial`, which requires a checkpoint store at
        // preflight. These fixtures exercise success/capability/control behaviour,
        // not durability, so disable it (mirrors host-inproc test fixtures).
        flow.policies.durability.mode = DurabilityMode::Off;
        flow.nodes
            .iter_mut()
            .find(|node| node.alias == "trigger")
            .unwrap()
            .kind = NodeKind::Trigger;
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

        let mut flow = builder.build();
        // See `build_flow`: disable default `Partial` durability for fixtures that
        // exercise streaming behaviour rather than durability preflight.
        flow.policies.durability.mode = DurabilityMode::Off;
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
        // Disable default `Partial` durability so preflight reaches the unsupported
        // control-surface check rather than failing on a missing checkpoint store.
        flow.policies.durability.mode = DurabilityMode::Off;
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
            route_aliases: _,
            workspace_factory,
            multipart_ingress,
            multipart_admission_limit,
            json_body_limit_bytes,
        } = config;

        let mut runtime = if environment_plugins.is_empty() {
            HostRuntime::new(executor, Arc::clone(&ir))
        } else {
            HostRuntime::with_plugins(executor, Arc::clone(&ir), environment_plugins)
        }
        .with_resource_bag(resources);
        if let Some(factory) = workspace_factory {
            runtime = runtime.with_workspace_factory(factory);
        }

        let flow_name = ir.flow().name.clone();
        let metrics = Arc::new(HostMetrics::new("web_axum", path.clone(), flow_name));
        let multipart_admission = multipart_ingress
            .as_ref()
            .map(|_| Arc::new(Semaphore::new(multipart_admission_limit)));
        SharedState {
            runtime,
            trigger_alias,
            capture_alias,
            deadline,
            metrics,
            multipart_ingress,
            multipart_admission,
            json_body_limit_bytes,
        }
    }

    #[derive(Default)]
    struct MultipartWorkspace {
        files: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl capabilities::Capability for MultipartWorkspace {
        fn name(&self) -> &'static str {
            "workspace.multipart-test"
        }
    }

    #[async_trait]
    impl Workspace for MultipartWorkspace {
        async fn read_normalized(
            &self,
            path: &str,
        ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
            Ok(self
                .files
                .lock()
                .unwrap()
                .get(path)
                .cloned()
                .map(WorkspaceReadResult::Bytes))
        }

        async fn write_normalized(
            &self,
            path: &str,
            data: &[u8],
            _options: WorkspaceWriteOptions,
        ) -> Result<WorkspaceWriteResult, WorkspaceError> {
            self.files
                .lock()
                .unwrap()
                .insert(path.to_string(), data.to_vec());
            Ok(WorkspaceWriteResult {
                path: path.to_string(),
                size_bytes: data.len() as u64,
                updated_at_ms: 0,
            })
        }

        async fn list_normalized(
            &self,
            _options: WorkspaceListOptions,
        ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
            Ok(Vec::new())
        }

        async fn delete_normalized(
            &self,
            path: &str,
        ) -> Result<WorkspaceDeleteResult, WorkspaceError> {
            Ok(WorkspaceDeleteResult {
                deleted: self.files.lock().unwrap().remove(path).is_some(),
            })
        }
    }

    #[derive(Default)]
    struct MultipartWorkspaceFactory {
        workspace: Arc<MultipartWorkspace>,
        opened: Mutex<Vec<WorkspaceRunScope>>,
        completed: Mutex<Vec<(WorkspaceRunScope, WorkspaceCompletionDisposition)>>,
    }

    #[async_trait]
    impl WorkspaceFactory for MultipartWorkspaceFactory {
        async fn open(&self, scope: WorkspaceRunScope) -> anyhow::Result<Arc<dyn Workspace>> {
            self.opened.lock().unwrap().push(scope);
            Ok(self.workspace.clone())
        }

        async fn complete(
            &self,
            scope: WorkspaceRunScope,
            disposition: WorkspaceCompletionDisposition,
        ) -> anyhow::Result<()> {
            self.workspace.files.lock().unwrap().clear();
            self.completed.lock().unwrap().push((scope, disposition));
            Ok(())
        }
    }

    fn build_multipart_flow() -> (FlowExecutor, Arc<ValidatedIR>) {
        build_multipart_flow_with_sink_gate(None)
    }

    fn build_multipart_flow_with_sink_gate(
        sink_gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    ) -> (FlowExecutor, Arc<ValidatedIR>) {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::multipart_trigger", |value: JsonValue| async move {
                Ok(value)
            })
            .unwrap();
        registry
            .register_fn("tests::multipart_sink", move |value: JsonValue| {
                let sink_gate = sink_gate.clone();
                async move {
                    if let Some((entered, release)) = sink_gate {
                        entered.notify_one();
                        release.notified().await;
                    }
                    let raw_payload = serde_json::to_string(&value).unwrap();
                    let artifact: capabilities::Artifact = serde_json::from_value(
                        value
                            .get("cv_artifact")
                            .cloned()
                            .ok_or_else(|| NodeError::new("missing cv artifact"))?,
                    )
                    .map_err(|err| NodeError::new(err.to_string()))?;
                    let path = artifact.handle.scope().path().to_string();
                    let bytes =
                        capabilities::context::with_current_async(move |resources| async move {
                            let reader = resources
                                .workspace_read()
                                .ok_or_else(|| NodeError::new("missing workspace read"))?;
                            reader
                                .read(&artifact.handle)
                                .await
                                .map_err(|err| NodeError::new(err.to_string()))
                        })
                        .await
                        .ok_or_else(|| NodeError::new("missing resource context"))??;
                    Ok(json!({
                        "full_name": value.get("full_name"),
                        "cv_filename": value.get("cv_filename"),
                        "artifact_path": path,
                        "byte_len": bytes.len(),
                        "raw_bytes_in_payload": raw_payload.contains("PRIVATE-CV-TEXT"),
                    }))
                }
            })
            .unwrap();

        let mut builder =
            FlowBuilder::new("multipart_web_host", Version::new(1, 0, 0), Profile::Web);
        let trigger = builder
            .add_node(
                "trigger",
                &NodeSpec::inline_with_hints(
                    "tests::multipart_trigger",
                    "MultipartTrigger",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Effectful,
                    Determinism::BestEffort,
                    None,
                    &[],
                    &[capabilities::workspace::HINT_WORKSPACE_WRITE],
                ),
            )
            .unwrap();
        let sink = builder
            .add_node(
                "respond",
                &NodeSpec::inline_with_hints(
                    "tests::multipart_sink",
                    "MultipartSink",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::ReadOnly,
                    Determinism::BestEffort,
                    None,
                    &[],
                    &[capabilities::workspace::HINT_WORKSPACE_READ],
                ),
            )
            .unwrap();
        builder.connect(&trigger, &sink);
        let mut flow = builder.build();
        flow.policies.durability.mode = DurabilityMode::Off;
        let trigger = flow
            .nodes
            .iter_mut()
            .find(|node| node.alias == "trigger")
            .unwrap();
        trigger.kind = NodeKind::Trigger;
        trigger.idempotency.key = Some("multipart-ingress".to_string());
        let ir = Arc::new(validate(&flow).expect("multipart flow validates"));
        (FlowExecutor::new(Arc::new(registry)), ir)
    }

    fn multipart_config() -> MultipartIngressConfig {
        MultipartIngressConfig::pdf("cv", "cv_artifact").with_filename_metadata_field("cv_filename")
    }

    fn multipart_body(
        boundary: &str,
        text_parts: &[(&str, &str)],
        file_parts: &[(&str, &str, &str, &[u8])],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        for (name, value) in text_parts {
            out.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            out.extend_from_slice(
                format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
            );
            out.extend_from_slice(value.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        for (name, filename, content_type, bytes) in file_parts {
            out.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            out.extend_from_slice(
                format!(
                    "Content-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\nContent-Type: {content_type}\r\n\r\n"
                )
                .as_bytes(),
            );
            out.extend_from_slice(bytes);
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        out
    }

    fn multipart_request(boundary: &str, body: Vec<u8>) -> Request<Body> {
        Request::builder()
            .method(Method::POST)
            .uri("/upload")
            .header(
                axum::http::header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body))
            .unwrap()
    }

    fn multipart_router(
        multipart: MultipartIngressConfig,
    ) -> (Router<()>, Arc<MultipartWorkspaceFactory>) {
        let (executor, ir) = build_multipart_flow();
        let factory = Arc::new(MultipartWorkspaceFactory::default());
        let config = RouteConfig::new("/upload")
            .with_method(Method::POST)
            .with_trigger_alias("trigger")
            .with_capture_alias("respond")
            .with_workspace_factory(factory.clone())
            .with_multipart_ingress(multipart);
        let router = HostHandle::try_new(executor, ir, config)
            .expect("multipart router builds")
            .router();
        (router, factory)
    }

    fn multipart_router_with_sink_gate(
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    ) -> Router<()> {
        let (executor, ir) = build_multipart_flow_with_sink_gate(Some((entered, release)));
        let factory = Arc::new(MultipartWorkspaceFactory::default());
        let config = RouteConfig::new("/upload")
            .with_method(Method::POST)
            .with_trigger_alias("trigger")
            .with_capture_alias("respond")
            .with_workspace_factory(factory)
            .with_multipart_ingress(multipart_config())
            .with_multipart_admission_limit(1);
        HostHandle::try_new(executor, ir, config)
            .expect("blocking multipart router builds")
            .router()
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
    fn multipart_and_transform_defaults_state_56_mib_input_envelope() {
        assert_eq!(DEFAULT_MULTIPART_ADMISSION_LIMIT, 4);
        assert_eq!(DEFAULT_TRANSFORM_ADMISSION_LIMIT, 2);
        assert_eq!(
            DEFAULT_MULTIPART_TRANSFORM_INPUT_ENVELOPE_BYTES,
            56 * 1024 * 1024
        );
    }

    #[tokio::test]
    async fn multipart_pdf_stages_artifact_without_json_bytes_and_cleans_workspace() {
        let (router, factory) = multipart_router(multipart_config());
        let boundary = "lattice-valid-boundary";
        let pdf = b"%PDF-1.4\nPRIVATE-CV-TEXT\n%%EOF";
        let body = multipart_body(
            boundary,
            &[("full_name", "Ada Example")],
            &[("cv", "ada-cv.pdf", "application/pdf", pdf)],
        );
        let split = body.len() / 3;
        let chunks = vec![
            Ok::<_, Infallible>(hyper::body::Bytes::copy_from_slice(&body[..split])),
            Ok::<_, Infallible>(hyper::body::Bytes::copy_from_slice(&body[split..split * 2])),
            Ok::<_, Infallible>(hyper::body::Bytes::copy_from_slice(&body[split * 2..])),
        ];
        let request = Request::builder()
            .method(Method::POST)
            .uri("/upload")
            .header(
                axum::http::header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from_stream(stream::iter(chunks)))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let value: JsonValue = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["full_name"], "Ada Example");
        assert_eq!(value["cv_filename"], "ada-cv.pdf");
        assert_eq!(value["byte_len"], pdf.len());
        assert_eq!(value["raw_bytes_in_payload"], false);
        let expected_path = format!("ingress/0000-{}", capabilities::artifact::sha256_hex(pdf));
        assert_eq!(value["artifact_path"], expected_path);
        assert!(!String::from_utf8_lossy(&bytes).contains("PRIVATE-CV-TEXT"));
        assert!(!value["artifact_path"].as_str().unwrap().contains("escape"));
        assert_eq!(factory.opened.lock().unwrap().len(), 1);
        assert_eq!(factory.completed.lock().unwrap().len(), 1);
        assert_eq!(
            factory.completed.lock().unwrap()[0].1,
            WorkspaceCompletionDisposition::Succeeded
        );
        assert!(factory.workspace.files.lock().unwrap().is_empty());
    }

    fn pending_multipart_request(polls: Arc<AtomicUsize>) -> Request<Body> {
        let body = Body::from_stream(futures::stream::poll_fn(move |_cx| {
            polls.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::<Option<Result<hyper::body::Bytes, Infallible>>>::Pending
        }));
        Request::builder()
            .method(Method::POST)
            .uri("/upload")
            .header(
                axum::http::header::CONTENT_TYPE,
                "multipart/form-data; boundary=lattice-pending",
            )
            .body(body)
            .unwrap()
    }

    #[tokio::test]
    async fn multipart_n_plus_one_saturates_before_rejected_body_is_polled() {
        let (router, _factory) = multipart_router(multipart_config());
        let mut admitted = Vec::new();
        let admitted_polls = Arc::new(AtomicUsize::new(0));
        for _ in 0..DEFAULT_MULTIPART_ADMISSION_LIMIT {
            let service = router.clone();
            let request = pending_multipart_request(admitted_polls.clone());
            admitted.push(tokio::spawn(async move { service.oneshot(request).await }));
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while admitted_polls.load(Ordering::SeqCst) < DEFAULT_MULTIPART_ADMISSION_LIMIT {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all admitted bodies must be polled");

        let rejected_polls = Arc::new(AtomicUsize::new(0));
        let response = router
            .oneshot(pending_multipart_request(rejected_polls.clone()))
            .await
            .expect("saturation response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            rejected_polls.load(Ordering::SeqCst),
            0,
            "the N+1 body must not be polled"
        );
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let value: JsonValue = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["class"], "busy");

        for task in admitted {
            task.abort();
        }
    }

    #[tokio::test]
    async fn multipart_permit_releases_after_staging_before_blocked_graph_execution() {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let router = multipart_router_with_sink_gate(entered.clone(), release.clone());
        let pdf = b"%PDF-1.4\nPRIVATE-CV-TEXT\n%%EOF";
        let first_request = multipart_request(
            "lattice-release",
            multipart_body(
                "lattice-release",
                &[("full_name", "Ada")],
                &[("cv", "cv.pdf", "application/pdf", pdf.as_slice())],
            ),
        );
        let first_router = router.clone();
        let first = tokio::spawn(async move { first_router.oneshot(first_request).await });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .expect("the first request reaches the blocked downstream node");

        let admitted_polls = Arc::new(AtomicUsize::new(0));
        let second_router = router.clone();
        let second_polls = admitted_polls.clone();
        let second = tokio::spawn(async move {
            second_router
                .oneshot(pending_multipart_request(second_polls))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while admitted_polls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a new request acquires the released route permit and polls its body");

        let rejected_polls = Arc::new(AtomicUsize::new(0));
        let response = router
            .oneshot(pending_multipart_request(rejected_polls.clone()))
            .await
            .expect("third request gets a saturation response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(rejected_polls.load(Ordering::SeqCst), 0);

        second.abort();
        release.notify_one();
        let response = first
            .await
            .expect("first task joins")
            .expect("first response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn multipart_n_plus_one_real_tcp_returns_busy() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (router, _factory) = multipart_router(multipart_config());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let pending_body = b"--lattice-pending\r\nContent-Disposition: form-data; name=\"cv\"; filename=\"pending.pdf\"\r\nContent-Type: application/pdf\r\n\r\n%PDF-";
        let mut admitted = Vec::new();
        for _ in 0..DEFAULT_MULTIPART_ADMISSION_LIMIT {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(
                    format!(
                        "POST /upload HTTP/1.1\r\nHost: {addr}\r\nContent-Type: multipart/form-data; boundary=lattice-pending\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n{:x}\r\n",
                        pending_body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            stream.write_all(pending_body).await.unwrap();
            stream.write_all(b"\r\n").await.unwrap();
            admitted.push(stream);
        }
        let mut busy_response = None;
        for _ in 0..50 {
            let mut rejected = tokio::net::TcpStream::connect(addr).await.unwrap();
            rejected
                .write_all(
                    format!(
                        "POST /upload HTTP/1.1\r\nHost: {addr}\r\nContent-Type: multipart/form-data; boundary=lattice-pending\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n0\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            tokio::time::timeout(Duration::from_secs(2), rejected.read_to_end(&mut response))
                .await
                .expect("probe response must be immediate")
                .unwrap();
            let response = String::from_utf8_lossy(&response).into_owned();
            if response.starts_with("HTTP/1.1 503") {
                busy_response = Some(response);
                break;
            }
            assert!(response.starts_with("HTTP/1.1 400"), "{response}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let response = busy_response.expect("four incomplete requests must saturate admission");
        assert!(response.contains("\"class\":\"busy\""), "{response}");

        drop(admitted);
        server.abort();
    }

    #[test]
    fn multipart_route_requires_workspace_factory() {
        let (executor, ir) = build_multipart_flow();
        let config = RouteConfig::new("/upload")
            .with_trigger_alias("trigger")
            .with_capture_alias("respond")
            .with_multipart_ingress(multipart_config());
        let err = match HostHandle::try_new(executor, ir, config) {
            Ok(_) => panic!("missing workspace factory must fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("workspace factory"));
    }

    #[tokio::test]
    async fn multipart_rejects_malformed_duplicate_mime_magic_and_unknown_files() {
        let boundary = "lattice-negative-boundary";
        let pdf = b"%PDF-1.4\ntext\n%%EOF";
        let cases = vec![
            (
                multipart_body(boundary, &[("full_name", "Ada")], &[]),
                StatusCode::BAD_REQUEST,
            ),
            (
                multipart_body(
                    boundary,
                    &[],
                    &[
                        ("cv", "one.pdf", "application/pdf", pdf.as_slice()),
                        ("cv", "two.pdf", "application/pdf", pdf.as_slice()),
                    ],
                ),
                StatusCode::BAD_REQUEST,
            ),
            (
                multipart_body(
                    boundary,
                    &[],
                    &[("other", "cv.pdf", "application/pdf", pdf.as_slice())],
                ),
                StatusCode::BAD_REQUEST,
            ),
            (
                multipart_body(
                    boundary,
                    &[("full_name", "Ada"), ("full_name", "Again")],
                    &[("cv", "cv.pdf", "application/pdf", pdf.as_slice())],
                ),
                StatusCode::BAD_REQUEST,
            ),
            (
                multipart_body(
                    boundary,
                    &[],
                    &[("cv", "", "application/pdf", pdf.as_slice())],
                ),
                StatusCode::BAD_REQUEST,
            ),
            (
                multipart_body(
                    boundary,
                    &[],
                    &[("cv", "cv.pdf", "text/plain", pdf.as_slice())],
                ),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ),
            (
                multipart_body(
                    boundary,
                    &[],
                    &[("cv", "cv.pdf", "application/pdf", b"not-a-pdf")],
                ),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ),
        ];

        for (body, expected) in cases {
            let (router, factory) = multipart_router(multipart_config());
            let response = router
                .oneshot(multipart_request(boundary, body))
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            assert!(factory.opened.lock().unwrap().is_empty());
        }

        let (router, factory) = multipart_router(multipart_config());
        let request = Request::builder()
            .method(Method::POST)
            .uri("/upload")
            .header(axum::http::header::CONTENT_TYPE, "multipart/form-data")
            .body(Body::from(b"malformed".as_slice()))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(factory.opened.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn multipart_enforces_total_file_and_text_limits_and_rejects_sse() {
        let boundary = "lattice-limit-boundary";
        let pdf = b"%PDF-12345";

        let (router, factory) = multipart_router(multipart_config().with_limits(1024, 6, 32));
        let response = router
            .oneshot(multipart_request(
                boundary,
                multipart_body(
                    boundary,
                    &[],
                    &[("cv", "cv.pdf", "application/pdf", pdf.as_slice())],
                ),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(factory.opened.lock().unwrap().is_empty());

        let (router, factory) = multipart_router(multipart_config().with_limits(1024, 64, 3));
        let response = router
            .oneshot(multipart_request(
                boundary,
                multipart_body(
                    boundary,
                    &[("full_name", "long")],
                    &[("cv", "cv.pdf", "application/pdf", pdf.as_slice())],
                ),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(factory.opened.lock().unwrap().is_empty());

        let (router, factory) = multipart_router(multipart_config().with_limits(64, 32, 16));
        let streamed_body = multipart_body(
            boundary,
            &[],
            &[("cv", "cv.pdf", "application/pdf", pdf.as_slice())],
        );
        let chunk_size = 17;
        let chunks = streamed_body
            .chunks(chunk_size)
            .map(|chunk| Ok::<_, Infallible>(hyper::body::Bytes::copy_from_slice(chunk)))
            .collect::<Vec<_>>();
        let request = Request::builder()
            .method(Method::POST)
            .uri("/upload")
            .header(
                axum::http::header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from_stream(stream::iter(chunks)))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let error = to_bytes(response.into_body(), 1024).await.unwrap();
        assert!(
            String::from_utf8_lossy(&error).contains("multipart request exceeds limit"),
            "streamed overflow must map to stable 413 rather than malformed 400"
        );
        assert!(factory.opened.lock().unwrap().is_empty());

        let (router, factory) = multipart_router(multipart_config());
        let mut request = multipart_request(
            boundary,
            multipart_body(
                boundary,
                &[],
                &[("cv", "cv.pdf", "application/pdf", pdf.as_slice())],
            ),
        );
        request.headers_mut().insert(
            axum::http::header::ACCEPT,
            axum::http::HeaderValue::from_static("text/event-stream"),
        );
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let value: JsonValue = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["class"], "not_acceptable");
        assert!(factory.opened.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn json_requests_are_bounded() {
        let (executor, ir) = build_flow();
        let config = RouteConfig::new("/echo")
            .with_trigger_alias("trigger")
            .with_capture_alias("respond")
            .with_json_body_limit(8);
        let router = HostHandle::try_new(executor, ir, config).unwrap().router();
        let request = Request::builder()
            .method(Method::POST)
            .uri("/echo")
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"value":"too large"}"#))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
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
                    let mut flow = builder.build();
                    flow.policies.durability.mode = DurabilityMode::Off;
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

    #[test]
    fn classified_node_failure_sanitizer_accepts_only_closed_codes_and_classes() {
        assert_eq!(
            sanitized_classified_node_failure(
                "APP-PDF-002: application operation failed [unsupported_document]"
            ),
            Some(("APP-PDF-002", "unsupported_document"))
        );
        assert_eq!(
            sanitized_classified_node_failure(
                "FLOW-ERR-7: application operation failed [platform_terminated]"
            ),
            Some(("FLOW-ERR-7", "platform_terminated"))
        );
        assert_eq!(
            sanitized_classified_node_failure(
                "APP-PDF-002: application operation failed [parser_secret]"
            ),
            None
        );
        assert_eq!(
            sanitized_classified_node_failure(
                "product.error: application operation failed [guest_failed]"
            ),
            None
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
        let mut flow = builder.build();
        // Disable default durability so the 500 asserts the node-failure mapping
        // rather than a missing checkpoint store (which would also yield 500).
        flow.policies.durability.mode = DurabilityMode::Off;
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
        let mut flow = builder.build();
        // Disable default durability so durability preflight passes and the
        // capability preflight (the assertion target) is actually exercised.
        flow.policies.durability.mode = DurabilityMode::Off;
        let validated = validate(&flow).expect("validated");

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
    async fn preflight_missing_durability_services_returns_code() {
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
        let mut builder =
            FlowBuilder::new("preflight_durable", Version::new(1, 0, 0), Profile::Web);
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
                    "Sink",
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
        flow.policies.durability.mode = DurabilityMode::Strong;
        let validated = validate(&flow).expect("validated");

        let config = RouteConfig::new("/preflight_durable")
            .with_method(Method::POST)
            .with_trigger_alias("trigger")
            .with_capture_alias("respond");
        let state = make_state(executor, Arc::new(validated), config);

        let request = Request::builder()
            .method(Method::POST)
            .uri("/preflight_durable")
            .header("content-type", "application/json")
            .body(Body::from(json!({"ok": true}).to_string()))
            .unwrap();

        let response = super::dispatch_request(State(state), request).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        let body: JsonValue = serde_json::from_slice(&bytes).expect("parse json body");
        assert_eq!(body["code"], json!("DAG-CKPT-003"));
        let missing = body["details"]["missing"]
            .as_array()
            .expect("details.missing array");
        assert!(missing.contains(&json!("durability::checkpoint_store")));
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
        // Disable default durability so capability preflight (the assertion target)
        // runs instead of failing first on a missing checkpoint store.
        flow.policies.durability.mode = DurabilityMode::Off;
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
        let mut flow = builder.build();
        // Disable default durability so the SSE error event reflects the capability
        // preflight failure (the assertion target), not a missing checkpoint store.
        flow.policies.durability.mode = DurabilityMode::Off;
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

    #[test]
    fn map_execution_error_reports_pinned_bundle_unavailable() {
        let (status, body) =
            super::map_execution_error(ExecutionError::CheckpointPinnedBundleUnavailable {
                checkpoint_id: "cp-1".to_string(),
                required_bundle_id: "sha256:abc".to_string(),
                runtime_bundle_id: Some("sha256:def".to_string()),
            });

        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["code"], json!("DAG-CKPT-010"));
        assert_eq!(body["details"]["checkpoint_id"], json!("cp-1"));
        assert_eq!(body["details"]["required_bundle_id"], json!("sha256:abc"));
        assert_eq!(body["details"]["runtime_bundle_id"], json!("sha256:def"));
    }
}
