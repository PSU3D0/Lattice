//! Test worker for host-workers E2E tests.
//!
//! Provides flow entrypoints that exercise the host-workers runtime path:
//! - GET /health - basic health check
//! - POST /echo - echo request body
//! - POST /stream - streaming SSE response
//! - POST /cancel - test cancellation (long-running request)
//! - POST /timer - halt + resume via Durable Object alarm dispatch

use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use async_stream::stream;
use cap_do_workers::{DurableObjectBinding, WorkersDurableObject};
use capabilities::ResourceBag;
use capabilities::durability::{CheckpointFilter, CheckpointStore};
use dag_core::{DurabilityMode, NodeError, NodeResult};
use dag_macros::{def_node, node};
use futures::Stream;
use host_inproc::{FlowBundle, FlowEntrypoint, NodeContract, NodeSource};
use kernel_exec::{NodeRegistry, RegistryError};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
#[cfg(target_arch = "wasm32")]
use worker::{Context, Env, Request, Response, Result, event};

pub use cap_do_workers::FlowDurableObject;

#[cfg(target_arch = "wasm32")]
#[event(fetch)]
async fn fetch(req: Request, env: Env, ctx: Context) -> Result<Response> {
    if req.path() == "/__test/checkpoint" {
        return handle_test_checkpoint(req, &env).await;
    }
    if req.path() == "/__test/alarm/tick" {
        return handle_test_alarm_tick(&env).await;
    }

    configure_resources(&env)?;
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
}

#[unsafe(no_mangle)]
pub extern "Rust" fn get_bundle() -> host_inproc::FlowBundle {
    bundle_with_policies()
}
