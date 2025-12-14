//! In-process executor for validated Flow IR graphs.
//!
//! This module provides a cooperative Tokio scheduler that wires node handlers
//! via bounded channels. It honours buffer capacities expressed on edges,
//! propagates cancellation on failures or deadlines, and exposes a thin
//! interface for hosts to drive requests (e.g. HTTP triggers).

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::{Context as AnyhowContext, Result as AnyhowResult};
use async_trait::async_trait;
use capabilities::{ResourceAccess, ResourceBag, context};
use dag_core::{NodeError, NodeResult, Profile};
use futures::{Stream, StreamExt};
use kernel_plan::ValidatedIR;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use tempfile::{Builder as TempDirBuilder, TempDir};
use thiserror::Error;
use tokio::fs;
use tokio::sync::{
    OwnedSemaphorePermit, Semaphore, mpsc,
    mpsc::error::{SendError, TrySendError},
};
use tokio::time;
use tokio_stream::{StreamMap, wrappers::ReceiverStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument, trace, warn};
use uuid::Uuid;

const DEFAULT_EDGE_CAPACITY: usize = 32;
const DEFAULT_TRIGGER_CAPACITY: usize = 8;
const DEFAULT_CAPTURE_CAPACITY: usize = 8;

/// Registry mapping node identifiers to executable handlers.
#[derive(Default)]
pub struct NodeRegistry {
    handlers: HashMap<&'static str, Arc<dyn NodeHandler>>,
}

impl NodeRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }
    /// Register a new async handler identified by the fully-qualified node identifier.
    pub fn register_fn<F, Fut, In, Out>(
        &mut self,
        identifier: &'static str,
        handler: F,
    ) -> Result<(), RegistryError>
    where
        F: Fn(In) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = NodeResult<Out>> + Send + 'static,
        In: DeserializeOwned + Send + Sync + 'static,
        Out: Serialize + Send + Sync + 'static,
    {
        if self.handlers.contains_key(identifier) {
            return Err(RegistryError::Duplicate(identifier));
        }
        let wrapper = FunctionHandler::new(handler);
        self.handlers
            .insert(identifier, Arc::new(wrapper) as Arc<dyn NodeHandler>);
        Ok(())
    }

    /// Register a streaming handler returning an async stream of payloads.
    pub fn register_stream_fn<F, Fut, In, S, Item>(
        &mut self,
        identifier: &'static str,
        handler: F,
    ) -> Result<(), RegistryError>
    where
        F: Fn(In) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = NodeResult<S>> + Send + 'static,
        In: DeserializeOwned + Send + Sync + 'static,
        S: Stream<Item = NodeResult<Item>> + Send + 'static,
        Item: Serialize + Send + Sync + 'static,
    {
        if self.handlers.contains_key(identifier) {
            return Err(RegistryError::Duplicate(identifier));
        }
        let wrapper = StreamingFunctionHandler::new(handler);
        self.handlers
            .insert(identifier, Arc::new(wrapper) as Arc<dyn NodeHandler>);
        Ok(())
    }

    /// Lookup a handler by implementation identifier.
    pub(crate) fn handler(&self, identifier: &str) -> Option<Arc<dyn NodeHandler>> {
        self.handlers.get(identifier).cloned()
    }
}

/// Errors produced when registering node handlers.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    /// Handler already registered.
    #[error("node `{0}` already registered")]
    Duplicate(&'static str),
}

#[async_trait]
trait NodeHandler: Send + Sync {
    async fn invoke(&self, input: JsonValue, ctx: &NodeContext) -> NodeResult<NodeOutput>;
}

struct FunctionHandler<F, Fut, In, Out>
where
    F: Fn(In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = NodeResult<Out>> + Send + 'static,
    In: DeserializeOwned + Send + 'static,
    Out: Serialize + Send + 'static,
{
    inner: F,
    _marker: PhantomData<(In, Out)>,
}

impl<F, Fut, In, Out> FunctionHandler<F, Fut, In, Out>
where
    F: Fn(In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = NodeResult<Out>> + Send + 'static,
    In: DeserializeOwned + Send + Sync + 'static,
    Out: Serialize + Send + Sync + 'static,
{
    fn new(inner: F) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<F, Fut, In, Out> NodeHandler for FunctionHandler<F, Fut, In, Out>
where
    F: Fn(In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = NodeResult<Out>> + Send + 'static,
    In: DeserializeOwned + Send + Sync + 'static,
    Out: Serialize + Send + Sync + 'static,
{
    async fn invoke(&self, input: JsonValue, _ctx: &NodeContext) -> NodeResult<NodeOutput> {
        let deserialised: In = serde_json::from_value(input.clone())
            .map_err(|err| NodeError::new(format!("failed to deserialize node input: {err}")))?;
        let output = (self.inner)(deserialised).await?;
        let json = serde_json::to_value(output)
            .map_err(|err| NodeError::new(format!("failed to serialise node output: {err}")))?;
        Ok(NodeOutput::Value(json))
    }
}

struct StreamingFunctionHandler<F, Fut, In, S, Item>
where
    F: Fn(In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = NodeResult<S>> + Send + 'static,
    In: DeserializeOwned + Send + Sync + 'static,
    S: Stream<Item = NodeResult<Item>> + Send + 'static,
    Item: Serialize + Send + Sync + 'static,
{
    inner: F,
    _marker: PhantomData<(In, Item)>,
}

impl<F, Fut, In, S, Item> StreamingFunctionHandler<F, Fut, In, S, Item>
where
    F: Fn(In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = NodeResult<S>> + Send + 'static,
    In: DeserializeOwned + Send + Sync + 'static,
    S: Stream<Item = NodeResult<Item>> + Send + 'static,
    Item: Serialize + Send + Sync + 'static,
{
    fn new(inner: F) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<F, Fut, In, S, Item> NodeHandler for StreamingFunctionHandler<F, Fut, In, S, Item>
where
    F: Fn(In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = NodeResult<S>> + Send + 'static,
    In: DeserializeOwned + Send + Sync + 'static,
    S: Stream<Item = NodeResult<Item>> + Send + 'static,
    Item: Serialize + Send + Sync + 'static,
{
    async fn invoke(&self, input: JsonValue, _ctx: &NodeContext) -> NodeResult<NodeOutput> {
        let deserialised: In = serde_json::from_value(input.clone())
            .map_err(|err| NodeError::new(format!("failed to deserialize node input: {err}")))?;
        let stream = (self.inner)(deserialised).await?;
        let json_stream = stream.map(|item| {
            item.and_then(|value| {
                serde_json::to_value(value).map_err(|err| {
                    NodeError::new(format!("failed to serialise stream item: {err}"))
                })
            })
        });
        Ok(NodeOutput::Stream(json_stream.boxed()))
    }
}

/// Lightweight execution context passed to nodes.
#[derive(Clone)]
pub struct NodeContext {
    cancellation: CancellationToken,
    resources: Arc<dyn ResourceAccess>,
}

impl NodeContext {
    fn new(cancellation: CancellationToken, resources: Arc<dyn ResourceAccess>) -> Self {
        Self {
            cancellation,
            resources,
        }
    }

    /// Returns `true` if execution has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Access the underlying cancellation token.
    pub fn token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Borrow the resource access collection for capability lookups.
    pub fn resources(&self) -> &dyn ResourceAccess {
        self.resources.as_ref()
    }

    /// Clone the underlying resource access handle.
    pub fn resource_handle(&self) -> Arc<dyn ResourceAccess> {
        self.resources.clone()
    }
}

enum CapturedPayload {
    Value(JsonValue),
    Stream(StreamingCapture),
}

struct StreamingCapture {
    alias: String,
    stream: futures::stream::BoxStream<'static, NodeResult<JsonValue>>,
    cancellation: CancellationToken,
    permit: Option<OwnedSemaphorePermit>,
}

impl StreamingCapture {
    fn new(
        alias: String,
        stream: futures::stream::BoxStream<'static, NodeResult<JsonValue>>,
        cancellation: CancellationToken,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            alias,
            stream,
            cancellation,
            permit,
        }
    }
}

/// Result produced by a node handler.
pub enum NodeOutput {
    /// Single JSON value to forward downstream.
    Value(JsonValue),
    /// Stream of JSON payloads emitted incrementally.
    Stream(futures::stream::BoxStream<'static, NodeResult<JsonValue>>),
    /// Node intentionally emitted no output.
    None,
}

impl fmt::Debug for NodeOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeOutput::Value(value) => f.debug_tuple("Value").field(value).finish(),
            NodeOutput::Stream(_) => f.write_str("Stream(..)"),
            NodeOutput::None => f.write_str("None"),
        }
    }
}

#[derive(Debug)]
struct ExecutorMetrics {
    flow: Arc<str>,
    profile: &'static str,
}

impl ExecutorMetrics {
    fn new(flow: String, profile: Profile) -> Self {
        Self {
            flow: Arc::from(flow),
            profile: profile_label(profile),
        }
    }

    fn flow(&self) -> &str {
        self.flow.as_ref()
    }

    fn profile(&self) -> &'static str {
        self.profile
    }
}

impl ExecutorMetrics {
    fn queue_tracker(self: &Arc<Self>, edge: Arc<str>, capacity: usize) -> Arc<QueueDepthTracker> {
        Arc::new(QueueDepthTracker::new(Arc::clone(self), edge, capacity))
    }

    fn track_node(self: &Arc<Self>, node: &str) -> NodeRunGuard {
        let flow_label = self.flow().to_string();
        let node_label = node.to_string();
        let profile_label = self.profile();
        metrics::gauge!(
            "lattice.executor.active_nodes",
            "flow" => flow_label,
            "node" => node_label,
            "profile" => profile_label
        )
        .increment(1.0);
        NodeRunGuard::new(Arc::clone(self), node.to_string())
    }

    fn record_node_error(self: &Arc<Self>, node: &str, error_kind: &str) {
        let flow_label = self.flow().to_string();
        let node_label = node.to_string();
        let error_label = error_kind.to_string();
        let profile_label = self.profile();
        metrics::counter!(
            "lattice.executor.node_errors_total",
            "flow" => flow_label,
            "node" => node_label,
            "profile" => profile_label,
            "error_kind" => error_label
        )
        .increment(1);
    }

    fn record_cancellation(self: &Arc<Self>, node: &str, reason: &str) {
        let flow_label = self.flow().to_string();
        let node_label = node.to_string();
        let reason_label = reason.to_string();
        let profile_label = self.profile();
        metrics::counter!(
            "lattice.executor.cancellations_total",
            "flow" => flow_label,
            "node" => node_label,
            "profile" => profile_label,
            "reason" => reason_label
        )
        .increment(1);
    }

    fn observe_capture_backpressure(self: &Arc<Self>, capture: &str, waited: Duration) {
        let millis = waited.as_secs_f64() * 1_000.0;
        let flow_label = self.flow().to_string();
        let capture_label = capture.to_string();
        let profile_label = self.profile();
        metrics::histogram!(
            "lattice.executor.capture_backpressure_ms",
            "flow" => flow_label,
            "capture" => capture_label,
            "profile" => profile_label
        )
        .record(millis);
    }

    fn record_stream_spawn(self: &Arc<Self>, node: &str) {
        let flow_label = self.flow().to_string();
        let node_label = node.to_string();
        let profile_label = self.profile();
        metrics::counter!(
            "lattice.executor.stream_clients_total",
            "flow" => flow_label,
            "node" => node_label,
            "profile" => profile_label
        )
        .increment(1);
    }
}

#[derive(Debug, Error)]
enum SpillError {
    #[error("failed to serialise payload for spill: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("failed to persist/load spill file: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug)]
struct BlobSpill {
    root: PathBuf,
    _guard: TempDir,
}

impl BlobSpill {
    fn new(flow_id: &str, tier: &str) -> AnyhowResult<Self> {
        let dir = TempDirBuilder::new()
            .prefix(&format!("lf-spill-{flow_id}-{tier}-"))
            .tempdir()
            .context("create spill directory")?;
        let root = dir.path().to_path_buf();
        Ok(Self { root, _guard: dir })
    }

    async fn persist(&self, payload: &JsonValue) -> Result<SpillRecord, SpillError> {
        let bytes = serde_json::to_vec(payload)?;
        let key = format!("{}.json", Uuid::new_v4());
        let path = self.root.join(&key);
        fs::write(&path, &bytes).await?;
        Ok(SpillRecord { key })
    }

    async fn load(&self, key: &str) -> Result<JsonValue, SpillError> {
        let path = self.root.join(key);
        let bytes = fs::read(&path).await?;
        let value = serde_json::from_slice(&bytes)?;
        Ok(value)
    }

    async fn remove(&self, key: &str) -> Result<(), SpillError> {
        let path = self.root.join(key);
        if path.exists() {
            fs::remove_file(path).await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct SpillRecord {
    key: String,
}

#[derive(Clone)]
struct SpillContext {
    storage: Arc<BlobSpill>,
    threshold_bytes: Option<u64>,
}

impl SpillContext {
    fn new(storage: Arc<BlobSpill>, threshold_bytes: Option<u64>) -> Self {
        Self {
            storage,
            threshold_bytes,
        }
    }

    fn exceeds_threshold(&self, payload: &JsonValue) -> bool {
        match self.threshold_bytes {
            Some(limit) => match serde_json::to_vec(payload) {
                Ok(bytes) => bytes.len() as u64 >= limit,
                Err(_) => false,
            },
            None => false,
        }
    }

    async fn spill_and_send(
        &self,
        sender: &mpsc::Sender<FlowMessage>,
        tracker: Arc<QueueDepthTracker>,
        message: FlowMessage,
    ) -> Result<(), SendError<FlowMessage>> {
        let FlowMessage::Data {
            payload,
            permit,
            queue_tracker,
        } = message
        else {
            return Err(SendError(FlowMessage::Data {
                payload: JsonValue::Null,
                permit: None,
                queue_tracker: None,
            }));
        };

        let record = match self.storage.persist(&payload).await {
            Ok(record) => record,
            Err(_) => {
                return Err(SendError(FlowMessage::Data {
                    payload,
                    permit,
                    queue_tracker,
                }));
            }
        };

        drop(payload);

        let spilled = FlowMessage::Spilled {
            key: record.key.clone(),
            storage: Arc::clone(&self.storage),
            permit,
            queue_tracker,
        };

        match sender.reserve().await {
            Ok(permit) => {
                tracker.increment();
                permit.send(spilled);
                Ok(())
            }
            Err(_) => {
                let _ = self.storage.remove(&record.key).await;
                Err(SendError(spilled))
            }
        }
    }
}

struct SpillManager {
    storages: HashMap<String, Arc<BlobSpill>>,
}

impl SpillManager {
    fn new(flow: &dag_core::FlowIR) -> AnyhowResult<Self> {
        let mut storages = HashMap::new();
        let flow_id = flow.id.as_str().to_string();
        for edge in &flow.edges {
            if let Some(tier) = &edge.buffer.spill_tier
                && !storages.contains_key(tier)
            {
                let storage = Arc::new(BlobSpill::new(&flow_id, tier)?);
                storages.insert(tier.clone(), storage);
            }
        }
        Ok(Self { storages })
    }

    fn context_for(&self, policy: &dag_core::BufferPolicy) -> Option<Arc<SpillContext>> {
        policy.spill_tier.as_ref().and_then(|tier| {
            self.storages.get(tier).map(|storage| {
                Arc::new(SpillContext::new(
                    Arc::clone(storage),
                    policy.spill_threshold_bytes,
                ))
            })
        })
    }
}

#[derive(Clone)]
struct InstrumentedSender {
    sender: mpsc::Sender<FlowMessage>,
    tracker: Arc<QueueDepthTracker>,
    spill: Option<Arc<SpillContext>>,
}

#[derive(Clone)]
struct OutgoingSender {
    to: String,
    sender: InstrumentedSender,
}

#[derive(Clone, Debug)]
struct SwitchRouting {
    surface_id: String,
    selector_pointer: String,
    cases: HashMap<String, String>,
    default: Option<String>,
    controlled_targets: HashSet<String>,
}

impl SwitchRouting {
    fn select_target<'a>(&'a self, value: &JsonValue) -> Result<Option<&'a str>, NodeError> {
        let selected = value
            .pointer(self.selector_pointer.as_str())
            .ok_or_else(|| {
                NodeError::new(format!(
                    "switch `{}` selector_pointer `{}` did not resolve",
                    self.surface_id, self.selector_pointer
                ))
            })?;

        let selector = selected.as_str().ok_or_else(|| {
            NodeError::new(format!(
                "switch `{}` selector_pointer `{}` must resolve to a string",
                self.surface_id, self.selector_pointer
            ))
        })?;

        if let Some(target) = self.cases.get(selector) {
            Ok(Some(target.as_str()))
        } else if let Some(default) = &self.default {
            Ok(Some(default.as_str()))
        } else {
            Ok(None)
        }
    }

    fn controls_edge_to(&self, target: &str) -> bool {
        self.controlled_targets.contains(target)
    }
}

#[derive(Clone, Debug)]
struct IfRouting {
    surface_id: String,
    selector_pointer: String,
    then_target: String,
    else_target: String,
}

impl IfRouting {
    fn select_target<'a>(&'a self, value: &JsonValue) -> Result<Option<&'a str>, NodeError> {
        let selected = value
            .pointer(self.selector_pointer.as_str())
            .ok_or_else(|| {
                NodeError::new(format!(
                    "if `{}` selector_pointer `{}` did not resolve",
                    self.surface_id, self.selector_pointer
                ))
            })?;

        let predicate = selected.as_bool().ok_or_else(|| {
            NodeError::new(format!(
                "if `{}` selector_pointer `{}` must resolve to a boolean",
                self.surface_id, self.selector_pointer
            ))
        })?;

        if predicate {
            Ok(Some(self.then_target.as_str()))
        } else {
            Ok(Some(self.else_target.as_str()))
        }
    }

    fn controls_edge_to(&self, target: &str) -> bool {
        self.then_target == target || self.else_target == target
    }
}

#[derive(Clone, Debug)]
enum RoutingControl {
    Switch(SwitchRouting),
    If(IfRouting),
}

impl RoutingControl {
    fn kind_label(&self) -> &'static str {
        match self {
            Self::Switch(_) => "switch",
            Self::If(_) => "if",
        }
    }

    fn failure_label(&self) -> &'static str {
        match self {
            Self::Switch(_) => "switch_routing_failed",
            Self::If(_) => "if_routing_failed",
        }
    }

    fn surface_id(&self) -> &str {
        match self {
            Self::Switch(routing) => routing.surface_id.as_str(),
            Self::If(routing) => routing.surface_id.as_str(),
        }
    }

    fn select_target<'a>(&'a self, value: &JsonValue) -> Result<Option<&'a str>, NodeError> {
        match self {
            Self::Switch(routing) => routing.select_target(value),
            Self::If(routing) => routing.select_target(value),
        }
    }

    fn controls_edge_to(&self, target: &str) -> bool {
        match self {
            Self::Switch(routing) => routing.controls_edge_to(target),
            Self::If(routing) => routing.controls_edge_to(target),
        }
    }
}

#[derive(Clone, Default)]
struct RoutingTable {
    controls: HashMap<String, RoutingControl>,
}

impl RoutingTable {
    fn from_flow(flow: &dag_core::FlowIR) -> Result<Self, ExecutionError> {
        let mut controls = HashMap::new();
        for surface in &flow.control_surfaces {
            match surface.kind {
                dag_core::ControlSurfaceKind::Switch => {
                    let source = routing_source(flow, surface, "switch")?;
                    if controls.contains_key(source.as_str()) {
                        return Err(ExecutionError::InvalidControlSurface {
                            id: surface.id.clone(),
                            kind: "switch".to_string(),
                            reason: "multiple routing control surfaces for same source".to_string(),
                        });
                    }
                    let routing = parse_switch_surface(flow, surface)?;
                    controls.insert(source, RoutingControl::Switch(routing));
                }
                dag_core::ControlSurfaceKind::If => {
                    let source = routing_source(flow, surface, "if")?;
                    if controls.contains_key(source.as_str()) {
                        return Err(ExecutionError::InvalidControlSurface {
                            id: surface.id.clone(),
                            kind: "if".to_string(),
                            reason: "multiple routing control surfaces for same source".to_string(),
                        });
                    }
                    let routing = parse_if_surface(flow, surface)?;
                    controls.insert(source, RoutingControl::If(routing));
                }
                _ => {
                    return Err(ExecutionError::UnsupportedControlSurface {
                        id: surface.id.clone(),
                        kind: format!("{:?}", surface.kind),
                    });
                }
            }
        }
        Ok(Self { controls })
    }

    fn control_for(&self, alias: &str) -> Option<&RoutingControl> {
        self.controls.get(alias)
    }
}

fn routing_source(
    flow: &dag_core::FlowIR,
    surface: &dag_core::ControlSurfaceIR,
    kind_label: &str,
) -> Result<String, ExecutionError> {
    let config =
        surface
            .config
            .as_object()
            .ok_or_else(|| ExecutionError::InvalidControlSurface {
                id: surface.id.clone(),
                kind: kind_label.to_string(),
                reason: "config must be object".to_string(),
            })?;

    let source = config
        .get("source")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ExecutionError::InvalidControlSurface {
            id: surface.id.clone(),
            kind: kind_label.to_string(),
            reason: "missing config.source".to_string(),
        })?;

    if flow.nodes.iter().any(|n| n.alias == source) {
        Ok(source.to_string())
    } else {
        Err(ExecutionError::InvalidControlSurface {
            id: surface.id.clone(),
            kind: kind_label.to_string(),
            reason: format!("unknown source node `{source}`"),
        })
    }
}

fn parse_switch_surface(
    flow: &dag_core::FlowIR,
    surface: &dag_core::ControlSurfaceIR,
) -> Result<SwitchRouting, ExecutionError> {
    let config =
        surface
            .config
            .as_object()
            .ok_or_else(|| ExecutionError::InvalidControlSurface {
                id: surface.id.clone(),
                kind: "switch".to_string(),
                reason: "config must be object".to_string(),
            })?;

    let v_ok = config
        .get("v")
        .and_then(|v| v.as_u64())
        .map(|v| v == 1)
        .unwrap_or(false);
    if !v_ok {
        return Err(ExecutionError::InvalidControlSurface {
            id: surface.id.clone(),
            kind: "switch".to_string(),
            reason: "config.v must be 1".to_string(),
        });
    }

    let selector_pointer = config
        .get("selector_pointer")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ExecutionError::InvalidControlSurface {
            id: surface.id.clone(),
            kind: "switch".to_string(),
            reason: "missing config.selector_pointer".to_string(),
        })?
        .to_string();

    let cases_obj = config
        .get("cases")
        .and_then(|v| v.as_object())
        .ok_or_else(|| ExecutionError::InvalidControlSurface {
            id: surface.id.clone(),
            kind: "switch".to_string(),
            reason: "missing config.cases".to_string(),
        })?;

    let mut cases = HashMap::new();
    let mut controlled_targets = HashSet::new();
    for (k, v) in cases_obj {
        let target = v
            .as_str()
            .ok_or_else(|| ExecutionError::InvalidControlSurface {
                id: surface.id.clone(),
                kind: "switch".to_string(),
                reason: "case targets must be strings".to_string(),
            })?;
        if !flow.nodes.iter().any(|n| n.alias == target) {
            return Err(ExecutionError::InvalidControlSurface {
                id: surface.id.clone(),
                kind: "switch".to_string(),
                reason: format!("unknown case target `{target}`"),
            });
        }
        cases.insert(k.clone(), target.to_string());
        controlled_targets.insert(target.to_string());
    }

    let default = match config.get("default") {
        Some(v) => {
            let target = v
                .as_str()
                .ok_or_else(|| ExecutionError::InvalidControlSurface {
                    id: surface.id.clone(),
                    kind: "switch".to_string(),
                    reason: "default must be string".to_string(),
                })?;
            controlled_targets.insert(target.to_string());
            Some(target.to_string())
        }
        None => None,
    };

    Ok(SwitchRouting {
        surface_id: surface.id.clone(),
        selector_pointer,
        cases,
        default,
        controlled_targets,
    })
}

fn parse_if_surface(
    flow: &dag_core::FlowIR,
    surface: &dag_core::ControlSurfaceIR,
) -> Result<IfRouting, ExecutionError> {
    let config =
        surface
            .config
            .as_object()
            .ok_or_else(|| ExecutionError::InvalidControlSurface {
                id: surface.id.clone(),
                kind: "if".to_string(),
                reason: "config must be object".to_string(),
            })?;

    let v_ok = config
        .get("v")
        .and_then(|v| v.as_u64())
        .map(|v| v == 1)
        .unwrap_or(false);
    if !v_ok {
        return Err(ExecutionError::InvalidControlSurface {
            id: surface.id.clone(),
            kind: "if".to_string(),
            reason: "config.v must be 1".to_string(),
        });
    }

    let selector_pointer = config
        .get("selector_pointer")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ExecutionError::InvalidControlSurface {
            id: surface.id.clone(),
            kind: "if".to_string(),
            reason: "missing config.selector_pointer".to_string(),
        })?
        .to_string();

    let then_target = config.get("then").and_then(|v| v.as_str()).ok_or_else(|| {
        ExecutionError::InvalidControlSurface {
            id: surface.id.clone(),
            kind: "if".to_string(),
            reason: "missing config.then".to_string(),
        }
    })?;

    let else_target = config.get("else").and_then(|v| v.as_str()).ok_or_else(|| {
        ExecutionError::InvalidControlSurface {
            id: surface.id.clone(),
            kind: "if".to_string(),
            reason: "missing config.else".to_string(),
        }
    })?;

    if !flow.nodes.iter().any(|n| n.alias == then_target) {
        return Err(ExecutionError::InvalidControlSurface {
            id: surface.id.clone(),
            kind: "if".to_string(),
            reason: format!("unknown then target `{then_target}`"),
        });
    }

    if !flow.nodes.iter().any(|n| n.alias == else_target) {
        return Err(ExecutionError::InvalidControlSurface {
            id: surface.id.clone(),
            kind: "if".to_string(),
            reason: format!("unknown else target `{else_target}`"),
        });
    }

    Ok(IfRouting {
        surface_id: surface.id.clone(),
        selector_pointer,
        then_target: then_target.to_string(),
        else_target: else_target.to_string(),
    })
}

impl InstrumentedSender {
    fn new(
        sender: mpsc::Sender<FlowMessage>,
        tracker: Arc<QueueDepthTracker>,
        spill: Option<Arc<SpillContext>>,
    ) -> Self {
        Self {
            sender,
            tracker,
            spill,
        }
    }

    async fn send(
        &self,
        payload: JsonValue,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Result<(), mpsc::error::SendError<FlowMessage>> {
        let tracker = self.tracker.clone();
        let mut message = FlowMessage::Data {
            payload,
            permit,
            queue_tracker: Some(tracker.clone()),
        };

        if let Some(spill) = &self.spill {
            if matches!(&message, FlowMessage::Data { payload, .. } if spill.exceeds_threshold(payload))
            {
                return spill.spill_and_send(&self.sender, tracker, message).await;
            }

            match self.sender.try_send(message) {
                Ok(()) => {
                    tracker.increment();
                    return Ok(());
                }
                Err(TrySendError::Full(returned)) => {
                    message = returned;
                    return spill.spill_and_send(&self.sender, tracker, message).await;
                }
                Err(TrySendError::Closed(returned)) => return Err(SendError(returned)),
            }
        }

        let result = self.sender.send(message).await;
        if result.is_ok() {
            tracker.increment();
        }
        result
    }
}

struct QueueDepthTracker {
    metrics: Arc<ExecutorMetrics>,
    edge: Arc<str>,
    depth: AtomicUsize,
    capacity: usize,
}

impl QueueDepthTracker {
    fn new(metrics: Arc<ExecutorMetrics>, edge: Arc<str>, capacity: usize) -> Self {
        let tracker = Self {
            metrics,
            edge,
            depth: AtomicUsize::new(0),
            capacity,
        };
        tracker.publish(0);
        tracker
    }

    fn increment(&self) {
        let prev = self
            .depth
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(1).min(self.capacity))
            })
            .expect("queue depth update should succeed");
        let depth = prev.saturating_add(1).min(self.capacity);
        self.publish(depth);
    }

    fn decrement(&self) {
        let prev = self
            .depth
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(1))
            })
            .expect("queue depth update should succeed");
        let depth = prev.saturating_sub(1);
        self.publish(depth);
    }

    fn publish(&self, depth: usize) {
        let flow_label = self.metrics.flow().to_string();
        let edge_label = self.edge.to_string();
        let profile_label = self.metrics.profile();
        metrics::gauge!(
            "lattice.executor.queue_depth",
            "flow" => flow_label,
            "edge" => edge_label,
            "profile" => profile_label
        )
        .set(depth as f64);
    }
}

impl fmt::Debug for QueueDepthTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueDepthTracker")
            .field("edge", &self.edge)
            .field("capacity", &self.capacity)
            .field("depth", &self.depth.load(Ordering::Relaxed))
            .finish()
    }
}

struct NodeRunGuard {
    metrics: Arc<ExecutorMetrics>,
    node: String,
    start: Instant,
}

impl NodeRunGuard {
    fn new(metrics: Arc<ExecutorMetrics>, node: String) -> Self {
        Self {
            metrics,
            node,
            start: Instant::now(),
        }
    }
}

impl Drop for NodeRunGuard {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_secs_f64() * 1_000.0;
        let flow_label = self.metrics.flow().to_string();
        let node_label = self.node.to_string();
        let profile_label = self.metrics.profile();
        metrics::histogram!(
            "lattice.executor.node_latency_ms",
            "flow" => flow_label.clone(),
            "node" => node_label.clone(),
            "profile" => profile_label
        )
        .record(elapsed);
        metrics::gauge!(
            "lattice.executor.active_nodes",
            "flow" => flow_label,
            "node" => node_label,
            "profile" => profile_label
        )
        .decrement(1.0);
    }
}

fn profile_label(profile: Profile) -> &'static str {
    match profile {
        Profile::Web => "web",
        Profile::Queue => "queue",
        Profile::Temporal => "temporal",
        Profile::Wasm => "wasm",
        Profile::Dev => "dev",
    }
}

/// High-level executor responsible for instantiating graph runs.
#[derive(Clone)]
pub struct FlowExecutor {
    registry: Arc<NodeRegistry>,
    edge_capacity: usize,
    trigger_capacity: usize,
    capture_capacity: usize,
    resources: Arc<dyn ResourceAccess>,
}

impl FlowExecutor {
    /// Construct a new executor backed by the supplied registry.
    pub fn new(registry: Arc<NodeRegistry>) -> Self {
        let default_resources: Arc<ResourceBag> = Arc::new(ResourceBag::new());
        Self {
            registry,
            edge_capacity: DEFAULT_EDGE_CAPACITY,
            trigger_capacity: DEFAULT_TRIGGER_CAPACITY,
            capture_capacity: DEFAULT_CAPTURE_CAPACITY,
            resources: default_resources,
        }
    }

    /// Override default edge channel capacity.
    pub fn with_edge_capacity(mut self, capacity: usize) -> Self {
        self.edge_capacity = capacity.max(1);
        self
    }

    /// Override capture channel capacity (defaults to 8).
    pub fn with_capture_capacity(mut self, capacity: usize) -> Self {
        self.capture_capacity = capacity.max(1);
        self
    }

    /// Provide a custom resource access handle used for node execution.
    pub fn with_resource_access<T>(mut self, resources: Arc<T>) -> Self
    where
        T: ResourceAccess,
    {
        self.resources = resources;
        self
    }

    /// Provide a convenient builder for resource bags.
    pub fn with_resource_bag(mut self, bag: ResourceBag) -> Self {
        let resources: Arc<ResourceBag> = Arc::new(bag);
        self.resources = resources;
        self
    }

    /// Instantiate a fresh flow instance ready to receive inputs.
    pub fn instantiate(
        &self,
        ir: &ValidatedIR,
        capture_alias: impl Into<String>,
    ) -> Result<FlowInstance, ExecutionError> {
        let capture_alias = capture_alias.into();
        let flow = ir.flow().clone();
        let metrics = Arc::new(ExecutorMetrics::new(flow.name.clone(), flow.profile));
        let routing = Arc::new(RoutingTable::from_flow(&flow)?);

        let mut handlers: HashMap<String, Arc<dyn NodeHandler>> = HashMap::new();
        for node in &flow.nodes {
            let identifier = node.identifier.clone();
            let handler = self
                .registry
                .handler(&identifier)
                .ok_or_else(|| ExecutionError::UnregisteredNode { identifier })?;
            handlers.insert(node.alias.clone(), handler);
        }

        let mut inbound: HashMap<String, Vec<mpsc::Receiver<FlowMessage>>> = HashMap::new();
        let mut outbound: HashMap<String, Vec<OutgoingSender>> = HashMap::new();

        for node in &flow.nodes {
            inbound.insert(node.alias.clone(), Vec::new());
            outbound.insert(node.alias.clone(), Vec::new());
        }

        let spill_manager = SpillManager::new(&flow).map_err(ExecutionError::SpillSetup)?;

        for edge in &flow.edges {
            let capacity = edge
                .buffer
                .max_items
                .map(|v| usize::try_from(v).unwrap_or(1))
                .unwrap_or(self.edge_capacity)
                .max(1);
            let (tx, rx) = mpsc::channel(capacity);
            let edge_label: Arc<str> =
                Arc::from(format!("{}->{}", edge.from.as_str(), edge.to.as_str()));
            let tracker = metrics.queue_tracker(edge_label, capacity);
            let spill_context = spill_manager.context_for(&edge.buffer);
            outbound
                .get_mut(&edge.from)
                .expect("source node exists")
                .push(OutgoingSender {
                    to: edge.to.clone(),
                    sender: InstrumentedSender::new(tx, tracker, spill_context),
                });
            inbound
                .get_mut(&edge.to)
                .expect("target node exists")
                .push(rx);
        }

        let mut trigger_inputs: HashMap<String, mpsc::Sender<FlowMessage>> = HashMap::new();
        for node in &flow.nodes {
            if inbound
                .get(&node.alias)
                .map(|r| r.is_empty())
                .unwrap_or(false)
            {
                let (tx, rx) = mpsc::channel(self.trigger_capacity);
                inbound
                    .get_mut(&node.alias)
                    .expect("node registered")
                    .push(rx);
                trigger_inputs.insert(node.alias.clone(), tx);
            }
        }

        if !flow.nodes.iter().any(|n| n.alias == capture_alias) {
            return Err(ExecutionError::UnknownCapture {
                alias: capture_alias,
            });
        }

        let (capture_tx, capture_rx) = mpsc::channel::<CapturedOutput>(self.capture_capacity);
        let cancellation = CancellationToken::new();
        let permits = Arc::new(Semaphore::new(self.capture_capacity));
        let capture_tracker = metrics.queue_tracker(
            Arc::from(format!("capture::{}", capture_alias.as_str())),
            self.capture_capacity,
        );

        let mut tasks = Vec::with_capacity(flow.nodes.len());
        for node in flow.nodes {
            let handler = handlers.get(&node.alias).expect("handler resolved").clone();
            let inputs = inbound.remove(&node.alias).expect("inputs allocated");
            let outputs = outbound.remove(&node.alias).expect("outputs allocated");
            let capture_sender = capture_tx.clone();
            let capture_target = capture_alias.clone();
            let token = cancellation.clone();
            let alias = node.alias.clone();
            let resource_handle = self.resources.clone();
            tasks.push(tokio::spawn(run_node(
                alias,
                capture_target,
                handler,
                inputs,
                outputs,
                capture_sender,
                metrics.clone(),
                capture_tracker.clone(),
                NodeContext::new(token, resource_handle),
                routing.clone(),
            )));
        }

        drop(capture_tx);

        Ok(FlowInstance {
            triggers: trigger_inputs,
            capture: capture_rx,
            cancellation,
            permits,
            tasks,
            metrics,
        })
    }

    /// Convenience helper to execute a flow once and await the first response.
    #[instrument(skip_all, fields(trigger = %trigger_alias, capture = %capture_alias))]
    pub async fn run_once(
        &self,
        ir: &ValidatedIR,
        trigger_alias: &str,
        payload: JsonValue,
        capture_alias: &str,
        deadline: Option<Duration>,
    ) -> Result<ExecutionResult, ExecutionError> {
        let mut instance = self.instantiate(ir, capture_alias)?;
        instance.send(trigger_alias, payload).await?;

        let start = Instant::now();
        let result: Result<CaptureResult, ExecutionError> = if let Some(duration) = deadline {
            match time::timeout(duration, instance.next()).await {
                Ok(Some(result)) => result,
                Ok(None) => Err(ExecutionError::MissingOutput {
                    alias: capture_alias.to_string(),
                }),
                Err(_) => {
                    instance.cancel_with_reason("deadline");
                    Err(ExecutionError::DeadlineExceeded { elapsed: duration })
                }
            }
        } else {
            match instance.next().await {
                Some(result) => result,
                None => Err(ExecutionError::MissingOutput {
                    alias: capture_alias.to_string(),
                }),
            }
        };

        let elapsed = start.elapsed();
        if let Err(err) = &result {
            debug!(?elapsed, "flow execution ended with error: {err:?}");
        } else {
            trace!(?elapsed, "flow execution completed successfully");
        }

        match result {
            Ok(CaptureResult::Value(value)) => {
                instance.shutdown().await?;
                Ok(ExecutionResult::Value(value))
            }
            Ok(CaptureResult::Stream(stream)) => {
                Ok(ExecutionResult::Stream(stream.into_handle(instance)))
            }
            Err(err) => {
                instance.shutdown().await?;
                Err(err)
            }
        }
    }
}

/// Active per-run state for a workflow.
pub struct FlowInstance {
    triggers: HashMap<String, mpsc::Sender<FlowMessage>>,
    capture: mpsc::Receiver<CapturedOutput>,
    cancellation: CancellationToken,
    permits: Arc<Semaphore>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    metrics: Arc<ExecutorMetrics>,
}

impl FlowInstance {
    /// Enqueue a payload onto a trigger node.
    pub async fn send(
        &self,
        trigger_alias: &str,
        payload: JsonValue,
    ) -> Result<(), ExecutionError> {
        let sender =
            self.triggers
                .get(trigger_alias)
                .ok_or_else(|| ExecutionError::UnknownTrigger {
                    alias: trigger_alias.to_string(),
                })?;
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ExecutionError::Cancelled)?;
        sender
            .send(FlowMessage::Data {
                payload,
                permit: Some(permit),
                queue_tracker: None,
            })
            .await
            .map_err(|_| ExecutionError::Cancelled)?;
        Ok(())
    }

    /// Fetch the next captured result from the configured sink.
    pub(crate) async fn next(&mut self) -> Option<Result<CaptureResult, ExecutionError>> {
        match self.capture.recv().await {
            Some(CapturedOutput {
                alias,
                result,
                permit,
                queue_tracker,
            }) => {
                if let Some(tracker) = queue_tracker {
                    tracker.decrement();
                }
                let outcome = match result {
                    Ok(CapturedPayload::Value(value)) => Ok(CaptureResult::Value(value)),
                    Ok(CapturedPayload::Stream(stream)) => Ok(CaptureResult::Stream(stream)),
                    Err(err) => Err(ExecutionError::NodeFailed { alias, source: err }),
                };
                drop(permit);
                Some(outcome)
            }
            None => None,
        }
    }

    /// Signal cooperative cancellation.
    pub fn cancel(&self) {
        self.cancel_with_reason("external");
    }

    /// Signal cancellation with an explicit reason (used for metrics labels).
    pub fn cancel_with_reason(&self, reason: &str) {
        self.metrics.record_cancellation("__instance", reason);
        self.cancellation.cancel();
    }

    /// Gracefully tear down the instance, awaiting node tasks.
    pub async fn shutdown(self) -> Result<(), ExecutionError> {
        self.cancellation.cancel();
        for handle in self.tasks {
            if let Err(join_err) = handle.await {
                error!("node task panicked: {join_err}");
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }
}

/// Result captured from a workflow execution.
pub(crate) enum CaptureResult {
    /// Single JSON value completion.
    Value(JsonValue),
    /// Streaming handle producing incremental events.
    Stream(StreamingCapture),
}

/// Public outcome returned by `FlowExecutor::run_once`.
pub enum ExecutionResult {
    /// Single JSON value response.
    Value(JsonValue),
    /// Streaming response handle.
    Stream(StreamHandle),
}

pub struct StreamHandle {
    alias: String,
    stream: futures::stream::BoxStream<'static, NodeResult<JsonValue>>,
    cancellation: CancellationToken,
    permit: Option<OwnedSemaphorePermit>,
    instance: Option<FlowInstance>,
}

impl StreamHandle {
    fn new(
        alias: String,
        stream: futures::stream::BoxStream<'static, NodeResult<JsonValue>>,
        cancellation: CancellationToken,
        permit: Option<OwnedSemaphorePermit>,
        instance: FlowInstance,
    ) -> Self {
        Self {
            alias,
            stream,
            cancellation,
            permit,
            instance: Some(instance),
        }
    }

    /// Clone the cancellation token associated with the stream.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    fn finalize(&mut self, cancel: bool) {
        if let Some(permit) = self.permit.take() {
            drop(permit);
        }
        if let Some(instance) = self.instance.take() {
            if cancel {
                instance.cancel_with_reason("stream_cancelled");
            }
            tokio::spawn(async move {
                if let Err(err) = instance.shutdown().await {
                    error!("stream shutdown failed: {err}");
                }
            });
        }
    }
}

impl Stream for StreamHandle {
    type Item = Result<JsonValue, ExecutionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = Pin::into_inner(self);
        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(value))) => Poll::Ready(Some(Ok(value))),
            Poll::Ready(Some(Err(err))) => {
                let alias = this.alias.clone();
                this.finalize(true);
                Poll::Ready(Some(Err(ExecutionError::NodeFailed { alias, source: err })))
            }
            Poll::Ready(None) => {
                this.finalize(false);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.finalize(true);
    }
}

impl StreamingCapture {
    fn into_handle(self, instance: FlowInstance) -> StreamHandle {
        StreamHandle::new(
            self.alias,
            self.stream,
            self.cancellation,
            self.permit,
            instance,
        )
    }
}

struct CapturedOutput {
    alias: String,
    result: NodeResult<CapturedPayload>,
    permit: Option<OwnedSemaphorePermit>,
    queue_tracker: Option<Arc<QueueDepthTracker>>,
}

#[derive(Debug)]
enum FlowMessage {
    Data {
        payload: JsonValue,
        permit: Option<OwnedSemaphorePermit>,
        queue_tracker: Option<Arc<QueueDepthTracker>>,
    },
    Spilled {
        key: String,
        storage: Arc<BlobSpill>,
        permit: Option<OwnedSemaphorePermit>,
        queue_tracker: Option<Arc<QueueDepthTracker>>,
    },
}

#[allow(clippy::too_many_arguments)]
#[instrument(
    level = "trace",
    skip_all,
    fields(node = %alias)
)]
async fn run_node(
    alias: String,
    capture_alias: String,
    handler: Arc<dyn NodeHandler>,
    mut inputs: Vec<mpsc::Receiver<FlowMessage>>,
    outputs: Vec<OutgoingSender>,
    capture: mpsc::Sender<CapturedOutput>,
    metrics: Arc<ExecutorMetrics>,
    capture_tracker: Arc<QueueDepthTracker>,
    ctx: NodeContext,
    routing: Arc<RoutingTable>,
) {
    if inputs.is_empty() {
        warn!("node `{alias}` has no inputs; dropping without execution");
        return;
    }

    let mut streams = StreamMap::new();
    for (idx, receiver) in inputs.drain(..).enumerate() {
        streams.insert(idx, ReceiverStream::new(receiver));
    }

    while !ctx.is_cancelled() {
        let cancel_token = ctx.token();
        let maybe_message = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                debug!("node `{alias}` observed cancellation");
                metrics.record_cancellation(alias.as_str(), "propagated");
                break;
            }
            message = streams.next() => message,
        };

        let (_, message) = match maybe_message {
            Some(pair) => pair,
            None => break,
        };

        let Some((mut permit, queue_tracker, payload)) = (match message {
            FlowMessage::Data {
                payload,
                permit,
                queue_tracker,
            } => Some((permit, queue_tracker, payload)),
            FlowMessage::Spilled {
                key,
                storage,
                permit,
                queue_tracker,
            } => match storage.load(&key).await {
                Ok(value) => {
                    if let Err(err) = storage.remove(&key).await {
                        warn!("failed to delete spilled payload `{key}` for node `{alias}`: {err}");
                    }
                    Some((permit, queue_tracker, value))
                }
                Err(err) => {
                    error!("node `{alias}` failed to hydrate spilled payload `{key}`: {err}");
                    metrics.record_node_error(alias.as_str(), "spill_load_failed");
                    let failure = NodeError::new(format!(
                        "failed to load spilled payload `{key}` from blob storage: {err}"
                    ));
                    let captured_permit = permit;
                    let send_result = capture
                        .send(CapturedOutput {
                            alias: alias.clone(),
                            result: Err(failure),
                            permit: captured_permit,
                            queue_tracker: queue_tracker.clone(),
                        })
                        .await;
                    if let Some(tracker) = &queue_tracker {
                        tracker.decrement();
                    }
                    if send_result.is_ok() {
                        capture_tracker.increment();
                        metrics
                            .observe_capture_backpressure(capture_alias.as_str(), Duration::ZERO);
                    }
                    metrics.record_cancellation(alias.as_str(), "spill_load_failed");
                    ctx.token().cancel();
                    None
                }
            },
        }) else {
            continue;
        };

        if let Some(tracker) = &queue_tracker {
            tracker.decrement();
        }
        let _guard = metrics.track_node(alias.as_str());

        let ctx_for_handler = ctx.clone();
        let resources = ctx_for_handler.resource_handle();
        let handler_clone = handler.clone();
        let result = context::with_resources(resources.clone(), async move {
            handler_clone.invoke(payload, &ctx_for_handler).await
        })
        .await;

        match result {
            Ok(NodeOutput::Value(value)) => {
                if alias == capture_alias {
                    let captured_permit = permit.take();
                    let start = Instant::now();
                    let send_result = capture
                        .send(CapturedOutput {
                            alias: alias.clone(),
                            result: Ok(CapturedPayload::Value(value.clone())),
                            permit: captured_permit,
                            queue_tracker: Some(capture_tracker.clone()),
                        })
                        .await;
                    if send_result.is_ok() {
                        capture_tracker.increment();
                        metrics
                            .observe_capture_backpressure(capture_alias.as_str(), start.elapsed());
                    }
                }
                if outputs.is_empty() {
                    // Leaf node; any outstanding permit will be released below.
                } else {
                    let control = routing.control_for(alias.as_str()).cloned();
                    let mut selected_target: Option<String> = None;

                    if let Some(control) = &control {
                        match control.select_target(&value) {
                            Ok(Some(target)) => {
                                selected_target = Some(target.to_string());
                            }
                            Ok(None) => {
                                selected_target = None;
                            }
                            Err(err) => {
                                error!(
                                    "node `{alias}` {} routing failed: {err}",
                                    control.kind_label()
                                );
                                metrics.record_node_error(alias.as_str(), control.failure_label());
                                let captured_permit = permit.take();
                                let send_result = capture
                                    .send(CapturedOutput {
                                        alias: alias.clone(),
                                        result: Err(err),
                                        permit: captured_permit,
                                        queue_tracker: Some(capture_tracker.clone()),
                                    })
                                    .await;
                                if send_result.is_ok() {
                                    capture_tracker.increment();
                                    metrics.observe_capture_backpressure(
                                        capture_alias.as_str(),
                                        Duration::ZERO,
                                    );
                                }
                                metrics
                                    .record_cancellation(alias.as_str(), control.failure_label());
                                ctx.token().cancel();
                                break;
                            }
                        }

                        if let Some(target) = selected_target.as_deref()
                            && !outputs.iter().any(|o| o.to == target)
                        {
                            let err = NodeError::new(format!(
                                "{} `{}` selected target `{}` but no such edge exists",
                                control.kind_label(),
                                control.surface_id(),
                                target
                            ));
                            error!(
                                "node `{alias}` {} routing failed: {err}",
                                control.kind_label()
                            );
                            metrics.record_node_error(alias.as_str(), control.failure_label());
                            let captured_permit = permit.take();
                            let send_result = capture
                                .send(CapturedOutput {
                                    alias: alias.clone(),
                                    result: Err(err),
                                    permit: captured_permit,
                                    queue_tracker: Some(capture_tracker.clone()),
                                })
                                .await;
                            if send_result.is_ok() {
                                capture_tracker.increment();
                                metrics.observe_capture_backpressure(
                                    capture_alias.as_str(),
                                    Duration::ZERO,
                                );
                            }
                            metrics.record_cancellation(alias.as_str(), control.failure_label());
                            ctx.token().cancel();
                            break;
                        }
                    }

                    let mut first_send = true;
                    for output in outputs.iter() {
                        if let Some(control) = &control
                            && control.controls_edge_to(output.to.as_str())
                            && selected_target.as_deref() != Some(output.to.as_str())
                        {
                            continue;
                        }

                        let downstream_permit = if first_send {
                            first_send = false;
                            permit.take()
                        } else {
                            None
                        };

                        if let Err(err) = output.sender.send(value.clone(), downstream_permit).await
                        {
                            debug!("downstream receiver for node `{alias}` dropped: {err}");
                        }
                    }

                    if control.is_some() && selected_target.is_none() && first_send {
                        metrics.record_cancellation(alias.as_str(), "switch_no_target");
                        ctx.token().cancel();
                        break;
                    }
                }
            }
            Ok(NodeOutput::Stream(stream)) => {
                if alias != capture_alias {
                    error!(
                        "node `{alias}` emitted a stream but capture alias is `{capture_alias}`; \
                        streaming outputs are only supported on the configured capture node"
                    );
                    metrics.record_node_error(alias.as_str(), "invalid_stream_target");
                    let captured_permit = permit.take();
                    let send_result = capture
                        .send(CapturedOutput {
                            alias: alias.clone(),
                            result: Err(NodeError::new(
                                "stream outputs are only supported on the capture node",
                            )),
                            permit: captured_permit,
                            queue_tracker: Some(capture_tracker.clone()),
                        })
                        .await;
                    if send_result.is_ok() {
                        capture_tracker.increment();
                        metrics
                            .observe_capture_backpressure(capture_alias.as_str(), Duration::ZERO);
                    }
                    metrics.record_cancellation(alias.as_str(), "invalid_stream_target");
                    ctx.token().cancel();
                    break;
                }

                let cancellation = ctx.token();
                let scoped_stream = stream.then({
                    let resources = resources.clone();
                    move |item| {
                        let resources = resources.clone();
                        async move { context::with_resources(resources, async move { item }).await }
                    }
                });
                let boxed_stream = scoped_stream
                    .take_until(cancellation.clone().cancelled_owned())
                    .boxed();
                let streaming =
                    StreamingCapture::new(alias.clone(), boxed_stream, cancellation, permit.take());
                let start = Instant::now();
                let send_result = capture
                    .send(CapturedOutput {
                        alias: alias.clone(),
                        result: Ok(CapturedPayload::Stream(streaming)),
                        permit: None,
                        queue_tracker: Some(capture_tracker.clone()),
                    })
                    .await;
                if send_result.is_ok() {
                    capture_tracker.increment();
                    metrics.observe_capture_backpressure(capture_alias.as_str(), start.elapsed());
                }
                metrics.record_stream_spawn(alias.as_str());
                break;
            }
            Ok(NodeOutput::None) => {
                trace!("node `{alias}` produced no output");
            }
            Err(err) => {
                error!("node `{alias}` failed: {err}");
                metrics.record_node_error(alias.as_str(), "handler_error");
                let captured_permit = permit.take();
                let send_result = capture
                    .send(CapturedOutput {
                        alias: alias.clone(),
                        result: Err(err),
                        permit: captured_permit,
                        queue_tracker: Some(capture_tracker.clone()),
                    })
                    .await;
                if send_result.is_ok() {
                    capture_tracker.increment();
                    metrics.observe_capture_backpressure(capture_alias.as_str(), Duration::ZERO);
                }
                metrics.record_cancellation(alias.as_str(), "node_error");
                ctx.token().cancel();
                break;
            }
        }
        if let Some(permit) = permit {
            drop(permit);
        }
    }

    debug!("node `{alias}` exiting");
}

/// Errors surfaced during flow execution.
#[derive(Debug, Error)]
pub enum ExecutionError {
    /// Unknown trigger alias or missing input queue.
    #[error("trigger alias `{alias}` is not part of the workflow")]
    UnknownTrigger { alias: String },
    /// Capture alias not found.
    #[error("capture alias `{alias}` is not part of the workflow")]
    UnknownCapture { alias: String },
    /// Execution cancelled before completion.
    #[error("execution cancelled")]
    Cancelled,
    /// Deadline exceeded.
    #[error("deadline exceeded after {elapsed:?}")]
    DeadlineExceeded { elapsed: Duration },
    /// Flow finished without producing a captured output.
    #[error("capture `{alias}` did not yield an output")]
    MissingOutput { alias: String },
    /// Node handler failed.
    #[error("node `{alias}` failed: {source}")]
    NodeFailed {
        alias: String,
        #[source]
        source: NodeError,
    },
    /// Handler not registered for node identifier.
    #[error("no handler registered for node `{identifier}`")]
    UnregisteredNode { identifier: String },
    /// Required capability bindings are missing from the host resource bag.
    #[error("missing required capabilities: {hints:?}")]
    MissingCapabilities { hints: Vec<String> },
    /// Control surface is present but not supported by this runtime.
    #[error("control surface `{id}` ({kind}) is not supported by this runtime")]
    UnsupportedControlSurface { id: String, kind: String },
    /// Control surface config is malformed.
    #[error("invalid control surface `{id}` ({kind}): {reason}")]
    InvalidControlSurface {
        id: String,
        kind: String,
        reason: String,
    },
    /// Spill storage could not be initialised.
    #[error("failed to configure spill storage: {0}")]
    SpillSetup(anyhow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use capabilities::{
        ResourceBag, context,
        kv::{KeyValue, MemoryKv},
    };
    use dag_core::prelude::*;
    use futures::StreamExt;
    use kernel_plan::validate;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
    use proptest::prelude::*;
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;
    use tokio::runtime::Builder as RuntimeBuilder;
    use tokio::time::{sleep, timeout};

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

    #[tokio::test]
    async fn emits_executor_metrics_on_success() {
        reset_metrics();

        const FORWARD_SPEC: NodeSpec = NodeSpec::inline(
            "tests::forward",
            "Forward",
            SchemaSpec::Opaque,
            SchemaSpec::Opaque,
            Effects::Pure,
            Determinism::Strict,
            None,
        );

        let mut builder = FlowBuilder::new("metrics_flow", Version::new(0, 1, 0), Profile::Dev);
        let trigger = builder.add_node("trigger", &FORWARD_SPEC).unwrap();
        let capture = builder.add_node("capture", &FORWARD_SPEC).unwrap();
        builder.connect(&trigger, &capture);
        let flow_ir = builder.build();
        let validated = validate(&flow_ir).expect("flow should validate");

        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::forward",
                |input: JsonValue| async move { Ok(input) },
            )
            .unwrap();
        let executor = FlowExecutor::new(Arc::new(registry));

        let payload = json!({"ping": "pong"});
        let result = executor
            .run_once(&validated, "trigger", payload.clone(), "capture", None)
            .await
            .expect("run succeeds");
        match result {
            ExecutionResult::Value(value) => assert_eq!(value, payload),
            ExecutionResult::Stream(_) => panic!("expected non-streaming result"),
        }

        let mut saw_latency = false;
        let mut saw_latency_samples = false;
        let mut saw_queue = false;

        for _ in 0..10 {
            saw_latency = false;
            saw_latency_samples = false;
            saw_queue = false;

            let snapshot = metrics_snapshotter().snapshot().into_vec();
            for (key, _unit, _desc, value) in snapshot.into_iter() {
                let name = key.key().name();
                match (name, value) {
                    ("lattice.executor.node_latency_ms", DebugValue::Histogram(vals)) => {
                        saw_latency = true;
                        saw_latency_samples = !vals.is_empty();
                    }
                    ("lattice.executor.queue_depth", DebugValue::Gauge(_)) => {
                        saw_queue = true;
                    }
                    _ => {}
                }
            }

            if saw_latency && saw_latency_samples && saw_queue {
                break;
            }

            sleep(Duration::from_millis(10)).await;
        }

        assert!(saw_latency, "expected node latency histogram to be emitted");
        assert!(
            saw_latency_samples,
            "node latency histogram recorded no samples"
        );
        assert!(saw_queue, "expected queue depth gauge to be emitted");
    }

    #[test]
    fn flow_instance_respects_capture_capacity() {
        let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
            cases: 32,
            ..ProptestConfig::default()
        });
        let strategy = (1usize..=4, 1usize..=5);

        runner
            .run(&strategy, |(capacity, cycles)| {
                let runtime = RuntimeBuilder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("tokio runtime");

                runtime.block_on(async move {
                    let mut registry = NodeRegistry::new();
                    registry
                        .register_fn(
                            "tests::forward",
                            |input: JsonValue| async move { Ok(input) },
                        )
                        .unwrap();

                    let mut builder =
                        FlowBuilder::new("prop_backpressure", Version::new(0, 0, 1), Profile::Dev);
                    let trigger = builder
                        .add_node(
                            "trigger",
                            &NodeSpec::inline(
                                "tests::forward",
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
                            "capture",
                            &NodeSpec::inline(
                                "tests::forward",
                                "Capture",
                                SchemaSpec::Opaque,
                                SchemaSpec::Opaque,
                                Effects::Pure,
                                Determinism::Strict,
                                None,
                            ),
                        )
                        .unwrap();
                    builder.connect(&trigger, &capture);

                    let flow = builder.build();
                    let validated = validate(&flow).expect("flow should validate");

                    let executor =
                        FlowExecutor::new(Arc::new(registry)).with_capture_capacity(capacity);
                    let mut instance = executor
                        .instantiate(&validated, "capture")
                        .expect("instance creation succeeds");

                    for idx in 0..capacity {
                        tokio::time::timeout(
                            std::time::Duration::from_millis(50),
                            instance.send("trigger", json!({ "prefill": idx })),
                        )
                        .await
                        .expect("prefill send completes")
                        .expect("prefill send succeeds");
                    }

                    prop_assert_eq!(
                        instance.available_permits(),
                        0,
                        "all permits should be consumed after prefill"
                    );

                    for cycle in 0..cycles {
                        let payload = json!({ "cycle": cycle });
                        let blocked = tokio::time::timeout(
                            std::time::Duration::from_millis(20),
                            instance.send("trigger", payload.clone()),
                        )
                        .await;
                        prop_assert!(
                            blocked.is_err(),
                            "send should block when permits are exhausted"
                        );

                        let drained = tokio::time::timeout(
                            std::time::Duration::from_millis(100),
                            instance.next(),
                        )
                        .await
                        .expect("drain future completes");
                        let result = drained.expect("capture channel open")?;
                        match result {
                            CaptureResult::Value(_) => {}
                            CaptureResult::Stream(_) => {
                                prop_assert!(false, "unexpected stream capture during drain cycle");
                            }
                        }

                        tokio::time::timeout(
                            std::time::Duration::from_millis(100),
                            instance.send("trigger", payload),
                        )
                        .await
                        .expect("send should succeed after draining one result")
                        .expect("send after drain succeeds");

                        prop_assert_eq!(
                            instance.available_permits(),
                            0,
                            "permits should be fully leased after refill"
                        );
                    }

                    for _ in 0..capacity {
                        let drained = tokio::time::timeout(
                            std::time::Duration::from_millis(100),
                            instance.next(),
                        )
                        .await
                        .expect("tail drain future completes");
                        let result = drained.expect("capture channel open while draining tail")?;
                        match result {
                            CaptureResult::Value(_) => {}
                            CaptureResult::Stream(_) => {
                                prop_assert!(
                                    false,
                                    "unexpected stream capture while draining tail"
                                );
                            }
                        }
                    }

                    prop_assert_eq!(
                        instance.available_permits(),
                        capacity,
                        "all permits should be restored after final drain"
                    );

                    instance.cancel();
                    instance
                        .shutdown()
                        .await
                        .expect("instance shutdown succeeds");

                    Ok(())
                })
            })
            .unwrap();
    }

    #[tokio::test]
    async fn backpressure_applies_when_capture_is_full() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::sink", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder = FlowBuilder::new("capture_flow", Version::new(1, 0, 0), Profile::Dev);
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
                "sink",
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
        for edge in &mut flow.edges {
            edge.buffer.max_items = Some(1);
        }
        let validated = validate(&flow).expect("flow should validate");

        let executor = FlowExecutor::new(Arc::new(registry)).with_capture_capacity(1);
        let mut instance = executor.instantiate(&validated, "sink").unwrap();

        instance
            .send("trigger", JsonValue::from(1))
            .await
            .expect("first send");

        match timeout(
            Duration::from_millis(50),
            instance.send("trigger", JsonValue::from(2)),
        )
        .await
        {
            Err(_) => {} // expected timeout indicates capture queue saturation
            Ok(result) => panic!("send should block until capture drains, got {result:?}"),
        }

        let first = match instance.next().await {
            Some(Ok(CaptureResult::Value(value))) => value,
            _ => panic!("unexpected capture result"),
        };
        assert_eq!(first, JsonValue::from(1));

        instance
            .send("trigger", JsonValue::from(2))
            .await
            .expect("second send completes once capture drained");
        let second = match instance.next().await {
            Some(Ok(CaptureResult::Value(value))) => value,
            _ => panic!("unexpected capture result"),
        };
        assert_eq!(second, JsonValue::from(2));

        instance.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn resource_bag_available_during_execution() {
        let kv_store = Arc::new(MemoryKv::new());

        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::worker", |val: JsonValue| async move {
                context::with_current_async(|resources| async move {
                    let kv = resources.kv().expect("kv capability available");
                    kv.put("resource-test", b"value", None)
                        .await
                        .expect("kv put succeeds");
                })
                .await
                .expect("resource context scoped during node execution");
                Ok(val)
            })
            .unwrap();
        registry
            .register_fn("tests::capture", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder = FlowBuilder::new("resource_flow", Version::new(1, 0, 0), Profile::Dev);
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
        let worker = builder
            .add_node(
                "worker",
                &NodeSpec::inline(
                    "tests::worker",
                    "Worker",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Effectful,
                    Determinism::BestEffort,
                    None,
                ),
            )
            .unwrap();
        let capture = builder
            .add_node(
                "capture",
                &NodeSpec::inline(
                    "tests::capture",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &worker);
        builder.connect(&worker, &capture);

        let mut flow = builder.build();
        if let Some(node) = flow.nodes.iter_mut().find(|node| node.alias == "worker") {
            node.idempotency.key = Some("resource-test".to_string());
        }
        let validated = validate(&flow).expect("flow validates");

        let executor = FlowExecutor::new(Arc::new(registry))
            .with_resource_bag(ResourceBag::new().with_kv(kv_store.clone()));

        let result = executor
            .run_once(&validated, "trigger", JsonValue::Null, "capture", None)
            .await
            .expect("execution succeeds");

        assert!(matches!(result, ExecutionResult::Value(JsonValue::Null)));
        assert_eq!(
            kv_store
                .get("resource-test")
                .await
                .expect("kv read after execution"),
            Some(b"value".to_vec())
        );
    }

    #[tokio::test]
    async fn spill_to_blob_handles_queue_overflow() {
        reset_metrics();

        const TRIGGER_SPEC: NodeSpec = NodeSpec::inline(
            "tests::trigger_spill",
            "Trigger",
            SchemaSpec::Opaque,
            SchemaSpec::Opaque,
            Effects::Pure,
            Determinism::Strict,
            None,
        );
        const WORKER_EFFECT_HINTS: &[&str] = &["resource::blob::write"];
        const WORKER_SPEC: NodeSpec = NodeSpec::inline_with_hints(
            "tests::slow_worker",
            "SlowWorker",
            SchemaSpec::Opaque,
            SchemaSpec::Opaque,
            Effects::Effectful,
            Determinism::BestEffort,
            None,
            &[],
            WORKER_EFFECT_HINTS,
        );
        const CAPTURE_SPEC: NodeSpec = NodeSpec::inline(
            "tests::capture_spill",
            "Capture",
            SchemaSpec::Opaque,
            SchemaSpec::Opaque,
            Effects::Pure,
            Determinism::Strict,
            None,
        );

        let mut builder = FlowBuilder::new("spill_flow", Version::new(1, 1, 0), Profile::Dev);
        let trigger = builder.add_node("trigger", &TRIGGER_SPEC).unwrap();
        let worker = builder.add_node("worker", &WORKER_SPEC).unwrap();
        let capture = builder.add_node("capture", &CAPTURE_SPEC).unwrap();
        builder.connect(&trigger, &worker);
        builder.connect(&worker, &capture);

        let mut flow_ir = builder.build();
        for edge in &mut flow_ir.edges {
            if edge.from == "trigger" && edge.to == "worker" {
                edge.buffer.max_items = Some(1);
                edge.buffer.spill_tier = Some("local".to_string());
            }
        }
        if let Some(node) = flow_ir.nodes.iter_mut().find(|node| node.alias == "worker") {
            node.idempotency.key = Some("spill-test".to_string());
        }
        let validated = validate(&flow_ir).expect("flow should validate");

        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger_spill", |input: JsonValue| async move {
                Ok(input)
            })
            .unwrap();
        registry
            .register_fn("tests::slow_worker", |input: JsonValue| async move {
                sleep(Duration::from_millis(100)).await;
                Ok(input)
            })
            .unwrap();
        registry
            .register_fn("tests::capture_spill", |input: JsonValue| async move {
                Ok(input)
            })
            .unwrap();

        let executor = FlowExecutor::new(Arc::new(registry));
        let mut instance = executor.instantiate(&validated, "capture").unwrap();

        let first = json!({"seq": 1});
        instance
            .send("trigger", first.clone())
            .await
            .expect("first payload enqueued");

        let second = json!({"seq": 2});
        timeout(
            Duration::from_millis(50),
            instance.send("trigger", second.clone()),
        )
        .await
        .expect("spill prevents sender from blocking")
        .expect("second payload enqueued via spill");

        let first_out = match instance.next().await {
            Some(Ok(CaptureResult::Value(value))) => value,
            Some(Ok(CaptureResult::Stream(_))) => {
                panic!("unexpected streaming capture result for first payload")
            }
            Some(Err(err)) => panic!("capture failed: {err}"),
            None => panic!("capture channel closed unexpectedly"),
        };
        assert_eq!(first_out, first);

        let second_out = match instance.next().await {
            Some(Ok(CaptureResult::Value(value))) => value,
            Some(Ok(CaptureResult::Stream(_))) => {
                panic!("unexpected streaming capture result for second payload")
            }
            Some(Err(err)) => panic!("capture failed: {err}"),
            None => panic!("capture channel closed unexpectedly"),
        };
        assert_eq!(second_out, second);

        instance.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_propagates_on_error() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::fails", |_val: JsonValue| async move {
                Err::<JsonValue, NodeError>(NodeError::new("boom"))
            })
            .unwrap();
        registry
            .register_fn("tests::sink", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder = FlowBuilder::new("cancel", Version::new(1, 0, 0), Profile::Dev);
        let trigger = builder
            .add_node(
                "trigger",
                &NodeSpec::inline(
                    "tests::trigger",
                    "Trig",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let failing = builder
            .add_node(
                "fails",
                &NodeSpec::inline(
                    "tests::fails",
                    "Fail",
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
                "sink",
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
        builder.connect(&trigger, &failing);
        builder.connect(&failing, &sink);

        let flow = builder.build();
        let validated = validate(&flow).expect("flow should validate");

        let executor = FlowExecutor::new(Arc::new(registry));
        let mut instance = executor.instantiate(&validated, "sink").unwrap();
        instance
            .send("trigger", JsonValue::from(1))
            .await
            .expect("send");

        match instance.next().await {
            Some(Err(ExecutionError::NodeFailed { alias, .. })) => {
                assert_eq!(alias, "fails");
            }
            _ => panic!("unexpected result"),
        }

        instance.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn switch_routes_only_selected_branch() {
        let branch_a_count = Arc::new(AtomicUsize::new(0));
        let branch_b_count = Arc::new(AtomicUsize::new(0));

        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::route", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        {
            let branch_a_count = Arc::clone(&branch_a_count);
            registry
                .register_fn("tests::branch_a", move |val: JsonValue| {
                    let branch_a_count = Arc::clone(&branch_a_count);
                    async move {
                        branch_a_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(val)
                    }
                })
                .unwrap();
        }

        {
            let branch_b_count = Arc::clone(&branch_b_count);
            registry
                .register_fn("tests::branch_b", move |val: JsonValue| {
                    let branch_b_count = Arc::clone(&branch_b_count);
                    async move {
                        branch_b_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(val)
                    }
                })
                .unwrap();
        }

        registry
            .register_fn("tests::capture", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder = FlowBuilder::new("switch_exec", Version::new(1, 0, 0), Profile::Dev);
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
        let route = builder
            .add_node(
                "route",
                &NodeSpec::inline(
                    "tests::route",
                    "Route",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let a = builder
            .add_node(
                "a",
                &NodeSpec::inline(
                    "tests::branch_a",
                    "A",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let b = builder
            .add_node(
                "b",
                &NodeSpec::inline(
                    "tests::branch_b",
                    "B",
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
                "capture",
                &NodeSpec::inline(
                    "tests::capture",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();

        builder.connect(&trigger, &route);
        builder.connect(&route, &a);
        builder.connect(&route, &b);
        builder.connect(&a, &capture);
        builder.connect(&b, &capture);

        let mut flow = builder.build();
        flow.control_surfaces.push(dag_core::ControlSurfaceIR {
            id: "switch:route:0".to_string(),
            kind: dag_core::ControlSurfaceKind::Switch,
            targets: vec!["route".into(), "a".into(), "b".into()],
            config: json!({
                "v": 1,
                "source": "route",
                "selector_pointer": "/type",
                "cases": { "a": "a", "b": "b" },
            }),
        });

        let validated = validate(&flow).expect("flow should validate");
        let executor = FlowExecutor::new(Arc::new(registry));

        let payload = json!({"type": "a", "value": 123});
        let result = executor
            .run_once(&validated, "trigger", payload.clone(), "capture", None)
            .await
            .expect("execution succeeds");

        match result {
            ExecutionResult::Value(value) => assert_eq!(value, payload),
            ExecutionResult::Stream(_) => panic!("unexpected stream"),
        }

        assert_eq!(branch_a_count.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(branch_b_count.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn switch_no_match_routes_to_default_branch() {
        let branch_a_count = Arc::new(AtomicUsize::new(0));
        let branch_b_count = Arc::new(AtomicUsize::new(0));

        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::route", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        {
            let branch_a_count = Arc::clone(&branch_a_count);
            registry
                .register_fn("tests::branch_a", move |val: JsonValue| {
                    let branch_a_count = Arc::clone(&branch_a_count);
                    async move {
                        branch_a_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(val)
                    }
                })
                .unwrap();
        }

        {
            let branch_b_count = Arc::clone(&branch_b_count);
            registry
                .register_fn("tests::branch_b", move |val: JsonValue| {
                    let branch_b_count = Arc::clone(&branch_b_count);
                    async move {
                        branch_b_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(val)
                    }
                })
                .unwrap();
        }

        registry
            .register_fn("tests::capture", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder = FlowBuilder::new("switch_default", Version::new(1, 0, 0), Profile::Dev);
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
        let route = builder
            .add_node(
                "route",
                &NodeSpec::inline(
                    "tests::route",
                    "Route",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let a = builder
            .add_node(
                "a",
                &NodeSpec::inline(
                    "tests::branch_a",
                    "A",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let b = builder
            .add_node(
                "b",
                &NodeSpec::inline(
                    "tests::branch_b",
                    "B",
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
                "capture",
                &NodeSpec::inline(
                    "tests::capture",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();

        builder.connect(&trigger, &route);
        builder.connect(&route, &a);
        builder.connect(&route, &b);
        builder.connect(&a, &capture);
        builder.connect(&b, &capture);

        let mut flow = builder.build();
        flow.control_surfaces.push(dag_core::ControlSurfaceIR {
            id: "switch:route:0".to_string(),
            kind: dag_core::ControlSurfaceKind::Switch,
            targets: vec!["route".into(), "a".into(), "b".into()],
            config: json!({
                "v": 1,
                "source": "route",
                "selector_pointer": "/type",
                "cases": { "a": "a", "b": "b" },
                "default": "a",
            }),
        });

        let validated = validate(&flow).expect("flow should validate");
        let executor = FlowExecutor::new(Arc::new(registry));

        let payload = json!({"type": "nope", "value": 123});
        let result = executor
            .run_once(&validated, "trigger", payload.clone(), "capture", None)
            .await
            .expect("execution succeeds");

        match result {
            ExecutionResult::Value(value) => assert_eq!(value, payload),
            ExecutionResult::Stream(_) => panic!("unexpected stream"),
        }

        assert_eq!(branch_a_count.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(branch_b_count.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn switch_no_match_without_default_yields_missing_output() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::route", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::branch", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::capture", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder = FlowBuilder::new("switch_none", Version::new(1, 0, 0), Profile::Dev);
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
        let route = builder
            .add_node(
                "route",
                &NodeSpec::inline(
                    "tests::route",
                    "Route",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let a = builder
            .add_node(
                "a",
                &NodeSpec::inline(
                    "tests::branch",
                    "A",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let b = builder
            .add_node(
                "b",
                &NodeSpec::inline(
                    "tests::branch",
                    "B",
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
                "capture",
                &NodeSpec::inline(
                    "tests::capture",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();

        builder.connect(&trigger, &route);
        builder.connect(&route, &a);
        builder.connect(&route, &b);
        builder.connect(&a, &capture);
        builder.connect(&b, &capture);

        let mut flow = builder.build();
        flow.control_surfaces.push(dag_core::ControlSurfaceIR {
            id: "switch:route:0".to_string(),
            kind: dag_core::ControlSurfaceKind::Switch,
            targets: vec!["route".into(), "a".into(), "b".into()],
            config: json!({
                "v": 1,
                "source": "route",
                "selector_pointer": "/type",
                "cases": { "a": "a", "b": "b" },
            }),
        });

        let validated = validate(&flow).expect("flow should validate");
        let executor = FlowExecutor::new(Arc::new(registry));

        let payload = json!({"type": "nope", "value": 123});
        let err = executor
            .run_once(&validated, "trigger", payload, "capture", None)
            .await
            .err()
            .expect("expected missing output");

        assert!(matches!(err, ExecutionError::MissingOutput { .. }));
    }

    #[tokio::test]
    async fn switch_selector_pointer_missing_fails_source_node() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::route", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::branch", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::capture", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder =
            FlowBuilder::new("switch_ptr_missing", Version::new(1, 0, 0), Profile::Dev);
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
        let route = builder
            .add_node(
                "route",
                &NodeSpec::inline(
                    "tests::route",
                    "Route",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let a = builder
            .add_node(
                "a",
                &NodeSpec::inline(
                    "tests::branch",
                    "A",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let b = builder
            .add_node(
                "b",
                &NodeSpec::inline(
                    "tests::branch",
                    "B",
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
                "capture",
                &NodeSpec::inline(
                    "tests::capture",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();

        builder.connect(&trigger, &route);
        builder.connect(&route, &a);
        builder.connect(&route, &b);
        builder.connect(&a, &capture);
        builder.connect(&b, &capture);

        let mut flow = builder.build();
        flow.control_surfaces.push(dag_core::ControlSurfaceIR {
            id: "switch:route:0".to_string(),
            kind: dag_core::ControlSurfaceKind::Switch,
            targets: vec!["route".into(), "a".into(), "b".into()],
            config: json!({
                "v": 1,
                "source": "route",
                "selector_pointer": "/type",
                "cases": { "a": "a", "b": "b" },
            }),
        });

        let validated = validate(&flow).expect("flow should validate");
        let executor = FlowExecutor::new(Arc::new(registry));

        let payload = json!({"value": 123});
        let err = executor
            .run_once(&validated, "trigger", payload, "capture", None)
            .await
            .err()
            .expect("expected error");

        assert!(matches!(err, ExecutionError::NodeFailed { alias, .. } if alias == "route"));
    }

    #[tokio::test]
    async fn switch_selector_pointer_wrong_type_fails_source_node() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::route", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::branch", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::capture", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder =
            FlowBuilder::new("switch_ptr_wrong_type", Version::new(1, 0, 0), Profile::Dev);
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
        let route = builder
            .add_node(
                "route",
                &NodeSpec::inline(
                    "tests::route",
                    "Route",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let a = builder
            .add_node(
                "a",
                &NodeSpec::inline(
                    "tests::branch",
                    "A",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let b = builder
            .add_node(
                "b",
                &NodeSpec::inline(
                    "tests::branch",
                    "B",
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
                "capture",
                &NodeSpec::inline(
                    "tests::capture",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();

        builder.connect(&trigger, &route);
        builder.connect(&route, &a);
        builder.connect(&route, &b);
        builder.connect(&a, &capture);
        builder.connect(&b, &capture);

        let mut flow = builder.build();
        flow.control_surfaces.push(dag_core::ControlSurfaceIR {
            id: "switch:route:0".to_string(),
            kind: dag_core::ControlSurfaceKind::Switch,
            targets: vec!["route".into(), "a".into(), "b".into()],
            config: json!({
                "v": 1,
                "source": "route",
                "selector_pointer": "/type",
                "cases": { "a": "a", "b": "b" },
            }),
        });

        let validated = validate(&flow).expect("flow should validate");
        let executor = FlowExecutor::new(Arc::new(registry));

        let payload = json!({"type": true, "value": 123});
        let err = executor
            .run_once(&validated, "trigger", payload, "capture", None)
            .await
            .err()
            .expect("expected error");

        assert!(matches!(err, ExecutionError::NodeFailed { alias, .. } if alias == "route"));
    }

    #[tokio::test]
    async fn switch_does_not_gate_edges_outside_targets() {
        let branch_a_count = Arc::new(AtomicUsize::new(0));
        let branch_b_count = Arc::new(AtomicUsize::new(0));
        let log_count = Arc::new(AtomicUsize::new(0));

        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::route", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        {
            let branch_a_count = Arc::clone(&branch_a_count);
            registry
                .register_fn("tests::branch_a", move |val: JsonValue| {
                    let branch_a_count = Arc::clone(&branch_a_count);
                    async move {
                        branch_a_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(val)
                    }
                })
                .unwrap();
        }

        {
            let branch_b_count = Arc::clone(&branch_b_count);
            registry
                .register_fn("tests::branch_b", move |val: JsonValue| {
                    let branch_b_count = Arc::clone(&branch_b_count);
                    async move {
                        branch_b_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(val)
                    }
                })
                .unwrap();
        }

        {
            let log_count = Arc::clone(&log_count);
            registry
                .register_fn("tests::log", move |val: JsonValue| {
                    let log_count = Arc::clone(&log_count);
                    async move {
                        log_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(val)
                    }
                })
                .unwrap();
        }

        registry
            .register_fn("tests::capture", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder =
            FlowBuilder::new("switch_extra_edge", Version::new(1, 0, 0), Profile::Dev);
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
        let route = builder
            .add_node(
                "route",
                &NodeSpec::inline(
                    "tests::route",
                    "Route",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let a = builder
            .add_node(
                "a",
                &NodeSpec::inline(
                    "tests::branch_a",
                    "A",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let b = builder
            .add_node(
                "b",
                &NodeSpec::inline(
                    "tests::branch_b",
                    "B",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let log = builder
            .add_node(
                "log",
                &NodeSpec::inline(
                    "tests::log",
                    "Log",
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
                "capture",
                &NodeSpec::inline(
                    "tests::capture",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();

        builder.connect(&trigger, &route);
        builder.connect(&route, &a);
        builder.connect(&route, &b);
        builder.connect(&route, &log);
        builder.connect(&a, &capture);
        builder.connect(&b, &capture);

        let mut flow = builder.build();
        flow.control_surfaces.push(dag_core::ControlSurfaceIR {
            id: "switch:route:0".to_string(),
            kind: dag_core::ControlSurfaceKind::Switch,
            targets: vec!["route".into(), "a".into(), "b".into()],
            config: json!({
                "v": 1,
                "source": "route",
                "selector_pointer": "/type",
                "cases": { "a": "a", "b": "b" },
            }),
        });

        let validated = validate(&flow).expect("flow should validate");
        let executor = FlowExecutor::new(Arc::new(registry));

        let payload = json!({"type": "a", "value": 123});
        let mut instance = executor.instantiate(&validated, "capture").unwrap();
        instance
            .send("trigger", payload.clone())
            .await
            .expect("send");

        match instance.next().await {
            Some(Ok(CaptureResult::Value(value))) => assert_eq!(value, payload),
            _ => panic!("unexpected capture result"),
        }

        timeout(Duration::from_secs(1), async {
            while log_count.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("log node should execute");

        instance.shutdown().await.unwrap();

        assert_eq!(branch_a_count.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(branch_b_count.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(log_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn if_routes_then_branch_when_true() {
        let then_count = Arc::new(AtomicUsize::new(0));
        let else_count = Arc::new(AtomicUsize::new(0));

        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::route", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        {
            let then_count = Arc::clone(&then_count);
            registry
                .register_fn("tests::then", move |val: JsonValue| {
                    let then_count = Arc::clone(&then_count);
                    async move {
                        then_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(val)
                    }
                })
                .unwrap();
        }

        {
            let else_count = Arc::clone(&else_count);
            registry
                .register_fn("tests::else", move |val: JsonValue| {
                    let else_count = Arc::clone(&else_count);
                    async move {
                        else_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(val)
                    }
                })
                .unwrap();
        }

        registry
            .register_fn("tests::capture", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder = FlowBuilder::new("if_then", Version::new(1, 0, 0), Profile::Dev);
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
        let route = builder
            .add_node(
                "route",
                &NodeSpec::inline(
                    "tests::route",
                    "Route",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let then_branch = builder
            .add_node(
                "then",
                &NodeSpec::inline(
                    "tests::then",
                    "Then",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let else_branch = builder
            .add_node(
                "else",
                &NodeSpec::inline(
                    "tests::else",
                    "Else",
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
                "capture",
                &NodeSpec::inline(
                    "tests::capture",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();

        builder.connect(&trigger, &route);
        builder.connect(&route, &then_branch);
        builder.connect(&route, &else_branch);
        builder.connect(&then_branch, &capture);
        builder.connect(&else_branch, &capture);

        let mut flow = builder.build();
        flow.control_surfaces.push(dag_core::ControlSurfaceIR {
            id: "if:route:0".to_string(),
            kind: dag_core::ControlSurfaceKind::If,
            targets: vec!["route".into(), "then".into(), "else".into()],
            config: json!({
                "v": 1,
                "source": "route",
                "selector_pointer": "/ok",
                "then": "then",
                "else": "else",
            }),
        });

        let validated = validate(&flow).expect("flow should validate");
        let executor = FlowExecutor::new(Arc::new(registry));

        let payload = json!({"ok": true, "value": 123});
        let result = executor
            .run_once(&validated, "trigger", payload.clone(), "capture", None)
            .await
            .expect("execution succeeds");

        match result {
            ExecutionResult::Value(value) => assert_eq!(value, payload),
            ExecutionResult::Stream(_) => panic!("unexpected stream"),
        }

        assert_eq!(then_count.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(else_count.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn if_routes_else_branch_when_false() {
        let then_count = Arc::new(AtomicUsize::new(0));
        let else_count = Arc::new(AtomicUsize::new(0));

        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::route", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        {
            let then_count = Arc::clone(&then_count);
            registry
                .register_fn("tests::then", move |val: JsonValue| {
                    let then_count = Arc::clone(&then_count);
                    async move {
                        then_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(val)
                    }
                })
                .unwrap();
        }

        {
            let else_count = Arc::clone(&else_count);
            registry
                .register_fn("tests::else", move |val: JsonValue| {
                    let else_count = Arc::clone(&else_count);
                    async move {
                        else_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(val)
                    }
                })
                .unwrap();
        }

        registry
            .register_fn("tests::capture", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder = FlowBuilder::new("if_else", Version::new(1, 0, 0), Profile::Dev);
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
        let route = builder
            .add_node(
                "route",
                &NodeSpec::inline(
                    "tests::route",
                    "Route",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let then_branch = builder
            .add_node(
                "then",
                &NodeSpec::inline(
                    "tests::then",
                    "Then",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let else_branch = builder
            .add_node(
                "else",
                &NodeSpec::inline(
                    "tests::else",
                    "Else",
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
                "capture",
                &NodeSpec::inline(
                    "tests::capture",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();

        builder.connect(&trigger, &route);
        builder.connect(&route, &then_branch);
        builder.connect(&route, &else_branch);
        builder.connect(&then_branch, &capture);
        builder.connect(&else_branch, &capture);

        let mut flow = builder.build();
        flow.control_surfaces.push(dag_core::ControlSurfaceIR {
            id: "if:route:0".to_string(),
            kind: dag_core::ControlSurfaceKind::If,
            targets: vec!["route".into(), "then".into(), "else".into()],
            config: json!({
                "v": 1,
                "source": "route",
                "selector_pointer": "/ok",
                "then": "then",
                "else": "else",
            }),
        });

        let validated = validate(&flow).expect("flow should validate");
        let executor = FlowExecutor::new(Arc::new(registry));

        let payload = json!({"ok": false, "value": 123});
        let result = executor
            .run_once(&validated, "trigger", payload.clone(), "capture", None)
            .await
            .expect("execution succeeds");

        match result {
            ExecutionResult::Value(value) => assert_eq!(value, payload),
            ExecutionResult::Stream(_) => panic!("unexpected stream"),
        }

        assert_eq!(then_count.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(else_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn if_selector_pointer_missing_fails_source_node() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::route", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::then", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::else", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::capture", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder = FlowBuilder::new("if_ptr_missing", Version::new(1, 0, 0), Profile::Dev);
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
        let route = builder
            .add_node(
                "route",
                &NodeSpec::inline(
                    "tests::route",
                    "Route",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let then_branch = builder
            .add_node(
                "then",
                &NodeSpec::inline(
                    "tests::then",
                    "Then",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let else_branch = builder
            .add_node(
                "else",
                &NodeSpec::inline(
                    "tests::else",
                    "Else",
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
                "capture",
                &NodeSpec::inline(
                    "tests::capture",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();

        builder.connect(&trigger, &route);
        builder.connect(&route, &then_branch);
        builder.connect(&route, &else_branch);
        builder.connect(&then_branch, &capture);
        builder.connect(&else_branch, &capture);

        let mut flow = builder.build();
        flow.control_surfaces.push(dag_core::ControlSurfaceIR {
            id: "if:route:0".to_string(),
            kind: dag_core::ControlSurfaceKind::If,
            targets: vec!["route".into(), "then".into(), "else".into()],
            config: json!({
                "v": 1,
                "source": "route",
                "selector_pointer": "/ok",
                "then": "then",
                "else": "else",
            }),
        });

        let validated = validate(&flow).expect("flow should validate");
        let executor = FlowExecutor::new(Arc::new(registry));

        let payload = json!({"value": 123});
        let err = executor
            .run_once(&validated, "trigger", payload, "capture", None)
            .await
            .err()
            .expect("expected error");

        assert!(matches!(err, ExecutionError::NodeFailed { alias, .. } if alias == "route"));
    }

    #[tokio::test]
    async fn if_selector_pointer_wrong_type_fails_source_node() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::route", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::then", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::else", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::capture", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder =
            FlowBuilder::new("if_ptr_wrong_type", Version::new(1, 0, 0), Profile::Dev);
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
        let route = builder
            .add_node(
                "route",
                &NodeSpec::inline(
                    "tests::route",
                    "Route",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let then_branch = builder
            .add_node(
                "then",
                &NodeSpec::inline(
                    "tests::then",
                    "Then",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let else_branch = builder
            .add_node(
                "else",
                &NodeSpec::inline(
                    "tests::else",
                    "Else",
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
                "capture",
                &NodeSpec::inline(
                    "tests::capture",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();

        builder.connect(&trigger, &route);
        builder.connect(&route, &then_branch);
        builder.connect(&route, &else_branch);
        builder.connect(&then_branch, &capture);
        builder.connect(&else_branch, &capture);

        let mut flow = builder.build();
        flow.control_surfaces.push(dag_core::ControlSurfaceIR {
            id: "if:route:0".to_string(),
            kind: dag_core::ControlSurfaceKind::If,
            targets: vec!["route".into(), "then".into(), "else".into()],
            config: json!({
                "v": 1,
                "source": "route",
                "selector_pointer": "/ok",
                "then": "then",
                "else": "else",
            }),
        });

        let validated = validate(&flow).expect("flow should validate");
        let executor = FlowExecutor::new(Arc::new(registry));

        let payload = json!({"ok": "true", "value": 123});
        let err = executor
            .run_once(&validated, "trigger", payload, "capture", None)
            .await
            .err()
            .expect("expected error");

        assert!(matches!(err, ExecutionError::NodeFailed { alias, .. } if alias == "route"));
    }

    #[tokio::test]
    async fn if_does_not_gate_edges_outside_targets() {
        let then_count = Arc::new(AtomicUsize::new(0));
        let else_count = Arc::new(AtomicUsize::new(0));
        let log_count = Arc::new(AtomicUsize::new(0));

        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_fn("tests::route", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        {
            let then_count = Arc::clone(&then_count);
            registry
                .register_fn("tests::then", move |val: JsonValue| {
                    let then_count = Arc::clone(&then_count);
                    async move {
                        then_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(val)
                    }
                })
                .unwrap();
        }

        {
            let else_count = Arc::clone(&else_count);
            registry
                .register_fn("tests::else", move |val: JsonValue| {
                    let else_count = Arc::clone(&else_count);
                    async move {
                        else_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(val)
                    }
                })
                .unwrap();
        }

        {
            let log_count = Arc::clone(&log_count);
            registry
                .register_fn("tests::log", move |val: JsonValue| {
                    let log_count = Arc::clone(&log_count);
                    async move {
                        log_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(val)
                    }
                })
                .unwrap();
        }

        registry
            .register_fn("tests::capture", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder = FlowBuilder::new("if_extra_edge", Version::new(1, 0, 0), Profile::Dev);
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
        let route = builder
            .add_node(
                "route",
                &NodeSpec::inline(
                    "tests::route",
                    "Route",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let then_branch = builder
            .add_node(
                "then",
                &NodeSpec::inline(
                    "tests::then",
                    "Then",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let else_branch = builder
            .add_node(
                "else",
                &NodeSpec::inline(
                    "tests::else",
                    "Else",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let log = builder
            .add_node(
                "log",
                &NodeSpec::inline(
                    "tests::log",
                    "Log",
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
                "capture",
                &NodeSpec::inline(
                    "tests::capture",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();

        builder.connect(&trigger, &route);
        builder.connect(&route, &then_branch);
        builder.connect(&route, &else_branch);
        builder.connect(&route, &log);
        builder.connect(&then_branch, &capture);
        builder.connect(&else_branch, &capture);

        let mut flow = builder.build();
        flow.control_surfaces.push(dag_core::ControlSurfaceIR {
            id: "if:route:0".to_string(),
            kind: dag_core::ControlSurfaceKind::If,
            targets: vec!["route".into(), "then".into(), "else".into()],
            config: json!({
                "v": 1,
                "source": "route",
                "selector_pointer": "/ok",
                "then": "then",
                "else": "else",
            }),
        });

        let validated = validate(&flow).expect("flow should validate");
        let executor = FlowExecutor::new(Arc::new(registry));

        let payload = json!({"ok": true, "value": 123});
        let mut instance = executor.instantiate(&validated, "capture").unwrap();
        instance
            .send("trigger", payload.clone())
            .await
            .expect("send");

        match instance.next().await {
            Some(Ok(CaptureResult::Value(value))) => assert_eq!(value, payload),
            _ => panic!("unexpected capture result"),
        }

        timeout(Duration::from_secs(1), async {
            while log_count.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("log node should execute");

        instance.shutdown().await.unwrap();

        assert_eq!(then_count.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(else_count.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(log_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unsupported_control_surface_fails_instantiation() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();

        let mut builder =
            FlowBuilder::new("unsupported_surface", Version::new(1, 0, 0), Profile::Dev);
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
                "capture",
                &NodeSpec::inline(
                    "tests::trigger",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &capture);

        let mut flow = builder.build();
        flow.control_surfaces.push(dag_core::ControlSurfaceIR {
            id: "rate_limit:0".to_string(),
            kind: dag_core::ControlSurfaceKind::RateLimit,
            targets: vec!["trigger".into()],
            config: json!({"v": 1, "target": "trigger", "qps": 1, "burst": 1}),
        });

        let validated = validate(&flow).expect("flow should validate");
        let executor = FlowExecutor::new(Arc::new(registry));

        let err = executor
            .instantiate(&validated, "capture")
            .err()
            .expect("expected error");
        assert!(matches!(
            err,
            ExecutionError::UnsupportedControlSurface { .. }
        ));
    }

    #[tokio::test]
    async fn streaming_capture_emits_events() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_stream_fn("tests::stream", |_val: JsonValue| async move {
                let events = vec![
                    Ok(JsonValue::from(1)),
                    Ok(JsonValue::from(2)),
                    Ok(JsonValue::from(3)),
                ];
                Ok(futures::stream::iter(events))
            })
            .unwrap();

        let mut builder = FlowBuilder::new("streaming", Version::new(1, 0, 0), Profile::Dev);
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
                    "tests::stream",
                    "StreamCapture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::ReadOnly,
                    Determinism::Stable,
                    Some("Emits incremental updates"),
                ),
            )
            .unwrap();
        builder.connect(&trigger, &capture);

        let flow = builder.build();
        let validated = validate(&flow).expect("flow should validate");

        let executor = FlowExecutor::new(Arc::new(registry));
        let result = executor
            .run_once(&validated, "trigger", json!({"seed": 1}), "stream", None)
            .await
            .expect("run once succeeds");

        let mut stream = match result {
            ExecutionResult::Stream(handle) => handle,
            ExecutionResult::Value(_) => panic!("expected streaming response"),
        };

        let mut collected = Vec::new();
        while let Some(event) = stream.next().await {
            let value = event.expect("stream event ok");
            collected.push(value);
        }

        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0], JsonValue::from(1));
        assert_eq!(collected[2], JsonValue::from(3));
    }

    #[tokio::test]
    async fn dropping_stream_cancels_run() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn("tests::trigger", |val: JsonValue| async move { Ok(val) })
            .unwrap();
        registry
            .register_stream_fn("tests::stream", |_val: JsonValue| async move {
                Ok(futures::stream::unfold(0, |state| async move {
                    Some((Ok(JsonValue::from(state)), state + 1))
                }))
            })
            .unwrap();

        let mut builder = FlowBuilder::new("stream_cancel", Version::new(1, 0, 0), Profile::Dev);
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
                    "tests::stream",
                    "StreamCapture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::ReadOnly,
                    Determinism::Stable,
                    Some("Infinite stream for cancellation test"),
                ),
            )
            .unwrap();
        builder.connect(&trigger, &capture);

        let flow = builder.build();
        let validated = validate(&flow).expect("flow should validate");

        let executor = FlowExecutor::new(Arc::new(registry));
        let result = executor
            .run_once(&validated, "trigger", JsonValue::Null, "stream", None)
            .await
            .expect("run once succeeds");

        let handle = match result {
            ExecutionResult::Stream(stream) => stream,
            ExecutionResult::Value(_) => panic!("expected streaming response"),
        };
        let cancel_token = handle.cancellation_token();
        drop(handle);

        tokio::time::timeout(Duration::from_millis(50), cancel_token.cancelled())
            .await
            .expect("cancellation should fire promptly");
    }
}
