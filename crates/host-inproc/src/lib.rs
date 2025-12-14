use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use capabilities::{ResourceAccess, ResourceBag};
use kernel_exec::{ExecutionError, ExecutionResult, FlowExecutor};
use kernel_plan::ValidatedIR;
use serde::Serialize;
use serde_json::Value as JsonValue;

/// Re-export handy executor result types for bridge crates.
pub use kernel_exec::{
    ExecutionError as HostExecutionError, ExecutionResult as HostExecutionResult,
};

/// Canonical invocation payload forwarded from bridges into the in-process host.
#[derive(Debug, Clone)]
pub struct Invocation {
    trigger_alias: String,
    capture_alias: String,
    payload: JsonValue,
    deadline: Option<Duration>,
    metadata: InvocationMetadata,
}

impl Invocation {
    /// Construct a new invocation with the required aliases and payload.
    pub fn new(
        trigger_alias: impl Into<String>,
        capture_alias: impl Into<String>,
        payload: JsonValue,
    ) -> Self {
        Self {
            trigger_alias: trigger_alias.into(),
            capture_alias: capture_alias.into(),
            payload,
            deadline: None,
            metadata: InvocationMetadata::default(),
        }
    }

    /// Attach an optional deadline (relative duration) for the capture response.
    pub fn with_deadline(mut self, deadline: Option<Duration>) -> Self {
        self.deadline = deadline;
        self
    }

    /// Borrow invocation metadata.
    pub fn metadata(&self) -> &InvocationMetadata {
        &self.metadata
    }

    /// Mutably borrow invocation metadata to add rich context.
    pub fn metadata_mut(&mut self) -> &mut InvocationMetadata {
        &mut self.metadata
    }

    /// Consume the invocation and expose its components for bridge serialisation.
    pub fn into_parts(self) -> InvocationParts {
        InvocationParts {
            trigger_alias: self.trigger_alias,
            capture_alias: self.capture_alias,
            payload: self.payload,
            deadline: self.deadline,
            metadata: self.metadata,
        }
    }

    /// Reconstruct an invocation from the supplied components.
    pub fn from_parts(parts: InvocationParts) -> Self {
        Self {
            trigger_alias: parts.trigger_alias,
            capture_alias: parts.capture_alias,
            payload: parts.payload,
            deadline: parts.deadline,
            metadata: parts.metadata,
        }
    }
}

/// Owned invocation pieces used by bridge crates when persisting work items.
#[derive(Debug)]
pub struct InvocationParts {
    pub trigger_alias: String,
    pub capture_alias: String,
    pub payload: JsonValue,
    pub deadline: Option<Duration>,
    pub metadata: InvocationMetadata,
}

/// Arbitrary metadata supplied by bridges (request IDs, headers, environment hints, etc.).
#[derive(Debug, Clone, Default)]
pub struct InvocationMetadata {
    labels: BTreeMap<String, String>,
    extensions: BTreeMap<String, JsonValue>,
}

impl InvocationMetadata {
    /// Insert a simple string label (e.g., request ID, tenant).
    pub fn insert_label(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.labels.insert(key.into(), value.into());
    }

    /// Insert structured metadata for downstream plugins.
    pub fn insert_extension<S>(&mut self, key: impl Into<String>, value: S)
    where
        S: Serialize,
    {
        if let Ok(serialized) = serde_json::to_value(value) {
            self.extensions.insert(key.into(), serialized);
        }
    }

    /// Retrieve labels for inspection.
    pub fn labels(&self) -> &BTreeMap<String, String> {
        &self.labels
    }

    /// Retrieve structured extensions.
    pub fn extensions(&self) -> &BTreeMap<String, JsonValue> {
        &self.extensions
    }
}

fn collect_required_effect_hints(ir: &ValidatedIR) -> Vec<String> {
    let mut set = BTreeSet::new();
    for node in &ir.flow().nodes {
        for hint in &node.effect_hints {
            if hint.starts_with("resource::") {
                set.insert(hint.clone());
            }
        }
    }
    set.into_iter().collect()
}

fn is_hint_satisfied_by_resources(hint: &str, resources: &dyn ResourceAccess) -> bool {
    match hint {
        capabilities::http::HINT_HTTP_READ => resources.http_read().is_some(),
        capabilities::http::HINT_HTTP_WRITE => resources.http_write().is_some(),
        capabilities::kv::HINT_KV_READ | capabilities::kv::HINT_KV_WRITE => {
            resources.kv().is_some()
        }
        capabilities::blob::HINT_BLOB_READ | capabilities::blob::HINT_BLOB_WRITE => {
            resources.blob().is_some()
        }
        capabilities::queue::HINT_QUEUE_PUBLISH | capabilities::queue::HINT_QUEUE_CONSUME => {
            resources.queue().is_some()
        }
        capabilities::dedupe::HINT_DEDUPE_WRITE => resources.dedupe_store().is_some(),
        _ => false,
    }
}

/// Shared in-process runtime that owns the executor and validated IR.
#[derive(Clone)]
pub struct HostRuntime {
    executor: FlowExecutor,
    ir: Arc<ValidatedIR>,
    plugins: Arc<Vec<Arc<dyn EnvironmentPlugin>>>,
    resources: Arc<dyn ResourceAccess>,
    required_effect_hints: Arc<Vec<String>>,
}

impl HostRuntime {
    /// Build a new runtime instance.
    pub fn new(mut executor: FlowExecutor, ir: Arc<ValidatedIR>) -> Self {
        let resource_bag: Arc<ResourceBag> = Arc::new(ResourceBag::new());
        executor = executor.with_resource_access(resource_bag.clone());
        let resources: Arc<dyn ResourceAccess> = resource_bag;
        let required_effect_hints = Arc::new(collect_required_effect_hints(ir.as_ref()));
        Self {
            executor,
            ir,
            plugins: Arc::new(Vec::new()),
            resources,
            required_effect_hints,
        }
    }

    /// Build a runtime with environment plugins already registered.
    pub fn with_plugins(
        mut executor: FlowExecutor,
        ir: Arc<ValidatedIR>,
        plugins: Vec<Arc<dyn EnvironmentPlugin>>,
    ) -> Self {
        let resource_bag: Arc<ResourceBag> = Arc::new(ResourceBag::new());
        executor = executor.with_resource_access(resource_bag.clone());
        let resources: Arc<dyn ResourceAccess> = resource_bag;
        let required_effect_hints = Arc::new(collect_required_effect_hints(ir.as_ref()));
        Self {
            executor,
            ir,
            plugins: Arc::new(plugins),
            resources,
            required_effect_hints,
        }
    }

    /// Access the underlying executor (read-only).
    pub fn executor(&self) -> &FlowExecutor {
        &self.executor
    }

    /// Access the validated flow IR backing this runtime.
    pub fn ir(&self) -> &ValidatedIR {
        self.ir.as_ref()
    }

    /// Replace the resource access collection used for node execution.
    pub fn with_resource_access<T>(mut self, resources: Arc<T>) -> Self
    where
        T: ResourceAccess,
    {
        self.executor = self
            .executor
            .clone()
            .with_resource_access(resources.clone());
        self.resources = resources;
        self
    }

    /// Convenience builder that accepts a [`ResourceBag`].
    pub fn with_resource_bag(self, bag: ResourceBag) -> Self {
        let resources: Arc<ResourceBag> = Arc::new(bag);
        self.with_resource_access(resources)
    }

    /// Clone the resource access handle currently configured.
    pub fn resources(&self) -> Arc<dyn ResourceAccess> {
        self.resources.clone()
    }

    /// Fail fast if the runtime is missing required capability domains.
    ///
    /// Derivation rule (0.1): required domains are inferred from `NodeIR.effect_hints`.
    pub fn preflight(&self) -> Result<(), ExecutionError> {
        let mut missing: Vec<String> = self
            .required_effect_hints
            .iter()
            .filter(|hint| !is_hint_satisfied_by_resources(hint.as_str(), self.resources.as_ref()))
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        missing.sort();
        missing.dedup();
        Err(ExecutionError::MissingCapabilities { hints: missing })
    }

    /// Execute a single invocation, returning the captured result or error.
    pub async fn execute(&self, invocation: Invocation) -> Result<ExecutionResult, ExecutionError> {
        self.preflight()?;

        let InvocationParts {
            trigger_alias,
            capture_alias,
            payload,
            deadline,
            metadata,
        } = invocation.into_parts();

        for plugin in self.plugins.iter() {
            plugin.before_execute(&metadata);
        }

        let result = self
            .executor
            .run_once(
                self.ir.as_ref(),
                &trigger_alias,
                payload,
                &capture_alias,
                deadline,
            )
            .await;

        for plugin in self.plugins.iter() {
            plugin.after_execute(
                &metadata,
                result
                    .as_ref()
                    .map(|value| value as &ExecutionResult)
                    .map_err(|err| err as &ExecutionError),
            );
        }

        result
    }
}

/// Environment plugins can inspect invocation metadata and execution outcomes to inject
/// environment-specific behaviour (tracing, logging, capability provisioning, etc.).
pub trait EnvironmentPlugin: Send + Sync {
    /// Invoked before execution begins. Use this to prepare context such as tracing spans.
    fn before_execute(&self, _metadata: &InvocationMetadata) {}

    /// Invoked once execution finishes (success or failure).
    fn after_execute(
        &self,
        _metadata: &InvocationMetadata,
        _outcome: Result<&ExecutionResult, &ExecutionError>,
    ) {
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dag_core::prelude::*;
    use kernel_exec::NodeRegistry;
    use kernel_plan::validate;
    use std::sync::{Arc as StdArc, Mutex};

    #[tokio::test]
    async fn executes_invocation_via_runtime() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::capture",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();

        let mut builder = FlowBuilder::new("runtime_test", Version::new(1, 0, 0), Profile::Dev);
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
        builder.connect(&trigger, &capture);
        let ir = Arc::new(validate(&builder.build()).expect("flow validates"));

        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir);
        let invocation = Invocation::new("trigger", "capture", serde_json::json!({"ok": true}));

        let result = runtime
            .execute(invocation)
            .await
            .expect("execution succeeds");
        match result {
            ExecutionResult::Value(value) => {
                assert_eq!(value, serde_json::json!({"ok": true}));
            }
            ExecutionResult::Stream(_) => panic!("expected value result"),
        }
    }

    #[tokio::test]
    async fn environment_plugin_hooks_fire() {
        struct RecordingPlugin {
            before: StdArc<Mutex<Vec<String>>>,
            after: StdArc<Mutex<Vec<String>>>,
        }

        impl EnvironmentPlugin for RecordingPlugin {
            fn before_execute(&self, metadata: &InvocationMetadata) {
                let mut guard = self.before.lock().unwrap();
                guard.push(
                    metadata
                        .labels()
                        .get("test.label")
                        .cloned()
                        .unwrap_or_default(),
                );
            }

            fn after_execute(
                &self,
                metadata: &InvocationMetadata,
                outcome: Result<&ExecutionResult, &ExecutionError>,
            ) {
                let mut guard = self.after.lock().unwrap();
                let mut summary = metadata
                    .labels()
                    .get("test.label")
                    .cloned()
                    .unwrap_or_default();
                summary.push(':');
                summary.push_str(match outcome {
                    Ok(_) => "ok",
                    Err(_) => "err",
                });
                guard.push(summary);
            }
        }

        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::capture",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();

        let mut builder = FlowBuilder::new("runtime_test", Version::new(1, 0, 0), Profile::Dev);
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
        builder.connect(&trigger, &capture);
        let ir = Arc::new(validate(&builder.build()).expect("flow validates"));

        let before = StdArc::new(Mutex::new(Vec::new()));
        let after = StdArc::new(Mutex::new(Vec::new()));
        let plugin = Arc::new(RecordingPlugin {
            before: before.clone(),
            after: after.clone(),
        });

        let runtime =
            HostRuntime::with_plugins(FlowExecutor::new(Arc::new(registry)), ir, vec![plugin]);
        let mut invocation = Invocation::new("trigger", "capture", serde_json::json!({"ok": true}));
        invocation
            .metadata_mut()
            .insert_label("test.label", "case1");

        let result = runtime
            .execute(invocation)
            .await
            .expect("execution succeeds");
        if let ExecutionResult::Stream(_) = result {
            panic!("expected value result");
        }

        assert_eq!(before.lock().unwrap().as_slice(), &["case1".to_string()]);
        assert_eq!(after.lock().unwrap().as_slice(), &["case1:ok".to_string()]);
    }

    #[tokio::test]
    async fn preflight_fails_when_required_kv_missing() {
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

        let mut builder = FlowBuilder::new("preflight_kv", Version::new(1, 0, 0), Profile::Dev);
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
        builder.connect(&trigger, &kv_node);
        let ir = Arc::new(validate(&builder.build()).expect("flow validates"));

        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(ResourceBag::new());
        let invocation = Invocation::new("trigger", "kv", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(err) => assert!(matches!(err, ExecutionError::MissingCapabilities { .. })),
        }
    }

    #[tokio::test]
    async fn preflight_passes_when_required_kv_present() {
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

        let mut builder = FlowBuilder::new("preflight_kv", Version::new(1, 0, 0), Profile::Dev);
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
        builder.connect(&trigger, &kv_node);
        let ir = Arc::new(validate(&builder.build()).expect("flow validates"));

        let resources = ResourceBag::new().with_kv(Arc::new(capabilities::kv::MemoryKv::new()));
        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(resources);
        let invocation = Invocation::new("trigger", "kv", serde_json::json!({"ok": true}));

        let result = runtime
            .execute(invocation)
            .await
            .expect("execution succeeds");
        match result {
            ExecutionResult::Value(value) => assert_eq!(value, serde_json::json!({"ok": true})),
            ExecutionResult::Stream(_) => panic!("expected value result"),
        }
    }

    #[tokio::test]
    async fn preflight_fails_when_required_http_write_missing() {
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
                "tests::http_node",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();

        let mut builder = FlowBuilder::new("preflight_http", Version::new(1, 0, 0), Profile::Dev);
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
        let http_node = builder
            .add_node(
                "http",
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
        builder.connect(&trigger, &http_node);

        let mut flow = builder.build();
        flow.nodes
            .iter_mut()
            .find(|node| node.alias == "http")
            .expect("http node")
            .idempotency
            .key = Some("idempotency".to_string());

        let ir = Arc::new(validate(&flow).expect("flow validates"));

        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(ResourceBag::new());
        let invocation = Invocation::new("trigger", "http", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingCapabilities { hints }) => {
                assert_eq!(hints, vec![capabilities::http::HINT_HTTP_WRITE.to_string()]);
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn preflight_passes_when_required_http_write_present() {
        const HTTP_WRITE_EFFECT_HINTS: [&str; 1] = [capabilities::http::HINT_HTTP_WRITE];

        struct NullHttp;

        #[async_trait::async_trait]
        impl capabilities::http::HttpWrite for NullHttp {
            async fn send(
                &self,
                _request: capabilities::http::HttpRequest,
            ) -> capabilities::http::HttpResult<capabilities::http::HttpResponse> {
                Ok(capabilities::http::HttpResponse {
                    status: 200,
                    headers: capabilities::http::HttpHeaders::default(),
                    body: Vec::new(),
                })
            }
        }

        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::http_node",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();

        let mut builder = FlowBuilder::new("preflight_http", Version::new(1, 0, 0), Profile::Dev);
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
        let http_node = builder
            .add_node(
                "http",
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
        builder.connect(&trigger, &http_node);

        let mut flow = builder.build();
        flow.nodes
            .iter_mut()
            .find(|node| node.alias == "http")
            .expect("http node")
            .idempotency
            .key = Some("idempotency".to_string());

        let ir = Arc::new(validate(&flow).expect("flow validates"));

        let resources = ResourceBag::new().with_http_write(Arc::new(NullHttp));
        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(resources);
        let invocation = Invocation::new("trigger", "http", serde_json::json!({"ok": true}));

        let result = runtime
            .execute(invocation)
            .await
            .expect("execution succeeds");
        if let ExecutionResult::Stream(_) = result {
            panic!("expected value result");
        }
    }

    #[tokio::test]
    async fn preflight_fails_when_required_dedupe_missing() {
        const DEDUPE_EFFECT_HINTS: [&str; 1] = [capabilities::dedupe::HINT_DEDUPE_WRITE];

        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::dedupe_node",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();

        let mut builder = FlowBuilder::new("preflight_dedupe", Version::new(1, 0, 0), Profile::Dev);
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
        let dedupe_node = builder
            .add_node(
                "dedupe",
                &NodeSpec::inline_with_hints(
                    "tests::dedupe_node",
                    "DedupeNode",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Effectful,
                    Determinism::BestEffort,
                    None,
                    &[],
                    &DEDUPE_EFFECT_HINTS,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &dedupe_node);

        let mut flow = builder.build();
        flow.nodes
            .iter_mut()
            .find(|node| node.alias == "dedupe")
            .expect("dedupe node")
            .idempotency
            .key = Some("idempotency".to_string());

        let ir = Arc::new(validate(&flow).expect("flow validates"));

        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(ResourceBag::new());
        let invocation = Invocation::new("trigger", "dedupe", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingCapabilities { hints }) => {
                assert_eq!(
                    hints,
                    vec![capabilities::dedupe::HINT_DEDUPE_WRITE.to_string()]
                );
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn preflight_fails_when_unknown_resource_hint_missing() {
        const UNKNOWN_EFFECT_HINTS: [&str; 1] = ["resource::mystery::read"];

        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn("tests::unknown_node", |value: JsonValue| async move {
                Ok(value)
            })
            .unwrap();

        let mut builder =
            FlowBuilder::new("preflight_unknown", Version::new(1, 0, 0), Profile::Dev);
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
        let unknown = builder
            .add_node(
                "unknown",
                &NodeSpec::inline_with_hints(
                    "tests::unknown_node",
                    "Unknown",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                    &[],
                    &UNKNOWN_EFFECT_HINTS,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &unknown);

        let ir = Arc::new(validate(&builder.build()).expect("flow validates"));

        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(ResourceBag::new());
        let invocation = Invocation::new("trigger", "unknown", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingCapabilities { hints }) => {
                assert_eq!(hints, vec!["resource::mystery::read".to_string()]);
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn preflight_ignores_determinism_hints() {
        const CLOCK_DET_HINTS: [&str; 1] = [capabilities::clock::HINT_CLOCK];

        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn("tests::clocky", |value: JsonValue| async move { Ok(value) })
            .unwrap();

        let mut builder = FlowBuilder::new("preflight_det", Version::new(1, 0, 0), Profile::Dev);
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
        let clocky = builder
            .add_node(
                "clocky",
                &NodeSpec::inline_with_hints(
                    "tests::clocky",
                    "Clocky",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::BestEffort,
                    None,
                    &CLOCK_DET_HINTS,
                    &[],
                ),
            )
            .unwrap();
        builder.connect(&trigger, &clocky);

        let ir = Arc::new(validate(&builder.build()).expect("flow validates"));

        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(ResourceBag::new());
        let invocation = Invocation::new("trigger", "clocky", serde_json::json!({"ok": true}));

        let result = runtime
            .execute(invocation)
            .await
            .expect("execution succeeds");
        if let ExecutionResult::Stream(_) = result {
            panic!("expected value result");
        }
    }

    #[tokio::test]
    async fn preflight_missing_hints_sorted_and_deduped() {
        const MULTI_EFFECT_HINTS: [&str; 3] = [
            "resource::mystery::read",
            capabilities::kv::HINT_KV_READ,
            capabilities::kv::HINT_KV_READ,
        ];
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

        let mut builder = FlowBuilder::new("preflight_multi", Version::new(1, 0, 0), Profile::Dev);
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
                    &MULTI_EFFECT_HINTS,
                ),
            )
            .unwrap();
        let http_node = builder
            .add_node(
                "http",
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
            .find(|node| node.alias == "http")
            .expect("http node")
            .idempotency
            .key = Some("idempotency".to_string());

        let ir = Arc::new(validate(&flow).expect("flow validates"));

        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(ResourceBag::new());
        let invocation = Invocation::new("trigger", "http", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingCapabilities { hints }) => {
                assert_eq!(
                    hints,
                    vec![
                        capabilities::http::HINT_HTTP_WRITE.to_string(),
                        capabilities::kv::HINT_KV_READ.to_string(),
                        "resource::mystery::read".to_string(),
                    ]
                );
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }
}
