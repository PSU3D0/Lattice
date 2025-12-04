use std::collections::BTreeMap;
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

/// Shared in-process runtime that owns the executor and validated IR.
#[derive(Clone)]
pub struct HostRuntime {
    executor: FlowExecutor,
    ir: Arc<ValidatedIR>,
    plugins: Arc<Vec<Arc<dyn EnvironmentPlugin>>>,
    resources: Arc<dyn ResourceAccess>,
}

impl HostRuntime {
    /// Build a new runtime instance.
    pub fn new(mut executor: FlowExecutor, ir: Arc<ValidatedIR>) -> Self {
        let resource_bag: Arc<ResourceBag> = Arc::new(ResourceBag::new());
        executor = executor.with_resource_access(resource_bag.clone());
        let resources: Arc<dyn ResourceAccess> = resource_bag;
        Self {
            executor,
            ir,
            plugins: Arc::new(Vec::new()),
            resources,
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
        Self {
            executor,
            ir,
            plugins: Arc::new(plugins),
            resources,
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

    /// Execute a single invocation, returning the captured result or error.
    pub async fn execute(&self, invocation: Invocation) -> Result<ExecutionResult, ExecutionError> {
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
                summary.push_str(":");
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
}
