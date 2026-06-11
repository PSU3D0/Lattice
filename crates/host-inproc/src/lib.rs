use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(not(target_arch = "wasm32"))]
use capabilities::connector::ConnectorBindingScope;
use capabilities::durability::{CheckpointError, CheckpointFilter, CheckpointHandle, FlowFrontier};
use capabilities::workspace::{
    Workspace, WorkspaceCompletionDisposition, WorkspaceFactory, WorkspaceRunScope,
};
use capabilities::{ResourceAccess, ResourceBag};
#[cfg(not(target_arch = "wasm32"))]
use dag_core::ConnectorResolutionModeDecl;
use dag_core::DurabilityMode;
use kernel_exec::{ExecutionError, ExecutionResult, FlowExecutor, NodeResolver};
use kernel_plan::ValidatedIR;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Re-export handy executor result types for bridge crates.
pub use kernel_exec::{
    ExecutionError as HostExecutionError, ExecutionResult as HostExecutionResult,
};

pub struct FlowBundle {
    pub validated_ir: ValidatedIR,
    pub entrypoints: Vec<FlowEntrypoint>,
    pub resolver: Arc<dyn NodeResolver>,
    pub node_contracts: Vec<NodeContract>,
    pub environment_plugins: Vec<Arc<dyn EnvironmentPlugin>>,
}

impl FlowBundle {
    pub fn executor(&self) -> FlowExecutor {
        self.validate_allowlist()
            .unwrap_or_else(|err| panic!("FlowBundle allowlist validation failed: {err}"));
        // TODO: swap in a wasm-backed resolver when available.
        FlowExecutor::new_with_resolver(self.resolver.clone())
    }

    pub fn validate_allowlist(&self) -> Result<(), BundleError> {
        let allowlist: BTreeSet<&str> = self
            .node_contracts
            .iter()
            .map(|contract| contract.identifier.as_str())
            .collect();
        for node in &self.validated_ir.flow().nodes {
            if !allowlist.contains(node.identifier.as_str()) {
                return Err(BundleError::UnknownIdentifier {
                    identifier: node.identifier.clone(),
                });
            }
        }
        Ok(())
    }
}

pub struct FlowEntrypoint {
    pub trigger_alias: String,
    pub capture_alias: String,
    pub route_path: Option<String>,
    pub method: Option<String>,
    pub deadline: Option<Duration>,
    pub route_aliases: Vec<String>,
}

pub struct NodeContract {
    pub identifier: String,
    pub contract_hash: Option<String>,
    pub source: NodeSource,
}

#[derive(Clone, Debug)]
pub enum NodeSource {
    Local,
    Plugin,
    Remote,
}

#[derive(Debug)]
pub enum BundleError {
    UnknownIdentifier { identifier: String },
}

impl std::fmt::Display for BundleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BundleError::UnknownIdentifier { identifier } => {
                write!(f, "unknown node identifier `{identifier}`")
            }
        }
    }
}

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

#[derive(Debug, Clone, Serialize)]
pub struct ResumeContext {
    pub frontier: FlowFrontier,
    pub attempt: u32,
    pub resumed_at_ms: u64,
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

    /// Attach resume metadata labels and extensions.
    pub fn insert_resume_context(
        &mut self,
        checkpoint_id: impl Into<String>,
        resume_id: impl Into<String>,
        attempt: u32,
        frontier: FlowFrontier,
        resumed_at_ms: u64,
    ) {
        self.insert_label("lf.checkpoint_id", checkpoint_id);
        self.insert_label("lf.resume_id", resume_id);
        self.insert_label("lf.resume_attempt", attempt.to_string());
        self.insert_extension(
            "lf.resume",
            ResumeContext {
                frontier,
                attempt,
                resumed_at_ms,
            },
        );
    }
}

fn collect_required_effect_hints(ir: &ValidatedIR) -> Vec<String> {
    let mut set = BTreeSet::new();
    for node in &ir.flow().nodes {
        for hint in &node.effect_hints {
            // Kernel-plan validation (EFFECT202) guarantees every effect hint
            // in a ValidatedIR is either a canonical dag_core::EffectHint or
            // a policy marker; only capability hints become requirements.
            if dag_core::EffectHint::parse(hint).is_ok() {
                set.insert(hint.clone());
            }
        }
    }
    set.into_iter().collect()
}

/// Per-node connector-resolved effect hints (keyed by node alias).
///
/// These hints are part of each node's declaration surface — the bound
/// connection's requirements, resolved at binding time — so packet A2 feeds
/// them into the node's scoped capability grant set in addition to checking
/// them during preflight.
type ConnectorResolvedGrants = BTreeMap<String, BTreeSet<dag_core::EffectHint>>;

#[cfg(not(target_arch = "wasm32"))]
fn collect_resolution_aware_effect_hints(
    ir: &ValidatedIR,
    resources: &dyn ResourceAccess,
) -> Result<ConnectorResolvedGrants, ExecutionError> {
    let Some(runtime) = resources.connector_runtime() else {
        let needs_bound = ir.flow().nodes.iter().any(|node| {
            node.connector_ops.iter().any(|op| {
                op.selected_resolution_mode == ConnectorResolutionModeDecl::BoundConnection
            })
        });
        if needs_bound {
            return Err(ExecutionError::HostEnvironment(anyhow::anyhow!(
                "missing connector runtime for bound-connection preflight"
            )));
        }
        return Ok(ConnectorResolvedGrants::new());
    };

    let mut grants = ConnectorResolvedGrants::new();
    for node in &ir.flow().nodes {
        for op in &node.connector_ops {
            if op.selected_resolution_mode != ConnectorResolutionModeDecl::BoundConnection {
                continue;
            }
            let scope = ConnectorBindingScope::new(
                ir.flow().id.as_str(),
                node.alias.clone(),
                node.identifier.clone(),
                op.connector_id.clone(),
            );
            let derived =
                host_block_on_preflight(runtime.clone(), scope, op.selected_resolution_mode)
                    .map_err(|err| ExecutionError::HostEnvironment(anyhow::anyhow!(err)))?;
            for hint in derived {
                // Connector-resolved hints are not covered by kernel-plan
                // validation, so unknown strings fail closed here instead of
                // being dropped (prefix typos) or surfacing later as a
                // misleading MissingCapabilities error (suffix typos).
                match dag_core::EffectHint::parse(&hint) {
                    Ok(parsed) => {
                        grants
                            .entry(node.alias.clone())
                            .or_default()
                            .insert(parsed);
                    }
                    Err(err) => {
                        return Err(ExecutionError::HostEnvironment(anyhow::anyhow!(
                            "connector `{}` (node `{}`) resolved an invalid effect hint: {err} \
                             (EFFECT202; see impl-docs/error-codes.md)",
                            op.connector_id,
                            node.alias,
                        )));
                    }
                }
            }
        }
    }

    Ok(grants)
}

#[cfg(not(target_arch = "wasm32"))]
fn host_block_on_preflight(
    runtime: Arc<dyn capabilities::connector::ConnectorRuntime>,
    scope: ConnectorBindingScope,
    selected_mode: ConnectorResolutionModeDecl,
) -> Result<Vec<String>, capabilities::connector::ConnectorRuntimeError> {
    std::thread::scope(|s| {
        s.spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("host_block_on_preflight: runtime");
            rt.block_on(runtime.resolve_required_effect_hints(&scope, selected_mode))
        })
        .join()
        .expect("host_block_on_preflight: thread panicked")
    })
}

#[cfg(target_arch = "wasm32")]
fn collect_resolution_aware_effect_hints(
    _ir: &ValidatedIR,
    _resources: &dyn ResourceAccess,
) -> Result<ConnectorResolvedGrants, ExecutionError> {
    Ok(ConnectorResolvedGrants::new())
}

fn is_hint_satisfied_by_resources(hint: &str, resources: &dyn ResourceAccess) -> bool {
    use dag_core::EffectHint;

    // Fail closed on anything that is not a canonical hint. Unknown strings
    // cannot reach this point through a ValidatedIR (kernel-plan EFFECT202)
    // or connector resolution (checked at collection), but never satisfy them
    // if they do.
    let Ok(hint) = EffectHint::parse(hint) else {
        return false;
    };

    // Exhaustive on purpose: adding an EffectHint variant must force a
    // decision here about what satisfies it.
    match hint {
        EffectHint::Http => resources.http_read().is_some() || resources.http_write().is_some(),
        EffectHint::HttpRead => resources.http_read().is_some(),
        EffectHint::HttpWrite => resources.http_write().is_some(),
        EffectHint::Clock => resources.clock().is_some(),
        // No RNG accessor exists on ResourceAccess yet; unsatisfiable
        // (matches the historical `_ => false` behavior for this hint).
        EffectHint::Rng => false,
        // The legacy db family has no ResourceAccess accessor (sql::* is the
        // supported relational surface); unsatisfiable, as before A1.
        EffectHint::Db | EffectHint::DbRead | EffectHint::DbWrite => false,
        EffectHint::Sql => {
            resources.sql_read().is_some()
                || resources.sql_write().is_some()
                || resources.sql_admin().is_some()
        }
        EffectHint::SqlRead => resources.sql_read().is_some(),
        EffectHint::SqlWrite => resources.sql_write().is_some(),
        EffectHint::SqlAdmin => resources.sql_admin().is_some(),
        EffectHint::Kv | EffectHint::KvRead | EffectHint::KvWrite => resources.kv().is_some(),
        EffectHint::Blob | EffectHint::BlobRead | EffectHint::BlobWrite => {
            resources.blob().is_some()
        }
        EffectHint::Queue | EffectHint::QueuePublish | EffectHint::QueueConsume => {
            resources.queue().is_some()
        }
        EffectHint::Dedupe | EffectHint::DedupeWrite => resources.dedupe_store().is_some(),
        EffectHint::Workspace | EffectHint::WorkspaceRead | EffectHint::WorkspaceWrite => {
            resources.workspace().is_some()
        }
    }
}

#[derive(Clone)]
struct InvocationResources {
    base: Arc<dyn ResourceAccess>,
    workspace: Arc<dyn Workspace>,
}

impl ResourceAccess for InvocationResources {
    fn http_read(&self) -> Option<&dyn capabilities::http::HttpRead> {
        self.base.http_read()
    }

    fn http_write(&self) -> Option<&dyn capabilities::http::HttpWrite> {
        self.base.http_write()
    }

    fn clock(&self) -> Option<&dyn capabilities::clock::Clock> {
        self.base.clock()
    }

    fn cache(&self) -> Option<&dyn capabilities::cache::Cache> {
        self.base.cache()
    }

    fn kv(&self) -> Option<&dyn capabilities::kv::KeyValue> {
        self.base.kv()
    }

    fn sql_read(&self) -> Option<&dyn capabilities::sql::SqlRead> {
        self.base.sql_read()
    }

    fn sql_write(&self) -> Option<&dyn capabilities::sql::SqlWrite> {
        self.base.sql_write()
    }

    fn sql_admin(&self) -> Option<&dyn capabilities::sql::SqlAdmin> {
        self.base.sql_admin()
    }

    fn blob(&self) -> Option<&dyn capabilities::blob::BlobStore> {
        self.base.blob()
    }

    fn queue(&self) -> Option<&dyn capabilities::queue::Queue> {
        self.base.queue()
    }

    fn dedupe_store(&self) -> Option<&dyn capabilities::dedupe::DedupeStore> {
        self.base.dedupe_store()
    }

    fn checkpoint_store(&self) -> Option<&dyn capabilities::durability::CheckpointStore> {
        self.base.checkpoint_store()
    }

    fn resume_scheduler(&self) -> Option<&dyn capabilities::durability::ResumeScheduler> {
        self.base.resume_scheduler()
    }

    fn resume_signal_source(&self) -> Option<&dyn capabilities::durability::ResumeSignalSource> {
        self.base.resume_signal_source()
    }

    fn checkpoint_blob_store(&self) -> Option<&dyn capabilities::durability::CheckpointBlobStore> {
        self.base.checkpoint_blob_store()
    }

    fn workspace(&self) -> Option<&dyn Workspace> {
        Some(self.workspace.as_ref())
    }

    fn connector_runtime(&self) -> Option<Arc<dyn capabilities::connector::ConnectorRuntime>> {
        self.base.connector_runtime()
    }

    fn connector_scope(&self) -> Option<capabilities::connector::ConnectorBindingScope> {
        self.base.connector_scope()
    }

    fn max_durability_mode(&self) -> dag_core::DurabilityMode {
        self.base.max_durability_mode()
    }
}

/// Shared in-process runtime that owns the executor and validated IR.
#[derive(Clone)]
pub struct HostRuntime {
    executor: FlowExecutor,
    ir: Arc<ValidatedIR>,
    plugins: Arc<Vec<Arc<dyn EnvironmentPlugin>>>,
    resources: Arc<dyn ResourceAccess>,
    workspace_factory: Option<Arc<dyn WorkspaceFactory>>,
    required_effect_hints: Arc<Vec<String>>,
    bundle_id: Option<String>,
    allow_legacy_unpinned_checkpoints: bool,
}

impl HostRuntime {
    /// Build a new runtime instance.
    pub fn new(mut executor: FlowExecutor, ir: Arc<ValidatedIR>) -> Self {
        let resource_bag: Arc<ResourceBag> = Arc::new(ResourceBag::new());
        executor = executor.with_resource_access(resource_bag.clone());
        let resources: Arc<dyn ResourceAccess> = resource_bag;
        let required_effect_hints = Arc::new(collect_required_effect_hints(ir.as_ref()));
        let bundle_id = Some(default_runtime_bundle_id(ir.as_ref()));
        if let Some(id) = bundle_id.clone() {
            executor = executor.with_bundle_id(id);
        }
        Self {
            executor,
            ir,
            plugins: Arc::new(Vec::new()),
            resources,
            workspace_factory: None,
            required_effect_hints,
            bundle_id,
            allow_legacy_unpinned_checkpoints: true,
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
        let bundle_id = Some(default_runtime_bundle_id(ir.as_ref()));
        if let Some(id) = bundle_id.clone() {
            executor = executor.with_bundle_id(id);
        }
        Self {
            executor,
            ir,
            plugins: Arc::new(plugins),
            resources,
            workspace_factory: None,
            required_effect_hints,
            bundle_id,
            allow_legacy_unpinned_checkpoints: true,
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
    pub fn with_resource_access(mut self, resources: Arc<dyn ResourceAccess>) -> Self {
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

    /// Bind a host-managed workspace factory used to open per-run workspaces.
    pub fn with_workspace_factory(mut self, factory: Arc<dyn WorkspaceFactory>) -> Self {
        self.workspace_factory = Some(factory);
        self
    }

    /// Override runtime bundle id used for checkpoint pinning.
    pub fn with_bundle_id(mut self, bundle_id: impl Into<String>) -> Self {
        let id = bundle_id.into();
        self.executor = self.executor.clone().with_bundle_id(id.clone());
        self.bundle_id = Some(id);
        self
    }

    /// Disable legacy compatibility path for checkpoints without `bundle_id`.
    pub fn with_legacy_unpinned_checkpoints_allowed(mut self, allow: bool) -> Self {
        self.allow_legacy_unpinned_checkpoints = allow;
        self
    }

    /// Clone the resource access handle currently configured.
    pub fn resources(&self) -> Arc<dyn ResourceAccess> {
        self.resources.clone()
    }

    /// Fail fast if the runtime is missing required capability domains.
    ///
    /// Derivation rule (0.1): required domains are inferred from `NodeIR.effect_hints`.
    pub fn preflight(&self) -> Result<(), ExecutionError> {
        self.preflight_with_resources(self.resources.as_ref())
            .map(|_| ())
    }

    /// Preflight against a concrete resource view. On success, returns the
    /// per-node connector-resolved hints so execution paths can extend each
    /// node's scoped capability grant set (packet A2).
    fn preflight_with_resources(
        &self,
        resources: &dyn ResourceAccess,
    ) -> Result<ConnectorResolvedGrants, ExecutionError> {
        let mut missing_durability =
            collect_missing_durability_services(self.ir.as_ref(), resources);
        if !missing_durability.is_empty() {
            missing_durability.sort();
            missing_durability.dedup();
            return Err(ExecutionError::MissingDurabilityServices {
                missing: missing_durability,
            });
        }

        let connector_grants =
            collect_resolution_aware_effect_hints(self.ir.as_ref(), resources)?;
        let mut required: BTreeSet<String> = self.required_effect_hints.iter().cloned().collect();
        for hints in connector_grants.values() {
            for hint in hints {
                required.insert(hint.as_str().to_string());
            }
        }

        let mut missing: Vec<String> = required
            .iter()
            .filter(|hint| !is_hint_satisfied_by_resources(hint.as_str(), resources))
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok(connector_grants);
        }
        missing.sort();
        missing.dedup();
        Err(ExecutionError::MissingCapabilities { hints: missing })
    }

    async fn bind_workspace_resources(
        &self,
        scope: WorkspaceRunScope,
    ) -> Result<Arc<dyn ResourceAccess>, ExecutionError> {
        let Some(factory) = self.workspace_factory.as_ref() else {
            return Ok(self.resources.clone());
        };
        let workspace = factory
            .open(scope)
            .await
            .map_err(ExecutionError::HostEnvironment)?;
        Ok(Arc::new(InvocationResources {
            base: self.resources.clone(),
            workspace,
        }))
    }

    async fn complete_workspace(
        &self,
        scope: WorkspaceRunScope,
        disposition: WorkspaceCompletionDisposition,
    ) -> Result<(), ExecutionError> {
        let Some(factory) = self.workspace_factory.as_ref() else {
            return Ok(());
        };
        factory
            .complete(scope, disposition)
            .await
            .map_err(ExecutionError::HostEnvironment)
    }

    /// Execute a single invocation, returning the captured result or error.
    pub async fn execute(&self, invocation: Invocation) -> Result<ExecutionResult, ExecutionError> {
        let InvocationParts {
            trigger_alias,
            capture_alias,
            payload,
            deadline,
            mut metadata,
        } = invocation.into_parts();

        let run_id = metadata
            .labels()
            .get("lf.run_id")
            .cloned()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let workspace_scope = WorkspaceRunScope::new(self.ir.flow().id.0.clone(), run_id.clone());
        let resources = self
            .bind_workspace_resources(workspace_scope.clone())
            .await?;
        let connector_grants = match self.preflight_with_resources(resources.as_ref()) {
            Ok(grants) => grants,
            Err(err) => {
                let _ = self
                    .complete_workspace(workspace_scope, WorkspaceCompletionDisposition::Failed)
                    .await;
                return Err(err);
            }
        };

        metadata.insert_label("lf.run_id", run_id.clone());
        if !metadata.labels().contains_key("lf.flow_id") {
            metadata.insert_label("lf.flow_id", self.ir.flow().id.0.clone());
        }
        if !metadata.labels().contains_key("lf.trigger_alias") {
            metadata.insert_label("lf.trigger_alias", trigger_alias.clone());
        }
        if !metadata.labels().contains_key("lf.capture_alias") {
            metadata.insert_label("lf.capture_alias", capture_alias.clone());
        }

        for plugin in self.plugins.iter() {
            plugin.before_execute(&metadata);
        }

        let result = self
            .executor
            .clone()
            .with_resource_access(resources)
            .with_node_capability_grants(connector_grants.into_iter().collect())
            .run_once_with_run_id(
                self.ir.as_ref(),
                &trigger_alias,
                payload,
                &capture_alias,
                &run_id,
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

        match result {
            Ok(value @ ExecutionResult::Halt { .. }) => Ok(value),
            Ok(value) => {
                self.complete_workspace(workspace_scope, WorkspaceCompletionDisposition::Succeeded)
                    .await?;
                Ok(value)
            }
            Err(err) => {
                let _ = self
                    .complete_workspace(workspace_scope, WorkspaceCompletionDisposition::Failed)
                    .await;
                Err(err)
            }
        }
    }

    /// Resume execution from an existing checkpoint id using the checkpoint's stored halt payload.
    pub async fn resume(&self, checkpoint_id: &str) -> Result<ExecutionResult, ExecutionError> {
        self.resume_internal(checkpoint_id, None).await
    }

    /// Resume execution from an existing checkpoint id using an explicit payload override.
    pub async fn resume_with_payload(
        &self,
        checkpoint_id: &str,
        resume_payload: JsonValue,
    ) -> Result<ExecutionResult, ExecutionError> {
        self.resume_internal(checkpoint_id, Some(resume_payload))
            .await
    }

    async fn resume_internal(
        &self,
        checkpoint_id: &str,
        resume_payload_override: Option<JsonValue>,
    ) -> Result<ExecutionResult, ExecutionError> {
        let Some(store) = self.resources.checkpoint_store() else {
            return Err(ExecutionError::MissingDurabilityServices {
                missing: vec!["durability::checkpoint_store".to_string()],
            });
        };

        let fallback = CheckpointHandle {
            checkpoint_id: checkpoint_id.to_string(),
            flow_id: self.ir.flow().id.clone(),
            run_id: "unknown".to_string(),
        };

        let handles = store
            .list(CheckpointFilter {
                flow_id: Some(self.ir.flow().id.clone()),
                run_id: None,
                status: None,
            })
            .await
            .map_err(|err| map_checkpoint_error(&fallback, err))?;

        let handle = handles
            .into_iter()
            .find(|handle| handle.checkpoint_id == checkpoint_id)
            .ok_or_else(|| ExecutionError::CheckpointNotFound {
                checkpoint_id: checkpoint_id.to_string(),
            })?;

        let lease_ttl = self
            .ir
            .flow()
            .policies
            .durability
            .lease_ttl
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_secs(30));

        let lease = store
            .lease(&handle, lease_ttl)
            .await
            .map_err(|err| map_checkpoint_error(&handle, err))?;

        let record = match store.get(&handle).await {
            Ok(record) => record,
            Err(err) => {
                let _ = store.release_lease(lease).await;
                return Err(map_checkpoint_error(&handle, err));
            }
        };

        if record.version != 1 {
            let _ = store.release_lease(lease).await;
            return Err(ExecutionError::CheckpointIncompatibleVersion {
                checkpoint_id: record.checkpoint_id,
                version: record.version,
            });
        }

        let legacy_unpinned_checkpoint = record.bundle_id.is_none();
        if let Some(required_bundle_id) = record.bundle_id.as_deref() {
            let runtime_bundle_id = self.bundle_id.clone();
            if runtime_bundle_id.as_deref() != Some(required_bundle_id) {
                let _ = store.release_lease(lease).await;
                return Err(ExecutionError::CheckpointPinnedBundleUnavailable {
                    checkpoint_id: record.checkpoint_id.clone(),
                    required_bundle_id: required_bundle_id.to_string(),
                    runtime_bundle_id,
                });
            }
        } else if !self.allow_legacy_unpinned_checkpoints {
            let _ = store.release_lease(lease).await;
            return Err(ExecutionError::CheckpointPinnedBundleUnavailable {
                checkpoint_id: record.checkpoint_id.clone(),
                required_bundle_id: "<missing checkpoint.bundle_id>".to_string(),
                runtime_bundle_id: self.bundle_id.clone(),
            });
        }

        let state = match kernel_exec::durability::rehydrate_state(
            &record.state,
            self.resources.checkpoint_blob_store(),
        )
        .await
        {
            Ok(state) => state,
            Err(err) => {
                let _ = store.release_lease(lease).await;
                return Err(map_checkpoint_error(&handle, err));
            }
        };

        let frame = match decode_resume_frame(&record, &state) {
            Ok(frame) => frame,
            Err(err) => {
                let _ = store.release_lease(lease).await;
                return Err(err);
            }
        };
        let halt_payload = resume_payload_override.unwrap_or_else(|| frame.halt_payload.clone());

        let workspace_scope =
            WorkspaceRunScope::new(self.ir.flow().id.0.clone(), handle.run_id.clone());
        let resources = match self.bind_workspace_resources(workspace_scope.clone()).await {
            Ok(resources) => resources,
            Err(err) => {
                let _ = store.release_lease(lease).await;
                return Err(err);
            }
        };
        let connector_grants = match self.preflight_with_resources(resources.as_ref()) {
            Ok(grants) => grants,
            Err(err) => {
                let _ = store.release_lease(lease).await;
                return Err(err);
            }
        };

        let mut metadata = InvocationMetadata::default();
        metadata.insert_label("lf.run_id", handle.run_id.clone());
        metadata.insert_label("lf.flow_id", self.ir.flow().id.0.clone());
        metadata.insert_label("lf.trigger_alias", frame.halt_alias.clone());
        metadata.insert_label("lf.capture_alias", frame.capture_alias.clone());
        metadata.insert_resume_context(
            record.checkpoint_id.clone(),
            format!("resume-{}", uuid::Uuid::new_v4()),
            1,
            record.frontier.clone(),
            now_ms(),
        );
        metadata.insert_label(
            "lf.resume.bundle_pin",
            if legacy_unpinned_checkpoint {
                "legacy-unpinned"
            } else {
                "pinned"
            },
        );
        if let Some(bundle_id) = &record.bundle_id {
            metadata.insert_label("lf.bundle_id", bundle_id.clone());
        }

        for plugin in self.plugins.iter() {
            plugin.before_execute(&metadata);
        }

        let result = self
            .executor
            .clone()
            .with_resource_access(resources)
            .with_node_capability_grants(connector_grants.into_iter().collect())
            .resume_once(
                self.ir.as_ref(),
                &frame.halt_alias,
                halt_payload,
                &frame.pending,
                &frame.capture_alias,
                &handle.run_id,
                None,
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

        match result {
            Ok(outcome @ ExecutionResult::Halt { .. }) => {
                let _ = store.release_lease(lease).await;
                Ok(outcome)
            }
            Ok(outcome) => {
                if let Err(err) = store.ack(&handle).await {
                    let _ = store.release_lease(lease).await;
                    return Err(map_checkpoint_error(&handle, err));
                }
                let _ = store.release_lease(lease).await;
                self.complete_workspace(workspace_scope, WorkspaceCompletionDisposition::Succeeded)
                    .await?;
                Ok(outcome)
            }
            Err(err) => {
                let _ = store.release_lease(lease).await;
                Err(err)
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct ResumeFrameV1 {
    version: u32,
    halt_alias: String,
    halt_payload: JsonValue,
    capture_alias: String,
    #[serde(default)]
    pending: Vec<String>,
    #[serde(default, rename = "resume_after_ms")]
    _resume_after_ms: Option<u64>,
}

fn decode_resume_frame(
    record: &capabilities::durability::CheckpointRecord,
    state: &JsonValue,
) -> Result<ResumeFrameV1, ExecutionError> {
    let Some(frame_value) = state
        .as_object()
        .and_then(|object| object.get("resume_frame"))
    else {
        return Err(ExecutionError::CheckpointStateCorrupted {
            checkpoint_id: record.checkpoint_id.clone(),
            message: "missing state.resume_frame".to_string(),
        });
    };

    let frame: ResumeFrameV1 = serde_json::from_value(frame_value.clone()).map_err(|err| {
        ExecutionError::CheckpointStateCorrupted {
            checkpoint_id: record.checkpoint_id.clone(),
            message: format!("invalid state.resume_frame: {err}"),
        }
    })?;

    if frame.version != 1 {
        return Err(ExecutionError::CheckpointIncompatibleVersion {
            checkpoint_id: record.checkpoint_id.clone(),
            version: frame.version,
        });
    }

    Ok(frame)
}

fn default_runtime_bundle_id(ir: &ValidatedIR) -> String {
    format!("flow://{}@{}", ir.flow().id.as_str(), ir.flow().version)
}

#[cfg(not(target_arch = "wasm32"))]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(target_arch = "wasm32")]
fn now_ms() -> u64 {
    let millis = js_sys::Date::now();
    if millis.is_finite() && millis >= 0.0 {
        millis as u64
    } else {
        0
    }
}

fn map_checkpoint_error(handle: &CheckpointHandle, err: CheckpointError) -> ExecutionError {
    match err {
        CheckpointError::NotFound => ExecutionError::CheckpointNotFound {
            checkpoint_id: handle.checkpoint_id.clone(),
        },
        CheckpointError::LeaseConflict | CheckpointError::LeaseExpired => {
            ExecutionError::CheckpointLeaseConflict {
                checkpoint_id: handle.checkpoint_id.clone(),
            }
        }
        CheckpointError::Storage(message) => ExecutionError::CheckpointStateCorrupted {
            checkpoint_id: handle.checkpoint_id.clone(),
            message,
        },
    }
}

fn collect_missing_durability_services(
    ir: &ValidatedIR,
    resources: &dyn ResourceAccess,
) -> Vec<String> {
    let flow = ir.flow();
    let mode = flow.policies.durability.mode;
    let has_halts = flow.nodes.iter().any(|node| node.durability.halts);

    let mut missing = Vec::new();

    if mode != DurabilityMode::Off {
        if resources.checkpoint_store().is_none() {
            missing.push("durability::checkpoint_store".to_string());
        }
    }

    if has_halts {
        let needs_scheduler = flow
            .nodes
            .iter()
            .any(|node| node.identifier == "std.timer.wait");
        let needs_signal = flow.nodes.iter().any(|node| {
            node.identifier == "std.callback.wait" || node.identifier == "std.hitl.approval"
        });

        if needs_scheduler && resources.resume_scheduler().is_none() {
            missing.push("durability::resume_scheduler".to_string());
        }
        if needs_signal && resources.resume_signal_source().is_none() {
            missing.push("durability::resume_signal_source".to_string());
        }
    }

    if mode != DurabilityMode::Off
        && flow.policies.durability.blob_threshold_bytes.is_some()
        && resources.checkpoint_blob_store().is_none()
    {
        missing.push("durability::checkpoint_blob_store".to_string());
    }

    missing
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
    use async_trait::async_trait;
    use cap_workspace_fs::{FsWorkspaceConfig, FsWorkspaceFactory};
    use capabilities::durability::{
        CheckpointError, CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStore,
        ResumeScheduler, ResumeSignalSource, ResumeToken, ScheduleError, ScheduleId,
        ScheduleStatus, TokenConfig, TokenError,
    };
    use capabilities::workspace::{
        WorkspaceFactory, WorkspacePolicy, WorkspaceRunScope, WorkspaceWriteOptions,
    };
    use dag_core::prelude::*;
    use kernel_exec::{NodeRegistry, RegistryResolver};
    use kernel_plan::validate;
    use std::collections::HashMap;
    use std::fs;
    use std::sync::{Arc as StdArc, Mutex};
    use std::time::Duration;
    use tempfile::tempdir;

    struct StubCheckpointStore;

    impl capabilities::Capability for StubCheckpointStore {
        fn name(&self) -> &'static str {
            "checkpoint.stub"
        }
    }

    #[async_trait]
    impl CheckpointStore for StubCheckpointStore {
        async fn put(&self, record: CheckpointRecord) -> Result<CheckpointHandle, CheckpointError> {
            Ok(CheckpointHandle {
                checkpoint_id: record.checkpoint_id,
                flow_id: record.flow_id,
                run_id: record.run_id,
            })
        }

        async fn get(
            &self,
            _handle: &CheckpointHandle,
        ) -> Result<CheckpointRecord, CheckpointError> {
            Err(CheckpointError::NotFound)
        }

        async fn ack(&self, _handle: &CheckpointHandle) -> Result<(), CheckpointError> {
            Ok(())
        }

        async fn lease(
            &self,
            _handle: &CheckpointHandle,
            _ttl: Duration,
        ) -> Result<capabilities::durability::Lease, CheckpointError> {
            Err(CheckpointError::LeaseConflict)
        }

        async fn release_lease(
            &self,
            _lease: capabilities::durability::Lease,
        ) -> Result<(), CheckpointError> {
            Ok(())
        }

        async fn list(
            &self,
            _filter: CheckpointFilter,
        ) -> Result<Vec<CheckpointHandle>, CheckpointError> {
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct MemoryCheckpointStore {
        records: Mutex<HashMap<String, CheckpointRecord>>,
        leases: Mutex<HashMap<String, capabilities::durability::Lease>>,
    }

    impl MemoryCheckpointStore {
        fn now_ms() -> u64 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX)
        }

        fn is_expired(record: &CheckpointRecord, now_ms: u64) -> bool {
            record
                .ttl_ms
                .map(|ttl_ms| record.created_at_ms.saturating_add(ttl_ms) <= now_ms)
                .unwrap_or(false)
        }
    }

    impl capabilities::Capability for MemoryCheckpointStore {
        fn name(&self) -> &'static str {
            "checkpoint.memory"
        }
    }

    #[async_trait]
    impl CheckpointStore for MemoryCheckpointStore {
        async fn put(&self, record: CheckpointRecord) -> Result<CheckpointHandle, CheckpointError> {
            let handle = CheckpointHandle {
                checkpoint_id: record.checkpoint_id.clone(),
                flow_id: record.flow_id.clone(),
                run_id: record.run_id.clone(),
            };
            self.records
                .lock()
                .expect("memory checkpoint lock")
                .insert(record.checkpoint_id.clone(), record);
            Ok(handle)
        }

        async fn get(
            &self,
            handle: &CheckpointHandle,
        ) -> Result<CheckpointRecord, CheckpointError> {
            self.records
                .lock()
                .expect("memory checkpoint lock")
                .get(&handle.checkpoint_id)
                .cloned()
                .ok_or(CheckpointError::NotFound)
        }

        async fn ack(&self, handle: &CheckpointHandle) -> Result<(), CheckpointError> {
            self.records
                .lock()
                .expect("memory checkpoint lock")
                .remove(&handle.checkpoint_id);
            self.leases
                .lock()
                .expect("memory lease lock")
                .remove(&handle.checkpoint_id);
            Ok(())
        }

        async fn lease(
            &self,
            handle: &CheckpointHandle,
            ttl: Duration,
        ) -> Result<capabilities::durability::Lease, CheckpointError> {
            if !self
                .records
                .lock()
                .expect("memory checkpoint lock")
                .contains_key(&handle.checkpoint_id)
            {
                return Err(CheckpointError::NotFound);
            }

            let now_ms = Self::now_ms();
            let mut leases = self.leases.lock().expect("memory lease lock");
            if let Some(existing) = leases.get(&handle.checkpoint_id)
                && existing.expires_at_ms > now_ms
            {
                return Err(CheckpointError::LeaseConflict);
            }

            let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1);
            let lease = capabilities::durability::Lease {
                lease_id: format!("lease:{}:{now_ms}", handle.checkpoint_id),
                expires_at_ms: now_ms.saturating_add(ttl_ms),
            };
            leases.insert(handle.checkpoint_id.clone(), lease.clone());
            Ok(lease)
        }

        async fn release_lease(
            &self,
            lease: capabilities::durability::Lease,
        ) -> Result<(), CheckpointError> {
            let mut leases = self.leases.lock().expect("memory lease lock");
            if let Some(key) = leases.iter().find_map(|(key, existing)| {
                (existing.lease_id == lease.lease_id).then_some(key.clone())
            }) {
                leases.remove(&key);
            }
            Ok(())
        }

        async fn list(
            &self,
            filter: CheckpointFilter,
        ) -> Result<Vec<CheckpointHandle>, CheckpointError> {
            let now_ms = Self::now_ms();
            let records = self.records.lock().expect("memory checkpoint lock");
            let mut handles = Vec::new();
            for record in records.values() {
                if let Some(flow_id) = &filter.flow_id
                    && &record.flow_id != flow_id
                {
                    continue;
                }
                if let Some(run_id) = &filter.run_id
                    && &record.run_id != run_id
                {
                    continue;
                }

                if let Some(status) = filter.status {
                    let expired = Self::is_expired(record, now_ms);
                    match status {
                        capabilities::durability::CheckpointStatus::Active if expired => continue,
                        capabilities::durability::CheckpointStatus::Expired if !expired => continue,
                        capabilities::durability::CheckpointStatus::Completed => continue,
                        _ => {}
                    }
                }

                handles.push(CheckpointHandle {
                    checkpoint_id: record.checkpoint_id.clone(),
                    flow_id: record.flow_id.clone(),
                    run_id: record.run_id.clone(),
                });
            }

            Ok(handles)
        }
    }

    struct StubResumeScheduler;

    impl capabilities::Capability for StubResumeScheduler {
        fn name(&self) -> &'static str {
            "resume.scheduler.stub"
        }
    }

    #[async_trait]
    impl ResumeScheduler for StubResumeScheduler {
        async fn schedule_at(
            &self,
            _handle: CheckpointHandle,
            _at_ms: u64,
        ) -> Result<ScheduleId, ScheduleError> {
            Ok(ScheduleId("schedule".to_string()))
        }

        async fn schedule_after(
            &self,
            _handle: CheckpointHandle,
            _delay: Duration,
        ) -> Result<ScheduleId, ScheduleError> {
            Ok(ScheduleId("schedule".to_string()))
        }

        async fn cancel(&self, _schedule_id: ScheduleId) -> Result<(), ScheduleError> {
            Ok(())
        }

        async fn status(&self, _schedule_id: ScheduleId) -> Result<ScheduleStatus, ScheduleError> {
            Ok(ScheduleStatus::Pending { fires_at_ms: 0 })
        }
    }

    struct StubResumeSignalSource;

    impl capabilities::Capability for StubResumeSignalSource {
        fn name(&self) -> &'static str {
            "resume.signal.stub"
        }
    }

    #[async_trait]
    impl ResumeSignalSource for StubResumeSignalSource {
        async fn create_token(
            &self,
            _handle: &CheckpointHandle,
            _config: TokenConfig,
        ) -> Result<ResumeToken, TokenError> {
            Ok(ResumeToken(format!("token-{}", uuid::Uuid::new_v4())))
        }

        async fn resolve_token(
            &self,
            _token: &ResumeToken,
        ) -> Result<CheckpointHandle, TokenError> {
            Err(TokenError::NotFound)
        }

        async fn revoke_token(&self, _token: &ResumeToken) -> Result<(), TokenError> {
            Ok(())
        }
    }

    fn resource_bag_with_checkpoint() -> ResourceBag {
        ResourceBag::new().with_checkpoint_store(Arc::new(StubCheckpointStore))
    }

    struct StubSql;

    impl capabilities::Capability for StubSql {
        fn name(&self) -> &'static str {
            "sql.stub"
        }
    }

    #[async_trait]
    impl capabilities::sql::SqlRead for StubSql {
        async fn query(
            &self,
            _statement: capabilities::sql::SqlStatement,
        ) -> Result<capabilities::sql::SqlQueryResult, capabilities::sql::SqlError> {
            Ok(capabilities::sql::SqlQueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                rows_returned: 0,
                cursor: None,
            })
        }

        fn capability_info(&self) -> capabilities::sql::SqlCapabilityInfo {
            capabilities::sql::SqlCapabilityInfo::default()
        }
    }

    #[async_trait]
    impl capabilities::sql::SqlWrite for StubSql {
        async fn execute(
            &self,
            _statement: capabilities::sql::SqlStatement,
        ) -> Result<capabilities::sql::SqlExecuteResult, capabilities::sql::SqlError> {
            Ok(capabilities::sql::SqlExecuteResult {
                rows_affected: Some(0),
                last_insert_id: None,
            })
        }

        fn capability_info(&self) -> capabilities::sql::SqlCapabilityInfo {
            capabilities::sql::SqlCapabilityInfo::default()
        }
    }

    #[async_trait]
    impl capabilities::sql::SqlAdmin for StubSql {
        async fn execute_ddl(
            &self,
            _statement: capabilities::sql::SqlStatement,
        ) -> Result<capabilities::sql::SqlExecuteResult, capabilities::sql::SqlError> {
            Ok(capabilities::sql::SqlExecuteResult {
                rows_affected: None,
                last_insert_id: None,
            })
        }

        fn capability_info(&self) -> capabilities::sql::SqlCapabilityInfo {
            capabilities::sql::SqlCapabilityInfo::default()
        }
    }

    fn preflight_runtime_for_hints(
        flow_id: &str,
        effects: Effects,
        effect_hints: &'static [&'static str],
        resources: ResourceBag,
    ) -> HostRuntime {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn("tests::node", |value: JsonValue| async move { Ok(value) })
            .unwrap();

        let mut builder = FlowBuilder::new(flow_id, Version::new(1, 0, 0), Profile::Dev);
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
        let node = builder
            .add_node(
                "node",
                &NodeSpec::inline_with_hints(
                    "tests::node",
                    "Node",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    effects,
                    Determinism::BestEffort,
                    None,
                    &[],
                    effect_hints,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &node);

        let mut flow = builder.build();
        flow.nodes
            .iter_mut()
            .find(|node| node.alias == "node")
            .expect("preflight node")
            .idempotency
            .key = Some("idempotency".to_string());

        let ir = Arc::new(validate(&flow).expect("flow validates"));
        HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir).with_resource_bag(resources)
    }

    #[derive(Default)]
    struct RecordingWorkspaceFactory {
        opened: StdArc<Mutex<Vec<WorkspaceRunScope>>>,
        completed: StdArc<Mutex<Vec<(WorkspaceRunScope, WorkspaceCompletionDisposition)>>>,
    }

    impl RecordingWorkspaceFactory {
        fn opened(&self) -> Vec<WorkspaceRunScope> {
            self.opened.lock().expect("opened lock").clone()
        }

        fn completed(&self) -> Vec<(WorkspaceRunScope, WorkspaceCompletionDisposition)> {
            self.completed.lock().expect("completed lock").clone()
        }
    }

    struct NoopWorkspace;

    impl capabilities::Capability for NoopWorkspace {
        fn name(&self) -> &'static str {
            "workspace.noop"
        }
    }

    #[async_trait]
    impl Workspace for NoopWorkspace {
        async fn read_normalized(
            &self,
            _normalized_path: &str,
        ) -> Result<
            Option<capabilities::workspace::WorkspaceReadResult>,
            capabilities::workspace::WorkspaceError,
        > {
            Ok(None)
        }

        async fn write_normalized(
            &self,
            normalized_path: &str,
            data: &[u8],
            _options: capabilities::workspace::WorkspaceWriteOptions,
        ) -> Result<
            capabilities::workspace::WorkspaceWriteResult,
            capabilities::workspace::WorkspaceError,
        > {
            Ok(capabilities::workspace::WorkspaceWriteResult {
                path: normalized_path.to_string(),
                size_bytes: data.len() as u64,
                updated_at_ms: 0,
            })
        }

        async fn list_normalized(
            &self,
            _options: capabilities::workspace::WorkspaceListOptions,
        ) -> Result<
            Vec<capabilities::workspace::WorkspaceEntry>,
            capabilities::workspace::WorkspaceError,
        > {
            Ok(Vec::new())
        }

        async fn delete_normalized(
            &self,
            _normalized_path: &str,
        ) -> Result<
            capabilities::workspace::WorkspaceDeleteResult,
            capabilities::workspace::WorkspaceError,
        > {
            Ok(capabilities::workspace::WorkspaceDeleteResult { deleted: false })
        }
    }

    #[async_trait]
    impl WorkspaceFactory for RecordingWorkspaceFactory {
        async fn open(&self, scope: WorkspaceRunScope) -> anyhow::Result<Arc<dyn Workspace>> {
            self.opened.lock().expect("opened lock").push(scope);
            Ok(Arc::new(NoopWorkspace))
        }

        async fn complete(
            &self,
            scope: WorkspaceRunScope,
            disposition: WorkspaceCompletionDisposition,
        ) -> anyhow::Result<()> {
            self.completed
                .lock()
                .expect("completed lock")
                .push((scope, disposition));
            Ok(())
        }
    }

    fn single_stage_flow(flow_name: &str, stage_spec: &NodeSpec) -> Arc<ValidatedIR> {
        let mut builder = FlowBuilder::new(flow_name, Version::new(1, 0, 0), Profile::Dev);
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
        let stage = builder.add_node("stage", stage_spec).unwrap();
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
        builder.connect(&trigger, &stage);
        builder.connect(&stage, &capture);
        Arc::new(validate(&builder.build()).expect("flow validates"))
    }

    fn retained_fs_workspace_factory(root: &std::path::Path) -> FsWorkspaceFactory {
        FsWorkspaceFactory::new(FsWorkspaceConfig {
            root: root.to_path_buf(),
            policy: WorkspacePolicy {
                retain_completed_for: Some(Duration::from_secs(60)),
                ..WorkspacePolicy::default()
            },
        })
    }

    fn invocation_with_run_id(payload: serde_json::Value, run_id: &str) -> Invocation {
        let mut invocation = Invocation::new("trigger", "capture", payload);
        invocation
            .metadata_mut()
            .insert_label("lf.run_id", run_id.to_string());
        invocation
    }

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

        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(resource_bag_with_checkpoint());
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
            ExecutionResult::Halt { .. } => panic!("unexpected halt result"),
        }
    }

    #[tokio::test]
    async fn workspace_factory_opens_and_completes_for_terminal_execute() {
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

        let mut builder = FlowBuilder::new(
            "workspace_terminal_execute",
            Version::new(1, 0, 0),
            Profile::Dev,
        );
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

        let workspace_factory = Arc::new(RecordingWorkspaceFactory::default());
        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), Arc::clone(&ir))
            .with_resource_bag(resource_bag_with_checkpoint())
            .with_workspace_factory(workspace_factory.clone());

        let result = runtime
            .execute(Invocation::new(
                "trigger",
                "capture",
                serde_json::json!({"ok": true}),
            ))
            .await
            .expect("execution succeeds");
        assert!(matches!(result, ExecutionResult::Value(_)));

        let opened = workspace_factory.opened();
        assert_eq!(opened.len(), 1);
        assert_eq!(opened[0].flow_id, ir.flow().id.0.clone());
        assert!(!opened[0].run_id.is_empty());

        let completed = workspace_factory.completed();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].0, opened[0]);
        assert_eq!(completed[0].1, WorkspaceCompletionDisposition::Succeeded);
    }

    #[tokio::test]
    async fn runtime_populates_lattice_metadata() {
        let run_ids: StdArc<Mutex<Vec<String>>> = StdArc::new(Mutex::new(Vec::new()));
        let flow_ids: StdArc<Mutex<Vec<String>>> = StdArc::new(Mutex::new(Vec::new()));
        let trigger_aliases: StdArc<Mutex<Vec<String>>> = StdArc::new(Mutex::new(Vec::new()));
        let capture_aliases: StdArc<Mutex<Vec<String>>> = StdArc::new(Mutex::new(Vec::new()));

        struct RecordingPlugin {
            run_ids: StdArc<Mutex<Vec<String>>>,
            flow_ids: StdArc<Mutex<Vec<String>>>,
            trigger_aliases: StdArc<Mutex<Vec<String>>>,
            capture_aliases: StdArc<Mutex<Vec<String>>>,
        }

        impl EnvironmentPlugin for RecordingPlugin {
            fn before_execute(&self, metadata: &InvocationMetadata) {
                self.run_ids.lock().unwrap().push(
                    metadata
                        .labels()
                        .get("lf.run_id")
                        .cloned()
                        .unwrap_or_default(),
                );
                self.flow_ids.lock().unwrap().push(
                    metadata
                        .labels()
                        .get("lf.flow_id")
                        .cloned()
                        .unwrap_or_default(),
                );
                self.trigger_aliases.lock().unwrap().push(
                    metadata
                        .labels()
                        .get("lf.trigger_alias")
                        .cloned()
                        .unwrap_or_default(),
                );
                self.capture_aliases.lock().unwrap().push(
                    metadata
                        .labels()
                        .get("lf.capture_alias")
                        .cloned()
                        .unwrap_or_default(),
                );
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

        let mut builder = FlowBuilder::new("runtime_meta", Version::new(1, 0, 0), Profile::Dev);
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
        let expected_flow_id = ir.flow().id.0.clone();

        let plugin = RecordingPlugin {
            run_ids: StdArc::clone(&run_ids),
            flow_ids: StdArc::clone(&flow_ids),
            trigger_aliases: StdArc::clone(&trigger_aliases),
            capture_aliases: StdArc::clone(&capture_aliases),
        };

        let runtime = HostRuntime::with_plugins(
            FlowExecutor::new(Arc::new(registry)),
            ir,
            vec![Arc::new(plugin)],
        )
        .with_resource_bag(resource_bag_with_checkpoint());

        runtime
            .execute(Invocation::new(
                "trigger",
                "capture",
                serde_json::json!({"ok": true}),
            ))
            .await
            .expect("exec ok");
        runtime
            .execute(Invocation::new(
                "trigger",
                "capture",
                serde_json::json!({"ok": true}),
            ))
            .await
            .expect("exec ok");

        let run_ids = run_ids.lock().unwrap().clone();
        assert_eq!(run_ids.len(), 2);
        assert!(!run_ids[0].is_empty());
        assert_ne!(run_ids[0], run_ids[1]);

        let flow_ids = flow_ids.lock().unwrap().clone();
        assert_eq!(flow_ids, vec![expected_flow_id.clone(), expected_flow_id]);

        let trigger_aliases = trigger_aliases.lock().unwrap().clone();
        assert_eq!(
            trigger_aliases,
            vec!["trigger".to_string(), "trigger".to_string()]
        );

        let capture_aliases = capture_aliases.lock().unwrap().clone();
        assert_eq!(
            capture_aliases,
            vec!["capture".to_string(), "capture".to_string()]
        );
    }

    #[test]
    fn resume_metadata_includes_checkpoint_and_resume_ids() {
        let mut invocation = Invocation::new("trigger", "capture", serde_json::json!({"ok": true}));
        let frontier = capabilities::durability::FlowFrontier {
            completed: vec![capabilities::durability::FrontierEntry {
                node_alias: "alpha".to_string(),
                output_port: "out".to_string(),
                cursor: None,
            }],
            pending: vec!["beta".to_string()],
        };

        invocation.metadata_mut().insert_resume_context(
            "ckpt-1",
            "resume-1",
            1,
            frontier,
            1_700_000_000_000,
        );

        let labels = invocation.metadata().labels();
        assert_eq!(labels.get("lf.checkpoint_id"), Some(&"ckpt-1".to_string()));
        assert_eq!(labels.get("lf.resume_id"), Some(&"resume-1".to_string()));
        assert_eq!(labels.get("lf.resume_attempt"), Some(&"1".to_string()));

        let extensions = invocation.metadata().extensions();
        assert!(extensions.contains_key("lf.resume"));
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
            HostRuntime::with_plugins(FlowExecutor::new(Arc::new(registry)), ir, vec![plugin])
                .with_resource_bag(resource_bag_with_checkpoint());
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
            .with_resource_bag(resource_bag_with_checkpoint());
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

        let resources =
            resource_bag_with_checkpoint().with_kv(Arc::new(capabilities::kv::MemoryKv::new()));
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
            ExecutionResult::Halt { .. } => panic!("unexpected halt result"),
        }
    }

    #[tokio::test]
    async fn preflight_fails_when_checkpoint_store_missing() {
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

        let mut builder =
            FlowBuilder::new("preflight_checkpoint", Version::new(1, 0, 0), Profile::Dev);
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
        flow.policies.durability.mode = DurabilityMode::Strong;
        let ir = Arc::new(validate(&flow).expect("flow validates"));

        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(ResourceBag::new());
        let invocation = Invocation::new("trigger", "sink", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingDurabilityServices { missing }) => {
                assert!(missing.contains(&"durability::checkpoint_store".to_string()));
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn preflight_fails_when_checkpoint_store_missing_in_partial_mode() {
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

        let mut builder = FlowBuilder::new(
            "preflight_checkpoint_partial",
            Version::new(1, 0, 0),
            Profile::Dev,
        );
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
        flow.policies.durability.mode = DurabilityMode::Partial;
        let ir = Arc::new(validate(&flow).expect("flow validates"));

        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(ResourceBag::new());
        let invocation = Invocation::new("trigger", "sink", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingDurabilityServices { missing }) => {
                assert!(missing.contains(&"durability::checkpoint_store".to_string()));
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn preflight_fails_when_resume_scheduler_missing() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "std.timer.wait",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();

        let mut builder = FlowBuilder::new("preflight_timer", Version::new(1, 0, 0), Profile::Dev);
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
        let wait = builder
            .add_node(
                "wait",
                &NodeSpec::inline(
                    "std.timer.wait",
                    "TimerWait",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::BestEffort,
                    None,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &wait);

        let mut flow = builder.build();
        if let Some(node) = flow.nodes.iter_mut().find(|node| node.alias == "wait") {
            node.durability.halts = true;
        }
        let ir = Arc::new(validate(&flow).expect("flow validates"));

        let resources = resource_bag_with_checkpoint();
        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(resources);
        let invocation = Invocation::new("trigger", "wait", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingDurabilityServices { missing }) => {
                assert!(missing.contains(&"durability::resume_scheduler".to_string()));
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn preflight_fails_when_resume_signal_source_missing() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "std.callback.wait",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();

        let mut builder =
            FlowBuilder::new("preflight_callback", Version::new(1, 0, 0), Profile::Dev);
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
        let callback = builder
            .add_node(
                "callback",
                &NodeSpec::inline(
                    "std.callback.wait",
                    "CallbackWait",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::BestEffort,
                    None,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &callback);

        let mut flow = builder.build();
        if let Some(node) = flow.nodes.iter_mut().find(|node| node.alias == "callback") {
            node.durability.halts = true;
        }
        let ir = Arc::new(validate(&flow).expect("flow validates"));

        let resources =
            resource_bag_with_checkpoint().with_resume_scheduler(Arc::new(StubResumeScheduler));
        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(resources);
        let invocation = Invocation::new("trigger", "callback", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingDurabilityServices { missing }) => {
                assert!(missing.contains(&"durability::resume_signal_source".to_string()));
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn preflight_fails_when_checkpoint_blob_store_missing() {
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

        let mut builder = FlowBuilder::new("preflight_blob", Version::new(1, 0, 0), Profile::Dev);
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
        flow.policies.durability.mode = DurabilityMode::Strong;
        flow.policies.durability.blob_threshold_bytes = Some(1);
        let ir = Arc::new(validate(&flow).expect("flow validates"));

        let resources = resource_bag_with_checkpoint();
        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(resources);
        let invocation = Invocation::new("trigger", "sink", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingDurabilityServices { missing }) => {
                assert!(missing.contains(&"durability::checkpoint_blob_store".to_string()));
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn timer_wait_halts_with_schedule() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: stdlib::timer::TimerWaitInput| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::capture",
                |value: stdlib::timer::TimerWaitOutput| async move { Ok(value) },
            )
            .unwrap();
        stdlib::timer::timer_wait_register(&mut registry).unwrap();

        let mut builder = FlowBuilder::new("timer_wait", Version::new(1, 0, 0), Profile::Dev);
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
        let wait = builder
            .add_node("wait", stdlib::timer::timer_wait_node_spec())
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
        builder.connect(&trigger, &wait);
        builder.connect(&wait, &capture);

        let ir = Arc::new(validate(&builder.build()).expect("flow validates"));
        let resources =
            resource_bag_with_checkpoint().with_resume_scheduler(Arc::new(StubResumeScheduler));
        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(resources);

        let payload = serde_json::json!({"ok": true});
        let input = stdlib::timer::TimerWaitInput {
            duration: Some(Duration::from_millis(10)),
            until: None,
            payload: payload.clone(),
        };
        let invocation =
            Invocation::new("trigger", "capture", serde_json::to_value(input).unwrap());

        let result = runtime.execute(invocation).await.expect("exec ok");
        let output = match result {
            ExecutionResult::Halt { alias, payload } => {
                assert_eq!(alias, "wait");
                serde_json::from_value::<stdlib::timer::TimerWaitOutput>(payload)
                    .expect("decode halt output")
            }
            ExecutionResult::Value(_) => panic!("unexpected value"),
            ExecutionResult::Stream(_) => panic!("unexpected stream"),
        };

        assert_eq!(output.payload, payload);
        assert!(output.scheduled_at_ms > 0);
    }

    #[tokio::test]
    async fn timer_wait_checkpoint_resumes_with_minimal_frame() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: stdlib::timer::TimerWaitInput| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::capture_resume",
                |value: stdlib::timer::TimerWaitOutput| async move {
                    Ok(serde_json::json!({
                        "resumed": true,
                        "scheduled_at_ms": value.scheduled_at_ms,
                        "payload": value.payload,
                    }))
                },
            )
            .unwrap();
        stdlib::timer::timer_wait_register(&mut registry).unwrap();

        let mut builder = FlowBuilder::new("timer_resume", Version::new(1, 0, 0), Profile::Dev);
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
        let wait = builder
            .add_node("wait", stdlib::timer::timer_wait_node_spec())
            .unwrap();
        let capture = builder
            .add_node(
                "capture",
                &NodeSpec::inline(
                    "tests::capture_resume",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &wait);
        builder.connect(&wait, &capture);

        let ir = Arc::new(validate(&builder.build()).expect("flow validates"));
        let checkpoint_store = Arc::new(MemoryCheckpointStore::default());
        let resources = ResourceBag::new()
            .with_checkpoint_store(Arc::clone(&checkpoint_store))
            .with_resume_scheduler(Arc::new(StubResumeScheduler));
        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), Arc::clone(&ir))
            .with_resource_bag(resources);

        let payload = serde_json::json!({"ok": true});
        let input = stdlib::timer::TimerWaitInput {
            duration: Some(Duration::from_millis(10)),
            until: None,
            payload: payload.clone(),
        };
        let invocation =
            Invocation::new("trigger", "capture", serde_json::to_value(input).unwrap());

        let halted = runtime.execute(invocation).await.expect("exec ok");
        let halted_payload = match halted {
            ExecutionResult::Halt { alias, payload } => {
                assert_eq!(alias, "wait");
                payload
            }
            _ => panic!("unexpected result variant"),
        };

        let checkpoint_id = halted_payload
            .get("checkpoint_id")
            .and_then(|value| value.as_str())
            .expect("checkpoint_id in halt payload")
            .to_string();

        let handles = checkpoint_store
            .list(CheckpointFilter {
                flow_id: Some(ir.flow().id.clone()),
                run_id: None,
                status: None,
            })
            .await
            .expect("list checkpoints");
        assert_eq!(handles.len(), 1);

        let record = checkpoint_store
            .get(&handles[0])
            .await
            .expect("checkpoint exists");
        let expected_bundle_id = format!("flow://{}@{}", ir.flow().id.as_str(), ir.flow().version);
        assert_eq!(
            record.bundle_id.as_deref(),
            Some(expected_bundle_id.as_str())
        );
        let resume_frame = record
            .state
            .data
            .get("resume_frame")
            .expect("resume frame in state data");
        assert!(resume_frame.get("halt_alias").is_some());
        assert!(resume_frame.get("halt_payload").is_some());
        assert!(resume_frame.get("capture_alias").is_some());
        assert!(record.state.data.get("node_outputs").is_none());
        assert!(record.state.data.get("history").is_none());

        let resumed = runtime.resume(&checkpoint_id).await.expect("resume ok");
        match resumed {
            ExecutionResult::Value(value) => {
                assert_eq!(value.get("resumed").and_then(|v| v.as_bool()), Some(true));
                assert_eq!(value.get("payload"), Some(&payload));
            }
            _ => panic!("unexpected resume result variant"),
        }

        let remaining = checkpoint_store
            .list(CheckpointFilter {
                flow_id: Some(ir.flow().id.clone()),
                run_id: None,
                status: None,
            })
            .await
            .expect("list checkpoints after resume");
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn workspace_factory_reuses_checkpoint_run_id_across_resume() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: stdlib::timer::TimerWaitInput| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::capture_resume",
                |value: stdlib::timer::TimerWaitOutput| async move {
                    Ok(serde_json::json!({
                        "resumed": true,
                        "scheduled_at_ms": value.scheduled_at_ms,
                        "payload": value.payload,
                    }))
                },
            )
            .unwrap();
        stdlib::timer::timer_wait_register(&mut registry).unwrap();

        let mut builder =
            FlowBuilder::new("timer_resume_scope", Version::new(1, 0, 0), Profile::Dev);
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
        let wait = builder
            .add_node("wait", stdlib::timer::timer_wait_node_spec())
            .unwrap();
        let capture = builder
            .add_node(
                "capture",
                &NodeSpec::inline(
                    "tests::capture_resume",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &wait);
        builder.connect(&wait, &capture);

        let ir = Arc::new(validate(&builder.build()).expect("flow validates"));
        let checkpoint_store = Arc::new(MemoryCheckpointStore::default());
        let workspace_factory = Arc::new(RecordingWorkspaceFactory::default());
        let resources = ResourceBag::new()
            .with_checkpoint_store(Arc::clone(&checkpoint_store))
            .with_resume_scheduler(Arc::new(StubResumeScheduler))
            .with_workspace(Arc::new(NoopWorkspace));
        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), Arc::clone(&ir))
            .with_resource_bag(resources)
            .with_workspace_factory(workspace_factory.clone());

        let input = stdlib::timer::TimerWaitInput {
            duration: Some(Duration::from_millis(10)),
            until: None,
            payload: serde_json::json!({"ok": true}),
        };
        let halted = runtime
            .execute(Invocation::new(
                "trigger",
                "capture",
                serde_json::to_value(input).unwrap(),
            ))
            .await
            .expect("execution halts");
        let checkpoint_id = match halted {
            ExecutionResult::Halt { payload, .. } => payload
                .get("checkpoint_id")
                .and_then(|value| value.as_str())
                .expect("checkpoint id")
                .to_string(),
            _ => panic!("expected halt result"),
        };

        let handles = checkpoint_store
            .list(CheckpointFilter {
                flow_id: Some(ir.flow().id.clone()),
                run_id: None,
                status: None,
            })
            .await
            .expect("list checkpoints");
        assert_eq!(handles.len(), 1);
        let checkpoint_run_id = handles[0].run_id.clone();

        let opened = workspace_factory.opened();
        assert_eq!(opened.len(), 1);
        assert_eq!(opened[0].flow_id, ir.flow().id.0.clone());
        assert_eq!(opened[0].run_id, checkpoint_run_id);
        assert!(workspace_factory.completed().is_empty());

        let resumed = runtime.resume(&checkpoint_id).await.expect("resume ok");
        match resumed {
            ExecutionResult::Value(value) => {
                assert_eq!(value.get("resumed").and_then(|v| v.as_bool()), Some(true));
            }
            _ => panic!("expected resumed value"),
        }

        let opened = workspace_factory.opened();
        assert_eq!(opened.len(), 2);
        assert_eq!(opened[1], opened[0]);

        let completed = workspace_factory.completed();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].0, opened[0]);
        assert_eq!(completed[0].1, WorkspaceCompletionDisposition::Succeeded);
    }

    #[tokio::test]
    async fn stdlib_workspace_write_executes_against_fs_factory() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: stdlib::workspace::WorkspaceWriteInput| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::capture",
                |value: stdlib::workspace::WorkspaceWriteOutput| async move { Ok(value) },
            )
            .unwrap();
        stdlib::workspace::workspace_write_register(&mut registry).unwrap();

        let ir = single_stage_flow(
            "workspace_write_native",
            stdlib::workspace::workspace_write_node_spec(),
        );
        let temp = tempdir().expect("tempdir");
        let factory = retained_fs_workspace_factory(temp.path());
        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), Arc::clone(&ir))
            .with_resource_bag(resource_bag_with_checkpoint())
            .with_workspace_factory(Arc::new(factory.clone()));

        let run_id = "run-write";
        let input = stdlib::workspace::WorkspaceWriteInput {
            path: "./artifacts//report.txt".to_string(),
            bytes: b"hello".to_vec(),
        };
        let result = runtime
            .execute(invocation_with_run_id(
                serde_json::to_value(input).unwrap(),
                run_id,
            ))
            .await
            .expect("execution ok");
        let value = match result {
            ExecutionResult::Value(value) => value,
            ExecutionResult::Halt { .. } => panic!("unexpected halt result"),
            ExecutionResult::Stream(_) => panic!("unexpected stream result"),
        };
        let output: stdlib::workspace::WorkspaceWriteOutput =
            serde_json::from_value(value).expect("decode write output");
        assert_eq!(output.path, "artifacts/report.txt");
        assert_eq!(output.size_bytes, 5);

        let scope = WorkspaceRunScope::new(ir.flow().id.0.clone(), run_id);
        let stored = factory.run_root_path(&scope).join("artifacts/report.txt");
        assert_eq!(fs::read(&stored).expect("read stored artifact"), b"hello");
    }

    #[tokio::test]
    async fn stdlib_workspace_read_returns_soft_miss_via_runtime() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: stdlib::workspace::WorkspaceReadInput| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::capture",
                |value: stdlib::workspace::WorkspaceReadOutput| async move { Ok(value) },
            )
            .unwrap();
        stdlib::workspace::workspace_read_register(&mut registry).unwrap();

        let ir = single_stage_flow(
            "workspace_read_native",
            stdlib::workspace::workspace_read_node_spec(),
        );
        let temp = tempdir().expect("tempdir");
        let factory = retained_fs_workspace_factory(temp.path());
        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), Arc::clone(&ir))
            .with_resource_bag(resource_bag_with_checkpoint())
            .with_workspace_factory(Arc::new(factory));

        let result = runtime
            .execute(invocation_with_run_id(
                serde_json::to_value(stdlib::workspace::WorkspaceReadInput {
                    path: "missing.txt".to_string(),
                })
                .unwrap(),
                "run-read-miss",
            ))
            .await
            .expect("execution ok");
        let value = match result {
            ExecutionResult::Value(value) => value,
            ExecutionResult::Halt { .. } => panic!("unexpected halt result"),
            ExecutionResult::Stream(_) => panic!("unexpected stream result"),
        };
        let output: stdlib::workspace::WorkspaceReadOutput =
            serde_json::from_value(value).expect("decode read output");
        assert_eq!(output.path, "missing.txt");
        assert!(!output.found);
        assert!(output.value.is_none());
    }

    #[tokio::test]
    async fn stdlib_workspace_list_reads_preseeded_entries_via_runtime() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: stdlib::workspace::WorkspaceListInput| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::capture",
                |value: stdlib::workspace::WorkspaceListOutput| async move { Ok(value) },
            )
            .unwrap();
        stdlib::workspace::workspace_list_register(&mut registry).unwrap();

        let ir = single_stage_flow(
            "workspace_list_native",
            stdlib::workspace::workspace_list_node_spec(),
        );
        let temp = tempdir().expect("tempdir");
        let factory = retained_fs_workspace_factory(temp.path());
        let run_id = "run-list";
        let scope = WorkspaceRunScope::new(ir.flow().id.0.clone(), run_id);
        let workspace = factory.open(scope.clone()).await.expect("open workspace");
        workspace
            .write("artifacts/b.txt", b"bbb", WorkspaceWriteOptions::default())
            .await
            .expect("seed b");
        workspace
            .write("artifacts/a.txt", b"a", WorkspaceWriteOptions::default())
            .await
            .expect("seed a");

        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), Arc::clone(&ir))
            .with_resource_bag(resource_bag_with_checkpoint())
            .with_workspace_factory(Arc::new(factory));
        let result = runtime
            .execute(invocation_with_run_id(
                serde_json::to_value(stdlib::workspace::WorkspaceListInput {
                    prefix: Some("artifacts".to_string()),
                })
                .unwrap(),
                run_id,
            ))
            .await
            .expect("execution ok");
        let value = match result {
            ExecutionResult::Value(value) => value,
            ExecutionResult::Halt { .. } => panic!("unexpected halt result"),
            ExecutionResult::Stream(_) => panic!("unexpected stream result"),
        };
        let output: stdlib::workspace::WorkspaceListOutput =
            serde_json::from_value(value).expect("decode list output");
        let paths = output
            .entries
            .into_iter()
            .map(|entry| entry.path)
            .collect::<Vec<_>>();
        assert_eq!(paths, vec!["artifacts/a.txt", "artifacts/b.txt"]);
    }

    #[tokio::test]
    async fn stdlib_workspace_delete_removes_preseeded_entry_via_runtime() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: stdlib::workspace::WorkspaceDeleteInput| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::capture",
                |value: stdlib::workspace::WorkspaceDeleteOutput| async move { Ok(value) },
            )
            .unwrap();
        stdlib::workspace::workspace_delete_register(&mut registry).unwrap();

        let ir = single_stage_flow(
            "workspace_delete_native",
            stdlib::workspace::workspace_delete_node_spec(),
        );
        let temp = tempdir().expect("tempdir");
        let factory = retained_fs_workspace_factory(temp.path());
        let run_id = "run-delete";
        let scope = WorkspaceRunScope::new(ir.flow().id.0.clone(), run_id);
        let workspace = factory.open(scope.clone()).await.expect("open workspace");
        workspace
            .write(
                "artifacts/report.txt",
                b"hello",
                WorkspaceWriteOptions::default(),
            )
            .await
            .expect("seed artifact");

        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), Arc::clone(&ir))
            .with_resource_bag(resource_bag_with_checkpoint())
            .with_workspace_factory(Arc::new(factory.clone()));
        let result = runtime
            .execute(invocation_with_run_id(
                serde_json::to_value(stdlib::workspace::WorkspaceDeleteInput {
                    path: "artifacts/report.txt".to_string(),
                })
                .unwrap(),
                run_id,
            ))
            .await
            .expect("execution ok");
        let value = match result {
            ExecutionResult::Value(value) => value,
            ExecutionResult::Halt { .. } => panic!("unexpected halt result"),
            ExecutionResult::Stream(_) => panic!("unexpected stream result"),
        };
        let output: stdlib::workspace::WorkspaceDeleteOutput =
            serde_json::from_value(value).expect("decode delete output");
        assert!(output.deleted);
        assert_eq!(output.path, "artifacts/report.txt");
        assert!(
            !factory
                .run_root_path(&scope)
                .join("artifacts/report.txt")
                .exists()
        );
    }

    #[tokio::test]
    async fn resume_fails_when_checkpoint_bundle_pin_mismatches_runtime() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: stdlib::timer::TimerWaitInput| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::capture_resume",
                |value: stdlib::timer::TimerWaitOutput| async move {
                    Ok(serde_json::json!({
                        "resumed": true,
                        "scheduled_at_ms": value.scheduled_at_ms,
                        "payload": value.payload,
                    }))
                },
            )
            .unwrap();
        stdlib::timer::timer_wait_register(&mut registry).unwrap();
        let registry = Arc::new(registry);

        let mut builder = FlowBuilder::new("timer_resume_pin", Version::new(1, 0, 0), Profile::Dev);
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
        let wait = builder
            .add_node("wait", stdlib::timer::timer_wait_node_spec())
            .unwrap();
        let capture = builder
            .add_node(
                "capture",
                &NodeSpec::inline(
                    "tests::capture_resume",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &wait);
        builder.connect(&wait, &capture);

        let ir = Arc::new(validate(&builder.build()).expect("flow validates"));
        let checkpoint_store = Arc::new(MemoryCheckpointStore::default());
        let resources = ResourceBag::new()
            .with_checkpoint_store(Arc::clone(&checkpoint_store))
            .with_resume_scheduler(Arc::new(StubResumeScheduler));

        let runtime_write =
            HostRuntime::new(FlowExecutor::new(Arc::clone(&registry)), Arc::clone(&ir))
                .with_resource_bag(resources.clone())
                .with_bundle_id("bundle://writer");

        let payload = serde_json::json!({"ok": true});
        let input = stdlib::timer::TimerWaitInput {
            duration: Some(Duration::from_millis(10)),
            until: None,
            payload: payload.clone(),
        };
        let invocation =
            Invocation::new("trigger", "capture", serde_json::to_value(input).unwrap());

        let halted = runtime_write.execute(invocation).await.expect("exec ok");
        let checkpoint_id = match halted {
            ExecutionResult::Halt { payload, .. } => payload
                .get("checkpoint_id")
                .and_then(|value| value.as_str())
                .expect("checkpoint_id in halt payload")
                .to_string(),
            _ => panic!("unexpected result variant"),
        };

        let runtime_resume =
            HostRuntime::new(FlowExecutor::new(Arc::clone(&registry)), Arc::clone(&ir))
                .with_resource_bag(resources)
                .with_bundle_id("bundle://reader");

        let err = match runtime_resume.resume(&checkpoint_id).await {
            Ok(_) => panic!("expected pinned bundle mismatch"),
            Err(err) => err,
        };

        match err {
            ExecutionError::CheckpointPinnedBundleUnavailable {
                checkpoint_id: actual_checkpoint_id,
                required_bundle_id,
                runtime_bundle_id,
            } => {
                assert_eq!(actual_checkpoint_id, checkpoint_id);
                assert_eq!(required_bundle_id, "bundle://writer");
                assert_eq!(runtime_bundle_id.as_deref(), Some("bundle://reader"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn legacy_checkpoint_without_bundle_id_resumes_via_compat_path() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: stdlib::timer::TimerWaitInput| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::capture_resume",
                |value: stdlib::timer::TimerWaitOutput| async move {
                    Ok(serde_json::json!({
                        "resumed": true,
                        "scheduled_at_ms": value.scheduled_at_ms,
                        "payload": value.payload,
                    }))
                },
            )
            .unwrap();
        stdlib::timer::timer_wait_register(&mut registry).unwrap();
        let registry = Arc::new(registry);

        let mut builder = FlowBuilder::new(
            "timer_resume_legacy_unpinned",
            Version::new(1, 0, 0),
            Profile::Dev,
        );
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
        let wait = builder
            .add_node("wait", stdlib::timer::timer_wait_node_spec())
            .unwrap();
        let capture = builder
            .add_node(
                "capture",
                &NodeSpec::inline(
                    "tests::capture_resume",
                    "Capture",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &wait);
        builder.connect(&wait, &capture);

        let ir = Arc::new(validate(&builder.build()).expect("flow validates"));
        let checkpoint_store = Arc::new(MemoryCheckpointStore::default());
        let resources = ResourceBag::new()
            .with_checkpoint_store(Arc::clone(&checkpoint_store))
            .with_resume_scheduler(Arc::new(StubResumeScheduler));
        let runtime_write =
            HostRuntime::new(FlowExecutor::new(Arc::clone(&registry)), Arc::clone(&ir))
                .with_resource_bag(resources.clone())
                .with_bundle_id("bundle://writer");

        let payload = serde_json::json!({"ok": true});
        let input = stdlib::timer::TimerWaitInput {
            duration: Some(Duration::from_millis(10)),
            until: None,
            payload: payload.clone(),
        };
        let invocation =
            Invocation::new("trigger", "capture", serde_json::to_value(input).unwrap());

        let halted = runtime_write.execute(invocation).await.expect("exec ok");
        let checkpoint_id = match halted {
            ExecutionResult::Halt { payload, .. } => payload
                .get("checkpoint_id")
                .and_then(|value| value.as_str())
                .expect("checkpoint_id in halt payload")
                .to_string(),
            _ => panic!("unexpected result variant"),
        };

        let handles = checkpoint_store
            .list(CheckpointFilter {
                flow_id: Some(ir.flow().id.clone()),
                run_id: None,
                status: None,
            })
            .await
            .expect("list checkpoints");
        assert_eq!(handles.len(), 1);
        let mut record = checkpoint_store
            .get(&handles[0])
            .await
            .expect("checkpoint exists");
        record.bundle_id = None;
        checkpoint_store
            .put(record)
            .await
            .expect("rewrite legacy checkpoint");

        let runtime_resume =
            HostRuntime::new(FlowExecutor::new(Arc::clone(&registry)), Arc::clone(&ir))
                .with_resource_bag(resources)
                .with_bundle_id("bundle://reader");

        let resumed = runtime_resume
            .resume(&checkpoint_id)
            .await
            .expect("legacy resume ok");
        match resumed {
            ExecutionResult::Value(value) => {
                assert_eq!(value.get("resumed").and_then(|v| v.as_bool()), Some(true));
                assert_eq!(value.get("payload"), Some(&payload));
            }
            _ => panic!("unexpected resume result variant"),
        }
    }

    #[tokio::test]
    async fn callback_wait_resumes_when_signaled() {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: stdlib::callback::CallbackWaitInput| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn(
                "tests::capture",
                |value: stdlib::callback::CallbackWaitOutput| async move { Ok(value) },
            )
            .unwrap();
        stdlib::callback::callback_wait_register(&mut registry).unwrap();

        let mut builder = FlowBuilder::new("callback_wait", Version::new(1, 0, 0), Profile::Dev);
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
        let wait = builder
            .add_node("wait", stdlib::callback::callback_wait_node_spec())
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
        builder.connect(&trigger, &wait);
        builder.connect(&wait, &capture);

        let ir = Arc::new(validate(&builder.build()).expect("flow validates"));
        let resources = resource_bag_with_checkpoint()
            .with_resume_signal_source(Arc::new(StubResumeSignalSource));
        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(resources);

        let input = stdlib::callback::CallbackWaitInput {
            timeout: Some(Duration::from_secs(1)),
            context: serde_json::json!({"source": "test"}),
        };
        let invocation =
            Invocation::new("trigger", "capture", serde_json::to_value(input).unwrap());

        let result = runtime.execute(invocation).await.expect("exec ok");
        let output = match result {
            ExecutionResult::Halt { alias, payload } => {
                assert_eq!(alias, "wait");
                serde_json::from_value::<stdlib::callback::CallbackWaitOutput>(payload)
                    .expect("decode halt output")
            }
            ExecutionResult::Value(_) => panic!("unexpected value"),
            ExecutionResult::Stream(_) => panic!("unexpected stream"),
        };

        assert_eq!(output.context, serde_json::json!({"source": "test"}));
        assert!(!output.resume_token.is_empty());
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
            .with_resource_bag(resource_bag_with_checkpoint());
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

        let resources = resource_bag_with_checkpoint().with_http_write(Arc::new(NullHttp));
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
    async fn preflight_fails_when_required_sql_read_missing() {
        const SQL_READ_EFFECT_HINTS: [&str; 1] = [capabilities::sql::HINT_SQL_READ];

        let runtime = preflight_runtime_for_hints(
            "preflight_sql_read_missing",
            Effects::ReadOnly,
            &SQL_READ_EFFECT_HINTS,
            resource_bag_with_checkpoint(),
        );
        let invocation = Invocation::new("trigger", "node", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingCapabilities { hints }) => {
                assert_eq!(hints, vec![capabilities::sql::HINT_SQL_READ.to_string()]);
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn preflight_fails_when_required_sql_write_missing() {
        const SQL_WRITE_EFFECT_HINTS: [&str; 1] = [capabilities::sql::HINT_SQL_WRITE];

        let runtime = preflight_runtime_for_hints(
            "preflight_sql_write_missing",
            Effects::Effectful,
            &SQL_WRITE_EFFECT_HINTS,
            resource_bag_with_checkpoint(),
        );
        let invocation = Invocation::new("trigger", "node", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingCapabilities { hints }) => {
                assert_eq!(hints, vec![capabilities::sql::HINT_SQL_WRITE.to_string()]);
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn preflight_fails_when_required_sql_admin_missing() {
        const SQL_ADMIN_EFFECT_HINTS: [&str; 1] = [capabilities::sql::HINT_SQL_ADMIN];

        let runtime = preflight_runtime_for_hints(
            "preflight_sql_admin_missing",
            Effects::Effectful,
            &SQL_ADMIN_EFFECT_HINTS,
            resource_bag_with_checkpoint(),
        );
        let invocation = Invocation::new("trigger", "node", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingCapabilities { hints }) => {
                assert_eq!(hints, vec![capabilities::sql::HINT_SQL_ADMIN.to_string()]);
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn preflight_sql_read_binding_does_not_satisfy_write() {
        const SQL_WRITE_EFFECT_HINTS: [&str; 1] = [capabilities::sql::HINT_SQL_WRITE];

        let resources = resource_bag_with_checkpoint().with_sql_read(Arc::new(StubSql));
        let runtime = preflight_runtime_for_hints(
            "preflight_sql_read_not_write",
            Effects::Effectful,
            &SQL_WRITE_EFFECT_HINTS,
            resources,
        );
        let invocation = Invocation::new("trigger", "node", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingCapabilities { hints }) => {
                assert_eq!(hints, vec![capabilities::sql::HINT_SQL_WRITE.to_string()]);
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn preflight_sql_write_binding_does_not_satisfy_admin() {
        const SQL_ADMIN_EFFECT_HINTS: [&str; 1] = [capabilities::sql::HINT_SQL_ADMIN];

        let resources = resource_bag_with_checkpoint().with_sql_write(Arc::new(StubSql));
        let runtime = preflight_runtime_for_hints(
            "preflight_sql_write_not_admin",
            Effects::Effectful,
            &SQL_ADMIN_EFFECT_HINTS,
            resources,
        );
        let invocation = Invocation::new("trigger", "node", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingCapabilities { hints }) => {
                assert_eq!(hints, vec![capabilities::sql::HINT_SQL_ADMIN.to_string()]);
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn preflight_passes_when_required_sql_bindings_present() {
        const SQL_EFFECT_HINTS: [&str; 3] = [
            capabilities::sql::HINT_SQL_READ,
            capabilities::sql::HINT_SQL_WRITE,
            capabilities::sql::HINT_SQL_ADMIN,
        ];

        let resources = resource_bag_with_checkpoint()
            .with_sql_read(Arc::new(StubSql))
            .with_sql_write(Arc::new(StubSql))
            .with_sql_admin(Arc::new(StubSql));
        let runtime = preflight_runtime_for_hints(
            "preflight_sql_present",
            Effects::Effectful,
            &SQL_EFFECT_HINTS,
            resources,
        );
        let invocation = Invocation::new("trigger", "node", serde_json::json!({"ok": true}));

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
            .with_resource_bag(resource_bag_with_checkpoint());
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
    async fn preflight_fails_when_required_workspace_hints_missing() {
        const WORKSPACE_EFFECT_HINTS: [&str; 2] = [
            capabilities::workspace::HINT_WORKSPACE_READ,
            capabilities::workspace::HINT_WORKSPACE_WRITE,
        ];

        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests::trigger",
                |value: JsonValue| async move { Ok(value) },
            )
            .unwrap();
        registry
            .register_fn("tests::workspace_node", |value: JsonValue| async move {
                Ok(value)
            })
            .unwrap();

        let mut builder =
            FlowBuilder::new("preflight_workspace", Version::new(1, 0, 0), Profile::Dev);
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
        let workspace_node = builder
            .add_node(
                "workspace",
                &NodeSpec::inline_with_hints(
                    "tests::workspace_node",
                    "WorkspaceNode",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Effectful,
                    Determinism::BestEffort,
                    None,
                    &[],
                    &WORKSPACE_EFFECT_HINTS,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &workspace_node);

        let mut flow = builder.build();
        flow.nodes
            .iter_mut()
            .find(|node| node.alias == "workspace")
            .expect("workspace node")
            .idempotency
            .key = Some("idempotency".to_string());

        for node in &mut flow.nodes {
            if node.summary.is_none() {
                node.summary = Some("preflight workspace test node".to_string());
            }
        }

        let ir = Arc::new(validate(&flow).expect("flow validates"));

        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(resource_bag_with_checkpoint());
        let invocation = Invocation::new("trigger", "workspace", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingCapabilities { hints }) => {
                assert_eq!(
                    hints,
                    vec![
                        capabilities::workspace::HINT_WORKSPACE_READ.to_string(),
                        capabilities::workspace::HINT_WORKSPACE_WRITE.to_string(),
                    ]
                );
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn preflight_passes_when_required_workspace_hints_present() {
        const WORKSPACE_EFFECT_HINTS: [&str; 2] = [
            capabilities::workspace::HINT_WORKSPACE_READ,
            capabilities::workspace::HINT_WORKSPACE_WRITE,
        ];

        struct NullWorkspace;

        impl capabilities::Capability for NullWorkspace {
            fn name(&self) -> &'static str {
                "workspace.null"
            }
        }

        #[async_trait]
        impl capabilities::workspace::Workspace for NullWorkspace {
            async fn read_normalized(
                &self,
                _normalized_path: &str,
            ) -> Result<
                Option<capabilities::workspace::WorkspaceReadResult>,
                capabilities::workspace::WorkspaceError,
            > {
                Ok(None)
            }

            async fn write_normalized(
                &self,
                normalized_path: &str,
                data: &[u8],
                _options: capabilities::workspace::WorkspaceWriteOptions,
            ) -> Result<
                capabilities::workspace::WorkspaceWriteResult,
                capabilities::workspace::WorkspaceError,
            > {
                Ok(capabilities::workspace::WorkspaceWriteResult {
                    path: normalized_path.to_string(),
                    size_bytes: data.len() as u64,
                    updated_at_ms: 0,
                })
            }

            async fn list_normalized(
                &self,
                _options: capabilities::workspace::WorkspaceListOptions,
            ) -> Result<
                Vec<capabilities::workspace::WorkspaceEntry>,
                capabilities::workspace::WorkspaceError,
            > {
                Ok(Vec::new())
            }

            async fn delete_normalized(
                &self,
                _normalized_path: &str,
            ) -> Result<
                capabilities::workspace::WorkspaceDeleteResult,
                capabilities::workspace::WorkspaceError,
            > {
                Ok(capabilities::workspace::WorkspaceDeleteResult { deleted: false })
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
            .register_fn("tests::workspace_node", |value: JsonValue| async move {
                Ok(value)
            })
            .unwrap();

        let mut builder =
            FlowBuilder::new("preflight_workspace", Version::new(1, 0, 0), Profile::Dev);
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
        let workspace_node = builder
            .add_node(
                "workspace",
                &NodeSpec::inline_with_hints(
                    "tests::workspace_node",
                    "WorkspaceNode",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Effectful,
                    Determinism::BestEffort,
                    None,
                    &[],
                    &WORKSPACE_EFFECT_HINTS,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &workspace_node);

        let mut flow = builder.build();
        flow.nodes
            .iter_mut()
            .find(|node| node.alias == "workspace")
            .expect("workspace node")
            .idempotency
            .key = Some("idempotency".to_string());

        for node in &mut flow.nodes {
            if node.summary.is_none() {
                node.summary = Some("preflight workspace test node".to_string());
            }
        }

        let ir = Arc::new(validate(&flow).expect("flow validates"));

        let resources = resource_bag_with_checkpoint().with_workspace(Arc::new(NullWorkspace));
        let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir)
            .with_resource_bag(resources);
        let invocation = Invocation::new("trigger", "workspace", serde_json::json!({"ok": true}));

        let result = runtime
            .execute(invocation)
            .await
            .expect("execution succeeds");
        if let ExecutionResult::Stream(_) = result {
            panic!("expected value result");
        }
    }

    /// Packet A1: unknown hint strings can no longer reach preflight at all —
    /// kernel-plan validation rejects them with EFFECT202, so a HostRuntime
    /// can never be constructed for such a flow. (Before A1, this test pinned
    /// the misleading `MissingCapabilities` error naming the unknown hint string
    /// preflight failure.)
    #[test]
    fn unknown_resource_hint_is_rejected_by_validation() {
        // Built via concat so the unknown-hint literal doesn't trip the
        // scripts/check-hint-literals.sh grep gate.
        let unknown_hint: &'static str =
            Box::leak(["resource", "::mystery::read"].concat().into_boxed_str());
        let unknown_hints: &'static [&'static str] =
            Box::leak(vec![unknown_hint].into_boxed_slice());

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
                    unknown_hints,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &unknown);

        let diagnostics =
            validate(&builder.build()).expect_err("unknown hint must fail validation");
        assert!(
            diagnostics
                .iter()
                .any(|d| d.code.code == "EFFECT202" && d.message.contains(unknown_hint)),
            "expected EFFECT202 naming the unknown hint, got: {diagnostics:?}"
        );
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
            .with_resource_bag(resource_bag_with_checkpoint());
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
        // A1: unknown hints now fail validation (EFFECT202) before preflight,
        // so this fixture only uses canonical hints (kv duplicated to pin the
        // dedupe behavior; workspace::read to pin sorting).
        const MULTI_EFFECT_HINTS: [&str; 3] = [
            capabilities::workspace::HINT_WORKSPACE_READ,
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
            .with_resource_bag(resource_bag_with_checkpoint());
        let invocation = Invocation::new("trigger", "http", serde_json::json!({"ok": true}));

        match runtime.execute(invocation).await {
            Ok(_) => panic!("expected preflight failure"),
            Err(ExecutionError::MissingCapabilities { hints }) => {
                assert_eq!(
                    hints,
                    vec![
                        capabilities::http::HINT_HTTP_WRITE.to_string(),
                        capabilities::kv::HINT_KV_READ.to_string(),
                        capabilities::workspace::HINT_WORKSPACE_READ.to_string(),
                    ]
                );
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    fn build_bundle_ir() -> ValidatedIR {
        let mut builder = FlowBuilder::new("bundle_test", Version::new(1, 0, 0), Profile::Dev);
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
        validate(&builder.build()).expect("flow validates")
    }

    #[tokio::test]
    async fn bundle_executor_runs_with_registry() {
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

        let bundle = FlowBundle {
            validated_ir: build_bundle_ir(),
            entrypoints: Vec::new(),
            resolver: Arc::new(RegistryResolver::new(Arc::new(registry))),
            node_contracts: vec![
                NodeContract {
                    identifier: "tests::trigger".to_string(),
                    contract_hash: None,
                    source: NodeSource::Local,
                },
                NodeContract {
                    identifier: "tests::capture".to_string(),
                    contract_hash: None,
                    source: NodeSource::Local,
                },
            ],
            environment_plugins: Vec::new(),
        };

        let result = bundle
            .executor()
            .run_once(
                &bundle.validated_ir,
                "trigger",
                serde_json::json!({"ok": true}),
                "capture",
                None,
            )
            .await
            .expect("execution succeeds");

        match result {
            ExecutionResult::Value(value) => assert_eq!(value, serde_json::json!({"ok": true})),
            ExecutionResult::Stream(_) => panic!("expected value result"),
            ExecutionResult::Halt { .. } => panic!("unexpected halt result"),
        }
    }

    #[test]
    fn bundle_allowlist_validates_identifiers() {
        let bundle = FlowBundle {
            validated_ir: build_bundle_ir(),
            entrypoints: Vec::new(),
            resolver: Arc::new(RegistryResolver::new(Arc::new(NodeRegistry::new()))),
            node_contracts: vec![
                NodeContract {
                    identifier: "tests::trigger".to_string(),
                    contract_hash: None,
                    source: NodeSource::Local,
                },
                NodeContract {
                    identifier: "tests::capture".to_string(),
                    contract_hash: None,
                    source: NodeSource::Local,
                },
            ],
            environment_plugins: Vec::new(),
        };

        bundle
            .validate_allowlist()
            .expect("allowlist should accept all identifiers");
    }

    #[test]
    fn bundle_allowlist_rejects_missing_identifier() {
        let bundle = FlowBundle {
            validated_ir: build_bundle_ir(),
            entrypoints: Vec::new(),
            resolver: Arc::new(RegistryResolver::new(Arc::new(NodeRegistry::new()))),
            node_contracts: vec![NodeContract {
                identifier: "tests::trigger".to_string(),
                contract_hash: None,
                source: NodeSource::Local,
            }],
            environment_plugins: Vec::new(),
        };

        match bundle.validate_allowlist() {
            Ok(_) => panic!("expected allowlist failure"),
            Err(BundleError::UnknownIdentifier { identifier }) => {
                assert_eq!(identifier, "tests::capture");
            }
        }
    }
}
