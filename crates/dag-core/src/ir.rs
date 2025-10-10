use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::effects::{Determinism, Effects};

mod version_serde {
    use semver::Version;
    use serde::de::Error as DeError;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(version: &Version, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&version.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Version, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Version::parse(&s).map_err(DeError::custom)
    }
}

/// Unique identifier for a workflow.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, PartialOrd, Ord,
)]
#[serde(transparent)]
pub struct FlowId(pub String);

impl FlowId {
    /// Deterministically derive a flow id from the workflow name and semantic version.
    pub fn new(name: &str, version: &Version) -> Self {
        let namespace = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"latticeflow.flow");
        let key = format!("{name}:{version}");
        let uuid = Uuid::new_v5(&namespace, key.as_bytes());
        Self(uuid.to_string())
    }

    /// Access the underlying UUID.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Unique identifier for a node inside a workflow.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, PartialOrd, Ord,
)]
#[serde(transparent)]
pub struct NodeId(pub String);

impl NodeId {
    /// Construct a node id.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

/// Workflow execution profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    /// HTTP/Axum host.
    Web,
    /// Queue/Redis-backed workers.
    Queue,
    /// Temporal orchestration.
    Temporal,
    /// WASM/Edge runtime.
    Wasm,
    /// Local developer profile.
    Dev,
}

impl Default for Profile {
    fn default() -> Self {
        Profile::Dev
    }
}

/// High-level node categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    /// Trigger originating the workflow.
    Trigger,
    /// Inline rust node.
    Inline,
    /// Activity/connector node.
    Activity,
    /// Subflow invocation.
    Subflow,
}

/// Schema reference used within Flow IR.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchemaRef {
    /// Schema is opaque/unknown.
    Opaque,
    /// Strongly named schema reference.
    Named { name: String },
}

impl SchemaRef {
    /// Construct a named schema.
    pub fn named(name: impl Into<String>) -> Self {
        SchemaRef::Named { name: name.into() }
    }
}

/// Compile-time schema reference emitted by macros.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaSpec {
    /// Opaque schema.
    Opaque,
    /// Named schema reference.
    Named(&'static str),
}

impl SchemaSpec {
    /// Convert into owned Flow IR schema representation.
    pub fn into_ref(self) -> SchemaRef {
        match self {
            SchemaSpec::Opaque => SchemaRef::Opaque,
            SchemaSpec::Named(name) => SchemaRef::named(name),
        }
    }
}

/// Compile-time node specification produced by macros.
#[derive(Debug, Clone)]
pub struct NodeSpec {
    /// Stable identifier (generally module path + function/struct name).
    pub identifier: &'static str,
    /// Human friendly name surfaced to authors.
    pub name: &'static str,
    /// Node category.
    pub kind: NodeKind,
    /// Optional short description.
    pub summary: Option<&'static str>,
    /// Input schema information.
    pub in_schema: SchemaSpec,
    /// Output schema information.
    pub out_schema: SchemaSpec,
    /// Declared effects metadata.
    pub effects: Effects,
    /// Declared determinism metadata.
    pub determinism: Determinism,
}

impl NodeSpec {
    /// Helper for inline nodes.
    pub const fn inline(
        identifier: &'static str,
        name: &'static str,
        in_schema: SchemaSpec,
        out_schema: SchemaSpec,
        effects: Effects,
        determinism: Determinism,
        summary: Option<&'static str>,
    ) -> Self {
        Self {
            identifier,
            name,
            kind: NodeKind::Inline,
            summary,
            in_schema,
            out_schema,
            effects,
            determinism,
        }
    }
}

/// Canonical Flow IR structure serialised to JSON/dot/etc.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FlowIR {
    /// Unique workflow identifier.
    pub id: FlowId,
    /// Workflow display name.
    pub name: String,
    /// Semantic version.
    #[serde(with = "version_serde")]
    #[schemars(with = "String")]
    pub version: Version,
    /// Target profile.
    pub profile: Profile,
    /// Optional human readable summary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// Nodes contained in the workflow.
    #[serde(default)]
    pub nodes: Vec<NodeIR>,
    /// Edges describing the DAG.
    #[serde(default)]
    pub edges: Vec<EdgeIR>,
    /// Control surface metadata (branching/loops/etc).
    #[serde(default)]
    pub control_surfaces: Vec<ControlSurfaceIR>,
    /// Declared checkpoints.
    #[serde(default)]
    pub checkpoints: Vec<CheckpointIR>,
    /// Policy metadata.
    #[serde(default)]
    pub policies: FlowPolicies,
    /// Documentation and tag metadata.
    #[serde(default)]
    pub metadata: FlowMetadata,
    /// Associated artifact references (DOT, WIT, etc.).
    #[serde(default)]
    pub artifacts: Vec<ArtifactRef>,
}

impl FlowIR {
    /// Convenience accessor to find a node by alias.
    pub fn node(&self, alias: &str) -> Option<&NodeIR> {
        self.nodes.iter().find(|n| n.alias == alias)
    }
}

/// Node entry within the Flow IR.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NodeIR {
    /// Stable identifier derived from the spec.
    pub id: NodeId,
    /// Alias used within the workflow definition.
    pub alias: String,
    /// Human readable name.
    pub name: String,
    /// Node kind.
    pub kind: NodeKind,
    /// Optional summary/description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// Input schema reference.
    pub in_schema: SchemaRef,
    /// Output schema reference.
    pub out_schema: SchemaRef,
    /// Declared effects metadata.
    pub effects: Effects,
    /// Declared determinism metadata.
    pub determinism: Determinism,
    /// Optional idempotency configuration.
    #[serde(default)]
    pub idempotency: IdempotencySpec,
}

/// Edge entry within the Flow IR.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EdgeIR {
    /// Source node alias.
    pub from: String,
    /// Destination node alias.
    pub to: String,
    /// Delivery semantics.
    pub delivery: Delivery,
    /// Ordering semantics.
    pub ordering: Ordering,
    /// Optional partition key expression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>,
    /// Optional timeout in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// Buffer policy.
    pub buffer: BufferPolicy,
}

impl Default for EdgeIR {
    fn default() -> Self {
        Self {
            from: String::new(),
            to: String::new(),
            delivery: Delivery::AtLeastOnce,
            ordering: Ordering::Ordered,
            partition_key: None,
            timeout_ms: None,
            buffer: BufferPolicy::default(),
        }
    }
}

/// Delivery semantics enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Delivery {
    /// At least once delivery (default).
    AtLeastOnce,
    /// At most once delivery.
    AtMostOnce,
    /// Exactly once delivery (requires dedupe).
    ExactlyOnce,
}

impl Default for Delivery {
    fn default() -> Self {
        Delivery::AtLeastOnce
    }
}

/// Edge ordering semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Ordering {
    /// FIFO semantics per partition.
    Ordered,
    /// No ordering guarantees.
    Unordered,
}

impl Default for Ordering {
    fn default() -> Self {
        Ordering::Ordered
    }
}

/// Buffering behaviour metadata.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BufferPolicy {
    /// Optional max items held in memory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_items: Option<u32>,
    /// Optional spill threshold in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spill_threshold_bytes: Option<u64>,
    /// Optional spill tier identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spill_tier: Option<String>,
    /// Drop behaviour description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_drop: Option<String>,
}

impl Default for BufferPolicy {
    fn default() -> Self {
        Self {
            max_items: None,
            spill_threshold_bytes: None,
            spill_tier: None,
            on_drop: None,
        }
    }
}

/// Control surface metadata for branching/looping constructs.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ControlSurfaceIR {
    /// Stable identifier for the control surface.
    pub id: String,
    /// Control surface type.
    pub kind: ControlSurfaceKind,
    /// Target node/edge aliases.
    pub targets: Vec<String>,
    /// JSON payload describing surface-specific configuration.
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub config: serde_json::Value,
}

impl Default for ControlSurfaceIR {
    fn default() -> Self {
        Self {
            id: String::new(),
            kind: ControlSurfaceKind::Switch,
            targets: Vec::new(),
            config: serde_json::Value::Null,
        }
    }
}

/// Supported control surface variants.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ControlSurfaceKind {
    /// Switch/multi-branch control flow.
    Switch,
    /// Binary branching (if).
    If,
    /// Loop construct.
    Loop,
    /// For-each iteration construct.
    ForEach,
    /// Windowing configuration.
    Window,
    /// Partition description.
    Partition,
    /// Timeout/latency guard.
    Timeout,
    /// Rate limit guard.
    RateLimit,
    /// Error-handling surface.
    ErrorHandler,
}

impl Default for ControlSurfaceKind {
    fn default() -> Self {
        ControlSurfaceKind::Switch
    }
}

/// Checkpoint definition.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct CheckpointIR {
    /// Checkpoint identifier.
    pub id: String,
    /// Optional summary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

/// Policy lint configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct FlowPolicies {
    /// Lint configuration.
    #[serde(default)]
    pub lint: PolicyLintSettings,
}

/// Lint-specific policy settings.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct PolicyLintSettings {
    /// Whether control surface hints are required.
    pub require_control_hints: bool,
}

/// Arbitrary metadata describing the workflow.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct FlowMetadata {
    /// Optional tags associated with the workflow.
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Artifact reference bundled alongside Flow IR.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ArtifactRef {
    /// Artifact kind (dot, wit, json-schema, etc.).
    pub kind: String,
    /// Relative path to the artifact.
    pub path: String,
    /// Optional format identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

/// Idempotency metadata for a node.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct IdempotencySpec {
    /// Canonical idempotency key expression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Scope for the idempotency key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<IdempotencyScope>,
}

/// Supported idempotency scopes.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyScope {
    /// Key applies to a specific node.
    Node,
    /// Key applies to a specific edge.
    Edge,
    /// Key applies to a partition.
    Partition,
}
