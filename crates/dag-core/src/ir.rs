use std::collections::{BTreeMap, BTreeSet};

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
        let namespace = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"lattice.flow");
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
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
    #[default]
    Dev,
}

/// Durability modes for checkpointing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityMode {
    /// No checkpoints written; resume disabled.
    Off,
    /// Checkpoints only at halt nodes.
    #[default]
    Partial,
    /// Checkpoints at every node boundary.
    Strong,
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

/// Declarative connector role kinds used by node and connector-op metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorRoleKindDecl {
    OutboundAuth,
    ProvisioningAuth,
    InboundVerifier,
    EndpointProfile,
}

/// Static connector role requirement emitted by connector crates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectorRoleRequirement {
    pub kind: ConnectorRoleKindDecl,
    pub name: &'static str,
    pub expected_handle_kind: &'static str,
    /// Whether lock-time binding must satisfy this role. Optional roles
    /// (`required: false`) may be left unbound; bound-but-wrong-kind is
    /// still a failure.
    pub required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorResolutionModeDecl {
    BoundConnection,
    LateBoundRefs,
    InlinePayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectorResolutionContract {
    pub supported_modes: &'static [ConnectorResolutionModeDecl],
    pub default_mode: ConnectorResolutionModeDecl,
}

/// Static reusable connector operation metadata emitted by connector crates.
#[derive(Debug, Clone)]
pub struct ConnectorOpMetadata {
    pub operation_id: &'static str,
    pub connector_id: &'static str,
    pub summary: &'static str,
    pub min_effects: Effects,
    pub max_determinism: Determinism,
    pub determinism_hints: &'static [&'static str],
    pub effect_hints: &'static [&'static str],
    pub roles: &'static [ConnectorRoleRequirement],
    pub resolution: ConnectorResolutionContract,
}

/// Product-neutral semantic operation-contract identity emitted alongside
/// connector metadata. Hosts may use it to select any isolated semantic
/// executor; dag-core has no dependency on, or knowledge of, a broker product.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerContractMetadata {
    pub contract_id: &'static str,
    pub contract_hash: &'static str,
}

/// Serializable connector role requirement emitted into Flow IR.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ConnectorRoleRequirementIR {
    pub kind: ConnectorRoleKindDecl,
    pub name: String,
    pub expected_handle_kind: String,
    /// Whether lock-time binding must satisfy this role. Defaults to `true`
    /// and is omitted from serialized output when `true`, so existing IR,
    /// requirements manifests, and goldens are byte-identical.
    #[serde(
        default = "connector_role_required_default",
        skip_serializing_if = "connector_role_required_is_default"
    )]
    pub required: bool,
}

fn connector_role_required_default() -> bool {
    true
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn connector_role_required_is_default(required: &bool) -> bool {
    *required
}

/// Serializable connector operation reference emitted into Flow IR.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ConnectorOpRefIR {
    pub operation_id: String,
    pub connector_id: String,
    #[serde(default)]
    pub roles: Vec<ConnectorRoleRequirementIR>,
    pub default_resolution_mode: ConnectorResolutionModeDecl,
    pub selected_resolution_mode: ConnectorResolutionModeDecl,
    #[serde(default)]
    pub supported_resolution_modes: Vec<ConnectorResolutionModeDecl>,
}

/// Generic class of fixed implementation invoked by a composite node.
///
/// Dag-core deliberately knows only the execution class, never product- or
/// sample-specific implementations. The stable key is owned by the declaring
/// node's crate and is requirements metadata, not a handler lookup alias.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ImplementationDependencyKind {
    SandboxedTransform,
}

/// Compile-time implementation dependency metadata used by [`NodeSpec`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ImplementationDependencySpec {
    pub kind: ImplementationDependencyKind,
    pub key: &'static str,
}

impl ImplementationDependencySpec {
    pub const fn sandboxed_transform(key: &'static str) -> Self {
        let bytes = key.as_bytes();
        assert!(
            !bytes.is_empty() && bytes.len() <= 128,
            "implementation dependency key length is invalid"
        );
        let first = bytes[0];
        assert!(
            (first >= b'a' && first <= b'z')
                || (first >= b'A' && first <= b'Z')
                || (first >= b'0' && first <= b'9'),
            "implementation dependency key must start with an ASCII alphanumeric"
        );
        let mut index = 0;
        while index < bytes.len() {
            let byte = bytes[index];
            assert!(
                (byte >= b'a' && byte <= b'z')
                    || (byte >= b'A' && byte <= b'Z')
                    || (byte >= b'0' && byte <= b'9')
                    || byte == b'.'
                    || byte == b'_'
                    || byte == b'-',
                "implementation dependency key contains an invalid byte"
            );
            index += 1;
        }
        Self {
            kind: ImplementationDependencyKind::SandboxedTransform,
            key,
        }
    }

    pub fn into_ir(self) -> ImplementationDependency {
        ImplementationDependency {
            kind: self.kind,
            key: self.key.to_string(),
        }
    }
}

/// Serializable implementation dependency emitted into Flow IR.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct ImplementationDependency {
    pub kind: ImplementationDependencyKind,
    pub key: String,
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
    /// Determinism hints emitted by macros or plugins.
    pub determinism_hints: &'static [&'static str],
    /// Effect hints emitted by macros or plugins.
    pub effect_hints: &'static [&'static str],
    /// Reusable connector operations this node may invoke internally.
    pub connector_ops: &'static [&'static ConnectorOpMetadata],
    /// Fixed implementations invoked by this typed composite handler.
    pub implementation_dependencies: &'static [ImplementationDependencySpec],
    /// Optional node-level override for the connector resolution mode used by declared ops.
    pub connector_resolution_mode: Option<ConnectorResolutionModeDecl>,
    /// Whether effects were explicitly declared by the author.
    pub effects_declared: bool,
    /// Whether determinism was explicitly declared by the author.
    pub determinism_declared: bool,
    /// Durability profile metadata (checkpoint/halts).
    pub durability: DurabilityProfile,
    /// Optional idempotency declaration.
    pub idempotency: IdempotencySpecStatic,
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
        Self::inline_with_hints(
            identifier,
            name,
            in_schema,
            out_schema,
            effects,
            determinism,
            summary,
            &[],
            &[],
        )
    }

    /// Helper for inline nodes with explicit determinism hints.
    #[allow(clippy::too_many_arguments)]
    pub const fn inline_with_hints(
        identifier: &'static str,
        name: &'static str,
        in_schema: SchemaSpec,
        out_schema: SchemaSpec,
        effects: Effects,
        determinism: Determinism,
        summary: Option<&'static str>,
        determinism_hints: &'static [&'static str],
        effect_hints: &'static [&'static str],
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
            determinism_hints,
            effect_hints,
            connector_ops: &[],
            implementation_dependencies: &[],
            connector_resolution_mode: None,
            effects_declared: true,
            determinism_declared: true,
            durability: DurabilityProfile {
                checkpointable: true,
                replayable: true,
                halts: false,
            },
            idempotency: IdempotencySpecStatic::empty(),
        }
    }

    /// Resolve effective effects after connector operation hoisting.
    pub fn resolved_effects(&self) -> Effects {
        if self.effects_declared {
            return self.effects;
        }

        self.connector_ops.iter().fold(self.effects, |current, op| {
            if op.min_effects.rank() > current.rank() {
                op.min_effects
            } else {
                current
            }
        })
    }

    /// Resolve effective determinism after connector operation hoisting.
    pub fn resolved_determinism(&self) -> Determinism {
        if self.determinism_declared {
            return self.determinism;
        }

        self.connector_ops
            .iter()
            .fold(self.determinism, |current, op| {
                if op.max_determinism.rank() > current.rank() {
                    op.max_determinism
                } else {
                    current
                }
            })
    }

    /// Resolve determinism hints including connector operation requirements.
    pub fn resolved_determinism_hints(&self) -> Vec<&'static str> {
        let mut resolved = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for hint in self.determinism_hints {
            if seen.insert(*hint) {
                resolved.push(*hint);
            }
        }
        for op in self.connector_ops {
            for hint in op.determinism_hints {
                if seen.insert(*hint) {
                    resolved.push(*hint);
                }
            }
        }

        resolved
    }

    /// Resolve effect hints including connector operation requirements.
    pub fn resolved_effect_hints(&self) -> Vec<&'static str> {
        let mut resolved = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for hint in self.effect_hints {
            if seen.insert(*hint) {
                resolved.push(*hint);
            }
        }
        for op in self.connector_ops {
            for hint in op.effect_hints {
                if seen.insert(*hint) {
                    resolved.push(*hint);
                }
            }
        }

        resolved
    }

    /// Convert connector operation declarations into Flow IR references.
    pub fn connector_op_refs(&self) -> Vec<ConnectorOpRefIR> {
        self.connector_ops
            .iter()
            .map(|op| ConnectorOpRefIR {
                operation_id: op.operation_id.to_string(),
                connector_id: op.connector_id.to_string(),
                roles: op
                    .roles
                    .iter()
                    .map(|role| ConnectorRoleRequirementIR {
                        kind: role.kind,
                        name: role.name.to_string(),
                        expected_handle_kind: role.expected_handle_kind.to_string(),
                        required: role.required,
                    })
                    .collect(),
                default_resolution_mode: op.resolution.default_mode,
                selected_resolution_mode: self
                    .connector_resolution_mode
                    .unwrap_or(op.resolution.default_mode),
                supported_resolution_modes: op.resolution.supported_modes.to_vec(),
            })
            .collect()
    }

    /// Materialize a node spec with connector operation envelopes hoisted.
    pub fn materialize(&self) -> Self {
        let determinism_hints = self.resolved_determinism_hints();
        let effect_hints = self.resolved_effect_hints();
        let determinism_hints = if determinism_hints.is_empty() {
            &[] as &[&'static str]
        } else {
            Box::leak(determinism_hints.into_boxed_slice())
        };
        let effect_hints = if effect_hints.is_empty() {
            &[] as &[&'static str]
        } else {
            Box::leak(effect_hints.into_boxed_slice())
        };

        Self {
            identifier: self.identifier,
            name: self.name,
            kind: self.kind,
            summary: self.summary,
            in_schema: self.in_schema,
            out_schema: self.out_schema,
            effects: self.resolved_effects(),
            determinism: self.resolved_determinism(),
            determinism_hints,
            effect_hints,
            connector_ops: self.connector_ops,
            implementation_dependencies: self.implementation_dependencies,
            connector_resolution_mode: self.connector_resolution_mode,
            effects_declared: true,
            determinism_declared: true,
            durability: self.durability.clone(),
            idempotency: self.idempotency,
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

    /// Validate all Broker V1 authority metadata before derivation or use.
    pub fn validate_broker_authority(&self) -> Result<(), Vec<BrokerAuthorityValidationError>> {
        let mut errors = Vec::new();
        for node in &self.nodes {
            let Some(authority) = &node.broker_authority else {
                continue;
            };
            if let Err(error) = authority.validate_for_node(node) {
                errors.push(error);
            }

            // Repeated activation is derived from the typed control-surface
            // vocabulary, including the real `config.body_entry` shape used
            // by for_each. Opaque config and timeout fields are never treated
            // as authority bounds. Nested bounds multiply and fail closed on
            // overflow.
            let mut nested_bound = 1_u64;
            for surface in &self.control_surfaces {
                let Some((entries, bound)) = repeated_surface_semantics(surface) else {
                    continue;
                };
                if entries
                    .iter()
                    .any(|entry| self.alias_reaches(entry, &node.alias))
                {
                    let Some(bound) = bound else {
                        errors.push(BrokerAuthorityValidationError::UnboundedFanout);
                        continue;
                    };
                    if bound == 0 {
                        errors.push(BrokerAuthorityValidationError::UnboundedFanout);
                    } else if let Some(product) = nested_bound.checked_mul(bound) {
                        nested_bound = product;
                    } else {
                        errors.push(BrokerAuthorityValidationError::FanoutOverflow);
                    }
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    fn alias_reaches(&self, start: &str, target: &str) -> bool {
        let mut pending = vec![start];
        let mut seen = BTreeSet::new();
        while let Some(alias) = pending.pop() {
            if alias == target {
                return true;
            }
            if !seen.insert(alias) {
                continue;
            }
            pending.extend(
                self.edges
                    .iter()
                    .filter(|edge| edge.from == alias)
                    .map(|edge| edge.to.as_str()),
            );
        }
        false
    }
}

fn repeated_surface_semantics(surface: &ControlSurfaceIR) -> Option<(Vec<&str>, Option<u64>)> {
    let config = surface.config.as_object();
    let mut entries = surface
        .targets
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let (entry_fields, bound_fields): (&[&str], &[&str]) = match surface.kind {
        ControlSurfaceKind::ForEach => (&["body_entry"], &["max_items", "max_iterations"]),
        ControlSurfaceKind::Loop => (&["body_entry", "loop_entry"], &["max_iterations"]),
        ControlSurfaceKind::Window => (
            &["body_entry", "window_entry"],
            &["max_windows", "max_items"],
        ),
        _ => return None,
    };
    if let Some(config) = config {
        for field in entry_fields {
            if let Some(entry) = config.get(*field).and_then(serde_json::Value::as_str) {
                entries.push(entry);
            }
        }
    }
    entries.sort_unstable();
    entries.dedup();
    let bound = config.and_then(|config| {
        bound_fields
            .iter()
            .find_map(|field| config.get(*field).and_then(serde_json::Value::as_u64))
    });
    Some((entries, bound))
}

pub const BROKER_MAX_EXACT_INTEGER: u64 = 9_007_199_254_740_991;

/// Broker V1 operation budget scoped per run and node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BrokerOperationBudget {
    #[schemars(
        regex(pattern = r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+@[1-9][0-9]*$"),
        length(min = 3, max = 1024)
    )]
    pub contract_id: String,
    #[schemars(length(min = 1, max = 1024))]
    pub semantic_effect_slots: Vec<String>,
    #[schemars(range(min = 1))]
    pub max_logical_calls: u64,
    #[schemars(range(min = 1, max = 255))]
    pub max_dispatch_attempts_per_call: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(regex(pattern = r"^[ -~]{1,256}$"), length(min = 1, max = 256))]
    pub connection_aggregate_key: Option<String>,
}

/// Hash-covered Broker V1 authority authoring carried by a node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BrokerAuthority {
    #[schemars(length(min = 1, max = 1024))]
    operation_budgets: Vec<BrokerOperationBudget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    flow_aggregate_max_logical_calls: Option<u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(length(max = 1024))]
    connection_aggregate_max_logical_calls: BTreeMap<String, u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BrokerAuthorityUnchecked {
    operation_budgets: Vec<BrokerOperationBudget>,
    #[serde(default)]
    flow_aggregate_max_logical_calls: Option<u64>,
    #[serde(default)]
    connection_aggregate_max_logical_calls: BTreeMap<String, u64>,
}

impl<'de> Deserialize<'de> for BrokerAuthority {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let unchecked = BrokerAuthorityUnchecked::deserialize(deserializer)?;
        Self::new(
            unchecked.operation_budgets,
            unchecked.flow_aggregate_max_logical_calls,
            unchecked.connection_aggregate_max_logical_calls,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl BrokerAuthority {
    pub fn new(
        operation_budgets: Vec<BrokerOperationBudget>,
        flow_aggregate_max_logical_calls: Option<u64>,
        connection_aggregate_max_logical_calls: BTreeMap<String, u64>,
    ) -> Result<Self, BrokerAuthorityValidationError> {
        let authority = Self {
            operation_budgets,
            flow_aggregate_max_logical_calls,
            connection_aggregate_max_logical_calls,
        };
        authority.validate()?;
        Ok(authority)
    }

    pub fn operation_budgets(&self) -> &[BrokerOperationBudget] {
        &self.operation_budgets
    }

    pub fn flow_aggregate_max_logical_calls(&self) -> Option<u64> {
        self.flow_aggregate_max_logical_calls
    }

    pub fn connection_aggregate_max_logical_calls(&self) -> &BTreeMap<String, u64> {
        &self.connection_aggregate_max_logical_calls
    }

    pub fn validate(&self) -> Result<(), BrokerAuthorityValidationError> {
        if self.operation_budgets.is_empty() {
            return Err(BrokerAuthorityValidationError::EmptyOperationBudgets);
        }
        if self.operation_budgets.len() > 1024
            || self.connection_aggregate_max_logical_calls.len() > 1024
        {
            return Err(BrokerAuthorityValidationError::InvalidMaximum);
        }
        let mut contract_ids = BTreeSet::new();
        for budget in &self.operation_budgets {
            if budget.contract_id.len() > 1024 || !valid_broker_contract_id(&budget.contract_id) {
                return Err(BrokerAuthorityValidationError::InvalidContractId);
            }
            if !contract_ids.insert(&budget.contract_id) {
                return Err(BrokerAuthorityValidationError::DuplicateContractId);
            }
            if !valid_semantic_effect_slots(&budget.semantic_effect_slots) {
                return Err(BrokerAuthorityValidationError::InvalidSemanticEffectSlots);
            }
            validate_broker_maximum(budget.max_logical_calls)?;
            validate_broker_maximum(u64::from(budget.max_dispatch_attempts_per_call))?;
            if budget
                .connection_aggregate_key
                .as_deref()
                .is_some_and(|key| !valid_broker_ascii_id(key, 256))
            {
                return Err(BrokerAuthorityValidationError::InvalidAggregateKey);
            }
        }
        if let Some(maximum) = self.flow_aggregate_max_logical_calls {
            validate_broker_maximum(maximum)?;
        }
        for (key, maximum) in &self.connection_aggregate_max_logical_calls {
            if !valid_broker_ascii_id(key, 256) {
                return Err(BrokerAuthorityValidationError::InvalidAggregateKey);
            }
            validate_broker_maximum(*maximum)?;
        }
        Ok(())
    }

    fn validate_for_node(&self, node: &NodeIR) -> Result<(), BrokerAuthorityValidationError> {
        self.validate()?;
        for budget in &self.operation_budgets {
            let operation_id = budget
                .contract_id
                .rsplit_once('@')
                .map(|(operation, _)| operation)
                .ok_or(BrokerAuthorityValidationError::InvalidContractId)?;
            if !node
                .connector_ops
                .iter()
                .any(|operation| operation.operation_id == operation_id)
            {
                return Err(BrokerAuthorityValidationError::ContractNotDeclaredByNode);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BrokerAuthorityValidationError {
    #[error("broker authority must contain at least one operation budget")]
    EmptyOperationBudgets,
    #[error("broker authority contains a malformed contract id")]
    InvalidContractId,
    #[error("broker authority repeats an operation contract")]
    DuplicateContractId,
    #[error("broker authority contains invalid semantic effect slots")]
    InvalidSemanticEffectSlots,
    #[error("broker authority maximum must be positive and exactly representable in JCS")]
    InvalidMaximum,
    #[error("broker authority contains an invalid aggregate key")]
    InvalidAggregateKey,
    #[error("broker authority contract is absent from node connector operation metadata")]
    ContractNotDeclaredByNode,
    #[error("brokered node is reachable from an unbounded repeated-activation control surface")]
    UnboundedFanout,
    #[error("nested repeated-activation bounds overflow")]
    FanoutOverflow,
}

fn validate_broker_maximum(value: u64) -> Result<(), BrokerAuthorityValidationError> {
    if value == 0 || value > BROKER_MAX_EXACT_INTEGER {
        Err(BrokerAuthorityValidationError::InvalidMaximum)
    } else {
        Ok(())
    }
}

fn valid_broker_contract_id(value: &str) -> bool {
    let Some((name, major)) = value.rsplit_once('@') else {
        return false;
    };
    if major.is_empty()
        || major.starts_with('0')
        || !major.bytes().all(|byte| byte.is_ascii_digit())
    {
        return false;
    }
    let segments = name.split('.').collect::<Vec<_>>();
    segments.len() >= 2
        && segments.iter().all(|segment| {
            segment
                .as_bytes()
                .first()
                .is_some_and(|byte| byte.is_ascii_lowercase())
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        })
}

fn valid_semantic_effect_slots(slots: &[String]) -> bool {
    !slots.is_empty()
        && slots.len() <= 1024
        && slots.iter().all(|slot| {
            valid_broker_ascii_id(slot, 128)
                && slot
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        })
        && slots.windows(2).all(|pair| pair[0] < pair[1])
}

fn valid_broker_ascii_id(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value.is_ascii()
        && value.bytes().all(|byte| !byte.is_ascii_control())
}

/// Node entry within the Flow IR.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NodeIR {
    /// Stable identifier derived from the spec.
    pub id: NodeId,
    /// Alias used within the workflow definition.
    pub alias: String,
    /// Fully-qualified implementation identifier used for runtime lookup.
    pub identifier: String,
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
    /// Optional durability profile (checkpoint/halts metadata).
    #[serde(default, skip_serializing_if = "DurabilityProfile::is_default")]
    pub durability: DurabilityProfile,
    /// Determinism hints recorded during macro expansion.
    #[serde(rename = "determinismHints", default)]
    pub determinism_hints: Vec<String>,
    /// Effect hints recorded during macro expansion.
    #[serde(rename = "effectHints", default)]
    pub effect_hints: Vec<String>,
    /// Structured connector operations declared for the node.
    #[serde(rename = "connectorOps", default)]
    pub connector_ops: Vec<ConnectorOpRefIR>,
    /// Optional Broker V1 budgets. Snake case is intentional and normative.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_authority: Option<BrokerAuthority>,
    /// Fixed implementations invoked internally by this typed composite node.
    #[serde(
        rename = "implementationDependencies",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub implementation_dependencies: Vec<ImplementationDependency>,
    /// Optional expanded subflow IR for analysis-only views.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subflow_ir: Option<Box<FlowIR>>,
}

/// Durability profile used for checkpoint validation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DurabilityProfile {
    /// Node state can be serialized and resumed.
    pub checkpointable: bool,
    /// Streaming outputs can be replayed on resume.
    pub replayable: bool,
    /// Node is a halting boundary requiring suspend + resume.
    pub halts: bool,
}

impl Default for DurabilityProfile {
    fn default() -> Self {
        Self {
            checkpointable: true,
            replayable: true,
            halts: false,
        }
    }
}

impl DurabilityProfile {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
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
    /// Optional transform metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<EdgeTransformIR>,
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
            transform: None,
            buffer: BufferPolicy::default(),
        }
    }
}

/// Edge transform metadata block.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EdgeTransformIR {
    /// Transform kind.
    pub kind: EdgeTransformKind,
}

/// Edge transform kind enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EdgeTransformKind {
    /// Rust Into conversion.
    Into,
}

/// Delivery semantics enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum Delivery {
    /// At least once delivery (default).
    #[default]
    AtLeastOnce,
    /// At most once delivery.
    AtMostOnce,
    /// Exactly once delivery (requires dedupe).
    ExactlyOnce,
}

/// Edge ordering semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum Ordering {
    /// FIFO semantics per partition.
    #[default]
    Ordered,
    /// No ordering guarantees.
    Unordered,
}

/// Buffering behaviour metadata.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum ControlSurfaceKind {
    /// Switch/multi-branch control flow.
    #[default]
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

impl ControlSurfaceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Switch => "switch",
            Self::If => "if",
            Self::Loop => "loop",
            Self::ForEach => "for_each",
            Self::Window => "window",
            Self::Partition => "partition",
            Self::Timeout => "timeout",
            Self::RateLimit => "rate_limit",
            Self::ErrorHandler => "error_handler",
        }
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
    /// Durability configuration.
    #[serde(default)]
    pub durability: DurabilityPolicy,
}

/// Durability policy settings.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct DurabilityPolicy {
    /// Requested durability mode.
    #[serde(default)]
    pub mode: DurabilityMode,
    /// Checkpoint interval (host-specific interpretation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_interval: Option<u64>,
    /// Blob spill threshold in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_threshold_bytes: Option<u64>,
    /// Lease TTL in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_ttl: Option<u64>,
    /// Maximum time a checkpoint remains valid (milliseconds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_ttl: Option<u64>,
    /// Retention window after ack (milliseconds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retain_completed_for: Option<u64>,
    /// Retry resume on lease conflict.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_on_lease_conflict: Option<bool>,
    /// Maximum number of resume attempts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_resume_attempts: Option<u32>,
}

/// Lint-specific policy settings.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct PolicyLintSettings {
    /// Whether control surface hints are required.
    #[serde(default)]
    pub require_control_hints: bool,
    /// Allow multiple trigger nodes in a single flow.
    ///
    /// Default is false when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_multiple_triggers: Option<bool>,
}

/// Arbitrary metadata describing the workflow.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct FlowMetadata {
    /// Optional tags associated with the workflow.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Optional entrypoint metadata provided by hosts or tooling.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(default, schema_with = "schema_vec_entrypoints")]
    pub entrypoints: Vec<EntrypointMetadata>,
}

/// Entrypoint metadata describing external ingress wiring.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct EntrypointMetadata {
    /// Trigger node alias for ingress.
    pub trigger_alias: String,
    /// Capture node alias for response/egress.
    pub capture_alias: String,
    /// Optional canonical route path (host-derived when omitted).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route_path: Option<String>,
    /// Optional HTTP method for HTTP-capable hosts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Optional non-authoritative aliases for the canonical route.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(default, schema_with = "schema_vec_strings_with_default")]
    pub route_aliases: Vec<String>,
    /// Optional cron expression for schedule-shaped entrypoints
    /// (5-field Cloudflare dialect, UTC). Mutually exclusive with `method`
    /// and `route_aliases` (TRIG002). See
    /// `impl-docs/spec/schedule-trigger.md`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
}

fn schema_vec_entrypoints(
    schema_gen: &mut schemars::r#gen::SchemaGenerator,
) -> schemars::schema::Schema {
    let mut schema = <Vec<EntrypointMetadata>>::json_schema(schema_gen);
    set_schema_default(&mut schema, serde_json::json!([]));
    schema
}

fn schema_vec_strings_with_default(
    schema_gen: &mut schemars::r#gen::SchemaGenerator,
) -> schemars::schema::Schema {
    let mut schema = <Vec<String>>::json_schema(schema_gen);
    set_schema_default(&mut schema, serde_json::json!([]));
    schema
}

fn set_schema_default(schema: &mut schemars::schema::Schema, value: serde_json::Value) {
    if let schemars::schema::Schema::Object(schema_obj) = schema {
        let metadata = schema_obj
            .metadata
            .get_or_insert_with(|| Box::new(schemars::schema::Metadata::default()));
        metadata.default = Some(value);
    }
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
    /// Optional TTL (in milliseconds) for dedupe reservations.
    #[serde(rename = "ttlMs", skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
}

/// Supported idempotency scopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyScope {
    /// Key applies to a specific node.
    Node,
    /// Key applies to a specific edge.
    Edge,
    /// Key applies to a partition.
    Partition,
}

/// Compile-time idempotency declaration produced by macros.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdempotencySpecStatic {
    pub key: Option<&'static str>,
    pub scope: Option<IdempotencyScope>,
    pub ttl_ms: Option<u64>,
}

impl IdempotencySpecStatic {
    pub const fn empty() -> Self {
        Self {
            key: None,
            scope: None,
            ttl_ms: None,
        }
    }

    pub fn into_owned(self) -> IdempotencySpec {
        IdempotencySpec {
            key: self.key.map(|value| value.to_string()),
            scope: self.scope,
            ttl_ms: self.ttl_ms,
        }
    }
}

#[cfg(test)]
mod broker_authority_tests {
    use super::*;
    use crate::builder::FlowBuilder;

    fn budget() -> BrokerOperationBudget {
        BrokerOperationBudget {
            contract_id: "dev.synthetic.echo_effect@1".to_string(),
            semantic_effect_slots: vec!["echo_effect".to_string()],
            max_logical_calls: 2,
            max_dispatch_attempts_per_call: 1,
            connection_aggregate_key: Some("synthetic-primary".to_string()),
        }
    }

    fn budgeted_flow() -> FlowIR {
        let spec = NodeSpec::inline(
            "dev.synthetic.echo_effect",
            "Synthetic echo",
            SchemaSpec::Opaque,
            SchemaSpec::Opaque,
            Effects::Effectful,
            Determinism::BestEffort,
            None,
        );
        let mut builder = FlowBuilder::new("budgeted", Version::new(1, 0, 0), Profile::Dev);
        builder.add_node("echo", &spec).expect("node");
        let mut flow = builder.build();
        flow.nodes[0].connector_ops.push(ConnectorOpRefIR {
            operation_id: "dev.synthetic.echo_effect".to_string(),
            connector_id: "dev.synthetic".to_string(),
            roles: Vec::new(),
            default_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
            selected_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
            supported_resolution_modes: vec![ConnectorResolutionModeDecl::BoundConnection],
        });
        flow.nodes[0].broker_authority = Some(
            BrokerAuthority::new(
                vec![budget()],
                Some(4),
                BTreeMap::from([("synthetic-primary".to_string(), 3)]),
            )
            .expect("valid authority"),
        );
        flow
    }

    #[test]
    fn budgeted_node_survives_flow_ir_round_trip() {
        let flow = budgeted_flow();
        flow.validate_broker_authority().expect("valid authority");
        let bytes = serde_json::to_vec(&flow).expect("serialize");
        let decoded: FlowIR = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(
            decoded.nodes[0].broker_authority,
            flow.nodes[0].broker_authority
        );
        let authority = decoded.nodes[0]
            .broker_authority
            .as_ref()
            .expect("authority");
        assert_eq!(authority.operation_budgets()[0].max_logical_calls, 2);
        assert_eq!(authority.flow_aggregate_max_logical_calls(), Some(4));
    }

    #[test]
    fn broker_authority_is_hash_covered_by_flow_ir_bytes() {
        let left = budgeted_flow();
        let mut right = left.clone();
        let authority = right.nodes[0].broker_authority.as_ref().expect("authority");
        right.nodes[0].broker_authority = Some(
            BrokerAuthority::new(
                authority.operation_budgets().to_vec(),
                Some(5),
                authority.connection_aggregate_max_logical_calls().clone(),
            )
            .expect("updated authority"),
        );
        let left_bytes = serde_json::to_vec(&left).expect("left bytes");
        let right_bytes = serde_json::to_vec(&right).expect("right bytes");
        assert_ne!(left_bytes, right_bytes);
        assert!(
            String::from_utf8(left_bytes)
                .expect("json utf8")
                .contains("\"broker_authority\"")
        );
    }

    #[test]
    fn invalid_authority_is_rejected_during_deserialization() {
        let flow = budgeted_flow();
        let mut value = serde_json::to_value(flow).expect("value");
        value["nodes"][0]["broker_authority"]["operation_budgets"][0]["max_logical_calls"] =
            serde_json::json!(0);
        assert!(serde_json::from_value::<FlowIR>(value).is_err());

        let flow = budgeted_flow();
        let mut value = serde_json::to_value(flow).expect("value");
        value["nodes"][0]["broker_authority"]["operation_budgets"][0]["max_logical_calls"] =
            serde_json::json!(BROKER_MAX_EXACT_INTEGER + 1);
        assert!(serde_json::from_value::<FlowIR>(value).is_err());

        let flow = budgeted_flow();
        let mut value = serde_json::to_value(flow).expect("value");
        value["nodes"][0]["broker_authority"]["operation_budgets"][0]["semantic_effect_slots"] =
            serde_json::json!(["z", "a"]);
        assert!(serde_json::from_value::<FlowIR>(value).is_err());

        let flow = budgeted_flow();
        let mut value = serde_json::to_value(flow).expect("value");
        let duplicate = value["nodes"][0]["broker_authority"]["operation_budgets"][0].clone();
        value["nodes"][0]["broker_authority"]["operation_budgets"] =
            serde_json::json!([duplicate.clone(), duplicate]);
        assert!(serde_json::from_value::<FlowIR>(value).is_err());

        let flow = budgeted_flow();
        let mut value = serde_json::to_value(flow).expect("value");
        value["nodes"][0]["broker_authority"]["unknown_security_field"] = serde_json::json!(true);
        assert!(serde_json::from_value::<FlowIR>(value).is_err());
    }

    #[test]
    fn undeclared_contract_and_unbounded_fanout_fail_validation() {
        let mut undeclared = budgeted_flow();
        undeclared.nodes[0].connector_ops.clear();
        assert!(matches!(
            undeclared.validate_broker_authority(),
            Err(errors) if errors.contains(&BrokerAuthorityValidationError::ContractNotDeclaredByNode)
        ));

        let mut unbounded = budgeted_flow();
        unbounded.control_surfaces.push(ControlSurfaceIR {
            id: "repeat".to_string(),
            kind: ControlSurfaceKind::ForEach,
            targets: vec!["echo".to_string()],
            config: serde_json::json!({"v": 1}),
        });
        assert!(matches!(
            unbounded.validate_broker_authority(),
            Err(errors) if errors.contains(&BrokerAuthorityValidationError::UnboundedFanout)
        ));
    }

    #[test]
    fn emitted_schema_contains_broker_authority() {
        let schema = crate::schema::flow_ir_schema();
        let json = serde_json::to_string(&schema).expect("schema json");
        assert!(json.contains("broker_authority"));
        assert!(json.contains("operation_budgets"));
    }
}
