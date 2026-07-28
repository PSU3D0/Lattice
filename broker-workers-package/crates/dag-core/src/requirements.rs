//! Static flow requirements manifest (packet C1).
//!
//! [`FlowRequirements`] is the machine-readable answer to "what does this flow
//! need to run?", computed entirely from Flow IR metadata — node effect hints,
//! connector operation declarations, durability policy, trigger/entrypoint
//! surface — without executing a node or calling a live connector runtime.
//!
//! It is the seed artifact for infra-from-code: an infrastructure planner
//! reads the manifest from a bundle and decides placement (CF worker sizing,
//! native host, etc.) with zero code execution.
//!
//! Static derivability rule: every field here MUST be computable from a
//! validated Flow IR plus the connector operation metadata already serialized
//! into it (`NodeIR.connector_ops` and `NodeIR.implementation_dependencies`). Where bound-connection resolution happens
//! today at runtime preflight (host-inproc), this manifest records only the
//! DECLARED contract (supported resolution modes, role requirements);
//! instance-binding satisfaction is a bindings.lock-time concern. See
//! `impl-docs/spec/flow-requirements.md`.

use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::effect_hint::EffectHint;
use crate::ir::{
    BrokerContractIdentityIR, ConnectorResolutionModeDecl, ConnectorRoleRequirementIR,
    DurabilityMode, FlowIR, FlowId, ImplementationDependencyKind, NodeIR, NodeKind, Profile,
};

/// Version of the FlowRequirements manifest shape itself (not the flow).
///
/// Bump on any breaking change to the manifest structure; consumers must
/// reject schema versions they do not understand.
pub const FLOW_REQUIREMENTS_SCHEMA_VERSION: &str = "0.4";

const MAX_SUBFLOW_REQUIREMENTS_DEPTH: usize = 64;

/// Prefix for policy markers that are allowed to appear in
/// `NodeIR.effect_hints` but are lint annotations, not capability
/// requirements (e.g. the TYPE001 `policy::json_boundary` marker accepted by
/// kernel-plan validation).
const POLICY_MARKER_PREFIX: &str = "policy::";

/// Stdlib node identifiers that require a resume scheduler when halting.
/// Mirrors host-inproc's `collect_missing_durability_services`.
const RESUME_SCHEDULER_IDENTIFIERS: &[&str] = &["std.timer.wait"];

/// Stdlib node identifiers that require a resume signal source when halting.
/// Mirrors host-inproc's `collect_missing_durability_services`.
const RESUME_SIGNAL_IDENTIFIERS: &[&str] = &["std.callback.wait", "std.hitl.approval"];

/// Error produced when requirements cannot be derived statically.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RequirementsError {
    /// A node declares a hint string that is neither a canonical
    /// [`EffectHint`] nor a `policy::*` marker. Derivation fails closed,
    /// matching kernel-plan's EFFECT202 validation.
    #[error(
        "node `{node}` declares an unknown effect hint `{hint}`; requirements derivation fails \
         closed (EFFECT202; see impl-docs/error-codes.md)"
    )]
    UnknownEffectHint {
        /// Alias of the offending node.
        node: String,
        /// The offending hint string.
        hint: String,
    },
    #[error("node `{node}` declares invalid implementation dependency key `{key}`")]
    InvalidImplementationDependency { node: String, key: String },
    #[error(
        "subflow node `{node}` has no embedded subflow IR; requirements derivation fails closed"
    )]
    MissingSubflowIr { node: String },
    #[error("subflow cycle detected while deriving requirements: {chain}")]
    SubflowCycle { chain: String },
    #[error("subflow nesting exceeds the maximum requirements depth of {maximum}")]
    SubflowDepthExceeded { maximum: usize },
    #[error("no pinned contract descriptor resolves hash `{contract_hash}` for `{contract_id}`")]
    UnknownContractHash {
        contract_id: String,
        contract_hash: String,
    },
    #[error(
        "descriptor resolved by hash `{contract_hash}` names `{actual_contract_id}`, expected `{expected_contract_id}`"
    )]
    ContractIdentityMismatch {
        contract_hash: String,
        expected_contract_id: String,
        actual_contract_id: String,
    },
    #[error("descriptor hash `{contract_hash}` resolves to conflicting contract scope metadata")]
    ConflictingContractDescriptor { contract_hash: String },
    #[error("operation contract descriptor is not valid bounded canonicalizable JSON")]
    InvalidContractDescriptor,
    #[error(
        "operation contract descriptor is missing a string contract_id or string minimum_scopes"
    )]
    InvalidContractDescriptorShape,
    #[error(
        "operation contract descriptor hash mismatch: expected `{expected}`, computed `{actual}`"
    )]
    ContractDescriptorHashMismatch { expected: String, actual: String },
    #[error(
        "descriptor `{contract_id}` has malformed minimum scopes; expected sorted unique non-empty ASCII values"
    )]
    InvalidMinimumScopes { contract_id: String },
    #[error("scope resolution Flow IR does not match the identity-phase requirements manifest")]
    ScopeResolutionFlowMismatch,
    #[error("scope resolution requires requirements pinned to the exact serialized Flow IR")]
    MissingScopeResolutionFlowHash,
    #[error("operation `{operation_id}` declares conflicting hash-pinned contract identities")]
    ConflictingOperationContract { operation_id: String },
    #[error("node `{node}` broker authority does not name exact contract `{contract_id}`")]
    ContractAuthorityMismatch { node: String, contract_id: String },
    #[error(
        "node `{node}` broker authority names contract `{contract_id}`, but operation `{operation_id}` has no pinned contract identity"
    )]
    MissingPinnedContractIdentity {
        node: String,
        contract_id: String,
        operation_id: String,
    },
}

/// Static requirements manifest for a single flow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "FlowRequirementsWire")]
pub struct FlowRequirements {
    /// Manifest shape version ([`FLOW_REQUIREMENTS_SCHEMA_VERSION`]).
    pub schema_version: String,
    /// Identity of the flow this manifest describes.
    pub flow: FlowIdentity,
    /// Target execution profile declared by the flow.
    pub profile: Profile,
    /// Typed capability requirements (union + per-node attribution).
    pub effects: EffectRequirements,
    /// Fixed implementation contracts, grouped by generic kind and declaring
    /// crate-owned key with the aliases that use each contract.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub implementation_dependencies: Vec<ImplementationDependencyRequirement>,
    /// Connector operation requirements, grouped by connector family.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connectors: Vec<ConnectorRequirement>,
    /// Report-only scope closure partitioned by connection aggregate key.
    /// Empty until descriptor resolution is requested explicitly.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scope_closures: Vec<ConnectionScopeClosure>,
    /// Explicit state of report-only scope resolution. This distinguishes a
    /// genuinely scope-free flow from a contracted or unexpanded flow whose
    /// descriptors have not been resolved yet.
    pub scope_resolution: ScopeClosureResolution,
    /// Durability mode and the host services it implies.
    pub durability: DurabilityRequirements,
    /// Trigger nodes that originate executions of this flow.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggers: Vec<TriggerRequirement>,
    /// External ingress surface (routes, methods, deadlines).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entrypoints: Vec<EntrypointRequirement>,
    /// Host constraints derivable from the IR today.
    pub host: HostConstraints,
    /// Hash of the serialized Flow IR this manifest was derived from
    /// (`sha256:<hex>`), populated at bundle-assembly time. The enclosing
    /// bundle id is intentionally NOT embedded: the manifest is hashed into
    /// the bundle id, so embedding it would be circular.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_ir_hash: Option<String>,
}

#[derive(Deserialize)]
struct FlowRequirementsWire {
    schema_version: String,
    flow: FlowIdentity,
    profile: Profile,
    effects: EffectRequirements,
    #[serde(default)]
    implementation_dependencies: Vec<ImplementationDependencyRequirement>,
    #[serde(default)]
    connectors: Vec<ConnectorRequirement>,
    #[serde(default)]
    scope_closures: Vec<ConnectionScopeClosure>,
    scope_resolution: ScopeClosureResolution,
    durability: DurabilityRequirements,
    #[serde(default)]
    triggers: Vec<TriggerRequirement>,
    #[serde(default)]
    entrypoints: Vec<EntrypointRequirement>,
    host: HostConstraints,
    #[serde(default)]
    flow_ir_hash: Option<String>,
}

impl TryFrom<FlowRequirementsWire> for FlowRequirements {
    type Error = String;

    fn try_from(wire: FlowRequirementsWire) -> Result<Self, Self::Error> {
        if wire.schema_version != FLOW_REQUIREMENTS_SCHEMA_VERSION {
            return Err(format!(
                "unsupported FlowRequirements schema_version `{}`; expected `{}`",
                wire.schema_version, FLOW_REQUIREMENTS_SCHEMA_VERSION
            ));
        }
        Ok(Self {
            schema_version: wire.schema_version,
            flow: wire.flow,
            profile: wire.profile,
            effects: wire.effects,
            implementation_dependencies: wire.implementation_dependencies,
            connectors: wire.connectors,
            scope_closures: wire.scope_closures,
            scope_resolution: wire.scope_resolution,
            durability: wire.durability,
            triggers: wire.triggers,
            entrypoints: wire.entrypoints,
            host: wire.host,
            flow_ir_hash: wire.flow_ir_hash,
        })
    }
}

/// Identity of the flow a requirements manifest describes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FlowIdentity {
    /// Stable flow identifier (UUIDv5 of name + version).
    pub id: FlowId,
    /// Display name of the flow.
    pub name: String,
    /// Semantic version string of the flow.
    pub version: String,
}

/// Typed capability requirements for a flow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EffectRequirements {
    /// Flow-wide union of declared capability hints, sorted by canonical
    /// string. This is what a planner provisions.
    #[serde(default)]
    pub union: Vec<EffectHint>,
    /// Capability families implied by `union` (e.g. `resource::http` for
    /// `resource::http::read`), sorted by canonical string.
    #[serde(default)]
    pub families: Vec<EffectHint>,
    /// Per-node attribution: node alias to its declared hints. Only nodes
    /// declaring at least one capability hint appear. This is what a
    /// debugger inspects.
    #[serde(default)]
    pub per_node: BTreeMap<String, Vec<EffectHint>>,
}

/// One fixed implementation contract used by a flow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImplementationDependencyRequirement {
    /// Generic execution class understood by host placement policy.
    pub kind: ImplementationDependencyKind,
    /// Stable contract key owned by the declaring node crate.
    pub key: String,
    /// Aliases of nodes using this contract (sorted).
    pub nodes: Vec<String>,
}

/// Requirements contributed by one connector family.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ConnectorRequirement {
    /// Connector family identifier (e.g. `connector.formualizer.sheetport`).
    pub connector_id: String,
    /// Operations of this connector the flow may invoke.
    pub operations: Vec<ConnectorOperationRequirement>,
}

/// Scope-bearing projection of a full operation contract descriptor whose
/// hash was recomputed by the constructor. Private fields make an unverified
/// `(claimed hash, scopes)` record unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractScopeDescriptor {
    contract_id: String,
    contract_hash: String,
    minimum_scopes: Vec<String>,
}

impl ContractScopeDescriptor {
    /// Verify a complete canonical descriptor preimage against its pinned
    /// hash and retain only the fields needed by report generation.
    pub fn verify_json(
        descriptor_json: &[u8],
        expected_hash: &str,
    ) -> Result<Self, RequirementsError> {
        const MAX_DESCRIPTOR_BYTES: usize = 256 * 1024;
        let canonical = jcs_canonical::canonicalize_bounded(descriptor_json, MAX_DESCRIPTOR_BYTES)
            .map_err(|_| RequirementsError::InvalidContractDescriptor)?;
        let actual_hash = format!(
            "sha256:{}",
            hex::encode(Sha256::digest(canonical.as_bytes()))
        );
        if actual_hash != expected_hash {
            return Err(RequirementsError::ContractDescriptorHashMismatch {
                expected: expected_hash.to_string(),
                actual: actual_hash,
            });
        }

        let value: serde_json::Value = serde_json::from_slice(canonical.as_bytes())
            .map_err(|_| RequirementsError::InvalidContractDescriptor)?;
        let contract_id = value
            .get("contract_id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or(RequirementsError::InvalidContractDescriptorShape)?
            .to_string();
        let minimum_scopes = value
            .get("minimum_scopes")
            .and_then(serde_json::Value::as_array)
            .ok_or(RequirementsError::InvalidContractDescriptorShape)?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or(RequirementsError::InvalidContractDescriptorShape)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if !valid_minimum_scopes(&minimum_scopes) {
            return Err(RequirementsError::InvalidMinimumScopes {
                contract_id: contract_id.clone(),
            });
        }
        Ok(Self {
            contract_id,
            contract_hash: expected_hash.to_string(),
            minimum_scopes,
        })
    }
}

/// Whether descriptor-backed scope closure has been computed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ScopeClosureResolution {
    /// The complete IR contains neither contracted operations nor unresolved
    /// subflows, so no descriptor lookup is needed.
    NotRequired,
    /// Resolution has not run, or cannot yet be complete because authored
    /// subflows have not been expanded.
    Unresolved {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        unresolved_subflows: Vec<String>,
    },
    /// Every contracted operation was resolved from its exact descriptor.
    Resolved,
}

/// Report-only required scopes for one connection aggregate partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ConnectionScopeClosure {
    /// Existing broker-authority aggregate key. `None` is the current unkeyed
    /// default connection partition, not a flow-wide union.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_aggregate_key: Option<String>,
    /// Exact contracts contributing scopes to this partition.
    pub contracts: Vec<BrokerContractIdentityIR>,
    /// Sorted, deduplicated union of descriptor-owned minimum scopes.
    pub required_scopes: Vec<String>,
}

/// Declared contract for a single connector operation used by the flow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ConnectorOperationRequirement {
    /// Operation identifier (e.g. `connector.formualizer.sheetport.evaluate`).
    pub operation_id: String,
    /// Optional hash-pinned semantic contract identity. Minimum scopes are
    /// resolved separately from the descriptor addressed by this hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_contract: Option<BrokerContractIdentityIR>,
    /// Auth/endpoint role requirements declared by the connector crate
    /// (`ConnectorOpMetadata.roles`). Lock-time binding must satisfy each
    /// role with a handle of the expected kind.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<ConnectorRoleRequirementIR>,
    /// Resolution modes the operation supports, in declaration order.
    pub supported_resolution_modes: Vec<ConnectorResolutionModeDecl>,
    /// Resolution mode used when a node does not override it.
    pub default_resolution_mode: ConnectorResolutionModeDecl,
    /// Resolution modes actually selected by nodes in this flow (sorted,
    /// deduplicated).
    pub selected_resolution_modes: Vec<ConnectorResolutionModeDecl>,
    /// True when any node selects `bound_connection`: a connection instance
    /// must be bound for this operation in bindings.lock before the flow can
    /// run.
    pub requires_bound_connection: bool,
    /// Aliases of the nodes that declare this operation (sorted).
    pub nodes: Vec<String>,
}

/// Durability mode and the host services it implies.
///
/// Derivation mirrors host-inproc preflight (`collect_missing_durability_services`)
/// so a planner can provision exactly what preflight will demand.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DurabilityRequirements {
    /// Requested durability mode from `FlowIR.policies.durability`.
    pub mode: DurabilityMode,
    /// True when any node is a halting boundary (suspend + resume).
    pub has_halting_nodes: bool,
    /// A checkpoint store must be bound (mode is not `off`).
    pub needs_checkpoint_store: bool,
    /// A resume scheduler must be bound (halting timer nodes present).
    pub needs_resume_scheduler: bool,
    /// A resume signal source must be bound (halting callback/approval nodes
    /// present).
    pub needs_resume_signal_source: bool,
    /// A checkpoint blob store must be bound (blob spill threshold
    /// configured while checkpointing is enabled).
    pub needs_checkpoint_blob_store: bool,
}

/// Kind of trigger surface, as derivable from the IR today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TriggerKind {
    /// Trigger is wired to an HTTP entrypoint (route/method declared).
    Http,
    /// Trigger is wired to a schedule (cron) entrypoint.
    ///
    /// Introduced with a deliberate schema-version migration; readers reject
    /// unknown unstable minor versions and enum values.
    Schedule,
    /// Trigger has no entrypoint wiring recorded in the IR; invocation
    /// mechanism is host-defined.
    Unspecified,
}

/// A trigger node that originates executions of the flow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TriggerRequirement {
    /// Node alias within the flow.
    pub alias: String,
    /// Fully-qualified implementation identifier.
    pub identifier: String,
    /// Trigger surface kind.
    pub kind: TriggerKind,
    /// Cron expressions of the schedule entrypoints wired to this trigger
    /// alias (exactly one in v1). Empty for non-schedule triggers;
    /// skip-when-absent keeps existing manifests byte-identical.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub crons: Vec<String>,
}

/// External ingress wiring for one entrypoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EntrypointRequirement {
    /// Trigger node alias for ingress.
    pub trigger_alias: String,
    /// Capture node alias for response/egress.
    pub capture_alias: String,
    /// Canonical route path when declared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_path: Option<String>,
    /// HTTP method for HTTP-capable hosts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Cron expression for schedule-shaped entrypoints (5-field Cloudflare
    /// dialect, UTC), byte-verbatim from the IR. The wrangler renderer's
    /// `[triggers].crons` union is built from this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    /// Non-authoritative aliases for the canonical route.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub route_aliases: Vec<String>,
    /// Response deadline in milliseconds. Not recorded in Flow IR metadata
    /// today; populated during bundle assembly from the flow registry's
    /// entrypoint specs when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
}

/// Host constraints derivable from the IR today.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HostConstraints {
    /// Flow targets the WASM profile and must compile/execute on wasm32.
    pub requires_wasm32_compatibility: bool,
    /// At least one connector operation is selected in `bound_connection`
    /// mode, so the host must provide a connector runtime (today) or a
    /// resolved bindings.lock (once lock-time resolution lands, packet C2).
    pub requires_connector_runtime: bool,
    /// Flow embeds subflow nodes; the host must support subflow expansion.
    pub has_subflows: bool,
}

impl FlowRequirements {
    /// Derive the identity-only requirements manifest from a Flow IR.
    ///
    /// This performs NO execution, connector-runtime calls, or descriptor
    /// lookups: every populated field is a pure function of the IR. Contract
    /// scopes are resolved explicitly with [`Self::resolve_scope_closure`].
    /// Callers should pass IR that already passed kernel-plan validation;
    /// `kernel_plan::derive_requirements` wraps this for `ValidatedIR`.
    pub fn derive(flow: &FlowIR) -> Result<Self, RequirementsError> {
        let (connectors, unresolved_subflows) = derive_connectors(flow)?;
        let has_contracts = connectors.iter().any(|connector| {
            connector
                .operations
                .iter()
                .any(|operation| operation.broker_contract.is_some())
        });
        let has_authority_contracts = recursive_nodes(flow)?.0.iter().any(|located| {
            located
                .node
                .broker_authority
                .as_ref()
                .is_some_and(|authority| !authority.operation_budgets().is_empty())
        });
        let scope_resolution =
            if has_contracts || has_authority_contracts || !unresolved_subflows.is_empty() {
                ScopeClosureResolution::Unresolved {
                    unresolved_subflows,
                }
            } else {
                ScopeClosureResolution::NotRequired
            };
        Ok(Self {
            schema_version: FLOW_REQUIREMENTS_SCHEMA_VERSION.to_string(),
            flow: FlowIdentity {
                id: flow.id.clone(),
                name: flow.name.clone(),
                version: flow.version.to_string(),
            },
            profile: flow.profile,
            effects: derive_effects(flow)?,
            implementation_dependencies: derive_implementation_dependencies(flow)?,
            connectors,
            scope_closures: Vec::new(),
            scope_resolution,
            durability: derive_durability(flow),
            triggers: derive_triggers(flow),
            entrypoints: derive_entrypoints(flow),
            host: derive_host_constraints(flow)?,
            flow_ir_hash: None,
        })
    }

    /// Resolve descriptor-owned scopes by exact contract hash and populate a
    /// report-only closure partitioned by connection aggregate key.
    pub fn resolve_scope_closure(
        mut self,
        flow: &FlowIR,
        descriptors: &[ContractScopeDescriptor],
    ) -> Result<Self, RequirementsError> {
        let (connectors, unresolved_subflows) = derive_connectors(flow)?;
        if self.flow.id != flow.id
            || self.flow.name != flow.name
            || self.flow.version != flow.version.to_string()
            || self.connectors != connectors
        {
            return Err(RequirementsError::ScopeResolutionFlowMismatch);
        }
        let expected_flow_hash = self
            .flow_ir_hash
            .as_deref()
            .ok_or(RequirementsError::MissingScopeResolutionFlowHash)?;
        let flow_bytes = serde_json::to_vec_pretty(flow)
            .map_err(|_| RequirementsError::ScopeResolutionFlowMismatch)?;
        let actual_flow_hash = format!("sha256:{}", hex::encode(Sha256::digest(&flow_bytes)));
        if actual_flow_hash != expected_flow_hash {
            return Err(RequirementsError::ScopeResolutionFlowMismatch);
        }
        if let Some(node) = unresolved_subflows.into_iter().next() {
            return Err(RequirementsError::MissingSubflowIr { node });
        }
        self.scope_closures = derive_scope_closures(flow, descriptors)?;
        self.scope_resolution = if self.connectors.iter().any(|connector| {
            connector
                .operations
                .iter()
                .any(|op| op.broker_contract.is_some())
        }) {
            ScopeClosureResolution::Resolved
        } else {
            ScopeClosureResolution::NotRequired
        };
        Ok(self)
    }

    /// Record the hash of the serialized Flow IR this manifest describes.
    pub fn with_flow_ir_hash(mut self, hash: impl Into<String>) -> Self {
        self.flow_ir_hash = Some(hash.into());
        self
    }
}

fn derive_effects(flow: &FlowIR) -> Result<EffectRequirements, RequirementsError> {
    let mut union: BTreeSet<EffectHint> = BTreeSet::new();
    let mut per_node: BTreeMap<String, Vec<EffectHint>> = BTreeMap::new();

    for node in &flow.nodes {
        let mut node_hints: BTreeSet<EffectHint> = BTreeSet::new();
        for hint in &node.effect_hints {
            if hint.starts_with(POLICY_MARKER_PREFIX) {
                // Policy lint markers are not capability requirements.
                continue;
            }
            let parsed =
                EffectHint::parse(hint).map_err(|err| RequirementsError::UnknownEffectHint {
                    node: node.alias.clone(),
                    hint: err.value,
                })?;
            node_hints.insert(parsed);
        }
        if !node_hints.is_empty() {
            union.extend(node_hints.iter().copied());
            per_node.insert(node.alias.clone(), sorted_hints(&node_hints));
        }
    }

    let families: BTreeSet<EffectHint> = union.iter().map(|hint| hint.family()).collect();

    Ok(EffectRequirements {
        union: sorted_hints(&union),
        families: sorted_hints(&families),
        per_node,
    })
}

fn sorted_hints(hints: &BTreeSet<EffectHint>) -> Vec<EffectHint> {
    let mut out: Vec<EffectHint> = hints.iter().copied().collect();
    out.sort_by_key(|hint| hint.as_str());
    out
}

fn derive_implementation_dependencies(
    flow: &FlowIR,
) -> Result<Vec<ImplementationDependencyRequirement>, RequirementsError> {
    let mut grouped: BTreeMap<(ImplementationDependencyKind, String), Vec<String>> =
        BTreeMap::new();

    for node in &flow.nodes {
        for dependency in &node.implementation_dependencies {
            if !valid_implementation_key(&dependency.key) {
                return Err(RequirementsError::InvalidImplementationDependency {
                    node: node.alias.clone(),
                    key: dependency.key.clone(),
                });
            }
            grouped
                .entry((dependency.kind, dependency.key.clone()))
                .or_default()
                .push(node.alias.clone());
        }
    }

    Ok(grouped
        .into_iter()
        .map(|((kind, key), mut nodes)| {
            nodes.sort();
            nodes.dedup();
            ImplementationDependencyRequirement { kind, key, nodes }
        })
        .collect())
}

fn valid_implementation_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 128
        && key
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

struct LocatedNode<'a> {
    node: &'a NodeIR,
    qualified_alias: String,
}

fn recursive_nodes(
    flow: &FlowIR,
) -> Result<(Vec<LocatedNode<'_>>, Vec<String>), RequirementsError> {
    let mut nodes = Vec::new();
    let mut unresolved_subflows = Vec::new();
    let mut ancestry = Vec::new();
    collect_recursive_nodes(
        flow,
        "",
        0,
        &mut ancestry,
        &mut nodes,
        &mut unresolved_subflows,
    )?;
    Ok((nodes, unresolved_subflows))
}

fn collect_recursive_nodes<'a>(
    flow: &'a FlowIR,
    prefix: &str,
    depth: usize,
    ancestry: &mut Vec<String>,
    nodes: &mut Vec<LocatedNode<'a>>,
    unresolved_subflows: &mut Vec<String>,
) -> Result<(), RequirementsError> {
    if depth > MAX_SUBFLOW_REQUIREMENTS_DEPTH {
        return Err(RequirementsError::SubflowDepthExceeded {
            maximum: MAX_SUBFLOW_REQUIREMENTS_DEPTH,
        });
    }
    let flow_id = flow.id.as_str().to_string();
    if let Some(cycle_start) = ancestry.iter().position(|id| id == &flow_id) {
        let mut chain = ancestry[cycle_start..].to_vec();
        chain.push(flow_id);
        return Err(RequirementsError::SubflowCycle {
            chain: chain.join(" -> "),
        });
    }
    ancestry.push(flow_id);

    for node in &flow.nodes {
        let qualified_alias = if prefix.is_empty() {
            node.alias.clone()
        } else {
            format!("{prefix}/{}", node.alias)
        };
        nodes.push(LocatedNode {
            node,
            qualified_alias: qualified_alias.clone(),
        });
        if node.kind == NodeKind::Subflow {
            if let Some(subflow) = node.subflow_ir.as_deref() {
                collect_recursive_nodes(
                    subflow,
                    &qualified_alias,
                    depth + 1,
                    ancestry,
                    nodes,
                    unresolved_subflows,
                )?;
            } else {
                unresolved_subflows.push(qualified_alias);
            }
        }
    }

    ancestry.pop();
    Ok(())
}

fn derive_connectors(
    flow: &FlowIR,
) -> Result<(Vec<ConnectorRequirement>, Vec<String>), RequirementsError> {
    // connector_id -> operation_id -> accumulating requirement
    let mut grouped: BTreeMap<String, BTreeMap<String, ConnectorOperationRequirement>> =
        BTreeMap::new();
    let (nodes, unresolved_subflows) = recursive_nodes(flow)?;

    for located in nodes {
        for op in &located.node.connector_ops {
            let entry = grouped
                .entry(op.connector_id.clone())
                .or_default()
                .entry(op.operation_id.clone())
                .or_insert_with(|| ConnectorOperationRequirement {
                    operation_id: op.operation_id.clone(),
                    broker_contract: op.broker_contract.clone(),
                    roles: op.roles.clone(),
                    supported_resolution_modes: op.supported_resolution_modes.clone(),
                    default_resolution_mode: op.default_resolution_mode,
                    selected_resolution_modes: Vec::new(),
                    requires_bound_connection: false,
                    nodes: Vec::new(),
                });

            if entry.broker_contract != op.broker_contract {
                return Err(RequirementsError::ConflictingOperationContract {
                    operation_id: op.operation_id.clone(),
                });
            }
            if !entry
                .selected_resolution_modes
                .contains(&op.selected_resolution_mode)
            {
                entry
                    .selected_resolution_modes
                    .push(op.selected_resolution_mode);
            }
            if op.selected_resolution_mode == ConnectorResolutionModeDecl::BoundConnection {
                entry.requires_bound_connection = true;
            }
            if !entry.nodes.contains(&located.qualified_alias) {
                entry.nodes.push(located.qualified_alias.clone());
            }
        }
    }

    let connectors = grouped
        .into_iter()
        .map(|(connector_id, operations)| ConnectorRequirement {
            connector_id,
            operations: operations
                .into_values()
                .map(|mut op| {
                    op.selected_resolution_modes
                        .sort_by_key(|mode| resolution_mode_rank(*mode));
                    op.nodes.sort();
                    op
                })
                .collect(),
        })
        .collect();
    Ok((connectors, unresolved_subflows))
}

fn derive_scope_closures(
    flow: &FlowIR,
    descriptors: &[ContractScopeDescriptor],
) -> Result<Vec<ConnectionScopeClosure>, RequirementsError> {
    let mut by_hash: BTreeMap<&str, &ContractScopeDescriptor> = BTreeMap::new();
    for descriptor in descriptors {
        if !valid_minimum_scopes(&descriptor.minimum_scopes) {
            return Err(RequirementsError::InvalidMinimumScopes {
                contract_id: descriptor.contract_id.clone(),
            });
        }
        if let Some(existing) = by_hash.insert(&descriptor.contract_hash, descriptor)
            && existing != descriptor
        {
            return Err(RequirementsError::ConflictingContractDescriptor {
                contract_hash: descriptor.contract_hash.clone(),
            });
        }
    }

    let mut grouped: BTreeMap<Option<String>, (BTreeSet<(String, String)>, BTreeSet<String>)> =
        BTreeMap::new();
    let (nodes, unresolved_subflows) = recursive_nodes(flow)?;
    if let Some(node) = unresolved_subflows.into_iter().next() {
        return Err(RequirementsError::MissingSubflowIr { node });
    }
    for located in nodes {
        if let Some(authority) = &located.node.broker_authority {
            for budget in authority.operation_budgets() {
                let operation_id = budget
                    .contract_id
                    .rsplit_once('@')
                    .map_or(budget.contract_id.as_str(), |(operation_id, _)| {
                        operation_id
                    });
                let Some(operation) = located
                    .node
                    .connector_ops
                    .iter()
                    .find(|operation| operation.operation_id == operation_id)
                else {
                    return Err(RequirementsError::MissingPinnedContractIdentity {
                        node: located.qualified_alias.clone(),
                        contract_id: budget.contract_id.clone(),
                        operation_id: operation_id.to_string(),
                    });
                };
                let Some(identity) = &operation.broker_contract else {
                    return Err(RequirementsError::MissingPinnedContractIdentity {
                        node: located.qualified_alias.clone(),
                        contract_id: budget.contract_id.clone(),
                        operation_id: operation.operation_id.clone(),
                    });
                };
                if identity.contract_id != budget.contract_id {
                    return Err(RequirementsError::ContractAuthorityMismatch {
                        node: located.qualified_alias.clone(),
                        contract_id: identity.contract_id.clone(),
                    });
                }
            }
        }
        for operation in &located.node.connector_ops {
            let Some(contract) = &operation.broker_contract else {
                continue;
            };
            let descriptor = by_hash
                .get(contract.contract_hash.as_str())
                .ok_or_else(|| RequirementsError::UnknownContractHash {
                    contract_id: contract.contract_id.clone(),
                    contract_hash: contract.contract_hash.clone(),
                })?;
            if descriptor.contract_id != contract.contract_id {
                return Err(RequirementsError::ContractIdentityMismatch {
                    contract_hash: contract.contract_hash.clone(),
                    expected_contract_id: contract.contract_id.clone(),
                    actual_contract_id: descriptor.contract_id.clone(),
                });
            }

            let connection_aggregate_key = if let Some(authority) = &located.node.broker_authority {
                authority
                    .operation_budgets()
                    .iter()
                    .find(|budget| budget.contract_id == contract.contract_id)
                    .ok_or_else(|| RequirementsError::ContractAuthorityMismatch {
                        node: located.qualified_alias.clone(),
                        contract_id: contract.contract_id.clone(),
                    })?
                    .connection_aggregate_key
                    .clone()
            } else {
                None
            };
            let (contracts, scopes) = grouped.entry(connection_aggregate_key).or_default();
            contracts.insert((contract.contract_id.clone(), contract.contract_hash.clone()));
            scopes.extend(descriptor.minimum_scopes.iter().cloned());
        }
    }

    Ok(grouped
        .into_iter()
        .map(
            |(connection_aggregate_key, (contracts, required_scopes))| ConnectionScopeClosure {
                connection_aggregate_key,
                contracts: contracts
                    .into_iter()
                    .map(|(contract_id, contract_hash)| BrokerContractIdentityIR {
                        contract_id,
                        contract_hash,
                    })
                    .collect(),
                required_scopes: required_scopes.into_iter().collect(),
            },
        )
        .collect())
}

fn valid_minimum_scopes(scopes: &[String]) -> bool {
    scopes.len() <= 1024
        && scopes
            .iter()
            .all(|scope| !scope.is_empty() && scope.len() <= 1024 && scope.is_ascii())
        && scopes.windows(2).all(|pair| pair[0] < pair[1])
}

/// Stable ordering for resolution modes in derived output (declaration order
/// of the enum; the enum does not implement `Ord`).
const fn resolution_mode_rank(mode: ConnectorResolutionModeDecl) -> u8 {
    match mode {
        ConnectorResolutionModeDecl::BoundConnection => 0,
        ConnectorResolutionModeDecl::LateBoundRefs => 1,
        ConnectorResolutionModeDecl::InlinePayload => 2,
    }
}

fn derive_durability(flow: &FlowIR) -> DurabilityRequirements {
    let mode = flow.policies.durability.mode;
    let has_halting_nodes = flow.nodes.iter().any(|node| node.durability.halts);
    let checkpointing = mode != DurabilityMode::Off;

    let needs_resume_scheduler = has_halting_nodes
        && flow
            .nodes
            .iter()
            .any(|node| RESUME_SCHEDULER_IDENTIFIERS.contains(&node.identifier.as_str()));
    let needs_resume_signal_source = has_halting_nodes
        && flow
            .nodes
            .iter()
            .any(|node| RESUME_SIGNAL_IDENTIFIERS.contains(&node.identifier.as_str()));

    DurabilityRequirements {
        mode,
        has_halting_nodes,
        needs_checkpoint_store: checkpointing,
        needs_resume_scheduler,
        needs_resume_signal_source,
        needs_checkpoint_blob_store: checkpointing
            && flow.policies.durability.blob_threshold_bytes.is_some(),
    }
}

fn derive_triggers(flow: &FlowIR) -> Vec<TriggerRequirement> {
    flow.nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Trigger)
        .map(|node| {
            let mut wired_to_entrypoint = false;
            let mut crons: Vec<String> = Vec::new();
            for entry in &flow.metadata.entrypoints {
                if entry.trigger_alias != node.alias {
                    continue;
                }
                wired_to_entrypoint = true;
                if let Some(schedule) = &entry.schedule {
                    crons.push(schedule.clone());
                }
            }
            // TRIG003 validation guarantees an alias is never wired to both
            // schedule and HTTP entrypoints, so the cases below are disjoint
            // on validated IR.
            let kind = if !crons.is_empty() {
                TriggerKind::Schedule
            } else if wired_to_entrypoint {
                TriggerKind::Http
            } else {
                TriggerKind::Unspecified
            };
            TriggerRequirement {
                alias: node.alias.clone(),
                identifier: node.identifier.clone(),
                kind,
                crons,
            }
        })
        .collect()
}

fn derive_entrypoints(flow: &FlowIR) -> Vec<EntrypointRequirement> {
    flow.metadata
        .entrypoints
        .iter()
        .map(|entry| EntrypointRequirement {
            trigger_alias: entry.trigger_alias.clone(),
            capture_alias: entry.capture_alias.clone(),
            route_path: entry.route_path.clone(),
            method: entry.method.clone(),
            schedule: entry.schedule.clone(),
            route_aliases: entry.route_aliases.clone(),
            deadline_ms: None,
        })
        .collect()
}

fn derive_host_constraints(flow: &FlowIR) -> Result<HostConstraints, RequirementsError> {
    let (nodes, _) = recursive_nodes(flow)?;
    let requires_connector_runtime =
        nodes.iter().any(|located| {
            located.node.connector_ops.iter().any(|op| {
                op.selected_resolution_mode == ConnectorResolutionModeDecl::BoundConnection
            })
        });
    Ok(HostConstraints {
        requires_wasm32_compatibility: flow.profile == Profile::Wasm,
        requires_connector_runtime,
        has_subflows: nodes
            .iter()
            .any(|located| located.node.kind == NodeKind::Subflow),
    })
}

#[cfg(test)]
mod tests {
    use semver::Version;

    use super::*;
    use crate::builder::FlowBuilder;
    use crate::effects::{Determinism, Effects};
    use crate::ir::{
        ConnectorOpRefIR, ConnectorResolutionModeDecl, ConnectorRoleKindDecl,
        ConnectorRoleRequirementIR, NodeSpec, SchemaSpec,
    };

    fn spec_with_hints(
        identifier: &'static str,
        name: &'static str,
        effects: Effects,
        effect_hints: &'static [&'static str],
    ) -> NodeSpec {
        NodeSpec::inline_with_hints(
            identifier,
            name,
            SchemaSpec::Opaque,
            SchemaSpec::Opaque,
            effects,
            Determinism::BestEffort,
            None,
            &[],
            effect_hints,
        )
    }

    const READER_HINTS: &[&str] = &[EffectHint::HttpRead.as_str()];
    const WRITER_HINTS: &[&str] = &[EffectHint::HttpRead.as_str(), EffectHint::KvWrite.as_str()];

    fn two_node_flow() -> FlowIR {
        let mut builder = FlowBuilder::new("reqs_demo", Version::new(1, 0, 0), Profile::Web);
        let reader = spec_with_hints("tests::reader", "Reader", Effects::ReadOnly, READER_HINTS);
        let writer = spec_with_hints("tests::writer", "Writer", Effects::Effectful, WRITER_HINTS);
        let reader = builder.add_node("reader", &reader).expect("reader");
        let writer = builder.add_node("writer", &writer).expect("writer");
        builder.connect(&reader, &writer);
        builder.build()
    }

    #[test]
    fn derives_union_and_per_node_attribution() {
        let flow = two_node_flow();
        let reqs = FlowRequirements::derive(&flow).expect("derive");

        assert_eq!(reqs.schema_version, FLOW_REQUIREMENTS_SCHEMA_VERSION);
        assert_eq!(reqs.flow.name, "reqs_demo");
        assert_eq!(reqs.flow.version, "1.0.0");
        assert_eq!(
            reqs.effects.union,
            vec![EffectHint::HttpRead, EffectHint::KvWrite]
        );
        assert_eq!(
            reqs.effects.families,
            vec![EffectHint::Http, EffectHint::Kv]
        );
        assert_eq!(
            reqs.effects.per_node.get("reader"),
            Some(&vec![EffectHint::HttpRead])
        );
        assert_eq!(
            reqs.effects.per_node.get("writer"),
            Some(&vec![EffectHint::HttpRead, EffectHint::KvWrite])
        );
    }

    fn transform_dependency(key: &str) -> crate::ImplementationDependency {
        crate::ImplementationDependency {
            kind: crate::ImplementationDependencyKind::SandboxedTransform,
            key: key.to_string(),
        }
    }

    #[test]
    fn node_identifiers_never_invent_implementation_requirements() {
        let mut flow = two_node_flow();
        flow.nodes[0].identifier = "any.product.specific.identifier".to_string();
        flow.nodes[0].summary = Some("mentions a transform key only as prose".to_string());

        let reqs = FlowRequirements::derive(&flow).expect("derive");
        assert!(reqs.implementation_dependencies.is_empty());
    }

    #[test]
    fn unknown_implementation_dependency_kind_is_rejected() {
        let flow = two_node_flow();
        let mut value = serde_json::to_value(flow).expect("serialize flow");
        value["nodes"][0]["implementationDependencies"] = serde_json::json!([{
            "kind": "sandboxed_transform_typo",
            "key": "example.transform.v1"
        }]);
        let error = serde_json::from_value::<FlowIR>(value).expect_err("unknown dependency kind");
        assert!(
            error.to_string().contains("unknown variant"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn malformed_dependency_keys_fail_closed() {
        let mut flow = two_node_flow();
        flow.nodes[0].implementation_dependencies = vec![transform_dependency("bad key")];
        assert!(matches!(
            FlowRequirements::derive(&flow),
            Err(RequirementsError::InvalidImplementationDependency { .. })
        ));
    }

    #[test]
    fn dependencies_are_derived_only_from_typed_metadata() {
        let mut flow = two_node_flow();
        flow.nodes[1].identifier = "example.inline_node".to_string();
        flow.nodes[1].implementation_dependencies = vec![
            transform_dependency("example.transform.v1"),
            transform_dependency("example.transform.v1"),
        ];

        let reqs = FlowRequirements::derive(&flow).expect("derive");
        assert_eq!(
            reqs.implementation_dependencies,
            vec![ImplementationDependencyRequirement {
                kind: ImplementationDependencyKind::SandboxedTransform,
                key: "example.transform.v1".to_string(),
                nodes: vec!["writer".to_string()],
            }]
        );
    }

    #[test]
    fn dependency_aliases_are_sorted_and_grouped_by_kind_and_key() {
        let mut flow = two_node_flow();
        for node in &mut flow.nodes {
            node.implementation_dependencies = vec![transform_dependency("example.transform.v1")];
        }

        let reqs = FlowRequirements::derive(&flow).expect("derive");
        assert_eq!(reqs.implementation_dependencies.len(), 1);
        assert_eq!(
            reqs.implementation_dependencies[0].nodes,
            vec!["reader".to_string(), "writer".to_string()]
        );
    }

    #[test]
    fn policy_markers_are_not_capability_requirements() {
        let mut flow = two_node_flow();
        flow.nodes[0]
            .effect_hints
            .push("policy::json_boundary".to_string());
        let reqs = FlowRequirements::derive(&flow).expect("derive");
        assert_eq!(
            reqs.effects.union,
            vec![EffectHint::HttpRead, EffectHint::KvWrite]
        );
    }

    #[test]
    fn unknown_hint_fails_closed() {
        let mut flow = two_node_flow();
        let typo = ["resource", "::http_raed"].concat();
        flow.nodes[0].effect_hints.push(typo.clone());
        let err = FlowRequirements::derive(&flow).expect_err("must fail closed");
        assert_eq!(
            err,
            RequirementsError::UnknownEffectHint {
                node: "reader".to_string(),
                hint: typo,
            }
        );
    }

    #[test]
    fn derives_connector_contracts_without_runtime_calls() {
        let mut flow = two_node_flow();
        flow.nodes[1].connector_ops.push(ConnectorOpRefIR {
            operation_id: "connector.demo.op".to_string(),
            connector_id: "connector.demo".to_string(),
            broker_contract: None,
            roles: vec![ConnectorRoleRequirementIR {
                kind: ConnectorRoleKindDecl::OutboundAuth,
                name: "api".to_string(),
                expected_handle_kind: "secret.api_key".to_string(),
                required: true,
            }],
            default_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
            selected_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
            supported_resolution_modes: vec![
                ConnectorResolutionModeDecl::BoundConnection,
                ConnectorResolutionModeDecl::InlinePayload,
            ],
        });

        let reqs = FlowRequirements::derive(&flow).expect("derive");
        assert_eq!(reqs.connectors.len(), 1);
        let connector = &reqs.connectors[0];
        assert_eq!(connector.connector_id, "connector.demo");
        let op = &connector.operations[0];
        assert_eq!(op.operation_id, "connector.demo.op");
        assert!(op.requires_bound_connection);
        assert_eq!(op.nodes, vec!["writer".to_string()]);
        assert_eq!(op.roles.len(), 1);
        assert!(reqs.host.requires_connector_runtime);
    }

    /// Additive-field compatibility: a required role (the only kind that
    /// existed before `required` was added) serializes without the field, so
    /// pre-existing IR/manifest goldens stay byte-identical, and legacy JSON
    /// without the field deserializes as `required: true`.
    #[test]
    fn required_role_serialization_is_backward_compatible() {
        let role = ConnectorRoleRequirementIR {
            kind: ConnectorRoleKindDecl::OutboundAuth,
            name: "api".to_string(),
            expected_handle_kind: "secret.api_key".to_string(),
            required: true,
        };
        let json = serde_json::to_value(&role).expect("serialize");
        assert_eq!(
            json,
            serde_json::json!({
                "kind": "outbound_auth",
                "name": "api",
                "expected_handle_kind": "secret.api_key",
            }),
            "required: true must be omitted so existing goldens stay byte-identical"
        );

        let legacy = serde_json::json!({
            "kind": "outbound_auth",
            "name": "api",
            "expected_handle_kind": "secret.api_key",
        });
        let parsed: ConnectorRoleRequirementIR =
            serde_json::from_value(legacy).expect("deserialize legacy role");
        assert!(parsed.required, "missing `required` must default to true");
    }

    /// An optional role (`required: false`) round-trips IR -> manifest ->
    /// JSON -> back with the flag preserved and explicitly serialized.
    #[test]
    fn optional_role_round_trips_through_ir_and_manifest() {
        let mut flow = two_node_flow();
        flow.nodes[1].connector_ops.push(ConnectorOpRefIR {
            operation_id: "connector.http.get".to_string(),
            connector_id: "connector.http".to_string(),
            broker_contract: None,
            roles: vec![
                ConnectorRoleRequirementIR {
                    kind: ConnectorRoleKindDecl::EndpointProfile,
                    name: "http_target".to_string(),
                    expected_handle_kind: "endpoint.profile".to_string(),
                    required: true,
                },
                ConnectorRoleRequirementIR {
                    kind: ConnectorRoleKindDecl::OutboundAuth,
                    name: "http_target_auth".to_string(),
                    expected_handle_kind: "http.bearer".to_string(),
                    required: false,
                },
            ],
            default_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
            selected_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
            supported_resolution_modes: vec![ConnectorResolutionModeDecl::BoundConnection],
        });

        // Flow IR JSON round-trip preserves the optional flag.
        let ir_json = serde_json::to_value(&flow).expect("serialize flow ir");
        let flow_back: FlowIR = serde_json::from_value(ir_json).expect("deserialize flow ir");
        assert_eq!(
            flow_back.nodes[1].connector_ops,
            flow.nodes[1].connector_ops
        );

        // Manifest derivation carries the flag; JSON round-trips it.
        let reqs = FlowRequirements::derive(&flow).expect("derive");
        let op = &reqs.connectors[0].operations[0];
        assert_eq!(op.roles.len(), 2);
        assert!(
            op.roles
                .iter()
                .any(|role| role.name == "http_target" && role.required)
        );
        assert!(
            op.roles
                .iter()
                .any(|role| role.name == "http_target_auth" && !role.required)
        );

        let manifest_json = serde_json::to_value(&reqs).expect("serialize manifest");
        let auth_role = &manifest_json["connectors"][0]["operations"][0]["roles"][1];
        assert_eq!(
            auth_role["required"],
            serde_json::json!(false),
            "required: false must serialize explicitly"
        );
        let back: FlowRequirements =
            serde_json::from_value(manifest_json).expect("deserialize manifest");
        assert_eq!(back, reqs);
    }

    /// A flow with no optional roles must produce a manifest whose JSON
    /// contains no `required` key anywhere (byte-identity guard for the
    /// goldens that predate the field).
    #[test]
    fn manifest_without_optional_roles_never_mentions_required_field() {
        let mut flow = two_node_flow();
        flow.nodes[1].connector_ops.push(ConnectorOpRefIR {
            operation_id: "connector.demo.op".to_string(),
            connector_id: "connector.demo".to_string(),
            broker_contract: None,
            roles: vec![ConnectorRoleRequirementIR {
                kind: ConnectorRoleKindDecl::OutboundAuth,
                name: "api".to_string(),
                expected_handle_kind: "secret.api_key".to_string(),
                required: true,
            }],
            default_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
            selected_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
            supported_resolution_modes: vec![ConnectorResolutionModeDecl::BoundConnection],
        });

        let reqs = FlowRequirements::derive(&flow).expect("derive");
        let manifest_json = serde_json::to_string(&reqs).expect("serialize manifest");
        assert!(
            !manifest_json.contains("\"required\""),
            "all-required manifest must not mention the `required` key"
        );
        let ir_json = serde_json::to_string(&flow).expect("serialize flow ir");
        assert!(
            !ir_json.contains("\"required\""),
            "all-required flow IR must not mention the `required` key"
        );
    }

    #[test]
    fn durability_defaults_require_checkpoint_store() {
        let flow = two_node_flow();
        let reqs = FlowRequirements::derive(&flow).expect("derive");
        assert_eq!(reqs.durability.mode, DurabilityMode::Partial);
        assert!(reqs.durability.needs_checkpoint_store);
        assert!(!reqs.durability.needs_resume_scheduler);
        assert!(!reqs.durability.needs_checkpoint_blob_store);
    }

    #[test]
    fn manifest_round_trips_through_json() {
        let flow = two_node_flow();
        let reqs = FlowRequirements::derive(&flow)
            .expect("derive")
            .with_flow_ir_hash(
                "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            );
        let json = serde_json::to_value(&reqs).expect("serialize");
        let back: FlowRequirements = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back, reqs);
    }

    #[test]
    fn unknown_unstable_minor_schema_is_rejected() {
        let mut json = serde_json::to_value(
            FlowRequirements::derive(&two_node_flow()).expect("derive requirements"),
        )
        .unwrap();
        json["schema_version"] = serde_json::Value::String("0.5".to_string());
        let error = serde_json::from_value::<FlowRequirements>(json).unwrap_err();
        assert!(error.to_string().contains("expected `0.4`"));
    }
}
