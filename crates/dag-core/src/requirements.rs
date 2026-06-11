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
//! into it (`NodeIR.connector_ops`). Where bound-connection resolution happens
//! today at runtime preflight (host-inproc), this manifest records only the
//! DECLARED contract (supported resolution modes, role requirements);
//! instance-binding satisfaction is a bindings.lock-time concern. See
//! `impl-docs/spec/flow-requirements.md`.

use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::effect_hint::EffectHint;
use crate::ir::{
    ConnectorResolutionModeDecl, ConnectorRoleRequirementIR, DurabilityMode, FlowIR, FlowId,
    NodeKind, Profile,
};

/// Version of the FlowRequirements manifest shape itself (not the flow).
///
/// Bump on any breaking change to the manifest structure; consumers must
/// reject schema versions they do not understand.
pub const FLOW_REQUIREMENTS_SCHEMA_VERSION: &str = "0.1";

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
}

/// Static requirements manifest for a single flow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FlowRequirements {
    /// Manifest shape version ([`FLOW_REQUIREMENTS_SCHEMA_VERSION`]).
    pub schema_version: String,
    /// Identity of the flow this manifest describes.
    pub flow: FlowIdentity,
    /// Target execution profile declared by the flow.
    pub profile: Profile,
    /// Typed capability requirements (union + per-node attribution).
    pub effects: EffectRequirements,
    /// Connector operation requirements, grouped by connector family.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connectors: Vec<ConnectorRequirement>,
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

/// Requirements contributed by one connector family.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ConnectorRequirement {
    /// Connector family identifier (e.g. `connector.formualizer.sheetport`).
    pub connector_id: String,
    /// Operations of this connector the flow may invoke.
    pub operations: Vec<ConnectorOperationRequirement>,
}

/// Declared contract for a single connector operation used by the flow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ConnectorOperationRequirement {
    /// Operation identifier (e.g. `connector.formualizer.sheetport.evaluate`).
    pub operation_id: String,
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
    /// Derive the requirements manifest from a Flow IR.
    ///
    /// This performs NO execution and NO connector-runtime calls: every field
    /// is a pure function of the IR. Callers should pass IR that already
    /// passed kernel-plan validation; `kernel_plan::derive_requirements`
    /// wraps this for `ValidatedIR`. On unvalidated IR with unknown hint
    /// strings this fails closed with [`RequirementsError::UnknownEffectHint`]
    /// (the same condition kernel-plan rejects as EFFECT202).
    pub fn derive(flow: &FlowIR) -> Result<Self, RequirementsError> {
        Ok(Self {
            schema_version: FLOW_REQUIREMENTS_SCHEMA_VERSION.to_string(),
            flow: FlowIdentity {
                id: flow.id.clone(),
                name: flow.name.clone(),
                version: flow.version.to_string(),
            },
            profile: flow.profile,
            effects: derive_effects(flow)?,
            connectors: derive_connectors(flow),
            durability: derive_durability(flow),
            triggers: derive_triggers(flow),
            entrypoints: derive_entrypoints(flow),
            host: derive_host_constraints(flow),
            flow_ir_hash: None,
        })
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

fn derive_connectors(flow: &FlowIR) -> Vec<ConnectorRequirement> {
    // connector_id -> operation_id -> accumulating requirement
    let mut grouped: BTreeMap<String, BTreeMap<String, ConnectorOperationRequirement>> =
        BTreeMap::new();

    for node in &flow.nodes {
        for op in &node.connector_ops {
            let entry = grouped
                .entry(op.connector_id.clone())
                .or_default()
                .entry(op.operation_id.clone())
                .or_insert_with(|| ConnectorOperationRequirement {
                    operation_id: op.operation_id.clone(),
                    roles: op.roles.clone(),
                    supported_resolution_modes: op.supported_resolution_modes.clone(),
                    default_resolution_mode: op.default_resolution_mode,
                    selected_resolution_modes: Vec::new(),
                    requires_bound_connection: false,
                    nodes: Vec::new(),
                });

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
            if !entry.nodes.contains(&node.alias) {
                entry.nodes.push(node.alias.clone());
            }
        }
    }

    grouped
        .into_iter()
        .map(|(connector_id, operations)| ConnectorRequirement {
            connector_id,
            operations: operations
                .into_values()
                .map(|mut op| {
                    op.selected_resolution_modes.sort_by_key(|mode| resolution_mode_rank(*mode));
                    op.nodes.sort();
                    op
                })
                .collect(),
        })
        .collect()
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
            let wired_to_entrypoint = flow
                .metadata
                .entrypoints
                .iter()
                .any(|entry| entry.trigger_alias == node.alias);
            TriggerRequirement {
                alias: node.alias.clone(),
                identifier: node.identifier.clone(),
                kind: if wired_to_entrypoint {
                    TriggerKind::Http
                } else {
                    TriggerKind::Unspecified
                },
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
            route_aliases: entry.route_aliases.clone(),
            deadline_ms: None,
        })
        .collect()
}

fn derive_host_constraints(flow: &FlowIR) -> HostConstraints {
    let requires_connector_runtime = flow.nodes.iter().any(|node| {
        node.connector_ops.iter().any(|op| {
            op.selected_resolution_mode == ConnectorResolutionModeDecl::BoundConnection
        })
    });
    HostConstraints {
        requires_wasm32_compatibility: flow.profile == Profile::Wasm,
        requires_connector_runtime,
        has_subflows: flow.nodes.iter().any(|node| node.kind == NodeKind::Subflow),
    }
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
            roles: vec![ConnectorRoleRequirementIR {
                kind: ConnectorRoleKindDecl::OutboundAuth,
                name: "api".to_string(),
                expected_handle_kind: "secret.api_key".to_string(),
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
            .with_flow_ir_hash("sha256:0000000000000000000000000000000000000000000000000000000000000000");
        let json = serde_json::to_value(&reqs).expect("serialize");
        let back: FlowRequirements = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back, reqs);
    }
}
