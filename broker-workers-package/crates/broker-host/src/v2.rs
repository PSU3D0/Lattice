use std::{collections::BTreeMap, sync::Mutex};

use broker_core::{
    BrokerError,
    artifacts::{SignatureAlg, SignatureEnvelope},
    commitment::CommitmentKey,
    credential::{
        AuthProfileRefV2, ParsedV2, RegistryPinV2,
        commitment::{CommitmentContextV2, ValueEncoding, commit_v2},
        connection::ConnectionAuthorityViewV2,
        grant::{ExecutionGrantV2, NodeLeaseV2},
        parse,
        policy::AssurancePredicateV2,
        receipt::BindingAttestationV2,
        signing::{BINDING_ATTESTATION_DOMAIN, NODE_LEASE_DOMAIN},
    },
    signing::{BrokerSigner, BrokerVerifyingKey},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{BrokerHostError, TrustedHostScope, VerifiedAuthorityManifest};

/// Live, broker-owned connection state used to reconcile immutable authority
/// and material generation immediately before lease/grant use.
pub trait LiveConnectionAuthority: Send + Sync {
    fn authority_view_jcs(&self, connection_ref: &str) -> Result<Vec<u8>, BrokerError>;
    fn material_generation(&self, connection_ref: &str) -> Result<u64, BrokerError>;
}

pub struct InMemoryLiveConnectionAuthority {
    entries: Mutex<BTreeMap<String, (Vec<u8>, u64)>>,
}
impl InMemoryLiveConnectionAuthority {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
        }
    }
    pub fn set(
        &self,
        connection_ref: impl Into<String>,
        authority_view_jcs: Vec<u8>,
        material_generation: u64,
    ) -> Result<(), BrokerError> {
        let parsed = parse::<ConnectionAuthorityViewV2>(&authority_view_jcs)?;
        if parsed.canonical_bytes() != authority_view_jcs {
            return Err(BrokerError::Brk109);
        }
        self.entries
            .lock()
            .map_err(|_| BrokerError::Brk401)?
            .insert(
                connection_ref.into(),
                (authority_view_jcs, material_generation),
            );
        Ok(())
    }
}
impl Default for InMemoryLiveConnectionAuthority {
    fn default() -> Self {
        Self::new()
    }
}
impl LiveConnectionAuthority for InMemoryLiveConnectionAuthority {
    fn authority_view_jcs(&self, connection_ref: &str) -> Result<Vec<u8>, BrokerError> {
        self.entries
            .lock()
            .map_err(|_| BrokerError::Brk401)?
            .get(connection_ref)
            .map(|entry| entry.0.clone())
            .ok_or(BrokerError::Brk103)
    }
    fn material_generation(&self, connection_ref: &str) -> Result<u64, BrokerError> {
        self.entries
            .lock()
            .map_err(|_| BrokerError::Brk401)?
            .get(connection_ref)
            .map(|entry| entry.1)
            .ok_or(BrokerError::Brk103)
    }
}

#[derive(Clone)]
pub struct BindingDerivationV2 {
    pub org_id: String,
    pub principal: String,
    pub issuer: String,
    pub deployment_id: String,
    pub standing_authority_ref: String,
    pub standing_authority_hash: String,
    pub contract_set_ref: String,
    pub contract_set_hash: String,
    pub connection_ref: String,
    pub minimum_material_generation: u64,
    pub auth_profile_ref: AuthProfileRefV2,
    pub auth_profile_pin: RegistryPinV2,
    pub supported_contract_hashes: Vec<String>,
    pub policy_instance_hashes: Vec<String>,
    pub required_assurance_predicates: Vec<AssurancePredicateV2>,
    pub observed_at: String,
    pub expires_at: String,
}

#[derive(Clone)]
pub struct BindingVerificationLockV2 {
    pub org_id: String,
    pub principal: String,
    pub issuer: String,
    pub broker_key_id: String,
    pub deployment_id: String,
    pub authority_manifest_hash: String,
    pub standing_authority_ref: String,
    pub standing_authority_hash: String,
    pub contract_set_ref: String,
    pub contract_set_hash: String,
    pub connection_ref: String,
    pub auth_profile_ref: AuthProfileRefV2,
    pub execution_lane: String,
    pub custody_location: String,
}

pub struct VerifiedBindingV2 {
    attestation: ParsedV2<BindingAttestationV2>,
    authority_view: ParsedV2<ConnectionAuthorityViewV2>,
}
impl VerifiedBindingV2 {
    pub fn attestation(&self) -> &ParsedV2<BindingAttestationV2> {
        &self.attestation
    }
    pub fn authority_view(&self) -> &ParsedV2<ConnectionAuthorityViewV2> {
        &self.authority_view
    }
}

/// Derive a signed binding from exact canonical Flow IR, a host-verified
/// authority manifest, and the immutable current authority view.
pub fn issue_binding_v2(
    exact_flow_ir: &[u8],
    authority_manifest: &VerifiedAuthorityManifest,
    authority_view_jcs: &[u8],
    input: BindingDerivationV2,
    signer: &BrokerSigner,
) -> Result<ParsedV2<BindingAttestationV2>, BrokerHostError> {
    let canonical_ir = broker_core::canonical::canonicalize_bounded(exact_flow_ir, 1024 * 1024)?;
    if canonical_ir.as_bytes() != exact_flow_ir
        || canonical_ir.sha256() != authority_manifest.manifest.view.flow_ir_hash
    {
        return Err(BrokerHostError::V2DerivationRejected);
    }
    let manifest_bytes = authority_manifest.manifest.canonical_bytes();
    let manifest_hash = hash(manifest_bytes);
    let authority = parse::<ConnectionAuthorityViewV2>(authority_view_jcs)?;
    if authority.canonical_bytes() != authority_view_jcs {
        return Err(BrokerHostError::V2DerivationRejected);
    }
    let view = authority.view.as_value();
    let authority_hash = authority.content_hash();
    if view.get("org_id").and_then(Value::as_str) != Some(input.org_id.as_str())
        || view.get("connection_ref").and_then(Value::as_str) != Some(input.connection_ref.as_str())
        || view.get("standing_authority_hash").and_then(Value::as_str)
            != Some(input.standing_authority_hash.as_str())
        || view.get("contract_set_hash").and_then(Value::as_str)
            != Some(input.contract_set_hash.as_str())
        || view.get("auth_profile_ref") != Some(input.auth_profile_ref.as_value())
        || view.get("auth_profile_pin") != Some(input.auth_profile_pin.as_value())
    {
        return Err(BrokerHostError::V2DerivationRejected);
    }
    let mut value = serde_json::json!({
        "schema_version":"0.2", "critical_fields":[], "extensions":{},
        "org_id":input.org_id, "principal":input.principal, "issuer":input.issuer,
        "broker_key_id":signer.key_id(), "deployment_id":input.deployment_id,
        "authority_manifest_hash":manifest_hash,
        "standing_authority_ref":input.standing_authority_ref,
        "standing_authority_hash":input.standing_authority_hash,
        "contract_set_ref":input.contract_set_ref, "contract_set_hash":input.contract_set_hash,
        "connection_ref":input.connection_ref, "authority_view_hash":authority_hash,
        "authority_epoch":view["authority_epoch"],
        "minimum_material_generation":input.minimum_material_generation,
        "auth_profile_ref":input.auth_profile_ref, "auth_profile_pin":input.auth_profile_pin,
        "authorization_claims_commitment":view["authorization_claims_commitment"],
        "public_claims_projection_evidence":view["public_claims_projection_evidence"],
        "principal_commitments":view["principal_commitments"],
        "broker_instance_commitment":view["broker_instance_commitment"],
        "custodian":view["custodian"], "transport":view["transport"],
        "execution_lane":view["execution_lane"], "custody_location":view["custody_location"],
        "endpoint_set_hash":view["endpoint_set_hash"], "public_config_hash":view["public_config_hash"],
        "supported_contract_hashes":input.supported_contract_hashes,
        "policy_instance_hashes":input.policy_instance_hashes,
        "required_assurance_predicates":input.required_assurance_predicates,
        "registry_decision_set_hash":view["registry_decision_set_hash"],
        "observed_at":input.observed_at, "expires_at":input.expires_at,
        "signature":signature_placeholder(signer.key_id())
    });
    sign_value(&mut value, BINDING_ATTESTATION_DOMAIN, signer)?;
    parse_value(value)
}

/// Verify the signature before reading claims, then compare every immutable
/// lock fact and reconcile against the live authority view and generation.
pub fn verify_binding_v2(
    canonical_binding: &[u8],
    lock: &BindingVerificationLockV2,
    key: &BrokerVerifyingKey,
    live: &dyn LiveConnectionAuthority,
    now: &str,
) -> Result<VerifiedBindingV2, BrokerHostError> {
    verify_signature_first(BINDING_ATTESTATION_DOMAIN, canonical_binding, key)?;
    let binding = parse::<BindingAttestationV2>(canonical_binding)?;
    if binding.canonical_bytes() != canonical_binding {
        return Err(BrokerHostError::BindingSignatureRejected);
    }
    let b = binding.view.as_value();
    for (field, expected) in [
        ("org_id", lock.org_id.as_str()),
        ("principal", lock.principal.as_str()),
        ("issuer", lock.issuer.as_str()),
        ("broker_key_id", lock.broker_key_id.as_str()),
        ("deployment_id", lock.deployment_id.as_str()),
        (
            "authority_manifest_hash",
            lock.authority_manifest_hash.as_str(),
        ),
        (
            "standing_authority_ref",
            lock.standing_authority_ref.as_str(),
        ),
        (
            "standing_authority_hash",
            lock.standing_authority_hash.as_str(),
        ),
        ("contract_set_ref", lock.contract_set_ref.as_str()),
        ("contract_set_hash", lock.contract_set_hash.as_str()),
        ("connection_ref", lock.connection_ref.as_str()),
        ("execution_lane", lock.execution_lane.as_str()),
        ("custody_location", lock.custody_location.as_str()),
    ] {
        if b.get(field).and_then(Value::as_str) != Some(expected) {
            return Err(BrokerHostError::V2DerivationRejected);
        }
    }
    if b.get("auth_profile_ref") != Some(lock.auth_profile_ref.as_value())
        || b.get("observed_at")
            .and_then(Value::as_str)
            .is_none_or(|value| value > now)
        || b.get("expires_at")
            .and_then(Value::as_str)
            .is_none_or(|value| value <= now)
    {
        return Err(BrokerHostError::V2DerivationRejected);
    }
    let current_bytes = live.authority_view_jcs(&lock.connection_ref)?;
    let authority = parse::<ConnectionAuthorityViewV2>(&current_bytes)?;
    if authority.canonical_bytes() != current_bytes
        || b.get("authority_view_hash").and_then(Value::as_str)
            != Some(authority.content_hash().as_str())
        || b.get("authority_epoch") != authority.view.as_value().get("authority_epoch")
        || b.get("auth_profile_pin") != authority.view.as_value().get("auth_profile_pin")
        || b.get("authorization_claims_commitment")
            != authority
                .view
                .as_value()
                .get("authorization_claims_commitment")
        || b.get("public_claims_projection_evidence")
            != authority
                .view
                .as_value()
                .get("public_claims_projection_evidence")
        || b.get("principal_commitments") != authority.view.as_value().get("principal_commitments")
        || b.get("broker_instance_commitment")
            != authority.view.as_value().get("broker_instance_commitment")
        || b.get("custodian") != authority.view.as_value().get("custodian")
        || b.get("transport") != authority.view.as_value().get("transport")
        || b.get("registry_decision_set_hash")
            != authority.view.as_value().get("registry_decision_set_hash")
        || live.material_generation(&lock.connection_ref)?
            < number(b, "minimum_material_generation")?
    {
        return Err(BrokerHostError::V2AuthorityDrift);
    }
    Ok(VerifiedBindingV2 {
        attestation: binding,
        authority_view: authority,
    })
}

#[derive(Clone, Debug)]
pub struct NodeLeaseLimitsV2 {
    pub operation_contract: String,
    pub contract_hash: String,
    pub logical_calls: u64,
    pub dispatch_attempts_per_call: u8,
    pub flow_logical_calls: u64,
    pub connection_logical_calls: u64,
    pub node_logical_calls: u64,
    pub first_activation_ordinal: u64,
    pub last_activation_ordinal: u64,
    pub semantic_effect_slots: Vec<String>,
    pub not_before: String,
    pub expires_at: String,
    pub node_lease_ref: String,
    pub jti: String,
}

#[derive(Clone)]
struct LeaseRecord {
    parsed: ParsedV2<NodeLeaseV2>,
    canonical: Vec<u8>,
    standing_authority_ref: String,
    standing_authority_hash: String,
    contract_set_ref: String,
    contract_set_hash: String,
    remaining: u64,
    cas_version: u64,
    children: BTreeMap<String, StoredGrant>,
}
#[derive(Clone)]
struct StoredGrant {
    input_hash: String,
    grant_ref: ExactGrantRefV2,
    canonical: Vec<u8>,
}
#[derive(Clone, Copy)]
struct AggregateState {
    ceiling: u64,
    remaining: u64,
}
#[derive(Default)]
struct LeaseStoreState {
    leases: BTreeMap<String, LeaseRecord>,
    flow_remaining: BTreeMap<(String, String), AggregateState>,
    connection_remaining: BTreeMap<(String, String), AggregateState>,
}
#[derive(Default)]
pub struct NodeLeaseStoreV2 {
    state: Mutex<LeaseStoreState>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeLeaseStoreSnapshotV2 {
    leases: Vec<PersistedLeaseV2>,
    flow_aggregates: Vec<PersistedAggregateV2>,
    connection_aggregates: Vec<PersistedAggregateV2>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedLeaseV2 {
    lease_ref: String,
    canonical_lease: Vec<u8>,
    standing_authority_ref: String,
    standing_authority_hash: String,
    contract_set_ref: String,
    contract_set_hash: String,
    remaining: u64,
    cas_version: u64,
    children: Vec<PersistedGrantV2>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedGrantV2 {
    logical_effect_id: String,
    input_hash: String,
    grant_ref: String,
    canonical_grant: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedAggregateV2 {
    left: String,
    run_id: String,
    ceiling: u64,
    remaining: u64,
}

/// Opaque exact child grant reference. It has no constructor and cannot hold a
/// node lease ref supplied by flow/node code.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ExactGrantRefV2(String);
impl ExactGrantRefV2 {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub struct DerivedGrantV2 {
    pub grant_ref: ExactGrantRefV2,
    pub grant: ParsedV2<ExecutionGrantV2>,
    pub canonical_grant: Vec<u8>,
    pub redelivery: bool,
    pub cas_version: u64,
}
impl std::fmt::Debug for DerivedGrantV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DerivedGrantV2")
            .field("grant_ref", &self.grant_ref)
            .field("redelivery", &self.redelivery)
            .field("cas_version", &self.cas_version)
            .finish_non_exhaustive()
    }
}

impl NodeLeaseStoreV2 {
    pub fn restore(snapshot: NodeLeaseStoreSnapshotV2) -> Result<Self, BrokerHostError> {
        let mut state = LeaseStoreState::default();
        for persisted in snapshot.leases {
            let parsed = parse::<NodeLeaseV2>(&persisted.canonical_lease)?;
            if parsed.canonical_bytes() != persisted.canonical_lease
                || parsed
                    .view
                    .as_value()
                    .get("node_lease_ref")
                    .and_then(Value::as_str)
                    != Some(persisted.lease_ref.as_str())
            {
                return Err(BrokerHostError::V2DerivationRejected);
            }
            let mut children = BTreeMap::new();
            for child in persisted.children {
                let grant = parse::<ExecutionGrantV2>(&child.canonical_grant)?;
                if grant.canonical_bytes() != child.canonical_grant
                    || grant
                        .view
                        .as_value()
                        .pointer("/grant_scope/logical_effect_id")
                        .and_then(Value::as_str)
                        != Some(child.logical_effect_id.as_str())
                    || grant
                        .view
                        .as_value()
                        .get("grant_ref")
                        .and_then(Value::as_str)
                        != Some(child.grant_ref.as_str())
                {
                    return Err(BrokerHostError::V2DerivationRejected);
                }
                children.insert(
                    child.logical_effect_id,
                    StoredGrant {
                        input_hash: child.input_hash,
                        grant_ref: ExactGrantRefV2(child.grant_ref),
                        canonical: child.canonical_grant,
                    },
                );
            }
            if state
                .leases
                .insert(
                    persisted.lease_ref,
                    LeaseRecord {
                        parsed,
                        canonical: persisted.canonical_lease,
                        standing_authority_ref: persisted.standing_authority_ref,
                        standing_authority_hash: persisted.standing_authority_hash,
                        contract_set_ref: persisted.contract_set_ref,
                        contract_set_hash: persisted.contract_set_hash,
                        remaining: persisted.remaining,
                        cas_version: persisted.cas_version,
                        children,
                    },
                )
                .is_some()
            {
                return Err(BrokerHostError::V2DerivationRejected);
            }
        }
        for (source, target) in [
            (snapshot.flow_aggregates, &mut state.flow_remaining),
            (
                snapshot.connection_aggregates,
                &mut state.connection_remaining,
            ),
        ] {
            for aggregate in source {
                if aggregate.ceiling == 0
                    || aggregate.remaining > aggregate.ceiling
                    || target
                        .insert(
                            (aggregate.left, aggregate.run_id),
                            AggregateState {
                                ceiling: aggregate.ceiling,
                                remaining: aggregate.remaining,
                            },
                        )
                        .is_some()
                {
                    return Err(BrokerHostError::V2DerivationRejected);
                }
            }
        }
        Ok(Self {
            state: Mutex::new(state),
        })
    }

    pub fn snapshot(&self) -> Result<NodeLeaseStoreSnapshotV2, BrokerHostError> {
        let state = self.state.lock().map_err(|_| BrokerError::Brk401)?;
        let leases = state
            .leases
            .iter()
            .map(|(lease_ref, lease)| PersistedLeaseV2 {
                lease_ref: lease_ref.clone(),
                canonical_lease: lease.canonical.clone(),
                standing_authority_ref: lease.standing_authority_ref.clone(),
                standing_authority_hash: lease.standing_authority_hash.clone(),
                contract_set_ref: lease.contract_set_ref.clone(),
                contract_set_hash: lease.contract_set_hash.clone(),
                remaining: lease.remaining,
                cas_version: lease.cas_version,
                children: lease
                    .children
                    .iter()
                    .map(|(effect, child)| PersistedGrantV2 {
                        logical_effect_id: effect.clone(),
                        input_hash: child.input_hash.clone(),
                        grant_ref: child.grant_ref.0.clone(),
                        canonical_grant: child.canonical.clone(),
                    })
                    .collect(),
            })
            .collect();
        let aggregates = |values: &BTreeMap<(String, String), AggregateState>| {
            values
                .iter()
                .map(|((left, run_id), value)| PersistedAggregateV2 {
                    left: left.clone(),
                    run_id: run_id.clone(),
                    ceiling: value.ceiling,
                    remaining: value.remaining,
                })
                .collect::<Vec<_>>()
        };
        Ok(NodeLeaseStoreSnapshotV2 {
            leases,
            flow_aggregates: aggregates(&state.flow_remaining),
            connection_aggregates: aggregates(&state.connection_remaining),
        })
    }

    pub fn issue(
        &self,
        binding: &VerifiedBindingV2,
        scope: &TrustedHostScope,
        limits: NodeLeaseLimitsV2,
        signer: &BrokerSigner,
    ) -> Result<ParsedV2<NodeLeaseV2>, BrokerHostError> {
        if binding
            .attestation
            .view
            .as_value()
            .get("broker_key_id")
            .and_then(Value::as_str)
            != Some(signer.key_id())
            || limits.node_lease_ref.starts_with("grant_")
            || !limits.node_lease_ref.starts_with("node_lease_")
            || limits.logical_calls == 0
            || limits.semantic_effect_slots.is_empty()
            || limits.first_activation_ordinal > limits.last_activation_ordinal
            || limits.contract_hash
                != binding.attestation.view.as_value()["supported_contract_hashes"]
                    .as_array()
                    .and_then(|values| {
                        values.iter().find_map(|v| {
                            (v.as_str() == Some(&limits.contract_hash))
                                .then_some(v.as_str().unwrap_or_default())
                        })
                    })
                    .unwrap_or_default()
        {
            return Err(BrokerHostError::V2DerivationRejected);
        }
        let b = binding.attestation.view.as_value();
        let mut slots = limits.semantic_effect_slots.clone();
        slots.sort();
        slots.dedup();
        if slots != limits.semantic_effect_slots {
            return Err(BrokerHostError::V2DerivationRejected);
        }
        let mut value = serde_json::json!({
            "schema_version":"0.2", "critical_fields":[], "extensions":{},
            "org_id":b["org_id"], "deployment_id":b["deployment_id"], "principal":b["principal"],
            "issuer":b["issuer"], "channel_binding":scope.channel_binding_v2(), "subject":scope.subject_v2(),
            "operation_contract":limits.operation_contract, "contract_hash":limits.contract_hash,
            "connection_ref":b["connection_ref"], "authority_view_hash":b["authority_view_hash"],
            "authority_epoch":b["authority_epoch"], "minimum_material_generation":b["minimum_material_generation"],
            "policy_instance_hashes":b["policy_instance_hashes"], "required_assurance_predicates":b["required_assurance_predicates"],
            "registry_decision_set_hash":b["registry_decision_set_hash"],
            "budgets":{"logical_calls":limits.logical_calls,"dispatch_attempts_per_call":limits.dispatch_attempts_per_call,
                "flow_logical_calls":limits.flow_logical_calls,"connection_logical_calls":limits.connection_logical_calls,
                "node_logical_calls":limits.node_logical_calls},
            "not_before":limits.not_before, "expires_at":limits.expires_at, "jti":limits.jti,
            "node_lease_ref":limits.node_lease_ref, "audience":"broker-grant-derivation",
            "first_activation_ordinal":limits.first_activation_ordinal,"last_activation_ordinal":limits.last_activation_ordinal,
            "semantic_effect_slots":limits.semantic_effect_slots,"max_logical_effects":limits.logical_calls,
            "auth_profile_ref":b["auth_profile_ref"],"auth_profile_pin":b["auth_profile_pin"],
            "authorization_claims_commitment":b["authorization_claims_commitment"],
            "public_claims_projection_evidence":b["public_claims_projection_evidence"],
            "principal_commitments":b["principal_commitments"],"custodian":b["custodian"],"transport":b["transport"],
            "execution_lane":b["execution_lane"],"custody_location":b["custody_location"],
            "broker_instance_commitment":b["broker_instance_commitment"],
            "signature":signature_placeholder(signer.key_id())
        });
        sign_value(&mut value, NODE_LEASE_DOMAIN, signer)?;
        let parsed: ParsedV2<NodeLeaseV2> = parse_value(value)?;
        let canonical = parsed.canonical_bytes().to_vec();
        let lease_ref = parsed.view.as_value()["node_lease_ref"]
            .as_str()
            .ok_or(BrokerHostError::V2DerivationRejected)?
            .to_owned();
        let flow_key = (
            b["deployment_id"].as_str().unwrap_or_default().to_owned(),
            scope.run_id().to_owned(),
        );
        let connection_key = (
            b["connection_ref"].as_str().unwrap_or_default().to_owned(),
            scope.run_id().to_owned(),
        );
        let mut state = self.state.lock().map_err(|_| BrokerError::Brk401)?;
        if state.leases.contains_key(&lease_ref) {
            return Err(BrokerError::Brk204.into());
        }
        install_aggregate(
            &mut state.flow_remaining,
            flow_key,
            limits.flow_logical_calls,
        )?;
        install_aggregate(
            &mut state.connection_remaining,
            connection_key,
            limits.connection_logical_calls,
        )?;
        state.leases.insert(
            lease_ref,
            LeaseRecord {
                parsed: parsed.clone(),
                canonical,
                standing_authority_ref: b["standing_authority_ref"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                standing_authority_hash: b["standing_authority_hash"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                contract_set_ref: b["contract_set_ref"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                contract_set_hash: b["contract_set_hash"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                remaining: limits.logical_calls,
                cas_version: 0,
                children: BTreeMap::new(),
            },
        );
        Ok(parsed)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn derive_child(
        &self,
        node_lease_ref: &str,
        scope: &TrustedHostScope,
        semantic_effect_slot: &str,
        canonical_input: &[u8],
        now: &str,
        expected_cas_version: u64,
        pop_proof: &[u8],
        expected_pop_proof: &[u8],
        live: &dyn LiveConnectionAuthority,
        commitments: &CommitmentKey,
    ) -> Result<DerivedGrantV2, BrokerHostError> {
        if !node_lease_ref.starts_with("node_lease_") || pop_proof != expected_pop_proof {
            return Err(BrokerError::Brk102.into());
        }
        let canonical = broker_core::canonical::canonicalize_bounded(
            canonical_input,
            broker_core::canonical::MAX_OPERATION_BYTES,
        )?;
        if canonical.as_bytes() != canonical_input {
            return Err(BrokerError::Brk001.into());
        }
        let logical_effect_id = broker_core::effect_id::derive(
            scope.run_id(),
            scope.node_id(),
            scope.activation_ordinal(),
            semantic_effect_slot,
        )?;
        let input_hash = hash(canonical_input);
        let mut state = self.state.lock().map_err(|_| BrokerError::Brk401)?;
        let lease = state
            .leases
            .get(node_lease_ref)
            .ok_or(BrokerError::Brk103)?;
        if let Some(child) = lease.children.get(&logical_effect_id) {
            if child.input_hash != input_hash {
                return Err(BrokerError::Brk203.into());
            }
            let grant = parse::<ExecutionGrantV2>(&child.canonical)?;
            return Ok(DerivedGrantV2 {
                grant_ref: child.grant_ref.clone(),
                grant,
                canonical_grant: child.canonical.clone(),
                redelivery: true,
                cas_version: lease.cas_version,
            });
        }
        if lease.cas_version != expected_cas_version {
            return Err(BrokerError::Brk204.into());
        }
        let l = lease.parsed.view.as_value();
        if scope.subject_v2() != l["subject"]
            || l.get("not_before")
                .and_then(Value::as_str)
                .is_none_or(|value| value > now)
            || l.get("expires_at")
                .and_then(Value::as_str)
                .is_none_or(|value| value <= now)
            || scope.activation_ordinal() < number(l, "first_activation_ordinal")?
            || scope.activation_ordinal() > number(l, "last_activation_ordinal")?
            || !l["semantic_effect_slots"]
                .as_array()
                .is_some_and(|v| v.iter().any(|x| x.as_str() == Some(semantic_effect_slot)))
        {
            return Err(BrokerHostError::V2DerivationRejected);
        }
        let connection_ref = l["connection_ref"]
            .as_str()
            .ok_or(BrokerHostError::V2DerivationRejected)?;
        let authority_bytes = live.authority_view_jcs(connection_ref)?;
        let authority = parse::<ConnectionAuthorityViewV2>(&authority_bytes)?;
        if authority.content_hash() != l["authority_view_hash"].as_str().unwrap_or_default()
            || authority.view.as_value()["authority_epoch"] != l["authority_epoch"]
            || live.material_generation(connection_ref)? < number(l, "minimum_material_generation")?
        {
            return Err(BrokerHostError::V2AuthorityDrift);
        }
        let flow_key = (
            l["deployment_id"].as_str().unwrap_or_default().to_owned(),
            scope.run_id().to_owned(),
        );
        let connection_key = (connection_ref.to_owned(), scope.run_id().to_owned());
        // Check every counter before mutating any of them: one lock is the CAS
        // boundary for parent, flow aggregate, and connection aggregate.
        if lease.remaining == 0
            || state
                .flow_remaining
                .get(&flow_key)
                .is_some_and(|v| v.remaining == 0)
            || state
                .connection_remaining
                .get(&connection_key)
                .is_some_and(|v| v.remaining == 0)
        {
            return Err(BrokerError::Brk201.into());
        }
        let parent_before = lease.remaining;
        let parent_after = parent_before - 1;
        let child_cas_version = lease.cas_version + 1;
        let parent_hash = hash(&lease.canonical);
        let parent_jti = l["jti"].as_str().unwrap_or_default().to_owned();
        let standing_authority_ref = lease.standing_authority_ref.clone();
        let standing_authority_hash = lease.standing_authority_hash.clone();
        let contract_set_ref = lease.contract_set_ref.clone();
        let contract_set_hash = lease.contract_set_hash.clone();
        let l = l.clone();
        let grant_ref_text = format!(
            "grant_{}",
            &logical_effect_id[logical_effect_id.len().saturating_sub(32)..]
        );
        let (input_commitment, _) = commit_v2(
            commitments,
            l["org_id"].as_str().unwrap_or_default(),
            &CommitmentContextV2::GrantCanonicalInput {
                issuer: l["issuer"].as_str().unwrap_or_default().to_owned(),
                grant_ref: grant_ref_text.clone(),
                effect: logical_effect_id.clone(),
            },
            canonical_input,
            ValueEncoding::Jcs,
            broker_core::artifacts::VerificationTier::BrokerOnly,
        )?;
        let input_commitment_value =
            serde_json::to_value(input_commitment).map_err(|_| BrokerError::Brk401)?;
        let record = serde_json::json!({"node_lease_ref":node_lease_ref,"logical_effect_id":logical_effect_id,"canonical_input_commitment":input_commitment_value,"cas_version":expected_cas_version});
        let reservation_hash = broker_core::canonical::from_serde(&record, 64 * 1024)?.sha256();
        let derivation_hash = hash(format!("{parent_hash}\0{reservation_hash}").as_bytes());
        let value = serde_json::json!({
            "schema_version":"0.2","critical_fields":[],"extensions":{},
            "org_id":l["org_id"],"deployment_id":l["deployment_id"],"principal":l["principal"],"issuer":l["issuer"],
            "channel_binding":l["channel_binding"],"subject":l["subject"],"operation_contract":l["operation_contract"],
            "contract_hash":l["contract_hash"],"connection_ref":l["connection_ref"],"authority_view_hash":l["authority_view_hash"],
            "authority_epoch":l["authority_epoch"],"minimum_material_generation":l["minimum_material_generation"],
            "policy_instance_hashes":l["policy_instance_hashes"],"required_assurance_predicates":l["required_assurance_predicates"],
            "registry_decision_set_hash":l["registry_decision_set_hash"],
            "budgets":{"logical_calls":1,"dispatch_attempts_per_call":l["budgets"]["dispatch_attempts_per_call"],
                "flow_logical_calls":l["budgets"]["flow_logical_calls"],"connection_logical_calls":l["budgets"]["connection_logical_calls"],"node_logical_calls":1},
            "not_before":l["not_before"],"expires_at":l["expires_at"],"jti":format!("child-{child_cas_version}"),
            "grant_ref":grant_ref_text,"audience":"broker-execution",
            "grant_scope":{"kind":"logical_effect","logical_effect_id":logical_effect_id,"activation_ordinal":scope.activation_ordinal(),"semantic_effect_slot":semantic_effect_slot},
            "canonical_input_commitment":input_commitment_value,
            "derivation_evidence":{"kind":"node_lease","standing_authority_ref":standing_authority_ref,"standing_authority_hash":standing_authority_hash,
                "contract_set_ref":contract_set_ref,"contract_set_hash":contract_set_hash,"derivation_record_hash":derivation_hash,
                "consumed_budget_unit":1,"canonical_input_commitment":input_commitment_value,
                "parent_node_lease_ref":node_lease_ref,"parent_node_lease_hash":parent_hash,"parent_node_lease_jti":parent_jti,
                "parent_budget_before":parent_before,"parent_budget_after":parent_after,"child_reservation_record_hash":reservation_hash},
            "auth_profile_ref":l["auth_profile_ref"],"auth_profile_pin":l["auth_profile_pin"],
            "authorization_claims_commitment":l["authorization_claims_commitment"],"public_claims_projection_evidence":l["public_claims_projection_evidence"],
            "principal_commitments":l["principal_commitments"],"custodian":l["custodian"],"transport":l["transport"],
            "execution_lane":l["execution_lane"],"custody_location":l["custody_location"],"broker_instance_commitment":l["broker_instance_commitment"]
        });
        let grant: ParsedV2<ExecutionGrantV2> = parse_value(value)?;
        let canonical_grant = grant.canonical_bytes().to_vec();
        let grant_ref = ExactGrantRefV2(grant_ref_text);
        if let Some(value) = state.flow_remaining.get_mut(&flow_key) {
            value.remaining -= 1;
        }
        if let Some(value) = state.connection_remaining.get_mut(&connection_key) {
            value.remaining -= 1;
        }
        let lease = state
            .leases
            .get_mut(node_lease_ref)
            .ok_or(BrokerError::Brk103)?;
        lease.remaining = parent_after;
        lease.cas_version = child_cas_version;
        lease.children.insert(
            logical_effect_id,
            StoredGrant {
                input_hash,
                grant_ref: grant_ref.clone(),
                canonical: canonical_grant.clone(),
            },
        );
        Ok(DerivedGrantV2 {
            grant_ref,
            grant,
            canonical_grant,
            redelivery: false,
            cas_version: child_cas_version,
        })
    }

    pub fn resolve_grant(
        &self,
        grant_ref: &ExactGrantRefV2,
    ) -> Result<ParsedV2<ExecutionGrantV2>, BrokerHostError> {
        let state = self.state.lock().map_err(|_| BrokerError::Brk401)?;
        for lease in state.leases.values() {
            if let Some(child) = lease
                .children
                .values()
                .find(|child| &child.grant_ref == grant_ref)
            {
                return parse::<ExecutionGrantV2>(&child.canonical).map_err(Into::into);
            }
        }
        Err(BrokerError::Brk103.into())
    }
}

fn install_aggregate<K: Ord>(
    map: &mut BTreeMap<K, AggregateState>,
    key: K,
    ceiling: u64,
) -> Result<(), BrokerHostError> {
    if ceiling == 0 {
        return Ok(());
    }
    if map
        .get(&key)
        .is_some_and(|existing| existing.ceiling != ceiling)
    {
        return Err(BrokerError::Brk204.into());
    }
    map.entry(key).or_insert(AggregateState {
        ceiling,
        remaining: ceiling,
    });
    Ok(())
}

fn signature_placeholder(key_id: &str) -> SignatureEnvelope {
    SignatureEnvelope {
        alg: SignatureAlg::Ed25519,
        key_id: key_id.to_owned(),
        value: "placeholder".into(),
    }
}
fn sign_value(
    value: &mut Value,
    domain: &str,
    signer: &BrokerSigner,
) -> Result<(), BrokerHostError> {
    let bytes = serde_json::to_vec(value).map_err(|_| BrokerError::Brk401)?;
    value["signature"] =
        serde_json::to_value(signer.sign_json(domain, &bytes)?).map_err(|_| BrokerError::Brk401)?;
    Ok(())
}
fn parse_value<T: broker_core::credential::SchemaType + serde::de::DeserializeOwned>(
    value: Value,
) -> Result<ParsedV2<T>, BrokerHostError> {
    let canonical = broker_core::canonical::from_serde(&value, T::MAX_BYTES)?;
    parse::<T>(canonical.as_bytes()).map_err(Into::into)
}
fn verify_signature_first(
    domain: &str,
    bytes: &[u8],
    key: &BrokerVerifyingKey,
) -> Result<(), BrokerHostError> {
    let untrusted: Value =
        serde_json::from_slice(bytes).map_err(|_| BrokerHostError::BindingSignatureRejected)?;
    let signature: SignatureEnvelope = serde_json::from_value(
        untrusted
            .get("signature")
            .cloned()
            .ok_or(BrokerHostError::BindingSignatureRejected)?,
    )
    .map_err(|_| BrokerHostError::BindingSignatureRejected)?;
    key.verify_json(domain, bytes, &signature)
        .map_err(|_| BrokerHostError::BindingSignatureRejected)
}
fn hash(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}
fn number(value: &Value, field: &str) -> Result<u64, BrokerHostError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| BrokerError::Brk109.into())
}
