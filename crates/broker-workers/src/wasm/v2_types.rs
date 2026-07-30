use super::v2_production::*;
use super::*;

pub(super) fn artifact_time_is_current(value: &Value, now: &str) -> bool {
    value
        .get("not_before")
        .and_then(Value::as_str)
        .is_some_and(|not_before| not_before <= now)
        && value
            .get("expires_at")
            .and_then(Value::as_str)
            .is_some_and(|expires_at| expires_at > now)
}

pub(super) fn valid_hash(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

pub(super) fn hash(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScopeInput {
    pub(super) org_id: String,
    pub(super) deployment_id: String,
    pub(super) session_ref: String,
    pub(super) pop_key_thumbprint: String,
    pub(super) bundle_id: String,
    pub(super) flow_ir_hash: String,
    pub(super) binding_lock_hash: String,
    pub(super) flow_id: String,
    pub(super) run_id: String,
    pub(super) node_id: String,
    pub(super) node_alias: String,
    pub(super) activation_ordinal: u64,
}

impl ScopeInput {
    pub(super) fn trusted(&self) -> Result<broker_host::TrustedHostScope, BrokerError> {
        let context = bootstrap_host_context(HostBootstrapIdentity {
            org_id: self.org_id.clone(),
            principal_id: self.deployment_id.clone(),
            bundle_id: self.bundle_id.clone(),
            flow_ir_hash: self.flow_ir_hash.clone(),
            binding_lock_hash: self.binding_lock_hash.clone(),
            flow_id: self.flow_id.clone(),
            run_id: self.run_id.clone(),
        })
        .map_err(host_error)?;
        context
            .scope_for_activation(
                self.node_id.clone(),
                self.node_alias.clone(),
                self.activation_ordinal,
            )
            .map_err(host_error)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SerializableBindingLock {
    pub(super) org_id: String,
    pub(super) principal: String,
    pub(super) issuer: String,
    pub(super) broker_key_id: String,
    pub(super) deployment_id: String,
    pub(super) authority_manifest_hash: String,
    pub(super) standing_authority_ref: String,
    pub(super) standing_authority_hash: String,
    pub(super) contract_set_ref: String,
    pub(super) contract_set_hash: String,
    pub(super) connection_ref: String,
    pub(super) auth_profile_ref_json: Value,
    pub(super) execution_lane: String,
    pub(super) custody_location: String,
}

impl SerializableBindingLock {
    pub(super) fn lock(&self) -> Result<BindingVerificationLockV2, BrokerError> {
        let canonical = broker_core::canonical::from_serde(&self.auth_profile_ref_json, 64 * 1024)?;
        let auth_profile_ref = parse::<AuthProfileRefV2>(canonical.as_bytes())?.view;
        Ok(BindingVerificationLockV2 {
            org_id: self.org_id.clone(),
            principal: self.principal.clone(),
            issuer: self.issuer.clone(),
            broker_key_id: self.broker_key_id.clone(),
            deployment_id: self.deployment_id.clone(),
            authority_manifest_hash: self.authority_manifest_hash.clone(),
            standing_authority_ref: self.standing_authority_ref.clone(),
            standing_authority_hash: self.standing_authority_hash.clone(),
            contract_set_ref: self.contract_set_ref.clone(),
            contract_set_hash: self.contract_set_hash.clone(),
            connection_ref: self.connection_ref.clone(),
            auth_profile_ref,
            execution_lane: self.execution_lane.clone(),
            custody_location: self.custody_location.clone(),
        })
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum V2AuthorityCommand {
    IssueLease {
        canonical_binding: Vec<u8>,
        lock: SerializableBindingLock,
        canonical_authority_view: Vec<u8>,
        material_generation: u64,
        now: String,
        scope: ScopeInput,
        limits: SerializableLeaseLimits,
    },
    DeriveGrant {
        canonical_authority_view: Vec<u8>,
        material_generation: u64,
        now: String,
        scope: ScopeInput,
        node_lease_ref: String,
        semantic_effect_slot: String,
        canonical_input: Vec<u8>,
        expected_cas_version: u64,
        pop_proof: Vec<u8>,
        expected_pop_proof: Vec<u8>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SerializableLeaseLimits {
    pub(super) logical_binding_ref: String,
    pub(super) binding_revision_ref: String,
    pub(super) operation_contract: String,
    pub(super) contract_hash: String,
    pub(super) logical_calls: u64,
    pub(super) dispatch_attempts_per_call: u8,
    pub(super) flow_logical_calls: u64,
    pub(super) connection_logical_calls: u64,
    pub(super) node_logical_calls: u64,
    pub(super) first_activation_ordinal: u64,
    pub(super) last_activation_ordinal: u64,
    pub(super) semantic_effect_slots: Vec<String>,
    pub(super) not_before: String,
    pub(super) expires_at: String,
    pub(super) node_lease_ref: String,
    pub(super) jti: String,
}
impl From<SerializableLeaseLimits> for NodeLeaseLimitsV2 {
    fn from(value: SerializableLeaseLimits) -> Self {
        Self {
            logical_binding_ref: value.logical_binding_ref,
            binding_revision_ref: value.binding_revision_ref,
            operation_contract: value.operation_contract,
            contract_hash: value.contract_hash,
            logical_calls: value.logical_calls,
            dispatch_attempts_per_call: value.dispatch_attempts_per_call,
            flow_logical_calls: value.flow_logical_calls,
            connection_logical_calls: value.connection_logical_calls,
            node_logical_calls: value.node_logical_calls,
            first_activation_ordinal: value.first_activation_ordinal,
            last_activation_ordinal: value.last_activation_ordinal,
            semantic_effect_slots: value.semantic_effect_slots,
            not_before: value.not_before,
            expires_at: value.expires_at,
            node_lease_ref: value.node_lease_ref,
            jti: value.jti,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct V2AuthorityReply {
    pub(super) artifact_ref: String,
    pub(super) canonical_artifact: Vec<u8>,
    pub(super) cas_version: u64,
    pub(super) redelivery: bool,
}
