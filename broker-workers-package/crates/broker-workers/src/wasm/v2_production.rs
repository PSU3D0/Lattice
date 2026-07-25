use super::*;
use broker_core::{
    commitment::CommitmentKey,
    credential::{
        AuthProfileRefV2, ContractSetV2, RegistryPinV2, StandingAuthorityV2,
        connection::ConnectionAuthorityViewV2,
        grant::{ExecutionGrantV2, NodeLeaseV2},
        parse,
        policy::AssurancePredicateV2,
        receipt::{BindingAttestationV2, InvocationReceiptV2},
    },
    signing::BrokerSigner,
};
use broker_host::{
    ApprovedAuthorityContract, BindingDerivationV2, BindingVerificationLockV2,
    HostBootstrapIdentity, InMemoryLiveConnectionAuthority, ManifestProvenance, NodeLeaseLimitsV2,
    NodeLeaseStoreSnapshotV2, NodeLeaseStoreV2, V2AttemptStage, V2DispatchEvidence, V2ReceiptIssue,
    bootstrap_host_context, issue_binding_v2, issue_v2_receipt, verify_authority_manifest,
    verify_binding_v2,
};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use worker::{D1Database, durable_object};

const V2_LEASE_STORE_KEY: &str = "broker:v2-node-lease-store:v1";
fn artifact_time_is_current(value: &Value, now: &str) -> bool {
    value
        .get("not_before")
        .and_then(Value::as_str)
        .is_some_and(|not_before| not_before <= now)
        && value
            .get("expires_at")
            .and_then(Value::as_str)
            .is_some_and(|expires_at| expires_at > now)
}

fn valid_hash(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn hash(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScopeInput {
    org_id: String,
    deployment_id: String,
    session_ref: String,
    pop_key_thumbprint: String,
    bundle_id: String,
    flow_ir_hash: String,
    binding_lock_hash: String,
    flow_id: String,
    run_id: String,
    node_id: String,
    node_alias: String,
    activation_ordinal: u64,
}

impl ScopeInput {
    fn trusted(&self) -> Result<broker_host::TrustedHostScope, BrokerError> {
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
struct SerializableBindingLock {
    org_id: String,
    principal: String,
    issuer: String,
    broker_key_id: String,
    deployment_id: String,
    authority_manifest_hash: String,
    standing_authority_ref: String,
    standing_authority_hash: String,
    contract_set_ref: String,
    contract_set_hash: String,
    connection_ref: String,
    auth_profile_ref_json: Value,
    execution_lane: String,
    custody_location: String,
}

impl SerializableBindingLock {
    fn lock(&self) -> Result<BindingVerificationLockV2, BrokerError> {
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
enum V2AuthorityCommand {
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
struct SerializableLeaseLimits {
    operation_contract: String,
    contract_hash: String,
    logical_calls: u64,
    dispatch_attempts_per_call: u8,
    flow_logical_calls: u64,
    connection_logical_calls: u64,
    node_logical_calls: u64,
    first_activation_ordinal: u64,
    last_activation_ordinal: u64,
    semantic_effect_slots: Vec<String>,
    not_before: String,
    expires_at: String,
    node_lease_ref: String,
    jti: String,
}
impl From<SerializableLeaseLimits> for NodeLeaseLimitsV2 {
    fn from(value: SerializableLeaseLimits) -> Self {
        Self {
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
struct V2AuthorityReply {
    artifact_ref: String,
    canonical_artifact: Vec<u8>,
    cas_version: u64,
    redelivery: bool,
}

#[durable_object]
pub struct V2AuthorityDurableObject {
    state: State,
    env: Env,
}

impl worker::DurableObject for V2AuthorityDurableObject {
    fn new(state: State, env: Env) -> Self {
        Self { state, env }
    }

    async fn fetch(&self, mut request: Request) -> worker::Result<Response> {
        let command: V2AuthorityCommand = match bounded_json(&mut request, MAX_INVOKE_BODY).await {
            Ok(value) => value,
            Err(_) => return do_error(BrokerError::Brk001),
        };
        let snapshot = self
            .state
            .storage()
            .get::<NodeLeaseStoreSnapshotV2>(V2_LEASE_STORE_KEY)
            .await?
            .unwrap_or_default();
        let store = match NodeLeaseStoreV2::restore(snapshot) {
            Ok(store) => store,
            Err(_) => return do_error(BrokerError::Brk401),
        };
        let signer = BrokerSigner::from_seed(
            "broker-v2-authority",
            match secret_32(&self.env, "BINDING_SIGNING_SEED") {
                Ok(seed) => seed,
                Err(_) => return do_error(BrokerError::Brk401),
            },
        );
        let result = match command {
            V2AuthorityCommand::IssueLease {
                canonical_binding,
                lock,
                canonical_authority_view,
                material_generation,
                now,
                scope,
                limits,
            } => {
                let live = InMemoryLiveConnectionAuthority::new();
                if live
                    .set(
                        lock.connection_ref.clone(),
                        canonical_authority_view,
                        material_generation,
                    )
                    .is_err()
                {
                    return do_error(BrokerError::Brk106);
                }
                let verified = match lock.lock().and_then(|lock| {
                    verify_binding_v2(
                        &canonical_binding,
                        &lock,
                        &signer.verifying_key(),
                        &live,
                        &now,
                    )
                    .map_err(host_error)
                }) {
                    Ok(value) => value,
                    Err(error) => return do_error(error),
                };
                let lease = match scope.trusted().and_then(|scope| {
                    store
                        .issue(&verified, &scope, limits.into(), &signer)
                        .map_err(host_error)
                }) {
                    Ok(value) => value,
                    Err(error) => return do_error(error),
                };
                V2AuthorityReply {
                    artifact_ref: lease.view.as_value()["node_lease_ref"]
                        .as_str()
                        .unwrap_or_default()
                        .into(),
                    canonical_artifact: lease.canonical_bytes().to_vec(),
                    cas_version: 0,
                    redelivery: false,
                }
            }
            V2AuthorityCommand::DeriveGrant {
                canonical_authority_view,
                material_generation,
                now,
                scope,
                node_lease_ref,
                semantic_effect_slot,
                canonical_input,
                expected_cas_version,
                pop_proof,
                expected_pop_proof,
            } => {
                let authority = match parse::<ConnectionAuthorityViewV2>(&canonical_authority_view)
                {
                    Ok(value) => value,
                    Err(error) => return do_error(error),
                };
                let connection_ref = authority.view.as_value()["connection_ref"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                let live = InMemoryLiveConnectionAuthority::new();
                if live
                    .set(
                        connection_ref,
                        canonical_authority_view,
                        material_generation,
                    )
                    .is_err()
                {
                    return do_error(BrokerError::Brk106);
                }
                let commitments = match CommitmentKey::new(
                    "broker-v2-input",
                    match secret_32(&self.env, "COMMITMENT_KEY") {
                        Ok(key) => key,
                        Err(_) => return do_error(BrokerError::Brk401),
                    },
                ) {
                    Ok(value) => value,
                    Err(error) => return do_error(error),
                };
                let derived = match scope.trusted().and_then(|scope| {
                    store
                        .derive_child(
                            &node_lease_ref,
                            &scope,
                            &semantic_effect_slot,
                            &canonical_input,
                            &now,
                            expected_cas_version,
                            &pop_proof,
                            &expected_pop_proof,
                            &live,
                            &commitments,
                        )
                        .map_err(host_error)
                }) {
                    Ok(value) => value,
                    Err(error) => return do_error(error),
                };
                V2AuthorityReply {
                    artifact_ref: derived.grant_ref.as_str().into(),
                    canonical_artifact: derived.canonical_grant,
                    cas_version: derived.cas_version,
                    redelivery: derived.redelivery,
                }
            }
        };
        self.state
            .storage()
            .put(
                V2_LEASE_STORE_KEY,
                store
                    .snapshot()
                    .map_err(|_| worker_rust_error("v2 lease"))?,
            )
            .await?;
        Response::from_json(&result)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstallBindingRequestV2 {
    connection_ref: String,
    deployment_id: String,
    bundle_id: String,
    flow_ir_hash: String,
    binding_lock_hash: String,
    flow_id: String,
    flow_ir_json: String,
    authority_manifest_json: String,
    contracts: Vec<String>,
}

#[derive(Deserialize)]
struct ConnectionV2Row {
    connection_ref: String,
    profile_ref: String,
    profile_version: String,
    canonical_authority_view_json: String,
    standing_authority_ref: String,
    standing_authority_hash: String,
    canonical_standing_authority_json: String,
    contract_set_ref: String,
    contract_set_hash: String,
    canonical_contract_set_json: String,
    active_material_generation: u64,
    material_mode: String,
    revocation_epoch: u64,
    status: String,
}

fn load_deployment_authority(
    env: &Env,
    org_id: &str,
    deployment_id: &str,
    connector_ref: &str,
) -> worker::Result<(
    broker_core::credential::ParsedV2<StandingAuthorityV2>,
    broker_core::credential::ParsedV2<ContractSetV2>,
)> {
    let standing_bytes = env
        .var("DEPLOYMENT_STANDING_AUTHORITY_JCS")?
        .to_string()
        .into_bytes();
    let contract_bytes = env
        .var("DEPLOYMENT_CONTRACT_SET_JCS")?
        .to_string()
        .into_bytes();
    let key_bytes = URL_SAFE_NO_PAD
        .decode(env.var("DEPLOYMENT_AUTHORITY_PUBLIC_KEY_B64U")?.to_string())
        .map_err(|_| worker_rust_error("deployment authority key"))?;
    let key_bytes: [u8; 32] = key_bytes
        .try_into()
        .map_err(|_| worker_rust_error("deployment authority key"))?;
    let key_id = env.var("DEPLOYMENT_AUTHORITY_KEY_ID")?.to_string();
    let key = broker_core::signing::BrokerVerifyingKey::from_bytes(key_id, key_bytes)
        .map_err(|_| worker_rust_error("deployment authority key"))?;
    let standing = parse::<StandingAuthorityV2>(&standing_bytes)
        .map_err(|_| worker_rust_error("standing authority"))?;
    let contracts =
        parse::<ContractSetV2>(&contract_bytes).map_err(|_| worker_rust_error("contract set"))?;
    broker_core::credential::signing::verify_signed(&standing, &key)
        .and_then(|_| broker_core::credential::signing::verify_signed(&contracts, &key))
        .map_err(|_| worker_rust_error("deployment authority signature"))?;
    let now = now_rfc3339();
    let s = standing.view.as_value();
    let c = contracts.view.as_value();
    if s["org_id"] != org_id
        || c["org_id"] != org_id
        || s["deployment_id"] != deployment_id
        || c["deployment_id"] != deployment_id
        || s["connector_ref"] != connector_ref
        || c["connector_ref"] != connector_ref
        || s["contract_set_ref"] != c["contract_set_ref"]
        || s["contract_set_hash"] != contracts.content_hash()
        || !artifact_time_is_current(s, &now)
    {
        return Err(worker_rust_error("deployment authority mismatch"));
    }
    let installed = crate::composition::installed_contracts(&now)
        .map_err(|_| worker_rust_error("contracts"))?;
    let installed = installed
        .into_iter()
        .map(|value| (value.contract_id, value.contract_hash))
        .collect::<BTreeSet<_>>();
    let authorized = c["contracts"]
        .as_array()
        .ok_or_else(|| worker_rust_error("contract set"))?
        .iter()
        .map(|value| {
            Ok((
                value["contract_id"]
                    .as_str()
                    .ok_or_else(|| worker_rust_error("contract set"))?,
                value["contract_hash"]
                    .as_str()
                    .ok_or_else(|| worker_rust_error("contract set"))?,
            ))
        })
        .collect::<worker::Result<BTreeSet<_>>>()?;
    if authorized.is_empty() || !authorized.is_subset(&installed) {
        return Err(worker_rust_error("contract authority"));
    }
    Ok((standing, contracts))
}

pub async fn prepare_activated_connection(
    db: &D1Database,
    env: &Env,
    org_id: &str,
    deployment_id: &str,
    connection_ref: &str,
    profile_ref_with_version: &str,
    account_subject_commitment: &str,
    normalized_claims: &[String],
    material_do_route: &str,
    sealed_material: &[u8],
    material_mode: &str,
    remote_binding_hash: Option<&str>,
    profile_descriptor_override: Option<&[u8]>,
    custodian_pin_override: Option<&Value>,
    transport_pin_override: Option<&Value>,
) -> worker::Result<()> {
    let plane = crate::composition::installed_provider_plane(&now_rfc3339())
        .map_err(|_| worker_rust_error("composition"))?;
    let (profile_ref, profile_version) = profile_ref_with_version
        .rsplit_once('@')
        .ok_or_else(|| worker_rust_error("profile"))?;
    let (standing, contract_set) =
        load_deployment_authority(env, org_id, deployment_id, &plane.management.connector_ref)?;
    let contract_set_ref = text(contract_set.view.as_value(), "contract_set_ref")?.to_owned();
    let standing_authority_ref =
        text(standing.view.as_value(), "standing_authority_ref")?.to_owned();
    let contract_set_hash = contract_set.content_hash();
    let standing_hash = standing.content_hash();
    let claims = broker_core::canonical::from_serde(&normalized_claims, 64 * 1024)
        .map_err(|_| worker_rust_error("claims"))?;
    let claims_commitment = commitment_value(env, "authorization-claims", claims.as_bytes())?;
    let account_commitment = serde_json::json!({
        "alg":"hmac-sha256","key_id":"broker-v2-account","verification_tier":"broker_only",
        "value":account_subject_commitment
    });
    let broker_commitment = commitment_value(env, "broker-instance", b"broker-v2-authority")?;
    let descriptor = profile_descriptor_override.map(<[u8]>::to_vec).unwrap_or(
        crate::composition::installed_profile_descriptor()
            .map_err(|_| worker_rust_error("profile descriptor"))?,
    );
    let descriptor_hash = hash(&descriptor);
    let auth_profile_pin = if profile_descriptor_override.is_some() {
        serde_json::json!({"entry_ref":profile_ref,"version":profile_version,"definition_hash":descriptor_hash,"approval_epoch":1,"revocation_epoch":0})
    } else {
        serde_json::to_value(&plane.auth_profile_pin)
            .map_err(|_| worker_rust_error("profile pin"))?
    };
    let custodian_pin = custodian_pin_override.cloned().unwrap_or(
        serde_json::to_value(&plane.custodian_pin)
            .map_err(|_| worker_rust_error("custodian pin"))?,
    );
    let transport_pin = transport_pin_override.cloned().unwrap_or(
        serde_json::to_value(&plane.transport_pin)
            .map_err(|_| worker_rust_error("transport pin"))?,
    );
    let authority_value = serde_json::json!({
        "schema_version":"0.2","critical_fields":[],"extensions":{},
        "org_id":org_id,"connection_ref":connection_ref,
        "auth_profile_ref":{"profile_ref":profile_ref,"version":profile_version},
        "auth_profile_pin":auth_profile_pin,
        "endpoint_set_hash":descriptor_hash,
        "public_config_hash":hash(format!("{}\0{}",plane.management.execution_lane,plane.management.custody).as_bytes()),
        "custodian":custodian_pin,"transport":transport_pin,
        "execution_lane":plane.management.execution_lane,"custody_location":plane.management.custody,
        "broker_instance_commitment":broker_commitment,
        "principal_commitments":[{"kind":"account_subject","commitment":account_commitment}],
        "authorization_claims_commitment":claims_commitment,
        "authorization_claims_schema_hash":hash(b"credential-plane.v0.2/google-exact-scopes"),
        "public_claims_projection_evidence":{"kind":"none","policy_hash":hash(b"credential-plane.v0.2/no-public-claims"),"source_claims_commitment":claims_commitment},
        "authority_epoch":1,"standing_authority_hash":standing_hash,"contract_set_hash":contract_set_hash,
        "compatible_policy_profile_hashes":[],"registry_decision_set_hash":plane.registry_decision_set_hash,
        "created_at":now_rfc3339()
    });
    let authority = parse_value::<ConnectionAuthorityViewV2>(&authority_value)
        .map_err(|_| worker_rust_error("authority"))?;
    let authority_hash = authority.content_hash();
    let initial_fence = serde_json::to_vec(&serde_json::json!({
        "schema_version":"0.2","critical_fields":[],"extensions":{},"phase":"v1_authoritative",
        "fence_generation":0,"v2_lease_ever_issued":false,"v2_rotation_ever_started":false,
        "v1_leasing_disabled":false,"active_v2_generation":null,"cas_version":0
    }))?;
    let initialize = CredentialStateEnvelope {
        org_id: org_id.into(),
        connection_ref: connection_ref.into(),
        command: CredentialStateCommand::Initialize {
            fence_json: initial_fence,
        },
    };
    let _: CredentialStateReply = credential_do(env, org_id, connection_ref, &initialize).await?;
    match material_mode {
        "local_sealed" if remote_binding_hash.is_none() && !sealed_material.is_empty() => {
            let seal = CredentialStateEnvelope {
                org_id: org_id.into(),
                connection_ref: connection_ref.into(),
                command: CredentialStateCommand::SealMaterial {
                    generation: 1,
                    sealed_envelope: sealed_material.to_vec(),
                },
            };
            let _: CredentialStateReply = credential_do(env, org_id, connection_ref, &seal).await?;
        }
        "remote_external" if sealed_material.is_empty() => {
            let bind = CredentialStateEnvelope {
                org_id: org_id.into(),
                connection_ref: connection_ref.into(),
                command: CredentialStateCommand::BindRemoteCustodian {
                    binding_hash: remote_binding_hash
                        .ok_or_else(|| worker_rust_error("remote binding"))?
                        .to_owned(),
                },
            };
            let _: CredentialStateReply = credential_do(env, org_id, connection_ref, &bind).await?;
        }
        _ => return Err(worker_rust_error("material mode")),
    }
    for (reference, schema, canonical) in [
        (
            "auth-profile",
            PublicRecordSchema::AuthProfileDescriptor,
            descriptor,
        ),
        (
            "authority-view",
            PublicRecordSchema::ConnectionAuthorityView,
            authority.canonical_bytes().to_vec(),
        ),
        (
            "standing-authority",
            PublicRecordSchema::StandingAuthority,
            standing.canonical_bytes().to_vec(),
        ),
        (
            "contract-set",
            PublicRecordSchema::ContractSet,
            contract_set.canonical_bytes().to_vec(),
        ),
    ] {
        let put = CredentialStateEnvelope {
            org_id: org_id.into(),
            connection_ref: connection_ref.into(),
            command: CredentialStateCommand::PutPublic {
                record_ref: format!("{connection_ref}:{reference}"),
                schema,
                canonical_json: canonical,
            },
        };
        let _: CredentialStateReply = credential_do(env, org_id, connection_ref, &put).await?;
    }
    let prepared_fence = serde_json::to_vec(&serde_json::json!({
        "schema_version":"0.2","critical_fields":[],"extensions":{},"phase":"v2_prepared",
        "fence_generation":1,"v2_lease_ever_issued":false,"v2_rotation_ever_started":false,
        "v1_leasing_disabled":false,"active_v2_generation":null,"cas_version":1
    }))?;
    let prepare = CredentialStateEnvelope {
        org_id: org_id.into(),
        connection_ref: connection_ref.into(),
        command: CredentialStateCommand::AdvanceFence {
            canonical_json: prepared_fence,
        },
    };
    let _: CredentialStateReply = credential_do(env, org_id, connection_ref, &prepare).await?;
    db.prepare("INSERT OR REPLACE INTO connections_v2(org_id,connection_ref,profile_ref,profile_version,profile_descriptor_hash,authority_view_hash,canonical_authority_view_json,standing_authority_ref,standing_authority_hash,canonical_standing_authority_json,contract_set_ref,contract_set_hash,canonical_contract_set_json,registry_decision_set_hash,account_subject_commitment,material_do_route,material_mode,active_material_generation,fence_generation,revocation_epoch,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,1,0,'reconciling')")
        .bind(&[
            JsValue::from_str(org_id),JsValue::from_str(connection_ref),JsValue::from_str(profile_ref),JsValue::from_str(profile_version),
            JsValue::from_str(&descriptor_hash),JsValue::from_str(&authority_hash),
            JsValue::from_str(std::str::from_utf8(authority.canonical_bytes()).map_err(|_|worker_rust_error("authority"))?),
            JsValue::from_str(&standing_authority_ref),JsValue::from_str(&standing_hash),JsValue::from_str(std::str::from_utf8(standing.canonical_bytes()).map_err(|_|worker_rust_error("standing"))?),
            JsValue::from_str(&contract_set_ref),JsValue::from_str(&contract_set_hash),JsValue::from_str(std::str::from_utf8(contract_set.canonical_bytes()).map_err(|_|worker_rust_error("contracts"))?),
            JsValue::from_str(&plane.registry_decision_set_hash),JsValue::from_str(account_subject_commitment),JsValue::from_str(material_do_route),JsValue::from_str(material_mode),
        ])?.run().await?;
    Ok(())
}

#[derive(Deserialize)]
struct HostRecordRow {
    canonical_artifact_json: String,
    connection_ref: String,
    deployment_id: String,
    parent_ref: Option<String>,
    cas_version: u64,
}

#[derive(Deserialize)]
struct OperatorBundleRow {
    canonical_bundle_hex: String,
}

async fn load_verified_operator_bundle(
    db: &D1Database,
    env: &Env,
) -> worker::Result<crate::operator_bundle::VerifiedOperatorBundle> {
    let expected_hash = env.var("OPERATOR_ARTIFACT_BUNDLE_SHA256")?.to_string();
    if !valid_hash(&expected_hash) {
        return Err(worker_rust_error("operator bundle hash"));
    }
    let result = db
        .prepare(
            "SELECT hex(canonical_bundle_jcs) AS canonical_bundle_hex \
             FROM operator_artifact_bundles WHERE bundle_hash = ? \
             ORDER BY deployment_id LIMIT 2",
        )
        .bind(&[JsValue::from_str(&expected_hash)])?
        .all()
        .await?;
    let rows = result.results::<OperatorBundleRow>()?;
    let candidates = rows
        .into_iter()
        .map(|row| {
            hex::decode(row.canonical_bundle_hex)
                .map_err(|_| worker_rust_error("operator bundle bytes"))
        })
        .collect::<worker::Result<Vec<_>>>()?;
    let key_id = env.var("OPERATOR_BUNDLE_KEY_ID")?.to_string();
    let public_key = env.var("OPERATOR_BUNDLE_PUBLIC_KEY_B64U")?.to_string();
    let recipient_key_id = env.var("GENERIC_ACTIVATION_RECIPIENT_KEY_ID")?.to_string();
    let recipient_public_key = env
        .var("GENERIC_ACTIVATION_RECIPIENT_PUBLIC_KEY_B64U")?
        .to_string();
    crate::operator_bundle::verify_loaded_operator_bundle(
        &candidates,
        &crate::operator_bundle::OperatorBundlePins {
            expected_hash: &expected_hash,
            key_id: &key_id,
            public_key_b64u: &public_key,
            activation_recipient_key_id: &recipient_key_id,
            activation_recipient_public_key_b64u: &recipient_public_key,
        },
        &now_rfc3339(),
    )
    .map_err(|_| worker_rust_error("operator bundle verification"))
}

pub async fn verified_configuration(
    env: &Env,
    db: &D1Database,
) -> Option<crate::operator_bundle::VerifiedOperatorBundle> {
    let required = [
        "BROKER_WORKER_WASM_SHA256",
        "AUTH_DRIVER_WORKER_SHA256",
        "GOOGLE_TOKEN_WORKER_SHA256",
        "GOOGLE_PROVIDER_WORKER_SHA256",
        "OPERATOR_ARTIFACT_BUNDLE_SHA256",
    ];
    if required.iter().any(|name| {
        env.var(name)
            .ok()
            .is_none_or(|value| !valid_hash(&value.to_string()))
    }) {
        return None;
    }
    let verified_bundle = load_verified_operator_bundle(db, env).await.ok()?;
    let artifacts = [
        "DEPLOYMENT_STANDING_AUTHORITY_JCS",
        "DEPLOYMENT_CONTRACT_SET_JCS",
    ];
    if artifacts.iter().any(|name| {
        env.var(name).ok().is_none_or(|value| {
            let value = value.to_string();
            value.contains("REPLACE_") || serde_json::from_str::<Value>(&value).is_err()
        })
    }) {
        return None;
    }
    if env.service("GOOGLE_TOKEN_SERVICE").is_err()
        || env.service("GOOGLE_PROVIDER_SERVICE").is_err()
        || env.service("AUTH_DRIVER_SERVICE").is_err()
        || env.durable_object("CREDENTIAL_STATE_V2_DO").is_err()
        || env.durable_object("V2_AUTHORITY_DO").is_err()
    {
        return None;
    }
    let standing = env
        .var("DEPLOYMENT_STANDING_AUTHORITY_JCS")
        .ok()
        .and_then(|value| parse::<StandingAuthorityV2>(value.to_string().as_bytes()).ok())?;
    let value = standing.view.as_value();
    let (Some(org), Some(deployment), Some(connector)) = (
        value["org_id"].as_str(),
        value["deployment_id"].as_str(),
        value["connector_ref"].as_str(),
    ) else {
        return None;
    };
    load_deployment_authority(env, org, deployment, connector).ok()?;
    Some(verified_bundle)
}

pub async fn configuration_ready(env: &Env, db: &D1Database) -> bool {
    verified_configuration(env, db).await.is_some()
}

pub fn receipt_trust(env: &Env) -> worker::Result<Response> {
    let signer =
        BrokerSigner::from_seed("broker-v2-receipt", secret_32(env, "RECEIPT_SIGNING_SEED")?);
    json(
        &serde_json::json!({
            "schema_version":"0.2",
            "issuer":"broker-v2-authority",
            "key_id":signer.key_id(),
            "alg":"ed25519",
            "public_key_b64u":URL_SAFE_NO_PAD.encode(signer.verifying_key().to_bytes())
        }),
        200,
    )
}

pub async fn install_binding(request: &mut Request, env: &Env) -> worker::Result<Response> {
    let exact_body = match bounded_body(request, MAX_MANAGEMENT_BODY).await {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let session = match authenticate(request, env, &exact_body).await {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    let body: InstallBindingRequestV2 = match parse_json(&exact_body) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    if body.deployment_id != session.deployment_id
        || body.contracts.is_empty()
        || body.bundle_id.is_empty()
        || !valid_hash(&body.binding_lock_hash)
    {
        return json(&PublicError::broker(BrokerError::Brk107), 403);
    }
    let mut contracts = Vec::new();
    for contract in &body.contracts {
        let installed = match installed_contract(contract) {
            Ok(value) => value,
            Err(error) => return json(&PublicError::broker(error), 403),
        };
        contracts.push(ApprovedAuthorityContract {
            contract_id: installed.contract_id,
            contract_hash: installed.contract_hash,
        });
    }
    let authority = match verify_authority_manifest(
        body.flow_ir_json.as_bytes(),
        &body.flow_ir_hash,
        body.authority_manifest_json.as_bytes(),
        ManifestProvenance {
            org_id: &session.org_id,
            principal_kind: broker_core::artifacts::PrincipalKind::Deployment,
            principal_id: &session.deployment_id,
        },
        &contracts,
    ) {
        Ok(value) if value.flow_id == body.flow_id => value,
        _ => return json(&PublicError::broker(BrokerError::Brk109), 400),
    };
    let manifest_contracts = authority
        .manifest
        .view
        .nodes
        .values()
        .flat_map(|node| {
            node.operations
                .iter()
                .map(|operation| operation.contract_id.as_str())
        })
        .collect::<BTreeSet<_>>();
    let requested_contracts = body
        .contracts
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if manifest_contracts != requested_contracts
        || requested_contracts.len() != body.contracts.len()
    {
        return json(&PublicError::broker(BrokerError::Brk109), 400);
    }
    let db = env.d1("BROKER_DB")?;
    let connection = match load_connection(&db, &session.org_id, &body.connection_ref).await? {
        Some(value)
            if matches!(value.status.as_str(), "reconciling" | "active")
                && value.registry_is_current(&now_rfc3339()) =>
        {
            value
        }
        _ => return json(&PublicError::broker(BrokerError::Brk106), 409),
    };
    #[derive(Deserialize)]
    struct ExistingBindingRow {
        artifact_ref: String,
        artifact_hash: String,
        canonical_artifact_json: String,
    }
    let existing = db.prepare("SELECT r.artifact_ref,r.artifact_hash,r.canonical_artifact_json FROM v2_host_records r JOIN v2_binding_manifests m ON m.org_id=r.org_id AND m.binding_ref=r.artifact_ref WHERE r.org_id=? AND r.connection_ref=? AND r.deployment_id=? AND r.artifact_kind='binding' AND m.flow_ir_hash=? AND m.flow_ir_json=? AND m.manifest_json=? LIMIT 1")
        .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&body.connection_ref),JsValue::from_str(&session.deployment_id),JsValue::from_str(&body.flow_ir_hash),JsValue::from_str(&body.flow_ir_json),JsValue::from_str(&body.authority_manifest_json)])?
        .first::<ExistingBindingRow>(None).await?;
    if let Some(existing) = existing {
        let parsed = parse::<BindingAttestationV2>(existing.canonical_artifact_json.as_bytes())
            .map_err(|_| worker_rust_error("binding"))?;
        let standing_value =
            parse::<StandingAuthorityV2>(connection.canonical_standing_authority_json.as_bytes())
                .map_err(|_| worker_rust_error("standing authority"))?;
        let _operator_policy_hash = standing_value.view.as_value()["operator_policy_hash"]
            .as_str()
            .ok_or_else(|| worker_rust_error("operator policy"))?
            .to_owned();
        let mut expected_policy_hashes = vec![
            hash(body.bundle_id.as_bytes()),
            body.binding_lock_hash.clone(),
            _operator_policy_hash,
        ];
        expected_policy_hashes.sort();
        if parsed.view.as_value()["policy_instance_hashes"]
            == serde_json::json!(expected_policy_hashes)
        {
            return json(
                &serde_json::json!({"binding_ref":existing.artifact_ref,"binding_hash":existing.artifact_hash,"binding":parsed.view.as_value(),"redelivery":true}),
                200,
            );
        }
    }
    let standing =
        match parse::<StandingAuthorityV2>(connection.canonical_standing_authority_json.as_bytes())
        {
            Ok(value) if value.view.as_value()["deployment_id"] == session.deployment_id => value,
            _ => return json(&PublicError::broker(BrokerError::Brk107), 403),
        };
    if !artifact_time_is_current(standing.view.as_value(), &now_rfc3339()) {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    let authorized_contracts =
        match parse::<ContractSetV2>(connection.canonical_contract_set_json.as_bytes()) {
            Ok(value) => value,
            Err(_) => return json(&PublicError::broker(BrokerError::Brk106), 409),
        };
    let authorized_contracts = authorized_contracts.view.as_value()["contracts"]
        .as_array()
        .map(|values| {
            values
                .iter()
                .filter_map(|value| {
                    Some((
                        value["contract_id"].as_str()?,
                        value["contract_hash"].as_str()?,
                    ))
                })
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    if contracts.iter().any(|contract| {
        !authorized_contracts.contains(&(contract.contract_id, contract.contract_hash))
    }) {
        return json(&PublicError::broker(BrokerError::Brk108), 403);
    }
    let authority_view = match parse::<ConnectionAuthorityViewV2>(
        connection.canonical_authority_view_json.as_bytes(),
    ) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::broker(BrokerError::Brk106), 409),
    };
    let view = authority_view.view.as_value();
    let profile_ref = match parse_value::<AuthProfileRefV2>(&serde_json::json!({
        "profile_ref":connection.profile_ref,"version":connection.profile_version
    })) {
        Ok(value) => value.view,
        Err(error) => return json(&PublicError::broker(error), 409),
    };
    let profile_pin = match parse_value::<RegistryPinV2>(&view["auth_profile_pin"]) {
        Ok(value) => value.view,
        Err(error) => return json(&PublicError::broker(error), 409),
    };
    let mut supported_contract_hashes = contracts
        .iter()
        .map(|contract| contract.contract_hash.to_owned())
        .collect::<Vec<_>>();
    supported_contract_hashes.sort();
    supported_contract_hashes.dedup();
    let standing_value = standing.view.as_value();
    let _operator_policy_hash = standing_value["operator_policy_hash"]
        .as_str()
        .filter(|value| valid_hash(value))
        .ok_or_else(|| worker_rust_error("operator policy"))?
        .to_owned();
    let mut required_predicates = Vec::new();
    let controls = BTreeSet::from(["durable_before_dispatch", "exact_redelivery", "pop_bound"]);
    for value in standing_value["required_assurance_predicates"]
        .as_array()
        .ok_or_else(|| worker_rust_error("assurance predicates"))?
    {
        let parsed = match parse_value::<AssurancePredicateV2>(value) {
            Ok(value) => value,
            Err(error) => return json(&PublicError::broker(error), 409),
        };
        let predicate = parsed.view.as_value();
        let required = predicate["required_kernel_controls"]
            .as_array()
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        if predicate["kind"] != "brokered_count"
            || required.is_empty()
            || !required.is_subset(&controls)
        {
            return json(&PublicError::broker(BrokerError::Brk108), 403);
        }
        required_predicates.push(parsed.view);
    }
    if required_predicates.is_empty() {
        return json(&PublicError::broker(BrokerError::Brk108), 403);
    }
    let signer = BrokerSigner::from_seed(
        "broker-v2-authority",
        secret_32(env, "BINDING_SIGNING_SEED")?,
    );
    let binding = match issue_binding_v2(
        body.flow_ir_json.as_bytes(),
        &authority,
        connection.canonical_authority_view_json.as_bytes(),
        BindingDerivationV2 {
            org_id: session.org_id.clone(),
            principal: session.deployment_id.clone(),
            issuer: "broker-v2-authority".into(),
            deployment_id: session.deployment_id.clone(),
            standing_authority_ref: connection.standing_authority_ref.clone(),
            standing_authority_hash: connection.standing_authority_hash.clone(),
            contract_set_ref: connection.contract_set_ref.clone(),
            contract_set_hash: connection.contract_set_hash.clone(),
            connection_ref: connection.connection_ref.clone(),
            minimum_material_generation: connection.active_material_generation,
            auth_profile_ref: profile_ref,
            auth_profile_pin: profile_pin,
            supported_contract_hashes,
            policy_instance_hashes: {
                let mut hashes = vec![
                    hash(body.bundle_id.as_bytes()),
                    body.binding_lock_hash.clone(),
                    _operator_policy_hash,
                ];
                hashes.sort();
                hashes
            },
            required_assurance_predicates: required_predicates,
            observed_at: now_rfc3339(),
            expires_at: rfc3339_from_seconds(now_seconds() + 3600),
        },
        &signer,
    ) {
        Ok(value) => value,
        Err(error) => return json(&PublicError::broker(host_error(error)), 409),
    };
    let binding_ref = opaque_id("binding_v2_")?;
    let canonical = binding.canonical_bytes();
    let artifact_hash = binding.content_hash();
    let inserted = db
        .prepare(
            "INSERT INTO v2_host_records (org_id,artifact_ref,artifact_kind,artifact_hash,canonical_artifact_json,connection_ref,deployment_id,parent_ref,cas_version,created_at) VALUES (?,?,'binding',?,?,?,?,NULL,0,?)",
        )
        .bind(&[
            JsValue::from_str(&session.org_id),
            JsValue::from_str(&binding_ref),
            JsValue::from_str(&artifact_hash),
            JsValue::from_str(std::str::from_utf8(canonical).map_err(|_| worker_rust_error("binding"))?),
            JsValue::from_str(&connection.connection_ref),
            JsValue::from_str(&session.deployment_id),
            JsValue::from_f64(now_seconds() as f64),
        ])?
        .run()
        .await;
    if inserted.is_err() {
        return json(&PublicError::broker(BrokerError::Brk203), 409);
    }
    db.prepare("INSERT INTO v2_binding_manifests(org_id,binding_ref,manifest_hash,manifest_json,flow_ir_hash,flow_ir_json) VALUES(?,?,?,?,?,?)")
        .bind(&[
            JsValue::from_str(&session.org_id), JsValue::from_str(&binding_ref),
            JsValue::from_str(&hash(body.authority_manifest_json.as_bytes())),
            JsValue::from_str(&body.authority_manifest_json), JsValue::from_str(&body.flow_ir_hash),
            JsValue::from_str(&body.flow_ir_json),
        ])?.run().await?;
    if connection.status == "reconciling" {
        let put = CredentialStateEnvelope {
            org_id: session.org_id.clone(),
            connection_ref: connection.connection_ref.clone(),
            command: CredentialStateCommand::PutPublic {
                record_ref: binding_ref.clone(),
                schema: PublicRecordSchema::BindingAttestation,
                canonical_json: binding.canonical_bytes().to_vec(),
            },
        };
        let _: CredentialStateReply =
            credential_do(env, &session.org_id, &connection.connection_ref, &put).await?;
        let active_fence = serde_json::to_vec(&serde_json::json!({
            "schema_version":"0.2","critical_fields":[],"extensions":{},"phase":"v2_authoritative",
            "fence_generation":2,"v2_lease_ever_issued":false,"v2_rotation_ever_started":false,
            "v1_leasing_disabled":true,"active_v2_generation":connection.active_material_generation,"cas_version":2
        }))?;
        let advance = CredentialStateEnvelope {
            org_id: session.org_id.clone(),
            connection_ref: connection.connection_ref.clone(),
            command: CredentialStateCommand::AdvanceFence {
                canonical_json: active_fence,
            },
        };
        let _: CredentialStateReply =
            credential_do(env, &session.org_id, &connection.connection_ref, &advance).await?;
        complete_fresh_cutover(
            &db,
            &session.org_id,
            &connection.connection_ref,
            connection.active_material_generation,
            &artifact_hash,
        )
        .await?;
        db.prepare("UPDATE connections_v2 SET status='active',fence_generation=2 WHERE org_id=? AND connection_ref=? AND status='reconciling'")
          .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?.run().await?;
    }
    json(
        &serde_json::json!({
            "binding_ref":binding_ref,
            "binding_hash":artifact_hash,
            "binding":binding.view.as_value()
        }),
        201,
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyReconcileRequest {
    session_ref: String,
    legacy_connection_ref: String,
    replacement_connection_ref: String,
    expected_cas_version: u64,
}

pub async fn reconcile_legacy(request: &mut Request, env: &Env) -> worker::Result<Response> {
    if !header_secret_matches(
        request,
        env,
        "x-lattice-service-auth",
        "INVOKE_SERVICE_AUTH",
    ) {
        return json(&PublicError::broker(BrokerError::Brk101), 401);
    }
    let exact = match bounded_body(request, MAX_MANAGEMENT_BODY).await {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let session = match authenticate(request, env, &exact).await {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    let body: LegacyReconcileRequest = match parse_json(&exact) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    if body.session_ref != session.session_ref
        || body.legacy_connection_ref == body.replacement_connection_ref
    {
        return json(&PublicError::broker(BrokerError::Brk107), 403);
    }
    let db = env.d1("BROKER_DB")?;
    #[derive(Deserialize)]
    struct InventoryAuthorityRow {
        canonical_inventory_json: String,
        canonical_decision_json: String,
    }
    let authority = db.prepare("SELECT i.canonical_inventory_json,d.canonical_decision_json FROM legacy_admission_inventories_v2 i JOIN legacy_inventory_decisions_v2 d ON d.org_id=i.org_id AND d.inventory_ref=i.inventory_ref WHERE i.org_id=? AND i.inventory_ref='legacy.production.inventory.final'")
        .bind(&[JsValue::from_str(&session.org_id)])?.first::<InventoryAuthorityRow>(None).await?;
    let Some(authority) = authority else {
        return json(&PublicError::broker(BrokerError::Brk109), 409);
    };
    let inventory = parse::<broker_core::credential::legacy::LegacyAdmissionInventoryV2>(
        authority.canonical_inventory_json.as_bytes(),
    )
    .map_err(|_| worker_rust_error("legacy inventory"))?;
    let decision = parse::<broker_core::credential::legacy::LegacyInventoryDecisionV2>(
        authority.canonical_decision_json.as_bytes(),
    )
    .map_err(|_| worker_rust_error("legacy inventory decision"))?;
    let root_bytes = URL_SAFE_NO_PAD
        .decode(
            env.var("LEGACY_CUTOVER_AUTHORITY_PUBLIC_KEY_B64U")?
                .to_string(),
        )
        .map_err(|_| worker_rust_error("legacy authority"))?;
    let root_bytes: [u8; 32] = root_bytes
        .try_into()
        .map_err(|_| worker_rust_error("legacy authority"))?;
    let root = broker_core::signing::BrokerVerifyingKey::from_bytes(
        env.var("LEGACY_CUTOVER_AUTHORITY_KEY_ID")?.to_string(),
        root_bytes,
    )
    .map_err(|_| worker_rust_error("legacy authority"))?;
    if broker_core::credential::signing::verify_signed(&inventory, &root).is_err()
        || broker_core::credential::signing::verify_signed(&decision, &root).is_err()
        || !inventory.view.as_value()["items"]
            .as_array()
            .is_some_and(|values| {
                values.iter().any(|value| {
                    value["org_id"] == session.org_id
                        && value["connection_ref"] == body.legacy_connection_ref
                })
            })
        || decision.view.as_value()["inventory_ref"] != inventory.view.as_value()["inventory_ref"]
        || decision.view.as_value()["inventory_hash"] != inventory.content_hash()
        || decision.view.as_value()["status"] != "approved"
        || decision.view.as_value()["effective_at"]
            .as_str()
            .is_none_or(|value| value > now_rfc3339().as_str())
        || decision.view.as_value()["expires_at"]
            .as_str()
            .is_none_or(|value| value <= now_rfc3339().as_str())
        || decision.view.as_value()["revocation_epoch"]
            .as_u64()
            .unwrap_or(1)
            != 0
    {
        return json(&PublicError::broker(BrokerError::Brk109), 409);
    }
    #[derive(Deserialize)]
    struct LegacyRow {
        refresh_do_route: String,
        auth_profile_ref: String,
    }
    #[derive(Deserialize)]
    struct ReplacementRow {
        profile_ref: String,
        active_material_generation: u64,
        status: String,
    }
    let legacy = db.prepare("SELECT refresh_do_route,auth_profile_ref FROM connections_v1_quarantine WHERE org_id=? AND connection_ref=? AND status!='revoked'")
        .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&body.legacy_connection_ref)])?.first::<LegacyRow>(None).await?;
    let replacement = db.prepare("SELECT profile_ref,active_material_generation,status FROM connections_v2 WHERE org_id=? AND connection_ref=?")
        .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&body.replacement_connection_ref)])?.first::<ReplacementRow>(None).await?;
    let (legacy, replacement) = match (legacy, replacement) {
        (Some(legacy), Some(replacement))
            if replacement.status == "active"
                && legacy.auth_profile_ref.trim_end_matches("@1") == replacement.profile_ref =>
        {
            (legacy, replacement)
        }
        _ => return json(&PublicError::broker(BrokerError::Brk106), 409),
    };
    #[derive(Deserialize)]
    struct StateRow {
        cas_version: u64,
        v2_lease_ever_issued: u64,
        v2_rotation_ever_started: u64,
    }
    let state = db.prepare("SELECT s.cas_version,f.v2_lease_ever_issued,f.v2_rotation_ever_started FROM credential_cutover_state_v2 s JOIN credential_fences_v2 f ON f.org_id=s.org_id AND f.connection_ref=s.connection_ref WHERE s.org_id=? AND s.connection_ref=?")
        .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&body.legacy_connection_ref)])?.first::<StateRow>(None).await?;
    let Some(state) = state else {
        return json(&PublicError::broker(BrokerError::Brk203), 409);
    };
    if state.cas_version != body.expected_cas_version {
        return json(&PublicError::broker(BrokerError::Brk203), 409);
    }
    let revoked: RefreshReply = do_request(
        env,
        "CONNECTION_REFRESH_DO",
        &legacy.refresh_do_route,
        &RefreshCommand::Revoke,
    )
    .await?;
    if !matches!(revoked, RefreshReply::Revoked { .. }) {
        return json(&PublicError::unavailable(), 503);
    }
    let confirmation = broker_core::canonical::from_serde(&serde_json::json!({
        "schema_version":"0.2","org_id":session.org_id,"legacy_connection_ref":body.legacy_connection_ref,
        "replacement_connection_ref":body.replacement_connection_ref,"legacy_material":"destroyed",
        "active_v2_generation":replacement.active_material_generation
    }),64*1024).map_err(|_|worker_rust_error("cutover"))?;
    let confirmation_hash = hash(confirmation.as_bytes());
    let fence = broker_core::canonical::from_serde(&serde_json::json!({
        "schema_version":"0.2","phase":"v2_authoritative","fence_generation":2,
        "v2_lease_ever_issued":state.v2_lease_ever_issued != 0,"v2_rotation_ever_started":state.v2_rotation_ever_started != 0,"v1_leasing_disabled":true,
        "active_v2_generation":replacement.active_material_generation,"cas_version":body.expected_cas_version+1
    }),64*1024).map_err(|_|worker_rust_error("cutover"))?;
    let event = broker_core::canonical::from_serde(&serde_json::json!({
        "schema_version":"0.2","org_id":session.org_id,"connection_ref":body.legacy_connection_ref,
        "phase":"complete","cas_version":body.expected_cas_version + 1,
        "destruction_evidence_hash":confirmation_hash
    }),64*1024).map_err(|_|worker_rust_error("cutover"))?;
    let confirmation_json =
        std::str::from_utf8(confirmation.as_bytes()).map_err(|_| worker_rust_error("cutover"))?;
    let fence_json =
        std::str::from_utf8(fence.as_bytes()).map_err(|_| worker_rust_error("cutover"))?;
    let event_json =
        std::str::from_utf8(event.as_bytes()).map_err(|_| worker_rust_error("cutover"))?;
    let statements = vec![
        db.prepare("INSERT OR IGNORE INTO legacy_destruction_evidence_v2(org_id,connection_ref,refresh_do_route_hash,destroyed_storage_key_hash,confirmation_hash,canonical_confirmation_json) VALUES(?,?,?,?,?,?)")
          .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&body.legacy_connection_ref),JsValue::from_str(&hash(legacy.refresh_do_route.as_bytes())),JsValue::from_str(&hash(format!("{}:material",legacy.refresh_do_route).as_bytes())),JsValue::from_str(&confirmation_hash),JsValue::from_str(confirmation_json)])?,
        db.prepare("UPDATE credential_cutover_state_v2 SET phase='complete',expected_material_generation=?,legacy_destruction_evidence_hash=?,cas_version=cas_version+1 WHERE org_id=? AND connection_ref=? AND cas_version=? AND phase!='complete'")
          .bind(&[JsValue::from_f64(replacement.active_material_generation as f64),JsValue::from_str(&confirmation_hash),JsValue::from_str(&session.org_id),JsValue::from_str(&body.legacy_connection_ref),JsValue::from_f64(body.expected_cas_version as f64)])?,
        db.prepare("UPDATE credential_fences_v2 SET phase='v2_authoritative',fence_generation=MAX(fence_generation,2),v1_leasing_disabled=1,active_v2_generation=?,cas_version=cas_version+1,canonical_fence_json=? WHERE org_id=? AND connection_ref=? AND cas_version=?")
          .bind(&[JsValue::from_f64(replacement.active_material_generation as f64),JsValue::from_str(fence_json),JsValue::from_str(&session.org_id),JsValue::from_str(&body.legacy_connection_ref),JsValue::from_f64(body.expected_cas_version as f64)])?,
        db.prepare("UPDATE connections_v1_quarantine SET status='revoked',revocation_epoch=revocation_epoch+1 WHERE org_id=? AND connection_ref=? AND status!='revoked'")
          .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&body.legacy_connection_ref)])?,
        db.prepare("UPDATE connections_v2 SET status='revoked',fence_generation=fence_generation+1 WHERE org_id=? AND connection_ref=? AND status!='revoked'")
          .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&body.legacy_connection_ref)])?,
        db.prepare("INSERT INTO credential_cutover_events_v2(org_id,connection_ref,phase,event_hash,canonical_event_json,recorded_at) VALUES(?,?,?,?,?,?)")
          .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&body.legacy_connection_ref),JsValue::from_str("complete"),JsValue::from_str(&hash(event.as_bytes())),JsValue::from_str(event_json),JsValue::from_f64(now_seconds() as f64)])?,
    ];
    let results = db.batch(statements).await?;
    if results.len() != 6 || results.iter().any(|result| !result.success()) {
        return json(&PublicError::unavailable(), 503);
    }
    json(
        &serde_json::json!({"status":"complete","legacy_connection_ref":body.legacy_connection_ref,"replacement_connection_ref":body.replacement_connection_ref,"destruction_evidence_hash":confirmation_hash}),
        200,
    )
}

async fn complete_fresh_cutover(
    db: &D1Database,
    org_id: &str,
    connection_ref: &str,
    generation: u64,
    binding_hash: &str,
) -> worker::Result<()> {
    #[derive(Deserialize)]
    struct CountRow {
        count: u64,
    }
    let legacy = db.prepare("SELECT COUNT(*) AS count FROM connections_v1_quarantine WHERE org_id=? AND connection_ref=?")
        .bind(&[JsValue::from_str(org_id),JsValue::from_str(connection_ref)])?
        .first::<CountRow>(None).await?.is_some_and(|row| row.count != 0);
    if legacy {
        return Err(worker_rust_error(
            "legacy cutover requires replacement activation",
        ));
    }
    let confirmation = broker_core::canonical::from_serde(
        &serde_json::json!({
            "schema_version":"0.2","org_id":org_id,"connection_ref":connection_ref,
            "legacy_material":"absent","active_v2_generation":generation
        }),
        64 * 1024,
    )
    .map_err(|_| worker_rust_error("cutover"))?;
    let confirmation_hash = hash(confirmation.as_bytes());
    db.prepare("INSERT OR IGNORE INTO legacy_destruction_evidence_v2(org_id,connection_ref,refresh_do_route_hash,destroyed_storage_key_hash,confirmation_hash,canonical_confirmation_json) VALUES(?,?,?,?,?,?)")
        .bind(&[JsValue::from_str(org_id),JsValue::from_str(connection_ref),JsValue::from_str(&hash(b"legacy-refresh-route-absent")),JsValue::from_str(&hash(b"legacy-storage-key-absent")),JsValue::from_str(&confirmation_hash),JsValue::from_str(std::str::from_utf8(confirmation.as_bytes()).map_err(|_|worker_rust_error("cutover"))?)])?.run().await?;
    for (cas, phase) in [
        "registry_verified",
        "binding_verified",
        "fence_switched",
        "legacy_material_destroyed",
        "complete",
    ]
    .into_iter()
    .enumerate()
    {
        let event = broker_core::canonical::from_serde(
            &serde_json::json!({
                "schema_version":"0.2","org_id":org_id,"connection_ref":connection_ref,
                "phase":phase,"cas_version":cas + 2
            }),
            64 * 1024,
        )
        .map_err(|_| worker_rust_error("cutover"))?;
        db.prepare("INSERT OR IGNORE INTO credential_cutover_events_v2(org_id,connection_ref,phase,event_hash,canonical_event_json,recorded_at) VALUES(?,?,?,?,?,?)")
            .bind(&[JsValue::from_str(org_id),JsValue::from_str(connection_ref),JsValue::from_str(phase),JsValue::from_str(&hash(event.as_bytes())),JsValue::from_str(std::str::from_utf8(event.as_bytes()).map_err(|_|worker_rust_error("cutover"))?),JsValue::from_f64(now_seconds() as f64)])?.run().await?;
    }
    let fence = broker_core::canonical::from_serde(
        &serde_json::json!({
            "schema_version":"0.2","phase":"v2_authoritative","fence_generation":2,
            "v2_lease_ever_issued":false,"v2_rotation_ever_started":false,
            "v1_leasing_disabled":true,"active_v2_generation":generation,"cas_version":2
        }),
        64 * 1024,
    )
    .map_err(|_| worker_rust_error("cutover"))?;
    db.prepare("UPDATE credential_fences_v2 SET phase='v2_authoritative',fence_generation=2,v1_leasing_disabled=1,active_v2_generation=?,cas_version=2,canonical_fence_json=? WHERE org_id=? AND connection_ref=? AND phase='v2_prepared'")
        .bind(&[JsValue::from_f64(generation as f64),JsValue::from_str(std::str::from_utf8(fence.as_bytes()).map_err(|_|worker_rust_error("cutover"))?),JsValue::from_str(org_id),JsValue::from_str(connection_ref)])?.run().await?;
    db.prepare("INSERT OR IGNORE INTO credential_cutover_state_v2(org_id,connection_ref,phase,expected_material_generation,binding_hash,legacy_destruction_evidence_hash,cas_version) VALUES(?,?,'complete',?,?,?,6)")
        .bind(&[JsValue::from_str(org_id),JsValue::from_str(connection_ref),JsValue::from_f64(generation as f64),JsValue::from_str(binding_hash),JsValue::from_str(&confirmation_hash)])?.run().await?;
    db.prepare("UPDATE credential_cutover_state_v2 SET phase='complete',binding_hash=?,legacy_destruction_evidence_hash=?,cas_version=6 WHERE org_id=? AND connection_ref=? AND phase IN ('material_sealed','registry_verified','binding_verified','fence_switched','legacy_material_destroyed','complete')")
        .bind(&[JsValue::from_str(binding_hash),JsValue::from_str(&confirmation_hash),JsValue::from_str(org_id),JsValue::from_str(connection_ref)])?.run().await?;
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeLeaseRequest {
    session_ref: String,
    binding_ref: String,
    bundle_id: String,
    flow_ir_hash: String,
    binding_lock_hash: String,
    flow_id: String,
    run_id: String,
    node_id: String,
    node_alias: String,
    activation_ordinal: u64,
    operation_contract: String,
}

pub async fn issue_node_lease(request: &mut Request, env: &Env) -> worker::Result<Response> {
    if !header_secret_matches(
        request,
        env,
        "x-lattice-service-auth",
        "INVOKE_SERVICE_AUTH",
    ) {
        return json(&PublicError::broker(BrokerError::Brk101), 401);
    }
    let exact_body = match bounded_body(request, MAX_INVOKE_BODY).await {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let session = match authenticate(request, env, &exact_body).await {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    let body: NodeLeaseRequest = match parse_json(&exact_body) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    if body.session_ref != session.session_ref {
        return json(&PublicError::broker(BrokerError::Brk102), 401);
    }
    let db = env.d1("BROKER_DB")?;
    let record = match load_host_record(&db, &session.org_id, &body.binding_ref, "binding").await? {
        Some(value) if value.deployment_id == session.deployment_id => value,
        _ => return json(&PublicError::broker(BrokerError::Brk109), 403),
    };
    let binding = match parse::<BindingAttestationV2>(record.canonical_artifact_json.as_bytes()) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::broker(BrokerError::Brk106), 409),
    };
    let connection = match load_connection(&db, &session.org_id, &record.connection_ref).await? {
        Some(value) if value.status == "active" && value.registry_is_current(&now_rfc3339()) => {
            value
        }
        _ => return json(&PublicError::broker(BrokerError::Brk106), 409),
    };
    let manifest_hash = binding.view.as_value()["authority_manifest_hash"]
        .as_str()
        .unwrap_or_default();
    let installed = match installed_contract(&body.operation_contract) {
        Ok(value) => value,
        Err(error) => return json(&PublicError::broker(error), 403),
    };
    // The binding was issued only after exact manifest verification. The lease
    // limits are loaded from a separately persisted exact manifest record.
    let authority_json = load_binding_manifest(&db, &session.org_id, &body.binding_ref).await?;
    let authority: broker_core::artifacts::ParsedArtifact<
        broker_core::artifacts::FlowAuthorityManifest,
    > = match broker_core::artifacts::parse(authority_json.as_bytes()) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::broker(BrokerError::Brk106), 409),
    };
    if authority.content_hash() != manifest_hash {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    let node = match authority.view.nodes.get(&body.node_alias) {
        Some(value) if value.node_id == body.node_id => value,
        _ => return json(&PublicError::broker(BrokerError::Brk107), 403),
    };
    let operation = match node
        .operations
        .iter()
        .find(|value| value.contract_id == body.operation_contract)
    {
        Some(value) if value.contract_hash == installed.contract_hash => value,
        _ => return json(&PublicError::broker(BrokerError::Brk108), 403),
    };
    let flow_limit = authority
        .view
        .aggregate_ceilings
        .as_ref()
        .and_then(|value| value.flow.as_ref())
        .map(|value| value.max_logical_calls)
        .unwrap_or(operation.call_budget.max_logical_calls);
    let connection_limit = operation
        .connection_aggregate_key
        .as_ref()
        .and_then(|key| {
            authority
                .view
                .aggregate_ceilings
                .as_ref()
                .and_then(|value| value.connections.get(key))
        })
        .map(|value| value.max_logical_calls)
        .unwrap_or(operation.call_budget.max_logical_calls);
    let lease_identity = broker_core::canonical::from_serde(&serde_json::json!({
        "org_id":session.org_id,"deployment_id":session.deployment_id,"session_ref":session.session_ref,
        "binding_ref":body.binding_ref,"bundle_id":body.bundle_id,"flow_ir_hash":body.flow_ir_hash,
        "binding_lock_hash":body.binding_lock_hash,"flow_id":body.flow_id,"run_id":body.run_id,
        "node_id":body.node_id,"node_alias":body.node_alias,"activation_ordinal":body.activation_ordinal,
        "operation_contract":body.operation_contract
    }),64*1024).map_err(|_|worker_rust_error("lease identity"))?;
    let node_lease_ref = format!("node_lease_{}", &hash(lease_identity.as_bytes())[7..]);
    let scope = ScopeInput {
        org_id: session.org_id.clone(),
        deployment_id: session.deployment_id.clone(),
        session_ref: session.session_ref.clone(),
        pop_key_thumbprint: session.pop_key_thumbprint.clone(),
        bundle_id: body.bundle_id.clone(),
        flow_ir_hash: body.flow_ir_hash,
        binding_lock_hash: body.binding_lock_hash.clone(),
        flow_id: body.flow_id,
        run_id: body.run_id,
        node_id: body.node_id,
        node_alias: body.node_alias,
        activation_ordinal: body.activation_ordinal,
    };
    let standing =
        parse::<StandingAuthorityV2>(connection.canonical_standing_authority_json.as_bytes())
            .map_err(|_| worker_rust_error("standing authority"))?;
    let operator_policy_hash = standing.view.as_value()["operator_policy_hash"]
        .as_str()
        .ok_or_else(|| worker_rust_error("operator policy"))?
        .to_owned();
    let mut expected_policy_hashes = vec![
        hash(body.bundle_id.as_bytes()),
        body.binding_lock_hash.clone(),
        operator_policy_hash,
    ];
    expected_policy_hashes.sort();
    if binding.view.as_value()["policy_instance_hashes"]
        != serde_json::json!(expected_policy_hashes)
    {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    if let Some(existing) =
        load_host_record(&db, &session.org_id, &node_lease_ref, "node_lease").await?
    {
        if existing.deployment_id != session.deployment_id
            || existing.parent_ref.as_deref() != Some(body.binding_ref.as_str())
        {
            return json(&PublicError::broker(BrokerError::Brk203), 409);
        }
        let lease = parse::<NodeLeaseV2>(existing.canonical_artifact_json.as_bytes())
            .map_err(|_| worker_rust_error("lease"))?;
        return json(
            &serde_json::json!({"node_lease_ref":node_lease_ref,"node_lease":lease.view.as_value(),"cas_version":existing.cas_version,"redelivery":true}),
            200,
        );
    }
    let standing =
        parse::<StandingAuthorityV2>(connection.canonical_standing_authority_json.as_bytes())
            .map_err(|_| worker_rust_error("standing authority"))?;
    if !artifact_time_is_current(standing.view.as_value(), &now_rfc3339()) {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    let maxima = &standing.view.as_value()["maximum_budgets"];
    let standing_limit = |field: &str| maxima[field].as_u64().unwrap_or(0);
    let standing_attempts =
        standing_limit("dispatch_attempts_per_call").min(u64::from(u8::MAX)) as u8;
    let lock = lock_from_binding(&binding)?;
    let limits = SerializableLeaseLimits {
        operation_contract: body.operation_contract,
        contract_hash: installed.contract_hash.into(),
        logical_calls: operation
            .call_budget
            .max_logical_calls
            .min(standing_limit("logical_calls")),
        dispatch_attempts_per_call: operation
            .call_budget
            .max_dispatch_attempts_per_call
            .min(standing_attempts),
        flow_logical_calls: flow_limit.min(standing_limit("flow_logical_calls")),
        connection_logical_calls: connection_limit.min(standing_limit("connection_logical_calls")),
        node_logical_calls: operation
            .call_budget
            .max_logical_calls
            .min(standing_limit("node_logical_calls")),
        first_activation_ordinal: scope.activation_ordinal,
        last_activation_ordinal: scope.activation_ordinal,
        semantic_effect_slots: vec![installed.semantic_effect_slot.into()],
        not_before: rfc3339_from_seconds(now_seconds() - 1),
        expires_at: rfc3339_from_seconds(now_seconds() + 300),
        node_lease_ref: node_lease_ref.clone(),
        jti: format!("lease_jti_{}", &hash(node_lease_ref.as_bytes())[7..]),
    };
    let reply: V2AuthorityReply = authority_do(
        env,
        &session.org_id,
        &body.binding_ref,
        &V2AuthorityCommand::IssueLease {
            canonical_binding: record.canonical_artifact_json.into_bytes(),
            lock,
            canonical_authority_view: connection
                .canonical_authority_view_json
                .clone()
                .into_bytes(),
            material_generation: connection.active_material_generation,
            now: now_rfc3339(),
            scope,
            limits,
        },
    )
    .await?;
    store_host_artifact(
        &db,
        &session.org_id,
        &reply.artifact_ref,
        "node_lease",
        &reply.canonical_artifact,
        &connection.connection_ref,
        &session.deployment_id,
        Some(&body.binding_ref),
        reply.cas_version,
    )
    .await?;
    let lease =
        parse::<NodeLeaseV2>(&reply.canonical_artifact).map_err(|_| worker_rust_error("lease"))?;
    json(
        &serde_json::json!({"node_lease_ref":reply.artifact_ref,"node_lease":lease.view.as_value(),"cas_version":reply.cas_version}),
        201,
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExactGrantRequest {
    session_ref: String,
    binding_ref: String,
    node_lease_ref: String,
    bundle_id: String,
    flow_ir_hash: String,
    binding_lock_hash: String,
    flow_id: String,
    run_id: String,
    node_id: String,
    node_alias: String,
    activation_ordinal: u64,
    semantic_effect_slot: String,
    expected_cas_version: u64,
    input: Value,
}

pub async fn derive_grant(request: &mut Request, env: &Env) -> worker::Result<Response> {
    if !header_secret_matches(
        request,
        env,
        "x-lattice-service-auth",
        "INVOKE_SERVICE_AUTH",
    ) {
        return json(&PublicError::broker(BrokerError::Brk101), 401);
    }
    let exact_body = match bounded_body(request, MAX_INVOKE_BODY).await {
        Ok(v) => v,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let session = match authenticate(request, env, &exact_body).await {
        Ok(v) => v,
        Err(r) => return Ok(r),
    };
    let body: ExactGrantRequest = match parse_json(&exact_body) {
        Ok(v) => v,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    if body.session_ref != session.session_ref {
        return json(&PublicError::broker(BrokerError::Brk102), 401);
    }
    let db = env.d1("BROKER_DB")?;
    let lease_row =
        match load_host_record(&db, &session.org_id, &body.node_lease_ref, "node_lease").await? {
            Some(v) if v.deployment_id == session.deployment_id => v,
            _ => return json(&PublicError::broker(BrokerError::Brk109), 403),
        };
    if lease_row.parent_ref.as_deref() != Some(body.binding_ref.as_str()) {
        return json(&PublicError::broker(BrokerError::Brk107), 403);
    }
    let lease = parse::<NodeLeaseV2>(lease_row.canonical_artifact_json.as_bytes())
        .map_err(|_| worker_rust_error("lease"))?;
    if !artifact_time_is_current(lease.view.as_value(), &now_rfc3339()) {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    let connection = match load_connection(&db, &session.org_id, &lease_row.connection_ref).await? {
        Some(v) if v.status == "active" && v.registry_is_current(&now_rfc3339()) => v,
        _ => return json(&PublicError::broker(BrokerError::Brk106), 409),
    };
    let canonical_input = broker_core::canonical::from_serde(
        &body.input,
        broker_core::canonical::MAX_OPERATION_BYTES,
    )
    .map_err(|_| worker_rust_error("canonical input"))?
    .into_bytes();
    let scope = ScopeInput {
        org_id: session.org_id.clone(),
        deployment_id: session.deployment_id.clone(),
        session_ref: session.session_ref.clone(),
        pop_key_thumbprint: session.pop_key_thumbprint.clone(),
        bundle_id: body.bundle_id,
        flow_ir_hash: body.flow_ir_hash,
        binding_lock_hash: body.binding_lock_hash,
        flow_id: body.flow_id,
        run_id: body.run_id,
        node_id: body.node_id,
        node_alias: body.node_alias,
        activation_ordinal: body.activation_ordinal,
    };
    let proof = verified_pop_proof(&session, &exact_body);
    let binding_ref = lease_row
        .parent_ref
        .as_deref()
        .ok_or_else(|| worker_rust_error("lease parent"))?;
    let reply: V2AuthorityReply = authority_do(
        env,
        &session.org_id,
        binding_ref,
        &V2AuthorityCommand::DeriveGrant {
            canonical_authority_view: connection
                .canonical_authority_view_json
                .clone()
                .into_bytes(),
            material_generation: connection.active_material_generation,
            now: now_rfc3339(),
            scope,
            node_lease_ref: body.node_lease_ref.clone(),
            semantic_effect_slot: body.semantic_effect_slot,
            canonical_input,
            expected_cas_version: body.expected_cas_version,
            pop_proof: proof.clone(),
            expected_pop_proof: proof,
        },
    )
    .await?;
    store_host_artifact(
        &db,
        &session.org_id,
        &reply.artifact_ref,
        "execution_grant",
        &reply.canonical_artifact,
        &connection.connection_ref,
        &session.deployment_id,
        Some(&body.node_lease_ref),
        reply.cas_version,
    )
    .await?;
    let grant = parse::<ExecutionGrantV2>(&reply.canonical_artifact)
        .map_err(|_| worker_rust_error("grant"))?;
    json(
        &serde_json::json!({"grant_ref":reply.artifact_ref,"grant":grant.view.as_value(),"cas_version":reply.cas_version,"redelivery":reply.redelivery}),
        if reply.redelivery { 200 } else { 201 },
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InvokeRequestV2 {
    session_ref: String,
    grant_ref: String,
    input: Value,
}

pub async fn invoke(request: &mut Request, env: &Env) -> worker::Result<Response> {
    if !header_secret_matches(
        request,
        env,
        "x-lattice-service-auth",
        "INVOKE_SERVICE_AUTH",
    ) {
        return json(&PublicError::broker(BrokerError::Brk101), 401);
    }
    let exact_body = match bounded_body(request, MAX_INVOKE_BODY).await {
        Ok(v) => v,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let session = match authenticate(request, env, &exact_body).await {
        Ok(v) => v,
        Err(r) => return Ok(r),
    };
    let body: InvokeRequestV2 = match parse_json(&exact_body) {
        Ok(v) => v,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    if body.session_ref != session.session_ref {
        return json(&PublicError::broker(BrokerError::Brk102), 401);
    }
    let db = env.d1("BROKER_DB")?;
    let row =
        match load_host_record(&db, &session.org_id, &body.grant_ref, "execution_grant").await? {
            Some(v) if v.deployment_id == session.deployment_id => v,
            _ => return json(&PublicError::broker(BrokerError::Brk109), 403),
        };
    let grant = parse::<ExecutionGrantV2>(row.canonical_artifact_json.as_bytes())
        .map_err(|_| worker_rust_error("grant"))?;
    broker_core::credential::grant::require_invocable(&grant.view)
        .map_err(|_| worker_rust_error("grant"))?;
    if !artifact_time_is_current(grant.view.as_value(), &now_rfc3339()) {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    let canonical_input = broker_core::canonical::from_serde(
        &body.input,
        broker_core::canonical::MAX_OPERATION_BYTES,
    )
    .map_err(|_| worker_rust_error("canonical input"))?
    .into_bytes();
    if hash(&canonical_input) != hash_from_commitment_context(&grant, &canonical_input, env)? {
        return json(&PublicError::broker(BrokerError::Brk203), 409);
    }
    let effect = grant
        .view
        .as_value()
        .pointer("/grant_scope/logical_effect_id")
        .and_then(Value::as_str)
        .ok_or_else(|| worker_rust_error("grant"))?
        .to_owned();
    if let Some(existing) = load_outbox(&db, &session.org_id, &body.grant_ref, &effect).await? {
        if existing.canonical_input_hash != hash(&canonical_input) {
            return json(&PublicError::broker(BrokerError::Brk203), 409);
        }
        if let Some(receipt) = existing.canonical_receipt_json {
            let parsed = parse::<InvocationReceiptV2>(receipt.as_bytes())
                .map_err(|_| worker_rust_error("receipt"))?;
            return json(
                &serde_json::json!({"receipt_ref":format!("receipt_v2_{}",&hash(parsed.canonical_bytes())[7..39]),"receipt":parsed.view.as_value(),"redelivery":true,"response":existing.response_projection_json.and_then(|v|serde_json::from_str::<Value>(&v).ok())}),
                200,
            );
        }
        if matches!(existing.phase.as_str(), "dispatched" | "ambiguous") {
            return finish_ambiguous(&db, env, &session, &grant, &row, &canonical_input, &effect)
                .await;
        }
    } else {
        db.prepare("INSERT INTO v2_invocation_outbox(org_id,grant_ref,logical_effect_id,canonical_input_hash,phase,dispatch_attempt) VALUES(?,?,?,?,'prepared',0)").bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&body.grant_ref),JsValue::from_str(&effect),JsValue::from_str(&hash(&canonical_input))])?.run().await?;
    }
    execute_grant(&db, env, &session, &grant, &row, &canonical_input, &effect).await
}

#[derive(Deserialize)]
struct OutboxRow {
    canonical_input_hash: String,
    phase: String,
    canonical_receipt_json: Option<String>,
    response_projection_json: Option<String>,
}

async fn execute_grant(
    db: &D1Database,
    env: &Env,
    session: &SessionRow,
    grant: &broker_core::credential::ParsedV2<ExecutionGrantV2>,
    record: &HostRecordRow,
    canonical_input: &[u8],
    effect: &str,
) -> worker::Result<Response> {
    let connection = match load_connection(db, &session.org_id, &record.connection_ref).await? {
        Some(v) if v.status == "active" && v.registry_is_current(&now_rfc3339()) => v,
        _ => return json(&PublicError::broker(BrokerError::Brk106), 409),
    };
    let g = grant.view.as_value();
    let operation = g["operation_contract"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let input: Value = serde_json::from_slice(canonical_input)
        .map_err(|_| worker_rust_error("canonical input"))?;
    let installed = match installed_contract(&operation) {
        Ok(v) => v,
        Err(e) => return json(&PublicError::broker(e), 403),
    };
    if g["contract_hash"] != installed.contract_hash
        || g["authority_view_hash"] != connection.authority_view_hash_value()
        || g["authority_epoch"] != serde_json::json!(1)
        || g["minimum_material_generation"]
            .as_u64()
            .is_none_or(|v| v > connection.active_material_generation)
    {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    let descriptor: connector_spec::BrokerDispatchDescriptor =
        serde_json::from_slice(installed.descriptor)
            .map_err(|_| worker_rust_error("descriptor"))?;
    let authority_facts = crate::composition::authority_facts_for_input(&operation, &input)
        .map_err(|_| worker_rust_error("authority facts"))?;
    let registry_now = now_rfc3339();
    let registry = crate::composition::trusted_host_registry(&registry_now)
        .map_err(|_| worker_rust_error("registry"))?;
    let template = match broker_host::descriptor_plan_template(
        &descriptor,
        canonical_input,
        &authority_facts,
        &registry,
        installed.implementation_hash,
        &registry_now,
    ) {
        Ok(value) => value,
        Err(error) => {
            worker::console_error!("V2 broker plan rejected: {:?}", error);
            return json(&PublicError::broker(BrokerError::Brk109), 409);
        }
    };
    let plan_hash = hash(&template);
    let facts_hash = hash(&authority_facts);
    db.prepare("UPDATE v2_invocation_outbox SET phase='planned' WHERE org_id=? AND grant_ref=? AND logical_effect_id=? AND phase='prepared'").bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(g["grant_ref"].as_str().unwrap_or_default()),JsValue::from_str(effect)])?.run().await?;
    if !artifact_time_is_current(g, &now_rfc3339()) {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    #[derive(Deserialize)]
    struct PreDispatchAuthority {
        status: String,
        revocation_epoch: u64,
        authority_view_hash: String,
        active_material_generation: u64,
    }
    let live=db.prepare("SELECT status,revocation_epoch,authority_view_hash,active_material_generation FROM connections_v2 WHERE org_id=? AND connection_ref=?")
        .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?.first::<PreDispatchAuthority>(None).await?;
    if live.as_ref().is_none_or(|value| {
        value.status != "active"
            || value.revocation_epoch != 0
            || value.authority_view_hash != connection.authority_view_hash_value()
            || value.active_material_generation != connection.active_material_generation
    }) {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    db.prepare("UPDATE v2_invocation_outbox SET phase='dispatched',dispatch_attempt=1 WHERE org_id=? AND grant_ref=? AND logical_effect_id=? AND phase='planned'").bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(g["grant_ref"].as_str().unwrap_or_default()),JsValue::from_str(effect)])?.run().await?;
    let dispatch = if connection.profile_ref == "auth.google.workspace.oauth2" {
        let access = match lease_v2_access_token(
            env,
            &session.org_id,
            &connection.connection_ref,
            connection.active_material_generation,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => return json(&PublicError::broker(error), 503),
        };
        dispatch_provider(env, &template, &descriptor, &access, effect).await
    } else if connection.material_mode == "local_sealed" {
        let access = match lease_v2_access_token(
            env,
            &session.org_id,
            &connection.connection_ref,
            connection.active_material_generation,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => return json(&PublicError::broker(error), 503),
        };
        dispatch_generic_driver(
            db,
            env,
            &session.org_id,
            &connection,
            &template,
            &descriptor,
            Some(&access),
            effect,
            false,
        )
        .await
    } else if connection.material_mode == "remote_external" {
        dispatch_generic_driver(
            db,
            env,
            &session.org_id,
            &connection,
            &template,
            &descriptor,
            None,
            effect,
            true,
        )
        .await
    } else {
        Err(ProviderDispatchFailure::Definite)
    };
    let (projection, request_id, remote_durable_proven, outcome) = match dispatch {
        Ok((p, id, proven)) => (p, id, proven, "confirmed"),
        Err(ProviderDispatchFailure::Definite) => (vec![], None, false, "failed"),
        Err(ProviderDispatchFailure::Ambiguous) => {
            return finish_ambiguous(db, env, session, grant, record, canonical_input, effect)
                .await;
        }
    };
    finish_receipt(
        db,
        env,
        session,
        grant,
        record,
        canonical_input,
        effect,
        V2AttemptStage::Dispatch(1),
        Some((&plan_hash, &facts_hash)),
        projection,
        request_id,
        remote_durable_proven,
        outcome,
    )
    .await
}

async fn finish_ambiguous(
    db: &D1Database,
    env: &Env,
    session: &SessionRow,
    grant: &broker_core::credential::ParsedV2<ExecutionGrantV2>,
    record: &HostRecordRow,
    canonical_input: &[u8],
    effect: &str,
) -> worker::Result<Response> {
    db.prepare("UPDATE v2_invocation_outbox SET phase='ambiguous',dispatch_attempt=1 WHERE org_id=? AND grant_ref=? AND logical_effect_id=?").bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(grant.view.as_value()["grant_ref"].as_str().unwrap_or_default()),JsValue::from_str(effect)])?.run().await?;
    let operation = text(grant.view.as_value(), "operation_contract")?;
    let installed = installed_contract(operation).map_err(|_| worker_rust_error("contract"))?;
    let descriptor: connector_spec::BrokerDispatchDescriptor =
        serde_json::from_slice(installed.descriptor)
            .map_err(|_| worker_rust_error("descriptor"))?;
    let input: Value =
        serde_json::from_slice(canonical_input).map_err(|_| worker_rust_error("input"))?;
    let facts = crate::composition::authority_facts_for_input(operation, &input)
        .map_err(|_| worker_rust_error("authority facts"))?;
    let registry_now = now_rfc3339();
    let registry = crate::composition::trusted_host_registry(&registry_now)
        .map_err(|_| worker_rust_error("registry"))?;
    let template = broker_host::descriptor_plan_template(
        &descriptor,
        canonical_input,
        &facts,
        &registry,
        installed.implementation_hash,
        &registry_now,
    )
    .map_err(|_| worker_rust_error("plan"))?;
    let plan_hash = hash(&template);
    let facts_hash = hash(&facts);
    finish_receipt(
        db,
        env,
        session,
        grant,
        record,
        canonical_input,
        effect,
        V2AttemptStage::Dispatch(1),
        Some((&plan_hash, &facts_hash)),
        vec![],
        None,
        false,
        "ambiguous",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn finish_receipt(
    db: &D1Database,
    env: &Env,
    session: &SessionRow,
    grant: &broker_core::credential::ParsedV2<ExecutionGrantV2>,
    record: &HostRecordRow,
    _canonical_input: &[u8],
    effect: &str,
    stage: V2AttemptStage,
    plan: Option<(&str, &str)>,
    projection: Vec<u8>,
    request_id: Option<String>,
    remote_durable_proven: bool,
    outcome: &str,
) -> worker::Result<Response> {
    let connection = load_connection(db, &session.org_id, &record.connection_ref)
        .await?
        .ok_or_else(|| worker_rust_error("connection"))?;
    let installed = installed_contract(
        grant.view.as_value()["operation_contract"]
            .as_str()
            .unwrap_or_default(),
    )
    .map_err(|_| worker_rust_error("contract"))?;
    let deployed_binary_hash = env.var("BROKER_WORKER_WASM_SHA256")?.to_string();
    if !valid_hash(&deployed_binary_hash) {
        return Err(worker_rust_error("deployment binary hash"));
    }
    let implementation = |pin: &broker_auth::RegistryPin, binary: &str| serde_json::json!({"kind":"native_component","registry_entry":pin,"binary_hash":binary});
    let implementations = if connection.profile_ref == "auth.google.workspace.oauth2" {
        let pins = provider_profile_pins()?;
        let token_binary = env.var("GOOGLE_TOKEN_WORKER_SHA256")?.to_string();
        let provider_binary = env.var("GOOGLE_PROVIDER_WORKER_SHA256")?.to_string();
        if !valid_hash(&token_binary) || !valid_hash(&provider_binary) {
            return Err(worker_rust_error("egress binary"));
        }
        BTreeMap::from([
            (
                "planner".into(),
                implementation(&installed.planner, &deployed_binary_hash),
            ),
            (
                "projector".into(),
                implementation(&installed.projector, &deployed_binary_hash),
            ),
            ("auth_driver".into(), implementation(&pins.0, &token_binary)),
            ("custodian".into(), implementation(&pins.1, &token_binary)),
            (
                "transport".into(),
                implementation(&pins.2, &provider_binary),
            ),
            (
                "privileged_response_firewall".into(),
                implementation(&installed.response_firewall, &deployed_binary_hash),
            ),
        ])
    } else {
        #[derive(Deserialize)]
        struct ProfileEvidenceRow {
            canonical_profile_json: String,
            driver_config_json: String,
        }
        let row=db.prepare("SELECT canonical_profile_json,driver_config_json FROM generic_activation_intents_v2 WHERE org_id=? AND connection_ref=?").bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?.first::<ProfileEvidenceRow>(None).await?.ok_or_else(||worker_rust_error("profile evidence"))?;
        let profile: Value = serde_json::from_str(&row.canonical_profile_json)?;
        let config: Value = serde_json::from_str(&row.driver_config_json)?;
        let binary = env.var("AUTH_DRIVER_WORKER_SHA256")?.to_string();
        if !valid_hash(&binary) {
            return Err(worker_rust_error("driver binary"));
        }
        let driver: broker_auth::RegistryPin =
            serde_json::from_value(profile["trusted_auth_driver"].clone())?;
        let custodian: broker_auth::RegistryPin =
            serde_json::from_value(config["custodian"].clone())?;
        let transport: broker_auth::RegistryPin =
            serde_json::from_value(config["transport"].clone())?;
        BTreeMap::from([
            (
                "planner".into(),
                implementation(&installed.planner, &deployed_binary_hash),
            ),
            (
                "projector".into(),
                implementation(&installed.projector, &deployed_binary_hash),
            ),
            ("auth_driver".into(), implementation(&driver, &binary)),
            ("custodian".into(), implementation(&custodian, &binary)),
            ("transport".into(), implementation(&transport, &binary)),
            (
                "privileged_response_firewall".into(),
                implementation(&installed.response_firewall, &deployed_binary_hash),
            ),
        ])
    };
    let g = grant.view.as_value();
    let grant_hash = hash(grant.canonical_bytes());
    let ledger_transition_hash =
        hash(format!("{}\0{}\0{}", g["grant_ref"], effect, outcome).as_bytes());
    let budget_before = g
        .pointer("/derivation_evidence/parent_budget_before")
        .and_then(Value::as_u64)
        .ok_or_else(|| worker_rust_error("budget evidence"))?;
    let budget_after = g
        .pointer("/derivation_evidence/parent_budget_after")
        .and_then(Value::as_u64)
        .ok_or_else(|| worker_rust_error("budget evidence"))?;
    if budget_before.checked_sub(1) != Some(budget_after) {
        return Err(worker_rust_error("budget evidence"));
    }
    let assurance = serde_json::json!({"kind":"brokered_count","predicate_id":"brokered-count-v2","grant_hash":grant_hash,"authority_view_hash":g["authority_view_hash"],"authority_epoch":g["authority_epoch"],"ledger_transition_hash":ledger_transition_hash,"budget_before":budget_before,"budget_after":budget_after,"pop_transcript_hash":hash(session.pop_key_thumbprint.as_bytes()),"registry_decision_set_hash":g["registry_decision_set_hash"]});
    let input_commitment = g["canonical_input_commitment"].clone();
    let attempt = match stage {
        V2AttemptStage::Dispatch(value) => u64::from(value),
        V2AttemptStage::PrePlanning | V2AttemptStage::PostPlanningPreDispatch => 0,
    };
    let issuer = text(g, "issuer")?;
    let run_id = g
        .pointer("/subject/run_id")
        .and_then(Value::as_str)
        .ok_or_else(|| worker_rust_error("receipt context"))?;
    let node_id = g
        .pointer("/subject/node_id")
        .and_then(Value::as_str)
        .ok_or_else(|| worker_rust_error("receipt context"))?;
    let response_bytes = if projection.is_empty() {
        b"{}".as_slice()
    } else {
        projection.as_slice()
    };
    let response_commitment = receipt_commitment(
        env,
        &session.org_id,
        broker_core::credential::commitment::CommitmentContextV2::ResponseCommitment {
            issuer: issuer.into(),
            run_id: run_id.into(),
            node_id: node_id.into(),
            effect: effect.into(),
            attempt,
        },
        response_bytes,
        broker_core::credential::commitment::ValueEncoding::Jcs,
    )?;
    let connection_commitment = receipt_commitment(
        env,
        &session.org_id,
        broker_core::credential::commitment::CommitmentContextV2::ConnectionCommitment {
            issuer: issuer.into(),
            run_id: run_id.into(),
            node_id: node_id.into(),
            effect: effect.into(),
            attempt,
        },
        record.connection_ref.as_bytes(),
        broker_core::credential::commitment::ValueEncoding::OpaqueBytes,
    )?;
    let firewall_evidence=broker_core::canonical::from_serde(&serde_json::json!({"registry_decision":installed.response_firewall,"bounded_response_hash":hash(response_bytes),"projection_hash":hash(&projection),"decision":"approved_and_scrubbed"}),64*1024).map_err(|_|worker_rust_error("firewall evidence"))?;
    let firewall_hash = hash(firewall_evidence.as_bytes());
    let (plan_hash, facts_hash) = plan.ok_or_else(|| worker_rust_error("receipt evidence"))?;
    let dispatch_observed = matches!(stage, V2AttemptStage::Dispatch(_));
    let signer =
        BrokerSigner::from_seed("broker-v2-receipt", secret_32(env, "RECEIPT_SIGNING_SEED")?);
    let receipt=issue_v2_receipt(V2ReceiptIssue{evidence:V2DispatchEvidence{grant:grant.clone(),canonical_grant:grant.canonical_bytes().to_vec(),canonical_input_commitment:input_commitment,implementations,evaluator_implementations:BTreeMap::new(),evaluator_output_hashes:vec![],claims:serde_json::json!({"trusted_host_scope_authenticated":true,"broker_admission_enforced":true,"provider_dispatch_observed":dispatch_observed,"remote_durable_state_proven":remote_durable_proven,"verifiable_execution_proven":false}),attempt_stage:stage,exact_material_generation:Some(connection.active_material_generation)},connection_commitment,request_plan_hash:Value::String(plan_hash.into()),authority_facts_hash:Value::String(facts_hash.into()),response_firewall_evidence_hash:Value::String(firewall_hash),budget_before,budget_after,provider_request_id:request_id,response_commitment,outcome:outcome.into(),assurance_evidence:vec![assurance],issued_at:now_rfc3339()},&signer).map_err(|error| { worker::console_error!("receipt issue rejected: {:?}", error); worker_rust_error("receipt") })?;
    let receipt_json = std::str::from_utf8(&receipt.canonical_receipt)
        .map_err(|_| worker_rust_error("receipt"))?;
    let projection_json = if projection.is_empty() {
        None
    } else {
        Some(std::str::from_utf8(&projection).map_err(|_| worker_rust_error("projection"))?)
    };
    db.prepare("UPDATE v2_invocation_outbox SET phase='terminal',canonical_receipt_json=?,response_projection_json=? WHERE org_id=? AND grant_ref=? AND logical_effect_id=?").bind(&[JsValue::from_str(receipt_json),projection_json.map(JsValue::from_str).unwrap_or(JsValue::NULL),JsValue::from_str(&session.org_id),JsValue::from_str(g["grant_ref"].as_str().unwrap_or_default()),JsValue::from_str(effect)])?.run().await?;
    let receipt_ref = format!("receipt_v2_{}", &hash(receipt_json.as_bytes())[7..39]);
    store_host_artifact(
        db,
        &session.org_id,
        &receipt_ref,
        "invocation_receipt",
        &receipt.canonical_receipt,
        &record.connection_ref,
        &session.deployment_id,
        Some(g["grant_ref"].as_str().unwrap_or_default()),
        0,
    )
    .await?;
    json(
        &serde_json::json!({"receipt_ref":receipt_ref,"receipt":receipt.receipt.view.as_value(),"redelivery":false,"response":projection_json.and_then(|v|serde_json::from_str::<Value>(v).ok())}),
        200,
    )
}

fn provider_profile_pins() -> worker::Result<(
    broker_auth::RegistryPin,
    broker_auth::RegistryPin,
    broker_auth::RegistryPin,
)> {
    let profile = crate::composition::installed_profile_pins(&now_rfc3339())
        .map_err(|_| worker_rust_error("composition"))?;
    Ok((profile.auth_driver, profile.custodian, profile.transport))
}

fn lock_from_binding(
    binding: &broker_core::credential::ParsedV2<BindingAttestationV2>,
) -> worker::Result<SerializableBindingLock> {
    let b = binding.view.as_value();
    Ok(SerializableBindingLock {
        org_id: text(b, "org_id")?.into(),
        principal: text(b, "principal")?.into(),
        issuer: text(b, "issuer")?.into(),
        broker_key_id: text(b, "broker_key_id")?.into(),
        deployment_id: text(b, "deployment_id")?.into(),
        authority_manifest_hash: text(b, "authority_manifest_hash")?.into(),
        standing_authority_ref: text(b, "standing_authority_ref")?.into(),
        standing_authority_hash: text(b, "standing_authority_hash")?.into(),
        contract_set_ref: text(b, "contract_set_ref")?.into(),
        contract_set_hash: text(b, "contract_set_hash")?.into(),
        connection_ref: text(b, "connection_ref")?.into(),
        auth_profile_ref_json: b["auth_profile_ref"].clone(),
        execution_lane: text(b, "execution_lane")?.into(),
        custody_location: text(b, "custody_location")?.into(),
    })
}
fn text<'a>(v: &'a Value, f: &str) -> worker::Result<&'a str> {
    v.get(f)
        .and_then(Value::as_str)
        .ok_or_else(|| worker_rust_error("v2 artifact"))
}
fn parse_value<T: broker_core::credential::SchemaType + serde::de::DeserializeOwned>(
    value: &Value,
) -> Result<broker_core::credential::ParsedV2<T>, BrokerError> {
    let c = broker_core::canonical::from_serde(value, T::MAX_BYTES)?;
    parse(c.as_bytes())
}
fn host_error(error: broker_host::BrokerHostError) -> BrokerError {
    match error {
        broker_host::BrokerHostError::Broker(e) => e,
        broker_host::BrokerHostError::V2AuthorityDrift => BrokerError::Brk106,
        _ => BrokerError::Brk109,
    }
}
fn verified_pop_proof(session: &SessionRow, body: &[u8]) -> Vec<u8> {
    Sha256::digest(
        [
            b"lattice.v2.verified-pop".as_slice(),
            session.session_ref.as_bytes(),
            session.pop_key_thumbprint.as_bytes(),
            &Sha256::digest(body),
        ]
        .concat(),
    )
    .to_vec()
}
fn hash_from_commitment_context(
    grant: &broker_core::credential::ParsedV2<ExecutionGrantV2>,
    input: &[u8],
    env: &Env,
) -> worker::Result<String> {
    use broker_core::credential::commitment::{CommitmentContextV2, ValueEncoding, commit_v2};
    let g = grant.view.as_value();
    let key = CommitmentKey::new("broker-v2-input", secret_32(env, "COMMITMENT_KEY")?)
        .map_err(|_| worker_rust_error("commitment"))?;
    let (c, _) = commit_v2(
        &key,
        text(g, "org_id")?,
        &CommitmentContextV2::GrantCanonicalInput {
            issuer: text(g, "issuer")?.into(),
            grant_ref: text(g, "grant_ref")?.into(),
            effect: g
                .pointer("/grant_scope/logical_effect_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .into(),
        },
        input,
        ValueEncoding::Jcs,
        broker_core::artifacts::VerificationTier::BrokerOnly,
    )
    .map_err(|_| worker_rust_error("commitment"))?;
    let expected = serde_json::to_value(c).map_err(|_| worker_rust_error("commitment"))?;
    if expected == g["canonical_input_commitment"] {
        Ok(hash(input))
    } else {
        Ok("mismatch".into())
    }
}
fn receipt_commitment(
    env: &Env,
    org_id: &str,
    context: broker_core::credential::commitment::CommitmentContextV2,
    value: &[u8],
    encoding: broker_core::credential::commitment::ValueEncoding,
) -> worker::Result<Value> {
    let key = CommitmentKey::new("broker-v2-input", secret_32(env, "COMMITMENT_KEY")?)
        .map_err(|_| worker_rust_error("commitment"))?;
    let (commitment, _) = broker_core::credential::commitment::commit_v2(
        &key,
        org_id,
        &context,
        value,
        encoding,
        broker_core::artifacts::VerificationTier::BrokerOnly,
    )
    .map_err(|_| worker_rust_error("commitment"))?;
    serde_json::to_value(commitment).map_err(|_| worker_rust_error("commitment"))
}

fn commitment_value(env: &Env, purpose: &str, bytes: &[u8]) -> worker::Result<Value> {
    let value = format!(
        "hmac-sha256:{}",
        hex::encode(keyed_hash(
            &secret_32(env, "COMMITMENT_KEY")?,
            purpose.as_bytes(),
            bytes
        ))
    );
    Ok(
        serde_json::json!({"alg":"hmac-sha256","key_id":"broker-v2-commitment","verification_tier":"broker_only","value":value}),
    )
}

impl ConnectionV2Row {
    fn registry_is_current(&self, now: &str) -> bool {
        crate::composition::installed_provider_plane(now)
            .ok()
            .and_then(|plane| {
                serde_json::from_str::<Value>(&self.canonical_authority_view_json)
                    .ok()
                    .and_then(|view| {
                        view.get("registry_decision_set_hash")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .map(|stored| stored == plane.registry_decision_set_hash)
            })
            .unwrap_or(false)
    }

    fn authority_view_hash_value(&self) -> Value {
        Value::String(hash(self.canonical_authority_view_json.as_bytes()))
    }
}
async fn load_connection(
    db: &D1Database,
    org: &str,
    connection: &str,
) -> worker::Result<Option<ConnectionV2Row>> {
    db.prepare("SELECT connection_ref,profile_ref,profile_version,canonical_authority_view_json,standing_authority_ref,standing_authority_hash,canonical_standing_authority_json,contract_set_ref,contract_set_hash,canonical_contract_set_json,active_material_generation,material_mode,revocation_epoch,status FROM connections_v2 WHERE org_id=? AND connection_ref=?").bind(&[JsValue::from_str(org),JsValue::from_str(connection)])?.first(None).await
}
async fn load_host_record(
    db: &D1Database,
    org: &str,
    reference: &str,
    kind: &str,
) -> worker::Result<Option<HostRecordRow>> {
    db.prepare("SELECT canonical_artifact_json,connection_ref,deployment_id,parent_ref,cas_version FROM v2_host_records WHERE org_id=? AND artifact_ref=? AND artifact_kind=?").bind(&[JsValue::from_str(org),JsValue::from_str(reference),JsValue::from_str(kind)])?.first(None).await
}
async fn store_host_artifact(
    db: &D1Database,
    org: &str,
    reference: &str,
    kind: &str,
    canonical: &[u8],
    connection: &str,
    deployment: &str,
    parent: Option<&str>,
    cas: u64,
) -> worker::Result<()> {
    let parent = parent.map(JsValue::from_str).unwrap_or(JsValue::NULL);
    let result=db.prepare("INSERT OR IGNORE INTO v2_host_records(org_id,artifact_ref,artifact_kind,artifact_hash,canonical_artifact_json,connection_ref,deployment_id,parent_ref,cas_version,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)").bind(&[JsValue::from_str(org),JsValue::from_str(reference),JsValue::from_str(kind),JsValue::from_str(&hash(canonical)),JsValue::from_str(std::str::from_utf8(canonical).map_err(|_|worker_rust_error("artifact"))?),JsValue::from_str(connection),JsValue::from_str(deployment),parent,JsValue::from_f64(cas as f64),JsValue::from_f64(now_seconds() as f64)])?.run().await?;
    if !result.success() {
        return Err(worker_rust_error("artifact"));
    }
    Ok(())
}
async fn credential_do<T: DeserializeOwned>(
    env: &Env,
    org: &str,
    connection: &str,
    envelope: &CredentialStateEnvelope,
) -> worker::Result<T> {
    let route = hash(format!("lattice.credential-state.v2\0{org}\0{connection}").as_bytes());
    do_request(env, "CREDENTIAL_STATE_V2_DO", &route, envelope).await
}

async fn dispatch_generic_driver(
    db: &D1Database,
    env: &Env,
    org_id: &str,
    connection: &ConnectionV2Row,
    template: &[u8],
    descriptor: &connector_spec::BrokerDispatchDescriptor,
    credential: Option<&crate::refresh::SecretBytes>,
    effect: &str,
    remote: bool,
) -> Result<(Vec<u8>, Option<String>, bool), ProviderDispatchFailure> {
    #[derive(Deserialize)]
    struct DriverRow {
        canonical_profile_json: String,
        driver_config_json: String,
        remote_handle: Option<String>,
    }
    let row=db.prepare("SELECT canonical_profile_json,driver_config_json,remote_handle FROM generic_activation_intents_v2 WHERE org_id=? AND connection_ref=? AND status='active'")
        .bind(&[JsValue::from_str(org_id),JsValue::from_str(&connection.connection_ref)])
        .map_err(|_|ProviderDispatchFailure::Ambiguous)?.first::<DriverRow>(None).await
        .map_err(|_|ProviderDispatchFailure::Ambiguous)?.ok_or(ProviderDispatchFailure::Definite)?;
    if remote != row.remote_handle.is_some() || remote == credential.is_some() {
        return Err(ProviderDispatchFailure::Definite);
    }
    let plan: Value =
        serde_json::from_slice(template).map_err(|_| ProviderDispatchFailure::Definite)?;
    let body = serde_json::json!({"connection_ref":connection.connection_ref,"profile":serde_json::from_str::<Value>(&row.canonical_profile_json).map_err(|_|ProviderDispatchFailure::Definite)?,"driver_config":serde_json::from_str::<Value>(&row.driver_config_json).map_err(|_|ProviderDispatchFailure::Definite)?,"credential_b64u":credential.map(|value|String::from_utf8_lossy(value.expose_to_internal_binding()).into_owned()),"remote_handle":row.remote_handle,"plan":plan});
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    init.with_body(Some(
        JsString::from(
            serde_json::to_string(&body).map_err(|_| ProviderDispatchFailure::Definite)?,
        )
        .into(),
    ));
    let path = if remote {
        "/authorize-and-dispatch"
    } else {
        "/dispatch"
    };
    let mut request = Request::new_with_init(&format!("http://auth-driver.internal{path}"), &init)
        .map_err(|_| ProviderDispatchFailure::Ambiguous)?;
    request
        .headers_mut()
        .map_err(|_| ProviderDispatchFailure::Ambiguous)?
        .set("content-type", JSON_CONTENT_TYPE)
        .map_err(|_| ProviderDispatchFailure::Ambiguous)?;
    request
        .headers_mut()
        .map_err(|_| ProviderDispatchFailure::Ambiguous)?
        .set(
            "x-lattice-auth-driver-service-auth",
            &env.secret("AUTH_DRIVER_SERVICE_AUTH")
                .map_err(|_| ProviderDispatchFailure::Ambiguous)?
                .to_string(),
        )
        .map_err(|_| ProviderDispatchFailure::Ambiguous)?;
    request
        .headers_mut()
        .map_err(|_| ProviderDispatchFailure::Ambiguous)?
        .set("x-lattice-correlation-id", effect)
        .map_err(|_| ProviderDispatchFailure::Ambiguous)?;
    let mut response = env
        .service("AUTH_DRIVER_SERVICE")
        .map_err(|_| ProviderDispatchFailure::Ambiguous)?
        .fetch_request(request)
        .await
        .map_err(|_| ProviderDispatchFailure::Ambiguous)?;
    if !(200..300).contains(&response.status_code()) {
        return Err(if (400..500).contains(&response.status_code()) {
            ProviderDispatchFailure::Definite
        } else {
            ProviderDispatchFailure::Ambiguous
        });
    }
    let request_id = response
        .headers()
        .get("x-request-id")
        .map_err(|_| ProviderDispatchFailure::Ambiguous)?
        .filter(|value| value.len() <= 128 && value.is_ascii());
    let remote_proof = response
        .headers()
        .get("x-lattice-remote-dispatch-proof")
        .map_err(|_| ProviderDispatchFailure::Ambiguous)?;
    if remote_proof.is_none() {
        return Err(ProviderDispatchFailure::Ambiguous);
    }
    let durable_proven = remote_proof.is_some();
    let bytes = bounded_response_bytes(&mut response, crate::protocol::MAX_PROVIDER_RESPONSE)
        .await
        .map_err(|_| ProviderDispatchFailure::Ambiguous)?;
    let projection = broker_host::descriptor_response_projection(descriptor, &bytes)
        .map_err(|_| ProviderDispatchFailure::Definite)?;
    Ok((projection, request_id, durable_proven))
}

async fn lease_v2_access_token(
    env: &Env,
    org: &str,
    connection: &str,
    generation: u64,
) -> Result<crate::refresh::SecretBytes, BrokerError> {
    let envelope = CredentialStateEnvelope {
        org_id: org.into(),
        connection_ref: connection.into(),
        command: CredentialStateCommand::LeaseMaterialForDispatch { generation },
    };
    let reply: CredentialStateReply = credential_do(env, org, connection, &envelope)
        .await
        .map_err(|error| {
            worker::console_error!("credential DO lease error: {:?}", error);
            BrokerError::Brk401
        })?;
    let sealed = reply.leased_material.ok_or(BrokerError::Brk103)?;
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SealedEnvelope {
        nonce: String,
        ciphertext: String,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Material {
        connection_ref: String,
        route: String,
        account_commitment: String,
        scopes: Vec<String>,
        refresh_token: String,
        access_token: String,
        access_expires_at: i64,
    }
    let sealed: SealedEnvelope =
        serde_json::from_slice(&sealed).map_err(|_| BrokerError::Brk401)?;
    let plaintext = open_activation_payload(
        env,
        &format!("{connection}\0v2-material"),
        &sealed.nonce,
        &sealed.ciphertext,
    )
    .map_err(|_| BrokerError::Brk401)?;
    let material: Material = serde_json::from_slice(&plaintext).map_err(|_| BrokerError::Brk401)?;
    if material.connection_ref != connection
        || material.access_token.is_empty()
        || material.refresh_token.is_empty()
        || material.account_commitment.is_empty()
        || material.scopes.is_empty()
        || material.route.is_empty()
        || material.access_expires_at <= now_seconds()
    {
        return Err(BrokerError::Brk106);
    }
    Ok(crate::refresh::SecretBytes::new(
        material.access_token.into_bytes(),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SignedGenericProfile {
    descriptor: Value,
    driver_config: Value,
    org_id: String,
    deployment_id: String,
    signature_b64u: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GenericActivationCreate {
    operator_id: String,
    request_jti: String,
    contract_ids: Vec<String>,
    signed_profile: SignedGenericProfile,
}
#[derive(Deserialize)]
struct GenericActivationRow {
    activation_ref: String,
    deployment_id: String,
    profile_ref: String,
    profile_version: String,
    canonical_profile_json: String,
    driver_config_json: String,
    activation_kind: String,
    expected_claims_json: String,
    channel_ref_hash: Option<String>,
    challenge_hash: Option<String>,
    workload_nonce_hash: Option<String>,
    expires_at: i64,
    status: String,
}
fn verify_generic_profile(
    env: &Env,
    signed: &SignedGenericProfile,
    org_id: &str,
    deployment_id: &str,
) -> Result<(Vec<u8>, Vec<u8>, String, String, String), BrokerError> {
    if signed.org_id != org_id || signed.deployment_id != deployment_id {
        return Err(BrokerError::Brk107);
    }
    let canonical = broker_core::canonical::from_serde(&signed.descriptor, 256 * 1024)?;
    let driver_config = broker_core::canonical::from_serde(&signed.driver_config, 64 * 1024)?;
    let signed_preimage = broker_core::canonical::from_serde(
        &serde_json::json!({
            "deployment_id": signed.deployment_id,
            "descriptor": signed.descriptor,
            "driver_config": signed.driver_config,
            "org_id": signed.org_id
        }),
        384 * 1024,
    )?;
    let parsed =
        parse::<broker_core::credential::profile::AuthProfileDescriptorV2>(canonical.as_bytes())?;
    let bytes = URL_SAFE_NO_PAD
        .decode(
            env.var("GENERIC_PROFILE_AUTHORITY_PUBLIC_KEY_B64U")
                .map_err(|_| BrokerError::Brk106)?
                .to_string(),
        )
        .map_err(|_| BrokerError::Brk106)?;
    let key = VerifyingKey::from_bytes(&bytes.try_into().map_err(|_| BrokerError::Brk106)?)
        .map_err(|_| BrokerError::Brk106)?;
    let signature = Signature::from_slice(
        &URL_SAFE_NO_PAD
            .decode(&signed.signature_b64u)
            .map_err(|_| BrokerError::Brk106)?,
    )
    .map_err(|_| BrokerError::Brk106)?;
    key.verify(signed_preimage.as_bytes(), &signature)
        .map_err(|_| BrokerError::Brk106)?;
    let value = parsed.view.as_value();
    let profile = text(value, "profile_ref")
        .map_err(|_| BrokerError::Brk004)?
        .to_owned();
    let version = text(value, "version")
        .map_err(|_| BrokerError::Brk004)?
        .to_owned();
    let kind = value
        .pointer("/activation_kind/kind")
        .and_then(Value::as_str)
        .ok_or(BrokerError::Brk004)?
        .to_owned();
    if !matches!(
        kind.as_str(),
        "secret_submission" | "workload_binding" | "external_custodian_binding"
    ) {
        return Err(BrokerError::Brk004);
    }
    let endpoint = signed
        .driver_config
        .get("endpoint")
        .and_then(Value::as_str)
        .ok_or(BrokerError::Brk302)?;
    let endpoint = worker::Url::parse(endpoint).map_err(|_| BrokerError::Brk302)?;
    if endpoint.scheme() != "https"
        || endpoint.host_str().is_none()
        || endpoint.username() != ""
        || endpoint.password().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(BrokerError::Brk302);
    }
    Ok((
        parsed.canonical_bytes().to_vec(),
        driver_config.as_bytes().to_vec(),
        profile,
        version,
        kind,
    ))
}
fn activation_private(request: &Request, env: &Env) -> bool {
    header_secret_matches(
        request,
        env,
        "x-lattice-activation-auth",
        "ACTIVATION_SERVICE_AUTH",
    )
}
pub async fn create_generic_activation(
    request: &mut Request,
    env: &Env,
) -> worker::Result<Response> {
    if !activation_private(request, env) {
        return json(&PublicError::broker(BrokerError::Brk101), 401);
    }
    let exact = match bounded_body(request, MAX_MANAGEMENT_BODY).await {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let session = match authenticate(request, env, &exact).await {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    let body: GenericActivationCreate = match parse_json(&exact) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    if body.operator_id.is_empty() || body.request_jti.is_empty() || body.contract_ids.is_empty() {
        return json(&PublicError::invalid(), 400);
    }
    let (descriptor, driver_config, profile_ref, version, kind) = match verify_generic_profile(
        env,
        &body.signed_profile,
        &session.org_id,
        &session.deployment_id,
    ) {
        Ok(value) => value,
        Err(error) => return json(&PublicError::broker(error), 409),
    };
    let plane = crate::composition::installed_provider_plane(&now_rfc3339())
        .map_err(|_| worker_rust_error("composition"))?;
    let (_, deployment_contracts) = match load_deployment_authority(
        env,
        &session.org_id,
        &session.deployment_id,
        &plane.management.connector_ref,
    ) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::broker(BrokerError::Brk108), 403),
    };
    let authorized = deployment_contracts.view.as_value()["contracts"]
        .as_array()
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value["contract_id"].as_str().map(str::to_owned))
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let expected = body
        .contract_ids
        .iter()
        .map(|id| {
            if authorized.contains(id) {
                installed_contract(id).map(|_| id.clone())
            } else {
                Err(BrokerError::Brk108)
            }
        })
        .collect::<Result<BTreeSet<_>, _>>();
    let expected = match expected {
        Ok(value) if !value.is_empty() => value,
        _ => return json(&PublicError::broker(BrokerError::Brk108), 403),
    };
    let activation_ref = opaque_id("activation_v2_")?;
    let expires = now_seconds() + 600;
    let channel = opaque_id("private_channel_")?;
    let challenge = opaque_id("custodian_challenge_")?;
    let nonce = opaque_id("workload_nonce_")?;
    let profile_json =
        std::str::from_utf8(&descriptor).map_err(|_| worker_rust_error("profile"))?;
    let claims = broker_core::canonical::from_serde(&expected, 64 * 1024)
        .map_err(|_| worker_rust_error("claims"))?;
    let db = env.d1("BROKER_DB")?;
    let inserted=db.prepare("INSERT INTO generic_activation_intents_v2(org_id,activation_ref,deployment_id,operator_id,request_jti,profile_ref,profile_version,profile_hash,canonical_profile_json,driver_config_json,activation_kind,expected_claims_json,channel_ref_hash,challenge_hash,workload_nonce_hash,expires_at,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'awaiting_action')")
      .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&activation_ref),JsValue::from_str(&session.deployment_id),JsValue::from_str(&body.operator_id),JsValue::from_str(&body.request_jti),JsValue::from_str(&profile_ref),JsValue::from_str(&version),JsValue::from_str(&hash(&descriptor)),JsValue::from_str(profile_json),JsValue::from_str(std::str::from_utf8(&driver_config).map_err(|_|worker_rust_error("driver config"))?),JsValue::from_str(&kind),JsValue::from_str(std::str::from_utf8(claims.as_bytes()).map_err(|_|worker_rust_error("claims"))?),JsValue::from_str(&hash(channel.as_bytes())),JsValue::from_str(&hash(challenge.as_bytes())),JsValue::from_str(&hash(nonce.as_bytes())),JsValue::from_f64(expires as f64)])?.run().await;
    if inserted.is_err() {
        return json(&PublicError::broker(BrokerError::Brk203), 409);
    }
    let recipient_key_id = env.var("GENERIC_ACTIVATION_RECIPIENT_KEY_ID")?.to_string();
    let recipient_public_key_b64u = env
        .var("GENERIC_ACTIVATION_RECIPIENT_PUBLIC_KEY_B64U")?
        .to_string();
    let action = match kind.as_str() {
        "secret_submission" => {
            serde_json::json!({"kind":"submit_private_material","activation_ref":activation_ref,"expires_at":expires,"channel_ref":channel,"recipient_key_id":recipient_key_id,"recipient_public_key_b64u":recipient_public_key_b64u,"hpke_suite":"DHKEM(X25519,HKDF-SHA256)/HKDF-SHA256/AES-256-GCM","aad_domain":"lattice.generic-activation.hpke.v1","service_identity":"lattice-broker-private.generic-activation","submission_schema_hash":body.signed_profile.descriptor.pointer("/activation_kind/submission_schema_hash")})
        }
        "workload_binding" => {
            serde_json::json!({"kind":"present_workload_assertion","activation_ref":activation_ref,"expires_at":expires,"channel_ref":channel,"recipient_key_id":recipient_key_id,"recipient_public_key_b64u":recipient_public_key_b64u,"hpke_suite":"DHKEM(X25519,HKDF-SHA256)/HKDF-SHA256/AES-256-GCM","aad_domain":"lattice.generic-activation.hpke.v1","service_identity":"lattice-broker-private.generic-activation","nonce":nonce,"assertion_schema_hash":body.signed_profile.descriptor.pointer("/activation_kind/assertion_schema_hash")})
        }
        _ => {
            serde_json::json!({"kind":"bind_external_custodian","activation_ref":activation_ref,"expires_at":expires,"challenge":challenge,"recipient_key_id":recipient_key_id,"recipient_public_key_b64u":recipient_public_key_b64u,"hpke_suite":"DHKEM(X25519,HKDF-SHA256)/HKDF-SHA256/AES-256-GCM","aad_domain":"lattice.generic-activation.hpke.v1","service_identity":"lattice-broker-private.generic-activation"})
        }
    };
    json(&action, 201)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GenericActivationEnvelope {
    request_jti: String,
    issued_at: i64,
    expires_at: i64,
    recipient_key_id: String,
    encapsulated_key_b64u: String,
    ciphertext_b64u: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GenericActivationSubmission {
    channel_ref: Option<String>,
    challenge: Option<String>,
    material_b64u: Option<String>,
    assertion_b64u: Option<String>,
    issuer: Option<String>,
    audience: Option<String>,
    nonce: Option<String>,
    issued_at: Option<i64>,
    custodian_ref: Option<String>,
    remote_proof: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DriverActivationReply {
    material_b64u: Option<String>,
    account_subject: String,
    claims: Vec<String>,
    remote_proof: Option<String>,
}
fn execute_generic_activation_driver(
    profile: &Value,
    submission: &GenericActivationSubmission,
    expected_claims_json: &str,
) -> Result<DriverActivationReply, BrokerError> {
    let scheme = profile
        .get("scheme_config")
        .and_then(Value::as_object)
        .ok_or(BrokerError::Brk004)?;
    let kind = scheme
        .get("kind")
        .and_then(Value::as_str)
        .ok_or(BrokerError::Brk004)?;
    let claims: Vec<String> =
        serde_json::from_str(expected_claims_json).map_err(|_| BrokerError::Brk004)?;
    let (material, remote) = match kind {
        "api_key_header"
            if scheme.get("redact_authenticated_header") == Some(&Value::Bool(true)) =>
        {
            (
                submission
                    .material_b64u
                    .clone()
                    .ok_or(BrokerError::Brk109)?,
                None,
            )
        }
        "api_key_query"
            if scheme.get("encoding").and_then(Value::as_str) == Some("rfc3986")
                && scheme.get("redact_authenticated_query") == Some(&Value::Bool(true)) =>
        {
            (
                submission
                    .material_b64u
                    .clone()
                    .ok_or(BrokerError::Brk109)?,
                None,
            )
        }
        "generic_bearer"
            if scheme.get("header_name").and_then(Value::as_str) == Some("Authorization") =>
        {
            (
                submission
                    .material_b64u
                    .clone()
                    .ok_or(BrokerError::Brk109)?,
                None,
            )
        }
        "http_basic"
            if scheme.get("header_name").and_then(Value::as_str) == Some("Authorization")
                && scheme.get("colon_rule").and_then(Value::as_str)
                    == Some("username_forbids_colon") =>
        {
            (
                submission
                    .material_b64u
                    .clone()
                    .ok_or(BrokerError::Brk109)?,
                None,
            )
        }
        "oauth_token_exchange_workload_oidc"
            if submission.audience.as_deref() == scheme.get("audience").and_then(Value::as_str)
                && submission
                    .issuer
                    .as_deref()
                    .is_some_and(|value| !value.is_empty()) =>
        {
            (
                submission
                    .assertion_b64u
                    .clone()
                    .ok_or(BrokerError::Brk109)?,
                None,
            )
        }
        "external_custodian_reference" => {
            let custodian = submission
                .custodian_ref
                .as_deref()
                .ok_or(BrokerError::Brk109)?;
            let allowed = scheme
                .get("allowed_custodians")
                .and_then(Value::as_array)
                .is_some_and(|values| {
                    values.iter().any(|value| {
                        value.get("entry_ref").and_then(Value::as_str) == Some(custodian)
                    })
                });
            if !allowed {
                return Err(BrokerError::Brk109);
            }
            let proof = submission.remote_proof.clone().ok_or(BrokerError::Brk109)?;
            (proof.clone(), Some(proof))
        }
        _ => return Err(BrokerError::Brk004),
    };
    URL_SAFE_NO_PAD
        .decode(&material)
        .map_err(|_| BrokerError::Brk109)?;
    Ok(DriverActivationReply {
        account_subject: hash(format!("generic-principal\0{kind}\0{material}").as_bytes()),
        material_b64u: if remote.is_some() {
            None
        } else {
            Some(material)
        },
        remote_proof: remote,
        claims,
    })
}
pub async fn submit_generic_activation(
    request: &mut Request,
    env: &Env,
    activation_ref: &str,
) -> worker::Result<Response> {
    if !activation_private(request, env) {
        return json(&PublicError::broker(BrokerError::Brk101), 401);
    }
    let exact = match bounded_body(request, MAX_MANAGEMENT_BODY).await {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let session = match authenticate(request, env, &exact).await {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    let envelope: GenericActivationEnvelope = match parse_json(&exact) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let db = env.d1("BROKER_DB")?;
    let Some(row)=db.prepare("SELECT activation_ref,deployment_id,profile_ref,profile_version,canonical_profile_json,driver_config_json,activation_kind,expected_claims_json,channel_ref_hash,challenge_hash,workload_nonce_hash,expires_at,status FROM generic_activation_intents_v2 WHERE org_id=? AND activation_ref=?")
      .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(activation_ref)])?.first::<GenericActivationRow>(None).await? else{return json(&PublicError::broker(BrokerError::Brk109),404)};
    if row.deployment_id != session.deployment_id
        || row.status != "awaiting_action"
        || row.expires_at <= now_seconds()
    {
        return json(&PublicError::broker(BrokerError::Brk203), 409);
    }
    if envelope.recipient_key_id != env.var("GENERIC_ACTIVATION_RECIPIENT_KEY_ID")?.to_string()
        || envelope.expires_at != row.expires_at
        || envelope.issued_at > now_seconds()
        || envelope.expires_at <= now_seconds()
        || envelope.expires_at - envelope.issued_at > 600
        || envelope.request_jti.len() < 8
        || envelope.request_jti.len() > 128
    {
        return json(&PublicError::broker(BrokerError::Brk109), 409);
    }
    let correlation_hash = match row.activation_kind.as_str() {
        "secret_submission" | "workload_binding" => row.channel_ref_hash.as_deref(),
        "external_custodian_binding" => row.challenge_hash.as_deref(),
        _ => None,
    }
    .ok_or_else(|| worker_rust_error("activation correlation"))?;
    let aad = broker_core::canonical::from_serde(
        &serde_json::json!({
            "activation_ref":activation_ref,
            "correlation_hash":correlation_hash,
            "deployment_id":session.deployment_id,
            "domain":"lattice.generic-activation.hpke.v1",
            "expires_at":envelope.expires_at,
            "issued_at":envelope.issued_at,
            "org_id":session.org_id,
            "recipient_key_id":envelope.recipient_key_id,
            "request_jti":envelope.request_jti,
            "service_identity":"lattice-broker-private.generic-activation"
        }),
        16 * 1024,
    )
    .map_err(|_| worker_rust_error("activation aad"))?;
    let private_key = URL_SAFE_NO_PAD
        .decode(
            env.secret("GENERIC_ACTIVATION_RECIPIENT_PRIVATE_KEY_B64U")?
                .to_string(),
        )
        .map_err(|_| worker_rust_error("activation recipient key"))?;
    let private_key: [u8; 32] = private_key
        .try_into()
        .map_err(|_| worker_rust_error("activation recipient key"))?;
    let public_key = crate::hpke::public_key(&private_key);
    let configured_public = URL_SAFE_NO_PAD
        .decode(
            env.var("GENERIC_ACTIVATION_RECIPIENT_PUBLIC_KEY_B64U")?
                .to_string(),
        )
        .map_err(|_| worker_rust_error("activation recipient key"))?;
    if configured_public.as_slice() != public_key {
        return json(&PublicError::broker(BrokerError::Brk109), 409);
    }
    let encapsulated = URL_SAFE_NO_PAD
        .decode(&envelope.encapsulated_key_b64u)
        .map_err(|_| worker_rust_error("activation envelope"))?;
    let encapsulated: [u8; 32] = encapsulated
        .try_into()
        .map_err(|_| worker_rust_error("activation envelope"))?;
    let ciphertext = URL_SAFE_NO_PAD
        .decode(&envelope.ciphertext_b64u)
        .map_err(|_| worker_rust_error("activation envelope"))?;
    let plaintext =
        match crate::hpke::open(&private_key, &encapsulated, aad.as_bytes(), &ciphertext) {
            Ok(value) => value,
            Err(_) => return json(&PublicError::broker(BrokerError::Brk109), 409),
        };
    let body: GenericActivationSubmission = match parse_json(&plaintext) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::broker(BrokerError::Brk109), 400),
    };
    let private_match = match row.activation_kind.as_str() {
        "secret_submission" => {
            body.channel_ref
                .as_ref()
                .is_some_and(|v| Some(hash(v.as_bytes())) == row.channel_ref_hash)
                && body.material_b64u.is_some()
        }
        "workload_binding" => {
            body.channel_ref
                .as_ref()
                .is_some_and(|v| Some(hash(v.as_bytes())) == row.channel_ref_hash)
                && body
                    .nonce
                    .as_ref()
                    .is_some_and(|v| Some(hash(v.as_bytes())) == row.workload_nonce_hash)
                && body.assertion_b64u.is_some()
                && body
                    .issued_at
                    .is_some_and(|v| v <= now_seconds() && now_seconds() - v <= 600)
        }
        "external_custodian_binding" => {
            body.challenge
                .as_ref()
                .is_some_and(|v| Some(hash(v.as_bytes())) == row.challenge_hash)
                && body.custodian_ref.is_some()
                && body.remote_proof.is_some()
        }
        _ => false,
    };
    if !private_match {
        return json(&PublicError::broker(BrokerError::Brk109), 400);
    }
    let claimed=db.prepare("UPDATE generic_activation_intents_v2 SET status='claimed' WHERE org_id=? AND activation_ref=? AND status='awaiting_action' AND expires_at>?")
      .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(activation_ref),JsValue::from_f64(now_seconds() as f64)])?.run().await?;
    if !claimed.success() || claimed.meta().ok().flatten().and_then(|meta| meta.changes) != Some(1)
    {
        return json(&PublicError::broker(BrokerError::Brk203), 409);
    }
    let profile: Value = serde_json::from_str(&row.canonical_profile_json)?;
    if execute_generic_activation_driver(&profile, &body, &row.expected_claims_json).is_err() {
        db.prepare("UPDATE generic_activation_intents_v2 SET status='failed' WHERE org_id=? AND activation_ref=?").bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(activation_ref)])?.run().await?;
        return json(&PublicError::broker(BrokerError::Brk109), 409);
    }
    let endpoint = match row.activation_kind.as_str() {
        "secret_submission" => "/validate",
        "workload_binding" => "/token-exchange",
        _ => "/bind-external",
    };
    let driver_config_value: Value = serde_json::from_str(&row.driver_config_json)?;
    let driver_request = serde_json::json!({"activation_ref":activation_ref,"profile":profile,"driver_config":driver_config_value,"submission":body,"expected_claims":serde_json::from_str::<Value>(&row.expected_claims_json)?});
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    init.with_body(Some(
        JsString::from(serde_json::to_string(&driver_request)?).into(),
    ));
    let mut driver =
        Request::new_with_init(&format!("http://auth-driver.internal{endpoint}"), &init)?;
    driver
        .headers_mut()?
        .set("content-type", JSON_CONTENT_TYPE)?;
    driver.headers_mut()?.set(
        "x-lattice-auth-driver-service-auth",
        &env.secret("AUTH_DRIVER_SERVICE_AUTH")?.to_string(),
    )?;
    let mut response = env
        .service("AUTH_DRIVER_SERVICE")?
        .fetch_request(driver)
        .await?;
    if response.status_code() != 200 {
        return json(&PublicError::broker(BrokerError::Brk109), 409);
    }
    let reply: DriverActivationReply =
        bounded_response_json(&mut response, crate::protocol::MAX_PROVIDER_RESPONSE).await?;
    let expected: Vec<String> = serde_json::from_str(&row.expected_claims_json)?;
    let mut actual = reply.claims.clone();
    actual.sort();
    actual.dedup();
    let mut expected_sorted = expected;
    expected_sorted.sort();
    expected_sorted.dedup();
    if actual != expected_sorted || reply.account_subject.is_empty() {
        return json(&PublicError::broker(BrokerError::Brk109), 409);
    }
    let connection_ref = opaque_id("connection_v2_")?;
    let account =
        commitment_value(env, "account-subject", reply.account_subject.as_bytes())?["value"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
    let remote = row.activation_kind == "external_custodian_binding";
    let (sealed, remote_handle, remote_binding_hash) = if remote {
        let handle = reply
            .remote_proof
            .ok_or_else(|| worker_rust_error("remote binding"))?;
        (
            Vec::new(),
            Some(handle.clone()),
            Some(hash(handle.as_bytes())),
        )
    } else {
        let opaque = reply
            .material_b64u
            .ok_or_else(|| worker_rust_error("driver material"))?;
        let payload = serde_json::to_vec(
            &serde_json::json!({"connection_ref":connection_ref,"route":"auth-driver","account_commitment":account,"scopes":actual,"refresh_token":opaque,"access_token":opaque,"access_expires_at":now_seconds()+3600}),
        )?;
        let (nonce, ciphertext) =
            seal_activation_payload(env, &format!("{connection_ref}\0v2-material"), &payload)?;
        (
            serde_json::to_vec(&serde_json::json!({"nonce":nonce,"ciphertext":ciphertext}))?,
            None,
            None,
        )
    };
    if prepare_activated_connection(
        &db,
        env,
        &session.org_id,
        &session.deployment_id,
        &connection_ref,
        &format!("{}@{}", row.profile_ref, row.profile_version),
        &account,
        &actual,
        "auth-driver",
        &sealed,
        if remote {
            "remote_external"
        } else {
            "local_sealed"
        },
        remote_binding_hash.as_deref(),
        Some(row.canonical_profile_json.as_bytes()),
        driver_config_value.get("custodian"),
        driver_config_value.get("transport"),
    )
    .await
    .is_err()
    {
        return json(&PublicError::unavailable(), 503);
    }
    db.prepare("UPDATE generic_activation_intents_v2 SET status='active',connection_ref=?,remote_handle=? WHERE org_id=? AND activation_ref=? AND status='claimed'").bind(&[JsValue::from_str(&connection_ref),remote_handle.as_deref().map(JsValue::from_str).unwrap_or(JsValue::NULL),JsValue::from_str(&session.org_id),JsValue::from_str(activation_ref)])?.run().await?;
    json(
        &serde_json::json!({"kind":"complete","activation_ref":row.activation_ref,"connection_ref":connection_ref,"status":"active"}),
        200,
    )
}

pub async fn connection_route(request: &Request, env: &Env) -> worker::Result<Response> {
    let session = match authenticate(request, env, &[]).await {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    let path = request.path();
    let connection_ref = path.trim_start_matches("/v0.2/connections/");
    if connection_ref.is_empty() || connection_ref.len() > 128 {
        return json(&PublicError::invalid(), 400);
    }
    let db = env.d1("BROKER_DB")?;
    let Some(connection) = load_connection(&db, &session.org_id, connection_ref).await? else {
        return json(&PublicError::broker(BrokerError::Brk109), 404);
    };
    if request.method() == Method::Get {
        return json(
            &serde_json::json!({
                "connection_ref": connection.connection_ref,
                "status": connection.status,
                "revocation_epoch": connection.revocation_epoch,
                "authority_view_hash": connection.authority_view_hash_value(),
                "active_material_generation": connection.active_material_generation
            }),
            200,
        );
    }
    if request.method() != Method::Delete {
        return json(&PublicError::invalid(), 405);
    }
    revoke_connection_v2(env, &db, &session, connection).await
}

#[derive(Deserialize)]
struct RevocationJournalRow {
    phase: String,
    provider_evidence_hash: Option<String>,
    destruction_evidence_hash: Option<String>,
    authority_epoch: u64,
    canonical_journal_json: String,
}

async fn revoke_connection_v2(
    env: &Env,
    db: &D1Database,
    session: &SessionRow,
    connection: ConnectionV2Row,
) -> worker::Result<Response> {
    if connection.status == "revoked" {
        return json(
            &serde_json::json!({
                "connection_ref": connection.connection_ref,
                "status":"revoked",
                "revocation_epoch":connection.revocation_epoch,
                "redelivery":true
            }),
            200,
        );
    }
    if !matches!(
        connection.status.as_str(),
        "active" | "reconciling" | "revoking" | "cleanup_pending"
    ) {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    let authority =
        parse::<ConnectionAuthorityViewV2>(connection.canonical_authority_view_json.as_bytes())
            .map_err(|_| worker_rust_error("authority"))?;
    let old_epoch = authority.view.as_value()["authority_epoch"]
        .as_u64()
        .ok_or_else(|| worker_rust_error("authority"))?;
    let next_epoch = old_epoch
        .checked_add(1)
        .ok_or_else(|| worker_rust_error("authority"))?;
    let mut revoking_authority_value = authority.view.as_value().clone();
    revoking_authority_value["authority_epoch"] = next_epoch.into();
    let revoking_authority = parse_value::<ConnectionAuthorityViewV2>(&revoking_authority_value)
        .map_err(|_| worker_rust_error("revoking authority"))?;
    let revoking_authority_hash = revoking_authority.content_hash();
    let revoking_authority_json = std::str::from_utf8(revoking_authority.canonical_bytes())
        .map_err(|_| worker_rust_error("revoking authority"))?;
    let snapshot = broker_core::canonical::from_serde(
        &serde_json::json!({
            "schema_version":"0.2","connection_ref":connection.connection_ref,
            "authority_view_hash":connection.authority_view_hash_value(),
            "authority_epoch":old_epoch,"next_authority_epoch":next_epoch,
            "material_generation":connection.active_material_generation,"phase":"snapshot"
        }),
        64 * 1024,
    )
    .map_err(|_| worker_rust_error("revocation"))?;
    db.prepare("INSERT OR IGNORE INTO connection_revocations_v2(org_id,connection_ref,phase,expected_generation,authority_epoch,canonical_journal_json,cas_version) VALUES(?,?,'snapshot',?,?,?,0)")
        .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref),JsValue::from_f64(connection.active_material_generation as f64),JsValue::from_f64(next_epoch as f64),JsValue::from_str(std::str::from_utf8(snapshot.as_bytes()).map_err(|_|worker_rust_error("revocation"))?)])?.run().await?;
    db.prepare("UPDATE connections_v2 SET status='revoking',revocation_epoch=?,authority_view_hash=?,canonical_authority_view_json=? WHERE org_id=? AND connection_ref=? AND status IN ('active','reconciling')")
        .bind(&[JsValue::from_f64(next_epoch as f64),JsValue::from_str(&revoking_authority_hash),JsValue::from_str(revoking_authority_json),JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?.run().await?;
    let mut journal = db.prepare("SELECT phase,provider_evidence_hash,destruction_evidence_hash,authority_epoch,canonical_journal_json FROM connection_revocations_v2 WHERE org_id=? AND connection_ref=?")
        .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?.first::<RevocationJournalRow>(None).await?.ok_or_else(||worker_rust_error("revocation"))?;
    let revocation_fence_hash = hash(journal.canonical_journal_json.as_bytes());
    let begin = CredentialStateEnvelope {
        org_id: session.org_id.clone(),
        connection_ref: connection.connection_ref.clone(),
        command: CredentialStateCommand::BeginRevocation {
            fence_hash: revocation_fence_hash,
        },
    };
    let _: CredentialStateReply =
        credential_do(env, &session.org_id, &connection.connection_ref, &begin).await?;
    if journal.phase == "snapshot" || journal.phase == "cleanup_pending" {
        let token = if connection.material_mode == "remote_external" {
            #[derive(Deserialize)]
            struct RemoteHandleRow {
                remote_handle: String,
            }
            db.prepare("SELECT remote_handle FROM generic_activation_intents_v2 WHERE org_id=? AND connection_ref=? AND status='active'")
                .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?.first::<RemoteHandleRow>(None).await?
                .map(|row|row.remote_handle).ok_or_else(||worker_rust_error("remote handle"))?
        } else {
            let read = CredentialStateEnvelope {
                org_id: session.org_id.clone(),
                connection_ref: connection.connection_ref.clone(),
                command: CredentialStateCommand::ReadMaterialForRevocation {
                    generation: connection.active_material_generation,
                },
            };
            let reply: CredentialStateReply = match credential_do(
                env,
                &session.org_id,
                &connection.connection_ref,
                &read,
            )
            .await
            {
                Ok(value) => value,
                Err(_) => {
                    db.prepare("UPDATE connections_v2 SET status='cleanup_pending' WHERE org_id=? AND connection_ref=?").bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?.run().await?;
                    return json(&PublicError::unavailable(), 503);
                }
            };
            let sealed = reply
                .revocation_material
                .ok_or_else(|| worker_rust_error("revocation"))?;
            refresh_token_from_v2_envelope(env, &connection.connection_ref, &sealed)?
        };
        let evidence = match revoke_profile_material(env, db, &session.org_id, &connection, &token)
            .await
        {
            Ok(value) => value,
            Err(_) => {
                db.prepare("UPDATE connection_revocations_v2 SET phase='cleanup_pending',cas_version=cas_version+1 WHERE org_id=? AND connection_ref=?").bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?.run().await?;
                db.prepare("UPDATE connections_v2 SET status='cleanup_pending' WHERE org_id=? AND connection_ref=?").bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?.run().await?;
                return json(&PublicError::unavailable(), 503);
            }
        };
        db.prepare("UPDATE connection_revocations_v2 SET phase='provider_revoked',provider_evidence_hash=?,provider_evidence_json=?,remote_destruction_proof_hash=?,cas_version=cas_version+1 WHERE org_id=? AND connection_ref=? AND phase IN ('snapshot','cleanup_pending')")
            .bind(&[JsValue::from_str(&evidence.evidence_hash),JsValue::from_str(&evidence.canonical_json),evidence.remote_proof_hash.as_deref().map(JsValue::from_str).unwrap_or(JsValue::NULL),JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?.run().await?;
        journal.phase = "provider_revoked".into();
        journal.provider_evidence_hash = Some(evidence.evidence_hash);
    }
    if journal.phase == "provider_revoked" {
        let evidence = hash(
            format!(
                "lattice.v2.material-destruction\0{}\0{}\0{}",
                session.org_id, connection.connection_ref, connection.active_material_generation
            )
            .as_bytes(),
        );
        let destroy = CredentialStateEnvelope {
            org_id: session.org_id.clone(),
            connection_ref: connection.connection_ref.clone(),
            command: CredentialStateCommand::DestroyMaterial {
                expected_generation: connection.active_material_generation,
                evidence_hash: evidence.clone(),
            },
        };
        let _: CredentialStateReply =
            credential_do(env, &session.org_id, &connection.connection_ref, &destroy).await?;
        db.prepare("UPDATE connection_revocations_v2 SET phase='material_destroyed',destruction_evidence_hash=?,cas_version=cas_version+1 WHERE org_id=? AND connection_ref=? AND phase='provider_revoked'")
            .bind(&[JsValue::from_str(&evidence),JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?.run().await?;
        journal.phase = "material_destroyed".into();
        journal.destruction_evidence_hash = Some(evidence);
    }
    if journal.phase == "material_destroyed" {
        let mut next_authority_value = authority.view.as_value().clone();
        next_authority_value["authority_epoch"] = journal.authority_epoch.into();
        let next_authority = parse_value::<ConnectionAuthorityViewV2>(&next_authority_value)
            .map_err(|_| worker_rust_error("revoked authority"))?;
        let next_authority_json = std::str::from_utf8(next_authority.canonical_bytes())
            .map_err(|_| worker_rust_error("revoked authority"))?;
        let next_authority_hash = next_authority.content_hash();
        let event=broker_core::canonical::from_serde(&serde_json::json!({
            "schema_version":"0.2","org_id":session.org_id,"connection_ref":connection.connection_ref,
            "phase":"revoked","authority_epoch":journal.authority_epoch,"provider_evidence_hash":journal.provider_evidence_hash,
            "destruction_evidence_hash":journal.destruction_evidence_hash,"material_generation":connection.active_material_generation
        }),64*1024).map_err(|_|worker_rust_error("revocation"))?;
        let statements=vec![
            db.prepare("UPDATE connections_v2 SET status='revoked',revocation_epoch=?,authority_view_hash=?,canonical_authority_view_json=?,fence_generation=fence_generation+1 WHERE org_id=? AND connection_ref=? AND status IN ('revoking','cleanup_pending')").bind(&[JsValue::from_f64(journal.authority_epoch as f64),JsValue::from_str(&next_authority_hash),JsValue::from_str(next_authority_json),JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?,
            db.prepare("UPDATE binding_attestations_v2 SET state='revoked' WHERE org_id=? AND connection_ref=?").bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?,
            db.prepare("UPDATE connection_revocations_v2 SET phase='complete',canonical_journal_json=?,cas_version=cas_version+1 WHERE org_id=? AND connection_ref=? AND phase='material_destroyed'").bind(&[JsValue::from_str(std::str::from_utf8(event.as_bytes()).map_err(|_|worker_rust_error("revocation"))?),JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?,
            db.prepare("INSERT INTO credential_cutover_events_v2(org_id,connection_ref,phase,event_hash,canonical_event_json,recorded_at) VALUES(?,?,'revoked',?,?,?)").bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref),JsValue::from_str(&hash(event.as_bytes())),JsValue::from_str(std::str::from_utf8(event.as_bytes()).map_err(|_|worker_rust_error("revocation"))?),JsValue::from_f64(now_seconds() as f64)])?
        ];
        let results = db.batch(statements).await?;
        if results.iter().any(|value| !value.success()) {
            return json(&PublicError::unavailable(), 503);
        }
    }
    json(
        &serde_json::json!({"connection_ref":connection.connection_ref,"status":"revoked","revocation_epoch":journal.authority_epoch,"redelivery":false}),
        200,
    )
}

fn refresh_token_from_v2_envelope(
    env: &Env,
    connection: &str,
    sealed: &[u8],
) -> worker::Result<String> {
    #[derive(Deserialize)]
    struct Envelope {
        nonce: String,
        ciphertext: String,
    }
    let sealed: Envelope =
        serde_json::from_slice(sealed).map_err(|_| worker_rust_error("revocation"))?;
    let bytes = open_activation_payload(
        env,
        &format!("{connection}\0v2-material"),
        &sealed.nonce,
        &sealed.ciphertext,
    )?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| worker_rust_error("revocation"))?;
    value
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| worker_rust_error("revocation"))
}

struct ProviderRevocationEvidence {
    evidence_hash: String,
    canonical_json: String,
    remote_proof_hash: Option<String>,
}

async fn revoke_profile_material(
    env: &Env,
    db: &D1Database,
    org_id: &str,
    connection: &ConnectionV2Row,
    token: &str,
) -> worker::Result<ProviderRevocationEvidence> {
    let correlation = hash(
        format!(
            "revoke\0{}\0{}",
            connection.connection_ref,
            connection.revocation_epoch + 1
        )
        .as_bytes(),
    );
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    let google = connection.profile_ref == "auth.google.workspace.oauth2";
    #[derive(Deserialize)]
    struct DriverConfigRow {
        driver_config_json: String,
    }
    let driver_config = if google {
        None
    } else {
        Some(db.prepare("SELECT driver_config_json FROM generic_activation_intents_v2 WHERE org_id=? AND connection_ref=? AND status='active'")
            .bind(&[JsValue::from_str(org_id),JsValue::from_str(&connection.connection_ref)])?
            .first::<DriverConfigRow>(None).await?
            .map(|row| serde_json::from_str::<Value>(&row.driver_config_json))
            .transpose()?
            .ok_or_else(||worker_rust_error("driver config"))?)
    };
    init.with_body(Some(
        JsString::from(serde_json::to_string(&if google {
            serde_json::json!({"token":token})
        } else {
            serde_json::json!({"connection_ref":connection.connection_ref,"profile_ref":connection.profile_ref,"material_or_remote_proof":token,"driver_config":driver_config})
        })?).into(),
    ));
    let mut request = Request::new_with_init(
        if google {
            "http://token.internal/revoke"
        } else {
            "http://auth-driver.internal/revoke"
        },
        &init,
    )?;
    request
        .headers_mut()?
        .set("content-type", JSON_CONTENT_TYPE)?;
    if google {
        request.headers_mut()?.set(
            "x-lattice-egress-auth",
            &env.secret("GOOGLE_EGRESS_SERVICE_AUTH")?.to_string(),
        )?;
    } else {
        request.headers_mut()?.set(
            "x-lattice-auth-driver-service-auth",
            &env.secret("AUTH_DRIVER_SERVICE_AUTH")?.to_string(),
        )?;
    }
    request
        .headers_mut()?
        .set("x-lattice-correlation-id", &correlation)?;
    request
        .headers_mut()?
        .set("x-lattice-idempotency-key", &correlation)?;
    let service = if google {
        env.service(
            crate::composition::installed_provider_plane(&now_rfc3339())
                .map_err(|_| worker_rust_error("composition"))?
                .token_service_binding,
        )?
    } else {
        env.service("AUTH_DRIVER_SERVICE")?
    };
    let mut response = service.fetch_request(request).await?;
    if response.status_code() != 200 {
        return Err(worker_rust_error("provider revoke"));
    }
    let response_value: Value =
        bounded_response_json(&mut response, crate::protocol::MAX_PROVIDER_RESPONSE).await?;
    let remote_proof_hash = if google {
        None
    } else {
        Some(hash(
            response_value
                .get("remote_destruction_proof")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| worker_rust_error("remote destruction proof"))?
                .as_bytes(),
        ))
    };
    let evidence=broker_core::canonical::from_serde(&serde_json::json!({"connection_ref":connection.connection_ref,"correlation_id":correlation,"profile_ref":connection.profile_ref,"provider_response_hash":hash(broker_core::canonical::from_serde(&response_value,64*1024).map_err(|_|worker_rust_error("revocation evidence"))?.as_bytes()),"remote_destruction_proof_hash":remote_proof_hash}),64*1024).map_err(|_|worker_rust_error("revocation evidence"))?;
    Ok(ProviderRevocationEvidence {
        evidence_hash: hash(evidence.as_bytes()),
        canonical_json: std::str::from_utf8(evidence.as_bytes())
            .map_err(|_| worker_rust_error("revocation evidence"))?
            .to_owned(),
        remote_proof_hash,
    })
}

async fn authority_do(
    env: &Env,
    org: &str,
    binding: &str,
    command: &V2AuthorityCommand,
) -> worker::Result<V2AuthorityReply> {
    do_request(env, "V2_AUTHORITY_DO", &format!("{org}:{binding}"), command).await
}
async fn load_outbox(
    db: &D1Database,
    org: &str,
    grant: &str,
    effect: &str,
) -> worker::Result<Option<OutboxRow>> {
    db.prepare("SELECT canonical_input_hash,phase,canonical_receipt_json,response_projection_json FROM v2_invocation_outbox WHERE org_id=? AND grant_ref=? AND logical_effect_id=?").bind(&[JsValue::from_str(org),JsValue::from_str(grant),JsValue::from_str(effect)])?.first(None).await
}
async fn load_binding_manifest(
    db: &D1Database,
    org: &str,
    binding: &str,
) -> worker::Result<String> {
    #[derive(Deserialize)]
    struct Row {
        manifest_json: String,
    }
    db.prepare("SELECT manifest_json FROM v2_binding_manifests WHERE org_id=? AND binding_ref=?")
        .bind(&[JsValue::from_str(org), JsValue::from_str(binding)])?
        .first::<Row>(None)
        .await?
        .map(|v| v.manifest_json)
        .ok_or_else(|| worker_rust_error("manifest"))
}
