use super::*;
pub(super) use broker_core::{
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
pub(super) use broker_host::{
    ApprovedAuthorityContract, BindingDerivationV2, BindingVerificationLockV2,
    HostBootstrapIdentity, InMemoryLiveConnectionAuthority, ManifestProvenance, NodeLeaseLimitsV2,
    NodeLeaseStoreSnapshotV2, NodeLeaseStoreV2, V2AttemptStage, V2DispatchEvidence, V2ReceiptIssue,
    bootstrap_host_context, issue_binding_v2, issue_v2_receipt, verify_authority_manifest,
    verify_binding_v2,
};
pub(super) use ed25519_dalek::{Signature, Verifier, VerifyingKey};
pub(super) use serde_json::Value;
pub(super) use std::collections::BTreeMap;
pub(super) use std::collections::BTreeSet;
pub(super) use worker::D1Database;

pub(super) use super::v2_binding::{
    configuration_ready, derive_grant, install_binding, issue_node_lease, receipt_trust,
    verified_configuration,
};
pub(super) use super::v2_dispatch::invoke;
pub use super::v2_legacy_authority::V2AuthorityDurableObject;
pub(super) use super::v2_legacy_authority::{connection_route, reconcile_legacy};
pub(super) use super::v2_provider_grant::{
    create_generic_activation, prepare_activated_connection, submit_generic_activation,
};

pub(super) fn provider_profile_pins() -> worker::Result<(
    broker_auth::RegistryPin,
    broker_auth::RegistryPin,
    broker_auth::RegistryPin,
)> {
    let profile = crate::composition::installed_profile_pins(&now_rfc3339())
        .map_err(|_| worker_rust_error("composition"))?;
    Ok((profile.auth_driver, profile.custodian, profile.transport))
}

pub(super) fn lock_from_binding(
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
pub(super) fn text<'a>(v: &'a Value, f: &str) -> worker::Result<&'a str> {
    v.get(f)
        .and_then(Value::as_str)
        .ok_or_else(|| worker_rust_error("v2 artifact"))
}
pub(super) fn parse_value<T: broker_core::credential::SchemaType + serde::de::DeserializeOwned>(
    value: &Value,
) -> Result<broker_core::credential::ParsedV2<T>, BrokerError> {
    let c = broker_core::canonical::from_serde(value, T::MAX_BYTES)?;
    parse(c.as_bytes())
}
pub(super) fn host_error(error: broker_host::BrokerHostError) -> BrokerError {
    match error {
        broker_host::BrokerHostError::Broker(e) => e,
        broker_host::BrokerHostError::V2AuthorityDrift => BrokerError::Brk106,
        _ => BrokerError::Brk109,
    }
}
pub(super) fn verified_pop_proof(session: &SessionRow, body: &[u8]) -> Vec<u8> {
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
impl ConnectionV2Row {
    pub(super) fn registry_is_current(&self, now: &str) -> bool {
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

    pub(super) fn authority_view_hash_value(&self) -> Value {
        Value::String(hash(self.canonical_authority_view_json.as_bytes()))
    }
}
pub(super) async fn load_connection(
    db: &D1Database,
    org: &str,
    connection: &str,
) -> worker::Result<Option<ConnectionV2Row>> {
    db.prepare("SELECT connection_ref,profile_ref,profile_version,canonical_authority_view_json,standing_authority_ref,standing_authority_hash,canonical_standing_authority_json,contract_set_ref,contract_set_hash,canonical_contract_set_json,active_material_generation,material_mode,revocation_epoch,status FROM connections_v2 WHERE org_id=? AND connection_ref=?").bind(&[JsValue::from_str(org),JsValue::from_str(connection)])?.first(None).await
}
pub(super) async fn load_host_record(
    db: &D1Database,
    org: &str,
    reference: &str,
    kind: &str,
) -> worker::Result<Option<HostRecordRow>> {
    db.prepare("SELECT canonical_artifact_json,connection_ref,deployment_id,parent_ref,cas_version FROM v2_host_records WHERE org_id=? AND artifact_ref=? AND artifact_kind=?").bind(&[JsValue::from_str(org),JsValue::from_str(reference),JsValue::from_str(kind)])?.first(None).await
}
pub(super) async fn store_host_artifact(
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
    let parent_value = parent.map(JsValue::from_str).unwrap_or(JsValue::NULL);
    let result=db.prepare("INSERT OR IGNORE INTO v2_host_records(org_id,artifact_ref,artifact_kind,artifact_hash,canonical_artifact_json,connection_ref,deployment_id,parent_ref,cas_version,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)").bind(&[JsValue::from_str(org),JsValue::from_str(reference),JsValue::from_str(kind),JsValue::from_str(&hash(canonical)),JsValue::from_str(std::str::from_utf8(canonical).map_err(|_|worker_rust_error("artifact"))?),JsValue::from_str(connection),JsValue::from_str(deployment),parent_value,JsValue::from_f64(cas as f64),JsValue::from_f64(now_seconds() as f64)])?.run().await?;
    if !result.success() {
        return Err(worker_rust_error("artifact"));
    }
    #[derive(Deserialize)]
    struct StoredArtifactConflictRow {
        artifact_kind: String,
        artifact_hash: String,
        canonical_artifact_json: String,
        connection_ref: String,
        deployment_id: String,
        parent_ref: Option<String>,
        cas_version: u64,
    }
    let stored = db.prepare("SELECT artifact_kind,artifact_hash,canonical_artifact_json,connection_ref,deployment_id,parent_ref,cas_version FROM v2_host_records WHERE org_id=? AND artifact_ref=?")
        .bind(&[JsValue::from_str(org),JsValue::from_str(reference)])?
        .first::<StoredArtifactConflictRow>(None).await?;
    let exact = stored.is_some_and(|stored| {
        stored.artifact_kind == kind
            && stored.artifact_hash == hash(canonical)
            && stored.canonical_artifact_json.as_bytes() == canonical
            && stored.connection_ref == connection
            && stored.deployment_id == deployment
            && stored.parent_ref.as_deref() == parent
            && stored.cas_version == cas
    });
    if !exact {
        return Err(worker_rust_error("artifact conflict"));
    }
    Ok(())
}
pub(super) async fn credential_do<T: DeserializeOwned>(
    env: &Env,
    org: &str,
    connection: &str,
    envelope: &CredentialStateEnvelope,
) -> worker::Result<T> {
    let route = hash(format!("lattice.credential-state.v2\0{org}\0{connection}").as_bytes());
    do_request(env, "CREDENTIAL_STATE_V2_DO", &route, envelope).await
}

pub(super) async fn authority_do(
    env: &Env,
    org: &str,
    binding: &str,
    command: &V2AuthorityCommand,
) -> worker::Result<V2AuthorityReply> {
    do_request(env, "V2_AUTHORITY_DO", &format!("{org}:{binding}"), command).await
}
pub(super) async fn load_outbox(
    db: &D1Database,
    org: &str,
    grant: &str,
    effect: &str,
) -> worker::Result<Option<OutboxRow>> {
    db.prepare("SELECT canonical_input_hash,phase,canonical_receipt_json,response_projection_json FROM v2_invocation_outbox WHERE org_id=? AND grant_ref=? AND logical_effect_id=?").bind(&[JsValue::from_str(org),JsValue::from_str(grant),JsValue::from_str(effect)])?.first(None).await
}
pub(super) async fn load_binding_manifest(
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
