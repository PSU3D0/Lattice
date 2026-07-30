use super::v2_production::*;
use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InstallBindingRequestV2 {
    pub(super) connection_ref: String,
    pub(super) deployment_id: String,
    pub(super) bundle_id: String,
    pub(super) flow_ir_hash: String,
    pub(super) binding_lock_hash: String,
    pub(super) flow_id: String,
    pub(super) flow_ir_json: String,
    pub(super) authority_manifest_json: String,
    pub(super) contracts: Vec<String>,
}

#[derive(Deserialize)]
pub(super) struct ConnectionV2Row {
    pub(super) connection_ref: String,
    pub(super) profile_ref: String,
    pub(super) profile_version: String,
    pub(super) canonical_authority_view_json: String,
    pub(super) standing_authority_ref: String,
    pub(super) standing_authority_hash: String,
    pub(super) canonical_standing_authority_json: String,
    pub(super) contract_set_ref: String,
    pub(super) contract_set_hash: String,
    pub(super) canonical_contract_set_json: String,
    pub(super) active_material_generation: u64,
    pub(super) material_mode: String,
    pub(super) revocation_epoch: u64,
    pub(super) status: String,
}

pub(super) fn load_deployment_authority(
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

#[derive(Deserialize)]
pub(super) struct HostRecordRow {
    pub(super) canonical_artifact_json: String,
    pub(super) connection_ref: String,
    pub(super) deployment_id: String,
    pub(super) parent_ref: Option<String>,
    pub(super) cas_version: u64,
}

#[derive(Deserialize)]
pub(super) struct BindingLifecycleRow {
    pub(super) logical_binding_ref: String,
    pub(super) logical_state: String,
    pub(super) revision_state: String,
}

impl BindingLifecycleRow {
    fn authority_route<'a>(&'a self, binding_ref: &'a str) -> &'a str {
        if self
            .logical_binding_ref
            .starts_with("logical_binding_v2_legacy_")
        {
            binding_ref
        } else {
            &self.logical_binding_ref
        }
    }
}

pub(super) async fn load_binding_lifecycle(
    db: &D1Database,
    org: &str,
    binding_ref: &str,
) -> worker::Result<Option<BindingLifecycleRow>> {
    db.prepare(
        "SELECT l.logical_binding_ref,l.state AS logical_state,r.state AS revision_state \
         FROM binding_revisions_v2 r JOIN logical_bindings_v2 l \
         ON l.org_id=r.org_id AND l.logical_binding_ref=r.logical_binding_ref \
         WHERE r.org_id=? AND r.binding_ref=?",
    )
    .bind(&[JsValue::from_str(org), JsValue::from_str(binding_ref)])?
    .first(None)
    .await
}

pub(super) fn binding_lock_for_live(
    connection: &ConnectionV2Row,
    org_id: &str,
    deployment_id: &str,
    authority_manifest_hash: &str,
) -> worker::Result<BindingVerificationLockV2> {
    let profile_ref = parse_value::<AuthProfileRefV2>(&serde_json::json!({
        "profile_ref":connection.profile_ref,"version":connection.profile_version
    }))
    .map_err(|_| worker_rust_error("binding lock"))?
    .view;
    let authority: Value = serde_json::from_str(&connection.canonical_authority_view_json)
        .map_err(|_| worker_rust_error("binding lock"))?;
    Ok(BindingVerificationLockV2 {
        org_id: org_id.to_owned(),
        principal: deployment_id.to_owned(),
        issuer: "broker-v2-authority".into(),
        broker_key_id: "broker-v2-authority".into(),
        deployment_id: deployment_id.to_owned(),
        authority_manifest_hash: authority_manifest_hash.to_owned(),
        standing_authority_ref: connection.standing_authority_ref.clone(),
        standing_authority_hash: connection.standing_authority_hash.clone(),
        contract_set_ref: connection.contract_set_ref.clone(),
        contract_set_hash: connection.contract_set_hash.clone(),
        connection_ref: connection.connection_ref.clone(),
        auth_profile_ref: profile_ref,
        execution_lane: text(&authority, "execution_lane")?.to_owned(),
        custody_location: text(&authority, "custody_location")?.to_owned(),
    })
}

pub(super) fn verify_binding_against_live(
    binding: &broker_core::credential::ParsedV2<BindingAttestationV2>,
    connection: &ConnectionV2Row,
    signer: &BrokerSigner,
    org_id: &str,
    deployment_id: &str,
    authority_manifest_hash: &str,
    now: &str,
) -> worker::Result<()> {
    let live = InMemoryLiveConnectionAuthority::new();
    live.set(
        connection.connection_ref.clone(),
        connection.canonical_authority_view_json.as_bytes().to_vec(),
        connection.active_material_generation,
    )
    .map_err(|_| worker_rust_error("live binding authority"))?;
    verify_binding_v2(
        binding.canonical_bytes(),
        &binding_lock_for_live(connection, org_id, deployment_id, authority_manifest_hash)?,
        &signer.verifying_key(),
        &live,
        now,
    )
    .map_err(|_| worker_rust_error("live binding verification"))?;
    if binding.view.as_value()["minimum_material_generation"].as_u64()
        != Some(connection.active_material_generation)
    {
        return Err(worker_rust_error("binding material generation drift"));
    }
    Ok(())
}

#[derive(Deserialize)]
pub(super) struct OperatorBundleRow {
    pub(super) canonical_bundle_hex: String,
}

pub(super) async fn load_verified_operator_bundle(
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
    let mut expected_policy_hashes = vec![
        hash(body.bundle_id.as_bytes()),
        body.binding_lock_hash.clone(),
        _operator_policy_hash.clone(),
    ];
    expected_policy_hashes.sort();
    let signer = BrokerSigner::from_seed(
        "broker-v2-authority",
        secret_32(env, "BINDING_SIGNING_SEED")?,
    );
    let mut identity_contracts = body.contracts.clone();
    identity_contracts.sort();
    let identity = broker_core::canonical::from_serde(
        &serde_json::json!({
            "schema_version":"lattice.logical-binding-identity.v1",
            "org_id":session.org_id,"deployment_id":session.deployment_id,
            "connection_ref":body.connection_ref,"bundle_id":body.bundle_id,
            "flow_ir_hash":body.flow_ir_hash,"flow_ir_json_hash":hash(body.flow_ir_json.as_bytes()),
            "authority_manifest_hash":hash(body.authority_manifest_json.as_bytes()),
            "binding_lock_hash":body.binding_lock_hash,"flow_id":body.flow_id,
            "contracts":identity_contracts
        }),
        64 * 1024,
    )
    .map_err(|_| worker_rust_error("logical binding identity"))?;
    let identity_hash = identity.sha256();
    let logical_binding_ref = format!("logical_binding_v2_{}", &identity_hash[7..39]);
    let now_seconds_value = now_seconds();
    db.prepare(
        "INSERT OR IGNORE INTO logical_bindings_v2(org_id,logical_binding_ref,identity_hash,connection_ref,deployment_id,state,current_binding_ref,created_at,updated_at) VALUES(?,?,?,?,?,'active',NULL,?,?)",
    )
    .bind(&[
        JsValue::from_str(&session.org_id),
        JsValue::from_str(&logical_binding_ref),
        JsValue::from_str(&identity_hash),
        JsValue::from_str(&body.connection_ref),
        JsValue::from_str(&session.deployment_id),
        JsValue::from_f64(now_seconds_value as f64),
        JsValue::from_f64(now_seconds_value as f64),
    ])?
    .run()
    .await?;
    #[derive(Deserialize)]
    struct LogicalBindingRow {
        identity_hash: String,
        connection_ref: String,
        deployment_id: String,
        state: String,
        current_binding_ref: Option<String>,
    }
    let logical = db
        .prepare("SELECT identity_hash,connection_ref,deployment_id,state,current_binding_ref FROM logical_bindings_v2 WHERE org_id=? AND logical_binding_ref=?")
        .bind(&[JsValue::from_str(&session.org_id), JsValue::from_str(&logical_binding_ref)])?
        .first::<LogicalBindingRow>(None)
        .await?
        .ok_or_else(|| worker_rust_error("logical binding"))?;
    if logical.identity_hash != identity_hash
        || logical.connection_ref != body.connection_ref
        || logical.deployment_id != session.deployment_id
    {
        return json(&PublicError::broker(BrokerError::Brk203), 409);
    }
    if logical.state != "active" {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    if let Some(current_ref) = logical.current_binding_ref {
        #[derive(Deserialize)]
        struct ExistingBindingRow {
            artifact_hash: String,
            canonical_artifact_json: String,
            revision_state: String,
            manifest_json: String,
            flow_ir_json: String,
        }
        let existing = db.prepare(
            "SELECT h.artifact_hash,h.canonical_artifact_json,r.state AS revision_state,m.manifest_json,m.flow_ir_json \
             FROM binding_revisions_v2 r JOIN v2_host_records h ON h.org_id=r.org_id AND h.artifact_ref=r.binding_ref \
             JOIN v2_binding_manifests m ON m.org_id=r.org_id AND m.binding_ref=r.binding_ref \
             WHERE r.org_id=? AND r.binding_ref=? AND r.logical_binding_ref=?",
        )
        .bind(&[
            JsValue::from_str(&session.org_id),
            JsValue::from_str(&current_ref),
            JsValue::from_str(&logical_binding_ref),
        ])?
        .first::<ExistingBindingRow>(None)
        .await?;
        if let Some(existing) = existing {
            let parsed = parse::<BindingAttestationV2>(existing.canonical_artifact_json.as_bytes());
            let verified = parsed.as_ref().is_ok_and(|parsed| {
                existing.revision_state == "active"
                    && existing.manifest_json == body.authority_manifest_json
                    && existing.flow_ir_json == body.flow_ir_json
                    && parsed.view.as_value()["policy_instance_hashes"]
                        == serde_json::json!(expected_policy_hashes)
                    && verify_binding_against_live(
                        parsed,
                        &connection,
                        &signer,
                        &session.org_id,
                        &session.deployment_id,
                        &authority.manifest.content_hash(),
                        &now_rfc3339(),
                    )
                    .is_ok()
            });
            if verified && connection.status == "active" {
                let parsed = parsed.expect("checked parsed binding");
                return json(
                    &serde_json::json!({
                        "logical_binding_ref":logical_binding_ref,
                        "binding_ref":current_ref,"binding_hash":existing.artifact_hash,
                        "binding":parsed.view.as_value(),"redelivery":true
                    }),
                    200,
                );
            }
        }
    }
    let validity_start = (now_seconds_value / 3600) * 3600;
    let revision_identity = broker_core::canonical::from_serde(
        &serde_json::json!({
            "schema_version":"lattice.binding-revision-identity.v1",
            "logical_binding_ref":logical_binding_ref,
            "authority_view_hash":hash(connection.canonical_authority_view_json.as_bytes()),
            "material_generation":connection.active_material_generation,
            "standing_authority_hash":connection.standing_authority_hash,
            "contract_set_hash":connection.contract_set_hash,
            "policy_instance_hashes":expected_policy_hashes,
            "validity_start":validity_start
        }),
        64 * 1024,
    )
    .map_err(|_| worker_rust_error("binding revision identity"))?;
    let revision_hash = revision_identity.sha256();
    let binding_ref = format!("binding_v2_{}", &revision_hash[7..39]);
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
            policy_instance_hashes: expected_policy_hashes.clone(),
            required_assurance_predicates: required_predicates,
            observed_at: rfc3339_from_seconds(validity_start),
            expires_at: rfc3339_from_seconds(validity_start + 3600),
        },
        &signer,
    ) {
        Ok(value) => value,
        Err(error) => return json(&PublicError::broker(host_error(error)), 409),
    };
    let canonical = binding.canonical_bytes();
    let artifact_hash = binding.content_hash();
    let canonical_text =
        std::str::from_utf8(canonical).map_err(|_| worker_rust_error("binding"))?;
    let expected_authority_hash = hash(connection.canonical_authority_view_json.as_bytes());
    let active_identity = "EXISTS(SELECT 1 FROM logical_bindings_v2 WHERE org_id=? AND logical_binding_ref=? AND identity_hash=? AND state='active') AND EXISTS(SELECT 1 FROM connections_v2 WHERE org_id=? AND connection_ref=? AND authority_view_hash=? AND active_material_generation=? AND standing_authority_hash=? AND contract_set_hash=? AND status IN ('reconciling','active'))";
    let statements = vec![
        db.prepare(&format!("INSERT OR IGNORE INTO v2_host_records(org_id,artifact_ref,artifact_kind,artifact_hash,canonical_artifact_json,connection_ref,deployment_id,parent_ref,cas_version,created_at) SELECT ?,?,'binding',?,?,?,?,NULL,0,? WHERE {active_identity}"))
            .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&binding_ref),JsValue::from_str(&artifact_hash),JsValue::from_str(canonical_text),JsValue::from_str(&connection.connection_ref),JsValue::from_str(&session.deployment_id),JsValue::from_f64(now_seconds_value as f64),JsValue::from_str(&session.org_id),JsValue::from_str(&logical_binding_ref),JsValue::from_str(&identity_hash),JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref),JsValue::from_str(&expected_authority_hash),JsValue::from_f64(connection.active_material_generation as f64),JsValue::from_str(&connection.standing_authority_hash),JsValue::from_str(&connection.contract_set_hash)])?,
        db.prepare(&format!("INSERT OR IGNORE INTO v2_binding_manifests(org_id,binding_ref,manifest_hash,manifest_json,flow_ir_hash,flow_ir_json) SELECT ?,?,?,?,?,? WHERE {active_identity}"))
            .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&binding_ref),JsValue::from_str(&hash(body.authority_manifest_json.as_bytes())),JsValue::from_str(&body.authority_manifest_json),JsValue::from_str(&body.flow_ir_hash),JsValue::from_str(&body.flow_ir_json),JsValue::from_str(&session.org_id),JsValue::from_str(&logical_binding_ref),JsValue::from_str(&identity_hash),JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref),JsValue::from_str(&expected_authority_hash),JsValue::from_f64(connection.active_material_generation as f64),JsValue::from_str(&connection.standing_authority_hash),JsValue::from_str(&connection.contract_set_hash)])?,
        db.prepare(&format!("INSERT OR IGNORE INTO binding_revisions_v2(org_id,binding_ref,logical_binding_ref,revision_hash,binding_hash,canonical_binding_json,authority_view_hash,material_generation,state,created_at) SELECT ?,?,?,?,?,?,?,?,'active',? WHERE {active_identity}"))
            .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&binding_ref),JsValue::from_str(&logical_binding_ref),JsValue::from_str(&revision_hash),JsValue::from_str(&artifact_hash),JsValue::from_str(canonical_text),JsValue::from_str(&hash(connection.canonical_authority_view_json.as_bytes())),JsValue::from_f64(connection.active_material_generation as f64),JsValue::from_f64(now_seconds_value as f64),JsValue::from_str(&session.org_id),JsValue::from_str(&logical_binding_ref),JsValue::from_str(&identity_hash),JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref),JsValue::from_str(&expected_authority_hash),JsValue::from_f64(connection.active_material_generation as f64),JsValue::from_str(&connection.standing_authority_hash),JsValue::from_str(&connection.contract_set_hash)])?,
        db.prepare("UPDATE binding_revisions_v2 SET state='superseded' WHERE org_id=? AND logical_binding_ref=? AND binding_ref<>? AND state='active'")
            .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&logical_binding_ref),JsValue::from_str(&binding_ref)])?,
        db.prepare("UPDATE logical_bindings_v2 SET current_binding_ref=?,updated_at=? WHERE org_id=? AND logical_binding_ref=? AND identity_hash=? AND state='active'")
            .bind(&[JsValue::from_str(&binding_ref),JsValue::from_f64(now_seconds_value as f64),JsValue::from_str(&session.org_id),JsValue::from_str(&logical_binding_ref),JsValue::from_str(&identity_hash)])?,
    ];
    let results = db.batch(statements).await?;
    if results.iter().any(|result| !result.success()) {
        return json(&PublicError::broker(BrokerError::Brk401), 503);
    }
    #[derive(Deserialize)]
    struct StoredRevisionRow {
        logical_state: String,
        current_binding_ref: Option<String>,
        revision_state: String,
        artifact_hash: String,
        canonical_artifact_json: String,
        manifest_json: String,
        flow_ir_json: String,
    }
    let stored = db.prepare("SELECT l.state AS logical_state,l.current_binding_ref,r.state AS revision_state,h.artifact_hash,h.canonical_artifact_json,m.manifest_json,m.flow_ir_json FROM logical_bindings_v2 l JOIN binding_revisions_v2 r ON r.org_id=l.org_id AND r.logical_binding_ref=l.logical_binding_ref JOIN v2_host_records h ON h.org_id=r.org_id AND h.artifact_ref=r.binding_ref JOIN v2_binding_manifests m ON m.org_id=r.org_id AND m.binding_ref=r.binding_ref WHERE r.org_id=? AND r.binding_ref=?")
        .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&binding_ref)])?
        .first::<StoredRevisionRow>(None).await?;
    let exact = stored.as_ref().is_some_and(|stored| {
        stored.logical_state == "active"
            && stored.current_binding_ref.as_deref() == Some(binding_ref.as_str())
            && stored.revision_state == "active"
            && stored.artifact_hash == artifact_hash
            && stored.canonical_artifact_json.as_bytes() == canonical
            && stored.manifest_json == body.authority_manifest_json
            && stored.flow_ir_json == body.flow_ir_json
    });
    if !exact {
        return json(&PublicError::broker(BrokerError::Brk203), 409);
    }
    let current_connection =
        match load_connection(&db, &session.org_id, &body.connection_ref).await? {
            Some(value)
                if matches!(value.status.as_str(), "reconciling" | "active")
                    && value.registry_is_current(&now_rfc3339()) =>
            {
                value
            }
            _ => return json(&PublicError::broker(BrokerError::Brk106), 409),
        };
    if verify_binding_against_live(
        &binding,
        &current_connection,
        &signer,
        &session.org_id,
        &session.deployment_id,
        &authority.manifest.content_hash(),
        &now_rfc3339(),
    )
    .is_err()
    {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
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
            "logical_binding_ref":logical_binding_ref,
            "binding_ref":binding_ref,
            "binding_hash":artifact_hash,
            "binding":binding.view.as_value()
        }),
        201,
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NodeLeaseRequest {
    pub(super) session_ref: String,
    pub(super) binding_ref: String,
    pub(super) bundle_id: String,
    pub(super) flow_ir_hash: String,
    pub(super) binding_lock_hash: String,
    pub(super) flow_id: String,
    pub(super) run_id: String,
    pub(super) node_id: String,
    pub(super) node_alias: String,
    pub(super) activation_ordinal: u64,
    pub(super) operation_contract: String,
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
    let lifecycle = match load_binding_lifecycle(&db, &session.org_id, &body.binding_ref).await? {
        Some(value)
            if value.logical_state == "active"
                && matches!(value.revision_state.as_str(), "active" | "superseded") =>
        {
            value
        }
        _ => return json(&PublicError::broker(BrokerError::Brk106), 409),
    };
    let connection = match load_connection(&db, &session.org_id, &record.connection_ref).await? {
        Some(value) if value.status == "active" && value.registry_is_current(&now_rfc3339()) => {
            value
        }
        _ => return json(&PublicError::broker(BrokerError::Brk106), 409),
    };
    let signer = BrokerSigner::from_seed(
        "broker-v2-authority",
        secret_32(env, "BINDING_SIGNING_SEED")?,
    );
    let manifest_hash = binding.view.as_value()["authority_manifest_hash"]
        .as_str()
        .unwrap_or_default();
    if verify_binding_against_live(
        &binding,
        &connection,
        &signer,
        &session.org_id,
        &session.deployment_id,
        manifest_hash,
        &now_rfc3339(),
    )
    .is_err()
    {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    db.prepare("INSERT OR IGNORE INTO binding_run_pins_v2(org_id,deployment_id,run_id,logical_binding_ref,binding_ref,created_at) VALUES(?,?,?,?,?,?)")
        .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&session.deployment_id),JsValue::from_str(&body.run_id),JsValue::from_str(&lifecycle.logical_binding_ref),JsValue::from_str(&body.binding_ref),JsValue::from_f64(now_seconds() as f64)])?
        .run().await?;
    #[derive(Deserialize)]
    struct RunBindingPinRow {
        logical_binding_ref: String,
        binding_ref: String,
    }
    let run_pin = db.prepare("SELECT logical_binding_ref,binding_ref FROM binding_run_pins_v2 WHERE org_id=? AND deployment_id=? AND run_id=?")
        .bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&session.deployment_id),JsValue::from_str(&body.run_id)])?
        .first::<RunBindingPinRow>(None).await?;
    if run_pin.is_none_or(|pin| {
        pin.logical_binding_ref != lifecycle.logical_binding_ref
            || pin.binding_ref != body.binding_ref
    }) {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
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
    if !artifact_time_is_current(standing.view.as_value(), &now_rfc3339()) {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    let authorized_contract =
        parse::<ContractSetV2>(connection.canonical_contract_set_json.as_bytes())
            .ok()
            .and_then(|contracts| contracts.view.as_value()["contracts"].as_array().cloned())
            .is_some_and(|contracts| {
                contracts.iter().any(|contract| {
                    contract["contract_id"] == installed.contract_id
                        && contract["contract_hash"] == installed.contract_hash
                })
            });
    if !authorized_contract {
        return json(&PublicError::broker(BrokerError::Brk108), 403);
    }
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
        logical_binding_ref: lifecycle.logical_binding_ref.clone(),
        binding_revision_ref: body.binding_ref.clone(),
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
        lifecycle.authority_route(&body.binding_ref),
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
pub(super) struct ExactGrantRequest {
    pub(super) session_ref: String,
    pub(super) binding_ref: String,
    pub(super) node_lease_ref: String,
    pub(super) bundle_id: String,
    pub(super) flow_ir_hash: String,
    pub(super) binding_lock_hash: String,
    pub(super) flow_id: String,
    pub(super) run_id: String,
    pub(super) node_id: String,
    pub(super) node_alias: String,
    pub(super) activation_ordinal: u64,
    pub(super) semantic_effect_slot: String,
    pub(super) expected_cas_version: u64,
    pub(super) input: Value,
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
    let authority =
        parse::<ConnectionAuthorityViewV2>(connection.canonical_authority_view_json.as_bytes())
            .map_err(|_| worker_rust_error("authority"))?;
    if lease.view.as_value()["authority_view_hash"] != serde_json::json!(authority.content_hash())
        || lease.view.as_value()["authority_epoch"] != authority.view.as_value()["authority_epoch"]
        || lease.view.as_value()["minimum_material_generation"].as_u64()
            != Some(connection.active_material_generation)
    {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
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
    let lifecycle = match load_binding_lifecycle(&db, &session.org_id, binding_ref).await? {
        Some(value)
            if value.logical_state == "active"
                && matches!(value.revision_state.as_str(), "active" | "superseded") =>
        {
            value
        }
        _ => return json(&PublicError::broker(BrokerError::Brk106), 409),
    };
    let reply: V2AuthorityReply = authority_do(
        env,
        &session.org_id,
        lifecycle.authority_route(binding_ref),
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
    if store_host_artifact(
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
    .await
    .is_err()
    {
        return json(&PublicError::broker(BrokerError::Brk203), 409);
    }
    let grant = parse::<ExecutionGrantV2>(&reply.canonical_artifact)
        .map_err(|_| worker_rust_error("grant"))?;
    json(
        &serde_json::json!({"grant_ref":reply.artifact_ref,"grant":grant.view.as_value(),"cas_version":reply.cas_version,"redelivery":reply.redelivery}),
        if reply.redelivery { 200 } else { 201 },
    )
}
