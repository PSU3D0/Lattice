use super::v2_production::*;
use super::*;

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
#[serde(deny_unknown_fields)]
pub(super) struct SignedGenericProfile {
    pub(super) descriptor: Value,
    pub(super) driver_config: Value,
    pub(super) org_id: String,
    pub(super) deployment_id: String,
    pub(super) signature_b64u: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GenericActivationCreate {
    pub(super) operator_id: String,
    pub(super) request_jti: String,
    pub(super) contract_ids: Vec<String>,
    pub(super) signed_profile: SignedGenericProfile,
}
#[derive(Deserialize)]
pub(super) struct GenericActivationRow {
    pub(super) activation_ref: String,
    pub(super) deployment_id: String,
    pub(super) profile_ref: String,
    pub(super) profile_version: String,
    pub(super) canonical_profile_json: String,
    pub(super) driver_config_json: String,
    pub(super) activation_kind: String,
    pub(super) expected_claims_json: String,
    pub(super) channel_ref_hash: Option<String>,
    pub(super) challenge_hash: Option<String>,
    pub(super) workload_nonce_hash: Option<String>,
    pub(super) expires_at: i64,
    pub(super) status: String,
}
pub(super) fn verify_generic_profile(
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
pub(super) fn activation_private(request: &Request, env: &Env) -> bool {
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
pub(super) struct GenericActivationEnvelope {
    pub(super) request_jti: String,
    pub(super) issued_at: i64,
    pub(super) expires_at: i64,
    pub(super) recipient_key_id: String,
    pub(super) encapsulated_key_b64u: String,
    pub(super) ciphertext_b64u: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GenericActivationSubmission {
    pub(super) channel_ref: Option<String>,
    pub(super) challenge: Option<String>,
    pub(super) material_b64u: Option<String>,
    pub(super) assertion_b64u: Option<String>,
    pub(super) issuer: Option<String>,
    pub(super) audience: Option<String>,
    pub(super) nonce: Option<String>,
    pub(super) issued_at: Option<i64>,
    pub(super) custodian_ref: Option<String>,
    pub(super) remote_proof: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DriverActivationReply {
    pub(super) material_b64u: Option<String>,
    pub(super) account_subject: String,
    pub(super) claims: Vec<String>,
    pub(super) remote_proof: Option<String>,
}
pub(super) fn execute_generic_activation_driver(
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
