use super::*;
use crate::management::decode_public_key;
use broker_auth::{
    ActivationKind, AuthProfile, AuthScheme, FinalizedUnsignedRequest, FirewallPolicy, Header,
    ProfileAuthDriver, PublicClaimsPolicy, RegistryPin, StrictResponseFirewall,
};
use broker_core::credential::spi::{BoundedRawResponse, authorize_with_driver};
use serde_json::Value;

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    InstallProfile {
        org_id: String,
        profile: TestProfile,
        standing_authority_ref: String,
        #[serde(default)]
        trusted_source_refs: Vec<String>,
        #[serde(default)]
        policy_refs: Vec<String>,
    },
    Create {
        org_id: String,
        activation_ref: String,
        profile_ref: String,
        request_jti: String,
    },
    #[serde(rename = "oauth_callback")]
    OAuthCallback {
        org_id: String,
        activation_ref: String,
        state: String,
        code: String,
        #[serde(default)]
        crash_phase: Option<String>,
    },
    SubmitPrivate {
        org_id: String,
        activation_ref: String,
        submission_jti: String,
        nonce_b64u: String,
        ciphertext_b64u: String,
    },
    ExternalBind {
        org_id: String,
        activation_ref: String,
        public_key_b64u: String,
        signature_b64u: String,
    },
    ActivateFence {
        org_id: String,
        activation_ref: String,
    },
    Dispatch {
        org_id: String,
        activation_ref: String,
        request_ref: String,
        response: Value,
        #[serde(default)]
        crash_phase: Option<String>,
    },
    Rotate {
        org_id: String,
        activation_ref: String,
        expected_cas: u64,
        submission_jti: String,
        nonce_b64u: String,
        ciphertext_b64u: String,
        #[serde(default)]
        crash_phase: Option<String>,
    },
    Read {
        org_id: String,
        activation_ref: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TestProfile {
    profile_ref: String,
    activation_kind: String,
    scheme_kind: String,
    endpoint: String,
    #[serde(default)]
    placement_name: String,
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    workload_issuer: String,
    #[serde(default)]
    workload_audience: String,
    #[serde(default)]
    external_public_key_b64u: String,
}

#[derive(Deserialize)]
struct ProfileRow {
    canonical_profile_config_json: String,
    standing_authority_ref: String,
    trusted_source_refs_json: String,
    policy_refs_json: String,
}
#[derive(Deserialize)]
struct ActivationRow {
    profile_ref: String,
    status: String,
    action_nonce_hash: String,
    connection_ref: Option<String>,
    authority_view_hash: Option<String>,
    material_generation: Option<u64>,
    cas_version: u64,
}
#[derive(Deserialize)]
struct OutboxRow {
    request_hash: String,
    phase: String,
    result_json: Option<String>,
}

pub(super) async fn route(request: &mut Request, env: &Env) -> worker::Result<Response> {
    if env
        .var(concat!("LOCAL_", "TEST_MODE"))
        .ok()
        .map(|value| value.to_string())
        .as_deref()
        != Some("true")
    {
        return json(&PublicError::invalid(), 404);
    }
    let raw: Value = match bounded_json(request, MAX_MANAGEMENT_BODY).await {
        Ok(command) => command,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let command: Command = match serde_json::from_value(raw) {
        Ok(command) => command,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let db = env.d1("BROKER_DB")?;
    ensure_schema(&db).await?;
    match command {
        Command::InstallProfile {
            org_id,
            profile,
            standing_authority_ref,
            trusted_source_refs,
            policy_refs,
        } => {
            install_profile(
                &db,
                org_id,
                profile,
                standing_authority_ref,
                trusted_source_refs,
                policy_refs,
            )
            .await
        }
        Command::Create {
            org_id,
            activation_ref,
            profile_ref,
            request_jti,
        } => create(&db, org_id, activation_ref, profile_ref, request_jti).await,
        Command::OAuthCallback {
            org_id,
            activation_ref,
            state,
            code,
            crash_phase,
        } => {
            oauth_callback(
                &db,
                env,
                &org_id,
                &activation_ref,
                &state,
                &code,
                crash_phase,
            )
            .await
        }
        Command::SubmitPrivate {
            org_id,
            activation_ref,
            submission_jti,
            nonce_b64u,
            ciphertext_b64u,
        } => {
            submit_private(
                &db,
                env,
                &org_id,
                &activation_ref,
                &submission_jti,
                &nonce_b64u,
                &ciphertext_b64u,
                false,
            )
            .await
        }
        Command::ExternalBind {
            org_id,
            activation_ref,
            public_key_b64u,
            signature_b64u,
        } => {
            external_bind(
                &db,
                env,
                &org_id,
                &activation_ref,
                &public_key_b64u,
                &signature_b64u,
            )
            .await
        }
        Command::ActivateFence {
            org_id,
            activation_ref,
        } => activate_fence(env, &db, &org_id, &activation_ref).await,
        Command::Dispatch {
            org_id,
            activation_ref,
            request_ref,
            response,
            crash_phase,
        } => {
            dispatch(
                &db,
                env,
                &org_id,
                &activation_ref,
                &request_ref,
                response,
                crash_phase,
            )
            .await
        }
        Command::Rotate {
            org_id,
            activation_ref,
            expected_cas,
            submission_jti,
            nonce_b64u,
            ciphertext_b64u,
            crash_phase,
        } => {
            rotate(
                &db,
                env,
                &org_id,
                &activation_ref,
                expected_cas,
                &submission_jti,
                &nonce_b64u,
                &ciphertext_b64u,
                crash_phase,
            )
            .await
        }
        Command::Read {
            org_id,
            activation_ref,
        } => read(&db, &org_id, &activation_ref).await,
    }
}

async fn install_profile(
    db: &D1Database,
    org_id: String,
    profile: TestProfile,
    standing_authority_ref: String,
    mut trusted_source_refs: Vec<String>,
    mut policy_refs: Vec<String>,
) -> worker::Result<Response> {
    if !profile.endpoint.starts_with("https://")
        || profile.profile_ref.is_empty()
        || standing_authority_ref.is_empty()
        || !matches!(
            profile.activation_kind.as_str(),
            "oauth" | "secret" | "workload" | "external"
        )
    {
        return json(&PublicError::broker(BrokerError::Brk109), 400);
    }
    trusted_source_refs.sort();
    trusted_source_refs.dedup();
    policy_refs.sort();
    policy_refs.dedup();
    let config = serde_json::to_string(&profile).map_err(|_| worker_rust_error("v2 profile"))?;
    let definition_hash = format!("sha256:{}", hex::encode(Sha256::digest(config.as_bytes())));
    db.prepare("INSERT OR REPLACE INTO activation_profiles_v2 (org_id, profile_ref, profile_version, registry_definition_hash, canonical_profile_config_json, standing_authority_ref, trusted_source_refs_json, policy_refs_json) VALUES (?, ?, '1', ?, ?, ?, ?, ?)")
        .bind(&[
            JsValue::from_str(&org_id), JsValue::from_str(&profile.profile_ref),
            JsValue::from_str(&definition_hash), JsValue::from_str(&config),
            JsValue::from_str(&standing_authority_ref),
            JsValue::from_str(&serde_json::to_string(&trusted_source_refs).map_err(|_|worker_rust_error("v2 profile"))?),
            JsValue::from_str(&serde_json::to_string(&policy_refs).map_err(|_|worker_rust_error("v2 profile"))?),
        ])?.run().await?;
    json(
        &serde_json::json!({"installed":true,"definition_hash":definition_hash}),
        201,
    )
}

async fn create(
    db: &D1Database,
    org_id: String,
    activation_ref: String,
    profile_ref: String,
    request_jti: String,
) -> worker::Result<Response> {
    let profile_row = load_profile(db, &org_id, &profile_ref).await?;
    let policy_refs: Vec<String> =
        serde_json::from_str(&profile_row.policy_refs_json).unwrap_or_default();
    if !policy_refs.is_empty() {
        return json(&PublicError::broker(BrokerError::Brk004), 409);
    }
    let profile: TestProfile = serde_json::from_str(&profile_row.canonical_profile_config_json)
        .map_err(|_| worker_rust_error("v2 profile"))?;
    let action_nonce = opaque_id("v2_action_")?;
    let action_nonce_hash = format!(
        "sha256:{}",
        hex::encode(Sha256::digest(action_nonce.as_bytes()))
    );
    let inserted = db.prepare("INSERT OR IGNORE INTO activation_intents_v2 (org_id, activation_ref, profile_ref, profile_version, request_jti, status, action_nonce_hash, standing_authority_ref, trusted_source_refs_json, policy_refs_json, cas_version) VALUES (?, ?, ?, '1', ?, 'awaiting_action', ?, ?, ?, ?, 0)")
        .bind(&[JsValue::from_str(&org_id),JsValue::from_str(&activation_ref),JsValue::from_str(&profile_ref),JsValue::from_str(&request_jti),JsValue::from_str(&action_nonce_hash),JsValue::from_str(&profile_row.standing_authority_ref),JsValue::from_str(&profile_row.trusted_source_refs_json),JsValue::from_str(&profile_row.policy_refs_json)])?.run().await?;
    if inserted
        .meta()
        .ok()
        .flatten()
        .and_then(|meta| meta.changes)
        .unwrap_or(0)
        == 0
    {
        return json(&PublicError::broker(BrokerError::Brk203), 409);
    }
    let kind = match profile.activation_kind.as_str() {
        "oauth" => "open_url",
        "secret" => "submit_private_material",
        "workload" => "present_workload_assertion",
        "external" => "bind_external_custodian",
        _ => return json(&PublicError::broker(BrokerError::Brk004), 409),
    };
    json(
        &serde_json::json!({"activation_ref":activation_ref,"kind":kind,"action_nonce":action_nonce,"endpoint":if kind=="open_url"{Some(profile.endpoint)}else{None}}),
        201,
    )
}

async fn oauth_callback(
    db: &D1Database,
    env: &Env,
    org_id: &str,
    activation_ref: &str,
    state: &str,
    code: &str,
    crash_phase: Option<String>,
) -> worker::Result<Response> {
    let row = load_activation(db, org_id, activation_ref).await?;
    if row.status == "active" {
        return completion(&row);
    }
    if !matches!(row.status.as_str(), "awaiting_action" | "action_claimed")
        || !constant_time_hash_matches(&row.action_nonce_hash, state.as_bytes())
        || code.is_empty()
    {
        return json(&PublicError::broker(BrokerError::Brk109), 409);
    }
    if crash_phase.as_deref() == Some("after_claim") {
        db.prepare("UPDATE activation_intents_v2 SET status='action_claimed',cas_version=cas_version+1 WHERE org_id=? AND activation_ref=? AND status='awaiting_action'")
            .bind(&[JsValue::from_str(org_id),JsValue::from_str(activation_ref)])?.run().await?;
        return json(&PublicError::unavailable(), 599);
    }
    let material = serde_json::to_vec(
        &serde_json::json!({"access_token":format!("synthetic-{}", &hash(code.as_bytes())[7..23])}),
    )
    .map_err(|_| worker_rust_error("v2 oauth"))?;
    finish_activation(db, env, org_id, activation_ref, &material).await
}

#[allow(clippy::too_many_arguments)]
async fn submit_private(
    db: &D1Database,
    env: &Env,
    org_id: &str,
    activation_ref: &str,
    submission_jti: &str,
    nonce_b64u: &str,
    ciphertext_b64u: &str,
    rotation: bool,
) -> worker::Result<Response> {
    let row = load_activation(db, org_id, activation_ref).await?;
    if !rotation && row.status != "awaiting_action" {
        return json(&PublicError::broker(BrokerError::Brk109), 409);
    }
    let request_hash = hash(format!("{nonce_b64u}\0{ciphertext_b64u}").as_bytes());
    let inserted = db.prepare("INSERT OR IGNORE INTO activation_private_replay_v2 (org_id,activation_ref,submission_jti,request_hash) VALUES (?,?,?,?)")
        .bind(&[JsValue::from_str(org_id),JsValue::from_str(activation_ref),JsValue::from_str(submission_jti),JsValue::from_str(&request_hash)])?.run().await?;
    if inserted
        .meta()
        .ok()
        .flatten()
        .and_then(|meta| meta.changes)
        .unwrap_or(0)
        == 0
    {
        return json(&PublicError::broker(BrokerError::Brk203), 409);
    }
    let material = open_submission(
        env,
        activation_ref,
        submission_jti,
        nonce_b64u,
        ciphertext_b64u,
    )?;
    let profile = load_test_profile(db, org_id, &row.profile_ref).await?;
    if profile.activation_kind == "workload" {
        let assertion: Value =
            serde_json::from_slice(&material).map_err(|_| worker_rust_error("workload"))?;
        if assertion.get("iss").and_then(Value::as_str) != Some(&profile.workload_issuer)
            || assertion.get("aud").and_then(Value::as_str) != Some(&profile.workload_audience)
            || assertion
                .get("nonce")
                .and_then(Value::as_str)
                .is_none_or(|nonce| {
                    !constant_time_hash_matches(&row.action_nonce_hash, nonce.as_bytes())
                })
        {
            return json(&PublicError::broker(BrokerError::Brk109), 409);
        }
        let exchanged =
            serde_json::to_vec(&serde_json::json!({"access_token":"synthetic-workload-token"}))
                .map_err(|_| worker_rust_error("workload"))?;
        return finish_activation(db, env, org_id, activation_ref, &exchanged).await;
    }
    finish_activation(db, env, org_id, activation_ref, &material).await
}

async fn external_bind(
    db: &D1Database,
    env: &Env,
    org_id: &str,
    activation_ref: &str,
    public_key: &str,
    signature: &str,
) -> worker::Result<Response> {
    let row = load_activation(db, org_id, activation_ref).await?;
    let profile = load_test_profile(db, org_id, &row.profile_ref).await?;
    if row.status != "awaiting_action"
        || profile.activation_kind != "external"
        || profile.external_public_key_b64u != public_key
    {
        return json(&PublicError::broker(BrokerError::Brk109), 409);
    }
    let key = decode_public_key(public_key).map_err(|_| worker_rust_error("external proof"))?;
    let challenge = profile_challenge(db, org_id, activation_ref).await?;
    verify_ed25519(&key, challenge.as_bytes(), signature)
        .map_err(|_| worker_rust_error("external proof"))?;
    finish_activation(
        db,
        env,
        org_id,
        activation_ref,
        br#"{"access_token":"external-custodian-reference"}"#,
    )
    .await
}

async fn finish_activation(
    db: &D1Database,
    env: &Env,
    org_id: &str,
    activation_ref: &str,
    material: &[u8],
) -> worker::Result<Response> {
    let row = load_activation(db, org_id, activation_ref).await?;
    if row.status == "active" {
        return completion(&row);
    }
    let profile = load_test_profile(db, org_id, &row.profile_ref).await?;
    let (nonce, ciphertext) = seal_v2_material(env, activation_ref, material)?;
    let connection_ref = format!("connection_{}", &hash(activation_ref.as_bytes())[7..39]);
    let authority_view_hash =
        hash(format!("{org_id}\0{connection_ref}\0{}", profile.profile_ref).as_bytes());
    db.prepare("UPDATE activation_intents_v2 SET status='active',connection_ref=?,authority_view_hash=?,material_generation=1,cas_version=cas_version+1 WHERE org_id=? AND activation_ref=? AND status IN ('awaiting_action','action_claimed')")
      .bind(&[JsValue::from_str(&connection_ref),JsValue::from_str(&authority_view_hash),JsValue::from_str(org_id),JsValue::from_str(activation_ref)])?.run().await?;
    let sealed = serde_json::to_vec(&serde_json::json!({"nonce":nonce,"ciphertext":ciphertext}))
        .map_err(|_| worker_rust_error("seal"))?;
    prepare_state_do(env, org_id, &connection_ref, 1, &sealed).await?;
    completion(&load_activation(db, org_id, activation_ref).await?)
}

async fn activate_fence(
    env: &Env,
    db: &D1Database,
    org_id: &str,
    activation_ref: &str,
) -> worker::Result<Response> {
    let row = load_activation(db, org_id, activation_ref).await?;
    if row.status != "active" {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    let connection = row
        .connection_ref
        .as_deref()
        .ok_or_else(|| worker_rust_error("v2 fence"))?;
    let generation = row.material_generation.unwrap_or(0);
    let fence=serde_json::to_vec(&serde_json::json!({"schema_version":"0.2","critical_fields":[],"extensions":{},"phase":"v2_authoritative","fence_generation":2,"v2_lease_ever_issued":true,"v2_rotation_ever_started":false,"v1_leasing_disabled":true,"active_v2_generation":generation,"cas_version":2})).map_err(|_|worker_rust_error("v2 fence"))?;
    let envelope = CredentialStateEnvelope {
        org_id: org_id.into(),
        connection_ref: connection.into(),
        command: CredentialStateCommand::ActivateV2ForTest {
            canonical_json: fence,
        },
    };
    let _: CredentialStateReply = credential_do(env, org_id, connection, &envelope).await?;
    json(
        &serde_json::json!({"phase":"v2_authoritative","production_authority":"v1"}),
        200,
    )
}

async fn dispatch(
    db: &D1Database,
    env: &Env,
    org_id: &str,
    activation_ref: &str,
    request_ref: &str,
    response: Value,
    crash_phase: Option<String>,
) -> worker::Result<Response> {
    let row = load_activation(db, org_id, activation_ref).await?;
    if row.status != "active" {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    let profile = load_test_profile(db, org_id, &row.profile_ref).await?;
    let connection_ref = row
        .connection_ref
        .as_deref()
        .ok_or_else(|| worker_rust_error("connection"))?;
    let read_envelope = CredentialStateEnvelope {
        org_id: org_id.into(),
        connection_ref: connection_ref.into(),
        command: CredentialStateCommand::Read,
    };
    let state: CredentialStateReply =
        credential_do(env, org_id, connection_ref, &read_envelope).await?;
    let fence: Value =
        serde_json::from_slice(&state.fence_json).map_err(|_| worker_rust_error("fence"))?;
    if fence.get("phase").and_then(Value::as_str) != Some("v2_authoritative") {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    let request_hash = hash(format!("{activation_ref}\0{request_ref}").as_bytes());
    if let Some(existing)=db.prepare("SELECT request_hash,phase,result_json FROM dispatch_outbox_v2 WHERE org_id=? AND request_ref=?").bind(&[JsValue::from_str(org_id),JsValue::from_str(request_ref)])?.first::<OutboxRow>(None).await? {
      if existing.request_hash!=request_hash{return json(&PublicError::broker(BrokerError::Brk203),409)}
      if let Some(result)=existing.result_json{return Response::ok(result).map(|r|r.with_headers(json_header()).with_status(200))}
      if existing.phase=="dispatched"||existing.phase=="ambiguous"{return json(&serde_json::json!({"outcome":"ambiguous"}),200)}
    } else {
      db.prepare("INSERT INTO dispatch_outbox_v2(org_id,request_ref,activation_ref,phase,request_hash) VALUES(?,?,?,'prepared',?)").bind(&[JsValue::from_str(org_id),JsValue::from_str(request_ref),JsValue::from_str(activation_ref),JsValue::from_str(&request_hash)])?.run().await?;
    }
    if crash_phase.as_deref() == Some("after_prepared") {
        return json(&PublicError::unavailable(), 599);
    }
    let material_envelope = CredentialStateEnvelope {
        org_id: org_id.into(),
        connection_ref: connection_ref.into(),
        command: CredentialStateCommand::ReadMaterialForTest {
            generation: row
                .material_generation
                .ok_or_else(|| worker_rust_error("material"))?,
        },
    };
    let material_reply: CredentialStateReply =
        credential_do(env, org_id, connection_ref, &material_envelope).await?;
    let sealed: Value = serde_json::from_slice(
        material_reply
            .sealed_material_for_internal_test
            .as_deref()
            .ok_or_else(|| worker_rust_error("material"))?,
    )
    .map_err(|_| worker_rust_error("material"))?;
    let material = open_v2_material(
        env,
        activation_ref,
        sealed
            .get("nonce")
            .and_then(Value::as_str)
            .ok_or_else(|| worker_rust_error("material"))?,
        sealed
            .get("ciphertext")
            .and_then(Value::as_str)
            .ok_or_else(|| worker_rust_error("material"))?,
    )?;
    let auth_profile = auth_profile(&profile).map_err(|_| worker_rust_error("profile"))?;
    let plan = FinalizedUnsignedRequest::validate(
        &auth_profile,
        "api",
        "POST",
        "/resource",
        vec![],
        vec![Header {
            name: "content-type".into(),
            value: "application/json".into(),
        }],
        br#"{"input":true}"#.to_vec(),
    )
    .map_err(|_| worker_rust_error("plan"))?
    .into_plan()
    .map_err(|_| worker_rust_error("plan"))?;
    let lease:broker_core::credential::CredentialLeaseV2=serde_json::from_value(serde_json::json!({"private_codec_version":"0.2","critical_fields":[],"extensions":{},"lease_ref":format!("lease_{request_ref}"),"effect_grant_hash":hash(request_ref.as_bytes()),"dispatch_attempt":1,"org_id":org_id,"connection_ref":row.connection_ref,"scheme_ref":auth_profile.scheme_ref,"auth_profile_pin":pin("profile"),"material_kind":"credential","authority_view_hash":row.authority_view_hash,"authority_epoch":1,"leased_material_generation":row.material_generation,"issued_at":"2026-01-01T00:00:00Z","expires_at":"2030-01-01T00:00:00Z","use_limit":1,"private_material_b64u":URL_SAFE_NO_PAD.encode(material)})).map_err(|_|worker_rust_error("lease"))?;
    let driver = ProfileAuthDriver::new(auth_profile, now_seconds)
        .map_err(|_| worker_rust_error("driver"))?;
    let authorized = authorize_with_driver(&driver, &plan, &lease, b"{}")
        .map_err(|_| worker_rust_error("driver"))?;
    let placement = authorized
        .with_transport_bytes(|bytes| serde_json::from_slice::<Value>(bytes))
        .map_err(|_| worker_rust_error("driver"))?;
    db.prepare("UPDATE dispatch_outbox_v2 SET phase='dispatched' WHERE org_id=? AND request_ref=?")
        .bind(&[JsValue::from_str(org_id), JsValue::from_str(request_ref)])?
        .run()
        .await?;
    if crash_phase.as_deref() == Some("after_dispatch") {
        db.prepare(
            "UPDATE dispatch_outbox_v2 SET phase='ambiguous' WHERE org_id=? AND request_ref=?",
        )
        .bind(&[JsValue::from_str(org_id), JsValue::from_str(request_ref)])?
        .run()
        .await?;
        return json(&PublicError::unavailable(), 599);
    }
    let raw = BoundedRawResponse::from_privileged_transport(
        serde_json::to_vec(&response).map_err(|_| worker_rust_error("firewall"))?,
    )
    .map_err(|_| worker_rust_error("firewall"))?;
    let result = match StrictResponseFirewall.apply(
        raw,
        &FirewallPolicy::Forbidden {
            sensitive_pointers: Default::default(),
        },
    ) {
        Ok(result) => result,
        Err(error) => return json(&PublicError::broker(error), 409),
    };
    let header_names = placement
        .get("headers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|header| header.get("name").and_then(Value::as_str))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let query_names = placement
        .get("query")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|query| query.get("name").and_then(Value::as_str))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let output = serde_json::json!({"outcome":"confirmed","scrubbed":result.scrubbed.value(),"auth_evidence":{"header_names":header_names,"query_names":query_names}});
    let output_json = serde_json::to_string(&output).map_err(|_| worker_rust_error("dispatch"))?;
    db.prepare("UPDATE dispatch_outbox_v2 SET phase='terminal',result_json=? WHERE org_id=? AND request_ref=?").bind(&[JsValue::from_str(&output_json),JsValue::from_str(org_id),JsValue::from_str(request_ref)])?.run().await?;
    json(&output, 200)
}

#[allow(clippy::too_many_arguments)]
async fn rotate(
    db: &D1Database,
    env: &Env,
    org_id: &str,
    activation_ref: &str,
    expected_cas: u64,
    submission_jti: &str,
    nonce: &str,
    ciphertext: &str,
    crash_phase: Option<String>,
) -> worker::Result<Response> {
    let row = load_activation(db, org_id, activation_ref).await?;
    if row.status != "active" || row.cas_version != expected_cas {
        return json(&PublicError::broker(BrokerError::Brk204), 409);
    }
    if crash_phase.as_deref() == Some("provider_uncertain") {
        db.prepare("UPDATE activation_intents_v2 SET status='restart_required',cas_version=cas_version+1 WHERE org_id=? AND activation_ref=? AND cas_version=?").bind(&[JsValue::from_str(org_id),JsValue::from_str(activation_ref),JsValue::from_f64(expected_cas as f64)])?.run().await?;
        return json(&PublicError::unavailable(), 599);
    }
    let material = open_submission(env, activation_ref, submission_jti, nonce, ciphertext)?;
    if crash_phase.as_deref() == Some("before_seal") {
        return json(&PublicError::unavailable(), 599);
    }
    let generation = row.material_generation.unwrap_or(1) + 1;
    let (sealed_nonce, sealed_ciphertext) = seal_v2_material(env, activation_ref, &material)?;
    let sealed = serde_json::to_vec(
        &serde_json::json!({"nonce":sealed_nonce,"ciphertext":sealed_ciphertext}),
    )
    .map_err(|_| worker_rust_error("rotation"))?;
    let connection_ref = row
        .connection_ref
        .as_deref()
        .ok_or_else(|| worker_rust_error("rotation"))?;
    let envelope = CredentialStateEnvelope {
        org_id: org_id.into(),
        connection_ref: connection_ref.into(),
        command: CredentialStateCommand::SealRotatedMaterialForTest {
            generation,
            sealed_envelope: sealed,
        },
    };
    let _: CredentialStateReply = credential_do(env, org_id, connection_ref, &envelope).await?;
    let updated=db.prepare("UPDATE activation_intents_v2 SET material_generation=?,cas_version=cas_version+1 WHERE org_id=? AND activation_ref=? AND cas_version=?").bind(&[JsValue::from_f64(generation as f64),JsValue::from_str(org_id),JsValue::from_str(activation_ref),JsValue::from_f64(expected_cas as f64)])?.run().await?;
    if updated
        .meta()
        .ok()
        .flatten()
        .and_then(|meta| meta.changes)
        .unwrap_or(0)
        != 1
    {
        return json(&PublicError::broker(BrokerError::Brk204), 409);
    }
    json(
        &serde_json::json!({"material_generation":generation,"cas_version":expected_cas+1}),
        200,
    )
}

async fn read(db: &D1Database, org_id: &str, activation_ref: &str) -> worker::Result<Response> {
    let row = load_activation(db, org_id, activation_ref).await?;
    json(
        &serde_json::json!({"profile_ref":row.profile_ref,"status":row.status,"connection_ref":row.connection_ref,"authority_view_hash":row.authority_view_hash,"material_generation":row.material_generation,"cas_version":row.cas_version}),
        200,
    )
}

fn completion(row: &ActivationRow) -> worker::Result<Response> {
    json(
        &serde_json::json!({"kind":"complete","connection_ref":row.connection_ref,"authority_view_hash":row.authority_view_hash,"material_generation":row.material_generation,"cas_version":row.cas_version}),
        200,
    )
}
async fn load_profile(db: &D1Database, org: &str, profile: &str) -> worker::Result<ProfileRow> {
    db.prepare("SELECT canonical_profile_config_json,standing_authority_ref,trusted_source_refs_json,policy_refs_json FROM activation_profiles_v2 WHERE org_id=? AND profile_ref=? AND profile_version='1'").bind(&[JsValue::from_str(org),JsValue::from_str(profile)])?.first::<ProfileRow>(None).await?.ok_or_else(||worker_rust_error("profile"))
}
async fn load_test_profile(
    db: &D1Database,
    org: &str,
    profile: &str,
) -> worker::Result<TestProfile> {
    let row = load_profile(db, org, profile).await?;
    serde_json::from_str(&row.canonical_profile_config_json)
        .map_err(|_| worker_rust_error("profile"))
}
async fn load_activation(
    db: &D1Database,
    org: &str,
    activation: &str,
) -> worker::Result<ActivationRow> {
    db.prepare("SELECT profile_ref,status,action_nonce_hash,connection_ref,authority_view_hash,material_generation,cas_version FROM activation_intents_v2 WHERE org_id=? AND activation_ref=?").bind(&[JsValue::from_str(org),JsValue::from_str(activation)])?.first::<ActivationRow>(None).await?.ok_or_else(||worker_rust_error("activation"))
}

fn open_submission(
    env: &Env,
    activation: &str,
    jti: &str,
    nonce: &str,
    ciphertext: &str,
) -> worker::Result<Vec<u8>> {
    crypt_open(
        env,
        nonce,
        ciphertext,
        format!("{activation}\0{jti}\0private-material-v2").as_bytes(),
    )
}
fn seal_v2_material(
    env: &Env,
    activation: &str,
    material: &[u8],
) -> worker::Result<(String, String)> {
    crypt_seal(
        env,
        material,
        format!("{activation}\0secret-envelope-v2").as_bytes(),
    )
}
fn open_v2_material(
    env: &Env,
    activation: &str,
    nonce: &str,
    ciphertext: &str,
) -> worker::Result<Vec<u8>> {
    crypt_open(
        env,
        nonce,
        ciphertext,
        format!("{activation}\0secret-envelope-v2").as_bytes(),
    )
}
fn crypt_seal(env: &Env, plaintext: &[u8], aad: &[u8]) -> worker::Result<(String, String)> {
    let key = secret_32(env, "CUSTODY_ROOT_KEY")?;
    let mut nonce = [0; 12];
    getrandom::getrandom(&mut nonce).map_err(|_| worker_rust_error("seal"))?;
    let ciphertext = ChaCha20Poly1305::new((&key).into())
        .encrypt(
            (&nonce).into(),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| worker_rust_error("seal"))?;
    Ok((
        URL_SAFE_NO_PAD.encode(nonce),
        URL_SAFE_NO_PAD.encode(ciphertext),
    ))
}
fn crypt_open(env: &Env, nonce: &str, ciphertext: &str, aad: &[u8]) -> worker::Result<Vec<u8>> {
    let nonce = URL_SAFE_NO_PAD
        .decode(nonce)
        .map_err(|_| worker_rust_error("open"))?;
    let nonce: [u8; 12] = <[u8; 12]>::try_from(nonce).map_err(|_| worker_rust_error("open"))?;
    let ciphertext = URL_SAFE_NO_PAD
        .decode(ciphertext)
        .map_err(|_| worker_rust_error("open"))?;
    let key = secret_32(env, "CUSTODY_ROOT_KEY")?;
    ChaCha20Poly1305::new((&key).into())
        .decrypt(
            (&nonce).into(),
            Payload {
                msg: &ciphertext,
                aad,
            },
        )
        .map_err(|_| worker_rust_error("open"))
}
fn constant_time_hash_matches(expected: &str, value: &[u8]) -> bool {
    let actual = format!("sha256:{}", hex::encode(Sha256::digest(value)));
    constant_time_bytes_equal(expected.as_bytes(), actual.as_bytes())
}
fn hash(value: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(value)))
}
async fn profile_challenge(db: &D1Database, org: &str, activation: &str) -> worker::Result<String> {
    let row = load_activation(db, org, activation).await?;
    Ok(row.action_nonce_hash)
}
fn pin(name: &str) -> RegistryPin {
    RegistryPin {
        entry_ref: name.into(),
        version: "1".into(),
        definition_hash: format!("sha256:{}", "a".repeat(64)),
        approval_epoch: 1,
        revocation_epoch: 0,
    }
}
fn auth_profile(profile: &TestProfile) -> Result<AuthProfile, BrokerError> {
    let scheme = match profile.scheme_kind.as_str() {
        "header" => AuthScheme::HeaderKey {
            name: profile.placement_name.clone(),
            prefix: profile.prefix.clone(),
        },
        "query" => AuthScheme::QueryKey {
            name: profile.placement_name.clone(),
        },
        "basic" => AuthScheme::Basic {
            header: "Authorization".into(),
        },
        "bearer" | "oauth" | "workload" | "external" => AuthScheme::Bearer {
            header: "Authorization".into(),
            prefix: "Bearer".into(),
        },
        _ => return Err(BrokerError::Brk004),
    };
    Ok(AuthProfile {
        profile_ref: profile.profile_ref.clone(),
        version: "1".into(),
        definition_hash: format!("sha256:{}", "b".repeat(64)),
        connector_ref: "connector.synthetic".into(),
        activation: ActivationKind::SecretSubmission,
        scheme_ref: format!("credential.synthetic.{}@1", profile.scheme_kind),
        scheme,
        endpoints: std::collections::BTreeMap::from([("api".into(), profile.endpoint.clone())]),
        callback_uri: "https://broker.invalid/v0.2/credential-callback".into(),
        connection_claims: std::collections::BTreeSet::new(),
        contract_claims: std::collections::BTreeMap::from([(
            "contract.synthetic".into(),
            std::collections::BTreeSet::from(["claim.synthetic".into()]),
        )]),
        lifecycle: std::collections::BTreeSet::from(["activate".into()]),
        material_schema_hash: format!("sha256:{}", "c".repeat(64)),
        assertion_schema_hash: None,
        claims_schema_hash: format!("sha256:{}", "d".repeat(64)),
        public_claims: PublicClaimsPolicy::None,
        auth_driver: pin("driver"),
        claim_normalizer: pin("normalizer"),
        custodian: pin("custodian"),
        transport: pin("transport"),
        response_firewall: pin("firewall"),
    })
}

async fn prepare_state_do(
    env: &Env,
    org: &str,
    connection: &str,
    generation: u64,
    sealed: &[u8],
) -> worker::Result<()> {
    let initial=serde_json::to_vec(&serde_json::json!({"schema_version":"0.2","critical_fields":[],"extensions":{},"phase":"v1_authoritative","fence_generation":0,"v2_lease_ever_issued":false,"v2_rotation_ever_started":false,"v1_leasing_disabled":false,"active_v2_generation":null,"cas_version":0})).map_err(|_|worker_rust_error("fence"))?;
    let init = CredentialStateEnvelope {
        org_id: org.into(),
        connection_ref: connection.into(),
        command: CredentialStateCommand::Initialize {
            fence_json: initial,
        },
    };
    let _: CredentialStateReply = credential_do(env, org, connection, &init).await?;
    let seal = CredentialStateEnvelope {
        org_id: org.into(),
        connection_ref: connection.into(),
        command: CredentialStateCommand::SealMaterial {
            generation,
            sealed_envelope: sealed.to_vec(),
        },
    };
    let _: CredentialStateReply = credential_do(env, org, connection, &seal).await?;
    let prepared=serde_json::to_vec(&serde_json::json!({"schema_version":"0.2","critical_fields":[],"extensions":{},"phase":"v2_prepared","fence_generation":1,"v2_lease_ever_issued":false,"v2_rotation_ever_started":false,"v1_leasing_disabled":false,"active_v2_generation":null,"cas_version":1})).map_err(|_|worker_rust_error("fence"))?;
    let advance = CredentialStateEnvelope {
        org_id: org.into(),
        connection_ref: connection.into(),
        command: CredentialStateCommand::AdvanceFence {
            canonical_json: prepared,
        },
    };
    let _: CredentialStateReply = credential_do(env, org, connection, &advance).await?;
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
fn json_header() -> worker::Headers {
    let headers = worker::Headers::new();
    let _ = headers.set("content-type", JSON_CONTENT_TYPE);
    headers
}

async fn ensure_schema(db: &D1Database) -> worker::Result<()> {
    for statement in [
        "CREATE TABLE IF NOT EXISTS activation_profiles_v2(org_id TEXT NOT NULL,profile_ref TEXT NOT NULL,profile_version TEXT NOT NULL,registry_definition_hash TEXT NOT NULL,canonical_profile_config_json TEXT NOT NULL,standing_authority_ref TEXT NOT NULL,trusted_source_refs_json TEXT NOT NULL,policy_refs_json TEXT NOT NULL,PRIMARY KEY(org_id,profile_ref,profile_version))",
        "CREATE TABLE IF NOT EXISTS activation_intents_v2(org_id TEXT NOT NULL,activation_ref TEXT NOT NULL,profile_ref TEXT NOT NULL,profile_version TEXT NOT NULL,request_jti TEXT NOT NULL,status TEXT NOT NULL,action_nonce_hash TEXT NOT NULL,standing_authority_ref TEXT NOT NULL,trusted_source_refs_json TEXT NOT NULL,policy_refs_json TEXT NOT NULL,connection_ref TEXT,authority_view_hash TEXT,material_generation INTEGER,cas_version INTEGER NOT NULL,PRIMARY KEY(org_id,activation_ref),UNIQUE(org_id,request_jti))",
        "CREATE TABLE IF NOT EXISTS activation_private_replay_v2(org_id TEXT NOT NULL,activation_ref TEXT NOT NULL,submission_jti TEXT NOT NULL,request_hash TEXT NOT NULL,terminal_result_hash TEXT,PRIMARY KEY(org_id,activation_ref,submission_jti))",
        "CREATE TABLE IF NOT EXISTS dispatch_outbox_v2(org_id TEXT NOT NULL,request_ref TEXT NOT NULL,activation_ref TEXT NOT NULL,phase TEXT NOT NULL,request_hash TEXT NOT NULL,result_json TEXT,PRIMARY KEY(org_id,request_ref))",
    ] {
        db.prepare(statement).run().await?;
    }
    Ok(())
}
