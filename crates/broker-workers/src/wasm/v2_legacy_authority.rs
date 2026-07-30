use super::v2_production::*;
use super::*;
use worker::durable_object;

pub(super) const V2_LEASE_STORE_KEY: &str = "broker:v2-node-lease-store:v1";
#[durable_object]
pub struct V2AuthorityDurableObject {
    pub(super) state: State,
    pub(super) env: Env,
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
pub(super) struct LegacyReconcileRequest {
    pub(super) session_ref: String,
    pub(super) legacy_connection_ref: String,
    pub(super) replacement_connection_ref: String,
    pub(super) expected_cas_version: u64,
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

pub(super) async fn complete_fresh_cutover(
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
pub(super) struct RevocationJournalRow {
    pub(super) phase: String,
    pub(super) provider_evidence_hash: Option<String>,
    pub(super) destruction_evidence_hash: Option<String>,
    pub(super) authority_epoch: u64,
    pub(super) canonical_journal_json: String,
}

pub(super) async fn revoke_connection_v2(
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
            db.prepare("UPDATE logical_bindings_v2 SET state='revoked',updated_at=? WHERE org_id=? AND connection_ref=? AND state='active'").bind(&[JsValue::from_f64(now_seconds() as f64),JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?,
            db.prepare("UPDATE binding_revisions_v2 SET state='revoked' WHERE org_id=? AND logical_binding_ref IN (SELECT logical_binding_ref FROM logical_bindings_v2 WHERE org_id=? AND connection_ref=? AND state='revoked')").bind(&[JsValue::from_str(&session.org_id),JsValue::from_str(&session.org_id),JsValue::from_str(&connection.connection_ref)])?,
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

pub(super) fn refresh_token_from_v2_envelope(
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

pub(super) struct ProviderRevocationEvidence {
    pub(super) evidence_hash: String,
    pub(super) canonical_json: String,
    pub(super) remote_proof_hash: Option<String>,
}

pub(super) async fn revoke_profile_material(
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
