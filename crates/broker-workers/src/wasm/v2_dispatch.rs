use super::v2_production::*;
use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InvokeRequestV2 {
    pub(super) session_ref: String,
    pub(super) grant_ref: String,
    pub(super) input: Value,
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
    let lease_ref = row
        .parent_ref
        .as_deref()
        .ok_or_else(|| worker_rust_error("grant parent"))?;
    let lease_row = load_host_record(&db, &session.org_id, lease_ref, "node_lease")
        .await?
        .ok_or_else(|| worker_rust_error("grant parent"))?;
    let binding_ref = lease_row
        .parent_ref
        .as_deref()
        .ok_or_else(|| worker_rust_error("lease parent"))?;
    let lifecycle = load_binding_lifecycle(&db, &session.org_id, binding_ref).await?;
    if lifecycle.is_none_or(|value| {
        value.logical_state != "active"
            || !matches!(value.revision_state.as_str(), "active" | "superseded")
    }) {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    let live_connection = match load_connection(&db, &session.org_id, &row.connection_ref).await? {
        Some(value) if value.status == "active" && value.registry_is_current(&now_rfc3339()) => {
            value
        }
        _ => return json(&PublicError::broker(BrokerError::Brk106), 409),
    };
    let live_authority = parse::<ConnectionAuthorityViewV2>(
        live_connection.canonical_authority_view_json.as_bytes(),
    )
    .map_err(|_| worker_rust_error("live authority"))?;
    if grant.view.as_value()["authority_view_hash"]
        != serde_json::json!(live_authority.content_hash())
        || grant.view.as_value()["authority_epoch"]
            != live_authority.view.as_value()["authority_epoch"]
        || grant.view.as_value()["minimum_material_generation"].as_u64()
            != Some(live_connection.active_material_generation)
    {
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
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
pub(super) struct OutboxRow {
    pub(super) canonical_input_hash: String,
    pub(super) phase: String,
    pub(super) canonical_receipt_json: Option<String>,
    pub(super) response_projection_json: Option<String>,
}

pub(super) async fn execute_grant(
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
    let authority =
        parse::<ConnectionAuthorityViewV2>(connection.canonical_authority_view_json.as_bytes())
            .map_err(|_| worker_rust_error("authority"))?;
    if g["contract_hash"] != installed.contract_hash
        || g["authority_view_hash"] != connection.authority_view_hash_value()
        || g["authority_epoch"] != authority.view.as_value()["authority_epoch"]
        || g["minimum_material_generation"].as_u64() != Some(connection.active_material_generation)
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

pub(super) async fn dispatch_generic_driver(
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

pub(super) async fn lease_v2_access_token(
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
