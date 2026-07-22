use super::*;

#[cfg(feature = "test-fixtures")]
pub(super) fn inject_activation_crash(request: &Request, phase: &str) -> bool {
    request
        .headers()
        .get(concat!("x-lattice-", "test-activation-crash"))
        .ok()
        .flatten()
        .as_deref()
        == Some(phase)
}

#[cfg(feature = "test-fixtures")]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureRequest {
    fixture: String,
    #[serde(default)]
    session_ref: Option<String>,
}

#[cfg(feature = "test-fixtures")]
pub(super) async fn provision_fixture(
    request: &mut Request,
    env: &Env,
) -> worker::Result<Response> {
    if env
        .var(concat!("LOCAL_", "TEST_MODE"))
        .ok()
        .map(|value| value.to_string())
        .as_deref()
        != Some("true")
    {
        return json(&PublicError::invalid(), 404);
    }
    let body: FixtureRequest = match bounded_json(request, MAX_MANAGEMENT_BODY).await {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    if body.fixture != concat!("google-semantic-", "broker-v1") {
        return json(&PublicError::broker(BrokerError::Brk109), 400);
    }
    ensure_test_schema(&env.d1("BROKER_DB")?).await?;
    let key = format!("lbk_{}", "a".repeat(64));
    let pepper = env.secret("KEY_HASH_PEPPER")?.to_string();
    let hash = hex::encode(keyed_hash(
        pepper.as_bytes(),
        b"deployment-key",
        key.as_bytes(),
    ));
    env.d1("BROKER_DB")?
        .prepare(
            "INSERT OR REPLACE INTO deployment_keys \
             (org_id, deployment_id, key_hash, expires_at, revoked) VALUES (?, ?, ?, ?, 0)",
        )
        .bind(&[
            JsValue::from_str("org-fixture"),
            JsValue::from_str("deployment-fixture"),
            JsValue::from_str(&hash),
            JsValue::from_f64((now_seconds() + 3600) as f64),
        ])?
        .run()
        .await?;
    let Some(session_ref) = body.session_ref else {
        // The key is a fixed documented local-only test input, not an arbitrary
        // secret echo. Production builds do not compile this route.
        return json(
            &serde_json::json!({
                "fixture":concat!("google-semantic-", "broker-v1"),
                "deployment_key":concat!("lbk_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            }),
            201,
        );
    };
    let db = env.d1("BROKER_DB")?;
    let session = db
        .prepare(
            "SELECT session_ref, org_id, deployment_id, pop_key_thumbprint, pop_public_key \
             FROM sessions WHERE session_ref = ? AND revoked = 0 LIMIT 1",
        )
        .bind(&[JsValue::from_str(&session_ref)])?
        .first::<SessionRow>(None)
        .await?;
    let Some(session) = session else {
        return json(&PublicError::broker(BrokerError::Brk102), 400);
    };
    let connection_ref = "connection_fixture_google";
    let binding_ref = "binding_fixture_google";
    let account_commitment =
        "hmac-sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let refresh_route = refresh_route(&session.org_id, connection_ref);
    let registration = ConnectionRegistration {
        org_id: session.org_id.clone(),
        connection_ref: connection_ref.into(),
        account_commitment: account_commitment.into(),
        granted_scopes: GOOGLE_SCOPES.iter().map(|scope| (*scope).into()).collect(),
        refresh_token: crate::refresh::SecretBytes::new(
            concat!("fixture-refresh-", "never-log").as_bytes().to_vec(),
        ),
        access_token: Some(crate::refresh::SecretBytes::new(
            concat!("fixture-access-", "never-log").as_bytes().to_vec(),
        )),
        access_expires_at: Some(0),
        revocation_epoch: 4,
    };
    let _: RefreshReply = do_request(
        env,
        "CONNECTION_REFRESH_DO",
        &refresh_route,
        &RefreshCommand::Register { registration },
    )
    .await?;
    db.prepare(
        "INSERT OR REPLACE INTO connections \
         (org_id, connection_ref, intent_ref, connector_ref, auth_profile_ref, execution_lane, custody, \
          account_commitment, actual_scopes_json, refresh_do_route, revocation_epoch, status) \
         VALUES (?, ?, 'intent_fixture_google', ?, ?, ?, ?, ?, ?, ?, 4, 'active')",
    )
    .bind(&[
        JsValue::from_str(&session.org_id),
        JsValue::from_str(connection_ref),
        JsValue::from_str(CONNECTOR_REF),
        JsValue::from_str(AUTH_PROFILE_REF),
        JsValue::from_str(EXECUTION_LANE),
        JsValue::from_str(CUSTODY),
        JsValue::from_str(account_commitment),
        JsValue::from_str(&serde_json::to_string(&GOOGLE_SCOPES).map_err(|_| worker_rust_error("scopes"))?),
        JsValue::from_str(&refresh_route),
    ])?
    .run()
    .await?;
    let binding_signer = broker_core::signing::BrokerSigner::from_seed(
        "broker-binding-v1",
        secret_32(env, "BINDING_SIGNING_SEED")?,
    );
    let placeholder = broker_core::artifacts::SignatureEnvelope {
        alg: broker_core::artifacts::SignatureAlg::Ed25519,
        key_id: binding_signer.key_id().into(),
        value: String::new(),
    };
    let mut fixture_attestation = broker_core::artifacts::BindingAttestation {
        schema_version: "0.1".into(),
        critical_fields: vec![],
        org_id: session.org_id.clone(),
        principal: broker_core::artifacts::PrincipalRef {
            kind: broker_core::artifacts::PrincipalKind::Broker,
            id: "broker-workers-v1".into(),
        },
        issuer: "broker-workers-v1".into(),
        broker_key_id: binding_signer.key_id().into(),
        lane: EXECUTION_LANE.into(),
        authority_manifest_hash: Some(
            "sha256:0000000000000000000000000000000000000000000000000000000000000000".into(),
        ),
        connection_ref: connection_ref.into(),
        provider: "google".into(),
        account_commitment: broker_core::artifacts::CommitmentEnvelope {
            alg: broker_core::artifacts::CommitmentAlg::HmacSha256,
            key_id: "account-commitment-v1".into(),
            verification_tier: None,
            value: account_commitment.into(),
            extensions: Default::default(),
        },
        roles: std::collections::BTreeMap::from([(
            "outbound_auth.google_workspace_auth".into(),
            "oauth2.access_token".into(),
        )]),
        scope_alignment: broker_core::artifacts::ScopeAlignment {
            required_scopes: GOOGLE_SCOPES.iter().map(|scope| (*scope).into()).collect(),
            actual_scopes: GOOGLE_SCOPES.iter().map(|scope| (*scope).into()).collect(),
            satisfied: true,
            extensions: Default::default(),
        },
        supported_contracts: vec![
            broker_core::artifacts::SupportedContract {
                contract_id: crate::protocol::SHEETS_CONTRACT_ID.into(),
                contract_hash: crate::protocol::SHEETS_CONTRACT_HASH.into(),
                observed_plugin_module_sha256: Some(
                    "sha256:54db6603967e1ce4e46ef45ece7bfa947c129564e40a290989fa2ae901a23966"
                        .into(),
                ),
                attenuation_profiles: vec![],
                extensions: Default::default(),
            },
            broker_core::artifacts::SupportedContract {
                contract_id: crate::protocol::GMAIL_CONTRACT_ID.into(),
                contract_hash: crate::protocol::GMAIL_CONTRACT_HASH.into(),
                observed_plugin_module_sha256: Some(
                    "sha256:5a5de77f756b49aac0fb5339bf764f9e53a9437cdc41619c2c978aa5dbb4e3fc"
                        .into(),
                ),
                attenuation_profiles: vec![],
                extensions: Default::default(),
            },
        ],
        endpoint_origins: vec![
            "https://gmail.googleapis.com".into(),
            "https://sheets.googleapis.com".into(),
        ],
        revocation_epoch: 4,
        not_before: now_rfc3339(),
        observed_at: now_rfc3339(),
        expires_at: rfc3339_from_seconds(now_seconds() + 3600),
        signature: placeholder,
        extensions: std::collections::BTreeMap::from([
            (
                "bundle_id".into(),
                serde_json::Value::String("bundle-fixture".into()),
            ),
            (
                "flow_ir_hash".into(),
                serde_json::Value::String(format!("sha256:{}", "2".repeat(64))),
            ),
            (
                "binding_lock_hash".into(),
                serde_json::Value::String(format!("sha256:{}", "3".repeat(64))),
            ),
            (
                "flow_id".into(),
                serde_json::Value::String("flow-fixture".into()),
            ),
            (
                "deployment_id".into(),
                serde_json::Value::String(session.deployment_id.clone()),
            ),
        ]),
    };
    let unsigned = broker_core::canonical::from_serde(
        &fixture_attestation,
        broker_core::artifacts::BINDING_MAX,
    )
    .map_err(|_| worker_rust_error("fixture binding"))?
    .into_bytes();
    fixture_attestation.signature = binding_signer
        .sign_json(broker_core::signing::BINDING_DOMAIN, &unsigned)
        .map_err(|_| worker_rust_error("fixture binding"))?;
    let fixture_attestation = broker_core::canonical::from_serde(
        &fixture_attestation,
        broker_core::artifacts::BINDING_MAX,
    )
    .map_err(|_| worker_rust_error("fixture binding"))?
    .into_bytes();
    db.prepare(
        "INSERT OR REPLACE INTO bindings \
         (org_id, binding_ref, connection_ref, deployment_id, bundle_id, flow_ir_hash, \
          binding_lock_hash, flow_id, contract_set_json, flow_ir_json, authority_manifest_json, \
          authority_manifest_hash, attestation_json, revoked) \
         VALUES (?, ?, ?, ?, 'bundle-fixture', ?, ?, 'flow-fixture', ?, '{}', '{}', \
                 'sha256:0000000000000000000000000000000000000000000000000000000000000000', ?, 0)",
    )
    .bind(&[
        JsValue::from_str(&session.org_id),
        JsValue::from_str(binding_ref),
        JsValue::from_str(connection_ref),
        JsValue::from_str(&session.deployment_id),
        JsValue::from_str(&format!("sha256:{}", "2".repeat(64))),
        JsValue::from_str(&format!("sha256:{}", "3".repeat(64))),
        JsValue::from_str(
            &serde_json::to_string(&[
                crate::protocol::SHEETS_CONTRACT_ID,
                crate::protocol::GMAIL_CONTRACT_ID,
            ])
            .map_err(|_| worker_rust_error("fixture"))?,
        ),
        JsValue::from_str(
            std::str::from_utf8(&fixture_attestation).map_err(|_| worker_rust_error("fixture"))?,
        ),
    ])?
    .run()
    .await?;
    let mut grants = serde_json::Map::new();
    for (name, contract_id, contract_hash, scopes) in [
        (
            "sheets",
            crate::protocol::SHEETS_CONTRACT_ID,
            crate::protocol::SHEETS_CONTRACT_HASH,
            GOOGLE_SCOPES
                .iter()
                .map(|scope| (*scope).to_string())
                .collect(),
        ),
        (
            "gmail",
            crate::protocol::GMAIL_CONTRACT_ID,
            crate::protocol::GMAIL_CONTRACT_HASH,
            GOOGLE_SCOPES
                .iter()
                .map(|scope| (*scope).to_string())
                .collect(),
        ),
    ] {
        let grant_ref = format!("grant_fixture_{name}_0000000000000001");
        let grant = broker_core::artifacts::ExecutionGrant {
            schema_version: "0.1".into(),
            critical_fields: vec![],
            org_id: session.org_id.clone(),
            principal: broker_core::artifacts::PrincipalRef {
                kind: broker_core::artifacts::PrincipalKind::Deployment,
                id: session.deployment_id.clone(),
            },
            grant_ref: grant_ref.clone(),
            authority_manifest_hash: Some(
                "sha256:0000000000000000000000000000000000000000000000000000000000000000".into(),
            ),
            issuer: "broker-workers-fixture".into(),
            audience: "broker-execution".into(),
            channel_binding: broker_core::artifacts::ChannelBinding {
                method: broker_core::artifacts::ChannelMethod::WorkersPrivateBinding,
                key_thumbprint: session.pop_key_thumbprint.clone(),
                session_id: session.session_ref.clone(),
            },
            subject: broker_core::artifacts::GrantSubject::FlowNodeRun {
                bundle_id: "bundle-fixture".into(),
                flow_ir_hash: format!("sha256:{}", "2".repeat(64)),
                binding_lock_hash: format!("sha256:{}", "3".repeat(64)),
                flow_id: "flow-fixture".into(),
                node_id: format!("node-{name}"),
                node_alias: name.into(),
                run_id: "run-fixture".into(),
            },
            operation_contract: contract_id.into(),
            contract_hash: contract_hash.into(),
            connection_ref: connection_ref.into(),
            provider: "google".into(),
            account_commitment: broker_core::artifacts::CommitmentEnvelope {
                alg: broker_core::artifacts::CommitmentAlg::HmacSha256,
                key_id: "account-commitment-v1".into(),
                verification_tier: None,
                value: account_commitment.into(),
                extensions: Default::default(),
            },
            roles: std::collections::BTreeMap::from([(
                "outbound_auth.google_workspace_auth".into(),
                "oauth2.access_token".into(),
            )]),
            scopes,
            budgets: broker_core::artifacts::GrantBudgets {
                logical_calls: 2,
                dispatch_attempts_per_call: 1,
            },
            aggregate_budgets: None,
            minimum_assurance: broker_core::artifacts::Assurance::BrokeredCount,
            required_attenuations: vec![],
            revocation_epoch: 4,
            not_before: rfc3339_from_seconds(now_seconds() - 60),
            expires_at: rfc3339_from_seconds(now_seconds() + 300),
            jti: format!("jti_fixture_{name}_00000000000000000001"),
            extensions: Default::default(),
        };
        let canonical =
            broker_core::canonical::from_serde(&grant, broker_core::artifacts::GRANT_MAX)
                .map_err(|_| worker_rust_error("fixture grant"))?
                .into_bytes();
        broker_core::grant::ExecutionGrantRecord::parse_canonical(&canonical)
            .map_err(|_| worker_rust_error("fixture grant"))?;
        db.prepare(
            "INSERT OR REPLACE INTO grants \
             (org_id, grant_ref, binding_ref, canonical_grant, node_id, operation_contract, \
              allocated_logical_calls, flow_aggregate_limit, connection_aggregate_key, \
              connection_aggregate_limit, expires_at, revoked) \
             VALUES (?, ?, ?, ?, ?, ?, 2, NULL, NULL, NULL, ?, 0)",
        )
        .bind(&[
            JsValue::from_str(&session.org_id),
            JsValue::from_str(&grant_ref),
            JsValue::from_str(binding_ref),
            JsValue::from_str(&String::from_utf8_lossy(&canonical)),
            JsValue::from_str(&format!("node-{name}")),
            JsValue::from_str(contract_id),
            JsValue::from_f64((now_seconds() + 300) as f64),
        ])?
        .run()
        .await?;
        grants.insert(name.into(), serde_json::Value::String(grant_ref));
    }
    json(
        &serde_json::json!({
            "fixture": concat!("google-semantic-", "broker-v1"),
            "connection_ref": connection_ref,
            "grants": grants
        }),
        201,
    )
}

#[cfg(feature = "test-fixtures")]
pub(super) async fn credential_state_v2(
    request: &mut Request,
    env: &Env,
) -> worker::Result<Response> {
    if env
        .var(concat!("LOCAL_", "TEST_MODE"))
        .ok()
        .map(|value| value.to_string())
        .as_deref()
        != Some("true")
    {
        return json(&PublicError::invalid(), 404);
    }
    let envelope: CredentialStateEnvelope = match bounded_json(request, MAX_MANAGEMENT_BODY).await {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let mut hash = Sha256::new();
    hash.update(b"lattice.credential-state.v2");
    hash.update([0]);
    hash.update(envelope.org_id.as_bytes());
    hash.update([0]);
    hash.update(envelope.connection_ref.as_bytes());
    let route = format!("credential-{}", hex::encode(hash.finalize()));
    match do_request::<CredentialStateReply>(env, "CREDENTIAL_STATE_V2_DO", &route, &envelope).await
    {
        Ok(reply) => json(&reply, 200),
        Err(_) => json(&PublicError::broker(BrokerError::Brk401), 409),
    }
}

#[cfg(feature = "test-fixtures")]
async fn ensure_test_schema(db: &D1Database) -> worker::Result<()> {
    for statement in include_str!("../../migrations/0001_broker.sql").split(';') {
        let statement = statement.trim();
        if !statement.is_empty() {
            db.prepare(statement).run().await?;
        }
    }
    Ok(())
}
