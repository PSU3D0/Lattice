use super::v2_production::*;
use super::*;

pub(super) async fn finish_ambiguous(
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
pub(super) async fn finish_receipt(
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

pub(super) fn hash_from_commitment_context(
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
pub(super) fn receipt_commitment(
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

pub(super) fn commitment_value(env: &Env, purpose: &str, bytes: &[u8]) -> worker::Result<Value> {
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
