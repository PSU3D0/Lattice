use std::collections::BTreeMap;

use broker_core::{
    BrokerError,
    artifacts::{
        Assurance, BINDING_MAX, BindingAttestation, ChannelBinding, ChannelMethod, CommitmentAlg,
        CommitmentEnvelope, ExecutionGrant, GrantBudgets, GrantSubject, PluginTrustTier,
        PrincipalKind, PrincipalRef, ScopeAlignment, SignatureAlg, SignatureEnvelope,
        SupportedContract,
    },
    canonical,
    commitment::CommitmentKey,
    dispatch::ScriptedDispatch,
    engine::ImplementationApproval,
    grant::{ExecutionGrantRecord, FixedClock, PopSession},
    signing::{BINDING_DOMAIN, BrokerSigner},
};
use broker_host::{
    AuthorityManifestError, BrokerBindingEvidence, BrokerHostError, BrokerOperation,
    ConnectorExecutor, LocalBrokerConfig, LocalBrokerExecutor, LocalContractApproval,
    ManifestProvenance, NodeOperationMetadata, RemoteBrokerExecutor, TestBrokerTransport,
    TrustedHostScope, UnavailableBrokerTransport, derive_authority_manifest,
};
use dag_core::prelude::Version;
use dag_core::{
    BrokerAuthority, BrokerContractMetadata, BrokerOperationBudget, ConnectorOpMetadata,
    ConnectorResolutionContract, ConnectorResolutionModeDecl, Determinism, Effects, FlowBuilder,
    NodeSpec, Profile, SchemaSpec,
};

const FLOW_HASH: &str = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
const LOCK_HASH: &str = "sha256:3333333333333333333333333333333333333333333333333333333333333333";
const LLM_HASH: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const APPEND_HASH: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SEND_HASH: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SUPPORTED: &[ConnectorResolutionModeDecl] = &[ConnectorResolutionModeDecl::BoundConnection];

static LLM_OP: ConnectorOpMetadata = ConnectorOpMetadata {
    operation_id: "connector.synthetic.llm_complete",
    connector_id: "connector.synthetic",
    summary: "synthetic llm",
    min_effects: Effects::Effectful,
    max_determinism: Determinism::Nondeterministic,
    determinism_hints: &[],
    effect_hints: &[],
    roles: &[],
    resolution: ConnectorResolutionContract {
        supported_modes: SUPPORTED,
        default_mode: ConnectorResolutionModeDecl::BoundConnection,
    },
};

fn validated_manifest_flow() -> kernel_plan::ValidatedIR {
    let spec = NodeSpec::inline(
        "test.synthetic",
        "Synthetic",
        SchemaSpec::Opaque,
        SchemaSpec::Opaque,
        Effects::Effectful,
        Determinism::Nondeterministic,
        None,
    );
    let mut builder = FlowBuilder::new(
        "broker-manifest",
        Version::parse("1.0.0").unwrap(),
        Profile::Dev,
    );
    builder.add_node("llm", &spec).unwrap();
    let mut flow = builder.build();
    flow.nodes[0].connector_ops = vec![dag_core::ConnectorOpRefIR {
        operation_id: LLM_OP.operation_id.into(),
        connector_id: LLM_OP.connector_id.into(),
        roles: vec![],
        default_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
        selected_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
        supported_resolution_modes: SUPPORTED.to_vec(),
    }];
    flow.nodes[0].broker_authority = Some(
        BrokerAuthority::new(
            vec![BrokerOperationBudget {
                contract_id: "connector.synthetic.llm_complete@1".into(),
                semantic_effect_slots: vec!["complete".into()],
                max_logical_calls: 2,
                max_dispatch_attempts_per_call: 1,
                connection_aggregate_key: Some("synthetic-primary".into()),
            }],
            Some(4),
            BTreeMap::from([("synthetic-primary".into(), 3)]),
        )
        .unwrap(),
    );
    kernel_plan::validate(&flow).unwrap()
}

fn contracts() -> Vec<(&'static str, &'static str)> {
    vec![
        ("connector.synthetic.llm_complete@1", LLM_HASH),
        ("connector.synthetic.append@1", APPEND_HASH),
        ("connector.synthetic.send@1", SEND_HASH),
    ]
}

fn signed_binding(signer: &BrokerSigner) -> Vec<u8> {
    let placeholder = SignatureEnvelope {
        alg: SignatureAlg::Ed25519,
        key_id: signer.key_id().into(),
        value: String::new(),
    };
    let mut binding = BindingAttestation {
        schema_version: "0.1".into(),
        critical_fields: vec![],
        org_id: "org".into(),
        principal: PrincipalRef {
            kind: PrincipalKind::Broker,
            id: "broker".into(),
        },
        issuer: "issuer".into(),
        broker_key_id: signer.key_id().into(),
        lane: "semantic_broker".into(),
        connection_ref: "connection".into(),
        provider: "synthetic".into(),
        account_commitment: CommitmentEnvelope {
            alg: CommitmentAlg::HmacSha256,
            key_id: "account-key".into(),
            verification_tier: None,
            value: format!("hmac-sha256:{}", "0".repeat(64)),
            extensions: Default::default(),
        },
        roles: BTreeMap::from([("outbound_auth.synthetic".into(), "synthetic.secret".into())]),
        scope_alignment: ScopeAlignment {
            required_scopes: vec!["synthetic.write".into()],
            actual_scopes: vec!["synthetic.write".into()],
            satisfied: true,
            extensions: Default::default(),
        },
        supported_contracts: contracts()
            .into_iter()
            .map(|(id, hash)| SupportedContract {
                contract_id: id.into(),
                contract_hash: hash.into(),
                observed_plugin_module_sha256: None,
                attenuation_profiles: vec![],
                extensions: Default::default(),
            })
            .collect(),
        endpoint_origins: vec!["https://provider.example".into()],
        revocation_epoch: 4,
        observed_at: "2026-07-19T11:00:00Z".into(),
        expires_at: "2026-07-19T13:00:00Z".into(),
        signature: placeholder,
        extensions: Default::default(),
    };
    let unsigned = canonical::from_serde(&binding, BINDING_MAX)
        .unwrap()
        .into_bytes();
    binding.signature = signer.sign_json(BINDING_DOMAIN, &unsigned).unwrap();
    canonical::from_serde(&binding, BINDING_MAX)
        .unwrap()
        .into_bytes()
}

fn evidence(signer: &BrokerSigner, aliases: &[&str]) -> BrokerBindingEvidence {
    let bytes = signed_binding(signer);
    let mut evidence = BrokerBindingEvidence::new();
    for alias in aliases {
        evidence
            .insert(*alias, &bytes, signer.verifying_key())
            .unwrap();
    }
    evidence
}

fn approval(hash: &str) -> ImplementationApproval {
    ImplementationApproval {
        contract_hash: hash.into(),
        implementation: "synthetic-v1".into(),
        plugin_module_sha256:
            "sha256:5555555555555555555555555555555555555555555555555555555555555555".into(),
        trust_tier: PluginTrustTier::LatticeFirstParty,
        policy_hash: "sha256:6666666666666666666666666666666666666666666666666666666666666666"
            .into(),
    }
}

fn local_executor(
    evidence: BrokerBindingEvidence,
    scripts: Vec<ScriptedDispatch>,
) -> LocalBrokerExecutor {
    LocalBrokerExecutor::new(LocalBrokerConfig {
        evidence,
        contracts: contracts()
            .into_iter()
            .map(|(id, hash)| LocalContractApproval {
                contract_id: id.into(),
                approval: approval(hash),
            })
            .collect(),
        clock: FixedClock("2026-07-19T12:00:00Z".into()),
        dispatch_scripts: scripts,
        request_template: br#"{"idempotency":{"$broker":"idempotency_key"},"method":"POST"}"#
            .to_vec(),
        authority_facts: br#"{"allowed":true}"#.to_vec(),
        implementation: "synthetic-v1".into(),
        endpoint_origin: "https://provider.example".into(),
        connection_ref: "connection".into(),
        provider: "synthetic".into(),
        account_subject: "account".into(),
        scopes: vec!["synthetic.write".into()],
        synthetic_secret: b"test-only-secret".to_vec(),
        expected_pop_proof: b"proof".to_vec(),
        receipt_signer: BrokerSigner::from_seed("receipt-key", [2; 32]),
        commitments: CommitmentKey::new("commit-key", [8; 32]).unwrap(),
        broker_principal_id: "broker".into(),
    })
    .unwrap()
}

fn scope(node: &str, alias: &str, activation: u64) -> TrustedHostScope {
    TrustedHostScope::from_host_execution_identity(
        "org",
        "deployment",
        "bundle",
        FLOW_HASH,
        LOCK_HASH,
        "flow",
        node,
        alias,
        "run",
        activation,
    )
    .unwrap()
}

fn operation(node: &str, alias: &str, contract: &str, hash: &str) -> BrokerOperation {
    let pop = PopSession {
        method: ChannelMethod::DeploymentKey,
        key_thumbprint: "sha256:4444444444444444444444444444444444444444444444444444444444444444"
            .into(),
        session_id: "session".into(),
        proof: b"proof".to_vec(),
    };
    let grant = ExecutionGrantRecord::from_grant(&ExecutionGrant {
        schema_version: "0.1".into(),
        critical_fields: vec![],
        org_id: "org".into(),
        principal: PrincipalRef {
            kind: PrincipalKind::Deployment,
            id: "deployment".into(),
        },
        grant_ref: format!("grant-{alias}"),
        issuer: "issuer".into(),
        audience: "broker-execution".into(),
        channel_binding: ChannelBinding {
            method: pop.method.clone(),
            key_thumbprint: pop.key_thumbprint.clone(),
            session_id: pop.session_id.clone(),
        },
        subject: GrantSubject::FlowNodeRun {
            bundle_id: "bundle".into(),
            flow_ir_hash: FLOW_HASH.into(),
            binding_lock_hash: LOCK_HASH.into(),
            flow_id: "flow".into(),
            node_id: node.into(),
            node_alias: alias.into(),
            run_id: "run".into(),
        },
        operation_contract: contract.into(),
        contract_hash: hash.into(),
        connection_ref: "connection".into(),
        provider: "synthetic".into(),
        account_commitment: CommitmentEnvelope {
            alg: CommitmentAlg::HmacSha256,
            key_id: "account-key".into(),
            verification_tier: None,
            value: format!("hmac-sha256:{}", "0".repeat(64)),
            extensions: Default::default(),
        },
        roles: BTreeMap::from([("outbound_auth.synthetic".into(), "synthetic.secret".into())]),
        scopes: vec!["synthetic.write".into()],
        budgets: GrantBudgets {
            logical_calls: 1,
            dispatch_attempts_per_call: 1,
        },
        aggregate_budgets: None,
        minimum_assurance: Assurance::BrokeredCount,
        required_attenuations: vec![],
        revocation_epoch: 4,
        not_before: "2026-07-19T11:55:00Z".into(),
        expires_at: "2026-07-19T12:05:00Z".into(),
        jti: format!("jti-{alias}"),
        extensions: Default::default(),
    })
    .unwrap();
    BrokerOperation {
        contract_id: contract.into(),
        contract_hash: hash.into(),
        connection_alias: alias.into(),
        semantic_effect_slot: "effect".into(),
        grant,
        pop,
        current_revocation_epoch: 4,
    }
}

fn confirmed(id: &str) -> ScriptedDispatch {
    ScriptedDispatch::Confirmed {
        projection: br#"{"ok":true}"#.to_vec(),
        provider_request_id: Some(id.into()),
    }
}

#[test]
fn manifest_and_binding_preflight_are_pure_and_fail_closed() {
    let validated = validated_manifest_flow();
    let metadata = [NodeOperationMetadata {
        node_alias: "llm",
        operation: &LLM_OP,
        contract: Some(BrokerContractMetadata {
            contract_id: "connector.synthetic.llm_complete@1",
            contract_hash: LLM_HASH,
        }),
    }];
    let manifest = derive_authority_manifest(
        &validated,
        FLOW_HASH,
        ManifestProvenance {
            org_id: "org",
            principal_kind: PrincipalKind::Service,
            principal_id: "bundle-builder",
        },
        &metadata,
    )
    .unwrap();
    let op = &manifest.nodes["llm"].operations[0];
    assert_eq!(op.call_budget.max_logical_calls, 2);
    assert_eq!(op.call_budget.max_dispatch_attempts_per_call, 1);
    assert_eq!(op.contract_hash, LLM_HASH);
    let ceilings = manifest.aggregate_ceilings.unwrap();
    assert_eq!(ceilings.flow.unwrap().max_logical_calls, 4);
    assert_eq!(
        ceilings.connections["synthetic-primary"].max_logical_calls,
        3
    );

    let missing_contract = [NodeOperationMetadata {
        contract: None,
        ..metadata[0]
    }];
    assert_eq!(
        derive_authority_manifest(
            &validated,
            FLOW_HASH,
            ManifestProvenance {
                org_id: "org",
                principal_kind: PrincipalKind::Service,
                principal_id: "builder",
            },
            &missing_contract,
        )
        .unwrap_err(),
        AuthorityManifestError::MissingContractMetadata
    );

    let signer = BrokerSigner::from_seed("binding-key", [1; 32]);
    let verification_evidence = evidence(&signer, &["llm"]);
    verification_evidence
        .verify(
            "llm",
            "connector.synthetic.llm_complete@1",
            LLM_HASH,
            4,
            "2026-07-19T12:00:00Z",
        )
        .unwrap();
    let executor = local_executor(evidence(&signer, &["llm"]), vec![]);
    assert_eq!(executor.dispatch_count(), 0);
    assert_eq!(executor.custodian_access_count(), 0);
    assert_eq!(
        verification_evidence
            .verify(
                "absent",
                "connector.synthetic.llm_complete@1",
                LLM_HASH,
                4,
                "2026-07-19T12:00:00Z",
            )
            .unwrap_err(),
        BrokerHostError::MissingBindingEvidence
    );
    assert_eq!(
        verification_evidence
            .verify(
                "llm",
                "connector.synthetic.llm_complete@1",
                LLM_HASH,
                4,
                "2026-07-19T14:00:00Z",
            )
            .unwrap_err(),
        BrokerHostError::Broker(BrokerError::Brk105)
    );

    let mut tampered: BindingAttestation =
        serde_json::from_slice(&signed_binding(&signer)).unwrap();
    tampered.signature.value.replace_range(..1, "A");
    let tampered = canonical::from_serde(&tampered, BINDING_MAX)
        .unwrap()
        .into_bytes();
    let mut invalid_evidence = BrokerBindingEvidence::new();
    invalid_evidence
        .insert("llm", &tampered, signer.verifying_key())
        .unwrap();
    assert_eq!(
        invalid_evidence
            .verify(
                "llm",
                "connector.synthetic.llm_complete@1",
                LLM_HASH,
                4,
                "2026-07-19T12:00:00Z",
            )
            .unwrap_err(),
        BrokerHostError::Broker(BrokerError::Brk109)
    );
}

#[test]
fn s21_shaped_local_path_enforces_scope_budget_replay_and_redelivery() {
    let binding_signer = BrokerSigner::from_seed("binding-key", [1; 32]);
    let executor = local_executor(
        evidence(&binding_signer, &["llm", "append", "send"]),
        vec![
            confirmed("llm-1"),
            confirmed("append-1"),
            confirmed("send-1"),
        ],
    );
    let llm = operation(
        "node-llm",
        "llm",
        "connector.synthetic.llm_complete@1",
        LLM_HASH,
    );
    let append = operation(
        "node-append",
        "append",
        "connector.synthetic.append@1",
        APPEND_HASH,
    );
    let send = operation("node-send", "send", "connector.synthetic.send@1", SEND_HASH);

    let llm_result = executor
        .invoke(&scope("node-llm", "llm", 1), &llm, br#"{"prompt":"cv"}"#)
        .unwrap();
    let append_result = executor
        .invoke(
            &scope("node-append", "append", 1),
            &append,
            br#"{"row":"candidate"}"#,
        )
        .unwrap();
    let send_scope = scope("node-send", "send", 1);
    let send_input = br#"{"message":"decision"}"#;
    let dispatches_before_send = executor.dispatch_count();
    let send_result = executor.invoke(&send_scope, &send, send_input).unwrap();
    assert_eq!(executor.dispatch_count(), dispatches_before_send + 1);
    for result in [&llm_result, &append_result, &send_result] {
        assert!(result.receipt.claims.provider_dispatch_observed);
        assert!(!result.receipt.claims.remote_durable_state_proven);
        assert!(!result.receipt.claims.verifiable_execution_proven);
    }
    let dispatches_after_send = executor.dispatch_count();
    let redelivery = executor.invoke(&send_scope, &send, send_input).unwrap();
    assert!(redelivery.redelivery);
    assert_eq!(redelivery.canonical_receipt, send_result.canonical_receipt);
    assert_eq!(executor.dispatch_count(), dispatches_after_send);

    assert_eq!(
        executor
            .invoke(&send_scope, &send, br#"{"message":"altered"}"#)
            .unwrap_err(),
        BrokerHostError::Broker(BrokerError::Brk203)
    );
    assert_eq!(
        executor
            .invoke(&scope("node-send", "send", 2), &send, send_input)
            .unwrap_err(),
        BrokerHostError::Broker(BrokerError::Brk201)
    );

    let forged = br#"{"_lattice_broker_scope":{"activation_ordinal":1,"binding_lock_hash":"sha256:3333333333333333333333333333333333333333333333333333333333333333","bundle_id":"bundle","flow_id":"wrong","flow_ir_hash":"sha256:2222222222222222222222222222222222222222222222222222222222222222","node_alias":"send","node_id":"node-send","org_id":"org","principal_id":"deployment","run_id":"run"},"message":"decision"}"#;
    assert_eq!(
        executor.invoke(&send_scope, &send, forged).unwrap_err(),
        BrokerHostError::GuestScopeMismatch
    );

    // Ordinary guest fields that resemble scope are data, never authority.
    let ignored_scope_fields = br#"{"flow_id":"forged","node_id":"forged","value":1}"#;
    let separate = operation(
        "node-extra",
        "extra",
        "connector.synthetic.send@1",
        SEND_HASH,
    );
    let separate_executor = local_executor(
        evidence(&binding_signer, &["extra"]),
        vec![confirmed("extra-1")],
    );
    assert!(
        separate_executor
            .invoke(
                &scope("node-extra", "extra", 1),
                &separate,
                ignored_scope_fields,
            )
            .is_ok()
    );

    let unavailable = local_executor(BrokerBindingEvidence::new(), vec![confirmed("unused")]);
    assert_eq!(
        unavailable
            .invoke(&send_scope, &send, send_input)
            .unwrap_err(),
        BrokerHostError::MissingBindingEvidence
    );
    assert_eq!(unavailable.dispatch_count(), 0);
    assert_eq!(unavailable.custodian_access_count(), 0);

    let remote = RemoteBrokerExecutor::new(
        UnavailableBrokerTransport,
        evidence(&binding_signer, &["send"]),
        "2026-07-19T12:00:00Z",
    );
    assert_eq!(
        remote.invoke(&send_scope, &send, send_input).unwrap_err(),
        BrokerHostError::ExecutorUnavailable
    );

    let test_transport = TestBrokerTransport::new(std::iter::empty());
    let remote_missing = RemoteBrokerExecutor::new(
        test_transport,
        BrokerBindingEvidence::new(),
        "2026-07-19T12:00:00Z",
    );
    assert_eq!(
        remote_missing
            .invoke(&send_scope, &send, send_input)
            .unwrap_err(),
        BrokerHostError::MissingBindingEvidence
    );
    assert_eq!(remote_missing.transport().call_count(), 0);
}
