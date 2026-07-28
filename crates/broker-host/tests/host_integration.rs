use std::collections::BTreeMap;

use broker_core::{
    BrokerError,
    artifacts::{
        BINDING_MAX, BindingAttestation, ChannelMethod, CommitmentAlg, CommitmentEnvelope,
        PluginTrustTier, PrincipalKind, PrincipalRef, ScopeAlignment, SignatureAlg,
        SignatureEnvelope, SupportedContract,
    },
    canonical,
    commitment::CommitmentKey,
    dispatch::ScriptedDispatch,
    engine::ImplementationApproval,
    grant::FixedClock,
    signing::{BINDING_DOMAIN, BrokerSigner},
};
use broker_host::{
    AuthorityManifestError, BindingLockRecord, BrokerBindingEvidence, BrokerDescriptorRegistry,
    BrokerHostError, ConnectorExecutor, HostBootstrapIdentity, LocalBrokerConfig,
    LocalBrokerExecutor, ManifestProvenance, NodeOperationMetadata, ReceiptTrustStore,
    RemoteBrokerExecutor, RemoteInvokeResponse, TestBrokerTransport, bootstrap_host_context,
    derive_authority_manifest,
};
use connector_spec::{
    BrokerDispatchDescriptor, BrokerRequestPlan, OperationContractDescriptor, QueryValueDecl,
    RequestMethod, RequestPlaceholderDecl, ResponseDataPolicy, ResponseDataPolicyKind,
    descriptor_hash,
};
use dag_core::prelude::Version;
use dag_core::{
    BrokerAuthority, BrokerContractMetadata, BrokerOperationBudget, ConnectorOpMetadata,
    ConnectorResolutionContract, ConnectorResolutionModeDecl, ControlSurfaceIR, ControlSurfaceKind,
    Determinism, Effects, FlowBuilder, NodeSpec, Profile, SchemaSpec,
};
use sha2::{Digest, Sha256};

const FLOW_HASH_PLACEHOLDER: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";
const LOCK_HASH: &str = "sha256:3333333333333333333333333333333333333333333333333333333333333333";
const SUPPORTED: &[ConnectorResolutionModeDecl] = &[ConnectorResolutionModeDecl::BoundConnection];

static OP: ConnectorOpMetadata = ConnectorOpMetadata {
    operation_id: "connector.synthetic.one",
    connector_id: "connector.synthetic",
    summary: "synthetic",
    min_effects: Effects::Effectful,
    max_determinism: Determinism::Nondeterministic,
    determinism_hints: &[],
    effect_hints: &[],
    broker_contract: None,
    roles: &[],
    resolution: ConnectorResolutionContract {
        supported_modes: SUPPORTED,
        default_mode: ConnectorResolutionModeDecl::BoundConnection,
    },
};

fn descriptor(id: &str, path: &str, slot: &str) -> (Vec<u8>, String) {
    let policy = ResponseDataPolicy {
        fields: vec!["ok".into()],
        kind: ResponseDataPolicyKind::JsonProjection,
        max_bytes: 1024,
    };
    let contract = OperationContractDescriptor {
        auth_role: "outbound_auth.synthetic".into(),
        broker_abi_version: "0.1".into(),
        contract_id: id.into(),
        effect_class: "effectful".into(),
        input_schema_hash: format!("sha256:{}", "1".repeat(64)),
        minimum_scopes: vec!["synthetic.write".into()],
        output_schema_hash: format!("sha256:{}", "2".repeat(64)),
        response_data_policy: policy.clone(),
        semantic_effect_slots: vec![slot.into()],
    };
    let hash = descriptor_hash(&contract).unwrap();
    let request_plan = BrokerRequestPlan {
        method: RequestMethod::Post,
        origin: "https://provider.example".into(),
        path_template: path.into(),
        placeholders: BTreeMap::from([(
            "effect_key".into(),
            RequestPlaceholderDecl {
                kind: "idempotency_key".into(),
                input_field: None,
            },
        )]),
        query: BTreeMap::from([
            (
                "at".into(),
                QueryValueDecl {
                    kind: "timestamp".into(),
                    input_field: None,
                    value: None,
                },
            ),
            (
                "boundary".into(),
                QueryValueDecl {
                    kind: "boundary".into(),
                    input_field: None,
                    value: None,
                },
            ),
            (
                "effect".into(),
                QueryValueDecl {
                    kind: "idempotency_key".into(),
                    input_field: None,
                    value: None,
                },
            ),
            (
                "mode".into(),
                QueryValueDecl {
                    kind: "static".into(),
                    input_field: None,
                    value: Some("strict".into()),
                },
            ),
            (
                "value".into(),
                QueryValueDecl {
                    kind: "input".into(),
                    input_field: Some("value".into()),
                    value: None,
                },
            ),
        ]),
        static_headers: BTreeMap::from([("Accept".into(), "application/json".into())]),
        body: BTreeMap::from([("value".into(), "value".into())]),
        trusted_adapter: None,
    };
    let request_plan_hash = connector_spec::request_plan_hash(&request_plan).unwrap();
    let descriptor = BrokerDispatchDescriptor {
        contract,
        contract_hash: hash.clone(),
        request_plan,
        request_plan_hash,
        response_data_policy: policy,
    };
    (serde_json::to_vec(&descriptor).unwrap(), hash)
}

fn approval(hash: &str, implementation: &str) -> ImplementationApproval {
    ImplementationApproval {
        contract_hash: hash.into(),
        implementation: implementation.into(),
        plugin_module_sha256: format!("sha256:{}", "5".repeat(64)),
        trust_tier: PluginTrustTier::LatticeFirstParty,
        policy_hash: format!("sha256:{}", "6".repeat(64)),
    }
}

fn registry() -> (BrokerDescriptorRegistry, Vec<(String, String)>) {
    let mut registry = BrokerDescriptorRegistry::new();
    let mut contracts = Vec::new();
    for (index, (id, path, slot)) in [
        ("connector.synthetic.one@1", "/v1/one/{effect_key}", "one"),
        ("connector.synthetic.two@1", "/v1/two/{effect_key}", "two"),
        (
            "connector.synthetic.three@1",
            "/v1/three/{effect_key}",
            "three",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let (bytes, hash) = descriptor(id, path, slot);
        registry
            .load_generated(&bytes, approval(&hash, &format!("impl-{index}")), 1, 1)
            .unwrap();
        contracts.push((id.into(), hash));
    }
    (registry, contracts)
}

fn account() -> CommitmentEnvelope {
    CommitmentEnvelope {
        alg: CommitmentAlg::HmacSha256,
        key_id: "account-key".into(),
        verification_tier: None,
        value: format!("hmac-sha256:{}", "0".repeat(64)),
        extensions: Default::default(),
    }
}

fn binding(
    signer: &BrokerSigner,
    contracts: &[(String, String)],
    mutate: impl FnOnce(&mut BindingAttestation),
) -> Vec<u8> {
    let placeholder = SignatureEnvelope {
        alg: SignatureAlg::Ed25519,
        key_id: signer.key_id().into(),
        value: String::new(),
    };
    let mut value = BindingAttestation {
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
        authority_manifest_hash: None,
        connection_ref: "connection".into(),
        provider: "synthetic".into(),
        account_commitment: account(),
        roles: BTreeMap::from([("outbound_auth.synthetic".into(), "synthetic.secret".into())]),
        scope_alignment: ScopeAlignment {
            required_scopes: vec!["synthetic.write".into()],
            actual_scopes: vec!["synthetic.write".into()],
            satisfied: true,
            extensions: Default::default(),
        },
        supported_contracts: contracts
            .iter()
            .map(|(id, hash)| SupportedContract {
                contract_id: id.clone(),
                contract_hash: hash.clone(),
                observed_plugin_module_sha256: Some(format!("sha256:{}", "5".repeat(64))),
                attenuation_profiles: vec![],
                extensions: Default::default(),
            })
            .collect(),
        endpoint_origins: vec!["https://provider.example".into()],
        revocation_epoch: 4,
        not_before: "2026-07-19T10:59:59Z".into(),
        observed_at: "2026-07-19T11:00:00Z".into(),
        expires_at: "2026-07-19T13:00:00Z".into(),
        signature: placeholder,
        extensions: Default::default(),
    };
    mutate(&mut value);
    let unsigned = canonical::from_serde(&value, BINDING_MAX)
        .unwrap()
        .into_bytes();
    value.signature = signer.sign_json(BINDING_DOMAIN, &unsigned).unwrap();
    canonical::from_serde(&value, BINDING_MAX)
        .unwrap()
        .into_bytes()
}

fn lock(signer: &BrokerSigner) -> BindingLockRecord {
    BindingLockRecord::from_binding_lock(
        "issuer",
        "binding-key",
        signer.verifying_key(),
        "org",
        "connection",
        "synthetic",
        account(),
        BTreeMap::from([("outbound_auth.synthetic".into(), "synthetic.secret".into())]),
        vec!["synthetic.write".into()],
        vec!["synthetic.write".into()],
    )
    .unwrap()
}

fn evidence(
    signer: &BrokerSigner,
    contracts: &[(String, String)],
    aliases: &[&str],
) -> BrokerBindingEvidence {
    let bytes = binding(signer, contracts, |_| {});
    let mut evidence = BrokerBindingEvidence::new();
    for alias in aliases {
        evidence
            .insert_lock_record(*alias, lock(signer), &bytes)
            .unwrap();
    }
    evidence
}

fn host_scope(node: &str, alias: &str, activation: u64) -> broker_host::TrustedHostScope {
    bootstrap_host_context(HostBootstrapIdentity {
        org_id: "org".into(),
        principal_id: "deployment".into(),
        bundle_id: "bundle".into(),
        flow_ir_hash: FLOW_HASH_PLACEHOLDER.into(),
        binding_lock_hash: LOCK_HASH.into(),
        flow_id: "flow".into(),
        run_id: "run".into(),
    })
    .unwrap()
    .scope_for_activation(node, alias, activation)
    .unwrap()
}

fn executor(
    evidence: BrokerBindingEvidence,
    registry: BrokerDescriptorRegistry,
    scripts: Vec<ScriptedDispatch>,
    receipt_signer: BrokerSigner,
) -> LocalBrokerExecutor {
    executor_with_pop_proof(
        evidence,
        registry,
        scripts,
        receipt_signer,
        b"proof".to_vec(),
    )
}

fn executor_with_pop_proof(
    evidence: BrokerBindingEvidence,
    registry: BrokerDescriptorRegistry,
    scripts: Vec<ScriptedDispatch>,
    receipt_signer: BrokerSigner,
    pop_proof: Vec<u8>,
) -> LocalBrokerExecutor {
    LocalBrokerExecutor::new(LocalBrokerConfig {
        evidence,
        descriptors: registry,
        clock: FixedClock("2026-07-19T12:00:00Z".into()),
        dispatch_scripts: scripts,
        authority_facts: br#"{"allowed":true}"#.to_vec(),
        custodian_epoch: 4,
        synthetic_secret: b"test-only-secret".to_vec(),
        pop_method: ChannelMethod::DeploymentKey,
        pop_key_thumbprint: format!("sha256:{}", "4".repeat(64)),
        pop_session_id: "session".into(),
        pop_proof,
        expected_pop_proof: b"proof".to_vec(),
        grant_issuer: "issuer".into(),
        grant_not_before: "2026-07-19T11:55:00Z".into(),
        grant_expires_at: "2026-07-19T12:05:00Z".into(),
        max_grant_lifetime_seconds: 600,
        receipt_signer,
        commitments: CommitmentKey::new("commit-key", [8; 32]).unwrap(),
        broker_principal_id: "broker".into(),
    })
    .unwrap()
}

fn confirmed(id: &str) -> ScriptedDispatch {
    ScriptedDispatch::Confirmed {
        projection: br#"{"ok":true}"#.to_vec(),
        provider_request_id: Some(id.into()),
    }
}

#[test]
fn lock_pinned_binding_matrix_rejects_before_any_dispatch() {
    let trusted = BrokerSigner::from_seed("binding-key", [1; 32]);
    let attacker = BrokerSigner::from_seed("attacker-key", [9; 32]);
    let (_, contracts) = registry();
    let (id, hash) = (&contracts[0].0, &contracts[0].1);
    let cases: Vec<(&str, Vec<u8>, BrokerHostError)> = vec![
        (
            "attacker key",
            binding(&attacker, &contracts, |_| {}),
            BrokerHostError::BindingSignatureRejected,
        ),
        (
            "issuer",
            binding(&trusted, &contracts, |v| v.issuer = "evil".into()),
            BrokerHostError::BindingIssuerMismatch,
        ),
        (
            "key id",
            binding(&trusted, &contracts, |v| {
                v.broker_key_id = "other-key".into()
            }),
            BrokerHostError::BindingKeyMismatch,
        ),
        (
            "org",
            binding(&trusted, &contracts, |v| v.org_id = "other".into()),
            BrokerHostError::BindingOrgMismatch,
        ),
        (
            "connection",
            binding(&trusted, &contracts, |v| v.connection_ref = "other".into()),
            BrokerHostError::BindingConnectionMismatch,
        ),
        (
            "provider",
            binding(&trusted, &contracts, |v| v.provider = "other".into()),
            BrokerHostError::BindingProviderMismatch,
        ),
        (
            "account",
            binding(&trusted, &contracts, |v| {
                v.account_commitment.value = format!("hmac-sha256:{}", "f".repeat(64))
            }),
            BrokerHostError::BindingAccountMismatch,
        ),
        (
            "role",
            binding(&trusted, &contracts, |v| {
                v.roles
                    .insert("outbound_auth.synthetic".into(), "synthetic.other".into());
            }),
            BrokerHostError::BindingRolesMismatch,
        ),
        (
            "scope",
            binding(&trusted, &contracts, |v| {
                v.scope_alignment.actual_scopes = vec!["synthetic.read".into()];
            }),
            BrokerHostError::BindingScopesMismatch,
        ),
    ];
    for (name, bytes, expected) in cases {
        let mut evidence = BrokerBindingEvidence::new();
        evidence
            .insert_lock_record("one", lock(&trusted), &bytes)
            .unwrap();
        assert_eq!(
            evidence
                .verify("one", id, hash, 4, "2026-07-19T12:00:00Z")
                .unwrap_err(),
            expected,
            "{name}"
        );
    }
    let valid = evidence(&trusted, &contracts, &["one"]);
    assert_eq!(
        valid
            .verify("one", id, hash, 5, "2026-07-19T12:00:00Z")
            .unwrap_err(),
        BrokerHostError::Broker(BrokerError::Brk106)
    );
}

#[test]
fn s21_distinct_descriptors_issue_grants_internally_and_plan_distinctly() {
    let binding_signer = BrokerSigner::from_seed("binding-key", [1; 32]);
    let (registry, contracts) = registry();
    let executor = executor(
        evidence(&binding_signer, &contracts, &["one", "two", "three"]),
        registry,
        vec![confirmed("1"), confirmed("2"), confirmed("3")],
        BrokerSigner::from_seed("receipt-key", [2; 32]),
    );
    assert_eq!(
        executor
            .operation(&contracts[0].0, &contracts[1].1, "one", "one")
            .unwrap_err(),
        BrokerHostError::DescriptorMismatch
    );
    assert_eq!(executor.custodian_call_count(), 0);
    assert_eq!(executor.dispatch_count(), 0);

    for (index, alias) in ["one", "two", "three"].into_iter().enumerate() {
        let operation = executor
            .operation(&contracts[index].0, &contracts[index].1, alias, alias)
            .unwrap();
        let result = executor
            .invoke(
                &host_scope(&format!("node-{alias}"), alias, 1),
                &operation,
                br#"{"value":"x"}"#,
            )
            .unwrap();
        assert!(result.receipt.claims.provider_dispatch_observed);
    }
    let plans = executor.recorded_plans();
    assert_eq!(plans.len(), 3);
    assert_ne!(plans[0], plans[1]);
    assert_ne!(plans[1], plans[2]);
    for plan in &plans {
        let text = String::from_utf8(plan.clone()).unwrap();
        assert!(!text.contains("$broker:"));
        assert!(text.contains("2026-07-19T12:00:00Z"));
        assert!(text.contains("lattice-"));
        assert!(text.contains("strict"));
    }

    let op = executor
        .operation(&contracts[0].0, &contracts[0].1, "one", "one")
        .unwrap();
    let before_dispatch = executor.dispatch_count();
    let before_custodian = executor.custodian_call_count();
    assert_eq!(
        executor
            .invoke(
                &host_scope("wrong-node", "one", 2),
                &op,
                br#"{"value":1} trailing"#
            )
            .unwrap_err(),
        BrokerHostError::Broker(BrokerError::Brk001)
    );
    assert_eq!(executor.dispatch_count(), before_dispatch);
    assert_eq!(executor.custodian_call_count(), before_custodian);
}

#[test]
fn pop_mismatch_fails_before_custodian_or_dispatch() {
    let binding_signer = BrokerSigner::from_seed("binding-key", [1; 32]);
    let (registry, contracts) = registry();
    let executor = executor_with_pop_proof(
        evidence(&binding_signer, &contracts, &["one"]),
        registry,
        vec![confirmed("unused")],
        BrokerSigner::from_seed("receipt-key", [2; 32]),
        b"wrong-proof".to_vec(),
    );
    let operation = executor
        .operation(&contracts[0].0, &contracts[0].1, "one", "one")
        .unwrap();
    assert_eq!(
        executor
            .invoke(
                &host_scope("node-one", "one", 1),
                &operation,
                br#"{"value":"x"}"#,
            )
            .unwrap_err(),
        BrokerHostError::Broker(BrokerError::Brk102)
    );
    assert_eq!(executor.custodian_call_count(), 0);
    assert_eq!(executor.dispatch_count(), 0);
}

#[test]
fn real_for_each_body_entry_fails_closed_without_bound_and_accepts_bound() {
    let spec = NodeSpec::inline(
        "test.synthetic",
        "Synthetic",
        SchemaSpec::Opaque,
        SchemaSpec::Opaque,
        Effects::Effectful,
        Determinism::Nondeterministic,
        None,
    );
    let mut builder = FlowBuilder::new("fanout", Version::new(1, 0, 0), Profile::Dev);
    let source = builder.add_node("source", &spec).unwrap();
    let body = builder.add_node("body", &spec).unwrap();
    builder.connect(&source, &body);
    let mut flow = builder.build();
    flow.nodes[1]
        .connector_ops
        .push(dag_core::ConnectorOpRefIR {
            operation_id: "connector.synthetic.one".into(),
            connector_id: "connector.synthetic".into(),
            broker_contract: None,
            roles: vec![],
            default_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
            selected_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
            supported_resolution_modes: SUPPORTED.to_vec(),
        });
    flow.nodes[1].broker_authority = Some(
        BrokerAuthority::new(
            vec![BrokerOperationBudget {
                contract_id: "connector.synthetic.one@1".into(),
                semantic_effect_slots: vec!["one".into()],
                max_logical_calls: 1,
                max_dispatch_attempts_per_call: 1,
                connection_aggregate_key: None,
            }],
            None,
            BTreeMap::new(),
        )
        .unwrap(),
    );
    flow.control_surfaces.push(ControlSurfaceIR {
        id: "for_each:source:0".into(),
        kind: ControlSurfaceKind::ForEach,
        targets: vec![],
        config: serde_json::json!({
            "v": 1,
            "source": "source",
            "items_pointer": "/items",
            "body_entry": "body"
        }),
    });
    assert!(
        kernel_plan::validate(&flow)
            .unwrap_err()
            .iter()
            .any(|diagnostic| diagnostic.code.code == "BRK001")
    );
    flow.control_surfaces[0].config["max_items"] = serde_json::json!(10);
    assert!(kernel_plan::validate(&flow).is_ok());
}

#[test]
fn manifest_binds_exact_ir_bytes_and_is_artifact_validated() {
    let spec = NodeSpec::inline(
        "test.synthetic",
        "Synthetic",
        SchemaSpec::Opaque,
        SchemaSpec::Opaque,
        Effects::Effectful,
        Determinism::Nondeterministic,
        None,
    );
    let mut builder = FlowBuilder::new("manifest", Version::new(1, 0, 0), Profile::Dev);
    let node = builder.add_node("one", &spec).unwrap();
    let mut flow = builder.build();
    flow.nodes[0]
        .connector_ops
        .push(dag_core::ConnectorOpRefIR {
            operation_id: OP.operation_id.into(),
            connector_id: OP.connector_id.into(),
            broker_contract: None,
            roles: vec![],
            default_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
            selected_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
            supported_resolution_modes: SUPPORTED.to_vec(),
        });
    flow.nodes[0].broker_authority = Some(
        BrokerAuthority::new(
            vec![BrokerOperationBudget {
                contract_id: "connector.synthetic.one@1".into(),
                semantic_effect_slots: vec!["one".into()],
                max_logical_calls: 1,
                max_dispatch_attempts_per_call: 1,
                connection_aggregate_key: None,
            }],
            None,
            BTreeMap::new(),
        )
        .unwrap(),
    );
    let validated = kernel_plan::validate(&flow).unwrap();
    let bytes = serde_json::to_vec(validated.flow()).unwrap();
    let hash = format!("sha256:{}", hex::encode(Sha256::digest(&bytes)));
    let (_, contracts) = registry();
    let metadata = [NodeOperationMetadata {
        node_alias: "one",
        operation: &OP,
        contract: Some(BrokerContractMetadata {
            contract_id: "connector.synthetic.one@1",
            contract_hash: Box::leak(contracts[0].1.clone().into_boxed_str()),
            minimum_scopes: &[],
        }),
    }];
    let manifest = derive_authority_manifest(
        &validated,
        &bytes,
        &hash,
        ManifestProvenance {
            org_id: "org",
            principal_kind: PrincipalKind::Service,
            principal_id: "builder",
        },
        &metadata,
    )
    .unwrap();
    assert_eq!(manifest.flow_ir_hash, hash);
    assert_eq!(
        derive_authority_manifest(
            &validated,
            &bytes,
            FLOW_HASH_PLACEHOLDER,
            ManifestProvenance {
                org_id: "org",
                principal_kind: PrincipalKind::Service,
                principal_id: "builder",
            },
            &metadata,
        )
        .unwrap_err(),
        AuthorityManifestError::FlowIrHashMismatch
    );
    let _ = node;
}

#[test]
fn remote_transport_cannot_fabricate_or_tamper_success() {
    let binding_signer = BrokerSigner::from_seed("binding-key", [1; 32]);
    let receipt_signer = BrokerSigner::from_seed("receipt-key", [2; 32]);
    let (registry, contracts) = registry();
    let local = executor(
        evidence(&binding_signer, &contracts, &["one"]),
        registry,
        vec![confirmed("1")],
        BrokerSigner::from_seed("receipt-key", [2; 32]),
    );
    let operation = local
        .operation(&contracts[0].0, &contracts[0].1, "one", "one")
        .unwrap();
    let scope = host_scope("node-one", "one", 1);
    let outcome = local
        .invoke(&scope, &operation, br#"{"value":"x"}"#)
        .unwrap();
    let mut trust = ReceiptTrustStore::new();
    trust
        .insert_key(
            "issuer",
            "receipt-key",
            receipt_signer.verifying_key(),
            [outcome.receipt.grant_hash.clone()],
        )
        .unwrap();
    trust.pin_connection_epoch("one", 4);

    let mut tampered = outcome.clone();
    tampered.outcome = broker_core::artifacts::Outcome::Failed;
    let transport = TestBrokerTransport::new([Ok(RemoteInvokeResponse { outcome: tampered })]);
    let remote = RemoteBrokerExecutor::new(
        transport,
        evidence(&binding_signer, &contracts, &["one"]),
        FixedClock("2026-07-19T12:00:00Z".into()),
        trust,
    );
    assert_eq!(
        remote
            .invoke(&scope, &operation, br#"{"value":"x"}"#)
            .unwrap_err(),
        BrokerHostError::ReceiptVerificationFailed
    );
    assert_eq!(remote.transport().call_count(), 1);
}
