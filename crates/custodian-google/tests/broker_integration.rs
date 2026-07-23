use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use broker_core::{
    BrokerError,
    artifacts::{
        BINDING_MAX, BindingAttestation, ChannelMethod, CommitmentAlg, CommitmentEnvelope, Outcome,
        PluginTrustTier, PrincipalKind, PrincipalRef, ScopeAlignment, SignatureAlg,
        SignatureEnvelope, SupportedContract,
    },
    canonical,
    commitment::CommitmentKey,
    custodian::CredentialCustodian,
    dispatch::ScriptedDispatch,
    engine::ImplementationApproval,
    grant::FixedClock,
    signing::{BINDING_DOMAIN, BrokerSigner},
};
use broker_host::{
    BindingLockRecord, BrokerBindingEvidence, BrokerDescriptorRegistry, BrokerHostError,
    ConnectorExecutor, HostBootstrapIdentity, HostContext, LocalBrokerConfig, LocalBrokerExecutor,
    bootstrap_host_context,
};
use connector_spec::BrokerDispatchDescriptor;
use sha2::{Digest, Sha256};

use custodian_google::{
    CachedAccessToken, ConnectionRegistration, ConnectionStore, GoogleCustodian,
    InMemoryConnectionStore, MockTokenEndpoint, MockTokenResult, RootKey, SecretBytes,
    TokenResponse,
};

const FLOW_HASH: &str = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
const LOCK_HASH: &str = "sha256:3333333333333333333333333333333333333333333333333333333333333333";
const GMAIL: &str = "https://www.googleapis.com/auth/gmail.send";
const SHEETS: &str = "https://www.googleapis.com/auth/spreadsheets";
const REFRESH: &str = "integration-refresh-never-log";
const STALE: &str = "integration-stale-never-log";
const FRESH: &str = "integration-fresh-never-log";

type TestCustodian = GoogleCustodian<InMemoryConnectionStore, MockTokenEndpoint, FixedClock>;
type TestExecutor = LocalBrokerExecutor<TestCustodian>;

#[derive(Clone, Copy)]
enum GoogleOperation {
    Sheets,
    Gmail,
}

impl GoogleOperation {
    fn id(self) -> &'static str {
        match self {
            Self::Sheets => "connector.google.sheets.append_row@1",
            Self::Gmail => "connector.google.gmail.send_message@1",
        }
    }

    fn scope(self) -> &'static str {
        match self {
            Self::Sheets => SHEETS,
            Self::Gmail => GMAIL,
        }
    }

    fn origin(self) -> &'static str {
        match self {
            Self::Sheets => "https://sheets.googleapis.com",
            Self::Gmail => "https://gmail.googleapis.com",
        }
    }

    fn slot(self) -> &'static str {
        match self {
            Self::Sheets => "append_row",
            Self::Gmail => "send_message",
        }
    }

    fn input(self) -> &'static [u8] {
        match self {
            Self::Sheets => {
                br#"{"row":{"column":"one"},"sheet":"Sheet1","spreadsheet_id":"sheet-1"}"#
            }
            Self::Gmail => br#"{"subject":"Hello","text_body":"body","to":"test@example.com"}"#,
        }
    }

    fn projection(self) -> &'static [u8] {
        match self {
            Self::Sheets => br#"{"row_index":2,"updated_range":"Sheet1!A2"}"#,
            Self::Gmail => br#"{"id":"msg-1","thread_id":"thread-1"}"#,
        }
    }
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

fn all_scopes() -> BTreeSet<String> {
    BTreeSet::from([GMAIL.into(), SHEETS.into()])
}

fn roles() -> BTreeMap<String, String> {
    BTreeMap::from([(
        "outbound_auth.google_workspace_auth".into(),
        "oauth2.access_token".into(),
    )])
}

fn descriptor(operation: GoogleOperation) -> (Vec<u8>, String) {
    let bytes = match operation {
        GoogleOperation::Sheets => {
            include_bytes!("../../connectors/google/sheets/broker/operations/append_row.json")
                .to_vec()
        }
        GoogleOperation::Gmail => {
            include_bytes!("../../connectors/google/gmail/broker/operations/send_message.json")
                .to_vec()
        }
    };
    let descriptor: BrokerDispatchDescriptor = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(descriptor.contract.contract_id, operation.id());
    assert_eq!(descriptor.request_plan.origin, operation.origin());
    (bytes, descriptor.contract_hash)
}

fn approval(hash: &str) -> ImplementationApproval {
    ImplementationApproval {
        contract_hash: hash.into(),
        implementation: "google-v1-request-plan".into(),
        plugin_module_sha256: format!("sha256:{}", "5".repeat(64)),
        trust_tier: PluginTrustTier::LatticeFirstParty,
        policy_hash: format!("sha256:{}", "6".repeat(64)),
    }
}

fn binding(signer: &BrokerSigner, operation: GoogleOperation, hash: &str) -> Vec<u8> {
    let placeholder = SignatureEnvelope {
        alg: SignatureAlg::Ed25519,
        key_id: signer.key_id().into(),
        value: String::new(),
    };
    let scopes = all_scopes().into_iter().collect::<Vec<_>>();
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
        connection_ref: "google-primary".into(),
        provider: "google".into(),
        account_commitment: account(),
        roles: roles(),
        scope_alignment: ScopeAlignment {
            required_scopes: vec![operation.scope().into()],
            actual_scopes: scopes,
            satisfied: true,
            extensions: Default::default(),
        },
        supported_contracts: vec![SupportedContract {
            contract_id: operation.id().into(),
            contract_hash: hash.into(),
            observed_plugin_module_sha256: Some(format!("sha256:{}", "5".repeat(64))),
            attenuation_profiles: vec![],
            extensions: Default::default(),
        }],
        endpoint_origins: vec![operation.origin().into()],
        revocation_epoch: 4,
        not_before: "2026-07-19T10:59:59Z".into(),
        observed_at: "2026-07-19T11:00:00Z".into(),
        expires_at: "2026-07-19T13:00:00Z".into(),
        signature: placeholder,
        extensions: Default::default(),
    };
    let unsigned = canonical::from_serde(&value, BINDING_MAX)
        .unwrap()
        .into_bytes();
    value.signature = signer.sign_json(BINDING_DOMAIN, &unsigned).unwrap();
    canonical::from_serde(&value, BINDING_MAX)
        .unwrap()
        .into_bytes()
}

fn host() -> HostContext {
    bootstrap_host_context(HostBootstrapIdentity {
        org_id: "org".into(),
        principal_id: "deployment".into(),
        bundle_id: "bundle".into(),
        flow_ir_hash: FLOW_HASH.into(),
        binding_lock_hash: LOCK_HASH.into(),
        flow_id: "flow".into(),
        run_id: "run".into(),
    })
    .unwrap()
}

fn registration(expiry: Option<i64>) -> ConnectionRegistration {
    ConnectionRegistration {
        connection_ref: "google-primary".into(),
        org_id: "org".into(),
        account_commitment: account(),
        roles: roles(),
        granted_scopes: all_scopes(),
        refresh_token: SecretBytes::new(REFRESH.as_bytes().to_vec()),
        cached_access_token: expiry.map(|expires_at| CachedAccessToken {
            token: SecretBytes::new(STALE.as_bytes().to_vec()),
            expires_at,
            scopes: all_scopes(),
        }),
        revocation_epoch: 4,
    }
}

fn build(
    operation: GoogleOperation,
    token_results: impl IntoIterator<Item = MockTokenResult>,
    expiry: Option<i64>,
    revoke_before_bootstrap: bool,
    install_adapters: bool,
) -> (
    HostContext,
    TestExecutor,
    Arc<MockTokenEndpoint>,
    Arc<InMemoryConnectionStore>,
    String,
) {
    let binding_signer = BrokerSigner::from_seed("binding-key", [1; 32]);
    let (descriptor, hash) = descriptor(operation);
    let mut descriptors = BrokerDescriptorRegistry::new();
    descriptors
        .load_generated(&descriptor, approval(&hash), 1, 1)
        .unwrap();
    let attestation = binding(&binding_signer, operation, &hash);
    let lock = BindingLockRecord::from_binding_lock(
        "issuer",
        "binding-key",
        binding_signer.verifying_key(),
        "org",
        "google-primary",
        "google",
        account(),
        roles(),
        vec![operation.scope().into()],
        all_scopes().into_iter().collect(),
    )
    .unwrap();
    let mut evidence = BrokerBindingEvidence::new();
    evidence
        .insert_lock_record("primary", lock, &attestation)
        .unwrap();
    let endpoint = Arc::new(MockTokenEndpoint::new(token_results));
    let store = Arc::new(InMemoryConnectionStore::new());
    let custodian = GoogleCustodian::register(
        store.clone(),
        endpoint.clone(),
        FixedClock("2026-07-19T12:00:00Z".into()),
        RootKey::new([7; 32]),
        registration(expiry),
    )
    .unwrap();
    if revoke_before_bootstrap {
        custodian.revoke().unwrap();
    }
    let config = LocalBrokerConfig {
        evidence,
        descriptors,
        clock: FixedClock("2026-07-19T12:00:00Z".into()),
        dispatch_scripts: vec![ScriptedDispatch::Confirmed {
            projection: operation.projection().to_vec(),
            provider_request_id: Some("google-request-1".into()),
        }],
        authority_facts: match operation {
            GoogleOperation::Sheets => br#"{"google":{"sheets":{"headers":["column"]}}}"#.to_vec(),
            GoogleOperation::Gmail => br#"{"allowed":true}"#.to_vec(),
        },
        custodian_epoch: 4,
        synthetic_secret: vec![],
        pop_method: ChannelMethod::DeploymentKey,
        pop_key_thumbprint: format!("sha256:{}", "4".repeat(64)),
        pop_session_id: "session".into(),
        pop_proof: b"proof".to_vec(),
        expected_pop_proof: b"proof".to_vec(),
        grant_issuer: "issuer".into(),
        grant_not_before: "2026-07-19T11:55:00Z".into(),
        grant_expires_at: "2026-07-19T12:05:00Z".into(),
        max_grant_lifetime_seconds: 600,
        receipt_signer: BrokerSigner::from_seed("receipt-key", [2; 32]),
        commitments: CommitmentKey::new("commit-key", [8; 32]).unwrap(),
        broker_principal_id: "broker".into(),
    };
    let host = host();
    let adapters = if install_adapters {
        provider_google::host_registry("2026-07-21T00:00:00Z").unwrap()
    } else {
        broker_host::TrustedAdapterRegistry::empty()
    };
    let executor = host
        .local_broker_executor(config, custodian, adapters)
        .unwrap();
    (host, executor, endpoint, store, hash)
}

fn assert_exact_plan(operation: GoogleOperation, plan: &[u8]) {
    let plan: serde_json::Value = serde_json::from_slice(plan).unwrap();
    assert_eq!(plan["method"], "POST");
    match operation {
        GoogleOperation::Sheets => {
            assert_eq!(
                plan["path"],
                "/v4/spreadsheets/sheet%2D1/values/%27Sheet1%27%21A1%3AA:append"
            );
            assert_eq!(
                plan["query"],
                serde_json::json!({
                    "insertDataOption": "INSERT_ROWS",
                    "valueInputOption": "RAW"
                })
            );
            assert_eq!(plan["body"], serde_json::json!({"values":[["one"]]}));
        }
        GoogleOperation::Gmail => {
            assert_eq!(plan["path"], "/gmail/v1/users/me/messages/send");
            assert_eq!(plan["query"], serde_json::json!({}));
            let message = connector_google_platform::gmail::build_plain_text_email(
                "test@example.com",
                None,
                None,
                "Hello",
                "body",
            );
            let raw = connector_google_platform::gmail::base64url_no_pad(message.as_bytes());
            assert_eq!(plan["body"], serde_json::json!({"raw": raw}));
        }
    }
}

fn invoke(
    host: &HostContext,
    executor: &TestExecutor,
    operation: GoogleOperation,
    hash: &str,
) -> Result<broker_host::ConnectorOutcome, BrokerHostError> {
    let broker_operation = executor
        .operation(operation.id(), hash, "primary", operation.slot())
        .unwrap();
    let scope = host
        .scope_for_activation("google-node", "google", 1)
        .unwrap();
    executor.invoke(&scope, &broker_operation, operation.input())
}

#[test]
fn both_google_contracts_dispatch_and_issue_correct_receipt_claims() {
    for operation in [GoogleOperation::Sheets, GoogleOperation::Gmail] {
        let (host, executor, endpoint, _, hash) = build(operation, [], Some(i64::MAX), false, true);
        let outcome = invoke(&host, &executor, operation, &hash).unwrap();
        assert_eq!(outcome.outcome, Outcome::Confirmed);
        assert_eq!(outcome.receipt.contract_hash, hash);
        assert!(outcome.receipt.claims.trusted_host_scope_authenticated);
        assert!(outcome.receipt.claims.broker_admission_enforced);
        assert!(outcome.receipt.claims.provider_dispatch_observed);
        assert_eq!(
            outcome.receipt.provider_request_id.as_deref(),
            Some("google-request-1")
        );
        assert_eq!(executor.dispatch_count(), 1);
        let plans = executor.recorded_plans();
        assert_exact_plan(operation, &plans[0]);
        let expected_plan_hash = format!("sha256:{}", hex::encode(Sha256::digest(&plans[0])));
        assert_eq!(
            outcome.receipt.request_plan_hash.as_deref(),
            Some(expected_plan_hash.as_str())
        );
        assert_eq!(endpoint.call_count(), 0);
        let public = format!(
            "{outcome:?} {}",
            String::from_utf8_lossy(&outcome.canonical_receipt)
        );
        for secret in [REFRESH, STALE, FRESH] {
            assert!(!public.contains(secret));
        }
    }
}

#[test]
fn expired_token_refreshes_once_before_google_dispatch() {
    for operation in [GoogleOperation::Sheets, GoogleOperation::Gmail] {
        let response = TokenResponse {
            access_token: SecretBytes::new(FRESH.as_bytes().to_vec()),
            expires_in: 3600,
            scopes: all_scopes(),
        };
        let (host, executor, endpoint, _, hash) = build(
            operation,
            [MockTokenResult::Response(response)],
            Some(0),
            false,
            true,
        );
        let outcome = invoke(&host, &executor, operation, &hash).unwrap();
        assert_eq!(outcome.outcome, Outcome::Confirmed);
        assert_eq!(endpoint.call_count(), 1);
        assert_eq!(executor.dispatch_count(), 1);
    }
}

#[test]
fn shrunk_scope_refresh_bumps_epoch_and_rejects_subsequent_admission() {
    let response = TokenResponse {
        access_token: SecretBytes::new(FRESH.as_bytes().to_vec()),
        expires_in: 3600,
        scopes: BTreeSet::from([GMAIL.into()]),
    };
    let (host, executor, endpoint, store, hash) = build(
        GoogleOperation::Sheets,
        [MockTokenResult::Response(response)],
        Some(0),
        false,
        true,
    );
    assert_eq!(
        invoke(&host, &executor, GoogleOperation::Sheets, &hash).unwrap_err(),
        BrokerHostError::Broker(BrokerError::Brk109)
    );
    assert_eq!(endpoint.call_count(), 1);
    assert_eq!(executor.dispatch_count(), 0);
    assert_eq!(
        store
            .load("google-primary")
            .unwrap()
            .unwrap()
            .revocation_epoch,
        5
    );
    assert_eq!(
        invoke(&host, &executor, GoogleOperation::Sheets, &hash).unwrap_err(),
        BrokerHostError::Broker(BrokerError::Brk106)
    );
    assert_eq!(executor.dispatch_count(), 0);
}

#[test]
fn unpinned_trusted_adapter_fails_closed_before_dispatch() {
    let (host, executor, endpoint, _, hash) =
        build(GoogleOperation::Gmail, [], Some(i64::MAX), false, false);
    assert_eq!(
        invoke(&host, &executor, GoogleOperation::Gmail, &hash).unwrap_err(),
        BrokerHostError::DescriptorMismatch
    );
    assert_eq!(endpoint.call_count(), 0);
    assert_eq!(executor.dispatch_count(), 0);
}

#[test]
fn invalid_grant_and_revoked_connection_fail_before_dispatch() {
    let (host, executor, endpoint, store, hash) = build(
        GoogleOperation::Gmail,
        [MockTokenResult::InvalidGrant],
        Some(0),
        false,
        true,
    );
    let error = invoke(&host, &executor, GoogleOperation::Gmail, &hash).unwrap_err();
    assert_eq!(error, BrokerHostError::Broker(BrokerError::Brk106));
    assert_eq!(endpoint.call_count(), 1);
    assert_eq!(executor.dispatch_count(), 0);
    assert_eq!(
        store
            .load("google-primary")
            .unwrap()
            .unwrap()
            .revocation_epoch,
        5
    );
    let public = format!("{error:?} {error}");
    for secret in [REFRESH, STALE, FRESH] {
        assert!(!public.contains(secret));
    }

    let (host, executor, endpoint, store, hash) =
        build(GoogleOperation::Gmail, [], Some(i64::MAX), true, true);
    assert_eq!(
        store
            .load("google-primary")
            .unwrap()
            .unwrap()
            .revocation_epoch,
        5
    );
    assert_eq!(
        invoke(&host, &executor, GoogleOperation::Gmail, &hash).unwrap_err(),
        BrokerHostError::Broker(BrokerError::Brk106)
    );
    assert_eq!(endpoint.call_count(), 0);
    assert_eq!(executor.dispatch_count(), 0);
}
